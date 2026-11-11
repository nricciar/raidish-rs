use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError};
use crate::stripe::{DISKS, Stripe};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const DATA_SHARDS: usize = 3;
pub const PARITY_SHARDS: usize = 2;

#[derive(Debug)]
pub enum RaidZError {
    DiskError(DiskError),
    ReedSolomonError(reed_solomon_simd::Error),
    SerializationError(bincode::Error),
    IncompleteStripe(u64),
    InconsistentStripe(u64),
    NotFormatted,
}

impl fmt::Display for RaidZError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaidZError::DiskError(err) => write!(f, "{}", err),
            RaidZError::ReedSolomonError(err) => write!(f, "Reed Solomon error: {}", err),
            RaidZError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            RaidZError::IncompleteStripe(stripe) => write!(f, "incomplete stripe: {}", stripe),
            RaidZError::InconsistentStripe(stripe) => {
                write!(f, "stripe {} parity is inconsistent", stripe)
            }
            RaidZError::NotFormatted => write!(f, "array is not formatted"),
        }
    }
}

impl From<DiskError> for RaidZError {
    fn from(error: DiskError) -> Self {
        RaidZError::DiskError(error)
    }
}

impl From<bincode::Error> for RaidZError {
    fn from(error: bincode::Error) -> Self {
        RaidZError::SerializationError(error)
    }
}

impl From<reed_solomon_simd::Error> for RaidZError {
    fn from(error: reed_solomon_simd::Error) -> Self {
        RaidZError::ReedSolomonError(error)
    }
}

#[derive(Debug)]
pub struct RaidZ {
    pub stripe: Stripe,
    writes: Arc<DashMap<u64, Arc<Mutex<StripeBuffer>>>>,
    stripe_pool: Arc<ArrayQueue<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>>,
}

#[async_trait]
impl BlockDevice for RaidZ {
    async fn flush(&self) -> Result<(), DiskError> {
        let stripe_ids: Vec<u64> = self.writes.iter().map(|r| *r.key()).collect();

        for stripe_id in stripe_ids {
            let stripe_buf_arc = match self.writes.get(&stripe_id) {
                Some(entry) => entry.value().clone(),
                None => continue,
            };

            let mut stripe_buf = stripe_buf_arc.lock().await;

            // Check if already flushed
            if stripe_buf.flushed && !stripe_buf.dirty {
                continue;
            }

            if !stripe_buf.is_complete() {
                let mut missing_indices = Vec::new();
                for i in 0..DATA_SHARDS {
                    if !stripe_buf.has_shard(i) {
                        missing_indices.push(i);
                    }
                }

                stripe_buf
                    .data
                    .get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));

                let mut read_tasks = FuturesUnordered::new();
                for i in missing_indices {
                    read_tasks.push(async move {
                        let mut buf = [0u8; BLOCK_SIZE];
                        // A missing disk shouldn't block the write: rebuild
                        // the shard from the rest of the stripe instead.
                        if self.stripe.read_shards(i, stripe_id, &mut buf).await.is_err() {
                            self.reconstruct_from_stripe(stripe_id, i, &mut buf)
                                .await
                                .map_err(|e| {
                                    DiskError::IoError(std::io::Error::other(format!("{}", e)))
                                })?;
                        }
                        Ok::<_, DiskError>((i, buf))
                    });
                }

                while let Some(result) = read_tasks.next().await {
                    let (shard_idx, buf) = result?;
                    let offset = shard_idx * BLOCK_SIZE;
                    if let Some(data) = stripe_buf.data.as_mut() {
                        data[offset..offset + BLOCK_SIZE].copy_from_slice(&buf);
                    }
                    stripe_buf.shard_mask |= 1 << shard_idx;
                }
            }

            // Now stripe is complete, write it. On failure the stripe stays
            // buffered, so a later flush retries it.
            self.write_full_stripe(&stripe_buf, stripe_id)
                .await
                .map_err(|e| DiskError::IoError(std::io::Error::other(format!("{}", e))))?;

            if let Some(buffer) = stripe_buf.take_buffer() {
                let _ = self.stripe_pool.push(buffer);
            }

            drop(stripe_buf);
            self.writes.remove(&stripe_id);
        }

        // Like stripe writes, flushing tolerates up to PARITY_SHARDS failed
        // disks: the data is still recoverable from the rest.
        let flushes = (0..DISKS).map(|d| self.stripe.flush_disk(d));
        let results = futures::future::join_all(flushes).await;
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures > PARITY_SHARDS {
            return results.into_iter().find(Result::is_err).unwrap();
        }

        Ok(())
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        if let Some(stripe_buf_ref) = self.writes.get(&(stripe as u64)) {
            let stripe_buf = stripe_buf_ref.lock().await;
            if stripe_buf.has_shard(data_index) && stripe_buf.get_shard(data_index, buf) {
                return Ok(());
            }
        }

        match self.stripe.read_block(lba, buf).await {
            Ok(()) => return Ok(()),
            Err(_disk_error) => {
                // Primary disk failed, reconstruct
                match self
                    .reconstruct_from_stripe(stripe as u64, data_index, buf)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(DiskError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{}", e),
                    ))),
                }
            }
        }
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;

        // Get or create stripe buffer
        let stripe = if let Some(stripe_ref) = self.writes.get(&stripe_id) {
            stripe_ref.value().clone()
        } else {
            let buffer = { self.stripe_pool.pop() };

            let new_stripe = Arc::new(Mutex::new(
                buffer
                    .map(StripeBuffer::with_buffer)
                    .unwrap_or_else(StripeBuffer::new),
            ));

            // 3. Insert into map using Entry API to handle races safely
            match self.writes.entry(stripe_id) {
                dashmap::mapref::entry::Entry::Occupied(o) => {
                    // We lost the race; another thread inserted the stripe while we were getting the buffer.
                    // We discard our 'new_stripe'. To prevent "leaking" the pooled buffer, return it.
                    {
                        // Locking new_stripe is safe/instant as we hold the only reference
                        let mut guard =
                            new_stripe.try_lock().expect("Exclusive ownership expected");
                        if let Some(buf) = guard.take_buffer() {
                            let _ = self.stripe_pool.push(buf);
                        }
                    }
                    o.get().clone()
                }
                dashmap::mapref::entry::Entry::Vacant(v) => {
                    // We won the race, insert our new stripe
                    v.insert(new_stripe.clone());
                    new_stripe
                }
            }
        };

        stripe.lock().await.set_shard(shard_idx, buf);
        Ok(())
    }

    /// Logical capacity: every stripe holds `DATA_SHARDS` data blocks.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        Ok(self.stripe.capacity_blocks().await? * DATA_SHARDS as u64)
    }

    /// Contiguous reads become one read per data disk. Stripes with
    /// unflushed writes (or a failed disk) fall back to per-block reads.
    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let n = (buf.len() / BLOCK_SIZE) as u64;
        if n == 0 {
            return Ok(());
        }
        let d = DATA_SHARDS as u64;
        let (s_first, s_last) = (start / d, (start + n - 1) / d);

        let buffered = (s_first..=s_last).any(|s| self.writes.contains_key(&s));
        if n > 1 && !buffered {
            let count = (s_last - s_first + 1) as usize;
            let runs = (0..DATA_SHARDS).map(|disk| async move {
                let mut run = vec![0u8; count * BLOCK_SIZE];
                self.stripe
                    .read_shards(disk, s_first, &mut run)
                    .await
                    .map(|_| run)
            });
            if let Ok(runs) = futures::future::try_join_all(runs).await {
                for (i, out) in buf.chunks_mut(BLOCK_SIZE).enumerate() {
                    let lba = start + i as u64;
                    let at = (lba / d - s_first) as usize * BLOCK_SIZE;
                    out.copy_from_slice(&runs[(lba % d) as usize][at..at + BLOCK_SIZE]);
                }
                return Ok(());
            }
        }

        for (i, out) in buf.chunks_mut(BLOCK_SIZE).enumerate() {
            self.read_block(start + i as u64, out).await?;
        }
        Ok(())
    }

    /// Whole stripes are encoded and written straight to the disks (one
    /// write per disk, no read-modify-write); partial stripes at either end
    /// go through the stripe buffer.
    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        let d = DATA_SHARDS as u64;
        let mut lba = start;
        let mut rest = buf;

        while !lba.is_multiple_of(d) && !rest.is_empty() {
            let (block, tail) = rest.split_at(BLOCK_SIZE);
            self.write_block(lba, block.try_into().unwrap()).await?;
            rest = tail;
            lba += 1;
        }

        let full_blocks = (rest.len() / BLOCK_SIZE) as u64 / d * d;
        if full_blocks > 0 {
            let (full, tail) = rest.split_at(full_blocks as usize * BLOCK_SIZE);
            self.write_full_stripes(lba / d, full)
                .await
                .map_err(|e| DiskError::IoError(std::io::Error::other(format!("{}", e))))?;
            lba += full_blocks;
            rest = tail;
        }

        while !rest.is_empty() {
            let (block, tail) = rest.split_at(BLOCK_SIZE);
            self.write_block(lba, block.try_into().unwrap()).await?;
            rest = tail;
            lba += 1;
        }
        Ok(())
    }

    async fn reconstruct_candidates(
        &self,
        lba: BlockId,
    ) -> Result<crate::disk::Candidates, DiskError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let target = (lba % DATA_SHARDS as u64) as usize;
        let reads = (0..DISKS).filter(|&i| i != target).map(|i| async move {
            let mut buf = [0u8; BLOCK_SIZE];
            self.stripe
                .read_shards(i, stripe_id, &mut buf)
                .await
                .map(|_| (i, buf))
        });
        let mut shards = [[0u8; BLOCK_SIZE]; DISKS];
        let mut present = [false; DISKS];
        for (i, buf) in futures::future::join_all(reads).await.into_iter().flatten() {
            shards[i] = buf;
            present[i] = true;
        }

        let mut out = vec![None; DISKS];
        for (exclude, slot) in out.iter_mut().enumerate() {
            if exclude == target {
                continue;
            }
            let mut using = present;
            using[exclude] = false;
            if using.iter().filter(|&&p| p).count() < DATA_SHARDS {
                continue;
            }
            // Exactly DATA_SHARDS inputs, so the decoder can't mix in a bad one.
            let mut kept = 0;
            for u in using.iter_mut() {
                if *u {
                    kept += 1;
                    if kept > DATA_SHARDS {
                        *u = false;
                    }
                }
            }
            let mut rebuilt = [0u8; BLOCK_SIZE];
            if self
                .reconstruct_shard(&mut shards.clone(), &using, target, &mut rebuilt)
                .is_ok()
            {
                *slot = Some(rebuilt.to_vec());
            }
        }
        Ok(crate::disk::Candidates {
            disk: target,
            rebuilt: out,
        })
    }

    fn pool(&self) -> Option<&RaidZ> {
        Some(self)
    }

    async fn repair_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let data_index = (lba % DATA_SHARDS as u64) as usize;
        // A buffered (unflushed) stripe will be rewritten in full on flush anyway.
        if self.writes.contains_key(&stripe_id) {
            return Ok(());
        }
        // Parity already encodes this data, so only the data shard needs rewriting.
        self.stripe.write_shards(data_index, stripe_id, buf).await
    }
}

impl RaidZ {
    pub fn is_local_disk(&self, index: usize) -> bool {
        self.stripe.disks[index].is_local()
    }

    pub async fn new(stripe: Stripe) -> Self {
        RaidZ {
            stripe,
            writes: Arc::new(DashMap::new()),
            stripe_pool: Arc::new(ArrayQueue::new(100)),
        }
    }

    pub async fn with_pool_size(mut self, pool_size: usize) -> Self {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        for _ in 0..pool_size {
            let _ = pool.push(Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
        }
        self.stripe_pool = pool;
        self
    }

    /// Reconstruct a block from other disks in the stripe
    async fn reconstruct_from_stripe(
        &self,
        stripe_id: u64,
        data_index: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        let mut shard_buffers: [[u8; BLOCK_SIZE]; DISKS] = [[0u8; BLOCK_SIZE]; DISKS];
        let mut shard_present: [bool; DISKS] = [false; DISKS];

        // Read all disks except the failed one
        let mut futures = FuturesUnordered::new();
        for i in 0..DISKS {
            if i != data_index {
                futures.push(async move {
                    let mut temp_buf = [0u8; BLOCK_SIZE];
                    self.stripe
                        .read_shards(i, stripe_id, &mut temp_buf)
                        .await
                        .map(|_| (i, temp_buf))
                });
            }
        }

        let mut successful_reads = 0;
        let mut failed_count = 1;

        while let Some(result) = futures.next().await {
            match result {
                Ok((i, data)) => {
                    shard_buffers[i].copy_from_slice(&data);
                    shard_present[i] = true;
                    successful_reads += 1;

                    // We need DATA_SHARDS successful reads to reconstruct
                    if successful_reads >= DATA_SHARDS {
                        return self.reconstruct_shard(
                            &mut shard_buffers,
                            &shard_present,
                            data_index,
                            output,
                        );
                    }
                }
                Err(_) => {
                    failed_count += 1;
                    // Can tolerate up to PARITY_SHARDS (2) failures total
                    if failed_count > PARITY_SHARDS {
                        return Err(RaidZError::IncompleteStripe(stripe_id));
                    }
                }
            }
        }

        Err(RaidZError::IncompleteStripe(stripe_id))
    }

    /// Reconstruct a single shard using Reed-Solomon SIMD
    /// Encode and write `data` (a whole number of stripes) starting at
    /// `first_stripe`, issuing one contiguous write per disk.
    async fn write_full_stripes(&self, first_stripe: u64, data: &[u8]) -> Result<(), RaidZError> {
        let stripe_bytes = DATA_SHARDS * BLOCK_SIZE;
        debug_assert!(data.len().is_multiple_of(stripe_bytes));
        let stripes = data.len() / stripe_bytes;

        // These stripes are being replaced wholesale; drop any buffered
        // partial writes so a later flush can't resurrect stale data.
        for s in first_stripe..first_stripe + stripes as u64 {
            if let Some((_, buffered)) = self.writes.remove(&s)
                && let Some(b) = buffered.lock().await.take_buffer()
            {
                let _ = self.stripe_pool.push(b);
            }
        }

        let disk_bufs = Self::encode_stripes(data)?;

        let writes = disk_bufs
            .iter()
            .enumerate()
            .map(|(d, buf)| self.stripe.write_shards(d, first_stripe, buf));
        let results = futures::future::join_all(writes).await;
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures > 0 {
            eprintln!(
                "WARN: {} disk(s) failed writing stripes {}..{}",
                failures,
                first_stripe,
                first_stripe + stripes as u64
            );
        }
        if DISKS - failures >= DATA_SHARDS {
            Ok(())
        } else {
            Err(RaidZError::IncompleteStripe(first_stripe))
        }
    }

    fn reconstruct_shard(
        &self,
        shard_buffers: &mut [[u8; BLOCK_SIZE]; DISKS],
        shard_present: &[bool; DISKS],
        target_shard: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        let mut decoder = ReedSolomonDecoder::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;

        // Add available shards
        for (i, present) in shard_present.iter().enumerate() {
            if *present {
                if i < DATA_SHARDS {
                    decoder.add_original_shard(i, &shard_buffers[i])?;
                } else {
                    decoder.add_recovery_shard(i - DATA_SHARDS, &shard_buffers[i])?;
                }
            }
        }

        let result = decoder.decode()?;

        // Get the reconstructed shard
        if let Some(reconstructed) = result
            .restored_original_iter()
            .find(|(idx, _)| *idx == target_shard)
        {
            output.copy_from_slice(reconstructed.1);
            Ok(())
        } else {
            Err(RaidZError::DiskError(DiskError::IoError(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "target shard not in reconstruction result",
                ),
            )))
        }
    }

    async fn write_full_stripe(
        &self,
        stripe_buf: &StripeBuffer,
        stripe_id: u64,
    ) -> Result<(), RaidZError> {
        let data = stripe_buf
            .data
            .as_ref()
            .expect("Cannot write stripe without data");

        // Use stack-allocated array for all shards
        let mut shard_buffers: [[u8; BLOCK_SIZE]; DISKS] = [[0u8; BLOCK_SIZE]; DISKS];

        // Copy data shards to buffer
        for i in 0..DATA_SHARDS {
            let offset = i * BLOCK_SIZE;
            shard_buffers[i].copy_from_slice(&data[offset..offset + BLOCK_SIZE]);
        }

        // Create encoder and add original shards (zero-copy - just references)
        let mut encoder = ReedSolomonEncoder::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;

        for i in 0..DATA_SHARDS {
            encoder.add_original_shard(&shard_buffers[i])?;
        }

        // Encode to generate parity shards
        let result = encoder.encode()?;

        // Copy parity shards to our buffer
        for (idx, recovery_shard) in result.recovery_iter().enumerate() {
            shard_buffers[DATA_SHARDS + idx].copy_from_slice(recovery_shard);
        }

        // Write all shards concurrently
        let write_futures = shard_buffers
            .iter()
            .enumerate()
            .map(|(disk_idx, shard)| async move {
                match self.stripe.write_shards(disk_idx, stripe_id, shard).await {
                    Ok(()) => Ok(disk_idx),
                    Err(e) => Err((disk_idx, e)),
                }
            });

        let results = futures::future::join_all(write_futures).await;

        let mut successes = 0;
        let mut failures = Vec::new();

        for result in results {
            match result {
                Ok(_) => {
                    successes += 1;
                }
                Err((disk_idx, disk_error)) => {
                    eprintln!(
                        "WARN: Failed to write stripe {} to disk {}: {:?}",
                        stripe_id, disk_idx, disk_error
                    );
                    failures.push((disk_idx, disk_error));
                }
            }
        }

        // We need at least DATA_SHARDS (3) successful writes to be able to reconstruct
        if successes >= DATA_SHARDS {
            if !failures.is_empty() {
                eprintln!(
                    "Stripe {} written with {} successes and {} failures (recoverable)",
                    stripe_id,
                    successes,
                    failures.len()
                );
            }
            Ok(())
        } else {
            Err(RaidZError::IncompleteStripe(stripe_id))
        }
    }
}

/// What `verify_stripes` found (and, if asked, fixed) per disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StripeCheck {
    pub stripes: u64,
    /// Shards that didn't match what the verified data encodes to.
    pub mismatched: [u64; DISKS],
    /// Of those, rewritten successfully.
    pub repaired: [u64; DISKS],
    /// Disks that couldn't be read (offline), so weren't verified.
    pub unreadable: [bool; DISKS],
    /// Disks a repair write failed on.
    pub write_failed: [bool; DISKS],
    /// Stripes skipped because they have unflushed buffered writes.
    pub skipped: u64,
}

impl StripeCheck {
    pub fn add(&mut self, o: &StripeCheck) {
        self.stripes += o.stripes;
        self.skipped += o.skipped;
        for d in 0..DISKS {
            self.mismatched[d] += o.mismatched[d];
            self.repaired[d] += o.repaired[d];
            self.unreadable[d] |= o.unreadable[d];
            self.write_failed[d] |= o.write_failed[d];
        }
    }
}

/// Every way of picking exactly `DATA_SHARDS` of the disks.
fn shard_subsets() -> Vec<[bool; DISKS]> {
    (0u32..1 << DISKS)
        .filter(|m| m.count_ones() as usize == DATA_SHARDS)
        .map(|m| std::array::from_fn(|d| m & (1 << d) != 0))
        .collect()
}

impl RaidZ {
    /// Encode whole stripes of `data` into one buffer per disk.
    fn encode_stripes(data: &[u8]) -> Result<Vec<Vec<u8>>, RaidZError> {
        let stripe_bytes = DATA_SHARDS * BLOCK_SIZE;
        debug_assert!(data.len().is_multiple_of(stripe_bytes));
        let stripes = data.len() / stripe_bytes;
        let mut disk_bufs: Vec<Vec<u8>> = vec![vec![0u8; stripes * BLOCK_SIZE]; DISKS];
        let mut encoder = ReedSolomonEncoder::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;
        for (j, stripe) in data.chunks(stripe_bytes).enumerate() {
            let at = j * BLOCK_SIZE..(j + 1) * BLOCK_SIZE;
            if j > 0 {
                encoder.reset(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;
            }
            for (i, shard) in stripe.chunks(BLOCK_SIZE).enumerate() {
                disk_bufs[i][at.clone()].copy_from_slice(shard);
                encoder.add_original_shard(shard)?;
            }
            let encoded = encoder.encode()?;
            for (p, parity) in encoded.recovery_iter().enumerate() {
                disk_bufs[DATA_SHARDS + p][at.clone()].copy_from_slice(parity);
            }
        }
        Ok(disk_bufs)
    }

    /// Check the stripes starting at `first_stripe` against `data` (whole
    /// stripes of known-good contents, e.g. a record that passed its
    /// checksum): every shard on every disk, data and parity, must be what
    /// `data` encodes to. With `repair`, mismatching shards are rewritten.
    /// Stripes with buffered writes are skipped (they'll be rewritten whole).
    pub async fn verify_stripes(
        &self,
        first_stripe: u64,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let n = data.len() / (DATA_SHARDS * BLOCK_SIZE);
        let expected = Self::encode_stripes(data)?;
        let reads = (0..DISKS).map(|d| async move {
            let mut run = vec![0u8; n * BLOCK_SIZE];
            self.stripe
                .read_shards_raw(d, first_stripe, &mut run)
                .await
                .ok()
                .map(|_| run)
        });
        let actual = futures::future::join_all(reads).await;

        let mut check = StripeCheck {
            stripes: n as u64,
            ..Default::default()
        };
        let buffered: Vec<bool> = (0..n as u64)
            .map(|j| self.writes.contains_key(&(first_stripe + j)))
            .collect();
        check.skipped = buffered.iter().filter(|&&b| b).count() as u64;

        for d in 0..DISKS {
            let Some(run) = &actual[d] else {
                check.unreadable[d] = true;
                continue;
            };
            for (j, &busy) in buffered.iter().enumerate() {
                if busy {
                    continue;
                }
                let at = j * BLOCK_SIZE..(j + 1) * BLOCK_SIZE;
                if run[at.clone()] == expected[d][at.clone()] {
                    continue;
                }
                check.mismatched[d] += 1;
                if repair {
                    match self
                        .stripe
                        .write_shards(d, first_stripe + j as u64, &expected[d][at])
                        .await
                    {
                        Ok(()) => check.repaired[d] += 1,
                        Err(_) => check.write_failed[d] = true,
                    }
                }
            }
        }
        Ok(check)
    }

    /// Verify a stripe of self-describing label blocks (superblock,
    /// uberblocks, space map slots), which have no parent checksum. If the
    /// shards as read are parity-consistent they're accepted; otherwise every
    /// way of rebuilding the data from exactly `DATA_SHARDS` disks is scored
    /// with `score` (per data block: `Some(txg)` for a valid label) and the
    /// best (most valid labels, then newest) is taken as the truth and
    /// checked/repaired with `verify_stripes`. Returns None if no rebuild
    /// produced a single valid label.
    pub async fn verify_label_stripe(
        &self,
        stripe: u64,
        repair: bool,
        score: impl Fn(&[u8]) -> Option<u64>,
    ) -> Result<Option<StripeCheck>, RaidZError> {
        let mut shards = [[0u8; BLOCK_SIZE]; DISKS];
        let mut present = [false; DISKS];
        let reads = (0..DISKS).map(|d| async move {
            let mut b = [0u8; BLOCK_SIZE];
            self.stripe
                .read_shards_raw(d, stripe, &mut b)
                .await
                .ok()
                .map(|_| b)
        });
        for (d, r) in futures::future::join_all(reads).await.into_iter().enumerate() {
            if let Some(b) = r {
                shards[d] = b;
                present[d] = true;
            }
        }

        let data_of = |use_: &[bool; DISKS]| -> Option<Vec<u8>> {
            let mut out = vec![0u8; DATA_SHARDS * BLOCK_SIZE];
            for i in 0..DATA_SHARDS {
                let slot = &mut out[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
                if use_[i] {
                    slot.copy_from_slice(&shards[i]);
                } else {
                    self.reconstruct_shard(&mut shards.clone(), use_, i, slot)
                        .ok()?;
                }
            }
            Some(out)
        };

        // Consistent as read (and fully readable): nothing to decide.
        if present.iter().all(|&p| p) {
            let mut data_only = [false; DISKS];
            data_only[..DATA_SHARDS].fill(true);
            if let Some(data) = data_of(&data_only) {
                let enc = Self::encode_stripes(&data)?;
                if (DATA_SHARDS..DISKS).all(|d| enc[d][..] == shards[d][..]) {
                    return Ok(Some(StripeCheck {
                        stripes: 1,
                        ..Default::default()
                    }));
                }
            }
        }

        // Rank the candidates. Every block of a label stripe is written in the
        // same TXG (the three uberblock copies, the superblock mirrors, the
        // blocks of one space map slot), so the truth is a full set of valid
        // labels sharing one TXG. Stale shards mixed with current ones can
        // decode into *valid* labels of other versions, or into rebuilds that
        // happen to re-encode consistently with several disks, so neither
        // validity nor agreement alone decides. Then prefer the candidate the
        // most disks already hold, then the newest.
        // (valid labels, one TXG, agreeing disks, TXG sum)
        type Rank = (usize, bool, usize, u64);
        let mut best: Option<(Rank, Vec<u8>)> = None;
        for use_ in shard_subsets() {
            if (0..DISKS).any(|d| use_[d] && !present[d]) {
                continue;
            }
            let Some(data) = data_of(&use_) else { continue };
            let enc = Self::encode_stripes(&data)?;
            let agree = (0..DISKS)
                .filter(|&d| present[d] && enc[d][..] == shards[d][..])
                .count();
            let txgs: Vec<u64> = data.chunks(BLOCK_SIZE).filter_map(&score).collect();
            let uniform = txgs.len() == DATA_SHARDS && txgs.iter().all(|&t| t == txgs[0]);
            let rank = (txgs.len(), uniform, agree, txgs.iter().sum());
            if !txgs.is_empty() && best.as_ref().is_none_or(|(r, _)| rank > *r) {
                best = Some((rank, data));
            }
        }
        match best {
            Some((_, data)) => Ok(Some(self.verify_stripes(stripe, &data, repair).await?)),
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct StripeBuffer {
    data: Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>,
    shard_mask: u8,
    dirty: bool,
    flushed: bool, // For optimistic writes
}

impl StripeBuffer {
    pub fn new() -> Self {
        StripeBuffer {
            data: None,
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }

    pub fn with_buffer(mut data: Box<[u8; BLOCK_SIZE * DATA_SHARDS]>) -> Self {
        data.fill(0);
        StripeBuffer {
            data: Some(data),
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }

    pub fn set_shard(&mut self, shard_idx: usize, block_data: &[u8; BLOCK_SIZE]) {
        assert!(shard_idx < DATA_SHARDS);

        let data_ptr = self
            .data
            .get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));

        let offset = shard_idx * BLOCK_SIZE;
        data_ptr[offset..offset + BLOCK_SIZE].copy_from_slice(block_data);

        self.shard_mask |= 1 << shard_idx;
        self.dirty = true;
        self.flushed = false;
    }

    pub fn has_shard(&self, shard_idx: usize) -> bool {
        (self.shard_mask & (1 << shard_idx)) != 0
    }

    pub fn is_complete(&self) -> bool {
        self.shard_mask == 0b111 // All 3 data shards present
    }

    pub fn get_shard(&self, shard_idx: usize, buf: &mut [u8]) -> bool {
        match &self.data {
            Some(d) if self.has_shard(shard_idx) => {
                let offset = shard_idx * BLOCK_SIZE;
                buf.copy_from_slice(&d[offset..offset + BLOCK_SIZE]);
                true
            }
            _ => false,
        }
    }

    pub fn take_buffer(&mut self) -> Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>> {
        self.data.take()
    }
}

#[cfg(test)]
#[path = "tests/raidz_tests.rs"]
mod tests;
