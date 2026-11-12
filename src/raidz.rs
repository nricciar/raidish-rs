use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, Candidates, DiskError};
use crate::disks::DiskSet;
use crate::vdev::{LabelScan, LabelScore, StripeCheck, Vdev};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

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

fn io_err(e: impl fmt::Display) -> DiskError {
    DiskError::IoError(std::io::Error::other(format!("{}", e)))
}

type Shard = [u8; BLOCK_SIZE];

/// A raidz group: `data` data shards plus `parity` Reed-Solomon parity
/// shards per stripe, over `members` (indices into the pool's `DiskSet`).
/// Shard position `i` of every stripe lives on disk `members[i]`; positions
/// below `data` hold data, the rest parity. Stripe `s` is shard `s` of each
/// member, and logical block `lba` is data shard `lba % data` of stripe
/// `lba / data`.
#[derive(Debug)]
pub struct RaidZ {
    pub disks: Arc<DiskSet>,
    members: Vec<usize>,
    data: usize,
    parity: usize,
    writes: Arc<DashMap<u64, Arc<Mutex<StripeBuffer>>>>,
    stripe_pool: Arc<ArrayQueue<Box<[u8]>>>,
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
                let missing_indices: Vec<usize> = (0..self.data)
                    .filter(|&i| !stripe_buf.has_shard(i))
                    .collect();
                stripe_buf.ensure_data();

                let mut read_tasks = FuturesUnordered::new();
                for i in missing_indices {
                    read_tasks.push(async move {
                        let mut buf = [0u8; BLOCK_SIZE];
                        // A missing disk shouldn't block the write: rebuild
                        // the shard from the rest of the stripe instead.
                        if self.read_shard(i, stripe_id, &mut buf).await.is_err() {
                            self.reconstruct_from_stripe(stripe_id, i, &mut buf)
                                .await
                                .map_err(io_err)?;
                        }
                        Ok::<_, DiskError>((i, buf))
                    });
                }

                while let Some(result) = read_tasks.next().await {
                    let (shard_idx, buf) = result?;
                    stripe_buf.fill_shard(shard_idx, &buf);
                }
            }

            // Now stripe is complete, write it. On failure the stripe stays
            // buffered, so a later flush retries it.
            let data = stripe_buf.data.as_ref().expect("stripe without data");
            self.write_encoded(stripe_id, data).await.map_err(io_err)?;

            if let Some(buffer) = stripe_buf.take_buffer() {
                let _ = self.stripe_pool.push(buffer);
            }

            drop(stripe_buf);
            self.writes.remove(&stripe_id);
        }

        // Like stripe writes, flushing tolerates up to `parity` failed
        // disks: the data is still recoverable from the rest.
        let flushes = self.members.iter().map(|&d| self.disks.flush_disk(d));
        let results = futures::future::join_all(flushes).await;
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures > self.parity {
            return results.into_iter().find(Result::is_err).unwrap();
        }

        Ok(())
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let stripe = lba / self.data as u64;
        let data_index = (lba % self.data as u64) as usize;

        if let Some(stripe_buf_ref) = self.writes.get(&stripe) {
            let stripe_buf = stripe_buf_ref.lock().await;
            if stripe_buf.has_shard(data_index) && stripe_buf.get_shard(data_index, buf) {
                return Ok(());
            }
        }

        match self
            .read_shard(data_index, stripe, &mut buf[..BLOCK_SIZE])
            .await
        {
            Ok(()) => Ok(()),
            // Primary disk failed, reconstruct
            Err(_) => self
                .reconstruct_from_stripe(stripe, data_index, buf)
                .await
                .map_err(|e| {
                    DiskError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{}", e),
                    ))
                }),
        }
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / self.data as u64;
        let shard_idx = (lba % self.data as u64) as usize;

        // Get or create stripe buffer
        let stripe = if let Some(stripe_ref) = self.writes.get(&stripe_id) {
            stripe_ref.value().clone()
        } else {
            let new_stripe = Arc::new(Mutex::new(match self.stripe_pool.pop() {
                Some(b) => StripeBuffer::with_buffer(b, self.data),
                None => StripeBuffer::new(self.data),
            }));

            // Insert into map using Entry API to handle races safely
            match self.writes.entry(stripe_id) {
                dashmap::mapref::entry::Entry::Occupied(o) => {
                    // We lost the race; another thread inserted the stripe
                    // while we were getting the buffer. Return ours to the pool.
                    let mut guard = new_stripe.try_lock().expect("Exclusive ownership expected");
                    if let Some(buf) = guard.take_buffer() {
                        let _ = self.stripe_pool.push(buf);
                    }
                    o.get().clone()
                }
                dashmap::mapref::entry::Entry::Vacant(v) => {
                    v.insert(new_stripe.clone());
                    new_stripe
                }
            }
        };

        stripe.lock().await.set_shard(shard_idx, buf);
        Ok(())
    }

    /// Logical capacity: every stripe holds `data` data blocks.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        Ok(self.disks.shard_capacity(&self.members).await? * self.data as u64)
    }

    /// Contiguous reads become one read per data disk. Stripes with
    /// unflushed writes (or a failed disk) fall back to per-block reads.
    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let n = (buf.len() / BLOCK_SIZE) as u64;
        if n == 0 {
            return Ok(());
        }
        let d = self.data as u64;
        let (s_first, s_last) = (start / d, (start + n - 1) / d);

        let buffered = (s_first..=s_last).any(|s| self.writes.contains_key(&s));
        if n > 1 && !buffered {
            let count = (s_last - s_first + 1) as usize;
            let runs = (0..self.data).map(|i| async move {
                let mut run = vec![0u8; count * BLOCK_SIZE];
                self.read_shard(i, s_first, &mut run).await.map(|_| run)
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
        let d = self.data as u64;
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
                .map_err(io_err)?;
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

    async fn reconstruct_candidates(&self, lba: BlockId) -> Result<Candidates, DiskError> {
        let n = self.members.len();
        let stripe_id = lba / self.data as u64;
        let target = (lba % self.data as u64) as usize;
        let reads = (0..n).filter(|&i| i != target).map(|i| async move {
            let mut buf = [0u8; BLOCK_SIZE];
            self.read_shard(i, stripe_id, &mut buf)
                .await
                .map(|_| (i, buf))
        });
        let mut shards = vec![[0u8; BLOCK_SIZE]; n];
        let mut present = vec![false; n];
        for (i, buf) in futures::future::join_all(reads).await.into_iter().flatten() {
            shards[i] = buf;
            present[i] = true;
        }

        // Several Reed-Solomon decodes per block: off the async thread, so a
        // self-heal of a whole record can't stall other tasks (disk
        // keepalives, servers) for hundreds of milliseconds.
        let (data, parity) = (self.data, self.parity);
        let rebuilds = tokio::task::spawn_blocking(move || {
            // (excluded shard position, or `target` for "only itself", rebuild)
            let mut out: Vec<(usize, Vec<u8>)> = Vec::new();
            for exclude in 0..n {
                let mut using = present.clone();
                using[exclude] = false;
                if using.iter().filter(|&&p| p).count() < data {
                    continue;
                }
                // Exactly `data` inputs, so the decoder can't mix in a bad one.
                let mut kept = 0;
                for u in using.iter_mut().filter(|u| **u) {
                    kept += 1;
                    *u = kept <= data;
                }
                let mut rebuilt = [0u8; BLOCK_SIZE];
                if decode_shard(data, parity, &shards, &using, target, &mut rebuilt).is_ok() {
                    out.push((exclude, rebuilt.to_vec()));
                }
            }
            out
        })
        .await
        .map_err(io_err)?;

        let mut out = vec![None; self.disks.len()];
        for (exclude, rebuilt) in rebuilds {
            out[self.members[exclude]] = Some(rebuilt);
        }
        // A bad disk in another vdev of the pool doesn't constrain how this
        // block is rebuilt.
        if let Some(any) = out.iter().flatten().next().cloned() {
            for (d, slot) in out.iter_mut().enumerate() {
                if !self.members.contains(&d) && d != self.members[target] {
                    *slot = Some(any.clone());
                }
            }
        }
        Ok(Candidates {
            disk: self.members[target],
            rebuilt: out,
        })
    }

    async fn repair_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / self.data as u64;
        let data_index = (lba % self.data as u64) as usize;
        // A buffered (unflushed) stripe will be rewritten in full on flush anyway.
        if self.writes.contains_key(&stripe_id) {
            return Ok(());
        }
        // Parity already encodes this data, so only the data shard needs rewriting.
        self.disks
            .write_shards(self.members[data_index], stripe_id, buf)
            .await
    }
}

impl RaidZ {
    pub fn new(disks: Arc<DiskSet>, members: Vec<usize>, parity: usize) -> Self {
        assert!(parity >= 1 && members.len() > parity, "invalid raidz shape");
        RaidZ {
            data: members.len() - parity,
            parity,
            disks,
            members,
            writes: Arc::new(DashMap::new()),
            stripe_pool: Arc::new(ArrayQueue::new(100)),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_local_disk(&self, index: usize) -> bool {
        self.disks.is_local(self.members[index])
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        for _ in 0..pool_size {
            let _ = pool.push(vec![0u8; BLOCK_SIZE * self.data].into_boxed_slice());
        }
        self.stripe_pool = pool;
        self
    }

    /// Read shard position `i` of stripes starting at `stripe`.
    async fn read_shard(&self, i: usize, stripe: u64, buf: &mut [u8]) -> Result<(), DiskError> {
        self.disks.read_shards(self.members[i], stripe, buf).await
    }

    /// Reconstruct a block from other disks in the stripe
    async fn reconstruct_from_stripe(
        &self,
        stripe_id: u64,
        data_index: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        let n = self.members.len();
        let mut shard_buffers = vec![[0u8; BLOCK_SIZE]; n];
        let mut shard_present = vec![false; n];

        // Read all disks except the failed one
        let mut futures = FuturesUnordered::new();
        for i in (0..n).filter(|&i| i != data_index) {
            futures.push(async move {
                let mut temp_buf = [0u8; BLOCK_SIZE];
                self.read_shard(i, stripe_id, &mut temp_buf)
                    .await
                    .map(|_| (i, temp_buf))
            });
        }

        let mut successful_reads = 0;
        let mut failed_count = 1;

        while let Some(result) = futures.next().await {
            match result {
                Ok((i, data)) => {
                    shard_buffers[i] = data;
                    shard_present[i] = true;
                    successful_reads += 1;

                    // We need `data` successful reads to reconstruct
                    if successful_reads >= self.data {
                        return self.reconstruct_shard(
                            &shard_buffers,
                            &shard_present,
                            data_index,
                            output,
                        );
                    }
                }
                Err(_) => {
                    failed_count += 1;
                    if failed_count > self.parity {
                        return Err(RaidZError::IncompleteStripe(stripe_id));
                    }
                }
            }
        }

        Err(RaidZError::IncompleteStripe(stripe_id))
    }

    /// Encode and write `data` (a whole number of stripes) starting at
    /// `first_stripe`, issuing one contiguous write per disk.
    async fn write_full_stripes(&self, first_stripe: u64, data: &[u8]) -> Result<(), RaidZError> {
        let stripes = data.len() / (self.data * BLOCK_SIZE);

        // These stripes are being replaced wholesale; drop any buffered
        // partial writes so a later flush can't resurrect stale data.
        for s in first_stripe..first_stripe + stripes as u64 {
            if let Some((_, buffered)) = self.writes.remove(&s)
                && let Some(b) = buffered.lock().await.take_buffer()
            {
                let _ = self.stripe_pool.push(b);
            }
        }

        self.write_encoded(first_stripe, data).await
    }

    /// Encode whole stripes of `data` and write every shard. Succeeds if
    /// enough disks took the write for the data to be recoverable.
    async fn write_encoded(&self, first_stripe: u64, data: &[u8]) -> Result<(), RaidZError> {
        let stripes = data.len() / (self.data * BLOCK_SIZE);
        let disk_bufs = self.encode_stripes(data)?;

        let writes = disk_bufs
            .iter()
            .enumerate()
            .map(|(i, buf)| self.disks.write_shards(self.members[i], first_stripe, buf));
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
        if self.members.len() - failures >= self.data {
            Ok(())
        } else {
            Err(RaidZError::IncompleteStripe(first_stripe))
        }
    }

    /// Reconstruct data shard `target_shard` from the `present` shards.
    fn reconstruct_shard(
        &self,
        shard_buffers: &[Shard],
        shard_present: &[bool],
        target_shard: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        decode_shard(
            self.data,
            self.parity,
            shard_buffers,
            shard_present,
            target_shard,
            output,
        )
    }

    /// Encode whole stripes of `data` into one buffer per shard position.
    fn encode_stripes(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, RaidZError> {
        encode_stripes(self.data, self.parity, data)
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
        let n = data.len() / (self.data * BLOCK_SIZE);
        let expected = self.encode_stripes(data)?;
        let reads = self.members.iter().map(|&d| async move {
            let mut run = vec![0u8; n * BLOCK_SIZE];
            self.disks
                .read_shards_raw(d, first_stripe, &mut run)
                .await
                .ok()
                .map(|_| run)
        });
        let actual = futures::future::join_all(reads).await;

        let mut check = StripeCheck::new(self.disks.len(), n as u64);
        let buffered: Vec<bool> = (0..n as u64)
            .map(|j| self.writes.contains_key(&(first_stripe + j)))
            .collect();
        check.skipped = buffered.iter().filter(|&&b| b).count() as u64;

        for (i, &d) in self.members.iter().enumerate() {
            let Some(run) = &actual[i] else {
                check.unreadable[d] = true;
                continue;
            };
            for (j, &busy) in buffered.iter().enumerate() {
                if busy {
                    continue;
                }
                let at = j * BLOCK_SIZE..(j + 1) * BLOCK_SIZE;
                if run[at.clone()] == expected[i][at.clone()] {
                    continue;
                }
                check.mismatched[d] += 1;
                if repair {
                    match self
                        .disks
                        .write_shards(d, first_stripe + j as u64, &expected[i][at])
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
    /// way of rebuilding the data from exactly `data` disks is scored
    /// with `score` (per data block: `Some(txg)` for a valid label) and the
    /// best (most valid labels, then newest) is taken as the truth and
    /// checked/repaired with `verify_stripes`. Returns None if no rebuild
    /// produced a single valid label.
    pub async fn verify_label_stripe(
        &self,
        stripe: u64,
        repair: bool,
        score: LabelScore<'_>,
    ) -> Result<Option<StripeCheck>, RaidZError> {
        let n = self.members.len();
        let mut shards = vec![[0u8; BLOCK_SIZE]; n];
        let mut present = vec![false; n];
        let reads = self.members.iter().map(|&d| async move {
            let mut b = [0u8; BLOCK_SIZE];
            self.disks
                .read_shards_raw(d, stripe, &mut b)
                .await
                .ok()
                .map(|_| b)
        });
        for (i, r) in futures::future::join_all(reads)
            .await
            .into_iter()
            .enumerate()
        {
            if let Some(b) = r {
                shards[i] = b;
                present[i] = true;
            }
        }

        // Decoding and re-encoding every subset of the shards is the heavy
        // part: off the async thread.
        let (data, parity) = (self.data, self.parity);
        let candidates =
            tokio::task::spawn_blocking(move || label_candidates(data, parity, &shards, &present))
                .await
                .map_err(|e| RaidZError::DiskError(io_err(e)))??;
        let Some(candidates) = candidates else {
            // Consistent as read (and fully readable): nothing to decide.
            return Ok(Some(StripeCheck::new(self.disks.len(), 1)));
        };

        // Rank the candidates. Every block of a label stripe is written in the
        // same TXG (the uberblock copies, the superblock mirrors, the
        // blocks of one space map slot), so the truth is a full set of valid
        // labels sharing one TXG. Stale shards mixed with current ones can
        // decode into *valid* labels of other versions, or into rebuilds that
        // happen to re-encode consistently with several disks, so neither
        // validity nor agreement alone decides. Then prefer the candidate the
        // most disks already hold, then the newest.
        // (valid labels, one TXG, agreeing disks, TXG sum)
        type Rank = (usize, bool, usize, u64);
        let mut best: Option<(Rank, Vec<u8>)> = None;
        for (data, agree) in candidates {
            let txgs: Vec<u64> = data.chunks(BLOCK_SIZE).filter_map(score).collect();
            let uniform = txgs.len() == self.data && txgs.iter().all(|&t| t == txgs[0]);
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

    /// Never written: every readable shard is zeros, and enough are readable
    /// to be sure.
    async fn label_stripe_is_blank(&self, stripe: u64) -> bool {
        let mut readable = 0;
        for &d in &self.members {
            let mut b = vec![0u8; BLOCK_SIZE];
            if self.disks.read_shards_raw(d, stripe, &mut b).await.is_ok() {
                readable += 1;
                if b.iter().any(|&x| x != 0) {
                    return false;
                }
            }
        }
        readable >= self.data
    }
}

#[async_trait]
impl Vdev for RaidZ {
    fn width(&self) -> u64 {
        self.data as u64
    }

    async fn verify_blocks(
        &self,
        lba: BlockId,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let d = self.data as u64;
        debug_assert!(lba.is_multiple_of(d));
        debug_assert!(data.len().is_multiple_of(self.data * BLOCK_SIZE));
        self.verify_stripes(lba / d, data, repair).await
    }

    async fn verify_labels(
        &self,
        lba: BlockId,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
    ) -> Result<LabelScan, RaidZError> {
        let d = self.data as u64;
        let mut scan = LabelScan {
            check: StripeCheck::new(self.disks.len(), 0),
            ..Default::default()
        };
        for stripe in lba / d..(lba + blocks).div_ceil(d) {
            scan.units += 1;
            match self.verify_label_stripe(stripe, repair, score).await? {
                Some(check) => scan.check.add(&check),
                None => {
                    // Never-written space map slots are all zeros and fine.
                    if !self.label_stripe_is_blank(stripe).await {
                        scan.unrecoverable.push(stripe * d);
                    }
                }
            }
        }
        Ok(scan)
    }
}

/// Encode whole stripes of `data` into one buffer per shard position.
fn encode_stripes(data: usize, parity: usize, bytes: &[u8]) -> Result<Vec<Vec<u8>>, RaidZError> {
    let stripe_bytes = data * BLOCK_SIZE;
    debug_assert!(bytes.len().is_multiple_of(stripe_bytes));
    let stripes = bytes.len() / stripe_bytes;
    let mut disk_bufs: Vec<Vec<u8>> = vec![vec![0u8; stripes * BLOCK_SIZE]; data + parity];
    let mut encoder = ReedSolomonEncoder::new(data, parity, BLOCK_SIZE)?;
    for (j, stripe) in bytes.chunks(stripe_bytes).enumerate() {
        let at = j * BLOCK_SIZE..(j + 1) * BLOCK_SIZE;
        if j > 0 {
            encoder.reset(data, parity, BLOCK_SIZE)?;
        }
        for (i, shard) in stripe.chunks(BLOCK_SIZE).enumerate() {
            disk_bufs[i][at.clone()].copy_from_slice(shard);
            encoder.add_original_shard(shard)?;
        }
        let encoded = encoder.encode()?;
        for (p, recovery) in encoded.recovery_iter().enumerate() {
            disk_bufs[data + p][at.clone()].copy_from_slice(recovery);
        }
    }
    Ok(disk_bufs)
}

/// A label stripe's data as rebuilt one way, and how many disks agree with it.
type LabelCandidate = (Vec<u8>, usize);

/// Every way of rebuilding a label stripe's data from exactly `data` of its
/// readable shards, each with how many disks already hold what it encodes
/// to. None if the shards as read are complete and parity-consistent.
fn label_candidates(
    data: usize,
    parity: usize,
    shards: &[Shard],
    present: &[bool],
) -> Result<Option<Vec<LabelCandidate>>, RaidZError> {
    let n = data + parity;
    let data_of = |use_: &[bool]| -> Option<Vec<u8>> {
        let mut out = vec![0u8; data * BLOCK_SIZE];
        for i in 0..data {
            let slot = &mut out[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
            if use_[i] {
                slot.copy_from_slice(&shards[i]);
            } else {
                decode_shard(data, parity, shards, use_, i, slot).ok()?;
            }
        }
        Some(out)
    };

    if present.iter().all(|&p| p) {
        let mut data_only = vec![false; n];
        data_only[..data].fill(true);
        if let Some(d) = data_of(&data_only) {
            let enc = encode_stripes(data, parity, &d)?;
            if (data..n).all(|i| enc[i][..] == shards[i][..]) {
                return Ok(None);
            }
        }
    }

    let mut out = Vec::new();
    // Every way of picking exactly `data` of the shards.
    for m in (0u64..1 << n).filter(|m| m.count_ones() as usize == data) {
        let use_: Vec<bool> = (0..n).map(|d| m & (1 << d) != 0).collect();
        if (0..n).any(|i| use_[i] && !present[i]) {
            continue;
        }
        let Some(d) = data_of(&use_) else { continue };
        let enc = encode_stripes(data, parity, &d)?;
        let agree = (0..n)
            .filter(|&i| present[i] && enc[i][..] == shards[i][..])
            .count();
        out.push((d, agree));
    }
    Ok(Some(out))
}

/// Reconstruct data shard `target_shard` of a `data` + `parity` stripe from
/// the `present` shards (shard position order: data, then parity).
fn decode_shard(
    data: usize,
    parity: usize,
    shard_buffers: &[Shard],
    shard_present: &[bool],
    target_shard: usize,
    output: &mut [u8],
) -> Result<(), RaidZError> {
    let mut decoder = ReedSolomonDecoder::new(data, parity, BLOCK_SIZE)?;

    for (i, present) in shard_present.iter().enumerate() {
        if *present {
            if i < data {
                decoder.add_original_shard(i, shard_buffers[i])?;
            } else {
                decoder.add_recovery_shard(i - data, shard_buffers[i])?;
            }
        }
    }

    let result = decoder.decode()?;

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

/// Partial writes to one stripe, held until the stripe is complete (or a
/// flush fills the rest in from disk).
#[derive(Debug)]
pub struct StripeBuffer {
    data: Option<Box<[u8]>>,
    width: usize,
    shard_mask: u32,
    dirty: bool,
    flushed: bool, // For optimistic writes
}

impl StripeBuffer {
    pub fn new(width: usize) -> Self {
        assert!(width <= 32);
        StripeBuffer {
            data: None,
            width,
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }

    pub fn with_buffer(mut data: Box<[u8]>, width: usize) -> Self {
        assert_eq!(data.len(), width * BLOCK_SIZE);
        data.fill(0);
        StripeBuffer {
            data: Some(data),
            ..Self::new(width)
        }
    }

    fn ensure_data(&mut self) -> &mut [u8] {
        let width = self.width;
        self.data
            .get_or_insert_with(|| vec![0u8; BLOCK_SIZE * width].into_boxed_slice())
    }

    /// Fill in a shard read back from disk (doesn't make the stripe dirty).
    fn fill_shard(&mut self, shard_idx: usize, block_data: &[u8]) {
        let offset = shard_idx * BLOCK_SIZE;
        self.ensure_data()[offset..offset + BLOCK_SIZE].copy_from_slice(block_data);
        self.shard_mask |= 1 << shard_idx;
    }

    pub fn set_shard(&mut self, shard_idx: usize, block_data: &[u8; BLOCK_SIZE]) {
        assert!(shard_idx < self.width);
        self.fill_shard(shard_idx, block_data);
        self.dirty = true;
        self.flushed = false;
    }

    pub fn has_shard(&self, shard_idx: usize) -> bool {
        (self.shard_mask & (1 << shard_idx)) != 0
    }

    pub fn is_complete(&self) -> bool {
        self.shard_mask == ((1u64 << self.width) - 1) as u32
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

    pub fn take_buffer(&mut self) -> Option<Box<[u8]>> {
        self.data.take()
    }
}

#[cfg(test)]
#[path = "tests/raidz_tests.rs"]
mod tests;
