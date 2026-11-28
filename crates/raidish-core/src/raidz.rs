use crate::coords::{MemberIdx, PoolDiskIdx, StripeId, VdevLba};
use crate::disk::{BLOCK_SIZE, Block, Candidates, DiskError, zeroed_blocks};
use crate::disks::{DiskSet, WriteAcks, fenced_any, ok_mask, read_or_fenced};
use crate::vdev::{LabelScan, LabelScore, StripeCheck, Vdev};
use async_trait::async_trait;
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
    IncompleteStripe(StripeId),
    InconsistentStripe(StripeId),
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

/// A stripe write's error as the disk error it reports: a member's own
/// error kept as is (a fencing refusal must stay one, see `fenced_any`).
fn write_err(e: RaidZError) -> DiskError {
    match e {
        RaidZError::DiskError(e) => e,
        e => io_err(e),
    }
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
    members: Vec<PoolDiskIdx>,
    data: usize,
    parity: usize,
    writes: Arc<DashMap<StripeId, Arc<Mutex<StripeBuffer>>>>,
    stripe_pool: Arc<ArrayQueue<Box<[u8]>>>,
    /// A stripe is durable once `data` of the disks that took it flush.
    acks: WriteAcks,
}

#[async_trait]
impl Vdev for RaidZ {
    fn width(&self) -> u64 {
        self.data as u64
    }

    async fn flush(&self) -> Result<(), DiskError> {
        let stripe_ids: Vec<StripeId> = self.writes.iter().map(|r| *r.key()).collect();

        for stripe_id in stripe_ids {
            let stripe_buf_arc = match self.writes.get(&stripe_id) {
                Some(entry) => entry.value().clone(),
                None => continue,
            };

            let mut stripe_buf = stripe_buf_arc.lock().await;

            // Already written by a concurrent flush (or replaced by a full
            // stripe write) while we waited for it.
            if stripe_buf.retired || (stripe_buf.flushed && !stripe_buf.dirty) {
                continue;
            }
            // Nothing was written into it (a full-stripe write's, left by
            // its cancellation): nothing to write.
            if !stripe_buf.dirty {
                stripe_buf.retired = true;
                drop(stripe_buf);
                self.writes
                    .remove_if(&stripe_id, |_, v| Arc::ptr_eq(v, &stripe_buf_arc));
                continue;
            }

            if !stripe_buf.is_complete() {
                let missing: Vec<usize> = (0..self.data)
                    .filter(|&i| !stripe_buf.has_shard(i))
                    .collect();
                stripe_buf.ensure_data();
                for (i, buf) in self
                    .rmw_shards(stripe_id, &missing)
                    .await
                    .map_err(write_err)?
                {
                    stripe_buf.fill_shard(i, &buf);
                }
            }

            // Now stripe is complete, write it. On failure the stripe stays
            // buffered, so a later flush retries it.
            let data = stripe_buf.data.as_ref().expect("stripe without data");
            self.write_encoded(stripe_id, data)
                .await
                .map_err(write_err)?;

            // Retire it: a write that picked this buffer up before it
            // leaves the map starts a new one instead of writing into this
            // (whose contents are now on disk and never flushed again).
            stripe_buf.retired = true;
            if let Some(buffer) = stripe_buf.take_buffer() {
                let _ = self.stripe_pool.push(buffer);
            }

            drop(stripe_buf);
            self.writes
                .remove_if(&stripe_id, |_, v| Arc::ptr_eq(v, &stripe_buf_arc));
        }

        // Then every stripe written since the last flush (here or by a
        // full-stripe write) must be durable on `data` of its disks.
        self.acks
            .flush(|| async {
                // Like stripe writes, flushing tolerates up to `parity` failed
                // disks: the data is still recoverable from the rest.
                let traced = crate::trace::enabled();
                let flushes = self.members.iter().map(|&d| async move {
                    #[cfg(not(target_arch = "wasm32"))]
                    if traced {
                        let t = std::time::Instant::now();
                        let r = self.disks.flush_disk(d).await;
                        eprintln!(
                            "trace disk.flush d{} {:.2}ms",
                            d,
                            t.elapsed().as_secs_f64() * 1e3
                        );
                        return r;
                    }
                    let _ = traced;
                    self.disks.flush_disk(d).await
                });
                let results = futures::future::join_all(flushes).await;
                if let Some(e) = fenced_any(&results) {
                    return Err(e);
                }
                let failures = results.iter().filter(|r| r.is_err()).count();
                if failures > self.parity {
                    return results
                        .into_iter()
                        .find(Result::is_err)
                        .unwrap()
                        .map(|_| Default::default());
                }
                Ok(ok_mask(&self.members, &results))
            })
            .await
    }

    async fn read_block(&self, lba: VdevLba, buf: &mut Block) -> Result<(), DiskError> {
        let (stripe, data_index) = lba.stripe_of(self.data);

        // Clone the buffer out and let go of the map's shard lock before
        // waiting on it: held across the await, every insert or removal on
        // that shard would block its thread while a flush holds the buffer
        // across disk I/O.
        let pending = self.writes.get(&stripe).map(|r| r.value().clone());
        if let Some(stripe_buf) = pending {
            let stripe_buf = stripe_buf.lock().await;
            if stripe_buf.has_shard(data_index) && stripe_buf.get_shard(data_index, buf) {
                return Ok(());
            }
        }

        match self
            .read_shard(MemberIdx(data_index), stripe, std::slice::from_mut(buf))
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

    async fn write_block(&self, lba: VdevLba, buf: &Block) -> Result<(), DiskError> {
        let (stripe_id, shard_idx) = lba.stripe_of(self.data);

        loop {
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
                        let mut guard =
                            new_stripe.try_lock().expect("Exclusive ownership expected");
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

            let mut guard = stripe.lock().await;
            if guard.retired {
                // Flushed (or replaced) since we found it: start a new one.
                drop(guard);
                self.writes
                    .remove_if(&stripe_id, |_, v| Arc::ptr_eq(v, &stripe));
                continue;
            }
            guard.set_shard(shard_idx, buf);
            return Ok(());
        }
    }

    /// Logical capacity: every stripe holds `data` data blocks.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        Ok(self.disks.shard_capacity(&self.members).await? * self.data as u64)
    }

    /// Contiguous reads become one read per data disk. Stripes with
    /// unflushed writes (or a failed disk) fall back to per-block reads.
    async fn read_blocks(&self, start: VdevLba, buf: &mut [Block]) -> Result<(), DiskError> {
        let n = buf.len() as u64;
        if n == 0 {
            return Ok(());
        }
        let (s_first, _) = start.stripe_of(self.data);
        let (s_last, _) = (start + (n - 1)).stripe_of(self.data);

        let buffered =
            (s_first.get()..=s_last.get()).any(|s| self.writes.contains_key(&StripeId::new(s)));
        if n > 1 && !buffered {
            let count = (s_last - s_first + 1) as usize;
            let runs = (0..self.data).map(|i| async move {
                let mut run = zeroed_blocks(count);
                self.read_shard(MemberIdx(i), s_first, &mut run)
                    .await
                    .map(|_| run)
            });
            if let Ok(runs) = futures::future::try_join_all(runs).await {
                for (i, out) in buf.iter_mut().enumerate() {
                    let (s, at) = (start + i as u64).stripe_of(self.data);
                    *out = runs[at][(s - s_first) as usize];
                }
                return Ok(());
            }
        }

        for (i, out) in buf.iter_mut().enumerate() {
            self.read_block(start + i as u64, out).await?;
        }
        Ok(())
    }

    /// Whole stripes are encoded and written straight to the disks (one
    /// write per disk, no read-modify-write); partial stripes at either end
    /// go through the stripe buffer.
    async fn write_blocks(&self, start: VdevLba, buf: &[Block]) -> Result<(), DiskError> {
        let d = self.data as u64;
        let mut lba = start;
        let mut rest = buf;

        while !lba.get().is_multiple_of(d)
            && let Some((block, tail)) = rest.split_first()
        {
            self.write_block(lba, block).await?;
            rest = tail;
            lba += 1;
        }

        let full_blocks = rest.len() as u64 / d * d;
        if full_blocks > 0 {
            let (full, tail) = rest.split_at(full_blocks as usize);
            self.write_full_stripes(lba.stripe_of(self.data).0, full.as_flattened())
                .await
                .map_err(write_err)?;
            lba += full_blocks;
            rest = tail;
        }

        while let Some((block, tail)) = rest.split_first() {
            self.write_block(lba, block).await?;
            rest = tail;
            lba += 1;
        }
        Ok(())
    }

    async fn reconstruct_candidates(&self, lba: VdevLba) -> Result<Candidates, DiskError> {
        let n = self.members.len();
        let (stripe_id, target) = lba.stripe_of(self.data);
        let reads = (0..n).filter(|&i| i != target).map(|i| async move {
            let mut buf = [0u8; BLOCK_SIZE];
            self.read_shard(MemberIdx(i), stripe_id, std::slice::from_mut(&mut buf))
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
        let rebuilds = crate::rt::spawn_blocking(move || {
            // (excluded shard position, or `target` for "only itself", rebuild)
            let mut out: Vec<(usize, Box<Block>)> = Vec::new();
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
                let mut rebuilt = Box::new([0u8; BLOCK_SIZE]);
                if decode_shard(data, parity, &shards, &using, target, &mut rebuilt[..]).is_ok() {
                    out.push((exclude, rebuilt));
                }
            }
            out
        })
        .await
        .map_err(io_err)?;

        let mut out = vec![None; self.disks.len()];
        for (exclude, rebuilt) in rebuilds {
            out[self.members[exclude].as_usize()] = Some(rebuilt);
        }
        // A bad disk in another vdev of the pool doesn't constrain how this
        // block is rebuilt.
        if let Some(any) = out.iter().flatten().next().cloned() {
            for (d, slot) in self.disks.indexes().zip(out.iter_mut()) {
                if !self.members.contains(&d) {
                    *slot = Some(any.clone());
                }
            }
        }
        Ok(Candidates {
            disk: self.members[target],
            rebuilt: out,
        })
    }

    async fn repair_block(&self, lba: VdevLba, buf: &Block) -> Result<(), DiskError> {
        let (stripe_id, data_index) = lba.stripe_of(self.data);
        // A buffered (unflushed) stripe will be rewritten in full on flush anyway.
        if self.writes.contains_key(&stripe_id) {
            return Ok(());
        }
        // Parity already encodes this data, so only the data shard needs rewriting.
        self.disks
            .repair_shards(
                self.members[data_index],
                stripe_id.shard(),
                std::slice::from_ref(buf),
            )
            .await
    }

    async fn verify_blocks(
        &self,
        lba: VdevLba,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let (stripe, at) = lba.stripe_of(self.data);
        debug_assert_eq!(at, 0);
        debug_assert!(data.len().is_multiple_of(self.data * BLOCK_SIZE));
        self.verify_stripes(stripe, data, repair).await
    }

    async fn verify_labels(
        &self,
        lba: VdevLba,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
        stale: &[bool],
    ) -> Result<LabelScan, RaidZError> {
        let d = self.data as u64;
        let first = lba.stripe_of(self.data).0;
        let end = (lba.get() + blocks).div_ceil(d);
        let mut scan = LabelScan {
            check: StripeCheck::new(self.disks.len(), 0),
            ..Default::default()
        };
        for stripe in (first.get()..end).map(StripeId::new) {
            scan.units += 1;
            let (check, stale_voted) = self
                .verify_label_stripe(stripe, repair, score, stale)
                .await?;
            if stale_voted {
                scan.stale_votes.push(stripe.first_lba(self.data));
            }
            match check {
                Some(check) => scan.check.add(&check),
                // Never-written space map slots are all zeros and fine. A
                // stale member's leftovers there don't count against that;
                // they're zeroed like any other stale shard.
                None => match self.label_stripe_blankness(stripe, stale).await? {
                    Blank::No => scan.unrecoverable.push(stripe.first_lba(self.data)),
                    Blank::Yes => scan.check.stripes += 1,
                    Blank::ButStale => {
                        let zeros = vec![0u8; self.data * BLOCK_SIZE];
                        scan.check
                            .add(&self.verify_stripes(stripe, &zeros, repair).await?);
                    }
                },
            }
        }
        Ok(scan)
    }
}

impl RaidZ {
    pub fn new(disks: Arc<DiskSet>, members: Vec<PoolDiskIdx>, parity: usize) -> Self {
        assert!(parity >= 1 && members.len() > parity, "invalid raidz shape");
        let data = members.len() - parity;
        RaidZ {
            data,
            parity,
            disks,
            members,
            writes: Arc::new(DashMap::new()),
            stripe_pool: Arc::new(ArrayQueue::new(100)),
            acks: WriteAcks::new(data),
        }
    }

    #[cfg(any(test, feature = "testutil"))]
    pub fn is_local_disk(&self, index: MemberIdx) -> bool {
        self.disks.is_local(self.members[index.0])
    }

    /// Read shard position `i` of stripes starting at `stripe`.
    async fn read_shard(
        &self,
        i: MemberIdx,
        stripe: StripeId,
        buf: &mut [Block],
    ) -> Result<(), DiskError> {
        self.disks
            .read_shards(self.members[i.0], stripe.shard(), buf)
            .await
    }

    /// The data shards `missing` of stripe `stripe` as on disk, for a
    /// read-modify-write. Nothing vouches for a shard read here (no record
    /// checksum covers a stripe's other blocks), and new parity is computed
    /// from it: a stale shard would make the stripe's parity encode stale
    /// data, losing what that disk missed for good. So a member that may
    /// lack writes (`DiskSet::may_lag`: degraded, or a write failed on it)
    /// isn't read; its shard is rebuilt from the members that don't lag,
    /// like an unreadable one's (structural triage I-20). Too few of them,
    /// and the stripe can't be rewritten yet (it stays buffered).
    async fn rmw_shards(
        &self,
        stripe: StripeId,
        missing: &[usize],
    ) -> Result<Vec<(usize, Shard)>, RaidZError> {
        let n = self.members.len();
        let lag: Vec<bool> = self
            .members
            .iter()
            .map(|&d| self.disks.may_lag(d))
            .collect();
        let lag = &lag;
        let read = |i: usize| async move {
            let mut b = [0u8; BLOCK_SIZE];
            if lag[i] {
                return (i, None);
            }
            let r = self
                .read_shard(MemberIdx(i), stripe, std::slice::from_mut(&mut b))
                .await;
            (i, r.ok().map(|_| b))
        };
        let mut out = Vec::with_capacity(missing.len());
        let mut need = Vec::new();
        for (i, b) in futures::future::join_all(missing.iter().map(|&i| read(i))).await {
            match b {
                Some(b) => out.push((i, b)),
                None => need.push(i),
            }
        }
        if need.is_empty() {
            return Ok(out);
        }

        let mut shards = vec![[0u8; BLOCK_SIZE]; n];
        let mut present = vec![false; n];
        for (i, b) in &out {
            shards[*i] = *b;
            present[*i] = true;
        }
        let rest = (0..n).filter(|&i| !lag[i] && !present[i] && !missing.contains(&i));
        for (i, b) in futures::future::join_all(rest.map(read)).await {
            if let Some(b) = b {
                shards[i] = b;
                present[i] = true;
            }
        }
        if present.iter().filter(|&&p| p).count() < self.data {
            return Err(RaidZError::IncompleteStripe(stripe));
        }
        for i in need {
            let mut b = [0u8; BLOCK_SIZE];
            self.reconstruct_shard(&shards, &present, i, &mut b)?;
            out.push((i, b));
        }
        Ok(out)
    }

    /// Reconstruct a block from other disks in the stripe
    async fn reconstruct_from_stripe(
        &self,
        stripe_id: StripeId,
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
                self.read_shard(MemberIdx(i), stripe_id, std::slice::from_mut(&mut temp_buf))
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
    ///
    /// Every stripe's buffer (a fresh, empty one where none was) is held
    /// locked across the write, so nothing reaches those stripes meanwhile:
    /// a partial write to another block of one waits, then starts a new
    /// buffer whose read-modify-write reads what this wrote, not what was
    /// there before (which its flush would otherwise write back over this).
    /// Only once the write succeeded are they retired, since it replaces
    /// them wholesale; on failure the partial writes buffered before it are
    /// kept for a later flush (structural triage I-21).
    async fn write_full_stripes(
        &self,
        first_stripe: StripeId,
        data: &[u8],
    ) -> Result<(), RaidZError> {
        let stripes = data.len() / (self.data * BLOCK_SIZE);

        // In stripe order, like any other taker of more than one (none
        // does: a flush or a write holds one at a time).
        let mut held = Vec::with_capacity(stripes);
        for s in (0..stripes as u64).map(|j| first_stripe + j) {
            loop {
                let buf = self
                    .writes
                    .entry(s)
                    .or_insert_with(|| Arc::new(Mutex::new(StripeBuffer::new(self.data))))
                    .value()
                    .clone();
                let guard = buf.clone().lock_owned().await;
                if guard.retired {
                    // Flushed since we found it: take a new one.
                    drop(guard);
                    self.writes.remove_if(&s, |_, v| Arc::ptr_eq(v, &buf));
                    continue;
                }
                held.push((s, buf, guard));
                break;
            }
        }

        let written = self.write_encoded(first_stripe, data).await;
        for (s, buf, mut guard) in held {
            // A buffer of our own (nothing written into it) goes either way.
            if written.is_ok() || !guard.dirty {
                guard.retired = true;
                if let Some(b) = guard.take_buffer() {
                    let _ = self.stripe_pool.push(b);
                }
                drop(guard);
                self.writes.remove_if(&s, |_, v| Arc::ptr_eq(v, &buf));
            }
        }
        written
    }

    /// Encode whole stripes of `data` and write every shard. Succeeds if
    /// enough disks took the write for the data to be recoverable.
    async fn write_encoded(&self, first_stripe: StripeId, data: &[u8]) -> Result<(), RaidZError> {
        let stripes = data.len() / (self.data * BLOCK_SIZE);
        let disk_bufs = self.encode_stripes(data)?;

        let writes = disk_bufs.iter().enumerate().map(|(i, buf)| {
            self.disks
                .write_shards(self.members[i], first_stripe.shard(), buf)
        });
        let results = futures::future::join_all(writes).await;
        if let Some(e) = fenced_any(&results) {
            return Err(RaidZError::DiskError(e));
        }
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
            self.acks.record(ok_mask(&self.members, &results));
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
    fn encode_stripes(&self, data: &[u8]) -> Result<Vec<Vec<Block>>, RaidZError> {
        encode_stripes(self.data, self.parity, data)
    }

    /// Check the stripes starting at `first_stripe` against `data` (whole
    /// stripes of known-good contents, e.g. a record that passed its
    /// checksum): every shard on every disk, data and parity, must be what
    /// `data` encodes to. With `repair`, mismatching shards are rewritten.
    /// Stripes with buffered writes are skipped (they'll be rewritten whole).
    pub async fn verify_stripes(
        &self,
        first_stripe: StripeId,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let n = data.len() / (self.data * BLOCK_SIZE);
        let expected = self.encode_stripes(data)?;
        let reads = self.members.iter().map(|&d| async move {
            let mut run = zeroed_blocks(n);
            self.disks
                .read_shards_raw(d, first_stripe.shard(), &mut run)
                .await
                .map(|_| run)
        });
        let actual = read_or_fenced(futures::future::join_all(reads).await)?;

        let mut check = StripeCheck::new(self.disks.len(), n as u64);
        let buffered: Vec<bool> = (0..n as u64)
            .map(|j| self.writes.contains_key(&(first_stripe + j)))
            .collect();
        check.skipped = buffered.iter().filter(|&&b| b).count() as u64;

        for (i, &disk) in self.members.iter().enumerate() {
            let d = disk.as_usize();
            let Some(run) = &actual[i] else {
                check.unreadable[d] = true;
                continue;
            };
            for (j, &busy) in buffered.iter().enumerate() {
                if busy {
                    continue;
                }
                if run[j] == expected[i][j] {
                    continue;
                }
                check.mismatched[d] += 1;
                if repair {
                    match self
                        .disks
                        .repair_shards(
                            disk,
                            (first_stripe + j as u64).shard(),
                            &expected[i][j..j + 1],
                        )
                        .await
                    {
                        Ok(()) => check.repaired[d] += 1,
                        Err(e) if e.is_fencing() => return Err(e.into()),
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
    /// produced a single valid label, and whether stale members' shards
    /// voted with some member unread, or outvoted a current shard (see
    /// `LabelScan::stale_votes`): then nothing is repaired.
    pub async fn verify_label_stripe(
        &self,
        stripe: StripeId,
        repair: bool,
        score: LabelScore<'_>,
        stale: &[bool],
    ) -> Result<(Option<StripeCheck>, bool), RaidZError> {
        let n = self.members.len();
        let mut shards = vec![[0u8; BLOCK_SIZE]; n];
        let mut present = vec![false; n];
        let reads = self.members.iter().map(|&d| async move {
            let mut b = [0u8; BLOCK_SIZE];
            self.disks
                .read_shards_raw(d, stripe.shard(), std::slice::from_mut(&mut b))
                .await
                .map(|_| b)
        });
        for (i, r) in read_or_fenced(futures::future::join_all(reads).await)?
            .into_iter()
            .enumerate()
        {
            if let Some(b) = r {
                shards[i] = b;
                present[i] = true;
            }
        }
        // Stale members' shards don't vote while the rest can rebuild the
        // stripe alone: they're rebuilt (and repaired) like missing ones.
        let is_stale = |i: usize| {
            stale
                .get(self.members[i].as_usize())
                .copied()
                .unwrap_or(false)
        };
        let current = (0..n).filter(|&i| present[i] && !is_stale(i)).count();
        let stale_help = if current >= self.data {
            for (i, p) in present.iter_mut().enumerate() {
                if is_stale(i) {
                    *p = false;
                }
            }
            false
        } else {
            (0..n).any(|i| present[i] && is_stale(i))
        };
        let current_shards: Vec<PoolDiskIdx> = (0..n)
            .filter(|&i| present[i] && !is_stale(i))
            .map(|i| self.members[i])
            .collect();
        let mut stale_voted = stale_help && present.iter().any(|p| !p);

        // Decoding and re-encoding every subset of the shards is the heavy
        // part: off the async thread.
        let (data, parity) = (self.data, self.parity);
        let candidates =
            crate::rt::spawn_blocking(move || label_candidates(data, parity, &shards, &present))
                .await
                .map_err(|e| RaidZError::DiskError(io_err(e)))??;
        let Some(candidates) = candidates else {
            // Consistent as read (and fully readable): nothing to decide.
            return Ok((Some(StripeCheck::new(self.disks.len(), 1)), stale_voted));
        };

        // Rank the candidates. Every block of a label stripe is written in the
        // same TXG (the uberblock copies, the superblock mirrors, the
        // blocks of one space map slot), so the truth is a full set of valid
        // labels sharing one TXG. Stale shards mixed with current ones can
        // decode into *valid* labels of other versions, or into rebuilds that
        // happen to re-encode consistently with several disks, so neither
        // validity nor agreement alone decides. Then prefer the newest (a
        // label is only rewritten in a later TXG, so older ones come from
        // disks that missed commits, however many agree: audit D3; `score`
        // rejects labels past the last commit), then the one most disks hold.
        // (valid labels, one TXG, TXG sum, agreeing disks)
        type Rank = (usize, bool, u64, usize);
        let mut best: Option<(Rank, Vec<u8>)> = None;
        for (data, agree) in candidates {
            let txgs: Vec<u64> = data.chunks(BLOCK_SIZE).filter_map(score).collect();
            let uniform = txgs.len() == self.data && txgs.iter().all(|&t| t == txgs[0]);
            let rank = (txgs.len(), uniform, txgs.iter().sum(), agree);
            if !txgs.is_empty() && best.as_ref().is_none_or(|(r, _)| rank > *r) {
                best = Some((rank, data));
            }
        }
        let Some((_, data)) = best else {
            return Ok((None, stale_voted));
        };
        if stale_help {
            // Stale shards helped decide, so the truth may be an older
            // version they agree on (a uniform stale rebuild outranks a mixed
            // one). It must not overwrite a current shard: one it contradicts
            // means stale won over current, and nothing is repaired. Nor is
            // anything repaired with some member unread, whose copy may be
            // the newer one (structural triage I-19 review).
            let dry = self.verify_stripes(stripe, &data, false).await?;
            if current_shards
                .iter()
                .any(|&d| dry.mismatched.get(d.as_usize()).is_some_and(|&m| m > 0))
            {
                stale_voted = true;
            }
            if stale_voted {
                return Ok((Some(dry), true));
            }
        }
        Ok((
            Some(self.verify_stripes(stripe, &data, repair).await?),
            stale_voted,
        ))
    }

    /// Never written: every readable current shard is zeros, and enough
    /// are readable to be sure. `ButStale`: a stale member's shard isn't.
    async fn label_stripe_blankness(
        &self,
        stripe: StripeId,
        stale: &[bool],
    ) -> Result<Blank, RaidZError> {
        let mut readable = 0;
        let mut stale_dirty = false;
        for &d in &self.members {
            let mut b = [0u8; BLOCK_SIZE];
            match self
                .disks
                .read_shards_raw(d, stripe.shard(), std::slice::from_mut(&mut b))
                .await
            {
                Ok(()) => {}
                Err(e) if e.is_fencing() => return Err(e.into()),
                Err(_) => continue,
            }
            let zero = b.iter().all(|&x| x == 0);
            if stale.get(d.as_usize()).copied().unwrap_or(false) {
                stale_dirty |= !zero;
                continue;
            }
            if !zero {
                return Ok(Blank::No);
            }
            readable += 1;
        }
        Ok(match (readable >= self.data, stale_dirty) {
            (false, _) => Blank::No,
            (true, false) => Blank::Yes,
            (true, true) => Blank::ButStale,
        })
    }
}

/// Encode whole stripes of `data` into one buffer per shard position.
/// Each disk's shards of whole stripes `bytes` (data disks, then parity).
fn encode_stripes(data: usize, parity: usize, bytes: &[u8]) -> Result<Vec<Vec<Block>>, RaidZError> {
    let stripe_bytes = data * BLOCK_SIZE;
    debug_assert!(bytes.len().is_multiple_of(stripe_bytes));
    let stripes = bytes.len() / stripe_bytes;
    let mut disk_bufs: Vec<Vec<Block>> = vec![zeroed_blocks(stripes); data + parity];
    let mut encoder = ReedSolomonEncoder::new(data, parity, BLOCK_SIZE)?;
    for (j, stripe) in bytes.chunks(stripe_bytes).enumerate() {
        if j > 0 {
            encoder.reset(data, parity, BLOCK_SIZE)?;
        }
        for (i, shard) in stripe.chunks(BLOCK_SIZE).enumerate() {
            disk_bufs[i][j].copy_from_slice(shard);
            encoder.add_original_shard(shard)?;
        }
        let encoded = encoder.encode()?;
        for (p, recovery) in encoded.recovery_iter().enumerate() {
            disk_bufs[data + p][j].copy_from_slice(recovery);
        }
    }
    Ok(disk_bufs)
}

/// A label stripe's data as rebuilt one way, and how many disks agree with it.
type LabelCandidate = (Vec<u8>, usize);

/// Whether a label stripe that decoded to nothing valid was never written.
enum Blank {
    No,
    Yes,
    /// Never written, but a stale member holds leftovers there.
    ButStale,
}

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
            if (data..n).all(|i| enc[i].as_flattened() == &shards[i][..]) {
                return Ok(None);
            }
        }
    }

    let mut out = Vec::new();
    // Every way of picking exactly `data` of the shards.
    for m in masks_of(n, data) {
        let use_: Vec<bool> = (0..n).map(|d| m & (1 << d) != 0).collect();
        if (0..n).any(|i| use_[i] && !present[i]) {
            continue;
        }
        let Some(d) = data_of(&use_) else { continue };
        let enc = encode_stripes(data, parity, &d)?;
        let agree = (0..n)
            .filter(|&i| present[i] && enc[i].as_flattened() == &shards[i][..])
            .count();
        out.push((d, agree));
    }
    Ok(Some(out))
}

/// Every `n`-bit mask with exactly `k` bits set, in ascending order (as
/// filtering `0..1 << n` would give them, so ties rank as they always
/// did), by Gosper's hack: C(n, k) steps rather than 2^n (audit D6b; a
/// 33-disk raidz3 is a valid pool), with no shift overflow at any width.
fn masks_of(n: usize, k: usize) -> impl Iterator<Item = u128> {
    let end: u128 = 1 << n;
    let first: u128 = (1 << k) - 1;
    std::iter::successors(Some(first), move |&m| {
        if m == 0 {
            return None; // k == 0: the empty mask only
        }
        let c = m.isolate_lowest_one();
        let r = m + c;
        Some((((r ^ m) >> 2) / c) | r)
    })
    .take_while(move |&m| m < end)
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
    /// Written out (or superseded) and out of the map, or about to be:
    /// nothing may be added to it any more.
    retired: bool,
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
            retired: false,
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
