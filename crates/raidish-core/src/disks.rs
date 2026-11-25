use crate::disk::{BLOCK_SIZE, Block, BlockDevice, BlockId, Disk, DiskError, DiskOptions};
use async_trait::async_trait;
use futures::future::join_all;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Blocks at the start of every member disk reserved for its identity label
/// (two copies), outside the RAID address space. Shard `s` lives at block
/// `s + DISK_LABEL_BLOCKS` of each disk.
pub const DISK_LABEL_BLOCKS: u64 = 2;

/// Most leaf disks a pool can have (fencing is a 64-bit mask).
pub const MAX_DISKS: usize = 64;

/// Every leaf disk of a pool, in config order (that order is the disk's
/// index everywhere: labels, `degraded`, fencing, status). Vdevs share it
/// and address their members by index.
///
/// All shard I/O goes through the `*_shards` helpers here, which apply the
/// label offset, refuse reads from *fenced* disks (members whose contents
/// can't be trusted until resilvered) and count every failed write or flush
/// per disk, so the pool knows which disks missed data.
#[derive(Debug)]
pub struct DiskSet {
    disks: Arc<Vec<Caddy>>,
    fenced: AtomicU64,
    misses: Vec<AtomicU64>,
    /// Per disk: the miss count as of the last time it was found in sync
    /// (`caught_up`). Any miss since may have left it without some write.
    synced_misses: Vec<AtomicU64>,
    /// The pool's record of which disks lack writes from which TXG on:
    /// `FileSystem::degraded` is this, shared, so vdevs read it live.
    pub(crate) degraded: Arc<std::sync::Mutex<Vec<Option<crate::types::Txg>>>>,
    /// Per disk: repair writes (`repair_shards`) made, ever.
    repairs: Vec<AtomicU64>,
    /// Per disk: how many of `repairs` a finished flush of that disk has
    /// covered (or lost: see `repairs_lost`). Both only grow, so overlapping
    /// flushes can't undercount.
    repairs_flushed: Vec<AtomicU64>,
    /// A disk failed the flush that would have made repairs on it durable
    /// (see `take_repairs_lost`).
    repairs_lost: std::sync::atomic::AtomicBool,
    /// The epochs this pool client's remote requests carry (see `crate::fence`).
    fence: Arc<crate::fence::ClientFence>,
}

impl DiskSet {
    /// Disk `index`.
    pub fn disk(&self, index: usize) -> &Caddy {
        &self.disks[index]
    }

    /// Every disk, in pool order (shared with the disk server).
    pub fn caddies(&self) -> &Arc<Vec<Caddy>> {
        &self.disks
    }

    /// The epochs this pool client's remote requests carry.
    pub fn fence(&self) -> &crate::fence::ClientFence {
        &self.fence
    }

    pub fn len(&self) -> usize {
        self.disks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.disks.is_empty()
    }

    pub fn is_local(&self, index: usize) -> bool {
        self.disks[index].is_local()
    }

    pub async fn new(paths: &[&str]) -> Self {
        Self::new_with(paths, DiskOptions::default()).await
    }

    pub async fn new_with(paths: &[&str], opts: DiskOptions) -> Self {
        Self::new_fenced(paths, opts, Default::default()).await
    }

    /// Open `paths`, their requests carrying `fence`'s epochs.
    pub async fn new_fenced(
        paths: &[&str],
        opts: DiskOptions,
        fence: Arc<crate::fence::ClientFence>,
    ) -> Self {
        let results = join_all(paths.iter().map(|path| Disk::open_with(path, opts))).await;
        Self::from_opened(paths, results, fence)
    }

    /// `new_with`, but a path already open in `open` shares that disk (the
    /// same file, and its lock) rather than opening it a second time.
    pub async fn new_sharing(paths: &[&str], opts: DiskOptions, open: &DiskSet) -> Self {
        let results = join_all(paths.iter().map(|&path| async move {
            match open.disks.iter().find(|c| c.path() == path) {
                Some(Caddy::Healthy(_, disk)) => Ok(disk.clone()),
                Some(Caddy::Unhealthy(_, e)) => Err(DiskError::IoError(std::io::Error::other(
                    format!("unavailable: {}", e),
                ))),
                None => Disk::open_with(path, opts).await,
            }
        }))
        .await;
        Self::from_opened(paths, results, Default::default())
    }

    fn from_opened(
        paths: &[&str],
        results: Vec<Result<Disk, DiskError>>,
        fence: Arc<crate::fence::ClientFence>,
    ) -> Self {
        assert!(paths.len() <= MAX_DISKS, "at most {} disks", MAX_DISKS);
        let disks: Vec<Caddy> = results
            .into_iter()
            .zip(paths.iter())
            .enumerate()
            .map(|(i, (result, &path))| match result {
                Ok(disk) => {
                    disk.set_fence(i, fence.clone());
                    Caddy::Healthy(path.to_string(), disk)
                }
                Err(error) => {
                    eprintln!("disk '{}' unavailable: {}", path, error);
                    Caddy::Unhealthy(path.to_string(), error)
                }
            })
            .collect();

        let mut set = Self::from_caddies(disks);
        set.fence = fence;
        set
    }

    pub fn from_caddies(disks: Vec<Caddy>) -> Self {
        DiskSet {
            fence: Default::default(),
            misses: (0..disks.len()).map(|_| AtomicU64::new(0)).collect(),
            synced_misses: (0..disks.len()).map(|_| AtomicU64::new(0)).collect(),
            degraded: Arc::new(std::sync::Mutex::new(vec![None; disks.len()])),
            repairs: (0..disks.len()).map(|_| AtomicU64::new(0)).collect(),
            repairs_flushed: (0..disks.len()).map(|_| AtomicU64::new(0)).collect(),
            repairs_lost: Default::default(),
            disks: Arc::new(disks),
            fenced: AtomicU64::new(0),
        }
    }

    /// Stop (or resume) serving reads from `disk`; writes still go to it.
    pub fn set_fenced(&self, disk: usize, fenced: bool) {
        if fenced {
            self.fenced.fetch_or(1 << disk, Ordering::SeqCst);
        } else {
            self.fenced.fetch_and(!(1 << disk), Ordering::SeqCst);
        }
    }

    pub fn is_fenced(&self, disk: usize) -> bool {
        self.fenced.load(Ordering::SeqCst) & (1 << disk) != 0
    }

    /// Failed writes/flushes on `disk` so far (monotonic).
    pub fn miss_count(&self, disk: usize) -> u64 {
        self.misses[disk].load(Ordering::SeqCst)
    }

    /// `disk` may lack writes the pool made: it's degraded, or a write or
    /// flush failed on it since it was last found in sync. Its shards are
    /// fine to read when a checksum vouches for them, but not where nothing
    /// does (raidz read-modify-write: structural triage I-20).
    pub fn may_lag(&self, disk: usize) -> bool {
        self.miss_count(disk) != self.synced_misses[disk].load(Ordering::SeqCst)
            || self
                .degraded
                .lock()
                .unwrap()
                .get(disk)
                .is_some_and(|d| d.is_some())
    }

    /// A resilver found `disk` in sync as of `misses` (its miss count when
    /// the resilver began): misses since still count.
    pub(crate) fn caught_up(&self, disk: usize, misses: u64) {
        self.synced_misses[disk].fetch_max(misses, Ordering::SeqCst);
    }

    pub fn is_online(&self, disk: usize) -> bool {
        self.disks[disk].is_online()
    }

    /// Readable for normal I/O: online and not fenced.
    pub fn is_readable(&self, disk: usize) -> bool {
        self.is_online(disk) && !self.is_fenced(disk)
    }

    /// Count a failed write or flush as a miss: the disk lacks data the
    /// pool wrote. A fencing refusal isn't one (the disk is fine; we may not
    /// write there): it fails the whole op instead (see `fenced_any`).
    fn miss<T>(&self, disk: usize, r: Result<T, DiskError>) -> Result<T, DiskError> {
        if let Err(e) = &r
            && !e.is_fencing()
        {
            self.misses[disk].fetch_add(1, Ordering::SeqCst);
        }
        r
    }

    /// Shards (blocks after the label area) available on every one of
    /// `members` (the smallest disk wins).
    pub async fn shard_capacity(&self, members: &[usize]) -> Result<u64, DiskError> {
        let mut min = u64::MAX;
        for &d in members {
            min = min.min(self.disks[d].capacity_blocks().await?);
        }
        Ok(min.saturating_sub(DISK_LABEL_BLOCKS))
    }

    /// Read `buf.len()` consecutive shards of `disk` starting at `shard`.
    pub async fn read_shards(
        &self,
        disk: usize,
        shard: u64,
        buf: &mut [Block],
    ) -> Result<(), DiskError> {
        if self.is_fenced(disk) {
            return Err(DiskError::UnhealthyDisk);
        }
        self.read_shards_raw(disk, shard, buf).await
    }

    /// `read_shards`, even from a fenced disk (for verification).
    pub async fn read_shards_raw(
        &self,
        disk: usize,
        shard: u64,
        buf: &mut [Block],
    ) -> Result<(), DiskError> {
        self.disks[disk]
            .read_blocks(shard + DISK_LABEL_BLOCKS, buf)
            .await
    }

    pub async fn write_shards(
        &self,
        disk: usize,
        shard: u64,
        buf: &[Block],
    ) -> Result<(), DiskError> {
        let r = self.disks[disk]
            .write_blocks(shard + DISK_LABEL_BLOCKS, buf)
            .await;
        self.miss(disk, r)
    }

    /// `write_shards` for a repair: a copy or shard rewritten on one disk
    /// because it was wrong there. It's no write a vdev flush is owed for
    /// (the data was already durable elsewhere), so a failed flush of that
    /// disk doesn't fail the vdev's flush (and every commit with it, for as
    /// long as the disk is bad). It's noted instead, for the scan that made
    /// the repair to ask (`take_repairs_lost`) before counting it done.
    pub async fn repair_shards(
        &self,
        disk: usize,
        shard: u64,
        buf: &[Block],
    ) -> Result<(), DiskError> {
        self.write_shards(disk, shard, buf).await?;
        self.repairs[disk].fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub async fn flush_disk(&self, disk: usize) -> Result<(), DiskError> {
        // Only repairs written before this flush began are covered by it.
        let repaired = self.repairs[disk].load(Ordering::SeqCst);
        let r = self.disks[disk].flush().await;
        if repaired > self.repairs_flushed[disk].load(Ordering::SeqCst) {
            if r.is_err() {
                self.repairs_lost.store(true, Ordering::SeqCst);
            }
            // Accounted for either way: lost repairs are reported once.
            self.repairs_flushed[disk].fetch_max(repaired, Ordering::SeqCst);
        }
        self.miss(disk, r)
    }

    /// Whether repairs (`repair_shards`) were lost to a failed flush since
    /// this was last asked, or are on disks not flushed since.
    pub fn take_repairs_lost(&self) -> bool {
        let unflushed = self
            .repairs
            .iter()
            .zip(&self.repairs_flushed)
            .any(|(n, f)| n.load(Ordering::SeqCst) > f.load(Ordering::SeqCst));
        self.repairs_lost.swap(false, Ordering::SeqCst) || unflushed
    }

    /// Raw access to label copy `copy` (< DISK_LABEL_BLOCKS) of `disk`.
    pub async fn read_label(
        &self,
        disk: usize,
        copy: u64,
        buf: &mut Block,
    ) -> Result<(), DiskError> {
        assert!(copy < DISK_LABEL_BLOCKS);
        self.disks[disk].read_block(copy, buf).await
    }

    pub async fn write_label(
        &self,
        disk: usize,
        copy: u64,
        buf: &[u8; BLOCK_SIZE],
    ) -> Result<(), DiskError> {
        assert!(copy < DISK_LABEL_BLOCKS);
        self.disks[disk].write_block(copy, buf).await
    }
}

#[derive(Debug)]
pub enum Caddy {
    Healthy(String, Disk),
    Unhealthy(String, DiskError),
}

#[async_trait]
impl BlockDevice for Caddy {
    async fn read_block(&self, block: BlockId, buf: &mut Block) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.read_block(block, buf).await.inspect_err(|e| {
                log_err("read from", dev, e);
            }),
        }
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.write_block(block, buf).await.inspect_err(|e| {
                log_err("write to", dev, e);
            }),
        }
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(_, disk) => disk.capacity_blocks().await,
        }
    }

    async fn read_blocks(&self, start: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.read_blocks(start, buf).await.inspect_err(|e| {
                log_err("read from", dev, e);
            }),
        }
    }

    async fn write_blocks(&self, start: BlockId, buf: &[Block]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.write_blocks(start, buf).await.inspect_err(|e| {
                log_err("write to", dev, e);
            }),
        }
    }

    async fn flush(&self) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.flush().await.inspect_err(|e| {
                log_err("flush", dev, e);
            }),
        }
    }
}

/// Log an I/O error, except the fast failures of a disk known to be offline
/// (those would flood the log while it reconnects).
fn log_err(op: &str, dev: &str, e: &DiskError) {
    if !matches!(e, DiskError::Disconnected) {
        eprintln!("caddy: unable to {} disk '{}': {}", op, dev, e);
    }
}

impl Caddy {
    pub fn path(&self) -> &str {
        match self {
            Caddy::Healthy(p, _) | Caddy::Unhealthy(p, _) => p,
        }
    }

    pub fn is_online(&self) -> bool {
        match self {
            Caddy::Unhealthy(_, _) => false,
            Caddy::Healthy(_, disk) => disk.is_online(),
        }
    }

    /// Changes whenever a remote disk reconnects (None for local disks).
    pub fn reconnects(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        match self {
            Caddy::Unhealthy(_, _) => None,
            Caddy::Healthy(_, disk) => disk.reconnects(),
        }
    }

    pub fn io_stats(&self) -> Option<crate::disk::IoStats> {
        match self {
            Caddy::Unhealthy(_, _) => None,
            Caddy::Healthy(_, disk) => disk.io_stats(),
        }
    }

    pub fn is_local(&self) -> bool {
        match self {
            Caddy::Unhealthy(_, _) => false,
            Caddy::Healthy(_, disk) => disk.is_local(),
        }
    }
}

/// Which members took each write a vdev acked since its last flush (audit
/// D1). A write is durable once `need` of the members that took it have
/// flushed after it; a flush that leaves any acked write short of that is a
/// failure, even if some members flushed fine (an `Ok` flush must never
/// cover a write no disk holds durably).
///
/// Flushes are serialized: a flush that started after a write completed
/// must not return before that write is durable, which a concurrent flush
/// that already took it off the set would otherwise allow.
#[derive(Debug)]
pub struct WriteAcks {
    need: u32,
    /// Distinct "members that acked" masks (usually just one: all of them).
    pending: std::sync::Mutex<std::collections::HashSet<u64>>,
    flushing: tokio::sync::Mutex<()>,
}

impl WriteAcks {
    pub fn new(need: usize) -> Self {
        WriteAcks {
            need: need as u32,
            pending: Default::default(),
            flushing: Default::default(),
        }
    }

    /// Record a write `acked` (a mask of disk indices) took.
    pub fn record(&self, acked: u64) {
        let mut p = self.pending.lock().unwrap();
        if !p.contains(&acked) {
            p.insert(acked);
        }
    }

    /// Run `flush` (which flushes every member and returns the mask that
    /// succeeded) and check every write recorded before it against that.
    /// On failure the writes stay pending, so a later flush retries them.
    pub async fn flush<F, Fut>(&self, flush: F) -> Result<(), DiskError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<u64, DiskError>>,
    {
        let _serial = self.flushing.lock().await;
        // Given up on mid-flush (a dropped future), the writes taken go
        // back: nothing says they're durable.
        let mut taken = Restore {
            acks: self,
            taken: Some(std::mem::take(&mut *self.pending.lock().unwrap())),
        };
        let r = flush().await.and_then(|flushed| {
            match taken
                .taken
                .iter()
                .flatten()
                .find(|&&m| (m & flushed).count_ones() < self.need)
            {
                None => Ok(()),
                Some(&m) => Err(DiskError::IoError(std::io::Error::other(format!(
                    "flush: writes acked by disks {:#x} are durable on only {} of them",
                    m,
                    (m & flushed).count_ones()
                )))),
            }
        });
        if r.is_ok() {
            taken.taken = None;
        }
        r
    }
}

/// Puts writes `WriteAcks::flush` took back as pending, unless the flush
/// covered them.
struct Restore<'a> {
    acks: &'a WriteAcks,
    taken: Option<std::collections::HashSet<u64>>,
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        if let Some(t) = self.taken.take() {
            self.acks.pending.lock().unwrap().extend(t);
        }
    }
}

/// The first fencing refusal among a vdev op's per-member `results`: a
/// write some members refused for who sent it must fail as a whole, never
/// "succeed" on the rest with the refusing members counted as degraded
/// (audit R2a). The other members did take it; the caller stops writing.
pub fn fenced_any<T>(results: &[Result<T, DiskError>]) -> Option<DiskError> {
    results.iter().find_map(|r| match r {
        Err(DiskError::StaleEpoch) => Some(DiskError::StaleEpoch),
        Err(DiskError::Fenced) => Some(DiskError::Fenced),
        _ => None,
    })
}

/// Each member's verification read, `None` where it failed, unless one
/// was a fencing refusal: that fails the whole scan, as it fails any vdev
/// op (audit R2a), rather than counting the disk as unreadable.
pub fn read_or_fenced<T>(results: Vec<Result<T, DiskError>>) -> Result<Vec<Option<T>>, DiskError> {
    if let Some(e) = fenced_any(&results) {
        return Err(e);
    }
    Ok(results.into_iter().map(Result::ok).collect())
}

/// The mask of `members` whose `results` are `Ok`.
pub fn ok_mask<T, E>(members: &[usize], results: &[Result<T, E>]) -> u64 {
    members
        .iter()
        .zip(results)
        .filter(|(_, r)| r.is_ok())
        .fold(0, |m, (&d, _)| m | 1 << d)
}
