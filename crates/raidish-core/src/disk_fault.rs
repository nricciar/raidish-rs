//! Test-only fault injection for one member disk (`Disk::faulty`).
//!
//! A `FaultDisk` is a local backing file with switches in front of it. Its
//! `FaultHandle` outlives any `Disk`/`Pool` opened over it, so a test can
//! open, crash, reopen and resilver a pool while steering what each member
//! disk does:
//!
//! - fail writes (all, or after N more), with any `FaultError` (including
//!   `StaleEpoch`); fail flushes; go offline (every op errors);
//! - drop writes: ack them, persist nothing (a lying disk);
//! - cache writes: ack them, keep them in volatile memory that reads see and
//!   a flush persists; `power_cut` throws away what wasn't flushed;
//! - hold writes: in flight until the test lands them, in any order (the
//!   caller waits for the ack; a crashed caller's write still lands);
//! - defer writes: ack (or fail) now, land when the test says, possibly after
//!   newer writes to the same blocks (a detached write that lands late);
//! - snapshot and later restore the disk image (a disk that drops out,
//!   misses TXGs and comes back stale with a valid label).
//!
//! Write faults can be limited to a block range (`target_blocks`); writes
//! outside it go straight through. Block numbers are the disk's own (labels
//! included, so shard `s` is block `s + DISK_LABEL_BLOCKS`).
//!
//! I/O is synchronous under one lock per disk, so a disk's operations are
//! totally ordered and nothing races the switches.

use super::{BLOCK_SIZE, Block, DiskError, read_at_zero_fill};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};

/// The error an injected fault reports (`DiskError` itself isn't `Clone`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultError {
    Io,
    StaleEpoch,
    Timeout,
    Disconnected,
    Unhealthy,
    /// The disk server refused the stamp kind (a pool write into a lent
    /// metaslab): `DiskError::Fenced`.
    Fenced,
}

impl FaultError {
    pub fn to_error(self) -> DiskError {
        match self {
            FaultError::Io => DiskError::IoError(std::io::Error::other("injected I/O error")),
            FaultError::StaleEpoch => DiskError::StaleEpoch,
            FaultError::Timeout => DiskError::Timeout,
            FaultError::Disconnected => DiskError::Disconnected,
            FaultError::Unhealthy => DiskError::UnhealthyDisk,
            FaultError::Fenced => DiskError::Fenced,
        }
    }
}

/// What happens to every operation on a disk while it is offline.
const OFFLINE: FaultError = FaultError::Disconnected;

/// Operation counts, for assertions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FaultStats {
    pub reads: u64,
    /// `write_blocks` calls, whatever became of them.
    pub writes: u64,
    /// Writes that reported an error.
    pub failed_writes: u64,
    /// Writes acked and thrown away (`drop_writes`).
    pub dropped_writes: u64,
    pub flushes: u64,
    pub failed_flushes: u64,
}

struct Deferred {
    start: u64,
    data: Vec<u8>,
}

/// A write in flight: it lands when released, whether or not its caller is
/// still waiting (a crashed writer's write still reaches the disk).
struct Held {
    write: Deferred,
    ack: oneshot::Sender<()>,
}

#[derive(Default)]
struct State {
    offline: bool,
    /// Writes that still succeed before every later one fails.
    fail_writes_after: Option<u64>,
    write_error: Option<FaultError>,
    fail_flushes: Option<FaultError>,
    drop_writes: bool,
    hold_writes: bool,
    /// Defer writes, then reply with this (`None`: ack).
    defer_writes: Option<Option<FaultError>>,
    target: Option<Range<u64>>,

    /// The volatile write cache, by block (when caching).
    cache: Option<BTreeMap<u64, Block>>,
    /// Blocks' contents as of `snapshot`, for those written since.
    undo: Option<HashMap<u64, Block>>,
    deferred: Vec<Deferred>,
    held: VecDeque<Held>,
    stats: FaultStats,
}

/// One fault-injectable member disk. Share it as a `FaultHandle`.
pub struct FaultDisk {
    path: PathBuf,
    file: std::fs::File,
    st: Mutex<State>,
    /// How many writes are being held right now.
    held: watch::Sender<usize>,
    /// Flushes wait while this is set (`hold_flushes`).
    hold_flushes: watch::Sender<bool>,
    /// How many flushes are waiting on `hold_flushes` right now.
    flushes_held: watch::Sender<usize>,
    /// Reads overlapping these blocks wait while it's set (`hold_reads`).
    hold_reads: watch::Sender<Option<Range<u64>>>,
    /// How many reads are waiting on `hold_reads` right now.
    reads_held: watch::Sender<usize>,
}

pub type FaultHandle = Arc<FaultDisk>;

impl std::fmt::Debug for FaultDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultDisk")
            .field("path", &self.path)
            .finish()
    }
}

impl FaultDisk {
    /// Open (or create) the backing file at `path`, with no faults.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<FaultHandle> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;
        Ok(Arc::new(FaultDisk {
            path: path.as_ref().to_path_buf(),
            file,
            st: Mutex::new(State::default()),
            held: watch::channel(0).0,
            hold_flushes: watch::channel(false).0,
            flushes_held: watch::channel(0).0,
            hold_reads: watch::channel(None).0,
            reads_held: watch::channel(0).0,
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---- switches ----

    /// Every write fails with `err` (`None`: back to normal).
    pub fn fail_writes(&self, err: Option<FaultError>) {
        let mut st = self.st();
        st.fail_writes_after = err.map(|_| 0);
        st.write_error = err;
    }

    /// The next `n` writes succeed, every later one fails with `err`.
    pub fn fail_writes_after(&self, n: u64, err: FaultError) {
        let mut st = self.st();
        st.fail_writes_after = Some(n);
        st.write_error = Some(err);
    }

    /// Every flush fails with `err` (`None`: back to normal).
    pub fn fail_flushes(&self, err: Option<FaultError>) {
        self.st().fail_flushes = err;
    }

    /// Every operation errors and the disk reports itself offline.
    pub fn set_offline(&self, offline: bool) {
        self.st().offline = offline;
    }

    /// Ack writes without persisting them.
    pub fn drop_writes(&self, on: bool) {
        self.st().drop_writes = on;
    }

    /// New writes stay in flight until released (`release_*`), which lands
    /// and acks them, in whatever order the test picks; a write whose caller
    /// has gone (crashed) still lands. Turning it off doesn't release the
    /// writes already held.
    pub fn hold_writes(&self, on: bool) {
        self.st().hold_writes = on;
    }

    /// New flushes wait until this is turned off (which releases them all).
    pub fn hold_flushes(&self, on: bool) {
        self.hold_flushes.send_replace(on);
    }

    /// New reads overlapping `blocks` wait until this is turned off
    /// (`None`, which releases them all).
    pub fn hold_reads(&self, blocks: Option<Range<u64>>) {
        self.hold_reads.send_replace(blocks);
    }

    /// Wait until at least `n` reads are held (panics after 10 s).
    pub async fn wait_reads_held(&self, n: usize) {
        let mut rx = self.reads_held.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.wait_for(|&h| h >= n))
            .await
            .unwrap_or_else(|_| panic!("{}: never held {} reads", self.path.display(), n))
            .unwrap();
    }

    /// Wait until `n` flushes are being held.
    pub async fn wait_flushes_held(&self, n: usize) {
        let mut rx = self.flushes_held.subscribe();
        rx.wait_for(|&h| h >= n).await.unwrap();
    }

    /// New writes are kept aside and reply `reply` at once (`Some(None)`:
    /// ack, `Some(Some(e))`: fail with `e`), landing only on
    /// `land_deferred`. `None`: stop deferring (keeps those already kept).
    pub fn defer_writes(&self, reply: Option<Option<FaultError>>) {
        self.st().defer_writes = reply;
    }

    /// Write faults apply only to writes touching these blocks (`None`:
    /// every write).
    pub fn target_blocks(&self, range: Option<Range<u64>>) {
        self.st().target = range;
    }

    /// Keep writes in a volatile cache until flushed (`false`: flush it
    /// first, then write through).
    pub fn cache_writes(&self, on: bool) {
        let cache = {
            let mut st = self.st();
            if on {
                st.cache.get_or_insert_with(BTreeMap::new);
                return;
            }
            st.cache.take()
        };
        if let Some(cache) = cache {
            let mut st = self.st();
            for (b, data) in cache {
                self.persist(&mut st, b, &data);
            }
        }
    }

    /// Lose the volatile cache (writes acked but not flushed). Caching
    /// stays on.
    pub fn power_cut(&self) {
        if let Some(c) = self.st().cache.as_mut() {
            c.clear();
        }
    }

    /// Clear every failure switch: no failures, online, nothing dropped,
    /// held or deferred from here on. Writes already held or deferred stay
    /// where they are, and a snapshot is kept. The write cache isn't a
    /// failure: it stays as it is (`cache_writes(false)` writes it back and
    /// stops caching; `power_cut` loses it).
    pub fn heal(&self) {
        let mut st = self.st();
        st.offline = false;
        st.fail_writes_after = None;
        st.write_error = None;
        st.fail_flushes = None;
        st.drop_writes = false;
        st.hold_writes = false;
        st.defer_writes = None;
        st.target = None;
        drop(st);
        self.hold_flushes.send_replace(false);
        self.hold_reads.send_replace(None);
    }

    // ---- held and deferred writes ----

    /// Writes being held right now.
    pub fn held(&self) -> usize {
        *self.held.borrow()
    }

    /// Wait until at least `n` writes are held (panics after 10 s).
    pub async fn wait_held(&self, n: usize) {
        let mut rx = self.held.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.wait_for(|&h| h >= n))
            .await
            .unwrap_or_else(|_| panic!("{}: never held {} writes", self.path.display(), n))
            .unwrap();
    }

    /// Land the oldest held write and ack it. False if none.
    pub fn release_oldest(&self) -> bool {
        self.release(|q| q.pop_front())
    }

    /// Land the newest held write and ack it. False if none.
    pub fn release_newest(&self) -> bool {
        self.release(|q| q.pop_back())
    }

    pub fn release_all(&self) {
        while self.release_oldest() {}
    }

    fn release(&self, pick: impl FnOnce(&mut VecDeque<Held>) -> Option<Held>) -> bool {
        let mut st = self.st();
        let Some(h) = pick(&mut st.held) else {
            return false;
        };
        self.persist_run(&mut st, h.write.start, &h.write.data);
        drop(st);
        self.held.send_modify(|n| *n -= 1);
        let _ = h.ack.send(());
        true
    }

    pub fn deferred(&self) -> usize {
        self.st().deferred.len()
    }

    /// Land every deferred write, oldest first (newer data they cover is
    /// overwritten, as a late write would). Returns how many.
    pub fn land_deferred(&self) -> usize {
        let mut st = self.st();
        let writes = std::mem::take(&mut st.deferred);
        let n = writes.len();
        for w in writes {
            self.persist_run(&mut st, w.start, &w.data);
        }
        n
    }

    /// Throw the deferred writes away (they never land).
    pub fn discard_deferred(&self) -> usize {
        std::mem::take(&mut self.st().deferred).len()
    }

    // ---- disk image ----

    /// Remember the disk image as it is now (replacing an earlier
    /// snapshot). Cached writes count as not there yet.
    pub fn snapshot(&self) {
        self.st().undo = Some(HashMap::new());
    }

    /// Put the disk image back as it was at `snapshot` and drop any cache,
    /// deferred and dropped state. Only while nothing has the disk open
    /// (or at least while no I/O is in flight). Returns how many blocks
    /// were rolled back.
    pub fn restore(&self) -> usize {
        let mut st = self.st();
        let undo = st.undo.take().expect("restore without a snapshot");
        if let Some(c) = st.cache.as_mut() {
            c.clear();
        }
        st.deferred.clear();
        let n = undo.len();
        for (b, data) in undo {
            self.file
                .write_all_at(&data, b * BLOCK_SIZE as u64)
                .expect("restore block");
        }
        self.file.sync_data().expect("restore sync");
        n
    }

    pub fn stats(&self) -> FaultStats {
        self.st().stats
    }

    pub fn is_online(&self) -> bool {
        !self.st().offline
    }

    // ---- I/O ----

    /// Write `data` at block `b` to the file, saving what it covers for
    /// `restore` first.
    fn persist(&self, st: &mut State, b: u64, data: &Block) {
        if let Some(undo) = st.undo.as_mut()
            && !undo.contains_key(&b)
        {
            let mut old = [0u8; BLOCK_SIZE];
            read_at_zero_fill(&self.file, &mut old, b * BLOCK_SIZE as u64).expect("undo read");
            undo.insert(b, old);
        }
        self.file
            .write_all_at(data, b * BLOCK_SIZE as u64)
            .expect("fault disk write");
    }

    /// Store a run at `start`: in the cache if caching, else on file.
    fn persist_run(&self, st: &mut State, start: u64, data: &[u8]) {
        let (blocks, _) = data.as_chunks::<BLOCK_SIZE>();
        for (i, blk) in blocks.iter().enumerate() {
            let b = start + i as u64;
            match st.cache.as_mut() {
                Some(c) => {
                    c.insert(b, *blk);
                }
                None => self.persist(st, b, blk),
            }
        }
    }

    fn targeted(st: &State, start: u64, n: u64) -> bool {
        st.target
            .as_ref()
            .is_none_or(|r| start < r.end && r.start < start + n)
    }

    pub(super) fn capacity(&self) -> Result<u64, DiskError> {
        if self.st().offline {
            return Err(OFFLINE.to_error());
        }
        Ok(self.file.metadata()?.len() / BLOCK_SIZE as u64)
    }

    pub(super) async fn read(
        &self,
        range: crate::coords::RawBlockRange,
        buf: &mut [u8],
    ) -> Result<(), DiskError> {
        let start = range.start().get();
        let n = (buf.len() / BLOCK_SIZE) as u64;
        let mut gate = self.hold_reads.subscribe();
        let holds = |r: &Option<Range<u64>>| {
            r.as_ref()
                .is_some_and(|r| start < r.end && r.start < start + n)
        };
        if holds(&gate.borrow_and_update()) {
            self.reads_held.send_modify(|n| *n += 1);
            let _ = gate.wait_for(|r| !holds(r)).await;
            self.reads_held.send_modify(|n| *n -= 1);
        }
        let mut st = self.st();
        if st.offline {
            return Err(OFFLINE.to_error());
        }
        st.stats.reads += 1;
        read_at_zero_fill(&self.file, buf, range.byte_offset())?;
        if let Some(c) = st.cache.as_ref() {
            let n = (buf.len() / BLOCK_SIZE) as u64;
            for (b, data) in c.range(start..start + n) {
                let at = (b - start) as usize * BLOCK_SIZE;
                buf[at..at + BLOCK_SIZE].copy_from_slice(data);
            }
        }
        Ok(())
    }

    pub(super) async fn write(
        &self,
        range: crate::coords::RawBlockRange,
        data: &[u8],
    ) -> Result<(), DiskError> {
        let start = range.start().get();
        let n = (data.len() / BLOCK_SIZE) as u64;
        let held = {
            let mut st = self.st();
            st.stats.writes += 1;
            if st.offline {
                st.stats.failed_writes += 1;
                return Err(OFFLINE.to_error());
            }
            if Self::targeted(&st, start, n) {
                if let Some(left) = st.fail_writes_after {
                    if left == 0 {
                        st.stats.failed_writes += 1;
                        return Err(st.write_error.unwrap_or(FaultError::Io).to_error());
                    }
                    st.fail_writes_after = Some(left - 1);
                }
                if st.drop_writes {
                    st.stats.dropped_writes += 1;
                    return Ok(());
                }
                if let Some(reply) = st.defer_writes {
                    st.deferred.push(Deferred {
                        start,
                        data: data.to_vec(),
                    });
                    return match reply {
                        None => Ok(()),
                        Some(e) => {
                            st.stats.failed_writes += 1;
                            Err(e.to_error())
                        }
                    };
                }
                if st.hold_writes {
                    let (ack, rx) = oneshot::channel();
                    st.held.push_back(Held {
                        write: Deferred {
                            start,
                            data: data.to_vec(),
                        },
                        ack,
                    });
                    self.held.send_modify(|h| *h += 1);
                    Some(rx)
                } else {
                    None
                }
            } else {
                None
            }
        };
        match held {
            // Landed by `release_*`.
            Some(rx) => rx.await.map_err(|_| OFFLINE.to_error()),
            None => {
                let mut st = self.st();
                self.persist_run(&mut st, start, data);
                Ok(())
            }
        }
    }

    pub(super) async fn flush(&self) -> Result<(), DiskError> {
        let mut gate = self.hold_flushes.subscribe();
        if *gate.borrow_and_update() {
            self.flushes_held.send_modify(|n| *n += 1);
            let _ = gate.wait_for(|&on| !on).await;
            self.flushes_held.send_modify(|n| *n -= 1);
        }
        let mut st = self.st();
        st.stats.flushes += 1;
        if st.offline {
            st.stats.failed_flushes += 1;
            return Err(OFFLINE.to_error());
        }
        if let Some(e) = st.fail_flushes {
            st.stats.failed_flushes += 1;
            return Err(e.to_error());
        }
        if let Some(cache) = st.cache.as_mut().map(std::mem::take) {
            for (b, data) in cache {
                self.persist(&mut st, b, &data);
            }
        }
        self.file.sync_data()?;
        Ok(())
    }
}
