//! Copy-on-write object trees.
//!
//! An object's contents are a sparse array of fixed-size records (`blksz`)
//! indexed by a tree of `BlockPtr`s:
//!
//! ```text
//!   root ──► L(n) indirect ──► … ──► L1 indirect ──► L0 data record
//! ```
//!
//! Each indirect block holds `INDIRECT_FANOUT` optional pointers; `None` is a
//! hole that reads as zeros, so a new object allocates nothing. A tree of
//! no levels is a single record (record 0), its root pointing straight at it. Every record
//! and indirect block is checksummed by its parent like any other object.
//!
//! Writes land in an in-memory map of dirty records. A sync (`build_tree`)
//! writes them to newly allocated space and rewrites only the indirect blocks
//! on their paths (bottom up); the caller points whatever owns the tree at
//! the new root and frees every replaced block (deferred, so the previous
//! uberblocks stay readable). Nothing is ever overwritten in place.
//!
//! An object is either fixed-size (a volume: writes past the end are cut
//! off) or growable (a file: writes past the end extend it, the tree gains
//! levels as needed, and it can be truncated).
//!
//! Reads look at, in order: dirty records, records being synced, the tree.

use crate::blkptr::BlockPtr;
use crate::disk::BLOCK_SIZE;
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Extent;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const INDIRECT_FANOUT: u64 = 512;
const INDIRECT_CACHE_ENTRIES: usize = 1024;
/// Clean data records kept in memory per object.
pub const RECORD_CACHE_BYTES: usize = 64 << 20;
const SYNC_WRITE_CONCURRENCY: usize = 32;
/// Consecutive dirty records are written as one contiguous run of at most
/// this many blocks (one allocation, one device write).
pub const RUN_MAX_BLOCKS: u64 = 256;
/// An indirect block on disk, at most: 512 `Option<BlockPtr>` at <= 57
/// bytes each, plus the length.
const INDIRECT_MAX_BYTES: u64 = INDIRECT_FANOUT * 57 + 8;

/// A write job of a sync: a run of consecutive records in space allocated
/// up front, or a single record allocated on its own.
enum SyncJob<'a> {
    Run {
        start: u64,
        len: u64,
        records: Vec<(u64, &'a [u8])>,
    },
    Single(u64, &'a [u8]),
}

pub(crate) type Indirect = Vec<Option<BlockPtr>>;

#[derive(Debug, Clone)]
pub(crate) enum Dirty {
    Data(Vec<u8>),
    Hole,
}

impl Dirty {
    pub(crate) fn is_hole(&self) -> bool {
        match self {
            Dirty::Hole => true,
            Dirty::Data(b) => b.iter().all(|&x| x == 0),
        }
    }
}

#[derive(Default)]
pub(crate) struct DirtyState {
    pub(crate) records: BTreeMap<u64, Dirty>,
    /// Records taken by an in-progress sync (readable until it finishes).
    pub(crate) syncing: Option<Arc<BTreeMap<u64, Dirty>>>,
    /// Dirty records changed since they were last written to the intent
    /// log (a writer's flush logs exactly these, see `crate::zil`).
    pub(crate) unlogged: std::collections::BTreeSet<u64>,
}

impl DirtyState {
    pub(crate) fn get(&self, idx: u64) -> Option<&Dirty> {
        self.records
            .get(&idx)
            .or_else(|| self.syncing.as_ref().and_then(|s| s.get(&idx)))
    }

    /// Unsynced records at `idx` or later: whether each holds data.
    fn from(&self, idx: u64) -> BTreeMap<u64, bool> {
        let mut out = BTreeMap::new();
        // Being synced first; newer writes over them.
        if let Some(syncing) = &self.syncing {
            for (&i, d) in syncing.range(idx..) {
                out.insert(i, matches!(d, Dirty::Data(_)));
            }
        }
        for (&i, d) in self.records.range(idx..) {
            out.insert(i, matches!(d, Dirty::Data(_)));
        }
        out
    }
}

/// Committed data records, keyed like the indirect cache by (lba, birth_txg)
/// so entries never go stale. Evicts oldest-inserted first, bounded by bytes.
#[derive(Default)]
struct RecordCache {
    map: HashMap<(u64, u64), Arc<Vec<u8>>>,
    order: std::collections::VecDeque<(u64, u64)>,
    bytes: usize,
}

impl RecordCache {
    fn get(&self, key: (u64, u64)) -> Option<Arc<Vec<u8>>> {
        self.map.get(&key).cloned()
    }

    fn insert(&mut self, key: (u64, u64), data: Arc<Vec<u8>>) {
        if self.map.contains_key(&key) {
            return;
        }
        self.bytes += data.len();
        self.map.insert(key, data);
        self.order.push_back(key);
        while self.bytes > RECORD_CACHE_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(d) = self.map.remove(&old) {
                self.bytes -= d.len();
            }
        }
    }
}

/// The committed shape of a tree: its root and how many indirect levels
/// sit above the data records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TreeRoot {
    pub ptr: Option<BlockPtr>,
    pub levels: u8,
}

impl TreeRoot {
    /// Nodes at `level` (0 = data records) the tree can address.
    fn span(&self, level: u8) -> u64 {
        span(self.levels.saturating_sub(level))
    }
}

/// `INDIRECT_FANOUT ^ levels`, saturating.
fn span(levels: u8) -> u64 {
    INDIRECT_FANOUT.saturating_pow(levels as u32)
}

/// Indirect levels needed to address `records` records. A single record
/// needs none: the root points straight at it.
pub(crate) fn levels_for(records: u64) -> u8 {
    let mut levels = 0;
    while span(levels) < records {
        levels += 1;
    }
    levels
}

/// Whether writes may move an object's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sizing {
    /// A volume: the size is set at creation; writes past it are cut off.
    Fixed,
    /// A file: writes past the end extend it, and it can be truncated.
    Growable,
}

/// An object's contents in memory: the committed tree, dirty records not
/// yet synced into it, and caches of committed records and indirects.
pub struct ObjTree {
    pub blksz: usize,
    pub sizing: Sizing,
    size: AtomicU64,
    root: Mutex<TreeRoot>,
    pub(crate) dirty: Mutex<DirtyState>,
    /// Indirect blocks keyed by (lba, birth_txg): immutable once written, and
    /// a reused lba always has a newer birth TXG.
    cache: Mutex<HashMap<(u64, u64), Arc<Indirect>>>,
    records: Mutex<RecordCache>,
}

impl ObjTree {
    pub fn new(blksz: usize, sizing: Sizing, size: u64, root: TreeRoot) -> Self {
        assert!(blksz > 0);
        ObjTree {
            blksz,
            sizing,
            size: AtomicU64::new(size),
            root: Mutex::new(root),
            dirty: Mutex::new(DirtyState::default()),
            cache: Mutex::new(HashMap::new()),
            records: Mutex::new(RecordCache::default()),
        }
    }

    /// The logical size in bytes (dirty writes included).
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst)
    }

    /// The committed tree.
    pub fn root(&self) -> TreeRoot {
        self.root.lock().unwrap().clone()
    }

    /// The root pointer of the committed tree.
    pub(crate) fn root_ptr(&self) -> Option<BlockPtr> {
        self.root.lock().unwrap().ptr.clone()
    }

    pub fn levels(&self) -> u8 {
        self.root.lock().unwrap().levels
    }

    /// Switch readers to a newly committed tree. A tree never loses levels.
    pub(crate) fn set_root(&self, root: TreeRoot) {
        let mut r = self.root.lock().unwrap();
        debug_assert!(root.levels >= r.levels, "tree lost levels");
        *r = root;
    }

    /// Switch readers to a new root pointer at the current height.
    pub(crate) fn set_root_ptr(&self, ptr: Option<BlockPtr>) {
        self.root.lock().unwrap().ptr = ptr;
    }

    pub fn dirty_bytes(&self) -> usize {
        let d = self.dirty.lock().unwrap();
        d.records
            .values()
            .filter(|r| matches!(r, Dirty::Data(_)))
            .count()
            * self.blksz
    }

    pub fn is_dirty(&self) -> bool {
        !self.dirty.lock().unwrap().records.is_empty()
    }

    /// Replace record `idx` whole (`Data` is zero-padded to `blksz`).
    pub(crate) fn put_record(&self, idx: u64, rec: Dirty) {
        let rec = match rec {
            Dirty::Data(mut b) => {
                assert!(b.len() <= self.blksz, "record larger than blksz");
                b.resize(self.blksz, 0);
                Dirty::Data(b)
            }
            Dirty::Hole => Dirty::Hole,
        };
        let mut d = self.dirty.lock().unwrap();
        d.records.insert(idx, rec);
        d.unlogged.insert(idx);
    }

    /// Point at a different committed tree, dropping nothing else: for a
    /// process that doesn't commit itself, catching up with what another
    /// committed. Never dirty there.
    pub(crate) fn follow_root(&self, root: TreeRoot) {
        debug_assert!(!self.is_dirty(), "following a root with dirty records");
        *self.root.lock().unwrap() = root;
    }

    /// Take the dirty records for a sync. They stay readable (as
    /// `syncing`) until `end_sync`.
    pub(crate) fn begin_sync(&self) -> Option<Arc<BTreeMap<u64, Dirty>>> {
        let mut d = self.dirty.lock().unwrap();
        if d.records.is_empty() {
            return None;
        }
        let taken = Arc::new(std::mem::take(&mut d.records));
        d.syncing = Some(taken.clone());
        Some(taken)
    }

    /// Finish a sync of `taken`: records not in `synced` (all of them when
    /// None) become dirty again unless rewritten meanwhile.
    pub(crate) fn end_sync(
        &self,
        taken: &BTreeMap<u64, Dirty>,
        synced: Option<&BTreeMap<u64, Dirty>>,
    ) {
        let mut d = self.dirty.lock().unwrap();
        d.syncing = None;
        // Anything not synced goes back; newer writes win over it.
        for (k, v) in taken.iter() {
            if synced.is_none_or(|s| !s.contains_key(k)) {
                d.records.entry(*k).or_insert_with(|| v.clone());
            }
        }
        // Synced and not written since: nothing left to log.
        let DirtyState {
            records, unlogged, ..
        } = &mut *d;
        unlogged.retain(|k| records.contains_key(k));
    }
}

impl<D: BlockDevice> FileSystem<D> {
    pub(crate) async fn load_indirect(
        &self,
        obj: &ObjTree,
        ptr: &BlockPtr,
    ) -> Result<Arc<Indirect>, FileSystemError> {
        let key = (ptr.lba, ptr.birth_txg);
        if let Some(ind) = obj.cache.lock().unwrap().get(&key) {
            return Ok(ind.clone());
        }
        let ind: Indirect = bincode::deserialize(&self.read_record(ptr).await?)?;
        let ind = Arc::new(ind);
        let mut cache = obj.cache.lock().unwrap();
        if cache.len() >= INDIRECT_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(key, ind.clone());
        Ok(ind)
    }

    /// Pointer to node `index` at `level` (0 = data records) of `root`.
    /// Nodes beyond what the tree addresses are holes.
    pub(crate) async fn tree_lookup(
        &self,
        obj: &ObjTree,
        root: &TreeRoot,
        level: u8,
        index: u64,
    ) -> Result<Option<BlockPtr>, FileSystemError> {
        let Some(mut ptr) = root.ptr.clone() else {
            return Ok(None);
        };
        if level > root.levels || index >= root.span(level) {
            return Ok(None);
        }
        for l in (level + 1..=root.levels).rev() {
            let ind = self.load_indirect(obj, &ptr).await?;
            let span = INDIRECT_FANOUT.pow((l - 1 - level) as u32);
            let slot = ((index / span) % INDIRECT_FANOUT) as usize;
            match &ind[slot] {
                Some(child) => ptr = child.clone(),
                None => return Ok(None),
            }
        }
        Ok(Some(ptr))
    }

    /// Committed contents of record `idx` (None = hole).
    pub(crate) async fn read_committed_record(
        &self,
        obj: &ObjTree,
        idx: u64,
    ) -> Result<Option<Arc<Vec<u8>>>, FileSystemError> {
        let Some(ptr) = self.tree_lookup(obj, &obj.root(), 0, idx).await? else {
            return Ok(None);
        };
        let key = (ptr.lba, ptr.birth_txg);
        if let Some(data) = obj.records.lock().unwrap().get(key) {
            return Ok(Some(data));
        }
        let mut data = self.read_record(&ptr).await?;
        data.resize(obj.blksz, 0);
        let data = Arc::new(data);
        obj.records.lock().unwrap().insert(key, data.clone());
        Ok(Some(data))
    }

    /// Read from `obj` at byte `offset`, up to its end. Returns the bytes read.
    pub(crate) async fn object_read(
        &self,
        obj: &ObjTree,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        let size = obj.size();
        if offset >= size {
            return Ok(0);
        }
        let len = ((size - offset) as usize).min(buf.len());
        if len == 0 {
            return Ok(0);
        }
        let bs = obj.blksz;

        // Fetch every committed record the range needs up front, so records
        // that are adjacent on disk are read together.
        let first = offset / bs as u64;
        let last = (offset + len as u64 - 1) / bs as u64;
        let fetched = if last > first {
            self.prefetch_records(obj, first..=last).await?
        } else {
            HashMap::new()
        };

        let mut done = 0;
        while done < len {
            let pos = offset + done as u64;
            let idx = pos / bs as u64;
            let at = (pos % bs as u64) as usize;
            let n = (bs - at).min(len - done);
            let out = &mut buf[done..done + n];

            let pending = obj.dirty.lock().unwrap().get(idx).cloned();
            match pending {
                Some(Dirty::Data(d)) => out.copy_from_slice(&d[at..at + n]),
                Some(Dirty::Hole) => out.fill(0),
                None => {
                    let committed = match fetched.get(&idx) {
                        Some(d) => d.clone(),
                        None => self.read_committed_record(obj, idx).await?,
                    };
                    match committed {
                        Some(d) => out.copy_from_slice(&d[at..at + n]),
                        None => out.fill(0),
                    }
                }
            }
            done += n;
        }
        Ok(done)
    }

    /// Committed contents of the records in `range` that aren't dirty (None
    /// = hole), reading uncached ones with coalesced reads.
    async fn prefetch_records(
        &self,
        obj: &ObjTree,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<HashMap<u64, Option<Arc<Vec<u8>>>>, FileSystemError> {
        let root = obj.root();
        let mut out = HashMap::new();
        let mut need: Vec<(u64, BlockPtr)> = Vec::new();
        for idx in range {
            if obj.dirty.lock().unwrap().get(idx).is_some() {
                continue;
            }
            match self.tree_lookup(obj, &root, 0, idx).await? {
                None => {
                    out.insert(idx, None);
                }
                Some(ptr) => match obj.records.lock().unwrap().get((ptr.lba, ptr.birth_txg)) {
                    Some(d) => {
                        out.insert(idx, Some(d));
                    }
                    None => need.push((idx, ptr)),
                },
            }
        }
        let ptrs: Vec<BlockPtr> = need.iter().map(|(_, p)| p.clone()).collect();
        let datas = self.read_records(&ptrs).await?;
        let mut cache = obj.records.lock().unwrap();
        for ((idx, ptr), mut data) in need.into_iter().zip(datas) {
            data.resize(obj.blksz, 0);
            let data = Arc::new(data);
            cache.insert((ptr.lba, ptr.birth_txg), data.clone());
            out.insert(idx, Some(data));
        }
        Ok(out)
    }

    /// Apply `data` (or zeros when `None`) at `offset`. A fixed-size object
    /// takes only what fits; a growable one is extended to cover the range.
    /// Returns the bytes applied.
    pub(crate) async fn object_modify(
        &self,
        obj: &ObjTree,
        offset: u64,
        len: usize,
        data: Option<&[u8]>,
    ) -> Result<usize, FileSystemError> {
        let len = match obj.sizing {
            Sizing::Fixed => {
                let size = obj.size();
                if offset >= size {
                    return Ok(0);
                }
                ((size - offset) as usize).min(len)
            }
            Sizing::Growable => len,
        };
        if len == 0 {
            return Ok(0);
        }
        let bs = obj.blksz;

        let mut done = 0;
        while done < len {
            let pos = offset + done as u64;
            let idx = pos / bs as u64;
            let at = (pos % bs as u64) as usize;
            let n = (bs - at).min(len - done);
            let src = data.map(|d| &d[done..done + n]);
            let apply = |buf: &mut Vec<u8>| match src {
                Some(src) => buf[at..at + n].copy_from_slice(src),
                None => buf[at..at + n].fill(0),
            };

            if n == bs {
                // Whole record: no need for the old contents.
                let rec = match src {
                    Some(src) => Dirty::Data(src.to_vec()),
                    None => Dirty::Hole,
                };
                let mut d = obj.dirty.lock().unwrap();
                d.records.insert(idx, rec);
                d.unlogged.insert(idx);
            } else {
                loop {
                    {
                        let mut d = obj.dirty.lock().unwrap();
                        if let Some(Dirty::Data(buf)) = d.records.get_mut(&idx) {
                            apply(buf);
                            d.unlogged.insert(idx);
                            break;
                        }
                        // Base on what's being synced, if anything.
                        let base = match d.get(idx) {
                            Some(Dirty::Data(b)) => Some(b.clone()),
                            Some(Dirty::Hole) => Some(vec![0u8; bs]),
                            None => None,
                        };
                        if let Some(mut buf) = base {
                            apply(&mut buf);
                            d.records.insert(idx, Dirty::Data(buf));
                            d.unlogged.insert(idx);
                            break;
                        }
                    }
                    // Read-modify-write against the committed tree.
                    let mut buf = match self.read_committed_record(obj, idx).await? {
                        Some(committed) => committed.as_ref().clone(),
                        None => vec![0u8; bs],
                    };
                    let mut d = obj.dirty.lock().unwrap();
                    if d.get(idx).is_some() {
                        continue; // raced with another writer; retry on top of theirs
                    }
                    apply(&mut buf);
                    d.records.insert(idx, Dirty::Data(buf));
                    d.unlogged.insert(idx);
                    break;
                }
            }
            done += n;
        }
        if obj.sizing == Sizing::Growable {
            obj.size.fetch_max(offset + len as u64, Ordering::SeqCst);
        }
        Ok(done)
    }

    /// Set a growable object's size. Shrinking zeroes the rest of the last
    /// record and turns every record past it into a hole (freed on the next
    /// sync), so growing again later reads zeros there. Callers serialize
    /// changes to one object (its writer holds its lease).
    pub(crate) async fn object_truncate(
        &self,
        obj: &ObjTree,
        new_size: u64,
    ) -> Result<(), FileSystemError> {
        if obj.sizing != Sizing::Growable {
            return Err(FileSystemError::InvalidArgument(
                "a fixed-size object can't be truncated".into(),
            ));
        }
        let old = obj.size();
        if new_size < old {
            let bs = obj.blksz as u64;
            let tail = new_size % bs;
            if tail != 0 {
                let end = (new_size - tail + bs).min(old);
                self.object_modify(obj, new_size, (end - new_size) as usize, None)
                    .await?;
            }
            let first_gone = new_size.div_ceil(bs);
            let committed = self.tree_leaves_from(obj, &obj.root(), first_gone).await?;
            let mut d = obj.dirty.lock().unwrap();
            let mut gone: Vec<u64> = d.records.split_off(&first_gone).into_keys().collect();
            if let Some(s) = &d.syncing {
                gone.extend(s.range(first_gone..).map(|(k, _)| *k));
            }
            gone.extend(committed.into_iter().map(|(i, _)| i));
            for idx in gone {
                d.records.insert(idx, Dirty::Hole);
                d.unlogged.insert(idx);
            }
        }
        obj.size.store(new_size, Ordering::SeqCst);
        Ok(())
    }

    /// Blocks a sync of `records` may allocate for the tree, worst case
    /// (not counting whatever the owner rewrites to point at it).
    pub(crate) fn tree_sync_reservation(
        &self,
        obj: &ObjTree,
        records: &BTreeMap<u64, Dirty>,
    ) -> u64 {
        let align = |b: u64| self.align_blocks(b);
        let record_blocks = align((obj.blksz / BLOCK_SIZE) as u64);
        let indirect_blocks = align(INDIRECT_MAX_BYTES.div_ceil(BLOCK_SIZE as u64));
        let data = records.values().filter(|r| !r.is_hole()).count() as u64 * record_blocks;
        let levels = target_levels(obj.levels(), records);
        let mut indirects = 0u64;
        let mut span = 1u64;
        for _ in 1..=levels {
            span = span.saturating_mul(INDIRECT_FANOUT);
            let mut last = None;
            for idx in records.keys() {
                let parent = idx / span;
                if last != Some(parent) {
                    indirects += 1;
                    last = Some(parent);
                }
            }
        }
        // Growing adds a node per new level on the leftmost path.
        let grown = (levels - obj.levels()) as u64;
        data + (indirects + grown) * indirect_blocks
    }

    /// Where to place a stretch of records starting at index `first`: after
    /// the neighbour it follows, else after whatever this sync placed last
    /// (`prev_end`: last index, end lba), else near where it used to be.
    async fn run_hint(
        &self,
        obj: &ObjTree,
        old_root: &TreeRoot,
        first: u64,
        prev_end: Option<(u64, u64)>,
    ) -> Result<Option<u64>, FileSystemError> {
        if let Some((last, end)) = prev_end
            && last + 1 == first
        {
            return Ok(Some(end));
        }
        let before = match first {
            0 => None,
            _ => self.tree_lookup(obj, old_root, 0, first - 1).await?,
        };
        Ok(match (before, prev_end) {
            (Some(p), _) => Some(p.lba + p.asize as u64),
            (None, Some((_, end))) => Some(end),
            (None, None) => self
                .tree_lookup(obj, old_root, 0, first)
                .await?
                .map(|p| p.lba),
        })
    }

    /// Give back the space pre-allocated for runs that were never written.
    async fn release_jobs(&self, jobs: &[SyncJob<'_>]) {
        for job in jobs {
            if let SyncJob::Run { start, len, .. } = job {
                self.unallocate(*start, *len).await;
            }
        }
    }

    /// Give back space allocated (in the open TXG) and never committed.
    pub(crate) async fn release_ptrs(&self, ptrs: &[BlockPtr]) {
        for p in ptrs {
            let e = p.extent();
            self.unallocate(e.start, e.len).await;
        }
    }

    /// Raise `root` to `levels`: each new level is an indirect block whose
    /// first slot is the level below. An empty tree just gets taller.
    async fn grow_root(
        &self,
        obj: &ObjTree,
        root: TreeRoot,
        levels: u8,
        allocated: &mut Vec<BlockPtr>,
    ) -> Result<TreeRoot, FileSystemError> {
        let mut root = root;
        while root.levels < levels {
            if let Some(ptr) = root.ptr.take() {
                let mut ind: Indirect = vec![None; INDIRECT_FANOUT as usize];
                ind[0] = Some(ptr);
                let new = self.write_record(&bincode::serialize(&ind)?).await?;
                allocated.push(new.clone());
                obj.cache
                    .lock()
                    .unwrap()
                    .insert((new.lba, new.birth_txg), Arc::new(ind));
                root.ptr = Some(new);
            }
            root.levels += 1;
        }
        Ok(root)
    }

    /// Write `records` and the indirect blocks above them. Returns the new
    /// tree without changing anything visible. Everything written is pushed
    /// to `allocated` (for the caller to release on failure) and every block
    /// the new tree no longer references to `replaced` (for the caller to
    /// free once the new root is committed).
    pub(crate) async fn build_tree(
        &self,
        obj: &ObjTree,
        records: &BTreeMap<u64, Dirty>,
        allocated: &mut Vec<BlockPtr>,
        replaced: &mut Vec<BlockPtr>,
    ) -> Result<TreeRoot, FileSystemError> {
        // 0. Make room for the highest index (growable objects only).
        let committed = obj.root();
        let levels = target_levels(committed.levels, records);
        let old_root = if levels > committed.levels {
            debug_assert_eq!(
                obj.sizing,
                Sizing::Growable,
                "fixed-size object outgrew its tree"
            );
            self.grow_root(obj, committed.clone(), levels, allocated)
                .await?
        } else {
            committed.clone()
        };

        // 1. Data records. Each stretch of consecutive indices gets one
        //    contiguous allocation (placed after its neighbour where possible)
        //    and is written in runs of up to RUN_MAX_BLOCKS, one device write
        //    each, concurrently. All-zero records become holes.
        let record_blocks = self.align_blocks(obj.blksz.div_ceil(BLOCK_SIZE) as u64);
        let run_max = (RUN_MAX_BLOCKS / record_blocks).max(1) as usize;
        let mut leaves: Vec<(u64, Option<BlockPtr>)> = Vec::with_capacity(records.len());
        let mut runs: Vec<Vec<(u64, &[u8])>> = Vec::new();
        for (&idx, rec) in records {
            match rec {
                Dirty::Data(buf) if buf.iter().any(|&b| b != 0) => match runs.last_mut() {
                    Some(run) if run.last().is_some_and(|(i, _)| i + 1 == idx) => {
                        run.push((idx, buf))
                    }
                    _ => runs.push(vec![(idx, buf)]),
                },
                _ => leaves.push((idx, None)),
            }
        }
        // Allocate every run up front, in index order, so placement doesn't
        // depend on which write finishes first.
        let mut jobs: Vec<SyncJob<'_>> = Vec::with_capacity(runs.len());
        let mut prev_end: Option<(u64, u64)> = None; // (last index, end lba)
        for run in runs {
            let first = run[0].0;
            let hint = match self.run_hint(obj, &old_root, first, prev_end).await {
                Ok(h) => h,
                Err(e) => {
                    self.release_jobs(&jobs).await;
                    return Err(e);
                }
            };
            // The whole stretch in one piece if possible, else piece by piece.
            let pieces: Vec<&[(u64, &[u8])]> = match self
                .allocate_run(run.len() as u64 * record_blocks, hint)
                .await
            {
                Ok(e) => {
                    let mut at = e.start;
                    for chunk in run.chunks(run_max) {
                        let len = chunk.len() as u64 * record_blocks;
                        jobs.push(SyncJob::Run {
                            start: at,
                            len,
                            records: chunk.to_vec(),
                        });
                        at += len;
                    }
                    prev_end = Some((run[run.len() - 1].0, e.start + e.len));
                    continue;
                }
                Err(FileSystemError::UnableToAllocateBlocks(_)) => run.chunks(run_max).collect(),
                Err(e) => {
                    self.release_jobs(&jobs).await;
                    return Err(e);
                }
            };
            for chunk in pieces {
                let len = chunk.len() as u64 * record_blocks;
                let hint = prev_end.map(|(_, end)| end).or(hint);
                match self.allocate_run(len, hint).await {
                    Ok(e) => {
                        prev_end = Some((chunk[chunk.len() - 1].0, e.start + e.len));
                        jobs.push(SyncJob::Run {
                            start: e.start,
                            len: e.len,
                            records: chunk.to_vec(),
                        });
                    }
                    Err(FileSystemError::UnableToAllocateBlocks(_)) => {
                        jobs.extend(chunk.iter().map(|&(i, b)| SyncJob::Single(i, b)));
                    }
                    Err(e) => {
                        self.release_jobs(&jobs).await;
                        return Err(e);
                    }
                }
            }
        }

        // (Futures are boxed explicitly: a combinator chain over borrowed
        // records trips rustc's higher-ranked Send inference, #100013.)
        type Job<'a> = Pin<
            Box<dyn Future<Output = Result<Vec<(u64, BlockPtr)>, FileSystemError>> + Send + 'a>,
        >;
        let mut pending = jobs.into_iter();
        let mut in_flight = FuturesUnordered::new();
        let mut failed = None;
        loop {
            while failed.is_none() && in_flight.len() < SYNC_WRITE_CONCURRENCY {
                let job: Job<'_> = match pending.next() {
                    Some(SyncJob::Run {
                        start,
                        len,
                        records,
                    }) => Box::pin(async move {
                        let datas: Vec<&[u8]> = records.iter().map(|(_, b)| *b).collect();
                        match self.write_records_at(start, record_blocks, &datas).await {
                            Ok(ptrs) => Ok(records.iter().map(|(i, _)| *i).zip(ptrs).collect()),
                            Err(e) => {
                                self.unallocate(start, len).await;
                                Err(e)
                            }
                        }
                    }),
                    Some(SyncJob::Single(idx, buf)) => {
                        Box::pin(async move { Ok(vec![(idx, self.write_record(buf).await?)]) })
                    }
                    None => break,
                };
                in_flight.push(job);
            }
            // On failure stop scheduling, but drain what's in flight so every
            // successful allocation is recorded and can be released.
            match in_flight.next().await {
                Some(Ok(written)) => {
                    for (idx, ptr) in written {
                        allocated.push(ptr.clone());
                        leaves.push((idx, Some(ptr)));
                    }
                }
                Some(Err(e)) => {
                    failed.get_or_insert(e);
                }
                None => break,
            }
        }
        if let Some(e) = failed {
            let rest: Vec<SyncJob<'_>> = pending.collect();
            self.release_jobs(&rest).await;
            return Err(e);
        }

        // 2. Indirect blocks, bottom up.
        let changes: BTreeMap<u64, Option<BlockPtr>> = leaves.into_iter().collect();
        self.build_indirects(obj, &old_root, changes, allocated, replaced)
            .await
    }

    /// Rewrite the indirect blocks of `old_root` above `changes` (data
    /// record index -> new pointer, None = hole), bottom up. Returns the new
    /// tree; `allocated` and `replaced` as for `build_tree`.
    pub(crate) async fn build_indirects(
        &self,
        obj: &ObjTree,
        old_root: &TreeRoot,
        mut changes: BTreeMap<u64, Option<BlockPtr>>,
        allocated: &mut Vec<BlockPtr>,
        replaced: &mut Vec<BlockPtr>,
    ) -> Result<TreeRoot, FileSystemError> {
        for level in 1..=old_root.levels {
            let mut parents: BTreeMap<u64, Vec<(u64, Option<BlockPtr>)>> = BTreeMap::new();
            for (child, ptr) in changes {
                parents
                    .entry(child / INDIRECT_FANOUT)
                    .or_default()
                    .push((child, ptr));
            }

            let mut next = BTreeMap::new();
            for (parent, children) in parents {
                let old = self.tree_lookup(obj, old_root, level, parent).await?;
                let mut ind: Indirect = match &old {
                    Some(ptr) => (*self.load_indirect(obj, ptr).await?).clone(),
                    None => vec![None; INDIRECT_FANOUT as usize],
                };
                for (child, ptr) in children {
                    let slot = (child % INDIRECT_FANOUT) as usize;
                    if let Some(old_child) = std::mem::replace(&mut ind[slot], ptr) {
                        replaced.push(old_child);
                    }
                }
                let new = if ind.iter().all(Option::is_none) {
                    None
                } else {
                    let ptr = self.write_record(&bincode::serialize(&ind)?).await?;
                    allocated.push(ptr.clone());
                    obj.cache
                        .lock()
                        .unwrap()
                        .insert((ptr.lba, ptr.birth_txg), Arc::new(ind));
                    Some(ptr)
                };
                next.insert(parent, new);
            }
            changes = next;
        }

        let new_ptr = changes.remove(&0).unwrap_or(old_root.ptr.clone());
        if let Some(old) = &old_root.ptr
            && new_ptr.as_ref() != Some(old)
        {
            replaced.push(old.clone());
        }
        Ok(TreeRoot {
            ptr: new_ptr,
            levels: old_root.levels,
        })
    }

    /// Blocks the indirect blocks of a new tree over `records` data
    /// records may take, worst case.
    pub(crate) fn fresh_indirect_blocks(&self, records: u64) -> u64 {
        let indirect = self.align_blocks(INDIRECT_MAX_BYTES.div_ceil(BLOCK_SIZE as u64));
        let mut n = 0;
        let mut at = records;
        for _ in 0..levels_for(records) {
            at = at.div_ceil(INDIRECT_FANOUT);
            n += at.max(1);
        }
        n * indirect
    }

    /// Build a new tree over data records already written: `leaves[i]` is
    /// record `i`. Returns the tree and pushes its indirect blocks to
    /// `allocated`.
    pub(crate) async fn fresh_tree(
        &self,
        leaves: &[BlockPtr],
        allocated: &mut Vec<BlockPtr>,
    ) -> Result<TreeRoot, FileSystemError> {
        let empty = TreeRoot {
            ptr: None,
            levels: levels_for(leaves.len() as u64),
        };
        if leaves.is_empty() {
            return Ok(empty);
        }
        let scratch = ObjTree::new(BLOCK_SIZE, Sizing::Growable, 0, empty.clone());
        let changes = leaves
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, Some(p.clone())))
            .collect();
        let mut replaced = Vec::new();
        self.build_indirects(&scratch, &empty, changes, allocated, &mut replaced)
            .await
    }

    /// Write `data` as a new tree of `blksz`-byte records (the last one
    /// short). Pushes everything written to `allocated`.
    pub(crate) async fn write_tree(
        &self,
        blksz: usize,
        data: &[u8],
        allocated: &mut Vec<BlockPtr>,
    ) -> Result<TreeRoot, FileSystemError> {
        let mut leaves = Vec::with_capacity(data.len().div_ceil(blksz));
        for chunk in data.chunks(blksz) {
            let ptr = self.write_record(chunk).await?;
            allocated.push(ptr.clone());
            leaves.push(ptr);
        }
        self.fresh_tree(&leaves, allocated).await
    }

    /// The first `size` bytes of the tree at `root` (holes read as zeros).
    pub(crate) async fn read_tree(
        &self,
        root: &TreeRoot,
        blksz: usize,
        size: u64,
    ) -> Result<Vec<u8>, FileSystemError> {
        let mut out = vec![0u8; size as usize];
        let leaves = self.tree_leaves(root).await?;
        let ptrs: Vec<BlockPtr> = leaves.iter().map(|(_, p)| p.clone()).collect();
        for ((idx, _), data) in leaves.iter().zip(self.read_records(&ptrs).await?) {
            let at = (*idx as usize).saturating_mul(blksz);
            if at >= out.len() {
                continue;
            }
            let n = data.len().min(out.len() - at);
            out[at..at + n].copy_from_slice(&data[..n]);
        }
        Ok(out)
    }

    /// Record `idx` of `obj` as readers see it (dirty first; None = hole).
    pub(crate) async fn object_record(
        &self,
        obj: &ObjTree,
        idx: u64,
    ) -> Result<Option<Arc<Vec<u8>>>, FileSystemError> {
        let pending = obj.dirty.lock().unwrap().get(idx).cloned();
        match pending {
            Some(Dirty::Data(d)) => Ok(Some(Arc::new(d))),
            Some(Dirty::Hole) => Ok(None),
            None => self.read_committed_record(obj, idx).await,
        }
    }

    /// Every extent a tree occupies (indirects and data records).
    pub(crate) async fn tree_extents(
        &self,
        root: &TreeRoot,
    ) -> Result<Vec<Extent>, FileSystemError> {
        let mut out = Vec::new();
        let mut stack: Vec<(BlockPtr, u8)> =
            root.ptr.iter().map(|r| (r.clone(), root.levels)).collect();
        while let Some((ptr, level)) = stack.pop() {
            out.push(ptr.extent());
            if level > 0 {
                let ind: Indirect = bincode::deserialize(&self.read_record(&ptr).await?)?;
                stack.extend(ind.into_iter().flatten().map(|c| (c, level - 1)));
            }
        }
        Ok(out)
    }

    /// Every allocated data record of a committed tree, in index order.
    pub(crate) async fn tree_leaves(
        &self,
        root: &TreeRoot,
    ) -> Result<Vec<(u64, BlockPtr)>, FileSystemError> {
        let mut out = Vec::new();
        // Depth first, children pushed in reverse so they pop in index order.
        let mut stack: Vec<(BlockPtr, u8, u64)> = root
            .ptr
            .iter()
            .map(|r| (r.clone(), root.levels, 0))
            .collect();
        while let Some((ptr, level, base)) = stack.pop() {
            if level == 0 {
                out.push((base, ptr));
                continue;
            }
            let ind: Indirect = bincode::deserialize(&self.read_record(&ptr).await?)?;
            let span = INDIRECT_FANOUT.pow((level - 1) as u32);
            for (slot, child) in ind.into_iter().enumerate().rev() {
                if let Some(c) = child {
                    stack.push((c, level - 1, base + slot as u64 * span));
                }
            }
        }
        Ok(out)
    }

    /// The allocated data records of a committed tree at index `first` or
    /// later, in index order (subtrees wholly before `first` aren't read).
    /// The first byte at `offset` or after that is data (`data`) or in a
    /// hole, by record (unsynced writes included); None: there is none
    /// before the end. `offset` must be within the object.
    pub(crate) async fn object_seek(
        &self,
        obj: &ObjTree,
        offset: u64,
        data: bool,
    ) -> Result<Option<u64>, FileSystemError> {
        let size = obj.size();
        let bs = obj.blksz as u64;
        let (start, last) = (offset / bs, size.saturating_sub(1) / bs);
        let mut held: std::collections::BTreeSet<u64> = self
            .tree_leaves_from(obj, &obj.root(), start)
            .await?
            .into_iter()
            .map(|(i, _)| i)
            .filter(|&i| i <= last)
            .collect();
        let dirty = obj.dirty.lock().unwrap().from(start);
        for (i, is_data) in dirty {
            if is_data && i <= last {
                held.insert(i);
            } else {
                held.remove(&i);
            }
        }
        let found = if data {
            held.range(start..).next().copied()
        } else {
            let mut i = start;
            while i <= last && held.contains(&i) {
                i += 1;
            }
            (i <= last).then_some(i)
        };
        Ok(found.map(|i| (i * bs).max(offset)))
    }

    async fn tree_leaves_from(
        &self,
        obj: &ObjTree,
        root: &TreeRoot,
        first: u64,
    ) -> Result<Vec<(u64, BlockPtr)>, FileSystemError> {
        let mut out = Vec::new();
        let mut stack: Vec<(BlockPtr, u8, u64)> = root
            .ptr
            .iter()
            .map(|r| (r.clone(), root.levels, 0))
            .collect();
        while let Some((ptr, level, base)) = stack.pop() {
            if level == 0 {
                // (A tree of no levels is just record 0.)
                if base >= first {
                    out.push((base, ptr));
                }
                continue;
            }
            let ind = self.load_indirect(obj, &ptr).await?;
            let span = INDIRECT_FANOUT.pow((level - 1) as u32);
            for (slot, child) in ind.iter().enumerate().rev() {
                let start = base + slot as u64 * span;
                if let Some(c) = child
                    && start + span > first
                {
                    stack.push((c.clone(), level - 1, start));
                }
            }
        }
        Ok(out)
    }
}

/// Levels a tree of `levels` needs once `records` are written (it never
/// shrinks).
fn target_levels(levels: u8, records: &BTreeMap<u64, Dirty>) -> u8 {
    match records.last_key_value() {
        Some((&max, _)) => levels.max(levels_for(max + 1)),
        None => levels,
    }
}

#[cfg(test)]
#[path = "tests/object_tests.rs"]
mod tests;
