//! Copy-on-write volumes (virtual block devices).
//!
//! A volume is a sparse array of fixed-size records (`volblocksize`) indexed
//! by a tree of `BlockPtr`s:
//!
//! ```text
//!   inode.root ──► L(n) indirect ──► … ──► L1 indirect ──► L0 data record
//! ```
//!
//! Each indirect block holds `INDIRECT_FANOUT` optional pointers; `None` is a
//! hole that reads as zeros, so a new volume allocates nothing. Every record
//! and indirect block is checksummed by its parent like any other object.
//!
//! Writes land in an in-memory map of dirty records. When the TXG commits,
//! dirty records are written to newly allocated space, only the indirect
//! blocks on their paths are rewritten (bottom up), the inode is pointed at
//! the new root, and every replaced block is freed (deferred, so the previous
//! uberblocks stay readable). Nothing is ever overwritten in place.
//!
//! Reads look at, in order: dirty records, records being synced, the tree.

use crate::blkptr::BlockPtr;
use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Extent, FileInode, InodeRef, InodeType, Permissions};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Preferred volume block size, rounded up to the pool's alignment.
pub const TARGET_VOLBLOCKSIZE: u32 = 48 * 1024;
pub const INDIRECT_FANOUT: u64 = 512;
/// Commit early once this much dirty data is buffered across all volumes.
pub const DIRTY_LIMIT_BYTES: usize = 256 << 20;
const INDIRECT_CACHE_ENTRIES: usize = 1024;
/// Clean data records kept in memory per volume.
pub const RECORD_CACHE_BYTES: usize = 64 << 20;
const SYNC_WRITE_CONCURRENCY: usize = 32;
/// Consecutive dirty records are written as one contiguous run of at most
/// this many blocks (one allocation, one device write).
pub const RUN_MAX_BLOCKS: u64 = 256;

/// How scattered a volume's records are on disk.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FragReport {
    /// Allocated (non-hole) data records.
    pub records: u64,
    /// Places where the next allocated record (by index) does not start
    /// right where the previous one ends: each costs a separate read.
    pub breaks: u64,
}

impl std::fmt::Display for FragReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} records, {} discontiguities ({:.1}% fragmented)",
            self.records,
            self.breaks,
            if self.records > 1 {
                self.breaks as f64 * 100.0 / (self.records - 1) as f64
            } else {
                0.0
            }
        )
    }
}

/// A write job of a volume sync: a run of consecutive records in space
/// allocated up front, or a single record allocated on its own.
enum SyncJob<'a> {
    Run {
        start: u64,
        len: u64,
        records: Vec<(u64, &'a [u8])>,
    },
    Single(u64, &'a [u8]),
}

type Indirect = Vec<Option<BlockPtr>>;

#[derive(Debug, Clone)]
enum Dirty {
    Data(Vec<u8>),
    Hole,
}

impl Dirty {
    fn is_hole(&self) -> bool {
        match self {
            Dirty::Hole => true,
            Dirty::Data(b) => b.iter().all(|&x| x == 0),
        }
    }
}

#[derive(Default)]
struct DirtyState {
    records: BTreeMap<u64, Dirty>,
    /// Records taken by an in-progress sync (readable until it finishes).
    syncing: Option<Arc<BTreeMap<u64, Dirty>>>,
}

impl DirtyState {
    fn get(&self, idx: u64) -> Option<&Dirty> {
        self.records
            .get(&idx)
            .or_else(|| self.syncing.as_ref().and_then(|s| s.get(&idx)))
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

pub struct Volume {
    pub name: String,
    pub size_bytes: u64,
    pub volblocksize: usize,
    pub levels: u8,
    root: Mutex<Option<BlockPtr>>,
    dirty: Mutex<DirtyState>,
    /// Indirect blocks keyed by (lba, birth_txg): immutable once written, and
    /// a reused lba always has a newer birth TXG.
    cache: Mutex<HashMap<(u64, u64), Arc<Indirect>>>,
    records: Mutex<RecordCache>,
}

impl Volume {
    fn root(&self) -> Option<BlockPtr> {
        self.root.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn root_for_test(&self) -> Option<BlockPtr> {
        self.root()
    }

    pub fn dirty_bytes(&self) -> usize {
        let d = self.dirty.lock().unwrap();
        d.records
            .values()
            .filter(|r| matches!(r, Dirty::Data(_)))
            .count()
            * self.volblocksize
    }

    pub fn is_dirty(&self) -> bool {
        !self.dirty.lock().unwrap().records.is_empty()
    }
}

fn frag_of(leaves: &[(u64, BlockPtr)]) -> FragReport {
    FragReport {
        records: leaves.len() as u64,
        breaks: leaves
            .windows(2)
            .filter(|w| w[1].1.lba != w[0].1.lba + w[0].1.asize as u64)
            .count() as u64,
    }
}

/// Indirect levels needed to address `records` records.
fn levels_for(records: u64) -> u8 {
    let mut levels = 1;
    let mut span = INDIRECT_FANOUT;
    while span < records {
        span = span.saturating_mul(INDIRECT_FANOUT);
        levels += 1;
    }
    levels
}

impl<D: BlockDevice> FileSystem<D> {
    /// Records must be whole stripes so a record never shares a stripe:
    /// volume block sizes are multiples of this.
    pub fn volblock_align(&self) -> u32 {
        (self.align as usize * BLOCK_SIZE) as u32
    }

    pub fn max_volblocksize(&self) -> u32 {
        (self.max_record_blocks() as usize * BLOCK_SIZE) as u32
    }

    /// `TARGET_VOLBLOCKSIZE` (48 KiB) rounded to this pool's alignment.
    pub fn default_volblocksize(&self) -> u32 {
        TARGET_VOLBLOCKSIZE
            .div_ceil(self.volblock_align())
            .saturating_mul(self.volblock_align())
            .min(self.max_volblocksize())
    }

    /// Create a sparse copy-on-write volume. `volblocksize` must be a multiple
    /// of `volblock_align()` and at most `max_volblocksize()`.
    pub async fn create_block(
        &self,
        name: &str,
        size_bytes: u64,
        volblocksize: u32,
    ) -> Result<FileId, FileSystemError> {
        if size_bytes == 0
            || volblocksize == 0
            || !volblocksize.is_multiple_of(self.volblock_align())
            || volblocksize > self.max_volblocksize()
        {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume size {} / volblocksize {} (must be a multiple of {} up to {})",
                size_bytes,
                volblocksize,
                self.volblock_align(),
                self.max_volblocksize()
            )));
        }
        let _tx = self.begin_tx().await;
        if self.file_index.read().await.files.contains_key(name) {
            return Err(FileSystemError::AlreadyExists);
        }

        let file_id = {
            let mut index = self.file_index.write().await;
            index.next_file_id += 1;
            index.next_file_id - 1
        };
        let levels = levels_for(size_bytes.div_ceil(volblocksize as u64));
        let inode = FileInode {
            id: file_id,
            inode_type: InodeType::Volume {
                size_bytes,
                volblocksize,
                levels,
                root: None,
            },
        };
        let inode_ptrs = self.write_value(&inode).await?;
        self.file_index.write().await.files.insert(
            name.to_string(),
            InodeRef {
                file_id,
                inode: inode_ptrs,
                permissions: Permissions::default(),
            },
        );

        self.persist_root_index().await?;
        self.commit_locked().await?;
        Ok(file_id)
    }

    pub async fn open_volume(&self, name: &str) -> Result<Arc<Volume>, FileSystemError> {
        if let Some(v) = self.volumes.lock().unwrap().get(name) {
            return Ok(v.clone());
        }

        let inode_ref = self
            .file_index
            .read()
            .await
            .files
            .get(name)
            .cloned()
            .ok_or(FileSystemError::FileNotFound)?;
        let InodeType::Volume {
            size_bytes,
            volblocksize,
            levels,
            root,
        } = self.read_inode(&inode_ref).await?.inode_type
        else {
            return Err(FileSystemError::NotAFile);
        };

        let vol = Arc::new(Volume {
            name: name.to_string(),
            size_bytes,
            volblocksize: volblocksize as usize,
            levels,
            root: Mutex::new(root),
            dirty: Mutex::new(DirtyState::default()),
            cache: Mutex::new(HashMap::new()),
            records: Mutex::new(RecordCache::default()),
        });
        // Another task may have opened it meanwhile; keep the first.
        Ok(self
            .volumes
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(vol)
            .clone())
    }

    pub(crate) fn volumes_dirty_bytes(&self) -> usize {
        self.volumes
            .lock()
            .unwrap()
            .values()
            .map(|v| v.dirty_bytes())
            .sum()
    }

    pub fn has_dirty_volumes(&self) -> bool {
        self.volumes.lock().unwrap().values().any(|v| v.is_dirty())
    }

    async fn load_indirect(
        &self,
        vol: &Volume,
        ptr: &BlockPtr,
    ) -> Result<Arc<Indirect>, FileSystemError> {
        let key = (ptr.lba, ptr.birth_txg);
        if let Some(ind) = vol.cache.lock().unwrap().get(&key) {
            return Ok(ind.clone());
        }
        let ind: Indirect = bincode::deserialize(&self.read_record(ptr).await?)?;
        let ind = Arc::new(ind);
        let mut cache = vol.cache.lock().unwrap();
        if cache.len() >= INDIRECT_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(key, ind.clone());
        Ok(ind)
    }

    /// Pointer to node `index` at `level` (0 = data records) under `root`.
    async fn tree_lookup(
        &self,
        vol: &Volume,
        root: Option<BlockPtr>,
        level: u8,
        index: u64,
    ) -> Result<Option<BlockPtr>, FileSystemError> {
        let Some(mut ptr) = root else {
            return Ok(None);
        };
        for l in (level + 1..=vol.levels).rev() {
            let ind = self.load_indirect(vol, &ptr).await?;
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
    async fn read_committed_record(
        &self,
        vol: &Volume,
        idx: u64,
    ) -> Result<Option<Arc<Vec<u8>>>, FileSystemError> {
        let Some(ptr) = self.tree_lookup(vol, vol.root(), 0, idx).await? else {
            return Ok(None);
        };
        let key = (ptr.lba, ptr.birth_txg);
        if let Some(data) = vol.records.lock().unwrap().get(key) {
            return Ok(Some(data));
        }
        let mut data = self.read_record(&ptr).await?;
        data.resize(vol.volblocksize, 0);
        let data = Arc::new(data);
        vol.records.lock().unwrap().insert(key, data.clone());
        Ok(Some(data))
    }

    /// Read data from a virtual block device at a specific byte offset
    pub async fn block_read(
        &self,
        name: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_volume(name).await?;
        if offset >= vol.size_bytes {
            return Ok(0);
        }
        let len = ((vol.size_bytes - offset) as usize).min(buf.len());
        if len == 0 {
            return Ok(0);
        }
        let vbs = vol.volblocksize;

        // Fetch every committed record the range needs up front, so records
        // that are adjacent on disk are read together.
        let first = offset / vbs as u64;
        let last = (offset + len as u64 - 1) / vbs as u64;
        let fetched = if last > first {
            self.prefetch_records(&vol, first..=last).await?
        } else {
            HashMap::new()
        };

        let mut done = 0;
        while done < len {
            let pos = offset + done as u64;
            let idx = pos / vbs as u64;
            let at = (pos % vbs as u64) as usize;
            let n = (vbs - at).min(len - done);
            let out = &mut buf[done..done + n];

            let pending = vol.dirty.lock().unwrap().get(idx).cloned();
            match pending {
                Some(Dirty::Data(d)) => out.copy_from_slice(&d[at..at + n]),
                Some(Dirty::Hole) => out.fill(0),
                None => {
                    let committed = match fetched.get(&idx) {
                        Some(d) => d.clone(),
                        None => self.read_committed_record(&vol, idx).await?,
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
        vol: &Volume,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<HashMap<u64, Option<Arc<Vec<u8>>>>, FileSystemError> {
        let root = vol.root();
        let mut out = HashMap::new();
        let mut need: Vec<(u64, BlockPtr)> = Vec::new();
        for idx in range {
            if vol.dirty.lock().unwrap().get(idx).is_some() {
                continue;
            }
            match self.tree_lookup(vol, root.clone(), 0, idx).await? {
                None => {
                    out.insert(idx, None);
                }
                Some(ptr) => match vol.records.lock().unwrap().get((ptr.lba, ptr.birth_txg)) {
                    Some(d) => {
                        out.insert(idx, Some(d));
                    }
                    None => need.push((idx, ptr)),
                },
            }
        }
        let ptrs: Vec<BlockPtr> = need.iter().map(|(_, p)| p.clone()).collect();
        let datas = self.read_records(&ptrs).await?;
        let mut cache = vol.records.lock().unwrap();
        for ((idx, ptr), mut data) in need.into_iter().zip(datas) {
            data.resize(vol.volblocksize, 0);
            let data = Arc::new(data);
            cache.insert((ptr.lba, ptr.birth_txg), data.clone());
            out.insert(idx, Some(data));
        }
        Ok(out)
    }

    /// Write data to a virtual block device at a specific byte offset
    pub async fn block_write(
        &self,
        name: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, FileSystemError> {
        let done = self
            .volume_modify(name, offset, data.len(), Some(data))
            .await?;
        if self.volumes_dirty_bytes() > DIRTY_LIMIT_BYTES {
            self.commit_txg().await?;
        }
        Ok(done)
    }

    /// Zero a range (NBD TRIM / WRITE_ZEROES). Whole records become holes and
    /// release their space on the next commit.
    pub async fn block_zero(
        &self,
        name: &str,
        offset: u64,
        len: usize,
    ) -> Result<usize, FileSystemError> {
        self.volume_modify(name, offset, len, None).await
    }

    /// Apply `data` (or zeros when `None`) at `offset`.
    async fn volume_modify(
        &self,
        name: &str,
        offset: u64,
        len: usize,
        data: Option<&[u8]>,
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_volume(name).await?;
        if offset >= vol.size_bytes {
            return Ok(0);
        }
        let len = ((vol.size_bytes - offset) as usize).min(len);
        let vbs = vol.volblocksize;

        let mut done = 0;
        while done < len {
            let pos = offset + done as u64;
            let idx = pos / vbs as u64;
            let at = (pos % vbs as u64) as usize;
            let n = (vbs - at).min(len - done);
            let src = data.map(|d| &d[done..done + n]);
            let apply = |buf: &mut Vec<u8>| match src {
                Some(src) => buf[at..at + n].copy_from_slice(src),
                None => buf[at..at + n].fill(0),
            };

            if n == vbs {
                // Whole record: no need for the old contents.
                let rec = match src {
                    Some(src) => Dirty::Data(src.to_vec()),
                    None => Dirty::Hole,
                };
                vol.dirty.lock().unwrap().records.insert(idx, rec);
            } else {
                loop {
                    {
                        let mut d = vol.dirty.lock().unwrap();
                        if let Some(Dirty::Data(buf)) = d.records.get_mut(&idx) {
                            apply(buf);
                            break;
                        }
                        // Base on what's being synced, if anything.
                        let base = match d.get(idx) {
                            Some(Dirty::Data(b)) => Some(b.clone()),
                            Some(Dirty::Hole) => Some(vec![0u8; vbs]),
                            None => None,
                        };
                        if let Some(mut buf) = base {
                            apply(&mut buf);
                            d.records.insert(idx, Dirty::Data(buf));
                            break;
                        }
                    }
                    // Read-modify-write against the committed tree.
                    let mut buf = match self.read_committed_record(&vol, idx).await? {
                        Some(committed) => committed.as_ref().clone(),
                        None => vec![0u8; vbs],
                    };
                    let mut d = vol.dirty.lock().unwrap();
                    if d.get(idx).is_some() {
                        continue; // raced with another writer; retry on top of theirs
                    }
                    apply(&mut buf);
                    d.records.insert(idx, Dirty::Data(buf));
                    break;
                }
            }
            done += n;
        }
        Ok(done)
    }

    /// Write every dirty volume into the open TXG. Called by `commit_txg`
    /// (under the commit lock) before space maps and the uberblock.
    ///
    /// Each volume syncs all-or-nothing: on failure its records stay dirty,
    /// its tree is untouched and everything allocated is given back. If the
    /// pool is too full for the data, holes (TRIM / zeroed records) are still
    /// committed on their own so a guest can free space, and the call returns
    /// `UnableToAllocateBlocks` because some data is still not durable.
    pub(crate) async fn sync_volumes(&self) -> Result<(), FileSystemError> {
        let vols: Vec<Arc<Volume>> = self.volumes.lock().unwrap().values().cloned().collect();
        let mut first_err = None;
        for vol in vols {
            let taken = {
                let mut d = vol.dirty.lock().unwrap();
                if d.records.is_empty() {
                    continue;
                }
                let taken = Arc::new(std::mem::take(&mut d.records));
                d.syncing = Some(taken.clone());
                taken
            };

            let mut synced: Option<Arc<BTreeMap<u64, Dirty>>> = None;
            let result = self.sync_volume(&vol, &taken).await;
            if result.is_ok() {
                synced = Some(taken.clone());
            } else if matches!(result, Err(FileSystemError::UnableToAllocateBlocks(_))) {
                let holes: BTreeMap<u64, Dirty> = taken
                    .iter()
                    .filter(|(_, r)| r.is_hole())
                    .map(|(k, r)| (*k, r.clone()))
                    .collect();
                if !holes.is_empty() && self.sync_volume(&vol, &holes).await.is_ok() {
                    synced = Some(Arc::new(holes));
                }
            }

            let mut d = vol.dirty.lock().unwrap();
            d.syncing = None;
            // Anything not synced goes back; newer writes win over it.
            for (k, v) in taken.iter() {
                if synced.as_ref().is_none_or(|s| !s.contains_key(k)) {
                    d.records.entry(*k).or_insert_with(|| v.clone());
                }
            }
            drop(d);
            if let Err(e) = result {
                eprintln!("volume '{}': sync failed: {}", vol.name, e);
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Blocks a sync of `records` may allocate, worst case.
    fn sync_reservation(&self, vol: &Volume, records: &BTreeMap<u64, Dirty>) -> u64 {
        let align = |b: u64| self.align_blocks(b);
        let record_blocks = align((vol.volblocksize / BLOCK_SIZE) as u64);
        // An indirect block: 512 Option<BlockPtr> at <= 57 bytes each, plus length.
        let indirect_blocks = align((INDIRECT_FANOUT * 57 + 8).div_ceil(BLOCK_SIZE as u64));
        let data = records.values().filter(|r| !r.is_hole()).count() as u64 * record_blocks;
        let mut indirects = 0u64;
        let mut span = 1u64;
        for _ in 1..=vol.levels {
            span *= INDIRECT_FANOUT;
            let mut last = None;
            for idx in records.keys() {
                let parent = idx / span;
                if last != Some(parent) {
                    indirects += 1;
                    last = Some(parent);
                }
            }
        }
        // Plus the inode and root index rewrite.
        data + indirects * indirect_blocks + 3 * self.max_record_blocks()
    }

    async fn sync_volume(
        &self,
        vol: &Volume,
        records: &BTreeMap<u64, Dirty>,
    ) -> Result<(), FileSystemError> {
        let need = self.sync_reservation(vol, records);
        if self.allocatable_blocks().await < need {
            return Err(FileSystemError::UnableToAllocateBlocks(need));
        }

        // Everything allocated here, so a failure can hand it all back, and
        // everything this sync replaces, freed only once it has succeeded.
        let mut allocated: Vec<BlockPtr> = Vec::new();
        let mut replaced: Vec<BlockPtr> = Vec::new();
        let result = self
            .build_tree(vol, records, &mut allocated, &mut replaced)
            .await;
        let new_root = match result {
            Ok(root) => root,
            Err(e) => {
                self.release(&allocated).await;
                return Err(e);
            }
        };

        // Point the inode at the new tree; only then is anything freed.
        let old_inode = match self.update_volume_inode(vol, new_root.clone()).await {
            Ok(old) => old,
            Err(e) => {
                self.release(&allocated).await;
                return Err(e);
            }
        };
        // Readers switch to the new tree only now (they see `syncing` until then).
        *vol.root.lock().unwrap() = new_root;
        for ptr in replaced.iter().chain(old_inode.iter()) {
            let e = ptr.extent();
            self.free(e.start, e.len).await?;
        }
        Ok(())
    }

    /// Give back the space pre-allocated for runs that were never written.
    async fn release_jobs(&self, jobs: &[SyncJob<'_>]) {
        for job in jobs {
            if let SyncJob::Run { start, len, .. } = job {
                self.unallocate(*start, *len).await;
            }
        }
    }

    async fn release(&self, ptrs: &[BlockPtr]) {
        for p in ptrs {
            let e = p.extent();
            self.unallocate(e.start, e.len).await;
        }
    }

    /// Write the dirty records and the indirect blocks above them. Returns
    /// the new root without changing anything visible.
    async fn build_tree(
        &self,
        vol: &Volume,
        records: &BTreeMap<u64, Dirty>,
        allocated: &mut Vec<BlockPtr>,
        replaced: &mut Vec<BlockPtr>,
    ) -> Result<Option<BlockPtr>, FileSystemError> {
        // 1. Data records. Each stretch of consecutive indices gets one
        //    contiguous allocation (placed after its neighbour where possible)
        //    and is written in runs of up to RUN_MAX_BLOCKS, one device write
        //    each, concurrently. All-zero records become holes.
        let old_root = vol.root();
        let record_blocks = self.align_blocks(vol.volblocksize.div_ceil(BLOCK_SIZE) as u64);
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
            let hint = match prev_end {
                Some((last, end)) if last + 1 == first => Some(end),
                _ => {
                    let before = match first {
                        0 => None,
                        _ => {
                            self.tree_lookup(vol, old_root.clone(), 0, first - 1)
                                .await?
                        }
                    };
                    // After the neighbour it follows, else after whatever this
                    // sync wrote last, else near where it used to be.
                    match (before, prev_end) {
                        (Some(p), _) => Some(p.lba + p.asize as u64),
                        (None, Some((_, end))) => Some(end),
                        (None, None) => self
                            .tree_lookup(vol, old_root.clone(), 0, first)
                            .await?
                            .map(|p| p.lba),
                    }
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
        let mut changes: BTreeMap<u64, Option<BlockPtr>> = leaves.into_iter().collect();
        for level in 1..=vol.levels {
            let mut parents: BTreeMap<u64, Vec<(u64, Option<BlockPtr>)>> = BTreeMap::new();
            for (child, ptr) in changes {
                parents
                    .entry(child / INDIRECT_FANOUT)
                    .or_default()
                    .push((child, ptr));
            }

            let mut next = BTreeMap::new();
            for (parent, children) in parents {
                let old = self
                    .tree_lookup(vol, old_root.clone(), level, parent)
                    .await?;
                let mut ind: Indirect = match &old {
                    Some(ptr) => (*self.load_indirect(vol, ptr).await?).clone(),
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
                    vol.cache
                        .lock()
                        .unwrap()
                        .insert((ptr.lba, ptr.birth_txg), Arc::new(ind));
                    Some(ptr)
                };
                next.insert(parent, new);
            }
            changes = next;
        }

        let new_root = changes.remove(&0).unwrap_or(old_root.clone());
        if let Some(old) = &old_root
            && new_root.as_ref() != Some(old)
        {
            replaced.push(old.clone());
        }
        Ok(new_root)
    }

    /// Link a new inode for `vol` into the root index. Returns the old inode's
    /// records for the caller to free once the sync is final; on failure the
    /// index is left as it was.
    async fn update_volume_inode(
        &self,
        vol: &Volume,
        root: Option<BlockPtr>,
    ) -> Result<Vec<BlockPtr>, FileSystemError> {
        let old_ref = self
            .file_index
            .read()
            .await
            .files
            .get(&vol.name)
            .cloned()
            .ok_or(FileSystemError::FileNotFound)?;

        let inode = FileInode {
            id: old_ref.file_id,
            inode_type: InodeType::Volume {
                size_bytes: vol.size_bytes,
                volblocksize: vol.volblocksize as u32,
                levels: vol.levels,
                root,
            },
        };
        let inode_ptrs = self.write_value(&inode).await?;
        self.file_index.write().await.files.insert(
            vol.name.clone(),
            InodeRef {
                inode: inode_ptrs.clone(),
                ..old_ref.clone()
            },
        );
        if let Err(e) = self.persist_root_index().await {
            self.file_index
                .write()
                .await
                .files
                .insert(vol.name.clone(), old_ref);
            self.release(&inode_ptrs).await;
            return Err(e);
        }
        Ok(old_ref.inode)
    }

    /// Every extent a volume's tree occupies (indirects and data records).
    pub(crate) async fn volume_tree_extents(
        &self,
        root: &Option<BlockPtr>,
        levels: u8,
    ) -> Result<Vec<Extent>, FileSystemError> {
        let mut out = Vec::new();
        let mut stack: Vec<(BlockPtr, u8)> = root.iter().map(|r| (r.clone(), levels)).collect();
        while let Some((ptr, level)) = stack.pop() {
            out.push(ptr.extent());
            if level > 0 {
                let ind: Indirect = bincode::deserialize(&self.read_record(&ptr).await?)?;
                stack.extend(ind.into_iter().flatten().map(|c| (c, level - 1)));
            }
        }
        Ok(out)
    }

    /// Every allocated data record of a committed volume tree, in index order.
    pub(crate) async fn volume_leaves(
        &self,
        root: &Option<BlockPtr>,
        levels: u8,
    ) -> Result<Vec<(u64, BlockPtr)>, FileSystemError> {
        let mut out = Vec::new();
        // Depth first, children pushed in reverse so they pop in index order.
        let mut stack: Vec<(BlockPtr, u8, u64)> =
            root.iter().map(|r| (r.clone(), levels, 0)).collect();
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

    /// How scattered volume `name`'s committed records are.
    pub async fn fragmentation(&self, name: &str) -> Result<FragReport, FileSystemError> {
        let vol = self.open_volume(name).await?;
        let leaves = self.volume_leaves(&vol.root(), vol.levels).await?;
        Ok(frag_of(&leaves))
    }

    /// Rewrite volume `name` so its records are laid out in index order,
    /// contiguously, in batches of `batch_records` (one commit each). Batches
    /// that are already contiguous are skipped; records written concurrently
    /// are left to the writer. Returns the fragmentation before and after.
    pub async fn defrag(
        &self,
        name: &str,
        batch_records: usize,
    ) -> Result<(FragReport, FragReport), FileSystemError> {
        let vol = self.open_volume(name).await?;
        // Start from a committed tree.
        self.sync().await?;
        let leaves = self.volume_leaves(&vol.root(), vol.levels).await?;
        let before = frag_of(&leaves);
        let record_blocks = self.align_blocks(vol.volblocksize.div_ceil(BLOCK_SIZE) as u64);
        let batch_records = batch_records.max(1);

        for batch in leaves.chunks(batch_records) {
            let ptrs: Vec<BlockPtr> = batch.iter().map(|(_, p)| p.clone()).collect();
            let contiguous = batch
                .windows(2)
                .all(|w| w[1].1.lba == w[0].1.lba + w[0].1.asize as u64);
            if contiguous {
                continue;
            }
            // Room for the rewritten batch plus the space it holds until its
            // old copy's free is released.
            let need = 2 * batch.len() as u64 * record_blocks;
            if self.allocatable_blocks().await < need {
                eprintln!("defrag: stopping, not enough free space for another batch");
                break;
            }
            let datas = self.read_records(&ptrs).await?;
            {
                let mut d = vol.dirty.lock().unwrap();
                for ((idx, _), mut data) in batch.iter().zip(datas) {
                    if d.get(*idx).is_none() {
                        data.resize(vol.volblocksize, 0);
                        d.records.insert(*idx, Dirty::Data(data));
                    }
                }
            }
            self.commit_txg().await?;
        }
        let after = frag_of(&self.volume_leaves(&vol.root(), vol.levels).await?);
        Ok((before, after))
    }

    /// Forget an open volume (on delete); its unsynced writes are discarded.
    pub(crate) fn close_volume(&self, name: &str) {
        self.volumes.lock().unwrap().remove(name);
    }
}

#[cfg(test)]
#[path = "tests/volume_tests.rs"]
mod tests;
