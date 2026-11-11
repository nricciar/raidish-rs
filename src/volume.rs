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

use crate::blkptr::{BlockPtr, MAX_RECORD_BLOCKS};
use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Extent, FileInode, InodeRef, InodeType, Permissions};
use crate::raidz::DATA_SHARDS;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Records must be whole stripes so a record never shares a stripe.
pub const VOLBLOCK_ALIGN: u32 = (DATA_SHARDS * BLOCK_SIZE) as u32;
pub const DEFAULT_VOLBLOCKSIZE: u32 = 4 * VOLBLOCK_ALIGN; // 48 KiB
pub const MAX_VOLBLOCKSIZE: u32 = MAX_RECORD_BLOCKS as u32 * BLOCK_SIZE as u32;
pub const INDIRECT_FANOUT: u64 = 512;
/// Commit early once this much dirty data is buffered across all volumes.
pub const DIRTY_LIMIT_BYTES: usize = 256 << 20;
const INDIRECT_CACHE_ENTRIES: usize = 1024;
/// Clean data records kept in memory per volume.
pub const RECORD_CACHE_BYTES: usize = 64 << 20;
const SYNC_WRITE_CONCURRENCY: usize = 32;

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
    /// Create a sparse copy-on-write volume. `volblocksize` must be a multiple
    /// of `VOLBLOCK_ALIGN` and at most `MAX_VOLBLOCKSIZE`.
    pub async fn create_block(
        &self,
        name: &str,
        size_bytes: u64,
        volblocksize: u32,
    ) -> Result<FileId, FileSystemError> {
        if size_bytes == 0
            || volblocksize == 0
            || !volblocksize.is_multiple_of(VOLBLOCK_ALIGN)
            || volblocksize > MAX_VOLBLOCKSIZE
        {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume size {} / volblocksize {} (must be a multiple of {} up to {})",
                size_bytes, volblocksize, VOLBLOCK_ALIGN, MAX_VOLBLOCKSIZE
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
        let vbs = vol.volblocksize;

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
                None => match self.read_committed_record(&vol, idx).await? {
                    Some(d) => out.copy_from_slice(&d[at..at + n]),
                    None => out.fill(0),
                },
            }
            done += n;
        }
        Ok(done)
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
    fn sync_reservation(vol: &Volume, records: &BTreeMap<u64, Dirty>) -> u64 {
        let align = |b: u64| b.div_ceil(DATA_SHARDS as u64) * DATA_SHARDS as u64;
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
        data + indirects * indirect_blocks + 3 * MAX_RECORD_BLOCKS
    }

    async fn sync_volume(
        &self,
        vol: &Volume,
        records: &BTreeMap<u64, Dirty>,
    ) -> Result<(), FileSystemError> {
        let need = Self::sync_reservation(vol, records);
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
        // 1. Data records, written concurrently. All-zero records become holes.
        //    (Futures are boxed explicitly: a combinator chain over borrowed
        //    records trips rustc's higher-ranked Send inference, #100013.)
        type Job<'a> = Pin<
            Box<dyn Future<Output = Result<(u64, Option<BlockPtr>), FileSystemError>> + Send + 'a>,
        >;
        let mut jobs = records.iter().map(|(&idx, rec)| -> Job<'_> {
            match rec {
                Dirty::Data(buf) if buf.iter().any(|&b| b != 0) => {
                    Box::pin(async move { Ok((idx, Some(self.write_record(buf).await?))) })
                }
                _ => Box::pin(async move { Ok((idx, None)) }),
            }
        });
        let mut in_flight = FuturesUnordered::new();
        let mut leaves = Vec::with_capacity(records.len());
        let mut failed = None;
        loop {
            while failed.is_none() && in_flight.len() < SYNC_WRITE_CONCURRENCY {
                match jobs.next() {
                    Some(job) => in_flight.push(job),
                    None => break,
                }
            }
            // On failure stop scheduling, but drain what's in flight so every
            // successful allocation is recorded and can be released.
            match in_flight.next().await {
                Some(Ok((idx, ptr))) => {
                    if let Some(p) = &ptr {
                        allocated.push(p.clone());
                    }
                    leaves.push((idx, ptr));
                }
                Some(Err(e)) => {
                    failed.get_or_insert(e);
                }
                None => break,
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }

        // 2. Indirect blocks, bottom up.
        let old_root = vol.root();
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

    /// Forget an open volume (on delete); its unsynced writes are discarded.
    pub(crate) fn close_volume(&self, name: &str) {
        self.volumes.lock().unwrap().remove(name);
    }
}

#[cfg(test)]
#[path = "tests/volume_tests.rs"]
mod tests;
