//! Copy-on-write volumes (virtual block devices).
//!
//! A volume is a fixed-size object (see `crate::object`): a sparse array of
//! `volblocksize` records under a tree of `BlockPtr`s, with placement
//! settings and an intent log on top. It is an object in the root
//! directory; its dnode records all of that.
//!
//! When the TXG commits, each dirty volume's records are synced into a new
//! tree (`build_tree`), the dnode is pointed at the new root, and every
//! replaced block is freed (deferred, so the previous uberblocks stay
//! readable). Nothing is ever overwritten in place.

use crate::blkptr::BlockPtr;
use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Dnode, ObjType, ROOT_OBJECT, VolumeLog, VolumeProps};
use crate::object::{Dirty, ObjTree, Sizing, levels_for};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Preferred volume block size, rounded up to the pool's alignment.
pub const TARGET_VOLBLOCKSIZE: u32 = 48 * 1024;
/// Commit early once this much dirty data is buffered across all volumes.
pub const DIRTY_LIMIT_BYTES: usize = 256 << 20;

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

/// A volume as listed (see `FileSystem::list_volumes`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct VolumeInfo {
    pub name: String,
    pub size_bytes: u64,
    pub volblocksize: u32,
    pub props: VolumeProps,
}

pub struct Volume {
    pub name: String,
    /// Its object number.
    pub id: FileId,
    pub size_bytes: u64,
    pub volblocksize: usize,
    /// The contents: the committed tree and dirty records.
    pub(crate) obj: ObjTree,
    /// Placement settings (rewritten with the dnode, see `VolumeProps`).
    pub(crate) props: Mutex<VolumeProps>,
    /// The intent log the dnode records (rewritten with it).
    pub(crate) log: Mutex<Option<VolumeLog>>,
    /// A writer's place in its own log (None: nothing to append to; the
    /// next flush commits instead). See `crate::zil`. Locked before `dirty`.
    pub(crate) cursor: Mutex<Option<crate::zil::LogCursor>>,
    /// Where the log stood when the sync in progress took its records: the
    /// region and the first record not covered by it.
    pub(crate) fold_from: Mutex<Option<(crate::inode::LogRegion, u64)>>,
}

impl Volume {
    pub fn dirty_bytes(&self) -> usize {
        self.obj.dirty_bytes()
    }

    pub fn is_dirty(&self) -> bool {
        self.obj.is_dirty()
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
        let l = self.lookup(std::path::Path::new(name)).await?;
        if l.parent_id != ROOT_OBJECT {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}': volumes live in the root directory",
                name
            )));
        }
        let levels = levels_for(size_bytes.div_ceil(volblocksize as u64));
        let file_id = self
            .link_new(l, Dnode::volume(volblocksize, size_bytes, levels))
            .await?;
        self.commit_locked().await?;
        Ok(file_id)
    }

    /// The volume `name`'s object number, from the root directory.
    pub(crate) async fn volume_id(&self, name: &str) -> Result<FileId, FileSystemError> {
        let (_, _, root) = self.resolve_dir(&[]).await?;
        match root.files.get(name) {
            Some(e) if e.kind == ObjType::Volume => Ok(e.id),
            Some(_) => Err(FileSystemError::NotAFile),
            None => Err(FileSystemError::FileNotFound),
        }
    }

    pub async fn open_volume(&self, name: &str) -> Result<Arc<Volume>, FileSystemError> {
        if let Some(v) = self.volumes.lock().unwrap().get(name) {
            return Ok(v.clone());
        }

        let id = self.volume_id(name).await?;
        let dnode = self.read_dnode(id).await?;
        let vol = Arc::new(Volume {
            name: name.to_string(),
            id,
            size_bytes: dnode.size,
            volblocksize: dnode.blksz as usize,
            obj: ObjTree::new(dnode.blksz as usize, Sizing::Fixed, dnode.size, dnode.tree),
            props: Mutex::new(dnode.props.unwrap_or_default()),
            log: Mutex::new(dnode.log),
            cursor: Mutex::new(None),
            fold_from: Mutex::new(None),
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

    /// Every volume in the root directory with its size and placement.
    pub async fn list_volumes(&self) -> Result<Vec<VolumeInfo>, FileSystemError> {
        let (_, _, root) = self.resolve_dir(&[]).await?;
        let mut out = Vec::new();
        for (name, e) in root.files {
            if e.kind != ObjType::Volume {
                continue;
            }
            let d = self.read_dnode(e.id).await?;
            out.push(VolumeInfo {
                name,
                size_bytes: d.size,
                volblocksize: d.blksz,
                props: d.props.unwrap_or_default(),
            });
        }
        Ok(out)
    }

    /// Change a volume's placement settings (committed right away).
    pub async fn set_volume_props(
        &self,
        name: &str,
        props: VolumeProps,
    ) -> Result<(), FileSystemError> {
        let _tx = self.begin_tx().await;
        let vol = self.open_volume(name).await?;
        let before = std::mem::replace(&mut *vol.props.lock().unwrap(), props);
        match self.update_volume_dnode(&vol, vol.obj.root_ptr()).await {
            Ok(()) => {}
            Err(e) => {
                *vol.props.lock().unwrap() = before;
                return Err(e);
            }
        }
        self.commit_locked().await?;
        Ok(())
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

    /// Read data from a virtual block device at a specific byte offset
    pub async fn block_read(
        &self,
        name: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_volume(name).await?;
        self.object_read(&vol.obj, offset, buf).await
    }

    /// Write data to a virtual block device at a specific byte offset
    pub async fn block_write(
        &self,
        name: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_volume(name).await?;
        let done = self
            .object_modify(&vol.obj, offset, data.len(), Some(data))
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
        let vol = self.open_volume(name).await?;
        self.object_modify(&vol.obj, offset, len, None).await
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
                // With the cursor held, no log append is half-planned: each
                // one is wholly before this cut (covered by this sync) or
                // after it (see `crate::zil`).
                let cursor = vol.cursor.lock().unwrap();
                let Some(taken) = vol.obj.begin_sync() else {
                    continue;
                };
                *vol.fold_from.lock().unwrap() = cursor.as_ref().map(|c| (c.region.clone(), c.seq));
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

            vol.obj.end_sync(&taken, synced.as_deref());
            if let Err(e) = result {
                eprintln!("volume '{}': sync failed: {}", vol.name, e);
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Blocks a sync of `records` may allocate, worst case.
    pub(crate) fn sync_reservation(&self, vol: &Volume, records: &BTreeMap<u64, Dirty>) -> u64 {
        // The tree, plus the object table records the dnode rewrite takes.
        self.tree_sync_reservation(&vol.obj, records) + 3 * self.max_record_blocks()
    }

    async fn sync_volume(
        &self,
        vol: &Volume,
        records: &BTreeMap<u64, Dirty>,
    ) -> Result<(), FileSystemError> {
        let need = self.sync_reservation(vol, records);
        if let Some(w) = self.writer.clone() {
            return self.writer_sync_volume(&w, vol, records, need).await;
        }
        if self.allocatable_blocks().await < need {
            return Err(FileSystemError::UnableToAllocateBlocks(need));
        }

        // Everything allocated here, so a failure can hand it all back, and
        // everything this sync replaces, freed only once it has succeeded.
        let mut allocated: Vec<BlockPtr> = Vec::new();
        let mut replaced: Vec<BlockPtr> = Vec::new();
        let result = self
            .build_tree(&vol.obj, records, &mut allocated, &mut replaced)
            .await;
        let new_root = match result {
            Ok(root) => root,
            Err(e) => {
                self.release_ptrs(&allocated).await;
                return Err(e);
            }
        };

        // Point the dnode at the new tree; only then is anything freed.
        if let Err(e) = self.update_volume_dnode(vol, new_root.ptr.clone()).await {
            self.release_ptrs(&allocated).await;
            return Err(e);
        }
        // Readers switch to the new tree only now (they see `syncing` until then).
        vol.obj.set_root(new_root);
        // What a snapshot still has stays (see `crate::snapshot`).
        let held = self.held_txg_of(vol.id).await?;
        self.free_unheld(
            replaced
                .iter()
                .map(|p| (p.lba, p.asize as u64, p.birth_txg))
                .collect(),
            held,
        )
        .await
    }

    /// Rewrite `vol`'s dnode (in the open TXG) with tree root `root` and
    /// its current settings and intent log. On failure nothing changed.
    pub(crate) async fn update_volume_dnode(
        &self,
        vol: &Volume,
        root: Option<BlockPtr>,
    ) -> Result<(), FileSystemError> {
        let mut dnode = self.read_dnode(vol.id).await?;
        dnode.tree = crate::object::TreeRoot {
            ptr: root,
            levels: vol.obj.levels(),
        };
        dnode.props = Some(vol.props.lock().unwrap().clone());
        dnode.log = vol.log.lock().unwrap().clone();
        self.write_dnode(vol.id, Some(dnode)).await
    }

    /// How scattered volume `name`'s committed records are.
    pub async fn fragmentation(&self, name: &str) -> Result<FragReport, FileSystemError> {
        let vol = self.open_volume(name).await?;
        let leaves = self.tree_leaves(&vol.obj.root()).await?;
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
        let leaves = self.tree_leaves(&vol.obj.root()).await?;
        let before = frag_of(&leaves);
        let record_blocks = self.align_blocks(vol.volblocksize.div_ceil(BLOCK_SIZE) as u64);
        let indices: Vec<u64> = leaves.iter().map(|(i, _)| *i).collect();
        let mut batch = batch_records.max(1);
        let mut at = 0;
        while at < indices.len() {
            let n = batch.min(indices.len() - at);
            // Room for the rewritten batch plus the space it holds until its
            // old copy's free is released; shrink the batch to fit.
            let need = 2 * n as u64 * record_blocks;
            if self.allocatable_blocks().await < need {
                if n == 1 {
                    eprintln!("defrag: stopping, not enough free space");
                    break;
                }
                batch = n / 2;
                continue;
            }
            // Under the commit lock, so nothing commits between reading the
            // records and queueing them: what's read is what's committed, and
            // writes queued by others (dirty) are left alone.
            let _tx = self.begin_tx().await;
            let root = vol.obj.root();
            let mut current: Vec<(u64, BlockPtr)> = Vec::with_capacity(n);
            for &idx in &indices[at..at + n] {
                if let Some(p) = self.tree_lookup(&vol.obj, &root, 0, idx).await? {
                    current.push((idx, p));
                }
            }
            at += n;
            let contiguous = current
                .windows(2)
                .all(|w| w[1].1.lba == w[0].1.lba + w[0].1.asize as u64);
            if contiguous {
                continue;
            }
            let ptrs: Vec<BlockPtr> = current.iter().map(|(_, p)| p.clone()).collect();
            let datas = self.read_records(&ptrs).await?;
            {
                let mut d = vol.obj.dirty.lock().unwrap();
                for ((idx, _), mut data) in current.iter().zip(datas) {
                    if d.get(*idx).is_none() {
                        data.resize(vol.volblocksize, 0);
                        d.records.insert(*idx, Dirty::Data(data));
                    }
                }
            }
            self.commit_locked().await?;
        }
        let after = frag_of(&self.tree_leaves(&vol.obj.root()).await?);
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
