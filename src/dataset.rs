//! Datasets: the pool's top-level namespaces, each with its own format.
//!
//! The uberblock names the *dataset directory*, an object (`ObjType::Datasets`)
//! whose contents map a dataset's name to its root object and format:
//!
//! ```text
//!   uberblock ──► object table
//!       └─ datasets ──► dataset directory
//!                         ├─ "fs"  (posix) ──► root directory ──► ...
//!                         └─ ...   (other formats, each its own root)
//! ```
//!
//! Every dataset's objects live in the one pool-wide object table (object
//! numbers stay pool-wide and never reused); a dataset is where a namespace
//! starts and how its contents are laid out. A pool is formatted with one
//! posix dataset, [`DEFAULT_FS`]: what `/` means to the file manager, NFS
//! and `/vms`.
//!
//! ## Tree datasets
//!
//! A dataset whose whole contents are one object's record tree (a *tree
//! dataset*, [`DatasetFormat::is_tree`]; today `block`, the volumes) is run
//! by the generic machinery here and in `crate::writer`, `crate::zil`,
//! `crate::snapshot` and `crate::send`:
//!
//! * one node runs it at a time, under its `ds/<name>` lease
//!   (`crate::committer::dataset_resource`);
//! * that node writes its records into metaslabs the committer lends it and
//!   commits by compare-and-swap of the root (`crate::writer::DatasetCommit`);
//! * flushes are acknowledged from its intent log (`crate::zil`), replayed
//!   by the committer if the node goes away;
//! * it can be snapshotted, cloned, rolled back, sent and received.
//!
//! What's particular to a format (how the records are addressed and what
//! serves them: NBD / NVMe for `block`) sits on top, in its own module.

use crate::blkptr::BlockPtr;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{DatasetLog, DatasetProps, Dnode, ObjType};
use crate::object::{Dirty, ObjTree, Sizing};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// The posix dataset every pool is formatted with.
pub const DEFAULT_FS: &str = "fs";

/// How a dataset's contents are laid out.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetFormat {
    /// Directories and files; the root is a directory. Changed through the
    /// committer, object by object (`crate::writer::FsOp`).
    Posix,
    /// A virtual block device (a volume, see `crate::volume`): a fixed-size
    /// tree of `volblocksize` records.
    Block,
    /// Byte-string keys to values, in a B+tree whose nodes are the root
    /// object's records (see `crate::kv`, `crate::btree`).
    Kv,
    /// An etcd v3 keyspace (revisions, history, leases; see `crate::etcd`)
    /// in a B+tree.
    Etcd,
    /// An S3 bucket (objects and their bodies, uploads; see `crate::s3`)
    /// in a B+tree.
    S3,
    /// An SQS queue (messages, their visibility; see `crate::sqs`) in a
    /// B+tree.
    Queue,
    /// The cluster's activity log (see `crate::events`): one per pool,
    /// written by the committer only, in a B+tree.
    Events,
}

impl DatasetFormat {
    pub fn name(&self) -> &'static str {
        match self {
            DatasetFormat::Posix => "posix",
            DatasetFormat::Block => "block",
            DatasetFormat::Kv => "kv",
            DatasetFormat::Etcd => "etcd",
            DatasetFormat::S3 => "s3",
            DatasetFormat::Queue => "queue",
            DatasetFormat::Events => "events",
        }
    }

    /// Whether its root object holds a B+tree (see `crate::btree`).
    pub fn is_btree(&self) -> bool {
        matches!(
            self,
            DatasetFormat::Kv
                | DatasetFormat::Etcd
                | DatasetFormat::S3
                | DatasetFormat::Queue
                | DatasetFormat::Events
        )
    }

    /// Whether a front end serves it (see `crate::frontend`).
    pub fn is_service(&self) -> bool {
        matches!(
            self,
            DatasetFormat::Etcd | DatasetFormat::S3 | DatasetFormat::Queue
        )
    }

    /// Whether the pool keeps it for itself (not listed, made or deleted
    /// by users).
    pub fn is_system(&self) -> bool {
        matches!(self, DatasetFormat::Events)
    }

    /// Whether its contents are its root object's tree, run as a whole by
    /// one writer (see the module docs).
    pub fn is_tree(&self) -> bool {
        !matches!(self, DatasetFormat::Posix)
    }

    /// The kind of its root object.
    pub fn root_kind(&self) -> ObjType {
        match self {
            DatasetFormat::Posix => ObjType::Directory,
            DatasetFormat::Block => ObjType::Volume,
            f if f.is_btree() => ObjType::KvTree,
            _ => unreachable!(),
        }
    }

    /// How writes may change a tree dataset's size.
    pub(crate) fn sizing(&self) -> Sizing {
        match self {
            DatasetFormat::Posix => Sizing::Growable,
            // A B+tree's node ids are a fixed (large, sparse) range.
            _ => Sizing::Fixed,
        }
    }
}

/// Refuse names a dataset can't have. `@` separates a snapshot's name from
/// its dataset's, and `:` a dataset from a path in it.
pub(crate) fn check_dataset_name(name: &str) -> Result<(), FileSystemError> {
    crate::inode::check_name(name)?;
    if name.contains('@') || name.contains(':') {
        return Err(FileSystemError::InvalidArgument(format!(
            "bad dataset name '{}' (no '@' or ':')",
            name
        )));
    }
    Ok(())
}

/// A dataset, as the dataset directory records it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetEntry {
    /// Its root object.
    pub root: FileId,
    pub format: DatasetFormat,
    /// Names this dataset in any pool.
    pub guid: u64,
    /// The TXG it was created in.
    pub created_txg: u64,
    /// When it was created (ms since the epoch; S3 shows it as a bucket's
    /// creation date).
    pub created_ms: u64,
}

/// A dataset other than volumes and posix ones (`kv`, `etcd`, `s3`,
/// `queue`), as listed.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DatasetInfo {
    pub name: String,
    pub format: DatasetFormat,
    pub guid: u64,
    pub created_ms: u64,
    pub props: DatasetProps,
}

/// The dataset directory's contents.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct DatasetIndex {
    pub datasets: BTreeMap<String, DatasetEntry>,
}

/// An open dataset whose contents are one object's tree (see
/// [`DatasetFormat::is_tree`]): what a writer runs under its `ds/<name>`
/// lease, syncs, logs, snapshots and sends.
pub struct Dataset {
    pub name: String,
    /// Its root object's number.
    pub id: FileId,
    pub format: DatasetFormat,
    /// The object's size in bytes when opened.
    pub size_bytes: u64,
    /// Bytes per record.
    pub blksz: usize,
    /// The contents: the committed tree and dirty records.
    pub(crate) obj: ObjTree,
    /// Placement settings (rewritten with the dnode, see `DatasetProps`).
    pub(crate) props: Mutex<DatasetProps>,
    /// The intent log the dnode records (rewritten with it).
    pub(crate) log: Mutex<Option<DatasetLog>>,
    /// A writer's place in its own log (None: nothing to append to; the
    /// next flush commits instead). See `crate::zil`. Locked before `dirty`.
    pub(crate) cursor: Mutex<Option<crate::zil::LogCursor>>,
    /// Where the log stood when the sync in progress took its records: the
    /// region and the first record not covered by it.
    pub(crate) fold_from: Mutex<Option<(crate::inode::LogRegion, u64)>>,
    /// Serializes the format's operations that read and rewrite several
    /// records (a B+tree's): readers share it, a change holds it alone.
    pub(crate) ops: tokio::sync::RwLock<()>,
}

impl Dataset {
    pub fn dirty_bytes(&self) -> usize {
        self.obj.dirty_bytes()
    }

    pub fn is_dirty(&self) -> bool {
        self.obj.is_dirty()
    }
}

/// Decode the dataset directory's contents (`size` bytes of its tree).
pub(crate) fn decode_datasets(data: &[u8]) -> Result<DatasetIndex, FileSystemError> {
    if data.is_empty() {
        return Ok(DatasetIndex::default());
    }
    Ok(bincode::deserialize(data)?)
}

impl<D: BlockDevice> FileSystem<D> {
    /// The dataset directory's contents (uncommitted changes included).
    pub async fn datasets(&self) -> Result<DatasetIndex, FileSystemError> {
        let dnode = self.read_dnode(self.datasets_object()).await?;
        self.read_datasets(&dnode).await
    }

    pub(crate) async fn read_datasets(
        &self,
        dnode: &Dnode,
    ) -> Result<DatasetIndex, FileSystemError> {
        if dnode.kind != ObjType::Datasets {
            return Err(FileSystemError::InvalidArgument(
                "not the dataset directory".into(),
            ));
        }
        let data = self
            .read_tree(&dnode.tree, dnode.blksz as usize, dnode.size)
            .await?;
        decode_datasets(&data)
    }

    /// Replace the dataset directory's contents (in the open TXG).
    pub(crate) async fn write_datasets(&self, index: &DatasetIndex) -> Result<(), FileSystemError> {
        let id = self.datasets_object();
        let mut dnode = self.read_dnode(id).await?;
        let data = if index.datasets.is_empty() {
            Vec::new()
        } else {
            bincode::serialize(index)?
        };
        self.replace_contents(id, &mut dnode, &data).await
    }

    /// The object number of the dataset directory.
    pub fn datasets_object(&self) -> FileId {
        self.datasets_obj.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The root directory of the default posix dataset: `/`.
    pub fn root_dir(&self) -> FileId {
        self.root_dir.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Dataset `name`'s entry (uncommitted changes included).
    pub async fn dataset_entry(&self, name: &str) -> Result<DatasetEntry, FileSystemError> {
        self.datasets()
            .await?
            .datasets
            .remove(name)
            .ok_or(FileSystemError::FileNotFound)
    }

    /// Tree dataset `name`'s root object number.
    pub(crate) async fn dataset_id(&self, name: &str) -> Result<FileId, FileSystemError> {
        let e = self.dataset_entry(name).await?;
        if !e.format.is_tree() {
            return Err(FileSystemError::NotAFile);
        }
        Ok(e.root)
    }

    /// Tree dataset `name`, opened (once per process: later calls share it).
    pub async fn open_dataset(&self, name: &str) -> Result<Arc<Dataset>, FileSystemError> {
        if let Some(v) = self.open_datasets.lock().unwrap().get(name) {
            return Ok(v.clone());
        }
        let e = self.dataset_entry(name).await?;
        if !e.format.is_tree() {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset",
                name,
                e.format.name()
            )));
        }
        let dnode = self.read_dnode(e.root).await?;
        if dnode.kind != e.format.root_kind() {
            return Err(FileSystemError::InvalidArgument(format!(
                "dataset '{}': its root object {} is a {}",
                name,
                e.root,
                dnode.kind.name()
            )));
        }
        let ds = Arc::new(Dataset {
            name: name.to_string(),
            id: e.root,
            format: e.format,
            size_bytes: dnode.size,
            blksz: dnode.blksz as usize,
            obj: ObjTree::new(
                dnode.blksz as usize,
                e.format.sizing(),
                dnode.size,
                dnode.tree,
            ),
            props: Mutex::new(dnode.props.unwrap_or_default()),
            log: Mutex::new(dnode.log),
            cursor: Mutex::new(None),
            fold_from: Mutex::new(None),
            ops: tokio::sync::RwLock::new(()),
        });
        // Another task may have opened it meanwhile; keep the first.
        Ok(self
            .open_datasets
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(ds)
            .clone())
    }

    /// Every B+tree dataset (kv and the services), with its settings.
    pub async fn list_btree_datasets(&self) -> Result<Vec<DatasetInfo>, FileSystemError> {
        let mut out = Vec::new();
        for (name, e) in self.datasets().await?.datasets {
            if !e.format.is_btree() || e.format.is_system() {
                continue;
            }
            let d = self.read_dnode(e.root).await?;
            out.push(DatasetInfo {
                name,
                format: e.format,
                guid: e.guid,
                created_ms: e.created_ms,
                props: d.props.unwrap_or_default(),
            });
        }
        Ok(out)
    }

    /// Every tree dataset's name, by root object number.
    pub(crate) async fn dataset_names(
        &self,
    ) -> Result<std::collections::HashMap<FileId, String>, FileSystemError> {
        Ok(self
            .datasets()
            .await?
            .datasets
            .into_iter()
            .filter(|(_, e)| e.format.is_tree())
            .map(|(n, e)| (e.root, n))
            .collect())
    }

    /// Create dataset `name` of `format` with root object `root` (under
    /// `begin_tx`, without committing). Returns the root's object number.
    pub(crate) async fn create_dataset_locked(
        &self,
        name: &str,
        format: DatasetFormat,
        mut root: Dnode,
    ) -> Result<FileId, FileSystemError> {
        check_dataset_name(name)?;
        debug_assert_eq!(root.kind, format.root_kind());
        let mut index = self.datasets().await?;
        if index.datasets.contains_key(name) {
            return Err(FileSystemError::AlreadyExists);
        }
        let id = self.alloc_object();
        // A posix root is its own parent (its `..`); anything else is
        // linked from the dataset directory.
        root.parent = if format == DatasetFormat::Posix {
            id
        } else {
            self.datasets_object()
        };
        self.write_dnode(id, Some(root)).await?;
        index.datasets.insert(
            name.to_string(),
            DatasetEntry {
                root: id,
                format,
                guid: crate::fs::new_guid(id),
                created_txg: self.open_txg().await?,
                created_ms: crate::fs::now_ms(),
            },
        );
        self.write_datasets(&index).await?;
        Ok(id)
    }

    /// Delete tree dataset `name` and free what only it references (it
    /// must not be running, and must have no snapshots).
    pub async fn delete_dataset(&self, name: &str) -> Result<(), FileSystemError> {
        let _tx = self.begin_tx().await;
        self.delete_dataset_locked(name).await?;
        self.commit_locked().await?;
        Ok(())
    }

    /// `delete_dataset` under `begin_tx`, without committing.
    pub(crate) async fn delete_dataset_locked(&self, name: &str) -> Result<(), FileSystemError> {
        let mut index = self.datasets().await?;
        let e = index
            .datasets
            .get(name)
            .cloned()
            .ok_or(FileSystemError::FileNotFound)?;
        if !e.format.is_tree() {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset; only tree datasets can be deleted",
                name,
                e.format.name()
            )));
        }
        let dnode = self.read_dnode(e.root).await?;
        // Blocks a snapshot or the origin has stay theirs.
        let held = self.unlink_dataset_lineage(e.root, &dnode).await?;
        for p in self
            .unique_blocks(&dnode.tree, dnode.blksz as usize, held, None)
            .await?
        {
            self.free(p.lba, p.asize as u64).await?;
        }
        for r in dnode.log.iter().flat_map(|l| &l.regions) {
            self.free(r.start, r.len).await?;
        }
        self.write_dnode(e.root, None).await?;
        index.datasets.remove(name);
        self.write_datasets(&index).await?;
        self.close_dataset(name);
        Ok(())
    }

    /// Change a dataset's placement settings (committed right away).
    pub async fn set_dataset_props(
        &self,
        name: &str,
        props: DatasetProps,
    ) -> Result<(), FileSystemError> {
        let _tx = self.begin_tx().await;
        let vol = self.open_dataset(name).await?;
        let before = std::mem::replace(&mut *vol.props.lock().unwrap(), props);
        match self.update_dataset_dnode(&vol, vol.obj.root_ptr()).await {
            Ok(()) => {}
            Err(e) => {
                *vol.props.lock().unwrap() = before;
                return Err(e);
            }
        }
        self.commit_locked().await?;
        Ok(())
    }

    pub(crate) fn datasets_dirty_bytes(&self) -> usize {
        self.open_datasets
            .lock()
            .unwrap()
            .values()
            .map(|v| v.dirty_bytes())
            .sum()
    }

    pub fn has_dirty_datasets(&self) -> bool {
        self.open_datasets
            .lock()
            .unwrap()
            .values()
            .any(|v| v.is_dirty())
    }

    /// Write every dirty dataset into the open TXG. Called by `commit_txg`
    /// (under the commit lock) before space maps and the uberblock.
    ///
    /// Each dataset syncs all-or-nothing: on failure its records stay dirty,
    /// its tree is untouched and everything allocated is given back. If the
    /// pool is too full for the data, holes (TRIM / zeroed records) are still
    /// committed on their own so a guest can free space, and the call returns
    /// `UnableToAllocateBlocks` because some data is still not durable.
    pub(crate) async fn sync_datasets(&self) -> Result<(), FileSystemError> {
        let vols: Vec<Arc<Dataset>> = self
            .open_datasets
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
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
            let result = self.sync_dataset(&vol, &taken).await;
            if result.is_ok() {
                synced = Some(taken.clone());
            } else if matches!(result, Err(FileSystemError::UnableToAllocateBlocks(_))) {
                let holes: BTreeMap<u64, Dirty> = taken
                    .iter()
                    .filter(|(_, r)| r.is_hole())
                    .map(|(k, r)| (*k, r.clone()))
                    .collect();
                if !holes.is_empty() && self.sync_dataset(&vol, &holes).await.is_ok() {
                    synced = Some(Arc::new(holes));
                }
            }

            vol.obj.end_sync(&taken, synced.as_deref());
            if let Err(e) = result {
                eprintln!("dataset '{}': sync failed: {}", vol.name, e);
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Blocks a sync of `records` may allocate, worst case.
    pub(crate) fn sync_reservation(&self, vol: &Dataset, records: &BTreeMap<u64, Dirty>) -> u64 {
        // The tree, plus the object table records the dnode rewrite takes.
        self.tree_sync_reservation(&vol.obj, records) + 3 * self.max_record_blocks()
    }

    async fn sync_dataset(
        &self,
        vol: &Dataset,
        records: &BTreeMap<u64, Dirty>,
    ) -> Result<(), FileSystemError> {
        let need = self.sync_reservation(vol, records);
        if let Some(w) = self.writer.clone() {
            return self.writer_sync_dataset(&w, vol, records, need).await;
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
        if let Err(e) = self.update_dataset_dnode(vol, new_root.ptr.clone()).await {
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
    pub(crate) async fn update_dataset_dnode(
        &self,
        vol: &Dataset,
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

    /// Forget an open dataset (on delete); its unsynced writes are discarded.
    pub(crate) fn close_dataset(&self, name: &str) {
        self.open_datasets.lock().unwrap().remove(name);
    }

    /// Find the default posix dataset's root in `index`.
    pub(crate) fn default_root(index: &DatasetIndex) -> Result<FileId, FileSystemError> {
        match index.datasets.get(DEFAULT_FS) {
            Some(e) if e.format == DatasetFormat::Posix => Ok(e.root),
            Some(e) => Err(FileSystemError::InvalidArgument(format!(
                "dataset '{}' is {}, not posix",
                DEFAULT_FS,
                e.format.name()
            ))),
            None => Err(FileSystemError::InvalidArgument(format!(
                "no '{}' dataset",
                DEFAULT_FS
            ))),
        }
    }
}

#[cfg(test)]
#[path = "tests/dataset_tests.rs"]
mod tests;
