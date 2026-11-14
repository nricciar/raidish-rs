//! Several nodes writing one pool: one *committer* and any number of
//! *volume writers*.
//!
//! The committer is the only process that writes the pool's metadata
//! (inodes, the root index, space maps, uberblocks, labels). A volume
//! writer runs volumes (each volume on exactly one writer, by its
//! `vol/<name>` lease) and writes their records and indirect blocks
//! straight to the disks, but only into metaslabs the committer *lent* it
//! (`ms/<idx>` grants, see `cluster::state`). When a volume syncs, the
//! writer builds the new tree exactly as a standalone pool does
//! (`build_tree`), then hands the committer a [`VolumeCommit`]: the new
//! root, what it allocated and what it replaced. The committer checks it,
//! points the inode at the new root, applies the space changes and commits
//! a TXG. Only then does the writer switch its readers to the new tree.
//!
//! A commit is a compare-and-swap on the volume's root, so a writer can
//! resend one after a committer failover: already applied is acknowledged,
//! anything else is refused. Fencing keeps a writer that lost its metaslab
//! or its volume from writing (see `crate::fence`).

use crate::blkptr::BlockPtr;
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Extent;
use crate::object::TreeRoot;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A metaslab lent to a writer, with the space it may allocate.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LentMetaslab {
    pub idx: u32,
    /// The lend's fencing epoch: the writer's requests there carry it.
    pub epoch: u64,
    /// Free extents (start, len) when lent.
    pub free: Vec<(u64, u64)>,
    /// The committer's open TXG (the writer stamps new blocks with it).
    pub open_txg: u64,
}

/// One volume sync, from a writer to the committer.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VolumeCommit {
    pub volume: String,
    /// The writer lease (`writer/...`) and its epoch.
    pub writer: String,
    pub writer_epoch: u64,
    /// The `vol/<name>` lease owner and epoch.
    pub vol_owner: String,
    pub vol_epoch: u64,
    /// The root the tree was built on, and the new one.
    pub base_root: Option<BlockPtr>,
    pub new_root: Option<BlockPtr>,
    /// Extents (start, len) the new tree allocated / replaced.
    pub allocs: Vec<(u64, u64)>,
    pub frees: Vec<(u64, u64)>,
    /// Pool disks the writer missed writes on since its last commit.
    pub missed: Vec<usize>,
    /// The TXG the new blocks were stamped with.
    pub birth_txg: u64,
    /// The volume's intent log from now on (see `crate::zil`): a fresh
    /// region (its extent is among `allocs`), or None. The one it replaces
    /// is freed.
    pub log: Option<crate::inode::VolumeLog>,
}

/// The committer's answer to a commit.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    /// The TXG that made it durable.
    pub txg: u64,
}

/// A namespace change (file or directory), from a writer to the committer.
///
/// The writer writes a file's data into its lent metaslabs itself; the
/// committer writes the inodes and the directory chain up to the root, so
/// writers never contend on the root index: two of them only conflict when
/// they change the same directory entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FsOp {
    /// Create the file at `path`, or replace its contents, with a tree the
    /// writer wrote.
    WriteFile {
        path: String,
        size_bytes: u64,
        blksz: u32,
        tree: TreeRoot,
        /// Every extent (start, len) the tree's records and indirects take.
        allocs: Vec<(u64, u64)>,
        /// The file and its contents as the writer saw them (None: absent).
        /// If they have changed since, the op is refused (compare-and-swap).
        base: Option<(crate::disk::FileId, TreeRoot)>,
    },
    Mkdir {
        path: String,
    },
    /// Delete the file or directory at `path`, if it is still `base`.
    Delete {
        path: String,
        base: Option<(crate::disk::FileId, TreeRoot)>,
    },
    /// Move object `id` from `from` to `to` (see `rename_locked`).
    Rename {
        from: String,
        to: String,
        id: crate::disk::FileId,
    },
    SetAttr {
        path: String,
        attrs: crate::inode::SetAttrs,
    },
    /// `create_at`: file or directory `name` in directory `dir`.
    CreateAt {
        dir: crate::disk::FileId,
        name: String,
        kind: crate::inode::ObjType,
        attrs: crate::inode::SetAttrs,
        exclusive: Option<u64>,
    },
    /// `remove_at`, if `name` still names object `id`.
    RemoveAt {
        dir: crate::disk::FileId,
        name: String,
        id: crate::disk::FileId,
    },
    /// `rename_at` of object `id`.
    RenameAt {
        from_dir: crate::disk::FileId,
        from_name: String,
        to_dir: crate::disk::FileId,
        to_name: String,
        id: crate::disk::FileId,
    },
    SetAttrId {
        id: crate::disk::FileId,
        attrs: crate::inode::SetAttrs,
    },
    /// Point file `id`'s dnode at a tree the writer built on `base` (its
    /// writes at any offset, see `crate::open_file`).
    UpdateFile {
        id: crate::disk::FileId,
        base: TreeRoot,
        tree: TreeRoot,
        size: u64,
        /// Extents (start, len) the new tree allocated / replaced.
        allocs: Vec<(u64, u64)>,
        frees: Vec<(u64, u64)>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FsCommit {
    /// The writer lease (`writer/...`) and its epoch.
    pub writer: String,
    pub writer_epoch: u64,
    pub op: FsOp,
    /// The `obj/<id>` lease this change is made under, if any. Without one,
    /// a change to an object someone else holds that lease on is refused.
    pub lease: Option<ObjLease>,
    /// Pool disks the writer missed writes on since its last commit.
    pub missed: Vec<usize>,
    /// The TXG the writer's new blocks were stamped with.
    pub birth_txg: u64,
}

/// An `obj/<id>` lease (see `crate::committer::object_resource`): held
/// while a file is open for writing, so no one else changes it meanwhile.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjLease {
    pub owner: String,
    pub epoch: u64,
}

/// The committer's answer to an [`FsCommit`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FsCommitted {
    pub txg: u64,
    /// The file id at the path (0 for a delete).
    pub file_id: crate::disk::FileId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum CommitError {
    /// Not now (no committer, quorum lost, lease not visible yet): the same
    /// request may be sent again.
    Retry(String),
    /// Refused for good (lost the lease, root moved on, bad extents).
    Refused(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Retry(m) => write!(f, "committer unavailable: {}", m),
            CommitError::Refused(m) => write!(f, "commit refused: {}", m),
        }
    }
}

impl From<CommitError> for FileSystemError {
    fn from(e: CommitError) -> Self {
        match e {
            CommitError::Retry(_) => FileSystemError::Unavailable(e.to_string()),
            CommitError::Refused(_) => FileSystemError::InvalidArgument(e.to_string()),
        }
    }
}

/// How a writer reaches the committer. The writer identity (lease and
/// epoch) belongs to the sink, which fills `writer`/`writer_epoch` in.
#[async_trait]
pub trait CommitSink: Send + Sync {
    /// The writer lease resource and epoch this sink commits as.
    fn writer(&self) -> (String, u64);
    /// Lend this writer a metaslab with at least some free space
    /// (preferably `want_blocks`).
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError>;
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError>;
    /// Commit; resends `Retry` answers itself until there is a definite one.
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError>;
    /// Commit a namespace change. Like `commit`, a resend of an applied op
    /// is acknowledged.
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError>;
    /// Get volume `volume` ready to run under its `vol/<name>` lease
    /// (`vol_owner`, `vol_epoch`): if a previous holder left an intent log,
    /// the committer replays it first.
    async fn claim_volume(
        &self,
        volume: &str,
        vol_owner: &str,
        vol_epoch: u64,
    ) -> Result<(), CommitError>;
}

/// A volume a writer runs.
pub(crate) struct Attached {
    /// The `vol/<name>` lease owner and epoch.
    pub owner: String,
    pub epoch: u64,
    /// Turns true once that lease is lost: a commit still waiting for the
    /// committer then gives up (see `attach_volume_until`).
    pub lost: Option<tokio::sync::watch::Receiver<bool>>,
    /// Whether that lease is still provably held (renewed well within its
    /// TTL): an intent-log flush is only acknowledged if so. None: always.
    pub fresh: Option<LeaseCheck>,
    /// Set by `detach_volume`: its last commit drops the intent log.
    pub detaching: bool,
}

/// See `Attached::fresh`.
pub type LeaseCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// A writer's side of things.
pub(crate) struct WriterState {
    pub sink: Arc<dyn CommitSink>,
    /// Volume name → its lease.
    pub volumes: std::sync::Mutex<HashMap<String, Attached>>,
    /// Metaslabs lent to this writer.
    pub lent: std::sync::Mutex<Vec<u32>>,
    /// Lent metaslabs holding records of a file still being written (not
    /// committed yet), with how many writes pin each: they can't be given
    /// back until then, or the committer could lend that space again.
    pub pinned: std::sync::Mutex<HashMap<u32, usize>>,
}

impl WriterState {
    pub(crate) fn pin(&self, idx: u32) {
        *self.pinned.lock().unwrap().entry(idx).or_default() += 1;
    }
    pub(crate) fn unpin(&self, idx: u32) {
        let mut p = self.pinned.lock().unwrap();
        if let Some(n) = p.get_mut(&idx) {
            *n -= 1;
            if *n == 0 {
                p.remove(&idx);
            }
        }
    }
    fn is_pinned(&self, idx: u32) -> bool {
        self.pinned.lock().unwrap().contains_key(&idx)
    }

    /// May a flush of volume `name` be acknowledged (see `Attached::fresh`)?
    pub(crate) fn lease_fresh(&self, name: &str) -> bool {
        self.volumes.lock().unwrap().get(name).is_some_and(|a| {
            !a.lost.as_ref().is_some_and(|l| *l.borrow()) && a.fresh.as_ref().is_none_or(|f| f())
        })
    }
}

/// Give a metaslab back once it has less than this many free blocks.
const RETURN_BELOW_RECORDS: u64 = 4;

pub(crate) fn path_string(path: &std::path::Path) -> Result<String, FileSystemError> {
    path.to_str()
        .map(String::from)
        .ok_or_else(|| FileSystemError::InvalidArgument("path isn't UTF-8".into()))
}

/// Resolves once `lost` turns true (never, if its sender is gone first).
async fn lease_lost(mut lost: tokio::sync::watch::Receiver<bool>) {
    if lost.wait_for(|l| *l).await.is_err() {
        std::future::pending::<()>().await;
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Open the pool as a volume writer: read-only, except for the records
    /// and indirect blocks of volumes it runs, which go into metaslabs
    /// `sink`'s committer lends it.
    pub async fn load_writer(dev: D, sink: Arc<dyn CommitSink>) -> Result<Self, FileSystemError> {
        let mut fs = Self::load_readonly(dev).await?;
        for ms in fs.metaslabs.iter() {
            let mut ms = ms.write().await;
            ms.free_extents.clear();
            ms.freeing.clear();
            ms.deferred.clear();
            ms.dirty = false;
        }
        fs.writer = Some(Arc::new(WriterState {
            sink,
            volumes: Default::default(),
            lent: Default::default(),
            pinned: Default::default(),
        }));
        Ok(fs)
    }

    pub fn is_writer(&self) -> bool {
        self.writer.is_some()
    }

    /// Start running volume `name` under its `vol/<name>` lease.
    pub async fn attach_volume(
        &self,
        name: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<(), FileSystemError> {
        self.attach_volume_until(name, owner, epoch, None).await
    }

    /// `attach_volume`, where `lost` turns true once the lease is lost. A
    /// sync waiting for the committer then stops waiting: otherwise a lease
    /// that merely lapsed (nobody took it over) would be answered "retry"
    /// forever, and the sync would never return.
    pub async fn attach_volume_until(
        &self,
        name: &str,
        owner: &str,
        epoch: u64,
        lost: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<(), FileSystemError> {
        self.attach_volume_leased(name, owner, epoch, lost, None)
            .await
    }

    /// `attach_volume_until`, where `fresh` says whether the lease is still
    /// provably held (see `Attached::fresh`).
    pub async fn attach_volume_leased(
        &self,
        name: &str,
        owner: &str,
        epoch: u64,
        lost: Option<tokio::sync::watch::Receiver<bool>>,
        fresh: Option<LeaseCheck>,
    ) -> Result<(), FileSystemError> {
        let w = self
            .writer
            .as_ref()
            .ok_or(FileSystemError::InvalidArgument(
                "not a volume writer".into(),
            ))?;
        // Whatever the last holder logged and acknowledged comes first.
        w.sink.claim_volume(name, owner, epoch).await?;
        // Reload the index: the volume may have been created, moved here or
        // replayed since this writer opened the pool.
        self.reload_root_index().await?;
        self.close_volume(name);
        self.open_volume(name).await?;
        w.volumes.lock().unwrap().insert(
            name.to_string(),
            Attached {
                owner: owner.to_string(),
                epoch,
                lost,
                fresh,
                detaching: false,
            },
        );
        Ok(())
    }

    /// Stop running `name`: sync what's dirty, then forget it.
    pub async fn detach_volume(&self, name: &str) -> Result<(), FileSystemError> {
        if let Some(w) = &self.writer
            && let Some(a) = w.volumes.lock().unwrap().get_mut(name)
        {
            a.detaching = true;
        }
        // Commit until nothing is left (a flush during a commit may have
        // logged writes it didn't take), then drop the log.
        let mut r = self.sync().await;
        for _ in 0..3 {
            if r.is_err()
                || self
                    .volumes
                    .lock()
                    .unwrap()
                    .get(name)
                    .is_none_or(|v| !v.is_dirty())
            {
                break;
            }
            r = self.sync().await;
        }
        if r.is_ok()
            && let Some(w) = self.writer.clone()
        {
            r = self.retire_log(&w, name).await;
        }
        if let Some(w) = &self.writer {
            if let Some(vol) = self.volumes.lock().unwrap().get(name).cloned() {
                self.set_volume_log(w, &vol, None);
                self.set_cursor(&vol, None);
            }
            w.volumes.lock().unwrap().remove(name);
        }
        self.close_volume(name);
        r
    }

    /// Commit `name` unchanged but without its intent log (a detach).
    async fn retire_log(&self, w: &WriterState, name: &str) -> Result<(), FileSystemError> {
        let Some(vol) = self.volumes.lock().unwrap().get(name).cloned() else {
            return Ok(());
        };
        if vol.log.lock().unwrap().is_none() {
            return Ok(());
        }
        let Some((vol_owner, vol_epoch)) = w
            .volumes
            .lock()
            .unwrap()
            .get(name)
            .map(|a| (a.owner.clone(), a.epoch))
        else {
            return Ok(());
        };
        // No flush may log into it meanwhile.
        let _io = self.log_io.lock().await;
        let _tx = self.begin_tx().await;
        if vol.is_dirty() {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' was written to while being detached",
                name
            )));
        }
        let (writer, writer_epoch) = w.sink.writer();
        let root = vol.obj.root_ptr();
        let commit = VolumeCommit {
            volume: name.to_string(),
            writer,
            writer_epoch,
            vol_owner,
            vol_epoch,
            base_root: root.clone(),
            new_root: root,
            allocs: Vec::new(),
            frees: Vec::new(),
            missed: self.take_new_misses(),
            birth_txg: self.open_txg().await?,
            log: None,
        };
        let c = w.sink.commit(commit).await?;
        self.note_open_txg(c.txg + 1).await;
        self.set_volume_log(w, &vol, None);
        self.set_cursor(&vol, None);
        Ok(())
    }

    /// Drop `name` without syncing (its lease is gone: nothing may be
    /// written). Its intent log is left for the committer to replay.
    pub fn abandon_volume(&self, name: &str) {
        if let Some(w) = &self.writer {
            if let Some(vol) = self.volumes.lock().unwrap().get(name).cloned() {
                self.set_volume_log(w, &vol, None);
                self.set_cursor(&vol, None);
            }
            w.volumes.lock().unwrap().remove(name);
        }
        self.close_volume(name);
    }

    /// Give every lent metaslab back (after the last sync), except those
    /// a file still being written is using.
    pub async fn return_metaslabs(&self) {
        let Some(w) = &self.writer else { return };
        let _tx = self.begin_tx().await;
        let lent: Vec<u32> = {
            let mut l = w.lent.lock().unwrap();
            let (pinned, free): (Vec<u32>, Vec<u32>) = l.iter().partition(|&&i| w.is_pinned(i));
            *l = pinned;
            free
        };
        for idx in lent {
            self.drop_lent(idx).await;
            if let Err(e) = w.sink.return_metaslab(idx).await {
                eprintln!("writer: returning metaslab {}: {}", idx, e);
            }
        }
    }

    /// Follow the newest committed object table (a writer's view of the
    /// pool goes stale as the committer commits).
    pub(crate) async fn reload_root_index(&self) -> Result<(), FileSystemError> {
        let ring = self.read_uberblock_ring().await;
        let newest = ring.first().ok_or(FileSystemError::NotFormatted)?;
        self.objects.follow_root(newest.objects.clone());
        self.note_open_txg(newest.txg + 1).await;
        Ok(())
    }

    /// Catch up with the newest committed root, if it moved since this
    /// process last looked (cheap otherwise: one uberblock ring read). For
    /// processes that don't commit themselves (writers, read-only opens);
    /// a no-op where this process is the one committing.
    pub async fn refresh_root(&self) -> Result<(), FileSystemError> {
        if self.writer.is_none() && !self.read_only {
            return Ok(());
        }
        let ring = self.read_uberblock_ring().await;
        let newest = ring.first().ok_or(FileSystemError::NotFormatted)?;
        if self.objects.root() == newest.objects {
            return Ok(());
        }
        self.objects.follow_root(newest.objects.clone());
        self.note_open_txg(newest.txg + 1).await;
        Ok(())
    }

    /// Note the committer's open TXG: new blocks are stamped with it.
    pub(crate) async fn note_open_txg(&self, open_txg: u64) {
        if let Some(u) = self.uberblock.lock().await.as_mut() {
            u.txg = u.txg.max(open_txg.saturating_sub(1));
        }
    }

    async fn drop_lent(&self, idx: u32) {
        if let Some(ms) = self.metaslabs.get(idx as usize) {
            ms.write().await.free_extents.clear();
        }
        if let Some(pool) = self.dev.pool() {
            pool.disks.fence.clear_metaslab(idx);
        }
    }

    /// Borrow metaslabs until `need` blocks are allocatable.
    pub(crate) async fn lease_space(
        &self,
        w: &WriterState,
        need: u64,
    ) -> Result<(), FileSystemError> {
        loop {
            let have = self.allocatable_blocks().await;
            if have >= need {
                return Ok(());
            }
            let lent = w
                .sink
                .lease_metaslab(need - have)
                .await
                .map_err(|e| match e {
                    CommitError::Refused(_) => FileSystemError::UnableToAllocateBlocks(need),
                    e => e.into(),
                })?;
            let Some(ms) = self.metaslabs.get(lent.idx as usize) else {
                return Err(FileSystemError::InvalidArgument(format!(
                    "committer lent unknown metaslab {}",
                    lent.idx
                )));
            };
            {
                let mut ms = ms.write().await;
                ms.free_extents = lent.free.iter().copied().collect();
            }
            if let Some(pool) = self.dev.pool() {
                pool.disks.fence.set_metaslab_epoch(lent.idx, lent.epoch);
            }
            self.note_open_txg(lent.open_txg).await;
            w.lent.lock().unwrap().push(lent.idx);
        }
    }

    /// Return lent metaslabs that are nearly full (between syncs only).
    pub(crate) async fn return_full(&self, w: &WriterState) {
        let threshold = RETURN_BELOW_RECORDS * self.max_record_blocks();
        let lent: Vec<u32> = w.lent.lock().unwrap().clone();
        for idx in lent {
            if w.is_pinned(idx) {
                continue;
            }
            let free = self.metaslabs[idx as usize].read().await.free_blocks();
            if free < threshold {
                self.drop_lent(idx).await;
                w.lent.lock().unwrap().retain(|&i| i != idx);
                if let Err(e) = w.sink.return_metaslab(idx).await {
                    eprintln!("writer: returning metaslab {}: {}", idx, e);
                }
            }
        }
    }

    /// Pool disks with failed writes since the last call.
    fn take_new_misses(&self) -> Vec<usize> {
        let counts = self.miss_counts();
        let mut seen = self.miss_seen.lock().unwrap();
        let mut out = Vec::new();
        for (d, (now, s)) in counts.iter().zip(seen.iter_mut()).enumerate() {
            if now != s {
                *s = *now;
                out.push(d);
            }
        }
        out
    }

    /// The writer's commit: sync every volume (each one a `VolumeCommit`),
    /// then give back full metaslabs.
    pub(crate) async fn writer_commit(&self, w: &WriterState) -> Result<u64, FileSystemError> {
        let volumes = self.sync_volumes().await;
        let result = volumes.and(self.sync_files().await);
        self.return_full(w).await;
        result?;
        Ok(self.open_txg().await?.saturating_sub(1))
    }

    /// `sync_volume` for a writer: build the tree in lent space, then have
    /// the committer commit it.
    pub(crate) async fn writer_sync_volume(
        &self,
        w: &WriterState,
        vol: &crate::volume::Volume,
        records: &std::collections::BTreeMap<u64, crate::object::Dirty>,
        need: u64,
    ) -> Result<(), FileSystemError> {
        let (vol_owner, vol_epoch, lost, detaching) = w
            .volumes
            .lock()
            .unwrap()
            .get(&vol.name)
            .map(|a| (a.owner.clone(), a.epoch, a.lost.clone(), a.detaching))
            .ok_or_else(|| {
                FileSystemError::InvalidArgument(format!("volume '{}' isn't attached", vol.name))
            })?;
        let log_blocks = if detaching {
            0
        } else {
            self.log_region_blocks()
        };
        self.lease_space(w, need + log_blocks).await?;
        // The log from now on (see `crate::zil`): what was appended after
        // the cut this sync took its records at, then a new region flushes
        // go to once it's committed. Without one (no contiguous space, or
        // detaching), flushes keep to the current region.
        let (writer, writer_epoch) = w.sink.writer();
        let from = vol.fold_from.lock().unwrap().take();
        let new_region = match detaching {
            true => None,
            false => self.new_log_region().await.ok(),
        };
        let mut regions: Vec<crate::inode::LogRegion> = Vec::new();
        let mut from_seq = 0;
        if let Some((r, k)) = from {
            regions.push(r);
            from_seq = k;
        }
        regions.extend(new_region.clone());
        let new_log = (!regions.is_empty()).then(|| crate::inode::VolumeLog {
            regions,
            from_seq,
            writer: writer.clone(),
            writer_epoch,
            vol_owner: vol_owner.clone(),
            vol_epoch,
        });
        let release_log = async |r: &Option<crate::inode::LogRegion>| {
            if let Some(r) = r {
                self.unallocate(r.start, r.len).await;
            }
        };

        let birth_txg = self.open_txg().await?;
        let base_root = vol.obj.root_ptr();
        let mut allocated: Vec<BlockPtr> = Vec::new();
        let mut replaced: Vec<BlockPtr> = Vec::new();
        let new_root = match self
            .build_tree(&vol.obj, records, &mut allocated, &mut replaced)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.release_ptrs(&allocated).await;
                release_log(&new_region).await;
                return Err(e);
            }
        };
        // The records must be on disk before the committer points at them.
        if let Err(e) = self.dev.flush().await {
            self.release_ptrs(&allocated).await;
            release_log(&new_region).await;
            return Err(e.into());
        }
        let ext = |p: &BlockPtr| {
            let Extent { start, len } = p.extent();
            (start, len)
        };
        let mut allocs: Vec<(u64, u64)> = allocated.iter().map(ext).collect();
        allocs.extend(new_region.iter().map(crate::zil::region_alloc));
        let commit = VolumeCommit {
            volume: vol.name.clone(),
            writer,
            writer_epoch,
            vol_owner,
            vol_epoch,
            base_root,
            new_root: new_root.ptr.clone(),
            allocs,
            frees: replaced.iter().map(ext).collect(),
            missed: self.take_new_misses(),
            birth_txg,
            log: new_log.clone(),
        };
        let outcome = match lost {
            None => w.sink.commit(commit).await,
            Some(lost) => tokio::select! {
                r = w.sink.commit(commit) => r,
                _ = lease_lost(lost) => Err(CommitError::Retry(format!(
                    "lost the lease on volume '{}'",
                    vol.name
                ))),
            },
        };
        match outcome {
            Ok(c) => {
                self.note_open_txg(c.txg + 1).await;
                vol.obj.set_root(new_root);
                self.set_volume_log(w, vol, new_log);
                if new_region.is_some() {
                    self.set_cursor(vol, new_region);
                }
                Ok(())
            }
            Err(CommitError::Refused(m)) => {
                // Definitely not applied: the space is ours to reuse (and the
                // old region is still the registered one).
                self.release_ptrs(&allocated).await;
                release_log(&new_region).await;
                Err(FileSystemError::InvalidArgument(format!(
                    "commit refused: {}",
                    m
                )))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free),
            // and log nowhere: the old region may have been freed.
            Err(e) => {
                self.set_cursor(vol, None);
                Err(e.into())
            }
        }
    }

    /// Send `op` to the committer, then reload the root index so this
    /// writer sees the result (and other writers' changes).
    pub(crate) async fn writer_commit_fs(
        &self,
        w: &WriterState,
        op: FsOp,
        lease: Option<ObjLease>,
    ) -> Result<FsCommitted, CommitError> {
        let (writer, writer_epoch) = w.sink.writer();
        let commit = FsCommit {
            writer,
            writer_epoch,
            op,
            lease,
            missed: self.take_new_misses(),
            birth_txg: self.open_txg().await.unwrap_or(0),
        };
        let r = w.sink.commit_fs(commit).await;
        if let Ok(c) = &r {
            self.note_open_txg(c.txg + 1).await;
            if let Err(e) = self.reload_root_index().await {
                eprintln!("writer: reloading the root index: {}", e);
            }
        }
        r
    }

    pub(crate) async fn writer_mkdir(
        &self,
        w: &WriterState,
        path: &std::path::Path,
    ) -> Result<crate::disk::FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        let op = FsOp::Mkdir {
            path: path_string(path)?,
        };
        Ok(self.writer_commit_fs(w, op, None).await?.file_id)
    }

    /// `delete` on a writer, under the file's `obj/<id>` lease if one is held.
    pub async fn delete_leased(
        &self,
        path: &std::path::Path,
        lease: Option<ObjLease>,
    ) -> Result<crate::disk::FileId, FileSystemError> {
        let w = self
            .writer
            .clone()
            .ok_or(FileSystemError::InvalidArgument("not a writer".into()))?;
        self.writer_delete(&w, path, lease).await
    }

    pub(crate) async fn writer_delete(
        &self,
        w: &WriterState,
        path: &std::path::Path,
        lease: Option<ObjLease>,
    ) -> Result<crate::disk::FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        self.reload_root_index().await?;
        let entry = self
            .lookup(path)
            .await?
            .entry
            .ok_or(FileSystemError::FileNotFound)?;
        let tree = self.read_dnode(entry.id).await?.tree;
        let op = FsOp::Delete {
            path: path_string(path)?,
            base: Some((entry.id, tree)),
        };
        self.writer_commit_fs(w, op, lease).await?;
        Ok(entry.id)
    }

    /// The object at `path`, if there is one.
    async fn id_at(&self, path: &std::path::Path) -> Option<crate::disk::FileId> {
        self.lookup(path)
            .await
            .ok()
            .and_then(|l| l.entry)
            .map(|e| e.id)
    }

    pub(crate) async fn writer_rename(
        &self,
        w: &WriterState,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<crate::disk::FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        self.reload_root_index().await?;
        let id = self.file_id(from).await?;
        let op = FsOp::Rename {
            from: path_string(from)?,
            to: path_string(to)?,
            id,
        };
        self.writer_commit_fs(w, op, None).await?;
        Ok(id)
    }

    pub(crate) async fn writer_set_attrs(
        &self,
        w: &WriterState,
        path: &std::path::Path,
        attrs: &crate::inode::SetAttrs,
    ) -> Result<crate::disk::FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        let op = FsOp::SetAttr {
            path: path_string(path)?,
            attrs: attrs.clone(),
        };
        Ok(self.writer_commit_fs(w, op, None).await?.file_id)
    }

    // ---- the committer's side ----

    /// Check that every extent is free space in a metaslab lent to `writer`,
    /// then mark them allocated.
    async fn claim_writer_allocs(
        &self,
        allocs: &[(u64, u64)],
        writer: &str,
        lent_to_writer: &impl Fn(u32) -> bool,
    ) -> Result<(), CommitError> {
        let refuse = |m: String| CommitError::Refused(m);
        for &(start, len) in allocs {
            let idx = self
                .layout
                .metaslab_for_block(start)
                .filter(|&i| self.layout.metaslab_for_block(start + len - 1) == Some(i))
                .ok_or_else(|| {
                    refuse(format!("allocation {}+{} outside a metaslab", start, len))
                })?;
            if !lent_to_writer(idx as u32) {
                return Err(refuse(format!(
                    "allocation {}+{} in metaslab {}, not lent to {}",
                    start, len, idx, writer
                )));
            }
            let ms = self.metaslabs[idx].read().await;
            if !ms.lent || !ms.is_free(start, len) {
                return Err(refuse(format!(
                    "allocation {}+{} isn't free space",
                    start, len
                )));
            }
        }
        for &(start, len) in allocs {
            let idx = self.layout.metaslab_for_block(start).unwrap();
            self.metaslabs[idx].write().await.mark_allocated(start, len);
        }
        Ok(())
    }

    /// Apply a writer's namespace change. `lent_to_writer(idx)` as for
    /// `apply_volume_commit`. Like it, the change is made durable by a
    /// commit shared with whatever else was applied meanwhile.
    ///
    /// `object_ok(id)` says whether `c` may change object `id` (the caller
    /// checks it against the `obj/<id>` leases).
    pub async fn apply_fs_commit(
        &self,
        c: &FsCommit,
        lent_to_writer: impl Fn(u32) -> bool,
        object_ok: impl Fn(crate::disk::FileId) -> Result<(), CommitError>,
    ) -> Result<FsCommitted, CommitError> {
        let file_id = {
            let _tx = self.begin_tx().await;
            self.stage_fs_commit(c, &lent_to_writer, &object_ok).await?
        };
        let txg = self.group_commit().await?;
        Ok(FsCommitted { txg, file_id })
    }

    /// Apply `c` in memory, in the open TXG (the caller holds `begin_tx`).
    /// Returns the file id at the path.
    async fn stage_fs_commit(
        &self,
        c: &FsCommit,
        lent_to_writer: &impl Fn(u32) -> bool,
        object_ok: &impl Fn(crate::disk::FileId) -> Result<(), CommitError>,
    ) -> Result<crate::disk::FileId, CommitError> {
        let refuse = |m: String| CommitError::Refused(m);
        let retry = |e: FileSystemError| CommitError::Retry(e.to_string());
        // Recorded whatever happens to the op: the misses are real either way.
        for &d in &c.missed {
            self.mark_degraded(d, c.birth_txg.saturating_sub(1));
        }
        let file_id = match &c.op {
            FsOp::WriteFile {
                path,
                size_bytes,
                blksz,
                tree,
                allocs,
                base,
            } => {
                let path = std::path::Path::new(path);
                let l = self
                    .lookup(path)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", path.display(), e)))?;
                let current = match self.current_file(&l).await {
                    Ok(c) => c,
                    Err(FileSystemError::NotAFile) => {
                        return Err(refuse(format!("{}: not a file", path.display())));
                    }
                    Err(e) => return Err(retry(e)),
                };
                let seen = current.as_ref().map(|(id, d)| (*id, d.tree.clone()));
                if let Some((id, d)) = &current
                    && d.tree == *tree
                    && seen != *base
                {
                    // A resend of a write already applied.
                    return Ok(*id);
                }
                if seen != *base {
                    return Err(refuse(format!(
                        "{} changed since this write was prepared",
                        path.display()
                    )));
                }
                match &current {
                    Some((id, _)) => object_ok(*id)?,
                    None if c.lease.is_some() => {
                        return Err(refuse(format!(
                            "{}: a lease was given for a file that doesn't exist",
                            path.display()
                        )));
                    }
                    None => {}
                }
                self.claim_writer_allocs(allocs, &c.writer, lent_to_writer)
                    .await?;
                match self
                    .set_file_contents(l, current, *blksz, *size_bytes, tree.clone())
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        for &(start, len) in allocs {
                            self.unallocate(start, len).await;
                        }
                        return Err(retry(e));
                    }
                }
            }
            FsOp::Mkdir { path } => self
                .mkdir_locked(std::path::Path::new(path))
                .await
                .map_err(|e| refuse(format!("mkdir {}: {}", path, e)))?,
            FsOp::Delete { path, base } => {
                let p = std::path::Path::new(path);
                let l = self
                    .lookup(p)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", path, e)))?;
                match l.entry {
                    // Already gone (a resend, or someone else deleted it).
                    None => 0,
                    Some(e) => {
                        let d = self.read_dnode(e.id).await.map_err(retry)?;
                        if base.as_ref().is_some_and(|b| *b != (e.id, d.tree.clone())) {
                            return Err(refuse(format!(
                                "{} changed since this delete was prepared",
                                path
                            )));
                        }
                        object_ok(e.id)?;
                        if d.kind == crate::inode::ObjType::Volume {
                            return Err(refuse(format!(
                                "{} is a volume (delete it with `volume delete`)",
                                path
                            )));
                        }
                        self.delete_locked(p)
                            .await
                            .map_err(|e| refuse(format!("delete {}: {}", path, e)))?;
                        0
                    }
                }
            }
            FsOp::Rename { from, to, id } => {
                let (fp, tp) = (std::path::Path::new(from), std::path::Path::new(to));
                if self.id_at(fp).await != Some(*id) {
                    if self.id_at(tp).await == Some(*id) {
                        // A resend of a rename already applied.
                        return Ok(*id);
                    }
                    return Err(refuse(format!("{} isn't object {} any more", from, id)));
                }
                // Only the replaced object's contents are at stake (a rename
                // doesn't touch the moved one's).
                if let Some(t) = self.id_at(tp).await {
                    object_ok(t)?;
                }
                self.rename_locked(fp, tp)
                    .await
                    .map_err(|e| refuse(format!("rename {} to {}: {}", from, to, e)))?
                    .0
            }
            FsOp::UpdateFile {
                id,
                base,
                tree,
                size,
                allocs,
                frees,
            } => {
                let mut dnode = match self.try_read_dnode(*id).await {
                    Ok(Some(d)) if d.is_file() => d,
                    Ok(_) => return Err(refuse(format!("object {} isn't a file any more", id))),
                    Err(e) => return Err(retry(e)),
                };
                if dnode.tree == *tree && dnode.size == *size && base != tree {
                    // A resend of an update already applied.
                    return Ok(*id);
                }
                if dnode.tree != *base {
                    return Err(refuse(format!(
                        "file {} changed since these writes were made",
                        id
                    )));
                }
                object_ok(*id)?;
                self.claim_writer_allocs(allocs, &c.writer, lent_to_writer)
                    .await?;
                dnode.tree = tree.clone();
                dnode.size = *size;
                dnode.attrs.touch();
                if let Err(e) = self.write_dnode(*id, Some(dnode)).await {
                    for &(start, len) in allocs {
                        self.unallocate(start, len).await;
                    }
                    return Err(retry(e));
                }
                for &(start, len) in frees {
                    let _ = self.free(start, len).await;
                }
                *id
            }
            FsOp::CreateAt {
                dir,
                name,
                kind,
                attrs,
                exclusive,
            } => {
                self.create_at_locked(*dir, name, *kind, attrs, *exclusive)
                    .await
                    .map_err(|e| refuse(format!("create {}: {}", name, e)))?
                    .0
            }
            FsOp::RemoveAt { dir, name, id } => {
                let l = self
                    .lookup_at(*dir, name)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", name, e)))?;
                match l.entry {
                    // Already gone (a resend, or someone else removed it).
                    None => 0,
                    Some(e) if e.id != *id => {
                        return Err(refuse(format!("{} isn't object {} any more", name, id)));
                    }
                    Some(_) => {
                        object_ok(*id)?;
                        self.remove_at_locked(*dir, name)
                            .await
                            .map_err(|e| refuse(format!("remove {}: {}", name, e)))?;
                        0
                    }
                }
            }
            FsOp::RenameAt {
                from_dir,
                from_name,
                to_dir,
                to_name,
                id,
            } => {
                let at = |l: &crate::inode::Lookup| l.entry.map(|e| e.id);
                let src = self
                    .lookup_at(*from_dir, from_name)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", from_name, e)))?;
                let dst = self
                    .lookup_at(*to_dir, to_name)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", to_name, e)))?;
                if at(&src) != Some(*id) {
                    if at(&dst) == Some(*id) {
                        // A resend of a rename already applied.
                        return Ok(*id);
                    }
                    return Err(refuse(format!(
                        "{} isn't object {} any more",
                        from_name, id
                    )));
                }
                if let Some(t) = at(&dst) {
                    object_ok(t)?;
                }
                self.rename_at_locked(src, dst)
                    .await
                    .map_err(|e| refuse(format!("rename {} to {}: {}", from_name, to_name, e)))?
                    .0
            }
            FsOp::SetAttrId { id, attrs } => {
                // Attributes aren't contents: no lease needed.
                self.set_attrs_id_locked(*id, attrs)
                    .await
                    .map_err(|e| refuse(format!("object {}: {}", id, e)))?;
                *id
            }
            FsOp::SetAttr { path, attrs } => {
                let p = std::path::Path::new(path);
                self.set_attrs_locked(p, attrs)
                    .await
                    .map_err(|e| refuse(format!("{}: {}", path, e)))?
            }
        };
        Ok(file_id)
    }

    /// Make everything applied so far durable. Commits that arrive while
    /// one is being written queue up behind it and are covered by the next
    /// one together (see `sync`), so concurrent writers share TXGs instead
    /// of each paying for one. Returns the newest committed TXG.
    async fn group_commit(&self) -> Result<u64, CommitError> {
        self.sync()
            .await
            .map_err(|e| CommitError::Retry(e.to_string()))?;
        Ok(self.open_txg().await.unwrap_or(1).saturating_sub(1))
    }

    /// Mark the metaslabs a cluster has lent out (after a committer start).
    pub async fn mark_lent(&self, lent: impl IntoIterator<Item = u32>) {
        for idx in lent {
            if let Some(ms) = self.metaslabs.get(idx as usize) {
                ms.write().await.lent = true;
            }
        }
    }

    /// Pick a metaslab to lend: the unlent one with the most free space,
    /// none untouched by the open TXG (so the committer has no write in
    /// flight there). Marks it lent; the caller must `lend_abort` if the
    /// cluster refuses. Holds the commit lock, so no commit runs meanwhile.
    pub async fn lend_pick(&self, min_free: u64) -> Option<(u32, Vec<(u64, u64)>)> {
        let _tx = self.begin_tx().await;
        let mut best: Option<(usize, u64)> = None;
        for (i, ms) in self.metaslabs.iter().enumerate() {
            let ms = ms.read().await;
            if ms.lent || ms.dirty {
                continue;
            }
            let free = ms.free_blocks();
            if free >= min_free && best.is_none_or(|(_, f)| free > f) {
                best = Some((i, free));
            }
        }
        let (i, _) = best?;
        let mut ms = self.metaslabs[i].write().await;
        ms.lent = true;
        Some((
            i as u32,
            ms.free_extents.iter().map(|(&s, &l)| (s, l)).collect(),
        ))
    }

    pub async fn lend_abort(&self, idx: u32) {
        if let Some(ms) = self.metaslabs.get(idx as usize) {
            ms.write().await.lent = false;
        }
    }

    /// Take a metaslab back (its return went through the cluster at
    /// `epoch`: the committer writes there with it from now on).
    pub async fn unlend(&self, idx: u32, epoch: u64) {
        if let Some(pool) = self.dev.pool() {
            pool.disks.fence.set_metaslab_epoch(idx, epoch);
        }
        if let Some(ms) = self.metaslabs.get(idx as usize) {
            ms.write().await.lent = false;
        }
    }

    pub async fn lent_metaslabs(&self) -> Vec<u32> {
        let mut out = Vec::new();
        for (i, ms) in self.metaslabs.iter().enumerate() {
            if ms.read().await.lent {
                out.push(i as u32);
            }
        }
        out
    }

    /// Apply a writer's commit. `lent_to_writer(idx)` says whether metaslab
    /// `idx` is lent to the committing writer (the caller checked the
    /// leases). Returns the durable TXG.
    pub async fn apply_volume_commit(
        &self,
        c: &VolumeCommit,
        lent_to_writer: impl Fn(u32) -> bool,
    ) -> Result<u64, CommitError> {
        {
            let _tx = self.begin_tx().await;
            self.stage_volume_commit(c, &lent_to_writer).await?;
        }
        // A resend after a failure here is acknowledged by the root check.
        self.group_commit().await
    }

    /// Apply `c` in memory, in the open TXG (the caller holds `begin_tx`).
    async fn stage_volume_commit(
        &self,
        c: &VolumeCommit,
        lent_to_writer: &impl Fn(u32) -> bool,
    ) -> Result<(), CommitError> {
        let refuse = |m: String| CommitError::Refused(m);
        let vol = self
            .open_volume(&c.volume)
            .await
            .map_err(|e| refuse(format!("volume '{}': {}", c.volume, e)))?;
        let current = vol.obj.root_ptr();
        if current == c.new_root && c.new_root != c.base_root {
            // A resend of a commit already applied: just make sure it's durable.
            return Ok(());
        }
        if current != c.base_root {
            return Err(refuse(format!(
                "volume '{}' moved on (its root isn't the one this sync was built on)",
                c.volume
            )));
        }

        // An intent log left by an earlier holder must be replayed first
        // (attaching claims the volume, see `claim_volume`).
        if let Some(l) = vol.log.lock().unwrap().as_ref()
            && (l.vol_owner != c.vol_owner || l.vol_epoch != c.vol_epoch)
        {
            return Err(refuse(format!(
                "volume '{}' has an unreplayed intent log from {}",
                c.volume, l.writer
            )));
        }
        if let Some(l) = &c.log {
            let current = vol.log.lock().unwrap().clone();
            let known = |r: &crate::inode::LogRegion| {
                current.as_ref().is_some_and(|o| o.regions.contains(r))
                    || c.allocs.contains(&crate::zil::region_alloc(r))
            };
            if l.vol_owner != c.vol_owner
                || l.vol_epoch != c.vol_epoch
                || l.writer != c.writer
                || !l.regions.iter().all(known)
            {
                return Err(refuse(format!(
                    "volume '{}': intent log doesn't belong to this commit",
                    c.volume
                )));
            }
        }

        // Every allocation must be free space in a metaslab lent to this writer.
        self.claim_writer_allocs(&c.allocs, &c.writer, lent_to_writer)
            .await?;
        let old_log = std::mem::replace(&mut *vol.log.lock().unwrap(), c.log.clone());

        if let Err(e) = self.update_volume_dnode(&vol, c.new_root.clone()).await {
            for &(start, len) in &c.allocs {
                self.unallocate(start, len).await;
            }
            *vol.log.lock().unwrap() = old_log;
            return Err(CommitError::Retry(e.to_string()));
        }
        // Regions the new log no longer has are done with.
        for r in old_log.iter().flat_map(|l| &l.regions) {
            if !c.log.as_ref().is_some_and(|l| l.regions.contains(r)) {
                let _ = self.free(r.start, r.len).await;
            }
        }
        vol.obj.set_root_ptr(c.new_root.clone());
        for &(start, len) in &c.frees {
            let _ = self.free(start, len).await;
        }
        for &d in &c.missed {
            self.mark_degraded(d, c.birth_txg.saturating_sub(1));
        }
        Ok(())
    }
}
