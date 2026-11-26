//! Several nodes writing one pool: one *committer* and any number of
//! *writers*.
//!
//! The committer is the only process that writes the pool's metadata
//! (inodes, the dataset directory, space maps, uberblocks, labels). A
//! writer runs tree datasets (see `crate::dataset`; each on exactly one
//! writer, by its `ds/<name>` lease) and writes their records and indirect
//! blocks straight to the disks, but only into metaslabs the committer
//! *lent* it (`ms/<idx>` grants, see `cluster::state`). When a dataset
//! syncs, the writer builds the new tree exactly as a standalone pool does
//! (`build_tree`), then hands the committer a [`DatasetCommit`]: the new
//! root, what it allocated and what it replaced. The committer checks it,
//! points the root object at the new tree, applies the space changes and
//! commits a TXG. Only then does the writer switch its readers to the new
//! tree. Posix datasets' files and directories go through [`FsOp`]s.
//!
//! A commit is a compare-and-swap on the dataset's root, so a writer can
//! resend one after a committer failover: already applied is acknowledged,
//! anything else is refused. Fencing keeps a writer that lost its metaslab
//! or its dataset from writing (see `crate::fence`).

use crate::blkptr::BlockPtr;
use crate::disk::PoolDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Extent;
use crate::object::TreeRoot;
use crate::types::DatasetName;
use crate::types::ErrorKind;
use crate::types::MetaslabIdx;
use crate::types::Txg;
use crate::types::{Epoch, Holding, LeaseRef, LeaseResource};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A metaslab lent to a writer, with the space it may allocate.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LentMetaslab {
    pub idx: MetaslabIdx,
    /// The lend's fencing epoch: the writer's requests there carry it.
    pub epoch: Epoch,
    /// Free extents when lent.
    pub free: Vec<Extent>,
    /// The committer's open TXG (the writer stamps new blocks with it).
    pub open_txg: Txg,
}

/// A writer's commit, sent, whose answer hasn't been acted on yet: what's
/// needed to act on it later (see `FileSystem::resolve_in_doubt`).
#[derive(Clone, Debug)]
pub(crate) struct InDoubt {
    commit: DatasetCommit,
    /// The misses it reports, counted as reported once it's accepted.
    misses: NewMisses,
    new_root: TreeRoot,
    allocated: Vec<BlockPtr>,
    new_log: Option<crate::inode::DatasetLog>,
    new_region: Option<crate::inode::LogRegion>,
}

/// Disk misses a commit reports: the disks, and every disk's miss count
/// when they were taken.
#[derive(Clone, Debug, Default)]
pub(crate) struct NewMisses {
    disks: Vec<usize>,
    counts: Vec<u64>,
}

/// While a commit's answer is awaited: dropped then (the caller gave up),
/// flushes stop logging into the dataset's current region, as after any
/// unknown outcome (it may be freed if that commit was applied).
struct LogNowhereOnDrop<'a>(Option<&'a crate::dataset::Dataset>);

impl Drop for LogNowhereOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(vol) = self.0 {
            *vol.cursor.lock().unwrap() = None;
        }
    }
}

/// One volume sync, from a writer to the committer.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DatasetCommit {
    pub dataset: DatasetName,
    /// The writer lease (`writer/...`).
    pub writer: LeaseRef,
    /// The `ds/<name>` lease it's committed under.
    pub ds: Holding,
    /// The root the tree was built on, and the new one.
    pub base_root: Option<BlockPtr>,
    pub new_root: Option<BlockPtr>,
    /// Extents (start, len) the new tree allocated / replaced.
    pub allocs: Vec<Extent>,
    pub frees: Vec<Extent>,
    /// The birth TXG of each of `frees` (a snapshot keeps blocks born at
    /// or before it; missing: all kept).
    #[serde(default)]
    pub free_births: Vec<Txg>,
    /// Pool disks the writer missed writes on since its last commit.
    pub missed: Vec<usize>,
    /// The TXG the new blocks were stamped with.
    pub birth_txg: Txg,
    /// The volume's intent log from now on (see `crate::zil`): a fresh
    /// region (its extent is among `allocs`), or None. The one it replaces
    /// is freed.
    pub log: Option<crate::inode::DatasetLog>,
    /// The lowest TXG any new block was stamped with (None: no new blocks).
    #[serde(default)]
    pub min_birth: Option<Txg>,
}

/// The committer's answer to a commit.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    /// The TXG that made it durable.
    pub txg: Txg,
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
        allocs: Vec<Extent>,
        /// The file and its contents as the writer saw them (None: absent).
        /// If they have changed since, the op is refused (compare-and-swap).
        base: Option<(crate::disk::FileId, TreeRoot)>,
        /// The lowest TXG any new block was stamped with.
        #[serde(default)]
        min_birth: Option<Txg>,
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
        allocs: Vec<Extent>,
        frees: Vec<Extent>,
        /// The lowest TXG any new block was stamped with.
        #[serde(default)]
        min_birth: Option<Txg>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FsCommit {
    /// The writer lease (`writer/...`).
    pub writer: LeaseRef,
    pub op: FsOp,
    /// The `obj/<id>` lease this change is made under, if any. Without one,
    /// a change to an object someone else holds that lease on is refused.
    pub lease: Option<ObjLease>,
    /// Pool disks the writer missed writes on since its last commit.
    pub missed: Vec<usize>,
    /// The TXG the writer's new blocks were stamped with.
    pub birth_txg: Txg,
}

/// An `obj/<id>` lease (see `crate::committer::object_resource`): held
/// while a file is open for writing, so no one else changes it meanwhile.
pub type ObjLease = Holding;

/// The committer's answer to an [`FsCommit`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FsCommitted {
    pub txg: Txg,
    /// The file id at the path (0 for a delete).
    pub file_id: crate::disk::FileId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum CommitError {
    /// Not now (no committer, quorum lost, lease not visible yet): the same
    /// request may be sent again.
    Retry(String),
    /// Refused for good (lost the lease, root moved on, bad extents, or
    /// what was asked failed): `kind` says how.
    Refused { kind: ErrorKind, msg: String },
    /// Refused because the new blocks were stamped with a TXG below the
    /// birth floor (see `FileSystem::raise_birth_floor`): rebuild, stamped
    /// with this open TXG, and send again.
    Restamp(Txg),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Retry(m) => write!(f, "committer unavailable: {}", m),
            CommitError::Refused { msg, .. } => write!(f, "refused: {}", msg),
            CommitError::Restamp(t) => write!(f, "births below the floor; restamp at TXG {}", t),
        }
    }
}

impl CommitError {
    /// Refused as `kind`, saying `msg`.
    pub fn refused(kind: ErrorKind, msg: impl Into<String>) -> Self {
        CommitError::Refused {
            kind,
            msg: msg.into(),
        }
    }

    /// Doing it failed with `e` (in `what`, if not empty): refused, unless
    /// that's something a later try may not hit (the pool's disks out of
    /// reach: retry; below the birth floor: restamp), as with `From`.
    pub fn failed(what: impl std::fmt::Display, e: &FileSystemError) -> Self {
        let what = what.to_string();
        let msg = if what.is_empty() {
            e.message()
        } else {
            format!("{}: {}", what, e.message())
        };
        match e {
            FileSystemError::Restamp(t) => CommitError::Restamp(*t),
            e if e.kind() == ErrorKind::Unavailable => CommitError::Retry(msg),
            e => CommitError::refused(e.kind(), msg),
        }
    }

    /// What sort of failure it is.
    pub fn kind(&self) -> ErrorKind {
        match self {
            CommitError::Retry(_) | CommitError::Restamp(_) => ErrorKind::Unavailable,
            CommitError::Refused { kind, .. } => *kind,
        }
    }
}

impl From<FileSystemError> for CommitError {
    /// Refused as it failed (a pool that can't be asked right now: retry).
    fn from(e: FileSystemError) -> Self {
        match e {
            FileSystemError::Restamp(t) => CommitError::Restamp(t),
            e => CommitError::failed("", &e),
        }
    }
}

impl From<CommitError> for FileSystemError {
    /// The error as the committer's side had it.
    fn from(e: CommitError) -> Self {
        match e {
            CommitError::Retry(m) => FileSystemError::Unavailable(format!("committer: {}", m)),
            CommitError::Refused { kind, msg } => FileSystemError::from_kind(kind, msg),
            CommitError::Restamp(t) => FileSystemError::Restamp(t),
        }
    }
}

/// How a writer reaches the committer. The writer identity (its lease)
/// belongs to the sink, which fills `writer` in.
#[async_trait]
pub trait CommitSink: Send + Sync {
    /// The writer lease resource and epoch this sink commits as.
    fn writer(&self) -> LeaseRef;
    /// Lend this writer a metaslab with at least some free space
    /// (preferably `want_blocks`).
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError>;
    async fn return_metaslab(&self, idx: MetaslabIdx) -> Result<(), CommitError>;
    /// Commit; resends `Retry` answers itself until there is a definite one.
    async fn commit(&self, c: DatasetCommit) -> Result<Committed, CommitError>;
    /// Commit a namespace change. Like `commit`, a resend of an applied op
    /// is acknowledged.
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError>;
    /// Get volume `volume` ready to run under its `ds/<name>` lease `ds`:
    /// if a previous holder left an intent log, the committer replays it
    /// first.
    async fn claim_dataset(&self, volume: &DatasetName, ds: &Holding) -> Result<(), CommitError>;
}

/// A volume a writer runs.
pub struct Attached {
    /// The `ds/<name>` lease.
    pub ds: Holding,
    /// Turns true once that lease is lost: a commit still waiting for the
    /// committer then gives up (see `attach_dataset_until`).
    pub lost: Option<tokio::sync::watch::Receiver<bool>>,
    /// Whether that lease is still provably held (renewed well within its
    /// TTL): an intent-log flush is only acknowledged if so. None: always.
    pub fresh: Option<LeaseCheck>,
    /// Set by `detach_dataset`: its last commit drops the intent log.
    pub detaching: bool,
}

/// See `Attached::fresh`.
pub type LeaseCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// A writer's side of things.
pub struct WriterState {
    pub sink: Arc<dyn CommitSink>,
    /// Dataset name → its lease.
    pub datasets: std::sync::Mutex<HashMap<DatasetName, Attached>>,
    /// Metaslabs lent to this writer.
    pub lent: std::sync::Mutex<Vec<MetaslabIdx>>,
    /// Lent metaslabs holding records of a file still being written (not
    /// committed yet), with how many writes pin each: they can't be given
    /// back until then, or the committer could lend that space again.
    pub pinned: std::sync::Mutex<HashMap<MetaslabIdx, usize>>,
}

impl WriterState {
    pub(crate) fn pin(&self, idx: MetaslabIdx) {
        *self.pinned.lock().unwrap().entry(idx).or_default() += 1;
    }
    pub(crate) fn unpin(&self, idx: MetaslabIdx) {
        let mut p = self.pinned.lock().unwrap();
        if let Some(n) = p.get_mut(&idx) {
            *n -= 1;
            if *n == 0 {
                p.remove(&idx);
            }
        }
    }
    /// Pin `idx` if it's still lent to this writer. Under the lent list's
    /// lock, where giving a metaslab back checks the pins and takes it off
    /// the list: either the pin lands first and it stays, or it's already
    /// gone and nothing is pinned.
    pub(crate) fn pin_if_lent(&self, idx: MetaslabIdx) -> bool {
        let lent = self.lent.lock().unwrap();
        if !lent.contains(&idx) {
            return false;
        }
        self.pin(idx);
        true
    }
    fn is_pinned(&self, idx: MetaslabIdx) -> bool {
        self.pinned.lock().unwrap().contains_key(&idx)
    }

    /// May a flush of volume `name` be acknowledged (see `Attached::fresh`)?
    pub(crate) fn lease_fresh(&self, name: &DatasetName) -> bool {
        self.datasets.lock().unwrap().get(name).is_some_and(|a| {
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

impl<D: PoolDevice> FileSystem<D> {
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
            datasets: Default::default(),
            lent: Default::default(),
            pinned: Default::default(),
        }));
        Ok(fs)
    }

    pub fn is_writer(&self) -> bool {
        self.writer.is_some()
    }

    /// Start running volume `name` under its `ds/<name>` lease.
    pub async fn attach_dataset(
        &self,
        name: &DatasetName,
        ds: Holding,
    ) -> Result<(), FileSystemError> {
        self.attach_dataset_until(name, ds, None).await
    }

    /// `attach_dataset`, where `lost` turns true once the lease is lost. A
    /// sync waiting for the committer then stops waiting: otherwise a lease
    /// that merely lapsed (nobody took it over) would be answered "retry"
    /// forever, and the sync would never return.
    pub async fn attach_dataset_until(
        &self,
        name: &DatasetName,
        ds: Holding,
        lost: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<(), FileSystemError> {
        self.attach_dataset_leased(name, ds, lost, None).await
    }

    /// `attach_dataset_until`, where `fresh` says whether the lease is still
    /// provably held (see `Attached::fresh`).
    pub async fn attach_dataset_leased(
        &self,
        name: &DatasetName,
        ds: Holding,
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
        w.sink.claim_dataset(name, &ds).await?;
        // Reload the index: the volume may have been created, moved here or
        // replayed since this writer opened the pool.
        self.reload_root_index().await?;
        self.close_dataset(name);
        self.load_dataset(name).await?;
        w.datasets.lock().unwrap().insert(
            name.clone(),
            Attached {
                ds,
                lost,
                fresh,
                detaching: false,
            },
        );
        Ok(())
    }

    /// Stop running `name`: sync what's dirty, then forget it.
    pub async fn detach_dataset(&self, name: &DatasetName) -> Result<(), FileSystemError> {
        if let Some(w) = &self.writer
            && let Some(a) = w.datasets.lock().unwrap().get_mut(name)
        {
            a.detaching = true;
        }
        // Commit until nothing is left (a flush during a commit may have
        // logged writes it didn't take), then drop the log.
        let mut r = self.sync().await;
        for _ in 0..3 {
            if r.is_err()
                || self
                    .open_datasets
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
        if r.is_err() {
            // Not all of it is committed: it stays attached, writes and
            // intent log included, so the caller can serve it on (a handoff
            // called off) or drop it knowingly (`abandon_dataset`: its log
            // is replayed once the lease goes). Closing it here lost the
            // unsynced writes and its log state (audit A5).
            if let Some(w) = &self.writer
                && let Some(a) = w.datasets.lock().unwrap().get_mut(name)
            {
                a.detaching = false;
            }
            return r;
        }
        self.abandon_dataset(name);
        Ok(())
    }

    /// Commit `name` unchanged but without its intent log (a detach).
    async fn retire_log(&self, w: &WriterState, name: &DatasetName) -> Result<(), FileSystemError> {
        let Some(vol) = self.open_datasets.lock().unwrap().get(name).cloned() else {
            return Ok(());
        };
        if vol.log.lock().unwrap().is_none() {
            return Ok(());
        }
        let Some(ds) = w.datasets.lock().unwrap().get(name).map(|a| a.ds.clone()) else {
            return Ok(());
        };
        // No flush may log into it meanwhile.
        let _io = self.lock_log_io().await;
        let _tx = self.begin_tx().await;
        // Its commit is built on the root: settle one in doubt first.
        self.resolve_in_doubt(w, &vol).await?;
        if vol.is_dirty() {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' was written to while being detached",
                name
            )));
        }
        let writer = w.sink.writer();
        let root = vol.obj.root();
        let misses = self.new_misses();
        let commit = DatasetCommit {
            dataset: name.clone(),
            writer,
            ds,
            base_root: root.ptr.clone(),
            new_root: root.ptr.clone(),
            allocs: Vec::new(),
            frees: Vec::new(),
            free_births: Vec::new(),
            missed: misses.disks.clone(),
            birth_txg: self.open_txg().await?,
            log: None,
            min_birth: None,
        };
        // In doubt like any volume sync (audit A5): applied, it drops the
        // log and the committer frees its regions, so on an unknown outcome
        // flushes must stop logging there (the dataset stays attached: a
        // handoff called off serves it on), and the next sync resends it.
        let sent = InDoubt {
            commit,
            misses,
            new_root: root,
            allocated: Vec::new(),
            new_log: None,
            new_region: None,
        };
        *vol.in_doubt.lock().unwrap() = Some(sent.clone());
        let mut waiting = LogNowhereOnDrop(Some(&vol));
        let outcome = self.send_commit(w, name, sent.commit.clone()).await;
        waiting.0 = None;
        self.settle_commit(w, &vol, sent, outcome).await
    }

    /// Report disk misses no commit has reported yet to the committer, in
    /// a commit of `vol` (attached here) that changes nothing else, and
    /// wait until it's durable (see `crate::zil`: a flush whose records a
    /// disk missed isn't acknowledged before that). Waits for a commit in
    /// progress, which may have reported them already.
    pub async fn writer_report_misses(
        &self,
        w: &WriterState,
        vol: &crate::dataset::Dataset,
    ) -> Result<(), FileSystemError> {
        let _tx = self.begin_tx().await;
        // Its commit is built on the root: settle one in doubt first.
        self.resolve_in_doubt(w, vol).await?;
        let counts = self.miss_counts();
        let missed: Vec<usize> = {
            let seen = self.miss_seen.lock().unwrap();
            (0..counts.len())
                .filter(|&d| counts[d] != seen[d])
                .collect()
        };
        if missed.is_empty() {
            return Ok(());
        }
        let ds = w
            .datasets
            .lock()
            .unwrap()
            .get(&vol.name)
            .map(|a| a.ds.clone())
            .ok_or_else(|| {
                FileSystemError::InvalidArgument(format!("volume '{}' isn't attached", vol.name))
            })?;
        let writer = w.sink.writer();
        let root = vol.obj.root_ptr();
        let commit = DatasetCommit {
            dataset: vol.name.clone(),
            writer,
            ds,
            base_root: root.clone(),
            new_root: root,
            allocs: Vec::new(),
            frees: Vec::new(),
            free_births: Vec::new(),
            missed: missed.clone(),
            birth_txg: self.open_txg().await?,
            // The log as registered: unchanged.
            log: vol.log.lock().unwrap().clone(),
            min_birth: None,
        };
        let c = w.sink.commit(commit).await?;
        self.note_open_txg(c.txg.next()).await;
        // Reported (only now: had it failed, the next try reports them).
        let mut seen = self.miss_seen.lock().unwrap();
        for d in missed {
            seen[d] = seen[d].max(counts[d]);
        }
        Ok(())
    }

    /// Drop `name` without syncing (its lease is gone: nothing may be
    /// written). Its intent log is left for the committer to replay.
    pub fn abandon_dataset(&self, name: &DatasetName) {
        if let Some(w) = &self.writer {
            if let Some(vol) = self.open_datasets.lock().unwrap().get(name).cloned() {
                self.set_dataset_log(w, &vol, None);
                self.set_cursor(&vol, None);
            }
            w.datasets.lock().unwrap().remove(name);
        }
        self.close_dataset(name);
    }

    /// Give every lent metaslab back (after the last sync), except those
    /// a file still being written is using.
    pub async fn return_metaslabs(&self) {
        let Some(w) = &self.writer else { return };
        let _tx = self.begin_tx().await;
        let lent: Vec<MetaslabIdx> = {
            let mut l = w.lent.lock().unwrap();
            let (pinned, free): (Vec<MetaslabIdx>, Vec<MetaslabIdx>) =
                l.iter().partition(|&&i| w.is_pinned(i));
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
    pub async fn reload_root_index(&self) -> Result<(), FileSystemError> {
        let ring = self.read_uberblock_ring().await;
        let newest = ring.first().ok_or(FileSystemError::NotFormatted)?;
        self.objects.follow_root(newest.objects.clone());
        self.note_open_txg(newest.txg.next()).await;
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
        self.note_open_txg(newest.txg.next()).await;
        Ok(())
    }

    /// Note the committer's open TXG: new blocks are stamped with it.
    pub(crate) async fn note_open_txg(&self, open_txg: Txg) {
        if let Some(u) = self.uberblock.lock().await.as_mut() {
            u.txg = u.txg.max(open_txg.prev());
        }
    }

    async fn drop_lent(&self, idx: MetaslabIdx) {
        if let Some(ms) = self.metaslabs.get(idx.index()) {
            ms.write().await.free_extents.clear();
        }
        self.dev.pool().disks().fence().clear_metaslab(idx);
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
                    CommitError::Refused {
                        kind: ErrorKind::NoSpace,
                        ..
                    } => FileSystemError::NoSpace(format!("{} blocks", need)),
                    e => e.into(),
                })?;
            let Some(ms) = self.metaslabs.get(lent.idx.index()) else {
                return Err(FileSystemError::InvalidArgument(format!(
                    "committer lent unknown metaslab {}",
                    lent.idx
                )));
            };
            {
                let mut ms = ms.write().await;
                ms.free_extents = lent.free.iter().map(|e| (e.start, e.len)).collect();
            }
            self.dev
                .pool()
                .disks()
                .fence()
                .set_lent(lent.idx, lent.epoch);
            self.note_open_txg(lent.open_txg).await;
            w.lent.lock().unwrap().push(lent.idx);
        }
    }

    /// Return lent metaslabs that are nearly full (between syncs only).
    pub(crate) async fn return_full(&self, w: &WriterState) {
        let threshold = RETURN_BELOW_RECORDS * self.max_record_blocks();
        let lent: Vec<MetaslabIdx> = w.lent.lock().unwrap().clone();
        for idx in lent {
            if w.is_pinned(idx) {
                continue;
            }
            let free = self.metaslabs[idx.index()].read().await.free_blocks();
            if free < threshold {
                // Pins are taken under the lent list's lock (see
                // `RepairHold::pin_lent`): check and leave under it too.
                {
                    let mut l = w.lent.lock().unwrap();
                    if w.is_pinned(idx) {
                        continue;
                    }
                    l.retain(|&i| i != idx);
                }
                self.drop_lent(idx).await;
                if let Err(e) = w.sink.return_metaslab(idx).await {
                    eprintln!("writer: returning metaslab {}: {}", idx, e);
                }
            }
        }
    }

    /// Pool disks with failed writes no accepted commit has reported yet.
    /// They count as reported only once a commit carrying them is accepted
    /// (`misses_reported`): one refused, or lost, reports them again next
    /// time (audit D4). Twice is harmless: a mark keeps the oldest TXG.
    fn new_misses(&self) -> NewMisses {
        let counts = self.miss_counts();
        let seen = self.miss_seen.lock().unwrap();
        let disks = (0..counts.len())
            .filter(|&d| counts[d] != seen[d])
            .collect();
        NewMisses { disks, counts }
    }

    /// An accepted commit carried `m`.
    fn misses_reported(&self, m: &NewMisses) {
        let mut seen = self.miss_seen.lock().unwrap();
        for &d in &m.disks {
            seen[d] = seen[d].max(m.counts[d]);
        }
    }

    /// The writer's commit: sync every volume (each one a `DatasetCommit`),
    /// then give back full metaslabs.
    /// Also returns the datasets it left clean (see `sync_datasets_noting`).
    pub(crate) async fn writer_commit(
        &self,
        tx: &mut crate::tx::Tx,
        w: &WriterState,
    ) -> (Result<Txg, FileSystemError>, Vec<DatasetName>) {
        // A sync stamped below the birth floor is rebuilt (with the TXG
        // the committer sent back) and sent again.
        let mut attempts = 0;
        let mut clean = Vec::new();
        let result = loop {
            let (now_clean, volumes) = self.sync_datasets_noting(tx).await;
            // Clean in any pass stays clean: a later pass only finds it
            // with nothing dirty from before this commit began, or syncs it.
            for n in now_clean {
                if !clean.contains(&n) {
                    clean.push(n);
                }
            }
            let result = volumes.and(self.sync_files(tx).await);
            attempts += 1;
            match result {
                Err(FileSystemError::Restamp(_)) if attempts < 3 => continue,
                r => break r,
            }
        };
        self.return_full(w).await;
        let r = match result {
            Ok(()) => self.open_txg().await.map(Txg::prev),
            Err(e) => Err(e),
        };
        (r, clean)
    }

    /// `sync_dataset` for a writer: build the tree in lent space, then have
    /// the committer commit it.
    pub(crate) async fn writer_sync_dataset(
        &self,
        w: &WriterState,
        vol: &crate::dataset::Dataset,
        records: &std::collections::BTreeMap<u64, crate::object::Dirty>,
        need: u64,
        fold: bool,
    ) -> Result<(), FileSystemError> {
        let (ds, detaching) = w
            .datasets
            .lock()
            .unwrap()
            .get(&vol.name)
            .map(|a| (a.ds.clone(), a.detaching))
            .ok_or_else(|| {
                FileSystemError::InvalidArgument(format!("volume '{}' isn't attached", vol.name))
            })?;
        let mut span = crate::trace::Span::new("writer.sync_dataset");
        span.note("records", records.len());
        let log_blocks = if detaching || !fold {
            0
        } else {
            self.log_region_blocks()
        };
        self.lease_space(w, need + log_blocks).await?;
        span.mark("lease_space");
        // The log from now on (see `crate::zil`): what was appended after
        // the cut this sync took its records at, then a new region flushes
        // go to once it's committed. Without one (no contiguous space, or
        // detaching), flushes keep to the current region.
        let writer = w.sink.writer();
        // Not folding (a holes-only retry, see `sync_datasets_noting`): the
        // log stays as registered, whole, and `fold_from` stays for the
        // next full sync.
        let from = match fold {
            true => vol.fold_from.lock().unwrap().take(),
            false => None,
        };
        let new_region = match detaching || !fold {
            true => None,
            false => self.new_log_region().await.ok(),
        };
        span.mark("log_region");
        let mut regions: Vec<crate::inode::LogRegion> = Vec::new();
        let mut from_seq = 0;
        if let Some((r, k)) = from {
            regions.push(r);
            from_seq = k;
        }
        regions.extend(new_region.clone());
        let new_log = match fold {
            true => (!regions.is_empty()).then(|| crate::inode::DatasetLog {
                regions,
                from_seq,
                writer: writer.clone(),
                ds: ds.clone(),
            }),
            false => vol.log.lock().unwrap().clone(),
        };
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
        span.mark("build_tree");
        // The records must be on disk before the committer points at them.
        let flushed = self.dev.flush().await;
        span.mark("dev_flush");
        if let Err(e) = flushed {
            self.release_ptrs(&allocated).await;
            release_log(&new_region).await;
            return Err(e.into());
        }
        let ext = |p: &BlockPtr| p.extent();
        let misses = self.new_misses();
        let mut allocs: Vec<Extent> = allocated.iter().map(ext).collect();
        allocs.extend(new_region.iter().map(crate::zil::region_alloc));
        let commit = DatasetCommit {
            dataset: vol.name.clone(),
            writer,
            ds,
            base_root,
            new_root: new_root.ptr.clone(),
            allocs,
            frees: replaced.iter().map(ext).collect(),
            free_births: replaced.iter().map(|p| p.birth_txg).collect(),
            missed: misses.disks.clone(),
            birth_txg,
            log: new_log.clone(),
            min_birth: allocated.iter().map(|p| p.birth_txg).min(),
        };
        // From the moment it's sent until its answer is settled, the commit
        // is in doubt: if this future is dropped meanwhile (a caller giving
        // up), the next sync resends it before building anything new.
        let sent = InDoubt {
            commit,
            misses,
            new_root: new_root.clone(),
            allocated,
            new_log,
            new_region,
        };
        *vol.in_doubt.lock().unwrap() = Some(sent.clone());
        let mut waiting = LogNowhereOnDrop(Some(vol));
        let outcome = self.send_commit(w, &vol.name, sent.commit.clone()).await;
        waiting.0 = None;
        span.mark("commit_rpc");
        self.settle_commit(w, vol, sent, outcome).await
    }

    /// Send `commit` of dataset `name`; a lost `ds/<name>` lease ends the
    /// wait (the committer would answer "retry" for as long as nobody took
    /// the lease over).
    async fn send_commit(
        &self,
        w: &WriterState,
        name: &DatasetName,
        commit: DatasetCommit,
    ) -> Result<Committed, CommitError> {
        let lost = w
            .datasets
            .lock()
            .unwrap()
            .get(name)
            .and_then(|a| a.lost.clone());
        match lost {
            None => w.sink.commit(commit).await,
            Some(lost) => tokio::select! {
                r = w.sink.commit(commit) => r,
                _ = lease_lost(lost) => Err(CommitError::Retry(format!(
                    "lost the lease on volume '{}'",
                    name
                ))),
            },
        }
    }

    /// Act on the answer to `sent` (`vol`'s commit in doubt). Every change
    /// to `vol` is made before the first await, so a caller giving up
    /// halfway can't leave it half-settled.
    async fn settle_commit(
        &self,
        w: &WriterState,
        vol: &crate::dataset::Dataset,
        sent: InDoubt,
        outcome: Result<Committed, CommitError>,
    ) -> Result<(), FileSystemError> {
        let InDoubt {
            misses,
            new_root,
            allocated,
            new_log,
            new_region,
            ..
        } = sent;
        match outcome {
            Ok(c) => {
                *vol.in_doubt.lock().unwrap() = None;
                self.misses_reported(&misses);
                vol.obj.set_root(new_root);
                // A new region moves flushes there; no log at all (a
                // retired one) leaves them nowhere to go.
                let no_log = new_log.is_none();
                self.set_dataset_log(w, vol, new_log);
                if new_region.is_some() || no_log {
                    self.set_cursor(vol, new_region);
                }
                self.note_open_txg(c.txg.next()).await;
                Ok(())
            }
            Err(CommitError::Refused { kind, msg: m }) => {
                // Definitely not applied: the space is ours to reuse (and the
                // old region is still the registered one).
                *vol.in_doubt.lock().unwrap() = None;
                self.release_ptrs(&allocated).await;
                if let Some(r) = &new_region {
                    self.unallocate(r.start, r.len).await;
                }
                Err(FileSystemError::from_kind(
                    kind,
                    format!("commit refused: {}", m),
                ))
            }
            Err(CommitError::Restamp(open)) => {
                // Not applied either: rebuild (the records stay dirty)
                // stamped with the committer's TXG.
                *vol.in_doubt.lock().unwrap() = None;
                self.release_ptrs(&allocated).await;
                if let Some(r) = &new_region {
                    self.unallocate(r.start, r.len).await;
                }
                self.note_open_txg(open).await;
                Err(FileSystemError::Restamp(open))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free),
            // log nowhere (the old region may have been freed), and leave it
            // in doubt for `resolve_in_doubt`.
            Err(e) => {
                self.set_cursor(vol, None);
                Err(e.into())
            }
        }
    }

    /// Settle `vol`'s commit in doubt, if it has one, by sending it again:
    /// the committer acknowledges a commit it already applied (its root is
    /// that commit's new root), applies one it never saw, or refuses one
    /// that can't be applied any more. Until then, a sync built on `vol`'s
    /// root (the one before that commit) would be refused as "moved on"
    /// for good if it had been applied. Err: still unknown (no committer).
    pub(crate) async fn resolve_in_doubt(
        &self,
        w: &WriterState,
        vol: &crate::dataset::Dataset,
    ) -> Result<(), FileSystemError> {
        let Some(sent) = vol.in_doubt.lock().unwrap().clone() else {
            return Ok(());
        };
        let outcome = self.send_commit(w, &vol.name, sent.commit.clone()).await;
        let known = !matches!(outcome, Err(CommitError::Retry(_)));
        match self.settle_commit(w, vol, sent, outcome).await {
            Err(e) if known => {
                // Not applied, and settled: the next sync builds anew.
                eprintln!(
                    "dataset '{}': an earlier commit wasn't applied: {}",
                    vol.name, e
                );
                Ok(())
            }
            r => r,
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
        let writer = w.sink.writer();
        let misses = self.new_misses();
        let commit = FsCommit {
            writer,
            op,
            lease,
            missed: misses.disks.clone(),
            birth_txg: self.open_txg().await.unwrap_or(Txg::ZERO),
        };
        let r = w.sink.commit_fs(commit).await;
        if let Ok(c) = &r {
            self.misses_reported(&misses);
            self.note_open_txg(c.txg.next()).await;
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

    /// Check the TXGs a writer stamped its new blocks with: none from the
    /// future (past the open TXG), and none below the birth floor (else
    /// `Restamp`: rebuild with the open TXG).
    async fn check_births(&self, min_birth: Option<Txg>, stamped: Txg) -> Result<(), CommitError> {
        let Some(min) = min_birth else {
            return Ok(());
        };
        let open = self
            .open_txg()
            .await
            .map_err(|e| CommitError::Retry(e.to_string()))?;
        if min > open || stamped > open {
            return Err(CommitError::refused(
                ErrorKind::Invalid,
                format!(
                    "blocks stamped with TXG {} past the open TXG {}",
                    stamped.max(min),
                    open
                ),
            ));
        }
        if min < self.birth_floor() {
            return Err(CommitError::Restamp(open));
        }
        Ok(())
    }

    /// Check that every extent is free space in a metaslab lent to `writer`,
    /// and that no two overlap, then mark them allocated: all of them or
    /// none. The metaslabs involved stay write-locked (in index order) from
    /// the check through the marking, and `lent_to_writer` is asked under
    /// those locks, so a metaslab can't be taken back and lent to another
    /// writer in between (audit S6a: that writer would be handed the same
    /// space as free).
    async fn claim_writer_allocs(
        &self,
        tx: &mut crate::tx::Tx,
        allocs: &[Extent],
        writer: &LeaseResource,
        lent_to_writer: &impl Fn(MetaslabIdx) -> bool,
    ) -> Result<(), CommitError> {
        let refuse = |m: String| CommitError::refused(ErrorKind::Invalid, m);
        let mut placed: Vec<(MetaslabIdx, Extent)> = Vec::with_capacity(allocs.len());
        for &e @ Extent { start, len } in allocs {
            let last = (len > 0).then(|| start.checked_add(len - 1)).flatten();
            let idx = self
                .layout
                .metaslab_for_block(start)
                .filter(|&i| last.and_then(|l| self.layout.metaslab_for_block(l)) == Some(i))
                .ok_or_else(|| {
                    refuse(format!("allocation {}+{} outside a metaslab", start, len))
                })?;
            placed.push((idx, e));
        }
        // Overlapping extents would each pass `is_free` and claim the same
        // blocks twice (audit S6c-2).
        let mut sorted: Vec<Extent> = allocs.to_vec();
        sorted.sort_unstable_by_key(|e| e.start);
        if let Some(w) = sorted
            .windows(2)
            .find(|w| w[0].start + w[0].len > w[1].start)
        {
            return Err(refuse(format!(
                "allocations {}+{} and {}+{} overlap",
                w[0].start, w[0].len, w[1].start, w[1].len
            )));
        }

        let mut idxs: Vec<MetaslabIdx> = placed.iter().map(|(i, _)| *i).collect();
        idxs.sort_unstable_by_key(|i| i.index());
        idxs.dedup();
        let mut locked = Vec::with_capacity(idxs.len());
        for &idx in &idxs {
            locked.push(self.metaslabs[idx.index()].write().await);
        }
        let guard = |idx: MetaslabIdx| idxs.iter().position(|i| *i == idx).unwrap();
        for &idx in &idxs {
            if !lent_to_writer(idx) {
                let e = placed.iter().find(|(i, _)| *i == idx).unwrap().1;
                return Err(refuse(format!(
                    "allocation {}+{} in metaslab {}, not lent to {}",
                    e.start, e.len, idx, writer
                )));
            }
        }
        for &(idx, Extent { start, len }) in &placed {
            let ms = &locked[guard(idx)];
            if !ms.lent || !ms.is_free(start, len) {
                return Err(refuse(format!(
                    "allocation {}+{} isn't free space",
                    start, len
                )));
            }
        }
        for &(idx, Extent { start, len }) in &placed {
            locked[guard(idx)].mark_allocated(start, len);
        }
        // An op refused after this gives them back (see `crate::tx`).
        tx.allocated(allocs.iter().map(|e| (e.start, e.len)));
        Ok(())
    }

    /// Apply a writer's namespace change. `lent_to_writer(idx)` as for
    /// `apply_dataset_commit`. Like it, the change is made durable by a
    /// commit shared with whatever else was applied meanwhile.
    ///
    /// `object_ok(id)` says whether `c` may change object `id` (the caller
    /// checks it against the `obj/<id>` leases).
    pub async fn apply_fs_commit(
        &self,
        c: &FsCommit,
        leases_ok: impl Fn(&crate::tx::Tx) -> Result<(), CommitError>,
        lent_to_writer: impl Fn(MetaslabIdx) -> bool,
        object_ok: impl Fn(crate::disk::FileId) -> Result<(), CommitError>,
    ) -> Result<FsCommitted, CommitError> {
        let file_id = {
            let mut tx = self.begin_tx().await;
            let tx = &mut tx;
            // As for a dataset commit: asked again under the tx.
            leases_ok(tx)?;
            // Applied, it stands for the group commit; refused partway,
            // none of it does (audit S5a).
            let r = self
                .stage_fs_commit(tx, c, &lent_to_writer, &object_ok)
                .await;
            tx.check(r).await?
        };
        let txg = self.group_commit().await?;
        Ok(FsCommitted { txg, file_id })
    }

    /// Apply `c` in memory, in the open TXG (the caller holds `begin_tx`).
    /// Returns the file id at the path.
    async fn stage_fs_commit(
        &self,
        tx: &mut crate::tx::Tx,
        c: &FsCommit,
        lent_to_writer: &impl Fn(MetaslabIdx) -> bool,
        object_ok: &impl Fn(crate::disk::FileId) -> Result<(), CommitError>,
    ) -> Result<crate::disk::FileId, CommitError> {
        let refuse = |m: String| CommitError::refused(ErrorKind::Conflict, m);
        let retry = |e: FileSystemError| CommitError::Retry(e.to_string());
        // Recorded whatever happens to the op: the misses are real either way.
        self.mark_missed(&c.missed, c.birth_txg.prev());
        let file_id = match &c.op {
            FsOp::WriteFile {
                path,
                size_bytes,
                blksz,
                tree,
                allocs,
                base,
                min_birth,
            } => {
                let path = std::path::Path::new(path);
                let l = self
                    .lookup(path)
                    .await
                    .map_err(|e| CommitError::failed(path.display(), &e))?;
                let current = match self.current_file(&l).await {
                    Ok(c) => c,
                    Err(FileSystemError::NotAFile) => {
                        return Err(CommitError::refused(
                            ErrorKind::NotFile,
                            format!("{}: not a file", path.display()),
                        ));
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
                        return Err(CommitError::refused(
                            ErrorKind::NotFound,
                            format!(
                                "{}: a lease was given for a file that doesn't exist",
                                path.display()
                            ),
                        ));
                    }
                    None => {}
                }
                self.check_births(*min_birth, c.birth_txg).await?;
                self.claim_writer_allocs(tx, allocs, &c.writer.resource, lent_to_writer)
                    .await?;
                self.set_file_contents(tx, l, current, *blksz, *size_bytes, tree.clone())
                    .await
                    .map_err(retry)?
            }
            FsOp::Mkdir { path } => self
                .mkdir_locked(tx, std::path::Path::new(path))
                .await
                .map_err(|e| CommitError::failed(format!("mkdir {}", path), &e))?,
            FsOp::Delete { path, base } => {
                let p = std::path::Path::new(path);
                let l = self
                    .lookup(p)
                    .await
                    .map_err(|e| CommitError::failed(path, &e))?;
                match l.entry {
                    // Already gone (a resend, or someone else deleted it).
                    None => crate::disk::FileId::NONE,
                    Some(e) => {
                        let d = self.read_dnode(e.id).await.map_err(retry)?;
                        if base.as_ref().is_some_and(|b| *b != (e.id, d.tree.clone())) {
                            return Err(refuse(format!(
                                "{} changed since this delete was prepared",
                                path
                            )));
                        }
                        object_ok(e.id)?;
                        self.delete_locked(tx, p)
                            .await
                            .map_err(|e| CommitError::failed(format!("delete {}", path), &e))?;
                        crate::disk::FileId::NONE
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
                self.rename_locked(tx, fp, tp)
                    .await
                    .map_err(|e| CommitError::failed(format!("rename {} to {}", from, to), &e))?
                    .0
            }
            FsOp::UpdateFile {
                id,
                base,
                tree,
                size,
                allocs,
                frees,
                min_birth,
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
                self.check_births(*min_birth, c.birth_txg).await?;
                self.claim_writer_allocs(tx, allocs, &c.writer.resource, lent_to_writer)
                    .await?;
                dnode.tree = tree.clone();
                dnode.size = *size;
                dnode.attrs.touch();
                self.write_dnode(tx, *id, Some(dnode))
                    .await
                    .map_err(retry)?;
                for &Extent { start, len } in frees {
                    let _ = self.free(tx, start, len).await;
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
                self.create_at_locked(tx, *dir, name, *kind, attrs, *exclusive)
                    .await
                    .map_err(|e| CommitError::failed(format!("create {}", name), &e))?
                    .0
            }
            FsOp::RemoveAt { dir, name, id } => {
                let l = self
                    .lookup_at(*dir, name)
                    .await
                    .map_err(|e| CommitError::failed(name, &e))?;
                match l.entry {
                    // Already gone (a resend, or someone else removed it).
                    None => crate::disk::FileId::NONE,
                    Some(e) if e.id != *id => {
                        return Err(refuse(format!("{} isn't object {} any more", name, id)));
                    }
                    Some(_) => {
                        object_ok(*id)?;
                        self.remove_at_locked(tx, *dir, name)
                            .await
                            .map_err(|e| CommitError::failed(format!("remove {}", name), &e))?;
                        crate::disk::FileId::NONE
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
                    .map_err(|e| CommitError::failed(from_name, &e))?;
                let dst = self
                    .lookup_at(*to_dir, to_name)
                    .await
                    .map_err(|e| CommitError::failed(to_name, &e))?;
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
                self.rename_at_locked(tx, src, dst)
                    .await
                    .map_err(|e| {
                        CommitError::failed(format!("rename {} to {}", from_name, to_name), &e)
                    })?
                    .0
            }
            FsOp::SetAttrId { id, attrs } => {
                // Attributes aren't contents: no lease needed.
                self.set_attrs_id_locked(tx, *id, attrs)
                    .await
                    .map_err(|e| CommitError::failed(format!("object {}", id), &e))?;
                *id
            }
            FsOp::SetAttr { path, attrs } => {
                let p = std::path::Path::new(path);
                self.set_attrs_locked(tx, p, attrs)
                    .await
                    .map_err(|e| CommitError::failed(path, &e))?
            }
        };
        Ok(file_id)
    }

    /// Make everything applied so far durable. Commits that arrive while
    /// one is being written queue up behind it and are covered by the next
    /// one together (see `sync`), so concurrent writers share TXGs instead
    /// of each paying for one. Returns the newest committed TXG.
    async fn group_commit(&self) -> Result<Txg, CommitError> {
        self.sync()
            .await
            .map_err(|e| CommitError::Retry(e.to_string()))?;
        Ok(self.open_txg().await.unwrap_or(Txg::from_raw(1)).prev())
    }

    /// Mark the metaslabs a cluster has lent out (after a committer start).
    pub async fn mark_lent(&self, lent: impl IntoIterator<Item = MetaslabIdx>) {
        for idx in lent {
            if let Some(ms) = self.metaslabs.get(idx.index()) {
                ms.write().await.lent = true;
            }
        }
    }

    /// Pick a metaslab to lend: the unlent one with the most free space,
    /// none untouched by the open TXG (so the committer has no write in
    /// flight there). Marks it lent; the caller must `lend_abort` if the
    /// cluster refuses. Holds the commit lock, so no commit runs meanwhile.
    pub async fn lend_pick(&self, min_free: u64) -> Option<(MetaslabIdx, Vec<Extent>)> {
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
            MetaslabIdx::of_index(i),
            ms.free_extents
                .iter()
                .map(|(&s, &l)| Extent::new(s, l))
                .collect(),
        ))
    }

    pub async fn lend_abort(&self, idx: MetaslabIdx) {
        if let Some(ms) = self.metaslabs.get(idx.index()) {
            ms.write().await.lent = false;
        }
    }

    /// Take a metaslab back (its return went through the cluster at
    /// `epoch`: the committer writes there with it from now on).
    pub async fn unlend(&self, idx: MetaslabIdx, epoch: Epoch) {
        self.dev.pool().disks().fence().set_returned(idx, epoch);
        if let Some(ms) = self.metaslabs.get(idx.index()) {
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
    pub async fn apply_dataset_commit(
        &self,
        c: &DatasetCommit,
        leases_ok: impl Fn(&crate::tx::Tx) -> Result<(), CommitError>,
        lent_to_writer: impl Fn(MetaslabIdx) -> bool,
    ) -> Result<Txg, CommitError> {
        let mut span = crate::trace::Span::new("committer.apply");
        {
            let mut tx = self.begin_tx().await;
            let tx = &mut tx;
            span.mark("begin_tx");
            // Asked again now that nothing else is applied: the wait for
            // the tx can be long, and a lease checked before it may be gone
            // (another writer's commit for the dataset applied first).
            leases_ok(tx)?;
            let r = self.stage_dataset_commit(tx, c, &lent_to_writer).await;
            tx.check(r).await?;
            span.mark("stage");
        }
        // A resend after a failure here is acknowledged by the root check.
        let r = self.group_commit().await;
        span.mark("group_commit");
        r
    }

    /// Apply `c` in memory, in the open TXG (the caller holds `begin_tx`).
    async fn stage_dataset_commit(
        &self,
        tx: &mut crate::tx::Tx,
        c: &DatasetCommit,
        lent_to_writer: &impl Fn(MetaslabIdx) -> bool,
    ) -> Result<(), CommitError> {
        let refuse = |m: String| CommitError::refused(ErrorKind::Conflict, m);
        let vol = self
            .open_dataset(&c.dataset)
            .await
            .map_err(|e| CommitError::failed(format!("dataset '{}'", c.dataset), &e))?;
        let current = vol.obj.root_ptr();
        // A resend of a commit already applied: just make sure it's durable.
        // A commit that leaves the tree as it was (`new_root == base_root`:
        // only the intent log moves on) is recognized by its log being the
        // dataset's already; otherwise it would be applied again, its new
        // region refused as no longer free, and the writer told to reuse
        // space the committed log points into (audit S6b). If it's a no-op
        // that was never applied, acknowledging it is all applying would do.
        let log_only_applied = || {
            // Such a commit allocates nothing but its log's region(s).
            let in_log = |a: &Extent| {
                c.log
                    .as_ref()
                    .is_some_and(|l| l.regions.iter().any(|r| crate::zil::region_alloc(r) == *a))
            };
            c.allocs.iter().all(in_log) && c.frees.is_empty() && *vol.log.lock().unwrap() == c.log
        };
        let applied = current == c.new_root && (c.new_root != c.base_root || log_only_applied());
        if applied {
            self.mark_missed(&c.missed, c.birth_txg.prev());
            return Ok(());
        }
        if current != c.base_root {
            let at = |p: &Option<BlockPtr>| match p {
                Some(p) => format!("{}@{}", p.lba, p.birth_txg),
                None => "empty".into(),
            };
            return Err(refuse(format!(
                "dataset '{}' moved on (its root isn't the one this sync was built on: \
                 it's {}, the sync was built on {})",
                c.dataset,
                at(&current),
                at(&c.base_root)
            )));
        }

        // An intent log left by an earlier holder must be replayed first
        // (attaching claims the volume, see `claim_dataset`).
        if let Some(l) = vol.log.lock().unwrap().as_ref()
            && l.ds != c.ds
        {
            return Err(refuse(format!(
                "dataset '{}' has an unreplayed intent log from {}",
                c.dataset, l.writer.resource
            )));
        }
        if let Some(l) = &c.log {
            let current = vol.log.lock().unwrap().clone();
            let known = |r: &crate::inode::LogRegion| {
                current.as_ref().is_some_and(|o| o.regions.contains(r))
                    || c.allocs.contains(&crate::zil::region_alloc(r))
            };
            if l.ds != c.ds || l.writer != c.writer || !l.regions.iter().all(known) {
                return Err(refuse(format!(
                    "dataset '{}': intent log doesn't belong to this commit",
                    c.dataset
                )));
            }
        }

        self.check_births(c.min_birth, c.birth_txg).await?;
        // Every allocation must be free space in a metaslab lent to this writer.
        self.claim_writer_allocs(tx, &c.allocs, &c.writer.resource, lent_to_writer)
            .await?;
        let old_log = std::mem::replace(&mut *vol.log.lock().unwrap(), c.log.clone());
        {
            let (vol, old_log) = (vol.clone(), old_log.clone());
            tx.on_rollback(move || *vol.log.lock().unwrap() = old_log);
        }
        self.update_dataset_dnode(tx, &vol, c.new_root.clone())
            .await
            .map_err(|e| CommitError::Retry(e.to_string()))?;
        // Regions the new log no longer has are done with.
        for r in old_log.iter().flat_map(|l| &l.regions) {
            if !c.log.as_ref().is_some_and(|l| l.regions.contains(r)) {
                let _ = self.free(tx, r.start, r.len).await;
            }
        }
        vol.obj.set_root_ptr(c.new_root.clone());
        {
            let vol = vol.clone();
            tx.on_rollback(move || vol.obj.set_root_ptr(current));
        }
        // What a snapshot still has stays (see `crate::snapshot`).
        let held = self.held_txg_of(vol.id).await.unwrap_or(Txg::MAX);
        let births = c
            .free_births
            .iter()
            .copied()
            .chain(std::iter::repeat(Txg::ZERO));
        let frees: Vec<crate::types::Born> = c
            .frees
            .iter()
            .zip(births)
            .map(|(&extent, birth)| crate::types::Born { extent, birth })
            .collect();
        let _ = self.free_unheld(tx, frees, held).await;
        self.mark_missed(&c.missed, c.birth_txg.prev());
        Ok(())
    }
}
