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
}

/// The committer's answer to a commit.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    /// The TXG that made it durable.
    pub txg: u64,
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
        FileSystemError::InvalidArgument(e.to_string())
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
}

/// A volume a writer runs.
pub(crate) struct Attached {
    /// The `vol/<name>` lease owner and epoch.
    pub owner: String,
    pub epoch: u64,
    /// Turns true once that lease is lost: a commit still waiting for the
    /// committer then gives up (see `attach_volume_until`).
    pub lost: Option<tokio::sync::watch::Receiver<bool>>,
}

/// A writer's side of things.
pub(crate) struct WriterState {
    pub sink: Arc<dyn CommitSink>,
    /// Volume name → its lease.
    pub volumes: std::sync::Mutex<HashMap<String, Attached>>,
    /// Metaslabs lent to this writer.
    pub lent: std::sync::Mutex<Vec<u32>>,
}

/// Give a metaslab back once it has less than this many free blocks.
const RETURN_BELOW_RECORDS: u64 = 4;

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
        let w = self
            .writer
            .as_ref()
            .ok_or(FileSystemError::InvalidArgument(
                "not a volume writer".into(),
            ))?;
        // Reload the index: the volume may have been created or moved here
        // since this writer opened the pool.
        self.reload_root_index().await?;
        self.close_volume(name);
        self.open_volume(name).await?;
        w.volumes.lock().unwrap().insert(
            name.to_string(),
            Attached {
                owner: owner.to_string(),
                epoch,
                lost,
            },
        );
        Ok(())
    }

    /// Stop running `name`: sync what's dirty, then forget it.
    pub async fn detach_volume(&self, name: &str) -> Result<(), FileSystemError> {
        let r = self.sync().await;
        if let Some(w) = &self.writer {
            w.volumes.lock().unwrap().remove(name);
        }
        self.close_volume(name);
        r
    }

    /// Drop `name` without syncing (its lease is gone: nothing may be written).
    pub fn abandon_volume(&self, name: &str) {
        if let Some(w) = &self.writer {
            w.volumes.lock().unwrap().remove(name);
        }
        self.close_volume(name);
    }

    /// Give every lent metaslab back (after the last sync).
    pub async fn return_metaslabs(&self) {
        let Some(w) = &self.writer else { return };
        let _tx = self.begin_tx().await;
        let lent: Vec<u32> = std::mem::take(&mut *w.lent.lock().unwrap());
        for idx in lent {
            self.drop_lent(idx).await;
            if let Err(e) = w.sink.return_metaslab(idx).await {
                eprintln!("writer: returning metaslab {}: {}", idx, e);
            }
        }
    }

    /// Re-read the newest committed root index (a writer's view of the
    /// pool goes stale as the committer commits).
    pub(crate) async fn reload_root_index(&self) -> Result<(), FileSystemError> {
        let ring = self.read_uberblock_ring().await;
        let newest = ring.first().ok_or(FileSystemError::NotFormatted)?;
        let index: crate::inode::FileIndex = self.read_value(&newest.root).await?;
        *self.file_index.write().await = index;
        *self.root_ptr.lock().await = newest.root.clone();
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
    async fn lease_space(&self, w: &WriterState, need: u64) -> Result<(), FileSystemError> {
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
    async fn return_full(&self, w: &WriterState) {
        let threshold = RETURN_BELOW_RECORDS * self.max_record_blocks();
        let lent: Vec<u32> = w.lent.lock().unwrap().clone();
        for idx in lent {
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
        let result = self.sync_volumes().await;
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
        records: &std::collections::BTreeMap<u64, crate::volume::Dirty>,
        need: u64,
    ) -> Result<(), FileSystemError> {
        let (vol_owner, vol_epoch, lost) = w
            .volumes
            .lock()
            .unwrap()
            .get(&vol.name)
            .map(|a| (a.owner.clone(), a.epoch, a.lost.clone()))
            .ok_or_else(|| {
                FileSystemError::InvalidArgument(format!("volume '{}' isn't attached", vol.name))
            })?;
        self.lease_space(w, need).await?;

        let birth_txg = self.open_txg().await?;
        let base_root = vol.root_ptr();
        let mut allocated: Vec<BlockPtr> = Vec::new();
        let mut replaced: Vec<BlockPtr> = Vec::new();
        let new_root = match self
            .build_tree(vol, records, &mut allocated, &mut replaced)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.release_ptrs(&allocated).await;
                return Err(e);
            }
        };
        // The records must be on disk before the committer points at them.
        if let Err(e) = self.dev.flush().await {
            self.release_ptrs(&allocated).await;
            return Err(e.into());
        }
        let ext = |p: &BlockPtr| {
            let Extent { start, len } = p.extent();
            (start, len)
        };
        let (writer, writer_epoch) = w.sink.writer();
        let commit = VolumeCommit {
            volume: vol.name.clone(),
            writer,
            writer_epoch,
            vol_owner,
            vol_epoch,
            base_root,
            new_root: new_root.clone(),
            allocs: allocated.iter().map(ext).collect(),
            frees: replaced.iter().map(ext).collect(),
            missed: self.take_new_misses(),
            birth_txg,
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
                vol.set_root(new_root);
                Ok(())
            }
            Err(CommitError::Refused(m)) => {
                // Definitely not applied: the space is ours to reuse.
                self.release_ptrs(&allocated).await;
                Err(FileSystemError::InvalidArgument(format!(
                    "commit refused: {}",
                    m
                )))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free).
            Err(e) => Err(e.into()),
        }
    }

    // ---- the committer's side ----

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
        let refuse = |m: String| CommitError::Refused(m);
        let _tx = self.begin_tx().await;
        let vol = self
            .open_volume(&c.volume)
            .await
            .map_err(|e| refuse(format!("volume '{}': {}", c.volume, e)))?;
        let current = vol.root_ptr();
        if current == c.new_root && c.new_root != c.base_root {
            // A resend of a commit already applied: just make sure it's durable.
            return self
                .commit_locked()
                .await
                .map_err(|e| CommitError::Retry(e.to_string()));
        }
        if current != c.base_root {
            return Err(refuse(format!(
                "volume '{}' moved on (its root isn't the one this sync was built on)",
                c.volume
            )));
        }

        // Every allocation must be free space in a metaslab lent to this writer.
        for &(start, len) in &c.allocs {
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
                    start, len, idx, c.writer
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
        for &(start, len) in &c.allocs {
            let idx = self.layout.metaslab_for_block(start).unwrap();
            self.metaslabs[idx].write().await.mark_allocated(start, len);
        }

        let old_inode = match self.update_volume_inode(&vol, c.new_root.clone()).await {
            Ok(old) => old,
            Err(e) => {
                for &(start, len) in &c.allocs {
                    self.unallocate(start, len).await;
                }
                return Err(CommitError::Retry(e.to_string()));
            }
        };
        vol.set_root(c.new_root.clone());
        for ptr in &old_inode {
            let e = ptr.extent();
            let _ = self.free(e.start, e.len).await;
        }
        for &(start, len) in &c.frees {
            let _ = self.free(start, len).await;
        }
        for &d in &c.missed {
            self.mark_degraded(d, c.birth_txg.saturating_sub(1));
        }
        // Applied in memory; the TXG stays open until it commits, and a
        // resend after a failure here is acknowledged by the root check.
        self.commit_locked()
            .await
            .map_err(|e| CommitError::Retry(e.to_string()))
    }
}
