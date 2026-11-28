//! Files open for writing at any offset.
//!
//! An open file is its tree (see `crate::object`) with dirty records, like
//! a volume's, but growable: writes past the end extend it, and it can be
//! truncated. Every commit syncs it: the dirty records go into a new tree
//! and the file's dnode is pointed at it, with a compare-and-swap on the
//! tree the writes were made on, so a file someone else replaced or deleted
//! meanwhile is never overwritten (or brought back). On a writer node the
//! tree is written into lent space and the committer applies it
//! (`FsOp::UpdateFile`), under the file's `obj/<id>` lease if one is held.
//!
//! `file_sync` returns once everything written before it is committed.
//! (Unlike a volume's, a file's flush doesn't go to an intent log yet: it
//! is a commit.)

use crate::blkptr::BlockPtr;
use crate::disk::{FileId, PoolDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::object::{Dirty, ObjTree, Sizing};
use crate::role::{Committer, MutView, Mutable, Role, Writer};
use crate::writer::{CommitError, FsOp, ObjLease};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct OpenFile {
    pub id: FileId,
    pub(crate) obj: ObjTree,
    /// The size changed with no record to sync (a truncate that only
    /// grows it): the dnode still needs rewriting.
    resized: AtomicBool,
    /// The `obj/<id>` lease writes are made under, if any.
    lease: std::sync::Mutex<Option<ObjLease>>,
    /// Set when its writes were dropped because the file was replaced or
    /// deleted meanwhile; reported by `file_sync` (not by the commit,
    /// which is fine).
    conflict: std::sync::Mutex<Option<String>>,
    /// On a writer: an update sent whose outcome is unknown. Resent before
    /// anything new is built: built on the old tree, a new update would be
    /// refused for good ("changed") if that one was applied, and the file's
    /// writes dropped with it (audit S6c-2, as `Dataset::in_doubt`).
    in_doubt: std::sync::Mutex<Option<FileInDoubt>>,
}

/// An `UpdateFile` sent, its answer not acted on.
#[derive(Clone)]
struct FileInDoubt {
    op: FsOp,
    tree: crate::object::TreeRoot,
    allocated: Vec<BlockPtr>,
}

impl OpenFile {
    /// Its unsynced writes were dropped (the file was replaced or deleted
    /// elsewhere), and it was closed.
    pub fn dropped(&self) -> bool {
        self.conflict.lock().unwrap().is_some()
    }

    pub fn size(&self) -> u64 {
        self.obj.size()
    }

    pub fn needs_sync(&self) -> bool {
        self.obj.is_dirty() || self.resized.load(Ordering::SeqCst)
    }
}

/// A file's sync under way (`sync_files`). Dropped before it settles (the
/// commit's future was dropped), it gives the taken records and the resize
/// back, as a failed sync does. Otherwise the file would look clean, with
/// its writes in `syncing` for good, never committed (structural triage
/// I-6).
struct FileSyncOnDrop<'a> {
    f: &'a OpenFile,
    taken: Option<Arc<BTreeMap<u64, Dirty>>>,
    resized: bool,
}

impl FileSyncOnDrop<'_> {
    /// The sync's outcome is in: its caller settles the records itself.
    fn settle(mut self) -> (Option<Arc<BTreeMap<u64, Dirty>>>, bool) {
        (self.taken.take(), std::mem::take(&mut self.resized))
    }
}

impl Drop for FileSyncOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(taken) = self.taken.take() {
            self.f.obj.end_sync(&taken, None);
        }
        if self.resized {
            self.f.resized.store(true, Ordering::SeqCst);
        }
    }
}

impl<D: PoolDevice, R: Role> FileSystem<D, R> {
    /// Open the file at `path` for reading and writing at any offset,
    /// optionally under its `obj/<id>` lease. Opening an open file returns
    /// the same one.
    pub async fn open_file(
        &self,
        path: &Path,
        lease: Option<ObjLease>,
    ) -> Result<Arc<OpenFile>, FileSystemError> {
        if self.role.writer().is_some() {
            self.reload_root_index().await?;
        }
        let (id, _) = self.stat_file(path).await?;
        self.open_file_id(id, lease).await
    }

    /// `open_file` by object number. Opening an open file returns the same
    /// one (with `lease`, if given).
    pub async fn open_file_id(
        &self,
        id: FileId,
        lease: Option<ObjLease>,
    ) -> Result<Arc<OpenFile>, FileSystemError> {
        if let Some(f) = self.files.lock().unwrap().get(&id).cloned() {
            if lease.is_some() {
                *f.lease.lock().unwrap() = lease;
            }
            return Ok(f);
        }
        let dnode = self.read_dnode(id).await?;
        if !dnode.is_file() {
            return Err(FileSystemError::NotAFile);
        }
        let mut files = self.files.lock().unwrap();
        let f = files
            .entry(id)
            .or_insert_with(|| {
                Arc::new(OpenFile {
                    id,
                    obj: ObjTree::new(
                        dnode.blksz as usize,
                        Sizing::Growable,
                        dnode.size,
                        dnode.tree,
                    ),
                    resized: AtomicBool::new(false),
                    lease: std::sync::Mutex::new(None),
                    conflict: std::sync::Mutex::new(None),
                    in_doubt: std::sync::Mutex::new(None),
                })
            })
            .clone();
        if lease.is_some() {
            *f.lease.lock().unwrap() = lease;
        }
        Ok(f)
    }

    /// Read from an open file (its unsynced writes included).
    pub async fn file_read(
        &self,
        f: &OpenFile,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        self.object_read(&f.obj, offset, buf).await
    }

    /// Where the next data (`data`) or hole is at or after `offset` (see
    /// `object_seek`).
    pub async fn file_seek(
        &self,
        f: &OpenFile,
        offset: u64,
        data: bool,
    ) -> Result<Option<u64>, FileSystemError> {
        self.object_seek(&f.obj, offset, data).await
    }

    /// The file `id`, if it's open here.
    pub fn opened(&self, id: FileId) -> Option<Arc<OpenFile>> {
        self.files.lock().unwrap().get(&id).cloned()
    }

    /// Forget an open file without syncing it (it was deleted).
    pub(crate) fn discard_file(&self, id: FileId) {
        self.files.lock().unwrap().remove(&id);
    }

    pub(crate) fn files_dirty_bytes(&self) -> usize {
        self.files
            .lock()
            .unwrap()
            .values()
            .map(|f| f.obj.dirty_bytes())
            .sum()
    }
}

impl<D: PoolDevice> FileSystem<D, Writer> {
    /// `sync_file` for a writer: build the tree in lent space, then have
    /// the committer point the dnode at it.
    async fn writer_sync_file(
        &self,
        f: &OpenFile,
        records: &BTreeMap<u64, Dirty>,
        size: u64,
        need: u64,
    ) -> Result<(), FileSystemError> {
        self.resolve_file_in_doubt(f).await?;
        self.lease_space(need).await?;
        let base = f.obj.root();
        let (mut allocated, mut replaced) = (Vec::new(), Vec::new());
        let tree = match self
            .build_tree(&f.obj, records, &mut allocated, &mut replaced)
            .await
        {
            Ok(t) => t,
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
        let ext = |p: &BlockPtr| p.extent();
        let op = FsOp::UpdateFile {
            id: f.id,
            base,
            tree: tree.clone(),
            size,
            allocs: allocated.iter().map(ext).collect(),
            frees: replaced.iter().map(ext).collect(),
            min_birth: allocated.iter().map(|p| p.birth_txg).min(),
        };
        let sent = FileInDoubt {
            op,
            tree,
            allocated,
        };
        *f.in_doubt.lock().unwrap() = Some(sent.clone());
        self.send_file_update(f, sent).await
    }

    /// Send (or resend) `sent` and act on the answer.
    async fn send_file_update(
        &self,
        f: &OpenFile,
        sent: FileInDoubt,
    ) -> Result<(), FileSystemError> {
        let lease = f.lease.lock().unwrap().clone();
        let r = self.writer_commit_fs(sent.op, lease).await;
        match r {
            Ok(_) => {
                *f.in_doubt.lock().unwrap() = None;
                f.obj.set_root(sent.tree);
                Ok(())
            }
            Err(e @ CommitError::Refused { .. }) => {
                // Definitely not applied: the space is ours to reuse.
                *f.in_doubt.lock().unwrap() = None;
                self.release_ptrs(&sent.allocated).await;
                Err(e.into())
            }
            Err(CommitError::Restamp(open)) => {
                // Not applied: rebuilt next round (the records stay dirty).
                *f.in_doubt.lock().unwrap() = None;
                self.release_ptrs(&sent.allocated).await;
                self.note_open_txg(open).await;
                Err(FileSystemError::Restamp(open))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free),
            // and the update in doubt for the next sync to resend.
            Err(e) => Err(e.into()),
        }
    }

    /// Settle `f`'s update in doubt, if any, by resending it: the committer
    /// acknowledges one it applied, applies one it never saw, or refuses
    /// one that can't be any more (then the next build starts from the
    /// tree as it was, with the records, kept dirty, on top). Err: still
    /// unknown.
    async fn resolve_file_in_doubt(&self, f: &OpenFile) -> Result<(), FileSystemError> {
        let Some(sent) = f.in_doubt.lock().unwrap().clone() else {
            return Ok(());
        };
        match self.send_file_update(f, sent).await {
            Err(FileSystemError::Unavailable(m)) => Err(FileSystemError::Unavailable(m)),
            Err(e) => {
                eprintln!("file {}: an earlier update wasn't applied: {}", f.id, e);
                Ok(())
            }
            Ok(()) => Ok(()),
        }
    }
}

impl<D: PoolDevice, R: Mutable> FileSystem<D, R> {
    /// Write `data` at `offset`, extending the file if it ends before.
    pub async fn file_write(
        &self,
        f: &OpenFile,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, FileSystemError> {
        let n = self
            .object_modify(&f.obj, offset, data.len(), Some(data))
            .await?;
        if self.datasets_dirty_bytes() + self.files_dirty_bytes() > crate::volume::DIRTY_LIMIT_BYTES
        {
            self.commit_txg().await?;
        }
        Ok(n)
    }

    /// Return once everything written to `f` so far is committed. Fails
    /// with `Conflict` if its writes were dropped because the file was
    /// replaced or deleted meanwhile (`f` is closed then).
    pub async fn file_sync(&self, f: &OpenFile) -> Result<(), FileSystemError> {
        if f.needs_sync() {
            self.sync().await?;
            if f.needs_sync() && f.conflict.lock().unwrap().is_none() {
                // Its sync failed (the error was reported to the commit), or
                // more was written since: one more round.
                self.sync().await?;
            }
        }
        if let Some(m) = f.conflict.lock().unwrap().clone() {
            return Err(FileSystemError::Conflict(m));
        }
        // Not the pool's open file any more (closed meanwhile): no commit
        // syncs it, so what's still dirty in it never will be (audit N1).
        if f.needs_sync() && !self.opened(f.id).is_some_and(|o| std::ptr::eq(&*o, f)) {
            return Err(FileSystemError::Conflict(format!(
                "file {} was closed with writes not committed",
                f.id
            )));
        }
        Ok(())
    }

    /// Sync and forget an open file. Forgotten even if the sync failed:
    /// its callers let the file's lease go next, and a commit must not
    /// write it after that (they report the writes as lost). Only `f`
    /// itself is forgotten: the file may have been closed and opened again
    /// meanwhile, by someone else (structural triage I-6).
    pub async fn close_file(&self, f: &OpenFile) -> Result<(), FileSystemError> {
        let r = self.file_sync(f).await;
        let mut files = self.files.lock().unwrap();
        if files.get(&f.id).is_some_and(|o| std::ptr::eq(&**o, f)) {
            files.remove(&f.id);
        }
        r
    }

    /// Sync `records` (taken from `f`) and `size`, the file's size as of
    /// them.
    async fn sync_file(
        &self,
        tx: &mut crate::tx::Tx,
        f: &OpenFile,
        records: &BTreeMap<u64, Dirty>,
        size: u64,
    ) -> Result<(), FileSystemError> {
        let need = self.tree_sync_reservation(&f.obj, records) + 2 * self.max_record_blocks();
        match self.mutable() {
            MutView::Writer(fs) => fs.writer_sync_file(f, records, size, need).await,
            MutView::Committer(fs) => fs.sync_file_local(tx, f, records, size, need).await,
        }
    }

    /// Sync every open file with changes into the open TXG. Called by
    /// commits (under the commit lock), after the volumes. Each file syncs
    /// all-or-nothing; one that fails keeps its changes for the next try,
    /// unless it was replaced or deleted elsewhere (`Conflict`): then they
    /// are dropped, the file closed, and only its own `file_sync` fails.
    pub(crate) async fn sync_files(&self, tx: &mut crate::tx::Tx) -> Result<(), FileSystemError> {
        let open: Vec<Arc<OpenFile>> = self.files.lock().unwrap().values().cloned().collect();
        let mut first_err = None;
        for f in open {
            if !f.needs_sync() {
                continue;
            }
            let resized = f.resized.swap(false, Ordering::SeqCst);
            let (taken, size) = f.obj.begin_sync_sized();
            let guard = FileSyncOnDrop {
                f: &f,
                taken,
                resized,
            };
            let empty = BTreeMap::new();
            let records = guard.taken.as_deref().unwrap_or(&empty);
            let result = self.sync_file(tx, &f, records, size).await;
            let (taken, resized) = guard.settle();
            let dropped = matches!(result, Err(FileSystemError::Conflict(_)));
            if let Some(t) = &taken
                && !dropped
            {
                f.obj.end_sync(t, result.is_ok().then_some(t.as_ref()));
            }
            match result {
                Ok(()) => {}
                Err(FileSystemError::Conflict(m)) => {
                    eprintln!("file {}: dropping unsynced writes: {}", f.id, m);
                    // Nothing of it may be written any more.
                    if let Some(t) = &taken {
                        f.obj.end_sync(t, Some(t));
                    }
                    f.obj.dirty.lock().unwrap().records.clear();
                    *f.conflict.lock().unwrap() = Some(m);
                    self.discard_file(f.id);
                }
                Err(e) => {
                    if resized {
                        f.resized.store(true, Ordering::SeqCst);
                    }
                    first_err.get_or_insert(e);
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Set the file's size: cut it short, or extend it with zeros.
    pub async fn file_truncate(&self, f: &OpenFile, size: u64) -> Result<(), FileSystemError> {
        self.object_truncate(&f.obj, size).await?;
        f.resized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Zero `len` bytes at `offset` (within the file: it doesn't grow).
    /// Whole records become holes and give their space back when synced.
    pub async fn file_zero(
        &self,
        f: &OpenFile,
        offset: u64,
        len: u64,
    ) -> Result<(), FileSystemError> {
        let size = f.size();
        if offset >= size || len == 0 {
            return Ok(());
        }
        let len = len.min(size - offset).min(usize::MAX as u64) as usize;
        self.object_modify(&f.obj, offset, len, None).await?;
        Ok(())
    }

    /// Forget an open file and every change to it not committed yet: for
    /// a writer of `f` (under its lease, so the only one) whose writes
    /// failed partway, so they aren't committed after. Taken under the
    /// commit lock, so no sync of them is under way. What a commit already
    /// took stays (a write too big to hold may have been partly committed,
    /// like a `pwrite` that fails), and so does what the intent log has
    /// until the next commit.
    pub async fn file_discard(&self, f: &Arc<OpenFile>) {
        let _tx = self.begin_tx().await;
        {
            let mut d = f.obj.dirty.lock().unwrap();
            d.records.clear();
            d.unlogged.clear();
        }
        f.resized.store(false, Ordering::SeqCst);
        let mut files = self.files.lock().unwrap();
        if files.get(&f.id).is_some_and(|o| Arc::ptr_eq(o, f)) {
            files.remove(&f.id);
        }
    }
}

impl<D: PoolDevice> FileSystem<D, Committer> {
    /// `sync_file` on the committer: `need` blocks are what it may take.
    async fn sync_file_local(
        &self,
        tx: &mut crate::tx::Tx,
        f: &OpenFile,
        records: &BTreeMap<u64, Dirty>,
        size: u64,
        need: u64,
    ) -> Result<(), FileSystemError> {
        if self.allocatable_blocks().await < need {
            return Err(FileSystemError::NoSpace(format!("{} blocks", need)));
        }
        let mut dnode = match self.try_read_dnode(f.id).await? {
            Some(d) if d.is_file() => d,
            _ => {
                return Err(FileSystemError::Conflict(format!(
                    "file {} was deleted",
                    f.id
                )));
            }
        };
        if dnode.tree != f.obj.root() {
            return Err(FileSystemError::Conflict(format!(
                "file {} was replaced",
                f.id
            )));
        }
        let (mut allocated, mut replaced) = (Vec::new(), Vec::new());
        let tree = match self
            .build_tree(&f.obj, records, &mut allocated, &mut replaced)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                self.release_ptrs(&allocated).await;
                return Err(e);
            }
        };
        dnode.tree = tree.clone();
        dnode.size = size;
        dnode.attrs.touch();
        // Given up on before the dnode points at it, the new tree is
        // given back (structural triage I-1).
        let mut unsettled = crate::object::Unsettled::all(self, &mut allocated);
        if let Err(e) = self.write_dnode(tx, f.id, Some(dnode)).await {
            unsettled.disarm();
            self.release_ptrs(unsettled.ptrs).await;
            return Err(e);
        }
        unsettled.disarm();
        f.obj.set_root(tree);
        // As in `sync_table`: the rest freed all the same if given up on.
        let mut left = crate::object::Unsettled::all(self, &mut replaced);
        while let Some(p) = left.ptrs.last().cloned() {
            self.free_record(tx, &p).await?;
            left.ptrs.pop();
        }
        left.disarm();
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/open_file_tests.rs"]
mod tests;
