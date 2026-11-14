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
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::object::{Dirty, ObjTree, Sizing};
use crate::writer::{CommitError, FsOp, ObjLease, WriterState};
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
}

impl OpenFile {
    pub fn size(&self) -> u64 {
        self.obj.size()
    }

    fn needs_sync(&self) -> bool {
        self.obj.is_dirty() || self.resized.load(Ordering::SeqCst)
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Open the file at `path` for reading and writing at any offset,
    /// optionally under its `obj/<id>` lease. Opening an open file returns
    /// the same one.
    pub async fn open_file(
        &self,
        path: &Path,
        lease: Option<ObjLease>,
    ) -> Result<Arc<OpenFile>, FileSystemError> {
        if self.writer.is_some() {
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
        if self.volumes_dirty_bytes() + self.files_dirty_bytes() > crate::volume::DIRTY_LIMIT_BYTES
        {
            self.commit_txg().await?;
        }
        Ok(n)
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
        match f.conflict.lock().unwrap().clone() {
            Some(m) => Err(FileSystemError::Conflict(m)),
            None => Ok(()),
        }
    }

    /// Sync and forget an open file.
    pub async fn close_file(&self, f: &OpenFile) -> Result<(), FileSystemError> {
        let r = self.file_sync(f).await;
        self.files.lock().unwrap().remove(&f.id);
        r
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

    /// Sync every open file with changes into the open TXG. Called by
    /// commits (under the commit lock), after the volumes. Each file syncs
    /// all-or-nothing; one that fails keeps its changes for the next try,
    /// unless it was replaced or deleted elsewhere (`Conflict`): then they
    /// are dropped, the file closed, and only its own `file_sync` fails.
    pub(crate) async fn sync_files(&self) -> Result<(), FileSystemError> {
        let open: Vec<Arc<OpenFile>> = self.files.lock().unwrap().values().cloned().collect();
        let mut first_err = None;
        for f in open {
            if !f.needs_sync() {
                continue;
            }
            let resized = f.resized.swap(false, Ordering::SeqCst);
            let taken = f.obj.begin_sync();
            let empty = BTreeMap::new();
            let records = taken.as_deref().unwrap_or(&empty);
            let result = self.sync_file(&f, records).await;
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

    async fn sync_file(
        &self,
        f: &OpenFile,
        records: &BTreeMap<u64, Dirty>,
    ) -> Result<(), FileSystemError> {
        let need = self.tree_sync_reservation(&f.obj, records) + 2 * self.max_record_blocks();
        if let Some(w) = self.writer.clone() {
            return self.writer_sync_file(&w, f, records, need).await;
        }
        if self.allocatable_blocks().await < need {
            return Err(FileSystemError::UnableToAllocateBlocks(need));
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
        dnode.size = f.obj.size();
        dnode.attrs.touch();
        if let Err(e) = self.write_dnode(f.id, Some(dnode)).await {
            self.release_ptrs(&allocated).await;
            return Err(e);
        }
        f.obj.set_root(tree);
        for p in &replaced {
            self.free(p.lba, p.asize as u64).await?;
        }
        Ok(())
    }

    /// `sync_file` for a writer: build the tree in lent space, then have
    /// the committer point the dnode at it.
    async fn writer_sync_file(
        &self,
        w: &WriterState,
        f: &OpenFile,
        records: &BTreeMap<u64, Dirty>,
        need: u64,
    ) -> Result<(), FileSystemError> {
        self.lease_space(w, need).await?;
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
        let ext = |p: &BlockPtr| (p.lba, p.asize as u64);
        let op = FsOp::UpdateFile {
            id: f.id,
            base,
            tree: tree.clone(),
            size: f.obj.size(),
            allocs: allocated.iter().map(ext).collect(),
            frees: replaced.iter().map(ext).collect(),
        };
        let lease = f.lease.lock().unwrap().clone();
        match self.writer_commit_fs(w, op, lease).await {
            Ok(_) => {
                f.obj.set_root(tree);
                Ok(())
            }
            Err(CommitError::Refused(m)) => {
                // Definitely not applied: the space is ours to reuse.
                self.release_ptrs(&allocated).await;
                Err(FileSystemError::Conflict(m))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free).
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
#[path = "tests/open_file_tests.rs"]
mod tests;
