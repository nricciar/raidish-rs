//! Streaming file writes: a file of any size is written record by record as
//! the data arrives, without holding it all in memory or holding any lock
//! while it uploads.
//!
//! A [`FileWriter`] creates or replaces one file's contents. Each full record
//! goes to disk as soon as it fills (on a writer, into its lent metaslabs,
//! which stay pinned until the file is committed); `finish` writes the
//! indirect blocks of the new tree and points the file's dnode at it with a
//! compare-and-swap on the contents it saw when it was opened: if someone
//! else changed the file meanwhile, the write is refused and its space
//! released. A replaced file keeps its object number.
//!
//! On a cluster, a long write can hold the file's `obj/<id>` lease (see
//! `crate::committer::object_resource`), so others are refused up front
//! instead of at the end; `finish` is refused if that lease was lost.

use crate::blkptr::BlockPtr;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Dnode, Lookup};
use crate::object::TreeRoot;
use crate::writer::{CommitError, FsOp, ObjLease, path_string};
use std::path::{Path, PathBuf};

/// A file being written. Call `finish` to commit it or `abort` to give its
/// space back; dropped without either, its records are left unreferenced
/// until the pool is reopened (a writer's never reach the committer).
pub struct FileWriter<'a, D: BlockDevice> {
    fs: &'a FileSystem<D>,
    path: PathBuf,
    /// The file and its contents when opened (None: it didn't exist).
    base: Option<(FileId, TreeRoot)>,
    lease: Option<ObjLease>,
    blksz: usize,
    /// Data records written so far: `leaves[i]` is record `i`.
    leaves: Vec<BlockPtr>,
    /// The new tree's indirect blocks (written by `finish`).
    indirects: Vec<BlockPtr>,
    pending: Vec<u8>,
    size: u64,
    /// Lent metaslabs pinned by what's written (a writer only), one per record.
    pinned: Vec<u32>,
}

/// How many times a record write borrows more space before giving up
/// (other writes on this node may take what was borrowed first).
const ALLOC_ATTEMPTS: usize = 4;

impl<D: BlockDevice> FileSystem<D> {
    /// Start writing `path` (created on `finish` if it doesn't exist,
    /// replaced if it does), optionally under the file's `obj/<id>` lease.
    pub async fn file_writer(
        &self,
        path: &Path,
        lease: Option<ObjLease>,
    ) -> Result<FileWriter<'_, D>, FileSystemError> {
        if self.writer.is_some() {
            // See what's committed now: that's what `finish` swaps against.
            self.reload_root_index().await?;
        }
        let base = match self.lookup(path).await?.entry {
            Some(e) => {
                let d = self.read_dnode(e.id).await?;
                if !d.is_file() {
                    return Err(FileSystemError::NotAFile);
                }
                Some((e.id, d.tree))
            }
            None => None,
        };
        if lease.is_some() && base.is_none() {
            return Err(FileSystemError::InvalidArgument(
                "an object lease needs an existing file (create it first)".into(),
            ));
        }
        Ok(FileWriter {
            fs: self,
            path: path.to_path_buf(),
            base,
            lease,
            blksz: self.file_blksz() as usize,
            leaves: Vec::new(),
            indirects: Vec::new(),
            pending: Vec::new(),
            size: 0,
            pinned: Vec::new(),
        })
    }

    /// Make sure `path` exists as a file (empty if new), and return its
    /// object number: what an `obj/<id>` lease is taken on.
    pub async fn create_file(&self, path: &Path) -> Result<FileId, FileSystemError> {
        for _ in 0..ALLOC_ATTEMPTS {
            let w = self.file_writer(path, None).await?;
            if let Some((id, _)) = &w.base {
                return Ok(*id);
            }
            match w.finish().await {
                Ok(id) => return Ok(id),
                // Someone else created it first: theirs it is.
                Err(FileSystemError::Conflict(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(FileSystemError::Conflict(format!(
            "{} keeps changing",
            path.display()
        )))
    }

    /// The object number of whatever is at `path` (file, directory or volume).
    pub async fn file_id(&self, path: &Path) -> Result<FileId, FileSystemError> {
        let entry = self.lookup(path).await?.entry;
        Ok(entry.ok_or(FileSystemError::FileNotFound)?.id)
    }

    /// Give file contents written elsewhere (`tree`, `size` bytes of
    /// `blksz` records) to the file at `l`: replace `current`'s (freeing
    /// the old), or create the file. Under `begin_tx`; the caller has done
    /// the compare-and-swap. Returns the file's object number.
    pub(crate) async fn set_file_contents(
        &self,
        l: Lookup,
        current: Option<(FileId, Dnode)>,
        blksz: u32,
        size: u64,
        tree: TreeRoot,
    ) -> Result<FileId, FileSystemError> {
        match current {
            Some((id, mut dnode)) => {
                dnode.blksz = blksz;
                self.replace_file_contents(id, dnode, size, tree).await?;
                Ok(id)
            }
            None => {
                let mut dnode = Dnode::file(blksz);
                dnode.size = size;
                dnode.tree = tree;
                self.link_new(l, dnode).await
            }
        }
    }

    /// The file at `l` with its dnode (None: nothing there). Refused if
    /// something else is there.
    pub(crate) async fn current_file(
        &self,
        l: &Lookup,
    ) -> Result<Option<(FileId, Dnode)>, FileSystemError> {
        let Some(e) = l.entry else {
            return Ok(None);
        };
        let d = self.read_dnode(e.id).await?;
        if !d.is_file() {
            return Err(FileSystemError::NotAFile);
        }
        Ok(Some((e.id, d)))
    }
}

impl<'a, D: BlockDevice> FileWriter<'a, D> {
    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.size + self.pending.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append `data`. Full records are written right away.
    pub async fn write(&mut self, mut data: &[u8]) -> Result<(), FileSystemError> {
        let rec = self.blksz;
        while !data.is_empty() {
            if self.pending.is_empty() && data.len() >= rec {
                // Straight from the caller's buffer.
                let (chunk, rest) = data.split_at(rec);
                self.write_record(chunk).await?;
                data = rest;
                continue;
            }
            let take = (rec - self.pending.len()).min(data.len());
            self.pending.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.pending.len() == rec {
                let chunk = std::mem::take(&mut self.pending);
                self.write_record(&chunk).await?;
            }
        }
        Ok(())
    }

    /// On a writer, borrow space for `blocks` more before allocating.
    async fn reserve(&self, blocks: u64) -> Result<(), FileSystemError> {
        if let Some(w) = &self.fs.writer {
            self.fs
                .lease_space(w, blocks + self.fs.max_record_blocks())
                .await?;
        }
        Ok(())
    }

    fn pin(&mut self, ptrs: &[BlockPtr]) {
        if let Some(w) = &self.fs.writer {
            for p in ptrs {
                if let Some(idx) = self.fs.layout.metaslab_for_block(p.lba) {
                    w.pin(idx as u32);
                    self.pinned.push(idx as u32);
                }
            }
        }
    }

    async fn write_record(&mut self, chunk: &[u8]) -> Result<(), FileSystemError> {
        let fs = self.fs;
        let blocks = fs.align_blocks(chunk.len().div_ceil(crate::disk::BLOCK_SIZE) as u64);
        let mut attempt = 0;
        let ptr = loop {
            self.reserve(blocks).await?;
            match fs.write_record(chunk).await {
                Err(FileSystemError::UnableToAllocateBlocks(_))
                    if fs.writer.is_some() && attempt + 1 < ALLOC_ATTEMPTS =>
                {
                    attempt += 1;
                }
                r => break r?,
            }
        };
        self.pin(std::slice::from_ref(&ptr));
        self.size += chunk.len() as u64;
        self.leaves.push(ptr);
        Ok(())
    }

    /// Write the indirect blocks over the records written.
    async fn build(&mut self) -> Result<TreeRoot, FileSystemError> {
        let fs = self.fs;
        let mut attempt = 0;
        loop {
            self.reserve(fs.fresh_indirect_blocks(self.leaves.len() as u64))
                .await?;
            let mut indirects = Vec::new();
            match fs.fresh_tree(&self.leaves, &mut indirects).await {
                Ok(tree) => {
                    self.pin(&indirects);
                    self.indirects = indirects;
                    return Ok(tree);
                }
                Err(FileSystemError::UnableToAllocateBlocks(_))
                    if fs.writer.is_some() && attempt + 1 < ALLOC_ATTEMPTS =>
                {
                    fs.release_ptrs(&indirects).await;
                    attempt += 1;
                }
                Err(e) => {
                    fs.release_ptrs(&indirects).await;
                    return Err(e);
                }
            }
        }
    }

    fn unpin(&mut self) {
        if let Some(w) = &self.fs.writer {
            for idx in self.pinned.drain(..) {
                w.unpin(idx);
            }
        }
    }

    /// Give the space written so far back, linking nothing in.
    pub async fn abort(mut self) {
        self.release().await;
        if self.fs.writer.is_none() {
            // A commit since a record was written may have recorded it as
            // allocated: record it free again.
            let _tx = self.fs.begin_tx().await;
            if let Err(e) = self.fs.commit_locked().await {
                eprintln!("file writer: committing released space: {}", e);
            }
        }
    }

    async fn release(&mut self) {
        self.fs.release_ptrs(&self.leaves).await;
        self.fs.release_ptrs(&self.indirects).await;
        self.leaves.clear();
        self.indirects.clear();
        self.unpin();
    }

    /// Write what's left and link the contents in. Refused (`Conflict`) if
    /// the file changed since this writer was opened.
    pub async fn finish(mut self) -> Result<FileId, FileSystemError> {
        if !self.pending.is_empty() {
            let chunk = std::mem::take(&mut self.pending);
            if let Err(e) = self.write_record(&chunk).await {
                self.abort().await;
                return Err(e);
            }
        }
        let tree = match self.build().await {
            Ok(t) => t,
            Err(e) => {
                self.abort().await;
                return Err(e);
            }
        };
        // The records must be on disk before anything points at them.
        if let Err(e) = self.fs.dev.flush().await {
            self.abort().await;
            return Err(e.into());
        }
        let r = match self.fs.writer.clone() {
            Some(w) => self.finish_writer(&w, tree).await,
            None => self.finish_local(tree).await,
        };
        self.unpin();
        if let Some(w) = &self.fs.writer {
            let _tx = self.fs.begin_tx().await;
            self.fs.return_full(w).await;
        }
        r
    }

    /// The lowest TXG the records and indirects are stamped with.
    fn min_birth(&self) -> Option<u64> {
        self.leaves
            .iter()
            .chain(&self.indirects)
            .map(|p| p.birth_txg)
            .min()
    }

    /// Stamp the records with `txg` and rebuild the indirect blocks over
    /// them. A record's birth is in the pointer to it, not in the record:
    /// the data stays where it is.
    async fn restamp(&mut self, txg: u64) -> Result<TreeRoot, FileSystemError> {
        for p in &mut self.leaves {
            p.birth_txg = txg;
        }
        let old = std::mem::take(&mut self.indirects);
        self.fs.release_ptrs(&old).await;
        let tree = self.build().await?;
        self.fs.dev.flush().await?;
        Ok(tree)
    }

    async fn finish_writer(
        &mut self,
        w: &crate::writer::WriterState,
        tree: TreeRoot,
    ) -> Result<FileId, FileSystemError> {
        let mut tree = tree;
        let mut attempts = 0;
        loop {
            let op = FsOp::WriteFile {
                path: path_string(&self.path)?,
                size_bytes: self.size,
                blksz: self.blksz as u32,
                tree: tree.clone(),
                allocs: self
                    .leaves
                    .iter()
                    .chain(&self.indirects)
                    .map(|p| (p.lba, p.asize as u64))
                    .collect(),
                base: self.base.clone(),
                min_birth: self.min_birth(),
            };
            match self.fs.writer_commit_fs(w, op, self.lease.clone()).await {
                Ok(c) => return Ok(c.file_id),
                Err(CommitError::Refused(m)) => {
                    // Definitely not applied: the space is ours to reuse.
                    self.release().await;
                    return Err(FileSystemError::Conflict(m));
                }
                Err(CommitError::Restamp(open)) if attempts < 3 => {
                    // Stamped below the birth floor (a snapshot was taken
                    // meanwhile): restamp and send again.
                    attempts += 1;
                    self.fs.note_open_txg(open).await;
                    tree = match self.restamp(open).await {
                        Ok(t) => t,
                        Err(e) => {
                            self.release().await;
                            return Err(e);
                        }
                    };
                }
                // Unknown outcome: keep the space out of use (if it was
                // applied, it's referenced; if not, the committer still has
                // it as free).
                Err(e) => return Err(e.into()),
            }
        }
    }

    async fn finish_local(&mut self, tree: TreeRoot) -> Result<FileId, FileSystemError> {
        let fs = self.fs;
        let _tx = fs.begin_tx().await;
        // Records written before a snapshot raised the birth floor are
        // restamped (no commit can run now: the open TXG is final).
        let mut tree = tree;
        if self.min_birth().is_some_and(|b| b < fs.birth_floor()) {
            let open = fs.open_txg().await?;
            tree = match self.restamp(open).await {
                Ok(t) => t,
                Err(e) => {
                    self.release().await;
                    return Err(e);
                }
            };
        }
        let l = fs.lookup(&self.path).await?;
        let current = fs.current_file(&l).await?;
        if current.as_ref().map(|(id, d)| (*id, d.tree.clone())) != self.base {
            // Released for good: a commit since a record was written may
            // have recorded it as allocated.
            self.release().await;
            fs.commit_locked().await?;
            return Err(FileSystemError::Conflict(format!(
                "{} changed since this write began",
                self.path.display()
            )));
        }
        let id = fs
            .set_file_contents(l, current, self.blksz as u32, self.size, tree)
            .await?;
        fs.commit_locked().await?;
        Ok(id)
    }
}

impl<D: BlockDevice> Drop for FileWriter<'_, D> {
    fn drop(&mut self) {
        self.unpin();
    }
}
