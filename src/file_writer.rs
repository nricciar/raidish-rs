//! Streaming file writes: a file of any size is written record by record as
//! the data arrives, without holding it all in memory or holding any lock
//! while it uploads.
//!
//! A [`FileWriter`] creates or replaces one file. Each full record goes to
//! disk as soon as it fills (on a writer, into its lent metaslabs, which stay
//! pinned until the file is committed). `finish` links the new contents in
//! with a compare-and-swap on the entry it saw when it was opened: if
//! someone else changed the file meanwhile, the write is refused and its
//! space released.
//!
//! On a cluster, a long write can hold the file's `obj/<id>` lease (see
//! `crate::committer::object_resource`), so others are refused up front
//! instead of at the end; `finish` is refused if that lease was lost.

use crate::blkptr::BlockPtr;
use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{FileInode, InodeRef, InodeType};
use crate::writer::{CommitError, FsOp, ObjLease, path_string};
use std::path::{Path, PathBuf};

/// A file being written. Call `finish` to commit it or `abort` to give its
/// space back; dropped without either, its records are left unreferenced
/// until the pool is reopened (a writer's never reach the committer).
pub struct FileWriter<'a, D: BlockDevice> {
    fs: &'a FileSystem<D>,
    path: PathBuf,
    /// The entry when opened (None: the file didn't exist).
    base: Option<InodeRef>,
    lease: Option<ObjLease>,
    blocks: Vec<BlockPtr>,
    pending: Vec<u8>,
    size: u64,
    /// Lent metaslabs pinned by `blocks` (a writer only), one per record.
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
        let (_, base) = self.get_maybe_inode_ref_at_path(path).await?;
        if let Some(r) = &base
            && !self.read_inode(r).await?.inode_type.is_file()
        {
            return Err(FileSystemError::NotAFile);
        }
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
            blocks: Vec::new(),
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
            if let Some(r) = &w.base {
                return Ok(r.file_id);
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
        let (_, entry) = self.get_maybe_inode_ref_at_path(path).await?;
        Ok(entry.ok_or(FileSystemError::FileNotFound)?.file_id)
    }
}

impl<'a, D: BlockDevice> FileWriter<'a, D> {
    fn record_bytes(&self) -> usize {
        self.fs.max_record_blocks() as usize * BLOCK_SIZE
    }

    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.size + self.pending.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append `data`. Full records are written right away.
    pub async fn write(&mut self, mut data: &[u8]) -> Result<(), FileSystemError> {
        let rec = self.record_bytes();
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

    async fn write_record(&mut self, chunk: &[u8]) -> Result<(), FileSystemError> {
        let fs = self.fs;
        let ptrs = match &fs.writer {
            None => fs.write_object(chunk).await?,
            Some(w) => {
                let blocks = (chunk.len().div_ceil(BLOCK_SIZE) as u64).div_ceil(fs.layout.align)
                    * fs.layout.align;
                let mut attempt = 0;
                loop {
                    fs.lease_space(w, blocks + fs.max_record_blocks()).await?;
                    match fs.write_object(chunk).await {
                        Err(FileSystemError::UnableToAllocateBlocks(_))
                            if attempt + 1 < ALLOC_ATTEMPTS =>
                        {
                            attempt += 1;
                        }
                        r => break r?,
                    }
                }
            }
        };
        if let Some(w) = &fs.writer {
            for p in &ptrs {
                if let Some(idx) = fs.layout.metaslab_for_block(p.lba) {
                    w.pin(idx as u32);
                    self.pinned.push(idx as u32);
                }
            }
        }
        self.size += chunk.len() as u64;
        self.blocks.extend(ptrs);
        Ok(())
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
        self.fs.release_ptrs(&self.blocks).await;
        self.blocks.clear();
        self.unpin();
    }

    /// Write what's left and link the file in. Refused (`Conflict`) if the
    /// file changed since this writer was opened.
    pub async fn finish(mut self) -> Result<FileId, FileSystemError> {
        if !self.pending.is_empty() {
            let chunk = std::mem::take(&mut self.pending);
            if let Err(e) = self.write_record(&chunk).await {
                self.abort().await;
                return Err(e);
            }
        }
        // The records must be on disk before anything points at them.
        if let Err(e) = self.fs.dev.flush().await {
            self.abort().await;
            return Err(e.into());
        }
        let r = match self.fs.writer.clone() {
            Some(w) => self.finish_writer(&w).await,
            None => self.finish_local().await,
        };
        self.unpin();
        if let Some(w) = &self.fs.writer {
            let _tx = self.fs.begin_tx().await;
            self.fs.return_full(w).await;
        }
        r
    }

    async fn finish_writer(
        &mut self,
        w: &crate::writer::WriterState,
    ) -> Result<FileId, FileSystemError> {
        let op = FsOp::WriteFile {
            path: path_string(&self.path)?,
            size_bytes: self.size,
            blocks: self.blocks.clone(),
            base: self.base.as_ref().map(|r| r.inode.clone()),
        };
        match self.fs.writer_commit_fs(w, op, self.lease.clone()).await {
            Ok(c) => Ok(c.file_id),
            Err(CommitError::Refused(m)) => {
                // Definitely not applied: the space is ours to reuse.
                self.release().await;
                Err(FileSystemError::Conflict(m))
            }
            // Unknown outcome: keep the space out of use (if it was applied,
            // it's referenced; if not, the committer still has it as free).
            Err(e) => Err(e.into()),
        }
    }

    async fn finish_local(&mut self) -> Result<FileId, FileSystemError> {
        let fs = self.fs;
        let _tx = fs.begin_tx().await;
        let (parent, current) = fs.get_maybe_inode_ref_at_path(&self.path).await?;
        if current.as_ref().map(|r| &r.inode) != self.base.as_ref().map(|r| &r.inode) {
            // Released for good: a commit since a record was written may
            // have recorded it as allocated.
            fs.release_ptrs(&self.blocks).await;
            self.blocks.clear();
            fs.commit_locked().await?;
            return Err(FileSystemError::Conflict(format!(
                "{} changed since this write began",
                self.path.display()
            )));
        }
        let file_id = match &current {
            Some(r) => r.file_id,
            None => fs.alloc_object(),
        };
        let inode = FileInode {
            id: file_id,
            inode_type: InodeType::File {
                size_bytes: self.size,
                blocks: self.blocks.clone(),
            },
        };
        fs.persist_directory_changes(&self.path, parent, Some(inode))
            .await?;
        if let Some(old) = current {
            fs.free_inode(&old).await?;
        }
        fs.commit_locked().await?;
        Ok(file_id)
    }
}

impl<D: BlockDevice> Drop for FileWriter<'_, D> {
    fn drop(&mut self) {
        self.unpin();
    }
}
