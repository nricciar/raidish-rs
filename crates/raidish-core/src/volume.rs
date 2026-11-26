//! Copy-on-write volumes (virtual block devices): `block` datasets.
//!
//! A volume is a dataset (see `crate::dataset`) whose object is fixed-size:
//! a sparse array of `volblocksize` records, read and written at byte
//! offsets (NBD, NVMe). Running, syncing, the intent log, snapshots and
//! send / receive are the generic dataset machinery; this is what's
//! particular to block devices.

use crate::blkptr::BlockPtr;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::{BLOCK_SIZE, FileId, PoolDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Dnode;
use crate::object::{Dirty, levels_for};
use crate::types::DatasetName;
use std::sync::Arc;

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

pub use crate::types::VolumeInfo;

fn frag_of(leaves: &[(u64, BlockPtr)]) -> FragReport {
    FragReport {
        records: leaves.len() as u64,
        breaks: leaves
            .windows(2)
            .filter(|w| w[1].1.lba != w[0].1.lba + w[0].1.asize as u64)
            .count() as u64,
    }
}

impl<D: PoolDevice> FileSystem<D> {
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
        name: &DatasetName,
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
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let levels = levels_for(size_bytes.div_ceil(volblocksize as u64));
        let file_id = self
            .create_dataset_locked(
                tx,
                name,
                DatasetFormat::Block,
                Dnode::volume(volblocksize, size_bytes, levels),
            )
            .await?;
        self.commit_locked(tx).await?;
        Ok(file_id)
    }

    /// Volume `name`, opened (refused if it's another kind of dataset).
    pub(crate) async fn open_block(
        &self,
        name: &DatasetName,
    ) -> Result<Arc<Dataset>, FileSystemError> {
        let ds = self.open_dataset(name).await?;
        if ds.format != DatasetFormat::Block {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset, not a volume",
                name,
                ds.format.name()
            )));
        }
        Ok(ds)
    }

    /// Delete volume `name` (it must have no snapshots).
    pub async fn delete_volume(&self, name: &DatasetName) -> Result<(), FileSystemError> {
        if self.dataset_entry(name).await?.format != DatasetFormat::Block {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' isn't a volume",
                name
            )));
        }
        self.delete_dataset(name).await
    }

    /// Every volume with its size and placement.
    pub async fn list_volumes(&self) -> Result<Vec<VolumeInfo>, FileSystemError> {
        let mut out = Vec::new();
        for (name, e) in self.datasets().await?.datasets {
            if e.format != DatasetFormat::Block {
                continue;
            }
            let d = self.read_dnode(e.root).await?;
            out.push(VolumeInfo {
                name: DatasetName::try_from(name)?,
                size_bytes: d.size,
                volblocksize: d.blksz,
                props: d.props().cloned().unwrap_or_default(),
            });
        }
        Ok(out)
    }

    /// Read data from a virtual block device at a specific byte offset
    pub async fn block_read(
        &self,
        name: &DatasetName,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_block(name).await?;
        self.object_read(&vol.obj, offset, buf).await
    }

    /// Write data to a virtual block device at a specific byte offset
    pub async fn block_write(
        &self,
        name: &DatasetName,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_block(name).await?;
        let done = self
            .object_modify(&vol.obj, offset, data.len(), Some(data))
            .await?;
        if self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES {
            // Writers that piled up behind a commit (one per request in
            // flight) don't each run another one once it has drained.
            self.commit_if(|| self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES)
                .await?;
        }
        Ok(done)
    }

    /// Zero a range (NBD TRIM / WRITE_ZEROES). Whole records become holes and
    /// release their space on the next commit.
    pub async fn block_zero(
        &self,
        name: &DatasetName,
        offset: u64,
        len: usize,
    ) -> Result<usize, FileSystemError> {
        let vol = self.open_block(name).await?;
        self.object_modify(&vol.obj, offset, len, None).await
    }
    /// How scattered volume `name`'s committed records are.
    pub async fn fragmentation(&self, name: &DatasetName) -> Result<FragReport, FileSystemError> {
        let vol = self.open_block(name).await?;
        let leaves = self.tree_leaves(&vol.obj.root()).await?;
        Ok(frag_of(&leaves))
    }

    /// Rewrite volume `name` so its records are laid out in index order,
    /// contiguously, in batches of `batch_records` (one commit each). Batches
    /// that are already contiguous are skipped; records written concurrently
    /// are left to the writer. Returns the fragmentation before and after.
    pub async fn defrag(
        &self,
        name: &DatasetName,
        batch_records: usize,
    ) -> Result<(FragReport, FragReport), FileSystemError> {
        let vol = self.open_block(name).await?;
        // Start from a committed tree.
        self.sync().await?;
        let leaves = self.tree_leaves(&vol.obj.root()).await?;
        let before = frag_of(&leaves);
        let record_blocks = self.align_blocks(vol.blksz.div_ceil(BLOCK_SIZE) as u64);
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
            let mut tx = self.begin_tx().await;
            let tx = &mut tx;
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
                        data.resize(vol.blksz, 0);
                        d.records.insert(*idx, Dirty::Data(data));
                    }
                }
            }
            self.commit_locked(tx).await?;
        }
        let after = frag_of(&self.tree_leaves(&vol.obj.root()).await?);
        Ok((before, after))
    }
}

#[cfg(test)]
#[path = "tests/volume_tests.rs"]
mod tests;
