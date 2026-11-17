//! Key-value datasets (`kv` format): byte-string keys to byte-string
//! values, ordered, in a B+tree (see `crate::btree`) whose nodes are the
//! records of the dataset's root object.
//!
//! A `kv` dataset is a tree dataset (see `crate::dataset`): it syncs,
//! snapshots, clones, sends and receives like a volume, and runs on one
//! writer at a time under its `ds/<name>` lease. Each operation here is
//! atomic: a commit (or an intent log flush) has all of it or none.
//! Changes are durable once `FileSystem::flush` returns: on the dataset's
//! writer, through its intent log (which carries the operations, see
//! `crate::zil`), elsewhere by a commit.

use crate::btree::{CheckReport, Txn};
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Dnode;
use crate::object::{ObjTree, Sizing};
use crate::volume::DIRTY_LIMIT_BYTES;
use std::ops::Bound;
use std::sync::Arc;

/// Target node size: rounded up to the pool's alignment.
pub const TARGET_NODE_BYTES: usize = 16 * 1024;
/// Node ids a `kv` dataset can hold. Unused ones are holes (no space).
pub const NODE_CAPACITY: u64 = 1 << 27;

impl<D: BlockDevice> FileSystem<D> {
    /// The node size of new `kv` datasets on this pool.
    pub fn default_kv_blksz(&self) -> u32 {
        self.btree_blksz(TARGET_NODE_BYTES)
    }

    /// Create an empty `kv` dataset.
    pub async fn create_kv(&self, name: &str) -> Result<FileId, FileSystemError> {
        self.create_kv_with(name, self.default_kv_blksz(), NODE_CAPACITY)
            .await
    }

    /// Create an empty `kv` dataset of `capacity` nodes of `blksz` bytes
    /// (whole stripes, as a volume's records).
    pub async fn create_kv_with(
        &self,
        name: &str,
        blksz: u32,
        capacity: u64,
    ) -> Result<FileId, FileSystemError> {
        self.create_btree(name, DatasetFormat::Kv, blksz, capacity, Default::default())
            .await
    }

    /// Create an empty dataset of B+tree format `format`, of `capacity`
    /// nodes of `blksz` bytes, with properties `props`.
    pub async fn create_btree(
        &self,
        name: &str,
        format: DatasetFormat,
        blksz: u32,
        capacity: u64,
        props: crate::inode::DatasetProps,
    ) -> Result<FileId, FileSystemError> {
        if !format.is_btree() {
            return Err(FileSystemError::InvalidArgument(format!(
                "{} datasets aren't B+trees",
                format.name()
            )));
        }
        if blksz == 0
            || !blksz.is_multiple_of(self.volblock_align())
            || blksz > self.max_volblocksize()
            || (blksz as usize) < 4096
            || capacity < 2
        {
            return Err(FileSystemError::InvalidArgument(format!(
                "B+tree nodes of {} bytes (multiples of {} up to {}), {} of them",
                blksz,
                self.volblock_align(),
                self.max_volblocksize(),
                capacity
            )));
        }
        let _tx = self.begin_tx().await;
        let mut root = Dnode::kv(blksz, capacity);
        root.props = Some(props);
        let id = self.create_dataset_locked(name, format, root).await?;
        self.commit_locked().await?;
        Ok(id)
    }

    /// The node size and capacity (in nodes) of new datasets of service
    /// format `format` (see `DatasetFormat::is_service`).
    pub fn service_shape(&self, format: DatasetFormat) -> (u32, u64) {
        match format {
            // Room for etcd's keys (escaped, with a revision) and S3's
            // (up to 1024 bytes, prefixed).
            DatasetFormat::Etcd | DatasetFormat::S3 => {
                (self.btree_blksz(32 * 1024), NODE_CAPACITY << 3)
            }
            _ => (self.default_kv_blksz(), NODE_CAPACITY),
        }
    }

    /// Create an empty dataset of service format `format` (etcd, s3,
    /// queue), with properties `props`.
    pub async fn create_service(
        &self,
        name: &str,
        format: DatasetFormat,
        props: crate::inode::DatasetProps,
    ) -> Result<FileId, FileSystemError> {
        if !format.is_service() {
            return Err(FileSystemError::InvalidArgument(format!(
                "{} isn't a service format",
                format.name()
            )));
        }
        let (blksz, capacity) = self.service_shape(format);
        self.create_btree(name, format, blksz, capacity, props)
            .await
    }

    /// Node size for new B+tree datasets of at least `bytes` (rounded up to
    /// the pool's alignment, at most its largest record).
    pub fn btree_blksz(&self, bytes: usize) -> u32 {
        let blocks = self
            .align_blocks(bytes.div_ceil(BLOCK_SIZE) as u64)
            .min(self.max_record_blocks());
        (blocks as usize * BLOCK_SIZE) as u32
    }

    /// B+tree dataset `name` of format `format`, opened.
    pub(crate) async fn open_btree(
        &self,
        name: &str,
        format: DatasetFormat,
    ) -> Result<Arc<Dataset>, FileSystemError> {
        let ds = self.open_dataset(name).await?;
        if ds.format != format {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset, not {}",
                name,
                ds.format.name(),
                format.name()
            )));
        }
        Ok(ds)
    }

    /// `kv` dataset `name`, opened.
    pub(crate) async fn open_kv(&self, name: &str) -> Result<Arc<Dataset>, FileSystemError> {
        self.open_btree(name, DatasetFormat::Kv).await
    }

    /// The value of `key` (uncommitted changes included).
    pub async fn kv_get(&self, name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, FileSystemError> {
        let ds = self.open_kv(name).await?;
        let _r = ds.ops.read().await;
        Txn::begin(self, &ds.obj).await?.get(key).await
    }

    /// Set `key` to `value`. Returns whether it replaced a value.
    pub async fn kv_put(
        &self,
        name: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, FileSystemError> {
        let ds = self.open_kv(name).await?;
        let replaced = {
            let _w = ds.ops.write().await;
            let mut t = Txn::begin(self, &ds.obj).await?;
            let r = t.put(key, value).await?;
            t.apply_logged()?;
            r
        };
        self.kv_settle().await?;
        Ok(replaced)
    }

    /// Remove `key`. Returns whether it was there.
    pub async fn kv_delete(&self, name: &str, key: &[u8]) -> Result<bool, FileSystemError> {
        let ds = self.open_kv(name).await?;
        let removed = {
            let _w = ds.ops.write().await;
            let mut t = Txn::begin(self, &ds.obj).await?;
            let r = t.delete(key).await?;
            t.apply_logged()?;
            r
        };
        self.kv_settle().await?;
        Ok(removed)
    }

    /// Up to `limit` entries with keys within `start` and `end`, in order.
    pub async fn kv_range(
        &self,
        name: &str,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FileSystemError> {
        let ds = self.open_kv(name).await?;
        let _r = ds.ops.read().await;
        Txn::begin(self, &ds.obj)
            .await?
            .range(start, end, limit)
            .await
    }

    /// Up to `limit` entries whose keys start with `prefix`, after `after`
    /// (exclusive; None: from the first).
    pub async fn kv_scan_prefix(
        &self,
        name: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FileSystemError> {
        let end = prefix_end(prefix);
        let start = match after {
            Some(a) if a >= prefix => Bound::Excluded(a),
            _ => Bound::Included(prefix),
        };
        let end = match &end {
            Some(e) => Bound::Excluded(e.as_slice()),
            None => Bound::Unbounded,
        };
        self.kv_range(name, start, end, limit).await
    }

    /// How many keys `name` holds.
    pub async fn kv_len(&self, name: &str) -> Result<u64, FileSystemError> {
        let ds = self.open_kv(name).await?;
        let _r = ds.ops.read().await;
        Ok(Txn::begin(self, &ds.obj).await?.header.entries)
    }

    /// Commit early if too much is dirty (as volume writes do).
    pub(crate) async fn kv_settle(&self) -> Result<(), FileSystemError> {
        if self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES {
            self.commit_if(|| self.datasets_dirty_bytes() > DIRTY_LIMIT_BYTES)
                .await?;
        }
        Ok(())
    }

    /// Verify the committed B+tree of object `dnode` (a `kv` dataset's
    /// root or a snapshot of one).
    pub(crate) async fn check_kv_tree(&self, dnode: &Dnode) -> CheckReport {
        let obj = ObjTree::new(
            dnode.blksz as usize,
            Sizing::Fixed,
            dnode.size,
            dnode.tree.clone(),
        );
        crate::btree::check(self, &obj).await
    }
}

/// The smallest key greater than every key starting with `prefix` (None:
/// there is none, the prefix is all 0xff or empty).
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
#[path = "tests/kv_tests.rs"]
mod tests;
