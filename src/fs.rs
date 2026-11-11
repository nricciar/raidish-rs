use crate::blkptr::BlockPtr;
use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError, FileId};
use crate::inode::{FileIndex, FileInode, InodeType};
use crate::metaslab::{MetaslabLayout, MetaslabState};
use crate::raidz::RaidZError;
use crate::vdev::Topology;
use crate::volume::Volume;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};

pub const FS_ID: &str = "RAIDISHV14";
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();
pub const BLOCK_PAYLOAD_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const SUPERBLOCK_LBA: u64 = 0;
pub const UBERBLOCK_MAGIC: u64 = 0x5241494449534855; // "RAIDISHU"
/// Fewest copies kept of the superblock and of each uberblock.
pub const LABEL_COPIES: u64 = 3;
/// Uberblock ring length (the newest valid one wins at load).
pub const UBERBLOCK_SLOTS: u64 = 5;

/// Where the fixed headers live, for a pool aligned to `align` blocks:
/// the superblock region (one copy per block), then the uberblock ring
/// (`UBERBLOCK_SLOTS` slots, one copy per block), then the space maps.
/// Every region is a whole number of aligned units, so a header never
/// shares a raidz stripe with anything else. With 3 data shards that's
/// superblock 0..3, uberblocks 3..18, space maps from 18.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderLayout {
    pub align: u64,
    pub superblock_blocks: u64,
    pub uberblock_start: u64,
    pub uberblock_slot_blocks: u64,
    /// First block after the fixed headers; start of the space map slot region.
    pub spacemap_start: u64,
}

impl HeaderLayout {
    pub fn new(align: u64) -> Self {
        let unit = LABEL_COPIES.div_ceil(align) * align;
        HeaderLayout {
            align,
            superblock_blocks: unit,
            uberblock_start: unit,
            uberblock_slot_blocks: unit,
            spacemap_start: unit + UBERBLOCK_SLOTS * unit,
        }
    }

    pub fn uberblock_blocks(&self) -> u64 {
        UBERBLOCK_SLOTS * self.uberblock_slot_blocks
    }
}

#[derive(Debug)]
pub enum FileSystemError {
    SerializationError(bincode::Error),
    DiskError(DiskError),
    RaidZError(RaidZError),
    UnableToAllocateBlocks(u64),
    NotFormatted,
    FileNotFound,
    ChecksumMismatch,
    InvalidMagic,
    DirectoryNotFound,
    NotADirectory,
    NotAFile,
    AlreadyExists,
    InvalidArgument(String),
    /// The pool was formatted with a different layout than configured.
    TopologyMismatch(String),
}

impl fmt::Display for FileSystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            FileSystemError::DiskError(err) => write!(f, "Disk error: {}", err),
            FileSystemError::RaidZError(err) => write!(f, "RaidZ error: {}", err),
            FileSystemError::TopologyMismatch(why) => write!(f, "topology mismatch: {}", why),
            FileSystemError::UnableToAllocateBlocks(count) => {
                write!(f, "Unable to allocate {} blocks", count)
            }
            FileSystemError::NotFormatted => write!(f, "Not formatted"),
            FileSystemError::FileNotFound => write!(f, "File not found"),
            FileSystemError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            FileSystemError::InvalidMagic => write!(f, "Invalid magic"),
            FileSystemError::DirectoryNotFound => write!(f, "Directory not found"),
            FileSystemError::NotADirectory => write!(f, "Not a directory"),
            FileSystemError::NotAFile => write!(f, "Not a file"),
            FileSystemError::AlreadyExists => write!(f, "Already exists"),
            FileSystemError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl From<bincode::Error> for FileSystemError {
    fn from(error: bincode::Error) -> Self {
        FileSystemError::SerializationError(error)
    }
}

impl From<DiskError> for FileSystemError {
    fn from(error: DiskError) -> Self {
        FileSystemError::DiskError(error)
    }
}

impl From<RaidZError> for FileSystemError {
    fn from(error: RaidZError) -> Self {
        FileSystemError::RaidZError(error)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Superblock {
    pub magic: String,
    pub block_size: u32,
    pub total_blocks: u64,
    pub metaslab_count: u64,
    pub metaslab_size_blocks: u64,
    pub spacemap_start: u64,
    pub spacemap_slot_blocks: u64,
    pub data_start: u64,
    /// Identifies the pool; every member disk's label carries it.
    pub pool_guid: u64,
    /// Record alignment in blocks (see `vdev`).
    pub align: u64,
    /// The vdev tree the pool was formatted with (None: not a pool device).
    pub topology: Option<Topology>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Uberblock {
    pub magic: u64,
    pub version: u32,
    pub txg: u64,       // transaction group
    pub timestamp: u64, // optional but useful
    /// The root directory's serialized `FileIndex`.
    pub root: Vec<BlockPtr>,
    /// Per member disk: the earliest TXG it may have missed writes from
    /// (None = in sync). Cleared by a successful resilver.
    pub degraded: Vec<Option<u64>>,
}

/// Identity of a member disk, stored (twice) in its label area outside the
/// RAID address space.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DiskLabel {
    pub pool_guid: u64,
    pub disk_index: u8,
    pub disk_guid: u64,
}

/// How a member disk's label compares to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelState {
    Ok,
    /// Couldn't be read: the disk is offline.
    Offline,
    /// Readable, but no valid label of this pool for this slot: a new,
    /// replaced or foreign disk. Its contents can't be trusted.
    Foreign,
}

/// A random-enough 64-bit id (no RNG dependency needed).
pub(crate) fn new_guid(salt: u64) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    h.update(&std::process::id().to_le_bytes());
    h.update(&salt.to_le_bytes());
    h.update(&(&salt as *const u64 as usize).to_le_bytes());
    u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
}

/// Header of a self-describing *label* block (superblock, uberblock, space
/// map slot). Everything else carries its checksum in the parent `BlockPtr`.
#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub magic: u32,
    pub txg: u64,
    pub checksum: [u8; 32],
    pub payload_len: u32,
}

/// Validate a self-describing label block in place; returns (txg, payload length).
pub(crate) fn parse_label(buf: &[u8]) -> Result<(u64, usize), FileSystemError> {
    let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;
    if header.magic != 0x52414944 {
        return Err(FileSystemError::InvalidMagic);
    }
    let payload_len = header.payload_len as usize;
    if BLOCK_HEADER_SIZE + payload_len > buf.len() {
        return Err(FileSystemError::ChecksumMismatch);
    }
    let payload = &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload_len];
    if *blake3::hash(payload).as_bytes() != header.checksum {
        return Err(FileSystemError::ChecksumMismatch);
    }
    Ok((header.txg, payload_len))
}

pub struct FileSystem<D: BlockDevice> {
    pub dev: D,
    superblock: Arc<RwLock<Option<Superblock>>>,
    /// Last committed uberblock. The open TXG is always `uberblock.txg + 1`.
    uberblock: Arc<Mutex<Option<Uberblock>>>,
    pub(crate) layout: MetaslabLayout,
    pub(crate) metaslabs: Arc<Vec<Arc<RwLock<MetaslabState>>>>,
    pub(crate) file_index: Arc<RwLock<FileIndex>>,
    /// Root file index location in the open TXG (committed by `commit_txg`).
    pub(crate) root_ptr: Arc<Mutex<Vec<BlockPtr>>>,
    /// Open copy-on-write volumes and their dirty records.
    pub(crate) volumes: Arc<std::sync::Mutex<HashMap<String, Arc<Volume>>>>,
    commit_lock: Arc<Mutex<()>>,
    /// Commits that have begun (taken their snapshot) / finished successfully.
    /// Lets concurrent `sync` calls share one commit (group commit).
    commits_started: Arc<AtomicU64>,
    commits_done: Arc<AtomicU64>,
    /// Opened for inspection: never writes (no commits, no self-heal repairs),
    /// so it is safe to point at a pool another process is serving.
    pub(crate) read_only: bool,
    /// See `Uberblock::degraded`; persisted with every commit.
    pub(crate) degraded: Arc<std::sync::Mutex<Vec<Option<u64>>>>,
    /// Per-disk missed-write counts already folded into `degraded`.
    miss_seen: Arc<std::sync::Mutex<Vec<u64>>>,
    /// Record alignment in blocks (the pool's, or 1 for a plain device).
    pub(crate) align: u64,
    pub(crate) header: HeaderLayout,
    /// While non-zero, expired deferred frees are not made allocatable, so
    /// nothing a running scan may still rewrite can be reallocated.
    pub(crate) scan_hold: Arc<AtomicUsize>,
    /// One scrub/resilver at a time.
    pub(crate) scan_lock: Arc<Mutex<()>>,
}

impl<D: BlockDevice> FileSystem<D> {
    fn empty(dev: D) -> Self {
        let align = dev.pool().map_or(1, |p| p.align);
        let disks = dev.pool().map_or(0, |p| p.disks.len());
        FileSystem {
            dev,
            superblock: Arc::new(RwLock::new(None)),
            uberblock: Arc::new(Mutex::new(None)),
            layout: MetaslabLayout::default(),
            metaslabs: Arc::new(Vec::new()),
            file_index: Arc::new(RwLock::new(FileIndex::new())),
            root_ptr: Arc::new(Mutex::new(Vec::new())),
            volumes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            commit_lock: Arc::new(Mutex::new(())),
            commits_started: Arc::new(AtomicU64::new(0)),
            commits_done: Arc::new(AtomicU64::new(0)),
            read_only: false,
            degraded: Arc::new(std::sync::Mutex::new(vec![None; disks])),
            miss_seen: Arc::new(std::sync::Mutex::new(vec![0; disks])),
            align,
            header: HeaderLayout::new(align),
            scan_hold: Arc::new(AtomicUsize::new(0)),
            scan_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Leaf disks of the pool underneath (0 for a plain device).
    pub fn disk_count(&self) -> usize {
        self.dev.pool().map_or(0, |p| p.disks.len())
    }

    /// Every disk's missed-write count right now.
    pub(crate) fn miss_counts(&self) -> Vec<u64> {
        match self.dev.pool() {
            Some(pool) => (0..pool.disks.len())
                .map(|d| pool.disks.miss_count(d))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Largest record, in blocks.
    pub fn max_record_blocks(&self) -> u64 {
        crate::vdev::max_record_blocks(self.align)
    }

    /// Round `blocks` up to whole aligned units.
    pub(crate) fn align_blocks(&self, blocks: u64) -> u64 {
        blocks.div_ceil(self.align) * self.align
    }

    pub async fn superblock(&self) -> Option<Superblock> {
        self.superblock.read().await.clone()
    }

    pub async fn uberblock(&self) -> Option<Uberblock> {
        self.uberblock.lock().await.clone()
    }

    /// TXG that blocks written right now belong to.
    pub(crate) async fn open_txg(&self) -> Result<u64, FileSystemError> {
        self.uberblock
            .lock()
            .await
            .as_ref()
            .map(|u| u.txg + 1)
            .ok_or(FileSystemError::NotFormatted)
    }

    /// Format file system
    pub async fn format(dev: D, total_blocks: u64) -> Result<Self, FileSystemError> {
        let mut fs = Self::empty(dev);

        // In-memory TXG 0; everything written by format belongs to TXG 1.
        let zero_uber = Uberblock {
            magic: 0,
            version: 0,
            txg: 0,
            timestamp: 0,
            root: Vec::new(),
            degraded: vec![None; fs.disk_count()],
        };
        fs.uberblock = Arc::new(Mutex::new(Some(zero_uber.clone())));
        // Disks that failed during format must not count as missed writes
        // from before the pool existed; they are marked degraded below.
        *fs.miss_seen.lock().unwrap() = fs.miss_counts();
        let header = fs.header;
        for slot in 0..header.uberblock_blocks() {
            fs.write_block_checked_txg(header.uberblock_start + slot, &zero_uber)
                .await?;
        }

        let layout = MetaslabLayout::compute(total_blocks, header.spacemap_start, fs.align);
        let superblock = Superblock {
            magic: FS_ID.to_string(),
            block_size: BLOCK_SIZE as u32,
            total_blocks,
            metaslab_count: layout.metaslab_count,
            metaslab_size_blocks: layout.metaslab_size_blocks,
            spacemap_start: layout.spacemap_start,
            spacemap_slot_blocks: layout.spacemap_slot_blocks,
            data_start: layout.data_start,
            pool_guid: new_guid(total_blocks),
            align: fs.align,
            topology: fs.dev.pool().map(|p| p.topology.clone()),
        };
        for mirror_idx in 0..header.superblock_blocks {
            fs.write_block_checked_txg(SUPERBLOCK_LBA + mirror_idx, &superblock)
                .await?;
        }
        fs.superblock = Arc::new(RwLock::new(Some(superblock)));
        fs.layout = layout;
        for d in 0..fs.disk_count() {
            if fs.write_disk_label(d).await.is_err() {
                eprintln!("format: disk {} label write failed; marking it degraded", d);
                fs.degraded.lock().unwrap()[d] = Some(0);
            }
        }

        // Metaslabs start dirty, so the first commit writes slot A for each.
        let metaslabs = (0..layout.metaslab_count as usize)
            .map(|i| {
                Arc::new(RwLock::new(MetaslabState::new_empty(
                    layout.metaslab_start(i),
                    layout.metaslab_size_blocks,
                    layout.align,
                )))
            })
            .collect();
        fs.metaslabs = Arc::new(metaslabs);

        fs.persist_root_index().await?;
        fs.commit_txg().await?;

        Ok(fs)
    }

    /// Write the root file index for the open TXG and free the previous one
    /// (in the same TXG that stops referencing it, so a crash can't leak it).
    /// Does not commit; callers finish with `commit_txg`.
    pub(crate) async fn persist_root_index(&self) -> Result<(), FileSystemError> {
        let index = self.file_index.read().await.clone();
        let ptrs = self.write_value(&index).await?;
        let old = std::mem::replace(&mut *self.root_ptr.lock().await, ptrs);
        self.free_object(&old).await
    }

    /// Read file system headers and load root file index
    pub async fn load(dev: D) -> Result<Self, FileSystemError> {
        Self::load_inner(dev, false).await
    }

    /// Open without ever writing to the device (inspection tools, or reading
    /// a pool another process is serving). Damaged blocks are still healed in
    /// memory for the caller, just not repaired on disk.
    pub async fn load_readonly(dev: D) -> Result<Self, FileSystemError> {
        Self::load_inner(dev, true).await
    }

    async fn load_inner(dev: D, read_only: bool) -> Result<Self, FileSystemError> {
        let mut fs = Self::empty(dev);
        fs.read_only = read_only;

        fs.scan_uberblocks().await;

        let superblock = fs.superblock().await.ok_or(FileSystemError::NotFormatted)?;
        if superblock.magic != FS_ID {
            return Err(FileSystemError::NotFormatted);
        }
        if superblock.align != fs.align {
            return Err(FileSystemError::TopologyMismatch(format!(
                "pool is aligned to {} blocks, configured layout to {}",
                superblock.align, fs.align
            )));
        }
        if let Some(pool) = fs.dev.pool()
            && superblock.topology.as_ref() != Some(&pool.topology)
        {
            return Err(FileSystemError::TopologyMismatch(format!(
                "pool was formatted as {}, configured as {}",
                superblock
                    .topology
                    .as_ref()
                    .map_or("a plain device".to_string(), |t| t.summary()),
                pool.topology.summary()
            )));
        }
        let uber = match fs.uberblock().await {
            Some(uber) => uber,
            None => return Ok(fs),
        };

        fs.layout = MetaslabLayout::from_superblock(&superblock);
        let mut degraded = uber.degraded;
        degraded.resize(fs.disk_count(), None);
        *fs.degraded.lock().unwrap() = degraded;
        *fs.miss_seen.lock().unwrap() = fs.miss_counts();
        fs.adopt_disks().await;
        let metaslabs = fs.load_metaslabs(uber.txg).await?;
        fs.metaslabs = Arc::new(
            metaslabs
                .into_iter()
                .map(|ms| Arc::new(RwLock::new(ms)))
                .collect(),
        );

        if let Some(p) = uber.root.iter().find(|p| p.birth_txg > uber.txg) {
            eprintln!(
                "WARNING: root index record born in TXG {} > uberblock TXG {}",
                p.birth_txg, uber.txg
            );
            return Err(FileSystemError::ChecksumMismatch);
        }
        let file_index: FileIndex = fs.read_value(&uber.root).await?;
        fs.file_index = Arc::new(RwLock::new(file_index));
        fs.root_ptr = Arc::new(Mutex::new(uber.root.clone()));

        Ok(fs)
    }

    /// Read a files contents
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, FileSystemError> {
        let inode_ref = self.get_file_at_path(path).await?;
        let inode = self.read_inode(&inode_ref).await?;

        match inode.inode_type {
            InodeType::File { size_bytes, blocks } => {
                let mut out = self.read_object(&blocks).await?;
                out.truncate(size_bytes as usize);
                Ok(out)
            }
            _ => Err(FileSystemError::NotAFile),
        }
    }

    /// List directory contents
    pub async fn ls(&self, path: &Path) -> Result<FileIndex, FileSystemError> {
        let (index, _) = self.get_directory_at_path(path).await?;
        Ok(index)
    }

    pub async fn mkdir(&self, path: &Path) -> Result<FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        let (mut parent, old_inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;

        if let Some(ref inode_ref) = old_inode_ref {
            let old_inode = self.read_inode(inode_ref).await?;
            if !old_inode.inode_type.is_directory() {
                return Err(FileSystemError::NotADirectory);
            }
            return Ok(old_inode.id);
        }

        let blocks = self.write_value(&FileIndex::new()).await?;
        let file_id = parent.next_file_id;
        parent.next_file_id += 1;

        let new_inode = FileInode {
            id: file_id,
            inode_type: InodeType::Directory { blocks },
        };

        self.persist_directory_changes(path, parent, Some(new_inode))
            .await?;
        self.commit_locked().await?;

        Ok(file_id)
    }

    /// Write or overwrite a files contents
    pub async fn write_file(&self, path: &Path, data: &[u8]) -> Result<FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        let (mut parent, old_inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;

        // If exists, ensure it's a file not a directory or virtual block device
        if let Some(ref inode_ref) = old_inode_ref {
            let old_inode = self.read_inode(inode_ref).await?;
            if !old_inode.inode_type.is_file() {
                return Err(FileSystemError::NotAFile);
            }
        }

        let blocks = self.write_object(data).await?;

        let file_id = match &old_inode_ref {
            Some(r) => r.file_id,
            None => {
                parent.next_file_id += 1;
                parent.next_file_id - 1
            }
        };

        let new_inode = FileInode {
            id: file_id,
            inode_type: InodeType::File {
                size_bytes: data.len() as u64,
                blocks,
            },
        };

        self.persist_directory_changes(path, parent, Some(new_inode))
            .await?;

        // Free the old version in the same TXG that drops it.
        if let Some(old_ref) = old_inode_ref {
            self.free_inode(&old_ref).await?;
        }

        self.commit_locked().await?;

        Ok(file_id)
    }

    /// Delete a virtual block device or file
    pub async fn delete(&self, path: &Path) -> Result<FileId, FileSystemError> {
        let _tx = self.begin_tx().await;
        let (mut parent, inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;
        let inode_ref = inode_ref.ok_or(FileSystemError::FileNotFound)?;

        let inode = self.read_inode(&inode_ref).await?;
        if matches!(inode.inode_type, InodeType::Volume { .. })
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            self.close_volume(name);
        }
        self.free_inode(&inode_ref).await?;

        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(FileSystemError::FileNotFound)?;
        parent.files.remove(filename);

        self.persist_directory_changes(path, parent, None).await?;
        self.commit_locked().await?;

        Ok(inode.id)
    }

    /// Make every write completed before this call durable.
    ///
    /// Any commit that *starts* after this call covers those writes, so if
    /// one finished while we waited for the commit lock there's nothing left
    /// to do. Concurrent flushes (fsync storms, multi-queue NVMe) therefore
    /// share commits instead of each running one.
    pub async fn sync(&self) -> Result<(), FileSystemError> {
        let need = self.commits_started.load(Ordering::SeqCst) + 1;
        let _guard = self.commit_lock.lock().await;
        if self.commits_done.load(Ordering::SeqCst) >= need {
            return Ok(());
        }
        self.commit_locked().await?;
        Ok(())
    }

    /// Commit the open TXG:
    ///   1. write dirty metaslabs to their inactive space map slots
    ///   2. flush
    ///   3. write the uberblock for the TXG
    ///   4. flush
    ///
    /// Only after step 4 does in-memory state advance (active slots, deferred
    /// frees). On failure, the TXG stays open and can be retried.
    pub(crate) async fn commit_txg(&self) -> Result<u64, FileSystemError> {
        let _guard = self.commit_lock.lock().await;
        self.commit_locked().await
    }

    /// Start a metadata transaction: excludes commits (and other metadata
    /// operations) until dropped, so no commit can capture a half-done
    /// operation. Finish with `commit_locked` while holding it.
    pub(crate) async fn begin_tx(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_lock.lock().await
    }

    pub(crate) async fn commit_locked(&self) -> Result<u64, FileSystemError> {
        if self.read_only {
            return Err(FileSystemError::InvalidArgument(
                "pool opened read-only".into(),
            ));
        }
        let id = self.commits_started.fetch_add(1, Ordering::SeqCst) + 1;

        let txg = self.open_txg().await?;
        // Volumes sync all-or-nothing each, so whatever did sync is safe to
        // commit; a failure is reported after the TXG is durable.
        let volumes = self.sync_volumes().await;
        let root = self.root_ptr.lock().await.clone();

        let synced = self.sync_metaslabs(txg).await?;

        // The uberblock-carrying flush below is the last write of this TXG;
        // misses from it are caught by the next commit (as `txg`).
        let degraded = self.account_misses(txg);
        match self.write_uberblock(txg, root, degraded).await {
            Ok(uberblock) => {
                *self.uberblock.lock().await = Some(uberblock);
                self.finish_metaslab_sync(txg, synced).await;
                if volumes.is_ok() {
                    self.commits_done.fetch_max(id, Ordering::SeqCst);
                }
                volumes.map(|_| txg)
            }
            Err(e) => {
                self.abort_metaslab_sync(synced).await;
                Err(e)
            }
        }
    }

    /// Fold missed writes since the last commit into `degraded`. A miss seen
    /// now happened during TXG `txg` or late in `txg - 1` (the previous
    /// commit's uberblock write), so the disk needs everything from `txg - 1`.
    fn account_misses(&self, txg: u64) -> Vec<Option<u64>> {
        let mut degraded = self.degraded.lock().unwrap();
        if let Some(pool) = self.dev.pool() {
            let mut seen = self.miss_seen.lock().unwrap();
            for d in 0..pool.disks.len() {
                let count = pool.disks.miss_count(d);
                if count != seen[d] {
                    seen[d] = count;
                    let since = txg.saturating_sub(1);
                    if degraded[d].is_none() {
                        eprintln!("disk {} missed writes; degraded since TXG {}", d, since);
                    }
                    degraded[d] = Some(degraded[d].map_or(since, |t| t.min(since)));
                }
            }
        }
        degraded.clone()
    }

    /// Mark `disk` as needing everything from TXG `since` (persisted at the
    /// next commit).
    pub(crate) fn mark_degraded(&self, disk: usize, since: u64) {
        let mut degraded = self.degraded.lock().unwrap();
        degraded[disk] = Some(degraded[disk].map_or(since, |t| t.min(since)));
    }

    /// Per disk: have writes failed since the last commit folded them into
    /// `degraded`?
    pub(crate) fn unaccounted_misses(&self) -> Vec<bool> {
        let seen = self.miss_seen.lock().unwrap();
        self.miss_counts()
            .iter()
            .zip(seen.iter())
            .map(|(now, seen)| now != seen)
            .collect()
    }

    pub fn degraded_disks(&self) -> Vec<Option<u64>> {
        self.degraded.lock().unwrap().clone()
    }

    /// The label this pool expects on member `disk`.
    async fn expected_label(&self, disk: usize) -> Option<DiskLabel> {
        let sb = self.superblock().await?;
        Some(DiskLabel {
            pool_guid: sb.pool_guid,
            disk_index: disk as u8,
            disk_guid: 0,
        })
    }

    /// Compare member `disk`'s label with the pool.
    pub async fn check_disk_label(&self, disk: usize) -> LabelState {
        let (Some(pool), Some(want)) = (self.dev.pool(), self.expected_label(disk).await) else {
            return LabelState::Ok;
        };
        let mut readable = false;
        for copy in 0..crate::disks::DISK_LABEL_BLOCKS {
            let mut buf = vec![0u8; BLOCK_SIZE];
            if pool.disks.read_label(disk, copy, &mut buf).await.is_err() {
                continue;
            }
            readable = true;
            if let Ok((_, len)) = parse_label(&buf)
                && let Ok(label) = bincode::deserialize::<DiskLabel>(
                    &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len],
                )
                && label.pool_guid == want.pool_guid
                && label.disk_index == want.disk_index
            {
                return LabelState::Ok;
            }
        }
        if readable {
            LabelState::Foreign
        } else {
            LabelState::Offline
        }
    }

    /// Write member `disk`'s label (both copies).
    pub(crate) async fn write_disk_label(&self, disk: usize) -> Result<(), FileSystemError> {
        let pool = self.dev.pool().ok_or(FileSystemError::NotFormatted)?;
        let mut label = self
            .expected_label(disk)
            .await
            .ok_or(FileSystemError::NotFormatted)?;
        label.disk_guid = new_guid(disk as u64);
        let payload = bincode::serialize(&label)?;
        let txg = self.uberblock().await.map_or(0, |u| u.txg);
        let mut buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, &payload, &mut buf)?;
        for copy in 0..crate::disks::DISK_LABEL_BLOCKS {
            pool.disks.write_label(disk, copy, &buf).await?;
        }
        pool.disks.flush_disk(disk).await?;
        Ok(())
    }

    /// Check every member's label (at load, or when a disk reconnects): a
    /// foreign disk is fenced (no reads) and needs a full resilver.
    pub(crate) async fn adopt_disks(&self) {
        for d in 0..self.disk_count() {
            self.adopt_disk(d).await;
        }
    }

    pub(crate) async fn adopt_disk(&self, d: usize) -> LabelState {
        let state = self.check_disk_label(d).await;
        let Some(pool) = self.dev.pool() else {
            return state;
        };
        match state {
            LabelState::Ok => {}
            // Whatever it misses from now on is counted as failed writes.
            LabelState::Offline => eprintln!("disk {} is offline", d),
            LabelState::Foreign => {
                eprintln!(
                    "disk {} has no label for this pool (new or replaced?); fencing it until resilvered",
                    d
                );
                pool.disks.set_fenced(d, true);
                self.mark_degraded(d, 0);
            }
        }
        state
    }

    async fn write_uberblock(
        &self,
        txg: u64,
        root: Vec<BlockPtr>,
        degraded: Vec<Option<u64>>,
    ) -> Result<Uberblock, FileSystemError> {
        // Everything the uberblock points at must be durable first.
        self.dev.flush().await?;

        let uberblock = Uberblock {
            magic: UBERBLOCK_MAGIC,
            version: 1,
            txg,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            root,
            degraded,
        };

        // Rotate through the ring; one copy per block of the slot.
        let slot_blocks = self.header.uberblock_slot_blocks;
        let base_lba = self.header.uberblock_start + (txg % UBERBLOCK_SLOTS) * slot_blocks;

        let payload = bincode::serialize(&uberblock)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, &payload, &mut block_buf)?;

        for copy in 0..slot_blocks {
            self.dev.write_block(base_lba + copy, &block_buf).await?;
        }

        self.dev.flush().await?;

        Ok(uberblock)
    }

    /// Write a raw payload tagged with the open TXG.
    pub(crate) async fn write_raw_block_checked_txg(
        &self,
        lba: BlockId,
        data: &[u8],
    ) -> Result<(), FileSystemError> {
        let txg = self.open_txg().await?;
        self.write_raw_block_txg(lba, txg, data).await
    }

    /// Write a raw payload tagged with an explicit TXG.
    pub(crate) async fn write_raw_block_txg(
        &self,
        lba: BlockId,
        txg: u64,
        data: &[u8],
    ) -> Result<(), FileSystemError> {
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, data, &mut block_buf)?;
        self.dev.write_block(lba, &block_buf).await?;
        Ok(())
    }

    pub(crate) async fn write_block_checked_txg<T: Serialize>(
        &self,
        lba: BlockId,
        value: &T,
    ) -> Result<(), FileSystemError> {
        let payload = bincode::serialize(value)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);
        self.write_raw_block_checked_txg(lba, &payload).await
    }

    fn prepare_block_buffer(
        &self,
        txg: u64,
        data: &[u8],
        buf: &mut [u8; BLOCK_SIZE],
    ) -> Result<(), FileSystemError> {
        let checksum = *blake3::hash(data).as_bytes();
        let header = BlockHeader {
            magic: 0x52414944,
            txg,
            checksum,
            payload_len: data.len() as u32,
        };
        buf.fill(0);
        let header_bytes = bincode::serialize(&header)?;
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data.len()].copy_from_slice(data);
        Ok(())
    }

    async fn scan_uberblocks(&self) -> Option<(u64, Uberblock)> {
        // The superblock is written to several mirrors; use the first good one.
        for mirror in 0..self.header.superblock_blocks {
            match self
                .read_block_checked::<Superblock>(SUPERBLOCK_LBA + mirror)
                .await
            {
                Ok((_, superblock)) => {
                    *self.superblock.write().await = Some(superblock);
                    break;
                }
                Err(e) => eprintln!("superblock mirror {}: {:?}", mirror, e),
            }
        }

        let mut best: Option<(u64, Uberblock)> = None;

        for i in 0..self.header.uberblock_blocks() {
            let lba = self.header.uberblock_start + i;
            for (txg, ub) in self.read_label_versions::<Uberblock>(lba).await {
                if ub.magic != UBERBLOCK_MAGIC {
                    continue;
                }
                match &best {
                    None => {
                        best = Some((txg, ub));
                    }
                    Some((best_txg, _)) if txg > *best_txg => {
                        best = Some((txg, ub));
                    }
                    _ => {}
                }
            }
        }

        // Initialize TXG state from the latest committed uberblock
        if let Some((_txg, uberblock)) = best.clone() {
            *self.uberblock.lock().await = Some(uberblock);
        }

        best
    }

    /// Every valid version of label block `lba`: as normally read, and as
    /// held by the other copies / rebuilt from parity. A disk that missed
    /// writes still returns well-formed (older) labels, and on a mirror (or
    /// a single-data-shard raidz) every copy of an uberblock slot may come
    /// from that one disk, so the normal read alone could roll the pool back.
    async fn read_label_versions<T: for<'a> Deserialize<'a>>(&self, lba: BlockId) -> Vec<(u64, T)> {
        let mut blocks = Vec::new();
        let mut buf = vec![0u8; BLOCK_SIZE];
        if self.dev.read_block(lba, &mut buf).await.is_ok() {
            blocks.push(buf);
        }
        if let Ok(c) = self.dev.reconstruct_candidates(lba).await {
            for b in c.rebuilt.into_iter().flatten() {
                if !blocks.contains(&b) {
                    blocks.push(b);
                }
            }
        }
        blocks
            .iter()
            .filter_map(|buf| {
                let (txg, len) = parse_label(buf).ok()?;
                let value =
                    bincode::deserialize(&buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len]).ok()?;
                Some((txg, value))
            })
            .collect()
    }

    /// Returns deserialized type from block
    pub(crate) async fn read_block_checked<T: for<'a> Deserialize<'a>>(
        &self,
        lba: BlockId,
    ) -> Result<(u64, T), FileSystemError> {
        let mut buf = vec![0u8; BLOCK_SIZE];
        self.dev.read_block(lba, &mut buf).await?;

        // parse_label bounds-checks the header (a corrupt length used to panic).
        let (txg, len) = parse_label(&buf)?;
        let value = bincode::deserialize(&buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len])?;
        Ok((txg, value))
    }
}

#[cfg(test)]
#[path = "tests/fs_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/crash_tests.rs"]
mod crash_tests;
