use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError, FileId};
use crate::inode::{Dnode, FileIndex, ROOT_OBJECT};
use crate::metaslab::{MetaslabLayout, MetaslabState};
use crate::object::{ObjTree, Sizing, TreeRoot};
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

pub const FS_ID: &str = "RAIDISHV18";
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
    /// Refused because the target changed meanwhile, or is open for writing
    /// elsewhere (a compare-and-swap or object lease check failed).
    Conflict(String),
    /// Can't be done right now (no committer reachable); may be retried.
    Unavailable(String),
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
            FileSystemError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            FileSystemError::Unavailable(msg) => write!(f, "Unavailable: {}", msg),
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

/// Which committed state to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Rewind {
    /// The newest valid uberblock (normal import).
    #[default]
    None,
    /// The newest uberblock whose whole tree (metadata and data) reads back
    /// with good checksums.
    Auto,
    /// The newest uberblock whose root directory reads (data may be damaged;
    /// for salvage).
    Readable,
    /// The uberblock of exactly this TXG.
    Txg(u64),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LoadOptions {
    pub read_only: bool,
    pub rewind: Rewind,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Uberblock {
    pub magic: u64,
    pub version: u32,
    pub txg: u64,       // transaction group
    pub timestamp: u64, // optional but useful
    /// The object table (see `crate::inode`).
    pub objects: TreeRoot,
    /// Per member disk: the earliest TXG it may have missed writes from
    /// (None = in sync). Cleared by a successful resilver.
    pub degraded: Vec<Option<u64>>,
    /// The next pool-wide object number (inode id). Never reused.
    pub next_object: u64,
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
        &web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
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

/// Read a pool device's superblock without loading the pool (None: not
/// formatted, or unreadable).
pub async fn read_superblock_of<D: BlockDevice>(dev: &D) -> Option<Superblock> {
    let header = HeaderLayout::new(dev.pool().map_or(1, |p| p.align));
    for mirror in 0..header.superblock_blocks {
        let mut buf = vec![0u8; BLOCK_SIZE];
        if dev
            .read_block(SUPERBLOCK_LBA + mirror, &mut buf)
            .await
            .is_err()
        {
            continue;
        }
        let Ok((_, len)) = parse_label(&buf) else {
            continue;
        };
        if let Ok(sb) =
            bincode::deserialize::<Superblock>(&buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len])
            && sb.magic == FS_ID
        {
            return Some(sb);
        }
    }
    None
}

pub struct FileSystem<D: BlockDevice> {
    pub dev: D,
    superblock: Arc<RwLock<Option<Superblock>>>,
    /// Last committed uberblock. The open TXG is always `uberblock.txg + 1`.
    pub(crate) uberblock: Arc<Mutex<Option<Uberblock>>>,
    pub(crate) layout: MetaslabLayout,
    pub(crate) metaslabs: Arc<Vec<Arc<RwLock<MetaslabState>>>>,
    /// The object table, with the open TXG's changes (see `crate::inode`).
    pub(crate) objects: Arc<ObjTree>,
    /// Open copy-on-write volumes and their dirty records.
    pub(crate) volumes: Arc<std::sync::Mutex<HashMap<String, Arc<Volume>>>>,
    /// Files open for writing at any offset (see `crate::open_file`).
    pub(crate) files: Arc<std::sync::Mutex<HashMap<FileId, Arc<crate::open_file::OpenFile>>>>,
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
    pub(crate) miss_seen: Arc<std::sync::Mutex<Vec<u64>>>,
    /// Record alignment in blocks (the pool's, or 1 for a plain device).
    pub(crate) align: u64,
    pub(crate) header: HeaderLayout,
    /// While non-zero, expired deferred frees are not made allocatable, so
    /// nothing a running scan may still rewrite can be reallocated.
    pub(crate) scan_hold: Arc<AtomicUsize>,
    /// One scrub/resilver at a time.
    pub(crate) scan_lock: Arc<Mutex<()>>,
    /// Set when opened as a volume writer (see `crate::writer`).
    pub(crate) writer: Option<Arc<crate::writer::WriterState>>,
    /// Next pool-wide object number (persisted in every uberblock).
    pub(crate) next_object: Arc<AtomicU64>,
    /// Serializes a writer's intent log appends (see `crate::zil`).
    pub(crate) log_io: Arc<Mutex<()>>,
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
            objects: Arc::new(ObjTree::new(
                BLOCK_SIZE,
                Sizing::Growable,
                0,
                TreeRoot {
                    ptr: None,
                    levels: 0,
                },
            )),
            volumes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            files: Arc::new(std::sync::Mutex::new(HashMap::new())),
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
            writer: None,
            next_object: Arc::new(AtomicU64::new(1)),
            log_io: Arc::new(Mutex::new(())),
        }
    }

    /// Tell this process's remote disk clients how the pool is laid out, so
    /// each request carries its fencing domain's epoch.
    fn publish_fence_map(&self) {
        if let Some(pool) = self.dev.pool() {
            pool.disks.fence.set_map(crate::fence::FenceMap::new(
                pool.topology.clone(),
                self.layout,
            ));
        }
    }

    /// A new pool-wide object number. Only the process that commits the
    /// pool (the committer, or a standalone pool holder) hands them out.
    pub(crate) fn alloc_object(&self) -> FileId {
        debug_assert!(self.writer.is_none(), "writers don't assign object numbers");
        self.next_object.fetch_add(1, Ordering::SeqCst)
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
            objects: TreeRoot {
                ptr: None,
                levels: 0,
            },
            degraded: vec![None; fs.disk_count()],
            next_object: 1,
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
        fs.publish_fence_map();
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

        fs.objects = Arc::new(fs.table_tree(TreeRoot {
            ptr: None,
            levels: 0,
        }));
        let root = fs.alloc_object();
        debug_assert_eq!(root, ROOT_OBJECT);
        fs.write_dnode(root, Some(Dnode::directory(fs.file_blksz())))
            .await?;
        fs.commit_txg().await?;

        Ok(fs)
    }

    /// The object table at `root`.
    pub(crate) fn table_tree(&self, root: TreeRoot) -> ObjTree {
        ObjTree::new(self.table_blksz(), Sizing::Growable, 0, root)
    }

    /// Whether the root directory of the object table at `root` reads.
    async fn root_readable(&self, root: &TreeRoot) -> bool {
        let table = self.table_tree(root.clone());
        let Ok(Some(buf)) = self
            .object_record(&table, ROOT_OBJECT / crate::inode::DNODES_PER_LEAF)
            .await
        else {
            return false;
        };
        let Ok(slots) = crate::inode::decode_leaf(&buf) else {
            return false;
        };
        let Some(Some(dir)) = slots
            .get((ROOT_OBJECT % crate::inode::DNODES_PER_LEAF) as usize)
            .cloned()
        else {
            return false;
        };
        self.read_dir(&dir).await.is_ok()
    }

    /// Read file system headers and load root file index
    pub async fn load(dev: D) -> Result<Self, FileSystemError> {
        Self::load_with(dev, LoadOptions::default()).await
    }

    /// Open without ever writing to the device (inspection tools, or reading
    /// a pool another process is serving). Damaged blocks are still healed in
    /// memory for the caller, just not repaired on disk.
    pub async fn load_readonly(dev: D) -> Result<Self, FileSystemError> {
        Self::load_with(
            dev,
            LoadOptions {
                read_only: true,
                ..Default::default()
            },
        )
        .await
    }

    /// Open the pool, possibly at an older committed state (`opts.rewind`).
    ///
    /// Rewinding to an uberblock older than the newest one, when writable,
    /// rebuilds the space maps from the chosen tree (which frees whatever the
    /// abandoned TXGs wrote) and commits right away with a TXG newer than any
    /// in the ring, so the abandoned uberblocks can never win a later import.
    pub async fn load_with(dev: D, opts: LoadOptions) -> Result<Self, FileSystemError> {
        let mut fs = Self::empty(dev);
        fs.read_only = opts.read_only;

        fs.read_superblock().await;
        let ring = fs.read_uberblock_ring().await;

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
        let Some(newest) = ring.first().cloned() else {
            return Ok(fs);
        };
        fs.layout = MetaslabLayout::from_superblock(&superblock);
        fs.publish_fence_map();

        let uber = match opts.rewind {
            Rewind::None => newest.clone(),
            Rewind::Txg(t) => ring.iter().find(|u| u.txg == t).cloned().ok_or_else(|| {
                FileSystemError::InvalidArgument(format!(
                    "no valid uberblock for TXG {} (have: {:?})",
                    t,
                    ring.iter().map(|u| u.txg).collect::<Vec<_>>()
                ))
            })?,
            Rewind::Auto => fs.first_intact(&ring).await?,
            Rewind::Readable => {
                let mut pick = None;
                for u in &ring {
                    if fs.root_readable(&u.objects).await {
                        pick = Some(u.clone());
                        break;
                    }
                    eprintln!("rewind: TXG {}: root directory unreadable", u.txg);
                }
                pick.ok_or_else(|| {
                    FileSystemError::InvalidArgument("no uberblock with a readable root".into())
                })?
            }
        };
        let rewound = uber.txg < newest.txg;
        if rewound {
            eprintln!(
                "rewind: opening TXG {} (newest is TXG {}; {} newer TXG(s) discarded)",
                uber.txg,
                newest.txg,
                ring.iter().filter(|u| u.txg > uber.txg).count()
            );
        }
        // A rewound pool's next TXG must be newer than everything in the
        // ring; it keeps the chosen tree.
        *fs.uberblock.lock().await = Some(Uberblock {
            txg: newest.txg,
            ..uber.clone()
        });
        // Never hand out a number any TXG in the ring may have used, even
        // one a rewind discards.
        fs.next_object.store(
            ring.iter().map(|u| u.next_object).max().unwrap_or(1).max(1),
            Ordering::SeqCst,
        );

        // Conservative: a disk degraded in any newer TXG stays degraded.
        let mut degraded = uber.degraded.clone();
        for u in ring.iter().filter(|u| u.txg > uber.txg) {
            for (d, since) in u.degraded.iter().enumerate() {
                if d < degraded.len()
                    && let Some(s) = since
                {
                    degraded[d] = Some(degraded[d].map_or(*s, |t| t.min(*s)));
                }
            }
        }
        degraded.resize(fs.disk_count(), None);
        *fs.degraded.lock().unwrap() = degraded;
        *fs.miss_seen.lock().unwrap() = fs.miss_counts();
        fs.adopt_disks().await;
        let (metaslabs, missing) = fs.load_metaslabs(uber.txg).await?;
        if !missing.is_empty() && !(rewound && !fs.read_only) {
            eprintln!(
                "WARNING: {} metaslab(s) without a valid space map are treated as full; \
                 run `fsck --rebuild-spacemaps` to recover the space",
                missing.len()
            );
        }
        fs.metaslabs = Arc::new(
            metaslabs
                .into_iter()
                .map(|ms| Arc::new(RwLock::new(ms)))
                .collect(),
        );

        if let Some(p) = uber.objects.ptr.as_ref().filter(|p| p.birth_txg > uber.txg) {
            eprintln!(
                "WARNING: object table root born in TXG {} > uberblock TXG {}",
                p.birth_txg, uber.txg
            );
            return Err(FileSystemError::ChecksumMismatch);
        }
        fs.objects = Arc::new(fs.table_tree(uber.objects.clone()));
        // The root directory must read.
        let root = fs.read_dnode(ROOT_OBJECT).await?;
        fs.read_dir(&root).await?;

        if rewound && !fs.read_only {
            let older: Vec<TreeRoot> = ring
                .iter()
                .filter(|u| u.txg < uber.txg)
                .map(|u| u.objects.clone())
                .collect();
            let _tx = fs.begin_tx().await;
            // Never forced: freeing what an unreadable part of the chosen
            // tree references would destroy data salvage could still get.
            let report = fs
                .rebuild_spacemaps_locked(&older, false)
                .await
                .map_err(|e| {
                    FileSystemError::InvalidArgument(format!(
                        "rewind to TXG {} refused: {} (open it read-only to salvage)",
                        uber.txg, e
                    ))
                })?;
            eprintln!("rewind: {}", report);
            let txg = fs.commit_locked().await?;
            eprintln!("rewind: committed as TXG {}", txg);
            // The discarded TXGs must never be imported again: their space
            // is free now and about to be reused.
            for u in ring
                .iter()
                .filter(|u| u.txg > uber.txg && u.txg % UBERBLOCK_SLOTS != txg % UBERBLOCK_SLOTS)
            {
                if let Err(e) = fs.invalidate_uberblock_slot(u.txg).await {
                    eprintln!(
                        "WARNING: rewind: could not erase discarded TXG {}: {}",
                        u.txg, e
                    );
                }
            }
            fs.dev.flush().await?;
        }

        Ok(fs)
    }

    /// The newest uberblock of `ring` whose whole tree verifies.
    async fn first_intact(&self, ring: &[Uberblock]) -> Result<Uberblock, FileSystemError> {
        for u in ring {
            if u.objects.ptr.as_ref().is_some_and(|p| p.birth_txg > u.txg) {
                eprintln!("rewind: TXG {}: root newer than its uberblock", u.txg);
                continue;
            }
            let walk = self.verify_tree(&u.objects).await;
            if walk.errors.is_empty() {
                return Ok(u.clone());
            }
            eprintln!(
                "rewind: TXG {} has {} damaged record(s), e.g. {}",
                u.txg,
                walk.errors.len(),
                walk.errors[0]
            );
        }
        Err(FileSystemError::InvalidArgument(
            "rewind: no uberblock in the ring has an intact tree".into(),
        ))
    }

    /// Every valid uberblock in the ring, newest first (one per TXG).
    pub async fn uberblock_ring(&self) -> Vec<Uberblock> {
        self.read_uberblock_ring().await
    }

    /// Read a file's contents.
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, FileSystemError> {
        let (_, dnode) = self.stat_file(path).await?;
        self.read_tree(&dnode.tree, dnode.blksz as usize, dnode.size)
            .await
    }

    /// List directory contents
    pub async fn ls(&self, path: &Path) -> Result<FileIndex, FileSystemError> {
        let (_, _, index) = self
            .resolve_dir(&crate::inode::path_components(path))
            .await?;
        Ok(index)
    }

    pub async fn mkdir(&self, path: &Path) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_mkdir(&w, path).await;
        }
        let _tx = self.begin_tx().await;
        let id = self.mkdir_locked(path).await?;
        self.commit_locked().await?;
        Ok(id)
    }

    /// `mkdir` under `begin_tx`, without committing (the committer applies
    /// writers' mkdirs this way). An existing directory is fine.
    pub(crate) async fn mkdir_locked(&self, path: &Path) -> Result<FileId, FileSystemError> {
        let l = self.lookup(path).await?;
        if let Some(e) = l.entry {
            if e.kind != crate::inode::ObjType::Directory {
                return Err(FileSystemError::NotADirectory);
            }
            return Ok(e.id);
        }
        self.link_new(l, Dnode::directory(self.file_blksz())).await
    }

    /// Create or replace a file with `data`. On a pool this process
    /// commits, it is one transaction: no commit sees it half done.
    pub async fn write_file(&self, path: &Path, data: &[u8]) -> Result<FileId, FileSystemError> {
        if self.writer.is_some() {
            let mut f = self.file_writer(path, None).await?;
            f.write(data).await?;
            return f.finish().await;
        }
        let _tx = self.begin_tx().await;
        let l = self.lookup(path).await?;
        let current = self.current_file(&l).await?;
        let blksz = self.file_blksz();
        let mut allocated = Vec::new();
        let written = match self.write_tree(blksz as usize, data, &mut allocated).await {
            Ok(tree) => {
                self.set_file_contents(l, current, blksz, data.len() as u64, tree)
                    .await
            }
            Err(e) => Err(e),
        };
        let id = match written {
            Ok(id) => id,
            Err(e) => {
                self.release_ptrs(&allocated).await;
                return Err(e);
            }
        };
        self.commit_locked().await?;
        Ok(id)
    }

    /// Delete a file, an empty directory or a volume.
    pub async fn delete(&self, path: &Path) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_delete(&w, path, None).await;
        }
        let _tx = self.begin_tx().await;
        let id = self.delete_locked(path).await?;
        self.commit_locked().await?;
        Ok(id)
    }

    /// Move `from` to `to` (see `rename_locked`). Returns the object's number.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_rename(&w, from, to).await;
        }
        let _tx = self.begin_tx().await;
        let (id, _) = self.rename_locked(from, to).await?;
        self.commit_locked().await?;
        Ok(id)
    }

    /// Change `path`'s mode, owner or modification time.
    pub async fn set_attrs(
        &self,
        path: &Path,
        set: &crate::inode::SetAttrs,
    ) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_set_attrs(&w, path, set).await;
        }
        let _tx = self.begin_tx().await;
        let id = self.set_attrs_locked(path, set).await?;
        self.commit_locked().await?;
        Ok(id)
    }

    /// `delete` under `begin_tx`, without committing.
    pub(crate) async fn delete_locked(&self, path: &Path) -> Result<FileId, FileSystemError> {
        let l = self.lookup(path).await?;
        let entry = l.entry.ok_or(FileSystemError::FileNotFound)?;
        if entry.kind == crate::inode::ObjType::Volume {
            self.close_volume(&l.name);
        }
        self.discard_file(entry.id);
        self.unlink(l).await
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
        if let Some(w) = self.writer.clone() {
            // A volume writer commits through the committer.
            let id = self.commits_started.fetch_add(1, Ordering::SeqCst) + 1;
            let r = self.writer_commit(&w).await;
            if r.is_ok() {
                self.commits_done.fetch_max(id, Ordering::SeqCst);
            }
            return r;
        }
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
        let files = self.sync_files().await;
        let volumes = volumes.and(files);
        // Then the object table, which now points at every new tree.
        self.sync_table().await?;
        let root = self.objects.root();

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

    /// Write the object table's changed records into the open TXG. On
    /// failure they stay dirty and nothing visible changes.
    async fn sync_table(&self) -> Result<(), FileSystemError> {
        let Some(taken) = self.objects.begin_sync() else {
            return Ok(());
        };
        let (mut allocated, mut replaced) = (Vec::new(), Vec::new());
        match self
            .build_tree(&self.objects, &taken, &mut allocated, &mut replaced)
            .await
        {
            Ok(root) => {
                self.objects.set_root(root);
                self.objects.end_sync(&taken, Some(&taken));
                for p in &replaced {
                    self.free(p.lba, p.asize as u64).await?;
                }
                Ok(())
            }
            Err(e) => {
                self.release_ptrs(&allocated).await;
                self.objects.end_sync(&taken, None);
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
        objects: TreeRoot,
        degraded: Vec<Option<u64>>,
    ) -> Result<Uberblock, FileSystemError> {
        // Everything the uberblock points at must be durable first.
        self.dev.flush().await?;

        let uberblock = Uberblock {
            magic: UBERBLOCK_MAGIC,
            version: 1,
            txg,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            objects,
            degraded,
            next_object: self.next_object.load(Ordering::SeqCst),
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

    /// Overwrite the ring slot holding TXG `txg` with an invalid (zero-magic)
    /// uberblock, so it can't be imported any more.
    async fn invalidate_uberblock_slot(&self, txg: u64) -> Result<(), FileSystemError> {
        let slot_blocks = self.header.uberblock_slot_blocks;
        let base_lba = self.header.uberblock_start + (txg % UBERBLOCK_SLOTS) * slot_blocks;
        let blank = Uberblock {
            magic: 0,
            version: 0,
            txg: 0,
            timestamp: 0,
            objects: TreeRoot {
                ptr: None,
                levels: 0,
            },
            degraded: Vec::new(),
            next_object: 0,
        };
        let mut buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(0, &bincode::serialize(&blank)?, &mut buf)?;
        for copy in 0..slot_blocks {
            self.dev.write_block(base_lba + copy, &buf).await?;
        }
        Ok(())
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

    async fn read_superblock(&self) {
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
    }

    /// Every valid uberblock version found in the ring, newest first.
    pub(crate) async fn read_uberblock_ring(&self) -> Vec<Uberblock> {
        let mut found: Vec<Uberblock> = Vec::new();
        for i in 0..self.header.uberblock_blocks() {
            let lba = self.header.uberblock_start + i;
            for (txg, ub) in self.read_label_versions::<Uberblock>(lba).await {
                if ub.magic != UBERBLOCK_MAGIC || ub.txg != txg {
                    continue;
                }
                if !found.iter().any(|u| u.txg == txg) {
                    found.push(ub);
                }
            }
        }
        found.sort_by_key(|u| std::cmp::Reverse(u.txg));
        found
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
