use crate::dataset::Dataset;
use crate::dataset::{DEFAULT_FS, DatasetEntry, DatasetFormat, DatasetIndex};
use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError, FileId};
use crate::inode::{Dnode, FileIndex};
use crate::metaslab::{MetaslabLayout, MetaslabState};
use crate::object::{ObjTree, Sizing, TreeRoot};
use crate::raidz::RaidZError;
use crate::types::DatasetName;
use crate::types::ErrorKind;
use crate::types::Txg;
use crate::vdev::Topology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};

/// The on-disk format: a superblock's magic. Bumped by every format change.
pub const FS_ID: u64 = u64::from_be_bytes(*b"RAIDIS24");
/// A self-describing label block's magic ("RAID").
pub const LABEL_MAGIC: u32 = 0x52414944;
/// Version of the uberblock layout written.
pub const UBERBLOCK_VERSION: u32 = 2;
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();
pub const BLOCK_PAYLOAD_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const SUPERBLOCK_LBA: u64 = 0;

/// Holds back the release of deferred frees (see `scan_hold`) while it
/// lives. Frees deferred meanwhile go at the first commit after it drops.
pub struct FreeHold(Arc<AtomicUsize>);

impl FreeHold {
    pub(crate) fn new(hold: &Arc<AtomicUsize>) -> Self {
        hold.fetch_add(1, Ordering::SeqCst);
        FreeHold(hold.clone())
    }
}

impl Drop for FreeHold {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Keeps the space under a repair from being freed and reused while the
/// repair is in flight (audit D5a): on a committer, its deferred frees
/// (`FreeHold`); on a writer, a pin on the lent metaslabs, so they aren't
/// given back meanwhile (only the committer frees and reuses space, and it
/// can't touch lent space; if it reclaims it anyway, the writer lost its
/// lease, and the disk servers fence its writes out).
pub enum RepairHold {
    /// Held for its drop.
    Frees(#[allow(dead_code)] FreeHold),
    Pinned(
        Arc<crate::writer::WriterState>,
        Vec<crate::types::MetaslabIdx>,
    ),
}

impl Drop for RepairHold {
    fn drop(&mut self) {
        if let RepairHold::Pinned(w, pins) = self {
            for &idx in pins.iter() {
                w.unpin(idx);
            }
        }
    }
}

impl RepairHold {
    /// The metaslabs pinned (none for a committer's hold).
    pub fn pinned(&self) -> &[crate::types::MetaslabIdx] {
        match self {
            RepairHold::Frees(_) => &[],
            RepairHold::Pinned(_, pins) => pins,
        }
    }

    /// Pin those of `want` still lent to writer `w`; with `all`, None
    /// unless every one of them is.
    pub(crate) fn pin_lent(
        w: &Arc<crate::writer::WriterState>,
        want: impl IntoIterator<Item = crate::types::MetaslabIdx>,
        all: bool,
    ) -> Option<RepairHold> {
        // Under the lent list's lock: giving a metaslab back checks the
        // pins and takes it off the list under that lock too.
        let lent = w.lent.lock().unwrap();
        let want: Vec<_> = want.into_iter().collect();
        let pins: Vec<_> = want.iter().copied().filter(|i| lent.contains(i)).collect();
        if all && pins.len() != want.len() {
            return None;
        }
        for &idx in &pins {
            w.pin(idx);
        }
        Some(RepairHold::Pinned(w.clone(), pins))
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// A hold for repairing pool blocks `lba..lba + n`, or None if this
    /// process mustn't repair them: a writer repairs only in space lent to
    /// it. The committer can't see a writer's hold, and the writer's writes
    /// anywhere else are fenced out.
    pub fn repair_hold(&self, lba: u64, n: u64) -> Option<RepairHold> {
        let Some(w) = &self.writer else {
            return Some(RepairHold::Frees(FreeHold::new(&self.scan_hold)));
        };
        let first = self.layout.metaslab_for_block(lba)?;
        let last = self
            .layout
            .metaslab_for_block(lba.checked_add(n.max(1) - 1)?)?;
        let span =
            (first.index()..=last.index()).map(|i| crate::types::MetaslabIdx::from_raw(i as u32));
        RepairHold::pin_lent(w, span, true)
    }
}
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
    /// Not enough free space (for what's said).
    NoSpace(String),
    NotFormatted,
    FileNotFound,
    ChecksumMismatch,
    InvalidMagic,
    DirectoryNotFound,
    NotADirectory,
    NotAFile,
    AlreadyExists,
    /// A directory (or the like) that still has entries.
    NotEmpty(String),
    InvalidArgument(String),
    /// Failed somewhere else (a remote committer) for an internal reason.
    Internal(String),
    /// The pool was formatted with a different layout than configured.
    TopologyMismatch(String),
    /// Refused because the target changed meanwhile, or is open for writing
    /// elsewhere (a compare-and-swap or object lease check failed).
    Conflict(String),
    /// Can't be done right now (no committer reachable); may be retried.
    Unavailable(String),
    /// A writer's blocks were stamped below the birth floor: rebuild them
    /// stamped with this TXG.
    Restamp(Txg),
}

impl fmt::Display for FileSystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            FileSystemError::DiskError(err) => write!(f, "Disk error: {}", err),
            FileSystemError::RaidZError(err) => write!(f, "RaidZ error: {}", err),
            FileSystemError::TopologyMismatch(why) => write!(f, "topology mismatch: {}", why),
            FileSystemError::NoSpace(what) => write!(f, "No space left for {}", what),
            FileSystemError::NotEmpty(what) => write!(f, "{} isn't empty", what),
            FileSystemError::Internal(why) => write!(f, "Internal error: {}", why),
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
            FileSystemError::Restamp(t) => {
                write!(f, "Stamped below the birth floor (now TXG {})", t)
            }
        }
    }
}

impl FileSystemError {
    /// What sort of failure it is (see [`ErrorKind`]).
    pub fn kind(&self) -> ErrorKind {
        use FileSystemError as E;
        match self {
            E::FileNotFound | E::DirectoryNotFound => ErrorKind::NotFound,
            E::AlreadyExists => ErrorKind::Exists,
            E::NotEmpty(_) => ErrorKind::NotEmpty,
            E::NotADirectory => ErrorKind::NotDir,
            E::NotAFile => ErrorKind::NotFile,
            E::InvalidArgument(_) => ErrorKind::Invalid,
            E::Conflict(_) => ErrorKind::Conflict,
            E::Unavailable(_) | E::NotFormatted | E::Restamp(_) => ErrorKind::Unavailable,
            E::NoSpace(_) => ErrorKind::NoSpace,
            // Disks out of reach (or our epoch behind): try again later.
            E::DiskError(d) | E::RaidZError(crate::raidz::RaidZError::DiskError(d))
                if d.is_transient() =>
            {
                ErrorKind::Unavailable
            }
            E::RaidZError(crate::raidz::RaidZError::IncompleteStripe(_)) => ErrorKind::Unavailable,
            E::SerializationError(_)
            | E::DiskError(_)
            | E::RaidZError(_)
            | E::ChecksumMismatch
            | E::InvalidMagic
            | E::TopologyMismatch(_)
            | E::Internal(_) => ErrorKind::Internal,
        }
    }

    /// What it says, without its kind's prefix: what [`Self::from_kind`]
    /// takes back.
    pub fn message(&self) -> String {
        use FileSystemError as E;
        match self {
            E::NotEmpty(m)
            | E::InvalidArgument(m)
            | E::Conflict(m)
            | E::Unavailable(m)
            | E::NoSpace(m)
            | E::Internal(m)
            | E::TopologyMismatch(m) => m.clone(),
            e => e.to_string(),
        }
    }

    /// An error of `kind` saying `msg`, as reported from elsewhere: the
    /// variant `kind` names, so it matches as if it were local.
    pub fn from_kind(kind: ErrorKind, msg: String) -> Self {
        use FileSystemError as E;
        match kind {
            ErrorKind::NotFound => E::FileNotFound,
            ErrorKind::Exists => E::AlreadyExists,
            ErrorKind::NotEmpty => E::NotEmpty(msg),
            ErrorKind::NotDir => E::NotADirectory,
            ErrorKind::NotFile => E::NotAFile,
            ErrorKind::Invalid | ErrorKind::Forbidden => E::InvalidArgument(msg),
            ErrorKind::Conflict => E::Conflict(msg),
            ErrorKind::Unavailable => E::Unavailable(msg),
            ErrorKind::NoSpace => E::NoSpace(msg),
            ErrorKind::Internal => E::Internal(msg),
        }
    }
}

impl From<crate::types::NameError> for FileSystemError {
    fn from(e: crate::types::NameError) -> Self {
        FileSystemError::InvalidArgument(e.to_string())
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
    /// [`FS_ID`].
    pub magic: u64,
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
    Txg(Txg),
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
    pub txg: Txg,       // transaction group
    pub timestamp: u64, // optional but useful
    /// The object table (see `crate::inode`).
    pub objects: TreeRoot,
    /// Per member disk: the earliest TXG it may have missed writes from
    /// (None = in sync). Cleared by a successful resilver.
    pub degraded: Vec<Option<Txg>>,
    /// The next pool-wide object number (inode id). Never reused.
    pub next_object: u64,
    /// The dataset directory's object number (see `crate::dataset`).
    pub datasets: FileId,
}

impl Uberblock {
    /// One that no import takes (it overwrites a ring slot).
    fn blank() -> Self {
        Uberblock {
            magic: 0,
            version: 0,
            txg: Txg::ZERO,
            timestamp: 0,
            objects: TreeRoot {
                ptr: None,
                levels: 0,
            },
            degraded: Vec::new(),
            next_object: 0,
            datasets: FileId::from_raw(0),
        }
    }

    /// Whether it's a pool state to import: its magic and version, and the
    /// TXG its label was written in (`txg`).
    pub fn validate(&self, txg: Txg) -> Result<(), String> {
        if self.magic != UBERBLOCK_MAGIC {
            return Err("not an uberblock".into());
        }
        if self.version != UBERBLOCK_VERSION {
            return Err(format!("uberblock version {}", self.version));
        }
        if self.txg != txg {
            return Err(format!("TXG {} in a label of TXG {}", self.txg, txg));
        }
        if self.txg == Txg::ZERO {
            return Err("TXG 0".into());
        }
        if self.datasets.get() == 0 || self.datasets.get() >= self.next_object {
            return Err(format!(
                "dataset directory object {} (next object {})",
                self.datasets, self.next_object
            ));
        }
        Ok(())
    }
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

pub use crate::types::now_ms;

/// A random-enough 64-bit id (no RNG dependency needed).
pub fn new_guid(salt: u64) -> u64 {
    let mut h = blake3::Hasher::new();
    // Secret per process: a restarted process (same pid in a container, a
    // clock stepped back) doesn't repeat ids, and nonces can't be guessed.
    #[cfg(not(target_arch = "wasm32"))]
    h.update(&process_seed().to_le_bytes());
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
    pub txg: Txg,
    pub checksum: [u8; 32],
    pub payload_len: u32,
}

/// A label's checksum: over the header fields as well as the payload, so a
/// flipped bit in the TXG (which decides which copy is newest) is caught.
fn label_checksum(txg: Txg, payload: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(&LABEL_MAGIC.to_le_bytes());
    h.update(&txg.get().to_le_bytes());
    h.update(&(payload.len() as u32).to_le_bytes());
    h.update(payload);
    *h.finalize().as_bytes()
}

/// Validate a self-describing label block in place; returns (txg, payload length).
pub(crate) fn parse_label(buf: &[u8]) -> Result<(Txg, usize), FileSystemError> {
    let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;
    if header.magic != LABEL_MAGIC {
        return Err(FileSystemError::InvalidMagic);
    }
    let payload_len = header.payload_len as usize;
    if payload_len > buf.len().saturating_sub(BLOCK_HEADER_SIZE) {
        return Err(FileSystemError::ChecksumMismatch);
    }
    let payload = &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload_len];
    if label_checksum(header.txg, payload) != header.checksum {
        return Err(FileSystemError::ChecksumMismatch);
    }
    Ok((header.txg, payload_len))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use crate::types::process_seed;

/// Read a pool device's superblock without loading the pool (None: not
/// formatted, or unreadable).
pub async fn read_superblock_of<D: BlockDevice>(dev: &D) -> Option<Superblock> {
    probe_superblock_of(dev).await.ok().flatten()
}

/// No copy of the superblock could be read.
#[derive(Debug)]
pub struct Unreadable;

/// `read_superblock_of`, telling the two Nones apart: `Ok(None)` if a copy
/// was read and none is a superblock (not formatted), `Err` if no copy
/// could be read at all.
pub async fn probe_superblock_of<D: BlockDevice>(
    dev: &D,
) -> Result<Option<Superblock>, Unreadable> {
    let header = HeaderLayout::new(dev.pool().map_or(1, |p| p.align()));
    let mut read_any = false;
    // Every copy of every mirror block, rebuilt ones too (audit D6c): on a
    // mirror every superblock block can come from one member, so a rotten
    // or blank copy there would fail the import with good copies elsewhere.
    // The normal read comes first, so a readable copy wins over a rebuild.
    for mirror in 0..header.superblock_blocks {
        let (read, versions) =
            label_versions_of::<_, Superblock>(dev, SUPERBLOCK_LBA + mirror).await;
        read_any |= read;
        if let Some((_, sb)) = versions.into_iter().find(|(_, sb)| sb.magic == FS_ID) {
            return Ok(Some(sb));
        }
    }
    if read_any { Ok(None) } else { Err(Unreadable) }
}

/// Every valid version of label block `lba` of `dev`: as normally read
/// first, then as held by the other copies / rebuilt from parity. And
/// whether any copy could be read at all.
pub(crate) async fn label_versions_of<D: BlockDevice, T: for<'a> Deserialize<'a>>(
    dev: &D,
    lba: BlockId,
) -> (bool, Vec<(Txg, T)>) {
    let mut blocks = Vec::new();
    let mut buf = [0u8; BLOCK_SIZE];
    if dev.read_block(lba, &mut buf).await.is_ok() {
        blocks.push(buf.to_vec());
    }
    if let Ok(c) = dev.reconstruct_candidates(lba).await {
        for b in c.rebuilt.into_iter().flatten() {
            if !blocks.contains(&b) {
                blocks.push(b);
            }
        }
    }
    let read_any = !blocks.is_empty();
    let versions = blocks
        .iter()
        .filter_map(|buf| {
            let (txg, len) = parse_label(buf).ok()?;
            let value =
                bincode::deserialize(&buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len]).ok()?;
            Some((txg, value))
        })
        .collect();
    (read_any, versions)
}

/// An early-fold request: raised by a flush, taken by the sync loop.
#[derive(Default)]
pub struct FoldWanted {
    raised: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl FoldWanted {
    /// Ask for a fold, unless one is already asked for (and not yet taken).
    pub(crate) fn raise(&self) {
        if !self.raised.swap(true, Ordering::SeqCst) {
            self.notify.notify_one();
        }
    }

    /// Wait for a request.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn wait(&self) {
        self.notify.notified().await
    }

    /// Clear the request once a fold has run; later flushes may raise a
    /// new one.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn take(&self) -> bool {
        self.raised.swap(false, Ordering::SeqCst)
    }
}

pub struct FileSystem<D: BlockDevice> {
    pub dev: D,
    superblock: Arc<RwLock<Option<Superblock>>>,
    /// Last committed uberblock. The open TXG is always `uberblock.txg + 1`.
    pub(crate) uberblock: Arc<Mutex<Option<Uberblock>>>,
    pub layout: MetaslabLayout,
    pub metaslabs: Arc<Vec<Arc<RwLock<MetaslabState>>>>,
    /// The object table, with the open TXG's changes (see `crate::inode`).
    pub(crate) objects: Arc<ObjTree>,
    /// The dataset directory's object number (from the uberblock; fixed
    /// once formatted).
    pub(crate) datasets_obj: Arc<AtomicU64>,
    /// The default posix dataset's root directory (from the dataset
    /// directory; fixed once formatted).
    pub(crate) root_dir: Arc<AtomicU64>,
    /// Open copy-on-write volumes and their dirty records.
    pub open_datasets: Arc<std::sync::Mutex<HashMap<DatasetName, Arc<Dataset>>>>,
    /// Files open for writing at any offset (see `crate::open_file`).
    pub(crate) files: Arc<std::sync::Mutex<HashMap<FileId, Arc<crate::open_file::OpenFile>>>>,
    commit_lock: Arc<Mutex<()>>,
    /// Commits that have begun (taken their snapshot) / finished successfully.
    /// Lets concurrent `sync` calls share one commit (group commit).
    commits_started: Arc<AtomicU64>,
    commits_done: Arc<AtomicU64>,
    /// Set while an uberblock's copies are being written, and left set if
    /// that fails (or is cancelled): some copy may have landed, so that
    /// TXG may be what an import picks, and the space-map slots it names
    /// must not be rewritten. A retry would rewrite them for the same TXG,
    /// with different contents (structural triage I-2). From then on this
    /// pool commits nothing; reopening it reads what's really there. A
    /// committer steps down on it.
    uberblock_in_doubt: Arc<AtomicBool>,
    /// Per open dataset, the newest commit (by `commits_started` id) that
    /// left it with nothing dirty from before that commit began: its writes
    /// up to then are durable, whatever happened to other datasets in that
    /// commit (see `sync_for`).
    clean_at: Arc<std::sync::Mutex<HashMap<DatasetName, u64>>>,
    /// Opened for inspection: never writes (no commits, no self-heal repairs),
    /// so it is safe to point at a pool another process is serving.
    pub(crate) read_only: bool,
    /// See `Uberblock::degraded`; persisted with every commit. The pool's
    /// `DiskSet::degraded`, shared.
    pub degraded: Arc<std::sync::Mutex<Vec<Option<Txg>>>>,
    /// Per disk: how many times it has been marked degraded (this process).
    /// A resilver clears a disk only if no mark came while it scanned: one
    /// that did is a write the scan's snapshot doesn't have (audit D4).
    degraded_marks: Arc<std::sync::Mutex<Vec<u64>>>,
    /// Per-disk missed-write counts already folded into `degraded`.
    pub(crate) miss_seen: Arc<std::sync::Mutex<Vec<u64>>>,
    /// Record alignment in blocks (the pool's, or 1 for a plain device).
    pub(crate) align: u64,
    pub header: HeaderLayout,
    /// While non-zero, expired deferred frees are not made allocatable, so
    /// nothing a running scan may still rewrite can be reallocated.
    pub scan_hold: Arc<AtomicUsize>,
    /// One scrub/resilver at a time.
    pub(crate) scan_lock: Arc<Mutex<()>>,
    /// Set when opened as a volume writer (see `crate::writer`).
    pub writer: Option<Arc<crate::writer::WriterState>>,
    /// Next pool-wide object number (persisted in every uberblock).
    pub(crate) next_object: Arc<AtomicU64>,
    /// Serializes a writer's intent log appends (see `crate::zil`).
    /// Taken through `lock_log_io`, whose guard proves it held.
    pub(crate) log_io: Arc<Mutex<()>>,
    /// A volume's intent log is filling up: the background sync loop (see
    /// `crate::nbd::ensure_sync_loop`) folds early, so the next region is
    /// registered before flushes would have to fall back to a commit.
    pub fold_wanted: Arc<FoldWanted>,
    /// The lowest TXG new blocks may be stamped with (see
    /// `raise_birth_floor`).
    pub(crate) birth_floor: Arc<AtomicU64>,
    /// Snapshots read so far, by object number (see `crate::snapshot`).
    pub(crate) snap_trees: Arc<std::sync::Mutex<HashMap<FileId, Arc<ObjTree>>>>,
}

impl<D: BlockDevice> FileSystem<D> {
    fn empty(dev: D) -> Self {
        let align = dev.pool().map_or(1, |p| p.align());
        let disks = dev.pool().map_or(0, |p| p.disks().len());
        let degraded = dev.pool().map_or_else(
            || Arc::new(std::sync::Mutex::new(Vec::new())),
            |p| p.disks().degraded.clone(),
        );
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
            datasets_obj: Arc::new(AtomicU64::new(0)),
            root_dir: Arc::new(AtomicU64::new(0)),
            open_datasets: Arc::new(std::sync::Mutex::new(HashMap::new())),
            files: Arc::new(std::sync::Mutex::new(HashMap::new())),
            commit_lock: Arc::new(Mutex::new(())),
            commits_started: Arc::new(AtomicU64::new(0)),
            commits_done: Arc::new(AtomicU64::new(0)),
            uberblock_in_doubt: Arc::new(AtomicBool::new(false)),
            clean_at: Default::default(),
            read_only: false,
            degraded,
            degraded_marks: Arc::new(std::sync::Mutex::new(vec![0; disks])),
            miss_seen: Arc::new(std::sync::Mutex::new(vec![0; disks])),
            align,
            header: HeaderLayout::new(align),
            scan_hold: Arc::new(AtomicUsize::new(0)),
            scan_lock: Arc::new(Mutex::new(())),
            writer: None,
            next_object: Arc::new(AtomicU64::new(1)),
            log_io: Arc::new(Mutex::new(())),
            fold_wanted: Arc::new(FoldWanted::default()),
            birth_floor: Arc::new(AtomicU64::new(0)),
            snap_trees: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Tell this process's remote disk clients how the pool is laid out, so
    /// each request carries its fencing domain's epoch.
    fn publish_fence_map(&self) {
        if let Some(pool) = self.dev.pool() {
            pool.disks().fence().set_map(crate::fence::FenceMap::new(
                pool.topology().clone(),
                self.layout,
            ));
        }
    }

    /// A new pool-wide object number. Only the process that commits the
    /// pool (the committer, or a standalone pool holder) hands them out.
    pub(crate) fn alloc_object(&self) -> FileId {
        debug_assert!(self.writer.is_none(), "writers don't assign object numbers");
        FileId::from_raw(self.next_object.fetch_add(1, Ordering::SeqCst))
    }

    /// From now on, commits must stamp every new block with TXG `txg` or
    /// later. A snapshot taken at TXG `t` raises it to `t + 1`, so every
    /// block a later commit adds is born after the snapshot (and a block's
    /// birth says whether a snapshot has it). Writers whose blocks are
    /// stamped lower (their view of the TXG lags) are told to restamp.
    pub fn raise_birth_floor(&self, txg: Txg) {
        self.birth_floor.fetch_max(txg.get(), Ordering::SeqCst);
    }

    pub fn birth_floor(&self) -> Txg {
        Txg::from_raw(self.birth_floor.load(Ordering::SeqCst))
    }

    /// Leaf disks of the pool underneath (0 for a plain device).
    pub fn disk_count(&self) -> usize {
        self.dev.pool().map_or(0, |p| p.disks().len())
    }

    /// Every disk's missed-write count right now.
    pub(crate) fn miss_counts(&self) -> Vec<u64> {
        match self.dev.pool() {
            Some(pool) => (0..pool.disks().len())
                .map(|d| pool.disks().miss_count(d))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Largest record, in blocks.
    pub fn max_record_blocks(&self) -> u64 {
        crate::vdev::max_record_blocks(self.align)
    }

    /// Round `blocks` up to whole aligned units.
    pub fn align_blocks(&self, blocks: u64) -> u64 {
        blocks.div_ceil(self.align) * self.align
    }

    pub async fn superblock(&self) -> Option<Superblock> {
        self.superblock.read().await.clone()
    }

    pub async fn uberblock(&self) -> Option<Uberblock> {
        self.uberblock.lock().await.clone()
    }

    /// TXG that blocks written right now belong to.
    pub async fn open_txg(&self) -> Result<Txg, FileSystemError> {
        self.uberblock
            .lock()
            .await
            .as_ref()
            .map(|u| u.txg.next())
            .ok_or(FileSystemError::NotFormatted)
    }

    /// Format file system
    pub async fn format(dev: D, total_blocks: u64) -> Result<Self, FileSystemError> {
        let mut fs = Self::empty(dev);

        // In-memory TXG 0; everything written by format belongs to TXG 1.
        let zero_uber = Uberblock {
            degraded: vec![None; fs.disk_count()],
            next_object: 1,
            ..Uberblock::blank()
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
            magic: FS_ID,
            block_size: BLOCK_SIZE as u32,
            total_blocks,
            metaslab_count: layout.metaslab_count,
            metaslab_size_blocks: layout.metaslab_size_blocks,
            spacemap_start: layout.spacemap_start,
            spacemap_slot_blocks: layout.spacemap_slot_blocks,
            data_start: layout.data_start,
            pool_guid: new_guid(total_blocks),
            align: fs.align,
            topology: fs.dev.pool().map(|p| p.topology().clone()),
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
                fs.degraded.lock().unwrap()[d] = Some(Txg::ZERO);
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
        // (After the table and metaslabs are in place: a tx journals into
        // the ones it began with.)
        let mut tx = fs.begin_tx().await;
        let tx = &mut tx;
        // The dataset directory, and the default posix dataset's root.
        let datasets = fs.alloc_object();
        fs.datasets_obj.store(datasets.get(), Ordering::SeqCst);
        fs.write_dnode(tx, datasets, Some(Dnode::datasets(fs.file_blksz())))
            .await?;
        let root = fs.alloc_object();
        let mut dir = Dnode::directory(fs.file_blksz());
        dir.parent = Some(root);
        fs.write_dnode(tx, root, Some(dir)).await?;
        let mut index = DatasetIndex::default();
        index.datasets.insert(
            DEFAULT_FS.to_string(),
            DatasetEntry {
                root,
                format: DatasetFormat::Posix,
                guid: new_guid(root.get()),
                created_txg: Txg::from_raw(1),
                created_ms: now_ms(),
            },
        );
        fs.write_datasets(tx, &index).await?;
        fs.root_dir.store(root.get(), Ordering::SeqCst);
        fs.commit_locked(tx).await?;
        tx.applied();

        Ok(fs)
    }

    /// The object table at `root`.
    pub(crate) fn table_tree(&self, root: TreeRoot) -> ObjTree {
        ObjTree::new(self.table_blksz(), Sizing::Growable, 0, root)
    }

    /// Object `id`'s dnode in the object table at `root` (None: missing or
    /// unreadable).
    async fn dnode_in(&self, table: &ObjTree, id: FileId) -> Option<Dnode> {
        let buf = self
            .object_record(table, id.get() / crate::inode::DNODES_PER_LEAF)
            .await
            .ok()??;
        let slots = crate::inode::decode_leaf(&buf).ok()?;
        slots
            .get((id.get() % crate::inode::DNODES_PER_LEAF) as usize)
            .cloned()
            .flatten()
    }

    /// Whether `uber`'s dataset directory and root directory read.
    async fn root_readable(&self, uber: &Uberblock) -> bool {
        let table = self.table_tree(uber.objects.clone());
        let Some(ds) = self.dnode_in(&table, uber.datasets).await else {
            return false;
        };
        let Ok(index) = self.read_datasets(&ds).await else {
            return false;
        };
        let Ok(root) = Self::default_root(&index) else {
            return false;
        };
        let Some(dir) = self.dnode_in(&table, root).await else {
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
            && superblock.topology.as_ref() != Some(pool.topology())
        {
            return Err(FileSystemError::TopologyMismatch(format!(
                "pool was formatted as {}, configured as {}",
                superblock
                    .topology
                    .as_ref()
                    .map_or("a plain device".to_string(), |t| t.summary()),
                pool.topology().summary()
            )));
        }
        let Some(newest) = ring.first().cloned() else {
            return Ok(fs);
        };
        fs.layout = MetaslabLayout::from_superblock(&superblock);
        fs.publish_fence_map();
        // Fixed once formatted, so every TXG in the ring has the same one;
        // choosing a rewind target walks namespaces from it.
        fs.datasets_obj
            .store(newest.datasets.get(), Ordering::SeqCst);

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
                    if fs.root_readable(u).await {
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
        // The dataset directory and the root directory must read.
        if uber.datasets != newest.datasets {
            return Err(FileSystemError::InvalidArgument(format!(
                "TXG {} names dataset directory {}, the newest TXG {}",
                uber.txg, uber.datasets, newest.datasets
            )));
        }
        let index = fs.datasets().await?;
        let root_id = Self::default_root(&index)?;
        let root = fs.read_dnode(root_id).await?;
        fs.read_dir(&root).await?;
        fs.root_dir.store(root_id.get(), Ordering::SeqCst);

        if rewound && !fs.read_only {
            let older: Vec<TreeRoot> = ring
                .iter()
                .filter(|u| u.txg < uber.txg)
                .map(|u| u.objects.clone())
                .collect();
            let mut tx = fs.begin_tx().await;
            let tx = &mut tx;
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
            // Committed above every discarded TXG (audit D3): a disk that
            // missed the rewind (degraded, offline) still has their
            // uberblocks, and newest-wins must never pick one again; nor
            // may a copy of one outrank the tombstones below in a repair.
            if let Some(newest) = ring.iter().map(|u| u.txg).max()
                && let Some(u) = fs.uberblock.lock().await.as_mut()
            {
                u.txg = u.txg.max(newest);
            }
            let txg = fs.commit_locked(tx).await?;
            eprintln!("rewind: committed as TXG {}", txg);
            // The discarded TXGs must never be imported again: their space
            // is free now and about to be reused.
            for u in ring.iter().filter(|u| {
                u.txg > uber.txg && u.txg.slot(UBERBLOCK_SLOTS) != txg.slot(UBERBLOCK_SLOTS)
            }) {
                if let Err(e) = fs.invalidate_uberblock_slot(u.txg, txg).await {
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
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.mkdir_locked(tx, path).await;
        let id = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    /// `mkdir` under `begin_tx`, without committing (the committer applies
    /// writers' mkdirs this way). An existing directory is fine.
    pub(crate) async fn mkdir_locked(
        &self,
        tx: &mut crate::tx::Tx,
        path: &Path,
    ) -> Result<FileId, FileSystemError> {
        let l = self.lookup(path).await?;
        if let Some(e) = l.entry {
            if e.kind != crate::inode::ObjType::Directory {
                return Err(FileSystemError::NotADirectory);
            }
            return Ok(e.id);
        }
        self.link_new(tx, l, Dnode::directory(self.file_blksz()))
            .await
    }

    /// Create or replace a file with `data`. On a pool this process
    /// commits, it is one transaction: no commit sees it half done.
    pub async fn write_file(&self, path: &Path, data: &[u8]) -> Result<FileId, FileSystemError> {
        if self.writer.is_some() {
            let mut f = self.file_writer(path, None).await?;
            f.write(data).await?;
            return f.finish().await;
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let l = self.lookup(path).await?;
        let current = self.current_file(&l).await?;
        let blksz = self.file_blksz();
        // The tx gives them back if the op fails or is dropped.
        let tree = {
            let mut allocs = tx.allocs();
            self.write_tree(blksz as usize, data, &mut allocs.ptrs)
                .await
        };
        let r = match tree {
            Ok(tree) => {
                self.set_file_contents(tx, l, current, blksz, data.len() as u64, tree)
                    .await
            }
            Err(e) => Err(e),
        };
        let id = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    /// Delete a file or an empty directory.
    pub async fn delete(&self, path: &Path) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_delete(&w, path, None).await;
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.delete_locked(tx, path).await;
        let id = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    /// Move `from` to `to` (see `rename_locked`). Returns the object's number.
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            return self.writer_rename(&w, from, to).await;
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.rename_locked(tx, from, to).await;
        let (id, _) = tx.check(r).await?;
        self.commit_locked(tx).await?;
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
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.set_attrs_locked(tx, path, set).await;
        let id = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    // ---- by object number (what NFS speaks) ----

    /// Entry `name` of directory `dir`: its number and dnode (None: no such
    /// entry).
    pub async fn lookup_in(
        &self,
        dir: FileId,
        name: &str,
    ) -> Result<Option<(FileId, Dnode)>, FileSystemError> {
        match self.lookup_at(dir, name).await?.entry {
            Some(e) => Ok(Some((e.id, self.read_dnode(e.id).await?))),
            None => Ok(None),
        }
    }

    /// Create file or directory `name` in `dir` (see `create_at_locked`).
    pub async fn create_at(
        &self,
        dir: FileId,
        name: &str,
        kind: crate::inode::ObjType,
        attrs: &crate::inode::SetAttrs,
        exclusive: Option<u64>,
    ) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            let op = crate::writer::FsOp::CreateAt {
                dir,
                name: name.to_string(),
                kind,
                attrs: attrs.clone(),
                exclusive,
            };
            return Ok(self.writer_commit_fs(&w, op, None).await?.file_id);
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self
            .create_at_locked(tx, dir, name, kind, attrs, exclusive)
            .await;
        let (id, created) = tx.check(r).await?;
        if created {
            self.commit_locked(tx).await?;
        }
        Ok(id)
    }

    /// Remove entry `name` of `dir` (a directory must be empty).
    pub async fn remove_at(&self, dir: FileId, name: &str) -> Result<FileId, FileSystemError> {
        self.remove_at_leased(dir, name, None).await
    }

    /// `remove_at`, on a writer under the file's `obj/<id>` lease if the
    /// caller holds it: without it, the committer refuses to remove a file
    /// someone holds that lease on, the caller included.
    pub async fn remove_at_leased(
        &self,
        dir: FileId,
        name: &str,
        lease: Option<crate::writer::ObjLease>,
    ) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            let _tx = self.begin_tx().await;
            self.reload_root_index().await?;
            let id = self
                .lookup_at(dir, name)
                .await?
                .entry
                .ok_or(FileSystemError::FileNotFound)?
                .id;
            let op = crate::writer::FsOp::RemoveAt {
                dir,
                name: name.to_string(),
                id,
            };
            self.writer_commit_fs(&w, op, lease).await?;
            return Ok(id);
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.remove_at_locked(tx, dir, name).await;
        let id = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    /// `remove_at` under `begin_tx`, without committing.
    pub(crate) async fn remove_at_locked(
        &self,
        tx: &mut crate::tx::Tx,
        dir: FileId,
        name: &str,
    ) -> Result<FileId, FileSystemError> {
        let l = self.lookup_at(dir, name).await?;
        let entry = l.entry.ok_or(FileSystemError::FileNotFound)?;
        let id = self.unlink(tx, l).await?;
        // Only once it's gone (this can't be undone); no sync can run
        // before this, under the tx.
        self.discard_file(entry.id);
        Ok(id)
    }

    /// Move entry `from_name` of `from_dir` to `to_name` in `to_dir` (see
    /// `rename_at_locked`). Returns the moved object's number.
    pub async fn rename_at(
        &self,
        from_dir: FileId,
        from_name: &str,
        to_dir: FileId,
        to_name: &str,
    ) -> Result<FileId, FileSystemError> {
        self.rename_at_leased(from_dir, from_name, to_dir, to_name, None)
            .await
    }

    /// `rename_at`, on a writer under the `obj/<id>` lease of the file it
    /// replaces, if the caller holds it (see `remove_at_leased`).
    pub async fn rename_at_leased(
        &self,
        from_dir: FileId,
        from_name: &str,
        to_dir: FileId,
        to_name: &str,
        lease: Option<crate::writer::ObjLease>,
    ) -> Result<FileId, FileSystemError> {
        if let Some(w) = self.writer.clone() {
            let _tx = self.begin_tx().await;
            self.reload_root_index().await?;
            let id = self
                .lookup_at(from_dir, from_name)
                .await?
                .entry
                .ok_or(FileSystemError::FileNotFound)?
                .id;
            let op = crate::writer::FsOp::RenameAt {
                from_dir,
                from_name: from_name.to_string(),
                to_dir,
                to_name: to_name.to_string(),
                id,
            };
            self.writer_commit_fs(&w, op, lease).await?;
            return Ok(id);
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let src = self.lookup_at(from_dir, from_name).await?;
        let dst = self.lookup_at(to_dir, to_name).await?;
        let r = self.rename_at_locked(tx, src, dst).await;
        let (id, _) = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(id)
    }

    /// Change object `id`'s mode, owner or modification time.
    pub async fn set_attrs_id(
        &self,
        id: FileId,
        set: &crate::inode::SetAttrs,
    ) -> Result<(), FileSystemError> {
        if let Some(w) = self.writer.clone() {
            let op = crate::writer::FsOp::SetAttrId {
                id,
                attrs: set.clone(),
            };
            self.writer_commit_fs(&w, op, None).await?;
            return Ok(());
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r = self.set_attrs_id_locked(tx, id, set).await;
        tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(())
    }

    /// `delete` under `begin_tx`, without committing.
    pub async fn delete_locked(
        &self,
        tx: &mut crate::tx::Tx,
        path: &Path,
    ) -> Result<FileId, FileSystemError> {
        let l = self.lookup(path).await?;
        let entry = l.entry.ok_or(FileSystemError::FileNotFound)?;
        let id = self.unlink(tx, l).await?;
        // Only once it's gone (this can't be undone); no sync can run
        // before this, under the tx.
        self.discard_file(entry.id);
        Ok(id)
    }

    /// Make every write completed before this call durable.
    ///
    /// Any commit that *starts* after this call covers those writes, so if
    /// one finished while we waited for the commit lock there's nothing left
    /// to do. Concurrent flushes (fsync storms, multi-queue NVMe) therefore
    /// share commits instead of each running one.
    pub async fn sync(&self) -> Result<(), FileSystemError> {
        self.sync_scoped(None).await
    }

    /// `sync` for dataset `name` alone: every write to it completed before
    /// this call is durable. Other datasets' failures in the same commit
    /// (one refused, out of space, its disk writes failing) don't fail it.
    pub async fn sync_for(&self, name: &DatasetName) -> Result<(), FileSystemError> {
        self.sync_scoped(Some(name)).await
    }

    /// `sync` (None) or `sync_for` (Some).
    pub(crate) async fn sync_scoped(
        &self,
        only: Option<&DatasetName>,
    ) -> Result<(), FileSystemError> {
        let need = self.commits_started.load(Ordering::SeqCst) + 1;
        let covered = || {
            self.commits_done.load(Ordering::SeqCst) >= need
                || only.is_some_and(|n| self.clean_since(n, need))
        };
        let mut tx = self.begin_tx().await;
        if covered() {
            return Ok(());
        }
        match self.commit_locked(&mut tx).await {
            Ok(_) => Ok(()),
            Err(_) if covered() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Whether a commit numbered `need` or later left `name` clean.
    fn clean_since(&self, name: &str, need: u64) -> bool {
        self.clean_at
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|&id| id >= need)
    }

    /// Note that commit `id` left each of `names` clean (durably).
    pub(crate) fn note_clean(&self, id: u64, names: &[DatasetName]) {
        let mut c = self.clean_at.lock().unwrap();
        for n in names {
            let e = c.entry(n.clone()).or_insert(0);
            *e = (*e).max(id);
        }
    }

    /// Commit the open TXG:
    ///   1. write dirty metaslabs to their inactive space map slots
    ///   2. flush
    ///   3. write the uberblock for the TXG
    ///   4. flush
    ///
    /// Only after step 4 does in-memory state advance (active slots, deferred
    /// frees). On failure, the TXG stays open and can be retried.
    pub async fn commit_txg(&self) -> Result<Txg, FileSystemError> {
        let mut tx = self.begin_tx().await;
        self.commit_locked(&mut tx).await
    }

    /// Commit only if `still_needed()` holds once the commit lock is ours:
    /// callers that queued behind a commit which already did the work
    /// return without running another one.
    pub(crate) async fn commit_if(
        &self,
        still_needed: impl Fn() -> bool,
    ) -> Result<(), FileSystemError> {
        let mut tx = self.begin_tx().await;
        if still_needed() {
            self.commit_locked(&mut tx).await?;
        }
        Ok(())
    }

    /// Start a metadata transaction (see `crate::tx`): excludes commits
    /// (and other metadata operations) until dropped, so no commit can
    /// capture a half-done operation, and undoes one that fails. Finish
    /// with `commit_locked`.
    pub async fn begin_tx(&self) -> crate::tx::Tx {
        crate::tx::Tx::begin(
            &self.commit_lock,
            self.objects.clone(),
            self.metaslabs.clone(),
            self.layout,
            self.scan_hold.clone(),
            self.uberblock.clone(),
        )
        .await
    }

    /// Commit the open TXG under `tx`. What it takes stands, whether it
    /// succeeds or not (see `crate::tx`).
    pub async fn commit_locked(&self, tx: &mut crate::tx::Tx) -> Result<Txg, FileSystemError> {
        debug_assert!(tx.is_of(&self.commit_lock), "another pool's tx");
        tx.begin_commit();
        let r = self.commit_in(tx).await;
        // Normally settled already; a defensive no-op otherwise.
        tx.settle_commit().await;
        tx.end_commit();
        r
    }

    async fn commit_in(&self, tx: &mut crate::tx::Tx) -> Result<Txg, FileSystemError> {
        // A disk server refused our pool epoch: someone else commits the
        // pool now, and the vdev already failed whatever it refused. Write
        // nothing more (audit R2a).
        if let Some(pool) = self.dev.pool()
            && pool.disks().fence().is_stale()
        {
            return Err(FileSystemError::DiskError(
                crate::disk::DiskError::StaleEpoch,
            ));
        }
        if let Some(w) = self.writer.clone() {
            // A volume writer commits through the committer.
            let id = self.commits_started.fetch_add(1, Ordering::SeqCst) + 1;
            let (r, clean) = self.writer_commit(tx, &w).await;
            // Each dataset's commit is durable once the committer took it.
            self.note_clean(id, &clean);
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
        if self.uberblock_in_doubt() {
            return Err(FileSystemError::Unavailable(
                "an uberblock write failed partway: reopen the pool to commit again".into(),
            ));
        }
        let id = self.commits_started.fetch_add(1, Ordering::SeqCst) + 1;

        let mut span = crate::trace::Span::new("commit");
        let txg = self.open_txg().await?;
        // Volumes sync all-or-nothing each, so whatever did sync is safe to
        // commit; a failure is reported after the TXG is durable.
        let (clean, volumes) = self.sync_datasets_noting(tx).await;
        let files = self.sync_files(tx).await;
        let volumes = volumes.and(files);
        span.mark("objects");
        // Then the object table, which now points at every new tree.
        self.sync_table(tx).await?;
        span.mark("table");
        let root = self.objects.root();

        // From here what the commit takes is carried in `tx` (I-1): given
        // up on, the tx settles it before the commit lock goes.
        self.sync_metaslabs(tx, txg).await?;
        span.mark("metaslabs");

        // The uberblock-carrying flush below is the last write of this TXG;
        // misses from it are caught by the next commit (as `txg`).
        let degraded = self.account_misses(txg);
        let written = self.write_uberblock(txg, root, degraded).await;
        span.mark("uberblock");
        match written {
            Ok(uberblock) => {
                tx.landed(uberblock);
                tx.settle_commit().await;
                self.note_clean(id, &clean);
                span.mark("finish");
                if volumes.is_ok() {
                    self.commits_done.fetch_max(id, Ordering::SeqCst);
                }
                volumes.map(|_| txg)
            }
            Err(e) => {
                tx.settle_commit().await;
                Err(e)
            }
        }
    }

    /// Whether an uberblock write failed after its first copy was started
    /// (see `uberblock_in_doubt`): this pool commits nothing more.
    pub fn uberblock_in_doubt(&self) -> bool {
        self.uberblock_in_doubt.load(Ordering::SeqCst)
    }

    /// Write the object table's changed records into the open TXG. On
    /// failure they stay dirty and nothing visible changes.
    async fn sync_table(&self, tx: &mut crate::tx::Tx) -> Result<(), FileSystemError> {
        let Some(taken) = self.objects.begin_sync() else {
            return Ok(());
        };
        // Given up on before the new root is in, the records go back to
        // dirty, not lost with the sync (I-1).
        let mut unfinished = crate::dataset::EndSyncOnDrop {
            obj: &self.objects,
            taken: Some(taken.clone()),
        };
        let (mut allocated, mut replaced) = (Vec::new(), Vec::new());
        match self
            .build_tree(&self.objects, &taken, &mut allocated, &mut replaced)
            .await
        {
            Ok(root) => {
                unfinished.taken = None;
                self.objects.set_root(root);
                self.objects.end_sync(&taken, Some(&taken));
                // What the new table no longer references is freed, the
                // rest of it as unsettled if this is given up on partway:
                // on a pool that commits, the same thing (I-1).
                let mut left = crate::object::Unsettled::all(self, &mut replaced);
                while let Some(p) = left.ptrs.last().cloned() {
                    self.free(tx, p.lba, p.asize as u64).await?;
                    left.ptrs.pop();
                }
                left.disarm();
                Ok(())
            }
            Err(e) => {
                self.release_ptrs(&allocated).await;
                drop(unfinished);
                Err(e)
            }
        }
    }

    /// Fold missed writes since the last commit into `degraded`. A miss seen
    /// now happened during TXG `txg` or late in `txg - 1` (the previous
    /// commit's uberblock write), so the disk needs everything from `txg - 1`.
    fn account_misses(&self, txg: Txg) -> Vec<Option<Txg>> {
        let mut degraded = self.degraded.lock().unwrap();
        if let Some(pool) = self.dev.pool() {
            let mut seen = self.miss_seen.lock().unwrap();
            for d in 0..pool.disks().len() {
                let count = pool.disks().miss_count(d);
                if count != seen[d] {
                    seen[d] = count;
                    let since = txg.prev();
                    if degraded[d].is_none() {
                        eprintln!("disk {} missed writes; degraded since TXG {}", d, since);
                    }
                    degraded[d] = Some(degraded[d].map_or(since, |t| t.min(since)));
                    self.degraded_marks.lock().unwrap()[d] += 1;
                }
            }
        }
        degraded.clone()
    }

    /// `disk` has everything (a resilver found it so): persisted at the
    /// next commit. Only under the tx, so no commit or label write can come
    /// between the check that allowed it and the clear, and only if `disk`
    /// hasn't been marked since `mark` (`degraded_mark`, taken when the scan
    /// began): a mark since is a miss the scan didn't see, such as a
    /// writer's (audit D4). `misses`: its miss count then (see
    /// `DiskSet::caught_up`). Whether it was cleared.
    pub(crate) fn clear_degraded(
        &self,
        _tx: &crate::tx::Tx,
        disk: usize,
        mark: u64,
        misses: u64,
    ) -> bool {
        let mut degraded = self.degraded.lock().unwrap();
        if self.degraded_marks.lock().unwrap()[disk] != mark {
            return false;
        }
        degraded[disk] = None;
        if let Some(pool) = self.dev.pool() {
            pool.disks().caught_up(disk, misses);
        }
        true
    }

    /// How many times `disk` has been marked (see `clear_degraded`).
    pub(crate) fn degraded_mark(&self, disk: usize) -> u64 {
        let _degraded = self.degraded.lock().unwrap();
        self.degraded_marks.lock().unwrap()[disk]
    }

    /// Mark `disk` as needing everything from TXG `since` (persisted at the
    /// next commit).
    pub fn mark_degraded(&self, disk: usize, since: Txg) {
        let mut degraded = self.degraded.lock().unwrap();
        degraded[disk] = Some(degraded[disk].map_or(since, |t| t.min(since)));
        self.degraded_marks.lock().unwrap()[disk] += 1;
    }

    /// Per disk: have writes failed since the last commit folded them into
    /// `degraded`?
    pub fn unaccounted_misses(&self) -> Vec<bool> {
        let seen = self.miss_seen.lock().unwrap();
        self.miss_counts()
            .iter()
            .zip(seen.iter())
            .map(|(now, seen)| now != seen)
            .collect()
    }

    pub fn degraded_disks(&self) -> Vec<Option<Txg>> {
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
            let mut buf = [0u8; BLOCK_SIZE];
            if pool.disks().read_label(disk, copy, &mut buf).await.is_err() {
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
        let txg = self.uberblock().await.map_or(Txg::ZERO, |u| u.txg);
        let mut buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, &payload, &mut buf)?;
        for copy in 0..crate::disks::DISK_LABEL_BLOCKS {
            pool.disks().write_label(disk, copy, &buf).await?;
        }
        pool.disks().flush_disk(disk).await?;
        Ok(())
    }

    /// Check every member's label (at load, or when a disk reconnects): a
    /// foreign disk is fenced (no reads) and needs a full resilver.
    pub(crate) async fn adopt_disks(&self) {
        for d in 0..self.disk_count() {
            self.adopt_disk(d).await;
        }
    }

    pub async fn adopt_disk(&self, d: usize) -> LabelState {
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
                pool.disks().set_fenced(d, true);
                self.mark_degraded(d, Txg::ZERO);
            }
        }
        state
    }

    async fn write_uberblock(
        &self,
        txg: Txg,
        objects: TreeRoot,
        degraded: Vec<Option<Txg>>,
    ) -> Result<Uberblock, FileSystemError> {
        // Everything the uberblock points at must be durable first.
        self.dev.flush().await?;

        let uberblock = Uberblock {
            magic: UBERBLOCK_MAGIC,
            version: UBERBLOCK_VERSION,
            txg,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            objects,
            degraded,
            next_object: self.next_object.load(Ordering::SeqCst),
            datasets: self.datasets_object(),
        };

        // Rotate through the ring; one copy per block of the slot.
        let slot_blocks = self.header.uberblock_slot_blocks;
        let base_lba = self.header.uberblock_start + txg.slot(UBERBLOCK_SLOTS) * slot_blocks;

        let payload = bincode::serialize(&uberblock)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, &payload, &mut block_buf)?;

        // Up to here a failure leaves no trace of `txg` anywhere an import
        // looks: the space-map slots it wrote aren't named by any
        // uberblock, so a retry may rewrite them. From the first copy on,
        // `txg` may be on disk (see `uberblock_in_doubt`). Cleared only on
        // success, so a cancelled write leaves it set too.
        self.uberblock_in_doubt.store(true, Ordering::SeqCst);
        for copy in 0..slot_blocks {
            self.dev.write_block(base_lba + copy, &block_buf).await?;
        }

        self.dev.flush().await?;
        self.uberblock_in_doubt.store(false, Ordering::SeqCst);

        Ok(uberblock)
    }

    /// Overwrite the ring slot holding TXG `txg` with an invalid (zero-magic)
    /// uberblock, so it can't be imported any more. The tombstone is a
    /// label of TXG `at` (the rewind's commit, above everything discarded):
    /// a stale copy of the discarded uberblock on a disk that missed this
    /// is older, so label repair rewrites it with the tombstone, never the
    /// other way round (audit D3).
    async fn invalidate_uberblock_slot(&self, txg: Txg, at: Txg) -> Result<(), FileSystemError> {
        let slot_blocks = self.header.uberblock_slot_blocks;
        let base_lba = self.header.uberblock_start + txg.slot(UBERBLOCK_SLOTS) * slot_blocks;
        let blank = Uberblock::blank();
        let mut buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(at, &bincode::serialize(&blank)?, &mut buf)?;
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
        txg: Txg,
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
        txg: Txg,
        data: &[u8],
        buf: &mut [u8; BLOCK_SIZE],
    ) -> Result<(), FileSystemError> {
        let checksum = label_checksum(txg, data);
        let header = BlockHeader {
            magic: LABEL_MAGIC,
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
        // Written to several mirror blocks, each with its copies or parity:
        // the first good one anywhere (see `probe_superblock_of`).
        match probe_superblock_of(&self.dev).await {
            Ok(Some(sb)) => *self.superblock.write().await = Some(sb),
            Ok(None) => eprintln!("superblock: no copy is a raidish superblock"),
            Err(Unreadable) => eprintln!("superblock: no copy could be read"),
        }
    }

    /// Every valid uberblock version found in the ring, newest first.
    pub(crate) async fn read_uberblock_ring(&self) -> Vec<Uberblock> {
        let mut found: Vec<Uberblock> = Vec::new();
        for i in 0..self.header.uberblock_blocks() {
            let lba = self.header.uberblock_start + i;
            for (txg, ub) in self.read_label_versions::<Uberblock>(lba).await {
                if ub.validate(txg).is_err() {
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
    async fn read_label_versions<T: for<'a> Deserialize<'a>>(&self, lba: BlockId) -> Vec<(Txg, T)> {
        label_versions_of(&self.dev, lba).await.1
    }
}

#[cfg(test)]
#[path = "tests/fs_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/crash_tests.rs"]
mod crash_tests;
