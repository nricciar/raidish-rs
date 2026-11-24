use crate::websocket::{WS_MAX_BLOCKS, WsOp, WsStatus};
use async_trait::async_trait;
use futures::future::try_join_all;
use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use url::ParseError;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u64;

/// The byte offset of block `start`, if `blocks` blocks from it end at a
/// byte offset a file can address (`i64`). `None` for a range whose bytes
/// would wrap: `start * BLOCK_SIZE` must never be computed unchecked, since
/// a wrapped offset lands on some other block (structural triage I-9).
pub fn byte_offset(start: BlockId, blocks: u64) -> Option<u64> {
    let end = start.checked_add(blocks)?.checked_mul(BLOCK_SIZE as u64)?;
    (end <= i64::MAX as u64).then(|| start * BLOCK_SIZE as u64)
}

fn bad_offset() -> DiskError {
    DiskError::IoError(std::io::ErrorKind::InvalidInput.into())
}
pub use crate::types::FileId;

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
    #[cfg(not(target_arch = "wasm32"))]
    TungsteniteError(tungstenite::error::Error),
    UnhealthyDisk,
    /// A remote disk with no live connection (reconnecting in the background).
    Disconnected,
    /// A remote disk didn't answer in time.
    Timeout,
    InvalidUri(ParseError),
    /// The disk server refused a write: our lease epoch is out of date.
    StaleEpoch,
    /// The disk server refused a write into a metaslab lent to a writer,
    /// stamped with something other than that lend's epoch (audit R2b).
    /// Says nothing about our standing in the pool.
    Fenced,
    /// The disk server is starting: it refuses writes until its fence is
    /// ready (`crate::fence::Ready`). The write missed the disk.
    NotReady,
}

impl DiskError {
    /// A fencing refusal: the write was refused for who sent it, not for
    /// the disk. Never a reason to count the disk as having missed data,
    /// and never tolerable on some members of a vdev (audit R2a).
    pub fn is_fencing(&self) -> bool {
        matches!(self, DiskError::StaleEpoch | DiskError::Fenced)
    }

    /// About reaching the disk (or our standing with it), not what's on
    /// it: something a later try may not hit.
    pub fn is_transient(&self) -> bool {
        match self {
            DiskError::UnhealthyDisk
            | DiskError::Disconnected
            | DiskError::Timeout
            | DiskError::StaleEpoch
            | DiskError::Fenced
            | DiskError::NotReady => true,
            #[cfg(not(target_arch = "wasm32"))]
            DiskError::TungsteniteError(_) => true,
            DiskError::IoError(_) | DiskError::InvalidUri(_) => false,
        }
    }
}

impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskError::IoError(err) => write!(f, "I/O error: {}", err),
            #[cfg(not(target_arch = "wasm32"))]
            DiskError::TungsteniteError(err) => write!(f, "WebSocket error: {}", err),
            DiskError::UnhealthyDisk => write!(f, "disk is unhealthy"),
            DiskError::Disconnected => write!(f, "disk is disconnected"),
            DiskError::Timeout => write!(f, "disk timed out"),
            DiskError::InvalidUri(err) => write!(f, "invalid URI: {}", err),
            DiskError::StaleEpoch => write!(f, "stale lease epoch (another writer holds the pool)"),
            DiskError::Fenced => write!(f, "fenced out: the space is lent to another writer"),
            DiskError::NotReady => write!(f, "disk server is starting (writes wait for its fence)"),
        }
    }
}

impl From<std::io::Error> for DiskError {
    fn from(error: std::io::Error) -> Self {
        DiskError::IoError(error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tungstenite::error::Error> for DiskError {
    fn from(error: tungstenite::error::Error) -> Self {
        DiskError::TungsteniteError(error)
    }
}

impl From<ParseError> for DiskError {
    fn from(error: ParseError) -> Self {
        DiskError::InvalidUri(error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tokio::task::JoinError> for DiskError {
    fn from(error: tokio::task::JoinError) -> Self {
        DiskError::IoError(std::io::Error::other(error))
    }
}

/// One block's bytes. Runs of blocks are `[Block]`: whole blocks by type
/// (`as_flattened` for their bytes).
pub type Block = [u8; BLOCK_SIZE];

/// `n` zeroed blocks.
pub fn zeroed_blocks(n: usize) -> Vec<Block> {
    vec![[0u8; BLOCK_SIZE]; n]
}

/// `bytes` as whole blocks; refused if it isn't a whole number of them.
pub fn as_blocks(bytes: &[u8]) -> Result<&[Block], DiskError> {
    match bytes.as_chunks::<BLOCK_SIZE>() {
        (blocks, []) => Ok(blocks),
        _ => Err(DiskError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} bytes isn't whole blocks", bytes.len()),
        ))),
    }
}

/// `bytes` as whole blocks, to read into; refused if it isn't a whole
/// number of them.
pub fn as_blocks_mut(bytes: &mut [u8]) -> Result<&mut [Block], DiskError> {
    let len = bytes.len();
    match bytes.as_chunks_mut::<BLOCK_SIZE>() {
        (blocks, []) => Ok(blocks),
        _ => Err(DiskError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} bytes isn't whole blocks", len),
        ))),
    }
}

#[async_trait]
pub trait BlockDevice: Send + Sync {
    async fn flush(&self) -> Result<(), DiskError>;
    async fn read_block(&self, block: BlockId, buf: &mut Block) -> Result<(), DiskError>;
    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError>;

    /// Usable capacity in blocks.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
    }

    /// Read `buf.len()` consecutive blocks starting at `start`.
    async fn read_blocks(&self, start: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        for (i, b) in buf.iter_mut().enumerate() {
            self.read_block(start + i as u64, b).await?;
        }
        Ok(())
    }

    /// Write `buf.len()` consecutive blocks starting at `start`.
    async fn write_blocks(&self, start: BlockId, buf: &[Block]) -> Result<(), DiskError> {
        for (i, b) in buf.iter().enumerate() {
            self.write_block(start + i as u64, b).await?;
        }
        Ok(())
    }

    /// Alternative rebuilds of a block, for a caller with an end-to-end
    /// checksum that has to work out which disks are wrong (e.g. a disk that
    /// came back stale after an outage, plus a corrupt one).
    async fn reconstruct_candidates(&self, _block: BlockId) -> Result<Candidates, DiskError> {
        Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
    }

    /// The RAID array underneath, for pool-level health operations (labels,
    /// missed-write tracking, scrub). Wrappers forward it.
    fn pool(&self) -> Option<&crate::vdev::Pool> {
        None
    }

    /// Overwrite the primary copy of a block with known-good data.
    async fn repair_block(
        &self,
        _block: BlockId,
        _buf: &[u8; BLOCK_SIZE],
    ) -> Result<(), DiskError> {
        Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
    }
}

/// A shared device (e.g. a pool both a disk server's fencing and its
/// read-only file endpoint look at).
#[async_trait]
impl<T: BlockDevice + ?Sized> BlockDevice for std::sync::Arc<T> {
    async fn flush(&self) -> Result<(), DiskError> {
        (**self).flush().await
    }
    async fn read_block(&self, block: BlockId, buf: &mut Block) -> Result<(), DiskError> {
        (**self).read_block(block, buf).await
    }
    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        (**self).write_block(block, buf).await
    }
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        (**self).capacity_blocks().await
    }
    async fn read_blocks(&self, start: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        (**self).read_blocks(start, buf).await
    }
    async fn write_blocks(&self, start: BlockId, buf: &[Block]) -> Result<(), DiskError> {
        (**self).write_blocks(start, buf).await
    }
    async fn reconstruct_candidates(&self, block: BlockId) -> Result<Candidates, DiskError> {
        (**self).reconstruct_candidates(block).await
    }
    async fn repair_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        (**self).repair_block(block, buf).await
    }
    fn pool(&self) -> Option<&crate::vdev::Pool> {
        (**self).pool()
    }
}

/// Rebuilds of one block. `rebuilt[d]` is the block reconstructed without
/// its own disk (`disk`) and without disk `d`; None if not possible.
/// `rebuilt[disk]` is a rebuild without just its own disk (with single
/// parity or a 2-way mirror, the only kind there is).
#[derive(Debug, Clone, Default)]
pub struct Candidates {
    pub disk: usize,
    pub rebuilt: Vec<Option<Vec<u8>>>,
}

/// Map a websocket reply status to a result.
pub(crate) fn check_status(status: u8) -> Result<(), DiskError> {
    match WsStatus::try_from(status) {
        Ok(WsStatus::Success) => Ok(()),
        Ok(WsStatus::ErrorNotFound) => Err(DiskError::IoError(std::io::ErrorKind::NotFound.into())),
        Ok(WsStatus::InvalidRequest) => {
            Err(DiskError::IoError(std::io::ErrorKind::InvalidInput.into()))
        }
        Ok(WsStatus::Forbidden) => Err(DiskError::IoError(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "this certificate or token may not write",
        ))),
        Ok(WsStatus::StaleEpoch) => Err(DiskError::StaleEpoch),
        Ok(WsStatus::Fenced) => Err(DiskError::Fenced),
        Ok(WsStatus::NotReady) => Err(DiskError::NotReady),
        Ok(WsStatus::Expired) => Err(DiskError::Timeout),
        Err(_) => Err(DiskError::IoError(std::io::Error::other(format!(
            "websocket: unknown status {}",
            status
        )))),
    }
}

/// I/O this process has done against one remote disk (monotonic
/// counters; only kept by the browser backend).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IoStats {
    pub reads: u64,
    pub writes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub errors: u64,
    /// Sum of round-trip times, in microseconds (divide by requests).
    pub rtt_us_total: u64,
    /// The last request's round trip, in microseconds.
    pub rtt_us_last: u64,
}

/// How disks are opened: timeouts and reconnect pacing for remote
/// (websocket) disks, and whether local files are locked.
#[derive(Debug, Clone, Copy)]
pub struct DiskOptions {
    /// Hold an exclusive `flock` on each local disk file for as long as
    /// it's open, so a second process (another `listen`, a CLI tool) can't
    /// write it beside this one (audit D6d). Off by default: tests open the
    /// same files many times in one process, as several nodes.
    pub lock: bool,
    /// A request (send + response) taking longer than this fails with
    /// `Timeout` and the connection is torn down and re-established.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// How often an idle connection is pinged.
    pub ping_interval: Duration,
    /// A connection that has received nothing (not even a pong) for this
    /// long is considered dead.
    pub idle_timeout: Duration,
    /// Reconnect backoff bounds (doubles after every failed attempt).
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl Default for DiskOptions {
    fn default() -> Self {
        DiskOptions {
            lock: false,
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(15),
            reconnect_min: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(30),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[path = "disk_ws.rs"]
mod remote;
#[cfg(target_arch = "wasm32")]
#[path = "disk_wasm.rs"]
mod remote;
pub use remote::RemoteDisk;
#[cfg(not(target_arch = "wasm32"))]
pub use remote::WsConn;
/// The process's TLS client config for remote disks (see `crate::tls`).
#[cfg(not(target_arch = "wasm32"))]
pub mod ws {
    pub use super::remote::{set_tls_client, tls_client, ws_connector};
}
#[cfg(target_arch = "wasm32")]
pub use remote::set_access_token;

#[derive(Debug, Clone)]
enum DiskBackend {
    /// Positional I/O (pread/pwrite) on a blocking thread; no shared cursor,
    /// so concurrent requests don't serialize on a lock.
    #[cfg(not(target_arch = "wasm32"))]
    Local(Arc<std::fs::File>),
    WebSocket(Arc<RemoteDisk>),
    /// A local file behind test-controlled faults (see `disk_fault`).
    #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
    Fault(Arc<fault::FaultDisk>),
}

#[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
#[path = "disk_fault.rs"]
pub mod fault;

#[derive(Debug, Clone)]
pub struct Disk {
    backend: DiskBackend,
}

impl Disk {
    pub fn is_local(&self) -> bool {
        #[cfg(target_arch = "wasm32")]
        return false;
        #[cfg(not(target_arch = "wasm32"))]
        {
            #[cfg(any(test, feature = "testutil"))]
            if matches!(self.backend, DiskBackend::Fault(_)) {
                return true;
            }
            matches!(self.backend, DiskBackend::Local(_))
        }
    }

    /// A disk whose I/O goes through `fault`'s switches (tests only).
    #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
    pub fn faulty(fault: fault::FaultHandle) -> Self {
        Disk {
            backend: DiskBackend::Fault(fault),
        }
    }

    /// Can I/O be attempted right now? (Local disks always; remote disks
    /// while connected.)
    pub fn is_online(&self) -> bool {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(_) => true,
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(f) => f.is_online(),
            DiskBackend::WebSocket(r) => r.is_online(),
        }
    }

    /// This process's I/O against the disk, where it is counted (remote
    /// disks in the browser).
    pub fn io_stats(&self) -> Option<IoStats> {
        match &self.backend {
            #[cfg(target_arch = "wasm32")]
            DiskBackend::WebSocket(r) => Some(r.io_stats()),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Tell a remote disk its index in the pool and its pool client's
    /// fence, so requests carry the right epoch (see `crate::fence`).
    /// No-op for local disks.
    pub fn set_fence(&self, index: usize, fence: std::sync::Arc<crate::fence::ClientFence>) {
        // The browser only reads.
        #[cfg(target_arch = "wasm32")]
        let _ = (index, fence);
        #[cfg(not(target_arch = "wasm32"))]
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(_) => {}
            #[cfg(any(test, feature = "testutil"))]
            DiskBackend::Fault(_) => {}
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::WebSocket(r) => r.set_fence(index, fence),
        }
    }

    /// Changes every time a remote disk reconnects. None for local disks.
    pub fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(_) => None,
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(_) => None,
            DiskBackend::WebSocket(r) => Some(r.subscribe()),
        }
    }

    pub async fn open(path: &str) -> Result<Self, DiskError> {
        Self::open_with(path, DiskOptions::default()).await
    }

    /// Open a local file, or a remote disk given as
    /// `http(s)://host:port/<anything>/<disk index>`. A remote disk whose
    /// server is down still opens (offline) and connects in the background.
    pub async fn open_with(path: &str, opts: DiskOptions) -> Result<Self, DiskError> {
        if path.starts_with("http://") || path.starts_with("https://") {
            let url = url::Url::parse(path)?;
            let scheme = if path.starts_with("https://") {
                "wss"
            } else {
                "ws"
            };
            let host = url.host_str().ok_or(ParseError::EmptyHost)?;
            let port = url.port().map(|p| format!(":{}", p)).unwrap_or_default();
            let ws_url = format!("{}://{}{}/ws", scheme, host, port);

            let disk_id = path
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .ok_or_else(|| {
                    DiskError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("remote disk '{}' must end in a disk index", path),
                    ))
                })?;
            Ok(Disk {
                backend: DiskBackend::WebSocket(RemoteDisk::open(ws_url, disk_id, opts).await),
            })
        } else {
            Self::open_local(path, opts.lock).await
        }
    }

    /// Open an existing local disk file or block device (audit D6d): a
    /// missing one is an error, not a new empty disk (a mistyped path
    /// used to make one), and with `lock` it's held exclusively.
    #[cfg(not(target_arch = "wasm32"))]
    async fn open_local(path: &str, lock: bool) -> Result<Self, DiskError> {
        let path = path.to_string();
        let file = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| std::io::Error::new(e.kind(), format!("opening '{}': {}", path, e)))?;
            if lock {
                use std::os::fd::AsRawFd;
                // Per open file description: held until the last clone of
                // this file is dropped.
                if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                    let e = std::io::Error::last_os_error();
                    let why = match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            "in use by another process (a running `raidish listen` serves it: \
                             reach it through its disk server URL)"
                                .to_string()
                        }
                        _ => e.to_string(),
                    };
                    return Err(std::io::Error::new(
                        e.kind(),
                        format!("locking '{}': {}", path, why),
                    ));
                }
            }
            Ok(file)
        })
        .await??;
        Ok(Disk {
            backend: DiskBackend::Local(Arc::new(file)),
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn open_local(path: &str, _lock: bool) -> Result<Self, DiskError> {
        Err(DiskError::IoError(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("'{}': only remote disks can be opened in the browser", path),
        )))
    }
}

/// pread that zero-fills past end of file.
#[cfg(not(target_arch = "wasm32"))]
fn read_at_zero_fill(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        match file.read_at(&mut buf[done..], offset + done as u64) {
            Ok(0) => {
                buf[done..].fill(0);
                break;
            }
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[async_trait]
impl BlockDevice for Disk {
    async fn flush(&self) -> Result<(), DiskError> {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let span = crate::trace::Span::new("disk.fdatasync");
                tokio::task::spawn_blocking(move || file.sync_data()).await??;
                drop(span);
            }
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(f) => f.flush().await?,
            DiskBackend::WebSocket(r) => {
                r.request(WsOp::Flush, 0, 0, None).await?;
            }
        }
        Ok(())
    }

    async fn read_block(&self, block: BlockId, buf: &mut Block) -> Result<(), DiskError> {
        self.read_blocks(block, std::slice::from_mut(buf)).await
    }

    async fn write_block(&self, block: BlockId, buf: &Block) -> Result<(), DiskError> {
        self.write_blocks(block, std::slice::from_ref(buf)).await
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                // Seeking to the end sizes block devices too (their
                // metadata says 0). I/O is positional: the cursor is unused.
                let len = tokio::task::spawn_blocking(move || {
                    use std::io::Seek;
                    (&*file).seek(std::io::SeekFrom::End(0))
                })
                .await??;
                Ok(len / BLOCK_SIZE as u64)
            }
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(f) => f.capacity(),
            DiskBackend::WebSocket(r) => {
                let data = r.request(WsOp::Info, 0, 0, None).await?;
                let bytes: [u8; 8] = data[..]
                    .try_into()
                    .map_err(|_| DiskError::IoError(std::io::ErrorKind::InvalidData.into()))?;
                Ok(u64::from_le_bytes(bytes))
            }
        }
    }

    async fn read_blocks(&self, start: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        let buf = buf.as_flattened_mut();
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let len = buf.len();
                let offset =
                    byte_offset(start, (len / BLOCK_SIZE) as u64).ok_or_else(bad_offset)?;
                let data = tokio::task::spawn_blocking(move || {
                    let mut data = vec![0u8; len];
                    read_at_zero_fill(&file, &mut data, offset).map(|_| data)
                })
                .await??;
                buf.copy_from_slice(&data);
                Ok(())
            }
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(f) => f.read(start, buf),
            DiskBackend::WebSocket(r) => {
                let chunk = WS_MAX_BLOCKS as usize * BLOCK_SIZE;
                let reads = buf.chunks_mut(chunk).enumerate().map(|(i, out)| {
                    let block = start + (i * WS_MAX_BLOCKS as usize) as u64;
                    let count = (out.len() / BLOCK_SIZE) as u32;
                    async move {
                        let data = r.request(WsOp::Read, block, count, None).await?;
                        if data.len() != out.len() {
                            return Err(DiskError::IoError(std::io::ErrorKind::InvalidData.into()));
                        }
                        out.copy_from_slice(&data);
                        Ok::<(), DiskError>(())
                    }
                });
                try_join_all(reads).await?;
                Ok(())
            }
        }
    }

    async fn write_blocks(&self, start: BlockId, buf: &[Block]) -> Result<(), DiskError> {
        let buf = buf.as_flattened();
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let offset =
                    byte_offset(start, (buf.len() / BLOCK_SIZE) as u64).ok_or_else(bad_offset)?;
                let data = buf.to_vec();
                tokio::task::spawn_blocking(move || file.write_all_at(&data, offset)).await??;
                Ok(())
            }
            #[cfg(all(any(test, feature = "testutil"), not(target_arch = "wasm32")))]
            DiskBackend::Fault(f) => f.write(start, buf).await,
            DiskBackend::WebSocket(r) => {
                let chunk = WS_MAX_BLOCKS as usize * BLOCK_SIZE;
                let writes = buf.chunks(chunk).enumerate().map(|(i, data)| {
                    let block = start + (i * WS_MAX_BLOCKS as usize) as u64;
                    let count = (data.len() / BLOCK_SIZE) as u32;
                    r.request(WsOp::Write, block, count, Some(data))
                });
                try_join_all(writes).await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/disk_tests.rs"]
mod tests;
