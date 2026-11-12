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
pub type FileId = u64;

/// Runs when a disk server says our epoch is stale: another writer has the
/// pool now. `main` makes this exit; it must never be retried.
static ON_STALE_EPOCH: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

/// This process's pool lease epoch, sent with remote requests outside
/// metaslabs lent to it (0: no lease). See `crate::fence`.
pub fn set_lease_epoch(epoch: u64) {
    crate::fence::client().set_pool_epoch(epoch);
}

pub fn lease_epoch() -> u64 {
    crate::fence::client().pool_epoch()
}

pub fn on_stale_epoch(f: fn()) {
    let _ = ON_STALE_EPOCH.set(f);
}

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

#[async_trait]
pub trait BlockDevice: Send + Sync {
    async fn flush(&self) -> Result<(), DiskError>;
    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError>;
    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError>;

    /// Usable capacity in blocks.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
    }

    /// Read `buf.len() / BLOCK_SIZE` consecutive blocks starting at `start`.
    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        debug_assert!(buf.len().is_multiple_of(BLOCK_SIZE));
        for (i, chunk) in buf.chunks_mut(BLOCK_SIZE).enumerate() {
            self.read_block(start + i as u64, chunk).await?;
        }
        Ok(())
    }

    /// Write `buf.len() / BLOCK_SIZE` consecutive blocks starting at `start`.
    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        debug_assert!(buf.len().is_multiple_of(BLOCK_SIZE));
        for (i, chunk) in buf.chunks(BLOCK_SIZE).enumerate() {
            self.write_block(start + i as u64, chunk.try_into().unwrap())
                .await?;
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
    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        (**self).read_block(block, buf).await
    }
    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        (**self).write_block(block, buf).await
    }
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        (**self).capacity_blocks().await
    }
    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        (**self).read_blocks(start, buf).await
    }
    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
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

/// Map a websocket reply status to a result. A stale epoch runs the
/// `on_stale_epoch` hook (another writer has the pool).
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn check_status(status: u8) -> Result<(), DiskError> {
    check_status_in(status, false)
}

/// `check_status` for a request fenced by a lent metaslab's epoch
/// (`lent`): a refusal there means this process's writer lost that
/// metaslab, which the writer handles itself; the hook isn't run.
pub(crate) fn check_status_in(status: u8, lent: bool) -> Result<(), DiskError> {
    match WsStatus::try_from(status) {
        Ok(WsStatus::Success) => Ok(()),
        Ok(WsStatus::ErrorNotFound) => Err(DiskError::IoError(std::io::ErrorKind::NotFound.into())),
        Ok(WsStatus::InvalidRequest) => {
            Err(DiskError::IoError(std::io::ErrorKind::InvalidInput.into()))
        }
        Ok(WsStatus::StaleEpoch) => {
            if !lent && let Some(f) = ON_STALE_EPOCH.get() {
                f();
            }
            Err(DiskError::StaleEpoch)
        }
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

/// Timeouts and reconnect pacing for remote (websocket) disks.
#[derive(Debug, Clone, Copy)]
pub struct DiskOptions {
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

#[derive(Debug, Clone)]
enum DiskBackend {
    /// Positional I/O (pread/pwrite) on a blocking thread; no shared cursor,
    /// so concurrent requests don't serialize on a lock.
    #[cfg(not(target_arch = "wasm32"))]
    Local(Arc<std::fs::File>),
    WebSocket(Arc<RemoteDisk>),
}

#[derive(Debug, Clone)]
pub struct Disk {
    backend: DiskBackend,
}

impl Disk {
    pub fn is_local(&self) -> bool {
        #[cfg(target_arch = "wasm32")]
        return false;
        #[cfg(not(target_arch = "wasm32"))]
        matches!(self.backend, DiskBackend::Local(_))
    }

    /// Can I/O be attempted right now? (Local disks always; remote disks
    /// while connected.)
    pub fn is_online(&self) -> bool {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(_) => true,
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
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::WebSocket(r) => r.set_fence(index, fence),
        }
    }

    /// Changes every time a remote disk reconnects. None for local disks.
    pub fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(_) => None,
            DiskBackend::WebSocket(r) => Some(r.subscribe()),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
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
            Self::open_local(path).await
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn open_local(path: &str) -> Result<Self, DiskError> {
        let path = path.to_string();
        let file = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
        })
        .await??;
        Ok(Disk {
            backend: DiskBackend::Local(Arc::new(file)),
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn open_local(path: &str) -> Result<Self, DiskError> {
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
                tokio::task::spawn_blocking(move || file.sync_data()).await??;
            }
            DiskBackend::WebSocket(r) => {
                r.request(WsOp::Flush, 0, 0, None).await?;
            }
        }
        Ok(())
    }

    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        self.read_blocks(block, &mut buf[..BLOCK_SIZE]).await
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        self.write_blocks(block, buf).await
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let len =
                    tokio::task::spawn_blocking(move || file.metadata().map(|m| m.len())).await??;
                Ok(len / BLOCK_SIZE as u64)
            }
            DiskBackend::WebSocket(r) => {
                let data = r.request(WsOp::Info, 0, 0, None).await?;
                let bytes: [u8; 8] = data[..]
                    .try_into()
                    .map_err(|_| DiskError::IoError(std::io::ErrorKind::InvalidData.into()))?;
                Ok(u64::from_le_bytes(bytes))
            }
        }
    }

    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        debug_assert!(buf.len().is_multiple_of(BLOCK_SIZE));
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let len = buf.len();
                let data = tokio::task::spawn_blocking(move || {
                    let mut data = vec![0u8; len];
                    read_at_zero_fill(&file, &mut data, start * BLOCK_SIZE as u64).map(|_| data)
                })
                .await??;
                buf.copy_from_slice(&data);
                Ok(())
            }
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

    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        debug_assert!(buf.len().is_multiple_of(BLOCK_SIZE));
        match &self.backend {
            #[cfg(not(target_arch = "wasm32"))]
            DiskBackend::Local(file) => {
                let file = file.clone();
                let data = buf.to_vec();
                tokio::task::spawn_blocking(move || {
                    file.write_all_at(&data, start * BLOCK_SIZE as u64)
                })
                .await??;
                Ok(())
            }
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
