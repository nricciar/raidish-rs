use crate::websocket::{WS_MAX_BLOCKS, WsOp, WsStatus, decode_response, encode_request};
use dashmap::DashMap;
use futures::future::try_join_all;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::{Bytes, Message};
use url::ParseError;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u64;
pub type FileId = u64;

/// This process's pool lease epoch, sent with every remote request (0: no
/// lease). See `crate::cluster`.
static LEASE_EPOCH: AtomicU64 = AtomicU64::new(0);
/// Runs when a disk server says our epoch is stale: another writer has the
/// pool now. `main` makes this exit; it must never be retried.
static ON_STALE_EPOCH: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

pub fn set_lease_epoch(epoch: u64) {
    LEASE_EPOCH.store(epoch, Ordering::SeqCst);
}

pub fn lease_epoch() -> u64 {
    LEASE_EPOCH.load(Ordering::SeqCst)
}

pub fn on_stale_epoch(f: fn()) {
    let _ = ON_STALE_EPOCH.set(f);
}

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
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

impl From<tokio::task::JoinError> for DiskError {
    fn from(error: tokio::task::JoinError) -> Self {
        DiskError::IoError(std::io::Error::other(error))
    }
}

#[async_trait::async_trait]
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

/// Rebuilds of one block. `rebuilt[d]` is the block reconstructed without
/// its own disk (`disk`) and without disk `d`; None if not possible.
/// `rebuilt[disk]` is a rebuild without just its own disk (with single
/// parity or a 2-way mirror, the only kind there is).
#[derive(Debug, Clone, Default)]
pub struct Candidates {
    pub disk: usize,
    pub rebuilt: Vec<Option<Vec<u8>>>,
}

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = DashMap<u64, oneshot::Sender<(u8, Bytes)>>;

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

/// State shared between a connection and its reader / keepalive tasks.
struct ConnState {
    pending: Pending,
    closed: AtomicBool,
    /// Flips to true once, when the connection dies.
    closed_tx: watch::Sender<bool>,
    /// Milliseconds since `epoch` at which the last frame arrived.
    last_rx: AtomicU64,
    epoch: Instant,
}

impl ConnState {
    fn touch(&self) {
        self.last_rx
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn idle_for(&self) -> Duration {
        let now = self.epoch.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_rx.load(Ordering::Relaxed)))
    }

    /// Mark the connection dead and fail every waiter.
    fn shut(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.closed_tx.send_replace(true);
        self.pending.clear();
    }
}

/// A multiplexed websocket connection: many requests may be in flight; a
/// reader task routes each response to its caller by request id. Every
/// request is bounded by `request_timeout`, and a keepalive task pings the
/// server and declares the connection dead after `idle_timeout` of silence.
/// A dead connection is never revived; `RemoteDisk` replaces it.
pub struct WsConn {
    sink: Mutex<WsSink>,
    state: Arc<ConnState>,
    next_id: AtomicU64,
    opts: DiskOptions,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl fmt::Debug for WsConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsConn")
            .field("in_flight", &self.state.pending.len())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl Drop for WsConn {
    fn drop(&mut self) {
        self.state.shut();
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl WsConn {
    async fn connect(url: &str, opts: DiskOptions) -> Result<Arc<Self>, DiskError> {
        let (ws, _) = tokio::time::timeout(opts.connect_timeout, connect_async(url))
            .await
            .map_err(|_| DiskError::Timeout)??;
        let (sink, mut stream) = ws.split();
        let state = Arc::new(ConnState {
            pending: DashMap::new(),
            closed: AtomicBool::new(false),
            closed_tx: watch::channel(false).0,
            last_rx: AtomicU64::new(0),
            epoch: Instant::now(),
        });

        let st = state.clone();
        let reader = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                st.touch();
                match msg {
                    Ok(Message::Binary(data)) => match decode_response(&data) {
                        Some((status, id)) => {
                            if let Some((_, tx)) = st.pending.remove(&id) {
                                let _ = tx.send((
                                    status,
                                    data.slice(crate::websocket::WS_RESPONSE_HEADER_LEN..),
                                ));
                            }
                        }
                        None => eprintln!("websocket: malformed response"),
                    },
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            st.shut();
        });

        let conn = Arc::new_cyclic(|weak: &std::sync::Weak<WsConn>| {
            let weak = weak.clone();
            let keepalive = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(opts.ping_interval).await;
                    let Some(conn) = weak.upgrade() else { return };
                    if conn.is_closed() {
                        return;
                    }
                    if conn.state.idle_for() > opts.idle_timeout {
                        eprintln!(
                            "websocket: no traffic for {:?}; dropping connection",
                            opts.idle_timeout
                        );
                        conn.state.shut();
                        return;
                    }
                    let ping = async {
                        conn.sink
                            .lock()
                            .await
                            .send(Message::Ping(Bytes::new()))
                            .await
                    };
                    if !matches!(
                        tokio::time::timeout(opts.request_timeout, ping).await,
                        Ok(Ok(()))
                    ) {
                        conn.state.shut();
                        return;
                    }
                }
            });
            WsConn {
                sink: Mutex::new(sink),
                state,
                next_id: AtomicU64::new(1),
                opts,
                tasks: vec![reader, keepalive],
            }
        });
        Ok(conn)
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::SeqCst)
    }

    fn closed_signal(&self) -> watch::Receiver<bool> {
        self.state.closed_tx.subscribe()
    }

    async fn request(
        &self,
        op: WsOp,
        disk_id: u16,
        block: u64,
        count: u32,
        data: Option<&[u8]>,
    ) -> Result<Bytes, DiskError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.state.pending.insert(id, tx);
        if self.is_closed() {
            self.state.pending.remove(&id);
            return Err(DiskError::Disconnected);
        }

        // One deadline covers queueing for the sink, sending and the reply.
        let msg = encode_request(op, disk_id, block, count, id, lease_epoch(), data);
        let exchange = async {
            self.sink
                .lock()
                .await
                .send(Message::Binary(msg.into()))
                .await
                .map_err(DiskError::from)?;
            rx.await.map_err(|_| DiskError::Disconnected)
        };
        let (status, payload) =
            match tokio::time::timeout(self.opts.request_timeout, exchange).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(e)) => {
                    self.state.pending.remove(&id);
                    if !matches!(e, DiskError::Disconnected) {
                        self.state.shut();
                    }
                    return Err(e);
                }
                Err(_) => {
                    // A server that stops answering is treated as gone: fail
                    // everything in flight and let the owner reconnect.
                    self.state.pending.remove(&id);
                    self.state.shut();
                    return Err(DiskError::Timeout);
                }
            };
        match WsStatus::try_from(status) {
            Ok(WsStatus::Success) => Ok(payload),
            Ok(WsStatus::ErrorNotFound) => {
                Err(DiskError::IoError(std::io::ErrorKind::NotFound.into()))
            }
            Ok(WsStatus::InvalidRequest) => {
                Err(DiskError::IoError(std::io::ErrorKind::InvalidInput.into()))
            }
            Ok(WsStatus::StaleEpoch) => {
                if let Some(f) = ON_STALE_EPOCH.get() {
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
}

/// A disk served by a remote `listen` node. Owns at most one live
/// connection; when it dies, requests fail fast with `Disconnected` (RAID
/// parity covers the gap) while a background task reconnects with
/// exponential backoff. Failed requests are never retried here: the pool
/// records the missed writes and resilvers them instead.
pub struct RemoteDisk {
    url: String,
    disk_id: u16,
    opts: DiskOptions,
    conn: std::sync::RwLock<Option<Arc<WsConn>>>,
    reconnecting: AtomicBool,
    /// Bumped every time a connection is (re-)established after a failure.
    generation: watch::Sender<u64>,
}

impl fmt::Debug for RemoteDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteDisk")
            .field("url", &self.url)
            .field("disk_id", &self.disk_id)
            .field("online", &self.current().is_some())
            .finish()
    }
}

impl RemoteDisk {
    async fn open(url: String, disk_id: u16, opts: DiskOptions) -> Arc<Self> {
        let disk = Arc::new(RemoteDisk {
            url,
            disk_id,
            opts,
            conn: std::sync::RwLock::new(None),
            reconnecting: AtomicBool::new(false),
            generation: watch::channel(0).0,
        });
        match WsConn::connect(&disk.url, opts).await {
            Ok(conn) => disk.install(conn),
            Err(e) => {
                eprintln!("disk {} ({}): offline: {}", disk_id, disk.url, e);
                disk.ensure_reconnect();
            }
        }
        disk
    }

    fn current(&self) -> Option<Arc<WsConn>> {
        self.conn
            .read()
            .unwrap()
            .as_ref()
            .filter(|c| !c.is_closed())
            .cloned()
    }

    /// Make `conn` the live connection and start reconnecting as soon as it
    /// dies, whether or not anyone is issuing requests at the time.
    fn install(self: &Arc<Self>, conn: Arc<WsConn>) {
        let mut closed = conn.closed_signal();
        *self.conn.write().unwrap() = Some(conn);
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let _ = closed.wait_for(|c| *c).await;
            if let Some(disk) = weak.upgrade() {
                eprintln!("disk {} ({}): connection lost", disk.disk_id, disk.url);
                disk.ensure_reconnect();
            }
        });
    }

    fn ensure_reconnect(self: &Arc<Self>) {
        if self.reconnecting.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        let (min, max) = (self.opts.reconnect_min, self.opts.reconnect_max);
        tokio::spawn(async move {
            let mut delay = min;
            loop {
                tokio::time::sleep(delay).await;
                let Some(disk) = weak.upgrade() else { return };
                if let Ok(conn) = WsConn::connect(&disk.url, disk.opts).await {
                    disk.reconnecting.store(false, Ordering::SeqCst);
                    disk.install(conn);
                    disk.generation.send_modify(|g| *g += 1);
                    eprintln!("disk {} ({}): reconnected", disk.disk_id, disk.url);
                    return;
                }
                delay = (delay * 2).min(max);
            }
        });
    }

    async fn request(
        self: &Arc<Self>,
        op: WsOp,
        block: u64,
        count: u32,
        data: Option<&[u8]>,
    ) -> Result<Bytes, DiskError> {
        let Some(conn) = self.current() else {
            self.ensure_reconnect();
            return Err(DiskError::Disconnected);
        };
        let result = conn.request(op, self.disk_id, block, count, data).await;
        if conn.is_closed() {
            self.ensure_reconnect();
        }
        result
    }
}

#[derive(Debug, Clone)]
enum DiskBackend {
    /// Positional I/O (pread/pwrite) on a blocking thread; no shared cursor,
    /// so concurrent requests don't serialize on a lock.
    Local(Arc<std::fs::File>),
    WebSocket(Arc<RemoteDisk>),
}

#[derive(Debug, Clone)]
pub struct Disk {
    backend: DiskBackend,
}

impl Disk {
    pub fn is_local(&self) -> bool {
        matches!(self.backend, DiskBackend::Local(_))
    }

    /// Can I/O be attempted right now? (Local disks always; remote disks
    /// while connected.)
    pub fn is_online(&self) -> bool {
        match &self.backend {
            DiskBackend::Local(_) => true,
            DiskBackend::WebSocket(r) => r.current().is_some(),
        }
    }

    /// Changes every time a remote disk reconnects. None for local disks.
    pub fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        match &self.backend {
            DiskBackend::Local(_) => None,
            DiskBackend::WebSocket(r) => Some(r.generation.subscribe()),
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
    }
}

/// pread that zero-fills past end of file.
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
