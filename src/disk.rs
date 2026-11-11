use crate::websocket::{WS_MAX_BLOCKS, WsOp, WsStatus, decode_response, encode_request};
use dashmap::DashMap;
use futures::future::try_join_all;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::fmt;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::{Bytes, Message};
use url::ParseError;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u64;
pub type FileId = u64;

#[derive(Debug)]
pub enum DiskError {
    IoError(std::io::Error),
    ReqwestError(reqwest::Error),
    TungsteniteError(tungstenite::error::Error),
    UnhealthyDisk,
    InvalidUri(ParseError),
}

impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskError::IoError(err) => write!(f, "I/O error: {}", err),
            DiskError::ReqwestError(err) => write!(f, "HTTP request error: {}", err),
            DiskError::TungsteniteError(err) => write!(f, "WebSocket error: {}", err),
            DiskError::UnhealthyDisk => write!(f, "disk is unhealthy"),
            DiskError::InvalidUri(err) => write!(f, "invalid URI: {}", err),
        }
    }
}

impl From<std::io::Error> for DiskError {
    fn from(error: std::io::Error) -> Self {
        DiskError::IoError(error)
    }
}

impl From<reqwest::Error> for DiskError {
    fn from(error: reqwest::Error) -> Self {
        DiskError::ReqwestError(error)
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

    /// Rebuild a block from redundancy *without* reading its primary copy.
    /// Used when a block reads fine but fails its checksum.
    async fn reconstruct_block(&self, _block: BlockId, _buf: &mut [u8]) -> Result<(), DiskError> {
        Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
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

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = DashMap<u64, oneshot::Sender<(u8, Bytes)>>;

/// A multiplexed websocket connection: many requests may be in flight; a
/// reader task routes each response to its caller by request id.
pub struct WsConn {
    sink: Mutex<WsSink>,
    pending: Arc<Pending>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
}

impl fmt::Debug for WsConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsConn")
            .field("in_flight", &self.pending.len())
            .finish()
    }
}

impl WsConn {
    async fn connect(url: &str) -> Result<Arc<Self>, DiskError> {
        let (ws, _) = connect_async(url).await?;
        let (sink, mut stream) = ws.split();
        let pending: Arc<Pending> = Arc::new(DashMap::new());
        let closed = Arc::new(AtomicBool::new(false));

        let (p, c) = (pending.clone(), closed.clone());
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(Message::Binary(data)) => match decode_response(&data) {
                        Some((status, id)) => {
                            if let Some((_, tx)) = p.remove(&id) {
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
            // Fail everything still waiting.
            c.store(true, Ordering::SeqCst);
            p.clear();
        });

        Ok(Arc::new(WsConn {
            sink: Mutex::new(sink),
            pending,
            next_id: AtomicU64::new(1),
            closed,
        }))
    }

    async fn request(
        &self,
        op: WsOp,
        disk_id: u16,
        block: u64,
        count: u32,
        data: Option<&[u8]>,
    ) -> Result<Bytes, DiskError> {
        let closed = || DiskError::TungsteniteError(tungstenite::error::Error::ConnectionClosed);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        if self.closed.load(Ordering::SeqCst) {
            self.pending.remove(&id);
            return Err(closed());
        }

        let msg = encode_request(op, disk_id, block, count, id, data);
        if let Err(e) = self
            .sink
            .lock()
            .await
            .send(Message::Binary(msg.into()))
            .await
        {
            self.pending.remove(&id);
            return Err(e.into());
        }

        let (status, payload) = rx.await.map_err(|_| closed())?;
        match WsStatus::try_from(status) {
            Ok(WsStatus::Success) => Ok(payload),
            Ok(WsStatus::ErrorNotFound) => {
                Err(DiskError::IoError(std::io::ErrorKind::NotFound.into()))
            }
            Ok(WsStatus::InvalidRequest) => {
                Err(DiskError::IoError(std::io::ErrorKind::InvalidInput.into()))
            }
            Err(_) => Err(DiskError::IoError(std::io::Error::other(format!(
                "websocket: unknown status {}",
                status
            )))),
        }
    }
}

#[derive(Debug, Clone)]
enum DiskBackend {
    /// Positional I/O (pread/pwrite) on a blocking thread; no shared cursor,
    /// so concurrent requests don't serialize on a lock.
    Local(Arc<std::fs::File>),
    Remote {
        base_url: String,
        disk_id: String,
        client: Client,
    },
    WebSocket {
        disk_id: u16,
        conn: Arc<WsConn>,
    },
}

#[derive(Debug, Clone)]
pub struct Disk {
    backend: DiskBackend,
}

impl Disk {
    pub fn is_local(&self) -> bool {
        matches!(self.backend, DiskBackend::Local(_))
    }

    pub async fn open(path: &str) -> Result<Self, DiskError> {
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

            let parts: Vec<&str> = path.rsplitn(2, '/').collect();
            let disk_id_str = parts[0];
            let base_url = parts[1].to_string();

            // Prefer the websocket transport; fall back to plain HTTP.
            if let Ok(disk_id) = disk_id_str.parse::<u16>()
                && let Ok(conn) = WsConn::connect(&ws_url).await
            {
                return Ok(Disk {
                    backend: DiskBackend::WebSocket { disk_id, conn },
                });
            }

            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()?;

            Ok(Disk {
                backend: DiskBackend::Remote {
                    base_url,
                    disk_id: disk_id_str.to_string(),
                    client,
                },
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
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/flush", base_url, disk_id);
                client.post(&url).send().await?;
            }
            DiskBackend::WebSocket { disk_id, conn } => {
                conn.request(WsOp::Flush, *disk_id, 0, 0, None).await?;
            }
        }
        Ok(())
    }

    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        match &self.backend {
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                let response = client.get(&url).send().await?;
                if response.status().is_success() {
                    let bytes = response.bytes().await?;
                    if bytes.len() == BLOCK_SIZE {
                        buf[..BLOCK_SIZE].copy_from_slice(&bytes);
                    } else if bytes.is_empty() {
                        buf[..BLOCK_SIZE].fill(0);
                    } else {
                        return Err(DiskError::IoError(std::io::ErrorKind::InvalidData.into()));
                    }
                } else {
                    // Treat errors as empty blocks
                    buf[..BLOCK_SIZE].fill(0);
                }
                Ok(())
            }
            _ => self.read_blocks(block, &mut buf[..BLOCK_SIZE]).await,
        }
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        match &self.backend {
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                let response = client.put(&url).body(buf.to_vec()).send().await?;
                if !response.status().is_success() {
                    return Err(DiskError::IoError(std::io::Error::other(format!(
                        "Remote disk write error: {}",
                        response.status()
                    ))));
                }
                Ok(())
            }
            _ => self.write_blocks(block, buf).await,
        }
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        match &self.backend {
            DiskBackend::Local(file) => {
                let file = file.clone();
                let len =
                    tokio::task::spawn_blocking(move || file.metadata().map(|m| m.len())).await??;
                Ok(len / BLOCK_SIZE as u64)
            }
            DiskBackend::WebSocket { disk_id, conn } => {
                let data = conn.request(WsOp::Info, *disk_id, 0, 0, None).await?;
                let bytes: [u8; 8] = data[..]
                    .try_into()
                    .map_err(|_| DiskError::IoError(std::io::ErrorKind::InvalidData.into()))?;
                Ok(u64::from_le_bytes(bytes))
            }
            DiskBackend::Remote { .. } => {
                Err(DiskError::IoError(std::io::ErrorKind::Unsupported.into()))
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
            DiskBackend::WebSocket { disk_id, conn } => {
                let chunk = WS_MAX_BLOCKS as usize * BLOCK_SIZE;
                let reads = buf.chunks_mut(chunk).enumerate().map(|(i, out)| {
                    let block = start + (i * WS_MAX_BLOCKS as usize) as u64;
                    let count = (out.len() / BLOCK_SIZE) as u32;
                    async move {
                        let data = conn
                            .request(WsOp::Read, *disk_id, block, count, None)
                            .await?;
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
            DiskBackend::Remote { .. } => {
                for (i, out) in buf.chunks_mut(BLOCK_SIZE).enumerate() {
                    self.read_block(start + i as u64, out).await?;
                }
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
            DiskBackend::WebSocket { disk_id, conn } => {
                let chunk = WS_MAX_BLOCKS as usize * BLOCK_SIZE;
                let writes = buf.chunks(chunk).enumerate().map(|(i, data)| {
                    let block = start + (i * WS_MAX_BLOCKS as usize) as u64;
                    let count = (data.len() / BLOCK_SIZE) as u32;
                    conn.request(WsOp::Write, *disk_id, block, count, Some(data))
                });
                try_join_all(writes).await?;
                Ok(())
            }
            DiskBackend::Remote { .. } => {
                for (i, data) in buf.chunks(BLOCK_SIZE).enumerate() {
                    self.write_block(start + i as u64, data.try_into().unwrap())
                        .await?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/disk_tests.rs"]
mod tests;
