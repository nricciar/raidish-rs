use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::Message;
use url::ParseError;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u64;
pub type FileId = u64;

thread_local! {
    static WS_READ_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(11));
    static WS_WRITE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(11 + BLOCK_SIZE));
    static WS_FLUSH_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(11));
}

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

#[async_trait::async_trait]
pub trait BlockDevice: Send + Sync {
    async fn flush(&self) -> Result<(), DiskError>;
    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError>;
    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError>;
}

#[derive(Debug, Clone)]
enum DiskBackend {
    Local(Arc<Mutex<File>>),
    Remote {
        base_url: String,
        disk_id: String,
        client: Client,
    },
    WebSocket {
        disk_id: u16,
        ws: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
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
            // Try WebSocket connection first
            let url = url::Url::parse(path)?;
            let scheme = if path.starts_with("https://") {
                "wss"
            } else {
                "ws"
            };
            let host = url.host_str().ok_or(ParseError::EmptyHost)?;
            let port = url.port().map(|p| format!(":{}", p)).unwrap_or_default();

            // Construct WebSocket URL
            let ws_url = format!("{}://{}{}/ws", scheme, host, port);

            let parts: Vec<&str> = path.rsplitn(2, '/').collect();
            let disk_id = parts[0].to_string();
            let base_url = parts[1].to_string();
            let disk_id_str = parts[0];

            // Try to establish WebSocket connection
            if let Ok((ws_stream, _)) = connect_async(&ws_url).await {
                // Parse disk_id as u16
                if let Ok(disk_id) = disk_id_str.parse::<u16>() {
                    return Ok(Disk {
                        backend: DiskBackend::WebSocket {
                            disk_id,
                            ws: Arc::new(Mutex::new(ws_stream)),
                        },
                    });
                }
            }

            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()?;

            Ok(Disk {
                backend: DiskBackend::Remote {
                    base_url,
                    disk_id,
                    client,
                },
            })
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                //.custom_flags(libc::O_DIRECT | libc::O_SYNC) // On Linux
                .open(path)
                .await?;
            Ok(Disk {
                backend: DiskBackend::Local(Arc::new(Mutex::new(file))),
            })
        }
    }
}

#[async_trait]
impl BlockDevice for Disk {
    async fn flush(&self) -> Result<(), DiskError> {
        match &self.backend {
            DiskBackend::Local(file) => {
                let mut file = file.lock().await;
                file.flush().await?;
            }
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/flush", base_url, disk_id);
                match client.post(&url).send().await {
                    Ok(_response) => (),
                    Err(e) => return Err(DiskError::ReqwestError(e)),
                }
            }
            DiskBackend::WebSocket { disk_id, ws } => {
                // Build read request: [op:1][disk_index:2][block_id:8]
                let request = WS_FLUSH_BUFFER.with(|buf| {
                    let mut request = buf.borrow_mut();
                    request.clear();
                    request.push(2); // op=2 for flush
                    request.extend_from_slice(&disk_id.to_le_bytes());
                    request.extend_from_slice(&(0_u64).to_le_bytes());
                    request.clone()
                });

                let mut ws_lock = ws.lock().await;

                // Send request
                ws_lock.send(Message::Binary(request.into())).await?;

                // Receive response
                match ws_lock.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        if data.is_empty() {
                            return Err(DiskError::IoError(
                                std::io::ErrorKind::UnexpectedEof.into(),
                            ));
                        }

                        let status = data[0];
                        match status {
                            0 => (), // Success
                            1 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::NotFound.into(),
                                ));
                            }
                            2 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::InvalidInput.into(),
                                ));
                            }
                            _ => {
                                return Err(DiskError::IoError(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("WebSocket read error: unknown status {}", status),
                                )));
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        return Err(DiskError::IoError(std::io::ErrorKind::InvalidData.into()));
                    }
                    Some(Err(e)) => return Err(DiskError::TungsteniteError(e)),
                    None => {
                        return Err(DiskError::TungsteniteError(
                            tungstenite::error::Error::ConnectionClosed,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        match &self.backend {
            DiskBackend::Local(file) => {
                let mut file = file.lock().await;
                file.seek(SeekFrom::Start(block as u64 * BLOCK_SIZE as u64))
                    .await?;
                match file.read_exact(buf).await {
                    Ok(_) => {}
                    // TODO: is this actually a good idea?
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        buf.fill(0);
                    }
                    Err(e) => return Err(DiskError::IoError(e)),
                }
            }
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                match client.get(&url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            let bytes = response.bytes().await?;
                            if bytes.len() == BLOCK_SIZE {
                                buf.copy_from_slice(&bytes);
                            } else if bytes.is_empty() {
                                buf.fill(0);
                            } else {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::InvalidData.into(),
                                ));
                            }
                        } else {
                            // Treat errors as empty blocks
                            buf.fill(0);
                        }
                    }
                    Err(e) => return Err(DiskError::ReqwestError(e)),
                }
            }
            DiskBackend::WebSocket { disk_id, ws } => {
                // Build read request: [op:1][disk_index:2][block_id:8]
                let request = WS_READ_BUFFER.with(|buf| {
                    let mut request = buf.borrow_mut();
                    request.clear();
                    request.push(0); // op=0 for read
                    request.extend_from_slice(&disk_id.to_le_bytes());
                    request.extend_from_slice(&block.to_le_bytes());
                    request.clone()
                });

                let mut ws_lock = ws.lock().await;

                // Send request
                ws_lock.send(Message::Binary(request.into())).await?;

                // Receive response
                match ws_lock.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        if data.is_empty() {
                            return Err(DiskError::IoError(
                                std::io::ErrorKind::UnexpectedEof.into(),
                            ));
                        }

                        let status = data[0];
                        match status {
                            0 => {
                                // Success
                                if data.len() == 1 + BLOCK_SIZE {
                                    buf.copy_from_slice(&data[1..]);
                                } else {
                                    return Err(DiskError::IoError(
                                        std::io::ErrorKind::InvalidData.into(),
                                    ));
                                }
                            }
                            1 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::NotFound.into(),
                                ));
                            }
                            2 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::InvalidInput.into(),
                                ));
                            }
                            _ => {
                                return Err(DiskError::IoError(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("WebSocket read error: unknown status {}", status),
                                )));
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        return Err(DiskError::IoError(std::io::ErrorKind::InvalidData.into()));
                    }
                    Some(Err(e)) => return Err(DiskError::TungsteniteError(e)),
                    None => {
                        return Err(DiskError::TungsteniteError(
                            tungstenite::error::Error::ConnectionClosed,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        match &self.backend {
            DiskBackend::Local(file) => {
                let mut file = file.lock().await;
                file.seek(SeekFrom::Start(block as u64 * BLOCK_SIZE as u64))
                    .await?;
                file.write_all(buf).await?;
            }
            DiskBackend::Remote {
                base_url,
                disk_id,
                client,
            } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                let response = client.put(&url).body(buf.to_vec()).send().await?;

                if !response.status().is_success() {
                    return Err(DiskError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Remote disk write error: {}", response.status()),
                    )));
                }
            }
            DiskBackend::WebSocket { disk_id, ws } => {
                // Build write request: [op:1][disk_index:2][block_id:8][data:4096]
                let request = WS_WRITE_BUFFER.with(|buffer| {
                    let mut request = buffer.borrow_mut();
                    request.clear();
                    request.push(1); // op=1 for write
                    request.extend_from_slice(&disk_id.to_le_bytes());
                    request.extend_from_slice(&block.to_le_bytes());
                    request.extend_from_slice(buf);
                    request.clone()
                });

                let mut ws_lock = ws.lock().await;

                // Send request
                ws_lock.send(Message::Binary(request.into())).await?;

                // Receive response
                match ws_lock.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        if data.is_empty() {
                            return Err(DiskError::IoError(
                                std::io::ErrorKind::UnexpectedEof.into(),
                            ));
                        }

                        let status = data[0];
                        match status {
                            0 => {} // Success
                            1 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::NotFound.into(),
                                ));
                            }
                            2 => {
                                return Err(DiskError::IoError(
                                    std::io::ErrorKind::InvalidInput.into(),
                                ));
                            }
                            _ => {
                                return Err(DiskError::IoError(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("WebSocket write error: unknown status {}", status),
                                )));
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        return Err(DiskError::IoError(std::io::ErrorKind::InvalidData.into()));
                    }
                    Some(Err(e)) => return Err(DiskError::TungsteniteError(e)),
                    None => {
                        return Err(DiskError::TungsteniteError(
                            tungstenite::error::Error::ConnectionClosed,
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
