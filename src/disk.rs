use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use reqwest::Client;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use futures_util::{SinkExt, StreamExt};
use tungstenite::Message;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u64;
pub type FileId = u64;

#[derive(Debug)]
enum DiskBackend {
    Local(File),
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

#[derive(Debug)]
pub struct Disk {
    backend: DiskBackend,
}

impl Disk {
    pub fn is_local(&self) -> bool {
        matches!(self.backend, DiskBackend::Local(_))
    }

    pub async fn open(path: &str) -> Self {
        if path.starts_with("http://") || path.starts_with("https://") {
            // Try WebSocket connection first
            let url = url::Url::parse(path).expect("Invalid URL");
            let scheme = if path.starts_with("https://") { "wss" } else { "ws" };
            let host = url.host_str().expect("No host in URL");
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
                    return Disk {
                        backend: DiskBackend::WebSocket {
                            disk_id,
                            ws: Arc::new(Mutex::new(ws_stream)),
                        }
                    };
                }
            }

            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap();
            
            Disk {
                backend: DiskBackend::Remote {
                    base_url,
                    disk_id,
                    client,
                }
            }
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                //.custom_flags(libc::O_DIRECT | libc::O_SYNC) // On Linux
                .open(path)
                .unwrap();
            Disk { backend: DiskBackend::Local(file) }
        }
    }

    pub fn flush(&mut self) {

    }

    pub async fn read_block(&mut self, block: BlockId, buf: &mut [u8]) {
        match &mut self.backend {
            DiskBackend::Local(file) => {
                file
                    .seek(SeekFrom::Start(block as u64 * BLOCK_SIZE as u64))
                    .unwrap();
                match file.read_exact(buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        buf.fill(0);
                    }
                    Err(e) => panic!("disk read error: {:?}", e),
                }
            },
            DiskBackend::Remote { base_url, disk_id, client } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                match client.get(&url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            let bytes = response.bytes().await.unwrap();
                            if bytes.len() == BLOCK_SIZE {
                                buf.copy_from_slice(&bytes);
                            } else if bytes.is_empty() {
                                buf.fill(0);
                            } else {
                                panic!("Invalid block size from remote: {}", bytes.len());
                            }
                        } else {
                            // Treat errors as empty blocks
                            buf.fill(0);
                        }
                    }
                    Err(e) => panic!("Remote disk read error: {:?}", e),
                }
            },
            DiskBackend::WebSocket { disk_id, ws } => {
                // Build read request: [op:1][disk_index:2][block_id:8]
                let mut request = Vec::with_capacity(11);
                request.push(0); // op=0 for read
                request.extend_from_slice(&disk_id.to_le_bytes());
                request.extend_from_slice(&block.to_le_bytes());
                
                let mut ws_lock = ws.lock().await;
                
                // Send request
                if let Err(e) = ws_lock.send(Message::Binary(request.into())).await {
                    panic!("WebSocket send error: {:?}", e);
                }
                
                // Receive response
                match ws_lock.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        if data.is_empty() {
                            panic!("Empty WebSocket response");
                        }
                        
                        let status = data[0];
                        match status {
                            0 => {
                                // Success
                                if data.len() == 1 + BLOCK_SIZE {
                                    buf.copy_from_slice(&data[1..]);
                                } else {
                                    panic!("Invalid block size from WebSocket: {}", data.len() - 1);
                                }
                            },
                            1 => panic!("WebSocket read error: not found or not local"),
                            2 => panic!("WebSocket read error: invalid request"),
                            _ => panic!("WebSocket read error: unknown status {}", status),
                        }
                    },
                    Some(Ok(_)) => panic!("Unexpected WebSocket message type"),
                    Some(Err(e)) => panic!("WebSocket receive error: {:?}", e),
                    None => panic!("WebSocket connection closed"),
                }
            }
        }
    }

    pub async fn write_block(&mut self, block: BlockId, buf: &[u8]) {
        match &mut self.backend {
            DiskBackend::Local(file) => {
                file
                    .seek(SeekFrom::Start(block as u64 * BLOCK_SIZE as u64))
                    .unwrap();
                file.write_all(buf).unwrap();
            },
            DiskBackend::Remote { base_url, disk_id, client } => {
                let url = format!("{}/{}/blocks/{}", base_url, disk_id, block);
                let response = client.put(&url)
                    .body(buf.to_vec())
                    .send().await
                    .unwrap();
                
                if !response.status().is_success() {
                    panic!("Remote disk write error: {}", response.status());
                }
            },
            DiskBackend::WebSocket { disk_id, ws } => {
                // Build write request: [op:1][disk_index:2][block_id:8][data:4096]
                let mut request = Vec::with_capacity(11 + BLOCK_SIZE);
                request.push(1); // op=1 for write
                request.extend_from_slice(&disk_id.to_le_bytes());
                request.extend_from_slice(&block.to_le_bytes());
                request.extend_from_slice(buf);
                
                let mut ws_lock = ws.lock().await;
                
                // Send request
                if let Err(e) = ws_lock.send(Message::Binary(request.into())).await {
                    panic!("WebSocket send error: {:?}", e);
                }
                
                // Receive response
                match ws_lock.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        if data.is_empty() {
                            panic!("Empty WebSocket response");
                        }
                        
                        let status = data[0];
                        match status {
                            0 => {}, // Success
                            1 => panic!("WebSocket write error: not found or not local"),
                            2 => panic!("WebSocket write error: invalid request"),
                            _ => panic!("WebSocket write error: unknown status {}", status),
                        }
                    },
                    Some(Ok(_)) => panic!("Unexpected WebSocket message type"),
                    Some(Err(e)) => panic!("WebSocket receive error: {:?}", e),
                    None => panic!("WebSocket connection closed"),
                }
            }
        }
    }
}