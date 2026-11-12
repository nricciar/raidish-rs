//! Remote disks over tokio-tungstenite (native). The browser build has its
//! own `RemoteDisk` in `disk_wasm.rs` with the same interface.

use super::*;
use crate::websocket::{WsOp, decode_response, encode_request};
use dashmap::DashMap;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
pub use tungstenite::Bytes;
use tungstenite::Message;

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = DashMap<u64, oneshot::Sender<(u8, Bytes)>>;

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
        check_status(status)?;
        Ok(payload)
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
    pub(super) async fn open(url: String, disk_id: u16, opts: DiskOptions) -> Arc<Self> {
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

    pub(super) fn is_online(&self) -> bool {
        self.current().is_some()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub(super) async fn request(
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
