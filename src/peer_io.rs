//! Peer I/O: a node's export proxied to the node that writes the volume.
//!
//! A volume attached on several nodes has one writer, the holder of its
//! `ds/<name>` lease; the others export it too, but send each request to
//! the writer ([`PeerIo`]) instead of opening the dataset
//! (`docs/design/mig-1-volume-handoff.md`).
//!
//! The wire: mutual TLS with the cluster's CA, node certificates only. The
//! proxy sends one line, `RAIDISH-VOL/1 <volume> <epoch>`, naming the lease
//! epoch it believes the writer holds; the writer answers `OK <size>` if
//! it does (and serves the volume), else `ERR <why>` and hangs up. Then it's
//! the NBD transmission phase, exactly as an NBD client speaks it (see
//! `crate::nbd::serve_requests`): concurrent requests matched by handle.
//!
//! Requests the proxy can't get answered (the connection dropped, the
//! writer moved or refused) are sent again, to wherever the writer is by
//! then: block writes are idempotent, so one that may have landed already
//! is safe to repeat.
//!
//! The preambles carry this protocol's version (`/1`); a node answers one
//! of another version with `ERR`, naming its own.

use crate::fs::FileSystemError;
use crate::nbd::{
    EIO, ENOSPC, ExportSession, NBD_CMD_FLUSH, NBD_CMD_READ, NBD_CMD_WRITE, NBD_CMD_WRITE_ZEROES,
    NBD_REPLY_MAGIC, NBD_REQUEST_MAGIC,
};
use crate::pki::Role;
use crate::types::Epoch;
use crate::volume_io::{SwitchableIo, VolumeIo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// The peer I/O port, unless configured otherwise.
pub const DEFAULT_PORT: u16 = 10810;

/// A proxy's preamble, with this protocol's version. Bump the version
/// (here and in `HANDOFF`) with any change to what follows it.
const PREAMBLE: &str = "RAIDISH-VOL/1";
/// What a writer handing a volume over starts with:
/// `RAIDISH-HANDOFF/1 <volume> <epoch> <transport>`.
pub const HANDOFF: &str = "RAIDISH-HANDOFF/1";
/// The longest preamble or answer line accepted.
const MAX_LINE: usize = 4096;
/// How long to wait for a connection, its TLS handshake and its answer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// What the writer serves a proxy: the export's size, its backend, and a
/// hold on it (removing the export waits for the proxy's connection to
/// finish its requests, like any client's).
pub struct PeerExport {
    pub size: u64,
    pub io: Arc<SwitchableIo>,
    pub session: ExportSession,
}

/// A peer connection's reading side, after its first line.
pub type PeerLines = BufReader<tokio::io::ReadHalf<tokio_rustls::server::TlsStream<TcpStream>>>;
/// A peer connection's writing side.
pub type PeerOut = tokio::io::WriteHalf<tokio_rustls::server::TlsStream<TcpStream>>;

/// What a node serves its peers.
#[async_trait]
pub trait PeerVolumes: Send + Sync + 'static {
    /// `volume`'s export, if this node writes it under its `ds/` lease at
    /// `epoch`; otherwise why not.
    fn export(&self, volume: &str, epoch: Epoch) -> Result<PeerExport, String>;

    /// The writer of `volume` (at lease `epoch`) wants to hand it to this
    /// node, exported over `transport`: the rest of the conversation is
    /// the handler's (see `crate::agent`'s handoff). Unsupported by default.
    async fn handoff(
        &self,
        _volume: &str,
        _epoch: Epoch,
        _transport: &str,
        _rd: PeerLines,
        mut wr: PeerOut,
    ) {
        let _ = wr.write_all(b"ERR handoffs aren't taken here\n").await;
        let _ = wr.flush().await;
    }
}

/// Serve peer I/O on `listener`: `tls` must require a client certificate
/// (`TlsConfig::server_config(true, ..)`), and only nodes are let in.
pub async fn serve(listener: TcpListener, tls: TlsAcceptor, volumes: Arc<dyn PeerVolumes>) {
    crate::net::accept_loop(listener, "peer-io", move |sock, peer| {
        let (tls, volumes) = (tls.clone(), volumes.clone());
        async move {
            if let Err(e) = serve_conn(sock, tls, volumes).await {
                eprintln!("peer-io: {}: {}", peer, e);
            }
        }
    })
    .await
}

async fn serve_conn(
    sock: TcpStream,
    tls: TlsAcceptor,
    volumes: Arc<dyn PeerVolumes>,
) -> std::io::Result<()> {
    let tls = crate::net::tls_accept(&tls, sock).await?;
    if crate::frontend::peer_role(&tls) != Some(Role::Node) {
        return Ok(()); // only nodes proxy volumes
    }
    let (rd, mut wr) = tokio::io::split(tls);
    let mut rd = BufReader::new(rd);
    let line = tokio::time::timeout(CONNECT_TIMEOUT, read_line(&mut rd))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
    let first = line.split(' ').next().unwrap_or("");
    if first != PREAMBLE
        && first != HANDOFF
        && (first.starts_with("RAIDISH-VOL") || first.starts_with("RAIDISH-HANDOFF"))
    {
        let why = format!(
            "ERR {} isn't spoken here: this node speaks {} and {}\n",
            first, PREAMBLE, HANDOFF
        );
        wr.write_all(why.as_bytes()).await?;
        wr.flush().await?;
        return Ok(());
    }
    let mut words = line.split(' ');
    if let (Some(HANDOFF), Some(volume), Some(epoch), Some(transport), None) = (
        words.next(),
        words.next(),
        words.next(),
        words.next(),
        words.next(),
    ) {
        let Ok(epoch) = epoch.parse::<Epoch>() else {
            wr.write_all(b"ERR bad epoch\n").await?;
            wr.flush().await?;
            return Ok(());
        };
        volumes.handoff(volume, epoch, transport, rd, wr).await;
        return Ok(());
    }
    let mut words = line.split(' ');
    let (Some(PREAMBLE), Some(volume), Some(epoch), None) =
        (words.next(), words.next(), words.next(), words.next())
    else {
        wr.write_all(b"ERR bad request\n").await?;
        wr.flush().await?;
        return Ok(());
    };
    let Ok(epoch) = epoch.parse::<Epoch>() else {
        wr.write_all(b"ERR bad epoch\n").await?;
        return Ok(());
    };
    let export = match volumes.export(volume, epoch) {
        Ok(e) => e,
        Err(why) => {
            let why = why.replace('\n', " ");
            wr.write_all(format!("ERR {}\n", why).as_bytes()).await?;
            wr.flush().await?;
            return Ok(());
        }
    };
    wr.write_all(format!("OK {}\n", export.size).as_bytes())
        .await?;
    wr.flush().await?;
    crate::nbd::serve_requests(rd, wr, volume, export.io, export.session).await
}

pub async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(rd: &mut R) -> std::io::Result<String> {
    let mut line = Vec::new();
    let n = (&mut *rd)
        .take(MAX_LINE as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if n == 0 || line.last() != Some(&b'\n') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "no line",
        ));
    }
    line.pop();
    String::from_utf8(line).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))
}

/// A TLS connection to a peer's peer I/O port, as this node.
pub async fn dial(
    connector: &TlsConnector,
    addr: &str,
    port: u16,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let go = async {
        let sock = TcpStream::connect((addr, port))
            .await
            .map_err(|e| e.to_string())?;
        let _ = sock.set_nodelay(true);
        let name =
            rustls::pki_types::ServerName::try_from(addr.to_string()).map_err(|e| e.to_string())?;
        connector
            .connect(name, sock)
            .await
            .map_err(|e| e.to_string())
    };
    tokio::time::timeout(CONNECT_TIMEOUT, go)
        .await
        .map_err(|_| "timed out".to_string())?
        .map_err(|e| format!("{}:{}: {}", addr, port, e))
}

/// Where a volume's writer is now: its peer I/O address and port, and the
/// lease epoch it writes under. Read from the live lease every time.
pub trait Locate: Send + Sync + 'static {
    fn writer(&self) -> Option<(String, u16, Epoch)>;
}

impl<F: Fn() -> Option<(String, u16, Epoch)> + Send + Sync + 'static> Locate for F {
    fn writer(&self) -> Option<(String, u16, Epoch)> {
        self()
    }
}

type Reply = Result<Vec<u8>, u32>;

/// One connection to the writer, shared by every request.
struct Conn {
    writer: Mutex<tokio::io::WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>>,
    /// Requests sent and not answered: handle → (read length, reply).
    pending: StdMutex<HashMap<u64, (Option<u32>, oneshot::Sender<Reply>)>>,
    next: AtomicU64,
    dead: AtomicBool,
}

impl Conn {
    /// Fail everything pending: the connection is gone.
    fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
        self.pending.lock().unwrap().clear(); // dropped senders: the waiters see it
    }
}

/// A volume's I/O, proxied to its writer on another node.
pub struct PeerIo {
    volume: String,
    connector: TlsConnector,
    locate: Arc<dyn Locate>,
    conn: Mutex<Option<Arc<Conn>>>,
    /// How long one request keeps being retried before it fails.
    retry_for: Duration,
    retired: AtomicBool,
    retiring: Notify,
    /// When a failed attempt was last logged (ms): at most one a while.
    logged_ms: AtomicU64,
}

/// How often a proxy that keeps failing says so.
const LOG_EVERY_MS: u64 = 5000;

impl PeerIo {
    pub fn new(volume: &str, connector: TlsConnector, locate: Arc<dyn Locate>) -> Self {
        PeerIo {
            volume: volume.to_string(),
            connector,
            locate,
            conn: Mutex::new(None),
            retry_for: Duration::from_secs(60),
            retired: AtomicBool::new(false),
            retiring: Notify::new(),
            logged_ms: AtomicU64::new(0),
        }
    }

    /// Keep retrying a request this long (default 60 s) before failing it.
    pub fn retry_for(mut self, d: Duration) -> Self {
        self.retry_for = d;
        self
    }

    /// Stop: requests waiting or retrying give up at once with an error
    /// `retired()` explains, so a `SwitchableIo` sends them to its next
    /// backend instead (this node took the volume over, say).
    pub fn retire(&self) {
        self.retired.store(true, Ordering::SeqCst);
        self.retiring.notify_waiters();
        if let Ok(mut c) = self.conn.try_lock()
            && let Some(c) = c.take()
        {
            c.kill();
        }
    }

    fn gone() -> FileSystemError {
        FileSystemError::Unavailable("peer i/o retired".into())
    }

    /// The live connection, or a new one to wherever the writer is now.
    async fn conn(&self) -> Result<Arc<Conn>, String> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref()
            && !c.dead.load(Ordering::SeqCst)
        {
            return Ok(c.clone());
        }
        *slot = None;
        let (addr, port, epoch) = self
            .locate
            .writer()
            .ok_or_else(|| format!("volume '{}' has no writer", self.volume))?;
        let c = tokio::time::timeout(CONNECT_TIMEOUT, self.connect(&addr, port, epoch))
            .await
            .map_err(|_| format!("{}:{}: timed out", addr, port))?
            .map_err(|e| format!("{}:{}: {}", addr, port, e))?;
        *slot = Some(c.clone());
        println!(
            "peer-io: '{}' proxied to {}:{} (epoch {})",
            self.volume, addr, port, epoch
        );
        Ok(c)
    }

    async fn connect(&self, addr: &str, port: u16, epoch: Epoch) -> Result<Arc<Conn>, String> {
        let tls = dial(&self.connector, addr, port).await?;
        let (rd, mut wr) = tokio::io::split(tls);
        wr.write_all(format!("{} {} {}\n", PREAMBLE, self.volume, epoch).as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        wr.flush().await.map_err(|e| e.to_string())?;
        let mut rd = BufReader::new(rd);
        let answer = read_line(&mut rd).await.map_err(|e| e.to_string())?;
        if answer.strip_prefix("OK ").is_none() {
            return Err(answer.strip_prefix("ERR ").unwrap_or(&answer).to_string());
        }
        let conn = Arc::new(Conn {
            writer: Mutex::new(wr),
            pending: StdMutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            dead: AtomicBool::new(false),
        });
        let c = conn.clone();
        tokio::spawn(async move {
            if let Err(e) = read_replies(rd, &c).await
                && !c.dead.load(Ordering::SeqCst)
            {
                eprintln!("peer-io: connection lost: {}", e);
            }
            c.kill();
        });
        Ok(conn)
    }

    /// Send one request on `conn` and wait for its reply. Err(None): the
    /// connection failed (worth retrying); Err(Some(errno)): the writer
    /// answered with an error.
    async fn send(
        &self,
        conn: &Conn,
        cmd: u16,
        offset: u64,
        len: u32,
        data: &[u8],
    ) -> Result<Vec<u8>, Option<u32>> {
        let handle = conn.next.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        let read_len = (cmd == NBD_CMD_READ).then_some(len);
        conn.pending.lock().unwrap().insert(handle, (read_len, tx));
        if conn.dead.load(Ordering::SeqCst) {
            conn.pending.lock().unwrap().remove(&handle);
            return Err(None);
        }
        let mut msg = Vec::with_capacity(28 + data.len());
        msg.extend_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        msg.extend_from_slice(&0u16.to_be_bytes());
        msg.extend_from_slice(&cmd.to_be_bytes());
        msg.extend_from_slice(&handle.to_be_bytes());
        msg.extend_from_slice(&offset.to_be_bytes());
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(data);
        let sent = {
            let mut w = conn.writer.lock().await;
            match w.write_all(&msg).await {
                Ok(()) => w.flush().await,
                Err(e) => Err(e),
            }
        };
        if sent.is_err() {
            conn.kill();
            return Err(None);
        }
        match rx.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(errno)) => Err(Some(errno)),
            Err(_) => Err(None),
        }
    }

    /// A request, retried on failed connections until it's answered, the
    /// time runs out, or this is retired.
    async fn call(
        &self,
        cmd: u16,
        offset: u64,
        len: u32,
        data: &[u8],
    ) -> Result<Vec<u8>, FileSystemError> {
        let deadline = Instant::now() + self.retry_for;
        let mut pause = Duration::from_millis(50);
        loop {
            // Armed before the flag is looked at: a retire in between wakes it.
            let retiring = self.retiring.notified();
            tokio::pin!(retiring);
            retiring.as_mut().enable();
            if self.retired.load(Ordering::SeqCst) {
                return Err(Self::gone());
            }
            let attempt = async {
                let conn = self.conn().await.map_err(|e| (None, e))?;
                self.send(&conn, cmd, offset, len, data)
                    .await
                    .map_err(|e| (e, String::from("connection lost")))
            };
            let why = tokio::select! {
                r = attempt => match r {
                    Ok(data) => return Ok(data),
                    Err((Some(errno), _)) => return Err(errno_error(errno)),
                    Err((None, why)) => why,
                },
                _ = &mut retiring => return Err(Self::gone()),
            };
            let now = crate::cluster::now_ms();
            let last = self.logged_ms.load(Ordering::Relaxed);
            if now.saturating_sub(last) >= LOG_EVERY_MS
                && self
                    .logged_ms
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                eprintln!("peer-io: '{}': {}; retrying", self.volume, why);
            }
            if Instant::now() >= deadline {
                return Err(FileSystemError::Unavailable(format!(
                    "volume '{}': its writer didn't answer for {:?}: {}",
                    self.volume, self.retry_for, why
                )));
            }
            tokio::select! {
                _ = tokio::time::sleep(pause) => {}
                _ = &mut retiring => return Err(Self::gone()),
            }
            pause = (pause * 2).min(Duration::from_secs(1));
        }
    }
}

impl Drop for PeerIo {
    fn drop(&mut self) {
        if let Some(c) = self.conn.get_mut().take() {
            c.kill();
        }
    }
}

/// What a writer's NBD error means here.
fn errno_error(errno: u32) -> FileSystemError {
    match errno {
        ENOSPC => FileSystemError::NoSpace("on the writer".into()),
        EIO => FileSystemError::Unavailable("the writer failed the request (EIO)".into()),
        e => FileSystemError::InvalidArgument(format!("the writer refused the request ({})", e)),
    }
}

/// Hand each reply on the connection to its waiting request.
async fn read_replies<R: tokio::io::AsyncRead + Unpin>(
    mut rd: R,
    conn: &Conn,
) -> std::io::Result<()> {
    loop {
        let mut hdr = [0u8; 16];
        rd.read_exact(&mut hdr).await?;
        if u32::from_be_bytes(hdr[0..4].try_into().unwrap()) != NBD_REPLY_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad reply magic",
            ));
        }
        let error = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
        let handle = u64::from_be_bytes(hdr[8..16].try_into().unwrap());
        let Some((read_len, tx)) = conn.pending.lock().unwrap().remove(&handle) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("reply for unknown handle {}", handle),
            ));
        };
        let reply = match (error, read_len) {
            (0, Some(len)) => {
                let mut buf = vec![0u8; len as usize];
                rd.read_exact(&mut buf).await?;
                Ok(buf)
            }
            (0, None) => Ok(Vec::new()),
            (e, _) => Err(e),
        };
        let _ = tx.send(reply);
    }
}

#[async_trait]
impl VolumeIo for PeerIo {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        let data = self
            .call(NBD_CMD_READ, offset, buf.len() as u32, &[])
            .await?;
        buf.copy_from_slice(&data);
        Ok(buf.len())
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.call(NBD_CMD_WRITE, offset, data.len() as u32, data)
            .await?;
        Ok(data.len())
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        // NBD lengths are 32 bits: a bigger range goes as several.
        let mut done = 0usize;
        while done < len {
            let n = (len - done).min(1 << 30);
            self.call(NBD_CMD_WRITE_ZEROES, offset + done as u64, n as u32, &[])
                .await?;
            done += n;
        }
        Ok(())
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.call(NBD_CMD_FLUSH, 0, 0, &[]).await.map(|_| ())
    }
    /// What this node has to commit for the volume: nothing (the writer
    /// commits). A flush makes what was written here durable there.
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.flush().await
    }
    fn retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[path = "tests/peer_io_tests.rs"]
mod tests;
