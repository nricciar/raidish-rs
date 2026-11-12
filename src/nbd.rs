//! NBD server for copy-on-write volumes.
//!
//! One listener serves any number of volumes ("exports"), chosen by name
//! during option haggling (`NBD_OPT_EXPORT_NAME` / `NBD_OPT_GO`); an empty
//! name picks the only export when there is exactly one. `NBD_OPT_LIST`
//! enumerates them. Exports are added and removed at runtime; removing one
//! closes its connections (in-flight requests finish first) and commits.
//!
//! Requests are handled concurrently and replied to as they finish (NBD
//! matches replies by handle), so the kernel can keep its queue full. Writes
//! complete once they're in the volume's dirty records; FLUSH commits a TXG,
//! which makes every completed write durable. A background task (one per
//! `FileSystem`, shared with the NVMe/TCP target) also commits every few
//! seconds while there is dirty data.

use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;

const NBD_INIT_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054; // "IHAVEOPT"
const NBD_REP_MAGIC: u64 = 0x3e889045565a9;
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: u32 = 0x67446698;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x80000001;
const NBD_REP_ERR_INVALID: u32 = 0x80000003;
const NBD_REP_ERR_UNKNOWN: u32 = 0x80000006;

const NBD_INFO_EXPORT: u16 = 0;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
const TRANSMISSION_FLAGS: u16 =
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_SEND_WRITE_ZEROES;

const EIO: u32 = 5;
const EINVAL: u32 = 22;
const ENOSPC: u32 = 28;

/// NBD error for a filesystem error: out of space is reported as such.
fn errno(e: &crate::fs::FileSystemError) -> u32 {
    match e {
        crate::fs::FileSystemError::UnableToAllocateBlocks(_) => ENOSPC,
        _ => EIO,
    }
}

/// How often dirty volume data is committed without an explicit FLUSH.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Largest READ/WRITE payload we accept (the kernel caps NBD I/O at 32 MiB).
const MAX_REQUEST_BYTES: u32 = 32 << 20;
/// Largest option payload we accept during haggling (names are <= 4 KiB).
const MAX_OPTION_BYTES: u32 = 64 << 10;
/// Address the block servers bind to unless told otherwise.
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// How long removing an export waits for its connections to wind down.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// File systems that already have a background sync loop, by address. Weak,
/// so a dropped file system (and its loop) doesn't pin the entry.
static SYNC_LOOPS: StdMutex<Vec<Weak<dyn Any + Send + Sync>>> = StdMutex::new(Vec::new());

/// Start the background TXG sync for `fs` unless one is already running.
/// Shared by every NBD server and NVMe/TCP target on the same file system;
/// the loop ends once the file system is dropped.
pub(crate) fn ensure_sync_loop<D>(fs: &Arc<FileSystem<D>>, who: &'static str)
where
    D: BlockDevice + Send + Sync + 'static,
{
    {
        let mut loops = SYNC_LOOPS.lock().unwrap();
        loops.retain(|w| w.strong_count() > 0);
        let ptr = Arc::as_ptr(fs) as *const ();
        if loops.iter().any(|w| w.as_ptr() as *const () == ptr) {
            return;
        }
        let any: Arc<dyn Any + Send + Sync> = fs.clone();
        loops.push(Arc::downgrade(&any));
    }
    let weak = Arc::downgrade(fs);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SYNC_INTERVAL);
        loop {
            tick.tick().await;
            let Some(fs) = weak.upgrade() else { return };
            if fs.has_dirty_volumes()
                && let Err(e) = fs.sync().await
            {
                eprintln!("{}: background sync failed: {}", who, e);
            }
        }
    });
}

/// Lifetime of one export as seen by its connections: they `join` it when
/// they attach and watch for `close`, which waits until they have all gone.
pub(crate) struct ExportLife {
    closed: watch::Sender<bool>,
    alive: StdMutex<Option<mpsc::Sender<()>>>,
    drained: Mutex<mpsc::Receiver<()>>,
}

/// A connection's hold on an export; dropping it lets `close` finish.
pub(crate) struct ExportSession {
    _alive: mpsc::Sender<()>,
    closed: watch::Receiver<bool>,
}

impl ExportSession {
    /// Another handle on the close signal, for a different task to wait on.
    pub(crate) fn close_signal(&self) -> watch::Receiver<bool> {
        self.closed.clone()
    }
}

/// Resolves once the export is closing (never, for `None`).
pub(crate) async fn export_closed(rx: &mut Option<watch::Receiver<bool>>) {
    match rx {
        // An error means the export itself is gone: closed too.
        Some(rx) => drop(rx.wait_for(|closed| *closed).await),
        None => std::future::pending().await,
    }
}

impl ExportLife {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        ExportLife {
            closed: watch::Sender::new(false),
            alive: StdMutex::new(Some(tx)),
            drained: Mutex::new(rx),
        }
    }

    /// Attach a connection; `None` once the export is closing.
    pub(crate) fn join(&self) -> Option<ExportSession> {
        let alive = self.alive.lock().unwrap().clone()?;
        Some(ExportSession {
            _alive: alive,
            closed: self.closed.subscribe(),
        })
    }

    /// Refuse new connections, tell current ones to stop, and wait (up to
    /// `DRAIN_TIMEOUT`) for them to finish their in-flight requests.
    pub(crate) async fn close(&self, what: &str) {
        self.alive.lock().unwrap().take();
        self.closed.send_replace(true);
        let mut drained = self.drained.lock().await;
        if tokio::time::timeout(DRAIN_TIMEOUT, drained.recv())
            .await
            .is_err()
        {
            eprintln!("{}: connections did not close in {:?}", what, DRAIN_TIMEOUT);
        }
    }
}

struct Export {
    name: String,
    size: u64,
    life: ExportLife,
}

/// An NBD server exporting any number of volumes of one file system.
pub struct NbdServer<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    bind: String,
    port: u16,
    exports: Exports,
}

/// Serve the single volume `zvol_name` on `127.0.0.1:port` (the CLI's `nbd`).
pub async fn serve_nbd<D>(
    fs: Arc<FileSystem<D>>,
    zvol_name: String,
    port: u16,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    let server = NbdServer::with_bind(fs, DEFAULT_BIND, port);
    server
        .add_export(&zvol_name)
        .await
        .map_err(|e| std::io::Error::other(format!("volume '{}': {}", zvol_name, e)))?;
    server.run().await
}

// The multi-export API is driven by the node agent; until it lands only the
// single-export CLI wrapper and the tests use all of it.
#[allow(dead_code)]
impl<D> NbdServer<D>
where
    D: BlockDevice + Send + Sync + 'static,
{
    /// A server with no exports that `run` binds to `bind:port`.
    pub fn with_bind(fs: Arc<FileSystem<D>>, bind: impl Into<String>, port: u16) -> Self {
        NbdServer {
            fs,
            bind: bind.into(),
            port,
            exports: Arc::new(StdMutex::new(BTreeMap::new())),
        }
    }

    /// Export volume `name` (under the same name).
    pub async fn add_export(&self, name: &str) -> Result<(), FileSystemError> {
        if name.is_empty() || name.len() > 4096 {
            return Err(FileSystemError::InvalidArgument(format!(
                "bad export name '{}'",
                name
            )));
        }
        let size = self.fs.open_volume(name).await?.size_bytes;
        let mut exports = self.exports.lock().unwrap();
        if exports.contains_key(name) {
            return Err(FileSystemError::AlreadyExists);
        }
        exports.insert(
            name.to_string(),
            Arc::new(Export {
                name: name.to_string(),
                size,
                life: ExportLife::new(),
            }),
        );
        Ok(())
    }

    /// Stop exporting `name`: new connections are refused, current ones are
    /// closed once their in-flight requests finish, then everything is
    /// committed. Returns false if there was no such export.
    pub async fn remove_export(&self, name: &str) -> Result<bool, FileSystemError> {
        let Some(export) = self.exports.lock().unwrap().remove(name) else {
            return Ok(false);
        };
        export.life.close(&format!("nbd: export '{}'", name)).await;
        self.fs.sync().await?;
        Ok(true)
    }

    /// Names of the current exports, sorted.
    pub fn exports(&self) -> Vec<String> {
        self.exports.lock().unwrap().keys().cloned().collect()
    }

    /// Bind `bind:port` and serve until the listener fails.
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind((self.bind.as_str(), self.port)).await?;
        self.serve(listener).await
    }

    /// Serve connections from an already-bound listener.
    pub async fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        ensure_sync_loop(&self.fs, "nbd");
        println!(
            "NBD server listening on {} (exports: {})",
            listener.local_addr()?,
            self.exports().join(", ")
        );
        loop {
            let (stream, addr) = listener.accept().await?;
            println!("Client connected from {}", addr);
            let (fs, exports) = (self.fs.clone(), self.exports.clone());
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, fs, exports).await {
                    eprintln!("Client error: {}", e);
                }
            });
        }
    }
}

type Exports = Arc<StdMutex<BTreeMap<String, Arc<Export>>>>;

/// Export by name; "" means the only export, if there is exactly one.
fn lookup(exports: &Exports, name: &str) -> Option<Arc<Export>> {
    let exports = exports.lock().unwrap();
    if name.is_empty() {
        return match exports.len() {
            1 => exports.values().next().cloned(),
            _ => None,
        };
    }
    exports.get(name).cloned()
}

/// Send one option reply.
async fn opt_reply(
    stream: &mut TcpStream,
    option: u32,
    kind: u32,
    data: &[u8],
) -> std::io::Result<()> {
    let mut msg = Vec::with_capacity(20 + data.len());
    msg.extend_from_slice(&NBD_REP_MAGIC.to_be_bytes());
    msg.extend_from_slice(&option.to_be_bytes());
    msg.extend_from_slice(&kind.to_be_bytes());
    msg.extend_from_slice(&(data.len() as u32).to_be_bytes());
    msg.extend_from_slice(data);
    stream.write_all(&msg).await
}

/// The export name in an NBD_OPT_INFO / NBD_OPT_GO payload, if well formed.
fn info_request_name(data: &[u8]) -> Option<String> {
    let len = u32::from_be_bytes(data.get(..4)?.try_into().unwrap()) as usize;
    let name = data.get(4..4 + len)?;
    let nreq = u16::from_be_bytes(data.get(4 + len..6 + len)?.try_into().unwrap()) as usize;
    if data.len() != 6 + len + 2 * nreq {
        return None;
    }
    String::from_utf8(name.to_vec()).ok()
}

async fn handle_client<D>(
    mut stream: TcpStream,
    fs: Arc<FileSystem<D>>,
    exports: Exports,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    stream.set_nodelay(true)?;

    // Initial handshake
    stream.write_u64(NBD_INIT_MAGIC).await?;
    stream.write_u64(NBD_OPTS_MAGIC).await?;
    stream
        .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
        .await?;

    let client_flags = stream.read_u32().await?;
    let no_zeroes = (client_flags & (NBD_FLAG_NO_ZEROES as u32)) != 0;

    // Option haggling phase
    loop {
        let opts_magic = stream.read_u64().await?;
        if opts_magic != NBD_OPTS_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Bad options magic: 0x{:x}", opts_magic),
            ));
        }

        let option = stream.read_u32().await?;
        let length = stream.read_u32().await?;
        if length > MAX_OPTION_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("option {} of {} bytes too large", option, length),
            ));
        }
        let mut option_data = vec![0u8; length as usize];
        if length > 0 {
            stream.read_exact(&mut option_data).await?;
        }

        match option {
            NBD_OPT_EXPORT_NAME => {
                // No way to report an error here: unknown names just hang up.
                let name = String::from_utf8_lossy(&option_data);
                let Some(export) = lookup(&exports, &name) else {
                    eprintln!("nbd: client asked for unknown export '{}'", name);
                    return Ok(());
                };
                let Some(session) = export.life.join() else {
                    return Ok(());
                };
                stream.write_u64(export.size).await?;
                stream.write_u16(TRANSMISSION_FLAGS).await?;
                if !no_zeroes {
                    stream.write_all(&[0u8; 124]).await?;
                }
                return transmission_phase(stream, fs, &export.name, session).await;
            }
            NBD_OPT_INFO | NBD_OPT_GO => {
                let Some(name) = info_request_name(&option_data) else {
                    opt_reply(&mut stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                };
                let found = lookup(&exports, &name);
                let session = found.as_ref().and_then(|e| e.life.join());
                let (Some(export), Some(session)) = (found, session) else {
                    opt_reply(&mut stream, option, NBD_REP_ERR_UNKNOWN, &[]).await?;
                    continue;
                };

                let mut info = Vec::new();
                info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&export.size.to_be_bytes());
                info.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());
                opt_reply(&mut stream, option, NBD_REP_INFO, &info).await?;
                opt_reply(&mut stream, option, NBD_REP_ACK, &[]).await?;

                if option == NBD_OPT_GO {
                    return transmission_phase(stream, fs, &export.name, session).await;
                }
            }
            NBD_OPT_LIST => {
                if length != 0 {
                    opt_reply(&mut stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }
                let names: Vec<String> = exports.lock().unwrap().keys().cloned().collect();
                for name in names {
                    let mut data = Vec::with_capacity(4 + name.len());
                    data.extend_from_slice(&(name.len() as u32).to_be_bytes());
                    data.extend_from_slice(name.as_bytes());
                    opt_reply(&mut stream, option, NBD_REP_SERVER, &data).await?;
                }
                opt_reply(&mut stream, option, NBD_REP_ACK, &[]).await?;
            }
            NBD_OPT_ABORT => {
                let _ = opt_reply(&mut stream, option, NBD_REP_ACK, &[]).await;
                return Ok(());
            }
            _ => opt_reply(&mut stream, option, NBD_REP_ERR_UNSUP, &[]).await?,
        }
    }
}

/// Send one simple reply (header plus optional read payload) atomically.
async fn reply(
    writer: &Mutex<OwnedWriteHalf>,
    handle: u64,
    error: u32,
    data: &[u8],
) -> std::io::Result<()> {
    let mut msg = Vec::with_capacity(16 + data.len());
    msg.extend_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
    msg.extend_from_slice(&error.to_be_bytes());
    msg.extend_from_slice(&handle.to_be_bytes());
    msg.extend_from_slice(data);
    writer.lock().await.write_all(&msg).await
}

/// The fixed 28-byte request header: (cmd, handle, offset, length).
async fn read_request(reader: &mut OwnedReadHalf) -> std::io::Result<(u16, u64, u64, u32)> {
    let mut hdr = [0u8; 28];
    reader.read_exact(&mut hdr).await?;
    let magic = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
    if magic != NBD_REQUEST_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Bad request magic: 0x{:x}", magic),
        ));
    }
    Ok((
        u16::from_be_bytes(hdr[6..8].try_into().unwrap()),
        u64::from_be_bytes(hdr[8..16].try_into().unwrap()),
        u64::from_be_bytes(hdr[16..24].try_into().unwrap()),
        u32::from_be_bytes(hdr[24..28].try_into().unwrap()),
    ))
}

async fn transmission_phase<D>(
    stream: TcpStream,
    fs: Arc<FileSystem<D>>,
    zvol_name: &str,
    session: ExportSession,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let name = Arc::new(zvol_name.to_string());
    let mut tasks = JoinSet::new();
    let mut closed = Some(session.close_signal());

    loop {
        let (cmd, handle, offset, length) = tokio::select! {
            req = read_request(&mut reader) => req?,
            _ = export_closed(&mut closed) => {
                // Export removed: let in-flight requests reply, then hang up.
                while let Some(res) = tasks.join_next().await {
                    res.map_err(std::io::Error::other)??;
                }
                println!("nbd: export '{}' removed; closing client", name);
                return Ok(());
            }
        };

        // Reap finished tasks so the set doesn't grow without bound.
        while let Some(res) = tasks.try_join_next() {
            res.map_err(std::io::Error::other)??;
        }

        // Only READ/WRITE carry a payload we have to buffer; TRIM and
        // WRITE_ZEROES can legitimately cover huge ranges (the kernel issues
        // discards up to ~4 GiB).
        if length > MAX_REQUEST_BYTES && matches!(cmd, NBD_CMD_READ | NBD_CMD_WRITE) {
            eprintln!(
                "nbd: rejecting cmd {} of {} bytes (max {})",
                cmd, length, MAX_REQUEST_BYTES
            );
            if cmd == NBD_CMD_WRITE {
                // Can't skip the payload safely; drop the connection.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("request of {} bytes too large", length),
                ));
            }
            reply(&writer, handle, EINVAL, &[]).await?;
            continue;
        }

        let (fs, writer, name) = (fs.clone(), writer.clone(), name.clone());
        match cmd {
            NBD_CMD_READ => {
                tasks.spawn(async move {
                    let mut buf = vec![0u8; length as usize];
                    match fs.block_read(&name, offset, &mut buf).await {
                        Ok(n) => {
                            buf[n..].fill(0);
                            reply(&writer, handle, 0, &buf).await
                        }
                        Err(e) => {
                            eprintln!("nbd: read {}+{}: {}", offset, length, e);
                            reply(&writer, handle, EIO, &[]).await
                        }
                    }
                });
            }
            NBD_CMD_WRITE => {
                let mut buf = vec![0u8; length as usize];
                reader.read_exact(&mut buf).await?;
                tasks.spawn(async move {
                    let error = match fs.block_write(&name, offset, &buf).await {
                        Ok(n) if n == buf.len() => 0,
                        Ok(_) => EIO,
                        Err(e) => {
                            eprintln!("nbd: write {}+{}: {}", offset, length, e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_TRIM | NBD_CMD_WRITE_ZEROES => {
                tasks.spawn(async move {
                    let error = match fs.block_zero(&name, offset, length as usize).await {
                        Ok(_) => 0,
                        Err(e) => {
                            eprintln!("nbd: zero {}+{}: {}", offset, length, e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_FLUSH => {
                tasks.spawn(async move {
                    let error = match fs.sync().await {
                        Ok(()) => 0,
                        Err(e) => {
                            eprintln!("nbd: flush: {}", e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_DISC => {
                while let Some(res) = tasks.join_next().await {
                    res.map_err(std::io::Error::other)??;
                }
                if let Err(e) = fs.sync().await {
                    eprintln!("nbd: sync on disconnect: {}", e);
                }
                println!("Client disconnected");
                return Ok(());
            }
            _ => {
                eprintln!("nbd: unsupported command {}", cmd);
                reply(&writer, handle, EINVAL, &[]).await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/nbd_tests.rs"]
mod tests;
