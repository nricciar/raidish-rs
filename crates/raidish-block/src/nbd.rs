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
use crate::export::{BlockExporter, ExportLife, ExportTable, Exported};
pub(crate) use crate::export::{ExportSession, export_closed};
use crate::fs::{FileSystem, FileSystemError};
use crate::types::DatasetName;
use crate::volume_io::{LocalIo, SwitchableIo, VolumeIo, ensure_sync_loop};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

/// The NBD port.
pub const NBD_PORT: u16 = 10809;

const NBD_INIT_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054; // "IHAVEOPT"
const NBD_REP_MAGIC: u64 = 0x3e889045565a9;
pub(crate) const NBD_REQUEST_MAGIC: u32 = 0x25609513;
pub(crate) const NBD_REPLY_MAGIC: u32 = 0x67446698;

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

pub(crate) const NBD_CMD_READ: u16 = 0;
pub(crate) const NBD_CMD_WRITE: u16 = 1;
pub(crate) const NBD_CMD_DISC: u16 = 2;
pub(crate) const NBD_CMD_FLUSH: u16 = 3;
pub(crate) const NBD_CMD_TRIM: u16 = 4;
pub(crate) const NBD_CMD_WRITE_ZEROES: u16 = 6;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
const TRANSMISSION_FLAGS: u16 =
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_SEND_WRITE_ZEROES;

pub(crate) const EIO: u32 = 5;
const EINVAL: u32 = 22;
pub(crate) const ENOSPC: u32 = 28;

/// NBD error for a filesystem error: out of space is reported as such.
fn errno(e: &crate::fs::FileSystemError) -> u32 {
    match e.kind() {
        crate::types::ErrorKind::NoSpace => ENOSPC,
        _ => EIO,
    }
}

/// Largest READ/WRITE payload we accept (the kernel caps NBD I/O at 32 MiB).
const MAX_REQUEST_BYTES: u32 = 32 << 20;
/// Requests one connection may have in flight, and the READ/WRITE payload
/// they may hold between them (`crate::net::InFlight`). The kernel client
/// queues up to 128 by default; past this, the connection isn't read until
/// one finishes.
pub(crate) const MAX_IN_FLIGHT: usize = 128;
pub(crate) const MAX_IN_FLIGHT_BYTES: u32 = 128 << 20;
/// Largest option payload we accept during haggling (names are <= 4 KiB).
const MAX_OPTION_BYTES: u32 = 64 << 10;
/// Address the block servers bind to unless told otherwise.
pub const DEFAULT_BIND: &str = "127.0.0.1";

struct Export {
    name: String,
    size: u64,
    life: ExportLife,
    /// Where its I/O goes (this node's file system, for now).
    io: Arc<SwitchableIo>,
}

impl Exported for Export {
    fn volume(&self) -> &str {
        &self.name
    }
    fn size(&self) -> u64 {
        self.size
    }
    fn life(&self) -> &ExportLife {
        &self.life
    }
    fn io(&self) -> &Arc<SwitchableIo> {
        &self.io
    }
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
    zvol_name: DatasetName,
    port: u16,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    let server = NbdServer::with_bind(fs, DEFAULT_BIND, port);
    // Served until the process ends.
    let _export = server
        .add_export(&zvol_name)
        .await
        .map_err(|e| std::io::Error::other(format!("volume '{}': {}", zvol_name, e)))?;
    server.run().await
}

// The multi-export API is driven by the node agent; until it lands only the
// single-export CLI wrapper and the tests use all of it.
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
            exports: Arc::new(ExportTable::new("nbd", str::to_string)),
        }
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
            self.exports.keys().join(", ")
        );
        let exports = self.exports.clone();
        crate::net::accept_loop(listener, "nbd", move |stream, addr| {
            let exports = exports.clone();
            async move {
                if let Err(e) = handle_client(stream, addr, exports).await {
                    eprintln!("nbd: {}: {}", addr, e);
                }
            }
        })
        .await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<D> BlockExporter for NbdServer<D>
where
    D: BlockDevice + Send + Sync + 'static,
{
    async fn add_export(
        &self,
        name: &DatasetName,
    ) -> Result<crate::export::ExportHandle, FileSystemError> {
        let size = self.fs.open_dataset(name).await?.size_bytes;
        let io = Arc::new(LocalIo::new(self.fs.clone(), name));
        self.add_export_io(name, size, io)
    }

    fn add_export_io(
        &self,
        name: &DatasetName,
        size: u64,
        io: Arc<dyn VolumeIo>,
    ) -> Result<crate::export::ExportHandle, FileSystemError> {
        self.exports.insert(Export {
            name: name.to_string(),
            size,
            life: ExportLife::new(),
            io: Arc::new(SwitchableIo::new(io)),
        })
    }

    fn export_io(&self, name: &DatasetName) -> Option<Arc<SwitchableIo>> {
        self.exports.export_io(name)
    }

    fn peer_export(&self, name: &DatasetName) -> Option<crate::peer_io::PeerExport> {
        self.exports.peer_export(name)
    }

    #[cfg(test)]
    fn exports(&self) -> Vec<String> {
        self.exports.keys()
    }
}

type Exports = Arc<ExportTable<Export>>;

/// Export by name; "" means the only export, if there is exactly one.
fn lookup(exports: &Exports, name: &str) -> Option<Arc<Export>> {
    if name.is_empty() {
        exports.only()
    } else {
        exports.by_key(name)
    }
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

/// Unknown-export refusals, logged at most once a minute per name: a
/// client retrying in a loop mustn't flood the log.
static REFUSALS: std::sync::LazyLock<crate::net::LogThrottle> =
    std::sync::LazyLock::new(|| crate::net::LogThrottle::new(std::time::Duration::from_secs(60)));

fn refused(addr: std::net::SocketAddr, name: &str) {
    if let Some(more) = REFUSALS.allow(name) {
        let also = match more {
            0 => String::new(),
            n => format!(" (and {} more times since last said)", n),
        };
        eprintln!("nbd: {} asked for unknown export '{}'{}", addr, name, also);
    }
}

async fn handle_client(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    exports: Exports,
) -> std::io::Result<()> {
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
                    refused(addr, &name);
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
                return transmission_phase(stream, addr, &export, session).await;
            }
            NBD_OPT_INFO | NBD_OPT_GO => {
                let Some(name) = info_request_name(&option_data) else {
                    opt_reply(&mut stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                };
                let found = lookup(&exports, &name);
                let session = found.as_ref().and_then(|e| e.life.join());
                let (Some(export), Some(session)) = (found, session) else {
                    if option == NBD_OPT_GO {
                        refused(addr, &name);
                    }
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
                    return transmission_phase(stream, addr, &export, session).await;
                }
            }
            NBD_OPT_LIST => {
                if length != 0 {
                    opt_reply(&mut stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }
                let names = exports.keys();
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
async fn reply<W: AsyncWrite + Unpin>(
    writer: &Mutex<W>,
    handle: u64,
    error: u32,
    data: &[u8],
) -> std::io::Result<()> {
    let mut msg = Vec::with_capacity(16 + data.len());
    msg.extend_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
    msg.extend_from_slice(&error.to_be_bytes());
    msg.extend_from_slice(&handle.to_be_bytes());
    msg.extend_from_slice(data);
    // Flushed: over TLS (peer I/O) a write can leave the reply buffered in
    // the TLS session until the next one, and a client waiting on its last
    // request would wait for good.
    let mut w = writer.lock().await;
    w.write_all(&msg).await?;
    w.flush().await
}

/// The fixed 28-byte request header: (cmd, handle, offset, length).
async fn read_request<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<(u16, u64, u64, u32)> {
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

async fn transmission_phase(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    export: &Export,
    session: ExportSession,
) -> std::io::Result<()> {
    println!("nbd: {} attached to '{}'", addr, export.name);
    let (reader, writer) = stream.into_split();
    let r = serve_requests(reader, writer, &export.name, export.io.clone(), session).await;
    println!("nbd: {} detached from '{}'", addr, export.name);
    r
}

/// The NBD transmission phase on any stream: requests for volume `name`
/// served from `io` until the client disconnects or the export closes.
/// Also what peer I/O (`crate::peer_io`) speaks between nodes.
pub(crate) async fn serve_requests<R, W>(
    mut reader: R,
    writer: W,
    name: &str,
    io: Arc<SwitchableIo>,
    session: ExportSession,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let writer = Arc::new(Mutex::new(writer));
    let name = Arc::new(name.to_string());
    let mut tasks = JoinSet::new();
    let mut closed = Some(session.close_signal());
    let budget = crate::net::InFlight::new(MAX_IN_FLIGHT, MAX_IN_FLIGHT_BYTES);

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

        // Room for it first (a WRITE's payload isn't read until then).
        let payload = match cmd {
            NBD_CMD_READ | NBD_CMD_WRITE => length as usize,
            _ => 0,
        };
        let admitted = budget.admit(payload).await;
        let (io, writer) = (io.clone(), writer.clone());
        match cmd {
            NBD_CMD_READ => {
                tasks.spawn(async move {
                    let _admitted = admitted;
                    let mut buf = vec![0u8; length as usize];
                    match io.read(offset, &mut buf).await {
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
                    let _admitted = admitted;
                    let error = match io.write(offset, &buf).await {
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
                    let _admitted = admitted;
                    let error = match io.zero(offset, length as usize).await {
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
                    let _admitted = admitted;
                    let error = match io.flush().await {
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
                if let Err(e) = io.sync().await {
                    eprintln!("nbd: sync on disconnect: {}", e);
                }
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
pub(crate) mod tests;
