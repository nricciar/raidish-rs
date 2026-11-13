//! NVMe over TCP target exporting copy-on-write volumes. Each volume is its
//! own subsystem, `nqn.2024-01.com.raidish:nvme:<volume>`, with the volume as
//! namespace 1; one listener serves all of them and the host picks one by
//! SUBNQN in Connect. Exports are added and removed at runtime; removing one
//! closes its queues (in-flight commands finish first) and commits.
//!
//! Each TCP connection is one queue pair. The first capsule on it must be a
//! Fabrics Connect, which decides whether it is the admin queue (qid 0, which
//! creates a controller) or an I/O queue (qid > 0, which joins one). Admin and
//! I/O opcodes overlap, so commands are always dispatched by queue type.
//!
//! Commands run concurrently; each reply (C2H data PDUs + response capsule) is
//! written in one piece under the writer lock. Host-to-controller data arrives
//! either in-capsule (up to `INLINE_DATA_SIZE`) or after we send an R2T.
//!
//! Writes complete once they are in the volume's dirty records (the
//! controller advertises a volatile write cache), and FLUSH commits a TXG.
//! Set `RAIDISH_NVME_TRACE=1` to log every command.

use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::nbd::{DEFAULT_BIND, ExportLife, ExportSession, ensure_sync_loop, export_closed};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, Notify, watch};
use tokio::task::JoinSet;

/// Subsystem NQNs are this followed by the volume name.
pub const NQN_PREFIX: &str = "nqn.2024-01.com.raidish:nvme:";
/// NQNs are at most 223 bytes.
const MAX_NQN: usize = 223;

/// The subsystem NQN a volume is exported under.
pub fn subsystem_nqn(volume: &str) -> String {
    format!("{}{}", NQN_PREFIX, volume)
}
pub const LBA_SIZE: u64 = 4096;
const LBA_SHIFT: u8 = 12;

// PDU types and flags.
const PDU_IC_REQ: u8 = 0x00;
const PDU_IC_RESP: u8 = 0x01;
const PDU_H2C_TERM: u8 = 0x02;
const PDU_CMD: u8 = 0x04;
const PDU_RSP: u8 = 0x05;
const PDU_H2C_DATA: u8 = 0x06;
const PDU_C2H_DATA: u8 = 0x07;
const PDU_R2T: u8 = 0x09;
const PDU_FLAG_HDGST: u8 = 0x01;
const PDU_FLAG_DDGST: u8 = 0x02;
const PDU_FLAG_LAST: u8 = 0x04;

/// Largest H2C data PDU we accept.
pub const MAX_H2C_DATA: u32 = 128 * 1024;
/// In-capsule data the host may send (writes up to this skip the R2T).
pub const INLINE_DATA_SIZE: usize = 8192;
/// Max data transfer: 2^MDTS * 4 KiB = 1 MiB.
const MDTS: u8 = 8;
pub const MAX_TRANSFER: usize = (1 << MDTS) * LBA_SIZE as usize;
const MAX_PDU: usize = MAX_TRANSFER + 8192;
const C2H_CHUNK: usize = 128 * 1024;
/// Entries per queue (CAP.MQES + 1).
pub const QUEUE_ENTRIES: u16 = 128;
pub const MAX_IO_QUEUES: u16 = 8;

// Admin opcodes.
const ADM_GET_LOG_PAGE: u8 = 0x02;
const ADM_IDENTIFY: u8 = 0x06;
const ADM_ABORT: u8 = 0x08;
const ADM_SET_FEATURES: u8 = 0x09;
const ADM_GET_FEATURES: u8 = 0x0a;
const ADM_ASYNC_EVENT: u8 = 0x0c;
const ADM_KEEP_ALIVE: u8 = 0x18;
const OPC_FABRICS: u8 = 0x7f;
// I/O opcodes.
const IO_FLUSH: u8 = 0x00;
const IO_WRITE: u8 = 0x01;
const IO_READ: u8 = 0x02;
const IO_WRITE_ZEROES: u8 = 0x08;
const IO_DSM: u8 = 0x09;
// Fabrics command types.
const FCT_PROP_SET: u8 = 0x00;
const FCT_CONNECT: u8 = 0x01;
const FCT_PROP_GET: u8 = 0x04;
// Properties.
const PROP_CAP: u32 = 0x00;
const PROP_VS: u32 = 0x08;
const PROP_CC: u32 = 0x14;
const PROP_CSTS: u32 = 0x1c;
const NVME_VERSION: u32 = 0x0001_0400; // 1.4

// Status codes (generic command status unless noted).
const SC_INVALID_OPCODE: u16 = 0x01;
const SC_INVALID_FIELD: u16 = 0x02;
const SC_DATA_XFER_ERROR: u16 = 0x04;
const SC_INTERNAL: u16 = 0x06;
const SC_INVALID_NS: u16 = 0x0b;
const SC_CMD_SEQ_ERROR: u16 = 0x0c;
const SC_LBA_RANGE: u16 = 0x80;
const SC_CAPACITY_EXCEEDED: u16 = 0x81;
const SCT_GENERIC: u16 = 0;
const SCT_FABRICS: u16 = 1;
const SC_CONNECT_INVALID_PARAM: u16 = 0x82;

static TRACE: LazyLock<bool> = LazyLock::new(|| std::env::var_os("RAIDISH_NVME_TRACE").is_some());

macro_rules! trace {
    ($($arg:tt)*) => { if *TRACE { eprintln!($($arg)*); } };
}

fn le16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
fn le64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}
fn put(b: &mut [u8], at: usize, v: &[u8]) {
    b[at..at + v.len()].copy_from_slice(v);
}
/// Space-padded ASCII field.
fn put_str(b: &mut [u8], at: usize, len: usize, s: &str) {
    let field = &mut b[at..at + len];
    field.fill(b' ');
    let n = s.len().min(len);
    field[..n].copy_from_slice(&s.as_bytes()[..n]);
}
fn cstr(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// A 64-byte submission queue entry.
#[derive(Clone)]
struct Sqe([u8; 64]);

impl Sqe {
    fn opcode(&self) -> u8 {
        self.0[0]
    }
    fn cid(&self) -> u16 {
        le16(&self.0, 2)
    }
    fn nsid(&self) -> u32 {
        le32(&self.0, 4)
    }
    fn fctype(&self) -> u8 {
        self.0[4]
    }
    fn cdw(&self, n: usize) -> u32 {
        le32(&self.0, 40 + (n - 10) * 4)
    }
    /// Length of the (single) SGL data block descriptor.
    fn data_len(&self) -> usize {
        le32(&self.0, 32) as usize
    }
    /// Data-transfer direction from the opcode (fabrics: from fctype).
    fn host_to_ctrl(&self) -> bool {
        let dir = if self.opcode() == OPC_FABRICS {
            self.fctype()
        } else {
            self.opcode()
        };
        dir & 0x1 != 0
    }
    fn slba(&self) -> u64 {
        self.cdw(10) as u64 | (self.cdw(11) as u64) << 32
    }
    fn nlb(&self) -> u64 {
        (self.cdw(12) & 0xffff) as u64 + 1
    }
}

/// What a command produces: completion fields plus optional C2H data.
struct Completion {
    status: u16,
    dw0: u32,
    dw1: u32,
    data: Option<Vec<u8>>,
}

impl Completion {
    fn ok() -> Self {
        Completion {
            status: 0,
            dw0: 0,
            dw1: 0,
            data: None,
        }
    }
    fn with_dw0(dw0: u32) -> Self {
        Completion { dw0, ..Self::ok() }
    }
    fn with_data(data: Vec<u8>) -> Self {
        Completion {
            data: Some(data),
            ..Self::ok()
        }
    }
    fn err(sct: u16, sc: u16) -> Self {
        Completion {
            // DNR: retrying won't help for any error we return.
            status: (sc << 1) | (sct << 9) | (1 << 15),
            ..Self::ok()
        }
    }
    fn generic(sc: u16) -> Self {
        Self::err(SCT_GENERIC, sc)
    }
}

struct CtrlRegs {
    cc: u32,
    csts: u32,
    io_queues: u16,
    kato: u32,
}

struct Controller {
    cntlid: u16,
    export: Arc<Export>,
    regs: Mutex<CtrlRegs>,
}

/// One exported volume: a subsystem with a single namespace.
struct Export {
    volume: String,
    nqn: String,
    serial: String,
    nsze: u64,
    uuid: [u8; 16],
    life: ExportLife,
}

impl Export {
    fn new(volume: &str, size: u64) -> Self {
        let hash = blake3::hash(format!("raidish-ns:{}", volume).as_bytes());
        // SN is 20 bytes and must differ between subsystems: names too long
        // to fit after the prefix are replaced by a hash.
        let serial = if volume.len() <= 12 && volume.is_ascii() {
            format!("RAIDISH-{}", volume)
        } else {
            format!("RAIDISH-{}", &hash.to_hex()[..12])
        };
        Export {
            volume: volume.to_string(),
            nqn: subsystem_nqn(volume),
            serial,
            nsze: size / LBA_SIZE,
            uuid: hash.as_bytes()[..16].try_into().unwrap(),
            life: ExportLife::new(),
        }
    }
}

#[derive(Clone)]
struct QueueInfo {
    qid: u16,
    /// 0-based queue size from Connect.
    sqsize: u16,
    ctrl: Arc<Controller>,
}

struct Target<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    /// By subsystem NQN.
    exports: Mutex<BTreeMap<String, Arc<Export>>>,
    ctrls: Mutex<HashMap<u16, Arc<Controller>>>,
    next_cntlid: AtomicU16,
}

struct Conn<D: BlockDevice> {
    target: Arc<Target<D>>,
    writer: AsyncMutex<OwnedWriteHalf>,
    queue: Mutex<Option<QueueInfo>>,
    /// Hold on the export this queue connected to.
    session: Mutex<Option<ExportSession>>,
    /// Signalled once Connect attaches the queue to an export.
    connected: Notify,
    sqhd: AtomicU32,
    /// PDU data offset for C2H data, honouring the host's HPDA.
    c2h_pdo: usize,
    peer: String,
}

/// An NVMe/TCP target exporting any number of volumes of one file system.
pub struct NvmeTcpServer<D: BlockDevice> {
    target: Arc<Target<D>>,
    bind: String,
    port: u16,
}

/// The NVMe/TCP port.
#[allow(dead_code)]
pub const NVME_PORT: u16 = 4420;

// The multi-export API is driven by the node agent; until it lands only the
// single-export CLI wrapper and the tests use all of it.
#[allow(dead_code)]
impl<D: BlockDevice + Send + Sync + 'static> NvmeTcpServer<D> {
    /// Export the single volume `volume` on `127.0.0.1:port` (the CLI's `nvme`).
    pub async fn new(
        fs: Arc<FileSystem<D>>,
        volume: String,
        port: u16,
    ) -> Result<Self, FileSystemError> {
        let server = Self::with_bind(fs, DEFAULT_BIND, port);
        server.add_export(&volume).await?;
        Ok(server)
    }

    /// A target with no exports that `run` binds to `bind:port`.
    pub fn with_bind(fs: Arc<FileSystem<D>>, bind: impl Into<String>, port: u16) -> Self {
        NvmeTcpServer {
            target: Arc::new(Target {
                fs,
                exports: Mutex::new(BTreeMap::new()),
                ctrls: Mutex::new(HashMap::new()),
                next_cntlid: AtomicU16::new(1),
            }),
            bind: bind.into(),
            port,
        }
    }

    /// Export volume `volume` as subsystem `subsystem_nqn(volume)`.
    pub async fn add_export(&self, volume: &str) -> Result<(), FileSystemError> {
        let nqn = subsystem_nqn(volume);
        if volume.is_empty() || nqn.len() > MAX_NQN || volume.contains('\0') {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume name '{}' does not fit in an NQN",
                volume
            )));
        }
        let size = self.target.fs.open_volume(volume).await?.size_bytes;
        let mut exports = self.target.exports.lock().unwrap();
        if exports.contains_key(&nqn) {
            return Err(FileSystemError::AlreadyExists);
        }
        exports.insert(nqn, Arc::new(Export::new(volume, size)));
        Ok(())
    }

    /// Stop exporting `volume`: new connects are refused, its queues are
    /// closed once their in-flight commands finish, then everything is
    /// committed. Returns false if it wasn't exported.
    pub async fn remove_export(&self, volume: &str) -> Result<bool, FileSystemError> {
        let nqn = subsystem_nqn(volume);
        let Some(export) = self.target.exports.lock().unwrap().remove(&nqn) else {
            return Ok(false);
        };
        export.life.close(&format!("nvme: {}", nqn)).await;
        self.target.fs.sync().await?;
        Ok(true)
    }

    /// Names of the exported volumes, sorted by NQN.
    pub fn exports(&self) -> Vec<String> {
        let exports = self.target.exports.lock().unwrap();
        exports.values().map(|e| e.volume.clone()).collect()
    }

    /// Bind `bind:port` and serve until the listener fails.
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind((self.bind.as_str(), self.port)).await?;
        self.serve(listener).await
    }

    /// Serve connections from an already-bound listener.
    pub async fn serve(&self, listener: TcpListener) -> std::io::Result<()> {
        ensure_sync_loop(&self.target.fs, "nvme");
        for e in self.target.exports.lock().unwrap().values() {
            println!(
                "NVMe/TCP target {} (volume '{}', {} x {} byte LBAs)",
                e.nqn, e.volume, e.nsze, LBA_SIZE
            );
        }
        println!("NVMe/TCP listening on {}", listener.local_addr()?);

        loop {
            let (stream, addr) = listener.accept().await?;
            let target = self.target.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(target, stream, addr.to_string()).await {
                    eprintln!("nvme: {}: {}", addr, e);
                }
            });
        }
    }
}

fn invalid(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into())
}

/// Read one whole PDU (common header included).
async fn read_pdu(rd: &mut OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 8];
    rd.read_exact(&mut hdr).await?;
    let plen = le32(&hdr, 4) as usize;
    let hlen = hdr[2] as usize;
    if !(8..=MAX_PDU).contains(&plen) || !(8..=plen).contains(&hlen) {
        return Err(invalid(format!(
            "bad PDU type {} hlen {} plen {}",
            hdr[0], hlen, plen
        )));
    }
    let mut pdu = vec![0u8; plen];
    pdu[..8].copy_from_slice(&hdr);
    rd.read_exact(&mut pdu[8..]).await?;
    Ok(pdu)
}

/// Data carried by a capsule or H2C data PDU.
fn pdu_data(pdu: &[u8]) -> std::io::Result<&[u8]> {
    let pdo = pdu[3] as usize;
    if pdu[1] & (PDU_FLAG_HDGST | PDU_FLAG_DDGST) != 0 {
        return Err(invalid("digests were not negotiated"));
    }
    if pdo == 0 || pdo >= pdu.len() {
        return Ok(&[]);
    }
    Ok(&pdu[pdo..])
}

struct PendingWrite {
    sqe: Sqe,
    buf: Vec<u8>,
    received: usize,
}

async fn handle_connection<D: BlockDevice + Send + Sync + 'static>(
    target: Arc<Target<D>>,
    stream: TcpStream,
    peer: String,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let (mut rd, mut wr) = stream.into_split();

    // Connection initialisation.
    let ic = read_pdu(&mut rd).await?;
    if ic[0] != PDU_IC_REQ || ic.len() != 128 {
        return Err(invalid("expected ICReq"));
    }
    let hpda = ic[10] as usize;
    let mut resp = [0u8; 128];
    resp[0] = PDU_IC_RESP;
    resp[2] = 128;
    put(&mut resp, 4, &128u32.to_le_bytes());
    // pfv 0, cpda 0, no digests.
    put(&mut resp, 12, &MAX_H2C_DATA.to_le_bytes());
    wr.write_all(&resp).await?;
    trace!(
        "nvme: {}: ICReq hpda={} dgst={:#x} maxr2t={}",
        peer,
        hpda,
        ic[11],
        le32(&ic, 12)
    );

    let align = (hpda + 1) * 4;
    let conn = Arc::new(Conn {
        target,
        writer: AsyncMutex::new(wr),
        queue: Mutex::new(None),
        session: Mutex::new(None),
        connected: Notify::new(),
        sqhd: AtomicU32::new(0),
        c2h_pdo: 24usize.div_ceil(align) * align,
        peer,
    });

    let result = connection_loop(&conn, &mut rd).await;

    // An admin queue going away tears down its controller.
    if let Some(q) = conn.queue.lock().unwrap().as_ref()
        && q.qid == 0
    {
        conn.target.ctrls.lock().unwrap().remove(&q.ctrl.cntlid);
        trace!("nvme: {}: controller {} removed", conn.peer, q.ctrl.cntlid);
    }
    result
}

async fn connection_loop<D: BlockDevice + Send + Sync + 'static>(
    conn: &Arc<Conn<D>>,
    rd: &mut OwnedReadHalf,
) -> std::io::Result<()> {
    let mut pending: HashMap<u16, PendingWrite> = HashMap::new();
    let mut tasks = JoinSet::new();
    // Close signal of the export, once Connect has picked one.
    let mut closed: Option<watch::Receiver<bool>> = None;

    let result = loop {
        while let Some(r) = tasks.try_join_next() {
            r.map_err(std::io::Error::other)??;
        }

        // Read the next PDU, giving up if the export is removed meanwhile.
        // The read itself is not cancel-safe, so it is only dropped then.
        let read = read_pdu(rd);
        tokio::pin!(read);
        let pdu = loop {
            if closed.is_none() {
                closed = conn
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|s| s.close_signal());
            }
            let waiting = closed.is_none();
            tokio::select! {
                r = &mut read => break Some(r),
                _ = export_closed(&mut closed) => break None,
                _ = conn.connected.notified(), if waiting => {}
            }
        };
        let pdu = match pdu {
            Some(Ok(pdu)) => pdu,
            Some(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break Ok(()),
            Some(Err(e)) => break Err(e),
            None => {
                trace!("nvme: {}: export removed, closing queue", conn.peer);
                break Ok(());
            }
        };

        match pdu[0] {
            PDU_CMD => {
                if pdu.len() < 72 {
                    break Err(invalid("short command capsule"));
                }
                let sqe = Sqe(pdu[8..72].try_into().unwrap());
                let inline = pdu_data(&pdu)?;
                let need = if sqe.host_to_ctrl() {
                    sqe.data_len()
                } else {
                    0
                };

                if need > MAX_TRANSFER {
                    send_completion(conn, sqe.cid(), Completion::generic(SC_INVALID_FIELD)).await?;
                } else if need > 0 && inline.is_empty() {
                    // Ask the host for the data; the command runs once it's all here.
                    let ttag = sqe.cid();
                    if pending.contains_key(&ttag) {
                        break Err(invalid(format!("duplicate outstanding cid {}", ttag)));
                    }
                    send_r2t(conn, sqe.cid(), ttag, 0, need as u32).await?;
                    pending.insert(
                        ttag,
                        PendingWrite {
                            sqe,
                            buf: vec![0u8; need],
                            received: 0,
                        },
                    );
                } else if need > 0 && inline.len() != need {
                    send_completion(conn, sqe.cid(), Completion::generic(SC_DATA_XFER_ERROR))
                        .await?;
                } else {
                    let (conn, data) = (conn.clone(), inline.to_vec());
                    tasks.spawn(async move { execute(&conn, sqe, data).await });
                }
            }
            PDU_H2C_DATA => {
                let ttag = le16(&pdu, 10);
                let datao = le32(&pdu, 12) as usize;
                let datal = le32(&pdu, 16) as usize;
                let data = pdu_data(&pdu)?;
                let Some(p) = pending.get_mut(&ttag) else {
                    break Err(invalid(format!("H2C data for unknown ttag {}", ttag)));
                };
                if data.len() != datal
                    || datao + datal > p.buf.len()
                    || datal > MAX_H2C_DATA as usize
                {
                    break Err(invalid("H2C data out of range"));
                }
                p.buf[datao..datao + datal].copy_from_slice(data);
                p.received += datal;
                if p.received >= p.buf.len() {
                    let p = pending.remove(&ttag).unwrap();
                    let conn = conn.clone();
                    tasks.spawn(async move { execute(&conn, p.sqe, p.buf).await });
                }
            }
            PDU_H2C_TERM => {
                trace!(
                    "nvme: {}: host terminated connection (fes {:#x})",
                    conn.peer,
                    le16(&pdu, 8)
                );
                break Ok(());
            }
            other => break Err(invalid(format!("unexpected PDU type {:#x}", other))),
        }
    };

    // Let in-flight commands finish (their replies may fail if the socket is gone).
    while let Some(r) = tasks.join_next().await {
        let _ = r;
    }
    result
}

async fn send_r2t<D: BlockDevice>(
    conn: &Conn<D>,
    cid: u16,
    ttag: u16,
    offset: u32,
    len: u32,
) -> std::io::Result<()> {
    let mut pdu = [0u8; 24];
    pdu[0] = PDU_R2T;
    pdu[2] = 24;
    put(&mut pdu, 4, &24u32.to_le_bytes());
    put(&mut pdu, 8, &cid.to_le_bytes());
    put(&mut pdu, 10, &ttag.to_le_bytes());
    put(&mut pdu, 12, &offset.to_le_bytes());
    put(&mut pdu, 16, &len.to_le_bytes());
    conn.writer.lock().await.write_all(&pdu).await
}

/// Write any C2H data followed by the response capsule, in one piece.
async fn send_completion<D: BlockDevice>(
    conn: &Conn<D>,
    cid: u16,
    c: Completion,
) -> std::io::Result<()> {
    let mut out = Vec::new();
    if let Some(data) = &c.data {
        let chunks = data.chunks(C2H_CHUNK).count();
        for (i, chunk) in data.chunks(C2H_CHUNK).enumerate() {
            let pdo = conn.c2h_pdo;
            let start = out.len();
            out.resize(start + pdo, 0);
            let h = &mut out[start..];
            h[0] = PDU_C2H_DATA;
            h[1] = if i + 1 == chunks { PDU_FLAG_LAST } else { 0 };
            h[2] = 24;
            h[3] = pdo as u8;
            put(h, 4, &((pdo + chunk.len()) as u32).to_le_bytes());
            put(h, 8, &cid.to_le_bytes());
            put(h, 12, &((i * C2H_CHUNK) as u32).to_le_bytes());
            put(h, 16, &(chunk.len() as u32).to_le_bytes());
            out.extend_from_slice(chunk);
        }
    }

    let (sqid, sqsize) = match conn.queue.lock().unwrap().as_ref() {
        Some(q) => (q.qid, q.sqsize as u32 + 1),
        None => (0, QUEUE_ENTRIES as u32),
    };
    let sqhd = (conn.sqhd.fetch_add(1, Ordering::Relaxed) + 1) % sqsize;

    let start = out.len();
    out.resize(start + 24, 0);
    let r = &mut out[start..];
    r[0] = PDU_RSP;
    r[2] = 24;
    put(r, 4, &24u32.to_le_bytes());
    put(r, 8, &c.dw0.to_le_bytes());
    put(r, 12, &c.dw1.to_le_bytes());
    put(r, 16, &(sqhd as u16).to_le_bytes());
    put(r, 18, &sqid.to_le_bytes());
    put(r, 20, &cid.to_le_bytes());
    put(r, 22, &c.status.to_le_bytes());

    conn.writer.lock().await.write_all(&out).await
}

async fn execute<D: BlockDevice + Send + Sync + 'static>(
    conn: &Conn<D>,
    sqe: Sqe,
    data: Vec<u8>,
) -> std::io::Result<()> {
    let queue = conn.queue.lock().unwrap().clone();
    let completion = if sqe.opcode() == OPC_FABRICS {
        fabrics(conn, &queue, &sqe, &data)
    } else {
        match &queue {
            None => Some(Completion::generic(SC_CMD_SEQ_ERROR)),
            Some(q) if q.qid == 0 => admin(conn, q, &sqe, &data),
            Some(q) => Some(io(conn, q, &sqe, &data).await),
        }
    };

    let Some(c) = completion else {
        // Async Event Requests stay outstanding until there is an event.
        return Ok(());
    };
    trace!(
        "nvme: {} q{} cid={} opc={:#04x} cdw10={:#x}{} -> status {:#06x}",
        conn.peer,
        queue.as_ref().map_or(-1, |q| q.qid as i32),
        sqe.cid(),
        sqe.opcode(),
        sqe.cdw(10),
        if sqe.opcode() == OPC_FABRICS {
            format!(" fctype={:#04x}", sqe.fctype())
        } else {
            String::new()
        },
        c.status
    );
    send_completion(conn, sqe.cid(), c).await
}

fn fabrics<D: BlockDevice>(
    conn: &Conn<D>,
    queue: &Option<QueueInfo>,
    sqe: &Sqe,
    data: &[u8],
) -> Option<Completion> {
    let target = &conn.target;
    Some(match sqe.fctype() {
        FCT_CONNECT => {
            if queue.is_some() {
                return Some(Completion::generic(SC_CMD_SEQ_ERROR));
            }
            if data.len() < 1024 {
                return Some(Completion::generic(SC_INVALID_FIELD));
            }
            let qid = le16(&sqe.0, 42);
            let sqsize = le16(&sqe.0, 44);
            let kato = le32(&sqe.0, 48);
            let cntlid = le16(data, 16);
            let subnqn = cstr(&data[256..512]);
            let hostnqn = cstr(&data[512..768]);
            trace!(
                "nvme: {}: connect qid={} sqsize={} cntlid={:#x} subnqn={} hostnqn={}",
                conn.peer, qid, sqsize, cntlid, subnqn, hostnqn
            );

            // Invalid-parameter responses say which field: bit 16 = in data, bits 15:0 = offset.
            let bad = |in_data: bool, offset: u32| Completion {
                dw0: ((in_data as u32) << 16) | offset,
                ..Completion::err(SCT_FABRICS, SC_CONNECT_INVALID_PARAM)
            };
            let Some(export) = target.exports.lock().unwrap().get(&subnqn).cloned() else {
                return Some(bad(true, 256));
            };
            if sqsize == 0 || sqsize >= QUEUE_ENTRIES {
                return Some(bad(false, 44));
            }

            // Refused once the export is being removed.
            let Some(session) = export.life.join() else {
                return Some(bad(true, 256));
            };

            let ctrl = if qid == 0 {
                if cntlid != 0xffff && cntlid != 0xfffe {
                    return Some(bad(true, 16));
                }
                let id = target.next_cntlid.fetch_add(1, Ordering::Relaxed);
                let ctrl = Arc::new(Controller {
                    cntlid: id,
                    export: export.clone(),
                    regs: Mutex::new(CtrlRegs {
                        cc: 0,
                        csts: 0,
                        io_queues: 0,
                        kato,
                    }),
                });
                target.ctrls.lock().unwrap().insert(id, ctrl.clone());
                ctrl
            } else {
                let Some(ctrl) = target.ctrls.lock().unwrap().get(&cntlid).cloned() else {
                    return Some(bad(true, 16));
                };
                if !Arc::ptr_eq(&ctrl.export, &export) {
                    // That controller belongs to another subsystem.
                    return Some(bad(true, 16));
                }
                let regs = ctrl.regs.lock().unwrap();
                if regs.csts & 1 == 0 || qid > regs.io_queues.max(1) {
                    drop(regs);
                    return Some(bad(false, 42));
                }
                drop(regs);
                ctrl
            };

            let cntlid = ctrl.cntlid;
            *conn.queue.lock().unwrap() = Some(QueueInfo { qid, sqsize, ctrl });
            *conn.session.lock().unwrap() = Some(session);
            conn.connected.notify_one();
            conn.sqhd.store(0, Ordering::Relaxed);
            Completion::with_dw0(cntlid as u32)
        }
        FCT_PROP_GET | FCT_PROP_SET => {
            let Some(q) = queue.as_ref().filter(|q| q.qid == 0) else {
                return Some(Completion::generic(SC_CMD_SEQ_ERROR));
            };
            let offset = le32(&sqe.0, 44);
            let mut regs = q.ctrl.regs.lock().unwrap();
            if sqe.fctype() == FCT_PROP_GET {
                let value: u64 = match offset {
                    PROP_CAP => {
                        (QUEUE_ENTRIES as u64 - 1) // MQES
                            | 1 << 16 // CQR
                            | 30 << 24 // TO: 15 s
                            | 1 << 37 // CSS: NVM command set
                    }
                    PROP_VS => NVME_VERSION as u64,
                    PROP_CC => regs.cc as u64,
                    PROP_CSTS => regs.csts as u64,
                    _ => return Some(Completion::generic(SC_INVALID_FIELD)),
                };
                Completion {
                    dw0: value as u32,
                    dw1: (value >> 32) as u32,
                    ..Completion::ok()
                }
            } else {
                if offset != PROP_CC {
                    return Some(Completion::generic(SC_INVALID_FIELD));
                }
                let cc = le32(&sqe.0, 48);
                let was_enabled = regs.cc & 1 != 0;
                if cc & 1 != 0 && !was_enabled {
                    regs.csts = 1; // RDY, shutdown status cleared
                } else if cc & 1 == 0 {
                    regs.csts &= !1;
                }
                if (cc >> 14) & 0x3 != 0 {
                    // Shutdown requested: nothing to drain beyond a commit,
                    // which the host follows with FLUSH anyway. Report complete.
                    regs.csts = (regs.csts & !0xc) | (0x2 << 2);
                }
                regs.cc = cc;
                trace!(
                    "nvme: {}: CC={:#x} CSTS={:#x}",
                    conn.peer, regs.cc, regs.csts
                );
                Completion::ok()
            }
        }
        _ => Completion::generic(SC_INVALID_OPCODE),
    })
}

fn admin<D: BlockDevice>(
    _conn: &Conn<D>,
    q: &QueueInfo,
    sqe: &Sqe,
    _data: &[u8],
) -> Option<Completion> {
    let export = &q.ctrl.export;
    Some(match sqe.opcode() {
        ADM_IDENTIFY => {
            let cns = sqe.cdw(10) & 0xff;
            let nsid = sqe.nsid();
            let page = match cns {
                0x00 if nsid == 1 => identify_ns(export),
                0x00 => return Some(Completion::generic(SC_INVALID_NS)),
                0x01 => identify_ctrl(export, q.ctrl.cntlid),
                0x02 => {
                    let mut list = vec![0u8; 4096];
                    if nsid < 1 {
                        put(&mut list, 0, &1u32.to_le_bytes());
                    }
                    list
                }
                0x03 if nsid == 1 => {
                    let mut list = vec![0u8; 4096];
                    list[0] = 0x03; // NIDT: UUID
                    list[1] = 16;
                    put(&mut list, 4, &export.uuid);
                    list
                }
                _ => return Some(Completion::generic(SC_INVALID_FIELD)),
            };
            Completion::with_data(page)
        }
        ADM_GET_LOG_PAGE => {
            let lid = sqe.cdw(10) & 0xff;
            let numd = ((sqe.cdw(10) >> 16) | ((sqe.cdw(11) & 0xffff) << 16)) as usize + 1;
            let len = (numd * 4).min(sqe.data_len());
            match lid {
                // Error information, SMART / health, firmware slots: all empty.
                0x01..=0x03 => Completion::with_data(vec![0u8; len]),
                _ => Completion::generic(SC_INVALID_FIELD),
            }
        }
        ADM_SET_FEATURES | ADM_GET_FEATURES => {
            let fid = sqe.cdw(10) & 0xff;
            let set = sqe.opcode() == ADM_SET_FEATURES;
            let mut regs = q.ctrl.regs.lock().unwrap();
            match fid {
                0x07 => {
                    // Number of queues (0-based in both directions).
                    if set {
                        let nsqr = (sqe.cdw(11) & 0xffff) as u16 + 1;
                        let ncqr = (sqe.cdw(11) >> 16) as u16 + 1;
                        regs.io_queues = nsqr.min(ncqr).min(MAX_IO_QUEUES);
                    }
                    let n = regs.io_queues.max(1) as u32 - 1;
                    Completion::with_dw0(n << 16 | n)
                }
                0x06 => Completion::with_dw0(1), // volatile write cache: enabled
                0x0b => Completion::ok(),        // async event configuration
                0x0f => {
                    if set {
                        regs.kato = sqe.cdw(11);
                    }
                    Completion::with_dw0(regs.kato)
                }
                _ => Completion::generic(SC_INVALID_FIELD),
            }
        }
        ADM_ASYNC_EVENT => return None,
        ADM_KEEP_ALIVE => Completion::ok(),
        ADM_ABORT => Completion::with_dw0(1), // command not aborted
        _ => Completion::generic(SC_INVALID_OPCODE),
    })
}

async fn io<D: BlockDevice + Send + Sync + 'static>(
    conn: &Conn<D>,
    q: &QueueInfo,
    sqe: &Sqe,
    data: &[u8],
) -> Completion {
    if sqe.nsid() != 1 {
        return Completion::generic(SC_INVALID_NS);
    }
    let export = &q.ctrl.export;
    let fs = &conn.target.fs;
    let name = &export.volume;

    let range = |slba: u64, nlb: u64| -> Option<(u64, usize)> {
        let end = slba.checked_add(nlb)?;
        (end <= export.nsze).then_some((slba * LBA_SIZE, (nlb * LBA_SIZE) as usize))
    };
    let fail = |what: &str, e: crate::fs::FileSystemError| {
        eprintln!("nvme: {} {} failed: {}", what, sqe.cid(), e);
        match e {
            crate::fs::FileSystemError::UnableToAllocateBlocks(_) => {
                Completion::generic(SC_CAPACITY_EXCEEDED)
            }
            _ => Completion::generic(SC_INTERNAL),
        }
    };

    match sqe.opcode() {
        IO_READ | IO_WRITE | IO_WRITE_ZEROES => {
            let Some((offset, len)) = range(sqe.slba(), sqe.nlb()) else {
                return Completion::generic(SC_LBA_RANGE);
            };
            match sqe.opcode() {
                IO_READ => {
                    if len > MAX_TRANSFER || len > sqe.data_len() {
                        return Completion::generic(SC_INVALID_FIELD);
                    }
                    let mut buf = vec![0u8; len];
                    match fs.block_read(name, offset, &mut buf).await {
                        Ok(_) => Completion::with_data(buf),
                        Err(e) => fail("read", e),
                    }
                }
                IO_WRITE => {
                    if data.len() != len {
                        return Completion::generic(SC_DATA_XFER_ERROR);
                    }
                    match fs.block_write(name, offset, data).await {
                        Ok(_) => Completion::ok(),
                        Err(e) => fail("write", e),
                    }
                }
                _ => match fs.block_zero(name, offset, len).await {
                    Ok(_) => Completion::ok(),
                    Err(e) => fail("write zeroes", e),
                },
            }
        }
        // Through the intent log on a writer (see `crate::zil`).
        IO_FLUSH => match fs.flush().await {
            Ok(()) => Completion::ok(),
            Err(e) => fail("flush", e),
        },
        IO_DSM => {
            let ranges = (sqe.cdw(10) & 0xff) as usize + 1;
            let deallocate = sqe.cdw(11) & (1 << 2) != 0;
            if data.len() < ranges * 16 {
                return Completion::generic(SC_DATA_XFER_ERROR);
            }
            if deallocate {
                for r in data.chunks(16).take(ranges) {
                    let nlb = le32(r, 4) as u64;
                    if nlb == 0 {
                        continue;
                    }
                    let Some((offset, len)) = range(le64(r, 8), nlb) else {
                        return Completion::generic(SC_LBA_RANGE);
                    };
                    if let Err(e) = fs.block_zero(name, offset, len).await {
                        return fail("deallocate", e);
                    }
                }
            }
            Completion::ok()
        }
        _ => Completion::generic(SC_INVALID_OPCODE),
    }
}

fn identify_ctrl(export: &Export, cntlid: u16) -> Vec<u8> {
    let mut id = vec![0u8; 4096];
    put_str(&mut id, 4, 20, &export.serial); // SN
    put_str(&mut id, 24, 40, "RAIDISH NVMe-TCP Volume"); // MN
    put_str(&mut id, 64, 8, env!("CARGO_PKG_VERSION")); // FR
    id[77] = MDTS;
    put(&mut id, 78, &cntlid.to_le_bytes());
    put(&mut id, 80, &NVME_VERSION.to_le_bytes());
    id[111] = 1; // CNTRLTYPE: I/O controller
    id[258] = 3; // ACL (0-based)
    id[259] = 3; // AERL (0-based)
    id[260] = 0x01; // FRMW: slot 1 read-only
    put(&mut id, 320, &100u16.to_le_bytes()); // KAS: 10 s granularity units of 100 ms
    id[512] = 0x66; // SQES: 64 bytes
    id[513] = 0x44; // CQES: 16 bytes
    put(&mut id, 514, &QUEUE_ENTRIES.to_le_bytes()); // MAXCMD
    put(&mut id, 516, &1u32.to_le_bytes()); // NN
    put(&mut id, 520, &((1u16 << 2) | (1 << 3)).to_le_bytes()); // ONCS: DSM, Write Zeroes
    id[525] = 0x01; // VWC present
    put(&mut id, 536, &((1u32 << 0) | (1 << 20)).to_le_bytes()); // SGLS: SGLs, offsets
    put(&mut id, 768, export.nqn.as_bytes()); // SUBNQN (NUL padded)
    // Fabrics: in-capsule size in 16-byte units, response size, no ICDOFF.
    put(
        &mut id,
        1792,
        &(((64 + INLINE_DATA_SIZE) / 16) as u32).to_le_bytes(),
    ); // IOCCSZ
    put(&mut id, 1796, &1u32.to_le_bytes()); // IORCSZ
    id[1803] = 1; // MSDBD
    id
}

fn identify_ns(export: &Export) -> Vec<u8> {
    let mut id = vec![0u8; 4096];
    put(&mut id, 0, &export.nsze.to_le_bytes()); // NSZE
    put(&mut id, 8, &export.nsze.to_le_bytes()); // NCAP
    put(&mut id, 16, &export.nsze.to_le_bytes()); // NUSE
    id[24] = 0x01; // NSFEAT: thin provisioned
    id[25] = 0; // NLBAF (0-based): one format
    id[26] = 0; // FLBAS: format 0
    id[33] = 0x09; // DLFEAT: deallocated reads zeros, Write Zeroes may deallocate
    put(&mut id, 104, &export.uuid); // NGUID
    put(&mut id, 128, &((LBA_SHIFT as u32) << 16).to_le_bytes()); // LBAF0: 4 KiB, no metadata
    id
}

#[cfg(test)]
#[path = "tests/nvme_tests.rs"]
mod tests;
