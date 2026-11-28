//! Drives the NBD server over a real socket with a minimal client.

use super::*;
use crate::export::BlockExporter;
use crate::test_util::DEFAULT_VOLBLOCKSIZE;
use crate::test_util::TestPool;
use crate::test_util::ds;
use raidish_core::role::{Committer, CommitterFs};
use std::time::Duration;
use tokio::net::TcpStream;

const MB: u64 = 1 << 20;

struct Client {
    s: TcpStream,
    next: u64,
}

/// Read the server greeting and send our flags.
async fn greet(addr: std::net::SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    assert_eq!(s.read_u64().await.unwrap(), NBD_INIT_MAGIC);
    assert_eq!(s.read_u64().await.unwrap(), NBD_OPTS_MAGIC);
    let _server_flags = s.read_u16().await.unwrap();
    s.write_u32((NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES) as u32)
        .await
        .unwrap();
    s
}

async fn send_opt(s: &mut TcpStream, option: u32, data: &[u8]) {
    s.write_u64(NBD_OPTS_MAGIC).await.unwrap();
    s.write_u32(option).await.unwrap();
    s.write_u32(data.len() as u32).await.unwrap();
    s.write_all(data).await.unwrap();
}

/// One option reply: (kind, data).
async fn read_opt_reply(s: &mut TcpStream, option: u32) -> (u32, Vec<u8>) {
    assert_eq!(s.read_u64().await.unwrap(), NBD_REP_MAGIC);
    assert_eq!(s.read_u32().await.unwrap(), option);
    let kind = s.read_u32().await.unwrap();
    let len = s.read_u32().await.unwrap();
    let mut data = vec![0u8; len as usize];
    s.read_exact(&mut data).await.unwrap();
    (kind, data)
}

/// NBD_OPT_GO (or INFO) for `name`: the export size, or the error reply.
async fn go(s: &mut TcpStream, option: u32, name: &str) -> Result<u64, u32> {
    let mut data = Vec::new();
    data.extend_from_slice(&(name.len() as u32).to_be_bytes());
    data.extend_from_slice(name.as_bytes());
    data.extend_from_slice(&0u16.to_be_bytes()); // no info requests
    send_opt(s, option, &data).await;
    let mut size = 0;
    loop {
        match read_opt_reply(s, option).await {
            (NBD_REP_INFO, data) => {
                size = u64::from_be_bytes(data[2..10].try_into().unwrap());
                let flags = u16::from_be_bytes(data[10..12].try_into().unwrap());
                assert_eq!(flags, TRANSMISSION_FLAGS);
            }
            (NBD_REP_ACK, _) => return Ok(size),
            (err, _) => return Err(err),
        }
    }
}

impl Client {
    /// Fixed-newstyle handshake with NBD_OPT_GO and an empty export name
    /// (the default export); returns the export size.
    async fn connect(addr: std::net::SocketAddr) -> (Self, u64) {
        Self::connect_to(addr, "").await.unwrap()
    }

    /// Handshake with NBD_OPT_GO for export `name`.
    async fn connect_to(addr: std::net::SocketAddr, name: &str) -> Result<(Self, u64), u32> {
        let mut s = greet(addr).await;
        let size = go(&mut s, NBD_OPT_GO, name).await?;
        Ok((Client { s, next: 1 }, size))
    }

    /// Send one request and wait for its reply. Returns (error, read data).
    async fn cmd(&mut self, cmd: u16, offset: u64, length: u32, data: &[u8]) -> (u32, Vec<u8>) {
        let handle = self.next;
        self.next += 1;
        let mut req = Vec::new();
        req.extend_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        req.extend_from_slice(&0u16.to_be_bytes());
        req.extend_from_slice(&cmd.to_be_bytes());
        req.extend_from_slice(&handle.to_be_bytes());
        req.extend_from_slice(&offset.to_be_bytes());
        req.extend_from_slice(&length.to_be_bytes());
        req.extend_from_slice(data);
        self.s.write_all(&req).await.unwrap();

        assert_eq!(self.s.read_u32().await.unwrap(), NBD_REPLY_MAGIC);
        let error = self.s.read_u32().await.unwrap();
        assert_eq!(self.s.read_u64().await.unwrap(), handle);
        let mut out = Vec::new();
        if cmd == NBD_CMD_READ && error == 0 {
            out = vec![0u8; length as usize];
            self.s.read_exact(&mut out).await.unwrap();
        }
        (error, out)
    }
}

type Fs = Arc<CommitterFs<crate::vdev::Pool>>;

/// Serve `volumes` (name, size) from one NBD server on an ephemeral port.
/// The exports `serve_many` made, by volume: each is up while held.
type Held = std::collections::BTreeMap<String, crate::export::ExportHandle>;

async fn serve_many(
    pool: &TestPool,
    volumes: &[(&str, u64)],
) -> (
    Fs,
    Arc<NbdServer<crate::vdev::Pool, Committer>>,
    std::net::SocketAddr,
    Held,
) {
    let fs = Arc::new(pool.format().await);
    let server = Arc::new(NbdServer::with_bind(fs.clone(), DEFAULT_BIND, 0));
    let mut held = Held::new();
    for &(name, size) in volumes {
        let name = &ds(name);
        fs.create_block(name, size, DEFAULT_VOLBLOCKSIZE)
            .await
            .unwrap();
        held.insert(name.to_string(), server.add_export(name).await.unwrap());
    }
    let listener = TcpListener::bind((DEFAULT_BIND, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = server.clone();
    tokio::spawn(async move { s.serve(listener).await });
    (fs, server, addr, held)
}

async fn serve(pool: &TestPool, size: u64) -> (Fs, std::net::SocketAddr) {
    let (fs, _server, addr, held) = serve_many(pool, &[("vol", size)]).await;
    // Up for the rest of the test (dropped, it would be taken down).
    std::mem::forget(held);
    (fs, addr)
}

/// The server hangs up on `s` (EOF, or a reset) within a few seconds.
async fn assert_hung_up(s: &mut TcpStream) {
    let mut b = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(10), s.read(&mut b))
        .await
        .expect("server should close the connection");
    assert!(matches!(r, Ok(0) | Err(_)), "expected EOF, got {:?}", r);
}

#[tokio::test]
async fn two_exports_on_one_server() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) =
        serve_many(&pool, &[("alpha", 16 * MB), ("beta", 32 * MB)]).await;
    assert_eq!(server.exports(), vec!["alpha", "beta"]);

    // Each name reaches its own volume.
    let (mut a, size) = Client::connect_to(addr, "alpha").await.unwrap();
    assert_eq!(size, 16 * MB);
    let (mut b, size) = Client::connect_to(addr, "beta").await.unwrap();
    assert_eq!(size, 32 * MB);
    let (da, db) = (vec![0xA1u8; 8192], vec![0xB2u8; 8192]);
    assert_eq!(a.cmd(NBD_CMD_WRITE, 4096, 8192, &da).await.0, 0);
    assert_eq!(b.cmd(NBD_CMD_WRITE, 4096, 8192, &db).await.0, 0);
    assert!(a.cmd(NBD_CMD_READ, 4096, 8192, &[]).await.1 == da);
    assert!(b.cmd(NBD_CMD_READ, 4096, 8192, &[]).await.1 == db);
    let mut buf = vec![0u8; 8192];
    fs.block_read(&ds("alpha"), 4096, &mut buf).await.unwrap();
    assert!(buf == da);
    fs.block_read(&ds("beta"), 4096, &mut buf).await.unwrap();
    assert!(buf == db);

    // Unknown names, and the empty name with more than one export, are
    // refused without ending the haggling.
    let mut s = greet(addr).await;
    assert_eq!(
        go(&mut s, NBD_OPT_GO, "gamma").await,
        Err(NBD_REP_ERR_UNKNOWN)
    );
    assert_eq!(go(&mut s, NBD_OPT_GO, "").await, Err(NBD_REP_ERR_UNKNOWN));
    assert_eq!(go(&mut s, NBD_OPT_INFO, "beta").await, Ok(32 * MB));
    send_opt(&mut s, NBD_OPT_GO, &[0, 0, 0, 9, b'x']).await; // name runs past the payload
    assert_eq!(
        read_opt_reply(&mut s, NBD_OPT_GO).await.0,
        NBD_REP_ERR_INVALID
    );

    // NBD_OPT_LIST names every export.
    send_opt(&mut s, NBD_OPT_LIST, &[]).await;
    let mut names = Vec::new();
    loop {
        match read_opt_reply(&mut s, NBD_OPT_LIST).await {
            (NBD_REP_SERVER, d) => {
                let len = u32::from_be_bytes(d[..4].try_into().unwrap()) as usize;
                names.push(String::from_utf8(d[4..4 + len].to_vec()).unwrap());
            }
            (NBD_REP_ACK, _) => break,
            (other, _) => panic!("unexpected list reply {other:#x}"),
        }
    }
    assert_eq!(names, vec!["alpha", "beta"]);

    // Old-style NBD_OPT_EXPORT_NAME by name, then the same connection serves it.
    let mut s = greet(addr).await;
    send_opt(&mut s, NBD_OPT_EXPORT_NAME, b"beta").await;
    assert_eq!(s.read_u64().await.unwrap(), 32 * MB);
    assert_eq!(s.read_u16().await.unwrap(), TRANSMISSION_FLAGS);
    let mut c = Client { s, next: 1 };
    assert!(c.cmd(NBD_CMD_READ, 4096, 8192, &[]).await.1 == db);

    // ... and an unknown one just hangs up (the option has no error reply).
    let mut s = greet(addr).await;
    send_opt(&mut s, NBD_OPT_EXPORT_NAME, b"gamma").await;
    assert_hung_up(&mut s).await;
}

#[tokio::test]
async fn add_export_validation() {
    let pool = TestPool::new().await;
    let (_fs, server, _addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    assert!(matches!(
        server.add_export(&ds("vol")).await,
        Err(FileSystemError::AlreadyExists)
    ));
    assert!(server.add_export(&ds("missing")).await.is_err());
    // An empty name doesn't get this far: it doesn't parse.
    assert!(crate::types::DatasetName::try_from("").is_err());
    assert_eq!(server.exports(), vec!["vol"]);
}

/// An export whose handle is dropped (not closed) is still taken down
/// properly, in the background: refused to new clients at once, but kept in
/// the table, so the volume can't be exported again, until its connected
/// client has drained and what it wrote is committed.
#[tokio::test]
async fn a_dropped_export_drains_before_it_can_come_back() {
    let pool = TestPool::new().await;
    let (fs, server, addr, mut held) = serve_many(&pool, &[("alpha", 16 * MB)]).await;
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    let data = vec![0x3Cu8; 4096];
    assert_eq!(a.cmd(NBD_CMD_WRITE, 0, 4096, &data).await.0, 0);
    assert!(fs.has_dirty_datasets(), "write not yet committed");

    drop(held.remove("alpha"));
    // Still listed while it drains: a second export would write alongside
    // the client that hasn't gone yet.
    assert!(matches!(
        server.add_export(&ds("alpha")).await,
        Err(FileSystemError::AlreadyExists)
    ));
    assert_eq!(
        Client::connect_to(addr, "alpha").await.err(),
        Some(NBD_REP_ERR_UNKNOWN),
        "a new client got in while it drained"
    );
    assert_hung_up(&mut a.s).await;
    let start = std::time::Instant::now();
    while !server.exports().is_empty() {
        assert!(start.elapsed() < Duration::from_secs(10), "never unlisted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!fs.has_dirty_datasets(), "the background close commits");
    let _again = server.add_export(&ds("alpha")).await.unwrap();
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    assert!(a.cmd(NBD_CMD_READ, 0, 4096, &[]).await.1 == data);
}

/// A `close` given up on partway (its future dropped, a timeout) still
/// takes the export down: the handle's drop finishes it in the background,
/// and the volume can be exported again once it has.
#[tokio::test]
async fn a_cancelled_close_still_takes_the_export_down() {
    let pool = TestPool::new().await;
    let (fs, server, addr, mut held) = serve_many(&pool, &[("alpha", 16 * MB)]).await;
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    let data = vec![0x6Du8; 4096];
    assert_eq!(a.cmd(NBD_CMD_WRITE, 0, 4096, &data).await.0, 0);

    // Polled once (shut, then waiting on the connected client), then dropped.
    let close = held.remove("alpha").unwrap().close();
    assert!(tokio::time::timeout(Duration::ZERO, close).await.is_err());
    assert_hung_up(&mut a.s).await;
    let start = std::time::Instant::now();
    while !server.exports().is_empty() {
        assert!(start.elapsed() < Duration::from_secs(10), "never unlisted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!fs.has_dirty_datasets(), "the background close commits");
    let _again = server.add_export(&ds("alpha")).await.unwrap();
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    assert!(a.cmd(NBD_CMD_READ, 0, 4096, &[]).await.1 == data);
}

#[tokio::test]
async fn remove_export_flushes_and_closes_its_connections() {
    let pool = TestPool::new().await;
    let (fs, server, addr, mut held) =
        serve_many(&pool, &[("alpha", 16 * MB), ("beta", 16 * MB)]).await;
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    let (mut b, _) = Client::connect_to(addr, "beta").await.unwrap();
    let data = vec![0x5Au8; 4096];
    assert_eq!(a.cmd(NBD_CMD_WRITE, 0, 4096, &data).await.0, 0);
    assert!(fs.has_dirty_datasets(), "write not yet committed");

    held.remove("alpha").unwrap().close().await.unwrap();
    assert!(!fs.has_dirty_datasets(), "removal commits");
    assert_hung_up(&mut a.s).await;
    assert_eq!(server.exports(), vec!["beta"]);

    // Gone for new clients; the other export is untouched.
    assert_eq!(
        Client::connect_to(addr, "alpha").await.err(),
        Some(NBD_REP_ERR_UNKNOWN)
    );
    assert_eq!(b.cmd(NBD_CMD_WRITE, 0, 4096, &data).await.0, 0);
    assert!(b.cmd(NBD_CMD_READ, 0, 4096, &[]).await.1 == data);
    // With one export left the empty name picks it.
    let (_, size) = Client::connect(addr).await;
    assert_eq!(size, 16 * MB);

    // The committed write survives a reload, and the volume can come back.
    let mut buf = vec![0u8; 4096];
    pool.load()
        .await
        .block_read(&ds("alpha"), 0, &mut buf)
        .await
        .unwrap();
    assert!(buf == data);
    let _again = server.add_export(&ds("alpha")).await.unwrap();
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    assert!(a.cmd(NBD_CMD_READ, 0, 4096, &[]).await.1 == data);
}

#[tokio::test]
async fn read_write_flush_round_trip() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 16 * MB).await;
    let (mut c, size) = Client::connect(addr).await;
    assert_eq!(size, 16 * MB);

    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    assert_eq!(
        c.cmd(NBD_CMD_WRITE, 12_345, data.len() as u32, &data)
            .await
            .0,
        0
    );
    assert_eq!(c.cmd(NBD_CMD_FLUSH, 0, 0, &[]).await.0, 0);
    let (err, got) = c.cmd(NBD_CMD_READ, 12_345, data.len() as u32, &[]).await;
    assert_eq!(err, 0);
    assert!(got == data);
}

/// Regression: the kernel issues discards far larger than any payload limit
/// (fstrim of a mostly-free fs sends ~GiB ranges). They must not be rejected.
#[tokio::test]
async fn trim_and_write_zeroes_accept_huge_ranges() {
    let pool = TestPool::new().await;
    let (fs, addr) = serve(&pool, 256 * MB).await;
    let (mut c, _) = Client::connect(addr).await;

    let data = vec![0xABu8; MB as usize];
    for off in [0, 100 * MB, 200 * MB] {
        assert_eq!(c.cmd(NBD_CMD_WRITE, off, MB as u32, &data).await.0, 0);
    }
    assert_eq!(c.cmd(NBD_CMD_FLUSH, 0, 0, &[]).await.0, 0);

    // 150 MiB TRIM over [0, 150) and 40 MiB WRITE_ZEROES over [150, 190):
    // both larger than MAX_REQUEST_BYTES; the write at 200 MiB is untouched.
    assert!(40 * MB > MAX_REQUEST_BYTES as u64);
    assert_eq!(c.cmd(NBD_CMD_TRIM, 0, (150 * MB) as u32, &[]).await.0, 0);
    assert_eq!(
        c.cmd(NBD_CMD_WRITE_ZEROES, 150 * MB, (40 * MB) as u32, &[])
            .await
            .0,
        0
    );
    assert_eq!(c.cmd(NBD_CMD_FLUSH, 0, 0, &[]).await.0, 0);

    for off in [0, 100 * MB] {
        let (err, got) = c.cmd(NBD_CMD_READ, off, MB as u32, &[]).await;
        assert_eq!(err, 0);
        assert!(got.iter().all(|&b| b == 0), "offset {off} should be zeroed");
    }
    let (_, got) = c.cmd(NBD_CMD_READ, 200 * MB, MB as u32, &[]).await;
    assert!(got == data, "data past the trimmed range must survive");

    // The trimmed records were released (only the last write remains).
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
    let vol = fs.open_dataset(&ds("vol")).await.unwrap();
    let used: u64 = fs
        .tree_extents(&vol.obj.root())
        .await
        .unwrap()
        .iter()
        .map(|e| e.len)
        .sum();
    assert!(
        used * 4096 < 2 * MB,
        "volume still holds {} bytes",
        used * 4096
    );
}

#[tokio::test]
async fn oversized_read_is_rejected_not_fatal() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 128 * MB).await;
    let (mut c, _) = Client::connect(addr).await;
    assert_eq!(
        c.cmd(NBD_CMD_READ, 0, MAX_REQUEST_BYTES + 4096, &[])
            .await
            .0,
        EINVAL
    );
    // Connection still usable.
    assert_eq!(c.cmd(NBD_CMD_READ, 0, 4096, &[]).await.0, 0);
}

#[tokio::test]
async fn out_of_space_is_reported_as_enospc() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 1024 * MB).await;
    let (mut c, _) = Client::connect(addr).await;
    let chunk: Vec<u8> = (0..MB as u32).map(|i| (i % 253) as u8 + 1).collect();
    for i in 0..150u64 {
        assert_eq!(c.cmd(NBD_CMD_WRITE, i * MB, MB as u32, &chunk).await.0, 0);
    }
    assert_eq!(c.cmd(NBD_CMD_FLUSH, 0, 0, &[]).await.0, ENOSPC);
    // Still serving.
    let (err, got) = c.cmd(NBD_CMD_READ, 149 * MB, MB as u32, &[]).await;
    assert_eq!(err, 0);
    assert!(got == chunk);
}

/// Counts the requests that reach the backend it wraps.
struct Counting {
    inner: Arc<dyn VolumeIo>,
    n: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl VolumeIo for Counting {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read(offset, buf).await
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.write(offset, data).await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.zero(offset, len).await
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.flush().await
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.inner.sync().await
    }
}

/// An export's backend is switched (and paused) under a live connection:
/// the client keeps its connection, a request sent while paused is held,
/// not failed, and everything written before and after reads back.
#[tokio::test]
async fn an_exports_backend_switches_under_a_live_connection() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let (mut c, _) = Client::connect(addr).await;
    let before = vec![1u8; 8192];
    assert_eq!(c.cmd(NBD_CMD_WRITE, 0, 8192, &before).await.0, 0);

    let io = server.export_io(&ds("vol")).unwrap();
    let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    io.switch(Arc::new(Counting {
        inner: Arc::new(crate::volume_io::LocalIo::new(fs.clone(), &ds("vol"))),
        n: n.clone(),
    }))
    .await;

    // Paused: a write sent now is held until the pause ends.
    let paused = io.pause().await;
    let after = vec![2u8; 8192];
    let send = after.clone();
    let mut pending = tokio::spawn(async move {
        let r = c.cmd(NBD_CMD_WRITE, 8192, 8192, &send).await.0;
        (c, r)
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut pending)
            .await
            .is_err(),
        "a write completed while the export was paused"
    );
    drop(paused);
    let (mut c, r) = pending.await.unwrap();
    assert_eq!(r, 0);

    assert_eq!(c.cmd(NBD_CMD_FLUSH, 0, 0, &[]).await.0, 0);
    let (err, got) = c.cmd(NBD_CMD_READ, 0, 16384, &[]).await;
    assert_eq!(err, 0);
    assert!(got[..8192] == before[..] && got[8192..] == after[..]);
    // The write, flush and read went through the new backend.
    assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 3);
}

/// Holds every request until opened, counting how many got in.
pub(crate) struct Gate {
    pub inner: Arc<dyn VolumeIo>,
    pub entered: Arc<std::sync::atomic::AtomicUsize>,
    pub open: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Gate {
    async fn wait(&self) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.open.subscribe().wait_for(|o| *o).await;
    }
}

#[async_trait::async_trait]
impl VolumeIo for Gate {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.wait().await;
        self.inner.read(offset, buf).await
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.wait().await;
        self.inner.write(offset, data).await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.wait().await;
        self.inner.zero(offset, len).await
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.wait().await;
        self.inner.flush().await
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.inner.sync().await
    }
}

/// Wait until `n` stops changing (nothing more is coming in).
pub(crate) async fn settled(n: &std::sync::atomic::AtomicUsize) -> usize {
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = n.load(std::sync::atomic::Ordering::SeqCst);
        if now == last {
            return now;
        }
        last = now;
    }
}

/// A client pipelining without limit gets `MAX_IN_FLIGHT` requests running
/// at once; the rest wait unread until one replies. Every request used to
/// get a task (and its payload a buffer) at once.
#[tokio::test]
async fn a_connection_has_at_most_so_many_requests_in_flight() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let (c, _) = Client::connect(addr).await;
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open = Arc::new(tokio::sync::watch::Sender::new(false));
    server
        .export_io(&ds("vol"))
        .unwrap()
        .switch(Arc::new(Gate {
            inner: Arc::new(crate::volume_io::LocalIo::new(fs.clone(), &ds("vol"))),
            entered: entered.clone(),
            open: open.clone(),
        }))
        .await;

    let n = MAX_IN_FLIGHT + 50;
    let (mut rd, mut wr) = c.s.into_split();
    let send = tokio::spawn(async move {
        for handle in 0..n as u64 {
            let mut req = Vec::new();
            req.extend_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
            req.extend_from_slice(&0u16.to_be_bytes());
            req.extend_from_slice(&NBD_CMD_WRITE.to_be_bytes());
            req.extend_from_slice(&handle.to_be_bytes());
            req.extend_from_slice(&(handle * 4096).to_be_bytes());
            req.extend_from_slice(&4096u32.to_be_bytes());
            req.extend_from_slice(&[handle as u8; 4096]);
            wr.write_all(&req).await.unwrap();
        }
        wr
    });
    assert_eq!(settled(&entered).await, MAX_IN_FLIGHT);

    open.send_replace(true);
    for _ in 0..n {
        assert_eq!(rd.read_u32().await.unwrap(), NBD_REPLY_MAGIC);
        assert_eq!(rd.read_u32().await.unwrap(), 0, "write failed");
        rd.read_u64().await.unwrap();
    }
    assert_eq!(entered.load(std::sync::atomic::Ordering::SeqCst), n);
    let _wr = send.await.unwrap();
}
