//! Drives the NBD server over a real socket with a minimal client.

use super::*;
use crate::test_util::DEFAULT_VOLBLOCKSIZE;
use crate::test_util::TestPool;
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

type Fs = Arc<FileSystem<crate::vdev::Pool>>;

/// Serve `volumes` (name, size) from one NBD server on an ephemeral port.
async fn serve_many(
    pool: &TestPool,
    volumes: &[(&str, u64)],
) -> (Fs, Arc<NbdServer<crate::vdev::Pool>>, std::net::SocketAddr) {
    let fs = Arc::new(pool.format().await);
    let server = Arc::new(NbdServer::with_bind(fs.clone(), DEFAULT_BIND, 0));
    for &(name, size) in volumes {
        fs.create_block(name, size, DEFAULT_VOLBLOCKSIZE)
            .await
            .unwrap();
        server.add_export(name).await.unwrap();
    }
    let listener = TcpListener::bind((DEFAULT_BIND, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = server.clone();
    tokio::spawn(async move { s.serve(listener).await });
    (fs, server, addr)
}

async fn serve(pool: &TestPool, size: u64) -> (Fs, std::net::SocketAddr) {
    let (fs, _server, addr) = serve_many(pool, &[("vol", size)]).await;
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
    let (fs, server, addr) = serve_many(&pool, &[("alpha", 16 * MB), ("beta", 32 * MB)]).await;
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
    fs.block_read("alpha", 4096, &mut buf).await.unwrap();
    assert!(buf == da);
    fs.block_read("beta", 4096, &mut buf).await.unwrap();
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
    let (_fs, server, _addr) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    assert!(matches!(
        server.add_export("vol").await,
        Err(FileSystemError::AlreadyExists)
    ));
    assert!(server.add_export("missing").await.is_err());
    assert!(server.add_export("").await.is_err());
    assert_eq!(server.exports(), vec!["vol"]);
}

#[tokio::test]
async fn remove_export_flushes_and_closes_its_connections() {
    let pool = TestPool::new().await;
    let (fs, server, addr) = serve_many(&pool, &[("alpha", 16 * MB), ("beta", 16 * MB)]).await;
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    let (mut b, _) = Client::connect_to(addr, "beta").await.unwrap();
    let data = vec![0x5Au8; 4096];
    assert_eq!(a.cmd(NBD_CMD_WRITE, 0, 4096, &data).await.0, 0);
    assert!(fs.has_dirty_volumes(), "write not yet committed");

    assert!(server.remove_export("alpha").await.unwrap());
    assert!(!fs.has_dirty_volumes(), "removal commits");
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

    assert!(!server.remove_export("alpha").await.unwrap());

    // The committed write survives a reload, and the volume can come back.
    let mut buf = vec![0u8; 4096];
    pool.load()
        .await
        .block_read("alpha", 0, &mut buf)
        .await
        .unwrap();
    assert!(buf == data);
    server.add_export("alpha").await.unwrap();
    let (mut a, _) = Client::connect_to(addr, "alpha").await.unwrap();
    assert!(a.cmd(NBD_CMD_READ, 0, 4096, &[]).await.1 == data);
}

#[tokio::test]
async fn one_sync_loop_per_file_system() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let count = |fs: &Fs| {
        let ptr = Arc::as_ptr(fs) as *const ();
        SYNC_LOOPS
            .lock()
            .unwrap()
            .iter()
            .filter(|w| w.as_ptr() as *const () == ptr)
            .count()
    };
    ensure_sync_loop(&fs, "nbd");
    ensure_sync_loop(&fs, "nvme");
    ensure_sync_loop(&fs, "nbd");
    assert_eq!(count(&fs), 1);
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
    let vol = fs.open_volume("vol").await.unwrap();
    let used: u64 = fs
        .volume_tree_extents(&vol.root_for_test(), vol.levels)
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
