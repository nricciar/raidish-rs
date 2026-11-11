//! Drives the NBD server over a real socket with a minimal client.

use super::*;
use crate::test_util::DEFAULT_VOLBLOCKSIZE;
use crate::test_util::TestPool;
use tokio::net::TcpStream;

const MB: u64 = 1 << 20;

struct Client {
    s: TcpStream,
    next: u64,
}

impl Client {
    /// Fixed-newstyle handshake with NBD_OPT_GO; returns the export size.
    async fn connect(addr: std::net::SocketAddr) -> (Self, u64) {
        let mut s = TcpStream::connect(addr).await.unwrap();
        assert_eq!(s.read_u64().await.unwrap(), NBD_INIT_MAGIC);
        assert_eq!(s.read_u64().await.unwrap(), NBD_OPTS_MAGIC);
        let _server_flags = s.read_u16().await.unwrap();
        s.write_u32((NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES) as u32)
            .await
            .unwrap();

        // NBD_OPT_GO: name length 0, no info requests.
        s.write_u64(NBD_OPTS_MAGIC).await.unwrap();
        s.write_u32(NBD_OPT_GO).await.unwrap();
        s.write_u32(6).await.unwrap();
        s.write_all(&[0, 0, 0, 0, 0, 0]).await.unwrap();

        let mut size = 0;
        loop {
            assert_eq!(s.read_u64().await.unwrap(), NBD_REP_MAGIC);
            assert_eq!(s.read_u32().await.unwrap(), NBD_OPT_GO);
            let kind = s.read_u32().await.unwrap();
            let len = s.read_u32().await.unwrap();
            let mut data = vec![0u8; len as usize];
            s.read_exact(&mut data).await.unwrap();
            match kind {
                NBD_REP_INFO => {
                    size = u64::from_be_bytes(data[2..10].try_into().unwrap());
                    let flags = u16::from_be_bytes(data[10..12].try_into().unwrap());
                    assert_eq!(flags, TRANSMISSION_FLAGS);
                }
                NBD_REP_ACK => break,
                other => panic!("unexpected option reply {other}"),
            }
        }
        (Client { s, next: 1 }, size)
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

async fn serve(
    pool: &TestPool,
    size: u64,
) -> (Arc<FileSystem<crate::vdev::Pool>>, std::net::SocketAddr) {
    let fs = Arc::new(pool.format().await);
    fs.create_block("vol", size, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_fs = fs.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let fs = server_fs.clone();
            tokio::spawn(async move {
                let _ = handle_nbd_client(stream, fs, "vol".into(), size).await;
            });
        }
    });
    (fs, addr)
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
