//! Shared helpers for filesystem tests.

use crate::disks::DiskSet;
use crate::fs::FileSystem;
use crate::vdev::{Pool, Topology};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;

pub const DISK_BYTES: u64 = 100 * 1024 * 1024;
/// Format size for the default pool (raidz2 over 5 disks).
pub const TOTAL_BLOCKS: u64 = DISK_BYTES / crate::disk::BLOCK_SIZE as u64;
/// Shape of the default test pool: raidz2, 3 data + 2 parity.
pub const DATA_SHARDS: usize = 3;
pub const DISKS: usize = 5;
/// Layout values of the default pool (checked in `topology_tests`).
pub const MAX_RECORD_BLOCKS: u64 = 30;
pub const VOLBLOCK_ALIGN: u32 = (DATA_SHARDS * crate::disk::BLOCK_SIZE) as u32;
pub const DEFAULT_VOLBLOCKSIZE: u32 = 4 * VOLBLOCK_ALIGN; // 48 KiB
pub const MAX_VOLBLOCKSIZE: u32 = MAX_RECORD_BLOCKS as u32 * crate::disk::BLOCK_SIZE as u32;
pub const UBERBLOCK_START: u64 = 3;
pub const SPACEMAP_START: u64 = 18;

/// Sparse backing files with a pool layout over them, that can be
/// formatted, reloaded and tampered with.
pub struct TestPool {
    _files: Vec<NamedTempFile>,
    pub paths: Vec<PathBuf>,
    pub topology: Topology,
}

impl TestPool {
    /// The default: raidz2 over five disks.
    pub async fn new() -> Self {
        Self::with_topology(Topology::RaidZ {
            parity: 2,
            disks: (0..DISKS).collect(),
        })
        .await
    }

    pub async fn with_topology(topology: Topology) -> Self {
        let mut files = Vec::new();
        let mut paths = Vec::new();
        for _ in 0..topology.leaves().len() {
            let f = NamedTempFile::new().unwrap();
            f.as_file().set_len(DISK_BYTES).unwrap();
            paths.push(f.path().to_path_buf());
            files.push(f);
        }
        TestPool {
            _files: files,
            paths,
            topology,
        }
    }

    /// Open the pool over `paths` (one per disk, in order).
    pub async fn open(&self, paths: &[String], opts: crate::disk::DiskOptions) -> Pool {
        let p: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let disks = Arc::new(DiskSet::new_with(&p, opts).await);
        Pool::new(disks, self.topology.clone()).unwrap()
    }

    fn path_strings(&self) -> Vec<String> {
        self.paths
            .iter()
            .map(|p| p.to_str().unwrap().to_string())
            .collect()
    }

    pub async fn dev(&self) -> Pool {
        self.open(&self.path_strings(), Default::default()).await
    }

    /// Format `TOTAL_BLOCKS` (or the pool's capacity, if smaller).
    pub async fn format(&self) -> FileSystem<Pool> {
        let dev = self.dev().await;
        let blocks = self.format_blocks(&dev).await;
        FileSystem::format(dev, blocks).await.unwrap()
    }

    pub async fn format_blocks(&self, dev: &Pool) -> u64 {
        use crate::disk::BlockDevice;
        TOTAL_BLOCKS.min(dev.capacity_blocks().await.unwrap())
    }

    pub async fn load(&self) -> FileSystem<Pool> {
        FileSystem::load(self.dev().await).await.unwrap()
    }

    /// Disk index and byte offset holding data block `lba` (data shard),
    /// for the default raidz2 pool.
    pub fn locate(lba: u64) -> (usize, u64) {
        let stripe = lba / DATA_SHARDS as u64;
        let disk = (lba % DATA_SHARDS as u64) as usize;
        (
            disk,
            (stripe + crate::disks::DISK_LABEL_BLOCKS) * crate::disk::BLOCK_SIZE as u64,
        )
    }

    /// Byte offset of a stripe's shard on any disk (default raidz2 pool).
    pub fn stripe_offset(lba: u64) -> u64 {
        (lba / DATA_SHARDS as u64 + crate::disks::DISK_LABEL_BLOCKS)
            * crate::disk::BLOCK_SIZE as u64
    }

    pub fn read_raw(&self, disk: usize, offset: u64) -> Vec<u8> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&self.paths[disk]).unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = vec![0u8; crate::disk::BLOCK_SIZE];
        f.read_exact(&mut buf).unwrap();
        buf
    }

    /// Flip every byte of one on-disk block, bypassing the filesystem.
    pub fn corrupt(&self, disk: usize, offset: u64) {
        use std::io::{Seek, SeekFrom, Write};
        let mut buf = self.read_raw(disk, offset);
        for b in buf.iter_mut() {
            *b ^= 0xA5;
        }
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.paths[disk])
            .unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }
}

/// Wraps a device and fails writes on demand, to exercise error paths.
/// `fail_after(n)`: the next `n` write calls succeed, every later one fails.
pub struct FaultyDev<D> {
    inner: D,
    remaining: std::sync::Arc<std::sync::atomic::AtomicI64>,
    flush_fails: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
pub struct FaultSwitch(
    std::sync::Arc<std::sync::atomic::AtomicI64>,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
);

impl FaultSwitch {
    /// Make every flush fail (`true`) or pass through again.
    pub fn fail_flushes(&self, fail: bool) {
        self.1.store(fail, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn fail_after(&self, writes: i64) {
        self.0.store(writes, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn heal(&self) {
        self.0.store(-1, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn remaining(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Has an injected failure been hit (i.e. has the "crash" happened)?
    pub fn tripped(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst) == 0
    }
}

impl<D> FaultyDev<D> {
    pub fn new(inner: D) -> (Self, FaultSwitch) {
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(-1));
        let flush_fails = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            FaultyDev {
                inner,
                remaining: remaining.clone(),
                flush_fails: flush_fails.clone(),
            },
            FaultSwitch(remaining, flush_fails),
        )
    }

    fn check(&self) -> Result<(), crate::disk::DiskError> {
        use std::sync::atomic::Ordering;
        let r = self.remaining.load(Ordering::SeqCst);
        if r < 0 {
            return Ok(());
        }
        if r == 0 {
            return Err(crate::disk::DiskError::IoError(std::io::Error::other(
                "injected write failure",
            )));
        }
        self.remaining.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl<D: crate::disk::BlockDevice> crate::disk::BlockDevice for FaultyDev<D> {
    async fn flush(&self) -> Result<(), crate::disk::DiskError> {
        if self.flush_fails.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(crate::disk::DiskError::IoError(std::io::Error::other(
                "injected flush failure",
            )));
        }
        self.inner.flush().await
    }
    async fn read_block(&self, b: u64, buf: &mut [u8]) -> Result<(), crate::disk::DiskError> {
        self.inner.read_block(b, buf).await
    }
    async fn read_blocks(&self, b: u64, buf: &mut [u8]) -> Result<(), crate::disk::DiskError> {
        self.inner.read_blocks(b, buf).await
    }
    async fn write_block(
        &self,
        b: u64,
        buf: &[u8; crate::disk::BLOCK_SIZE],
    ) -> Result<(), crate::disk::DiskError> {
        self.check()?;
        self.inner.write_block(b, buf).await
    }
    async fn write_blocks(&self, b: u64, buf: &[u8]) -> Result<(), crate::disk::DiskError> {
        self.check()?;
        self.inner.write_blocks(b, buf).await
    }
    async fn reconstruct_candidates(
        &self,
        b: u64,
    ) -> Result<crate::disk::Candidates, crate::disk::DiskError> {
        self.inner.reconstruct_candidates(b).await
    }
    async fn repair_block(
        &self,
        b: u64,
        buf: &[u8; crate::disk::BLOCK_SIZE],
    ) -> Result<(), crate::disk::DiskError> {
        self.inner.repair_block(b, buf).await
    }
    fn pool(&self) -> Option<&Pool> {
        self.inner.pool()
    }
}

impl TestPool {
    pub async fn format_faulty(&self) -> (FileSystem<FaultyDev<Pool>>, FaultSwitch) {
        let dev = self.dev().await;
        let blocks = self.format_blocks(&dev).await;
        let (dev, switch) = FaultyDev::new(dev);
        (FileSystem::format(dev, blocks).await.unwrap(), switch)
    }
}

impl TestPool {
    /// Open the pool with the given disks unavailable (as if unplugged).
    pub async fn dev_without(&self, missing: &[usize]) -> Pool {
        let p: Vec<String> = self
            .paths
            .iter()
            .enumerate()
            .map(|(i, p)| {
                if missing.contains(&i) {
                    "/nonexistent-raidish-test/disk".to_string()
                } else {
                    p.to_str().unwrap().to_string()
                }
            })
            .collect();
        self.open(&p, Default::default()).await
    }
}

/// How a `TestDiskServer` treats requests.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ServerMode {
    Normal = 0,
    /// Keep reading (and answering pings) but never reply to a request.
    Silent = 1,
    /// Stop reading the connection entirely (no pongs either).
    Frozen = 2,
}

/// An in-process disk server speaking the websocket block protocol (the same
/// request handler `listen` uses), on an ephemeral port that can be stopped,
/// restarted and made to misbehave.
pub struct TestDiskServer {
    pub port: u16,
    disks: std::sync::Arc<Vec<crate::disks::Caddy>>,
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    /// The server's lease epoch floor (see `api::EpochFloor`).
    pub floor: std::sync::Arc<crate::api::EpochFloor>,
    tasks: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl TestDiskServer {
    /// Serve `paths` (local files) as disks 0..n.
    pub async fn start(paths: &[PathBuf]) -> Self {
        let mut disks = Vec::new();
        for p in paths {
            let p = p.to_str().unwrap();
            disks.push(crate::disks::Caddy::Healthy(
                p.to_string(),
                crate::disk::Disk::open(p).await.unwrap(),
            ));
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = TestDiskServer {
            port: listener.local_addr().unwrap().port(),
            disks: std::sync::Arc::new(disks),
            mode: Default::default(),
            floor: Default::default(),
            tasks: Default::default(),
        };
        server.serve(listener);
        server
    }

    pub fn url(&self, disk: usize) -> String {
        format!("http://127.0.0.1:{}/api/v1/disks/{}", self.port, disk)
    }

    pub fn set_mode(&self, mode: ServerMode) {
        self.mode
            .store(mode as u8, std::sync::atomic::Ordering::SeqCst);
    }

    /// Close the listener and every connection.
    pub fn stop(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    /// Listen again on the same port.
    pub async fn restart(&self) {
        self.stop();
        let mut tries = 0;
        let listener = loop {
            match tokio::net::TcpListener::bind(("127.0.0.1", self.port)).await {
                Ok(l) => break l,
                Err(e) if tries < 50 => {
                    tries += 1;
                    let _ = e;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(e) => panic!("rebind {}: {}", self.port, e),
            }
        };
        self.serve(listener);
    }

    fn serve(&self, listener: tokio::net::TcpListener) {
        use futures_util::{SinkExt, StreamExt};
        let (disks, mode, tasks) = (self.disks.clone(), self.mode.clone(), self.tasks.clone());
        let floor = self.floor.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (disks, mode, floor) = (disks.clone(), mode.clone(), floor.clone());
                let conn = tokio::spawn(async move {
                    let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let (mut sink, mut source) = ws.split();
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                    let writer = tokio::spawn(async move {
                        while let Some(r) = rx.recv().await {
                            if sink
                                .send(tungstenite::Message::Binary(r.into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                    loop {
                        if mode.load(std::sync::atomic::Ordering::SeqCst)
                            == ServerMode::Frozen as u8
                        {
                            std::future::pending::<()>().await;
                        }
                        match source.next().await {
                            Some(Ok(tungstenite::Message::Binary(data))) => {
                                if mode.load(std::sync::atomic::Ordering::SeqCst)
                                    == ServerMode::Silent as u8
                                {
                                    continue;
                                }
                                let (disks, tx, floor) = (disks.clone(), tx.clone(), floor.clone());
                                tokio::spawn(async move {
                                    let _ = tx.send(
                                        crate::api::handle_ws_message(&data, &disks, &floor).await,
                                    );
                                });
                            }
                            Some(Ok(_)) => {}
                            _ => break,
                        }
                    }
                    writer.abort();
                });
                tasks.lock().unwrap().push(conn.abort_handle());
            }
        });
        self.tasks.lock().unwrap().push(accept.abort_handle());
    }
}

impl Drop for TestDiskServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Aggressive timeouts so transport failure tests run quickly.
pub fn fast_disk_options() -> crate::disk::DiskOptions {
    use std::time::Duration;
    crate::disk::DiskOptions {
        request_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
        ping_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_millis(400),
        reconnect_min: Duration::from_millis(20),
        reconnect_max: Duration::from_millis(200),
    }
}

/// Poll `cond` every 10 ms for up to `secs` seconds.
pub async fn wait_until(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cond()
}

impl TestPool {
    /// A device whose disks listed in `remote` are reached through `server`
    /// (which must serve this pool's files in order), the rest opened locally.
    pub async fn dev_remote(&self, server: &TestDiskServer, remote: &[usize]) -> Pool {
        let p: Vec<String> = (0..self.paths.len())
            .map(|i| {
                if remote.contains(&i) {
                    server.url(i)
                } else {
                    self.paths[i].to_str().unwrap().to_string()
                }
            })
            .collect();
        self.open(&p, fast_disk_options()).await
    }
}
