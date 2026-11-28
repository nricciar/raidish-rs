//! Shared helpers for filesystem tests.

use crate::coords::PoolLba;
use crate::disk::{Block, Candidates, DiskError, PoolDevice};
use crate::disks::DiskSet;
use crate::fs::FileSystem;
use crate::role::CommitterFs;
use crate::vdev::{Pool, Topology};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Pool disk `i` (tests name disks by number).
pub fn disk(i: usize) -> crate::coords::PoolDiskIdx {
    crate::coords::PoolDiskIdx::new(i).expect("a pool disk index")
}

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
    pub async fn format(&self) -> CommitterFs<Pool> {
        let dev = self.dev().await;
        let blocks = self.format_blocks(&dev).await;
        FileSystem::format(dev, blocks).await.unwrap()
    }

    pub async fn format_blocks(&self, dev: &Pool) -> u64 {
        TOTAL_BLOCKS.min(dev.capacity_blocks().await.unwrap())
    }

    pub async fn load(&self) -> CommitterFs<Pool> {
        FileSystem::load(self.dev().await).await.unwrap()
    }

    /// Disk index and byte offset holding data block `lba` (data shard),
    /// for the default raidz2 pool.
    pub fn locate(lba: PoolLba) -> (usize, u64) {
        let lba = lba.get();
        let stripe = lba / DATA_SHARDS as u64;
        let disk = (lba % DATA_SHARDS as u64) as usize;
        (
            disk,
            (stripe + crate::disks::DISK_LABEL_BLOCKS) * crate::disk::BLOCK_SIZE as u64,
        )
    }

    /// Byte offset of a stripe's shard on any disk (default raidz2 pool).
    pub fn stripe_offset(lba: PoolLba) -> u64 {
        (lba.get() / DATA_SHARDS as u64 + crate::disks::DISK_LABEL_BLOCKS)
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
    stall: Stall,
}

/// Writes touching a block range wait (unacked, unwritten) until the
/// range is cleared; how many have waited.
type Stall = (
    std::sync::Arc<tokio::sync::watch::Sender<Option<std::ops::Range<u64>>>>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
);

#[derive(Clone)]
pub struct FaultSwitch(
    std::sync::Arc<std::sync::atomic::AtomicI64>,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    Stall,
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
    /// Writes touching `range` (pool LBAs) wait until it's cleared
    /// (`None` lets every waiting one through).
    pub fn stall_writes(&self, range: Option<std::ops::Range<PoolLba>>) {
        self.2
            .0
            .send_replace(range.map(|r| r.start.get()..r.end.get()));
    }
    /// Wait until `n` writes in all have stalled.
    pub async fn wait_stalled(&self, n: usize) {
        while self.2.1.load(std::sync::atomic::Ordering::SeqCst) < n {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
}

impl<D> FaultyDev<D> {
    pub fn new(inner: D) -> (Self, FaultSwitch) {
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(-1));
        let flush_fails = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stall: Stall = (
            std::sync::Arc::new(tokio::sync::watch::Sender::new(None)),
            Default::default(),
        );
        (
            FaultyDev {
                inner,
                remaining: remaining.clone(),
                flush_fails: flush_fails.clone(),
                stall: stall.clone(),
            },
            FaultSwitch(remaining, flush_fails, stall),
        )
    }

    async fn stalled(&self, b: PoolLba, n: u64) {
        let b = b.get();
        let hit = |r: &Option<std::ops::Range<u64>>| {
            r.as_ref().is_some_and(|r| b < r.end && b + n > r.start)
        };
        let mut rx = self.stall.0.subscribe();
        if hit(&rx.borrow_and_update()) {
            self.stall
                .1
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = rx.wait_for(|r| !hit(r)).await;
        }
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

#[async_trait::async_trait]
impl<D: PoolDevice> PoolDevice for FaultyDev<D> {
    async fn flush(&self) -> Result<(), DiskError> {
        if self.flush_fails.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(DiskError::IoError(std::io::Error::other(
                "injected flush failure",
            )));
        }
        self.inner.flush().await
    }
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        self.inner.capacity_blocks().await
    }
    async fn read_block(&self, b: PoolLba, buf: &mut Block) -> Result<(), DiskError> {
        self.inner.read_block(b, buf).await
    }
    async fn read_blocks(&self, b: PoolLba, buf: &mut [Block]) -> Result<(), DiskError> {
        self.inner.read_blocks(b, buf).await
    }
    async fn write_block(&self, b: PoolLba, buf: &Block) -> Result<(), DiskError> {
        self.stalled(b, 1).await;
        self.check()?;
        self.inner.write_block(b, buf).await
    }
    async fn write_blocks(&self, b: PoolLba, buf: &[Block]) -> Result<(), DiskError> {
        self.stalled(b, buf.len() as u64).await;
        self.check()?;
        self.inner.write_blocks(b, buf).await
    }
    async fn reconstruct_candidates(&self, b: PoolLba) -> Result<Candidates, DiskError> {
        self.inner.reconstruct_candidates(b).await
    }
    async fn repair_block(&self, b: PoolLba, buf: &Block) -> Result<(), DiskError> {
        self.inner.repair_block(b, buf).await
    }
    fn pool(&self) -> &Pool {
        self.inner.pool()
    }
}

impl TestPool {
    pub async fn format_faulty(&self) -> (CommitterFs<FaultyDev<Pool>>, FaultSwitch) {
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

/// Aggressive timeouts so transport failure tests run quickly.
pub fn fast_disk_options() -> crate::disk::DiskOptions {
    use std::time::Duration;
    crate::disk::DiskOptions {
        lock: false,
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
    pub async fn dev_remote(&self, server: &impl DiskUrls, remote: &[usize]) -> Pool {
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

/// A disk server's URL for each of its disks (see `TestPool::dev_remote`):
/// the server lives with the crate that has one.
pub trait DiskUrls {
    fn url(&self, disk: usize) -> String;
}

/// A stand-in HTTP server: answers every request with `body` (JSON) after
/// `delay`, or, `body` None, hangs up without answering. Its URL, and how
/// many requests it has had.
pub async fn fake_json_server(
    body: Option<String>,
    delay: std::time::Duration,
) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    fake_http_server(200, body, delay).await
}

/// `fake_json_server`, answering with HTTP `status` (`body` is sent as
/// JSON with a 200, as plain text otherwise, as Rocket's catchers do).
pub async fn fake_http_server(
    status: u16,
    body: Option<String>,
    delay: std::time::Duration,
) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let h = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let (body, h) = (body.clone(), h.clone());
            tokio::spawn(async move {
                let mut rd = BufReader::new(sock);
                // One request after another on the connection (keep-alive).
                loop {
                    let mut len = 0usize;
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if rd.read_line(&mut line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let l = line.trim_end();
                        if l.is_empty() {
                            break;
                        }
                        if let Some(v) = l.to_ascii_lowercase().strip_prefix("content-length:") {
                            len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut req = vec![0u8; len];
                    if rd.read_exact(&mut req).await.is_err() {
                        return;
                    }
                    h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    let Some(body) = &body else { return };
                    let resp = format!(
                        "HTTP/1.1 {} Answer\r\ncontent-type: {}\r\ncontent-length: {}\r\n\r\n{}",
                        status,
                        if status == 200 {
                            "application/json"
                        } else {
                            "text/plain"
                        },
                        body.len(),
                        body
                    );
                    if rd.get_mut().write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (url, hits)
}

pub use crate::disk::fault::{FaultDisk, FaultError, FaultHandle, FaultStats};

/// A test pool whose every member disk sits behind its own `FaultHandle`
/// (indexed like the pool's disks). The handles outlive the pools opened
/// over them, so faults carry across crash/reopen.
pub struct FaultPool {
    pub pool: TestPool,
    pub disks: Vec<FaultHandle>,
}

impl TestPool {
    /// An `n`-way mirror.
    pub async fn mirror(n: usize) -> Self {
        Self::with_topology(Topology::Mirror {
            disks: (0..n).collect(),
        })
        .await
    }

    /// raidz`parity` over `n` disks.
    pub async fn raidz(parity: usize, n: usize) -> Self {
        Self::with_topology(Topology::RaidZ {
            parity,
            disks: (0..n).collect(),
        })
        .await
    }

    /// Put a fault layer in front of every disk.
    pub fn faulty(self) -> FaultPool {
        let disks = self
            .paths
            .iter()
            .map(|p| FaultDisk::open(p).unwrap())
            .collect();
        FaultPool { pool: self, disks }
    }
}

impl FaultPool {
    pub async fn mirror(n: usize) -> Self {
        TestPool::mirror(n).await.faulty()
    }

    pub async fn raidz(parity: usize, n: usize) -> Self {
        TestPool::raidz(parity, n).await.faulty()
    }

    pub fn disk(&self, i: usize) -> &FaultHandle {
        &self.disks[i]
    }

    /// Clear every disk's switches.
    pub fn heal(&self) {
        for d in &self.disks {
            d.heal();
        }
    }

    /// A fresh `Pool` over the fault disks (what a process opening the pool
    /// gets).
    pub fn dev(&self) -> Pool {
        let caddies = self
            .disks
            .iter()
            .map(|h| {
                crate::disks::Caddy::Healthy(
                    h.path().to_string_lossy().into_owned(),
                    crate::disk::Disk::faulty(h.clone()),
                )
            })
            .collect();
        Pool::new(
            Arc::new(DiskSet::from_caddies(caddies)),
            self.pool.topology.clone(),
        )
        .unwrap()
    }

    pub async fn format(&self) -> CommitterFs<Pool> {
        let dev = self.dev();
        let blocks = self.pool.format_blocks(&dev).await;
        FileSystem::format(dev, blocks).await.unwrap()
    }

    pub async fn load(&self) -> CommitterFs<Pool> {
        FileSystem::load(self.dev()).await.unwrap()
    }
}

// ---- files with known contents (scrub / resilver tests) ----

/// `len` bytes for file `name` at `generation`.
pub fn content(name: &str, generation: u8, len: usize) -> Vec<u8> {
    let seed = blake3::hash(name.as_bytes()).as_bytes()[0];
    (0..len)
        .map(|i| {
            (i as u8)
                .wrapping_mul(31)
                .wrapping_add(seed)
                .wrapping_add(generation)
        })
        .collect()
}

pub async fn write_files(fs: &CommitterFs<Pool>, prefix: &str, generation: u8, n: usize) {
    for i in 0..n {
        let name = format!("/{}{}", prefix, i);
        fs.write_file(
            Path::new(&name),
            &content(&name, generation, 150_000 + i * 7_001),
        )
        .await
        .unwrap();
    }
}

pub async fn check_files<R: crate::role::Role>(
    fs: &FileSystem<Pool, R>,
    prefix: &str,
    generation: u8,
    n: usize,
) {
    for i in 0..n {
        let name = format!("/{}{}", prefix, i);
        let data = fs.read_file(Path::new(&name)).await.unwrap();
        assert!(
            data == content(&name, generation, 150_000 + i * 7_001),
            "{} has wrong contents",
            name
        );
    }
}

/// Helper to create a 100MB temporary file for testing
pub async fn create_test_disk_file() -> (NamedTempFile, PathBuf) {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path().to_path_buf();

    // Allocate 100MB (similar to fallocate)
    let size = 100 * 1024 * 1024; // 100MB
    let file = tokio::fs::File::create(&path)
        .await
        .expect("Failed to open temp file");

    // Write zeros to allocate space
    file.set_len(size).await.expect("Failed to set file length");
    file.sync_all().await.expect("Failed to sync file");

    (temp_file, path)
}

/// Dataset name `s`, which a test knows is valid.
#[track_caller]
pub fn ds(s: &str) -> crate::types::DatasetName {
    s.parse().unwrap()
}

/// VM name `s`, which a test knows is valid.
#[track_caller]
pub fn vmn(s: &str) -> crate::types::VmName {
    s.parse().unwrap()
}

/// Dataset name `s`, which a test knows is valid.
#[track_caller]
pub fn dsn(s: &str) -> crate::types::DatasetName {
    s.parse().unwrap()
}
