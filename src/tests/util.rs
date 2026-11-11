//! Shared helpers for filesystem tests.

use crate::fs::FileSystem;
use crate::raidz::{DATA_SHARDS, RaidZ};
use crate::stripe::Stripe;
use std::path::PathBuf;
use tempfile::NamedTempFile;

pub const DISK_BYTES: u64 = 100 * 1024 * 1024;
pub const TOTAL_BLOCKS: u64 = DISK_BYTES / crate::disk::BLOCK_SIZE as u64;

/// Five sparse backing files that can be formatted, reloaded and tampered with.
pub struct TestPool {
    _files: Vec<NamedTempFile>,
    pub paths: Vec<PathBuf>,
}

impl TestPool {
    pub async fn new() -> Self {
        let mut files = Vec::new();
        let mut paths = Vec::new();
        for _ in 0..crate::stripe::DISKS {
            let f = NamedTempFile::new().unwrap();
            f.as_file().set_len(DISK_BYTES).unwrap();
            paths.push(f.path().to_path_buf());
            files.push(f);
        }
        TestPool {
            _files: files,
            paths,
        }
    }

    pub async fn dev(&self) -> RaidZ {
        let p: Vec<&str> = self.paths.iter().map(|p| p.to_str().unwrap()).collect();
        let stripe = Stripe::new([p[0], p[1], p[2], p[3], p[4]], DATA_SHARDS).await;
        RaidZ::new(stripe).await
    }

    pub async fn format(&self) -> FileSystem<RaidZ> {
        FileSystem::format(self.dev().await, TOTAL_BLOCKS)
            .await
            .unwrap()
    }

    pub async fn load(&self) -> FileSystem<RaidZ> {
        FileSystem::load(self.dev().await).await.unwrap()
    }

    /// Disk index and byte offset holding data block `lba` (data shard).
    pub fn locate(lba: u64) -> (usize, u64) {
        let stripe = lba / DATA_SHARDS as u64;
        let disk = (lba % DATA_SHARDS as u64) as usize;
        (disk, stripe * crate::disk::BLOCK_SIZE as u64)
    }

    /// Byte offset of a stripe's shard on any disk.
    pub fn stripe_offset(lba: u64) -> u64 {
        (lba / DATA_SHARDS as u64) * crate::disk::BLOCK_SIZE as u64
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
}

#[derive(Clone)]
pub struct FaultSwitch(std::sync::Arc<std::sync::atomic::AtomicI64>);

impl FaultSwitch {
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
        (
            FaultyDev {
                inner,
                remaining: remaining.clone(),
            },
            FaultSwitch(remaining),
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
}

impl TestPool {
    pub async fn format_faulty(&self) -> (FileSystem<FaultyDev<RaidZ>>, FaultSwitch) {
        let (dev, switch) = FaultyDev::new(self.dev().await);
        (FileSystem::format(dev, TOTAL_BLOCKS).await.unwrap(), switch)
    }
}

impl TestPool {
    /// Open the pool with the given disks unavailable (as if unplugged).
    pub async fn dev_without(&self, missing: &[usize]) -> RaidZ {
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
        let stripe = Stripe::new([&p[0], &p[1], &p[2], &p[3], &p[4]], DATA_SHARDS).await;
        RaidZ::new(stripe).await
    }
}
