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
