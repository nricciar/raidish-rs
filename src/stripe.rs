use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, Disk, DiskError, DiskOptions};
use futures::future::join_all;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

pub const DISKS: usize = 5;
/// Blocks at the start of every member disk reserved for its identity label
/// (two copies), outside the RAID address space. Stripe `s` lives at block
/// `s + DISK_LABEL_BLOCKS` of each disk.
pub const DISK_LABEL_BLOCKS: u64 = 2;

/// The member disks of the array. All shard I/O goes through the `*_shards`
/// helpers here, which apply the label offset, refuse reads from *fenced*
/// disks (members whose contents can't be trusted until resilvered) and
/// count every failed write or flush per disk, so the pool knows which disks
/// missed data.
#[derive(Debug)]
pub struct Stripe {
    pub disks: Arc<Vec<Caddy>>,
    data_shards: usize,
    fenced: AtomicU8,
    misses: [AtomicU64; DISKS],
}

impl Stripe {
    pub fn len(&self) -> usize {
        self.disks.len()
    }

    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    pub fn is_local(&self, index: usize) -> bool {
        self.disks[index].is_local()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn new(paths: [&str; DISKS], data_shards: usize) -> Self {
        Self::new_with(paths, data_shards, DiskOptions::default()).await
    }

    pub async fn new_with(paths: [&str; DISKS], data_shards: usize, opts: DiskOptions) -> Self {
        let disk_futures: Vec<_> = paths
            .iter()
            .map(|path| Disk::open_with(path, opts))
            .collect();
        let results = join_all(disk_futures).await;

        let disks: Vec<Caddy> = results
            .into_iter()
            .zip(paths.iter())
            .map(|(result, &path)| match result {
                Ok(disk) => Caddy::Healthy(path.to_string(), disk),
                Err(error) => {
                    eprintln!("disk '{}' unavailable: {}", path, error);
                    Caddy::Unhealthy(path.to_string(), error)
                }
            })
            .collect();

        Stripe {
            disks: Arc::new(disks),
            data_shards,
            fenced: AtomicU8::new(0),
            misses: Default::default(),
        }
    }

    /// Stop (or resume) serving reads from `disk`; writes still go to it.
    pub fn set_fenced(&self, disk: usize, fenced: bool) {
        if fenced {
            self.fenced.fetch_or(1 << disk, Ordering::SeqCst);
        } else {
            self.fenced.fetch_and(!(1 << disk), Ordering::SeqCst);
        }
    }

    pub fn is_fenced(&self, disk: usize) -> bool {
        self.fenced.load(Ordering::SeqCst) & (1 << disk) != 0
    }

    /// Failed writes/flushes on `disk` so far (monotonic).
    pub fn miss_count(&self, disk: usize) -> u64 {
        self.misses[disk].load(Ordering::SeqCst)
    }

    pub fn is_online(&self, disk: usize) -> bool {
        self.disks[disk].is_online()
    }

    fn miss<T>(&self, disk: usize, r: Result<T, DiskError>) -> Result<T, DiskError> {
        if r.is_err() {
            self.misses[disk].fetch_add(1, Ordering::SeqCst);
        }
        r
    }

    /// Read `buf.len() / BLOCK_SIZE` consecutive shards of `disk` starting at `stripe`.
    pub async fn read_shards(&self, disk: usize, stripe: u64, buf: &mut [u8]) -> Result<(), DiskError> {
        if self.is_fenced(disk) {
            return Err(DiskError::UnhealthyDisk);
        }
        self.read_shards_raw(disk, stripe, buf).await
    }

    /// `read_shards`, even from a fenced disk (for verification).
    pub async fn read_shards_raw(
        &self,
        disk: usize,
        stripe: u64,
        buf: &mut [u8],
    ) -> Result<(), DiskError> {
        self.disks[disk]
            .read_blocks(stripe + DISK_LABEL_BLOCKS, buf)
            .await
    }

    pub async fn write_shards(&self, disk: usize, stripe: u64, buf: &[u8]) -> Result<(), DiskError> {
        let r = self.disks[disk]
            .write_blocks(stripe + DISK_LABEL_BLOCKS, buf)
            .await;
        self.miss(disk, r)
    }

    pub async fn flush_disk(&self, disk: usize) -> Result<(), DiskError> {
        let r = self.disks[disk].flush().await;
        self.miss(disk, r)
    }

    /// Raw access to label copy `copy` (< DISK_LABEL_BLOCKS) of `disk`.
    pub async fn read_label(&self, disk: usize, copy: u64, buf: &mut [u8]) -> Result<(), DiskError> {
        assert!(copy < DISK_LABEL_BLOCKS);
        self.disks[disk].read_block(copy, buf).await
    }

    pub async fn write_label(
        &self,
        disk: usize,
        copy: u64,
        buf: &[u8; BLOCK_SIZE],
    ) -> Result<(), DiskError> {
        assert!(copy < DISK_LABEL_BLOCKS);
        self.disks[disk].write_block(copy, buf).await
    }
}

#[async_trait]
impl BlockDevice for Stripe {
    /// Stripes available on every disk (the smallest disk wins), after the
    /// label area.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        let mut min = u64::MAX;
        for disk in self.disks.iter() {
            min = min.min(disk.capacity_blocks().await?);
        }
        Ok(min.saturating_sub(DISK_LABEL_BLOCKS))
    }

    async fn flush(&self) -> Result<(), DiskError> {
        let futures = (0..self.disks.len()).map(|i| async move {
            match self.flush_disk(i).await {
                Ok(_) => Ok(()),
                Err(e) => Err((i, e)),
            }
        });
        let results = futures::future::join_all(futures).await;

        let mut failures = Vec::new();

        for result in results {
            match result {
                Ok(_) => (),
                Err((_disk_idx, disk_error)) => {
                    failures.push(disk_error);
                }
            }
        }

        if let Some(failure) = failures.pop() {
            Err(failure)
        } else {
            Ok(())
        }
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let stripe = lba / self.data_shards as u64;
        let data_index = lba as usize % self.data_shards;

        self.read_shards(data_index, stripe, &mut buf[..BLOCK_SIZE])
            .await
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe = lba / self.data_shards as u64;
        let data_index = lba as usize % self.data_shards;

        self.write_shards(data_index, stripe, buf).await
    }
}

#[derive(Debug)]
pub enum Caddy {
    Healthy(String, Disk),
    Unhealthy(String, DiskError),
}

#[async_trait]
impl BlockDevice for Caddy {
    async fn read_block(&self, block: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.read_block(block, buf).await.map_err(|e| {
                log_err("read from", dev, &e);
                e
            }),
        }
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.write_block(block, buf).await.map_err(|e| {
                log_err("write to", dev, &e);
                e
            }),
        }
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(_, disk) => disk.capacity_blocks().await,
        }
    }

    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.read_blocks(start, buf).await.map_err(|e| {
                log_err("read from", dev, &e);
                e
            }),
        }
    }

    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.write_blocks(start, buf).await.map_err(|e| {
                log_err("write to", dev, &e);
                e
            }),
        }
    }

    async fn flush(&self) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(_, _) => Err(DiskError::UnhealthyDisk),
            Caddy::Healthy(dev, disk) => disk.flush().await.map_err(|e| {
                log_err("flush", dev, &e);
                e
            }),
        }
    }
}

/// Log an I/O error, except the fast failures of a disk known to be offline
/// (those would flood the log while it reconnects).
fn log_err(op: &str, dev: &str, e: &DiskError) {
    if !matches!(e, DiskError::Disconnected) {
        eprintln!("caddy: unable to {} disk '{}': {}", op, dev, e);
    }
}

impl Caddy {
    pub fn path(&self) -> &str {
        match self {
            Caddy::Healthy(p, _) | Caddy::Unhealthy(p, _) => p,
        }
    }

    pub fn is_online(&self) -> bool {
        match self {
            Caddy::Unhealthy(_, _) => false,
            Caddy::Healthy(_, disk) => disk.is_online(),
        }
    }

    /// Changes whenever a remote disk reconnects (None for local disks).
    pub fn reconnects(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        match self {
            Caddy::Unhealthy(_, _) => None,
            Caddy::Healthy(_, disk) => disk.reconnects(),
        }
    }

    pub fn is_local(&self) -> bool {
        match self {
            Caddy::Unhealthy(_, _) => false,
            Caddy::Healthy(_, disk) => disk.is_local(),
        }
    }
}
