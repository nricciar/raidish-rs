use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, Disk, DiskError};
use futures::future::join_all;
use std::sync::Arc;

pub const DISKS: usize = 5;

#[derive(Debug)]
pub struct Stripe {
    pub disks: Arc<Vec<Caddy>>,
    data_shards: usize,
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

    pub async fn new(paths: [&str; DISKS], data_shards: usize) -> Self {
        let disk_futures: Vec<_> = paths.iter().map(|path| Disk::open(path)).collect();
        let results = join_all(disk_futures).await;

        let disks: Vec<Caddy> = results
            .into_iter()
            .zip(paths.iter())
            .map(|(result, &path)| match result {
                Ok(disk) => Caddy::Healthy(path.to_string(), disk),
                Err(error) => Caddy::Unhealthy(path.to_string(), error),
            })
            .collect();

        Stripe {
            disks: Arc::new(disks),
            data_shards,
        }
    }
}

#[async_trait]
impl BlockDevice for Stripe {
    async fn flush(&self) -> Result<(), DiskError> {
        let futures = self.disks.iter().enumerate().map(|(i, disk)| async move {
            match disk.flush().await {
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
        let stripe = lba as usize / self.data_shards;
        let data_index = lba as usize % self.data_shards;

        self.disks[data_index].read_block(stripe as u64, buf).await
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe = lba as usize / self.data_shards;
        let data_index = lba as usize % self.data_shards;

        self.disks[data_index].write_block(stripe as u64, buf).await
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
            Caddy::Unhealthy(dev, e) => {
                eprintln!("caddy: unable to read from unhealthy disk '{}': {}", dev, e);
                Err(DiskError::UnhealthyDisk)
            }
            Caddy::Healthy(dev, disk) => disk.read_block(block, buf).await.map_err(|e| {
                eprintln!("caddy: unable to read from disk '{}': {}", dev, e);
                e
            }),
        }
    }

    async fn write_block(&self, block: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(dev, e) => {
                eprintln!("caddy: unable to write to unhealthy disk '{}': {}", dev, e);
                Err(DiskError::UnhealthyDisk)
            }
            Caddy::Healthy(dev, disk) => disk.write_block(block, buf).await.map_err(|e| {
                eprintln!("caddy: unable to write to disk '{}': {}", dev, e);
                e
            }),
        }
    }

    async fn flush(&self) -> Result<(), DiskError> {
        match self {
            Caddy::Unhealthy(dev, e) => {
                eprintln!("caddy: unable to flush unhealthy disk '{}': {}", dev, e);
                Err(DiskError::UnhealthyDisk)
            }
            Caddy::Healthy(dev, disk) => disk.flush().await.map_err(|e| {
                eprintln!("caddy: unable to flush disk '{}': {}", dev, e);
                e
            }),
        }
    }
}

impl Caddy {
    pub fn is_local(&self) -> bool {
        match self {
            Caddy::Unhealthy(_, _) => false,
            Caddy::Healthy(_, disk) => disk.is_local(),
        }
    }
}
