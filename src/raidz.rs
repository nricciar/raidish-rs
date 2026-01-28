use reed_solomon_erasure::galois_8::ReedSolomon;
use crate::disk::{BLOCK_SIZE, BlockId, DiskError, BlockDevice};
use std::collections::BTreeMap;
use futures::stream::{FuturesUnordered, StreamExt};
use crate::stripe::{Stripe, DISKS};

pub const DATA_SHARDS: usize = 3;
pub const PARITY_SHARDS: usize = 2;

#[derive(Debug)]
pub enum RaidZError {
    DiskError(DiskError),
    ReedSolomonError(reed_solomon_erasure::Error),
    SerializationError(bincode::Error),
    IncompleteStripe(u64),
    NotFormatted,
}

impl From<DiskError> for RaidZError {
    fn from(error: DiskError) -> Self {
        RaidZError::DiskError(error)
    }
}

impl From<bincode::Error> for RaidZError {
    fn from(error: bincode::Error) -> Self {
        RaidZError::SerializationError(error)
    }
}

impl From<reed_solomon_erasure::Error> for RaidZError {
    fn from(error: reed_solomon_erasure::Error) -> Self {
        RaidZError::ReedSolomonError(error)
    }
}

#[derive(Debug)]
pub struct StripeBuffer {
    data: Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>,
    shard_mask: u8,
    dirty: bool,
    flushed: bool,  // For optimistic writes
}

impl StripeBuffer {
    pub fn new() -> Self {
        StripeBuffer {
            data: None,
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }
    
    pub fn set_shard(&mut self, shard_idx: usize, block_data: &[u8; BLOCK_SIZE]) {
        assert!(shard_idx < DATA_SHARDS);
        
        let data_ptr = self.data.get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
        
        let offset = shard_idx * BLOCK_SIZE;
        data_ptr[offset..offset + BLOCK_SIZE].copy_from_slice(block_data);
        
        self.shard_mask |= 1 << shard_idx;
        self.dirty = true;
        self.flushed = false;
    }
    
    pub fn has_shard(&self, shard_idx: usize) -> bool {
        (self.shard_mask & (1 << shard_idx)) != 0
    }
    
    pub fn is_complete(&self) -> bool {
        self.shard_mask == 0b111  // All 3 data shards present
    }
    
    pub fn get_shard(&self, shard_idx: usize, buf: &mut [u8]) -> bool {
        match &self.data {
            Some(d) if self.has_shard(shard_idx) => {
                let offset = shard_idx * BLOCK_SIZE;
                buf.copy_from_slice(&d[offset..offset + BLOCK_SIZE]);
                true
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct RaidZ {
    pub stripe: Stripe,
    rs: ReedSolomon,
    writes: BTreeMap<u64, StripeBuffer>,
}

#[async_trait]
impl BlockDevice for RaidZ {
    async fn flush(&mut self) -> Result<(),DiskError> {
        let stripes = std::mem::take(&mut self.writes);

        for (stripe_id, mut stripe_buf) in stripes {
            // Check if already flushed
            if stripe_buf.flushed && !stripe_buf.dirty {
                continue;
            }
            
            if !stripe_buf.is_complete() {
                let mut missing_indices = Vec::new();
                for i in 0..DATA_SHARDS {
                    if !stripe_buf.has_shard(i) {
                        missing_indices.push(i);
                    }
                }

                let data = stripe_buf.data.get_or_insert_with(|| {
                    Box::new([0u8; BLOCK_SIZE * DATA_SHARDS])
                });
                
                let mut read_tasks = FuturesUnordered::new();
                for i in missing_indices {
                    let disk = &self.stripe.disks[i];
                    read_tasks.push(async move {
                        let mut buf = [0u8; BLOCK_SIZE];
                        disk.read_block(stripe_id, &mut buf).await.map(|_| (i, buf))
                    });
                }

                while let Some(result) = read_tasks.next().await {
                    let (shard_idx, buf) = result?;
                    let offset = shard_idx * BLOCK_SIZE;
                    data[offset..offset + BLOCK_SIZE].copy_from_slice(&buf);
                    stripe_buf.shard_mask |= 1 << shard_idx;
                }
            }
            
            // Now stripe is complete, write it
            self.write_full_stripe(&stripe_buf, stripe_id).await.unwrap();
        }

        self.stripe.flush().await?;

        Ok(())
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        if let Some(stripe_buf) = self.writes.get(&(stripe as u64)) {
            if stripe_buf.has_shard(data_index) && stripe_buf.get_shard(data_index, buf) {
                return Ok(())
            }
        }

        match self.stripe.read_block(lba, buf).await {
            Ok(()) => return Ok(()),
            Err(_disk_error) => {
                // Primary disk failed, need to reconstruct from stripe
                let mut shards: Vec<Option<Vec<u8>>> = vec![None; DISKS];
                
                // Read all other disks
                let mut futures = self.stripe.disks.iter()
                    .enumerate()
                    .filter(|(i, _)| *i != data_index) // Skip the failed disk
                    .map(|(i, disk)| {
                        async move {
                            let mut shard = vec![0u8; BLOCK_SIZE];
                            disk.read_block(stripe as u64, &mut shard).await?;
                            Ok::<_, RaidZError>((i, shard))
                        }
                    })
                    .collect::<FuturesUnordered<_>>();
                
                let mut successful_reads = 0;
                let mut failed_indices = vec![data_index];
                
                while let Some(result) = futures.next().await {
                    match result {
                        Ok((i, shard)) => {
                            shards[i] = Some(shard);
                            successful_reads += 1;
                            
                            // We need DATA_SHARDS successful reads (3 out of the other 4 disks)
                            if successful_reads >= DATA_SHARDS {
                                match self.rs.reconstruct_data(&mut shards) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        return Err(DiskError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, 
                                            format!("unable to reconstruct data: {}", e))));
                                    }
                                }

                                if let Some(reconstructed) = &shards[data_index] {
                                    buf.copy_from_slice(reconstructed);
                                    return Ok(());
                                } else {
                                    return Err(DiskError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, 
                                        format!("incomplete stripe: {}", stripe))));
                                }
                            }
                        },
                        Err(e) => {
                            if let RaidZError::DiskError(_de) = e {
                                failed_indices.push(failed_indices.len());
                            }
                            
                            // Can tolerate up to PARITY_SHARDS (2) failures total
                            if failed_indices.len() > PARITY_SHARDS {
                                return Err(DiskError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, 
                                        format!("too many failures to reconstruct data"))));
                            }
                        }
                    }
                }
                
                return Err(DiskError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, 
                        format!("incomplete stripe: {}", stripe))));
            }
        }
    }

    async fn write_block(&mut self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;
        
        // Get or create stripe buffer
        let stripe = self.writes
            .entry(stripe_id)
            .or_insert_with(StripeBuffer::new);
        
        stripe.set_shard(shard_idx, buf);
        Ok(())
    }
}

impl RaidZ {
    pub fn is_local_disk(&self, index: usize) -> bool {
        self.stripe.disks[index].is_local()
    }

    pub async fn new(stripe: Stripe) -> Self {
        let rs = 
            ReedSolomon::new(stripe.data_shards(), stripe.len()-stripe.data_shards())
                .expect("reed solomon initialization failure");

        RaidZ { stripe, rs, writes: BTreeMap::new() }
    }

    async fn write_full_stripe(&mut self, stripe_buf: &StripeBuffer, stripe_id: u64) -> Result<(), RaidZError> {
        let data = stripe_buf.data.as_ref().expect("Cannot write stripe without data");

        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        for i in 0..DATA_SHARDS {
            let offset = i * BLOCK_SIZE;
            shards[i].copy_from_slice(&data[offset..offset + BLOCK_SIZE]);
        }

        self.rs.encode(&mut shards)?;

        let write_futures = shards
            .into_iter()
            .zip(self.stripe.disks.iter_mut())
            .enumerate()
            .map(|(disk_idx, (shard, disk))| {
                async move {
                    let block: [u8; BLOCK_SIZE] = shard
                        .try_into()
                        .map_err(|_| (disk_idx, DiskError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid data size"))) )?;
                    match disk.write_block(stripe_id, &block).await {
                        Ok(()) => Ok(disk_idx),
                        Err(e) => Err((disk_idx, e))
                    }
                }
            });

        let results = futures::future::join_all(write_futures).await;

        let mut successes = 0;
        let mut failures = Vec::new();

        for result in results {
            match result {
                Ok(_) => {
                    successes += 1;
                }
                Err((disk_idx, disk_error)) => {
                    println!("WARN: Failed to write stripe {} to disk {}: {:?}", stripe_id, disk_idx, disk_error);
                    failures.push((disk_idx, disk_error));
                }
            }
        }

        // We need at least DATA_SHARDS (3) successful writes to be able to reconstruct
        if successes >= DATA_SHARDS {
            if !failures.is_empty() {
                println!("Stripe {} written with {} successes and {} failures (recoverable)", 
                        stripe_id, successes, failures.len());
            }
            Ok(())
        } else {
            Err(RaidZError::IncompleteStripe(stripe_id))
        }
    }
}