use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError};
use crate::stripe::{DISKS, Stripe};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use crossbeam_queue::ArrayQueue;

pub const DATA_SHARDS: usize = 3;
pub const PARITY_SHARDS: usize = 2;

#[derive(Debug)]
pub enum RaidZError {
    DiskError(DiskError),
    ReedSolomonError(reed_solomon_simd::Error),
    SerializationError(bincode::Error),
    IncompleteStripe(u64),
    NotFormatted,
}

impl fmt::Display for RaidZError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaidZError::DiskError(err) => write!(f, "{}", err),
            RaidZError::ReedSolomonError(err) => write!(f, "Reed Solomon error: {}", err),
            RaidZError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            RaidZError::IncompleteStripe(stripe) => write!(f, "incomplete stripe: {}", stripe),
            RaidZError::NotFormatted => write!(f, "array is not formatted"),
        }
    }
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

impl From<reed_solomon_simd::Error> for RaidZError {
    fn from(error: reed_solomon_simd::Error) -> Self {
        RaidZError::ReedSolomonError(error)
    }
}

#[derive(Debug)]
pub struct RaidZ {
    pub stripe: Stripe,
    writes: Arc<DashMap<u64, Arc<Mutex<StripeBuffer>>>>,
    stripe_pool: Arc<ArrayQueue<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>>,
}

#[async_trait]
impl BlockDevice for RaidZ {
    async fn flush(&self) -> Result<(), DiskError> {
        let stripe_ids: Vec<u64> = self.writes.iter().map(|r| *r.key()).collect();

        for stripe_id in stripe_ids {
            let stripe_buf_arc = match self.writes.get(&stripe_id) {
                Some(entry) => entry.value().clone(),
                None => continue,
            };

            let mut stripe_buf = stripe_buf_arc.lock().await;

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

                stripe_buf
                    .data
                    .get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));

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
                    if let Some(data) = stripe_buf.data.as_mut() {
                        data[offset..offset + BLOCK_SIZE].copy_from_slice(&buf);
                    }
                    stripe_buf.shard_mask |= 1 << shard_idx;
                }
            }

            // Now stripe is complete, write it
            self.write_full_stripe(&stripe_buf, stripe_id)
                .await
                .unwrap();

            if let Some(buffer) = stripe_buf.take_buffer() {
                let _ = self.stripe_pool.push(buffer);
            }

            drop(stripe_buf);
            self.writes.remove(&stripe_id);
        }

        self.stripe.flush().await?;

        Ok(())
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        if let Some(stripe_buf_ref) = self.writes.get(&(stripe as u64)) {
            let stripe_buf = stripe_buf_ref.lock().await;
            if stripe_buf.has_shard(data_index) && stripe_buf.get_shard(data_index, buf) {
                return Ok(());
            }
        }

        match self.stripe.read_block(lba, buf).await {
            Ok(()) => return Ok(()),
            Err(_disk_error) => {
                // Primary disk failed, reconstruct
                match self
                    .reconstruct_from_stripe(stripe as u64, data_index, buf)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(DiskError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{}", e),
                    ))),
                }
            }
        }
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;

        // Get or create stripe buffer
        let stripe = if let Some(stripe_ref) = self.writes.get(&stripe_id) {
            stripe_ref.value().clone()
        } else {
            let buffer = {
                self.stripe_pool.pop()
            };

            let new_stripe = Arc::new(Mutex::new(
                buffer
                    .map(StripeBuffer::with_buffer)
                    .unwrap_or_else(StripeBuffer::new),
            ));

            // 3. Insert into map using Entry API to handle races safely
            match self.writes.entry(stripe_id) {
                dashmap::mapref::entry::Entry::Occupied(o) => {
                    // We lost the race; another thread inserted the stripe while we were getting the buffer.
                    // We discard our 'new_stripe'. To prevent "leaking" the pooled buffer, return it.
                    {
                        // Locking new_stripe is safe/instant as we hold the only reference
                        let mut guard =
                            new_stripe.try_lock().expect("Exclusive ownership expected");
                        if let Some(buf) = guard.take_buffer() {
                            //let mut pool = self.stripe_pool.lock().await;
                            let _ = self.stripe_pool.push(buf);
                        }
                    }
                    o.get().clone()
                }
                dashmap::mapref::entry::Entry::Vacant(v) => {
                    // We won the race, insert our new stripe
                    v.insert(new_stripe.clone());
                    new_stripe
                }
            }
        };

        stripe.lock().await.set_shard(shard_idx, buf);
        Ok(())
    }
}

impl RaidZ {
    pub fn is_local_disk(&self, index: usize) -> bool {
        self.stripe.disks[index].is_local()
    }

    pub async fn new(stripe: Stripe) -> Self {
        RaidZ {
            stripe,
            writes: Arc::new(DashMap::new()),
            stripe_pool: Arc::new(ArrayQueue::new(100)),
        }
    }

    pub async fn with_pool_size(mut self, pool_size: usize) -> Self {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        for _ in 0..pool_size {
            let _ = pool.push(Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
        }
        self.stripe_pool = pool;
        self
    }

    /// Reconstruct a block from other disks in the stripe
    async fn reconstruct_from_stripe(
        &self,
        stripe_id: u64,
        data_index: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        let mut shard_buffers: [[u8; BLOCK_SIZE]; DISKS] = [[0u8; BLOCK_SIZE]; DISKS];
        let mut shard_present: [bool; DISKS] = [false; DISKS];

        // Read all disks except the failed one
        let mut futures = FuturesUnordered::new();
        for (i, disk) in self.stripe.disks.iter().enumerate() {
            if i != data_index {
                futures.push(async move {
                    let mut temp_buf = [0u8; BLOCK_SIZE];
                    disk.read_block(stripe_id, &mut temp_buf)
                        .await
                        .map(|_| (i, temp_buf))
                });
            }
        }

        let mut successful_reads = 0;
        let mut failed_count = 1;

        while let Some(result) = futures.next().await {
            match result {
                Ok((i, data)) => {
                    shard_buffers[i].copy_from_slice(&data);
                    shard_present[i] = true;
                    successful_reads += 1;

                    // We need DATA_SHARDS successful reads to reconstruct
                    if successful_reads >= DATA_SHARDS {
                        return self.reconstruct_shard(
                            &mut shard_buffers,
                            &shard_present,
                            data_index,
                            output,
                        );
                    }
                }
                Err(_) => {
                    failed_count += 1;
                    // Can tolerate up to PARITY_SHARDS (2) failures total
                    if failed_count > PARITY_SHARDS {
                        return Err(RaidZError::IncompleteStripe(stripe_id));
                    }
                }
            }
        }

        Err(RaidZError::IncompleteStripe(stripe_id))
    }

    /// Reconstruct a single shard using Reed-Solomon SIMD
    fn reconstruct_shard(
        &self,
        shard_buffers: &mut [[u8; BLOCK_SIZE]; DISKS],
        shard_present: &[bool; DISKS],
        target_shard: usize,
        output: &mut [u8],
    ) -> Result<(), RaidZError> {
        let mut decoder = ReedSolomonDecoder::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;

        // Add available shards
        for (i, present) in shard_present.iter().enumerate() {
            if *present {
                if i < DATA_SHARDS {
                    decoder.add_original_shard(i, &shard_buffers[i])?;
                } else {
                    decoder.add_recovery_shard(i - DATA_SHARDS, &shard_buffers[i])?;
                }
            }
        }

        let result = decoder.decode()?;

        // Get the reconstructed shard
        if let Some(reconstructed) = result
            .restored_original_iter()
            .find(|(idx, _)| *idx == target_shard)
        {
            output.copy_from_slice(reconstructed.1);
            Ok(())
        } else {
            Err(RaidZError::DiskError(DiskError::IoError(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "target shard not in reconstruction result",
                ),
            )))
        }
    }

    async fn write_full_stripe(
        &self,
        stripe_buf: &StripeBuffer,
        stripe_id: u64,
    ) -> Result<(), RaidZError> {
        let data = stripe_buf
            .data
            .as_ref()
            .expect("Cannot write stripe without data");

        // Use stack-allocated array for all shards
        let mut shard_buffers: [[u8; BLOCK_SIZE]; DISKS] = [[0u8; BLOCK_SIZE]; DISKS];

        // Copy data shards to buffer
        for i in 0..DATA_SHARDS {
            let offset = i * BLOCK_SIZE;
            shard_buffers[i].copy_from_slice(&data[offset..offset + BLOCK_SIZE]);
        }

        // Create encoder and add original shards (zero-copy - just references)
        let mut encoder = ReedSolomonEncoder::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE)?;

        for i in 0..DATA_SHARDS {
            encoder.add_original_shard(&shard_buffers[i])?;
        }

        // Encode to generate parity shards
        let result = encoder.encode()?;

        // Copy parity shards to our buffer
        for (idx, recovery_shard) in result.recovery_iter().enumerate() {
            shard_buffers[DATA_SHARDS + idx].copy_from_slice(recovery_shard);
        }

        // Write all shards concurrently
        let write_futures = shard_buffers
            .iter()
            .zip(self.stripe.disks.iter())
            .enumerate()
            .map(|(disk_idx, (shard, disk))| async move {
                match disk.write_block(stripe_id, shard).await {
                    Ok(()) => Ok(disk_idx),
                    Err(e) => Err((disk_idx, e)),
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
                    eprintln!(
                        "WARN: Failed to write stripe {} to disk {}: {:?}",
                        stripe_id, disk_idx, disk_error
                    );
                    failures.push((disk_idx, disk_error));
                }
            }
        }

        // We need at least DATA_SHARDS (3) successful writes to be able to reconstruct
        if successes >= DATA_SHARDS {
            if !failures.is_empty() {
                eprintln!(
                    "Stripe {} written with {} successes and {} failures (recoverable)",
                    stripe_id,
                    successes,
                    failures.len()
                );
            }
            Ok(())
        } else {
            Err(RaidZError::IncompleteStripe(stripe_id))
        }
    }
}

#[derive(Debug)]
pub struct StripeBuffer {
    data: Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>,
    shard_mask: u8,
    dirty: bool,
    flushed: bool, // For optimistic writes
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

    pub fn with_buffer(mut data: Box<[u8; BLOCK_SIZE * DATA_SHARDS]>) -> Self {
        data.fill(0);
        StripeBuffer {
            data: Some(data),
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }

    pub fn set_shard(&mut self, shard_idx: usize, block_data: &[u8; BLOCK_SIZE]) {
        assert!(shard_idx < DATA_SHARDS);

        let data_ptr = self
            .data
            .get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));

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
        self.shard_mask == 0b111 // All 3 data shards present
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

    pub fn take_buffer(&mut self) -> Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>> {
        self.data.take()
    }
}
