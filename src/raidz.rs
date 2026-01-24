use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Serialize, Deserialize};
use blake3;
use std::future::AsyncDrop;
use std::pin::Pin;
use futures::future::join_all;
use crate::disk::{BLOCK_SIZE, BlockId, Disk, DiskError};
use std::collections::BTreeMap;
use futures::stream::{FuturesUnordered, StreamExt};

pub const FS_ID: &str = "RAIDISHV10";
pub const DISKS: usize = 5;
pub const DATA_SHARDS: usize = 3;
pub const PARITY_SHARDS: usize = 2;
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();
pub const BLOCK_PAYLOAD_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const SUPERBLOCK_LBA: u64 = 0;
pub const UBERBLOCK_START: u64 = 3;
pub const UBERBLOCK_BLOCK_COUNT: u64 = 15;
pub const UBERBLOCK_STRIPE_COUNT: u64 = UBERBLOCK_BLOCK_COUNT / 3; // needs to be a multiple of DATA_SHARDS
pub const UBERBLOCK_MAGIC: u64 = 0x5241494449534855; // "RAIDISHU"
pub const METASLAB_SIZE_BLOCKS: u64 = 1026; // 1026 is stripe alined
pub const METASLAB_TABLE_START: u64 = UBERBLOCK_START + UBERBLOCK_BLOCK_COUNT;
pub const SPACEMAP_LOG_BLOCKS_PER_METASLAB: u64 = 16;

#[derive(Debug)]
pub enum RaidZError {
    DiskError(DiskError),
    ReedSolomonError(reed_solomon_erasure::Error),
    SerializationError(bincode::Error),
    ChecksumMismatch,
    InvalidMagic,
    IncompleteStripe(u64),
    NotFormatted
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
struct StripeBuffer {
    data: Option<Box<[u8; BLOCK_SIZE * DATA_SHARDS]>>,
    shard_mask: u8,
    dirty: bool,
    flushed: bool,  // For optimistic writes
}

impl StripeBuffer {
    fn new() -> Self {
        StripeBuffer {
            data: None,
            shard_mask: 0,
            dirty: false,
            flushed: false,
        }
    }
    
    fn set_shard(&mut self, shard_idx: usize, block_data: &[u8; BLOCK_SIZE]) {
        assert!(shard_idx < DATA_SHARDS);
        
        let data_ptr = self.data.get_or_insert_with(|| Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
        
        let offset = shard_idx * BLOCK_SIZE;
        data_ptr[offset..offset + BLOCK_SIZE].copy_from_slice(block_data);
        
        self.shard_mask |= 1 << shard_idx;
        self.dirty = true;
        self.flushed = false;
    }
    
    fn has_shard(&self, shard_idx: usize) -> bool {
        (self.shard_mask & (1 << shard_idx)) != 0
    }
    
    fn is_complete(&self) -> bool {
        self.shard_mask == 0b111  // All 3 data shards present
    }
    
    fn get_shard(&self, shard_idx: usize, buf: &mut [u8]) -> bool {
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
struct TxgState {
    txg: u64,
    uberblock: Option<Uberblock>,
    stripes: BTreeMap<u64, StripeBuffer>,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub magic: u32,
    pub txg: u64,
    pub checksum: [u8; 32],
    pub payload_len: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Superblock {
    pub magic: String,
    pub block_size: u32,
    pub total_blocks: u64,
    pub metaslab_count: u32,
    pub metaslab_size_blocks: u64,
    pub metaslab_table_start: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Uberblock {
    pub magic: u64,
    pub version: u32,
    pub txg: u64,              // transaction group
    pub timestamp: u64,        // optional but useful
    pub file_index_extent: Vec<Extent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Extent {
    pub start: BlockId,
    pub len: u64, // number of blocks
}

#[derive(Debug)]
pub struct RaidZ {
    pub disks: Vec<Disk>,
    superblock: Option<Superblock>,
    rs: ReedSolomon,
    txg_state: TxgState
}

impl AsyncDrop for RaidZ {
    async fn drop(mut self: Pin<&mut RaidZ>) {
        self.sync().await.expect("sync failure")
    }
}
impl Drop for RaidZ {
    fn drop(self: &mut RaidZ) {

    }
}

impl RaidZ {
    pub fn is_local_disk(&self, index: usize) -> bool {
        self.disks[index].is_local()
    }

    pub fn superblock(&self) -> Option<&Superblock> {
        self.superblock.as_ref()
    }

    pub fn uberblock(&self) -> Option<&Uberblock> {
        self.txg_state.uberblock.as_ref()
    }

    pub fn is_formatted(&self) -> bool {
        self.superblock.is_some() && self.txg_state.uberblock.is_some()
    }

    pub fn txg(&self) -> u64 {
        self.txg_state.txg
    }

    pub async fn new(paths: [&str; DISKS]) -> Self {
        // Open all disks concurrently
        let disk_futures: Vec<_> = paths.into_iter().map(|path| Disk::open(path)).collect();
        let (disks, errors): (Vec<_>, Vec<_>) =
            join_all(disk_futures)
                .await
                .into_iter()
                .fold((Vec::new(), Vec::new()), |(mut oks, mut errs), r| {
                    match r {
                        Ok(d) => oks.push(d),
                        Err(e) => errs.push(e),
                    }
                    (oks, errs)
                });

        if errors.len() > 0 {
            // FIXME: this should be more descriptive and/or pass errors back as a Result
            panic!("disk open failure")
        }

        let rs = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).expect("reed solomon initialization failure");

        // find our last txg and setup txg_state
        let txg_state = TxgState { txg: 0, uberblock: None, stripes: BTreeMap::new() };
        let mut rz = RaidZ { disks, rs, txg_state, superblock: None };
        rz.scan_uberblocks().await;

        rz
    }

    async fn scan_uberblocks(&mut self) -> Option<(u64, Uberblock)> {
        // Load superblock (TXG 0 from format)
        match self.read_block_checked::<Superblock>(SUPERBLOCK_LBA).await {
            Ok((_, superblock)) => {
                self.superblock = Some(superblock)
            },
            Err(e) => {
                println!("WARNING! {:?}", e);
            }
        }

        let mut best: Option<(u64, Uberblock)> = None;

        for i in 0..UBERBLOCK_BLOCK_COUNT {
            let lba = UBERBLOCK_START + i;
            if let Ok((txg, ub)) = self.read_block_checked::<Uberblock>(lba).await {
                if ub.magic != UBERBLOCK_MAGIC {
                    continue;
                }
                match &best {
                    None => {
                        best = Some((txg, ub));
                    }
                    Some((best_txg, _)) if txg > *best_txg => {
                        best = Some((txg, ub));
                    }
                    _ => {}
                }
            }
        }

        // Initialize TXG state from the latest committed uberblock
        if let Some((txg, uberblock)) = best.clone() {
            self.txg_state.txg = txg + 1;
            self.txg_state.uberblock = Some(uberblock);
        }

        best
    }

    pub fn format(&mut self, total_blocks: u64) -> Result<(), RaidZError> {
        let metaslab_count = (total_blocks / METASLAB_SIZE_BLOCKS) as u32;

        let superblock = Superblock {
            magic: FS_ID.to_string(),
            block_size: BLOCK_SIZE as u32,
            total_blocks,
            metaslab_count,
            metaslab_size_blocks: METASLAB_SIZE_BLOCKS,
            metaslab_table_start: METASLAB_TABLE_START,
        };
        
        // TXG 0 is reserved for initial format
        for mirror_idx in 0..3 {
            self.write_block_checked_txg(SUPERBLOCK_LBA + mirror_idx, &superblock)?;
        }

        self.txg_state.txg = 0;
        // Zero out uberblock slots (TXG 0)
        let zero_uber = Uberblock {
            magic: 0,
            version: 0,
            txg: 0,
            timestamp: 0,
            file_index_extent: Vec::new(),
        };
        for slot in 0..UBERBLOCK_BLOCK_COUNT {
            self.write_block_checked_txg(UBERBLOCK_START + slot, &zero_uber)?;
        }
        Ok(())
    }

    pub async fn sync(&mut self) -> Result<(), RaidZError> {
        //println!("sync called with {} pending writes", self.txg_state.writes.len());
        if !self.txg_state.stripes.is_empty() {
            let uber = self.txg_state.uberblock.as_ref().ok_or(RaidZError::NotFormatted)?;
            self.commit_txg(uber.file_index_extent.clone()).await?;
        }
        Ok(())
    }

    pub fn write_raw_block_checked_txg(&mut self, lba: BlockId, data: &[u8]) -> Result<(), RaidZError> {
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;
        
        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(self.txg_state.txg, data, &mut block_buf)?;
        
        // Get or create stripe buffer
        let stripe = self.txg_state.stripes
            .entry(stripe_id)
            .or_insert_with(StripeBuffer::new);
        
        stripe.set_shard(shard_idx, &block_buf);
        Ok(())
    }

    pub fn write_block_checked_txg<T: Serialize>(&mut self, lba: BlockId, value: &T) -> Result<(), RaidZError> {
        let payload = bincode::serialize(value)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;
        
        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(self.txg_state.txg, &payload, &mut block_buf)?;
        
        // Get or create stripe buffer
        let stripe = self.txg_state.stripes
            .entry(stripe_id)
            .or_insert_with(StripeBuffer::new);
        
        // Convert Vec to array reference for set_shard
        stripe.set_shard(shard_idx, &block_buf);
        Ok(())
    }

    fn prepare_block_buffer(&self, txg: u64, data: &[u8], buf: &mut [u8; BLOCK_SIZE]) -> Result<(), RaidZError> {
        let checksum = *blake3::hash(data).as_bytes();
        let header = BlockHeader {
            magic: 0x52414944,
            txg,
            checksum,
            payload_len: data.len() as u32,
        };
        buf.fill(0);
        let header_bytes = bincode::serialize(&header)?;
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data.len()].copy_from_slice(data);
        Ok(())
    }

    pub async fn commit_txg(&mut self, file_index_extent: Vec<Extent>) -> Result<u64, RaidZError> {
        let txg = self.txg_state.txg;

        // Take ownership of stripes
        let stripes = std::mem::take(&mut self.txg_state.stripes);

        // increment the txg for the buffer
        self.txg_state.txg += 1;

        for (stripe_id, mut stripe_buf) in stripes {
            // Check if already flushed
            if stripe_buf.flushed && !stripe_buf.dirty {
                continue;
            }
            
            // Handle incomplete stripes (RMW)
            // FIXME: if we can ensure that all writes are full stripes we can get rid of this
            if !stripe_buf.is_complete() {
                //println!("WARN: RMW operation in commit_txg for stripe {}", stripe_id);
                // Identify missing shards before borrowing the data buffer
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
                    let disk = &self.disks[i];
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
            self.write_full_stripe(&stripe_buf, stripe_id).await?;
        }

        // Flush all disks to ensure all blocks are durable before uberblock
        self.flush_all_disks();

        // Rotate and write uberblock
        //let uberblock_lba = UBERBLOCK_START + (txg % UBERBLOCK_COUNT) as u64;
        let uberblock = Uberblock {
            magic: UBERBLOCK_MAGIC,
            version: 1,
            txg,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            file_index_extent,
        };

        // Determine which stripe to use (rotating through UBERBLOCK_COUNT stripes)
        let uberblock_stripe_idx = (txg % UBERBLOCK_STRIPE_COUNT) as u64;
        let uberblock_stripe_id = (UBERBLOCK_START / DATA_SHARDS as u64) + uberblock_stripe_idx;

        // Serialize and prepare the uberblock block
        let payload = bincode::serialize(&uberblock)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);
        
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(txg, &payload, &mut block_buf)?;

        let mut uber_stripe = StripeBuffer::new();
        uber_stripe.set_shard(0, &block_buf);
        uber_stripe.set_shard(1, &block_buf);
        uber_stripe.set_shard(2, &block_buf);
        self.write_full_stripe(&uber_stripe, uberblock_stripe_id).await?;

        // Flush again to ensure uberblock is durable
        self.flush_all_disks();

        self.txg_state.uberblock = Some(uberblock);

        Ok(txg)
    }

    fn flush_all_disks(&mut self) {
        for disk in &mut self.disks {
            disk.flush();
        }
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
            .zip(self.disks.iter_mut())
            .enumerate()
            .map(|(disk_idx, (shard, disk))| {
                async move {
                    match disk.write_block(stripe_id, &shard).await {
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

    /// Refactored to take a mutable buffer instead of returning a Vec
    pub async fn read_raw_block_checked_into(
        &mut self,
        lba: BlockId,
        buf: &mut [u8],
    ) -> Result<(u64, usize), RaidZError> {
        assert!(buf.len() >= BLOCK_SIZE);

        self.read_logical_block(lba, buf).await?;
        
        let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;

        if header.magic != 0x52414944 {
            return Err(RaidZError::InvalidMagic)
        }

        let payload_len = header.payload_len as usize;
        let payload = &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + header.payload_len as usize];

        // Checksum verification
        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return Err(RaidZError::ChecksumMismatch);
        }

        buf.copy_within(BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload_len, 0);

        Ok((header.txg, payload_len))
    }

    pub async fn read_block_checked<T: for<'a> Deserialize<'a>>(
        &mut self,
        lba: BlockId,
    ) -> Result<(u64, T), RaidZError> {
        let mut buf = vec![0u8; BLOCK_SIZE];
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;

        match self.txg_state.stripes.get(&stripe_id) {
            Some(stripe_buf) if stripe_buf.has_shard(shard_idx) => {
                stripe_buf.get_shard(shard_idx, &mut buf);
            },
            Some(_) |
            None => {
                self.read_logical_block(lba, &mut buf).await?;
            }
        }

        let header: BlockHeader =
            bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;

        let payload_start = BLOCK_HEADER_SIZE;
        let payload_end = payload_start + header.payload_len as usize;
        let payload = &buf[payload_start..payload_end];

        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return Err(RaidZError::ChecksumMismatch);
        }

        let value = bincode::deserialize(payload)?;
        Ok((header.txg, value))
    }

    async fn read_logical_block(&mut self, lba: BlockId, buf: &mut [u8]) -> Result<(), RaidZError> {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;
        
        match self.disks[data_index].read_block(stripe as u64, buf).await {
            Ok(()) => return Ok(()),
            Err(_disk_error) => {
                // Primary disk failed, need to reconstruct from stripe
                let mut shards: Vec<Option<Vec<u8>>> = vec![None; DISKS];
                
                // Read all other disks
                let mut futures = self.disks.iter()
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
                                self.rs.reconstruct_data(&mut shards)?;

                                if let Some(reconstructed) = &shards[data_index] {
                                    buf.copy_from_slice(reconstructed);
                                    return Ok(());
                                } else {
                                    return Err(RaidZError::IncompleteStripe(stripe as u64));
                                }
                            }
                        },
                        Err(e) => {
                            if let RaidZError::DiskError(_de) = e {
                                failed_indices.push(failed_indices.len());
                            }
                            
                            // Can tolerate up to PARITY_SHARDS (2) failures total
                            if failed_indices.len() > PARITY_SHARDS {
                                // Too many failures to reconstruct
                                return Err(RaidZError::IncompleteStripe(stripe as u64));
                            }
                        }
                    }
                }
                
                Err(RaidZError::IncompleteStripe(stripe as u64))
            }
        }
    }
}