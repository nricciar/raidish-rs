use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Serialize, Deserialize};
use blake3;
use std::future::AsyncDrop;
use std::pin::Pin;
use futures::future::join_all;
use crate::disk::{Disk,BLOCK_SIZE,BlockId};
use std::collections::BTreeMap;

pub const DISKS: usize = 5;
pub const DATA_SHARDS: usize = 3;
pub const PARITY_SHARDS: usize = 2;
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();
pub const BLOCK_PAYLOAD_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const UBERBLOCK_START: u64 = 1;
pub const UBERBLOCK_COUNT: u64 = 16;
pub const UBERBLOCK_MAGIC: u64 = 0x5241494449534855; // "RAIDISHU"

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
        
        // Lazy allocation on first write
        if self.data.is_none() {
            self.data = Some(Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
        }
        
        let offset = shard_idx * BLOCK_SIZE;
        self.data.as_mut().unwrap()[offset..offset + BLOCK_SIZE]
            .copy_from_slice(block_data);
        
        self.shard_mask |= 1 << shard_idx;
        self.dirty = true;
        self.flushed = false; // Mark as needing flush again
    }
    
    fn has_shard(&self, shard_idx: usize) -> bool {
        (self.shard_mask & (1 << shard_idx)) != 0
    }
    
    fn is_complete(&self) -> bool {
        self.shard_mask == 0b111  // All 3 data shards present
    }
    
    fn get_shard(&self, shard_idx: usize, buf: &mut [u8]) -> bool {
        if !self.has_shard(shard_idx) {
            return false;
        }
        let offset = shard_idx * BLOCK_SIZE;
        buf.copy_from_slice(&self.data.as_ref().unwrap()[offset..offset + BLOCK_SIZE]);
        true
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
    rs: ReedSolomon,
    txg_state: TxgState
}

impl AsyncDrop for RaidZ {
    async fn drop(mut self: Pin<&mut RaidZ>) {
        self.sync().await
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

    pub async fn new(paths: [&str; DISKS]) -> Self {
        // Open all disks concurrently
        let disk_futures: Vec<_> = paths.into_iter().map(|path| Disk::open(path)).collect();
        let disks: Vec<Disk> = join_all(disk_futures).await;

        let rs = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).expect("reed solomon initialization failure");

        // find our last txg and setup txg_state
        let txg_state = TxgState { txg: 0, uberblock: None, stripes: BTreeMap::new() };
        let mut rz = RaidZ { disks, rs, txg_state };
        rz.scan_uberblocks().await;

        rz
    }

    async fn scan_uberblocks(&mut self) -> Option<(u64, Uberblock)> {
        let mut best: Option<(u64, Uberblock)> = None;

        for i in 0..UBERBLOCK_COUNT {
            let lba = UBERBLOCK_START + i;
            if let Some((txg, ub)) = self.read_block_checked::<Uberblock>(lba).await {
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

    pub fn format(&mut self) {
        self.txg_state.txg = 0;
        // Zero out uberblock slots (TXG 0)
        let zero_uber = Uberblock {
            magic: 0,
            version: 0,
            txg: 0,
            timestamp: 0,
            file_index_extent: Vec::new(),
        };
        for slot in 0..UBERBLOCK_COUNT {
            self.write_block_checked_txg(UBERBLOCK_START + slot, &zero_uber);
        }
    }

    pub async fn sync(&mut self) {
        //println!("sync called with {} pending writes", self.txg_state.writes.len());
        if !self.txg_state.stripes.is_empty() {
            let uber = self.txg_state.uberblock.as_ref().expect("cannot sync to a device with no uberblocks");
            self.commit_txg(uber.file_index_extent.clone()).await;
        }
    }

    pub fn write_raw_block_checked_txg(&mut self, lba: BlockId, data: &[u8]) {
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;
        
        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(self.txg_state.txg, data, &mut block_buf);
        
        // Get or create stripe buffer
        let stripe = self.txg_state.stripes
            .entry(stripe_id)
            .or_insert_with(StripeBuffer::new);
        
        stripe.set_shard(shard_idx, &block_buf);
    }

    pub fn write_block_checked_txg<T: Serialize>(&mut self, lba: BlockId, value: &T) {
        let payload = bincode::serialize(value).unwrap();
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;
        
        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(self.txg_state.txg, &payload, &mut block_buf);
        
        // Get or create stripe buffer
        let stripe = self.txg_state.stripes
            .entry(stripe_id)
            .or_insert_with(StripeBuffer::new);
        
        // Convert Vec to array reference for set_shard
        stripe.set_shard(shard_idx, &block_buf);
    }

    fn prepare_block_buffer(&self, txg: u64, data: &[u8], buf: &mut [u8; BLOCK_SIZE]) {
        let checksum = *blake3::hash(data).as_bytes();
        let header = BlockHeader {
            magic: 0x52414944,
            txg,
            checksum,
            payload_len: data.len() as u32,
        };
        buf.fill(0);
        let header_bytes = bincode::serialize(&header).unwrap();
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data.len()].copy_from_slice(data);
    }

    pub async fn commit_txg(&mut self, file_index_extent: Vec<Extent>) -> u64 {
        let txg = self.txg_state.txg;

        // Take ownership of stripes
        let stripes = std::mem::take(&mut self.txg_state.stripes);

        for (stripe_id, mut stripe_buf) in stripes {
            // Check if already flushed
            if stripe_buf.flushed && !stripe_buf.dirty {
                continue;
            }
            
            // Handle incomplete stripes (RMW)
            // FIXME: if we can ensure that all writes are full stripes we can get rid of this
            if !stripe_buf.is_complete() {
                //println!("WARN: RMW operation in commit_txg for stripe {}", stripe_id);
                
                if stripe_buf.data.is_none() {
                    stripe_buf.data = Some(Box::new([0u8; BLOCK_SIZE * DATA_SHARDS]));
                }
                
                let mut missing_shards = Vec::new();
                for i in 0..DATA_SHARDS {
                    if !stripe_buf.has_shard(i) {
                        missing_shards.push(i);
                    }
                }

                // read missing shards
                let data = stripe_buf.data.as_mut().unwrap();
                for i in missing_shards {
                    let offset = i * BLOCK_SIZE;
                    self.disks[i].read_block(stripe_id, &mut data[offset..offset + BLOCK_SIZE]).await;
                }
            }
            
            // Now stripe is complete, write it
            self.write_full_stripe(&stripe_buf, stripe_id).await;
        }

        // Flush all disks to ensure all blocks are durable before uberblock
        self.flush_all_disks();

        // Rotate and write uberblock
        let uberblock_lba = UBERBLOCK_START + (txg % UBERBLOCK_COUNT) as u64;
        let uberblock = Uberblock {
            magic: UBERBLOCK_MAGIC,
            version: 1,
            txg,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            file_index_extent,
        };

        self.write_block_checked(uberblock_lba, txg, &uberblock).await;

        // Flush again to ensure uberblock is durable
        self.flush_all_disks();

        // Clear TXG writes and increment txg
        self.txg_state = TxgState { txg: txg + 1, uberblock: Some(uberblock), stripes: BTreeMap::new() };

        txg
    }

    fn flush_all_disks(&mut self) {
        for disk in &mut self.disks {
            disk.flush();
        }
    }

    async fn write_full_stripe(&mut self, stripe_buf: &StripeBuffer, stripe_id: u64) {
        let data = stripe_buf.data.as_ref().expect("Cannot write stripe without data");

        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        for i in 0..DATA_SHARDS {
            let offset = i * BLOCK_SIZE;
            shards[i].copy_from_slice(&data[offset..offset + BLOCK_SIZE]);
        }

        self.rs.encode(&mut shards).expect("RS encoding failed");

        let write_futures = shards
            .into_iter()
            .zip(self.disks.iter_mut())
            .map(|(shard, disk)| {
                async move {
                    disk.write_block(stripe_id, &shard).await
                }
            });

        futures::future::join_all(write_futures).await;
    }

    /// Refactored to take a mutable buffer instead of returning a Vec
    pub async fn read_raw_block_checked_into(
        &mut self,
        lba: BlockId,
        buf: &mut [u8],
    ) -> Result<(u64, usize), String> {
        assert!(buf.len() >= BLOCK_SIZE);

        self.read_logical_block(lba, buf).await;
        
        let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])
            .map_err(|e| e.to_string())?;

        if header.magic != 0x52414944 {
            return Err("Invalid magic number".to_string());
        }

        let payload_len = header.payload_len as usize;
        let payload = &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + header.payload_len as usize];

        // Checksum verification
        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return Err("Checksum mismatch".to_string());
        }

        buf.copy_within(BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload_len, 0);

        Ok((header.txg, payload_len))
    }

    pub async fn read_block_checked<T: for<'a> Deserialize<'a>>(
        &mut self,
        lba: BlockId,
    ) -> Option<(u64, T)> {
        let mut buf = vec![0u8; BLOCK_SIZE];
        let stripe_id = lba / DATA_SHARDS as u64;
        let shard_idx = (lba % DATA_SHARDS as u64) as usize;

        match self.txg_state.stripes.get(&stripe_id) {
            Some(stripe_buf) if stripe_buf.has_shard(shard_idx) => {
                stripe_buf.get_shard(shard_idx, &mut buf);
            },
            Some(_) |
            None => {
                self.read_logical_block(lba, &mut buf).await;
            }
        }

        let header: BlockHeader =
            bincode::deserialize(&buf[..BLOCK_HEADER_SIZE]).ok()?;

        let payload_start = BLOCK_HEADER_SIZE;
        let payload_end = payload_start + header.payload_len as usize;
        let payload = &buf[payload_start..payload_end];

        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return None;
        }

        let value = bincode::deserialize(payload).ok()?;
        Some((header.txg, value))
    }

    async fn read_logical_block(&mut self, lba: BlockId, buf: &mut [u8]) {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        let futures: Vec<_> = self.disks.iter()
            .enumerate()
            .map(|(i, disk)| {
                async move {
                    let mut shard = vec![0u8; BLOCK_SIZE];
                    disk.read_block(stripe as u64, &mut shard).await;
                    (i, shard)
                }
            })
            .collect();
        
        let results = join_all(futures).await;
        
        for (i, shard) in results {
            shards[i] = shard;
        }

        // Reconstruction would go here if shards missing
        buf.copy_from_slice(&shards[data_index]);
    }

    // FIXME: need to remove only used for writing the uberblocks
    async fn write_logical_block(&mut self, lba: BlockId, data: &[u8]) {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        for (i, shard) in shards.iter_mut().enumerate() {
            self.disks[i].read_block(stripe as u64, shard).await;
        }

        shards[data_index][..data.len()].copy_from_slice(data);
        self.rs.encode(&mut shards).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            self.disks[i].write_block(stripe as u64, shard).await;
        }
    }

    // FIXME: need to remove only used for writing the uberblocks
    async fn write_block_checked<T: Serialize>(
        &mut self,
        lba: BlockId,
        txg: u64,
        value: &T,
    ) {
        let payload = bincode::serialize(value).unwrap();
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        let checksum = *blake3::hash(&payload).as_bytes();

        let header = BlockHeader {
            magic: 0x52414944, // "RAID"
            txg,
            checksum,
            payload_len: payload.len() as u32,
        };

        let mut buf = vec![0u8; BLOCK_SIZE];
        let header_bytes = bincode::serialize(&header).unwrap();

        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload.len()]
            .copy_from_slice(&payload);

        self.write_logical_block(lba, &buf).await;
    }
}