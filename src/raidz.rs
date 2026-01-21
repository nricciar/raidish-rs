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
enum TxgPayload {
    Raw(Vec<u8>),
    Checked(Vec<u8>), // Already serialized + ready to write
}

#[derive(Debug)]
struct TxgWrite {
    lba: BlockId,
    data: TxgPayload,
}

#[derive(Debug)]
struct TxgState {
    txg: u64,
    uberblock: Option<Uberblock>,
    writes: Vec<TxgWrite>,
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
        //let disks: Vec<Disk> = paths.into_iter().map(Disk::open).collect();
        let rs = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).unwrap();
        let txg_state = TxgState { txg: 0, uberblock: None, writes: Vec::new() };

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
                if best.is_none() || txg > best.as_ref().unwrap().0 {
                    best = Some((txg, ub));
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
        if !self.txg_state.writes.is_empty() {
            let uber = self.txg_state.uberblock.as_ref().expect("cannot sync to a device with no uberblocks");
            self.commit_txg(uber.file_index_extent.clone()).await;
        }
    }

    pub fn write_raw_block_checked_txg(&mut self, lba: BlockId, data: &[u8]) {
        // Check if we already have a pending write for this block
        let mut found_index = None;
        for (i, write) in self.txg_state.writes.iter().enumerate().rev() {
            if write.lba == lba {
                found_index = Some(i);
                break;
            }
        }

        let new_write = TxgWrite {
            lba,
            data: TxgPayload::Raw(data.to_vec()),
        };

        if let Some(idx) = found_index {
            // Replace existing pending write (avoid duplicates)
            self.txg_state.writes[idx] = new_write;
        } else {
            // Add new pending write
            self.txg_state.writes.push(new_write);
        }
    }

    pub fn write_block_checked_txg<T: Serialize>(&mut self, lba: BlockId, value: &T) {
        let payload = bincode::serialize(value).unwrap();
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        // Same deduplication logic
        let mut found_index = None;
        for (i, write) in self.txg_state.writes.iter().enumerate().rev() {
            if write.lba == lba {
                found_index = Some(i);
                break;
            }
        }

        let new_write = TxgWrite {
            lba,
            data: TxgPayload::Checked(payload),
        };

        if let Some(idx) = found_index {
            self.txg_state.writes[idx] = new_write;
        } else {
            self.txg_state.writes.push(new_write);
        }
    }

    fn prepare_block_buffer(&self, txg: u64, data: &[u8]) -> Vec<u8> {
        let checksum = *blake3::hash(data).as_bytes();
        let header = BlockHeader {
            magic: 0x52414944,
            txg,
            checksum,
            payload_len: data.len() as u32,
        };
        let mut buf = vec![0u8; BLOCK_SIZE];
        let header_bytes = bincode::serialize(&header).unwrap();
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data.len()].copy_from_slice(data);
        buf
    }

    pub async fn commit_txg(&mut self, file_index_extent: Vec<Extent>) -> u64 {
        let txg = self.txg_state.txg;

        // Take ownership of writes to avoid borrowing conflicts
        let writes = std::mem::take(&mut self.txg_state.writes);

        // Group writes by Stripe ID
        let mut stripes: BTreeMap<u64, Vec<Option<Vec<u8>>>> = BTreeMap::new();
        for w in writes {
            let data = match w.data {
                TxgPayload::Raw(d) | TxgPayload::Checked(d) => d,
            };
            
            // Re-package into the block format (Header + Payload)
            let full_block = self.prepare_block_buffer(txg, &data);
            
            let stripe_id = w.lba / DATA_SHARDS as u64;
            let shard_index = (w.lba % DATA_SHARDS as u64) as usize;
            
            let stripe_entry = stripes.entry(stripe_id)
                .or_insert_with(|| vec![None; DATA_SHARDS]);
            stripe_entry[shard_index] = Some(full_block);
        }

        // TODO: Write full stripes
        // FIXME: this should eventually be unnessicary once everything is fully on board with the
        // stripe alignment and using the write_block_checked_txg method
        for (stripe_id, mut data_shards) in stripes {
            for i in 0..DATA_SHARDS {
                if data_shards[i].is_none() {
                    //println!("WARN: RMW: operation in commit_txg for stripe {}", stripe_id);
                    let mut existing = vec![0u8; BLOCK_SIZE];
                    self.disks[i].read_block(stripe_id, &mut existing).await;
                    data_shards[i] = Some(existing);
                }
            }
            self.write_full_stripe(stripe_id, data_shards).await;
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
        self.txg_state = TxgState { txg: txg + 1, uberblock: Some(uberblock), writes: Vec::new() };

        txg
    }

    fn flush_all_disks(&mut self) {
        for disk in &mut self.disks {
            disk.flush();
        }
    }

    async fn write_full_stripe(&mut self, stripe_id: u64, data_shards: Vec<Option<Vec<u8>>>) {
        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        for (i, data) in data_shards.into_iter().enumerate() {
            if let Some(d) = data {
                shards[i] = d;
            }
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

    pub async fn read_raw_block_checked(&mut self, lba: BlockId) -> Option<(u64, Vec<u8>)> {
        // Check pending writes first 
        let cur_txg = self.txg_state.txg;
        for write in self.txg_state.writes.iter().rev() {
            if write.lba == lba {
                match &write.data {
                    TxgPayload::Raw(d) => return Some((cur_txg, d.clone())),
                    TxgPayload::Checked(_) => (),
                };
            }
        }

        let mut buf = vec![0u8; BLOCK_SIZE];
        self.read_logical_block(lba, &mut buf).await;

        let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE]).ok()?;

        let payload_start = BLOCK_HEADER_SIZE;
        let payload_end = payload_start + header.payload_len as usize;
        let payload = &buf[payload_start..payload_end];

        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return None;
        }

        Some((header.txg, payload.to_vec()))
    }

    pub async fn read_block_checked<T: for<'a> Deserialize<'a>>(
        &mut self,
        lba: BlockId,
    ) -> Option<(u64, T)> {
        // Check pending writes first 
        let cur_txg = self.txg_state.txg;
        for write in self.txg_state.writes.iter().rev() {
            if write.lba == lba {
                match &write.data {
                    TxgPayload::Raw(_) => (),
                    TxgPayload::Checked(d) => {
                        let value = bincode::deserialize(d).ok()?;
                        return Some((cur_txg, value))
                    },
                };
            }
        }

        let mut buf = vec![0u8; BLOCK_SIZE];
        self.read_logical_block(lba, &mut buf).await;

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