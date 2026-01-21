use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Serialize, Deserialize};
use blake3;
use std::future::AsyncDrop;
use std::pin::Pin;
use futures::future::join_all;
use crate::disk::{Disk,BLOCK_SIZE,BlockId};

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

    pub async fn commit_txg(&mut self, file_index_extent: Vec<Extent>) -> u64 {
        let txg = self.txg_state.txg;

        // Take ownership of writes to avoid borrowing conflicts
        let writes = std::mem::take(&mut self.txg_state.writes);

        // Write all blocks
        for w in writes {
            match w.data {
                TxgPayload::Raw(data) => self.write_raw_block_checked(w.lba, txg, &data).await,
                TxgPayload::Checked(data) => self.write_raw_block_checked(w.lba, txg, &data).await,
            }
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

    /// Write raw bytes to a block with checksum (no serialization)
    async fn write_raw_block_checked(&mut self, lba: BlockId, txg: u64, data: &[u8]) {
        assert!(data.len() <= BLOCK_PAYLOAD_SIZE, 
                "Raw data too large: {} > {}", data.len(), BLOCK_PAYLOAD_SIZE);

        let checksum = *blake3::hash(data).as_bytes();
        
        let header = crate::raidz::BlockHeader {
            magic: 0x52414944, // "RAID"
            txg,
            checksum,
            payload_len: data.len() as u32,
        };

        let mut buf = vec![0u8; BLOCK_SIZE];
        let header_bytes = bincode::serialize(&header).unwrap();
        
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data.len()]
            .copy_from_slice(data);

        self.write_logical_block(lba, &buf).await;
    }

    /// Read raw bytes from a block with checksum validation (no deserialization)
    pub async fn read_raw_block_checked(&mut self, lba: BlockId) -> Option<(u64, Vec<u8>)> {
        // Check pending writes first 
        let cur_txg = self.txg_state.txg;
        for write in self.txg_state.writes.iter().rev() {
            if write.lba == lba {
                match &write.data {
                    TxgPayload::Raw(d) => return Some((cur_txg, d.clone())),
                    TxgPayload::Checked(d) => (),
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

    pub async fn write_block_checked<T: Serialize>(
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

    async fn write_logical_block(&mut self, lba: BlockId, data: &[u8]) {
        let stripe = lba as usize / DATA_SHARDS;
        let data_index = lba as usize % DATA_SHARDS;

        // In a simulation, if we aren't doing full stripe writes, 
        // we must ensure we don't have a race condition on the parity.
        let mut shards = vec![vec![0u8; BLOCK_SIZE]; DISKS];

        // TEMPORARY: Zero-fill instead of reading to test if RMW is the cause
        // for (i, shard) in shards.iter_mut().enumerate() {
        //     self.disks[i].read_block(stripe as u64, shard);
        // }

        for (i, shard) in shards.iter_mut().enumerate() {
            self.disks[i].read_block(stripe as u64, shard).await;
        }

        shards[data_index][..data.len()].copy_from_slice(data);
        self.rs.encode(&mut shards).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            self.disks[i].write_block(stripe as u64, shard).await;
        }
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
}