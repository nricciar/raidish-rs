//! Block pointers and the object layer.
//!
//! Everything except the self-describing labels (superblock, uberblocks and
//! space map slots) is stored as raw, full-size blocks with no in-block
//! header. Integrity lives in the *parent*: a `BlockPtr` records where a
//! record is, how big it is, the TXG it was born in and its checksum. A
//! corrupted or misdirected block can therefore never vouch for itself.
//!
//! An *object* (file contents, an inode, a directory index) is a byte string
//! stored as a list of records. Each record is a contiguous, stripe-aligned
//! run of at most `max_record_blocks()` blocks, so a record never shares a
//! RaidZ stripe with anything else.

use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Extent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BlockPtr {
    /// First block of the record.
    pub lba: u64,
    /// Blocks allocated to the record (stripe aligned; may exceed what `lsize` needs).
    pub asize: u32,
    /// Logical bytes stored in the record.
    pub lsize: u32,
    /// TXG in which the record was written.
    pub birth_txg: u64,
    /// blake3 of the record's `lsize` bytes.
    pub checksum: [u8; 32],
}

impl BlockPtr {
    /// The space this record occupies on disk.
    pub fn extent(&self) -> Extent {
        Extent {
            start: self.lba,
            len: self.asize as u64,
        }
    }

    /// Blocks that actually hold data and need to be read.
    pub fn data_blocks(&self) -> u64 {
        (self.lsize as u64).div_ceil(BLOCK_SIZE as u64)
    }
}

/// Logical size of an object.
pub fn object_size(ptrs: &[BlockPtr]) -> u64 {
    ptrs.iter().map(|p| p.lsize as u64).sum()
}

impl<D: BlockDevice> FileSystem<D> {
    /// Allocate space for `data`, write it, and return the record pointers.
    /// The records belong to the open TXG.
    pub(crate) async fn write_object(&self, data: &[u8]) -> Result<Vec<BlockPtr>, FileSystemError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let txg = self.open_txg().await?;
        let blocks_needed = data.len().div_ceil(BLOCK_SIZE) as u64;
        let extents = self.allocate_blocks_stripe_aligned(blocks_needed).await?;

        let mut ptrs = Vec::new();
        let mut rest = data;
        for extent in extents {
            let mut lba = extent.start;
            let end = extent.start + extent.len;
            while lba < end {
                let asize = self.max_record_blocks().min(end - lba);
                let take = rest.len().min(asize as usize * BLOCK_SIZE);
                let (chunk, tail) = rest.split_at(take);
                rest = tail;

                self.write_record_blocks(lba, chunk).await?;
                ptrs.push(BlockPtr {
                    lba,
                    asize: asize as u32,
                    lsize: chunk.len() as u32,
                    birth_txg: txg,
                    checksum: *blake3::hash(chunk).as_bytes(),
                });
                lba += asize;
            }
        }
        debug_assert!(rest.is_empty());

        Ok(ptrs)
    }

    /// Write a record's data, zero-padded to whole stripes so the device can
    /// write full stripes directly instead of read-modify-writing parity.
    /// Write `data` as a single contiguous record (at most `max_record_blocks()`).
    pub(crate) async fn write_record(&self, data: &[u8]) -> Result<BlockPtr, FileSystemError> {
        let blocks = data.len().div_ceil(BLOCK_SIZE) as u64;
        assert!(
            blocks > 0 && blocks <= self.max_record_blocks(),
            "record of {} blocks",
            blocks
        );
        let txg = self.open_txg().await?;
        let extent = self.allocate_contiguous(blocks).await?;
        if let Err(e) = self.write_record_blocks(extent.start, data).await {
            self.unallocate(extent.start, extent.len).await;
            return Err(e);
        }
        Ok(BlockPtr {
            lba: extent.start,
            asize: extent.len as u32,
            lsize: data.len() as u32,
            birth_txg: txg,
            checksum: *blake3::hash(data).as_bytes(),
        })
    }

    async fn write_record_blocks(&self, lba: u64, data: &[u8]) -> Result<(), FileSystemError> {
        let blocks = self.align_blocks(data.len().div_ceil(BLOCK_SIZE) as u64);
        let mut buf = vec![0u8; blocks as usize * BLOCK_SIZE];
        buf[..data.len()].copy_from_slice(data);
        self.dev.write_blocks(lba, &buf).await?;
        Ok(())
    }

    /// Read and verify a whole object.
    pub(crate) async fn read_object(&self, ptrs: &[BlockPtr]) -> Result<Vec<u8>, FileSystemError> {
        let mut out = Vec::with_capacity(object_size(ptrs) as usize);
        for ptr in ptrs {
            out.extend_from_slice(&self.read_record(ptr).await?);
        }
        Ok(out)
    }

    /// Read and verify one record. On a checksum mismatch, try rebuilding each
    /// block from parity; if that yields the right checksum, repair the bad
    /// shard on disk and return the good data.
    pub(crate) async fn read_record(&self, ptr: &BlockPtr) -> Result<Vec<u8>, FileSystemError> {
        self.read_record_healing(ptr, !self.read_only).await
    }

    /// `read_record`, choosing whether bad shards found on the way are
    /// rewritten (a scrub repairs, and accounts for, them itself).
    pub(crate) async fn read_record_healing(
        &self,
        ptr: &BlockPtr,
        heal: bool,
    ) -> Result<Vec<u8>, FileSystemError> {
        let n = ptr.data_blocks();
        let mut raw = vec![0u8; n as usize * BLOCK_SIZE];
        self.dev.read_blocks(ptr.lba, &mut raw).await?;
        let blocks: Vec<Vec<u8>> = raw.chunks(BLOCK_SIZE).map(|b| b.to_vec()).collect();

        let lsize = ptr.lsize as usize;
        let assemble = |blocks: &[Vec<u8>]| -> Vec<u8> {
            let mut data: Vec<u8> = blocks.concat();
            data.truncate(lsize);
            data
        };

        let data = assemble(&blocks);
        if *blake3::hash(&data).as_bytes() == ptr.checksum {
            return Ok(data);
        }

        // Rebuild every block several ways (see `Candidates`), then test
        // hypotheses about which disks are bad; the record checksum decides.
        let rebuilds = (0..n).map(|i| async move {
            self.dev
                .reconstruct_candidates(ptr.lba + i)
                .await
                .unwrap_or_default()
        });
        let cands: Vec<crate::disk::Candidates> = futures::future::join_all(rebuilds).await;
        let disks = cands.iter().map(|c| c.rebuilt.len()).max().unwrap_or(0);

        // Hypotheses: every single disk and every pair of disks is bad (two
        // is all the parity can cover). Under a hypothesis, each block on a
        // bad disk is rebuilt without the bad disks; other blocks stay as read.
        let mut hypotheses: Vec<Vec<usize>> = (0..disks).map(|d| vec![d]).collect();
        for a in 0..disks {
            for b in a + 1..disks {
                hypotheses.push(vec![a, b]);
            }
        }
        let mut trials: Vec<Vec<(usize, Vec<u8>)>> = Vec::new();
        'hypothesis: for bad in &hypotheses {
            let mut fix = Vec::new();
            for (i, c) in cands.iter().enumerate() {
                if c.rebuilt.is_empty() || !bad.contains(&c.disk) {
                    continue;
                }
                // Rebuild without the *other* bad disk; if this is the only
                // one, any rebuild will do (they agree when the rest is good).
                let other = bad.iter().find(|&&d| d != c.disk);
                let rebuilt = match other {
                    Some(&d) => c.rebuilt.get(d).cloned().flatten(),
                    None => c.rebuilt.iter().flatten().next().cloned(),
                };
                let Some(rebuilt) = rebuilt else {
                    continue 'hypothesis;
                };
                if rebuilt != blocks[i] {
                    fix.push((i, rebuilt));
                }
            }
            if !fix.is_empty() && !trials.contains(&fix) {
                trials.push(fix);
            }
        }

        for fix in trials {
            let mut trial = blocks.clone();
            for (i, b) in &fix {
                trial[*i] = b.clone();
            }
            let data = assemble(&trial);
            if *blake3::hash(&data).as_bytes() != ptr.checksum {
                continue;
            }
            for (i, b) in &fix {
                let lba = ptr.lba + *i as u64;
                eprintln!(
                    "self-heal: block {} failed its checksum; rebuilt from parity",
                    lba
                );
                let fixed: &[u8; BLOCK_SIZE] = b.as_slice().try_into().unwrap();
                if !heal {
                    continue;
                }
                if let Err(e) = self.dev.repair_block(lba, fixed).await {
                    eprintln!("self-heal: unable to repair block {}: {}", lba, e);
                }
            }
            return Ok(data);
        }

        Err(FileSystemError::ChecksumMismatch)
    }

    /// Free every record of an object (deferred, see metaslab).
    pub(crate) async fn free_object(&self, ptrs: &[BlockPtr]) -> Result<(), FileSystemError> {
        for p in ptrs {
            self.free(p.lba, p.asize as u64).await?;
        }
        Ok(())
    }

    pub(crate) async fn write_value<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<Vec<BlockPtr>, FileSystemError> {
        self.write_object(&bincode::serialize(value)?).await
    }

    pub(crate) async fn read_value<T: for<'a> Deserialize<'a>>(
        &self,
        ptrs: &[BlockPtr],
    ) -> Result<T, FileSystemError> {
        Ok(bincode::deserialize(&self.read_object(ptrs).await?)?)
    }
}

#[cfg(test)]
#[path = "tests/blkptr_tests.rs"]
mod tests;
