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
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};

/// Most blocks fetched by one coalesced read of physically adjacent records.
pub const COALESCE_MAX_BLOCKS: u64 = 256;
/// Coalesced reads in flight at once.
const READ_CONCURRENCY: usize = 8;

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

/// Split `ptrs` into runs of records that are physically adjacent (each
/// starts where the previous one's allocation ends), so each run can be
/// fetched with one read of at most `COALESCE_MAX_BLOCKS` blocks.
pub fn coalesce_groups(ptrs: &[BlockPtr]) -> Vec<std::ops::Range<usize>> {
    let mut groups = Vec::new();
    let mut i = 0;
    while i < ptrs.len() {
        let mut j = i + 1;
        while j < ptrs.len()
            && ptrs[j].lba == ptrs[j - 1].lba + ptrs[j - 1].asize as u64
            && ptrs[j].lba + ptrs[j].data_blocks() - ptrs[i].lba <= COALESCE_MAX_BLOCKS
        {
            j += 1;
        }
        groups.push(i..j);
        i = j;
    }
    groups
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

    /// Write `records` back to back at `start`, each in `record_blocks`
    /// (stripe aligned) blocks, with a single device write. The space must
    /// already be allocated; returns one pointer per record.
    pub(crate) async fn write_records_at(
        &self,
        start: u64,
        record_blocks: u64,
        records: &[&[u8]],
    ) -> Result<Vec<BlockPtr>, FileSystemError> {
        debug_assert_eq!(record_blocks, self.align_blocks(record_blocks));
        let txg = self.open_txg().await?;
        let stride = record_blocks as usize * BLOCK_SIZE;
        let mut buf = vec![0u8; stride * records.len()];
        let mut ptrs = Vec::with_capacity(records.len());
        for (i, data) in records.iter().enumerate() {
            assert!(data.len() <= stride, "record larger than its slot");
            buf[i * stride..i * stride + data.len()].copy_from_slice(data);
            ptrs.push(BlockPtr {
                lba: start + i as u64 * record_blocks,
                asize: record_blocks as u32,
                lsize: data.len() as u32,
                birth_txg: txg,
                checksum: *blake3::hash(data).as_bytes(),
            });
        }
        self.dev.write_blocks(start, &buf).await?;
        Ok(ptrs)
    }

    pub(crate) async fn write_record_blocks(
        &self,
        lba: u64,
        data: &[u8],
    ) -> Result<(), FileSystemError> {
        let blocks = self.align_blocks(data.len().div_ceil(BLOCK_SIZE) as u64);
        let mut buf = vec![0u8; blocks as usize * BLOCK_SIZE];
        buf[..data.len()].copy_from_slice(data);
        self.dev.write_blocks(lba, &buf).await?;
        Ok(())
    }

    /// Read and verify a whole object.
    pub(crate) async fn read_object(&self, ptrs: &[BlockPtr]) -> Result<Vec<u8>, FileSystemError> {
        let mut out = Vec::with_capacity(object_size(ptrs) as usize);
        for data in self.read_records(ptrs).await? {
            out.extend_from_slice(&data);
        }
        Ok(out)
    }

    /// Read and verify several records, in order. Records that sit back to
    /// back on disk are fetched with one device read (up to
    /// `COALESCE_MAX_BLOCKS`); any record of such a read that fails its
    /// checksum is re-read on its own through the self-healing path.
    pub(crate) async fn read_records(
        &self,
        ptrs: &[BlockPtr],
    ) -> Result<Vec<Vec<u8>>, FileSystemError> {
        let groups = coalesce_groups(ptrs);
        let reads = groups.into_iter().map(|g| self.read_group(&ptrs[g]));
        let results: Vec<_> = stream::iter(reads)
            .buffered(READ_CONCURRENCY)
            .collect()
            .await;
        let mut out = Vec::with_capacity(ptrs.len());
        for r in results {
            out.extend(r?);
        }
        Ok(out)
    }

    async fn read_group(&self, ptrs: &[BlockPtr]) -> Result<Vec<Vec<u8>>, FileSystemError> {
        if ptrs.len() == 1 {
            return Ok(vec![self.read_record(&ptrs[0]).await?]);
        }
        let first = ptrs[0].lba;
        let last = &ptrs[ptrs.len() - 1];
        let span = last.lba + last.data_blocks() - first;
        let mut raw = vec![0u8; span as usize * BLOCK_SIZE];
        let read_ok = self.dev.read_blocks(first, &mut raw).await.is_ok();
        let mut out = Vec::with_capacity(ptrs.len());
        for p in ptrs {
            if read_ok {
                let at = (p.lba - first) as usize * BLOCK_SIZE;
                let data = &raw[at..at + p.lsize as usize];
                if *blake3::hash(data).as_bytes() == p.checksum {
                    out.push(data.to_vec());
                    continue;
                }
            }
            out.push(self.read_record(p).await?);
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
        let want = ptr.checksum;
        self.read_verified(
            ptr.lba,
            ptr.data_blocks(),
            ptr.lsize as usize,
            &|data: &[u8]| *blake3::hash(data).as_bytes() == want,
            heal,
        )
        .await
    }

    /// Read `n` blocks at `lba` (the first `lsize` bytes of them) that
    /// `check` accepts, rebuilding bad blocks from parity until it does (see
    /// `read_record_healing`). For records whose checksum isn't in a
    /// pointer, like intent log records (see `crate::zil`).
    pub(crate) async fn read_verified(
        &self,
        lba: u64,
        n: u64,
        lsize: usize,
        check: &(dyn Fn(&[u8]) -> bool + Send + Sync),
        heal: bool,
    ) -> Result<Vec<u8>, FileSystemError> {
        let mut raw = vec![0u8; n as usize * BLOCK_SIZE];
        self.dev.read_blocks(lba, &mut raw).await?;
        let blocks: Vec<Vec<u8>> = raw.chunks(BLOCK_SIZE).map(|b| b.to_vec()).collect();

        let assemble = |blocks: &[Vec<u8>]| -> Vec<u8> {
            let mut data: Vec<u8> = blocks.concat();
            data.truncate(lsize);
            data
        };

        let data = assemble(&blocks);
        if check(&data) {
            return Ok(data);
        }

        // Rebuild every block several ways (see `Candidates`), then test
        // hypotheses about which disks are bad; the record checksum decides.
        let rebuilds = (0..n).map(|i| async move {
            self.dev
                .reconstruct_candidates(lba + i)
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
            if !check(&data) {
                continue;
            }
            for (i, b) in &fix {
                let lba = lba + *i as u64;
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
