use crate::disk::BlockDevice;
use crate::fs::{
    BLOCK_PAYLOAD_SIZE, FileSystem, FileSystemError, SPACEMAP_LOG_BLOCKS_PER_METASLAB,
};
use crate::inode::Extent;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetaslabHeader {
    pub start_block: u64,
    pub block_count: u64,
    pub spacemap_start: u64,
    pub spacemap_blocks: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SpaceMapEntry {
    Invalid, // Zeros now map here
    Alloc { start: u64, len: u64 },
    Free { start: u64, len: u64 },
}

#[derive(Debug)]
pub struct MetaslabState {
    pub header: MetaslabHeader,
    pub header_block: u64,
    pub free_extents: BTreeMap<u64, u64>,
    pub write_cursor: u64, // next space map log block
}

impl<D: BlockDevice> FileSystem<D> {
    pub(crate) fn consume_blocks_from_pool(
        pool: &mut Vec<Extent>,
        blocks_needed: usize,
    ) -> Vec<Extent> {
        let mut allocated = Vec::new();
        let mut remaining = blocks_needed as u64;

        while remaining > 0 && !pool.is_empty() {
            if pool[0].len <= remaining {
                // Take the whole extent
                let extent = pool.remove(0);
                remaining -= extent.len;
                allocated.push(extent);
            } else {
                // Take part of the extent
                allocated.push(Extent {
                    start: pool[0].start,
                    len: remaining,
                });
                pool[0].start += remaining;
                pool[0].len -= remaining;
                remaining = 0;
            }
        }

        allocated
    }

    pub(crate) async fn free(&self, start: u64, len: u64) -> Result<(), FileSystemError> {
        println!("free called: start: {}, len: {}", start, len);

        for (ms_idx, ms) in self.metaslabs.iter().enumerate() {
            let (ms_start, ms_end, log_used) = {
                let ms = ms.read().await;
                let ms_start = ms.header.start_block;
                let ms_end = ms_start + ms.header.block_count;
                let log_used = ms.write_cursor - ms.header.spacemap_start;
                (ms_start, ms_end, log_used)
            };

            if start >= ms_start && start < ms_end {
                // Check if compaction needed
                let log_capacity = SPACEMAP_LOG_BLOCKS_PER_METASLAB as u64;

                if log_used >= log_capacity {
                    self.compact_spacemap_log(ms_idx).await?;
                }

                return self.free_in_metaslab(ms_idx, start, len).await;
            }
        }

        Ok(())
    }

    async fn free_in_metaslab(
        &self,
        ms_idx: usize,
        start: u64,
        len: u64,
    ) -> Result<(), FileSystemError> {
        let mut ms = self.metaslabs[ms_idx].write().await;
        
        println!("  -> Block {} belongs to metaslab {}", start, ms_idx);
        println!("  -> Before merge, free_extents: {:?}", ms.free_extents);

        if let Some(&existing_len) = ms.free_extents.get(&start) {
            if existing_len == len {
                println!(
                    "WARNING: Attempted to free already-free extent {{{}:{}}}",
                    start, len
                );
                return Ok(());
            }
        }

        let mut new_start = start;
        let mut new_len = len;

        // Merge with preceding overlapping/adjacent free blocks
        if let Some((&prev_start, &prev_len)) = ms.free_extents.range(..start).next_back() {
            if prev_start + prev_len >= start {
                new_start = prev_start;
                new_len = (prev_start + prev_len).max(start + len) - new_start;
                ms.free_extents.remove(&prev_start);
            }
        }

        // Merge with following overlapping/adjacent free blocks
        loop {
            if let Some((&next_start, &next_len)) = ms.free_extents.range(new_start..).next() {
                if next_start <= new_start + new_len {
                    new_len = (next_start + next_len).max(new_start + new_len) - new_start;
                    ms.free_extents.remove(&next_start);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        ms.free_extents.insert(new_start, new_len);

        let entry = SpaceMapEntry::Free {
            start: new_start,
            len: new_len,
        };
        let write_cursor = ms.write_cursor;
        ms.write_cursor += 1;
        ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;

        println!("  -> After merge, inserting {{{}:{}}}", new_start, new_len);

        let header_block = ms.header_block;
        let header = ms.header.clone();
        
        drop(ms);

        self.write_block_checked_txg(write_cursor, &entry).await?;
        self.write_block_checked_txg(header_block, &header).await?;

        Ok(())
    }

    pub(crate) async fn allocate_blocks_stripe_aligned(
        &self,
        blocks_needed: u64,
    ) -> Result<Vec<Extent>, FileSystemError> {
        if blocks_needed == 0 {
            return Ok(Vec::new());
        }

        // Round up to stripe boundary
        let alignment = crate::raidz::DATA_SHARDS as u64;
        let aligned_blocks = ((blocks_needed + alignment - 1) / alignment) * alignment;

        println!(
            "allocate_blocks_stripe_aligned: requested {}, allocating {} (aligned to {} block stripes)",
            blocks_needed, aligned_blocks, alignment
        );

        let mut allocated_extents = Vec::new();
        let mut remaining = aligned_blocks;
        let mut ms_idx = 0;

        while ms_idx < self.metaslabs.len() && remaining > 0 {
            let (log_used, log_capacity) = 
                {
                    let ms = self.metaslabs[ms_idx].read().await;
                    let log_capacity = SPACEMAP_LOG_BLOCKS_PER_METASLAB as u64;
                    let log_used = ms.write_cursor - ms.header.spacemap_start;
                    (log_used, log_capacity)
                };

            if log_used >= log_capacity {
                println!(
                        "  MS{}: Skipping - log at capacity ({}/{})",
                        ms_idx, log_used, log_capacity
                    );
                ms_idx += 1;
                continue;
            }

            let mut ms = self.metaslabs[ms_idx].write().await;
            let log_capacity = SPACEMAP_LOG_BLOCKS_PER_METASLAB as u64;
            let mut rejected_extents = Vec::new();
            let mut io_batch = Vec::new();
            
            while remaining > 0 {
                let current_log_used = ms.write_cursor - ms.header.spacemap_start;
                if current_log_used >= log_capacity {
                    println!("  MS{}: Log filled during allocation sequence", ms_idx);
                    break;
                }

                // Find suitable extent (direct access, no lock needed)
                let found = ms.free_extents.iter()
                    .map(|(&s, &l)| (s, l))
                    .find(|(s, _)| !rejected_extents.iter().any(|(rs, _)| rs == s));

                if let Some((start, len)) = found {
                    let ms_end = ms.header.start_block + ms.header.block_count;
                    
                    // Remove from free pool
                    ms.free_extents.remove(&start);

                    // Align start to stripe boundary
                    let aligned_start = ((start + alignment - 1) / alignment) * alignment;
                    let skip = aligned_start.saturating_sub(start);

                    if skip >= len {
                        // Extent too small/misaligned, reject it and try next
                        rejected_extents.push((start, len));
                        continue;
                    }

                    let usable_len = len - skip;
                    let max_in_metaslab = ms_end.saturating_sub(aligned_start);
                    let available = usable_len.min(max_in_metaslab);

                    // Round down to stripe boundary
                    let aligned_available = (available / alignment) * alignment;

                    if aligned_available == 0 {
                        // Extent too small after alignment, reject it
                        rejected_extents.push((start, len));
                        continue;
                    }

                    let take = aligned_available.min(remaining);

                    println!(
                        "  MS{}: Allocating stripe-aligned extent {{{}: {}}} from {{{}: {}}}",
                        ms_idx, aligned_start, take, start, len
                    );

                    // Return skipped blocks before alignment
                    if skip > 0 {
                        ms.free_extents.insert(start, skip);
                        println!("    -> Returned {} pre-alignment blocks to free pool", skip);
                    }

                    // Prepare allocation entry
                    let entry = SpaceMapEntry::Alloc {
                        start: aligned_start,
                        len: take,
                    };
                    let write_cursor = ms.write_cursor;
                    ms.write_cursor += 1;
                    ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;

                    // Return remainder after allocation
                    let remainder = (len - skip) - take;
                    if remainder > 0 {
                        ms.free_extents.insert(aligned_start + take, remainder);
                        println!(
                            "    -> Returned {} post-allocation blocks to free pool",
                            remainder
                        );
                    }

                    // Add to batch for I/O (will execute after lock release)
                    io_batch.push((write_cursor, ms.header_block, ms.header.clone(), entry));
                    
                    allocated_extents.push(Extent {
                        start: aligned_start,
                        len: take,
                    });
                    remaining -= take;
                } else {
                    // No more suitable extents in this metaslab
                    break;
                }
            }

            // Return rejected extents to free pool
            for (start, len) in rejected_extents {
                ms.free_extents.insert(start, len);
            }
            
            // Release write lock before I/O
            drop(ms);

            for (write_cursor, header_block, header, entry) in io_batch {
                self.write_block_checked_txg(write_cursor, &entry).await?;
                self.write_block_checked_txg(header_block, &header).await?;
            }

            if remaining == 0 {
                break;
            }
            
            ms_idx += 1;
        }

        if remaining == 0 {
            Ok(allocated_extents)
        } else {
            // Rollback: return all allocated blocks
            for extent in allocated_extents {
                self.free(extent.start(), extent.len()).await?;
            }
            Err(FileSystemError::UnableToAllocateBlocks(remaining))
        }
    }

    async fn compact_spacemap_log(&self, ms_idx: usize) -> Result<(), FileSystemError> {
        let (free_extents_vec, spacemap_start) = {
            let mut ms = self.metaslabs[ms_idx].write().await;

            println!(
                "  COMPACTING spacemap log for metaslab {} (was at {}/{})",
                ms_idx,
                ms.write_cursor - ms.header.spacemap_start,
                SPACEMAP_LOG_BLOCKS_PER_METASLAB
            );

            let free_extents_vec: Vec<(u64, u64)> = ms.free_extents.iter()
                .map(|(&start, &len)| (start, len))
                .collect();
            
            ms.write_cursor = ms.header.spacemap_start;
            
            (free_extents_vec, ms.header.spacemap_start)
        };

        // Write compacted state - one Free entry per extent
        for (start, len) in &free_extents_vec {
            let (write_cursor, entry) = {
                let mut ms = self.metaslabs[ms_idx].write().await;
                let entry = SpaceMapEntry::Free { 
                    start: *start, 
                    len: *len 
                };
                let write_cursor = ms.write_cursor;
                ms.write_cursor += 1;

                // Safety check
                if ms.write_cursor - ms.header.spacemap_start >= SPACEMAP_LOG_BLOCKS_PER_METASLAB as u64 {
                    panic!(
                        "Metaslab {} still doesn't fit after compaction! Has {} free extents",
                        ms_idx,
                        free_extents_vec.len()
                    );
                }

                (write_cursor, entry)
            };

            self.write_block_checked_txg(write_cursor, &entry).await?;
        }

        let (header_block, header, write_cursor) = {
            let mut ms = self.metaslabs[ms_idx].write().await;
            ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;
            (ms.header_block, ms.header.clone(), ms.write_cursor)
        };

        self.write_block_checked_txg(header_block, &header).await?;

        println!(
            "  Compacted to {}/{} entries",
            write_cursor - spacemap_start,
            SPACEMAP_LOG_BLOCKS_PER_METASLAB
        );

        Ok(())
    }

    /// Calculate how many blocks are needed considering payload overhead
    pub(crate) fn calculate_blocks_needed(data_len: usize) -> u64 {
        ((data_len + BLOCK_PAYLOAD_SIZE - 1) / BLOCK_PAYLOAD_SIZE) as u64
    }

    /// Find all blocks that are allocated but not referenced by any live data
    pub async fn find_orphaned_blocks(&self) -> Result<Vec<(u64, u64)>, FileSystemError> {
        let uber = self
            .uberblock()
            .await
            .ok_or(FileSystemError::NotFormatted)?;

        // Step 1: Build set of all blocks that SHOULD be allocated
        let mut live_blocks = std::collections::HashSet::new();

        // Mark file data blocks AND inode blocks
        for inode_ref in self.file_index.read().await.files.values() {
            // Mark inode storage blocks
            for extent in &inode_ref.extents {
                for i in 0..extent.len() {
                    live_blocks.insert(extent.start() + i);
                }
            }

            // Read inode and mark its data blocks
            let inode = self.read_inode(inode_ref).await?;
            for extent in inode.inode_type.extents() {
                for i in 0..extent.len() {
                    live_blocks.insert(extent.start() + i);
                }
            }
        }

        // Mark file index blocks
        for extent in &uber.file_index_extent {
            for i in 0..extent.len() {
                live_blocks.insert(extent.start() + i);
            }
        }

        // Step 2: Find blocks that are allocated but not live
        let mut orphaned = Vec::new();

        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            let ms_start = ms.header.start_block;
            let ms_end = ms_start + ms.header.block_count;

            // Find allocated ranges by inverting free_extents
            let mut allocated_ranges = Vec::new();
            let mut cursor = ms_start;

            for (&free_start, &free_len) in &ms.free_extents {
                if cursor < free_start {
                    allocated_ranges.push((cursor, free_start - cursor));
                }
                cursor = free_start + free_len;
            }
            if cursor < ms_end {
                allocated_ranges.push((cursor, ms_end - cursor));
            }

            // Check which allocated blocks aren't live
            for (start, len) in allocated_ranges {
                let mut orphan_start = None;
                let mut orphan_len = 0;

                for i in 0..len {
                    let block = start + i;
                    if !live_blocks.contains(&block) {
                        if orphan_start.is_none() {
                            orphan_start = Some(block);
                        }
                        orphan_len += 1;
                    } else {
                        if let Some(os) = orphan_start {
                            orphaned.push((os, orphan_len));
                            orphan_start = None;
                            orphan_len = 0;
                        }
                    }
                }

                if let Some(os) = orphan_start {
                    orphaned.push((os, orphan_len));
                }
            }
        }

        Ok(orphaned)
    }
}

#[cfg(test)]
#[path = "tests/metaslab_tests.rs"]
mod tests;
