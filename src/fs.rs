use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use crate::disk::{BLOCK_SIZE,FileId};
use crate::raidz::{RaidZ,BLOCK_PAYLOAD_SIZE,UBERBLOCK_START,UBERBLOCK_COUNT,Uberblock,Extent,UBERBLOCK_MAGIC};

// TODO
// Superblock, Uberblock and Metaslab alignment to stripe needs to be considered
pub const FS_ID: &str = "RAIDISHV10";
pub const METASLAB_SIZE_BLOCKS: u64 = 1026; // 1026 is stripe alined
//pub const MAX_METASLABS: u32 = 128;
pub const SUPERBLOCK_LBA: u64 = 0;
pub const METASLAB_TABLE_START: u64 = UBERBLOCK_START + UBERBLOCK_COUNT + 2; // +2 is to keep stripe alignment
pub const SPACEMAP_LOG_BLOCKS_PER_METASLAB: u64 = 16;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Superblock {
    pub magic: String,
    pub block_size: u32,
    pub total_blocks: u64,
    pub metaslab_count: u32,
    pub metaslab_size_blocks: u64,
    pub metaslab_table_start: u64,
}

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
    Free  { start: u64, len: u64 },
}

#[derive(Debug)]
pub struct MetaslabState {
    pub header: MetaslabHeader,
    pub header_block: u64,
    pub free_extents: BTreeMap<u64, u64>,
    pub write_cursor: u64, // next space map log block
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InodeType {
    File {
        size_bytes: u64,
        extents: Vec<Extent>,
    },
    Block {
        size_bytes: u64,
        block_size: u32,  // block size (e.g., 4KB, 8KB, 64KB)
        extents: Vec<Extent>,
    },
}

impl InodeType {
    pub fn size_bytes(&self) -> u64 {
        match self {
            InodeType::File { size_bytes, .. } => *size_bytes,
            InodeType::Block { size_bytes, .. } => *size_bytes,
        }
    }

    pub fn extents(&self) -> &[Extent] {
        match self {
            InodeType::File { extents, .. } => extents,
            InodeType::Block { extents, .. } => extents,
        }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, InodeType::Block { .. })
    }

    pub fn is_file(&self) -> bool {
        matches!(self, InodeType::File { .. })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInode {
    pub id: FileId,
    pub inode_type: InodeType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileIndex {
    pub next_file_id: FileId,
    pub files: BTreeMap<String, FileId>,
    pub inodes: BTreeMap<FileId, FileInode>,
}

#[derive(Debug)]
pub struct FileSystem {
    pub dev: RaidZ,
    pub superblock: Superblock,
    pub metaslabs: Vec<MetaslabState>,
    pub file_index: FileIndex,
    pub current_file_index_extent: Vec<Extent>,
}

impl FileSystem {
    pub async fn create_block(&mut self, name: &str, size_bytes: u64, block_size: u32) -> FileId {
        // Check if name already exists
        if self.file_index.files.contains_key(name) {
            panic!("File or block device '{}' already exists", name);
        }

        let blocks_needed = Self::calculate_blocks_needed(size_bytes as usize);
        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed)
            .expect("out of space for virtual block device");

        println!("Created virtual block device '{}' with {} blocks: {:?}", name, blocks_needed, allocated_extents);

        // Zero out the blocks
        for extent in &allocated_extents {
            for i in 0..extent.len {
                //let empty: &[u8] = &[];
                let zeros = vec![0u8; BLOCK_PAYLOAD_SIZE];
                self.dev.write_raw_block_checked_txg(extent.start + i, &zeros);
            }
        }

        let file_id = self.file_index.next_file_id;
        self.file_index.next_file_id += 1;

        let inode = FileInode {
            id: file_id,
            inode_type: InodeType::Block {
                size_bytes,
                block_size,
                extents: allocated_extents,
            },
        };

        self.file_index.files.insert(name.to_string(), file_id);
        self.file_index.inodes.insert(file_id, inode);

        self.persist_file_index().await;

        file_id
    }

    /// Read data from a virtual block device at a specific byte offset
    pub async fn block_read(&mut self, name: &str, offset: u64, buf: &mut [u8]) -> usize {
        println!("block_read called: name={}, offset={}, buf.len={}", name, offset, buf.len());

        let file_id = *self.file_index.files.get(name)
            .expect(&format!("'{}' does not exist", name));
        
        let inode = self.file_index.inodes.get(&file_id)
            .expect("Inode missing");

        println!("  -> Inode type: {:?}", inode.inode_type.is_block());

        // Ensure this is actually a zvol
        if !inode.inode_type.is_block() {
            panic!("'{}' is not a block device", name);
        }

        let size_bytes = inode.inode_type.size_bytes();
        let extents = inode.inode_type.extents();

        println!("  -> size_bytes={}, extents.len={}", size_bytes, extents.len());

        if offset >= size_bytes {
            return 0;
        }

        let max_read = ((size_bytes - offset) as usize).min(buf.len());
        let mut bytes_read = 0;
        let mut current_offset = offset;

        println!("  -> Starting read loop, max_read={}", max_read);

        let mut block_buf = [0u8; BLOCK_SIZE];
        while bytes_read < max_read {
            println!("    -> Loop iteration: bytes_read={}, current_offset={}", bytes_read, current_offset);
            let (block_lba, byte_offset) = match map_offset_to_extent(
                extents,
                current_offset,
                size_bytes,
            ) {
                Some(mapping) => {
                    println!("    -> Mapped to block_lba={}, byte_offset={}", mapping.0, mapping.1);
                    mapping
                },
                None => {
                    println!("    -> map_offset_to_extent returned None, breaking");
                    break
                },
            };

            println!("    -> About to call read_raw_block_checked({})", block_lba);

            let (_txg, payload_len) = match self.dev.read_raw_block_checked_into(
                block_lba, 
                &mut block_buf
            ).await {
                Ok(result) => result,
                Err(_) => {
                    println!("Warning: Failed to read block at {}", block_lba);
                    break;
                }
            };

            let payload = &block_buf[..payload_len];
            let available_in_block = payload.len().saturating_sub(byte_offset);
            let to_copy = available_in_block.min(max_read - bytes_read);

            buf[bytes_read..bytes_read + to_copy]
                .copy_from_slice(&payload[byte_offset..byte_offset + to_copy]);

            bytes_read += to_copy;
            current_offset += to_copy as u64;
        }

        println!("  -> block_read returning {}", bytes_read);

        bytes_read
    }

    /// Write data to a virtual block device at a specific byte offset
    pub async fn block_write(&mut self, name: &str, offset: u64, data: &[u8]) -> usize {
        let file_id = *self.file_index.files.get(name)
            .expect(&format!("'{}' does not exist", name));
        
        let inode = self.file_index.inodes.get(&file_id)
            .expect("Inode missing");

        if !inode.inode_type.is_block() {
            panic!("'{}' is not a block device", name);
        }

        let size_bytes = inode.inode_type.size_bytes();
        let extents = inode.inode_type.extents().to_vec();

        if offset >= size_bytes {
            return 0;
        }

        let max_write = ((size_bytes - offset) as usize).min(data.len());
        let mut bytes_written = 0;
        let mut current_offset = offset;

        let mut block_buf = [0u8; BLOCK_SIZE];
        while bytes_written < max_write {
            let (block_lba, byte_offset) = match map_offset_to_extent(
                &extents,
                current_offset,
                size_bytes,
            ) {
                Some(mapping) => mapping,
                None => break,
            };

            let to_write = (BLOCK_PAYLOAD_SIZE - byte_offset).min(max_write - bytes_written);

            if byte_offset == 0 && to_write == BLOCK_PAYLOAD_SIZE {
                // Full block write
                self.dev.write_raw_block_checked_txg(
                    block_lba,
                    &data[bytes_written..bytes_written + to_write],
                );
            } else {
                let (_txg, payload_len) = match self.dev.read_raw_block_checked_into(
                    block_lba, 
                    &mut block_buf
                ).await {
                    Ok(result) => result,
                    Err(_) => {
                        println!("Warning: Failed to read block at {}", block_lba);
                        break;
                    }
                };
                // Ensure we have enough space for the write
                let needed_len = byte_offset + to_write;
                
                if needed_len > payload_len {
                    // Zero-extend if writing beyond current payload
                    block_buf[payload_len..needed_len].fill(0);
                }

                // Copy the data to write into the appropriate offset
                block_buf[byte_offset..byte_offset + to_write]
                    .copy_from_slice(&data[bytes_written..bytes_written + to_write]);

                // Write back the modified payload (only up to what we need)
                let final_len = needed_len.max(payload_len);
                self.dev.write_raw_block_checked_txg(
                    block_lba, 
                    &block_buf[..final_len]
                );
            }

            bytes_written += to_write;
            current_offset += to_write as u64;
        }

        bytes_written
    }

    /// Delete a virtual block device or file
    pub async fn delete(&mut self, name: &str) {
        let file_id = match self.file_index.files.remove(name) {
            Some(id) => id,
            None => {
                println!("'{}' does not exist", name);
                return;
            }
        };

        let inode = self.file_index.inodes.remove(&file_id)
            .expect("Inode missing");

        // Free all blocks
        for extent in inode.inode_type.extents() {
            self.free(extent.start, extent.len);
        }

        self.persist_file_index().await;
    }

    /// Find all blocks that are allocated but not referenced by any live data
    pub fn find_orphaned_blocks(&self) -> Vec<(u64, u64)> {
        // Step 1: Build set of all blocks that SHOULD be allocated
        let mut live_blocks = std::collections::HashSet::new();
        
        // Mark file data blocks
        for inode in self.file_index.inodes.values() {
            for extent in inode.inode_type.extents() {
                for i in 0..extent.len {
                    live_blocks.insert(extent.start + i);
                }
            }
        }
        
        // Mark file index blocks
        for extent in &self.current_file_index_extent {
            for i in 0..extent.len {
                live_blocks.insert(extent.start + i);
            }
        }
        
        // Step 2: Find blocks that are allocated but not live
        let mut orphaned = Vec::new();
        
        for ms in &self.metaslabs {
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
        
        orphaned
    }
    
    pub async fn format(mut dev: RaidZ, total_blocks: u64) -> Self {
        let metaslab_count = (total_blocks / METASLAB_SIZE_BLOCKS) as u32;

        // Calculate metadata layout
        let metaslab_headers_start = METASLAB_TABLE_START;
        let metaslab_headers_end = metaslab_headers_start + metaslab_count as u64;
        
        let spacemap_logs_start = metaslab_headers_end;
        let spacemap_logs_end = spacemap_logs_start + (metaslab_count as u64 * SPACEMAP_LOG_BLOCKS_PER_METASLAB);
        
        let data_blocks_start = spacemap_logs_end;

        let superblock = Superblock {
            magic: FS_ID.to_string(),
            block_size: BLOCK_SIZE as u32,
            total_blocks,
            metaslab_count,
            metaslab_size_blocks: METASLAB_SIZE_BLOCKS,
            metaslab_table_start: METASLAB_TABLE_START,
        };
        
        // TXG 0 is reserved for initial format
        dev.write_block_checked_txg(SUPERBLOCK_LBA, &superblock);

        let mut metaslabs = Vec::new();
        for i in 0..metaslab_count {
            let start_block = data_blocks_start + (i as u64 * METASLAB_SIZE_BLOCKS);

            let header = MetaslabHeader {
                start_block,
                block_count: METASLAB_SIZE_BLOCKS,
                spacemap_start: spacemap_logs_start + (i as u64 * SPACEMAP_LOG_BLOCKS_PER_METASLAB),
                spacemap_blocks: 1,
            };

            // Write metaslab header with checksum
            dev.write_block_checked_txg(
                METASLAB_TABLE_START + i as u64,
                &header,
            );

            let entry = SpaceMapEntry::Free {
                start: start_block,
                len: METASLAB_SIZE_BLOCKS,
            };

            // Write initial space map entry with checksum
            dev.write_block_checked_txg(header.spacemap_start, &entry);

            let mut free_extents = BTreeMap::new();
            free_extents.insert(start_block, METASLAB_SIZE_BLOCKS);

            let write_cursor = header.spacemap_start + 1;
            metaslabs.push(MetaslabState {
                header,
                header_block: METASLAB_TABLE_START + i as u64,
                free_extents,
                write_cursor
            });
        }

        let mut fs = FileSystem {
            dev,
            superblock,
            metaslabs,
            file_index: FileIndex {
                next_file_id: 1,
                files: BTreeMap::new(),
                inodes: BTreeMap::new(),
            },
            current_file_index_extent: Vec::new()
        };

        fs.dev.format();
        fs.persist_file_index().await;
        fs
    }

    pub fn free(&mut self, start: u64, len: u64) {
        println!("free called: start: {}, len: {}", start, len);
        
        for (ms_idx, ms) in self.metaslabs.iter_mut().enumerate() {
            let ms_start = ms.header.start_block;
            let ms_end = ms_start + ms.header.block_count;

            println!("  Checking metaslab {} range [{}, {})", ms_idx, ms_start, ms_end);

            if start >= ms_start && start < ms_end {
                println!("  -> Block {} belongs to metaslab {}", start, ms_idx);
                println!("  -> Before merge, free_extents: {:?}", ms.free_extents);

                // Check if this exact extent is already free
                if let Some(&existing_len) = ms.free_extents.get(&start) {
                    if existing_len == len {
                        println!("WARNING: Attempted to free already-free extent {{{}:{}}}", start, len);
                        return;
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

                // Write the merged extent to space map log with current TXG
                let entry = SpaceMapEntry::Free { start: new_start, len: new_len };
                self.dev.write_block_checked_txg(ms.write_cursor, &entry);
                ms.write_cursor += 1;

                ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;
                self.dev.write_block_checked_txg(ms.header_block, &ms.header);

                println!("  -> After merge, inserting {{{}:{}}}", new_start, new_len);
                //println!("  -> Writing to space map log at block {} with TXG {}", 
                //         ms.write_cursor - 1, self.dev.txg_state.txg);

                return;
            }
        }
    }

    pub fn allocate_blocks_stripe_aligned(&mut self, blocks_needed: u64) -> Option<Vec<Extent>> {
        if blocks_needed == 0 { return Some(Vec::new()); }
        
        // Round up to stripe boundary
        let alignment = crate::raidz::DATA_SHARDS as u64;
        let aligned_blocks = ((blocks_needed + alignment - 1) / alignment) * alignment;
        
        println!("allocate_blocks_stripe_aligned: requested {}, allocating {} (aligned to {} block stripes)", 
                 blocks_needed, aligned_blocks, alignment);
        
        let mut allocated_extents = Vec::new();
        let mut remaining = aligned_blocks;

        for (ms_idx, ms) in self.metaslabs.iter_mut().enumerate() {
            let ms_start = ms.header.start_block;
            let ms_end = ms_start + ms.header.block_count;
            
            // Collect extents we've rejected to avoid infinite loop
            let mut rejected_extents = Vec::new();
            
            while remaining > 0 {
                // Skip extents we've already rejected
                let found = ms.free_extents.iter()
                    .map(|(&s, &l)| (s, l))
                    .find(|(s, _)| !rejected_extents.iter().any(|(rs, _)| rs == s));
                
                if let Some((start, len)) = found {
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
                    
                    println!("  MS{}: Allocating stripe-aligned extent {{{}: {}}} from {{{}: {}}}", 
                             ms_idx, aligned_start, take, start, len);

                    // Return skipped blocks before alignment
                    if skip > 0 {
                        ms.free_extents.insert(start, skip);
                        println!("    -> Returned {} pre-alignment blocks to free pool", skip);
                    }
                    
                    // Persist allocation
                    let entry = SpaceMapEntry::Alloc { start: aligned_start, len: take };
                    self.dev.write_block_checked_txg(ms.write_cursor, &entry);
                    ms.write_cursor += 1;
                    ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;
                    self.dev.write_block_checked_txg(ms.header_block, &ms.header);

                    // Return remainder after allocation
                    let remainder = aligned_available - take;
                    if remainder > 0 {
                        ms.free_extents.insert(aligned_start + take, remainder);
                        println!("    -> Returned {} post-allocation blocks to free pool", remainder);
                    }

                    allocated_extents.push(Extent { start: aligned_start, len: take });
                    remaining -= take;
                } else {
                    // No more suitable extents in this metaslab
                    // Return rejected extents back to free pool
                    for (start, len) in rejected_extents.drain(..) {
                        ms.free_extents.insert(start, len);
                    }
                    break;
                }
            }
            
            // Return any remaining rejected extents
            for (start, len) in rejected_extents.drain(..) {
                if ms.free_extents.get(&start).is_none() {
                    ms.free_extents.insert(start, len);
                }
            }
            
            if remaining == 0 { break; }
        }

        if remaining == 0 { 
            Some(allocated_extents) 
        } else { 
            // Rollback: return all allocated blocks
            for extent in allocated_extents {
                self.free(extent.start, extent.len);
            }
            None 
        }
    }

    /// Calculate how many blocks are needed considering payload overhead
    fn calculate_blocks_needed(data_len: usize) -> u64 {
        ((data_len + BLOCK_PAYLOAD_SIZE - 1) / BLOCK_PAYLOAD_SIZE) as u64
    }

    async fn persist_file_index(&mut self) {
        println!("persist_file_index called");
        let data = bincode::serialize(&self.file_index).unwrap();
        
        // Account for block header overhead when calculating space needed
        let blocks_needed = Self::calculate_blocks_needed(data.len());

        let allocated_extents = self
            .allocate_blocks_stripe_aligned(blocks_needed)
            .expect("out of space for file index");

        // Write index data using checksummed blocks
        let mut bytes_left = &data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);
                
                if take > 0 {
                    let chunk = &bytes_left[..take];
                    // Write with current TXG + 1 (the new transaction)
                    self.dev.write_block_checked_txg(extent.start + i, &chunk);
                    bytes_left = &bytes_left[take..];
                } else {
                    // Write empty block to maintain extent structure
                    let empty: &[u8] = &[];
                    self.dev.write_block_checked_txg(extent.start + i, &empty);
                }
            }
        }

        self.dev.commit_txg(allocated_extents.clone()).await;

        // Free old index blocks AFTER commit
        let old_extents = std::mem::replace(&mut self.current_file_index_extent, allocated_extents);
        for extent in old_extents {
            self.free(extent.start, extent.len);
        }
    }

    pub async fn load(mut dev: RaidZ) -> Self {
        // Load superblock (TXG 0 from format)
        let (_, superblock): (_, Superblock) =
            dev.read_block_checked(SUPERBLOCK_LBA).await.expect("invalid superblock");
        assert_eq!(superblock.magic, FS_ID);

        // Load metaslabs and replay space maps
        let mut metaslabs = Vec::new();
        for i in 0..superblock.metaslab_count {
            let header: MetaslabHeader = dev.read_block_checked(superblock.metaslab_table_start + i as u64).await
                .map(|(_, h)| h)
                .expect("failed to read metaslab header");
            
            let mut free_extents = BTreeMap::new();

            // Replay spacemap log entries
            for b in 0..header.spacemap_blocks {
                // Try to read the space map entry
                if let Some((_entry_txg, entry)) = dev.read_block_checked::<SpaceMapEntry>(
                    header.spacemap_start + b as u64
                ).await {
                    // Process valid entries only
                    match entry {
                        SpaceMapEntry::Alloc { start, len } => {
                            if len == 0 { continue; }
                            
                            // Remove allocated chunk from free map (slicing logic)
                            let mut to_add = Vec::new();
                            let mut to_rem = Vec::new();
                            for (&fs, &fl) in free_extents.iter() {
                                let fe = fs + fl;
                                let ae = start + len;
                                if ae <= fs || start >= fe { continue; }
                                to_rem.push(fs);
                                if fs < start { to_add.push((fs, start - fs)); }
                                if ae < fe { to_add.push((ae, fe - ae)); }
                            }
                            for r in to_rem { free_extents.remove(&r); }
                            for (s, l) in to_add { free_extents.insert(s, l); }
                            
                            //println!("LOAD: Replayed Alloc {{{}:{}}}, TXG {}", start, len, entry_txg);
                        }
                        SpaceMapEntry::Free { start, len } => {
                            if len == 0 { continue; }
                            
                            // Apply merge logic consistent with free()
                            let mut new_start = start;
                            let mut new_len = len;

                            if let Some((&prev_start, &prev_len)) = free_extents.range(..start).next_back() {
                                if prev_start + prev_len >= start {
                                    new_start = prev_start;
                                    new_len = (prev_start + prev_len).max(start + len) - new_start;
                                    free_extents.remove(&prev_start);
                                }
                            }

                            loop {
                                if let Some((&next_start, &next_len)) = free_extents.range(new_start..).next() {
                                    if next_start <= new_start + new_len {
                                        new_len = (next_start + next_len).max(new_start + new_len) - new_start;
                                        free_extents.remove(&next_start);
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }

                            free_extents.insert(new_start, new_len);
                            //println!("LOAD: Replayed Free {{{}:{}}}, TXG {}", new_start, new_len, entry_txg);
                        }
                        SpaceMapEntry::Invalid => break, 
                    }
                } else {
                    // Checksum failure or invalid block - stop replaying this log
                    println!("LOAD: Skipping corrupt space map entry at block {}", 
                             header.spacemap_start + b as u64);
                    break;
                }
            }

            let write_cursor = header.spacemap_start + header.spacemap_blocks as u64;
            metaslabs.push(MetaslabState {
                header,
                header_block: superblock.metaslab_table_start + i as u64,
                free_extents,
                write_cursor
            });
        }

        // Find most recent valid uberblock
        let mut best_uber: Option<(u64, u64, Uberblock)> = None;
        for slot in 0..UBERBLOCK_COUNT {
            let lba = UBERBLOCK_START + slot;
            
            if let Some((txg, uber)) = dev.read_block_checked::<Uberblock>(lba).await {
                if uber.magic == UBERBLOCK_MAGIC && uber.version == 1 {
                    match &best_uber {
                        Some((best_txg, _, _)) if *best_txg >= txg => {}
                        _ => {
                            println!("LOAD: Found valid uberblock at slot {} with TXG {}", slot, txg);
                            best_uber = Some((txg, slot, uber));
                        }
                    }
                }
            }
        }

        let (current_txg, _current_uber_slot, uber) =
            best_uber.expect("No valid uberblock found. Disk might be corrupted.");

        // Reconstruct file index from extents with validation
        let mut index_bytes = Vec::new();
        for extent in &uber.file_index_extent {
            for i in 0..extent.len {
                if let Some((block_txg, chunk)) = dev.read_block_checked::<Vec<u8>>(extent.start + i).await {
                    // Validate TXG matches or is from earlier transaction
                    if block_txg <= current_txg {
                        index_bytes.extend_from_slice(&chunk);
                    } else {
                        println!("WARNING: File index block has TXG {} > uberblock TXG {}", 
                                 block_txg, current_txg);
                    }
                } else {
                    panic!("Failed to read file index block at {} - corruption detected", extent.start + i);
                }
            }
        }
        
        let file_index: FileIndex = bincode::deserialize(&index_bytes)
            .expect("Failed to deserialize FileIndex");

        FileSystem {
            dev,
            superblock,
            metaslabs,
            file_index,
            current_file_index_extent: uber.file_index_extent
        }
    }

    pub async fn read_file(&mut self, name: &str) -> Vec<u8> {
        let file_id = self.file_index.files[name];
        let inode = &self.file_index.inodes[&file_id];

        if !inode.inode_type.is_file() {
            panic!("'{}' is not a regular file", name);
        }

        let mut out = Vec::with_capacity(inode.inode_type.size_bytes() as usize);
        let mut remaining = inode.inode_type.size_bytes();

        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode.inode_type.extents() {
            for i in 0..extent.len {
                if let Ok((_, len)) = self.dev.read_raw_block_checked_into(extent.start + i, &mut block_buf).await {
                    let take = remaining.min(len as u64) as usize;
                    out.extend_from_slice(&block_buf[..take]);
                    remaining -= take as u64;
                } else {
                    panic!("Checksum failure reading file '{}' at block {}", name, extent.start + i);
                }
            }
        }

        out
    }

    pub async fn write_file(&mut self, name: &str, data: &[u8]) -> FileId {
        let old_inode = self.file_index.files.get(name)
            .and_then(|fid| self.file_index.inodes.get(fid)).cloned();

        // If exists, ensure it's a file not a virtual block device
        if let Some(ref inode) = old_inode {
            if !inode.inode_type.is_file() {
                panic!("'{}' is a block device, not a file", name);
            }
        }

        // Calculate blocks needed accounting for header overhead
        let blocks_needed = Self::calculate_blocks_needed(data.len());
        
        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed).expect("out of space");
        println!("allocated extents: {:?}", allocated_extents);

        // Write file data in chunks that fit in BLOCK_PAYLOAD_SIZE
        let mut current_offset = 0;
        for extent in &allocated_extents {
            for i in 0..extent.len {
                if current_offset >= data.len() { 
                    // Write empty block for unused allocated space
                    let empty: &[u8] = &[];
                    self.dev.write_raw_block_checked_txg(extent.start + i, &empty);
                    continue; 
                }
                
                let take = (data.len() - current_offset).min(BLOCK_PAYLOAD_SIZE);
                let chunk = &data[current_offset..current_offset + take];
                
                self.dev.write_raw_block_checked_txg(extent.start + i, &chunk);
                current_offset += take;
            }
        }

        let file_id = *self.file_index.files.get(name).unwrap_or(&self.file_index.next_file_id);
        if !self.file_index.files.contains_key(name) {
            self.file_index.next_file_id += 1;
            self.file_index.files.insert(name.to_string(), file_id);
        }

        self.file_index.inodes.insert(file_id, FileInode {
            id: file_id,
            inode_type: InodeType::File {
                size_bytes: data.len() as u64,
                extents: allocated_extents,
            },
        });

        self.persist_file_index().await;

        if let Some(old) = old_inode {
            for extent in old.inode_type.extents() { 
                self.free(extent.start, extent.len); 
            }
        }
        
        file_id
    }
}

/// Map a byte offset within a zvol/file to a specific filesystem block and byte offset
fn map_offset_to_extent(
    extents: &[Extent],
    offset: u64,
    size_bytes: u64,
) -> Option<(u64, usize)> {
    if offset >= size_bytes {
        return None;
    }

    let mut bytes_traversed = 0u64;
    
    for extent in extents {
        let extent_bytes = extent.len * BLOCK_PAYLOAD_SIZE as u64;
        
        if offset < bytes_traversed + extent_bytes {
            let offset_in_extent = offset - bytes_traversed;
            let block_offset = offset_in_extent / BLOCK_PAYLOAD_SIZE as u64;
            let byte_offset = (offset_in_extent % BLOCK_PAYLOAD_SIZE as u64) as usize;
            
            return Some((extent.start + block_offset, byte_offset));
        }
        
        bytes_traversed += extent_bytes;
    }
    
    None
}