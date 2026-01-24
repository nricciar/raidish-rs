use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use crate::disk::{BLOCK_SIZE,FileId,DiskError};
use crate::raidz::{
    RaidZ,
    BLOCK_PAYLOAD_SIZE,
    Extent,
    RaidZError,
    METASLAB_SIZE_BLOCKS,
    METASLAB_TABLE_START,
    SPACEMAP_LOG_BLOCKS_PER_METASLAB
};

#[derive(Debug)]
pub enum FileSystemError {
    DiskError(DiskError),
    RaidZError(RaidZError),
    UnableToAllocateBlocks(u64),
    NotFormatted,
    FileNotFound
}

impl From<DiskError> for FileSystemError {
    fn from(error: DiskError) -> Self {
        FileSystemError::DiskError(error)
    }
}

impl From<RaidZError> for FileSystemError {
    fn from(error: RaidZError) -> Self {
        FileSystemError::RaidZError(error)
    }
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
pub struct InodeRef {
    pub file_id: FileId,
    pub inode_extent: Vec<Extent>,  // Where the inode is stored on disk
}

impl InodeRef {
    pub fn extents(&self) -> &[Extent] {
        &self.inode_extent
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileIndex {
    pub next_file_id: FileId,
    pub files: BTreeMap<String, InodeRef>
}

#[derive(Debug)]
pub struct FileSystem {
    pub dev: RaidZ,
    pub metaslabs: Vec<MetaslabState>,
    pub file_index: FileIndex,
    pub current_file_index_extent: Vec<Extent>,
}

impl FileSystem {
    /// Delete a virtual block device or file
    pub async fn delete(&mut self, name: &str) -> Result<(), FileSystemError> {
        let inode_ref = match self.file_index.files.remove(name) {
            Some(iref) => iref,
            None => {
                println!("'{}' does not exist", name);
                return Ok(());
            }
        };

        // Read the inode to get its extents
        let inode = self.read_inode(&inode_ref).await?;

        // Free file data blocks
        for extent in inode.inode_type.extents() {
            self.free(extent.start, extent.len)?;
        }

        // Free inode blocks
        for extent in &inode_ref.inode_extent {
            self.free(extent.start, extent.len)?;
        }

        self.persist_file_index().await?;

        Ok(())
    }

    /// Find all blocks that are allocated but not referenced by any live data
    pub async fn find_orphaned_blocks(&self) -> Result<Vec<(u64, u64)>, FileSystemError> {
        // Step 1: Build set of all blocks that SHOULD be allocated
        let mut live_blocks = std::collections::HashSet::new();
        
        // Mark file data blocks AND inode blocks
        for inode_ref in self.file_index.files.values() {
            // Mark inode storage blocks
            for extent in &inode_ref.inode_extent {
                for i in 0..extent.len {
                    live_blocks.insert(extent.start + i);
                }
            }
            
            // Read inode and mark its data blocks
            let inode = self.read_inode(inode_ref).await?;
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
        
        Ok(orphaned)
    }
    
    pub async fn format(mut dev: RaidZ, total_blocks: u64) -> Result<Self, FileSystemError> {
        let metaslab_count = (total_blocks / METASLAB_SIZE_BLOCKS) as u32;

        // Calculate metadata layout
        let metaslab_headers_start = METASLAB_TABLE_START;
        let metaslab_headers_end = metaslab_headers_start + metaslab_count as u64;
        
        let spacemap_logs_start = metaslab_headers_end;
        let spacemap_logs_end = spacemap_logs_start + (metaslab_count as u64 * SPACEMAP_LOG_BLOCKS_PER_METASLAB);
        
        let data_blocks_start = spacemap_logs_end;

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
            )?;

            let entry = SpaceMapEntry::Free {
                start: start_block,
                len: METASLAB_SIZE_BLOCKS,
            };

            // Write initial space map entry with checksum
            dev.write_block_checked_txg(header.spacemap_start, &entry)?;

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
            metaslabs,
            file_index: FileIndex {
                next_file_id: 1,
                files: BTreeMap::new(),
            },
            current_file_index_extent: Vec::new()
        };

        fs.dev.format(total_blocks)?;
        fs.persist_file_index().await?;
        Ok(fs)
    }

    pub fn free(&mut self, start: u64, len: u64) -> Result<(), FileSystemError> {
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

                // Write the merged extent to space map log with current TXG
                let entry = SpaceMapEntry::Free { start: new_start, len: new_len };
                self.dev.write_block_checked_txg(ms.write_cursor, &entry)?;
                ms.write_cursor += 1;

                ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;
                self.dev.write_block_checked_txg(ms.header_block, &ms.header)?;

                println!("  -> After merge, inserting {{{}:{}}}", new_start, new_len);
                //println!("  -> Writing to space map log at block {} with TXG {}", 
                //         ms.write_cursor - 1, self.dev.txg_state.txg);

                return Ok(());
            }
        }

        Ok(())
    }

    pub fn allocate_blocks_stripe_aligned(&mut self, blocks_needed: u64) -> Result<Vec<Extent>, FileSystemError> {
        if blocks_needed == 0 { return Ok(Vec::new()); }
        
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
                    self.dev.write_block_checked_txg(ms.write_cursor, &entry)?;
                    ms.write_cursor += 1;
                    ms.header.spacemap_blocks = (ms.write_cursor - ms.header.spacemap_start) as u32;
                    self.dev.write_block_checked_txg(ms.header_block, &ms.header)?;

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
            Ok(allocated_extents) 
        } else { 
            // Rollback: return all allocated blocks
            for extent in allocated_extents {
                self.free(extent.start, extent.len)?;
            }
            Err(FileSystemError::UnableToAllocateBlocks(remaining)) 
        }
    }

    /// Calculate how many blocks are needed considering payload overhead
    pub fn calculate_blocks_needed(data_len: usize) -> u64 {
        ((data_len + BLOCK_PAYLOAD_SIZE - 1) / BLOCK_PAYLOAD_SIZE) as u64
    }

    pub async fn persist_file_index(&mut self) -> Result<(), FileSystemError> {
        println!("persist_file_index called");
        let data = bincode::serialize(&self.file_index).unwrap();
        
        // Account for block header overhead when calculating space needed
        let blocks_needed = Self::calculate_blocks_needed(data.len());

        let allocated_extents = self
            .allocate_blocks_stripe_aligned(blocks_needed)?;

        // Write index data using checksummed blocks
        let mut bytes_left = &data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);
                
                if take > 0 {
                    let chunk = &bytes_left[..take];
                    // Write with current TXG + 1 (the new transaction)
                    self.dev.write_block_checked_txg(extent.start + i, &chunk)?;
                    bytes_left = &bytes_left[take..];
                } else {
                    // Write empty block to maintain extent structure
                    let empty: &[u8] = &[];
                    self.dev.write_block_checked_txg(extent.start + i, &empty)?;
                }
            }
        }

        self.dev.commit_txg(allocated_extents.clone()).await?;

        // Free old index blocks AFTER commit
        let old_extents = std::mem::replace(&mut self.current_file_index_extent, allocated_extents);
        for extent in old_extents {
            self.free(extent.start, extent.len)?;
        }
        Ok(())
    }

    pub async fn load(mut dev: RaidZ) -> Result<Self, FileSystemError> {
        let superblock =
            dev.superblock().ok_or(FileSystemError::NotFormatted)?.clone();
        let uber =
            dev.uberblock().ok_or(FileSystemError::NotFormatted)?.clone();

        // Load metaslabs and replay space maps
        let mut metaslabs = Vec::new();

        for i in 0..superblock.metaslab_count {
            let (_, header) : (u64, MetaslabHeader) = 
                dev.read_block_checked(superblock.metaslab_table_start + i as u64).await?;
            
            let mut free_extents = BTreeMap::new();

            // Replay spacemap log entries
            for b in 0..header.spacemap_blocks {
                // Try to read the space map entry
                let (_entry_txg, entry) = dev.read_block_checked::<SpaceMapEntry>(
                    header.spacemap_start + b as u64
                ).await?;
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
            }

            let write_cursor = header.spacemap_start + header.spacemap_blocks as u64;
            metaslabs.push(MetaslabState {
                header,
                header_block: superblock.metaslab_table_start + i as u64,
                free_extents,
                write_cursor
            });
        }

        // Reconstruct file index from extents with validation
        let mut index_bytes = Vec::new();
        for extent in &uber.file_index_extent {
            for i in 0..extent.len {
                let (block_txg, chunk) = dev.read_block_checked::<Vec<u8>>(extent.start + i).await?;
                // Validate TXG matches or is from earlier transaction
                if block_txg <= dev.txg() {
                    index_bytes.extend_from_slice(&chunk);
                } else {
                    println!("WARNING: File index block has TXG {} > uberblock TXG {}", 
                             block_txg, dev.txg());
                }
            }
        }
        
        let file_index: FileIndex = bincode::deserialize(&index_bytes)
            .expect("Failed to deserialize FileIndex");

        Ok(FileSystem {
            dev,
            metaslabs,
            file_index,
            current_file_index_extent: uber.file_index_extent
        })
    }

    pub async fn read_file(&mut self, name: &str) -> Result<Vec<u8>, FileSystemError> {
        let inode_ref = self.file_index.files.get(name)
            .ok_or_else(|| FileSystemError::FileNotFound)?.clone();
        
        let inode = self.read_inode(&inode_ref).await?;
        
        if !inode.inode_type.is_file() {
            panic!("'{}' is not a regular file", name);
        }

        let mut out = Vec::with_capacity(inode.inode_type.size_bytes() as usize);
        let mut remaining = inode.inode_type.size_bytes();

        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode.inode_type.extents() {
            for i in 0..extent.len {
                let (_, len) = self.dev.read_raw_block_checked_into(extent.start + i, &mut block_buf).await?;
                let take = remaining.min(len as u64) as usize;
                out.extend_from_slice(&block_buf[..take]);
                remaining -= take as u64;
            }
        }

        Ok(out)
    }

    pub async fn write_file(&mut self, name: &str, data: &[u8]) -> Result<FileId, FileSystemError> {
        let old_inode_ref = self.file_index.files.get(name).cloned();

        // If exists, ensure it's a file not a virtual block device
        if let Some(ref inode_ref) = old_inode_ref {
            let old_inode = self.read_inode(inode_ref).await?;
            if !old_inode.inode_type.is_file() {
                panic!("'{}' is a block device, not a file", name);
            }
        }

        // Calculate blocks needed accounting for header overhead
        let blocks_needed = Self::calculate_blocks_needed(data.len());
        
        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed)?;
        println!("allocated extents: {:?}", allocated_extents);

        // Write file data in chunks that fit in BLOCK_PAYLOAD_SIZE
        let mut current_offset = 0;
        for extent in &allocated_extents {
            for i in 0..extent.len {
                if current_offset >= data.len() { 
                    // Write empty block for unused allocated space
                    let empty: &[u8] = &[];
                    self.dev.write_raw_block_checked_txg(extent.start + i, &empty)?;
                    continue; 
                }
                
                let take = (data.len() - current_offset).min(BLOCK_PAYLOAD_SIZE);
                let chunk = &data[current_offset..current_offset + take];
                
                self.dev.write_raw_block_checked_txg(extent.start + i, &chunk)?;
                current_offset += take;
            }
        }

        // Create or update file ID
        let file_id = old_inode_ref
            .as_ref()
            .map(|r| r.file_id)
            .unwrap_or(self.file_index.next_file_id);

        if old_inode_ref.is_none() {
            self.file_index.next_file_id += 1;
        }

        // Create new inode
        let new_inode = FileInode {
            id: file_id,
            inode_type: InodeType::File {
                size_bytes: data.len() as u64,
                extents: allocated_extents,
            },
        };

        // Write inode and get its extent
        let inode_extent = self.write_inode(&new_inode).await?;

        // Update file index with new inode reference
        self.file_index.files.insert(
            name.to_string(), 
            InodeRef {
                file_id,
                inode_extent,
            }
        );

        self.persist_file_index().await?;

        // Free old inode and data blocks
        if let Some(old_ref) = old_inode_ref {
            let old_inode = self.read_inode(&old_ref).await?;
            
            // Free old data blocks
            for extent in old_inode.inode_type.extents() { 
                self.free(extent.start, extent.len)?; 
            }
            
            // Free old inode blocks
            for extent in &old_ref.inode_extent {
                self.free(extent.start, extent.len)?;
            }
        }

        Ok(file_id)
    }


    pub async fn read_inode(&self, inode_ref: &InodeRef) -> Result<FileInode, FileSystemError> {
        let mut inode_bytes = Vec::new();
        
        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode_ref.extents() {
            for i in 0..extent.len {
                let (_, _chunk) = self.dev.read_raw_block_checked_into(extent.start + i, &mut block_buf).await?;
                inode_bytes.extend_from_slice(&block_buf);
            }
        }
        
        let inode: FileInode = bincode::deserialize(&inode_bytes)
            .expect("Failed to deserialize inode");
        
        Ok(inode)
    }

    pub async fn write_inode(&mut self, inode: &FileInode) -> Result<Vec<Extent>, FileSystemError> {
        let inode_data = bincode::serialize(inode).unwrap();
        let blocks_needed = Self::calculate_blocks_needed(inode_data.len());
        
        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed)?;
        
        let mut bytes_left = &inode_data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);
                
                if take > 0 {
                    let chunk = &bytes_left[..take];
                    self.dev.write_raw_block_checked_txg(extent.start + i, &chunk)?;
                    bytes_left = &bytes_left[take..];
                } else {
                    let empty: &[u8] = &[];
                    self.dev.write_raw_block_checked_txg(extent.start + i, &empty)?;
                }
            }
        }
        
        Ok(allocated_extents)
    }

}