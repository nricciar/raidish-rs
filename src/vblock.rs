use crate::disk::{BLOCK_SIZE,FileId};
use crate::raidz::{
    BLOCK_PAYLOAD_SIZE,
    Extent,
};
use crate::fs::{FileInode,FileSystem,FileSystemError,InodeType};

impl FileSystem {
    pub async fn create_block(&mut self, name: &str, size_bytes: u64, block_size: u32) -> Result<FileId, FileSystemError> {
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
                self.dev.write_raw_block_checked_txg(extent.start + i, &zeros)?;
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

        self.persist_file_index().await?;

        Ok(file_id)
    }

    /// Read data from a virtual block device at a specific byte offset
    pub async fn block_read(&mut self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
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
            return Ok(0);
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

            let (_txg, payload_len) = self.dev.read_raw_block_checked_into(
                block_lba, 
                &mut block_buf
            ).await?;

            let payload = &block_buf[..payload_len];
            let available_in_block = payload.len().saturating_sub(byte_offset);
            let to_copy = available_in_block.min(max_read - bytes_read);

            buf[bytes_read..bytes_read + to_copy]
                .copy_from_slice(&payload[byte_offset..byte_offset + to_copy]);

            bytes_read += to_copy;
            current_offset += to_copy as u64;
        }

        println!("  -> block_read returning {}", bytes_read);

        Ok(bytes_read)
    }

    /// Write data to a virtual block device at a specific byte offset
    pub async fn block_write(&mut self, name: &str, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
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
            return Ok(0);
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
                )?;
            } else {
                let (_txg, payload_len) = self.dev.read_raw_block_checked_into(
                    block_lba, 
                    &mut block_buf
                ).await?;
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
                )?;
            }

            bytes_written += to_write;
            current_offset += to_write as u64;
        }

        Ok(bytes_written)
    }
}

/// Map a byte offset within a block device to a specific filesystem block and byte offset
/// Returns None if the block is not yet allocated (sparse)
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
    
    // Offset is beyond allocated extents but within device size - sparse block
    None
}