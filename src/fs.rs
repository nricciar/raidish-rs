use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, DiskError, FileId};
use crate::inode::{Extent, FileIndex, FileInode, InodeRef, InodeType, Permissions};
use crate::metaslab::{MetaslabHeader, MetaslabState, SpaceMapEntry};
use crate::raidz::{DATA_SHARDS, RaidZError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

pub const FS_ID: &str = "RAIDISHV10";
pub const BLOCK_HEADER_SIZE: usize = std::mem::size_of::<BlockHeader>();
pub const BLOCK_PAYLOAD_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;
pub const SUPERBLOCK_LBA: u64 = 0;
pub const UBERBLOCK_START: u64 = 3;
pub const UBERBLOCK_BLOCK_COUNT: u64 = 15;
pub const UBERBLOCK_STRIPE_COUNT: u64 = UBERBLOCK_BLOCK_COUNT / 3; // needs to be a multiple of DATA_SHARDS
pub const UBERBLOCK_MAGIC: u64 = 0x5241494449534855; // "RAIDISHU"
pub const METASLAB_SIZE_BLOCKS: u64 = 1026; // 1026 is stripe alined
pub const METASLAB_TABLE_START: u64 = UBERBLOCK_START + UBERBLOCK_BLOCK_COUNT + 1; // TODO: + 1 to keep it stripe aligned
pub const SPACEMAP_LOG_BLOCKS_PER_METASLAB: u64 = 16;

#[derive(Debug)]
pub enum FileSystemError {
    SerializationError(bincode::Error),
    DiskError(DiskError),
    RaidZError(RaidZError),
    UnableToAllocateBlocks(u64),
    NotFormatted,
    FileNotFound,
    ChecksumMismatch,
    InvalidMagic,
    DirectoryNotFound,
    NotADirectory,
    NotAFile,
}

impl fmt::Display for FileSystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSystemError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            FileSystemError::DiskError(err) => write!(f, "Disk error: {}", err),
            FileSystemError::RaidZError(err) => write!(f, "RaidZ error: {}", err),
            FileSystemError::UnableToAllocateBlocks(count) => {
                write!(f, "Unable to allocate {} blocks", count)
            }
            FileSystemError::NotFormatted => write!(f, "Not formatted"),
            FileSystemError::FileNotFound => write!(f, "File not found"),
            FileSystemError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            FileSystemError::InvalidMagic => write!(f, "Invalid magic"),
            FileSystemError::DirectoryNotFound => write!(f, "Directory not found"),
            FileSystemError::NotADirectory => write!(f, "Not a directory"),
            FileSystemError::NotAFile => write!(f, "Not a file"),
        }
    }
}

impl From<bincode::Error> for FileSystemError {
    fn from(error: bincode::Error) -> Self {
        FileSystemError::SerializationError(error)
    }
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
    pub txg: u64,       // transaction group
    pub timestamp: u64, // optional but useful
    pub file_index_extent: Vec<Extent>,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub magic: u32,
    pub txg: u64,
    pub checksum: [u8; 32],
    pub payload_len: u32,
}

pub struct FileSystem<D: BlockDevice> {
    pub dev: D,
    superblock: Option<Superblock>,
    uberblock: Option<Uberblock>,
    pub metaslabs: Vec<MetaslabState>,
    pub file_index: FileIndex,
}

impl<D: BlockDevice> FileSystem<D> {
    pub fn superblock(&self) -> Option<&Superblock> {
        self.superblock.as_ref()
    }

    pub fn uberblock(&self) -> Option<&Uberblock> {
        self.uberblock.as_ref()
    }

    /// Format file system
    pub async fn format(dev: D, total_blocks: u64) -> Result<Self, FileSystemError> {
        let mut fs = FileSystem {
            dev: dev,
            superblock: None,
            uberblock: None,
            metaslabs: Vec::new(),
            file_index: FileIndex {
                next_file_id: 1,
                files: BTreeMap::new(),
            },
        };

        // Zero out uberblock slots (TXG 0)
        let zero_uber = Uberblock {
            magic: 0,
            version: 0,
            txg: 0,
            timestamp: 0,
            file_index_extent: Vec::new(),
        };
        fs.uberblock = Some(zero_uber.clone());
        for slot in 0..UBERBLOCK_BLOCK_COUNT {
            fs.write_block_checked_txg(UBERBLOCK_START + slot, &zero_uber)
                .await?;
        }

        // Write superblock
        let metaslab_count = (total_blocks / METASLAB_SIZE_BLOCKS) as u32;
        let superblock = Superblock {
            magic: FS_ID.to_string(),
            block_size: BLOCK_SIZE as u32,
            total_blocks,
            metaslab_count,
            metaslab_size_blocks: METASLAB_SIZE_BLOCKS,
            metaslab_table_start: METASLAB_TABLE_START,
        };
        for mirror_idx in 0..3 {
            fs.write_block_checked_txg(SUPERBLOCK_LBA + mirror_idx, &superblock)
                .await?;
        }
        fs.superblock = Some(superblock);

        // Calculate metadata layout
        let metaslab_headers_start = METASLAB_TABLE_START;
        let metaslab_headers_end = metaslab_headers_start + metaslab_count as u64;

        let spacemap_logs_start = metaslab_headers_end;
        let spacemap_logs_end =
            spacemap_logs_start + (metaslab_count as u64 * SPACEMAP_LOG_BLOCKS_PER_METASLAB);

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
            fs.write_block_checked_txg(METASLAB_TABLE_START + i as u64, &header)
                .await?;

            let entry = SpaceMapEntry::Free {
                start: start_block,
                len: METASLAB_SIZE_BLOCKS,
            };

            // Write initial space map entry with checksum
            fs.write_block_checked_txg(header.spacemap_start, &entry)
                .await?;

            let mut free_extents = BTreeMap::new();
            free_extents.insert(start_block, METASLAB_SIZE_BLOCKS);

            let write_cursor = header.spacemap_start + 1;
            metaslabs.push(MetaslabState {
                header,
                header_block: METASLAB_TABLE_START + i as u64,
                free_extents,
                write_cursor,
            });
        }
        fs.metaslabs = metaslabs;

        // write root file index
        fs.persist_root_index().await?;

        Ok(fs)
    }

    /// Update root file index
    pub async fn persist_root_index(&mut self) -> Result<(), FileSystemError> {
        println!("persist_file_index called");
        let data = bincode::serialize(&self.file_index).unwrap();

        // Account for block header overhead when calculating space needed
        let blocks_needed = Self::calculate_blocks_needed(data.len());

        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed).await?;

        // Write index data using checksummed blocks
        let mut bytes_left = &data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len() {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);

                if take > 0 {
                    let chunk = &bytes_left[..take];
                    // Write with current TXG + 1 (the new transaction)
                    self.write_block_checked_txg(extent.start() + i, &chunk)
                        .await?;
                    bytes_left = &bytes_left[take..];
                } else {
                    // Write empty block to maintain extent structure
                    let empty: &[u8] = &[];
                    self.write_block_checked_txg(extent.start() + i, &empty)
                        .await?;
                }
            }
        }

        let old_extents = self.uberblock().map(|ub| ub.file_index_extent.clone());

        self.commit_txg(allocated_extents.clone()).await?;

        // Free old index blocks AFTER commit
        if let Some(old_extents) = old_extents {
            for extent in &old_extents {
                self.free(extent.start(), extent.len()).await?;
            }
        }

        Ok(())
    }

    /// Read file system headers and load root file index
    pub async fn load(dev: D) -> Result<Self, FileSystemError> {
        let mut fs = FileSystem {
            dev: dev,
            superblock: None,
            uberblock: None,
            metaslabs: Vec::new(),
            file_index: FileIndex {
                next_file_id: 1,
                files: BTreeMap::new(),
            },
        };

        fs.scan_uberblocks().await;

        let superblock = fs
            .superblock()
            .ok_or(FileSystemError::NotFormatted)?
            .clone();
        let uber = fs.uberblock().ok_or(FileSystemError::NotFormatted)?.clone();

        // Load metaslabs and replay space maps
        let mut metaslabs = Vec::new();

        for i in 0..superblock.metaslab_count {
            let (_, header): (u64, MetaslabHeader) = fs
                .read_block_checked(superblock.metaslab_table_start + i as u64)
                .await?;

            let mut free_extents = BTreeMap::new();

            // Replay spacemap log entries
            for b in 0..header.spacemap_blocks {
                // Try to read the space map entry
                let (_entry_txg, entry) = fs
                    .read_block_checked::<SpaceMapEntry>(header.spacemap_start + b as u64)
                    .await?;
                // Process valid entries only
                match entry {
                    SpaceMapEntry::Alloc { start, len } => {
                        if len == 0 {
                            continue;
                        }

                        // Remove allocated chunk from free map (slicing logic)
                        let mut to_add = Vec::new();
                        let mut to_rem = Vec::new();
                        for (&fs, &fl) in free_extents.iter() {
                            let fe = fs + fl;
                            let ae = start + len;
                            if ae <= fs || start >= fe {
                                continue;
                            }
                            to_rem.push(fs);
                            if fs < start {
                                to_add.push((fs, start - fs));
                            }
                            if ae < fe {
                                to_add.push((ae, fe - ae));
                            }
                        }
                        for r in to_rem {
                            free_extents.remove(&r);
                        }
                        for (s, l) in to_add {
                            free_extents.insert(s, l);
                        }

                        //println!("LOAD: Replayed Alloc {{{}:{}}}, TXG {}", start, len, entry_txg);
                    }
                    SpaceMapEntry::Free { start, len } => {
                        if len == 0 {
                            continue;
                        }

                        // Apply merge logic consistent with free()
                        let mut new_start = start;
                        let mut new_len = len;

                        if let Some((&prev_start, &prev_len)) =
                            free_extents.range(..start).next_back()
                        {
                            if prev_start + prev_len >= start {
                                new_start = prev_start;
                                new_len = (prev_start + prev_len).max(start + len) - new_start;
                                free_extents.remove(&prev_start);
                            }
                        }

                        loop {
                            if let Some((&next_start, &next_len)) =
                                free_extents.range(new_start..).next()
                            {
                                if next_start <= new_start + new_len {
                                    new_len = (next_start + next_len).max(new_start + new_len)
                                        - new_start;
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
                write_cursor,
            });
        }
        fs.metaslabs = metaslabs;

        // Reconstruct file index from extents with validation
        let mut index_bytes = Vec::new();
        for extent in &uber.file_index_extent {
            for i in 0..extent.len() {
                let (block_txg, chunk) =
                    fs.read_block_checked::<Vec<u8>>(extent.start() + i).await?;
                // Validate TXG matches or is from earlier transaction
                if block_txg <= uber.txg {
                    index_bytes.extend_from_slice(&chunk);
                } else {
                    println!(
                        "WARNING: File index block has TXG {} > uberblock TXG {}",
                        block_txg, uber.txg
                    );
                }
            }
        }

        let file_index: FileIndex =
            bincode::deserialize(&index_bytes).expect("Failed to deserialize FileIndex");
        fs.file_index = file_index;

        Ok(fs)
    }

    /// Read a files contents
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, FileSystemError> {
        let inode_ref = self.get_file_at_path(path).await?;

        let inode = self.read_inode(&inode_ref).await?;

        if !inode.inode_type.is_file() {
            panic!("'{}' is not a regular file", path.display());
        }

        let mut out = Vec::with_capacity(inode.inode_type.size_bytes() as usize);
        let mut remaining = inode.inode_type.size_bytes();

        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode.inode_type.extents() {
            for i in 0..extent.len() {
                let (_, len) = self
                    .read_raw_block_checked_into(extent.start() + i, &mut block_buf)
                    .await?;
                let take = remaining.min(len as u64) as usize;
                out.extend_from_slice(&block_buf[..take]);
                remaining -= take as u64;
            }
        }

        Ok(out)
    }

    /// List directory contents
    pub async fn ls(&self, path: &Path) -> Result<FileIndex, FileSystemError> {
        let (index, _) = self.get_directory_at_path(path).await?;
        Ok(index)
    }

    pub async fn mkdir(&mut self, path: &Path) -> Result<FileId, FileSystemError> {
        let (mut parent, old_inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;

        if let Some(ref inode_ref) = old_inode_ref {
            let old_inode = self.read_inode(inode_ref).await?;
            if !old_inode.inode_type.is_directory() {
                panic!("'{}' is not a directory", path.display());
            }
            Ok(old_inode.id)
        } else {
            let dir = FileIndex {
                next_file_id: 1,
                files: BTreeMap::new(),
            };
            let data = bincode::serialize(&dir).unwrap();

            // Account for block header overhead when calculating space needed
            let blocks_needed = Self::calculate_blocks_needed(data.len());

            let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed).await?;

            // Write index data using checksummed blocks
            let mut bytes_left = &data[..];
            for extent in &allocated_extents {
                for i in 0..extent.len() {
                    let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);

                    if take > 0 {
                        let chunk = &bytes_left[..take];
                        // Write with current TXG + 1 (the new transaction)
                        self.write_raw_block_checked_txg(extent.start() + i, &chunk)
                            .await?;
                        bytes_left = &bytes_left[take..];
                    } else {
                        // Write empty block to maintain extent structure
                        let empty: &[u8] = &[];
                        self.write_raw_block_checked_txg(extent.start() + i, &empty)
                            .await?;
                    }
                }
            }

            // Create or update file ID
            let file_id = old_inode_ref
                .as_ref()
                .map(|r| r.file_id)
                .unwrap_or(parent.next_file_id);

            if old_inode_ref.is_none() {
                parent.next_file_id += 1;
            }

            // Create new inode
            let new_inode = FileInode {
                id: file_id,
                inode_type: InodeType::Directory {
                    extents: allocated_extents,
                },
            };

            // Write inode and get its extent
            let extents = self.write_inode(&new_inode).await?;

            // Update file index with new inode reference
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or(FileSystemError::FileNotFound)?;
            parent.files.insert(
                filename.to_string(),
                InodeRef {
                    file_id,
                    extents,
                    permissions: Permissions::default(),
                },
            );

            self.persist_directory_changes(path, parent).await?;
            Ok(new_inode.id)
        }
    }

    /// Write or overwrite a files contents
    pub async fn write_file(
        &mut self,
        path: &Path,
        data: &[u8],
    ) -> Result<FileId, FileSystemError> {
        let (mut parent, old_inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;

        // If exists, ensure it's a file not a virtual block device
        if let Some(ref inode_ref) = old_inode_ref {
            let old_inode = self.read_inode(inode_ref).await?;
            if !old_inode.inode_type.is_file() {
                return Err(FileSystemError::NotAFile);
            }
        }

        // Calculate blocks needed accounting for header overhead
        let blocks_needed = Self::calculate_blocks_needed(data.len());

        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed).await?;
        println!("allocated extents: {:?}", allocated_extents);

        // Write file data in chunks that fit in BLOCK_PAYLOAD_SIZE
        let mut current_offset = 0;
        for extent in &allocated_extents {
            for i in 0..extent.len() {
                if current_offset >= data.len() {
                    // Write empty block for unused allocated space
                    let empty: &[u8] = &[];
                    self.write_raw_block_checked_txg(extent.start() + i, &empty)
                        .await?;
                    continue;
                }

                let take = (data.len() - current_offset).min(BLOCK_PAYLOAD_SIZE);
                let chunk = &data[current_offset..current_offset + take];

                self.write_raw_block_checked_txg(extent.start() + i, &chunk)
                    .await?;
                current_offset += take;
            }
        }

        // Create or update file ID
        let file_id = old_inode_ref
            .as_ref()
            .map(|r| r.file_id)
            .unwrap_or(parent.next_file_id);

        if old_inode_ref.is_none() {
            parent.next_file_id += 1;
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
        let extents = self.write_inode(&new_inode).await?;

        // Update file index with new inode reference
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(FileSystemError::FileNotFound)?;
        parent.files.insert(
            filename.to_string(),
            InodeRef {
                file_id,
                extents,
                permissions: Permissions::default(),
            },
        );

        self.persist_directory_changes(path, parent).await?;

        // Free old inode and data blocks
        if let Some(old_ref) = old_inode_ref {
            let old_inode = self.read_inode(&old_ref).await?;

            // Free old data blocks
            for extent in old_inode.inode_type.extents() {
                self.free(extent.start(), extent.len()).await?;
            }

            // Free old inode blocks
            for extent in &old_ref.extents {
                self.free(extent.start(), extent.len()).await?;
            }
        }

        Ok(file_id)
    }

    /// Delete a virtual block device or file
    pub async fn delete(&mut self, path: &Path) -> Result<FileId, FileSystemError> {
        let (mut parent, inode_ref_opt) = self.get_maybe_inode_ref_at_path(path).await?;

        let inode_ref = match inode_ref_opt {
            Some(ref inode_ref) => inode_ref,
            None => return Err(FileSystemError::FileNotFound),
        };

        let inode = self.read_inode(&inode_ref).await?;

        for extent in inode.inode_type.extents() {
            self.free(extent.start(), extent.len()).await?;
        }

        for extent in &inode_ref.extents {
            self.free(extent.start(), extent.len()).await?;
        }

        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(FileSystemError::FileNotFound)?;

        parent.files.remove(filename);

        self.persist_directory_changes(path, parent).await?;

        Ok(inode.id)
    }

    pub async fn sync(&mut self) -> Result<(), FileSystemError> {
        let uber = self.uberblock.as_ref().ok_or(RaidZError::NotFormatted)?;
        self.commit_txg(uber.file_index_extent.clone()).await?;
        Ok(())
    }

    async fn commit_txg(&mut self, file_index_extent: Vec<Extent>) -> Result<u64, FileSystemError> {
        let uber = self
            .uberblock()
            .ok_or(FileSystemError::NotFormatted)?
            .clone();
        let txg = uber.txg;

        self.dev.flush().await?;

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

        let base_lba = uberblock_stripe_id * DATA_SHARDS as u64;
        for shard_idx in 0..DATA_SHARDS {
            self.dev
                .write_block(base_lba + shard_idx as u64, &block_buf)
                .await?;
        }

        // Flush again to ensure uberblock is durable
        self.dev.flush().await?;

        self.uberblock = Some(uberblock);

        Ok(txg)
    }

    pub async fn write_raw_block_checked_txg(
        &mut self,
        lba: BlockId,
        data: &[u8],
    ) -> Result<(), FileSystemError> {
        let uber = self
            .uberblock()
            .ok_or(FileSystemError::NotFormatted)?
            .clone();

        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(uber.txg, data, &mut block_buf)?;

        self.dev.write_block(lba, &block_buf).await?;
        Ok(())
    }

    pub async fn write_block_checked_txg<T: Serialize>(
        &mut self,
        lba: BlockId,
        value: &T,
    ) -> Result<(), FileSystemError> {
        let uber = self
            .uberblock()
            .ok_or(FileSystemError::NotFormatted)?
            .clone();

        let payload = bincode::serialize(value)?;
        assert!(payload.len() <= BLOCK_PAYLOAD_SIZE);

        // Prepare the full block with header
        let mut block_buf = [0u8; BLOCK_SIZE];
        self.prepare_block_buffer(uber.txg, &payload, &mut block_buf)?;

        self.dev.write_block(lba, &block_buf).await?;
        Ok(())
    }

    fn prepare_block_buffer(
        &self,
        txg: u64,
        data: &[u8],
        buf: &mut [u8; BLOCK_SIZE],
    ) -> Result<(), FileSystemError> {
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

    /// Returns the blocks raw payload excluding headers
    pub async fn read_raw_block_checked_into(
        &self,
        lba: BlockId,
        buf: &mut [u8],
    ) -> Result<(u64, usize), FileSystemError> {
        assert!(buf.len() >= BLOCK_SIZE);

        self.dev.read_block(lba, buf).await?;

        let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;

        if header.magic != 0x52414944 {
            return Err(FileSystemError::InvalidMagic);
        }

        let payload_len = header.payload_len as usize;
        let payload = &buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + header.payload_len as usize];

        // Checksum verification
        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return Err(FileSystemError::ChecksumMismatch);
        }

        buf.copy_within(BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload_len, 0);

        Ok((header.txg, payload_len))
    }

    async fn scan_uberblocks(&mut self) -> Option<(u64, Uberblock)> {
        // Load superblock (TXG 0 from format)
        match self.read_block_checked::<Superblock>(SUPERBLOCK_LBA).await {
            Ok((_, superblock)) => self.superblock = Some(superblock),
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
        if let Some((_txg, uberblock)) = best.clone() {
            self.uberblock = Some(uberblock);
        }

        best
    }

    /// Returns deserialized type from block
    pub async fn read_block_checked<T: for<'a> Deserialize<'a>>(
        &self,
        lba: BlockId,
    ) -> Result<(u64, T), FileSystemError> {
        let mut buf = vec![0u8; BLOCK_SIZE];
        self.dev.read_block(lba, &mut buf).await?;

        let header: BlockHeader = bincode::deserialize(&buf[..BLOCK_HEADER_SIZE])?;

        let payload_start = BLOCK_HEADER_SIZE;
        let payload_end = payload_start + header.payload_len as usize;
        let payload = &buf[payload_start..payload_end];

        let checksum = *blake3::hash(payload).as_bytes();
        if checksum != header.checksum {
            return Err(FileSystemError::ChecksumMismatch);
        }

        let value = bincode::deserialize(payload)?;
        Ok((header.txg, value))
    }
}

#[cfg(test)]
#[path = "tests/fs_tests.rs"]
mod tests;
