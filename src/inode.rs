use serde::{Serialize, Deserialize};
use crate::disk::{BLOCK_SIZE,FileId,BlockDevice};
use crate::fs::{FileSystem,FileSystemError,BLOCK_PAYLOAD_SIZE};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Extent {
    Full { start: u64, len: u64 },
    Partial { start: u64, len: u64, used: u64 },
}

impl Extent {
    pub fn start(&self) -> u64 {
        match self {
            Extent::Full { start, .. } => *start,
            Extent::Partial { start, .. } => *start,
        }
    }
    
    pub fn len(&self) -> u64 {
        match self {
            Extent::Full { len, .. } => *len,
            Extent::Partial { len, .. } => *len,
        }
    }
    
    pub fn used(&self) -> u64 {
        match self {
            Extent::Full { len, .. } => *len,  // Fully used
            Extent::Partial { used, .. } => *used,
        }
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum SparseExtent {
    Allocated(Extent),
    Sparse(u64)
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

    pub fn extents(&self) -> &[Extent] { //Vec<Extent> {
        match self {
            InodeType::File { extents, .. } => extents, //.clone(),
            InodeType::Block { extents, size_bytes: _, block_size: _ } => extents, /*{
                extents.iter().filter_map(|value| {
                    if let SparseExtent::Allocated(e) = value {
                        Some(e.clone())
                    } else {
                        None
                    }
                }).collect()
            }*/
        }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, InodeType::Block { .. })
    }

    pub fn is_file(&self) -> bool {
        matches!(self, InodeType::File { .. })
    }

    /*pub fn sparse_extents(&self) -> &[SparseExtent] {
        match self {
            InodeType::Block { extents, .. } => extents,
            _ => panic!("Not a block device"),
        }
    }*/
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

impl<D: BlockDevice> FileSystem<D> {
    pub async fn read_inode(&self, inode_ref: &InodeRef) -> Result<FileInode, FileSystemError> {
        let mut inode_bytes = Vec::new();
        
        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode_ref.extents() {
            for i in 0..extent.len() {
                let (_, _chunk) = self.read_raw_block_checked_into(extent.start() + i, &mut block_buf).await?;
                inode_bytes.extend_from_slice(&block_buf);
            }
        }
        
        let inode: FileInode = bincode::deserialize(&inode_bytes)?;
        
        Ok(inode)
    }

    pub async fn write_inode(&mut self, inode: &FileInode) -> Result<Vec<Extent>, FileSystemError> {
        let inode_data = bincode::serialize(inode).unwrap();
        let blocks_needed = Self::calculate_blocks_needed(inode_data.len());
        
        let allocated_extents = self.allocate_blocks_stripe_aligned(blocks_needed).await?;
        
        let mut bytes_left = &inode_data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len() {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);
                
                if take > 0 {
                    let chunk = &bytes_left[..take];
                    self.write_raw_block_checked_txg(extent.start() + i, &chunk).await?;
                    bytes_left = &bytes_left[take..];
                } else {
                    let empty: &[u8] = &[];
                    self.write_raw_block_checked_txg(extent.start() + i, &empty).await?;
                }
            }
        }
        
        Ok(allocated_extents)
    }

}