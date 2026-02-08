use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{BLOCK_PAYLOAD_SIZE, FileSystem, FileSystemError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileIndex {
    pub next_file_id: FileId,
    pub files: BTreeMap<String, InodeRef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Extent {
    pub start: u64,
    pub len: u64,
}

impl Extent {
    pub fn start(&self) -> u64 {
        self.start
    }
    pub fn len(&self) -> u64 {
        self.len
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InodeType {
    File {
        size_bytes: u64,
        extents: Vec<Extent>,
    },
    Directory {
        extents: Vec<Extent>,
    },
    Block {
        size_bytes: u64,
        block_size: u32, // block size (e.g., 4KB, 8KB, 64KB)
        extents: Vec<Extent>,
    },
}

impl InodeType {
    pub fn size_bytes(&self) -> u64 {
        match self {
            InodeType::File { size_bytes, .. } => *size_bytes,
            InodeType::Block { size_bytes, .. } => *size_bytes,
            InodeType::Directory { extents: _ } => 0,
        }
    }

    pub fn extents(&self) -> &[Extent] {
        match self {
            InodeType::File { extents, .. } => extents,
            InodeType::Directory { extents } => extents,
            InodeType::Block {
                extents,
                size_bytes: _,
                block_size: _,
            } => extents,
        }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, InodeType::Block { .. })
    }

    pub fn is_file(&self) -> bool {
        matches!(self, InodeType::File { .. })
    }

    pub fn is_directory(&self) -> bool {
        matches!(self, InodeType::Directory { .. })
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
    pub extents: Vec<Extent>, // Where the inode is stored on disk
    pub permissions: Permissions,
}

impl InodeRef {
    pub fn extents(&self) -> &[Extent] {
        &self.extents
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Permissions {
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
}

impl Permissions {
    pub fn default() -> Permissions {
        Permissions {
            mode: 600,
            uid: 0,
            gid: 0,
        }
    }
}

impl<D: BlockDevice> FileSystem<D> {
    pub async fn read_file_index(
        &self,
        inode_type: &InodeType,
    ) -> Result<FileIndex, FileSystemError> {
        if let InodeType::Directory { extents } = inode_type {
            let mut index_bytes = Vec::new();
            let mut block_buf = [0u8; BLOCK_SIZE];

            for extent in extents {
                for i in 0..extent.len() {
                    let (_, _chunk) = self
                        .read_raw_block_checked_into(extent.start() + i, &mut block_buf)
                        .await?;
                    index_bytes.extend_from_slice(&block_buf);
                }
            }

            let index: FileIndex = bincode::deserialize(&index_bytes)?;
            Ok(index)
        } else {
            Err(FileSystemError::DirectoryNotFound)
        }
    }

    pub async fn read_inode(&self, inode_ref: &InodeRef) -> Result<FileInode, FileSystemError> {
        let mut inode_bytes = Vec::new();

        let mut block_buf = [0u8; BLOCK_SIZE];
        for extent in inode_ref.extents() {
            for i in 0..extent.len() {
                let (_, _chunk) = self
                    .read_raw_block_checked_into(extent.start() + i, &mut block_buf)
                    .await?;
                inode_bytes.extend_from_slice(&block_buf);
            }
        }

        let inode: FileInode = bincode::deserialize(&inode_bytes)?;

        Ok(inode)
    }

    pub async fn write_inode(
        &self,
        inode: &FileInode,
        extent_pool: Option<&mut Vec<Extent>>,
    ) -> Result<Vec<Extent>, FileSystemError> {
        let inode_data = bincode::serialize(inode)?;
        let blocks_needed = Self::calculate_blocks_needed(inode_data.len());

        let allocated_extents = match extent_pool {
            Some(pool) => Self::consume_blocks_from_pool(pool, blocks_needed as usize),
            None => self.allocate_blocks_stripe_aligned(blocks_needed).await?,
        };

        let mut bytes_left = &inode_data[..];
        for extent in &allocated_extents {
            for i in 0..extent.len() {
                let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);

                if take > 0 {
                    let chunk = &bytes_left[..take];
                    self.write_raw_block_checked_txg(extent.start() + i, &chunk)
                        .await?;
                    bytes_left = &bytes_left[take..];
                } else {
                    let empty: &[u8] = &[];
                    self.write_raw_block_checked_txg(extent.start() + i, &empty)
                        .await?;
                }
            }
        }

        Ok(allocated_extents)
    }

    /// Convert Path to components
    fn path_components(path: &Path) -> Vec<String> {
        path.components()
            .filter_map(|c| {
                use std::path::Component;
                match c {
                    Component::Normal(s) => s.to_str().map(String::from),
                    _ => None,
                }
            })
            .collect()
    }

    /// Navigate to directory at path
    pub async fn get_directory_at_path(
        &self,
        path: &Path,
    ) -> Result<(FileIndex, Option<InodeRef>), FileSystemError> {
        let components = Self::path_components(path);

        if components.is_empty() {
            return Ok((self.file_index.read().await.clone(), None));
        }

        let mut current_index = self.file_index.read().await.clone();
        let mut current_inode_ref = None;

        for component in &components {
            let inode_ref = current_index
                .files
                .get(component)
                .ok_or(FileSystemError::DirectoryNotFound)?
                .clone();

            let inode = self.read_inode(&inode_ref).await?;

            if !inode.inode_type.is_directory() {
                return Err(FileSystemError::NotADirectory);
            }

            current_index = self.read_file_index(&inode.inode_type).await?;
            current_inode_ref = Some(inode_ref);
        }

        Ok((current_index, current_inode_ref))
    }

    pub(crate) async fn get_directory_at_path_with_count(
        &self,
        path: &Path,
    ) -> Result<(FileIndex, Option<InodeRef>, usize), FileSystemError> {
        let components = Self::path_components(path);

        let mut total_blocks = 0;

        if components.is_empty() {
            // Count root index
            let index = self.file_index.read().await;
            let root_data = bincode::serialize(&*index)?;
            total_blocks += Self::calculate_blocks_needed(root_data.len());
            return Ok((index.clone(), None, total_blocks as usize));
        }

        let mut current_index = self.file_index.read().await.clone();
        let mut current_inode_ref = None;

        for component in &components {
            let inode_ref = current_index
                .files
                .get(component)
                .ok_or(FileSystemError::DirectoryNotFound)?
                .clone();

            let inode = self.read_inode(&inode_ref).await?;

            if !inode.inode_type.is_directory() {
                return Err(FileSystemError::NotADirectory);
            }

            let dir_index = self.read_file_index(&inode.inode_type).await?;

            // Count blocks for this directory's FileIndex
            let index_data = bincode::serialize(&dir_index)?;
            total_blocks += Self::calculate_blocks_needed(index_data.len());

            // Count blocks for this directory's FileInode
            let inode_data = bincode::serialize(&inode)?;
            total_blocks += Self::calculate_blocks_needed(inode_data.len());

            current_index = dir_index;
            current_inode_ref = Some(inode_ref);
        }

        // Count root index
        let index = self.file_index.read().await;
        let root_data = bincode::serialize(&*index)?;
        total_blocks += Self::calculate_blocks_needed(root_data.len());

        Ok((current_index, current_inode_ref, total_blocks as usize))
    }

    /// Navigate to a file at path and return its InodeRef
    pub async fn get_file_at_path(&self, path: &Path) -> Result<InodeRef, FileSystemError> {
        let components = Self::path_components(path);

        if components.is_empty() {
            return Err(FileSystemError::FileNotFound);
        }

        // Navigate to parent directory
        let parent_path: std::path::PathBuf = if components.len() == 1 {
            std::path::PathBuf::new()
        } else {
            components[..components.len() - 1].iter().collect()
        };

        let (parent_index, _) = self.get_directory_at_path(&parent_path).await?;

        // Get the file from parent directory
        let file_name = &components[components.len() - 1];
        let inode_ref = parent_index
            .files
            .get(file_name)
            .ok_or(FileSystemError::FileNotFound)?
            .clone();

        // Verify it's actually a file (not a directory)
        let inode = self.read_inode(&inode_ref).await?;
        if !inode.inode_type.is_file() {
            return Err(FileSystemError::NotAFile);
        }

        Ok(inode_ref)
    }

    pub(crate) async fn get_maybe_inode_ref_at_path_with_count(
        &self,
        path: &Path,
    ) -> Result<(FileIndex, Option<InodeRef>, usize), FileSystemError> {
        let components = Self::path_components(path);

        if components.is_empty() {
            return Err(FileSystemError::FileNotFound);
        }

        // Navigate to parent directory
        let parent_path: std::path::PathBuf = if components.len() == 1 {
            std::path::PathBuf::new()
        } else {
            components[..components.len() - 1].iter().collect()
        };

        let (parent_index, _, total_blocks) =
            self.get_directory_at_path_with_count(&parent_path).await?;

        // Get the file from parent directory
        let file_name = &components[components.len() - 1];
        let inode_ref = parent_index.files.get(file_name).map(|i| i.clone());

        Ok((parent_index, inode_ref, total_blocks))
    }

    /// Persist directory changes using copy-on-write from the target directory back to root.
    /// This updates the parent directory's index and propagates changes up the directory tree.
    pub async fn persist_directory_changes(
        &self,
        file_path: &Path,
        mut updated_index: FileIndex,
        updated_inode: Option<FileInode>,
        mut extent_pool: Option<&mut Vec<Extent>>,
    ) -> Result<(), FileSystemError> {
        match updated_inode {
            Some(updated_inode) => {
                // Write inode and get its extent
                let extents = self
                    .write_inode(&updated_inode, extent_pool.as_mut().map(|p| &mut **p))
                    .await?;

                // Update file index with new inode reference
                let filename = file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or(FileSystemError::FileNotFound)?;
                updated_index.files.insert(
                    filename.to_string(),
                    InodeRef {
                        file_id: updated_inode.id,
                        extents,
                        permissions: Permissions::default(),
                    },
                );
            }
            _ => (),
        }

        let components = Self::path_components(file_path);

        // If we're in the root directory, just persist the file index
        if components.len() <= 1 {
            *self.file_index.write().await = updated_index;
            self.persist_root_index(extent_pool.as_mut().map(|p| &mut **p))
                .await?;
            return Ok(());
        }

        let dir_components = &components[..components.len() - 1];
        let mut current_index = updated_index;

        // Iterate backwards through the directory path
        for depth in (0..dir_components.len()).rev() {
            // Step 1: Write the FileIndex to disk
            let index_data = bincode::serialize(&current_index)?;
            let blocks_needed = Self::calculate_blocks_needed(index_data.len());
            let index_extents = match extent_pool.as_mut() {
                Some(pool) => Self::consume_blocks_from_pool(pool, blocks_needed as usize),
                None => self.allocate_blocks_stripe_aligned(blocks_needed).await?,
            };

            let mut bytes_left = &index_data[..];
            for extent in &index_extents {
                for i in 0..extent.len() {
                    let take = bytes_left.len().min(BLOCK_PAYLOAD_SIZE);
                    if take > 0 {
                        let chunk = &bytes_left[..take];
                        self.write_raw_block_checked_txg(extent.start() + i, chunk)
                            .await?;
                        bytes_left = &bytes_left[take..];
                    } else {
                        let empty: &[u8] = &[];
                        self.write_raw_block_checked_txg(extent.start() + i, empty)
                            .await?;
                    }
                }
            }

            // Step 2: Get the file_id and old inode ref
            let (dir_file_id, old_dir_ref) = if depth == 0 {
                let dir_name = &dir_components[0];
                let old_ref = self
                    .file_index
                    .read()
                    .await
                    .files
                    .get(dir_name)
                    .cloned()
                    .ok_or(FileSystemError::DirectoryNotFound)?;
                let inode = self.read_inode(&old_ref).await?;
                (inode.id, old_ref)
            } else {
                let parent_path: std::path::PathBuf = dir_components[..depth].iter().collect();
                let (parent_index, _) = self.get_directory_at_path(&parent_path).await?;
                let dir_name = &dir_components[depth];
                let old_ref = parent_index
                    .files
                    .get(dir_name)
                    .cloned()
                    .ok_or(FileSystemError::DirectoryNotFound)?;
                let inode = self.read_inode(&old_ref).await?;
                (inode.id, old_ref)
            };

            // Step 3: Create new FileInode pointing to the FileIndex
            let new_inode = FileInode {
                id: dir_file_id,
                inode_type: InodeType::Directory {
                    extents: index_extents.clone(),
                },
            };

            // Step 4: Write the FileInode to disk
            let inode_extents = self
                .write_inode(&new_inode, extent_pool.as_mut().map(|p| &mut **p))
                .await?;

            // Step 5: Update the parent's reference to this directory
            if depth == 0 {
                let dir_name = &dir_components[0];

                self.file_index.write().await.files.insert(
                    dir_name.to_string(),
                    InodeRef {
                        file_id: dir_file_id,
                        extents: inode_extents,
                        permissions: Permissions::default(),
                    },
                );

                // Free old blocks (both inode and index)
                for extent in &old_dir_ref.extents {
                    self.free(extent.start(), extent.len()).await?;
                }
                let old_inode = self.read_inode(&old_dir_ref).await.ok();
                if let Some(old_inode) = old_inode {
                    for extent in old_inode.inode_type.extents() {
                        self.free(extent.start(), extent.len()).await?;
                    }
                }

                // Persist root index
                self.persist_root_index(extent_pool.as_mut().map(|p| &mut **p))
                    .await?;
            } else {
                let parent_path: std::path::PathBuf = dir_components[..depth].iter().collect();
                let (mut parent_index, _) = self.get_directory_at_path(&parent_path).await?;
                let dir_name = &dir_components[depth];

                parent_index.files.insert(
                    dir_name.to_string(),
                    InodeRef {
                        file_id: dir_file_id,
                        extents: inode_extents,
                        permissions: Permissions::default(),
                    },
                );

                // Free old blocks (both inode and index)
                for extent in &old_dir_ref.extents {
                    self.free(extent.start(), extent.len()).await?;
                }
                let old_inode = self.read_inode(&old_dir_ref).await.ok();
                if let Some(old_inode) = old_inode {
                    for extent in old_inode.inode_type.extents() {
                        self.free(extent.start(), extent.len()).await?;
                    }
                }

                // This becomes the index we need to write in the next iteration
                current_index = parent_index;
            }
        }

        Ok(())
    }
}
