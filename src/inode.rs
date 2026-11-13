use crate::blkptr::BlockPtr;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileIndex {
    pub files: BTreeMap<String, InodeRef>,
}

impl FileIndex {
    pub fn new() -> Self {
        FileIndex {
            files: BTreeMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
        blocks: Vec<BlockPtr>,
    },
    Directory {
        /// The directory's serialized `FileIndex`.
        blocks: Vec<BlockPtr>,
    },
    /// Copy-on-write virtual block device (see `crate::volume`).
    Volume {
        size_bytes: u64,
        volblocksize: u32,
        levels: u8,
        root: Option<BlockPtr>,
        props: VolumeProps,
        /// The intent log of the writer running it, if any (see
        /// `crate::zil`): writes acknowledged since the tree was committed.
        log: Option<VolumeLog>,
    },
}

/// A volume's intent log: the regions its writer appends flushed writes
/// to, until a commit folds them into the tree. Registered (and its space
/// claimed) by a commit; replayed by the committer if the writer goes away
/// first.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VolumeLog {
    /// Oldest first (at most two: the one a commit was taken from, and the
    /// one started by that commit).
    pub regions: Vec<LogRegion>,
    /// Records of `regions[0]` before this one are already in the tree.
    pub from_seq: u64,
    /// The writer lease (`writer/...`) whose lent space holds it.
    pub writer: String,
    pub writer_epoch: u64,
    /// The `vol/<name>` lease it was written under: once that's gone, no
    /// more records can be acknowledged, and it may be replayed.
    pub vol_owner: String,
    pub vol_epoch: u64,
}

/// A run of blocks holding intent log records.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LogRegion {
    pub start: u64,
    pub len: u64,
    /// Tells this region's records from anything else ever written there.
    pub guid: u64,
}

impl LogRegion {
    pub fn extent(&self) -> Extent {
        Extent {
            start: self.start,
            len: self.len,
        }
    }
}

/// How a volume is exported to the kernel when it is auto-mounted.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Nbd,
    Nvme,
}

impl std::str::FromStr for Transport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "nbd" => Ok(Transport::Nbd),
            "nvme" => Ok(Transport::Nvme),
            _ => Err(format!("unknown transport '{}' (nbd or nvme)", s)),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Transport::Nbd => "nbd",
            Transport::Nvme => "nvme",
        })
    }
}

/// Placement of a volume across the cluster's `listen` nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VolumeProps {
    /// Nodes (by name) that may run the volume, most preferred first. It
    /// runs on the first live one and stays there (no fail-back) until
    /// moved. Empty: any node.
    pub nodes: Vec<String>,
    /// Export it and attach the kernel device wherever it runs. None: only
    /// run by hand (`nbd` / `nvme`).
    pub automount: Option<Transport>,
    /// A disabled volume isn't placed anywhere.
    pub enabled: bool,
    /// Asked to move to this node (`volume move`): it goes there while that
    /// node is live, and the node clears this once it runs the volume.
    pub move_to: Option<String>,
}

impl Default for VolumeProps {
    fn default() -> Self {
        VolumeProps {
            nodes: Vec::new(),
            automount: None,
            enabled: true,
            move_to: None,
        }
    }
}

impl InodeType {
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
    /// Where the serialized `FileInode` is stored.
    pub inode: Vec<BlockPtr>,
    pub permissions: Permissions,
}

impl InodeRef {
    /// Every on-disk extent holding the inode itself.
    pub fn inode_extents(&self) -> Vec<Extent> {
        self.inode.iter().map(BlockPtr::extent).collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Permissions {
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
}

impl Default for Permissions {
    fn default() -> Permissions {
        Permissions {
            mode: 0o600,
            uid: 0,
            gid: 0,
        }
    }
}

fn path_components(path: &Path) -> Vec<String> {
    use std::path::Component;
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str().map(String::from),
            _ => None,
        })
        .collect()
}

impl<D: BlockDevice> FileSystem<D> {
    pub async fn read_inode(&self, inode_ref: &InodeRef) -> Result<FileInode, FileSystemError> {
        self.read_value(&inode_ref.inode).await
    }

    pub async fn read_file_index(
        &self,
        inode_type: &InodeType,
    ) -> Result<FileIndex, FileSystemError> {
        match inode_type {
            InodeType::Directory { blocks } => self.read_value(blocks).await,
            _ => Err(FileSystemError::DirectoryNotFound),
        }
    }

    /// Every on-disk extent an inode's contents occupy (not the inode itself).
    pub(crate) async fn content_extents(
        &self,
        inode_type: &InodeType,
    ) -> Result<Vec<Extent>, FileSystemError> {
        match inode_type {
            InodeType::File { blocks, .. } | InodeType::Directory { blocks } => {
                Ok(blocks.iter().map(BlockPtr::extent).collect())
            }
            InodeType::Volume {
                root, levels, log, ..
            } => {
                let mut out = self.volume_tree_extents(root, *levels).await?;
                out.extend(
                    log.iter()
                        .flat_map(|l| l.regions.iter().map(LogRegion::extent)),
                );
                Ok(out)
            }
        }
    }

    /// Free an inode and everything its contents occupy.
    pub(crate) async fn free_inode(&self, inode_ref: &InodeRef) -> Result<(), FileSystemError> {
        let inode = self.read_inode(inode_ref).await?;
        for e in self.content_extents(&inode.inode_type).await? {
            self.free(e.start, e.len).await?;
        }
        self.free_object(&inode_ref.inode).await
    }

    /// Navigate to directory at path
    pub async fn get_directory_at_path(
        &self,
        path: &Path,
    ) -> Result<(FileIndex, Option<InodeRef>), FileSystemError> {
        let mut current_index = self.file_index.read().await.clone();
        let mut current_inode_ref = None;

        for component in path_components(path) {
            let inode_ref = current_index
                .files
                .get(&component)
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

    /// Navigate to a file at path and return its InodeRef
    pub async fn get_file_at_path(&self, path: &Path) -> Result<InodeRef, FileSystemError> {
        let (_, inode_ref) = self.get_maybe_inode_ref_at_path(path).await?;
        let inode_ref = inode_ref.ok_or(FileSystemError::FileNotFound)?;

        let inode = self.read_inode(&inode_ref).await?;
        if !inode.inode_type.is_file() {
            return Err(FileSystemError::NotAFile);
        }

        Ok(inode_ref)
    }

    /// Returns the parent directory's index and the entry at `path`, if any.
    pub(crate) async fn get_maybe_inode_ref_at_path(
        &self,
        path: &Path,
    ) -> Result<(FileIndex, Option<InodeRef>), FileSystemError> {
        let components = path_components(path);
        let (file_name, parents) = components
            .split_last()
            .ok_or(FileSystemError::FileNotFound)?;

        let parent_path: PathBuf = parents.iter().collect();
        let (parent_index, _) = self.get_directory_at_path(&parent_path).await?;
        let inode_ref = parent_index.files.get(file_name).cloned();

        Ok((parent_index, inode_ref))
    }

    /// Copy-on-write the directory chain from `file_path`'s parent up to the root.
    ///
    /// `updated_index` is the new contents of the parent directory. If
    /// `updated_inode` is given, it is written and linked under the file name.
    /// Every rewritten directory frees its previous inode and index. Does not
    /// commit; callers finish with `commit_txg`.
    pub async fn persist_directory_changes(
        &self,
        file_path: &Path,
        mut updated_index: FileIndex,
        updated_inode: Option<FileInode>,
    ) -> Result<(), FileSystemError> {
        let components = path_components(file_path);
        let (file_name, dir_components) = components
            .split_last()
            .ok_or(FileSystemError::FileNotFound)?;

        if let Some(inode) = updated_inode {
            let permissions = updated_index
                .files
                .get(file_name)
                .map(|r| r.permissions.clone())
                .unwrap_or_default();
            let inode_ptrs = self.write_value(&inode).await?;
            updated_index.files.insert(
                file_name.clone(),
                InodeRef {
                    file_id: inode.id,
                    inode: inode_ptrs,
                    permissions,
                },
            );
        }

        // ancestors[i] is the index of the directory at dir_components[..i].
        let mut ancestors = vec![self.file_index.read().await.clone()];
        for (i, name) in dir_components.iter().enumerate() {
            let dir_ref = ancestors[i]
                .files
                .get(name)
                .ok_or(FileSystemError::DirectoryNotFound)?;
            let dir_inode = self.read_inode(dir_ref).await?;
            if !dir_inode.inode_type.is_directory() {
                return Err(FileSystemError::NotADirectory);
            }
            if i + 1 < dir_components.len() {
                ancestors.push(self.read_file_index(&dir_inode.inode_type).await?);
            }
        }

        let mut current_index = updated_index;
        for depth in (0..dir_components.len()).rev() {
            let name = &dir_components[depth];
            let mut parent_index = ancestors.pop().unwrap();
            let old_ref = parent_index
                .files
                .get(name)
                .cloned()
                .ok_or(FileSystemError::DirectoryNotFound)?;

            let index_ptrs = self.write_value(&current_index).await?;
            let new_inode = FileInode {
                id: old_ref.file_id,
                inode_type: InodeType::Directory { blocks: index_ptrs },
            };
            let inode_ptrs = self.write_value(&new_inode).await?;

            self.free_inode(&old_ref).await?;

            parent_index.files.insert(
                name.clone(),
                InodeRef {
                    file_id: old_ref.file_id,
                    inode: inode_ptrs,
                    permissions: old_ref.permissions,
                },
            );
            current_index = parent_index;
        }

        *self.file_index.write().await = current_index;
        self.persist_root_index().await
    }
}
