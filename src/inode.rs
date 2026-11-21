//! The namespace: objects, the object table, and directories.
//!
//! Every object (file, directory, volume) is a *dnode* in the pool's
//! object table, indexed by its object number. A dnode holds the object's
//! tree of records (see `crate::object`), its size and attributes:
//!
//! ```text
//!   uberblock ──► object table (a tree of dnode leaves, by object number)
//!                   └─ dnode ──► the object's own tree ──► data records
//! ```
//!
//! A directory is an object whose contents are its entries, name → object
//! number. Changing a file's contents rewrites only its dnode (and the
//! table's path to it), never a directory; changing a directory rewrites
//! only that directory. The root directory is the default posix dataset's
//! root (see `crate::dataset`).
//!
//! Object numbers are pool-wide and never reused.

use crate::disk::{BLOCK_SIZE, BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::object::{Dirty, TreeRoot};
use crate::types::Txg;
use crate::types::{Holding, LeaseRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Dnodes per object table record.
pub const DNODES_PER_LEAF: u64 = 32;
/// Target size of an object table record, in blocks (rounded up to the
/// pool's alignment).
const TABLE_LEAF_BLOCKS: u64 = 8;

/// What an object is. In JSON it's named as `name` gives it (on disk,
/// bincode stores the variant's index).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ObjType {
    File,
    #[serde(rename = "dir")]
    Directory,
    /// Copy-on-write virtual block device (see `crate::volume`).
    Volume,
    /// A volume as it was at some TXG, read-only (see `crate::snapshot`).
    /// No directory links it: its volume's dnode does.
    Snapshot,
    /// The dataset directory (see `crate::dataset`): the uberblock links it.
    Datasets,
    /// A `kv` dataset's B+tree (see `crate::btree`): its records are the
    /// tree's nodes.
    #[serde(rename = "kv")]
    KvTree,
}

impl ObjType {
    pub fn name(&self) -> &'static str {
        match self {
            ObjType::File => "file",
            ObjType::Directory => "dir",
            ObjType::Volume => "volume",
            ObjType::Snapshot => "snapshot",
            ObjType::Datasets => "datasets",
            ObjType::KvTree => "kv",
        }
    }
}

/// A directory entry.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirEntry {
    pub id: FileId,
    pub kind: ObjType,
}

/// A directory's contents.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct FileIndex {
    pub files: BTreeMap<String, DirEntry>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

pub use crate::types::Extent;

/// An object's attributes.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Attrs {
    /// Permission bits.
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    /// Directory entries naming it.
    pub nlink: u32,
    /// Last content change / last change of anything, ns since the epoch.
    pub mtime_ns: u64,
    pub ctime_ns: u64,
}

impl Attrs {
    pub fn new(mode: u16) -> Self {
        let now = now_ns();
        Attrs {
            mode,
            uid: 0,
            gid: 0,
            nlink: 1,
            mtime_ns: now,
            ctime_ns: now,
        }
    }

    /// The contents changed.
    pub fn touch(&mut self) {
        let now = now_ns();
        self.mtime_ns = now;
        self.ctime_ns = now;
    }
}

/// Attributes to change (None: leave as is).
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct SetAttrs {
    pub mode: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub mtime_ns: Option<u64>,
}

pub(crate) fn now_ns() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// An object as the object table stores it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Dnode {
    /// What it is, and what only that kind of object has.
    pub body: Body,
    /// Bytes per data record.
    pub blksz: u32,
    /// Logical size in bytes.
    pub size: u64,
    pub tree: TreeRoot,
    pub attrs: Attrs,
    /// The directory it is linked from (a posix root's is itself: a
    /// directory's `..`; a tree dataset's root: the dataset directory; a
    /// snapshot's: its volume). None: linked from nowhere (yet).
    pub parent: Option<FileId>,
    /// Bumped by every change to the object (contents, entries,
    /// attributes): NFS clients validate their caches on it. Unlike a
    /// time, it never goes backwards, whichever node's clock is involved.
    pub change: u64,
}

/// What an object is (see [`ObjType`]), with what only its kind has.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Body {
    File,
    Directory,
    Datasets,
    Volume(Head),
    KvTree(Head),
    /// A snapshot of the volume that is its `parent`.
    Snapshot(Snap),
}

/// A tree dataset's root object (a volume, a kv tree): what it holds
/// besides its tree.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Head {
    /// Its placement settings.
    pub props: DatasetProps,
    /// Its intent log, if a writer runs it (see `crate::zil`): writes
    /// acknowledged since the tree was committed.
    pub log: Option<DatasetLog>,
    /// The snapshot it was cloned from (see `crate::snapshot`): blocks
    /// born at or before it are the origin's.
    pub origin: Option<SnapRef>,
    /// Its snapshots, oldest first.
    pub snapshots: Vec<SnapRef>,
}

/// A snapshot's place among its volume's (see `crate::snapshot`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Snap {
    pub txg: Txg,
    /// See `SnapRef::guid`.
    pub guid: u64,
    /// Volumes cloned from it (it can't be destroyed while they exist).
    pub clones: Vec<FileId>,
}

/// A snapshot, as its volume (or a clone made from it) refers to it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SnapRef {
    /// `volume@snapshot`.
    pub name: String,
    pub id: FileId,
    /// The TXG it was taken in: every block it references was born at or
    /// before it, and every block born after it is someone else's.
    pub txg: Txg,
    /// Names these contents in any pool: a received snapshot keeps the
    /// sender's (see `crate::send`).
    pub guid: u64,
}

impl Body {
    pub fn kind(&self) -> ObjType {
        match self {
            Body::File => ObjType::File,
            Body::Directory => ObjType::Directory,
            Body::Datasets => ObjType::Datasets,
            Body::Volume(_) => ObjType::Volume,
            Body::KvTree(_) => ObjType::KvTree,
            Body::Snapshot(_) => ObjType::Snapshot,
        }
    }

    /// A tree dataset root of `kind` (Volume or KvTree) holding `head`.
    pub fn tree_root(kind: ObjType, head: Head) -> Self {
        match kind {
            ObjType::Volume => Body::Volume(head),
            ObjType::KvTree => Body::KvTree(head),
            k => panic!("a {} is no tree dataset root", k.name()),
        }
    }
}

impl Dnode {
    fn new(body: Body, blksz: u32, mode: u16) -> Self {
        Dnode {
            body,
            blksz,
            size: 0,
            tree: TreeRoot {
                ptr: None,
                levels: 0,
            },
            attrs: Attrs::new(mode),
            parent: None,
            change: 0,
        }
    }

    pub fn directory(blksz: u32) -> Self {
        Self::new(Body::Directory, blksz, 0o755)
    }

    /// The dataset directory (see `crate::dataset`).
    pub fn datasets(blksz: u32) -> Self {
        Self::new(Body::Datasets, blksz, 0o700)
    }

    pub fn file(blksz: u32) -> Self {
        Self::new(Body::File, blksz, 0o644)
    }

    pub fn volume(volblocksize: u32, size: u64, levels: u8) -> Self {
        let mut d = Self::new(Body::Volume(Head::default()), volblocksize, 0o600);
        d.size = size;
        d.tree.levels = levels;
        d
    }

    /// A `kv` dataset's tree: `capacity` node records of `blksz` bytes,
    /// none allocated (an empty B+tree).
    pub fn kv(blksz: u32, capacity: u64) -> Self {
        let mut d = Self::new(Body::KvTree(Head::default()), blksz, 0o600);
        d.size = capacity * blksz as u64;
        d.tree.levels = crate::object::levels_for(capacity);
        d
    }

    pub fn kind(&self) -> ObjType {
        self.body.kind()
    }

    pub fn is_file(&self) -> bool {
        matches!(self.body, Body::File)
    }

    pub fn is_directory(&self) -> bool {
        matches!(self.body, Body::Directory)
    }

    /// A tree dataset root's settings, log and lineage.
    pub fn head(&self) -> Option<&Head> {
        match &self.body {
            Body::Volume(h) | Body::KvTree(h) => Some(h),
            _ => None,
        }
    }

    pub fn head_mut(&mut self) -> Option<&mut Head> {
        match &mut self.body {
            Body::Volume(h) | Body::KvTree(h) => Some(h),
            _ => None,
        }
    }

    /// A tree dataset root's settings (None: not one).
    pub fn props(&self) -> Option<&DatasetProps> {
        self.head().map(|h| &h.props)
    }

    /// A tree dataset root's intent log, if it has one.
    pub fn log(&self) -> Option<&DatasetLog> {
        self.head().and_then(|h| h.log.as_ref())
    }

    /// A snapshot's place among its volume's.
    pub fn snap(&self) -> Option<&Snap> {
        match &self.body {
            Body::Snapshot(s) => Some(s),
            _ => None,
        }
    }

    pub fn snap_mut(&mut self) -> Option<&mut Snap> {
        match &mut self.body {
            Body::Snapshot(s) => Some(s),
            _ => None,
        }
    }
}

/// A tree dataset's intent log: the regions its writer appends flushed writes
/// to, until a commit folds them into the tree. Registered (and its space
/// claimed) by a commit; replayed by the committer if the writer goes away
/// first.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DatasetLog {
    /// Oldest first (at most two: the one a commit was taken from, and the
    /// one started by that commit).
    pub regions: Vec<LogRegion>,
    /// Records of `regions[0]` before this one are already in the tree.
    pub from_seq: u64,
    /// The writer lease (`writer/...`) whose lent space holds it.
    pub writer: LeaseRef,
    /// The `ds/<name>` lease it was written under: once that's gone, no
    /// more records can be acknowledged, and it may be replayed.
    pub ds: Holding,
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

/// Placement of a tree dataset across the cluster's `listen` nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DatasetProps {
    /// Nodes (by name) that may run the dataset, most preferred first. It
    /// runs on the first live one and stays there (no fail-back) until
    /// moved. Empty: any node.
    pub nodes: Vec<String>,
    /// A volume's: export it and attach the kernel device wherever it runs.
    /// None: only run by hand (`nbd` / `nvme`).
    pub automount: Option<Transport>,
    /// A disabled dataset isn't placed anywhere.
    pub enabled: bool,
    /// Asked to move to this node (`volume move`): it goes there while that
    /// node is live, and the node clears this once it runs the volume.
    pub move_to: Option<String>,
    /// An `etcd` dataset's: the port every node takes its clients on (they
    /// reach it wherever it runs, see `crate::frontend`).
    #[serde(default)]
    pub port: Option<u16>,
}

impl Default for DatasetProps {
    fn default() -> Self {
        DatasetProps {
            nodes: Vec::new(),
            automount: None,
            enabled: true,
            move_to: None,
            port: None,
        }
    }
}

/// An object table record: `DNODES_PER_LEAF` slots, length-prefixed (the
/// record is zero-padded to its block size).
fn encode_leaf(slots: &[Option<Dnode>]) -> Result<Vec<u8>, FileSystemError> {
    let body = bincode::serialize(slots)?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub(crate) fn decode_leaf(buf: &[u8]) -> Result<Vec<Option<Dnode>>, FileSystemError> {
    let bad = || FileSystemError::InvalidArgument("malformed object table record".into());
    let len = u32::from_le_bytes(buf.get(..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
    let body = buf.get(4..4 + len).ok_or_else(bad)?;
    let mut slots: Vec<Option<Dnode>> = bincode::deserialize(body)?;
    slots.resize(DNODES_PER_LEAF as usize, None);
    Ok(slots)
}

/// Decode a directory's contents (`size` bytes of its tree).
pub(crate) fn decode_dir(data: &[u8]) -> Result<FileIndex, FileSystemError> {
    if data.is_empty() {
        return Ok(FileIndex::new());
    }
    Ok(bincode::deserialize(data)?)
}

pub(crate) fn path_components(path: &Path) -> Vec<String> {
    use std::path::Component;
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str().map(String::from),
            _ => None,
        })
        .collect()
}

/// Where a path leads: its parent directory and the entry, if any.
pub(crate) struct Lookup {
    pub parent_id: FileId,
    pub parent: Dnode,
    pub index: FileIndex,
    pub name: String,
    pub entry: Option<DirEntry>,
}

/// Refuse names a directory can't hold.
pub(crate) fn check_name(name: &str) -> Result<(), FileSystemError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.len() > MAX_NAME_BYTES
        || name.contains('/')
        || name.contains('\0')
    {
        return Err(FileSystemError::InvalidArgument(format!(
            "bad name '{}'",
            name
        )));
    }
    Ok(())
}

/// Longest name in a directory, in bytes.
pub const MAX_NAME_BYTES: usize = 255;

impl<D: BlockDevice> FileSystem<D> {
    /// Bytes per object table record.
    pub(crate) fn table_blksz(&self) -> usize {
        (self
            .align_blocks(TABLE_LEAF_BLOCKS)
            .min(self.max_record_blocks()) as usize)
            * BLOCK_SIZE
    }

    /// Bytes per record of files and directories (the largest record).
    pub fn file_blksz(&self) -> u32 {
        (self.max_record_blocks() as usize * BLOCK_SIZE) as u32
    }

    async fn table_leaf(&self, leaf: u64) -> Result<Vec<Option<Dnode>>, FileSystemError> {
        match self.object_record(&self.objects, leaf).await? {
            Some(buf) => decode_leaf(&buf),
            None => Ok(vec![None; DNODES_PER_LEAF as usize]),
        }
    }

    /// Object `id`'s dnode, if it exists (uncommitted changes included).
    pub async fn try_read_dnode(&self, id: FileId) -> Result<Option<Dnode>, FileSystemError> {
        let mut leaf = self.table_leaf(id.get() / DNODES_PER_LEAF).await?;
        Ok(leaf.swap_remove((id.get() % DNODES_PER_LEAF) as usize))
    }

    pub async fn read_dnode(&self, id: FileId) -> Result<Dnode, FileSystemError> {
        self.try_read_dnode(id)
            .await?
            .ok_or(FileSystemError::FileNotFound)
    }

    /// Set (or, with None, remove) object `id`'s dnode in the open TXG.
    /// Only the process that commits the pool changes the table, under its
    /// `tx` (which can undo it).
    pub(crate) async fn write_dnode(
        &self,
        tx: &mut crate::tx::Tx,

        id: FileId,
        dnode: Option<Dnode>,
    ) -> Result<(), FileSystemError> {
        debug_assert!(
            self.writer.is_none(),
            "writers don't change the object table"
        );
        let leaf = id.get() / DNODES_PER_LEAF;
        let mut slots = self.table_leaf(leaf).await?;
        let slot = &mut slots[(id.get() % DNODES_PER_LEAF) as usize];
        let mut dnode = dnode;
        if let Some(d) = &mut dnode {
            // Every rewrite is a change (callers only rewrite to change it).
            d.change = slot.as_ref().map_or(0, |old| old.change) + 1;
        }
        *slot = dnode;
        let prior = self.objects.dirty_record(leaf);
        if slots.iter().all(Option::is_none) {
            self.objects.put_record(leaf, Dirty::Hole);
            tx.journal(crate::tx::Undo::Record { leaf, prior });
            return Ok(());
        }
        let buf = encode_leaf(&slots)?;
        if buf.len() > self.objects.blksz {
            return Err(FileSystemError::InvalidArgument(format!(
                "object {}: its table record would be {} bytes (at most {})",
                id,
                buf.len(),
                self.objects.blksz
            )));
        }
        self.objects.put_record(leaf, Dirty::Data(buf));
        tx.journal(crate::tx::Undo::Record { leaf, prior });
        Ok(())
    }

    /// Every on-disk extent an object occupies (its tree and log regions).
    pub(crate) async fn dnode_extents(
        &self,
        dnode: &Dnode,
    ) -> Result<Vec<Extent>, FileSystemError> {
        let mut out = self.tree_extents(&dnode.tree).await?;
        out.extend(
            dnode
                .log()
                .iter()
                .flat_map(|l| l.regions.iter().map(LogRegion::extent)),
        );
        Ok(out)
    }

    /// Free an object's space and remove it from the table.
    pub(crate) async fn free_dnode(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        dnode: &Dnode,
    ) -> Result<(), FileSystemError> {
        for e in self.dnode_extents(dnode).await? {
            self.free(tx, e.start, e.len).await?;
        }
        self.write_dnode(tx, id, None).await
    }

    /// Free a tree that nothing references any more (in the open TXG).
    pub(crate) async fn free_tree(
        &self,
        tx: &mut crate::tx::Tx,
        tree: &TreeRoot,
    ) -> Result<(), FileSystemError> {
        for e in self.tree_extents(tree).await? {
            self.free(tx, e.start, e.len).await?;
        }
        Ok(())
    }

    /// A directory's contents.
    pub(crate) async fn read_dir(&self, dnode: &Dnode) -> Result<FileIndex, FileSystemError> {
        if !dnode.is_directory() {
            return Err(FileSystemError::NotADirectory);
        }
        let data = self
            .read_tree(&dnode.tree, dnode.blksz as usize, dnode.size)
            .await?;
        decode_dir(&data)
    }

    /// Replace directory `id`'s contents with `index` (in the open TXG;
    /// `dnode` is updated to match).
    pub(crate) async fn write_dir(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        dnode: &mut Dnode,
        index: &FileIndex,
    ) -> Result<(), FileSystemError> {
        let data = if index.files.is_empty() {
            Vec::new()
        } else {
            bincode::serialize(index)?
        };
        self.replace_contents(tx, id, dnode, &data).await
    }

    /// Replace object `id`'s whole contents with `data` (in the open TXG;
    /// `dnode` is updated to match, and the old tree freed).
    pub(crate) async fn replace_contents(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        dnode: &mut Dnode,
        data: &[u8],
    ) -> Result<(), FileSystemError> {
        let tree = {
            // The tx gives them back if the op fails or is dropped.
            let mut allocs = tx.allocs();
            match self
                .write_tree(dnode.blksz as usize, data, &mut allocs.ptrs)
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    // Now: in a commit, nothing is journaled.
                    let ptrs = std::mem::take(&mut allocs.ptrs);
                    self.release_ptrs(&ptrs).await;
                    return Err(e);
                }
            }
        };
        let mut new = dnode.clone();
        new.tree = tree;
        new.size = data.len() as u64;
        new.attrs.touch();
        self.write_dnode(tx, id, Some(new.clone())).await?;
        let old = std::mem::replace(dnode, new);
        self.free_tree(tx, &old.tree).await
    }

    /// The directory at `components` (from the root): its object number,
    /// dnode and contents.
    pub(crate) async fn resolve_dir(
        &self,
        components: &[String],
    ) -> Result<(FileId, Dnode, FileIndex), FileSystemError> {
        let mut id = self.root_dir();
        let mut dnode = self.read_dnode(id).await?;
        let mut index = self.read_dir(&dnode).await?;
        for name in components {
            let entry = *index
                .files
                .get(name)
                .ok_or(FileSystemError::DirectoryNotFound)?;
            if entry.kind != ObjType::Directory {
                return Err(FileSystemError::NotADirectory);
            }
            id = entry.id;
            dnode = self.read_dnode(id).await?;
            index = self.read_dir(&dnode).await?;
        }
        Ok((id, dnode, index))
    }

    /// Find `path`'s parent directory and its entry (if any).
    pub(crate) async fn lookup(&self, path: &Path) -> Result<Lookup, FileSystemError> {
        let components = path_components(path);
        let (name, parents) = components
            .split_last()
            .ok_or(FileSystemError::FileNotFound)?;
        let (parent_id, parent, index) = self.resolve_dir(parents).await?;
        let entry = index.files.get(name).copied();
        Ok(Lookup {
            parent_id,
            parent,
            index,
            name: name.clone(),
            entry,
        })
    }

    /// Entry `name` of directory `dir`.
    pub(crate) async fn lookup_at(
        &self,
        dir: FileId,
        name: &str,
    ) -> Result<Lookup, FileSystemError> {
        let parent = self.read_dnode(dir).await?;
        let index = self.read_dir(&parent).await?;
        let entry = index.files.get(name).copied();
        Ok(Lookup {
            parent_id: dir,
            parent,
            index,
            name: name.to_string(),
            entry,
        })
    }

    /// What `path` names: its object number and dnode.
    pub async fn stat(&self, path: &Path) -> Result<(FileId, Dnode), FileSystemError> {
        let entry = self
            .lookup(path)
            .await?
            .entry
            .ok_or(FileSystemError::FileNotFound)?;
        Ok((entry.id, self.read_dnode(entry.id).await?))
    }

    /// The file at `path`.
    pub async fn stat_file(&self, path: &Path) -> Result<(FileId, Dnode), FileSystemError> {
        let (id, dnode) = self.stat(path).await?;
        if !dnode.is_file() {
            return Err(FileSystemError::NotAFile);
        }
        Ok((id, dnode))
    }

    /// Create a new object at `l` (from `lookup`, under `begin_tx`) and
    /// link it into its parent. Returns its object number.
    pub(crate) async fn link_new(
        &self,
        tx: &mut crate::tx::Tx,
        mut l: Lookup,
        mut dnode: Dnode,
    ) -> Result<FileId, FileSystemError> {
        if l.entry.is_some() {
            return Err(FileSystemError::AlreadyExists);
        }
        check_name(&l.name)?;
        let id = self.alloc_object();
        dnode.parent = Some(l.parent_id);
        let kind = dnode.kind();
        self.write_dnode(tx, id, Some(dnode)).await?;
        l.index.files.insert(l.name.clone(), DirEntry { id, kind });
        self.write_dir(tx, l.parent_id, &mut l.parent, &l.index)
            .await?;
        Ok(id)
    }

    /// Remove `l`'s entry from its parent and free the object (under
    /// `begin_tx`). A directory must be empty.
    pub(crate) async fn unlink(
        &self,
        tx: &mut crate::tx::Tx,
        mut l: Lookup,
    ) -> Result<FileId, FileSystemError> {
        let entry = l.entry.ok_or(FileSystemError::FileNotFound)?;
        let dnode = self.read_dnode(entry.id).await?;
        if dnode.is_directory() && dnode.size > 0 && !self.read_dir(&dnode).await?.files.is_empty()
        {
            return Err(FileSystemError::NotEmpty(format!("directory '{}'", l.name)));
        }
        self.free_dnode(tx, entry.id, &dnode).await?;
        l.index.files.remove(&l.name);
        self.write_dir(tx, l.parent_id, &mut l.parent, &l.index)
            .await?;
        Ok(entry.id)
    }

    /// Move `from` to `to` (under `begin_tx`); see `rename_at_locked`.
    pub(crate) async fn rename_locked(
        &self,
        tx: &mut crate::tx::Tx,
        from: &Path,
        to: &Path,
    ) -> Result<(FileId, Option<FileId>), FileSystemError> {
        let src = self.lookup(from).await?;
        let dst = self.lookup(to).await?;
        self.rename_at_locked(tx, src, dst).await
    }

    /// Move `src`'s entry to `dst` (both from `lookup`, under `begin_tx`).
    /// `dst` may replace a file with a file or an empty directory with a
    /// directory (it is freed). Returns the moved object's number, and the
    /// replaced one's.
    pub(crate) async fn rename_at_locked(
        &self,
        tx: &mut crate::tx::Tx,
        src: Lookup,
        mut dst: Lookup,
    ) -> Result<(FileId, Option<FileId>), FileSystemError> {
        let moved = src.entry.ok_or(FileSystemError::FileNotFound)?;
        if src.parent_id == dst.parent_id && src.name == dst.name {
            return Ok((moved.id, None));
        }
        check_name(&dst.name)?;
        if moved.kind == ObjType::Directory {
            // Not into itself: walk up from the destination.
            let mut at = dst.parent_id;
            for _ in 0..4096 {
                if at == moved.id {
                    return Err(FileSystemError::InvalidArgument(format!(
                        "can't move '{}' inside itself",
                        src.name
                    )));
                }
                if at == self.root_dir() {
                    break;
                }
                match self.read_dnode(at).await?.parent {
                    Some(p) => at = p,
                    None => break,
                }
            }
        }
        let replaced = match dst.entry {
            None => None,
            Some(t) if t.id == moved.id => return Ok((moved.id, None)),
            Some(t) => {
                let target = self.read_dnode(t.id).await?;
                match (moved.kind, t.kind) {
                    (ObjType::File, ObjType::File) => {}
                    (ObjType::Directory, ObjType::Directory) => {
                        if !self.read_dir(&target).await?.files.is_empty() {
                            return Err(FileSystemError::NotEmpty(format!(
                                "directory '{}'",
                                dst.name
                            )));
                        }
                    }
                    (ObjType::File, _) => return Err(FileSystemError::NotAFile),
                    _ => return Err(FileSystemError::NotADirectory),
                }
                self.free_dnode(tx, t.id, &target).await?;
                Some(t.id)
            }
        };
        if src.parent_id == dst.parent_id {
            let mut index = src.index;
            index.files.remove(&src.name);
            index.files.insert(dst.name, moved);
            let mut parent = src.parent;
            self.write_dir(tx, src.parent_id, &mut parent, &index)
                .await?;
        } else {
            let mut from_index = src.index;
            from_index.files.remove(&src.name);
            let mut from_parent = src.parent;
            self.write_dir(tx, src.parent_id, &mut from_parent, &from_index)
                .await?;
            dst.index.files.insert(dst.name.clone(), moved);
            self.write_dir(tx, dst.parent_id, &mut dst.parent, &dst.index)
                .await?;
        }
        let mut d = self.read_dnode(moved.id).await?;
        d.parent = Some(dst.parent_id);
        d.attrs.ctime_ns = now_ns();
        self.write_dnode(tx, moved.id, Some(d)).await?;
        Ok((moved.id, replaced))
    }

    /// Create a file or directory `name` in directory `dir` (under
    /// `begin_tx`) with attributes `attrs` (mode, owner; unset: defaults). An existing one of the same
    /// kind is fine, unless `exclusive`: then only if it was created with
    /// the same `verifier` (a retransmitted exclusive create). Returns its
    /// number and whether it is new.
    pub(crate) async fn create_at_locked(
        &self,
        tx: &mut crate::tx::Tx,
        dir: FileId,
        name: &str,
        kind: ObjType,
        attrs: &SetAttrs,
        exclusive: Option<u64>,
    ) -> Result<(FileId, bool), FileSystemError> {
        let l = self.lookup_at(dir, name).await?;
        if let Some(e) = l.entry {
            let d = self.read_dnode(e.id).await?;
            if d.kind() != kind {
                return Err(FileSystemError::AlreadyExists);
            }
            return match exclusive {
                Some(v) if d.attrs.mtime_ns != v => Err(FileSystemError::AlreadyExists),
                _ => Ok((e.id, false)),
            };
        }
        let mut dnode = match kind {
            ObjType::File => Dnode::file(self.file_blksz()),
            ObjType::Directory => Dnode::directory(self.file_blksz()),
            ObjType::Volume | ObjType::Snapshot | ObjType::Datasets | ObjType::KvTree => {
                return Err(FileSystemError::InvalidArgument(format!(
                    "a directory holds files and directories, not a {}",
                    kind.name()
                )));
            }
        };
        if let Some(m) = attrs.mode {
            dnode.attrs.mode = m & 0o7777;
        }
        dnode.attrs.uid = attrs.uid.unwrap_or(0);
        dnode.attrs.gid = attrs.gid.unwrap_or(0);
        if let Some(t) = attrs.mtime_ns {
            dnode.attrs.mtime_ns = t;
        }
        if let Some(v) = exclusive {
            // Where an exclusive create's verifier is kept (NFS does this
            // too): the caller sets the real times afterwards.
            dnode.attrs.mtime_ns = v;
        }
        Ok((self.link_new(tx, l, dnode).await?, true))
    }

    /// Change `path`'s attributes (under `begin_tx`). Returns its number.
    pub(crate) async fn set_attrs_locked(
        &self,
        tx: &mut crate::tx::Tx,
        path: &Path,
        set: &SetAttrs,
    ) -> Result<FileId, FileSystemError> {
        let (id, _) = self.stat(path).await?;
        self.set_attrs_id_locked(tx, id, set).await?;
        Ok(id)
    }

    /// Change object `id`'s attributes (under `begin_tx`).
    pub(crate) async fn set_attrs_id_locked(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        set: &SetAttrs,
    ) -> Result<(), FileSystemError> {
        let mut d = self.read_dnode(id).await?;
        if let Some(m) = set.mode {
            d.attrs.mode = m & 0o7777;
        }
        if let Some(u) = set.uid {
            d.attrs.uid = u;
        }
        if let Some(g) = set.gid {
            d.attrs.gid = g;
        }
        if let Some(t) = set.mtime_ns {
            d.attrs.mtime_ns = t;
        }
        d.attrs.ctime_ns = now_ns();
        self.write_dnode(tx, id, Some(d)).await
    }

    /// Point file `id` at new contents (under `begin_tx`), freeing the old.
    pub(crate) async fn replace_file_contents(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        mut dnode: Dnode,
        size: u64,
        tree: TreeRoot,
    ) -> Result<(), FileSystemError> {
        let old = std::mem::replace(&mut dnode.tree, tree);
        dnode.size = size;
        dnode.attrs.touch();
        self.write_dnode(tx, id, Some(dnode)).await?;
        self.free_tree(tx, &old).await
    }
}

#[cfg(test)]
#[path = "tests/inode_tests.rs"]
mod tests;
