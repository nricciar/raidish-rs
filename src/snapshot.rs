//! Snapshots and clones of tree datasets (see `crate::dataset`; today the
//! volumes, which the rest of this describes).
//!
//! A snapshot is a volume's tree as it was committed at some TXG, kept
//! read-only as an object of its own (`ObjType::Snapshot`), named
//! `volume@snapshot`. Nothing is copied: the snapshot's dnode points at the
//! same root, so the volume and the snapshot share every block until the
//! volume overwrites one. A clone is a new dataset of the same format whose
//! tree starts as a snapshot's.
//!
//! Which blocks are whose follows from birth TXGs (exact, see
//! `FileSystem::raise_birth_floor`):
//!
//! ```text
//!   origin (clone of)     snapshots, oldest first        the volume
//!        O ─────────────── S1 ──── S2 ──── ... ──── Sn ─── head
//!   a block born at or before Sn.txg may be in a snapshot (or the origin);
//!   one born after it is only the head's
//! ```
//!
//! - When the head replaces a block (a sync, a writer's commit), it frees
//!   it only if it was born after its newest snapshot (or its origin, for a
//!   clone without snapshots): `held_txg`. Older blocks belong to a
//!   snapshot, which keeps them.
//! - Destroying a snapshot S frees the blocks only S references: those born
//!   after the snapshot before it (P; nothing older can be S's alone, since
//!   copy-on-write never brings a block back) that the next one (or the
//!   head) no longer has at the same place. The trees are walked together,
//!   skipping every subtree that's shared or older than P, so it costs what
//!   changed between P and the next, not the volume's size. This is what a
//!   deadlist gives ZFS, without one to keep.
//! - A snapshot clones were made from can't be destroyed; a volume with
//!   snapshots can't be deleted (destroy them first). Deleting a clone frees
//!   what it wrote (born after its origin).
//! - Rolling a volume back to its newest snapshot frees everything born
//!   since and points it at the snapshot's tree.
//!
//! A snapshot is taken by the process that commits the pool. It holds the
//! volume as last committed: on a volume a writer runs, writes acknowledged
//! since its last commit (still only in its intent log) aren't in it, so it
//! is the volume as of a moment shortly before (crash-consistent).

use crate::blkptr::BlockPtr;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Body, Dnode, Head, Snap, SnapRef};
use crate::object::{INDIRECT_FANOUT, ObjTree, Sizing, TreeRoot};
use crate::types::Txg;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A snapshot as listed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// `volume@snapshot`.
    pub name: String,
    pub txg: Txg,
    /// Names these contents in any pool (kept by send/receive).
    pub guid: u64,
    /// When it was taken, ns since the epoch.
    pub created_ns: u64,
    /// Bytes only it references (freed if it's destroyed).
    pub unique_bytes: u64,
    /// Volumes cloned from it.
    pub clones: Vec<String>,
}

impl SnapshotInfo {
    /// The snapshot's own name (after the `@`).
    pub fn short(&self) -> &str {
        self.name.split_once('@').map_or(&self.name, |(_, s)| s)
    }
}

fn check_snap_name(name: &str) -> Result<(), FileSystemError> {
    crate::types::SnapshotName::try_from(name)?;
    Ok(())
}

/// A head's snapshots and origin.
fn head_lineage(d: &Dnode) -> (Option<SnapRef>, Vec<SnapRef>) {
    match d.head() {
        Some(h) => (h.origin.clone(), h.snapshots.clone()),
        None => (None, Vec::new()),
    }
}

fn set_head_lineage(d: &mut Dnode, origin: Option<SnapRef>, snapshots: Vec<SnapRef>) {
    if let Some(h) = d.head_mut() {
        h.origin = origin;
        h.snapshots = snapshots;
    }
}

/// Blocks born at or before this belong to `d`'s snapshots or origin: a
/// head never frees them itself.
pub(crate) fn held_txg(d: &Dnode) -> Txg {
    let (origin, snapshots) = head_lineage(d);
    snapshots
        .last()
        .map(|s| s.txg)
        .or(origin.map(|o| o.txg))
        .unwrap_or(Txg::ZERO)
}

/// The short name of snapshot `r` (`volume@snapshot` → `snapshot`).
fn short(r: &SnapRef) -> &str {
    r.name.split_once('@').map_or(&r.name, |(_, s)| s)
}

impl<D: BlockDevice> FileSystem<D> {
    /// The volume `name` and its dnode (a volume, not a snapshot).
    async fn head(&self, name: &str) -> Result<(FileId, Dnode), FileSystemError> {
        let e = self.dataset_entry(name).await?;
        if !e.format.is_tree() {
            return Err(FileSystemError::NotAFile);
        }
        let d = self.read_dnode(e.root).await?;
        if d.kind() != e.format.root_kind() {
            return Err(FileSystemError::NotAFile);
        }
        Ok((e.root, d))
    }

    /// Blocks born at or before this are object `id`'s snapshots' (or
    /// origin's), if it is a volume that has any.
    pub(crate) async fn held_txg_of(&self, id: FileId) -> Result<Txg, FileSystemError> {
        Ok(held_txg(&self.read_dnode(id).await?))
    }

    /// Free the blocks of `ptrs` a head may free itself (born after `held`).
    pub(crate) async fn free_unheld(
        &self,
        tx: &mut crate::tx::Tx,
        ptrs: Vec<crate::types::Born>,
        held: Txg,
    ) -> Result<(), FileSystemError> {
        for b in ptrs {
            if b.birth > held {
                self.free(tx, b.extent.start, b.extent.len).await?;
            }
        }
        Ok(())
    }

    /// The blocks of `tree` born after `after` that `next` doesn't have at
    /// the same place: what nothing else references once `tree` is gone, if
    /// `next` is the tree after it (None: there is none). Shared subtrees
    /// and subtrees older than `after` aren't read.
    pub(crate) async fn unique_blocks(
        &self,
        tree: &TreeRoot,
        blksz: usize,
        after: Txg,
        next: Option<&TreeRoot>,
    ) -> Result<Vec<BlockPtr>, FileSystemError> {
        // Scratch trees for their indirect caches.
        let mine = ObjTree::new(blksz, Sizing::Fixed, 0, tree.clone());
        let theirs = next.map(|n| ObjTree::new(blksz, Sizing::Fixed, 0, n.clone()));
        let mut out = Vec::new();
        let mut stack: Vec<(BlockPtr, u8, u64)> = tree
            .ptr
            .iter()
            .map(|p| (p.clone(), tree.levels, 0))
            .collect();
        while let Some((ptr, level, index)) = stack.pop() {
            if ptr.birth_txg <= after {
                continue;
            }
            if let (Some(n), Some(t)) = (next, &theirs)
                && self.tree_lookup(t, n, level, index).await?.as_ref() == Some(&ptr)
            {
                continue;
            }
            if level > 0 {
                let ind = self.load_indirect(&mine, &ptr).await?;
                for (slot, c) in ind.iter().enumerate() {
                    if let Some(c) = c {
                        stack.push((c.clone(), level - 1, index * INDIRECT_FANOUT + slot as u64));
                    }
                }
            }
            out.push(ptr);
        }
        Ok(out)
    }

    /// Snapshot volume `name` as `snap` (`name@snap`), as last committed
    /// (everything written here so far is committed first). Returns its
    /// TXG.
    pub async fn create_snapshot(&self, name: &str, snap: &str) -> Result<Txg, FileSystemError> {
        self.create_snapshot_as(name, snap, None, None).await
    }

    /// `create_snapshot`, naming its contents `guid` and dating it
    /// `created_ns` (a received snapshot keeps the sender's; None: new).
    pub(crate) async fn create_snapshot_as(
        &self,
        name: &str,
        snap: &str,
        guid: Option<u64>,
        created_ns: Option<u64>,
    ) -> Result<Txg, FileSystemError> {
        check_snap_name(snap)?;
        if self.writer.is_some() {
            return Err(FileSystemError::InvalidArgument(
                "snapshots are taken by the committer".into(),
            ));
        }
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        // Nothing of the open TXG is in the tree after this, so the
        // snapshot is exactly TXG `txg`, and every block committed from
        // now on (stamped with the open TXG or later) is born after it.
        let txg = self.commit_locked(tx).await?;
        self.raise_birth_floor(txg.next());
        let (id, mut head) = self.head(name).await?;
        let (origin, mut snapshots) = head_lineage(&head);
        if snapshots.iter().any(|s| short(s) == snap) {
            return Err(FileSystemError::AlreadyExists);
        }
        let sid = self.alloc_object();
        let guid = guid.unwrap_or_else(|| crate::fs::new_guid(sid.get() ^ txg.get()));
        let mut s = Dnode::volume(head.blksz, head.size, head.tree.levels);
        s.body = Body::Snapshot(Snap {
            txg,
            guid,
            clones: Vec::new(),
        });
        s.tree = head.tree.clone();
        s.parent = Some(id);
        if let Some(t) = created_ns {
            s.attrs.ctime_ns = t;
            s.attrs.mtime_ns = t;
        }
        self.write_dnode(tx, sid, Some(s)).await?;
        snapshots.push(SnapRef {
            name: format!("{}@{}", name, snap),
            id: sid,
            txg,
            guid,
        });
        set_head_lineage(&mut head, origin, snapshots);
        let r = self.write_dnode(tx, id, Some(head)).await;
        // The raised birth floor stays: higher than needed only restamps.
        tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(txg)
    }

    /// Volume `name`'s snapshots, oldest first.
    pub async fn list_snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, FileSystemError> {
        let (_, head) = self.head(name).await?;
        let (origin, snapshots) = head_lineage(&head);
        let names = self.dataset_names().await?;
        let mut out = Vec::new();
        for (i, r) in snapshots.iter().enumerate() {
            let d = self.read_dnode(r.id).await?;
            let prev = if i == 0 {
                origin.as_ref().map_or(Txg::ZERO, |o| o.txg)
            } else {
                snapshots[i - 1].txg
            };
            let next = match snapshots.get(i + 1) {
                Some(n) => self.read_dnode(n.id).await?.tree,
                None => head.tree.clone(),
            };
            let unique = self
                .unique_blocks(&d.tree, d.blksz as usize, prev, Some(&next))
                .await?;
            let clones = match d.snap() {
                Some(s) => s
                    .clones
                    .iter()
                    .map(|c| names.get(c).cloned().unwrap_or_else(|| format!("#{}", c)))
                    .collect(),
                None => Vec::new(),
            };
            out.push(SnapshotInfo {
                name: r.name.clone(),
                txg: r.txg,
                guid: r.guid,
                created_ns: d.attrs.ctime_ns,
                unique_bytes: unique.iter().map(|p| p.asize as u64).sum::<u64>()
                    * crate::disk::BLOCK_SIZE as u64,
                clones,
            });
        }
        Ok(out)
    }

    /// Destroy snapshot `name@snap`, freeing what only it referenced.
    /// Refused while clones of it exist.
    pub async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), FileSystemError> {
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r: Result<_, FileSystemError> = async {
            let (id, mut head) = self.head(name).await?;
            let (origin, mut snapshots) = head_lineage(&head);
            let i = snapshots
                .iter()
                .position(|s| short(s) == snap)
                .ok_or(FileSystemError::FileNotFound)?;
            let r = snapshots[i].clone();
            let d = self.read_dnode(r.id).await?;
            if let Some(Snap { clones, .. }) = d.snap()
                && !clones.is_empty()
            {
                let names = self.dataset_names().await?;
                let list: Vec<String> = clones
                    .iter()
                    .map(|c| names.get(c).cloned().unwrap_or_else(|| format!("#{}", c)))
                    .collect();
                return Err(FileSystemError::InvalidArgument(format!(
                    "{} has clones ({}); delete them first",
                    r.name,
                    list.join(", ")
                )));
            }
            let prev = if i == 0 {
                origin.as_ref().map_or(Txg::ZERO, |o| o.txg)
            } else {
                snapshots[i - 1].txg
            };
            // The head's tree as the table has it: what the volume has
            // committed (a writer's commit is applied under the same lock, so
            // what it replaces is either still here or already kept for us).
            let next = match snapshots.get(i + 1) {
                Some(n) => self.read_dnode(n.id).await?.tree,
                None => head.tree.clone(),
            };
            let unique = self
                .unique_blocks(&d.tree, d.blksz as usize, prev, Some(&next))
                .await?;
            snapshots.remove(i);
            set_head_lineage(&mut head, origin, snapshots);
            self.write_dnode(tx, id, Some(head)).await?;
            self.write_dnode(tx, r.id, None).await?;
            for p in &unique {
                self.free(tx, p.lba, p.asize as u64).await?;
            }
            Ok(r.id)
        }
        .await;
        let sid = tx.check(r).await?;
        // A cache: dropped once nothing can fail.
        self.snap_trees.lock().unwrap().remove(&sid);
        self.commit_locked(tx).await?;
        Ok(())
    }

    /// Create volume `new` from snapshot `name@snap`: it starts with the
    /// snapshot's contents and shares its blocks.
    pub async fn clone_snapshot(
        &self,
        name: &str,
        snap: &str,
        new: &str,
    ) -> Result<FileId, FileSystemError> {
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r: Result<_, FileSystemError> = async {
            let (_, head) = self.head(name).await?;
            let (_, snapshots) = head_lineage(&head);
            let r = snapshots
                .iter()
                .find(|s| short(s) == snap)
                .cloned()
                .ok_or(FileSystemError::FileNotFound)?;
            let mut s = self.read_dnode(r.id).await?;
            let format = self.dataset_entry(name).await?.format;
            let mut c = head.clone();
            c.body = Body::tree_root(
                format.root_kind(),
                Head {
                    props: crate::inode::DatasetProps {
                        // A new volume: placed and started only when asked to.
                        enabled: false,
                        move_to: None,
                        ..head.props().cloned().unwrap_or_default()
                    },
                    log: None,
                    origin: Some(r.clone()),
                    snapshots: Vec::new(),
                },
            );
            c.blksz = s.blksz;
            c.size = s.size;
            c.tree = s.tree.clone();
            c.attrs = crate::inode::Attrs::new(head.attrs.mode);
            let cid = self.create_dataset_locked(tx, new, format, c).await?;
            if let Some(s) = s.snap_mut() {
                s.clones.push(cid);
            }
            self.write_dnode(tx, r.id, Some(s)).await?;
            Ok(cid)
        }
        .await;
        let cid = tx.check(r).await?;
        self.commit_locked(tx).await?;
        Ok(cid)
    }

    /// Roll volume `name` back to its newest snapshot, discarding every
    /// write since (unsynced ones too). The volume must not be running.
    pub async fn rollback_volume(&self, name: &str) -> Result<(), FileSystemError> {
        let mut tx = self.begin_tx().await;
        let tx = &mut tx;
        let r: Result<_, FileSystemError> = async {
            let (id, mut head) = self.head(name).await?;
            let (_, snapshots) = head_lineage(&head);
            let Some(r) = snapshots.last().cloned() else {
                return Err(FileSystemError::InvalidArgument(format!(
                    "volume '{}' has no snapshots",
                    name
                )));
            };
            if head.log().is_some() {
                return Err(FileSystemError::InvalidArgument(format!(
                    "volume '{}' has an intent log (it's running, or its writer's log \
                     is yet to be replayed)",
                    name
                )));
            }
            let s = self.read_dnode(r.id).await?;
            let gone = self
                .unique_blocks(&head.tree, head.blksz as usize, r.txg, None)
                .await?;
            head.tree = s.tree.clone();
            head.attrs.touch();
            self.write_dnode(tx, id, Some(head)).await?;
            for p in &gone {
                self.free(tx, p.lba, p.asize as u64).await?;
            }
            Ok(())
        }
        .await;
        tx.check(r).await?;
        // Its dirty records go with the cached volume: only once nothing
        // can fail, as that can't be undone.
        self.close_dataset(name);
        self.commit_locked(tx).await?;
        Ok(())
    }

    /// Read snapshot `name@snap` at byte `offset`.
    pub async fn snapshot_read(
        &self,
        name: &str,
        snap: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FileSystemError> {
        let obj = self.open_snapshot(name, snap).await?;
        self.object_read(&obj, offset, buf).await
    }

    /// Snapshot `name@snap`'s size in bytes.
    pub async fn snapshot_size(&self, name: &str, snap: &str) -> Result<u64, FileSystemError> {
        Ok(self.open_snapshot(name, snap).await?.size())
    }

    async fn open_snapshot(&self, name: &str, snap: &str) -> Result<Arc<ObjTree>, FileSystemError> {
        let (_, head) = self.head(name).await?;
        let (_, snapshots) = head_lineage(&head);
        let r = snapshots
            .iter()
            .find(|s| short(s) == snap)
            .ok_or(FileSystemError::FileNotFound)?;
        if let Some(t) = self.snap_trees.lock().unwrap().get(&r.id) {
            return Ok(t.clone());
        }
        let d = self.read_dnode(r.id).await?;
        let t = Arc::new(ObjTree::new(
            d.blksz as usize,
            Sizing::Fixed,
            d.size,
            d.tree,
        ));
        self.snap_trees.lock().unwrap().insert(r.id, t.clone());
        Ok(t)
    }

    /// Before volume `d` (object `id`) is deleted: refuse if it has
    /// snapshots; for a clone, take it off its origin's list. Returns the
    /// TXG at or before which its blocks aren't its own to free.
    pub(crate) async fn unlink_dataset_lineage(
        &self,
        tx: &mut crate::tx::Tx,
        id: FileId,
        d: &Dnode,
    ) -> Result<Txg, FileSystemError> {
        let (origin, snapshots) = head_lineage(d);
        if !snapshots.is_empty() {
            return Err(FileSystemError::InvalidArgument(format!(
                "the volume has snapshots ({}); destroy them first",
                snapshots
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let Some(o) = origin else {
            return Ok(Txg::ZERO);
        };
        if let Ok(mut s) = self.read_dnode(o.id).await {
            if let Some(s) = s.snap_mut() {
                s.clones.retain(|&c| c != id);
            }
            self.write_dnode(tx, o.id, Some(s)).await?;
        }
        Ok(o.txg)
    }
}

#[cfg(test)]
#[path = "tests/snapshot_tests.rs"]
mod tests;
