//! Consistency checking and recovery.
//!
//! Everything here is built on one fault-tolerant walk of the tree
//! (`walk_tree`): root index → directories → inodes → file data and volume
//! trees. Unlike a normal read, the walk never stops at a damaged record; it
//! notes the error and carries on with the rest of the tree.
//!
//! * `fsck` compares the blocks the tree references with the space maps:
//!   *leaks* (allocated, referenced by nothing) and *referenced-but-free*
//!   blocks (live data the allocator could hand out again), plus
//!   *cross-links* (two pointers to the same blocks). `--repair` frees leaks
//!   and re-allocates live space.
//! * `rebuild_spacemaps` throws the space maps away and derives them from the
//!   tree (used after a rewind, or when a space map is lost).
//! * `salvage` copies out whatever is still readable, zero-filling the rest.

use crate::blkptr::BlockPtr;
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Extent, FileIndex, FileInode, InodeType};
use crate::metaslab::TXG_DEFER_SIZE;
use crate::volume::INDIRECT_FANOUT;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;

/// What a record of the tree holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjKind {
    RootIndex,
    DirIndex,
    Inode,
    FileData,
    /// A volume tree node; level 0 is a data record.
    Volume(u8),
}

/// Callbacks of `walk_tree`. All have empty defaults.
pub trait Visitor {
    /// Every reachable record, whether or not it can be read.
    fn record(&mut self, _ptr: &BlockPtr, _kind: ObjKind) {}
    fn dir(&mut self, _path: &str) {}
    fn file(&mut self, _path: &str, _size_bytes: u64, _blocks: &[BlockPtr]) {}
    fn volume(&mut self, _path: &str, _inode: &InodeType) {}
}

/// A visitor that only wants the walk's own report.
pub struct NoVisitor;
impl Visitor for NoVisitor {}

#[derive(Debug, Default, Clone)]
pub struct WalkReport {
    /// Records reached (read or not).
    pub records: u64,
    /// Unreadable or undecodable records, with their location.
    pub errors: Vec<String>,
    /// Some metadata (a directory, inode or volume indirect block) could not
    /// be read, so part of the tree was never reached: whatever it references
    /// is unknown.
    pub incomplete: bool,
}

/// A pending node of a tree walk (shared with scrub, see `scan`).
pub(crate) enum Node {
    Object {
        ptrs: Vec<BlockPtr>,
        kind: ObjKind,
        path: String,
    },
    Volume {
        ptr: BlockPtr,
        level: u8,
        path: String,
    },
}

/// The children of a decoded directory index or inode (none for file
/// data), reporting directories, files and volumes to `v`. This is the one
/// place that knows how objects reference each other.
pub(crate) fn expand<V: Visitor>(
    kind: ObjKind,
    path: &str,
    data: &[u8],
    v: &mut V,
) -> Result<Vec<Node>, String> {
    match kind {
        ObjKind::RootIndex | ObjKind::DirIndex => {
            let index = bincode::deserialize::<FileIndex>(data)
                .map_err(|e| format!("{}: directory: {}", path, e))?;
            v.dir(path);
            Ok(index
                .files
                .into_iter()
                .rev()
                .map(|(name, r)| Node::Object {
                    ptrs: r.inode,
                    kind: ObjKind::Inode,
                    path: join(path, &name),
                })
                .collect())
        }
        ObjKind::Inode => {
            let inode = bincode::deserialize::<FileInode>(data)
                .map_err(|e| format!("{}: inode: {}", path, e))?;
            Ok(match &inode.inode_type {
                InodeType::File { size_bytes, blocks } => {
                    v.file(path, *size_bytes, blocks);
                    vec![Node::Object {
                        ptrs: blocks.clone(),
                        kind: ObjKind::FileData,
                        path: path.to_string(),
                    }]
                }
                InodeType::Directory { blocks } => vec![Node::Object {
                    ptrs: blocks.clone(),
                    kind: ObjKind::DirIndex,
                    path: path.to_string(),
                }],
                InodeType::Volume { root, levels, .. } => {
                    v.volume(path, &inode.inode_type);
                    root.iter()
                        .map(|ptr| Node::Volume {
                            ptr: ptr.clone(),
                            level: *levels,
                            path: path.to_string(),
                        })
                        .collect()
                }
            })
        }
        ObjKind::FileData | ObjKind::Volume(_) => Ok(Vec::new()),
    }
}

/// The children of a volume indirect block at `level` (> 0).
pub(crate) fn expand_indirect(level: u8, path: &str, data: &[u8]) -> Result<Vec<Node>, String> {
    let ind = bincode::deserialize::<Vec<Option<BlockPtr>>>(data)
        .map_err(|e| format!("{} (volume L{}): {}", path, level, e))?;
    Ok(ind
        .into_iter()
        .flatten()
        .map(|c| Node::Volume {
            ptr: c,
            level: level - 1,
            path: path.to_string(),
        })
        .collect())
}

fn join(path: &str, name: &str) -> String {
    if path.ends_with('/') {
        format!("{}{}", path, name)
    } else {
        format!("{}/{}", path, name)
    }
}

// ----- interval maps (start -> len, sorted, non-overlapping, merged) -----

/// Insert a range, merging with neighbours. Returns the part of it that
/// overlapped what was already there (empty when disjoint).
fn insert_range(map: &mut BTreeMap<u64, u64>, start: u64, len: u64) -> Vec<(u64, u64)> {
    let end = start + len;
    let mut overlap = Vec::new();
    let mut new_start = start;
    let mut new_end = end;
    if let Some((&ps, &pl)) = map.range(..=start).next_back()
        && ps + pl >= start
    {
        if ps + pl > start {
            overlap.push((start, (ps + pl).min(end) - start));
        }
        new_start = ps;
        new_end = new_end.max(ps + pl);
        map.remove(&ps);
    }
    while let Some((&ns, &nl)) = map.range(new_start..).next() {
        if ns > new_end {
            break;
        }
        if ns < end {
            overlap.push((ns, (ns + nl).min(end) - ns));
        }
        new_end = new_end.max(ns + nl);
        map.remove(&ns);
    }
    if len > 0 {
        map.insert(new_start, new_end - new_start);
    }
    overlap
}

/// The part of `map` within `[start, end)`.
fn clip(map: &BTreeMap<u64, u64>, start: u64, end: u64) -> BTreeMap<u64, u64> {
    let mut out = BTreeMap::new();
    let first = map
        .range(..=start)
        .next_back()
        .map(|(&s, _)| s)
        .unwrap_or(start);
    for (&s, &l) in map.range(first..end) {
        let (a, b) = (s.max(start), (s + l).min(end));
        if a < b {
            out.insert(a, b - a);
        }
    }
    out
}

/// `[start, end)` minus `map`.
fn complement(map: &BTreeMap<u64, u64>, start: u64, end: u64) -> BTreeMap<u64, u64> {
    let mut out = BTreeMap::new();
    let mut cursor = start;
    for (&s, &l) in &clip(map, start, end) {
        if cursor < s {
            out.insert(cursor, s - cursor);
        }
        cursor = s + l;
    }
    if cursor < end {
        out.insert(cursor, end - cursor);
    }
    out
}

/// Ranges in both `a` and `b`.
fn intersect(a: &BTreeMap<u64, u64>, b: &BTreeMap<u64, u64>) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for (&s, &l) in a {
        for (&bs, &bl) in &clip(b, s, s + l) {
            out.push((bs, bl));
        }
    }
    out
}

fn total(ranges: &[(u64, u64)]) -> u64 {
    ranges.iter().map(|(_, l)| l).sum()
}

/// Collects every extent the tree references.
#[derive(Default)]
pub struct LiveSet {
    pub live: BTreeMap<u64, u64>,
    /// Blocks referenced more than once.
    pub cross_links: Vec<(u64, u64)>,
}

impl Visitor for LiveSet {
    fn record(&mut self, ptr: &BlockPtr, _kind: ObjKind) {
        let overlap = insert_range(&mut self.live, ptr.lba, ptr.asize as u64);
        self.cross_links.extend(overlap);
    }
}

#[derive(Debug, Default, Clone)]
pub struct FsckReport {
    pub txg: u64,
    pub repair: bool,
    pub records: u64,
    pub live_blocks: u64,
    /// Allocated but referenced by nothing.
    pub leaks: Vec<(u64, u64)>,
    /// Referenced, but free (or pending free) in the space maps.
    pub referenced_free: Vec<(u64, u64)>,
    /// Blocks referenced by more than one pointer.
    pub cross_links: Vec<(u64, u64)>,
    /// References outside the data area.
    pub out_of_range: Vec<(u64, u64)>,
    pub errors: Vec<String>,
    pub incomplete: bool,
    pub leaks_repaired: bool,
    pub referenced_free_repaired: bool,
}

impl FsckReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
            && self.cross_links.is_empty()
            && self.out_of_range.is_empty()
            && (self.leaks.is_empty() || self.leaks_repaired)
            && (self.referenced_free.is_empty() || self.referenced_free_repaired)
    }
}

impl std::fmt::Display for FsckReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "fsck at TXG {}: {} records, {} blocks in use",
            self.txg, self.records, self.live_blocks
        )?;
        let show = |f: &mut std::fmt::Formatter<'_>, what: &str, r: &[(u64, u64)], fixed: &str| {
            if r.is_empty() {
                return Ok(());
            }
            let sample: Vec<String> = r.iter().take(8).map(|(s, l)| format!("{s}+{l}")).collect();
            writeln!(
                f,
                "  {}: {} blocks in {} extents{} [{}{}]",
                what,
                total(r),
                r.len(),
                fixed,
                sample.join(", "),
                if r.len() > 8 { ", ..." } else { "" }
            )
        };
        let fixed = |done: bool| if done { " (repaired)" } else { "" };
        show(f, "leaked", &self.leaks, fixed(self.leaks_repaired))?;
        show(
            f,
            "referenced but free",
            &self.referenced_free,
            fixed(self.referenced_free_repaired),
        )?;
        show(f, "cross-linked", &self.cross_links, "")?;
        show(f, "outside the data area", &self.out_of_range, "")?;
        if self.repair && self.incomplete && !self.leaks.is_empty() {
            writeln!(f, "  leaks not repaired: tree incomplete")?;
        }
        for e in self.errors.iter().take(20) {
            writeln!(f, "  error: {}", e)?;
        }
        if self.errors.len() > 20 {
            writeln!(f, "  ... and {} more errors", self.errors.len() - 20)?;
        }
        if self.is_clean() {
            write!(f, "  clean")?;
        } else if !self.repair {
            write!(f, "  (check only: nothing repaired)")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct RebuildReport {
    pub live_blocks: u64,
    /// Blocks held back for older uberblocks (deferred, not yet reusable).
    pub deferred_blocks: u64,
    /// Blocks the old maps had allocated that are now free.
    pub reclaimed: u64,
    /// Blocks the old maps had released that are live.
    pub claimed: u64,
    pub errors: Vec<String>,
}

impl std::fmt::Display for RebuildReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "space maps rebuilt: {} blocks live, {} held for older uberblocks, {} reclaimed, {} re-allocated",
            self.live_blocks, self.deferred_blocks, self.reclaimed, self.claimed
        )?;
        for e in self.errors.iter().take(20) {
            write!(f, "\n  error: {}", e)?;
        }
        Ok(())
    }
}

/// One salvaged file or volume.
#[derive(Debug, Clone)]
pub struct Salvaged {
    pub path: String,
    pub bytes: u64,
    /// (offset, length) ranges that could not be read and were zero-filled.
    pub lost: Vec<(u64, u64)>,
}

#[derive(Debug, Default, Clone)]
pub struct SalvageReport {
    pub items: Vec<Salvaged>,
    /// Directories / inodes that could not be read (their contents are lost).
    pub errors: Vec<String>,
}

impl SalvageReport {
    pub fn is_complete(&self) -> bool {
        self.errors.is_empty() && self.items.iter().all(|i| i.lost.is_empty())
    }
}

impl std::fmt::Display for SalvageReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in &self.items {
            if i.lost.is_empty() {
                writeln!(f, "  ok      {} ({} bytes)", i.path, i.bytes)?;
            } else {
                let lost: u64 = i.lost.iter().map(|(_, l)| l).sum();
                let at: Vec<String> = i.lost.iter().take(4).map(|(o, _)| o.to_string()).collect();
                writeln!(
                    f,
                    "  DAMAGED {} ({} bytes, {} lost at offsets {}{})",
                    i.path,
                    i.bytes,
                    lost,
                    at.join(", "),
                    if i.lost.len() > 4 { ", ..." } else { "" }
                )?;
            }
        }
        for e in &self.errors {
            writeln!(f, "  error: {}", e)?;
        }
        let damaged = self.items.iter().filter(|i| !i.lost.is_empty()).count();
        write!(
            f,
            "salvaged {} items, {} damaged, {} unreadable directories/inodes",
            self.items.len(),
            damaged,
            self.errors.len()
        )
    }
}

#[derive(Default)]
struct Collect {
    dirs: Vec<String>,
    files: Vec<(String, u64, Vec<BlockPtr>)>,
    volumes: Vec<(String, InodeType)>,
}

impl Visitor for Collect {
    fn dir(&mut self, path: &str) {
        self.dirs.push(path.to_string());
    }
    fn file(&mut self, path: &str, size_bytes: u64, blocks: &[BlockPtr]) {
        self.files
            .push((path.to_string(), size_bytes, blocks.to_vec()));
    }
    fn volume(&mut self, path: &str, inode: &InodeType) {
        self.volumes.push((path.to_string(), inode.clone()));
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Walk every record reachable from `root`, reporting each to `v`.
    /// File data and volume data records are only read when `verify_data`.
    pub(crate) async fn walk_tree<V: Visitor>(
        &self,
        root: &[BlockPtr],
        verify_data: bool,
        v: &mut V,
    ) -> WalkReport {
        let mut report = WalkReport::default();
        let mut stack = vec![Node::Object {
            ptrs: root.to_vec(),
            kind: ObjKind::RootIndex,
            path: "/".to_string(),
        }];
        while let Some(node) = stack.pop() {
            match node {
                Node::Object { ptrs, kind, path } => {
                    for p in &ptrs {
                        report.records += 1;
                        v.record(p, kind);
                    }
                    if kind == ObjKind::FileData {
                        if verify_data {
                            for p in &ptrs {
                                if let Err(e) = self.read_record(p).await {
                                    report
                                        .errors
                                        .push(format!("{}: data record at {}: {}", path, p.lba, e));
                                }
                            }
                        }
                        continue;
                    }
                    if ptrs.is_empty() {
                        continue;
                    }
                    let data = match self.read_object(&ptrs).await {
                        Ok(d) => d,
                        Err(e) => {
                            report.errors.push(format!("{} ({:?}): {}", path, kind, e));
                            report.incomplete = true;
                            continue;
                        }
                    };
                    match expand(kind, &path, &data, v) {
                        Ok(children) => stack.extend(children),
                        Err(e) => {
                            report.errors.push(e);
                            report.incomplete = true;
                        }
                    }
                }
                Node::Volume { ptr, level, path } => {
                    report.records += 1;
                    v.record(&ptr, ObjKind::Volume(level));
                    if level == 0 && !verify_data {
                        continue;
                    }
                    let data = match self.read_record(&ptr).await {
                        Ok(d) => d,
                        Err(e) => {
                            report.errors.push(format!(
                                "{} (volume L{}): record at {}: {}",
                                path, level, ptr.lba, e
                            ));
                            report.incomplete |= level > 0;
                            continue;
                        }
                    };
                    if level > 0 {
                        match expand_indirect(level, &path, &data) {
                            Ok(children) => stack.extend(children),
                            Err(e) => {
                                report.errors.push(e);
                                report.incomplete = true;
                            }
                        }
                    }
                }
            }
        }
        report
    }

    /// Read every record of the tree under `root`, data included.
    pub(crate) async fn verify_tree(&self, root: &[BlockPtr]) -> WalkReport {
        self.walk_tree(root, true, &mut NoVisitor).await
    }

    /// Blocks referenced from the committed root (and the walk's report).
    async fn live_set(&self) -> (LiveSet, WalkReport) {
        let root = self.root_ptr.lock().await.clone();
        let mut live = LiveSet::default();
        let walk = self.walk_tree(&root, false, &mut live).await;
        (live, walk)
    }

    /// Blocks referenced by `roots` (older trees), ignoring damage.
    async fn protected_set(&self, roots: &[Vec<BlockPtr>]) -> BTreeMap<u64, u64> {
        let mut set = LiveSet::default();
        for root in roots {
            self.walk_tree(root, false, &mut set).await;
        }
        set.live
    }

    /// Roots of the valid ring uberblocks older than the committed one.
    async fn older_roots(&self) -> Vec<Vec<BlockPtr>> {
        let txg = self.uberblock().await.map_or(0, |u| u.txg);
        self.uberblock_ring()
            .await
            .into_iter()
            .filter(|u| u.txg < txg && u.txg + TXG_DEFER_SIZE >= txg)
            .map(|u| u.root)
            .collect()
    }

    /// Compare the tree with the space maps: (leaks, referenced-but-free),
    /// both per metaslab, plus references outside every metaslab.
    async fn diff_spacemaps(
        &self,
        live: &BTreeMap<u64, u64>,
    ) -> (Vec<(u64, u64)>, Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let (mut leaks, mut referenced_free) = (Vec::new(), Vec::new());
        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            let (start, end) = (ms.start_block, ms.start_block + ms.block_count);
            let released = ms.released_map();
            let allocated = complement(&released, start, end);
            let live_here = clip(live, start, end);
            // Allocated minus live.
            for (&s, &l) in &allocated {
                leaks.extend(complement(&live_here, s, s + l));
            }
            referenced_free.extend(intersect(&live_here, &released));
        }
        let data_start = self.layout.data_start;
        let data_end = data_start + self.layout.metaslab_count * self.layout.metaslab_size_blocks;
        let mut out_of_range: Vec<(u64, u64)> = clip(live, 0, data_start).into_iter().collect();
        out_of_range.extend(clip(live, data_end, u64::MAX));
        (leaks, referenced_free, out_of_range)
    }

    /// Check that the space maps agree with the tree; with `repair`, free
    /// leaked space and re-allocate live space the maps think is free. Holds
    /// the commit lock throughout (writers wait), so the comparison is exact.
    pub async fn fsck(&self, repair: bool) -> Result<FsckReport, FileSystemError> {
        let repair = repair && !self.read_only;
        let _tx = self.begin_tx().await;
        if !self.read_only {
            self.commit_locked().await?;
        }
        let (live, walk) = self.live_set().await;
        let (leaks, referenced_free, out_of_range) = self.diff_spacemaps(&live.live).await;
        let mut report = FsckReport {
            txg: self.uberblock().await.map_or(0, |u| u.txg),
            repair,
            records: walk.records,
            live_blocks: live.live.values().sum(),
            leaks,
            referenced_free,
            cross_links: live.cross_links,
            out_of_range,
            errors: walk.errors,
            incomplete: walk.incomplete,
            ..Default::default()
        };
        if !repair {
            return Ok(report);
        }

        let mut changed = false;
        if !report.referenced_free.is_empty() {
            for &(s, l) in &report.referenced_free {
                if let Some(idx) = self.layout.metaslab_for_block(s) {
                    self.metaslabs[idx].write().await.mark_allocated(s, l);
                }
            }
            report.referenced_free_repaired = true;
            changed = true;
        }
        if !report.leaks.is_empty() && !report.incomplete {
            for &(s, l) in &report.leaks {
                self.free(s, l).await?;
            }
            report.leaks_repaired = true;
            changed = true;
        }
        if changed {
            report.txg = self.commit_locked().await?;
        }
        Ok(report)
    }

    /// Replace the space maps with ones derived from the tree: everything
    /// the committed tree references is allocated, what the older ring
    /// uberblocks reference is deferred, and the rest is free. Refuses when
    /// part of the tree is unreadable (that would free live data), unless
    /// `force`.
    pub async fn rebuild_spacemaps(&self, force: bool) -> Result<RebuildReport, FileSystemError> {
        if self.read_only {
            return Err(FileSystemError::InvalidArgument(
                "pool opened read-only".into(),
            ));
        }
        let _tx = self.begin_tx().await;
        self.commit_locked().await?;
        let older = self.older_roots().await;
        let report = self.rebuild_spacemaps_locked(&older, force).await?;
        self.commit_locked().await?;
        Ok(report)
    }

    /// `rebuild_spacemaps` under the commit lock, without committing.
    /// `older` are the trees to hold freed space back for.
    pub(crate) async fn rebuild_spacemaps_locked(
        &self,
        older: &[Vec<BlockPtr>],
        force: bool,
    ) -> Result<RebuildReport, FileSystemError> {
        let (live, walk) = self.live_set().await;
        if walk.incomplete && !force {
            return Err(FileSystemError::InvalidArgument(format!(
                "tree incomplete ({} errors, e.g. {}); rebuilding would free live data",
                walk.errors.len(),
                walk.errors.first().map_or("", |s| s.as_str())
            )));
        }
        let protected = self.protected_set(older).await;
        let release_txg = self.uberblock().await.map_or(0, |u| u.txg) + TXG_DEFER_SIZE;
        let mut report = RebuildReport {
            live_blocks: live.live.values().sum(),
            errors: walk.errors,
            ..Default::default()
        };

        for ms in self.metaslabs.iter() {
            let mut ms = ms.write().await;
            let (start, end) = (ms.start_block, ms.start_block + ms.block_count);
            let live_here = clip(&live.live, start, end);
            let mut held = clip(&protected, start, end);
            for (&s, &l) in &live_here {
                crate::metaslab::MetaslabState::remove_range(&mut held, s, l);
            }
            let mut in_use = live_here.clone();
            for (&s, &l) in &held {
                insert_range(&mut in_use, s, l);
            }
            let free = complement(&in_use, start, end);

            let old_released = ms.released_map();
            let old_allocated = complement(&old_released, start, end);
            report.reclaimed += total(&intersect(
                &old_allocated,
                &complement(&live_here, start, end),
            ));
            report.claimed += total(&intersect(&live_here, &old_released));
            report.deferred_blocks += held.values().sum::<u64>();

            ms.free_extents = free;
            ms.freeing.clear();
            ms.deferred = VecDeque::new();
            if !held.is_empty() {
                ms.deferred.push_back((
                    release_txg,
                    held.into_iter()
                        .map(|(start, len)| Extent { start, len })
                        .collect(),
                ));
            }
            ms.dirty = true;
        }
        Ok(report)
    }

    /// Find all blocks that are allocated but not referenced by any live data
    pub async fn find_orphaned_blocks(&self) -> Result<Vec<(u64, u64)>, FileSystemError> {
        let (live, walk) = self.live_set().await;
        if walk.incomplete {
            return Err(FileSystemError::ChecksumMismatch);
        }
        Ok(self.diff_spacemaps(&live.live).await.0)
    }

    /// Copy everything still readable under `dest`: files as files, volumes
    /// as sparse `<name>.img` images. Unreadable records are zero-filled and
    /// reported; unreadable directories are reported and skipped.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn salvage(&self, dest: &Path) -> Result<SalvageReport, FileSystemError> {
        use std::os::unix::fs::FileExt;
        let io = |e: std::io::Error| FileSystemError::DiskError(e.into());
        let root = self.root_ptr.lock().await.clone();
        let mut found = Collect::default();
        let walk = self.walk_tree(&root, false, &mut found).await;
        let mut report = SalvageReport {
            errors: walk.errors,
            ..Default::default()
        };
        let local = |path: &str| dest.join(path.trim_start_matches('/'));

        std::fs::create_dir_all(dest).map_err(io)?;
        for d in &found.dirs {
            std::fs::create_dir_all(local(d)).map_err(io)?;
        }

        for (path, size, blocks) in found.files {
            let out = std::fs::File::create(local(&path)).map_err(io)?;
            let mut item = Salvaged {
                path,
                bytes: size,
                lost: Vec::new(),
            };
            let mut offset = 0u64;
            for p in &blocks {
                match self.read_record(p).await {
                    Ok(data) => out.write_all_at(&data, offset).map_err(io)?,
                    Err(_) => item.lost.push((offset, p.lsize as u64)),
                }
                offset += p.lsize as u64;
            }
            out.set_len(size).map_err(io)?;
            report.items.push(item);
        }

        for (path, inode) in found.volumes {
            let InodeType::Volume {
                size_bytes,
                volblocksize,
                levels,
                root,
                ..
            } = inode
            else {
                continue;
            };
            let vbs = volblocksize as u64;
            let out = std::fs::File::create(local(&format!("{}.img", path))).map_err(io)?;
            out.set_len(size_bytes).map_err(io)?;
            let mut item = Salvaged {
                path: format!("{}.img", path),
                bytes: size_bytes,
                lost: Vec::new(),
            };
            let mut stack: Vec<(BlockPtr, u8, u64)> =
                root.into_iter().map(|r| (r, levels, 0)).collect();
            while let Some((ptr, level, base)) = stack.pop() {
                let span = INDIRECT_FANOUT.pow(level as u32);
                let offset = base * vbs;
                let data = match self.read_record(&ptr).await {
                    Ok(d) => d,
                    Err(_) => {
                        let len = (span * vbs).min(size_bytes.saturating_sub(offset));
                        item.lost.push((offset, len));
                        continue;
                    }
                };
                if level == 0 {
                    let n = (data.len() as u64).min(size_bytes.saturating_sub(offset));
                    out.write_all_at(&data[..n as usize], offset).map_err(io)?;
                    continue;
                }
                match bincode::deserialize::<Vec<Option<BlockPtr>>>(&data) {
                    Ok(ind) => {
                        let child_span = span / INDIRECT_FANOUT;
                        for (slot, c) in ind.into_iter().enumerate() {
                            if let Some(c) = c {
                                stack.push((c, level - 1, base + slot as u64 * child_span));
                            }
                        }
                    }
                    Err(_) => {
                        let len = (span * vbs).min(size_bytes.saturating_sub(offset));
                        item.lost.push((offset, len));
                    }
                }
            }
            item.lost.sort_unstable();
            report.items.push(item);
        }
        Ok(report)
    }
}

#[cfg(test)]
#[path = "tests/fsck_tests.rs"]
mod tests;
