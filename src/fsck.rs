//! Consistency checking and recovery.
//!
//! Everything here is built on one fault-tolerant walk of the pool
//! (`walk_tree`): the object table → every object's tree, then the
//! namespace from the root directory (for paths). Unlike a normal read, the
//! walk never stops at a damaged record; it notes the error and carries on
//! with the rest. Since the table lists every object, a damaged directory
//! hides no space: only an unreadable table or indirect block does.
//!
//! * `fsck` compares the blocks the tree references with the space maps:
//!   *leaks* (allocated, referenced by nothing) and *referenced-but-free*
//!   blocks (live data the allocator could hand out again), plus
//!   *cross-links* (two pointers to the same blocks). `--repair` frees leaks
//!   and re-allocates live space. Objects no directory links to, and
//!   entries for objects that don't exist, are reported as errors.
//! * `rebuild_spacemaps` throws the space maps away and derives them from the
//!   tree (used after a rewind, or when a space map is lost).
//! * `salvage` copies out whatever is still readable, zero-filling the rest.

use crate::blkptr::BlockPtr;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{DNODES_PER_LEAF, Dnode, Extent, ObjType, ROOT_OBJECT, decode_leaf};
use crate::metaslab::TXG_DEFER_SIZE;
use crate::object::{INDIRECT_FANOUT, TreeRoot};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

/// What a record holds: a node of some tree, at a level (0 = data).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjKind {
    /// The object table; its level 0 records hold dnodes.
    Table(u8),
    /// A directory; its data is its entries.
    Dir(u8),
    File(u8),
    Volume(u8),
}

impl ObjKind {
    fn of(t: ObjType, level: u8) -> Self {
        match t {
            ObjType::File => ObjKind::File(level),
            ObjType::Directory => ObjKind::Dir(level),
            ObjType::Volume => ObjKind::Volume(level),
        }
    }

    pub fn level(&self) -> u8 {
        match *self {
            ObjKind::Table(l) | ObjKind::Dir(l) | ObjKind::File(l) | ObjKind::Volume(l) => l,
        }
    }

    fn child(&self) -> Self {
        let l = self.level() - 1;
        match self {
            ObjKind::Table(_) => ObjKind::Table(l),
            ObjKind::Dir(_) => ObjKind::Dir(l),
            ObjKind::File(_) => ObjKind::File(l),
            ObjKind::Volume(_) => ObjKind::Volume(l),
        }
    }

    /// Whether a walk has to read this record to find the rest (indirect
    /// blocks and object table records).
    pub fn is_metadata(&self) -> bool {
        self.level() > 0 || matches!(self, ObjKind::Table(_))
    }
}

/// Callbacks of `walk_tree`. All have empty defaults.
pub trait Visitor {
    /// Every reachable record, whether or not it can be read.
    fn record(&mut self, _ptr: &BlockPtr, _kind: ObjKind) {}
    fn dir(&mut self, _path: &str) {}
    fn file(&mut self, _path: &str, _id: FileId, _dnode: &Dnode) {}
    fn volume(&mut self, _path: &str, _id: FileId, _dnode: &Dnode) {}
    /// A volume's intent log region (in use, though no pointer reaches it).
    fn log_region(&mut self, _start: u64, _len: u64) {}
}

/// A visitor that only wants the walk's own report.
pub struct NoVisitor;
impl Visitor for NoVisitor {}

#[derive(Debug, Default, Clone)]
pub struct WalkReport {
    /// Records reached (read or not).
    pub records: u64,
    /// Unreadable or undecodable records, with their location, and
    /// namespace inconsistencies.
    pub errors: Vec<String>,
    /// Some metadata (an object table record or an indirect block) could
    /// not be read, so part of the pool was never reached: whatever it
    /// references is unknown.
    pub incomplete: bool,
}

/// A pending node of a walk (shared with scrub, see `scan`): record
/// `index` at its level of some tree.
pub(crate) struct Node {
    pub ptr: BlockPtr,
    pub kind: ObjKind,
    pub index: u64,
    /// Whose tree, for messages.
    pub owner: String,
}

/// The root node of `tree` (if it has one).
pub(crate) fn tree_node(tree: &TreeRoot, kind: ObjKind, owner: String) -> Option<Node> {
    tree.ptr.clone().map(|ptr| Node {
        ptr,
        kind,
        index: 0,
        owner,
    })
}

/// The root node of the object table at `table`.
pub(crate) fn table_node(table: &TreeRoot) -> Option<Node> {
    tree_node(table, ObjKind::Table(table.levels), "object table".into())
}

/// What `expand` finds under a record: nodes to walk, and dnodes.
pub(crate) type Expanded = (Vec<Node>, Vec<(FileId, Dnode)>);

/// The children of a metadata record (see `ObjKind::is_metadata`), and
/// for an object table record the dnodes it holds. This is the one place
/// that knows how records reference each other.
pub(crate) fn expand(node: &Node, data: &[u8]) -> Result<Expanded, String> {
    if node.kind.level() > 0 {
        let ind = bincode::deserialize::<Vec<Option<BlockPtr>>>(data)
            .map_err(|e| format!("{} ({:?}): indirect block: {}", node.owner, node.kind, e))?;
        let children = ind
            .into_iter()
            .enumerate()
            .rev()
            .filter_map(|(slot, c)| {
                c.map(|ptr| Node {
                    ptr,
                    kind: node.kind.child(),
                    index: node.index * INDIRECT_FANOUT + slot as u64,
                    owner: node.owner.clone(),
                })
            })
            .collect();
        return Ok((children, Vec::new()));
    }
    if !matches!(node.kind, ObjKind::Table(_)) {
        return Ok((Vec::new(), Vec::new()));
    }
    let slots =
        decode_leaf(data).map_err(|e| format!("object table record {}: {}", node.index, e))?;
    let mut children = Vec::new();
    let mut found = Vec::new();
    for (slot, d) in slots.into_iter().enumerate() {
        let Some(d) = d else { continue };
        let id = node.index * DNODES_PER_LEAF + slot as u64;
        children.extend(tree_node(
            &d.tree,
            ObjKind::of(d.kind, d.tree.levels),
            format!("object {}", id),
        ));
        found.push((id, d));
    }
    Ok((children, found))
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
    fn log_region(&mut self, start: u64, len: u64) {
        let overlap = insert_range(&mut self.live, start, len);
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
    /// Files and volumes.
    objects: Vec<(String, Dnode)>,
}

impl Visitor for Collect {
    fn dir(&mut self, path: &str) {
        self.dirs.push(path.to_string());
    }
    fn file(&mut self, path: &str, _id: FileId, d: &Dnode) {
        self.objects.push((path.to_string(), d.clone()));
    }
    fn volume(&mut self, path: &str, _id: FileId, d: &Dnode) {
        self.objects.push((format!("{}.img", path), d.clone()));
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Walk every record reachable from the object table at `table`,
    /// reporting each to `v`, then the namespace from the root directory.
    /// File and volume data records are only read when `verify_data`.
    pub(crate) async fn walk_tree<V: Visitor>(
        &self,
        table: &TreeRoot,
        verify_data: bool,
        v: &mut V,
    ) -> WalkReport {
        let mut report = WalkReport::default();
        let mut dnodes: BTreeMap<FileId, Dnode> = BTreeMap::new();
        let mut stack: Vec<Node> = table_node(table).into_iter().collect();
        while let Some(node) = stack.pop() {
            report.records += 1;
            v.record(&node.ptr, node.kind);
            // Directory data is read by the namespace walk below.
            let needed =
                node.kind.is_metadata() || (verify_data && !matches!(node.kind, ObjKind::Dir(0)));
            if !needed {
                continue;
            }
            let data = match self.read_record(&node.ptr).await {
                Ok(d) => d,
                Err(e) => {
                    report.errors.push(format!(
                        "{} ({:?}): record at {}: {}",
                        node.owner, node.kind, node.ptr.lba, e
                    ));
                    report.incomplete |= node.kind.is_metadata();
                    continue;
                }
            };
            if !node.kind.is_metadata() {
                continue;
            }
            match expand(&node, &data) {
                Ok((children, found)) => {
                    for (id, d) in found {
                        for r in d.log.iter().flat_map(|l| &l.regions) {
                            v.log_region(r.start, r.len);
                        }
                        dnodes.insert(id, d);
                    }
                    stack.extend(children);
                }
                Err(e) => {
                    report.errors.push(e);
                    report.incomplete = true;
                }
            }
        }
        self.walk_namespace(&dnodes, v, &mut report).await;
        report
    }

    /// Visit every object linked from the root directory, by path; report
    /// entries for missing objects and objects nothing links to.
    async fn walk_namespace<V: Visitor>(
        &self,
        dnodes: &BTreeMap<FileId, Dnode>,
        v: &mut V,
        report: &mut WalkReport,
    ) {
        let mut complete = !report.incomplete;
        let mut reached: BTreeSet<FileId> = BTreeSet::new();
        let mut stack: Vec<(FileId, String)> = Vec::new();
        match dnodes.get(&ROOT_OBJECT) {
            Some(_) => stack.push((ROOT_OBJECT, "/".to_string())),
            None => {
                report.errors.push("the root directory is missing".into());
                complete = false;
            }
        }
        while let Some((id, path)) = stack.pop() {
            reached.insert(id);
            let d = &dnodes[&id];
            match d.kind {
                ObjType::File => v.file(&path, id, d),
                ObjType::Volume => v.volume(&path, id, d),
                ObjType::Directory => {
                    v.dir(&path);
                    let index = match self.read_dir(d).await {
                        Ok(i) => i,
                        Err(e) => {
                            report.errors.push(format!("{}: directory: {}", path, e));
                            complete = false;
                            continue;
                        }
                    };
                    for (name, e) in index.files.into_iter().rev() {
                        let child = join(&path, &name);
                        match dnodes.get(&e.id) {
                            None => report.errors.push(format!(
                                "{}: entry for object {}, which doesn't exist",
                                child, e.id
                            )),
                            Some(cd) if cd.kind != e.kind => report.errors.push(format!(
                                "{}: entry says {}, object {} is a {}",
                                child,
                                e.kind.name(),
                                e.id,
                                cd.kind.name()
                            )),
                            Some(_) if reached.contains(&e.id) => report.errors.push(format!(
                                "{}: object {} is linked more than once",
                                child, e.id
                            )),
                            Some(_) => {
                                reached.insert(e.id);
                                stack.push((e.id, child));
                            }
                        }
                    }
                }
            }
        }
        if complete {
            for id in dnodes.keys().filter(|id| !reached.contains(id)) {
                report
                    .errors
                    .push(format!("object {} isn't linked from any directory", id));
            }
        }
    }

    /// Read every record of the tree under `root`, data included.
    pub(crate) async fn verify_tree(&self, root: &TreeRoot) -> WalkReport {
        self.walk_tree(root, true, &mut NoVisitor).await
    }

    /// Blocks referenced from the committed root (and the walk's report).
    async fn live_set(&self) -> (LiveSet, WalkReport) {
        let root = self.objects.root();
        let mut live = LiveSet::default();
        let walk = self.walk_tree(&root, false, &mut live).await;
        (live, walk)
    }

    /// Blocks referenced by `roots` (older trees), ignoring damage.
    async fn protected_set(&self, roots: &[TreeRoot]) -> BTreeMap<u64, u64> {
        let mut set = LiveSet::default();
        for root in roots {
            self.walk_tree(root, false, &mut set).await;
        }
        set.live
    }

    /// Roots of the valid ring uberblocks older than the committed one.
    async fn older_roots(&self) -> Vec<TreeRoot> {
        let txg = self.uberblock().await.map_or(0, |u| u.txg);
        self.uberblock_ring()
            .await
            .into_iter()
            .filter(|u| u.txg < txg && u.txg + TXG_DEFER_SIZE >= txg)
            .map(|u| u.objects)
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
        older: &[TreeRoot],
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
        let root = self.objects.root();
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

        // Files and volumes alike: a tree of `blksz` records, holes sparse.
        for (path, d) in found.objects {
            let size = d.size;
            let bs = d.blksz as u64;
            let out = std::fs::File::create(local(&path)).map_err(io)?;
            out.set_len(size).map_err(io)?;
            let mut item = Salvaged {
                path,
                bytes: size,
                lost: Vec::new(),
            };
            let mut stack: Vec<(BlockPtr, u8, u64)> = d
                .tree
                .ptr
                .into_iter()
                .map(|r| (r, d.tree.levels, 0))
                .collect();
            while let Some((ptr, level, base)) = stack.pop() {
                let span = INDIRECT_FANOUT.pow(level as u32);
                let offset = base * bs;
                if offset >= size {
                    continue;
                }
                let lost = (span * bs).min(size - offset);
                let data = match self.read_record(&ptr).await {
                    Ok(d) => d,
                    Err(_) => {
                        item.lost.push((offset, lost));
                        continue;
                    }
                };
                if level == 0 {
                    let n = (data.len() as u64).min(size - offset);
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
                    Err(_) => item.lost.push((offset, lost)),
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
