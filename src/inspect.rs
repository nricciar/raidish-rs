//! Read-only views of a pool for dashboards and the terminal tools: health,
//! space, metaslabs, the objects in the tree, and where everything lives.
//!
//! Portable (the web dashboard runs it in the browser over the websocket
//! block protocol). Everything here only reads.

use crate::disk::{BlockDevice, IoStats};
use crate::fs::{FileSystem, LabelState, SUPERBLOCK_LBA};
use crate::inode::{FileIndex, InodeType};
use crate::metaslab::SPACEMAP_SLOTS;
use crate::vdev::Topology;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Every disk online and in sync.
    Online,
    /// Data is intact but redundancy is reduced (disks offline, foreign, or
    /// waiting for a resilver).
    Degraded,
    /// More disks are missing than some vdev can tolerate.
    Faulted,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiskReport {
    pub index: usize,
    pub path: String,
    pub local: bool,
    pub online: bool,
    pub label: LabelState,
    /// Earliest TXG it may have missed (needs a resilver), if any.
    pub degraded_since: Option<u64>,
    /// Failed writes/flushes seen by this process.
    pub missed_writes: u64,
    pub fenced: bool,
    pub capacity_blocks: Option<u64>,
    pub io: Option<IoStats>,
}

impl DiskReport {
    /// Can't serve data right now (offline, or not this pool's disk).
    pub fn unavailable(&self) -> bool {
        !self.online || self.label != LabelState::Ok
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoolReport {
    pub health: Health,
    pub layout: String,
    pub topology: Option<Topology>,
    /// Disk failures the layout survives when healthy, and right now.
    pub fault_tolerance: usize,
    pub failures_left: usize,
    pub disks: Vec<DiskReport>,
    pub txg: u64,
    /// Unix milliseconds of the last commit.
    pub committed_ms: Option<u64>,
    /// Rewind targets: (txg, unix ms), newest first.
    pub ring: Vec<(u64, u64)>,
    pub total_blocks: u64,
    pub metaslab_count: u64,
    pub used_blocks: u64,
    pub free_blocks: u64,
    /// Freed, but not reusable for a few TXGs (rewind safety).
    pub deferred_blocks: u64,
    /// Sum of the member disks' capacities (parity and labels included).
    pub raw_blocks: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetaslabReport {
    pub index: usize,
    pub start: u64,
    pub blocks: u64,
    pub used: u64,
    pub free: u64,
    pub deferred: u64,
    /// Free extents: a rough fragmentation measure.
    pub free_extents: usize,
    pub active_slot: Option<u64>,
    pub synced_txg: u64,
    pub dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    File,
    Directory,
    Volume,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectReport {
    pub path: String,
    pub kind: ObjectKind,
    /// Logical size (0 for directories).
    pub size_bytes: u64,
    /// Blocks the inode and its contents occupy.
    pub allocated_blocks: u64,
    pub volblocksize: Option<u32>,
}

/// What a block is used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BlockKind {
    Superblock,
    Uberblock,
    SpaceMap,
    FileIndex,
    Inode,
    Data,
    /// Allocated but not reached from the tree walk here (indirect blocks
    /// of deeper directories, leaks).
    Other,
    Free,
    /// Past the last metaslab: never allocated.
    Slack,
}

impl BlockKind {
    pub const ALL: [BlockKind; 9] = [
        BlockKind::Superblock,
        BlockKind::Uberblock,
        BlockKind::SpaceMap,
        BlockKind::FileIndex,
        BlockKind::Inode,
        BlockKind::Data,
        BlockKind::Other,
        BlockKind::Free,
        BlockKind::Slack,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            BlockKind::Superblock => "Superblock",
            BlockKind::Uberblock => "Uberblocks",
            BlockKind::SpaceMap => "Space maps",
            BlockKind::FileIndex => "Root index",
            BlockKind::Inode => "Inodes",
            BlockKind::Data => "Data",
            BlockKind::Other => "Other",
            BlockKind::Free => "Free",
            BlockKind::Slack => "Slack",
        }
    }

    /// Overlapping spans resolve to the higher priority (headers win).
    fn priority(&self) -> u8 {
        match self {
            BlockKind::Free | BlockKind::Slack => 0,
            BlockKind::Other => 1,
            BlockKind::Data => 2,
            BlockKind::Inode => 3,
            BlockKind::FileIndex => 4,
            BlockKind::SpaceMap => 5,
            BlockKind::Uberblock => 6,
            BlockKind::Superblock => 7,
        }
    }
}

/// A run of blocks with one use (and, for objects, an owner).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u64,
    pub len: u64,
    pub kind: BlockKind,
    /// Index into `Layout::owners`.
    pub owner: Option<u32>,
}

/// Where everything in the pool lives, as non-overlapping spans sorted by
/// start. Blocks covered by no span are `Other`. Built once (it reads the
/// tree); any window of it can then be bucketed without I/O.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Layout {
    pub total_blocks: u64,
    pub spans: Vec<Span>,
    /// Object paths (files, directories, volumes).
    pub owners: Vec<String>,
}

/// One cell of a block map.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Bucket {
    pub start: u64,
    pub len: u64,
    /// Blocks of each kind, indexed like `BlockKind::ALL`.
    pub counts: [u64; 9],
    /// The object owning most of the bucket's blocks.
    pub owner: Option<u32>,
}

impl Bucket {
    /// The kind most of the bucket is.
    pub fn dominant(&self) -> BlockKind {
        let (i, _) = self
            .counts
            .iter()
            .enumerate()
            .max_by_key(|(i, c)| (**c, std::cmp::Reverse(*i)))
            .unwrap();
        BlockKind::ALL[i]
    }

    /// Share of the bucket that is allocated (not free).
    pub fn fill(&self) -> f64 {
        if self.len == 0 {
            return 0.0;
        }
        let unused =
            self.counts[kind_index(BlockKind::Free)] + self.counts[kind_index(BlockKind::Slack)];
        1.0 - unused as f64 / self.len as f64
    }
}

fn kind_index(k: BlockKind) -> usize {
    BlockKind::ALL.iter().position(|x| *x == k).unwrap()
}

impl Layout {
    /// Split `start..start+len` into `buckets` cells (fewer if the window is
    /// smaller than that) and count what's in each.
    pub fn buckets(&self, start: u64, len: u64, buckets: usize) -> Vec<Bucket> {
        let end = (start + len).min(self.total_blocks);
        if end <= start || buckets == 0 {
            return Vec::new();
        }
        let len = end - start;
        let per = len.div_ceil(buckets as u64).max(1);
        let n = len.div_ceil(per) as usize;
        let mut out: Vec<Bucket> = (0..n)
            .map(|i| {
                let s = start + i as u64 * per;
                Bucket {
                    start: s,
                    len: per.min(end - s),
                    ..Default::default()
                }
            })
            .collect();
        let mut owned: Vec<Vec<(u32, u64)>> = vec![Vec::new(); n];

        let first = self.spans.partition_point(|s| s.start + s.len <= start);
        for span in &self.spans[first..] {
            if span.start >= end {
                break;
            }
            let (s, e) = (span.start.max(start), (span.start + span.len).min(end));
            let mut b = ((s - start) / per) as usize;
            let mut pos = s;
            while pos < e {
                let bucket_end = start + (b as u64 + 1) * per;
                let take = e.min(bucket_end) - pos;
                out[b].counts[kind_index(span.kind)] += take;
                if let Some(o) = span.owner {
                    match owned[b].iter_mut().find(|(x, _)| *x == o) {
                        Some((_, c)) => *c += take,
                        None => owned[b].push((o, take)),
                    }
                }
                pos += take;
                b += 1;
            }
        }
        for (bucket, owners) in out.iter_mut().zip(owned) {
            let counted: u64 = bucket.counts.iter().sum();
            bucket.counts[kind_index(BlockKind::Other)] += bucket.len.saturating_sub(counted);
            bucket.owner = owners.iter().max_by_key(|(_, c)| *c).map(|(o, _)| *o);
        }
        out
    }

    /// Blocks of each kind in the whole pool, indexed like `BlockKind::ALL`.
    pub fn totals(&self) -> [u64; 9] {
        let mut t = [0u64; 9];
        for s in &self.spans {
            t[kind_index(s.kind)] += s.len;
        }
        let counted: u64 = t.iter().sum();
        t[kind_index(BlockKind::Other)] += self.total_blocks.saturating_sub(counted);
        t
    }

    /// Spans at `block` (for a hover on a single block).
    pub fn span_at(&self, block: u64) -> Option<&Span> {
        let i = self.spans.partition_point(|s| s.start + s.len <= block);
        self.spans.get(i).filter(|s| s.start <= block)
    }
}

/// Turn possibly overlapping spans into sorted, disjoint ones (the higher
/// priority kind wins where they overlap).
fn flatten(mut spans: Vec<Span>, total: u64) -> Vec<Span> {
    // Boundaries sweep: at each boundary, the highest priority live span.
    let mut points: Vec<(u64, bool, usize)> = Vec::with_capacity(spans.len() * 2);
    spans.retain(|s| s.len > 0 && s.start < total);
    for (i, s) in spans.iter().enumerate() {
        points.push((s.start, true, i));
        points.push(((s.start + s.len).min(total), false, i));
    }
    points.sort_by_key(|(p, open, _)| (*p, *open));
    let mut live: std::collections::BTreeSet<(u8, usize)> = Default::default();
    let mut out: Vec<Span> = Vec::new();
    let mut prev = 0u64;
    for (p, open, i) in points {
        if p > prev
            && let Some(&(_, top)) = live.iter().next_back()
        {
            let s = spans[top];
            match out.last_mut() {
                Some(last)
                    if last.start + last.len == prev
                        && last.kind == s.kind
                        && last.owner == s.owner =>
                {
                    last.len += p - prev
                }
                _ => out.push(Span {
                    start: prev,
                    len: p - prev,
                    kind: s.kind,
                    owner: s.owner,
                }),
            }
        }
        prev = p;
        let key = (spans[i].kind.priority(), i);
        if open {
            live.insert(key);
        } else {
            live.remove(&key);
        }
    }
    out
}

impl<D: BlockDevice> FileSystem<D> {
    /// Health, space and per-disk state of the pool.
    pub async fn pool_report(&self) -> PoolReport {
        let sb = self.superblock().await;
        let ub = self.uberblock().await;
        let degraded = self.degraded_disks();
        let mut disks = Vec::new();
        let mut raw_blocks = 0;
        if let Some(pool) = self.dev.pool() {
            for (d, caddy) in pool.disks.disks.iter().enumerate() {
                let capacity = caddy.capacity_blocks().await.ok();
                raw_blocks += capacity.unwrap_or(0);
                disks.push(DiskReport {
                    index: d,
                    path: caddy.path().to_string(),
                    local: caddy.is_local(),
                    online: pool.disks.is_online(d),
                    label: self.check_disk_label(d).await,
                    degraded_since: degraded.get(d).copied().flatten(),
                    missed_writes: pool.disks.miss_count(d),
                    fenced: pool.disks.is_fenced(d),
                    capacity_blocks: capacity,
                    io: caddy.io_stats(),
                });
            }
        }
        let topology = self.dev.pool().map(|p| p.topology.clone());
        let (health, tolerance, left) = match &topology {
            Some(t) => {
                let margin = margin(t, &disks);
                let any_bad = disks
                    .iter()
                    .any(|d| d.unavailable() || d.degraded_since.is_some());
                let health = if margin < 0 {
                    Health::Faulted
                } else if any_bad {
                    Health::Degraded
                } else {
                    Health::Online
                };
                (health, t.fault_tolerance(), margin.max(0) as usize)
            }
            None => (Health::Online, 0, 0),
        };

        let (mut used, mut free, mut deferred) = (0, 0, 0);
        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            let (f, p) = (ms.free_blocks(), ms.pending_free_blocks());
            free += f;
            deferred += p;
            used += ms.block_count - f - p;
        }

        PoolReport {
            health,
            layout: topology
                .as_ref()
                .map_or("single device".into(), |t| t.summary()),
            topology,
            fault_tolerance: tolerance,
            failures_left: left,
            disks,
            txg: ub.as_ref().map_or(0, |u| u.txg),
            committed_ms: ub.as_ref().map(|u| u.timestamp),
            ring: self
                .uberblock_ring()
                .await
                .iter()
                .map(|u| (u.txg, u.timestamp))
                .collect(),
            total_blocks: sb.as_ref().map_or(0, |s| s.total_blocks),
            metaslab_count: sb.as_ref().map_or(0, |s| s.metaslab_count),
            used_blocks: used,
            free_blocks: free,
            deferred_blocks: deferred,
            raw_blocks,
        }
    }

    pub async fn metaslab_reports(&self) -> Vec<MetaslabReport> {
        let mut out = Vec::with_capacity(self.metaslabs.len());
        for (i, ms) in self.metaslabs.iter().enumerate() {
            let ms = ms.read().await;
            let (free, deferred) = (ms.free_blocks(), ms.pending_free_blocks());
            out.push(MetaslabReport {
                index: i,
                start: ms.start_block,
                blocks: ms.block_count,
                used: ms.block_count - free - deferred,
                free,
                deferred,
                free_extents: ms.free_extents.len(),
                active_slot: ms.active_slot,
                synced_txg: ms.synced_txg,
                dirty: ms.dirty,
            });
        }
        out
    }

    /// Every object in the tree, and the layout of the whole pool.
    pub async fn layout(&self) -> (Layout, Vec<ObjectReport>) {
        let Some(sb) = self.superblock().await else {
            return (Layout::default(), Vec::new());
        };
        let total = sb.total_blocks;
        let mut spans = Vec::new();
        let mut owners = Vec::new();
        let mut objects = Vec::new();
        let push = |spans: &mut Vec<Span>, start, len, kind, owner| {
            spans.push(Span {
                start,
                len,
                kind,
                owner,
            })
        };

        let mut metaslabs_end = sb.spacemap_start;
        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            metaslabs_end = metaslabs_end.max(ms.start_block + ms.block_count);
            let pending = ms
                .freeing
                .iter()
                .chain(ms.deferred.iter().flat_map(|(_, v)| v.iter()))
                .map(|e| (e.start, e.len));
            for (s, l) in ms.free_extents.iter().map(|(&s, &l)| (s, l)).chain(pending) {
                push(&mut spans, s, l, BlockKind::Free, None);
            }
        }
        push(
            &mut spans,
            metaslabs_end,
            total.saturating_sub(metaslabs_end),
            BlockKind::Slack,
            None,
        );
        push(
            &mut spans,
            SUPERBLOCK_LBA,
            self.header.superblock_blocks,
            BlockKind::Superblock,
            None,
        );
        push(
            &mut spans,
            self.header.uberblock_start,
            self.header.uberblock_blocks(),
            BlockKind::Uberblock,
            None,
        );
        push(
            &mut spans,
            sb.spacemap_start,
            sb.metaslab_count * SPACEMAP_SLOTS * sb.spacemap_slot_blocks,
            BlockKind::SpaceMap,
            None,
        );
        for p in self.root_ptr.lock().await.iter() {
            let e = p.extent();
            push(&mut spans, e.start(), e.len(), BlockKind::FileIndex, None);
        }

        // Walk the tree (directories breadth first).
        let root = self.file_index.read().await.clone();
        let mut dirs: Vec<(String, FileIndex)> = vec![(String::new(), root)];
        while let Some((prefix, index)) = dirs.pop() {
            for (name, inode_ref) in &index.files {
                let path = format!("{}/{}", prefix, name);
                let owner = owners.len() as u32;
                owners.push(path.clone());
                let mut allocated = 0;
                for e in inode_ref.inode_extents() {
                    allocated += e.len();
                    push(
                        &mut spans,
                        e.start(),
                        e.len(),
                        BlockKind::Inode,
                        Some(owner),
                    );
                }
                let Ok(inode) = self.read_inode(inode_ref).await else {
                    continue;
                };
                for e in self
                    .content_extents(&inode.inode_type)
                    .await
                    .unwrap_or_default()
                {
                    allocated += e.len();
                    push(&mut spans, e.start(), e.len(), BlockKind::Data, Some(owner));
                }
                let (kind, size, vbs) = match &inode.inode_type {
                    InodeType::File { size_bytes, .. } => (ObjectKind::File, *size_bytes, None),
                    InodeType::Directory { .. } => {
                        if let Ok(sub) = self.read_file_index(&inode.inode_type).await {
                            dirs.push((path.clone(), sub));
                        }
                        (ObjectKind::Directory, 0, None)
                    }
                    InodeType::Volume {
                        size_bytes,
                        volblocksize,
                        ..
                    } => (ObjectKind::Volume, *size_bytes, Some(*volblocksize)),
                };
                objects.push(ObjectReport {
                    path,
                    kind,
                    size_bytes: size,
                    allocated_blocks: allocated,
                    volblocksize: vbs,
                });
            }
        }
        objects.sort_by(|a, b| a.path.cmp(&b.path));
        (
            Layout {
                total_blocks: total,
                spans: flatten(spans, total),
                owners,
            },
            objects,
        )
    }

    /// (records, discontiguities) of a volume (see `defrag --check`).
    pub async fn volume_fragmentation(&self, name: &str) -> Result<(u64, u64), String> {
        let name = name.trim_start_matches('/');
        let r = self.fragmentation(name).await.map_err(|e| e.to_string())?;
        Ok((r.records, r.breaks))
    }
}

/// Disk failures `t` can still take: its tolerance minus the disks that are
/// already unavailable (negative: data is missing).
fn margin(t: &Topology, disks: &[DiskReport]) -> isize {
    margin_by(t, &|d| disks.get(d).is_some_and(|r| r.unavailable()))
}

/// Like the pool health's margin, but a disk waiting for a resilver counts
/// as bad too (it may hold stale data for anything written while it was
/// away): how many more disk failures a read of recent writes survives.
pub(crate) fn margin_for_recent_writes(report: &PoolReport) -> isize {
    let Some(t) = &report.topology else {
        return 0;
    };
    margin_by(t, &|d| {
        report
            .disks
            .get(d)
            .is_some_and(|r| r.unavailable() || r.degraded_since.is_some())
    })
}

fn margin_by(t: &Topology, bad_disk: &dyn Fn(usize) -> bool) -> isize {
    let bad = |d: &usize| bad_disk(*d);
    match t {
        Topology::Disk(d) => -(bad(d) as isize),
        Topology::RaidZ { parity, disks: ds } => {
            *parity as isize - ds.iter().filter(|d| bad(d)).count() as isize
        }
        Topology::Mirror { disks: ds } => {
            ds.len() as isize - 1 - ds.iter().filter(|d| bad(d)).count() as isize
        }
        Topology::Stripe { children } => children
            .iter()
            .map(|c| margin_by(c, bad_disk))
            .min()
            .unwrap_or(0),
    }
}

#[cfg(test)]
#[path = "tests/inspect_tests.rs"]
mod tests;

/// Where each of the pool's disks is served, as a `listen` node tells a
/// dashboard (`GET /api/v1/pool`): the browser opens the pool from this.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PoolMap {
    pub topology: Topology,
    /// In pool order.
    pub disks: Vec<DiskAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiskAddr {
    /// Served by another node (its config's URL).
    Url(String),
    /// Served by the node that answered, as this disk index.
    This(u16),
}

impl PoolMap {
    /// Every disk as an `http(s)://host:port/api/v1/disks/N` URL, with the
    /// answering node's disks under `origin` (e.g. `http://nas:8881`).
    pub fn urls(&self, origin: &str) -> Vec<String> {
        let origin = origin.trim_end_matches('/');
        self.disks
            .iter()
            .map(|d| match d {
                DiskAddr::Url(u) => u.clone(),
                DiskAddr::This(i) => format!("{}/api/v1/disks/{}", origin, i),
            })
            .collect()
    }
}
