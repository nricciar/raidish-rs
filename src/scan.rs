//! Scrub and resilver.
//!
//! Both walk every block pointer reachable from the root index (directories,
//! inodes, file data, volume indirect trees), read each record through the
//! normal checksummed path (which rebuilds from parity if needed), then check
//! every shard of the record's stripes, data *and* parity, against what the
//! verified contents encode to, rewriting the ones that differ. The fixed
//! label regions (superblock, uberblocks, space maps) carry no parent
//! checksum and are verified separately (see `RaidZ::verify_label_stripe`).
//!
//! A *resilver* is a scrub restricted to records born at or after the TXG
//! the earliest degraded disk started missing writes. Because the tree is
//! copy-on-write, a block is never newer than its parent, so whole subtrees
//! older than that TXG are skipped without being read.
//!
//! Safety while the pool is in use: the scan snapshots the root under the
//! commit lock and holds back deferred frees (`scan_hold`) until it ends, so
//! no block reachable from the snapshot can be reallocated and overwritten
//! while the scan may still rewrite it. Blocks written after the snapshot
//! went to every disk that was online, and the per-disk miss counters tell
//! whether that included the disks being resilvered.

use crate::blkptr::BlockPtr;
use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{FileSystem, FileSystemError, LabelState, SPACEMAP_START, parse_label};
use crate::inode::{FileIndex, FileInode, InodeType};
use crate::raidz::{DATA_SHARDS, StripeCheck};
use crate::stripe::DISKS;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct ScanReport {
    /// Only records born at or after this TXG were checked (0 = everything).
    pub since: u64,
    pub repair: bool,
    pub records: u64,
    pub bytes: u64,
    /// Records skipped (older than `since`, with their subtrees).
    pub pruned: u64,
    pub shards: StripeCheck,
    pub label_stripes: u64,
    /// Label stripes for which no valid contents could be found.
    pub label_unrecoverable: Vec<u64>,
    /// Records that could not be read or verified, with their location.
    pub errors: Vec<String>,
    /// Disks brought back in sync (no longer degraded).
    pub cleared: Vec<usize>,
    /// Why the scan stopped early, if it did.
    pub aborted: Option<String>,
}

impl ScanReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty() && self.label_unrecoverable.is_empty() && self.aborted.is_none()
    }

    pub fn repaired(&self) -> u64 {
        self.shards.repaired.iter().sum()
    }

    pub fn mismatched(&self) -> u64 {
        self.shards.mismatched.iter().sum()
    }
}

impl std::fmt::Display for ScanReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = if self.since == 0 {
            "scrub".to_string()
        } else {
            format!("resilver (TXG >= {})", self.since)
        };
        writeln!(
            f,
            "{}: {} records ({:.1} MiB) checked, {} skipped as unchanged, {} label stripes",
            what,
            self.records,
            self.bytes as f64 / (1u64 << 20) as f64,
            self.pruned,
            self.label_stripes
        )?;
        for d in 0..DISKS {
            let (m, r) = (self.shards.mismatched[d], self.shards.repaired[d]);
            let mut notes = Vec::new();
            if self.shards.unreadable[d] {
                notes.push("unreadable".to_string());
            }
            if self.shards.write_failed[d] {
                notes.push("repair writes failed".to_string());
            }
            if self.cleared.contains(&d) {
                notes.push("back in sync".to_string());
            }
            if m > 0 || !notes.is_empty() {
                writeln!(
                    f,
                    "  disk {}: {} bad shards, {} repaired {}",
                    d,
                    m,
                    r,
                    notes.join(", ")
                )?;
            }
        }
        if self.shards.skipped > 0 {
            writeln!(f, "  {} stripes skipped (being written)", self.shards.skipped)?;
        }
        for e in &self.errors {
            writeln!(f, "  error: {}", e)?;
        }
        for s in &self.label_unrecoverable {
            writeln!(f, "  error: label stripe {} unrecoverable", s)?;
        }
        if let Some(why) = &self.aborted {
            writeln!(f, "  aborted: {}", why)?;
        }
        if self.is_clean() && self.mismatched() == 0 {
            write!(f, "  no errors")?;
        } else if self.is_clean() && self.repair && self.repaired() == self.mismatched() {
            write!(f, "  all errors repaired")?;
        } else if !self.repair && self.mismatched() > 0 {
            write!(f, "  (dry run: nothing repaired)")?;
        }
        Ok(())
    }
}

/// What a node of the walk is (for pruning, decoding and error messages).
#[derive(Debug, Clone, Copy)]
enum ObjKind {
    RootIndex,
    DirIndex,
    Inode,
    FileData,
}

enum Node {
    /// A whole object: all its records are needed to decode it.
    Object {
        ptrs: Vec<BlockPtr>,
        kind: ObjKind,
        path: String,
    },
    /// A volume tree node; `level` 0 is a data record.
    Volume {
        ptr: BlockPtr,
        level: u8,
        path: String,
    },
}

/// Free space below which a running scan gives up, since it keeps freed
/// space from being reused while it runs.
const MIN_FREE_FRACTION: u64 = 50; // 1/50 = 2%

impl<D: BlockDevice> FileSystem<D> {
    /// Check everything; with `repair`, fix what's wrong and bring degraded
    /// disks back in sync.
    pub async fn scrub(&self, repair: bool) -> Result<ScanReport, FileSystemError> {
        self.scan(Some(0), repair).await
    }

    /// Rebuild what degraded (online) disks missed. Does nothing if no
    /// online disk is degraded.
    pub async fn resilver(&self) -> Result<ScanReport, FileSystemError> {
        self.scan(None, true).await
    }

    /// Does any reachable disk need a resilver (or have unrecorded misses)?
    pub fn needs_resilver(&self) -> bool {
        let Some(pool) = self.dev.pool() else {
            return false;
        };
        let degraded = self.degraded_disks();
        let missed = self.unaccounted_misses();
        (0..DISKS).any(|d| pool.stripe.is_online(d) && (degraded[d].is_some() || missed[d]))
    }

    /// `since`: None = resilver (worked out from the degraded disks once
    /// the snapshot is taken).
    async fn scan(&self, since: Option<u64>, repair: bool) -> Result<ScanReport, FileSystemError> {
        let repair = repair && !self.read_only;
        let _one = self.scan_lock.lock().await;
        let pool = self.dev.pool();

        // Snapshot: commit first so everything written so far is in the tree
        // being walked (and missed writes are folded into `degraded`).
        let (root, misses_at_start, since) = {
            let _tx = self.begin_tx().await;
            if !self.read_only {
                self.commit_locked().await?;
            }
            let misses: [u64; DISKS] =
                std::array::from_fn(|d| pool.map_or(0, |p| p.stripe.miss_count(d)));
            let degraded = self.degraded_disks();
            let since = match since {
                Some(s) => s,
                None => {
                    let online = |d: usize| pool.is_some_and(|p| p.stripe.is_online(d));
                    match (0..DISKS).filter(|&d| online(d)).filter_map(|d| degraded[d]).min() {
                        Some(s) => s,
                        None => {
                            return Ok(ScanReport {
                                repair,
                                ..Default::default()
                            });
                        }
                    }
                }
            };
            self.scan_hold.fetch_add(1, Ordering::SeqCst);
            (self.root_ptr.lock().await.clone(), misses, since)
        };

        let mut report = ScanReport {
            since,
            repair,
            ..Default::default()
        };
        let walked = self.scan_tree(root, since, repair, &mut report).await;
        {
            let _tx = self.begin_tx().await;
            self.scan_labels(repair, &mut report).await;
        }
        self.scan_hold.fetch_sub(1, Ordering::SeqCst);
        self.release_held_frees().await;
        walked?;

        if repair
            && report.is_clean()
            && let Some(pool) = pool
        {
            let degraded = self.degraded_disks();
            for d in 0..DISKS {
                let covered = degraded[d].is_some_and(|t| t >= since);
                let in_sync = pool.stripe.is_online(d)
                    && pool.stripe.miss_count(d) == misses_at_start[d]
                    && !report.shards.unreadable[d]
                    && !report.shards.write_failed[d];
                if !(covered && in_sync) {
                    continue;
                }
                if pool.stripe.is_fenced(d) {
                    if let Err(e) = self.write_disk_label(d).await {
                        eprintln!("disk {}: label write failed: {}", d, e);
                        continue;
                    }
                    pool.stripe.set_fenced(d, false);
                }
                self.degraded.lock().unwrap()[d] = None;
                report.cleared.push(d);
            }
            if !report.cleared.is_empty() {
                self.commit_txg().await?;
            }
        }
        Ok(report)
    }

    async fn scan_tree(
        &self,
        root: Vec<BlockPtr>,
        since: u64,
        repair: bool,
        report: &mut ScanReport,
    ) -> Result<(), FileSystemError> {
        let total = self.layout.metaslab_count * self.layout.metaslab_size_blocks;
        let mut stack = vec![Node::Object {
            ptrs: root,
            kind: ObjKind::RootIndex,
            path: "/".to_string(),
        }];

        while let Some(node) = stack.pop() {
            if !self.read_only
                && report.records % 256 == 255
                && self.allocatable_blocks().await < total / MIN_FREE_FRACTION
            {
                report.aborted = Some("pool almost full (scan holds freed space)".into());
                return Ok(());
            }
            match node {
                Node::Object { ptrs, kind, path } => {
                    if ptrs.is_empty() {
                        continue;
                    }
                    if ptrs.iter().all(|p| p.birth_txg < since) {
                        report.pruned += ptrs.len() as u64;
                        continue;
                    }
                    let mut data = Vec::new();
                    let mut complete = true;
                    for ptr in &ptrs {
                        match self.scan_record(ptr, repair, report).await {
                            Some(d) => data.extend_from_slice(&d),
                            None => {
                                report.errors.push(format!(
                                    "{} ({:?}): record at {} unreadable",
                                    path, kind, ptr.lba
                                ));
                                complete = false;
                            }
                        }
                    }
                    if complete {
                        self.descend(kind, &path, &data, &mut stack, report);
                    }
                }
                Node::Volume { ptr, level, path } => {
                    if ptr.birth_txg < since {
                        report.pruned += 1;
                        continue;
                    }
                    let Some(data) = self.scan_record(&ptr, repair, report).await else {
                        report.errors.push(format!(
                            "{} (volume L{}): record at {} unreadable",
                            path, level, ptr.lba
                        ));
                        continue;
                    };
                    if level > 0 {
                        match bincode::deserialize::<Vec<Option<BlockPtr>>>(&data) {
                            Ok(ind) => stack.extend(ind.into_iter().flatten().map(|c| {
                                Node::Volume {
                                    ptr: c,
                                    level: level - 1,
                                    path: path.clone(),
                                }
                            })),
                            Err(e) => report
                                .errors
                                .push(format!("{} (volume L{}): {}", path, level, e)),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn descend(
        &self,
        kind: ObjKind,
        path: &str,
        data: &[u8],
        stack: &mut Vec<Node>,
        report: &mut ScanReport,
    ) {
        let join = |name: &str| {
            if path.ends_with('/') {
                format!("{}{}", path, name)
            } else {
                format!("{}/{}", path, name)
            }
        };
        match kind {
            ObjKind::RootIndex | ObjKind::DirIndex => match bincode::deserialize::<FileIndex>(data)
            {
                Ok(index) => {
                    for (name, r) in index.files {
                        stack.push(Node::Object {
                            ptrs: r.inode,
                            kind: ObjKind::Inode,
                            path: join(&name),
                        });
                    }
                }
                Err(e) => report.errors.push(format!("{}: directory: {}", path, e)),
            },
            ObjKind::Inode => match bincode::deserialize::<FileInode>(data) {
                Ok(inode) => match inode.inode_type {
                    InodeType::File { blocks, .. } => stack.push(Node::Object {
                        ptrs: blocks,
                        kind: ObjKind::FileData,
                        path: path.to_string(),
                    }),
                    InodeType::Directory { blocks } => stack.push(Node::Object {
                        ptrs: blocks,
                        kind: ObjKind::DirIndex,
                        path: path.to_string(),
                    }),
                    InodeType::Volume { root, levels, .. } => {
                        stack.extend(root.map(|ptr| Node::Volume {
                            ptr,
                            level: levels,
                            path: path.to_string(),
                        }))
                    }
                },
                Err(e) => report.errors.push(format!("{}: inode: {}", path, e)),
            },
            ObjKind::FileData => {}
        }
    }

    /// Read one record (checksum verified, rebuilt from parity if needed),
    /// then check all its shards against the verified contents.
    async fn scan_record(
        &self,
        ptr: &BlockPtr,
        repair: bool,
        report: &mut ScanReport,
    ) -> Option<Vec<u8>> {
        let data = self.read_record_healing(ptr, false).await.ok()?;
        report.records += 1;
        report.bytes += ptr.lsize as u64;
        if let Some(pool) = self.dev.pool() {
            // Exactly what `write_record_blocks` wrote: the data zero-padded
            // to whole stripes (the rest of `asize`, if any, was never written).
            let d = DATA_SHARDS as u64;
            let blocks = ptr.data_blocks().div_ceil(d) * d;
            let mut padded = vec![0u8; blocks as usize * BLOCK_SIZE];
            padded[..data.len()].copy_from_slice(&data);
            match pool.verify_stripes(ptr.lba / d, &padded, repair).await {
                Ok(check) => report.shards.add(&check),
                Err(e) => {
                    report.errors.push(format!("record at {}: {}", ptr.lba, e));
                }
            }
        }
        Some(data)
    }

    /// Verify the fixed label regions: superblock, uberblock ring and space
    /// map slots. Must run under the commit lock (commits rewrite them).
    async fn scan_labels(&self, repair: bool, report: &mut ScanReport) {
        let Some(pool) = self.dev.pool() else {
            return;
        };
        let d = DATA_SHARDS as u64;
        let header = 0..SPACEMAP_START / d;
        let spacemaps = self.layout.spacemap_start / d..self.layout.data_start / d;
        let score = |b: &[u8]| parse_label(b).ok().map(|(txg, _)| txg);
        for stripe in header.chain(spacemaps) {
            report.label_stripes += 1;
            match pool.verify_label_stripe(stripe, repair, score).await {
                Ok(Some(check)) => report.shards.add(&check),
                Ok(None) => {
                    // Never-written space map slots are all zeros and fine.
                    if !self.label_stripe_is_blank(stripe).await {
                        report.label_unrecoverable.push(stripe);
                    }
                }
                Err(e) => report.errors.push(format!("label stripe {}: {}", stripe, e)),
            }
        }
    }

    async fn label_stripe_is_blank(&self, stripe: u64) -> bool {
        let Some(pool) = self.dev.pool() else {
            return false;
        };
        let mut readable = 0;
        for disk in 0..DISKS {
            let mut b = vec![0u8; BLOCK_SIZE];
            if pool.stripe.read_shards_raw(disk, stripe, &mut b).await.is_ok() {
                readable += 1;
                if b.iter().any(|&x| x != 0) {
                    return false;
                }
            }
        }
        readable >= DATA_SHARDS
    }
}

/// Keep a served pool healthy: whenever a disk (re)connects, check its label
/// (a replaced disk is fenced and fully resilvered), and whenever an online
/// disk is degraded, resilver it. Also re-checks periodically, since misses
/// are only noticed at commit time.
pub fn spawn_health_monitor<D: BlockDevice + 'static>(
    fs: Arc<FileSystem<D>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    if let Some(pool) = fs.dev.pool() {
        for d in 0..DISKS {
            if let Some(mut events) = pool.stripe.disks[d].reconnects() {
                let tx = tx.clone();
                tokio::spawn(async move {
                    while events.changed().await.is_ok() {
                        if tx.send(d).is_err() {
                            return;
                        }
                    }
                });
            }
        }
    }
    drop(tx);

    tokio::spawn(async move {
        // A resilver that can't finish clean (say, an unreadable record) is
        // retried with backoff instead of rescanning every interval.
        let mut wait = interval;
        loop {
            if fs.needs_resilver() {
                match fs.resilver().await {
                    Ok(report) => {
                        if report.since > 0 || !report.cleared.is_empty() {
                            eprintln!("{}", report);
                        }
                        wait = if report.is_clean() {
                            interval
                        } else {
                            (wait * 2).min(interval * 20)
                        };
                    }
                    Err(e) => {
                        eprintln!("resilver failed: {}", e);
                        wait = (wait * 2).min(interval * 20);
                    }
                }
            }
            tokio::select! {
                Some(d) = rx.recv() => {
                    wait = interval;
                    if fs.adopt_disk(d).await == LabelState::Foreign {
                        // Persist the degraded mark before resilvering.
                        let _ = fs.commit_txg().await;
                    }
                }
                _ = tokio::time::sleep(wait) => {}
            }
        }
    })
}

#[cfg(test)]
#[path = "tests/scan_tests.rs"]
mod tests;
