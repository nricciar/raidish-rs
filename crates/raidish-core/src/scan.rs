//! Scrub and resilver.
//!
//! Both walk every block pointer reachable from the root index (directories,
//! inodes, file data, volume indirect trees), read each record through the
//! normal checksummed path (which rebuilds from parity if needed), then check
//! every shard of the record's stripes, data *and* parity, against what the
//! verified contents encode to, rewriting the ones that differ. The fixed
//! label regions (superblock, uberblocks, space maps) carry no parent
//! checksum and are verified separately (see `Vdev::verify_labels`).
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
use crate::fs::{FileSystem, FileSystemError, parse_label};
use crate::fsck::{Node, table_node};
use crate::object::TreeRoot;
use crate::types::DatasetName;
use crate::types::Txg;
use crate::vdev::StripeCheck;

#[derive(Debug, Default, Clone)]
pub struct ScanReport {
    /// Only records born at or after this TXG were checked (0 = everything).
    pub since: Txg,
    pub repair: bool,
    pub records: u64,
    pub bytes: u64,
    /// Records skipped (older than `since`, with their subtrees).
    pub pruned: u64,
    /// Intent log records checked (they're outside the tree).
    pub log_records: u64,
    pub shards: StripeCheck,
    pub label_stripes: u64,
    /// Label units (first block of each) for which no valid contents could be found.
    pub label_unrecoverable: Vec<u64>,
    /// Label units decided with stale (degraded) members' copies voting:
    /// see `LabelScan::stale_votes`.
    pub label_stale_votes: Vec<u64>,
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

    /// Clean, and good enough to vouch for degraded disks: nothing skipped
    /// as being written (structural triage I-21) and no label decided by a
    /// stale member's own copy (I-19). Otherwise a disk could be cleared
    /// with something on it that nothing checked.
    pub fn certifies(&self) -> bool {
        self.is_clean() && self.shards.skipped == 0 && self.label_stale_votes.is_empty()
    }

    pub fn repaired(&self) -> u64 {
        self.shards.repaired.iter().sum()
    }

    pub fn mismatched(&self) -> u64 {
        self.shards.mismatched.iter().sum()
    }

    /// One line, for the activity log.
    pub fn summary(&self) -> String {
        let mut s = format!(
            "{} records ({:.1} MiB) checked, {} repaired",
            self.records,
            self.bytes as f64 / (1u64 << 20) as f64,
            self.repaired()
        );
        if !self.cleared.is_empty() {
            let disks: Vec<String> = self.cleared.iter().map(|d| d.to_string()).collect();
            s += &format!("; disk {} back in sync", disks.join(", "));
        }
        if !self.errors.is_empty() {
            s += &format!("; {} unreadable records", self.errors.len());
        }
        if let Some(a) = &self.aborted {
            s += &format!("; stopped: {}", a);
        }
        s
    }
}

impl std::fmt::Display for ScanReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = if self.since == Txg::ZERO {
            "scrub".to_string()
        } else {
            format!("resilver (TXG >= {})", self.since)
        };
        writeln!(
            f,
            "{}: {} records ({:.1} MiB) checked, {} skipped as unchanged, {} label stripes, \
             {} intent log records",
            what,
            self.records,
            self.bytes as f64 / (1u64 << 20) as f64,
            self.pruned,
            self.label_stripes,
            self.log_records
        )?;
        for d in 0..self.shards.disks() {
            let (m, r) = (self.shards.mismatched[d], self.shards.repaired[d]);
            let mut notes = Vec::new();
            if self.shards.is_unreadable(d) {
                notes.push("unreadable".to_string());
            }
            if self.shards.has_write_failed(d) {
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
            writeln!(
                f,
                "  {} stripes skipped (being written)",
                self.shards.skipped
            )?;
        }
        for e in &self.errors {
            writeln!(f, "  error: {}", e)?;
        }
        for s in &self.label_unrecoverable {
            writeln!(f, "  error: label block {} unrecoverable", s)?;
        }
        if !self.label_stale_votes.is_empty() {
            writeln!(
                f,
                "  {} label units checked against degraded disks only (no disk cleared)",
                self.label_stale_votes.len()
            )?;
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

/// Free space below which a running scan gives up, since it keeps freed
/// space from being reused while it runs.
const MIN_FREE_FRACTION: u64 = 50; // 1/50 = 2%

impl<D: BlockDevice> FileSystem<D> {
    /// Check everything; with `repair`, fix what's wrong and bring degraded
    /// disks back in sync.
    pub async fn scrub(&self, repair: bool) -> Result<ScanReport, FileSystemError> {
        self.scan(Some(Txg::ZERO), repair).await
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
        (0..pool.disks().len()).any(|d| {
            pool.disks().is_online(d)
                && (degraded.get(d).is_some_and(|t| t.is_some())
                    || missed.get(d).copied().unwrap_or(false))
        })
    }

    /// `since`: None = resilver (worked out from the degraded disks once
    /// the snapshot is taken).
    async fn scan(&self, since: Option<Txg>, repair: bool) -> Result<ScanReport, FileSystemError> {
        let repair = repair && !self.read_only;
        let _one = self.scan_lock.lock().await;
        let pool = self.dev.pool();

        // Snapshot: commit first so everything written so far is in the tree
        // being walked (and missed writes are folded into `degraded`).
        let (root, misses_at_start, marks_at_start, since, hold) = {
            let mut tx = self.begin_tx().await;
            let tx = &mut tx;
            if !self.read_only {
                self.commit_locked(tx).await?;
            }
            let misses = self.miss_counts();
            let marks: Vec<u64> = (0..misses.len()).map(|d| self.degraded_mark(d)).collect();
            let degraded = self.degraded_disks();
            let since = match since {
                Some(s) => s,
                None => {
                    let online = |d: usize| pool.is_some_and(|p| p.disks().is_online(d));
                    match (0..degraded.len())
                        .filter(|&d| online(d))
                        .filter_map(|d| degraded[d])
                        .min()
                    {
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
            // Dropped with the scan, even a cancelled one.
            let hold = crate::fs::FreeHold::new(&self.scan_hold);
            (self.objects.root(), misses, marks, since, hold)
        };

        let mut report = ScanReport {
            since,
            repair,
            ..Default::default()
        };
        let mut walked = self.scan_tree(root, since, &|_| repair, &mut report).await;
        // Intent logs are outside the tree: a record a disk missed is on
        // fewer copies than the pool says until it's rewritten (audit D4).
        // Their space is held like the tree's (`scan_hold`), so a log
        // dropped meanwhile isn't reused under a repair.
        if walked.is_ok() {
            walked = match self.dataset_logs().await {
                Ok(logs) => {
                    self.scan_logs(&logs, &|_| repair, &mut report).await;
                    Ok(())
                }
                Err(e) => Err(e),
            };
        }
        {
            let tx = self.begin_tx().await;
            self.scan_labels(&tx, repair, &mut report).await;
        }
        drop(hold);
        self.release_held_frees().await;
        walked?;

        // A disk is cleared only once what was repaired on it is durable:
        // flushed, and no repair lost to a flush that failed (audit D1).
        if repair
            && report.is_clean()
            && let Some(pool) = pool
            && report.repaired() > 0
        {
            let flushed = self.dev.flush().await;
            if pool.disks().take_repairs_lost() || flushed.is_err() {
                report.errors.push(format!(
                    "repairs not made durable (a disk's flush failed): {}",
                    flushed
                        .err()
                        .map_or_else(|| "lost".to_string(), |e| e.to_string())
                ));
            }
        }
        if repair
            && report.certifies()
            && let Some(pool) = pool
        {
            // Under the tx: no commit can write a label or fold a miss in
            // between the label write and the clear (audit D4).
            let mut tx = self.begin_tx().await;
            let degraded = self.degraded_disks();
            for d in 0..pool.disks().len() {
                let covered = degraded[d].is_some_and(|t| t >= since);
                // Marked since the snapshot (a writer's report of a miss):
                // something the walk didn't see is missing.
                let in_sync = pool.disks().is_online(d)
                    && pool.disks().miss_count(d) == misses_at_start[d]
                    && self.degraded_mark(d) == marks_at_start[d]
                    && !report.shards.is_unreadable(d)
                    && !report.shards.has_write_failed(d);
                if !(covered && in_sync) {
                    continue;
                }
                if pool.disks().is_fenced(d) {
                    if let Err(e) = self.write_disk_label(d).await {
                        eprintln!("disk {}: label write failed: {}", d, e);
                        continue;
                    }
                    pool.disks().set_fenced(d, false);
                }
                if self.clear_degraded(&tx, d, marks_at_start[d], misses_at_start[d]) {
                    report.cleared.push(d);
                }
            }
            if !report.cleared.is_empty() {
                self.commit_locked(&mut tx).await?;
            }
        }
        Ok(report)
    }

    /// Walk the tree from `root`, checking records born at or after
    /// `since`; `repair(ptr)` says whether bad shards of that record may be
    /// rewritten.
    async fn scan_tree(
        &self,
        root: TreeRoot,
        since: Txg,
        repair: &(dyn Fn(&BlockPtr) -> bool + Send + Sync),
        report: &mut ScanReport,
    ) -> Result<(), FileSystemError> {
        let total = self.layout.metaslab_count * self.layout.metaslab_size_blocks;
        let mut stack: Vec<Node> = table_node(&root).into_iter().collect();
        // Snapshots share subtrees with their volumes: each record once.
        let mut seen: std::collections::HashSet<(u64, Txg)> = Default::default();

        while let Some(node) = stack.pop() {
            if !seen.insert((node.ptr.lba, node.ptr.birth_txg)) {
                continue;
            }
            // Only a committer's scan holds freed space (a writer's frees
            // are the committer's to release).
            if !self.read_only
                && self.writer.is_none()
                && report.records % 256 == 255
                && self.allocatable_blocks().await < total / MIN_FREE_FRACTION
            {
                report.aborted = Some("pool almost full (scan holds freed space)".into());
                return Ok(());
            }
            // Copy-on-write: nothing under a record is newer than it.
            if node.ptr.birth_txg < since {
                report.pruned += 1;
                continue;
            }
            let Some(data) = self.scan_record(&node.ptr, repair(&node.ptr), report).await else {
                report.errors.push(format!(
                    "{} ({:?}): record at {} unreadable",
                    node.owner, node.kind, node.ptr.lba
                ));
                continue;
            };
            if node.kind.is_metadata() {
                match crate::fsck::expand(&node, &data) {
                    Ok((children, _)) => stack.extend(children),
                    Err(e) => report.errors.push(e),
                }
            }
        }
        Ok(())
    }

    /// Check every record of the intent logs `logs` that replay would read,
    /// as `scan_record` checks a tree record: read verified (rebuilt from
    /// parity if a copy is bad), then every shard against it, rewriting
    /// those `repair(lba)` allows. A record that can't be read verified is
    /// the end of its log, as for replay: not an error.
    async fn scan_logs(
        &self,
        logs: &[(DatasetName, crate::inode::DatasetLog)],
        repair: &(dyn Fn(u64) -> bool + Send + Sync),
        report: &mut ScanReport,
    ) {
        let Some(pool) = self.dev.pool() else {
            return;
        };
        for (name, log) in logs {
            for (i, region) in log.regions.iter().enumerate() {
                let from = if i == 0 { log.from_seq } else { 0 };
                let records = match self.region_records(region, from).await {
                    Ok(records) => records,
                    Err(e) => {
                        report.errors.push(format!(
                            "volume '{}': intent log region at {}: {}",
                            name, region.start, e
                        ));
                        continue;
                    }
                };
                for rec in records {
                    let blocks = self.align_blocks(rec.bytes.len().div_ceil(BLOCK_SIZE) as u64);
                    let mut padded = vec![0u8; blocks as usize * BLOCK_SIZE];
                    padded[..rec.bytes.len()].copy_from_slice(&rec.bytes);
                    report.log_records += 1;
                    match pool
                        .root()
                        .verify_blocks(rec.lba, &padded, repair(rec.lba))
                        .await
                    {
                        Ok(check) => report.shards.add(&check),
                        Err(e) => report.errors.push(format!(
                            "volume '{}': intent log record at {}: {}",
                            name, rec.lba, e
                        )),
                    }
                }
            }
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
            let blocks = self.align_blocks(ptr.data_blocks());
            let mut padded = vec![0u8; blocks as usize * BLOCK_SIZE];
            padded[..data.len()].copy_from_slice(&data);
            match pool.root().verify_blocks(ptr.lba, &padded, repair).await {
                Ok(check) => report.shards.add(&check),
                Err(e) => {
                    report.errors.push(format!("record at {}: {}", ptr.lba, e));
                }
            }
        }
        Some(data)
    }

    /// On a writer: rebuild what degraded disks missed in the metaslabs lent
    /// to it. The committer's resilver can't write there (its writes are
    /// fenced out while they're lent; a running volume's intent log keeps
    /// one lent for as long as it runs), so the writer does, under its own
    /// fencing epoch. Committed blocks in lent space can't be reallocated
    /// meanwhile (only this writer allocates there, and only what it was
    /// lent as free). Once they're fixed, the committer's next resilver finds
    /// them good and clears the disk. None: nothing to do.
    pub async fn heal_lent(&self) -> Result<Option<ScanReport>, FileSystemError> {
        let (Some(w), Some(pool)) = (self.writer.clone(), self.dev.pool()) else {
            return Ok(None);
        };
        let lent: std::collections::HashSet<crate::types::MetaslabIdx> =
            w.lent.lock().unwrap().iter().copied().collect();
        if lent.is_empty() {
            return Ok(None);
        }
        let ring = self.read_uberblock_ring().await;
        let Some(newest) = ring.first() else {
            return Ok(None);
        };
        let Some(since) = (0..newest.degraded.len())
            .filter(|&d| pool.disks().is_online(d))
            .filter_map(|d| newest.degraded[d])
            .min()
        else {
            return Ok(None);
        };
        let _one = self.scan_lock.lock().await;
        let layout = self.layout;
        let mut report = ScanReport {
            since,
            repair: true,
            ..Default::default()
        };
        {
            // As in a scrub: nothing walked may be freed and reused under a
            // repair. Here that means the lent metaslabs stay lent until the
            // walk ends (audit D5a): only those still lent now are repaired.
            let hold = crate::fs::RepairHold::pin_lent(&w, lent, false);
            let pinned: std::collections::HashSet<_> = hold
                .as_ref()
                .map_or(&[][..], |h| h.pinned())
                .iter()
                .copied()
                .collect();
            let in_lent = move |lba: u64| {
                layout
                    .metaslab_for_block(lba)
                    .is_some_and(|i| pinned.contains(&i))
            };
            self.scan_tree(
                newest.objects.clone(),
                since,
                &|p: &BlockPtr| in_lent(p.lba),
                &mut report,
            )
            .await?;
            // And the logs of the volumes it runs: they're in its lent
            // space too, where only it can rewrite them (audit D4).
            let names: Vec<DatasetName> = w.datasets.lock().unwrap().keys().cloned().collect();
            let mut logs = Vec::new();
            for name in names {
                if let Ok(vol) = self.open_dataset(&name).await
                    && let Some(log) = vol.log.lock().unwrap().clone()
                {
                    logs.push((name, log));
                }
            }
            self.scan_logs(&logs, &in_lent, &mut report).await;
        }
        // The repairs are what lets the committer's resilver clear the disk;
        // make them durable before it can look.
        if report.repaired() > 0 {
            self.dev.flush().await?;
            if pool.disks().take_repairs_lost() {
                return Err(FileSystemError::Unavailable(
                    "repairs in lent space not made durable (a disk's flush failed)".into(),
                ));
            }
        }
        Ok(Some(report))
    }

    /// Verify the fixed label regions: superblock, uberblock ring and space
    /// map slots. Must run under the commit lock (commits rewrite them).
    async fn scan_labels(&self, _tx: &crate::tx::Tx, repair: bool, report: &mut ScanReport) {
        let Some(pool) = self.dev.pool() else {
            return;
        };
        // A label from past the last commit is a torn, uncommitted write:
        // never the truth, however many copies hold it.
        let committed = self.uberblock().await.map_or(0, |u| u.txg.get());
        let score = move |b: &[u8]| {
            parse_label(b)
                .ok()
                .map(|(txg, _)| txg.get())
                .filter(|&t| t <= committed)
        };
        // Degraded members missed label writes: their copies don't vote
        // (audit D3), they're what gets repaired. So may a disk with a
        // write failed since the snapshot, not yet folded into `degraded`.
        let stale: Vec<bool> = (0..pool.disks().len())
            .map(|d| pool.disks().may_lag(d))
            .collect();
        let header = (0, self.header.spacemap_start);
        let spacemaps = (
            self.layout.spacemap_start,
            self.layout.data_start - self.layout.spacemap_start,
        );
        for (lba, blocks) in [header, spacemaps] {
            match pool
                .root()
                .verify_labels(lba, blocks, repair, &score, &stale)
                .await
            {
                Ok(scan) => {
                    report.label_stripes += scan.units;
                    report.shards.add(&scan.check);
                    report.label_unrecoverable.extend(scan.unrecoverable);
                    report.label_stale_votes.extend(scan.stale_votes);
                }
                Err(e) => {
                    report
                        .errors
                        .push(format!("label blocks {}..{}: {}", lba, lba + blocks, e))
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/scan_tests.rs"]
mod tests;
