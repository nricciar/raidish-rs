//! The dataset intent log: how a writer acknowledges a flush of a tree
//! dataset (a volume's NBD FLUSH / NVMe FLUSH) without waiting for the
//! committer. It logs dirty records, whatever the dataset's format makes
//! of them, so it serves every tree dataset alike.
//!
//! A dataset a writer runs has a log of one or two *regions*: runs of blocks
//! in the writer's lent space, recorded in the volume's inode by a commit
//! (which also claims their space). A flush appends one record holding
//! every dirty record changed since the last flush, flushes the disks, and
//! is done. The committer only hears about the data when the writer next
//! *folds*: its regular volume commit (`writer_sync_dataset`), which builds
//! the tree as before and starts a new region.
//!
//! ```text
//!  region:  [hdr|entries][hdr|entries][hdr|entries] ...   (seq 0, 1, 2, ...)
//! ```
//!
//! A record is self-describing: a header (magic, the region's guid, its
//! sequence number, payload length and checksum, and a checksum of the
//! header itself), then the payload. Replay reads records in order until one
//! doesn't verify; parity rebuilds bad blocks on the way, as for any record.
//!
//! **Keyed datasets log operations, not records.** A `kv` put changes a
//! leaf (and maybe its ancestors, the header, a split's new node...),
//! whole records of a B+tree; logging them would cost a node or more per
//! acknowledged write. Instead each operation queues its effect
//! ([`KeyedOp`]) with its records, under the same lock, and a flush logs
//! the queued operations. Replay runs them through the B+tree again, over
//! the tree the log was registered with. The records of such a dataset
//! are never logged as records (a receive's are durable when the receive
//! commits), so a log never mixes record images with operations, which
//! would replay into a different tree than the one the images came from.
//!
//! **Flushes don't wait for folds.** A fold takes the dirty records at a cut:
//! the current region and its next sequence number `k`, read under the
//! cursor lock that log appends plan under, so each append is wholly before
//! the cut (its data is in what the fold commits) or after it. The fold
//! registers the log `[current from k, new region]`: what was appended
//! while the fold was in flight (a committer failover can take a while)
//! stays part of the log, and flushes carry on into the current region
//! until the fold returns. The next fold drops that region.
//!
//! **Only whole records are acknowledged.** A record some disk missed is
//! as safe as the disks that have it, and only its writer knows that disk
//! missed it; a replay after losing the writer's node (often holding a
//! disk too) could find it unreadable and take it for the end of the log.
//! So a flush whose records any disk missed commits instead: the commit
//! reports the miss (the disk is degraded from then on, which replay and
//! resilver both see) as it makes the writes durable. While a disk is
//! missing, flushes cost a commit.
//!
//! **Who replays.** A writer may only acknowledge a flush while its
//! `ds/<name>` lease is provably live (renewed well within its TTL). The
//! log is replayed by the committer once that lease is gone, so everything
//! acknowledged is already on disk when it reads the log: when a new writer
//! claims the volume (before opening it), or when the committer notices the
//! lease has lapsed (its reclaim loop). A lapsed lease can still be renewed
//! by its holder, so the committer takes it over first (a newer epoch) and
//! lets go of it once the log is replayed.

use crate::dataset::Dataset;
use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{DatasetLog, LogRegion};
use crate::object::{Dirty, Unlogged};
use crate::writer::WriterState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

const LOG_MAGIC: u64 = 0x5241_4944_5a49_4c31; // "RAIDZIL1"
/// Bytes at the start of a record that hold its header.
const HEADER_BYTES: usize = 128;
/// Preferred log region size: 16 MiB (at most a quarter of a metaslab).
pub const LOG_REGION_BLOCKS: u64 = 4096;
/// Ask for an early fold once a region is this full (percent), so its
/// successor is registered before it runs out.
pub const FOLD_AT_PERCENT: u64 = 75;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Header {
    magic: u64,
    guid: u64,
    seq: u64,
    payload_len: u64,
    payload_sum: [u8; 32],
}

/// One change to a volume record. Entries apply in log order, on top of
/// the tree the log was registered with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// The whole record.
    Data(u64, Vec<u8>),
    Hole(u64),
    /// Bytes at an offset into the record, over what it held before (the
    /// earlier entries, else the committed record, else zeros). Added after
    /// `Data` and `Hole`, so logs written before it still decode; a log
    /// with patches needs a replayer that knows them.
    Patch(u64, u32, Vec<u8>),
    /// A keyed operation, replayed through the dataset's format (a `kv`
    /// dataset's B+tree) rather than as the records it changed.
    Keyed(KeyedOp),
}

/// A change to a keyed dataset, as its intent log carries it: the effect
/// of an operation (what a conditional one did, not what it asked), so a
/// replay redoes exactly it, whatever the tree looks like underneath.
///
/// Each is a blind assignment to one key, so a run of them can be replayed
/// over a tree that already has some of them: those a fold committed but
/// whose log records follow its cut (see the module docs). Redoing a prefix
/// of the operations before the rest leaves every key as the last of them
/// set it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyedOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    /// Several, atomically (one transaction: replayed all or none).
    Batch(Vec<KeyedOp>),
}

/// A writer's place in a volume's log: the region it appends to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LogCursor {
    pub region: LogRegion,
    /// Blocks used so far.
    pub used: u64,
    /// The next record's sequence number.
    pub seq: u64,
}

fn encode(guid: u64, seq: u64, entries: &[Entry]) -> Result<Vec<u8>, FileSystemError> {
    let payload = bincode::serialize(entries)?;
    let header = bincode::serialize(&Header {
        magic: LOG_MAGIC,
        guid,
        seq,
        payload_len: payload.len() as u64,
        payload_sum: *blake3::hash(&payload).as_bytes(),
    })?;
    let mut out = vec![0u8; HEADER_BYTES];
    out[..header.len()].copy_from_slice(&header);
    out[header.len()..header.len() + 32].copy_from_slice(blake3::hash(&header).as_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// The header at the start of `data`, if it is record `seq` of region `guid`.
fn parse_header(data: &[u8], guid: u64, seq: u64) -> Option<Header> {
    if data.len() < HEADER_BYTES {
        return None;
    }
    let h: Header = bincode::deserialize(data).ok()?;
    let len = bincode::serialized_size(&h).ok()? as usize;
    if &data[len..len + 32] != blake3::hash(&data[..len]).as_bytes() {
        return None;
    }
    (h.magic == LOG_MAGIC && h.guid == guid && h.seq == seq).then_some(h)
}

/// A log append planned under the cursor lock, to be written.
struct Planned {
    vol: Arc<Dataset>,
    lba: u64,
    bytes: Vec<u8>,
    idxs: BTreeMap<u64, Unlogged>,
    ops: Vec<(u64, KeyedOp)>,
}

impl Planned {
    /// The append didn't happen (or may not have): its changes are unlogged
    /// again, so a later flush logs them (or a commit takes them).
    fn restore(self) {
        self.vol
            .obj
            .dirty
            .lock()
            .unwrap()
            .restore_unlogged(self.idxs, self.ops);
    }

    /// `restore`, and log nothing more into this region: its slot may be
    /// unwritten, and a record after a gap is never replayed.
    fn put_back(self) {
        *self.vol.cursor.lock().unwrap() = None;
        self.restore();
    }
}

/// Appends planned but not yet accounted for. Dropped while still holding
/// some (the flush's future was dropped mid-way), it puts them back.
struct Unaccounted(Vec<Planned>);

impl Unaccounted {
    /// Take them over: the caller accounts for each from now on.
    fn give_back(mut self) -> Vec<Planned> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for Unaccounted {
    fn drop(&mut self) {
        for p in self.0.drain(..) {
            p.put_back();
        }
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Size of a new log region, in blocks.
    pub(crate) fn log_region_blocks(&self) -> u64 {
        self.align_blocks(LOG_REGION_BLOCKS.min(self.layout.metaslab_size_blocks / 4))
    }

    /// Make every volume write completed before this call durable.
    ///
    /// On a writer, through the intent log: one record per changed volume
    /// and one disk flush, without the committer (and without waiting for a
    /// commit in progress). Where that can't be done (no log registered yet,
    /// it's full, a log write failed), it commits instead, like `sync`.
    /// Elsewhere it is `sync`.
    pub async fn flush(&self) -> Result<(), FileSystemError> {
        self.flush_scoped(None).await
    }

    /// `flush` for dataset `name` alone: other datasets' failures (a commit
    /// of theirs refused, their lease unconfirmed) don't fail it.
    pub async fn flush_for(&self, name: &str) -> Result<(), FileSystemError> {
        self.flush_scoped(Some(name)).await
    }

    async fn flush_scoped(&self, only: Option<&str>) -> Result<(), FileSystemError> {
        let Some(w) = self.writer.clone() else {
            return self.sync_scoped(only).await;
        };
        let mut span = crate::trace::Span::new("zil.flush");
        // One flush appends at a time: a record must never follow a gap.
        let io = self.log_io.lock().await;
        span.mark("lock");
        let vols: Vec<Arc<Dataset>> = self
            .open_datasets
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        // From here until each append is accounted for, a caller that gives
        // up (a dropped future: a gRPC client's deadline) mustn't leave
        // records taken but neither logged nor unlogged, nor a reserved
        // slot unwritten (see `Unaccounted`).
        let mut planned = Unaccounted(Vec::new());
        let mut commit = false;
        for vol in vols {
            match self.plan_append(&vol) {
                Ok(Some(p)) => planned.0.push(p),
                Ok(None) => {}
                Err(()) => {
                    let why = match vol.cursor.lock().unwrap().as_ref() {
                        None => "no_log",
                        Some(_) => "log_full",
                    };
                    span.note("fallback", why);
                    commit = true
                }
            }
        }
        span.note(
            "bytes",
            planned.0.iter().map(|p| p.bytes.len()).sum::<usize>(),
        );
        span.mark("plan");
        let mut failed = Vec::new();
        for (i, p) in planned.0.iter().enumerate() {
            if let Err(e) = self.write_record_blocks(p.lba, &p.bytes).await {
                eprintln!("volume '{}': intent log write failed: {}", p.vol.name, e);
                failed.push(i);
            }
        }
        if !failed.is_empty() {
            // Those slots are spent: later records would follow a gap.
            for i in failed.into_iter().rev() {
                planned.0.remove(i).put_back();
            }
            commit = true;
        }
        span.mark("log_write");
        if commit {
            drop(planned.give_back());
            drop(io);
            // Commits what was logged too.
            let r = self.sync_scoped(only).await;
            span.mark("commit");
            return r;
        }
        if planned.0.is_empty() {
            return Ok(());
        }
        let flushed = self.dev.flush().await;
        span.mark("dev_flush");
        if let Err(e) = flushed {
            // These records may or may not be on disk. One that isn't would
            // be a gap, hiding every later record from replay: log nothing
            // more here (flushes commit until the next fold starts a region).
            for p in planned.give_back() {
                p.put_back();
            }
            return Err(e.into());
        }
        // A disk that missed these records (unreachable, or failing) leaves
        // them only as safe as the disks that have them, and nobody but this
        // writer knows: the committer counts that disk healthy until a commit
        // reports the miss. Lose this node (and its disk) and a replay would
        // read a record it can't rebuild, take it for the end of the log and
        // drop what was acknowledged. So acknowledge only records every disk
        // has; otherwise commit, which reports the miss as it makes the
        // writes durable (the log records stay, harmless: replay redoes them).
        if self.unaccounted_misses().iter().any(|&m| m) {
            span.note("fallback", "missed");
            let vol = planned.0[0].vol.clone();
            for p in planned.give_back() {
                p.restore();
            }
            drop(io);
            let r = self.sync_scoped(only).await;
            span.mark("commit");
            // The commit reported the misses, unless nothing was left for it
            // to commit (a commit in progress took the records).
            let vol = only
                .and_then(|n| self.open_datasets.lock().unwrap().get(n).cloned())
                .unwrap_or(vol);
            let r = match r {
                Ok(()) => self.writer_report_misses(&w, &vol).await,
                e => e,
            };
            return r;
        }
        let written = planned.give_back();
        drop(io);
        if written.iter().any(|p| {
            p.vol
                .cursor
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|c| c.used * 100 >= c.region.len * FOLD_AT_PERCENT)
        }) {
            span.note("fold", "wanted");
            self.fold_wanted.raise();
        }
        // Acknowledge only under a lease nobody can have taken over yet (a
        // replay after it lapses then sees every acknowledged record).
        // A record that isn't acknowledged stays unlogged, so a retried
        // flush logs it again and checks the lease again (rather than
        // finding nothing to log and acknowledging it).
        let mut stale = None;
        for p in written {
            if !w.lease_fresh(&p.vol.name) {
                let name = p.vol.name.clone();
                p.restore();
                if only.is_none_or(|n| n == name) {
                    stale.get_or_insert(name);
                }
            }
        }
        match stale {
            Some(name) => Err(FileSystemError::Unavailable(format!(
                "can't confirm the lease on volume '{}'; flush not acknowledged",
                name
            ))),
            None => Ok(()),
        }
    }

    /// Take `vol`'s unlogged records and reserve the next slot of its log
    /// for them. `Ok(None)`: nothing to log; `Err`: no room or no log (the
    /// records stay unlogged; commit instead).
    fn plan_append(&self, vol: &Arc<Dataset>) -> Result<Option<Planned>, ()> {
        let mut cursor = vol.cursor.lock().unwrap();
        let mut d = vol.obj.dirty.lock().unwrap();
        if d.unlogged.is_empty() && d.ops.is_empty() {
            return Ok(None);
        }
        let Some(c) = cursor.as_mut() else {
            return Err(());
        };
        // Records a sync in progress took count too: until it commits,
        // only the log has them. A record mostly unchanged logs just the
        // bytes that changed.
        let mut entries: Vec<Entry> = Vec::new();
        for (&i, changed) in &d.unlogged {
            match (d.get(i), changed) {
                (Some(Dirty::Data(b)), Unlogged::Ranges(rs))
                    if changed.bytes().is_some_and(|n| n * 2 <= b.len()) =>
                {
                    entries.extend(
                        rs.iter()
                            .map(|&(s, e)| Entry::Patch(i, s, b[s as usize..e as usize].to_vec())),
                    );
                }
                (Some(Dirty::Data(b)), _) => entries.push(Entry::Data(i, b.clone())),
                (Some(Dirty::Hole), _) => entries.push(Entry::Hole(i)),
                (None, _) => {}
            }
        }
        // (Only keyed datasets queue operations, and their records are
        // never unlogged: one or the other.)
        debug_assert!(d.ops.is_empty() || entries.is_empty());
        entries.extend(d.ops.iter().map(|(_, op)| Entry::Keyed(op.clone())));
        let bytes = encode(c.region.guid, c.seq, &entries).map_err(|_| ())?;
        let blocks = self.align_blocks(bytes.len().div_ceil(BLOCK_SIZE) as u64);
        if c.used + blocks > c.region.len {
            return Err(());
        }
        let lba = c.region.start + c.used;
        c.used += blocks;
        c.seq += 1;
        let idxs = std::mem::take(&mut d.unlogged);
        let ops = std::mem::take(&mut d.ops);
        Ok(Some(Planned {
            vol: vol.clone(),
            lba,
            bytes,
            idxs,
            ops,
        }))
    }

    /// Allocate a log region in lent space (for a commit to register).
    pub(crate) async fn new_log_region(&self) -> Result<LogRegion, FileSystemError> {
        let e = self.allocate_contiguous(self.log_region_blocks()).await?;
        Ok(LogRegion {
            start: e.start,
            len: e.len,
            guid: crate::fs::new_guid(e.start),
        })
    }

    /// Record that `vol`'s registered log is now `log`, keeping the
    /// metaslabs its regions are in from being given back meanwhile.
    pub(crate) fn set_dataset_log(&self, w: &WriterState, vol: &Dataset, log: Option<DatasetLog>) {
        let ms = |r: &LogRegion| self.layout.metaslab_for_block(r.start);
        let mut cur = vol.log.lock().unwrap();
        for r in cur.iter().flat_map(|l| &l.regions) {
            if let Some(i) = ms(r) {
                w.unpin(i);
            }
        }
        for r in log.iter().flat_map(|l| &l.regions) {
            if let Some(i) = ms(r) {
                w.pin(i);
            }
        }
        *cur = log;
    }

    /// Append to `region` from now on (None: nowhere; flushes commit).
    pub(crate) fn set_cursor(&self, vol: &Dataset, region: Option<LogRegion>) {
        *vol.cursor.lock().unwrap() = region.map(|region| LogCursor {
            region,
            used: 0,
            seq: 0,
        });
    }

    // ---- the committer's side ----

    /// The intent log of every tree dataset that has one.
    pub async fn dataset_logs(&self) -> Result<Vec<(String, DatasetLog)>, FileSystemError> {
        let names: Vec<String> = self
            .datasets()
            .await?
            .datasets
            .into_iter()
            .filter(|(_, e)| e.format.is_tree())
            .map(|(n, _)| n)
            .collect();
        let mut out = Vec::new();
        for name in names {
            if let Ok(vol) = self.open_dataset(&name).await
                && let Some(log) = vol.log.lock().unwrap().clone()
            {
                out.push((name, log));
            }
        }
        Ok(out)
    }

    /// The records of `region` from `from_seq` on, in order, up to the first
    /// that doesn't verify (the end of the log, or a write that never
    /// completed).
    async fn read_region(&self, region: &LogRegion, from_seq: u64) -> Vec<Entry> {
        let unit = self.align_blocks(1);
        let mut out = Vec::new();
        let (mut used, mut seq) = (0u64, 0u64);
        while used + unit <= region.len {
            let lba = region.start + used;
            let check_header = move |d: &[u8]| parse_header(d, region.guid, seq).is_some();
            let Ok(head) = self
                .read_verified(lba, unit, HEADER_BYTES, &check_header, false)
                .await
            else {
                break;
            };
            let Some(h) = parse_header(&head, region.guid, seq) else {
                break;
            };
            let total = HEADER_BYTES + h.payload_len as usize;
            let blocks = self.align_blocks(total.div_ceil(BLOCK_SIZE) as u64);
            if used + blocks > region.len {
                break;
            }
            if seq >= from_seq {
                let sum = h.payload_sum;
                let check_record = move |d: &[u8]| {
                    d.len() == total && blake3::hash(&d[HEADER_BYTES..]).as_bytes() == &sum
                };
                let Ok(rec) = self
                    .read_verified(lba, blocks, total, &check_record, false)
                    .await
                else {
                    break;
                };
                let Ok(entries) = bincode::deserialize::<Vec<Entry>>(&rec[HEADER_BYTES..]) else {
                    break;
                };
                out.extend(entries);
            }
            used += blocks;
            seq += 1;
        }
        out
    }

    /// Apply volume `name`'s intent log `log` and commit, dropping the log
    /// (its writer's lease must be gone). Returns how many record writes it
    /// replayed. A no-op if the volume no longer has that log.
    pub async fn replay_dataset_log(
        &self,
        name: &str,
        log: &DatasetLog,
    ) -> Result<usize, FileSystemError> {
        // A record that can't be read looks just like the end of the log: on
        // a pool with more disks missing or stale (awaiting a resilver) than
        // parity can rebuild, replaying now would drop acknowledged writes.
        // Keep the log until it can be read.
        if self.dev.pool().is_some()
            && crate::inspect::margin_for_recent_writes(&self.pool_report().await) < 0
        {
            return Err(FileSystemError::Unavailable(format!(
                "too many disks missing or awaiting a resilver; keeping volume '{}''s \
                 intent log until it can be read",
                name
            )));
        }
        let _tx = self.begin_tx().await;
        let vol = self.open_dataset(name).await?;
        if vol.log.lock().unwrap().as_ref() != Some(log) {
            return Ok(0);
        }
        let mut entries = Vec::new();
        for (i, r) in log.regions.iter().enumerate() {
            let from = if i == 0 { log.from_seq } else { 0 };
            entries.extend(self.read_region(r, from).await);
        }
        let n = entries.len();
        let bs = vol.obj.blksz;
        // Keyed operations go through the tree, one at a time.
        let _ops = vol.ops.write().await;
        for e in entries {
            let (i, at, bytes) = match e {
                Entry::Keyed(op) => {
                    let mut t = crate::btree::Txn::begin(self, &vol.obj).await?;
                    t.redo(&op).await?;
                    t.apply(None)?;
                    continue;
                }
                Entry::Data(i, b) => {
                    vol.obj
                        .dirty
                        .lock()
                        .unwrap()
                        .records
                        .insert(i, Dirty::Data(b));
                    continue;
                }
                Entry::Hole(i) => {
                    vol.obj.dirty.lock().unwrap().records.insert(i, Dirty::Hole);
                    continue;
                }
                Entry::Patch(i, at, bytes) => (i, at as usize, bytes),
            };
            if at + bytes.len() > bs {
                return Err(FileSystemError::InvalidArgument(format!(
                    "volume '{}': intent log patch past the end of record {}",
                    name, i
                )));
            }
            // Over what the record holds so far: an earlier entry, else
            // the committed record, else zeros.
            let applied = {
                let mut d = vol.obj.dirty.lock().unwrap();
                match d.records.get_mut(&i) {
                    Some(Dirty::Data(buf)) => {
                        buf[at..at + bytes.len()].copy_from_slice(&bytes);
                        true
                    }
                    Some(Dirty::Hole) => {
                        let mut buf = vec![0u8; bs];
                        buf[at..at + bytes.len()].copy_from_slice(&bytes);
                        d.records.insert(i, Dirty::Data(buf));
                        true
                    }
                    None => false,
                }
            };
            if !applied {
                let mut buf = match self.read_committed_record(&vol.obj, i).await? {
                    Some(c) => c.as_ref().clone(),
                    None => vec![0u8; bs],
                };
                buf[at..at + bytes.len()].copy_from_slice(&bytes);
                vol.obj
                    .dirty
                    .lock()
                    .unwrap()
                    .records
                    .insert(i, Dirty::Data(buf));
            }
        }
        *vol.log.lock().unwrap() = None;
        if !vol.is_dirty() {
            // Nothing to commit for the tree: rewrite the dnode without the log.
            self.update_dataset_dnode(&vol, vol.obj.root_ptr()).await?;
        }
        for r in &log.regions {
            self.free(r.start, r.len).await?;
        }
        self.commit_locked().await?;
        println!(
            "volume '{}': replayed {} record write(s) from {}'s intent log",
            name, n, log.writer
        );
        Ok(n)
    }
}

/// The extent of a log region, as a commit's allocation.
pub(crate) fn region_alloc(r: &LogRegion) -> crate::types::Extent {
    crate::types::Extent::new(r.start, r.len)
}

#[cfg(test)]
#[path = "tests/zil_tests.rs"]
mod tests;
