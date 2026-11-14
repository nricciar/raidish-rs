//! The volume intent log: how a writer acknowledges a volume flush (NBD
//! FLUSH, NVMe FLUSH) without waiting for the committer.
//!
//! A volume a writer runs has a log of one or two *regions*: runs of blocks
//! in the writer's lent space, recorded in the volume's inode by a commit
//! (which also claims their space). A flush appends one record holding
//! every dirty record changed since the last flush, flushes the disks, and
//! is done. The committer only hears about the data when the writer next
//! *folds*: its regular volume commit (`writer_sync_volume`), which builds
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
//! **Flushes don't wait for folds.** A fold takes the dirty records at a cut:
//! the current region and its next sequence number `k`, read under the
//! cursor lock that log appends plan under, so each append is wholly before
//! the cut (its data is in what the fold commits) or after it. The fold
//! registers the log `[current from k, new region]`: what was appended
//! while the fold was in flight (a committer failover can take a while)
//! stays part of the log, and flushes carry on into the current region
//! until the fold returns. The next fold drops that region.
//!
//! **Who replays.** A writer may only acknowledge a flush while its
//! `vol/<name>` lease is provably live (renewed well within its TTL). The
//! log is replayed by the committer once that lease is gone, so everything
//! acknowledged is already on disk when it reads the log: when a new writer
//! claims the volume (before opening it), or when the committer notices the
//! lease has lapsed (its reclaim loop). A lapsed lease can still be renewed
//! by its holder, so the committer takes it over first (a newer epoch) and
//! lets go of it once the log is replayed.

use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{LogRegion, VolumeLog};
use crate::object::Dirty;
use crate::volume::Volume;
use crate::writer::WriterState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

const LOG_MAGIC: u64 = 0x5241_4944_5a49_4c31; // "RAIDZIL1"
/// Bytes at the start of a record that hold its header.
const HEADER_BYTES: usize = 128;
/// Preferred log region size: 16 MiB (at most a quarter of a metaslab).
pub const LOG_REGION_BLOCKS: u64 = 4096;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Header {
    magic: u64,
    guid: u64,
    seq: u64,
    payload_len: u64,
    payload_sum: [u8; 32],
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum Entry {
    Data(u64, Vec<u8>),
    Hole(u64),
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
    vol: Arc<Volume>,
    lba: u64,
    bytes: Vec<u8>,
    idxs: BTreeSet<u64>,
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
        let Some(w) = self.writer.clone() else {
            return self.sync().await;
        };
        // One flush appends at a time: a record must never follow a gap.
        let io = self.log_io.lock().await;
        let vols: Vec<Arc<Volume>> = self.volumes.lock().unwrap().values().cloned().collect();
        let mut planned: Vec<Planned> = Vec::new();
        let mut commit = false;
        for vol in vols {
            match self.plan_append(&vol) {
                Ok(Some(p)) => planned.push(p),
                Ok(None) => {}
                Err(()) => commit = true,
            }
        }
        let mut written: Vec<Planned> = Vec::new();
        for p in planned {
            match self.write_record_blocks(p.lba, &p.bytes).await {
                Ok(()) => written.push(p),
                Err(e) => {
                    eprintln!("volume '{}': intent log write failed: {}", p.vol.name, e);
                    // The slot is spent: later records would follow a gap.
                    *p.vol.cursor.lock().unwrap() = None;
                    p.vol.obj.dirty.lock().unwrap().unlogged.extend(p.idxs);
                    commit = true;
                }
            }
        }
        if commit {
            drop(io);
            // Commits what was logged too.
            return self.sync().await;
        }
        if written.is_empty() {
            return Ok(());
        }
        if let Err(e) = self.dev.flush().await {
            for p in written {
                // These records may or may not be on disk. One that isn't
                // would be a gap, hiding every later record from replay: log
                // nothing more here (flushes commit until the next fold
                // starts a region).
                *p.vol.cursor.lock().unwrap() = None;
                p.vol.obj.dirty.lock().unwrap().unlogged.extend(p.idxs);
            }
            return Err(e.into());
        }
        drop(io);
        // Acknowledge only under a lease nobody can have taken over yet (a
        // replay after it lapses then sees every acknowledged record).
        // A record that isn't acknowledged stays unlogged, so a retried
        // flush logs it again and checks the lease again (rather than
        // finding nothing to log and acknowledging it).
        let mut stale = None;
        for p in written {
            if !w.lease_fresh(&p.vol.name) {
                p.vol.obj.dirty.lock().unwrap().unlogged.extend(p.idxs);
                stale.get_or_insert(p.vol.name.clone());
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
    fn plan_append(&self, vol: &Arc<Volume>) -> Result<Option<Planned>, ()> {
        let mut cursor = vol.cursor.lock().unwrap();
        let mut d = vol.obj.dirty.lock().unwrap();
        if d.unlogged.is_empty() {
            return Ok(None);
        }
        let Some(c) = cursor.as_mut() else {
            return Err(());
        };
        // Records a sync in progress took count too: until it commits,
        // only the log has them.
        let entries: Vec<Entry> = d
            .unlogged
            .iter()
            .filter_map(|&i| {
                d.get(i).map(|r| match r {
                    Dirty::Data(b) => Entry::Data(i, b.clone()),
                    Dirty::Hole => Entry::Hole(i),
                })
            })
            .collect();
        let bytes = encode(c.region.guid, c.seq, &entries).map_err(|_| ())?;
        let blocks = self.align_blocks(bytes.len().div_ceil(BLOCK_SIZE) as u64);
        if c.used + blocks > c.region.len {
            return Err(());
        }
        let lba = c.region.start + c.used;
        c.used += blocks;
        c.seq += 1;
        let idxs = std::mem::take(&mut d.unlogged);
        Ok(Some(Planned {
            vol: vol.clone(),
            lba,
            bytes,
            idxs,
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
    pub(crate) fn set_volume_log(&self, w: &WriterState, vol: &Volume, log: Option<VolumeLog>) {
        let ms = |r: &LogRegion| self.layout.metaslab_for_block(r.start).map(|i| i as u32);
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
    pub(crate) fn set_cursor(&self, vol: &Volume, region: Option<LogRegion>) {
        *vol.cursor.lock().unwrap() = region.map(|region| LogCursor {
            region,
            used: 0,
            seq: 0,
        });
    }

    // ---- the committer's side ----

    /// The intent log of every volume in the root directory that has one.
    pub async fn volume_logs(&self) -> Result<Vec<(String, VolumeLog)>, FileSystemError> {
        let names: Vec<String> = self
            .ls(std::path::Path::new(""))
            .await?
            .files
            .into_keys()
            .collect();
        let mut out = Vec::new();
        for name in names {
            if let Ok(vol) = self.open_volume(&name).await
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
    pub async fn replay_volume_log(
        &self,
        name: &str,
        log: &VolumeLog,
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
        let vol = self.open_volume(name).await?;
        if vol.log.lock().unwrap().as_ref() != Some(log) {
            return Ok(0);
        }
        let mut entries = Vec::new();
        for (i, r) in log.regions.iter().enumerate() {
            let from = if i == 0 { log.from_seq } else { 0 };
            entries.extend(self.read_region(r, from).await);
        }
        let n = entries.len();
        {
            let mut d = vol.obj.dirty.lock().unwrap();
            for e in entries {
                match e {
                    Entry::Data(i, b) => d.records.insert(i, Dirty::Data(b)),
                    Entry::Hole(i) => d.records.insert(i, Dirty::Hole),
                };
            }
        }
        *vol.log.lock().unwrap() = None;
        if !vol.is_dirty() {
            // Nothing to commit for the tree: rewrite the dnode without the log.
            self.update_volume_dnode(&vol, vol.obj.root_ptr()).await?;
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
pub(crate) fn region_alloc(r: &LogRegion) -> (u64, u64) {
    (r.start, r.len)
}

#[cfg(test)]
#[path = "tests/zil_tests.rs"]
mod tests;
