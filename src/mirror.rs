//! N-way mirror vdev (a one-member mirror is a plain disk, for stripes).
//!
//! Every member holds the full copy: block `lba` is shard `lba` of each
//! member. Reads go to one member, rotating every `READ_REGION` blocks to
//! spread load, and fail over to the others. Writes go to all members and
//! succeed if any copy lands; the `DiskSet` counts the misses, so the pool
//! knows which members need a resilver. A flush succeeds only if every
//! write since the last one is then durable on a member that took it.

use crate::disk::{BLOCK_SIZE, Block, BlockDevice, BlockId, Candidates, DiskError, zeroed_blocks};
use crate::disks::{DiskSet, WriteAcks, fenced_any, ok_mask, read_or_fenced};
use crate::raidz::RaidZError;
use crate::vdev::{LabelScan, LabelScore, StripeCheck, Vdev};
use async_trait::async_trait;
use std::sync::Arc;

/// Reads of a region this many blocks long prefer the same member.
const READ_REGION: u64 = 64;

#[derive(Debug)]
pub struct Mirror {
    disks: Arc<DiskSet>,
    members: Vec<usize>,
    acks: WriteAcks,
}

impl Mirror {
    pub fn new(disks: Arc<DiskSet>, members: Vec<usize>) -> Self {
        assert!(!members.is_empty());
        Mirror {
            disks,
            members,
            acks: WriteAcks::new(1),
        }
    }

    /// Members in the order reads of `lba` try them.
    fn read_order(&self, lba: BlockId) -> impl Iterator<Item = usize> + '_ {
        let n = self.members.len();
        let start = (lba / READ_REGION) as usize % n;
        (0..n).map(move |i| self.members[(start + i) % n])
    }

    /// The member normal reads of `lba` are served from.
    fn primary(&self, lba: BlockId) -> usize {
        self.read_order(lba)
            .find(|&d| self.disks.is_readable(d))
            .unwrap_or_else(|| self.read_order(lba).next().unwrap())
    }

    async fn read_run(&self, lba: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        let order: Vec<usize> = {
            let mut v: Vec<usize> = self.read_order(lba).collect();
            // Known-good members first; the rest only as a last resort.
            v.sort_by_key(|&d| !self.disks.is_readable(d));
            v
        };
        let mut last = DiskError::UnhealthyDisk;
        for d in order {
            match self.disks.read_shards(d, lba, buf).await {
                Ok(()) => return Ok(()),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Read every member's copy of `n` blocks at `lba` (fenced ones too).
    /// Every member's copy, `None` where unreadable (a fencing refusal
    /// fails it all, see `read_or_fenced`).
    async fn read_all_raw(
        &self,
        lba: BlockId,
        n: usize,
    ) -> Result<Vec<Option<Vec<Block>>>, DiskError> {
        let reads = self.members.iter().map(|&d| async move {
            let mut run = zeroed_blocks(n);
            self.disks
                .read_shards_raw(d, lba, &mut run)
                .await
                .map(|_| run)
        });
        read_or_fenced(futures::future::join_all(reads).await)
    }
}

#[async_trait]
impl BlockDevice for Mirror {
    async fn flush(&self) -> Result<(), DiskError> {
        self.acks
            .flush(|| async {
                let flushes = self.members.iter().map(|&d| self.disks.flush_disk(d));
                let results = futures::future::join_all(flushes).await;
                if let Some(e) = fenced_any(&results) {
                    return Err(e);
                }
                if results.iter().any(|r| r.is_ok()) {
                    Ok(ok_mask(&self.members, &results))
                } else {
                    results.into_iter().find(Result::is_err).unwrap().map(|_| 0)
                }
            })
            .await
    }

    async fn read_block(&self, lba: BlockId, buf: &mut Block) -> Result<(), DiskError> {
        self.read_run(lba, std::slice::from_mut(buf)).await
    }

    async fn write_block(&self, lba: BlockId, buf: &Block) -> Result<(), DiskError> {
        self.write_blocks(lba, std::slice::from_ref(buf)).await
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        self.disks.shard_capacity(&self.members).await
    }

    /// Split at read-region boundaries so every block is read from the
    /// member `primary` names (the self-heal logic relies on that).
    async fn read_blocks(&self, start: BlockId, buf: &mut [Block]) -> Result<(), DiskError> {
        let mut lba = start;
        let mut rest = buf;
        while !rest.is_empty() {
            let n = (READ_REGION - lba % READ_REGION).min(rest.len() as u64);
            let (run, tail) = rest.split_at_mut(n as usize);
            self.read_run(lba, run).await?;
            rest = tail;
            lba += n;
        }
        Ok(())
    }

    async fn write_blocks(&self, start: BlockId, buf: &[Block]) -> Result<(), DiskError> {
        let writes = self
            .members
            .iter()
            .map(|&d| self.disks.write_shards(d, start, buf));
        let results = futures::future::join_all(writes).await;
        if let Some(e) = fenced_any(&results) {
            return Err(e);
        }
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures == results.len() {
            return results.into_iter().find(Result::is_err).unwrap();
        }
        self.acks.record(ok_mask(&self.members, &results));
        if failures > 0 {
            eprintln!(
                "WARN: {} mirror member(s) failed writing blocks {}..{}",
                failures,
                start,
                start + buf.len() as u64
            );
        }
        Ok(())
    }

    /// `rebuilt[d]`: a copy from a member other than the primary and `d`
    /// (`rebuilt[primary]`: any other copy).
    async fn reconstruct_candidates(&self, lba: BlockId) -> Result<Candidates, DiskError> {
        let target = self.primary(lba);
        let reads = self
            .members
            .iter()
            .filter(|&&d| d != target)
            .map(|&d| async move {
                let mut b = [0u8; BLOCK_SIZE];
                self.disks
                    .read_shards(d, lba, std::slice::from_mut(&mut b))
                    .await
                    .ok()
                    .map(|_| (d, b.to_vec()))
            });
        let copies: Vec<(usize, Vec<u8>)> = futures::future::join_all(reads)
            .await
            .into_iter()
            .flatten()
            .collect();
        let rebuilt = (0..self.disks.len())
            .map(|exclude| {
                copies
                    .iter()
                    .find(|(d, _)| *d != exclude)
                    .map(|(_, b)| b.clone())
            })
            .collect();
        Ok(Candidates {
            disk: target,
            rebuilt,
        })
    }

    async fn repair_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        self.disks
            .repair_shards(self.primary(lba), lba, std::slice::from_ref(buf))
            .await
    }
}

#[async_trait]
impl Vdev for Mirror {
    fn width(&self) -> u64 {
        1
    }

    async fn verify_blocks(
        &self,
        lba: BlockId,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let n = data.len() / BLOCK_SIZE;
        let copies = self.read_all_raw(lba, n).await?;
        let mut check = StripeCheck::new(self.disks.len(), n as u64);
        for (i, &d) in self.members.iter().enumerate() {
            let Some(run) = &copies[i] else {
                check.unreadable[d] = true;
                continue;
            };
            let (wanted, _) = data.as_chunks::<BLOCK_SIZE>();
            for (j, (have, want)) in run.iter().zip(wanted).enumerate() {
                if have == want {
                    continue;
                }
                check.mismatched[d] += 1;
                if repair {
                    match self
                        .disks
                        .repair_shards(d, lba + j as u64, std::slice::from_ref(want))
                        .await
                    {
                        Ok(()) => check.repaired[d] += 1,
                        Err(e) if e.is_fencing() => return Err(e.into()),
                        Err(_) => check.write_failed[d] = true,
                    }
                }
            }
        }
        Ok(check)
    }

    /// Per block: if the copies disagree, the newest valid label wins (then
    /// the one held by the most members) and the others are rewritten. A
    /// label is only ever rewritten in a later TXG, so an older one is from a
    /// member that missed commits, however many such members agree on it
    /// (audit D3); `score` rejects labels newer than the last commit.
    async fn verify_labels(
        &self,
        lba: BlockId,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
        stale: &[bool],
    ) -> Result<LabelScan, RaidZError> {
        let is_stale = |i: usize| stale.get(self.members[i]).copied().unwrap_or(false);
        let mut scan = LabelScan {
            check: StripeCheck::new(self.disks.len(), 0),
            ..Default::default()
        };
        for b in lba..lba + blocks {
            scan.units += 1;
            let mut copies = self.read_all_raw(b, 1).await?;
            // A stale member's copy doesn't vote while a current one is here.
            if (0..copies.len()).any(|i| copies[i].is_some() && !is_stale(i)) {
                for (i, c) in copies.iter_mut().enumerate() {
                    if is_stale(i) {
                        *c = None;
                    }
                }
            }
            let present: Vec<&[u8]> = copies.iter().flatten().map(|c| c.as_flattened()).collect();
            if present.is_empty() {
                scan.unrecoverable.push(b);
                continue;
            }
            if present.len() == copies.len() && present.iter().all(|c| *c == present[0]) {
                scan.check.stripes += 1;
                continue;
            }
            // (txg, agreeing copies)
            let mut best: Option<((u64, usize), &[u8])> = None;
            for c in &present {
                let Some(txg) = score(c) else { continue };
                let agree = present.iter().filter(|o| **o == *c).count();
                if best.as_ref().is_none_or(|(r, _)| (txg, agree) > *r) {
                    best = Some(((txg, agree), c));
                }
            }
            match best {
                Some((_, truth)) => {
                    let truth = truth.to_vec();
                    scan.check
                        .add(&self.verify_blocks(b, &truth, repair).await?);
                }
                // Never written. A copy left out (a stale member's, an
                // unreadable one) may hold leftovers: zero it too.
                None if present.iter().all(|c| c.iter().all(|&x| x == 0)) => {
                    if present.len() < copies.len() {
                        scan.check
                            .add(&self.verify_blocks(b, &[0u8; BLOCK_SIZE], repair).await?);
                    } else {
                        scan.check.stripes += 1;
                    }
                }
                None => scan.unrecoverable.push(b),
            }
        }
        Ok(scan)
    }
}
