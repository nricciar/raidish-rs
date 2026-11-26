//! N-way mirror vdev (a one-member mirror is a plain disk, for stripes).
//!
//! Every member holds the full copy: block `lba` is shard `lba` of each
//! member. Reads go to one member, rotating every `READ_REGION` blocks to
//! spread load, and fail over to the others. Writes go to all members and
//! succeed if any copy lands; the `DiskSet` counts the misses, so the pool
//! knows which members need a resilver. A flush succeeds only if every
//! write since the last one is then durable on a member that took it.

use crate::coords::{PoolDiskIdx, VdevLba};
use crate::disk::{BLOCK_SIZE, Block, Candidates, DiskError, zeroed_blocks};
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
    members: Vec<PoolDiskIdx>,
    acks: WriteAcks,
}

impl Mirror {
    pub fn new(disks: Arc<DiskSet>, members: Vec<PoolDiskIdx>) -> Self {
        assert!(!members.is_empty());
        Mirror {
            disks,
            members,
            acks: WriteAcks::new(1),
        }
    }

    /// Members in the order reads of `lba` try them.
    fn read_order(&self, lba: VdevLba) -> impl Iterator<Item = PoolDiskIdx> + '_ {
        let n = self.members.len();
        let start = (lba.get() / READ_REGION) as usize % n;
        (0..n).map(move |i| self.members[(start + i) % n])
    }

    /// The member normal reads of `lba` are served from.
    fn primary(&self, lba: VdevLba) -> PoolDiskIdx {
        self.read_order(lba)
            .find(|&d| self.disks.is_readable(d))
            .unwrap_or_else(|| self.read_order(lba).next().unwrap())
    }

    async fn read_run(&self, lba: VdevLba, buf: &mut [Block]) -> Result<(), DiskError> {
        let order: Vec<PoolDiskIdx> = {
            let mut v: Vec<PoolDiskIdx> = self.read_order(lba).collect();
            // Known-good members first; the rest only as a last resort.
            v.sort_by_key(|&d| !self.disks.is_readable(d));
            v
        };
        let mut last = DiskError::UnhealthyDisk;
        for d in order {
            match self.disks.read_shards(d, lba.mirror_shard(), buf).await {
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
        lba: VdevLba,
        n: usize,
    ) -> Result<Vec<Option<Vec<Block>>>, DiskError> {
        let reads = self.members.iter().map(|&d| async move {
            let mut run = zeroed_blocks(n);
            self.disks
                .read_shards_raw(d, lba.mirror_shard(), &mut run)
                .await
                .map(|_| run)
        });
        read_or_fenced(futures::future::join_all(reads).await)
    }
}

#[async_trait]
impl Vdev for Mirror {
    fn width(&self) -> u64 {
        1
    }

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
                    results
                        .into_iter()
                        .find(Result::is_err)
                        .unwrap()
                        .map(|_| Default::default())
                }
            })
            .await
    }

    async fn read_block(&self, lba: VdevLba, buf: &mut Block) -> Result<(), DiskError> {
        self.read_run(lba, std::slice::from_mut(buf)).await
    }

    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        self.disks.shard_capacity(&self.members).await
    }

    /// Split at read-region boundaries so every block is read from the
    /// member `primary` names (the self-heal logic relies on that).
    async fn read_blocks(&self, start: VdevLba, buf: &mut [Block]) -> Result<(), DiskError> {
        let mut lba = start;
        let mut rest = buf;
        while !rest.is_empty() {
            let n = (READ_REGION - lba.get() % READ_REGION).min(rest.len() as u64);
            let (run, tail) = rest.split_at_mut(n as usize);
            self.read_run(lba, run).await?;
            rest = tail;
            lba += n;
        }
        Ok(())
    }

    async fn write_blocks(&self, start: VdevLba, buf: &[Block]) -> Result<(), DiskError> {
        let writes = self
            .members
            .iter()
            .map(|&d| self.disks.write_shards(d, start.mirror_shard(), buf));
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
    async fn reconstruct_candidates(&self, lba: VdevLba) -> Result<Candidates, DiskError> {
        let target = self.primary(lba);
        let reads = self
            .members
            .iter()
            .filter(|&&d| d != target)
            .map(|&d| async move {
                let mut b = [0u8; BLOCK_SIZE];
                self.disks
                    .read_shards(d, lba.mirror_shard(), std::slice::from_mut(&mut b))
                    .await
                    .ok()
                    .map(|_| (d, Box::new(b)))
            });
        let copies: Vec<(PoolDiskIdx, Box<Block>)> = futures::future::join_all(reads)
            .await
            .into_iter()
            .flatten()
            .collect();
        let rebuilt = self
            .disks
            .indexes()
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

    async fn repair_block(&self, lba: VdevLba, buf: &Block) -> Result<(), DiskError> {
        self.disks
            .repair_shards(
                self.primary(lba),
                lba.mirror_shard(),
                std::slice::from_ref(buf),
            )
            .await
    }

    async fn verify_blocks(
        &self,
        lba: VdevLba,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let n = data.len() / BLOCK_SIZE;
        let copies = self.read_all_raw(lba, n).await?;
        let mut check = StripeCheck::new(self.disks.len(), n as u64);
        for (i, &disk) in self.members.iter().enumerate() {
            let d = disk.as_usize();
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
                        .repair_shards(
                            disk,
                            (lba + j as u64).mirror_shard(),
                            std::slice::from_ref(want),
                        )
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
        lba: VdevLba,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
        stale: &[bool],
    ) -> Result<LabelScan, RaidZError> {
        let is_stale = |i: usize| {
            stale
                .get(self.members[i].as_usize())
                .copied()
                .unwrap_or(false)
        };
        let mut scan = LabelScan {
            check: StripeCheck::new(self.disks.len(), 0),
            ..Default::default()
        };
        for b in (0..blocks).map(|i| lba + i) {
            scan.units += 1;
            let mut copies = self.read_all_raw(b, 1).await?;
            // A stale member's copy doesn't vote while a current one is here.
            // Without one, the stale copies decide; with some member unread
            // (it may hold the newer copy) nothing is repaired from them and
            // nothing cleared (structural triage I-19).
            let mut repair = repair;
            if (0..copies.len()).any(|i| copies[i].is_some() && !is_stale(i)) {
                for (i, c) in copies.iter_mut().enumerate() {
                    if is_stale(i) {
                        *c = None;
                    }
                }
            } else if copies.iter().any(Option::is_none)
                && (0..copies.len()).any(|i| copies[i].is_some() && is_stale(i))
            {
                scan.stale_votes.push(b);
                repair = false;
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
