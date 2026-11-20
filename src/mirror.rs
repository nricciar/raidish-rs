//! N-way mirror vdev (a one-member mirror is a plain disk, for stripes).
//!
//! Every member holds the full copy: block `lba` is shard `lba` of each
//! member. Reads go to one member, rotating every `READ_REGION` blocks to
//! spread load, and fail over to the others. Writes go to all members and
//! succeed if any copy lands; the `DiskSet` counts the misses, so the pool
//! knows which members need a resilver.

use crate::disk::{BLOCK_SIZE, Block, BlockDevice, BlockId, Candidates, DiskError, zeroed_blocks};
use crate::disks::DiskSet;
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
}

impl Mirror {
    pub fn new(disks: Arc<DiskSet>, members: Vec<usize>) -> Self {
        assert!(!members.is_empty());
        Mirror { disks, members }
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
    async fn read_all_raw(&self, lba: BlockId, n: usize) -> Vec<Option<Vec<Block>>> {
        let reads = self.members.iter().map(|&d| async move {
            let mut run = zeroed_blocks(n);
            self.disks
                .read_shards_raw(d, lba, &mut run)
                .await
                .ok()
                .map(|_| run)
        });
        futures::future::join_all(reads).await
    }
}

#[async_trait]
impl BlockDevice for Mirror {
    async fn flush(&self) -> Result<(), DiskError> {
        let flushes = self.members.iter().map(|&d| self.disks.flush_disk(d));
        let results = futures::future::join_all(flushes).await;
        if results.iter().any(|r| r.is_ok()) {
            Ok(())
        } else {
            results.into_iter().find(Result::is_err).unwrap()
        }
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
        let failures = results.iter().filter(|r| r.is_err()).count();
        if failures == results.len() {
            return results.into_iter().find(Result::is_err).unwrap();
        }
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
            .write_shards(self.primary(lba), lba, std::slice::from_ref(buf))
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
        let copies = self.read_all_raw(lba, n).await;
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
                        .write_shards(d, lba + j as u64, std::slice::from_ref(want))
                        .await
                    {
                        Ok(()) => check.repaired[d] += 1,
                        Err(_) => check.write_failed[d] = true,
                    }
                }
            }
        }
        Ok(check)
    }

    /// Per block: if the copies disagree, the valid label held by the most
    /// members (then the newest) wins and the others are rewritten.
    async fn verify_labels(
        &self,
        lba: BlockId,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
    ) -> Result<LabelScan, RaidZError> {
        let mut scan = LabelScan {
            check: StripeCheck::new(self.disks.len(), 0),
            ..Default::default()
        };
        for b in lba..lba + blocks {
            scan.units += 1;
            let copies = self.read_all_raw(b, 1).await;
            let present: Vec<&[u8]> = copies.iter().flatten().map(|c| c.as_flattened()).collect();
            if present.is_empty() {
                scan.unrecoverable.push(b);
                continue;
            }
            if present.len() == copies.len() && present.iter().all(|c| *c == present[0]) {
                scan.check.stripes += 1;
                continue;
            }
            // (agreeing copies, txg)
            let mut best: Option<((usize, u64), &[u8])> = None;
            for c in &present {
                let Some(txg) = score(c) else { continue };
                let agree = present.iter().filter(|o| **o == *c).count();
                if best.as_ref().is_none_or(|(r, _)| (agree, txg) > *r) {
                    best = Some(((agree, txg), c));
                }
            }
            match best {
                Some((_, truth)) => {
                    let truth = truth.to_vec();
                    scan.check
                        .add(&self.verify_blocks(b, &truth, repair).await?);
                }
                None if present.iter().all(|c| c.iter().all(|&x| x == 0)) => {
                    scan.check.stripes += 1;
                }
                None => scan.unrecoverable.push(b),
            }
        }
        Ok(scan)
    }
}
