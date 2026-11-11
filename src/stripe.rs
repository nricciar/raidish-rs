//! Stripe (RAID0) vdev over child vdevs (raidz groups, mirrors or single
//! disks).
//!
//! The address space is interleaved across the children in chunks of
//! `chunk` blocks: chunk `c` lives on child `c % n`, as that child's chunk
//! `c / n`. The chunk is a multiple of every child's width, so an aligned
//! record split at a chunk boundary is still aligned on both children.
//! Requests that cross a boundary are split. A stripe tolerates no more
//! failures than its weakest child.

use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, Candidates, DiskError};
use crate::raidz::RaidZError;
use crate::vdev::{LabelScan, LabelScore, StripeCheck, Vdev, lcm, max_record_blocks};

#[derive(Debug)]
pub struct Stripe {
    children: Vec<Box<dyn Vdev>>,
    width: u64,
    chunk: u64,
}

/// One piece of a request: `len` blocks at `local` on `child`, starting
/// `offset` blocks into the request.
struct Segment {
    child: usize,
    local: u64,
    offset: u64,
    len: u64,
}

impl Stripe {
    pub fn new(children: Vec<Box<dyn Vdev>>) -> Self {
        assert!(!children.is_empty());
        let width = children.iter().map(|c| c.width()).fold(1, lcm);
        Stripe {
            children,
            width,
            chunk: max_record_blocks(width),
        }
    }

    fn locate(&self, lba: BlockId) -> (usize, u64) {
        let n = self.children.len() as u64;
        let c = lba / self.chunk;
        ((c % n) as usize, (c / n) * self.chunk + lba % self.chunk)
    }

    /// Inverse of `locate`.
    fn global(&self, child: usize, local: u64) -> u64 {
        let n = self.children.len() as u64;
        ((local / self.chunk) * n + child as u64) * self.chunk + local % self.chunk
    }

    fn segments(&self, lba: BlockId, blocks: u64) -> Vec<Segment> {
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < blocks {
            let at = lba + offset;
            let len = (self.chunk - at % self.chunk).min(blocks - offset);
            let (child, local) = self.locate(at);
            out.push(Segment {
                child,
                local,
                offset,
                len,
            });
            offset += len;
        }
        out
    }
}

#[async_trait]
impl BlockDevice for Stripe {
    async fn flush(&self) -> Result<(), DiskError> {
        let results = futures::future::join_all(self.children.iter().map(|c| c.flush())).await;
        results.into_iter().find(Result::is_err).unwrap_or(Ok(()))
    }

    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let (child, local) = self.locate(lba);
        self.children[child].read_block(local, buf).await
    }

    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let (child, local) = self.locate(lba);
        self.children[child].write_block(local, buf).await
    }

    /// Every child contributes as many whole chunks as the smallest has.
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        let mut min = u64::MAX;
        for c in &self.children {
            min = min.min(c.capacity_blocks().await?);
        }
        Ok(min / self.chunk * self.chunk * self.children.len() as u64)
    }

    async fn read_blocks(&self, start: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        let segs = self.segments(start, (buf.len() / BLOCK_SIZE) as u64);
        let mut rest = buf;
        let mut reads = Vec::new();
        for s in &segs {
            let (part, tail) = rest.split_at_mut(s.len as usize * BLOCK_SIZE);
            rest = tail;
            reads.push(self.children[s.child].read_blocks(s.local, part));
        }
        futures::future::try_join_all(reads).await.map(|_| ())
    }

    async fn write_blocks(&self, start: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        let segs = self.segments(start, (buf.len() / BLOCK_SIZE) as u64);
        let writes = segs.iter().map(|s| {
            let at = s.offset as usize * BLOCK_SIZE;
            self.children[s.child].write_blocks(s.local, &buf[at..at + s.len as usize * BLOCK_SIZE])
        });
        futures::future::try_join_all(writes).await.map(|_| ())
    }

    async fn reconstruct_candidates(&self, lba: BlockId) -> Result<Candidates, DiskError> {
        let (child, local) = self.locate(lba);
        self.children[child].reconstruct_candidates(local).await
    }

    async fn repair_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        let (child, local) = self.locate(lba);
        self.children[child].repair_block(local, buf).await
    }
}

#[async_trait]
impl Vdev for Stripe {
    fn width(&self) -> u64 {
        self.width
    }

    async fn verify_blocks(
        &self,
        lba: BlockId,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError> {
        let mut check = StripeCheck::default();
        for s in self.segments(lba, (data.len() / BLOCK_SIZE) as u64) {
            let at = s.offset as usize * BLOCK_SIZE;
            let part = &data[at..at + s.len as usize * BLOCK_SIZE];
            check.add(
                &self.children[s.child]
                    .verify_blocks(s.local, part, repair)
                    .await?,
            );
        }
        Ok(check)
    }

    async fn verify_labels(
        &self,
        lba: BlockId,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
    ) -> Result<LabelScan, RaidZError> {
        let mut scan = LabelScan::default();
        for s in self.segments(lba, blocks) {
            let mut part = self.children[s.child]
                .verify_labels(s.local, s.len, repair, score)
                .await?;
            for b in part.unrecoverable.iter_mut() {
                *b = self.global(s.child, *b);
            }
            scan.add(part);
        }
        Ok(scan)
    }
}
