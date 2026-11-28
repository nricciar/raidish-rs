//! The disk server's served disks, behind its fence (audit R1a/b/c, D5b).
//!
//! The served disks live only in here: a request reaches a disk through
//! [`FencedDisks`], which checks it in one place:
//!
//! * Shape (R1a): a [`ValidatedWrite`] or [`ValidatedRead`] is the only way
//!   to name blocks. Building one checks that the disk is local and served,
//!   that `1 <= count <= WS_MAX_BLOCKS`, that the range doesn't wrap (in
//!   blocks or in bytes: it is a [`RawBlockRange`]), and
//!   (for a write) that the data is exactly `count` blocks, before anything
//!   is admitted (an admitted request raises floors).
//! * Readiness (R1b): writes wait for the fence's startup barrier
//!   ([`crate::fence::Ready`]).
//! * Order (R1c): a write takes its blocks' stripe locks, then is admitted,
//!   then written, all under the locks. An older epoch's write admitted
//!   before a newer one's lands before it, or is refused once the newer one
//!   is admitted; it can't land after.
//! * Reads after writes: a stamped read takes the same stripe locks,
//!   shared, before it's admitted. A write already admitted lands before
//!   the read sees the blocks; once the read's stamp is admitted, an older
//!   epoch's write arriving later is refused. A new committer's first read
//!   of a label can't come back before an old committer's write to it,
//!   already admitted, lands. Reads don't wait on each other.
//! * Expiry (D5b): under the locks, just before it's admitted, a write whose
//!   client has given up on it (its connection closed, or its deadline
//!   passed) is refused instead of landing later.

use crate::coords::{RawBlockRange, RawDiskBlock, ServedDiskIdx};
use crate::disk::{Block, RawDisk};
use crate::disks::Caddy;
use crate::fence::{Admit, Ready, ServerFence, Stamp};
use crate::websocket::{WS_MAX_BLOCKS, WsStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::Instant;

/// Blocks per stripe lock: a request (at most `WS_MAX_BLOCKS`) spans at
/// most two stripes.
const STRIPE_BLOCKS: u64 = WS_MAX_BLOCKS as u64;
/// Stripe locks per disk (stripes further apart share one).
const STRIPES: usize = 64;

/// What a request is still wanted for: refused once its connection closes
/// or its deadline passes.
#[derive(Clone, Default)]
pub struct Expiry {
    deadline: Option<Instant>,
    closed: Option<Arc<AtomicBool>>,
}

impl Expiry {
    /// Wanted until it's done (an HTTP request, a test).
    pub fn never() -> Self {
        Expiry::default()
    }

    /// Wanted until `deadline` or until `closed` is set.
    pub fn new(deadline: Option<Instant>, closed: Option<Arc<AtomicBool>>) -> Self {
        Expiry { deadline, closed }
    }

    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
            || self
                .closed
                .as_ref()
                .is_some_and(|c| c.load(Ordering::SeqCst))
    }
}

/// A write of a valid shape to a served disk (see the module docs).
pub struct ValidatedWrite<'a> {
    disk: ServedDiskIdx,
    range: RawBlockRange,
    data: &'a [Block],
}

/// A read of a valid shape from a served disk.
pub struct ValidatedRead {
    disk: ServedDiskIdx,
    range: RawBlockRange,
}

pub struct FencedDisks {
    disks: Arc<Vec<Caddy>>,
    floor: Arc<ServerFence>,
    /// Per served disk, its stripe locks: writes exclusive, stamped reads
    /// shared.
    stripes: Vec<Vec<RwLock<()>>>,
    /// Per served disk, its capacity in blocks as last seen (0: not yet).
    capacity: Vec<AtomicU64>,
}

impl FencedDisks {
    pub fn new(disks: Arc<Vec<Caddy>>, floor: Arc<ServerFence>) -> Self {
        let stripes = disks
            .iter()
            .map(|_| (0..STRIPES).map(|_| RwLock::new(())).collect())
            .collect();
        let capacity = disks.iter().map(|_| AtomicU64::new(0)).collect();
        FencedDisks {
            disks,
            floor,
            stripes,
            capacity,
        }
    }

    #[cfg(test)]
    pub fn floor(&self) -> &Arc<ServerFence> {
        &self.floor
    }

    /// How many disks are served (local or not).
    pub fn disk_count(&self) -> usize {
        self.disks.len()
    }

    /// Whether served disk `disk` is this node's (only those are served).
    pub fn is_local(&self, disk: usize) -> bool {
        self.disks.get(disk).is_some_and(|d| d.is_local())
    }

    /// Served disk `disk`, if it is local.
    fn served(&self, disk: usize) -> Result<ServedDiskIdx, WsStatus> {
        let d = u16::try_from(disk).map_err(|_| WsStatus::ErrorNotFound)?;
        self.local(disk)?;
        Ok(ServedDiskIdx::new(d))
    }

    fn local(&self, disk: usize) -> Result<&Caddy, WsStatus> {
        self.disks
            .get(disk)
            .filter(|d| d.is_local())
            .ok_or(WsStatus::ErrorNotFound)
    }

    /// The request's shape. The range must be addressable in bytes, not
    /// just in blocks (I-9): a block number whose byte offset wraps would
    /// be fenced, and stripe-locked, as one block and land on another.
    fn check_range(
        &self,
        disk: usize,
        block: u64,
        count: u32,
    ) -> Result<(ServedDiskIdx, RawBlockRange), WsStatus> {
        let disk = self.served(disk)?;
        if count == 0 || count > WS_MAX_BLOCKS {
            return Err(WsStatus::InvalidRequest);
        }
        let range = RawDiskBlock::new(block)
            .range(count)
            .ok_or(WsStatus::InvalidRequest)?;
        Ok((disk, range))
    }

    /// Whether `count` blocks from `block` are on the disk. The capacity is
    /// cached, and asked again only for a range past it, so a disk that
    /// grew is seen and the common case costs nothing. Without this, a
    /// write past the end of a file disk grows the file, and a read past it
    /// returns zeros, both outside anything the fence maps.
    async fn check_capacity(
        &self,
        disk: ServedDiskIdx,
        range: RawBlockRange,
    ) -> Result<(), WsStatus> {
        let end = range.end().get();
        let seen = &self.capacity[disk.as_usize()];
        if end <= seen.load(Ordering::Relaxed) {
            return Ok(());
        }
        let cap = self.capacity_blocks(disk.as_usize()).await?;
        seen.fetch_max(cap, Ordering::Relaxed);
        if end > cap {
            return Err(WsStatus::InvalidRequest);
        }
        Ok(())
    }

    pub fn validate_read(
        &self,
        disk: usize,
        block: u64,
        count: u32,
    ) -> Result<ValidatedRead, WsStatus> {
        let (disk, range) = self.check_range(disk, block, count)?;
        Ok(ValidatedRead { disk, range })
    }

    /// `data`: the raw payload, exactly `count` blocks.
    pub fn validate_write<'a>(
        &self,
        disk: usize,
        block: u64,
        count: u32,
        data: &'a [u8],
    ) -> Result<ValidatedWrite<'a>, WsStatus> {
        let (disk, range) = self.check_range(disk, block, count)?;
        let data = crate::disk::as_blocks(data).map_err(|_| WsStatus::InvalidRequest)?;
        if data.len() != count as usize {
            return Err(WsStatus::InvalidRequest);
        }
        Ok(ValidatedWrite { disk, range, data })
    }

    /// Read `r`. With a stamp, it's admitted first (a pool stamp raises
    /// the pool floor: a new committer's first reads fence the old one out),
    /// under its blocks' stripe locks, shared: writes admitted before it
    /// have landed (see the module docs).
    pub async fn read(&self, r: ValidatedRead, stamp: Option<Stamp>) -> Result<Vec<u8>, WsStatus> {
        self.check_capacity(r.disk, r.range).await?;
        let _locked = match stamp {
            Some(stamp) => {
                let locked = self.lock_shared(r.disk, r.range).await;
                self.admit(r.disk, r.range, stamp, false)?;
                Some(locked)
            }
            None => None,
        };
        let mut buffer = crate::disk::zeroed_blocks(r.range.count() as usize);
        self.disks[r.disk.as_usize()]
            .read_blocks(r.range.start(), &mut buffer)
            .await?;
        Ok(buffer.into_flattened())
    }

    /// Write `w` (see the module docs).
    pub async fn write(
        &self,
        w: ValidatedWrite<'_>,
        stamp: Stamp,
        expiry: &Expiry,
    ) -> Result<(), WsStatus> {
        if self.floor.readiness() != Ready::Ready {
            return Err(WsStatus::NotReady);
        }
        self.check_capacity(w.disk, w.range).await?;
        let _locked = self.lock(w.disk, w.range).await;
        // From here to the write starting: no await.
        if expiry.is_expired() {
            return Err(WsStatus::Expired);
        }
        self.admit(w.disk, w.range, stamp, true)?;
        self.disks[w.disk.as_usize()]
            .write_blocks(w.range.start(), w.data)
            .await?;
        Ok(())
    }

    pub async fn flush(&self, disk: usize) -> Result<(), WsStatus> {
        self.local(disk)?.flush().await?;
        Ok(())
    }

    pub async fn capacity_blocks(&self, disk: usize) -> Result<u64, WsStatus> {
        Ok(self.local(disk)?.capacity_blocks().await?)
    }

    fn admit(
        &self,
        disk: ServedDiskIdx,
        range: RawBlockRange,
        stamp: Stamp,
        write: bool,
    ) -> Result<(), WsStatus> {
        match self
            .floor
            .admit_stamp(disk, range.start(), range.count(), stamp, write)
        {
            Admit::Yes => Ok(()),
            Admit::Stale => Err(WsStatus::StaleEpoch),
            Admit::Fenced => Err(WsStatus::Fenced),
        }
    }

    /// The stripe locks over `range`, in index order (so two requests
    /// never wait on each other's).
    fn stripe_indexes(range: RawBlockRange) -> Vec<usize> {
        let first = range.start().get() / STRIPE_BLOCKS;
        let last = (range.end().get() - 1) / STRIPE_BLOCKS;
        let mut idx: Vec<usize> = (first..=last)
            .map(|s| (s % STRIPES as u64) as usize)
            .collect();
        idx.sort_unstable();
        idx.dedup();
        idx
    }

    /// The stripe locks over a write, exclusive.
    async fn lock(
        &self,
        disk: ServedDiskIdx,
        range: RawBlockRange,
    ) -> Vec<RwLockWriteGuard<'_, ()>> {
        let mut out = Vec::new();
        for i in Self::stripe_indexes(range) {
            out.push(self.stripes[disk.as_usize()][i].write().await);
        }
        out
    }

    /// The stripe locks over a stamped read, shared.
    async fn lock_shared(
        &self,
        disk: ServedDiskIdx,
        range: RawBlockRange,
    ) -> Vec<RwLockReadGuard<'_, ()>> {
        let mut out = Vec::new();
        for i in Self::stripe_indexes(range) {
            out.push(self.stripes[disk.as_usize()][i].read().await);
        }
        out
    }
}

#[cfg(test)]
#[path = "tests/fenced_disks_tests.rs"]
mod tests;
