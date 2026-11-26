//! Block coordinates, typed by the address space they belong to.
//!
//! A pool has several, all of them block numbers, and code mixing them up
//! has had bugs that type-checked (a stripe child's address passed on as
//! the pool's, a raw disk block wrapping in bytes onto another):
//!
//! * [`PoolLba`]: what a `FileSystem` reads and writes (a `PoolDevice`).
//! * [`VdevLba`]: a vdev's own address space. The root vdev's is the
//!   pool's; a stripe child's is not (`Stripe` maps between them).
//! * [`StripeId`] and [`DiskShard`]: a raidz stripe, and the shard it is on
//!   each member: shard `s` of a member disk is the `s`th block after its
//!   label area. Mirrors use shards too (block `lba` is shard `lba`).
//! * [`RawDiskBlock`] and [`RawBlockRange`]: a block of one member disk,
//!   label area included: what a `RawDisk` reads and writes.
//!
//! And disk indexes, which count different things:
//!
//! * [`PoolDiskIdx`]: a leaf disk of the pool (its `DiskSet` position,
//!   labels, `degraded`, fencing); below [`MAX_DISKS`].
//! * [`MemberIdx`]: a position in one vdev's member list.
//! * [`ServedDiskIdx`]: a disk a disk server serves, in its own order.
//!
//! Conversions between them are explicit methods, never `From`.

use crate::disk::BLOCK_SIZE;
use crate::disks::{DISK_LABEL_BLOCKS, MAX_DISKS};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! block_number {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(
            Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(n: u64) -> Self {
                $name(n)
            }
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        /// `n` blocks further on.
        impl std::ops::Add<u64> for $name {
            type Output = $name;
            fn add(self, n: u64) -> $name {
                $name(self.0 + n)
            }
        }

        impl std::ops::AddAssign<u64> for $name {
            fn add_assign(&mut self, n: u64) {
                self.0 += n;
            }
        }

        /// How many blocks apart.
        impl std::ops::Sub for $name {
            type Output = u64;
            fn sub(self, o: $name) -> u64 {
                self.0 - o.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

block_number!(
    /// A block of the pool's address space: what block pointers point at.
    PoolLba
);
block_number!(
    /// A block of one vdev's address space (the pool's, for the root).
    VdevLba
);
block_number!(
    /// A raidz stripe: shard `s` of every member of its group.
    StripeId
);
block_number!(
    /// A shard of a member disk: its `s`th block after the label area.
    DiskShard
);
block_number!(
    /// A block of one member disk, label area included.
    RawDiskBlock
);

impl PoolLba {
    /// The root vdev's address of it: the same block.
    pub fn at_root(self) -> VdevLba {
        VdevLba(self.0)
    }
}

impl VdevLba {
    /// The pool's address of the root vdev's block `self`.
    pub fn root_to_pool(self) -> PoolLba {
        PoolLba(self.0)
    }

    /// The raidz stripe it falls in, for a group of `data` data shards, and
    /// its data shard position there.
    pub fn stripe_of(self, data: usize) -> (StripeId, usize) {
        let d = data as u64;
        (StripeId(self.0 / d), (self.0 % d) as usize)
    }

    /// The shard it is on every member of a mirror.
    pub fn mirror_shard(self) -> DiskShard {
        DiskShard(self.0)
    }
}

impl StripeId {
    /// Its first block, for a group of `data` data shards.
    pub fn first_lba(self, data: usize) -> VdevLba {
        VdevLba(self.0 * data as u64)
    }

    /// The shard it is on each member.
    pub fn shard(self) -> DiskShard {
        DiskShard(self.0)
    }
}

impl DiskShard {
    /// The member's raw block holding it; `None` past the end of a disk's
    /// address space (no shard there).
    pub fn to_raw(self) -> Option<RawDiskBlock> {
        self.0.checked_add(DISK_LABEL_BLOCKS).map(RawDiskBlock)
    }

    /// The mirror block it holds.
    pub fn mirror_lba(self) -> VdevLba {
        VdevLba(self.0)
    }
}

impl RawDiskBlock {
    /// The shard it holds; `None` for a label block.
    pub fn to_shard(self) -> Option<DiskShard> {
        self.0.checked_sub(DISK_LABEL_BLOCKS).map(DiskShard)
    }

    /// `count` blocks from here, if they end at a byte offset a file can
    /// address (`i64`). Checked at the end, not the start: `start *
    /// BLOCK_SIZE` must never be computed unchecked, since a wrapped offset
    /// lands on some other block (structural triage I-9).
    pub fn range(self, count: u32) -> Option<RawBlockRange> {
        let end = self
            .0
            .checked_add(count as u64)?
            .checked_mul(BLOCK_SIZE as u64)?;
        (end <= i64::MAX as u64).then_some(RawBlockRange { start: self, count })
    }
}

/// `count` raw blocks from `start`, addressable in bytes: built only by
/// [`RawDiskBlock::range`], so its byte offsets never wrap.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RawBlockRange {
    start: RawDiskBlock,
    count: u32,
}

impl RawBlockRange {
    pub fn start(&self) -> RawDiskBlock {
        self.start
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    /// One past its last block.
    pub fn end(&self) -> RawDiskBlock {
        RawDiskBlock(self.start.0 + self.count as u64)
    }

    pub fn byte_offset(&self) -> u64 {
        self.start.0 * BLOCK_SIZE as u64
    }

    pub fn byte_len(&self) -> u64 {
        self.count as u64 * BLOCK_SIZE as u64
    }
}

/// A leaf disk of the pool: its position in the pool's `DiskSet`, below
/// [`MAX_DISKS`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolDiskIdx(u8);

impl PoolDiskIdx {
    pub fn new(i: usize) -> Option<Self> {
        (i < MAX_DISKS).then_some(PoolDiskIdx(i as u8))
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// Every index below `n` (at most `MAX_DISKS`).
    pub fn all(n: usize) -> impl Iterator<Item = PoolDiskIdx> + Clone {
        (0..n.min(MAX_DISKS)).map(|i| PoolDiskIdx(i as u8))
    }
}

impl fmt::Display for PoolDiskIdx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A position in one vdev's member list (`0..members.len()`): a raidz
/// shard position, a mirror copy. Not a pool disk: in a stripe of groups,
/// member 0 of the second group is some other pool disk.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberIdx(pub usize);

/// A disk a disk server serves, in its own order (mapped to the pool's by
/// `ServerFence::set_served`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServedDiskIdx(u16);

impl ServedDiskIdx {
    pub fn new(i: u16) -> Self {
        ServedDiskIdx(i)
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for ServedDiskIdx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A set of pool disks.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DiskMask(u64);

impl DiskMask {
    pub const EMPTY: DiskMask = DiskMask(0);

    pub fn set(&mut self, d: PoolDiskIdx) {
        self.0 |= 1 << d.0;
    }

    pub fn clear(&mut self, d: PoolDiskIdx) {
        self.0 &= !(1 << d.0);
    }

    pub fn with(mut self, d: PoolDiskIdx) -> Self {
        self.set(d);
        self
    }

    pub fn contains(self, d: PoolDiskIdx) -> bool {
        self.0 & (1 << d.0) != 0
    }

    pub fn len(self) -> u32 {
        self.0.count_ones()
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The disks in both.
    pub fn and(self, o: DiskMask) -> DiskMask {
        DiskMask(self.0 & o.0)
    }

    pub fn iter(self) -> impl Iterator<Item = PoolDiskIdx> {
        (0..MAX_DISKS as u8)
            .filter(move |&i| self.0 & (1 << i) != 0)
            .map(PoolDiskIdx)
    }
}

impl FromIterator<PoolDiskIdx> for DiskMask {
    fn from_iter<I: IntoIterator<Item = PoolDiskIdx>>(it: I) -> Self {
        it.into_iter().fold(DiskMask::EMPTY, DiskMask::with)
    }
}

impl fmt::LowerHex for DiskMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

/// A [`DiskMask`] shared between threads.
#[derive(Debug, Default)]
pub struct AtomicDiskMask(AtomicU64);

impl AtomicDiskMask {
    pub fn set(&self, d: PoolDiskIdx, on: bool) {
        if on {
            self.0.fetch_or(1 << d.0, Ordering::SeqCst);
        } else {
            self.0.fetch_and(!(1 << d.0), Ordering::SeqCst);
        }
    }

    pub fn contains(&self, d: PoolDiskIdx) -> bool {
        self.load().contains(d)
    }

    pub fn load(&self) -> DiskMask {
        DiskMask(self.0.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
#[path = "tests/coords_tests.rs"]
mod tests;
