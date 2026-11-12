//! Pool topology: a small tree of vdevs over the pool's leaf disks.
//!
//! A pool is one root vdev: a `RaidZ` group, a `Mirror`, or a `Stripe`
//! (RAID0) whose children are raidz groups, mirrors or single disks. Every
//! vdev addresses its member disks by their index in the pool's `DiskSet`,
//! so health tracking (labels, fencing, missed writes) stays per leaf disk
//! no matter how the tree is shaped.
//!
//! Each vdev has a *width*: the alignment, in blocks, that records must keep
//! so a record never shares a raidz stripe with anything else (the number of
//! data shards for raidz, 1 for a mirror, the LCM of its children for a
//! stripe). The pool's alignment is the root's width.

use crate::disk::{BLOCK_SIZE, BlockDevice, BlockId, Candidates, DiskError};
use crate::disks::{DiskSet, MAX_DISKS};
use crate::mirror::Mirror;
use crate::raidz::{RaidZ, RaidZError};
use crate::stripe::Stripe;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Target record size in blocks; the largest record is the biggest multiple
/// of the pool's alignment that fits (30 blocks, ~120 KiB, for most pools).
pub const RECORD_TARGET_BLOCKS: u64 = 30;

/// Largest record, in blocks, for a pool aligned to `align`.
pub fn max_record_blocks(align: u64) -> u64 {
    align * (RECORD_TARGET_BLOCKS / align).max(1)
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

pub fn lcm(a: u64, b: u64) -> u64 {
    a / gcd(a, b) * b
}

/// The shape of a pool, with leaf disks as indices into its `DiskSet`.
/// Stored in the superblock, so a pool can't be opened with a different
/// layout than it was formatted with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Topology {
    /// A single disk (no redundancy); only as a stripe member.
    Disk(usize),
    RaidZ {
        parity: usize,
        disks: Vec<usize>,
    },
    Mirror {
        disks: Vec<usize>,
    },
    Stripe {
        children: Vec<Topology>,
    },
}

impl Topology {
    /// Leaf disks, in order.
    pub fn leaves(&self) -> Vec<usize> {
        match self {
            Topology::Disk(d) => vec![*d],
            Topology::RaidZ { disks, .. } | Topology::Mirror { disks } => disks.clone(),
            Topology::Stripe { children } => children.iter().flat_map(|c| c.leaves()).collect(),
        }
    }

    /// Alignment in blocks (see the module docs).
    pub fn width(&self) -> u64 {
        match self {
            Topology::Disk(_) | Topology::Mirror { .. } => 1,
            Topology::RaidZ { parity, disks } => (disks.len() - parity) as u64,
            Topology::Stripe { children } => children.iter().map(|c| c.width()).fold(1, lcm),
        }
    }

    /// The pool blocks `[start, end)` whose data or parity shard `shard`
    /// (a block after the label area) of leaf `disk` holds, or None if
    /// `disk` isn't in this tree. The inverse of how each vdev places
    /// blocks: raidz stripe `s` holds blocks `s*data..(s+1)*data`, a
    /// mirror (or plain disk) block `s`, a stripe interleaves its
    /// children in chunks.
    pub fn pool_range_of(&self, disk: usize, shard: u64) -> Option<(u64, u64)> {
        match self {
            Topology::Disk(d) => (*d == disk).then_some((shard, shard + 1)),
            Topology::Mirror { disks } => disks.contains(&disk).then_some((shard, shard + 1)),
            Topology::RaidZ { parity, disks } => {
                let data = (disks.len() - parity) as u64;
                disks
                    .contains(&disk)
                    .then_some((shard * data, (shard + 1) * data))
            }
            Topology::Stripe { children } => {
                let n = children.len() as u64;
                let chunk = max_record_blocks(self.width());
                children.iter().enumerate().find_map(|(i, c)| {
                    let (s, e) = c.pool_range_of(disk, shard)?;
                    // A child's range never crosses a chunk (the chunk is
                    // a multiple of every child's width).
                    let global =
                        |local: u64| ((local / chunk) * n + i as u64) * chunk + local % chunk;
                    Some((global(s), global(e - 1) + 1))
                })
            }
        }
    }

    /// How many disks can fail (in the worst place) without losing data.
    pub fn fault_tolerance(&self) -> usize {
        match self {
            Topology::Disk(_) => 0,
            Topology::Mirror { disks } => disks.len() - 1,
            Topology::RaidZ { parity, .. } => *parity,
            Topology::Stripe { children } => children
                .iter()
                .map(|c| c.fault_tolerance())
                .min()
                .unwrap_or(0),
        }
    }

    /// Check the shape is buildable over `disk_count` leaf disks, each used
    /// exactly once.
    pub fn validate(&self, disk_count: usize) -> Result<(), String> {
        if disk_count > MAX_DISKS {
            return Err(format!("at most {} disks are supported", MAX_DISKS));
        }
        self.validate_node(true)?;
        let mut leaves = self.leaves();
        leaves.sort_unstable();
        if leaves != (0..disk_count).collect::<Vec<_>>() {
            return Err("every disk must be used exactly once".into());
        }
        let width = self.width();
        if width > RECORD_TARGET_BLOCKS {
            return Err(format!(
                "pool alignment of {} blocks is too wide (at most {}); use narrower raidz groups",
                width, RECORD_TARGET_BLOCKS
            ));
        }
        Ok(())
    }

    fn validate_node(&self, top: bool) -> Result<(), String> {
        match self {
            Topology::Disk(_) if top => {
                Err("a single disk pool has no redundancy; use a stripe of one disk".into())
            }
            Topology::Disk(_) => Ok(()),
            Topology::RaidZ { parity, disks } => {
                if *parity == 0 {
                    return Err("raidz parity must be at least 1".into());
                }
                if disks.len() <= *parity {
                    return Err(format!(
                        "raidz{} needs more than {} disks (has {})",
                        parity,
                        parity,
                        disks.len()
                    ));
                }
                Ok(())
            }
            Topology::Mirror { disks } => {
                if disks.len() < 2 {
                    return Err("a mirror needs at least 2 disks".into());
                }
                Ok(())
            }
            Topology::Stripe { children } => {
                if !top {
                    return Err("stripes can't be nested".into());
                }
                if children.is_empty() {
                    return Err("a stripe needs at least one member".into());
                }
                children.iter().try_for_each(|c| c.validate_node(false))
            }
        }
    }

    /// One-line description, e.g. `raidz2 (5 disks)`.
    pub fn summary(&self) -> String {
        match self {
            Topology::Disk(d) => format!("disk {}", d),
            Topology::RaidZ { parity, disks } => format!("raidz{} ({} disks)", parity, disks.len()),
            Topology::Mirror { disks } => format!("mirror ({} disks)", disks.len()),
            Topology::Stripe { children } => format!(
                "stripe of [{}]",
                children
                    .iter()
                    .map(|c| c.summary())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// What a vdev's shard check found (and, if asked, fixed), per leaf disk
/// (indexed like the pool's `DiskSet`; vectors grow as needed).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StripeCheck {
    pub stripes: u64,
    /// Shards that didn't match what the verified data encodes to.
    pub mismatched: Vec<u64>,
    /// Of those, rewritten successfully.
    pub repaired: Vec<u64>,
    /// Disks that couldn't be read (offline), so weren't verified.
    pub unreadable: Vec<bool>,
    /// Disks a repair write failed on.
    pub write_failed: Vec<bool>,
    /// Stripes skipped because they have unflushed buffered writes.
    pub skipped: u64,
}

impl StripeCheck {
    pub fn new(disks: usize, stripes: u64) -> Self {
        StripeCheck {
            stripes,
            mismatched: vec![0; disks],
            repaired: vec![0; disks],
            unreadable: vec![false; disks],
            write_failed: vec![false; disks],
            skipped: 0,
        }
    }

    fn grow(&mut self, n: usize) {
        if self.mismatched.len() < n {
            self.mismatched.resize(n, 0);
            self.repaired.resize(n, 0);
            self.unreadable.resize(n, false);
            self.write_failed.resize(n, false);
        }
    }

    pub fn add(&mut self, o: &StripeCheck) {
        self.grow(o.mismatched.len());
        self.stripes += o.stripes;
        self.skipped += o.skipped;
        for d in 0..o.mismatched.len() {
            self.mismatched[d] += o.mismatched[d];
            self.repaired[d] += o.repaired[d];
            self.unreadable[d] |= o.unreadable[d];
            self.write_failed[d] |= o.write_failed[d];
        }
    }

    pub fn disks(&self) -> usize {
        self.mismatched.len()
    }

    pub fn is_unreadable(&self, d: usize) -> bool {
        self.unreadable.get(d).copied().unwrap_or(false)
    }

    pub fn has_write_failed(&self, d: usize) -> bool {
        self.write_failed.get(d).copied().unwrap_or(false)
    }
}

/// Result of verifying a range of label blocks (see `Vdev::verify_labels`).
#[derive(Debug, Default, Clone)]
pub struct LabelScan {
    pub check: StripeCheck,
    /// Units checked (raidz stripes, mirror blocks).
    pub units: u64,
    /// First block (in the caller's address space) of every unit for which
    /// no valid contents could be found.
    pub unrecoverable: Vec<u64>,
}

impl LabelScan {
    pub fn add(&mut self, o: LabelScan) {
        self.check.add(&o.check);
        self.units += o.units;
        self.unrecoverable.extend(o.unrecoverable);
    }
}

/// Scores a label block: `Some(txg)` if it is a valid label.
pub type LabelScore<'a> = &'a (dyn Fn(&[u8]) -> Option<u64> + Send + Sync);

/// A node of the pool tree. Addresses are vdev-local.
#[async_trait]
pub trait Vdev: BlockDevice + std::fmt::Debug {
    /// Alignment in blocks.
    fn width(&self) -> u64;

    /// Check `data` (whole multiples of `width`, known-good contents, e.g. a
    /// record that passed its checksum, starting at aligned `lba`) against
    /// every copy and every parity shard on disk. With `repair`, rewrite the
    /// ones that differ.
    async fn verify_blocks(
        &self,
        lba: BlockId,
        data: &[u8],
        repair: bool,
    ) -> Result<StripeCheck, RaidZError>;

    /// Verify `blocks` self-describing label blocks (superblock, uberblocks,
    /// space map slots) starting at aligned `lba`. They have no parent
    /// checksum, so `score` decides which version is right. Never-written
    /// (all zero) units are fine.
    async fn verify_labels(
        &self,
        lba: BlockId,
        blocks: u64,
        repair: bool,
        score: LabelScore<'_>,
    ) -> Result<LabelScan, RaidZError>;
}

/// Build the vdev tree for `t` over `disks`.
fn build(disks: &Arc<DiskSet>, t: &Topology) -> Box<dyn Vdev> {
    match t {
        Topology::Disk(d) => Box::new(Mirror::new(disks.clone(), vec![*d])),
        Topology::Mirror { disks: members } => {
            Box::new(Mirror::new(disks.clone(), members.clone()))
        }
        Topology::RaidZ {
            parity,
            disks: members,
        } => Box::new(RaidZ::new(disks.clone(), members.clone(), *parity)),
        Topology::Stripe { children } => Box::new(Stripe::new(
            children.iter().map(|c| build(disks, c)).collect(),
        )),
    }
}

/// A pool: its leaf disks and the vdev tree over them. This is the device a
/// `FileSystem` sits on.
#[derive(Debug)]
pub struct Pool {
    pub disks: Arc<DiskSet>,
    pub root: Box<dyn Vdev>,
    pub topology: Topology,
    /// Record alignment in blocks (the root's width).
    pub align: u64,
}

impl Pool {
    pub fn new(disks: Arc<DiskSet>, topology: Topology) -> Result<Self, String> {
        topology.validate(disks.len())?;
        let root = build(&disks, &topology);
        let align = root.width();
        Ok(Pool {
            disks,
            root,
            topology,
            align,
        })
    }

    /// The 5-disk raidz2 every pool used to be.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn raidz(disks: Arc<DiskSet>, parity: usize) -> Self {
        let n = disks.len();
        Self::new(
            disks,
            Topology::RaidZ {
                parity,
                disks: (0..n).collect(),
            },
        )
        .expect("valid raidz")
    }
}

#[async_trait]
impl BlockDevice for Pool {
    async fn flush(&self) -> Result<(), DiskError> {
        self.root.flush().await
    }
    async fn read_block(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        self.root.read_block(lba, buf).await
    }
    async fn write_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        self.root.write_block(lba, buf).await
    }
    async fn capacity_blocks(&self) -> Result<u64, DiskError> {
        self.root.capacity_blocks().await
    }
    async fn read_blocks(&self, lba: BlockId, buf: &mut [u8]) -> Result<(), DiskError> {
        self.root.read_blocks(lba, buf).await
    }
    async fn write_blocks(&self, lba: BlockId, buf: &[u8]) -> Result<(), DiskError> {
        self.root.write_blocks(lba, buf).await
    }
    async fn reconstruct_candidates(&self, lba: BlockId) -> Result<Candidates, DiskError> {
        self.root.reconstruct_candidates(lba).await
    }
    async fn repair_block(&self, lba: BlockId, buf: &[u8; BLOCK_SIZE]) -> Result<(), DiskError> {
        self.root.repair_block(lba, buf).await
    }
    fn pool(&self) -> Option<&Pool> {
        Some(self)
    }
}

#[cfg(test)]
#[path = "tests/topology_tests.rs"]
mod tests;
