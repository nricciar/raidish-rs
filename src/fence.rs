//! Write fencing by *domain*.
//!
//! Several processes write one pool: the committer (the `pool` lease
//! holder) writes labels, uberblocks, space maps and its own metaslabs; each
//! volume writer writes only into metaslabs lent to it. Every part of the
//! pool belongs to one fencing [`Domain`], and a write is only admitted if
//! it carries that domain's current epoch (or a newer one):
//!
//! * `Pool`: the `pool` lease epoch.
//! * `Metaslab(i)` while lent: the epoch of the lend. A new committer
//!   doesn't change it, so writers keep going across a committer failover.
//! * `Metaslab(i)` otherwise: the newer of the pool epoch and the epoch of
//!   the metaslab's return.
//!
//! The disk server and the client compute the domain of a raw disk block
//! the same way ([`FenceMap`]): leaf disk + block → pool block (the inverse
//! of the vdev placement) → metaslab. Each lend or return takes a fresh
//! cluster-wide epoch, so per domain the fencing epoch only ever grows, and
//! a server that lags the cluster can safely learn newer epochs from the
//! requests themselves (as with the single pool epoch before).

use crate::disks::DISK_LABEL_BLOCKS;
use crate::metaslab::MetaslabLayout;
use crate::types::Epoch;
use crate::types::MetaslabIdx;
use crate::vdev::Topology;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Domain {
    Pool,
    Metaslab(MetaslabIdx),
}

/// Where a raw block of a pool's leaf disk falls.
#[derive(Debug, Clone)]
pub struct FenceMap {
    pub topology: Topology,
    pub layout: MetaslabLayout,
}

impl FenceMap {
    pub fn new(topology: Topology, layout: MetaslabLayout) -> Self {
        FenceMap { topology, layout }
    }

    /// The domain of raw block `block` (label area included) of leaf `disk`.
    pub fn domain_of(&self, disk: usize, block: u64) -> Domain {
        if block < DISK_LABEL_BLOCKS || self.layout.metaslab_size_blocks == 0 {
            return Domain::Pool;
        }
        match self
            .topology
            .pool_range_of(disk, block - DISK_LABEL_BLOCKS)
            .and_then(|r| self.layout.metaslab_for_block(r.start))
        {
            Some(i) => Domain::Metaslab(i),
            None => Domain::Pool,
        }
    }

    /// The distinct domains `count` raw blocks from `block` fall in.
    pub fn domains(&self, disk: usize, block: u64, count: u32) -> Vec<Domain> {
        let mut out: Vec<Domain> = Vec::new();
        for b in block..block + count.max(1) as u64 {
            let d = self.domain_of(disk, b);
            if !out.contains(&d) {
                out.push(d);
            }
        }
        out
    }
}

/// A metaslab's fencing grant, as the cluster recorded it.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grant {
    /// Lent to a writer at this epoch: only that lend's writes are admitted
    /// (a new committer doesn't change it).
    Lent(Epoch),
    /// Back with the committer since this epoch: the pool floor applies too.
    Returned(Epoch),
}

/// The fencing state a disk server knows: from the cluster (what was
/// granted) and from requests (newer epochs it saw first).
#[derive(Debug, Default)]
#[cfg(not(target_arch = "wasm32"))]
pub struct ServerFence {
    /// Pool floor: the newest pool epoch from the cluster or from requests.
    pool: AtomicU64,
    /// Per metaslab: the grant from the cluster.
    grants: RwLock<HashMap<MetaslabIdx, Grant>>,
    /// Per metaslab: newest epoch seen on an admitted write.
    seen: RwLock<HashMap<MetaslabIdx, Epoch>>,
    /// Domain map; None until the pool's layout is known (then everything
    /// is the pool domain).
    map: RwLock<Option<FenceMap>>,
    /// Served disk index → pool leaf index (None: identity).
    served: RwLock<Option<HashMap<usize, usize>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ServerFence {
    /// The pool floor (for status and tests).
    pub fn pool_floor(&self) -> Epoch {
        Epoch::from_raw(self.pool.load(Ordering::SeqCst))
    }

    /// Raise the pool floor (cluster grant or a test).
    pub fn raise_pool(&self, epoch: Epoch) {
        self.pool.fetch_max(epoch.get(), Ordering::SeqCst);
    }

    /// Take in the cluster's view: the pool epoch and every metaslab grant.
    pub fn observe(
        &self,
        pool_epoch: Epoch,
        grants: impl IntoIterator<Item = (MetaslabIdx, Grant)>,
    ) {
        self.raise_pool(pool_epoch);
        let mut g = self.grants.write().unwrap();
        for (idx, grant) in grants {
            g.insert(idx, grant);
        }
    }

    pub fn set_map(&self, map: Option<FenceMap>) {
        *self.map.write().unwrap() = map;
    }

    pub fn has_map(&self) -> bool {
        self.map.read().unwrap().is_some()
    }

    pub fn set_served(&self, served: HashMap<usize, usize>) {
        *self.served.write().unwrap() = Some(served);
    }

    /// The current floor of `domain`.
    pub fn floor(&self, domain: Domain) -> Epoch {
        let pool = self.pool_floor();
        match domain {
            Domain::Pool => pool,
            Domain::Metaslab(i) => {
                let seen = self
                    .seen
                    .read()
                    .unwrap()
                    .get(&i)
                    .copied()
                    .unwrap_or_default();
                let base = match self.grants.read().unwrap().get(&i) {
                    Some(&Grant::Lent(e)) => e,
                    Some(&Grant::Returned(e)) => e.max(pool),
                    None => pool,
                };
                base.max(seen)
            }
        }
    }

    fn domains(&self, served_disk: usize, block: u64, count: u32) -> Vec<Domain> {
        let map = self.map.read().unwrap();
        let Some(map) = map.as_ref() else {
            return vec![Domain::Pool];
        };
        let disk = match self.served.read().unwrap().as_ref() {
            None => served_disk,
            Some(m) => match m.get(&served_disk) {
                Some(&d) => d,
                None => return vec![Domain::Pool],
            },
        };
        map.domains(disk, block, count)
    }

    /// Admit a request (or not). Writes must carry at least the floor of
    /// every domain they touch; admitted, they raise those floors. Reads
    /// are always served and raise only the pool floor (a new committer's
    /// first reads fence the old one out). Flushes are always served: they
    /// only make admitted writes durable.
    pub fn admit(
        &self,
        served_disk: usize,
        block: u64,
        count: u32,
        epoch: Epoch,
        write: bool,
    ) -> bool {
        let domains = self.domains(served_disk, block, count);
        if !write {
            if domains.contains(&Domain::Pool) {
                self.raise_pool(epoch);
            }
            return true;
        }
        if domains.iter().any(|&d| epoch < self.floor(d)) {
            return false;
        }
        for d in domains {
            match d {
                Domain::Pool => self.raise_pool(epoch),
                Domain::Metaslab(i) => {
                    let mut seen = self.seen.write().unwrap();
                    let e = seen.entry(i).or_default();
                    *e = (*e).max(epoch);
                }
            }
        }
        true
    }
}

/// A pool client's epochs, as sent with its requests: the pool epoch (the
/// `pool` lease of the committer or command writing it, 0 without one)
/// and, per metaslab, the epoch of a lend to this client's writer or of a
/// return to its committer. Each `DiskSet` has one, so a committer and a
/// writer in one process fence separately.
#[derive(Debug, Default)]
pub struct ClientFence {
    pool: AtomicU64,
    /// Run when a disk server refuses the pool epoch: another writer has
    /// the pool now. It must never be retried.
    on_stale: std::sync::OnceLock<fn()>,
    metaslabs: RwLock<HashMap<MetaslabIdx, Epoch>>,
    map: RwLock<Option<FenceMap>>,
}

/// The epoch a request carries, and where it came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stamp {
    /// The pool lease epoch (or none).
    Pool(Epoch),
    /// A metaslab lent to this process's writer: a refusal means that
    /// writer lost the metaslab, not that the pool has a new committer.
    Lent(Epoch),
}

impl Stamp {
    pub fn epoch(self) -> Epoch {
        match self {
            Stamp::Pool(e) | Stamp::Lent(e) => e,
        }
    }
}

impl ClientFence {
    /// Run `f` once a disk server refuses our pool epoch.
    pub fn on_stale(&self, f: fn()) {
        let _ = self.on_stale.set(f);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn stale(&self) {
        if let Some(f) = self.on_stale.get() {
            f();
        }
    }

    pub fn set_pool_epoch(&self, epoch: Epoch) {
        self.pool.store(epoch.get(), Ordering::SeqCst);
    }

    pub fn pool_epoch(&self) -> Epoch {
        Epoch::from_raw(self.pool.load(Ordering::SeqCst))
    }

    pub fn set_metaslab_epoch(&self, idx: MetaslabIdx, epoch: Epoch) {
        self.metaslabs.write().unwrap().insert(idx, epoch);
    }

    pub fn clear_metaslab(&self, idx: MetaslabIdx) {
        self.metaslabs.write().unwrap().remove(&idx);
    }

    pub fn set_map(&self, map: FenceMap) {
        *self.map.write().unwrap() = Some(map);
    }

    /// The epoch to send with a request for `count` raw blocks from
    /// `block` of pool leaf `disk` (None: not a pool disk, or a flush).
    pub fn epoch_for(&self, disk: Option<usize>, block: u64, count: u32) -> Stamp {
        let pool = Stamp::Pool(self.pool_epoch());
        let ms = self.metaslabs.read().unwrap();
        if ms.is_empty() {
            return pool;
        }
        let map = self.map.read().unwrap();
        let (Some(map), Some(disk)) = (map.as_ref(), disk) else {
            return pool;
        };
        let mut out = pool;
        for d in map.domains(disk, block, count) {
            if let Domain::Metaslab(i) = d
                && let Some(&e) = ms.get(&i)
                && e > out.epoch()
            {
                out = Stamp::Lent(e);
            }
        }
        out
    }
}

#[cfg(test)]
#[path = "tests/fence_tests.rs"]
mod tests;
