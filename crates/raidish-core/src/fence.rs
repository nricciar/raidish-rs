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

#[cfg(not(target_arch = "wasm32"))]
use crate::coords::ServedDiskIdx;
use crate::coords::{PoolDiskIdx, RawDiskBlock};
use crate::metaslab::MetaslabLayout;
use crate::types::Epoch;
use crate::types::MetaslabIdx;
use crate::vdev::Topology;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub use crate::types::{Domain, Grant};

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
    pub fn domain_of(&self, disk: PoolDiskIdx, block: RawDiskBlock) -> Domain {
        let Some(shard) = block.to_shard() else {
            return Domain::Pool;
        };
        if self.layout.metaslab_size_blocks == 0 {
            return Domain::Pool;
        }
        // A shard holds pool blocks at or after its own number: one past the
        // last metaslab is no metaslab's (nor mapped, which could overflow).
        let end = self.layout.data_start.saturating_add(
            self.layout
                .metaslab_count
                .saturating_mul(self.layout.metaslab_size_blocks),
        );
        if shard.get() >= end {
            return Domain::Pool;
        }
        match self
            .topology
            .pool_range_of(disk, shard)
            .and_then(|r| self.layout.metaslab_for_block(r.start.get()))
        {
            Some(i) => Domain::Metaslab(i),
            None => Domain::Pool,
        }
    }

    /// The distinct domains `count` raw blocks from `block` fall in.
    pub fn domains(&self, disk: PoolDiskIdx, block: RawDiskBlock, count: u32) -> Vec<Domain> {
        let mut out: Vec<Domain> = Vec::new();
        // Never empty and never wrapping: a range that ends past the last
        // block (its caller refuses it) still names the domain it starts in.
        let blocks = (0..count.max(1) as u64).map_while(|i| block.get().checked_add(i));
        for b in blocks {
            let d = self.domain_of(disk, RawDiskBlock::new(b));
            if !out.contains(&d) {
                out.push(d);
            }
        }
        out
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::types::FenceObserver for ServerFence {
    fn observe_grants(
        &self,
        pool_epoch: Epoch,
        grants: &mut dyn Iterator<Item = (MetaslabIdx, Grant)>,
    ) {
        self.observe(pool_epoch, grants)
    }
}

/// The fencing state a disk server knows: from the cluster (what was
/// granted) and from requests (newer epochs it saw first). All of it sits
/// under one lock, so an admission sees one consistent state: a grant
/// observed between its checks can't let a stamp past the kind check and
/// then judge it against another grant's floor (structural triage I-11).
#[derive(Debug, Default)]
#[cfg(not(target_arch = "wasm32"))]
pub struct ServerFence {
    inner: std::sync::Mutex<FenceInner>,
}

#[derive(Debug, Default)]
#[cfg(not(target_arch = "wasm32"))]
struct FenceInner {
    /// Pool floor: the newest pool epoch from the cluster or from requests.
    pool: Epoch,
    /// Per metaslab: the grant from the cluster.
    grants: HashMap<MetaslabIdx, Grant>,
    /// Per metaslab: newest epoch seen on an admitted write.
    seen: HashMap<MetaslabIdx, Epoch>,
    /// Domain map; None until the pool's layout is known (then everything
    /// is the pool domain).
    map: Option<FenceMap>,
    /// Served disk → pool leaf (None: the same index).
    served: Option<HashMap<ServedDiskIdx, PoolDiskIdx>>,
    /// The pool was found unformatted (or there's no pool to map): with no
    /// map, every block is the pool's. See [`Ready`].
    unmapped: bool,
    /// Whether one linearizable read of the cluster's grants is in.
    synced: bool,
}

/// Whether a disk server may admit writes (audit R1b). Its floors live in
/// memory: started (or restarted), it knows no grant and no epoch, so it
/// would map every block to the pool domain and admit any epoch. Writes
/// wait for `Ready`: the cluster's grants as of a linearizable read (newer
/// than anything admitted before the restart), and the domain map.
///
/// Without a map (the pool reads as unformatted, or there's no pool
/// config) every block is the pool's, which fences rightly only while no
/// metaslab has ever been lent: lending takes a loaded pool, so a grant
/// means the superblock exists and just wasn't read (a blank disk where
/// it's looked for, say). Then the server stays `Starting`, refusing
/// writes rather than letting pool-stamped ones into lent space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ready {
    Starting,
    Ready,
}

#[cfg(not(target_arch = "wasm32"))]
impl FenceInner {
    fn raise_pool(&mut self, epoch: Epoch) {
        self.pool = self.pool.max(epoch);
    }

    fn floor(&self, domain: Domain) -> Epoch {
        match domain {
            Domain::Pool => self.pool,
            Domain::Metaslab(i) => {
                let seen = self.seen.get(&i).copied().unwrap_or_default();
                let base = match self.grants.get(&i) {
                    Some(&Grant::Lent(e)) => e,
                    Some(&Grant::Returned(e)) => e.max(self.pool),
                    None => self.pool,
                };
                base.max(seen)
            }
        }
    }

    fn domains(&self, served_disk: ServedDiskIdx, block: RawDiskBlock, count: u32) -> Vec<Domain> {
        let Some(map) = self.map.as_ref() else {
            return vec![Domain::Pool];
        };
        let disk = match self.served.as_ref() {
            None => PoolDiskIdx::new(served_disk.as_usize()),
            Some(m) => m.get(&served_disk).copied(),
        };
        match disk {
            Some(disk) => map.domains(disk, block, count),
            None => vec![Domain::Pool],
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ServerFence {
    fn lock(&self) -> std::sync::MutexGuard<'_, FenceInner> {
        // Nothing panics under this lock; a poisoned one still holds a
        // consistent state (every update is a single assignment).
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A fence that has done its startup (tests, and servers of raw disks
    /// with no cluster to wait for).
    pub fn ready() -> Self {
        let f = ServerFence::default();
        {
            let mut i = f.lock();
            i.unmapped = true;
            i.synced = true;
        }
        f
    }

    /// Asked live (see [`Ready`]): a lend arriving while there's no map
    /// takes a ready server back to `Starting`.
    pub fn readiness(&self) -> Ready {
        let i = self.lock();
        if !i.synced {
            return Ready::Starting;
        }
        if i.map.is_some() || (i.unmapped && i.grants.is_empty()) {
            Ready::Ready
        } else {
            Ready::Starting
        }
    }

    /// The cluster's grants as of a linearizable read are in (`observe`d).
    pub fn mark_synced(&self) {
        self.lock().synced = true;
    }

    /// The pool layout is known to be none: no pool here, or not formatted.
    pub fn mark_unmapped(&self) {
        let mut i = self.lock();
        i.map = None;
        i.unmapped = true;
    }

    /// The pool floor (for status and tests).
    pub fn pool_floor(&self) -> Epoch {
        self.lock().pool
    }

    /// Raise the pool floor (cluster grant or a test).
    pub fn raise_pool(&self, epoch: Epoch) {
        self.lock().raise_pool(epoch);
    }

    /// Take in the cluster's view: the pool epoch and every metaslab grant.
    ///
    /// Views come from more than one place (the startup `sync_fence` read,
    /// and each Raft apply), and may arrive out of order, so a grant only
    /// ever moves forward: one is taken only if it's newer than the one
    /// held (structural triage I-10). Every grant has its own epoch, and
    /// epochs only grow, so newer is never ambiguous. A grant missing from
    /// a view is kept: the cluster never forgets a metaslab's grant, so a
    /// view without it is just from before its first lend, and dropping it
    /// would let pool stamps into lent space.
    pub fn observe(
        &self,
        pool_epoch: Epoch,
        grants: impl IntoIterator<Item = (MetaslabIdx, Grant)>,
    ) {
        let mut i = self.lock();
        i.raise_pool(pool_epoch);
        for (idx, grant) in grants {
            match i.grants.get(&idx) {
                Some(held) if held.epoch() >= grant.epoch() => {}
                _ => {
                    i.grants.insert(idx, grant);
                }
            }
        }
    }

    pub fn set_map(&self, map: Option<FenceMap>) {
        let mut i = self.lock();
        if map.is_none() {
            i.unmapped = true;
        }
        i.map = map;
    }

    pub fn has_map(&self) -> bool {
        self.lock().map.is_some()
    }

    pub fn set_served(&self, served: HashMap<ServedDiskIdx, PoolDiskIdx>) {
        self.lock().served = Some(served);
    }

    /// The current floor of `domain`.
    pub fn floor(&self, domain: Domain) -> Epoch {
        self.lock().floor(domain)
    }

    /// `admit_stamp` for a request stamped with a pool epoch: admitted or
    /// not.
    pub fn admit(
        &self,
        served_disk: ServedDiskIdx,
        block: RawDiskBlock,
        count: u32,
        epoch: Epoch,
        write: bool,
    ) -> bool {
        self.admit_stamp(served_disk, block, count, Stamp::Pool(epoch), write) == Admit::Yes
    }

    /// Admit a request (or not). Writes must carry at least the floor of
    /// every domain they touch; admitted, they raise those floors. A
    /// metaslab lent to a writer admits only writes stamped with a lend
    /// (that one, or a newer one this server hasn't heard of): a pool or
    /// return stamp, however new, is the committer's, and must not land in
    /// a writer's space nor raise its floor past the lend. The pool domain
    /// admits only pool stamps: a lend or return epoch is no pool lease
    /// (audit R2b). A client splits its requests so each carries one kind
    /// of stamp (`ClientFence::stamps`). Reads
    /// are always served and raise only the pool floor, and only with a
    /// pool stamp (a new committer's first reads fence the old one out).
    /// Flushes are always served: they only make admitted writes durable.
    ///
    /// The checks and the floor raises happen under one lock: what is
    /// checked is what is raised (I-11).
    pub fn admit_stamp(
        &self,
        served_disk: ServedDiskIdx,
        block: RawDiskBlock,
        count: u32,
        stamp: Stamp,
        write: bool,
    ) -> Admit {
        let mut i = self.lock();
        let domains = i.domains(served_disk, block, count);
        let epoch = stamp.epoch();
        let pool_stamp = matches!(stamp, Stamp::Pool(_));
        if !write {
            if pool_stamp && domains.contains(&Domain::Pool) {
                i.raise_pool(epoch);
            }
            return Admit::Yes;
        }
        for &d in &domains {
            let lent = match d {
                Domain::Pool => false,
                Domain::Metaslab(m) => matches!(i.grants.get(&m), Some(Grant::Lent(_))),
            };
            let fits = match stamp {
                Stamp::Pool(_) => !lent,
                Stamp::Lent(_) => d != Domain::Pool,
                Stamp::Returned(_) => d != Domain::Pool && !lent,
            };
            if !fits {
                return Admit::Fenced;
            }
            if epoch < i.floor(d) {
                return Admit::Stale;
            }
        }
        for d in domains {
            match d {
                Domain::Pool => {
                    if pool_stamp {
                        i.raise_pool(epoch)
                    }
                }
                Domain::Metaslab(m) => {
                    let e = i.seen.entry(m).or_default();
                    *e = (*e).max(epoch);
                }
            }
        }
        Admit::Yes
    }
}

/// A disk server's answer to a request (see `ServerFence::admit_stamp`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admit {
    Yes,
    /// An epoch older than the domain's floor.
    Stale,
    /// A write stamped with the wrong kind of epoch for where it lands: in
    /// a lent metaslab, not a lend; in the pool domain, not the pool's.
    Fenced,
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
    /// Set once a disk server refused the pool epoch: from then on nothing
    /// this client commits can be trusted to be the pool's (audit R2a).
    stale: std::sync::atomic::AtomicBool,
    /// Per metaslab: `Stamp::Lent` or `Stamp::Returned`.
    metaslabs: RwLock<HashMap<MetaslabIdx, Stamp>>,
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
    /// A metaslab returned to this process's committer: a refusal means,
    /// as for `Pool`, that another committer has the pool.
    Returned(Epoch),
}

impl Stamp {
    pub fn epoch(self) -> Epoch {
        match self {
            Stamp::Pool(e) | Stamp::Lent(e) | Stamp::Returned(e) => e,
        }
    }

    /// Whether a refusal of this stamp means this process lost the pool.
    pub fn is_committers(self) -> bool {
        !matches!(self, Stamp::Lent(_))
    }

    /// Its kind on the wire (see `crate::websocket`).
    pub fn kind(self) -> u8 {
        match self {
            Stamp::Pool(_) => 0,
            Stamp::Lent(_) => 1,
            Stamp::Returned(_) => 2,
        }
    }

    pub fn from_wire(kind: u8, epoch: Epoch) -> Option<Stamp> {
        match kind {
            0 => Some(Stamp::Pool(epoch)),
            1 => Some(Stamp::Lent(epoch)),
            2 => Some(Stamp::Returned(epoch)),
            _ => None,
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
        self.stale.store(true, Ordering::SeqCst);
        if let Some(f) = self.on_stale.get() {
            f();
        }
    }

    /// Whether a disk server has refused this client's pool epoch: another
    /// committer or writer holds the pool.
    pub fn is_stale(&self) -> bool {
        self.stale.load(Ordering::SeqCst)
    }

    pub fn set_pool_epoch(&self, epoch: Epoch) {
        self.pool.store(epoch.get(), Ordering::SeqCst);
    }

    pub fn pool_epoch(&self) -> Epoch {
        Epoch::from_raw(self.pool.load(Ordering::SeqCst))
    }

    /// Metaslab `idx` is lent to this client's writer at `epoch`.
    pub fn set_lent(&self, idx: MetaslabIdx, epoch: Epoch) {
        self.metaslabs
            .write()
            .unwrap()
            .insert(idx, Stamp::Lent(epoch));
    }

    /// Metaslab `idx` was returned to this client's committer at `epoch`.
    pub fn set_returned(&self, idx: MetaslabIdx, epoch: Epoch) {
        self.metaslabs
            .write()
            .unwrap()
            .insert(idx, Stamp::Returned(epoch));
    }

    pub fn clear_metaslab(&self, idx: MetaslabIdx) {
        self.metaslabs.write().unwrap().remove(&idx);
    }

    pub fn set_map(&self, map: FenceMap) {
        *self.map.write().unwrap() = Some(map);
    }

    /// The stamp for raw block `block` of pool leaf `disk`.
    fn stamp_of(
        &self,
        map: Option<&FenceMap>,
        ms: &HashMap<MetaslabIdx, Stamp>,
        disk: PoolDiskIdx,
        block: RawDiskBlock,
    ) -> Stamp {
        let pool = Stamp::Pool(self.pool_epoch());
        let Some(map) = map else {
            return pool;
        };
        match map.domain_of(disk, block) {
            Domain::Metaslab(i) => match ms.get(&i) {
                Some(&s @ Stamp::Lent(_)) => s,
                // A return older than our pool epoch: the pool's floor
                // covers it.
                Some(&s @ Stamp::Returned(e)) if e > pool.epoch() => s,
                _ => pool,
            },
            Domain::Pool => pool,
        }
    }

    /// The requests to send for `count` raw blocks from `block` of pool
    /// leaf `disk` (None: not a pool disk): `(block, count, stamp)` runs,
    /// in order, each with one stamp that's right for every block in it.
    /// Usually one run; more where the range crosses a domain boundary
    /// whose stamps differ (one stamp for both would be wrong for one:
    /// audit R2b).
    pub fn stamps(
        &self,
        disk: Option<PoolDiskIdx>,
        block: RawDiskBlock,
        count: u32,
    ) -> Vec<(RawDiskBlock, u32, Stamp)> {
        let pool = Stamp::Pool(self.pool_epoch());
        let ms = self.metaslabs.read().unwrap();
        let map = self.map.read().unwrap();
        let (false, Some(map), Some(disk)) = (ms.is_empty(), map.as_ref(), disk) else {
            return vec![(block, count, pool)];
        };
        let mut out: Vec<(RawDiskBlock, u32, Stamp)> = Vec::new();
        for i in 0..count.max(1) {
            let Some(b) = block.get().checked_add(i as u64).map(RawDiskBlock::new) else {
                // Past the last block (the server refuses it): keep the
                // rest in the last run rather than drop it.
                if let Some((_, n, _)) = out.last_mut() {
                    *n += count - i;
                }
                break;
            };
            let s = self.stamp_of(Some(map), &ms, disk, b);
            match out.last_mut() {
                Some((_, n, last)) if *last == s && count > 0 => *n += 1,
                _ => out.push((b, (count > 0) as u32, s)),
            }
        }
        out
    }

    /// The one stamp for a request that can't be split (a range in one
    /// domain, as `stamps` gives).
    pub fn epoch_for(&self, disk: Option<PoolDiskIdx>, block: RawDiskBlock, count: u32) -> Stamp {
        self.stamps(disk, block, count)[0].2
    }
}

#[cfg(test)]
#[path = "tests/fence_tests.rs"]
mod tests;
