//! The replicated state: leases on named resources, each grant stamped with
//! a cluster-wide, monotonically increasing *epoch*.
//!
//! The epoch is the fencing token. Whoever holds the `pool` lease sends its
//! epoch with every block request, and the disk servers refuse writes that
//! carry an epoch older than the newest one they know of. So a writer that
//! lost its lease (paused, partitioned) can't damage the pool even if it
//! doesn't know yet that it lost it.
//!
//! Everything here is deterministic: the leader stamps `now_ms` into each
//! request before proposing it, so every replica computes the same result.

use crate::types::{Epoch, Holder, HolderRole, Holding, LeaseRef, LeaseResource, MetaslabIdx};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The resource a pool writer (the committer) leases.
pub const POOL: LeaseResource = LeaseResource::Pool;

/// The resource the NFSv4 state owner leases (see `crate::nfs::v4`).
pub const NFS4: LeaseResource = LeaseResource::Nfs4;

pub use crate::fence::Domain;

/// Who may write a metaslab's data, and since which epoch.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MetaslabGrant {
    /// Lent to writer lease `writer` (`writer/<node>` at its epoch);
    /// `epoch` (the lend's) fences the metaslab's data.
    ///
    /// `from` (in both): the epoch of the grant this one replaced (none:
    /// the first), so a resent `Lend` or `Return` can tell it already
    /// happened (audit R4d).
    Lent {
        writer: LeaseRef,
        epoch: Epoch,
        #[serde(default)]
        from: Epoch,
    },
    /// Back with the committer since `epoch`.
    Returned {
        epoch: Epoch,
        #[serde(default)]
        from: Epoch,
    },
}

impl MetaslabGrant {
    /// The epoch of the lend or return.
    pub fn epoch(&self) -> Epoch {
        match self {
            MetaslabGrant::Lent { epoch, .. } | MetaslabGrant::Returned { epoch, .. } => *epoch,
        }
    }

    /// The writer lease it's lent to.
    pub fn writer(&self) -> Option<&LeaseRef> {
        match self {
            MetaslabGrant::Lent { writer, .. } => Some(writer),
            MetaslabGrant::Returned { .. } => None,
        }
    }

    /// What a disk server needs of it.
    pub fn fence(&self) -> crate::fence::Grant {
        match self {
            MetaslabGrant::Lent { epoch, .. } => crate::fence::Grant::Lent(*epoch),
            MetaslabGrant::Returned { epoch, .. } => crate::fence::Grant::Returned(*epoch),
        }
    }
}

/// A change to the cluster state. Callers propose these (see
/// `crate::cluster::Cluster::propose`); the leader stamps them into a
/// [`Request`].
/// The longest lease a [`LeaseOp`] can ask for. Leases are renewed every
/// third of `LEASE_TTL` (10 s), so this is far more than any real holder
/// asks for. It bounds what a client can send: an unbounded `ttl_ms` would
/// overflow `now_ms + ttl_ms` in `apply`, on every node and on every replay
/// of the log (structural triage I-16).
pub const MAX_LEASE_TTL_MS: u64 = 10 * 60 * 1000;

/// When a lease granted or renewed at `now_ms` for `ttl_ms` expires.
/// `ttl_ms` is clamped to [`MAX_LEASE_TTL_MS`], so this never overflows.
pub fn lease_expiry(now_ms: u64, ttl_ms: u64) -> u64 {
    now_ms.saturating_add(ttl_ms.min(MAX_LEASE_TTL_MS))
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LeaseOp {
    Acquire {
        resource: LeaseResource,
        owner: String,
        holder: Holder,
        ttl_ms: u64,
    },
    /// Extend `holder`'s `resource` lease (even past expiry, as long as
    /// nobody took it in between).
    Renew {
        resource: LeaseResource,
        holder: Holding,
        ttl_ms: u64,
    },
    Release {
        resource: LeaseResource,
        holder: Holding,
    },
    /// Hand a lease straight to another owner: applied only if `from`
    /// holds the live `resource` lease. `to_owner` gets it at a new epoch
    /// in the same step, so it's never free in between (a plain release
    /// and acquire would let any other node take it), and the new epoch
    /// fences whatever the old holder still tries to write.
    Transfer {
        resource: LeaseResource,
        from: Holding,
        to_owner: String,
        to_holder: Holder,
        ttl_ms: u64,
    },
    /// The NFSv4 state owner (`nfs`, the `nfs4` lease holder) records that
    /// NFSv4 client `client` (its owner string, hex) has state that may be
    /// reclaimed after a failover, or (`present: false`) that it hasn't any
    /// more.
    NfsClient {
        client: String,
        present: bool,
        nfs: Holding,
    },
    /// The committer (`pool`, the current `pool` holder) lends metaslab
    /// `idx` to writer lease `writer`, if its grant is still the one of
    /// epoch `after` (none: never granted). Sent again after it was
    /// applied, it's answered as the first time; after anything else
    /// happened to the metaslab, refused (audit R4d).
    Lend {
        idx: MetaslabIdx,
        writer: LeaseRef,
        pool: Holding,
        #[serde(default)]
        after: Epoch,
    },
    /// The committer takes metaslab `idx` back from the lend of epoch
    /// `lend`. Idempotent like `Lend`.
    Return {
        idx: MetaslabIdx,
        pool: Holding,
        #[serde(default)]
        lend: Epoch,
    },
    /// Whether VM `name` should be running (see `crate::vm`). Only the
    /// intent: what actually runs is whatever libvirt says on the node that
    /// holds the VM's disk.
    SetVm { name: String, running: bool },
    /// The key S3/SQS access keys are derived with (see
    /// `crate::frontend::keys`): set once, by whichever node asks first.
    SetServiceKey { key: String },
    /// The nodes volume `volume` is attached to (exported on), most
    /// preferred writer first; empty: back to its own placement. Only
    /// intent: who writes it is still its `ds/` lease (see `crate::agent`).
    SetAttached { volume: String, nodes: Vec<String> },
    /// Attach `volume` to `node` too (last in its list), or (`on: false`)
    /// no longer: one node at a time, so callers attaching different nodes
    /// at once (a CSI attacher) don't undo each other.
    Attach {
        volume: String,
        node: String,
        on: bool,
    },
}

impl LeaseOp {
    /// Stamp with the leader's clock, ready to propose.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn stamp(self, now_ms: u64) -> Request {
        Request { op: self, now_ms }
    }
}

/// A command proposed through Raft: an op and the leader's clock when it
/// was proposed, so every replica computes the same result.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub op: LeaseOp,
    pub now_ms: u64,
}

/// What applying a [`Request`] produced.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Response {
    /// The caller holds the lease at `epoch` until `expires_ms`.
    Granted {
        epoch: Epoch,
        expires_ms: u64,
    },
    /// Someone else holds it.
    Held {
        owner: String,
        holder: Holder,
        expires_ms: u64,
    },
    /// The caller's lease is gone (taken over, or released).
    Lost,
    Released,
    /// A metaslab lend/return went through; `epoch` fences its data now.
    Fenced {
        epoch: Epoch,
    },
    /// A lend/return was refused (not the committer, writer not live, ...).
    Refused(String),
    /// An NFSv4 client record was written.
    Recorded,
    /// Raft-internal entries (blank, membership).
    #[default]
    None,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub owner: String,
    pub holder: Holder,
    pub epoch: Epoch,
    pub expires_ms: u64,
    pub released: bool,
}

impl Lease {
    /// Held (not released, not expired) at `now_ms`.
    pub fn is_live(&self, now_ms: u64) -> bool {
        !self.released && self.expires_ms > now_ms
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterState {
    /// The newest epoch handed out (none yet: `Epoch::NONE`).
    pub epoch: Epoch,
    /// The newest leader time applied: the state machine's clock. It never
    /// goes back, so a new leader whose clock is behind can't un-expire a
    /// lease (or extend one) by stamping an earlier time (audit R3a). A
    /// leader clock far *ahead* still expires leases early: bounded clock
    /// skew between nodes (well under a lease's safety margin) is assumed.
    #[serde(default)]
    pub clock_ms: u64,
    /// Every resource ever leased. Released or expired leases stay, so the
    /// resource's last epoch is never forgotten.
    pub leases: BTreeMap<LeaseResource, Lease>,
    /// Metaslabs ever lent, by index. Never forgotten, like leases.
    #[serde(default)]
    pub metaslabs: BTreeMap<MetaslabIdx, MetaslabGrant>,
    /// NFSv4 clients that may reclaim state after a state owner failover:
    /// owner string (hex) → when recorded.
    #[serde(default)]
    pub nfs_clients: BTreeMap<String, u64>,
    /// VMs that should be running: name → when that was asked. Absent:
    /// stopped (see `crate::vm`).
    #[serde(default)]
    pub vms: BTreeMap<String, u64>,
    /// The key S3/SQS secrets are derived with (hex), once some node made
    /// one. Secret: `crate::cluster::Cluster::status` leaves it out.
    #[serde(default)]
    pub service_key: Option<String>,
    /// Volumes attached to several nodes at once (a live migration, a
    /// multi-node block volume): name → nodes. Absent: placed as usual.
    #[serde(default)]
    pub attached: BTreeMap<String, Vec<String>>,
}

impl ClusterState {
    /// Epoch of the newest `pool` grant: block writes older than this are stale.
    pub fn pool_epoch(&self) -> Epoch {
        self.leases.get(&POOL).map_or(Epoch::NONE, |l| l.epoch)
    }

    /// The epoch that fences writes to `domain`: the pool lease for the pool
    /// and metaslabs the committer keeps, the lend for lent metaslabs.
    pub fn fence_epoch(&self, domain: Domain) -> Epoch {
        match domain {
            Domain::Pool => self.pool_epoch(),
            Domain::Metaslab(i) => match self.metaslabs.get(&i) {
                Some(MetaslabGrant::Lent { epoch, .. }) => *epoch,
                Some(MetaslabGrant::Returned { epoch, .. }) => (*epoch).max(self.pool_epoch()),
                None => self.pool_epoch(),
            },
        }
    }

    /// The writer lease `idx` is lent to (and its epoch), if any.
    pub fn lent_to(&self, idx: MetaslabIdx) -> Option<&LeaseRef> {
        self.metaslabs.get(&idx).and_then(MetaslabGrant::writer)
    }

    /// The live lease on `resource`, if any.
    pub fn live_lease(&self, resource: &LeaseResource, now_ms: u64) -> Option<&Lease> {
        // Never earlier than the cluster's own clock: a reader whose clock
        // lags must not see a lease the cluster has expired as live.
        let now_ms = now_ms.max(self.clock_ms);
        self.leases.get(resource).filter(|l| l.is_live(now_ms))
    }

    /// The node whose lease `resource` is (live), if a node's.
    pub fn holder_node(&self, resource: &LeaseResource, now_ms: u64) -> Option<&str> {
        self.live_lease(resource, now_ms)?.holder.node.as_deref()
    }

    /// The node running dataset `name` (its agent holding its live `ds/`
    /// lease). Not the committer replaying a gone writer's log: that holds
    /// the lease, but runs nothing (see [`Self::dataset_held`]).
    pub fn dataset_node(&self, name: &str, now_ms: u64) -> Option<&str> {
        let l = self.live_lease(&LeaseResource::dataset(name), now_ms)?;
        match l.holder.role {
            HolderRole::Agent => l.holder.node.as_deref(),
            _ => None,
        }
    }

    /// Whether anyone (an agent, or the committer replaying) holds dataset
    /// `name`'s live `ds/` lease: who'd run it must wait for them.
    pub fn dataset_held(&self, name: &str, now_ms: u64) -> bool {
        self.live_lease(&LeaseResource::dataset(name), now_ms)
            .is_some()
    }

    /// The nodes whose agents are running (hold their live `node/` lease),
    /// with their leases.
    pub fn live_nodes(&self, now_ms: u64) -> impl Iterator<Item = (&str, &Lease)> {
        let now_ms = now_ms.max(self.clock_ms);
        self.leases.iter().filter_map(move |(r, l)| match r {
            LeaseResource::Node(n) if l.is_live(now_ms) => Some((n.as_str(), l)),
            _ => None,
        })
    }

    /// Does `holder` hold the live `resource` lease?
    pub fn holds(&self, resource: &LeaseResource, holder: &Holding, now_ms: u64) -> bool {
        self.live_lease(resource, now_ms)
            .is_some_and(|l| l.epoch == holder.epoch && l.owner == holder.owner)
    }

    /// Is `lease` (whoever holds it) live?
    pub fn is_live(&self, lease: &LeaseRef, now_ms: u64) -> bool {
        self.live_lease(&lease.resource, now_ms)
            .is_some_and(|l| l.epoch == lease.epoch)
    }

    pub fn apply(&mut self, req: &Request) -> Response {
        self.clock_ms = self.clock_ms.max(req.now_ms);
        let now_ms = self.clock_ms;
        match &req.op {
            LeaseOp::Lend {
                idx,
                writer,
                pool,
                after,
            } => {
                if !self.holds(&POOL, pool, now_ms) {
                    return Response::Refused("not the committer".into());
                }
                let current = self.metaslabs.get(idx);
                if let Some(MetaslabGrant::Lent {
                    writer: w,
                    epoch,
                    from,
                }) = current
                {
                    // This very lend, sent again: answered as the first time.
                    if w == writer && from == after {
                        return Response::Fenced { epoch: *epoch };
                    }
                    return Response::Refused(format!(
                        "metaslab {} is lent to {}",
                        idx, w.resource
                    ));
                }
                let now = current.map_or(Epoch::NONE, MetaslabGrant::epoch);
                if now != *after {
                    return Response::Refused(format!(
                        "metaslab {} changed hands (its grant is of epoch {}, not {})",
                        idx, now, after
                    ));
                }
                if !self.is_live(writer, now_ms) {
                    return Response::Refused(format!("{} is not live", writer));
                }
                self.epoch = self.epoch.next();
                self.metaslabs.insert(
                    *idx,
                    MetaslabGrant::Lent {
                        writer: writer.clone(),
                        epoch: self.epoch,
                        from: *after,
                    },
                );
                Response::Fenced { epoch: self.epoch }
            }
            LeaseOp::Return { idx, pool, lend } => {
                if !self.holds(&POOL, pool, now_ms) {
                    return Response::Refused("not the committer".into());
                }
                match self.metaslabs.get(idx) {
                    Some(MetaslabGrant::Lent { epoch, .. }) if epoch == lend => {}
                    // This very return, sent again.
                    Some(MetaslabGrant::Returned { epoch, from }) if from == lend => {
                        return Response::Fenced { epoch: *epoch };
                    }
                    other => {
                        return Response::Refused(format!(
                            "metaslab {} isn't on the lend of epoch {} ({:?})",
                            idx, lend, other
                        ));
                    }
                }
                self.epoch = self.epoch.next();
                self.metaslabs.insert(
                    *idx,
                    MetaslabGrant::Returned {
                        epoch: self.epoch,
                        from: *lend,
                    },
                );
                Response::Fenced { epoch: self.epoch }
            }
            LeaseOp::Acquire {
                resource,
                owner,
                holder,
                ttl_ms,
            } => {
                let expires_ms = lease_expiry(now_ms, *ttl_ms);
                match self.leases.get_mut(resource) {
                    Some(l) if l.is_live(now_ms) && l.owner != *owner => Response::Held {
                        owner: l.owner.clone(),
                        holder: l.holder.clone(),
                        expires_ms: l.expires_ms,
                    },
                    // Asking again for a lease we hold: same epoch, extended.
                    Some(l) if l.is_live(now_ms) => {
                        l.expires_ms = l.expires_ms.max(expires_ms);
                        Response::Granted {
                            epoch: l.epoch,
                            expires_ms: l.expires_ms,
                        }
                    }
                    _ => {
                        self.epoch = self.epoch.next();
                        self.leases.insert(
                            resource.clone(),
                            Lease {
                                owner: owner.clone(),
                                holder: holder.clone(),
                                epoch: self.epoch,
                                expires_ms,
                                released: false,
                            },
                        );
                        Response::Granted {
                            epoch: self.epoch,
                            expires_ms,
                        }
                    }
                }
            }
            LeaseOp::Renew {
                resource,
                holder,
                ttl_ms,
            } => match self.leases.get_mut(resource) {
                // Renewing after expiry is fine as long as nobody took the
                // lease in between: the epoch is unchanged, so nobody else
                // can have written.
                Some(l) if l.owner == holder.owner && l.epoch == holder.epoch && !l.released => {
                    l.expires_ms = l.expires_ms.max(lease_expiry(now_ms, *ttl_ms));
                    Response::Granted {
                        epoch: l.epoch,
                        expires_ms: l.expires_ms,
                    }
                }
                _ => Response::Lost,
            },
            LeaseOp::Release { resource, holder } => match self.leases.get_mut(resource) {
                Some(l) if l.owner == holder.owner && l.epoch == holder.epoch => {
                    l.released = true;
                    Response::Released
                }
                _ => Response::Lost,
            },
            LeaseOp::Transfer {
                resource,
                from,
                to_owner,
                to_holder,
                ttl_ms,
            } => {
                if to_owner.is_empty() || *to_owner == from.owner {
                    return Response::Refused("no new owner to transfer to".into());
                }
                if self.holds(resource, from, now_ms) {
                    self.epoch = self.epoch.next();
                    let expires_ms = lease_expiry(now_ms, *ttl_ms);
                    self.leases.insert(
                        resource.clone(),
                        Lease {
                            owner: to_owner.clone(),
                            holder: to_holder.clone(),
                            epoch: self.epoch,
                            expires_ms,
                            released: false,
                        },
                    );
                    return Response::Granted {
                        epoch: self.epoch,
                        expires_ms,
                    };
                }
                match self.leases.get(resource) {
                    // The same transfer again (clients send every request to
                    // every node, see `LeaseClient::call`): it went through.
                    Some(l)
                        if l.owner == *to_owner && l.epoch > from.epoch && l.is_live(now_ms) =>
                    {
                        Response::Granted {
                            epoch: l.epoch,
                            expires_ms: l.expires_ms,
                        }
                    }
                    _ => Response::Lost,
                }
            }
            LeaseOp::NfsClient {
                client,
                present,
                nfs,
            } => {
                if !self.holds(&NFS4, nfs, now_ms) {
                    return Response::Refused("not the NFSv4 state owner".into());
                }
                if *present {
                    self.nfs_clients.insert(client.clone(), now_ms);
                } else {
                    self.nfs_clients.remove(client);
                }
                Response::Recorded
            }
            LeaseOp::SetVm { name, running } => {
                if *running {
                    self.vms.insert(name.clone(), now_ms);
                } else {
                    self.vms.remove(name);
                }
                Response::Recorded
            }
            LeaseOp::SetAttached { volume, nodes } => {
                if nodes.is_empty() {
                    self.attached.remove(volume);
                } else {
                    let mut seen = Vec::new();
                    for n in nodes {
                        if !seen.contains(n) {
                            seen.push(n.clone());
                        }
                    }
                    self.attached.insert(volume.clone(), seen);
                }
                Response::Recorded
            }
            LeaseOp::Attach { volume, node, on } => {
                let nodes = self.attached.entry(volume.clone()).or_default();
                match on {
                    true if !nodes.contains(node) => nodes.push(node.clone()),
                    true => {}
                    false => nodes.retain(|n| n != node),
                }
                if nodes.is_empty() {
                    self.attached.remove(volume);
                }
                Response::Recorded
            }
            LeaseOp::SetServiceKey { key } => {
                if self.service_key.is_none() {
                    self.service_key = Some(key.clone());
                }
                Response::Recorded
            }
        }
    }
}
