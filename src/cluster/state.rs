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

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The resource a pool writer (the committer) leases.
pub const POOL: &str = "pool";

pub use crate::fence::Domain;

/// Who may write a metaslab's data, and since which epoch.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MetaslabGrant {
    /// The writer lease resource (`writer/<node>`) it's lent to; None:
    /// back with the committer.
    pub writer: Option<String>,
    /// That writer lease's epoch.
    pub writer_epoch: u64,
    /// Epoch of the lend (or of the return): the fencing token of the
    /// metaslab while lent.
    pub epoch: u64,
}

/// A command proposed through Raft.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Acquire {
        resource: String,
        owner: String,
        desc: String,
        ttl_ms: u64,
        now_ms: u64,
    },
    Renew {
        resource: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    },
    Release {
        resource: String,
        owner: String,
        epoch: u64,
    },
    /// The committer (current `pool` holder) lends metaslab `idx` to the
    /// holder of `writer` at `writer_epoch`.
    Lend {
        idx: u32,
        writer: String,
        writer_epoch: u64,
        pool_owner: String,
        pool_epoch: u64,
        now_ms: u64,
    },
    /// The committer takes metaslab `idx` back.
    Return {
        idx: u32,
        pool_owner: String,
        pool_epoch: u64,
        now_ms: u64,
    },
}

/// What applying a [`Request`] produced.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Response {
    /// The caller holds the lease at `epoch` until `expires_ms`.
    Granted {
        epoch: u64,
        expires_ms: u64,
    },
    /// Someone else holds it.
    Held {
        owner: String,
        desc: String,
        expires_ms: u64,
    },
    /// The caller's lease is gone (taken over, or released).
    Lost,
    Released,
    /// A metaslab lend/return went through; `epoch` fences its data now.
    Fenced {
        epoch: u64,
    },
    /// A lend/return was refused (not the committer, writer not live, ...).
    Refused(String),
    /// Raft-internal entries (blank, membership).
    #[default]
    None,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub owner: String,
    pub desc: String,
    pub epoch: u64,
    pub expires_ms: u64,
    pub released: bool,
}

impl Lease {
    fn live(&self, now_ms: u64) -> bool {
        !self.released && self.expires_ms > now_ms
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterState {
    /// The newest epoch handed out (0: none yet).
    pub epoch: u64,
    /// Every resource ever leased. Released or expired leases stay, so the
    /// resource's last epoch is never forgotten.
    pub leases: BTreeMap<String, Lease>,
    /// Metaslabs ever lent, by index. Never forgotten, like leases.
    #[serde(default)]
    pub metaslabs: BTreeMap<u32, MetaslabGrant>,
}

impl ClusterState {
    /// Epoch of the newest `pool` grant: block writes older than this are stale.
    pub fn pool_epoch(&self) -> u64 {
        self.leases.get(POOL).map_or(0, |l| l.epoch)
    }

    /// The epoch that fences writes to `domain`: the pool lease for the pool
    /// and metaslabs the committer keeps, the lend for lent metaslabs.
    pub fn fence_epoch(&self, domain: Domain) -> u64 {
        match domain {
            Domain::Pool => self.pool_epoch(),
            Domain::Metaslab(i) => match self.metaslabs.get(&i) {
                Some(g) if g.writer.is_some() => g.epoch,
                Some(g) => g.epoch.max(self.pool_epoch()),
                None => self.pool_epoch(),
            },
        }
    }

    /// Is `idx` lent to a writer?
    pub fn lent_to(&self, idx: u32) -> Option<&MetaslabGrant> {
        self.metaslabs.get(&idx).filter(|g| g.writer.is_some())
    }

    /// Does `owner` hold the live `resource` lease at `epoch`?
    pub fn holds(&self, resource: &str, owner: Option<&str>, epoch: u64, now_ms: u64) -> bool {
        self.leases.get(resource).is_some_and(|l| {
            l.epoch == epoch && l.live(now_ms) && owner.is_none_or(|o| o == l.owner)
        })
    }

    pub fn apply(&mut self, req: &Request) -> Response {
        match req {
            Request::Lend {
                idx,
                writer,
                writer_epoch,
                pool_owner,
                pool_epoch,
                now_ms,
            } => {
                if !self.holds(POOL, Some(pool_owner), *pool_epoch, *now_ms) {
                    return Response::Refused("not the committer".into());
                }
                if !self.holds(writer, None, *writer_epoch, *now_ms) {
                    return Response::Refused(format!(
                        "{} is not live at {}",
                        writer, writer_epoch
                    ));
                }
                if let Some(g) = self.lent_to(*idx) {
                    return Response::Refused(format!(
                        "metaslab {} is lent to {:?}",
                        idx, g.writer
                    ));
                }
                self.epoch += 1;
                self.metaslabs.insert(
                    *idx,
                    MetaslabGrant {
                        writer: Some(writer.clone()),
                        writer_epoch: *writer_epoch,
                        epoch: self.epoch,
                    },
                );
                Response::Fenced { epoch: self.epoch }
            }
            Request::Return {
                idx,
                pool_owner,
                pool_epoch,
                now_ms,
            } => {
                if !self.holds(POOL, Some(pool_owner), *pool_epoch, *now_ms) {
                    return Response::Refused("not the committer".into());
                }
                self.epoch += 1;
                self.metaslabs.insert(
                    *idx,
                    MetaslabGrant {
                        writer: None,
                        writer_epoch: 0,
                        epoch: self.epoch,
                    },
                );
                Response::Fenced { epoch: self.epoch }
            }
            Request::Acquire {
                resource,
                owner,
                desc,
                ttl_ms,
                now_ms,
            } => {
                let expires_ms = now_ms + ttl_ms;
                match self.leases.get_mut(resource) {
                    Some(l) if l.live(*now_ms) && l.owner != *owner => Response::Held {
                        owner: l.owner.clone(),
                        desc: l.desc.clone(),
                        expires_ms: l.expires_ms,
                    },
                    // Asking again for a lease we hold: same epoch, extended.
                    Some(l) if l.live(*now_ms) => {
                        l.expires_ms = l.expires_ms.max(expires_ms);
                        Response::Granted {
                            epoch: l.epoch,
                            expires_ms: l.expires_ms,
                        }
                    }
                    _ => {
                        self.epoch += 1;
                        self.leases.insert(
                            resource.clone(),
                            Lease {
                                owner: owner.clone(),
                                desc: desc.clone(),
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
            Request::Renew {
                resource,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            } => match self.leases.get_mut(resource) {
                // Renewing after expiry is fine as long as nobody took the
                // lease in between: the epoch is unchanged, so nobody else
                // can have written.
                Some(l) if l.owner == *owner && l.epoch == *epoch && !l.released => {
                    l.expires_ms = l.expires_ms.max(now_ms + ttl_ms);
                    Response::Granted {
                        epoch: l.epoch,
                        expires_ms: l.expires_ms,
                    }
                }
                _ => Response::Lost,
            },
            Request::Release {
                resource,
                owner,
                epoch,
            } => match self.leases.get_mut(resource) {
                Some(l) if l.owner == *owner && l.epoch == *epoch => {
                    l.released = true;
                    Response::Released
                }
                _ => Response::Lost,
            },
        }
    }
}
