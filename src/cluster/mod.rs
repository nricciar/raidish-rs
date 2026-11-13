//! The control plane: every `listen` node runs a member of one small Raft
//! group (openraft) that hands out leases, so there's nothing extra to
//! deploy. The members come from the pool config (see [`members`]): each
//! remote disk's `listen` node is one, local disks are this node, and a pool
//! of local disks only is a cluster of one.
//!
//! Raft traffic and lease requests share the disk server's HTTP port. The
//! members are fixed when the group first starts (no membership changes yet).

// openraft's error types are large by design.
#![allow(clippy::result_large_err)]

pub mod lease;
pub mod members;
pub mod network;
pub mod routes;
pub mod state;
pub mod store;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use state::{ClusterState, Request, Response};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use store::Store;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ClusterConfig {
    /// Where this node keeps its Raft state (default `~/.raidish/<name>`).
    pub data_dir: Option<String>,
}

/// Header carrying the sender's layout fingerprint on Raft RPCs.
pub const FINGERPRINT_HEADER: &str = "X-Raidish-Layout";
/// Header carrying the id the sender believes the receiver has. A node
/// whose config disagrees about who is who gets refused.
pub const TO_HEADER: &str = "X-Raidish-To";

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// A lease request as sent by clients (the leader adds the time).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LeaseOp {
    Acquire {
        resource: String,
        owner: String,
        desc: String,
        ttl_ms: u64,
    },
    Renew {
        resource: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
    },
    Release {
        resource: String,
        owner: String,
        epoch: u64,
    },
    Lend {
        idx: u32,
        writer: String,
        writer_epoch: u64,
        pool_owner: String,
        pool_epoch: u64,
    },
    Return {
        idx: u32,
        pool_owner: String,
        pool_epoch: u64,
    },
}

impl LeaseOp {
    pub(crate) fn stamp(&self, now_ms: u64) -> Request {
        match self.clone() {
            LeaseOp::Acquire {
                resource,
                owner,
                desc,
                ttl_ms,
            } => Request::Acquire {
                resource,
                owner,
                desc,
                ttl_ms,
                now_ms,
            },
            LeaseOp::Renew {
                resource,
                owner,
                epoch,
                ttl_ms,
            } => Request::Renew {
                resource,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            },
            LeaseOp::Release {
                resource,
                owner,
                epoch,
            } => Request::Release {
                resource,
                owner,
                epoch,
            },
            LeaseOp::Lend {
                idx,
                writer,
                writer_epoch,
                pool_owner,
                pool_epoch,
            } => Request::Lend {
                idx,
                writer,
                writer_epoch,
                pool_owner,
                pool_epoch,
                now_ms,
            },
            LeaseOp::Return {
                idx,
                pool_owner,
                pool_epoch,
            } => Request::Return {
                idx,
                pool_owner,
                pool_epoch,
                now_ms,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MemberView {
    pub id: u64,
    /// None for the node that answered.
    pub addr: Option<String>,
    pub disks: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Status {
    /// The node that answered (name and id).
    pub node: String,
    pub id: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub members: Vec<MemberView>,
    pub fingerprint: String,
    pub state: ClusterState,
    pub now_ms: u64,
}

pub struct Cluster {
    pub name: String,
    pub raft: Raft<TypeConfig>,
    pub members: members::Members,
    store: Store,
    http: reqwest::Client,
}

impl Cluster {
    /// Start this node's Raft member. `fence` learns every grant as it is
    /// applied (the disk server fences with it).
    pub async fn start(
        name: &str,
        members: members::Members,
        data_dir: PathBuf,
        fence: Arc<crate::fence::ServerFence>,
    ) -> Result<Arc<Cluster>, String> {
        let config = openraft::Config {
            cluster_name: "raidish".into(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(200),
            max_in_snapshot_log_to_keep: 50,
            ..Default::default()
        };
        let config = Arc::new(config.validate().map_err(|e| e.to_string())?);
        let store = Store::open(&data_dir, fence)?;
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .map_err(|e| e.to_string())?;
        let network = network::Network {
            http: http.clone(),
            peers: Arc::new(members.peers.clone()),
            fingerprint: members.fingerprint.clone(),
        };
        let raft = Raft::new(members.me, config, network, log_store, state_machine)
            .await
            .map_err(|e| e.to_string())?;

        // Every node initializes with the same membership (ids only, no
        // addresses), so whichever wins the first election, they agree.
        // Already-initialized nodes just restart.
        let initial: BTreeMap<u64, BasicNode> =
            members.ids().map(|id| (id, BasicNode::default())).collect();
        if !raft.is_initialized().await.map_err(|e| e.to_string())?
            && let Err(e) = raft.initialize(initial).await
        {
            eprintln!("cluster: initialize: {}", e);
        }

        let cluster = Arc::new(Cluster {
            name: name.to_string(),
            raft,
            members,
            store,
            http,
        });
        let weak = Arc::downgrade(&cluster);
        tokio::spawn(async move {
            // Check every peer once it answers, then stop.
            let mut pending: Vec<u64> = match weak.upgrade() {
                Some(c) => c.members.peers.keys().copied().collect(),
                None => return,
            };
            while !pending.is_empty() {
                let Some(c) = weak.upgrade() else { return };
                let mut left = Vec::new();
                for id in pending {
                    match c.check_peer(id).await {
                        Ok(None) => {}
                        Ok(Some(problem)) => eprintln!("cluster: CONFIG MISMATCH: {}", problem),
                        Err(_) => left.push(id),
                    }
                }
                pending = left;
                drop(c);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(cluster)
    }

    /// Ask peer `id` who it is. `Ok(Some(problem))` if its answer doesn't
    /// match this node's view of the pool, `Err` if it didn't answer.
    pub async fn check_peer(&self, id: u64) -> Result<Option<String>, String> {
        let addr = self.members.peers.get(&id).ok_or("unknown peer")?;
        let status: Status = self
            .http
            .get(format!("{}/api/v1/cluster/status", addr))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(if status.id != id {
            Some(format!(
                "{} says it is node {} (disks {:?}), but this node's pool config makes it node {} \
                 (disks {:?}): the nodes list the pool's disks in different orders",
                addr,
                status.id,
                status
                    .members
                    .iter()
                    .find(|m| m.id == status.id)
                    .map(|m| m.disks.clone())
                    .unwrap_or_default(),
                id,
                self.members.disks.get(&id).cloned().unwrap_or_default()
            ))
        } else if status.fingerprint != self.members.fingerprint {
            Some(format!(
                "{} (node {}) has a different pool layout (fingerprint {}, ours {})",
                addr, id, status.fingerprint, self.members.fingerprint
            ))
        } else {
            None
        })
    }

    /// Apply a lease op, forwarding it to the leader if this node isn't.
    pub async fn lease(&self, op: &LeaseOp, forwarded: bool) -> Result<Response, String> {
        match self.raft.client_write(op.stamp(now_ms())).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) if !forwarded => {
                let Some(addr) = f.leader_id.and_then(|id| self.members.peers.get(&id)) else {
                    return Err("no cluster leader (quorum lost?)".into());
                };
                let url = format!("{}/api/v1/cluster/lease?forwarded=true", addr);
                let resp = self
                    .http
                    .post(&url)
                    .timeout(Duration::from_secs(5))
                    .json(op)
                    .send()
                    .await
                    .map_err(|e| format!("forwarding to leader: {}", e))?;
                resp.json::<Result<Response, String>>()
                    .await
                    .map_err(|e| format!("forwarding to leader: {}", e))?
            }
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn status(&self) -> Status {
        let m = self.raft.metrics().borrow().clone();
        Status {
            node: self.name.clone(),
            id: self.members.me,
            leader: m.current_leader,
            term: m.current_term,
            members: self
                .members
                .disks
                .iter()
                .map(|(id, disks)| MemberView {
                    id: *id,
                    addr: self.members.peers.get(id).cloned(),
                    disks: disks.clone(),
                })
                .collect(),
            fingerprint: self.members.fingerprint.clone(),
            state: self.store.state(),
            now_ms: now_ms(),
        }
    }
}

/// `~/.raidish/<name>`, or `./.raidish/<name>` without a home directory.
pub fn default_data_dir(name: &str) -> PathBuf {
    let base = std::env::var("HOME").map_or(PathBuf::from("."), PathBuf::from);
    base.join(".raidish").join(name)
}

#[cfg(test)]
#[path = "../tests/cluster_tests.rs"]
mod tests;
