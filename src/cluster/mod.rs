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

#[cfg(not(target_arch = "wasm32"))]
pub mod lease;
#[cfg(not(target_arch = "wasm32"))]
pub mod members;
#[cfg(not(target_arch = "wasm32"))]
pub mod network;
#[cfg(not(target_arch = "wasm32"))]
pub mod routes;
pub mod state;
#[cfg(not(target_arch = "wasm32"))]
pub mod store;

#[cfg(not(target_arch = "wasm32"))]
use openraft::error::{ClientWriteError, RaftError};
#[cfg(not(target_arch = "wasm32"))]
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use state::ClusterState;
pub use state::LeaseOp;
#[cfg(not(target_arch = "wasm32"))]
use state::{Request, Response};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Cursor;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use store::Store;

#[cfg(not(target_arch = "wasm32"))]
openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

#[cfg(not(target_arch = "wasm32"))]
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ClusterConfig {
    /// Where this node keeps its Raft state (default `~/.raidish/<name>`).
    pub data_dir: Option<String>,
}

/// Header carrying the sender's layout fingerprint on Raft RPCs.
#[cfg(not(target_arch = "wasm32"))]
pub const FINGERPRINT_HEADER: &str = "X-Raidish-Layout";
/// Header carrying the id the sender believes the receiver has. A node
/// whose config disagrees about who is who gets refused.
#[cfg(not(target_arch = "wasm32"))]
pub const TO_HEADER: &str = "X-Raidish-To";
/// Header carrying the Raft protocol version the sender speaks.
#[cfg(not(target_arch = "wasm32"))]
pub const PROTOCOL_HEADER: &str = "X-Raidish-Raft";
/// The version of the Raft messages and the entries in them (`LeaseOp`,
/// the state machine's records). Bump it with any change to them: nodes
/// of different versions refuse each other's Raft traffic, and a node
/// won't start on Raft data of another version.
#[cfg(not(target_arch = "wasm32"))]
pub const RAFT_PROTOCOL: u32 = 1;

/// How long a lease request waits for a leader (one that answers) before
/// giving up: a few election timeouts, and under the clients' request
/// timeout (see `lease::LeaseClient`).
#[cfg(not(target_arch = "wasm32"))]
pub const FORWARD_WAIT: Duration = Duration::from_secs(4);

pub use crate::types::now_ms;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MemberView {
    pub id: u64,
    /// None for the node that answered.
    pub addr: Option<String>,
    pub disks: Vec<usize>,
}

/// `GET /api/v1/cluster/status` (what `raidish cluster status` prints).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

impl Status {
    /// The member serving pool disk `disk`.
    pub fn owner_of(&self, disk: usize) -> Option<&MemberView> {
        self.members.iter().find(|m| m.disks.contains(&disk))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct Cluster {
    pub name: String,
    pub raft: Raft<TypeConfig>,
    pub members: members::Members,
    store: Store,
    http: reqwest::Client,
}

#[cfg(not(target_arch = "wasm32"))]
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
        let http = crate::tls::http_client(None)?;
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
    ///
    /// While there's no leader, or the one this node knows of doesn't
    /// answer (it has just stopped, say), it waits for the next one for up
    /// to [`FORWARD_WAIT`] and tries again: losing the leader shouldn't
    /// cost healthy holders their leases, so their renewals have to reach
    /// the new leader as soon as there is one.
    pub async fn lease(&self, op: &LeaseOp, forwarded: bool) -> Result<Response, String> {
        let deadline = tokio::time::Instant::now() + FORWARD_WAIT;
        loop {
            let (leader, err) = match self.raft.client_write(op.clone().stamp(now_ms())).await {
                Ok(resp) => return Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) if !forwarded => {
                    match f.leader_id.and_then(|id| self.members.peers.get(&id)) {
                        None => (None, "no cluster leader (quorum lost?)".to_string()),
                        Some(addr) => match self.forward(addr, op, deadline).await {
                            Ok(r) => return r,
                            Err(e) => (f.leader_id, e),
                        },
                    }
                }
                Err(e) => return Err(e.to_string()),
            };
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(err);
            }
            // A (different) leader, or a second's pause before asking the
            // same one again.
            let _ = self
                .raft
                .wait(Some(left.min(Duration::from_secs(1))))
                .metrics(
                    |m| m.current_leader.is_some() && m.current_leader != leader,
                    "a new leader",
                )
                .await;
        }
    }

    /// Send `op` to the leader at `addr`. `Err` if it didn't answer; its
    /// answer, refusals included, otherwise.
    async fn forward(
        &self,
        addr: &str,
        op: &LeaseOp,
        deadline: tokio::time::Instant,
    ) -> Result<Result<Response, String>, String> {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let url = format!("{}/api/v1/cluster/lease?forwarded=true", addr);
        let resp = self
            .http
            .post(&url)
            .timeout(left.max(Duration::from_secs(1)))
            .json(op)
            .send()
            .await
            .map_err(|e| format!("forwarding to leader: {}", e))?;
        resp.json::<Result<Response, String>>()
            .await
            .map_err(|e| format!("forwarding to leader: {}", e))
    }

    /// The NFSv4 client records (owner strings, hex) as this node has them.
    pub fn nfs_clients(&self) -> Vec<String> {
        self.store.nfs_clients()
    }

    /// The key S3/SQS secrets are derived with, if made yet.
    pub fn service_key(&self) -> Option<Vec<u8>> {
        self.store
            .state()
            .service_key
            .and_then(|k| hex::decode(k).ok())
    }

    /// The service key, made (randomly, by this node) if there's none yet.
    pub async fn ensure_service_key(&self) -> Result<Vec<u8>, String> {
        if let Some(k) = self.service_key() {
            return Ok(k);
        }
        let mut key = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut key)
            .map_err(|_| "no randomness")?;
        self.lease(
            &LeaseOp::SetServiceKey {
                key: hex::encode(key),
            },
            false,
        )
        .await?;
        // Whichever node's key went in first (applied here by now).
        for _ in 0..50 {
            if let Some(k) = self.service_key() {
                return Ok(k);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err("the service key didn't arrive".into())
    }

    /// Lease `resource` as this node's applied state has it.
    pub fn lease_state(&self, resource: &crate::types::LeaseResource) -> Option<state::Lease> {
        self.store.lease(resource)
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
            state: ClusterState {
                // Never handed out (status is for any reader).
                service_key: None,
                ..self.store.state()
            },
            now_ms: now_ms(),
        }
    }
}

/// `~/.raidish/<name>`, or `./.raidish/<name>` without a home directory.
#[cfg(not(target_arch = "wasm32"))]
pub fn default_data_dir(name: &str) -> PathBuf {
    let base = std::env::var("HOME").map_or(PathBuf::from("."), PathBuf::from);
    base.join(".raidish").join(name)
}

#[cfg(test)]
#[path = "../tests/cluster_tests.rs"]
mod tests;
