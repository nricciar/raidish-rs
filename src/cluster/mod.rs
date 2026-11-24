//! The control plane: every `listen` node runs a member of one small Raft
//! group (openraft) that hands out leases, so there's nothing extra to
//! deploy. The members come from the pool config (see [`members`]): each
//! remote disk's `listen` node is one, local disks are this node, and a pool
//! of local disks only is a cluster of one.
//!
//! Raft traffic and lease requests share the disk server's HTTP port. The
//! members are fixed when the group first starts (no membership changes yet).
//!
//! Starting without Raft state (audit R4b; see [`Cluster::start`]): the
//! group is formed only by nodes that all find each other fresh. A node
//! whose state is gone while the group exists (a wiped `data_dir`) acts as
//! a learner until it has caught up: Raft's safety rests on every voter
//! remembering its log and its votes, and it remembers neither.

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
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
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
use std::sync::atomic::Ordering::SeqCst;
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
    /// The answering node has Raft state (a log or a vote).
    #[serde(default)]
    pub initialized: bool,
    /// Its last log index. A group that never elected a leader has only
    /// the initial membership entry (index 0): nothing was ever committed.
    #[serde(default)]
    pub last_log_index: Option<u64>,
    /// It votes: false while it catches up after losing its state.
    #[serde(default)]
    pub voting: bool,
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
    data_dir: PathBuf,
    /// False while this node catches up (see [`Cluster::start`]): it grants
    /// no votes and doesn't campaign.
    voting: std::sync::atomic::AtomicBool,
    /// Votes are granted only in later terms than this: the newest term
    /// any peer reported when this node started without its state. It may
    /// have voted in any of those before it lost the record.
    vote_floor: std::sync::atomic::AtomicU64,
    /// See [`Cluster::misconfigured`].
    misconfigured: std::sync::atomic::AtomicBool,
}

/// What a peer's answer says about this node's view of the pool.
#[cfg(not(target_arch = "wasm32"))]
enum PeerCheck {
    Agrees,
    /// It's another node than this one's config has at its address.
    OtherId(String),
    /// Its pool layout differs.
    OtherLayout(String),
}

/// Why a vote request is refused (see [`Cluster::vote_refusal`]).
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Catching up after losing its Raft state: no votes at all.
    CatchingUp,
    /// At or below the vote floor (the term carried).
    Below(u64),
}

/// In the data dir while the node is catching up: a restart midway
/// resumes it (by then the node has a log, so that alone wouldn't tell).
#[cfg(not(target_arch = "wasm32"))]
const CATCHING_UP: &str = "catching-up";

/// What this node's peers say, for a node starting without Raft state.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, PartialEq, Eq)]
enum PeerView {
    /// Every peer answered and none has committed anything.
    Fresh,
    /// A peer has committed entries: the group exists.
    Formed,
    /// Not every peer answered (the reason), and none has committed.
    Unknown(String),
}

#[cfg(not(target_arch = "wasm32"))]
impl Cluster {
    /// Start this node's Raft member. `fence` learns every grant as it is
    /// applied (the disk server fences with it).
    ///
    /// A node without Raft state starts out not voting, and in the
    /// background (`bootstrap`):
    ///
    /// * If every peer answers and none has committed anything, the group
    ///   is new: it initializes (every node with the same membership, so
    ///   whichever wins the first election, they agree) and votes.
    /// * If a peer has committed entries, the group exists and this node
    ///   lost its state. It must not initialize (a membership log of its
    ///   own), nor vote (it may have acknowledged entries it no longer has
    ///   and so vote for a candidate missing them, or vote twice in a
    ///   term). It's a member the leader replicates to, like one that fell
    ///   behind; once it has applied up to the leader's read index, it has
    ///   everything it may have acknowledged before, and votes again.
    /// * Otherwise (peers unreachable) it waits: never initialize alone.
    pub async fn start(
        name: &str,
        members: members::Members,
        data_dir: PathBuf,
        fence: Arc<dyn crate::types::FenceObserver>,
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
        let network = network::Network {
            http: crate::tls::http_client(None)?,
            peers: Arc::new(members.peers.clone()),
            fingerprint: members.fingerprint.clone(),
        };
        let raft = Raft::new(members.me, config, network, log_store, state_machine)
            .await
            .map_err(|e| e.to_string())?;

        // Without Raft state, this node doesn't vote until it knows it may
        // (`bootstrap`). Recorded first, so a restart midway can't skip it.
        let http = crate::tls::http_client(None)?;
        let marker = data_dir.join(CATCHING_UP);
        if !raft.is_initialized().await.map_err(|e| e.to_string())? {
            write_durably(&marker)?;
        }
        let voting = !marker.exists();
        raft.runtime_config().elect(voting);

        let cluster = Arc::new(Cluster {
            name: name.to_string(),
            raft,
            members,
            store,
            http,
            data_dir,
            voting: std::sync::atomic::AtomicBool::new(voting),
            vote_floor: std::sync::atomic::AtomicU64::new(0),
            misconfigured: std::sync::atomic::AtomicBool::new(false),
        });
        if !voting {
            tokio::spawn(bootstrap(Arc::downgrade(&cluster)));
        }
        let weak = Arc::downgrade(&cluster);
        tokio::spawn(async move {
            // Check every peer once it answers, then stop.
            let mut pending: Vec<u64> = match weak.upgrade() {
                Some(c) => c.members.peers.keys().copied().collect(),
                None => return,
            };
            // Peers that agree with this node about who is who, and that
            // don't (another id at the address this node has for them).
            let (mut agree, mut other_id) = (0, 0);
            let peers = pending.len();
            while !pending.is_empty() {
                let Some(c) = weak.upgrade() else { return };
                let mut left = Vec::new();
                for id in pending {
                    match c.peer_check(id).await {
                        Ok(PeerCheck::Agrees) => agree += 1,
                        Ok(PeerCheck::OtherId(problem)) => {
                            other_id += 1;
                            eprintln!("cluster: CONFIG MISMATCH: {}", problem);
                        }
                        Ok(PeerCheck::OtherLayout(problem)) => {
                            eprintln!("cluster: CONFIG MISMATCH: {}", problem)
                        }
                        Err(_) => left.push(id),
                    }
                }
                // Most peers are at another id than this node has them at,
                // none agrees: this node's config is the odd one out. (Most,
                // not every one that answered so far: a good node hearing
                // only from the bad one, the others not up yet, must not
                // shut itself out; and since a quorum needs most peers up,
                // the bad node sits out by the time one could form.) It
                // sits out for good, refusing Raft traffic: accepting the
                // requests sent to its id, it would answer from elections
                // of its own that can never succeed (its requests reach the
                // wrong nodes, which refuse them), and its ever newer term
                // would make the others' candidates step down.
                if agree == 0 && other_id * 2 > peers && !c.misconfigured.swap(true, SeqCst) {
                    eprintln!(
                        "cluster: this node's pool config disagrees with every peer about \
                         which node is which: taking no part in the cluster until it's fixed"
                    );
                    c.raft.runtime_config().elect(false);
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
        Ok(match self.peer_check(id).await? {
            PeerCheck::Agrees => None,
            PeerCheck::OtherId(p) | PeerCheck::OtherLayout(p) => Some(p),
        })
    }

    /// Whether this node found its config to be the odd one out (see
    /// `start`): it then refuses all Raft traffic.
    pub fn misconfigured(&self) -> bool {
        self.misconfigured.load(SeqCst)
    }

    async fn peer_check(&self, id: u64) -> Result<PeerCheck, String> {
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
            PeerCheck::OtherId(format!(
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
            PeerCheck::OtherLayout(format!(
                "{} (node {}) has a different pool layout (fingerprint {}, ours {})",
                addr, id, status.fingerprint, self.members.fingerprint
            ))
        } else {
            PeerCheck::Agrees
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

    /// The cluster state, linearizably: everything committed before this
    /// call is in it (a read index). The leader confirms it still leads
    /// and names its commit index; a follower asks the leader for that
    /// index. Either then waits until it has applied that far.
    pub async fn linearizable_state(&self, timeout: Duration) -> Result<ClusterState, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        let index = self.read_index(false, deadline).await?;
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.raft
            .wait(Some(left))
            .applied_index_at_least(index, "read index")
            .await
            .map_err(|e| format!("applying up to the read index: {}", e))?;
        Ok(self.store.state())
    }

    /// The leader's read index: the commit index once it has confirmed,
    /// with a quorum, that it still leads. A follower forwards (unless the
    /// request was forwarded to it). With no leader known, or one that
    /// doesn't answer or no longer leads, it waits for the next one, as
    /// `lease` does, until `deadline`.
    pub async fn read_index(
        &self,
        forwarded: bool,
        deadline: tokio::time::Instant,
    ) -> Result<Option<u64>, String> {
        loop {
            let (leader, err) = match self.raft.get_read_log_id().await {
                Ok((read, _applied)) => return Ok(read.map(|l| l.index)),
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) if !forwarded => {
                    match f
                        .leader_id
                        .and_then(|id| self.members.peers.get(&id).map(|a| (id, a)))
                    {
                        None => (None, "no cluster leader (quorum lost?)".to_string()),
                        Some((id, addr)) => match self.leaders_read_index(id, addr, deadline).await
                        {
                            Ok(r) => return Ok(r),
                            Err(e) => (Some(id), e),
                        },
                    }
                }
                Err(e) => return Err(e.to_string()),
            };
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(err);
            }
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

    /// Ask leader `id` at `addr` for its read index.
    async fn leaders_read_index(
        &self,
        id: u64,
        addr: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Option<u64>, String> {
        let what =
            |e: &dyn std::fmt::Display| format!("asking the leader for its read index: {}", e);
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let resp = self
            .http
            .get(format!("{}/api/v1/cluster/read_index", addr))
            .header(FINGERPRINT_HEADER, &self.members.fingerprint)
            .header(TO_HEADER, id.to_string())
            .header(PROTOCOL_HEADER, RAFT_PROTOCOL.to_string())
            .timeout(left.max(Duration::from_millis(100)))
            .send()
            .await
            .map_err(|e| what(&e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(what(&format!("{} {}", status, body.trim())));
        }
        resp.json::<Result<Option<u64>, String>>()
            .await
            .map_err(|e| what(&e))?
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
            initialized: m.last_log_index.is_some() || m.vote != openraft::Vote::default(),
            last_log_index: m.last_log_index,
            voting: self.voting.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}

/// Run in the background by a node that started without Raft state (or
/// restarted while catching up), until it votes (see [`Cluster::start`]).
#[cfg(not(target_arch = "wasm32"))]
async fn bootstrap(weak: std::sync::Weak<Cluster>) {
    let mut said = String::new();
    loop {
        let Some(c) = weak.upgrade() else { return };
        match c.bootstrap_step().await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) if e != said => {
                eprintln!("cluster: {}", e);
                said = e;
            }
            Err(_) => {}
        }
        drop(c);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Cluster {
    /// One try at [`bootstrap`]: true once this node votes.
    async fn bootstrap_step(&self) -> Result<bool, String> {
        let (view, max_term) = self.peer_view().await;
        self.vote_floor
            .fetch_max(max_term, std::sync::atomic::Ordering::SeqCst);
        // Whether a group is new is judged by the same test as the peers
        // (nothing past the initial membership), not by having Raft state:
        // a node that initialized and stopped before it voted has state, but
        // is still one of a new group's members, and may be needed to elect
        // its first leader.
        let committed = self
            .raft
            .metrics()
            .borrow()
            .last_log_index
            .is_some_and(|i| i > 0);
        if !committed {
            match view {
                PeerView::Fresh => {
                    let initial: BTreeMap<u64, BasicNode> = self
                        .members
                        .ids()
                        .map(|id| (id, BasicNode::default()))
                        .collect();
                    match self.raft.initialize(initial).await {
                        Ok(())
                        | Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(
                            _,
                        ))) => {}
                        Err(e) => return Err(format!("initialize: {}", e)),
                    }
                    eprintln!("cluster: every member is new: formed the group");
                    self.start_voting()?;
                    return Ok(true);
                }
                PeerView::Formed => {}
                PeerView::Unknown(why) => {
                    return Err(format!(
                        "no Raft state here: waiting to hear from every member before \
                         forming the group ({})",
                        why
                    ));
                }
            }
        }
        // The group exists: catch up to the leader's read index.
        self.linearizable_state(Duration::from_secs(30))
            .await
            .map_err(|e| {
                format!(
                    "no Raft state here and the group exists: catching up from the \
                     leader before voting ({})",
                    e
                )
            })?;
        eprintln!("cluster: caught up with the group: voting again");
        self.start_voting()?;
        Ok(true)
    }

    fn start_voting(&self) -> Result<(), String> {
        remove_durably(&self.data_dir.join(CATCHING_UP))?;
        self.voting.store(true, std::sync::atomic::Ordering::SeqCst);
        self.raft.runtime_config().elect(!self.misconfigured());
        Ok(())
    }

    /// Why a vote request for `term` is refused before Raft sees it, if it
    /// is (see [`Cluster::start`]).
    pub fn vote_refusal(&self, term: u64) -> Option<Refusal> {
        let floor = self.vote_floor.load(std::sync::atomic::Ordering::SeqCst);
        if !self.voting.load(std::sync::atomic::Ordering::SeqCst) {
            Some(Refusal::CatchingUp)
        } else if term <= floor {
            Some(Refusal::Below(floor))
        } else {
            None
        }
    }

    /// What the peers say (see [`PeerView`]), and the newest term any of
    /// them reported.
    async fn peer_view(&self) -> (PeerView, u64) {
        let asks = self.members.peers.iter().map(|(&id, addr)| async move {
            let status: Status = self
                .http
                .get(format!("{}/api/v1/cluster/status", addr))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .map_err(|e| format!("node {} at {}: {}", id, addr, e))?
                .json()
                .await
                .map_err(|e| format!("node {} at {}: {}", id, addr, e))?;
            // One whose pool config disagrees (reported by the peer check
            // in `start`) can't take part, but what it has is still what
            // that machine has: it counts for whether the group is new.
            // Not its term, though: none of our members' votes went to it,
            // and shut out, it campaigns on and on.
            let ours = status.id == id && status.fingerprint == self.members.fingerprint;
            Ok::<_, String>((status, ours))
        });
        let answers: Vec<Result<(Status, bool), String>> = futures::future::join_all(asks).await;
        let max_term = answers
            .iter()
            .flatten()
            .filter(|(_, ours)| *ours)
            .map(|(s, _)| s.term)
            .max()
            .unwrap_or(0);
        let answers: Vec<Result<Status, String>> =
            answers.into_iter().map(|a| a.map(|(s, _)| s)).collect();
        let view = if answers
            .iter()
            .flatten()
            .any(|s| s.last_log_index.is_some_and(|i| i > 0))
        {
            PeerView::Formed
        } else if let Some(Err(why)) = answers.into_iter().find(|a| a.is_err()) {
            PeerView::Unknown(why)
        } else {
            PeerView::Fresh
        };
        (view, max_term)
    }
}

/// Create `path` (empty) and make that durable.
#[cfg(not(target_arch = "wasm32"))]
fn write_durably(path: &std::path::Path) -> Result<(), String> {
    let what = |e: std::io::Error| format!("{}: {}", path.display(), e);
    std::fs::File::create(path)
        .and_then(|f| f.sync_all())
        .map_err(what)?;
    sync_parent(path).map_err(what)
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_durably(path: &std::path::Path) -> Result<(), String> {
    let what = |e: std::io::Error| format!("{}: {}", path.display(), e);
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(what(e)),
        _ => {}
    }
    sync_parent(path).map_err(what)
}

#[cfg(not(target_arch = "wasm32"))]
fn sync_parent(path: &std::path::Path) -> std::io::Result<()> {
    match path.parent() {
        Some(dir) => std::fs::File::open(dir)?.sync_all(),
        None => Ok(()),
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
