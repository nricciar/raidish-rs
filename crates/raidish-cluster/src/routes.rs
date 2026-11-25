//! HTTP routes for Raft RPCs and lease requests, mounted by `api::listen`.

use super::state::Response;
use super::{Cluster, LeaseOp, TypeConfig};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use rocket::{State, get, post};
use std::sync::Arc;

use crate::types::NodeId;
type Shared = Arc<Cluster>;

/// Refuses Raft RPCs from a node that speaks another Raft protocol version
/// (426), or whose pool layout differs from ours (409): its node ids would
/// mean different nodes.
pub struct SameLayout;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SameLayout {
    type Error = String;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, String> {
        let Some(cluster) = req.rocket().state::<Shared>() else {
            return Outcome::Error((Status::ServiceUnavailable, "no cluster".into()));
        };
        let version = req.headers().get_one(super::PROTOCOL_HEADER);
        if version.and_then(|v| v.parse().ok()) != Some(super::RAFT_PROTOCOL) {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                eprintln!(
                    "cluster: refusing Raft traffic in protocol {:?}: this node speaks {} \
                     (every node must run the same raidish)",
                    version,
                    super::RAFT_PROTOCOL
                )
            });
            return Outcome::Error((Status::UpgradeRequired, "other raft protocol".into()));
        }
        let theirs = req
            .headers()
            .get_one(super::FINGERPRINT_HEADER)
            .unwrap_or("");
        let to = req
            .headers()
            .get_one(super::TO_HEADER)
            .and_then(super::parse_to_header);
        if cluster.misconfigured() {
            // Sitting out: see `Cluster::misconfigured`.
            return Outcome::Error((
                Status::Conflict,
                "this node's pool config is the odd one out".into(),
            ));
        }
        if theirs == cluster.members.fingerprint && to == Some(cluster.members.me) {
            return Outcome::Success(SameLayout);
        }
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            eprintln!(
                "cluster: refusing Raft traffic from a node whose pool config disagrees \
                 (layout {:?} vs ours {}, sent to {} but we are {}): every node \
                 must list the same disks in the same order",
                theirs,
                cluster.members.fingerprint,
                to.map_or_else(|| "nobody".to_string(), |t| t.to_string()),
                cluster.members.me
            )
        });
        Outcome::Error((Status::Conflict, "different pool layout".into()))
    }
}

#[post("/api/v1/raft/append", data = "<rpc>")]
pub async fn append(
    _auth: crate::auth::IsNode,
    rpc: Json<AppendEntriesRequest<TypeConfig>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>> {
    Json(cluster.raft.append_entries(rpc.into_inner()).await)
}

#[post("/api/v1/raft/vote", data = "<rpc>")]
pub async fn vote(
    _auth: crate::auth::IsNode,
    rpc: Json<VoteRequest<NodeId>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Result<Json<Result<VoteResponse<NodeId>, RaftError<NodeId>>>, (Status, &'static str)> {
    // Not even seen by Raft: a node catching up after losing its state
    // must not vote (audit R4b), and refusing outright leaves no trace.
    match cluster.vote_refusal(rpc.vote.leader_id().term) {
        None => Ok(Json(cluster.raft.vote(rpc.into_inner()).await)),
        Some(super::Refusal::CatchingUp) => {
            Err((Status::ServiceUnavailable, "not voting: catching up"))
        }
        // A term this node may have voted in before it lost its state:
        // refused as Raft refuses a stale term, with a newer vote, so the
        // candidate moves past it at once instead of a term per election.
        Some(super::Refusal::Below(floor)) => Ok(Json(Ok(VoteResponse {
            vote: openraft::Vote::new(floor + 1, cluster.members.me),
            vote_granted: false,
            last_log_id: None,
        }))),
    }
}

#[post("/api/v1/raft/snapshot", data = "<rpc>")]
pub async fn snapshot(
    _auth: crate::auth::IsNode,
    rpc: Json<InstallSnapshotRequest<TypeConfig>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>> {
    Json(cluster.raft.install_snapshot(rpc.into_inner()).await)
}

#[post("/api/v1/cluster/lease?<forwarded>", data = "<op>")]
pub async fn lease(
    _auth: crate::auth::CanWrite,
    op: Json<LeaseOp>,
    forwarded: Option<bool>,
    cluster: &State<Shared>,
) -> Json<Result<Response, String>> {
    Json(cluster.lease(&op, forwarded.unwrap_or(false)).await)
}

#[get("/api/v1/cluster/status")]
pub async fn status(_auth: crate::auth::CanRead, cluster: &State<Shared>) -> Json<super::Status> {
    Json(cluster.status())
}

/// A follower's read index (see `Cluster::linearizable_state`): the leader's
/// commit index once it has confirmed it leads. Never forwarded further.
#[get("/api/v1/cluster/read_index")]
pub async fn read_index(
    _auth: crate::auth::IsNode,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<Option<u64>, String>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    Json(cluster.read_index(true, deadline).await)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![append, vote, snapshot, lease, status, read_index]
}

/// Stops this node's Raft member as soon as the server is asked to shut
/// down. Rocket keeps serving open connections for its grace period
/// (seconds), and a leader whose heartbeats still get through keeps the
/// followers from electing anyone while the lease requests they forward to
/// it fail: long enough for their leases to run out. Stopped, it goes
/// quiet at once and the others elect a new leader within an election
/// timeout.
pub fn stop_raft_on_shutdown() -> rocket::fairing::AdHoc {
    rocket::fairing::AdHoc::on_shutdown("stop raft", |rocket| {
        Box::pin(async move {
            if let Some(cluster) = rocket.state::<Shared>() {
                let _ = cluster.raft.shutdown().await;
            }
        })
    })
}
