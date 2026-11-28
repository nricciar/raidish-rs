//! HTTP routes for Raft RPCs and lease requests, mounted by `api::listen`.

use super::state::Response;
use super::{Cluster, LeaseOp, TypeConfig};
use axum::extract::{FromRequestParts, State};
use axum::http::StatusCode as Status;
use axum::http::request::Parts;
use axum::routing::{get, post};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use raidish_auth::http::{Json, Query, Refusal};
use serde::Deserialize;
use std::sync::Arc;

use crate::types::NodeId;
type Shared = Arc<Cluster>;

/// Refuses Raft RPCs from a node that speaks another Raft protocol version
/// (426), or whose pool layout differs from ours (409): its node ids would
/// mean different nodes.
pub struct SameLayout;

fn header<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts.headers.get(name).and_then(|v| v.to_str().ok())
}

impl FromRequestParts<Shared> for SameLayout {
    type Rejection = Refusal;

    async fn from_request_parts(parts: &mut Parts, cluster: &Shared) -> Result<Self, Refusal> {
        let version = header(parts, super::PROTOCOL_HEADER);
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
            return Err((Status::UPGRADE_REQUIRED, "other raft protocol".into()));
        }
        let theirs = header(parts, super::FINGERPRINT_HEADER).unwrap_or("");
        let to = header(parts, super::TO_HEADER).and_then(super::parse_to_header);
        if cluster.misconfigured() {
            // Sitting out: see `Cluster::misconfigured`.
            return Err((
                Status::CONFLICT,
                "this node's pool config is the odd one out".into(),
            ));
        }
        if theirs == cluster.members.fingerprint && to == Some(cluster.members.me) {
            return Ok(SameLayout);
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
        Err((Status::CONFLICT, "different pool layout".into()))
    }
}

pub async fn append(
    _auth: crate::auth::IsNode,
    _layout: SameLayout,
    State(cluster): State<Shared>,
    rpc: Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>> {
    Json(cluster.raft.append_entries(rpc.into_inner()).await)
}

pub async fn vote(
    _auth: crate::auth::IsNode,
    _layout: SameLayout,
    State(cluster): State<Shared>,
    rpc: Json<VoteRequest<NodeId>>,
) -> Result<Json<Result<VoteResponse<NodeId>, RaftError<NodeId>>>, (Status, &'static str)> {
    // Not even seen by Raft: a node catching up after losing its state
    // must not vote (audit R4b), and refusing outright leaves no trace.
    match cluster.vote_refusal(rpc.vote.leader_id().term) {
        None => Ok(Json(cluster.raft.vote(rpc.into_inner()).await)),
        Some(super::Refusal::CatchingUp) => {
            Err((Status::SERVICE_UNAVAILABLE, "not voting: catching up"))
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

pub async fn snapshot(
    _auth: crate::auth::IsNode,
    _layout: SameLayout,
    State(cluster): State<Shared>,
    rpc: Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>> {
    Json(cluster.raft.install_snapshot(rpc.into_inner()).await)
}

#[derive(Deserialize, Default)]
pub struct Forwarded {
    #[serde(default)]
    pub forwarded: bool,
}

pub async fn lease(
    auth: crate::auth::CanWrite,
    Query(Forwarded { forwarded }): Query<Forwarded>,
    State(cluster): State<Shared>,
    op: Json<LeaseOp>,
) -> Json<Result<Response, String>> {
    // CA changes are proposed by a node's CA service in-process, and reach
    // here only as a node forwarding one to the leader. Anyone else must
    // go through the CA's own endpoints (which check what they propose).
    // A node that does is trusted no further for it: what grants
    // authority is checked when applied or when used (see `crate::pki`).
    if matches!(*op, LeaseOp::Pki(_))
        && !(forwarded && auth.0.role == raidish_auth::pki::Role::Node)
    {
        return Json(Err(
            "CA changes go through /api/v1/pki, not the lease API".into()
        ));
    }
    Json(cluster.lease(&op, forwarded).await)
}

pub async fn status(
    _auth: crate::auth::CanRead,
    State(cluster): State<Shared>,
) -> Json<super::Status> {
    Json(cluster.status())
}

/// A follower's read index (see `Cluster::linearizable_state`): the leader's
/// commit index once it has confirmed it leads. Never forwarded further.
pub async fn read_index(
    _auth: crate::auth::IsNode,
    _layout: SameLayout,
    State(cluster): State<Shared>,
) -> Json<Result<Option<u64>, String>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    Json(cluster.read_index(true, deadline).await)
}

/// The routes, on this node's member.
pub fn router(cluster: Shared) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/raft/append", post(append))
        .route("/api/v1/raft/vote", post(vote))
        .route("/api/v1/raft/snapshot", post(snapshot))
        .route("/api/v1/cluster/lease", post(lease))
        .route("/api/v1/cluster/status", get(status))
        .route("/api/v1/cluster/read_index", get(read_index))
        .with_state(cluster)
}
