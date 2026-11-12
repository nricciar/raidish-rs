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

type NodeId = u64;
type Shared = Arc<Cluster>;

/// Refuses Raft RPCs from a node whose pool layout differs from ours: its
/// node ids would mean different nodes.
pub struct SameLayout;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SameLayout {
    type Error = String;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, String> {
        let Some(cluster) = req.rocket().state::<Shared>() else {
            return Outcome::Error((Status::ServiceUnavailable, "no cluster".into()));
        };
        let theirs = req
            .headers()
            .get_one(super::FINGERPRINT_HEADER)
            .unwrap_or("");
        let to: Option<u64> = req
            .headers()
            .get_one(super::TO_HEADER)
            .and_then(|v| v.parse().ok());
        if theirs == cluster.members.fingerprint && to == Some(cluster.members.me) {
            return Outcome::Success(SameLayout);
        }
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            eprintln!(
                "cluster: refusing Raft traffic from a node whose pool config disagrees \
                 (layout {:?} vs ours {}, sent to node {:?} but we are node {}): every node \
                 must list the same disks in the same order",
                theirs, cluster.members.fingerprint, to, cluster.members.me
            )
        });
        Outcome::Error((Status::Conflict, "different pool layout".into()))
    }
}

#[post("/api/v1/raft/append", data = "<rpc>")]
pub async fn append(
    rpc: Json<AppendEntriesRequest<TypeConfig>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>> {
    Json(cluster.raft.append_entries(rpc.into_inner()).await)
}

#[post("/api/v1/raft/vote", data = "<rpc>")]
pub async fn vote(
    rpc: Json<VoteRequest<NodeId>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<VoteResponse<NodeId>, RaftError<NodeId>>> {
    Json(cluster.raft.vote(rpc.into_inner()).await)
}

#[post("/api/v1/raft/snapshot", data = "<rpc>")]
pub async fn snapshot(
    rpc: Json<InstallSnapshotRequest<TypeConfig>>,
    _layout: SameLayout,
    cluster: &State<Shared>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>> {
    Json(cluster.raft.install_snapshot(rpc.into_inner()).await)
}

#[post("/api/v1/cluster/lease?<forwarded>", data = "<op>")]
pub async fn lease(
    op: Json<LeaseOp>,
    forwarded: Option<bool>,
    cluster: &State<Shared>,
) -> Json<Result<Response, String>> {
    Json(cluster.lease(&op, forwarded.unwrap_or(false)).await)
}

#[get("/api/v1/cluster/status")]
pub async fn status(cluster: &State<Shared>) -> Json<super::Status> {
    Json(cluster.status())
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![append, vote, snapshot, lease, status]
}
