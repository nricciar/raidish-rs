//! The committer's HTTP API, served by every `listen` node. Each route is
//! `PoolAdmin` (or the storage calls) on this node's committer; a node
//! that isn't the committer answers `CommitError::Retry`, and
//! `CommitClient` tries the next.

use super::*;
use crate::api_types::NodeFacts;
use crate::api_types::VmDef;
use crate::auth::{Actor, CanRead, CanWrite, IsNode};
use crate::events::{EventPage, EventQuery};
use crate::types::{DatasetName, VmName};
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::routing::{get, post, put};
use raidish_auth::http::{Json, Query};
use serde::Deserialize;
use std::collections::BTreeMap;

/// This node's committer for one request, acting for its caller: Err
/// (Retry) while this node isn't the committer.
pub struct Here(Result<LocalAdmin<crate::vdev::Pool>, CommitError>);

impl FromRequestParts<CommitterSlot> for Here {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        slot: &CommitterSlot,
    ) -> Result<Self, std::convert::Infallible> {
        let Ok(Actor(by)) = Actor::from_request_parts(parts, slot).await;
        let c = slot.read().await.clone();
        Ok(Here(c.map(|c| LocalAdmin::new(c, by)).ok_or_else(|| {
            CommitError::Retry("this node isn't the committer".into())
        })))
    }
}

/// `?force=` on a receive.
#[derive(Deserialize, Default)]
pub struct Force {
    #[serde(default)]
    force: bool,
}

/// `?change=` on a delete.
#[derive(Deserialize, Default)]
pub struct Change {
    change: Option<u64>,
}

/// The committer, or the route answers Retry.
macro_rules! here {
    ($h:expr) => {
        match $h.0 {
            Ok(a) => a,
            Err(e) => return Json(Err(e)),
        }
    };
}

/// A dataset name from the URL, or the route answers why it isn't one.
macro_rules! dataset {
    ($n:expr) => {
        named!(DatasetName, AsRef::<str>::as_ref(&$n))
    };
}

/// A VM name from the path, or the route answers with why it isn't one.
macro_rules! vm {
    ($n:expr) => {
        named!(VmName, AsRef::<str>::as_ref(&$n))
    };
}

macro_rules! named {
    ($t:ident, $n:expr) => {
        match <$t as TryFrom<&str>>::try_from($n) {
            Ok(n) => n,
            Err(e) => return Json(Err(crate::fs::FileSystemError::from(e).into())),
        }
    };
}

type Answer<T> = Json<Result<T, CommitError>>;

// ---- storage: writers' metaslabs and commits ----

pub async fn committer(_auth: CanRead, here: Here) -> Answer<CommitterInfo> {
    Json(Ok(here!(here).committer.info()))
}

pub async fn lend(_auth: CanWrite, here: Here, req: Json<LendRequest>) -> Answer<LentMetaslab> {
    Json(
        here!(here)
            .committer
            .lend(&req.writer, req.want_blocks)
            .await,
    )
}

pub async fn give_back(_auth: CanWrite, here: Here, req: Json<ReturnRequest>) -> Answer<()> {
    Json(here!(here).committer.give_back(&req.writer, req.idx).await)
}

pub async fn commit(_auth: CanWrite, here: Here, req: Json<DatasetCommit>) -> Answer<Committed> {
    Json(here!(here).committer.commit(&req).await)
}

pub async fn commit_fs(_auth: CanWrite, here: Here, req: Json<FsCommit>) -> Answer<FsCommitted> {
    Json(here!(here).committer.commit_fs(&req).await)
}

pub async fn claim(_auth: CanWrite, here: Here, req: Json<ClaimRequest>) -> Answer<()> {
    Json(
        here!(here)
            .committer
            .claim_dataset(&req.dataset, &req.ds)
            .await,
    )
}

// ---- PoolAdmin ----

pub async fn volumes(_auth: CanRead, here: Here) -> Answer<Vec<VolumeInfo>> {
    Json(here!(here).volumes().await)
}

pub async fn create(_auth: CanWrite, here: Here, req: Json<CreateVolume>) -> Answer<()> {
    Json(here!(here).create_volume(&req.name, req.size_bytes).await)
}

pub async fn remove(_auth: CanWrite, here: Here, Path(name): Path<String>) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).delete_volume(name).await)
}

pub async fn props(
    _auth: CanWrite,
    here: Here,
    Path(name): Path<String>,
    props: Json<DatasetProps>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).set_props(name, &props).await)
}

pub async fn datasets(_auth: CanRead, here: Here) -> Answer<Vec<DatasetInfo>> {
    Json(here!(here).datasets().await)
}

pub async fn create_dataset(_auth: CanWrite, here: Here, req: Json<CreateDataset>) -> Answer<()> {
    Json(here!(here).create_dataset(&req).await)
}

pub async fn delete_dataset(_auth: CanWrite, here: Here, Path(name): Path<String>) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).delete_dataset(name).await)
}

pub async fn snapshots(
    _auth: CanRead,
    here: Here,
    Path(name): Path<String>,
) -> Answer<Vec<SnapshotInfo>> {
    let name = &dataset!(name);
    Json(here!(here).snapshots(name).await)
}

pub async fn snapshot(
    _auth: CanWrite,
    here: Here,
    Path(name): Path<String>,
    req: Json<NewName>,
) -> Answer<Txg> {
    let name = &dataset!(name);
    Json(here!(here).create_snapshot(name, &req.name).await)
}

pub async fn destroy_snapshot(
    _auth: CanWrite,
    here: Here,
    Path((name, snap)): Path<(String, String)>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).destroy_snapshot(name, &snap).await)
}

pub async fn clone_snapshot(
    _auth: CanWrite,
    here: Here,
    Path((name, snap)): Path<(String, String)>,
    req: Json<NewName>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(
        here!(here)
            .clone_snapshot(name, &snap, &dataset!(req.name))
            .await,
    )
}

pub async fn rollback(_auth: CanWrite, here: Here, Path(name): Path<String>) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).rollback(name).await)
}

pub async fn recv(
    _auth: CanWrite,
    here: Here,
    Path(name): Path<String>,
    Query(Force { force }): Query<Force>,
    data: axum::body::Body,
) -> Answer<StreamReport> {
    let name = &dataset!(name);
    let a = here!(here);
    let mut stream = raidish_auth::http::body_reader(data);
    Json(a.recv(name, &mut stream, force).await)
}

pub async fn recv_check(
    _auth: CanWrite,
    here: Here,
    Path(name): Path<String>,
    Query(Force { force }): Query<Force>,
    header: Json<crate::send::SendHeader>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).recv_check(name, &header, force).await)
}

pub async fn events(_auth: CanRead, here: Here, Query(q): Query<EventQuery>) -> Answer<EventPage> {
    Json(here!(here).events(&q).await)
}

/// Nodes report what happened on them.
pub async fn report_events(
    _auth: IsNode,
    here: Here,
    events: Json<Vec<NewEvent>>,
) -> Answer<Vec<u64>> {
    Json(here!(here).report_events(&events).await)
}

pub async fn vms(_auth: CanRead, here: Here) -> Answer<BTreeMap<VmName, VmDef>> {
    Json(here!(here).vms().await)
}

pub async fn put_vm(_auth: CanWrite, here: Here, req: Json<PutVm>) -> Answer<VmDef> {
    Json(here!(here).put_vm(&req).await)
}

pub async fn delete_vm(
    _auth: CanWrite,
    here: Here,
    Path(name): Path<String>,
    Query(Change { change }): Query<Change>,
) -> Answer<()> {
    let name = &vm!(name);
    Json(here!(here).delete_vm(name, change).await)
}

pub async fn node_facts(_auth: CanRead, here: Here) -> Answer<BTreeMap<String, NodeFacts>> {
    Json(here!(here).node_facts().await)
}

/// A node's own facts (nodes only).
pub async fn put_node_facts(_node: IsNode, here: Here, facts: Json<NodeFacts>) -> Answer<()> {
    Json(here!(here).put_node_facts(&facts).await)
}

/// The routes, on this node's committer slot.
pub fn router(slot: CommitterSlot) -> axum::Router {
    let p = "/api/v1/pool";
    axum::Router::new()
        .route(&format!("{p}/committer"), get(committer))
        .route(&format!("{p}/lend"), post(lend))
        .route(&format!("{p}/return"), post(give_back))
        .route(&format!("{p}/commit"), post(commit))
        .route(&format!("{p}/commit_fs"), post(commit_fs))
        .route(&format!("{p}/claim"), post(claim))
        .route(&format!("{p}/volumes"), get(volumes).post(create))
        .route(
            &format!("{p}/volumes/{{name}}"),
            axum::routing::delete(remove),
        )
        .route(&format!("{p}/volumes/{{name}}/props"), put(props))
        .route(&format!("{p}/datasets"), get(datasets).post(create_dataset))
        .route(
            &format!("{p}/datasets/{{name}}"),
            axum::routing::delete(delete_dataset),
        )
        .route(
            &format!("{p}/volumes/{{name}}/snapshots"),
            get(snapshots).post(snapshot),
        )
        .route(
            &format!("{p}/volumes/{{name}}/snapshots/{{snap}}"),
            axum::routing::delete(destroy_snapshot),
        )
        .route(
            &format!("{p}/volumes/{{name}}/snapshots/{{snap}}/clone"),
            post(clone_snapshot),
        )
        .route(&format!("{p}/volumes/{{name}}/rollback"), post(rollback))
        .route(&format!("{p}/volumes/{{name}}/recv"), post(recv))
        .route(
            &format!("{p}/volumes/{{name}}/recv/check"),
            post(recv_check),
        )
        .route(&format!("{p}/events"), get(events).post(report_events))
        .route(&format!("{p}/vms"), get(vms).put(put_vm))
        .route(
            &format!("{p}/vms/{{name}}"),
            axum::routing::delete(delete_vm),
        )
        .route(
            &format!("{p}/nodes/facts"),
            get(node_facts).put(put_node_facts),
        )
        .with_state(slot)
}
