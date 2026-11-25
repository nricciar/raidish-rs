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
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use rocket::{delete, get, post, put};
use std::collections::BTreeMap;

/// This node's committer for one request, acting for its caller: Err
/// (Retry) while this node isn't the committer.
pub struct Here(Result<LocalAdmin<crate::vdev::Pool>, CommitError>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Here {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, ()> {
        let by = match req.guard::<Actor>().await {
            Outcome::Success(a) => a.0,
            _ => None,
        };
        let c = match req.rocket().state::<CommitterSlot>() {
            Some(slot) => slot.read().await.clone(),
            None => None,
        };
        Outcome::Success(Here(c.map(|c| LocalAdmin::new(c, by)).ok_or_else(|| {
            CommitError::Retry("this node isn't the committer".into())
        })))
    }
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
        named!(DatasetName, $n)
    };
}

/// A VM name from the path, or the route answers with why it isn't one.
macro_rules! vm {
    ($n:expr) => {
        named!(VmName, $n)
    };
}

macro_rules! named {
    ($t:ident, $n:expr) => {
        match $t::try_from($n) {
            Ok(n) => n,
            Err(e) => return Json(Err(crate::fs::FileSystemError::from(e).into())),
        }
    };
}

type Answer<T> = Json<Result<T, CommitError>>;

// ---- storage: writers' metaslabs and commits ----

#[get("/api/v1/pool/committer")]
pub async fn committer(_auth: CanRead, here: Here) -> Answer<CommitterInfo> {
    Json(Ok(here!(here).committer.info()))
}

#[post("/api/v1/pool/lend", data = "<req>")]
pub async fn lend(_auth: CanWrite, here: Here, req: Json<LendRequest>) -> Answer<LentMetaslab> {
    Json(
        here!(here)
            .committer
            .lend(&req.writer, req.want_blocks)
            .await,
    )
}

#[post("/api/v1/pool/return", data = "<req>")]
pub async fn give_back(_auth: CanWrite, here: Here, req: Json<ReturnRequest>) -> Answer<()> {
    Json(here!(here).committer.give_back(&req.writer, req.idx).await)
}

#[post("/api/v1/pool/commit", data = "<req>")]
pub async fn commit(_auth: CanWrite, here: Here, req: Json<DatasetCommit>) -> Answer<Committed> {
    Json(here!(here).committer.commit(&req).await)
}

#[post("/api/v1/pool/commit_fs", data = "<req>")]
pub async fn commit_fs(_auth: CanWrite, here: Here, req: Json<FsCommit>) -> Answer<FsCommitted> {
    Json(here!(here).committer.commit_fs(&req).await)
}

#[post("/api/v1/pool/claim", data = "<req>")]
pub async fn claim(_auth: CanWrite, here: Here, req: Json<ClaimRequest>) -> Answer<()> {
    Json(
        here!(here)
            .committer
            .claim_dataset(&req.dataset, &req.ds)
            .await,
    )
}

// ---- PoolAdmin ----

#[get("/api/v1/pool/volumes")]
pub async fn volumes(_auth: CanRead, here: Here) -> Answer<Vec<VolumeInfo>> {
    Json(here!(here).volumes().await)
}

#[post("/api/v1/pool/volumes", data = "<req>")]
pub async fn create(_auth: CanWrite, here: Here, req: Json<CreateVolume>) -> Answer<()> {
    Json(here!(here).create_volume(&req.name, req.size_bytes).await)
}

#[delete("/api/v1/pool/volumes/<name>")]
pub async fn remove(_auth: CanWrite, here: Here, name: &str) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).delete_volume(name).await)
}

#[put("/api/v1/pool/volumes/<name>/props", data = "<props>")]
pub async fn props(
    _auth: CanWrite,
    here: Here,
    name: &str,
    props: Json<DatasetProps>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).set_props(name, &props).await)
}

#[get("/api/v1/pool/datasets")]
pub async fn datasets(_auth: CanRead, here: Here) -> Answer<Vec<DatasetInfo>> {
    Json(here!(here).datasets().await)
}

#[post("/api/v1/pool/datasets", data = "<req>")]
pub async fn create_dataset(_auth: CanWrite, here: Here, req: Json<CreateDataset>) -> Answer<()> {
    Json(here!(here).create_dataset(&req).await)
}

#[delete("/api/v1/pool/datasets/<name>")]
pub async fn delete_dataset(_auth: CanWrite, here: Here, name: &str) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).delete_dataset(name).await)
}

#[get("/api/v1/pool/volumes/<name>/snapshots")]
pub async fn snapshots(_auth: CanRead, here: Here, name: &str) -> Answer<Vec<SnapshotInfo>> {
    let name = &dataset!(name);
    Json(here!(here).snapshots(name).await)
}

#[post("/api/v1/pool/volumes/<name>/snapshots", data = "<req>")]
pub async fn snapshot(_auth: CanWrite, here: Here, name: &str, req: Json<NewName>) -> Answer<Txg> {
    let name = &dataset!(name);
    Json(here!(here).create_snapshot(name, &req.name).await)
}

#[delete("/api/v1/pool/volumes/<name>/snapshots/<snap>")]
pub async fn destroy_snapshot(_auth: CanWrite, here: Here, name: &str, snap: &str) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).destroy_snapshot(name, snap).await)
}

#[post("/api/v1/pool/volumes/<name>/snapshots/<snap>/clone", data = "<req>")]
pub async fn clone_snapshot(
    _auth: CanWrite,
    here: Here,
    name: &str,
    snap: &str,
    req: Json<NewName>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(
        here!(here)
            .clone_snapshot(name, snap, &dataset!(req.name.as_str()))
            .await,
    )
}

#[post("/api/v1/pool/volumes/<name>/rollback")]
pub async fn rollback(_auth: CanWrite, here: Here, name: &str) -> Answer<()> {
    let name = &dataset!(name);
    Json(here!(here).rollback(name).await)
}

#[post("/api/v1/pool/volumes/<name>/recv?<force>", data = "<data>")]
pub async fn recv(
    _auth: CanWrite,
    here: Here,
    name: &str,
    force: Option<bool>,
    data: rocket::Data<'_>,
) -> Answer<StreamReport> {
    let name = &dataset!(name);
    let a = here!(here);
    let mut stream = data.open(rocket::data::ByteUnit::max_value());
    Json(a.recv(name, &mut stream, force.unwrap_or(false)).await)
}

#[post("/api/v1/pool/volumes/<name>/recv/check?<force>", data = "<header>")]
pub async fn recv_check(
    _auth: CanWrite,
    here: Here,
    name: &str,
    force: Option<bool>,
    header: Json<crate::send::SendHeader>,
) -> Answer<()> {
    let name = &dataset!(name);
    Json(
        here!(here)
            .recv_check(name, &header, force.unwrap_or(false))
            .await,
    )
}

#[get("/api/v1/pool/events?<q..>")]
pub async fn events(_auth: CanRead, here: Here, q: EventQuery) -> Answer<EventPage> {
    Json(here!(here).events(&q).await)
}

/// Nodes report what happened on them.
#[post("/api/v1/pool/events", data = "<events>")]
pub async fn report_events(
    _auth: IsNode,
    here: Here,
    events: Json<Vec<NewEvent>>,
) -> Answer<Vec<u64>> {
    Json(here!(here).report_events(&events).await)
}

#[get("/api/v1/pool/vms")]
pub async fn vms(_auth: CanRead, here: Here) -> Answer<BTreeMap<VmName, VmDef>> {
    Json(here!(here).vms().await)
}

#[put("/api/v1/pool/vms", data = "<req>")]
pub async fn put_vm(_auth: CanWrite, here: Here, req: Json<PutVm>) -> Answer<VmDef> {
    Json(here!(here).put_vm(&req).await)
}

#[delete("/api/v1/pool/vms/<name>?<change>")]
pub async fn delete_vm(_auth: CanWrite, here: Here, name: &str, change: Option<u64>) -> Answer<()> {
    let name = &vm!(name);
    Json(here!(here).delete_vm(name, change).await)
}

#[get("/api/v1/pool/nodes/facts")]
pub async fn node_facts(_auth: CanRead, here: Here) -> Answer<BTreeMap<String, NodeFacts>> {
    Json(here!(here).node_facts().await)
}

/// A node's own facts (nodes only).
#[put("/api/v1/pool/nodes/facts", data = "<facts>")]
pub async fn put_node_facts(_node: IsNode, here: Here, facts: Json<NodeFacts>) -> Answer<()> {
    Json(here!(here).put_node_facts(&facts).await)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        committer,
        lend,
        give_back,
        commit,
        commit_fs,
        claim,
        volumes,
        create,
        remove,
        props,
        datasets,
        create_dataset,
        delete_dataset,
        snapshots,
        snapshot,
        destroy_snapshot,
        clone_snapshot,
        rollback,
        recv,
        recv_check,
        events,
        report_events,
        vms,
        put_vm,
        node_facts,
        put_node_facts,
        delete_vm
    ]
}
