//! Volume and snapshot management for the dashboard (and any HTTP client
//! that doesn't want to find the committer itself). Served by every
//! `listen` node: each request is passed on to whichever node is the
//! committer (`/api/v1/pool/volumes...`, see `crate::committer::routes`),
//! and its answer comes back as a plain HTTP status and JSON body.
//!
//! * `GET /api/v1/volumes`: every volume, with its snapshots.
//! * `POST /api/v1/volumes` (`{"name", "size_bytes"}`): make a volume.
//! * `DELETE /api/v1/volumes/<name>`: delete a volume.
//! * `PUT /api/v1/volumes/<name>/props` (`VolumeProps`): placement settings.
//! * `POST /api/v1/volumes/<name>/snapshots` (`{"name"}`): take a snapshot.
//! * `DELETE /api/v1/volumes/<name>/snapshots/<snap>`: destroy one.
//! * `POST /api/v1/volumes/<name>/snapshots/<snap>/clone` (`{"name"}`): a new
//!   volume from a snapshot.
//! * `POST /api/v1/volumes/<name>/rollback`: back to its newest snapshot.

use crate::committer::{CommitClient, CreateVolume, NewName};
use crate::inode::VolumeProps;
use crate::snapshot::SnapshotInfo;
use crate::volume::VolumeInfo;
use crate::writer::CommitError;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, delete, get, post, put};
use serde::{Deserialize, Serialize};

/// Talks to the committer, wherever it is.
pub struct VolumesState {
    pub client: CommitClient,
}

type ApiError = (Status, String);

fn api_err(e: CommitError) -> ApiError {
    match e {
        CommitError::Retry(m) => (
            Status::ServiceUnavailable,
            format!("no committer reachable: {}", m),
        ),
        CommitError::Refused(m) => {
            let status = if m.contains("not found") || m.contains("No such") {
                Status::NotFound
            } else if m.contains("exists") {
                Status::Conflict
            } else {
                Status::BadRequest
            };
            (status, m)
        }
        e @ CommitError::Restamp(_) => (Status::ServiceUnavailable, e.to_string()),
    }
}

/// A volume and its snapshots (oldest first).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct VolumeView {
    #[serde(flatten)]
    pub info: VolumeInfo,
    pub snapshots: Vec<SnapshotInfo>,
}

#[get("/api/v1/volumes")]
async fn list(
    _auth: crate::auth::CanRead,
    st: &State<VolumesState>,
) -> Result<Json<Vec<VolumeView>>, ApiError> {
    let volumes = st.client.volumes().await.map_err(api_err)?;
    let mut out = Vec::with_capacity(volumes.len());
    for info in volumes {
        // One gone meanwhile (deleted between the calls) just has none.
        let snapshots = st.client.snapshots(&info.name).await.unwrap_or_default();
        out.push(VolumeView { info, snapshots });
    }
    Ok(Json(out))
}

#[post("/api/v1/volumes", data = "<req>")]
async fn create(
    _auth: crate::auth::CanWrite,
    req: Json<CreateVolume>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client
        .create_volume(&req.name, req.size_bytes)
        .await
        .map(Json)
        .map_err(api_err)
}

#[delete("/api/v1/volumes/<name>")]
async fn remove(
    _auth: crate::auth::CanWrite,
    name: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client
        .delete_volume(name)
        .await
        .map(Json)
        .map_err(api_err)
}

#[put("/api/v1/volumes/<name>/props", data = "<props>")]
async fn props(
    _auth: crate::auth::CanWrite,
    name: &str,
    props: Json<VolumeProps>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client
        .set_props(name, &props)
        .await
        .map(Json)
        .map_err(api_err)
}

#[post("/api/v1/volumes/<name>/snapshots", data = "<req>")]
async fn snapshot(
    _auth: crate::auth::CanWrite,
    name: &str,
    req: Json<NewName>,
    st: &State<VolumesState>,
) -> Result<Json<u64>, ApiError> {
    st.client
        .create_snapshot(name, &req.name)
        .await
        .map(Json)
        .map_err(api_err)
}

#[delete("/api/v1/volumes/<name>/snapshots/<snap>")]
async fn destroy_snapshot(
    _auth: crate::auth::CanWrite,
    name: &str,
    snap: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client
        .destroy_snapshot(name, snap)
        .await
        .map(Json)
        .map_err(api_err)
}

#[post("/api/v1/volumes/<name>/snapshots/<snap>/clone", data = "<req>")]
async fn clone_snapshot(
    _auth: crate::auth::CanWrite,
    name: &str,
    snap: &str,
    req: Json<NewName>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client
        .clone_snapshot(name, snap, &req.name)
        .await
        .map(Json)
        .map_err(api_err)
}

#[post("/api/v1/volumes/<name>/rollback")]
async fn rollback(
    _auth: crate::auth::CanWrite,
    name: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.client.rollback(name).await.map(Json).map_err(api_err)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        list,
        create,
        remove,
        props,
        snapshot,
        destroy_snapshot,
        clone_snapshot,
        rollback
    ]
}
