//! Volume and snapshot management for the dashboard (and any HTTP client
//! that doesn't want to find the committer itself). Served by every
//! `listen` node: each request goes to the committer (in process on the
//! committer's node, else `/api/v1/pool/volumes...` on whichever node is
//! it, see `crate::committer::Admin`), and its answer comes back as a
//! plain HTTP status and JSON body.
//!
//! * `GET /api/v1/volumes`: every volume, with its snapshots.
//! * `POST /api/v1/volumes` (`{"name", "size_bytes"}`): make a volume.
//! * `DELETE /api/v1/volumes/<name>`: delete a volume.
//! * `PUT /api/v1/volumes/<name>/props` (`DatasetProps`): placement settings.
//! * `POST /api/v1/volumes/<name>/snapshots` (`{"name"}`): take a snapshot.
//! * `DELETE /api/v1/volumes/<name>/snapshots/<snap>`: destroy one.
//! * `POST /api/v1/volumes/<name>/snapshots/<snap>/clone` (`{"name"}`): a new
//!   volume from a snapshot.
//! * `POST /api/v1/volumes/<name>/rollback`: back to its newest snapshot.

use crate::api_error::ApiError;
use crate::committer::PoolAdmin;
use crate::committer::{Admin, CreateVolume, NewName};
use crate::inode::DatasetProps;
use rocket::serde::json::Json;
use rocket::{State, delete, get, post, put};

pub use crate::api_types::VolumeView;

/// The committer, wherever it is.
pub struct VolumesState {
    pub admin: Admin,
}

#[get("/api/v1/volumes")]
async fn list(
    _auth: crate::auth::CanRead,
    st: &State<VolumesState>,
) -> Result<Json<Vec<VolumeView>>, ApiError> {
    let volumes = st.admin.volumes().await?;
    let mut out = Vec::with_capacity(volumes.len());
    for info in volumes {
        // One gone meanwhile (deleted between the calls) just has none.
        let snapshots = st.admin.snapshots(&info.name).await.unwrap_or_default();
        out.push(VolumeView { info, snapshots });
    }
    Ok(Json(out))
}

#[post("/api/v1/volumes", data = "<req>")]
async fn create(
    auth: crate::auth::CanWrite,
    req: Json<CreateVolume>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .create_volume(&req.name, req.size_bytes)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[delete("/api/v1/volumes/<name>")]
async fn remove(
    auth: crate::auth::CanWrite,
    name: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .delete_volume(name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[put("/api/v1/volumes/<name>/props", data = "<props>")]
async fn props(
    auth: crate::auth::CanWrite,
    name: &str,
    props: Json<DatasetProps>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .set_props(name, &props)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[post("/api/v1/volumes/<name>/snapshots", data = "<req>")]
async fn snapshot(
    auth: crate::auth::CanWrite,
    name: &str,
    req: Json<NewName>,
    st: &State<VolumesState>,
) -> Result<Json<crate::types::Txg>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .create_snapshot(name, &req.name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[delete("/api/v1/volumes/<name>/snapshots/<snap>")]
async fn destroy_snapshot(
    auth: crate::auth::CanWrite,
    name: &str,
    snap: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .destroy_snapshot(name, snap)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[post("/api/v1/volumes/<name>/snapshots/<snap>/clone", data = "<req>")]
async fn clone_snapshot(
    auth: crate::auth::CanWrite,
    name: &str,
    snap: &str,
    req: Json<NewName>,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .clone_snapshot(name, snap, &req.name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[post("/api/v1/volumes/<name>/rollback")]
async fn rollback(
    auth: crate::auth::CanWrite,
    name: &str,
    st: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .rollback(name)
        .await
        .map(Json)
        .map_err(ApiError::from)
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
