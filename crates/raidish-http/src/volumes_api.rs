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
use crate::types::DatasetName;
use axum::extract::{Path, State};
use axum::http::StatusCode as Status;
use axum::routing::{get, post, put};
use raidish_auth::http::Json;
use std::sync::Arc;

pub use crate::api_types::VolumeView;

/// The committer, wherever it is.
pub struct VolumesState {
    pub admin: Admin,
}

async fn list(
    _auth: crate::auth::CanRead,
    State(st): State<Arc<VolumesState>>,
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

async fn create(
    auth: crate::auth::CanWrite,
    State(st): State<Arc<VolumesState>>,
    req: Json<CreateVolume>,
) -> Result<Json<()>, ApiError> {
    st.admin
        .on_behalf(&auth.0)
        .create_volume(&req.name, req.size_bytes)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn remove(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    State(st): State<Arc<VolumesState>>,
) -> Result<Json<()>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .delete_volume(name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn props(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    State(st): State<Arc<VolumesState>>,
    props: Json<DatasetProps>,
) -> Result<Json<()>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .set_props(name, &props)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn snapshot(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    State(st): State<Arc<VolumesState>>,
    req: Json<NewName>,
) -> Result<Json<crate::types::Txg>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .create_snapshot(name, &req.name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn destroy_snapshot(
    auth: crate::auth::CanWrite,
    Path((name, snap)): Path<(String, String)>,
    State(st): State<Arc<VolumesState>>,
) -> Result<Json<()>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .destroy_snapshot(name, &snap)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn clone_snapshot(
    auth: crate::auth::CanWrite,
    Path((name, snap)): Path<(String, String)>,
    State(st): State<Arc<VolumesState>>,
    req: Json<NewName>,
) -> Result<Json<()>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .clone_snapshot(name, &snap, &dataset(&req.name)?)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn rollback(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    State(st): State<Arc<VolumesState>>,
) -> Result<Json<()>, ApiError> {
    let name = &dataset(&name)?;
    st.admin
        .on_behalf(&auth.0)
        .rollback(name)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

pub fn router(st: Arc<VolumesState>) -> axum::Router {
    let v = "/api/v1/volumes";
    axum::Router::new()
        .route(v, get(list).post(create))
        .route(&format!("{v}/{{name}}"), axum::routing::delete(remove))
        .route(&format!("{v}/{{name}}/props"), put(props))
        .route(&format!("{v}/{{name}}/snapshots"), post(snapshot))
        .route(
            &format!("{v}/{{name}}/snapshots/{{snap}}"),
            axum::routing::delete(destroy_snapshot),
        )
        .route(
            &format!("{v}/{{name}}/snapshots/{{snap}}/clone"),
            post(clone_snapshot),
        )
        .route(&format!("{v}/{{name}}/rollback"), post(rollback))
        .with_state(st)
}

/// A dataset name from the request, or 400 saying why it isn't one.
fn dataset(name: &str) -> Result<DatasetName, ApiError> {
    DatasetName::try_from(name).map_err(|e| ApiError::new(Status::BAD_REQUEST, e.to_string()))
}
