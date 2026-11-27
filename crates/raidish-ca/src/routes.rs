//! `/api/v1/pki/*` (see `docs/ca-service-plan.md` §6). A node that can't
//! sign answers `503`: clients try the next node. Only `/join`, which a
//! token authenticates, is passed on by the node itself.

use crate::CaService;
use crate::api::{
    EnrollmentCreated, EnrollmentRequest, Issued, JoinRequest, RenewRequest, SignRequest,
    UnsealRequest,
};
use crate::service::CaError;
use axum::extract::{ConnectInfo, Extension, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use raidish_auth::auth::{CanRead, IsAdmin, IsAdminCert};
use raidish_auth::http::{Json, Peer, Query};
use raidish_types::pki::{PkiStatus, TokenId};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

type Ca = State<Arc<CaService>>;

impl IntoResponse for CaError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, format!("{}\n", self.msg)).into_response()
    }
}

/// The root, for anyone (a joiner pins it by the fingerprint in its token).
pub async fn ca_pem(State(ca): Ca) -> Result<Response, CaError> {
    let pki = ca
        .current()
        .await
        .ok_or_else(|| CaError::new(404, "no CA on this cluster"))?;
    Ok((
        [(header::CONTENT_TYPE, "application/x-pem-file")],
        pki.anchor.cert_pem.clone(),
    )
        .into_response())
}

pub async fn status(_auth: CanRead, State(ca): Ca) -> Json<PkiStatus> {
    Json(ca.status().await)
}

#[derive(Deserialize, Default)]
pub struct Forwarded {
    #[serde(default)]
    forwarded: bool,
}

pub async fn join(
    addr: Option<Extension<ConnectInfo<SocketAddr>>>,
    Query(Forwarded { forwarded }): Query<Forwarded>,
    State(ca): Ca,
    req: Json<JoinRequest>,
) -> Result<Json<Issued>, CaError> {
    let ip = addr.map(|Extension(ConnectInfo(a))| a.ip());
    if !ca.join_allowed(ip) {
        return Err(CaError::new(
            429,
            "too many joins from this address: wait a minute",
        ));
    }
    ca.join(req.into_inner(), forwarded).await.map(Json)
}

/// The caller's own certificate (by TLS, never a token), renewed.
pub async fn renew(
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<RenewRequest>,
) -> Result<Json<Issued>, CaError> {
    let Some(Extension(peer)) = peer else {
        return Err(CaError::new(
            401,
            "renewing takes the certificate being renewed, presented over TLS",
        ));
    };
    ca.renew(peer.cert(), &req.csr_pem).await.map(Json)
}

pub async fn sign(
    admin: IsAdminCert,
    State(ca): Ca,
    req: Json<SignRequest>,
) -> Result<Json<Issued>, CaError> {
    ca.sign(&admin.0, req.into_inner()).await.map(Json)
}

pub async fn create_enrollment(
    admin: IsAdminCert,
    State(ca): Ca,
    req: Json<EnrollmentRequest>,
) -> Result<Json<EnrollmentCreated>, CaError> {
    ca.create_enrollment(&admin.0, req.into_inner())
        .await
        .map(Json)
}

pub async fn cancel_enrollment(
    _admin: IsAdmin,
    Path(id): Path<String>,
    State(ca): Ca,
) -> Result<(), CaError> {
    let id = TokenId::parse(&id).map_err(|e| CaError::new(400, e))?;
    ca.cancel_enrollment(id).await
}

/// Unseal this node (the one asked).
pub async fn unseal(
    _admin: IsAdminCert,
    State(ca): Ca,
    req: Json<UnsealRequest>,
) -> Result<Json<PkiStatus>, CaError> {
    ca.unseal(&req.recovery_key).await?;
    Ok(Json(ca.status().await))
}

/// The routes, on this node's CA service.
pub fn router(ca: Arc<CaService>) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/pki/ca.pem", get(ca_pem))
        .route("/api/v1/pki/status", get(status))
        .route("/api/v1/pki/join", post(join))
        .route("/api/v1/pki/renew", post(renew))
        .route("/api/v1/pki/sign", post(sign))
        .route("/api/v1/pki/enrollments", post(create_enrollment))
        .route("/api/v1/pki/enrollments/{id}", delete(cancel_enrollment))
        .route("/api/v1/pki/unseal", post(unseal))
        .with_state(ca)
}
