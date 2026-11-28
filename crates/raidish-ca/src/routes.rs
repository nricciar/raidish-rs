//! `/api/v1/pki/*` (see `docs/ca-service-plan.md` §6). A node that can't
//! sign answers `503`: clients try the next node. Only `/join`, which a
//! token authenticates, is passed on by the node itself.

use crate::CaService;
use crate::api::{
    ClientEnrollmentCreated, ClientEnrollmentRequest, EnrollRequest, Enrolled, EnrollmentCreated,
    EnrollmentRequest, Issued, IssuerAdded, JoinRequest, NewIssuerRequest, RekeyRequest, Rekeyed,
    RemoveSlotQuery, RenewRequest, RevokeCertRequest, RevokeTokenRequest, RootCrlRequest,
    SealRequest, SignRequest, SlotCreated, SlotRequest, TokenCreated, TokenRequest, UnsealRequest,
};
use crate::service::CaError;
use axum::extract::{ConnectInfo, Extension, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use raidish_auth::auth::{CanRead, IsAdmin, IsAdminCert};
use raidish_auth::http::{Json, Peer, Query};
use raidish_types::pki::{CertListing, IssuerId, IssuerUse, PkiStatus, Serial, TokenId};
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

#[derive(Deserialize, Default)]
pub struct StatusQuery {
    /// Every member's seal too, asked now (CA-3c).
    #[serde(default)]
    all: bool,
}

pub async fn status(
    _auth: CanRead,
    Query(q): Query<StatusQuery>,
    State(ca): Ca,
) -> Json<PkiStatus> {
    Json(match q.all {
        true => ca.status_all().await,
        false => ca.status().await,
    })
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

/// A client's certificate, by its enrollment token (CA-4c): as `/join`.
pub async fn enroll(
    addr: Option<Extension<ConnectInfo<SocketAddr>>>,
    Query(Forwarded { forwarded }): Query<Forwarded>,
    State(ca): Ca,
    req: Json<EnrollRequest>,
) -> Result<Json<Enrolled>, CaError> {
    let ip = addr.map(|Extension(ConnectInfo(a))| a.ip());
    if !ca.join_allowed(ip) {
        return Err(CaError::new(
            429,
            "too many enrollments from this address: wait a minute",
        ));
    }
    ca.enroll(req.into_inner(), forwarded).await.map(Json)
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

/// Renew this node's own certificate now (CA-4a): it changes nobody's
/// authority, so an admin's certificate or token. It answers at once; the
/// renewal is the renewer's (`ca status` and the log say how it went).
pub async fn renew_self(_admin: IsAdmin, State(ca): Ca) -> Result<StatusCode, CaError> {
    ca.renew_now()?;
    Ok(StatusCode::ACCEPTED)
}

/// The certificate a request came with (`IsAdminCert` means there is one).
fn presented(peer: Option<Extension<Peer>>) -> Result<Peer, CaError> {
    peer.map(|Extension(p)| p)
        .ok_or_else(|| CaError::new(401, "it takes a certificate, presented over TLS"))
}

pub async fn sign(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<SignRequest>,
) -> Result<Json<Issued>, CaError> {
    let peer = presented(peer)?;
    ca.sign(&admin.0, peer.cert(), req.into_inner())
        .await
        .map(Json)
}

pub async fn create_enrollment(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<EnrollmentRequest>,
) -> Result<Json<EnrollmentCreated>, CaError> {
    let peer = presented(peer)?;
    ca.create_enrollment(&admin.0, peer.cert(), req.into_inner())
        .await
        .map(Json)
}

/// A client enrollment token: it grants a certificate, so an admin's own
/// certificate only (D8).
pub async fn create_client_enrollment(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<ClientEnrollmentRequest>,
) -> Result<Json<ClientEnrollmentCreated>, CaError> {
    let peer = presented(peer)?;
    ca.create_client_enrollment(&admin.0, peer.cert(), req.into_inner())
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

/// Unseal this node (the one asked), with a recovery key or a passphrase.
pub async fn unseal(
    _admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    addr: Option<Extension<ConnectInfo<SocketAddr>>>,
    State(ca): Ca,
    req: Json<UnsealRequest>,
) -> Result<Json<PkiStatus>, CaError> {
    let peer = presented(peer)?;
    if !ca.unseal_allowed(addr.map(|Extension(ConnectInfo(a))| a.ip())) {
        return Err(CaError::new(
            429,
            "too many unseal attempts from this address: wait a minute",
        ));
    }
    ca.for_caller(peer.cert()).await?;
    ca.unseal(&req.secret).await?;
    Ok(Json(ca.status().await))
}

#[derive(Deserialize, Default)]
pub struct CrlFormat {
    #[serde(default)]
    format: Option<String>,
}

/// The CRL, for anyone (DER; `?format=pem` for PEM). `404` before the
/// first one is signed.
pub async fn crl(
    Query(CrlFormat { format }): Query<CrlFormat>,
    State(ca): Ca,
) -> Result<Response, CaError> {
    let crl = ca
        .crl()
        .await
        .ok_or_else(|| CaError::new(404, "no CRL yet: it's signed by an unsealed node"))?;
    Ok(match format.as_deref() {
        Some("pem") => (
            [(header::CONTENT_TYPE, "application/x-pem-file")],
            crate::crl_pem(&crl.der),
        )
            .into_response(),
        None | Some("der") => {
            ([(header::CONTENT_TYPE, "application/pkix-crl")], crl.der).into_response()
        }
        Some(f) => return Err(CaError::new(400, format!("format '{}': der or pem", f))),
    })
}

/// Every certificate on record.
pub async fn certs(_auth: CanRead, State(ca): Ca) -> Result<Json<Vec<CertListing>>, CaError> {
    ca.list_certs().await.map(Json)
}

/// Revoke a certificate: by an admin's certificate or token (it only takes
/// authority away, D8).
pub async fn revoke_cert(
    _admin: IsAdmin,
    peer: Option<Extension<Peer>>,
    Path(serial): Path<String>,
    State(ca): Ca,
    req: Json<RevokeCertRequest>,
) -> Result<(), CaError> {
    let serial = Serial::parse(&serial).map_err(|e| CaError::new(400, e))?;
    let caller = peer.as_ref().map(|Extension(p)| p.serial());
    ca.revoke_cert(serial, req.into_inner(), caller).await
}

/// Mint an `rdt2` token: it grants authority, so an admin's own
/// certificate only (D8). A sealed node answers 503.
pub async fn create_token(
    _admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<TokenRequest>,
) -> Result<Json<TokenCreated>, CaError> {
    let peer = presented(peer)?;
    ca.create_token(peer.cert(), req.into_inner())
        .await
        .map(Json)
}

/// Revoke a token (or the S3/SQS keys made from it).
pub async fn revoke_token(
    _admin: IsAdmin,
    Path(id): Path<String>,
    State(ca): Ca,
    req: Json<RevokeTokenRequest>,
) -> Result<(), CaError> {
    let id = TokenId::parse(&id).map_err(|e| CaError::new(400, e))?;
    ca.revoke_token(id, req.into_inner()).await
}

/// Seal every node. It only takes authority away (D8): an admin's
/// certificate or token.
pub async fn seal(
    admin: IsAdmin,
    State(ca): Ca,
    req: Json<SealRequest>,
) -> Result<Json<PkiStatus>, CaError> {
    ca.seal(req.into_inner(), Some(admin.0.name)).await?;
    Ok(Json(ca.status().await))
}

/// A new DEK and recovery key in place of every slot (CA-3d). It changes
/// what unlocks the CA, so an admin's own certificate only (D8), on an
/// unsealed node: the one that keeps the new DEK.
pub async fn rekey(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<RekeyRequest>,
) -> Result<Json<Rekeyed>, CaError> {
    let peer = presented(peer)?;
    ca.rekey(peer.cert(), req.into_inner(), Some(admin.0.name))
        .await
        .map(Json)
}

/// Add a key slot: it changes how the CA is unlocked, so an admin's own
/// certificate only (D8), on an unsealed node.
pub async fn add_slot(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<SlotRequest>,
) -> Result<Json<SlotCreated>, CaError> {
    let peer = presented(peer)?;
    ca.add_slot(peer.cert(), req.into_inner(), Some(admin.0.name))
        .await
        .map(Json)
}

pub async fn remove_slot(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    Path(id): Path<String>,
    Query(q): Query<RemoveSlotQuery>,
    State(ca): Ca,
) -> Result<(), CaError> {
    let peer = presented(peer)?;
    ca.for_caller(peer.cert()).await?;
    ca.remove_slot(&id, q.force, Some(admin.0.name)).await
}

fn issuer_id(s: &str) -> Result<IssuerId, CaError> {
    s.trim_start_matches("issuer-")
        .parse()
        .map(IssuerId)
        .map_err(|_| CaError::new(400, format!("'{}': an issuer is issuer-N", s)))
}

/// A new active issuer (CA-4d): it grants authority, so an admin's own
/// certificate only (D8), on an unsealed node.
pub async fn add_issuer(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<NewIssuerRequest>,
) -> Result<Json<IssuerAdded>, CaError> {
    let peer = presented(peer)?;
    ca.add_issuer(peer.cert(), req.into_inner(), Some(admin.0.name))
        .await
        .map(Json)
}

/// What still depends on an issuer.
pub async fn issuer_use(
    _auth: CanRead,
    Path(id): Path<String>,
    State(ca): Ca,
) -> Result<Json<IssuerUse>, CaError> {
    ca.issuer_use(issuer_id(&id)?).await.map(Json)
}

#[derive(Deserialize, Default)]
pub struct Force {
    #[serde(default)]
    force: bool,
}

/// Revoke an issuer: it takes authority away (D8), but every certificate
/// it signed with it, so it waits for the cluster's own to move
/// (`?force=true` doesn't).
pub async fn revoke_issuer(
    admin: IsAdmin,
    Path(id): Path<String>,
    Query(Force { force }): Query<Force>,
    State(ca): Ca,
) -> Result<Json<IssuerUse>, CaError> {
    ca.revoke_issuer(issuer_id(&id)?, force, Some(admin.0.name))
        .await
        .map(Json)
}

/// The root's CRL, signed by the admin with the root.
pub async fn set_root_crl(
    admin: IsAdminCert,
    peer: Option<Extension<Peer>>,
    State(ca): Ca,
    req: Json<RootCrlRequest>,
) -> Result<(), CaError> {
    let peer = presented(peer)?;
    ca.set_root_crl(peer.cert(), &req.crl_pem, Some(admin.0.name))
        .await
        .map(|_| ())
}

/// The root's CRL, for anyone (DER; `?format=pem`). `404` before the
/// first issuer is revoked.
pub async fn root_crl(
    Query(CrlFormat { format }): Query<CrlFormat>,
    State(ca): Ca,
) -> Result<Response, CaError> {
    let crl = ca
        .root_crl()
        .await
        .ok_or_else(|| CaError::new(404, "no root CRL: no issuer has been revoked"))?;
    Ok(match format.as_deref() {
        Some("pem") => (
            [(header::CONTENT_TYPE, "application/x-pem-file")],
            crate::crl_pem(&crl.der),
        )
            .into_response(),
        None | Some("der") => {
            ([(header::CONTENT_TYPE, "application/pkix-crl")], crl.der).into_response()
        }
        Some(f) => return Err(CaError::new(400, format!("format '{}': der or pem", f))),
    })
}

/// The routes, on this node's CA service.
pub fn router(ca: Arc<CaService>) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/pki/ca.pem", get(ca_pem))
        .route("/api/v1/pki/status", get(status))
        .route("/api/v1/pki/join", post(join))
        .route("/api/v1/pki/enroll", post(enroll))
        .route("/api/v1/pki/renew", post(renew))
        .route("/api/v1/pki/self/renew", post(renew_self))
        .route("/api/v1/pki/sign", post(sign))
        .route("/api/v1/pki/enrollments", post(create_enrollment))
        .route(
            "/api/v1/pki/enrollments/client",
            post(create_client_enrollment),
        )
        .route("/api/v1/pki/enrollments/{id}", delete(cancel_enrollment))
        .route("/api/v1/pki/unseal", post(unseal))
        .route("/api/v1/pki/seal", post(seal))
        .route("/api/v1/pki/rekey", post(rekey))
        .route("/api/v1/pki/slots", post(add_slot))
        .route("/api/v1/pki/slots/{id}", delete(remove_slot))
        .route("/api/v1/pki/certs", get(certs))
        .route("/api/v1/pki/crl", get(crl))
        .route("/api/v1/pki/root-crl", get(root_crl).post(set_root_crl))
        .route("/api/v1/pki/issuers", post(add_issuer))
        .route("/api/v1/pki/issuers/{id}", get(issuer_use))
        .route("/api/v1/pki/issuers/{id}/revoke", post(revoke_issuer))
        .route("/api/v1/pki/certs/{serial}/revoke", post(revoke_cert))
        .route("/api/v1/pki/tokens", post(create_token))
        .route("/api/v1/pki/tokens/{id}/revoke", post(revoke_token))
        .with_state(ca)
}
