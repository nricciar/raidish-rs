//! `/api/v1/pki/*` (see `docs/ca-service-plan.md` §6). A node that can't
//! sign answers `503`: clients try the next node. Only `/join`, which a
//! token authenticates, is passed on by the node itself.

use crate::CaService;
use crate::api::{
    EnrollmentCreated, EnrollmentRequest, Issued, JoinRequest, RenewRequest, SignRequest,
    UnsealRequest,
};
use crate::service::CaError;
use raidish_auth::auth::{CanRead, IsAdmin, IsAdminCert};
use raidish_types::pki::{PkiStatus, TokenId};
use rocket::http::{ContentType, Status};
use rocket::serde::json::Json;
use rocket::{State, delete, get, post};
use std::net::IpAddr;
use std::sync::Arc;

type Ca = State<Arc<CaService>>;

impl<'r> rocket::response::Responder<'r, 'static> for CaError {
    fn respond_to(self, req: &'r rocket::Request<'_>) -> rocket::response::Result<'static> {
        let status = Status::from_code(self.status).unwrap_or(Status::InternalServerError);
        (status, format!("{}\n", self.msg)).respond_to(req)
    }
}

/// The root, for anyone (a joiner pins it by the fingerprint in its token).
#[get("/api/v1/pki/ca.pem")]
pub async fn ca_pem(ca: &Ca) -> Result<(ContentType, String), CaError> {
    let pki = ca
        .current()
        .await
        .ok_or_else(|| CaError::new(404, "no CA on this cluster"))?;
    Ok((
        ContentType::new("application", "x-pem-file"),
        pki.anchor.cert_pem.clone(),
    ))
}

#[get("/api/v1/pki/status")]
pub async fn status(_auth: CanRead, ca: &Ca) -> Json<PkiStatus> {
    Json(ca.status().await)
}

#[post("/api/v1/pki/join?<forwarded>", data = "<req>")]
pub async fn join(
    ip: Option<IpAddr>,
    forwarded: Option<bool>,
    req: Json<JoinRequest>,
    ca: &Ca,
) -> Result<Json<Issued>, CaError> {
    if !ca.join_allowed(ip) {
        return Err(CaError::new(
            429,
            "too many joins from this address: wait a minute",
        ));
    }
    ca.join(req.into_inner(), forwarded.unwrap_or(false))
        .await
        .map(Json)
}

/// The caller's own certificate (by TLS, never a token), renewed.
#[post("/api/v1/pki/renew", data = "<req>")]
pub async fn renew(
    cert: rocket::mtls::Certificate<'_>,
    req: Json<RenewRequest>,
    ca: &Ca,
) -> Result<Json<Issued>, CaError> {
    ca.renew(cert.as_bytes(), &req.csr_pem).await.map(Json)
}

#[post("/api/v1/pki/sign", data = "<req>")]
pub async fn sign(
    admin: IsAdminCert,
    req: Json<SignRequest>,
    ca: &Ca,
) -> Result<Json<Issued>, CaError> {
    ca.sign(&admin.0, req.into_inner()).await.map(Json)
}

#[post("/api/v1/pki/enrollments", data = "<req>")]
pub async fn create_enrollment(
    admin: IsAdminCert,
    req: Json<EnrollmentRequest>,
    ca: &Ca,
) -> Result<Json<EnrollmentCreated>, CaError> {
    ca.create_enrollment(&admin.0, req.into_inner())
        .await
        .map(Json)
}

#[delete("/api/v1/pki/enrollments/<id>")]
pub async fn cancel_enrollment(_admin: IsAdmin, id: &str, ca: &Ca) -> Result<(), CaError> {
    let id = TokenId::parse(id).map_err(|e| CaError::new(400, e))?;
    ca.cancel_enrollment(id).await
}

/// Unseal this node (the one asked).
#[post("/api/v1/pki/unseal", data = "<req>")]
pub async fn unseal(
    _admin: IsAdminCert,
    req: Json<UnsealRequest>,
    ca: &Ca,
) -> Result<Json<PkiStatus>, CaError> {
    ca.unseal(&req.recovery_key).await?;
    Ok(Json(ca.status().await))
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        ca_pem,
        status,
        join,
        renew,
        sign,
        create_enrollment,
        cancel_enrollment,
        unseal
    ]
}
