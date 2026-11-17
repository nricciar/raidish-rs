//! `POST /api/v1/services/keys`: trade a token (or an admin certificate)
//! for an S3/SQS access key (see [`super::keys`]).

use super::keys::{self, AccessKey};
use crate::auth::{AuthState, Principal, Via};
use crate::cluster::Cluster;
use crate::pki::Role;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, post};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Default, Debug)]
pub struct KeyRequest {
    /// The token to make it from (instead of whoever is asking: lets the
    /// command line, which has a certificate, make a key for a token).
    #[serde(default)]
    pub token: Option<String>,
    /// readonly or admin (default: the caller's role, at most admin).
    #[serde(default)]
    pub role: Option<String>,
    /// Lifetime (`90d`, `12h`...), at most the token's.
    #[serde(default)]
    pub expires: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KeyResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// What revokes it (`auth.revoked`): the token's id, for a token's key.
    pub id: String,
    pub role: String,
    /// Unix seconds; None: never.
    pub expires: Option<i64>,
}

type Res<T> = Result<T, (Status, String)>;

/// Make the key `req` asks for, as `caller`.
pub fn make_key(caller: &Principal, req: &KeyRequest, service_key: &[u8]) -> Res<KeyResponse> {
    let bad = |m: String| (Status::BadRequest, m);
    let top = caller.role.min(Role::Admin);
    let role = match &req.role {
        Some(r) => r.parse::<Role>().map_err(bad)?,
        None => top,
    };
    if role == Role::Node {
        return Err(bad("access keys are readonly or admin".into()));
    }
    if role > top {
        return Err((
            Status::Forbidden,
            format!("{} '{}' can't make {} keys", caller.role, caller.name, role),
        ));
    }
    let now = chrono::Utc::now().timestamp();
    let asked = match &req.expires {
        Some(e) => Some(now + crate::cli_pki::parse_duration(e).map_err(bad)?),
        None => None,
    };
    let exp = match (asked, caller.exp) {
        (Some(a), Some(t)) => Some(a.min(t)),
        (a, t) => a.or(t),
    };
    // A token's keys carry its id: revoking the token revokes them.
    let id = match &caller.via {
        Via::Token(id) => id.clone(),
        _ => keys::random_id(),
    };
    let key = AccessKey { id, role, exp };
    let (access_key_id, secret_access_key) = keys::mint(service_key, &key).map_err(bad)?;
    Ok(KeyResponse {
        access_key_id,
        secret_access_key,
        id: key.id,
        role: role.to_string(),
        expires: exp,
    })
}

#[post("/api/v1/services/keys", data = "<req>")]
pub async fn create_key(
    caller: Result<Principal, String>,
    req: Json<KeyRequest>,
    auth: &State<AuthState>,
    cluster: &State<Arc<Cluster>>,
) -> Res<Json<KeyResponse>> {
    let Some(verifier) = &auth.0 else {
        return Err((
            Status::BadRequest,
            "S3 and SQS need TLS (`tls:` in the config)".into(),
        ));
    };
    let caller = match &req.token {
        Some(t) => verifier
            .verify_token(t)
            .map_err(|e| (Status::Unauthorized, e))?,
        None => caller.map_err(|e| (Status::Unauthorized, e))?,
    };
    let service_key = cluster
        .ensure_service_key()
        .await
        .map_err(|e| (Status::ServiceUnavailable, e))?;
    make_key(&caller, &req, &service_key).map(Json)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![create_key]
}

#[cfg(test)]
#[path = "../tests/service_keys_tests.rs"]
mod tests;
