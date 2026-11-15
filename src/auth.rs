//! Who is asking, and may they: client certificates and tokens.
//!
//! With `tls:` configured, every request to a `listen` node must come with
//! one of:
//!
//! * a **client certificate** issued by a trusted CA (`raidish cert issue`).
//!   Its OU is the role. Nodes and the command line use these.
//! * a **token** (`raidish token create`), as `Authorization: Bearer …` or,
//!   for websockets (browsers can't set headers on them), `?access_token=…`.
//!   The dashboard uses these.
//!
//! Roles, weakest first: `readonly` (reads, status, the dashboard), `admin`
//! (everything the command line does), `node` (admin, plus Raft between
//! peers). Without `tls:`, anyone may do anything, as before.
//!
//! A token is `rdt1.<payload>.<signature>.<signer certificate>` (base64url),
//! signed with an **admin** certificate's key. Any node can check it on its
//! own: the signer certificate must chain to a trusted CA and be an admin's,
//! and the token's id must not be in `auth.revoked`. So there is no user
//! database and nothing to replicate; dropping the signer's CA from the
//! bundle, or listing the id, revokes it.

use crate::pki::{self, Role};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use rustls::pki_types::{CertificateDer, UnixTime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use x509_parser::prelude::*;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct AuthConfig {
    /// Token ids that are no longer accepted (`raidish token inspect` shows
    /// a token's id).
    #[serde(default)]
    pub revoked: Vec<String>,
}

const TOKEN_PREFIX: &str = "rdt1";

/// What a token says.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// Unique id (for revocation).
    pub id: String,
    /// Who it's for.
    pub sub: String,
    pub role: String,
    /// Issued at (unix seconds).
    pub iat: i64,
    /// Expires at (unix seconds), never if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Mint a token for `sub` with `role`, signed by the admin identity
/// (`cert_der`, `key_pkcs8`). `ttl` in seconds; None: never expires.
pub fn create_token(
    sub: &str,
    role: Role,
    ttl: Option<i64>,
    cert_der: &[u8],
    key_pkcs8: &[u8],
) -> Result<(String, Claims), String> {
    if role == Role::Node {
        return Err("tokens can't carry the node role (nodes use certificates)".into());
    }
    let signer = pki::inspect_der(cert_der)?;
    if signer.role != Some(Role::Admin) {
        return Err(format!(
            "tokens must be signed with an admin certificate ('{}' is {})",
            signer.name,
            signer
                .role
                .map(|r| r.to_string())
                .unwrap_or("not a raidish cert".into())
        ));
    }
    let rng = ring::rand::SystemRandom::new();
    let mut id = [0u8; 8];
    ring::rand::SecureRandom::fill(&rng, &mut id).map_err(|_| "no randomness")?;
    let iat = now();
    let claims = Claims {
        id: id.iter().map(|b| format!("{:02x}", b)).collect(),
        sub: sub.to_string(),
        role: role.to_string(),
        iat,
        exp: ttl.map(|t| iat + t),
    };
    let payload = B64.encode(serde_json::to_vec(&claims).map_err(|e| e.to_string())?);
    let key = ring::signature::EcdsaKeyPair::from_pkcs8(
        &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        key_pkcs8,
        &rng,
    )
    .map_err(|e| format!("signing key: {}", e))?;
    let signed = format!("{}.{}", TOKEN_PREFIX, payload);
    let sig = key
        .sign(&rng, signed.as_bytes())
        .map_err(|_| "signing failed".to_string())?;
    Ok((
        format!(
            "{}.{}.{}",
            signed,
            B64.encode(sig.as_ref()),
            B64.encode(cert_der)
        ),
        claims,
    ))
}

/// A token, decoded but not yet trusted.
pub struct Parsed {
    pub claims: Claims,
    pub signer: pki::CertInfo,
    signed: String,
    sig: Vec<u8>,
}

pub fn parse_token(token: &str) -> Result<Parsed, String> {
    let parts: Vec<&str> = token.trim().split('.').collect();
    let [prefix, payload, sig, cert] = parts[..] else {
        return Err("not a raidish token".into());
    };
    if prefix != TOKEN_PREFIX {
        return Err("not a raidish token".into());
    }
    let bad = |_| "malformed token".to_string();
    let claims: Claims = serde_json::from_slice(&B64.decode(payload).map_err(bad)?)
        .map_err(|_| "malformed token")?;
    let cert = B64.decode(cert).map_err(bad)?;
    Ok(Parsed {
        claims,
        signer: pki::inspect_der(&cert)?,
        signed: format!("{}.{}", prefix, payload),
        sig: B64.decode(sig).map_err(bad)?,
    })
}

/// Checks client certificates and tokens against the trust bundle.
pub struct Verifier {
    roots: Arc<rustls::RootCertStore>,
    revoked: Vec<String>,
}

impl Verifier {
    pub fn new(roots: Vec<CertificateDer<'static>>, revoked: Vec<String>) -> Result<Self, String> {
        crate::tls::install_provider();
        let mut store = rustls::RootCertStore::empty();
        for c in roots {
            store.add(c).map_err(|e| format!("bad CA: {}", e))?;
        }
        Ok(Verifier {
            roots: Arc::new(store),
            revoked,
        })
    }

    pub fn from_config(tls: &crate::tls::TlsConfig, auth: &AuthConfig) -> Result<Self, String> {
        Self::new(tls.roots()?, auth.revoked.clone())
    }

    /// Does `der` chain to a trusted CA as a client certificate?
    fn chain_ok(&self, der: &[u8]) -> Result<(), String> {
        let verifier = rustls::server::WebPkiClientVerifier::builder(self.roots.clone())
            .build()
            .map_err(|e| e.to_string())?;
        verifier
            .verify_client_cert(&CertificateDer::from(der.to_vec()), &[], UnixTime::now())
            .map(|_| ())
            .map_err(|e| format!("signer certificate: {}", e))
    }

    /// A token's holder, if the token is good.
    pub fn verify_token(&self, token: &str) -> Result<Principal, String> {
        let t = parse_token(token)?;
        if t.signer.role != Some(Role::Admin) {
            return Err("token not signed by an admin".into());
        }
        self.chain_ok(&t.signer.der)?;
        let (_, cert) = X509Certificate::from_der(&t.signer.der).map_err(|e| e.to_string())?;
        let key = ring::signature::UnparsedPublicKey::new(
            &ring::signature::ECDSA_P256_SHA256_ASN1,
            &cert.public_key().subject_public_key.data,
        );
        key.verify(t.signed.as_bytes(), &t.sig)
            .map_err(|_| "bad token signature".to_string())?;
        if let Some(exp) = t.claims.exp
            && now() >= exp
        {
            return Err("token expired".into());
        }
        if self.revoked.contains(&t.claims.id) {
            return Err("token revoked".into());
        }
        let role: Role = t.claims.role.parse()?;
        if role == Role::Node {
            return Err("tokens can't carry the node role".into());
        }
        Ok(Principal {
            name: t.claims.sub,
            role,
            via: Via::Token(t.claims.id),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Via {
    /// Plain HTTP: TLS is off, so there's no authentication.
    Open,
    Cert,
    Token(String),
}

/// Who a request is from.
#[derive(Debug, Clone, Serialize)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    pub via: Via,
}

impl Principal {
    pub fn can_write(&self) -> bool {
        self.role >= Role::Admin
    }
}

/// Server-side state: None without TLS (no authentication).
pub struct AuthState(pub Option<Verifier>);

// ---- Rocket request guards ------------------------------------------------

use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};

fn bearer<'r>(req: &'r Request<'_>) -> Option<&'r str> {
    if let Some(h) = req.headers().get_one("Authorization") {
        return h.strip_prefix("Bearer ").map(str::trim);
    }
    req.query_value::<&str>("access_token").and_then(|r| r.ok())
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Principal {
    type Error = String;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, String> {
        let cached = req
            .local_cache_async(async {
                let Some(AuthState(Some(verifier))) = req.rocket().state::<AuthState>() else {
                    // No TLS: no authentication, everyone may do anything.
                    return Ok(Principal {
                        name: "anonymous".into(),
                        role: Role::Node,
                        via: Via::Open,
                    });
                };
                // Rocket has already checked a presented certificate's chain.
                if let Outcome::Success(cert) = req.guard::<rocket::mtls::Certificate<'_>>().await {
                    let info = pki::inspect_der(cert.as_bytes())?;
                    let role = info
                        .role
                        .ok_or("client certificate has no raidish role (OU)")?;
                    return Ok(Principal {
                        name: info.name,
                        role,
                        via: Via::Cert,
                    });
                }
                match bearer(req) {
                    Some(t) => verifier.verify_token(t),
                    None => Err("authentication required: a client certificate or a token".into()),
                }
            })
            .await;
        match cached {
            Ok(p) => Outcome::Success(p.clone()),
            Err(e) => Outcome::Error((Status::Unauthorized, e.clone())),
        }
    }
}

macro_rules! role_guard {
    ($name:ident, $doc:literal, |$p:ident| $ok:expr) => {
        #[doc = $doc]
        pub struct $name(pub Principal);

        #[rocket::async_trait]
        impl<'r> FromRequest<'r> for $name {
            type Error = String;

            async fn from_request(req: &'r Request<'_>) -> Outcome<Self, String> {
                match req.guard::<Principal>().await {
                    Outcome::Success($p) if $ok => Outcome::Success($name($p)),
                    Outcome::Success(p) => Outcome::Error((
                        Status::Forbidden,
                        format!("{} '{}' may not do this", p.role, p.name),
                    )),
                    Outcome::Error(e) => Outcome::Error(e),
                    Outcome::Forward(f) => Outcome::Forward(f),
                }
            }
        }
    };
}

role_guard!(CanRead, "Any authenticated caller.", |_p| true);
role_guard!(CanWrite, "Admins and nodes.", |p| p.role >= Role::Admin);
role_guard!(IsNode, "Nodes only (Raft between peers).", |p| p.role
    == Role::Node);

/// GET /api/v1/auth/whoami: who the server thinks you are (401 if nobody).
#[rocket::get("/api/v1/auth/whoami")]
pub fn whoami(p: Principal) -> rocket::serde::json::Json<Principal> {
    rocket::serde::json::Json(p)
}

/// Turn 401/403 into a short plain-text reason (Rocket's default is HTML).
#[rocket::catch(401)]
pub fn unauthorized() -> &'static str {
    "401 Unauthorized: present a client certificate or a token (raidish token create)\n"
}

#[rocket::catch(403)]
pub fn forbidden() -> &'static str {
    "403 Forbidden: your role may not do this\n"
}

#[cfg(test)]
#[path = "tests/auth_tests.rs"]
mod tests;
