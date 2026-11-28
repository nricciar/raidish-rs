//! Who is asking, and may they: client certificates and tokens.
//!
//! With `tls:` configured, every request to a `listen` node must come with
//! one of:
//!
//! * a **client certificate** issued by the cluster's CA (`raidish ca issue`,
//!   `raidish enroll`).
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
//!
//! A managed CA's tokens are `rdt2.<payload>.<signature>.<signer>.<issuer>`
//! (CA-2c, `docs/ca-service-plan.md` D6): signed by the cluster's token
//! signer (OU `token-signer`, codeSigning alone, so useless in TLS), minted
//! by `POST /api/v1/pki/tokens`, always expiring, by the signer's expiry at
//! the latest. They carry the issuer, so a token outlives a rotation of the
//! issuer, and revoking the signer ends every token it signed.

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
const TOKEN_PREFIX_2: &str = "rdt2";
/// id-kp-codeSigning (1.3.6.1.5.5.7.3.3), its DER contents.
const CODE_SIGNING: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x03];

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

/// Mint an `rdt1` token for `sub` with `role`, signed by an admin
/// certificate (`cert_der`, `key_pkcs8`). `ttl` in seconds; None: never
/// expires. Only tests make these now: nodes still accept them, but the
/// cluster mints `rdt2` ([`create_token_rdt2`]).
#[cfg(any(test, feature = "testutil"))]
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
    let iat = now();
    let claims = claims(sub, role, iat, ttl.map(|t| iat.saturating_add(t)))?;
    let token = sign(TOKEN_PREFIX, &claims, key_pkcs8, &[cert_der])?;
    Ok((token, claims))
}

/// Mint an `rdt2` token for `sub` with `role`, signed by the token signer
/// (`chain`: its certificate, then its issuer's; `key_pkcs8`). It expires
/// after `ttl` seconds, or with the signer, whichever is sooner.
pub fn create_token_rdt2(
    sub: &str,
    role: Role,
    ttl: Option<i64>,
    chain: &[CertificateDer<'_>],
    key_pkcs8: &[u8],
) -> Result<(String, Claims), String> {
    if role == Role::Node {
        return Err("tokens can't carry the node role (nodes use certificates)".into());
    }
    let [leaf, issuer] = chain else {
        return Err("the token signer's chain is its certificate and its issuer's".into());
    };
    let (_, cert) = X509Certificate::from_der(leaf).map_err(|e| e.to_string())?;
    crate::ca::check_token_signer_cert(&cert)?;
    let not_after = cert.validity().not_after.timestamp();
    let iat = now();
    let exp = match ttl {
        Some(t) => iat.saturating_add(t).min(not_after),
        None => not_after,
    };
    let claims = claims(sub, role, iat, Some(exp))?;
    let token = sign(TOKEN_PREFIX_2, &claims, key_pkcs8, &[leaf, issuer])?;
    Ok((token, claims))
}

fn claims(sub: &str, role: Role, iat: i64, exp: Option<i64>) -> Result<Claims, String> {
    let mut id = [0u8; 8];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut id)
        .map_err(|_| "no randomness")?;
    Ok(Claims {
        id: id.iter().map(|b| format!("{:02x}", b)).collect(),
        sub: sub.to_string(),
        role: role.to_string(),
        iat,
        exp,
    })
}

/// `<prefix>.<payload>.<signature>` then each of `certs`, all base64url.
fn sign(
    prefix: &str,
    claims: &Claims,
    key_pkcs8: &[u8],
    certs: &[&[u8]],
) -> Result<String, String> {
    let rng = ring::rand::SystemRandom::new();
    let payload = B64.encode(serde_json::to_vec(claims).map_err(|e| e.to_string())?);
    let key = ring::signature::EcdsaKeyPair::from_pkcs8(
        &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        key_pkcs8,
        &rng,
    )
    .map_err(|e| format!("signing key: {}", e))?;
    let signed = format!("{}.{}", prefix, payload);
    let sig = key
        .sign(&rng, signed.as_bytes())
        .map_err(|_| "signing failed".to_string())?;
    let mut token = format!("{}.{}", signed, B64.encode(sig.as_ref()));
    for c in certs {
        token.push('.');
        token.push_str(&B64.encode(c));
    }
    Ok(token)
}

/// A token, decoded but not yet trusted.
pub struct Parsed {
    pub claims: Claims,
    pub signer: pki::CertInfo,
    /// `rdt2`: the signer's issuer.
    pub issuer: Option<Vec<u8>>,
    signed: String,
    sig: Vec<u8>,
}

impl Parsed {
    /// `rdt2`: signed by the cluster's token signer.
    pub fn is_rdt2(&self) -> bool {
        self.issuer.is_some()
    }
}

pub fn parse_token(token: &str) -> Result<Parsed, String> {
    let parts: Vec<&str> = token.trim().split('.').collect();
    let bad = |_| "malformed token".to_string();
    let (prefix, payload, sig, cert, issuer) = match parts[..] {
        [p, payload, sig, cert] if p == TOKEN_PREFIX => (p, payload, sig, cert, None),
        [p, payload, sig, cert, issuer] if p == TOKEN_PREFIX_2 => (
            p,
            payload,
            sig,
            cert,
            Some(B64.decode(issuer).map_err(bad)?),
        ),
        _ => return Err("not a raidish token".into()),
    };
    let claims: Claims = serde_json::from_slice(&B64.decode(payload).map_err(bad)?)
        .map_err(|_| "malformed token")?;
    let cert = B64.decode(cert).map_err(bad)?;
    Ok(Parsed {
        claims,
        signer: pki::inspect_der(&cert)?,
        issuer,
        signed: format!("{}.{}", prefix, payload),
        sig: B64.decode(sig).map_err(bad)?,
    })
}

/// Checks client certificates and tokens against the trust bundle.
pub struct Verifier {
    roots: Arc<rustls::RootCertStore>,
    /// Issuers a token's signer certificate may chain through to a root:
    /// a managed CA's admin certs are signed by an issuer, and an `rdt1`
    /// token carries only the leaf. This process's own chain has the active
    /// issuer (see `from_config`).
    intermediates: Vec<CertificateDer<'static>>,
    /// What's revoked, live (see `crate::trust`).
    trust: Arc<crate::trust::Trust>,
}

impl Verifier {
    /// Refusing the token ids `revoked`, and nothing else until
    /// [`Verifier::with_trust`].
    pub fn new(roots: Vec<CertificateDer<'static>>, revoked: Vec<String>) -> Result<Self, String> {
        crate::tls::install_provider();
        let mut store = rustls::RootCertStore::empty();
        for c in roots {
            store.add(c).map_err(|e| format!("bad CA: {}", e))?;
        }
        Ok(Verifier {
            roots: Arc::new(store),
            intermediates: Vec::new(),
            trust: crate::trust::Trust::new(revoked),
        })
    }

    /// Refuse what `trust` does (the node's live revocations).
    pub fn with_trust(mut self, trust: Arc<crate::trust::Trust>) -> Self {
        self.trust = trust;
        self
    }

    pub fn trust(&self) -> &Arc<crate::trust::Trust> {
        &self.trust
    }

    /// Let token signers chain through `issuers` too.
    pub fn with_intermediates(mut self, issuers: Vec<CertificateDer<'static>>) -> Self {
        self.intermediates = issuers;
        self
    }

    pub fn from_config(tls: &crate::tls::TlsConfig, auth: &AuthConfig) -> Result<Self, String> {
        let issuers = match tls.identity()? {
            Some((chain, _)) => chain.into_iter().skip(1).collect(),
            None => Vec::new(),
        };
        let v = Self::new(tls.roots()?, auth.revoked.clone())?.with_intermediates(issuers);
        // The node's, which has `auth.revoked` in it already.
        Ok(match &tls.trust {
            Some(t) => v.with_trust(t.clone()),
            None => v,
        })
    }

    /// Does `der` chain to a trusted CA as a client certificate?
    fn chain_ok(&self, der: &[u8]) -> Result<(), String> {
        let verifier = rustls::server::WebPkiClientVerifier::builder(self.roots.clone())
            .build()
            .map_err(|e| e.to_string())?;
        verifier
            .verify_client_cert(
                &CertificateDer::from(der.to_vec()),
                &self.intermediates,
                UnixTime::now(),
            )
            .map(|_| ())
            .map_err(|e| format!("signer certificate: {}", e))
    }

    /// An `rdt2` signer: chains through `issuer` to a trusted root for
    /// codeSigning, and is a token signer (OU, nothing but codeSigning).
    fn signer_ok_rdt2(&self, leaf: &[u8], issuer: &[u8]) -> Result<(), String> {
        let leaf_der = CertificateDer::from(leaf);
        let cert = webpki::EndEntityCert::try_from(&leaf_der)
            .map_err(|e| format!("token signer: {:?}", e))?;
        cert.verify_for_usage(
            webpki::ALL_VERIFICATION_ALGS,
            &self.roots.roots,
            &[CertificateDer::from(issuer)],
            UnixTime::now(),
            // Checked on every certificate of the path, and issuers have
            // none: the leaf's own EKU (codeSigning, alone) is required
            // just below.
            webpki::KeyUsage::required_if_present(CODE_SIGNING),
            None,
            None,
        )
        .map_err(|e| format!("token signer: {:?}", e))?;
        let (_, x) = X509Certificate::from_der(leaf).map_err(|e| e.to_string())?;
        crate::ca::check_token_signer_cert(&x).map_err(|e| format!("token signer: {}", e))
    }

    /// Whether token (or access key) `id` is revoked.
    pub fn is_revoked(&self, id: &str) -> bool {
        self.trust.token_revoked(id)
    }

    /// A token's holder, if the token is good.
    pub fn verify_token(&self, token: &str) -> Result<Principal, String> {
        let t = parse_token(token)?;
        match &t.issuer {
            Some(issuer) => self.signer_ok_rdt2(&t.signer.der, issuer)?,
            None if t.signer.role != Some(Role::Admin) => {
                return Err("token not signed by an admin".into());
            }
            None => self.chain_ok(&t.signer.der)?,
        }
        if t.is_rdt2() && t.claims.exp.is_none() {
            return Err("an rdt2 token without an expiry".into());
        }
        self.trust
            .check_leaf(&t.signer.der)
            .map_err(|e| format!("token signer: {}", e))?;
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
        if self.trust.token_revoked(&t.claims.id) {
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
            exp: t.claims.exp,
        })
    }
}

pub use crate::api_types::{Principal, Via};

/// Server-side state: None without TLS (no authentication).
pub struct AuthState(pub Option<Verifier>);

// ---- request extractors (axum) --------------------------------------------

use crate::http::Refusal;
use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;

/// A request's token: `Authorization: Bearer …`, or `?access_token=…`.
fn bearer(parts: &Parts) -> Option<String> {
    if let Some(h) = parts.headers.get(axum::http::header::AUTHORIZATION) {
        return h
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")
            .map(|t| t.trim().to_string());
    }
    let q: Vec<(String, String)> = serde_urlencoded::from_str(parts.uri.query()?).ok()?;
    q.into_iter()
        .find(|(k, _)| k == "access_token")
        .map(|(_, v)| v)
}

/// One request's caller, worked out once.
#[derive(Clone)]
struct Resolved(Result<Principal, Refusal>);

fn resolve(parts: &Parts) -> Result<Principal, Refusal> {
    let Some(state) = parts.extensions.get::<Arc<AuthState>>() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "no authentication configured for this route".into(),
        ));
    };
    let Some(verifier) = &state.0 else {
        // No TLS: no authentication, everyone may do anything.
        return Ok(Principal {
            name: "anonymous".into(),
            role: Role::Node,
            via: Via::Open,
            exp: None,
        });
    };
    let unauthorized = |e: String| (StatusCode::UNAUTHORIZED, e);
    // The handshake has already checked a presented certificate's chain.
    if let Some(peer) = parts.extensions.get::<crate::http::Peer>() {
        // Revoked since the handshake: its connection is being closed.
        verifier
            .trust()
            .check_leaf(peer.cert())
            .map_err(unauthorized)?;
        let info = pki::inspect_der(peer.cert()).map_err(unauthorized)?;
        let role = info
            .role
            .ok_or_else(|| unauthorized("client certificate has no raidish role (OU)".into()))?;
        return Ok(Principal {
            name: info.name,
            role,
            via: Via::Cert,
            exp: None,
        });
    }
    match bearer(parts) {
        Some(t) => verifier.verify_token(&t).map_err(unauthorized),
        None => Err(unauthorized(
            "authentication required: present a client certificate or a token \
             (raidish token create)"
                .into(),
        )),
    }
}

fn caller(parts: &mut Parts) -> Result<Principal, Refusal> {
    if let Some(Resolved(r)) = parts.extensions.get::<Resolved>() {
        return r.clone();
    }
    let r = resolve(parts);
    parts.extensions.insert(Resolved(r.clone()));
    r
}

/// Any authenticated caller. A newtype because `Principal` is plain data
/// (`crate::api_types`, shared with the web dashboard).
#[derive(Clone, Debug)]
pub struct Authed(pub Principal);

impl std::ops::Deref for Authed {
    type Target = Principal;
    fn deref(&self) -> &Principal {
        &self.0
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Authed {
    type Rejection = Refusal;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Refusal> {
        caller(parts).map(Authed)
    }
}

/// The caller, or why there is none (for routes that take either).
impl<S: Send + Sync> axum::extract::OptionalFromRequestParts<S> for Authed {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> Result<Option<Self>, std::convert::Infallible> {
        Ok(caller(parts).ok().map(Authed))
    }
}

fn forbidden(p: &Principal, why: &str) -> Refusal {
    (
        StatusCode::FORBIDDEN,
        format!("{} '{}' may not do this{}", p.role, p.name, why),
    )
}

macro_rules! role_guard {
    ($name:ident, $doc:literal, |$p:ident| $ok:expr) => {
        #[doc = $doc]
        pub struct $name(pub Principal);

        impl<S: Send + Sync> FromRequestParts<S> for $name {
            type Rejection = Refusal;

            async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Refusal> {
                let $p = caller(parts)?;
                if $ok {
                    Ok($name($p))
                } else {
                    Err(forbidden(&$p, ""))
                }
            }
        }
    };
}

/// The header a node passes on an admin's request with: whose it is (for
/// the activity log, see `crate::events`). Believed only from nodes.
pub const ON_BEHALF: &str = "X-Raidish-On-Behalf-Of";

/// Who asked for a change: the caller, or the admin a node passes on a
/// request for (None: a node acting on its own).
pub struct Actor(pub Option<String>);

impl<S: Send + Sync> FromRequestParts<S> for Actor {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> Result<Self, std::convert::Infallible> {
        let Ok(p) = caller(parts) else {
            return Ok(Actor(None));
        };
        if p.role == Role::Node {
            let on_behalf = parts
                .headers
                .get(ON_BEHALF)
                .and_then(|v| v.to_str().ok())
                .map(|n| n.chars().filter(|c| !c.is_control()).take(128).collect());
            return Ok(Actor(on_behalf));
        }
        Ok(Actor(Some(p.name)))
    }
}

role_guard!(CanRead, "Any authenticated caller.", |_p| true);
role_guard!(CanWrite, "Admins and nodes.", |p| p.role >= Role::Admin);
role_guard!(IsNode, "Nodes only (Raft between peers).", |p| p.role
    == Role::Node);
role_guard!(
    IsAdmin,
    "Admins only, by certificate or token (not nodes): for what only takes \
     authority away (revoking, sealing).",
    |p| p.role == Role::Admin
);

/// An admin, by its own certificate on this very connection: no token, no
/// node passing a request on. For everything that grants authority or
/// changes how the CA is unlocked (signing, enrollments, key slots), so a
/// leaked dashboard token can't, and a compromised node can't claim to
/// speak for an admin (see `docs/ca-service-plan.md` D8).
pub struct IsAdminCert(pub Principal);

impl<S: Send + Sync> FromRequestParts<S> for IsAdminCert {
    type Rejection = Refusal;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Refusal> {
        let p = caller(parts)?;
        if p.role == Role::Admin && p.via == Via::Cert && !parts.headers.contains_key(ON_BEHALF) {
            Ok(IsAdminCert(p))
        } else {
            Err(forbidden(
                &p,
                ": it takes an admin certificate, presented directly",
            ))
        }
    }
}

/// GET /api/v1/auth/whoami: who the server thinks you are (401 if nobody).
pub async fn whoami(p: Authed) -> crate::http::Json<Principal> {
    crate::http::Json(p.0)
}

/// The routes of this module.
pub fn router() -> axum::Router {
    axum::Router::new().route("/api/v1/auth/whoami", axum::routing::get(whoami))
}

#[cfg(test)]
#[path = "tests/auth_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/rdt2_tests.rs"]
mod rdt2_tests;
