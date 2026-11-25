//! S3/SQS access keys, derived from raidish tokens: nothing per user is
//! stored anywhere.
//!
//! SigV4 is symmetric (the server needs each key's secret), so a token
//! can't be an S3 key as it is. Instead a token holder trades it for a key
//! (`raidish token s3-key`, `POST /api/v1/services/keys`):
//!
//! ```text
//!   access key id  RDK1 <hex: token id (8 bytes), role (1; top bit: internal), expiry (u32 secs, 0: never)>
//!   secret         base64url(HMAC-SHA256(service key, "raidish-service-key-v1\n" + access key id))
//! ```
//!
//! The service key is one random key in the cluster's replicated state
//! (`ClusterState::service_key`, made by the first node that needs it, and
//! never shown by `cluster status`). So any node checks any request on its
//! own: the access key id says who and what (its claims can't be changed
//! without changing the secret), and the secret is recomputed. A key made
//! from a token carries the token's id, role and expiry: revoking the token
//! (`auth.revoked`) revokes its keys, and a readonly token's keys only read.

use crate::auth::Verifier;
use crate::pki::Role;
use crate::sigv4::{AuthError, Keys};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;

const PREFIX: &str = "RDK1";
const CONTEXT: &[u8] = b"raidish-service-key-v1\n";
/// The role byte's flag for an internal key (`AccessKey::internal`).
const INTERNAL: u8 = 0x80;

/// Where the cluster's service key comes from (live, from the replicated
/// state; None: not made yet).
pub type KeySource = Arc<dyn Fn() -> Option<Vec<u8>> + Send + Sync>;

/// What an access key id says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessKey {
    /// The token's (or the key's own) id, hex: what `auth.revoked` lists.
    pub id: String,
    pub role: Role,
    /// Unix seconds; None: never.
    pub exp: Option<i64>,
    /// Minted by a node for asking another (`ServiceAuth::internal`), not
    /// traded for a token: only such a key may run the internal actions
    /// (an SQS dead-letter move). Covered by the secret like the rest.
    pub internal: bool,
}

impl AccessKey {
    pub fn encode(&self) -> Result<String, String> {
        let id = hex::decode(&self.id)
            .ok()
            .filter(|b| b.len() == 8)
            .ok_or_else(|| format!("bad key id '{}'", self.id))?;
        let role = match self.role {
            Role::Readonly => 1u8,
            Role::Admin => 2,
            Role::Node => return Err("access keys can't carry the node role".into()),
        } | if self.internal { INTERNAL } else { 0 };
        let exp = match self.exp {
            None => 0u32,
            Some(e) => u32::try_from(e.max(1)).map_err(|_| "expiry out of range")?,
        };
        let mut b = id;
        b.push(role);
        b.extend(exp.to_be_bytes());
        Ok(format!("{}{}", PREFIX, hex::encode_upper(b)))
    }

    pub fn parse(s: &str) -> Option<AccessKey> {
        let b = hex::decode(s.strip_prefix(PREFIX)?).ok()?;
        if b.len() != 13 {
            return None;
        }
        let role = match b[8] & !INTERNAL {
            1 => Role::Readonly,
            2 => Role::Admin,
            _ => return None,
        };
        let exp = u32::from_be_bytes(b[9..13].try_into().ok()?);
        Some(AccessKey {
            id: hex::encode(&b[..8]),
            role,
            exp: (exp != 0).then_some(exp as i64),
            internal: b[8] & INTERNAL != 0,
        })
    }
}

/// The secret of access key id `access_key`.
pub fn secret(service_key: &[u8], access_key: &str) -> String {
    let mut m = Hmac::<Sha256>::new_from_slice(service_key).expect("any key length");
    m.update(CONTEXT);
    m.update(access_key.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(m.finalize().into_bytes())
}

/// An access key id and its secret.
pub fn mint(service_key: &[u8], key: &AccessKey) -> Result<(String, String), String> {
    let id = key.encode()?;
    let s = secret(service_key, &id);
    Ok((id, s))
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// A random key id (hex).
pub fn random_id() -> String {
    let mut id = [0u8; 8];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut id).expect("randomness");
    hex::encode(id)
}

/// Checks S3/SQS requests' keys.
pub struct ServiceAuth {
    region: String,
    verifier: Arc<Verifier>,
    key: KeySource,
}

impl ServiceAuth {
    pub fn new(region: String, verifier: Arc<Verifier>, key: KeySource) -> Self {
        ServiceAuth {
            region,
            verifier,
            key,
        }
    }

    /// What a (verified) request's access key may do.
    pub fn role(&self, access_key: &str) -> Role {
        AccessKey::parse(access_key).map_or(Role::Readonly, |k| k.role)
    }

    /// Whether a (verified) request's access key is one a node minted to
    /// ask another (`internal`).
    pub fn is_internal(&self, access_key: &str) -> bool {
        AccessKey::parse(access_key).is_some_and(|k| k.internal)
    }

    /// A short-lived admin key for one node asking another.
    pub fn internal(&self) -> Result<(String, String), AuthError> {
        let k = (self.key)().ok_or(AuthError::UnknownKey)?;
        mint(
            &k,
            &AccessKey {
                id: random_id(),
                role: Role::Admin,
                exp: Some(now() + 3600),
                internal: true,
            },
        )
        .map_err(AuthError::Malformed)
    }
}

impl Keys for ServiceAuth {
    fn region(&self) -> String {
        self.region.clone()
    }

    fn secret(&self, id: &str) -> Result<String, AuthError> {
        let k = AccessKey::parse(id).ok_or(AuthError::UnknownKey)?;
        if k.exp.is_some_and(|e| now() >= e) {
            return Err(AuthError::KeyExpired);
        }
        if self.verifier.is_revoked(&k.id) {
            return Err(AuthError::UnknownKey);
        }
        let key = (self.key)().ok_or(AuthError::UnknownKey)?;
        Ok(secret(&key, id))
    }
}
