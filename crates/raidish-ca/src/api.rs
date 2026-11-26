//! The `/api/v1/pki/*` request and answer bodies.

use raidish_types::pki::{ProfileKind, Serial, TokenId};
use raidish_types::types::NodeId;
use serde::{Deserialize, Serialize};

/// `POST /api/v1/pki/join`: a node asking for its certificate.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinRequest {
    /// From the join token.
    pub id: TokenId,
    /// The token's secret (base64url, as in the token).
    pub secret: String,
    /// Who the joiner is by its own config, and that config's pool layout
    /// fingerprint: both must agree with the signer's.
    pub member: NodeId,
    pub fingerprint: String,
    /// Only its key is used.
    pub csr_pem: String,
}

/// A certificate issued: `join`, `renew` and `sign` answer with it.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Issued {
    pub serial: Serial,
    /// The leaf, then its issuer (what `cert.pem` holds).
    pub cert_pem: String,
    /// The root.
    pub ca_pem: String,
    pub not_after_ms: u64,
}

/// `POST /api/v1/pki/renew`: a new certificate for the caller's identity.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RenewRequest {
    pub csr_pem: String,
}

/// `POST /api/v1/pki/sign`: an admin issuing a client or server
/// certificate (`ca issue`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SignRequest {
    pub csr_pem: String,
    pub profile: ProfileKind,
    pub cn: String,
    #[serde(default)]
    pub sans: Vec<String>,
    /// Lifetime (default: the profile's).
    #[serde(default)]
    pub days: Option<u32>,
    /// For a libvirt certificate: the member it's for (revoked with it on
    /// `--replace`).
    #[serde(default)]
    pub member: Option<NodeId>,
}

/// `POST /api/v1/pki/enrollments`: a node join token (`ca token create`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EnrollmentRequest {
    pub member: NodeId,
    /// Names on top of the member's host in the signer's config.
    #[serde(default)]
    pub extra_sans: Vec<String>,
    /// May take the place of a member with a live certificate, revoking
    /// its certificates (as key-compromise if `compromised`).
    #[serde(default)]
    pub replace: bool,
    #[serde(default)]
    pub compromised: bool,
    /// How long it's good for (default a day).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EnrollmentCreated {
    pub id: TokenId,
    pub member: NodeId,
    /// The `rdj1.…` token: shown once, never stored.
    pub token: String,
    pub expires_ms: u64,
}

/// `POST /api/v1/pki/unseal`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UnsealRequest {
    pub recovery_key: String,
}

/// How long a join token is good for unless asked otherwise.
pub const DEFAULT_TOKEN_TTL_SECS: u64 = 24 * 3600;
/// And at most.
pub const MAX_TOKEN_TTL_SECS: u64 = 30 * 24 * 3600;
