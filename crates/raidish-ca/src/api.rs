//! The `/api/v1/pki/*` request and answer bodies.

use raidish_types::pki::{IssuerId, ProfileKind, Serial, TokenId};
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

/// `POST /api/v1/pki/enrollments/client`: a token a client enrolls with
/// (`ca token create --client`, CA-4c).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientEnrollmentRequest {
    /// `admin`, `readonly` or `client`.
    pub profile: ProfileKind,
    pub cn: String,
    /// How long the token is good for (default a day).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientEnrollmentCreated {
    pub id: TokenId,
    pub profile: ProfileKind,
    pub cn: String,
    /// The `rdj1.…` token: shown once, never stored.
    pub token: String,
    pub expires_ms: u64,
}

/// `POST /api/v1/pki/enroll`: a client's certificate, by its token.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EnrollRequest {
    pub id: TokenId,
    /// The token's secret (base64url, as in the token).
    pub secret: String,
    /// Only its key is used: the name and profile are the token's.
    pub csr_pem: String,
}

/// What `/enroll` answers.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Enrolled {
    #[serde(flatten)]
    pub issued: Issued,
    pub profile: ProfileKind,
    pub cn: String,
    /// The nodes the signer knows of (by its pool config), to ask later.
    pub endpoints: Vec<String>,
}

/// `POST /api/v1/pki/issuers`: a new issuer, made by the admin with the
/// root (`ca rotate-issuer`, CA-4d). It becomes the active one.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NewIssuerRequest {
    /// The id it's made for (the next one): its key is sealed under it.
    pub id: IssuerId,
    pub cert_pem: String,
    pub key_pem: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IssuerAdded {
    pub id: IssuerId,
    pub serial: Serial,
    pub not_after_ms: u64,
    /// Was active, now retired.
    pub retired: Vec<IssuerId>,
}

/// `POST /api/v1/pki/root-crl`: the root's CRL, signed by the admin.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RootCrlRequest {
    pub crl_pem: String,
}

/// `POST /api/v1/pki/unseal`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UnsealRequest {
    /// A recovery key (`RDK1-…`) or a passphrase.
    #[serde(alias = "recovery_key")]
    pub secret: String,
}

/// `POST /api/v1/pki/seal`.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SealRequest {
    /// Keep node-key slots (those nodes unseal again when they restart).
    /// Default: they go, so a sealed cluster stays sealed through a reboot.
    #[serde(default)]
    pub keep_node_keys: bool,
}

/// `POST /api/v1/pki/rekey` (CA-3d).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RekeyRequest {
    /// The new recovery key (`RDK1-…`), made by the caller: then it's had
    /// before the rekey is sent, and an answer lost on the way back loses
    /// nothing. None: the node makes one and returns it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_key: Option<String>,
}

/// `POST /api/v1/pki/rekey`'s answer (CA-3d).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Rekeyed {
    /// The one slot left: a new recovery key's.
    pub slot_id: String,
    /// That key, shown this once, if the node made it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_key: Option<String>,
    /// The slots it replaced (passphrases and node keys must be added
    /// again).
    pub removed_slots: Vec<String>,
    /// Join tokens in Raft without the signer's MAC (forged through Raft):
    /// removed, not MACed anew.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_enrollments: Vec<String>,
    /// The node that rekeyed holds the new DEK (false: the CA was sealed
    /// meanwhile, and the new recovery key unseals it).
    #[serde(default)]
    pub held: bool,
}

/// What a slot opens with.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum NewSlotKind {
    /// A new `RDK1-` key, made by the node and returned once.
    RecoveryKey,
    Passphrase,
    /// A new `RNK1-` key for one member, made by the node and returned
    /// once: that member unseals itself with it at start (CA-3b).
    NodeKey,
}

/// `POST /api/v1/pki/slots`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SlotRequest {
    pub kind: NewSlotKind,
    /// Who or what it's for ("Alice's", "the safe").
    #[serde(default)]
    pub label: String,
    /// For `passphrase`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passphrase: Option<String>,
    /// For `node-key`: the member it's for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<raidish_types::types::NodeId>,
}

/// A slot added.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SlotCreated {
    pub id: String,
    /// For `recovery-key`: the key, shown this once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_key: Option<String>,
    /// For `node-key`: the key, shown this once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_key: Option<String>,
}

/// `DELETE /api/v1/pki/slots/{id}`.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RemoveSlotQuery {
    /// Even the last recovery-key slot.
    #[serde(default)]
    pub force: bool,
}

/// How long a join token is good for unless asked otherwise.
pub const DEFAULT_TOKEN_TTL_SECS: u64 = 24 * 3600;
/// And at most.
pub const MAX_TOKEN_TTL_SECS: u64 = 30 * 24 * 3600;

/// `POST /api/v1/pki/certs/{serial}/revoke`.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RevokeCertRequest {
    /// Default: unspecified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<raidish_types::pki::RevocationReason>,
    /// Revoke a member's only live node certificate anyway (that node
    /// is cut off until it joins again).
    #[serde(default)]
    pub force: bool,
}

/// `POST /api/v1/pki/tokens`: an `rdt2` token, signed by the cluster's
/// token signer.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TokenRequest {
    /// Who it's for.
    pub sub: String,
    /// `readonly` or `admin`.
    pub role: String,
    /// Lifetime in seconds; None: as long as the token signer lasts.
    /// Clamped to the signer's expiry either way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_s: Option<i64>,
}

/// A token minted.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TokenCreated {
    pub token: String,
    pub id: TokenId,
    pub sub: String,
    pub role: String,
    /// Unix seconds.
    pub exp: i64,
}

/// `POST /api/v1/pki/tokens/{id}/revoke`.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RevokeTokenRequest {
    /// When the token expires anyway (the revocation is kept until then).
    /// None: kept for good.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
}
