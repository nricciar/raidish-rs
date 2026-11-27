//! The cluster CA's replicated state: what `docs/ca-service-plan.md` §3
//! calls the domain model.
//!
//! Only intent and public facts live here: the trust anchor (root), the
//! issuers (intermediates) with their keys *sealed* under the cluster DEK,
//! the key slots that unwrap that DEK, a compact registry of issued
//! certificates, revocations, enrollment tokens (hashed, MACed) and profile
//! overrides. No private key is ever here in the clear, no leaf PEM, no
//! CRL (derived), and nothing about which nodes are unsealed (asked of the
//! nodes live).
//!
//! A compromised node is a Raft voter and can commit any [`PkiOp`]. So
//! nothing here that grants authority is trusted for being here: issuers
//! must chain to the anchor (checked in [`apply`] through a
//! [`CertChecker`], at the entry's stamped time), and an enrollment counts
//! only with a MAC that an unsealed signer checks before redeeming it.
//!
//! Plain data, serde only: it builds for the wasm dashboard too. The crypto
//! that uses it is in `raidish-auth`.

use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

const DAY_MS: u64 = 86_400_000;

// ---- hex (de)serialization of byte strings ---------------------------------

fn to_hex(b: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(D[(x >> 4) as usize] as char);
        s.push(D[(x & 15) as usize] as char);
    }
    s
}

fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "bad hex".to_string()))
        .collect()
}

mod hex_vec {
    pub fn serialize<S: serde::Serializer>(b: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::to_hex(b))
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s: String = serde::Deserialize::deserialize(d)?;
        super::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

mod hex_array {
    pub fn serialize<S: serde::Serializer, const N: usize>(
        b: &[u8; N],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::to_hex(b))
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>, const N: usize>(
        d: D,
    ) -> Result<[u8; N], D::Error> {
        let s: String = serde::Deserialize::deserialize(d)?;
        let v = super::from_hex(&s).map_err(serde::de::Error::custom)?;
        v.try_into()
            .map_err(|_| serde::de::Error::custom(format!("expected {} bytes", N)))
    }
}

// ---- identifiers -------------------------------------------------------------

/// An X.509 serial number: lowercase hex of its value, without the ASN.1
/// sign-padding `00` byte or other leading zeros, so serials read by rcgen,
/// x509-parser and rustls all compare equal. Build it with
/// [`Serial::from_be_bytes`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct Serial(String);

impl Serial {
    /// From the serial's big-endian bytes (as DER encodes them, with or
    /// without the sign byte).
    pub fn from_be_bytes(b: &[u8]) -> Serial {
        let first = b.iter().position(|&x| x != 0).unwrap_or(b.len());
        let s = to_hex(&b[first..]);
        Serial(if s.is_empty() { "00".into() } else { s })
    }

    /// Parse a serial as `ca list` prints it (hex, any case, `:` allowed).
    pub fn parse(s: &str) -> Result<Serial, String> {
        let clean: String = s.chars().filter(|c| *c != ':').collect();
        let clean = if clean.len() % 2 == 1 {
            format!("0{}", clean)
        } else {
            clean
        };
        Ok(Serial::from_be_bytes(
            &from_hex(&clean.to_ascii_lowercase())
                .map_err(|_| format!("'{}' is not a serial number (hex)", s))?,
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Serial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// SHA-256 of a certificate's DER: what a join token pins the root by.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct CertFingerprint(#[serde(with = "hex_array")] pub [u8; 32]);

impl fmt::Display for CertFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

/// A Subject (or Authority) Key Identifier: links a leaf to its issuer
/// without trusting the registry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct KeyId(#[serde(with = "hex_vec")] pub Vec<u8>);

/// An issuer's number: 1, 2, … in creation order.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct IssuerId(pub u32);

impl fmt::Display for IssuerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "issuer-{}", self.0)
    }
}

/// The id of a token (bearer, S3 key, or enrollment): 16 lowercase hex
/// digits (8 random bytes), as `rdt1` ids are.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct TokenId(String);

impl TokenId {
    pub fn from_bytes(b: [u8; 8]) -> TokenId {
        TokenId(to_hex(&b))
    }

    pub fn parse(s: &str) -> Result<TokenId, String> {
        if s.len() == 16
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            Ok(TokenId(s.to_string()))
        } else {
            Err(format!("'{}' is not a token id (16 hex digits)", s))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---- profiles ----------------------------------------------------------------

/// The kinds of certificate the CA issues. A closed set: each kind's OU,
/// key usages and how its identity is derived are fixed in code
/// ([`ProfileKind::spec`]); only lifetimes can be changed
/// ([`PkiOp::SetProfile`]).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileKind {
    /// A `listen` node (OU `node`): server and client.
    Node,
    /// The command line (OU `admin`).
    Admin,
    /// Read-only access (OU `readonly`).
    Readonly,
    /// Signs `rdt2` tokens (OU `token-signer`, no TLS use). CA-2.
    TokenSigner,
    /// A host's libvirtd and qemu (OU `libvirt`, no raidish role).
    Libvirt,
    /// A TLS server outside raidish (OU `service`).
    Server,
    /// A TLS client outside raidish, e.g. k3s's etcd client (OU `service`).
    Client,
}

/// What a [`ProfileKind`] is, fixed in code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileSpec {
    pub ou: &'static str,
    pub server_auth: bool,
    pub client_auth: bool,
    /// The codeSigning EKU: the token signer's, so no TLS stack accepts it.
    pub code_signing: bool,
    /// gnutls (libvirt, qemu) wants KeyEncipherment on a server cert.
    pub key_encipherment: bool,
    /// May its holder renew it itself (same identity)?
    pub self_renew: bool,
    pub default_days: u32,
    pub max_days: u32,
    /// What `max_days` may be raised to at most.
    pub hard_max_days: u32,
}

impl ProfileKind {
    pub const ALL: [ProfileKind; 7] = [
        ProfileKind::Node,
        ProfileKind::Admin,
        ProfileKind::Readonly,
        ProfileKind::TokenSigner,
        ProfileKind::Libvirt,
        ProfileKind::Server,
        ProfileKind::Client,
    ];

    pub fn spec(self) -> ProfileSpec {
        let base = ProfileSpec {
            ou: "",
            server_auth: false,
            client_auth: true,
            code_signing: false,
            key_encipherment: false,
            self_renew: true,
            default_days: 90,
            max_days: 365,
            hard_max_days: 730,
        };
        match self {
            ProfileKind::Node => ProfileSpec {
                ou: "node",
                server_auth: true,
                default_days: 30,
                max_days: 90,
                hard_max_days: 365,
                ..base
            },
            ProfileKind::Admin => ProfileSpec {
                ou: "admin",
                ..base
            },
            ProfileKind::Readonly => ProfileSpec {
                ou: "readonly",
                ..base
            },
            ProfileKind::TokenSigner => ProfileSpec {
                ou: "token-signer",
                client_auth: false,
                code_signing: true,
                self_renew: false,
                default_days: 730,
                max_days: 730,
                ..base
            },
            ProfileKind::Libvirt => ProfileSpec {
                ou: "libvirt",
                server_auth: true,
                key_encipherment: true,
                default_days: 365,
                max_days: 730,
                ..base
            },
            ProfileKind::Server => ProfileSpec {
                ou: "service",
                server_auth: true,
                client_auth: false,
                self_renew: false,
                ..base
            },
            ProfileKind::Client => ProfileSpec {
                ou: "service",
                self_renew: false,
                ..base
            },
        }
    }

    /// May an admin have it signed directly (`ca issue`, `/sign`)? Node
    /// certs come only from joins and renewals, the token signer only from
    /// the CA itself.
    pub fn admin_issuable(self) -> bool {
        !matches!(self, ProfileKind::Node | ProfileKind::TokenSigner)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ProfileKind::Node => "node",
            ProfileKind::Admin => "admin",
            ProfileKind::Readonly => "readonly",
            ProfileKind::TokenSigner => "token-signer",
            ProfileKind::Libvirt => "libvirt",
            ProfileKind::Server => "server",
            ProfileKind::Client => "client",
        }
    }
}

impl fmt::Display for ProfileKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ProfileKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        ProfileKind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| {
                format!(
                    "unknown profile '{}' (one of: {})",
                    s,
                    ProfileKind::ALL.map(|k| k.as_str()).join(", ")
                )
            })
    }
}

/// A profile's lifetimes, where they differ from its spec's.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileParams {
    pub default_days: u32,
    pub max_days: u32,
}

// ---- sealed material ---------------------------------------------------------

/// AES-256-GCM ciphertext (with its tag) and nonce. What it decrypts with,
/// and the AAD, depend on where it is.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SealedBlob {
    pub v: u8,
    #[serde(with = "hex_array")]
    pub nonce: [u8; 12],
    #[serde(with = "hex_vec")]
    pub ct: Vec<u8>,
}

/// Argon2id parameters for a slot whose secret a person types or keeps.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KdfParams {
    #[serde(with = "hex_array")]
    pub salt: [u8; 16],
    pub m_kib: u32,
    pub t: u32,
    pub p: u32,
}

/// How a key slot's KEK is had.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SlotKind {
    /// A passphrase someone chose (CA-3).
    Passphrase(KdfParams),
    /// A generated `RDK1-…` recovery key.
    RecoveryKey(KdfParams),
    /// A 256-bit key one node holds (keyfile, systemd-creds; CA-3).
    NodeKey { node: NodeId },
}

/// One way to unwrap the cluster DEK.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeySlot {
    pub id: String,
    pub label: String,
    pub kind: SlotKind,
    /// The DEK, sealed under this slot's KEK (AAD: cluster id, slot id).
    pub wrapped_dek: SealedBlob,
}

/// What unlocks the CA, replicated. Whether a node *is* unsealed is that
/// node's runtime state, never stored.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SealConfig {
    /// Random per cluster, bound into every AAD.
    #[serde(with = "hex_array")]
    pub cluster_id: [u8; 16],
    pub slots: Vec<KeySlot>,
    /// Bumped by `ca seal` and slot changes: an unsealed node whose epoch
    /// is older drops its DEK (CA-3).
    pub epoch: u64,
}

// ---- the CA's certificates ---------------------------------------------------

/// The root. Set once by [`PkiOp::Bootstrap`], never changed.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TrustAnchor {
    pub cert_pem: String,
    pub fingerprint: CertFingerprint,
    /// Only if the operator kept the root in the cluster (sealed under a
    /// recovery key, not the DEK). Offline by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_key: Option<SealedBlob>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum IssuerStatus {
    /// Signs new certificates. Exactly one issuer is.
    Active,
    /// Signs nothing new; what it signed stays valid until it expires.
    Retired,
    /// Everything it signed is refused.
    Revoked { at_ms: u64 },
}

/// An intermediate CA (`pathLen=0`), signed by the root.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IssuerRecord {
    pub cert_pem: String,
    pub serial: Serial,
    pub ski: KeyId,
    pub not_after_ms: u64,
    /// Its PKCS#8 key under the DEK (AAD: "raidish-issuer", anchor
    /// fingerprint, id).
    pub sealed_key: SealedBlob,
    pub status: IssuerStatus,
}

/// The identity that signs `rdt2` tokens (CA-2).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TokenSignerRecord {
    /// The leaf and the issuer that signed it.
    pub chain_pem: String,
    pub serial: Serial,
    pub not_after_ms: u64,
    pub sealed_key: SealedBlob,
}

/// What a certificate asserts. Always derived by the signer: a CSR gives
/// only its public key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub profile: ProfileKind,
    pub cn: String,
    pub sans: Vec<String>,
    /// For node and libvirt certs: the member it was issued to (what a
    /// `--replace` join revokes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<NodeId>,
}

/// The registry's record of a certificate issued (no PEM).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IssuedCert {
    pub identity: Identity,
    pub issuer: IssuerId,
    pub not_before_ms: u64,
    pub not_after_ms: u64,
    /// The serial this one renewed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renews: Option<Serial>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RevocationReason {
    Unspecified,
    KeyCompromise,
    Superseded,
    CessationOfOperation,
}

impl std::str::FromStr for RevocationReason {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "unspecified" => RevocationReason::Unspecified,
            "compromised" | "key-compromise" => RevocationReason::KeyCompromise,
            "superseded" => RevocationReason::Superseded,
            "retired" | "cessation-of-operation" => RevocationReason::CessationOfOperation,
            _ => {
                return Err(format!(
                    "unknown reason '{}' (compromised, superseded, retired, unspecified)",
                    s
                ));
            }
        })
    }
}

/// "This was revoked": intent only, kept until what it revoked would have
/// expired anyway.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Revocation {
    pub reason: RevocationReason,
    pub at_ms: u64,
    /// When the revoked thing expires (None: never, kept for ever).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
}

// ---- enrollment ----------------------------------------------------------------

/// What an enrollment token may obtain.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EnrollScope {
    /// A node certificate for the pool layout `fingerprint`: for `member`
    /// only (None: any member), with `extra_sans` an admin approved on top
    /// of the derived ones (a private IP, say). `replace`: may take over a
    /// member id that has a live cert, revoking its certs (as
    /// key-compromise if `compromised`).
    Node {
        fingerprint: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<NodeId>,
        #[serde(default)]
        extra_sans: Vec<String>,
        #[serde(default)]
        replace: bool,
        #[serde(default)]
        compromised: bool,
    },
    /// One identity of a client profile, its CN fixed now (CA-4).
    Client { profile: ProfileKind, cn: String },
}

/// A capability to obtain one kind of identity. The token string the admin
/// gets is `rdj1.<root fp>.<id>.<secret>`; only the secret's hash is here.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Enrollment {
    pub scope: EnrollScope,
    #[serde(with = "hex_array")]
    pub secret_sha256: [u8; 32],
    /// As created (covered by the MAC).
    pub uses: u32,
    /// Raft-tracked; only goes down.
    pub uses_left: u32,
    pub expires_ms: u64,
    pub created_by: String,
    /// HMAC-SHA256 of [`Enrollment::mac_input`] under a key derived from
    /// the DEK: only an unsealed signer can make one, and the signer checks
    /// it before redeeming the enrollment. One committed through Raft by a
    /// node without the DEK is refused.
    #[serde(with = "hex_array")]
    pub mac: [u8; 32],
}

fn put(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

impl Enrollment {
    /// The bytes the MAC covers: every field but `uses_left` and the MAC,
    /// length-prefixed so no two enrollments encode alike.
    pub fn mac_input(&self, id: &TokenId) -> Vec<u8> {
        let mut out = b"raidish-enrollment-v1".to_vec();
        put(&mut out, id.as_str().as_bytes());
        match &self.scope {
            EnrollScope::Node {
                fingerprint,
                member,
                extra_sans,
                replace,
                compromised,
            } => {
                put(&mut out, b"node");
                put(&mut out, fingerprint.as_bytes());
                put(
                    &mut out,
                    member
                        .map(|m| m.get().to_string())
                        .unwrap_or_default()
                        .as_bytes(),
                );
                put(&mut out, &(extra_sans.len() as u32).to_be_bytes());
                for s in extra_sans {
                    put(&mut out, s.as_bytes());
                }
                put(&mut out, &[*replace as u8, *compromised as u8]);
            }
            EnrollScope::Client { profile, cn } => {
                put(&mut out, b"client");
                put(&mut out, profile.as_str().as_bytes());
                put(&mut out, cn.as_bytes());
            }
        }
        put(&mut out, &self.secret_sha256);
        put(&mut out, &self.uses.to_be_bytes());
        put(&mut out, &self.expires_ms.to_be_bytes());
        put(&mut out, self.created_by.as_bytes());
        out
    }
}

/// The revocation list for the active issuer's certificates (CA-2b),
/// signed by that issuer. In Raft, so every node serves the same one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CrlRecord {
    /// The issuer that signed it (the active one when it was made).
    pub issuer: IssuerId,
    #[serde(with = "hex_vec")]
    pub der: Vec<u8>,
    /// The CRL number: only ever goes up.
    pub number: u64,
    pub this_update_ms: u64,
    pub next_update_ms: u64,
    /// The `PkiState::generation` it covers: behind if less.
    pub generation: u64,
}

/// What [`CertChecker::crl`] reads off a signed CRL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrlFacts {
    pub number: u64,
    pub this_update_ms: u64,
    pub next_update_ms: u64,
}

// ---- the state -----------------------------------------------------------------

/// The whole CA, as Raft has it (`ClusterState::pki`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PkiState {
    pub anchor: TrustAnchor,
    pub issuers: BTreeMap<IssuerId, IssuerRecord>,
    /// Signs `rdt2` tokens (CA-2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_signer: Option<TokenSignerRecord>,
    pub seal: SealConfig,
    #[serde(default)]
    pub certs: BTreeMap<Serial, IssuedCert>,
    #[serde(default)]
    pub revoked_certs: BTreeMap<Serial, Revocation>,
    #[serde(default)]
    pub revoked_tokens: BTreeMap<TokenId, Revocation>,
    #[serde(default)]
    pub enrollments: BTreeMap<TokenId, Enrollment>,
    /// Lifetime overrides only.
    #[serde(default)]
    pub profiles: BTreeMap<ProfileKind, ProfileParams>,
    /// The current CRL (CA-2b). Not counted in `generation`: verifiers
    /// don't read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crl: Option<CrlRecord>,
    /// Bumped by every change a verifier must see.
    #[serde(default)]
    pub generation: u64,
}

/// A change to [`PkiState`], proposed through Raft as `LeaseOp::Pki`.
/// Signatures and keys are made before proposing (by a signer); applying
/// only checks and records.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PkiOp {
    /// Install the CA: only if there is none yet.
    Bootstrap(Box<PkiState>),
    /// A new issuer, which must chain to the anchor.
    AddIssuer {
        id: IssuerId,
        record: Box<IssuerRecord>,
        activate: bool,
    },
    RevokeIssuer {
        id: IssuerId,
    },
    RecordIssued {
        serial: Serial,
        cert: IssuedCert,
    },
    RevokeCert {
        serial: Serial,
        reason: RevocationReason,
    },
    RevokeToken {
        id: TokenId,
        expires_ms: Option<u64>,
    },
    CreateEnrollment {
        id: TokenId,
        enrollment: Box<Enrollment>,
    },
    /// Use an enrollment once, for `member` (node scopes). Refused without
    /// a trace (nothing decremented) if anything is off.
    ConsumeEnrollment {
        id: TokenId,
        #[serde(with = "hex_array")]
        secret_sha256: [u8; 32],
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<NodeId>,
    },
    CancelEnrollment {
        id: TokenId,
    },
    SetSeal(SealConfig),
    Seal,
    SetProfile {
        kind: ProfileKind,
        params: ProfileParams,
    },
    /// A newer CRL, signed by the active issuer.
    SetCrl(Box<CrlRecord>),
    /// The identity that signs `rdt2` tokens, issued by the active issuer
    /// (D6). Replaces the one there is.
    SetTokenSigner(Box<TokenSignerRecord>),
}

/// What [`CertChecker`] reads off a CA certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertFacts {
    pub serial: Serial,
    pub ski: KeyId,
    pub not_before_ms: u64,
    pub not_after_ms: u64,
    pub fingerprint: CertFingerprint,
}

/// Checks CA certificates for [`apply`]. Implemented with real X.509 in
/// `raidish-auth` (this crate has no crypto); `at_ms` is always the Raft
/// entry's stamped time, never the wall clock, so replaying an old log gives
/// the same answers.
pub trait CertChecker {
    /// `pem` is a self-signed CA allowing one intermediate (`pathLen=1`),
    /// valid at `at_ms`.
    fn anchor(&self, pem: &str, at_ms: u64) -> Result<CertFacts, String>;
    /// `pem` is a CA with `pathLen=path_len`, signed by `parent_pem`'s key
    /// under its name, valid at `at_ms`.
    fn ca(
        &self,
        pem: &str,
        parent_pem: &str,
        path_len: u8,
        at_ms: u64,
    ) -> Result<CertFacts, String>;
    /// `der` is a CRL signed by `issuer_pem`'s key under its name.
    fn crl(&self, der: &[u8], issuer_pem: &str) -> Result<CrlFacts, String>;
    /// `chain_pem` is a token signer's leaf (OU `token-signer`, its only
    /// EKU codeSigning, not a CA) then `issuer_pem`, signed by that issuer,
    /// valid at `at_ms`.
    fn token_signer(
        &self,
        chain_pem: &str,
        issuer_pem: &str,
        at_ms: u64,
    ) -> Result<CertFacts, String>;
}

/// A checker that accepts nothing: ops that need a certificate checked are
/// refused. For state machines that never see the CA (tests, the
/// committer's in-memory one).
pub struct NoCertChecker;

impl CertChecker for NoCertChecker {
    fn anchor(&self, _: &str, _: u64) -> Result<CertFacts, String> {
        Err("no certificate checker here".into())
    }
    fn ca(&self, _: &str, _: &str, _: u8, _: u64) -> Result<CertFacts, String> {
        Err("no certificate checker here".into())
    }
    fn crl(&self, _: &[u8], _: &str) -> Result<CrlFacts, String> {
        Err("no certificate checker here".into())
    }
    fn token_signer(&self, _: &str, _: &str, _: u64) -> Result<CertFacts, String> {
        Err("no certificate checker here".into())
    }
}

impl PkiState {
    /// The issuer that signs new certificates.
    pub fn active_issuer(&self) -> Option<(IssuerId, &IssuerRecord)> {
        self.issuers
            .iter()
            .find(|(_, r)| r.status == IssuerStatus::Active)
            .map(|(id, r)| (*id, r))
    }

    /// The lifetimes `kind` is issued with.
    pub fn params(&self, kind: ProfileKind) -> ProfileParams {
        self.profiles.get(&kind).copied().unwrap_or_else(|| {
            let s = kind.spec();
            ProfileParams {
                default_days: s.default_days,
                max_days: s.max_days,
            }
        })
    }

    pub fn is_revoked(&self, serial: &Serial) -> bool {
        self.revoked_certs.contains_key(serial)
    }

    /// Is `serial` a recorded, unexpired, unrevoked certificate at `now_ms`?
    pub fn is_live(&self, serial: &Serial, now_ms: u64) -> bool {
        self.certs
            .get(serial)
            .is_some_and(|c| c.not_after_ms > now_ms && !self.is_revoked(serial))
    }

    /// `member`'s live certificates (its node cert, its libvirt certs).
    pub fn member_certs(
        &self,
        member: NodeId,
        now_ms: u64,
    ) -> impl Iterator<Item = (&Serial, &IssuedCert)> {
        self.certs
            .iter()
            .filter(move |(s, c)| c.identity.member == Some(member) && self.is_live(s, now_ms))
    }

    fn has_live_node_cert(&self, member: NodeId, now_ms: u64) -> bool {
        self.member_certs(member, now_ms)
            .any(|(_, c)| c.identity.profile == ProfileKind::Node)
    }

    /// Install-time checks: the anchor is a root, issuers chain to it,
    /// exactly one is active.
    fn validate(&self, now_ms: u64, checker: &dyn CertChecker) -> Result<(), String> {
        let facts = checker
            .anchor(&self.anchor.cert_pem, now_ms)
            .map_err(|e| format!("trust anchor: {}", e))?;
        if facts.fingerprint != self.anchor.fingerprint {
            return Err("trust anchor: fingerprint doesn't match its certificate".into());
        }
        let active = self
            .issuers
            .values()
            .filter(|r| r.status == IssuerStatus::Active)
            .count();
        if active != 1 {
            return Err(format!("{} active issuers (must be one)", active));
        }
        for (id, r) in &self.issuers {
            check_issuer(&self.anchor, *id, r, now_ms, checker)?;
        }
        if let Some(t) = &self.token_signer {
            check_token_signer(self, t, now_ms, checker)?;
        }
        for (serial, c) in &self.certs {
            if !self.issuers.contains_key(&c.issuer) {
                return Err(format!("certificate {}: unknown {}", serial, c.issuer));
            }
        }
        for (id, e) in &self.enrollments {
            if e.uses_left > e.uses {
                return Err(format!("enrollment {}: more uses left than made", id));
            }
        }
        Ok(())
    }

    fn apply_op(
        &mut self,
        op: &PkiOp,
        now_ms: u64,
        checker: &dyn CertChecker,
    ) -> Result<(), String> {
        match op {
            PkiOp::Bootstrap(_) => return Err("the CA is already set up".into()),
            PkiOp::AddIssuer {
                id,
                record,
                activate,
            } => {
                if self.issuers.contains_key(id) {
                    return Err(format!("{} exists", id));
                }
                let mut record = (**record).clone();
                record.status = if *activate {
                    IssuerStatus::Active
                } else {
                    IssuerStatus::Retired
                };
                check_issuer(&self.anchor, *id, &record, now_ms, checker)?;
                if *activate {
                    for r in self.issuers.values_mut() {
                        if r.status == IssuerStatus::Active {
                            r.status = IssuerStatus::Retired;
                        }
                    }
                }
                self.issuers.insert(*id, record);
                self.generation += 1;
            }
            PkiOp::RevokeIssuer { id } => {
                let r = self
                    .issuers
                    .get_mut(id)
                    .ok_or_else(|| format!("no {}", id))?;
                match r.status {
                    IssuerStatus::Active => {
                        return Err(format!(
                            "{} is the active issuer: activate another one first",
                            id
                        ));
                    }
                    IssuerStatus::Revoked { .. } => {}
                    IssuerStatus::Retired => {
                        r.status = IssuerStatus::Revoked { at_ms: now_ms };
                        self.generation += 1;
                    }
                }
            }
            PkiOp::RecordIssued { serial, cert } => {
                if let Some(existing) = self.certs.get(serial) {
                    // The same record sent again (a retried proposal).
                    return match existing == cert {
                        true => Ok(()),
                        false => Err(format!("serial {} is taken", serial)),
                    };
                }
                if cert.identity.profile == ProfileKind::TokenSigner {
                    return Err("the token signer isn't recorded as a certificate".into());
                }
                let issuer = self
                    .issuers
                    .get(&cert.issuer)
                    .ok_or_else(|| format!("no {}", cert.issuer))?;
                if issuer.status != IssuerStatus::Active {
                    return Err(format!("{} doesn't sign any more", cert.issuer));
                }
                if cert.not_after_ms <= cert.not_before_ms {
                    return Err("a certificate must end after it starts".into());
                }
                if cert.not_after_ms > issuer.not_after_ms {
                    return Err(format!("a certificate can't outlive {}", cert.issuer));
                }
                let max = self.params(cert.identity.profile).max_days as u64 * DAY_MS;
                // Slack for the backdated start (clocks slightly behind).
                if cert.not_after_ms - cert.not_before_ms > max + DAY_MS {
                    return Err(format!(
                        "{} certificates live {} days at most",
                        cert.identity.profile,
                        self.params(cert.identity.profile).max_days
                    ));
                }
                self.certs.insert(serial.clone(), cert.clone());
                self.generation += 1;
            }
            PkiOp::RevokeCert { serial, reason } => {
                // The token signer isn't in `certs`, but can be revoked
                // (its tokens with it).
                let expires_ms = match (self.certs.get(serial), &self.token_signer) {
                    (Some(c), _) => Some(c.not_after_ms),
                    (None, Some(t)) if t.serial == *serial => Some(t.not_after_ms),
                    _ => return Err(format!("no certificate {} on record", serial)),
                };
                self.revoked_certs
                    .entry(serial.clone())
                    .or_insert(Revocation {
                        reason: *reason,
                        at_ms: now_ms,
                        expires_ms,
                    });
                self.generation += 1;
            }
            PkiOp::RevokeToken { id, expires_ms } => {
                self.revoked_tokens.entry(id.clone()).or_insert(Revocation {
                    reason: RevocationReason::Unspecified,
                    at_ms: now_ms,
                    expires_ms: *expires_ms,
                });
                self.generation += 1;
            }
            PkiOp::CreateEnrollment { id, enrollment } => {
                if self.enrollments.contains_key(id) {
                    return Err(format!("enrollment {} exists", id));
                }
                if enrollment.expires_ms <= now_ms {
                    return Err("the enrollment has already expired".into());
                }
                if enrollment.uses == 0 || enrollment.uses_left != enrollment.uses {
                    return Err("an enrollment starts with all its uses".into());
                }
                if let EnrollScope::Client { profile, .. } = &enrollment.scope
                    && !profile.admin_issuable()
                {
                    return Err(format!("no enrollment issues {} certificates", profile));
                }
                self.enrollments.insert(id.clone(), (**enrollment).clone());
            }
            PkiOp::ConsumeEnrollment {
                id,
                secret_sha256,
                member,
            } => {
                let e = self
                    .enrollments
                    .get(id)
                    .ok_or("unknown or used-up token")?
                    .clone();
                if e.expires_ms <= now_ms {
                    return Err("the token has expired".into());
                }
                if e.secret_sha256 != *secret_sha256 {
                    return Err("wrong token secret".into());
                }
                if e.uses_left == 0 {
                    return Err("the token is used up".into());
                }
                match (&e.scope, member) {
                    (
                        EnrollScope::Node {
                            member: scoped,
                            replace,
                            compromised,
                            ..
                        },
                        Some(m),
                    ) => {
                        if scoped.is_some_and(|s| s != *m) {
                            return Err(format!("the token is for {}, not {}", scoped.unwrap(), m));
                        }
                        if self.has_live_node_cert(*m, now_ms) {
                            if !*replace {
                                return Err(format!(
                                    "{} already has a live certificate (make a token with \
                                     --replace to take its place)",
                                    m
                                ));
                            }
                            let reason = if *compromised {
                                RevocationReason::KeyCompromise
                            } else {
                                RevocationReason::Superseded
                            };
                            let old: Vec<(Serial, u64)> = self
                                .member_certs(*m, now_ms)
                                .map(|(s, c)| (s.clone(), c.not_after_ms))
                                .collect();
                            for (s, exp) in old {
                                self.revoked_certs.insert(
                                    s,
                                    Revocation {
                                        reason,
                                        at_ms: now_ms,
                                        expires_ms: Some(exp),
                                    },
                                );
                            }
                            self.generation += 1;
                        }
                    }
                    (EnrollScope::Node { .. }, None) => {
                        return Err("a node token is used for a member".into());
                    }
                    (EnrollScope::Client { .. }, None) => {}
                    (EnrollScope::Client { .. }, Some(_)) => {
                        return Err("not a node token".into());
                    }
                }
                let left = e.uses_left - 1;
                if left == 0 {
                    self.enrollments.remove(id);
                } else if let Some(e) = self.enrollments.get_mut(id) {
                    e.uses_left = left;
                }
            }
            PkiOp::CancelEnrollment { id } => {
                self.enrollments.remove(id);
            }
            PkiOp::SetSeal(cfg) => {
                if cfg.cluster_id != self.seal.cluster_id {
                    return Err("the cluster id can't change".into());
                }
                if cfg.epoch < self.seal.epoch {
                    return Err("the seal epoch can't go back".into());
                }
                if cfg.slots.is_empty() {
                    return Err("the CA needs at least one key slot".into());
                }
                self.seal = cfg.clone();
                self.generation += 1;
            }
            PkiOp::Seal => {
                self.seal.epoch += 1;
                self.generation += 1;
            }
            PkiOp::SetProfile { kind, params } => {
                let hard = kind.spec().hard_max_days;
                if params.default_days == 0
                    || params.default_days > params.max_days
                    || params.max_days > hard
                {
                    return Err(format!(
                        "{} certificates: 0 < default ({}) ≤ max ({}) ≤ {} days",
                        kind, params.default_days, params.max_days, hard
                    ));
                }
                self.profiles.insert(*kind, *params);
            }
            PkiOp::SetTokenSigner(record) => {
                check_token_signer(self, record, now_ms, checker)?;
                self.token_signer = Some((**record).clone());
                self.generation += 1;
            }
            PkiOp::SetCrl(record) => {
                let (active, issuer) = self.active_issuer().ok_or("the CA has no active issuer")?;
                if record.issuer != active {
                    return Err(format!(
                        "a CRL from {}, not the active {}",
                        record.issuer, active
                    ));
                }
                let facts = checker
                    .crl(&record.der, &issuer.cert_pem)
                    .map_err(|e| format!("CRL: {}", e))?;
                if facts.number != record.number
                    || facts.this_update_ms != record.this_update_ms
                    || facts.next_update_ms != record.next_update_ms
                {
                    return Err("CRL: record doesn't match the CRL".into());
                }
                if let Some(cur) = &self.crl
                    && cur.issuer == record.issuer
                    && record.number <= cur.number
                {
                    return Err(format!(
                        "CRL {} is no newer than the current {}",
                        record.number, cur.number
                    ));
                }
                if record.generation > self.generation {
                    return Err("CRL: covers a state not reached yet".into());
                }
                if record.next_update_ms <= now_ms || record.this_update_ms > now_ms + 5 * 60_000 {
                    return Err("CRL: not current at this time".into());
                }
                self.crl = Some((**record).clone());
            }
        }
        Ok(())
    }

    /// Forget what no longer matters: certificates and revocations a day
    /// past their expiry (WebPKI refuses expired certs anyway), expired
    /// enrollments, and retired or revoked issuers once no leaf of theirs
    /// can be valid (leaves can't outlive their issuer).
    pub fn prune(&mut self, now_ms: u64) {
        let gone = |t: u64| t.saturating_add(DAY_MS) <= now_ms;
        self.certs.retain(|_, c| !gone(c.not_after_ms));
        self.revoked_certs
            .retain(|_, r| !r.expires_ms.is_some_and(gone));
        self.revoked_tokens
            .retain(|_, r| !r.expires_ms.is_some_and(gone));
        self.enrollments.retain(|_, e| e.expires_ms > now_ms);
        self.issuers
            .retain(|_, r| r.status == IssuerStatus::Active || !gone(r.not_after_ms));
    }

    /// What `cluster status` and the dashboard may see: no sealed keys,
    /// no slots' wrapped DEKs, no enrollment hashes.
    pub fn view(&self) -> PkiView {
        PkiView {
            anchor_cert_pem: self.anchor.cert_pem.clone(),
            anchor_fingerprint: self.anchor.fingerprint,
            root_in_cluster: self.anchor.sealed_key.is_some(),
            issuers: self
                .issuers
                .iter()
                .map(|(id, r)| IssuerView {
                    id: *id,
                    serial: r.serial.clone(),
                    not_after_ms: r.not_after_ms,
                    status: r.status.clone(),
                })
                .collect(),
            slots: self
                .seal
                .slots
                .iter()
                .map(|s| SlotView {
                    id: s.id.clone(),
                    label: s.label.clone(),
                    kind: match &s.kind {
                        SlotKind::Passphrase(_) => "passphrase".into(),
                        SlotKind::RecoveryKey(_) => "recovery-key".into(),
                        SlotKind::NodeKey { node } => format!("node-key ({})", node),
                    },
                })
                .collect(),
            seal_epoch: self.seal.epoch,
            certs: self.certs.len(),
            revoked_certs: self.revoked_certs.len(),
            revoked_tokens: self.revoked_tokens.len(),
            enrollments: self.enrollments.len(),
            token_signer: self.token_signer.as_ref().map(|t| TokenSignerView {
                serial: t.serial.clone(),
                not_after_ms: t.not_after_ms,
                revoked: self.is_revoked(&t.serial),
            }),
            generation: self.generation,
        }
    }
}

fn check_issuer(
    anchor: &TrustAnchor,
    id: IssuerId,
    r: &IssuerRecord,
    now_ms: u64,
    checker: &dyn CertChecker,
) -> Result<(), String> {
    if matches!(r.status, IssuerStatus::Revoked { .. }) {
        return Ok(());
    }
    let facts = checker
        .ca(&r.cert_pem, &anchor.cert_pem, 0, now_ms)
        .map_err(|e| format!("{}: {}", id, e))?;
    if facts.serial != r.serial || facts.ski != r.ski || facts.not_after_ms != r.not_after_ms {
        return Err(format!("{}: record doesn't match its certificate", id));
    }
    Ok(())
}

/// `t` is a token signer issued by `pki`'s active issuer, valid at `now_ms`.
fn check_token_signer(
    pki: &PkiState,
    t: &TokenSignerRecord,
    now_ms: u64,
    checker: &dyn CertChecker,
) -> Result<(), String> {
    let (_, issuer) = pki.active_issuer().ok_or("the CA has no active issuer")?;
    let facts = checker
        .token_signer(&t.chain_pem, &issuer.cert_pem, now_ms)
        .map_err(|e| format!("token signer: {}", e))?;
    if facts.serial != t.serial || facts.not_after_ms != t.not_after_ms {
        return Err("token signer: record doesn't match its certificate".into());
    }
    Ok(())
}

/// Apply `op` to the CA state at the cluster's time `now_ms`: all or
/// nothing (a refused op changes nothing), then prune.
pub fn apply(
    pki: &mut Option<Arc<PkiState>>,
    op: &PkiOp,
    now_ms: u64,
    checker: &dyn CertChecker,
) -> Result<(), String> {
    let mut next = match (op, pki.as_deref()) {
        (PkiOp::Bootstrap(_), Some(_)) => return Err("the CA is already set up".into()),
        (PkiOp::Bootstrap(state), None) => {
            state.validate(now_ms, checker)?;
            (**state).clone()
        }
        (_, None) => return Err("no CA (run `raidish ca init`)".into()),
        (op, Some(state)) => {
            let mut next = state.clone();
            next.apply_op(op, now_ms, checker)?;
            next
        }
    };
    next.prune(now_ms);
    *pki = Some(Arc::new(next));
    Ok(())
}

// ---- the read side ---------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct IssuerView {
    pub id: IssuerId,
    pub serial: Serial,
    pub not_after_ms: u64,
    pub status: IssuerStatus,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SlotView {
    pub id: String,
    pub label: String,
    pub kind: String,
}

/// The CA as anyone who may read cluster status sees it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PkiView {
    pub anchor_cert_pem: String,
    pub anchor_fingerprint: CertFingerprint,
    pub root_in_cluster: bool,
    pub issuers: Vec<IssuerView>,
    pub slots: Vec<SlotView>,
    pub seal_epoch: u64,
    pub certs: usize,
    pub revoked_certs: usize,
    pub revoked_tokens: usize,
    pub enrollments: usize,
    /// What signs `rdt2` tokens (revoke it by its serial to end them all).
    #[serde(default)]
    pub token_signer: Option<TokenSignerView>,
    pub generation: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TokenSignerView {
    pub serial: Serial,
    pub not_after_ms: u64,
    pub revoked: bool,
}

/// `GET /api/v1/pki/status`: the CA and this node's part in it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PkiStatus {
    /// The node that answered.
    pub node: String,
    /// Whether it can sign (holds the DEK).
    pub unsealed: bool,
    /// The CA is only staged on this node (before the cluster's first
    /// quorum), not yet in Raft.
    pub staged: bool,
    /// None: no CA anywhere this node knows of.
    pub pki: Option<PkiView>,
    /// Problems worth saying (e.g. a leftover one-shot unseal file).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// What learns the cluster CA's state as it's applied (the node's live
/// trust, `raidish_auth::live::Trust`; `docs/ca-service-plan.md` §3.7).
/// Told when `generation` changes, or the CA appears, and once when it's
/// added. Called while applying: it must only swap in what it builds.
pub trait PkiObserver: Send + Sync {
    fn observe_pki(&self, pki: Option<&PkiState>);
}

/// A certificate in `GET /api/v1/pki/certs` and `ca issue`'s answer.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CertListing {
    pub serial: Serial,
    pub cert: IssuedCert,
    pub revoked: Option<Revocation>,
}

#[cfg(test)]
#[path = "tests/pki_tests.rs"]
mod tests;
