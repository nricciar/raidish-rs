//! `raidish ca init`: make the cluster CA on node 0, staged in its data dir
//! for the daemon to hand to Raft (see the crate docs).

use crate::service::{NodeScope, node_enrollment, write_staged};
use crate::token::JoinToken;
use crate::{Bundle, bootstrap_path, node_sans, now_ms, unseal_once_path, url_host};
use raidish_auth::ca::{self, LeafSpec, Signer, X509Checker};
use raidish_auth::envelope::{self, RootEnvelope};
use raidish_cluster::members::Members;
use raidish_types::pki::{
    self, Identity, IssuedCert, IssuerId, IssuerRecord, IssuerStatus, KdfParams, PkiOp, PkiState,
    ProfileKind, ProfileParams, SealConfig, TrustAnchor,
};
use raidish_types::types::NodeId;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

pub const ROOT_DAYS: u32 = 20 * 365;
pub const ISSUER_DAYS: u32 = 2 * 365;

pub struct InitOptions<'a> {
    /// Node 0's view of the pool (it's `me`).
    pub members: &'a Members,
    /// Node 0's Raft data dir.
    pub data_dir: &'a Path,
    /// Node 0's names (the first is its CN). Peers reach it by one of them.
    pub sans: Vec<String>,
    /// Extra names per peer (a private IP, say), put in its join token.
    pub peer_sans: BTreeMap<NodeId, Vec<String>>,
    /// Node certificates' lifetime (default: the profile's).
    pub node_days: Option<u32>,
    /// The operator's admin certificate's name.
    pub admin_name: String,
    /// How long the peers' join tokens last.
    pub token_ttl_ms: u64,
    /// Leave the one-shot unseal file for node 0's first start.
    pub unseal_once: bool,
    /// Replace an earlier `ca init`'s staged CA.
    pub force: bool,
    /// Argon2id cost (tests make it cheap).
    pub kdf: (u32, u32, u32),
}

pub struct PeerToken {
    pub member: NodeId,
    /// Its address in node 0's config.
    pub base: String,
    pub token: Zeroizing<String>,
}

pub struct InitOutput {
    pub root_fingerprint: pki::CertFingerprint,
    /// Unseals the CA (`ca unseal`): the only way back in after a full
    /// cluster restart. Shown once.
    pub recovery_key: Zeroizing<String>,
    /// Opens `root.enc`. Shown once.
    pub root_key: Zeroizing<String>,
    pub root_envelope: RootEnvelope,
    pub node0: Bundle,
    pub node0_sans: Vec<String>,
    pub admin: Bundle,
    pub tokens: Vec<PeerToken>,
}

fn kdf(opts: &InitOptions) -> Result<KdfParams, String> {
    envelope::kdf_params(opts.kdf.0, opts.kdf.1, opts.kdf.2)
}

/// Make the CA and stage it in `opts.data_dir` (`pki_bootstrap.json`, and
/// `pki/unseal-once` if asked). The certificates and secrets come back to
/// the caller to write and show; nothing else is written.
pub fn init(opts: &InitOptions) -> Result<InitOutput, String> {
    let staged = bootstrap_path(opts.data_dir);
    if staged.exists() && !opts.force {
        return Err(format!(
            "{} exists: a CA is staged already (--force replaces it)",
            staged.display()
        ));
    }
    if opts.sans.is_empty() {
        return Err("node 0 needs a name its peers reach it by (--san)".into());
    }
    raidish_auth::tls::install_provider();
    let now = now_ms();

    // The root, offline from here on: only `root.enc` has its key.
    let (root_pem, root_key_pem) = ca::create_root("raidish root", ROOT_DAYS)?;
    let root_fp = ca::fingerprint(&ca::first_der(&root_pem)?);
    let root_key = envelope::new_recovery_key()?;
    let root_envelope = RootEnvelope::seal(
        &root_pem,
        &root_key_pem,
        &envelope::parse_recovery_key(&root_key)?,
        kdf(opts)?,
    )?;

    // The issuer, its key under the cluster DEK.
    let (issuer_pem, issuer_key) =
        ca::create_issuer("raidish issuer 1", &root_pem, &root_key_pem, ISSUER_DAYS)?;
    drop(root_key_pem);
    let issuer_id = IssuerId(1);
    let issuer_facts = ca::facts(&ca::first_der(&issuer_pem)?)?;
    let dek = envelope::new_key()?;
    let cluster_id: [u8; 16] = envelope::random()?;
    let recovery_key = envelope::new_recovery_key()?;
    let slot = envelope::recovery_slot(
        &dek,
        &cluster_id,
        "recovery-1",
        "recovery key (ca init)",
        &envelope::parse_recovery_key(&recovery_key)?,
        kdf(opts)?,
    )?;
    let base = PkiState {
        anchor: TrustAnchor {
            cert_pem: root_pem.clone(),
            fingerprint: root_fp,
            sealed_key: None,
        },
        issuers: BTreeMap::from([(
            issuer_id,
            IssuerRecord {
                cert_pem: issuer_pem.clone(),
                serial: issuer_facts.serial.clone(),
                ski: issuer_facts.ski.clone(),
                not_after_ms: issuer_facts.not_after_ms,
                sealed_key: envelope::seal(
                    &dek,
                    &envelope::issuer_aad(&root_fp, issuer_id),
                    issuer_key.as_bytes(),
                )?,
                status: IssuerStatus::Active,
            },
        )]),
        token_signer: None,
        seal: SealConfig {
            cluster_id,
            slots: vec![slot],
            epoch: 0,
        },
        certs: BTreeMap::new(),
        revoked_certs: BTreeMap::new(),
        revoked_tokens: BTreeMap::new(),
        enrollments: BTreeMap::new(),
        profiles: BTreeMap::new(),
        crl: None,
        generation: 0,
    };
    // Everything after the anchor goes in as the ops the daemon would
    // make, through the same checks.
    let mut state: Option<Arc<PkiState>> = None;
    // At seal epoch 0, as the staged CA's signer makes them.
    let mut apply = |op: PkiOp| pki::apply(&mut state, &op.at_epoch(0), now, &X509Checker);
    apply(PkiOp::Bootstrap(Box::new(base)))?;
    if let Some(days) = opts.node_days {
        let spec = ProfileKind::Node.spec();
        apply(PkiOp::SetProfile {
            kind: ProfileKind::Node,
            params: ProfileParams {
                default_days: days,
                max_days: days.max(spec.max_days).min(spec.hard_max_days),
            },
        })?;
    }
    let signer = Signer::new(issuer_id, &issuer_pem, &issuer_key)?;
    let issue = |profile: ProfileKind,
                 cn: &str,
                 sans: Vec<String>,
                 member: Option<NodeId>,
                 state: &mut dyn FnMut(PkiOp) -> Result<(), String>,
                 days: u32|
     -> Result<Bundle, String> {
        let (key_pem, csr) = ca::generate_csr(cn)?;
        let identity = Identity {
            profile,
            cn: cn.to_string(),
            sans,
            member,
        };
        let issued = signer.sign(
            &csr,
            &LeafSpec {
                profile,
                cn: identity.cn.clone(),
                sans: identity.sans.clone(),
                days,
                life_s: None,
            },
        )?;
        state(PkiOp::RecordIssued {
            serial: issued.serial.clone(),
            cert: IssuedCert {
                identity,
                issuer: issuer_id,
                not_before_ms: issued.not_before_ms,
                not_after_ms: issued.not_after_ms,
                renews: None,
            },
        })?;
        Ok(Bundle {
            ca_pem: root_pem.clone(),
            cert_pem: signer.chain(&issued.cert_pem),
            key_pem,
        })
    };
    let node_days = opts
        .node_days
        .unwrap_or(ProfileKind::Node.spec().default_days);
    let node0_sans = node_sans(&opts.sans);
    let node0 = issue(
        ProfileKind::Node,
        &opts.sans[0].trim().to_ascii_lowercase(),
        node0_sans.clone(),
        Some(opts.members.me),
        &mut apply,
        node_days,
    )?;
    let admin = issue(
        ProfileKind::Admin,
        &opts.admin_name,
        Vec::new(),
        None,
        &mut apply,
        ProfileKind::Admin.spec().default_days,
    )?;
    let mut tokens = Vec::new();
    for (member, base) in &opts.members.peers {
        url_host(base).ok_or_else(|| format!("{}: no host in {}", member, base))?;
        let token = JoinToken::generate(root_fp)?;
        let e = node_enrollment(
            &dek,
            &token,
            NodeScope {
                fingerprint: opts.members.fingerprint.clone(),
                member: *member,
                extra_sans: opts.peer_sans.get(member).cloned().unwrap_or_default(),
                replace: false,
                compromised: false,
            },
            now + opts.token_ttl_ms,
            &opts.admin_name,
        );
        apply(PkiOp::CreateEnrollment {
            id: token.id.clone(),
            enrollment: Box::new(e),
        })?;
        tokens.push(PeerToken {
            member: *member,
            base: base.clone(),
            token: token.encode(),
        });
    }
    if let Some(m) = opts
        .peer_sans
        .keys()
        .find(|m| !opts.members.peers.contains_key(m))
    {
        return Err(format!("--peer-san: {} is not a peer in this pool", m));
    }
    let state = state.expect("bootstrapped");

    std::fs::create_dir_all(opts.data_dir)
        .map_err(|e| format!("{}: {}", opts.data_dir.display(), e))?;
    raidish_cluster::store::restrict(opts.data_dir)
        .map_err(|e| format!("{}: {}", opts.data_dir.display(), e))?;
    write_staged(opts.data_dir, &state)?;
    let once = unseal_once_path(opts.data_dir);
    if opts.unseal_once {
        crate::write_file(&once, hex::encode(*dek).as_bytes(), 0o600)?;
    } else {
        let _ = std::fs::remove_file(&once);
    }
    Ok(InitOutput {
        root_fingerprint: root_fp,
        recovery_key,
        root_key,
        root_envelope,
        node0,
        node0_sans,
        admin,
        tokens,
    })
}

/// The production Argon2id cost.
pub const KDF: (u32, u32, u32) = (envelope::KDF_M_KIB, envelope::KDF_T, envelope::KDF_P);
