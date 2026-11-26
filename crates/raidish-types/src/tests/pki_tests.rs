//! The CA state machine (`crate::pki`): every op's precondition and
//! effect, pruning, replay determinism and the size budget. Certificates
//! are fakes: `FakeChecker` knows which "PEM" is a CA signed by which.

use super::*;
use crate::types::NodeId;
use std::collections::HashMap;

const DAY: u64 = DAY_MS;
const T0: u64 = 1_700_000_000_000;

/// A fake CA certificate: its facts, its parent (None: self-signed) and
/// path length.
struct Fake {
    facts: CertFacts,
    parent: Option<String>,
    path_len: u8,
}

#[derive(Default)]
struct FakeChecker(HashMap<String, Fake>);

impl FakeChecker {
    fn add(
        &mut self,
        pem: &str,
        serial: u8,
        parent: Option<&str>,
        path_len: u8,
        not_after_ms: u64,
    ) {
        self.0.insert(
            pem.to_string(),
            Fake {
                facts: CertFacts {
                    serial: Serial::from_be_bytes(&[serial]),
                    ski: KeyId(vec![serial]),
                    not_before_ms: T0 - DAY,
                    not_after_ms,
                    fingerprint: CertFingerprint([serial; 32]),
                },
                parent: parent.map(str::to_string),
                path_len,
            },
        );
    }

    fn get(&self, pem: &str, at_ms: u64) -> Result<&Fake, String> {
        let f = self.0.get(pem).ok_or("not a certificate")?;
        if at_ms < f.facts.not_before_ms || at_ms >= f.facts.not_after_ms {
            return Err("not valid at that time".into());
        }
        Ok(f)
    }
}

impl CertChecker for FakeChecker {
    fn anchor(&self, pem: &str, at_ms: u64) -> Result<CertFacts, String> {
        let f = self.get(pem, at_ms)?;
        match (&f.parent, f.path_len) {
            (None, 1) => Ok(f.facts.clone()),
            _ => Err("not a root".into()),
        }
    }
    fn ca(&self, pem: &str, parent: &str, path_len: u8, at_ms: u64) -> Result<CertFacts, String> {
        let f = self.get(pem, at_ms)?;
        if f.parent.as_deref() != Some(parent) || f.path_len != path_len {
            return Err("not signed by that CA".into());
        }
        Ok(f.facts.clone())
    }
}

fn checker() -> FakeChecker {
    let mut c = FakeChecker::default();
    c.add("ROOT", 1, None, 1, T0 + 7300 * DAY);
    c.add("ISSUER1", 2, Some("ROOT"), 0, T0 + 730 * DAY);
    c.add("ISSUER2", 3, Some("ROOT"), 0, T0 + 1460 * DAY);
    c.add("FOREIGN-ROOT", 9, None, 1, T0 + 7300 * DAY);
    c.add("FOREIGN", 10, Some("FOREIGN-ROOT"), 0, T0 + 730 * DAY);
    c
}

fn blob() -> SealedBlob {
    SealedBlob {
        v: 1,
        nonce: [0; 12],
        ct: vec![1, 2, 3],
    }
}

fn issuer(pem: &str, serial: u8, not_after_ms: u64) -> IssuerRecord {
    IssuerRecord {
        cert_pem: pem.into(),
        serial: Serial::from_be_bytes(&[serial]),
        ski: KeyId(vec![serial]),
        not_after_ms,
        sealed_key: blob(),
        status: IssuerStatus::Active,
    }
}

fn fresh() -> PkiState {
    PkiState {
        anchor: TrustAnchor {
            cert_pem: "ROOT".into(),
            fingerprint: CertFingerprint([1; 32]),
            sealed_key: None,
        },
        issuers: BTreeMap::from([(IssuerId(1), issuer("ISSUER1", 2, T0 + 730 * DAY))]),
        token_signer: None,
        seal: SealConfig {
            cluster_id: [7; 16],
            slots: vec![KeySlot {
                id: "recovery".into(),
                label: "recovery key".into(),
                kind: SlotKind::RecoveryKey(KdfParams {
                    salt: [0; 16],
                    m_kib: 8,
                    t: 1,
                    p: 1,
                }),
                wrapped_dek: blob(),
            }],
            epoch: 0,
        },
        certs: BTreeMap::new(),
        revoked_certs: BTreeMap::new(),
        revoked_tokens: BTreeMap::new(),
        enrollments: BTreeMap::new(),
        profiles: BTreeMap::new(),
        generation: 0,
    }
}

fn installed() -> Option<Arc<PkiState>> {
    let mut pki = None;
    apply(
        &mut pki,
        &PkiOp::Bootstrap(Box::new(fresh())),
        T0,
        &checker(),
    )
    .unwrap();
    pki
}

fn run(pki: &mut Option<Arc<PkiState>>, op: PkiOp, now: u64) -> Result<(), String> {
    apply(pki, &op, now, &checker())
}

fn m(n: u64) -> NodeId {
    NodeId::from_raw(n)
}

fn node_cert(member: u64, not_after_ms: u64) -> IssuedCert {
    IssuedCert {
        identity: Identity {
            profile: ProfileKind::Node,
            cn: format!("node{}", member),
            sans: vec![format!("node{}", member)],
            member: Some(m(member)),
        },
        issuer: IssuerId(1),
        not_before_ms: T0,
        not_after_ms,
        renews: None,
    }
}

fn record(pki: &mut Option<Arc<PkiState>>, serial: u8, cert: IssuedCert) {
    run(
        pki,
        PkiOp::RecordIssued {
            serial: Serial::from_be_bytes(&[serial]),
            cert,
        },
        T0,
    )
    .unwrap();
}

fn tid(n: u8) -> TokenId {
    TokenId::from_bytes([n; 8])
}

fn node_enrollment(member: Option<u64>, replace: bool, uses: u32) -> Enrollment {
    Enrollment {
        scope: EnrollScope::Node {
            fingerprint: "fp".into(),
            member: member.map(m),
            extra_sans: vec![],
            replace,
            compromised: false,
        },
        secret_sha256: [5; 32],
        uses,
        uses_left: uses,
        expires_ms: T0 + DAY,
        created_by: "admin".into(),
        mac: [0; 32],
    }
}

fn enroll(pki: &mut Option<Arc<PkiState>>, id: u8, e: Enrollment) {
    run(
        pki,
        PkiOp::CreateEnrollment {
            id: tid(id),
            enrollment: Box::new(e),
        },
        T0,
    )
    .unwrap();
}

fn consume(pki: &mut Option<Arc<PkiState>>, id: u8, member: Option<u64>) -> Result<(), String> {
    run(
        pki,
        PkiOp::ConsumeEnrollment {
            id: tid(id),
            secret_sha256: [5; 32],
            member: member.map(m),
        },
        T0 + 1,
    )
}

// ---- identifiers ---------------------------------------------------------

#[test]
fn serials_compare_equal_whatever_their_padding() {
    // rcgen's bytes, x509-parser's DER integer with the sign byte, and a
    // leading zero all name the same serial.
    let a = Serial::from_be_bytes(&[0x80, 0x01]);
    let b = Serial::from_be_bytes(&[0x00, 0x80, 0x01]);
    let c = Serial::parse("80:01").unwrap();
    assert_eq!(a, b);
    assert_eq!(a, c);
    assert_eq!(a.as_str(), "8001");
    assert_eq!(Serial::from_be_bytes(&[0, 0]).as_str(), "00");
    assert_eq!(Serial::parse("ABC").unwrap().as_str(), "0abc");
}

#[test]
fn token_ids_are_sixteen_hex_digits() {
    assert!(TokenId::parse("0123456789abcdef").is_ok());
    assert!(TokenId::parse("0123456789ABCDEF").is_err());
    assert!(TokenId::parse("abc").is_err());
}

#[test]
fn state_round_trips_through_json() {
    let pki = installed().unwrap();
    let json = serde_json::to_string(&*pki).unwrap();
    let back: PkiState = serde_json::from_str(&json).unwrap();
    assert_eq!(*pki, back);
    // Byte strings are hex, not arrays of numbers.
    assert!(json.contains("\"nonce\":\"000000000000000000000000\""));
}

#[test]
fn the_mac_covers_every_field_but_uses_left() {
    let e = node_enrollment(Some(1), false, 2);
    let base = e.mac_input(&tid(1));
    assert_ne!(base, e.mac_input(&tid(2)));
    let mut used = e.clone();
    used.uses_left = 1;
    assert_eq!(base, used.mac_input(&tid(1)));
    for change in [
        |e: &mut Enrollment| e.uses = 3,
        |e: &mut Enrollment| e.expires_ms += 1,
        |e: &mut Enrollment| e.created_by.push('x'),
        |e: &mut Enrollment| e.secret_sha256[0] ^= 1,
        |e: &mut Enrollment| {
            if let EnrollScope::Node { replace, .. } = &mut e.scope {
                *replace = true
            }
        },
        |e: &mut Enrollment| {
            if let EnrollScope::Node { extra_sans, .. } = &mut e.scope {
                extra_sans.push("10.0.0.1".into())
            }
        },
    ] {
        let mut other = e.clone();
        change(&mut other);
        assert_ne!(base, other.mac_input(&tid(1)));
    }
}

// ---- Bootstrap -----------------------------------------------------------

#[test]
fn bootstrap_installs_once() {
    let mut pki = installed();
    assert!(pki.is_some());
    let again = run(&mut pki, PkiOp::Bootstrap(Box::new(fresh())), T0);
    assert!(again.unwrap_err().contains("already"));
}

#[test]
fn ops_before_bootstrap_are_refused() {
    let mut pki = None;
    let e = run(&mut pki, PkiOp::Seal, T0).unwrap_err();
    assert!(e.contains("ca init"), "{}", e);
}

#[test]
fn bootstrap_checks_the_anchor_and_issuers() {
    let mut bad_root = fresh();
    bad_root.anchor.cert_pem = "ISSUER1".into(); // not self-signed
    let mut foreign = fresh();
    foreign.issuers = BTreeMap::from([(IssuerId(1), issuer("FOREIGN", 10, T0 + 730 * DAY))]);
    let mut fingerprint = fresh();
    fingerprint.anchor.fingerprint = CertFingerprint([2; 32]);
    let mut none_active = fresh();
    none_active.issuers.get_mut(&IssuerId(1)).unwrap().status = IssuerStatus::Retired;
    let mut lying = fresh();
    lying.issuers.get_mut(&IssuerId(1)).unwrap().not_after_ms += 1;
    let mut signer = fresh();
    signer.token_signer = Some(TokenSignerRecord {
        chain_pem: "X".into(),
        serial: Serial::from_be_bytes(&[4]),
        not_after_ms: T0 + DAY,
        sealed_key: blob(),
    });
    for (what, s) in [
        ("not a root", bad_root),
        ("foreign issuer", foreign),
        ("wrong fingerprint", fingerprint),
        ("no active issuer", none_active),
        ("record lies about its cert", lying),
        ("token signer", signer),
    ] {
        let mut pki = None;
        assert!(
            run(&mut pki, PkiOp::Bootstrap(Box::new(s)), T0).is_err(),
            "{} accepted",
            what
        );
        assert!(pki.is_none());
    }
}

#[test]
fn nothing_is_installed_without_a_checker() {
    let mut pki = None;
    let r = apply(
        &mut pki,
        &PkiOp::Bootstrap(Box::new(fresh())),
        T0,
        &NoCertChecker,
    );
    assert!(r.is_err());
}

// ---- issuers ---------------------------------------------------------------

#[test]
fn add_issuer_must_chain_to_the_anchor() {
    let mut pki = installed();
    let foreign = PkiOp::AddIssuer {
        id: IssuerId(2),
        record: Box::new(issuer("FOREIGN", 10, T0 + 730 * DAY)),
        activate: true,
    };
    assert!(run(&mut pki, foreign, T0).is_err());
    let good = PkiOp::AddIssuer {
        id: IssuerId(2),
        record: Box::new(issuer("ISSUER2", 3, T0 + 1460 * DAY)),
        activate: true,
    };
    run(&mut pki, good.clone(), T0).unwrap();
    let s = pki.as_deref().unwrap();
    assert_eq!(s.active_issuer().unwrap().0, IssuerId(2));
    assert_eq!(s.issuers[&IssuerId(1)].status, IssuerStatus::Retired);
    // The id is taken now.
    assert!(run(&mut pki, good, T0).is_err());
}

#[test]
fn chain_checks_use_the_entrys_time() {
    // Replaying an old log after the issuer expired gives the same state:
    // the check runs at the stamped time, not now.
    let op = PkiOp::AddIssuer {
        id: IssuerId(2),
        record: Box::new(issuer("ISSUER2", 3, T0 + 1460 * DAY)),
        activate: true,
    };
    let mut first = installed();
    run(&mut first, op.clone(), T0 + DAY).unwrap();
    let mut replayed = installed();
    run(&mut replayed, op.clone(), T0 + DAY).unwrap();
    assert_eq!(first, replayed);
    // Stamped after it expired, it's refused (on every replica alike).
    let mut late = installed();
    assert!(run(&mut late, op, T0 + 1500 * DAY).is_err());
}

#[test]
fn only_a_retired_issuer_can_be_revoked() {
    let mut pki = installed();
    assert!(run(&mut pki, PkiOp::RevokeIssuer { id: IssuerId(1) }, T0).is_err());
    run(
        &mut pki,
        PkiOp::AddIssuer {
            id: IssuerId(2),
            record: Box::new(issuer("ISSUER2", 3, T0 + 1460 * DAY)),
            activate: true,
        },
        T0,
    )
    .unwrap();
    run(&mut pki, PkiOp::RevokeIssuer { id: IssuerId(1) }, T0 + 5).unwrap();
    assert_eq!(
        pki.as_deref().unwrap().issuers[&IssuerId(1)].status,
        IssuerStatus::Revoked { at_ms: T0 + 5 }
    );
}

// ---- issued certificates -------------------------------------------------

#[test]
fn record_issued_checks_issuer_and_lifetime() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    // Retried with the same record: fine. Another record: refused.
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    let taken = PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[20]),
        cert: node_cert(2, T0 + 30 * DAY),
    };
    assert!(run(&mut pki, taken, T0).is_err());
    // Longer than the node profile's 90 days.
    let long = PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[21]),
        cert: node_cert(1, T0 + 200 * DAY),
    };
    assert!(run(&mut pki, long, T0).is_err());
    // Past the issuer.
    let mut admin = node_cert(1, T0 + 731 * DAY);
    admin.identity.profile = ProfileKind::Admin;
    admin.not_before_ms = T0 + 400 * DAY;
    let outlives = PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[22]),
        cert: admin,
    };
    assert!(run(&mut pki, outlives, T0).is_err());
    // Unknown issuer.
    let mut orphan = node_cert(1, T0 + 30 * DAY);
    orphan.issuer = IssuerId(9);
    let orphan = PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[23]),
        cert: orphan,
    };
    assert!(run(&mut pki, orphan, T0).is_err());
}

#[test]
fn profile_overrides_are_capped_in_code() {
    let mut pki = installed();
    let set = |default_days, max_days| PkiOp::SetProfile {
        kind: ProfileKind::Node,
        params: ProfileParams {
            default_days,
            max_days,
        },
    };
    assert!(run(&mut pki, set(400, 400), T0).is_err()); // past the hard cap
    assert!(run(&mut pki, set(60, 30), T0).is_err()); // default > max
    assert!(run(&mut pki, set(0, 30), T0).is_err());
    run(&mut pki, set(365, 365), T0).unwrap();
    record(&mut pki, 20, node_cert(1, T0 + 365 * DAY));
}

#[test]
fn revoking_records_intent_until_expiry() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    let serial = Serial::from_be_bytes(&[20]);
    let revoke = PkiOp::RevokeCert {
        serial: serial.clone(),
        reason: RevocationReason::KeyCompromise,
    };
    run(&mut pki, revoke.clone(), T0 + 10).unwrap();
    // Again: still the first revocation.
    run(&mut pki, revoke, T0 + 20).unwrap();
    let s = pki.as_deref().unwrap();
    let r = &s.revoked_certs[&serial];
    assert_eq!((r.at_ms, r.expires_ms), (T0 + 10, Some(T0 + 30 * DAY)));
    assert!(!s.is_live(&serial, T0 + 30));
    let unknown = PkiOp::RevokeCert {
        serial: Serial::from_be_bytes(&[99]),
        reason: RevocationReason::Unspecified,
    };
    assert!(run(&mut pki, unknown, T0).is_err());
}

// ---- enrollments ---------------------------------------------------------

#[test]
fn create_enrollment_checks() {
    let mut pki = installed();
    let mut expired = node_enrollment(None, false, 1);
    expired.expires_ms = T0;
    let mut partial = node_enrollment(None, false, 2);
    partial.uses_left = 1;
    let mut signer = node_enrollment(None, false, 1);
    signer.scope = EnrollScope::Client {
        profile: ProfileKind::TokenSigner,
        cn: "x".into(),
    };
    for e in [expired, partial, signer, node_enrollment(None, false, 0)] {
        let op = PkiOp::CreateEnrollment {
            id: tid(1),
            enrollment: Box::new(e),
        };
        assert!(run(&mut pki, op, T0).is_err());
    }
    enroll(&mut pki, 1, node_enrollment(None, false, 1));
    let dup = PkiOp::CreateEnrollment {
        id: tid(1),
        enrollment: Box::new(node_enrollment(None, false, 1)),
    };
    assert!(run(&mut pki, dup, T0).is_err());
}

#[test]
fn consuming_counts_down_and_removes() {
    let mut pki = installed();
    enroll(&mut pki, 1, node_enrollment(None, false, 2));
    consume(&mut pki, 1, Some(1)).unwrap();
    assert_eq!(pki.as_deref().unwrap().enrollments[&tid(1)].uses_left, 1);
    consume(&mut pki, 1, Some(2)).unwrap();
    assert!(pki.as_deref().unwrap().enrollments.is_empty());
    assert!(consume(&mut pki, 1, Some(3)).is_err());
}

#[test]
fn a_refused_consume_changes_nothing() {
    let mut pki = installed();
    enroll(&mut pki, 1, node_enrollment(Some(1), false, 1));
    let before = pki.clone();
    let wrong_secret = PkiOp::ConsumeEnrollment {
        id: tid(1),
        secret_sha256: [6; 32],
        member: Some(m(1)),
    };
    assert!(run(&mut pki, wrong_secret, T0).is_err());
    assert!(consume(&mut pki, 1, Some(2)).is_err()); // another member
    assert!(consume(&mut pki, 1, None).is_err()); // no member
    assert_eq!(pki, before);
    consume(&mut pki, 1, Some(1)).unwrap();
}

#[test]
fn an_occupied_member_needs_replace() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    enroll(&mut pki, 1, node_enrollment(None, false, 1));
    let e = consume(&mut pki, 1, Some(1)).unwrap_err();
    assert!(e.contains("--replace"), "{}", e);
    // The token survived the refusal.
    assert_eq!(pki.as_deref().unwrap().enrollments[&tid(1)].uses_left, 1);
    // Another member is free.
    consume(&mut pki, 1, Some(2)).unwrap();
}

#[test]
fn replace_revokes_the_members_old_certs_in_the_same_entry() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    let mut libvirt = node_cert(1, T0 + 300 * DAY);
    libvirt.identity.profile = ProfileKind::Libvirt;
    record(&mut pki, 21, libvirt);
    record(&mut pki, 22, node_cert(2, T0 + 30 * DAY));
    enroll(&mut pki, 1, node_enrollment(Some(1), true, 1));
    consume(&mut pki, 1, Some(1)).unwrap();
    let s = pki.as_deref().unwrap();
    for serial in [20, 21] {
        let r = &s.revoked_certs[&Serial::from_be_bytes(&[serial])];
        assert_eq!(r.reason, RevocationReason::Superseded);
    }
    assert!(s.is_live(&Serial::from_be_bytes(&[22]), T0 + 2));
}

#[test]
fn compromised_replacements_say_so() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    let mut e = node_enrollment(Some(1), true, 1);
    if let EnrollScope::Node { compromised, .. } = &mut e.scope {
        *compromised = true;
    }
    enroll(&mut pki, 1, e);
    consume(&mut pki, 1, Some(1)).unwrap();
    let s = pki.as_deref().unwrap();
    assert_eq!(
        s.revoked_certs[&Serial::from_be_bytes(&[20])].reason,
        RevocationReason::KeyCompromise
    );
}

#[test]
fn two_joins_for_one_member_one_wins() {
    let mut pki = installed();
    enroll(&mut pki, 1, node_enrollment(None, false, 1));
    enroll(&mut pki, 2, node_enrollment(None, false, 1));
    consume(&mut pki, 1, Some(1)).unwrap();
    // The first signer records its cert before the second consume lands.
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    assert!(consume(&mut pki, 2, Some(1)).is_err());
}

// ---- seal and profiles -----------------------------------------------------

#[test]
fn seal_config_keeps_its_cluster_and_moves_forward() {
    let mut pki = installed();
    let mut cfg = pki.as_deref().unwrap().seal.clone();
    cfg.epoch = 3;
    run(&mut pki, PkiOp::SetSeal(cfg.clone()), T0).unwrap();
    run(&mut pki, PkiOp::Seal, T0).unwrap();
    assert_eq!(pki.as_deref().unwrap().seal.epoch, 4);
    cfg.epoch = 2;
    assert!(run(&mut pki, PkiOp::SetSeal(cfg.clone()), T0).is_err());
    cfg.epoch = 5;
    cfg.cluster_id = [8; 16];
    assert!(run(&mut pki, PkiOp::SetSeal(cfg.clone()), T0).is_err());
    cfg.cluster_id = [7; 16];
    cfg.slots.clear();
    assert!(run(&mut pki, PkiOp::SetSeal(cfg), T0).is_err());
}

// ---- pruning -------------------------------------------------------------------

#[test]
fn pruning_forgets_what_expired() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    record(&mut pki, 21, node_cert(2, T0 + 30 * DAY));
    let revoke = PkiOp::RevokeCert {
        serial: Serial::from_be_bytes(&[20]),
        reason: RevocationReason::Superseded,
    };
    run(&mut pki, revoke, T0).unwrap();
    run(
        &mut pki,
        PkiOp::RevokeToken {
            id: tid(3),
            expires_ms: None,
        },
        T0,
    )
    .unwrap();
    enroll(&mut pki, 1, node_enrollment(None, false, 1));
    // Two days in: the enrollment is gone, certs stay.
    run(&mut pki, PkiOp::Seal, T0 + 2 * DAY).unwrap();
    let s = pki.as_deref().unwrap();
    assert!(s.enrollments.is_empty());
    assert_eq!(s.certs.len(), 2);
    // A day past their expiry: certs and their revocation go; a token
    // revocation that never expires stays.
    run(&mut pki, PkiOp::Seal, T0 + 32 * DAY).unwrap();
    let s = pki.as_deref().unwrap();
    assert!(s.certs.is_empty() && s.revoked_certs.is_empty());
    assert_eq!(s.revoked_tokens.len(), 1);
}

#[test]
fn revoked_issuers_stay_until_they_expire() {
    let mut pki = installed();
    run(
        &mut pki,
        PkiOp::AddIssuer {
            id: IssuerId(2),
            record: Box::new(issuer("ISSUER2", 3, T0 + 1460 * DAY)),
            activate: true,
        },
        T0,
    )
    .unwrap();
    run(&mut pki, PkiOp::RevokeIssuer { id: IssuerId(1) }, T0).unwrap();
    run(&mut pki, PkiOp::Seal, T0 + 700 * DAY).unwrap();
    assert!(pki.as_deref().unwrap().issuers.contains_key(&IssuerId(1)));
    run(&mut pki, PkiOp::Seal, T0 + 732 * DAY).unwrap();
    let s = pki.as_deref().unwrap();
    assert!(!s.issuers.contains_key(&IssuerId(1)));
    assert!(s.issuers.contains_key(&IssuerId(2)));
}

// ---- the read side, and size -----------------------------------------------

#[test]
fn the_view_leaves_secrets_out() {
    let mut pki = installed();
    enroll(&mut pki, 1, node_enrollment(None, false, 1));
    let view = pki.as_deref().unwrap().view();
    let json = serde_json::to_string(&view).unwrap();
    assert!(!json.contains("wrapped_dek") && !json.contains("sealed_key"));
    assert!(!json.contains("secret_sha256") && !json.contains("mac"));
    assert_eq!(view.enrollments, 1);
    assert_eq!(view.slots[0].kind, "recovery-key");
}

#[test]
fn a_year_of_renewals_stays_small() {
    // Three nodes renewing every 20 days and a dozen other certs renewed
    // yearly, with pruning: the state `save_sm` rewrites on every apply
    // stays well under 64 KiB.
    let mut pki = installed();
    run(
        &mut pki,
        PkiOp::SetProfile {
            kind: ProfileKind::Libvirt,
            params: ProfileParams {
                default_days: 365,
                max_days: 365,
            },
        },
        T0,
    )
    .unwrap();
    let mut serial: u32 = 1000;
    let mut next = || {
        serial += 1;
        Serial::from_be_bytes(&serial.to_be_bytes())
    };
    for i in 0..12u64 {
        let mut c = node_cert(10 + i, T0 + 364 * DAY);
        c.identity.profile = ProfileKind::Libvirt;
        c.identity.sans = vec![
            "a-long-host-name.example.internal".into(),
            "10.0.0.1".into(),
        ];
        run(
            &mut pki,
            PkiOp::RecordIssued {
                serial: next(),
                cert: c,
            },
            T0,
        )
        .unwrap();
    }
    let mut day = 0;
    while day < 365 {
        let now = T0 + day * DAY;
        for n in 0..3 {
            let mut c = node_cert(n, now + 30 * DAY);
            c.not_before_ms = now;
            c.identity.sans = vec![
                format!("node{}", n),
                "10.132.132.180".into(),
                "localhost".into(),
                "127.0.0.1".into(),
            ];
            run(
                &mut pki,
                PkiOp::RecordIssued {
                    serial: next(),
                    cert: c,
                },
                now,
            )
            .unwrap();
        }
        day += 20;
    }
    let size = serde_json::to_vec(&*pki.unwrap()).unwrap().len();
    assert!(size < 64 * 1024, "{} bytes", size);
}
