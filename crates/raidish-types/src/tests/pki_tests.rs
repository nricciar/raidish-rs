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
    /// A fake token signer is `SIGNER:<serial>|<issuer>`; one whose
    /// serial is 66 has the wrong EKU.
    fn token_signer(&self, chain: &str, issuer: &str, _at: u64) -> Result<CertFacts, String> {
        let rest = chain.strip_prefix("SIGNER:").ok_or("not a token signer")?;
        let (serial, by) = rest.split_once('|').ok_or("not a chain")?;
        if by != issuer {
            return Err("not signed by that CA".into());
        }
        let serial: u8 = serial.parse().map_err(|_| "bad serial")?;
        if serial == 66 {
            return Err("its EKU isn't only codeSigning".into());
        }
        Ok(CertFacts {
            serial: Serial::from_be_bytes(&[serial]),
            ski: KeyId(vec![serial]),
            not_before_ms: T0,
            not_after_ms: T0 + 730 * DAY,
            fingerprint: CertFingerprint([serial; 32]),
        })
    }
    /// A fake CRL is `<issuer>|<number>|<this>|<next>`.
    fn crl(&self, der: &[u8], issuer_pem: &str) -> Result<CrlFacts, String> {
        let s = std::str::from_utf8(der).map_err(|e| e.to_string())?;
        let f: Vec<&str> = s.split('|').collect();
        let [by, n, this, next] = f[..] else {
            return Err("not a CRL".into());
        };
        if by != issuer_pem {
            return Err("not signed by that CA".into());
        }
        let num = |x: &str| x.parse::<u64>().map_err(|e| e.to_string());
        Ok(CrlFacts {
            number: num(n)?,
            this_update_ms: num(this)?,
            next_update_ms: num(next)?,
        })
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
        crl: None,
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

/// Apply `op`, made (if it takes the DEK) at the CA's current seal epoch,
/// as an unsealed node makes it.
fn run(pki: &mut Option<Arc<PkiState>>, op: PkiOp, now: u64) -> Result<(), String> {
    let epoch = pki.as_deref().map_or(0, |p| p.seal.epoch);
    apply(pki, &op.at_epoch(epoch), now, &checker())
}

fn seal() -> PkiOp {
    PkiOp::Seal {
        remove_slots: vec![],
    }
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
    let e = run(&mut pki, seal(), T0).unwrap_err();
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

fn slot(id: &str) -> PkiOp {
    PkiOp::AddSlot(Box::new(KeySlot {
        id: id.into(),
        label: "x".into(),
        kind: SlotKind::Passphrase(KdfParams {
            salt: [0; 16],
            m_kib: 8,
            t: 1,
            p: 1,
        }),
        wrapped_dek: blob(),
    }))
}

#[test]
fn slots_come_and_go_but_never_the_last() {
    let mut pki = installed();
    let first = pki.as_deref().unwrap().seal.slots[0].id.clone();
    run(&mut pki, slot("pass-1"), T0).unwrap();
    assert!(
        run(&mut pki, slot("pass-1"), T0).is_err(),
        "the id is taken"
    );
    assert!(run(&mut pki, slot(""), T0).is_err());
    // Neither moves the epoch: whoever is unsealed stays so.
    assert_eq!(pki.as_deref().unwrap().seal.epoch, 0);
    run(&mut pki, PkiOp::RemoveSlot { id: first }, T0).unwrap();
    let last = PkiOp::RemoveSlot {
        id: "pass-1".into(),
    };
    assert!(run(&mut pki, last, T0).unwrap_err().contains("last"));
    assert!(run(&mut pki, PkiOp::RemoveSlot { id: "nope".into() }, T0).is_err());
    for i in 0..(MAX_SLOTS - 1) {
        run(&mut pki, slot(&format!("s{}", i)), T0).unwrap();
    }
    assert!(
        run(&mut pki, slot("one-more"), T0).is_err(),
        "at most {}",
        MAX_SLOTS
    );
}

#[test]
fn sealing_moves_the_epoch_and_takes_the_slots_it_names() {
    let mut pki = installed();
    run(&mut pki, slot("node-key-1"), T0).unwrap();
    let only = |ids: &[&str]| PkiOp::Seal {
        remove_slots: ids.iter().map(|s| s.to_string()).collect(),
    };
    assert!(run(&mut pki, only(&["nope"]), T0).is_err());
    let all: Vec<String> = pki
        .as_deref()
        .unwrap()
        .seal
        .slots
        .iter()
        .map(|s| s.id.clone())
        .collect();
    let every = PkiOp::Seal { remove_slots: all };
    assert!(run(&mut pki, every, T0).unwrap_err().contains("every"));
    assert_eq!(
        pki.as_deref().unwrap().seal.epoch,
        0,
        "refused: nothing changed"
    );
    run(&mut pki, only(&["node-key-1"]), T0).unwrap();
    let s = pki.as_deref().unwrap();
    assert_eq!(s.seal.epoch, 1);
    assert!(s.seal.slots.iter().all(|x| x.id != "node-key-1"));
}

#[test]
fn what_the_dek_made_carries_the_epoch_it_was_made_at() {
    let mut pki = installed();
    let cert = || PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[70]),
        cert: node_cert(1, T0 + 30 * DAY),
    };
    // Bare: refused.
    let e = apply(&mut pki, &cert(), T0, &checker()).unwrap_err();
    assert!(e.contains("seal epoch"), "{}", e);
    // Made before a `Seal` it hadn't applied: refused.
    run(&mut pki, seal(), T0).unwrap();
    let stale = cert().at_epoch(0);
    let e = apply(&mut pki, &stale, T0, &checker()).unwrap_err();
    assert!(e.contains("was sealed"), "{}", e);
    // At the CA's epoch: recorded.
    apply(&mut pki, &cert().at_epoch(1), T0, &checker()).unwrap();
    // What doesn't take the DEK isn't wrapped, and can't be.
    let wrapped = PkiOp::AtEpoch {
        seal_epoch: 1,
        op: Box::new(PkiOp::RevokeToken {
            id: tid(1),
            expires_ms: None,
        }),
    };
    assert!(apply(&mut pki, &wrapped, T0, &checker()).is_err());
    assert_eq!(seal().at_epoch(1), seal());
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
    run(&mut pki, seal(), T0 + 2 * DAY).unwrap();
    let s = pki.as_deref().unwrap();
    assert!(s.enrollments.is_empty());
    assert_eq!(s.certs.len(), 2);
    // A day past their expiry: certs and their revocation go; a token
    // revocation that never expires stays.
    run(&mut pki, seal(), T0 + 32 * DAY).unwrap();
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
    run(&mut pki, seal(), T0 + 700 * DAY).unwrap();
    assert!(pki.as_deref().unwrap().issuers.contains_key(&IssuerId(1)));
    run(&mut pki, seal(), T0 + 732 * DAY).unwrap();
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
    // And a CRL listing a few dozen serials (about 4 KB of DER).
    let mut state = (*pki.unwrap()).clone();
    state.crl = Some(CrlRecord {
        issuer: IssuerId(1),
        der: vec![0xab; 4096],
        number: 999,
        this_update_ms: T0,
        next_update_ms: T0 + 30 * DAY,
        generation: state.generation,
    });
    let size = serde_json::to_vec(&state).unwrap().len();
    assert!(size < 64 * 1024, "{} bytes", size);
}

// ---- the CRL -----------------------------------------------------------------

fn crl(issuer: &str, id: u32, number: u64, this: u64, next: u64, generation: u64) -> PkiOp {
    PkiOp::SetCrl(Box::new(CrlRecord {
        issuer: IssuerId(id),
        der: format!("{}|{}|{}|{}", issuer, number, this, next).into_bytes(),
        number,
        this_update_ms: this,
        next_update_ms: next,
        generation,
    }))
}

#[test]
fn a_crl_is_the_active_issuers_and_only_moves_forward() {
    let mut pki = installed();
    let generation = pki.as_deref().unwrap().generation;
    run(
        &mut pki,
        crl("ISSUER1", 1, 1, T0, T0 + 30 * DAY, generation),
        T0,
    )
    .unwrap();
    let p = pki.as_deref().unwrap();
    assert_eq!(p.crl.as_ref().unwrap().number, 1);
    assert_eq!(p.generation, generation, "verifiers don't read it");
    // Not newer.
    assert!(
        run(
            &mut pki,
            crl("ISSUER1", 1, 1, T0, T0 + 30 * DAY, generation),
            T0
        )
        .is_err()
    );
    run(
        &mut pki,
        crl("ISSUER1", 1, 2, T0 + 1, T0 + 30 * DAY, generation),
        T0 + 1,
    )
    .unwrap();
    // Signed by another CA, or said to be by another issuer.
    assert!(
        run(
            &mut pki,
            crl("FOREIGN", 1, 3, T0, T0 + 30 * DAY, generation),
            T0
        )
        .is_err()
    );
    assert!(
        run(
            &mut pki,
            crl("ISSUER1", 2, 3, T0, T0 + 30 * DAY, generation),
            T0
        )
        .is_err()
    );
    // A record that doesn't say what its CRL does.
    let mut op = crl("ISSUER1", 1, 3, T0, T0 + 30 * DAY, generation);
    if let PkiOp::SetCrl(r) = &mut op {
        r.next_update_ms += DAY;
    }
    assert!(run(&mut pki, op, T0).is_err());
    // Out of date, from the future, or ahead of the state.
    assert!(
        run(
            &mut pki,
            crl("ISSUER1", 1, 3, T0, T0 + DAY, generation),
            T0 + 2 * DAY
        )
        .is_err()
    );
    assert!(
        run(
            &mut pki,
            crl("ISSUER1", 1, 3, T0 + DAY, T0 + 30 * DAY, generation),
            T0
        )
        .is_err()
    );
    assert!(
        run(
            &mut pki,
            crl("ISSUER1", 1, 3, T0, T0 + 30 * DAY, generation + 1),
            T0
        )
        .is_err()
    );
    assert_eq!(pki.as_deref().unwrap().crl.as_ref().unwrap().number, 2);
}

#[test]
fn a_new_issuers_crl_starts_its_own_numbers() {
    let mut pki = installed();
    let g = pki.as_deref().unwrap().generation;
    run(&mut pki, crl("ISSUER1", 1, 5, T0, T0 + 30 * DAY, g), T0).unwrap();
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
    // The retired issuer's CRL is no longer taken.
    assert!(run(&mut pki, crl("ISSUER1", 1, 6, T0, T0 + 30 * DAY, g), T0).is_err());
    run(&mut pki, crl("ISSUER2", 2, 1, T0, T0 + 30 * DAY, g), T0).unwrap();
}

fn signer_op(chain: &str, serial: u8) -> PkiOp {
    PkiOp::SetTokenSigner(Box::new(TokenSignerRecord {
        chain_pem: chain.into(),
        serial: Serial::from_be_bytes(&[serial]),
        not_after_ms: T0 + 730 * DAY,
        sealed_key: blob(),
    }))
}

#[test]
fn a_token_signer_is_the_active_issuers_and_codesigning_only() {
    let mut pki = installed();
    let generation = pki.as_deref().unwrap().generation;
    // Another CA's, or one with the wrong EKU (clientAuth): refused.
    assert!(run(&mut pki, signer_op("SIGNER:40|FOREIGN", 40), T0).is_err());
    assert!(run(&mut pki, signer_op("SIGNER:66|ISSUER1", 66), T0).is_err());
    // A record that doesn't say what its certificate does.
    assert!(run(&mut pki, signer_op("SIGNER:40|ISSUER1", 41), T0).is_err());
    assert!(pki.as_deref().unwrap().token_signer.is_none());
    run(&mut pki, signer_op("SIGNER:40|ISSUER1", 40), T0).unwrap();
    let p = pki.as_deref().unwrap();
    assert_eq!(
        p.token_signer.as_ref().unwrap().serial,
        Serial::from_be_bytes(&[40])
    );
    assert_eq!(p.generation, generation + 1, "verifiers read it");
    // Replaced by a newer one.
    run(&mut pki, signer_op("SIGNER:42|ISSUER1", 42), T0 + DAY).unwrap();
    assert_eq!(
        pki.as_deref()
            .unwrap()
            .token_signer
            .as_ref()
            .unwrap()
            .serial,
        Serial::from_be_bytes(&[42])
    );
    // Revocable though it isn't in `certs`: its tokens go with it.
    run(
        &mut pki,
        PkiOp::RevokeCert {
            serial: Serial::from_be_bytes(&[42]),
            reason: RevocationReason::KeyCompromise,
        },
        T0 + DAY,
    )
    .unwrap();
    assert!(
        pki.as_deref()
            .unwrap()
            .is_revoked(&Serial::from_be_bytes(&[42]))
    );
}

#[test]
fn a_renewal_is_of_a_good_certificate_kept_as_it_is() {
    let mut pki = installed();
    record(&mut pki, 50, node_cert(1, T0 + 30 * DAY));
    record(&mut pki, 51, node_cert(2, T0 + 30 * DAY));
    let renewal = |of: u8, serial: u8, cert: IssuedCert| PkiOp::RecordIssued {
        serial: Serial::from_be_bytes(&[serial]),
        cert: IssuedCert {
            renews: Some(Serial::from_be_bytes(&[of])),
            not_before_ms: T0 + DAY,
            not_after_ms: T0 + 40 * DAY,
            ..cert
        },
    };
    // Of an unknown cert, or with another identity: refused.
    assert!(run(&mut pki, renewal(99, 60, node_cert(1, 0)), T0 + DAY).is_err());
    assert!(run(&mut pki, renewal(50, 60, node_cert(3, 0)), T0 + DAY).is_err());
    // Of a cert revoked before the renewal is ordered: refused, though
    // the signer that signed it hadn't seen the revocation yet.
    run(
        &mut pki,
        PkiOp::RevokeCert {
            serial: Serial::from_be_bytes(&[50]),
            reason: RevocationReason::KeyCompromise,
        },
        T0 + DAY,
    )
    .unwrap();
    let e = run(&mut pki, renewal(50, 60, node_cert(1, 0)), T0 + DAY).unwrap_err();
    assert!(e.contains("revoked"), "{}", e);
    assert!(
        !pki.as_deref()
            .unwrap()
            .certs
            .contains_key(&Serial::from_be_bytes(&[60]))
    );
    // A good one.
    run(&mut pki, renewal(51, 61, node_cert(2, 0)), T0 + DAY).unwrap();
    // Expired: refused.
    assert!(run(&mut pki, renewal(51, 62, node_cert(2, 0)), T0 + 31 * DAY).is_err());
    // Its issuer revoked: neither live nor renewable.
    run(
        &mut pki,
        PkiOp::AddIssuer {
            id: IssuerId(2),
            record: Box::new(issuer("ISSUER2", 3, T0 + 1460 * DAY)),
            activate: true,
        },
        T0 + DAY,
    )
    .unwrap();
    run(&mut pki, PkiOp::RevokeIssuer { id: IssuerId(1) }, T0 + DAY).unwrap();
    let p = pki.as_deref().unwrap();
    assert!(!p.is_live(&Serial::from_be_bytes(&[61]), T0 + 2 * DAY));
    let mut again = node_cert(2, 0);
    again.issuer = IssuerId(2);
    let e = run(&mut pki, renewal(61, 63, again), T0 + 2 * DAY).unwrap_err();
    assert!(e.contains("issuer is revoked"), "{}", e);
}

fn node_slot(id: &str, node: u64) -> PkiOp {
    PkiOp::AddSlot(Box::new(KeySlot {
        id: id.into(),
        label: "x".into(),
        kind: SlotKind::NodeKey { node: m(node) },
        wrapped_dek: blob(),
    }))
}

fn has_slot(pki: &Option<Arc<PkiState>>, id: &str) -> bool {
    pki.as_deref()
        .unwrap()
        .seal
        .slots
        .iter()
        .any(|s| s.id == id)
}

#[test]
fn a_node_key_slot_is_for_a_node_with_a_live_cert() {
    let mut pki = installed();
    assert!(
        run(&mut pki, node_slot("nk-1", 1), T0)
            .unwrap_err()
            .contains("no live node certificate")
    );
    // A libvirt cert isn't a node cert.
    let mut libvirt = node_cert(1, T0 + 300 * DAY);
    libvirt.identity.profile = ProfileKind::Libvirt;
    record(&mut pki, 21, libvirt);
    assert!(run(&mut pki, node_slot("nk-1", 1), T0).is_err());
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    run(&mut pki, node_slot("nk-1", 1), T0).unwrap();
    // Expired: no more slots for it.
    assert!(run(&mut pki, node_slot("nk-1b", 1), T0 + 31 * DAY).is_err());
}

#[test]
fn revoking_a_members_last_node_cert_takes_its_node_key_slots() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    record(&mut pki, 21, node_cert(1, T0 + 30 * DAY)); // renewed, say
    record(&mut pki, 22, node_cert(2, T0 + 30 * DAY));
    run(&mut pki, node_slot("nk-1", 1), T0).unwrap();
    run(&mut pki, node_slot("nk-2", 2), T0).unwrap();
    let revoke = |n: u8| PkiOp::RevokeCert {
        serial: Serial::from_be_bytes(&[n]),
        reason: RevocationReason::KeyCompromise,
    };
    run(&mut pki, revoke(20), T0).unwrap();
    assert!(has_slot(&pki, "nk-1"), "node 1 still has a live node cert");
    run(&mut pki, revoke(21), T0).unwrap();
    assert!(!has_slot(&pki, "nk-1"), "gone in the same entry");
    assert!(has_slot(&pki, "nk-2"), "another node's stays");
}

#[test]
fn a_replaced_member_loses_its_node_key_slots() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    run(&mut pki, node_slot("nk-1", 1), T0).unwrap();
    enroll(&mut pki, 1, node_enrollment(Some(1), true, 1));
    consume(&mut pki, 1, Some(1)).unwrap();
    assert!(!has_slot(&pki, "nk-1"));
}

#[test]
fn revoking_an_issuer_takes_the_node_key_slots_of_its_nodes() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    run(&mut pki, node_slot("nk-1", 1), T0).unwrap();
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
    assert!(has_slot(&pki, "nk-1"), "a retired issuer's certs stay good");
    run(&mut pki, PkiOp::RevokeIssuer { id: IssuerId(1) }, T0 + 5).unwrap();
    assert!(!has_slot(&pki, "nk-1"));
}

#[test]
fn the_last_slot_stays_even_for_a_revoked_node() {
    let mut pki = installed();
    record(&mut pki, 20, node_cert(1, T0 + 30 * DAY));
    run(&mut pki, node_slot("nk-1", 1), T0).unwrap();
    let first = pki.as_deref().unwrap().seal.slots[0].id.clone();
    run(&mut pki, PkiOp::RemoveSlot { id: first }, T0).unwrap();
    run(
        &mut pki,
        PkiOp::RevokeCert {
            serial: Serial::from_be_bytes(&[20]),
            reason: RevocationReason::Unspecified,
        },
        T0,
    )
    .unwrap();
    assert!(has_slot(&pki, "nk-1"), "never the last slot");
}
