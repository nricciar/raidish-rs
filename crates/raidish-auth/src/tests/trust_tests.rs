use super::*;
use crate::ca::{LeafSpec, Signer, create_issuer, create_root, facts, first_der, generate_csr};
use crate::net::Revocable;
use raidish_types::pki::{
    CertFingerprint, IssuerId, IssuerRecord, KdfParams, KeySlot, ProfileKind, Revocation,
    RevocationReason, SealConfig, SealedBlob, SlotKind, TokenId, TrustAnchor,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn blob() -> SealedBlob {
    SealedBlob {
        v: 1,
        nonce: [0; 12],
        ct: vec![],
    }
}

struct Ca {
    root_pem: String,
    root_key: zeroize::Zeroizing<String>,
    issuer: Signer,
}

fn ca() -> Ca {
    crate::tls::install_provider();
    let (root_pem, root_key) = create_root("test root", 7300).unwrap();
    let (issuer_pem, issuer_key) = create_issuer("issuer", &root_pem, &root_key, 730).unwrap();
    Ca {
        issuer: Signer::new(IssuerId(1), &issuer_pem, &issuer_key).unwrap(),
        root_pem,
        root_key,
    }
}

fn leaf(signer: &Signer, cn: &str) -> Vec<u8> {
    let (_key, csr) = generate_csr("ignored").unwrap();
    let issued = signer
        .sign(
            &csr,
            &LeafSpec {
                profile: ProfileKind::Admin,
                cn: cn.into(),
                sans: vec![],
                days: 30,
            },
        )
        .unwrap();
    first_der(&issued.cert_pem).unwrap().to_vec()
}

fn record(pem: &str, status: IssuerStatus) -> IssuerRecord {
    let f = facts(&first_der(pem).unwrap()).unwrap();
    IssuerRecord {
        cert_pem: pem.into(),
        serial: f.serial,
        ski: f.ski,
        not_after_ms: f.not_after_ms,
        sealed_key: blob(),
        status,
    }
}

fn state(ca: &Ca) -> PkiState {
    PkiState {
        anchor: TrustAnchor {
            cert_pem: ca.root_pem.clone(),
            fingerprint: CertFingerprint([1; 32]),
            sealed_key: None,
        },
        issuers: BTreeMap::from([(
            IssuerId(1),
            record(&ca.issuer.cert_pem, IssuerStatus::Active),
        )]),
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
        generation: 1,
    }
}

fn revocation() -> Revocation {
    Revocation {
        reason: RevocationReason::KeyCompromise,
        at_ms: 1,
        expires_ms: None,
    }
}

#[test]
fn a_revoked_serial_is_refused_once_observed() {
    let ca = ca();
    let (a, b) = (leaf(&ca.issuer, "a"), leaf(&ca.issuer, "b"));
    let trust = Trust::new(vec![]);
    let mut pki = state(&ca);
    trust.observe_pki(Some(&pki));
    assert!(trust.check_leaf(&a).is_ok() && trust.check_leaf(&b).is_ok());
    pki.revoked_certs
        .insert(facts(&a).unwrap().serial, revocation());
    trust.observe_pki(Some(&pki));
    assert!(trust.check_leaf(&a).unwrap_err().contains("revoked"));
    assert!(trust.check_leaf(&b).is_ok());
    // Gone from the state (pruned, or another CA): good again.
    trust.observe_pki(None);
    assert!(trust.check_leaf(&a).is_ok());
}

#[test]
fn a_revoked_issuers_leaves_are_refused_by_key_id() {
    let ca = ca();
    // A second issuer, revoked: its key mints a leaf the registry never saw.
    let (pem2, key2) = create_issuer("issuer 2", &ca.root_pem, &ca.root_key, 730).unwrap();
    let issuer2 = Signer::new(IssuerId(2), &pem2, &key2).unwrap();
    let off_the_books = leaf(&issuer2, "mallory");
    let good = leaf(&ca.issuer, "alice");
    let trust = Trust::new(vec![]);
    let mut pki = state(&ca);
    pki.issuers.insert(
        IssuerId(2),
        record(&pem2, IssuerStatus::Revoked { at_ms: 1 }),
    );
    trust.observe_pki(Some(&pki));
    assert!(
        trust
            .check_leaf(&off_the_books)
            .unwrap_err()
            .contains("revoked issuer")
    );
    assert!(trust.check_leaf(&good).is_ok());
}

#[test]
fn configured_token_ids_stay_with_the_clusters() {
    let ca = ca();
    let trust = Trust::new(vec!["00000000000000aa".into()]);
    assert!(trust.token_revoked("00000000000000aa"));
    let mut pki = state(&ca);
    let id = TokenId::parse("00000000000000bb").unwrap();
    pki.revoked_tokens.insert(id, revocation());
    trust.observe_pki(Some(&pki));
    assert!(trust.token_revoked("00000000000000aa"));
    assert!(trust.token_revoked("00000000000000bb"));
    assert!(!trust.token_revoked("00000000000000cc"));
    trust.observe_pki(None);
    assert!(trust.token_revoked("00000000000000aa"));
    assert!(!trust.token_revoked("00000000000000bb"));
}

#[test]
fn a_verifier_refuses_what_its_trust_does() {
    let ca = ca();
    let trust = Trust::new(vec![]);
    let v = crate::auth::Verifier::new(crate::ca::pem_certs(&ca.root_pem).unwrap(), vec![])
        .unwrap()
        .with_trust(trust.clone());
    assert!(!v.is_revoked("00000000000000bb"));
    let mut pki = state(&ca);
    pki.revoked_tokens
        .insert(TokenId::parse("00000000000000bb").unwrap(), revocation());
    trust.observe_pki(Some(&pki));
    assert!(v.is_revoked("00000000000000bb"), "S3/SQS keys read it live");
}

#[tokio::test]
async fn revoking_closes_what_is_open_and_only_that() {
    let ca = ca();
    let (a, b) = (leaf(&ca.issuer, "a"), leaf(&ca.issuer, "b"));
    let trust = Trust::new(vec![]);
    let conns = trust.connections();
    let pa = conns.register(a.clone().into()).unwrap();
    let pb = conns.register(b.clone().into()).unwrap();
    let mut pki = state(&ca);
    trust.observe_pki(Some(&pki));
    pki.revoked_certs
        .insert(facts(&a).unwrap().serial, revocation());
    trust.observe_pki(Some(&pki));
    tokio::time::timeout(Duration::from_secs(1), pa.closed())
        .await
        .expect("a's connection is closed");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pb.closed())
            .await
            .is_err(),
        "b's stays open"
    );
}

#[tokio::test]
async fn revoking_an_issuer_closes_its_leaves_connections() {
    let ca = ca();
    let (pem2, key2) = create_issuer("issuer 2", &ca.root_pem, &ca.root_key, 730).unwrap();
    let issuer2 = Signer::new(IssuerId(2), &pem2, &key2).unwrap();
    let trust = Trust::new(vec![]);
    let p = trust
        .connections()
        .register(leaf(&issuer2, "x").into())
        .unwrap();
    let mut pki = state(&ca);
    pki.issuers
        .insert(IssuerId(2), record(&pem2, IssuerStatus::Retired));
    trust.observe_pki(Some(&pki));
    pki.issuers.get_mut(&IssuerId(2)).unwrap().status = IssuerStatus::Revoked { at_ms: 2 };
    trust.observe_pki(Some(&pki));
    tokio::time::timeout(Duration::from_secs(1), p.closed())
        .await
        .expect("closed with its issuer");
}

#[tokio::test]
async fn a_revocable_stream_fails_once_its_peer_is_closed() {
    let ca = ca();
    let der = leaf(&ca.issuer, "a");
    let trust = Trust::new(vec![]);
    let peer = trust.connections().register(der.clone().into()).unwrap();
    let serial = peer.serial().clone();
    let (mine, mut theirs) = tokio::io::duplex(64);
    let mut s = Revocable::new(mine, Some(peer));
    theirs.write_all(b"hi").await.unwrap();
    let mut buf = [0u8; 2];
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hi");
    // A read waiting when the revocation comes ends with it.
    let reading = tokio::spawn(async move {
        let mut buf = [0u8; 1];
        let r = s.read(&mut buf).await;
        (r, s)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    trust.connections().close(&serial);
    let (r, mut s) = tokio::time::timeout(Duration::from_secs(1), reading)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::ConnectionAborted);
    assert!(s.write_all(b"x").await.is_err(), "writes fail too");
    // Without a registration: a plain stream.
    let (mine, mut theirs) = tokio::io::duplex(64);
    let mut s = Revocable::new(mine, None);
    theirs.write_all(b"ok").await.unwrap();
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ok");
}

#[tokio::test]
async fn a_split_stream_wakes_its_parked_reader_after_another_task_wrote() {
    // As NBD over peer_io: the read half parked in one task, a reply
    // written from another that then ends. The revocation must reach the
    // reader.
    let ca = ca();
    let trust = Trust::new(vec![]);
    let peer = trust
        .connections()
        .register(leaf(&ca.issuer, "a").into())
        .unwrap();
    let serial = peer.serial().clone();
    let (mine, mut theirs) = tokio::io::duplex(64);
    let (mut rd, mut wr) = tokio::io::split(Revocable::new(mine, Some(peer)));
    let reading = tokio::spawn(async move {
        let mut buf = [0u8; 1];
        rd.read(&mut buf).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::spawn(async move {
        wr.write_all(b"reply").await.unwrap();
        wr.flush().await.unwrap();
    })
    .await
    .unwrap();
    let mut got = [0u8; 5];
    theirs.read_exact(&mut got).await.unwrap();
    trust.connections().close(&serial);
    let r = tokio::time::timeout(Duration::from_secs(1), reading)
        .await
        .expect("the parked reader is woken")
        .unwrap();
    assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::ConnectionAborted);
}
