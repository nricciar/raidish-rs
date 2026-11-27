//! `rdt2` tokens (CA-2c): signed by the cluster's token signer, checked
//! against the trusted roots for codeSigning, its OU, and what's revoked.

use super::*;
use crate::ca::{LeafSpec, Signer, create_issuer, create_root, facts, generate_csr, pem_certs};
use raidish_types::pki::{
    IssuerId, PkiObserver, ProfileKind, Revocation, RevocationReason, TokenId,
};
use rustls::pki_types::{PrivatePkcs8KeyDer, pem::PemObject};
use std::sync::Arc;

struct Ca {
    root_pem: String,
    issuer: Signer,
}

fn ca() -> Ca {
    crate::tls::install_provider();
    let (root_pem, root_key) = create_root("test root", 7300).unwrap();
    let (issuer_pem, issuer_key) = create_issuer("issuer", &root_pem, &root_key, 730).unwrap();
    Ca {
        issuer: Signer::new(IssuerId(1), &issuer_pem, &issuer_key).unwrap(),
        root_pem,
    }
}

/// A leaf of `profile` from `ca`: its chain (leaf, issuer) and PKCS#8 key.
fn leaf(ca: &Ca, profile: ProfileKind, days: u32) -> (Vec<CertificateDer<'static>>, Vec<u8>) {
    let (key_pem, csr) = generate_csr("ignored").unwrap();
    let issued = ca
        .issuer
        .sign(
            &csr,
            &LeafSpec {
                profile,
                cn: "raidish-tokens".into(),
                sans: vec![],
                days,
            },
        )
        .unwrap();
    let key = PrivatePkcs8KeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
    (
        pem_certs(&ca.issuer.chain(&issued.cert_pem)).unwrap(),
        key.secret_pkcs8_der().to_vec(),
    )
}

fn verifier(ca: &Ca) -> Verifier {
    Verifier::new(pem_certs(&ca.root_pem).unwrap(), vec![]).unwrap()
}

#[test]
fn a_token_signers_token_verifies() {
    let ca = ca();
    let (chain, key) = leaf(&ca, ProfileKind::TokenSigner, 730);
    let (tok, claims) = create_token_rdt2("alice", Role::Admin, Some(3600), &chain, &key).unwrap();
    assert!(tok.starts_with("rdt2.") && tok.split('.').count() == 5);
    let p = verifier(&ca).verify_token(&tok).unwrap();
    assert_eq!(p.name, "alice");
    assert_eq!(p.role, Role::Admin);
    assert_eq!(p.via, Via::Token(claims.id.clone()));
    assert!(parse_token(&tok).unwrap().is_rdt2());
    // Tampered with: the signature fails.
    let mut parts: Vec<String> = tok.split('.').map(str::to_string).collect();
    let mut c = claims.clone();
    c.role = "admin".into();
    c.sub = "mallory".into();
    parts[1] = B64.encode(serde_json::to_vec(&c).unwrap());
    assert!(
        verifier(&ca)
            .verify_token(&parts.join("."))
            .unwrap_err()
            .contains("signature")
    );
    // No node role.
    assert!(create_token_rdt2("n", Role::Node, None, &chain, &key).is_err());
}

#[test]
fn exp_is_clamped_to_the_signer() {
    let ca = ca();
    let (chain, key) = leaf(&ca, ProfileKind::TokenSigner, 10);
    let not_after = facts(&chain[0]).unwrap().not_after_ms as i64 / 1000;
    let (_, c) = create_token_rdt2("a", Role::Readonly, Some(100 * 86_400), &chain, &key).unwrap();
    assert_eq!(c.exp, Some(not_after));
    let (_, c) = create_token_rdt2("a", Role::Readonly, None, &chain, &key).unwrap();
    assert_eq!(c.exp, Some(not_after), "never is the signer's expiry");
    let (_, c) = create_token_rdt2("a", Role::Readonly, Some(60), &chain, &key).unwrap();
    assert_eq!(c.exp, Some(c.iat + 60));
    // An expired one is refused.
    let (tok, _) = create_token_rdt2("a", Role::Readonly, Some(-1), &chain, &key).unwrap();
    assert!(
        verifier(&ca)
            .verify_token(&tok)
            .unwrap_err()
            .contains("expired")
    );
}

#[test]
fn only_a_token_signer_signs_them() {
    let ca = ca();
    // An admin certificate (clientAuth) passed off as the token signer.
    let (chain, key) = leaf(&ca, ProfileKind::Admin, 30);
    assert!(create_token_rdt2("a", Role::Admin, None, &chain, &key).is_err());
    let iat = now();
    let c = claims("mallory", Role::Admin, iat, Some(iat + 60)).unwrap();
    let forged = sign(TOKEN_PREFIX_2, &c, &key, &[&chain[0], &chain[1]]).unwrap();
    assert!(
        verifier(&ca).verify_token(&forged).is_err(),
        "an admin cert's rdt2 token"
    );
    // Without an expiry.
    let (chain, key) = leaf(&ca, ProfileKind::TokenSigner, 30);
    let c = claims("a", Role::Admin, iat, None).unwrap();
    let forged = sign(TOKEN_PREFIX_2, &c, &key, &[&chain[0], &chain[1]]).unwrap();
    assert!(
        verifier(&ca)
            .verify_token(&forged)
            .unwrap_err()
            .contains("expiry")
    );
    // A token signer of another CA.
    let other = self::ca();
    let (chain, key) = leaf(&other, ProfileKind::TokenSigner, 30);
    let (tok, _) = create_token_rdt2("a", Role::Admin, None, &chain, &key).unwrap();
    assert!(verifier(&ca).verify_token(&tok).is_err());
    // An rdt1 token signed with a token signer (not an admin): refused.
    let (tok, _) = {
        let c = claims("a", Role::Admin, iat, None).unwrap();
        (sign(TOKEN_PREFIX, &c, &key, &[&chain[0]]).unwrap(), c)
    };
    assert!(verifier(&other).verify_token(&tok).is_err());
}

#[test]
fn the_token_signer_is_no_tls_client() {
    let ca = ca();
    let (chain, _) = leaf(&ca, ProfileKind::TokenSigner, 30);
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(pem_certs(&ca.root_pem).unwrap().remove(0))
        .unwrap();
    let v = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    assert!(
        v.verify_client_cert(&chain[0], &chain[1..], UnixTime::now())
            .is_err()
    );
}

#[test]
fn revoking_the_signer_or_the_token_refuses_it() {
    let ca = ca();
    let (chain, key) = leaf(&ca, ProfileKind::TokenSigner, 30);
    let (tok, claims) = create_token_rdt2("a", Role::Readonly, None, &chain, &key).unwrap();
    let trust = crate::trust::Trust::new(vec![]);
    let v = verifier(&ca).with_trust(trust.clone());
    assert!(v.verify_token(&tok).is_ok());
    let revoked = Revocation {
        reason: RevocationReason::KeyCompromise,
        at_ms: 1,
        expires_ms: None,
    };
    // Enough of a CA state for the trust to read its revocations.
    let mut pki: raidish_types::pki::PkiState = serde_json::from_value(serde_json::json!({
        "anchor": {"cert_pem": ca.root_pem, "fingerprint": "00".repeat(32)},
        "issuers": {},
        "seal": {"cluster_id": "00".repeat(16), "slots": [], "epoch": 0},
        "generation": 1
    }))
    .unwrap();
    pki.revoked_tokens
        .insert(TokenId::parse(&claims.id).unwrap(), revoked.clone());
    trust.observe_pki(Some(&pki));
    assert!(v.verify_token(&tok).unwrap_err().contains("revoked"));
    pki.revoked_tokens.clear();
    trust.observe_pki(Some(&pki));
    assert!(v.verify_token(&tok).is_ok());
    // The signer: every token it signed.
    pki.revoked_certs
        .insert(facts(&chain[0]).unwrap().serial, revoked);
    trust.observe_pki(Some(&pki));
    assert!(v.verify_token(&tok).unwrap_err().contains("revoked"));
}
