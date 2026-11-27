use super::*;
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{ServerName, UnixTime};
use std::sync::Arc;

struct Ca {
    root_pem: String,
    issuer: Signer,
}

fn ca() -> Ca {
    crate::tls::install_provider();
    let (root_pem, root_key) = create_root("test root", 7300).unwrap();
    let (issuer_pem, issuer_key) = create_issuer("test issuer", &root_pem, &root_key, 730).unwrap();
    Ca {
        issuer: Signer::new(IssuerId(1), &issuer_pem, &issuer_key).unwrap(),
        root_pem,
    }
}

fn spec(profile: ProfileKind, cn: &str, sans: &[&str]) -> LeafSpec {
    LeafSpec {
        profile,
        cn: cn.into(),
        sans: sans.iter().map(|s| s.to_string()).collect(),
        days: 30,
    }
}

fn roots(pem: &str) -> Arc<rustls::RootCertStore> {
    let mut store = rustls::RootCertStore::empty();
    for c in pem_certs(pem).unwrap() {
        store.add(c).unwrap();
    }
    Arc::new(store)
}

#[test]
fn a_two_tier_chain_verifies_in_rustls() {
    let ca = ca();
    let (_key, csr) = generate_csr("ignored").unwrap();
    let node = ca
        .issuer
        .sign(
            &csr,
            &spec(
                ProfileKind::Node,
                "node1",
                &["node1", "10.0.0.1", "localhost"],
            ),
        )
        .unwrap();
    let leaf = first_der(&node.cert_pem).unwrap();
    let inter = first_der(&ca.issuer.cert_pem).unwrap();
    let roots = roots(&ca.root_pem);
    rustls::client::WebPkiServerVerifier::builder(roots.clone())
        .build()
        .unwrap()
        .verify_server_cert(
            &leaf,
            std::slice::from_ref(&inter),
            &ServerName::try_from("node1").unwrap(),
            &[],
            UnixTime::now(),
        )
        .unwrap();
    rustls::server::WebPkiClientVerifier::builder(roots.clone())
        .build()
        .unwrap()
        .verify_client_cert(&leaf, std::slice::from_ref(&inter), UnixTime::now())
        .unwrap();
    // Without the intermediate there's no path.
    assert!(
        rustls::server::WebPkiClientVerifier::builder(roots)
            .build()
            .unwrap()
            .verify_client_cert(&leaf, &[], UnixTime::now())
            .is_err()
    );
}

#[test]
fn the_csr_only_gives_its_key() {
    let ca = ca();
    // A CSR asking for other names, an admin OU and client and server
    // usages: none of it is used.
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let evil = vec!["evil.example".to_string()];
    let mut params = CertificateParams::new(evil).unwrap();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "evil");
    dn.push(DnType::OrganizationalUnitName, "admin");
    params.distinguished_name = dn;
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::CodeSigning,
    ];
    let csr = params.serialize_request(&key).unwrap().pem().unwrap();
    let issued = ca
        .issuer
        .sign(&csr, &spec(ProfileKind::Readonly, "viewer", &[]))
        .unwrap();
    let info = crate::pki::inspect_der(&first_der(&issued.cert_pem).unwrap()).unwrap();
    assert_eq!(info.name, "viewer");
    assert_eq!(info.ou.as_deref(), Some("readonly"));
    assert!(!info.is_ca);
    assert!(info.sans.is_empty());
    // But the key is the CSR's.
    let der = first_der(&issued.cert_pem).unwrap();
    let (_, cert) = X509Certificate::from_der(&der).unwrap();
    assert_eq!(
        cert.public_key().subject_public_key.data,
        key.public_key_raw()
    );
}

#[test]
fn a_tampered_csr_is_refused() {
    let ca = ca();
    let (_key, csr) = generate_csr("x").unwrap();
    let der = rustls::pki_types::CertificateSigningRequestDer::from_pem_slice(csr.as_bytes())
        .unwrap()
        .as_ref()
        .to_vec();
    let mut bad = der.clone();
    let n = bad.len();
    bad[n - 5] ^= 1; // in the signature
    let pem = format!(
        "-----BEGIN CERTIFICATE REQUEST-----\n{}\n-----END CERTIFICATE REQUEST-----\n",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bad)
    );
    assert!(
        ca.issuer
            .sign(&pem, &spec(ProfileKind::Admin, "a", &[]))
            .is_err()
    );
}

#[test]
fn leaves_name_their_issuer_by_key_id() {
    let ca = ca();
    let (_key, csr) = generate_csr("x").unwrap();
    let leaf = ca
        .issuer
        .sign(&csr, &spec(ProfileKind::Admin, "a", &[]))
        .unwrap();
    let issuer = facts(&first_der(&ca.issuer.cert_pem).unwrap()).unwrap();
    let aki = authority_key_id(&first_der(&leaf.cert_pem).unwrap()).unwrap();
    assert!(!issuer.ski.0.is_empty());
    assert_eq!(aki, issuer.ski);
    // And the serial the signer reports is the certificate's.
    assert_eq!(
        facts(&first_der(&leaf.cert_pem).unwrap()).unwrap().serial,
        leaf.serial
    );
}

#[test]
fn leaves_end_with_their_issuer() {
    let ca = ca();
    let (_key, csr) = generate_csr("x").unwrap();
    let mut long = spec(ProfileKind::Libvirt, "h", &["h"]);
    long.days = 5000;
    let leaf = ca.issuer.sign(&csr, &long).unwrap();
    let issuer = facts(&first_der(&ca.issuer.cert_pem).unwrap()).unwrap();
    assert!(leaf.not_after_ms <= issuer.not_after_ms);
}

#[test]
fn profiles_set_their_usages() {
    let ca = ca();
    let eku = |profile| {
        let (_key, csr) = generate_csr("x").unwrap();
        let leaf = ca.issuer.sign(&csr, &spec(profile, "x", &[])).unwrap();
        let der = first_der(&leaf.cert_pem).unwrap();
        let (_, cert) = X509Certificate::from_der(&der).unwrap();
        let e = cert.extended_key_usage().unwrap().unwrap().value.clone();
        (e.server_auth, e.client_auth, e.code_signing)
    };
    assert_eq!(eku(ProfileKind::Node), (true, true, false));
    assert_eq!(eku(ProfileKind::Admin), (false, true, false));
    assert_eq!(eku(ProfileKind::Server), (true, false, false));
    assert_eq!(eku(ProfileKind::TokenSigner), (false, false, true));
}

#[test]
fn the_checker_accepts_only_the_real_chain() {
    let ca = ca();
    let now = ms(OffsetDateTime::now_utc());
    let anchor = X509Checker.anchor(&ca.root_pem, now).unwrap();
    assert_eq!(
        anchor.fingerprint,
        fingerprint(&first_der(&ca.root_pem).unwrap())
    );
    X509Checker
        .ca(&ca.issuer.cert_pem, &ca.root_pem, 0, now)
        .unwrap();
    // The issuer isn't a root; the root isn't pathLen 0; another root's
    // issuer doesn't chain to this one.
    assert!(X509Checker.anchor(&ca.issuer.cert_pem, now).is_err());
    assert!(X509Checker.ca(&ca.root_pem, &ca.root_pem, 0, now).is_err());
    let other = self::ca();
    assert!(
        X509Checker
            .ca(&other.issuer.cert_pem, &ca.root_pem, 0, now)
            .is_err()
    );
    // Time is the caller's: after the issuer ends, it's refused.
    let end = facts(&first_der(&ca.issuer.cert_pem).unwrap())
        .unwrap()
        .not_after_ms;
    assert!(
        X509Checker
            .ca(&ca.issuer.cert_pem, &ca.root_pem, 0, end)
            .is_err()
    );
    // A leaf is no CA.
    let (_key, csr) = generate_csr("x").unwrap();
    let leaf = ca
        .issuer
        .sign(&csr, &spec(ProfileKind::Admin, "a", &[]))
        .unwrap();
    assert!(
        X509Checker
            .ca(&leaf.cert_pem, &ca.issuer.cert_pem, 0, now)
            .is_err()
    );
}

#[test]
fn a_crl_verifies_against_its_issuer_and_lists_its_serials() {
    let ca = ca();
    let other = self::ca();
    let a = Serial::parse("8a0f00000000000000000000000000ff").unwrap();
    let b = Serial::parse("01").unwrap();
    let next = crate::ca::now().unix_timestamp() as u64 * 1000 + 30 * 86_400_000;
    let (der, facts) = ca
        .issuer
        .sign_crl(
            &[
                (
                    a.clone(),
                    RevocationReason::KeyCompromise,
                    1_700_000_000_000,
                ),
                (b.clone(), RevocationReason::Superseded, 1_700_000_000_000),
            ],
            7,
            next,
        )
        .unwrap();
    assert_eq!(facts.number, 7);
    assert_eq!(facts.next_update_ms, next - next % 1000);
    assert!(facts.this_update_ms < facts.next_update_ms);
    assert_eq!(X509Checker.crl(&der, &ca.issuer.cert_pem).unwrap(), facts);
    // Not the other CA's, and not once changed.
    assert!(X509Checker.crl(&der, &other.issuer.cert_pem).is_err());
    let mut bad = der.clone();
    let n = bad.len();
    bad[n - 5] ^= 1;
    assert!(X509Checker.crl(&bad, &ca.issuer.cert_pem).is_err());
    let mut serials = crl_serials(&der).unwrap();
    serials.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(serials, want);
    // Its authority key id is the issuer's key id: what finds the issuer.
    let (_, crl) = CertificateRevocationList::from_der(&der).unwrap();
    let aki = crl
        .extensions()
        .iter()
        .find_map(|e| match e.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(k) => {
                k.key_identifier.as_ref().map(|k| k.0.to_vec())
            }
            _ => None,
        })
        .unwrap();
    let ski = facts_of(&ca.issuer.cert_pem);
    assert_eq!(KeyId(aki), ski);
    // An empty one too.
    let (der, _) = ca.issuer.sign_crl(&[], 8, next).unwrap();
    assert!(crl_serials(&der).unwrap().is_empty());
}

fn facts_of(pem: &str) -> KeyId {
    facts(&first_der(pem).unwrap()).unwrap().ski
}
