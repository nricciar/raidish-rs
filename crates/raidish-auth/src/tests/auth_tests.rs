use super::*;
use crate::pki::Ca;
use rustls::pki_types::{PrivateKeyDer, pem::PemObject};

struct Fixture {
    _dir: tempfile::TempDir,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().to_path_buf();
        Ca::init(&dir, "ca", None, false).unwrap();
        let ca = Ca::load(&dir).unwrap();
        ca.issue("admin", Role::Admin, &[], None, false).unwrap();
        ca.issue("viewer", Role::Readonly, &[], None, false)
            .unwrap();
        Fixture { _dir: d, dir }
    }

    fn identity(&self, name: &str) -> (Vec<u8>, Vec<u8>) {
        let pem = std::fs::read_to_string(self.dir.join(name).join("cert.pem")).unwrap();
        let cert = pki::read_pem_certs(&pem).unwrap().remove(0);
        let key = match PrivateKeyDer::from_pem_file(self.dir.join(name).join("key.pem")).unwrap() {
            PrivateKeyDer::Pkcs8(k) => k.secret_pkcs8_der().to_vec(),
            _ => panic!("expected pkcs8"),
        };
        (cert, key)
    }

    fn verifier(&self, revoked: Vec<String>) -> Verifier {
        let tls = crate::tls::TlsConfig {
            dir: Some(self.dir.join("admin").to_string_lossy().into()),
            ..Default::default()
        };
        Verifier::new(tls.roots().unwrap(), revoked).unwrap()
    }
}

#[test]
fn token_round_trip() {
    let f = Fixture::new();
    let (cert, key) = f.identity("admin");
    let (tok, claims) = create_token("alice", Role::Readonly, None, &cert, &key).unwrap();
    let p = f.verifier(vec![]).verify_token(&tok).unwrap();
    assert_eq!(p.name, "alice");
    assert_eq!(p.role, Role::Readonly);
    assert_eq!(p.via, Via::Token(claims.id.clone()));
    assert!(!p.can_write());

    // Revoked.
    assert!(f.verifier(vec![claims.id]).verify_token(&tok).is_err());
}

#[test]
fn token_rejections() {
    let f = Fixture::new();
    let (cert, key) = f.identity("admin");

    // Expired.
    let (tok, _) = create_token("bob", Role::Admin, Some(-1), &cert, &key).unwrap();
    assert_eq!(
        f.verifier(vec![]).verify_token(&tok).unwrap_err(),
        "token expired"
    );

    // Only admins sign; no node tokens.
    let (vcert, vkey) = f.identity("viewer");
    assert!(create_token("x", Role::Readonly, None, &vcert, &vkey).is_err());
    assert!(create_token("x", Role::Node, None, &cert, &key).is_err());

    // Tampered payload: escalate readonly -> admin.
    let (tok, _) = create_token("eve", Role::Readonly, None, &cert, &key).unwrap();
    let parts: Vec<&str> = tok.split('.').collect();
    let mut claims: Claims = serde_json::from_slice(&B64.decode(parts[1]).unwrap()).unwrap();
    claims.role = "admin".into();
    let forged = format!(
        "{}.{}.{}.{}",
        parts[0],
        B64.encode(serde_json::to_vec(&claims).unwrap()),
        parts[2],
        parts[3]
    );
    assert_eq!(
        f.verifier(vec![]).verify_token(&forged).unwrap_err(),
        "bad token signature"
    );

    // Signed by an admin of a different CA.
    let other = Fixture::new();
    let (ocert, okey) = other.identity("admin");
    let (tok, _) = create_token("mallory", Role::Admin, None, &ocert, &okey).unwrap();
    assert!(f.verifier(vec![]).verify_token(&tok).is_err());

    assert!(f.verifier(vec![]).verify_token("garbage").is_err());
}
