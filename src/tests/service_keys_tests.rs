//! Access keys made from tokens: what they say, who may make which,
//! expiry and revocation.

use super::*;
use crate::frontend::keys::{AccessKey, ServiceAuth};
use crate::frontend::testing::Fixture;
use crate::sigv4::{AuthError, Keys};

fn caller(role: Role, via: Via, exp: Option<i64>) -> Principal {
    Principal {
        name: "alice".into(),
        role,
        via,
        exp,
    }
}

fn req(role: Option<&str>, expires: Option<&str>) -> KeyRequest {
    KeyRequest {
        token: None,
        role: role.map(str::to_string),
        expires: expires.map(str::to_string),
    }
}

#[test]
fn keys_from_tokens_and_certs() {
    let sk = [9u8; 32];
    let tok = caller(
        Role::Admin,
        Via::Token("00112233aabbccdd".into()),
        Some(4_000_000_000),
    );
    let k = make_key(&tok, &req(None, None), &sk).unwrap();
    // The token's id, role and expiry.
    assert_eq!(k.id, "00112233aabbccdd");
    let a = AccessKey::parse(&k.access_key_id).unwrap();
    assert_eq!((a.role, a.exp), (Role::Admin, Some(4_000_000_000)));
    assert_eq!(k.secret_access_key, keys::secret(&sk, &k.access_key_id));
    // Narrower: readonly, shorter.
    let k = make_key(&tok, &req(Some("readonly"), Some("1h")), &sk).unwrap();
    let a = AccessKey::parse(&k.access_key_id).unwrap();
    assert_eq!(a.role, Role::Readonly);
    assert!(a.exp.unwrap() < 4_000_000_000);
    // Not wider than the caller.
    let ro = caller(Role::Readonly, Via::Token("0011223344556677".into()), None);
    assert_eq!(
        make_key(&ro, &req(Some("admin"), None), &sk).unwrap_err().0,
        rocket::http::Status::Forbidden
    );
    assert!(make_key(&ro, &req(Some("node"), None), &sk).is_err());
    // A node (certificate) makes admin keys of their own id.
    let node = caller(Role::Node, Via::Cert, None);
    let k = make_key(&node, &req(None, None), &sk).unwrap();
    assert_eq!((k.role.as_str(), k.expires), ("admin", None));
}

#[test]
fn checking_keys() {
    let fx = Fixture::new(vec!["00112233aabbccdd".into()]);
    let k = fx.service_key.clone();
    let auth = ServiceAuth::new(
        "us-east-1".into(),
        fx.verifier.clone(),
        Arc::new(move || Some(k.clone())),
    );
    let (id, secret) = fx.key(Role::Admin);
    assert_eq!(auth.secret(&id).unwrap(), secret);
    assert_eq!(auth.role(&id), Role::Admin);
    // Revoked (its token's id).
    let (revoked, _) = keys::mint(
        &fx.service_key,
        &AccessKey {
            id: "00112233aabbccdd".into(),
            role: Role::Admin,
            exp: None,
        },
    )
    .unwrap();
    assert_eq!(auth.secret(&revoked), Err(AuthError::UnknownKey));
    // Expired.
    let (old, _) = keys::mint(
        &fx.service_key,
        &AccessKey {
            id: keys::random_id(),
            role: Role::Admin,
            exp: Some(1000),
        },
    )
    .unwrap();
    assert_eq!(auth.secret(&old), Err(AuthError::KeyExpired));
    assert_eq!(
        auth.secret("AKIAIOSFODNN7EXAMPLE"),
        Err(AuthError::UnknownKey)
    );
    // No service key yet: nothing is accepted.
    let none = ServiceAuth::new("us-east-1".into(), fx.verifier.clone(), Arc::new(|| None));
    assert_eq!(none.secret(&id), Err(AuthError::UnknownKey));
}
