use super::*;
use raidish_types::pki::EnrollScope;
use raidish_types::types::NodeId;

/// Cheap Argon2id for tests.
fn kdf() -> KdfParams {
    kdf_params(8, 1, 1).unwrap()
}

#[test]
fn seal_round_trip_and_tampering() {
    let key = new_key().unwrap();
    let blob = seal(&key, b"aad", b"secret").unwrap();
    assert_eq!(&open(&key, b"aad", &blob).unwrap()[..], b"secret");
    // Another AAD, another key, a flipped bit: all refused.
    assert!(open(&key, b"other", &blob).is_err());
    assert!(open(&new_key().unwrap(), b"aad", &blob).is_err());
    let mut bad = blob.clone();
    bad.ct[0] ^= 1;
    assert!(open(&key, b"aad", &bad).is_err());
    // A fresh nonce every time.
    assert_ne!(seal(&key, b"aad", b"secret").unwrap().nonce, blob.nonce);
}

#[test]
fn recovery_keys_parse_however_typed() {
    let k = new_recovery_key().unwrap();
    assert!(k.starts_with("RDK1-"));
    assert_eq!(k.len(), 4 + 8 * 9);
    let bytes = parse_recovery_key(&k).unwrap();
    assert_eq!(bytes.len(), 32);
    let sloppy = format!("  {}\n", k.to_lowercase().replace('-', " "));
    assert_eq!(parse_recovery_key(&sloppy).unwrap(), bytes);
    assert!(parse_recovery_key("RDK1-abcd").is_err());
    assert!(parse_recovery_key("hello").is_err());
}

#[test]
fn slots_unwrap_the_dek_only_with_their_key() {
    let dek = new_key().unwrap();
    let cluster = [3u8; 16];
    let (a, b) = (new_recovery_key().unwrap(), new_recovery_key().unwrap());
    let (a, b) = (
        parse_recovery_key(&a).unwrap(),
        parse_recovery_key(&b).unwrap(),
    );
    let slots = vec![
        recovery_slot(&dek, &cluster, "s1", "first", &a, kdf()).unwrap(),
        recovery_slot(&dek, &cluster, "s2", "second", &b, kdf()).unwrap(),
    ];
    // Either key opens the DEK (multi-slot: rekeying adds a slot, never
    // re-encrypts the CA).
    assert_eq!(*unwrap_dek(&slots, &cluster, &a).unwrap(), *dek);
    assert_eq!(*unwrap_dek(&slots, &cluster, &b).unwrap(), *dek);
    assert!(unwrap_dek(&slots, &cluster, &[0; 32]).is_err());
    // Slots are bound to their cluster and id.
    assert!(unwrap_dek(&slots, &[4; 16], &a).is_err());
    let mut moved = slots.clone();
    moved[0].id = "s9".into();
    moved.truncate(1);
    assert!(unwrap_dek(&moved, &cluster, &a).is_err());
}

#[test]
fn enrollment_macs_need_the_dek() {
    let dek = new_key().unwrap();
    let id = raidish_types::pki::TokenId::from_bytes([1; 8]);
    let mut e = raidish_types::pki::Enrollment {
        scope: EnrollScope::Node {
            fingerprint: "fp".into(),
            member: Some(NodeId::from_raw(1)),
            extra_sans: vec![],
            replace: false,
            compromised: false,
        },
        secret_sha256: sha256(b"secret"),
        uses: 1,
        uses_left: 1,
        expires_ms: 1,
        created_by: "admin".into(),
        mac: [0; 32],
    };
    assert!(!enrollment_mac_ok(&dek, &id, &e));
    e.mac = enrollment_mac(&dek, &id, &e);
    assert!(enrollment_mac_ok(&dek, &id, &e));
    assert!(!enrollment_mac_ok(&new_key().unwrap(), &id, &e));
    let mut forged = e.clone();
    forged.scope = EnrollScope::Client {
        profile: raidish_types::pki::ProfileKind::Admin,
        cn: "evil".into(),
    };
    assert!(!enrollment_mac_ok(&dek, &id, &forged));
}

#[test]
fn the_root_envelope_opens_with_its_recovery_key() {
    let rk = parse_recovery_key(&new_recovery_key().unwrap()).unwrap();
    let env = RootEnvelope::seal("CERT", "KEY PEM", &rk, kdf()).unwrap();
    assert_eq!(&*env.open(&rk).unwrap(), "KEY PEM");
    assert!(env.open(&[0; 32]).is_err());
    let mut swapped = env;
    swapped.cert_pem = "OTHER".into();
    assert!(swapped.open(&rk).is_err());
}

#[test]
fn subkeys_differ_by_job() {
    let k = new_key().unwrap();
    assert_ne!(*subkey(&k, "a"), *subkey(&k, "b"));
    assert_eq!(*subkey(&k, "a"), *subkey(&k, "a"));
    assert!(ct_eq(b"abc", b"abc") && !ct_eq(b"abc", b"abd") && !ct_eq(b"ab", b"abc"));
}
