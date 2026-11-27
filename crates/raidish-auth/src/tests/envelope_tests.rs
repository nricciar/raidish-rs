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
    let s = |b: &[u8]| Secret::RecoveryKey(zeroize::Zeroizing::new(b.to_vec()));
    assert_eq!(*unwrap_dek(&slots, &cluster, &s(&a)).unwrap(), *dek);
    assert_eq!(*unwrap_dek(&slots, &cluster, &s(&b)).unwrap(), *dek);
    assert!(unwrap_dek(&slots, &cluster, &s(&[0; 32])).is_err());
    // Slots are bound to their cluster and id.
    assert!(unwrap_dek(&slots, &[4; 16], &s(&a)).is_err());
    let mut moved = slots.clone();
    moved[0].id = "s9".into();
    moved.truncate(1);
    assert!(unwrap_dek(&moved, &cluster, &s(&a)).is_err());
}

#[test]
fn a_passphrase_opens_only_passphrase_slots() {
    let dek = new_key().unwrap();
    let cluster = [3u8; 16];
    let phrase = "correct horse battery";
    assert!(passphrase_slot(&dek, &cluster, "p", "", "too short", kdf()).is_err());
    assert!(passphrase_slot(&dek, &cluster, "p", "", "RDK1-but-long-enough", kdf()).is_err());
    let rk = new_recovery_key().unwrap();
    let slots = vec![
        recovery_slot(
            &dek,
            &cluster,
            "r",
            "",
            &parse_recovery_key(&rk).unwrap(),
            kdf(),
        )
        .unwrap(),
        passphrase_slot(&dek, &cluster, "p", "", phrase, kdf()).unwrap(),
    ];
    let by = |t: &str| unwrap_dek(&slots, &cluster, &Secret::parse(t).unwrap());
    assert_eq!(*by(phrase).unwrap(), *dek);
    assert_eq!(*by(&rk).unwrap(), *dek, "a recovery key, as typed");
    assert!(by("correct horse battery!").is_err());
    // Typed as RDK1-… it must be a whole key.
    assert!(Secret::parse("RDK1-12").is_err());
    // A passphrase slot whose passphrase is a recovery key's bytes isn't
    // opened by that recovery key (kinds don't mix).
    let only_pass = vec![slots[1].clone()];
    assert!(unwrap_dek(&only_pass, &cluster, &Secret::parse(&rk).unwrap()).is_err());
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

#[test]
fn a_node_key_opens_only_its_nodes_slots() {
    let dek = new_key().unwrap();
    let cluster = [3u8; 16];
    let (k1, text1) = new_node_key().unwrap();
    let (k2, _) = new_node_key().unwrap();
    assert!(text1.starts_with("RNK1-") && text1.len() == 5 + 64);
    // As written to a file or a credential: a newline, any case.
    assert_eq!(
        *parse_node_key(&format!(
            "{}\n",
            text1.to_ascii_uppercase().replace("RNK1", "rnk1")
        ))
        .unwrap(),
        *k1
    );
    assert!(parse_node_key("RDK1-00").is_err());
    let (n1, n2) = (NodeId::from_raw(1), NodeId::from_raw(2));
    let rk = new_recovery_key().unwrap();
    let slots = vec![
        recovery_slot(
            &dek,
            &cluster,
            "r",
            "",
            &parse_recovery_key(&rk).unwrap(),
            kdf(),
        )
        .unwrap(),
        node_key_slot(&dek, &cluster, "nk-2", "", n2, &k2).unwrap(),
        node_key_slot(&dek, &cluster, "nk-1", "", n1, &k1).unwrap(),
    ];
    assert_eq!(*unwrap_dek_node(&slots, &cluster, n1, &k1).unwrap(), *dek);
    assert_eq!(*unwrap_dek_node(&slots, &cluster, n2, &k2).unwrap(), *dek);
    // Node 2's key isn't tried on node 1's slot, nor the other way.
    assert!(unwrap_dek_node(&slots, &cluster, n1, &k2).is_err());
    // Another cluster's slots: no.
    assert!(unwrap_dek_node(&slots, &[4u8; 16], n1, &k1).is_err());
    // A slot renamed to another node in Raft opens for no one.
    let mut moved = slots[2].clone();
    moved.kind = SlotKind::NodeKey { node: n2 };
    assert!(unwrap_dek_node(&[moved.clone()], &cluster, n2, &k1).is_err());
    assert!(unwrap_dek_node(&[moved], &cluster, n1, &k1).is_err());
    // A recovery key or passphrase opens no node-key slot.
    assert!(unwrap_dek(&slots[1..], &cluster, &Secret::parse(&rk).unwrap()).is_err());
}
