use super::*;

#[test]
fn ca_issue_and_inspect() {
    let dir = tempfile::tempdir().unwrap();
    let ca = Ca::init(dir.path(), "test CA", None, false).unwrap();
    assert!(ca.is_ca);
    assert!(ca.never_expires());
    // A second init needs --force.
    assert!(Ca::init(dir.path(), "again", None, false).is_err());

    let ca = Ca::load(dir.path()).unwrap();
    let node = ca
        .issue("node1", Role::Node, &["10.0.0.5".into()], None, false)
        .unwrap();
    assert_eq!(node.name, "node1");
    assert_eq!(node.role, Some(Role::Node));
    assert!(!node.is_ca);
    for san in ["node1", "10.0.0.5", "localhost", "127.0.0.1"] {
        assert!(node.sans.iter().any(|s| s == san), "{:?}", node.sans);
    }
    assert_eq!(node.issuer, "test CA");

    let admin = ca
        .issue("admin", Role::Admin, &[], Some(30), false)
        .unwrap();
    assert_eq!(admin.role, Some(Role::Admin));
    assert!(admin.sans.is_empty());
    assert!(!admin.never_expires());
    assert!(ca.issue("admin", Role::Admin, &[], None, false).is_err());
    assert!(ca.issue("admin", Role::Admin, &[], None, true).is_ok());

    let b = dir.path().join("node1");
    for f in ["ca.pem", "cert.pem", "key.pem"] {
        assert!(b.join(f).exists());
    }
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(b.join("key.pem"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);

    let (cas, issued) = status(dir.path()).unwrap();
    assert_eq!(cas.len(), 1);
    assert_eq!(issued.len(), 2);
}

#[test]
fn trust_and_untrust_rewrite_bundles() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    Ca::init(a.path(), "old", None, false).unwrap();
    let new = Ca::init(b.path(), "new", None, false).unwrap();
    Ca::load(a.path())
        .unwrap()
        .issue("n", Role::Node, &[], None, false)
        .unwrap();

    let pem = std::fs::read_to_string(b.path().join("ca.pem")).unwrap();
    assert_eq!(trust(a.path(), &pem).unwrap().len(), 1);
    // Idempotent.
    assert!(trust(a.path(), &pem).unwrap().is_empty());
    let leaf_bundle = std::fs::read_to_string(a.path().join("n/ca.pem")).unwrap();
    assert_eq!(read_pem_certs(&leaf_bundle).unwrap().len(), 2);
    // The signing CA is still found in a multi-CA bundle.
    assert!(Ca::load(a.path()).is_ok());

    assert!(untrust(a.path(), "old").is_err(), "own CA can't be dropped");
    assert_eq!(untrust(a.path(), &new.fingerprint).unwrap().len(), 1);
    let leaf_bundle = std::fs::read_to_string(a.path().join("n/ca.pem")).unwrap();
    assert_eq!(read_pem_certs(&leaf_bundle).unwrap().len(), 1);
}

#[test]
fn hosts_from_urls() {
    let paths = vec![
        "/data/disk".to_string(),
        "https://node1:8882/api/v1/disks/0".to_string(),
        "https://node1:8882/api/v1/disks/1".to_string(),
        "http://[::1]:8883/api/v1/disks/0".to_string(),
    ];
    assert_eq!(
        hosts_in(&paths),
        vec!["node1".to_string(), "::1".to_string()]
    );
}

#[test]
fn roles_order() {
    assert!(Role::Node > Role::Admin && Role::Admin > Role::Readonly);
    assert_eq!("admin".parse::<Role>().unwrap(), Role::Admin);
    assert!("root".parse::<Role>().is_err());
}
