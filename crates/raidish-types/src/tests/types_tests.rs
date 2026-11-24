use super::*;

#[test]
fn epochs_order_and_advance() {
    assert!(Epoch::NONE < Epoch::NONE.next());
    assert!(Epoch::NONE.is_none());
    assert_eq!(Epoch::from_raw(7).next().get(), 8);
    assert_eq!("12".parse::<Epoch>().unwrap(), Epoch::from_raw(12));
    assert!("x".parse::<Epoch>().is_err());
    let e = Epoch::from_raw(0x0102);
    assert_eq!(Epoch::from_le_bytes(e.to_le_bytes()), e);
}

#[test]
fn dataset_names() {
    for ok in [
        "a",
        "vol-1",
        "pvc-0c1d2e3f.data_x",
        "_x",
        &"a".repeat(MAX_DATASET_NAME),
    ] {
        assert!(DatasetName::user(ok).is_ok(), "{}", ok);
    }
    for bad in [
        "",
        ".",
        "..",
        ".hidden",
        "-x",
        "a/b",
        "a b",
        "a@b",
        "a:b",
        "a+b",
        &"a".repeat(MAX_DATASET_NAME + 1),
    ] {
        assert!(DatasetName::user(bad).is_err(), "{}", bad);
    }
    // The pool's own: valid names, but not ones a user can pick.
    let sys = DatasetName::try_from(".vms").unwrap();
    assert!(sys.is_system());
    assert!(DatasetName::user(".vms").is_err());
    assert!(DatasetName::try_from("..vms").is_err());
}

#[test]
fn names_check_when_deserialized() {
    let n: DatasetName = serde_json::from_str("\"vol\"").unwrap();
    assert_eq!(n, "vol");
    assert!(serde_json::from_str::<DatasetName>("\"a/b\"").is_err());
    assert_eq!(serde_json::to_string(&n).unwrap(), "\"vol\"");
}

#[test]
fn vm_names() {
    assert!(VmName::try_from("web-1.prod_x").is_ok());
    for bad in ["", ".hidden", "a/b", "a b", &"v".repeat(MAX_VM_NAME + 1)] {
        assert!(VmName::try_from(bad).is_err(), "{}", bad);
    }
}

#[test]
fn snapshot_names() {
    assert!(SnapshotName::try_from("2026-11-18T10:00").is_ok());
    assert!(SnapshotName::try_from("_auto.1").is_ok());
    for bad in ["", "-rf", ".", "..", "a b", "a/b", &"s".repeat(129)] {
        assert!(SnapshotName::try_from(bad).is_err(), "{}", bad);
    }
}

#[test]
fn system_dataset_names_count_the_dot() {
    let max = "d".repeat(MAX_DATASET_NAME);
    assert!(DatasetName::try_from(max.as_str()).is_ok());
    assert!(DatasetName::try_from(format!(".{}", &max[1..]).as_str()).is_ok());
    assert!(DatasetName::try_from(format!(".{}", max).as_str()).is_err());
}

#[test]
fn extent_end_saturates() {
    assert_eq!(
        Extent {
            start: u64::MAX - 1,
            len: 5
        }
        .end(),
        u64::MAX
    );
}

#[test]
fn snapshot_refs() {
    let r: SnapshotRef = "vol@2026-11-18T10:00".parse().unwrap();
    assert_eq!(r.volume, "vol");
    assert_eq!(r.snap, "2026-11-18T10:00");
    assert_eq!(r.to_string(), "vol@2026-11-18T10:00");
    for bad in ["vol", "@s", "vol@", "a/b@s", "vol@s s"] {
        assert!(bad.parse::<SnapshotRef>().is_err(), "{}", bad);
    }
}

#[test]
fn service_names_are_dataset_names() {
    for ok in ["my-bucket", "a.b.c", "b12"] {
        let b = BucketName::try_from(ok).unwrap();
        assert!(DatasetName::user(&b).is_ok());
    }
    for bad in ["ab", "Upper", "a..b", "-ab", "ab-", "192.168.0.1"] {
        assert!(BucketName::try_from(bad).is_err(), "{}", bad);
    }
    assert!(QueueName::try_from("jobs_1-x").is_ok());
    for bad in ["", "q.fifo", "-q", "a.b", &"q".repeat(81)] {
        assert!(QueueName::try_from(bad).is_err(), "{}", bad);
    }
}

#[test]
fn lease_resources_round_trip() {
    for r in [
        LeaseResource::Pool,
        LeaseResource::Nfs4,
        LeaseResource::dataset("vol-1"),
        LeaseResource::node("node-2"),
        LeaseResource::writer("node-2"),
        LeaseResource::Object(FileId::from_raw(42)),
    ] {
        let s = r.to_string();
        assert_eq!(s.parse::<LeaseResource>().unwrap(), r, "{}", s);
        let j = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<LeaseResource>(&j).unwrap(), r);
    }
    for bad in ["", "vol/x", "ds/", "obj/x", "pools"] {
        assert!(bad.parse::<LeaseResource>().is_err(), "{}", bad);
    }
    // As map keys too (the cluster state's lease table).
    let m: std::collections::BTreeMap<LeaseResource, u8> =
        [(LeaseResource::dataset("a"), 1)].into_iter().collect();
    let j = serde_json::to_string(&m).unwrap();
    assert_eq!(j, r#"{"ds/a":1}"#);
    assert_eq!(
        serde_json::from_str::<std::collections::BTreeMap<LeaseResource, u8>>(&j).unwrap(),
        m
    );
}

#[test]
fn holders_describe_themselves() {
    let h = Holder::new(HolderRole::Agent, Some("node-1")).with_addr("10.0.0.1:443");
    assert_eq!(h.to_string(), "agent node-1 @10.0.0.1:443");
    assert_eq!(h.node_as(HolderRole::Agent), Some("node-1"));
    assert_eq!(h.node_as(HolderRole::Nfs), None);
}
