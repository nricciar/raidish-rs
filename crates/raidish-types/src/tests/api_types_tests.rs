//! The JSON the API speaks: what clients written against it (the
//! dashboard, scripts) see.

use super::*;
use crate::types::fid;
use serde_json::json;

#[test]
fn entries_name_their_kind_in_lowercase() {
    let e = DirEntry {
        name: "a".into(),
        id: fid(7),
        kind: ObjType::Directory,
        size: 0,
        mode: 0o755,
        mtime_ns: 1,
    };
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["kind"], "dir");
    assert_eq!(serde_json::from_value::<DirEntry>(v).unwrap(), e);
    for k in [
        ObjType::File,
        ObjType::Directory,
        ObjType::Volume,
        ObjType::KvTree,
    ] {
        assert_eq!(serde_json::to_value(k).unwrap(), k.name());
    }
}

#[test]
fn verdicts_and_events_read_back_as_sent() {
    let v: Verdict = serde_json::from_value(json!({
        "node": "node-1", "mode": "cold",
        "reasons": [{"level": "cold", "text": "no peer I/O"}, {"level": "warn", "text": "w"}],
    }))
    .unwrap();
    assert_eq!(
        (v.mode, v.reasons[0].level),
        (Mode::Cold, ReasonLevel::Cold)
    );
    assert_eq!(v.why(), "no peer I/O");

    // An event is its report, numbered: the fields sit side by side.
    let e: Event = serde_json::from_value(json!({
        "seq": 3, "ts_ms": 9, "node": "node-0", "level": "warn",
        "kind": "volume.lost", "subject": "volume/db", "message": "m",
    }))
    .unwrap();
    assert_eq!(
        (e.seq, e.level, e.kind),
        (3, Level::Warn, EventKind::VolumeLost)
    );
    assert_eq!(e.subject, Subject::volume("db"));
    assert_eq!(e.by, None);
    let back = serde_json::to_value(&e).unwrap();
    assert_eq!(back["subject"], "volume/db");
    assert!(back.get("by").is_none());
}

#[test]
fn whoami_tells_open_nodes_from_signed_in_callers() {
    let open: Principal =
        serde_json::from_value(json!({"name": "anonymous", "role": "admin", "via": "open"}))
            .unwrap();
    assert_eq!(open.via, Via::Open);
    let tok: Principal =
        serde_json::from_value(json!({"name": "dash", "role": "readonly", "via": {"token": "ab"}}))
            .unwrap();
    assert_eq!(tok.via, Via::Token("ab".into()));
    assert!(!tok.can_write());
}
