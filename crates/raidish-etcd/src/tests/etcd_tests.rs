//! The etcd keyspace (`store`) on a test pool: revisions, history,
//! transactions, compaction, leases, events, reloads.

use super::pb::etcdserverpb as pb;
use super::store::{EtcdError, EtcdStore, KeyRange};
use crate::dataset::DatasetFormat;
use crate::fs::FileSystem;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::vdev::Pool;
use pb::request_op::Request;
use std::sync::Arc;

async fn store(pool: &TestPool) -> (Arc<FileSystem<Pool>>, Arc<EtcdStore<Pool>>) {
    let fs = Arc::new(pool.format().await);
    fs.create_service(&ds("e"), DatasetFormat::Etcd, Default::default())
        .await
        .unwrap();
    let s = EtcdStore::open(fs.clone(), &ds("e"), 7, 1).await.unwrap();
    (fs, s)
}

fn put(k: &str, v: &str) -> pb::PutRequest {
    pb::PutRequest {
        key: k.into(),
        value: v.into(),
        ..Default::default()
    }
}

fn get(k: &str) -> pb::RangeRequest {
    pb::RangeRequest {
        key: k.into(),
        ..Default::default()
    }
}

fn prefix(k: &str) -> pb::RangeRequest {
    let end = crate::kv::prefix_end(k.as_bytes()).unwrap();
    pb::RangeRequest {
        key: k.into(),
        range_end: end,
        ..Default::default()
    }
}

fn keys(r: &pb::RangeResponse) -> Vec<String> {
    r.kvs
        .iter()
        .map(|kv| String::from_utf8(kv.key.clone()).unwrap())
        .collect()
}

fn value(r: &pb::RangeResponse) -> Option<String> {
    r.kvs
        .first()
        .map(|kv| String::from_utf8(kv.value.clone()).unwrap())
}

#[tokio::test]
async fn revisions_versions_and_history() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    // An empty keyspace is at revision 1.
    let r = s.range(get("a")).await.unwrap();
    assert_eq!((r.header.unwrap().revision, r.count), (1, 0));
    let p = s.put(put("a", "1")).await.unwrap();
    assert_eq!(p.header.unwrap().revision, 2);
    s.put(put("b", "x")).await.unwrap(); // 3
    let p = s
        .put(pb::PutRequest {
            prev_kv: true,
            ..put("a", "2")
        })
        .await
        .unwrap(); // 4
    assert_eq!(p.prev_kv.unwrap().value, b"1");
    let r = s.range(get("a")).await.unwrap();
    let kv = &r.kvs[0];
    assert_eq!((kv.create_revision, kv.mod_revision, kv.version), (2, 4, 2));
    assert_eq!(value(&r).unwrap(), "2");
    // Back in time.
    let at = |rev| pb::RangeRequest {
        revision: rev,
        ..get("a")
    };
    assert_eq!(value(&s.range(at(2)).await.unwrap()).unwrap(), "1");
    assert_eq!(value(&s.range(at(3)).await.unwrap()).unwrap(), "1");
    assert_eq!(s.range(at(1)).await.unwrap().count, 0);
    assert!(matches!(s.range(at(5)).await, Err(EtcdError::FutureRev)));
    // Deleted: gone now, there before.
    let d = s
        .delete_range(pb::DeleteRangeRequest {
            key: "a".into(),
            prev_kv: true,
            ..Default::default()
        })
        .await
        .unwrap(); // 5
    assert_eq!((d.deleted, d.prev_kvs.len()), (1, 1));
    assert_eq!(d.header.unwrap().revision, 5);
    assert_eq!(s.range(get("a")).await.unwrap().count, 0);
    assert_eq!(value(&s.range(at(4)).await.unwrap()).unwrap(), "2");
    // Deleting nothing doesn't make a revision.
    let d = s
        .delete_range(pb::DeleteRangeRequest {
            key: "zzz".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!((d.deleted, d.header.unwrap().revision), (0, 5));
    // Recreated: a new create revision and version 1.
    s.put(put("a", "3")).await.unwrap(); // 6
    let kv = s.range(get("a")).await.unwrap().kvs[0].clone();
    assert_eq!((kv.create_revision, kv.mod_revision, kv.version), (6, 6, 1));
    // Keys with bytes the history escapes.
    s.put(pb::PutRequest {
        key: b"n\0ul\0".to_vec(),
        value: b"z".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let r = s
        .range(pb::RangeRequest {
            key: b"n\0ul\0".to_vec(),
            revision: 7,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(r.kvs[0].value, b"z");
}

#[tokio::test]
async fn ranges_limits_sorts_and_filters() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    for i in 0..30 {
        s.put(put(&format!("dir/{:02}", i), &format!("v{}", 29 - i)))
            .await
            .unwrap();
    }
    s.put(put("dir0", "outside")).await.unwrap();
    s.put(put("dir/05", "again")).await.unwrap();
    let r = s.range(prefix("dir/")).await.unwrap();
    assert_eq!((r.count, r.kvs.len(), r.more), (30, 30, false));
    let r = s
        .range(pb::RangeRequest {
            limit: 7,
            ..prefix("dir/")
        })
        .await
        .unwrap();
    assert_eq!((r.count, r.kvs.len(), r.more), (30, 7, true));
    assert_eq!(keys(&r)[6], "dir/06");
    // Every key from one on ("\0"), and every key ("\0" to "\0").
    let all = s
        .range(pb::RangeRequest {
            key: b"dir/29".to_vec(),
            range_end: vec![0],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(keys(&all), vec!["dir/29", "dir0"]);
    let all = s
        .range(pb::RangeRequest {
            key: vec![0],
            range_end: vec![0],
            count_only: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!((all.count, all.kvs.len()), (31, 0));
    // Sorted descending by key, by value; keys only.
    let r = s
        .range(pb::RangeRequest {
            sort_order: 2,
            limit: 2,
            keys_only: true,
            ..prefix("dir/")
        })
        .await
        .unwrap();
    assert_eq!(keys(&r), vec!["dir/29", "dir/28"]);
    assert!(r.kvs[0].value.is_empty());
    let r = s
        .range(pb::RangeRequest {
            sort_target: 4,
            sort_order: 1,
            limit: 3,
            ..prefix("dir/")
        })
        .await
        .unwrap();
    assert_eq!(keys(&r), vec!["dir/05", "dir/29", "dir/28"]);
    // Modified at the last revision only.
    let r = s
        .range(pb::RangeRequest {
            min_mod_revision: 33,
            ..prefix("dir/")
        })
        .await
        .unwrap();
    assert_eq!(keys(&r), vec!["dir/05"]);
    // A range at an old revision.
    let r = s
        .range(pb::RangeRequest {
            revision: 11,
            ..prefix("dir/")
        })
        .await
        .unwrap();
    assert_eq!(r.count, 10);
    assert_eq!(r.kvs[5].value, b"v24");
    assert!(matches!(
        s.range(pb::RangeRequest::default()).await,
        Err(EtcdError::EmptyKey)
    ));
}

#[tokio::test]
async fn transactions() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    s.put(put("lock", "free")).await.unwrap(); // 2
    let cas = |want: &str, to: &str| pb::TxnRequest {
        compare: vec![pb::Compare {
            result: 0, // equal
            target: 3, // value
            key: "lock".into(),
            target_union: Some(pb::compare::TargetUnion::Value(want.into())),
            ..Default::default()
        }],
        success: vec![
            pb::RequestOp {
                request: Some(Request::RequestPut(put("lock", to))),
            },
            pb::RequestOp {
                request: Some(Request::RequestPut(put("owner", to))),
            },
            pb::RequestOp {
                request: Some(Request::RequestRange(get("owner"))),
            },
        ],
        failure: vec![pb::RequestOp {
            request: Some(Request::RequestRange(get("lock"))),
        }],
    };
    let t = s.txn(cas("free", "me")).await.unwrap();
    assert!(t.succeeded);
    // One revision for all of it; the range saw the put before it.
    assert_eq!(t.header.unwrap().revision, 3);
    let Some(pb::response_op::Response::ResponseRange(r)) = &t.responses[2].response else {
        panic!()
    };
    assert_eq!(value(r).unwrap(), "me");
    assert_eq!(r.kvs[0].mod_revision, 3);
    let owner = s.range(get("owner")).await.unwrap();
    assert_eq!(owner.kvs[0].mod_revision, 3);
    // Taken: the failure branch (a read) runs.
    let t = s.txn(cas("free", "you")).await.unwrap();
    assert!(!t.succeeded);
    assert_eq!(t.header.unwrap().revision, 3);
    // Missing keys: version 0, and a value compare is false.
    let absent = |target: i32, union| pb::TxnRequest {
        compare: vec![pb::Compare {
            result: 0,
            target,
            key: "nope".into(),
            target_union: Some(union),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        s.txn(absent(0, pb::compare::TargetUnion::Version(0)))
            .await
            .unwrap()
            .succeeded
    );
    assert!(
        !s.txn(absent(3, pb::compare::TargetUnion::Value(vec![])))
            .await
            .unwrap()
            .succeeded
    );
    // A key put twice, or put and deleted, in a branch is refused.
    let dup = pb::TxnRequest {
        success: vec![
            pb::RequestOp {
                request: Some(Request::RequestPut(put("x", "1"))),
            },
            pb::RequestOp {
                request: Some(Request::RequestDeleteRange(pb::DeleteRangeRequest {
                    key: "a".into(),
                    range_end: "z".into(),
                    ..Default::default()
                })),
            },
        ],
        ..Default::default()
    };
    assert!(matches!(s.txn(dup).await, Err(EtcdError::DuplicateKey)));
    // A failing op undoes the whole transaction.
    let bad = pb::TxnRequest {
        success: vec![
            pb::RequestOp {
                request: Some(Request::RequestPut(put("y", "1"))),
            },
            pb::RequestOp {
                request: Some(Request::RequestPut(pb::PutRequest {
                    lease: 12345,
                    ..put("z", "1")
                })),
            },
        ],
        ..Default::default()
    };
    assert!(matches!(s.txn(bad).await, Err(EtcdError::LeaseNotFound)));
    assert_eq!(s.range(get("y")).await.unwrap().count, 0);
    assert_eq!(s.revisions().await.unwrap().0, 3);
    // Nested.
    let nested = pb::TxnRequest {
        success: vec![pb::RequestOp {
            request: Some(Request::RequestTxn(pb::TxnRequest {
                success: vec![pb::RequestOp {
                    request: Some(Request::RequestPut(put("deep", "1"))),
                }],
                ..Default::default()
            })),
        }],
        ..Default::default()
    };
    assert_eq!(s.txn(nested).await.unwrap().header.unwrap().revision, 4);
    assert_eq!(value(&s.range(get("deep")).await.unwrap()).unwrap(), "1");
}

/// How many history entries the tree holds.
async fn history_len(fs: &FileSystem<Pool>) -> usize {
    let ds = fs.open_btree(&ds("e"), DatasetFormat::Etcd).await.unwrap();
    let t = crate::btree::Txn::begin(fs, &ds.obj).await.unwrap();
    t.range_raw(
        std::ops::Bound::Included(&b"H"[..]),
        std::ops::Bound::Excluded(&b"I"[..]),
        usize::MAX,
    )
    .await
    .unwrap()
    .len()
}

#[tokio::test]
async fn compaction() {
    let pool = TestPool::new().await;
    let (fs, s) = store(&pool).await;
    for round in 0..5 {
        for k in 0..20 {
            s.put(put(&format!("k{}", k), &format!("{}", round)))
                .await
                .unwrap();
        }
    }
    // 2..=101; then delete half.
    s.delete_range(pb::DeleteRangeRequest {
        key: "k1".into(),
        range_end: "k2".into(),
        ..Default::default()
    })
    .await
    .unwrap(); // 102: k1, k10..k19
    assert_eq!(history_len(&fs).await, 100 + 11);
    s.compact(81, true).await.unwrap();
    // Kept: each key's value as of 81 (one each), and everything after.
    // The deleted keys keep only their tombstone's past at 81 and newer.
    // The key changed at 81 itself keeps the change before it too (its
    // event's `prev_kv`, audit E2).
    assert_eq!(history_len(&fs).await, 20 + 20 + 11 + 1);
    let at = |rev| pb::RangeRequest {
        revision: rev,
        ..prefix("k")
    };
    assert!(matches!(s.range(at(80)).await, Err(EtcdError::Compacted)));
    let r = s.range(at(81)).await.unwrap();
    assert_eq!(r.count, 20);
    assert_eq!(s.range(prefix("k")).await.unwrap().count, 9);
    assert!(matches!(
        s.compact(81, false).await,
        Err(EtcdError::Compacted)
    ));
    assert!(matches!(
        s.compact(500, false).await,
        Err(EtcdError::FutureRev)
    ));
    // Compacting at the delete keeps the deleted keys' tombstones and what
    // they deleted (the delete events' `prev_kv`)...
    s.compact(102, true).await.unwrap();
    assert_eq!(history_len(&fs).await, 9 + 2 * 11);
    assert_eq!(s.range(prefix("k")).await.unwrap().count, 9);
    let r = s.range(at(102)).await.unwrap();
    assert_eq!(r.count, 9);
    // Events below the compaction are gone.
    let all = KeyRange::new(b"\0", b"\0");
    assert!(matches!(
        s.events(&all, 101, 102, false, 100).await,
        Err(EtcdError::Compacted)
    ));
    let (ev, upto) = s.events(&all, 102, 102, true, 100).await.unwrap();
    assert_eq!((ev.len(), upto), (11, 102));
    assert!(ev.iter().all(|e| e.prev_kv.is_some()));
    // ...and compacting past it drops them entirely.
    s.put(put("z", "1")).await.unwrap(); // 103
    s.compact(103, true).await.unwrap();
    assert_eq!(history_len(&fs).await, 9 + 1);
    assert_eq!(s.range(prefix("k")).await.unwrap().count, 9);
}

#[tokio::test]
async fn leases_attach_revoke_and_expire() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    let g = s
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap();
    assert!(g.id > 0);
    assert!(matches!(
        s.lease_grant(pb::LeaseGrantRequest { ttl: 60, id: g.id })
            .await,
        Err(EtcdError::LeaseExists)
    ));
    for k in ["l/a", "l/b"] {
        s.put(pb::PutRequest {
            lease: g.id,
            ..put(k, "1")
        })
        .await
        .unwrap();
    }
    // Moved to no lease: not revoked with it.
    s.put(put("l/b", "2")).await.unwrap();
    let ttl = s
        .lease_time_to_live(pb::LeaseTimeToLiveRequest {
            id: g.id,
            keys: true,
        })
        .await
        .unwrap();
    assert_eq!(ttl.keys, vec![b"l/a".to_vec()]);
    assert_eq!(ttl.granted_ttl, 60);
    assert!(ttl.ttl > 50);
    assert_eq!(s.lease_leases().await.unwrap().leases.len(), 1);
    let before = s.revisions().await.unwrap().0;
    s.lease_revoke(g.id).await.unwrap();
    assert_eq!(s.revisions().await.unwrap().0, before + 1);
    assert_eq!(keys(&s.range(prefix("l/")).await.unwrap()), vec!["l/b"]);
    assert!(matches!(
        s.lease_revoke(g.id).await,
        Err(EtcdError::LeaseNotFound)
    ));
    let gone = s
        .lease_time_to_live(pb::LeaseTimeToLiveRequest {
            id: g.id,
            keys: false,
        })
        .await
        .unwrap();
    assert_eq!(gone.ttl, -1);
    assert_eq!(s.lease_keep_alive(g.id).await.unwrap().ttl, 0);
    // Expiry, kept off by keepalives until they stop.
    let short = s
        .lease_grant(pb::LeaseGrantRequest { ttl: 1, id: 99 })
        .await
        .unwrap();
    assert_eq!((short.id, short.ttl), (99, super::store::MIN_LEASE_TTL));
    s.put(pb::PutRequest {
        lease: 99,
        ..put("eph", "1")
    })
    .await
    .unwrap();
    for _ in 0..6 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(s.lease_keep_alive(99).await.unwrap().ttl, 2);
    }
    assert_eq!(s.range(get("eph")).await.unwrap().count, 1);
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert_eq!(s.range(get("eph")).await.unwrap().count, 0);
    assert_eq!(s.lease_leases().await.unwrap().leases.len(), 0);
}

#[tokio::test]
async fn events_carry_history() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    s.put(put("w/a", "1")).await.unwrap(); // 2
    s.put(put("other", "1")).await.unwrap(); // 3
    s.put(put("w/a", "2")).await.unwrap(); // 4
    s.delete_range(pb::DeleteRangeRequest {
        key: "w/a".into(),
        ..Default::default()
    })
    .await
    .unwrap(); // 5
    let kr = KeyRange::new(b"w/", b"w0");
    let (ev, upto) = s.events(&kr, 2, 5, true, 100).await.unwrap();
    assert_eq!(upto, 5);
    assert_eq!(ev.len(), 3);
    assert_eq!(ev[0].r#type, 0);
    assert!(ev[0].prev_kv.is_none());
    assert_eq!(ev[1].kv.as_ref().unwrap().value, b"2");
    assert_eq!(ev[1].prev_kv.as_ref().unwrap().value, b"1");
    assert_eq!(ev[2].r#type, 1);
    assert_eq!(ev[2].kv.as_ref().unwrap().mod_revision, 5);
    assert_eq!(ev[2].prev_kv.as_ref().unwrap().value, b"2");
    // A cap stops at a whole revision.
    let (ev, upto) = s.events(&kr, 2, 5, false, 1).await.unwrap();
    assert_eq!((ev.len(), upto), (1, 2));
}

#[tokio::test]
async fn survives_a_reload() {
    let pool = TestPool::new().await;
    {
        let (_fs, s) = store(&pool).await;
        for i in 0..50 {
            s.put(put(&format!("k{}", i), "v")).await.unwrap();
        }
        s.lease_grant(pb::LeaseGrantRequest { ttl: 100, id: 5 })
            .await
            .unwrap();
        s.stop();
    }
    let fs = Arc::new(pool.load().await);
    let s = EtcdStore::open(fs.clone(), &ds("e"), 7, 2).await.unwrap();
    assert_eq!(s.range(prefix("k")).await.unwrap().count, 50);
    assert_eq!(s.revisions().await.unwrap().0, 51);
    assert_eq!(s.lease_keep_alive(5).await.unwrap().ttl, 100);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    // Only etcd datasets.
    fs.create_kv(&ds("plain")).await.unwrap();
    assert!(
        EtcdStore::open(fs.clone(), &ds("plain"), 7, 2)
            .await
            .is_err()
    );
}

/// Serve `s` over gRPC on a local port; its address.
/// Serve `s` (plain TCP: the frontend's TLS is tested elsewhere), every
/// connection's caller with `role`.
async fn serve_as(s: Arc<EtcdStore<Pool>>, role: crate::api_types::Role) -> String {
    use super::server::{Caller, EtcdConn};
    use futures::StreamExt;
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    let router = super::server::EtcdServer {
        store: s,
        node: "n1".into(),
        client_urls: vec![addr.clone()],
    }
    .router();
    let conns = tokio_stream::wrappers::TcpListenerStream::new(l)
        .map(move |c| c.map(|c| EtcdConn::new(c, Caller { role })));
    tokio::spawn(router.serve_with_incoming(conns));
    addr
}

async fn serve(s: Arc<EtcdStore<Pool>>) -> String {
    serve_as(s, crate::api_types::Role::Admin).await
}

#[tokio::test]
async fn readonly_callers_only_read() {
    use pb::kv_client::KvClient;
    use pb::lease_client::LeaseClient;
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    let rw = serve(s.clone()).await;
    let ro = serve_as(s.clone(), crate::api_types::Role::Readonly).await;
    KvClient::connect(rw)
        .await
        .unwrap()
        .put(put("k", "v"))
        .await
        .unwrap();
    let mut kv = KvClient::connect(ro.clone()).await.unwrap();
    let got = kv
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v");
    let e = kv.put(put("k", "w")).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    let e = LeaseClient::connect(ro)
        .await
        .unwrap()
        .lease_grant(pb::LeaseGrantRequest { ttl: 10, id: 0 })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn over_grpc_kv_watch_and_leases() {
    use pb::kv_client::KvClient;
    use pb::lease_client::LeaseClient;
    use pb::watch_client::WatchClient;
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    let addr = serve(s.clone()).await;
    let mut kv = KvClient::connect(addr.clone()).await.unwrap();
    kv.put(put("before", "1")).await.unwrap(); // 2

    // A watch from revision 2: history, then live events.
    let mut w = WatchClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                start_revision: 2,
                prev_kv: true,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut events = w
        .watch(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let created = events.message().await.unwrap().unwrap();
    assert!(created.created);
    let first = events.message().await.unwrap().unwrap();
    assert_eq!(first.events[0].kv.as_ref().unwrap().key, b"before");
    kv.put(put("before", "2")).await.unwrap(); // 3
    let next = events.message().await.unwrap().unwrap();
    assert_eq!(next.events[0].kv.as_ref().unwrap().mod_revision, 3);
    assert_eq!(next.events[0].prev_kv.as_ref().unwrap().value, b"1");
    // Progress: everything up to the current revision was sent.
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::ProgressRequest(
            pb::WatchProgressRequest {},
        )),
    })
    .await
    .unwrap();
    let p = events.message().await.unwrap().unwrap();
    assert_eq!((p.watch_id, p.header.unwrap().revision), (-1, 3));

    // Errors come back as etcd's.
    let e = kv
        .range(pb::RangeRequest {
            revision: 99,
            ..get("before")
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::OutOfRange);
    assert!(e.message().contains("future revision"), "{}", e.message());

    // Keepalives over a stream.
    let mut leases = LeaseClient::connect(addr.clone()).await.unwrap();
    let g = leases
        .lease_grant(pb::LeaseGrantRequest { ttl: 30, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let (ktx, krx) = tokio::sync::mpsc::channel(4);
    let mut ka = leases
        .lease_keep_alive(tokio_stream::wrappers::ReceiverStream::new(krx))
        .await
        .unwrap()
        .into_inner();
    for _ in 0..2 {
        ktx.send(pb::LeaseKeepAliveRequest { id: g.id })
            .await
            .unwrap();
        let r = ka.message().await.unwrap().unwrap();
        assert_eq!((r.id, r.ttl), (g.id, 30));
    }

    // Stopping ends the streams.
    s.stop();
    assert!(events.message().await.is_err());
}

/// E2: compacted at the revision of a delete, a watch from that revision
/// still gets the delete with the object deleted as `prev_kv`, and the key
/// stays deleted (the history kept for it doesn't bring it back). The next
/// compaction past it drops both.
#[tokio::test]
async fn compacting_at_a_change_keeps_its_prev_kv() {
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    s.put(put("w/a", "1")).await.unwrap(); // 2
    s.put(put("w/a", "2")).await.unwrap(); // 3
    s.delete_range(pb::DeleteRangeRequest {
        key: "w/a".into(),
        ..Default::default()
    })
    .await
    .unwrap(); // 4
    s.put(put("w/b", "1")).await.unwrap(); // 5
    s.compact(4, true).await.unwrap();
    let kr = KeyRange::new(b"w/", b"w0");
    let (ev, _) = s.events(&kr, 4, 5, true, 100).await.unwrap();
    assert_eq!(ev.len(), 2);
    assert_eq!(ev[0].r#type, 1, "a delete");
    assert_eq!(
        ev[0].prev_kv.as_ref().map(|kv| kv.value.as_slice()),
        Some(&b"2"[..])
    );
    assert_eq!(s.range(get("w/a")).await.unwrap().count, 0);
    s.compact(5, true).await.unwrap();
    assert_eq!(s.range(get("w/a")).await.unwrap().count, 0);
    assert_eq!(s.range(get("w/b")).await.unwrap().count, 1);
}

fn event_at_rev(rev: i64, bytes: usize) -> super::pb::mvccpb::Event {
    super::pb::mvccpb::Event {
        kv: Some(super::pb::mvccpb::KeyValue {
            key: b"k".to_vec(),
            value: vec![0; bytes],
            mod_revision: rev,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// E3: watch responses are split only between revisions, so a
/// transaction's events (one revision) arrive together however large.
#[test]
fn watch_responses_never_split_a_revision() {
    use super::server::{WATCH_BYTES, chunks};
    let third = WATCH_BYTES / 3 + 1;
    // Revision 2 is three events of over a third of the limit each.
    let events = vec![
        event_at_rev(2, third),
        event_at_rev(2, third),
        event_at_rev(2, third),
        event_at_rev(3, 10),
        event_at_rev(4, 10),
    ];
    let revs: Vec<Vec<i64>> = chunks(events)
        .iter()
        .map(|c| {
            c.iter()
                .map(|e| e.kv.as_ref().unwrap().mod_revision)
                .collect()
        })
        .collect();
    assert_eq!(revs, vec![vec![2, 2, 2], vec![3, 4]]);
    // Small revisions still share a response.
    let small = (1..=5).map(|r| event_at_rev(r, 10)).collect();
    assert_eq!(chunks(small).len(), 1);
}

/// E3: a watch created from "now" (start revision 0) on a stream that
/// was idle while the store moved on starts after the latest revision,
/// not after the one the stream last saw: it doesn't replay the put just
/// before it. The session is driven directly, on this one thread: the new
/// revision and the create are both waiting when it next runs, and its
/// select picks either first (each round is a coin flip for a stale `cur`).
#[tokio::test]
async fn a_watch_from_now_doesnt_replay_what_came_before_it() {
    use pb::watch_request::RequestUnion;
    let pool = TestPool::new().await;
    let (_fs, s) = store(&pool).await;
    let rev_now = |p: pb::PutResponse| p.header.unwrap().revision;
    let first = rev_now(s.put(put("start", "x")).await.unwrap());
    let (durable_tx, durable) = tokio::sync::watch::channel(first);
    let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
    let (out_tx, mut out) = tokio::sync::mpsc::channel(64);
    tokio::spawn(super::server::watch_session(
        s.clone(),
        tokio_stream::wrappers::ReceiverStream::new(in_rx),
        out_tx,
        durable,
    ));
    let create = |key: &str| pb::WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(pb::WatchCreateRequest {
            key: key.into(),
            ..Default::default()
        })),
    };
    let mut next = async || out.recv().await.unwrap().unwrap();
    for i in 0..30 {
        let key = format!("k{}", i);
        let before = rev_now(s.put(put(&key, "before")).await.unwrap());
        // No await between: the session sees both at once.
        durable_tx.send(before).unwrap();
        in_tx.try_send(Ok(create(&key))).unwrap();
        let created = next().await;
        assert!(created.created, "{:?}", created);
        let after = rev_now(s.put(put(&key, "after")).await.unwrap());
        durable_tx.send(after).unwrap();
        let got = next().await;
        assert_eq!(got.watch_id, created.watch_id);
        let kv = got.events[0].kv.as_ref().unwrap();
        assert_eq!(
            (kv.mod_revision, kv.value.as_slice()),
            (after, b"after".as_slice()),
            "round {}: the watch replayed a put from before it",
            i
        );
    }
}
