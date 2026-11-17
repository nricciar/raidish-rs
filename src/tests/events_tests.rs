//! The activity log: appending, reading pages, keeping what the committer
//! saw, pruning; the committer's diff of the cluster state; a node's sink.

use super::*;
use crate::cluster::state::{ClusterState, Lease};
use crate::test_util::TestPool;

fn ev(node: &str, msg: &str) -> NewEvent {
    NewEvent {
        ts_ms: crate::fs::now_ms(),
        node: node.into(),
        level: Level::Info,
        kind: "test".into(),
        subject: Some("x/y".into()),
        message: msg.into(),
        by: None,
    }
}

fn msgs(p: &EventPage) -> Vec<String> {
    p.events.iter().map(|e| e.event.message.clone()).collect()
}

#[tokio::test]
async fn append_read_and_reopen() {
    let pool = TestPool::new().await;
    {
        let fs = Arc::new(pool.format().await);
        let log = EventLog::open(fs.clone()).await.unwrap();
        assert_eq!(log.read(&EventQuery::default()).await.unwrap().next, 1);
        let seqs = log
            .append(
                (0..10).map(|i| ev("n1", &format!("e{}", i))).collect(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seqs, (1..=10).collect::<Vec<_>>());
        // Not a dataset users see.
        assert!(fs.list_btree_datasets().await.unwrap().is_empty());
        let seen = Observed {
            leader: Some(1),
            ..Default::default()
        };
        log.append(vec![ev("n2", "e10")], Some(&seen))
            .await
            .unwrap();
    }
    // Committed: a new committer finds it all.
    let fs = Arc::new(pool.load().await);
    let log = EventLog::open(fs.clone()).await.unwrap();
    assert_eq!(log.seen().await.unwrap().unwrap().leader, Some(1));
    // The newest, oldest first.
    let p = log
        .read(&EventQuery {
            limit: Some(3),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(msgs(&p), vec!["e8", "e9", "e10"]);
    assert_eq!((p.next, p.first), (12, 1));
    assert_eq!(p.events[2].event.node, "n2");
    // Older ones, a page before.
    let p = log
        .read(&EventQuery {
            before: Some(9),
            limit: Some(3),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(msgs(&p), vec!["e5", "e6", "e7"]);
    // What's new since: nothing, then one more.
    let after = Some(11);
    let q = EventQuery {
        after,
        ..Default::default()
    };
    assert!(log.read(&q).await.unwrap().events.is_empty());
    log.append(vec![ev("n1", "e11")], None).await.unwrap();
    assert_eq!(msgs(&log.read(&q).await.unwrap()), vec!["e11"]);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn old_events_go() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let log = EventLog::open(fs).await.unwrap();
    let mut old = ev("n1", "ancient");
    old.ts_ms -= MAX_AGE_MS + 1000;
    log.append(vec![old.clone(), old], None).await.unwrap();
    // Each append prunes: the two old ones go, the new one stays.
    log.append(vec![ev("n1", "new")], None).await.unwrap();
    let p = log.read(&EventQuery::default()).await.unwrap();
    assert_eq!(msgs(&p), vec!["new"]);
    assert_eq!((p.first, p.next), (3, 4));
}

fn lease(owner_node: &str, epoch: u64, live: bool, released: bool) -> Lease {
    Lease {
        owner: format!("id-{}", owner_node),
        desc: format!("agent {} @{}:1", owner_node, owner_node),
        epoch,
        expires_ms: if live { 2_000 } else { 500 },
        released,
    }
}

fn state(leases: &[(&str, Lease)]) -> ClusterState {
    ClusterState {
        leases: leases
            .iter()
            .map(|(r, l)| (r.to_string(), l.clone()))
            .collect(),
        ..Default::default()
    }
}

const NOW: u64 = 1_000;

fn me(node: &str, epoch: u64) -> Held {
    Held {
        node: node.into(),
        epoch,
    }
}

fn kinds(v: &[Noticed]) -> Vec<(&'static str, String)> {
    v.iter().map(|(_, k, s, _)| (*k, s.clone())).collect()
}

fn label(_: &str) -> String {
    "Volume".into()
}

#[test]
fn a_new_log_starts_without_replaying_the_cluster() {
    let s = state(&[("node/a", lease("a", 1, true, false))]);
    let new = observe(&s, NOW, me("a", 5), None);
    assert_eq!(
        kinds(&diff(None, &new, &s, &label)),
        vec![("events.started", "pool".to_string())]
    );
}

#[test]
fn nodes_and_datasets_coming_and_going() {
    let s0 = state(&[
        ("node/a", lease("a", 1, true, false)),
        ("node/b", lease("b", 2, true, false)),
        ("node/c", lease("c", 3, true, false)),
        ("ds/v1", lease("b", 4, true, false)),
        ("ds/v2", lease("c", 5, true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("a", 9), Some(1));
    assert_eq!(o0.datasets["v1"], me("b", 4));
    // b went quiet (and v1 with it); c shut down cleanly (and let go of
    // v2: its agent says so); d is new; a restarted.
    let s1 = state(&[
        ("node/a", lease("a", 6, true, false)),
        ("node/b", lease("b", 2, false, false)),
        ("node/c", lease("c", 3, false, true)),
        ("node/d", lease("d", 7, true, false)),
        ("ds/v1", lease("b", 4, false, false)),
        ("ds/v2", lease("c", 5, false, true)),
    ]);
    let o1 = observe(&s1, NOW, me("a", 9), Some(4));
    let mut got = kinds(&diff(Some(&o0), &o1, &s1, &label));
    got.sort();
    assert_eq!(
        got,
        vec![
            ("dataset.lost", "volume/v1".to_string()),
            ("node.down", "node/b".to_string()),
            ("node.left", "node/c".to_string()),
            ("node.restarted", "node/a".to_string()),
            ("node.up", "node/d".to_string()),
            ("raft.leader", "cluster".to_string()),
        ]
    );
    // Nothing changed: nothing to say.
    assert!(diff(Some(&o1), &o1, &s1, &label).is_empty());
    // b comes back.
    let s2 = state(&[
        ("node/a", lease("a", 6, true, false)),
        ("node/b", lease("b", 8, true, false)),
        ("node/c", lease("c", 3, false, true)),
        ("node/d", lease("d", 7, true, false)),
    ]);
    let o2 = observe(&s2, NOW, me("a", 9), Some(4));
    assert_eq!(
        kinds(&diff(Some(&o1), &o2, &s2, &label)),
        vec![("node.up", "node/b".to_string())]
    );
}

#[test]
fn a_live_node_reports_its_own_lost_leases() {
    let s0 = state(&[
        ("node/b", lease("b", 2, true, false)),
        ("ds/v1", lease("b", 4, true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("a", 9), None);
    let s1 = state(&[
        ("node/b", lease("b", 2, true, false)),
        ("ds/v1", lease("b", 4, false, false)),
    ]);
    let o1 = observe(&s1, NOW, me("a", 9), None);
    assert!(diff(Some(&o0), &o1, &s1, &label).is_empty());
}

#[test]
fn failover_between_looks_and_committer_takeovers() {
    let s0 = state(&[
        ("node/a", lease("a", 1, true, false)),
        ("node/b", lease("b", 2, true, false)),
        ("ds/v1", lease("b", 4, true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("b", 3), None);
    // b died, and a took over both v1 and the committer, all while no
    // committer looked.
    let s1 = state(&[
        ("node/a", lease("a", 1, true, false)),
        ("node/b", lease("b", 2, false, false)),
        ("ds/v1", lease("a", 10, true, false)),
    ]);
    let o1 = observe(&s1, NOW, me("a", 11), None);
    let d = diff(Some(&o0), &o1, &s1, &label);
    let mut got = kinds(&d);
    got.sort();
    assert_eq!(
        got,
        vec![
            ("committer.moved", "node/a".to_string()),
            ("dataset.failover", "volume/v1".to_string()),
            ("node.down", "node/b".to_string()),
        ]
    );
    assert!(
        d.iter()
            .any(|(_, _, _, m)| m == "a took over as committer from b")
    );
}

#[test]
fn sink_reports_repeats_once_and_keeps_what_it_cant_send() {
    let sink = EventSink::new("n1");
    assert!(sink.once("vm/x", Level::Error, "vm.start_failed", "vm/x", "boom"));
    assert!(!sink.once("vm/x", Level::Error, "vm.start_failed", "vm/x", "boom"));
    assert!(sink.once("vm/x", Level::Error, "vm.start_failed", "vm/x", "other"));
    assert!(sink.clear("vm/x"));
    assert!(sink.once("vm/x", Level::Error, "vm.start_failed", "vm/x", "other"));
    assert_eq!(sink.pending(), 3);
    // Past its cap, the oldest go, and a note says how many.
    for i in 0..SINK_CAP {
        sink.info("t", "s", format!("{}", i));
    }
    assert_eq!(sink.pending(), SINK_CAP);
    let batch = sink.take(2);
    assert_eq!(batch[0].kind, "events.dropped");
    assert!(batch[0].message.starts_with("3 events"));
    assert_eq!(batch[1].message, "0");
    sink.put_back(batch);
    assert_eq!(sink.take(1)[0].kind, "events.dropped");
}

#[tokio::test]
async fn sink_retries_until_sent() {
    let sink = EventSink::new("n1");
    sink.info("t", "s", "hello");
    let got = Arc::new(std::sync::Mutex::new(Vec::<NewEvent>::new()));
    let tries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (g, t) = (got.clone(), tries.clone());
    let task = tokio::spawn(sink.clone().run(move |batch| {
        let (g, t) = (g.clone(), t.clone());
        async move {
            if t.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                return Err("no committer".to_string());
            }
            g.lock().unwrap().extend(batch);
            Ok(())
        }
    }));
    for _ in 0..100 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    task.abort();
    let got = got.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "hello");
    assert_eq!(got[0].node, "n1");
}

#[test]
fn byte_sizes() {
    assert_eq!(bytes(512), "512 B");
    assert_eq!(bytes(1 << 30), "1 GiB");
    assert_eq!(bytes(1536 << 20), "1.5 GiB");
}

#[tokio::test]
async fn the_committer_records_what_it_sees() {
    use crate::cluster::LeaseOp;
    use crate::committer::{Authority, Committer, LocalAuthority};
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let acquire = |r: &str, node: &str| LeaseOp::Acquire {
        resource: r.into(),
        owner: format!("id-{}", node),
        desc: format!("agent {}", node),
        ttl_ms: 600_000,
    };
    auth.propose(acquire("pool", "n0")).await.unwrap();
    auth.propose(acquire("node/n0", "n0")).await.unwrap();
    let c = Committer::new(fs.clone(), auth.clone(), "id-n0".into(), 1, "n0".into()).await;
    c.observe().await.unwrap();
    auth.propose(acquire("node/n1", "n1")).await.unwrap();
    c.observe().await.unwrap();
    // Unchanged: nothing more.
    c.observe().await.unwrap();
    let log = c.events().await.unwrap();
    let p = log.read(&EventQuery::default()).await.unwrap();
    let k: Vec<&str> = p.events.iter().map(|e| e.event.kind.as_str()).collect();
    assert_eq!(k, vec!["events.started", "node.up"]);
    assert_eq!(p.events[1].event.message, "Node n1 joined the cluster");
    // What nodes report, and changes asked of it.
    c.record(vec![ev("n1", "from n1")]).await.unwrap();
    c.note(
        Level::Info,
        "volume.created",
        "volume/v".into(),
        "Volume 'v' created".into(),
        Some("dave".into()),
    )
    .await;
    let p = log
        .read(&EventQuery {
            after: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(msgs(&p), vec!["from n1", "Volume 'v' created"]);
    assert_eq!(p.events[1].event.by.as_deref(), Some("dave"));
    // Users can't take the log's name, or delete it.
    assert!(c.create_volume(".events", 1 << 20).await.is_err());
    assert!(c.delete_dataset(EVENTS_DATASET).await.is_err());
    // A new committer (a new epoch) picks up where this one was.
    drop(c);
    let c = Committer::new(fs, auth, "id-n0".into(), 2, "n0".into()).await;
    c.observe().await.unwrap();
    let p = log_of(&c).await;
    assert_eq!(p.events.last().unwrap().event.kind, "committer.started");
    assert_eq!(
        p.events.last().unwrap().event.message,
        "Committer restarted on n0"
    );
}

async fn log_of<D: crate::disk::BlockDevice>(c: &crate::committer::Committer<D>) -> EventPage {
    c.events()
        .await
        .unwrap()
        .read(&EventQuery::default())
        .await
        .unwrap()
}
