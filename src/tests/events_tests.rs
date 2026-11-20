//! The activity log: appending, reading pages, keeping what the committer
//! saw, pruning; the committer's diff of the cluster state; a node's sink.

use super::*;
use crate::cluster::state::{ClusterState, Lease};
use crate::test_util::TestPool;
use crate::types::{Epoch, ep};

fn ev(node: &str, msg: &str) -> NewEvent {
    NewEvent {
        ts_ms: crate::fs::now_ms(),
        node: node.into(),
        level: Level::Info,
        kind: EventKind::NodeUp,
        subject: Subject::Pool,
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

fn lease(owner_node: &str, epoch: Epoch, live: bool, released: bool) -> Lease {
    Lease {
        owner: format!("id-{}", owner_node),
        holder: crate::types::Holder::new(crate::types::HolderRole::Agent, Some(owner_node))
            .with_addr(format!("{}:1", owner_node)),
        epoch,
        expires_ms: if live { 2_000 } else { 500 },
        released,
    }
}

fn state(leases: &[(&str, Lease)]) -> ClusterState {
    ClusterState {
        leases: leases
            .iter()
            .map(|(r, l)| (crate::types::res(r), l.clone()))
            .collect(),
        ..Default::default()
    }
}

const NOW: u64 = 1_000;

fn me(node: &str, epoch: Epoch) -> Held {
    Held {
        node: node.into(),
        epoch,
    }
}

fn kinds(v: &[Noticed]) -> Vec<(EventKind, String)> {
    v.iter().map(|(_, k, s, _)| (*k, s.to_string())).collect()
}

fn label(_: &str) -> Option<DatasetFormat> {
    Some(DatasetFormat::Block)
}

#[test]
fn a_new_log_starts_without_replaying_the_cluster() {
    let s = state(&[("node/a", lease("a", ep(1), true, false))]);
    let new = observe(&s, NOW, me("a", ep(5)), None);
    assert_eq!(
        kinds(&diff(None, &new, &s, &label)),
        vec![(EventKind::EventsStarted, "pool".to_string())]
    );
}

#[test]
fn nodes_and_datasets_coming_and_going() {
    let s0 = state(&[
        ("node/a", lease("a", ep(1), true, false)),
        ("node/b", lease("b", ep(2), true, false)),
        ("node/c", lease("c", ep(3), true, false)),
        ("ds/v1", lease("b", ep(4), true, false)),
        ("ds/v2", lease("c", ep(5), true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("a", ep(9)), Some(1));
    assert_eq!(o0.datasets["v1"], me("b", ep(4)));
    // b went quiet (and v1 with it); c shut down cleanly (and let go of
    // v2: its agent says so); d is new; a restarted.
    let s1 = state(&[
        ("node/a", lease("a", ep(6), true, false)),
        ("node/b", lease("b", ep(2), false, false)),
        ("node/c", lease("c", ep(3), false, true)),
        ("node/d", lease("d", ep(7), true, false)),
        ("ds/v1", lease("b", ep(4), false, false)),
        ("ds/v2", lease("c", ep(5), false, true)),
    ]);
    let o1 = observe(&s1, NOW, me("a", ep(9)), Some(4));
    let mut got = kinds(&diff(Some(&o0), &o1, &s1, &label));
    got.sort_by(|a, b| (a.0.as_str(), &a.1).cmp(&(b.0.as_str(), &b.1)));
    assert_eq!(
        got,
        vec![
            (EventKind::DatasetLost, "volume/v1".to_string()),
            (EventKind::NodeDown, "node/b".to_string()),
            (EventKind::NodeLeft, "node/c".to_string()),
            (EventKind::NodeRestarted, "node/a".to_string()),
            (EventKind::NodeUp, "node/d".to_string()),
            (EventKind::RaftLeader, "cluster".to_string()),
        ]
    );
    // Nothing changed: nothing to say.
    assert!(diff(Some(&o1), &o1, &s1, &label).is_empty());
    // b comes back.
    let s2 = state(&[
        ("node/a", lease("a", ep(6), true, false)),
        ("node/b", lease("b", ep(8), true, false)),
        ("node/c", lease("c", ep(3), false, true)),
        ("node/d", lease("d", ep(7), true, false)),
    ]);
    let o2 = observe(&s2, NOW, me("a", ep(9)), Some(4));
    assert_eq!(
        kinds(&diff(Some(&o1), &o2, &s2, &label)),
        vec![(EventKind::NodeUp, "node/b".to_string())]
    );
}

#[test]
fn a_live_node_reports_its_own_lost_leases() {
    let s0 = state(&[
        ("node/b", lease("b", ep(2), true, false)),
        ("ds/v1", lease("b", ep(4), true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("a", ep(9)), None);
    let s1 = state(&[
        ("node/b", lease("b", ep(2), true, false)),
        ("ds/v1", lease("b", ep(4), false, false)),
    ]);
    let o1 = observe(&s1, NOW, me("a", ep(9)), None);
    assert!(diff(Some(&o0), &o1, &s1, &label).is_empty());
}

#[test]
fn failover_between_looks_and_committer_takeovers() {
    let s0 = state(&[
        ("node/a", lease("a", ep(1), true, false)),
        ("node/b", lease("b", ep(2), true, false)),
        ("ds/v1", lease("b", ep(4), true, false)),
    ]);
    let o0 = observe(&s0, NOW, me("b", ep(3)), None);
    // b died, and a took over both v1 and the committer, all while no
    // committer looked.
    let s1 = state(&[
        ("node/a", lease("a", ep(1), true, false)),
        ("node/b", lease("b", ep(2), false, false)),
        ("ds/v1", lease("a", ep(10), true, false)),
    ]);
    let o1 = observe(&s1, NOW, me("a", ep(11)), None);
    let d = diff(Some(&o0), &o1, &s1, &label);
    let mut got = kinds(&d);
    got.sort_by(|a, b| (a.0.as_str(), &a.1).cmp(&(b.0.as_str(), &b.1)));
    assert_eq!(
        got,
        vec![
            (EventKind::CommitterMoved, "node/a".to_string()),
            (EventKind::DatasetFailover, "volume/v1".to_string()),
            (EventKind::NodeDown, "node/b".to_string()),
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
    assert!(sink.once(
        Level::Error,
        EventKind::VmStartFailed,
        Subject::vm("x"),
        "boom"
    ));
    assert!(!sink.once(
        Level::Error,
        EventKind::VmStartFailed,
        Subject::vm("x"),
        "boom"
    ));
    assert!(sink.once(
        Level::Error,
        EventKind::VmStartFailed,
        Subject::vm("x"),
        "other"
    ));
    assert!(sink.clear(&Subject::vm("x")));
    assert!(sink.once(
        Level::Error,
        EventKind::VmStartFailed,
        Subject::vm("x"),
        "other"
    ));
    assert_eq!(sink.pending(), 3);
    // Past its cap, the oldest go, and a note says how many.
    for i in 0..SINK_CAP {
        sink.info(EventKind::NodeUp, Subject::Pool, format!("{}", i));
    }
    assert_eq!(sink.pending(), SINK_CAP);
    let batch = sink.take(2);
    assert_eq!(batch[0].kind, EventKind::EventsDropped);
    assert!(batch[0].message.starts_with("3 events"));
    assert_eq!(batch[1].message, "0");
    sink.put_back(batch);
    assert_eq!(sink.take(1)[0].kind, EventKind::EventsDropped);
}

#[tokio::test]
async fn sink_retries_until_sent() {
    let sink = EventSink::new("n1");
    sink.info(EventKind::NodeUp, Subject::Pool, "hello");
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
        resource: crate::types::res(r),
        owner: format!("id-{}", node),
        holder: crate::types::Holder::new(crate::types::HolderRole::Agent, Some(node)),
        ttl_ms: 600_000,
    };
    auth.propose(acquire("pool", "n0")).await.unwrap();
    auth.propose(acquire("node/n0", "n0")).await.unwrap();
    let c = Committer::new(fs.clone(), auth.clone(), "id-n0".into(), ep(1), "n0".into()).await;
    c.observe().await.unwrap();
    auth.propose(acquire("node/n1", "n1")).await.unwrap();
    c.observe().await.unwrap();
    // Unchanged: nothing more.
    c.observe().await.unwrap();
    let log = c.events().await.unwrap();
    let p = log.read(&EventQuery::default()).await.unwrap();
    let k: Vec<EventKind> = p.events.iter().map(|e| e.event.kind).collect();
    assert_eq!(k, vec![EventKind::EventsStarted, EventKind::NodeUp]);
    assert_eq!(p.events[1].event.message, "Node n1 joined the cluster");
    // What nodes report, and changes asked of it.
    c.record(vec![ev("n1", "from n1")]).await.unwrap();
    c.note(
        Level::Info,
        EventKind::VolumeCreated,
        Subject::volume("v"),
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
    let c = Committer::new(fs, auth, "id-n0".into(), ep(2), "n0".into()).await;
    c.observe().await.unwrap();
    let p = log_of(&c).await;
    assert_eq!(
        p.events.last().unwrap().event.kind,
        EventKind::CommitterStarted
    );
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

#[test]
fn kinds_and_subjects_round_trip() {
    for k in EventKind::ALL {
        assert_eq!(k.as_str().parse::<EventKind>(), Ok(*k));
    }
    assert!("vm.nonsense".parse::<EventKind>().is_err());
    for s in [
        Subject::Pool,
        Subject::Cluster,
        Subject::node("node-1"),
        Subject::Disk(3),
        Subject::vm("web"),
        Subject::volume("db"),
        Subject::dataset(Some(DatasetFormat::S3), "logs"),
        Subject::dataset(None, "x"),
    ] {
        let text = s.to_string();
        assert_eq!(text.parse::<Subject>(), Ok(s.clone()), "{}", text);
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<Subject>(&json).unwrap(), s);
    }
    assert_eq!(
        Subject::dataset(Some(DatasetFormat::S3), "logs").to_string(),
        "bucket/logs"
    );
    for bad in ["", "node/", "disk/x", "nope/x", "nope"] {
        assert!(bad.parse::<Subject>().is_err(), "{}", bad);
    }
}

/// A change asked of the committer in process (`LocalAdmin`, which is
/// also what the HTTP routes are) is recorded as its caller's, and one
/// that fails isn't recorded at all.
#[tokio::test]
async fn admin_changes_are_logged_as_their_callers() {
    use crate::committer::{Authority, Committer, LocalAdmin, LocalAuthority, PoolAdmin};
    use crate::types::{DatasetName, ErrorKind};
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    auth.propose(crate::cluster::LeaseOp::Acquire {
        resource: crate::types::res("pool"),
        owner: "id-n0".into(),
        holder: crate::types::Holder::new(crate::types::HolderRole::Agent, Some("n0")),
        ttl_ms: 600_000,
    })
    .await
    .unwrap();
    let c = Arc::new(Committer::new(fs, auth, "id-n0".into(), ep(1), "n0".into()).await);
    let dave = LocalAdmin::new(c.clone(), Some("dave".into()));
    let v = DatasetName::user("v").unwrap();
    dave.create_volume(&v, 1 << 20).await.unwrap();
    dave.create_snapshot("v", "s1").await.unwrap();
    // Already there: refused as such, and nothing recorded.
    let e = dave.create_volume(&v, 1 << 20).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Exists);
    let p = log_of(&c).await;
    let mine: Vec<(EventKind, Option<&str>)> = p
        .events
        .iter()
        .filter(|e| e.event.by.is_some())
        .map(|e| (e.event.kind, e.event.by.as_deref()))
        .collect();
    assert_eq!(
        mine,
        vec![
            (EventKind::VolumeCreated, Some("dave")),
            (EventKind::SnapshotCreated, Some("dave")),
        ]
    );
}
