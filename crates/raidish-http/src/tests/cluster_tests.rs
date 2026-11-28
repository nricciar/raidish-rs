use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::cluster::state::{ClusterState, LeaseOp, POOL, Request, Response};
use crate::cluster::*;
use crate::test_util::wait_until;
use crate::types::{Epoch, Holding, LeaseRef, MetaslabIdx, NodeId, ep, ms};

fn nid(i: u64) -> NodeId {
    NodeId::from_raw(i)
}
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn acquire(owner: &str, now_ms: u64, ttl_ms: u64) -> Request {
    LeaseOp::Acquire {
        resource: POOL,
        owner: owner.into(),
        holder: crate::types::Holder::default().with_detail(format!("test {}", owner)),
        ttl_ms,
    }
    .stamp(now_ms)
}

fn renew(owner: &str, epoch: Epoch, now_ms: u64, ttl_ms: u64) -> Request {
    LeaseOp::Renew {
        resource: POOL,
        holder: Holding::new(owner, epoch),
        ttl_ms,
    }
    .stamp(now_ms)
}

/// I-16: a lease op's TTL is bounded, so `now_ms + ttl_ms` can't overflow
/// in `apply` (a panic under the store's lock, on every node and every
/// replay), nor wrap into a lease that never expires.
#[test]
fn a_huge_ttl_is_clamped() {
    use crate::cluster::state::MAX_LEASE_TTL_MS;
    let mut s = ClusterState::default();
    let now = 1_000;
    let Response::Granted { epoch, expires_ms } = s.apply(&acquire("a", now, u64::MAX)) else {
        panic!("not granted");
    };
    assert_eq!(expires_ms, now + MAX_LEASE_TTL_MS);
    let Response::Granted { expires_ms, .. } = s.apply(&renew("a", epoch, now + 1, u64::MAX - 1))
    else {
        panic!("not renewed");
    };
    assert_eq!(expires_ms, now + 1 + MAX_LEASE_TTL_MS);
    assert!(
        s.live_lease(&POOL, now + MAX_LEASE_TTL_MS + 2).is_none(),
        "expires"
    );
    // A clock near the end of time saturates instead of wrapping.
    let mut s = ClusterState::default();
    let Response::Granted { expires_ms, .. } = s.apply(&acquire("a", u64::MAX - 5, 100)) else {
        panic!("not granted");
    };
    assert_eq!(expires_ms, u64::MAX);
}

/// A reader whose clock lags the cluster's doesn't see a lease the
/// cluster has expired as live (`ClusterState::clock_ms`).
#[test]
fn a_lagging_clock_doesnt_revive_an_expired_lease() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    assert!(s.live_lease(&POOL, 50).is_some());
    // Any request moves the cluster's clock on (this one is refused).
    s.apply(&renew("nobody", ep(9), 500, 100));
    assert_eq!(
        s.live_lease(&POOL, 50),
        None,
        "expired at 100, clock at 500"
    );
    assert_eq!(s.live_nodes(50).count(), 0);
}

#[test]
fn lease_grant_hold_expire() {
    let mut s = ClusterState::default();
    assert_eq!(
        s.apply(&acquire("a", 0, 100)),
        Response::Granted {
            epoch: ep(1),
            expires_ms: 100
        }
    );
    assert_eq!(s.pool_epoch(), 1);
    // Someone else, while it's live: refused.
    assert!(
        matches!(s.apply(&acquire("b", 50, 100)), Response::Held { ref owner, .. } if owner == "a")
    );
    // Same owner again: same epoch, extended.
    assert_eq!(
        s.apply(&acquire("a", 60, 100)),
        Response::Granted {
            epoch: ep(1),
            expires_ms: 160
        }
    );
    // Renew extends.
    assert_eq!(
        s.apply(&renew("a", ep(1), 100, 100)),
        Response::Granted {
            epoch: ep(1),
            expires_ms: 200
        }
    );
    // After expiry, someone else gets it at a new epoch.
    assert_eq!(
        s.apply(&acquire("b", 200, 100)),
        Response::Granted {
            epoch: ep(2),
            expires_ms: 300
        }
    );
    // The old holder can't renew any more.
    assert_eq!(s.apply(&renew("a", ep(1), 210, 100)), Response::Lost);
    assert_eq!(s.pool_epoch(), 2);
}

#[test]
fn renew_after_expiry_without_takeover() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    // Nobody took it, so the epoch is still ours.
    assert_eq!(
        s.apply(&renew("a", ep(1), 500, 100)),
        Response::Granted {
            epoch: ep(1),
            expires_ms: 600
        }
    );
}

#[test]
fn release_frees_but_keeps_epoch() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 1000));
    let release = LeaseOp::Release {
        resource: POOL,
        holder: Holding::new("a", ep(1)),
    }
    .stamp(0);
    assert_eq!(s.apply(&release), Response::Released);
    assert_eq!(s.pool_epoch(), 1, "the floor never goes back");
    assert!(matches!(
        s.apply(&acquire("b", 10, 1000)),
        Response::Granted { epoch, .. } if epoch == 2
    ));
    // Renewing a released lease fails.
    assert_eq!(s.apply(&renew("a", ep(1), 20, 1000)), Response::Lost);
}

#[test]
fn epochs_increase_across_resources() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 10));
    let other = LeaseOp::Acquire {
        resource: crate::types::res("ds/vm1"),
        owner: "b".into(),
        holder: crate::types::Holder::default().with_detail(String::new()),
        ttl_ms: 10,
    }
    .stamp(0);
    assert!(matches!(
        s.apply(&other),
        Response::Granted { epoch, .. } if epoch == 2
    ));
    assert!(matches!(
        s.apply(&acquire("c", 100, 10)),
        Response::Granted { epoch, .. } if epoch == 3
    ));
}

// ---- node ids on the wire ----

#[test]
fn to_header_is_the_bare_number() {
    // Older nodes parse `X-Raidish-To` as a u64: `node-3` would be refused.
    assert_eq!(to_header(nid(3)), "3");
    assert_eq!(parse_to_header("3"), Some(nid(3)));
    assert_eq!(parse_to_header("node-3"), None);
}

#[test]
fn node_id_raft_state_encodes_as_before() {
    // Raft's id type went from u64 to NodeId: the persisted vote, log ids
    // and membership must read and write the same bytes.
    let old = openraft::Vote::new_committed(7, 2u64);
    let new = openraft::Vote::new_committed(7, nid(2));
    assert_eq!(
        serde_json::to_string(&old).unwrap(),
        serde_json::to_string(&new).unwrap()
    );
    let old: BTreeMap<u64, BasicNode> = BTreeMap::from([(0, BasicNode::default())]);
    let new: BTreeMap<NodeId, BasicNode> = BTreeMap::from([(nid(0), BasicNode::default())]);
    let (old, new) = (
        serde_json::to_string(&old).unwrap(),
        serde_json::to_string(&new).unwrap(),
    );
    assert_eq!(old, new);
    let back: BTreeMap<NodeId, BasicNode> = serde_json::from_str(&old).unwrap();
    assert!(back.contains_key(&nid(0)));
}

// ---- membership from the pool config ----

use crate::cluster::members::{Members, derive, is_local_base};

fn urls(ports: &[u16]) -> Vec<String> {
    ports
        .iter()
        .enumerate()
        .map(|(i, p)| format!("http://127.0.0.1:{}/api/v1/disks/{}", p, i))
        .collect()
}

#[test]
fn members_from_mixed_pool() {
    // This node has disks 0 and 3; node B has 1 and 4 (one member); C has 2.
    let paths: Vec<String> = vec![
        "/dev/a".into(),
        "http://b:8881/api/v1/disks/1".into(),
        "http://c:8881/api/v1/disks/2".into(),
        "disks/d3".into(),
        "ws://b:8881/api/v1/disks/4".into(),
    ];
    let m = derive(&paths, "raidz2", |_| false).unwrap();
    assert_eq!(m.me, nid(0));
    assert_eq!(
        m.peers,
        BTreeMap::from([
            (nid(1), "http://b:8881".into()),
            (nid(2), "http://c:8881".into())
        ])
    );
    assert_eq!(m.disks[&nid(0)], vec![0, 3]);
    assert_eq!(m.disks[&nid(1)], vec![1, 4]);
    assert_eq!(m.disks[&nid(2)], vec![2]);
}

#[test]
fn every_node_derives_the_same_ids_and_fingerprint() {
    let all = urls(&[9001, 9002, 9003]);
    let view = |me: usize| -> Members {
        let mut paths = all.clone();
        paths[me] = format!("/local/disk{}", me);
        derive(&paths, "raidz1", |_| false).unwrap()
    };
    let (a, b, c) = (view(0), view(1), view(2));
    assert_eq!((a.me, b.me, c.me), (nid(0), nid(1), nid(2)));
    assert_eq!(a.fingerprint, b.fingerprint);
    assert_eq!(b.fingerprint, c.fingerprint);
    assert_eq!(a.disks, c.disks);
}

#[test]
fn disagreeing_configs_have_different_fingerprints() {
    let a = derive(&urls(&[1, 2, 3])[..], "raidz1", |b| b.ends_with(":1")).unwrap();
    let c = derive(&urls(&[1, 2, 3])[..], "mirror", |b| b.ends_with(":1")).unwrap();
    // Two disks on node 1 instead of one each.
    let d = derive(&urls(&[1, 1, 3])[..], "raidz1", |b| b.ends_with(":1")).unwrap();
    assert_ne!(a.fingerprint, c.fingerprint);
    assert_ne!(a.fingerprint, d.fingerprint);
    // Swapping two single-disk nodes keeps the shape, so the fingerprint
    // can't tell; the receiver-id check catches that (see
    // `swapped_nodes_are_detected`).
    let b = derive(&urls(&[2, 1, 3])[..], "raidz1", |b| b.ends_with(":2")).unwrap();
    assert_eq!(a.fingerprint, b.fingerprint);
}

#[test]
fn all_local_pool_is_a_cluster_of_one() {
    let paths: Vec<String> = (0..5).map(|i| format!("disks/d{}", i)).collect();
    let m = derive(&paths, "raidz2", |_| false).unwrap();
    assert_eq!(m.me, nid(0));
    assert!(m.peers.is_empty());
    assert_eq!(m.disks.len(), 1);
}

#[test]
fn pool_without_this_node_is_an_error() {
    assert!(derive(&urls(&[1, 2, 3]), "raidz1", |_| false).is_err());
}

#[test]
fn url_pointing_here_is_this_node() {
    assert!(is_local_base("http://127.0.0.1:18881", 18881));
    assert!(is_local_base("http://localhost:18881", 18881));
    assert!(
        !is_local_base("http://127.0.0.1:18882", 18881),
        "another port"
    );
    assert!(
        !is_local_base("http://192.0.2.1:18881", 18881),
        "not our address"
    );
}

// ---- an in-process cluster ----

struct TestNode {
    cluster: Arc<Cluster>,
    floor: Arc<crate::fence::ServerFence>,
    shutdown: Arc<tokio::sync::Notify>,
    url: String,
}

use raidish_auth::testing::free_port;

/// Start a node whose pool config is `paths` (every node gets the same
/// URLs; each finds itself by port).
async fn start_node(name: &str, paths: &[String], port: u16, dir: &std::path::Path) -> TestNode {
    let floor = Arc::new(crate::fence::ServerFence::default());
    let members = derive(paths, "test", |b| is_local_base(b, port)).unwrap();
    let cluster = Cluster::start(name, members, dir.join(name), floor.clone())
        .await
        .unwrap();
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let stop = shutdown.clone();
    raidish_auth::testing::spawn_on(
        port,
        None,
        raidish_auth::http::finish(
            routes::router(cluster.clone()),
            crate::auth::AuthState(None),
        ),
        async move { stop.notified().await },
    )
    .await
    .unwrap();
    TestNode {
        cluster,
        floor,
        shutdown,
        url: format!("http://127.0.0.1:{}", port),
    }
}

async fn stop_node(node: &TestNode) {
    node.shutdown.notify_one();
    let _ = node.cluster.raft.shutdown().await;
}

fn leader_of(nodes: &[&TestNode]) -> Option<String> {
    nodes
        .iter()
        .find_map(|n| n.cluster.raft.metrics().borrow().current_leader)
        .and_then(|id| {
            nodes
                .iter()
                .find(|n| n.cluster.members.me == id)
                .map(|n| n.cluster.name.clone())
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_grant_and_fail_over() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(
        wait_until(10, || leader_of(&all).is_some()).await,
        "no leader elected"
    );

    // Any node takes lease requests (followers forward).
    let client = LeaseClient::new(nodes.iter().map(|n| n.url.clone()).collect());
    let a = HeldLease::acquire(
        client.clone(),
        POOL,
        crate::types::Holder::default().with_detail("writer a"),
    )
    .await
    .unwrap();
    let err = HeldLease::acquire(
        client.clone(),
        POOL,
        crate::types::Holder::default().with_detail("writer b"),
    )
    .await
    .err()
    .unwrap();
    assert!(err.contains("writer a"), "{}", err);

    // Every node's fencing floor learns the epoch.
    assert!(wait_until(5, || nodes.iter().all(|n| n.floor.pool_floor() == a.epoch)).await);

    // Lose the leader: the other two elect a new one, and the lease survives.
    let leader = leader_of(&all).unwrap();
    let old = nodes.iter().find(|n| n.cluster.name == leader).unwrap();
    stop_node(old).await;
    let rest: Vec<&TestNode> = nodes.iter().filter(|n| n.cluster.name != leader).collect();
    assert!(
        wait_until(10, || leader_of(&rest).is_some_and(|l| l != leader)).await,
        "no new leader"
    );
    let survivors = LeaseClient::new(rest.iter().map(|n| n.url.clone()).collect());
    let renewed = survivors
        .call(&LeaseOp::Renew {
            resource: POOL,
            holder: Holding::new(a.owner.clone(), a.epoch),
            ttl_ms: 300,
        })
        .await
        .unwrap();
    assert!(matches!(renewed, Response::Granted { epoch, .. } if epoch == a.epoch));

    // Released: a new writer gets a newer epoch.
    a.release().await;
    let b = HeldLease::acquire(
        survivors.clone(),
        POOL,
        crate::types::Holder::default().with_detail("writer b"),
    )
    .await
    .unwrap();
    assert!(b.epoch > a.epoch);
    assert!(wait_until(5, || rest.iter().all(|n| n.floor.pool_floor() == b.epoch)).await);
    // The old writer's lease is gone for good.
    let lost = survivors
        .call(&LeaseOp::Renew {
            resource: POOL,
            holder: Holding::new(a.owner.clone(), a.epoch),
            ttl_ms: 1000,
        })
        .await
        .unwrap();
    assert_eq!(lost, Response::Lost);

    // A lease that isn't renewed expires and can be taken over.
    let short = |owner: &str| LeaseOp::Acquire {
        resource: crate::types::res("ds/short"),
        owner: owner.into(),
        holder: crate::types::Holder::default().with_detail(owner),
        ttl_ms: 300,
    };
    let Response::Granted { epoch: c, .. } = survivors.call(&short("c")).await.unwrap() else {
        panic!("c not granted");
    };
    assert!(matches!(
        survivors.call(&short("d")).await.unwrap(),
        Response::Held { .. }
    ));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let Response::Granted { epoch: d, .. } = survivors.call(&short("d")).await.unwrap() else {
        panic!("d not granted after expiry");
    };
    assert!(d > c);

    let status = survivors.status().await.unwrap();
    assert_eq!(status.members.len(), 3);
    assert_eq!(status.state.pool_epoch(), b.epoch);

    for n in &rest {
        stop_node(n).await;
    }
}

/// The leader is stopped the way `docker stop` does it (the server is asked
/// to shut down, nothing more) just before a follower's next renewal: the
/// follower's lease must survive the election, renewed by the new leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_followers_lease_survives_losing_the_leader() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(
        wait_until(10, || leader_of(&all).is_some()).await,
        "no leader elected"
    );
    let leader = leader_of(&all).unwrap();
    let old = nodes.iter().find(|n| n.cluster.name == leader).unwrap();
    let follower = nodes.iter().find(|n| n.cluster.name != leader).unwrap();

    // As an agent does: its own node first, then the others.
    let mut endpoints = vec![follower.url.clone()];
    endpoints.extend(
        nodes
            .iter()
            .filter(|n| n.url != follower.url)
            .map(|n| n.url.clone()),
    );
    let client = LeaseClient::new(endpoints);
    let held = Arc::new(
        HeldLease::acquire(
            client,
            crate::types::res("node/follower"),
            crate::types::Holder::default().with_detail("follower"),
        )
        .await
        .unwrap(),
    );
    let lost = Arc::new(std::sync::Mutex::new(None::<String>));
    let l = lost.clone();
    held.hold(move |m| *l.lock().unwrap() = Some(m));

    // Wait for a renewal, then stop the leader just before the next one.
    let expires = |n: &TestNode| {
        n.cluster
            .lease_state(&crate::types::res("node/follower"))
            .map_or(0, |l| l.expires_ms)
    };
    assert!(wait_until(5, || expires(follower) > 0).await);
    let first = expires(follower);
    assert!(
        wait_until(10, || expires(follower) > first).await,
        "never renewed"
    );
    tokio::time::sleep(lease::LEASE_TTL / 3 - std::time::Duration::from_millis(300)).await;
    let stopped_ms = now_ms();
    stop_node(old).await;

    // Well past a TTL later, it's still held, and the new leader renewed it.
    let rest: Vec<&TestNode> = nodes.iter().filter(|n| n.cluster.name != leader).collect();
    tokio::time::sleep(lease::LEASE_TTL * 3 / 2).await;
    assert_eq!(*lost.lock().unwrap(), None, "the follower lost its lease");
    assert!(held.fresh(), "the lease went stale");
    assert!(
        leader_of(&rest).is_some_and(|l| l != leader),
        "no new leader"
    );
    assert!(
        expires(follower) > stopped_ms + lease::LEASE_TTL.as_millis() as u64,
        "not renewed since the leader stopped"
    );

    held.release().await;
    for n in &nodes {
        stop_node(n).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_restarts_with_its_state() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let pool: Vec<String> = vec!["disks/a".into(), "disks/b".into()];
    let node = start_node("solo", &pool, port, dir.path()).await;
    let client = LeaseClient::new(vec![node.url.clone()]);
    assert!(wait_until(10, || leader_of(&[&node]).is_some()).await);
    let a = HeldLease::acquire(
        client.clone(),
        POOL,
        crate::types::Holder::default().with_detail("a"),
    )
    .await
    .unwrap();
    stop_node(&node).await;
    drop(node);

    // Same data dir: the epoch (and so the fencing floor) is remembered.
    let node = start_node("solo", &pool, port, dir.path()).await;
    assert_eq!(node.floor.pool_floor(), a.epoch);
    assert!(wait_until(10, || leader_of(&[&node]).is_some()).await);
    let err = HeldLease::acquire(
        client.clone(),
        POOL,
        crate::types::Holder::default().with_detail("b"),
    )
    .await
    .err();
    assert!(err.is_some(), "a's lease is still live");
    stop_node(&node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_with_a_different_layout_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    // Node 3 thinks node 1 serves disks 0 and 1.
    let mut odd = pool.clone();
    odd[1] = format!("http://127.0.0.1:{}/api/v1/disks/1", ports[0]);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &odd, ports[2], dir.path()).await,
    ];
    assert_ne!(
        nodes[0].cluster.members.fingerprint,
        nodes[2].cluster.members.fingerprint
    );
    // The two that agree still have a quorum (2 of 3) and work.
    let good: Vec<&TestNode> = nodes[..2].iter().collect();
    assert!(wait_until(10, || leader_of(&good).is_some()).await);
    let client = LeaseClient::new(vec![nodes[0].url.clone(), nodes[1].url.clone()]);
    let a = HeldLease::acquire(
        client,
        POOL,
        crate::types::Holder::default().with_detail("a"),
    )
    .await
    .unwrap();
    assert!(wait_until(5, || good.iter().all(|n| n.floor.pool_floor() == a.epoch)).await);
    // The odd one never joins in.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(nodes[2].floor.pool_floor(), 0);
    assert!(
        nodes[2]
            .cluster
            .raft
            .metrics()
            .borrow()
            .current_leader
            .is_none()
    );
    assert!(nodes[2].cluster.check_peer(nid(0)).await.unwrap().is_some());
    for n in &nodes {
        stop_node(n).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swapped_nodes_are_detected() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    // Node 3 lists node 1 and node 2 the other way round.
    let mut odd = pool.clone();
    odd.swap(0, 1);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &odd, ports[2], dir.path()).await,
    ];
    // Same shape, so the same fingerprint...
    assert_eq!(
        nodes[0].cluster.members.fingerprint,
        nodes[2].cluster.members.fingerprint
    );
    // ...but node 3 can tell its view is wrong, and the others can't.
    let problem = nodes[2].cluster.check_peer(nid(0)).await.unwrap();
    assert!(problem.is_some_and(|p| p.contains("different orders")));
    assert_eq!(nodes[0].cluster.check_peer(nid(1)).await.unwrap(), None);
    assert_eq!(nodes[0].cluster.check_peer(nid(2)).await.unwrap(), None);

    // A Raft RPC addressed to the wrong node id is refused.
    let vote = openraft::raft::VoteRequest::<u64> {
        vote: openraft::Vote::new(1, 2),
        last_log_id: None,
    };
    let ask = |version: Option<&str>| {
        let mut req = reqwest::Client::new()
            .post(format!("{}/api/v1/raft/vote", nodes[1].url))
            .header(FINGERPRINT_HEADER, &nodes[1].cluster.members.fingerprint)
            .header(TO_HEADER, "0")
            .json(&vote);
        if let Some(v) = version {
            req = req.header(PROTOCOL_HEADER, v);
        }
        req.send()
    };
    let resp = ask(Some(&RAFT_PROTOCOL.to_string())).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    // So is one in another (or no) Raft protocol version, first.
    for v in [None, Some("0")] {
        let resp = ask(v).await.unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UPGRADE_REQUIRED,
            "{:?}",
            v
        );
    }

    // Node 3 finds itself at odds with every peer and sits out, so its
    // own hopeless elections don't keep the others' candidates stepping
    // down; the agreeing two don't blame themselves, and still work.
    assert!(wait_until(10, || nodes[2].cluster.misconfigured()).await);
    assert!(!nodes[0].cluster.misconfigured() && !nodes[1].cluster.misconfigured());
    let good: Vec<&TestNode> = nodes[..2].iter().collect();
    assert!(wait_until(10, || leader_of(&good).is_some()).await);
    for n in &nodes {
        stop_node(n).await;
    }
}

fn lend(
    idx: MetaslabIdx,
    writer: &str,
    writer_epoch: Epoch,
    pool_epoch: Epoch,
    now_ms: u64,
) -> Request {
    LeaseOp::Lend {
        idx,
        writer: LeaseRef::new(crate::types::res(writer), writer_epoch),
        pool: Holding::new("c", pool_epoch),
        after: Epoch::NONE,
    }
    .stamp(now_ms)
}

#[test]
fn metaslab_lend_and_return() {
    use crate::cluster::state::Domain;
    let mut s = ClusterState::default();
    s.apply(&acquire("c", 0, 1000)); // committer: epoch 1
    let w = LeaseOp::Acquire {
        resource: crate::types::res("writer/2"),
        owner: "w".into(),
        holder: crate::types::Holder::default().with_detail(String::new()),
        ttl_ms: 1000,
    }
    .stamp(0);
    assert!(matches!(s.apply(&w), Response::Granted { epoch, .. } if epoch == 2));
    assert_eq!(s.fence_epoch(Domain::Metaslab(ms(3))), 1);

    assert_eq!(
        s.apply(&lend(ms(3), "writer/2", ep(2), ep(1), 10)),
        Response::Fenced { epoch: ep(3) }
    );
    assert_eq!(s.fence_epoch(Domain::Metaslab(ms(3))), 3);
    assert_eq!(s.fence_epoch(Domain::Pool), 1);
    // The same lend sent again (a retry whose answer was lost) is answered
    // as the first time (audit R4d); to anyone else, refused.
    assert_eq!(
        s.apply(&lend(ms(3), "writer/2", ep(2), ep(1), 20)),
        Response::Fenced { epoch: ep(3) }
    );
    assert!(matches!(
        s.apply(&lend(ms(3), "writer/9", ep(2), ep(1), 20)),
        Response::Refused(_)
    ));
    // Not by someone who isn't the committer, not to a writer that isn't
    // live.
    assert!(matches!(
        s.apply(&lend(ms(4), "writer/2", ep(2), ep(9), 20)),
        Response::Refused(_)
    ));
    assert!(matches!(
        s.apply(&lend(ms(4), "writer/2", ep(1), ep(1), 20)),
        Response::Refused(_)
    ));
    assert!(matches!(
        s.apply(&lend(ms(4), "writer/2", ep(2), ep(1), 5000)),
        Response::Refused(_)
    ));

    // A new committer doesn't move a lent metaslab's fence.
    s.apply(&acquire("c2", 2000, 1000)); // epoch 4
    assert_eq!(s.fence_epoch(Domain::Metaslab(ms(3))), 3);
    assert_eq!(s.fence_epoch(Domain::Metaslab(ms(4))), 4);
    let ret = |lend: Epoch| {
        LeaseOp::Return {
            idx: ms(3),
            pool: Holding::new("c2", ep(4)),
            lend,
        }
        .stamp(2100)
    };
    assert!(
        matches!(s.apply(&ret(ep(2))), Response::Refused(_)),
        "not that lend"
    );
    assert_eq!(s.apply(&ret(ep(3))), Response::Fenced { epoch: ep(5) });
    assert_eq!(s.fence_epoch(Domain::Metaslab(ms(3))), 5);
    assert!(s.lent_to(ms(3)).is_none());
    // Resent: the same answer, nothing changes.
    assert_eq!(s.apply(&ret(ep(3))), Response::Fenced { epoch: ep(5) });
    // A lend resent after the return doesn't lend it again: it names the
    // grant it replaced, and that's gone.
    assert!(matches!(
        s.apply(&lend(ms(3), "writer/2", ep(2), ep(4), 2200)),
        Response::Refused(_)
    ));
    assert!(s.lent_to(ms(3)).is_none());
}

/// NFSv4 client records are written only by the `nfs4` lease holder.
#[test]
fn nfs_client_records_are_fenced_by_the_nfs4_lease() {
    use crate::cluster::state::NFS4;
    let mut s = ClusterState::default();
    let grant = s.apply(
        &LeaseOp::Acquire {
            resource: NFS4,
            owner: "n1".into(),
            holder: crate::types::Holder::default().with_detail("nfs4 node-1"),
            ttl_ms: 10_000,
        }
        .stamp(1000),
    );
    let Response::Granted { epoch, .. } = grant else {
        panic!("{:?}", grant)
    };
    let rec = |owner: &str, epoch: Epoch, present: bool, now_ms: u64| {
        LeaseOp::NfsClient {
            client: "6162".into(),
            present,
            nfs: Holding::new(owner, epoch),
        }
        .stamp(now_ms)
    };
    assert_eq!(s.apply(&rec("n1", epoch, true, 2000)), Response::Recorded);
    assert!(s.nfs_clients.contains_key("6162"));
    // Not the holder, an old epoch, or after the lease ran out: refused.
    assert!(matches!(
        s.apply(&rec("n2", epoch, false, 2000)),
        Response::Refused(_)
    ));
    assert!(matches!(
        s.apply(&rec("n1", ep(epoch.get() - 1), false, 2000)),
        Response::Refused(_)
    ));
    assert!(s.nfs_clients.contains_key("6162"));
    assert_eq!(s.apply(&rec("n1", epoch, false, 3000)), Response::Recorded);
    assert!(s.nfs_clients.is_empty());
    assert!(matches!(
        s.apply(&rec("n1", epoch, true, 20_000)),
        Response::Refused(_)
    ));
    // The state machine's clock doesn't go back (audit R3a): a leader
    // stamping an earlier time can't bring the expired lease back.
    assert!(matches!(
        s.apply(&rec("n1", epoch, true, 3000)),
        Response::Refused(_)
    ));
    assert!(s.nfs_clients.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_lease_nobody_holds_any_more_is_given_back() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port()];
    let pool = urls(&ports);
    let node = start_node("n1", &pool, ports[0], dir.path()).await;
    assert!(
        wait_until(10, || leader_of(&[&node]).is_some()).await,
        "no leader elected"
    );
    let client = LeaseClient::new(vec![node.url.clone()]);
    // Taken and renewed in the background, then dropped without `release`
    // (as a start that fails part way does).
    let a = Arc::new(
        HeldLease::acquire(
            client.clone(),
            crate::types::res("writer/x"),
            crate::types::Holder::default().with_detail("a"),
        )
        .await
        .unwrap(),
    );
    a.hold(|m| panic!("lost: {}", m));
    let epoch = a.epoch;
    drop(a);
    // Given back at the next renewal (a third of a TTL), well before it
    // would expire.
    let t = std::time::Instant::now();
    let b = loop {
        match HeldLease::acquire(
            client.clone(),
            crate::types::res("writer/x"),
            crate::types::Holder::default().with_detail("b"),
        )
        .await
        {
            Ok(b) => break b,
            Err(e) => {
                assert!(
                    t.elapsed() < lease::LEASE_TTL * 2 / 3,
                    "still held after {:?}: {}",
                    t.elapsed(),
                    e
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    };
    assert!(b.epoch > epoch);
    stop_node(&node).await;
}

fn transfer(from: &str, from_epoch: Epoch, to: &str, now_ms: u64) -> Request {
    LeaseOp::Transfer {
        resource: POOL,
        from: Holding::new(from, from_epoch),
        to_owner: to.into(),
        to_holder: crate::types::Holder::default().with_detail(format!("test {}", to)),
        ttl_ms: 100,
    }
    .stamp(now_ms)
}

#[test]
fn transfer_hands_a_lease_over_at_a_new_epoch() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    assert_eq!(
        s.apply(&transfer("a", ep(1), "b", 50)),
        Response::Granted {
            epoch: ep(2),
            expires_ms: 150
        }
    );
    // b holds it now, at the new epoch (which fences a).
    assert_eq!(s.pool_epoch(), 2);
    assert!(s.holds(&POOL, &Holding::new("b", ep(2)), 60));
    assert_eq!(s.leases[&POOL].holder.detail, "test b");
    // a's renewals and release are refused; b renews.
    assert_eq!(s.apply(&renew("a", ep(1), 60, 100)), Response::Lost);
    assert_eq!(
        s.apply(
            &LeaseOp::Release {
                resource: POOL,
                holder: Holding::new("a", ep(1)),
            }
            .stamp(0)
        ),
        Response::Lost
    );
    assert!(s.holds(&POOL, &Holding::new("b", ep(2)), 60));
    assert!(matches!(
        s.apply(&renew("b", ep(2), 60, 100)),
        Response::Granted { epoch, .. } if epoch == 2
    ));
    // Nobody else can take it while b's is live.
    assert!(
        matches!(s.apply(&acquire("c", 70, 100)), Response::Held { ref owner, .. } if owner == "b")
    );
}

#[test]
fn transfer_repeated_is_the_same_transfer() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    let first = s.apply(&transfer("a", ep(1), "b", 50));
    // The duplicate a client sends to another node lands after it.
    assert_eq!(s.apply(&transfer("a", ep(1), "b", 51)), first);
    assert_eq!(s.pool_epoch(), 2);
    // But not once b's lease is gone: then it's just refused.
    assert_eq!(s.apply(&transfer("a", ep(1), "b", 500)), Response::Lost);
}

#[test]
fn transfer_needs_the_live_lease() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    // Wrong epoch, wrong owner, to itself, to nobody.
    assert_eq!(s.apply(&transfer("a", ep(7), "b", 50)), Response::Lost);
    assert_eq!(s.apply(&transfer("x", ep(1), "b", 50)), Response::Lost);
    assert!(matches!(
        s.apply(&transfer("a", ep(1), "a", 50)),
        Response::Refused(_)
    ));
    assert!(matches!(
        s.apply(&transfer("a", ep(1), "", 50)),
        Response::Refused(_)
    ));
    // Expired (even though nobody took it, unlike a renewal): refused, the
    // receiver might have assumed it was free to wait out.
    assert_eq!(s.apply(&transfer("a", ep(1), "b", 150)), Response::Lost);
    // Released: refused.
    s.apply(&acquire("a", 200, 100));
    let e = s.pool_epoch();
    s.apply(
        &LeaseOp::Release {
            resource: POOL,
            holder: Holding::new("a", e),
        }
        .stamp(0),
    );
    assert_eq!(s.apply(&transfer("a", e, "b", 210)), Response::Lost);
    // Nothing changed hands in any of it.
    assert_eq!(s.pool_epoch(), e);
    assert_eq!(s.leases[&POOL].owner, "a");
}

/// A held lease, renewing in the background, is handed to another owner
/// through a real cluster: the old holder stops renewing without counting
/// it as lost, the new one renews it, and nobody else can take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_lease_transfers_without_being_lost() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(wait_until(10, || leader_of(&all).is_some()).await);
    let client = LeaseClient::new(nodes.iter().map(|n| n.url.clone()).collect());

    let a = Arc::new(
        HeldLease::acquire(
            client.clone(),
            crate::types::res("ds/vol"),
            crate::types::Holder::default().with_detail("agent a"),
        )
        .await
        .unwrap(),
    );
    let a_lost = Arc::new(AtomicBool::new(false));
    let flag = a_lost.clone();
    a.hold(move |_| flag.store(true, Ordering::SeqCst));
    // Let a renew a couple of times first.
    tokio::time::sleep(lease::LEASE_TTL / 2).await;

    let b_owner = lease::owner_id() + ":b";
    let asked = std::time::Instant::now();
    let epoch = a
        .transfer(
            &b_owner,
            crate::types::Holder::default().with_detail("agent b"),
        )
        .await
        .unwrap();
    assert!(epoch > a.epoch);
    // (A repeated transfer is `transfer_repeated_is_the_same_transfer`.)
    // Having let go, the old holder can't transfer it again.
    let b = Arc::new(HeldLease::adopt(
        client.clone(),
        crate::types::res("ds/vol"),
        &b_owner,
        epoch,
        asked,
    ));
    assert!(
        a.transfer(
            "someone else",
            crate::types::Holder::default().with_detail("x")
        )
        .await
        .is_err()
    );
    let b_lost = Arc::new(AtomicBool::new(false));
    let flag = b_lost.clone();
    b.hold(move |_| flag.store(true, Ordering::SeqCst));

    // Past a whole TTL: b kept it alive, a never thought it lost it.
    tokio::time::sleep(lease::LEASE_TTL + std::time::Duration::from_secs(1)).await;
    assert!(
        !a_lost.load(Ordering::SeqCst),
        "the old holder saw a lost lease"
    );
    assert!(!b_lost.load(Ordering::SeqCst), "the new holder lost it");
    assert!(b.fresh());
    let status = client.status().await.unwrap();
    let l = &status.state.leases[&crate::types::res("ds/vol")];
    assert_eq!(
        (l.owner.as_str(), l.epoch, l.holder.detail.as_str()),
        (b_owner.as_str(), epoch, "agent b")
    );
    assert!(!l.released);
    let err = HeldLease::acquire(
        client.clone(),
        crate::types::res("ds/vol"),
        crate::types::Holder::default().with_detail("agent c"),
    )
    .await
    .err()
    .unwrap();
    assert!(err.contains("agent b"), "{}", err);

    // Dropping a's handle (it let go) leaves b's lease alone.
    drop(a);
    tokio::time::sleep(lease::LEASE_TTL / 2).await;
    let status = client.status().await.unwrap();
    assert!(status.state.holds(
        &crate::types::res("ds/vol"),
        &Holding::new(b_owner.clone(), epoch),
        crate::cluster::now_ms()
    ));

    for n in &nodes {
        stop_node(n).await;
    }
}

#[test]
fn attached_sets_are_intent_kept_in_order() {
    let mut s = ClusterState::default();
    let set = |nodes: &[&str]| {
        LeaseOp::SetAttached {
            volume: "vol".into(),
            nodes: nodes.iter().map(|n| n.to_string()).collect(),
        }
        .stamp(0)
    };
    assert_eq!(
        s.apply(&set(&["node-1", "node-2", "node-1"])),
        Response::Recorded
    );
    assert_eq!(s.attached["vol"], vec!["node-1", "node-2"]);
    s.apply(&set(&["node-2"]));
    assert_eq!(s.attached["vol"], vec!["node-2"]);
    s.apply(&set(&[]));
    assert!(s.attached.is_empty());
    // A state from before attachments existed still reads.
    let old = r#"{"epoch":3,"leases":{}}"#;
    let st: ClusterState = serde_json::from_str(old).unwrap();
    assert!(st.attached.is_empty());
}

#[test]
fn nodes_attach_and_detach_one_at_a_time() {
    let mut s = ClusterState::default();
    let at = |node: &str, on: bool| {
        LeaseOp::Attach {
            volume: "vol".into(),
            node: node.into(),
            on,
        }
        .stamp(0)
    };
    s.apply(&at("node-1", true));
    s.apply(&at("node-2", true));
    // Sent to every endpoint: applying it twice changes nothing.
    s.apply(&at("node-2", true));
    assert_eq!(s.attached["vol"], vec!["node-1", "node-2"]);
    s.apply(&at("node-1", false));
    s.apply(&at("node-1", false));
    assert_eq!(s.attached["vol"], vec!["node-2"]);
    s.apply(&at("node-2", false));
    assert!(s.attached.is_empty());
    // Detaching from nothing leaves nothing behind.
    s.apply(&at("node-0", false));
    assert!(s.attached.is_empty());
}

// ---- HttpLeases: which nodes a call asks ----

fn released() -> String {
    serde_json::to_string(&Ok::<Response, String>(Response::Released)).unwrap()
}

fn release_op() -> LeaseOp {
    LeaseOp::Release {
        resource: POOL,
        holder: Holding::new("o", ep(1)),
    }
}

/// A node that answers promptly is the only one asked: the others aren't
/// sent copies to forward (each would be applied again).
#[tokio::test]
async fn lease_calls_ask_one_node_while_it_answers() {
    use crate::test_util::fake_json_server;
    use std::sync::atomic::Ordering::SeqCst;
    let quick = std::time::Duration::ZERO;
    let (a, a_hits) = fake_json_server(Some(released()), quick).await;
    let (b, b_hits) = fake_json_server(Some(released()), quick).await;
    let (c, c_hits) = fake_json_server(Some(released()), quick).await;
    let client = LeaseClient::new(vec![a, b, c]);
    for _ in 0..5 {
        assert_eq!(
            client.call(&release_op()).await.unwrap(),
            Response::Released
        );
    }
    assert_eq!(a_hits.load(SeqCst), 5);
    assert_eq!(b_hits.load(SeqCst) + c_hits.load(SeqCst), 0);
}

/// A node that fails sends the call to the others, and the one that
/// answers is asked first from then on; one that's slow is hedged against.
#[tokio::test]
async fn lease_calls_move_to_the_node_that_answers() {
    use crate::test_util::fake_json_server;
    use std::sync::atomic::Ordering::SeqCst;
    let quick = std::time::Duration::ZERO;
    let (dead, dead_hits) = fake_json_server(None, quick).await;
    let (slow, _) = fake_json_server(Some(released()), std::time::Duration::from_secs(4)).await;
    let (good, good_hits) = fake_json_server(Some(released()), quick).await;

    // The first node hangs up: the call goes on to the others.
    let client = LeaseClient::new(vec![dead.clone(), good.clone()]);
    assert_eq!(
        client.call(&release_op()).await.unwrap(),
        Response::Released
    );
    assert_eq!((dead_hits.load(SeqCst), good_hits.load(SeqCst)), (1, 1));
    // ...and next time only the one that answered is asked.
    assert_eq!(
        client.call(&release_op()).await.unwrap(),
        Response::Released
    );
    assert_eq!((dead_hits.load(SeqCst), good_hits.load(SeqCst)), (1, 2));

    // The first node is slow: the others are asked once it's had its
    // chance, well before it would answer.
    let client = LeaseClient::new(vec![slow, good]);
    let t = std::time::Instant::now();
    assert_eq!(
        client.call(&release_op()).await.unwrap(),
        Response::Released
    );
    assert!(
        t.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        t.elapsed()
    );

    // None answers: every node's reason.
    let client = LeaseClient::new(vec![dead.clone(), dead]);
    let e = client.call(&release_op()).await.unwrap_err();
    assert_eq!(e.split("; ").count(), 2, "{}", e);
}

/// A Raft data directory is stamped with the protocol it's written in, and
/// one of another version (or from before versions) isn't opened.
#[test]
fn raft_data_of_another_version_is_refused() {
    use crate::cluster::store::Store;
    let fence = || Arc::new(crate::fence::ServerFence::default());
    let dir = tempfile::tempdir().unwrap();
    let fresh = dir.path().join("fresh");
    Store::open(&fresh, fence()).unwrap();
    let stamp = std::fs::read_to_string(fresh.join("version")).unwrap();
    assert_eq!(stamp.trim(), RAFT_PROTOCOL.to_string());
    Store::open(&fresh, fence()).unwrap();

    std::fs::write(fresh.join("version"), "999\n").unwrap();
    let e = Store::open(&fresh, fence()).err().unwrap();
    assert!(e.contains("protocol 999"), "{}", e);

    let old = dir.path().join("old");
    std::fs::create_dir_all(&old).unwrap();
    std::fs::write(old.join("log.json"), "{}").unwrap();
    let e = Store::open(&old, fence()).err().unwrap();
    assert!(e.contains("older raidish"), "{}", e);
}

fn answer(r: Response) -> String {
    serde_json::to_string(&Ok::<Response, String>(r)).unwrap()
}

/// R3b: a transfer that may have gone through (no answer) or found the
/// lease gone leaves the holder in doubt: not `fresh` (until a renewal
/// says it's still this holder's). Only a refusal puts it back as held.
#[tokio::test]
async fn a_transfer_in_doubt_isnt_fresh() {
    use crate::test_util::fake_json_server;
    let quick = std::time::Duration::ZERO;
    let held = |url: String| {
        HeldLease::adopt(
            LeaseClient::new(vec![url]),
            POOL,
            "o",
            ep(3),
            std::time::Instant::now(),
        )
    };
    let to = || crate::types::Holder::default().with_detail("b");
    for (reply, fresh_after) in [
        (Some(answer(Response::Lost)), false),
        (Some(answer(Response::Refused("no".into()))), true),
        (Some("not json".to_string()), false),
    ] {
        let (url, _) = fake_json_server(reply.clone(), quick).await;
        let l = held(url);
        assert!(l.fresh());
        assert!(l.transfer("b", to()).await.is_err());
        assert_eq!(l.fresh(), fresh_after, "{:?}", reply);
    }
}

/// I-15: once a renewal is refused (the lease was taken or released), the
/// holder isn't `fresh`, even though its last renewal is recent.
#[tokio::test]
async fn a_lost_lease_isnt_fresh() {
    use crate::test_util::fake_json_server;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let (url, _) = fake_json_server(Some(answer(Response::Lost)), std::time::Duration::ZERO).await;
    let l = std::sync::Arc::new(HeldLease::adopt(
        LeaseClient::new(vec![url]),
        POOL,
        "o",
        ep(3),
        // Late enough that `hold` renews at once.
        std::time::Instant::now() - lease::LEASE_TTL / 2,
    ));
    assert!(l.fresh());
    let lost = std::sync::Arc::new(AtomicBool::new(false));
    let flag = lost.clone();
    l.hold(move |_| flag.store(true, SeqCst));
    assert!(wait_until(2, || lost.load(SeqCst)).await, "on_lost ran");
    assert!(!l.fresh(), "fresh after the lease was lost");
    assert!(
        l.transfer("b", crate::types::Holder::default())
            .await
            .is_err()
    );
}

/// I-15: a lease released while a renewal is failing isn't reported lost
/// when the renewals run out: it was let go of, not lost.
#[tokio::test]
async fn a_released_lease_isnt_reported_lost() {
    use crate::test_util::fake_json_server;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    use std::time::Duration;
    // Nothing answers in time.
    let (url, hits) = fake_json_server(Some(answer(Response::Lost)), Duration::from_secs(5)).await;
    let l = std::sync::Arc::new(HeldLease::adopt(
        LeaseClient::new(vec![url]),
        POOL,
        "o",
        ep(3),
        // The renewal sent at once is the last one there's time for.
        std::time::Instant::now() - (lease::LEASE_TTL * 4 / 5 - Duration::from_millis(400)),
    ));
    let lost = std::sync::Arc::new(AtomicBool::new(false));
    let flag = lost.clone();
    l.hold(move |_| flag.store(true, SeqCst));
    assert!(wait_until(2, || hits.load(SeqCst) > 0).await, "renewing");
    let r = l.clone();
    tokio::spawn(async move { r.release().await });
    // Past the renewal's deadline.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(!lost.load(SeqCst), "on_lost ran for a released lease");
}

/// R3a: a lease adopted well into its TTL is renewed at once, not a third
/// of a TTL after `hold` (by then too late to try: lost unrenewed).
#[tokio::test]
async fn a_late_hold_renews_at_once() {
    use crate::test_util::fake_json_server;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let (url, hits) = fake_json_server(
        Some(answer(Response::Granted {
            epoch: ep(3),
            expires_ms: u64::MAX,
        })),
        std::time::Duration::ZERO,
    )
    .await;
    let l = std::sync::Arc::new(HeldLease::adopt(
        LeaseClient::new(vec![url]),
        POOL,
        "o",
        ep(3),
        std::time::Instant::now() - lease::LEASE_TTL / 2,
    ));
    let lost = std::sync::Arc::new(AtomicBool::new(false));
    let flag = lost.clone();
    l.hold(move |_| flag.store(true, SeqCst));
    assert!(
        wait_until(2, || hits.load(SeqCst) > 0).await,
        "renewed at once"
    );
    assert!(l.fresh());
    assert!(!lost.load(SeqCst));
}

/// R1b / R2b: a linearizable read, from the leader or a follower, has
/// everything committed before it; a disk server's fence becomes ready
/// from one (learning the pool epoch); and with no quorum there's no
/// read, so a fence that starts then stays `Starting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_reads_and_the_fence_barrier() {
    use crate::fence::{Ready, ServerFence};
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(wait_until(10, || leader_of(&all).is_some()).await);
    let client = LeaseClient::new(vec![nodes[0].url.clone()]);
    let held = HeldLease::acquire(client, POOL, crate::types::Holder::default())
        .await
        .unwrap();
    let t = Duration::from_secs(5);
    for n in &nodes {
        let state = n.cluster.linearizable_state(t).await.unwrap();
        assert_eq!(state.pool_epoch(), held.epoch, "on {}", n.cluster.name);
        let fence = Arc::new(ServerFence::default());
        assert_eq!(fence.readiness(), Ready::Starting);
        fence.mark_unmapped();
        tokio::time::timeout(t, crate::api::sync_fence(n.cluster.clone(), fence.clone()))
            .await
            .unwrap();
        assert_eq!(fence.readiness(), Ready::Ready);
        assert_eq!(fence.pool_floor(), held.epoch);
    }

    // Quorum lost: a lone node has no linearizable read.
    stop_node(&nodes[1]).await;
    stop_node(&nodes[2]).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        nodes[0]
            .cluster
            .linearizable_state(Duration::from_secs(2))
            .await
            .is_err()
    );
    let fence = Arc::new(ServerFence::default());
    fence.mark_unmapped();
    let r = tokio::time::timeout(
        Duration::from_secs(3),
        crate::api::sync_fence(nodes[0].cluster.clone(), fence.clone()),
    )
    .await;
    assert!(r.is_err(), "the barrier passed with no quorum");
    assert_eq!(fence.readiness(), Ready::Starting);
    stop_node(&nodes[0]).await;
}

/// A linearizable read asked for before there's a leader (a cluster just
/// started, or mid-election) waits for one, up to its timeout, rather
/// than failing at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_linearizable_read_waits_for_a_leader() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let reads = nodes
        .iter()
        .map(|n| n.cluster.linearizable_state(Duration::from_secs(15)));
    for (n, r) in nodes.iter().zip(futures::future::join_all(reads).await) {
        assert!(r.is_ok(), "on {}: {:?}", n.cluster.name, r.err());
    }
    for n in &nodes {
        stop_node(n).await;
    }
}

/// Three nodes formed and holding a pool lease: (nodes, the lease's epoch).
async fn formed(
    dir: &std::path::Path,
    pool: &[String],
    ports: &[u16; 3],
) -> (Vec<TestNode>, Epoch) {
    let nodes = vec![
        start_node("n1", pool, ports[0], dir).await,
        start_node("n2", pool, ports[1], dir).await,
        start_node("n3", pool, ports[2], dir).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(
        wait_until(15, || leader_of(&all).is_some()).await,
        "no leader"
    );
    assert!(wait_until(10, || nodes.iter().all(|n| n.cluster.status().voting)).await);
    let client = LeaseClient::new(nodes.iter().map(|n| n.url.clone()).collect());
    let a = HeldLease::acquire(
        client,
        POOL,
        crate::types::Holder::default().with_detail("a"),
    )
    .await
    .unwrap();
    let epoch = a.epoch;
    assert!(wait_until(5, || nodes.iter().all(|n| n.floor.pool_floor() == epoch)).await);
    std::mem::forget(a);
    (nodes, epoch)
}

/// R4b: a node without Raft state never forms a group on its own, neither
/// on a first start (until every member is there and new) nor after being
/// wiped while the others are down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_state_never_forms_a_group_alone() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    // First start, alone: it waits for the others.
    let n1 = start_node("n1", &pool, ports[0], dir.path()).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let s = n1.cluster.status();
    assert!(!s.initialized && !s.voting, "{:?}", s);
    stop_node(&n1).await;
    drop(n1);

    let (nodes, _) = formed(dir.path(), &pool, &ports).await;
    for n in &nodes {
        stop_node(n).await;
    }
    drop(nodes);
    // n3 wiped, the others down: it must not start a group of its own.
    std::fs::remove_dir_all(dir.path().join("n3")).unwrap();
    let n3 = start_node("n3", &pool, ports[2], dir.path()).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let s = n3.cluster.status();
    assert!(!s.initialized && !s.voting, "{:?}", s);
    stop_node(&n3).await;
}

/// R4b: a wiped node comes back while the leader is down. The survivor
/// alone isn't a quorum, and the wiped node has lost the log it
/// acknowledged and its votes: it must not vote for the survivor (which
/// may be missing entries committed with the wiped node's help). Once the
/// old leader is back, it catches up and votes again, with the group's
/// state and membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wiped_node_catches_up_before_it_votes() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let (nodes, epoch) = formed(dir.path(), &pool, &ports).await;
    let leader = leader_of(&nodes.iter().collect::<Vec<_>>()).unwrap();
    let mut rest = nodes.iter().filter(|n| n.cluster.name != leader);
    let (survivor, wiped) = (rest.next().unwrap(), rest.next().unwrap());
    let (wiped_name, wiped_port) = (
        wiped.cluster.name.clone(),
        ports[pool_index(&nodes, &wiped.cluster.name)],
    );
    let leader_port = ports[pool_index(&nodes, &leader)];
    let old_leader = nodes.iter().find(|n| n.cluster.name == leader).unwrap();
    stop_node(old_leader).await;
    stop_node(wiped).await;
    std::fs::remove_dir_all(dir.path().join(&wiped_name)).unwrap();
    let back = start_node(&wiped_name, &pool, wiped_port, dir.path()).await;

    // The survivor's elections get no vote from it.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    assert!(
        leader_of(&[survivor, &back]).is_none(),
        "elected with the vote of a node that lost its state"
    );
    assert!(!back.cluster.status().voting);

    // The old leader returns: a leader again, and the wiped node catches up.
    let returned = start_node(&leader, &pool, leader_port, dir.path()).await;
    let three = [survivor, &back, &returned];
    assert!(
        wait_until(15, || leader_of(&three).is_some()).await,
        "no leader"
    );
    assert!(
        wait_until(20, || back.cluster.status().voting).await,
        "never caught up"
    );
    assert_eq!(back.floor.pool_floor(), epoch);
    assert!(!dir.path().join(&wiped_name).join(CATCHING_UP).exists());
    let voters: Vec<NodeId> = back
        .cluster
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(voters, back.cluster.members.ids().collect::<Vec<_>>());
    for n in [survivor, &back, &returned] {
        stop_node(n).await;
    }
}

fn pool_index(nodes: &[TestNode], name: &str) -> usize {
    nodes.iter().position(|n| n.cluster.name == name).unwrap()
}

/// R4b: on a new group, a node that initialized but stopped before it
/// started voting (its marker still there) is still a new group's member.
/// Both nodes of a 2-node group in that state must elect a leader once
/// restarted, not wait forever to catch up from a leader only they can elect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_between_initializing_and_voting_still_forms_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port()];
    let pool = urls(&ports);
    // Each alone (so its bootstrap waits), initialized by hand: the state
    // a stop right after `initialize` leaves.
    for (i, name) in ["n1", "n2"].iter().enumerate() {
        let n = start_node(name, &pool, ports[i], dir.path()).await;
        let initial: BTreeMap<NodeId, BasicNode> = n
            .cluster
            .members
            .ids()
            .map(|id| (id, BasicNode::default()))
            .collect();
        n.cluster.raft.initialize(initial).await.unwrap();
        assert!(wait_until(5, || n.cluster.status().initialized).await);
        assert!(!n.cluster.status().voting);
        stop_node(&n).await;
    }
    for name in ["n1", "n2"] {
        assert!(dir.path().join(name).join(CATCHING_UP).exists());
    }
    let nodes = [
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(
        wait_until(15, || leader_of(&all).is_some()).await,
        "no leader"
    );
    assert!(wait_until(5, || nodes.iter().all(|n| n.cluster.status().voting)).await);
    for n in &nodes {
        stop_node(n).await;
    }
}

/// A good node that hears only from a misconfigured peer at first (the
/// other good one not up yet) must not take itself for the odd one out:
/// it would sit out for good. Node 3's config swaps nodes 2 and 3, so it
/// answers node 1 as node 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_good_node_hearing_only_a_bad_peer_doesnt_sit_out() {
    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let mut odd = pool.clone();
    odd.swap(1, 2);
    let bad = start_node("n3", &odd, ports[2], dir.path()).await;
    let n1 = start_node("n1", &pool, ports[0], dir.path()).await;
    assert!(
        n1.cluster.check_peer(nid(2)).await.unwrap().is_some(),
        "node 1 sees node 3 at another id"
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!n1.cluster.misconfigured());
    // Node 2 comes up: node 1 still takes part.
    let n2 = start_node("n2", &pool, ports[1], dir.path()).await;
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    assert!(!n1.cluster.misconfigured() && !n2.cluster.misconfigured());
    for n in [&n1, &n2, &bad] {
        stop_node(n).await;
    }
}

/// A migration's record (I-29): begun once, the same one again answered
/// as the first time, another refused while it's there; ended only as
/// itself, and ending it again is fine.
#[test]
fn migrations_are_recorded_once_and_ended_as_themselves() {
    use crate::types::{DatasetName, VmName};
    let mut s = ClusterState::default();
    let vm = VmName::from_static("mig");
    let vols = vec![DatasetName::from_static("mig")];
    let begin = |from: &str, to: &str| {
        LeaseOp::BeginMigration {
            vm: vm.clone(),
            from: from.into(),
            to: to.into(),
            volumes: vols.clone(),
        }
        .stamp(5)
    };
    let end = |from: &str, to: &str| {
        LeaseOp::EndMigration {
            vm: vm.clone(),
            from: from.into(),
            to: to.into(),
        }
        .stamp(6)
    };
    assert_eq!(s.apply(&begin("node-0", "node-1")), Response::Recorded);
    assert_eq!(s.apply(&begin("node-0", "node-1")), Response::Recorded);
    assert!(matches!(
        s.apply(&begin("node-1", "node-2")),
        Response::Refused(_)
    ));
    let m = &s.migrations[&vm];
    assert_eq!(
        (m.from.as_str(), m.to.as_str(), m.since_ms),
        ("node-0", "node-1", 5)
    );
    assert!(matches!(
        s.apply(&end("node-1", "node-2")),
        Response::Refused(_)
    ));
    assert!(s.migrations.contains_key(&vm));
    assert_eq!(s.apply(&end("node-0", "node-1")), Response::Recorded);
    assert!(s.migrations.is_empty());
    assert_eq!(s.apply(&end("node-0", "node-1")), Response::Recorded);
    // A state from before migrations were recorded still reads.
    let old = r#"{"epoch":3,"leases":{}}"#;
    let st: ClusterState = serde_json::from_str(old).unwrap();
    assert!(st.migrations.is_empty());
}

/// H-10: `HeldLease::adopt_renewed` refreshes the adopted lease's clock from a
/// synchronous `Renew` after the transfer, so a slow sender drain/sync doesn't
/// cause `HeldLease::hold()` to immediately expire the brand-new lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h10_adopt_renewed_starts_clock_fresh_after_slow_transfer() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port(), free_port()];
    let pool = urls(&ports);
    let nodes = vec![
        start_node("n1", &pool, ports[0], dir.path()).await,
        start_node("n2", &pool, ports[1], dir.path()).await,
        start_node("n3", &pool, ports[2], dir.path()).await,
    ];
    let all: Vec<&TestNode> = nodes.iter().collect();
    assert!(wait_until(10, || leader_of(&all).is_some()).await);
    let client = LeaseClient::new(nodes.iter().map(|n| n.url.clone()).collect());
    let resource = crate::types::res("ds/handoff-h10");
    let a = Arc::new(
        HeldLease::acquire(
            client.clone(),
            resource.clone(),
            crate::types::Holder::default().with_detail("sender"),
        )
        .await
        .unwrap(),
    );
    let b_owner = lease::owner_id() + ":receiver";
    let new_epoch = a
        .transfer(
            &b_owner,
            crate::types::Holder::default().with_detail("receiver"),
        )
        .await
        .unwrap();

    // Even if the caller started waiting for this handoff long ago, adopting
    // via `adopt_renewed` starts the lease clock from the synchronous `Renew`.
    let b = Arc::new(
        HeldLease::adopt_renewed(client.clone(), resource.clone(), &b_owner, new_epoch)
            .await
            .unwrap(),
    );
    assert!(b.fresh(), "adopted lease must be immediately fresh");
    let lost = Arc::new(AtomicBool::new(false));
    let flag = lost.clone();
    b.hold(move |_| flag.store(true, Ordering::SeqCst));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !lost.load(Ordering::SeqCst),
        "adopted lease must not be declared lost"
    );

    // Adopting an owner/epoch that doesn't hold the lease must be refused.
    let bad = HeldLease::adopt_renewed(client, resource, "wrong-owner", new_epoch).await;
    assert!(bad.is_err());

    b.release().await;
    drop(nodes);
}
