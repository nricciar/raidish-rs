use super::lease::{HeldLease, LeaseClient};
use super::state::{ClusterState, POOL, Request, Response};
use super::*;
use crate::test_util::wait_until;
use std::sync::atomic::Ordering;

fn acquire(owner: &str, now_ms: u64, ttl_ms: u64) -> Request {
    Request::Acquire {
        resource: POOL.into(),
        owner: owner.into(),
        desc: format!("test {}", owner),
        ttl_ms,
        now_ms,
    }
}

fn renew(owner: &str, epoch: u64, now_ms: u64, ttl_ms: u64) -> Request {
    Request::Renew {
        resource: POOL.into(),
        owner: owner.into(),
        epoch,
        ttl_ms,
        now_ms,
    }
}

#[test]
fn lease_grant_hold_expire() {
    let mut s = ClusterState::default();
    assert_eq!(
        s.apply(&acquire("a", 0, 100)),
        Response::Granted {
            epoch: 1,
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
            epoch: 1,
            expires_ms: 160
        }
    );
    // Renew extends.
    assert_eq!(
        s.apply(&renew("a", 1, 100, 100)),
        Response::Granted {
            epoch: 1,
            expires_ms: 200
        }
    );
    // After expiry, someone else gets it at a new epoch.
    assert_eq!(
        s.apply(&acquire("b", 200, 100)),
        Response::Granted {
            epoch: 2,
            expires_ms: 300
        }
    );
    // The old holder can't renew any more.
    assert_eq!(s.apply(&renew("a", 1, 210, 100)), Response::Lost);
    assert_eq!(s.pool_epoch(), 2);
}

#[test]
fn renew_after_expiry_without_takeover() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 100));
    // Nobody took it, so the epoch is still ours.
    assert_eq!(
        s.apply(&renew("a", 1, 500, 100)),
        Response::Granted {
            epoch: 1,
            expires_ms: 600
        }
    );
}

#[test]
fn release_frees_but_keeps_epoch() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 1000));
    let release = Request::Release {
        resource: POOL.into(),
        owner: "a".into(),
        epoch: 1,
    };
    assert_eq!(s.apply(&release), Response::Released);
    assert_eq!(s.pool_epoch(), 1, "the floor never goes back");
    assert!(matches!(
        s.apply(&acquire("b", 10, 1000)),
        Response::Granted { epoch: 2, .. }
    ));
    // Renewing a released lease fails.
    assert_eq!(s.apply(&renew("a", 1, 20, 1000)), Response::Lost);
}

#[test]
fn epochs_increase_across_resources() {
    let mut s = ClusterState::default();
    s.apply(&acquire("a", 0, 10));
    let other = Request::Acquire {
        resource: "vol/vm1".into(),
        owner: "b".into(),
        desc: String::new(),
        ttl_ms: 10,
        now_ms: 0,
    };
    assert!(matches!(
        s.apply(&other),
        Response::Granted { epoch: 2, .. }
    ));
    assert!(matches!(
        s.apply(&acquire("c", 100, 10)),
        Response::Granted { epoch: 3, .. }
    ));
}

// ---- membership from the pool config ----

use super::members::{Members, derive, is_local_base};

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
    assert_eq!(m.me, 0);
    assert_eq!(
        m.peers,
        BTreeMap::from([(1, "http://b:8881".into()), (2, "http://c:8881".into())])
    );
    assert_eq!(m.disks[&0], vec![0, 3]);
    assert_eq!(m.disks[&1], vec![1, 4]);
    assert_eq!(m.disks[&2], vec![2]);
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
    assert_eq!((a.me, b.me, c.me), (0, 1, 2));
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
    assert_eq!(m.me, 0);
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
    floor: Arc<AtomicU64>,
    shutdown: rocket::Shutdown,
    url: String,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Start a node whose pool config is `paths` (every node gets the same
/// URLs; each finds itself by port).
async fn start_node(name: &str, paths: &[String], port: u16, dir: &std::path::Path) -> TestNode {
    let floor = Arc::new(AtomicU64::new(0));
    let members = derive(paths, "test", |b| is_local_base(b, port)).unwrap();
    let cluster = Cluster::start(name, members, dir.join(name), floor.clone())
        .await
        .unwrap();
    let rocket = rocket::build()
        .manage(cluster.clone())
        .configure(
            rocket::Config::figment()
                .merge(("port", port))
                .merge(("log_level", "off")),
        )
        .mount("/", routes::routes())
        .ignite()
        .await
        .unwrap();
    let shutdown = rocket.shutdown();
    tokio::spawn(rocket.launch());
    TestNode {
        cluster,
        floor,
        shutdown,
        url: format!("http://127.0.0.1:{}", port),
    }
}

async fn stop_node(node: &TestNode) {
    node.shutdown.clone().notify();
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
    let a = HeldLease::acquire(client.clone(), POOL, "writer a")
        .await
        .unwrap();
    let err = HeldLease::acquire(client.clone(), POOL, "writer b")
        .await
        .err()
        .unwrap();
    assert!(err.contains("writer a"), "{}", err);

    // Every node's fencing floor learns the epoch.
    assert!(
        wait_until(5, || nodes
            .iter()
            .all(|n| n.floor.load(Ordering::SeqCst) == a.epoch))
        .await
    );

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
            resource: POOL.into(),
            owner: a.owner.clone(),
            epoch: a.epoch,
            ttl_ms: 300,
        })
        .await
        .unwrap();
    assert!(matches!(renewed, Response::Granted { epoch, .. } if epoch == a.epoch));

    // Released: a new writer gets a newer epoch.
    a.release().await;
    let b = HeldLease::acquire(survivors.clone(), POOL, "writer b")
        .await
        .unwrap();
    assert!(b.epoch > a.epoch);
    assert!(
        wait_until(5, || rest
            .iter()
            .all(|n| n.floor.load(Ordering::SeqCst) == b.epoch))
        .await
    );
    // The old writer's lease is gone for good.
    let lost = survivors
        .call(&LeaseOp::Renew {
            resource: POOL.into(),
            owner: a.owner.clone(),
            epoch: a.epoch,
            ttl_ms: 1000,
        })
        .await
        .unwrap();
    assert_eq!(lost, Response::Lost);

    // A lease that isn't renewed expires and can be taken over.
    let short = |owner: &str| LeaseOp::Acquire {
        resource: "short".into(),
        owner: owner.into(),
        desc: owner.into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_restarts_with_its_state() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let pool: Vec<String> = vec!["disks/a".into(), "disks/b".into()];
    let node = start_node("solo", &pool, port, dir.path()).await;
    let client = LeaseClient::new(vec![node.url.clone()]);
    assert!(wait_until(10, || leader_of(&[&node]).is_some()).await);
    let a = HeldLease::acquire(client.clone(), POOL, "a").await.unwrap();
    stop_node(&node).await;
    drop(node);

    // Same data dir: the epoch (and so the fencing floor) is remembered.
    let node = start_node("solo", &pool, port, dir.path()).await;
    assert_eq!(node.floor.load(Ordering::SeqCst), a.epoch);
    assert!(wait_until(10, || leader_of(&[&node]).is_some()).await);
    let err = HeldLease::acquire(client.clone(), POOL, "b").await.err();
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
    let a = HeldLease::acquire(client, POOL, "a").await.unwrap();
    assert!(
        wait_until(5, || good
            .iter()
            .all(|n| n.floor.load(Ordering::SeqCst) == a.epoch))
        .await
    );
    // The odd one never joins in.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(nodes[2].floor.load(Ordering::SeqCst), 0);
    assert!(
        nodes[2]
            .cluster
            .raft
            .metrics()
            .borrow()
            .current_leader
            .is_none()
    );
    assert!(nodes[2].cluster.check_peer(0).await.unwrap().is_some());
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
    let problem = nodes[2].cluster.check_peer(0).await.unwrap();
    assert!(problem.is_some_and(|p| p.contains("different orders")));
    assert_eq!(nodes[0].cluster.check_peer(1).await.unwrap(), None);
    assert_eq!(nodes[0].cluster.check_peer(2).await.unwrap(), None);

    // A Raft RPC addressed to the wrong node id is refused.
    let vote = openraft::raft::VoteRequest::<u64> {
        vote: openraft::Vote::new(1, 2),
        last_log_id: None,
    };
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/raft/vote", nodes[1].url))
        .header(FINGERPRINT_HEADER, &nodes[1].cluster.members.fingerprint)
        .header(TO_HEADER, "0")
        .json(&vote)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);

    // The agreeing two still work.
    let good: Vec<&TestNode> = nodes[..2].iter().collect();
    assert!(wait_until(10, || leader_of(&good).is_some()).await);
    for n in &nodes {
        stop_node(n).await;
    }
}
