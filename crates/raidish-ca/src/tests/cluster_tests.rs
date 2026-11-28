//! The CA-1 flow on an in-process cluster: `ca init`, node 0 staged and
//! answering joins before there's a quorum, the hand-over to Raft, and the
//! refusals (wrong root, occupied member, unMACed enrollment, CA ops on the
//! lease API).
//!
//! The process's TLS client config is global (`raidish_auth::tls::init`), so
//! Raft runs on plain HTTP here, each node on its own port. Each node also
//! serves the CA (and cluster) routes over mTLS on a second port with its
//! own certificate, as `listen` does on its one port: that's what the
//! joiners, the admin and the nodes' own certificates are tested against.

use crate::api::{EnrollmentCreated, EnrollmentRequest, Issued, RenewRequest};
use crate::init::{InitOptions, init};
use crate::token::JoinToken;
use crate::{CaOptions, CaService};
use raidish_auth::tls::TlsConfig;
use raidish_cluster::Cluster;
use raidish_cluster::members::{Members, derive, is_local_base};
use raidish_cluster::state::{LeaseOp, Response};
use raidish_types::api_types::{EventKind, Level, Subject};
use raidish_types::pki::{EnrollScope, Enrollment, PkiOp, ProfileKind, RevocationReason, TokenId};
use raidish_types::types::{Epoch, Grant, MetaslabIdx, NodeId};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

struct NoFence;
impl raidish_types::types::FenceObserver for NoFence {
    fn observe_grants(&self, _: Epoch, _: &mut dyn Iterator<Item = (MetaslabIdx, Grant)>) {}
}

use raidish_auth::testing::free_port;

fn nid(i: u64) -> NodeId {
    NodeId::from_raw(i)
}

async fn wait_for(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < end {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

/// The activity log, as a node's `EventSink` would keep it.
#[derive(Default)]
struct TestEvents {
    log: std::sync::Mutex<Vec<(EventKind, Subject, String)>>,
    last: std::sync::Mutex<std::collections::HashMap<Subject, String>>,
}

impl TestEvents {
    fn of(&self, kind: EventKind) -> Vec<(Subject, String)> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _, _)| *k == kind)
            .map(|(_, s, m)| (s.clone(), m.clone()))
            .collect()
    }
}

impl crate::CaEvents for TestEvents {
    fn emit(
        &self,
        _: Level,
        kind: EventKind,
        subject: Subject,
        message: String,
        _: Option<String>,
    ) {
        self.log.lock().unwrap().push((kind, subject, message));
    }
    fn once(&self, level: Level, kind: EventKind, subject: Subject, message: String) -> bool {
        if self.last.lock().unwrap().get(&subject) == Some(&message) {
            return false;
        }
        self.last
            .lock()
            .unwrap()
            .insert(subject.clone(), message.clone());
        self.emit(level, kind, subject, message, None);
        true
    }
    fn clear(&self, subject: &Subject) -> bool {
        self.last.lock().unwrap().remove(subject).is_some()
    }
}

struct Node {
    events: Arc<TestEvents>,
    cluster: Arc<Cluster>,
    ca: Arc<CaService>,
    /// Its live revocations, as `listen` wires them.
    trust: Arc<raidish_auth::trust::Trust>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Its mTLS address.
    url: String,
}

impl Node {
    async fn stop(&self) {
        // One permit per server (each waits for one).
        self.shutdown.notify_one();
        self.shutdown.notify_one();
        let _ = self.cluster.raft.shutdown().await;
    }
}

fn tls_in(dir: &Path) -> TlsConfig {
    TlsConfig {
        dir: Some(dir.to_string_lossy().into()),
        ..Default::default()
    }
}

/// An HTTPS client presenting the bundle in `dir`.
fn client(dir: &Path) -> reqwest::Client {
    reqwest::Client::builder()
        .use_preconfigured_tls(tls_in(dir).client_config().unwrap())
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap()
}

fn members(paths: &[String], port: u16) -> Members {
    derive(paths, "test", |b| is_local_base(b, port)).unwrap()
}

/// Serve `app` on `port` until `stop` is notified.
async fn launch(
    port: u16,
    tls: Option<raidish_auth::http::Server>,
    app: axum::Router,
    stop: Arc<tokio::sync::Notify>,
) {
    raidish_auth::testing::spawn_on(port, tls, app, async move { stop.notified().await })
        .await
        .unwrap();
}

/// Start a member: Raft (and the CA routes, as peers forward to them) on
/// `raft_port`, the CA and cluster routes over mTLS with `tls_dir`'s
/// certificate on `tls_port`.
async fn start(
    paths: &[String],
    raft_port: u16,
    tls_port: u16,
    root: &Path,
    tls_dir: &Path,
) -> Node {
    start_with(paths, raft_port, tls_port, root, tls_dir, None).await
}

async fn start_with(
    paths: &[String],
    raft_port: u16,
    tls_port: u16,
    root: &Path,
    tls_dir: &Path,
    node_key: Option<crate::NodeKeySource>,
) -> Node {
    start_full(paths, raft_port, tls_port, root, tls_dir, node_key, None).await
}

/// How a node renews its own certificate in a test: certificates of
/// `.0` seconds, the peers' mTLS ports `.1` (by member index).
type Renewing = (u64, [u16; 3]);

async fn start_full(
    paths: &[String],
    raft_port: u16,
    tls_port: u16,
    root: &Path,
    tls_dir: &Path,
    node_key: Option<crate::NodeKeySource>,
    renewing: Option<Renewing>,
) -> Node {
    let m = members(paths, raft_port);
    let name = m.me.name();
    let me = m.me;
    // As `listen`: one live identity for the server and the client.
    let mut tls = tls_in(tls_dir);
    let mut renew_opts = (None, None, None);
    if let Some((life, ports)) = renewing {
        let (chain, key) = tls.identity().unwrap().unwrap();
        let live = raidish_auth::http::LiveIdentity::new(chain, key).unwrap();
        tls.live = Some(live.clone());
        let peers = (0..3u64)
            .map(|i| (nid(i), format!("https://127.0.0.1:{}", ports[i as usize])))
            .filter(|(n, _)| *n != me)
            .collect();
        renew_opts = (
            Some(life),
            Some(crate::NodeIdentity {
                live,
                cert_path: tls.cert_path().unwrap(),
                key_path: tls.key_path().unwrap(),
            }),
            Some((Arc::new(tls.client_config().unwrap()), peers)),
        );
    }
    let cluster = Cluster::start(&name, m, root.join(&name), Arc::new(NoFence))
        .await
        .unwrap();
    let events = Arc::new(TestEvents::default());
    let ca = CaService::start(
        cluster.clone(),
        CaOptions {
            kdf: (8, 1, 1),
            node_key,
            events: Some(events.clone()),
            node_life_s: renew_opts.0,
            identity: renew_opts.1,
            peers: renew_opts.2,
            ..Default::default()
        },
    )
    .unwrap();
    use raidish_auth::auth::AuthState;
    use raidish_auth::http::finish;
    let routes = || {
        raidish_cluster::routes::router(cluster.clone()).merge(crate::routes::router(ca.clone()))
    };
    let shutdown = Arc::new(tokio::sync::Notify::new());
    launch(
        raft_port,
        None,
        finish(routes(), AuthState(None)),
        shutdown.clone(),
    )
    .await;
    let trust = raidish_auth::trust::Trust::new(vec![]);
    cluster.observe_pki(trust.clone());
    tls.trust = Some(trust.clone());
    let verifier = raidish_auth::auth::Verifier::from_config(&tls, &Default::default()).unwrap();
    let secure = finish(
        routes().merge(raidish_auth::auth::router()),
        AuthState(Some(verifier)),
    );
    launch(
        tls_port,
        Some(raidish_auth::http::Server::from_config(&tls).unwrap()),
        secure,
        shutdown.clone(),
    )
    .await;
    Node {
        events,
        cluster,
        ca,
        trust,
        shutdown,
        url: format!("https://127.0.0.1:{}", tls_port),
    }
}

async fn whoami(c: &reqwest::Client, url: &str) -> serde_json::Value {
    c.get(format!("{}/api/v1/auth/whoami", url))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn make_token(admin: &reqwest::Client, url: &str, req: &EnrollmentRequest) -> JoinToken {
    let r = admin
        .post(format!("{}/api/v1/pki/enrollments", url))
        .json(req)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let e: EnrollmentCreated = r.json().await.unwrap();
    JoinToken::parse(&e.token).unwrap()
}

fn node_token(member: u64, replace: bool) -> EnrollmentRequest {
    EnrollmentRequest {
        member: nid(member),
        extra_sans: vec![],
        replace,
        compromised: false,
        ttl_secs: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_join_hand_over_and_refusals() {
    raidish_auth::tls::install_provider();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("raft");
    let raft = [free_port(), free_port(), free_port()];
    let tls = [free_port(), free_port(), free_port()];
    let paths: Vec<String> = raft
        .iter()
        .enumerate()
        .map(|(i, p)| format!("http://127.0.0.1:{}/api/v1/disks/{}", p, i))
        .collect();
    let tls_dirs: Vec<PathBuf> = (0..3)
        .map(|i| dir.path().join(format!("tls{}", i)))
        .collect();
    let admin_dir = dir.path().join("admin");

    // ---- ca init, on node 0 ----
    let m0 = members(&paths, raft[0]);
    let out = init(&InitOptions {
        members: &m0,
        data_dir: &root.join("node-0"),
        sans: vec!["127.0.0.1".into()],
        peer_sans: [(nid(2), vec!["10.9.9.2".to_string()])].into(),
        node_days: None,
        admin_name: "admin".into(),
        token_ttl_ms: 3_600_000,
        unseal_once: true,
        force: false,
        kdf: (8, 1, 1),
    })
    .unwrap();
    out.node0.write_dir(&tls_dirs[0]).unwrap();
    out.admin.write_dir(&admin_dir).unwrap();
    assert_eq!(out.tokens.len(), 2);
    let tokens: Vec<JoinToken> = out
        .tokens
        .iter()
        .map(|t| JoinToken::parse(&t.token).unwrap())
        .collect();
    let staged = crate::bootstrap_path(&root.join("node-0"));
    let once = crate::unseal_once_path(&root.join("node-0"));
    assert!(staged.exists() && once.exists());
    // A second init doesn't clobber the first.
    assert!(
        init(&InitOptions {
            members: &m0,
            data_dir: &root.join("node-0"),
            sans: vec!["127.0.0.1".into()],
            peer_sans: Default::default(),
            node_days: None,
            admin_name: "admin".into(),
            token_ttl_ms: 1,
            unseal_once: true,
            force: false,
            kdf: (8, 1, 1),
        })
        .is_err()
    );

    // ---- node 0 alone: staged, unsealed by the one-shot file ----
    let n0 = start(&paths, raft[0], tls[0], &root, &tls_dirs[0]).await;
    assert!(!once.exists(), "the one-shot unseal file is used up");
    assert!(n0.ca.unsealed());
    let st = n0.ca.status().await;
    assert!(st.staged && st.unsealed);
    assert!(n0.cluster.pki().is_none(), "no quorum, nothing in Raft");
    let admin = client(&admin_dir);
    assert_eq!(whoami(&admin, &n0.url).await["role"], "admin");

    // A token naming another root fails before it's sent.
    let wrong = JoinToken {
        root: raidish_types::pki::CertFingerprint([7; 32]),
        id: tokens[0].id.clone(),
        secret: tokens[0].secret.clone(),
    };
    let m1 = members(&paths, raft[1]);
    let m2 = members(&paths, raft[2]);
    let e = match crate::join::join(&n0.url, &wrong, m1.me, &m1.fingerprint).await {
        Err(e) => e,
        Ok(_) => panic!("joined with the wrong root"),
    };
    assert!(e.contains("another CA"), "{}", e);
    assert!(
        n0.ca
            .current()
            .await
            .unwrap()
            .enrollments
            .contains_key(&tokens[0].id),
        "the token is untouched"
    );
    // Node 1's token is node 1's.
    assert!(
        crate::join::join(&n0.url, &tokens[0], m2.me, &m2.fingerprint)
            .await
            .is_err()
    );

    // ---- the peers join before there's a quorum ----
    let j1 = crate::join::join(&n0.url, &tokens[0], m1.me, &m1.fingerprint)
        .await
        .unwrap();
    j1.bundle.write_dir(&tls_dirs[1]).unwrap();
    let j2 = crate::join::join(&n0.url, &tokens[1], m2.me, &m2.fingerprint)
        .await
        .unwrap();
    j2.bundle.write_dir(&tls_dirs[2]).unwrap();
    // Single-use.
    assert!(
        crate::join::join(&n0.url, &tokens[0], m1.me, &m1.fingerprint)
            .await
            .is_err()
    );
    let staged_state = n0.ca.current().await.unwrap();
    assert_eq!(staged_state.certs.len(), 4, "node 0, admin, node 1, node 2");
    assert!(staged_state.enrollments.is_empty());
    let node2_cert = staged_state.certs.get(&j2.serial).unwrap();
    assert!(node2_cert.identity.sans.contains(&"10.9.9.2".to_string()));
    assert_eq!(node2_cert.identity.member, Some(nid(2)));

    // ---- quorum: the staged CA goes into Raft ----
    let n1 = start(&paths, raft[1], tls[1], &root, &tls_dirs[1]).await;
    let n2 = start(&paths, raft[2], tls[2], &root, &tls_dirs[2]).await;
    let nodes = [&n0, &n1, &n2];
    assert!(
        wait_for(30, || nodes.iter().all(|n| n.cluster.pki().is_some())).await,
        "the CA reached every node"
    );
    assert!(
        wait_for(10, || !staged.exists()).await,
        "pki_bootstrap.json is gone"
    );
    assert!(!n0.ca.status().await.staged);
    assert_eq!(n2.cluster.pki().unwrap().certs.len(), 4);
    assert!(!n1.ca.unsealed() && !n2.ca.unsealed());
    // Every node serves its chained certificate; every node's verifies.
    for (i, a) in nodes.iter().enumerate() {
        for b in nodes.iter() {
            let who = whoami(&client(&tls_dirs[i]), &b.url).await;
            assert_eq!(who["role"], "node", "{} → {}", a.url, b.url);
        }
    }
    // The cluster status has the CA's view, nothing sealed in it.
    let status = n1.cluster.status();
    assert!(status.pki.is_some());
    let st = serde_json::to_string(&status).unwrap();
    assert!(!st.contains("sealed_key") && !st.contains("wrapped_dek"));

    // ---- refusals ----
    // A token for an occupied member, through a sealed node (it passes the
    // join on): refused, and the token stays usable.
    let t = make_token(&admin, &n0.url, &node_token(1, false)).await;
    let e = match crate::join::join(&n1.url, &t, m1.me, &m1.fingerprint).await {
        Err(e) => e,
        Ok(_) => panic!("joined over a live member"),
    };
    assert!(e.contains("already has a live certificate"), "{}", e);
    assert!(n0.cluster.pki().unwrap().enrollments.contains_key(&t.id));

    // Nodes and tokens don't make tokens: only an admin's own certificate.
    let r = client(&tls_dirs[1])
        .post(format!("{}/api/v1/pki/enrollments", n0.url))
        .json(&node_token(1, true))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // An enrollment put in through Raft, without the signer's MAC.
    let forged = JoinToken::generate(t.root).unwrap();
    let e = Enrollment {
        scope: EnrollScope::Node {
            fingerprint: m1.fingerprint.clone(),
            member: Some(m1.me),
            extra_sans: vec!["evil.example".into()],
            replace: true,
            compromised: false,
        },
        secret_sha256: forged.secret_sha256(),
        uses: 1,
        uses_left: 1,
        expires_ms: crate::now_ms() + 3_600_000,
        created_by: "nobody".into(),
        mac: [0; 32],
    };
    let r = n2
        .cluster
        .lease(
            // A compromised node reads the epoch and wraps it too: only
            // the MAC stops it.
            &LeaseOp::Pki(
                PkiOp::CreateEnrollment {
                    id: forged.id.clone(),
                    enrollment: Box::new(e),
                }
                .at_epoch(n2.cluster.pki().unwrap().seal.epoch),
            ),
            false,
        )
        .await
        .unwrap();
    assert_eq!(r, Response::Recorded);
    // Committed through node 2: wait for node 0 to have applied it, so the
    // join is refused for its MAC and not as unknown.
    assert!(
        wait_for(10, || n0
            .cluster
            .pki()
            .is_some_and(|p| p.enrollments.contains_key(&forged.id)))
        .await
    );
    let e = match crate::join::join(&n0.url, &forged, m1.me, &m1.fingerprint).await {
        Err(e) => e,
        Ok(_) => panic!("joined with a forged enrollment"),
    };
    assert!(e.contains("MAC"), "{}", e);

    // CA ops on the lease API, from outside: refused, even "forwarded" by
    // an admin.
    let seal = LeaseOp::Pki(PkiOp::Seal {
        remove_slots: vec![],
    });
    let r: Result<Response, String> = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/api/v1/cluster/lease", raft[0]))
        .json(&seal)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(r.unwrap_err().contains("/api/v1/pki"));
    let r: Result<Response, String> = admin
        .post(format!("{}/api/v1/cluster/lease?forwarded=true", n0.url))
        .json(&seal)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(r.is_err());
    assert_eq!(n0.cluster.pki().unwrap().seal.epoch, 0);

    // ---- --replace: the member's old certificate is revoked with the join ----
    let t = make_token(&admin, &n0.url, &node_token(2, true)).await;
    let j = crate::join::join(&n0.url, &t, m2.me, &m2.fingerprint)
        .await
        .unwrap();
    let pki = n0.cluster.pki().unwrap();
    assert_eq!(
        pki.revoked_certs.get(&j2.serial).map(|r| r.reason),
        Some(RevocationReason::Superseded)
    );
    assert!(pki.is_live(&j.serial, crate::now_ms()));
    assert!(!pki.enrollments.contains_key(&t.id));

    // ---- renewal: a node's own identity again, by its certificate ----
    let (_key, csr) = raidish_auth::ca::generate_csr("x").unwrap();
    let r = client(&tls_dirs[1])
        .post(format!("{}/api/v1/pki/renew", n0.url))
        .json(&RenewRequest { csr_pem: csr })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let renewed: Issued = r.json().await.unwrap();
    let rec = n0
        .cluster
        .pki()
        .unwrap()
        .certs
        .get(&renewed.serial)
        .cloned()
        .unwrap();
    assert_eq!(rec.renews, Some(j1.serial.clone()));
    assert_eq!(rec.identity, staged_state.certs[&j1.serial].identity);
    // A sealed node can't sign: 503, so the client asks the next.
    let (_key, csr) = raidish_auth::ca::generate_csr("x").unwrap();
    let r = client(&tls_dirs[1])
        .post(format!("{}/api/v1/pki/renew", n1.url))
        .json(&RenewRequest { csr_pem: csr })
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);

    // ---- an admin issuing, and unsealing another node ----
    let (_key, csr) = raidish_auth::ca::generate_csr("x").unwrap();
    let r = admin
        .post(format!("{}/api/v1/pki/sign", n0.url))
        .json(&crate::api::SignRequest {
            csr_pem: csr,
            profile: ProfileKind::Readonly,
            cn: "viewer".into(),
            sans: vec![],
            days: None,
            member: None,
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let r = admin
        .post(format!("{}/api/v1/pki/unseal", n1.url))
        .json(&crate::api::UnsealRequest {
            secret: out.recovery_key.to_string(),
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    assert!(n1.ca.unsealed());
    let r = admin
        .post(format!("{}/api/v1/pki/unseal", n2.url))
        .json(&crate::api::UnsealRequest {
            secret: "RDK1-00000000-00000000-00000000-00000000-00000000-00000000-00000000-00000000"
                .into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // Withdrawing a token.
    // Node 1 has no address for itself: it says to ask another.
    let r = admin
        .post(format!("{}/api/v1/pki/enrollments", n1.url))
        .json(&node_token(1, true))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let t = make_token(&admin, &n1.url, &node_token(2, true)).await;
    let r = admin
        .delete(format!("{}/api/v1/pki/enrollments/{}", n2.url, t.id))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    assert!(
        wait_for(5, || !n0
            .cluster
            .pki()
            .unwrap()
            .enrollments
            .contains_key(&t.id))
        .await
    );
    let _: TokenId = t.id;

    for n in nodes {
        n.stop().await;
    }
}

/// `ca init` on node 0, the two peers joined, all three started and the
/// CA in Raft.
struct Up {
    _dir: tempfile::TempDir,
    nodes: Vec<Node>,
    tls_dirs: Vec<PathBuf>,
    admin_dir: PathBuf,
    base: PathBuf,
    recovery_key: String,
    paths: Vec<String>,
    raft: [u16; 3],
    tls: [u16; 3],
    renewing: Option<Renewing>,
}

impl Up {
    /// Stop node `i` and start it again (its Raft log kept), with `key`.
    async fn restart(&mut self, i: usize, key: Option<crate::NodeKeySource>) {
        self.nodes[i].stop().await;
        // Its servers let go of their ports.
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.nodes[i] = start_full(
            &self.paths,
            self.raft[i],
            self.tls[i],
            &self.base.join("raft"),
            &self.tls_dirs[i],
            key,
            self.renewing,
        )
        .await;
    }
}

async fn bring_up() -> Up {
    bring_up_with(None).await
}

/// As `bring_up`, with node certificates of `node_days`.
async fn bring_up_with(node_days: Option<u32>) -> Up {
    bring_up_full(node_days, None).await
}

/// As `bring_up`, each node renewing its own certificate (`life` seconds
/// long once renewed), through its peers' mTLS ports.
async fn bring_up_renewing(life: u64) -> Up {
    bring_up_full(None, Some(life)).await
}

async fn bring_up_full(node_days: Option<u32>, life: Option<u64>) -> Up {
    raidish_auth::tls::install_provider();
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_path_buf();
    let root = base.join("raft");
    let raft = [free_port(), free_port(), free_port()];
    let tls = [free_port(), free_port(), free_port()];
    let renewing = life.map(|l| (l, tls));
    let paths: Vec<String> = raft
        .iter()
        .enumerate()
        .map(|(i, p)| format!("http://127.0.0.1:{}/api/v1/disks/{}", p, i))
        .collect();
    let tls_dirs: Vec<PathBuf> = (0..3).map(|i| base.join(format!("tls{}", i))).collect();
    let admin_dir = base.join("admin");
    let m0 = members(&paths, raft[0]);
    let out = init(&InitOptions {
        members: &m0,
        data_dir: &root.join("node-0"),
        sans: vec!["127.0.0.1".into()],
        peer_sans: Default::default(),
        node_days,
        admin_name: "admin".into(),
        token_ttl_ms: 3_600_000,
        unseal_once: true,
        force: false,
        kdf: (8, 1, 1),
    })
    .unwrap();
    out.node0.write_dir(&tls_dirs[0]).unwrap();
    out.admin.write_dir(&admin_dir).unwrap();
    let n0 = start_full(&paths, raft[0], tls[0], &root, &tls_dirs[0], None, renewing).await;
    for (i, t) in out.tokens.iter().enumerate() {
        let m = members(&paths, raft[i + 1]);
        let t = JoinToken::parse(&t.token).unwrap();
        let j = crate::join::join(&n0.url, &t, m.me, &m.fingerprint)
            .await
            .unwrap();
        j.bundle.write_dir(&tls_dirs[i + 1]).unwrap();
    }
    let n1 = start_full(&paths, raft[1], tls[1], &root, &tls_dirs[1], None, renewing).await;
    let n2 = start_full(&paths, raft[2], tls[2], &root, &tls_dirs[2], None, renewing).await;
    let nodes = vec![n0, n1, n2];
    assert!(
        wait_for(30, || nodes.iter().all(|n| n.cluster.pki().is_some())).await,
        "the CA reached every node"
    );
    Up {
        _dir: dir,
        nodes,
        tls_dirs,
        admin_dir,
        base,
        recovery_key: out.recovery_key.to_string(),
        paths,
        raft,
        tls,
        renewing,
    }
}

/// A new certificate signed by the cluster, written to `dir`.
async fn issue(
    admin: &reqwest::Client,
    url: &str,
    profile: ProfileKind,
    cn: &str,
    dir: &Path,
) -> Issued {
    let (key, csr) = raidish_auth::ca::generate_csr(cn).unwrap();
    let r = admin
        .post(format!("{}/api/v1/pki/sign", url))
        .json(&crate::api::SignRequest {
            csr_pem: csr,
            profile,
            cn: cn.into(),
            sans: vec![],
            days: None,
            member: None,
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let issued: Issued = r.json().await.unwrap();
    crate::Bundle {
        ca_pem: issued.ca_pem.clone(),
        cert_pem: issued.cert_pem.clone(),
        key_pem: key,
    }
    .write_dir(dir)
    .unwrap();
    issued
}

/// Whether `c` is let in at `url` (a refused handshake, or a 401: not).
async fn let_in(c: &reqwest::Client, url: &str) -> bool {
    match c.get(format!("{}/api/v1/auth/whoami", url)).send().await {
        Ok(r) => r.status().is_success(),
        Err(_) => false,
    }
}

async fn revoke(
    c: &reqwest::Client,
    url: &str,
    serial: &raidish_types::pki::Serial,
    force: bool,
) -> reqwest::StatusCode {
    c.post(format!("{}/api/v1/pki/certs/{}/revoke", url, serial))
        .json(&crate::api::RevokeCertRequest {
            reason: Some(RevocationReason::KeyCompromise),
            force,
        })
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revocations_apply_everywhere_at_once() {
    let up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    let n0 = &up.nodes[0];

    // ---- an admin certificate ----
    let ops_dir = up.base.join("ops");
    let ops = issue(&admin, &n0.url, ProfileKind::Admin, "ops", &ops_dir).await;
    let ops_client = client(&ops_dir);
    for u in &urls {
        assert!(let_in(&ops_client, u).await, "{}", u);
    }
    // Its connections stay open (keep-alive): the revocation closes them.
    assert!(
        up.nodes
            .iter()
            .all(|n| n.trust.connections().count(&ops.serial) > 0)
    );
    // An admin can't revoke the cert it asks with.
    assert_eq!(revoke(&ops_client, &n0.url, &ops.serial, false).await, 409);
    // Through a sealed node: revoking signs nothing.
    assert!(!up.nodes[1].ca.unsealed());
    assert_eq!(revoke(&admin, &urls[1], &ops.serial, false).await, 200);
    for n in &up.nodes {
        assert!(
            wait_for(5, || n.trust.connections().count(&ops.serial) == 0).await,
            "{}: the revoked cert's connections are closed",
            n.url
        );
    }
    for u in &urls {
        assert!(
            !let_in(&ops_client, u).await,
            "{} still lets a revoked cert in",
            u
        );
    }
    // Listed as revoked.
    let certs: Vec<raidish_types::pki::CertListing> = admin
        .get(format!("{}/api/v1/pki/certs", urls[2]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let listed = certs.iter().find(|c| c.serial == ops.serial).unwrap();
    assert_eq!(
        listed.revoked.as_ref().map(|r| r.reason),
        Some(RevocationReason::KeyCompromise)
    );
    assert_eq!(
        revoke(&admin, &n0.url, &ops.serial, false).await,
        200,
        "again: no-op"
    );

    // ---- a token ----
    let admin_bundle = std::fs::read_to_string(up.admin_dir.join("cert.pem")).unwrap();
    let admin_key = std::fs::read_to_string(up.admin_dir.join("key.pem")).unwrap();
    let admin_der = raidish_auth::ca::first_der(&admin_bundle).unwrap();
    let key_der = {
        use rustls::pki_types::pem::PemObject;
        rustls::pki_types::PrivateKeyDer::from_pem_slice(admin_key.as_bytes()).unwrap()
    };
    let (token, claims) = raidish_auth::auth::create_token(
        "dash",
        raidish_auth::pki::Role::Readonly,
        Some(3600),
        &admin_der,
        key_der.secret_der(),
    )
    .unwrap();
    // No client certificate: the token alone.
    let anon = reqwest::Client::builder()
        .use_preconfigured_tls(
            TlsConfig {
                ca: Some(up.admin_dir.join("ca.pem").to_string_lossy().into()),
                ..Default::default()
            }
            .client_config()
            .unwrap(),
        )
        .build()
        .unwrap();
    let with_token = |u: &str| {
        anon.get(format!("{}/api/v1/auth/whoami", u))
            .bearer_auth(&token)
            .send()
    };
    for u in &urls {
        assert!(with_token(u).await.unwrap().status().is_success(), "{}", u);
    }
    // A readonly token can't revoke anything.
    let r = anon
        .post(format!(
            "{}/api/v1/pki/tokens/{}/revoke",
            urls[0], claims.id
        ))
        .bearer_auth(&token)
        .json(&crate::api::RevokeTokenRequest::default())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = admin
        .post(format!(
            "{}/api/v1/pki/tokens/{}/revoke",
            urls[2], claims.id
        ))
        .json(&crate::api::RevokeTokenRequest {
            expires_ms: claims.exp.map(|e| e as u64 * 1000),
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    for u in &urls {
        let mut refused = false;
        for _ in 0..50 {
            if with_token(u).await.unwrap().status() == 401 {
                refused = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(refused, "{} still takes a revoked token", u);
    }

    // ---- a node ----
    let pki = n0.cluster.pki().unwrap();
    let (node2_serial, _) = pki
        .member_certs(nid(2), crate::now_ms())
        .find(|(_, c)| c.identity.profile == ProfileKind::Node)
        .map(|(s, c)| (s.clone(), c.clone()))
        .unwrap();
    // Its only node certificate: not without force.
    assert_eq!(revoke(&admin, &n0.url, &node2_serial, false).await, 409);
    let node2 = client(&up.tls_dirs[2]);
    assert!(let_in(&node2, &urls[0]).await);
    assert!(n0.trust.connections().count(&node2_serial) > 0);
    assert_eq!(revoke(&admin, &n0.url, &node2_serial, true).await, 200);
    assert!(
        wait_for(5, || n0.trust.connections().count(&node2_serial) == 0).await,
        "node 2's connections to node 0 are closed"
    );
    assert!(!let_in(&node2, &urls[0]).await);
    assert!(!let_in(&node2, &urls[1]).await);
    // The others are unaffected.
    assert!(let_in(&client(&up.tls_dirs[1]), &urls[0]).await);
    assert!(let_in(&admin, &urls[2]).await);

    for n in &up.nodes {
        n.stop().await;
    }
}

async fn crl_of(url: &str) -> Option<Vec<u8>> {
    let r = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .get(format!("{}/api/v1/pki/crl", url))
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return None;
    }
    Some(r.bytes().await.ok()?.to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_crl_follows_revocations_on_every_node() {
    let up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    let n0 = &up.nodes[0];
    // Node 0 (unsealed by `ca init`) signs the first one as soon as the CA
    // is in Raft; every node serves it, sealed or not.
    assert!(
        wait_for(10, || n0.cluster.pki().unwrap().crl.is_some()).await,
        "a first CRL"
    );
    // Making the token signer is a change too: once it's in, a CRL that
    // covers it.
    assert!(
        wait_for(10, || {
            let p = n0.cluster.pki().unwrap();
            p.token_signer.is_some() && p.crl.as_ref().is_some_and(|c| c.generation == p.generation)
        })
        .await,
        "the token signer, and a CRL after it"
    );
    let first = n0.cluster.pki().unwrap().crl.clone().unwrap();
    assert!(
        raidish_auth::ca::crl_serials(&first.der)
            .unwrap()
            .is_empty()
    );
    // Each node serves it once it has applied it.
    for n in &up.nodes {
        assert!(
            wait_for(10, || n.cluster.pki().unwrap().crl.as_ref() == Some(&first)).await,
            "{} has the CRL",
            n.url
        );
    }
    for u in &urls {
        assert_eq!(crl_of(u).await.as_deref(), Some(&first.der[..]), "{}", u);
    }
    // Unseal node 1 too: two signers, still one CRL per change.
    let r = admin
        .post(format!("{}/api/v1/pki/unseal", urls[1]))
        .json(&crate::api::UnsealRequest {
            secret: up.recovery_key.clone(),
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);

    let ops = issue(
        &admin,
        &n0.url,
        ProfileKind::Admin,
        "ops",
        &up.base.join("ops"),
    )
    .await;
    assert_eq!(revoke(&admin, &urls[2], &ops.serial, false).await, 200);
    let listed = |n: &Node| {
        n.cluster
            .pki()
            .and_then(|p| p.crl.clone())
            .is_some_and(|c| {
                raidish_auth::ca::crl_serials(&c.der)
                    .unwrap()
                    .contains(&ops.serial)
            })
    };
    for n in &up.nodes {
        assert!(
            wait_for(10, || listed(n)).await,
            "{}: the CRL lists it",
            n.url
        );
    }
    // Let both signers settle, then: exactly one CRL for the change.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let now = n0.cluster.pki().unwrap();
    let crl = now.crl.clone().unwrap();
    assert_eq!(crl.generation, now.generation);
    // The issue was a change too (RecordIssued): at most two newer.
    assert!(
        crl.number > first.number && crl.number <= first.number + 2,
        "{} after {}",
        crl.number,
        first.number
    );
    let crl_der = crl_of(&urls[1]).await.unwrap();
    assert!(
        raidish_auth::ca::crl_serials(&crl_der)
            .unwrap()
            .contains(&ops.serial)
    );
    // It verifies against the issuer, for anyone (openssl, if it's here).
    let crl_path = up.base.join("crl.der");
    std::fs::write(&crl_path, &crl_der).unwrap();
    let ca_pem = std::fs::read_to_string(up.admin_dir.join("cert.pem")).unwrap();
    let chain: Vec<_> = ca_pem
        .split_inclusive("-----END CERTIFICATE-----\n")
        .collect();
    let bundle = up.base.join("chain.pem");
    std::fs::write(
        &bundle,
        format!(
            "{}{}",
            chain.get(1).copied().unwrap_or(""),
            std::fs::read_to_string(up.admin_dir.join("ca.pem")).unwrap()
        ),
    )
    .unwrap();
    if let Ok(out) = std::process::Command::new("openssl")
        .args(["crl", "-inform", "DER", "-noout", "-in"])
        .arg(&crl_path)
        .arg("-CAfile")
        .arg(&bundle)
        .output()
    {
        let all = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success() && all.contains("verify OK"), "{}", all);
    }
    // PEM too.
    let pem = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .get(format!("{}/api/v1/pki/crl?format=pem", urls[2]))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(pem.starts_with("-----BEGIN X509 CRL-----"));

    for n in &up.nodes {
        n.stop().await;
    }
}

/// `POST /api/v1/pki/tokens` at `url`.
async fn mint(
    c: &reqwest::Client,
    url: &str,
    role: &str,
    ttl_s: Option<i64>,
) -> Result<crate::api::TokenCreated, (u16, String)> {
    let r = c
        .post(format!("{}/api/v1/pki/tokens", url))
        .json(&crate::api::TokenRequest {
            sub: "dash".into(),
            role: role.into(),
            ttl_s,
        })
        .send()
        .await
        .unwrap();
    let status = r.status().as_u16();
    if r.status().is_success() {
        Ok(r.json().await.unwrap())
    } else {
        Err((status, r.text().await.unwrap_or_default()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rdt2_tokens_work_everywhere_and_are_revoked_everywhere() {
    let up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    let n0 = &up.nodes[0];
    // Node 0 (unsealed by `ca init`) makes the token signer; every node
    // learns it.
    for n in &up.nodes {
        assert!(
            wait_for(10, || n.cluster.pki().unwrap().token_signer.is_some()).await,
            "{}: a token signer",
            n.url
        );
    }
    let signer = n0.cluster.pki().unwrap().token_signer.clone().unwrap();
    // A sealed node can't mint (503: the CLI tries the next); no node role.
    assert_eq!(
        mint(&admin, &urls[1], "admin", None).await.unwrap_err().0,
        503
    );
    assert_eq!(
        mint(&admin, &urls[0], "node", None).await.unwrap_err().0,
        400
    );
    // Clamped to the signer.
    let t = mint(&admin, &urls[0], "readonly", Some(10 * 365 * 86_400))
        .await
        .unwrap();
    assert_eq!(t.exp, (signer.not_after_ms / 1000) as i64);
    let t = mint(&admin, &urls[0], "admin", Some(3600)).await.unwrap();
    assert!(t.token.starts_with("rdt2."));

    let anon = reqwest::Client::builder()
        .use_preconfigured_tls(
            TlsConfig {
                ca: Some(up.admin_dir.join("ca.pem").to_string_lossy().into()),
                ..Default::default()
            }
            .client_config()
            .unwrap(),
        )
        .build()
        .unwrap();
    let whoami = |u: &str, tok: &str| {
        anon.get(format!("{}/api/v1/auth/whoami", u))
            .bearer_auth(tok)
            .send()
    };
    for u in &urls {
        let r = whoami(u, &t.token).await.unwrap();
        assert!(r.status().is_success(), "{}: {:?}", u, r.text().await);
    }
    // An admin token grants nothing (D8): no signing, enrolling, minting.
    let (_, csr) = raidish_auth::ca::generate_csr("x").unwrap();
    let posts: [(&str, serde_json::Value); 3] = [
        (
            "sign",
            serde_json::to_value(crate::api::SignRequest {
                csr_pem: csr,
                profile: ProfileKind::Admin,
                cn: "x".into(),
                sans: vec![],
                days: None,
                member: None,
            })
            .unwrap(),
        ),
        (
            "enrollments",
            serde_json::to_value(node_token(1, true)).unwrap(),
        ),
        (
            "tokens",
            serde_json::to_value(crate::api::TokenRequest {
                sub: "x".into(),
                role: "admin".into(),
                ttl_s: None,
            })
            .unwrap(),
        ),
    ];
    for (path, body) in &posts {
        let r = anon
            .post(format!("{}/api/v1/pki/{}", urls[0], path))
            .bearer_auth(&t.token)
            .json(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403, "/{}", path);
    }
    // But it can revoke (it only takes authority away): another token.
    let other = mint(&admin, &urls[0], "readonly", Some(3600))
        .await
        .unwrap();
    let r = anon
        .post(format!("{}/api/v1/pki/tokens/{}/revoke", urls[1], other.id))
        .bearer_auth(&t.token)
        .json(&crate::api::RevokeTokenRequest {
            expires_ms: Some(other.exp as u64 * 1000),
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    let refused = |tok: String| {
        let urls = urls.clone();
        let whoami = &whoami;
        async move {
            for u in &urls {
                let mut ok = false;
                for _ in 0..50 {
                    if whoami(u, &tok).await.unwrap().status() == 401 {
                        ok = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                assert!(ok, "{} still takes a revoked token", u);
            }
        }
    };
    refused(other.token).await;
    assert!(
        whoami(&urls[2], &t.token)
            .await
            .unwrap()
            .status()
            .is_success()
    );

    // Revoking the signer ends every token it signed; node 0 makes a new
    // one, whose tokens work.
    assert_eq!(revoke(&admin, &urls[2], &signer.serial, false).await, 200);
    refused(t.token).await;
    assert!(
        wait_for(10, || n0
            .cluster
            .pki()
            .unwrap()
            .token_signer
            .as_ref()
            .is_some_and(|s| s.serial != signer.serial))
        .await,
        "a new token signer"
    );
    let t = mint(&admin, &urls[0], "readonly", Some(60)).await.unwrap();
    for u in &urls {
        let mut ok = false;
        for _ in 0..50 {
            if whoami(u, &t.token).await.unwrap().status().is_success() {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ok, "{}: the new signer's token", u);
    }
    for n in up.nodes {
        n.stop().await;
    }
}

async fn unseal_with(c: &reqwest::Client, url: &str, secret: &str) -> u16 {
    c.post(format!("{}/api/v1/pki/unseal", url))
        .json(&crate::api::UnsealRequest {
            secret: secret.into(),
        })
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn add_slot(
    c: &reqwest::Client,
    url: &str,
    req: &crate::api::SlotRequest,
) -> Result<crate::api::SlotCreated, u16> {
    let r = c
        .post(format!("{}/api/v1/pki/slots", url))
        .json(req)
        .send()
        .await
        .unwrap();
    match r.status().is_success() {
        true => Ok(r.json().await.unwrap()),
        false => Err(r.status().as_u16()),
    }
}

fn passphrase(p: &str) -> crate::api::SlotRequest {
    crate::api::SlotRequest {
        kind: crate::api::NewSlotKind::Passphrase,
        label: "test".into(),
        passphrase: Some(p.into()),
        node: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slots_and_sealing_on_every_node() {
    let up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    let (n0, n1) = (&up.nodes[0], &up.nodes[1]);
    assert!(n0.ca.unsealed() && !n1.ca.unsealed());

    // A slot wraps the DEK: a sealed node can't add one (503).
    let phrase = "a long enough passphrase";
    assert_eq!(
        add_slot(&admin, &urls[1], &passphrase(phrase))
            .await
            .unwrap_err(),
        503
    );
    assert_eq!(
        add_slot(&admin, &urls[0], &passphrase("short"))
            .await
            .unwrap_err(),
        400
    );
    let pass = add_slot(&admin, &urls[0], &passphrase(phrase))
        .await
        .unwrap();
    assert!(pass.id.starts_with("passphrase-") && pass.recovery_key.is_none());
    // Node 1 unseals with it; a wrong one is refused.
    assert_eq!(
        unseal_with(&admin, &urls[1], "not the passphrase at all").await,
        403
    );
    assert_eq!(unseal_with(&admin, &urls[1], phrase).await, 200);
    assert!(n1.ca.unsealed());
    // Removing the slot doesn't seal the node it unsealed.
    let r = admin
        .delete(format!("{}/api/v1/pki/slots/{}", urls[2], pass.id))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    assert!(n1.ca.unsealed());
    assert_eq!(unseal_with(&admin, &urls[2], phrase).await, 403, "gone");

    // Two at once, through two nodes: both land.
    let (pa, pb) = (
        passphrase("first long passphrase"),
        passphrase("second long passphrase"),
    );
    let (a, b) = tokio::join!(
        add_slot(&admin, &urls[0], &pa),
        add_slot(&admin, &urls[1], &pb),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    // Node 0 applies node 1's in its own time.
    let ids = || -> Vec<String> {
        n0.cluster
            .pki()
            .unwrap()
            .seal
            .slots
            .iter()
            .map(|s| s.id.clone())
            .collect()
    };
    assert!(
        wait_for(10, || ids().contains(&a.id) && ids().contains(&b.id)).await,
        "{:?}",
        ids()
    );

    // The last recovery-key slot stays unless forced; a new recovery key
    // replaces it.
    let last = admin
        .delete(format!("{}/api/v1/pki/slots/recovery-1", urls[0]))
        .send()
        .await
        .unwrap();
    assert_eq!(last.status(), 409);
    let rk = add_slot(
        &admin,
        &urls[0],
        &crate::api::SlotRequest {
            kind: crate::api::NewSlotKind::RecoveryKey,
            label: "new".into(),
            passphrase: None,
            node: None,
        },
    )
    .await
    .unwrap();
    let new_key = rk.recovery_key.clone().unwrap();
    let r = admin
        .delete(format!("{}/api/v1/pki/slots/recovery-1", urls[0]))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());

    // Seal, through node 2: every node drops its DEK.
    let token = mint(&admin, &urls[0], "readonly", Some(3600))
        .await
        .unwrap();
    let r = admin
        .post(format!("{}/api/v1/pki/seal", urls[2]))
        .json(&crate::api::SealRequest::default())
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    for n in &up.nodes {
        assert!(wait_for(10, || !n.ca.unsealed()).await, "{} sealed", n.url);
    }
    assert_eq!(n0.cluster.pki().unwrap().seal.epoch, 1);
    for u in &urls {
        let (_, csr) = raidish_auth::ca::generate_csr("x").unwrap();
        let r = admin
            .post(format!("{}/api/v1/pki/sign", u))
            .json(&crate::api::SignRequest {
                csr_pem: csr,
                profile: ProfileKind::Readonly,
                cn: "x".into(),
                sans: vec![],
                days: None,
                member: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 503, "{}: signing", u);
        assert_eq!(
            mint(&admin, u, "readonly", None).await.unwrap_err().0,
            503,
            "{}: minting",
            u
        );
        // mTLS, and a token already minted, still work.
        assert!(let_in(&admin, u).await);
        let r = admin
            .get(format!("{}/api/v1/auth/whoami", u))
            .bearer_auth(&token.token)
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
    }
    // The old recovery key's slot is gone; the new one unseals, and the CA
    // signs again.
    assert_eq!(unseal_with(&admin, &urls[1], &up.recovery_key).await, 403);
    assert_eq!(unseal_with(&admin, &urls[1], &new_key).await, 200);
    issue(
        &admin,
        &urls[1],
        ProfileKind::Readonly,
        "after",
        &up.base.join("after"),
    )
    .await;
    for n in up.nodes {
        n.stop().await;
    }
}

fn node_key_for(n: u64) -> crate::api::SlotRequest {
    crate::api::SlotRequest {
        kind: crate::api::NewSlotKind::NodeKey,
        label: String::new(),
        passphrase: None,
        node: Some(nid(n)),
    }
}

async fn status_of(c: &reqwest::Client, url: &str) -> raidish_types::pki::PkiStatus {
    c.get(format!("{}/api/v1/pki/status", url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_keys_unseal_their_own_node_only() {
    let mut up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    assert!(up.nodes[0].ca.unsealed() && !up.nodes[1].ca.unsealed());
    assert_eq!(
        status_of(&admin, &urls[0]).await.unsealed_by.as_deref(),
        Some("unseal-once")
    );

    // Not for a stranger; a sealed node can't make one.
    let mut stranger = node_key_for(7);
    assert_eq!(
        add_slot(&admin, &urls[0], &stranger).await.unwrap_err(),
        400
    );
    stranger.node = None;
    assert_eq!(
        add_slot(&admin, &urls[0], &stranger).await.unwrap_err(),
        400
    );
    assert_eq!(
        add_slot(&admin, &urls[1], &node_key_for(1))
            .await
            .unwrap_err(),
        503
    );

    // Node 1's key, as a file; node 2 is configured with the same file but
    // has no slot of its own: it never tries node 1's.
    let key1 = add_slot(&admin, &urls[0], &node_key_for(1)).await.unwrap();
    assert!(key1.id.starts_with("node-key-node-1-") && key1.recovery_key.is_none());
    let file = up.base.join("node1.key");
    std::fs::write(&file, format!("{}\n", key1.node_key.unwrap())).unwrap();
    let src = || Some(crate::NodeKeySource::File(file.clone()));
    up.restart(1, src()).await;
    up.restart(2, src()).await;
    assert!(
        wait_for(20, || up.nodes[1].ca.unsealed()).await,
        "node 1 unseals itself at start"
    );
    assert_eq!(
        status_of(&admin, &urls[1]).await.unsealed_by.as_deref(),
        Some("node-key (file)")
    );
    assert!(up.nodes[2].cluster.pki().is_some());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !up.nodes[2].ca.unsealed(),
        "node 2 never tries node 1's slot"
    );
    assert!(up.nodes[2].ca.node_key_seen.lock().unwrap().is_empty());

    // A slot for node 2 appears, made with another key: it tries it once,
    // fails, and doesn't try again on later changes.
    let key2 = add_slot(&admin, &urls[1], &node_key_for(2)).await.unwrap();
    assert!(
        wait_for(10, || up.nodes[2]
            .ca
            .node_key_seen
            .lock()
            .unwrap()
            .contains(&key2.id))
        .await
    );
    assert!(!up.nodes[2].ca.unsealed());
    // Now with the right key in its file: still nothing until a new slot
    // or a restart.
    let file2 = up.base.join("node2.key");
    std::fs::write(&file2, key2.node_key.unwrap()).unwrap();
    up.restart(2, Some(crate::NodeKeySource::File(file2.clone())))
        .await;
    assert!(
        wait_for(20, || up.nodes[2].ca.unsealed()).await,
        "after a restart"
    );

    // Signing through node 1, which unsealed itself.
    issue(
        &admin,
        &urls[1],
        ProfileKind::Readonly,
        "by-node-key",
        &up.base.join("by-node-key"),
    )
    .await;

    // Sealed keeping node keys: node 1 stays sealed until it restarts,
    // then unseals itself.
    let r = admin
        .post(format!("{}/api/v1/pki/seal", urls[0]))
        .json(&crate::api::SealRequest {
            keep_node_keys: true,
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    for n in &up.nodes {
        assert!(wait_for(10, || !n.ca.unsealed()).await, "{} sealed", n.url);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!up.nodes[1].ca.unsealed(), "not until a restart");
    up.restart(1, src()).await;
    assert!(wait_for(20, || up.nodes[1].ca.unsealed()).await);

    // Revoking node 2's node cert takes its slot in the same entry.
    let pki = up.nodes[0].cluster.pki().unwrap();
    let serial2 = pki
        .certs
        .iter()
        .find(|(s, c)| c.identity.member == Some(nid(2)) && pki.is_live(s, crate::now_ms()))
        .map(|(s, _)| s.clone())
        .unwrap();
    assert!(revoke(&admin, &urls[0], &serial2, true).await.is_success());
    assert!(
        wait_for(10, || !up.nodes[0]
            .cluster
            .pki()
            .unwrap()
            .seal
            .slots
            .iter()
            .any(|s| s.id == key2.id))
        .await
    );

    // A plain seal takes the node keys: node 1 restarts sealed.
    let r = admin
        .post(format!("{}/api/v1/pki/seal", urls[0]))
        .json(&crate::api::SealRequest::default())
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    assert!(wait_for(10, || !up.nodes[1].ca.unsealed()).await);
    assert!(
        !up.nodes[0]
            .cluster
            .pki()
            .unwrap()
            .seal
            .slots
            .iter()
            .any(|s| matches!(s.kind, raidish_types::pki::SlotKind::NodeKey { .. }))
    );
    up.restart(1, src()).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!up.nodes[1].ca.unsealed());
    for n in up.nodes {
        n.stop().await;
    }
}

async fn status_all(c: &reqwest::Client, url: &str) -> raidish_types::pki::PkiStatus {
    c.get(format!("{}/api/v1/pki/status?all=true", url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn seal_all(c: &reqwest::Client, url: &str) {
    let r = c
        .post(format!("{}/api/v1/pki/seal", url))
        .json(&crate::api::SealRequest::default())
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
}

/// A change to the CA every node applies (wakes their loops).
async fn poke(c: &reqwest::Client, url: &str, n: u8) {
    let r = c
        .post(format!("{}/api/v1/pki/tokens/{:016x}/revoke", url, n))
        .json(&crate::api::RevokeTokenRequest::default())
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_cluster_says_so_and_warns_once() {
    // Node certificates of 5 days: within the 10 days warned about.
    let up = bring_up_with(Some(5)).await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    assert_eq!(up.nodes[0].events.of(EventKind::CaUnsealed).len(), 1);

    // Unsealed: every member asked, no warning.
    let s = status_all(&admin, &urls[1]).await;
    let seals: Vec<(String, bool)> = s
        .nodes
        .iter()
        .map(|n| (n.node.clone(), n.unsealed))
        .collect();
    assert_eq!(
        seals,
        vec![
            ("node-0".to_string(), true),
            ("node-1".to_string(), false),
            ("node-2".to_string(), false)
        ]
    );
    assert!(s.nodes.iter().all(|n| n.reachable));
    assert_eq!(s.nodes[0].unsealed_by.as_deref(), Some("unseal-once"));
    assert!(!s.warnings.iter().any(|w| w.contains("no node is unsealed")));
    for n in &up.nodes {
        assert!(n.events.of(EventKind::CaUnsealedNone).is_empty());
    }

    // Sealed: every node says so, and warns once about its own cert.
    seal_all(&admin, &urls[2]).await;
    for n in &up.nodes {
        assert!(wait_for(10, || !n.ca.unsealed()).await);
    }
    assert_eq!(up.nodes[2].events.of(EventKind::CaSealed).len(), 1);
    let s = status_all(&admin, &urls[0]).await;
    assert!(s.nodes.iter().all(|n| n.reachable && !n.unsealed));
    let w: Vec<&String> = s
        .warnings
        .iter()
        .filter(|w| w.contains("no node is unsealed"))
        .collect();
    assert_eq!(w.len(), 1, "{:?}", s.warnings);
    assert!(w[0].contains("node-0's certificate") && w[0].contains("node-2's certificate"));
    for n in &up.nodes {
        let me = Subject::node(&n.cluster.name);
        assert!(
            wait_for(10, || n.events.of(EventKind::CaUnsealedNone).len() == 1).await,
            "{}: {:?}",
            n.cluster.name,
            n.events.of(EventKind::CaUnsealedNone)
        );
        assert_eq!(
            n.events.of(EventKind::CaUnsealedNone)[0].0,
            me,
            "its own cert only"
        );
    }
    // Changes since: not said again.
    poke(&admin, &urls[0], 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    for n in &up.nodes {
        assert_eq!(n.events.of(EventKind::CaUnsealedNone).len(), 1);
    }

    // A node unsealed: the warning is over (each sealed node clears it at
    // its next check), and comes back with the next seal.
    assert_eq!(unseal_with(&admin, &urls[1], &up.recovery_key).await, 200);
    assert_eq!(
        up.nodes[1]
            .events
            .of(EventKind::CaUnsealed)
            .last()
            .unwrap()
            .1,
        "node-1 unsealed by recovery-key"
    );
    poke(&admin, &urls[0], 2).await;
    for n in &up.nodes {
        assert!(
            wait_for(10, || !n
                .ca
                .warned_unsealed_none
                .load(std::sync::atomic::Ordering::Relaxed))
            .await,
            "{} cleared",
            n.cluster.name
        );
    }
    assert!(
        !status_all(&admin, &urls[2])
            .await
            .warnings
            .iter()
            .any(|w| w.contains("no node is unsealed"))
    );
    seal_all(&admin, &urls[0]).await;
    for n in &up.nodes {
        assert!(
            wait_for(10, || n.events.of(EventKind::CaUnsealedNone).len() == 2).await,
            "{} warned again",
            n.cluster.name
        );
    }

    // A member down: "no reachable node".
    up.nodes[2].stop().await;
    let s = status_all(&admin, &urls[0]).await;
    assert!(!s.nodes[2].reachable && s.nodes[2].error.is_some());
    assert!(
        s.warnings
            .iter()
            .any(|w| w.contains("no reachable node is unsealed")),
        "{:?}",
        s.warnings
    );
    for n in up.nodes {
        n.stop().await;
    }
}

async fn rekey_through(c: &reqwest::Client, url: &str) -> Result<crate::api::Rekeyed, u16> {
    rekey_with(c, url, None).await
}

async fn rekey_with(
    c: &reqwest::Client,
    url: &str,
    recovery_key: Option<String>,
) -> Result<crate::api::Rekeyed, u16> {
    let r = c
        .post(format!("{}/api/v1/pki/rekey", url))
        .json(&crate::api::RekeyRequest { recovery_key })
        .send()
        .await
        .unwrap();
    match r.status().is_success() {
        true => Ok(r.json().await.unwrap()),
        false => Err(r.status().as_u16()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rekey_replaces_the_dek_and_every_slot() {
    let up = bring_up().await;
    let urls: Vec<String> = up.nodes.iter().map(|n| n.url.clone()).collect();
    let admin = client(&up.admin_dir);
    let (n0, n1, n2) = (&up.nodes[0], &up.nodes[1], &up.nodes[2]);
    // Node 1 unsealed too, a passphrase slot, a join token outstanding,
    // and the token signer (sealed under the DEK as well).
    assert_eq!(unseal_with(&admin, &urls[1], &up.recovery_key).await, 200);
    let phrase = "a long enough passphrase";
    add_slot(&admin, &urls[0], &passphrase(phrase))
        .await
        .unwrap();
    let token = make_token(&admin, &urls[0], &node_token(2, true)).await;
    assert!(wait_for(10, || n0.cluster.pki().unwrap().token_signer.is_some()).await);
    let before = n0.cluster.pki().unwrap();

    // Not on a sealed node; not by a token (it grants authority).
    assert_eq!(rekey_through(&admin, &urls[2]).await.unwrap_err(), 503);
    let bearer = mint(&admin, &urls[0], "admin", Some(600)).await.unwrap();
    let r = reqwest::Client::builder()
        .use_preconfigured_tls(
            TlsConfig {
                ca: Some(up.admin_dir.join("ca.pem").to_string_lossy().into()),
                ..Default::default()
            }
            .client_config()
            .unwrap(),
        )
        .build()
        .unwrap()
        .post(format!("{}/api/v1/pki/rekey", urls[0]))
        .bearer_auth(&bearer.token)
        .json(&crate::api::RekeyRequest::default())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // An enrollment put in through Raft by a node without the DEK (as in
    // `init_join_hand_over_and_refusals`): the rekey drops it, it doesn't
    // MAC it anew.
    let m1 = members(&up.paths, up.raft[1]);
    let forged = JoinToken::generate(token.root).unwrap();
    let e = Enrollment {
        scope: EnrollScope::Node {
            fingerprint: m1.fingerprint.clone(),
            member: Some(m1.me),
            extra_sans: vec![],
            replace: true,
            compromised: false,
        },
        secret_sha256: forged.secret_sha256(),
        uses: 1,
        uses_left: 1,
        expires_ms: crate::now_ms() + 3_600_000,
        created_by: "nobody".into(),
        mac: [0; 32],
    };
    let epoch = n2.cluster.pki().unwrap().seal.epoch;
    let forge = LeaseOp::Pki(
        PkiOp::CreateEnrollment {
            id: forged.id.clone(),
            enrollment: Box::new(e),
        }
        .at_epoch(epoch),
    );
    assert_eq!(
        n2.cluster.lease(&forge, false).await.unwrap(),
        Response::Recorded
    );
    assert!(
        wait_for(10, || n0
            .cluster
            .pki()
            .unwrap()
            .enrollments
            .contains_key(&forged.id))
        .await
    );

    let r = rekey_through(&admin, &urls[0]).await.unwrap();
    assert_eq!(r.removed_slots.len(), 2, "{:?}", r.removed_slots);
    assert!(r.held);
    assert_eq!(r.dropped_enrollments, vec![forged.id.to_string()]);
    assert!(
        !n0.cluster
            .pki()
            .unwrap()
            .enrollments
            .contains_key(&forged.id)
    );
    assert!(
        n0.cluster
            .pki()
            .unwrap()
            .enrollments
            .contains_key(&token.id)
    );
    let r = crate::api::Rekeyed {
        recovery_key: Some(r.recovery_key.clone().expect("the node made it")),
        ..r
    };
    let after = n0.cluster.pki().unwrap();
    let ids: Vec<&str> = after.seal.slots.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, vec![r.slot_id.as_str()]);
    assert_eq!(after.seal.epoch, before.seal.epoch + 1);
    assert_ne!(
        after.issuers[&raidish_types::pki::IssuerId(1)].sealed_key,
        before.issuers[&raidish_types::pki::IssuerId(1)].sealed_key
    );
    // The rekey, and a warning naming the enrollment it dropped.
    let said = n0.events.of(EventKind::CaRekeyed);
    assert_eq!(said.len(), 2, "{:?}", said);
    assert!(said.iter().any(|(_, m)| m.contains(&forged.id.to_string())));

    // Node 0 keeps the new DEK; node 1 is sealed by it.
    assert!(n0.ca.unsealed());
    assert_eq!(n0.ca.unsealed_by().as_deref(), Some("rekey"));
    assert!(wait_for(10, || !n1.ca.unsealed()).await, "node 1 sealed");
    // The old secrets open nothing; the new key unseals, and that node signs.
    assert_eq!(unseal_with(&admin, &urls[2], &up.recovery_key).await, 403);
    assert_eq!(unseal_with(&admin, &urls[2], phrase).await, 403);
    let new_key = r.recovery_key.clone().unwrap();
    assert_eq!(unseal_with(&admin, &urls[2], &new_key).await, 200);
    assert!(n2.ca.unsealed());
    issue(
        &admin,
        &urls[2],
        ProfileKind::Readonly,
        "after-rekey",
        &up.base.join("after-rekey"),
    )
    .await;
    // Tokens minted before still work; new ones are minted.
    let r2 = admin
        .get(format!("{}/api/v1/auth/whoami", urls[1]))
        .bearer_auth(&bearer.token)
        .send()
        .await
        .unwrap();
    assert!(r2.status().is_success());
    mint(&admin, &urls[2], "readonly", Some(60)).await.unwrap();

    // The join token made before still redeems (its MAC is the new DEK's).
    let m2 = members(&up.paths, up.raft[2]);
    let j = crate::join::join(&urls[0], &token, m2.me, &m2.fingerprint)
        .await
        .unwrap();
    assert!(
        n0.cluster
            .pki()
            .unwrap()
            .is_live(&j.serial, crate::now_ms())
    );
    // The forged one, dropped, stays unredeemable.
    match crate::join::join(&urls[0], &forged, m1.me, &m1.fingerprint).await {
        Err(e) => assert!(e.contains("unknown"), "{}", e),
        Ok(_) => panic!("joined with a forged enrollment"),
    }

    // A recovery key the caller made: not sent back; it unseals.
    let mine = raidish_auth::envelope::new_recovery_key().unwrap();
    assert_eq!(
        rekey_with(&admin, &urls[2], Some("RDK1-not-a-key".into()))
            .await
            .unwrap_err(),
        400
    );
    let r = rekey_with(&admin, &urls[2], Some(mine.to_string()))
        .await
        .unwrap();
    assert!(r.recovery_key.is_none() && r.held);
    assert!(wait_for(10, || !n0.ca.unsealed()).await);
    assert_eq!(unseal_with(&admin, &urls[0], &new_key).await, 403);
    assert_eq!(unseal_with(&admin, &urls[0], &mine).await, 200);

    // A DEK opened at an epoch the CA has since moved past (a seal while
    // Argon2id ran) isn't held, and wouldn't count as unsealed.
    let epoch = n0.cluster.pki().unwrap().seal.epoch;
    seal_all(&admin, &urls[1]).await;
    assert!(wait_for(10, || n0.cluster.pki().unwrap().seal.epoch == epoch + 1).await);
    let stale = raidish_auth::envelope::new_key().unwrap();
    assert_eq!(
        n0.ca.keep_dek(stale, epoch, "test").unwrap_err().status,
        409
    );
    assert!(!n0.ca.unsealed());
    for n in up.nodes {
        n.stop().await;
    }
}

/// A TLS connection to `port`, presenting the bundle in `dir`, kept open.
async fn connect(dir: &Path, port: u16) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let mut cfg = tls_in(dir).client_config().unwrap();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect("127.0.0.1".try_into().unwrap(), tcp)
        .await
        .unwrap()
}

/// The certificate the server presented on `s`.
fn served(s: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> Vec<u8> {
    s.get_ref().1.peer_certificates().unwrap()[0].to_vec()
}

/// One keep-alive request on `s`: its status line.
async fn get_on(s: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    s.write_all(b"GET /api/v1/pki/status HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    s.flush().await.unwrap();
    // Headers, then the body by its length.
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        assert_eq!(
            s.read(&mut byte).await.unwrap(),
            1,
            "the connection is open"
        );
        buf.push(byte[0]);
    }
    let head = String::from_utf8(buf).unwrap();
    let len: usize = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().unwrap())
        })
        .expect("a content-length");
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await.unwrap();
    head.lines().next().unwrap().to_string()
}

fn serial_of(der: &[u8]) -> raidish_types::pki::Serial {
    raidish_auth::ca::facts(der).unwrap().serial
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_renew_their_own_certificates_live() {
    const LIFE: u64 = 30;
    let up = bring_up_renewing(LIFE).await;
    let admin = client(&up.admin_dir);
    let leaf = |i: usize| {
        up.nodes[i]
            .ca
            .identity
            .as_ref()
            .unwrap()
            .live
            .leaf()
            .to_vec()
    };
    let first: Vec<_> = (0..3).map(|i| serial_of(&leaf(i))).collect();
    // Opened before any renewal, and kept.
    let mut kept = connect(&up.admin_dir, up.tls[1]).await;
    assert_eq!(serial_of(&served(&kept)), first[1]);
    assert!(get_on(&mut kept).await.contains("200"));
    assert!(up.nodes[0].ca.unsealed() && !up.nodes[1].ca.unsealed());

    // The certificates from `ca init` and `join` aren't due for months:
    // asked for, every node renews now (1 and 2 through node 0, sealed).
    for n in &up.nodes {
        let r = admin
            .post(format!("{}/api/v1/pki/self/renew", n.url))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
    }
    assert!(
        wait_for(30, || (0..3).all(|i| serial_of(&leaf(i)) != first[i])).await,
        "every node renewed when asked"
    );

    // Then by themselves, at 2/3 of 30 s: twice more each, at least.
    let mut seen: Vec<Vec<_>> = (0..3).map(|i| vec![first[i].clone()]).collect();
    let end = tokio::time::Instant::now() + Duration::from_secs(4 * LIFE);
    while seen.iter().any(|s| s.len() < 4) && tokio::time::Instant::now() < end {
        for (i, s) in seen.iter_mut().enumerate() {
            let now = serial_of(&leaf(i));
            if s.last() != Some(&now) {
                s.push(now);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        seen.iter().all(|s| s.len() >= 4),
        "each renewed three times: {:?}",
        seen
    );

    let pki = up.nodes[0].ca.current_linearizable().await.unwrap();
    for (i, s) in seen.iter().enumerate() {
        // Each renewed from the one before it: the node presented its
        // current certificate every time, never a pooled or resumed one.
        for w in s.windows(2) {
            let rec = pki.certs.get(&w[1]).expect("on record");
            assert_eq!(rec.renews.as_ref(), Some(&w[0]), "node {}", i);
            assert_eq!(rec.identity.member, Some(nid(i as u64)));
            assert!(
                rec.not_after_ms - rec.not_before_ms
                    <= raidish_auth::ca::BACKDATE_MS + LIFE * 1000 + 1000
            );
        }
        // The files hold what's served (key and certificate as a pair).
        let tls = tls_in(&up.tls_dirs[i]);
        let (chain, _key) = tls.identity().unwrap().unwrap();
        assert_eq!(chain[0].to_vec(), leaf(i));
        let dir = &up.tls_dirs[i];
        assert!(!dir.join("cert.pem.new").exists() && !dir.join("key.pem.new").exists());
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(tls.key_path().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // A new connection gets the new certificate; the old one, whose
    // server certificate has run out since, is still up.
    let fresh = connect(&up.admin_dir, up.tls[1]).await;
    assert_eq!(served(&fresh), leaf(1));
    assert_ne!(serial_of(&served(&kept)), serial_of(&leaf(1)));
    assert!(get_on(&mut kept).await.contains("200"));
    // Node to node too: node 1's peers took its newest (it renewed
    // through them with it), and Raft never stopped.
    assert!(up.nodes.iter().all(|n| n.cluster.pki().is_some()));
    for n in &up.nodes {
        n.stop().await;
    }
}
