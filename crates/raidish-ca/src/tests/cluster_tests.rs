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
use raidish_types::pki::{EnrollScope, Enrollment, PkiOp, ProfileKind, RevocationReason, TokenId};
use raidish_types::types::{Epoch, Grant, MetaslabIdx, NodeId};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

struct NoFence;
impl raidish_types::types::FenceObserver for NoFence {
    fn observe_grants(&self, _: Epoch, _: &mut dyn Iterator<Item = (MetaslabIdx, Grant)>) {}
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

struct Node {
    cluster: Arc<Cluster>,
    ca: Arc<CaService>,
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
    raidish_auth::http::spawn(("127.0.0.1", port), tls, app, async move {
        stop.notified().await
    })
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
    let m = members(paths, raft_port);
    let name = m.me.name();
    let cluster = Cluster::start(&name, m, root.join(&name), Arc::new(NoFence))
        .await
        .unwrap();
    let ca = CaService::start(cluster.clone(), CaOptions::default()).unwrap();
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
    let tls = tls_in(tls_dir);
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
        cluster,
        ca,
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
            &LeaseOp::Pki(PkiOp::CreateEnrollment {
                id: forged.id.clone(),
                enrollment: Box::new(e),
            }),
            false,
        )
        .await
        .unwrap();
    assert_eq!(r, Response::Recorded);
    let e = match crate::join::join(&n0.url, &forged, m1.me, &m1.fingerprint).await {
        Err(e) => e,
        Ok(_) => panic!("joined with a forged enrollment"),
    };
    assert!(e.contains("MAC"), "{}", e);

    // CA ops on the lease API, from outside: refused, even "forwarded" by
    // an admin.
    let seal = LeaseOp::Pki(PkiOp::Seal);
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
            recovery_key: out.recovery_key.to_string(),
        })
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{:?}", r.text().await);
    assert!(n1.ca.unsealed());
    let r = admin
        .post(format!("{}/api/v1/pki/unseal", n2.url))
        .json(&crate::api::UnsealRequest {
            recovery_key:
                "RDK1-00000000-00000000-00000000-00000000-00000000-00000000-00000000-00000000"
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
