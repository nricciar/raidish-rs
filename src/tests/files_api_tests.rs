//! The file API on two "nodes" (each its own writer) sharing a committer.

use super::*;
use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority};
use crate::test_util::TestPool;
use rocket::figment::util::map;
use rocket::local::asynchronous::Client;

const MB: usize = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn lease(auth: &LocalAuthority, resource: &str, owner: &str) -> u64 {
    let op = LeaseOp::Acquire {
        resource: resource.into(),
        owner: owner.into(),
        desc: format!("test {}", owner),
        ttl_ms: 600_000,
    };
    match auth.propose(op).await.unwrap() {
        Response::Granted { epoch, .. } => epoch,
        r => panic!("{} not granted: {:?}", resource, r),
    }
}

/// `ObjectLeases` over the in-memory cluster state.
struct TestLeases(Arc<LocalAuthority>);

struct TestGuard {
    auth: Arc<LocalAuthority>,
    resource: String,
    lease: ObjLease,
}

#[async_trait]
impl ObjectGuard for TestGuard {
    fn lease(&self) -> ObjLease {
        self.lease.clone()
    }
    async fn release(&self) {
        let op = LeaseOp::Release {
            resource: self.resource.clone(),
            owner: self.lease.owner.clone(),
            epoch: self.lease.epoch,
        };
        self.auth.propose(op).await.unwrap();
    }
}

#[async_trait]
impl ObjectLeases for TestLeases {
    async fn take(&self, resource: &str, desc: &str) -> Result<Box<dyn ObjectGuard>, String> {
        let owner = format!("upload-{}", desc);
        let op = LeaseOp::Acquire {
            resource: resource.into(),
            owner: owner.clone(),
            desc: desc.into(),
            ttl_ms: 600_000,
        };
        match self.0.propose(op).await? {
            Response::Granted { epoch, .. } => Ok(Box::new(TestGuard {
                auth: self.0.clone(),
                resource: resource.into(),
                lease: ObjLease { owner, epoch },
            })),
            other => Err(format!("{} is held: {:?}", resource, other)),
        }
    }
}

struct Setup {
    pool: TestPool,
    auth: Arc<LocalAuthority>,
    committer: Arc<Committer<Pool>>,
}

async fn setup() -> Setup {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, POOL, "c").await;
    let fs = Arc::new(pool.load().await);
    let committer =
        Arc::new(Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await);
    Setup {
        pool,
        auth,
        committer,
    }
}

/// A node's file API, with its own writer (None: no writer yet).
async fn node(s: &Setup, id: &str, with_writer: bool) -> (Client, Option<Arc<FileSystem<Pool>>>) {
    let writer = if with_writer {
        let resource = format!("writer/{}", id);
        let epoch = lease(&s.auth, &resource, id).await;
        let sink = Arc::new(DirectSink {
            committer: s.committer.clone(),
            writer: resource,
            epoch,
        });
        Some(Arc::new(
            FileSystem::load_writer(s.pool.dev().await, sink)
                .await
                .unwrap(),
        ))
    } else {
        None
    };
    let state = FilesState {
        node: id.into(),
        writer: Arc::new(std::sync::RwLock::new(writer.clone())),
        read_only: None,
        leases: Arc::new(TestLeases(s.auth.clone())),
    };
    let rocket = rocket::custom(
        rocket::Config::figment()
            .merge(("log_level", "off"))
            .merge(("limits", map!["upload" => "8 MiB"])),
    )
    .manage(state)
    .mount("/", routes());
    (Client::untracked(rocket).await.unwrap(), writer)
}

async fn get(c: &Client, uri: &str) -> (Status, Vec<u8>) {
    let r = c.get(uri.to_string()).dispatch().await;
    let status = r.status();
    (status, r.into_bytes().await.unwrap_or_default())
}

async fn put(c: &Client, uri: &str, body: &[u8]) -> Status {
    c.put(uri.to_string()).body(body).dispatch().await.status()
}

async fn ls(c: &Client, uri: &str) -> Vec<DirEntry> {
    let r = c.get(uri.to_string()).dispatch().await;
    assert_eq!(r.status(), Status::Ok, "{}", uri);
    r.into_json().await.unwrap()
}

#[tokio::test]
async fn any_node_reads_and_writes_files() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let (b, wb) = node(&s, "b", true).await;

    let big = pattern(3 * MB + 11, 1);
    let r = a.put("/api/v1/files/big").body(&big).dispatch().await;
    assert_eq!(r.status(), Status::Ok);
    let written: Written = r.into_json().await.unwrap();
    assert_eq!(written.size, big.len() as u64);

    // The other node sees it as soon as it's committed.
    assert_eq!(
        get(&b, "/api/v1/files/big").await,
        (Status::Ok, big.clone())
    );

    // Directories and nested files, from either node.
    assert_eq!(put(&a, "/api/v1/dir/docs", b"").await, Status::Ok);
    assert_eq!(put(&b, "/api/v1/files/docs/x", b"hello").await, Status::Ok);
    let root = ls(&b, "/api/v1/dir").await;
    let names: Vec<(&str, &str, u64)> = root
        .iter()
        .map(|e| (e.name.as_str(), e.kind.as_str(), e.size))
        .collect();
    assert_eq!(
        names,
        vec![("big", "file", big.len() as u64), ("docs", "dir", 0)]
    );
    assert_eq!(root[0].id, written.id);
    let docs = ls(&a, "/api/v1/dir/docs").await;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].name, "x");

    // Replace on one node, read on the other; delete likewise.
    assert_eq!(put(&b, "/api/v1/files/big", b"small now").await, Status::Ok);
    assert_eq!(
        get(&a, "/api/v1/files/big").await,
        (Status::Ok, b"small now".to_vec())
    );
    assert_eq!(ls(&a, "/api/v1/dir").await[0].id, written.id, "same object");
    assert_eq!(
        b.delete("/api/v1/files/docs/x").dispatch().await.status(),
        Status::Ok
    );
    assert_eq!(get(&a, "/api/v1/files/docs/x").await.0, Status::NotFound);

    for w in [wa, wb].into_iter().flatten() {
        w.return_metaslabs().await;
    }
    drop((a, b));
    let fs = s.pool.load().await;
    assert_eq!(
        fs.read_file(Path::new("big")).await.unwrap(),
        b"small now".to_vec()
    );
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn uploads_are_refused_cleanly() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let (none, _) = node(&s, "z", false).await;

    // A file someone else has open for writing.
    assert_eq!(put(&a, "/api/v1/files/f", b"v1").await, Status::Ok);
    let id = ls(&a, "/api/v1/dir").await[0].id;
    lease(&s.auth, &object_resource(id), "someone").await;
    assert_eq!(put(&a, "/api/v1/files/f", b"v2").await, Status::Conflict);
    assert_eq!(
        get(&a, "/api/v1/files/f").await,
        (Status::Ok, b"v1".to_vec())
    );

    // Too big: refused, and the new file isn't left behind.
    let huge = pattern(9 * MB, 2);
    assert_eq!(
        put(&a, "/api/v1/files/huge", &huge).await,
        Status::BadRequest
    );
    assert_eq!(get(&a, "/api/v1/files/huge").await.0, Status::NotFound);

    // Not a file / not there / no writer.
    assert_eq!(put(&a, "/api/v1/dir/d", b"").await, Status::Ok);
    assert_eq!(put(&a, "/api/v1/files/d", b"x").await, Status::BadRequest);
    assert_eq!(get(&a, "/api/v1/files/nope").await.0, Status::NotFound);
    assert_eq!(
        put(&none, "/api/v1/files/g", b"x").await,
        Status::ServiceUnavailable
    );

    wa.unwrap().return_metaslabs().await;
    drop(a);
    let fs = s.pool.load().await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}
