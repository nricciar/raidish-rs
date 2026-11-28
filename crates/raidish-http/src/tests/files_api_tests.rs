//! The file API on two "nodes" (each its own writer) sharing a committer.

use super::*;
use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority};
use crate::object_lease::ObjectGuard;
use crate::test_util::TestPool;
use crate::types::{Epoch, Holding, LeaseRef};
use async_trait::async_trait;
use axum::http::StatusCode as Status;
use raidish_auth::testing::Client;
use raidish_core::role::WriterFs;

const MB: usize = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn lease(auth: &LocalAuthority, resource: impl std::fmt::Display, owner: &str) -> Epoch {
    let resource = crate::types::res(resource);
    let op = LeaseOp::Acquire {
        resource: resource.clone(),
        owner: owner.into(),
        holder: crate::types::Holder::default().with_detail(format!("test {}", owner)),
        ttl_ms: 600_000,
    };
    match auth.propose(op).await.unwrap() {
        Response::Granted { epoch, .. } => epoch,
        r => panic!("{} not granted: {:?}", resource, r),
    }
}

/// Run (once) just before the next object lease is taken.
type BeforeTake = Arc<std::sync::Mutex<Option<futures::future::BoxFuture<'static, ()>>>>;

/// `ObjectLeases` over the in-memory cluster state.
struct TestLeases(Arc<LocalAuthority>, BeforeTake);

struct TestGuard {
    auth: Arc<LocalAuthority>,
    resource: crate::types::LeaseResource,
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
            holder: Holding::new(self.lease.owner.clone(), self.lease.epoch),
        };
        self.auth.propose(op).await.unwrap();
    }
}

#[async_trait]
impl ObjectLeases for TestLeases {
    async fn take(
        &self,
        resource: crate::types::LeaseResource,
        desc: crate::types::Holder,
    ) -> Result<Box<dyn ObjectGuard>, String> {
        let before = self.1.lock().unwrap().take();
        if let Some(before) = before {
            before.await;
        }
        let owner = format!("upload-{}", desc);
        let op = LeaseOp::Acquire {
            resource: resource.clone(),
            owner: owner.clone(),
            holder: desc,
            ttl_ms: 600_000,
        };
        match self.0.propose(op).await? {
            Response::Granted { epoch, .. } => Ok(Box::new(TestGuard {
                auth: self.0.clone(),
                resource: resource.clone(),
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
    before_take: BeforeTake,
}

async fn setup() -> Setup {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, &POOL, "c").await;
    let fs = Arc::new(pool.load().await);
    let committer =
        Arc::new(Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await);
    Setup {
        pool,
        auth,
        committer,
        before_take: Default::default(),
    }
}

/// A node's file API, with its own writer (None: no writer yet).
async fn node(s: &Setup, id: &str, with_writer: bool) -> (Client, Option<Arc<WriterFs<Pool>>>) {
    let (app, writer) = node_app(s, id, with_writer).await;
    (
        Client::new(app, raidish_auth::auth::AuthState(None)),
        writer,
    )
}

/// `node`'s routes.
async fn node_app(
    s: &Setup,
    id: &str,
    with_writer: bool,
) -> (axum::Router, Option<Arc<WriterFs<Pool>>>) {
    let writer = if with_writer {
        let resource = format!("writer/{}", id);
        let epoch = lease(&s.auth, &resource, id).await;
        let sink = Arc::new(DirectSink {
            committer: s.committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
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
        leases: Arc::new(TestLeases(s.auth.clone(), s.before_take.clone())),
        upload_limit: 8 << 20,
    };
    (router(Arc::new(state)), writer)
}

async fn get(c: &Client, uri: &str) -> (Status, Vec<u8>) {
    let r = c.get(uri).dispatch().await;
    let status = r.status();
    (status, r.into_bytes().await.unwrap_or_default())
}

async fn put(c: &Client, uri: &str, body: &[u8]) -> Status {
    c.put(uri).body(body).dispatch().await.status()
}

async fn ls(c: &Client, uri: &str) -> Vec<DirEntry> {
    let r = c.get(uri).dispatch().await;
    assert_eq!(r.status(), Status::OK, "{}", uri);
    r.into_json().await.unwrap()
}

#[tokio::test]
async fn any_node_reads_and_writes_files() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let (b, wb) = node(&s, "b", true).await;

    let big = pattern(3 * MB + 11, 1);
    let r = a.put("/api/v1/files/big").body(&big).dispatch().await;
    assert_eq!(r.status(), Status::OK);
    let written: Written = r.into_json().await.unwrap();
    assert_eq!(written.size, big.len() as u64);

    // The other node sees it as soon as it's committed.
    assert_eq!(
        get(&b, "/api/v1/files/big").await,
        (Status::OK, big.clone())
    );

    // Directories and nested files, from either node.
    assert_eq!(put(&a, "/api/v1/dir/docs", b"").await, Status::OK);
    assert_eq!(put(&b, "/api/v1/files/docs/x", b"hello").await, Status::OK);
    let root = ls(&b, "/api/v1/dir").await;
    let names: Vec<(&str, &str, u64)> = root
        .iter()
        .map(|e| (e.name.as_str(), e.kind.name(), e.size))
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
    assert_eq!(put(&b, "/api/v1/files/big", b"small now").await, Status::OK);
    assert_eq!(
        get(&a, "/api/v1/files/big").await,
        (Status::OK, b"small now".to_vec())
    );
    assert_eq!(ls(&a, "/api/v1/dir").await[0].id, written.id, "same object");
    assert_eq!(
        b.delete("/api/v1/files/docs/x").dispatch().await.status(),
        Status::OK
    );
    assert_eq!(get(&a, "/api/v1/files/docs/x").await.0, Status::NOT_FOUND);

    for w in [wa, wb].into_iter().flatten() {
        w.return_metaslabs().await;
    }
    drop((a, b));
    let fs = s.pool.load().await;
    assert_eq!(
        fs.read_file(Path::new("big")).await.unwrap(),
        b"small now".to_vec()
    );
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn uploads_are_refused_cleanly() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let (none, _) = node(&s, "z", false).await;

    // A file someone else has open for writing.
    assert_eq!(put(&a, "/api/v1/files/f", b"v1").await, Status::OK);
    let id = ls(&a, "/api/v1/dir").await[0].id;
    lease(&s.auth, &object_resource(id), "someone").await;
    assert_eq!(put(&a, "/api/v1/files/f", b"v2").await, Status::CONFLICT);
    assert_eq!(
        get(&a, "/api/v1/files/f").await,
        (Status::OK, b"v1".to_vec())
    );

    // Too big: refused, and the new file isn't left behind.
    let huge = pattern(9 * MB, 2);
    assert_eq!(
        put(&a, "/api/v1/files/huge", &huge).await,
        Status::BAD_REQUEST
    );
    assert_eq!(get(&a, "/api/v1/files/huge").await.0, Status::NOT_FOUND);

    // Not a file / not there / no writer.
    assert_eq!(put(&a, "/api/v1/dir/d", b"").await, Status::OK);
    assert_eq!(put(&a, "/api/v1/files/d", b"x").await, Status::BAD_REQUEST);
    assert_eq!(get(&a, "/api/v1/files/nope").await.0, Status::NOT_FOUND);
    assert_eq!(
        put(&none, "/api/v1/files/g", b"x").await,
        Status::SERVICE_UNAVAILABLE
    );

    wa.unwrap().return_metaslabs().await;
    drop(a);
    let fs = s.pool.load().await;
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Two uploads of a new file: the first to get the lease writes it, and
/// the second, failing, mustn't then remove it as "the empty file it made"
/// (audit N3).
#[tokio::test]
async fn a_failed_upload_leaves_a_file_another_wrote() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let wa = wa.unwrap();
    let other = wa.clone();
    *s.before_take.lock().unwrap() = Some(Box::pin(async move {
        let mut f = other.file_writer(Path::new("f"), None).await.unwrap();
        f.write(b"theirs").await.unwrap();
        f.finish().await.unwrap();
    }));
    let huge = pattern(9 * MB, 3);
    assert_eq!(put(&a, "/api/v1/files/f", &huge).await, Status::BAD_REQUEST);
    assert!(s.before_take.lock().unwrap().is_none());
    assert_eq!(
        get(&a, "/api/v1/files/f").await,
        (Status::OK, b"theirs".to_vec())
    );

    wa.return_metaslabs().await;
    drop(a);
    let fs = s.pool.load().await;
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[test]
fn ranges_parse() {
    use ByteRange::*;
    let p = |h: &str| parse_range(Some(h), 1000);
    assert_eq!(parse_range(None, 1000), All);
    assert_eq!(p("bytes=0-99"), Part(0..100));
    assert_eq!(p("bytes=900-"), Part(900..1000));
    assert_eq!(p("bytes=-10"), Part(990..1000));
    assert_eq!(p("bytes=-5000"), Part(0..1000));
    assert_eq!(p("bytes=990-5000"), Part(990..1000));
    assert_eq!(p("bytes=1000-"), Unsatisfiable);
    assert_eq!(p("bytes=-0"), Unsatisfiable);
    // Ignored: another unit, several ranges, garbage, reversed.
    for h in [
        "items=0-1",
        "bytes=0-1,5-9",
        "bytes=x-y",
        "bytes=9-3",
        "bytes",
    ] {
        assert_eq!(p(h), All, "{}", h);
    }
    assert_eq!(parse_range(Some("bytes=0-"), 0), Unsatisfiable);
}

async fn get_range(c: &Client, uri: &str, range: &str) -> (Status, Option<String>, Vec<u8>) {
    let r = c.get(uri).header("Range", range).dispatch().await;
    let status = r.status();
    let cr = r.header("Content-Range").map(String::from);
    (status, cr, r.into_bytes().await.unwrap_or_default())
}

async fn patch(c: &Client, path_query: &str, body: &[u8]) -> (Status, Option<Written>) {
    let r = c
        .patch(format!("/api/v1/files/{}", path_query))
        .body(body)
        .dispatch()
        .await;
    let status = r.status();
    (status, r.into_json().await)
}

#[tokio::test]
async fn range_reads() {
    let s = setup().await;
    let (a, _wa) = node(&s, "a", true).await;
    let (b, _wb) = node(&s, "b", true).await;
    let data = pattern(MB / 2 + 33, 3);
    assert_eq!(put(&a, "/api/v1/files/f", &data).await, Status::OK);
    let n = data.len();

    let r = b.get("/api/v1/files/f").dispatch().await;
    assert_eq!(r.status(), Status::OK);
    assert_eq!(r.header("Accept-Ranges"), Some("bytes"));

    let (st, cr, body) = get_range(&b, "/api/v1/files/f", "bytes=100-199").await;
    assert_eq!(st, Status::PARTIAL_CONTENT);
    assert_eq!(cr.as_deref(), Some(format!("bytes 100-199/{}", n).as_str()));
    assert_eq!(body, data[100..200]);
    // Across records, to the end, and the last few bytes.
    let (_, _, body) = get_range(&b, "/api/v1/files/f", "bytes=120000-400000").await;
    assert_eq!(body, data[120000..=400000]);
    let (_, _, body) = get_range(&b, "/api/v1/files/f", "bytes=300000-").await;
    assert_eq!(body, data[300000..]);
    let (_, cr, body) = get_range(&b, "/api/v1/files/f", "bytes=-7").await;
    assert_eq!(body, data[n - 7..]);
    assert_eq!(
        cr.as_deref(),
        Some(format!("bytes {}-{}/{}", n - 7, n - 1, n).as_str())
    );
    let (st, _, _) = get_range(&b, "/api/v1/files/f", &format!("bytes={}-", n)).await;
    assert_eq!(st, Status::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn partial_writes_rename_and_attributes() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let (b, wb) = node(&s, "b", true).await;
    let mut expect = pattern(300_000, 4);
    assert_eq!(put(&a, "/api/v1/dir/d", b"").await, Status::OK);
    assert_eq!(put(&a, "/api/v1/files/d/f", &expect).await, Status::OK);
    let id = ls(&a, "/api/v1/dir/d").await[0].id;

    // Write in the middle and past the end, from the other node.
    let (st, _) = patch(&b, "d/f?offset=1000", b"hello").await;
    assert_eq!(st, Status::OK);
    expect[1000..1005].copy_from_slice(b"hello");
    let (_, w) = patch(&b, "d/f?offset=300000", &pattern(5000, 5)).await;
    let w = w.unwrap();
    assert_eq!((w.id, w.size), (id, 305_000));
    expect.extend_from_slice(&pattern(5000, 5));
    assert_eq!(
        get(&a, "/api/v1/files/d/f").await,
        (Status::OK, expect.clone())
    );

    // Truncate (no body), then grow with zeros.
    assert_eq!(patch(&a, "d/f?size=2000", b"").await.0, Status::OK);
    expect.truncate(2000);
    assert_eq!(get(&b, "/api/v1/files/d/f").await.1, expect);
    assert_eq!(
        patch(&b, "d/f?offset=0&size=3000", b"AB").await.0,
        Status::OK
    );
    expect[..2].copy_from_slice(b"AB");
    expect.resize(3000, 0);
    assert_eq!(get(&a, "/api/v1/files/d/f").await.1, expect);
    assert_eq!(patch(&a, "d/f?offset=0", b"x").await.0, Status::OK);
    expect[0] = b'x';
    // Not there: nothing is created.
    assert_eq!(
        patch(&b, "d/nope?offset=0", b"x").await.0,
        Status::NOT_FOUND
    );

    // Rename (into a new directory), from either node.
    assert_eq!(put(&b, "/api/v1/dir/e", b"").await, Status::OK);
    let r = b
        .post("/api/v1/rename")
        .json(&RenameRequest {
            from: "d/f".into(),
            to: "e/g".into(),
        })
        .dispatch()
        .await;
    assert_eq!(r.status(), Status::OK);
    assert_eq!(r.into_json::<FileId>().await, Some(id));
    assert_eq!(get(&a, "/api/v1/files/d/f").await.0, Status::NOT_FOUND);
    assert_eq!(get(&a, "/api/v1/files/e/g").await.1, expect);

    // Attributes.
    let status = a
        .patch("/api/v1/attrs/e/g")
        .json(&crate::inode::SetAttrs {
            mode: Some(0o640),
            uid: Some(33),
            ..Default::default()
        })
        .dispatch()
        .await
        .status();
    assert_eq!(status, Status::OK);
    let st: Stat = b
        .get("/api/v1/stat/e/g")
        .dispatch()
        .await
        .into_json()
        .await
        .unwrap();
    assert_eq!((st.id, st.kind.name(), st.size), (id, "file", 3000));
    assert_eq!((st.mode, st.uid, st.nlink), (0o640, 33, 1));
    assert_eq!(ls(&b, "/api/v1/dir/e").await[0].mode, 0o640);

    for w in [wa, wb].into_iter().flatten() {
        w.return_metaslabs().await;
    }
    drop((a, b));
    let fs = s.pool.load().await;
    assert_eq!(fs.read_file(Path::new("e/g")).await.unwrap(), expect);
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// A partial write is refused while someone else holds the file's lease.
#[tokio::test]
async fn partial_writes_respect_object_leases() {
    let s = setup().await;
    let (a, _wa) = node(&s, "a", true).await;
    assert_eq!(put(&a, "/api/v1/files/f", b"0123456789").await, Status::OK);
    let id = ls(&a, "/api/v1/dir").await[0].id;
    lease(&s.auth, &crate::committer::object_resource(id), "someone").await;
    assert_eq!(patch(&a, "f?offset=2", b"xx").await.0, Status::CONFLICT);
    assert_eq!(get(&a, "/api/v1/files/f").await.1, b"0123456789");
}

/// N3: a PATCH whose body is refused partway (too big) commits none of it,
/// nor its new size.
#[tokio::test]
async fn a_refused_patch_changes_nothing() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let orig = pattern(MB, 3);
    assert_eq!(put(&a, "/api/v1/files/f", &orig).await, Status::OK);
    let huge = pattern(9 * MB, 4);
    assert_eq!(
        patch(&a, "f?offset=0&size=5", &huge).await.0,
        Status::BAD_REQUEST
    );
    assert_eq!(get(&a, "/api/v1/files/f").await, (Status::OK, orig.clone()));
    // Later writes go on from the committed file.
    let (st, w) = patch(&a, "f?offset=0", b"AB").await;
    assert_eq!(st, Status::OK);
    assert_eq!(w.unwrap().size, orig.len() as u64);
    let mut want = orig.clone();
    want[..2].copy_from_slice(b"AB");
    assert_eq!(get(&a, "/api/v1/files/f").await, (Status::OK, want));

    wa.unwrap().return_metaslabs().await;
    drop(a);
    let fs = s.pool.load().await;
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Make record `idx` of file `name` unreadable: every block of it flipped
/// on every disk, past what parity rebuilds.
async fn ruin_record(s: &Setup, name: &str, idx: u64) {
    let fs = s.pool.load().await;
    let (_, d) = fs.stat_file(Path::new(name)).await.unwrap();
    let obj = ObjTree::new(d.blksz as usize, Sizing::Fixed, d.size, d.tree.clone());
    let leaf = fs
        .tree_lookup(&obj, &obj.root(), 0, idx)
        .await
        .unwrap()
        .unwrap();
    drop(fs);
    let mut spots = std::collections::BTreeSet::new();
    for lba in (0..leaf.asize as u64).map(|i| leaf.lba + i) {
        for disk in 0..s.pool.paths.len() {
            spots.insert((disk, TestPool::stripe_offset(lba)));
        }
    }
    for (disk, off) in spots {
        s.pool.corrupt(disk, off);
    }
}

/// N3: a GET whose file can't be read fails: with an error status if it
/// can't start, and with the body cut off (not ended short as if complete)
/// if it fails partway.
#[tokio::test]
async fn an_unreadable_file_fails_rather_than_ends_short() {
    let s = setup().await;
    let (a, wa) = node(&s, "a", true).await;
    let data = pattern(3 * MB, 5);
    assert_eq!(put(&a, "/api/v1/files/late", &data).await, Status::OK);
    assert_eq!(put(&a, "/api/v1/files/early", &data).await, Status::OK);
    wa.unwrap().return_metaslabs().await;
    drop(a);
    let blksz = s.pool.load().await.file_blksz() as u64;
    // Past the first piece the GET reads before answering.
    let late = (2 * MB as u64) / blksz;
    assert!(late >= crate::files_api::STREAM_RECORDS as u64);
    ruin_record(&s, "late", late).await;
    ruin_record(&s, "early", 0).await;

    // Over a real connection, as a client sees it: an in-process request
    // reads the body directly, and would see the failure where a socket
    // sees a body that just ends.
    let (app, _) = node_app(&s, "b", true).await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let at = raidish_auth::http::spawn(
        "127.0.0.1:0",
        None,
        raidish_auth::http::finish(app, raidish_auth::auth::AuthState(None)),
        async move {
            let _ = stopped.await;
        },
    )
    .await
    .unwrap();
    let base = format!("http://{}/api/v1/files", at);
    let http = reqwest::Client::new();
    let mut up = false;
    for _ in 0..100 {
        if http.get(format!("{}/early", base)).send().await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(up, "server up");
    let r = http.get(format!("{}/early", base)).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 500);
    let r = http.get(format!("{}/late", base)).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200, "it starts");
    assert_eq!(
        r.content_length(),
        Some(data.len() as u64),
        "declared in full"
    );
    let body = r.bytes().await;
    assert!(
        body.is_err(),
        "and is cut off, not ended short: {:?}",
        body.map(|b| b.len())
    );
    let _ = stop.send(());
}
