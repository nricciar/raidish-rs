//! The S3 REST API over HTTP, with signed requests: objects, ranges,
//! conditions, listings, multipart, copies, auth and digest failures.

use super::*;
use crate::frontend::testing::Fixture;
use crate::frontend::{Owner, Route};
use crate::pki::Role;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::types::ep;
use std::collections::BTreeMap;

struct S3 {
    addr: String,
    http: reqwest::Client,
    key: (String, String),
    fx: Fixture,
    _fe: Arc<Frontend>,
    _fs: Arc<crate::fs::FileSystem<Pool>>,
}

struct Resp {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
}

impl Resp {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    fn h(&self, n: &str) -> String {
        self.headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    }
}

async fn server(pool: &TestPool) -> S3 {
    let fs = Arc::new(pool.format().await);
    fs.create_service(&ds("b"), DatasetFormat::S3, Default::default())
        .await
        .unwrap();
    let fx = Fixture::new(vec![]);
    let fe = fx.frontend();
    let mut routes = BTreeMap::new();
    routes.insert(
        "b".to_string(),
        Route {
            format: DatasetFormat::S3,
            port: None,
            created_ms: 1,
            owner: Some(Owner {
                node: "n1".into(),
                addr: "127.0.0.1".into(),
            }),
        },
    );
    fe.set_routes(routes, BTreeMap::new());
    fe.serve_local(&ds("b"), DatasetFormat::S3, fs.clone(), ep(1))
        .await
        .unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(serve(fe.clone(), l));
    S3 {
        addr,
        http: fx.http(),
        key: fx.key(Role::Admin),
        fx,
        _fe: fe,
        _fs: fs,
    }
}

impl S3 {
    /// A signed request; `extra` headers are signed too.
    async fn req(&self, method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]) -> Resp {
        let key = self.key.clone();
        self.req_as(&key, method, path, extra, body).await
    }

    async fn req_as(
        &self,
        key: &(String, String),
        method: &str,
        path: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> Resp {
        let m: Method = method.parse().unwrap();
        let uri: hyper::Uri = format!("https://{}{}", self.addr, path).parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("host", HeaderValue::from_str(&self.addr).unwrap());
        for (k, v) in extra {
            h.insert(
                hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let signed = sigv4::sign(&key.0, &key.1, "us-east-1", "s3", &m, &uri, &h, body);
        self.send(method, path, &h, &signed, body).await
    }

    async fn unsigned(&self, method: &str, path: &str) -> Resp {
        self.send(method, path, &HeaderMap::new(), &[], b"").await
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        h: &HeaderMap,
        signed: &[(String, String)],
        body: &[u8],
    ) -> Resp {
        let mut rb = self.http.request(
            reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
            format!("https://{}{}", self.addr, path),
        );
        for (k, v) in h {
            if k != "host" {
                rb = rb.header(k.as_str(), v.to_str().unwrap());
            }
        }
        for (k, v) in signed {
            rb = rb.header(k.as_str(), v.as_str());
        }
        let r = rb.body(body.to_vec()).send().await.unwrap();
        Resp {
            status: r.status().as_u16(),
            headers: r.headers().clone(),
            body: r.bytes().await.unwrap().to_vec(),
        }
    }
}

fn data(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(29) ^ seed).collect()
}

#[tokio::test]
async fn objects_ranges_and_conditions() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let body = data(CHUNK + 12345, 3);
    let r = s
        .req(
            "PUT",
            "/b/dir/obj%20one",
            &[("content-type", "text/plain"), ("x-amz-meta-color", "blue")],
            &body,
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let etag = r.h("etag");
    assert_eq!(etag, format!("\"{}\"", hex::encode(Md5::digest(&body))));

    let r = s.req("GET", "/b/dir/obj%20one", &[], b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, body);
    assert_eq!(r.h("content-type"), "text/plain");
    assert_eq!(r.h("x-amz-meta-color"), "blue");

    let r = s.req("HEAD", "/b/dir/obj%20one", &[], b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.h("content-length"), body.len().to_string());
    assert!(r.body.is_empty());

    // A range across the chunk boundary, and a suffix.
    let a = CHUNK - 10;
    let range = format!("bytes={}-{}", a, a + 99);
    let r = s
        .req("GET", "/b/dir/obj%20one", &[("range", &range)], b"")
        .await;
    assert_eq!(r.status, 206);
    assert_eq!(r.body, &body[a..a + 100]);
    assert_eq!(
        r.h("content-range"),
        format!("bytes {}-{}/{}", a, a + 99, body.len())
    );
    let r = s
        .req("GET", "/b/dir/obj%20one", &[("range", "bytes=-5")], b"")
        .await;
    assert_eq!(r.body, &body[body.len() - 5..]);
    let r = s
        .req(
            "GET",
            "/b/dir/obj%20one",
            &[("range", "bytes=99999999-")],
            b"",
        )
        .await;
    assert_eq!(r.status, 416);

    // Conditions.
    let r = s
        .req("GET", "/b/dir/obj%20one", &[("if-none-match", &etag)], b"")
        .await;
    assert_eq!(r.status, 304);
    let r = s
        .req("GET", "/b/dir/obj%20one", &[("if-match", "\"nope\"")], b"")
        .await;
    assert_eq!(r.status, 412);
    assert!(r.text().contains("PreconditionFailed"));

    // Missing, then deleted.
    let r = s.req("GET", "/b/missing", &[], b"").await;
    assert_eq!(r.status, 404);
    assert!(r.text().contains("<Code>NoSuchKey</Code>"));
    let r = s.req("DELETE", "/b/dir/obj%20one", &[], b"").await;
    assert_eq!(r.status, 204);
    let r = s.req("HEAD", "/b/dir/obj%20one", &[], b"").await;
    assert_eq!(r.status, 404);
    // An empty object.
    let r = s.req("PUT", "/b/empty", &[], b"").await;
    assert_eq!(r.status, 200);
    let r = s.req("GET", "/b/empty", &[], b"").await;
    assert_eq!((r.status, r.body.len()), (200, 0));
}

#[tokio::test]
async fn auth_and_digests() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let r = s.unsigned("GET", "/b/x").await;
    assert_eq!(r.status, 403, "{}", r.text());
    let r = s.unsigned("GET", "/nobucket/x").await;
    assert_eq!(r.status, 403);
    // A readonly key reads, and writes nothing.
    let ro = s.fx.key(Role::Readonly);
    let r = s.req_as(&ro, "PUT", "/b/ro", &[], b"x").await;
    assert_eq!(r.status, 403, "{}", r.text());
    assert!(r.text().contains("AccessDenied"));
    assert_eq!(
        s.req_as(&ro, "GET", "/b?list-type=2", &[], b"")
            .await
            .status,
        200
    );
    // A key the cluster didn't make (or a changed one).
    let mut claims = crate::frontend::keys::AccessKey::parse(&ro.0).unwrap();
    claims.role = Role::Admin;
    let forged = (claims.encode().unwrap(), ro.1.clone());
    let r = s.req_as(&forged, "PUT", "/b/ro", &[], b"x").await;
    assert_eq!(r.status, 403, "{}", r.text());
    let r = s
        .req_as(&("AKIAEXAMPLE".into(), "x".into()), "GET", "/b/x", &[], b"")
        .await;
    assert!(r.text().contains("InvalidAccessKeyId"), "{}", r.text());
    // Signed, with a wrong MD5.
    let r = s
        .req(
            "PUT",
            "/b/x",
            &[("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==")],
            b"hello",
        )
        .await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("BadDigest"));
    let r = s.req("HEAD", "/b/x", &[], b"").await;
    assert_eq!(r.status, 404);
    // A checksum, kept and returned on request.
    let r = s
        .req(
            "PUT",
            "/b/x",
            &[("x-amz-checksum-crc32", "NhCmhg==")],
            b"hello",
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = s
        .req(
            "PUT",
            "/b/y",
            &[("x-amz-checksum-crc32", "AAAAAA==")],
            b"hello",
        )
        .await;
    assert_eq!(r.status, 400, "{}", r.text());
    let r = s
        .req("HEAD", "/b/x", &[("x-amz-checksum-mode", "ENABLED")], b"")
        .await;
    assert_eq!(r.h("x-amz-checksum-crc32"), "NhCmhg==");
    let r = s.req("HEAD", "/b/x", &[], b"").await;
    assert_eq!(r.h("x-amz-checksum-crc32"), "");
    // Unknown buckets and subresources.
    let r = s.req("GET", "/nobucket?list-type=2", &[], b"").await;
    assert_eq!(r.status, 404);
    assert!(r.text().contains("NoSuchBucket"));
    let r = s.req("GET", "/b?acl", &[], b"").await;
    assert_eq!(r.status, 501);
}

#[tokio::test]
async fn listings() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    for k in ["a/1", "a/2", "a/b/3", "c", "d e"] {
        assert_eq!(
            s.req("PUT", &format!("/b/{}", k.replace(' ', "%20")), &[], b"x")
                .await
                .status,
            200
        );
    }
    let r = s.req("GET", "/b?list-type=2&delimiter=%2F", &[], b"").await;
    let t = r.text();
    assert!(t.contains("<Prefix>a/</Prefix>"), "{}", t);
    assert!(t.contains("<Key>c</Key>") && t.contains("<Key>d e</Key>"));
    assert!(t.contains("<KeyCount>3</KeyCount>"));
    let r = s
        .req("GET", "/b?list-type=2&prefix=a%2F&delimiter=%2F", &[], b"")
        .await;
    let t = r.text();
    assert!(
        t.contains("<Key>a/1</Key>") && t.contains("<Prefix>a/b/</Prefix>"),
        "{}",
        t
    );

    // Pages of two.
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let q = match &token {
            Some(t) => format!("/b?list-type=2&max-keys=2&continuation-token={}", t),
            None => "/b?list-type=2&max-keys=2".to_string(),
        };
        let x = parse_xml(&s.req("GET", &q, &[], b"").await.body).unwrap();
        keys.extend(x.find_all("Contents").map(|c| c.text_of("Key").unwrap()));
        match x.text_of("NextContinuationToken") {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(keys, ["a/1", "a/2", "a/b/3", "c", "d e"]);
    // v1, url-encoded.
    let r = s
        .req("GET", "/b?marker=c&encoding-type=url", &[], b"")
        .await;
    let t = r.text();
    assert!(
        t.contains("<Key>d%20e</Key>") && !t.contains("<Key>c</Key>"),
        "{}",
        t
    );

    // Batch delete.
    let del = "<Delete><Object><Key>a/1</Key></Object><Object><Key>c</Key></Object></Delete>";
    let r = s.req("POST", "/b?delete", &[], del.as_bytes()).await;
    assert!(
        r.text().contains("<Deleted><Key>a/1</Key></Deleted>"),
        "{}",
        r.text()
    );
    let x = parse_xml(&s.req("GET", "/b?list-type=2", &[], b"").await.body).unwrap();
    let left: Vec<_> = x
        .find_all("Contents")
        .map(|c| c.text_of("Key").unwrap())
        .collect();
    assert_eq!(left, ["a/2", "a/b/3", "d e"]);
    // Buckets.
    let r = s.req("GET", "/", &[], b"").await;
    assert!(r.text().contains("<Name>b</Name>"));
    assert_eq!(s.req("HEAD", "/b", &[], b"").await.status, 200);
}

#[tokio::test]
async fn multipart_and_copies() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let r = s
        .req(
            "POST",
            "/b/big?uploads",
            &[("content-type", "video/x"), ("x-amz-meta-m", "1")],
            b"",
        )
        .await;
    let id = parse_xml(&r.body).unwrap().text_of("UploadId").unwrap();
    let p1 = data(super::super::MIN_PART as usize, 1);
    let p2 = data(1000, 2);
    let e1 = s
        .req(
            "PUT",
            &format!("/b/big?partNumber=1&uploadId={}", id),
            &[],
            &p1,
        )
        .await
        .h("etag");
    let e2 = s
        .req(
            "PUT",
            &format!("/b/big?partNumber=2&uploadId={}", id),
            &[],
            &p2,
        )
        .await
        .h("etag");
    let r = s
        .req("GET", &format!("/b/big?uploadId={}", id), &[], b"")
        .await;
    assert!(
        r.text().contains("<PartNumber>2</PartNumber>"),
        "{}",
        r.text()
    );
    let r = s.req("GET", "/b?uploads", &[], b"").await;
    assert!(r.text().contains(&id));
    let done = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        e1, e2
    );
    let r = s
        .req(
            "POST",
            &format!("/b/big?uploadId={}", id),
            &[],
            done.as_bytes(),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text().contains("-2&quot;") || r.text().contains("-2\""),
        "{}",
        r.text()
    );
    let whole: Vec<u8> = [p1.clone(), p2.clone()].concat();
    let r = s.req("GET", "/b/big", &[], b"").await;
    assert_eq!(r.body, whole);
    assert_eq!(r.h("content-type"), "video/x");
    let r = s.req("GET", "/b/big?partNumber=2", &[], b"").await;
    assert_eq!((r.status, r.body.clone()), (206, p2.clone()));
    assert_eq!(r.h("x-amz-mp-parts-count"), "2");
    // Gone from the uploads.
    let r = s.req("GET", "/b?uploads", &[], b"").await;
    assert!(!r.text().contains(&id));

    // Copy (metadata kept), then replaced.
    let r = s
        .req("PUT", "/b/copy", &[("x-amz-copy-source", "/b/big")], b"")
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = s.req("GET", "/b/copy", &[], b"").await;
    assert_eq!(r.body, whole);
    assert_eq!(r.h("x-amz-meta-m"), "1");
    let r = s
        .req(
            "PUT",
            "/b/copy",
            &[
                ("x-amz-copy-source", "b/copy"),
                ("x-amz-metadata-directive", "REPLACE"),
                ("content-type", "a/b"),
            ],
            b"",
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = s.req("HEAD", "/b/copy", &[], b"").await;
    assert_eq!(
        (r.h("content-type"), r.h("x-amz-meta-m")),
        ("a/b".into(), "".into())
    );
    let r = s
        .req("PUT", "/b/copy", &[("x-amz-copy-source", "b/copy")], b"")
        .await;
    assert_eq!(r.status, 400);

    // A part copied from a range; an aborted upload.
    let r = s.req("POST", "/b/small?uploads", &[], b"").await;
    let id = parse_xml(&r.body).unwrap().text_of("UploadId").unwrap();
    let r = s
        .req(
            "PUT",
            &format!("/b/small?partNumber=1&uploadId={}", id),
            &[
                ("x-amz-copy-source", "b/big"),
                ("x-amz-copy-source-range", "bytes=10-19"),
            ],
            b"",
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let e = parse_xml(&r.body).unwrap().text_of("ETag").unwrap();
    // A last byte of u64::MAX: refused, not overflowed.
    let max = format!("bytes=0-{}", u64::MAX);
    let bad = s
        .req(
            "PUT",
            &format!("/b/small?partNumber=2&uploadId={}", id),
            &[
                ("x-amz-copy-source", "b/big"),
                ("x-amz-copy-source-range", &max),
            ],
            b"",
        )
        .await;
    assert_eq!(bad.status, 400, "{}", bad.text());
    let done = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        e
    );
    let r = s
        .req(
            "POST",
            &format!("/b/small?uploadId={}", id),
            &[],
            done.as_bytes(),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        s.req("GET", "/b/small", &[], b"").await.body,
        &whole[10..20]
    );
    let r = s.req("POST", "/b/gone?uploads", &[], b"").await;
    let id = parse_xml(&r.body).unwrap().text_of("UploadId").unwrap();
    assert_eq!(
        s.req("DELETE", &format!("/b/gone?uploadId={}", id), &[], b"")
            .await
            .status,
        204
    );
    let r = s
        .req(
            "PUT",
            &format!("/b/gone?partNumber=1&uploadId={}", id),
            &[],
            b"x",
        )
        .await;
    assert_eq!(r.status, 404);
}

/// I-35: a DeleteBucket that ends any way but deleting (here: cancelled
/// while the committer is out of reach) lets writes in again. The bucket
/// used to stay closed, every write answered 503, for good.
#[tokio::test]
async fn a_cancelled_delete_bucket_reopens_it() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let local = s._fe.local("b").unwrap();
    let crate::frontend::Local::S3(bk) = &*local else {
        panic!("b isn't served here");
    };
    // No committer endpoints: `delete_service` retries for seconds.
    let r = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        delete_bucket(&s._fe, bk),
    )
    .await;
    assert!(r.is_err(), "the delete finished");
    assert!(bk.check_writable().is_ok(), "left closed");
    let r = s.req("PUT", "/b/after", &[], b"data").await;
    assert_eq!(r.status, 200, "{}", r.text());
}

/// I-35: a service delete disables the service first. One that ends any
/// way but deleting (here: cancelled while it waits for the service to
/// stop being served) enables it again. It used to stay disabled for good.
#[tokio::test]
async fn a_cancelled_delete_service_enables_it_again() {
    use crate::cluster::LeaseOp;
    use crate::cluster::state::{POOL, Response};
    use crate::committer::{Authority, PoolAdmin};
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.create_service(&ds("b"), DatasetFormat::S3, Default::default())
        .await
        .unwrap();
    let auth = Arc::new(crate::committer::LocalAuthority::default());
    let op = LeaseOp::Acquire {
        resource: POOL.clone(),
        owner: "c".into(),
        holder: crate::types::Holder::default().with_detail("c"),
        ttl_ms: 600_000,
    };
    let Response::Granted { epoch, .. } = auth.propose(op).await.unwrap() else {
        panic!("pool lease");
    };
    let committer = Arc::new(
        crate::committer::Committer::new(fs.clone(), auth, "c".into(), epoch, "n0".into()).await,
    );
    let slot: crate::committer::CommitterSlot =
        Arc::new(tokio::sync::RwLock::new(Some(committer.clone())));
    let fx = Fixture::new(vec![]);
    let fe = fx.frontend_with(slot);
    let mut routes = BTreeMap::new();
    routes.insert(
        "b".to_string(),
        Route {
            format: DatasetFormat::S3,
            port: None,
            created_ms: 1,
            owner: Some(Owner {
                node: "n1".into(),
                addr: "127.0.0.1".into(),
            }),
        },
    );
    fe.set_routes(routes, BTreeMap::new());
    let admin = crate::committer::LocalAdmin::new(committer, None);
    let enabled = || async {
        admin
            .datasets()
            .await
            .unwrap()
            .into_iter()
            .find(|d| d.name == "b")
            .unwrap()
            .props
            .enabled
    };
    assert!(enabled().await);

    // Still served: the delete waits for that to stop, and is cancelled.
    let r = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        fe.delete_service(&ds("b")),
    )
    .await;
    assert!(r.is_err(), "the delete finished");
    let mut back = false;
    for _ in 0..100 {
        if enabled().await {
            back = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(back, "left disabled");
}
