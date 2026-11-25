//! The S3 REST API (path-style, and virtual-hosted under `s3_domain`):
//! buckets (list, create, delete, head, location), objects (put, get with
//! ranges and conditions, head, delete, batch delete, copy), listings (v1
//! and v2, prefixes, delimiters, pages), multipart uploads (create, parts,
//! copy parts, list, complete, abort). Requests are signed with SigV4
//! (headers or presigned URLs, plain or `aws-chunked` bodies).
//!
//! A request for a bucket another node runs is proxied there (see
//! `crate::frontend`); that node checks its signature.

use super::{Bucket, CHUNK, MAX_PUT, ObjectMeta, Part, Reading, S3Error, Segment};
use crate::committer::PoolAdmin;
use crate::dataset::DatasetFormat;
use crate::frontend::http::{self as fhttp, Body, BodyError, PayloadReader, empty, full};
use crate::frontend::{Frontend, Local, Located};
use crate::sigv4::{self, AuthError};
use crate::vdev::Pool;
use base64::Engine;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use md5::{Digest, Md5};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// Bucket subresources not supported (answered NotImplemented).
const UNSUPPORTED_BUCKET: &[&str] = &[
    "acl",
    "policy",
    "cors",
    "lifecycle",
    "tagging",
    "encryption",
    "website",
    "logging",
    "notification",
    "replication",
    "requestPayment",
    "object-lock",
    "ownershipControls",
    "publicAccessBlock",
    "accelerate",
    "analytics",
    "inventory",
    "metrics",
    "intelligent-tiering",
    "versions",
    "policyStatus",
];
const UNSUPPORTED_OBJECT: &[&str] = &[
    "acl",
    "tagging",
    "retention",
    "legal-hold",
    "torrent",
    "restore",
    "select",
    "attributes",
];
/// The standard headers an object keeps.
const KEPT_HEADERS: &[&str] = &[
    "content-encoding",
    "content-disposition",
    "cache-control",
    "content-language",
    "expires",
];
const MAX_USER_META: usize = 2048;

pub async fn serve(fe: Arc<Frontend>, l: TcpListener) {
    fhttp::serve(l, fe.tls().https.clone(), move |req| {
        let fe = fe.clone();
        async move { handle(fe, req).await }
    })
    .await
}

/// An S3 error response.
#[derive(Debug)]
pub struct Err3 {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

fn e3(status: StatusCode, code: &'static str, message: impl Into<String>) -> Err3 {
    Err3 {
        status,
        code,
        message: message.into(),
    }
}

impl From<S3Error> for Err3 {
    fn from(e: S3Error) -> Self {
        use StatusCode as S;
        match e {
            S3Error::NoSuchKey => e3(
                S::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
            ),
            S3Error::NoSuchUpload => e3(
                S::NOT_FOUND,
                "NoSuchUpload",
                "The specified multipart upload does not exist.",
            ),
            S3Error::InvalidPart(m) => e3(S::BAD_REQUEST, "InvalidPart", m),
            S3Error::InvalidPartOrder => e3(
                S::BAD_REQUEST,
                "InvalidPartOrder",
                "The list of parts was not in ascending order.",
            ),
            S3Error::EntityTooSmall => e3(
                S::BAD_REQUEST,
                "EntityTooSmall",
                "Your proposed upload is smaller than the minimum allowed object size.",
            ),
            S3Error::EntityTooLarge => e3(
                S::BAD_REQUEST,
                "EntityTooLarge",
                "Your proposed upload exceeds the maximum allowed object size.",
            ),
            S3Error::KeyTooLong => e3(S::BAD_REQUEST, "KeyTooLongError", "Your key is too long."),
            S3Error::InvalidArgument(m) => e3(S::BAD_REQUEST, "InvalidArgument", m),
            S3Error::Unavailable => unavailable(),
            S3Error::Fs(e) => {
                use crate::types::ErrorKind as K;
                match e.kind() {
                    K::Invalid => e3(S::BAD_REQUEST, "InvalidArgument", e.message()),
                    // Its lease is someone else's, or no committer: later.
                    K::Unavailable | K::Conflict => {
                        e3(S::SERVICE_UNAVAILABLE, "ServiceUnavailable", e.message())
                    }
                    _ => e3(S::INTERNAL_SERVER_ERROR, "InternalError", e.to_string()),
                }
            }
        }
    }
}

impl From<AuthError> for Err3 {
    fn from(e: AuthError) -> Self {
        let status = match e {
            AuthError::Malformed(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::FORBIDDEN,
        };
        e3(status, e.s3_code(), e.message())
    }
}

impl From<BodyError> for Err3 {
    fn from(e: BodyError) -> Self {
        use StatusCode as S;
        match e {
            BodyError::Sha256Mismatch => e3(
                S::BAD_REQUEST,
                "XAmzContentSHA256Mismatch",
                "The provided 'x-amz-content-sha256' header does not match what was computed.",
            ),
            BodyError::BadChunkSignature => e3(
                S::FORBIDDEN,
                "SignatureDoesNotMatch",
                "A chunk signature does not match.",
            ),
            BodyError::BadChecksum(h) => e3(
                S::BAD_REQUEST,
                "BadDigest",
                format!(
                    "The {} you specified did not match the calculated checksum.",
                    h
                ),
            ),
            BodyError::Malformed(m) => e3(S::BAD_REQUEST, "InvalidRequest", m),
            BodyError::TooLarge => S3Error::EntityTooLarge.into(),
            BodyError::BadLength => e3(
                S::BAD_REQUEST,
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            ),
            BodyError::Io(m) => e3(S::BAD_REQUEST, "IncompleteBody", m),
        }
    }
}

fn unavailable() -> Err3 {
    e3(
        StatusCode::SERVICE_UNAVAILABLE,
        "SlowDown",
        "The bucket is moving between nodes; please retry.",
    )
}

fn no_bucket() -> Err3 {
    e3(
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "The specified bucket does not exist",
    )
}

fn not_implemented(what: &str) -> Err3 {
    e3(
        StatusCode::NOT_IMPLEMENTED,
        "NotImplemented",
        format!("{} isn't supported by raidish", what),
    )
}

fn request_id() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("{:016X}", crate::fs::new_guid(n))
}

// ---- XML ----------------------------------------------------------------

pub fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&apos;"),
            c => o.push(c),
        }
    }
    o
}

/// A small XML writer.
struct Xml(String);

impl Xml {
    fn new(root: &str) -> Self {
        Xml(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{} xmlns=\"{}\">",
            root, XMLNS
        ))
    }
    fn open(&mut self, n: &str) -> &mut Self {
        self.0.push_str(&format!("<{}>", n));
        self
    }
    fn close(&mut self, n: &str) -> &mut Self {
        self.0.push_str(&format!("</{}>", n));
        self
    }
    fn el(&mut self, n: &str, v: &str) -> &mut Self {
        self.0.push_str(&format!("<{0}>{1}</{0}>", n, esc(v)));
        self
    }
    fn done(mut self, root: &str) -> String {
        self.0.push_str(&format!("</{}>", root));
        self.0
    }
}

/// A request body's XML; malformed is S3's `MalformedXML`.
fn parse_xml(b: &[u8]) -> Result<crate::xml::El, Err3> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|x| crate::xml::parse_root(x).ok())
        .ok_or_else(|| {
            e3(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "The XML you provided was not well-formed",
            )
        })
}

// ---- times ----------------------------------------------------------------

fn iso(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn http_date(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

fn parse_http_date(s: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc2822(s)
        .ok()
        .map(|d| d.timestamp_millis() as u64)
}

// ---- requests --------------------------------------------------------------

/// What a request is addressed to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Service,
    Bucket(String),
    Object(String, String),
}

fn target(
    fe: &Frontend,
    method: &Method,
    uri: &hyper::Uri,
    headers: &HeaderMap,
) -> Result<Target, Err3> {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .or_else(|| uri.authority().map(|a| a.as_str().to_string()))
        .unwrap_or_default();
    let host = host.rsplit_once(':').map_or(host.as_str(), |(h, p)| {
        if p.bytes().all(|b| b.is_ascii_digit()) {
            h
        } else {
            host.as_str()
        }
    });
    let path = uri.path();
    let decode = |s: &str| {
        String::from_utf8(sigv4::uri_decode(s)).map_err(|_| {
            e3(
                StatusCode::BAD_REQUEST,
                "InvalidURI",
                "Couldn't parse the specified URI.",
            )
        })
    };
    if let Some(domain) = &fe.config().s3_domain
        && let Some(b) = host.strip_suffix(&format!(".{}", domain))
        && !b.is_empty()
    {
        let key = decode(path.strip_prefix('/').unwrap_or(path))?;
        return Ok(if key.is_empty() {
            Target::Bucket(b.to_string())
        } else {
            Target::Object(b.to_string(), key)
        });
    }
    let rest = path.strip_prefix('/').unwrap_or(path);
    if rest.is_empty() {
        let _ = method;
        return Ok(Target::Service);
    }
    let (b, key) = rest.split_once('/').unwrap_or((rest, ""));
    let b = decode(b)?;
    let key = decode(key)?;
    Ok(if key.is_empty() {
        Target::Bucket(b)
    } else {
        Target::Object(b, key)
    })
}

/// A request, parsed and authenticated.
struct Req {
    method: Method,
    headers: HeaderMap,
    query: Vec<(String, String)>,
    signed: sigv4::Signed,
    body: Option<Incoming>,
}

impl Req {
    fn q(&self, k: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
    fn has(&self, k: &str) -> bool {
        self.query.iter().any(|(n, _)| n == k)
    }
    fn h(&self, k: &str) -> Option<&str> {
        self.headers.get(k).and_then(|v| v.to_str().ok())
    }
    fn reader(&mut self) -> PayloadReader {
        PayloadReader::new(
            self.body.take().expect("one body"),
            self.signed.payload.clone(),
            self.signed.chunks.clone(),
            &self.headers,
        )
    }
    /// The whole (small) body, checked.
    async fn small_body(&mut self, max: usize) -> Result<Vec<u8>, Err3> {
        let mut r = self.reader();
        let mut out = Vec::new();
        while let Some(d) = r.next().await? {
            out.extend_from_slice(&d);
            if out.len() > max {
                return Err(BodyError::TooLarge.into());
            }
        }
        if let Some(md5) = self.h("content-md5") {
            let got = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&out));
            if got != md5 {
                return Err(e3(
                    StatusCode::BAD_REQUEST,
                    "BadDigest",
                    "The Content-MD5 you specified did not match what we received.",
                ));
            }
        }
        Ok(out)
    }
}

async fn handle(fe: Arc<Frontend>, req: Request<Incoming>) -> Response<Body> {
    let rid = request_id();
    let head = req.method() == Method::HEAD;
    let resource = req.uri().path().to_string();
    let mut resp = match route(&fe, req).await {
        Ok(r) => r,
        Err(e) => {
            let body = if head {
                empty()
            } else {
                full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>{}</RequestId></Error>",
                    e.code,
                    esc(&e.message),
                    esc(&resource),
                    rid
                ))
            };
            let mut r = fhttp::status(e.status, body);
            r.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/xml"));
            r
        }
    };
    if let Ok(v) = HeaderValue::from_str(&rid) {
        resp.headers_mut().insert("x-amz-request-id", v.clone());
        resp.headers_mut().insert("x-amz-id-2", v);
    }
    resp.headers_mut()
        .insert("server", HeaderValue::from_static("raidish"));
    resp
}

async fn route(fe: &Arc<Frontend>, req: Request<Incoming>) -> Result<Response<Body>, Err3> {
    let t = target(fe, req.method(), req.uri(), req.headers())?;
    let bucket_name = match &t {
        Target::Bucket(b) | Target::Object(b, _) => Some(b.clone()),
        Target::Service => None,
    };
    let mut local = None;
    let mut exists = false;
    if let Some(b) = &bucket_name {
        match fe.locate(b, DatasetFormat::S3, fhttp::forwarded(&req), Local::bucket) {
            Located::Here(bk) => {
                local = Some(bk);
                exists = true;
            }
            Located::There(o) => {
                let port = fe.config().s3_port().ok_or_else(unavailable)?;
                return fhttp::proxy(fe.tls(), req, &o.addr, port)
                    .await
                    .ok_or_else(unavailable);
            }
            Located::Unavailable => return Err(unavailable()),
            // Only a bucket's creation goes on (below).
            Located::Missing => {}
        }
    }
    let (parts, body) = req.into_parts();
    let signed = sigv4::verify(
        fe.auth(),
        "s3",
        &parts.method,
        &parts.uri,
        &parts.headers,
        None,
    )?;
    // A readonly token's keys read (GET, HEAD) and nothing else.
    if !matches!(parts.method, Method::GET | Method::HEAD)
        && fe.auth().role(&signed.access_key) < crate::pki::Role::Admin
    {
        return Err(e3(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "Access Denied: this access key is read-only",
        ));
    }
    let mut r = Req {
        query: sigv4::query_pairs(parts.uri.query()),
        method: parts.method,

        headers: parts.headers,
        signed,
        body: Some(body),
    };
    match (t, local) {
        (Target::Service, _) => match r.method {
            Method::GET => list_buckets(fe),
            _ => Err(e3(
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
            )),
        },
        (Target::Bucket(b), None) if !exists => {
            if r.method == Method::PUT && r.query.is_empty() {
                create_bucket(fe, &b, &mut r).await
            } else {
                Err(no_bucket())
            }
        }
        (Target::Object(..), None) => Err(no_bucket()),
        (Target::Bucket(_), None) => Err(unavailable()),
        (Target::Bucket(_), Some(bk)) => bucket_op(fe, &bk, &mut r).await,
        (Target::Object(_, key), Some(bk)) => object_op(fe, &bk, &key, &mut r).await,
    }
}

fn xml_response(status: StatusCode, xml: String) -> Response<Body> {
    let mut r = fhttp::status(status, full(xml));
    r.headers_mut()
        .insert("content-type", HeaderValue::from_static("application/xml"));
    r
}

fn ok_empty(status: StatusCode) -> Response<Body> {
    fhttp::status(status, empty())
}

// ---- the service ------------------------------------------------------------

fn list_buckets(fe: &Frontend) -> Result<Response<Body>, Err3> {
    let mut x = Xml::new("ListAllMyBucketsResult");
    x.open("Owner")
        .el("ID", "raidish")
        .el("DisplayName", "raidish")
        .close("Owner")
        .open("Buckets");
    for (name, r) in fe.routes() {
        if r.format != DatasetFormat::S3 {
            continue;
        }
        x.open("Bucket")
            .el("Name", &name)
            .el("CreationDate", &iso(r.created_ms))
            .close("Bucket");
    }
    x.close("Buckets");
    Ok(xml_response(
        StatusCode::OK,
        x.done("ListAllMyBucketsResult"),
    ))
}

async fn create_bucket(
    fe: &Arc<Frontend>,
    name: &str,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    let bucket = crate::types::BucketName::try_from(name)
        .map_err(|e| e3(StatusCode::BAD_REQUEST, "InvalidBucketName", e.to_string()))?;
    let body = r.small_body(64 * 1024).await?;
    if !body.iter().all(|b| b.is_ascii_whitespace()) {
        let x = parse_xml(&body)?;
        if let Some(loc) = x.text_of("LocationConstraint")
            && !loc.is_empty()
            && loc != fe.config().region()
        {
            return Err(e3(
                StatusCode::BAD_REQUEST,
                "IllegalLocationConstraintException",
                format!("This node's region is {}", fe.config().region()),
            ));
        }
    }
    let req = crate::committer::CreateDataset {
        name: bucket.into(),

        format: DatasetFormat::S3,
        props: Default::default(),
    };
    match fe.committer().create_dataset(&req).await {
        Ok(()) => {}
        Err(e) => {
            if e.kind() == crate::types::ErrorKind::Exists {
                return Err(e3(
                    StatusCode::CONFLICT,
                    "BucketAlreadyExists",
                    "The requested bucket name is not available.",
                ));
            }
            return Err(e3(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                e.to_string(),
            ));
        }
    }
    // Answer once it's served somewhere (the agents place it).
    let n = name.to_string();
    fe.wait_routes(20, |routes| {
        routes.get(&n).is_some_and(|r| r.owner.is_some())
    })
    .await;
    let mut resp = ok_empty(StatusCode::OK);
    if let Ok(v) = HeaderValue::from_str(&format!("/{}", name)) {
        resp.headers_mut().insert("location", v);
    }
    Ok(resp)
}

// ---- buckets ---------------------------------------------------------------

async fn bucket_op(
    fe: &Arc<Frontend>,
    bk: &Arc<Bucket<Pool>>,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    if let Some(sub) = UNSUPPORTED_BUCKET.iter().find(|s| r.has(s)) {
        return Err(not_implemented(&format!(
            "the bucket's {} subresource",
            sub
        )));
    }
    match r.method.clone() {
        Method::HEAD => {
            let mut resp = ok_empty(StatusCode::OK);
            if let Ok(v) = HeaderValue::from_str(&fe.config().region()) {
                resp.headers_mut().insert("x-amz-bucket-region", v);
            }
            Ok(resp)
        }
        Method::GET if r.has("location") => {
            let region = fe.config().region();
            let v = if region == "us-east-1" {
                String::new()
            } else {
                region
            };
            Ok(xml_response(
                StatusCode::OK,
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"{}\">{}</LocationConstraint>",
                    XMLNS,
                    esc(&v)
                ),
            ))
        }
        Method::GET if r.has("versioning") => Ok(xml_response(
            StatusCode::OK,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"{}\"/>",
                XMLNS
            ),
        )),
        Method::GET if r.has("uploads") => list_uploads(bk, r).await,
        Method::GET => list_objects(bk, r).await,
        Method::PUT => Err(e3(
            StatusCode::CONFLICT,
            "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded and you already own it.",
        )),
        Method::DELETE => delete_bucket(fe, bk).await,
        Method::POST if r.has("delete") => delete_objects(bk, r).await,
        _ => Err(not_implemented("this bucket request")),
    }
}

async fn delete_bucket(fe: &Arc<Frontend>, bk: &Arc<Bucket<Pool>>) -> Result<Response<Body>, Err3> {
    let not_empty = || {
        e3(
            StatusCode::CONFLICT,
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
        )
    };
    if !bk.is_empty().await? {
        return Err(not_empty());
    }
    // Reopened when this ends, whichever way (I-35), unless deleted.
    let closed = bk.close();
    if !bk.is_empty().await? {
        return Err(not_empty());
    }
    match fe.delete_service(&bk.tree.name).await {
        Ok(true) => {
            closed.keep();
            Ok(ok_empty(StatusCode::NO_CONTENT))
        }
        Ok(false) => Err(no_bucket()),
        Err(m) => Err(e3(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable", m)),
    }
}

fn token_encode(s: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s)
}

fn token_decode(s: &str) -> Result<String, Err3> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| {
            e3(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "The continuation token provided is incorrect",
            )
        })
}

async fn list_objects(bk: &Arc<Bucket<Pool>>, r: &Req) -> Result<Response<Body>, Err3> {
    let v2 = r.q("list-type") == Some("2");
    let prefix = r.q("prefix").unwrap_or("").to_string();
    let delimiter = r
        .q("delimiter")
        .filter(|d| !d.is_empty())
        .map(str::to_string);
    let max: usize = match r.q("max-keys") {
        Some(m) => m
            .parse::<i64>()
            .map_err(|_| {
                e3(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Provided max-keys not an integer or within integer range",
                )
            })?
            .clamp(0, 1000) as usize,
        None => 1000,
    };
    let url = r.q("encoding-type") == Some("url");
    let enc = |s: &str| {
        if url {
            sigv4::uri_encode(s.as_bytes(), false)
        } else {
            s.to_string()
        }
    };
    let after = if v2 {
        match r.q("continuation-token") {
            Some(t) => Some(token_decode(t)?),
            None => r.q("start-after").map(str::to_string),
        }
    } else {
        r.q("marker").map(str::to_string)
    };
    let l = bk
        .list(&prefix, after.as_deref(), delimiter.as_deref(), max)
        .await?;
    let mut x = Xml::new("ListBucketResult");
    x.el("Name", &bk.tree.name).el("Prefix", &enc(&prefix));
    if let Some(d) = &delimiter {
        x.el("Delimiter", &enc(d));
    }
    x.el("MaxKeys", &max.to_string());
    if url {
        x.el("EncodingType", "url");
    }
    x.el("IsTruncated", if l.truncated { "true" } else { "false" });
    if v2 {
        x.el(
            "KeyCount",
            &(l.objects.len() + l.prefixes.len()).to_string(),
        );
        if let Some(t) = r.q("continuation-token") {
            x.el("ContinuationToken", t);
        }
        if let Some(s) = r.q("start-after") {
            x.el("StartAfter", &enc(s));
        }
        if l.truncated
            && let Some(last) = &l.last
        {
            x.el("NextContinuationToken", &token_encode(last));
        }
    } else {
        x.el("Marker", &enc(r.q("marker").unwrap_or("")));
        if l.truncated
            && let Some(last) = &l.last
        {
            x.el("NextMarker", &enc(last));
        }
    }
    let owner = !v2 || r.q("fetch-owner") == Some("true");
    for (k, m) in &l.objects {
        x.open("Contents")
            .el("Key", &enc(k))
            .el("LastModified", &iso(m.last_modified_ms))
            .el("ETag", &format!("\"{}\"", m.etag))
            .el("Size", &m.size.to_string())
            .el("StorageClass", "STANDARD");
        if owner {
            x.open("Owner")
                .el("ID", "raidish")
                .el("DisplayName", "raidish")
                .close("Owner");
        }
        x.close("Contents");
    }
    for p in &l.prefixes {
        x.open("CommonPrefixes")
            .el("Prefix", &enc(p))
            .close("CommonPrefixes");
    }
    Ok(xml_response(StatusCode::OK, x.done("ListBucketResult")))
}

async fn delete_objects(bk: &Arc<Bucket<Pool>>, r: &mut Req) -> Result<Response<Body>, Err3> {
    let body = r.small_body(2 << 20).await?;
    let x = parse_xml(&body)?;
    let quiet = x.text_of("Quiet").as_deref() == Some("true");
    let keys: Vec<String> = x
        .find_all("Object")
        .filter_map(|o| o.text_of("Key"))
        .collect();
    if keys.len() > 1000 {
        return Err(e3(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "At most 1000 keys per request",
        ));
    }
    let mut out = Xml::new("DeleteResult");
    for k in keys {
        match bk.delete_object(&k).await {
            Ok(_) => {
                if !quiet {
                    out.open("Deleted").el("Key", &k).close("Deleted");
                }
            }
            Err(e) => {
                let e: Err3 = e.into();
                out.open("Error")
                    .el("Key", &k)
                    .el("Code", e.code)
                    .el("Message", &e.message)
                    .close("Error");
            }
        }
    }
    Ok(xml_response(StatusCode::OK, out.done("DeleteResult")))
}

async fn list_uploads(bk: &Arc<Bucket<Pool>>, r: &Req) -> Result<Response<Body>, Err3> {
    let max: usize = r
        .q("max-uploads")
        .and_then(|m| m.parse().ok())
        .unwrap_or(1000usize)
        .min(1000);
    let prefix = r.q("prefix").unwrap_or("");
    let key_marker = r.q("key-marker").unwrap_or("");
    let id_marker = r.q("upload-id-marker");
    // By key, then id (uploads are kept by id).
    let mut all: Vec<(String, super::Upload)> = bk
        .uploads(None, usize::MAX)
        .await?
        .into_iter()
        .filter(|(_, u)| u.key.starts_with(prefix))
        .filter(|(id, u)| {
            u.key.as_str() > key_marker
                || (u.key == key_marker && id_marker.is_some_and(|m| id.as_str() > m))
        })
        .collect();
    all.sort_by(|a, b| (&a.1.key, &a.0).cmp(&(&b.1.key, &b.0)));
    let truncated = all.len() > max;
    all.truncate(max);
    let mut x = Xml::new("ListMultipartUploadsResult");
    x.el("Bucket", &bk.tree.name)
        .el("KeyMarker", key_marker)
        .el("UploadIdMarker", id_marker.unwrap_or(""))
        .el("Prefix", prefix)
        .el("MaxUploads", &max.to_string())
        .el("IsTruncated", if truncated { "true" } else { "false" });
    if truncated && let Some((id, u)) = all.last() {
        x.el("NextKeyMarker", &u.key).el("NextUploadIdMarker", id);
    }
    for (id, u) in &all {
        x.open("Upload")
            .el("Key", &u.key)
            .el("UploadId", id)
            .open("Initiator")
            .el("ID", "raidish")
            .el("DisplayName", "raidish")
            .close("Initiator")
            .open("Owner")
            .el("ID", "raidish")
            .el("DisplayName", "raidish")
            .close("Owner")
            .el("StorageClass", "STANDARD")
            .el("Initiated", &iso(u.initiated_ms))
            .close("Upload");
    }
    Ok(xml_response(
        StatusCode::OK,
        x.done("ListMultipartUploadsResult"),
    ))
}

// ---- objects ---------------------------------------------------------------

async fn object_op(
    fe: &Arc<Frontend>,
    bk: &Arc<Bucket<Pool>>,
    key: &str,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    if let Some(sub) = UNSUPPORTED_OBJECT.iter().find(|s| r.has(s)) {
        return Err(not_implemented(&format!(
            "the object's {} subresource",
            sub
        )));
    }
    match r.method.clone() {
        Method::PUT if r.has("uploadId") => {
            if r.h("x-amz-copy-source").is_some() {
                upload_part_copy(fe, bk, r).await
            } else {
                upload_part(bk, r).await
            }
        }
        Method::PUT if r.h("x-amz-copy-source").is_some() => copy_object(fe, bk, key, r).await,
        Method::PUT => put_object(bk, key, r).await,
        Method::GET if r.has("uploadId") => list_parts(bk, key, r).await,
        Method::GET | Method::HEAD => get_object(bk, key, r).await,
        Method::DELETE if r.has("uploadId") => {
            bk.abort(r.q("uploadId").unwrap_or("")).await?;
            Ok(ok_empty(StatusCode::NO_CONTENT))
        }
        Method::DELETE => {
            bk.delete_object(key).await?;
            Ok(ok_empty(StatusCode::NO_CONTENT))
        }
        Method::POST if r.has("uploads") => create_upload(bk, key, r).await,
        Method::POST if r.has("uploadId") => complete_upload(fe, bk, key, r).await,
        _ => Err(not_implemented("this object request")),
    }
}

/// The metadata a PUT (or a copy with REPLACE, or a new upload) gives.
fn meta_from(r: &Req) -> Result<ObjectMeta, Err3> {
    let mut headers = Vec::new();
    for h in KEPT_HEADERS {
        if let Some(v) = r.h(h) {
            let v = if *h == "content-encoding" {
                // aws-chunked is how it was sent, not what it is.
                v.split(',')
                    .map(str::trim)
                    .filter(|e| !e.eq_ignore_ascii_case("aws-chunked"))
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                v.to_string()
            };
            if !v.is_empty() {
                headers.push((h.to_string(), v));
            }
        }
    }
    let mut user = Vec::new();
    let mut size = 0;
    for (k, v) in &r.headers {
        if let Some(n) = k.as_str().strip_prefix("x-amz-meta-") {
            let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
            size += n.len() + v.len();
            user.push((n.to_string(), v));
        }
    }
    if size > MAX_USER_META {
        return Err(e3(
            StatusCode::BAD_REQUEST,
            "MetadataTooLarge",
            "Your metadata headers exceed the maximum allowed metadata size.",
        ));
    }
    user.sort();
    Ok(ObjectMeta {
        size: 0,
        etag: String::new(),
        last_modified_ms: 0,
        content_type: Some(
            r.h("content-type")
                .unwrap_or("binary/octet-stream")
                .to_string(),
        ),
        headers,
        user,
        segments: Vec::new(),
    })
}

/// Read the request's body into a new body of `bk`: the body, its MD5,
/// the checksums it was sent with.
async fn receive(
    bk: &Arc<Bucket<Pool>>,
    r: &mut Req,
) -> Result<(Segment, [u8; 16], Vec<(String, String)>), Err3> {
    let mut rd = r.reader();
    if rd.expected.is_some_and(|n| n > MAX_PUT) {
        return Err(S3Error::EntityTooLarge.into());
    }
    let mut w = bk.new_body(rd.expected).await?;
    loop {
        match rd.next().await {
            Ok(Some(d)) => {
                if let Err(e) = w.write(&d).await {
                    w.abort().await;
                    return Err(e.into());
                }
                if w.size > MAX_PUT {
                    w.abort().await;
                    return Err(S3Error::EntityTooLarge.into());
                }
            }
            Ok(None) => break,
            Err(e) => {
                w.abort().await;
                return Err(e.into());
            }
        }
    }
    let (seg, md5) = w.finish().await?;
    if let Some(want) = r.h("content-md5") {
        let got = base64::engine::general_purpose::STANDARD.encode(md5);
        if got != want {
            let _ = bk.discard(seg.body).await;
            return Err(e3(
                StatusCode::BAD_REQUEST,
                "BadDigest",
                "The Content-MD5 you specified did not match what we received.",
            ));
        }
    }
    Ok((seg, md5, rd.verified.clone()))
}

fn etag_header(resp: &mut Response<Body>, etag: &str) {
    if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", etag)) {
        resp.headers_mut().insert("etag", v);
    }
}

async fn put_object(
    bk: &Arc<Bucket<Pool>>,
    key: &str,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    if key.len() > super::MAX_KEY {
        return Err(S3Error::KeyTooLong.into());
    }
    let mut meta = meta_from(r)?;
    let (seg, md5, sums) = receive(bk, r).await?;
    meta.size = seg.size;
    meta.etag = hex::encode(md5);
    meta.last_modified_ms = crate::cluster::now_ms();
    meta.segments = vec![seg];
    meta.headers.extend(sums.iter().cloned());
    if let Err(e) = bk.put_object(key, meta.clone()).await {
        let _ = bk.discard(seg.body).await;
        return Err(e.into());
    }
    let mut resp = ok_empty(StatusCode::OK);
    etag_header(&mut resp, &meta.etag);
    for (k, v) in sums {
        if let (Ok(n), Ok(v)) = (
            hyper::header::HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            resp.headers_mut().insert(n, v);
        }
    }
    Ok(resp)
}

/// Whether `etag` is among a conditional header's ETags.
fn etag_in(list: &str, etag: &str) -> bool {
    list.split(',')
        .map(str::trim)
        .any(|t| t == "*" || t.trim_start_matches("W/").trim_matches('"') == etag)
}

/// The conditional headers (`prefix`: "" or "x-amz-copy-source-") against
/// `meta`: Err(status) to answer instead.
fn conditions(r: &Req, prefix: &str, meta: &ObjectMeta) -> Result<(), StatusCode> {
    let h = |n: &str| r.h(&format!("{}{}", prefix, n));
    let modified = meta.last_modified_ms / 1000 * 1000;
    let failed = StatusCode::PRECONDITION_FAILED;
    let not_modified = if prefix.is_empty() {
        StatusCode::NOT_MODIFIED
    } else {
        StatusCode::PRECONDITION_FAILED
    };
    if let Some(m) = h("if-match") {
        if !etag_in(m, &meta.etag) {
            return Err(failed);
        }
    } else if let Some(since) = h("if-unmodified-since").and_then(parse_http_date)
        && modified > since
    {
        return Err(failed);
    }
    if let Some(m) = h("if-none-match") {
        if etag_in(m, &meta.etag) {
            return Err(not_modified);
        }
    } else if let Some(since) = h("if-modified-since").and_then(parse_http_date)
        && modified <= since
    {
        return Err(not_modified);
    }
    Ok(())
}

/// The chunks to read for bytes `start..=end` of `meta`: (body, chunk,
/// from, to) within each chunk.
fn plan(meta: &ObjectMeta, start: u64, end: u64) -> Vec<(u64, u64, usize, usize)> {
    let mut out = Vec::new();
    let mut off = 0u64;
    for s in &meta.segments {
        let (lo, hi) = (off, off + s.size); // [lo, hi)
        off = hi;
        if s.size == 0 || hi <= start || lo > end {
            continue;
        }
        let a = start.max(lo) - lo;
        let b = (end + 1).min(hi) - lo; // exclusive
        let mut c = a / CHUNK as u64;
        while c * (CHUNK as u64) < b {
            let cs = c * CHUNK as u64;
            let from = a.max(cs) - cs;
            let to = b.min(cs + CHUNK as u64) - cs;
            out.push((s.body, c, from as usize, to as usize));
            c += 1;
        }
    }
    out
}

/// Bytes `start..=end` of `meta`, as a streamed body.
fn object_body(
    bk: &Arc<Bucket<Pool>>,
    meta: &ObjectMeta,
    hold: Reading<Pool>,
    start: u64,
    end: u64,
) -> Body {
    let steps = if meta.size == 0 {
        Vec::new()
    } else {
        plan(meta, start, end)
    };
    // Read on a task (the reads aren't Sync; the channel is), a few
    // chunks ahead.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(2);
    let bk = bk.clone();
    tokio::spawn(async move {
        let _hold = hold;
        for (body, chunk, from, to) in steps {
            let item = match bk.read_chunk(body, chunk).await {
                Ok(Some(c)) if c.len() >= to => Ok(Frame::data(Bytes::from(c).slice(from..to))),
                Ok(_) => Err(std::io::Error::other("an object's body is missing a chunk")),
                Err(e) => Err(std::io::Error::other(format!("{:?}", e))),
            };
            let failed = item.is_err();
            if tx.send(item).await.is_err() || failed {
                return;
            }
        }
    });
    let s = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) });
    http_body_util::StreamBody::new(s).boxed()
}

fn object_headers(resp: &mut Response<Body>, meta: &ObjectMeta, r: &Req, checksums: bool) {
    let h = resp.headers_mut();
    let mut set = |k: &str, v: &str| {
        if let (Ok(n), Ok(v)) = (
            hyper::header::HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            h.insert(n, v);
        }
    };
    set("etag", &format!("\"{}\"", meta.etag));
    set("last-modified", &http_date(meta.last_modified_ms));
    set("accept-ranges", "bytes");
    if let Some(ct) = &meta.content_type {
        set("content-type", ct);
    }
    for (k, v) in &meta.headers {
        if k.starts_with("x-amz-checksum-") && !checksums {
            continue;
        }
        set(k, v);
    }
    for (k, v) in &meta.user {
        set(&format!("x-amz-meta-{}", k), v);
    }
    if meta.segments.len() > 1 && r.has("partNumber") {
        set("x-amz-mp-parts-count", &meta.segments.len().to_string());
    }
    // Overrides asked for in the query (presigned downloads).
    for (q, header) in [
        ("response-content-type", "content-type"),
        ("response-content-language", "content-language"),
        ("response-expires", "expires"),
        ("response-cache-control", "cache-control"),
        ("response-content-disposition", "content-disposition"),
        ("response-content-encoding", "content-encoding"),
    ] {
        if let Some(v) = r.q(q) {
            set(header, v);
        }
    }
}

async fn get_object(bk: &Arc<Bucket<Pool>>, key: &str, r: &Req) -> Result<Response<Body>, Err3> {
    let (meta, hold) = bk.head_held(key).await?.ok_or(S3Error::NoSuchKey)?;
    if let Err(code) = conditions(r, "", &meta) {
        let mut resp = ok_empty(code);
        if code == StatusCode::NOT_MODIFIED {
            etag_header(&mut resp, &meta.etag);
        } else {
            return Err(e3(
                code,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
        return Ok(resp);
    }
    let size = meta.size;
    let mut range = None;
    if let Some(p) = r.q("partNumber") {
        let n: usize = p.parse().map_err(|_| {
            e3(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Part number must be an integer",
            )
        })?;
        if n == 0 || n > meta.segments.len().max(1) {
            return Err(e3(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "InvalidPartNumber",
                "The requested partnumber is not satisfiable",
            ));
        }
        let start: u64 = meta.segments[..n - 1].iter().map(|s| s.size).sum();
        let len = meta.segments.get(n - 1).map_or(0, |s| s.size);
        if len > 0 {
            range = Some(start..start + len);
        }
    } else {
        match fhttp::parse_range(r.h("range"), size) {
            fhttp::ByteRange::Part(rg) => range = Some(rg),
            fhttp::ByteRange::Unsatisfiable => {
                return Err(e3(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "The requested range is not satisfiable",
                ));
            }
            fhttp::ByteRange::All => {}
        }
    }
    let status = match range {
        Some(_) => StatusCode::PARTIAL_CONTENT,
        None => StatusCode::OK,
    };
    let span = range.clone().unwrap_or(0..size);
    // `end` is the last byte (HTTP's ranges are inclusive).
    let (start, end, len) = (
        span.start,
        span.end.saturating_sub(1),
        span.end - span.start,
    );
    let body = if r.method == Method::HEAD || len == 0 {
        empty()
    } else {
        object_body(bk, &meta, hold, start, end)
    };
    let mut resp = fhttp::status(status, body);
    let checksums = range.is_none()
        && r.h("x-amz-checksum-mode")
            .is_some_and(|m| m.eq_ignore_ascii_case("enabled"));
    object_headers(&mut resp, &meta, r, checksums);
    let h = resp.headers_mut();
    h.insert("content-length", HeaderValue::from(len));
    if status == StatusCode::PARTIAL_CONTENT
        && let Ok(v) = HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, size))
    {
        h.insert("content-range", v);
    }
    Ok(resp)
}

// ---- copies ----------------------------------------------------------------

/// The source of a copy: its metadata and (a range of) its bytes.
async fn open_source(
    fe: &Arc<Frontend>,
    r: &Req,
    range: Option<std::ops::Range<u64>>,
) -> Result<(String, String, ObjectMeta, Body), Err3> {
    let src = r.h("x-amz-copy-source").unwrap_or("");
    let src = src.split('?').next().unwrap_or("");
    let src = String::from_utf8(sigv4::uri_decode(src.trim_start_matches('/'))).map_err(|_| {
        e3(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Bad x-amz-copy-source",
        )
    })?;
    let (sb, sk) = src
        .split_once('/')
        .filter(|(b, k)| !b.is_empty() && !k.is_empty())
        .ok_or_else(|| {
            e3(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Copy Source must mention the source bucket and key: sourcebucket/sourcekey",
            )
        })?;
    let (sb, sk) = (sb.to_string(), sk.to_string());
    if let Some(l) = fe.local(&sb)
        && let Local::S3(b) = &*l
    {
        let (meta, hold) = b.head_held(&sk).await?.ok_or(S3Error::NoSuchKey)?;
        if let Err(code) = conditions(r, "x-amz-copy-source-", &meta) {
            return Err(e3(
                code,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
        let (a, z) = match range {
            Some(rg) if meta.size == 0 || rg.end > meta.size => {
                return Err(e3(
                    StatusCode::BAD_REQUEST,
                    "InvalidRange",
                    "The specified copy source range is not satisfiable",
                ));
            }
            Some(rg) => (rg.start, rg.end - 1),
            None => (0, meta.size.saturating_sub(1)),
        };
        let body = if meta.size == 0 {
            empty()
        } else {
            object_body(b, &meta, hold, a, z)
        };
        return Ok((sb, sk, meta, body));
    }
    // Another node's: ask it, signing as one of ours.
    let route = fe
        .route(&sb)
        .filter(|r| r.format == DatasetFormat::S3)
        .ok_or_else(no_bucket)?;
    let owner = route.owner.ok_or_else(unavailable)?;
    let (ak, secret) = fe.auth().internal()?;
    let port = fe.config().s3_port().ok_or_else(unavailable)?;
    let path = format!(
        "/{}/{}",
        sigv4::uri_encode(sb.as_bytes(), true),
        sigv4::uri_encode(sk.as_bytes(), false)
    );
    let uri: hyper::Uri = path.parse().map_err(|_| no_bucket())?;
    let mut headers = HeaderMap::new();
    let host = format!("{}:{}", owner.addr, port);
    headers.insert(
        "host",
        HeaderValue::from_str(&host).map_err(|_| unavailable())?,
    );
    if let Some(rg) = range {
        headers.insert(
            "range",
            HeaderValue::from_str(&format!("bytes={}-{}", rg.start, rg.end - 1)).unwrap(),
        );
    }
    for n in [
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
    ] {
        if let Some(v) = r.h(&format!("x-amz-copy-source-{}", n))
            && let Ok(v) = HeaderValue::from_str(v)
        {
            headers.insert(n, v);
        }
    }
    let full_uri: hyper::Uri = format!("https://{}{}", host, path)
        .parse()
        .map_err(|_| unavailable())?;
    for (k, v) in sigv4::sign(
        &ak,
        &secret,
        &fe.config().region(),
        "s3",
        &Method::GET,
        &full_uri,
        &headers,
        b"",
    ) {
        headers.insert(
            hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(&v).unwrap(),
        );
    }
    let mut req = Request::new(http_body_util::Empty::<Bytes>::new());
    *req.method_mut() = Method::GET;
    *req.uri_mut() = uri;
    *req.headers_mut() = headers;
    let resp = fhttp::proxy(fe.tls(), req, &owner.addr, port)
        .await
        .ok_or_else(unavailable)?;
    match resp.status() {
        StatusCode::OK | StatusCode::PARTIAL_CONTENT => {}
        StatusCode::NOT_FOUND => return Err(S3Error::NoSuchKey.into()),
        StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED => {
            return Err(e3(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
        StatusCode::RANGE_NOT_SATISFIABLE => {
            return Err(e3(
                StatusCode::BAD_REQUEST,
                "InvalidRange",
                "The specified copy source range is not satisfiable",
            ));
        }
        s => return Err(e3(s, "InternalError", "Reading the copy source failed")),
    }
    let h = resp.headers();
    let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
    let meta = ObjectMeta {
        size: get("content-range")
            .and_then(|c| c.rsplit('/').next().and_then(|s| s.parse().ok()))
            .or_else(|| get("content-length").and_then(|s| s.parse().ok()))
            .unwrap_or(0),
        etag: get("etag")
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        last_modified_ms: get("last-modified")
            .and_then(|d| parse_http_date(&d))
            .unwrap_or(0),
        content_type: get("content-type"),
        headers: KEPT_HEADERS
            .iter()
            .filter_map(|k| get(k).map(|v| (k.to_string(), v)))
            .collect(),
        user: h
            .iter()
            .filter_map(|(k, v)| {
                k.as_str().strip_prefix("x-amz-meta-").map(|n| {
                    (
                        n.to_string(),
                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                    )
                })
            })
            .collect(),
        segments: Vec::new(),
    };
    Ok((sb, sk, meta, resp.into_body()))
}

/// Copy `body` into a new body of `bk`.
async fn copy_into(
    bk: &Arc<Bucket<Pool>>,
    mut body: Body,
    expected: Option<u64>,
) -> Result<(Segment, [u8; 16]), Err3> {
    let mut w = bk.new_body(expected).await?;
    while let Some(f) = body.frame().await {
        let f = match f {
            Ok(f) => f,
            Err(e) => {
                w.abort().await;
                return Err(e3(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    format!("reading the copy source: {}", e),
                ));
            }
        };
        if let Ok(d) = f.into_data()
            && let Err(e) = w.write(&d).await
        {
            w.abort().await;
            return Err(e.into());
        }
    }
    Ok(w.finish().await?)
}

async fn copy_object(
    fe: &Arc<Frontend>,
    bk: &Arc<Bucket<Pool>>,
    key: &str,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    let replace = r
        .h("x-amz-metadata-directive")
        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"));
    let (sb, sk, src, body) = open_source(fe, r, None).await?;
    if sb == bk.tree.name && sk == key && !replace {
        return Err(e3(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
        ));
    }
    if src.size > MAX_PUT {
        return Err(e3(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "The specified copy source is larger than the maximum allowable size for a copy source: 5368709120",
        ));
    }
    let mut meta = if replace {
        meta_from(r)?
    } else {
        ObjectMeta {
            headers: src
                .headers
                .iter()
                .filter(|(k, _)| !k.starts_with("x-amz-checksum-"))
                .cloned()
                .collect(),
            ..src.clone()
        }
    };
    let (seg, md5) = copy_into(bk, body, Some(src.size)).await?;
    meta.size = seg.size;
    meta.etag = hex::encode(md5);
    meta.last_modified_ms = crate::cluster::now_ms();
    meta.segments = vec![seg];
    if let Err(e) = bk.put_object(key, meta.clone()).await {
        let _ = bk.discard(seg.body).await;
        return Err(e.into());
    }
    let mut x = Xml::new("CopyObjectResult");
    x.el("LastModified", &iso(meta.last_modified_ms))
        .el("ETag", &format!("\"{}\"", meta.etag));
    Ok(xml_response(StatusCode::OK, x.done("CopyObjectResult")))
}

// ---- multipart -------------------------------------------------------------

async fn create_upload(bk: &Arc<Bucket<Pool>>, key: &str, r: &Req) -> Result<Response<Body>, Err3> {
    let meta = meta_from(r)?;
    let id = bk.create_upload(key, meta).await?;
    let mut x = Xml::new("InitiateMultipartUploadResult");
    x.el("Bucket", &bk.tree.name)
        .el("Key", key)
        .el("UploadId", &id);
    Ok(xml_response(
        StatusCode::OK,
        x.done("InitiateMultipartUploadResult"),
    ))
}

fn part_number(r: &Req) -> Result<u32, Err3> {
    r.q("partNumber")
        .and_then(|p| p.parse().ok())
        .filter(|n| (1..=super::MAX_PARTS).contains(n))
        .ok_or_else(|| {
            e3(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Part number must be an integer between 1 and 10000, inclusive",
            )
        })
}

async fn upload_part(bk: &Arc<Bucket<Pool>>, r: &mut Req) -> Result<Response<Body>, Err3> {
    let n = part_number(r)?;
    let id = r.q("uploadId").unwrap_or("").to_string();
    bk.upload(&id).await?;
    let (seg, md5, sums) = receive(bk, r).await?;
    let part = Part {
        body: seg.body,
        size: seg.size,
        md5,
        last_modified_ms: crate::cluster::now_ms(),
    };
    bk.put_part(&id, n, part.clone()).await?;
    let mut resp = ok_empty(StatusCode::OK);
    etag_header(&mut resp, &part.etag());
    for (k, v) in sums {
        if let (Ok(n), Ok(v)) = (
            hyper::header::HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            resp.headers_mut().insert(n, v);
        }
    }
    Ok(resp)
}

async fn upload_part_copy(
    fe: &Arc<Frontend>,
    bk: &Arc<Bucket<Pool>>,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    let n = part_number(r)?;
    let id = r.q("uploadId").unwrap_or("").to_string();
    bk.upload(&id).await?;
    let range = match r.h("x-amz-copy-source-range") {
        Some(h) => {
            let spec = h.trim().strip_prefix("bytes=").unwrap_or("");
            let (a, b) = spec.split_once('-').unwrap_or(("", ""));
            match (a.parse::<u64>(), b.parse::<u64>()) {
                (Ok(a), Ok(b)) if a <= b && b < u64::MAX => Some(a..b + 1),
                _ => {
                    return Err(e3(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "The x-amz-copy-source-range value must be of the form bytes=first-last",
                    ));
                }
            }
        }
        None => None,
    };
    let wanted = range.as_ref().map(|rg| rg.end - rg.start);
    let (_, _, src, body) = open_source(fe, r, range).await?;
    let expected = wanted.unwrap_or(src.size);
    let (seg, md5) = copy_into(bk, body, Some(expected)).await?;
    let part = Part {
        body: seg.body,
        size: seg.size,
        md5,
        last_modified_ms: crate::cluster::now_ms(),
    };
    bk.put_part(&id, n, part.clone()).await?;
    let mut x = Xml::new("CopyPartResult");
    x.el("LastModified", &iso(part.last_modified_ms))
        .el("ETag", &format!("\"{}\"", part.etag()));
    Ok(xml_response(StatusCode::OK, x.done("CopyPartResult")))
}

async fn list_parts(bk: &Arc<Bucket<Pool>>, key: &str, r: &Req) -> Result<Response<Body>, Err3> {
    let id = r.q("uploadId").unwrap_or("");
    let up = bk.upload(id).await?;
    if up.key != key {
        return Err(S3Error::NoSuchUpload.into());
    }
    let max: usize = r
        .q("max-parts")
        .and_then(|m| m.parse().ok())
        .unwrap_or(1000usize)
        .min(1000);
    let marker: u32 = r
        .q("part-number-marker")
        .and_then(|m| m.parse().ok())
        .unwrap_or(0);
    let mut parts = bk.parts(id, marker, max + 1).await?;
    let truncated = parts.len() > max;
    parts.truncate(max);
    let mut x = Xml::new("ListPartsResult");
    x.el("Bucket", &bk.tree.name)
        .el("Key", key)
        .el("UploadId", id)
        .el("PartNumberMarker", &marker.to_string())
        .el("MaxParts", &max.to_string())
        .el("IsTruncated", if truncated { "true" } else { "false" })
        .el("StorageClass", "STANDARD");
    if let Some((n, _)) = parts.last() {
        x.el("NextPartNumberMarker", &n.to_string());
    }
    for (n, p) in &parts {
        x.open("Part")
            .el("PartNumber", &n.to_string())
            .el("LastModified", &iso(p.last_modified_ms))
            .el("ETag", &format!("\"{}\"", p.etag()))
            .el("Size", &p.size.to_string())
            .close("Part");
    }
    Ok(xml_response(StatusCode::OK, x.done("ListPartsResult")))
}

async fn complete_upload(
    fe: &Arc<Frontend>,
    bk: &Arc<Bucket<Pool>>,
    key: &str,
    r: &mut Req,
) -> Result<Response<Body>, Err3> {
    let id = r.q("uploadId").unwrap_or("").to_string();
    let up = bk.upload(&id).await?;
    if up.key != key {
        return Err(S3Error::NoSuchUpload.into());
    }
    let body = r.small_body(4 << 20).await?;
    let x = parse_xml(&body)?;
    let mut parts = Vec::new();
    for p in x.find_all("Part") {
        let n: u32 = p
            .text_of("PartNumber")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| {
                e3(
                    StatusCode::BAD_REQUEST,
                    "MalformedXML",
                    "A part has no PartNumber",
                )
            })?;
        parts.push((n, p.text_of("ETag").unwrap_or_default()));
    }
    let (key, meta) = bk.complete(&id, parts).await?;
    let host = r.h("host").unwrap_or(&fe.addr()).to_string();
    let mut out = Xml::new("CompleteMultipartUploadResult");
    out.el(
        "Location",
        &format!(
            "https://{}/{}/{}",
            host,
            bk.tree.name,
            sigv4::uri_encode(key.as_bytes(), false)
        ),
    )
    .el("Bucket", &bk.tree.name)
    .el("Key", &key)
    .el("ETag", &format!("\"{}\"", meta.etag));
    Ok(xml_response(
        StatusCode::OK,
        out.done("CompleteMultipartUploadResult"),
    ))
}

#[cfg(test)]
#[path = "../tests/s3_http_tests.rs"]
mod tests;
