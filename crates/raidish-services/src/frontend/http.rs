//! What the S3 and SQS endpoints share: the HTTP server, proxying a
//! request to the node that runs its dataset, and reading request bodies
//! as their signatures describe them (plain or `aws-chunked`, checked
//! against their hashes, chunk signatures and checksums).

use super::FORWARDED_HEADER;
use crate::sigv4::{ChunkSigner, Payload};
use bytes::{Buf, Bytes, BytesMut};
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::{Body as _, Incoming};
use hyper::{Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use std::future::Future;
use tokio::net::TcpListener;

pub type Body = BoxBody<Bytes, std::io::Error>;

pub fn full(b: impl Into<Bytes>) -> Body {
    Full::new(b.into()).map_err(|e| match e {}).boxed()
}

pub fn empty() -> Body {
    Empty::new().map_err(|e| match e {}).boxed()
}

/// Serve HTTPS (HTTP/1.1 and HTTP/2) on `l`, each request through
/// `handle`.
pub async fn serve<F, Fut>(l: TcpListener, tls: tokio_rustls::TlsAcceptor, handle: F)
where
    F: Fn(Request<Incoming>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    crate::net::accept_loop(l, "https", move |sock, _| {
        let handle = handle.clone();
        let tls = tls.clone();
        async move {
            let Ok(sock) = crate::net::tls_accept(&tls, sock).await else {
                return;
            };
            let svc = hyper::service::service_fn(move |req| {
                let handle = handle.clone();
                async move { Ok::<_, std::convert::Infallible>(handle(req).await) }
            });
            let _ =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(hyper_util::rt::TokioIo::new(sock), svc)
                    .await;
        }
    })
    .await
}

/// Whether another node already proxied this request here.
pub fn forwarded<B>(req: &Request<B>) -> bool {
    forwarded_parts(req.headers())
}

pub fn forwarded_parts(headers: &hyper::HeaderMap) -> bool {
    headers.contains_key(FORWARDED_HEADER)
}

/// Send `req` on to `addr:port` (as it is, marked forwarded; over TLS, as
/// this node) and return its answer. None: it couldn't be reached.
pub async fn proxy<B>(
    tls: &super::ServiceTls,
    mut req: Request<B>,
    addr: &str,
    port: u16,
) -> Option<Response<Body>>
where
    B: hyper::body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let sock = tls.connect(addr, port).await.ok()?;
    let (mut send, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(sock))
            .await
            .ok()?;
    tokio::spawn(conn);
    req.headers_mut().insert(
        FORWARDED_HEADER,
        hyper::header::HeaderValue::from_static("1"),
    );
    // The request line carries just the path (the Host header is kept).
    let pq = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".into());
    *req.uri_mut() = pq.parse().ok()?;
    *req.version_mut() = hyper::Version::HTTP_11;
    if !req.headers().contains_key(hyper::header::HOST)
        && let Ok(v) = format!("{}:{}", addr, port).parse()
    {
        req.headers_mut().insert(hyper::header::HOST, v);
    }
    let resp = send.send_request(req).await.ok()?;
    Some(resp.map(|b| b.map_err(std::io::Error::other).boxed()))
}

/// The whole of a (small) body, up to `max` bytes.
pub async fn collect(body: Incoming, max: usize) -> Result<Bytes, BodyError> {
    if body.size_hint().lower() as usize > max {
        return Err(BodyError::TooLarge);
    }
    let mut out = BytesMut::new();
    let mut body = body;
    while let Some(f) = body.frame().await {
        let f = f.map_err(|e| BodyError::Io(e.to_string()))?;
        if let Some(d) = f.data_ref() {
            out.extend_from_slice(d);
            if out.len() > max {
                return Err(BodyError::TooLarge);
            }
        }
    }
    Ok(out.freeze())
}

/// Why a body was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    /// Its SHA-256 isn't the signed one.
    Sha256Mismatch,
    /// A chunk's (or the trailer's) signature is wrong.
    BadChunkSignature,
    /// A checksum doesn't match (the header).
    BadChecksum(String),
    /// Not well-formed `aws-chunked`.
    Malformed(String),
    TooLarge,
    /// Shorter or longer than it said.
    BadLength,
    Io(String),
}

/// Reads a request body the way its signature says it was sent.
pub struct PayloadReader {
    body: Incoming,
    buf: BytesMut,
    eof: bool,
    done: bool,
    payload: Payload,
    signer: ChunkSigner,
    sha: Option<Sha256>,
    /// Checksums to check: (header, expected or None if it comes in the
    /// trailer, running hash).
    checks: Vec<(String, Option<String>, crate::s3::checksum::Hasher)>,
    /// The body's length as decoded, if known in advance.
    pub expected: Option<u64>,
    read: u64,
    /// The checksums the trailer gave (header, value).
    pub trailer: Vec<(String, String)>,
    /// The checksums checked (header, value), once read to the end.
    pub verified: Vec<(String, String)>,
}

impl PayloadReader {
    /// `headers`: the request's (checksum headers, `x-amz-trailer`, and
    /// the lengths).
    pub fn new(
        body: Incoming,
        payload: Payload,
        signer: ChunkSigner,
        headers: &hyper::HeaderMap,
    ) -> Self {
        let h = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
        let chunked = matches!(payload, Payload::Chunked { .. } | Payload::ChunkedUnsigned);
        let expected = if chunked {
            h("x-amz-decoded-content-length").and_then(|v| v.parse().ok())
        } else {
            h("content-length").and_then(|v| v.parse().ok())
        };
        let mut checks = Vec::new();
        for (name, value) in headers {
            let n = name.as_str();
            if let Some(hs) = crate::s3::checksum::Hasher::for_header(n)
                && let Ok(v) = value.to_str()
            {
                checks.push((n.to_string(), Some(v.to_string()), hs));
            }
        }
        if let Some(t) = h("x-amz-trailer") {
            for n in t.split(',').map(|s| s.trim().to_ascii_lowercase()) {
                if let Some(hs) = crate::s3::checksum::Hasher::for_header(&n)
                    && !checks.iter().any(|c| c.0 == n)
                {
                    checks.push((n, None, hs));
                }
            }
        }
        PayloadReader {
            body,
            buf: BytesMut::new(),
            eof: false,
            done: false,
            sha: matches!(payload, Payload::Sha256(_)).then(Sha256::new),
            payload,
            signer,
            checks,
            expected,
            read: 0,
            trailer: Vec::new(),
            verified: Vec::new(),
        }
    }

    /// Read more of the raw body into the buffer; false at its end.
    async fn fill(&mut self) -> Result<bool, BodyError> {
        if self.eof {
            return Ok(false);
        }
        loop {
            match self.body.frame().await {
                None => {
                    self.eof = true;
                    return Ok(false);
                }
                Some(Err(e)) => return Err(BodyError::Io(e.to_string())),
                Some(Ok(f)) => {
                    if let Ok(d) = f.into_data() {
                        if d.is_empty() {
                            continue;
                        }
                        self.buf.extend_from_slice(&d);
                        return Ok(true);
                    }
                }
            }
        }
    }

    async fn line(&mut self) -> Result<Vec<u8>, BodyError> {
        loop {
            if let Some(i) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let l = self.buf.split_to(i).to_vec();
                self.buf.advance(2);
                return Ok(l);
            }
            if self.buf.len() > 64 * 1024 {
                return Err(BodyError::Malformed("chunk header too long".into()));
            }
            if !self.fill().await? {
                return Err(BodyError::Malformed("truncated chunk header".into()));
            }
        }
    }

    async fn exactly(&mut self, n: usize) -> Result<Bytes, BodyError> {
        while self.buf.len() < n {
            if !self.fill().await? {
                return Err(BodyError::Malformed("truncated chunk".into()));
            }
        }
        Ok(self.buf.split_to(n).freeze())
    }

    /// The next piece of the decoded body (None: the end, everything
    /// checked).
    pub async fn next(&mut self) -> Result<Option<Bytes>, BodyError> {
        if self.done {
            return Ok(None);
        }
        let data = match self.payload.clone() {
            Payload::Unsigned | Payload::Sha256(_) => {
                if self.buf.is_empty() && !self.fill().await? {
                    None
                } else {
                    Some(self.buf.split().freeze())
                }
            }
            Payload::Chunked { .. } | Payload::ChunkedUnsigned => {
                let header = self.line().await?;
                let header = String::from_utf8_lossy(&header).into_owned();
                let (size, sig) = match header.split_once(';') {
                    Some((s, rest)) => (s, rest.trim().strip_prefix("chunk-signature=")),
                    None => (header.as_str(), None),
                };
                let size = usize::from_str_radix(size.trim(), 16)
                    .map_err(|_| BodyError::Malformed(format!("bad chunk size '{}'", size)))?;
                if size > 64 << 20 {
                    return Err(BodyError::Malformed("chunk too large".into()));
                }
                let data = self.exactly(size).await?;
                if let Payload::Chunked { .. } = self.payload {
                    let sig = sig.ok_or(BodyError::BadChunkSignature)?;
                    if !self.signer.chunk(sig, &Sha256::digest(&data)) {
                        return Err(BodyError::BadChunkSignature);
                    }
                }
                if size == 0 {
                    self.trailer_lines().await?;
                    None
                } else {
                    let crlf = self.exactly(2).await?;
                    if &crlf[..] != b"\r\n" {
                        return Err(BodyError::Malformed("chunk not followed by CRLF".into()));
                    }
                    Some(data)
                }
            }
        };
        match data {
            Some(d) => {
                self.read += d.len() as u64;
                if self.expected.is_some_and(|e| self.read > e) {
                    return Err(BodyError::BadLength);
                }
                if let Some(s) = &mut self.sha {
                    s.update(&d);
                }
                for c in &mut self.checks {
                    c.2.update(&d);
                }
                Ok(Some(d))
            }
            None => {
                self.done = true;
                self.finish()?;
                Ok(None)
            }
        }
    }

    /// What follows the last chunk: trailer headers (maybe signed), then
    /// an empty line.
    async fn trailer_lines(&mut self) -> Result<(), BodyError> {
        let signed_trailer = matches!(self.payload, Payload::Chunked { trailer: true });
        let mut lines = Vec::new();
        let mut sig = None;
        loop {
            let l = match self.line().await {
                Ok(l) => l,
                // Some clients end without the final empty line.
                Err(_) if self.eof && self.buf.is_empty() => break,
                Err(e) => return Err(e),
            };
            if l.is_empty() {
                break;
            }
            let l = String::from_utf8_lossy(&l).into_owned();
            let Some((k, v)) = l.split_once(':') else {
                return Err(BodyError::Malformed(format!("bad trailer line '{}'", l)));
            };
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
            if k == "x-amz-trailer-signature" {
                sig = Some(v);
            } else {
                lines.push((k, v));
            }
        }
        if signed_trailer {
            let mut canon = Vec::new();
            for (k, v) in &lines {
                canon.extend_from_slice(format!("{}:{}\n", k, v).as_bytes());
            }
            let sig = sig.ok_or(BodyError::BadChunkSignature)?;
            if !self.signer.trailer(&sig, &canon) {
                return Err(BodyError::BadChunkSignature);
            }
        }
        for (k, v) in &lines {
            if let Some(c) = self.checks.iter_mut().find(|c| &c.0 == k) {
                c.1 = Some(v.clone());
            }
        }
        self.trailer = lines;
        Ok(())
    }

    /// Everything read: check it all.
    fn finish(&mut self) -> Result<(), BodyError> {
        if self.expected.is_some_and(|e| self.read != e) {
            return Err(BodyError::BadLength);
        }
        if let (Some(s), Payload::Sha256(want)) = (self.sha.take(), &self.payload)
            && hex::encode(s.finalize()) != *want
        {
            return Err(BodyError::Sha256Mismatch);
        }
        for (name, want, h) in std::mem::take(&mut self.checks) {
            let got = h.finish();
            match want {
                Some(w) if w != got => return Err(BodyError::BadChecksum(name)),
                Some(_) => self.verified.push((name, got)),
                None => {}
            }
        }
        Ok(())
    }
}

/// A plain response.
pub fn status(code: StatusCode, body: Body) -> Response<Body> {
    let mut r = Response::new(body);
    *r.status_mut() = code;
    r
}

/// What a `Range` header asks of a `size`-byte file or object.
#[derive(Debug, PartialEq, Eq)]
pub enum ByteRange {
    /// No (usable) range: the whole thing.
    All,
    Part(std::ops::Range<u64>),
    /// Nothing of it is in it (`416`).
    Unsatisfiable,
}

/// Parse `bytes=a-b`, `bytes=a-` or `bytes=-n`. Anything else (another
/// unit, several ranges, garbage) is ignored, as RFC 9110 allows.
pub fn parse_range(header: Option<&str>, size: u64) -> ByteRange {
    let Some(spec) = header.and_then(|h| h.trim().strip_prefix("bytes=")) else {
        return ByteRange::All;
    };
    if spec.contains(',') {
        return ByteRange::All;
    }
    let Some((a, b)) = spec.trim().split_once('-') else {
        return ByteRange::All;
    };
    let (a, b) = (a.trim(), b.trim());
    let range = match (a.parse::<u64>(), b.parse::<u64>()) {
        (Ok(first), Ok(last)) if first <= last => first..last.saturating_add(1).min(size),
        (Ok(first), Err(_)) if b.is_empty() => first..size,
        (Err(_), Ok(n)) if a.is_empty() => size.saturating_sub(n)..size,
        _ => return ByteRange::All,
    };
    if range.start >= size || range.is_empty() {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Part(range)
}
