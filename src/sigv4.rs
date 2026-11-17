//! AWS Signature Version 4: checking the requests S3 and SQS clients sign
//! (in the `Authorization` header, or presigned in the query string), the
//! chunk signatures of `aws-chunked` uploads, and signing requests of our
//! own (one node asking another, see `crate::sqs`).

use hmac::{Hmac, Mac};
use hyper::http::{HeaderMap, Method, Uri};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const UNSIGNED: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
/// The hash of nothing.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// How far a request's time may be from ours.
const MAX_SKEW_SECS: i64 = 15 * 60;
/// The longest a presigned URL may be good for.
const MAX_EXPIRES_SECS: i64 = 7 * 24 * 3600;

/// Why a request isn't accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Not signed at all.
    Missing,
    /// Signed with an unknown key.
    UnknownKey,
    /// Not well formed (what's wrong).
    Malformed(String),
    /// The signature doesn't match.
    Mismatch,
    /// Its time is too far from ours.
    Skewed,
    /// A presigned URL past its time.
    Expired,
    /// An access key past its expiry (its token's).
    KeyExpired,
}

impl AuthError {
    /// S3's error code for it.
    pub fn s3_code(&self) -> &'static str {
        match self {
            AuthError::Missing => "AccessDenied",
            AuthError::UnknownKey => "InvalidAccessKeyId",
            AuthError::Malformed(_) => "AuthorizationHeaderMalformed",
            AuthError::Mismatch => "SignatureDoesNotMatch",
            AuthError::Skewed => "RequestTimeTooSkewed",
            AuthError::Expired => "AccessDenied",
            AuthError::KeyExpired => "ExpiredToken",
        }
    }

    pub fn message(&self) -> String {
        match self {
            AuthError::Missing => "Requests must be signed (AWS Signature Version 4)".into(),
            AuthError::UnknownKey => "The access key ID does not exist".into(),
            AuthError::Malformed(m) => m.clone(),
            AuthError::Mismatch => {
                "The request signature we calculated does not match the signature you provided"
                    .into()
            }
            AuthError::Skewed => {
                "The difference between the request time and the server's time is too large".into()
            }
            AuthError::Expired => "Request has expired".into(),
            AuthError::KeyExpired => "The provided token has expired.".into(),
        }
    }
}

/// Where verification finds secrets.
pub trait Keys {
    /// The region requests must be signed for.
    fn region(&self) -> String;
    /// The secret of access key `id`.
    fn secret(&self, id: &str) -> Result<String, AuthError>;
}

/// A fixed set of keys (`(id, secret)`), as tests and tools use.
pub struct StaticKeys {
    pub region: String,
    pub keys: Vec<(String, String)>,
}

impl Keys for StaticKeys {
    fn region(&self) -> String {
        self.region.clone()
    }
    fn secret(&self, id: &str) -> Result<String, AuthError> {
        self.keys
            .iter()
            .find(|(k, _)| k == id)
            .map(|(_, s)| s.clone())
            .ok_or(AuthError::UnknownKey)
    }
}

/// What the request's signature says about its body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Not covered.
    Unsigned,
    /// Its SHA-256 (hex): check it once read.
    Sha256(String),
    /// `aws-chunked`, each chunk signed (and maybe signed trailers).
    Chunked { trailer: bool },
    /// `aws-chunked`, unsigned, with trailers.
    ChunkedUnsigned,
}

/// A request whose signature checked out.
#[derive(Debug, Clone)]
pub struct Signed {
    pub access_key: String,
    pub payload: Payload,
    /// For the chunk signatures that follow (`aws-chunked`).
    pub chunks: ChunkSigner,
}

/// Checks `aws-chunked` signatures: each chunk's chains on the one before,
/// starting from the request's.
#[derive(Debug, Clone)]
pub struct ChunkSigner {
    key: Vec<u8>,
    amz_date: String,
    scope: String,
    prev: String,
}

impl ChunkSigner {
    /// Whether `sig` signs a chunk whose bytes hash to `hash`.
    pub fn chunk(&mut self, sig: &str, hash: &[u8]) -> bool {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.prev,
            EMPTY_SHA256,
            hex::encode(hash)
        );
        self.check(sig, &sts)
    }

    /// Whether `sig` signs the trailer `trailer` (its header lines, each
    /// "name:value\n").
    pub fn trailer(&mut self, sig: &str, trailer: &[u8]) -> bool {
        let sts = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.prev,
            hex::encode(Sha256::digest(trailer))
        );
        self.check(sig, &sts)
    }

    fn check(&mut self, sig: &str, sts: &str) -> bool {
        let want = hex::encode(hmac(&self.key, sts.as_bytes()));
        let ok = constant_eq(want.as_bytes(), sig.as_bytes());
        self.prev = want;
        ok
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The key a day's signatures are made with.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k = hmac(format!("AWS4{}", secret).as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, service.as_bytes());
    hmac(&k, b"aws4_request")
}

/// AWS's URI encoding: everything but unreserved characters (and `/`
/// unless `slash`).
pub fn uri_encode(s: &[u8], slash: bool) -> String {
    let mut out = String::with_capacity(s.len() * 3 / 2);
    for &b in s {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || (b == b'/' && !slash)
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Percent-decode (`+` stays `+`).
pub fn uri_decode(s: &str) -> Vec<u8> {
    percent_encoding::percent_decode_str(s).collect()
}

/// The query string's parameters, decoded, in order.
pub fn query_pairs(q: Option<&str>) -> Vec<(String, String)> {
    let Some(q) = q else { return Vec::new() };
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (
                String::from_utf8_lossy(&uri_decode(k)).into_owned(),
                String::from_utf8_lossy(&uri_decode(v)).into_owned(),
            )
        })
        .collect()
}

fn canonical_query(pairs: &[(String, String)], skip: Option<&str>) -> String {
    let mut enc: Vec<(String, String)> = pairs
        .iter()
        .filter(|(k, _)| Some(k.as_str()) != skip)
        .map(|(k, v)| {
            (
                uri_encode(k.as_bytes(), true),
                uri_encode(v.as_bytes(), true),
            )
        })
        .collect();
    enc.sort();
    enc.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// A header's value as signed: every value of it, trimmed, inner runs of
/// spaces as one, comma-joined.
fn header_value(headers: &HeaderMap, uri: &Uri, name: &str) -> Option<String> {
    let vals: Vec<String> = headers
        .get_all(name)
        .iter()
        .map(|v| {
            String::from_utf8_lossy(v.as_bytes())
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    if vals.is_empty() {
        // HTTP/2 has no Host header: its authority.
        if name == "host" {
            return uri.authority().map(|a| a.as_str().to_string());
        }
        return None;
    }
    Some(vals.join(","))
}

#[allow(clippy::too_many_arguments)]
fn canonical_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    query: &[(String, String)],
    skip_query: Option<&str>,
    signed: &[String],
    payload: &str,
) -> Result<String, AuthError> {
    let path = uri_encode(&uri_decode(uri.path()), false);
    let mut canon_headers = String::new();
    for h in signed {
        let v = header_value(headers, uri, h)
            .ok_or_else(|| AuthError::Malformed(format!("signed header '{}' is missing", h)))?;
        canon_headers.push_str(h);
        canon_headers.push(':');
        canon_headers.push_str(&v);
        canon_headers.push('\n');
    }
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        if path.is_empty() { "/".into() } else { path },
        canonical_query(query, skip_query),
        canon_headers,
        signed.join(";"),
        payload
    ))
}

fn parse_amz_date(d: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::NaiveDateTime::parse_from_str(d, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|t| t.and_utc())
}

/// Check a request's signature (for `service`: "s3" or "sqs").
/// `body_sha256`: the body's hash, for services whose clients don't send
/// `x-amz-content-sha256` (SQS); S3 requests carry it.
pub fn verify(
    keys: &dyn Keys,
    service: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body_sha256: Option<&str>,
) -> Result<Signed, AuthError> {
    verify_at(
        keys,
        service,
        method,
        uri,
        headers,
        body_sha256,
        chrono::Utc::now(),
    )
}

/// `verify`, as of `now`.
pub fn verify_at(
    keys: &dyn Keys,
    service: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body_sha256: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Signed, AuthError> {
    let query = query_pairs(uri.query());
    let presigned = query.iter().any(|(k, _)| k == "X-Amz-Signature");
    let q = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let (credential, signed_headers, signature, amz_date) = if presigned {
        if q("X-Amz-Algorithm").as_deref() != Some(ALGORITHM) {
            return Err(AuthError::Malformed("unsupported X-Amz-Algorithm".into()));
        }
        (
            q("X-Amz-Credential").ok_or(AuthError::Malformed("no X-Amz-Credential".into()))?,
            q("X-Amz-SignedHeaders")
                .ok_or(AuthError::Malformed("no X-Amz-SignedHeaders".into()))?,
            q("X-Amz-Signature").unwrap_or_default(),
            q("X-Amz-Date").ok_or(AuthError::Malformed("no X-Amz-Date".into()))?,
        )
    } else {
        let Some(auth) = headers.get("authorization") else {
            return Err(AuthError::Missing);
        };
        let auth = auth
            .to_str()
            .map_err(|_| AuthError::Malformed("bad Authorization header".into()))?;
        let rest = auth
            .strip_prefix(ALGORITHM)
            .ok_or_else(|| AuthError::Malformed("only AWS4-HMAC-SHA256 is supported".into()))?;
        let mut cred = None;
        let mut sh = None;
        let mut sig = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                cred = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                sh = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("Signature=") {
                sig = Some(v.to_string());
            }
        }
        let date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| AuthError::Malformed("no X-Amz-Date header".into()))?;
        (
            cred.ok_or_else(|| AuthError::Malformed("no Credential".into()))?,
            sh.ok_or_else(|| AuthError::Malformed("no SignedHeaders".into()))?,
            sig.ok_or_else(|| AuthError::Malformed("no Signature".into()))?,
            date,
        )
    };
    // Credential: key/date/region/service/aws4_request.
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(AuthError::Malformed(format!(
            "bad credential scope '{}'",
            credential
        )));
    }
    let (key_id, date, region, svc) = (parts[0], parts[1], parts[2], parts[3]);
    let region_ok = region == keys.region();
    if !region_ok || svc != service {
        return Err(AuthError::Malformed(format!(
            "the credential scope is for {} in {}; this is {} in {}",
            svc,
            region,
            service,
            keys.region()
        )));
    }
    let secret = keys.secret(key_id)?;
    let when = parse_amz_date(&amz_date).ok_or(AuthError::Malformed("bad X-Amz-Date".into()))?;
    if !amz_date.starts_with(date) {
        return Err(AuthError::Malformed(
            "the credential's date isn't the request's".into(),
        ));
    }
    if presigned {
        let expires: i64 = q("X-Amz-Expires")
            .and_then(|e| e.parse().ok())
            .ok_or(AuthError::Malformed("bad X-Amz-Expires".into()))?;
        if !(1..=MAX_EXPIRES_SECS).contains(&expires) {
            return Err(AuthError::Malformed("X-Amz-Expires out of range".into()));
        }
        if (when - now).num_seconds() > MAX_SKEW_SECS {
            return Err(AuthError::Skewed);
        }
        if (now - when).num_seconds() > expires {
            return Err(AuthError::Expired);
        }
    } else if (now - when).num_seconds().abs() > MAX_SKEW_SECS {
        return Err(AuthError::Skewed);
    }
    let signed: Vec<String> = signed_headers
        .split(';')
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect();
    if !signed.iter().any(|h| h == "host") {
        return Err(AuthError::Malformed(
            "the Host header must be signed".into(),
        ));
    }
    let content = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok());
    let payload_str = match (presigned, content, body_sha256) {
        (true, None, _) => UNSIGNED.to_string(),
        (_, Some(c), _) => c.to_string(),
        (false, None, Some(h)) => h.to_string(),
        (false, None, None) => {
            return Err(AuthError::Malformed(
                "x-amz-content-sha256 is required".into(),
            ));
        }
    };
    let payload = parse_payload(&payload_str)?;
    let creq = canonical_request(
        method,
        uri,
        headers,
        &query,
        presigned.then_some("X-Amz-Signature"),
        &signed,
        &payload_str,
    )?;
    let scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let sts = format!(
        "{}\n{}\n{}\n{}",
        ALGORITHM,
        amz_date,
        scope,
        hex::encode(Sha256::digest(creq.as_bytes()))
    );
    let key = signing_key(&secret, date, region, service);
    let want = hex::encode(hmac(&key, sts.as_bytes()));
    if !constant_eq(want.as_bytes(), signature.as_bytes()) {
        return Err(AuthError::Mismatch);
    }
    Ok(Signed {
        access_key: key_id.to_string(),
        payload,
        chunks: ChunkSigner {
            key,
            amz_date,
            scope,
            prev: want,
        },
    })
}

fn parse_payload(p: &str) -> Result<Payload, AuthError> {
    Ok(match p {
        UNSIGNED => Payload::Unsigned,
        STREAMING => Payload::Chunked { trailer: false },
        STREAMING_TRAILER => Payload::Chunked { trailer: true },
        STREAMING_UNSIGNED_TRAILER => Payload::ChunkedUnsigned,
        h if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) => {
            Payload::Sha256(h.to_ascii_lowercase())
        }
        other => {
            return Err(AuthError::Malformed(format!(
                "unsupported x-amz-content-sha256 '{}'",
                other
            )));
        }
    })
}

/// Sign a request of our own: the headers to add (`x-amz-date`,
/// `x-amz-content-sha256`, `authorization`). `headers` must hold `host`
/// and whatever else is to be signed.
#[allow(clippy::too_many_arguments)]
pub fn sign(
    access_key: &str,
    secret: &str,
    region: &str,
    service: &str,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Vec<(String, String)> {
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload = hex::encode(Sha256::digest(body));
    let mut h = headers.clone();
    h.insert("x-amz-date", amz_date.parse().unwrap());
    h.insert("x-amz-content-sha256", payload.parse().unwrap());
    let mut signed: Vec<String> = h.keys().map(|k| k.as_str().to_ascii_lowercase()).collect();
    signed.sort();
    signed.dedup();
    let query = query_pairs(uri.query());
    let creq = canonical_request(method, uri, &h, &query, None, &signed, &payload)
        .expect("our own headers are there");
    let scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let sts = format!(
        "{}\n{}\n{}\n{}",
        ALGORITHM,
        amz_date,
        scope,
        hex::encode(Sha256::digest(creq.as_bytes()))
    );
    let key = signing_key(secret, &date, region, service);
    let sig = hex::encode(hmac(&key, sts.as_bytes()));
    vec![
        ("x-amz-date".into(), amz_date),
        ("x-amz-content-sha256".into(), payload),
        (
            "authorization".into(),
            format!(
                "{} Credential={}/{}, SignedHeaders={}, Signature={}",
                ALGORITHM,
                access_key,
                scope,
                signed.join(";"),
                sig
            ),
        ),
    ]
}

#[cfg(test)]
#[path = "tests/sigv4_tests.rs"]
mod tests;
