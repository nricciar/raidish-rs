//! The SQS API, AWS JSON 1.0 protocol (`X-Amz-Target: AmazonSQS.<op>`, as
//! current AWS SDKs and the CLI speak it): queues (create, delete, list,
//! URLs, attributes, tags, purge, dead-letter sources), messages (send,
//! receive with long polling, delete, change visibility, all in batches
//! too). Dead-letter queues: a message received more often than its
//! queue's redrive policy allows moves to the target queue, wherever that
//! is served. Standard queues only (no FIFO); the older query (XML)
//! protocol isn't spoken.
//!
//! A request for a queue another node runs is proxied there (see
//! `crate::frontend`); that node checks its signature.

use super::{
    MAX_BATCH, MAX_VISIBILITY, MAX_WAIT_SECS, MsgAttr, NewMsg, Queue, Received, SqsError, md5_hex,
    md5_of_attrs,
};
use crate::committer::PoolAdmin;
use crate::dataset::DatasetFormat;
use crate::frontend::http::{self as fhttp, Body, full};
use crate::frontend::{Frontend, Local, Located};
use crate::sigv4::{self, AuthError};
use crate::vdev::Pool;
use base64::Engine;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use raidish_core::role::Mutable;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;

pub const ACCOUNT: &str = "000000000000";
const MAX_REQUEST: usize = 4 << 20;
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

pub async fn serve<R: Mutable>(fe: Arc<Frontend<R>>, l: tokio::net::TcpListener) {
    fhttp::serve(l, fe.tls().https.clone(), move |req| {
        let fe = fe.clone();
        async move { handle(fe, req).await }
    })
    .await
}

/// An SQS error.
#[derive(Debug, Clone)]
pub struct ErrQ {
    pub status: StatusCode,
    /// Its JSON `__type` name.
    pub kind: &'static str,
    /// Its query-protocol code (`x-amzn-query-error`).
    pub query: &'static str,
    pub message: String,
}

fn eq(kind: &'static str, query: &'static str, message: impl Into<String>) -> ErrQ {
    ErrQ {
        status: StatusCode::BAD_REQUEST,
        kind,
        query,
        message: message.into(),
    }
}

fn no_queue() -> ErrQ {
    eq(
        "QueueDoesNotExist",
        "AWS.SimpleQueueService.NonExistentQueue",
        "The specified queue does not exist.",
    )
}

fn missing(p: &str) -> ErrQ {
    eq(
        "MissingParameter",
        "MissingParameter",
        format!("The request must contain the parameter {}.", p),
    )
}

fn bad_value(m: impl Into<String>) -> ErrQ {
    eq("InvalidParameterValue", "InvalidParameterValue", m)
}

/// This node's SQS host, as queue URLs name it.
fn host_of<R: Mutable>(fe: &Frontend<R>) -> String {
    match fe.config().sqs_port() {
        Some(p) => format!("{}:{}", fe.addr(), p),
        None => fe.addr(),
    }
}

fn unavailable() -> ErrQ {
    ErrQ {
        status: StatusCode::SERVICE_UNAVAILABLE,
        kind: "ServiceUnavailable",
        query: "ServiceUnavailable",
        message: "The queue is moving between nodes; please retry.".into(),
    }
}

/// The JSON `__type` for a code (the AWS SDKs read the part after `#`).
fn kind_of(code: &str) -> &'static str {
    match code {
        "InvalidAttributeName" => "InvalidAttributeName",
        "InvalidAttributeValue" => "InvalidAttributeValue",
        "InvalidMessageContents" => "InvalidMessageContents",
        "MissingParameter" => "MissingParameter",
        _ => "InvalidParameterValue",
    }
}

impl From<SqsError> for ErrQ {
    fn from(e: SqsError) -> Self {
        match e {
            SqsError::Invalid(code, m) => {
                let k = kind_of(code);
                eq(k, k, m)
            }
            SqsError::ReceiptHandleIsInvalid(m) => {
                eq("ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid", m)
            }
            SqsError::MessageNotInflight => eq(
                "MessageNotInflight",
                "AWS.SimpleQueueService.MessageNotInflight",
                "The message referred to isn't in flight.",
            ),
            SqsError::Unavailable => unavailable(),
            SqsError::Fs { kind, msg } => {
                use crate::types::ErrorKind as K;
                match kind {
                    K::Invalid => eq("InvalidParameterValue", "InvalidParameterValue", msg),
                    // Its lease is someone else's, or no committer: later.
                    K::Unavailable | K::Conflict => ErrQ {
                        message: msg,
                        ..unavailable()
                    },
                    _ => ErrQ {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        kind: "InternalError",
                        query: "InternalError",
                        message: msg,
                    },
                }
            }
        }
    }
}

impl From<AuthError> for ErrQ {
    fn from(e: AuthError) -> Self {
        let kind = match e {
            AuthError::UnknownKey => "InvalidClientTokenId",
            AuthError::Missing => "MissingAuthenticationToken",
            AuthError::Skewed | AuthError::Expired => "RequestExpired",
            AuthError::KeyExpired => "ExpiredToken",
            AuthError::Malformed(_) => "IncompleteSignature",
            AuthError::Mismatch => "SignatureDoesNotMatch",
        };
        ErrQ {
            status: StatusCode::FORBIDDEN,
            kind,
            query: kind,
            message: e.message(),
        }
    }
}

fn error_response(e: &ErrQ) -> Response<Body> {
    let body = json!({
        "__type": format!("com.amazonaws.sqs#{}", e.kind),
        "message": e.message,
    });
    let mut r = fhttp::status(e.status, full(body.to_string()));
    let h = r.headers_mut();
    h.insert(
        "content-type",
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    let fault = if e.status.is_server_error() {
        "Receiver"
    } else {
        "Sender"
    };
    if let Ok(v) = HeaderValue::from_str(&format!("{};{}", e.query, fault)) {
        h.insert("x-amzn-query-error", v);
    }
    r
}

fn json_response(v: Value) -> Response<Body> {
    let mut r = fhttp::status(StatusCode::OK, full(v.to_string()));
    r.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    r
}

// ---- names and URLs ---------------------------------------------------------

/// The queue a URL (or ARN) names.
pub fn queue_of_url(url: &str) -> Option<String> {
    let name = if url.starts_with("arn:") {
        url.rsplit(':').next()?
    } else {
        url.trim_end_matches('/').rsplit('/').next()?
    };
    (!name.is_empty()).then(|| name.to_string())
}

pub fn arn<R: Mutable>(fe: &Frontend<R>, name: &str) -> String {
    format!("arn:aws:sqs:{}:{}:{}", fe.config().region(), ACCOUNT, name)
}

fn url(host: &str, name: &str) -> String {
    format!("https://{}/{}/{}", host, ACCOUNT, name)
}

fn exists<R: Mutable>(fe: &Frontend<R>, name: &str) -> bool {
    fe.route(name)
        .is_some_and(|r| r.format == DatasetFormat::Queue)
}

// ---- requests ---------------------------------------------------------------

fn s<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

fn need<'a>(v: &'a Value, k: &str) -> Result<&'a str, ErrQ> {
    s(v, k).ok_or_else(|| missing(k))
}

/// An integer parameter (the SDKs send numbers; strings are accepted too).
fn int(v: &Value, k: &str, lo: u32, hi: u32) -> Result<Option<u32>, ErrQ> {
    let n = match v.get(k) {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    };
    match n {
        Some(n) if n >= lo as i64 && n <= hi as i64 => Ok(Some(n as u32)),
        _ => Err(bad_value(format!(
            "Value for parameter {} is invalid. Reason: Must be between {} and {}.",
            k, lo, hi
        ))),
    }
}

fn strings(v: &Value, k: &str) -> Vec<String> {
    v.get(k)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn string_map(v: &Value, k: &str) -> Result<Vec<(String, String)>, ErrQ> {
    let Some(m) = v.get(k) else {
        return Ok(Vec::new());
    };
    let m = m
        .as_object()
        .ok_or_else(|| bad_value(format!("{} must be a map", k)))?;
    m.iter()
        .map(|(k, v)| match v {
            Value::String(s) => Ok((k.clone(), s.clone())),
            Value::Number(n) => Ok((k.clone(), n.to_string())),
            Value::Bool(b) => Ok((k.clone(), b.to_string())),
            _ => Err(bad_value(format!("The value of {} must be a string", k))),
        })
        .collect()
}

fn msg_attrs(v: &Value, k: &str) -> Result<Vec<(String, MsgAttr)>, ErrQ> {
    let Some(m) = v.get(k) else {
        return Ok(Vec::new());
    };
    let m = m
        .as_object()
        .ok_or_else(|| bad_value(format!("{} must be a map", k)))?;
    let mut out = Vec::new();
    for (n, a) in m {
        let data_type = s(a, "DataType")
            .ok_or_else(|| {
                bad_value(format!(
                    "The message attribute '{}' must contain a DataType.",
                    n
                ))
            })?
            .to_string();
        let binary = match s(a, "BinaryValue") {
            Some(b) => Some(B64.decode(b).map_err(|_| {
                bad_value(format!(
                    "The message attribute '{}' has an invalid binary value.",
                    n
                ))
            })?),
            None => None,
        };
        out.push((
            n.clone(),
            MsgAttr {
                data_type,
                string: s(a, "StringValue").map(str::to_string),
                binary,
            },
        ));
    }
    Ok(out)
}

fn attr_json(a: &MsgAttr) -> Value {
    let mut o = Map::new();
    o.insert("DataType".into(), json!(a.data_type));
    if let Some(s) = &a.string {
        o.insert("StringValue".into(), json!(s));
    }
    if let Some(b) = &a.binary {
        o.insert("BinaryValue".into(), json!(B64.encode(b)));
    }
    Value::Object(o)
}

/// The access key a request was signed with: its SenderId.
fn sender_of(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|a| a.split("Credential=").nth(1))
        .and_then(|c| c.split('/').next())
        .unwrap_or("raidish")
        .to_string()
}

async fn handle<R: Mutable>(fe: Arc<Frontend<R>>, req: Request<Incoming>) -> Response<Body> {
    match handle_inner(&fe, req).await {
        Ok(r) => r,
        Err(e) => error_response(&e),
    }
}

async fn handle_inner<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    req: Request<Incoming>,
) -> Result<Response<Body>, ErrQ> {
    let (parts, body) = req.into_parts();
    let target = parts
        .headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .and_then(|t| t.strip_prefix("AmazonSQS."))
        .map(str::to_string);
    let Some(target) = target else {
        return Err(eq(
            "InvalidAction",
            "InvalidAction",
            "raidish speaks the SQS JSON protocol (X-Amz-Target: AmazonSQS.<Action>); the query protocol isn't supported",
        ));
    };
    if parts.method != Method::POST {
        return Err(eq(
            "InvalidAction",
            "InvalidAction",
            "SQS requests are POSTs",
        ));
    }
    let bytes = fhttp::collect(body, MAX_REQUEST).await.map_err(|_| {
        eq(
            "BatchRequestTooLong",
            "AWS.SimpleQueueService.BatchRequestTooLong",
            "The request is too long.",
        )
    })?;
    let input: Value = if bytes.iter().all(u8::is_ascii_whitespace) {
        json!({})
    } else {
        serde_json::from_slice(&bytes).map_err(|_| {
            eq(
                "InvalidParameterValue",
                "MalformedQueryString",
                "The request isn't valid JSON.",
            )
        })?
    };
    // A queue's request goes where it's served.
    let queue = match target.as_str() {
        "CreateQueue" | "GetQueueUrl" | "ListQueues" | "DeleteQueue" => None,
        _ => match s(&input, "QueueUrl") {
            Some(u) => Some(queue_of_url(u).ok_or_else(no_queue)?),
            None => return Err(missing("QueueUrl")),
        },
    };
    if let Some(q) = &queue {
        let forwarded = fhttp::forwarded_parts(&parts.headers);
        match fe.locate(q, DatasetFormat::Queue, forwarded, Local::queue) {
            Located::Here(_) => {}
            Located::There(o) => {
                let req = Request::from_parts(parts, http_body_util::Full::new(bytes));
                let port = fe.config().sqs_port().ok_or_else(unavailable)?;
                return fhttp::proxy(fe.tls(), req, &o.addr, port)
                    .await
                    .ok_or_else(unavailable);
            }
            Located::Unavailable => return Err(unavailable()),
            Located::Missing => return Err(no_queue()),
        }
    }
    let sha = hex::encode(Sha256::digest(&bytes));
    let signed = sigv4::verify(
        fe.auth(),
        "sqs",
        &parts.method,
        &parts.uri,
        &parts.headers,
        Some(&sha),
    )?;
    // A readonly token's keys look, and change nothing (receiving does).
    const READS: &[&str] = &[
        "GetQueueUrl",
        "ListQueues",
        "GetQueueAttributes",
        "ListQueueTags",
        "ListDeadLetterSourceQueues",
    ];
    if !READS.contains(&target.as_str())
        && fe.auth().role(&signed.access_key) < crate::pki::Role::Admin
    {
        return Err(ErrQ {
            status: StatusCode::FORBIDDEN,
            kind: "AccessDenied",
            query: "AccessDenied",
            message: "This access key is read-only.".into(),
        });
    }
    // A dead-letter move carries a message's history (when it was sent,
    // how often received): only a node moving one may claim that (I-34).
    if target == MOVE_MESSAGE && !fe.auth().is_internal(&signed.access_key) {
        return Err(ErrQ {
            status: StatusCode::FORBIDDEN,
            kind: "AccessDenied",
            query: "AccessDenied",
            message: format!("{} is internal.", MOVE_MESSAGE),
        });
    }
    let host = parts
        .headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| host_of(fe));
    let sender = sender_of(&parts.headers);
    let out = dispatch(fe, &target, &input, &host, &sender).await?;
    Ok(json_response(out))
}

/// Run one action (here, or for a queue served elsewhere, there).
pub async fn dispatch<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    target: &str,
    input: &Value,
    host: &str,
    sender: &str,
) -> Result<Value, ErrQ> {
    match target {
        "CreateQueue" => create_queue(fe, input, host).await,
        "GetQueueUrl" => {
            let name = need(input, "QueueName")?;
            if !exists(fe, name) {
                return Err(no_queue());
            }
            Ok(json!({ "QueueUrl": url(host, name) }))
        }
        "ListQueues" => list_queues(fe, input, host),
        "DeleteQueue" => {
            let name = queue_of_url(need(input, "QueueUrl")?).ok_or_else(no_queue)?;
            if !exists(fe, &name) {
                return Err(no_queue());
            }
            let Ok(name) = crate::types::DatasetName::try_from(name.as_str()) else {
                return Err(no_queue());
            };
            match fe.delete_service(&name).await {
                Ok(true) => Ok(json!({})),
                Ok(false) => Err(no_queue()),
                Err(m) => Err(ErrQ {
                    message: m,
                    ..unavailable()
                }),
            }
        }
        _ => {
            let name = queue_of_url(need(input, "QueueUrl")?).ok_or_else(no_queue)?;
            match fe.local(&name).as_deref() {
                Some(Local::Queue(q)) => queue_op(fe, q, target, input, host, sender).await,
                _ => call(fe, &name, target, input.clone()).await,
            }
        }
    }
}

/// Ask the node serving queue `name` to run an action (signed as one of
/// a short-lived key of our own).
pub fn call<'a, R: Mutable>(
    fe: &'a Arc<Frontend<R>>,
    name: &'a str,
    target: &'a str,
    input: Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, ErrQ>> + Send + 'a>> {
    Box::pin(call_inner(fe, name, target, input))
}

async fn call_inner<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    name: &str,
    target: &str,
    mut input: Value,
) -> Result<Value, ErrQ> {
    let owner = match fe.locate(name, DatasetFormat::Queue, false, Local::queue) {
        Located::Here(q) => {
            let host = host_of(fe);
            return queue_op(fe, &q, target, &input, &host, "raidish").await;
        }
        Located::There(owner) => owner,
        Located::Unavailable => return Err(unavailable()),
        Located::Missing => return Err(no_queue()),
    };
    let (ak, sk) = fe.auth().internal()?;
    let port = fe.config().sqs_port().ok_or_else(unavailable)?;
    let host = format!("{}:{}", owner.addr, port);
    input["QueueUrl"] = json!(url(&host, name));
    let body = input.to_string().into_bytes();
    let mut headers = HeaderMap::new();
    headers.insert(
        "host",
        HeaderValue::from_str(&host).map_err(|_| unavailable())?,
    );
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-amz-json-1.0"),
    );
    headers.insert(
        "x-amz-target",
        HeaderValue::from_str(&format!("AmazonSQS.{}", target)).map_err(|_| unavailable())?,
    );
    let uri: hyper::Uri = format!("https://{}/", host)
        .parse()
        .map_err(|_| unavailable())?;
    for (k, v) in sigv4::sign(
        &ak,
        &sk,
        &fe.config().region(),
        "sqs",
        &Method::POST,
        &uri,
        &headers,
        &body,
    ) {
        if let (Ok(k), Ok(v)) = (
            hyper::header::HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            headers.insert(k, v);
        }
    }
    let mut req = Request::new(http_body_util::Full::new(Bytes::from(body)));
    *req.method_mut() = Method::POST;
    *req.uri_mut() = "/".parse().unwrap();
    *req.headers_mut() = headers;
    let resp = fhttp::proxy(fe.tls(), req, &owner.addr, port)
        .await
        .ok_or_else(unavailable)?;
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|_| unavailable())?
        .to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!({}));
    if status.is_success() {
        return Ok(v);
    }
    let kind = s(&v, "__type")
        .and_then(|t| t.rsplit('#').next())
        .unwrap_or("InternalError");
    let message = s(&v, "message").unwrap_or("").to_string();
    Err(match kind {
        "QueueDoesNotExist" => no_queue(),
        "ServiceUnavailable" => unavailable(),
        k => ErrQ {
            status,
            kind: kind_of(k),
            query: kind_of(k),
            message,
        },
    })
}

// ---- queues -----------------------------------------------------------------

fn check_redrive_target<R: Mutable>(
    fe: &Frontend<R>,
    attrs: &[(String, String)],
) -> Result<(), ErrQ> {
    for (k, v) in attrs {
        if k == "RedrivePolicy" && !v.is_empty() {
            let r = super::parse_redrive(v).map_err(ErrQ::from)?;
            let target = queue_of_url(&r.target_arn).unwrap_or_default();
            if !exists(fe, &target) {
                return Err(bad_value(format!(
                    "Value {} for parameter RedrivePolicy is invalid. Reason: Dead letter target does not exist.",
                    v
                )));
            }
        }
    }
    Ok(())
}

async fn create_queue<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    input: &Value,
    host: &str,
) -> Result<Value, ErrQ> {
    let name = need(input, "QueueName")?.to_string();
    if name.ends_with(".fifo") {
        return Err(bad_value("FIFO queues aren't supported by raidish."));
    }
    let queue =
        crate::types::QueueName::try_from(name.as_str()).map_err(|e| bad_value(e.to_string()))?;
    let attrs = string_map(input, "Attributes")?;
    let tags = string_map(input, "tags")?;
    // Checked before anything's made.
    let mut probe = super::QueueAttrs::default();
    for (k, v) in &attrs {
        probe.set(k, v).map_err(ErrQ::from)?;
    }
    check_redrive_target(fe, &attrs)?;
    if exists(fe, &name) {
        // The same queue again is fine; different attributes aren't.
        if !attrs.is_empty() {
            let names: Vec<&str> = attrs.iter().map(|(k, _)| k.as_str()).collect();
            let have = call(
                fe,
                &name,
                "GetQueueAttributes",
                json!({ "AttributeNames": names }),
            )
            .await?;
            let differs = attrs.iter().any(|(k, v)| {
                have.get("Attributes").and_then(|a| s(a, k)) != Some(normalize(k, v).as_str())
            });
            if differs {
                return Err(eq(
                    "QueueNameExists",
                    "QueueAlreadyExists",
                    "A queue already exists with the same name and a different value for attribute(s).",
                ));
            }
        }
        return Ok(json!({ "QueueUrl": url(host, &name) }));
    }
    let req = crate::committer::CreateDataset {
        name: queue.into(),

        format: DatasetFormat::Queue,
        props: Default::default(),
    };
    if let Err(e) = fe.committer().create_dataset(&req).await
        && e.kind() != crate::types::ErrorKind::Exists
    {
        return Err(ErrQ {
            message: e.to_string(),
            ..unavailable()
        });
    }
    let n = name.clone();
    fe.wait_routes(20, |routes| {
        routes.get(&n).is_some_and(|r| r.owner.is_some())
    })
    .await;
    if !attrs.is_empty() || !tags.is_empty() {
        // Its first attributes, wherever it's served (retried while it
        // starts there).
        let mut last = None;
        for _ in 0..40 {
            let mut r = Ok(json!({}));
            if !attrs.is_empty() {
                r = call(fe, &name, "SetQueueAttributes", json!({ "Attributes": Map::from_iter(attrs.iter().map(|(k, v)| (k.clone(), json!(v)))) })).await;
            }
            if r.is_ok() && !tags.is_empty() {
                r = call(fe, &name, "TagQueue", json!({ "Tags": Map::from_iter(tags.iter().map(|(k, v)| (k.clone(), json!(v)))) })).await;
            }
            match r {
                Ok(_) => {
                    last = None;
                    break;
                }
                Err(e)
                    if e.status == StatusCode::SERVICE_UNAVAILABLE
                        || e.kind == "QueueDoesNotExist" =>
                {
                    last = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                Err(e) => return Err(e),
            }
        }
        if let Some(e) = last {
            return Err(e);
        }
    }
    Ok(json!({ "QueueUrl": url(host, &name) }))
}

/// An attribute's value as GetQueueAttributes gives it back.
fn normalize(k: &str, v: &str) -> String {
    let mut a = super::QueueAttrs::default();
    match a.set(k, v) {
        Ok(()) => a.to_map().get(k).cloned().unwrap_or_else(|| v.to_string()),
        Err(_) => v.to_string(),
    }
}

fn list_queues<R: Mutable>(fe: &Frontend<R>, input: &Value, host: &str) -> Result<Value, ErrQ> {
    let prefix = s(input, "QueueNamePrefix").unwrap_or("");
    let max = int(input, "MaxResults", 1, 1000)?;
    let after = match s(input, "NextToken") {
        Some(t) => Some(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(t)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| bad_value("Invalid NextToken"))?,
        ),
        None => None,
    };
    let names: Vec<String> = fe
        .routes()
        .into_iter()
        .filter(|(n, r)| r.format == DatasetFormat::Queue && n.starts_with(prefix))
        .map(|(n, _)| n)
        .filter(|n| after.as_ref().is_none_or(|a| n > a))
        .collect();
    let limit = max.unwrap_or(1000) as usize;
    let truncated = names.len() > limit;
    let page = &names[..names.len().min(limit)];
    let mut out = Map::new();
    if !page.is_empty() {
        out.insert(
            "QueueUrls".into(),
            json!(page.iter().map(|n| url(host, n)).collect::<Vec<_>>()),
        );
    }
    if truncated
        && max.is_some()
        && let Some(last) = page.last()
    {
        out.insert(
            "NextToken".into(),
            json!(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(last)),
        );
    }
    Ok(Value::Object(out))
}

// ---- a queue's actions ------------------------------------------------------

/// Check a batch's entries: their ids, distinct, 1 to 10.
fn batch(input: &Value, max_bytes: Option<usize>) -> Result<&Vec<Value>, ErrQ> {
    let entries = input
        .get("Entries")
        .and_then(Value::as_array)
        .ok_or_else(|| missing("Entries"))?;
    if entries.is_empty() {
        return Err(eq(
            "EmptyBatchRequest",
            "AWS.SimpleQueueService.EmptyBatchRequest",
            "There should be at least one SendMessageBatchRequestEntry in the request.",
        ));
    }
    if entries.len() > MAX_BATCH {
        return Err(eq(
            "TooManyEntriesInBatchRequest",
            "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
            format!("Maximum number of entries per request are {}.", MAX_BATCH),
        ));
    }
    let mut ids = BTreeSet::new();
    for e in entries {
        let id = need(e, "Id")?;
        if id.is_empty()
            || id.len() > 80
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(eq(
                "InvalidBatchEntryId",
                "AWS.SimpleQueueService.InvalidBatchEntryId",
                "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
            ));
        }
        if !ids.insert(id) {
            return Err(eq(
                "BatchEntryIdsNotDistinct",
                "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                format!("Id {} repeated.", id),
            ));
        }
    }
    if let Some(max) = max_bytes {
        let total: usize = entries
            .iter()
            .map(|e| s(e, "MessageBody").map_or(0, str::len))
            .sum();
        if total > max {
            return Err(eq(
                "BatchRequestTooLong",
                "AWS.SimpleQueueService.BatchRequestTooLong",
                format!("Batch requests cannot be longer than {} bytes.", max),
            ));
        }
    }
    Ok(entries)
}

fn failed(id: &str, e: ErrQ) -> Value {
    json!({
        "Id": id,
        "SenderFault": !e.status.is_server_error(),
        "Code": e.kind,
        "Message": e.message,
    })
}

/// The internal action a dead-letter move sends a message with: a
/// SendMessage that also carries `RaidishMoved` (its history), accepted
/// only from a node's own key. SendMessage itself ignores `RaidishMoved`:
/// any client could set it, and a `SentMs` of `u64::MAX` overflowed the
/// retention arithmetic (I-34).
pub const MOVE_MESSAGE: &str = "RaidishMoveMessage";

fn new_msg(e: &Value, sender: &str) -> Result<NewMsg, ErrQ> {
    if e.get("MessageGroupId").is_some() || e.get("MessageDeduplicationId").is_some() {
        return Err(bad_value(
            "MessageGroupId and MessageDeduplicationId are for FIFO queues, which raidish doesn't support.",
        ));
    }
    let m = NewMsg {
        body: s(e, "MessageBody")
            .ok_or_else(|| missing("MessageBody"))?
            .to_string(),
        delay: int(e, "DelaySeconds", 0, super::MAX_DELAY)?,
        attrs: msg_attrs(e, "MessageAttributes")?,
        sys_attrs: msg_attrs(e, "MessageSystemAttributes")?,
        sender: sender.to_string(),
        ..Default::default()
    };
    Ok(m)
}

/// A message a dead-letter move sends (`MOVE_MESSAGE`): `new_msg`, with
/// the history the source queue kept. Times are no later than now (a
/// message can't have been sent in the future), counts saturate.
fn moved_msg(e: &Value, sender: &str) -> Result<NewMsg, ErrQ> {
    let mut m = new_msg(e, sender)?;
    if let Some(mv) = e.get("RaidishMoved") {
        let now = super::now_ms();
        m.sent_ms = mv.get("SentMs").and_then(Value::as_u64).map(|t| t.min(now));
        m.receive_count = mv
            .get("ReceiveCount")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(u32::MAX as u64) as u32;
        m.first_receive_ms = mv
            .get("FirstReceiveMs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .min(now);
        m.dead_from = s(mv, "From").map(str::to_string);
        if let Some(sd) = s(mv, "Sender") {
            m.sender = sd.to_string();
        }
    }
    Ok(m)
}

fn sent_json(sent: &super::Sent) -> Map<String, Value> {
    let mut o = Map::new();
    o.insert("MessageId".into(), json!(sent.id));
    o.insert("MD5OfMessageBody".into(), json!(sent.md5_body));
    if let Some(m) = &sent.md5_attrs {
        o.insert("MD5OfMessageAttributes".into(), json!(m));
    }
    if let Some(m) = &sent.md5_sys {
        o.insert("MD5OfMessageSystemAttributes".into(), json!(m));
    }
    o
}

async fn queue_op<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    q: &Arc<Queue<Pool, R>>,
    target: &str,
    input: &Value,
    host: &str,
    sender: &str,
) -> Result<Value, ErrQ> {
    match target {
        "SendMessage" | MOVE_MESSAGE => {
            let m = if target == MOVE_MESSAGE {
                moved_msg(input, sender)?
            } else {
                new_msg(input, sender)?
            };
            let sent = q.send(vec![m]).await?.remove(0)?;
            Ok(Value::Object(sent_json(&sent)))
        }
        "SendMessageBatch" => {
            let entries = batch(input, Some(super::MAX_MESSAGE as usize))?;
            let mut ok_ids = Vec::new();
            let mut msgs = Vec::new();
            let mut fails = Vec::new();
            for e in entries {
                let id = s(e, "Id").unwrap_or("").to_string();
                match new_msg(e, sender) {
                    Ok(m) => {
                        ok_ids.push(id);
                        msgs.push(m);
                    }
                    Err(err) => fails.push(failed(&id, err)),
                }
            }
            let mut good = Vec::new();
            for (id, r) in ok_ids.into_iter().zip(q.send(msgs).await?) {
                match r {
                    Ok(sent) => {
                        let mut o = sent_json(&sent);
                        o.insert("Id".into(), json!(id));
                        good.push(Value::Object(o));
                    }
                    Err(e) => fails.push(failed(&id, e.into())),
                }
            }
            Ok(json!({ "Successful": good, "Failed": fails }))
        }
        "ReceiveMessage" => receive(fe, q, input).await,
        "DeleteMessage" => {
            let r = need(input, "ReceiptHandle")?.to_string();
            q.delete(vec![r]).await?.remove(0)?;
            Ok(json!({}))
        }
        "DeleteMessageBatch" => {
            let entries = batch(input, None)?;
            let mut receipts = Vec::new();
            let mut ids = Vec::new();
            let mut fails = Vec::new();
            for e in entries {
                let id = s(e, "Id").unwrap_or("").to_string();
                match s(e, "ReceiptHandle") {
                    Some(r) => {
                        ids.push(id);
                        receipts.push(r.to_string());
                    }
                    None => fails.push(failed(&id, missing("ReceiptHandle"))),
                }
            }
            let mut good = Vec::new();
            for (id, r) in ids.into_iter().zip(q.delete(receipts).await?) {
                match r {
                    Ok(()) => good.push(json!({ "Id": id })),
                    Err(e) => fails.push(failed(&id, e.into())),
                }
            }
            Ok(json!({ "Successful": good, "Failed": fails }))
        }
        "ChangeMessageVisibility" => {
            let r = need(input, "ReceiptHandle")?.to_string();
            let v = int(input, "VisibilityTimeout", 0, MAX_VISIBILITY)?
                .ok_or_else(|| missing("VisibilityTimeout"))?;
            q.change_visibility(vec![(r, v)]).await?.remove(0)?;
            Ok(json!({}))
        }
        "ChangeMessageVisibilityBatch" => {
            let entries = batch(input, None)?;
            let mut todo = Vec::new();
            let mut ids = Vec::new();
            let mut fails = Vec::new();
            for e in entries {
                let id = s(e, "Id").unwrap_or("").to_string();
                let r = s(e, "ReceiptHandle").ok_or_else(|| missing("ReceiptHandle"));
                let v = int(e, "VisibilityTimeout", 0, MAX_VISIBILITY)
                    .and_then(|v| v.ok_or_else(|| missing("VisibilityTimeout")));
                match (r, v) {
                    (Ok(r), Ok(v)) => {
                        ids.push(id);
                        todo.push((r.to_string(), v));
                    }
                    (Err(e), _) | (_, Err(e)) => fails.push(failed(&id, e)),
                }
            }
            let mut good = Vec::new();
            for (id, r) in ids.into_iter().zip(q.change_visibility(todo).await?) {
                match r {
                    Ok(()) => good.push(json!({ "Id": id })),
                    Err(e) => fails.push(failed(&id, e.into())),
                }
            }
            Ok(json!({ "Successful": good, "Failed": fails }))
        }
        "GetQueueAttributes" => {
            let want = strings(input, "AttributeNames");
            let all = want.iter().any(|w| w == "All");
            let meta = q.meta().await?;
            let mut out = Map::new();
            let mut add = |k: &str, v: String| {
                if all || want.iter().any(|w| w == k) {
                    out.insert(k.to_string(), json!(v));
                }
            };
            for (k, v) in meta.attrs.to_map() {
                add(k, v);
            }
            add("QueueArn", arn(fe, &q.tree.name));
            add("CreatedTimestamp", (meta.created_ms / 1000).to_string());
            add(
                "LastModifiedTimestamp",
                (meta.modified_ms / 1000).to_string(),
            );
            let counts = [
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ];
            if all || want.iter().any(|w| counts.contains(&w.as_str())) {
                let c = q.counts().await?;
                add(counts[0], c.visible.to_string());
                add(counts[1], c.inflight.to_string());
                add(counts[2], c.delayed.to_string());
            }
            for w in &want {
                let known = w == "All"
                    || counts.contains(&w.as_str())
                    || out.contains_key(w)
                    || matches!(
                        w.as_str(),
                        "RedrivePolicy"
                            | "RedriveAllowPolicy"
                            | "Policy"
                            | "QueueArn"
                            | "CreatedTimestamp"
                            | "LastModifiedTimestamp"
                    );
                if !known {
                    return Err(eq(
                        "InvalidAttributeName",
                        "InvalidAttributeName",
                        format!("Unknown Attribute {}.", w),
                    ));
                }
            }
            if out.is_empty() {
                return Ok(json!({}));
            }
            Ok(json!({ "Attributes": out }))
        }
        "SetQueueAttributes" => {
            let attrs = string_map(input, "Attributes")?;
            check_redrive_target(fe, &attrs)?;
            q.set_attrs(attrs).await?;
            Ok(json!({}))
        }
        "PurgeQueue" => {
            q.purge().await?;
            Ok(json!({}))
        }
        "TagQueue" => {
            q.tag(string_map(input, "Tags")?, Vec::new()).await?;
            Ok(json!({}))
        }
        "UntagQueue" => {
            q.tag(Vec::new(), strings(input, "TagKeys")).await?;
            Ok(json!({}))
        }
        "ListQueueTags" => {
            let tags = q.meta().await?.tags;
            if tags.is_empty() {
                return Ok(json!({}));
            }
            Ok(json!({ "Tags": tags }))
        }
        "ListDeadLetterSourceQueues" => {
            let me = arn(fe, &q.tree.name);
            let mut urls = Vec::new();
            for (name, r) in fe.routes() {
                if r.format != DatasetFormat::Queue || name == q.tree.name.as_str() {
                    continue;
                }
                let a = call(
                    fe,
                    &name,
                    "GetQueueAttributes",
                    json!({ "AttributeNames": ["RedrivePolicy"] }),
                )
                .await;
                if let Ok(a) = a
                    && let Some(p) = a.get("Attributes").and_then(|a| s(a, "RedrivePolicy"))
                    && super::parse_redrive(p).is_ok_and(|r| r.target_arn == me)
                {
                    urls.push(url(host, &name));
                }
            }
            Ok(json!({ "queueUrls": urls }))
        }
        t => Err(eq(
            "UnsupportedOperation",
            "AWS.SimpleQueueService.UnsupportedOperation",
            format!("{} isn't supported by raidish", t),
        )),
    }
}

/// Whether a message attribute name is asked for (`All`, `.*`, a name, or
/// `prefix.*`).
fn wanted(names: &[String], n: &str) -> bool {
    names.iter().any(|w| {
        w == "All"
            || w == ".*"
            || w == n
            || w.strip_suffix(".*")
                .is_some_and(|p| n.starts_with(&format!("{}.", p)))
    })
}

async fn receive<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    q: &Arc<Queue<Pool, R>>,
    input: &Value,
) -> Result<Value, ErrQ> {
    let max = int(input, "MaxNumberOfMessages", 1, MAX_BATCH as u32)?.unwrap_or(1) as usize;
    let vis = int(input, "VisibilityTimeout", 0, MAX_VISIBILITY)?;
    let wait = int(input, "WaitTimeSeconds", 0, MAX_WAIT_SECS)?;
    let mut sys = strings(input, "AttributeNames");
    sys.extend(strings(input, "MessageSystemAttributeNames"));
    let user = strings(input, "MessageAttributeNames");
    let (got, dead) = q.receive_wait(max, vis, wait).await?;
    if !dead.is_empty() {
        let fe = fe.clone();
        let q = q.clone();
        tokio::spawn(async move { move_dead(&fe, &q, dead).await });
    }
    let msgs: Vec<Value> = got
        .iter()
        .map(|r| message_json(fe, q, r, &sys, &user))
        .collect();
    if msgs.is_empty() {
        return Ok(json!({}));
    }
    Ok(json!({ "Messages": msgs }))
}

fn message_json<R: Mutable>(
    fe: &Frontend<R>,
    q: &Queue<Pool, R>,
    r: &Received,
    sys: &[String],
    user: &[String],
) -> Value {
    let m = &r.msg;
    let mut o = Map::new();
    o.insert("MessageId".into(), json!(m.id));
    o.insert("ReceiptHandle".into(), json!(r.receipt));
    o.insert("MD5OfBody".into(), json!(md5_hex(m.body.as_bytes())));
    o.insert("Body".into(), json!(m.body));
    let all = sys.iter().any(|s| s == "All");
    let mut a = Map::new();
    let mut add = |k: &str, v: String| {
        if all || sys.iter().any(|s| s == k) {
            a.insert(k.into(), json!(v));
        }
    };
    add("SenderId", m.sender.clone());
    add("SentTimestamp", m.sent_ms.to_string());
    add("ApproximateReceiveCount", m.receive_count.to_string());
    add(
        "ApproximateFirstReceiveTimestamp",
        m.first_receive_ms.to_string(),
    );
    if let Some(from) = &m.dead_from {
        add("DeadLetterQueueSourceArn", arn(fe, from));
    }
    for (k, v) in &m.sys_attrs {
        if let Some(s) = &v.string {
            add(k, s.clone());
        }
    }
    let _ = q;
    if !a.is_empty() {
        o.insert("Attributes".into(), Value::Object(a));
    }
    let chosen: Vec<(String, MsgAttr)> = m
        .attrs
        .iter()
        .filter(|(n, _)| wanted(user, n))
        .cloned()
        .collect();
    if !chosen.is_empty() {
        o.insert(
            "MD5OfMessageAttributes".into(),
            json!(md5_of_attrs(&chosen).unwrap()),
        );
        o.insert(
            "MessageAttributes".into(),
            Value::Object(
                chosen
                    .iter()
                    .map(|(n, a)| (n.clone(), attr_json(a)))
                    .collect(),
            ),
        );
    }
    Value::Object(o)
}

/// Move messages received too often to the dead-letter queue (they stay
/// hidden a while meanwhile; if the move fails they come back).
async fn move_dead<R: Mutable>(
    fe: &Arc<Frontend<R>>,
    q: &Arc<Queue<Pool, R>>,
    dead: Vec<super::Dead>,
) {
    let Ok(meta) = q.meta().await else { return };
    let Some(target) = meta
        .attrs
        .redrive()
        .and_then(|r| queue_of_url(&r.target_arn))
    else {
        return;
    };
    for d in dead {
        let m = &d.msg;
        let attrs: Map<String, Value> = m
            .attrs
            .iter()
            .map(|(n, a)| (n.clone(), attr_json(a)))
            .collect();
        let sys: Map<String, Value> = m
            .sys_attrs
            .iter()
            .map(|(n, a)| (n.clone(), attr_json(a)))
            .collect();
        let mut input = json!({
            "MessageBody": m.body,
            "RaidishMoved": {
                "SentMs": m.sent_ms,
                "ReceiveCount": m.receive_count,
                "FirstReceiveMs": m.first_receive_ms,
                "From": m.dead_from.clone().unwrap_or_else(|| q.tree.name.to_string()),
                "Sender": m.sender,
            },
        });
        if !attrs.is_empty() {
            input["MessageAttributes"] = Value::Object(attrs);
        }
        if !sys.is_empty() {
            input["MessageSystemAttributes"] = Value::Object(sys);
        }
        match call(fe, &target, MOVE_MESSAGE, input).await {
            Ok(_) => {
                if let Err(e) = q
                    .remove(super::Receipt {
                        seq: d.seq,
                        nonce: m.nonce,
                    })
                    .await
                {
                    eprintln!(
                        "sqs '{}': dropping a message moved to '{}': {:?}",
                        q.tree.name, target, e
                    );
                }
            }
            Err(e) => eprintln!(
                "sqs '{}': moving a message to its dead-letter queue '{}': {}",
                q.tree.name, target, e.message
            ),
        }
    }
}

#[cfg(test)]
#[path = "../tests/sqs_http_tests.rs"]
mod tests;
