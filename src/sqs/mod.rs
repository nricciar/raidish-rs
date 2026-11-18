//! SQS queues: one queue is one dataset of format `queue`, a B+tree (see
//! `crate::btree`) holding its messages in the order sent, and an index of
//! when each is next visible. The HTTP API is in [`http`].
//!
//! ```text
//!   M                   the queue's attributes, tags, counters
//!   Q <seq>             a message: body, attributes, receive count, when
//!                       it's visible next, its current receipt
//!   V <visible> <seq>   the index of messages by when they're visible
//!                       (a byte: 0 never received yet, 1 received)
//! ```
//!
//! Standard queues: at-least-once, roughly in order. Sends, receives and
//! deletes are made durable before they're answered (a receive's new
//! visibility and count too, so a crash can't lose a receive count that
//! decides a move to a dead-letter queue).
//!
//! **Receipts.** Each receive of a message gives it a new receipt (its seq
//! and a fresh nonce). Only the latest one deletes it or changes its
//! visibility: a consumer whose visibility timeout ran out, and whose
//! message went to another consumer, can't delete it from under that one
//! (its delete is a no-op, as for a message already deleted).
//!
//! **Retention.** Messages older than the retention period are deleted in
//! the background (and skipped when received).

pub mod http;

use crate::btree::Txn;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::BlockDevice;
use crate::frontend::Settler;
use crate::fs::{FileSystem, FileSystemError};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub const MAX_BATCH: usize = 10;
pub const MAX_WAIT_SECS: u32 = 20;
pub const MAX_VISIBILITY: u32 = 43_200;
pub const MAX_DELAY: u32 = 900;
pub const MAX_MESSAGE: u32 = 262_144;
pub const MAX_ATTRIBUTES: usize = 10;
/// How long a message being moved to a dead-letter queue is hidden.
const MOVE_HIDE_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqsError {
    /// (code, message): a request the queue refuses.
    Invalid(&'static str, String),
    ReceiptHandleIsInvalid(String),
    MessageNotInflight,
    Unavailable,
    Fs(String),
}

impl From<FileSystemError> for SqsError {
    fn from(e: FileSystemError) -> Self {
        match e {
            FileSystemError::Unavailable(_) => SqsError::Unavailable,
            e => SqsError::Fs(e.to_string()),
        }
    }
}

impl From<bincode::Error> for SqsError {
    fn from(e: bincode::Error) -> Self {
        SqsError::Fs(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SqsError>;

fn invalid(code: &'static str, m: impl Into<String>) -> SqsError {
    SqsError::Invalid(code, m.into())
}

/// A queue's settable attributes.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueAttrs {
    pub delay_seconds: u32,
    pub maximum_message_size: u32,
    pub message_retention_period: u32,
    pub receive_message_wait_time_seconds: u32,
    pub visibility_timeout: u32,
    /// JSON, as given (`deadLetterTargetArn`, `maxReceiveCount`).
    pub redrive_policy: Option<String>,
    pub redrive_allow_policy: Option<String>,
    pub policy: Option<String>,
    pub sqs_managed_sse_enabled: bool,
}

impl Default for QueueAttrs {
    fn default() -> Self {
        QueueAttrs {
            delay_seconds: 0,
            maximum_message_size: MAX_MESSAGE,
            message_retention_period: 345_600,
            receive_message_wait_time_seconds: 0,
            visibility_timeout: 30,
            redrive_policy: None,
            redrive_allow_policy: None,
            policy: None,
            sqs_managed_sse_enabled: true,
        }
    }
}

/// A parsed redrive policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redrive {
    pub target_arn: String,
    pub max_receive_count: u32,
}

fn num(v: &str, name: &str, lo: u32, hi: u32) -> Result<u32> {
    v.trim()
        .parse::<u32>()
        .ok()
        .filter(|n| (lo..=hi).contains(n))
        .ok_or_else(|| {
            invalid(
                "InvalidAttributeValue",
                format!(
                    "Invalid value for the parameter {}: must be {} to {}",
                    name, lo, hi
                ),
            )
        })
}

/// Parse a redrive policy (`maxReceiveCount` may be a number or a string).
pub fn parse_redrive(s: &str) -> Result<Redrive> {
    let bad = || {
        invalid(
            "InvalidAttributeValue",
            "Value for parameter RedrivePolicy is invalid.",
        )
    };
    let v: serde_json::Value = serde_json::from_str(s).map_err(|_| bad())?;
    let target_arn = v
        .get("deadLetterTargetArn")
        .and_then(|a| a.as_str())
        .filter(|a| a.starts_with("arn:"))
        .ok_or_else(bad)?
        .to_string();
    let max_receive_count = match v.get("maxReceiveCount") {
        Some(serde_json::Value::Number(n)) => n.as_u64().map(|n| n as u32),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
    .filter(|n| (1..=1000).contains(n))
    .ok_or_else(bad)?;
    Ok(Redrive {
        target_arn,
        max_receive_count,
    })
}

impl QueueAttrs {
    /// Set one attribute by its SQS name.
    pub fn set(&mut self, name: &str, v: &str) -> Result<()> {
        match name {
            "DelaySeconds" => self.delay_seconds = num(v, name, 0, MAX_DELAY)?,
            "MaximumMessageSize" => self.maximum_message_size = num(v, name, 1024, MAX_MESSAGE)?,
            "MessageRetentionPeriod" => {
                self.message_retention_period = num(v, name, 60, 1_209_600)?
            }
            "ReceiveMessageWaitTimeSeconds" => {
                self.receive_message_wait_time_seconds = num(v, name, 0, MAX_WAIT_SECS)?
            }
            "VisibilityTimeout" => self.visibility_timeout = num(v, name, 0, MAX_VISIBILITY)?,
            "RedrivePolicy" => {
                self.redrive_policy = if v.is_empty() {
                    None
                } else {
                    parse_redrive(v)?;
                    Some(v.to_string())
                }
            }
            "RedriveAllowPolicy" => {
                self.redrive_allow_policy = (!v.is_empty()).then(|| v.to_string())
            }
            "Policy" => self.policy = (!v.is_empty()).then(|| v.to_string()),
            "SqsManagedSseEnabled" => {
                self.sqs_managed_sse_enabled = match v {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(invalid(
                            "InvalidAttributeValue",
                            "SqsManagedSseEnabled: true or false",
                        ));
                    }
                }
            }
            "FifoQueue"
            | "ContentBasedDeduplication"
            | "DeduplicationScope"
            | "FifoThroughputLimit" => {
                return Err(invalid(
                    "InvalidAttributeName",
                    "FIFO queues aren't supported by raidish",
                ));
            }
            "KmsMasterKeyId" | "KmsDataKeyReusePeriodSeconds" => {
                return Err(invalid(
                    "InvalidAttributeName",
                    format!("{} isn't supported by raidish", name),
                ));
            }
            _ => {
                return Err(invalid(
                    "InvalidAttributeName",
                    format!("Unknown Attribute {}.", name),
                ));
            }
        }
        Ok(())
    }

    /// The settable attributes by SQS name.
    pub fn to_map(&self) -> BTreeMap<&'static str, String> {
        let mut m = BTreeMap::new();
        m.insert("DelaySeconds", self.delay_seconds.to_string());
        m.insert("MaximumMessageSize", self.maximum_message_size.to_string());
        m.insert(
            "MessageRetentionPeriod",
            self.message_retention_period.to_string(),
        );
        m.insert(
            "ReceiveMessageWaitTimeSeconds",
            self.receive_message_wait_time_seconds.to_string(),
        );
        m.insert("VisibilityTimeout", self.visibility_timeout.to_string());
        if let Some(r) = &self.redrive_policy {
            m.insert("RedrivePolicy", r.clone());
        }
        if let Some(r) = &self.redrive_allow_policy {
            m.insert("RedriveAllowPolicy", r.clone());
        }
        if let Some(p) = &self.policy {
            m.insert("Policy", p.clone());
        }
        m.insert(
            "SqsManagedSseEnabled",
            self.sqs_managed_sse_enabled.to_string(),
        );
        m
    }

    pub fn redrive(&self) -> Option<Redrive> {
        self.redrive_policy
            .as_deref()
            .and_then(|r| parse_redrive(r).ok())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Meta {
    pub attrs: QueueAttrs,
    pub next_seq: u64,
    pub created_ms: u64,
    pub modified_ms: u64,
    pub tags: BTreeMap<String, String>,
    /// Messages in the queue.
    pub messages: u64,
}

/// A message attribute (or message system attribute).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MsgAttr {
    pub data_type: String,
    pub string: Option<String>,
    pub binary: Option<Vec<u8>>,
}

impl MsgAttr {
    fn size(&self, name: &str) -> usize {
        name.len()
            + self.data_type.len()
            + self.string.as_ref().map_or(0, |s| s.len())
            + self.binary.as_ref().map_or(0, |b| b.len())
    }
}

/// The MD5 of message attributes, as SQS computes it (clients check it).
pub fn md5_of_attrs(attrs: &[(String, MsgAttr)]) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    let mut sorted: Vec<&(String, MsgAttr)> = attrs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = Md5::new();
    let put = |h: &mut Md5, b: &[u8]| {
        h.update((b.len() as u32).to_be_bytes());
        h.update(b);
    };
    for (n, a) in sorted {
        put(&mut h, n.as_bytes());
        put(&mut h, a.data_type.as_bytes());
        if let Some(b) = &a.binary {
            h.update([2u8]);
            put(&mut h, b);
        } else {
            h.update([1u8]);
            put(&mut h, a.string.as_deref().unwrap_or("").as_bytes());
        }
    }
    Some(hex::encode(h.finalize()))
}

pub fn md5_hex(b: &[u8]) -> String {
    hex::encode(Md5::digest(b))
}

/// A message, stored.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Msg {
    pub id: String,
    pub body: String,
    pub attrs: Vec<(String, MsgAttr)>,
    pub sys_attrs: Vec<(String, MsgAttr)>,
    pub sent_ms: u64,
    pub first_receive_ms: u64,
    pub receive_count: u32,
    pub visible_at_ms: u64,
    /// The latest receipt's nonce (0: never received).
    pub nonce: u64,
    pub sender: String,
    /// The queue it was first sent to, if moved here as dead.
    pub dead_from: Option<String>,
}

/// A message to send.
#[derive(Debug, Clone, Default)]
pub struct NewMsg {
    pub body: String,
    pub delay: Option<u32>,
    pub attrs: Vec<(String, MsgAttr)>,
    pub sys_attrs: Vec<(String, MsgAttr)>,
    pub sender: String,
    /// Kept from a move to a dead-letter queue.
    pub sent_ms: Option<u64>,
    pub receive_count: u32,
    pub first_receive_ms: u64,
    pub dead_from: Option<String>,
}

/// A message sent.
#[derive(Debug, Clone)]
pub struct Sent {
    pub id: String,
    pub md5_body: String,
    pub md5_attrs: Option<String>,
    pub md5_sys: Option<String>,
}

/// A message received.
#[derive(Debug, Clone)]
pub struct Received {
    pub msg: Msg,
    pub receipt: String,
}

/// A message received too often: to move to the dead-letter queue.
#[derive(Debug, Clone)]
pub struct Dead {
    pub seq: u64,
    pub msg: Msg,
}

/// Valid message characters (XML's).
fn valid_chars(s: &str) -> bool {
    s.chars().all(|c| {
        matches!(c, '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
    })
}

fn check_attrs(attrs: &[(String, MsgAttr)], system: bool) -> Result<()> {
    if attrs.len() > MAX_ATTRIBUTES {
        return Err(invalid(
            "InvalidParameterValue",
            "Number of message attributes exceeds the allowed maximum of 10.",
        ));
    }
    for (n, a) in attrs {
        let bad = |m: String| Err(invalid("InvalidParameterValue", m));
        if system && n != "AWSTraceHeader" {
            return bad(format!("Message system attribute {} isn't supported", n));
        }
        if n.is_empty()
            || n.len() > 256
            || n.starts_with('.')
            || n.ends_with('.')
            || n.contains("..")
            || n.to_ascii_lowercase().starts_with("aws.") && !system
            || n.to_ascii_lowercase().starts_with("amazon.")
            || !n
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        {
            return bad(format!("Message attribute name '{}' is invalid.", n));
        }
        let base = a.data_type.split('.').next().unwrap_or("");
        match base {
            "String" | "Number" => {
                let Some(s) = &a.string else {
                    return bad(format!(
                        "Message attribute '{}' must contain a non-empty value of message attribute type '{}'.",
                        n, a.data_type
                    ));
                };
                if s.is_empty() || !valid_chars(s) {
                    return bad(format!("Message attribute '{}' has an invalid value.", n));
                }
                if base == "Number" && s.trim().parse::<f64>().is_err() {
                    return bad(format!(
                        "Can't cast the value of message (user) attribute '{}' to a number.",
                        n
                    ));
                }
            }
            "Binary" => {
                if a.binary.as_ref().is_none_or(|b| b.is_empty()) {
                    return bad(format!(
                        "Message attribute '{}' must contain a non-empty value of message attribute type 'Binary'.",
                        n
                    ));
                }
            }
            _ => {
                return bad(format!(
                    "The type of message (user) attribute '{}' is invalid.",
                    n
                ));
            }
        }
    }
    Ok(())
}

fn q_key(seq: u64) -> Vec<u8> {
    let mut k = vec![b'Q'];
    k.extend(seq.to_be_bytes());
    k
}

fn v_key(at: u64, seq: u64) -> Vec<u8> {
    let mut k = vec![b'V'];
    k.extend(at.to_be_bytes());
    k.extend(seq.to_be_bytes());
    k
}

fn v_parts(k: &[u8]) -> Option<(u64, u64)> {
    if k.len() != 17 || k[0] != b'V' {
        return None;
    }
    Some((
        u64::from_be_bytes(k[1..9].try_into().ok()?),
        u64::from_be_bytes(k[9..17].try_into().ok()?),
    ))
}

pub fn encode_receipt(seq: u64, nonce: u64) -> String {
    use base64::Engine;
    let mut b = Vec::with_capacity(16);
    b.extend(seq.to_be_bytes());
    b.extend(nonce.to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

pub fn decode_receipt(r: &str) -> Result<(u64, u64)> {
    use base64::Engine;
    let bad = || {
        SqsError::ReceiptHandleIsInvalid(format!(
            "The input receipt handle \"{}\" is not a valid receipt handle.",
            r
        ))
    };
    let b = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(r)
        .map_err(|_| bad())?;
    if b.len() != 16 {
        return Err(bad());
    }
    Ok((
        u64::from_be_bytes(b[..8].try_into().unwrap()),
        u64::from_be_bytes(b[8..].try_into().unwrap()),
    ))
}

/// A new message id, as a (version 4) UUID.
fn message_id(seq: u64) -> String {
    let a = crate::fs::new_guid(seq);
    let b = crate::fs::new_guid(seq ^ a.rotate_left(17));
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        a >> 32,
        (a >> 16) & 0xffff,
        a & 0xfff,
        0x8000 | (b >> 48) & 0x3fff,
        b & 0xffff_ffff_ffff
    )
}

fn nonce(seq: u64) -> u64 {
    crate::fs::new_guid(seq).max(1)
}

fn now_ms() -> u64 {
    crate::cluster::now_ms()
}

/// A queue, served from this node.
pub struct Queue<D: BlockDevice> {
    pub fs: Arc<FileSystem<D>>,
    pub name: String,
    stopped: AtomicBool,
    settler: Settler,
    /// Signalled when messages may have become receivable.
    arrived: tokio::sync::Notify,
}

impl<D: BlockDevice + 'static> Queue<D> {
    /// Serve queue `name` of `fs`.
    pub async fn open(
        fs: Arc<FileSystem<D>>,
        name: &str,
    ) -> std::result::Result<Arc<Self>, FileSystemError> {
        fs.open_btree(name, DatasetFormat::Queue).await?;
        let q = Arc::new(Queue {
            fs,
            name: name.to_string(),
            stopped: AtomicBool::new(false),
            settler: Settler::default(),
            arrived: tokio::sync::Notify::new(),
        });
        // A new queue: its meta.
        q.write(|t| {
            Box::pin(async move {
                if t.get(b"M").await?.is_none() {
                    let now = now_ms();
                    let m = Meta {
                        next_seq: 1,
                        created_ms: now,
                        modified_ms: now,
                        ..Default::default()
                    };
                    t.put(b"M", &bincode::serialize(&m)?).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| FileSystemError::Unavailable(format!("{:?}", e)))?;
        let weak = Arc::downgrade(&q);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let Some(q) = weak.upgrade() else { return };
                if q.is_stopped() {
                    return;
                }
                if let Err(e) = q.sweep().await
                    && e != SqsError::Unavailable
                {
                    eprintln!("sqs '{}': deleting old messages: {:?}", q.name, e);
                }
            }
        });
        Ok(q)
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.arrived.notify_waiters();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    async fn ds(&self) -> Result<Arc<Dataset>> {
        if self.is_stopped() {
            return Err(SqsError::Unavailable);
        }
        Ok(self.fs.open_btree(&self.name, DatasetFormat::Queue).await?)
    }

    /// Run a logged write, and make it durable.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a, 'b> FnOnce(
            &'a mut Txn<'b, D>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
    {
        let ds = self.ds().await?;
        let out = {
            let _w = ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &ds.obj).await?;
            let out = f(&mut t).await?;
            if !t.is_empty() {
                t.apply_logged()?;
                self.settler.applied();
            }
            out
        };
        self.settler.settle(&self.fs, &self.name).await?;
        Ok(out)
    }

    /// Run a read (of durable state only).
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a> FnOnce(
            &'a Txn<'a, D>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
    {
        let ds = self.ds().await?;
        let _r = ds.ops.read().await;
        self.settler.settle(&self.fs, &self.name).await?;
        let t = Txn::begin(&self.fs, &ds.obj).await?;
        f(&t).await
    }

    pub async fn meta(&self) -> Result<Meta> {
        self.read(|t| Box::pin(async move { meta_of(t).await }))
            .await
    }

    /// Set attributes (by SQS name), all or none.
    pub async fn set_attrs(&self, attrs: Vec<(String, String)>) -> Result<()> {
        self.write(move |t| {
            Box::pin(async move {
                let mut m = meta_of(t).await?;
                for (k, v) in &attrs {
                    m.attrs.set(k, v)?;
                }
                m.modified_ms = now_ms();
                put_meta(t, &m).await
            })
        })
        .await?;
        self.arrived.notify_waiters();
        Ok(())
    }

    pub async fn tag(&self, add: Vec<(String, String)>, remove: Vec<String>) -> Result<()> {
        self.write(move |t| {
            Box::pin(async move {
                let mut m = meta_of(t).await?;
                for k in &remove {
                    m.tags.remove(k);
                }
                for (k, v) in add {
                    m.tags.insert(k, v);
                }
                if m.tags.len() > 50 {
                    return Err(invalid(
                        "InvalidParameterValue",
                        "A queue can have at most 50 tags.",
                    ));
                }
                put_meta(t, &m).await
            })
        })
        .await
    }

    /// Send messages (in one transaction): each sent, or why not.
    pub async fn send(&self, msgs: Vec<NewMsg>) -> Result<Vec<Result<Sent>>> {
        let out = self
            .write(move |t| {
                Box::pin(async move {
                    let mut m = meta_of(t).await?;
                    let now = now_ms();
                    let mut out = Vec::with_capacity(msgs.len());
                    for n in msgs {
                        out.push(Self::send_one(t, &mut m, n, now).await?);
                    }
                    put_meta(t, &m).await?;
                    Ok(out)
                })
            })
            .await?;
        self.arrived.notify_waiters();
        Ok(out)
    }

    async fn send_one(
        t: &mut Txn<'_, D>,
        m: &mut Meta,
        n: NewMsg,
        now: u64,
    ) -> Result<Result<Sent>> {
        if n.body.is_empty() {
            return Ok(Err(invalid(
                "MissingParameter",
                "The request must contain the parameter MessageBody.",
            )));
        }
        if !valid_chars(&n.body) {
            return Ok(Err(invalid(
                "InvalidMessageContents",
                "Invalid characters found in the message body.",
            )));
        }
        if let Err(e) = check_attrs(&n.attrs, false).and(check_attrs(&n.sys_attrs, true)) {
            return Ok(Err(e));
        }
        let size = n.body.len() + n.attrs.iter().map(|(k, a)| a.size(k)).sum::<usize>();
        if size > m.attrs.maximum_message_size as usize {
            return Ok(Err(invalid(
                "InvalidParameterValue",
                format!(
                    "One or more parameters are invalid. Reason: Message must be shorter than {} bytes.",
                    m.attrs.maximum_message_size
                ),
            )));
        }
        let delay = n.delay.unwrap_or(m.attrs.delay_seconds);
        if delay > MAX_DELAY {
            return Ok(Err(invalid(
                "InvalidParameterValue",
                "Value for parameter DelaySeconds is invalid. Reason: must be between 0 and 900.",
            )));
        }
        let seq = m.next_seq;
        m.next_seq += 1;
        m.messages += 1;
        let visible = now + delay as u64 * 1000;
        let msg = Msg {
            id: message_id(seq),
            body: n.body,
            attrs: n.attrs,
            sys_attrs: n.sys_attrs,
            sent_ms: n.sent_ms.unwrap_or(now),
            first_receive_ms: n.first_receive_ms,
            receive_count: n.receive_count,
            visible_at_ms: visible,
            nonce: 0,
            sender: n.sender,
            dead_from: n.dead_from,
        };
        t.put(&q_key(seq), &bincode::serialize(&msg)?).await?;
        t.put(&v_key(visible, seq), &[0]).await?;
        Ok(Ok(Sent {
            id: msg.id.clone(),
            md5_body: md5_hex(msg.body.as_bytes()),
            md5_attrs: md5_of_attrs(&msg.attrs),
            md5_sys: md5_of_attrs(&msg.sys_attrs),
        }))
    }

    /// Receive up to `max` messages now, hiding them for `visibility`
    /// seconds (default: the queue's). Messages received more often than
    /// the redrive policy allows come back as dead instead (hidden a
    /// while), for the caller to move.
    pub async fn receive(
        &self,
        max: usize,
        visibility: Option<u32>,
    ) -> Result<(Vec<Received>, Vec<Dead>)> {
        let out = self
            .write(move |t| {
                Box::pin(async move {
                    let mut m = meta_of(t).await?;
                    let now = now_ms();
                    let vis = visibility.unwrap_or(m.attrs.visibility_timeout) as u64 * 1000;
                    let retention = m.attrs.message_retention_period as u64 * 1000;
                    let redrive = m.attrs.redrive();
                    let mut got = Vec::new();
                    let mut dead = Vec::new();
                    let mut changed = false;
                    let hi = v_key(now, u64::MAX);
                    let mut from = vec![b'V'];
                    'scan: while got.len() < max {
                        let page = t
                            .range(Bound::Excluded(&from[..]), Bound::Included(&hi[..]), 64)
                            .await?;
                        if page.is_empty() {
                            break;
                        }
                        for (k, _) in page {
                            from = k.clone();
                            let Some((_, seq)) = v_parts(&k) else {
                                continue;
                            };
                            t.delete(&k).await?;
                            changed = true;
                            let Some(raw) = t.get(&q_key(seq)).await? else {
                                continue;
                            };
                            let mut msg: Msg = bincode::deserialize(&raw)?;
                            if msg.sent_ms + retention <= now {
                                t.delete(&q_key(seq)).await?;
                                m.messages = m.messages.saturating_sub(1);
                                continue;
                            }
                            if let Some(r) = &redrive
                                && msg.receive_count >= r.max_receive_count
                            {
                                msg.visible_at_ms = now + MOVE_HIDE_MS;
                                t.put(&q_key(seq), &bincode::serialize(&msg)?).await?;
                                t.put(&v_key(msg.visible_at_ms, seq), &[1]).await?;
                                dead.push(Dead { seq, msg });
                                continue;
                            }
                            msg.receive_count += 1;
                            if msg.first_receive_ms == 0 {
                                msg.first_receive_ms = now;
                            }
                            msg.nonce = nonce(seq ^ now);
                            msg.visible_at_ms = now + vis;
                            t.put(&q_key(seq), &bincode::serialize(&msg)?).await?;
                            t.put(&v_key(msg.visible_at_ms, seq), &[1]).await?;
                            got.push(Received {
                                receipt: encode_receipt(seq, msg.nonce),
                                msg,
                            });
                            if got.len() >= max {
                                break 'scan;
                            }
                        }
                    }
                    if changed {
                        put_meta(t, &m).await?;
                    }
                    Ok((got, dead))
                })
            })
            .await?;
        Ok(out)
    }

    /// `receive`, waiting up to `wait` seconds for a message.
    pub async fn receive_wait(
        &self,
        max: usize,
        visibility: Option<u32>,
        wait: Option<u32>,
    ) -> Result<(Vec<Received>, Vec<Dead>)> {
        let wait = match wait {
            Some(w) => w,
            None => self.meta().await?.attrs.receive_message_wait_time_seconds,
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait as u64);
        loop {
            let arrived = self.arrived.notified();
            let (got, dead) = self.receive(max, visibility).await?;
            if !got.is_empty() || !dead.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok((got, dead));
            }
            // Woken by a send, or recheck now and then (hidden messages
            // becoming visible again).
            let next = (tokio::time::Instant::now() + Duration::from_millis(500)).min(deadline);
            let _ = tokio::time::timeout_at(next, arrived).await;
            if self.is_stopped() {
                return Err(SqsError::Unavailable);
            }
        }
    }

    /// Delete messages by receipt: each done (a stale or used receipt is a
    /// no-op), or why not.
    pub async fn delete(&self, receipts: Vec<String>) -> Result<Vec<Result<()>>> {
        self.write(move |t| {
            Box::pin(async move {
                let mut m = meta_of(t).await?;
                let mut out = Vec::new();
                let mut changed = false;
                for r in receipts {
                    let (seq, nonce) = match decode_receipt(&r) {
                        Ok(x) => x,
                        Err(e) => {
                            out.push(Err(e));
                            continue;
                        }
                    };
                    if let Some(raw) = t.get(&q_key(seq)).await? {
                        let msg: Msg = bincode::deserialize(&raw)?;
                        if msg.nonce == nonce {
                            t.delete(&q_key(seq)).await?;
                            t.delete(&v_key(msg.visible_at_ms, seq)).await?;
                            m.messages = m.messages.saturating_sub(1);
                            changed = true;
                        }
                    }
                    out.push(Ok(()));
                }
                if changed {
                    put_meta(t, &m).await?;
                }
                Ok(out)
            })
        })
        .await
    }

    /// Delete one message by seq (moved to a dead-letter queue), unless it
    /// was received again since.
    pub async fn remove(&self, seq: u64, nonce: u64) -> Result<()> {
        self.write(move |t| {
            Box::pin(async move {
                if let Some(raw) = t.get(&q_key(seq)).await? {
                    let msg: Msg = bincode::deserialize(&raw)?;
                    if msg.nonce == nonce {
                        let mut m = meta_of(t).await?;
                        t.delete(&q_key(seq)).await?;
                        t.delete(&v_key(msg.visible_at_ms, seq)).await?;
                        m.messages = m.messages.saturating_sub(1);
                        put_meta(t, &m).await?;
                    }
                }
                Ok(())
            })
        })
        .await
    }

    /// Change messages' visibility timeouts (from now), by receipt.
    pub async fn change_visibility(&self, entries: Vec<(String, u32)>) -> Result<Vec<Result<()>>> {
        let out = self
            .write(move |t| {
                Box::pin(async move {
                    let now = now_ms();
                    let mut out = Vec::new();
                    for (r, secs) in entries {
                        if secs > MAX_VISIBILITY {
                            out.push(Err(invalid(
                                "InvalidParameterValue",
                                "Value for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200.",
                            )));
                            continue;
                        }
                        let (seq, nonce) = match decode_receipt(&r) {
                            Ok(x) => x,
                            Err(e) => {
                                out.push(Err(e));
                                continue;
                            }
                        };
                        let Some(raw) = t.get(&q_key(seq)).await? else {
                            out.push(Err(SqsError::ReceiptHandleIsInvalid(
                                "The receipt handle has expired.".into(),
                            )));
                            continue;
                        };
                        let mut msg: Msg = bincode::deserialize(&raw)?;
                        if msg.nonce != nonce {
                            out.push(Err(SqsError::ReceiptHandleIsInvalid(
                                "The receipt handle has expired.".into(),
                            )));
                            continue;
                        }
                        if msg.visible_at_ms <= now {
                            out.push(Err(SqsError::MessageNotInflight));
                            continue;
                        }
                        t.delete(&v_key(msg.visible_at_ms, seq)).await?;
                        msg.visible_at_ms = now + secs as u64 * 1000;
                        t.put(&q_key(seq), &bincode::serialize(&msg)?).await?;
                        t.put(&v_key(msg.visible_at_ms, seq), &[1]).await?;
                        out.push(Ok(()));
                    }
                    Ok(out)
                })
            })
            .await?;
        self.arrived.notify_waiters();
        Ok(out)
    }

    /// Delete every message.
    pub async fn purge(&self) -> Result<()> {
        loop {
            let more = self
                .write(|t| {
                    Box::pin(async move {
                        let mut m = meta_of(t).await?;
                        let page = t
                            .range(Bound::Included(b"Q"), Bound::Excluded(b"R"), 512)
                            .await?;
                        for (k, raw) in &page {
                            let msg: Msg = bincode::deserialize(raw)?;
                            let seq = u64::from_be_bytes(k[1..9].try_into().unwrap());
                            t.delete(k).await?;
                            t.delete(&v_key(msg.visible_at_ms, seq)).await?;
                            m.messages = m.messages.saturating_sub(1);
                        }
                        if page.is_empty() {
                            // Stray index entries.
                            for (k, _) in t
                                .range(Bound::Included(b"V"), Bound::Excluded(b"W"), 512)
                                .await?
                            {
                                t.delete(&k).await?;
                            }
                            m.messages = 0;
                        }
                        put_meta(t, &m).await?;
                        Ok(!page.is_empty())
                    })
                })
                .await?;
            if !more {
                return Ok(());
            }
        }
    }

    /// (visible, in flight, delayed) messages, roughly.
    pub async fn counts(&self) -> Result<(u64, u64, u64)> {
        self.read(|t| {
            Box::pin(async move {
                let now = now_ms();
                let (mut visible, mut inflight, mut delayed) = (0, 0, 0);
                let mut from = vec![b'V'];
                loop {
                    let page = t
                        .range(Bound::Excluded(&from[..]), Bound::Excluded(b"W"), 1024)
                        .await?;
                    let Some((last, _)) = page.last() else { break };
                    from = last.clone();
                    for (k, v) in &page {
                        let Some((at, _)) = v_parts(k) else { continue };
                        if at <= now {
                            visible += 1;
                        } else if v.first() == Some(&1) {
                            inflight += 1;
                        } else {
                            delayed += 1;
                        }
                    }
                }
                Ok((visible, inflight, delayed))
            })
        })
        .await
    }

    /// Delete messages past the retention period (the oldest first; stops
    /// at the first one that isn't).
    pub async fn sweep(&self) -> Result<u64> {
        let mut total = 0;
        loop {
            let (n, more) = self
                .write(|t| {
                    Box::pin(async move {
                        let mut m = meta_of(t).await?;
                        let now = now_ms();
                        let retention = m.attrs.message_retention_period as u64 * 1000;
                        let page = t
                            .range(Bound::Included(b"Q"), Bound::Excluded(b"R"), 256)
                            .await?;
                        let mut n = 0;
                        let mut more = !page.is_empty();
                        for (k, raw) in &page {
                            let msg: Msg = bincode::deserialize(raw)?;
                            if msg.sent_ms + retention > now {
                                more = false;
                                break;
                            }
                            let seq = u64::from_be_bytes(k[1..9].try_into().unwrap());
                            t.delete(k).await?;
                            t.delete(&v_key(msg.visible_at_ms, seq)).await?;
                            m.messages = m.messages.saturating_sub(1);
                            n += 1;
                        }
                        if n > 0 {
                            put_meta(t, &m).await?;
                        }
                        Ok((n, more && n > 0))
                    })
                })
                .await?;
            total += n;
            if !more {
                return Ok(total);
            }
        }
    }
}

async fn meta_of<D: BlockDevice>(t: &Txn<'_, D>) -> Result<Meta> {
    match t.get(b"M").await? {
        Some(raw) => Ok(bincode::deserialize(&raw)?),
        None => Ok(Meta {
            next_seq: 1,
            ..Default::default()
        }),
    }
}

async fn put_meta<D: BlockDevice>(t: &mut Txn<'_, D>, m: &Meta) -> Result<()> {
    t.put(b"M", &bincode::serialize(m)?).await?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/sqs_tests.rs"]
mod tests;
