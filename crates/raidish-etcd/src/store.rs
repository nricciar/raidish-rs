//! etcd's multi-version keyspace over a B+tree dataset (the layout is in
//! the module docs, `crate::etcd`): ranges at any revision since the last
//! compaction, puts, deletes, transactions, compaction, leases, and the
//! event log watchers read.

use super::pb::etcdserverpb as pb;
use super::pb::mvccpb;
use crate::btree::Txn;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::PoolDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::service_tree::spawn_while_alive;
use pb::compare::{CompareResult, CompareTarget, TargetUnion};
use pb::range_request::{SortOrder, SortTarget};
use pb::request_op::Request;
use pb::response_op::Response;
use raidish_core::role::Mutable;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// The revision of an empty keyspace (etcd's: the first change is 2).
const FIRST_REV: i64 = 1;
/// Most operations in one transaction (etcd's default `--max-txn-ops`).
pub const MAX_TXN_OPS: usize = 128;
/// The largest request (etcd's default `--max-request-bytes`).
pub const MAX_REQUEST_BYTES: usize = 3 * 512 * 1024;
/// The shortest lease (etcd's, with its default election timeout).
pub const MIN_LEASE_TTL: i64 = 2;
/// The longest lease.
pub const MAX_LEASE_TTL: i64 = 9_000_000_000;
/// Entries read from the tree at a time when scanning.
const SCAN_CHUNK: usize = 1024;
/// Deletes per transaction while compacting.
const COMPACT_BATCH: usize = 512;

/// What a request can fail with: etcd's errors (clients match on their
/// codes and messages), or the pool's.
#[derive(Debug)]
pub enum EtcdError {
    Compacted,
    FutureRev,
    LeaseNotFound,
    LeaseExists,
    TtlTooLarge,
    DuplicateKey,
    EmptyKey,
    KeyNotFound,
    ValueProvided,
    LeaseProvided,
    TooManyOps,
    /// A key longer than the tree takes.
    KeyTooLong(usize),
    /// This node doesn't serve the dataset (any more).
    NotServing,
    Fs(FileSystemError),
}

impl From<FileSystemError> for EtcdError {
    fn from(e: FileSystemError) -> Self {
        EtcdError::Fs(e)
    }
}

impl std::fmt::Display for EtcdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = match self {
            EtcdError::Compacted => "etcdserver: mvcc: required revision has been compacted",
            EtcdError::FutureRev => "etcdserver: mvcc: required revision is a future revision",
            EtcdError::LeaseNotFound => "etcdserver: requested lease not found",
            EtcdError::LeaseExists => "etcdserver: lease already exists",
            EtcdError::TtlTooLarge => "etcdserver: too large lease TTL",
            EtcdError::DuplicateKey => "etcdserver: duplicate key given in txn request",
            EtcdError::EmptyKey => "etcdserver: key is not provided",
            EtcdError::KeyNotFound => "etcdserver: key not found",
            EtcdError::ValueProvided => "etcdserver: value is provided",
            EtcdError::LeaseProvided => "etcdserver: lease is provided",
            EtcdError::TooManyOps => "etcdserver: too many operations in txn request",
            EtcdError::KeyTooLong(n) => {
                return write!(f, "etcdserver: key is too long (at most {} bytes)", n);
            }
            EtcdError::NotServing => "etcdserver: no leader",
            EtcdError::Fs(e) => return write!(f, "etcdserver: {}", e),
        };
        f.write_str(m)
    }
}

impl EtcdError {
    /// As etcd answers it over gRPC.
    pub fn status(&self) -> tonic::Status {
        use tonic::Code;
        let code = match self {
            EtcdError::Compacted | EtcdError::FutureRev | EtcdError::TtlTooLarge => {
                Code::OutOfRange
            }
            EtcdError::LeaseNotFound => Code::NotFound,
            EtcdError::LeaseExists => Code::FailedPrecondition,
            EtcdError::DuplicateKey
            | EtcdError::EmptyKey
            | EtcdError::KeyNotFound
            | EtcdError::ValueProvided
            | EtcdError::LeaseProvided
            | EtcdError::TooManyOps
            | EtcdError::KeyTooLong(_) => Code::InvalidArgument,
            EtcdError::NotServing => Code::Unavailable,
            EtcdError::Fs(e) => {
                use crate::types::ErrorKind as K;
                match e.kind() {
                    K::Invalid | K::NotDir | K::NotFile => Code::InvalidArgument,
                    K::NotFound => Code::NotFound,
                    K::Exists => Code::AlreadyExists,
                    K::NotEmpty => Code::FailedPrecondition,
                    K::NoSpace => Code::ResourceExhausted,
                    K::Forbidden => Code::PermissionDenied,
                    K::Internal => Code::Internal,
                    // Its lease is someone else's, or no committer: retry.
                    K::Conflict | K::Unavailable => Code::Unavailable,
                }
            }
        };
        tonic::Status::new(code, self.to_string())
    }
}

pub type Result<T> = std::result::Result<T, EtcdError>;

// ---- the layout -------------------------------------------------------

const META: &[u8] = b"M";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Meta {
    /// The current revision.
    rev: i64,
    /// Revisions below this one are gone.
    compact: i64,
    /// A compaction in progress: its revision and how far it got (the
    /// next tree key to look at).
    compacting: Option<(i64, Vec<u8>)>,
    /// The next lease id to try.
    next_lease: i64,
}

impl Default for Meta {
    fn default() -> Self {
        Meta {
            rev: FIRST_REV,
            compact: 0,
            compacting: None,
            next_lease: 0,
        }
    }
}

/// A live key (`K`): where its current value is in its history.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
struct Current {
    create: i64,
    /// Its revision: the history entry holding its value.
    main: i64,
    sub: i64,
    version: i64,
    lease: i64,
}

/// A history entry (`H`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum Hist {
    Put {
        create: i64,
        version: i64,
        lease: i64,
        value: Vec<u8>,
    },
    Tombstone,
}

/// An event (`R`): the key that changed, and its previous revision.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Change {
    key: Vec<u8>,
    prev: Option<(i64, i64)>,
}

/// A lease (`L`).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
struct LeaseRec {
    ttl: i64,
}

use crate::service_tree::enc_bin as enc;

fn dec<T: for<'a> Deserialize<'a>>(b: &[u8], what: &str) -> Result<T> {
    Ok(crate::service_tree::dec_bin(b, &format!("etcd {}", what))?)
}

fn be(v: i64) -> [u8; 8] {
    (v as u64).to_be_bytes()
}

fn rd_be(b: &[u8]) -> i64 {
    u64::from_be_bytes(b[..8].try_into().unwrap()) as i64
}

fn k_key(key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(key.len() + 1);
    k.push(b'K');
    k.extend_from_slice(key);
    k
}

/// `key`, escaped: ordered as the keys are and prefix-free once
/// terminated (0x00 → 0x00 0xff; the terminator is 0x00 0x01).
fn esc(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 4);
    for &b in key {
        out.push(b);
        if b == 0 {
            out.push(0xff);
        }
    }
    out
}

/// A key's history: every `H` entry of it starts with this.
fn h_prefix(key: &[u8]) -> Vec<u8> {
    let mut k = vec![b'H'];
    k.extend(esc(key));
    k.extend([0, 1]);
    k
}

fn h_key(key: &[u8], main: i64, sub: i64) -> Vec<u8> {
    let mut k = h_prefix(key);
    k.extend(be(main));
    k.extend(be(sub));
    k
}

/// The (key, main, sub) of an `H` entry's tree key.
fn parse_h(k: &[u8]) -> Option<(Vec<u8>, i64, i64)> {
    if k.len() < 1 + 2 + 16 || k[0] != b'H' {
        return None;
    }
    let (body, revs) = k[1..].split_at(k.len() - 1 - 16);
    let body = body.strip_suffix(&[0, 1])?;
    let mut key = Vec::with_capacity(body.len());
    let mut i = 0;
    while i < body.len() {
        key.push(body[i]);
        if body[i] == 0 {
            if body.get(i + 1) != Some(&0xff) {
                return None;
            }
            i += 1;
        }
        i += 1;
    }
    Some((key, rd_be(&revs[..8]), rd_be(&revs[8..])))
}

fn r_key(main: i64, sub: i64) -> Vec<u8> {
    let mut k = vec![b'R'];
    k.extend(be(main));
    k.extend(be(sub));
    k
}

fn l_key(id: i64) -> Vec<u8> {
    let mut k = vec![b'L'];
    k.extend(be(id));
    k
}

fn a_prefix(id: i64) -> Vec<u8> {
    let mut k = vec![b'A'];
    k.extend(be(id));
    k
}

fn a_key(id: i64, key: &[u8]) -> Vec<u8> {
    let mut k = a_prefix(id);
    k.extend_from_slice(key);
    k
}

/// A range of keys as etcd asks for it: `range_end` empty is `key` alone,
/// "\0" every key from `key` on, otherwise `[key, range_end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Vec<u8>,
    /// None: `start` alone. Some(None): unbounded.
    pub end: Option<Option<Vec<u8>>>,
}

impl KeyRange {
    pub fn new(key: &[u8], range_end: &[u8]) -> Self {
        let end = match range_end {
            [] => None,
            [0] => Some(None),
            e => Some(Some(e.to_vec())),
        };
        KeyRange {
            start: key.to_vec(),
            end,
        }
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        match &self.end {
            None => k == self.start.as_slice(),
            Some(None) => k >= self.start.as_slice(),
            Some(Some(e)) => k >= self.start.as_slice() && k < e.as_slice(),
        }
    }

    /// Whether it is a range at all (not one key).
    fn is_range(&self) -> bool {
        self.end.is_some()
    }

    /// The tree keys of its live keys.
    fn current_bounds(&self) -> (Vec<u8>, Vec<u8>) {
        let hi = match &self.end {
            Some(Some(e)) => k_key(e),
            _ => b"L".to_vec(),
        };
        (k_key(&self.start), hi)
    }

    /// The tree keys of its history.
    fn history_bounds(&self) -> (Vec<u8>, Vec<u8>) {
        let mut lo = vec![b'H'];
        lo.extend(esc(&self.start));
        let hi = match &self.end {
            Some(Some(e)) => {
                let mut h = vec![b'H'];
                h.extend(esc(e));
                h
            }
            _ => b"I".to_vec(),
        };
        (lo, hi)
    }
}

// ---- the store --------------------------------------------------------

/// Where the store's writes are: every one applied, and every one durable.
/// A write is a change to the tree (one tree transaction); `rev` is the
/// keyspace's revision once it was applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Mark {
    writes: u64,
    rev: i64,
}

/// One etcd keyspace, served from this node.
pub struct EtcdStore<D: PoolDevice, R: Mutable> {
    fs: Arc<FileSystem<D, R>>,
    pub name: crate::types::DatasetName,
    pub cluster_id: u64,
    pub member_id: u64,
    /// The `ds/<name>` lease's epoch: the "raft term" clients see.
    pub term: u64,
    applied: Mutex<Mark>,
    durable: watch::Sender<Mark>,
    /// Live leases: their TTL and deadline. Not stored: a new server
    /// starts every lease afresh (as etcd does on a leader change).
    leases: Mutex<HashMap<i64, (i64, Instant)>>,
    /// The revision compacted up to so far.
    compacted: watch::Sender<i64>,
    compactor: Arc<tokio::sync::Notify>,
    stopped: watch::Sender<bool>,
}

impl<D: PoolDevice + 'static, R: Mutable> crate::service_tree::Served for EtcdStore<D, R> {
    fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }
}

/// A request's view of the header fields.
fn header_of<D: PoolDevice, R: Mutable>(
    s: &EtcdStore<D, R>,
    rev: i64,
) -> Option<pb::ResponseHeader> {
    Some(pb::ResponseHeader {
        cluster_id: s.cluster_id,
        member_id: s.member_id,
        revision: rev,
        raft_term: s.term,
    })
}

impl<D: PoolDevice + 'static, R: Mutable> EtcdStore<D, R> {
    /// Serve etcd dataset `name` of `fs` (opened as its writer, or as the
    /// committer in tests). Leases start afresh.
    pub async fn open(
        fs: Arc<FileSystem<D, R>>,
        name: &crate::types::DatasetName,
        member_id: u64,
        term: u64,
    ) -> Result<Arc<Self>> {
        let entry = fs.dataset_entry(name).await?;
        if entry.format != DatasetFormat::Etcd {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset, not etcd",
                name,
                entry.format.name()
            ))
            .into());
        }
        let ds = fs.open_btree(name, DatasetFormat::Etcd).await?;
        let (meta, leases) = {
            let _r = ds.ops.read().await;
            let t = Txn::begin(&fs, &ds.obj).await?;
            let meta = read_meta(&t).await?;
            let mut leases = HashMap::new();
            let now = Instant::now();
            for (k, v) in scan(&t, b"L".to_vec(), b"M".to_vec()).await? {
                let l: LeaseRec = dec(&v, "lease")?;
                leases.insert(rd_be(&k[1..]), (l.ttl, now + secs(l.ttl)));
            }
            (meta, leases)
        };
        let mark = Mark {
            writes: 0,
            rev: meta.rev,
        };
        let s = Arc::new(EtcdStore {
            fs,
            name: name.clone(),
            cluster_id: entry.guid,
            member_id,
            term,
            applied: Mutex::new(mark),
            durable: watch::Sender::new(mark),
            leases: Mutex::new(leases),
            compacted: watch::Sender::new(if meta.compacting.is_some() {
                0
            } else {
                meta.compact
            }),
            compactor: Arc::new(tokio::sync::Notify::new()),
            stopped: watch::Sender::new(false),
        });
        // Background work, for as long as the store is served.
        let wake = Some(s.compactor.clone());
        spawn_while_alive(&s, Duration::from_secs(5), wake, |s| async move {
            if let Err(e) = s.compact_some().await {
                eprintln!("etcd '{}': compacting: {}", s.name, e);
            }
        });
        spawn_while_alive(&s, Duration::from_millis(500), None, |s| async move {
            if let Err(e) = s.expire_leases().await {
                eprintln!("etcd '{}': expiring leases: {}", s.name, e);
            }
        });
        Ok(s)
    }

    /// Stop serving: requests fail from now on, watches end.
    pub fn stop(&self) {
        self.stopped.send_replace(true);
        self.durable.send_modify(|_| {});
        // A permit, not a wake-up: its loop may be mid-pass, not waiting.
        self.compactor.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// Set once the store stops.
    pub fn stopped(&self) -> watch::Receiver<bool> {
        self.stopped.subscribe()
    }

    /// The durable revision, as it moves.
    pub fn durable_rev(&self) -> watch::Receiver<i64> {
        // A receiver of revisions: map the mark's channel.
        let (tx, rx) = watch::channel(self.durable.borrow().rev);
        let mut marks = self.durable.subscribe();
        let mut stopped = self.stopped.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    r = marks.changed() => if r.is_err() { return },
                    _ = tx.closed() => return,
                    _ = stopped.changed() => return,
                }
                let rev = marks.borrow_and_update().rev;
                if tx.send(rev).is_err() {
                    return;
                }
            }
        });
        rx
    }

    pub fn header(&self, rev: i64) -> Option<pb::ResponseHeader> {
        header_of(self, rev)
    }

    async fn ds(&self) -> Result<Arc<Dataset>> {
        if self.is_stopped() {
            return Err(EtcdError::NotServing);
        }
        Ok(self.fs.open_btree(&self.name, DatasetFormat::Etcd).await?)
    }

    /// Make every applied write durable (an intent log flush) and confirm
    /// this node's lease is still provably fresh (H-11).
    async fn settle(&self) -> Result<()> {
        let target = *self.applied.lock().unwrap();
        if self.durable.borrow().writes >= target.writes {
            self.fs.check_lease_fresh(&self.name)?;
            return Ok(());
        }
        // In a task of its own: a client that gives up (its gRPC deadline
        // drops this request's future) doesn't cut the flush short; the
        // next request finds it done or waits on it.
        crate::service_tree::flush_spawned(&self.fs, &self.name).await?;
        self.durable.send_if_modified(|d| {
            if d.writes < target.writes {
                *d = target;
                true
            } else {
                false
            }
        });
        Ok(())
    }

    /// Wait for reads to see only durable writes, and confirm the lease is
    /// fresh. Called with the dataset's operations read-locked (no write is
    /// applied meanwhile).
    async fn read_barrier(&self) -> Result<()> {
        let applied = *self.applied.lock().unwrap();
        if self.durable.borrow().writes < applied.writes {
            self.settle().await?;
        } else {
            self.fs.check_lease_fresh(&self.name)?;
        }
        Ok(())
    }

    /// Run a write: apply it (one tree transaction), make it durable.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a, 'b> FnOnce(
            &'a mut Writer<'b, D, R>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
        T: Send,
    {
        let ds = self.ds().await?;
        let out = {
            let _w = ds.ops.write().await;
            let t = Txn::begin(&self.fs, &ds.obj).await?;
            let meta = read_meta(&t).await?;
            let mut w = Writer {
                max_key: t.max_key(),
                t,
                meta: meta.clone(),
                sub: 0,
                start_meta: meta,
                leases_added: Vec::new(),
                leases_removed: Vec::new(),
            };
            let out = f(&mut w).await?;
            let (changed, rev, added, removed) = w.finish().await?;
            if changed {
                let mut a = self.applied.lock().unwrap();
                a.writes += 1;
                a.rev = rev;
            }
            let mut l = self.leases.lock().unwrap();
            let now = Instant::now();
            for (id, ttl) in added {
                l.insert(id, (ttl, now + secs(ttl)));
            }
            for id in removed {
                l.remove(&id);
            }
            out
        };
        // Durable before it's answered (and so is anything it read).
        self.settle().await?;
        Ok(out)
    }

    /// Run a read at the durable state.
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a> FnOnce(
            &'a Txn<'a, D, R>,
            &'a Meta,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
    {
        let ds = self.ds().await?;
        let _r = ds.ops.read().await;
        self.read_barrier().await?;
        let t = Txn::begin(&self.fs, &ds.obj).await?;
        let meta = read_meta(&t).await?;
        let out = f(&t, &meta).await?;
        self.fs.check_lease_fresh(&self.name)?;
        Ok(out)
    }

    // ---- KV -----------------------------------------------------------

    pub async fn range(&self, r: pb::RangeRequest) -> Result<pb::RangeResponse> {
        if r.key.is_empty() {
            return Err(EtcdError::EmptyKey);
        }
        let (mut resp, rev) = self
            .read(|t, meta| {
                Box::pin(async move { range(t, meta, meta.rev, &r).await.map(|x| (x, meta.rev)) })
            })
            .await?;
        resp.header = self.header(rev);
        Ok(resp)
    }

    pub async fn put(&self, r: pb::PutRequest) -> Result<pb::PutResponse> {
        check_put(&r)?;
        let (mut resp, rev) = self
            .write(|w| {
                Box::pin(async move {
                    let p = w.put(&r).await?;
                    Ok((p, w.cur_rev()))
                })
            })
            .await?;
        resp.header = self.header(rev);
        Ok(resp)
    }

    pub async fn delete_range(&self, r: pb::DeleteRangeRequest) -> Result<pb::DeleteRangeResponse> {
        if r.key.is_empty() {
            return Err(EtcdError::EmptyKey);
        }
        let (mut resp, rev) = self
            .write(|w| {
                Box::pin(async move {
                    let d = w.delete_range(&r).await?;
                    Ok((d, w.cur_rev()))
                })
            })
            .await?;
        resp.header = self.header(rev);
        Ok(resp)
    }

    pub async fn txn(&self, r: pb::TxnRequest) -> Result<pb::TxnResponse> {
        check_txn(&r)?;
        let header = self.header(0);
        let read_only = txn_is_read_only(&r);
        let (mut resp, rev) = if read_only {
            // No writes either way: a read (no flush needed).
            self.read(|t, meta| {
                Box::pin(async move {
                    let ok = compares_hold(t, &r.compare).await?;
                    let ops = if ok { &r.success } else { &r.failure };
                    let mut responses = Vec::new();
                    for op in ops {
                        if let Some(Request::RequestRange(q)) = &op.request {
                            let mut x = range(t, meta, meta.rev, q).await?;
                            x.header = header.map(|h| pb::ResponseHeader {
                                revision: meta.rev,
                                ..h
                            });
                            responses.push(pb::ResponseOp {
                                response: Some(Response::ResponseRange(x)),
                            });
                        }
                    }
                    Ok((
                        pb::TxnResponse {
                            header: None,
                            succeeded: ok,
                            responses,
                        },
                        meta.rev,
                    ))
                })
            })
            .await?
        } else {
            self.write(|w| {
                Box::pin(async move {
                    let x = w.txn(&r, &header).await?;
                    Ok((x, w.cur_rev()))
                })
            })
            .await?
        };
        resp.header = self.header(rev);
        fix_headers(&mut resp.responses, rev);
        Ok(resp)
    }

    /// Compact history below `rev`. With `physical`, return once it's done.
    pub async fn compact(&self, rev: i64, physical: bool) -> Result<pb::CompactionResponse> {
        let cur = self
            .write(|w| {
                Box::pin(async move {
                    if rev > w.meta.rev {
                        return Err(EtcdError::FutureRev);
                    }
                    if rev <= w.meta.compact {
                        return Err(EtcdError::Compacted);
                    }
                    w.meta.compact = rev;
                    w.meta.compacting = Some((rev, b"H".to_vec()));
                    Ok(w.meta.rev)
                })
            })
            .await?;
        self.compactor.notify_one();
        if physical {
            let mut done = self.compacted.subscribe();
            let mut stopped = self.stopped.subscribe();
            loop {
                if *done.borrow_and_update() >= rev {
                    break;
                }
                if *stopped.borrow_and_update() {
                    return Err(EtcdError::NotServing);
                }
                tokio::select! {
                    _ = done.changed() => {}
                    _ = stopped.changed() => {}
                }
            }
        }
        Ok(pb::CompactionResponse {
            header: self.header(cur),
        })
    }

    /// One step of the compaction in progress, if any; repeats until done.
    async fn compact_some(&self) -> Result<()> {
        loop {
            let done = self
                .write(|w| Box::pin(async move { w.compact_batch().await }))
                .await?;
            match done {
                None => return Ok(()),
                Some(Some(rev)) => {
                    self.compacted.send_replace(rev);
                    return Ok(());
                }
                Some(None) => {}
            }
            if self.is_stopped() {
                return Ok(());
            }
        }
    }

    // ---- leases -------------------------------------------------------

    pub async fn lease_grant(&self, r: pb::LeaseGrantRequest) -> Result<pb::LeaseGrantResponse> {
        if r.ttl > MAX_LEASE_TTL {
            return Err(EtcdError::TtlTooLarge);
        }
        let ttl = r.ttl.max(MIN_LEASE_TTL);
        let (id, rev) = self
            .write(|w| {
                Box::pin(async move {
                    let id = w.lease_grant(r.id, ttl).await?;
                    Ok((id, w.cur_rev()))
                })
            })
            .await?;
        Ok(pb::LeaseGrantResponse {
            header: self.header(rev),
            id,
            ttl,
            error: String::new(),
        })
    }

    pub async fn lease_revoke(&self, id: i64) -> Result<pb::LeaseRevokeResponse> {
        let rev = self
            .write(|w| {
                Box::pin(async move {
                    w.lease_revoke(id).await?;
                    Ok(w.cur_rev())
                })
            })
            .await?;
        Ok(pb::LeaseRevokeResponse {
            header: self.header(rev),
        })
    }

    /// Renew lease `id`: its TTL, 0 if there's no such lease.
    pub async fn lease_keep_alive(&self, id: i64) -> Result<pb::LeaseKeepAliveResponse> {
        if self.is_stopped() {
            return Err(EtcdError::NotServing);
        }
        // Renewing one that's just been granted must see it: wait for the
        // grant (any applied write) to be durable, as reads do.
        let ds = self.ds().await?;
        let _r = ds.ops.read().await;
        self.read_barrier().await?;
        self.fs.check_lease_fresh(&self.name)?;
        let rev = self.durable.borrow().rev;
        let ttl = {
            let mut l = self.leases.lock().unwrap();
            match l.get_mut(&id) {
                Some((ttl, deadline)) => {
                    *deadline = Instant::now() + secs(*ttl);
                    *ttl
                }
                None => 0,
            }
        };
        Ok(pb::LeaseKeepAliveResponse {
            header: self.header(rev),
            id,
            ttl,
        })
    }

    pub async fn lease_time_to_live(
        &self,
        r: pb::LeaseTimeToLiveRequest,
    ) -> Result<pb::LeaseTimeToLiveResponse> {
        let (keys, exists, rev) = self
            .read(|t, meta| {
                Box::pin(async move {
                    let exists = t.get(&l_key(r.id)).await?.is_some();
                    let mut keys = Vec::new();
                    if exists && r.keys {
                        let p = a_prefix(r.id);
                        let end = crate::kv::prefix_end(&p).unwrap();
                        for (k, _) in scan(t, p.clone(), end).await? {
                            keys.push(k[p.len()..].to_vec());
                        }
                    }
                    Ok((keys, exists, meta.rev))
                })
            })
            .await?;
        let entry = self.leases.lock().unwrap().get(&r.id).copied();
        let (ttl, granted) = match (exists, entry) {
            (true, Some((granted, deadline))) => {
                let left = deadline.saturating_duration_since(Instant::now());
                (left.as_secs() as i64, granted)
            }
            _ => (-1, 0),
        };
        Ok(pb::LeaseTimeToLiveResponse {
            header: self.header(rev),
            id: r.id,
            ttl,
            granted_ttl: granted,
            keys,
        })
    }

    pub async fn lease_leases(&self) -> Result<pb::LeaseLeasesResponse> {
        let (ids, rev) = self
            .read(|t, meta| {
                Box::pin(async move {
                    let mut ids = Vec::new();
                    for (k, _) in scan(t, b"L".to_vec(), b"M".to_vec()).await? {
                        ids.push(rd_be(&k[1..]));
                    }
                    Ok((ids, meta.rev))
                })
            })
            .await?;
        Ok(pb::LeaseLeasesResponse {
            header: self.header(rev),
            leases: ids.into_iter().map(|id| pb::LeaseStatus { id }).collect(),
        })
    }

    /// Revoke every lease past its deadline.
    async fn expire_leases(&self) -> Result<()> {
        let now = Instant::now();
        let expired: Vec<i64> = self
            .leases
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, d))| *d <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            match self.expire_lease(id).await {
                Ok(_) | Err(EtcdError::LeaseNotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Revoke lease `id` if it is still past its deadline, judged under
    /// the write lock: a keep-alive (which renews under the read lock)
    /// between the scan and now has saved it. Whether it was revoked.
    async fn expire_lease(&self, id: i64) -> Result<bool> {
        let leases = &self.leases;
        self.write(move |w| {
            let due = leases
                .lock()
                .unwrap()
                .get(&id)
                .is_none_or(|(_, d)| *d <= Instant::now());
            Box::pin(async move {
                if !due {
                    return Ok(false);
                }
                match w.lease_revoke(id).await {
                    Ok(()) => Ok(true),
                    // Gone from the store already: forget it here too.
                    Err(EtcdError::LeaseNotFound) => {
                        w.leases_removed.push(id);
                        Ok(true)
                    }
                    Err(e) => Err(e),
                }
            })
        })
        .await
    }

    // ---- watch --------------------------------------------------------

    /// The events of revisions `from..=to` (at most about `max`; whole
    /// revisions only) on keys in `range`, and the revision read up to.
    /// Errors with `Compacted` if `from` was compacted.
    pub async fn events(
        &self,
        range: &KeyRange,
        from: i64,
        to: i64,
        prev_kv: bool,
        max: usize,
    ) -> Result<(Vec<mvccpb::Event>, i64)> {
        let range = range.clone();
        self.read(|t, meta| {
            Box::pin(async move {
                if from < meta.compact {
                    return Err(EtcdError::Compacted);
                }
                let mut out: Vec<(i64, mvccpb::Event)> = Vec::new();
                let mut lo = r_key(from, 0);
                let hi = r_key(to + 1, 0);
                let mut upto = to;
                let mut first = true;
                'scan: loop {
                    let lo_b = if first {
                        Bound::Included(lo.as_slice())
                    } else {
                        Bound::Excluded(lo.as_slice())
                    };
                    first = false;
                    let page = t
                        .range(lo_b, Bound::Excluded(hi.as_slice()), SCAN_CHUNK)
                        .await?;
                    let n = page.len();
                    let Some((last, _)) = page.last() else { break };
                    let next = last.clone();
                    for (k, v) in page {
                        let (main, sub) = (rd_be(&k[1..9]), rd_be(&k[9..17]));
                        // Enough: stop at a revision boundary.
                        if out.len() >= max && out.last().is_some_and(|(m, _)| *m < main) {
                            upto = main - 1;
                            break 'scan;
                        }
                        let c: Change = dec(&v, "event")?;
                        if !range.contains(&c.key) {
                            continue;
                        }
                        let ev = event_at(t, &c, main, sub, prev_kv).await?;
                        out.push((main, ev));
                    }
                    if n < SCAN_CHUNK {
                        break;
                    }
                    lo = next;
                }
                Ok((out.into_iter().map(|(_, e)| e).collect(), upto))
            })
        })
        .await
    }

    /// The current and compacted revisions.
    pub async fn revisions(&self) -> Result<(i64, i64)> {
        self.read(|_, meta| Box::pin(async move { Ok((meta.rev, meta.compact)) }))
            .await
    }

    /// For Maintenance.Status: (bytes allocated, bytes in use) of the tree.
    pub async fn db_size(&self) -> Result<(i64, i64)> {
        let ds = self.ds().await?;
        let _r = ds.ops.read().await;
        let t = Txn::begin(&self.fs, &ds.obj).await?;
        let b = ds.blksz as i64;
        let all = t.header.next_id as i64 * b;
        Ok((all, all - t.header.free_total as i64 * b))
    }
}

fn secs(n: i64) -> Duration {
    Duration::from_secs(n.max(0) as u64)
}

/// Sub-responses carry the transaction's revision.
fn fix_headers(rs: &mut [pb::ResponseOp], rev: i64) {
    for r in rs {
        match &mut r.response {
            Some(Response::ResponseRange(x)) => set_rev(&mut x.header, rev),
            Some(Response::ResponsePut(x)) => set_rev(&mut x.header, rev),
            Some(Response::ResponseDeleteRange(x)) => set_rev(&mut x.header, rev),
            Some(Response::ResponseTxn(x)) => {
                set_rev(&mut x.header, rev);
                fix_headers(&mut x.responses, rev);
            }
            None => {}
        }
    }
}

fn set_rev(h: &mut Option<pb::ResponseHeader>, rev: i64) {
    if let Some(h) = h {
        h.revision = rev;
    }
}

async fn read_meta<D: PoolDevice, R: Mutable>(t: &Txn<'_, D, R>) -> Result<Meta> {
    match t.get(META).await? {
        None => Ok(Meta::default()),
        Some(b) => dec(&b, "meta"),
    }
}

/// Every entry with a tree key in `[lo, hi)`.
async fn scan<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    lo: Vec<u8>,
    hi: Vec<u8>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut from = Bound::Included(lo);
    loop {
        let b = match &from {
            Bound::Included(k) => Bound::Included(k.as_slice()),
            Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let page = t.range(b, Bound::Excluded(&hi), SCAN_CHUNK).await?;
        let n = page.len();
        let Some((last, _)) = page.last() else { break };
        from = Bound::Excluded(last.clone());
        out.extend(page);
        if n < SCAN_CHUNK {
            break;
        }
    }
    Ok(out)
}

async fn hist_at<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    key: &[u8],
    main: i64,
    sub: i64,
) -> Result<Option<Hist>> {
    match t.get(&h_key(key, main, sub)).await? {
        None => Ok(None),
        Some(b) => Ok(Some(dec(&b, "history")?)),
    }
}

fn kv_of(key: &[u8], main: i64, h: Hist) -> Option<mvccpb::KeyValue> {
    match h {
        Hist::Tombstone => None,
        Hist::Put {
            create,
            version,
            lease,
            value,
        } => Some(mvccpb::KeyValue {
            key: key.to_vec(),
            create_revision: create,
            mod_revision: main,
            version,
            value,
            lease,
        }),
    }
}

/// The value of `cur` (a live key).
async fn current_kv<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    key: &[u8],
    cur: &Current,
) -> Result<mvccpb::KeyValue> {
    let value = match hist_at(t, key, cur.main, cur.sub).await? {
        Some(Hist::Put { value, .. }) => value,
        _ => {
            return Err(FileSystemError::InvalidArgument(format!(
                "etcd key {:?}: its current revision {}.{} is missing from its history",
                String::from_utf8_lossy(key),
                cur.main,
                cur.sub
            ))
            .into());
        }
    };
    Ok(mvccpb::KeyValue {
        key: key.to_vec(),
        create_revision: cur.create,
        mod_revision: cur.main,
        version: cur.version,
        value,
        lease: cur.lease,
    })
}

async fn get_current<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    key: &[u8],
) -> Result<Option<Current>> {
    match t.get(&k_key(key)).await? {
        None => Ok(None),
        Some(b) => Ok(Some(dec(&b, "key")?)),
    }
}

/// The event for change `c` at `main.sub`.
async fn event_at<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    c: &Change,
    main: i64,
    sub: i64,
    prev_kv: bool,
) -> Result<mvccpb::Event> {
    let now = hist_at(t, &c.key, main, sub).await?;
    let (ty, kv) = match now.and_then(|h| kv_of(&c.key, main, h)) {
        Some(kv) => (mvccpb::event::EventType::Put, kv),
        None => (
            mvccpb::event::EventType::Delete,
            mvccpb::KeyValue {
                key: c.key.clone(),
                mod_revision: main,
                ..Default::default()
            },
        ),
    };
    let prev = match (prev_kv, c.prev) {
        (true, Some((pm, ps))) => hist_at(t, &c.key, pm, ps)
            .await?
            .and_then(|h| kv_of(&c.key, pm, h)),
        _ => None,
    };
    Ok(mvccpb::Event {
        r#type: ty as i32,
        kv: Some(kv),
        prev_kv: prev,
    })
}

/// A key as a range sees it: its metadata, its value read later (only
/// for what's returned).
struct Found {
    key: Vec<u8>,
    create: i64,
    main: i64,
    sub: i64,
    version: i64,
    lease: i64,
    /// Read already (history scans).
    value: Option<Vec<u8>>,
}

/// The keys of `kr` as of revision `at` (None: as the tree has them now).
async fn found<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    kr: &KeyRange,
    at: Option<i64>,
) -> Result<Vec<Found>> {
    let mut out = Vec::new();
    let Some(rev) = at else {
        if !kr.is_range() {
            if let Some(c) = get_current(t, &kr.start).await? {
                out.push(found_of(kr.start.clone(), c));
            }
            return Ok(out);
        }
        let (lo, hi) = kr.current_bounds();
        for (k, v) in scan(t, lo, hi).await? {
            let c: Current = dec(&v, "key")?;
            out.push(found_of(k[1..].to_vec(), c));
        }
        return Ok(out);
    };
    // At an earlier revision: each key's last history entry at or below it.
    let (lo, hi) = if kr.is_range() {
        kr.history_bounds()
    } else {
        let p = h_prefix(&kr.start);
        let hi = crate::kv::prefix_end(&p).unwrap();
        (p, hi)
    };
    let mut from = lo;
    // Each key's last entry at or below `rev` (entries come oldest first).
    let mut pending: Vec<(Vec<u8>, i64, i64, crate::btree::Val)> = Vec::new();
    let mut first = true;
    loop {
        let lo_b = if first {
            Bound::Included(from.as_slice())
        } else {
            Bound::Excluded(from.as_slice())
        };
        first = false;
        let page = t
            .range_raw(lo_b, Bound::Excluded(hi.as_slice()), SCAN_CHUNK)
            .await?;
        let n = page.len();
        let Some((lk, _)) = page.last() else { break };
        let next = lk.clone();
        for (k, v) in page {
            let Some((key, main, sub)) = parse_h(&k) else {
                continue;
            };
            if main > rev || !kr.contains(&key) {
                continue;
            }
            match pending.last_mut() {
                Some(l) if l.0 == key => *l = (key, main, sub, v),
                _ => pending.push((key, main, sub, v)),
            }
        }
        from = next;
        if n < SCAN_CHUNK {
            break;
        }
    }
    for (key, main, sub, v) in pending {
        let h: Hist = dec(&t.read_val(&v).await?, "history")?;
        if let Hist::Put {
            create,
            version,
            lease,
            value,
        } = h
        {
            out.push(Found {
                key,
                create,
                main,
                sub,
                version,
                lease,
                value: Some(value),
            });
        }
    }
    Ok(out)
}

fn found_of(key: Vec<u8>, c: Current) -> Found {
    Found {
        key,
        create: c.create,
        main: c.main,
        sub: c.sub,
        version: c.version,
        lease: c.lease,
        value: None,
    }
}

async fn range<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    meta: &Meta,
    cur: i64,
    r: &pb::RangeRequest,
) -> Result<pb::RangeResponse> {
    if r.key.is_empty() {
        return Err(EtcdError::EmptyKey);
    }
    let rev = if r.revision <= 0 { cur } else { r.revision };
    if rev > cur {
        return Err(EtcdError::FutureRev);
    }
    if rev < meta.compact {
        return Err(EtcdError::Compacted);
    }
    let kr = KeyRange::new(&r.key, &r.range_end);
    let at = (rev != cur).then_some(rev);
    let mut all = found(t, &kr, at).await?;
    let count = all.len() as i64;
    all.retain(|f| {
        (r.min_mod_revision == 0 || f.main >= r.min_mod_revision)
            && (r.max_mod_revision == 0 || f.main <= r.max_mod_revision)
            && (r.min_create_revision == 0 || f.create >= r.min_create_revision)
            && (r.max_create_revision == 0 || f.create <= r.max_create_revision)
    });
    let target = SortTarget::try_from(r.sort_target).unwrap_or(SortTarget::Key);
    let mut order = SortOrder::try_from(r.sort_order).unwrap_or(SortOrder::None);
    if order == SortOrder::None && target != SortTarget::Key {
        order = SortOrder::Ascend;
    }
    let need_values = !r.count_only && (!r.keys_only || target == SortTarget::Value);
    if target == SortTarget::Value {
        for f in all.iter_mut() {
            if f.value.is_none() {
                f.value = Some(value_of(t, f).await?);
            }
        }
    }
    match (target, order) {
        (_, SortOrder::None) | (SortTarget::Key, SortOrder::Ascend) => {}
        (SortTarget::Key, SortOrder::Descend) => all.reverse(),
        (target, order) => {
            all.sort_by(|a, b| {
                let o = match target {
                    SortTarget::Version => a.version.cmp(&b.version),
                    SortTarget::Create => a.create.cmp(&b.create),
                    SortTarget::Mod => a.main.cmp(&b.main),
                    SortTarget::Value => a.value.cmp(&b.value),
                    SortTarget::Key => a.key.cmp(&b.key),
                };
                if order == SortOrder::Descend {
                    o.reverse()
                } else {
                    o
                }
            });
        }
    }
    let mut more = false;
    if r.limit > 0 && all.len() > r.limit as usize {
        all.truncate(r.limit as usize);
        more = true;
    }
    let mut kvs = Vec::new();
    if !r.count_only {
        for mut f in all {
            let value = if !need_values || r.keys_only {
                Vec::new()
            } else {
                match f.value.take() {
                    Some(v) => v,
                    None => value_of(t, &f).await?,
                }
            };
            kvs.push(mvccpb::KeyValue {
                key: f.key,
                create_revision: f.create,
                mod_revision: f.main,
                version: f.version,
                value,
                lease: f.lease,
            });
        }
    }
    Ok(pb::RangeResponse {
        header: None,
        kvs,
        more,
        count,
    })
}

async fn value_of<D: PoolDevice, R: Mutable>(t: &Txn<'_, D, R>, f: &Found) -> Result<Vec<u8>> {
    if let Some(v) = &f.value {
        return Ok(v.clone());
    }
    let c = Current {
        create: f.create,
        main: f.main,
        sub: f.sub,
        version: f.version,
        lease: f.lease,
    };
    Ok(current_kv(t, &f.key, &c).await?.value)
}

/// Whether every comparison holds (at the current state).
async fn compares_hold<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    cs: &[pb::Compare],
) -> Result<bool> {
    for c in cs {
        let kr = KeyRange::new(&c.key, &c.range_end);
        let keys = found(t, &kr, None).await?;
        let target = CompareTarget::try_from(c.target).unwrap_or(CompareTarget::Version);
        if keys.is_empty() {
            if target == CompareTarget::Value {
                return Ok(false);
            }
            let zero = Found {
                key: c.key.clone(),
                create: 0,
                main: 0,
                sub: 0,
                version: 0,
                lease: 0,
                value: Some(Vec::new()),
            };
            if !compare_one(t, c, target, &zero).await? {
                return Ok(false);
            }
            continue;
        }
        for f in &keys {
            if !compare_one(t, c, target, f).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn compare_one<D: PoolDevice, R: Mutable>(
    t: &Txn<'_, D, R>,
    c: &pb::Compare,
    target: CompareTarget,
    f: &Found,
) -> Result<bool> {
    use std::cmp::Ordering;
    let ord: Ordering = match (target, &c.target_union) {
        (CompareTarget::Version, Some(TargetUnion::Version(v))) => f.version.cmp(v),
        (CompareTarget::Create, Some(TargetUnion::CreateRevision(v))) => f.create.cmp(v),
        (CompareTarget::Mod, Some(TargetUnion::ModRevision(v))) => f.main.cmp(v),
        (CompareTarget::Lease, Some(TargetUnion::Lease(v))) => f.lease.cmp(v),
        (CompareTarget::Value, Some(TargetUnion::Value(v))) => {
            value_of(t, f).await?.as_slice().cmp(v.as_slice())
        }
        // A target with no (or the wrong) value: compare against zero, as
        // etcd does for the numeric ones.
        (CompareTarget::Version, _) => f.version.cmp(&0),
        (CompareTarget::Create, _) => f.create.cmp(&0),
        (CompareTarget::Mod, _) => f.main.cmp(&0),
        (CompareTarget::Lease, _) => f.lease.cmp(&0),
        (CompareTarget::Value, _) => value_of(t, f).await?.as_slice().cmp(&[][..]),
    };
    Ok(
        match CompareResult::try_from(c.result).unwrap_or(CompareResult::Equal) {
            CompareResult::Equal => ord == Ordering::Equal,
            CompareResult::Greater => ord == Ordering::Greater,
            CompareResult::Less => ord == Ordering::Less,
            CompareResult::NotEqual => ord != Ordering::Equal,
        },
    )
}

fn check_put(r: &pb::PutRequest) -> Result<()> {
    if r.key.is_empty() {
        return Err(EtcdError::EmptyKey);
    }
    if r.ignore_value && !r.value.is_empty() {
        return Err(EtcdError::ValueProvided);
    }
    if r.ignore_lease && r.lease != 0 {
        return Err(EtcdError::LeaseProvided);
    }
    Ok(())
}

/// etcd's checks on a transaction: sizes, keys, and no key written twice
/// in a branch (nor put and deleted).
fn check_txn(r: &pb::TxnRequest) -> Result<()> {
    if r.compare.len() > MAX_TXN_OPS
        || r.success.len() > MAX_TXN_OPS
        || r.failure.len() > MAX_TXN_OPS
    {
        return Err(EtcdError::TooManyOps);
    }
    for c in &r.compare {
        if c.key.is_empty() {
            return Err(EtcdError::EmptyKey);
        }
    }
    for branch in [&r.success, &r.failure] {
        for op in branch.iter() {
            match &op.request {
                Some(Request::RequestRange(q)) if q.key.is_empty() => {
                    return Err(EtcdError::EmptyKey);
                }
                Some(Request::RequestPut(p)) => check_put(p)?,
                Some(Request::RequestDeleteRange(d)) if d.key.is_empty() => {
                    return Err(EtcdError::EmptyKey);
                }
                Some(Request::RequestTxn(t)) => check_txn(t)?,
                _ => {}
            }
        }
        let mut puts = BTreeSet::new();
        let mut dels = Vec::new();
        branch_writes(branch, &mut puts, &mut dels)?;
        for d in &dels {
            for p in &puts {
                if d.contains(p) {
                    return Err(EtcdError::DuplicateKey);
                }
            }
        }
    }
    Ok(())
}

/// The keys a branch puts and the ranges it deletes (a nested
/// transaction's either branch).
fn branch_writes(
    ops: &[pb::RequestOp],
    puts: &mut BTreeSet<Vec<u8>>,
    dels: &mut Vec<KeyRange>,
) -> Result<()> {
    for op in ops {
        match &op.request {
            Some(Request::RequestPut(p)) => {
                if !puts.insert(p.key.clone()) {
                    return Err(EtcdError::DuplicateKey);
                }
            }
            Some(Request::RequestDeleteRange(d)) => dels.push(KeyRange::new(&d.key, &d.range_end)),
            Some(Request::RequestTxn(t)) => {
                // Only one of its branches runs: each against ours.
                let mut best: Option<(BTreeSet<Vec<u8>>, Vec<KeyRange>)> = None;
                for b in [&t.success, &t.failure] {
                    let mut p = puts.clone();
                    let mut d = dels.clone();
                    branch_writes(b, &mut p, &mut d)?;
                    best = Some(match best {
                        None => (p, d),
                        Some((mut bp, mut bd)) => {
                            bp.extend(p);
                            bd.extend(d);
                            (bp, bd)
                        }
                    });
                }
                if let Some((p, d)) = best {
                    *puts = p;
                    *dels = d;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn txn_is_read_only(r: &pb::TxnRequest) -> bool {
    fn ro(ops: &[pb::RequestOp]) -> bool {
        ops.iter().all(|op| match &op.request {
            Some(Request::RequestRange(_)) | None => true,
            Some(Request::RequestTxn(t)) => ro(&t.success) && ro(&t.failure),
            _ => false,
        })
    }
    ro(&r.success) && ro(&r.failure) && !has_txn(&r.success) && !has_txn(&r.failure)
}

fn has_txn(ops: &[pb::RequestOp]) -> bool {
    ops.iter()
        .any(|op| matches!(op.request, Some(Request::RequestTxn(_))))
}

/// One write transaction on the keyspace: every change in it is at
/// revision `meta.rev + 1` (sub-revisions in order), all applied together.
pub(crate) struct Writer<'a, D: PoolDevice, R: Mutable> {
    t: Txn<'a, D, R>,
    max_key: usize,
    meta: Meta,
    start_meta: Meta,
    /// Changes made so far (the next sub-revision).
    sub: i64,
    leases_added: Vec<(i64, i64)>,
    leases_removed: Vec<i64>,
}

impl<D: PoolDevice, R: Mutable> Writer<'_, D, R> {
    /// The revision reads in this transaction see.
    fn cur_rev(&self) -> i64 {
        if self.sub > 0 {
            self.meta.rev + 1
        } else {
            self.meta.rev
        }
    }

    /// Apply it (one intent log entry). Returns whether it changed
    /// anything, the revision after, and the leases it granted and revoked.
    async fn finish(mut self) -> Result<(bool, i64, Vec<(i64, i64)>, Vec<i64>)> {
        if self.sub > 0 {
            self.meta.rev += 1;
        }
        if self.meta != self.start_meta {
            self.t.put(META, &enc(&self.meta)).await?;
        }
        let changed = !self.t.is_empty();
        self.t.apply_logged()?;
        Ok((
            changed,
            self.meta.rev,
            self.leases_added,
            self.leases_removed,
        ))
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(EtcdError::EmptyKey);
        }
        // Its history entries are the longest tree keys it gets.
        let max = (self.max_key - 1 - 2 - 16) / 2;
        if h_prefix(key).len() + 16 > self.max_key {
            return Err(EtcdError::KeyTooLong(max));
        }
        Ok(())
    }

    async fn record(&mut self, key: &[u8], prev: Option<(i64, i64)>, h: Hist) -> Result<()> {
        let (main, sub) = (self.meta.rev + 1, self.sub);
        self.t.put(&h_key(key, main, sub), &enc(&h)).await?;
        self.t
            .put(
                &r_key(main, sub),
                &enc(&Change {
                    key: key.to_vec(),
                    prev,
                }),
            )
            .await?;
        self.sub += 1;
        Ok(())
    }

    async fn put(&mut self, r: &pb::PutRequest) -> Result<pb::PutResponse> {
        self.check_key(&r.key)?;
        let cur = get_current(&self.t, &r.key).await?;
        let prev_kv = match &cur {
            Some(c) => Some(current_kv(&self.t, &r.key, c).await?),
            None => None,
        };
        let value = if r.ignore_value {
            match &prev_kv {
                Some(p) => p.value.clone(),
                None => return Err(EtcdError::KeyNotFound),
            }
        } else {
            r.value.clone()
        };
        let lease = if r.ignore_lease {
            match &cur {
                Some(c) => c.lease,
                None => return Err(EtcdError::KeyNotFound),
            }
        } else {
            r.lease
        };
        if lease != 0 && self.t.get(&l_key(lease)).await?.is_none() {
            return Err(EtcdError::LeaseNotFound);
        }
        let main = self.meta.rev + 1;
        let sub = self.sub;
        let next = Current {
            create: cur.map_or(main, |c| c.create),
            main,
            sub,
            version: cur.map_or(1, |c| c.version + 1),
            lease,
        };
        self.record(
            &r.key,
            cur.map(|c| (c.main, c.sub)),
            Hist::Put {
                create: next.create,
                version: next.version,
                lease,
                value,
            },
        )
        .await?;
        self.t.put(&k_key(&r.key), &enc(&next)).await?;
        let old_lease = cur.map_or(0, |c| c.lease);
        if old_lease != lease {
            if old_lease != 0 {
                self.t.delete(&a_key(old_lease, &r.key)).await?;
            }
            if lease != 0 {
                self.t.put(&a_key(lease, &r.key), b"").await?;
            }
        }
        Ok(pb::PutResponse {
            header: None,
            prev_kv: if r.prev_kv { prev_kv } else { None },
        })
    }

    /// Delete live key `key` (its state `cur`).
    async fn delete_key(&mut self, key: &[u8], cur: Current) -> Result<()> {
        self.record(key, Some((cur.main, cur.sub)), Hist::Tombstone)
            .await?;
        self.t.delete(&k_key(key)).await?;
        if cur.lease != 0 {
            self.t.delete(&a_key(cur.lease, key)).await?;
        }
        Ok(())
    }

    async fn delete_range(
        &mut self,
        r: &pb::DeleteRangeRequest,
    ) -> Result<pb::DeleteRangeResponse> {
        let kr = KeyRange::new(&r.key, &r.range_end);
        let keys = found(&self.t, &kr, None).await?;
        let mut prev_kvs = Vec::new();
        let deleted = keys.len() as i64;
        for f in keys {
            let c = Current {
                create: f.create,
                main: f.main,
                sub: f.sub,
                version: f.version,
                lease: f.lease,
            };
            if r.prev_kv {
                prev_kvs.push(current_kv(&self.t, &f.key, &c).await?);
            }
            self.delete_key(&f.key, c).await?;
        }
        Ok(pb::DeleteRangeResponse {
            header: None,
            deleted,
            prev_kvs,
        })
    }

    async fn txn(
        &mut self,
        r: &pb::TxnRequest,
        header: &Option<pb::ResponseHeader>,
    ) -> Result<pb::TxnResponse> {
        let ok = compares_hold(&self.t, &r.compare).await?;
        let ops = if ok { &r.success } else { &r.failure };
        let mut responses = Vec::new();
        for op in ops {
            let response = match &op.request {
                None => continue,
                Some(Request::RequestRange(q)) => {
                    let cur = self.cur_rev();
                    let mut x = range(&self.t, &self.meta, cur, q).await?;
                    x.header = *header;
                    Response::ResponseRange(x)
                }
                Some(Request::RequestPut(p)) => {
                    let mut x = self.put(p).await?;
                    x.header = *header;
                    Response::ResponsePut(x)
                }
                Some(Request::RequestDeleteRange(d)) => {
                    let mut x = self.delete_range(d).await?;
                    x.header = *header;
                    Response::ResponseDeleteRange(x)
                }
                Some(Request::RequestTxn(t)) => {
                    let mut x = Box::pin(self.txn(t, header)).await?;
                    x.header = *header;
                    Response::ResponseTxn(x)
                }
            };
            responses.push(pb::ResponseOp {
                response: Some(response),
            });
        }
        Ok(pb::TxnResponse {
            header: None,
            succeeded: ok,
            responses,
        })
    }

    async fn lease_grant(&mut self, id: i64, ttl: i64) -> Result<i64> {
        let id = if id != 0 {
            if self.t.get(&l_key(id)).await?.is_some() {
                return Err(EtcdError::LeaseExists);
            }
            id
        } else {
            // Ids from a counter starting at a random-looking point (as
            // etcd's look), skipping any a client chose.
            if self.meta.next_lease <= 0 {
                let seed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(1);
                self.meta.next_lease = (blake3::hash(&seed.to_le_bytes()).as_bytes()[0..8]
                    .iter()
                    .fold(0u64, |a, b| (a << 8) | *b as u64)
                    >> 16) as i64
                    | 1;
            }
            loop {
                let id = self.meta.next_lease;
                self.meta.next_lease += 1;
                if self.t.get(&l_key(id)).await?.is_none() {
                    break id;
                }
            }
        };
        self.t.put(&l_key(id), &enc(&LeaseRec { ttl })).await?;
        self.leases_added.push((id, ttl));
        Ok(id)
    }

    async fn lease_revoke(&mut self, id: i64) -> Result<()> {
        if self.t.get(&l_key(id)).await?.is_none() {
            return Err(EtcdError::LeaseNotFound);
        }
        let p = a_prefix(id);
        let end = crate::kv::prefix_end(&p).unwrap();
        for (k, _) in scan(&self.t, p.clone(), end).await? {
            let key = &k[p.len()..];
            if let Some(c) = get_current(&self.t, key).await?
                && c.lease == id
            {
                self.delete_key(key, c).await?;
            }
            self.t.delete(&k).await?;
        }
        self.t.delete(&l_key(id)).await?;
        self.leases_removed.push(id);
        Ok(())
    }

    /// Some of the compaction in progress. None: there's none; Some(Some(rev)):
    /// it's done (up to rev); Some(None): more to do.
    async fn compact_batch(&mut self) -> Result<Option<Option<i64>>> {
        let Some((target, cursor)) = self.meta.compacting.clone() else {
            return Ok(None);
        };
        let mut deletes = 0;
        let mut cursor = cursor;
        if cursor.first() == Some(&b'H') {
            // Each key's history at or below the target: only its last
            // entry stays, and not even that if it's a tombstone.
            while deletes < COMPACT_BATCH {
                let next = self
                    .t
                    .range_raw(
                        Bound::Included(cursor.as_slice()),
                        Bound::Excluded(&b"I"[..]),
                        1,
                    )
                    .await?;
                let Some((hk, _)) = next.into_iter().next() else {
                    cursor = b"R".to_vec();
                    break;
                };
                let Some((key, _, _)) = parse_h(&hk) else {
                    // Not ours; step over it.
                    cursor = hk;
                    cursor.push(0);
                    continue;
                };
                let prefix = h_prefix(&key);
                let mut upto = prefix.clone();
                upto.extend(be(target + 1));
                let old = self
                    .t
                    .range_raw(
                        Bound::Included(prefix.as_slice()),
                        Bound::Excluded(upto.as_slice()),
                        usize::MAX,
                    )
                    .await?;
                if let Some(((lk, lv), rest)) = old.split_last() {
                    // A change at the target itself is an event a watch
                    // from the compacted revision still gets, with the one
                    // before it as `prev_kv` (a delete's is the object
                    // deleted, which kube-apiserver needs): that one stays
                    // too, and so does the change (a tombstone gone would
                    // bring the value before it back). The next compaction
                    // past it takes both (audit E2).
                    let at_target = parse_h(lk).is_some_and(|(_, main, _)| main == target);
                    let keep = if at_target { 1 } else { 0 };
                    for (k, _) in &rest[..rest.len() - keep.min(rest.len())] {
                        self.t.delete(k).await?;
                        deletes += 1;
                    }
                    let last: Hist = dec(&self.t.read_val(lv).await?, "history")?;
                    if last == Hist::Tombstone && !at_target {
                        let lk = lk.clone();
                        self.t.delete(&lk).await?;
                        deletes += 1;
                    }
                }
                cursor = crate::kv::prefix_end(&prefix).unwrap();
            }
        }
        if cursor.first() == Some(&b'R') {
            // Events below the target.
            let hi = r_key(target, 0);
            let old = self
                .t
                .range_raw(
                    Bound::Included(&b"R"[..]),
                    Bound::Excluded(hi.as_slice()),
                    COMPACT_BATCH.saturating_sub(deletes).max(1),
                )
                .await?;
            let n = old.len();
            for (k, _) in old {
                self.t.delete(&k).await?;
            }
            if n == 0 {
                self.meta.compacting = None;
                return Ok(Some(Some(target)));
            }
        }
        self.meta.compacting = Some((target, cursor));
        Ok(Some(None))
    }
}
