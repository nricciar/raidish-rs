//! S3 buckets: one bucket is one dataset of format `s3`, a B+tree (see
//! `crate::btree`) holding its objects, their bodies and the multipart
//! uploads in progress. The HTTP API is in [`http`].
//!
//! ```text
//!   M                 the bucket's counters (the next body id)
//!   O <key>           an object: its metadata, and the bodies holding it
//!   B <body> <chunk>  4 MiB of a body
//!   U <upload>        a multipart upload: its key and metadata
//!   P <upload> <part> a part uploaded: its body, size, ETag
//!   W <body>          a body being written (dropped if it never makes it)
//!   D <body>          a body nothing uses any more, to delete
//! ```
//!
//! **Writing an object.** Its body goes in first, under a new body id
//! marked `W`: small ones through the intent log, large ones straight to
//! the tree and committed before going on. Then one logged transaction
//! points `O <key>` at it (and marks the body it replaces `D`), and is
//! made durable before the request is answered. A crash midway leaves a
//! `W` body, which is deleted when the bucket is next served; a crash
//! after leaves the whole object.
//!
//! **Bodies are deleted in the background**, never while being read: a
//! GET holds its bodies until done.

pub mod checksum;
pub mod http;

use crate::btree::Txn;
use crate::dataset::DatasetFormat;
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::service_tree::{Served, ServiceError, ServiceTree, spawn_while_alive};
use crate::types::DatasetName;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Bytes per body chunk.
pub const CHUNK: usize = 4 << 20;
/// Bodies up to this size go through the intent log.
const LOGGED_BODY: u64 = 1 << 20;
/// The largest object a single PUT makes (S3's).
pub const MAX_PUT: u64 = 5 << 30;
/// The smallest part but the last (S3's).
pub const MIN_PART: u64 = 5 << 20;
pub const MAX_PARTS: u32 = 10_000;
/// The longest key (S3's).
pub const MAX_KEY: usize = 1024;
/// Chunks deleted per transaction in the background.
const GC_BATCH: usize = 64;

/// What S3 requests can fail with (beyond the pool's errors).
#[derive(Debug)]
pub enum S3Error {
    NoSuchKey,
    NoSuchUpload,
    InvalidPart(String),
    InvalidPartOrder,
    EntityTooSmall,
    EntityTooLarge,
    KeyTooLong,
    InvalidArgument(String),
    /// Not served here (any more).
    Unavailable,
    Fs(FileSystemError),
}

impl From<FileSystemError> for S3Error {
    fn from(e: FileSystemError) -> Self {
        S3Error::Fs(e)
    }
}

impl ServiceError for S3Error {
    fn stopped() -> Self {
        S3Error::Unavailable
    }
}

impl<D: BlockDevice + 'static> Served for Bucket<D> {
    fn is_stopped(&self) -> bool {
        self.tree.is_stopped()
    }
}

pub type Result<T> = std::result::Result<T, S3Error>;

/// An object's metadata.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    /// Its ETag, unquoted: the MD5 of the body, or for a multipart object
    /// the MD5 of its parts' MD5s and "-<parts>".
    pub etag: String,
    pub last_modified_ms: u64,
    pub content_type: Option<String>,
    /// Standard headers kept with it (Content-Encoding, Cache-Control...).
    pub headers: Vec<(String, String)>,
    /// x-amz-meta-* (names lowercase, without the prefix).
    pub user: Vec<(String, String)>,
    /// The bodies holding it, in order: one, or one per part.
    pub segments: Vec<Segment>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    pub body: u64,
    pub size: u64,
}

/// A multipart upload.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    pub key: String,
    pub initiated_ms: u64,
    /// What the object gets (size, ETag and segments are filled in when
    /// it's completed).
    pub meta: ObjectMeta,
}

/// A part of a multipart upload.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub body: u64,
    pub size: u64,
    /// MD5 of the part (raw).
    pub md5: [u8; 16],
    pub last_modified_ms: u64,
}

impl Part {
    pub fn etag(&self) -> String {
        hex::encode(self.md5)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
struct Meta {
    next_body: u64,
    next_upload: u64,
}

use crate::service_tree::enc_bin as enc;

fn dec<T: for<'a> Deserialize<'a>>(b: &[u8], what: &str) -> Result<T> {
    Ok(crate::service_tree::dec_bin(b, &format!("s3 {}", what))?)
}

fn key_of(tag: u8, rest: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(rest.len() + 1);
    k.push(tag);
    k.extend_from_slice(rest);
    k
}

fn o_key(key: &str) -> Vec<u8> {
    key_of(b'O', key.as_bytes())
}

fn b_key(body: u64, chunk: u64) -> Vec<u8> {
    let mut k = key_of(b'B', &body.to_be_bytes());
    k.extend(chunk.to_be_bytes());
    k
}

fn body_key(tag: u8, body: u64) -> Vec<u8> {
    key_of(tag, &body.to_be_bytes())
}

fn u_key(upload: &str) -> Vec<u8> {
    key_of(b'U', upload.as_bytes())
}

fn p_prefix(upload: &str) -> Vec<u8> {
    let mut k = key_of(b'P', upload.as_bytes());
    k.push(0);
    k
}

fn p_key(upload: &str, part: u32) -> Vec<u8> {
    let mut k = p_prefix(upload);
    k.extend(part.to_be_bytes());
    k
}

/// Every entry in `[lo, hi)`, up to `limit`.
async fn scan<D: BlockDevice>(
    t: &Txn<'_, D>,
    lo: Bound<&[u8]>,
    hi: &[u8],
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(t.range(lo, Bound::Excluded(hi), limit).await?)
}

/// One page of a listing.
#[derive(Debug, Default)]
pub struct Listing {
    pub objects: Vec<(String, ObjectMeta)>,
    /// Keys rolled up by the delimiter.
    pub prefixes: Vec<String>,
    pub truncated: bool,
    /// The last key or prefix listed (where the next page starts after).
    pub last: Option<String>,
}

/// A bucket, served from this node.
pub struct Bucket<D: BlockDevice> {
    pub tree: ServiceTree<D>,
    /// Bodies being read: not to be deleted yet.
    readers: Mutex<HashMap<u64, usize>>,
    gc: Arc<tokio::sync::Notify>,
    /// Refusing writes (being deleted).
    /// DeleteBuckets in progress (`close`): writes are refused while any is.
    closing: AtomicUsize,
}

/// Holds bodies against deletion while they're read.
pub struct Reading<D: BlockDevice + 'static> {
    bucket: Arc<Bucket<D>>,
    bodies: Vec<u64>,
}

impl<D: BlockDevice + 'static> Drop for Reading<D> {
    fn drop(&mut self) {
        let mut r = self.bucket.readers.lock().unwrap();
        for b in &self.bodies {
            if let Some(n) = r.get_mut(b) {
                *n -= 1;
                if *n == 0 {
                    r.remove(b);
                }
            }
        }
    }
}

/// Writes to a bucket refused while held (see `Bucket::close`).
pub struct Closed<'a> {
    closing: &'a AtomicUsize,
    keep: bool,
}

impl Closed<'_> {
    /// The bucket was deleted: leave it closed.
    pub fn keep(mut self) {
        self.keep = true;
    }
}

impl Drop for Closed<'_> {
    fn drop(&mut self) {
        if !self.keep {
            self.closing.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<D: BlockDevice + 'static> Bucket<D> {
    /// Serve bucket `name` of `fs`. Bodies left half-written by an earlier
    /// server are dropped.
    pub async fn open(
        fs: Arc<FileSystem<D>>,
        name: &DatasetName,
    ) -> std::result::Result<Arc<Self>, FileSystemError> {
        let b = Arc::new(Bucket {
            tree: ServiceTree::open(fs, name, DatasetFormat::S3).await?,
            readers: Mutex::new(HashMap::new()),
            gc: Default::default(),
            closing: AtomicUsize::new(0),
        });
        // Half-written bodies: nothing points at them.
        b.write(|t| {
            Box::pin(async move {
                for (k, _) in scan(t, Bound::Included(b"W"), b"X", usize::MAX).await? {
                    t.delete(&k).await?;
                    t.put(&key_of(b'D', &k[1..]), b"").await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| match e {
            S3Error::Fs(e) => e,
            e => FileSystemError::InvalidArgument(format!("{:?}", e)),
        })?;
        let gc = Some(b.gc.clone());
        spawn_while_alive(&b, Duration::from_secs(10), gc, |b| async move {
            if let Err(e) = b.collect().await {
                eprintln!("s3 '{}': deleting old bodies: {:?}", b.tree.name, e);
            }
        });
        Ok(b)
    }

    pub fn stop(&self) {
        self.tree.stop();
        // A permit, not a wake-up: its loop may be mid-pass, not waiting.
        self.gc.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.tree.is_stopped()
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
        self.tree.write(f).await
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
        self.tree.read(f).await
    }

    /// Refuse writes while the guard lives (a DeleteBucket in progress).
    /// Writes are allowed again when it's dropped, however the delete ends
    /// (an error, or the request cancelled), unless `Closed::keep` says
    /// the bucket is gone. A flag set here and cleared by hand used to stay
    /// set when the delete ended any other way, refusing writes for good.
    pub fn close(&self) -> Closed<'_> {
        self.closing.fetch_add(1, Ordering::SeqCst);
        Closed {
            closing: &self.closing,
            keep: false,
        }
    }

    fn check_writable(&self) -> Result<()> {
        if self.closing.load(Ordering::SeqCst) > 0 {
            return Err(S3Error::Unavailable);
        }
        Ok(())
    }

    // ---- bodies --------------------------------------------------------

    /// Start writing a body of about `expected` bytes (None: unknown).
    pub async fn new_body(self: &Arc<Self>, expected: Option<u64>) -> Result<BodyWriter<D>> {
        self.check_writable()?;
        let body = self
            .write(|t| {
                Box::pin(async move {
                    let mut m: Meta = match t.get(b"M").await? {
                        Some(b) => dec(&b, "meta")?,
                        None => Meta::default(),
                    };
                    m.next_body += 1;
                    let body = m.next_body;
                    t.put(b"M", &enc(&m)).await?;
                    t.put(&body_key(b'W', body), b"").await?;
                    Ok(body)
                })
            })
            .await?;
        Ok(BodyWriter {
            bucket: self.clone(),
            body,
            chunk: 0,
            buf: Vec::new(),
            size: 0,
            md5: Md5::new(),
            logged: expected.is_some_and(|n| n <= LOGGED_BODY),
        })
    }

    /// Chunk `chunk` of `body`.
    pub async fn read_chunk(&self, body: u64, chunk: u64) -> Result<Option<Vec<u8>>> {
        self.read(|t| Box::pin(async move { Ok(t.get(&b_key(body, chunk)).await?) }))
            .await
    }

    /// Hold `meta`'s bodies against deletion while reading them. Under the
    /// operations lock, so the collector (which looks under the write lock)
    /// sees it before it could delete them.
    pub(crate) fn reading(
        self: &Arc<Self>,
        _held: &crate::service_tree::OpsHeld<'_>,
        meta: &ObjectMeta,
    ) -> Reading<D> {
        let bodies: Vec<u64> = meta.segments.iter().map(|s| s.body).collect();
        let mut r = self.readers.lock().unwrap();
        for b in &bodies {
            *r.entry(*b).or_default() += 1;
        }
        Reading {
            bucket: self.clone(),
            bodies,
        }
    }

    /// The bodies being read now. With the write lock held, no new reader
    /// can register until it's dropped (audit X1).
    fn readers(&self, _held: &crate::service_tree::OpsWriteHeld<'_>) -> Vec<u64> {
        self.readers.lock().unwrap().keys().copied().collect()
    }

    /// Delete some unused bodies; again until there are none.
    async fn collect(&self) -> Result<()> {
        loop {
            if self.is_stopped() {
                return Ok(());
            }
            let ds = self.tree.ds::<S3Error>().await?;
            let more = {
                let w = ds.ops.write().await;
                // Under the write lock: a reader registers under a read
                // (`reading`), so every body read now is in this list.
                let readers = self.readers(&crate::service_tree::OpsWriteHeld::of(&w));
                let mut t = Txn::begin(&self.tree.fs, &ds.obj).await?;
                let dead = scan(&t, Bound::Included(b"D"), b"E", 64).await?;
                let mut deleted = 0;
                let mut done_any = false;
                for (k, _) in dead {
                    let body = u64::from_be_bytes(k[1..9].try_into().unwrap());
                    if readers.contains(&body) {
                        continue;
                    }
                    done_any = true;
                    let lo = b_key(body, 0);
                    let hi = b_key(body + 1, 0);
                    let chunks = t
                        .range_raw(
                            Bound::Included(&lo),
                            Bound::Excluded(&hi),
                            GC_BATCH - deleted,
                        )
                        .await?;
                    let n = chunks.len();
                    for (c, _) in chunks {
                        t.delete(&c).await?;
                    }
                    deleted += n;
                    if deleted >= GC_BATCH {
                        break; // its marker stays: more of it next time
                    }
                    t.delete(&k).await?;
                }
                // Unlogged: a crash forgets the deletes and their markers
                // alike, and they're done again.
                t.apply(None)?;
                done_any && deleted > 0
            };
            self.tree.fs.kv_settle().await?;
            if !more {
                return Ok(());
            }
        }
    }

    // ---- objects -------------------------------------------------------

    /// The object at `key`, with its bodies held against deletion from the
    /// same read that found it: a delete and collection in between can't
    /// take them from under the caller (see `collect`).
    pub async fn head_held(
        self: &Arc<Self>,
        key: &str,
    ) -> Result<Option<(ObjectMeta, Reading<D>)>> {
        let k = o_key(key);
        let me = self.clone();
        self.tree
            .read_held(|t, held| {
                Box::pin(async move {
                    match t.get(&k).await? {
                        Some(b) => {
                            let meta: ObjectMeta = dec(&b, "object")?;
                            let hold = me.reading(held, &meta);
                            Ok(Some((meta, hold)))
                        }
                        None => Ok(None),
                    }
                })
            })
            .await
    }

    /// Drop a body written with `new_body` that won't be used after all.
    pub async fn discard(&self, body: u64) -> Result<()> {
        self.write(|t| {
            Box::pin(async move {
                t.delete(&body_key(b'W', body)).await?;
                t.put(&body_key(b'D', body), b"").await?;
                Ok(())
            })
        })
        .await?;
        self.gc.notify_one();
        Ok(())
    }

    /// Make `key` the object `meta` (its bodies written with `new_body`).
    pub async fn put_object(&self, key: &str, meta: ObjectMeta) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(S3Error::KeyTooLong);
        }
        self.check_writable()?;
        let k = o_key(key);
        self.write(|t| {
            Box::pin(async move {
                if let Some(old) = t.get(&k).await? {
                    let old: ObjectMeta = dec(&old, "object")?;
                    for s in old.segments {
                        t.put(&body_key(b'D', s.body), b"").await?;
                    }
                }
                for s in &meta.segments {
                    t.delete(&body_key(b'W', s.body)).await?;
                }
                t.put(&k, &enc(&meta)).await?;
                Ok(())
            })
        })
        .await?;
        self.gc.notify_one();
        Ok(())
    }

    /// Delete `key`; whether it was there.
    pub async fn delete_object(&self, key: &str) -> Result<bool> {
        self.check_writable()?;
        let k = o_key(key);
        let gone = self
            .write(|t| {
                Box::pin(async move {
                    let Some(old) = t.get(&k).await? else {
                        return Ok(false);
                    };
                    let old: ObjectMeta = dec(&old, "object")?;
                    for s in old.segments {
                        t.put(&body_key(b'D', s.body), b"").await?;
                    }
                    t.delete(&k).await?;
                    Ok(true)
                })
            })
            .await?;
        self.gc.notify_one();
        Ok(gone)
    }

    /// Objects with keys starting with `prefix`, after `after`, up to
    /// `max` (objects and rolled-up prefixes together), keys sharing a
    /// prefix up to `delimiter` rolled up.
    pub async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        delimiter: Option<&str>,
        max: usize,
    ) -> Result<Listing> {
        let prefix = prefix.to_string();
        let after = after.map(str::to_string);
        let delimiter = delimiter.filter(|d| !d.is_empty()).map(str::to_string);
        self.read(|t| {
            Box::pin(async move {
                let mut out = Listing::default();
                let lo_key = o_key(&prefix);
                let hi = crate::kv::prefix_end(&lo_key).unwrap();
                let (mut from, mut inclusive) = match (&after, &delimiter) {
                    (Some(a), _) if a.as_str() < prefix.as_str() => (lo_key.clone(), true),
                    // After a rolled-up prefix: after everything under it.
                    (Some(a), Some(d))
                        if a.ends_with(d.as_str())
                            && a.strip_prefix(prefix.as_str()).is_some_and(|rest| {
                                // The delimiter ends it and isn't in it before.
                                rest.strip_suffix(d.as_str())
                                    .is_some_and(|before| !before.contains(d.as_str()))
                            }) =>
                    {
                        (crate::kv::prefix_end(&o_key(a)).unwrap(), true)
                    }
                    (Some(a), _) => (o_key(a), false),
                    (None, _) => (lo_key.clone(), true),
                };
                if max == 0 {
                    return Ok(out);
                }
                loop {
                    let lo = if inclusive {
                        Bound::Included(from.as_slice())
                    } else {
                        Bound::Excluded(from.as_slice())
                    };
                    let page = scan(t, lo, &hi, 256).await?;
                    if page.is_empty() {
                        return Ok(out);
                    }
                    let mut next: Option<(Vec<u8>, bool)> = None;
                    for (k, v) in page.iter() {
                        let key = String::from_utf8_lossy(&k[1..]).into_owned();
                        if out.objects.len() + out.prefixes.len() >= max {
                            out.truncated = true;
                            return Ok(out);
                        }
                        let rest = &key[prefix.len()..];
                        if let Some(d) = &delimiter
                            && let Some(i) = rest.find(d.as_str())
                        {
                            let p = format!("{}{}", prefix, &rest[..i + d.len()]);
                            if out.last.as_deref() != Some(p.as_str()) {
                                out.prefixes.push(p.clone());
                                out.last = Some(p.clone());
                            }
                            // Skip the rest of it.
                            let skip = crate::kv::prefix_end(&o_key(&p)).unwrap();
                            next = Some((skip, true));
                            break;
                        }
                        out.objects.push((key.clone(), dec(v, "object")?));
                        out.last = Some(key);
                        next = Some((k.clone(), false));
                    }
                    let (n, incl) = next.unwrap();
                    from = n;
                    inclusive = incl;
                }
            })
        })
        .await
    }

    /// Whether it holds no objects nor uploads.
    pub async fn is_empty(&self) -> Result<bool> {
        self.read(|t| {
            Box::pin(async move {
                let o = scan(t, Bound::Included(b"O"), b"P", 1).await?;
                let u = scan(t, Bound::Included(b"U"), b"V", 1).await?;
                Ok(o.is_empty() && u.is_empty())
            })
        })
        .await
    }

    // ---- multipart uploads ---------------------------------------------

    pub async fn create_upload(&self, key: &str, meta: ObjectMeta) -> Result<String> {
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(S3Error::KeyTooLong);
        }
        self.check_writable()?;
        let key = key.to_string();
        let name = self.tree.name.clone();
        self.write(|t| {
            Box::pin(async move {
                let mut m: Meta = match t.get(b"M").await? {
                    Some(b) => dec(&b, "meta")?,
                    None => Meta::default(),
                };
                m.next_upload += 1;
                // Unique, and not guessable from outside.
                let mut h = blake3::Hasher::new();
                h.update(name.as_bytes());
                h.update(&m.next_upload.to_le_bytes());
                h.update(&crate::fs::new_guid(m.next_upload).to_le_bytes());
                let id = hex::encode(&h.finalize().as_bytes()[..20]);
                t.put(b"M", &enc(&m)).await?;
                let up = Upload {
                    key,
                    initiated_ms: crate::cluster::now_ms(),
                    meta,
                };
                t.put(&u_key(&id), &enc(&up)).await?;
                Ok(id)
            })
        })
        .await
    }

    pub async fn upload(&self, id: &str) -> Result<Upload> {
        let k = u_key(id);
        self.read(|t| {
            Box::pin(async move {
                match t.get(&k).await? {
                    Some(b) => dec(&b, "upload"),
                    None => Err(S3Error::NoSuchUpload),
                }
            })
        })
        .await
    }

    /// Record part `n` of upload `id` (its body written with `new_body`).
    pub async fn put_part(&self, id: &str, n: u32, part: Part) -> Result<()> {
        if !(1..=MAX_PARTS).contains(&n) {
            return Err(S3Error::InvalidArgument(
                "Part number must be an integer between 1 and 10000".into(),
            ));
        }
        self.check_writable()?;
        let uk = u_key(id);
        let pk = p_key(id, n);
        self.write(|t| {
            Box::pin(async move {
                if t.get(&uk).await?.is_none() {
                    // Its body won't be used.
                    t.delete(&body_key(b'W', part.body)).await?;
                    t.put(&body_key(b'D', part.body), b"").await?;
                    return Ok(Err(S3Error::NoSuchUpload));
                }
                if let Some(old) = t.get(&pk).await? {
                    let old: Part = dec(&old, "part")?;
                    t.put(&body_key(b'D', old.body), b"").await?;
                }
                t.delete(&body_key(b'W', part.body)).await?;
                t.put(&pk, &enc(&part)).await?;
                Ok(Ok(()))
            })
        })
        .await??;
        self.gc.notify_one();
        Ok(())
    }

    /// Upload `id`'s parts after part `after`, up to `max`.
    pub async fn parts(&self, id: &str, after: u32, max: usize) -> Result<Vec<(u32, Part)>> {
        let uk = u_key(id);
        let lo = p_key(id, after);
        let hi = crate::kv::prefix_end(&p_prefix(id)).unwrap();
        self.read(|t| {
            Box::pin(async move {
                if t.get(&uk).await?.is_none() {
                    return Err(S3Error::NoSuchUpload);
                }
                let mut out = Vec::new();
                for (k, v) in scan(t, Bound::Excluded(&lo), &hi, max).await? {
                    let n = u32::from_be_bytes(k[k.len() - 4..].try_into().unwrap());
                    out.push((n, dec(&v, "part")?));
                }
                Ok(out)
            })
        })
        .await
    }

    /// Complete upload `id` from `parts` (number, ETag), ascending: the
    /// object it makes.
    pub async fn complete(
        &self,
        id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<(String, ObjectMeta)> {
        self.check_writable()?;
        let uk = u_key(id);
        let id = id.to_string();
        let done = self
            .write(|t| {
                Box::pin(async move {
                    let Some(up) = t.get(&uk).await? else {
                        return Ok(Err(S3Error::NoSuchUpload));
                    };
                    let up: Upload = dec(&up, "upload")?;
                    if parts.is_empty() {
                        return Ok(Err(S3Error::InvalidArgument(
                            "You must specify at least one part".into(),
                        )));
                    }
                    if parts.windows(2).any(|w| w[0].0 >= w[1].0) {
                        return Ok(Err(S3Error::InvalidPartOrder));
                    }
                    let mut chosen = Vec::new();
                    for (i, (n, etag)) in parts.iter().enumerate() {
                        let Some(p) = t.get(&p_key(&id, *n)).await? else {
                            return Ok(Err(S3Error::InvalidPart(format!(
                                "part {} wasn't uploaded",
                                n
                            ))));
                        };
                        let p: Part = dec(&p, "part")?;
                        if etag.trim_matches('"') != p.etag() {
                            return Ok(Err(S3Error::InvalidPart(format!(
                                "part {}'s ETag isn't {}",
                                n, etag
                            ))));
                        }
                        if i + 1 < parts.len() && p.size < MIN_PART {
                            return Ok(Err(S3Error::EntityTooSmall));
                        }
                        chosen.push((*n, p));
                    }
                    // Every part not chosen, and the old object, go.
                    let lo = p_prefix(&id);
                    let hi = crate::kv::prefix_end(&lo).unwrap();
                    for (k, v) in scan(t, Bound::Included(&lo), &hi, usize::MAX).await? {
                        let n = u32::from_be_bytes(k[k.len() - 4..].try_into().unwrap());
                        if !chosen.iter().any(|(c, _)| *c == n) {
                            let p: Part = dec(&v, "part")?;
                            t.put(&body_key(b'D', p.body), b"").await?;
                        }
                        t.delete(&k).await?;
                    }
                    t.delete(&uk).await?;
                    let ok = o_key(&up.key);
                    if let Some(old) = t.get(&ok).await? {
                        let old: ObjectMeta = dec(&old, "object")?;
                        for s in old.segments {
                            t.put(&body_key(b'D', s.body), b"").await?;
                        }
                    }
                    let mut md5 = Md5::new();
                    for (_, p) in &chosen {
                        md5.update(p.md5);
                    }
                    let meta = ObjectMeta {
                        size: chosen.iter().map(|(_, p)| p.size).sum(),
                        etag: format!("{}-{}", hex::encode(md5.finalize()), chosen.len()),
                        last_modified_ms: crate::cluster::now_ms(),
                        segments: chosen
                            .iter()
                            .map(|(_, p)| Segment {
                                body: p.body,
                                size: p.size,
                            })
                            .collect(),
                        ..up.meta
                    };
                    t.put(&ok, &enc(&meta)).await?;
                    Ok(Ok((up.key, meta)))
                })
            })
            .await??;
        self.gc.notify_one();
        Ok(done)
    }

    pub async fn abort(&self, id: &str) -> Result<()> {
        let uk = u_key(id);
        let id = id.to_string();
        self.write(|t| {
            Box::pin(async move {
                if t.get(&uk).await?.is_none() {
                    return Ok(Err(S3Error::NoSuchUpload));
                }
                let lo = p_prefix(&id);
                let hi = crate::kv::prefix_end(&lo).unwrap();
                for (k, v) in scan(t, Bound::Included(&lo), &hi, usize::MAX).await? {
                    let p: Part = dec(&v, "part")?;
                    t.put(&body_key(b'D', p.body), b"").await?;
                    t.delete(&k).await?;
                }
                t.delete(&uk).await?;
                Ok(Ok(()))
            })
        })
        .await??;
        self.gc.notify_one();
        Ok(())
    }

    /// Uploads in progress (id, upload), after `after` (id), up to `max`.
    pub async fn uploads(&self, after: Option<&str>, max: usize) -> Result<Vec<(String, Upload)>> {
        let lo = after.map(u_key);
        self.read(|t| {
            Box::pin(async move {
                let b = match &lo {
                    Some(l) => Bound::Excluded(l.as_slice()),
                    None => Bound::Included(&b"U"[..]),
                };
                let mut out = Vec::new();
                for (k, v) in scan(t, b, b"V", max).await? {
                    out.push((
                        String::from_utf8_lossy(&k[1..]).into_owned(),
                        dec(&v, "upload")?,
                    ));
                }
                Ok(out)
            })
        })
        .await
    }
}

/// A body being written: chunked into the tree as it comes.
pub struct BodyWriter<D: BlockDevice + 'static> {
    bucket: Arc<Bucket<D>>,
    pub body: u64,
    chunk: u64,
    buf: Vec<u8>,
    pub size: u64,
    md5: Md5,
    /// Through the intent log (small bodies); otherwise committed at the
    /// end.
    logged: bool,
}

impl<D: BlockDevice + 'static> BodyWriter<D> {
    pub async fn write(&mut self, mut data: &[u8]) -> Result<()> {
        self.size += data.len() as u64;
        self.md5.update(data);
        while !data.is_empty() {
            let n = (CHUNK - self.buf.len()).min(data.len());
            self.buf.extend_from_slice(&data[..n]);
            data = &data[n..];
            if self.buf.len() == CHUNK {
                self.put_chunk().await?;
            }
        }
        Ok(())
    }

    async fn put_chunk(&mut self) -> Result<()> {
        let chunk = std::mem::take(&mut self.buf);
        let ds = self.bucket.tree.ds::<S3Error>().await?;
        {
            let _w = ds.ops.write().await;
            let mut t = Txn::begin(&self.bucket.tree.fs, &ds.obj).await?;
            t.put(&b_key(self.body, self.chunk), &chunk).await?;
            if self.logged {
                t.apply_logged()?;
                self.bucket.tree.settler.applied();
            } else {
                t.apply(None)?;
            }
        }
        self.chunk += 1;
        self.bucket.tree.fs.kv_settle().await?;
        Ok(())
    }

    /// Done: the body is in (durably, once the object pointing at it is
    /// written). Returns its MD5.
    pub async fn finish(mut self) -> Result<(Segment, [u8; 16])> {
        if !self.buf.is_empty() {
            self.put_chunk().await?;
        }
        if !self.logged && self.chunk > 0 {
            // The chunks went straight to the tree: commit them before
            // anything that's logged points at them.
            self.bucket.tree.fs.sync().await?;
        }
        let md5: [u8; 16] = std::mem::take(&mut self.md5).finalize().into();
        Ok((
            Segment {
                body: self.body,
                size: self.size,
            },
            md5,
        ))
    }

    /// Give up: the body goes.
    pub async fn abort(self) {
        let body = self.body;
        let r = self
            .bucket
            .write(|t| {
                Box::pin(async move {
                    t.delete(&body_key(b'W', body)).await?;
                    t.put(&body_key(b'D', body), b"").await?;
                    Ok(())
                })
            })
            .await;
        if let Err(e) = r {
            eprintln!(
                "s3 '{}': dropping body {}: {:?}",
                self.bucket.tree.name, body, e
            );
        }
        self.bucket.gc.notify_one();
    }
}

#[cfg(test)]
#[path = "../tests/s3_tests.rs"]
mod tests;
