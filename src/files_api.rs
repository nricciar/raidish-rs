//! The file API, served by every `listen` node. Any node takes any request;
//! no client needs to know which node commits the pool.
//!
//! * `GET /api/v1/files/<path>`: a file's contents (streamed). A `Range:
//!   bytes=...` header (one range) gets just that part (`206`).
//! * `PUT /api/v1/files/<path>`: create or replace a file with the body
//!   (streamed into this node's lent space record by record, under the
//!   file's `obj/<id>` lease, then committed).
//! * `PATCH /api/v1/files/<path>?offset=N[&size=M]`: write the body at byte
//!   `N` of an existing file (extending it if need be), then, with `size`,
//!   cut it short or extend it with zeros to `M` bytes. Under the file's
//!   `obj/<id>` lease; committed before the answer.
//! * `DELETE /api/v1/files/<path>`: delete a file or an empty directory.
//! * `GET /api/v1/stat/<path>`: an object's size and attributes.
//! * `POST /api/v1/rename` (`{"from": ..., "to": ...}`): move or rename;
//!   `to` may replace a file, or an empty directory.
//! * `PATCH /api/v1/attrs/<path>` (`{"mode", "uid", "gid", "mtime_ns"}`, each
//!   optional): change attributes.
//! * `GET /api/v1/dir[/<path>]`: list a directory.
//! * `PUT /api/v1/dir/<path>`: make a directory.
//!
//! Writes go through this node's writer (the agent's, see `crate::agent`),
//! which sends the committer only the namespace change. Reads are served
//! from this node, after catching up with the newest committed root
//! (close-to-open: a change is visible to every node once committed).

use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::object_resource;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Dnode;
use crate::object::{ObjTree, Sizing};
use crate::vdev::Pool;
use crate::writer::ObjLease;
use async_trait::async_trait;
use rocket::data::{Data, Limits, ToByteUnit};
use rocket::futures::stream::{BoxStream, StreamExt};
use rocket::http::Status;
use rocket::response::Responder;
use rocket::response::stream::ByteStream;
use rocket::serde::json::Json;
use rocket::{State, delete, get, patch, post, put};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// This node's writer, once the agent has one.
pub type WriterSlot = Arc<std::sync::RwLock<Option<Arc<FileSystem<Pool>>>>>;

/// A held `obj/<id>` lease.
#[async_trait]
pub trait ObjectGuard: Send + Sync {
    fn lease(&self) -> ObjLease;
    async fn release(&self);
}

/// Where uploads take their `obj/<id>` leases.
#[async_trait]
pub trait ObjectLeases: Send + Sync {
    /// Take `resource` (and keep renewing it until released), or say who has it.
    async fn take(&self, resource: &str, desc: &str) -> Result<Box<dyn ObjectGuard>, String>;
}

struct Held(Arc<HeldLease>);

#[async_trait]
impl ObjectGuard for Held {
    fn lease(&self) -> ObjLease {
        ObjLease {
            owner: self.0.owner.clone(),
            epoch: self.0.epoch,
        }
    }
    async fn release(&self) {
        self.0.release().await
    }
}

#[async_trait]
impl ObjectLeases for LeaseClient {
    async fn take(&self, resource: &str, desc: &str) -> Result<Box<dyn ObjectGuard>, String> {
        let held = Arc::new(HeldLease::acquire(self.clone(), resource, desc).await?);
        // If it's lost, the upload's commit is refused: nothing to do here.
        held.hold(|m| eprintln!("files: {}", m));
        Ok(Box::new(Held(held)))
    }
}

pub struct FilesState {
    pub node: String,
    pub writer: WriterSlot,
    /// For reads before this node has a writer (no committer yet).
    pub read_only: Option<Arc<FileSystem<Arc<Pool>>>>,
    pub leases: Arc<dyn ObjectLeases>,
}

type ApiError = (Status, String);

fn api_err(e: FileSystemError) -> ApiError {
    let status = match &e {
        FileSystemError::FileNotFound | FileSystemError::DirectoryNotFound => Status::NotFound,
        FileSystemError::NotAFile
        | FileSystemError::NotADirectory
        | FileSystemError::InvalidArgument(_) => Status::BadRequest,
        FileSystemError::AlreadyExists | FileSystemError::Conflict(_) => Status::Conflict,
        FileSystemError::Unavailable(_) | FileSystemError::NotFormatted => {
            Status::ServiceUnavailable
        }
        FileSystemError::UnableToAllocateBlocks(_) => Status::InsufficientStorage,
        _ => Status::InternalServerError,
    };
    (status, e.to_string())
}

fn unavailable() -> ApiError {
    (
        Status::ServiceUnavailable,
        "this node has no pool writer yet (is a committer running?)".into(),
    )
}

enum Reader {
    Writer(Arc<FileSystem<Pool>>),
    ReadOnly(Arc<FileSystem<Arc<Pool>>>),
}

impl FilesState {
    fn writer(&self) -> Result<Arc<FileSystem<Pool>>, ApiError> {
        self.writer.read().unwrap().clone().ok_or_else(unavailable)
    }

    fn reader(&self) -> Result<Reader, ApiError> {
        if let Some(fs) = self.writer.read().unwrap().clone() {
            return Ok(Reader::Writer(fs));
        }
        self.read_only
            .clone()
            .map(Reader::ReadOnly)
            .ok_or_else(unavailable)
    }
}

// ---- reads ----

/// A file's dnode, as of the newest committed root.
async fn file_dnode<D: BlockDevice>(
    fs: &FileSystem<D>,
    path: &Path,
) -> Result<Dnode, FileSystemError> {
    fs.refresh_root().await?;
    Ok(fs.stat_file(path).await?.1)
}

/// Records read this many at a time while streaming a file out.
const STREAM_RECORDS: usize = 8;

/// Bytes `range` of the file, read a few records at a time.
fn record_stream<D: BlockDevice + 'static>(
    fs: Arc<FileSystem<D>>,
    dnode: Dnode,
    range: std::ops::Range<u64>,
) -> BoxStream<'static, Vec<u8>> {
    let chunk = dnode.blksz as u64 * STREAM_RECORDS as u64;
    let obj = Arc::new(ObjTree::new(
        dnode.blksz as usize,
        Sizing::Fixed,
        dnode.size,
        dnode.tree,
    ));
    let end = range.end;
    // Pieces end on chunk boundaries, so each reads whole records.
    let pieces = std::iter::successors(Some(range.start), move |&at| {
        let next = (at / chunk + 1) * chunk;
        (next < end).then_some(next)
    })
    .filter(move |&at| at < end);
    rocket::futures::stream::iter(pieces)
        .then(move |offset| {
            let (fs, obj) = (fs.clone(), obj.clone());
            async move {
                let len = ((offset / chunk + 1) * chunk).min(end) - offset;
                let mut buf = vec![0u8; len as usize];
                fs.object_read(&obj, offset, &mut buf).await.map(|_| buf)
            }
        })
        .scan((), |_, r| {
            std::future::ready(match r {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    // The status is already sent: end the body short.
                    eprintln!("files: reading records: {}", e);
                    None
                }
            })
        })
        .boxed()
}

/// The request's `Range` header, if any.
pub struct RangeHeader(Option<String>);

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for RangeHeader {
    type Error = std::convert::Infallible;
    async fn from_request(
        req: &'r rocket::Request<'_>,
    ) -> rocket::request::Outcome<Self, Self::Error> {
        rocket::request::Outcome::Success(RangeHeader(
            req.headers().get_one("Range").map(String::from),
        ))
    }
}

/// What a `Range` header asks of a `size`-byte file.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ByteRange {
    /// No (usable) range: the whole file.
    All,
    Part(std::ops::Range<u64>),
    /// Nothing of the file is in it (`416`).
    Unsatisfiable,
}

/// Parse `bytes=a-b`, `bytes=a-` or `bytes=-n`. Anything else (another
/// unit, several ranges, garbage) is ignored, as RFC 9110 allows.
pub(crate) fn parse_range(header: Option<&str>, size: u64) -> ByteRange {
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

/// A file's bytes: all of them (`200`) or a range (`206`).
pub struct FileBody {
    stream: BoxStream<'static, Vec<u8>>,
    part: Option<(std::ops::Range<u64>, u64)>,
}

impl<'r> Responder<'r, 'r> for FileBody {
    fn respond_to(self, req: &'r rocket::Request<'_>) -> rocket::response::Result<'r> {
        let mut r = ByteStream(self.stream).respond_to(req)?;
        r.set_raw_header("Accept-Ranges", "bytes");
        r.set_raw_header("Content-Type", "application/octet-stream");
        if let Some((range, size)) = self.part {
            r.set_status(Status::PartialContent);
            r.set_raw_header(
                "Content-Range",
                format!("bytes {}-{}/{}", range.start, range.end - 1, size),
            );
        }
        Ok(r)
    }
}

async fn file_body<D: BlockDevice + 'static>(
    fs: Arc<FileSystem<D>>,
    path: &Path,
    range: Option<&str>,
) -> Result<FileBody, ApiError> {
    let dnode = file_dnode(&fs, path).await.map_err(api_err)?;
    let size = dnode.size;
    let (range, part) = match parse_range(range, size) {
        ByteRange::All => (0..size, None),
        ByteRange::Part(r) => (r.clone(), Some((r, size))),
        ByteRange::Unsatisfiable => {
            return Err((Status::RangeNotSatisfiable, format!("bytes */{}", size)));
        }
    };
    Ok(FileBody {
        stream: record_stream(fs, dnode, range),
        part,
    })
}

#[get("/api/v1/files/<path..>")]
async fn get_file(
    _auth: crate::auth::CanRead,
    path: PathBuf,
    range: RangeHeader,
    st: &State<FilesState>,
) -> Result<FileBody, ApiError> {
    match st.reader()? {
        Reader::Writer(fs) => file_body(fs, &path, range.0.as_deref()).await,
        Reader::ReadOnly(fs) => file_body(fs, &path, range.0.as_deref()).await,
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    /// The pool-wide object number.
    pub id: FileId,
    /// `file`, `dir` or `volume`.
    pub kind: String,
    pub size: u64,
    #[serde(default)]
    pub mode: u16,
    /// Last modification, ns since the epoch.
    #[serde(default)]
    pub mtime_ns: u64,
}

/// An object's size and attributes (`GET /api/v1/stat`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Stat {
    pub id: FileId,
    pub kind: String,
    pub size: u64,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub mtime_ns: u64,
    pub ctime_ns: u64,
}

impl Stat {
    fn of(id: FileId, d: &Dnode) -> Self {
        Stat {
            id,
            kind: d.kind.name().into(),
            size: if d.is_directory() { 0 } else { d.size },
            mode: d.attrs.mode,
            uid: d.attrs.uid,
            gid: d.attrs.gid,
            nlink: d.attrs.nlink,
            mtime_ns: d.attrs.mtime_ns,
            ctime_ns: d.attrs.ctime_ns,
        }
    }
}

async fn list<D: BlockDevice>(
    fs: &FileSystem<D>,
    path: &Path,
) -> Result<Vec<DirEntry>, FileSystemError> {
    fs.refresh_root().await?;
    let index = fs.ls(path).await?;
    let mut out = Vec::new();
    for (name, e) in &index.files {
        let d = fs.read_dnode(e.id).await?;
        out.push(DirEntry {
            name: name.clone(),
            id: e.id,
            kind: d.kind.name().into(),
            size: if d.is_directory() { 0 } else { d.size },
            mode: d.attrs.mode,
            mtime_ns: d.attrs.mtime_ns,
        });
    }
    Ok(out)
}

async fn list_any(st: &FilesState, path: &Path) -> Result<Json<Vec<DirEntry>>, ApiError> {
    let r = match st.reader()? {
        Reader::Writer(fs) => list(&fs, path).await,
        Reader::ReadOnly(fs) => list(&fs, path).await,
    };
    r.map(Json).map_err(api_err)
}

#[get("/api/v1/dir")]
async fn list_root(
    _auth: crate::auth::CanRead,
    st: &State<FilesState>,
) -> Result<Json<Vec<DirEntry>>, ApiError> {
    list_any(st, Path::new("")).await
}

#[get("/api/v1/dir/<path..>")]
async fn list_dir(
    _auth: crate::auth::CanRead,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<Vec<DirEntry>>, ApiError> {
    list_any(st, &path).await
}

// ---- writes ----

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Written {
    pub id: FileId,
    pub size: u64,
}

/// Largest upload accepted unless the `upload` limit says otherwise.
const DEFAULT_UPLOAD_LIMIT_GIB: u64 = 64;
/// How much of the body is read at a time.
const UPLOAD_CHUNK: usize = 1 << 20;

/// Stream `data` into `path` under `lease`.
async fn upload(
    fs: &FileSystem<Pool>,
    path: &Path,
    data: Data<'_>,
    limit: rocket::data::ByteUnit,
    lease: ObjLease,
) -> Result<u64, FileSystemError> {
    let mut f = fs.file_writer(path, Some(lease)).await?;
    // One byte over the limit tells a body that's too big from one that fits.
    let mut body = data.open(limit + 1.bytes());
    let mut buf = vec![0u8; UPLOAD_CHUNK];
    loop {
        let n = match body.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                f.abort().await;
                return Err(FileSystemError::InvalidArgument(format!(
                    "reading the upload: {}",
                    e
                )));
            }
        };
        if f.len() + n as u64 > limit.as_u64() {
            f.abort().await;
            return Err(FileSystemError::InvalidArgument(format!(
                "upload larger than the limit ({})",
                limit
            )));
        }
        if let Err(e) = f.write(&buf[..n]).await {
            f.abort().await;
            return Err(e);
        }
    }
    let size = f.len();
    f.finish().await?;
    Ok(size)
}

#[put("/api/v1/files/<path..>", data = "<data>")]
async fn put_file(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    data: Data<'_>,
    limits: &Limits,
    st: &State<FilesState>,
) -> Result<Json<Written>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    let existed = match fs.file_id(&path).await {
        Ok(_) => true,
        Err(FileSystemError::FileNotFound) => false,
        Err(e) => return Err(api_err(e)),
    };
    // Make sure it exists, so there's an object to lease (like O_CREAT).
    let id = fs.create_file(&path).await.map_err(api_err)?;
    let guard = st
        .leases
        .take(
            &object_resource(id),
            &format!("upload {} {}", st.node, path.display()),
        )
        .await
        .map_err(|m| (Status::Conflict, m))?;
    let limit = limits
        .get("upload")
        .unwrap_or(DEFAULT_UPLOAD_LIMIT_GIB.gibibytes());
    let r = upload(&fs, &path, data, limit, guard.lease()).await;
    if r.is_err() && !existed {
        // Don't leave the empty file this upload made behind (still under
        // the lease, so it's still ours).
        if let Err(e) = fs.delete_leased(&path, Some(guard.lease())).await {
            eprintln!("files: removing {}: {}", path.display(), e);
        }
    }
    guard.release().await;
    let size = r.map_err(api_err)?;
    Ok(Json(Written { id, size }))
}

/// Write `data` at `offset` of the open file `f`, then set its size.
async fn patch_body(
    fs: &FileSystem<Pool>,
    f: &crate::open_file::OpenFile,
    data: Data<'_>,
    limit: rocket::data::ByteUnit,
    mut offset: u64,
    size: Option<u64>,
) -> Result<(), FileSystemError> {
    let mut body = data.open(limit + 1.bytes());
    let mut buf = vec![0u8; UPLOAD_CHUNK];
    let mut total = 0u64;
    loop {
        let n = match body.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                return Err(FileSystemError::InvalidArgument(format!(
                    "reading the body: {}",
                    e
                )));
            }
        };
        total += n as u64;
        if total > limit.as_u64() {
            return Err(FileSystemError::InvalidArgument(format!(
                "body larger than the limit ({})",
                limit
            )));
        }
        fs.file_write(f, offset, &buf[..n]).await?;
        offset += n as u64;
    }
    if let Some(size) = size {
        fs.file_truncate(f, size).await?;
    }
    Ok(())
}

#[patch("/api/v1/files/<path..>?<offset>&<size>", data = "<data>")]
async fn patch_file(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    offset: Option<u64>,
    size: Option<u64>,
    data: Data<'_>,
    limits: &Limits,
    st: &State<FilesState>,
) -> Result<Json<Written>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    let id = fs.file_id(&path).await.map_err(api_err)?;
    let guard = st
        .leases
        .take(
            &object_resource(id),
            &format!("write {} {}", st.node, path.display()),
        )
        .await
        .map_err(|m| (Status::Conflict, m))?;
    let limit = limits
        .get("upload")
        .unwrap_or(DEFAULT_UPLOAD_LIMIT_GIB.gibibytes());
    let r = async {
        let f = fs.open_file(&path, Some(guard.lease())).await?;
        if f.id != id {
            return Err(FileSystemError::Conflict(format!(
                "{} was replaced meanwhile",
                path.display()
            )));
        }
        let written = patch_body(&fs, &f, data, limit, offset.unwrap_or(0), size).await;
        // Commit what was written either way (or drop it on a conflict).
        let closed = fs.close_file(&f).await;
        written.and(closed).map(|_| f.size())
    }
    .await;
    guard.release().await;
    let size = r.map_err(api_err)?;
    Ok(Json(Written { id, size }))
}

async fn stat<D: BlockDevice>(fs: &FileSystem<D>, path: &Path) -> Result<Stat, FileSystemError> {
    fs.refresh_root().await?;
    let (id, d) = fs.stat(path).await?;
    Ok(Stat::of(id, &d))
}

#[get("/api/v1/stat/<path..>")]
async fn stat_path(
    _auth: crate::auth::CanRead,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<Stat>, ApiError> {
    let r = match st.reader()? {
        Reader::Writer(fs) => stat(&fs, &path).await,
        Reader::ReadOnly(fs) => stat(&fs, &path).await,
    };
    r.map(Json).map_err(api_err)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameRequest {
    pub from: String,
    pub to: String,
}

#[post("/api/v1/rename", data = "<req>")]
async fn rename(
    _auth: crate::auth::CanWrite,
    req: Json<RenameRequest>,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    fs.rename(Path::new(&req.from), Path::new(&req.to))
        .await
        .map(Json)
        .map_err(api_err)
}

#[patch("/api/v1/attrs/<path..>", data = "<req>")]
async fn set_attrs(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    req: Json<crate::inode::SetAttrs>,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    fs.set_attrs(&path, &req).await.map(Json).map_err(api_err)
}

#[delete("/api/v1/files/<path..>")]
async fn delete_file(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    fs.delete(&path).await.map(Json).map_err(api_err)
}

#[put("/api/v1/dir/<path..>")]
async fn make_dir(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.mkdir(&path).await.map(Json).map_err(api_err)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        get_file,
        list_root,
        list_dir,
        put_file,
        patch_file,
        delete_file,
        make_dir,
        stat_path,
        rename,
        set_attrs
    ]
}

#[cfg(test)]
#[path = "tests/files_api_tests.rs"]
mod tests;
