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

use crate::api_error::ApiError;
use crate::committer::object_resource;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Dnode;
use crate::object::{ObjTree, Sizing};
use crate::object_lease::ObjectLeases;
use crate::types::{Holder, HolderRole};
use crate::vdev::Pool;
use crate::writer::ObjLease;
use rocket::data::{Data, Limits, ToByteUnit};
use rocket::futures::stream::{BoxStream, StreamExt};
use rocket::http::Status;
use rocket::response::Responder;
use rocket::serde::json::Json;
use rocket::{State, delete, get, patch, post, put};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// This node's writer, once the agent has one.
pub type WriterSlot = Arc<std::sync::RwLock<Option<Arc<FileSystem<Pool>>>>>;

pub struct FilesState {
    pub node: String,
    pub writer: WriterSlot,
    /// For reads before this node has a writer (no committer yet).
    pub read_only: Option<Arc<FileSystem<Arc<Pool>>>>,
    pub leases: Arc<dyn ObjectLeases>,
}

fn unavailable() -> ApiError {
    ApiError::new(
        Status::ServiceUnavailable,
        "this node has no pool writer yet (is a committer running?)",
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
pub(crate) const STREAM_RECORDS: usize = 8;

/// Bytes `range` of the file, read a few records at a time. A piece that
/// can't be read is an error, which ends the stream.
fn record_stream<D: BlockDevice + 'static>(
    fs: Arc<FileSystem<D>>,
    dnode: Dnode,
    range: std::ops::Range<u64>,
) -> BoxStream<'static, Result<Vec<u8>, FileSystemError>> {
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
        .scan(false, |failed, r| {
            std::future::ready((!*failed).then(|| {
                *failed = r.is_err();
                r
            }))
        })
        .boxed()
}

/// A response body read from a stream of pieces. A piece that fails fails
/// the read, which ends the body short of its declared `Content-Length`:
/// the server then cuts the connection (or resets the stream) instead of
/// ending it as if the file ended there.
struct PieceReader {
    pieces: BoxStream<'static, Result<Vec<u8>, FileSystemError>>,
    buf: Vec<u8>,
    at: usize,
}

impl tokio::io::AsyncRead for PieceReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        loop {
            let me = &mut *self;
            if me.at < me.buf.len() {
                let n = out.remaining().min(me.buf.len() - me.at);
                out.put_slice(&me.buf[me.at..me.at + n]);
                me.at += n;
                return Poll::Ready(Ok(()));
            }
            match me.pieces.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(b))) => (me.buf, me.at) = (b, 0),
                Poll::Ready(Some(Err(e))) => {
                    eprintln!("files: reading records: {}", e);
                    return Poll::Ready(Err(std::io::Error::other(e.to_string())));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
            }
        }
    }
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

pub(crate) use crate::frontend::http::{ByteRange, parse_range};

/// A file's bytes: all of them (`200`) or a range (`206`).
pub struct FileBody {
    stream: BoxStream<'static, Result<Vec<u8>, FileSystemError>>,
    /// Bytes the body holds.
    len: u64,
    part: Option<(std::ops::Range<u64>, u64)>,
}

impl<'r> Responder<'r, 'r> for FileBody {
    fn respond_to(self, _req: &'r rocket::Request<'_>) -> rocket::response::Result<'r> {
        let mut r = rocket::Response::build()
            .streamed_body(PieceReader {
                pieces: self.stream,
                buf: Vec::new(),
                at: 0,
            })
            .finalize();
        // Declared, so a body that ends short is an error to the client:
        // Rocket ends a streamed body that fails as if it were complete (it
        // drops hyper's sender rather than aborting it), and hyper closes
        // the connection when fewer bytes than declared were sent.
        r.set_raw_header("Content-Length", self.len.to_string());
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
    let dnode = file_dnode(&fs, path).await?;
    let size = dnode.size;
    let (range, part) = match parse_range(range, size) {
        ByteRange::All => (0..size, None),
        ByteRange::Part(r) => (r.clone(), Some((r, size))),
        ByteRange::Unsatisfiable => {
            return Err(ApiError::new(
                Status::RangeNotSatisfiable,
                format!("bytes */{}", size),
            ));
        }
    };
    // The first piece is read before the status goes out, so a file that
    // can't be read from the start fails with an error status. A later
    // piece can only abort the body (see `PieceReader`).
    let len = range.end - range.start;
    let mut stream = record_stream(fs, dnode, range);
    let first = match stream.next().await {
        Some(Err(e)) => return Err(e.into()),
        first => first,
    };
    Ok(FileBody {
        stream: rocket::futures::stream::iter(first).chain(stream).boxed(),
        len,
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

pub use crate::api_types::{DirEntry, RenameRequest, Stat};

impl Stat {
    fn of(id: FileId, d: &Dnode) -> Self {
        Stat {
            id,
            kind: d.kind(),
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
            kind: d.kind(),
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
    r.map(Json).map_err(ApiError::from)
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
    fs.refresh_root().await?;
    let existed = match fs.file_id(&path).await {
        Ok(_) => true,
        Err(FileSystemError::FileNotFound) => false,
        Err(e) => return Err(e.into()),
    };
    // Make sure it exists, so there's an object to lease (like O_CREAT).
    let id = fs.create_file(&path).await?;
    // The file as made: only that, untouched since, is this upload's to
    // remove on failure. Another request may have made it first (and even
    // written it) since we looked; one with content is theirs.
    let made = if existed {
        None
    } else {
        file_dnode(&fs, &path)
            .await
            .ok()
            .filter(|d| d.size == 0 && d.tree.ptr.is_none())
    };
    let guard = st
        .leases
        .take(
            object_resource(id),
            Holder::new(HolderRole::Files, Some(&st.node))
                .with_detail(format!("upload {}", path.display())),
        )
        .await
        .map_err(|m| ApiError::new(Status::Conflict, m))?;
    let limit = limits
        .get("upload")
        .unwrap_or(DEFAULT_UPLOAD_LIMIT_GIB.gibibytes());
    let r = upload(&fs, &path, data, limit, guard.lease()).await;
    if r.is_err()
        && let Some(made) = made
    {
        // Don't leave the empty file this upload made behind. Under the
        // lease nobody else changes it now, but another upload of the same
        // path may have (holding the lease first) since it was made: then
        // it's theirs (audit N3).
        let now = file_dnode(&fs, &path).await.ok();
        if fs.file_id(&path).await.ok() == Some(id)
            && now.as_ref() == Some(&made)
            && let Err(e) = fs.delete_leased(&path, Some(guard.lease())).await
        {
            eprintln!("files: removing {}: {}", path.display(), e);
        }
    }
    guard.release().await;
    let size = r?;
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
    fs.refresh_root().await?;
    let id = fs.file_id(&path).await?;
    let guard = st
        .leases
        .take(
            object_resource(id),
            Holder::new(HolderRole::Files, Some(&st.node))
                .with_detail(format!("write {}", path.display())),
        )
        .await
        .map_err(|m| ApiError::new(Status::Conflict, m))?;
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
        // What's left from before is committed first, so what's dirty
        // after is this request's alone (the lease keeps other writers out).
        fs.file_sync(&f).await?;
        if let Err(e) = patch_body(&fs, &f, data, limit, offset.unwrap_or(0), size).await {
            // A body cut short or too big: not committed (and the file
            // isn't closed, which would commit it).
            fs.file_discard(&f).await;
            return Err(e);
        }
        let size = f.size();
        fs.close_file(&f).await.map(|_| size)
    }
    .await;
    guard.release().await;
    let size = r?;
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
    r.map(Json).map_err(ApiError::from)
}

#[post("/api/v1/rename", data = "<req>")]
async fn rename(
    _auth: crate::auth::CanWrite,
    req: Json<RenameRequest>,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.rename(Path::new(&req.from), Path::new(&req.to))
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[patch("/api/v1/attrs/<path..>", data = "<req>")]
async fn set_attrs(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    req: Json<crate::inode::SetAttrs>,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.set_attrs(&path, &req)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[delete("/api/v1/files/<path..>")]
async fn delete_file(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.delete(&path).await.map(Json).map_err(ApiError::from)
}

#[put("/api/v1/dir/<path..>")]
async fn make_dir(
    _auth: crate::auth::CanWrite,
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.mkdir(&path).await.map(Json).map_err(ApiError::from)
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
