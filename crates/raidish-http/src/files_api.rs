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
use crate::disk::{FileId, PoolDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::Dnode;
use crate::object::{ObjTree, Sizing};
use crate::object_lease::ObjectLeases;
use crate::types::{Holder, HolderRole};
use crate::vdev::Pool;
use crate::writer::ObjLease;
use axum::body::Body;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode as Status, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::stream::{BoxStream, StreamExt};
use raidish_auth::http::{Json, Query};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

pub use crate::agent::WriterSlot;

pub struct FilesState {
    pub node: String,
    pub writer: WriterSlot,
    /// For reads before this node has a writer (no committer yet).
    pub read_only: Option<Arc<FileSystem<Arc<Pool>>>>,
    pub leases: Arc<dyn ObjectLeases>,
    /// The largest upload (or PATCH body) taken, in bytes.
    pub upload_limit: u64,
}

fn unavailable() -> ApiError {
    ApiError::new(
        Status::SERVICE_UNAVAILABLE,
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
async fn file_dnode<D: PoolDevice>(
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
fn record_stream<D: PoolDevice + 'static>(
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
    futures_util::stream::iter(pieces)
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

pub(crate) use crate::frontend::http::{ByteRange, parse_range};

/// A file's bytes: all of them (`200`) or a range (`206`).
pub struct FileBody {
    stream: BoxStream<'static, Result<Vec<u8>, FileSystemError>>,
    /// Bytes the body holds.
    len: u64,
    part: Option<(std::ops::Range<u64>, u64)>,
}

impl IntoResponse for FileBody {
    fn into_response(self) -> Response {
        // Declared, so a body that ends short is an error to the client:
        // a piece that fails fails the body, and hyper then cuts the
        // connection (or resets the stream) with fewer bytes than declared
        // sent, instead of ending it as if the file ended there.
        let body = Body::from_stream(self.stream.map(|r| {
            r.map(axum::body::Bytes::from).map_err(|e| {
                eprintln!("files: reading records: {}", e);
                std::io::Error::other(e.to_string())
            })
        }));
        let mut r = Response::new(body);
        let h = r.headers_mut();
        h.insert(header::CONTENT_LENGTH, self.len.into());
        h.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
        h.insert(
            header::CONTENT_TYPE,
            "application/octet-stream".parse().unwrap(),
        );
        if let Some((range, size)) = self.part {
            h.insert(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", range.start, range.end - 1, size)
                    .parse()
                    .unwrap(),
            );
            *r.status_mut() = Status::PARTIAL_CONTENT;
        }
        r
    }
}

async fn file_body<D: PoolDevice + 'static>(
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
                Status::RANGE_NOT_SATISFIABLE,
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
        stream: futures_util::stream::iter(first).chain(stream).boxed(),
        len,
        part,
    })
}

/// A `{*path}` from the URL as a path (400 if it isn't one).
fn url_path(p: &str) -> Result<PathBuf, ApiError> {
    raidish_auth::http::path_buf(p).map_err(ApiError::from)
}

async fn get_file(
    _auth: crate::auth::CanRead,
    UrlPath(path): UrlPath<String>,
    headers: HeaderMap,
    State(st): State<Arc<FilesState>>,
) -> Result<FileBody, ApiError> {
    let path = url_path(&path)?;
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match st.reader()? {
        Reader::Writer(fs) => file_body(fs, &path, range).await,
        Reader::ReadOnly(fs) => file_body(fs, &path, range).await,
    }
}

pub use crate::api_types::{DirEntry, RenameRequest, Stat};

/// A file's `Stat`, from its dnode.
fn stat_of(id: FileId, d: &Dnode) -> Stat {
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

async fn list<D: PoolDevice>(
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

async fn list_root(
    _auth: crate::auth::CanRead,
    State(st): State<Arc<FilesState>>,
) -> Result<Json<Vec<DirEntry>>, ApiError> {
    list_any(&st, Path::new("")).await
}

async fn list_dir(
    _auth: crate::auth::CanRead,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
) -> Result<Json<Vec<DirEntry>>, ApiError> {
    list_any(&st, &url_path(&path)?).await
}

// ---- writes ----

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Written {
    pub id: FileId,
    pub size: u64,
}

/// Largest upload accepted (unless the `FilesState` says otherwise).
pub const UPLOAD_LIMIT: u64 = 64 << 30;
/// How much of the body is read at a time.
const UPLOAD_CHUNK: usize = 1 << 20;

/// Stream `data` into `path` under `lease`.
async fn upload(
    fs: &FileSystem<Pool>,
    path: &Path,
    data: Body,
    limit: u64,
    lease: ObjLease,
) -> Result<u64, FileSystemError> {
    let mut f = fs.file_writer(path, Some(lease)).await?;
    let mut body = raidish_auth::http::body_reader(data);
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
        if f.len() + n as u64 > limit {
            f.abort().await;
            return Err(FileSystemError::InvalidArgument(format!(
                "upload larger than the limit ({} bytes)",
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

async fn put_file(
    _auth: crate::auth::CanWrite,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
    data: Body,
) -> Result<Json<Written>, ApiError> {
    let path = url_path(&path)?;
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
        .map_err(|m| ApiError::new(Status::CONFLICT, m))?;
    let r = upload(&fs, &path, data, st.upload_limit, guard.lease()).await;
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
    data: Body,
    limit: u64,
    mut offset: u64,
    size: Option<u64>,
) -> Result<(), FileSystemError> {
    let mut body = raidish_auth::http::body_reader(data);
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
        if total > limit {
            return Err(FileSystemError::InvalidArgument(format!(
                "body larger than the limit ({} bytes)",
                limit
            )));
        }
        fs.file_write(f, offset, &buf[..n]).await?;
        // `file_write` refuses a range whose end isn't a size (I-31).
        offset = offset.checked_add(n as u64).ok_or_else(|| {
            FileSystemError::InvalidArgument("the body ends past the largest size".into())
        })?;
    }
    if let Some(size) = size {
        fs.file_truncate(f, size).await?;
    }
    Ok(())
}

/// `?offset=N&size=M` on a PATCH.
#[derive(Deserialize, Default)]
struct PatchAt {
    offset: Option<u64>,
    size: Option<u64>,
}

async fn patch_file(
    _auth: crate::auth::CanWrite,
    UrlPath(path): UrlPath<String>,
    Query(PatchAt { offset, size }): Query<PatchAt>,
    State(st): State<Arc<FilesState>>,
    data: Body,
) -> Result<Json<Written>, ApiError> {
    let path = url_path(&path)?;
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
        .map_err(|m| ApiError::new(Status::CONFLICT, m))?;
    let limit = st.upload_limit;
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

async fn stat<D: PoolDevice>(fs: &FileSystem<D>, path: &Path) -> Result<Stat, FileSystemError> {
    fs.refresh_root().await?;
    let (id, d) = fs.stat(path).await?;
    Ok(stat_of(id, &d))
}

async fn stat_path(
    _auth: crate::auth::CanRead,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
) -> Result<Json<Stat>, ApiError> {
    let path = url_path(&path)?;
    let r = match st.reader()? {
        Reader::Writer(fs) => stat(&fs, &path).await,
        Reader::ReadOnly(fs) => stat(&fs, &path).await,
    };
    r.map(Json).map_err(ApiError::from)
}

async fn rename(
    _auth: crate::auth::CanWrite,
    State(st): State<Arc<FilesState>>,
    req: Json<RenameRequest>,
) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.rename(Path::new(&req.from), Path::new(&req.to))
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn set_attrs(
    _auth: crate::auth::CanWrite,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
    req: Json<crate::inode::SetAttrs>,
) -> Result<Json<FileId>, ApiError> {
    let path = url_path(&path)?;
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.set_attrs(&path, &req)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn delete_file(
    _auth: crate::auth::CanWrite,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
) -> Result<Json<FileId>, ApiError> {
    let path = url_path(&path)?;
    let fs = st.writer()?;
    fs.refresh_root().await?;
    fs.delete(&path).await.map(Json).map_err(ApiError::from)
}

async fn make_dir(
    _auth: crate::auth::CanWrite,
    UrlPath(path): UrlPath<String>,
    State(st): State<Arc<FilesState>>,
) -> Result<Json<FileId>, ApiError> {
    let path = url_path(&path)?;
    let fs = st.writer()?;
    fs.mkdir(&path).await.map(Json).map_err(ApiError::from)
}

pub fn router(st: Arc<FilesState>) -> axum::Router {
    axum::Router::new()
        .route(
            "/api/v1/files/{*path}",
            get(get_file)
                .put(put_file)
                .patch(patch_file)
                .delete(delete_file),
        )
        .route("/api/v1/dir", get(list_root))
        .route("/api/v1/dir/{*path}", get(list_dir).put(make_dir))
        .route("/api/v1/stat/{*path}", get(stat_path))
        .route("/api/v1/rename", post(rename))
        .route("/api/v1/attrs/{*path}", axum::routing::patch(set_attrs))
        .with_state(st)
}

#[cfg(test)]
#[path = "tests/files_api_tests.rs"]
mod tests;
