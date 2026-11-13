//! The file API, served by every `listen` node. Any node takes any request;
//! no client needs to know which node commits the pool.
//!
//! * `GET /api/v1/files/<path>`: a file's contents (streamed).
//! * `PUT /api/v1/files/<path>`: create or replace a file with the body
//!   (streamed into this node's lent space record by record, under the
//!   file's `obj/<id>` lease, then committed).
//! * `DELETE /api/v1/files/<path>`: delete a file or directory.
//! * `GET /api/v1/dir[/<path>]`: list a directory.
//! * `PUT /api/v1/dir/<path>`: make a directory.
//!
//! Writes go through this node's writer (the agent's, see `crate::agent`),
//! which sends the committer only the namespace change. Reads are served
//! from this node, after catching up with the newest committed root
//! (close-to-open: a change is visible to every node once committed).

use crate::blkptr::BlockPtr;
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::object_resource;
use crate::disk::{BlockDevice, FileId};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::InodeType;
use crate::vdev::Pool;
use crate::writer::ObjLease;
use async_trait::async_trait;
use rocket::data::{Data, Limits, ToByteUnit};
use rocket::futures::stream::{BoxStream, StreamExt};
use rocket::http::Status;
use rocket::response::stream::ByteStream;
use rocket::serde::json::Json;
use rocket::{State, delete, get, put};
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

/// A file's size and records, as of the newest committed root.
async fn file_records<D: BlockDevice>(
    fs: &FileSystem<D>,
    path: &Path,
) -> Result<(u64, Vec<BlockPtr>), FileSystemError> {
    fs.refresh_root().await?;
    let r = fs.get_file_at_path(path).await?;
    match fs.read_inode(&r).await?.inode_type {
        InodeType::File { size_bytes, blocks } => Ok((size_bytes, blocks)),
        _ => Err(FileSystemError::NotAFile),
    }
}

/// Records read this many at a time while streaming a file out.
const STREAM_RECORDS: usize = 8;

fn record_stream<D: BlockDevice + 'static>(
    fs: Arc<FileSystem<D>>,
    size: u64,
    blocks: Vec<BlockPtr>,
) -> BoxStream<'static, Vec<u8>> {
    let groups: Vec<Vec<BlockPtr>> = blocks.chunks(STREAM_RECORDS).map(|c| c.to_vec()).collect();
    rocket::futures::stream::iter(groups)
        .then(move |g| {
            let fs = fs.clone();
            async move { fs.read_records(&g).await }
        })
        .scan(size, |left, r| {
            let out = match r {
                Ok(records) => {
                    let mut bytes: Vec<u8> = records.concat();
                    bytes.truncate(*left as usize);
                    *left -= bytes.len() as u64;
                    Some(bytes)
                }
                Err(e) => {
                    // The status is already sent: end the body short.
                    eprintln!("files: reading records: {}", e);
                    None
                }
            };
            std::future::ready(out)
        })
        .boxed()
}

#[get("/api/v1/files/<path..>")]
async fn get_file(
    path: PathBuf,
    st: &State<FilesState>,
) -> Result<ByteStream<BoxStream<'static, Vec<u8>>>, ApiError> {
    let stream = match st.reader()? {
        Reader::Writer(fs) => {
            let (size, blocks) = file_records(&fs, &path).await.map_err(api_err)?;
            record_stream(fs, size, blocks)
        }
        Reader::ReadOnly(fs) => {
            let (size, blocks) = file_records(&fs, &path).await.map_err(api_err)?;
            record_stream(fs, size, blocks)
        }
    };
    Ok(ByteStream(stream))
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    /// The pool-wide object number.
    pub id: FileId,
    /// `file`, `dir` or `volume`.
    pub kind: String,
    pub size: u64,
}

async fn list<D: BlockDevice>(
    fs: &FileSystem<D>,
    path: &Path,
) -> Result<Vec<DirEntry>, FileSystemError> {
    fs.refresh_root().await?;
    let index = fs.ls(path).await?;
    let mut out = Vec::new();
    for (name, r) in &index.files {
        let (kind, size) = match fs.read_inode(r).await?.inode_type {
            InodeType::File { size_bytes, .. } => ("file", size_bytes),
            InodeType::Directory { .. } => ("dir", 0),
            InodeType::Volume { size_bytes, .. } => ("volume", size_bytes),
        };
        out.push(DirEntry {
            name: name.clone(),
            id: r.file_id,
            kind: kind.into(),
            size,
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
async fn list_root(st: &State<FilesState>) -> Result<Json<Vec<DirEntry>>, ApiError> {
    list_any(st, Path::new("")).await
}

#[get("/api/v1/dir/<path..>")]
async fn list_dir(path: PathBuf, st: &State<FilesState>) -> Result<Json<Vec<DirEntry>>, ApiError> {
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

#[delete("/api/v1/files/<path..>")]
async fn delete_file(path: PathBuf, st: &State<FilesState>) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.refresh_root().await.map_err(api_err)?;
    fs.delete(&path).await.map(Json).map_err(api_err)
}

#[put("/api/v1/dir/<path..>")]
async fn make_dir(path: PathBuf, st: &State<FilesState>) -> Result<Json<FileId>, ApiError> {
    let fs = st.writer()?;
    fs.mkdir(&path).await.map(Json).map_err(api_err)
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        get_file,
        list_root,
        list_dir,
        put_file,
        delete_file,
        make_dir
    ]
}

#[cfg(test)]
#[path = "tests/files_api_tests.rs"]
mod tests;
