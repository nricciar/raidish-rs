//! What any NFS version needs from the pool, by object number.
//!
//! * **File handles** are the pool GUID and the object number. Object
//!   numbers are never reused and mean the same on every node, so a handle
//!   stays valid across restarts and on whichever node a client fails over
//!   to. An object that no longer exists is `STALE`.
//! * **Unstable writes** go to this node's open file (see
//!   `crate::open_file`) and become durable with a commit. The write
//!   verifier is per process: a client that resends `COMMIT` after this
//!   node restarted, or to another node, gets a different one and resends
//!   whatever wasn't committed.
//! * **One node writes a file at a time.** The first write here takes the
//!   file's `obj/<id>` lease (on a cluster). While another node holds it for
//!   NFS, whatever depends on the file's unsynced contents (`WRITE`,
//!   `COMMIT`, `READ`, attributes, truncating) is forwarded there (see
//!   `Peers`), and answered with that node's write verifier: the one that
//!   has the data. A file idle for a while is committed and closed, and its
//!   lease let go, so another node can take over.

use crate::committer::object_resource;
use crate::disk::{FileId, PoolDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::{Dnode, ObjType, SetAttrs};
use crate::object::{ObjTree, Sizing};
use crate::object_lease::{ObjectGuard, ObjectLeases};
use crate::open_file::OpenFile;
use raidish_core::role::Mutable;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// NFS status codes (NFSv3's numbering; v4 shares most of them).
/// An NFS status (v3 `nfsstat3`, v4 `nfsstat4`: one code space), as sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct NfsStat(u32);

impl NfsStat {
    pub const fn code(self) -> u32 {
        self.0
    }

    /// A status as a peer sent it.
    pub const fn from_code(code: u32) -> Self {
        NfsStat(code)
    }

    #[cfg(test)]
    pub fn is_ok(self) -> bool {
        self.0 == 0
    }
}

/// Tests read statuses off the wire as numbers.
#[cfg(test)]
impl PartialEq<NfsStat> for u32 {
    fn eq(&self, s: &NfsStat) -> bool {
        *self == s.0
    }
}

impl std::fmt::Display for NfsStat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NFS status {}", self.0)
    }
}

impl crate::xdr::Writer {
    /// Write status `s`.
    pub fn stat(&mut self, s: NfsStat) -> &mut Self {
        self.u32(s.code())
    }
}

/// NFS status codes.
pub mod status {
    use super::NfsStat;
    pub const OK: NfsStat = NfsStat::from_code(0);
    pub const PERM: NfsStat = NfsStat::from_code(1);
    pub const NOENT: NfsStat = NfsStat::from_code(2);
    pub const IO: NfsStat = NfsStat::from_code(5);
    pub const NXIO: NfsStat = NfsStat::from_code(6);
    pub const EXIST: NfsStat = NfsStat::from_code(17);
    pub const NOTDIR: NfsStat = NfsStat::from_code(20);
    pub const ISDIR: NfsStat = NfsStat::from_code(21);
    pub const INVAL: NfsStat = NfsStat::from_code(22);
    pub const FBIG: NfsStat = NfsStat::from_code(27);
    pub const NOSPC: NfsStat = NfsStat::from_code(28);
    pub const NAMETOOLONG: NfsStat = NfsStat::from_code(63);
    pub const NOTEMPTY: NfsStat = NfsStat::from_code(66);
    pub const STALE: NfsStat = NfsStat::from_code(70);
    pub const BADHANDLE: NfsStat = NfsStat::from_code(10001);
    pub const NOT_SYNC: NfsStat = NfsStat::from_code(10002);
    pub const BAD_COOKIE: NfsStat = NfsStat::from_code(10003);
    pub const NOTSUPP: NfsStat = NfsStat::from_code(10004);
    pub const TOOSMALL: NfsStat = NfsStat::from_code(10005);
    pub const SERVERFAULT: NfsStat = NfsStat::from_code(10006);
    pub const JUKEBOX: NfsStat = NfsStat::from_code(10008);
}

pub type NfsResult<T> = Result<T, NfsStat>;

/// What a failed pool call means to an NFS client.
pub fn nfs_status(e: &FileSystemError) -> NfsStat {
    use crate::types::ErrorKind as K;
    match e.kind() {
        K::NotFound => status::NOENT,
        K::NotDir => status::NOTDIR,
        K::NotFile => status::ISDIR,
        K::Exists => status::EXIST,
        K::NotEmpty => status::NOTEMPTY,
        K::NoSpace => status::NOSPC,
        K::Invalid => status::INVAL,
        K::Forbidden => status::PERM,
        // Someone else holds the object, or no committer right now: later.
        K::Conflict | K::Unavailable => status::JUKEBOX,
        K::Internal => status::IO,
    }
}

fn err(e: FileSystemError) -> NfsStat {
    nfs_status(&e)
}

/// An object's attributes, as NFS reports them.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Attr {
    pub id: FileId,
    pub kind: ObjType,
    pub mode: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: u64,
    pub ctime_ns: u64,
    pub fsid: u64,
    /// See `Dnode::change`.
    pub change: u64,
}

/// A request one node's NFS server passes to the node that writes the file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Forward {
    Getattr {
        id: FileId,
    },
    Read {
        id: FileId,
        offset: u64,
        count: u32,
    },
    Write {
        id: FileId,
        offset: u64,
        data: Vec<u8>,
        stable: bool,
    },
    Commit {
        id: FileId,
    },
    Truncate {
        id: FileId,
        size: u64,
    },
    Seek {
        id: FileId,
        offset: u64,
        data: bool,
    },
    Deallocate {
        id: FileId,
        offset: u64,
        len: u64,
    },
}

/// The answer to a `Forward`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Forwarded {
    Attr(Attr),
    Read {
        data: Vec<u8>,
        eof: bool,
    },
    Write {
        attr: Attr,
        verifier: [u8; 8],
    },
    Commit {
        verifier: [u8; 8],
    },
    Seek {
        offset: u64,
        eof: bool,
    },
    Done,
    /// An NFS status.
    Err(NfsStat),
}

/// The other nodes' NFS servers.
#[async_trait::async_trait]
pub trait Peers: Send + Sync {
    /// The node (by name) holding object `id`'s write lease for NFS, if any.
    fn holder(&self, id: FileId) -> Option<String>;
    /// Have `node` do `req`.
    async fn forward(&self, node: &str, req: &Forward) -> Result<Forwarded, String>;
}

/// A directory entry, as `readdir` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub id: FileId,
    pub kind: ObjType,
}

/// Commit an open file once it has had unsynced writes this long.
const SYNC_AFTER: Duration = Duration::from_secs(5);
/// Commit and close an open file (letting its lease go) once it has gone
/// this long without a write (by default; see `with_close_after`).
const CLOSE_AFTER: Duration = Duration::from_secs(3);
/// Catch up with the committed state at most this often (writer nodes).
const REFRESH_EVERY: Duration = Duration::from_millis(100);

/// A file this node writes.
struct Held {
    file: Arc<OpenFile>,
    guard: Option<Box<dyn ObjectGuard>>,
    last_write: Instant,
    dirty_since: Option<Instant>,
}

/// Where the pool is (on a `listen` node: its writer, once it has one).
pub type FsSource<D, R> = Arc<dyn Fn() -> Option<Arc<FileSystem<D, R>>> + Send + Sync>;

pub struct Backend<D: PoolDevice, R: Mutable> {
    source: FsSource<D, R>,
    leases: Option<Arc<dyn ObjectLeases>>,
    node: String,
    /// This node's write verifier: changed whenever unstable writes
    /// acknowledged here may have been lost, so clients resend them.
    verifier: std::sync::Mutex<[u8; 8]>,
    held: tokio::sync::Mutex<HashMap<FileId, Held>>,
    /// Held for writing while `tidy` closes a file (from leaving `held` to
    /// a failed close's new verifier), for reading by `commit_local`: a
    /// COMMIT never finds the file gone and the verifier not yet changed,
    /// and answers with the old one for writes that were lost (audit N1).
    closing: tokio::sync::RwLock<()>,
    last_refresh: std::sync::Mutex<Option<Instant>>,
    close_after: Duration,
    peers: Option<Arc<dyn Peers>>,
}

impl<D: PoolDevice + 'static, R: Mutable> Backend<D, R> {
    /// `leases`: where per-file write leases come from (None: a pool only
    /// this process writes).
    pub fn new(
        source: FsSource<D, R>,
        leases: Option<Arc<dyn ObjectLeases>>,
        node: String,
    ) -> Self {
        Backend {
            source,
            leases,
            verifier: std::sync::Mutex::new(
                crate::fs::new_guid(std::process::id() as u64).to_be_bytes(),
            ),
            node,
            held: Default::default(),
            closing: Default::default(),
            last_refresh: Default::default(),
            close_after: CLOSE_AFTER,
            peers: None,
        }
    }

    /// Forward to the other nodes what depends on a file one of them writes.
    pub fn with_peers(mut self, peers: Arc<dyn Peers>) -> Self {
        self.peers = Some(peers);
        self
    }

    /// The node (by name) writing object `id` over NFS, if any (this one
    /// included).
    pub async fn writer_of(&self, id: FileId) -> Option<String> {
        if self.held.lock().await.contains_key(&id) {
            return Some(self.node.clone());
        }
        self.peers.as_ref()?.holder(id)
    }

    /// The other node writing object `id`, if one is.
    async fn remote(&self, id: FileId) -> Option<String> {
        let peers = self.peers.as_ref()?;
        if self.held.lock().await.contains_key(&id) {
            return None;
        }
        peers.holder(id).filter(|n| *n != self.node)
    }

    /// Have `node` do `req`. A node that can't be reached is a "later".
    async fn forward(&self, node: &str, req: Forward) -> NfsResult<Forwarded> {
        let peers = self.peers.as_ref().ok_or(status::SERVERFAULT)?;
        match peers.forward(node, &req).await {
            Ok(Forwarded::Err(st)) => Err(st),
            Ok(r) => Ok(r),
            Err(e) => {
                eprintln!("nfs: forwarding to {}: {}", node, e);
                Err(status::JUKEBOX)
            }
        }
    }

    /// Do a request another node forwarded here (never forwarded again).
    pub async fn serve_forward(&self, req: Forward) -> Forwarded {
        let r = match req {
            Forward::Getattr { id } => self.getattr_local(id).await.map(Forwarded::Attr),
            Forward::Read { id, offset, count } => self
                .read_local(id, offset, count)
                .await
                .map(|(data, eof)| Forwarded::Read { data, eof }),
            Forward::Write {
                id,
                offset,
                data,
                stable,
            } => self
                .write_local(id, offset, &data, stable)
                .await
                .map(|(attr, verifier)| Forwarded::Write { attr, verifier }),
            Forward::Commit { id } => self
                .commit_local(id)
                .await
                .map(|verifier| Forwarded::Commit { verifier }),
            Forward::Truncate { id, size } => {
                self.truncate_local(id, size).await.map(|_| Forwarded::Done)
            }
            Forward::Seek { id, offset, data } => self
                .seek_local(id, offset, data)
                .await
                .map(|(offset, eof)| Forwarded::Seek { offset, eof }),
            Forward::Deallocate { id, offset, len } => self
                .deallocate_local(id, offset, len)
                .await
                .map(|_| Forwarded::Done),
        };
        r.unwrap_or_else(Forwarded::Err)
    }

    /// How long a file goes without a write before it is committed and
    /// closed (and another node may write it).
    #[cfg(test)]
    pub fn with_close_after(mut self, d: Duration) -> Self {
        self.close_after = d;
        self
    }

    /// This node's name.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// This node's current write verifier.
    pub fn verifier(&self) -> [u8; 8] {
        *self.verifier.lock().unwrap()
    }

    /// Unstable writes acknowledged with the current verifier may be gone
    /// (dropped on a conflict, a failed close, written to a file closed
    /// meanwhile): a new verifier makes every client resend what it hasn't
    /// seen committed.
    fn lost_writes(&self, id: FileId, why: &str) {
        let mut v = self.verifier.lock().unwrap();
        let old = u64::from_be_bytes(*v);
        *v = crate::fs::new_guid(old ^ id.get()).to_be_bytes();
        eprintln!("nfs: object {}: {}; new write verifier", id, why);
    }

    /// Whether `f` is still the file the pool has open as `id` (writes to
    /// it can still be committed).
    fn still_open(fs: &FileSystem<D, R>, id: FileId, f: &Arc<OpenFile>) -> bool {
        !f.dropped() && fs.opened(id).is_some_and(|o| Arc::ptr_eq(&o, f))
    }

    /// The pool, caught up with what's committed.
    pub async fn fs(&self) -> NfsResult<Arc<FileSystem<D, R>>> {
        let fs = (self.source)().ok_or(status::JUKEBOX)?;
        let due = {
            let mut last = self.last_refresh.lock().unwrap();
            let due = last.is_none_or(|t| t.elapsed() >= REFRESH_EVERY);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            fs.refresh_root().await.map_err(err)?;
        }
        Ok(fs)
    }

    async fn pool_guid(&self, fs: &FileSystem<D, R>) -> u64 {
        fs.superblock().pool_guid
    }

    // ---- handles ----

    pub async fn handle(&self, id: FileId) -> NfsResult<Vec<u8>> {
        let fs = self.fs().await?;
        let mut h = b"RDH1".to_vec();
        h.extend_from_slice(&self.pool_guid(&fs).await.to_be_bytes());
        h.extend_from_slice(&id.get().to_be_bytes());
        Ok(h)
    }

    /// The object a handle names (`BADHANDLE` if it isn't ours, `STALE` if
    /// it's from another pool or the object is gone).
    pub async fn object(&self, fh: &[u8]) -> NfsResult<FileId> {
        if fh.len() != 20 || &fh[..4] != b"RDH1" {
            return Err(status::BADHANDLE);
        }
        let fs = self.fs().await?;
        let guid = u64::from_be_bytes(fh[4..12].try_into().unwrap());
        if guid != self.pool_guid(&fs).await {
            return Err(status::STALE);
        }
        Ok(FileId::from_raw(u64::from_be_bytes(
            fh[12..20].try_into().unwrap(),
        )))
    }

    // ---- reading ----

    async fn dnode(&self, fs: &FileSystem<D, R>, id: FileId) -> NfsResult<Dnode> {
        let mut found = fs.try_read_dnode(id).await.map_err(err)?;
        if found.is_none() {
            // Made through another node a moment ago? Catch up, look again.
            self.catch_up(fs).await?;
            found = fs.try_read_dnode(id).await.map_err(err)?;
        }
        match found {
            // Only files and directories are NFS objects (not volumes,
            // snapshots or the dataset directory).
            Some(d) if matches!(d.kind(), ObjType::File | ObjType::Directory) => Ok(d),
            _ => Err(status::STALE),
        }
    }

    /// Catch up with the committed state now (not waiting for the next
    /// periodic refresh).
    async fn catch_up(&self, fs: &FileSystem<D, R>) -> NfsResult<()> {
        fs.refresh_root().await.map_err(err)?;
        *self.last_refresh.lock().unwrap() = Some(Instant::now());
        Ok(())
    }

    /// Entry `name` of `dir`, catching up once if it isn't there (it may
    /// have been made through another node a moment ago).
    async fn entry(
        &self,
        fs: &FileSystem<D, R>,
        dir: FileId,
        name: &str,
    ) -> NfsResult<Option<(FileId, Dnode)>> {
        if let Some(e) = fs.lookup_in(dir, name).await.map_err(err)? {
            return Ok(Some(e));
        }
        self.catch_up(fs).await?;
        fs.lookup_in(dir, name).await.map_err(err)
    }

    fn attr_of(&self, fs: &FileSystem<D, R>, guid: u64, id: FileId, d: &Dnode) -> Attr {
        let size = match fs.opened(id) {
            Some(f) => f.size(),
            None if d.is_directory() => d.size,
            None => d.size,
        };
        Attr {
            id,
            kind: d.kind(),
            mode: d.attrs.mode,
            nlink: if d.is_directory() { 2 } else { d.attrs.nlink },
            uid: d.attrs.uid,
            gid: d.attrs.gid,
            size,
            mtime_ns: d.attrs.mtime_ns,
            ctime_ns: d.attrs.ctime_ns,
            fsid: guid,
            change: d.change,
        }
    }

    pub async fn getattr(&self, id: FileId) -> NfsResult<Attr> {
        if let Some(node) = self.remote(id).await {
            return match self.forward(&node, Forward::Getattr { id }).await? {
                Forwarded::Attr(a) => Ok(a),
                _ => Err(status::SERVERFAULT),
            };
        }
        self.getattr_local(id).await
    }

    async fn getattr_local(&self, id: FileId) -> NfsResult<Attr> {
        let fs = self.fs().await?;
        let d = self.dnode(&fs, id).await?;
        Ok(self.attr_of(&fs, self.pool_guid(&fs).await, id, &d))
    }

    /// `name` in directory `dir` (`..` is its parent, `.` itself).
    pub async fn lookup(&self, dir: FileId, name: &str) -> NfsResult<Attr> {
        let fs = self.fs().await?;
        let d = self.dnode(&fs, dir).await?;
        if !d.is_directory() {
            return Err(status::NOTDIR);
        }
        let id = match name {
            "." => dir,
            ".." => d.parent.unwrap_or_else(|| fs.root_dir()),
            _ => match self.entry(&fs, dir, name).await? {
                Some((id, _)) => id,
                None => return Err(status::NOENT),
            },
        };
        self.getattr(id).await
    }

    /// A directory's entries (volumes left out), by name.
    pub async fn readdir(&self, dir: FileId) -> NfsResult<(Attr, Vec<Entry>)> {
        let fs = self.fs().await?;
        let d = self.dnode(&fs, dir).await?;
        if !d.is_directory() {
            return Err(status::NOTDIR);
        }
        let index = fs.read_dir(&d).await.map_err(err)?;
        let entries = index
            .files
            .into_iter()
            .map(|(name, e)| Entry {
                name,
                id: e.id,
                kind: e.kind,
            })
            .collect();
        Ok((
            self.attr_of(&fs, self.pool_guid(&fs).await, dir, &d),
            entries,
        ))
    }

    /// Up to `count` bytes at `offset`; true if that reaches the end.
    pub async fn read(&self, id: FileId, offset: u64, count: u32) -> NfsResult<(Vec<u8>, bool)> {
        if let Some(node) = self.remote(id).await {
            return match self
                .forward(&node, Forward::Read { id, offset, count })
                .await?
            {
                Forwarded::Read { data, eof } => Ok((data, eof)),
                _ => Err(status::SERVERFAULT),
            };
        }
        self.read_local(id, offset, count).await
    }

    async fn read_local(&self, id: FileId, offset: u64, count: u32) -> NfsResult<(Vec<u8>, bool)> {
        let fs = self.fs().await?;
        let (size, data) = match fs.opened(id) {
            Some(f) => {
                let size = f.size();
                let mut buf = vec![
                    0u8;
                    count.min(size.saturating_sub(offset).min(u32::MAX as u64) as u32)
                        as usize
                ];
                let n = fs.file_read(&f, offset, &mut buf).await.map_err(err)?;
                buf.truncate(n);
                (size, buf)
            }
            None => {
                let d = self.dnode(&fs, id).await?;
                if !d.is_file() {
                    return Err(status::ISDIR);
                }
                let obj = ObjTree::new(d.blksz as usize, Sizing::Fixed, d.size, d.tree.clone());
                let want = (count as u64).min(d.size.saturating_sub(offset)) as usize;
                let mut buf = vec![0u8; want];
                let n = fs.object_read(&obj, offset, &mut buf).await.map_err(err)?;
                buf.truncate(n);
                (d.size, buf)
            }
        };
        let eof = offset + data.len() as u64 >= size;
        Ok((data, eof))
    }

    pub async fn fsstat(&self) -> NfsResult<(u64, u64)> {
        let fs = self.fs().await?;
        let total = fs.layout.metaslab_count * fs.layout.metaslab_size_blocks;
        let free = fs.allocatable_blocks().await;
        let b = crate::disk::BLOCK_SIZE as u64;
        Ok((total * b, free.min(total) * b))
    }

    /// The directory an export path names (from the root).
    /// The exported root: the default posix dataset's root directory.
    pub async fn root(&self) -> NfsResult<FileId> {
        Ok(self.fs().await?.root_dir())
    }

    pub async fn export(&self, path: &str) -> NfsResult<FileId> {
        let fs = self.fs().await?;
        let parts = crate::inode::path_components(std::path::Path::new(path));
        let (id, _, _) = fs.resolve_dir(&parts).await.map_err(err)?;
        Ok(id)
    }

    // ---- writing ----

    /// File `id` open for writing here, taking its lease first if need be.
    async fn writable(&self, fs: &FileSystem<D, R>, id: FileId) -> NfsResult<Arc<OpenFile>> {
        let mut held = self.held.lock().await;
        if let Some(h) = held.get_mut(&id) {
            if Self::still_open(fs, id, &h.file) {
                h.last_write = Instant::now();
                return Ok(h.file.clone());
            }
            // The pool dropped it (a conflict): open it afresh.
            if let Some(h) = held.remove(&id) {
                self.lost_writes(id, "the file was dropped");
                if let Some(g) = h.guard {
                    g.release().await;
                }
            }
        }
        let d = self.dnode(fs, id).await?;
        if !d.is_file() {
            return Err(status::ISDIR);
        }
        let guard = match &self.leases {
            Some(l) => Some(
                l.take(
                    object_resource(id),
                    crate::types::Holder::new(crate::types::HolderRole::Nfs, Some(&self.node)),
                )
                .await
                .map_err(|_| status::JUKEBOX)?,
            ),
            None => None,
        };
        let file = match fs.open_file_id(id, guard.as_ref().map(|g| g.lease())).await {
            Ok(f) => f,
            Err(e) => {
                if let Some(g) = guard {
                    g.release().await;
                }
                return Err(err(e));
            }
        };
        held.insert(
            id,
            Held {
                file: file.clone(),
                guard,
                last_write: Instant::now(),
                dirty_since: None,
            },
        );
        Ok(file)
    }

    /// Write `data` at `offset`; with `stable`, committed before returning.
    /// Returns the attributes after, and the write verifier of the node
    /// that has the data.
    pub async fn write(
        &self,
        id: FileId,
        offset: u64,
        data: &[u8],
        stable: bool,
    ) -> NfsResult<(Attr, [u8; 8])> {
        if let Some(node) = self.remote(id).await {
            let req = Forward::Write {
                id,
                offset,
                data: data.to_vec(),
                stable,
            };
            return match self.forward(&node, req).await? {
                Forwarded::Write { attr, verifier } => Ok((attr, verifier)),
                _ => Err(status::SERVERFAULT),
            };
        }
        self.write_local(id, offset, data, stable).await
    }

    async fn write_local(
        &self,
        id: FileId,
        offset: u64,
        data: &[u8],
        stable: bool,
    ) -> NfsResult<(Attr, [u8; 8])> {
        let fs = self.fs().await?;
        offset.checked_add(data.len() as u64).ok_or(status::FBIG)?;
        // Taken before writing: if the write is lost, the verifier has
        // changed by the time a COMMIT could answer.
        let verifier = self.verifier();
        let f = self.writable(&fs, id).await?;
        fs.file_write(&f, offset, data).await.map_err(err)?;
        if stable {
            match fs.file_sync(&f).await {
                Ok(()) => {}
                // The pool dropped the file's unsynced writes, the unstable
                // ones acknowledged before this among them: as in
                // `commit_local`, they're owed a new verifier (I-33).
                Err(FileSystemError::Conflict(m)) => {
                    self.lost_writes(id, &format!("unsynced writes dropped: {}", m));
                    return Err(status::JUKEBOX);
                }
                Err(e) => return Err(err(e)),
            }
        } else if let Some(h) = self.held.lock().await.get_mut(&id) {
            h.dirty_since.get_or_insert_with(Instant::now);
        }
        if !Self::still_open(&fs, id, &f) {
            // Closed (or dropped) while this wrote to it.
            self.lost_writes(id, "a write raced with closing the file");
            return Err(status::JUKEBOX);
        }
        Ok((self.getattr_local(id).await?, verifier))
    }

    /// Make every write to `id` so far durable. Returns the write verifier
    /// of the node that had the data.
    pub async fn commit(&self, id: FileId) -> NfsResult<[u8; 8]> {
        if let Some(node) = self.remote(id).await {
            return match self.forward(&node, Forward::Commit { id }).await? {
                Forwarded::Commit { verifier } => Ok(verifier),
                _ => Err(status::SERVERFAULT),
            };
        }
        self.commit_local(id).await
    }

    async fn commit_local(&self, id: FileId) -> NfsResult<[u8; 8]> {
        let fs = self.fs().await?;
        let _closing = self.closing.read().await;
        // The file this node writes through, even if the pool already
        // dropped it (a conflict): its sync then reports what was lost.
        let held = self.held.lock().await.get(&id).map(|h| h.file.clone());
        if let Some(f) = held.or_else(|| fs.opened(id)) {
            match fs.file_sync(&f).await {
                Ok(()) => {}
                Err(FileSystemError::Conflict(m)) => {
                    self.lost_writes(id, &format!("unsynced writes dropped: {}", m));
                    return Ok(self.verifier());
                }
                Err(e) => return Err(err(e)),
            }
            if let Some(h) = self.held.lock().await.get_mut(&id) {
                h.dirty_since = None;
            }
        }
        // As of now, after the sync: a loss noted meanwhile has changed it,
        // so writes acknowledged with the old one are resent.
        Ok(self.verifier())
    }

    /// The first byte at `offset` or after that is data (`data`) or in a
    /// hole: (where, whether that's the end). `NXIO` past the end.
    pub async fn seek(&self, id: FileId, offset: u64, data: bool) -> NfsResult<(u64, bool)> {
        if let Some(node) = self.remote(id).await {
            return match self
                .forward(&node, Forward::Seek { id, offset, data })
                .await?
            {
                Forwarded::Seek { offset, eof } => Ok((offset, eof)),
                _ => Err(status::SERVERFAULT),
            };
        }
        self.seek_local(id, offset, data).await
    }

    async fn seek_local(&self, id: FileId, offset: u64, data: bool) -> NfsResult<(u64, bool)> {
        let fs = self.fs().await?;
        let found = match fs.opened(id) {
            Some(f) => {
                if offset >= f.size() {
                    return Err(status::NXIO);
                }
                let size = f.size();
                (fs.file_seek(&f, offset, data).await.map_err(err)?, size)
            }
            None => {
                let d = self.dnode(&fs, id).await?;
                if !d.is_file() {
                    return Err(status::ISDIR);
                }
                if offset >= d.size {
                    return Err(status::NXIO);
                }
                let obj = ObjTree::new(d.blksz as usize, Sizing::Fixed, d.size, d.tree.clone());
                (
                    fs.object_seek(&obj, offset, data).await.map_err(err)?,
                    d.size,
                )
            }
        };
        // Nothing before the end: the end (a hole there is implied).
        Ok(match found {
            (Some(at), _) => (at, false),
            (None, size) => (size, true),
        })
    }

    /// Punch a hole: zero `len` bytes at `offset` (the size stays), freeing
    /// whole records, committed before returning.
    pub async fn deallocate(&self, id: FileId, offset: u64, len: u64) -> NfsResult<()> {
        if let Some(node) = self.remote(id).await {
            self.forward(&node, Forward::Deallocate { id, offset, len })
                .await?;
            return Ok(());
        }
        self.deallocate_local(id, offset, len).await
    }

    async fn deallocate_local(&self, id: FileId, offset: u64, len: u64) -> NfsResult<()> {
        let fs = self.fs().await?;
        let f = self.writable(&fs, id).await?;
        fs.file_zero(&f, offset, len).await.map_err(err)?;
        fs.file_sync(&f).await.map_err(err)
    }

    /// Set file `id`'s size here, committed before returning.
    async fn truncate_local(&self, id: FileId, size: u64) -> NfsResult<()> {
        let fs = self.fs().await?;
        let f = self.writable(&fs, id).await?;
        fs.file_truncate(&f, size).await.map_err(err)?;
        fs.file_sync(&f).await.map_err(err)
    }

    /// Change attributes; a size goes through the open file (here, or on
    /// the node writing it) and is committed before returning.
    pub async fn setattr(&self, id: FileId, set: &SetAttrs, size: Option<u64>) -> NfsResult<Attr> {
        let fs = self.fs().await?;
        if let Some(size) = size {
            match self.remote(id).await {
                Some(node) => {
                    self.forward(&node, Forward::Truncate { id, size }).await?;
                }
                None => self.truncate_local(id, size).await?,
            }
        }
        if *set != SetAttrs::default() {
            self.dnode(&fs, id).await?;
            fs.set_attrs_id(id, set).await.map_err(err)?;
        }
        self.getattr(id).await
    }

    /// Create a file or directory (see `FileSystem::create_at`).
    pub async fn create(
        &self,
        dir: FileId,
        name: &str,
        kind: ObjType,
        attrs: &SetAttrs,
        exclusive: Option<u64>,
        guarded: bool,
    ) -> NfsResult<Attr> {
        let fs = self.fs().await?;
        if name.len() > crate::inode::MAX_NAME_BYTES {
            return Err(status::NAMETOOLONG);
        }
        if !self.dnode(&fs, dir).await?.is_directory() {
            return Err(status::NOTDIR);
        }
        if let Some((_, existing)) = fs.lookup_in(dir, name).await.map_err(err)? {
            let retransmit = exclusive.is_some_and(|v| existing.attrs.mtime_ns == v);
            if guarded || (exclusive.is_some() && !retransmit) || existing.kind() != kind {
                return Err(status::EXIST);
            }
        }
        let id = fs
            .create_at(dir, name, kind, attrs, exclusive)
            .await
            .map_err(err)?;
        self.getattr(id).await
    }

    /// Remove `name` from `dir`: a file, or (with `dir_only`) an empty
    /// directory.
    pub async fn remove(&self, dir: FileId, name: &str, dir_only: bool) -> NfsResult<()> {
        let fs = self.fs().await?;
        let (id, d) = match self.entry(&fs, dir, name).await? {
            Some(x) => x,
            None => return Err(status::NOENT),
        };
        match (dir_only, d.is_directory()) {
            (true, false) => return Err(status::NOTDIR),
            (false, true) => return Err(status::ISDIR),
            _ => {}
        }
        if d.is_directory() && !fs.read_dir(&d).await.map_err(err)?.files.is_empty() {
            return Err(status::NOTEMPTY);
        }
        // Under our own lease on it, if we hold one: the committer refuses
        // a remove made under none while anyone holds it, us included. It
        // goes only once the file has (I-33): dropped first, a remove that
        // then failed left the file, and our dirty copy of it, unleased.
        // Holding `closing` keeps `tidy` from letting it go meanwhile.
        let _closing = self.closing.read().await;
        let lease = self.lease_of(id).await;
        fs.remove_at_leased(dir, name, lease).await.map_err(err)?;
        self.forget(id).await;
        Ok(())
    }

    pub async fn rename(
        &self,
        from_dir: FileId,
        from: &str,
        to_dir: FileId,
        to: &str,
    ) -> NfsResult<()> {
        let fs = self.fs().await?;
        if to.len() > crate::inode::MAX_NAME_BYTES {
            return Err(status::NAMETOOLONG);
        }
        let moved = match self.entry(&fs, from_dir, from).await? {
            Some(x) => x,
            None => return Err(status::NOENT),
        };
        let mut replaced = None;
        if let Some((tid, target)) = fs.lookup_in(to_dir, to).await.map_err(err)? {
            match (moved.1.is_directory(), target.is_directory()) {
                (true, false) => return Err(status::NOTDIR),
                (false, true) => return Err(status::ISDIR),
                (true, true) if !fs.read_dir(&target).await.map_err(err)?.files.is_empty() => {
                    return Err(status::NOTEMPTY);
                }
                _ => {}
            }
            if tid != moved.0 {
                replaced = Some(tid);
            }
        }
        // As for `remove`: under our lease on the file it replaces, which
        // goes only once it's replaced. The moved file stays open here if
        // it is: its number doesn't change.
        let _closing = self.closing.read().await;
        let lease = match replaced {
            Some(tid) => self.lease_of(tid).await,
            None => None,
        };
        fs.rename_at_leased(from_dir, from, to_dir, to, lease)
            .await
            .map_err(err)?;
        if let Some(tid) = replaced {
            self.forget(tid).await;
        }
        Ok(())
    }

    /// The `obj/<id>` lease this node holds on `id`, if any.
    async fn lease_of(&self, id: FileId) -> Option<raidish_core::writer::ObjLease> {
        self.held
            .lock()
            .await
            .get(&id)
            .and_then(|h| h.guard.as_ref().map(|g| g.lease()))
    }

    /// Drop file `id` from what this node writes, without committing.
    async fn forget(&self, id: FileId) {
        let h = self.held.lock().await.remove(&id);
        if let Some(h) = h
            && let Some(g) = h.guard
        {
            g.release().await;
        }
    }

    /// Commit files written to a while ago, and close idle ones (letting
    /// their leases go). Run every so often.
    pub async fn tidy(&self) {
        let Some(fs) = (self.source)() else { return };
        let now = Instant::now();
        let mut to_close = Vec::new();
        let mut to_sync = Vec::new();
        {
            let mut held = self.held.lock().await;
            for (id, h) in held.iter_mut() {
                if now.duration_since(h.last_write) >= self.close_after {
                    to_close.push(*id);
                } else if h
                    .dirty_since
                    .is_some_and(|t| now.duration_since(t) >= SYNC_AFTER)
                {
                    h.dirty_since = None;
                    to_sync.push(h.file.clone());
                }
            }
        }
        for f in to_sync {
            match fs.file_sync(&f).await {
                Ok(()) => {}
                // Dropped: writes acknowledged unstable are gone (I-33).
                Err(FileSystemError::Conflict(m)) => {
                    self.lost_writes(f.id, &format!("unsynced writes dropped: {}", m));
                }
                Err(e) => eprintln!("nfs: committing object {}: {}", f.id, e),
            }
        }
        for id in to_close {
            let _closing = self.closing.write().await;
            let h = {
                let mut held = self.held.lock().await;
                // Written to since we looked: keep it.
                match held.get(&id) {
                    Some(h) if Instant::now().duration_since(h.last_write) >= self.close_after => {
                        held.remove(&id)
                    }
                    _ => None,
                }
            };
            let Some(h) = h else { continue };
            if let Err(e) = fs.close_file(&h.file).await {
                // Closed anyway: whatever it didn't commit is gone.
                eprintln!("nfs: closing object {}: {}", id, e);
                self.lost_writes(id, "closing it failed");
            }
            if let Some(g) = h.guard {
                g.release().await;
            }
        }
    }

    /// Commit and close every file this node writes, letting their leases
    /// go (on shutdown, or to hand them over now).
    #[cfg(test)]
    pub async fn close_all(&self) {
        let Some(fs) = (self.source)() else { return };
        let all: Vec<Held> = self.held.lock().await.drain().map(|(_, h)| h).collect();
        for h in all {
            if let Err(e) = fs.close_file(&h.file).await {
                eprintln!("nfs: closing object {}: {}", h.file.id, e);
            }
            if let Some(g) = h.guard {
                g.release().await;
            }
        }
    }

    /// Files this node is writing now.
    #[cfg(test)]
    pub async fn writing(&self) -> Vec<FileId> {
        self.held.lock().await.keys().copied().collect()
    }
}
