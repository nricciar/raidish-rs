//! The NFS server over a real socket, with a small NFSv3 client.

use super::backend::{Backend, Forward, Forwarded, NfsStat, Peers, status};
use super::rpc::Client;
use super::v3::{MOUNT_PROG, NFS_PROG, PMAP_PROG};
use super::xdr::{Reader, Writer};
use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority};
use crate::fs::FileSystem;
use crate::object_lease::{ObjectGuard, ObjectLeases};
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::types::{Epoch, Holding, LeaseRef};
use crate::vdev::Pool;
use crate::writer::ObjLease;
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 3) as u8).wrapping_mul(41).wrapping_add(seed))
        .collect()
}

/// An NFS server on 127.0.0.1 over `fs`, and a client connected to it.
async fn serve(
    fs: Arc<FileSystem<Pool>>,
    leases: Option<Arc<dyn ObjectLeases>>,
    node: &str,
) -> (Nfs, Arc<Backend<Pool>>) {
    serve_with(fs, leases, node, std::time::Duration::from_secs(3)).await
}

async fn serve_with(
    fs: Arc<FileSystem<Pool>>,
    leases: Option<Arc<dyn ObjectLeases>>,
    node: &str,
    close_after: std::time::Duration,
) -> (Nfs, Arc<Backend<Pool>>) {
    serve_full(fs, leases, node, close_after, None).await
}

async fn serve_full(
    fs: Arc<FileSystem<Pool>>,
    leases: Option<Arc<dyn ObjectLeases>>,
    node: &str,
    close_after: std::time::Duration,
    peers: Option<Arc<dyn Peers>>,
) -> (Nfs, Arc<Backend<Pool>>) {
    let mut backend = Backend::new(Arc::new(move || Some(fs.clone())), leases, node.into())
        .with_close_after(close_after);
    if let Some(p) = peers {
        backend = backend.with_peers(p);
    }
    let backend = Arc::new(backend);
    let (addr, _) = super::start("127.0.0.1:0", backend.clone(), None)
        .await
        .unwrap();
    (
        Nfs {
            c: Client::connect(addr).await,
            port: addr.port(),
        },
        backend,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fattr {
    ftype: u32,
    mode: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    size: u64,
    fileid: u64,
    mtime: (u32, u32),
}

fn fattr(r: &mut Reader<'_>) -> Fattr {
    let ftype = r.u32().unwrap();
    let mode = r.u32().unwrap();
    let nlink = r.u32().unwrap();
    let uid = r.u32().unwrap();
    let gid = r.u32().unwrap();
    let size = r.u64().unwrap();
    let _used = r.u64().unwrap();
    r.u64().unwrap(); // rdev
    let _fsid = r.u64().unwrap();
    let fileid = r.u64().unwrap();
    r.u64().unwrap(); // atime
    let mtime = (r.u32().unwrap(), r.u32().unwrap());
    r.u64().unwrap(); // ctime
    Fattr {
        ftype,
        mode,
        nlink,
        uid,
        gid,
        size,
        fileid,
        mtime,
    }
}

fn post_op(r: &mut Reader<'_>) -> Option<Fattr> {
    r.bool().unwrap().then(|| fattr(r))
}

fn wcc(r: &mut Reader<'_>) -> Option<Fattr> {
    if r.bool().unwrap() {
        r.fixed(24).unwrap();
    }
    post_op(r)
}

/// `sattr3` setting only what's given.
fn sattr(w: &mut Writer, mode: Option<u32>, size: Option<u64>) {
    match mode {
        Some(m) => w.bool(true).u32(m),
        None => w.bool(false),
    };
    w.bool(false).bool(false); // uid, gid
    match size {
        Some(s) => w.bool(true).u64(s),
        None => w.bool(false),
    };
    w.u32(0).u32(0); // atime, mtime: don't change
}

struct Nfs {
    c: Client,
    port: u16,
}

impl Nfs {
    async fn nfs(&mut self, proc_: u32, args: Writer) -> Vec<u8> {
        let (st, body) = self.c.call(NFS_PROG, 3, proc_, &args.buf).await;
        assert_eq!(st, 0, "accepted (proc {})", proc_);
        body
    }

    async fn mnt(&mut self, path: &str) -> (u32, Vec<u8>) {
        let mut w = Writer::new();
        w.string(path);
        let (st, body) = self.c.call(MOUNT_PROG, 3, 1, &w.buf).await;
        assert_eq!(st, 0);
        let mut r = Reader::new(&body);
        let s = r.u32().unwrap();
        if s != 0 {
            return (s, Vec::new());
        }
        (0, r.opaque(64).unwrap().to_vec())
    }

    async fn getattr(&mut self, fh: &[u8]) -> Result<Fattr, NfsStat> {
        let mut w = Writer::new();
        w.opaque(fh);
        let b = self.nfs(1, w).await;
        let mut r = Reader::new(&b);
        match r.u32().unwrap() {
            0 => Ok(fattr(&mut r)),
            s => Err(NfsStat::from_code(s)),
        }
    }

    async fn lookup(&mut self, dir: &[u8], name: &str) -> Result<Vec<u8>, NfsStat> {
        let mut w = Writer::new();
        w.opaque(dir).string(name);
        let b = self.nfs(3, w).await;
        let mut r = Reader::new(&b);
        match r.u32().unwrap() {
            0 => Ok(r.opaque(64).unwrap().to_vec()),
            s => Err(NfsStat::from_code(s)),
        }
    }

    /// CREATE (0 UNCHECKED, 1 GUARDED) or MKDIR.
    async fn make(
        &mut self,
        dir: &[u8],
        name: &str,
        mkdir: bool,
        how: u32,
    ) -> Result<Vec<u8>, NfsStat> {
        let mut w = Writer::new();
        w.opaque(dir).string(name);
        if !mkdir {
            w.u32(how);
        }
        sattr(&mut w, Some(if mkdir { 0o750 } else { 0o640 }), None);
        let b = self.nfs(if mkdir { 9 } else { 8 }, w).await;
        let mut r = Reader::new(&b);
        match r.u32().unwrap() {
            0 => {
                assert!(r.bool().unwrap(), "a handle");
                Ok(r.opaque(64).unwrap().to_vec())
            }
            s => Err(NfsStat::from_code(s)),
        }
    }

    async fn create_exclusive(
        &mut self,
        dir: &[u8],
        name: &str,
        verf: u64,
    ) -> Result<Vec<u8>, NfsStat> {
        let mut w = Writer::new();
        w.opaque(dir).string(name).u32(2).u64(verf);
        let b = self.nfs(8, w).await;
        let mut r = Reader::new(&b);
        match r.u32().unwrap() {
            0 => {
                assert!(r.bool().unwrap());
                Ok(r.opaque(64).unwrap().to_vec())
            }
            s => Err(NfsStat::from_code(s)),
        }
    }

    /// WRITE: (status, count, committed, verifier).
    async fn write(
        &mut self,
        fh: &[u8],
        offset: u64,
        data: &[u8],
        stable: u32,
    ) -> (u32, u32, u32, [u8; 8]) {
        let mut w = Writer::new();
        w.opaque(fh)
            .u64(offset)
            .u32(data.len() as u32)
            .u32(stable)
            .opaque(data);
        let b = self.nfs(7, w).await;
        let mut r = Reader::new(&b);
        let st = r.u32().unwrap();
        wcc(&mut r);
        if st != 0 {
            return (st, 0, 0, [0; 8]);
        }
        let count = r.u32().unwrap();
        let committed = r.u32().unwrap();
        let verf = r.fixed(8).unwrap().try_into().unwrap();
        (st, count, committed, verf)
    }

    async fn read(
        &mut self,
        fh: &[u8],
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), NfsStat> {
        let mut w = Writer::new();
        w.opaque(fh).u64(offset).u32(count);
        let b = self.nfs(6, w).await;
        let mut r = Reader::new(&b);
        let st = r.u32().unwrap();
        post_op(&mut r);
        if st != 0 {
            return Err(NfsStat::from_code(st));
        }
        let n = r.u32().unwrap();
        let eof = r.bool().unwrap();
        let data = r.opaque(1 << 21).unwrap().to_vec();
        assert_eq!(n as usize, data.len());
        Ok((data, eof))
    }

    async fn commit(&mut self, fh: &[u8]) -> Result<[u8; 8], NfsStat> {
        let mut w = Writer::new();
        w.opaque(fh).u64(0).u32(0);
        let b = self.nfs(21, w).await;
        let mut r = Reader::new(&b);
        let st = r.u32().unwrap();
        wcc(&mut r);
        if st != 0 {
            return Err(NfsStat::from_code(st));
        }
        Ok(r.fixed(8).unwrap().try_into().unwrap())
    }

    async fn setattr(&mut self, fh: &[u8], mode: Option<u32>, size: Option<u64>) -> NfsStat {
        let mut w = Writer::new();
        w.opaque(fh);
        sattr(&mut w, mode, size);
        w.bool(false); // no guard
        let b = self.nfs(2, w).await;
        NfsStat::from_code(Reader::new(&b).u32().unwrap())
    }

    /// REMOVE or RMDIR.
    async fn remove(&mut self, dir: &[u8], name: &str, rmdir: bool) -> NfsStat {
        let mut w = Writer::new();
        w.opaque(dir).string(name);
        let b = self.nfs(if rmdir { 13 } else { 12 }, w).await;
        NfsStat::from_code(Reader::new(&b).u32().unwrap())
    }

    async fn rename(&mut self, fd: &[u8], from: &str, td: &[u8], to: &str) -> NfsStat {
        let mut w = Writer::new();
        w.opaque(fd).string(from).opaque(td).string(to);
        let b = self.nfs(14, w).await;
        NfsStat::from_code(Reader::new(&b).u32().unwrap())
    }

    async fn access(&mut self, fh: &[u8], want: u32) -> u32 {
        let mut w = Writer::new();
        w.opaque(fh).u32(want);
        let b = self.nfs(4, w).await;
        let mut r = Reader::new(&b);
        assert_eq!(r.u32().unwrap(), 0);
        post_op(&mut r);
        r.u32().unwrap()
    }

    /// READDIR(PLUS) once from `cookie`: (names with their cookies, eof, verifier).
    async fn readdir_page(
        &mut self,
        dir: &[u8],
        cookie: u64,
        verf: u64,
        count: u32,
        plus: bool,
    ) -> Result<(Vec<(String, u64)>, bool, u64), NfsStat> {
        let mut w = Writer::new();
        w.opaque(dir).u64(cookie).u64(verf);
        if plus {
            w.u32(count).u32(count);
        } else {
            w.u32(count);
        }
        let b = self.nfs(if plus { 17 } else { 16 }, w).await;
        let mut r = Reader::new(&b);
        let st = r.u32().unwrap();
        post_op(&mut r);
        if st != 0 {
            return Err(NfsStat::from_code(st));
        }
        let verf = r.u64().unwrap();
        let mut out = Vec::new();
        while r.bool().unwrap() {
            let _id = r.u64().unwrap();
            let name = r.string(255).unwrap();
            let cookie = r.u64().unwrap();
            if plus {
                post_op(&mut r);
                if r.bool().unwrap() {
                    r.opaque(64).unwrap();
                }
            }
            out.push((name, cookie));
        }
        let eof = r.bool().unwrap();
        Ok((out, eof, verf))
    }

    /// Every name in a directory, page by page.
    async fn ls(&mut self, dir: &[u8], count: u32, plus: bool) -> Vec<String> {
        let (mut cookie, mut verf, mut names) = (0, 0, Vec::new());
        loop {
            let (page, eof, v) = self
                .readdir_page(dir, cookie, verf, count, plus)
                .await
                .unwrap();
            verf = v;
            if let Some((_, c)) = page.last() {
                cookie = *c;
            }
            names.extend(page.into_iter().map(|(n, _)| n));
            if eof {
                return names;
            }
        }
    }
}

#[tokio::test]
async fn mount_create_write_read_commit() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, _) = serve(fs.clone(), None, "n0").await;

    let (st, root) = c.mnt("/").await;
    assert_eq!(st, 0);
    assert_eq!(c.getattr(&root).await.unwrap().ftype, 2, "a directory");
    assert_eq!(c.mnt("/nope").await.0, 2, "MNT3ERR_NOENT");

    let dir = c.make(&root, "vm1", true, 0).await.unwrap();
    let f = c.make(&dir, "disk.raw", false, 0).await.unwrap();
    let a = c.getattr(&f).await.unwrap();
    assert_eq!((a.ftype, a.mode, a.size, a.uid), (1, 0o640, 0, 1000));

    // Unstable writes: readable right away, durable at COMMIT.
    let data = pattern(3 * 1024 * 1024 + 17, 1);
    let mut verf = None;
    for (i, chunk) in data.chunks(1 << 20).enumerate() {
        let (st, n, committed, v) = c.write(&f, (i << 20) as u64, chunk, 0).await;
        assert_eq!((st, n as usize, committed), (0, chunk.len(), 0));
        assert!(verf.is_none_or(|x| x == v), "one verifier");
        verf = Some(v);
    }
    let (got, eof) = c.read(&f, 1 << 20, 1 << 20).await.unwrap();
    assert_eq!(got, data[1 << 20..2 << 20]);
    assert!(!eof);
    let (tail, eof) = c.read(&f, 3 << 20, 1 << 20).await.unwrap();
    assert_eq!(tail, data[3 << 20..]);
    assert!(eof);
    assert_eq!(c.getattr(&f).await.unwrap().size, data.len() as u64);
    assert_eq!(c.commit(&f).await.unwrap(), verf.unwrap());
    assert_eq!(fs.read_file(Path::new("vm1/disk.raw")).await.unwrap(), data);

    // A stable write is committed before the answer.
    let (st, _, committed, _) = c.write(&f, 5, b"STABLE", 2).await;
    assert_eq!((st, committed), (0, 2));
    let mut expect = data.clone();
    expect[5..11].copy_from_slice(b"STABLE");
    assert_eq!(
        fs.read_file(Path::new("vm1/disk.raw")).await.unwrap(),
        expect
    );

    // The same handles after a reload (another process, same pool).
    drop(c);
    drop(fs);
    let fs = Arc::new(pool.load().await);
    let (mut c, _) = serve(fs.clone(), None, "n1").await;
    assert_eq!(c.read(&f, 0, 11).await.unwrap().0, expect[..11]);
    assert_eq!(c.lookup(&root, "vm1").await.unwrap(), dir);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn namespace_operations() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, _) = serve(fs.clone(), None, "n0").await;
    let (_, root) = c.mnt("/").await;
    let d = c.make(&root, "d", true, 0).await.unwrap();
    let f = c.make(&d, "f", false, 0).await.unwrap();
    c.write(&f, 0, b"hello", 2).await;

    // Creates: existing is fine unchecked, refused guarded; exclusive
    // creates are idempotent for the same verifier only.
    assert_eq!(c.make(&d, "f", false, 0).await.unwrap(), f);
    assert_eq!(c.make(&d, "f", false, 1).await, Err(status::EXIST));
    let x = c.create_exclusive(&d, "x", 77).await.unwrap();
    assert_eq!(c.create_exclusive(&d, "x", 77).await.unwrap(), x);
    assert_eq!(c.create_exclusive(&d, "x", 78).await, Err(status::EXIST));
    assert_eq!(c.make(&d, "a/b", false, 0).await, Err(status::INVAL));

    // Lookups, `.` and `..`.
    assert_eq!(c.lookup(&d, "f").await.unwrap(), f);
    assert_eq!(c.lookup(&d, "..").await.unwrap(), root);
    assert_eq!(c.lookup(&d, ".").await.unwrap(), d);
    assert_eq!(c.lookup(&d, "nope").await, Err(status::NOENT));
    assert_eq!(c.lookup(&f, "x").await, Err(status::NOTDIR));

    // Removes check the kind and emptiness.
    assert_eq!(c.remove(&root, "d", false).await, status::ISDIR);
    assert_eq!(c.remove(&root, "d", true).await, status::NOTEMPTY);
    assert_eq!(c.remove(&d, "f", true).await, status::NOTDIR);
    assert_eq!(c.remove(&d, "x", false).await, status::OK);
    assert_eq!(c.getattr(&x).await, Err(status::STALE), "gone");

    // Rename across directories, over a file, and not into itself.
    let e = c.make(&root, "e", true, 0).await.unwrap();
    assert_eq!(c.rename(&d, "f", &e, "g").await, status::OK);
    assert_eq!(c.lookup(&e, "g").await.unwrap(), f, "same handle");
    let other = c.make(&e, "other", false, 0).await.unwrap();
    assert_eq!(c.rename(&e, "g", &e, "other").await, status::OK);
    assert_eq!(c.getattr(&other).await, Err(status::STALE));
    assert_eq!(c.read(&f, 0, 100).await.unwrap().0, b"hello");
    let sub = c.make(&e, "sub", true, 0).await.unwrap();
    assert_eq!(c.rename(&root, "e", &sub, "e2").await, status::INVAL);
    assert_eq!(c.lookup(&sub, "..").await.unwrap(), e);
    assert_eq!(c.rename(&root, "e", &d, "e").await, status::OK);
    assert_eq!(
        c.lookup(&e, "..").await.unwrap(),
        d,
        "parent follows a move"
    );

    // Attributes: size (truncate) and mode; ACCESS follows the mode.
    assert_eq!(c.setattr(&f, Some(0o600), Some(2)).await, status::OK);
    let a = c.getattr(&f).await.unwrap();
    assert_eq!((a.mode, a.size), (0o600, 2));
    assert_eq!(c.read(&f, 0, 100).await.unwrap(), (b"he".to_vec(), true));
    assert_eq!(c.access(&f, 0x3f).await, 0x01 | 0x04 | 0x08, "owner rw-");
    c.c.cred.uid = 4242;
    c.c.cred.gids.clear();
    c.c.cred.gid = 4242;
    assert_eq!(c.access(&f, 0x3f).await, 0, "others ---");

    drop(c);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn readdir_pages_through_a_big_directory() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, _) = serve(fs.clone(), None, "n0").await;
    let (_, root) = c.mnt("/").await;
    let d = c.make(&root, "many", true, 0).await.unwrap();
    let mut want: Vec<String> = (0..150).map(|i| format!("file-{:03}", i)).collect();
    for n in &want {
        c.make(&d, n, false, 0).await.unwrap();
    }
    // Volumes in the root aren't NFS objects.
    fs.create_block(&ds("vol"), 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    want.insert(0, "..".into());
    want.insert(0, ".".into());
    for plus in [false, true] {
        assert_eq!(c.ls(&d, 1024, plus).await, want, "plus: {}", plus);
    }
    assert_eq!(c.ls(&root, 4096, true).await, [".", "..", "many"]);
    assert_eq!(c.lookup(&root, "vol").await, Err(status::NOENT));

    // A directory that changed under a pager: its old cookies are refused.
    let (page, _, verf) = c.readdir_page(&d, 0, 0, 1024, false).await.unwrap();
    c.make(&d, "new", false, 0).await.unwrap();
    let last = page.last().unwrap().1;
    assert_eq!(
        c.readdir_page(&d, last, verf, 1024, false).await.err(),
        Some(status::BAD_COOKIE)
    );
}

#[tokio::test]
async fn bad_and_stale_handles() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, _) = serve(fs.clone(), None, "n0").await;
    let (_, root) = c.mnt("/").await;
    assert_eq!(c.getattr(b"nonsense").await, Err(status::BADHANDLE));
    // Another pool's handle.
    let mut other = root.clone();
    other[4] ^= 0xff;
    assert_eq!(c.getattr(&other).await, Err(status::STALE));
    let f = c.make(&root, "f", false, 0).await.unwrap();
    c.remove(&root, "f", false).await;
    assert_eq!(c.getattr(&f).await, Err(status::STALE));
    let (st, ..) = c.write(&f, 0, b"x", 0).await;
    assert_eq!(st, status::STALE);
}

#[tokio::test]
async fn rpc_and_portmapper() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, _) = serve(fs, None, "n0").await;
    // GETPORT: our programs over TCP, nothing else.
    for (prog, vers, prot, want) in [
        (NFS_PROG, 3, 6, c.port as u32),
        (MOUNT_PROG, 3, 6, c.port as u32),
        (NFS_PROG, 4, 6, c.port as u32),
        (NFS_PROG, 3, 17, 0),
        (NFS_PROG, 2, 6, 0),
    ] {
        let mut w = Writer::new();
        w.u32(prog).u32(vers).u32(prot).u32(0);
        let (st, body) = c.c.call(PMAP_PROG, 2, 3, &w.buf).await;
        assert_eq!(st, 0);
        assert_eq!(
            Reader::new(&body).u32().unwrap(),
            want,
            "{} v{}",
            prog,
            vers
        );
    }
    // NULL, a program we don't have, a version we don't speak, a procedure
    // that doesn't exist, and arguments that don't decode.
    assert_eq!(c.c.call(NFS_PROG, 3, 0, &[]).await.0, 0);
    assert_eq!(c.c.call(100021, 4, 0, &[]).await.0, 1, "PROG_UNAVAIL");
    assert_eq!(c.c.call(NFS_PROG, 4, 0, &[]).await.0, 0, "v4 NULL");
    let (st, body) = c.c.call(NFS_PROG, 2, 0, &[]).await;
    assert_eq!(st, 2, "PROG_MISMATCH");
    let mut r = Reader::new(&body);
    assert_eq!((r.u32().unwrap(), r.u32().unwrap()), (3, 4));
    assert_eq!(c.c.call(NFS_PROG, 3, 99, &[]).await.0, 3, "PROC_UNAVAIL");
    assert_eq!(c.c.call(NFS_PROG, 3, 1, &[0, 0]).await.0, 4, "GARBAGE_ARGS");
}

/// An idle file is committed and closed by the housekeeping.
#[tokio::test]
async fn idle_files_are_committed_and_closed() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (mut c, backend) = serve(fs.clone(), None, "n0").await;
    let (_, root) = c.mnt("/").await;
    let f = c.make(&root, "f", false, 0).await.unwrap();
    c.write(&f, 0, b"unstable", 0).await;
    assert_eq!(backend.writing().await.len(), 1);
    assert!(fs.read_file(Path::new("f")).await.unwrap().is_empty());
    tokio::time::sleep(std::time::Duration::from_millis(3600)).await;
    backend.tidy().await;
    assert!(backend.writing().await.is_empty());
    assert_eq!(fs.read_file(Path::new("f")).await.unwrap(), b"unstable");
}

// ---- two nodes ----

/// `ObjectLeases` over the in-memory cluster state.
struct TestLeases(Arc<LocalAuthority>);

struct TestGuard {
    auth: Arc<LocalAuthority>,
    resource: crate::types::LeaseResource,
    lease: ObjLease,
}

#[async_trait]
impl ObjectGuard for TestGuard {
    fn lease(&self) -> ObjLease {
        self.lease.clone()
    }
    async fn release(&self) {
        let op = LeaseOp::Release {
            resource: self.resource.clone(),
            holder: Holding::new(self.lease.owner.clone(), self.lease.epoch),
        };
        self.auth.propose(op).await.unwrap();
    }
}

#[async_trait]
impl ObjectLeases for TestLeases {
    async fn take(
        &self,
        resource: crate::types::LeaseResource,
        desc: crate::types::Holder,
    ) -> Result<Box<dyn ObjectGuard>, String> {
        let owner = format!("nfs-{}", desc);
        let op = LeaseOp::Acquire {
            resource: resource.clone(),
            owner: owner.clone(),
            holder: desc,
            ttl_ms: 600_000,
        };
        match self.0.propose(op).await? {
            Response::Granted { epoch, .. } => Ok(Box::new(TestGuard {
                auth: self.0.clone(),
                resource: resource.clone(),
                lease: ObjLease { owner, epoch },
            })),
            other => Err(format!("{} is held: {:?}", resource, other)),
        }
    }
}

async fn grant(auth: &LocalAuthority, resource: impl std::fmt::Display, owner: &str) -> Epoch {
    let resource = crate::types::res(resource);
    let op = LeaseOp::Acquire {
        resource: resource.clone(),
        owner: owner.into(),
        holder: crate::types::Holder::default().with_detail(owner),
        ttl_ms: 600_000,
    };
    match auth.propose(op).await.unwrap() {
        Response::Granted { epoch, .. } => epoch,
        r => panic!("{} not granted: {:?}", resource, r),
    }
}

/// Two writer nodes, each with its NFS server, sharing a committer: the
/// same handles work on both, one node writes a file at a time (the other
/// is told to retry), and it hands over once the first goes idle.
#[tokio::test]
async fn two_nodes_share_handles_and_hand_files_over() {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = grant(&auth, &POOL, "c").await;
    let committer = Arc::new(
        Committer::new(
            Arc::new(pool.load().await),
            auth.clone(),
            "c".into(),
            epoch,
            "n0".into(),
        )
        .await,
    );
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        let resource = format!("writer/{}", id);
        let epoch = grant(&auth, &resource, id).await;
        let sink = Arc::new(DirectSink {
            committer: committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        });
        let fs = Arc::new(
            FileSystem::load_writer(pool.dev().await, sink)
                .await
                .unwrap(),
        );
        let leases: Arc<dyn ObjectLeases> = Arc::new(TestLeases(auth.clone()));
        // Long enough that a still holds the file when b writes, however
        // slow this machine is.
        nodes.push(serve_with(fs, Some(leases), id, std::time::Duration::from_secs(600)).await);
    }
    let [(mut a, ba), (mut b, _)] = <[_; 2]>::try_from(nodes).ok().unwrap();

    let (_, root) = a.mnt("/").await;
    assert_eq!(b.mnt("/").await.1, root, "same root handle on every node");
    let f = a.make(&root, "shared", false, 0).await.unwrap();
    let data = pattern(500_000, 3);
    let (st, ..) = a.write(&f, 0, &data, 0).await;
    assert_eq!(st, 0);
    a.commit(&f).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        b.read(&f, 0, 1 << 20).await.unwrap().0,
        data,
        "visible on b"
    );

    // a holds the file: b is told to try again later.
    let (st, ..) = b.write(&f, 10, b"from b", 0).await;
    assert_eq!(st, status::JUKEBOX);

    // Once a lets go (idle, or here: right away), b takes over.
    ba.close_all().await;
    let (st, ..) = b.write(&f, 10, b"from b", 2).await;
    assert_eq!(st, 0);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut expect = data.clone();
    expect[10..16].copy_from_slice(b"from b");
    assert_eq!(
        a.read(&f, 0, 1 << 20).await.unwrap().0,
        expect,
        "visible on a"
    );
    // A rename through the other node keeps the handle.
    assert_eq!(
        a.rename(&root, "shared", &root, "renamed").await,
        status::OK
    );
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(b.lookup(&root, "renamed").await.unwrap(), f);
}

/// `Peers` between backends in this process, over the in-memory leases.
struct TestPeers {
    auth: Arc<LocalAuthority>,
    nodes: std::sync::Mutex<std::collections::HashMap<String, Arc<Backend<Pool>>>>,
    down: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[async_trait]
impl Peers for TestPeers {
    fn holder(&self, id: crate::disk::FileId) -> Option<String> {
        let state = self.auth.state();
        let l = state.leases.get(&crate::committer::object_resource(id))?;
        if l.released || l.expires_ms <= crate::cluster::now_ms() {
            return None;
        }
        l.holder
            .node_as(crate::types::HolderRole::Nfs)
            .map(String::from)
    }

    async fn forward(&self, node: &str, req: &Forward) -> Result<Forwarded, String> {
        if self.down.lock().unwrap().contains(node) {
            return Err(format!("{} is down", node));
        }
        let b = self.nodes.lock().unwrap().get(node).cloned();
        match b {
            Some(b) => Ok(b.serve_forward(req.clone()).await),
            None => Err(format!("no node {}", node)),
        }
    }
}

/// With peers, a write through a node that doesn't hold the file goes to
/// the one that does (no "try later"), and so does whatever depends on its
/// unsynced data: reads, sizes, commits (with that node's verifier).
#[tokio::test]
async fn requests_for_a_file_another_node_writes_are_forwarded() {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = grant(&auth, &POOL, "c").await;
    let committer = Arc::new(
        Committer::new(
            Arc::new(pool.load().await),
            auth.clone(),
            "c".into(),
            epoch,
            "n0".into(),
        )
        .await,
    );
    let peers = Arc::new(TestPeers {
        auth: auth.clone(),
        nodes: Default::default(),
        down: Default::default(),
    });
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        let resource = format!("writer/{}", id);
        let epoch = grant(&auth, &resource, id).await;
        let sink = Arc::new(DirectSink {
            committer: committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        });
        let fs = Arc::new(
            FileSystem::load_writer(pool.dev().await, sink)
                .await
                .unwrap(),
        );
        let leases: Arc<dyn ObjectLeases> = Arc::new(TestLeases(auth.clone()));
        let (c, backend) = serve_full(
            fs,
            Some(leases),
            id,
            std::time::Duration::from_secs(600),
            Some(peers.clone() as Arc<dyn Peers>),
        )
        .await;
        peers
            .nodes
            .lock()
            .unwrap()
            .insert(id.into(), backend.clone());
        nodes.push((c, backend));
    }
    let [(mut a, ba), (mut b, bb)] = <[_; 2]>::try_from(nodes).ok().unwrap();

    let (_, root) = a.mnt("/").await;
    let f = a.make(&root, "disk.raw", false, 0).await.unwrap();
    let (st, _, _, va) = a.write(&f, 0, &pattern(300_000, 1), 0).await;
    assert_eq!(st, 0);
    assert_eq!(ba.verifier(), va);

    // Through b, unstable: lands on a (a's verifier), and b sees a's
    // unsynced data and size right away.
    let (st, _, committed, vb) = b.write(&f, 300_000, b"tail from b", 0).await;
    assert_eq!((st, committed), (0, 0));
    assert_eq!(vb, va, "a has the data, so a's verifier");
    assert!(bb.writing().await.is_empty(), "b doesn't write it itself");
    let mut expect = pattern(300_000, 1);
    expect.extend_from_slice(b"tail from b");
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(b.getattr(&f).await.unwrap().size, expect.len() as u64);
    assert_eq!(b.read(&f, 0, 1 << 20).await.unwrap().0, expect);

    // COMMIT through b: a commits, with a's verifier.
    assert_eq!(b.commit(&f).await.unwrap(), va);
    // Truncating through b goes to a too.
    assert_eq!(b.setattr(&f, None, Some(100_000)).await, status::OK);
    expect.truncate(100_000);
    assert_eq!(a.read(&f, 0, 1 << 20).await.unwrap().0, expect);

    // a unreachable while it holds the file: try later.
    peers.down.lock().unwrap().insert("a".into());
    assert_eq!(b.write(&f, 0, b"x", 0).await.0, status::JUKEBOX);
    peers.down.lock().unwrap().clear();

    // Once a lets go, b writes it itself (with its own verifier).
    ba.close_all().await;
    let (st, _, _, v) = b.write(&f, 0, b"B", 2).await;
    assert_eq!((st, v), (0, bb.verifier()));
    assert_eq!(bb.writing().await.len(), 1);
    expect[0] = b'B';
    assert_eq!(
        a.read(&f, 0, 1 << 20).await.unwrap().0,
        expect,
        "a forwards now"
    );
}

/// The node API route other nodes forward to: bincode in, bincode out.
#[tokio::test]
async fn the_forward_route_serves_this_nodes_backend() {
    use axum::http::StatusCode;
    use raidish_auth::testing::Client as Http;
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.write_file(Path::new("f"), b"hello").await.unwrap();
    let id = fs.file_id(Path::new("f")).await.unwrap();
    let backend = Arc::new(Backend::new(
        Arc::new(move || Some(fs.clone())),
        None,
        "n0".into(),
    ));
    let app = |b: Option<Arc<Backend<Pool>>>| {
        let st = super::peers::NfsForwarding(b.map(|b| {
            let v4 = Arc::new(super::v4::Server::new(b.clone(), "n0"));
            (b, v4)
        }));
        Http::new(
            super::peers::router(st),
            raidish_auth::auth::AuthState(None),
        )
    };
    let http = app(Some(backend.clone()));
    let ask = |req: Forward| bincode::serialize(&req).unwrap();
    let r = http
        .post("/api/v1/nfs/forward")
        .body(ask(Forward::Read {
            id,
            offset: 1,
            count: 3,
        }))
        .send()
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let reply: Forwarded = bincode::deserialize(&r.body).unwrap();
    assert_eq!(
        reply,
        Forwarded::Read {
            data: b"ell".to_vec(),
            eof: false
        }
    );
    let r = http
        .post("/api/v1/nfs/forward")
        .body(ask(Forward::Write {
            id,
            offset: 5,
            data: b"!".to_vec(),
            stable: true,
        }))
        .send()
        .await;
    let reply: Forwarded = bincode::deserialize(&r.body).unwrap();
    assert!(
        matches!(reply, Forwarded::Write { ref attr, verifier } if attr.size == 6 && verifier == backend.verifier()),
        "{:?}",
        reply
    );
    // No NFS server on this node: 503, and garbage is a 400.
    let none = app(None);
    let r = none
        .post("/api/v1/nfs/forward")
        .body(ask(Forward::Getattr { id }))
        .send()
        .await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    let r = http.post("/api/v1/nfs/forward").body("junk").send().await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

/// A `DirectSink` whose namespace changes can be made to fail.
struct FlakySink {
    inner: DirectSink<Pool>,
    fail_fs: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl crate::writer::CommitSink for FlakySink {
    fn writer(&self) -> LeaseRef {
        self.inner.writer()
    }
    async fn lease_metaslab(
        &self,
        want: u64,
    ) -> Result<crate::writer::LentMetaslab, crate::writer::CommitError> {
        self.inner.lease_metaslab(want).await
    }
    async fn return_metaslab(
        &self,
        idx: crate::types::MetaslabIdx,
    ) -> Result<(), crate::writer::CommitError> {
        self.inner.return_metaslab(idx).await
    }
    async fn commit(
        &self,
        c: crate::writer::DatasetCommit,
    ) -> Result<crate::writer::Committed, crate::writer::CommitError> {
        self.inner.commit(c).await
    }
    async fn commit_fs(
        &self,
        c: crate::writer::FsCommit,
    ) -> Result<crate::writer::FsCommitted, crate::writer::CommitError> {
        if self.fail_fs.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(crate::writer::CommitError::refused(
                crate::types::ErrorKind::Unavailable,
                "test: no namespace changes now",
            ));
        }
        self.inner.commit_fs(c).await
    }
    async fn claim_dataset(
        &self,
        volume: &crate::types::DatasetName,
        ds: &Holding,
    ) -> Result<(), crate::writer::CommitError> {
        self.inner.claim_dataset(volume, ds).await
    }
}

/// I-33: a REMOVE (or a RENAME over a file) that fails leaves the file
/// this node writes as it was: still held, under its `obj/` lease. The
/// lease used to go first, so the file and its unsynced writes stayed with
/// no lease. One that succeeds is made under that lease (the committer
/// refuses it otherwise), and lets it go.
#[tokio::test]
async fn a_failed_remove_keeps_the_files_lease() {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = grant(&auth, &POOL, "c").await;
    let committer = Arc::new(
        Committer::new(
            Arc::new(pool.load().await),
            auth.clone(),
            "c".into(),
            epoch,
            "n0".into(),
        )
        .await,
    );
    let resource = "writer/a";
    let epoch = grant(&auth, resource, "a").await;
    let sink = Arc::new(FlakySink {
        inner: DirectSink {
            committer: committer.clone(),
            writer: LeaseRef::new(crate::types::res(resource), epoch),
        },
        fail_fs: Default::default(),
    });
    let fs = Arc::new(
        FileSystem::load_writer(pool.dev().await, sink.clone())
            .await
            .unwrap(),
    );
    let leases: Arc<dyn ObjectLeases> = Arc::new(TestLeases(auth.clone()));
    let (mut a, backend) =
        serve_with(fs, Some(leases), "a", std::time::Duration::from_secs(600)).await;
    let (_, root) = a.mnt("/").await;
    let held = |id| {
        auth.state()
            .leases
            .get(&crate::committer::object_resource(id))
            .is_some_and(|l| !l.released)
    };

    for replace in [false, true] {
        let name = if replace { "g" } else { "f" };
        let fh = a.make(&root, name, false, 0).await.unwrap();
        let (st, ..) = a.write(&fh, 0, b"unstable", 0).await;
        assert_eq!(st, 0);
        let id = *backend.writing().await.first().expect("held after a write");
        assert!(held(id));
        if replace {
            a.make(&root, "other", false, 0).await.unwrap();
        }

        sink.fail_fs
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let st = if replace {
            a.rename(&root, "other", &root, name).await
        } else {
            a.remove(&root, name, false).await
        };
        assert_ne!(st, status::OK);
        assert_eq!(backend.writing().await, vec![id], "{}: dropped", name);
        assert!(held(id), "{}: the lease went with a failed op", name);

        sink.fail_fs
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let st = if replace {
            a.rename(&root, "other", &root, name).await
        } else {
            a.remove(&root, name, false).await
        };
        assert_eq!(st, status::OK, "{}: refused under our own lease", name);
        assert!(backend.writing().await.is_empty());
        assert!(!held(id), "{}: the lease outlived the file", name);
    }
}

/// I-33: a stable WRITE whose sync finds the file gone from under this
/// node (another node removed it) drops the unstable writes acknowledged
/// before it: like a COMMIT that finds that, it changes the verifier, so
/// clients resend them. It used to just answer JUKEBOX.
#[tokio::test]
async fn a_stable_write_that_finds_its_writes_dropped_changes_the_verifier() {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = grant(&auth, &POOL, "c").await;
    let committer = Arc::new(
        Committer::new(
            Arc::new(pool.load().await),
            auth.clone(),
            "c".into(),
            epoch,
            "n0".into(),
        )
        .await,
    );
    let mut fss = Vec::new();
    for id in ["a", "b"] {
        let resource = format!("writer/{}", id);
        let epoch = grant(&auth, &resource, id).await;
        let sink = Arc::new(DirectSink {
            committer: committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        });
        fss.push(Arc::new(
            FileSystem::load_writer(pool.dev().await, sink)
                .await
                .unwrap(),
        ));
    }
    // No object leases: nothing keeps b from removing what a writes.
    let (mut a, backend) = serve_with(
        fss[0].clone(),
        None,
        "a",
        std::time::Duration::from_secs(600),
    )
    .await;
    let (_, root) = a.mnt("/").await;
    let fh = a.make(&root, "f", false, 0).await.unwrap();
    let (st, _, _, before) = a.write(&fh, 0, b"unstable", 0).await;
    assert_eq!(st, 0);

    fss[1].refresh_root().await.unwrap();
    fss[1].delete(Path::new("f")).await.unwrap();

    let (st, ..) = a.write(&fh, 8, b"stable", 2).await;
    assert_ne!(st, 0);
    assert_ne!(
        backend.verifier(),
        before,
        "unstable writes dropped under the same verifier"
    );
}
