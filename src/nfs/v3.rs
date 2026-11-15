//! NFS version 3 (RFC 1813), its MOUNT protocol, and enough of the
//! portmapper (RFC 1833) for `mount.nfs` to find them, all on one port.
//!
//! Not supported (`NOTSUPP`): symlinks, hard links, device nodes.
//! Permissions are reported (`ACCESS`, from the mode bits and the caller's
//! `AUTH_SYS` identity) but not enforced on the server.

use super::backend::{Attr, Backend, NfsResult, status};
use super::rpc::{Call, Cred, Reply};
use super::xdr::{Reader, Writer, XdrResult};
use crate::disk::{BlockDevice, FileId};
use crate::inode::{ObjType, SetAttrs};
use std::sync::Arc;

pub const PMAP_PROG: u32 = 100000;
pub const NFS_PROG: u32 = 100003;
pub const MOUNT_PROG: u32 = 100005;

/// Largest READ / WRITE.
pub const MAX_IO: u32 = 1 << 20;
const FH_MAX: usize = 64;
const NAME_MAX: usize = 255;
const PATH_MAX: usize = 1024;

const NF3REG: u32 = 1;
const NF3DIR: u32 = 2;

const ACCESS_READ: u32 = 0x01;
const ACCESS_LOOKUP: u32 = 0x02;
const ACCESS_MODIFY: u32 = 0x04;
const ACCESS_EXTEND: u32 = 0x08;
const ACCESS_DELETE: u32 = 0x10;
const ACCESS_EXECUTE: u32 = 0x20;

const UNSTABLE: u32 = 0;
const FILE_SYNC: u32 = 2;

pub struct Server<D: BlockDevice> {
    pub backend: Arc<Backend<D>>,
    /// The port this serves on (what the portmapper answers).
    pub port: u16,
}

fn time(w: &mut Writer, ns: u64) {
    w.u32((ns / 1_000_000_000) as u32)
        .u32((ns % 1_000_000_000) as u32);
}

fn fattr(w: &mut Writer, a: &Attr) {
    let dir = a.kind == ObjType::Directory;
    w.u32(if dir { NF3DIR } else { NF3REG })
        .u32(a.mode as u32)
        .u32(a.nlink)
        .u32(a.uid)
        .u32(a.gid)
        .u64(a.size)
        .u64(a.size.div_ceil(4096) * 4096)
        .u32(0)
        .u32(0)
        .u64(a.fsid)
        .u64(a.id);
    time(w, a.mtime_ns); // atime (not kept)
    time(w, a.mtime_ns);
    time(w, a.ctime_ns);
}

fn post_op(w: &mut Writer, a: Option<&Attr>) {
    match a {
        Some(a) => {
            w.bool(true);
            fattr(w, a);
        }
        None => {
            w.bool(false);
        }
    }
}

/// `wcc_data` without the pre-operation part.
fn wcc(w: &mut Writer, after: Option<&Attr>) {
    w.bool(false);
    post_op(w, after);
}

/// A reply starting with `status`.
fn reply(st: u32) -> Writer {
    let mut w = Writer::new();
    w.u32(st);
    w
}

/// What the caller may do to an object, by its mode bits.
pub(crate) fn access_bits(a: &Attr, cred: Option<&Cred>, want: u32) -> u32 {
    let (uid, gid, gids) = match cred {
        Some(c) => (c.uid, c.gid, c.gids.as_slice()),
        None => (u32::MAX - 1, u32::MAX - 1, &[][..]),
    };
    let m = a.mode as u32;
    let any_x = m & 0o111 != 0;
    let (r, wr, x) = if uid == 0 {
        (true, true, a.kind == ObjType::Directory || any_x)
    } else {
        let bits = if uid == a.uid {
            m >> 6
        } else if gid == a.gid || gids.contains(&a.gid) {
            m >> 3
        } else {
            m
        } & 7;
        (bits & 4 != 0, bits & 2 != 0, bits & 1 != 0)
    };
    let mut ok = 0;
    if r {
        ok |= ACCESS_READ;
    }
    if a.kind == ObjType::Directory {
        if x {
            ok |= ACCESS_LOOKUP;
        }
        if wr && x {
            ok |= ACCESS_MODIFY | ACCESS_EXTEND | ACCESS_DELETE;
        }
    } else {
        if wr {
            ok |= ACCESS_MODIFY | ACCESS_EXTEND;
        }
        if x {
            ok |= ACCESS_EXECUTE;
        }
    }
    ok & want
}

/// `sattr3`: attributes to set, and a size if one is given.
fn sattr(r: &mut Reader<'_>, now: u64) -> XdrResult<(SetAttrs, Option<u64>)> {
    let mode = r.optional(|r| r.u32())?;
    let uid = r.optional(|r| r.u32())?;
    let gid = r.optional(|r| r.u32())?;
    let size = r.optional(|r| r.u64())?;
    let set_time = |r: &mut Reader<'_>| -> XdrResult<Option<u64>> {
        Ok(match r.u32()? {
            1 => Some(now),
            2 => {
                let s = r.u32()? as u64;
                let ns = r.u32()? as u64;
                Some(s * 1_000_000_000 + ns)
            }
            _ => None,
        })
    };
    let _atime = set_time(r)?;
    let mtime = set_time(r)?;
    Ok((
        SetAttrs {
            mode: mode.map(|m| (m & 0o7777) as u16),
            uid,
            gid,
            mtime_ns: mtime,
        },
        size,
    ))
}

impl<D: BlockDevice + 'static> Server<D> {
    pub fn new(backend: Arc<Backend<D>>, port: u16) -> Self {
        Server { backend, port }
    }

    /// A file handle: the object it names, or why not. Arguments that
    /// don't decode are an `Err` of their own (`GARBAGE_ARGS`).
    async fn fh(&self, r: &mut Reader<'_>) -> XdrResult<Result<FileId, u32>> {
        let fh = r.opaque(FH_MAX)?;
        Ok(self.backend.object(fh).await)
    }

    async fn post_op_of(&self, id: FileId) -> Option<Attr> {
        self.backend.getattr(id).await.ok()
    }

    // ---- NFS ----

    pub(crate) async fn nfs(&self, call: &Call, r: &mut Reader<'_>) -> Reply {
        let w = match call.proc_ {
            0 => Ok(Writer::new()),
            1 => self.getattr(r).await,
            2 => self.setattr(r).await,
            3 => self.lookup(r).await,
            4 => self.access(r, call.cred.as_ref()).await,
            5 => self.attr_only(r, status::NOTSUPP).await, // READLINK
            6 => self.read(r).await,
            7 => self.write(r).await,
            8 => self.create(r, call.cred.as_ref()).await,
            9 => self.mkdir(r, call.cred.as_ref()).await,
            10 | 11 => self.dir_only(r, status::NOTSUPP).await, // SYMLINK, MKNOD
            12 => self.remove(r, false).await,
            13 => self.remove(r, true).await,
            14 => self.rename(r).await,
            15 => self.link(r).await,
            16 => self.readdir(r, false).await,
            17 => self.readdir(r, true).await,
            18 => self.fsstat(r).await,
            19 => self.fsinfo(r).await,
            20 => self.pathconf(r).await,
            21 => self.commit(r).await,
            _ => return Reply::ProcUnavail,
        };
        match w {
            Ok(w) => Reply::Success(w),
            Err(e) => e.into(),
        }
    }

    async fn getattr(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let id = match self.fh(r).await? {
            Ok(id) => id,
            Err(st) => return Ok(reply(st)),
        };
        Ok(match self.backend.getattr(id).await {
            Ok(a) => {
                let mut w = reply(status::OK);
                fattr(&mut w, &a);
                w
            }
            Err(st) => reply(st),
        })
    }

    async fn setattr(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let (set, size) = sattr(r, crate::inode::now_ns())?;
        let guard = r.optional(|r| Ok((r.u32()? as u64) * 1_000_000_000 + r.u32()? as u64))?;
        let id = match fh {
            Ok(id) => id,
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, None);
                return Ok(w);
            }
        };
        if let Some(ctime) = guard
            && let Ok(a) = self.backend.getattr(id).await
            && a.ctime_ns != ctime
        {
            let mut w = reply(status::NOT_SYNC);
            wcc(&mut w, Some(&a));
            return Ok(w);
        }
        let result = self.backend.setattr(id, &set, size).await;
        let after = self.post_op_of(id).await;
        let mut w = reply(result.err().unwrap_or(status::OK));
        wcc(&mut w, after.as_ref());
        Ok(w)
    }

    /// `diropargs3`: a directory and a name.
    async fn dirop(&self, r: &mut Reader<'_>) -> XdrResult<(Result<FileId, u32>, String)> {
        let dir = self.fh(r).await?;
        let name = r.string(NAME_MAX + 1)?;
        let dir = match dir {
            Ok(_) if name.len() > NAME_MAX => Err(status::NAMETOOLONG),
            d => d,
        };
        Ok((dir, name))
    }

    async fn lookup(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let (dir, name) = self.dirop(r).await?;
        let dir = match dir {
            Ok(d) => d,
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, None);
                return Ok(w);
            }
        };
        let dir_attr = self.post_op_of(dir).await;
        Ok(match self.backend.lookup(dir, &name).await {
            Ok(a) => match self.backend.handle(a.id).await {
                Ok(h) => {
                    let mut w = reply(status::OK);
                    w.opaque(&h);
                    post_op(&mut w, Some(&a));
                    post_op(&mut w, dir_attr.as_ref());
                    w
                }
                Err(st) => {
                    let mut w = reply(st);
                    post_op(&mut w, dir_attr.as_ref());
                    w
                }
            },
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, dir_attr.as_ref());
                w
            }
        })
    }

    async fn access(&self, r: &mut Reader<'_>, cred: Option<&Cred>) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let want = r.u32()?;
        let a = match fh {
            Ok(id) => self.backend.getattr(id).await,
            Err(st) => Err(st),
        };
        Ok(match a {
            Ok(a) => {
                let mut w = reply(status::OK);
                post_op(&mut w, Some(&a));
                w.u32(access_bits(&a, cred, want));
                w
            }
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, None);
                w
            }
        })
    }

    /// An answer of `st` with the object's attributes (`READLINK`).
    async fn attr_only(&self, r: &mut Reader<'_>, st: u32) -> XdrResult<Writer> {
        let a = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(_) => None,
        };
        let mut w = reply(st);
        post_op(&mut w, a.as_ref());
        Ok(w)
    }

    /// An answer of `st` with the directory's `wcc_data` (`SYMLINK`, `MKNOD`).
    async fn dir_only(&self, r: &mut Reader<'_>, st: u32) -> XdrResult<Writer> {
        let a = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(_) => None,
        };
        let mut w = reply(st);
        wcc(&mut w, a.as_ref());
        Ok(w)
    }

    async fn link(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let file = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(_) => None,
        };
        let (dir, _) = self.dirop(r).await?;
        let dir = match dir {
            Ok(d) => self.post_op_of(d).await,
            Err(_) => None,
        };
        let mut w = reply(status::NOTSUPP);
        post_op(&mut w, file.as_ref());
        wcc(&mut w, dir.as_ref());
        Ok(w)
    }

    async fn read(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let offset = r.u64()?;
        let count = r.u32()?.min(MAX_IO);
        let id = match fh {
            Ok(id) => id,
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, None);
                return Ok(w);
            }
        };
        Ok(match self.backend.read(id, offset, count).await {
            Ok((data, eof)) => {
                let a = self.post_op_of(id).await;
                let mut w = reply(status::OK);
                post_op(&mut w, a.as_ref());
                w.u32(data.len() as u32).bool(eof).opaque(&data);
                w
            }
            Err(st) => {
                let a = self.post_op_of(id).await;
                let mut w = reply(st);
                post_op(&mut w, a.as_ref());
                w
            }
        })
    }

    async fn write(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let offset = r.u64()?;
        let count = r.u32()?;
        let stable = r.u32()?;
        let data = r.opaque(MAX_IO as usize)?;
        let data = &data[..(count as usize).min(data.len())];
        let id = match fh {
            Ok(id) => id,
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, None);
                return Ok(w);
            }
        };
        let synced = stable != UNSTABLE;
        Ok(match self.backend.write(id, offset, data, synced).await {
            Ok((a, verifier)) => {
                let mut w = reply(status::OK);
                wcc(&mut w, Some(&a));
                w.u32(data.len() as u32)
                    .u32(if synced { FILE_SYNC } else { UNSTABLE })
                    .fixed(&verifier);
                w
            }
            Err(st) => {
                let a = self.post_op_of(id).await;
                let mut w = reply(st);
                wcc(&mut w, a.as_ref());
                w
            }
        })
    }

    /// The result of a `CREATE` or `MKDIR`.
    async fn created(&self, dir: FileId, made: NfsResult<Attr>) -> Writer {
        let dir_attr = self.post_op_of(dir).await;
        let made = match made {
            Ok(a) => self.backend.handle(a.id).await.map(|h| (a, h)),
            Err(st) => Err(st),
        };
        match made {
            Ok((a, h)) => {
                let mut w = reply(status::OK);
                w.bool(true).opaque(&h);
                post_op(&mut w, Some(&a));
                wcc(&mut w, dir_attr.as_ref());
                w
            }
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, dir_attr.as_ref());
                w
            }
        }
    }

    /// Owner of new objects: the caller (unless the attributes say otherwise).
    fn owned(mut set: SetAttrs, cred: Option<&Cred>) -> SetAttrs {
        if let Some(c) = cred {
            set.uid.get_or_insert(c.uid);
            set.gid.get_or_insert(c.gid);
        }
        set
    }

    async fn create(&self, r: &mut Reader<'_>, cred: Option<&Cred>) -> XdrResult<Writer> {
        let (dir, name) = self.dirop(r).await?;
        let how = r.u32()?;
        let now = crate::inode::now_ns();
        let (set, size, exclusive) = match how {
            0 | 1 => {
                let (set, size) = sattr(r, now)?;
                (set, size, None)
            }
            _ => {
                let v = r.fixed(8)?;
                (
                    SetAttrs::default(),
                    None,
                    Some(u64::from_be_bytes(v.try_into().unwrap())),
                )
            }
        };
        let dir = match dir {
            Ok(d) => d,
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, None);
                return Ok(w);
            }
        };
        let mut set = Self::owned(set, cred);
        set.mode.get_or_insert(0o644);
        let made = self
            .backend
            .create(dir, &name, ObjType::File, &set, exclusive, how == 1)
            .await;
        // UNCHECKED with a size: an existing file is truncated.
        let made = match (made, size) {
            (Ok(a), Some(s)) if a.size != s => {
                self.backend
                    .setattr(a.id, &SetAttrs::default(), Some(s))
                    .await
            }
            (m, _) => m,
        };
        Ok(self.created(dir, made).await)
    }

    async fn mkdir(&self, r: &mut Reader<'_>, cred: Option<&Cred>) -> XdrResult<Writer> {
        let (dir, name) = self.dirop(r).await?;
        let (set, _) = sattr(r, crate::inode::now_ns())?;
        let dir = match dir {
            Ok(d) => d,
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, None);
                return Ok(w);
            }
        };
        let mut set = Self::owned(set, cred);
        set.mode.get_or_insert(0o755);
        let made = self
            .backend
            .create(dir, &name, ObjType::Directory, &set, None, true)
            .await;
        Ok(self.created(dir, made).await)
    }

    async fn remove(&self, r: &mut Reader<'_>, dir_only: bool) -> XdrResult<Writer> {
        let (dir, name) = self.dirop(r).await?;
        let (st, dir) = match dir {
            Ok(d) => (
                self.backend
                    .remove(d, &name, dir_only)
                    .await
                    .err()
                    .unwrap_or(status::OK),
                Some(d),
            ),
            Err(st) => (st, None),
        };
        let a = match dir {
            Some(d) => self.post_op_of(d).await,
            None => None,
        };
        let mut w = reply(st);
        wcc(&mut w, a.as_ref());
        Ok(w)
    }

    async fn rename(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let (from_dir, from) = self.dirop(r).await?;
        let (to_dir, to) = self.dirop(r).await?;
        let st = match (from_dir, to_dir) {
            (Ok(fd), Ok(td)) => self
                .backend
                .rename(fd, &from, td, &to)
                .await
                .err()
                .unwrap_or(status::OK),
            (Err(st), _) | (_, Err(st)) => st,
        };
        let fa = match from_dir {
            Ok(d) => self.post_op_of(d).await,
            Err(_) => None,
        };
        let ta = match to_dir {
            Ok(d) => self.post_op_of(d).await,
            Err(_) => None,
        };
        let mut w = reply(st);
        wcc(&mut w, fa.as_ref());
        wcc(&mut w, ta.as_ref());
        Ok(w)
    }

    async fn readdir(&self, r: &mut Reader<'_>, plus: bool) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let cookie = r.u64()?;
        let verf = r.fixed(8)?;
        let verf = u64::from_be_bytes(verf.try_into().unwrap());
        let (dircount, maxcount) = if plus {
            (r.u32()? as usize, r.u32()? as usize)
        } else {
            let c = r.u32()? as usize;
            (c, c)
        };
        let dir = match fh {
            Ok(d) => d,
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, None);
                return Ok(w);
            }
        };
        let (dattr, entries) = match self.backend.readdir(dir).await {
            Ok(x) => x,
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, None);
                return Ok(w);
            }
        };
        // A changed directory invalidates cookies handed out before.
        let dir_verf = dattr.mtime_ns;
        if cookie != 0 && verf != 0 && verf != dir_verf {
            let mut w = reply(status::BAD_COOKIE);
            post_op(&mut w, Some(&dattr));
            return Ok(w);
        }
        let parent = self
            .backend
            .lookup(dir, "..")
            .await
            .map(|a| a.id)
            .unwrap_or(dir);
        let mut all = vec![(".".to_string(), dir), ("..".to_string(), parent)];
        all.extend(entries.into_iter().map(|e| (e.name, e.id)));

        let mut w = reply(status::OK);
        post_op(&mut w, Some(&dattr));
        w.u64(dir_verf);
        let (mut used, mut dir_used, mut sent) = (w.len() + 8, 0usize, 0usize);
        let mut eof = true;
        for (i, (name, id)) in all.iter().enumerate().skip(cookie as usize) {
            let mut e = Writer::new();
            e.bool(true).u64(*id).string(name).u64(i as u64 + 1);
            let name_bytes = e.len();
            if plus {
                let a = self.backend.getattr(*id).await.ok();
                post_op(&mut e, a.as_ref());
                match self.backend.handle(*id).await {
                    Ok(h) => {
                        e.bool(true).opaque(&h);
                    }
                    Err(_) => {
                        e.bool(false);
                    }
                }
            }
            if used + e.len() > maxcount || dir_used + name_bytes > dircount {
                eof = false;
                break;
            }
            used += e.len();
            dir_used += name_bytes;
            sent += 1;
            w.buf.extend_from_slice(&e.buf);
        }
        if sent == 0 && !eof {
            // Not even one entry fits.
            let mut w = reply(status::TOOSMALL);
            post_op(&mut w, Some(&dattr));
            return Ok(w);
        }
        w.bool(false).bool(eof);
        Ok(w)
    }

    async fn fsstat(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let a = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(st) => return Ok(reply(st)),
        };
        Ok(match self.backend.fsstat().await {
            Ok((total, free)) => {
                let mut w = reply(status::OK);
                post_op(&mut w, a.as_ref());
                let files = 1u64 << 40;
                w.u64(total)
                    .u64(free)
                    .u64(free)
                    .u64(files)
                    .u64(files)
                    .u64(files)
                    .u32(0);
                w
            }
            Err(st) => {
                let mut w = reply(st);
                post_op(&mut w, a.as_ref());
                w
            }
        })
    }

    async fn fsinfo(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let a = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(st) => return Ok(reply(st)),
        };
        let mut w = reply(status::OK);
        post_op(&mut w, a.as_ref());
        w.u32(MAX_IO).u32(MAX_IO).u32(4096); // rtmax, rtpref, rtmult
        w.u32(MAX_IO).u32(MAX_IO).u32(4096); // wtmax, wtpref, wtmult
        w.u32(64 * 1024); // dtpref
        w.u64(1 << 62);
        w.u32(0).u32(1); // time_delta: 1 ns
        w.u32(0x08 | 0x10); // FSF3_HOMOGENEOUS | FSF3_CANSETTIME
        Ok(w)
    }

    async fn pathconf(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let a = match self.fh(r).await? {
            Ok(id) => self.post_op_of(id).await,
            Err(st) => return Ok(reply(st)),
        };
        let mut w = reply(status::OK);
        post_op(&mut w, a.as_ref());
        w.u32(1).u32(NAME_MAX as u32);
        w.bool(true).bool(true).bool(false).bool(true);
        Ok(w)
    }

    async fn commit(&self, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let fh = self.fh(r).await?;
        let _offset = r.u64()?;
        let _count = r.u32()?;
        let id = match fh {
            Ok(id) => id,
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, None);
                return Ok(w);
            }
        };
        let result = self.backend.commit(id).await;
        let a = self.post_op_of(id).await;
        Ok(match result {
            Ok(verifier) => {
                let mut w = reply(status::OK);
                wcc(&mut w, a.as_ref());
                w.fixed(&verifier);
                w
            }
            Err(st) => {
                let mut w = reply(st);
                wcc(&mut w, a.as_ref());
                w
            }
        })
    }

    // ---- MOUNT ----

    pub(crate) async fn mount(&self, call: &Call, r: &mut Reader<'_>) -> Reply {
        let w = match call.proc_ {
            0 | 3 | 4 => {
                // NULL, UMNT, UMNTALL: nothing is tracked.
                if call.proc_ == 3 && r.string(PATH_MAX).is_err() {
                    return Reply::GarbageArgs;
                }
                Writer::new()
            }
            1 => {
                let Ok(path) = r.string(PATH_MAX) else {
                    return Reply::GarbageArgs;
                };
                match self.backend.export(&path).await {
                    Ok(id) => match self.backend.handle(id).await {
                        Ok(h) => {
                            let mut w = reply(0);
                            w.opaque(&h);
                            // AUTH_SYS, AUTH_NONE
                            w.u32(2).u32(1).u32(0);
                            w
                        }
                        Err(_) => reply(10006),
                    },
                    // MNT3ERR_NOENT, MNT3ERR_NOTDIR, or a server fault.
                    Err(status::NOENT) => reply(2),
                    Err(status::NOTDIR) => reply(20),
                    Err(_) => reply(10006),
                }
            }
            2 => {
                let mut w = Writer::new();
                w.bool(false);
                w
            }
            5 => {
                // One export, "/", open to every host.
                let mut w = Writer::new();
                w.bool(true).string("/").bool(false).bool(false);
                w
            }
            _ => return Reply::ProcUnavail,
        };
        Reply::Success(w)
    }

    // ---- portmapper ----

    pub(crate) fn pmap(&self, call: &Call, r: &mut Reader<'_>) -> Reply {
        let ours = [(NFS_PROG, 3), (NFS_PROG, 4), (MOUNT_PROG, 3)];
        let mut w = Writer::new();
        match call.proc_ {
            0 => {}
            3 => {
                let (Ok(prog), Ok(vers), Ok(prot)) = (r.u32(), r.u32(), r.u32()) else {
                    return Reply::GarbageArgs;
                };
                // TCP only.
                let found = prot == 6 && ours.contains(&(prog, vers));
                w.u32(if found { self.port as u32 } else { 0 });
            }
            4 => {
                for (prog, vers) in ours {
                    w.bool(true)
                        .u32(prog)
                        .u32(vers)
                        .u32(6)
                        .u32(self.port as u32);
                }
                w.bool(false);
            }
            _ => return Reply::ProcUnavail,
        }
        Reply::Success(w)
    }
}
