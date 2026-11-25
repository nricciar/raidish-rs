//! Writing: file data, attributes and the namespace.

use super::super::*;

impl<D: BlockDevice + 'static> Server<D> {
    pub(in crate::v4) async fn write(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let sid = read_stateid(r)?;
        let offset = r.u64()?;
        let stable = r.u32()?;
        let data = r.opaque(attrs::MAX_IO as usize)?;
        let go = async {
            let id = ctx.fh()?;
            let sid = ctx.stateid(sid)?;
            self.check_io(ctx, &sid, id, true)?;
            self.backend
                .write(id, offset, data, stable != 0)
                .await
                .map_err(v4)
        };
        let res = go.await;
        Ok(match res {
            Ok((_, verifier)) => {
                // FILE_SYNC4 for anything but UNSTABLE4.
                w.u32(data.len() as u32)
                    .u32(if stable != 0 { 2 } else { 0 })
                    .fixed(&verifier);
                err::OK
            }
            Err(e) => e,
        })
    }

    /// SEEK: the next data or hole (NFSv4.2).
    pub(in crate::v4) async fn seek(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let sid = read_stateid(r)?;
        let offset = r.u64()?;
        let what = r.u32()?;
        let go = async {
            let id = ctx.fh()?;
            if what > 1 {
                return Err(err::INVAL);
            }
            let sid = ctx.stateid(sid)?;
            if sid != Stateid::BYPASS {
                self.check_io(ctx, &sid, id, false)?;
            }
            self.backend.seek(id, offset, what == 0).await.map_err(v4)
        };
        let res = go.await;
        Ok(match res {
            Ok((at, eof)) => {
                w.bool(eof).u64(at);
                err::OK
            }
            Err(e) => e,
        })
    }

    /// DEALLOCATE: punch a hole (NFSv4.2).
    pub(in crate::v4) async fn deallocate(&self, ctx: &mut Ctx, r: &mut Reader<'_>) -> OpResult {
        let sid = read_stateid(r)?;
        let offset = r.u64()?;
        let len = r.u64()?;
        let go = async {
            let id = ctx.fh()?;
            let sid = ctx.stateid(sid)?;
            self.check_io(ctx, &sid, id, true)?;
            self.backend.deallocate(id, offset, len).await.map_err(v4)
        };
        Ok(go.await.err().unwrap_or(err::OK))
    }

    pub(in crate::v4) async fn commit(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        r.u64()?;
        r.u32()?;
        let id = match ctx.fh() {
            Ok(id) => id,
            Err(e) => return Ok(e),
        };
        Ok(match self.backend.commit(id).await {
            Ok(v) => {
                w.fixed(&v);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    pub(in crate::v4) async fn setattr(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let sid = read_stateid(r)?;
        let decoded = attrs::decode(r, crate::inode::now_ns());
        let go = async {
            let id = ctx.fh()?;
            let s = decoded?;
            if let Some(size) = s.size {
                let a = self.backend.getattr(id).await.map_err(v4)?;
                if a.kind == ObjType::Directory {
                    return Err(err::ISDIR);
                }
                // The all-ones stateid bypasses share reservations only
                // for READ: with SETATTR it is the anonymous one, so an
                // open denying writes refuses it (RFC 8881 §8.2.3). It used
                // to skip the check, and truncate a file opened DENY_WRITE.
                let sid = ctx.stateid(sid)?;
                self.check_io(ctx, &sid, id, true)?;
                self.backend
                    .setattr(id, &SetAttrs::default(), Some(size))
                    .await
                    .map_err(v4)?;
            }
            if s.set != SetAttrs::default() {
                self.backend.setattr(id, &s.set, None).await.map_err(v4)?;
            }
            Ok(s.bitmap)
        };
        // attrsset comes with failures too (empty then).
        let (st, bm) = match go.await {
            Ok(bm) => (err::OK, bm),
            Err(e) => (e, Vec::new()),
        };
        attrs::write_bitmap(w, &bm);
        Ok(st)
    }

    /// The owner of new objects: the caller, unless the attributes say.
    pub(in crate::v4) fn owned(&self, mut set: SetAttrs, ctx: &Ctx) -> SetAttrs {
        if let Some(c) = &ctx.cred {
            set.uid.get_or_insert(c.uid);
            set.gid.get_or_insert(c.gid);
        }
        set
    }

    pub(in crate::v4) async fn dir_change(&self, dir: FileId) -> u64 {
        self.backend.getattr(dir).await.map_or(0, |a| a.change)
    }

    pub(in crate::v4) async fn create(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let kind = r.u32()?;
        match kind {
            5 => {
                r.string(4096)?;
            }
            3 | 4 => {
                r.u32()?;
                r.u32()?;
            }
            _ => {}
        }
        let name = r.string(1024)?;
        let decoded = attrs::decode(r, crate::inode::now_ns());
        let go = async {
            let dir = ctx.fh()?;
            name_ok(&name)?;
            match kind {
                2 => {}
                5 => return Err(err::NOTSUPP),
                _ => return Err(err::BADTYPE),
            }
            let s = decoded?;
            let mut set = self.owned(s.set, ctx);
            set.mode.get_or_insert(0o755);
            let before = self.dir_change(dir).await;
            let a = self
                .backend
                .create(dir, &name, ObjType::Directory, &set, None, true)
                .await
                .map_err(v4)?;
            Ok((a.id, before, self.dir_change(dir).await, s.bitmap))
        };
        let res = go.await;
        Ok(match res {
            Ok((id, before, after, bm)) => {
                ctx.cfh = Some(id);
                change_info(w, before, after);
                attrs::write_bitmap(w, &bm);
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::v4) async fn remove(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let name = r.string(1024)?;
        let go = async {
            let dir = ctx.fh()?;
            name_ok(&name)?;
            let target = self.backend.lookup(dir, &name).await.map_err(v4)?;
            let before = self.dir_change(dir).await;
            self.backend
                .remove(dir, &name, target.kind == ObjType::Directory)
                .await
                .map_err(v4)?;
            self.st().forget_file(target.id);
            Ok((before, self.dir_change(dir).await))
        };
        let res = go.await;
        Ok(match res {
            Ok((before, after)) => {
                change_info(w, before, after);
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::v4) async fn rename(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let from = r.string(1024)?;
        let to = r.string(1024)?;
        let go = async {
            let src = ctx.sfh.ok_or(err::NOFILEHANDLE)?;
            let dst = ctx.fh()?;
            name_ok(&from)?;
            name_ok(&to)?;
            let (sb, db) = (self.dir_change(src).await, self.dir_change(dst).await);
            let replaced = self.backend.lookup(dst, &to).await.ok().map(|a| a.id);
            let moved = self.backend.lookup(src, &from).await.ok().map(|a| a.id);
            self.backend
                .rename(src, &from, dst, &to)
                .await
                .map_err(v4)?;
            // The file replaced is gone, and its state with it; unless it's
            // the one moved (a rename onto itself, or onto another link to
            // it), which is still there and still open (I-32).
            if let Some(id) = replaced
                && replaced != moved
            {
                self.st().forget_file(id);
            }
            Ok((
                sb,
                self.dir_change(src).await,
                db,
                self.dir_change(dst).await,
            ))
        };
        let res = go.await;
        Ok(match res {
            Ok((sb, sa, db, da)) => {
                change_info(w, sb, sa);
                change_info(w, db, da);
                err::OK
            }
            Err(e) => e,
        })
    }
}
