//! Reading: attributes, lookups, file data and directories.

use super::super::*;

impl<D: BlockDevice + 'static> Server<D> {
    pub(in crate::nfs::v4) async fn attrs(
        &self,
        w: &mut Writer,
        id: FileId,
        want: &[u32],
        minor: u32,
    ) -> Result<(), NfsStat> {
        let a = self.backend.getattr(id).await.map_err(v4)?;
        let fh = self.backend.handle(id).await.map_err(v4)?;
        let space = if attrs::wants_space(want) {
            Some(self.backend.fsstat().await.map_err(v4)?)
        } else {
            None
        };
        let locations = match &self.cluster {
            Some(c) if attrs::has(want, attrs::FS_LOCATIONS) => c.locations(),
            _ => Vec::new(),
        };
        attrs::encode(
            w,
            want,
            &a,
            &attrs::Extra {
                fh: &fh,
                space,
                locations: &locations,
                minor,
                pnfs: self.pnfs(),
            },
        );
        Ok(())
    }

    pub(in crate::nfs::v4) async fn getattr(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let want = attrs::read_bitmap(r)?;
        let id = match ctx.fh() {
            Ok(id) => id,
            Err(e) => return Ok(e),
        };
        Ok(self
            .attrs(w, id, &want, ctx.minor)
            .await
            .err()
            .unwrap_or(err::OK))
    }

    pub(in crate::nfs::v4) async fn getfh(&self, ctx: &mut Ctx, w: &mut Writer) -> OpResult {
        let id = match ctx.fh() {
            Ok(id) => id,
            Err(e) => return Ok(e),
        };
        Ok(match self.backend.handle(id).await {
            Ok(h) => {
                w.opaque(&h);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    pub(in crate::nfs::v4) async fn lookup(&self, ctx: &mut Ctx, r: &mut Reader<'_>) -> OpResult {
        let name = r.string(1024)?;
        let dir = match ctx.fh() {
            Ok(d) => d,
            Err(e) => return Ok(e),
        };
        if let Err(e) = name_ok(&name) {
            return Ok(e);
        }
        Ok(match self.backend.lookup(dir, &name).await {
            Ok(a) => {
                ctx.cfh = Some(a.id);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    pub(in crate::nfs::v4) async fn lookupp(&self, ctx: &mut Ctx) -> OpResult {
        let dir = match ctx.fh() {
            Ok(d) => d,
            Err(e) => return Ok(e),
        };
        match self.backend.root().await {
            Ok(root) if dir == root => return Ok(err::NOENT),
            Ok(_) => {}
            Err(st) => return Ok(v4(st)),
        }
        Ok(match self.backend.lookup(dir, "..").await {
            Ok(a) => {
                ctx.cfh = Some(a.id);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    pub(in crate::nfs::v4) async fn verify(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        want_same: bool,
    ) -> OpResult {
        let bm = attrs::read_bitmap(r)?;
        let theirs = r.opaque(1 << 16)?.to_vec();
        let id = match ctx.fh() {
            Ok(id) => id,
            Err(e) => return Ok(e),
        };
        if bm.iter().enumerate().any(|(i, word)| {
            (0..32).any(|b| {
                word & (1 << b) != 0 && !attrs::supported(ctx.minor).contains(&(i as u32 * 32 + b))
            })
        }) {
            return Ok(err::ATTRNOTSUPP);
        }
        let mut ours = Writer::new();
        if let Err(e) = self.attrs(&mut ours, id, &bm, ctx.minor).await {
            return Ok(e);
        }
        let mut rr = Reader::new(&ours.buf);
        attrs::read_bitmap(&mut rr)?;
        let same = rr.opaque(1 << 16)? == theirs.as_slice();
        Ok(match (want_same, same) {
            (true, true) | (false, false) => err::OK,
            (true, false) => err::NOT_SAME,
            (false, true) => err::SAME,
        })
    }

    pub(in crate::nfs::v4) async fn access(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let want = r.u32()?;
        let id = match ctx.fh() {
            Ok(id) => id,
            Err(e) => return Ok(e),
        };
        Ok(match self.backend.getattr(id).await {
            Ok(a) => {
                let ok = crate::nfs::v3::access_bits(&a, ctx.cred.as_ref(), want);
                w.u32(want & 0x3f).u32(ok);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    pub(in crate::nfs::v4) async fn read(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let sid = read_stateid(r)?;
        let offset = r.u64()?;
        let count = r.u32()?;
        let go = async {
            let id = ctx.fh()?;
            let a = self.backend.getattr(id).await.map_err(v4)?;
            if a.kind == ObjType::Directory {
                return Err(err::ISDIR);
            }
            let sid = ctx.stateid(sid)?;
            if sid != Stateid::BYPASS {
                self.check_io(ctx, &sid, id, false)?;
            }
            self.backend
                .read(id, offset, count.min(attrs::MAX_IO as u32))
                .await
                .map_err(v4)
        };
        let res = go.await;
        Ok(match res {
            Ok((data, eof)) => {
                w.bool(eof).opaque(&data);
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::nfs::v4) async fn readdir(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let cookie = r.u64()?;
        let verf = r.u64()?;
        let _dircount = r.u32()? as usize;
        let maxcount = r.u32()? as usize;
        let want = attrs::read_bitmap(r)?;
        let dir = match ctx.fh() {
            Ok(d) => d,
            Err(e) => return Ok(e),
        };
        let (dattr, entries) = match self.backend.readdir(dir).await {
            Ok(x) => x,
            Err(st) => return Ok(v4(st)),
        };
        // Cookies 1 and 2 are reserved: entry i is cookie i + 3.
        if cookie == 1 || cookie == 2 {
            return Ok(err::BAD_COOKIE);
        }
        if cookie != 0 && verf != dattr.change {
            return Ok(err::NOT_SAME);
        }
        let skip = cookie.saturating_sub(2) as usize;
        let mut body = Writer::new();
        body.u64(dattr.change);
        // status, verifier, the list's end and eof, roughly.
        let mut used = 4 + 8 + 8;
        let mut eof = true;
        let mut sent = 0;
        for (i, e) in entries.iter().enumerate().skip(skip) {
            let mut ent = Writer::new();
            ent.bool(true).u64(i as u64 + 3).string(&e.name);
            if self.attrs(&mut ent, e.id, &want, ctx.minor).await.is_err() {
                // Gone meanwhile.
                continue;
            }
            if used + ent.len() > maxcount {
                eof = false;
                break;
            }
            used += ent.len();
            sent += 1;
            body.buf.extend_from_slice(&ent.buf);
        }
        if sent == 0 && !eof {
            return Ok(err::TOOSMALL);
        }
        body.bool(false).bool(eof);
        w.buf.extend_from_slice(&body.buf);
        Ok(err::OK)
    }
}
