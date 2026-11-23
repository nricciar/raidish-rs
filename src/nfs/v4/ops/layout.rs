//! pNFS: flexfiles layouts and the devices (nodes) they name.

use super::super::*;

impl<D: BlockDevice + 'static> Server<D> {
    /// The nodes as pNFS data servers (none: no pNFS).
    pub(in crate::nfs::v4) fn devices(&self) -> Vec<Device> {
        self.cluster.as_ref().map_or(Vec::new(), |c| c.devices())
    }

    pub(in crate::nfs::v4) fn pnfs(&self) -> bool {
        !self.devices().is_empty()
    }

    /// LAYOUTGET: a flexfiles layout of the current file.
    pub(in crate::nfs::v4) async fn layoutget(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let _signal = r.bool()?;
        let kind = r.u32()?;
        let iomode = r.u32()?;
        r.u64()?; // offset
        r.u64()?; // length
        r.u64()?; // minlength
        let by = read_stateid(r)?;
        let _maxcount = r.u32()?;
        let go = async {
            let file = ctx.fh()?;
            let devices = self.devices();
            if devices.is_empty() {
                return Err(err::LAYOUTUNAVAILABLE);
            }
            if kind != LAYOUT4_FLEX_FILES {
                return Err(err::UNKNOWN_LAYOUTTYPE);
            }
            let rw = match iomode {
                1 => false,
                2 => true,
                _ => return Err(err::BADIOMODE),
            };
            let a = self.backend.getattr(file).await.map_err(v4)?;
            if a.kind != ObjType::File {
                return Err(err::INVAL);
            }
            let by = ctx.stateid(by)?;
            let sid = self.st().layout_get(ctx.client()?, &by, file, rw)?;
            // Reads: every node. Writes: the node writing it, else this one.
            let mirrors: Vec<&Device> = if rw {
                let writer = self
                    .backend
                    .writer_of(file)
                    .await
                    .unwrap_or_else(|| self.node.clone());
                let pick = devices
                    .iter()
                    .find(|d| d.node == writer)
                    .or(devices.first());
                pick.into_iter().collect()
            } else {
                devices.iter().collect()
            };
            let fh = self.backend.handle(file).await.map_err(v4)?;
            let mut body = Writer::new();
            body.u64(0); // stripe unit: no striping
            body.u32(mirrors.len() as u32);
            for d in &mirrors {
                body.u32(1); // one data server per mirror
                body.fixed(&device_id(d.id)).u32(1);
                write_stateid(&mut body, &Stateid::ANONYMOUS);
                body.u32(1).opaque(&fh);
                body.string(&a.uid.to_string()).string(&a.gid.to_string());
            }
            // FF_FLAGS_NO_LAYOUTCOMMIT: sizes and times are the pool's.
            body.u32(1).u32(0);
            Ok((sid, if rw { 2 } else { 1 }, body))
        };
        let res = go.await;
        Ok(match res {
            Ok((sid, mode, body)) => {
                ctx.cur = Some(sid);
                w.bool(true); // returned at close
                write_stateid(w, &sid);
                w.u32(1).u64(0).u64(u64::MAX).u32(mode);
                w.u32(LAYOUT4_FLEX_FILES).opaque(&body.buf);
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::nfs::v4) async fn layoutreturn(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let _reclaim = r.bool()?;
        let _kind = r.u32()?;
        let _iomode = r.u32()?;
        let returntype = r.u32()?;
        let file_return = if returntype == 1 {
            r.u64()?;
            r.u64()?;
            let sid = read_stateid(r)?;
            r.opaque(1 << 16)?; // flexfiles I/O errors and stats: not kept
            Some(sid)
        } else {
            None
        };
        let client = match ctx.client() {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };
        match file_return {
            Some(sid) => {
                let file = match ctx.fh() {
                    Ok(f) => f,
                    Err(e) => return Ok(e),
                };
                let sid = match ctx.stateid(sid) {
                    Ok(s) => s,
                    Err(e) => return Ok(e),
                };
                // Already gone (returned at close) is fine.
                let _ = self.st().layout_return(client, &sid, file);
            }
            None => self.st().layout_return_all(client),
        }
        w.bool(false); // no layout left
        Ok(err::OK)
    }

    pub(in crate::nfs::v4) async fn layoutcommit(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        r.u64()?;
        r.u64()?;
        r.bool()?;
        read_stateid(r)?;
        if r.bool()? {
            r.u64()?;
        }
        if r.bool()? {
            r.u64()?;
            r.u32()?;
        }
        r.u32()?;
        r.opaque(1 << 16)?;
        if let Err(e) = ctx.fh() {
            return Ok(e);
        }
        // Nothing to update: the pool keeps sizes and times itself.
        w.bool(false);
        Ok(err::OK)
    }

    pub(in crate::nfs::v4) async fn getdeviceinfo(
        &self,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let id: [u8; 16] = r.fixed(16)?.try_into().unwrap();
        let kind = r.u32()?;
        let maxcount = r.u32()?;
        attrs::read_bitmap(r)?;
        if kind != LAYOUT4_FLEX_FILES {
            return Ok(err::UNKNOWN_LAYOUTTYPE);
        }
        let devices = self.devices();
        let Some(d) = node_of_device(&id).and_then(|n| devices.iter().find(|d| d.id == n)) else {
            return Ok(err::NOENT);
        };
        let (netid, addr) = uaddr(&d.addr, d.port);
        let mut body = Writer::new();
        body.u32(1).string(netid).string(&addr);
        // NFSv3, loosely coupled, 1 MiB I/O.
        body.u32(1)
            .u32(3)
            .u32(0)
            .u32(attrs::MAX_IO as u32)
            .u32(attrs::MAX_IO as u32)
            .bool(false);
        if maxcount != 0 && body.len() + 16 > maxcount as usize {
            w.u32(body.len() as u32 + 16);
            return Ok(err::TOOSMALL);
        }
        w.u32(LAYOUT4_FLEX_FILES).opaque(&body.buf);
        w.u32(0); // no notifications
        Ok(err::OK)
    }

    pub(in crate::nfs::v4) async fn layouterror(&self, r: &mut Reader<'_>) -> OpResult {
        r.u64()?;
        r.u64()?;
        read_stateid(r)?;
        let n = r.u32()?;
        if n > 64 {
            return Err(XdrError("too many errors"));
        }
        for _ in 0..n {
            let d: [u8; 16] = r.fixed(16)?.try_into().unwrap();
            let status = r.u32()?;
            let opnum = r.u32()?;
            eprintln!(
                "nfs: a client reports error {} (op {}) from data server node-{}",
                status,
                opnum,
                node_of_device(&d).map_or("?".to_string(), |n| n.to_string())
            );
        }
        Ok(err::OK)
    }
}
