//! Opens, with share reservations (state in `state`).

use super::super::*;

impl<D: BlockDevice + 'static> Server<D> {
    pub(in crate::nfs::v4) async fn open(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let _seqid = r.u32()?;
        let access = r.u32()?;
        let deny = r.u32()?;
        let owner_client = r.u64()?;
        let owner = r.opaque(1024)?.to_vec();
        let now = crate::inode::now_ns();
        // How: (create, guarded, exclusive verifier, attributes).
        let opentype = r.u32()?;
        let mut how: Option<(bool, Option<u64>, Result<attrs::Settable, NfsStat>)> = None;
        if opentype == 1 {
            how = Some(match r.u32()? {
                m @ (0 | 1) => (m == 1, None, attrs::decode(r, now)),
                2 => {
                    let v = r.u64()?;
                    (false, Some(v), Ok(empty_settable()))
                }
                3 => {
                    let v = r.u64()?;
                    (false, Some(v), attrs::decode(r, now))
                }
                _ => return Err(XdrError("createmode")),
            });
        }
        let claim = r.u32()?;
        let name = match claim {
            0 => Some(r.string(1024)?),
            1 => {
                r.u32()?;
                None
            }
            2 => {
                read_stateid(r)?;
                Some(r.string(1024)?)
            }
            3 => Some(r.string(1024)?),
            5 => {
                read_stateid(r)?;
                None
            }
            4 | 6 => None,
            _ => return Err(XdrError("claim")),
        };
        let go = async {
            let client = ctx.client()?;
            if owner_client != client {
                return Err(err::STALE_CLIENTID);
            }
            let (acc, den) = (access & 3, deny & 3);
            if acc == 0 {
                return Err(err::INVAL);
            }
            let here = ctx.fh()?;
            // Nothing changes before the grace and share checks: a create
            // waits for grace, and the share reservation is taken before a
            // truncate (and given back if it fails).
            self.st().may_open(client, claim == 1)?;
            let (file, cinfo, bitmap, truncate) = match claim {
                // CLAIM_NULL: by name in the current directory.
                0 => {
                    let name = name.unwrap();
                    name_ok(&name)?;
                    let before = self.dir_change(here).await;
                    let (id, bm, truncate) = match how {
                        None => {
                            let a = self.backend.lookup(here, &name).await.map_err(v4)?;
                            (a.id, Vec::new(), None)
                        }
                        Some((guarded, exclusive, attrs)) => {
                            let s = attrs?;
                            let mut set = self.owned(s.set.clone(), ctx);
                            set.mode.get_or_insert(0o644);
                            let a = self
                                .backend
                                .create(here, &name, ObjType::File, &set, exclusive, guarded)
                                .await
                                .map_err(v4)?;
                            // UNCHECKED on an existing file with a size: truncate.
                            let truncate = s.size.filter(|&size| a.size != size);
                            (a.id, s.bitmap, truncate)
                        }
                    };
                    (
                        id,
                        Some((before, self.dir_change(here).await)),
                        bm,
                        truncate,
                    )
                }
                // CLAIM_FH: the current filehandle itself.
                4 => {
                    if how.is_some() {
                        return Err(err::INVAL);
                    }
                    (here, None, Vec::new(), None)
                }
                // CLAIM_PREVIOUS: a reclaim of the current filehandle.
                1 => {
                    if how.is_some() {
                        return Err(err::INVAL);
                    }
                    (here, None, Vec::new(), None)
                }
                // There are no delegations to claim.
                _ => return Err(err::NOTSUPP),
            };
            let a = self.backend.getattr(file).await.map_err(v4)?;
            if a.kind == ObjType::Directory {
                return Err(err::ISDIR);
            }
            let (sid, undo) =
                self.st()
                    .open_undoable(client, &owner, file, acc, den, claim == 1)?;
            if let Some(size) = truncate
                && let Err(e) = self
                    .backend
                    .setattr(file, &SetAttrs::default(), Some(size))
                    .await
            {
                self.st().unopen(undo);
                return Err(v4(e));
            }
            Ok((file, sid, cinfo, bitmap))
        };
        let res = go.await;
        Ok(match res {
            Ok((file, sid, cinfo, bitmap)) => {
                ctx.cfh = Some(file);
                ctx.cur = Some(sid);
                write_stateid(w, &sid);
                let (b, a) = cinfo.unwrap_or((0, 0));
                change_info(w, b, a);
                w.u32(0x4); // OPEN4_RESULT_LOCKTYPE_POSIX
                attrs::write_bitmap(w, &bitmap);
                w.u32(0); // OPEN_DELEGATE_NONE
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::nfs::v4) async fn open_downgrade(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let sid = read_stateid(r)?;
        let _seqid = r.u32()?;
        let access = r.u32()? & 3;
        let deny = r.u32()? & 3;
        let go = async {
            let id = ctx.fh()?;
            let sid = ctx.stateid(sid)?;
            self.st().downgrade(ctx.client()?, &sid, id, access, deny)
        };
        let res = go.await;
        Ok(match res {
            Ok(s) => {
                ctx.cur = Some(s);
                write_stateid(w, &s);
                err::OK
            }
            Err(e) => e,
        })
    }

    pub(in crate::nfs::v4) async fn close(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let _seqid = r.u32()?;
        let sid = read_stateid(r)?;
        let go = async {
            let id = ctx.fh()?;
            let sid = ctx.stateid(sid)?;
            self.st().close(ctx.client()?, &sid, id)
        };
        let res = go.await;
        Ok(match res {
            Ok(()) => {
                write_stateid(w, &Stateid::INVALID);
                err::OK
            }
            Err(e) => e,
        })
    }
}
