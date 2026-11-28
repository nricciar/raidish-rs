//! Byte-range locks.

use super::super::*;
use raidish_core::role::Mutable;

impl<D: PoolDevice + 'static, R: Mutable> Server<D, R> {
    pub(in crate::v4) fn denied(w: &mut Writer, d: &state::Denied) {
        w.u64(d.range.start)
            .u64(d.range.length())
            .u32(if d.write { 2 } else { 1 })
            .u64(d.client)
            .opaque(&d.owner);
    }

    pub(in crate::v4) async fn lock(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let locktype = r.u32()?;
        let reclaim = r.bool()?;
        let offset = r.u64()?;
        let length = r.u64()?;
        let new_owner = r.bool()?;
        let (open, existing, owner) = if new_owner {
            r.u32()?;
            let open = read_stateid(r)?;
            r.u32()?;
            r.u64()?;
            let owner = r.opaque(1024)?.to_vec();
            (Some(open), None, owner)
        } else {
            let s = read_stateid(r)?;
            r.u32()?;
            (None, Some(s), Vec::new())
        };
        let go = async {
            if !(1..=4).contains(&locktype) {
                return Err(err::INVAL);
            }
            let id = ctx.fh()?;
            let range = Range::of(offset, length)?;
            let by = match (open, existing) {
                (Some(o), _) => Locker::New {
                    open: ctx.stateid(o)?,
                    owner: &owner,
                },
                (_, Some(s)) => Locker::Existing(ctx.stateid(s)?),
                _ => unreachable!(),
            };
            self.st()
                .lock(ctx.client()?, id, by, range, locktype % 2 == 0, reclaim)
        };
        let res = go.await;
        Ok(match res {
            Ok(Ok(sid)) => {
                ctx.cur = Some(sid);
                write_stateid(w, &sid);
                err::OK
            }
            Ok(Err(d)) => {
                Self::denied(w, &d);
                err::DENIED
            }
            Err(e) => e,
        })
    }

    pub(in crate::v4) async fn lockt(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let locktype = r.u32()?;
        let offset = r.u64()?;
        let length = r.u64()?;
        let _clientid = r.u64()?;
        let owner = r.opaque(1024)?.to_vec();
        let go = async {
            let id = ctx.fh()?;
            let range = Range::of(offset, length)?;
            Ok(self
                .st()
                .test_lock(ctx.client()?, &owner, id, range, locktype % 2 == 0))
        };
        let res = go.await;
        Ok(match res {
            Ok(None) => err::OK,
            Ok(Some(d)) => {
                Self::denied(w, &d);
                err::DENIED
            }
            Err(e) => e,
        })
    }

    pub(in crate::v4) async fn locku(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let _locktype = r.u32()?;
        let _seqid = r.u32()?;
        let sid = read_stateid(r)?;
        let offset = r.u64()?;
        let length = r.u64()?;
        let go = async {
            let id = ctx.fh()?;
            let range = Range::of(offset, length)?;
            let sid = ctx.stateid(sid)?;
            self.st().unlock(ctx.client()?, &sid, id, range)
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
}
