//! Client IDs and sessions: EXCHANGE_ID, CREATE_SESSION and the SEQUENCE every other request starts with.

use super::super::*;

impl<D: BlockDevice + 'static> Server<D> {
    pub(in crate::nfs::v4) async fn sequence(
        &self,
        ctx: &mut Ctx,
        r: &mut Reader<'_>,
        w: &mut Writer,
        count: u32,
        pre: Option<(u64, u32)>,
    ) -> XdrResult<Seq> {
        let sid: [u8; 16] = r.fixed(16)?.try_into().unwrap();
        let seqid = r.u32()?;
        let slotid = r.u32()?;
        let _highest = r.u32()?;
        let cache = r.bool()?;
        if count > state::MAX_OPS {
            return Ok(Seq::Run(err::TOO_MANY_OPS, None));
        }
        if let Some((client, slots)) = pre {
            ctx.client = Some(client);
            ctx.trusted = true;
            w.fixed(&sid).u32(seqid).u32(slotid);
            w.u32(slots - 1).u32(slots - 1).u32(0);
            return Ok(Seq::Run(err::OK, None));
        }
        let st = self.st();
        match st.sequence(&sid, slotid, seqid) {
            Err(st) => Ok(Seq::Run(st, None)),
            Ok(Sequenced::Replay(bytes)) => Ok(Seq::Replay(bytes)),
            Ok(Sequenced::New { client }) => {
                ctx.client = Some(client);
                let slots = st.slots(&sid).unwrap_or(1);
                w.fixed(&sid).u32(seqid).u32(slotid);
                w.u32(slots - 1).u32(slots - 1).u32(0);
                Ok(Seq::Run(err::OK, Some((sid, slotid, cache))))
            }
        }
    }

    pub(in crate::nfs::v4) async fn exchange_id(
        &self,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let verifier: [u8; 8] = r.fixed(8)?.try_into().unwrap();
        let owner = r.opaque(1024)?.to_vec();
        let flags = r.u32()?;
        let how = r.u32()?;
        match how {
            0 => {}
            1 => {
                attrs::read_bitmap(r)?;
                attrs::read_bitmap(r)?;
            }
            _ => return Ok(err::NOTSUPP),
        }
        let impls = r.u32()?;
        if impls > 1 {
            return Err(XdrError("impl id"));
        }
        for _ in 0..impls {
            r.string(1024)?;
            r.string(1024)?;
            r.u64()?;
            r.u32()?;
        }
        if how != 0 {
            // Machine credential protection needs Kerberos.
            return Ok(err::NOTSUPP);
        }
        let update = flags & 0x4000_0000 != 0;
        let Exchanged {
            clientid,
            sequenceid,
            confirmed,
        } = match self.st().exchange_id(&owner, verifier, update) {
            Ok(x) => x,
            Err(e) => return Ok(e),
        };
        let identity = self.identity().await;
        w.u64(clientid).u32(sequenceid);
        // USE_PNFS_MDS when layouts are handed out (else USE_NON_PNFS),
        // and CONFIRMED_R for a confirmed record.
        let role = if self.pnfs() {
            0x0002_0000
        } else {
            0x0001_0000
        };
        w.u32(role | if confirmed { 0x8000_0000 } else { 0 });
        w.u32(0); // SP4_NONE
        w.u64(0).opaque(&identity); // server_owner
        w.opaque(&identity); // server_scope
        w.u32(1).string("raidish").string("raidish").u64(0).u32(0);
        Ok(err::OK)
    }

    pub(in crate::nfs::v4) async fn create_session(
        &self,
        r: &mut Reader<'_>,
        w: &mut Writer,
    ) -> OpResult {
        let clientid = r.u64()?;
        let seq = r.u32()?;
        let _flags = r.u32()?;
        let fore = read_channel(r)?;
        let back = read_channel(r)?;
        let _cb_program = r.u32()?;
        skip_cb_sec(r)?;
        let granted = Channel {
            max_request: fore.max_request.min(state::MAX_REQUEST),
            max_response: fore.max_response.min(state::MAX_REQUEST),
            max_cached: fore.max_cached.min(state::MAX_CACHED_REPLY),
            max_ops: fore.max_ops.min(state::MAX_OPS),
            max_requests: fore.max_requests.clamp(1, state::MAX_SLOTS),
        };
        let slots = granted.max_requests;
        let made = self.st().create_session(clientid, seq, slots, |sid| {
            let mut b = Writer::new();
            // No flags: not persistent, and no back channel on it.
            b.fixed(&sid).u32(seq).u32(0);
            write_channel(&mut b, &granted);
            write_channel(&mut b, &back);
            b.into_bytes()
        });
        Ok(match made {
            Ok((body, newly)) => {
                if let Some(owner) = newly {
                    // Before the client relies on it: it may reclaim after
                    // a failover.
                    self.record(&owner, true).await;
                }
                w.buf.extend_from_slice(&body);
                err::OK
            }
            Err(e) => e,
        })
    }
}
