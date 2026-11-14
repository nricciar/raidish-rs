//! NFS version 4.1 (RFC 8881) on one node, over the shared backend.
//!
//! * Sessions with exactly-once semantics: every request but the session
//!   management ones starts with `SEQUENCE`, whose slot keeps the reply
//!   for retransmissions when the client asks (`sa_cachethis`).
//! * Opens (with share reservations) and byte-range locks, in `state`.
//! * Reads, writes and commits go through the backend: the same write
//!   leases, forwarding and verifiers as NFSv3.
//! * pNFS with flexfiles layouts (RFC 8435) on a cluster: every node's
//!   NFSv3 server is a data server with the same file handles, so a read
//!   layout lists every node as a mirror (a client reads from any and
//!   moves on if one fails), and a read-write layout the node writing the
//!   file (others forward to it anyway). Layouts are whole-file and handed
//!   back at close; with nothing to recall them by (no callbacks), they
//!   can't go stale in a harmful way: every data server serves the pool.
//! * No delegations (never granted), no symlinks or hard links.
//!   Directory change info is reported as not atomic (the client then
//!   checks the directory again).
//! * On a cluster, one node owns the state (the `nfs4` lease holder; see
//!   `V4Cluster`). Elsewhere, a request that only reads or writes data
//!   (Linux's READ, WRITE, COMMIT, GETATTR... without `sa_cachethis`) has
//!   its SEQUENCE checked by the owner, then runs here; anything else (opens,
//!   locks, namespace changes, cached replies) is handed to the owner whole.
//!   Stateids in such data requests aren't checked here: every request's
//!   SEQUENCE is, so a client whose state is gone finds out from that.
//! * A new owner takes the client records the cluster keeps and gives
//!   those clients a grace period to reclaim their opens and locks.
//! * Every node is the same server to clients: one `server_owner` and
//!   `server_scope` (from the pool), so a client may spread one session
//!   over connections to several nodes (session trunking), and
//!   `fs_locations` lists every node's address, for clients to find them.

pub mod attrs;
pub mod state;

use crate::disk::{BlockDevice, FileId};
use crate::inode::{ObjType, ROOT_OBJECT, SetAttrs};
use crate::nfs::backend::{Backend, status as s3};
use crate::nfs::rpc::{Call, Cred, Reply};
use crate::nfs::xdr::{Reader, Writer, XdrError, XdrResult};
use state::{Exchanged, Locker, Range, Sequenced, StateTable, Stateid};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The flexfiles layout type.
pub const LAYOUT4_FLEX_FILES: u32 = 4;

/// A node's pNFS device id (16 bytes, from its id).
fn device_id(node: u64) -> [u8; 16] {
    let mut d = [0u8; 16];
    d[..8].copy_from_slice(b"raidish\0");
    d[8..].copy_from_slice(&node.to_be_bytes());
    d
}

fn node_of_device(d: &[u8; 16]) -> Option<u64> {
    (&d[..8] == b"raidish\0").then(|| u64::from_be_bytes(d[8..].try_into().unwrap()))
}

/// An RPC universal address (RFC 5665) and its netid.
fn uaddr(addr: &str, port: u16) -> (&'static str, String) {
    let netid = if addr.contains(':') { "tcp6" } else { "tcp" };
    (netid, format!("{}.{}.{}", addr, port >> 8, port & 0xff))
}

/// How long a new state owner waits for clients to reclaim.
pub const GRACE: std::time::Duration = std::time::Duration::from_secs(state::LEASE_SECS as u64 + 5);

/// A request one node's NFSv4 server hands the state owner.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum V4Forward {
    /// A whole COMPOUND (its arguments), with the caller's credentials.
    Compound { args: Vec<u8>, cred: Option<Cred> },
    /// Just a SEQUENCE check, for a request that runs where it arrived.
    Sequence {
        session: [u8; 16],
        slot: u32,
        seqid: u32,
    },
}

/// The state owner's answer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum V4Forwarded {
    /// The COMPOUND's reply, as is.
    Reply(Vec<u8>),
    /// The SEQUENCE went through (the client, the session's slot count),
    /// or its status.
    Sequenced(Result<(u64, u32), u32>),
    /// Not the owner (any more): try again.
    NotOwner,
}

/// The cluster, as the NFSv4 server needs it.
#[async_trait::async_trait]
pub trait V4Cluster: Send + Sync {
    /// The node (by name) that owns the state now, if any.
    fn owner(&self) -> Option<String>;
    async fn forward(&self, node: &str, req: &V4Forward) -> Result<V4Forwarded, String>;
    /// Record (or forget) that the client with owner string `client`
    /// holds state that it may reclaim after a failover.
    async fn record(&self, client: &[u8], present: bool) -> Result<(), String>;
    /// Addresses (host names or IPs) of every node's NFS server, for
    /// `fs_locations`. Empty: none to tell.
    fn locations(&self) -> Vec<String> {
        Vec::new()
    }
    /// Every node as a pNFS data server (its NFSv3 server). Empty: no pNFS.
    fn devices(&self) -> Vec<Device> {
        Vec::new()
    }
}

/// A node as a pNFS (flexfiles) data server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// The node's id (its device id is made from it).
    pub id: u64,
    /// Its name (`node-<id>`).
    pub node: String,
    /// An IP address clients reach its NFS server at, and the port.
    pub addr: String,
    pub port: u16,
}

/// NFSv4 status codes.
pub mod err {
    pub const OK: u32 = 0;
    pub const NOENT: u32 = 2;
    pub const IO: u32 = 5;
    pub const NXIO: u32 = 6;
    pub const EXIST: u32 = 17;
    pub const NOTDIR: u32 = 20;
    pub const ISDIR: u32 = 21;
    pub const INVAL: u32 = 22;
    pub const NOSPC: u32 = 28;
    pub const NAMETOOLONG: u32 = 63;
    pub const NOTEMPTY: u32 = 66;
    pub const STALE: u32 = 70;
    pub const BADHANDLE: u32 = 10001;
    pub const BAD_COOKIE: u32 = 10003;
    pub const GRACE: u32 = 10013;
    pub const RECLAIM_BAD: u32 = 10034;
    pub const NOTSUPP: u32 = 10004;
    pub const TOOSMALL: u32 = 10005;
    pub const SERVERFAULT: u32 = 10006;
    pub const BADTYPE: u32 = 10007;
    pub const DELAY: u32 = 10008;
    pub const SAME: u32 = 10009;
    pub const DENIED: u32 = 10010;
    pub const LOCKED: u32 = 10012;
    pub const SHARE_DENIED: u32 = 10015;
    pub const NOFILEHANDLE: u32 = 10020;
    pub const MINOR_VERS_MISMATCH: u32 = 10021;
    pub const STALE_CLIENTID: u32 = 10022;
    pub const STALE_STATEID: u32 = 10023;
    pub const OLD_STATEID: u32 = 10024;
    pub const BAD_STATEID: u32 = 10025;
    pub const NOT_SAME: u32 = 10027;
    pub const RESTOREFH: u32 = 10030;
    pub const ATTRNOTSUPP: u32 = 10032;
    pub const NO_GRACE: u32 = 10033;
    pub const BADXDR: u32 = 10036;
    pub const LOCKS_HELD: u32 = 10037;
    pub const BADOWNER: u32 = 10039;
    pub const BADNAME: u32 = 10041;
    pub const OP_ILLEGAL: u32 = 10044;
    pub const BADIOMODE: u32 = 10049;
    pub const BADSESSION: u32 = 10052;
    pub const LAYOUTUNAVAILABLE: u32 = 10059;
    pub const UNKNOWN_LAYOUTTYPE: u32 = 10062;
    pub const BADSLOT: u32 = 10053;
    pub const COMPLETE_ALREADY: u32 = 10054;
    pub const SEQ_MISORDERED: u32 = 10063;
    pub const SEQUENCE_POS: u32 = 10064;
    pub const RETRY_UNCACHED_REP: u32 = 10068;
    pub const TOO_MANY_OPS: u32 = 10070;
    pub const OP_NOT_IN_SESSION: u32 = 10071;
    pub const CLIENTID_BUSY: u32 = 10074;
    pub const NOT_ONLY_OP: u32 = 10081;
}

/// Operation numbers.
pub mod op {
    pub const ACCESS: u32 = 3;
    pub const CLOSE: u32 = 4;
    pub const COMMIT: u32 = 5;
    pub const CREATE: u32 = 6;
    pub const GETATTR: u32 = 9;
    pub const GETFH: u32 = 10;
    pub const LINK: u32 = 11;
    pub const LOCK: u32 = 12;
    pub const LOCKT: u32 = 13;
    pub const LOCKU: u32 = 14;
    pub const LOOKUP: u32 = 15;
    pub const LOOKUPP: u32 = 16;
    pub const NVERIFY: u32 = 17;
    pub const OPEN: u32 = 18;
    pub const OPEN_DOWNGRADE: u32 = 21;
    pub const PUTFH: u32 = 22;
    pub const PUTPUBFH: u32 = 23;
    pub const PUTROOTFH: u32 = 24;
    pub const READ: u32 = 25;
    pub const READDIR: u32 = 26;
    pub const READLINK: u32 = 27;
    pub const REMOVE: u32 = 28;
    pub const RENAME: u32 = 29;
    pub const RESTOREFH: u32 = 31;
    pub const SAVEFH: u32 = 32;
    pub const SECINFO: u32 = 33;
    pub const SETATTR: u32 = 34;
    pub const VERIFY: u32 = 37;
    pub const WRITE: u32 = 38;
    pub const BACKCHANNEL_CTL: u32 = 40;
    pub const BIND_CONN_TO_SESSION: u32 = 41;
    pub const EXCHANGE_ID: u32 = 42;
    pub const CREATE_SESSION: u32 = 43;
    pub const DESTROY_SESSION: u32 = 44;
    pub const FREE_STATEID: u32 = 45;
    pub const SECINFO_NO_NAME: u32 = 52;
    pub const SEQUENCE: u32 = 53;
    pub const TEST_STATEID: u32 = 55;
    pub const DESTROY_CLIENTID: u32 = 57;
    pub const RECLAIM_COMPLETE: u32 = 58;
    pub const GETDEVICEINFO: u32 = 47;
    pub const LAYOUTCOMMIT: u32 = 49;
    pub const LAYOUTGET: u32 = 50;
    pub const LAYOUTRETURN: u32 = 51;
    pub const DEALLOCATE: u32 = 62;
    pub const LAYOUTERROR: u32 = 64;
    pub const LAYOUTSTATS: u32 = 65;
    pub const SEEK: u32 = 69;
    pub const ILLEGAL: u32 = 10044;
}

/// What the backend's (NFSv3-numbered) status is in NFSv4 terms. The
/// shared codes have the same numbers; `JUKEBOX` is `DELAY`.
fn v4(st: u32) -> u32 {
    match st {
        s3::NOT_SYNC => err::INVAL,
        s3::JUKEBOX => err::DELAY,
        other => other,
    }
}

/// The per-request context: current and saved filehandle and stateid.
#[derive(Default)]
struct Ctx {
    /// The COMPOUND's minor version (1 or 2).
    minor: u32,
    /// The SEQUENCE was checked by the state owner, which has the state:
    /// stateids can't be checked here.
    trusted: bool,
    cfh: Option<FileId>,
    sfh: Option<FileId>,
    cur: Option<Stateid>,
    saved: Option<Stateid>,
    client: Option<u64>,
    cred: Option<Cred>,
}

impl Ctx {
    fn fh(&self) -> Result<FileId, u32> {
        self.cfh.ok_or(err::NOFILEHANDLE)
    }
    fn client(&self) -> Result<u64, u32> {
        self.client.ok_or(err::OP_NOT_IN_SESSION)
    }
    /// A stateid argument, with "the current stateid" resolved.
    fn stateid(&self, s: Stateid) -> Result<Stateid, u32> {
        if s == Stateid::CURRENT {
            return self.cur.ok_or(err::BAD_STATEID);
        }
        Ok(s)
    }
}

/// An operation's outcome: its status, then (if OK) its results in `w`.
type OpResult = Result<u32, XdrError>;

fn read_stateid(r: &mut Reader<'_>) -> XdrResult<Stateid> {
    let seqid = r.u32()?;
    let other = r.fixed(12)?.try_into().unwrap();
    Ok(Stateid { seqid, other })
}

fn write_stateid(w: &mut Writer, s: &Stateid) {
    w.u32(s.seqid).fixed(&s.other);
}

/// `component4`: a name in a directory.
fn name_ok(n: &str) -> Result<(), u32> {
    if n.is_empty() {
        return Err(err::INVAL);
    }
    if n == "." || n == ".." || n.contains('/') || n.contains('\0') {
        return Err(err::BADNAME);
    }
    if n.len() > crate::inode::MAX_NAME_BYTES {
        return Err(err::NAMETOOLONG);
    }
    Ok(())
}

fn change_info(w: &mut Writer, before: u64, after: u64) {
    // Not atomic: the client looks at the directory again.
    w.bool(false).u64(before).u64(after);
}

fn secinfo(w: &mut Writer) {
    // AUTH_SYS only.
    w.u32(1).u32(1);
}

/// Session channel attributes as asked and as granted.
struct Channel {
    max_request: u32,
    max_response: u32,
    max_cached: u32,
    max_ops: u32,
    max_requests: u32,
}

fn read_channel(r: &mut Reader<'_>) -> XdrResult<Channel> {
    let _pad = r.u32()?;
    let c = Channel {
        max_request: r.u32()?,
        max_response: r.u32()?,
        max_cached: r.u32()?,
        max_ops: r.u32()?,
        max_requests: r.u32()?,
    };
    let n = r.u32()?;
    if n > 1 {
        return Err(XdrError("rdma_ird"));
    }
    for _ in 0..n {
        r.u32()?;
    }
    Ok(c)
}

fn write_channel(w: &mut Writer, c: &Channel) {
    w.u32(0)
        .u32(c.max_request)
        .u32(c.max_response)
        .u32(c.max_cached)
        .u32(c.max_ops)
        .u32(c.max_requests)
        .u32(0);
}

/// `callback_sec_parms4<>` (read and ignored: there are no callbacks).
fn skip_cb_sec(r: &mut Reader<'_>) -> XdrResult<()> {
    let n = r.u32()?;
    if n > 16 {
        return Err(XdrError("cb sec parms"));
    }
    for _ in 0..n {
        match r.u32()? {
            0 => {}
            1 => {
                r.u32()?;
                r.string(255)?;
                r.u32()?;
                r.u32()?;
                let g = r.u32()?;
                if g > 16 {
                    return Err(XdrError("gids"));
                }
                for _ in 0..g {
                    r.u32()?;
                }
            }
            6 => {
                r.u32()?;
                r.opaque(1024)?;
                r.opaque(1024)?;
            }
            _ => return Err(XdrError("cb sec flavor")),
        }
    }
    Ok(())
}

pub struct Server<D: BlockDevice> {
    backend: Arc<Backend<D>>,
    node: String,
    state: std::sync::RwLock<Arc<StateTable>>,
    cluster: Option<Arc<dyn V4Cluster>>,
    /// Whether this node owns the state (always, without a cluster).
    owning: AtomicBool,
}

impl<D: BlockDevice + 'static> Server<D> {
    /// A server that owns its state (a pool only this node serves).
    pub fn new(backend: Arc<Backend<D>>, node: &str) -> Self {
        Self::make(backend, node, None, true)
    }

    /// A server on a cluster node: a follower until `become_owner`.
    pub fn with_cluster(backend: Arc<Backend<D>>, node: &str, cluster: Arc<dyn V4Cluster>) -> Self {
        Self::make(backend, node, Some(cluster), false)
    }

    fn make(
        backend: Arc<Backend<D>>,
        node: &str,
        cluster: Option<Arc<dyn V4Cluster>>,
        owning: bool,
    ) -> Self {
        Server {
            backend,
            node: node.to_string(),
            state: std::sync::RwLock::new(Arc::new(StateTable::new())),
            cluster,
            owning: AtomicBool::new(owning),
        }
    }

    /// The state table (it's replaced on an ownership change).
    pub fn st(&self) -> Arc<StateTable> {
        self.state.read().unwrap().clone()
    }

    pub fn is_owner(&self) -> bool {
        self.owning.load(Ordering::SeqCst)
    }

    /// Take over the state: none is carried over, but `records` (clients
    /// that held state before) may reclaim theirs for `GRACE`.
    pub fn become_owner(&self, records: Vec<Vec<u8>>) {
        *self.state.write().unwrap() = Arc::new(StateTable::with_grace(records, GRACE));
        self.owning.store(true, Ordering::SeqCst);
    }

    /// Hand the state over (it's dropped here).
    pub fn become_follower(&self) {
        self.owning.store(false, Ordering::SeqCst);
        *self.state.write().unwrap() = Arc::new(StateTable::new());
    }

    async fn record(&self, client: &[u8], present: bool) {
        if let Some(c) = &self.cluster
            && let Err(e) = c.record(client, present).await
        {
            eprintln!("nfs: recording an NFSv4 client: {}", e);
        }
    }

    /// Expire clients that stopped renewing, end the grace period when
    /// it's over, and keep the cluster's records in step.
    pub async fn tidy(&self) {
        if !self.is_owner() {
            return;
        }
        let st = self.st();
        let expired = st.expire();
        if !expired.is_empty() {
            eprintln!(
                "nfs: {} NFSv4 client(s) let their lease expire",
                expired.len()
            );
        }
        let missed = st.grace_over().unwrap_or_default();
        if !missed.is_empty() {
            eprintln!(
                "nfs: grace period over; {} client(s) didn't reclaim",
                missed.len()
            );
        }
        for c in expired.iter().chain(&missed) {
            self.record(c, false).await;
        }
    }

    /// Do a request another node handed this one (the state owner).
    pub async fn serve_forward(&self, req: V4Forward) -> V4Forwarded {
        if !self.is_owner() {
            return V4Forwarded::NotOwner;
        }
        match req {
            V4Forward::Compound { args, cred } => {
                let call = Call {
                    xid: 0,
                    prog: crate::nfs::v3::NFS_PROG,
                    vers: 4,
                    proc_: 1,
                    cred,
                };
                match self.compound(&call, &mut Reader::new(&args), None).await {
                    Ok(w) => V4Forwarded::Reply(w.into_bytes()),
                    Err(_) => V4Forwarded::NotOwner,
                }
            }
            V4Forward::Sequence {
                session,
                slot,
                seqid,
            } => {
                let st = self.st();
                V4Forwarded::Sequenced(match st.sequence(&session, slot, seqid) {
                    Ok(Sequenced::New { client }) => {
                        // It runs elsewhere and isn't cached: done here.
                        st.finish(&session, slot, None);
                        Ok((client, st.slots(&session).unwrap_or(1)))
                    }
                    Ok(Sequenced::Replay(_)) => Err(err::RETRY_UNCACHED_REP),
                    Err(e) => Err(e),
                })
            }
        }
    }

    /// A request on a node that doesn't own the state.
    async fn route(&self, call: &Call, r: &mut Reader<'_>) -> Reply {
        let args = r.rest().to_vec();
        let owner = self.cluster.as_ref().and_then(|c| c.owner());
        let (Some(cl), Some(owner)) = (self.cluster.clone(), owner) else {
            return delay_reply(&args);
        };
        if owner == self.node {
            // Ours by the lease, not taken over yet.
            return delay_reply(&args);
        }
        if let Some((session, seqid, slot)) = pure_data(&args) {
            let req = V4Forward::Sequence {
                session,
                slot,
                seqid,
            };
            return match cl.forward(&owner, &req).await {
                Ok(V4Forwarded::Sequenced(Ok(pre))) => {
                    match self
                        .compound(call, &mut Reader::new(&args), Some(pre))
                        .await
                    {
                        Ok(w) => Reply::Success(w),
                        Err(_) => Reply::GarbageArgs,
                    }
                }
                Ok(V4Forwarded::Sequenced(Err(st))) => fail_reply(&args, st),
                _ => delay_reply(&args),
            };
        }
        let req = V4Forward::Compound {
            args: args.clone(),
            cred: call.cred.clone(),
        };
        match cl.forward(&owner, &req).await {
            Ok(V4Forwarded::Reply(bytes)) => Reply::Success(Writer { buf: bytes }),
            _ => delay_reply(&args),
        }
    }

    /// Server owner and scope: per node until state is shared.
    /// Server owner and scope: the pool's, the same on every node.
    async fn identity(&self) -> Vec<u8> {
        let mut id = b"raidish".to_vec();
        if let Ok(h) = self.backend.handle(ROOT_OBJECT).await {
            id.push(b' ');
            id.extend_from_slice(&h[4..12]);
        }
        id
    }

    pub async fn call(&self, call: &Call, r: &mut Reader<'_>) -> Reply {
        match call.proc_ {
            0 => Reply::Success(Writer::new()),
            1 if !self.is_owner() => self.route(call, r).await,
            1 => match self.compound(call, r, None).await {
                Ok(w) => Reply::Success(w),
                Err(_) => Reply::GarbageArgs,
            },
            _ => Reply::ProcUnavail,
        }
    }

    /// Run a COMPOUND. `pre`: its SEQUENCE was already checked by the
    /// state owner (the client, the slot count).
    async fn compound(
        &self,
        call: &Call,
        r: &mut Reader<'_>,
        pre: Option<(u64, u32)>,
    ) -> XdrResult<Writer> {
        let tag = r.opaque(1024)?.to_vec();
        let minor = r.u32()?;
        let count = r.u32()?;
        let head = |st: u32, n: u32| {
            let mut w = Writer::new();
            w.u32(st).opaque(&tag).u32(n);
            w
        };
        if minor != 1 && minor != 2 {
            return Ok(head(err::MINOR_VERS_MISMATCH, 0));
        }
        let mut ctx = Ctx {
            minor,
            cred: call.cred.clone(),
            ..Default::default()
        };
        let mut results = Writer::new();
        let mut done = 0u32;
        let mut last = err::OK;
        // The slot to finish, and whether to keep the reply.
        let mut slot: Option<([u8; 16], u32, bool)> = None;
        for i in 0..count {
            // Past SEQUENCE, a request that doesn't decode is answered
            // (BADXDR), never dropped: its slot must be finished.
            let Ok(opnum) = r.u32() else {
                results.u32(op::ILLEGAL).u32(err::BADXDR);
                done += 1;
                last = err::BADXDR;
                break;
            };
            let mut w = Writer::new();
            let status = if i == 0 && opnum == op::SEQUENCE {
                match self.sequence(&mut ctx, r, &mut w, count, pre).await? {
                    Seq::Run(st, s) => {
                        slot = s;
                        st
                    }
                    Seq::Replay(bytes) => return Ok(Writer { buf: bytes }),
                }
            } else if i == 0 {
                let alone = [
                    op::EXCHANGE_ID,
                    op::CREATE_SESSION,
                    op::DESTROY_SESSION,
                    op::DESTROY_CLIENTID,
                    op::BIND_CONN_TO_SESSION,
                ];
                if !alone.contains(&opnum) {
                    err::OP_NOT_IN_SESSION
                } else if count > 1 {
                    err::NOT_ONLY_OP
                } else {
                    self.op(&mut ctx, opnum, r, &mut w).await?
                }
            } else if opnum == op::SEQUENCE {
                err::SEQUENCE_POS
            } else {
                match self.op(&mut ctx, opnum, r, &mut w).await {
                    Ok(st) => st,
                    Err(_) => {
                        w = Writer::new();
                        err::BADXDR
                    }
                }
            };
            let reported = if status == err::OP_ILLEGAL {
                op::ILLEGAL
            } else {
                opnum
            };
            results.u32(reported).u32(status);
            if status == err::OK {
                results.buf.extend_from_slice(&w.buf);
            } else if !w.is_empty() {
                // Some results come with failures too (SETATTR's attrsset,
                // LOCK's denial): the op wrote them after its status.
                results.buf.extend_from_slice(&w.buf);
            }
            done += 1;
            last = status;
            if status != err::OK {
                break;
            }
        }
        let mut out = head(last, done);
        out.buf.extend_from_slice(&results.buf);
        if let Some((sid, slotid, cache)) = slot {
            let kept =
                (cache && out.len() <= state::MAX_CACHED_REPLY as usize).then(|| out.buf.clone());
            self.st().finish(&sid, slotid, kept);
        }
        Ok(out)
    }

    async fn sequence(
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

    /// Run one operation (its arguments are next in `r`).
    async fn op(&self, ctx: &mut Ctx, opnum: u32, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
        match opnum {
            op::ACCESS => self.access(ctx, r, w).await,
            op::CLOSE => self.close(ctx, r, w).await,
            op::COMMIT => self.commit(ctx, r, w).await,
            op::CREATE => self.create(ctx, r, w).await,
            op::GETATTR => self.getattr(ctx, r, w).await,
            op::GETFH => self.getfh(ctx, w).await,
            op::LINK => {
                r.string(1024)?;
                Ok(err::NOTSUPP)
            }
            op::LOCK => self.lock(ctx, r, w).await,
            op::LOCKT => self.lockt(ctx, r, w).await,
            op::LOCKU => self.locku(ctx, r, w).await,
            op::LOOKUP => self.lookup(ctx, r).await,
            op::LOOKUPP => self.lookupp(ctx).await,
            op::NVERIFY => self.verify(ctx, r, false).await,
            op::VERIFY => self.verify(ctx, r, true).await,
            op::OPEN => self.open(ctx, r, w).await,
            op::OPEN_DOWNGRADE => self.open_downgrade(ctx, r, w).await,
            op::PUTFH => {
                let fh = r.opaque(128)?;
                Ok(match self.backend.object(fh).await {
                    Ok(id) => {
                        ctx.cfh = Some(id);
                        err::OK
                    }
                    Err(st) => v4(st),
                })
            }
            op::PUTROOTFH | op::PUTPUBFH => {
                ctx.cfh = Some(ROOT_OBJECT);
                Ok(err::OK)
            }
            op::READ => self.read(ctx, r, w).await,
            op::READDIR => self.readdir(ctx, r, w).await,
            op::READLINK => Ok(match ctx.fh() {
                Ok(_) => err::INVAL, // nothing here is a symlink
                Err(e) => e,
            }),
            op::REMOVE => self.remove(ctx, r, w).await,
            op::RENAME => self.rename(ctx, r, w).await,
            op::RESTOREFH => Ok(match ctx.sfh {
                Some(f) => {
                    ctx.cfh = Some(f);
                    ctx.cur = ctx.saved;
                    err::OK
                }
                None => err::RESTOREFH,
            }),
            op::SAVEFH => Ok(match ctx.fh() {
                Ok(f) => {
                    ctx.sfh = Some(f);
                    ctx.saved = ctx.cur;
                    err::OK
                }
                Err(e) => e,
            }),
            op::SECINFO => {
                let name = r.string(1024)?;
                if let Err(e) = ctx.fh().and_then(|_| name_ok(&name)) {
                    return Ok(e);
                }
                ctx.cfh = None;
                secinfo(w);
                Ok(err::OK)
            }
            op::SECINFO_NO_NAME => {
                r.u32()?;
                if let Err(e) = ctx.fh() {
                    return Ok(e);
                }
                ctx.cfh = None;
                secinfo(w);
                Ok(err::OK)
            }
            op::SETATTR => self.setattr(ctx, r, w).await,
            op::WRITE => self.write(ctx, r, w).await,
            op::BACKCHANNEL_CTL => {
                r.u32()?;
                skip_cb_sec(r)?;
                Ok(err::OK)
            }
            op::BIND_CONN_TO_SESSION => {
                let sid: [u8; 16] = r.fixed(16)?.try_into().unwrap();
                r.u32()?;
                r.bool()?;
                if !self.st().session_exists(&sid) {
                    return Ok(err::BADSESSION);
                }
                // The fore channel only: there are no callbacks.
                w.fixed(&sid).u32(1).bool(false);
                Ok(err::OK)
            }
            op::EXCHANGE_ID => self.exchange_id(r, w).await,
            op::CREATE_SESSION => self.create_session(r, w).await,
            op::DESTROY_SESSION => {
                let sid: [u8; 16] = r.fixed(16)?.try_into().unwrap();
                Ok(self.st().destroy_session(&sid).err().unwrap_or(err::OK))
            }
            op::DESTROY_CLIENTID => {
                let id = r.u64()?;
                Ok(match self.st().destroy_clientid(id) {
                    Ok(owner) => {
                        self.record(&owner, false).await;
                        err::OK
                    }
                    Err(e) => e,
                })
            }
            op::FREE_STATEID => {
                let s = read_stateid(r)?;
                let client = match ctx.client() {
                    Ok(c) => c,
                    Err(e) => return Ok(e),
                };
                Ok(self.st().free(client, &s).err().unwrap_or(err::OK))
            }
            op::TEST_STATEID => {
                let n = r.u32()?;
                if n > 1024 {
                    return Err(XdrError("too many stateids"));
                }
                let ids = (0..n)
                    .map(|_| read_stateid(r))
                    .collect::<XdrResult<Vec<_>>>()?;
                let client = match ctx.client() {
                    Ok(c) => c,
                    Err(e) => return Ok(e),
                };
                w.u32(n);
                for s in &ids {
                    w.u32(self.st().test(client, s));
                }
                Ok(err::OK)
            }
            op::RECLAIM_COMPLETE => {
                let one_fs = r.bool()?;
                let client = match ctx.client() {
                    Ok(c) => c,
                    Err(e) => return Ok(e),
                };
                if one_fs {
                    return Ok(err::OK);
                }
                Ok(self.st().reclaim_complete(client).err().unwrap_or(err::OK))
            }
            // Delegations, pNFS, 4.0-only and the rest.
            op::LAYOUTGET => self.layoutget(ctx, r, w).await,
            op::LAYOUTRETURN => self.layoutreturn(ctx, r, w).await,
            op::LAYOUTCOMMIT => self.layoutcommit(ctx, r, w).await,
            op::GETDEVICEINFO => self.getdeviceinfo(r, w).await,
            op::LAYOUTERROR if ctx.minor >= 2 => self.layouterror(r).await,
            op::LAYOUTSTATS if ctx.minor >= 2 => {
                r.u64()?;
                r.u64()?;
                read_stateid(r)?;
                for _ in 0..4 {
                    r.u64()?;
                }
                r.fixed(16)?;
                r.u32()?;
                r.opaque(1 << 16)?;
                Ok(err::OK)
            }
            7 | 8 | 19 | 20 | 30 | 35 | 36 | 39 | 46 | 48 | 54 | 56 => Ok(err::NOTSUPP),
            // NFSv4.2.
            op::SEEK if ctx.minor >= 2 => self.seek(ctx, r, w).await,
            op::DEALLOCATE if ctx.minor >= 2 => self.deallocate(ctx, r).await,
            // ALLOCATE: copy-on-write space can't be set aside for later
            // writes, so preallocating would promise what can't be kept.
            // COPY, CLONE, READ_PLUS and the rest aren't here yet.
            59..=71 if ctx.minor >= 2 => Ok(err::NOTSUPP),
            _ => Ok(err::OP_ILLEGAL),
        }
    }

    /// The nodes as pNFS data servers (none: no pNFS).
    fn devices(&self) -> Vec<Device> {
        self.cluster.as_ref().map_or(Vec::new(), |c| c.devices())
    }

    fn pnfs(&self) -> bool {
        !self.devices().is_empty()
    }

    /// LAYOUTGET: a flexfiles layout of the current file.
    async fn layoutget(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn layoutreturn(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn layoutcommit(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn getdeviceinfo(&self, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn layouterror(&self, r: &mut Reader<'_>) -> OpResult {
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

    /// Check an I/O stateid (unless the owner checked the request).
    fn check_io(&self, ctx: &Ctx, sid: &Stateid, id: FileId, write: bool) -> Result<(), u32> {
        if ctx.trusted {
            return Ok(());
        }
        self.st().check_io(ctx.client()?, sid, id, write)
    }

    // ---- client IDs and sessions ----

    async fn exchange_id(&self, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn create_session(&self, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    // ---- reading ----

    async fn attrs(&self, w: &mut Writer, id: FileId, want: &[u32], minor: u32) -> Result<(), u32> {
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

    async fn getattr(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn getfh(&self, ctx: &mut Ctx, w: &mut Writer) -> OpResult {
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

    async fn lookup(&self, ctx: &mut Ctx, r: &mut Reader<'_>) -> OpResult {
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

    async fn lookupp(&self, ctx: &mut Ctx) -> OpResult {
        let dir = match ctx.fh() {
            Ok(d) => d,
            Err(e) => return Ok(e),
        };
        if dir == ROOT_OBJECT {
            return Ok(err::NOENT);
        }
        Ok(match self.backend.lookup(dir, "..").await {
            Ok(a) => {
                ctx.cfh = Some(a.id);
                err::OK
            }
            Err(st) => v4(st),
        })
    }

    async fn verify(&self, ctx: &mut Ctx, r: &mut Reader<'_>, want_same: bool) -> OpResult {
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

    async fn access(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn read(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn readdir(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    // ---- writing ----

    async fn write(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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
    async fn seek(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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
    async fn deallocate(&self, ctx: &mut Ctx, r: &mut Reader<'_>) -> OpResult {
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

    async fn commit(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn setattr(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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
                let sid = ctx.stateid(sid)?;
                if sid != Stateid::BYPASS {
                    self.check_io(ctx, &sid, id, true)?;
                }
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
    fn owned(&self, mut set: SetAttrs, ctx: &Ctx) -> SetAttrs {
        if let Some(c) = &ctx.cred {
            set.uid.get_or_insert(c.uid);
            set.gid.get_or_insert(c.gid);
        }
        set
    }

    async fn dir_change(&self, dir: FileId) -> u64 {
        self.backend.getattr(dir).await.map_or(0, |a| a.change)
    }

    async fn create(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn remove(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn rename(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
        let from = r.string(1024)?;
        let to = r.string(1024)?;
        let go = async {
            let src = ctx.sfh.ok_or(err::NOFILEHANDLE)?;
            let dst = ctx.fh()?;
            name_ok(&from)?;
            name_ok(&to)?;
            let (sb, db) = (self.dir_change(src).await, self.dir_change(dst).await);
            let replaced = self.backend.lookup(dst, &to).await.ok().map(|a| a.id);
            self.backend
                .rename(src, &from, dst, &to)
                .await
                .map_err(v4)?;
            if let Some(id) = replaced {
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

    // ---- opens ----

    async fn open(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
        let _seqid = r.u32()?;
        let access = r.u32()?;
        let deny = r.u32()?;
        let owner_client = r.u64()?;
        let owner = r.opaque(1024)?.to_vec();
        let now = crate::inode::now_ns();
        // How: (create, guarded, exclusive verifier, attributes).
        let opentype = r.u32()?;
        let mut how: Option<(bool, Option<u64>, Result<attrs::Settable, u32>)> = None;
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
            let (file, cinfo, bitmap) = match claim {
                // CLAIM_NULL: by name in the current directory.
                0 => {
                    let name = name.unwrap();
                    name_ok(&name)?;
                    let before = self.dir_change(here).await;
                    let (id, bm) = match how {
                        None => {
                            let a = self.backend.lookup(here, &name).await.map_err(v4)?;
                            (a.id, Vec::new())
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
                            if let Some(size) = s.size
                                && a.size != size
                            {
                                self.backend
                                    .setattr(a.id, &SetAttrs::default(), Some(size))
                                    .await
                                    .map_err(v4)?;
                            }
                            (a.id, s.bitmap)
                        }
                    };
                    (id, Some((before, self.dir_change(here).await)), bm)
                }
                // CLAIM_FH: the current filehandle itself.
                4 => {
                    if how.is_some() {
                        return Err(err::INVAL);
                    }
                    (here, None, Vec::new())
                }
                // CLAIM_PREVIOUS: a reclaim of the current filehandle.
                1 => {
                    if how.is_some() {
                        return Err(err::INVAL);
                    }
                    (here, None, Vec::new())
                }
                // There are no delegations to claim.
                _ => return Err(err::NOTSUPP),
            };
            let a = self.backend.getattr(file).await.map_err(v4)?;
            if a.kind == ObjType::Directory {
                return Err(err::ISDIR);
            }
            let sid = self.st().open(client, &owner, file, acc, den, claim == 1)?;
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

    async fn open_downgrade(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn close(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    // ---- locks ----

    fn denied(w: &mut Writer, d: &state::Denied) {
        w.u64(d.range.start)
            .u64(d.range.length())
            .u32(if d.write { 2 } else { 1 })
            .u64(d.client)
            .opaque(&d.owner);
    }

    async fn lock(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn lockt(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

    async fn locku(&self, ctx: &mut Ctx, r: &mut Reader<'_>, w: &mut Writer) -> OpResult {
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

/// A reply failing the COMPOUND in `args` with `st` at its first op.
fn fail_reply(args: &[u8], st: u32) -> Reply {
    let mut r = Reader::new(args);
    let (Ok(tag), Ok(_minor), Ok(_count), Ok(first)) = (r.opaque(1024), r.u32(), r.u32(), r.u32())
    else {
        return Reply::GarbageArgs;
    };
    let mut w = Writer::new();
    w.u32(st).opaque(tag).u32(1).u32(first).u32(st);
    Reply::Success(w)
}

/// "Try again" (no state owner reachable right now).
fn delay_reply(args: &[u8]) -> Reply {
    fail_reply(args, err::DELAY)
}

/// If the COMPOUND in `args` only reads or writes data (no state, no
/// namespace change) and doesn't ask for its reply to be cached: its
/// SEQUENCE's session, seqid and slot.
fn pure_data(args: &[u8]) -> Option<([u8; 16], u32, u32)> {
    let mut r = Reader::new(args);
    r.opaque(1024).ok()?;
    if !matches!(r.u32().ok()?, 1 | 2) {
        return None;
    }
    let count = r.u32().ok()?;
    if count == 0 || r.u32().ok()? != op::SEQUENCE {
        return None;
    }
    let session: [u8; 16] = r.fixed(16).ok()?.try_into().ok()?;
    let seqid = r.u32().ok()?;
    let slot = r.u32().ok()?;
    r.u32().ok()?;
    if r.bool().ok()? {
        return None;
    }
    let skip_stateid = |r: &mut Reader<'_>| r.fixed(16).map(|_| ());
    for _ in 1..count {
        let ok = match r.u32().ok()? {
            op::PUTFH => r.opaque(128).map(|_| ()),
            op::PUTROOTFH
            | op::PUTPUBFH
            | op::GETFH
            | op::SAVEFH
            | op::RESTOREFH
            | op::LOOKUPP
            | op::READLINK => Ok(()),
            op::GETATTR => attrs::read_bitmap(&mut r).map(|_| ()),
            op::ACCESS | op::SECINFO_NO_NAME => r.u32().map(|_| ()),
            op::LOOKUP => r.opaque(1024).map(|_| ()),
            op::READ => skip_stateid(&mut r)
                .and_then(|_| r.u64())
                .and_then(|_| r.u32())
                .map(|_| ()),
            op::WRITE => skip_stateid(&mut r)
                .and_then(|_| r.u64())
                .and_then(|_| r.u32())
                .and_then(|_| r.opaque(attrs::MAX_IO as usize))
                .map(|_| ()),
            op::COMMIT => r.u64().and_then(|_| r.u32()).map(|_| ()),
            op::SEEK => skip_stateid(&mut r)
                .and_then(|_| r.u64())
                .and_then(|_| r.u32())
                .map(|_| ()),
            op::DEALLOCATE => skip_stateid(&mut r)
                .and_then(|_| r.u64())
                .and_then(|_| r.u64())
                .map(|_| ()),
            op::READDIR => r
                .u64()
                .and_then(|_| r.u64())
                .and_then(|_| r.u32())
                .and_then(|_| r.u32())
                .and_then(|_| attrs::read_bitmap(&mut r))
                .map(|_| ()),
            op::VERIFY | op::NVERIFY => attrs::read_bitmap(&mut r)
                .and_then(|_| r.opaque(1 << 16))
                .map(|_| ()),
            _ => return None,
        };
        ok.ok()?;
    }
    Some((session, seqid, slot))
}

fn empty_settable() -> attrs::Settable {
    attrs::Settable {
        set: SetAttrs::default(),
        size: None,
        bitmap: Vec::new(),
    }
}

/// What SEQUENCE decided.
enum Seq {
    /// Go on (with SEQUENCE's status); the slot to finish, if any.
    Run(u32, Option<([u8; 16], u32, bool)>),
    /// Send this cached reply again, as is.
    Replay(Vec<u8>),
}

#[cfg(test)]
#[path = "../../tests/nfs4_tests.rs"]
mod tests;
