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
mod ops;
pub mod state;

use crate::backend::{Backend, NfsStat, status as s3};
use crate::disk::{BlockDevice, FileId};
use crate::inode::{ObjType, SetAttrs};
use crate::rpc::{Call, Cred, Reply};
use crate::xdr::{Reader, Writer, XdrError, XdrResult};
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
    /// Just a SEQUENCE check, for a request that runs where it arrived,
    /// and what each stateid its I/O names allows.
    Sequence {
        session: [u8; 16],
        slot: u32,
        seqid: u32,
        stateids: Vec<Stateid>,
    },
}

/// The owner's word on a forwarded request's stateids: each one's file and
/// whether it may write, or why it may not be used.
pub type Grants = Vec<(Stateid, Result<(FileId, bool), NfsStat>)>;

/// A SEQUENCE the state owner checked: the client, the session's slot
/// count, and the grants of the request's stateids.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Presequenced {
    pub client: u64,
    pub slots: u32,
    pub grants: Grants,
}

/// The state owner's answer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum V4Forwarded {
    /// The COMPOUND's reply, as is.
    Reply(Vec<u8>),
    /// The SEQUENCE went through, or its status.
    Sequenced(Result<Presequenced, NfsStat>),
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
    use crate::backend::NfsStat;
    pub const OK: NfsStat = NfsStat::from_code(0);
    pub const NOENT: NfsStat = NfsStat::from_code(2);
    #[cfg(test)]
    pub const NXIO: NfsStat = NfsStat::from_code(6);
    pub const ISDIR: NfsStat = NfsStat::from_code(21);
    pub const INVAL: NfsStat = NfsStat::from_code(22);
    pub const NAMETOOLONG: NfsStat = NfsStat::from_code(63);
    pub const BAD_COOKIE: NfsStat = NfsStat::from_code(10003);
    pub const GRACE: NfsStat = NfsStat::from_code(10013);
    pub const RECLAIM_BAD: NfsStat = NfsStat::from_code(10034);
    pub const NOTSUPP: NfsStat = NfsStat::from_code(10004);
    pub const TOOSMALL: NfsStat = NfsStat::from_code(10005);
    pub const BADTYPE: NfsStat = NfsStat::from_code(10007);
    pub const DELAY: NfsStat = NfsStat::from_code(10008);
    pub const SAME: NfsStat = NfsStat::from_code(10009);
    pub const DENIED: NfsStat = NfsStat::from_code(10010);
    pub const LOCKED: NfsStat = NfsStat::from_code(10012);
    pub const SHARE_DENIED: NfsStat = NfsStat::from_code(10015);
    pub const NOFILEHANDLE: NfsStat = NfsStat::from_code(10020);
    pub const MINOR_VERS_MISMATCH: NfsStat = NfsStat::from_code(10021);
    pub const STALE_CLIENTID: NfsStat = NfsStat::from_code(10022);
    pub const STALE_STATEID: NfsStat = NfsStat::from_code(10023);
    pub const OLD_STATEID: NfsStat = NfsStat::from_code(10024);
    pub const BAD_STATEID: NfsStat = NfsStat::from_code(10025);
    pub const NOT_SAME: NfsStat = NfsStat::from_code(10027);
    pub const RESTOREFH: NfsStat = NfsStat::from_code(10030);
    pub const ATTRNOTSUPP: NfsStat = NfsStat::from_code(10032);
    pub const NO_GRACE: NfsStat = NfsStat::from_code(10033);
    pub const BADXDR: NfsStat = NfsStat::from_code(10036);
    pub const LOCKS_HELD: NfsStat = NfsStat::from_code(10037);
    pub const OPENMODE: NfsStat = NfsStat::from_code(10038);
    pub const BADOWNER: NfsStat = NfsStat::from_code(10039);
    pub const BADNAME: NfsStat = NfsStat::from_code(10041);
    pub const OP_ILLEGAL: NfsStat = NfsStat::from_code(10044);
    pub const BADIOMODE: NfsStat = NfsStat::from_code(10049);
    pub const BADSESSION: NfsStat = NfsStat::from_code(10052);
    pub const LAYOUTUNAVAILABLE: NfsStat = NfsStat::from_code(10059);
    pub const UNKNOWN_LAYOUTTYPE: NfsStat = NfsStat::from_code(10062);
    pub const BADSLOT: NfsStat = NfsStat::from_code(10053);
    pub const COMPLETE_ALREADY: NfsStat = NfsStat::from_code(10054);
    pub const SEQ_MISORDERED: NfsStat = NfsStat::from_code(10063);
    pub const SEQUENCE_POS: NfsStat = NfsStat::from_code(10064);
    pub const RETRY_UNCACHED_REP: NfsStat = NfsStat::from_code(10068);
    pub const TOO_MANY_OPS: NfsStat = NfsStat::from_code(10070);
    pub const OP_NOT_IN_SESSION: NfsStat = NfsStat::from_code(10071);
    pub const CLIENTID_BUSY: NfsStat = NfsStat::from_code(10074);
    pub const NOT_ONLY_OP: NfsStat = NfsStat::from_code(10081);
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
fn v4(st: NfsStat) -> NfsStat {
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
    /// stateids are checked against what it said of them.
    grants: Option<Grants>,
    cfh: Option<FileId>,
    sfh: Option<FileId>,
    cur: Option<Stateid>,
    saved: Option<Stateid>,
    client: Option<u64>,
    cred: Option<Cred>,
}

impl Ctx {
    fn fh(&self) -> Result<FileId, NfsStat> {
        self.cfh.ok_or(err::NOFILEHANDLE)
    }
    fn client(&self) -> Result<u64, NfsStat> {
        self.client.ok_or(err::OP_NOT_IN_SESSION)
    }
    /// A stateid argument, with "the current stateid" resolved.
    fn stateid(&self, s: Stateid) -> Result<Stateid, NfsStat> {
        if s == Stateid::CURRENT {
            return self.cur.ok_or(err::BAD_STATEID);
        }
        Ok(s)
    }
}

/// An operation's outcome: its status, then (if OK) its results in `w`.
type OpResult = Result<NfsStat, XdrError>;

fn read_stateid(r: &mut Reader<'_>) -> XdrResult<Stateid> {
    let seqid = r.u32()?;
    let other = r.fixed(12)?.try_into().unwrap();
    Ok(Stateid { seqid, other })
}

fn write_stateid(w: &mut Writer, s: &Stateid) {
    w.u32(s.seqid).fixed(&s.other);
}

/// `component4`: a name in a directory.
fn name_ok(n: &str) -> Result<(), NfsStat> {
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
                    prog: crate::v3::NFS_PROG,
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
                stateids,
            } => {
                let st = self.st();
                V4Forwarded::Sequenced(match st.sequence(&session, slot, seqid) {
                    Ok(Sequenced::New { client }) => {
                        // It runs elsewhere and isn't cached: done here.
                        st.finish(&session, slot, None);
                        Ok(Presequenced {
                            client,
                            slots: st.slots(&session).unwrap_or(1),
                            grants: stateids
                                .into_iter()
                                .map(|sid| (sid, st.io_grant(client, &sid)))
                                .collect(),
                        })
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
        if let Some((session, seqid, slot, stateids)) = pure_data(&args) {
            let req = V4Forward::Sequence {
                session,
                slot,
                seqid,
                stateids,
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
        if let Ok(root) = self.backend.root().await
            && let Ok(h) = self.backend.handle(root).await
        {
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
        mut pre: Option<Presequenced>,
    ) -> XdrResult<Writer> {
        let tag = r.opaque(1024)?.to_vec();
        let minor = r.u32()?;
        let count = r.u32()?;
        let head = |st: NfsStat, n: u32| {
            let mut w = Writer::new();
            w.stat(st).opaque(&tag).u32(n);
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
                results.u32(op::ILLEGAL).stat(err::BADXDR);
                done += 1;
                last = err::BADXDR;
                break;
            };
            let mut w = Writer::new();
            let status = if i == 0 && opnum == op::SEQUENCE {
                match self
                    .sequence(&mut ctx, r, &mut w, count, pre.take())
                    .await?
                {
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
            results.u32(reported).stat(status);
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
                ctx.cfh = Some(match self.backend.root().await {
                    Ok(r) => r,
                    Err(st) => return Ok(v4(st)),
                });
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
                    w.stat(self.st().test(client, s));
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

    /// Check an I/O stateid: here, or against what the state owner said
    /// of it when it sequenced the request.
    fn check_io(&self, ctx: &Ctx, sid: &Stateid, id: FileId, write: bool) -> Result<(), NfsStat> {
        if let Some(grants) = &ctx.grants {
            // Not one the owner was asked about (a special stateid isn't
            // sent here at all: its share checks need the owner's opens).
            let (_, grant) = grants
                .iter()
                .find(|(s, _)| s == sid)
                .ok_or(err::BAD_STATEID)?;
            let (file, may_write) = (*grant)?;
            if file != id {
                return Err(err::BAD_STATEID);
            }
            if write && !may_write {
                return Err(err::OPENMODE);
            }
            return Ok(());
        }
        self.st().check_io(ctx.client()?, sid, id, write)
    }
}

/// A reply failing the COMPOUND in `args` with `st` at its first op.
fn fail_reply(args: &[u8], st: NfsStat) -> Reply {
    let mut r = Reader::new(args);
    let (Ok(tag), Ok(_minor), Ok(_count), Ok(first)) = (r.opaque(1024), r.u32(), r.u32(), r.u32())
    else {
        return Reply::GarbageArgs;
    };
    let mut w = Writer::new();
    w.stat(st).opaque(tag).u32(1).u32(first).stat(st);
    Reply::Success(w)
}

/// "Try again" (no state owner reachable right now).
fn delay_reply(args: &[u8]) -> Reply {
    fail_reply(args, err::DELAY)
}

/// If the COMPOUND in `args` only reads or writes data (no state, no
/// namespace change) and doesn't ask for its reply to be cached: its
/// SEQUENCE's session, seqid and slot, and the stateids its I/O names
/// (none special: anonymous I/O is checked against the owner's opens, so
/// such a request goes to the owner whole).
fn pure_data(args: &[u8]) -> Option<([u8; 16], u32, u32, Vec<Stateid>)> {
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
    let mut stateids = Vec::new();
    let mut skip_stateid = |r: &mut Reader<'_>| {
        let sid = read_stateid(r)?;
        if sid.is_special() {
            return Err(XdrError("special stateid"));
        }
        if !stateids.contains(&sid) {
            stateids.push(sid);
        }
        Ok(())
    };
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
    Some((session, seqid, slot, stateids))
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
    Run(NfsStat, Option<([u8; 16], u32, bool)>),
    /// Send this cached reply again, as is.
    Replay(Vec<u8>),
}

#[cfg(test)]
#[path = "../tests/nfs4_tests.rs"]
mod tests;
