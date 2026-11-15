//! ONC RPC version 2 (RFC 5531) over TCP, with record marking.
//!
//! Each connection reads calls as they come and runs them concurrently (a
//! client keeps many requests in flight on one connection); replies go
//! back in whatever order they finish, matched by `xid`.

use super::xdr::{Reader, Writer, XdrError};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Largest record accepted (a 1 MiB WRITE plus its headers, with room).
pub const MAX_RECORD: usize = 4 << 20;

pub const AUTH_NONE: u32 = 0;
pub const AUTH_SYS: u32 = 1;

/// Who's calling, as `AUTH_SYS` claims (trusted as is, like any NFSv3
/// server without Kerberos).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cred {
    pub uid: u32,
    pub gid: u32,
    pub gids: Vec<u32>,
    pub machine: String,
}

#[derive(Debug, Clone)]
pub struct Call {
    pub xid: u32,
    pub prog: u32,
    pub vers: u32,
    pub proc_: u32,
    /// None: `AUTH_NONE` (or a flavor we don't read).
    pub cred: Option<Cred>,
}

/// How a call was answered.
pub enum Reply {
    /// The procedure's results.
    Success(Writer),
    ProgUnavail,
    /// The program, but not this version: the versions it has.
    ProgMismatch(u32, u32),
    ProcUnavail,
    GarbageArgs,
}

impl From<XdrError> for Reply {
    fn from(_: XdrError) -> Self {
        Reply::GarbageArgs
    }
}

/// The programs a server runs.
#[async_trait]
pub trait Dispatch: Send + Sync + 'static {
    async fn call(&self, call: &Call, args: &mut Reader<'_>) -> Reply;
}

/// Parse a call's header and credentials; the arguments follow in `r`.
pub fn parse_call(r: &mut Reader<'_>) -> Result<Call, (u32, &'static str)> {
    let xid = r.u32().map_err(|_| (0, "no xid"))?;
    let bad = |m| (xid, m);
    if r.u32().map_err(|_| bad("no type"))? != 0 {
        return Err(bad("not a call"));
    }
    let rpcvers = r.u32().map_err(|_| bad("no rpc version"))?;
    if rpcvers != 2 {
        return Err(bad("rpc version"));
    }
    let prog = r.u32().map_err(|_| bad("no program"))?;
    let vers = r.u32().map_err(|_| bad("no version"))?;
    let proc_ = r.u32().map_err(|_| bad("no procedure"))?;
    let flavor = r.u32().map_err(|_| bad("no cred"))?;
    let body = r.opaque(400).map_err(|_| bad("cred"))?;
    let cred = if flavor == AUTH_SYS {
        let mut b = Reader::new(body);
        let parse = |b: &mut Reader<'_>| -> Result<Cred, XdrError> {
            let _stamp = b.u32()?;
            let machine = b.string(255)?;
            let uid = b.u32()?;
            let gid = b.u32()?;
            let n = b.u32()? as usize;
            if n > 16 {
                return Err(XdrError("too many groups"));
            }
            let gids = (0..n).map(|_| b.u32()).collect::<Result<_, _>>()?;
            Ok(Cred {
                uid,
                gid,
                gids,
                machine,
            })
        };
        Some(parse(&mut b).map_err(|_| bad("auth_sys"))?)
    } else {
        None
    };
    let _verf_flavor = r.u32().map_err(|_| bad("no verifier"))?;
    r.opaque(400).map_err(|_| bad("verifier"))?;
    Ok(Call {
        xid,
        prog,
        vers,
        proc_,
        cred,
    })
}

/// A reply message (without record marking).
pub fn encode_reply(xid: u32, reply: Reply) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(xid).u32(1); // REPLY
    w.u32(0); // MSG_ACCEPTED
    w.u32(AUTH_NONE).opaque(&[]);
    match reply {
        Reply::Success(body) => {
            w.u32(0);
            w.buf.extend_from_slice(&body.buf);
        }
        Reply::ProgUnavail => {
            w.u32(1);
        }
        Reply::ProgMismatch(lo, hi) => {
            w.u32(2).u32(lo).u32(hi);
        }
        Reply::ProcUnavail => {
            w.u32(3);
        }
        Reply::GarbageArgs => {
            w.u32(4);
        }
    }
    w.into_bytes()
}

/// A denied reply: we only speak RPC version 2.
fn encode_rpc_mismatch(xid: u32) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(xid).u32(1).u32(1); // REPLY, MSG_DENIED
    w.u32(0).u32(2).u32(2); // RPC_MISMATCH, 2..2
    w.into_bytes()
}

/// Read one record (all its fragments). None: the connection closed.
pub async fn read_record<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    loop {
        let mut head = [0u8; 4];
        match r.read_exact(&mut head).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && out.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        let h = u32::from_be_bytes(head);
        let len = (h & 0x7fff_ffff) as usize;
        if out.len() + len > MAX_RECORD {
            return Err(std::io::Error::other("RPC record too large"));
        }
        let at = out.len();
        out.resize(at + len, 0);
        r.read_exact(&mut out[at..]).await?;
        if h & 0x8000_0000 != 0 {
            return Ok(Some(out));
        }
    }
}

/// `msg` as one record (a single last fragment).
pub fn frame(msg: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + msg.len());
    out.extend_from_slice(&(0x8000_0000u32 | msg.len() as u32).to_be_bytes());
    out.extend_from_slice(msg);
    out
}

/// Serve `dispatch` on `listener` until the task is dropped.
pub async fn serve(listener: TcpListener, dispatch: Arc<dyn Dispatch>) {
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("nfs: accept: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = sock.set_nodelay(true);
        let dispatch = dispatch.clone();
        tokio::spawn(async move {
            if let Err(e) = connection(sock, dispatch).await {
                eprintln!("nfs: {}: {}", peer, e);
            }
        });
    }
}

async fn connection(
    sock: tokio::net::TcpStream,
    dispatch: Arc<dyn Dispatch>,
) -> std::io::Result<()> {
    let (mut rd, mut wr) = sock.into_split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if wr.write_all(&frame(&msg)).await.is_err() {
                break;
            }
        }
    });
    let result = loop {
        let record = match read_record(&mut rd).await {
            Ok(Some(r)) => r,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        };
        let (dispatch, tx) = (dispatch.clone(), tx.clone());
        tokio::spawn(async move {
            let mut r = Reader::new(&record);
            let reply = match parse_call(&mut r) {
                Ok(call) => {
                    let reply = dispatch.call(&call, &mut r).await;
                    encode_reply(call.xid, reply)
                }
                Err((xid, "rpc version")) => encode_rpc_mismatch(xid),
                Err((xid, _)) => encode_reply(xid, Reply::GarbageArgs),
            };
            let _ = tx.send(reply).await;
        });
    };
    drop(tx);
    let _ = writer.await;
    result
}

/// A minimal client, for tests (and the curious): one call at a time.
#[cfg(test)]
pub struct Client {
    sock: tokio::net::TcpStream,
    xid: u32,
    pub cred: Cred,
}

#[cfg(test)]
impl Client {
    pub async fn connect(addr: std::net::SocketAddr) -> Self {
        Client {
            sock: tokio::net::TcpStream::connect(addr).await.unwrap(),
            xid: 1,
            cred: Cred {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
                machine: "test".into(),
            },
        }
    }

    /// Call and return the accept status and the results.
    pub async fn call(&mut self, prog: u32, vers: u32, proc_: u32, args: &[u8]) -> (u32, Vec<u8>) {
        self.xid += 1;
        let mut w = Writer::new();
        w.u32(self.xid).u32(0).u32(2).u32(prog).u32(vers).u32(proc_);
        let mut cred = Writer::new();
        cred.u32(0)
            .string(&self.cred.machine)
            .u32(self.cred.uid)
            .u32(self.cred.gid);
        cred.u32(self.cred.gids.len() as u32);
        for g in &self.cred.gids {
            cred.u32(*g);
        }
        w.u32(AUTH_SYS).opaque(&cred.buf);
        w.u32(AUTH_NONE).opaque(&[]);
        w.buf.extend_from_slice(args);
        self.sock.write_all(&frame(&w.buf)).await.unwrap();
        let rec = read_record(&mut self.sock).await.unwrap().unwrap();
        let mut r = Reader::new(&rec);
        assert_eq!(r.u32().unwrap(), self.xid);
        assert_eq!(r.u32().unwrap(), 1, "a reply");
        assert_eq!(r.u32().unwrap(), 0, "accepted");
        r.u32().unwrap();
        r.opaque(400).unwrap();
        let stat = r.u32().unwrap();
        let n = r.remaining();
        let rest = r.fixed(n).unwrap().to_vec();
        (stat, rest)
    }
}
