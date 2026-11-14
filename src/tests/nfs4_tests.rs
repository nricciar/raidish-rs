//! NFSv4.1 over a real socket, with a small client.

use super::attrs::{self, bitmap_of, read_bitmap, write_bitmap};
use super::{err, op};
use crate::fs::FileSystem;
use crate::nfs::backend::Backend;
use crate::nfs::rpc::Client;
use crate::nfs::v3::NFS_PROG;
use crate::nfs::xdr::{Reader, Writer};
use crate::test_util::TestPool;
use crate::vdev::Pool;
use std::path::Path;
use std::sync::Arc;

async fn serve(fs: Arc<FileSystem<Pool>>) -> std::net::SocketAddr {
    let backend = Arc::new(Backend::new(
        Arc::new(move || Some(fs.clone())),
        None,
        "n0".into(),
    ));
    crate::nfs::start("127.0.0.1:0", backend, None)
        .await
        .unwrap()
        .0
}

/// A COMPOUND under construction.
struct Ops {
    w: Writer,
    n: u32,
}

impl Ops {
    fn new() -> Self {
        Ops {
            w: Writer::new(),
            n: 0,
        }
    }
    fn op(mut self, num: u32, args: impl FnOnce(&mut Writer)) -> Self {
        self.w.u32(num);
        args(&mut self.w);
        self.n += 1;
        self
    }
}

/// The results of a COMPOUND: its status, and a reader at the first
/// op's results.
struct Res {
    status: u32,
    count: u32,
    body: Vec<u8>,
}

impl Res {
    fn reader(&self) -> Reader<'_> {
        Reader::new(&self.body)
    }
}

/// Read the next op's number and status; true if OK.
fn next(r: &mut Reader<'_>, want_op: u32) -> u32 {
    assert_eq!(r.u32().unwrap(), want_op, "op number");
    r.u32().unwrap()
}

fn stateid(r: &mut Reader<'_>) -> (u32, [u8; 12]) {
    (r.u32().unwrap(), r.fixed(12).unwrap().try_into().unwrap())
}

fn put_stateid(w: &mut Writer, s: (u32, [u8; 12])) {
    w.u32(s.0).fixed(&s.1);
}

fn skip_sequence(r: &mut Reader<'_>) {
    assert_eq!(next(r, op::SEQUENCE), 0);
    r.fixed(16).unwrap();
    for _ in 0..5 {
        r.u32().unwrap();
    }
}

fn skip_cinfo(r: &mut Reader<'_>) -> (u64, u64) {
    r.bool().unwrap();
    (r.u64().unwrap(), r.u64().unwrap())
}

struct V4 {
    c: Client,
    session: [u8; 16],
    clientid: u64,
    seq: u32,
}

impl V4 {
    async fn raw(c: &mut Client, minor: u32, ops: Ops) -> Res {
        let mut w = Writer::new();
        w.opaque(b"test").u32(minor).u32(ops.n);
        w.buf.extend_from_slice(&ops.w.buf);
        let (st, body) = c.call(NFS_PROG, 4, 1, &w.buf).await;
        assert_eq!(st, 0, "accepted");
        let mut r = Reader::new(&body);
        let status = r.u32().unwrap();
        r.opaque(1024).unwrap();
        let count = r.u32().unwrap();
        let rest = r.fixed(r.remaining()).unwrap().to_vec();
        Res {
            status,
            count,
            body: rest,
        }
    }

    /// EXCHANGE_ID + CREATE_SESSION + RECLAIM_COMPLETE as `owner`.
    async fn connect(addr: std::net::SocketAddr, owner: &str) -> V4 {
        let mut v = Self::session(addr, owner).await;
        let res = v
            .call(Ops::new().op(op::RECLAIM_COMPLETE, |w| {
                w.bool(false);
            }))
            .await;
        assert_eq!(res.status, 0);
        v
    }

    /// EXCHANGE_ID + CREATE_SESSION (no RECLAIM_COMPLETE yet).
    async fn session(addr: std::net::SocketAddr, owner: &str) -> V4 {
        let mut c = Client::connect(addr).await;
        let ops = Ops::new().op(op::EXCHANGE_ID, |w| {
            w.u64(7).opaque(owner.as_bytes()).u32(0).u32(0).u32(0);
        });
        let res = Self::raw(&mut c, 1, ops).await;
        assert_eq!(res.status, 0);
        let mut r = res.reader();
        assert_eq!(next(&mut r, op::EXCHANGE_ID), 0);
        let clientid = r.u64().unwrap();
        let seq = r.u32().unwrap();
        let flags = r.u32().unwrap();
        assert_eq!(flags & 0x8000_0000, 0, "not confirmed yet");
        let ops = Ops::new().op(op::CREATE_SESSION, |w| {
            w.u64(clientid).u32(seq).u32(0);
            for _ in 0..2 {
                w.u32(0)
                    .u32(1 << 20)
                    .u32(1 << 20)
                    .u32(4096)
                    .u32(16)
                    .u32(8)
                    .u32(0);
            }
            w.u32(0x40000000).u32(1).u32(0); // cb program, AUTH_NONE
        });
        let res = Self::raw(&mut c, 1, ops).await;
        assert_eq!(res.status, 0);
        let mut r = res.reader();
        assert_eq!(next(&mut r, op::CREATE_SESSION), 0);
        let session: [u8; 16] = r.fixed(16).unwrap().try_into().unwrap();
        assert_eq!(r.u32().unwrap(), seq);
        V4 {
            c,
            session,
            clientid,
            seq: 0,
        }
    }

    /// SEQUENCE (slot 0, the next seqid) + `ops`.
    async fn call(&mut self, ops: Ops) -> Res {
        self.seq += 1;
        self.call_seq(self.seq, true, ops).await
    }

    async fn call_seq(&mut self, seqid: u32, cache: bool, ops: Ops) -> Res {
        self.call_minor(1, seqid, cache, ops).await
    }

    async fn call_minor(&mut self, minor: u32, seqid: u32, cache: bool, ops: Ops) -> Res {
        let mut all = Ops::new().op(op::SEQUENCE, |w| {
            w.fixed(&self.session).u32(seqid).u32(0).u32(0).bool(cache);
        });
        all.w.buf.extend_from_slice(&ops.w.buf);
        all.n += ops.n;
        Self::raw(&mut self.c, minor, all).await
    }

    /// SEQUENCE + `ops` as NFSv4.2.
    async fn call42(&mut self, ops: Ops) -> Res {
        self.seq += 1;
        self.call_minor(2, self.seq, true, ops).await
    }
}

fn putroot(o: Ops) -> Ops {
    o.op(op::PUTROOTFH, |_| {})
}

fn getattr(o: Ops, attrs: &[u32]) -> Ops {
    let bm = bitmap_of(attrs);
    o.op(op::GETATTR, move |w| write_bitmap(w, &bm))
}

/// OPEN with CREATE (UNCHECKED, mode) or not, by name, for read+write.
fn open(o: Ops, clientid: u64, owner: &str, name: &str, create: bool, deny: u32) -> Ops {
    let owner = owner.to_string();
    let name = name.to_string();
    o.op(op::OPEN, move |w| {
        w.u32(0)
            .u32(3)
            .u32(deny)
            .u64(clientid)
            .opaque(owner.as_bytes());
        if create {
            w.u32(1).u32(0);
            let mut vals = Writer::new();
            vals.u32(0o640);
            write_bitmap(w, &bitmap_of(&[attrs::MODE]));
            w.opaque(&vals.buf);
        } else {
            w.u32(0);
        }
        w.u32(0).string(&name);
    })
}

/// Read OPEN's results: the stateid.
fn opened(r: &mut Reader<'_>) -> (u32, [u8; 12]) {
    assert_eq!(next(r, op::OPEN), 0);
    let s = stateid(r);
    skip_cinfo(r);
    r.u32().unwrap(); // rflags
    read_bitmap(r).unwrap();
    assert_eq!(r.u32().unwrap(), 0, "no delegation");
    s
}

#[tokio::test]
async fn sessions_and_their_rules() {
    let pool = TestPool::new().await;
    let addr = serve(Arc::new(pool.format().await)).await;
    let mut v = V4::connect(addr, "client-a").await;

    // The root's attributes.
    let res = v
        .call(getattr(
            putroot(Ops::new()),
            &[
                attrs::TYPE,
                attrs::CHANGE,
                attrs::SIZE,
                attrs::FSID,
                attrs::LEASE_TIME,
                attrs::FILEID,
            ],
        ))
        .await;
    assert_eq!((res.status, res.count), (0, 3));
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTROOTFH), 0);
    assert_eq!(next(&mut r, op::GETATTR), 0);
    assert_eq!(
        read_bitmap(&mut r).unwrap(),
        bitmap_of(&[
            attrs::TYPE,
            attrs::CHANGE,
            attrs::SIZE,
            attrs::FSID,
            attrs::LEASE_TIME,
            attrs::FILEID
        ])
    );
    let vals = r.opaque(4096).unwrap();
    let mut a = Reader::new(vals);
    assert_eq!(a.u32().unwrap(), 2, "a directory");
    a.u64().unwrap(); // change
    a.u64().unwrap(); // size
    a.u64().unwrap();
    a.u64().unwrap(); // fsid
    assert_eq!(a.u32().unwrap(), super::state::LEASE_SECS);
    assert_eq!(a.u64().unwrap(), crate::inode::ROOT_OBJECT);

    // Replays: the cached reply, byte for byte; uncached ones refused.
    let ask = || getattr(putroot(Ops::new()), &[attrs::CHANGE]);
    let first = v.call_seq(v.seq + 1, true, ask()).await;
    let again = v.call_seq(v.seq + 1, true, ask()).await;
    assert_eq!(first.body, again.body);
    v.seq += 1;
    let _ = v.call_seq(v.seq + 1, false, ask()).await;
    v.seq += 1;
    let res = v.call_seq(v.seq, false, ask()).await;
    assert_eq!(res.status, err::RETRY_UNCACHED_REP);
    let res = v.call_seq(v.seq + 5, true, ask()).await;
    assert_eq!(res.status, err::SEQ_MISORDERED);

    // Without SEQUENCE; SEQUENCE in the wrong place; a session op with
    // company; an operation that doesn't exist; another minor version.
    let res = V4::raw(&mut v.c, 1, ask()).await;
    assert_eq!(res.status, err::OP_NOT_IN_SESSION);
    let res = v
        .call(Ops::new().op(op::SEQUENCE, |w| {
            w.fixed(&[0; 16]).u32(1).u32(0).u32(0).bool(false);
        }))
        .await;
    assert_eq!(res.status, err::SEQUENCE_POS);
    let two = Ops::new()
        .op(op::DESTROY_SESSION, |w| {
            w.fixed(&[0; 16]);
        })
        .op(op::PUTROOTFH, |_| {});
    assert_eq!(V4::raw(&mut v.c, 1, two).await.status, err::NOT_ONLY_OP);
    let res = v.call(Ops::new().op(9999, |_| {})).await;
    assert_eq!(res.status, err::OP_ILLEGAL);
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(r.u32().unwrap(), op::ILLEGAL);
    assert_eq!(
        V4::raw(&mut v.c, 0, ask()).await.status,
        err::MINOR_VERS_MISMATCH
    );

    // RECLAIM_COMPLETE once; reclaims get no grace (none is kept).
    let res = v
        .call(Ops::new().op(op::RECLAIM_COMPLETE, |w| {
            w.bool(false);
        }))
        .await;
    assert_eq!(res.status, err::COMPLETE_ALREADY);

    // Tear down: the client ID is busy while it has a session.
    let id = v.clientid;
    let destroy_client = |id: u64| {
        Ops::new().op(op::DESTROY_CLIENTID, move |w| {
            w.u64(id);
        })
    };
    assert_eq!(
        V4::raw(&mut v.c, 1, destroy_client(id)).await.status,
        err::CLIENTID_BUSY
    );
    let sid = v.session;
    let res = V4::raw(
        &mut v.c,
        1,
        Ops::new().op(op::DESTROY_SESSION, move |w| {
            w.fixed(&sid);
        }),
    )
    .await;
    assert_eq!(res.status, 0);
    assert_eq!(V4::raw(&mut v.c, 1, destroy_client(id)).await.status, 0);
}

#[tokio::test]
async fn files_from_open_to_close() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let addr = serve(fs.clone()).await;
    let mut v = V4::connect(addr, "client-a").await;
    let cid = v.clientid;

    // mkdir d; create d/f; write; read; commit; close.
    let res = v
        .call(
            putroot(Ops::new())
                .op(op::CREATE, |w| {
                    w.u32(2).string("d");
                    write_bitmap(w, &[]);
                    w.opaque(&[]);
                })
                .op(op::GETFH, |_| {}),
        )
        .await;
    assert_eq!(res.status, 0);
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTROOTFH), 0);
    assert_eq!(next(&mut r, op::CREATE), 0);
    let (before, after) = skip_cinfo(&mut r);
    assert!(after > before, "the root changed");
    read_bitmap(&mut r).unwrap();
    assert_eq!(next(&mut r, op::GETFH), 0);
    let dir = r.opaque(128).unwrap().to_vec();

    let data = b"hello over nfs4".to_vec();
    let dir2 = dir.clone();
    let res = v
        .call(
            open(
                Ops::new().op(op::PUTFH, move |w| {
                    w.opaque(&dir2);
                }),
                cid,
                "owner1",
                "f",
                true,
                0,
            )
            .op(op::GETFH, |_| {})
            .op(op::WRITE, {
                let data = data.clone();
                move |w| {
                    // The current stateid: the one OPEN just made.
                    put_stateid(w, (1, [0; 12]));
                    w.u64(0).u32(0).opaque(&data);
                }
            }),
        )
        .await;
    assert_eq!(res.status, 0, "open+write");
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTFH), 0);
    let sid = opened(&mut r);
    assert_eq!(next(&mut r, op::GETFH), 0);
    let f = r.opaque(128).unwrap().to_vec();
    assert_eq!(next(&mut r, op::WRITE), 0);
    assert_eq!(r.u32().unwrap() as usize, data.len());
    assert_eq!(r.u32().unwrap(), 0, "unstable");
    let verf = r.fixed(8).unwrap().to_vec();

    let f2 = f.clone();
    let res = v
        .call(
            Ops::new()
                .op(op::PUTFH, move |w| {
                    w.opaque(&f2);
                })
                .op(op::READ, move |w| {
                    put_stateid(w, sid);
                    w.u64(6).u32(100);
                })
                .op(op::COMMIT, |w| {
                    w.u64(0).u32(0);
                })
                .op(op::CLOSE, move |w| {
                    w.u32(0);
                    put_stateid(w, sid);
                }),
        )
        .await;
    assert_eq!(res.status, 0, "read+commit+close");
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTFH), 0);
    assert_eq!(next(&mut r, op::READ), 0);
    assert!(r.bool().unwrap(), "eof");
    assert_eq!(r.opaque(1024).unwrap(), &data[6..]);
    assert_eq!(next(&mut r, op::COMMIT), 0);
    assert_eq!(r.fixed(8).unwrap(), verf.as_slice());
    assert_eq!(fs.read_file(Path::new("d/f")).await.unwrap(), data);

    // The closed stateid is no good any more.
    let f3 = f.clone();
    let res = v
        .call(
            Ops::new()
                .op(op::PUTFH, move |w| {
                    w.opaque(&f3);
                })
                .op(op::READ, move |w| {
                    put_stateid(w, sid);
                    w.u64(0).u32(1);
                }),
        )
        .await;
    assert_eq!(res.status, err::BAD_STATEID);

    // READDIR, then rename (SAVEFH) and remove; and v3 sees the same handle.
    let dir3 = dir.clone();
    let res = v
        .call(
            Ops::new()
                .op(op::PUTFH, move |w| {
                    w.opaque(&dir3);
                })
                .op(op::READDIR, |w| {
                    w.u64(0).u64(0).u32(4096).u32(8192);
                    write_bitmap(w, &bitmap_of(&[attrs::FILEID, attrs::SIZE]));
                }),
        )
        .await;
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTFH), 0);
    assert_eq!(next(&mut r, op::READDIR), 0);
    r.u64().unwrap();
    assert!(r.bool().unwrap());
    assert_eq!(r.u64().unwrap(), 3, "first cookie");
    assert_eq!(r.string(255).unwrap(), "f");
    let bm = read_bitmap(&mut r).unwrap();
    assert_eq!(bm, bitmap_of(&[attrs::SIZE, attrs::FILEID]));
    let vals = r.opaque(1024).unwrap();
    let mut a = Reader::new(vals);
    assert_eq!(a.u64().unwrap(), data.len() as u64);
    assert!(!r.bool().unwrap());
    assert!(r.bool().unwrap(), "eof");

    let dir4 = dir.clone();
    let res = v
        .call(
            putroot(Ops::new())
                .op(op::SAVEFH, |_| {})
                .op(op::PUTFH, move |w| {
                    w.opaque(&dir4);
                })
                .op(op::RESTOREFH, |_| {})
                .op(op::LOOKUP, |w| {
                    w.string("d");
                })
                .op(op::SAVEFH, |_| {})
                .op(op::RENAME, |w| {
                    w.string("f").string("g");
                })
                .op(op::LOOKUP, |w| {
                    w.string("g");
                })
                .op(op::GETFH, |_| {}),
        )
        .await;
    assert_eq!(res.status, 0, "rename");
    assert_eq!(fs.read_file(Path::new("d/g")).await.unwrap(), data);
    let id = fs.file_id(Path::new("d/g")).await.unwrap();
    let backend = Backend::new(
        {
            let fs = fs.clone();
            Arc::new(move || Some(fs.clone()))
        },
        None,
        "x".into(),
    );
    assert_eq!(
        backend.handle(id).await.unwrap(),
        f,
        "v3 and v4 share handles"
    );

    let dir5 = dir.clone();
    let res = v
        .call(
            Ops::new()
                .op(op::PUTFH, move |w| {
                    w.opaque(&dir5);
                })
                .op(op::REMOVE, |w| {
                    w.string("g");
                }),
        )
        .await;
    assert_eq!(res.status, 0);
    // Names that aren't names.
    let res = v
        .call(putroot(Ops::new()).op(op::LOOKUP, |w| {
            w.string("..");
        }))
        .await;
    assert_eq!(res.status, err::BADNAME);
    drop(v);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Share reservations and byte-range locks between two clients.
#[tokio::test]
async fn opens_and_locks_between_clients() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.write_file(Path::new("f"), b"0123456789").await.unwrap();
    let addr = serve(fs.clone()).await;
    let mut a = V4::connect(addr, "client-a").await;
    let mut b = V4::connect(addr, "client-b").await;

    // a opens denying writes; b can't open for writing.
    let res = a
        .call(open(putroot(Ops::new()), a.clientid, "oa", "f", false, 2))
        .await;
    assert_eq!(res.status, 0);
    let mut r = res.reader();
    skip_sequence(&mut r);
    assert_eq!(next(&mut r, op::PUTROOTFH), 0);
    let sa = opened(&mut r);
    let res = b
        .call(open(putroot(Ops::new()), b.clientid, "ob", "f", false, 0))
        .await;
    assert_eq!(res.status, err::SHARE_DENIED);

    // a locks bytes 0..5 for writing.
    let lock = |o: Ops, cid: u64, open: (u32, [u8; 12]), owner: &str, off: u64, len: u64| {
        let owner = owner.to_string();
        o.op(op::LOCK, move |w| {
            w.u32(2).bool(false).u64(off).u64(len).bool(true);
            w.u32(0);
            put_stateid(w, open);
            w.u32(0).u64(cid).opaque(owner.as_bytes());
        })
    };
    let res = a
        .call(lock(
            putroot(Ops::new()).op(op::LOOKUP, |w| {
                w.string("f");
            }),
            a.clientid,
            sa,
            "la",
            0,
            5,
        ))
        .await;
    assert_eq!(res.status, 0, "a locks");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    next(&mut r, op::LOOKUP);
    assert_eq!(next(&mut r, op::LOCK), 0);
    let la = stateid(&mut r);

    // b opens for reading, and LOCKT/LOCK see a's lock.
    let res = b
        .call({
            let o = putroot(Ops::new());
            o.op(op::OPEN, move |w| {
                w.u32(0).u32(1).u32(0).u64(b.clientid).opaque(b"ob");
                w.u32(0).u32(0).string("f");
            })
        })
        .await;
    assert_eq!(res.status, 0, "b reads");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    let sb = opened(&mut r);
    let bid = b.clientid;
    let res = b
        .call(
            putroot(Ops::new())
                .op(op::LOOKUP, |w| {
                    w.string("f");
                })
                .op(op::LOCKT, move |w| {
                    w.u32(1).u64(3).u64(1).u64(bid).opaque(b"lb");
                }),
        )
        .await;
    assert_eq!(res.status, err::DENIED);
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    next(&mut r, op::LOOKUP);
    assert_eq!(next(&mut r, op::LOCKT), err::DENIED);
    assert_eq!(
        (r.u64().unwrap(), r.u64().unwrap(), r.u32().unwrap()),
        (0, 5, 2)
    );
    assert_eq!(r.u64().unwrap(), a.clientid);
    assert_eq!(r.opaque(1024).unwrap(), b"la");
    let res = b
        .call(lock(
            putroot(Ops::new()).op(op::LOOKUP, |w| {
                w.string("f");
            }),
            bid,
            sb,
            "lb",
            5,
            u64::MAX,
        ))
        .await;
    assert_eq!(res.status, 0, "past a's range is free");

    // a can't close while holding its lock; unlocks, then closes.
    let close = |o: Ops, s: (u32, [u8; 12])| {
        o.op(op::CLOSE, move |w| {
            w.u32(0);
            put_stateid(w, (0, s.1));
        })
    };
    let lookup_f = || {
        putroot(Ops::new()).op(op::LOOKUP, |w| {
            w.string("f");
        })
    };
    assert_eq!(a.call(close(lookup_f(), sa)).await.status, err::LOCKS_HELD);
    let res = a
        .call(lookup_f().op(op::LOCKU, move |w| {
            w.u32(2).u32(0);
            put_stateid(w, la);
            w.u64(0).u64(5);
        }))
        .await;
    assert_eq!(res.status, 0);
    assert_eq!(a.call(close(lookup_f(), sa)).await.status, 0);
    // Now b may open for writing.
    let res = b
        .call(open(putroot(Ops::new()), bid, "ob2", "f", false, 0))
        .await;
    assert_eq!(res.status, 0);
}

// ---- the state owner and the other nodes ----

use super::{Server, V4Cluster, V4Forward, V4Forwarded};

/// The cluster in this process: who owns the state, the servers by node,
/// the client records.
#[derive(Default)]
struct TestCluster {
    owner: std::sync::Mutex<Option<String>>,
    servers: std::sync::Mutex<std::collections::HashMap<String, Arc<Server<Pool>>>>,
    records: std::sync::Mutex<std::collections::BTreeSet<Vec<u8>>>,
    locations: Vec<String>,
    devices: std::sync::Mutex<Vec<super::Device>>,
}

#[async_trait::async_trait]
impl V4Cluster for TestCluster {
    fn owner(&self) -> Option<String> {
        self.owner.lock().unwrap().clone()
    }
    async fn forward(&self, node: &str, req: &V4Forward) -> Result<V4Forwarded, String> {
        let s = self.servers.lock().unwrap().get(node).cloned();
        match s {
            Some(s) => Ok(s.serve_forward(req.clone()).await),
            None => Err(format!("no node {}", node)),
        }
    }
    fn locations(&self) -> Vec<String> {
        self.locations.clone()
    }
    fn devices(&self) -> Vec<super::Device> {
        self.devices.lock().unwrap().clone()
    }
    async fn record(&self, client: &[u8], present: bool) -> Result<(), String> {
        let mut r = self.records.lock().unwrap();
        if present {
            r.insert(client.to_vec());
        } else {
            r.remove(client);
        }
        Ok(())
    }
}

impl TestCluster {
    /// Make `node` the state owner (the other, a follower).
    fn hand_to(&self, node: &str) {
        let records: Vec<Vec<u8>> = self.records.lock().unwrap().iter().cloned().collect();
        for (n, s) in self.servers.lock().unwrap().iter() {
            if n == node {
                s.become_owner(records.clone());
            } else {
                s.become_follower();
            }
        }
        *self.owner.lock().unwrap() = Some(node.into());
    }
}

/// Two nodes' NFS servers over one pool, `a` owning the state.
async fn two_nodes(
    fs: Arc<FileSystem<Pool>>,
) -> (Arc<TestCluster>, std::net::SocketAddr, std::net::SocketAddr) {
    let cluster = Arc::new(TestCluster {
        locations: vec!["node-a.example".into(), "10.0.0.2".into()],
        ..Default::default()
    });
    let mut addrs = Vec::new();
    for node in ["a", "b"] {
        let fs = fs.clone();
        let backend = Arc::new(Backend::new(
            Arc::new(move || Some(fs.clone())),
            None,
            node.into(),
        ));
        let (addr, server) = crate::nfs::start(
            "127.0.0.1:0",
            backend,
            Some(cluster.clone() as Arc<dyn V4Cluster>),
        )
        .await
        .unwrap();
        cluster.servers.lock().unwrap().insert(node.into(), server);
        addrs.push(addr);
    }
    cluster.hand_to("a");
    (cluster, addrs[0], addrs[1])
}

/// LOCK by a new lock owner under `open`; the status (and the stateid).
fn lock_op(o: Ops, cid: u64, open: (u32, [u8; 12]), owner: &str, reclaim: bool) -> Ops {
    let owner = owner.to_string();
    o.op(op::LOCK, move |w| {
        w.u32(2).bool(reclaim).u64(0).u64(100).bool(true);
        w.u32(0);
        put_stateid(w, open);
        w.u32(0).u64(cid).opaque(owner.as_bytes());
    })
}

fn put_file(fh: &[u8]) -> Ops {
    let fh = fh.to_vec();
    Ops::new().op(op::PUTFH, move |w| {
        w.opaque(&fh);
    })
}

/// A client on a node that doesn't own the state: its sessions, opens and
/// locks live on the owner (so they hold against clients of every node),
/// and its plain reads and writes run where it is.
#[tokio::test]
async fn state_lives_on_the_owner() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.write_file(Path::new("f"), b"0123456789").await.unwrap();
    let (cluster, on_a, on_b) = two_nodes(fs.clone()).await;
    let mut x = V4::connect(on_b, "client-x").await;
    let mut y = V4::connect(on_a, "client-y").await;
    assert_eq!(cluster.records.lock().unwrap().len(), 2, "both recorded");

    // x opens and locks through b.
    let res = x
        .call(open(putroot(Ops::new()), x.clientid, "ox", "f", false, 0).op(op::GETFH, |_| {}))
        .await;
    assert_eq!(res.status, 0, "open through b");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    let sx = opened(&mut r);
    assert_eq!(next(&mut r, op::GETFH), 0);
    let fh = r.opaque(128).unwrap().to_vec();
    let res = x
        .call(lock_op(put_file(&fh), x.clientid, sx, "lx", false))
        .await;
    assert_eq!(res.status, 0, "lock through b");

    // y, on a, sees it.
    let yid = y.clientid;
    let res = y
        .call(put_file(&fh).op(op::LOCKT, move |w| {
            w.u32(2).u64(10).u64(1).u64(yid).opaque(b"ly");
        }))
        .await;
    assert_eq!(res.status, err::DENIED, "x's lock holds on a too");

    // A plain write (not cached) runs on b; its SEQUENCE is the owner's.
    x.seq += 1;
    let seq = x.seq;
    let res = x
        .call_seq(
            seq,
            false,
            put_file(&fh).op(op::WRITE, move |w| {
                put_stateid(w, sx);
                w.u64(0).u32(2).opaque(b"AB");
            }),
        )
        .await;
    assert_eq!(res.status, 0, "write on b");
    assert_eq!(fs.read_file(Path::new("f")).await.unwrap(), b"AB23456789");
    // ... and counts: the next seqid goes on from it.
    assert_eq!(x.call(putroot(Ops::new())).await.status, 0);
}

/// The owner changes: sessions are gone, recorded clients reclaim their
/// opens and locks in the grace period, new ones wait (GRACE) until they
/// have, and the reclaimed locks hold after.
#[tokio::test]
async fn a_new_owner_lets_clients_reclaim() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.write_file(Path::new("f"), b"0123456789").await.unwrap();
    let (cluster, on_a, on_b) = two_nodes(fs.clone()).await;
    let mut x = V4::connect(on_b, "client-x").await;
    let y0 = V4::connect(on_a, "client-y").await;
    drop(y0);
    let res = x
        .call(open(putroot(Ops::new()), x.clientid, "ox", "f", false, 0).op(op::GETFH, |_| {}))
        .await;
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    let sx = opened(&mut r);
    next(&mut r, op::GETFH);
    let fh = r.opaque(128).unwrap().to_vec();
    assert_eq!(
        x.call(lock_op(put_file(&fh), x.clientid, sx, "lx", false))
            .await
            .status,
        0
    );

    // a goes away; b owns the state now.
    cluster.hand_to("b");
    assert_eq!(x.call(putroot(Ops::new())).await.status, err::BADSESSION);

    // x comes back and reclaims (CLAIM_PREVIOUS, then a reclaim LOCK).
    let mut x = V4::session(on_b, "client-x").await;
    let xid = x.clientid;
    let res = x
        .call(put_file(&fh).op(op::OPEN, move |w| {
            w.u32(0).u32(3).u32(0).u64(xid).opaque(b"ox");
            w.u32(0).u32(1).u32(0); // no create; CLAIM_PREVIOUS, no delegation
        }))
        .await;
    assert_eq!(res.status, 0, "reclaim open");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTFH);
    let sx = opened(&mut r);
    assert_eq!(
        x.call(lock_op(put_file(&fh), xid, sx, "lx", true))
            .await
            .status,
        0,
        "reclaim lock"
    );

    // y (through a, now a follower) may not take new state yet...
    let mut y = V4::session(on_a, "client-y").await;
    let res = y
        .call(open(putroot(Ops::new()), y.clientid, "oy", "f", false, 0))
        .await;
    assert_eq!(res.status, err::GRACE);
    // ... and a client the old owner never knew may not reclaim.
    let mut z = V4::session(on_a, "client-z").await;
    let zid = z.clientid;
    let res = z
        .call(put_file(&fh).op(op::OPEN, move |w| {
            w.u32(0).u32(1).u32(0).u64(zid).opaque(b"oz");
            w.u32(0).u32(1).u32(0);
        }))
        .await;
    assert_eq!(res.status, err::RECLAIM_BAD);

    // Everyone that held state is done: the grace period is over.
    for v in [&mut x, &mut y] {
        let res = v
            .call(Ops::new().op(op::RECLAIM_COMPLETE, |w| {
                w.bool(false);
            }))
            .await;
        assert_eq!(res.status, 0);
    }
    let res = y
        .call(open(putroot(Ops::new()), y.clientid, "oy", "f", false, 0))
        .await;
    assert_eq!(res.status, 0, "new opens after grace");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    let sy = opened(&mut r);
    let res = y
        .call(lock_op(put_file(&fh), y.clientid, sy, "ly", false))
        .await;
    assert_eq!(res.status, err::DENIED, "x's reclaimed lock holds");
}

/// EXCHANGE_ID through `addr`: (clientid, server owner major id, scope).
async fn exchange(addr: std::net::SocketAddr, owner: &str) -> (u64, Vec<u8>, Vec<u8>) {
    let mut c = Client::connect(addr).await;
    let ops = Ops::new().op(op::EXCHANGE_ID, |w| {
        w.u64(7).opaque(owner.as_bytes()).u32(0).u32(0).u32(0);
    });
    let res = V4::raw(&mut c, 1, ops).await;
    assert_eq!(res.status, 0);
    let mut r = res.reader();
    assert_eq!(next(&mut r, op::EXCHANGE_ID), 0);
    let clientid = r.u64().unwrap();
    r.u32().unwrap();
    r.u32().unwrap();
    assert_eq!(r.u32().unwrap(), 0, "SP4_NONE");
    assert_eq!(r.u64().unwrap(), 0, "minor id");
    let major = r.opaque(1024).unwrap().to_vec();
    let scope = r.opaque(1024).unwrap().to_vec();
    (clientid, major, scope)
}

/// Every node is the same server: the same identity from each, and one
/// session works over connections to any of them (session trunking).
#[tokio::test]
async fn every_node_is_the_same_server() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let (_cluster, on_a, on_b) = two_nodes(fs).await;
    let mut x = V4::connect(on_b, "client-x").await;
    let (ida, major_a, scope_a) = exchange(on_a, "client-x").await;
    let (idb, major_b, scope_b) = exchange(on_b, "client-x").await;
    assert_eq!((major_a.clone(), scope_a), (major_b, scope_b));
    assert_eq!(
        (ida, idb),
        (x.clientid, x.clientid),
        "the same client, confirmed"
    );

    // The session made through b, used through a, then b again.
    let sid = x.session;
    x.c = Client::connect(on_a).await;
    let res = V4::raw(
        &mut x.c,
        1,
        Ops::new().op(op::BIND_CONN_TO_SESSION, move |w| {
            w.fixed(&sid).u32(3).bool(false);
        }),
    )
    .await;
    assert_eq!(res.status, 0, "bind a connection to a on the session");
    for addr in [on_a, on_b, on_a] {
        x.c = Client::connect(addr).await;
        let res = x
            .call(getattr(putroot(Ops::new()), &[attrs::FS_LOCATIONS]))
            .await;
        assert_eq!(res.status, 0, "through {}", addr);
        let mut r = res.reader();
        skip_sequence(&mut r);
        next(&mut r, op::PUTROOTFH);
        assert_eq!(next(&mut r, op::GETATTR), 0);
        assert_eq!(
            read_bitmap(&mut r).unwrap(),
            bitmap_of(&[attrs::FS_LOCATIONS])
        );
        let vals = r.opaque(4096).unwrap();
        let mut v = Reader::new(vals);
        assert_eq!(v.u32().unwrap(), 0, "fs_root: the root");
        assert_eq!(v.u32().unwrap(), 1, "one location");
        assert_eq!(v.u32().unwrap(), 2, "two addresses");
        assert_eq!(v.string(255).unwrap(), "node-a.example");
        assert_eq!(v.string(255).unwrap(), "10.0.0.2");
        assert_eq!(v.u32().unwrap(), 0, "rootpath: the root");
    }
}

// ---- NFSv4.2 ----

fn seek_op(o: Ops, offset: u64, data: bool) -> Ops {
    o.op(op::SEEK, move |w| {
        put_stateid(w, (0, [0; 12]));
        w.u64(offset).u32(if data { 0 } else { 1 });
    })
}

/// The SEEK result: (eof, offset), or the status.
fn seek_result(res: &Res) -> Result<(bool, u64), u32> {
    if res.status != 0 {
        return Err(res.status);
    }
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTFH);
    assert_eq!(next(&mut r, op::SEEK), 0);
    Ok((r.bool().unwrap(), r.u64().unwrap()))
}

/// SEEK finds data and holes by record; DEALLOCATE punches one (and frees
/// it); both only in 4.2.
#[tokio::test]
async fn seek_and_punch_holes() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let bs = fs.file_blksz() as u64;
    // data | hole | data
    fs.write_file(Path::new("sparse"), b"").await.unwrap();
    let f = fs.open_file(Path::new("sparse"), None).await.unwrap();
    fs.file_write(&f, 0, &vec![1u8; bs as usize]).await.unwrap();
    fs.file_write(&f, 2 * bs, &vec![2u8; bs as usize])
        .await
        .unwrap();
    fs.close_file(&f).await.unwrap();
    let size = 3 * bs;
    let addr = serve(fs.clone()).await;
    let mut v = V4::connect(addr, "client-a").await;
    let res = v
        .call(
            putroot(Ops::new())
                .op(op::LOOKUP, |w| {
                    w.string("sparse");
                })
                .op(op::GETFH, |_| {}),
        )
        .await;
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    next(&mut r, op::LOOKUP);
    next(&mut r, op::GETFH);
    let fh = r.opaque(128).unwrap().to_vec();

    for (from, data, want) in [
        (0, true, (false, 0)),
        (0, false, (false, bs)),
        (10, false, (false, bs)),
        (bs + 5, true, (false, 2 * bs)),
        (bs + 5, false, (false, bs + 5)),
        (2 * bs, false, (true, size)),
    ] {
        let res = v.call42(seek_op(put_file(&fh), from, data)).await;
        assert_eq!(seek_result(&res), Ok(want), "from {} data {}", from, data);
    }
    let res = v.call42(seek_op(put_file(&fh), size, true)).await;
    assert_eq!(seek_result(&res), Err(err::NXIO), "past the end");

    // Punch the first record: data now starts at the third, the size stays,
    // and the record's space is given back.
    let res = v
        .call42(put_file(&fh).op(op::DEALLOCATE, move |w| {
            put_stateid(w, (0, [0; 12]));
            w.u64(0).u64(bs);
        }))
        .await;
    assert_eq!(res.status, 0, "deallocate");
    let res = v.call42(seek_op(put_file(&fh), 0, true)).await;
    assert_eq!(seek_result(&res), Ok((false, 2 * bs)));
    let (_, d) = fs.stat_file(Path::new("sparse")).await.unwrap();
    assert_eq!(d.size, size);
    assert_eq!(fs.tree_leaves(&d.tree).await.unwrap().len(), 1);
    let data = fs.read_file(Path::new("sparse")).await.unwrap();
    assert!(data[..2 * bs as usize].iter().all(|&b| b == 0));
    assert!(data[2 * bs as usize..].iter().all(|&b| b == 2));

    // 4.2 operations aren't 4.1's; ALLOCATE isn't offered.
    let res = v.call(seek_op(put_file(&fh), 0, true)).await;
    assert_eq!(res.status, err::OP_ILLEGAL);
    let res = v
        .call42(put_file(&fh).op(59, move |w| {
            put_stateid(w, (0, [0; 12]));
            w.u64(0).u64(bs);
        }))
        .await;
    assert_eq!(res.status, err::NOTSUPP, "ALLOCATE");

    // change_attr_type is a 4.2 attribute.
    for (minor, has) in [(1, false), (2, true)] {
        v.seq += 1;
        let res = v
            .call_minor(
                minor,
                v.seq,
                true,
                getattr(put_file(&fh), &[attrs::SUPPORTED_ATTRS]),
            )
            .await;
        let mut r = res.reader();
        skip_sequence(&mut r);
        next(&mut r, op::PUTFH);
        next(&mut r, op::GETATTR);
        read_bitmap(&mut r).unwrap();
        let vals = r.opaque(1024).unwrap();
        let bm = read_bitmap(&mut Reader::new(vals)).unwrap();
        assert_eq!(
            attrs::has(&bm, attrs::CHANGE_ATTR_TYPE),
            has,
            "minor {}",
            minor
        );
    }
    drop(v);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

// ---- pNFS ----

/// Read layouts list every node; their data servers are the nodes' NFSv3
/// servers, which read the file by the same handle.
#[tokio::test]
async fn flexfiles_layouts_point_at_every_node() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.write_file(Path::new("f"), b"pnfs data").await.unwrap();
    fs.set_attrs(
        Path::new("f"),
        &crate::inode::SetAttrs {
            uid: Some(1000),
            gid: Some(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (cluster, on_a, on_b) = two_nodes(fs.clone()).await;
    *cluster.devices.lock().unwrap() = [(0, "a", on_a), (1, "b", on_b)]
        .iter()
        .map(|(id, node, addr)| super::Device {
            id: *id,
            node: node.to_string(),
            addr: addr.ip().to_string(),
            port: addr.port(),
        })
        .collect();

    // A metadata server now (USE_PNFS_MDS).
    let mut c = Client::connect(on_b).await;
    let ops = Ops::new().op(op::EXCHANGE_ID, |w| {
        w.u64(7).opaque(b"probe").u32(0).u32(0).u32(0);
    });
    let res = V4::raw(&mut c, 1, ops).await;
    let mut r = res.reader();
    next(&mut r, op::EXCHANGE_ID);
    r.u64().unwrap();
    r.u32().unwrap();
    assert_eq!(r.u32().unwrap() & 0x0007_0000, 0x0002_0000, "pNFS MDS");

    let mut x = V4::connect(on_b, "client-x").await;
    let res = x
        .call(open(putroot(Ops::new()), x.clientid, "ox", "f", false, 0).op(op::GETFH, |_| {}))
        .await;
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTROOTFH);
    let sx = opened(&mut r);
    next(&mut r, op::GETFH);
    let fh = r.opaque(128).unwrap().to_vec();

    let layoutget = |iomode: u32, sid: (u32, [u8; 12])| {
        put_file(&fh).op(op::LAYOUTGET, move |w| {
            w.bool(false).u32(super::LAYOUT4_FLEX_FILES).u32(iomode);
            w.u64(0).u64(u64::MAX).u64(0);
            put_stateid(w, sid);
            w.u32(1 << 16);
        })
    };
    // Read layout: two mirrors, one data server each, the same handle.
    let res = x.call(layoutget(1, sx)).await;
    assert_eq!(res.status, 0, "layoutget");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTFH);
    assert_eq!(next(&mut r, op::LAYOUTGET), 0);
    assert!(r.bool().unwrap(), "returned at close");
    let lsid = stateid(&mut r);
    assert_eq!(r.u32().unwrap(), 1);
    assert_eq!(
        (r.u64().unwrap(), r.u64().unwrap(), r.u32().unwrap()),
        (0, u64::MAX, 1)
    );
    assert_eq!(r.u32().unwrap(), super::LAYOUT4_FLEX_FILES);
    let body = r.opaque(4096).unwrap().to_vec();
    let mut b = Reader::new(&body);
    assert_eq!(b.u64().unwrap(), 0, "no striping");
    assert_eq!(b.u32().unwrap(), 2, "a mirror per node");
    let mut devices = Vec::new();
    for _ in 0..2 {
        assert_eq!(b.u32().unwrap(), 1);
        devices.push(b.fixed(16).unwrap().to_vec());
        b.u32().unwrap();
        stateid(&mut b);
        assert_eq!(b.u32().unwrap(), 1);
        assert_eq!(b.opaque(128).unwrap(), fh.as_slice());
        assert_eq!(
            (b.string(64).unwrap(), b.string(64).unwrap()),
            ("1000".into(), "100".into())
        );
    }

    // Node b's device: its address; its NFSv3 server reads the file.
    let dev = devices[1].clone();
    let res = x
        .call(Ops::new().op(op::GETDEVICEINFO, move |w| {
            w.fixed(&dev).u32(super::LAYOUT4_FLEX_FILES).u32(4096);
            write_bitmap(w, &[]);
        }))
        .await;
    assert_eq!(res.status, 0, "getdeviceinfo");
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::GETDEVICEINFO);
    assert_eq!(r.u32().unwrap(), super::LAYOUT4_FLEX_FILES);
    let body = r.opaque(4096).unwrap().to_vec();
    let mut b = Reader::new(&body);
    assert_eq!(b.u32().unwrap(), 1);
    assert_eq!(b.string(16).unwrap(), "tcp");
    let ua = b.string(64).unwrap();
    let port = on_b.port();
    assert_eq!(ua, format!("{}.{}.{}", on_b.ip(), port >> 8, port & 0xff));
    assert_eq!(b.u32().unwrap(), 1);
    assert_eq!((b.u32().unwrap(), b.u32().unwrap()), (3, 0), "NFSv3");
    let mut ds = Client::connect(on_b).await;
    let mut args = Writer::new();
    args.opaque(&fh).u64(0).u32(100);
    let (st, reply) = ds.call(crate::nfs::v3::NFS_PROG, 3, 6, &args.buf).await;
    assert_eq!(st, 0);
    let mut r = Reader::new(&reply);
    assert_eq!(r.u32().unwrap(), 0, "v3 READ ok");
    if r.bool().unwrap() {
        r.fixed(84).unwrap();
    }
    r.u32().unwrap();
    r.bool().unwrap();
    assert_eq!(r.opaque(100).unwrap(), b"pnfs data");

    // A read-write layout: one mirror (the node writing it, or the owner).
    let res = x.call(layoutget(2, lsid)).await;
    assert_eq!(res.status, 0);
    let mut r = res.reader();
    skip_sequence(&mut r);
    next(&mut r, op::PUTFH);
    next(&mut r, op::LAYOUTGET);
    r.bool().unwrap();
    stateid(&mut r);
    r.u32().unwrap();
    r.u64().unwrap();
    r.u64().unwrap();
    assert_eq!(r.u32().unwrap(), 2, "rw");
    r.u32().unwrap();
    let body = r.opaque(4096).unwrap().to_vec();
    let mut b = Reader::new(&body);
    b.u64().unwrap();
    assert_eq!(b.u32().unwrap(), 1, "one mirror");

    // CLOSE hands the layout back.
    let cid = x.clientid;
    let res = x
        .call(put_file(&fh).op(op::CLOSE, move |w| {
            w.u32(0);
            put_stateid(w, (0, sx.1));
        }))
        .await;
    assert_eq!(res.status, 0);
    let owner = cluster.servers.lock().unwrap()["a"].clone();
    let id = fs.file_id(Path::new("f")).await.unwrap();
    assert!(!owner.st().has_layout(cid, id));
    // Layouts on a non-cluster server: unavailable.
    *cluster.devices.lock().unwrap() = Vec::new();
    let res = x.call(layoutget(1, sx)).await;
    assert_eq!(res.status, err::LAYOUTUNAVAILABLE);
}
