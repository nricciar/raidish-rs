//! Drives the NVMe/TCP target over real sockets with a minimal host that
//! follows the same sequence as the Linux initiator.

use super::*;
use crate::export::BlockExporter;
use crate::test_util::DEFAULT_VOLBLOCKSIZE;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::vdev::Pool;
use raidish_core::role::{Committer, CommitterFs};

const MB: u64 = 1 << 20;
const HOSTNQN: &str = "nqn.2024-01.com.raidish:host:unit";
/// Subsystem of the volume `serve` exports.
const VOL_NQN: &str = "nqn.2024-01.com.raidish:nvme:vol";

struct Rsp {
    dw0: u32,
    dw1: u32,
    status: u16,
    data: Vec<u8>,
}

impl Rsp {
    fn sc(&self) -> u16 {
        (self.status >> 1) & 0xff
    }
    fn sct(&self) -> u16 {
        (self.status >> 9) & 0x7
    }
    fn ok(&self) -> bool {
        self.status >> 1 == 0
    }
}

struct Host {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    next_cid: u16,
    maxh2cdata: u32,
}

fn sqe(opcode: u8, cid: u16, nsid: u32) -> [u8; 64] {
    let mut s = [0u8; 64];
    s[0] = opcode;
    s[2..4].copy_from_slice(&cid.to_le_bytes());
    s[4..8].copy_from_slice(&nsid.to_le_bytes());
    s
}
fn set_cdw(s: &mut [u8; 64], n: usize, v: u32) {
    s[40 + (n - 10) * 4..44 + (n - 10) * 4].copy_from_slice(&v.to_le_bytes());
}
/// Transport SGL data block (data via R2T / C2H).
fn sgl_transport(s: &mut [u8; 64], len: usize) {
    s[32..36].copy_from_slice(&(len as u32).to_le_bytes());
    s[39] = 0x5a;
}
/// In-capsule (offset) SGL data block.
fn sgl_inline(s: &mut [u8; 64], len: usize) {
    s[32..36].copy_from_slice(&(len as u32).to_le_bytes());
    s[39] = 0x01;
}

impl Host {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = stream.into_split();
        let mut ic = [0u8; 128];
        ic[0] = PDU_IC_REQ;
        ic[2] = 128;
        ic[4..8].copy_from_slice(&128u32.to_le_bytes());
        wr.write_all(&ic).await.unwrap();
        let resp = read_pdu(&mut rd).await.unwrap();
        assert_eq!(resp[0], PDU_IC_RESP);
        assert_eq!(resp.len(), 128);
        assert_eq!(resp[10], 0, "cpda");
        assert_eq!(resp[11], 0, "no digests");
        let maxh2cdata = le32(&resp, 12);
        Host {
            rd,
            wr,
            next_cid: 1,
            maxh2cdata,
        }
    }

    fn cid(&mut self) -> u16 {
        self.next_cid += 1;
        self.next_cid
    }

    async fn send_capsule(&mut self, s: &[u8; 64], inline: &[u8]) {
        let mut pdu = vec![0u8; 72];
        pdu[0] = PDU_CMD;
        pdu[2] = 72;
        pdu[3] = if inline.is_empty() { 0 } else { 72 };
        pdu[8..72].copy_from_slice(s);
        pdu.extend_from_slice(inline);
        let plen = pdu.len() as u32;
        pdu[4..8].copy_from_slice(&plen.to_le_bytes());
        self.wr.write_all(&pdu).await.unwrap();
    }

    /// Read PDUs for `cid` until its response arrives, answering R2Ts with `h2c`.
    async fn wait(&mut self, cid: u16, h2c: &[u8]) -> Rsp {
        let mut data = Vec::new();
        loop {
            let pdu = read_pdu(&mut self.rd).await.unwrap();
            match pdu[0] {
                PDU_C2H_DATA => {
                    assert_eq!(le16(&pdu, 8), cid);
                    let datao = le32(&pdu, 12) as usize;
                    let datal = le32(&pdu, 16) as usize;
                    let pdo = pdu[3] as usize;
                    assert_eq!(datao, data.len(), "C2H data in order");
                    data.extend_from_slice(&pdu[pdo..pdo + datal]);
                }
                PDU_R2T => {
                    assert_eq!(le16(&pdu, 8), cid);
                    let ttag = le16(&pdu, 10);
                    let r2to = le32(&pdu, 12) as usize;
                    let r2tl = le32(&pdu, 16) as usize;
                    assert_eq!(r2to + r2tl, h2c.len());
                    // Split into maxh2cdata-sized H2C PDUs, like the kernel does.
                    let mut off = r2to;
                    while off < r2to + r2tl {
                        let n = (self.maxh2cdata as usize).min(r2to + r2tl - off);
                        let mut h = vec![0u8; 24];
                        h[0] = PDU_H2C_DATA;
                        h[1] = if off + n == r2to + r2tl {
                            PDU_FLAG_LAST
                        } else {
                            0
                        };
                        h[2] = 24;
                        h[3] = 24;
                        h[4..8].copy_from_slice(&((24 + n) as u32).to_le_bytes());
                        h[8..10].copy_from_slice(&cid.to_le_bytes());
                        h[10..12].copy_from_slice(&ttag.to_le_bytes());
                        h[12..16].copy_from_slice(&(off as u32).to_le_bytes());
                        h[16..20].copy_from_slice(&(n as u32).to_le_bytes());
                        h.extend_from_slice(&h2c[off..off + n]);
                        self.wr.write_all(&h).await.unwrap();
                        off += n;
                    }
                }
                PDU_RSP => {
                    assert_eq!(le16(&pdu, 20), cid, "response for our command");
                    return Rsp {
                        dw0: le32(&pdu, 8),
                        dw1: le32(&pdu, 12),
                        status: le16(&pdu, 22),
                        data,
                    };
                }
                t => panic!("unexpected PDU {t:#x}"),
            }
        }
    }

    async fn fabrics_connect(&mut self, qid: u16, cntlid: u16, subnqn: &str) -> Rsp {
        let cid = self.send_connect(qid, cntlid, subnqn).await;
        self.wait(cid, &[]).await
    }

    /// Send a Connect without waiting for its answer; its cid.
    async fn send_connect(&mut self, qid: u16, cntlid: u16, subnqn: &str) -> u16 {
        let cid = self.cid();
        let mut s = sqe(OPC_FABRICS, cid, 0);
        s[4] = FCT_CONNECT;
        s[42..44].copy_from_slice(&qid.to_le_bytes());
        s[44..46].copy_from_slice(&31u16.to_le_bytes()); // sqsize 32
        s[48..52].copy_from_slice(&5000u32.to_le_bytes());
        let mut data = vec![0u8; 1024];
        data[16..18].copy_from_slice(&cntlid.to_le_bytes());
        data[256..256 + subnqn.len()].copy_from_slice(subnqn.as_bytes());
        data[512..512 + HOSTNQN.len()].copy_from_slice(HOSTNQN.as_bytes());
        sgl_inline(&mut s, data.len());
        self.send_capsule(&s, &data).await;
        cid
    }

    /// Send an I/O command without waiting for its answer: data in the
    /// capsule (`inline`), or `len` bytes asked for by R2T. Its cid.
    async fn send_io(&mut self, opcode: u8, slba: u64, nlb: u32, len: usize, inline: &[u8]) -> u16 {
        let cid = self.cid();
        let mut s = sqe(opcode, cid, 1);
        set_cdw(&mut s, 10, slba as u32);
        set_cdw(&mut s, 11, (slba >> 32) as u32);
        set_cdw(&mut s, 12, nlb.saturating_sub(1));
        if inline.is_empty() {
            sgl_transport(&mut s, len);
        } else {
            sgl_inline(&mut s, inline.len());
        }
        self.send_capsule(&s, inline).await;
        cid
    }

    /// Send one H2C data PDU.
    async fn send_h2c(&mut self, cid: u16, ttag: u16, off: usize, data: &[u8], last: bool) {
        let mut h = vec![0u8; 24];
        h[0] = PDU_H2C_DATA;
        h[1] = if last { PDU_FLAG_LAST } else { 0 };
        h[2] = 24;
        h[3] = 24;
        h[4..8].copy_from_slice(&((24 + data.len()) as u32).to_le_bytes());
        h[8..10].copy_from_slice(&cid.to_le_bytes());
        h[10..12].copy_from_slice(&ttag.to_le_bytes());
        h[12..16].copy_from_slice(&(off as u32).to_le_bytes());
        h[16..20].copy_from_slice(&(data.len() as u32).to_le_bytes());
        h.extend_from_slice(data);
        self.wr.write_all(&h).await.unwrap();
    }

    async fn prop_get(&mut self, offset: u32, eight: bool) -> u64 {
        let cid = self.cid();
        let mut s = sqe(OPC_FABRICS, cid, 0);
        s[4] = FCT_PROP_GET;
        s[40] = eight as u8;
        s[44..48].copy_from_slice(&offset.to_le_bytes());
        self.send_capsule(&s, &[]).await;
        let r = self.wait(cid, &[]).await;
        assert!(r.ok());
        r.dw0 as u64 | (r.dw1 as u64) << 32
    }

    async fn prop_set(&mut self, offset: u32, value: u32) {
        let cid = self.cid();
        let mut s = sqe(OPC_FABRICS, cid, 0);
        s[4] = FCT_PROP_SET;
        s[44..48].copy_from_slice(&offset.to_le_bytes());
        s[48..52].copy_from_slice(&value.to_le_bytes());
        self.send_capsule(&s, &[]).await;
        assert!(self.wait(cid, &[]).await.ok());
    }

    async fn admin(&mut self, opcode: u8, nsid: u32, cdw10: u32, cdw11: u32, len: usize) -> Rsp {
        let cid = self.cid();
        let mut s = sqe(opcode, cid, nsid);
        set_cdw(&mut s, 10, cdw10);
        set_cdw(&mut s, 11, cdw11);
        sgl_transport(&mut s, len);
        self.send_capsule(&s, &[]).await;
        self.wait(cid, &[]).await
    }

    /// Full admin bring-up of `vol`; returns the controller id.
    async fn bring_up_admin(&mut self, io_queues: u16) -> u16 {
        self.bring_up_admin_of(VOL_NQN, io_queues).await
    }

    /// Full admin bring-up of subsystem `nqn`; returns the controller id.
    async fn bring_up_admin_of(&mut self, nqn: &str, io_queues: u16) -> u16 {
        let r = self.fabrics_connect(0, 0xffff, nqn).await;
        assert!(r.ok(), "admin connect status {:#x}", r.status);
        let cntlid = r.dw0 as u16;

        let cap = self.prop_get(PROP_CAP, true).await;
        assert_eq!(cap & 0xffff, QUEUE_ENTRIES as u64 - 1);
        assert_eq!(self.prop_get(PROP_VS, false).await as u32, NVME_VERSION);
        self.prop_set(PROP_CC, 0x0046_0001).await; // IOCQES=4, IOSQES=6, EN
        assert_eq!(self.prop_get(PROP_CSTS, false).await & 1, 1, "CSTS.RDY");

        let n = (io_queues - 1) as u32;
        let r = self.admin(ADM_SET_FEATURES, 0, 0x07, n << 16 | n, 0).await;
        assert!(r.ok());
        assert_eq!(r.dw0 & 0xffff, n);
        cntlid
    }

    async fn io(
        &mut self,
        opcode: u8,
        slba: u64,
        nlb: u32,
        h2c: &[u8],
        c2h_len: usize,
        inline: bool,
    ) -> Rsp {
        let cid = self.cid();
        let mut s = sqe(opcode, cid, 1);
        set_cdw(&mut s, 10, slba as u32);
        set_cdw(&mut s, 11, (slba >> 32) as u32);
        set_cdw(&mut s, 12, nlb.saturating_sub(1));
        let len = if h2c.is_empty() { c2h_len } else { h2c.len() };
        if inline {
            sgl_inline(&mut s, len);
            self.send_capsule(&s, h2c).await;
        } else {
            sgl_transport(&mut s, len);
            self.send_capsule(&s, &[]).await;
        }
        self.wait(cid, h2c).await
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 5) as u8).wrapping_mul(7).wrapping_add(seed))
        .collect()
}

/// Export `volumes` (name, size) from one target on an ephemeral port.
/// The exports `serve_many` made, by volume: each is up while held.
type Held = std::collections::BTreeMap<String, crate::export::ExportHandle>;

async fn serve_many(
    pool: &TestPool,
    volumes: &[(&str, u64)],
) -> (
    Arc<CommitterFs<Pool>>,
    Arc<NvmeTcpServer<Pool, Committer>>,
    std::net::SocketAddr,
    Held,
) {
    let fs = Arc::new(pool.format().await);
    let server = Arc::new(NvmeTcpServer::with_bind(fs.clone(), DEFAULT_BIND, 0));
    let mut held = Held::new();
    for &(name, size) in volumes {
        let name = &ds(name);
        fs.create_block(name, size, DEFAULT_VOLBLOCKSIZE)
            .await
            .unwrap();
        held.insert(name.to_string(), server.add_export(name).await.unwrap());
    }
    let listener = TcpListener::bind((DEFAULT_BIND, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = server.clone();
    tokio::spawn(async move { s.serve(listener).await });
    (fs, server, addr, held)
}

async fn serve(pool: &TestPool, size: u64) -> (Arc<CommitterFs<Pool>>, std::net::SocketAddr) {
    let fs = Arc::new(pool.format().await);
    fs.create_block(&ds("vol"), size, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    // The single-export constructor the CLI uses.
    let server = NvmeTcpServer::new(fs.clone(), ds("vol"), 0).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { server.serve(listener).await });
    (fs, addr)
}

#[test]
fn nqns_and_serials() {
    assert_eq!(subsystem_nqn("vol"), VOL_NQN);
    // The NQN the kernel test script connects to is unchanged.
    assert_eq!(
        subsystem_nqn("vdisk0"),
        "nqn.2024-01.com.raidish:nvme:vdisk0"
    );
    assert_eq!(serial_of("vdisk0"), "RAIDISH-vdisk0");
    // Long names that would truncate to the same SN get distinct hashes.
    let (a, b) = (
        serial_of("database-volume-1"),
        serial_of("database-volume-2"),
    );
    assert_ne!(a, b);
    assert!(a.len() <= 20);
}

#[tokio::test]
async fn two_volumes_on_one_target() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) =
        serve_many(&pool, &[("alpha", 16 * MB), ("beta", 32 * MB)]).await;
    assert_eq!(server.exports(), vec!["alpha", "beta"]);
    let (alpha, beta) = (subsystem_nqn("alpha"), subsystem_nqn("beta"));

    let mut admin_a = Host::connect(addr).await;
    let cntlid_a = admin_a.bring_up_admin_of(&alpha, 1).await;
    let mut admin_b = Host::connect(addr).await;
    let cntlid_b = admin_b.bring_up_admin_of(&beta, 1).await;
    assert_ne!(cntlid_a, cntlid_b);

    // Identify reports each subsystem's own NQN, serial and size.
    for (h, nqn, sn, size) in [
        (&mut admin_a, &alpha, "RAIDISH-alpha", 16 * MB),
        (&mut admin_b, &beta, "RAIDISH-beta", 32 * MB),
    ] {
        let id = h.admin(ADM_IDENTIFY, 0, 0x01, 0, 4096).await.data;
        assert_eq!(cstr(&id[768..1024]), *nqn);
        assert_eq!(String::from_utf8_lossy(&id[4..24]).trim_end(), sn);
        let ns = h.admin(ADM_IDENTIFY, 1, 0x00, 0, 4096).await;
        assert_eq!(le64(&ns.data, 0), size / LBA_SIZE, "NSZE");
    }

    // An I/O queue can only join a controller of the subsystem it names.
    let mut h = Host::connect(addr).await;
    let r = h.fabrics_connect(1, cntlid_a, &beta).await;
    assert_eq!((r.sct(), r.sc()), (SCT_FABRICS, SC_CONNECT_INVALID_PARAM));
    assert_eq!(r.dw0, 1 << 16 | 16, "points at CNTLID");

    // Unknown subsystems are refused.
    let mut h = Host::connect(addr).await;
    let r = h.fabrics_connect(0, 0xffff, &subsystem_nqn("gamma")).await;
    assert_eq!((r.sct(), r.sc()), (SCT_FABRICS, SC_CONNECT_INVALID_PARAM));
    assert_eq!(r.dw0, 1 << 16 | 256, "points at SUBNQN");

    // Each queue reaches its own volume.
    let mut qa = Host::connect(addr).await;
    assert!(qa.fabrics_connect(1, cntlid_a, &alpha).await.ok());
    let mut qb = Host::connect(addr).await;
    assert!(qb.fabrics_connect(1, cntlid_b, &beta).await.ok());
    let (da, db) = (pattern(8192, 1), pattern(8192, 2));
    assert!(qa.io(IO_WRITE, 3, 2, &da, 0, true).await.ok());
    assert!(qb.io(IO_WRITE, 3, 2, &db, 0, false).await.ok());
    assert!(qa.io(IO_READ, 3, 2, &[], 8192, false).await.data == da);
    assert!(qb.io(IO_READ, 3, 2, &[], 8192, false).await.data == db);
    let mut buf = vec![0u8; 8192];
    fs.block_read(&ds("alpha"), 3 * 4096, &mut buf)
        .await
        .unwrap();
    assert!(buf == da);
    fs.block_read(&ds("beta"), 3 * 4096, &mut buf)
        .await
        .unwrap();
    assert!(buf == db);
    // Alpha's namespace ends at 16 MiB even though beta's doesn't.
    let last = 16 * MB / LBA_SIZE;
    assert_eq!(
        qa.io(IO_READ, last, 1, &[], 4096, false).await.sc(),
        SC_LBA_RANGE
    );
    assert!(qb.io(IO_READ, last, 1, &[], 4096, false).await.ok());
}

#[tokio::test]
async fn add_export_validation() {
    let pool = TestPool::new().await;
    let (_fs, server, _addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    assert!(matches!(
        server.add_export(&ds("vol")).await,
        Err(FileSystemError::AlreadyExists)
    ));
    assert!(server.add_export(&ds("missing")).await.is_err());
    // Empty and over-long names don't get this far: they don't parse, and
    // every name that does fits in an NQN (checked at compile time).
    assert!(crate::types::DatasetName::try_from("").is_err());
    assert!(crate::types::DatasetName::try_from("x".repeat(200).as_str()).is_err());
    let longest =
        crate::types::DatasetName::try_from("x".repeat(crate::types::MAX_DATASET_NAME).as_str())
            .unwrap();
    assert!(crate::nvme::subsystem_nqn(&longest).len() <= 223);
    assert_eq!(server.exports(), vec!["vol"]);
}

/// The target hangs up on `h` within a few seconds.
async fn assert_hung_up(h: &mut Host) {
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), read_pdu(&mut h.rd))
        .await
        .expect("target should close the queue");
    assert!(r.is_err(), "expected EOF");
}

#[tokio::test]
async fn remove_export_flushes_and_closes_its_queues() {
    let pool = TestPool::new().await;
    let (fs, server, addr, mut held) =
        serve_many(&pool, &[("alpha", 16 * MB), ("beta", 16 * MB)]).await;
    let (alpha, beta) = (subsystem_nqn("alpha"), subsystem_nqn("beta"));

    let mut admin_a = Host::connect(addr).await;
    let cntlid_a = admin_a.bring_up_admin_of(&alpha, 1).await;
    let mut qa = Host::connect(addr).await;
    assert!(qa.fabrics_connect(1, cntlid_a, &alpha).await.ok());
    let mut admin_b = Host::connect(addr).await;
    let cntlid_b = admin_b.bring_up_admin_of(&beta, 1).await;
    let mut qb = Host::connect(addr).await;
    assert!(qb.fabrics_connect(1, cntlid_b, &beta).await.ok());
    // A connection that hasn't sent Connect yet belongs to no export.
    let mut idle = Host::connect(addr).await;

    let data = pattern(4096, 7);
    assert!(qa.io(IO_WRITE, 0, 1, &data, 0, true).await.ok());
    assert!(fs.has_dirty_datasets(), "write not yet committed");

    held.remove("alpha").unwrap().close().await.unwrap();
    assert!(!fs.has_dirty_datasets(), "removal commits");
    assert_hung_up(&mut qa).await;
    assert_hung_up(&mut admin_a).await;
    assert_eq!(server.exports(), vec!["beta"]);

    // Gone for new connects; beta (and not-yet-connected queues) unaffected.
    let r = idle.fabrics_connect(0, 0xffff, &alpha).await;
    assert_eq!(r.dw0, 1 << 16 | 256);
    assert!(qb.io(IO_WRITE, 0, 1, &data, 0, true).await.ok());
    assert!(qb.io(IO_READ, 0, 1, &[], 4096, false).await.data == data);
    assert!(admin_b.admin(ADM_KEEP_ALIVE, 0, 0, 0, 0).await.ok());

    // The committed write survives a reload, and the volume can come back.
    let mut buf = vec![0u8; 4096];
    pool.load()
        .await
        .block_read(&ds("alpha"), 0, &mut buf)
        .await
        .unwrap();
    assert!(buf == data);
    let _again = server.add_export(&ds("alpha")).await.unwrap();
    let mut admin_a = Host::connect(addr).await;
    let cntlid_a = admin_a.bring_up_admin_of(&alpha, 1).await;
    let mut qa = Host::connect(addr).await;
    assert!(qa.fabrics_connect(1, cntlid_a, &alpha).await.ok());
    assert!(qa.io(IO_READ, 0, 1, &[], 4096, false).await.data == data);
}

#[tokio::test]
async fn admin_bring_up_and_identify() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 64 * MB).await;
    let mut h = Host::connect(addr).await;
    let cntlid = h.bring_up_admin(4).await;

    let ctrl = h.admin(ADM_IDENTIFY, 0, 0x01, 0, 4096).await;
    assert!(ctrl.ok());
    let id = &ctrl.data;
    assert_eq!(id.len(), 4096);
    assert!(id[24..64].starts_with(b"RAIDISH"));
    assert_eq!(le16(id, 78), cntlid);
    assert_eq!(cstr(&id[768..1024]), VOL_NQN);
    assert_eq!(id[111], 1, "I/O controller");
    assert_eq!(
        le32(id, 1792) as usize * 16,
        64 + INLINE_DATA_SIZE,
        "IOCCSZ"
    );
    assert_eq!(le32(id, 516), 1, "one namespace");

    let list = h.admin(ADM_IDENTIFY, 0, 0x02, 0, 4096).await;
    assert_eq!(le32(&list.data, 0), 1);
    assert_eq!(le32(&list.data, 4), 0);

    let ns = h.admin(ADM_IDENTIFY, 1, 0x00, 0, 4096).await;
    assert!(ns.ok());
    assert_eq!(le64(&ns.data, 0), 64 * MB / LBA_SIZE, "NSZE");
    assert_eq!((le32(&ns.data, 128) >> 16) & 0xff, 12, "4 KiB LBAs");

    let descs = h.admin(ADM_IDENTIFY, 1, 0x03, 0, 4096).await;
    assert_eq!(descs.data[0], 0x03);
    assert_eq!(&descs.data[4..20], &ns.data[104..120], "UUID matches NGUID");

    assert_eq!(
        h.admin(ADM_IDENTIFY, 2, 0x00, 0, 4096).await.sc(),
        SC_INVALID_NS
    );
    assert_eq!(
        h.admin(ADM_IDENTIFY, 0, 0x55, 0, 4096).await.sc(),
        SC_INVALID_FIELD
    );
    assert!(
        h.admin(ADM_GET_LOG_PAGE, 0, 0x02 | (127 << 16), 0, 512)
            .await
            .ok()
    );
    assert!(h.admin(ADM_KEEP_ALIVE, 0, 0, 0, 0).await.ok());

    // Admin opcode 0x01 is CREATE_IO_SQ, not WRITE: must not touch the volume.
    assert_eq!(h.admin(0x01, 1, 0, 0, 0).await.sc(), SC_INVALID_OPCODE);

    // Shutdown handshake.
    h.prop_set(PROP_CC, 0x0046_4001).await; // SHN=normal
    assert_eq!(
        (h.prop_get(PROP_CSTS, false).await >> 2) & 0x3,
        0x2,
        "shutdown complete"
    );
}

#[tokio::test]
async fn connect_validation() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 16 * MB).await;

    let mut h = Host::connect(addr).await;
    let r = h
        .fabrics_connect(0, 0xffff, "nqn.2014-08.org.nvmexpress:not-us")
        .await;
    assert_eq!((r.sct(), r.sc()), (SCT_FABRICS, SC_CONNECT_INVALID_PARAM));
    assert_eq!(r.dw0, 1 << 16 | 256, "points at SUBNQN in the data");

    // Commands before Connect.
    let mut h = Host::connect(addr).await;
    assert_eq!(
        h.admin(ADM_IDENTIFY, 0, 1, 0, 4096).await.sc(),
        SC_CMD_SEQ_ERROR
    );

    // I/O queue for a controller that doesn't exist.
    let mut h = Host::connect(addr).await;
    let r = h.fabrics_connect(1, 0x4242, VOL_NQN).await;
    assert_eq!(r.sc(), SC_CONNECT_INVALID_PARAM);

    // I/O queue before the controller is enabled.
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.fabrics_connect(0, 0xffff, VOL_NQN).await.dw0 as u16;
    let mut h = Host::connect(addr).await;
    assert!(!h.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
}

#[tokio::test]
async fn io_queue_read_write_flush_trim() {
    let pool = TestPool::new().await;
    let size = 64 * MB;
    let (_fs, addr) = serve(&pool, size).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(2).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());

    // In-capsule write (8 KiB), R2T write (1 MiB, several H2C PDUs).
    let small = pattern(8192, 1);
    let big = pattern(MAX_TRANSFER, 2);
    assert!(q.io(IO_WRITE, 10, 2, &small, 0, true).await.ok());
    assert!(
        q.io(IO_WRITE, 1000, (big.len() / 4096) as u32, &big, 0, false)
            .await
            .ok()
    );
    assert!(q.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok());

    let r = q.io(IO_READ, 10, 2, &[], 8192, false).await;
    assert!(r.ok());
    assert!(r.data == small);
    let r = q
        .io(
            IO_READ,
            1000,
            (big.len() / 4096) as u32,
            &[],
            big.len(),
            false,
        )
        .await;
    assert!(r.ok());
    assert!(r.data == big, "1 MiB read spans several C2H PDUs");

    // Deallocate part of the big write, Write Zeroes over the small one.
    let mut ranges = vec![0u8; 16];
    ranges[4..8].copy_from_slice(&64u32.to_le_bytes()); // 64 LBAs
    ranges[8..16].copy_from_slice(&1000u64.to_le_bytes());
    let cid = q.cid();
    let mut s = sqe(IO_DSM, cid, 1);
    set_cdw(&mut s, 10, 0); // one range
    set_cdw(&mut s, 11, 1 << 2); // AD
    sgl_inline(&mut s, 16);
    q.send_capsule(&s, &ranges).await;
    assert!(q.wait(cid, &[]).await.ok());
    assert!(q.io(IO_WRITE_ZEROES, 10, 2, &[], 0, false).await.ok());

    let r = q.io(IO_READ, 1000, 128, &[], 128 * 4096, false).await;
    assert!(
        r.data[..64 * 4096].iter().all(|&b| b == 0),
        "deallocated range reads zeros"
    );
    assert!(
        r.data[64 * 4096..] == big[64 * 4096..128 * 4096],
        "rest intact"
    );
    assert!(
        q.io(IO_READ, 10, 2, &[], 8192, false)
            .await
            .data
            .iter()
            .all(|&b| b == 0)
    );

    // Errors.
    let last = size / LBA_SIZE;
    assert_eq!(
        q.io(IO_READ, last - 1, 2, &[], 8192, false).await.sc(),
        SC_LBA_RANGE
    );
    assert_eq!(
        q.io(IO_READ, u64::MAX - 1, 4, &[], 16384, false).await.sc(),
        SC_LBA_RANGE
    );
    let cid = q.cid();
    let mut s = sqe(IO_READ, cid, 2);
    sgl_transport(&mut s, 4096);
    q.send_capsule(&s, &[]).await;
    assert_eq!(q.wait(cid, &[]).await.sc(), SC_INVALID_NS);
    // I/O opcode 0x06 is not Identify on an I/O queue.
    assert_eq!(
        q.io(0x06, 0, 1, &[], 4096, false).await.sc(),
        SC_INVALID_OPCODE
    );
}

#[tokio::test]
async fn data_survives_reload() {
    let pool = TestPool::new().await;
    let data = pattern(300 * 4096, 9);
    {
        let (fs, addr) = serve(&pool, 32 * MB).await;
        let mut admin = Host::connect(addr).await;
        let cntlid = admin.bring_up_admin(1).await;
        let mut q = Host::connect(addr).await;
        assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
        for (i, chunk) in data.chunks(64 * 4096).enumerate() {
            let r = q
                .io(
                    IO_WRITE,
                    77 + i as u64 * 64,
                    (chunk.len() / 4096) as u32,
                    chunk,
                    0,
                    false,
                )
                .await;
            assert!(r.ok());
        }
        assert!(q.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok());
        drop(fs);
    }

    let fs = pool.load().await;
    let mut buf = vec![0u8; data.len()];
    fs.block_read(&ds("vol"), 77 * 4096, &mut buf)
        .await
        .unwrap();
    assert!(buf == data);
}

#[tokio::test]
async fn pipelined_commands_on_one_queue() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 64 * MB).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());

    // 32 in-capsule writes sent back to back before reading any response.
    let mut cids = Vec::new();
    for i in 0..32u64 {
        let cid = q.cid();
        let mut s = sqe(IO_WRITE, cid, 1);
        set_cdw(&mut s, 10, (i * 3) as u32);
        let data = pattern(4096, i as u8);
        sgl_inline(&mut s, 4096);
        q.send_capsule(&s, &data).await;
        cids.push(cid);
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..32 {
        let pdu = read_pdu(&mut q.rd).await.unwrap();
        assert_eq!(pdu[0], PDU_RSP);
        assert_eq!(le16(&pdu, 22) >> 1, 0);
        assert!(seen.insert(le16(&pdu, 20)));
    }
    assert_eq!(seen.len(), 32);
    for i in 0..32u64 {
        let r = q.io(IO_READ, i * 3, 1, &[], 4096, false).await;
        assert!(r.data == pattern(4096, i as u8), "write {i}");
    }
}

/// Counts the I/O commands that reach the backend it wraps.
struct Counting {
    inner: Arc<dyn VolumeIo>,
    n: Arc<std::sync::atomic::AtomicUsize>,
}

impl Counting {
    fn hit(&self) {
        self.n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl VolumeIo for Counting {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.hit();
        self.inner.read(offset, buf).await
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.hit();
        self.inner.write(offset, data).await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.hit();
        self.inner.zero(offset, len).await
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.hit();
        self.inner.flush().await
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.inner.sync().await
    }
}

/// An export's backend is switched under a connected I/O queue: the queue
/// stays up and its commands go to the new backend, data intact.
#[tokio::test]
async fn an_exports_backend_switches_under_a_connected_queue() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let before = pattern(8192, 3);
    assert!(q.io(IO_WRITE, 0, 2, &before, 0, true).await.ok());

    let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let io = server.export_io(&ds("vol")).unwrap();
    io.switch(Arc::new(Counting {
        inner: Arc::new(crate::volume_io::LocalIo::new(fs.clone(), &ds("vol"))),
        n: n.clone(),
    }))
    .await;

    let after = pattern(8192, 4);
    assert!(q.io(IO_WRITE, 2, 2, &after, 0, true).await.ok());
    assert!(q.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok());
    let r = q.io(IO_READ, 0, 4, &[], 16384, false).await;
    assert!(r.ok());
    assert!(r.data[..8192] == before[..] && r.data[8192..] == after[..]);
    assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 3);
}

/// Remote input bounds: a SET_FEATURES asking for 0x10000 queues (0-based
/// 0xffff) gets the most there are, not an overflow that drops the
/// connection; and a GET_LOG_PAGE naming a huge length gets the page, not
/// that many bytes made for it.
#[tokio::test]
async fn admin_lengths_and_counts_are_bounded() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 64 * MB).await;
    let mut h = Host::connect(addr).await;
    h.bring_up_admin(4).await;

    // An overflow there kills the queue's task, and the reply never comes.
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        h.admin(ADM_SET_FEATURES, 0, 0x07, u32::MAX, 0),
    )
    .await
    .expect("SET_FEATURES answered");
    assert!(r.ok(), "status {:#x}", r.status);
    let n = MAX_IO_QUEUES as u32 - 1;
    assert_eq!(r.dw0, n << 16 | n);

    let r = h
        .admin(
            ADM_GET_LOG_PAGE,
            0,
            0x02 | (0xffff << 16),
            0xffff,
            16 * MB as usize,
        )
        .await;
    assert!(r.ok(), "status {:#x}", r.status);
    assert!(r.data.len() <= 4096, "{} bytes", r.data.len());
}

/// I-24 review: a controller shut down (CC.SHN) makes what it wrote
/// durable, but its error goes only to the log. It must not take the
/// export's lost-writes poison with it: the next host's flush is owed it.
#[tokio::test]
async fn a_shutdown_leaves_lost_writes_for_the_next_hosts_flush() {
    let pool = TestPool::new().await;
    let (_fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    admin.bring_up_admin(1).await;
    server.export_io(&ds("vol")).unwrap().poison();
    // SHN=normal. The sync fails (writes were lost), so the shutdown isn't
    // reported complete: it used to be (SHST=2) whatever the sync said.
    let cid = admin.cid();
    let mut s = sqe(OPC_FABRICS, cid, 0);
    s[4] = FCT_PROP_SET;
    s[44..48].copy_from_slice(&PROP_CC.to_le_bytes());
    s[48..52].copy_from_slice(&0x0046_4001u32.to_le_bytes());
    admin.send_capsule(&s, &[]).await;
    assert!(
        !admin.wait(cid, &[]).await.ok(),
        "a failed sync reported as done"
    );
    assert_eq!(
        (admin.prop_get(PROP_CSTS, false).await >> 2) & 0x3,
        0,
        "SHST"
    );

    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    assert!(
        !q.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok(),
        "the shutdown took the poison"
    );
    assert!(q.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok());
}

/// I-26: H2C data PDUs must come in order. One sent twice used to count
/// twice, so the write ran with the tail it never got (zeros) in it.
#[tokio::test]
async fn overlapping_h2c_data_is_refused_not_written() {
    let pool = TestPool::new().await;
    let (_fs, _server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let old = pattern(4096, 9);
    assert!(q.io(IO_WRITE, 1, 1, &old, 0, true).await.ok());

    // 8 KiB at LBA 0 by R2T, its first half sent twice.
    let cid = q.send_io(IO_WRITE, 0, 2, 8192, &[]).await;
    let r2t = read_pdu(&mut q.rd).await.unwrap();
    assert_eq!(r2t[0], PDU_R2T);
    let ttag = le16(&r2t, 10);
    let new = pattern(8192, 1);
    q.send_h2c(cid, ttag, 0, &new[..4096], false).await;
    q.send_h2c(cid, ttag, 0, &new[..4096], false).await;
    assert_hung_up(&mut q).await;

    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let r = q.io(IO_READ, 1, 1, &[], 4096, false).await;
    assert!(r.data == old, "the unreceived half was written");
}

/// Two Connects pipelined on one queue: one controller, the second
/// refused. (The race this guards, two threads each seeing no queue yet
/// and each making a controller, is a few instructions wide; this test
/// doesn't catch it, but `fabrics` now checks and sets under one lock.)
#[tokio::test]
async fn a_second_connect_on_a_queue_is_refused() {
    let pool = TestPool::new().await;
    let (_fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut h = Host::connect(addr).await;
    let a = h.send_connect(0, 0xffff, VOL_NQN).await;
    let b = h.send_connect(0, 0xffff, VOL_NQN).await;
    let mut ok = 0;
    for _ in 0..2 {
        let pdu = read_pdu(&mut h.rd).await.unwrap();
        assert_eq!(pdu[0], PDU_RSP);
        assert!([a, b].contains(&le16(&pdu, 20)));
        if le16(&pdu, 22) >> 1 == 0 {
            ok += 1;
        }
    }
    assert_eq!(ok, 1, "both Connects made a controller");
    assert_eq!(server.target.ctrls.lock().unwrap().len(), 1);
}

/// Controller IDs wrap within the dynamic range and skip live ones. They
/// used to count on into the reserved IDs, and onto live ones.
#[tokio::test]
async fn controller_ids_skip_live_and_reserved_ones() {
    let pool = TestPool::new().await;
    let (_fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut first = Host::connect(addr).await;
    assert_eq!(first.bring_up_admin(1).await, 1);
    server
        .target
        .next_cntlid
        .store(MAX_CNTLID, Ordering::Relaxed);
    let mut last = Host::connect(addr).await;
    assert_eq!(last.bring_up_admin(1).await, MAX_CNTLID);
    let mut next = Host::connect(addr).await;
    assert_eq!(next.bring_up_admin(1).await, 2, "1 is live");

    let next = AtomicU16::new(7);
    assert_eq!(alloc_cntlid(&next, |_| true), None, "all taken");
}

/// A controller's I/O queues close with its admin queue. They used to go
/// on serving a controller that was gone.
#[tokio::test]
async fn closing_the_admin_queue_closes_its_io_queues() {
    let pool = TestPool::new().await;
    let (_fs, _server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(2).await;
    let mut q1 = Host::connect(addr).await;
    assert!(q1.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let mut q2 = Host::connect(addr).await;
    assert!(q2.fabrics_connect(2, cntlid, VOL_NQN).await.ok());
    assert!(q1.io(IO_FLUSH, 0, 0, &[], 0, false).await.ok());
    drop(admin);
    assert_hung_up(&mut q1).await;
    assert_hung_up(&mut q2).await;
}

/// Writes by R2T ask for at most `R2T_BUDGET` of data at a time; the rest
/// wait for their R2T. Every one used to get its R2T, and a buffer for its
/// data, at once (up to 64 Ki of them, 1 MiB each).
#[tokio::test]
async fn r2ts_ask_for_at_most_the_budget_at_once() {
    let pool = TestPool::new().await;
    let (_fs, _server, addr, _held) = serve_many(&pool, &[("vol", 64 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());

    let n = 32;
    let len = MAX_TRANSFER;
    let mut data = HashMap::new();
    for i in 0..n {
        let cid = q
            .send_io(
                IO_WRITE,
                (i * len) as u64 / LBA_SIZE,
                (len as u64 / LBA_SIZE) as u32,
                len,
                &[],
            )
            .await;
        data.insert(cid, pattern(len, i as u8));
    }
    let mut r2ts = Vec::new();
    while let Ok(pdu) =
        tokio::time::timeout(std::time::Duration::from_millis(300), read_pdu(&mut q.rd)).await
    {
        let pdu = pdu.unwrap();
        assert_eq!(pdu[0], PDU_R2T);
        r2ts.push(pdu);
    }
    assert_eq!(r2ts.len(), R2T_BUDGET / len, "R2Ts before any data came");

    // Answer each R2T, and the ones that follow, until every write is done.
    let mut done = 0;
    while done < n {
        for r2t in std::mem::take(&mut r2ts) {
            let (cid, ttag) = (le16(&r2t, 8), le16(&r2t, 10));
            let d = data[&cid].clone();
            for (k, chunk) in d.chunks(q.maxh2cdata as usize).enumerate() {
                let off = k * q.maxh2cdata as usize;
                q.send_h2c(cid, ttag, off, chunk, off + chunk.len() == d.len())
                    .await;
            }
        }
        let pdu = read_pdu(&mut q.rd).await.unwrap();
        match pdu[0] {
            PDU_R2T => r2ts.push(pdu),
            PDU_RSP => {
                assert_eq!(le16(&pdu, 22) >> 1, 0, "write failed");
                done += 1;
            }
            t => panic!("unexpected PDU {t:#x}"),
        }
    }
    let r = q
        .io(IO_READ, (5 * len) as u64 / LBA_SIZE, 1, &[], 4096, false)
        .await;
    assert!(r.data[..] == pattern(len, 5)[..4096]);
}

/// Holds every command until opened, counting how many got in.
struct Gate {
    inner: Arc<dyn VolumeIo>,
    entered: Arc<std::sync::atomic::AtomicUsize>,
    open: Arc<watch::Sender<bool>>,
}

impl Gate {
    async fn wait(&self) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.open.subscribe().wait_for(|o| *o).await;
    }
}

#[async_trait::async_trait]
impl VolumeIo for Gate {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.wait().await;
        self.inner.read(offset, buf).await
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.wait().await;
        self.inner.write(offset, data).await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.wait().await;
        self.inner.zero(offset, len).await
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.wait().await;
        self.inner.flush().await
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.inner.sync().await
    }
}

/// A host sending past the queue's depth gets `QUEUE_ENTRIES` commands
/// running at once; the rest wait unread. Every command used to get a
/// task at once.
#[tokio::test]
async fn a_queue_has_at_most_its_depth_of_commands_running() {
    let pool = TestPool::new().await;
    let (fs, server, addr, _held) = serve_many(&pool, &[("vol", 16 * MB)]).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open = Arc::new(watch::Sender::new(false));
    server
        .export_io(&ds("vol"))
        .unwrap()
        .switch(Arc::new(Gate {
            inner: Arc::new(crate::volume_io::LocalIo::new(fs.clone(), &ds("vol"))),
            entered: entered.clone(),
            open: open.clone(),
        }))
        .await;

    let n = QUEUE_ENTRIES as usize + 50;
    let Host { mut rd, mut wr, .. } = q;
    let send = tokio::spawn(async move {
        for i in 0..n {
            let cid = i as u16 + 100;
            let mut s = sqe(IO_WRITE, cid, 1);
            set_cdw(&mut s, 10, i as u32);
            sgl_inline(&mut s, 4096);
            let mut pdu = vec![0u8; 72];
            pdu[0] = PDU_CMD;
            pdu[2] = 72;
            pdu[3] = 72;
            pdu[8..72].copy_from_slice(&s);
            pdu.extend_from_slice(&pattern(4096, i as u8));
            let plen = pdu.len() as u32;
            pdu[4..8].copy_from_slice(&plen.to_le_bytes());
            wr.write_all(&pdu).await.unwrap();
        }
        wr
    });
    assert_eq!(
        crate::nbd::tests::settled(&entered).await,
        QUEUE_ENTRIES as usize
    );
    open.send_replace(true);
    for _ in 0..n {
        let pdu = read_pdu(&mut rd).await.unwrap();
        assert_eq!(pdu[0], PDU_RSP);
        assert_eq!(le16(&pdu, 22) >> 1, 0, "write failed");
    }
    let _wr = send.await.unwrap();
}

/// H-12: NVMe/TCP must refuse disabling the volatile write cache (`WCE=0`),
/// reject `ICReq` with `hpda > 31`, and fail short writes from the backend.
#[tokio::test]
async fn h12_nvme_rejects_vwc_disable_short_write_and_bad_hpda() {
    struct ShortWriteIo;
    #[async_trait::async_trait]
    impl crate::volume_io::VolumeIo for ShortWriteIo {
        async fn read(&self, _: u64, buf: &mut [u8]) -> Result<usize, crate::fs::FileSystemError> {
            Ok(buf.len())
        }
        async fn write(&self, _: u64, data: &[u8]) -> Result<usize, crate::fs::FileSystemError> {
            Ok(data.len() / 2)
        }
        async fn zero(&self, _: u64, _: usize) -> Result<(), crate::fs::FileSystemError> {
            Ok(())
        }
        async fn flush(&self) -> Result<(), crate::fs::FileSystemError> {
            Ok(())
        }
        async fn sync(&self) -> Result<(), crate::fs::FileSystemError> {
            Ok(())
        }
    }

    let pool = TestPool::new().await;
    let (_fs, server, addr, _held) = serve_many(&pool, &[("vol", 1024 * 1024)]).await;

    // 1. Bad HPDA (> 31) in ICReq must be rejected instead of truncating c2h_pdo.
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = stream.into_split();
        let mut ic = [0u8; 128];
        ic[0] = PDU_IC_REQ;
        ic[2] = 128;
        ic[4..8].copy_from_slice(&128u32.to_le_bytes());
        ic[10] = 64;
        wr.write_all(&ic).await.unwrap();
        assert!(
            read_pdu(&mut rd).await.is_err(),
            "ICReq with hpda=64 must be rejected"
        );
    }

    // 2. SET_FEATURES FID=0x06: WCE=0 must be refused; WCE=1 succeeds.
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let r = admin.admin(ADM_SET_FEATURES, 0, 0x06, 0, 0).await;
    assert_eq!(
        r.sc(),
        SC_INVALID_FIELD,
        "disabling VWC (WCE=0) must return SC_INVALID_FIELD"
    );
    let r = admin.admin(ADM_SET_FEATURES, 0, 0x06, 1, 0).await;
    assert!(r.ok(), "enabling VWC (WCE=1) must succeed");
    assert_eq!(r.dw0, 1);

    // 3. Short write from VolumeIo must fail with SC_INTERNAL, not succeed.
    server
        .export_io(&ds("vol"))
        .unwrap()
        .switch(Arc::new(ShortWriteIo))
        .await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, VOL_NQN).await.ok());
    let data = pattern(4096, 3);
    let r = q.io(IO_WRITE, 0, 1, &data, 0, true).await;
    assert_eq!(
        r.sc(),
        SC_INTERNAL,
        "short backend write must not return success"
    );
}
