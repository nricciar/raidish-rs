//! Drives the NVMe/TCP target over real sockets with a minimal host that
//! follows the same sequence as the Linux initiator.

use super::*;
use crate::test_util::DEFAULT_VOLBLOCKSIZE;
use crate::test_util::TestPool;
use crate::vdev::Pool;

const MB: u64 = 1 << 20;
const HOSTNQN: &str = "nqn.2024-01.com.raidish:host:unit";

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
        self.wait(cid, &[]).await
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

    /// Full admin bring-up; returns the controller id.
    async fn bring_up_admin(&mut self, io_queues: u16) -> u16 {
        let r = self.fabrics_connect(0, 0xffff, SUBSYS_NQN).await;
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

async fn serve(pool: &TestPool, size: u64) -> (Arc<FileSystem<Pool>>, std::net::SocketAddr) {
    let fs = Arc::new(pool.format().await);
    fs.create_block("vol", size, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let server = NvmeTcpServer::new(fs.clone(), "vol".into(), 0)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { server.serve(listener).await });
    (fs, addr)
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
    assert_eq!(cstr(&id[768..1024]), SUBSYS_NQN);
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
    let r = h.fabrics_connect(1, 0x4242, SUBSYS_NQN).await;
    assert_eq!(r.sc(), SC_CONNECT_INVALID_PARAM);

    // I/O queue before the controller is enabled.
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.fabrics_connect(0, 0xffff, SUBSYS_NQN).await.dw0 as u16;
    let mut h = Host::connect(addr).await;
    assert!(!h.fabrics_connect(1, cntlid, SUBSYS_NQN).await.ok());
}

#[tokio::test]
async fn io_queue_read_write_flush_trim() {
    let pool = TestPool::new().await;
    let size = 64 * MB;
    let (_fs, addr) = serve(&pool, size).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(2).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, SUBSYS_NQN).await.ok());

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
        assert!(q.fabrics_connect(1, cntlid, SUBSYS_NQN).await.ok());
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
    fs.block_read("vol", 77 * 4096, &mut buf).await.unwrap();
    assert!(buf == data);
}

#[tokio::test]
async fn pipelined_commands_on_one_queue() {
    let pool = TestPool::new().await;
    let (_fs, addr) = serve(&pool, 64 * MB).await;
    let mut admin = Host::connect(addr).await;
    let cntlid = admin.bring_up_admin(1).await;
    let mut q = Host::connect(addr).await;
    assert!(q.fabrics_connect(1, cntlid, SUBSYS_NQN).await.ok());

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
