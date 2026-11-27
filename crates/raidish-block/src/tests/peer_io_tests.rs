//! Peer I/O over real TLS: a writer serving a volume's export to proxies.

use super::*;
use crate::export::BlockExporter;
use crate::fs::FileSystem;
use crate::nbd::NbdServer;
use crate::test_util::ds;
use crate::test_util::{DEFAULT_VOLBLOCKSIZE, TestPool};
use crate::types::DatasetName;
use crate::types::Epoch;
use crate::vdev::Pool;
use std::sync::atomic::AtomicU64;

const MB: u64 = 1 << 20;

/// The writer: its file system, an export of "vol", and the epoch it
/// writes under (anything else is refused).
struct Writer {
    fs: Arc<FileSystem<Pool>>,
    nbd: Arc<NbdServer<Pool>>,
    /// The volume's export (None: taken down).
    held: std::sync::Mutex<Option<crate::export::ExportHandle>>,
    epoch: AtomicU64,
    /// The epoch it took the volume over from cleanly (0: none).
    from: AtomicU64,
}

#[async_trait]
impl PeerVolumes for Writer {
    fn export(&self, volume: &DatasetName, epoch: Epoch) -> Result<PeerExport, String> {
        let mine = self.epoch.load(Ordering::SeqCst);
        if epoch != mine {
            return Err(format!("not the writer of {} at epoch {}", volume, epoch));
        }
        self.nbd
            .peer_export(volume)
            .map(|e| PeerExport {
                from: Epoch::from_raw(self.from.load(Ordering::SeqCst)),
                ..e
            })
            .ok_or_else(|| format!("{} isn't exported here", volume))
    }
}

impl Writer {
    /// Take the export down (its connections drain).
    async fn unexport(&self) -> Result<(), FileSystemError> {
        let h = self.held.lock().unwrap().take().expect("exported");
        h.close().await
    }

    async fn export_again(&self) {
        let h = self.nbd.add_export(&ds("vol")).await.unwrap();
        *self.held.lock().unwrap() = Some(h);
    }
}

struct Setup {
    _pool: TestPool,
    fixture: crate::testing::Pki,
    writer: Arc<Writer>,
    port: u16,
}

async fn setup() -> Setup {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.create_block(&ds("vol"), 16 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let nbd = Arc::new(NbdServer::with_bind(fs.clone(), "127.0.0.1", 0));
    let held = std::sync::Mutex::new(Some(nbd.add_export(&ds("vol")).await.unwrap()));
    let writer = Arc::new(Writer {
        fs,
        nbd,
        held,
        epoch: AtomicU64::new(7),
        from: AtomicU64::new(0),
    });
    let fixture = crate::testing::Pki::new(vec![]);
    let acceptor = TlsAcceptor::from(Arc::new(fixture.tls.server_config(true, &[]).unwrap()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let w: Arc<dyn PeerVolumes> = writer.clone();
    tokio::spawn(async move { serve(listener, acceptor, w, None).await });
    Setup {
        _pool: pool,
        fixture,
        writer,
        port,
    }
}

impl Setup {
    /// A proxy as node `n1`, finding the writer at `epoch`.
    fn proxy(&self, epoch: Arc<AtomicU64>) -> PeerIo {
        self.proxy_as(&self.fixture.tls, epoch)
    }

    fn proxy_as(&self, tls: &crate::tls::TlsConfig, epoch: Arc<AtomicU64>) -> PeerIo {
        let connector = TlsConnector::from(Arc::new(tls.client_config().unwrap()));
        let port = self.port;
        let locate = move || {
            Some((
                "127.0.0.1".to_string(),
                port,
                Epoch::from_raw(epoch.load(Ordering::SeqCst)),
            ))
        };
        PeerIo::new(&ds("vol"), connector, Arc::new(locate))
    }
}

fn epoch(e: u64) -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(e))
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn proxied_io_reaches_the_writer() {
    let s = setup().await;
    let p = Arc::new(s.proxy(epoch(7)));

    // Concurrent writes on the one connection, then read back through it.
    let mut tasks = Vec::new();
    for i in 0..32u64 {
        let p = p.clone();
        tasks.push(tokio::spawn(async move {
            let data = pattern(8192, i as u8);
            assert_eq!(p.write(i * 8192, &data).await.unwrap(), 8192);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    p.zero(3 * 8192, 4096).await.unwrap();
    p.flush().await.unwrap();
    let mut got = vec![0u8; 32 * 8192];
    assert_eq!(p.read(0, &mut got).await.unwrap(), got.len());
    for i in 0..32u64 {
        let mut want = pattern(8192, i as u8);
        if i == 3 {
            want[..4096].fill(0);
        }
        let at = (i * 8192) as usize;
        assert!(got[at..at + 8192] == want[..], "block {}", i);
    }
    // It's the writer's volume that has it.
    let mut direct = vec![0u8; 8192];
    s.writer
        .fs
        .block_read(&ds("vol"), 5 * 8192, &mut direct)
        .await
        .unwrap();
    assert!(direct == pattern(8192, 5));
}

/// The proxy names a stale epoch (the writer moved on, say): refused, it
/// keeps asking, and gets through once it finds the writer's epoch.
#[tokio::test]
async fn a_stale_epoch_is_refused_until_the_proxy_finds_the_writer() {
    let s = setup().await;
    let e = epoch(6);
    let p = Arc::new(s.proxy(e.clone()));
    let w = p.clone();
    let mut write = tokio::spawn(async move { w.write(0, &[9u8; 4096]).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(400), &mut write)
            .await
            .is_err(),
        "a write went through with a stale epoch"
    );
    e.store(7, Ordering::SeqCst);
    assert_eq!(write.await.unwrap().unwrap(), 4096);
    let mut b = [0u8; 4096];
    s.writer.fs.block_read(&ds("vol"), 0, &mut b).await.unwrap();
    assert_eq!(b, [9u8; 4096]);
}

/// Only node certificates may proxy: an admin's is turned away (and the
/// request fails once its retry time is up).
#[tokio::test]
async fn only_nodes_may_proxy() {
    let s = setup().await;
    let admin = crate::tls::TlsConfig {
        dir: Some(s.fixture.dir.join("admin").to_string_lossy().into()),
        ..Default::default()
    };
    let p = s
        .proxy_as(&admin, epoch(7))
        .retry_for(Duration::from_millis(500));
    let err = p.write(0, &[1u8; 4096]).await.unwrap_err();
    assert!(matches!(err, FileSystemError::Unavailable(_)), "{}", err);
    let mut b = [0u8; 4096];
    s.writer.fs.block_read(&ds("vol"), 0, &mut b).await.unwrap();
    assert_eq!(b, [0u8; 4096], "an admin's write reached the volume");
}

/// The writer closes the export (a handoff draining it): the proxy's
/// connection goes, and its next request reconnects once it's back.
#[tokio::test]
async fn a_closed_connection_is_reconnected() {
    let s = setup().await;
    let p = Arc::new(s.proxy(epoch(7)));
    p.write(0, &[1u8; 4096]).await.unwrap();
    // Removing the export drains the proxy's connection like any client's.
    tokio::time::timeout(Duration::from_secs(5), s.writer.unexport())
        .await
        .expect("the proxy's connection held the export open")
        .unwrap();
    let w = p.clone();
    let mut write = tokio::spawn(async move { w.write(4096, &[2u8; 4096]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!write.is_finished(), "a write went through with no export");
    s.writer.export_again().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), &mut write)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        4096
    );
    let mut b = vec![0u8; 8192];
    p.read(0, &mut b).await.unwrap();
    assert!(b[..4096] == [1u8; 4096] && b[4096..] == [2u8; 4096]);
}

/// A request stuck retrying (no writer anywhere) is let go by `retire`, and
/// the `SwitchableIo` around it runs it again on the backend that replaces
/// it: the client sees the write succeed, not fail.
#[tokio::test]
async fn retiring_moves_waiting_requests_to_the_next_backend() {
    let s = setup().await;
    let connector = TlsConnector::from(Arc::new(s.fixture.tls.client_config().unwrap()));
    let nowhere = || None::<(String, u16, Epoch)>;
    let peer = Arc::new(PeerIo::new(&ds("vol"), connector, Arc::new(nowhere)));
    let io = Arc::new(SwitchableIo::new(peer.clone()));

    let w = io.clone();
    let mut write = tokio::spawn(async move { w.write(0, &[5u8; 4096]).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished());

    // This node takes the volume over: retire the proxy, switch to local.
    peer.retire();
    io.switch(Arc::new(crate::volume_io::LocalIo::new(
        s.writer.fs.clone(),
        &ds("vol"),
    )))
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), &mut write)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        4096
    );
    let mut b = [0u8; 4096];
    s.writer.fs.block_read(&ds("vol"), 0, &mut b).await.unwrap();
    assert_eq!(b, [5u8; 4096]);
}

/// A burst of large reads and then nothing (a kernel scanning a new
/// device): every reply arrives. (Over TLS a reply written while the socket
/// is full stays in the session until flushed; loopback rarely fills, so
/// this guards the path more than it reproduces that.)
#[tokio::test]
async fn a_burst_of_reads_is_answered_in_full() {
    let s = setup().await;
    let p = Arc::new(s.proxy(epoch(7)));
    p.write(0, &pattern(1 << 20, 3)).await.unwrap();
    for round in 0..5 {
        let mut tasks = Vec::new();
        for i in 0..64u64 {
            let p = p.clone();
            tasks.push(tokio::spawn(async move {
                let mut b = vec![0u8; 128 << 10];
                p.read((i % 8) * (128 << 10), &mut b).await.unwrap();
                b
            }));
        }
        let all = async {
            for t in tasks {
                t.await.unwrap();
            }
        };
        tokio::time::timeout(Duration::from_secs(10), all)
            .await
            .unwrap_or_else(|_| panic!("round {}: replies left unsent", round));
    }
}

/// A preamble of another protocol version is answered with what this node
/// speaks, not misread.
#[tokio::test]
async fn another_protocol_version_is_refused_by_name() {
    let s = setup().await;
    let connector = TlsConnector::from(Arc::new(s.fixture.tls.client_config().unwrap()));
    for line in ["RAIDISH-VOL/0 vol 7\n", "RAIDISH-HANDOFF vol 7 nbd\n"] {
        let tls = dial(&connector, "127.0.0.1", s.port).await.unwrap();
        let (rd, mut wr) = tokio::io::split(tls);
        wr.write_all(line.as_bytes()).await.unwrap();
        wr.flush().await.unwrap();
        let answer = read_line(&mut BufReader::new(rd)).await.unwrap();
        assert!(answer.starts_with("ERR "), "{:?} got {:?}", line, answer);
        assert!(
            answer.contains(PREAMBLE),
            "{:?} names ours: {:?}",
            line,
            answer
        );
    }
}

/// A2: writes the writer acknowledged but nobody flushed are lost if it
/// goes and the next writer didn't take over from it cleanly: the proxy's
/// next flush fails rather than vouch for them. After a clean handoff
/// (the new writer took over from that one) it doesn't.
#[tokio::test]
async fn a_flush_after_an_unclean_writer_change_fails() {
    let s = setup().await;
    let e = epoch(7);
    let p = Arc::new(s.proxy(e.clone()));
    // The writer changes with this write unflushed.
    let change = |s: &Setup, to: u64, from: u64| {
        s.writer.epoch.store(to, Ordering::SeqCst);
        s.writer.from.store(from, Ordering::SeqCst);
        e.store(to, Ordering::SeqCst);
    };
    let reconnect = async |s: &Setup| {
        tokio::time::timeout(Duration::from_secs(5), s.writer.unexport())
            .await
            .unwrap()
            .unwrap();
        s.writer.export_again().await;
    };

    p.write(0, &[1u8; 4096]).await.unwrap();
    assert!(p.unflushed());
    reconnect(&s).await;
    change(&s, 8, 0); // a crash: the new writer started afresh
    let r = p.flush().await;
    assert!(matches!(r, Err(FileSystemError::Unavailable(_))), "{:?}", r);
    // Reported once; from here on, flushes cover what's written.
    p.flush().await.unwrap();
    assert!(!p.unflushed());

    p.write(0, &[2u8; 4096]).await.unwrap();
    reconnect(&s).await;
    change(&s, 9, 8); // a clean handoff from the writer at 8
    p.flush().await.unwrap();

    // The same writer reconnected: nothing is lost.
    p.write(0, &[3u8; 4096]).await.unwrap();
    reconnect(&s).await;
    p.flush().await.unwrap();

    // Found lost by a read (no flush since): still owed, though nothing is
    // unflushed by count any more. A takeover asking `unflushed` (crash)
    // or `lost` (clean) must still see it (audit A2).
    p.write(0, &[4u8; 4096]).await.unwrap();
    reconnect(&s).await;
    change(&s, 10, 0);
    let mut buf = [0u8; 4096];
    p.read(0, &mut buf).await.unwrap();
    assert!(p.lost());
    assert!(p.unflushed(), "lost counts as unflushed");
    assert!(p.flush().await.is_err());
    assert!(!p.lost() && !p.unflushed());
}

/// I-24 review: writes found lost on a writer change are reported by a
/// sync (a client hanging up, the export going away) but left for the next
/// client flush: nobody sees a sync's error, and taking them there let
/// that flush vouch for writes that were lost.
#[tokio::test]
async fn a_sync_leaves_lost_writes_for_the_next_flush() {
    let s = setup().await;
    let e = epoch(7);
    let p = Arc::new(s.proxy(e.clone()));
    p.write(0, &[1u8; 4096]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.writer.unexport())
        .await
        .unwrap()
        .unwrap();
    s.writer.export_again().await;
    // A crash: the new writer started afresh, with that write unflushed.
    s.writer.epoch.store(8, Ordering::SeqCst);
    s.writer.from.store(0, Ordering::SeqCst);
    e.store(8, Ordering::SeqCst);
    let mut buf = [0u8; 4096];
    p.read(0, &mut buf).await.unwrap();
    assert!(p.lost());

    assert!(p.sync().await.is_err());
    assert!(p.lost(), "a sync took the lost writes");
    assert!(p.flush().await.is_err());
    p.flush().await.unwrap();
    p.sync().await.unwrap();
}

/// I-22: an idle proxy whose last writer went before it reconnected never
/// judged those writes (`reached` runs on a connection), so `lost` is
/// still false. A clean handoff from a later writer, which never
/// acknowledged them, must still owe them a failed flush; one from the
/// writer that acknowledged them doesn't.
#[tokio::test]
async fn a_handoff_from_another_writer_owes_an_idle_proxys_writes() {
    let s = setup().await;
    let p = Arc::new(s.proxy(epoch(7)));
    p.write(0, &[1u8; 4096]).await.unwrap();
    assert!(p.unflushed() && !p.lost());
    // The writer at 7 crashes; one at 8 takes the volume and hands it to
    // this node cleanly, the proxy idle all along.
    assert!(
        p.owed_on_handoff(Epoch::from_raw(8)),
        "writes acknowledged at 7, handed off by 8"
    );
    assert!(
        !p.owed_on_handoff(Epoch::from_raw(7)),
        "the writer that acknowledged them handed them off"
    );
}
