use super::*;
use crate::committer::dataset_resource;
use crate::inode::{DatasetProps, Transport};
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::test_util::dsn;
use std::collections::BTreeSet;

fn props(nodes: &[&str]) -> DatasetProps {
    DatasetProps {
        nodes: nodes.iter().map(|s| s.to_string()).collect(),
        automount: Some(Transport::Nvme),
        enabled: true,
        move_to: None,
        port: None,
    }
}

fn live(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|s| s.to_string()).collect()
}

#[test]
fn first_live_node_of_the_list() {
    let p = props(&["node-1", "node-2"]);
    assert_eq!(
        place(&p, None, &live(&["node-0", "node-1", "node-2"])).as_deref(),
        Some("node-1")
    );
    // node-1 is down: the next one.
    assert_eq!(
        place(&p, None, &live(&["node-0", "node-2"])).as_deref(),
        Some("node-2")
    );
    // None of the list is live: nowhere (not node-0).
    assert_eq!(place(&p, None, &live(&["node-0"])), None);
}

#[test]
fn no_fail_back() {
    let p = props(&["node-1", "node-2"]);
    // Running on node-2 after node-1 failed; node-1 is back: it stays.
    assert_eq!(
        place(&p, Some("node-2"), &live(&["node-1", "node-2"])).as_deref(),
        Some("node-2")
    );
    // Its holder died: back to the list order.
    assert_eq!(
        place(&p, Some("node-2"), &live(&["node-1"])).as_deref(),
        Some("node-1")
    );
    // A holder outside the list (taken off it) doesn't keep it.
    assert_eq!(
        place(
            &props(&["node-1"]),
            Some("node-2"),
            &live(&["node-1", "node-2"])
        )
        .as_deref(),
        Some("node-1")
    );
}

#[test]
fn moves_go_where_asked_while_that_node_is_live() {
    let mut p = props(&["node-1", "node-2"]);
    p.move_to = Some("node-1".into());
    let all = live(&["node-1", "node-2"]);
    assert_eq!(place(&p, Some("node-2"), &all).as_deref(), Some("node-1"));
    // The target is down: stay put.
    assert_eq!(
        place(&p, Some("node-2"), &live(&["node-2"])).as_deref(),
        Some("node-2")
    );
}

#[test]
fn any_node_and_off() {
    let p = props(&[]);
    assert_eq!(
        place(&p, None, &live(&["node-10", "node-2"])).as_deref(),
        Some("node-2")
    );
    assert_eq!(
        place(&p, Some("node-10"), &live(&["node-10", "node-2"])).as_deref(),
        Some("node-10")
    );
    assert_eq!(place(&p, None, &live(&[])), None);
    let mut off = props(&[]);
    off.enabled = false;
    assert_eq!(place(&off, None, &live(&["node-1"])), None);
    let mut manual = props(&[]);
    manual.automount = None;
    assert_eq!(place(&manual, None, &live(&["node-1"])), None);
}

fn names(nodes: &[&str]) -> Vec<String> {
    nodes.iter().map(|s| s.to_string()).collect()
}

#[test]
fn an_attached_volume_is_written_by_one_of_its_attached_nodes() {
    let p = props(&["node-0"]);
    let all = live(&["node-0", "node-1", "node-2"]);
    // Not attached anywhere in particular: its own placement.
    assert_eq!(
        place_attached(&p, None, &all, &[]).as_deref(),
        Some("node-0")
    );
    // Attached to node-1 and node-2: its node list doesn't count.
    let att = names(&["node-1", "node-2"]);
    assert_eq!(
        place_attached(&p, None, &all, &att).as_deref(),
        Some("node-1")
    );
    // Written by node-2 (attached, live): it stays there.
    assert_eq!(
        place_attached(&p, Some("node-2"), &all, &att).as_deref(),
        Some("node-2")
    );
    // Written by node-0, which isn't attached any more: to the first
    // attached node (a handoff).
    assert_eq!(
        place_attached(&p, Some("node-0"), &all, &att).as_deref(),
        Some("node-1")
    );
    // The first attached node is down: the next.
    assert_eq!(
        place_attached(&p, Some("node-0"), &live(&["node-0", "node-2"]), &att).as_deref(),
        Some("node-2")
    );
    // None of them live: nowhere.
    assert_eq!(place_attached(&p, None, &live(&["node-0"]), &att), None);
}

#[test]
fn a_move_within_the_attached_nodes_wins_and_off_is_off() {
    let mut p = props(&[]);
    p.move_to = Some("node-2".into());
    let all = live(&["node-1", "node-2"]);
    let att = names(&["node-1", "node-2"]);
    assert_eq!(
        place_attached(&p, Some("node-1"), &all, &att).as_deref(),
        Some("node-2")
    );
    // A move somewhere not attached doesn't count.
    p.move_to = Some("node-3".into());
    assert_eq!(
        place_attached(
            &p,
            Some("node-1"),
            &live(&["node-1", "node-2", "node-3"]),
            &att
        )
        .as_deref(),
        Some("node-1")
    );
    p.enabled = false;
    assert_eq!(place_attached(&p, Some("node-1"), &all, &att), None);
    p.enabled = true;
    p.automount = None;
    assert_eq!(place_attached(&p, Some("node-1"), &all, &att), None);
}

/// A6: a failed migration's guest may have switched over before the
/// failure; only where it runs decides what's done, and not knowing
/// leaves it alone (never moved cold, so never started twice).
#[test]
fn a_failed_migration_is_settled_by_where_its_guest_runs() {
    use vms::migrate::{Whereabouts::*, whereabouts};
    assert_eq!(whereabouts(Some(true), None), Here);
    assert_eq!(whereabouts(Some(true), Some(true)), Here);
    assert_eq!(whereabouts(Some(true), Some(false)), Here);
    assert_eq!(whereabouts(Some(false), Some(true)), There);
    assert_eq!(whereabouts(Some(false), Some(false)), Neither);
    assert_eq!(whereabouts(Some(false), None), Unknown);
    assert_eq!(whereabouts(None, Some(false)), Unknown);
    assert_eq!(whereabouts(None, None), Unknown);
}

/// One HTTP answer to whatever is asked, from a throwaway listener.
async fn answer_once(status: &'static str, body: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut c, _) = l.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = c.read(&mut buf).await;
        let r = format!(
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            status,
            body.len(),
            body
        );
        let _ = c.write_all(r.as_bytes()).await;
    });
    url
}

#[tokio::test]
async fn runs_there_says_none_unless_the_target_could_really_be_asked() {
    use crate::vm::LocalView;
    use crate::vm::{DomMeta, DomOwner, DomState, LocalVm};
    let http = reqwest::Client::new();
    let vm = |name: &str| LocalVm {
        name: name.parse().unwrap(),
        id: Some(1),
        state: DomState::Running,
        meta: DomMeta {
            anchor: dsn(name),
            volumes: vec![dsn(name)],
            change: 1,
            owner: DomOwner::Migrating,
            pool: String::new(),
            node: "2".into(),
        },
        vcpus: None,
        memory: None,
        cpu_s: None,
        vnc_port: None,
    };
    let view = |vms, error: Option<&str>| {
        serde_json::to_string(&LocalView {
            node: "2".into(),
            enabled: true,
            vms,
            errors: Default::default(),
            error: error.map(String::from),
        })
        .unwrap()
    };
    let url = answer_once("200 OK", view(vec![vm("web")], None)).await;
    assert_eq!(
        vms::migrate::runs_there(&http, &url, "web").await,
        Some(true)
    );
    let url = answer_once("200 OK", view(vec![vm("db")], None)).await;
    assert_eq!(
        vms::migrate::runs_there(&http, &url, "web").await,
        Some(false)
    );
    // Its libvirt couldn't be asked: not "it doesn't run there".
    let url = answer_once("200 OK", view(vec![], Some("libvirt down"))).await;
    assert_eq!(vms::migrate::runs_there(&http, &url, "web").await, None);
    let url = answer_once("503 Service Unavailable", "{}".into()).await;
    assert_eq!(vms::migrate::runs_there(&http, &url, "web").await, None);
    // Nobody there.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    drop(l);
    assert_eq!(vms::migrate::runs_there(&http, &url, "web").await, None);
}

// ---- volume slots (structural triage I-27, Phase B) ----

use crate::export::BlockExporter;
use volumes::{Claim, Owned, Proxied, Took, WriterGen, WriterParts};

struct Node {
    _pool: crate::test_util::TestPool,
    parts: WriterParts,
    shared: Arc<volumes::handoff::Shared>,
    pki: raidish_auth::testing::Pki,
}

fn vol() -> DatasetName {
    dsn("vol")
}

/// A standalone pool with volume "vol", its NBD and NVMe servers (not
/// listening) as this node's writer.
async fn node() -> Node {
    let pool = crate::test_util::TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.create_block(&vol(), 16 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let parts = writer_on(fs);
    let shared = Arc::new(volumes::handoff::Shared::new(LeaseClient::new(vec![])));
    shared.volumes.set_writer(parts.clone());
    Node {
        _pool: pool,
        parts,
        shared,
        pki: raidish_auth::testing::Pki::new(vec![]),
    }
}

/// A writer (new servers, new generation) on `fs`.
fn writer_on(fs: Arc<crate::fs::FileSystem<crate::vdev::Pool>>) -> WriterParts {
    WriterParts {
        wgen: WriterGen::next(),
        nbd: Arc::new(NbdServer::with_bind(fs.clone(), "127.0.0.1", 0)),
        nvme: Arc::new(NvmeTcpServer::with_bind(fs.clone(), "127.0.0.1", 0)),
        fs,
    }
}

impl Node {
    fn vols(&self) -> &volumes::Volumes {
        &self.shared.volumes
    }

    fn state(&self) -> Option<&'static str> {
        self.vols().state(&vol())
    }

    /// "vol" exported over `t` on `w`, proxied to a writer that's nowhere.
    fn proxy_on(&self, w: &WriterParts, t: Transport) -> Proxied {
        let connector =
            tokio_rustls::TlsConnector::from(Arc::new(self.pki.tls.client_config().unwrap()));
        let nowhere = || None::<(String, u16, crate::types::Epoch)>;
        let peer = Arc::new(
            crate::peer_io::PeerIo::new(&vol(), connector, Arc::new(nowhere))
                .retry_for(Duration::from_millis(200)),
        );
        let handle = w
            .exporter(t)
            .add_export_io(&vol(), 16 << 20, peer.clone())
            .unwrap();
        Proxied {
            peer,
            export: volumes::Export::new(t, handle),
            attach: Attach::Exported,
        }
    }

    fn proxy(&self, t: Transport) -> Proxied {
        self.proxy_on(&self.parts, t)
    }

    /// "vol" as written here at `epoch`, exported over NBD.
    async fn owned(&self, epoch: u64, attach: Attach) -> Owned {
        let lease = Arc::new(HeldLease::adopt(
            LeaseClient::new(vec![]),
            dataset_resource(&vol()),
            "owner",
            crate::types::Epoch::from_raw(epoch),
            std::time::Instant::now(),
        ));
        Owned {
            lease,
            lost: Lost(Arc::new(watch::Sender::new(false))),
            handed_from: None,
            export: self.parts.add_export(&vol(), Transport::Nbd).await.unwrap(),
            attach,
            attach_tried: std::time::Instant::now(),
        }
    }

    fn exported(&self, w: &WriterParts) -> bool {
        w.nbd.export_io(&vol()).is_some()
    }

    /// "vol" running here (taken over by the loop) at `epoch`.
    async fn run(&self, epoch: u64, attach: Attach) {
        let g = self
            .vols()
            .begin_takeover(&vol(), self.parts.wgen, Transport::Nbd)
            .unwrap();
        let o = self.owned(epoch, attach).await;
        assert!(g.finish(o, Took::Running).is_ok());
    }
}

/// A takeover that fails puts its proxy back, but only while its claim on
/// the slot stands and its writer is still this node's. Once the writer
/// was dropped meanwhile, the proxy is dead: it used to be put back, served
/// again to nobody, its export never removed. Now it's taken down instead;
/// and a slot made since (a new writer's) is left be.
#[tokio::test]
async fn a_failed_takeover_puts_back_only_a_proxy_whose_writer_is_still_here() {
    let n = node().await;
    let w = n.parts.wgen;
    assert!(
        n.vols()
            .add_proxied(&vol(), w, n.proxy(Transport::Nbd))
            .is_ok()
    );

    // Writer still here, claim standing: put back.
    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    assert!(g.prior.is_some());
    assert_eq!(n.state(), Some("taking over"));
    g.abort().await;
    assert_eq!(n.state(), Some("proxied"));
    assert!(n.exported(&n.parts));

    // The writer dropped while the takeover ran: taken down, not back.
    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    let peer = g.prior.as_ref().unwrap().peer.clone();
    let dropped = n.vols().drop_writer(w);
    assert!(dropped.owned.is_empty() && dropped.proxies.is_empty());
    assert_eq!(n.state(), None, "the claim went with its writer");
    let t = std::time::Instant::now();
    g.abort().await;
    // And at once: its last sync doesn't wait for a replacement backend
    // (`REPLACE_WAIT`, 30 s) nor retry the writer that's gone.
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "took {:?}",
        t.elapsed()
    );
    assert_eq!(n.state(), None, "a dead proxy was put back");
    assert!(!n.exported(&n.parts), "its export stayed up");
    assert!(crate::volume_io::VolumeIo::retired(&*peer));

    // A claim made under a writer since replaced: refused outright.
    let w2 = writer_on(n.parts.fs.clone());
    n.vols().set_writer(w2.clone());
    assert!(matches!(
        n.vols().begin_takeover(&vol(), w, Transport::Nbd),
        Err(Claim::StaleWriter)
    ));
    // A slot made since (the new writer's proxy) isn't an old claim's to
    // undo: one taken under the new writer, then that writer dropped and
    // replaced again, leaves the newest one's slot alone.
    let g = n
        .vols()
        .begin_takeover(&vol(), w2.wgen, Transport::Nbd)
        .unwrap();
    n.vols().drop_writer(w2.wgen);
    let w3 = writer_on(n.parts.fs.clone());
    n.vols().set_writer(w3.clone());
    assert!(
        n.vols()
            .add_proxied(&vol(), w3.wgen, n.proxy_on(&w3, Transport::Nbd))
            .is_ok()
    );
    g.abort().await;
    assert_eq!(n.state(), Some("proxied"));
    assert!(n.exported(&w3));
}

/// A takeover task cancelled (or erring out) without saying so gives its
/// claim up all the same: dropped, the guard puts a live proxy back at
/// once, and takes a dead one down in the background.
#[tokio::test]
async fn a_dropped_takeover_guard_gives_its_claim_up() {
    let n = node().await;
    let w = n.parts.wgen;
    assert!(
        n.vols()
            .add_proxied(&vol(), w, n.proxy(Transport::Nbd))
            .is_ok()
    );
    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    drop(g);
    assert_eq!(n.state(), Some("proxied"));

    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    let peer = g.prior.as_ref().unwrap().peer.clone();
    n.vols().drop_writer(w);
    drop(g);
    assert_eq!(n.state(), None);
    assert!(crate::volume_io::VolumeIo::retired(&*peer));
    let t = std::time::Instant::now();
    while n.exported(&n.parts) {
        assert!(t.elapsed() < Duration::from_secs(10), "never taken down");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A takeover that finishes after its writer was dropped is turned away
/// (its volume would run on a writer nothing renews or stops): the caller
/// gets it back, to undo. A volume is never claimed twice, nor over a
/// proxy of another transport (I-27: served over one, recorded as the
/// other), and a refused claim changes nothing.
#[tokio::test]
async fn takeovers_are_checked_against_the_writer_and_the_slot() {
    let n = node().await;
    let w = n.parts.wgen;
    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    assert!(matches!(
        n.vols().begin_takeover(&vol(), w, Transport::Nbd),
        Err(Claim::Busy)
    ));
    let o = n.owned(4, Attach::Exported).await;
    n.vols().drop_writer(w);
    let back = g.finish(o, Took::Adopted);
    assert!(back.is_err(), "finished onto a dropped writer");
    assert_eq!(n.state(), None);
    back.err().unwrap().export.close().await.unwrap();

    let w2 = writer_on(n.parts.fs.clone());
    n.vols().set_writer(w2.clone());
    assert!(
        n.vols()
            .add_proxied(&vol(), w2.wgen, n.proxy_on(&w2, Transport::Nbd))
            .is_ok()
    );
    assert_eq!(
        n.vols()
            .begin_takeover(&vol(), w2.wgen, Transport::Nvme)
            .err(),
        Some(Claim::Transport(Transport::Nbd))
    );
    assert_eq!(n.state(), Some("proxied"));
    assert_eq!(n.vols().proxy_transport(&vol()), Some(Transport::Nbd));
}

/// A handoff from here holds the volume whole while it runs: peers aren't
/// pointed at it, nothing claims it. Called off, it runs here again as it
/// was, device and all; with its writer gone meanwhile, it's the caller's
/// to stop instead.
#[tokio::test]
async fn an_aborted_handoff_puts_the_volume_back_whole() {
    let n = node().await;
    let w = n.parts.wgen;
    let dev = Attach::Attached {
        device: "/dev/nbd3".into(),
    };
    n.run(5, dev.clone()).await;
    let at = |e| {
        n.vols()
            .peer_export(&vol(), crate::types::Epoch::from_raw(e))
    };
    assert!(at(5).is_ok());
    assert!(at(4).is_err(), "answered for another epoch");

    let o = n.vols().begin_handoff(&vol(), w).unwrap();
    assert_eq!(n.state(), Some("handing off"));
    assert!(
        at(5).is_err(),
        "peers still pointed at a volume being handed off"
    );
    assert!(matches!(
        n.vols().begin_takeover(&vol(), w, Transport::Nbd),
        Err(Claim::Busy)
    ));
    assert!(n.vols().abort_handoff(&vol(), w, o).is_ok());
    assert_eq!(n.state(), Some("running"));
    assert_eq!(n.vols().owned(&vol(), |o| o.attach.clone()), Some(dev));
    assert!(at(5).is_ok());

    let o = n.vols().begin_handoff(&vol(), w).unwrap();
    n.vols().drop_writer(w);
    let back = n.vols().abort_handoff(&vol(), w, o);
    assert!(back.is_err(), "put back onto a dropped writer");
    assert_eq!(n.state(), None);
    back.err().unwrap().export.close().await.unwrap();
}

/// What was handed over is written here at once (peers proxy to it, with
/// the epoch it came from), and running once the loop picks it up; it goes
/// with its own lease and with its writer, picked up or not (audit A1).
#[tokio::test]
async fn a_handed_over_volume_is_written_here_until_its_lease_or_writer_goes() {
    let n = node().await;
    let w = n.parts.wgen;
    let g = n.vols().begin_takeover(&vol(), w, Transport::Nbd).unwrap();
    let mut o = n.owned(6, Attach::Exported).await;
    o.handed_from = Some(crate::types::Epoch::from_raw(5));
    let lost = o.lost.clone();
    assert!(g.finish(o, Took::Adopted).is_ok());
    assert_eq!(n.state(), Some("adopted"));
    let e = n
        .vols()
        .peer_export(&vol(), crate::types::Epoch::from_raw(6))
        .unwrap();
    assert_eq!(e.from, crate::types::Epoch::from_raw(5));
    // A peer's session on the export: its close would wait for it.
    drop(e);
    assert!(
        n.vols().running().is_empty(),
        "running before it's picked up"
    );
    assert_eq!(n.vols().published().len(), 1);

    lost.0.send_replace(true);
    assert_eq!(n.vols().lost(), vec![vol()]);
    lost.0.send_replace(false);

    let picked = n.vols().pick_up();
    assert_eq!(picked.len(), 1);
    assert_eq!(n.vols().running(), vec![(vol(), Transport::Nbd)]);

    let dropped = n.vols().drop_writer(w);
    assert_eq!(dropped.owned.len(), 1);
    assert_eq!(n.state(), None);
    assert!(n.vols().writer().is_none());
    for (_, o) in dropped.owned {
        o.export.close().await.unwrap();
    }
}

/// I-27: asked to take a volume over a transport other than the one it's
/// proxied here over, the receiver refuses (taken over, it would be served
/// over the proxy's while recorded as the other, and its export leak), and
/// the proxy is left as it was.
#[tokio::test]
async fn a_handoff_over_another_transport_than_the_proxys_is_refused() {
    use tokio::io::{AsyncWriteExt, BufReader};
    let n = node().await;
    assert!(
        n.vols()
            .add_proxied(&vol(), n.parts.wgen, n.proxy(Transport::Nbd))
            .is_ok()
    );

    let acceptor =
        tokio_rustls::TlsAcceptor::from(Arc::new(n.pki.tls.server_config(true, &[]).unwrap()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peers: Arc<dyn crate::peer_io::PeerVolumes> = n.shared.clone();
    tokio::spawn(crate::peer_io::serve(listener, acceptor, peers));

    let connector = tokio_rustls::TlsConnector::from(Arc::new(n.pki.tls.client_config().unwrap()));
    let tls = crate::peer_io::dial(&connector, "127.0.0.1", port)
        .await
        .unwrap();
    let (rd, mut wr) = tokio::io::split(tls);
    let mut rd = BufReader::new(rd);
    wr.write_all(format!("{} vol 3 {}\n", crate::peer_io::HANDOFF, Transport::Nvme).as_bytes())
        .await
        .unwrap();
    wr.flush().await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(10), crate::peer_io::read_line(&mut rd))
        .await
        .unwrap()
        .unwrap();
    assert!(answer.starts_with("ERR "), "{}", answer);
    assert_eq!(n.state(), Some("proxied"));
    assert!(n.exported(&n.parts));
}

/// The receiver waits for the transfer longer than the sender may take
/// to get there (after which it gives up instead), with room for the
/// transfer: else the lease could go to an owner that stopped waiting.
#[test]
fn the_receiver_outwaits_the_sender() {
    assert!(
        volumes::handoff::TRANSFER_WAIT
            >= volumes::handoff::SENDER_BUDGET + Duration::from_secs(20)
    );
    // The drain alone takes up to 30 s (`DRAIN_TIMEOUT`).
    assert!(volumes::handoff::SENDER_BUDGET > Duration::from_secs(30));
}

/// Every handoff message reads back as itself, an error stays one line,
/// and a garbled line is refused.
#[test]
fn handoff_messages_round_trip() {
    use volumes::handoff::Msg;
    let holder = crate::types::Holder {
        role: crate::types::HolderRole::Agent,
        node: Some("node-2".into()),
        addr: Some("10.0.0.2:7000".into()),
        detail: String::new(),
    };
    for m in [
        Msg::Ready {
            owner: "agent:node-2".into(),
            holder,
        },
        Msg::Transferred(crate::types::Epoch::from(7)),
        Msg::Done,
        Msg::Abort,
        Msg::Err("pool busy".into()),
    ] {
        assert_eq!(Msg::parse(&m.to_string()), Ok(m));
    }
    let e = Msg::Err("one\ntwo".into()).to_string();
    assert!(!e.contains('\n'));
    assert_eq!(Msg::parse(&e), Ok(Msg::Err("one two".into())));
    for bad in [
        "",
        "READY agent:node-2",
        "READY x\t{",
        "TRANSFERRED x",
        "HELLO",
    ] {
        assert!(Msg::parse(bad).is_err(), "{:?} parsed", bad);
    }
}

/// A VM that can't move live is shut down at once: `NotLive` (and a failed
/// migration found still here) leave it `Stopping` with no shutdown sent,
/// so the next stop pass sends it instead of waiting out the timeout.
#[test]
fn vm_moving_cold_is_shut_down_at_once() {
    use vms::migrate::{Failure, Unsettled, Whereabouts};
    use vms::state::{self, StopStep, Stopping, VmState};
    let timeout = Duration::from_secs(120);
    let now = std::time::Instant::now();
    let unsettled = || Unsettled {
        to: "node-2".into(),
        volumes: vec![vol()],
        why: "lost".into(),
        told: false,
        recovered: false,
    };
    for st in [
        state::finished(
            "node-2".into(),
            vec![vol()],
            Err(Failure::NotLive("no shared CPU".into())),
        ),
        state::settled(unsettled(), Whereabouts::Here),
        state::settled(unsettled(), Whereabouts::Neither),
    ] {
        let VmState::Stopping(s) = st else {
            panic!("not stopping");
        };
        assert_eq!(s, Stopping::cold("node-2".into()));
        assert_eq!(
            state::stop_step(s.shutdown_sent, now, timeout),
            StopStep::Shutdown
        );
    }
    // Sent: it gets the timeout, then it's forced off.
    let sent = Some(now);
    assert_eq!(
        state::stop_step(sent, now + timeout / 2, timeout),
        StopStep::Wait
    );
    assert_eq!(
        state::stop_step(sent, now + timeout + Duration::from_secs(1), timeout),
        StopStep::ForceOff
    );
}

/// Where a migration leaves a VM, and the cold move's memo.
#[tokio::test]
async fn vm_state_transitions() {
    use vms::migrate::{self, Failure, Unsettled, Whereabouts};
    use vms::state::{self, Stopping, VmState};
    let u = Unsettled {
        to: "node-2".into(),
        volumes: vec![vol()],
        why: "lost".into(),
        told: false,
        recovered: false,
    };
    // Done: its volumes are handed over, starting with this node detaching.
    let VmState::HandingOffVolumes(m) = state::finished("node-2".into(), vec![vol()], Ok(()))
    else {
        panic!("not handing off");
    };
    assert_eq!(
        (m.to.as_str(), m.volumes, m.detached),
        ("node-2", vec![vol()], false)
    );
    // Failed: unsettled until where the guest runs is known.
    let st = state::finished(
        "node-2".into(),
        vec![vol()],
        Err(Failure::Failed("lost".into())),
    );
    assert!(matches!(&st, VmState::Unsettled(x) if x.why == "lost" && !x.told));
    assert!(st.leaving());
    // B can't be asked: still unsettled, and the operator was told.
    let st = state::settled(u, Whereabouts::Unknown);
    let VmState::Unsettled(u) = st else {
        panic!("settled");
    };
    assert!(u.told);
    // It switched over after all: handed over like a success.
    let st = state::settled(u, Whereabouts::There);
    assert!(matches!(&st, VmState::HandingOffVolumes(m) if !m.detached));
    assert!(st.leaving());
    // A migration under way is leaving; a stop isn't.
    let running = migrate::Migration {
        to: "node-2".into(),
        volumes: vec![vol()],
        started: std::time::Instant::now(),
        task: tokio::spawn(async { Ok(()) }),
    };
    assert!(VmState::Migrating(running).leaving());
    assert!(!VmState::Stopping(Stopping::default()).leaving());
    // The memo lasts while the anchor is asked to move there.
    let to = || Some("node-2".to_string());
    assert_eq!(state::keep_cold_to(to(), Some("node-2")), to());
    assert_eq!(state::keep_cold_to(to(), Some("node-3")), None);
    assert_eq!(state::keep_cold_to(to(), None), None);
    assert!(Stopping::default().idle());
    assert!(!Stopping::cold("node-2".into()).idle());
}

fn record(from: &str, to: &str) -> crate::cluster::state::MigrationRecord {
    crate::cluster::state::MigrationRecord {
        from: from.into(),
        to: to.into(),
        volumes: vec![vol()],
        since_ms: 0,
    }
}

/// Who finishes a recorded migration (I-29): its source once it has no
/// job for it, its target only once the source is gone, nobody else.
#[test]
fn recorded_migrations_are_finished_by_their_source_or_a_gone_ones_target() {
    use vms::state::{Recovery, recovery};
    let rec = record("node-0", "node-1");
    let live = |ns: &[&str]| {
        ns.iter()
            .map(|n| n.to_string())
            .collect::<BTreeSet<String>>()
    };
    let both = live(&["node-0", "node-1", "node-2"]);
    // The source: its job (or handoff, or stop) runs; or it restarted.
    assert_eq!(recovery(&rec, "node-0", &both, true), Recovery::Nothing);
    assert_eq!(recovery(&rec, "node-0", &both, false), Recovery::Resettle);
    // The target: never while the source is live (it may be under way).
    assert_eq!(recovery(&rec, "node-1", &both, false), Recovery::Nothing);
    let gone = live(&["node-1", "node-2"]);
    assert_eq!(recovery(&rec, "node-1", &gone, false), Recovery::StandIn);
    // Anyone else: never.
    assert_eq!(recovery(&rec, "node-2", &gone, false), Recovery::Nothing);
}

/// A target finishing a gone source's migration: the source detached
/// first, then (its leases lapsed) the target, then the record.
#[test]
fn a_stand_in_detaches_the_source_then_itself() {
    use vms::state::{StandInStep, stand_in_step};
    let rec = record("node-0", "node-1");
    let at = |ns: &'static [&'static str]| {
        move |_: &DatasetName| -> Vec<String> { ns.iter().map(|n| n.to_string()).collect() }
    };
    let writer = |n: Option<&'static str>| move |_: &DatasetName| n.map(str::to_string);
    assert_eq!(
        stand_in_step(
            &rec,
            "node-1",
            at(&["node-0", "node-1"]),
            writer(Some("node-0"))
        ),
        StandInStep::Detach("node-0".into(), vec![vol()])
    );
    // Its `ds/` lease hasn't lapsed yet.
    assert_eq!(
        stand_in_step(&rec, "node-1", at(&["node-1"]), writer(Some("node-0"))),
        StandInStep::Wait
    );
    for w in [None, Some("node-1")] {
        assert_eq!(
            stand_in_step(&rec, "node-1", at(&["node-1"]), writer(w)),
            StandInStep::Detach("node-1".into(), vec![vol()])
        );
    }
    assert_eq!(
        stand_in_step(&rec, "node-1", at(&[]), writer(None)),
        StandInStep::End
    );
}

/// The safety net picks a running VM's attached volumes, unless a
/// migration of it is recorded.
#[test]
fn the_safety_net_skips_recorded_migrations() {
    use vms::state::leaked;
    let other = DatasetName::from_static("other");
    let vols = vec![vol(), other.clone()];
    let attached = |v: &DatasetName| *v == other;
    assert_eq!(leaked(&vols, false, attached), vec![other.clone()]);
    assert!(leaked(&vols, true, attached).is_empty());
    assert!(leaked(&vols, false, |_| false).is_empty());
}
