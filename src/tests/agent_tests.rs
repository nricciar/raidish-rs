use super::*;
use crate::test_util::dsn;

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

/// Audit A1: a handed-over volume not adopted yet goes with its own lease,
/// and with the writer's: lost, or gone already (a takeover that finished
/// after the writer was dropped).
#[test]
fn handed_over_volumes_go_with_their_lease_or_the_writers() {
    let slots = [(dsn("kept"), false), (dsn("lapsed"), true)];
    let drop = |w| adopted_to_drop(slots.iter().map(|(n, l)| (n, *l)), w);
    assert_eq!(drop(WriterStanding::Held), vec!["lapsed"]);
    assert_eq!(drop(WriterStanding::Lost), vec!["kept", "lapsed"]);
    assert_eq!(drop(WriterStanding::Gone), vec!["kept", "lapsed"]);
}

/// A6: a failed migration's guest may have switched over before the
/// failure; only where it runs decides what's done, and not knowing
/// leaves it alone (never moved cold, so never started twice).
#[test]
fn a_failed_migration_is_settled_by_where_its_guest_runs() {
    use migrate::{Whereabouts::*, whereabouts};
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
    use crate::vm::{DomMeta, DomOwner, DomState, LocalVm};
    use crate::vms_api::LocalView;
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
    assert_eq!(migrate::runs_there(&http, &url, "web").await, Some(true));
    let url = answer_once("200 OK", view(vec![vm("db")], None)).await;
    assert_eq!(migrate::runs_there(&http, &url, "web").await, Some(false));
    // Its libvirt couldn't be asked: not "it doesn't run there".
    let url = answer_once("200 OK", view(vec![], Some("libvirt down"))).await;
    assert_eq!(migrate::runs_there(&http, &url, "web").await, None);
    let url = answer_once("503 Service Unavailable", "{}".into()).await;
    assert_eq!(migrate::runs_there(&http, &url, "web").await, None);
    // Nobody there.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    drop(l);
    assert_eq!(migrate::runs_there(&http, &url, "web").await, None);
}

// ---- handoff slots (structural triage I-27) ----

use crate::export::BlockExporter;

struct Node {
    _pool: crate::test_util::TestPool,
    parts: handoff::WriterParts,
    shared: Arc<handoff::Shared>,
    pki: raidish_auth::testing::Pki,
}

/// A standalone pool with volume "vol", its NBD and NVMe servers (not
/// listening) as this node's writer.
async fn node() -> Node {
    let pool = crate::test_util::TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.create_block(&dsn("vol"), 16 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let parts = handoff::WriterParts {
        nbd: Arc::new(NbdServer::with_bind(fs.clone(), "127.0.0.1", 0)),
        nvme: Arc::new(NvmeTcpServer::with_bind(fs.clone(), "127.0.0.1", 0)),
        fs,
    };
    let shared = Arc::new(handoff::Shared::new(LeaseClient::new(vec![])));
    *shared.writer.lock().unwrap() = Some(parts.clone());
    Node {
        _pool: pool,
        parts,
        shared,
        pki: raidish_auth::testing::Pki::new(vec![]),
    }
}

impl Node {
    /// "vol" exported over `t`, proxied to a writer that's nowhere.
    fn proxy(&self, t: Transport) -> handoff::Proxy {
        let connector =
            tokio_rustls::TlsConnector::from(Arc::new(self.pki.tls.client_config().unwrap()));
        let nowhere = || None::<(String, u16, crate::types::Epoch)>;
        let peer = Arc::new(
            crate::peer_io::PeerIo::new(&dsn("vol"), connector, Arc::new(nowhere))
                .retry_for(Duration::from_millis(200)),
        );
        let exporter = self.parts.exporter(t);
        exporter
            .add_export_io(&dsn("vol"), 16 << 20, peer.clone())
            .unwrap();
        handoff::Proxy {
            io: exporter.export_io(&dsn("vol")).unwrap(),
            peer,
            transport: t,
            device: None,
        }
    }

    fn slot(&self) -> Option<&'static str> {
        self.shared
            .slots
            .lock()
            .unwrap()
            .get("vol")
            .map(|s| match s {
                handoff::Slot::Proxy(_) => "proxy",
                handoff::Slot::TakingOver => "taking over",
                handoff::Slot::Adopted { .. } => "adopted",
            })
    }
}

/// A takeover that fails puts its proxy back, but only while its claim on
/// the slot stands and its writer is still this node's. Once the writer
/// was dropped meanwhile (`drop_proxies` took the claim), the proxy is
/// dead: it used to be put back, served again to nobody, its export never
/// removed. Now it's taken down instead; and a slot made since is left be.
#[tokio::test]
async fn a_failed_takeover_puts_back_only_a_proxy_whose_writer_is_still_here() {
    let n = node().await;
    let set = |s: handoff::Slot| {
        n.shared.slots.lock().unwrap().insert(dsn("vol"), s);
    };

    // Writer still here, claim standing: put back.
    let p = n.proxy(Transport::Nbd);
    set(handoff::Slot::TakingOver);
    n.shared.restore(&dsn("vol"), Some(p), &n.parts).await;
    assert_eq!(n.slot(), Some("proxy"));
    assert!(n.parts.nbd.export_io(&dsn("vol")).is_some());

    // The writer dropped while the takeover ran: taken down, not back.
    let Some(handoff::Slot::Proxy(p)) = n.shared.slots.lock().unwrap().remove("vol") else {
        panic!("no proxy");
    };
    let peer = p.peer.clone();
    set(handoff::Slot::TakingOver);
    *n.shared.writer.lock().unwrap() = None;
    n.shared.slots.lock().unwrap().clear();
    let t = std::time::Instant::now();
    n.shared.restore(&dsn("vol"), Some(p), &n.parts).await;
    // And at once: its last sync doesn't wait for a replacement backend
    // (`REPLACE_WAIT`, 30 s) nor retry the writer that's gone.
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "took {:?}",
        t.elapsed()
    );
    assert_eq!(n.slot(), None, "a dead proxy was put back");
    assert!(
        n.parts.nbd.export_io(&dsn("vol")).is_none(),
        "its export stayed up"
    );
    assert!(crate::volume_io::VolumeIo::retired(&*peer));

    // A slot made since (a new writer's proxy) isn't the claim's to undo.
    *n.shared.writer.lock().unwrap() = Some(n.parts.clone());
    let p2 = n.proxy(Transport::Nvme);
    set(handoff::Slot::Proxy(p2));
    n.shared.restore(&dsn("vol"), None, &n.parts).await;
    assert_eq!(n.slot(), Some("proxy"));
}

/// I-27: asked to take a volume over a transport other than the one it's
/// proxied here over, the receiver refuses (taken over, it would be served
/// over the proxy's while recorded as the other, and its export leak), and
/// the proxy is left as it was.
#[tokio::test]
async fn a_handoff_over_another_transport_than_the_proxys_is_refused() {
    use tokio::io::{AsyncWriteExt, BufReader};
    let n = node().await;
    let p = n.proxy(Transport::Nbd);
    n.shared
        .slots
        .lock()
        .unwrap()
        .insert(dsn("vol"), handoff::Slot::Proxy(p));

    let acceptor =
        tokio_rustls::TlsAcceptor::from(Arc::new(n.pki.tls.server_config(true, &[]).unwrap()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let volumes: Arc<dyn crate::peer_io::PeerVolumes> = n.shared.clone();
    tokio::spawn(crate::peer_io::serve(listener, acceptor, volumes));

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
    assert_eq!(n.slot(), Some("proxy"));
    assert!(n.parts.nbd.export_io(&dsn("vol")).is_some());
}

/// The receiver waits for the transfer longer than the sender may take
/// to get there (after which it gives up instead), with room for the
/// transfer: else the lease could go to an owner that stopped waiting.
#[test]
fn the_receiver_outwaits_the_sender() {
    assert!(handoff::TRANSFER_WAIT >= handoff::SENDER_BUDGET + Duration::from_secs(20));
    // The drain alone takes up to 30 s (`DRAIN_TIMEOUT`).
    assert!(handoff::SENDER_BUDGET > Duration::from_secs(30));
}
