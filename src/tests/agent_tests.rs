use super::*;

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
    let slots = [("kept", false), ("lapsed", true)];
    let drop = |w| adopted_to_drop(slots.iter().copied(), w);
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
    use crate::vm::{DomMeta, DomOwner, DomState, Domain, LocalVm};
    use crate::vms_api::LocalView;
    let http = reqwest::Client::new();
    let vm = |name: &str| LocalVm {
        domain: Domain {
            name: name.into(),
            id: Some(1),
            state: DomState::Running,
        },
        meta: DomMeta {
            anchor: name.into(),
            volumes: vec![name.into()],
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
