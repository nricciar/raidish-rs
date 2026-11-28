//! VM management (see `crate::vm`), served by every `listen` node.
//!
//! Nothing here is remembered: what a VM is comes from its definition (see
//! `crate::vm_store`, through the committer),
//! where it runs from its anchor volume's lease, whether it should run from
//! the cluster state, and what it's doing from libvirt on that node, asked
//! each time.
//!
//! * `GET /api/v1/vms`: every VM, joined with its node's live state.
//! * `GET /api/v1/vms/<name>/definition`: its XML and change number.
//! * `PUT /api/v1/vms/<name>[?create=true][&change=N]` (the XML as the
//!   body): define or change it; checked as it's stored.
//! * `DELETE /api/v1/vms/<name>[?change=N]`: delete its definition (and
//!   stop it); its volumes stay.
//! * `POST /api/v1/vms/<name>/start` / `stop`: should (not) run; the node
//!   with its disks acts on it.
//! * `POST /api/v1/vms/<name>/move` (`{"node"}`): move its disks, and so the
//!   VM: migrated live if both nodes can (see `crate::agent`'s
//!   migrations), else shut down here and started there.
//! * `POST /api/v1/vms/<name>/<reboot|reset|restart|kill>`: on its node.
//!   `restart` shuts it down and lets it start again from the current
//!   XML; `kill` stops it at once (destroy).
//! * `GET /api/v1/vms/<name>/console` (websocket): its VNC console, RFB
//!   bytes in binary frames (what noVNC speaks). Any node: a node without
//!   the VM passes the connection on to the one with it.
//! * `GET /api/v1/vm/local`, `POST /api/v1/vm/local/<name>/<action>`: this
//!   node's libvirt, for the above.
//! * `POST /api/v1/vm/incoming/<name>` (nodes only): ready for VM `name`
//!   to migrate here? 409 until every one of its volumes is exported here
//!   (proxied to the node it runs on), then how to reach this node; 422 if
//!   it never will be (no VMs here).

use crate::api_error::ApiError;
pub use crate::api_types::{CheckView, FactsView, MoveTo, VmView};
use crate::cluster::{Cluster, LeaseOp};
use crate::committer::PoolAdmin;
use crate::committer::PutVm;
use crate::committer::dataset_resource;
use crate::events::{EventKind, Subject};
use crate::types::VmName;
use crate::vm::{VmRuntime, VmSpec};
use crate::vm_store::{DefSpec, VmDef};
use crate::volumes_api::VolumesState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRef, Path, State};
use axum::http::StatusCode as Status;
use axum::response::Response;
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use raidish_auth::http::{Json, Query};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// This node's VM runtime, if it runs VMs.
pub struct VmsState {
    pub runtime: Option<Arc<VmRuntime>>,
    pub http: reqwest::Client,
    /// This node's facts (`crate::node_facts`).
    pub facts: crate::node_facts::FactsHandle,
}

impl VmsState {
    pub fn new(runtime: Option<Arc<VmRuntime>>, facts: crate::node_facts::FactsHandle) -> Self {
        VmsState {
            runtime,
            facts,
            http: crate::tls::http_client(Some(Duration::from_secs(5))).expect("http client"),
        }
    }
}

pub use crate::vm::LocalView;

/// The node holding VM `spec`'s anchor volume (where it runs, or would).
fn vm_node(cluster: &Cluster, spec: &VmSpec) -> Option<String> {
    holder(cluster, spec.anchor()?)
}

fn holder(cluster: &Cluster, volume: &str) -> Option<String> {
    let now = crate::cluster::now_ms();
    cluster
        .lease_state(&dataset_resource(volume))
        .filter(|l| l.is_live(now))
        .and_then(|l| l.holder.node)
}

/// A node's base URL; None: this node.
fn peer_url(cluster: &Cluster, node: &str) -> Result<Option<String>, ApiError> {
    if node == cluster.name {
        return Ok(None);
    }
    cluster
        .members
        .peer_url(node)
        .map(|u| Some(u.to_string()))
        .ok_or_else(|| ApiError::new(Status::BAD_GATEWAY, format!("no address for {}", node)))
}

async fn local_view(
    st: &VmsState,
    cluster: &Cluster,
    agent: &crate::agent::AgentState,
) -> LocalView {
    let mut view = LocalView {
        node: cluster.name.clone(),
        enabled: st.runtime.is_some(),
        errors: agent.read().unwrap().vm_errors.clone(),
        ..Default::default()
    };
    if let Some(rt) = &st.runtime {
        match rt.local_vms().await {
            Ok(vms) => view.vms = vms.into_iter().filter(|v| v.id.is_some()).collect(),
            Err(e) => view.error = Some(e),
        }
    }
    view
}

async fn local(
    _auth: crate::auth::CanRead,
    State(st): State<Arc<VmsState>>,
    State(cluster): State<Arc<Cluster>>,
    State(agent): State<crate::agent::AgentState>,
) -> Json<LocalView> {
    Json(local_view(&st, &cluster, &agent).await)
}

/// Do `action` to this node's domain `name` (`-` with `poke`).
async fn local_do(st: &VmsState, name: &str, action: &str) -> Result<(), ApiError> {
    let rt = st
        .runtime
        .as_ref()
        .ok_or_else(|| ApiError::new(Status::NOT_FOUND, "this node runs no VMs".to_string()))?;
    if action == "poke" {
        rt.poke();
        return Ok(());
    }
    let name = &vm(name)?;
    // Only a domain this node started (see `crate::vm::DomMeta::pool`).
    let ours = match (rt.ident(), rt.virsh.meta(name).await) {
        (Some(me), Ok(Some(m))) => me.owns(&m),
        (_, Err(e)) => return Err(ApiError::new(Status::SERVICE_UNAVAILABLE, e.to_string())),
        _ => false,
    };
    if !ours {
        return Err(ApiError::new(
            Status::NOT_FOUND,
            format!("no VM '{}' runs here", name),
        ));
    }
    let r = match action {
        "reboot" => rt.virsh.reboot(name).await,
        "reset" => rt.virsh.reset(name).await,
        // The agent shuts it down (destroying it after the timeout) and
        // starts it again from the current XML.
        "restart" => {
            rt.restart(name);
            Ok(())
        }
        "kill" => rt.virsh.destroy(name).await,
        a => return Err(ApiError::bad(format!("unknown action '{}'", a))),
    };
    rt.poke();
    r.map_err(ApiError::from)
}

async fn local_action(
    _auth: crate::auth::CanWrite,
    Path((name, action)): Path<(String, String)>,
    State(st): State<Arc<VmsState>>,
) -> Result<Json<()>, ApiError> {
    local_do(&st, &name, &action).await.map(Json)
}

/// Do `action` to `name` on `node`, here or there.
async fn on_node(
    st: &VmsState,
    cluster: &Cluster,
    node: &str,
    name: &str,
    action: &str,
) -> Result<(), ApiError> {
    let Some(base) = peer_url(cluster, node)? else {
        return local_do(st, name, action).await;
    };
    let resp = st
        .http
        .post(format!("{}/api/v1/vm/local/{}/{}", base, name, action))
        .send()
        .await
        .map_err(|e| ApiError::new(Status::BAD_GATEWAY, format!("{}: {}", node, e)))?;
    if resp.status().is_success() {
        return Ok(());
    }
    let code = Status::from_u16(resp.status().as_u16()).unwrap_or(Status::BAD_GATEWAY);
    Err(ApiError::new(code, resp.text().await.unwrap_or_default()))
}

async fn list(
    _auth: crate::auth::CanRead,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
    State(cluster): State<Arc<Cluster>>,
    State(agent): State<crate::agent::AgentState>,
) -> Result<Json<Vec<VmView>>, ApiError> {
    let (st, cluster) = (&*st, &*cluster);
    let specs = specs(&volumes).await?;
    let intent = cluster.status().state.vms;
    // Ask each node with a VM's disks once.
    let mut views: BTreeMap<String, Result<LocalView, String>> = BTreeMap::new();
    for spec in specs.values() {
        let Some(node) = vm_node(cluster, spec) else {
            continue;
        };
        if views.contains_key(&node) {
            continue;
        }
        let view = match peer_url(cluster, &node) {
            Ok(None) => Ok(local_view(st, cluster, &agent).await),
            Ok(Some(base)) => {
                async {
                    st.http
                        .get(format!("{}/api/v1/vm/local", base))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?
                        .error_for_status()
                        .map_err(|e| e.to_string())?
                        .json::<LocalView>()
                        .await
                        .map_err(|e| e.to_string())
                }
                .await
            }
            Err(e) => Err(e.msg),
        };
        views.insert(node, view);
    }
    let out = specs
        .into_values()
        .map(|spec| {
            let node = vm_node(cluster, &spec);
            let mut v = VmView {
                want_running: intent.contains_key(spec.name.as_str()),
                node: node.clone(),
                state: None,
                unreachable: None,
                restart_pending: false,
                vcpus: None,
                memory: None,
                cpu_s: None,
                console: false,
                start_error: None,
                spec,
            };
            match node.as_ref().and_then(|n| views.get(n)) {
                Some(Ok(view)) => {
                    if let Some(l) = view.vms.iter().find(|l| l.name == v.spec.name) {
                        v.state = Some(l.state.clone());
                        v.restart_pending = l.meta.change != v.spec.change;
                        v.vcpus = l.vcpus;
                        v.memory = l.memory;
                        v.cpu_s = l.cpu_s;
                        v.console = l.vnc_port.is_some();
                    }
                    v.start_error = view.errors.get(v.spec.name.as_str()).cloned().or_else(|| {
                        (!view.enabled && v.want_running)
                            .then(|| format!("{} runs no VMs (no `vm:` config)", view.node))
                    });
                    if let Some(e) = &view.error {
                        v.unreachable = Some(e.clone());
                    }
                }
                Some(Err(e)) => v.unreachable = Some(e.clone()),
                None => {}
            }
            v
        })
        .collect();
    Ok(Json(out))
}

/// Every VM definition, as the committer has them (one sharing a volume
/// with an earlier one is unusable, see `crate::vm::mark_conflicts`).
async fn specs(volumes: &VolumesState) -> Result<BTreeMap<VmName, VmSpec>, ApiError> {
    let mut specs: BTreeMap<VmName, VmSpec> = volumes
        .admin
        .vms()
        .await?
        .into_iter()
        .map(|(n, d)| (n, d.spec()))
        .collect();
    crate::vm::mark_conflicts(&mut specs);
    Ok(specs)
}

async fn spec(volumes: &VolumesState, name: &VmName) -> Result<VmSpec, ApiError> {
    specs(volumes)
        .await?
        .remove(name)
        .ok_or_else(|| ApiError::new(Status::NOT_FOUND, format!("no VM '{}'", name)))
}

/// A VM name from the URL (400 if it isn't one).
fn vm(name: &str) -> Result<VmName, ApiError> {
    VmName::try_from(name).map_err(|e| ApiError::bad(e.to_string()))
}

// ---- definitions ------------------------------------------------------------

/// GET /api/v1/vms/<name>/definition: its XML and change number.
async fn definition(
    _auth: crate::auth::CanRead,
    Path(name): Path<String>,
    State(volumes): State<Arc<VolumesState>>,
) -> Result<Json<VmDef>, ApiError> {
    let name = &vm(&name)?;
    volumes
        .admin
        .vms()
        .await?
        .remove(name)
        .map(Json)
        .ok_or_else(|| ApiError::new(Status::NOT_FOUND, format!("no VM '{}'", name)))
}

/// PUT /api/v1/vms/<name>[?create=true][&change=N], the domain XML as the
/// body: store it, once it checks out (see `crate::vm_store`). `create`:
/// refuse if there is one; `change`: refuse unless the one there is that
/// change.
/// `?create=&change=` on a PUT.
#[derive(Deserialize, Default)]
struct PutAt {
    #[serde(default)]
    create: bool,
    change: Option<u64>,
}

async fn put_definition(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    Query(PutAt { create, change }): Query<PutAt>,
    State(volumes): State<Arc<VolumesState>>,
    body: axum::body::Body,
) -> Result<Json<VmDef>, ApiError> {
    let name = &vm(&name)?;
    let bytes = raidish_auth::http::read_body(body, crate::vm_store::MAX_XML)
        .await
        .map_err(|(s, m)| {
            if s == Status::PAYLOAD_TOO_LARGE {
                ApiError::new(
                    s,
                    format!(
                        "a VM definition is at most {} bytes",
                        crate::vm_store::MAX_XML
                    ),
                )
            } else {
                ApiError::new(s, m)
            }
        })?;
    let xml = String::from_utf8(bytes.to_vec()).map_err(|e| ApiError::bad(e.to_string()))?;
    let req = PutVm {
        name: name.clone(),
        xml,
        create,
        change,
    };
    volumes
        .admin
        .on_behalf(&auth.0)
        .put_vm(&req)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

/// DELETE /api/v1/vms/<name>[?change=N]: delete its definition (its
/// volumes stay). A running VM is stopped by its node, as for any VM that
/// has no definition.
/// `?change=` on a DELETE.
#[derive(Deserialize, Default)]
struct ChangeAt {
    change: Option<u64>,
}

async fn delete_definition(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    Query(ChangeAt { change }): Query<ChangeAt>,
    State(st): State<Arc<VmsState>>,
    State(cluster): State<Arc<Cluster>>,
    State(volumes): State<Arc<VolumesState>>,
) -> Result<Json<()>, ApiError> {
    let name = &vm(&name)?;
    let spec = spec(&volumes, name).await.ok();
    volumes
        .admin
        .on_behalf(&auth.0)
        .delete_vm(name, change)
        .await?;
    // Nothing should run it any more.
    set_intent(&cluster, name, false).await?;
    if let Some(spec) = spec {
        poke(&st, &cluster, &spec).await;
    }
    Ok(Json(()))
}

async fn set_intent(cluster: &Cluster, name: &VmName, running: bool) -> Result<(), ApiError> {
    let op = LeaseOp::SetVm {
        name: name.to_string(),
        running,
    };
    cluster
        .lease(&op, false)
        .await
        .map(|_| ())
        .map_err(|e| ApiError::new(Status::SERVICE_UNAVAILABLE, e))
}

/// Tell the VM's node to look now (best effort: it looks every tick anyway).
async fn poke(st: &VmsState, cluster: &Cluster, spec: &VmSpec) {
    if let Some(node) = vm_node(cluster, spec) {
        let _ = on_node(st, cluster, &node, "-", "poke").await;
    }
}

// ---- can it move there (MIG-4) --------------------------------------------

/// The nodes whose `node/` lease is live.
fn live_nodes(cluster: &Cluster) -> std::collections::BTreeSet<String> {
    let now = crate::cluster::now_ms();
    cluster
        .status()
        .state
        .live_nodes(now)
        .map(|(n, _)| n.to_string())
        .collect()
}

async fn nodes_facts(
    _auth: crate::auth::CanRead,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
    State(cluster): State<Arc<Cluster>>,
) -> Result<Json<FactsView>, ApiError> {
    let mut nodes = volumes.admin.node_facts().await?;
    // This node's own, as fresh as it has them.
    if let Some(f) = st.facts.latest() {
        nodes.insert(f.node.clone(), f);
    }
    Ok(Json(FactsView {
        nodes,
        live: live_nodes(&cluster).into_iter().collect(),
    }))
}

/// POST /api/v1/vm/check (nodes only): what this node says now about
/// running a VM with that CPU (see `crate::vm_check::Live`).
async fn check_here(
    _node: crate::auth::IsNode,
    State(st): State<Arc<VmsState>>,
    ask: Json<crate::vm_check::LiveAsk>,
) -> Json<crate::vm_check::Live> {
    let cpu = match (&st.runtime, &ask.cpu) {
        (Some(rt), Some(cpu)) => Some(rt.virsh.cpu_compare(cpu).await.map_err(String::from)),
        _ => None,
    };
    Json(crate::vm_check::Live {
        cpu,
        available: Some(crate::node_facts::available_memory()),
    })
}

/// What the node running a VM says about moving it (`POST
/// /api/v1/vm/probe/<node>?vm=`).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SourceNow {
    /// Its facts, its libvirt TLS to `node` probed now.
    pub facts: Option<crate::node_facts::NodeFacts>,
    /// The VM's domain as it runs (its CPU as the guest has it, see
    /// `vm_check::judged_domain`).
    pub domain: Option<String>,
}

/// `?vm=` on a probe.
#[derive(Deserialize, Default)]
struct VmAt {
    vm: Option<String>,
}

/// POST /api/v1/vm/probe/<node>[?vm=] (nodes only): see `SourceNow`.
async fn probe_now(
    _n: crate::auth::IsNode,
    Path(node): Path<String>,
    Query(VmAt { vm }): Query<VmAt>,
    State(st): State<Arc<VmsState>>,
) -> Json<SourceNow> {
    Json(source_now(&st, &node, vm.as_deref()).await)
}

async fn source_now(st: &VmsState, to: &str, vm: Option<&str>) -> SourceNow {
    let domain = match (&st.runtime, vm) {
        (Some(rt), Some(vm)) => rt.virsh.dumpxml_judged(vm).await.ok(),
        _ => None,
    };
    SourceNow {
        facts: st.facts.with_fresh_probe(to).await,
        domain,
    }
}

/// The source's say (see `SourceNow`) about moving `vm` to `to`.
async fn fresh_source(
    st: &VmsState,
    cluster: &Cluster,
    source: &str,
    to: &str,
    vm: &str,
) -> SourceNow {
    match peer_url(cluster, source) {
        Ok(None) => source_now(st, to, Some(vm)).await,
        Ok(Some(url)) => async {
            st.http
                .post(format!("{}/api/v1/vm/probe/{}?vm={}", url, to, vm))
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .ok()?
                .json::<SourceNow>()
                .await
                .ok()
        }
        .await
        .unwrap_or_default(),
        Err(_) => SourceNow::default(),
    }
}

async fn check_vm(
    st: &VmsState,
    cluster: &Cluster,
    volumes: &VolumesState,
    name: &VmName,
    only: Option<&str>,
) -> Result<CheckView, ApiError> {
    use crate::vm_check::{Mode, check, cpu_to_compare, needs};
    let spec = spec(volumes, name).await?;
    if let Some(e) = spec.error() {
        return Err(ApiError::bad(e.to_string()));
    }
    let needs = needs(&spec.xml)?;
    let mut facts = volumes.admin.node_facts().await?;
    let live = live_nodes(cluster);
    let holder = vm_node(cluster, &spec);
    let running_on = holder.filter(|_| cluster.status().state.vms.contains_key(name.as_str()));
    let source = running_on.as_ref().and_then(|n| facts.get(n)).cloned();
    let mut names: std::collections::BTreeSet<String> = facts.keys().cloned().collect();
    names.insert(cluster.name.clone());
    names.extend(cluster.members.peers.keys().map(|id| id.name()));
    let mut verdicts = Vec::new();
    for node in names {
        if only.is_some_and(|o| o != node) || running_on.as_deref() == Some(node.as_str()) {
            continue;
        }
        let target = facts.remove(&node);
        let up = live.contains(&node);
        // The source's say, now: reaching this node's libvirt, and the VM
        // as it runs (judged by that, as the migration will be).
        let now = match (&running_on, up) {
            (Some(s), true) => fresh_source(st, cluster, s, &node, name).await,
            _ => SourceNow::default(),
        };
        let src = now.facts.or_else(|| source.clone());
        let needs = now
            .domain
            .as_deref()
            .and_then(|x| crate::vm_check::needs(x).ok())
            .unwrap_or_else(|| needs.clone());
        let cpu = cpu_to_compare(&needs, src.as_ref());
        let live_ans = if !up {
            None
        } else {
            match peer_url(cluster, &node) {
                Ok(None) => Some(crate::vm_check::Live {
                    cpu: match (&st.runtime, &cpu) {
                        (Some(rt), Some(c)) => {
                            Some(rt.virsh.cpu_compare(c).await.map_err(String::from))
                        }
                        _ => None,
                    },
                    available: Some(crate::node_facts::available_memory()),
                }),
                Ok(Some(url)) => crate::agent::migrate_ask_live(&st.http, &url, cpu.clone()).await,
                Err(_) => None,
            }
        };
        let mut v = check(
            &needs,
            &node,
            src.as_ref(),
            target.as_ref(),
            live_ans.as_ref(),
        );
        if !up {
            v.mode = Mode::No;
            v.reasons.insert(
                0,
                crate::vm_check::Reason {
                    level: crate::vm_check::ReasonLevel::No,
                    text: format!("{} is down", node),
                },
            );
        }
        verdicts.push(v);
    }
    if let Some(o) = only {
        if verdicts.is_empty() && running_on.as_deref() != Some(o) {
            return Err(ApiError::new(Status::NOT_FOUND, format!("no node '{}'", o)));
        }
    }
    Ok(CheckView {
        vm: name.to_string(),
        running_on,
        verdicts,
    })
}

/// `?nodes=a,b`.
#[derive(Deserialize, Default)]
struct NodesAt {
    nodes: Option<String>,
}

/// GET /api/v1/vm/cpu-baseline[?nodes=a,b]: the CPU every one of those
/// nodes (default: all that run VMs) can give a guest, migratable, as a
/// `<cpu>` for a domain's XML. Worked out by this node's libvirt from the
/// nodes' stored domcapabilities.
async fn cpu_baseline(
    _auth: crate::auth::CanRead,
    Query(NodesAt { nodes }): Query<NodesAt>,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
) -> Result<String, ApiError> {
    let nodes = nodes.as_deref();
    let rt = st.runtime.as_ref().ok_or_else(|| {
        // Another node may: the CLI tries the next on a 5xx.
        ApiError::new(
            Status::SERVICE_UNAVAILABLE,
            "this node runs no VMs (no libvirt to work it out): ask one that does".to_string(),
        )
    })?;
    let facts = volumes.admin.node_facts().await?;
    let want: Option<Vec<&str>> = nodes.map(|n| n.split(',').map(str::trim).collect());
    let mut caps = Vec::new();
    let mut used = Vec::new();
    for (n, f) in &facts {
        if want.as_ref().is_some_and(|w| !w.contains(&n.as_str())) {
            continue;
        }
        if let Some(l) = f.libvirt.as_ref().filter(|l| !l.domcapabilities.is_empty()) {
            caps.push(l.domcapabilities.clone());
            used.push(n.clone());
        }
    }
    if let Some(w) = &want {
        if let Some(missing) = w.iter().find(|n| !used.iter().any(|u| u == *n)) {
            return Err(ApiError::bad(format!(
                "no libvirt facts from '{}'",
                missing
            )));
        }
    }
    if caps.is_empty() {
        return Err(ApiError::bad(
            "no node has reported its libvirt yet".to_string(),
        ));
    }
    let cpu = rt.virsh.cpu_baseline(&caps).await?;
    Ok(format!(
        "<!-- every one of {} can run this -->\n{}",
        used.join(", "),
        cpu.trim_end()
    ))
}

/// `?node=`.
#[derive(Deserialize, Default)]
struct NodeAt {
    node: Option<String>,
}

async fn check_route(
    _auth: crate::auth::CanRead,
    Path(name): Path<String>,
    Query(NodeAt { node }): Query<NodeAt>,
    State(st): State<Arc<VmsState>>,
    State(cluster): State<Arc<Cluster>>,
    State(volumes): State<Arc<VolumesState>>,
) -> Result<Json<CheckView>, ApiError> {
    let name = &vm(&name)?;
    check_vm(&st, &cluster, &volumes, name, node.as_deref())
        .await
        .map(Json)
}

#[allow(clippy::too_many_arguments)]
async fn move_vm(
    auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    State(events): State<crate::events::EventSink>,
    State(st): State<Arc<VmsState>>,
    State(cluster): State<Arc<Cluster>>,
    State(volumes): State<Arc<VolumesState>>,
    to: Json<MoveTo>,
) -> Result<Json<CheckView>, ApiError> {
    let (st, cluster, volumes) = (&*st, &*cluster, &*volumes);
    let name = &vm(&name)?;
    let spec = spec(volumes, name).await?;
    let Some(anchor) = spec.anchor() else {
        return Err(ApiError::bad(spec.error().unwrap_or_default().to_string()));
    };
    // Where it'd go, and how: refused if it can't run there (or, asked
    // for live, if it can't move live).
    let view = check_vm(st, cluster, volumes, name, Some(&to.node)).await?;
    if let Some(v) = view.verdicts.first() {
        use crate::vm_check::Mode;
        if v.mode == Mode::No {
            return Err(ApiError::new(
                Status::CONFLICT,
                format!("VM '{}' can't run on {}: {}", name, to.node, v.why()),
            ));
        }
        if to.live && view.running_on.is_some() && v.mode != Mode::Live {
            return Err(ApiError::new(
                Status::CONFLICT,
                format!("VM '{}' can't move to {} live: {}", name, to.node, v.why()),
            ));
        }
    }
    let vols = volumes
        .admin
        .volumes()
        .await
        .map_err(|e| ApiError::new(Status::SERVICE_UNAVAILABLE, e.to_string()))?;
    let mut props = vols
        .into_iter()
        .find(|v| &v.name == anchor)
        .ok_or_else(|| ApiError::new(Status::NOT_FOUND, format!("no volume '{}'", anchor)))?
        .props;
    if !props.nodes.is_empty() && !props.nodes.contains(&to.node) {
        props.nodes.insert(0, to.node.clone());
    }
    props.move_to = Some(to.node.clone());
    volumes
        .admin
        .on_behalf(&auth.0)
        .set_props(anchor, &props)
        .await
        .map_err(|e| ApiError::new(Status::SERVICE_UNAVAILABLE, e.to_string()))?;
    events.emit(
        crate::events::Level::Info,
        EventKind::VmMove,
        Subject::vm(name),
        format!("VM '{}' asked to move to {}", name, to.node),
        Some(auth.0.name.clone()),
    );
    poke(st, cluster, &spec).await;
    Ok(Json(view))
}

async fn action(
    auth: crate::auth::CanWrite,
    Path((name, action)): Path<(String, String)>,
    State(events): State<crate::events::EventSink>,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
    State(cluster): State<Arc<Cluster>>,
) -> Result<Json<()>, ApiError> {
    let (st, cluster, action) = (&*st, &*cluster, action.as_str());
    let name = &vm(&name)?;
    let spec = spec(&volumes, name).await?;
    match action {
        "start" => {
            if let Some(e) = spec.error() {
                return Err(ApiError::bad(e.to_string()));
            }
            set_intent(cluster, name, true).await?;
            poke(st, cluster, &spec).await;
        }
        "stop" => {
            set_intent(cluster, name, false).await?;
            poke(st, cluster, &spec).await;
        }
        "kill" => {
            set_intent(cluster, name, false).await?;
            let node = vm_node(cluster, &spec).ok_or_else(|| {
                ApiError::new(Status::CONFLICT, format!("'{}' isn't running", name))
            })?;
            on_node(st, cluster, &node, name, "kill").await?;
        }
        "reboot" | "reset" | "restart" => {
            let node = vm_node(cluster, &spec).ok_or_else(|| {
                ApiError::new(Status::CONFLICT, format!("'{}' isn't running", name))
            })?;
            on_node(st, cluster, &node, name, action).await?;
        }
        a => return Err(ApiError::bad(format!("unknown action '{}'", a))),
    }
    let (kind, what) = match action {
        "start" => (EventKind::VmStart, "asked to start"),
        "stop" => (EventKind::VmStop, "asked to shut down"),
        "kill" => (EventKind::VmKill, "forced off"),
        "reboot" | "restart" => (EventKind::VmReboot, "rebooted"),
        _ => (EventKind::VmReset, "reset"),
    };
    events.emit(
        crate::events::Level::Info,
        kind,
        Subject::vm(name),
        format!("VM '{}' {}", name, what),
        Some(auth.0.name.clone()),
    );
    Ok(Json(()))
}

// ---- console --------------------------------------------------------------

async fn console(
    _auth: crate::auth::CanWrite,
    Path(name): Path<String>,
    peer: Option<axum::Extension<raidish_auth::http::Peer>>,
    ws: WebSocketUpgrade,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
    State(cluster): State<Arc<Cluster>>,
) -> Result<Response, ApiError> {
    let peer = peer.map(|axum::Extension(p)| p);
    let name = &vm(&name)?;
    let spec = spec(&volumes, name).await?;
    let node = vm_node(&cluster, &spec)
        .ok_or_else(|| ApiError::new(Status::CONFLICT, format!("'{}' isn't running", name)))?;
    match peer_url(&cluster, &node)? {
        None => {
            let rt = st.runtime.as_ref().ok_or_else(|| {
                ApiError::new(Status::NOT_FOUND, "this node runs no VMs".to_string())
            })?;
            // Only a domain this node started (as `local_do`).
            let ours = match (rt.ident(), rt.virsh.meta(name).await) {
                (Some(me), Ok(Some(m))) => me.owns(&m),
                (_, Err(e)) => {
                    return Err(ApiError::new(Status::SERVICE_UNAVAILABLE, e.to_string()));
                }
                _ => false,
            };
            if !ours {
                return Err(ApiError::new(
                    Status::CONFLICT,
                    format!("'{}' isn't running here", name),
                ));
            }
            let port = rt.virsh.vnc_port(name).await.ok_or_else(|| {
                ApiError::new(Status::CONFLICT, format!("'{}' has no console", name))
            })?;
            let host = rt.console.clone();
            Ok(ws.on_upgrade(move |s| until_closed(peer, bridge_tcp(s, host, port))))
        }
        Some(base) => {
            let url = format!(
                "{}/api/v1/vms/{}/console",
                base.replacen("https://", "wss://", 1)
                    .replacen("http://", "ws://", 1),
                name
            );
            Ok(ws.on_upgrade(move |s| until_closed(peer, bridge_ws(s, url))))
        }
    }
}

/// Run a console bridge until it ends, or until its peer's connections
/// are closed (its certificate revoked: `raidish_auth::http::Connections`).
async fn until_closed(
    peer: Option<raidish_auth::http::Peer>,
    bridge: impl std::future::Future<Output = std::io::Result<()>>,
) {
    let closed = async {
        match &peer {
            Some(p) => p.closed().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        r = bridge => if let Err(e) = r {
            eprintln!("console: {}", e);
        },
        _ = closed => {}
    }
}

fn ws_err(e: axum::Error) -> std::io::Error {
    std::io::Error::other(e)
}

/// Browser ⇄ the VNC server on `host`:`port`.
async fn bridge_tcp(stream: WebSocket, host: String, port: u16) -> std::io::Result<()> {
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
    tcp.set_nodelay(true)?;
    let (mut rd, mut wr) = tcp.into_split();
    let (mut sink, mut source) = stream.split();
    let up = async {
        while let Some(m) = source.next().await {
            match m.map_err(ws_err)? {
                Message::Binary(b) => wr.write_all(&b).await?,
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<_, std::io::Error>(())
    };
    let down = async {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = rd.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            sink.send(Message::Binary(buf[..n].to_vec().into()))
                .await
                .map_err(ws_err)?;
        }
        let _ = sink.send(Message::Close(None)).await;
        Ok::<_, std::io::Error>(())
    };
    tokio::select! {
        r = up => r,
        r = down => r,
    }
}

/// Browser ⇄ the console on the node that runs the VM (as this node).
async fn bridge_ws(stream: WebSocket, url: String) -> std::io::Result<()> {
    use tokio_tungstenite::tungstenite::Message as M;
    let (peer, _) = tokio_tungstenite::connect_async_tls_with_config(
        url.as_str(),
        None,
        false,
        crate::tls::ws_connector(),
    )
    .await
    .map_err(|e| std::io::Error::other(format!("{}: {}", url, e)))?;
    let (mut psink, mut psource) = peer.split();
    let (mut sink, mut source) = stream.split();
    let up = async {
        while let Some(m) = source.next().await {
            match m.map_err(ws_err)? {
                Message::Binary(b) => {
                    psink
                        .send(M::Binary(b))
                        .await
                        .map_err(std::io::Error::other)?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = psink.send(M::Close(None)).await;
        Ok::<_, std::io::Error>(())
    };
    let down = async {
        while let Some(m) = psource.next().await {
            match m.map_err(std::io::Error::other)? {
                M::Binary(b) => sink.send(Message::Binary(b)).await.map_err(ws_err)?,
                M::Close(_) => break,
                _ => {}
            }
        }
        let _ = sink.send(Message::Close(None)).await;
        Ok::<_, std::io::Error>(())
    };
    tokio::select! {
        r = up => r,
        r = down => r,
    }
}

/// Is this node ready for VM `name` to migrate here (see the module doc)?
async fn incoming(
    _node: crate::auth::IsNode,
    Path(name): Path<String>,
    State(st): State<Arc<VmsState>>,
    State(volumes): State<Arc<VolumesState>>,
    State(agent): State<crate::agent::AgentState>,
) -> Result<Json<crate::vm::Incoming>, ApiError> {
    let name = &vm(&name)?;
    let not_yet = |m: String| ApiError::new(Status::CONFLICT, m);
    let never = |m: &str| ApiError::new(Status::UNPROCESSABLE_ENTITY, m.to_string());
    let rt = st
        .runtime
        .as_ref()
        .ok_or_else(|| never("this node runs no VMs"))?;
    if rt.ident().is_none() {
        return Err(not_yet("the pool isn't open here yet".into()));
    }
    let spec = spec(&volumes, name).await?;
    if let Some(e) = spec.error() {
        return Err(ApiError::bad(e.to_string()));
    }
    let proxied: Vec<crate::types::DatasetName> = agent
        .read()
        .unwrap()
        .volumes
        .iter()
        .filter(|v| v.proxied && v.transport == crate::inode::Transport::Nbd)
        .map(|v| v.name.clone())
        .collect();
    let missing: Vec<&crate::types::DatasetName> = spec
        .volumes()
        .iter()
        .filter(|v| !proxied.contains(v))
        .collect();
    if !missing.is_empty() {
        return Err(not_yet(format!(
            "not exported here yet: {}",
            missing
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(Json(crate::vm::Incoming {
        uri: rt.migrate_uri.clone(),
        migrate_host: rt.migrate_host.clone(),
        nbd_host: rt.nbd.0.clone(),
        nbd_port: rt.nbd.1,
        console: rt.console.clone(),
        node: rt.node.clone(),
    }))
}

/// What the VM routes use.
#[derive(Clone)]
pub struct VmsCtx {
    pub vms: Arc<VmsState>,
    pub volumes: Arc<VolumesState>,
    pub cluster: Arc<Cluster>,
    pub agent: crate::agent::AgentState,
    pub events: crate::events::EventSink,
}

impl FromRef<VmsCtx> for Arc<VmsState> {
    fn from_ref(c: &VmsCtx) -> Self {
        c.vms.clone()
    }
}
impl FromRef<VmsCtx> for Arc<VolumesState> {
    fn from_ref(c: &VmsCtx) -> Self {
        c.volumes.clone()
    }
}
impl FromRef<VmsCtx> for Arc<Cluster> {
    fn from_ref(c: &VmsCtx) -> Self {
        c.cluster.clone()
    }
}
impl FromRef<VmsCtx> for crate::agent::AgentState {
    fn from_ref(c: &VmsCtx) -> Self {
        c.agent.clone()
    }
}
impl FromRef<VmsCtx> for crate::events::EventSink {
    fn from_ref(c: &VmsCtx) -> Self {
        c.events.clone()
    }
}

pub fn router(ctx: VmsCtx) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/vm/incoming/{name}", post(incoming))
        .route("/api/v1/vm/check", post(check_here))
        .route("/api/v1/vms/{name}/check", get(check_route))
        .route("/api/v1/vm/probe/{node}", post(probe_now))
        .route("/api/v1/vm/cpu-baseline", get(cpu_baseline))
        .route("/api/v1/nodes/facts", get(nodes_facts))
        .route("/api/v1/vm/local", get(local))
        .route("/api/v1/vm/local/{name}/{action}", post(local_action))
        .route("/api/v1/vms", get(list))
        .route("/api/v1/vms/{name}/definition", get(definition))
        .route(
            "/api/v1/vms/{name}",
            axum::routing::put(put_definition).delete(delete_definition),
        )
        .route("/api/v1/vms/{name}/move", post(move_vm))
        .route("/api/v1/vms/{name}/{action}", post(action))
        .route("/api/v1/vms/{name}/console", get(console))
        .with_state(ctx)
}
