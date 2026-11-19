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
//!   it never will be (no VMs here, or no `vm.migrate_uri`).

use crate::cluster::{Cluster, LeaseOp};
use crate::committer::PutVm;
use crate::committer::dataset_resource;
use crate::vm::{LocalVm, VmRuntime, VmSpec};
use crate::vm_store::VmDef;
use crate::volumes_api::VolumesState;
use rocket::futures::{SinkExt, StreamExt};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{State, delete, get, post, put};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// This node's VM runtime, if it runs VMs.
pub struct VmsState {
    pub runtime: Option<Arc<VmRuntime>>,
    pub http: reqwest::Client,
}

impl VmsState {
    pub fn new(runtime: Option<Arc<VmRuntime>>) -> Self {
        VmsState {
            runtime,
            http: crate::tls::http_builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .build()
                .expect("http client"),
        }
    }
}

type ApiError = (Status, String);

fn bad(m: impl Into<String>) -> ApiError {
    (Status::BadRequest, m.into())
}

/// What `GET /api/v1/vm/local` answers.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct LocalView {
    pub node: String,
    /// Does this node run VMs at all (a `vm:` config section)?
    pub enabled: bool,
    pub vms: Vec<LocalVm>,
    /// Why VMs that should run here don't.
    pub errors: BTreeMap<String, String>,
    /// libvirt couldn't be asked.
    pub error: Option<String>,
}

/// A VM, as `GET /api/v1/vms` shows it.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VmView {
    #[serde(flatten)]
    pub spec: VmSpec,
    /// Should it run (the cluster's intent)?
    pub want_running: bool,
    /// The node holding its anchor volume (where it runs, or would).
    pub node: Option<String>,
    /// libvirt's state on that node; None: not running there.
    pub state: Option<String>,
    /// The node couldn't be asked: `state` is unknown.
    pub unreachable: Option<String>,
    /// Started from an older version of its XML.
    pub restart_pending: bool,
    pub vcpus: Option<u32>,
    pub memory_kib: Option<u64>,
    pub cpu_s: Option<f64>,
    pub console: bool,
    /// Why its node couldn't start it.
    pub start_error: Option<String>,
}

fn holder(cluster: &Cluster, volume: &str) -> Option<String> {
    let now = crate::cluster::now_ms();
    cluster
        .lease_state(&dataset_resource(volume))
        .filter(|l| !l.released && l.expires_ms > now)
        .map(|l| crate::frontend::parse_desc(&l.desc).0)
}

/// A node's base URL; None: this node.
fn peer_url(cluster: &Cluster, node: &str) -> Result<Option<String>, ApiError> {
    if node == cluster.name {
        return Ok(None);
    }
    node.strip_prefix("node-")
        .and_then(|id| id.parse::<u64>().ok())
        .and_then(|id| cluster.members.peers.get(&id))
        .map(|u| Some(u.trim_end_matches('/').to_string()))
        .ok_or_else(|| (Status::BadGateway, format!("no address for {}", node)))
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
            Ok(vms) => view.vms = vms.into_iter().filter(|v| v.domain.id.is_some()).collect(),
            Err(e) => view.error = Some(e),
        }
    }
    view
}

#[get("/api/v1/vm/local")]
async fn local(
    _auth: crate::auth::CanRead,
    st: &State<VmsState>,
    cluster: &State<Arc<Cluster>>,
    agent: &State<crate::agent::AgentState>,
) -> Json<LocalView> {
    Json(local_view(st, cluster, agent).await)
}

/// Do `action` to this node's domain `name` (`-` with `poke`).
async fn local_do(st: &VmsState, name: &str, action: &str) -> Result<(), ApiError> {
    let rt = st
        .runtime
        .as_ref()
        .ok_or_else(|| (Status::NotFound, "this node runs no VMs".to_string()))?;
    if action == "poke" {
        rt.poke.notify_one();
        return Ok(());
    }
    // Only a domain this node started (see `crate::vm::DomMeta::pool`).
    let ours = match (rt.ident(), rt.virsh.meta(name).await) {
        (Some(me), Some(m)) => me.owns(&m),
        _ => false,
    };
    if !ours {
        return Err((Status::NotFound, format!("no VM '{}' runs here", name)));
    }
    let r = match action {
        "reboot" => rt.virsh.reboot(name).await,
        "reset" => rt.virsh.reset(name).await,
        // The agent shuts it down (destroying it after the timeout) and
        // starts it again from the current XML.
        "restart" => {
            rt.restart.lock().unwrap().insert(name.to_string());
            Ok(())
        }
        "kill" => rt.virsh.destroy(name).await,
        a => return Err(bad(format!("unknown action '{}'", a))),
    };
    rt.poke.notify_one();
    r.map_err(|e| (Status::InternalServerError, e))
}

#[post("/api/v1/vm/local/<name>/<action>")]
async fn local_action(
    _auth: crate::auth::CanWrite,
    name: &str,
    action: &str,
    st: &State<VmsState>,
) -> Result<Json<()>, ApiError> {
    local_do(st, name, action).await.map(Json)
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
        .map_err(|e| (Status::BadGateway, format!("{}: {}", node, e)))?;
    if resp.status().is_success() {
        return Ok(());
    }
    let code = Status::from_code(resp.status().as_u16()).unwrap_or(Status::BadGateway);
    Err((code, resp.text().await.unwrap_or_default()))
}

#[get("/api/v1/vms")]
async fn list(
    _auth: crate::auth::CanRead,
    st: &State<VmsState>,
    volumes: &State<VolumesState>,
    cluster: &State<Arc<Cluster>>,
    agent: &State<crate::agent::AgentState>,
) -> Result<Json<Vec<VmView>>, ApiError> {
    let specs = specs(volumes).await?;
    let intent = cluster.status().state.vms;
    // Ask each node with a VM's disks once.
    let mut views: BTreeMap<String, Result<LocalView, String>> = BTreeMap::new();
    for spec in specs.values() {
        let Some(node) = holder(cluster, &spec.anchor) else {
            continue;
        };
        if views.contains_key(&node) {
            continue;
        }
        let view = match peer_url(cluster, &node) {
            Ok(None) => Ok(local_view(st, cluster, agent).await),
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
            Err((_, e)) => Err(e),
        };
        views.insert(node, view);
    }
    let out = specs
        .into_values()
        .map(|spec| {
            let node = if spec.anchor.is_empty() {
                None
            } else {
                holder(cluster, &spec.anchor)
            };
            let mut v = VmView {
                want_running: intent.contains_key(&spec.name),
                node: node.clone(),
                state: None,
                unreachable: None,
                restart_pending: false,
                vcpus: None,
                memory_kib: None,
                cpu_s: None,
                console: false,
                start_error: None,
                spec,
            };
            match node.as_ref().and_then(|n| views.get(n)) {
                Some(Ok(view)) => {
                    if let Some(l) = view.vms.iter().find(|l| l.domain.name == v.spec.name) {
                        v.state = Some(l.domain.state.clone());
                        v.restart_pending = l.meta.change != v.spec.change;
                        v.vcpus = l.vcpus;
                        v.memory_kib = l.memory_kib;
                        v.cpu_s = l.cpu_s;
                        v.console = l.vnc_port.is_some();
                    }
                    v.start_error = view.errors.get(&v.spec.name).cloned().or_else(|| {
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
async fn specs(volumes: &VolumesState) -> Result<BTreeMap<String, VmSpec>, ApiError> {
    let mut specs: BTreeMap<String, VmSpec> = volumes
        .client
        .vms()
        .await
        .map_err(crate::volumes_api::api_err)?
        .into_iter()
        .map(|(n, d)| (n, d.spec()))
        .collect();
    crate::vm::mark_conflicts(&mut specs);
    Ok(specs)
}

async fn spec(volumes: &VolumesState, name: &str) -> Result<VmSpec, ApiError> {
    specs(volumes)
        .await?
        .remove(name)
        .ok_or_else(|| (Status::NotFound, format!("no VM '{}'", name)))
}

// ---- definitions ------------------------------------------------------------

/// GET /api/v1/vms/<name>/definition: its XML and change number.
#[get("/api/v1/vms/<name>/definition")]
async fn definition(
    _auth: crate::auth::CanRead,
    name: &str,
    volumes: &State<VolumesState>,
) -> Result<Json<VmDef>, ApiError> {
    volumes
        .client
        .vms()
        .await
        .map_err(crate::volumes_api::api_err)?
        .remove(name)
        .map(Json)
        .ok_or_else(|| (Status::NotFound, format!("no VM '{}'", name)))
}

/// PUT /api/v1/vms/<name>[?create=true][&change=N], the domain XML as the
/// body: store it, once it checks out (see `crate::vm_store`). `create`:
/// refuse if there is one; `change`: refuse unless the one there is that
/// change.
#[put("/api/v1/vms/<name>?<create>&<change>", data = "<body>")]
async fn put_definition(
    auth: crate::auth::CanWrite,
    name: &str,
    create: Option<bool>,
    change: Option<u64>,
    body: rocket::Data<'_>,
    volumes: &State<VolumesState>,
) -> Result<Json<VmDef>, ApiError> {
    use rocket::data::ToByteUnit;
    let xml = body
        .open((crate::vm_store::MAX_XML + 1).bytes())
        .into_string()
        .await
        .map_err(|e| bad(e.to_string()))?;
    if !xml.is_complete() {
        return Err((
            Status::PayloadTooLarge,
            format!(
                "a VM definition is at most {} bytes",
                crate::vm_store::MAX_XML
            ),
        ));
    }
    let req = PutVm {
        name: name.to_string(),
        xml: xml.into_inner(),
        create: create.unwrap_or(false),
        change,
    };
    volumes
        .client
        .on_behalf(&auth.0)
        .put_vm(&req)
        .await
        .map(Json)
        .map_err(crate::volumes_api::api_err)
}

/// DELETE /api/v1/vms/<name>[?change=N]: delete its definition (its
/// volumes stay). A running VM is stopped by its node, as for any VM that
/// has no definition.
#[delete("/api/v1/vms/<name>?<change>")]
async fn delete_definition(
    auth: crate::auth::CanWrite,
    name: &str,
    change: Option<u64>,
    st: &State<VmsState>,
    cluster: &State<Arc<Cluster>>,
    volumes: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    let spec = spec(volumes, name).await.ok();
    volumes
        .client
        .on_behalf(&auth.0)
        .delete_vm(name, change)
        .await
        .map_err(crate::volumes_api::api_err)?;
    // Nothing should run it any more.
    set_intent(cluster, name, false).await?;
    if let Some(spec) = spec {
        poke(st, cluster, &spec).await;
    }
    Ok(Json(()))
}

async fn set_intent(cluster: &Cluster, name: &str, running: bool) -> Result<(), ApiError> {
    let op = LeaseOp::SetVm {
        name: name.to_string(),
        running,
    };
    cluster
        .lease(&op, false)
        .await
        .map(|_| ())
        .map_err(|e| (Status::ServiceUnavailable, e))
}

/// Tell the VM's node to look now (best effort: it looks every tick anyway).
async fn poke(st: &VmsState, cluster: &Cluster, spec: &VmSpec) {
    if let Some(node) = holder(cluster, &spec.anchor) {
        let _ = on_node(st, cluster, &node, "-", "poke").await;
    }
}

#[derive(Deserialize)]
pub struct MoveTo {
    pub node: String,
}

#[post("/api/v1/vms/<name>/move", data = "<to>")]
#[allow(clippy::too_many_arguments)]
async fn move_vm(
    auth: crate::auth::CanWrite,
    name: &str,
    to: Json<MoveTo>,
    events: &State<crate::events::EventSink>,
    st: &State<VmsState>,
    cluster: &State<Arc<Cluster>>,
    volumes: &State<VolumesState>,
) -> Result<Json<()>, ApiError> {
    let spec = spec(volumes, name).await?;
    if !spec.usable() {
        return Err(bad(spec.error.unwrap_or_default()));
    }
    let vols = volumes
        .client
        .volumes()
        .await
        .map_err(|e| (Status::ServiceUnavailable, e.to_string()))?;
    let mut props = vols
        .into_iter()
        .find(|v| v.name == spec.anchor)
        .ok_or_else(|| (Status::NotFound, format!("no volume '{}'", spec.anchor)))?
        .props;
    if !props.nodes.is_empty() && !props.nodes.contains(&to.node) {
        props.nodes.insert(0, to.node.clone());
    }
    props.move_to = Some(to.node.clone());
    volumes
        .client
        .on_behalf(&auth.0)
        .set_props(&spec.anchor, &props)
        .await
        .map_err(|e| (Status::ServiceUnavailable, e.to_string()))?;
    events.emit(
        crate::events::Level::Info,
        "vm.move",
        format!("vm/{}", name),
        format!("VM '{}' asked to move to {}", name, to.node),
        Some(auth.0.name.clone()),
    );
    poke(st, cluster, &spec).await;
    Ok(Json(()))
}

#[post("/api/v1/vms/<name>/<action>", rank = 2)]
async fn action(
    auth: crate::auth::CanWrite,
    name: &str,
    action: &str,
    events: &State<crate::events::EventSink>,
    st: &State<VmsState>,
    volumes: &State<VolumesState>,
    cluster: &State<Arc<Cluster>>,
) -> Result<Json<()>, ApiError> {
    let spec = spec(volumes, name).await?;
    match action {
        "start" => {
            if let Some(e) = &spec.error {
                return Err(bad(e.clone()));
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
            let node = holder(cluster, &spec.anchor)
                .ok_or_else(|| (Status::Conflict, format!("'{}' isn't running", name)))?;
            on_node(st, cluster, &node, name, "kill").await?;
        }
        "reboot" | "reset" | "restart" => {
            let node = holder(cluster, &spec.anchor)
                .ok_or_else(|| (Status::Conflict, format!("'{}' isn't running", name)))?;
            on_node(st, cluster, &node, name, action).await?;
        }
        a => return Err(bad(format!("unknown action '{}'", a))),
    }
    let what = match action {
        "start" => "asked to start",
        "stop" => "asked to shut down",
        "kill" => "forced off",
        "reboot" | "restart" => "rebooted",
        _ => "reset",
    };
    events.emit(
        crate::events::Level::Info,
        &format!("vm.{}", action),
        format!("vm/{}", name),
        format!("VM '{}' {}", name, what),
        Some(auth.0.name.clone()),
    );
    Ok(Json(()))
}

// ---- console --------------------------------------------------------------

#[get("/api/v1/vms/<name>/console")]
async fn console(
    _auth: crate::auth::CanWrite,
    name: &str,
    ws: ws::WebSocket,
    st: &State<VmsState>,
    volumes: &State<VolumesState>,
    cluster: &State<Arc<Cluster>>,
) -> Result<ws::Channel<'static>, ApiError> {
    let spec = spec(volumes, name).await?;
    let node = holder(cluster, &spec.anchor)
        .ok_or_else(|| (Status::Conflict, format!("'{}' isn't running", name)))?;
    match peer_url(cluster, &node)? {
        None => {
            let rt = st
                .runtime
                .as_ref()
                .ok_or_else(|| (Status::NotFound, "this node runs no VMs".to_string()))?;
            let port = rt
                .virsh
                .vnc_port(name)
                .await
                .ok_or_else(|| (Status::Conflict, format!("'{}' has no console", name)))?;
            let host = rt.console.clone();
            Ok(ws.channel(move |stream| Box::pin(bridge_tcp(stream, host, port))))
        }
        Some(base) => {
            let url = format!(
                "{}/api/v1/vms/{}/console",
                base.replacen("https://", "wss://", 1)
                    .replacen("http://", "ws://", 1),
                name
            );
            Ok(ws.channel(move |stream| Box::pin(bridge_ws(stream, url))))
        }
    }
}

/// Browser ⇄ the VNC server on `host`:`port`.
#[allow(clippy::result_large_err)] // rocket_ws's error type
async fn bridge_tcp(
    stream: ws::stream::DuplexStream,
    host: String,
    port: u16,
) -> ws::result::Result<()> {
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
    tcp.set_nodelay(true)?;
    let (mut rd, mut wr) = tcp.into_split();
    let (mut sink, mut source) = stream.split();
    let up = async {
        while let Some(m) = source.next().await {
            match m? {
                ws::Message::Binary(b) => wr.write_all(&b).await?,
                ws::Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<_, ws::result::Error>(())
    };
    let down = async {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = rd.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            sink.send(ws::Message::Binary(buf[..n].to_vec())).await?;
        }
        let _ = sink.send(ws::Message::Close(None)).await;
        Ok::<_, ws::result::Error>(())
    };
    tokio::select! {
        r = up => r,
        r = down => r,
    }
}

/// Browser ⇄ the console on the node that runs the VM (as this node).
#[allow(clippy::result_large_err)] // rocket_ws's error type
async fn bridge_ws(stream: ws::stream::DuplexStream, url: String) -> ws::result::Result<()> {
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
            match m? {
                ws::Message::Binary(b) => {
                    psink
                        .send(M::Binary(b.into()))
                        .await
                        .map_err(std::io::Error::other)?;
                }
                ws::Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = psink.send(M::Close(None)).await;
        Ok::<_, ws::result::Error>(())
    };
    let down = async {
        while let Some(m) = psource.next().await {
            match m.map_err(std::io::Error::other)? {
                M::Binary(b) => sink.send(ws::Message::Binary(b.to_vec())).await?,
                M::Close(_) => break,
                _ => {}
            }
        }
        let _ = sink.send(ws::Message::Close(None)).await;
        Ok::<_, ws::result::Error>(())
    };
    tokio::select! {
        r = up => r,
        r = down => r,
    }
}

/// Is this node ready for VM `name` to migrate here (see the module doc)?
#[post("/api/v1/vm/incoming/<name>")]
async fn incoming(
    _node: crate::auth::IsNode,
    name: &str,
    st: &State<VmsState>,
    volumes: &State<VolumesState>,
    agent: &State<crate::agent::AgentState>,
) -> Result<Json<crate::vm::Incoming>, ApiError> {
    let not_yet = |m: String| (Status::Conflict, m);
    let never = |m: &str| (Status::UnprocessableEntity, m.to_string());
    let rt = st
        .runtime
        .as_ref()
        .ok_or_else(|| never("this node runs no VMs"))?;
    let uri = rt
        .migrate_uri
        .clone()
        .ok_or_else(|| never("this node takes no live migrations (no vm.migrate_uri)"))?;
    if rt.ident().is_none() {
        return Err(not_yet("the pool isn't open here yet".into()));
    }
    let spec = spec(volumes, name).await?;
    if !spec.usable() {
        return Err(bad(spec.error.unwrap_or_default()));
    }
    let proxied: Vec<String> = agent
        .read()
        .unwrap()
        .volumes
        .iter()
        .filter(|v| v.proxied && v.transport == crate::inode::Transport::Nbd)
        .map(|v| v.name.clone())
        .collect();
    let missing: Vec<&String> = spec
        .volumes
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
        uri,
        migrate_host: rt.migrate_host.clone(),
        tls: rt.migrate_tls,
        nbd_host: rt.nbd.0.clone(),
        nbd_port: rt.nbd.1,
        console: rt.console.clone(),
        node: rt.node.clone(),
    }))
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        incoming,
        local,
        local_action,
        list,
        definition,
        put_definition,
        delete_definition,
        move_vm,
        action,
        console
    ]
}
