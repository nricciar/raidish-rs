//! The node agent: every `listen` node runs one. It decides which volumes
//! belong on this node (their placement settings, see `inode::DatasetProps`),
//! runs them as a volume writer (see `crate::writer`), exports them over
//! NBD or NVMe/TCP and attaches the kernel device.
//!
//! Liveness is a `node/<name>` lease each agent keeps renewing. A volume
//! runs on the first live node of its list (any live node with an empty
//! list) and *stays* where it runs while that node is live: there's no
//! fail-back, only `volume move`. Who actually runs it is decided by its
//! `ds/<name>` lease, so two agents that both think a volume is theirs
//! can't both run it.

#[path = "agent_handoff.rs"]
mod handoff;
#[path = "agent_migrate.rs"]
mod migrate;

use crate::cluster::Cluster;
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::{CommitClient, PoolAccess, dataset_resource};
use crate::events::{EventSink, Level};
use crate::fs::FileSystem;
use crate::inode::{DatasetProps, Transport};
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::vdev::Pool;
use crate::volume::VolumeInfo;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::watch;

/// The `desc` an agent puts on the leases it holds.
const DESC_PREFIX: &str = "agent ";

/// How often the agent looks at what should run where.
pub const TICK: Duration = Duration::from_secs(2);

/// How often a node checks whether space lent to it needs a resilver.
const HEAL_INTERVAL: Duration = Duration::from_secs(10);

pub fn node_resource(node: &str) -> String {
    format!("node/{}", node)
}

/// The `automount` section of the config.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct AutomountConfig {
    /// Address the exports listen on and the kernel initiator connects to
    /// (default 127.0.0.1).
    pub bind: Option<String>,
    pub nbd_port: Option<u16>,
    pub nvme_port: Option<u16>,
    /// The root helper attaching kernel devices (default
    /// /usr/local/sbin/raidish-attach); `none` only exports.
    pub helper: Option<String>,
    /// Peer I/O (other nodes' exports of volumes this node writes, and
    /// handoffs; see `crate::peer_io`): the address and port it listens
    /// on (default 0.0.0.0:10810). TLS with node certificates only; keep
    /// it on the cluster's network anyway.
    pub peer_bind: Option<String>,
    pub peer_port: Option<u16>,
}

impl AutomountConfig {
    fn bind(&self) -> String {
        self.bind.clone().unwrap_or_else(|| "127.0.0.1".into())
    }
    fn nbd_port(&self) -> u16 {
        self.nbd_port.unwrap_or(10809)
    }
    fn nvme_port(&self) -> u16 {
        self.nvme_port.unwrap_or(4420)
    }
    fn peer_bind(&self) -> String {
        self.peer_bind.clone().unwrap_or_else(|| "0.0.0.0".into())
    }
    fn peer_port(&self) -> u16 {
        self.peer_port.unwrap_or(crate::peer_io::DEFAULT_PORT)
    }
    fn helper(&self) -> Option<String> {
        match self.helper.as_deref() {
            Some("none") | Some("") => None,
            Some(h) => Some(h.to_string()),
            None => Some("/usr/local/sbin/raidish-attach".into()),
        }
    }
}

/// Which node should run a volume, given who runs it now (`holder`) and
/// which nodes are live. None: nowhere.
pub fn place(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
) -> Option<String> {
    props.automount?;
    place_on(props, holder, live)
}

/// Which node should write a volume attached to `attached` (see
/// `ClusterState::attached`; empty: not attached anywhere in particular,
/// so `place`). The writer is always one of the attached nodes: the one
/// writing it now while it stays attached and live (moving it would mean a
/// handoff for nothing), else the one it's asked to move to, else the
/// first live one.
pub fn place_attached(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
    attached: &[String],
) -> Option<String> {
    if attached.is_empty() {
        return place(props, holder, live);
    }
    props.automount?;
    if !props.enabled {
        return None;
    }
    let ok = |n: &str| live.contains(n) && attached.iter().any(|a| a == n);
    if let Some(m) = &props.move_to
        && ok(m)
    {
        return Some(m.clone());
    }
    if let Some(h) = holder
        && ok(h)
    {
        return Some(h.to_string());
    }
    attached.iter().find(|n| live.contains(*n)).cloned()
}

/// Which of the `live` nodes should run a dataset (enabled, by its node
/// list), given who runs it now.
pub fn place_on(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
) -> Option<String> {
    if !props.enabled {
        return None;
    }
    let allowed = |n: &str| props.nodes.is_empty() || props.nodes.iter().any(|x| x == n);
    if let Some(m) = &props.move_to
        && live.contains(m)
        && allowed(m)
    {
        return Some(m.clone());
    }
    if let Some(h) = holder
        && live.contains(h)
        && allowed(h)
    {
        return Some(h.to_string());
    }
    if props.nodes.is_empty() {
        // Any node: the lowest (node-2 before node-10).
        live.iter().min_by_key(|n| (n.len(), n.as_str())).cloned()
    } else {
        props.nodes.iter().find(|n| live.contains(*n)).cloned()
    }
}

/// A volume this agent runs, as reported by its status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RunningVolume {
    pub name: String,
    pub transport: Transport,
    /// The attached kernel device, if any.
    pub device: Option<String>,
    /// Why it isn't attached (helper missing, failed, ...).
    pub note: Option<String>,
    /// Exported here but written by another node (see `handoff`).
    #[serde(default)]
    pub proxied: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AgentStatus {
    pub node: String,
    pub volumes: Vec<RunningVolume>,
    /// Why VMs that should run here don't (as of the last tick).
    #[serde(default)]
    pub vm_errors: BTreeMap<String, String>,
}

pub type AgentState = Arc<std::sync::RwLock<AgentStatus>>;

/// Why a volume or service dataset didn't start here.
#[derive(Debug)]
enum StartError {
    /// Its lease wasn't to be had (someone else still holds it, or the
    /// cluster can't be asked): not its failure, it's tried again.
    Busy(String),
    Failed(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::Busy(e) | StartError::Failed(e) => f.write_str(e),
        }
    }
}

struct Running {
    lease: Arc<HeldLease>,
    lost: Lost,
    info: RunningVolume,
    /// When attaching its kernel device last failed (it's tried again).
    attach_failed: Option<std::time::Instant>,
}

/// How long after a failed attach the agent tries again.
const ATTACH_RETRY: Duration = Duration::from_secs(10);

/// A service dataset this agent runs (see `crate::frontend`).
struct RunningService {
    lease: Arc<HeldLease>,
    lost: Lost,
    format: crate::dataset::DatasetFormat,
}

/// This node's volume writer: its lease, the pool in writer role, the
/// export servers.
struct NodeWriter {
    /// Held (and renewed) for as long as this writer exists.
    _lease: Arc<HeldLease>,
    lost: Lost,
    fs: Arc<FileSystem<Pool>>,
    nbd: Arc<NbdServer<Pool>>,
    nvme: Arc<NvmeTcpServer<Pool>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for NodeWriter {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

pub struct Agent {
    node: String,
    cluster: Arc<Cluster>,
    leases: LeaseClient,
    committer: CommitClient,
    endpoints: Vec<String>,
    access: PoolAccess,
    config: AutomountConfig,
    state: AgentState,
    writer: Option<NodeWriter>,
    /// Where this node's file API finds the writer.
    files: crate::files_api::WriterSlot,
    running: BTreeMap<String, Running>,
    /// Running VMs here (None: this node runs none).
    vm: Option<Arc<crate::vm::VmRuntime>>,
    /// The VM definitions as last read.
    specs: BTreeMap<String, crate::vm::VmSpec>,
    /// VMs asked to shut down here, and when.
    stopping: BTreeMap<String, std::time::Instant>,
    /// This node's service front ends (None: it serves none).
    frontend: Option<Arc<crate::frontend::Frontend>>,
    services: BTreeMap<String, RunningService>,
    /// What this agent does goes to the activity log (see `crate::events`).
    events: EventSink,
    /// Shared with the peer I/O listener (see `handoff`).
    shared: Arc<handoff::Shared>,
    /// Peer I/O, if this node has TLS (None: no proxied exports, no
    /// handoffs; volumes move by stopping and starting).
    peer: Option<PeerConf>,
    /// Live migrations of VMs from here, under way (see `migrate`).
    migrations: BTreeMap<String, migrate::Migration>,
    /// VMs migrated away whose volumes are being handed over.
    migrated: BTreeMap<String, migrate::Migrated>,
    /// VMs whose live migration to a node failed: they move there cold.
    migrate_failed: BTreeMap<String, String>,
}

/// Peer I/O's TLS and address.
struct PeerConf {
    acceptor: tokio_rustls::TlsAcceptor,
    connector: tokio_rustls::TlsConnector,
    port: u16,
}

impl Agent {
    pub fn new(
        node: String,
        cluster: Arc<Cluster>,
        endpoints: Vec<String>,
        access: PoolAccess,
        config: AutomountConfig,
        state: AgentState,
        files: crate::files_api::WriterSlot,
    ) -> Self {
        state.write().unwrap().node = node.clone();
        let events = EventSink::new(&node);
        let leases = LeaseClient::new(endpoints.clone());
        Agent {
            node,
            cluster,
            shared: Arc::new(handoff::Shared::new(leases.clone())),
            peer: None,
            leases,
            committer: CommitClient::new(endpoints.clone()),
            endpoints,
            access,
            config,
            state,
            writer: None,
            files,
            running: BTreeMap::new(),
            vm: None,
            specs: BTreeMap::new(),
            stopping: BTreeMap::new(),
            migrations: BTreeMap::new(),
            migrated: BTreeMap::new(),
            migrate_failed: BTreeMap::new(),
            frontend: None,
            services: BTreeMap::new(),
            events,
        }
    }

    /// Run service datasets too (see `crate::frontend`).
    pub fn with_frontend(mut self, fe: Arc<crate::frontend::Frontend>) -> Self {
        self.frontend = Some(fe);
        self
    }

    /// Report to this activity log sink (otherwise events stay here).
    pub fn with_events(mut self, events: EventSink) -> Self {
        self.events = events;
        self
    }

    /// Serve peer I/O with this node's certificate: volumes attached here
    /// and written elsewhere are proxied, and volumes move by handoff.
    pub fn with_peer_io(mut self, tls: &crate::tls::TlsConfig) -> Result<Self, String> {
        self.peer = Some(PeerConf {
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(tls.server_config(true, &[])?)),
            connector: tokio_rustls::TlsConnector::from(Arc::new(tls.client_config()?)),
            port: self.config.peer_port(),
        });
        Ok(self)
    }

    /// Run VMs too (see `crate::vm`).
    pub fn with_vms(mut self, vm: Arc<crate::vm::VmRuntime>) -> Self {
        self.vm = Some(vm);
        self
    }

    /// Where qemu reaches this node's NBD exports.
    pub fn nbd_target(config: &AutomountConfig) -> (String, u16) {
        let bind = config.bind();
        let host = match bind.as_str() {
            "0.0.0.0" | "::" | "[::]" => "127.0.0.1".to_string(),
            b => b.to_string(),
        };
        (host, config.nbd_port())
    }

    /// What this agent's leases say: who it is, and (if it serves) where
    /// others reach it.
    fn desc(&self) -> String {
        match &self.frontend {
            Some(fe) => format!("{}{} @{}", DESC_PREFIX, self.node, fe.addr()),
            None => format!("{}{}", DESC_PREFIX, self.node),
        }
    }

    /// Keep `node/<name>` and run the placement loop, forever.
    pub async fn run(mut self) {
        *self.shared.desc.lock().unwrap() = self.desc();
        if let Some(p) = &self.peer {
            let addr = (self.config.peer_bind(), p.port);
            match tokio::net::TcpListener::bind((addr.0.as_str(), addr.1)).await {
                Ok(l) => {
                    println!("agent: peer I/O on {}:{}", addr.0, addr.1);
                    let (acceptor, shared) = (p.acceptor.clone(), self.shared.clone());
                    tokio::spawn(async move {
                        if let Err(e) = crate::peer_io::serve(l, acceptor, shared).await {
                            eprintln!("agent: peer I/O: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!(
                        "agent: peer I/O on {}:{}: {} (no proxied exports or handoffs)",
                        addr.0, addr.1, e
                    );
                    self.peer = None;
                }
            }
        }
        let mut node_lease: Option<(Arc<HeldLease>, Lost)> = None;
        loop {
            if node_lease.as_ref().is_none_or(|(_, lost)| lost.is_lost()) {
                node_lease = match HeldLease::acquire(
                    self.leases.clone(),
                    &node_resource(&self.node),
                    &self.desc(),
                )
                .await
                {
                    Ok(l) => {
                        let l = Arc::new(l);
                        Some((l.clone(), hold(&l, None)))
                    }
                    Err(e) => {
                        eprintln!("agent: {}", e);
                        None
                    }
                };
            }
            if node_lease.is_some()
                && let Err(e) = self.tick().await
            {
                eprintln!("agent: {}", e);
            }
            self.publish();
            match self.vm.clone() {
                Some(vm) => {
                    tokio::select! {
                        _ = tokio::time::sleep(TICK) => {}
                        _ = vm.poke.notified() => {}
                    }
                }
                None => tokio::time::sleep(TICK).await,
            }
        }
    }

    fn publish(&self) {
        let mut volumes: Vec<RunningVolume> =
            self.running.values().map(|r| r.info.clone()).collect();
        for (name, slot) in self.shared.slots.lock().unwrap().iter() {
            if let handoff::Slot::Proxy(p) = slot {
                volumes.push(RunningVolume {
                    name: name.clone(),
                    transport: p.transport,
                    device: p.device.clone(),
                    note: None,
                    proxied: true,
                });
            }
        }
        self.state.write().unwrap().volumes = volumes;
    }

    async fn tick(&mut self) -> Result<(), String> {
        // Volumes whose lease is gone: stop serving them at once, and kill
        // any VM using them first (its writes are fenced already; this
        // keeps it from running on with I/O errors while another node
        // starts it).
        let lost: Vec<String> = self
            .running
            .iter()
            .filter(|(_, r)| r.lost.is_lost())
            .map(|(n, _)| n.clone())
            .collect();
        if !lost.is_empty() {
            self.destroy_vms_using(&lost).await;
        }
        for name in lost {
            eprintln!("agent: lost the lease on volume '{}'", name);
            self.events.error(
                "volume.lost",
                format!("volume/{}", name),
                format!(
                    "Volume '{}' stopped on {}: its lease was lost",
                    name, self.node
                ),
            );
            self.stop(&name, false).await;
        }
        let lost: Vec<String> = self
            .services
            .iter()
            .filter(|(_, r)| r.lost.is_lost())
            .map(|(n, _)| n.clone())
            .collect();
        for name in lost {
            eprintln!("agent: lost the lease on dataset '{}'", name);
            let label = self.service_label(&name);
            self.events.error(
                "dataset.lost",
                format!("{}/{}", label.to_lowercase(), name),
                format!(
                    "{} '{}' stopped on {}: its lease was lost",
                    label, name, self.node
                ),
            );
            self.stop_service(&name, false).await;
        }
        if self.writer.as_ref().is_some_and(|w| w.lost.is_lost()) {
            eprintln!("agent: lost the writer lease; dropping every volume");
            self.events.error(
                "node.writer_lost",
                format!("node/{}", self.node),
                format!(
                    "{} lost its writer lease: everything it ran was stopped",
                    self.node
                ),
            );
            let names: Vec<String> = self.running.keys().cloned().collect();
            self.destroy_vms_using(&names).await;
            for name in names {
                self.stop(&name, false).await;
            }
            let names: Vec<String> = self.services.keys().cloned().collect();
            for name in names {
                self.stop_service(&name, false).await;
            }
            self.drop_proxies();
            self.writer = None;
            *self.shared.writer.lock().unwrap() = None;
            self.shared.writing.lock().unwrap().clear();
            *self.files.write().unwrap() = None;
        }

        self.poll_migrations().await;

        let Ok(volumes) = self.committer.volumes().await else {
            return Ok(()); // no committer (yet): nothing to decide
        };
        // The file API writes through this node's writer: have one whether
        // or not a volume runs here.
        if let Err(e) = self.ensure_writer().await {
            eprintln!("agent: opening the pool as a writer: {}", e);
        }
        // Which libvirt domains are this node's: those started for this
        // pool (by its GUID) by this node.
        if let (Some(vm), Some(w)) = (&self.vm, &self.writer)
            && let Some(sb) = w.fs.superblock().await
        {
            *vm.pool.write().unwrap() = Some(format!("{:016x}", sb.pool_guid));
        }
        // VM definitions. On a read error keep the last ones read: losing
        // them would look like every VM was deleted.
        if self.vm.is_some() {
            match self.committer.vms().await {
                Ok(defs) => {
                    let mut specs: BTreeMap<String, crate::vm::VmSpec> =
                        defs.into_iter().map(|(n, d)| (n, d.spec())).collect();
                    crate::vm::mark_conflicts(&mut specs);
                    self.specs = specs;
                }
                Err(e) => eprintln!("agent: reading VM definitions: {}", e),
            }
        }
        let state = self.cluster.status().state;
        let now = crate::cluster::now_ms();
        let live: BTreeSet<String> = state
            .leases
            .iter()
            .filter(|(r, l)| r.starts_with("node/") && !l.released && l.expires_ms > now)
            .map(|(r, _)| r["node/".len()..].to_string())
            .collect();
        let holder = |name: &str| -> Option<String> {
            state
                .leases
                .get(&dataset_resource(name))
                .filter(|l| !l.released && l.expires_ms > now)
                .map(|l| crate::frontend::parse_desc(&l.desc).0)
        };
        // Attached to several nodes (only with peer I/O: without it there's
        // no proxying, so no point).
        let no_nodes: Vec<String> = Vec::new();
        let peer_on = self.peer.is_some();
        let attached = |name: &str| -> &Vec<String> {
            match peer_on {
                true => state.attached.get(name).unwrap_or(&no_nodes),
                false => &no_nodes,
            }
        };
        // What was handed to this node since the last tick.
        self.adopt_handed_over(&volumes).await;

        let by_name: BTreeMap<&str, &VolumeInfo> =
            volumes.iter().map(|v| (v.name.as_str(), v)).collect();
        // A VM's volumes are placed by its anchor's settings and holder, so
        // they all land on one node, exported over NBD for qemu (and not
        // attached to the host kernel).
        let mut vm_place: BTreeMap<String, (DatasetProps, Option<String>)> = BTreeMap::new();
        if self.vm.is_some() {
            for spec in self.specs.values().filter(|s| s.usable()) {
                let Some(anchor) = by_name.get(spec.anchor.as_str()) else {
                    continue;
                };
                let props = DatasetProps {
                    automount: Some(Transport::Nbd),
                    ..anchor.props.clone()
                };
                let h = holder(&spec.anchor);
                for v in &spec.volumes {
                    vm_place
                        .entry(v.clone())
                        .or_insert_with(|| (props.clone(), h.clone()));
                }
            }
        }
        // A migrating VM's volumes stay here until it's gone (see
        // `migrate`), whatever their placement says.
        let me = self.node.clone();
        let migrating: BTreeSet<String> = self
            .migrations
            .values()
            .flat_map(|m| m.volumes.iter().cloned())
            .collect();
        let target = |name: &str, own: &DatasetProps| -> Option<String> {
            if migrating.contains(name) {
                return Some(me.clone());
            }
            match vm_place.get(name) {
                Some((p, h)) => place_attached(p, h.as_deref(), &live, attached(name)),
                None => place_attached(own, holder(name).as_deref(), &live, attached(name)),
            }
        };
        let transport = |name: &str, own: &DatasetProps| -> Option<Transport> {
            match vm_place.get(name) {
                Some((p, _)) => p.automount,
                None => own.automount,
            }
        };

        // What libvirt runs here. Unknown (libvirt down): leave VM volumes
        // where they are, they may be in use.
        // Not knowing which are ours yet counts as unknown too.
        let doms = match &self.vm {
            Some(vm) if vm.ident().is_none() => Err("the pool's GUID isn't known yet".to_string()),
            Some(vm) => vm.virsh.list().await.map_err(|e| {
                eprintln!("agent: {}", e);
                e
            }),
            None => Ok(Vec::new()),
        };
        // Only this node's: whatever else shares its libvirt (other nodes,
        // other clusters, domains nobody stamped) is left alone.
        let mut ours: Vec<crate::vm::LocalVm> = Vec::new();
        if let (Some(vm), Ok(doms)) = (&self.vm, &doms) {
            for d in doms.iter().filter(|d| d.id.is_some()) {
                if let Some(v) = vm.our_vm(d.clone()).await {
                    ours.push(v);
                }
            }
        }
        let in_use: BTreeSet<String> = ours
            .iter()
            .flat_map(|v| v.meta.volumes.iter().cloned())
            .collect();

        // Stop what no longer belongs here (deleted, disabled, moved,
        // changed transport), once no VM here uses it.
        let stop: Vec<String> = self
            .running
            .iter()
            .filter(|(name, r)| match by_name.get(name.as_str()) {
                None => true,
                Some(v) => {
                    target(name, &v.props).as_deref() != Some(&self.node)
                        || transport(name, &v.props) != Some(r.info.transport)
                }
            })
            .filter(|(name, _)| !in_use.contains(*name))
            .filter(|(name, _)| doms.is_ok() || !vm_place.contains_key(*name))
            .map(|(n, _)| n.clone())
            .collect();
        for name in stop {
            // Going to a live node it's attached to (or asked to move to):
            // hand it over, connections and all. Otherwise stop it here
            // and let the next node start it.
            if let Some(v) = by_name.get(name.as_str())
                && let Some(to) = target(&name, &v.props)
                && peer_on
                && to != self.node
                && live.contains(&to)
                && transport(&name, &v.props) == self.running.get(&name).map(|r| r.info.transport)
                && (attached(&name).contains(&to)
                    || v.props.move_to.as_deref() == Some(to.as_str()))
            {
                match self.hand_off(&name, &to).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("agent: handing '{}' to {}: {}", name, to, e);
                        continue; // still the writer: again next tick
                    }
                }
                if !self.running.contains_key(&name) {
                    continue;
                }
            }
            println!("agent: stopping volume '{}' here", name);
            self.stop(&name, true).await;
            self.events.info(
                "volume.stopped",
                format!("volume/{}", name),
                format!("Volume '{}' stopped on {}", name, self.node),
            );
        }
        // Start what belongs here.
        for v in &volumes {
            if self.running.contains_key(&v.name) {
                continue;
            }
            if target(&v.name, &v.props).as_deref() != Some(&self.node) {
                continue;
            }
            if holder(&v.name).is_some() {
                continue; // someone still has it: wait for them to let go
            }
            let Some(t) = transport(&v.name, &v.props) else {
                continue;
            };
            let attach = !vm_place.contains_key(&v.name);
            if let Err(e) = self.start(v, t, attach).await {
                eprintln!("agent: starting volume '{}': {}", v.name, e);
                let StartError::Failed(e) = e else {
                    continue;
                };
                self.events.once(
                    &format!("volume/{}", v.name),
                    Level::Error,
                    "volume.start_failed",
                    format!("volume/{}", v.name),
                    format!(
                        "Volume '{}' failed to start on {}: {}",
                        v.name, self.node, e
                    ),
                );
            }
        }

        // Exports of volumes attached here and written elsewhere.
        if peer_on {
            for v in &volumes {
                let here = attached(&v.name).contains(&self.node);
                let (proxied, slot) = match self.shared.slots.lock().unwrap().get(&v.name) {
                    Some(handoff::Slot::Proxy(_)) => (true, true),
                    Some(_) => (false, true), // being (or just) handed over
                    None => (false, false),
                };
                // Only once another node writes it: proxying to nobody,
                // the kernel's first reads (attaching) would just wait.
                let elsewhere = holder(&v.name).is_some_and(|h| h != self.node);
                if here && !slot && elsewhere && !self.running.contains_key(&v.name) {
                    let Some(t) = transport(&v.name, &v.props) else {
                        continue;
                    };
                    let attach = !vm_place.contains_key(&v.name);
                    if let Err(e) = self.add_proxy(v, t, attach).await {
                        eprintln!("agent: proxying volume '{}': {}", v.name, e);
                    }
                } else if proxied && (!here || self.running.contains_key(&v.name)) {
                    self.remove_proxy(&v.name).await;
                }
            }
            let gone: Vec<String> = self
                .shared
                .slots
                .lock()
                .unwrap()
                .iter()
                .filter(|(n, s)| {
                    matches!(s, handoff::Slot::Proxy(_)) && !by_name.contains_key(n.as_str())
                })
                .map(|(n, _)| n.clone())
                .collect();
            for name in gone {
                self.remove_proxy(&name).await;
            }
        }

        self.retry_attach().await;
        self.tick_services(&state, now, &live).await;

        // VMs.
        let Some(vm) = self.vm.clone() else {
            return Ok(());
        };
        let Ok(doms) = doms else {
            return Ok(());
        };
        let anchor_here = |spec: &crate::vm::VmSpec| -> bool {
            by_name
                .get(spec.anchor.as_str())
                .is_some_and(|a| target(&spec.anchor, &a.props).as_deref() == Some(me.as_str()))
        };
        // A failed migration is tried again only once the VM is asked to
        // move somewhere (else) again.
        let specs = &self.specs;
        self.migrate_failed.retain(|vm, to| {
            specs
                .get(vm)
                .and_then(|s| by_name.get(s.anchor.as_str()))
                .is_some_and(|a| a.props.move_to.as_deref() == Some(to.as_str()))
        });
        for v in &ours {
            let name = &v.domain.name;
            if self.migrations.contains_key(name) {
                continue; // leaving: see `poll_migrations`
            }
            // Migrated here: left alone while its volumes are proxied here
            // or being handed over, and made this node's once they run here.
            if v.meta.migrating {
                let incoming = {
                    let slots = self.shared.slots.lock().unwrap();
                    v.meta
                        .volumes
                        .iter()
                        .all(|x| self.running.contains_key(x) || slots.contains_key(x))
                };
                let arrived = v.meta.volumes.iter().all(|x| self.running.contains_key(x));
                if arrived {
                    self.arrived(&vm, v).await;
                    continue;
                }
                if incoming {
                    continue;
                }
            }
            // Running without this process holding its disks, or started
            // under an earlier lease (an orphan of an earlier raidish here:
            // its NBD connections go nowhere, its writes would be refused).
            let current = self
                .running
                .get(&v.meta.anchor)
                .is_some_and(|r| r.lease.owner == v.meta.owner);
            if !current || !v.meta.volumes.iter().all(|x| self.running.contains_key(x)) {
                eprintln!(
                    "agent: VM '{}' runs here without this node's hold on its volumes \
                     (an orphan); destroying it",
                    name
                );
                if let Err(e) = vm.virsh.destroy(name).await {
                    eprintln!("agent: {}", e);
                }
                self.events.warn(
                    "vm.orphan",
                    format!("vm/{}", name),
                    format!(
                        "VM '{}' was running on {} without its volumes held there; destroyed it",
                        name, self.node
                    ),
                );
                self.stopping.remove(name);
                continue;
            }
            let wanted = state.vms.contains_key(name)
                && !vm.restart.lock().unwrap().contains(name)
                && self
                    .specs
                    .get(name)
                    .is_some_and(|s| s.usable() && anchor_here(s));
            if wanted {
                self.stopping.remove(name);
                continue;
            }
            // Asked to move: live if it can be, instead of shutting down.
            if !self.stopping.contains_key(name)
                && self.start_migration(&vm, v, &by_name, &target, &live, &state)
            {
                continue;
            }
            match self.stopping.get(name) {
                None => {
                    println!("agent: shutting VM '{}' down", name);
                    self.events.info(
                        "vm.stopping",
                        format!("vm/{}", name),
                        format!("VM '{}' shutting down on {}", name, self.node),
                    );
                    if let Err(e) = vm.virsh.shutdown(name).await {
                        eprintln!("agent: {}", e);
                    }
                    self.stopping
                        .insert(name.clone(), std::time::Instant::now());
                }
                Some(t) if t.elapsed() > vm.shutdown_timeout => {
                    eprintln!(
                        "agent: VM '{}' didn't shut down in {:?}; destroying it",
                        name, vm.shutdown_timeout
                    );
                    self.events.warn(
                        "vm.forced_off",
                        format!("vm/{}", name),
                        format!(
                            "VM '{}' didn't shut down in {:?}; forced it off",
                            name, vm.shutdown_timeout
                        ),
                    );
                    if let Err(e) = vm.virsh.destroy(name).await {
                        eprintln!("agent: {}", e);
                    }
                }
                Some(_) => {}
            }
        }
        for n in self.stopping.keys() {
            if !ours.iter().any(|v| &v.domain.name == n) {
                self.events.info(
                    "vm.stopped",
                    format!("vm/{}", n),
                    format!("VM '{}' stopped on {}", n, self.node),
                );
            }
        }
        self.stopping
            .retain(|n, _| ours.iter().any(|v| &v.domain.name == n));
        // Restarts are done once the old domain is gone (it's started again
        // below, from the current XML).
        vm.restart
            .lock()
            .unwrap()
            .retain(|n| ours.iter().any(|v| &v.domain.name == n));
        let mut errors = BTreeMap::new();
        for spec in self.specs.values() {
            if !state.vms.contains_key(&spec.name) || !anchor_here(spec) {
                continue;
            }
            // Leaving, or just left, by migration: it runs elsewhere even
            // where its domain is already gone from here.
            if self.migrations.contains_key(&spec.name) || self.migrated.contains_key(&spec.name) {
                continue;
            }
            if let Some(e) = &spec.error {
                self.events.once(
                    &format!("vm/{}", spec.name),
                    Level::Warn,
                    "vm.invalid",
                    format!("vm/{}", spec.name),
                    format!("VM '{}' can't start: {}", spec.name, e),
                );
                errors.insert(spec.name.clone(), e.clone());
                continue;
            }
            if let Some(d) = doms.iter().find(|d| d.name == spec.name && d.id.is_some()) {
                if !ours.iter().any(|v| v.domain.name == d.name) {
                    let e = format!(
                        "a domain '{}' this node didn't start is running on its libvirt",
                        d.name
                    );
                    self.events.once(
                        &format!("vm/{}", spec.name),
                        Level::Warn,
                        "vm.conflict",
                        format!("vm/{}", spec.name),
                        format!("VM '{}' can't start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.clone(), e);
                } else {
                    self.events.clear(&format!("vm/{}", spec.name));
                }
                continue;
            }
            if !spec.volumes.iter().all(|v| self.running.contains_key(v)) {
                continue; // its disks aren't all here yet
            }
            let owner = self.running[&spec.anchor].lease.owner.clone();
            let Some(ident) = vm.ident() else {
                continue;
            };
            let rendered =
                crate::vm::render(spec, (&vm.nbd.0, vm.nbd.1), &vm.console, &owner, &ident);
            let started = match rendered {
                Ok(xml) => vm.virsh.create(&spec.name, &xml).await,
                Err(e) => Err(e),
            };
            match started {
                Ok(()) => {
                    println!("agent: started VM '{}' here", spec.name);
                    self.events.clear(&format!("vm/{}", spec.name));
                    self.events.info(
                        "vm.started",
                        format!("vm/{}", spec.name),
                        format!("VM '{}' started on {}", spec.name, self.node),
                    );
                }
                Err(e) => {
                    eprintln!("agent: starting VM '{}': {}", spec.name, e);
                    self.events.once(
                        &format!("vm/{}", spec.name),
                        Level::Error,
                        "vm.start_failed",
                        format!("vm/{}", spec.name),
                        format!("VM '{}' failed to start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.clone(), e);
                }
            }
        }
        self.state.write().unwrap().vm_errors = errors;
        Ok(())
    }

    /// Destroy (no shutdown: there's no time) every VM of ours here using
    /// one of `volumes`.
    async fn destroy_vms_using(&mut self, volumes: &[String]) {
        let Some(vm) = self.vm.clone() else {
            return;
        };
        let Ok(vms) = vm.local_vms().await else {
            return;
        };
        for v in vms.iter().filter(|v| v.domain.id.is_some()) {
            if v.meta.volumes.iter().any(|x| volumes.contains(x)) {
                eprintln!(
                    "agent: destroying VM '{}' (its volume lease is gone)",
                    v.domain.name
                );
                self.events.error(
                    "vm.killed",
                    format!("vm/{}", v.domain.name),
                    format!(
                        "VM '{}' was killed on {}: its volume lease was lost",
                        v.domain.name, self.node
                    ),
                );
                if let Err(e) = vm.virsh.destroy(&v.domain.name).await {
                    eprintln!("agent: {}", e);
                }
                self.stopping.remove(&v.domain.name);
            }
        }
    }

    async fn ensure_writer(&mut self) -> Result<&NodeWriter, String> {
        if self.writer.is_none() {
            let resource = format!("writer/{}", self.node);
            let lease =
                Arc::new(HeldLease::acquire(self.leases.clone(), &resource, &self.desc()).await?);
            let sink = CommitClient::new(self.endpoints.clone()).as_writer(&resource, lease.epoch);
            // Once the writer lease is gone, commits stop waiting for the
            // committer (it would answer "retry" for as long as nobody
            // else takes the lease, which for `writer/<node>` is forever).
            let lost = hold(&lease, Some(sink.cancel.clone()));
            let fs = match FileSystem::load_writer(self.access.open().await, Arc::new(sink)).await {
                Ok(fs) => Arc::new(fs),
                Err(e) => {
                    // Let go now, so the next tick can take it again
                    // (dropping it would too, a little later).
                    lease.release().await;
                    return Err(format!("opening the pool: {}", e));
                }
            };
            let bind = self.config.bind();
            let nbd = Arc::new(NbdServer::with_bind(
                fs.clone(),
                bind.clone(),
                self.config.nbd_port(),
            ));
            let nvme = Arc::new(NvmeTcpServer::with_bind(
                fs.clone(),
                bind,
                self.config.nvme_port(),
            ));
            let mut tasks = Vec::new();
            let n = nbd.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = n.run().await {
                    eprintln!("agent: NBD server: {}", e);
                }
            }));
            let n = nvme.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = n.run().await {
                    eprintln!("agent: NVMe/TCP target: {}", e);
                }
            }));
            // The committer's health monitor resilvers the pool, except the
            // space lent here: its writes are fenced out of it. This node
            // repairs that itself (see `FileSystem::heal_lent`).
            let healer = fs.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(HEAL_INTERVAL).await;
                    match healer.heal_lent().await {
                        Ok(Some(r)) if r.repaired() > 0 || !r.is_clean() => {
                            eprintln!("agent: lent space: {}", r);
                        }
                        Ok(_) => {}
                        Err(e) => eprintln!("agent: healing lent space: {}", e),
                    }
                }
            }));
            *self.files.write().unwrap() = Some(fs.clone());
            *self.shared.writer.lock().unwrap() = Some(handoff::WriterParts {
                fs: fs.clone(),
                nbd: nbd.clone(),
                nvme: nvme.clone(),
            });
            self.writer = Some(NodeWriter {
                _lease: lease,
                lost,
                fs,
                nbd,
                nvme,
                tasks,
            });
        }
        Ok(self.writer.as_ref().unwrap())
    }

    /// Run volume `v` here, exported over `transport`; `attach` the
    /// kernel device too (a VM's disks aren't: qemu is the NBD client).
    async fn start(
        &mut self,
        v: &VolumeInfo,
        transport: Transport,
        attach: bool,
    ) -> Result<(), StartError> {
        // Exported here already, proxied to a writer that's gone: that
        // export is taken over (its clients keep their connections).
        let proxy = {
            let mut slots = self.shared.slots.lock().unwrap();
            match slots.remove(&v.name) {
                Some(handoff::Slot::Proxy(p)) => Some(p),
                None => None,
                Some(other) => {
                    slots.insert(v.name.clone(), other);
                    return Err(StartError::Busy("a handoff is under way".into()));
                }
            }
        };
        let restore = |shared: &handoff::Shared, proxy: Option<handoff::Proxy>| {
            if let Some(p) = proxy {
                shared
                    .slots
                    .lock()
                    .unwrap()
                    .insert(v.name.clone(), handoff::Slot::Proxy(p));
            }
        };
        let lease = match HeldLease::acquire(
            self.leases.clone(),
            &dataset_resource(&v.name),
            &self.desc(),
        )
        .await
        {
            Ok(l) => Arc::new(l),
            Err(e) => {
                restore(&self.shared, proxy);
                return Err(StartError::Busy(e));
            }
        };
        let lost = hold(&lease, None);
        let started = async {
            let w = self.ensure_writer().await?;
            let l = lease.clone();
            w.fs.attach_dataset_leased(
                &v.name,
                &lease.owner,
                lease.epoch,
                Some(lost.watch()),
                Some(Arc::new(move || l.fresh())),
            )
            .await
            .map_err(|e| e.to_string())?;
            match &proxy {
                Some(p) => {
                    // Its writer is gone (that's why this node starts it):
                    // requests stuck retrying it let go now and wait for
                    // the local backend, or the pause would wait them out.
                    p.peer.retire();
                    let mut paused = p.io.pause_owned().await;
                    paused.set(Arc::new(crate::volume_io::LocalIo::new(
                        w.fs.clone(),
                        &v.name,
                    )));
                    p.peer.retire();
                    Ok(())
                }
                None => match transport {
                    Transport::Nbd => w.nbd.add_export(&v.name).await,
                    Transport::Nvme => w.nvme.add_export(&v.name).await,
                }
                .map_err(|e| e.to_string()),
            }
        }
        .await;
        if let Err(e) = started {
            if let Some(w) = &self.writer {
                w.fs.abandon_dataset(&v.name);
            }
            lease.release().await;
            restore(&self.shared, proxy);
            return Err(StartError::Failed(e));
        }
        self.shared
            .writing
            .lock()
            .unwrap()
            .insert(v.name.clone(), lease.epoch);
        let (device, note) = if let Some(p) = &proxy {
            (p.device.clone(), None)
        } else if !attach {
            (None, None)
        } else {
            match self.attach(&v.name, transport).await {
                Ok(d) => (d, None),
                Err(e) => (None, Some(e)),
            }
        };
        println!(
            "agent: running volume '{}' here ({}{})",
            v.name,
            transport,
            device
                .as_deref()
                .map(|d| format!(" on {}", d))
                .unwrap_or_default()
        );
        self.events.clear(&format!("volume/{}", v.name));
        self.events.info(
            "volume.started",
            format!("volume/{}", v.name),
            format!(
                "Volume '{}' started on {} ({}{})",
                v.name,
                self.node,
                transport,
                device
                    .as_deref()
                    .map(|d| format!(" on {}", d))
                    .unwrap_or_default()
            ),
        );
        if let Some(n) = &note {
            eprintln!(
                "agent: volume '{}' exported but not attached: {}",
                v.name, n
            );
            self.events.warn(
                "volume.not_attached",
                format!("volume/{}", v.name),
                format!(
                    "Volume '{}' is exported but not attached on {}: {}",
                    v.name, self.node, n
                ),
            );
        }
        // Moved here on request: done, clear the request.
        if v.props.move_to.as_deref() == Some(&self.node) {
            let props = DatasetProps {
                move_to: None,
                ..v.props.clone()
            };
            if let Err(e) = self.committer.set_props(&v.name, &props).await {
                eprintln!("agent: clearing the move of '{}': {}", v.name, e);
            }
        }
        self.running.insert(
            v.name.clone(),
            Running {
                lease,
                lost,
                attach_failed: note.as_ref().map(|_| std::time::Instant::now()),
                info: RunningVolume {
                    name: v.name.clone(),
                    transport,
                    device,
                    note,
                    proxied: false,
                },
            },
        );
        Ok(())
    }

    /// Attach again the kernel devices of running volumes whose attach
    /// failed (the helper or a kernel module was missing, a transient
    /// error), so whatever waits for the device (see `crate::csi`) gets it.
    async fn retry_attach(&mut self) {
        let due: Vec<(String, Transport)> = self
            .running
            .iter()
            .filter(|(_, r)| r.attach_failed.is_some_and(|t| t.elapsed() >= ATTACH_RETRY))
            .map(|(n, r)| (n.clone(), r.info.transport))
            .collect();
        for (name, t) in due {
            let result = self.attach(&name, t).await;
            let Some(r) = self.running.get_mut(&name) else {
                continue;
            };
            match result {
                Ok(device) => {
                    println!(
                        "agent: attached volume '{}'{}",
                        name,
                        device
                            .as_deref()
                            .map(|d| format!(" on {}", d))
                            .unwrap_or_default()
                    );
                    r.info.device = device;
                    r.info.note = None;
                    r.attach_failed = None;
                }
                Err(e) => {
                    r.info.note = Some(e);
                    r.attach_failed = Some(std::time::Instant::now());
                }
            }
        }
    }

    /// Place, start and stop the service datasets (see `crate::frontend`),
    /// and tell the front ends where each runs.
    async fn tick_services(
        &mut self,
        state: &crate::cluster::state::ClusterState,
        now: u64,
        live: &BTreeSet<String>,
    ) {
        let Some(fe) = self.frontend.clone() else {
            return;
        };
        let Ok(datasets) = self.committer.datasets().await else {
            return;
        };
        let lease_of = |resource: &str| {
            state
                .leases
                .get(resource)
                .filter(|l| !l.released && l.expires_ms > now)
        };
        // The live nodes that serve, and where.
        let nodes: BTreeMap<String, String> = live
            .iter()
            .filter_map(|n| {
                let (_, addr) = crate::frontend::parse_desc(&lease_of(&node_resource(n))?.desc);
                Some((n.clone(), addr?))
            })
            .collect();
        let serving: BTreeSet<String> = nodes.keys().cloned().collect();
        let mut routes = BTreeMap::new();
        let mut wanted = BTreeMap::new();
        for d in datasets.iter().filter(|d| d.format.is_service()) {
            let holder =
                lease_of(&dataset_resource(&d.name)).map(|l| crate::frontend::parse_desc(&l.desc));
            let owner = holder
                .clone()
                .and_then(|(node, addr)| Some(crate::frontend::Owner { node, addr: addr? }));
            routes.insert(
                d.name.clone(),
                crate::frontend::Route {
                    format: d.format,
                    port: d.props.port,
                    created_ms: d.created_ms,
                    owner,
                },
            );
            let target = place_on(&d.props, holder.as_ref().map(|h| h.0.as_str()), &serving);
            if target.as_deref() == Some(&self.node) {
                wanted.insert(d.name.clone(), (d.format, holder.is_some()));
            }
        }
        fe.set_routes(routes, nodes);
        let stop: Vec<String> = self
            .services
            .keys()
            .filter(|n| !wanted.contains_key(*n))
            .cloned()
            .collect();
        for name in stop {
            println!("agent: stopping dataset '{}' here", name);
            let label = self.service_label(&name);
            self.stop_service(&name, true).await;
            self.events.info(
                "dataset.stopped",
                format!("{}/{}", label.to_lowercase(), name),
                format!("{} '{}' stopped on {}", label, name, self.node),
            );
        }
        for (name, (format, held)) in wanted {
            if self.services.contains_key(&name) || held {
                continue; // running, or someone still has it
            }
            let label = crate::events::format_label(format);
            let subject = format!("{}/{}", label.to_lowercase(), name);
            match self.start_service(&fe, &name, format).await {
                Ok(()) => {
                    println!("agent: serving {} dataset '{}' here", format.name(), name);
                    self.events.clear(&subject);
                    self.events.info(
                        "dataset.started",
                        subject,
                        format!("{} '{}' started on {}", label, name, self.node),
                    );
                }
                Err(StartError::Busy(e)) => {
                    eprintln!("agent: starting dataset '{}': {}", name, e);
                }
                Err(StartError::Failed(e)) => {
                    eprintln!("agent: starting dataset '{}': {}", name, e);
                    self.events.once(
                        &subject,
                        Level::Error,
                        "dataset.start_failed",
                        subject.clone(),
                        format!(
                            "{} '{}' failed to start on {}: {}",
                            label, name, self.node, e
                        ),
                    );
                }
            }
        }
    }

    async fn start_service(
        &mut self,
        fe: &Arc<crate::frontend::Frontend>,
        name: &str,
        format: crate::dataset::DatasetFormat,
    ) -> Result<(), StartError> {
        let lease = Arc::new(
            HeldLease::acquire(self.leases.clone(), &dataset_resource(name), &self.desc())
                .await
                .map_err(StartError::Busy)?,
        );
        let lost = hold(&lease, None);
        let started = async {
            let w = self.ensure_writer().await?;
            let l = lease.clone();
            w.fs.attach_dataset_leased(
                name,
                &lease.owner,
                lease.epoch,
                Some(lost.watch()),
                Some(Arc::new(move || l.fresh())),
            )
            .await
            .map_err(|e| e.to_string())?;
            fe.serve_local(name, format, w.fs.clone(), lease.epoch)
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        if let Err(e) = started {
            fe.stop_local(name);
            if let Some(w) = &self.writer {
                w.fs.abandon_dataset(name);
            }
            lease.release().await;
            return Err(StartError::Failed(e));
        }
        self.services.insert(
            name.to_string(),
            RunningService {
                lease,
                lost,
                format,
            },
        );
        Ok(())
    }

    /// What service dataset `name` is ("Bucket"...), if it runs here.
    fn service_label(&self, name: &str) -> &'static str {
        self.services
            .get(name)
            .map_or("Dataset", |r| crate::events::format_label(r.format))
    }

    /// Stop serving service dataset `name`; `clean`: sync it and give its
    /// lease back (otherwise the lease is lost: drop it, writing nothing).
    async fn stop_service(&mut self, name: &str, clean: bool) {
        let Some(r) = self.services.remove(name) else {
            return;
        };
        if let Some(fe) = &self.frontend {
            fe.stop_local(name);
        }
        if let Some(w) = &self.writer {
            if clean {
                if let Err(e) = w.fs.detach_dataset(name).await {
                    eprintln!("agent: last sync of '{}': {}", name, e);
                }
            } else {
                w.fs.abandon_dataset(name);
            }
        }
        if clean {
            r.lease.release().await;
        }
    }

    /// Stop running `name`: detach, unexport, and (`clean`) sync and release
    /// the lease; otherwise (lease lost) drop it without writing.
    async fn stop(&mut self, name: &str, clean: bool) {
        let Some(r) = self.running.remove(name) else {
            return;
        };
        self.shared.writing.lock().unwrap().remove(name);
        if r.info.device.is_some()
            && let Err(e) = self.detach(name, r.info.transport).await
        {
            eprintln!("agent: detaching '{}': {}", name, e);
        }
        if let Some(w) = &self.writer {
            let removed = match r.info.transport {
                Transport::Nbd => w.nbd.remove_export(name).await,
                Transport::Nvme => w.nvme.remove_export(name).await,
            };
            if clean {
                if let Err(e) = removed {
                    eprintln!("agent: unexporting '{}': {}", name, e);
                }
                if let Err(e) = w.fs.detach_dataset(name).await {
                    eprintln!("agent: last sync of '{}': {}", name, e);
                }
            } else {
                w.fs.abandon_dataset(name);
            }
        }
        if clean {
            r.lease.release().await;
        }
    }

    /// Where node `node`'s peer I/O is: the host of its disk URLs (the
    /// name its certificate is issued for).
    fn peer_addr(&self, node: &str) -> Result<String, String> {
        peer_host(&self.cluster.members.peers, node)
            .ok_or_else(|| format!("{} isn't a known peer", node))
    }

    /// Where volume `name`'s writer is, looked up live each time it's asked.
    fn locator(&self, name: &str, port: u16) -> Arc<dyn crate::peer_io::Locate> {
        let cluster = self.cluster.clone();
        let resource = dataset_resource(name);
        let me = self.node.clone();
        let peers = self.cluster.members.peers.clone();
        Arc::new(move || {
            let state = cluster.status().state;
            let now = crate::cluster::now_ms();
            let l = state
                .leases
                .get(&resource)
                .filter(|l| !l.released && l.expires_ms > now)?;
            let node = crate::frontend::parse_desc(&l.desc).0;
            if node == me {
                return None; // written here: nothing to proxy to
            }
            Some((peer_host(&peers, &node)?, port, l.epoch))
        })
    }

    /// Migrate VM `v` live to where its anchor is asked to move, if it can
    /// be (see `migrate`): true if a migration started (or the VM is
    /// already leaving), false to move it cold.
    #[allow(clippy::too_many_arguments)]
    fn start_migration(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        v: &crate::vm::LocalVm,
        by_name: &BTreeMap<&str, &VolumeInfo>,
        target: &dyn Fn(&str, &DatasetProps) -> Option<String>,
        live: &BTreeSet<String>,
        state: &crate::cluster::state::ClusterState,
    ) -> bool {
        let name = &v.domain.name;
        if self.peer.is_none() || v.domain.state != "running" {
            return false;
        }
        let Some(spec) = self.specs.get(name).filter(|s| s.usable()) else {
            return false;
        };
        let Some(anchor) = by_name.get(spec.anchor.as_str()) else {
            return false;
        };
        // An explicit move to a live node, the VM still wanted running
        // (not stopped, not restarted), all its disks here.
        let Some(to) = anchor.props.move_to.clone() else {
            return false;
        };
        if to == self.node
            || !live.contains(&to)
            || target(&spec.anchor, &anchor.props).as_deref() != Some(to.as_str())
            || !state.vms.contains_key(name)
            || vm.restart.lock().unwrap().contains(name)
            || !spec.volumes.iter().all(|x| self.running.contains_key(x))
            || self.migrate_failed.get(name) == Some(&to)
        {
            return false;
        }
        let Some(url) = peer_url(&self.cluster.members.peers, &to) else {
            return false;
        };
        let http = match crate::tls::http_builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(h) => h,
            Err(e) => {
                eprintln!("agent: migrating VM '{}': {}", name, e);
                return false;
            }
        };
        let job = migrate::Job {
            leases: self.leases.clone(),
            http,
            url,
            name: name.clone(),
            volumes: spec.volumes.clone(),
            me: self.node.clone(),
            to: to.clone(),
            virsh: vm.virsh.clone(),
            meta: v.meta.clone(),
            timeout: vm.migrate_timeout,
        };
        println!("agent: migrating VM '{}' to {} (live)", name, to);
        self.events.info(
            "vm.migrating",
            format!("vm/{}", name),
            format!("VM '{}' migrating live from {} to {}", name, self.node, to),
        );
        self.migrations.insert(
            name.clone(),
            migrate::Migration {
                to,
                volumes: spec.volumes.clone(),
                started: std::time::Instant::now(),
                task: tokio::spawn(job.run()),
            },
        );
        true
    }

    /// Finish what migrations from here did (see `migrate`): hand the
    /// volumes of a VM that left to where it went, or put a failed one's
    /// back, to move cold.
    async fn poll_migrations(&mut self) {
        let done: Vec<String> = self
            .migrations
            .iter()
            .filter(|(_, m)| m.task.is_finished())
            .map(|(n, _)| n.clone())
            .collect();
        for name in done {
            let m = self.migrations.remove(&name).unwrap();
            let result = match m.task.await {
                Ok(r) => r,
                Err(e) => Err(e.to_string()),
            };
            match result {
                Ok(()) => {
                    println!(
                        "agent: migrated VM '{}' to {} in {:?}",
                        name,
                        m.to,
                        m.started.elapsed()
                    );
                    self.events.info(
                        "vm.migrated",
                        format!("vm/{}", name),
                        format!(
                            "VM '{}' migrated live from {} to {} in {:.1}s",
                            name,
                            self.node,
                            m.to,
                            m.started.elapsed().as_secs_f64()
                        ),
                    );
                    self.migrated.insert(
                        name,
                        migrate::Migrated {
                            to: m.to,
                            volumes: m.volumes,
                            detached: false,
                        },
                    );
                }
                Err(e) => {
                    eprintln!(
                        "agent: migrating VM '{}' to {} failed: {} (moving it cold)",
                        name, m.to, e
                    );
                    self.events.warn(
                        "vm.migrate_failed",
                        format!("vm/{}", name),
                        format!(
                            "VM '{}' couldn't migrate live from {} to {} ({}); moving it cold",
                            name, self.node, m.to, e
                        ),
                    );
                    for v in &m.volumes {
                        for node in [m.to.as_str(), self.node.as_str()] {
                            if let Err(e) = migrate::attach(&self.leases, v, node, false).await {
                                eprintln!("agent: detaching '{}' from {}: {}", v, node, e);
                            }
                        }
                    }
                    self.migrate_failed.insert(name, m.to);
                }
            }
        }
        // Migrated: detach here (so each volume is handed over), then, once
        // none runs here, there too (back to their own placement).
        let names: Vec<String> = self.migrated.keys().cloned().collect();
        for name in names {
            let mut m = self.migrated.remove(&name).unwrap();
            if !m.detached {
                let mut ok = true;
                for v in &m.volumes {
                    if let Err(e) = migrate::attach(&self.leases, v, &self.node, false).await {
                        eprintln!("agent: detaching '{}' from {}: {}", v, self.node, e);
                        ok = false;
                    }
                }
                m.detached = ok;
                self.migrated.insert(name, m);
                continue;
            }
            if m.volumes.iter().any(|v| self.running.contains_key(v)) {
                self.migrated.insert(name, m);
                continue;
            }
            let mut ok = true;
            for v in &m.volumes {
                if let Err(e) = migrate::attach(&self.leases, v, &m.to, false).await {
                    eprintln!("agent: detaching '{}' from {}: {}", v, m.to, e);
                    ok = false;
                }
            }
            if !ok {
                self.migrated.insert(name, m);
            }
        }
    }

    /// VM `v` migrated here and its volumes run here now: make it this
    /// node's (its metadata names this node's lease owner).
    async fn arrived(&mut self, vm: &Arc<crate::vm::VmRuntime>, v: &crate::vm::LocalVm) {
        let name = &v.domain.name;
        let Some(r) = self.running.get(&v.meta.anchor) else {
            return;
        };
        let meta = crate::vm::DomMeta {
            owner: r.lease.owner.clone(),
            node: self.node.clone(),
            migrating: false,
            ..v.meta.clone()
        };
        match vm.virsh.set_meta(name, &meta).await {
            Ok(()) => {
                println!("agent: VM '{}' migrated here", name);
                self.events.info(
                    "vm.arrived",
                    format!("vm/{}", name),
                    format!("VM '{}' now runs on {} (migrated)", name, self.node),
                );
            }
            Err(e) => eprintln!("agent: taking migrated VM '{}' on: {}", name, e),
        }
    }

    /// Put what was handed to this node (see `handoff`) with what it runs.
    async fn adopt_handed_over(&mut self, volumes: &[VolumeInfo]) {
        let adopted: Vec<(String, handoff::Slot)> = {
            let mut slots = self.shared.slots.lock().unwrap();
            let names: Vec<String> = slots
                .iter()
                .filter(|(_, s)| matches!(s, handoff::Slot::Adopted { .. }))
                .map(|(n, _)| n.clone())
                .collect();
            names
                .into_iter()
                .filter_map(|n| slots.remove(&n).map(|s| (n, s)))
                .collect()
        };
        for (name, slot) in adopted {
            let handoff::Slot::Adopted {
                lease,
                lost,
                transport,
                mut device,
            } = slot
            else {
                continue;
            };
            let mut note = None;
            let vm_disk = self.specs.values().any(|s| s.volumes.contains(&name));
            if device.is_none() && !vm_disk {
                match self.attach(&name, transport).await {
                    Ok(d) => device = d,
                    Err(e) => note = Some(e),
                }
            }
            self.events.info(
                "volume.handed_over",
                format!("volume/{}", name),
                format!("Volume '{}' was handed over to {}", name, self.node),
            );
            if let Some(v) = volumes.iter().find(|v| v.name == name)
                && v.props.move_to.as_deref() == Some(&self.node)
            {
                let props = DatasetProps {
                    move_to: None,
                    ..v.props.clone()
                };
                if let Err(e) = self.committer.set_props(&name, &props).await {
                    eprintln!("agent: clearing the move of '{}': {}", name, e);
                }
            }
            self.running.insert(
                name.clone(),
                Running {
                    lease,
                    lost,
                    attach_failed: note.as_ref().map(|_| std::time::Instant::now()),
                    info: RunningVolume {
                        name,
                        transport,
                        device,
                        note,
                        proxied: false,
                    },
                },
            );
        }
    }

    /// Hand volume `name`, which this node writes, to node `to` (see
    /// `handoff`). Ok(true): it's not run here any more (handed over, or
    /// stopped cleanly when the handoff failed after this node let go);
    /// Ok(false): no peer I/O, stop it instead; Err: still run here.
    async fn hand_off(&mut self, name: &str, to: &str) -> Result<bool, String> {
        use tokio::io::{AsyncWriteExt, BufReader};
        let Some(p) = &self.peer else {
            return Ok(false);
        };
        let (connector, port) = (p.connector.clone(), p.port);
        let addr = self.peer_addr(to)?;
        let (epoch, transport, device) = {
            let r = self.running.get(name).ok_or("not running here")?;
            (r.lease.epoch, r.info.transport, r.info.device.clone())
        };
        let parts = self
            .shared
            .writer
            .lock()
            .unwrap()
            .clone()
            .ok_or("no writer here")?;
        let tls = crate::peer_io::dial(&connector, &addr, port).await?;
        let (rd, mut wr) = tokio::io::split(tls);
        let mut rd = BufReader::new(rd);
        let ask = format!(
            "{} {} {} {}\n",
            crate::peer_io::HANDOFF,
            name,
            epoch,
            transport
        );
        wr.write_all(ask.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        wr.flush().await.map_err(|e| e.to_string())?;
        let answer =
            tokio::time::timeout(handoff::HANDOFF_TIMEOUT, crate::peer_io::read_line(&mut rd))
                .await
                .map_err(|_| format!("{} didn't answer", to))?
                .map_err(|e| format!("{}: {}", to, e))?;
        let Some((owner, desc)) = answer
            .strip_prefix("READY ")
            .and_then(|r| r.split_once('\t'))
        else {
            return Err(format!("{} said: {}", to, answer));
        };
        let (owner, desc) = (owner.to_string(), desc.to_string());
        println!("agent: handing volume '{}' to {}", name, to);
        let started = std::time::Instant::now();

        // Stop serving it here: clients (the peer's proxy too) drain, then
        // the last sync.
        if device.is_some()
            && let Err(e) = self.detach(name, transport).await
        {
            eprintln!("agent: detaching '{}': {}", name, e);
        }
        if let Err(e) = parts.remove_export(name, transport).await {
            eprintln!("agent: unexporting '{}': {}", name, e);
        }
        self.shared.writing.lock().unwrap().remove(name);
        if let Err(e) = parts.fs.detach_dataset(name).await {
            // Not all of it is committed: keep it, call the handoff off.
            let _ = wr.write_all(b"ABORT\n").await;
            let _ = wr.flush().await;
            let _ = parts.add_export(name, transport).await;
            self.shared
                .writing
                .lock()
                .unwrap()
                .insert(name.to_string(), epoch);
            if let Some(r) = self.running.get_mut(name)
                && r.info.device.take().is_some()
            {
                r.attach_failed = Some(std::time::Instant::now() - ATTACH_RETRY);
            }
            return Err(format!("last sync: {}", e));
        }

        let r = self.running.remove(name).ok_or("not running here")?;
        let mut transferred = r.lease.transfer(&owner, &desc).await;
        if let Err(e) = &transferred
            && !e.contains("lost")
            && !e.contains("released")
        {
            // No answer may still have gone through: the same transfer
            // again says which.
            transferred = r.lease.transfer(&owner, &desc).await;
        }
        let new_epoch = match transferred {
            Ok(e) => e,
            Err(e) => {
                // Synced and let go of: whoever's next starts it.
                let _ = wr.write_all(b"ABORT\n").await;
                let _ = wr.flush().await;
                let _ = wr.flush().await;
                r.lease.release().await;
                self.events.warn(
                    "volume.handoff_failed",
                    format!("volume/{}", name),
                    format!(
                        "Volume '{}' couldn't be handed from {} to {} ({}); stopped here instead",
                        name, self.node, to, e
                    ),
                );
                return Ok(true);
            }
        };
        let _ = wr
            .write_all(format!("TRANSFERRED {}\n", new_epoch).as_bytes())
            .await;
        let _ = wr.flush().await;
        let done =
            tokio::time::timeout(handoff::HANDOFF_TIMEOUT, crate::peer_io::read_line(&mut rd))
                .await;
        match done {
            Ok(Ok(l)) if l == "DONE" => {
                println!(
                    "agent: handed volume '{}' to {} in {:?}",
                    name,
                    to,
                    started.elapsed()
                );
                self.events.info(
                    "volume.handed_off",
                    format!("volume/{}", name),
                    format!("Volume '{}' was handed from {} to {}", name, self.node, to),
                );
            }
            other => {
                // Its lease is theirs either way: if they couldn't take it,
                // they let go of it and it starts wherever it goes next.
                eprintln!(
                    "agent: {} didn't confirm taking '{}' over: {:?}",
                    to, name, other
                );
            }
        }
        Ok(true)
    }

    /// Export volume `v` here over `t`, its I/O proxied to whichever node
    /// writes it; `attach` the kernel device too.
    async fn add_proxy(
        &mut self,
        v: &VolumeInfo,
        t: Transport,
        attach: bool,
    ) -> Result<(), String> {
        let Some(p) = &self.peer else {
            return Ok(());
        };
        let (connector, port) = (p.connector.clone(), p.port);
        self.ensure_writer().await?;
        let parts = self
            .shared
            .writer
            .lock()
            .unwrap()
            .clone()
            .ok_or("no writer here")?;
        let locate = self.locator(&v.name, port);
        let peer = Arc::new(
            crate::peer_io::PeerIo::new(&v.name, connector, locate).retry_for(handoff::PROXY_RETRY),
        );
        let io = match t {
            Transport::Nbd => {
                parts
                    .nbd
                    .add_export_io(&v.name, v.size_bytes, peer.clone())
                    .map_err(|e| e.to_string())?;
                parts.nbd.export_io(&v.name)
            }
            Transport::Nvme => {
                parts
                    .nvme
                    .add_export_io(&v.name, v.size_bytes, peer.clone())
                    .map_err(|e| e.to_string())?;
                parts.nvme.export_io(&v.name)
            }
        }
        .ok_or("the export vanished")?;
        let device = if attach {
            match self.attach(&v.name, t).await {
                Ok(d) => d,
                Err(e) => {
                    // Not exported without its device: the next tick tries
                    // the whole thing again.
                    peer.retire();
                    if let Err(e) = parts.remove_export(&v.name, t).await {
                        eprintln!("agent: unexporting proxied '{}': {}", v.name, e);
                    }
                    return Err(format!("attaching: {}", e));
                }
            }
        } else {
            None
        };
        println!(
            "agent: exporting volume '{}' here ({}), proxied to its writer",
            v.name, t
        );
        self.shared.slots.lock().unwrap().insert(
            v.name.clone(),
            handoff::Slot::Proxy(handoff::Proxy {
                peer,
                io,
                transport: t,
                device,
            }),
        );
        Ok(())
    }

    /// Stop exporting the proxied volume `name` here.
    async fn remove_proxy(&mut self, name: &str) {
        let proxy = {
            let mut slots = self.shared.slots.lock().unwrap();
            match slots.remove(name) {
                Some(handoff::Slot::Proxy(p)) => p,
                Some(other) => {
                    slots.insert(name.to_string(), other);
                    return;
                }
                None => return,
            }
        };
        if proxy.device.is_some()
            && let Err(e) = self.detach(name, proxy.transport).await
        {
            eprintln!("agent: detaching proxied '{}': {}", name, e);
        }
        let parts = self.shared.writer.lock().unwrap().clone();
        if let Some(parts) = parts
            && let Err(e) = parts.remove_export(name, proxy.transport).await
        {
            eprintln!("agent: unexporting proxied '{}': {}", name, e);
        }
        proxy.peer.retire();
        println!("agent: no longer exporting proxied volume '{}' here", name);
    }

    /// The writer is gone: so are its exports, proxied ones included.
    fn drop_proxies(&mut self) {
        let mut slots = self.shared.slots.lock().unwrap();
        for s in slots.values() {
            if let handoff::Slot::Proxy(p) = s {
                p.peer.retire();
            }
        }
        slots.clear();
    }

    async fn helper(&self, args: &[&str]) -> Result<Option<String>, String> {
        let Some(helper) = self.config.helper() else {
            return Ok(None);
        };
        if !std::path::Path::new(&helper).exists() {
            return Err(format!("{} not installed", helper));
        }
        let root = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("Uid:"))
                    .map(|l| l.split_whitespace().nth(1) == Some("0"))
            })
            .unwrap_or(false);
        let mut cmd = if root {
            tokio::process::Command::new(&helper)
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n").arg(&helper);
            c
        };
        let out = cmd
            .args(args)
            .output()
            .await
            .map_err(|e| format!("{}: {}", helper, e))?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .rev()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .map(str::to_string))
    }

    async fn attach(&self, name: &str, t: Transport) -> Result<Option<String>, String> {
        let bind = self.config.bind();
        let port = match t {
            Transport::Nbd => self.config.nbd_port(),
            Transport::Nvme => self.config.nvme_port(),
        }
        .to_string();
        let cmd = match t {
            Transport::Nbd => "nbd-attach",
            Transport::Nvme => "nvme-attach",
        };
        self.helper(&[cmd, name, &bind, &port]).await
    }

    async fn detach(&self, name: &str, t: Transport) -> Result<(), String> {
        let cmd = match t {
            Transport::Nbd => "nbd-detach",
            Transport::Nvme => "nvme-detach",
        };
        self.helper(&[cmd, name]).await.map(|_| ())
    }
}

/// Set once a held lease is lost.
#[derive(Clone)]
struct Lost(Arc<watch::Sender<bool>>);

impl Lost {
    fn is_lost(&self) -> bool {
        *self.0.borrow()
    }
    fn watch(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }
}

/// The host of node `node`'s base URL among `peers` (by id: `node-<id>`).
/// Node `node`'s API base URL (`https://node1:8881`).
fn peer_url(peers: &BTreeMap<u64, String>, node: &str) -> Option<String> {
    let id: u64 = node.strip_prefix("node-")?.parse().ok()?;
    Some(peers.get(&id)?.trim_end_matches('/').to_string())
}

fn peer_host(peers: &BTreeMap<u64, String>, node: &str) -> Option<String> {
    let id: u64 = node.strip_prefix("node-")?.parse().ok()?;
    let url = reqwest::Url::parse(peers.get(&id)?).ok()?;
    Some(url.host_str()?.trim_matches(['[', ']']).to_string())
}

/// Renew `lease` in the background. Once it's lost the returned flag is
/// set, and so is `cancel` (a commit client's, to stop its retries).
fn hold(lease: &Arc<HeldLease>, cancel: Option<Arc<AtomicBool>>) -> Lost {
    let lost = Lost(Arc::new(watch::Sender::new(false)));
    let flag = lost.clone();
    lease.hold(move |msg| {
        eprintln!("agent: {}", msg);
        if let Some(c) = cancel {
            c.store(true, Ordering::SeqCst);
        }
        flag.0.send_replace(true);
    });
    lost
}

#[cfg(test)]
#[path = "tests/agent_tests.rs"]
mod tests;
