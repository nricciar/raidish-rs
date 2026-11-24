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
use crate::committer::PoolAdmin;
use crate::committer::{CommitClient, PoolAccess, dataset_resource};
use crate::events::{EventKind, EventSink, Level, Subject};
use crate::fs::FileSystem;
use crate::inode::{DatasetProps, Transport};
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::types::{Holder, HolderRole, LeaseResource};
use crate::vm_store::DefSpec;
use crate::volume::VolumeInfo;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::watch;

/// This node's writer, once the agent has one.
pub type WriterSlot = std::sync::Arc<
    std::sync::RwLock<Option<std::sync::Arc<crate::fs::FileSystem<crate::vdev::Pool>>>>,
>;

/// How often the agent looks at what should run where.
pub const TICK: Duration = Duration::from_secs(2);

/// How often a node checks whether space lent to it needs a resilver.
const HEAL_INTERVAL: Duration = Duration::from_secs(10);

pub fn node_resource(node: &str) -> LeaseResource {
    LeaseResource::node(node)
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
        self.nbd_port.unwrap_or(crate::nbd::NBD_PORT)
    }
    fn nvme_port(&self) -> u16 {
        self.nvme_port.unwrap_or(crate::nvme::NVME_PORT)
    }
    fn peer_bind(&self) -> String {
        self.peer_bind.clone().unwrap_or_else(|| "0.0.0.0".into())
    }
    pub fn peer_port(&self) -> u16 {
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

pub use crate::api_types::{AgentStatus, Attach, RunningVolume};

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
    /// When its kernel device was last attached, or tried (a failed
    /// attach is tried again `ATTACH_RETRY` later).
    attach_tried: std::time::Instant,
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
    /// Its file system and block servers (`w.fs`, `w.exporter(t)`).
    parts: handoff::WriterParts,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for NodeWriter {
    type Target = handoff::WriterParts;
    fn deref(&self) -> &handoff::WriterParts {
        &self.parts
    }
}

impl Drop for NodeWriter {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Where one tick wants each volume, from one look at the cluster state.
struct Placement {
    /// This node.
    me: String,
    state: crate::cluster::state::ClusterState,
    now: u64,
    /// Nodes whose agents hold their `node/` lease.
    live: BTreeSet<String>,
    /// Peer I/O is on: volumes can be proxied and handed over.
    peer_on: bool,
    volumes: BTreeMap<String, VolumeInfo>,
    /// VM volumes: placed by their VM's anchor (its settings, over NBD, and
    /// its holder).
    vm_place: BTreeMap<String, (DatasetProps, Option<String>)>,
    /// Volumes of VMs migrating away from here: kept here until they're
    /// gone.
    migrating: BTreeSet<String>,
}

impl Placement {
    fn volume(&self, name: &str) -> Option<&VolumeInfo> {
        self.volumes.get(name)
    }

    /// The node whose lease on volume `name` is live, if any.
    fn holder(&self, name: &str) -> Option<String> {
        self.state.dataset_node(name, self.now).map(str::to_string)
    }

    /// The nodes volume `name` is attached to (only with peer I/O: without
    /// it there's no proxying, so no point).
    fn attached(&self, name: &str) -> &[String] {
        match self.peer_on {
            true => self.state.attached.get(name).map_or(&[], |v| v.as_slice()),
            false => &[],
        }
    }

    /// Whether `name` is a VM's volume (exported for qemu, not attached).
    fn for_vm(&self, name: &str) -> bool {
        self.vm_place.contains_key(name)
    }

    /// The node volume `name` (with settings `own`) should run on.
    fn target(&self, name: &str, own: &DatasetProps) -> Option<String> {
        if self.migrating.contains(name) {
            return Some(self.me.clone());
        }
        let (props, holder) = match self.vm_place.get(name) {
            Some((p, h)) => (p, h.clone()),
            None => (own, self.holder(name)),
        };
        place_attached(props, holder.as_deref(), &self.live, self.attached(name))
    }

    /// How volume `name` (with settings `own`) is exported.
    fn transport(&self, name: &str, own: &DatasetProps) -> Option<Transport> {
        match self.vm_place.get(name) {
            Some((p, _)) => p.automount,
            None => own.automount,
        }
    }

    /// Whether VM `spec`'s volumes belong on this node.
    fn anchor_here(&self, spec: &crate::vm::VmSpec) -> bool {
        spec.anchor().is_some_and(|n| {
            self.volume(n)
                .is_some_and(|a| self.target(n, &a.props).as_deref() == Some(self.me.as_str()))
        })
    }
}

/// What libvirt runs on this node.
struct Domains {
    /// Every domain (Err: libvirt is unknown, so VM volumes stay where
    /// they are: they may be in use).
    listed: Result<Vec<crate::vm::Domain>, String>,
    /// This node's own, running.
    ours: Vec<crate::vm::LocalVm>,
}

impl Domains {
    /// Volumes a VM of ours uses.
    fn in_use(&self) -> BTreeSet<String> {
        self.ours
            .iter()
            .flat_map(|v| v.meta.volumes.iter().cloned())
            .collect()
    }
}

pub struct Agent {
    node: String,
    cluster: Arc<Cluster>,
    leases: LeaseClient,
    committer: crate::committer::Admin,
    endpoints: Vec<String>,
    access: PoolAccess,
    config: AutomountConfig,
    state: AgentState,
    writer: Option<NodeWriter>,
    /// Where this node's file API finds the writer.
    files: WriterSlot,
    running: BTreeMap<String, Running>,
    /// Running VMs here (None: this node runs none).
    vm: Option<Arc<crate::vm::VmRuntime>>,
    /// What it knows and has under way about VMs.
    vms: VmBook,
    services: Services,
    /// What this agent does goes to the activity log (see `crate::events`).
    events: EventSink,
    /// Shared with the peer I/O listener (see `handoff`).
    shared: Arc<handoff::Shared>,
    /// Peer I/O, if this node has TLS (None: no proxied exports, no
    /// handoffs; volumes move by stopping and starting).
    peer: Option<PeerConf>,
    /// This node's facts (`crate::node_facts`).
    facts: crate::node_facts::FactsHandle,
}

/// The agent's VM bookkeeping (the runtime is `Agent::vm`).
#[derive(Default)]
struct VmBook {
    /// The VM definitions as last read.
    specs: BTreeMap<String, crate::vm::VmSpec>,
    /// VMs asked to shut down here, and when.
    stopping: BTreeMap<String, std::time::Instant>,
    /// Live migrations of VMs from here, under way (see `migrate`).
    migrations: BTreeMap<String, migrate::Migration>,
    /// VMs migrated away whose volumes are being handed over.
    migrated: BTreeMap<String, migrate::Migrated>,
    /// VMs whose live migration to a node failed: they move there cold.
    migrate_failed: BTreeMap<String, String>,
    /// Failed live migrations whose guest's whereabouts aren't settled yet
    /// (see `Agent::settle_failed`).
    unsettled: BTreeMap<String, migrate::Unsettled>,
}

/// The services (S3, SQS, etcd datasets) this node serves.
#[derive(Default)]
struct Services {
    /// This node's service front ends (None: it serves none).
    frontend: Option<Arc<crate::frontend::Frontend>>,
    running: BTreeMap<String, RunningService>,
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
        committer: crate::committer::Admin,
        access: PoolAccess,
        config: AutomountConfig,
        state: AgentState,
        files: WriterSlot,
    ) -> Self {
        // Leases and writers' commits go to the same nodes.
        let endpoints = committer.client.endpoints().to_vec();
        state.write().unwrap().node = node.clone();
        let events = EventSink::new(&node);
        let leases = LeaseClient::new(endpoints.clone());
        Agent {
            node,
            cluster,
            shared: Arc::new(handoff::Shared::new(leases.clone())),
            peer: None,
            leases,
            committer,
            endpoints,
            access,
            config,
            state,
            writer: None,
            files,
            running: BTreeMap::new(),
            vm: None,
            vms: VmBook::default(),
            facts: Default::default(),
            services: Services::default(),
            events,
        }
    }

    /// Run service datasets too (see `crate::frontend`).
    pub fn with_frontend(mut self, fe: Arc<crate::frontend::Frontend>) -> Self {
        self.services.frontend = Some(fe);
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
    pub fn with_facts(mut self, facts: crate::node_facts::FactsHandle) -> Self {
        self.facts = facts;
        self
    }

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
    fn desc(&self) -> Holder {
        let h = Holder::new(HolderRole::Agent, Some(&self.node));
        match &self.services.frontend {
            Some(fe) => h.with_addr(fe.addr()),
            None => h,
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
                    tokio::spawn(crate::peer_io::serve(l, acceptor, shared));
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
                    node_resource(&self.node),
                    self.desc(),
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
            if node_lease.is_some() {
                if let Err(e) = self.tick().await {
                    eprintln!("agent: {}", e);
                }
            } else {
                // Cut off from the cluster (no node lease): place nothing,
                // but still let go of every volume, service and VM whose
                // lease lapsed. The rest of the cluster starts them
                // elsewhere once it does.
                self.drop_lost().await;
            }
            self.publish();
            match self.vm.clone() {
                Some(vm) => {
                    tokio::select! {
                        _ = tokio::time::sleep(TICK) => {}
                        _ = vm.poked() => {}
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
                    attach: Attach::device_of(p.device.clone()),
                    proxied: true,
                });
            }
        }
        self.state.write().unwrap().volumes = volumes;
    }

    /// One pass of the placement loop: let go of what this node lost, then
    /// bring volumes, proxies, services and VMs here in line with what the
    /// pool and the cluster say.
    async fn tick(&mut self) -> Result<(), String> {
        self.drop_lost().await;
        self.poll_migrations().await;

        let Ok(volumes) = self.committer.volumes().await else {
            return Ok(()); // no committer (yet): nothing to decide
        };
        // The file API writes through this node's writer: have one whether
        // or not a volume runs here.
        if let Err(e) = self.ensure_writer().await {
            eprintln!("agent: opening the pool as a writer: {}", e);
        }
        self.refresh_vms().await;
        let place = self.placement(volumes);
        // What was handed to this node since the last tick.
        let handed: Vec<VolumeInfo> = place.volumes.values().cloned().collect();
        self.adopt_handed_over(&handed).await;

        let doms = self.domains().await;
        self.reconcile_volumes(&place, &doms).await;
        if place.peer_on {
            self.reconcile_proxies(&place).await;
        }
        self.retry_attach().await;
        self.tick_services(&place.state, place.now, &place.live)
            .await;

        if let (Some(vm), Ok(listed)) = (self.vm.clone(), &doms.listed) {
            self.stop_unwanted_vms(&vm, &place, &doms.ours).await;
            self.start_wanted_vms(&vm, &place, listed, &doms.ours).await;
        }
        Ok(())
    }

    /// Volumes, services or the whole writer whose lease is gone: stop
    /// serving them at once, and kill any VM using them first (its writes
    /// are fenced already; this keeps it from running on with I/O errors
    /// while another node starts it).
    async fn drop_lost(&mut self) {
        self.drop_lost_adopted().await;
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
                EventKind::VolumeLost,
                Subject::volume(&name),
                format!(
                    "Volume '{}' stopped on {}: its lease was lost",
                    name, self.node
                ),
            );
            self.stop(&name, false).await;
        }
        let lost: Vec<String> = self
            .services
            .running
            .iter()
            .filter(|(_, r)| r.lost.is_lost())
            .map(|(n, _)| n.clone())
            .collect();
        for name in lost {
            eprintln!("agent: lost the lease on dataset '{}'", name);
            let f = self.service_format(&name);
            let label = crate::events::label_of(f);
            self.events.error(
                EventKind::DatasetLost,
                Subject::dataset(f, &name),
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
                EventKind::NodeWriterLost,
                Subject::node(&self.node),
                format!(
                    "{} lost its writer lease: everything it ran was stopped",
                    self.node
                ),
            );
            // No takeover may publish onto this writer from here on (it
            // checks under this lock, and undoes itself); those that did
            // already are taken in with the rest (audit A1).
            *self.shared.writer.lock().unwrap() = None;
            self.drop_lost_adopted().await;
            let names: Vec<String> = self.running.keys().cloned().collect();
            self.destroy_vms_using(&names).await;
            for name in names {
                self.stop(&name, false).await;
            }
            let names: Vec<String> = self.services.running.keys().cloned().collect();
            for name in names {
                self.stop_service(&name, false).await;
            }
            self.drop_proxies();
            self.writer = None;
            self.shared.writing.lock().unwrap().clear();
            *self.files.write().unwrap() = None;
        }
    }

    /// Volumes handed over to this node but not adopted yet (that's in
    /// `tick`, which doesn't run without the node lease) are run all the
    /// same: their exports and VMs must go once their lease does, or the
    /// writer's (audit A1). Those that must go are put with the running
    /// ones, for the rest of `drop_lost` to stop. With no writer at all
    /// (a takeover that finished after it was dropped), they're stopped
    /// here: nothing else would.
    async fn drop_lost_adopted(&mut self) {
        let writer = match &self.writer {
            None => WriterStanding::Gone,
            Some(w) if w.lost.is_lost() => WriterStanding::Lost,
            Some(_) => WriterStanding::Held,
        };
        let taken: Vec<(String, handoff::Slot)> = {
            let mut slots = self.shared.slots.lock().unwrap();
            let adopted = slots.iter().filter_map(|(n, s)| match s {
                handoff::Slot::Adopted { lost, .. } => Some((n.as_str(), lost.is_lost())),
                _ => None,
            });
            adopted_to_drop(adopted, writer)
                .into_iter()
                .filter_map(|n| slots.remove(&n).map(|s| (n, s)))
                .collect()
        };
        let mut names = Vec::new();
        for (name, slot) in taken {
            let handoff::Slot::Adopted {
                lease,
                lost,
                transport,
                device,
            } = slot
            else {
                continue;
            };
            eprintln!(
                "agent: dropping volume '{}', handed over but not adopted",
                name
            );
            self.running.insert(
                name.clone(),
                Running {
                    lease,
                    lost,
                    attach_tried: std::time::Instant::now(),
                    info: RunningVolume {
                        name: name.clone(),
                        transport,
                        attach: match device {
                            Some(device) => Attach::Attached { device },
                            None => Attach::Exported,
                        },
                        proxied: false,
                    },
                },
            );
            names.push(name);
        }
        if writer == WriterStanding::Gone && !names.is_empty() {
            self.destroy_vms_using(&names).await;
            for name in names {
                self.stop(&name, false).await;
            }
        }
    }

    /// Which libvirt domains are this node's (those started for this pool,
    /// by its GUID, by this node), and the VM definitions. On a read error
    /// the last ones read are kept: losing them would look like every VM
    /// was deleted.
    async fn refresh_vms(&mut self) {
        let Some(vm) = &self.vm else {
            return;
        };
        if let Some(w) = &self.writer
            && let Some(sb) = w.fs.superblock().await
        {
            vm.set_pool(sb.pool_guid);
        }
        match self.committer.vms().await {
            Ok(defs) => {
                let mut specs: BTreeMap<String, crate::vm::VmSpec> =
                    defs.into_iter().map(|(n, d)| (n, d.spec())).collect();
                crate::vm::mark_conflicts(&mut specs);
                self.vms.specs = specs;
            }
            Err(e) => eprintln!("agent: reading VM definitions: {}", e),
        }
    }

    /// Where `volumes` belong, from one look at the cluster state.
    fn placement(&self, volumes: Vec<VolumeInfo>) -> Placement {
        let state = self.cluster.status().state;
        let now = crate::cluster::now_ms();
        let live = state.live_nodes(now).map(|(n, _)| n.to_string()).collect();
        let volumes: BTreeMap<String, VolumeInfo> =
            volumes.into_iter().map(|v| (v.name.clone(), v)).collect();
        // A VM's volumes are placed by its anchor's settings and holder, so
        // they all land on one node, exported over NBD for qemu (and not
        // attached to the host kernel).
        let mut vm_place = BTreeMap::new();
        if self.vm.is_some() {
            for spec in self.vms.specs.values().filter(|s| s.usable()) {
                let Some((anchor_name, anchor)) =
                    spec.anchor().and_then(|a| Some((a, volumes.get(a)?)))
                else {
                    continue;
                };
                let props = DatasetProps {
                    automount: Some(Transport::Nbd),
                    ..anchor.props.clone()
                };
                let h = state.dataset_node(anchor_name, now).map(str::to_string);
                for v in spec.volumes() {
                    vm_place
                        .entry(v.clone())
                        .or_insert_with(|| (props.clone(), h.clone()));
                }
            }
        }
        // A migrating VM's volumes stay here until it's gone (see
        // `migrate`), whatever their placement says.
        let migrating = self
            .vms
            .migrations
            .values()
            .flat_map(|m| m.volumes.iter().cloned())
            .collect();
        Placement {
            me: self.node.clone(),
            state,
            now,
            live,
            peer_on: self.peer.is_some(),
            volumes,
            vm_place,
            migrating,
        }
    }

    /// What libvirt runs here.
    async fn domains(&self) -> Domains {
        // Not knowing which are ours yet counts as not knowing.
        let listed = match &self.vm {
            Some(vm) if vm.ident().is_none() => Err("the pool's GUID isn't known yet".to_string()),
            Some(vm) => vm.virsh.list().await.map_err(|e| {
                eprintln!("agent: {}", e);
                e.to_string()
            }),
            None => Ok(Vec::new()),
        };
        // Only this node's: whatever else shares its libvirt (other nodes,
        // other clusters, domains nobody stamped) is left alone.
        // A running domain whose owner libvirt couldn't tell (I-25) makes
        // the whole listing unknown: it may be ours, using volumes that
        // must stay, or one to start again.
        let mut listed = listed;
        let mut ours = Vec::new();
        if let (Some(vm), Ok(doms)) = (&self.vm, &listed) {
            let mut unknown = None;
            for d in doms.iter().filter(|d| d.id.is_some()) {
                match vm.our_vm(d.clone()).await {
                    Ok(Some(v)) => ours.push(v),
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("agent: who owns domain '{}' is unknown: {}", d.name, e);
                        unknown.get_or_insert(format!("domain '{}': {}", d.name, e));
                    }
                }
            }
            if let Some(e) = unknown {
                listed = Err(e);
            }
        }
        Domains { listed, ours }
    }

    /// Stop what no longer belongs here (deleted, disabled, moved, changed
    /// transport) once no VM here uses it, handing it over where it can
    /// be; start what belongs here.
    async fn reconcile_volumes(&mut self, p: &Placement, doms: &Domains) {
        let in_use = doms.in_use();
        let stop: Vec<String> = self
            .running
            .iter()
            .filter(|(name, r)| match p.volume(name) {
                None => true,
                Some(v) => {
                    p.target(name, &v.props).as_deref() != Some(&self.node)
                        || p.transport(name, &v.props) != Some(r.info.transport)
                }
            })
            .filter(|(name, _)| !in_use.contains(*name))
            // Libvirt unknown: VM volumes stay where they are, they may be
            // in use.
            .filter(|(name, _)| doms.listed.is_ok() || !p.for_vm(name))
            .map(|(n, _)| n.clone())
            .collect();
        for name in stop {
            // Going to a live node it's attached to (or asked to move to):
            // hand it over, connections and all. Otherwise stop it here
            // and let the next node start it.
            if let Some(v) = p.volume(&name)
                && let Some(to) = p.target(&name, &v.props)
                && p.peer_on
                && to != self.node
                && p.live.contains(&to)
                && p.transport(&name, &v.props) == self.running.get(&name).map(|r| r.info.transport)
                && (p.attached(&name).contains(&to)
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
                EventKind::VolumeStopped,
                Subject::volume(&name),
                format!("Volume '{}' stopped on {}", name, self.node),
            );
        }
        for v in p.volumes.values() {
            if self.running.contains_key(&v.name) {
                continue;
            }
            if p.target(&v.name, &v.props).as_deref() != Some(&self.node) {
                continue;
            }
            if p.state.dataset_held(&v.name, p.now) {
                continue; // someone still has it: wait for them to let go
            }
            let Some(t) = p.transport(&v.name, &v.props) else {
                continue;
            };
            if let Err(e) = self.start(v, t, !p.for_vm(&v.name)).await {
                eprintln!("agent: starting volume '{}': {}", v.name, e);
                let StartError::Failed(e) = e else {
                    continue;
                };
                self.events.once(
                    Level::Error,
                    EventKind::VolumeStartFailed,
                    Subject::volume(&v.name),
                    format!(
                        "Volume '{}' failed to start on {}: {}",
                        v.name, self.node, e
                    ),
                );
            }
        }
    }

    /// Exports of volumes attached here and written elsewhere.
    async fn reconcile_proxies(&mut self, p: &Placement) {
        for v in p.volumes.values() {
            let here = p.attached(&v.name).contains(&self.node);
            let (proxied, slot) = match self.shared.slots.lock().unwrap().get(&v.name) {
                Some(handoff::Slot::Proxy(_)) => (true, true),
                Some(_) => (false, true), // being (or just) handed over
                None => (false, false),
            };
            // Only once another node writes it: proxying to nobody, the
            // kernel's first reads (attaching) would just wait.
            let elsewhere = p.holder(&v.name).is_some_and(|h| h != self.node);
            if here && !slot && elsewhere && !self.running.contains_key(&v.name) {
                let Some(t) = p.transport(&v.name, &v.props) else {
                    continue;
                };
                if let Err(e) = self.add_proxy(v, t, !p.for_vm(&v.name)).await {
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
            .filter(|(n, s)| matches!(s, handoff::Slot::Proxy(_)) && p.volume(n).is_none())
            .map(|(n, _)| n.clone())
            .collect();
        for name in gone {
            self.remove_proxy(&name).await;
        }
    }

    /// This node's domains that shouldn't run here (any more): orphans are
    /// destroyed, VMs asked to move migrate live if they can, the rest shut
    /// down (destroyed after the timeout). VMs that just arrived by
    /// migration are made this node's.
    async fn stop_unwanted_vms(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        p: &Placement,
        ours: &[crate::vm::LocalVm],
    ) {
        // A failed migration is tried again only once the VM is asked to
        // move somewhere (else) again.
        let specs = &self.vms.specs;
        self.vms.migrate_failed.retain(|vm, to| {
            specs
                .get(vm)
                .and_then(|s| p.volume(s.anchor()?))
                .is_some_and(|a| a.props.move_to.as_deref() == Some(to.as_str()))
        });
        for v in ours {
            let name = &v.domain.name;
            if self.vms.migrations.contains_key(name) || self.vms.unsettled.contains_key(name) {
                continue; // leaving: see `poll_migrations`
            }
            // Migrated here: left alone while its volumes are proxied here
            // or being handed over, and made this node's once they run here.
            if v.meta.owner == crate::vm::DomOwner::Migrating {
                let incoming = {
                    let slots = self.shared.slots.lock().unwrap();
                    v.meta
                        .volumes
                        .iter()
                        .all(|x| self.running.contains_key(x) || slots.contains_key(x))
                };
                let arrived = v.meta.volumes.iter().all(|x| self.running.contains_key(x));
                if arrived {
                    self.arrived(vm, v).await;
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
                .is_some_and(|r| v.meta.owner.is(&r.lease.owner));
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
                    EventKind::VmOrphan,
                    Subject::vm(name),
                    format!(
                        "VM '{}' was running on {} without its volumes held there; destroyed it",
                        name, self.node
                    ),
                );
                self.vms.stopping.remove(name);
                continue;
            }
            let wanted = p.state.vms.contains_key(name)
                && !vm.restarting(name)
                && self
                    .vms
                    .specs
                    .get(name)
                    .is_some_and(|s| s.usable() && p.anchor_here(s));
            if wanted {
                self.vms.stopping.remove(name);
                continue;
            }
            // Asked to move: live if it can be, instead of shutting down.
            if !self.vms.stopping.contains_key(name) && self.start_migration(vm, v, p) {
                continue;
            }
            match self.vms.stopping.get(name) {
                None => {
                    println!("agent: shutting VM '{}' down", name);
                    self.events.info(
                        EventKind::VmStopping,
                        Subject::vm(name),
                        format!("VM '{}' shutting down on {}", name, self.node),
                    );
                    if let Err(e) = vm.virsh.shutdown(name).await {
                        eprintln!("agent: {}", e);
                    }
                    self.vms
                        .stopping
                        .insert(name.clone(), std::time::Instant::now());
                }
                Some(t) if t.elapsed() > vm.shutdown_timeout => {
                    eprintln!(
                        "agent: VM '{}' didn't shut down in {:?}; destroying it",
                        name, vm.shutdown_timeout
                    );
                    self.events.warn(
                        EventKind::VmForcedOff,
                        Subject::vm(name),
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
        let up = |n: &str| ours.iter().any(|v| v.domain.name == n);
        for n in self.vms.stopping.keys().filter(|n| !up(n)) {
            self.events.info(
                EventKind::VmStopped,
                Subject::vm(n),
                format!("VM '{}' stopped on {}", n, self.node),
            );
        }
        self.vms.stopping.retain(|n, _| up(n));
        // Restarts are done once the old domain is gone (it's started again
        // by `start_wanted_vms`, from the current XML).
        vm.restarts_done(up);
    }

    /// Start the VMs that belong here once all their volumes run here, and
    /// publish why the others can't start.
    async fn start_wanted_vms(
        &self,
        vm: &Arc<crate::vm::VmRuntime>,
        p: &Placement,
        listed: &[crate::vm::Domain],
        ours: &[crate::vm::LocalVm],
    ) {
        let mut errors = BTreeMap::new();
        for spec in self.vms.specs.values() {
            if !p.state.vms.contains_key(&spec.name) || !p.anchor_here(spec) {
                continue;
            }
            // Leaving, or just left, by migration: it runs elsewhere even
            // where its domain is already gone from here.
            // Or a failed migration's guest may run elsewhere (unknown yet).
            if self.vms.migrations.contains_key(&spec.name)
                || self.vms.migrated.contains_key(&spec.name)
                || self.vms.unsettled.contains_key(&spec.name)
            {
                continue;
            }
            if let Some(e) = spec.error() {
                self.events.once(
                    Level::Warn,
                    EventKind::VmInvalid,
                    Subject::vm(&spec.name),
                    format!("VM '{}' can't start: {}", spec.name, e),
                );
                errors.insert(spec.name.clone(), e.to_string());
                continue;
            }
            if let Some(d) = listed
                .iter()
                .find(|d| d.name == spec.name && d.id.is_some())
            {
                if !ours.iter().any(|v| v.domain.name == d.name) {
                    let e = format!(
                        "a domain '{}' this node didn't start is running on its libvirt",
                        d.name
                    );
                    self.events.once(
                        Level::Warn,
                        EventKind::VmConflict,
                        Subject::vm(&spec.name),
                        format!("VM '{}' can't start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.clone(), e);
                } else {
                    self.events.clear(&Subject::vm(&spec.name));
                }
                continue;
            }
            let Some(anchor) = spec.anchor() else {
                continue;
            };
            if !spec.volumes().iter().all(|v| self.running.contains_key(v)) {
                continue; // its disks aren't all here yet
            }
            let owner = self.running[anchor].lease.owner.clone();
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
                    self.events.clear(&Subject::vm(&spec.name));
                    self.events.info(
                        EventKind::VmStarted,
                        Subject::vm(&spec.name),
                        format!("VM '{}' started on {}", spec.name, self.node),
                    );
                }
                Err(e) => {
                    eprintln!("agent: starting VM '{}': {}", spec.name, e);
                    self.events.once(
                        Level::Error,
                        EventKind::VmStartFailed,
                        Subject::vm(&spec.name),
                        format!("VM '{}' failed to start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.clone(), e.to_string());
                }
            }
        }
        self.state.write().unwrap().vm_errors = errors;
    }

    /// Destroy (no shutdown: there's no time) every VM of ours here using
    /// one of `volumes`.
    async fn destroy_vms_using(&mut self, volumes: &[String]) {
        let Some(vm) = self.vm.clone() else {
            return;
        };
        let doms = match vm.virsh.list().await {
            Ok(d) => d,
            Err(e) => {
                eprintln!(
                    "agent: the leases of {:?} are gone, but libvirt can't be asked what \
                     uses them: {}",
                    volumes, e
                );
                return;
            }
        };
        for d in doms.into_iter().filter(|d| d.id.is_some()) {
            // Whose it is unknown (I-25): it goes if its disks reach a lost
            // volume through this node's export, which is going anyway;
            // that fails safe without touching another node's domains.
            let uses = match vm.our_vm(d.clone()).await {
                Ok(Some(v)) => v.meta.volumes.iter().any(|x| volumes.contains(x)),
                Ok(None) => false,
                Err(e) => match vm.exported_volumes_of(&d.name, volumes).await {
                    Ok(used) => !used.is_empty(),
                    Err(e2) => {
                        eprintln!(
                            "agent: whether domain '{}' uses a lost volume is unknown: {}; {}",
                            d.name, e, e2
                        );
                        false
                    }
                },
            };
            if !uses {
                continue;
            }
            eprintln!(
                "agent: destroying VM '{}' (its volume lease is gone)",
                d.name
            );
            self.events.error(
                EventKind::VmKilled,
                Subject::vm(&d.name),
                format!(
                    "VM '{}' was killed on {}: its volume lease was lost",
                    d.name, self.node
                ),
            );
            if let Err(e) = vm.virsh.destroy(&d.name).await {
                eprintln!("agent: {}", e);
            }
            self.vms.stopping.remove(&d.name);
        }
    }

    async fn ensure_writer(&mut self) -> Result<&NodeWriter, String> {
        if self.writer.is_none() {
            let resource = LeaseResource::writer(&self.node);
            let lease =
                Arc::new(HeldLease::acquire(self.leases.clone(), resource, self.desc()).await?);
            let sink = CommitClient::new(self.endpoints.clone()).as_writer(lease.lease_ref());
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
            let parts = handoff::WriterParts { fs, nbd, nvme };
            *self.shared.writer.lock().unwrap() = Some(parts.clone());
            self.writer = Some(NodeWriter {
                _lease: lease,
                lost,
                parts,
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
        let lease =
            match HeldLease::acquire(self.leases.clone(), dataset_resource(&v.name), self.desc())
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
                lease.holding(),
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
                    // Whatever it acknowledged and nobody flushed went with
                    // it (only its intent log is replayed): say so at the
                    // next flush (audit A2).
                    p.peer.retire();
                    let mut paused = p.io.pause_owned().await;
                    // Asked once paused: a write acknowledged while the
                    // pause drained requests counts too.
                    if p.peer.unflushed() {
                        p.io.poison();
                    }
                    paused.set(Arc::new(crate::volume_io::LocalIo::new(
                        w.fs.clone(),
                        &v.name,
                    )));
                    p.peer.retire();
                    Ok(())
                }
                None => w.add_export(&v.name, transport).await,
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
        let attached = if let Some(p) = &proxy {
            Attach::device_of(p.device.clone())
        } else if !attach {
            Attach::Exported
        } else {
            Attach::of(self.attach(&v.name, transport).await)
        };
        println!(
            "agent: running volume '{}' here ({}{})",
            v.name,
            transport,
            attached.on()
        );
        self.events.clear(&Subject::volume(&v.name));
        self.events.info(
            EventKind::VolumeStarted,
            Subject::volume(&v.name),
            format!(
                "Volume '{}' started on {} ({}{})",
                v.name,
                self.node,
                transport,
                attached.on()
            ),
        );
        if let Some(n) = attached.error() {
            eprintln!(
                "agent: volume '{}' exported but not attached: {}",
                v.name, n
            );
            self.events.warn(
                EventKind::VolumeNotAttached,
                Subject::volume(&v.name),
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
                attach_tried: std::time::Instant::now(),
                info: RunningVolume {
                    name: v.name.clone(),
                    transport,
                    attach: attached,
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
            .filter(|(_, r)| {
                matches!(r.info.attach, Attach::Failed { .. })
                    && r.attach_tried.elapsed() >= ATTACH_RETRY
            })
            .map(|(n, r)| (n.clone(), r.info.transport))
            .collect();
        for (name, t) in due {
            let result = self.attach(&name, t).await;
            let Some(r) = self.running.get_mut(&name) else {
                continue;
            };
            r.info.attach = Attach::of(result);
            r.attach_tried = std::time::Instant::now();
            if r.info.attach.error().is_none() {
                println!("agent: attached volume '{}'{}", name, r.info.attach.on());
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
        let Some(fe) = self.services.frontend.clone() else {
            return;
        };
        let Ok(datasets) = self.committer.datasets().await else {
            return;
        };
        // The live nodes that serve, and where.
        let nodes: BTreeMap<String, String> = live
            .iter()
            .filter_map(|n| {
                let addr = state
                    .live_lease(&node_resource(n), now)?
                    .holder
                    .addr
                    .clone()?;
                Some((n.clone(), addr))
            })
            .collect();
        let serving: BTreeSet<String> = nodes.keys().cloned().collect();
        let mut routes = BTreeMap::new();
        let mut wanted = BTreeMap::new();
        for d in datasets.iter().filter(|d| d.format.is_service()) {
            let holder = state
                .live_lease(&dataset_resource(&d.name), now)
                .map(|l| &l.holder);
            let owner = holder.and_then(|h| {
                Some(crate::frontend::Owner {
                    node: h.node.clone()?,
                    addr: h.addr.clone()?,
                })
            });
            routes.insert(
                d.name.clone(),
                crate::frontend::Route {
                    format: d.format,
                    port: d.props.port,
                    created_ms: d.created_ms,
                    owner,
                },
            );
            let target = place_on(&d.props, holder.and_then(|h| h.node.as_deref()), &serving);
            if target.as_deref() == Some(&self.node) {
                wanted.insert(d.name.clone(), (d.format, holder.is_some()));
            }
        }
        fe.set_routes(routes, nodes);
        let stop: Vec<String> = self
            .services
            .running
            .keys()
            .filter(|n| !wanted.contains_key(*n))
            .cloned()
            .collect();
        for name in stop {
            println!("agent: stopping dataset '{}' here", name);
            let f = self.service_format(&name);
            let label = crate::events::label_of(f);
            self.stop_service(&name, true).await;
            self.events.info(
                EventKind::DatasetStopped,
                Subject::dataset(f, &name),
                format!("{} '{}' stopped on {}", label, name, self.node),
            );
        }
        for (name, (format, held)) in wanted {
            if self.services.running.contains_key(&name) || held {
                continue; // running, or someone still has it
            }
            let label = crate::events::format_label(format);
            let subject = Subject::dataset(Some(format), &name);
            match self.start_service(&fe, &name, format).await {
                Ok(()) => {
                    println!("agent: serving {} dataset '{}' here", format.name(), name);
                    self.events.clear(&subject);
                    self.events.info(
                        EventKind::DatasetStarted,
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
                        Level::Error,
                        EventKind::DatasetStartFailed,
                        subject,
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
            HeldLease::acquire(self.leases.clone(), dataset_resource(name), self.desc())
                .await
                .map_err(StartError::Busy)?,
        );
        let lost = hold(&lease, None);
        let started = async {
            let w = self.ensure_writer().await?;
            let l = lease.clone();
            w.fs.attach_dataset_leased(
                name,
                lease.holding(),
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
        self.services.running.insert(
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
    /// Service dataset `name`'s format, if it runs here.
    fn service_format(&self, name: &str) -> Option<crate::dataset::DatasetFormat> {
        self.services.running.get(name).map(|r| r.format)
    }

    /// Stop serving service dataset `name`; `clean`: sync it and give its
    /// lease back (otherwise the lease is lost: drop it, writing nothing).
    async fn stop_service(&mut self, name: &str, clean: bool) {
        let Some(r) = self.services.running.remove(name) else {
            return;
        };
        if let Some(fe) = &self.services.frontend {
            fe.stop_local(name);
        }
        if let Some(w) = &self.writer {
            if clean {
                if let Err(e) = w.fs.detach_dataset(name).await {
                    // Its intent log is replayed once the lease is released.
                    eprintln!("agent: last sync of '{}': {}", name, e);
                    w.fs.abandon_dataset(name);
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
        if r.info.attach.device().is_some()
            && let Err(e) = self.detach(name, r.info.transport).await
        {
            eprintln!("agent: detaching '{}': {}", name, e);
        }
        if let Some(w) = &self.writer {
            let removed = w.exporter(r.info.transport).remove_export(name).await;
            if clean {
                if let Err(e) = removed {
                    eprintln!("agent: unexporting '{}': {}", name, e);
                }
                if let Err(e) = w.fs.detach_dataset(name).await {
                    // Its intent log is replayed once the lease is released.
                    eprintln!("agent: last sync of '{}': {}", name, e);
                    w.fs.abandon_dataset(name);
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
            let l = state.live_lease(&resource, now)?;
            let node = l.holder.node.clone()?;
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
        p: &Placement,
    ) -> bool {
        let name = &v.domain.name;
        if self.peer.is_none() || v.domain.state != crate::vm::DomState::Running {
            return false;
        }
        let Some(spec) = self.vms.specs.get(name).filter(|s| s.usable()) else {
            return false;
        };
        let Some((anchor_name, anchor)) = spec.anchor().and_then(|a| Some((a, p.volume(a)?)))
        else {
            return false;
        };
        // An explicit move to a live node, the VM still wanted running
        // (not stopped, not restarted), all its disks here.
        let Some(to) = anchor.props.move_to.clone() else {
            return false;
        };
        if to == self.node
            || !p.live.contains(&to)
            || p.target(anchor_name, &anchor.props).as_deref() != Some(to.as_str())
            || !p.state.vms.contains_key(name)
            || vm.restarting(name)
            || !spec.volumes().iter().all(|x| self.running.contains_key(x))
            || self.vms.migrate_failed.get(name) == Some(&to)
        {
            return false;
        }
        let peers = &self.cluster.members.peers;
        let (Some(url), Some(host)) = (peer_url(peers, &to), peer_host(peers, &to)) else {
            return false;
        };
        let http = match crate::tls::http_client(Some(Duration::from_secs(10))) {
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
            host,
            name: name.clone(),
            volumes: spec.volumes().to_vec(),
            me: self.node.clone(),
            to: to.clone(),
            virsh: vm.virsh.clone(),
            meta: v.meta.clone(),
            timeout: vm.migrate_timeout,
            facts: self.facts.clone(),
            committer: self.committer.clone(),
        };
        println!("agent: moving VM '{}' to {} (live, if it can be)", name, to);
        self.vms.migrations.insert(
            name.clone(),
            migrate::Migration {
                to,
                volumes: spec.volumes().to_vec(),
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
            .vms
            .migrations
            .iter()
            .filter(|(_, m)| m.task.is_finished())
            .map(|(n, _)| n.clone())
            .collect();
        for name in done {
            let m = self.vms.migrations.remove(&name).unwrap();
            let result = match m.task.await {
                Ok(r) => r,
                Err(e) => Err(migrate::Failure::Failed(e.to_string())),
            };
            // A failure is noted once it's settled (`settle_failed`).
            if result.is_ok() {
                self.facts.note_migration(&m.to, &Ok(()));
            }
            match result {
                Ok(()) => {
                    println!(
                        "agent: migrated VM '{}' to {} in {:?}",
                        name,
                        m.to,
                        m.started.elapsed()
                    );
                    self.events.info(
                        EventKind::VmMigrated,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' migrated live from {} to {} in {:.1}s",
                            name,
                            self.node,
                            m.to,
                            m.started.elapsed().as_secs_f64()
                        ),
                    );
                    self.vms.migrated.insert(
                        name,
                        migrate::Migrated {
                            to: m.to,
                            volumes: m.volumes,
                            detached: false,
                        },
                    );
                }
                Err(migrate::Failure::NotLive(why)) => {
                    println!(
                        "agent: VM '{}' can't move to {} live: {} (moving it cold)",
                        name, m.to, why
                    );
                    self.events.info(
                        EventKind::VmMovingCold,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' moves cold from {} to {}: {}",
                            name, self.node, m.to, why
                        ),
                    );
                    self.vms.migrate_failed.insert(name, m.to);
                }
                Err(migrate::Failure::Failed(e)) => {
                    self.vms.unsettled.insert(
                        name,
                        migrate::Unsettled {
                            to: m.to,
                            volumes: m.volumes,
                            why: e,
                            told: false,
                        },
                    );
                }
            }
        }
        self.settle_failed().await;
        // Migrated: detach here (so each volume is handed over), then, once
        // none runs here, there too (back to their own placement).
        let names: Vec<String> = self.vms.migrated.keys().cloned().collect();
        for name in names {
            let mut m = self.vms.migrated.remove(&name).unwrap();
            if !m.detached {
                let mut ok = true;
                for v in &m.volumes {
                    if let Err(e) = migrate::attach(&self.leases, v, &self.node, false).await {
                        eprintln!("agent: detaching '{}' from {}: {}", v, self.node, e);
                        ok = false;
                    }
                }
                m.detached = ok;
                self.vms.migrated.insert(name, m);
                continue;
            }
            if m.volumes.iter().any(|v| self.running.contains_key(v)) {
                self.vms.migrated.insert(name, m);
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
                self.vms.migrated.insert(name, m);
            }
        }
    }

    /// Failed live migrations: where does the guest run now? It may have
    /// switched over before the failure (`migrate::Whereabouts`).
    async fn settle_failed(&mut self) {
        let names: Vec<String> = self.vms.unsettled.keys().cloned().collect();
        for name in names {
            let mut u = self.vms.unsettled.remove(&name).unwrap();
            let here = match &self.vm {
                Some(vm) => vm
                    .virsh
                    .list()
                    .await
                    .ok()
                    .map(|l| l.iter().any(|d| d.name == name && d.id.is_some())),
                None => Some(false),
            };
            // B only matters if it isn't here.
            let there = if here == Some(false) {
                self.ask_runs_there(&u.to, &name).await
            } else {
                None
            };
            match migrate::whereabouts(here, there) {
                migrate::Whereabouts::Unknown => {
                    if !u.told {
                        u.told = true;
                        eprintln!(
                            "agent: migrating VM '{}' to {} failed ({}) and it isn't here: \
                             waiting for {} to say whether it runs there",
                            name, u.to, u.why, u.to
                        );
                        self.events.warn(
                            EventKind::VmMigrateFailed,
                            Subject::vm(&name),
                            format!(
                                "VM '{}' couldn't migrate live from {} to {} ({}) and no \
                                 longer runs on {}; {} can't be asked whether it runs there, \
                                 so it's left alone until it can",
                                name, self.node, u.to, u.why, self.node, u.to
                            ),
                        );
                    }
                    self.vms.unsettled.insert(name, u);
                }
                migrate::Whereabouts::There => {
                    println!(
                        "agent: migrating VM '{}' to {} failed ({}) but it runs there: \
                         handing its volumes over",
                        name, u.to, u.why
                    );
                    self.facts.note_migration(&u.to, &Ok(()));
                    self.events.info(
                        EventKind::VmMigrated,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' migrated live from {} to {} (though {} said it failed: {})",
                            name, self.node, u.to, self.node, u.why
                        ),
                    );
                    self.vms.migrated.insert(
                        name,
                        migrate::Migrated {
                            to: u.to,
                            volumes: u.volumes,
                            detached: false,
                        },
                    );
                }
                w @ (migrate::Whereabouts::Here | migrate::Whereabouts::Neither) => {
                    let at = if w == migrate::Whereabouts::Here {
                        format!("it still runs on {}", self.node)
                    } else {
                        "it runs nowhere".to_string()
                    };
                    eprintln!(
                        "agent: migrating VM '{}' to {} failed: {}; {} (moving it cold)",
                        name, u.to, u.why, at
                    );
                    self.facts.note_migration(&u.to, &Err(u.why.clone()));
                    self.events.warn(
                        EventKind::VmMigrateFailed,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' couldn't migrate live from {} to {} ({}); {}; moving it cold",
                            name, self.node, u.to, u.why, at
                        ),
                    );
                    for v in &u.volumes {
                        for node in [u.to.as_str(), self.node.as_str()] {
                            if let Err(e) = migrate::attach(&self.leases, v, node, false).await {
                                eprintln!("agent: detaching '{}' from {}: {}", v, node, e);
                            }
                        }
                    }
                    self.vms.migrate_failed.insert(name, u.to);
                }
            }
        }
    }

    /// Does `node` run VM `name`? None: it couldn't be asked.
    async fn ask_runs_there(&self, node: &str, name: &str) -> Option<bool> {
        let url = peer_url(&self.cluster.members.peers, node)?;
        let http = crate::tls::http_client(Some(Duration::from_secs(5))).ok()?;
        migrate::runs_there(&http, &url, name).await
    }

    /// VM `v` migrated here and its volumes run here now: make it this
    /// node's (its metadata names this node's lease owner).
    async fn arrived(&mut self, vm: &Arc<crate::vm::VmRuntime>, v: &crate::vm::LocalVm) {
        let name = &v.domain.name;
        let Some(r) = self.running.get(&v.meta.anchor) else {
            return;
        };
        let meta = crate::vm::DomMeta {
            owner: crate::vm::DomOwner::Lease(r.lease.owner.clone()),
            node: self.node.clone(),
            ..v.meta.clone()
        };
        match vm.virsh.set_meta(name, &meta).await {
            Ok(()) => {
                println!("agent: VM '{}' migrated here", name);
                self.events.info(
                    EventKind::VmArrived,
                    Subject::vm(name),
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
                device,
            } = slot
            else {
                continue;
            };
            let vm_disk = self.vms.specs.values().any(|s| s.volumes().contains(&name));
            let attached = match device {
                Some(device) => Attach::Attached { device },
                None if vm_disk => Attach::Exported,
                None => Attach::of(self.attach(&name, transport).await),
            };
            self.events.info(
                EventKind::VolumeHandedOver,
                Subject::volume(&name),
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
                    attach_tried: std::time::Instant::now(),
                    info: RunningVolume {
                        name,
                        transport,
                        attach: attached,
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
            (
                r.lease.epoch,
                r.info.transport,
                r.info.attach.device().map(str::to_string),
            )
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
        let desc: Holder =
            serde_json::from_str(desc).map_err(|e| format!("{}'s holder: {}", to, e))?;
        let owner = owner.to_string();
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
                && r.info.attach.device().is_some()
            {
                // Detached for the handoff: attach it again right away.
                r.info.attach = Attach::Failed {
                    error: "detached for a handoff that failed".into(),
                };
                r.attach_tried = std::time::Instant::now()
                    .checked_sub(ATTACH_RETRY)
                    .unwrap_or_else(std::time::Instant::now);
            }
            return Err(format!("last sync: {}", e));
        }

        let r = self.running.remove(name).ok_or("not running here")?;
        let mut transferred = r.lease.transfer(&owner, desc.clone()).await;
        if let Err(crate::cluster::lease::TransferError::NoAnswer(_)) = &transferred {
            // No answer may still have gone through: the same transfer
            // again says which.
            transferred = r.lease.transfer(&owner, desc.clone()).await;
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
                    EventKind::VolumeHandoffFailed,
                    Subject::volume(name),
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
                    EventKind::VolumeHandedOff,
                    Subject::volume(name),
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
        let exporter = parts.exporter(t);
        exporter
            .add_export_io(&v.name, v.size_bytes, peer.clone())
            .map_err(|e| e.to_string())?;
        let io = exporter.export_io(&v.name).ok_or("the export vanished")?;
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
        // A handed-over volume is this node's to stop (`drop_lost_adopted`),
        // not to forget: its export and VMs would run on.
        slots.retain(|_, s| matches!(s, handoff::Slot::Adopted { .. }));
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
        cmd.args(args);
        let out = crate::proc::ran(cmd).await?;
        if !out.ok() {
            return Err(out.stderr.trim().to_string());
        }
        Ok(out
            .stdout
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

/// Where this node's writer lease stands, for what it runs under it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterStanding {
    Held,
    Lost,
    /// No writer (dropped after a loss, or not opened yet).
    Gone,
}

/// Of the handed-over volumes not adopted yet, `(name, its lease lost)`,
/// those to drop: every one once the writer is lost or gone (they ran
/// under it), else those whose own lease is lost (audit A1).
fn adopted_to_drop<'a>(
    adopted: impl Iterator<Item = (&'a str, bool)>,
    writer: WriterStanding,
) -> Vec<String> {
    adopted
        .filter(|&(_, lost)| lost || writer != WriterStanding::Held)
        .map(|(n, _)| n.to_string())
        .collect()
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
/// See `migrate::ask_live`.
pub(crate) async fn migrate_ask_live(
    http: &reqwest::Client,
    url: &str,
    cpu: Option<String>,
) -> Option<crate::vm_check::Live> {
    migrate::ask_live(http, url, cpu).await
}

fn peer_url(peers: &BTreeMap<u64, String>, node: &str) -> Option<String> {
    let id: crate::types::NodeId = node.parse().ok()?;
    Some(peers.get(&id.get())?.trim_end_matches('/').to_string())
}

fn peer_host(peers: &BTreeMap<u64, String>, node: &str) -> Option<String> {
    let id: crate::types::NodeId = node.parse().ok()?;
    let url = reqwest::Url::parse(peers.get(&id.get())?).ok()?;

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
