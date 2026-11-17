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
}

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
        Agent {
            node,
            cluster,
            leases: LeaseClient::new(endpoints.clone()),
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
        self.state.write().unwrap().volumes =
            self.running.values().map(|r| r.info.clone()).collect();
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
            self.writer = None;
            *self.files.write().unwrap() = None;
        }

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
        let target = |name: &str, own: &DatasetProps| -> Option<String> {
            match vm_place.get(name) {
                Some((p, h)) => place(p, h.as_deref(), &live),
                None => place(own, holder(name).as_deref(), &live),
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
                .is_some_and(|a| target(&spec.anchor, &a.props).as_deref() == Some(&self.node))
        };
        for v in &ours {
            let name = &v.domain.name;
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
        let lease = Arc::new(
            HeldLease::acquire(
                self.leases.clone(),
                &dataset_resource(&v.name),
                &self.desc(),
            )
            .await
            .map_err(StartError::Busy)?,
        );
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
            match transport {
                Transport::Nbd => w.nbd.add_export(&v.name).await,
                Transport::Nvme => w.nvme.add_export(&v.name).await,
            }
            .map_err(|e| e.to_string())
        }
        .await;
        if let Err(e) = started {
            if let Some(w) = &self.writer {
                w.fs.abandon_dataset(&v.name);
            }
            lease.release().await;
            return Err(StartError::Failed(e));
        }
        let (device, note) = if !attach {
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
                info: RunningVolume {
                    name: v.name.clone(),
                    transport,
                    device,
                    note,
                },
            },
        );
        Ok(())
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
