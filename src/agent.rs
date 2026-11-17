//! The node agent: every `listen` node runs one. It decides which volumes
//! belong on this node (their placement settings, see `inode::VolumeProps`),
//! runs them as a volume writer (see `crate::writer`), exports them over
//! NBD or NVMe/TCP and attaches the kernel device.
//!
//! Liveness is a `node/<name>` lease each agent keeps renewing. A volume
//! runs on the first live node of its list (any live node with an empty
//! list) and *stays* where it runs while that node is live: there's no
//! fail-back, only `volume move`. Who actually runs it is decided by its
//! `vol/<name>` lease, so two agents that both think a volume is theirs
//! can't both run it.

use crate::cluster::Cluster;
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::{CommitClient, PoolAccess, volume_resource};
use crate::fs::FileSystem;
use crate::inode::{Transport, VolumeProps};
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
pub fn place(props: &VolumeProps, holder: Option<&str>, live: &BTreeSet<String>) -> Option<String> {
    if !props.enabled || props.automount.is_none() {
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

struct Running {
    lease: Arc<HeldLease>,
    lost: Lost,
    info: RunningVolume,
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
        }
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

    fn desc(&self) -> String {
        format!("{}{}", DESC_PREFIX, self.node)
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
            self.stop(&name, false).await;
        }
        if self.writer.as_ref().is_some_and(|w| w.lost.is_lost()) {
            eprintln!("agent: lost the writer lease; dropping every volume");
            let names: Vec<String> = self.running.keys().cloned().collect();
            self.destroy_vms_using(&names).await;
            for name in names {
                self.stop(&name, false).await;
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
        // VM definitions. On a read error keep the last ones read: losing
        // them would look like every VM was deleted.
        if self.vm.is_some()
            && let Some(w) = &self.writer
            && let Err(e) = crate::vm::load_specs(&w.fs, &mut self.specs).await
        {
            eprintln!("agent: reading VM definitions: {}", e);
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
                .get(&volume_resource(name))
                .filter(|l| !l.released && l.expires_ms > now)
                .map(|l| {
                    l.desc
                        .strip_prefix(DESC_PREFIX)
                        .unwrap_or(&l.desc)
                        .to_string()
                })
        };

        let by_name: BTreeMap<&str, &VolumeInfo> =
            volumes.iter().map(|v| (v.name.as_str(), v)).collect();
        // A VM's volumes are placed by its anchor's settings and holder, so
        // they all land on one node, exported over NBD for qemu (and not
        // attached to the host kernel).
        let mut vm_place: BTreeMap<String, (VolumeProps, Option<String>)> = BTreeMap::new();
        if self.vm.is_some() {
            for spec in self.specs.values().filter(|s| s.usable()) {
                let Some(anchor) = by_name.get(spec.anchor.as_str()) else {
                    continue;
                };
                let props = VolumeProps {
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
        let target = |name: &str, own: &VolumeProps| -> Option<String> {
            match vm_place.get(name) {
                Some((p, h)) => place(p, h.as_deref(), &live),
                None => place(own, holder(name).as_deref(), &live),
            }
        };
        let transport = |name: &str, own: &VolumeProps| -> Option<Transport> {
            match vm_place.get(name) {
                Some((p, _)) => p.automount,
                None => own.automount,
            }
        };

        // What libvirt runs here. Unknown (libvirt down): leave VM volumes
        // where they are, they may be in use.
        let doms = match &self.vm {
            Some(vm) => vm.virsh.list().await.map_err(|e| {
                eprintln!("agent: {}", e);
                e
            }),
            None => Ok(Vec::new()),
        };
        let mut ours: Vec<crate::vm::LocalVm> = Vec::new();
        if let (Some(vm), Ok(doms)) = (&self.vm, &doms) {
            for d in doms.iter().filter(|d| d.id.is_some()) {
                if let Some(v) = vm.virsh.local_vm(d.clone()).await {
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
            }
        }

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
                    if let Err(e) = vm.virsh.destroy(name).await {
                        eprintln!("agent: {}", e);
                    }
                }
                Some(_) => {}
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
                errors.insert(spec.name.clone(), e.clone());
                continue;
            }
            if let Some(d) = doms.iter().find(|d| d.name == spec.name && d.id.is_some()) {
                if !ours.iter().any(|v| v.domain.name == d.name) {
                    errors.insert(
                        spec.name.clone(),
                        format!("a domain '{}' not run by raidish is running here", d.name),
                    );
                }
                continue;
            }
            if !spec.volumes.iter().all(|v| self.running.contains_key(v)) {
                continue; // its disks aren't all here yet
            }
            let owner = self.running[&spec.anchor].lease.owner.clone();
            let started = match crate::vm::render(spec, (&vm.nbd.0, vm.nbd.1), &owner) {
                Ok(xml) => vm.virsh.create(&spec.name, &xml).await,
                Err(e) => Err(e),
            };
            match started {
                Ok(()) => println!("agent: started VM '{}' here", spec.name),
                Err(e) => {
                    eprintln!("agent: starting VM '{}': {}", spec.name, e);
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
        let Ok(vms) = vm.virsh.local_vms().await else {
            return;
        };
        for v in vms.iter().filter(|v| v.domain.id.is_some()) {
            if v.meta.volumes.iter().any(|x| volumes.contains(x)) {
                eprintln!(
                    "agent: destroying VM '{}' (its volume lease is gone)",
                    v.domain.name
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
            let fs = Arc::new(
                FileSystem::load_writer(self.access.open().await, Arc::new(sink))
                    .await
                    .map_err(|e| format!("opening the pool: {}", e))?,
            );
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
    ) -> Result<(), String> {
        let lease = Arc::new(
            HeldLease::acquire(self.leases.clone(), &volume_resource(&v.name), &self.desc())
                .await?,
        );
        let lost = hold(&lease, None);
        let started = async {
            let w = self.ensure_writer().await?;
            let l = lease.clone();
            w.fs.attach_volume_leased(
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
                w.fs.abandon_volume(&v.name);
            }
            lease.release().await;
            return Err(e);
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
        if let Some(n) = &note {
            eprintln!(
                "agent: volume '{}' exported but not attached: {}",
                v.name, n
            );
        }
        // Moved here on request: done, clear the request.
        if v.props.move_to.as_deref() == Some(&self.node) {
            let props = VolumeProps {
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
                if let Err(e) = w.fs.detach_volume(name).await {
                    eprintln!("agent: last sync of '{}': {}", name, e);
                }
            } else {
                w.fs.abandon_volume(name);
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
