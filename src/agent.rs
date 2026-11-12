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

/// The `desc` an agent puts on the leases it holds.
const DESC_PREFIX: &str = "agent ";

/// How often the agent looks at what should run where.
pub const TICK: Duration = Duration::from_secs(2);

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
}

pub type AgentState = Arc<std::sync::RwLock<AgentStatus>>;

struct Running {
    lease: Arc<HeldLease>,
    lost: Arc<AtomicBool>,
    info: RunningVolume,
}

/// This node's volume writer: its lease, the pool in writer role, the
/// export servers.
struct NodeWriter {
    /// Held (and renewed) for as long as this writer exists.
    _lease: Arc<HeldLease>,
    lost: Arc<AtomicBool>,
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
    running: BTreeMap<String, Running>,
}

impl Agent {
    pub fn new(
        node: String,
        cluster: Arc<Cluster>,
        endpoints: Vec<String>,
        access: PoolAccess,
        config: AutomountConfig,
        state: AgentState,
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
            running: BTreeMap::new(),
        }
    }

    fn desc(&self) -> String {
        format!("{}{}", DESC_PREFIX, self.node)
    }

    /// Keep `node/<name>` and run the placement loop, forever.
    pub async fn run(mut self) {
        let mut node_lease: Option<(Arc<HeldLease>, Arc<AtomicBool>)> = None;
        loop {
            if node_lease
                .as_ref()
                .is_none_or(|(_, lost)| lost.load(Ordering::SeqCst))
            {
                node_lease = match HeldLease::acquire(
                    self.leases.clone(),
                    &node_resource(&self.node),
                    &self.desc(),
                )
                .await
                {
                    Ok(l) => {
                        let l = Arc::new(l);
                        Some((l.clone(), hold(&l)))
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
            tokio::time::sleep(TICK).await;
        }
    }

    fn publish(&self) {
        self.state.write().unwrap().volumes =
            self.running.values().map(|r| r.info.clone()).collect();
    }

    async fn tick(&mut self) -> Result<(), String> {
        // Volumes whose lease is gone: stop serving them at once.
        let lost: Vec<String> = self
            .running
            .iter()
            .filter(|(_, r)| r.lost.load(Ordering::SeqCst))
            .map(|(n, _)| n.clone())
            .collect();
        for name in lost {
            eprintln!("agent: lost the lease on volume '{}'", name);
            self.stop(&name, false).await;
        }
        if self
            .writer
            .as_ref()
            .is_some_and(|w| w.lost.load(Ordering::SeqCst))
        {
            eprintln!("agent: lost the writer lease; dropping every volume");
            let names: Vec<String> = self.running.keys().cloned().collect();
            for name in names {
                self.stop(&name, false).await;
            }
            self.writer = None;
        }

        let Ok(volumes) = self.committer.volumes().await else {
            return Ok(()); // no committer (yet): nothing to decide
        };
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
        // Stop what no longer belongs here (deleted, disabled, moved, changed transport).
        let stop: Vec<String> = self
            .running
            .iter()
            .filter(|(name, r)| match by_name.get(name.as_str()) {
                None => true,
                Some(v) => {
                    place(&v.props, Some(&self.node), &live).as_deref() != Some(&self.node)
                        || v.props.automount != Some(r.info.transport)
                }
            })
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
            let h = holder(&v.name);
            if place(&v.props, h.as_deref(), &live).as_deref() != Some(&self.node) {
                continue;
            }
            if h.is_some() {
                continue; // someone still has it: wait for them to let go
            }
            if let Err(e) = self.start(v).await {
                eprintln!("agent: starting volume '{}': {}", v.name, e);
            }
        }
        Ok(())
    }

    async fn ensure_writer(&mut self) -> Result<&NodeWriter, String> {
        if self.writer.is_none() {
            let resource = format!("writer/{}", self.node);
            let lease =
                Arc::new(HeldLease::acquire(self.leases.clone(), &resource, &self.desc()).await?);
            let lost = hold(&lease);
            let sink = CommitClient::new(self.endpoints.clone()).as_writer(&resource, lease.epoch);
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
            crate::scan::spawn_health_monitor(fs.clone(), Duration::from_secs(30));
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

    async fn start(&mut self, v: &VolumeInfo) -> Result<(), String> {
        let transport = v.props.automount.ok_or("not auto-mounted")?;
        let lease = Arc::new(
            HeldLease::acquire(self.leases.clone(), &volume_resource(&v.name), &self.desc())
                .await?,
        );
        let lost = hold(&lease);
        let started = async {
            let w = self.ensure_writer().await?;
            w.fs.attach_volume(&v.name, &lease.owner, lease.epoch)
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
        let (device, note) = match self.attach(&v.name, transport).await {
            Ok(d) => (d, None),
            Err(e) => (None, Some(e)),
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

/// Renew `lease` in the background; the flag is set once it's lost.
fn hold(lease: &Arc<HeldLease>) -> Arc<AtomicBool> {
    let lost = Arc::new(AtomicBool::new(false));
    let flag = lost.clone();
    lease.hold(move |msg| {
        eprintln!("agent: {}", msg);
        flag.store(true, Ordering::SeqCst);
    });
    lost
}

#[cfg(test)]
#[path = "tests/agent_tests.rs"]
mod tests;
