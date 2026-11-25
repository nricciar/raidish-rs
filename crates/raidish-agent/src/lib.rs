//! The node agent: every `listen` node runs one. It decides which volumes
//! belong on this node (their placement settings, see `inode::DatasetProps`),
//! runs them as a volume writer (see `raidish_core::writer`), exports them over
//! NBD or NVMe/TCP and attaches the kernel device.
//!
//! Liveness is a `node/<name>` lease each agent keeps renewing. A volume
//! runs on the first live node of its list (any live node with an empty
//! list) and *stays* where it runs while that node is live: there's no
//! fail-back, only `volume move`. Who actually runs it is decided by its
//! `ds/<name>` lease, so two agents that both think a volume is theirs
//! can't both run it.
//!
//! One loop ([`Agent::run`]) does it all, every `TICK`, in this order:
//!
//! | Module | What it does |
//! |---|---|
//! | `lib` | the loop, the node lease, letting go of what lost its lease |
//! | `supervisor` | this node's writer (`NodeWriter`): its lease, the pool, the export servers |
//! | `placement` | where each volume belongs (`place*`, one tick's `Placement`) |
//! | `volumes` | the volumes here: started, stopped, proxied, handed over (`handoff`), in one table (`table`) |
//! | `services` | the S3, SQS and etcd datasets served here |
//! | `vms` | the VMs here: started, stopped, orphans, arrivals, live migration (`migrate`) |
//! | `attach` | kernel devices for volumes, through the root helper |
//! | `node_facts`, `vm_check` | this node's facts; whether a VM can move live |

mod attach;
pub mod node_facts;
mod placement;
mod services;
mod supervisor;
pub mod vm_check;
mod vms;
mod volumes;

pub use placement::{place, place_attached, place_on};
use services::Services;
use supervisor::NodeWriter;
use vms::VmBook;

pub(crate) use raidish_auth::{pki, tls};
pub(crate) use raidish_block::{export, nbd, nvme, peer_io, volume_io};
pub(crate) use raidish_cluster as cluster;
pub(crate) use raidish_committer as committer;
pub(crate) use raidish_committer::events;
#[cfg(test)]
pub(crate) use raidish_core::test_util;
pub(crate) use raidish_core::{dataset, fs, inode, vdev, volume};
pub(crate) use raidish_services::frontend;
pub(crate) use raidish_types::{api_types, types, vm_def};
pub(crate) use raidish_vm::{proc, vm, vm_store};

use crate::cluster::Cluster;
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::PoolAccess;
use crate::committer::PoolAdmin;
use crate::events::{EventKind, EventSink, Level, Subject};
use crate::types::DatasetName;
use crate::types::{Holder, HolderRole, LeaseResource};
use serde::Deserialize;
use std::collections::BTreeMap;
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

pub use crate::api_types::{AgentStatus, Attach};

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
    /// Running VMs here (None: this node runs none).
    vm: Option<Arc<crate::vm::VmRuntime>>,
    /// What it knows and has under way about VMs.
    vms: VmBook,
    services: Services,
    /// What this agent does goes to the activity log (see `crate::events`).
    events: EventSink,
    /// Shared with the peer I/O listener (see `handoff`).
    shared: Arc<volumes::handoff::Shared>,
    /// Peer I/O, if this node has TLS (None: no proxied exports, no
    /// handoffs; volumes move by stopping and starting).
    peer: Option<PeerConf>,
    /// This node's facts (`crate::node_facts`).
    facts: crate::node_facts::FactsHandle,
    /// Attaches volumes' kernel devices (the root helper).
    attacher: Arc<dyn attach::Attacher>,
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
        let attacher = Arc::new(attach::Helper::new(&config));
        Agent {
            node,
            cluster,
            shared: Arc::new(volumes::handoff::Shared::new(leases.clone())),
            peer: None,
            leases,
            committer,
            endpoints,
            access,
            config,
            state,
            writer: None,
            files,
            vm: None,
            vms: VmBook::default(),
            facts: Default::default(),
            attacher,
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

    /// This node's facts (see `node_facts`), for its migrations.
    pub fn with_facts(mut self, facts: crate::node_facts::FactsHandle) -> Self {
        self.facts = facts;
        self
    }

    /// Run VMs too (see `raidish_vm::vm`).
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
                    LeaseResource::node(&self.node),
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
                self.tick().await;
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

    /// Say what happened, once: in this process's log (`agent: <message>`;
    /// info to stdout, warnings and errors to stderr) and in the activity
    /// log (see `events`).
    pub(crate) fn note(&self, level: Level, kind: EventKind, subject: Subject, message: String) {
        log(level, &message);
        match level {
            Level::Info => self.events.info(kind, subject, message),
            Level::Warn => self.events.warn(kind, subject, message),
            Level::Error => self.events.error(kind, subject, message),
        }
    }

    /// The same for a condition that's retried (a volume or VM that won't
    /// start, every tick): noted once, and again only when its message
    /// changes or `subject` is cleared (`EventSink::once`).
    pub(crate) fn note_once(
        &self,
        level: Level,
        kind: EventKind,
        subject: Subject,
        message: String,
    ) {
        if self.events.once(level, kind, subject, message.clone()) {
            log(level, &message);
        }
    }

    fn publish(&self) {
        self.state.write().unwrap().volumes = self.shared.volumes.published();
    }

    /// One pass of the placement loop: let go of what this node lost, then
    /// bring volumes, proxies, services and VMs here in line with what the
    /// pool and the cluster say.
    async fn tick(&mut self) {
        self.drop_lost().await;
        self.poll_migrations().await;

        let Ok(volumes) = self.committer.volumes().await else {
            return; // no committer (yet): nothing to decide
        };
        // The file API writes through this node's writer: have one whether
        // or not a volume runs here.
        if let Err(e) = self.ensure_writer().await {
            eprintln!("agent: opening the pool as a writer: {}", e);
        }
        self.refresh_vms().await;
        let mut place = self.placement(volumes);
        // Migrations recorded but no longer run by anyone (I-29).
        if self.vm.is_some() {
            self.recover_migrations(&place).await;
        }
        // What was handed to this node since the last tick.
        self.adopt_handed_over(&place).await;

        let doms = self.domains().await;
        place.vm.saw(&doms);
        self.reconcile_volumes(&place).await;
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
    }

    /// Volumes, services or the whole writer whose lease is gone: stop
    /// serving them at once, and kill any VM using them first (its writes
    /// are fenced already; this keeps it from running on with I/O errors
    /// while another node starts it).
    async fn drop_lost(&mut self) {
        // Handed over but not picked up yet counts too: their exports and
        // VMs must go once their lease does (audit A1).
        let lost = self.shared.volumes.lost();
        if !lost.is_empty() {
            self.destroy_vms_using(&lost).await;
        }
        for name in lost {
            self.note(
                Level::Error,
                EventKind::VolumeLost,
                Subject::volume(&name),
                format!(
                    "Volume '{}' stopped on {}: its lease was lost",
                    name, self.node
                ),
            );
            self.stop(&name, false).await;
        }
        let lost: Vec<DatasetName> = self
            .services
            .running
            .iter()
            .filter(|(_, r)| r.lost.is_lost())
            .map(|(n, _)| n.clone())
            .collect();
        for name in lost {
            let f = self.service_format(&name);
            let label = crate::events::label_of(f);
            self.note(
                Level::Error,
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
            self.note(
                Level::Error,
                EventKind::NodeWriterLost,
                Subject::node(&self.node),
                format!(
                    "{} lost its writer lease: everything it ran was stopped",
                    self.node
                ),
            );
            // Everything that ran on it, handed over or not: no takeover
            // may finish onto this writer from here on (it checks under the
            // same lock, and undoes itself), and those that did are taken
            // with the rest (audit A1).
            let wgen = self.writer.as_ref().map(|w| w.wgen);
            let dropped = match wgen {
                Some(g) => self.shared.volumes.drop_writer(g),
                None => volumes::Dropped {
                    owned: Vec::new(),
                    proxies: Vec::new(),
                },
            };
            let names: Vec<DatasetName> = dropped.owned.iter().map(|(n, _)| n.clone()).collect();
            self.destroy_vms_using(&names).await;
            // All at once: each waits out its clients' drain (I-27).
            let me = &*self;
            futures::future::join_all(
                dropped
                    .owned
                    .into_iter()
                    .map(|(n, v)| async move { me.wind_down(&n, v, false).await }),
            )
            .await;
            let names: Vec<DatasetName> = self.services.running.keys().cloned().collect();
            for name in names {
                self.stop_service(&name, false).await;
            }
            // Its proxied exports too, devices and all.
            let me = &*self;
            futures::future::join_all(
                dropped
                    .proxies
                    .into_iter()
                    .map(|(n, p)| async move { me.take_down_proxy(&n, p).await }),
            )
            .await;
            self.writer = None;
            *self.files.write().unwrap() = None;
        }
    }
}

/// `message` in this process's log, by `level`.
fn log(level: Level, message: &str) {
    match level {
        Level::Info => println!("agent: {}", message),
        Level::Warn | Level::Error => eprintln!("agent: {}", message),
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

pub use vms::migrate::ask_live as migrate_ask_live;

/// Node `node`'s API base URL among `peers` (`https://node1:8881`; by id:
/// `node-<id>`).
fn peer_url(peers: &BTreeMap<crate::types::NodeId, String>, node: &str) -> Option<String> {
    let id: crate::types::NodeId = node.parse().ok()?;
    Some(peers.get(&id)?.trim_end_matches('/').to_string())
}

/// The host of node `node`'s base URL among `peers` (the name its
/// certificate is issued for).
fn peer_host(peers: &BTreeMap<crate::types::NodeId, String>, node: &str) -> Option<String> {
    let id: crate::types::NodeId = node.parse().ok()?;
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
