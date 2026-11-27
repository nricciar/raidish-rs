//! The plain data the HTTP API sends and takes: what the servers answer
//! with and what clients (the command line, the web dashboard) read back.
//! Portable, so the dashboard (wasm) uses these very types and can't drift
//! from the server. Each lives here rather than in its server module
//! (which re-exports it); the server-side behaviour stays there.

use crate::types::DatasetFormat;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Serialized as text, `Display` / `FromStr`.
macro_rules! serde_as_str {
    ($t:ty) => {
        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }
        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = std::borrow::Cow::<str>::deserialize(d)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

// ---- events (`GET /api/v1/events`, see `crate::events`) ----

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Info,
    Warn,
    Error,
}

macro_rules! event_kinds {
    ($($(#[$doc:meta])* $v:ident => $s:literal,)*) => {
        /// What sort of event it is; `area.what` as text.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum EventKind {
            $($(#[$doc])* $v,)*
        }

        impl EventKind {
            pub const ALL: &[EventKind] = &[$(EventKind::$v,)*];

            pub fn as_str(self) -> &'static str {
                match self {
                    $(EventKind::$v => $s,)*
                }
            }
        }

        impl FromStr for EventKind {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, String> {
                match s {
                    $($s => Ok(EventKind::$v),)*
                    _ => Err(format!("unknown event kind '{}'", s)),
                }
            }
        }
    };
}

event_kinds! {
    EventsStarted => "events.started",
    /// A node's events were dropped while it had no committer to send to.
    EventsDropped => "events.dropped",
    CommitterStarted => "committer.started",
    CommitterMoved => "committer.moved",
    RaftLeader => "raft.leader",
    NodeUp => "node.up",
    NodeRestarted => "node.restarted",
    NodeLeft => "node.left",
    NodeDown => "node.down",
    NodeWriterLost => "node.writer_lost",
    DiskOffline => "disk.offline",
    DiskOnline => "disk.online",
    ResilverDone => "resilver.done",
    ResilverIncomplete => "resilver.incomplete",
    ResilverFailed => "resilver.failed",
    DatasetCreated => "dataset.created",
    DatasetDeleted => "dataset.deleted",
    DatasetSettings => "dataset.settings",
    DatasetStarted => "dataset.started",
    DatasetStopped => "dataset.stopped",
    DatasetStartFailed => "dataset.start_failed",
    /// Its node went down and another took it.
    DatasetFailover => "dataset.failover",
    DatasetLost => "dataset.lost",
    VolumeCreated => "volume.created",
    VolumeDeleted => "volume.deleted",
    VolumeStarted => "volume.started",
    VolumeStopped => "volume.stopped",
    VolumeStartFailed => "volume.start_failed",
    VolumeNotAttached => "volume.not_attached",
    VolumeLost => "volume.lost",
    VolumeHandedOff => "volume.handed_off",
    VolumeHandedOver => "volume.handed_over",
    VolumeHandoffFailed => "volume.handoff_failed",
    SnapshotCreated => "snapshot.created",
    SnapshotDestroyed => "snapshot.destroyed",
    SnapshotCloned => "snapshot.cloned",
    SnapshotReceived => "snapshot.received",
    SnapshotRollback => "snapshot.rollback",
    VmDefined => "vm.defined",
    VmChanged => "vm.changed",
    VmDeleted => "vm.deleted",
    /// What an admin asked of it.
    VmMove => "vm.move",
    VmStart => "vm.start",
    VmStop => "vm.stop",
    VmKill => "vm.kill",
    VmReboot => "vm.reboot",
    VmReset => "vm.reset",
    VmInvalid => "vm.invalid",
    VmConflict => "vm.conflict",
    VmStarted => "vm.started",
    VmStartFailed => "vm.start_failed",
    VmStopping => "vm.stopping",
    VmStopped => "vm.stopped",
    VmForcedOff => "vm.forced_off",
    VmKilled => "vm.killed",
    /// Running here, but neither defined nor anchored here.
    VmOrphan => "vm.orphan",
    VmMigrated => "vm.migrated",
    VmMigrateFailed => "vm.migrate_failed",
    VmMovingCold => "vm.moving_cold",
    VmArrived => "vm.arrived",
    VmMigrationRecovered => "vm.migration_recovered",
    /// A node was unsealed (it can sign).
    CaUnsealed => "ca.unsealed",
    /// Every node was sealed (`ca seal`).
    CaSealed => "ca.sealed",
    CaSlotAdded => "ca.slot_added",
    CaSlotRemoved => "ca.slot_removed",
    /// The cluster DEK was replaced (`ca rekey`).
    CaRekeyed => "ca.rekeyed",
    /// No node is unsealed, and something runs out before one is.
    CaUnsealedNone => "ca.unsealed_none",
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What an event is about; `<kind>/<name>` as text (`node/node-1`,
/// `volume/db`, `bucket/logs`, `disk/2`), or `pool`, `cluster`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Subject {
    Pool,
    Cluster,
    Node(String),
    /// A pool disk, by index.
    Disk(usize),
    Vm(String),
    /// A dataset: a volume, bucket, queue... (None: of a format not known).
    Dataset(Option<DatasetFormat>, String),
}

impl Subject {
    pub fn node(n: impl std::fmt::Display) -> Self {
        Subject::Node(n.to_string())
    }
    pub fn vm(n: impl std::fmt::Display) -> Self {
        Subject::Vm(n.to_string())
    }
    pub fn volume(n: impl std::fmt::Display) -> Self {
        Subject::Dataset(Some(DatasetFormat::Block), n.to_string())
    }
    pub fn dataset(f: Option<DatasetFormat>, n: impl std::fmt::Display) -> Self {
        Subject::Dataset(f, n.to_string())
    }
}

/// A dataset subject's kind, as text (`volume`, `bucket`...).
pub fn format_slug(f: Option<DatasetFormat>) -> &'static str {
    match f {
        None => "dataset",
        Some(DatasetFormat::Block) => "volume",
        Some(DatasetFormat::S3) => "bucket",
        Some(DatasetFormat::Queue) => "queue",
        Some(DatasetFormat::Etcd) => "etcd",
        Some(DatasetFormat::Kv) => "kv",
        Some(DatasetFormat::Posix) => "filesystem",
        Some(DatasetFormat::Events) => "events",
        Some(DatasetFormat::Vms) => "vms",
    }
}

const DATASET_FORMATS: [Option<DatasetFormat>; 9] = [
    None,
    Some(DatasetFormat::Block),
    Some(DatasetFormat::S3),
    Some(DatasetFormat::Queue),
    Some(DatasetFormat::Etcd),
    Some(DatasetFormat::Kv),
    Some(DatasetFormat::Posix),
    Some(DatasetFormat::Events),
    Some(DatasetFormat::Vms),
];

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subject::Pool => f.write_str("pool"),
            Subject::Cluster => f.write_str("cluster"),
            Subject::Node(n) => write!(f, "node/{}", n),
            Subject::Disk(d) => write!(f, "disk/{}", d),
            Subject::Vm(n) => write!(f, "vm/{}", n),
            Subject::Dataset(k, n) => write!(f, "{}/{}", format_slug(*k), n),
        }
    }
}

impl FromStr for Subject {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("bad event subject '{}'", s);
        match s.split_once('/') {
            None if s == "pool" => Ok(Subject::Pool),
            None if s == "cluster" => Ok(Subject::Cluster),
            None => Err(bad()),
            Some((_, "")) => Err(bad()),
            Some(("node", n)) => Ok(Subject::node(n)),
            Some(("disk", d)) => d.parse().map(Subject::Disk).map_err(|_| bad()),
            Some(("vm", n)) => Ok(Subject::vm(n)),
            Some((k, n)) => DATASET_FORMATS
                .into_iter()
                .find(|f| format_slug(*f) == k)
                .map(|f| Subject::dataset(f, n))
                .ok_or_else(bad),
        }
    }
}

serde_as_str!(EventKind);
serde_as_str!(Subject);

/// An event as reported, before the log numbers it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NewEvent {
    /// When (ms since the epoch, the reporting node's clock).
    pub ts_ms: u64,
    /// The node it happened on (or that noticed it).
    pub node: String,
    pub level: Level,
    pub kind: EventKind,
    pub subject: Subject,
    pub message: String,
    /// Who asked for it (an admin's name), if someone did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
}

/// An event in the log.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub seq: u64,
    #[serde(flatten)]
    pub event: NewEvent,
}

/// An event's fields are its report's.
impl std::ops::Deref for Event {
    type Target = NewEvent;
    fn deref(&self) -> &NewEvent {
        &self.event
    }
}

/// A page of the log, oldest first.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct EventPage {
    pub events: Vec<Event>,
    /// The number the next event will get (what to ask `after` from:
    /// `next - 1`).
    pub next: u64,
    /// The oldest event's number still kept (0: none).
    pub first: u64,
}

// ---- nodes (`GET /api/v1/nodes/facts`, see `crate::node_facts`) ----

use crate::types::Bytes;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct NodeFacts {
    /// `node-1`.
    pub node: String,
    /// When collected (ms since the epoch).
    pub collected_at: u64,
    /// The raidish version that collected them.
    pub raidish: String,
    pub host: HostFacts,
    /// Peer I/O is on (`tls:` in the config): volumes can be shared with,
    /// and handed to, other nodes without stopping (live migration).
    pub peer_io: bool,
    /// None: no `vm:` section, this node runs no VMs.
    pub libvirt: Option<LibvirtFacts>,
    /// The other nodes, as this one reaches them.
    pub peers: BTreeMap<String, PeerFacts>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct HostFacts {
    pub hostname: String,
    pub kernel: String,
    pub arch: String,
    /// `AuthenticAMD`, `GenuineIntel`.
    pub cpu_vendor: String,
    /// `AMD Ryzen 9 5900X 12-Core Processor`.
    pub cpu_model: String,
    pub cpu_threads: u32,
    pub memory: Bytes,
    /// `/dev/kvm` exists.
    pub kvm: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct LibvirtFacts {
    pub uri: String,
    /// libvirt couldn't be asked: why (everything else is then empty).
    pub error: Option<String>,
    /// `12.0.0`.
    pub libvirt_version: String,
    /// `10.2.1`.
    pub qemu_version: String,
    /// `kvm` or `qemu` (emulated): what domcapabilities says guests get.
    pub virt_type: String,
    /// What `host-model` gives a guest here.
    pub cpu: CpuFacts,
    /// Named CPU models a guest can be given here.
    pub cpu_models: Vec<String>,
    /// Machine types (`pc-q35-10.2`, `q35`, ...).
    pub machines: Vec<String>,
    /// Active libvirt networks.
    pub networks: Vec<String>,
    /// Linux bridges.
    pub bridges: Vec<String>,
    /// The whole `virsh domcapabilities`, for `vm cpu-baseline`.
    pub domcapabilities: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct CpuFacts {
    /// `EPYC-Milan`.
    pub model: String,
    pub vendor: String,
    /// Features on top of the model.
    pub features: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct PeerFacts {
    /// How it's reached (the host in its URL).
    pub host: String,
    /// Its libvirt, over TLS (None: this node has no libvirt to try from).
    pub libvirt: Option<Probe>,
    /// Its peer I/O port.
    pub peer_io: Option<Probe>,
    /// The last live migration from here to it, if it failed on TLS
    /// (qemu's stream: nothing else tests that).
    pub migrate_tls_error: Option<String>,
}

/// One check: ok, or why not and what to do.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct Probe {
    pub ok: bool,
    pub checked_at: u64,
    pub error: Option<String>,
    pub hint: Option<String>,
    /// The certificate the server sent, if it got that far.
    pub cert: Option<CertSummary>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct CertSummary {
    pub subject: String,
    pub ou: Option<String>,
    pub issuer: String,
    pub sans: Vec<String>,
    /// Unix seconds.
    pub not_after: i64,
}

// ---- moving VMs (`GET /api/v1/vms/<name>/check`, see `crate::vm_check`) ----

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// It can't run there.
    No,
    /// Shut down here, started there.
    Cold,
    /// Migrated live.
    Live,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::No => "no",
            Mode::Cold => "cold",
            Mode::Live => "live",
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self.as_str())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Reason {
    pub level: ReasonLevel,
    pub text: String,
}

impl Verdict {
    /// The reasons that set its mode, joined.
    pub fn why(&self) -> String {
        let want = match self.mode {
            Mode::No => ReasonLevel::No,
            Mode::Cold => ReasonLevel::Cold,
            Mode::Live => ReasonLevel::Warn,
        };
        self.reasons
            .iter()
            .filter(|r| r.level == want)
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// What a [`Reason`] rules out.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasonLevel {
    /// It can't run there.
    No,
    /// Not live.
    Cold,
    /// Nothing, but worth knowing.
    Warn,
}

impl std::fmt::Display for ReasonLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(match self {
            ReasonLevel::No => "no",
            ReasonLevel::Cold => "cold",
            ReasonLevel::Warn => "warn",
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Verdict {
    pub node: String,
    pub mode: Mode,
    pub reasons: Vec<Reason>,
}

// ---- VMs (`/api/v1/vms`, see `crate::vms_api`) ----

use crate::vm_def::{DomState, VmSpec};

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
    pub state: Option<DomState>,
    /// The node couldn't be asked: `state` is unknown.
    pub unreachable: Option<String>,
    /// Started from an older version of its XML.
    pub restart_pending: bool,
    pub vcpus: Option<u32>,
    pub memory: Option<Bytes>,
    pub cpu_s: Option<f64>,
    pub console: bool,
    /// Why its node couldn't start it.
    pub start_error: Option<String>,
}

impl VmView {
    /// Is libvirt running it (in any state)?
    pub fn running(&self) -> bool {
        self.state.is_some()
    }
    /// vCPUs: libvirt's while it runs, else the XML's.
    pub fn vcpus(&self) -> Option<u32> {
        self.vcpus.or(self.spec.hw.vcpus)
    }
    /// Memory: libvirt's while it runs, else the XML's.
    pub fn memory(&self) -> Option<Bytes> {
        self.memory.or(self.spec.hw.memory)
    }
}

/// A stored definition.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VmDef {
    pub name: crate::types::VmName,
    pub xml: String,
    pub change: u64,
}

/// Move a VM (`POST /api/v1/vms/<name>/move`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MoveTo {
    pub node: String,
    /// Refuse unless it can move live.
    #[serde(default)]
    pub live: bool,
}

/// Every node's facts, and which nodes are up (`GET /api/v1/nodes/facts`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FactsView {
    pub nodes: BTreeMap<String, NodeFacts>,
    /// Nodes up now; the others' facts are as they last were.
    pub live: Vec<String>,
}

/// Where a VM could move, and how (`GET /api/v1/vms/<name>/check`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CheckView {
    pub vm: String,
    /// Where it runs now (a move from there is checked as live); None: not
    /// running, so a move is where it starts next.
    pub running_on: Option<String>,
    pub verdicts: Vec<Verdict>,
}

// ---- volumes (`/api/v1/volumes`, see `crate::volumes_api`) ----

use crate::types::SnapshotInfo;
use crate::types::VolumeInfo;

/// A volume and its snapshots (oldest first).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct VolumeView {
    #[serde(flatten)]
    pub info: VolumeInfo,
    pub snapshots: Vec<SnapshotInfo>,
}

/// A volume to make.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateVolume {
    pub name: crate::types::DatasetName,
    pub size_bytes: u64,
}

/// A snapshot or clone to make.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NewName {
    pub name: String,
}

// ---- files (`/api/v1/dir`, `/api/v1/stat`..., see `crate::files_api`) ----

use crate::types::FileId;
use crate::types::ObjType;

/// A directory entry (`GET /api/v1/dir/<path>`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    /// The pool-wide object number.
    pub id: FileId,
    pub kind: ObjType,
    pub size: u64,
    #[serde(default)]
    pub mode: u16,
    /// Last modification, ns since the epoch.
    #[serde(default)]
    pub mtime_ns: u64,
}

/// An object's size and attributes (`GET /api/v1/stat`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Stat {
    pub id: FileId,
    pub kind: ObjType,
    pub size: u64,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub mtime_ns: u64,
    pub ctime_ns: u64,
}

impl DirEntry {
    pub fn is_dir(&self) -> bool {
        self.kind == ObjType::Directory
    }
}

/// Rename `from` to `to` (`POST /api/v1/rename`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameRequest {
    pub from: String,
    pub to: String,
}

// ---- who's asking (`GET /api/v1/auth/whoami`, see `crate::auth`) ----

/// What a certificate (or token) may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Dashboard and other read-only access.
    Readonly,
    /// Everything the command line does.
    Admin,
    /// A `listen` node: everything, plus Raft between peers.
    Node,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Readonly => "readonly",
            Role::Admin => "admin",
            Role::Node => "node",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "readonly" => Ok(Role::Readonly),
            "admin" => Ok(Role::Admin),
            "node" => Ok(Role::Node),
            _ => Err(format!(
                "unknown role '{}' (expected node, admin or readonly)",
                s
            )),
        }
    }
}

/// How a request proved who it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Via {
    /// Plain HTTP: TLS is off, so there's no authentication.
    Open,
    Cert,
    Token(String),
}

/// Who a request is from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    pub via: Via,
    /// A token's expiry (unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
}

impl Principal {
    pub fn can_write(&self) -> bool {
        self.role >= Role::Admin
    }
}

#[cfg(test)]
#[path = "tests/api_types_tests.rs"]
mod tests;

// ---- the agent (`GET /api/v1/agent/status`, see `crate::agent`) ----

use crate::types::Transport;

/// A volume this agent runs, as reported by its status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RunningVolume {
    pub name: crate::types::DatasetName,
    pub transport: Transport,
    /// Its kernel device here.
    pub attach: Attach,
    /// Exported here but written by another node (see `handoff`).
    #[serde(default)]
    pub proxied: bool,
}

/// A running volume's kernel device on its node.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase", tag = "state")]
pub enum Attach {
    /// Exported only: no device wanted (a VM's disk), or the agent attaches
    /// none (`automount.helper` is none).
    #[default]
    Exported,
    Attached {
        device: String,
    },
    /// Attaching failed (helper missing, a kernel module, ...): it's tried
    /// again.
    Failed {
        error: String,
    },
}

impl Attach {
    /// What an attach (see `Agent::attach`) came to.
    pub fn of(r: Result<Option<String>, String>) -> Self {
        match r {
            Ok(Some(device)) => Attach::Attached { device },
            Ok(None) => Attach::Exported,
            Err(error) => Attach::Failed { error },
        }
    }

    /// From a proxied export's device, if it has one.
    pub fn device_of(device: Option<String>) -> Self {
        device.map_or(Attach::Exported, |device| Attach::Attached { device })
    }

    pub fn device(&self) -> Option<&str> {
        match self {
            Attach::Attached { device } => Some(device),
            _ => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Attach::Failed { error } => Some(error),
            _ => None,
        }
    }

    /// " on <device>", if attached: for messages.
    pub fn on(&self) -> String {
        self.device()
            .map(|d| format!(" on {}", d))
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AgentStatus {
    pub node: String,
    pub volumes: Vec<RunningVolume>,
    /// Why VMs that should run here don't (as of the last tick).
    #[serde(default)]
    pub vm_errors: BTreeMap<String, String>,
}

impl Probe {
    /// "ok", or the error and hint.
    pub fn describe(&self) -> String {
        if self.ok {
            return "ok".into();
        }
        let mut s = self.error.clone().unwrap_or_else(|| "failed".into());
        if let Some(h) = &self.hint {
            s.push_str(" (");
            s.push_str(h);
            s.push(')');
        }
        s
    }
}

impl NodeFacts {
    /// The same facts, timestamps aside.
    pub fn same_as(&self, other: &NodeFacts) -> bool {
        fn strip(f: &NodeFacts) -> NodeFacts {
            let mut f = f.clone();
            f.collected_at = 0;
            for p in f.peers.values_mut() {
                for pr in [&mut p.libvirt, &mut p.peer_io].into_iter().flatten() {
                    pr.checked_at = 0;
                }
            }
            f
        }
        strip(self) == strip(other)
    }

    /// The qemu version as numbers, for comparing.
    pub fn qemu_version(&self) -> Option<Vec<u64>> {
        version_nums(&self.libvirt.as_ref()?.qemu_version)
    }
}

pub fn version_nums(v: &str) -> Option<Vec<u64>> {
    let n: Vec<u64> = v
        .split('.')
        .map(|p| p.trim().parse().ok())
        .collect::<Option<_>>()?;
    (!n.is_empty()).then_some(n)
}
