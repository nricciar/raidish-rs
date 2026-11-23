//! Virtual machines whose disks are raidish volumes, run through libvirt.
//!
//! A VM is a libvirt domain XML kept in the pool's `.vms` dataset (see
//! `crate::vm_store`), stored and changed only through the VM API
//! (`PUT /api/v1/vms/<name>`, `raidish vm import`, the dashboard), which
//! checks each one as it's stored. A disk is a raidish volume when its
//! `<source>` says so:
//!
//! ```xml
//! <source file='raidish:vm1'/>                 <!-- or dev=, or -->
//! <source protocol='nbd' name='vm1'/>
//! ```
//!
//! **What a definition may hold.** Whoever can store one gets libvirt (as
//! root, on every node that runs VMs) to do what it says, so it may only
//! say what keeps a guest inside its VM (`check_domain`): every disk is a
//! raidish volume (or an empty cdrom), and nothing reaches the host (host
//! files or devices, passthrough, host scripts, file-backed character
//! devices, the qemu command line, direct kernel boot, firmware from
//! outside `/usr/share`). What `virsh dumpxml` adds that raidish decides
//! itself (the emulator, security labels, graphics, pty paths) is
//! dropped when the domain is rendered.
//!
//! Nothing about a VM lives in `/etc/libvirt` on any host. The node that
//! runs a VM's disks renders the XML (disks pointed at its own NBD export,
//! a VNC console on 127.0.0.1, raidish metadata) and starts it as a
//! *transient* domain, so libvirt forgets it once it stops.
//!
//! Where a VM runs follows its **anchor**, the first raidish disk: that
//! volume's placement settings (`DatasetProps`: nodes, `move_to`, enabled)
//! place the VM, and its other disks go wherever the anchor goes. A volume
//! belongs to one VM at most. Whether it *should* run is one replicated bit
//! (`ClusterState::vms`). Whether it *does* is never stored: it is what
//! libvirt on the anchor's node says.

use quick_xml::events::{BytesText, Event};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

pub use crate::vm_def::{DomState, VmDisk, VmDisks, VmHardware, VmNic, VmSpec};
pub(crate) use crate::xml::{El, Node, el_xml, parse_root};

/// The XML namespace of the metadata raidish puts on the domains it runs.
pub const META_NS: &str = "https://raidish.dev/xmlns/vm/1";

/// The `vm` section of the config. Absent: this node runs no VMs. Present,
/// even empty (`vm: {}`), it runs them through the local libvirt, and
/// takes and makes live migrations, with nothing else set: every field is
/// an override of a default.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct VmConfig {
    /// libvirt connection (default qemu:///system).
    pub uri: Option<String>,
    /// The virsh binary (default virsh).
    pub virsh: Option<String>,
    /// How long a guest gets to shut down before it's destroyed (default 60).
    pub shutdown_timeout_s: Option<u64>,
    /// Where qemu reaches this node's NBD exports (default: the
    /// `automount` bind address and port, 127.0.0.1 for a wildcard). For
    /// a libvirt that isn't on this machine's network stack: raidish in a
    /// container with its NBD port published, say.
    pub nbd_host: Option<String>,
    pub nbd_port: Option<u16>,
    /// The address a VM's VNC console listens on, and this node connects
    /// to for the dashboard's console (default 127.0.0.1). Unpassworded:
    /// keep it on an address only this node can reach.
    pub console_listen: Option<String>,
    /// How other nodes' libvirt reaches this node's, for live migrations
    /// to here (default `qemu+tls://<host>/system`, `<host>` as the other
    /// nodes' configs name this node). TLS only.
    pub migrate_uri: Option<String>,
    /// Where qemu's (TLS) migration stream to this node connects (default:
    /// that same host).
    pub migrate_host: Option<String>,
    /// How long a live migration from this node may take before it's
    /// aborted (default 300).
    pub migrate_timeout_s: Option<u64>,
}

impl VmConfig {
    /// Where qemu reaches this node's NBD exports, given the default.
    pub fn nbd(&self, default: (String, u16)) -> (String, u16) {
        (
            self.nbd_host.clone().unwrap_or(default.0),
            self.nbd_port.unwrap_or(default.1),
        )
    }
    pub fn console_listen(&self) -> String {
        self.console_listen
            .clone()
            .unwrap_or_else(|| "127.0.0.1".into())
    }
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_s.unwrap_or(60))
    }
    pub fn migrate_timeout(&self) -> Duration {
        Duration::from_secs(self.migrate_timeout_s.unwrap_or(300))
    }
    /// What's wrong with it, if anything: migrations are TLS only.
    pub fn check(&self) -> Result<(), VmError> {
        match &self.migrate_uri {
            Some(u) if !u.starts_with("qemu+tls://") => invalid(format!(
                "vm.migrate_uri '{}': live migrations are TLS only (qemu+tls://...)",
                u
            )),
            _ => Ok(()),
        }
    }
    pub fn virsh(&self) -> Virsh {
        Virsh {
            bin: self.virsh.clone().unwrap_or_else(|| "virsh".into()),
            uri: self.uri.clone().unwrap_or_else(|| "qemu:///system".into()),
        }
    }
}

/// Shared by the agent and the API on a node that runs VMs.
pub struct VmRuntime {
    pub virsh: Virsh,
    pub shutdown_timeout: Duration,
    /// Wakes the agent to reconcile now rather than at its next tick.
    poke: tokio::sync::Notify,
    /// Where qemu finds this node's NBD exports.
    pub nbd: (String, u16),
    /// Where VNC consoles listen (see `VmConfig::console_listen`).
    pub console: String,
    /// This node's name.
    pub node: String,
    /// The pool's GUID (hex), once the agent has read it: until then no
    /// domain is this node's.
    pool: std::sync::RwLock<Option<String>>,
    /// VMs to shut down (destroyed after the timeout) and start again from
    /// their current XML.
    restart: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// See `VmConfig::migrate_uri`, `migrate_host`, `migrate_timeout_s`.
    pub migrate_uri: Option<String>,
    pub migrate_host: Option<String>,
    pub migrate_timeout: Duration,
}

/// How a node that takes a live migration is reached, as it answers
/// `POST /api/v1/vm/incoming/<name>` (see `crate::agent`'s migrations).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Incoming {
    /// Its libvirt, from the source's libvirt (`virsh migrate` p2p), if
    /// not the default (see `Incoming::resolved`).
    pub uri: Option<String>,
    /// Where qemu's migration stream connects, if not the default.
    pub migrate_host: Option<String>,
    /// Where its qemu finds its NBD exports.
    pub nbd_host: String,
    pub nbd_port: u16,
    /// Its VNC console address.
    pub console: String,
    /// Its name.
    pub node: String,
}

impl Incoming {
    /// With the defaults filled in for a node reached as `host` (the host
    /// of its URL in the source's config): libvirt at
    /// `qemu+tls://<host>/system`, qemu's stream to `<host>`.
    pub fn resolved(mut self, host: &str) -> Incoming {
        let h = if host.contains(':') {
            format!("[{}]", host)
        } else {
            host.to_string()
        };
        self.uri
            .get_or_insert_with(|| format!("qemu+tls://{}/system", h));
        self.migrate_host.get_or_insert_with(|| host.to_string());
        self
    }
}

/// Whose domains are whose (see `DomMeta::pool`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmIdent {
    pub pool: String,
    pub node: String,
}

impl VmIdent {
    pub fn owns(&self, m: &DomMeta) -> bool {
        m.pool == self.pool && m.node == self.node
    }
}

impl VmRuntime {
    /// Node `node`'s VMs as `c` says to run them; `nbd`: where qemu finds
    /// the node's NBD exports unless `c` says otherwise.
    pub fn new(c: &VmConfig, nbd: (String, u16), node: String) -> Self {
        VmRuntime {
            virsh: c.virsh(),
            shutdown_timeout: c.shutdown_timeout(),
            poke: Default::default(),
            nbd: c.nbd(nbd),
            console: c.console_listen(),
            node,
            pool: Default::default(),
            restart: Default::default(),
            migrate_uri: c.migrate_uri.clone(),
            migrate_host: c.migrate_host.clone(),
            migrate_timeout: c.migrate_timeout(),
        }
    }

    /// Have the agent reconcile now.
    pub fn poke(&self) {
        self.poke.notify_one();
    }

    /// Until someone calls `poke`.
    pub async fn poked(&self) {
        self.poke.notified().await
    }

    /// The pool's GUID is known: domains can be told apart as this node's.
    pub fn set_pool(&self, guid: u64) {
        *self.pool.write().unwrap() = Some(format!("{:016x}", guid));
    }

    /// Shut VM `name` down and start it again from its current XML.
    pub fn restart(&self, name: &str) {
        self.restart.lock().unwrap().insert(name.to_string());
    }

    /// Whether VM `name` is to be restarted.
    pub fn restarting(&self, name: &str) -> bool {
        self.restart.lock().unwrap().contains(name)
    }

    /// Forget the restarts of VMs `running` no longer says are up (done:
    /// their old domain is gone).
    pub fn restarts_done(&self, running: impl Fn(&str) -> bool) {
        self.restart.lock().unwrap().retain(|n| running(n));
    }

    /// Who this node is to libvirt; None until the pool's GUID is known.
    pub fn ident(&self) -> Option<VmIdent> {
        Some(VmIdent {
            pool: self.pool.read().unwrap().clone()?,
            node: self.node.clone(),
        })
    }

    /// `d`, if this node started it.
    pub async fn our_vm(&self, d: Domain) -> Option<LocalVm> {
        let me = self.ident()?;
        self.virsh.local_vm(d).await.filter(|v| me.owns(&v.meta))
    }

    /// Every domain this node started. Err: libvirt can't be asked, or
    /// the pool's GUID isn't known yet (which domains are ours is unknown).
    pub async fn local_vms(&self) -> Result<Vec<LocalVm>, String> {
        let me = self
            .ident()
            .ok_or("the pool isn't open here yet: which domains are this node's is unknown")?;
        Ok(self
            .virsh
            .local_vms()
            .await?
            .into_iter()
            .filter(|v| me.owns(&v.meta))
            .collect())
    }
}

/// An element's text.
fn hardware(root: &El) -> VmHardware {
    let mut hw = VmHardware {
        vcpus: root.child("vcpu").and_then(|v| v.text().parse().ok()),
        memory: root
            .child("currentMemory")
            .or_else(|| root.child("memory"))
            .and_then(|m| crate::vm_def::libvirt_memory(&m.text(), m.attr("unit"))),
        ..Default::default()
    };
    if let Some(os) = root.child("os") {
        if let Some(t) = os.child("type") {
            hw.arch = t.attr("arch").map(str::to_string);
            hw.machine = t.attr("machine").map(str::to_string);
        }
        hw.boot = os
            .elements()
            .filter(|e| e.name == "boot")
            .filter_map(|e| e.attr("dev").map(str::to_string))
            .collect();
    }
    let Some(devices) = root.child("devices") else {
        return hw;
    };
    for d in devices.elements() {
        match d.name.as_str() {
            "disk" => {
                let target = d.child("target");
                let src = d.child("source");
                let volume = disk_volume(d);
                hw.disks.push(VmDisk {
                    device: d.attr("device").unwrap_or("disk").to_string(),
                    target: target.and_then(|t| t.attr("dev")).map(str::to_string),
                    bus: target.and_then(|t| t.attr("bus")).map(str::to_string),
                    source: if volume.is_some() {
                        None
                    } else {
                        src.and_then(|s| {
                            ["file", "dev", "name", "volume"]
                                .iter()
                                .find_map(|k| s.attr(k))
                                .map(str::to_string)
                        })
                    },
                    volume,
                });
            }
            "interface" => {
                let src = d.child("source");
                hw.nics.push(VmNic {
                    kind: d.attr("type").unwrap_or_default().to_string(),
                    source: src.and_then(|s| {
                        ["network", "bridge", "dev"]
                            .iter()
                            .find_map(|k| s.attr(k))
                            .map(str::to_string)
                    }),
                    mac: d
                        .child("mac")
                        .and_then(|m| m.attr("address"))
                        .map(str::to_string),
                    model: d
                        .child("model")
                        .and_then(|m| m.attr("type"))
                        .map(str::to_string),
                });
            }
            _ => {}
        }
    }
    hw
}

impl VmSpec {
    pub fn parse(name: &str, xml: &str, change: u64) -> VmSpec {
        let mut spec = VmSpec {
            name: name.to_string(),
            change,
            disks: VmDisks::Unusable {
                error: String::new(),
            },
            hw: VmHardware::default(),
            xml: xml.to_string(),
        };
        let root = parse_domain(xml);
        if let Ok(root) = &root {
            spec.hw = hardware(root);
        }
        spec.disks = match root.and_then(|root| {
            check_domain(&root)?;
            raidish_disks(&root)
        }) {
            Ok(vols) => VmDisks::of(vols),
            Err(e) => VmDisks::Unusable {
                error: e.to_string(),
            },
        };
        spec
    }

    pub fn usable(&self) -> bool {
        self.error().is_none()
    }

    /// The volume that places it (its first raidish disk), if usable.
    pub fn anchor(&self) -> Option<&str> {
        self.disks.anchor()
    }

    /// Every raidish volume it uses, the anchor first (none if unusable).
    pub fn volumes(&self) -> &[String] {
        self.disks.volumes()
    }

    /// Why the XML is unusable, if it is.
    pub fn error(&self) -> Option<&str> {
        self.disks.error()
    }
}

/// Mark each VM that uses a volume an earlier one (by name) already uses
/// as unusable: two guests writing one disk corrupt it. The store refuses
/// such definitions (`crate::vm_store`); this keeps any that got past it
/// from being started.
pub fn mark_conflicts(specs: &mut BTreeMap<String, VmSpec>) {
    let mut owner: BTreeMap<String, String> = BTreeMap::new();
    for spec in specs.values_mut() {
        if !spec.usable() {
            continue;
        }
        let taken: Vec<String> = spec
            .volumes()
            .iter()
            .filter_map(|v| {
                owner
                    .get(v)
                    .map(|o| format!("volume '{}' is VM '{}''s", v, o))
            })
            .collect();
        if !taken.is_empty() {
            spec.disks = VmDisks::Unusable {
                error: taken.join("; "),
            };
            continue;
        }
        for v in spec.volumes() {
            owner.insert(v.clone(), spec.name.clone());
        }
    }
}

// ---- a tiny XML tree, enough to rewrite a domain --------------------------

/// Why a VM couldn't be read, checked or run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmError {
    /// The definition (or other XML) isn't one raidish will run:
    /// malformed, or reaching beyond its VM.
    Invalid(String),
    /// virsh ran `cmd` and libvirt said no; `reason` is libvirt's.
    Virsh { cmd: String, reason: String },
    /// virsh couldn't be run (or its scratch file written).
    Host(String),
    /// It didn't finish in time, and was aborted.
    Timeout(Duration),
}

impl VmError {
    pub fn invalid(e: impl ToString) -> Self {
        VmError::Invalid(e.to_string())
    }
    pub fn kind(&self) -> crate::types::ErrorKind {
        use crate::types::ErrorKind;
        match self {
            VmError::Invalid(_) => ErrorKind::Invalid,
            VmError::Timeout(_) => ErrorKind::Unavailable,
            VmError::Virsh { .. } | VmError::Host(_) => ErrorKind::Internal,
        }
    }
}

impl std::fmt::Display for VmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmError::Invalid(m) | VmError::Host(m) => f.write_str(m),
            VmError::Virsh { cmd, reason } => write!(f, "virsh {}: {}", cmd, reason),
            VmError::Timeout(t) => write!(f, "didn't finish in {:?}; aborted", t),
        }
    }
}

impl std::error::Error for VmError {}

/// Malformed XML is a definition raidish won't run.
impl From<crate::xml::XmlError> for VmError {
    fn from(e: crate::xml::XmlError) -> Self {
        VmError::Invalid(e.to_string())
    }
}

/// For callers that only report it.
impl From<VmError> for String {
    fn from(e: VmError) -> String {
        e.to_string()
    }
}

fn invalid<T>(m: impl ToString) -> Result<T, VmError> {
    Err(VmError::invalid(m))
}

fn parse_domain(xml: &str) -> Result<El, VmError> {
    let root = parse_root(xml)?;
    if root.name != "domain" {
        return invalid(format!("expected a <domain>, found <{}>", root.name));
    }
    Ok(root)
}

/// The raidish volume a `<disk>` uses, if it is one.
fn disk_volume(disk: &El) -> Option<String> {
    let src = disk.child("source")?;
    for k in ["file", "dev", "name"] {
        if let Some(v) = src.attr(k).and_then(|v| v.strip_prefix("raidish:")) {
            return Some(v.to_string());
        }
    }
    if src.attr("protocol") == Some("nbd") {
        return src.attr("name").map(str::to_string);
    }
    None
}

// ---- what a definition may hold ---------------------------------------------

/// Elements a `<domain>` may have. Each says what it is about the guest
/// (the host decides the rest).
const DOMAIN_KIDS: &[&str] = &[
    "name",
    "uuid",
    "genid",
    "title",
    "description",
    "metadata",
    "maxMemory",
    "memory",
    "currentMemory",
    "memoryBacking",
    "memtune",
    "vcpu",
    "vcpus",
    "iothreads",
    "iothreadids",
    "cputune",
    "numatune",
    "os",
    "features",
    "cpu",
    "clock",
    "on_poweroff",
    "on_reboot",
    "on_crash",
    "on_lockfailure",
    "pm",
    "perf",
    "sysinfo",
    "devices",
    "keywrap",
    // Dropped by `render` (raidish decides these).
    "seclabel",
    "resource",
];

/// What `<os>` may have: no direct kernel boot (`kernel`, `initrd`, `dtb`
/// are host files), no ACPI tables from host files.
const OS_KIDS: &[&str] = &[
    "type", "boot", "bootmenu", "smbios", "bios", "loader", "nvram", "firmware",
];

/// What a raidish disk may have besides what `render` replaces.
const DISK_KIDS: &[&str] = &[
    "driver",
    "source",
    "auth",
    "target",
    "boot",
    "readonly",
    "shareable",
    "serial",
    "address",
    "alias",
    "iotune",
    "blockio",
    "geometry",
    "wwn",
    "vendor",
    "product",
    "backingStore",
];

/// What an `<interface>` may have: no host scripts, option ROM files or
/// tap/vhost device paths.
const NIC_KIDS: &[&str] = &[
    "mac",
    "source",
    "model",
    "address",
    "boot",
    "alias",
    "mtu",
    "link",
    "driver",
    "virtualport",
    "vlan",
    "filterref",
    "bandwidth",
    "coalesce",
    "target",
];

/// Devices that stay inside the guest whatever their settings.
const PLAIN_DEVICES: &[&str] = &[
    "controller",
    "video",
    "sound",
    "memballoon",
    "watchdog",
    "panic",
    "hub",
    "iommu",
    "vsock",
    // Dropped by `render`.
    "emulator",
    "graphics",
];

fn refuse(what: impl std::fmt::Display) -> Result<(), VmError> {
    invalid(format!("{} isn't allowed in a VM definition", what))
}

fn kids_in(el: &El, allowed: &[&str]) -> Result<(), VmError> {
    match el.elements().find(|k| !allowed.contains(&k.name.as_str())) {
        Some(k) => refuse(format!("<{}> in <{}>", k.name, el.name)),
        None => Ok(()),
    }
}

fn type_in(el: &El, allowed: &[&str]) -> Result<(), VmError> {
    let t = el.attr("type").unwrap_or("");
    if allowed.contains(&t) {
        Ok(())
    } else {
        refuse(format!("<{} type='{}'>", el.name, t))
    }
}

/// A firmware file libvirt may read: one shipped by the host's packages.
fn shipped_file(path: &str) -> bool {
    path.starts_with("/usr/share/") && !path.split('/').any(|c| c == "..")
}

/// No element from another XML namespace (`qemu:commandline` and the
/// like: libvirt hands those to the hypervisor as they are) and no
/// namespace declarations below the root, whatever the prefix.
fn no_foreign(el: &El, root: bool) -> Result<(), VmError> {
    if el.name.contains(':') {
        return refuse(format!("<{}>", el.name));
    }
    for (k, _) in &el.attrs {
        if k == "xmlns" || (!root && k.starts_with("xmlns:")) {
            return refuse(format!("a namespace declaration ({}) on <{}>", k, el.name));
        }
    }
    for k in el.elements() {
        if root && k.name == "metadata" {
            continue; // replaced by `render`
        }
        no_foreign(k, false)?;
    }
    Ok(())
}

/// Refuse `el` having more than one child named any of `names`: libvirt
/// reads one of them, and which isn't ours to guess, so the one checked
/// must be the only one.
fn single(el: &El, names: &[&str]) -> Result<(), VmError> {
    for n in names {
        if el.elements().filter(|e| e.name == *n).count() > 1 {
            return invalid(format!("<{}> has more than one <{}>", el.name, n));
        }
    }
    Ok(())
}

fn check_device(d: &El) -> Result<(), VmError> {
    single(d, &["source", "backend", "target", "driver"])?;
    match d.name.as_str() {
        "disk" => {
            if disk_volume(d).is_some() {
                kids_in(d, DISK_KIDS)?;
                single(d, &["backingStore"])?;
                // `virsh dumpxml` of a running domain says `<backingStore/>`
                // (none); one with anything in it is a host file.
                if let Some(b) = d.child("backingStore")
                    && (!b.attrs.is_empty() || !b.kids.is_empty())
                {
                    return refuse("a disk's <backingStore>");
                }
                return Ok(());
            }
            let removable = matches!(d.attr("device"), Some("cdrom" | "floppy"));
            if removable && d.child("source").is_none() {
                // An empty drive.
                return kids_in(d, &["driver", "target", "readonly", "address", "alias"]);
            }
            invalid(format!(
                "disk {} isn't a raidish volume (<source file='raidish:VOLUME'/>): \
                 host files and devices can't be given to a VM",
                d.child("target")
                    .and_then(|t| t.attr("dev"))
                    .map_or_else(|| "?".to_string(), |t| format!("'{}'", t))
            ))
        }
        "interface" => {
            type_in(d, &["network", "bridge", "direct", "user"])?;
            kids_in(d, NIC_KIDS)
        }
        "input" => type_in(d, &["tablet", "mouse", "keyboard"]),
        "audio" => type_in(d, &["none", "spice"]),
        // A pty is the guest's console; anything else is a host file,
        // device or socket.
        "serial" | "console" | "parallel" => {
            type_in(d, &["pty"])?;
            kids_in(d, &["source", "target", "alias", "address"])
        }
        "channel" => {
            type_in(d, &["unix", "spicevmc", "pty", "qemu-vdagent"])?;
            kids_in(d, &["source", "target", "alias", "address"])
        }
        "rng" => {
            kids_in(d, &["backend", "rate", "alias", "address", "driver"])?;
            for b in d.elements().filter(|e| e.name == "backend") {
                match b.attr("model") {
                    Some("builtin") => {}
                    Some("random")
                        if matches!(
                            b.text().as_str(),
                            "" | "/dev/urandom" | "/dev/random" | "/dev/hwrng"
                        ) => {}
                    _ => return refuse("an <rng> backend other than the host's random device"),
                }
            }
            Ok(())
        }
        "tpm" => {
            kids_in(d, &["backend", "alias", "address"])?;
            match d.child("backend") {
                Some(b) if b.attr("type") == Some("emulator") => Ok(()),
                _ => refuse("a <tpm> other than an emulated one"),
            }
        }
        "redirdev" => type_in(d, &["spicevmc"]),
        n if PLAIN_DEVICES.contains(&n) => Ok(()),
        n => invalid(format!(
            "<{}> devices aren't allowed in a VM definition (they reach the host)",
            n
        )),
    }
}

/// Refuse a domain that would reach beyond its VM (see the module docs).
fn check_domain(root: &El) -> Result<(), VmError> {
    no_foreign(root, true)?;
    if let Some(t) = root.attr("type")
        && !matches!(t, "kvm" | "qemu")
    {
        return refuse(format!("<domain type='{}'>", t));
    }
    kids_in(root, DOMAIN_KIDS)?;
    single(root, &["os", "devices", "name", "metadata"])?;
    for si in root.elements().filter(|e| e.name == "sysinfo") {
        if si.attr("type").is_some_and(|t| t != "smbios") {
            // fwcfg entries are host files.
            return refuse(format!(
                "<sysinfo type='{}'>",
                si.attr("type").unwrap_or("")
            ));
        }
    }
    if let Some(os) = root.child("os") {
        kids_in(os, OS_KIDS)?;
        for l in os.elements().filter(|e| e.name == "loader") {
            if !l.text().is_empty() && !shipped_file(&l.text()) {
                return refuse(format!(
                    "firmware from '{}' (only /usr/share/...)",
                    l.text()
                ));
            }
        }
        for n in os.elements().filter(|e| e.name == "nvram") {
            // Where the variables go is libvirt's choice: a path would
            // have it write wherever that says.
            if !n.text().is_empty() || n.elements().next().is_some() {
                return refuse("an <nvram> path (leave it empty: libvirt picks one)");
            }
            if let Some(t) = n.attr("template")
                && !shipped_file(t)
            {
                return refuse(format!(
                    "an nvram template from '{}' (only /usr/share/...)",
                    t
                ));
            }
        }
    }
    if let Some(devices) = root.child("devices") {
        for d in devices.elements() {
            check_device(d)?;
        }
    }
    Ok(())
}

/// Every raidish volume in the domain's disks, the anchor (first non-cdrom
/// one) first.
fn raidish_disks(root: &El) -> Result<Vec<String>, VmError> {
    let Some(devices) = root.child("devices") else {
        return Ok(Vec::new());
    };
    let mut disks: Vec<(bool, String)> = Vec::new();
    for d in devices.elements().filter(|e| e.name == "disk") {
        if let Some(v) = disk_volume(d) {
            crate::types::DatasetName::user(&v).map_err(VmError::invalid)?;
            if disks.iter().any(|(_, x)| *x == v) {
                return invalid(format!("volume '{}' is used twice", v));
            }
            disks.push((d.attr("device") == Some("cdrom"), v));
        }
    }
    // Stable: a disk before a cdrom, otherwise XML order.
    disks.sort_by_key(|(cdrom, _)| *cdrom);
    Ok(disks.into_iter().map(|(_, v)| v).collect())
}

/// The domain XML libvirt is given: the VM's XML with its name forced,
/// raidish disks pointed at `nbd` (host, port), any graphics replaced by a
/// VNC console on `console` (an address), and raidish's metadata in place of any other.
/// `owner` is the anchor's `ds/` lease owner: a domain whose owner isn't
/// the lease's current one is an orphan of an earlier raidish (its disks'
/// NBD connections go nowhere).
pub fn render(
    spec: &VmSpec,
    nbd: (&str, u16),
    console: &str,
    owner: &str,
    ident: &VmIdent,
) -> Result<String, VmError> {
    let VmDisks::Usable { anchor, volumes } = &spec.disks else {
        return invalid(spec.error().unwrap_or("unusable"));
    };
    let mut root = parse_domain(&spec.xml)?;
    // Checked when it was stored; checked again here, so nothing that got
    // past that (or predates it) reaches libvirt.
    check_domain(&root)?;
    let port = nbd.1.to_string();
    // libvirt's defaults: confined, in its own cgroup partition.
    root.remove("seclabel");
    root.remove("resource");

    // Name.
    let name_el = El {
        name: "name".into(),
        attrs: Vec::new(),
        kids: vec![Node::Raw(Event::Text(
            BytesText::new(&spec.name).into_owned(),
        ))],
    };
    match root.child_mut("name") {
        Some(n) => *n = name_el,
        None => root.kids.insert(0, Node::El(name_el)),
    }

    // Metadata: ours only, right after the name.
    root.remove("metadata");
    let mut meta = El::new("metadata", &[]);
    meta.kids.push(Node::El(meta_el(&DomMeta {
        anchor: anchor.clone(),
        volumes: volumes.clone(),
        change: spec.change,
        owner: DomOwner::Lease(owner.to_string()),
        pool: ident.pool.clone(),
        node: ident.node.clone(),
    })));
    let at = root
        .kids
        .iter()
        .position(|k| matches!(k, Node::El(e) if e.name == "name"))
        .map_or(0, |i| i + 1);
    root.kids.insert(at, Node::El(meta));

    let devices = match root.child_mut("devices") {
        Some(d) => d,
        None => {
            root.kids.push(Node::El(El::new("devices", &[])));
            root.child_mut("devices").unwrap()
        }
    };
    // Disks.
    for k in devices.kids.iter_mut() {
        let Node::El(disk) = k else { continue };
        if disk.name != "disk" {
            continue;
        }
        let Some(vol) = disk_volume(disk) else {
            continue;
        };
        let device = disk.attr("device").unwrap_or("disk").to_string();
        let mut new = El::new("disk", &[("type", "network"), ("device", &device)]);
        new.kids.push(Node::El(El::new(
            "driver",
            &[
                ("name", "qemu"),
                ("type", "raw"),
                ("cache", "none"),
                ("discard", "unmap"),
            ],
        )));
        let mut src = El::new("source", &[("protocol", "nbd"), ("name", &vol)]);
        src.kids.push(Node::El(El::new(
            "host",
            &[("name", nbd.0), ("port", &port)],
        )));
        new.kids.push(Node::El(src));
        for kid in &disk.kids {
            match kid {
                Node::El(e)
                    if matches!(
                        e.name.as_str(),
                        "driver" | "source" | "auth" | "backingStore"
                    ) => {}
                Node::El(e) => new.kids.push(Node::El(e.clone())),
                Node::Raw(_) => {}
            }
        }
        *disk = new;
    }
    // libvirt's own emulator for the domain's type.
    devices.remove("emulator");
    // Character devices: where a pty or socket goes is libvirt's choice
    // (`virsh dumpxml` of a running domain says where it went).
    for k in devices.kids.iter_mut() {
        let Node::El(d) = k else { continue };
        match (d.name.as_str(), d.attr("type")) {
            ("serial" | "console" | "parallel" | "channel", Some("pty")) => d.remove("source"),
            ("channel", Some("unix")) => {
                if let Some(src) = d.child_mut("source") {
                    src.attrs.retain(|(k, _)| k != "path");
                }
            }
            _ => {}
        }
    }
    // Console.
    devices.remove("graphics");
    let mut g = El::new(
        "graphics",
        &[
            ("type", "vnc"),
            ("port", "-1"),
            ("autoport", "yes"),
            ("listen", console),
        ],
    );
    g.kids.push(Node::El(El::new(
        "listen",
        &[("type", "address"), ("address", console)],
    )));
    devices.kids.push(Node::El(g));
    Ok(el_xml(&root))
}

/// What raidish's metadata on a running domain says.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DomMeta {
    pub anchor: String,
    pub volumes: Vec<String>,
    pub change: u64,
    /// Whose it is (see `DomOwner`).
    pub owner: DomOwner,
    /// The pool (its GUID, hex) and node that started it: a node acts only
    /// on its own domains, whatever else shares its libvirt (another
    /// node, another cluster's).
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub node: String,
}

/// Which lease a domain runs under.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DomOwner {
    /// Started (or re-stamped after a migration) under the anchor's lease
    /// held by this owner (see `render`).
    Lease(String),
    /// Migrated in, not yet re-stamped by the node it went to (see
    /// `crate::agent`'s migrations).
    Migrating,
}

impl DomOwner {
    /// Started under `owner`'s lease?
    pub fn is(&self, owner: &str) -> bool {
        matches!(self, DomOwner::Lease(o) if o == owner)
    }
}

/// raidish's metadata element for `m`.
fn meta_el(m: &DomMeta) -> El {
    let volumes = m.volumes.join(",");
    let change = m.change.to_string();
    let mut attrs = vec![
        ("xmlns:raidish", META_NS),
        ("anchor", m.anchor.as_str()),
        ("volumes", volumes.as_str()),
        ("change", change.as_str()),
        ("pool", m.pool.as_str()),
        ("node", m.node.as_str()),
    ];
    match &m.owner {
        DomOwner::Lease(o) => attrs.push(("owner", o.as_str())),
        DomOwner::Migrating => attrs.push(("migrating", "1")),
    }
    El::new("raidish:vm", &attrs)
}

/// The metadata `virsh metadata --set` is given for `m`: unprefixed, no
/// namespace declaration (libvirt puts it in `--uri`'s, as `--key`).
pub fn meta_xml(m: &DomMeta) -> String {
    let mut el = meta_el(m);
    el.name = "vm".into();
    el.attrs.retain(|(k, _)| k != "xmlns:raidish");
    el_xml(&el)
}

fn parse_meta(xml: &str) -> Option<DomMeta> {
    let el = parse_root(xml).ok()?;
    if el.local() != "vm" {
        return None;
    }
    Some(DomMeta {
        anchor: el.attr("anchor")?.to_string(),
        volumes: el
            .attr("volumes")
            .unwrap_or_default()
            .split(',')
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .collect(),
        change: el.attr("change").and_then(|c| c.parse().ok()).unwrap_or(0),
        owner: match el.attr("migrating") {
            Some("1") => DomOwner::Migrating,
            _ => DomOwner::Lease(el.attr("owner").unwrap_or_default().to_string()),
        },
        pool: el.attr("pool").unwrap_or_default().to_string(),
        node: el.attr("node").unwrap_or_default().to_string(),
    })
}

/// A running domain's migratable XML (`virsh dumpxml --migratable`) made
/// fit for node `to`: its raidish (NBD) disks pointed at `to`'s exports,
/// its VNC console on `to`'s address, and raidish's metadata `meta`.
/// Nothing else changes (the UUID and device addresses libvirt's ABI check
/// compares stay as they are).
pub fn retarget(xml: &str, to: &Incoming, meta: &DomMeta) -> Result<String, VmError> {
    let mut root = parse_domain(xml)?;
    let port = to.nbd_port.to_string();
    let md = match root.child_mut("metadata") {
        Some(m) => m,
        None => {
            let at = root
                .kids
                .iter()
                .position(|k| matches!(k, Node::El(e) if e.name == "name"))
                .map_or(0, |i| i + 1);
            root.kids.insert(at, Node::El(El::new("metadata", &[])));
            root.child_mut("metadata").unwrap()
        }
    };
    md.kids
        .retain(|k| !matches!(k, Node::El(e) if e.local() == "vm"));
    md.kids.push(Node::El(meta_el(meta)));
    let devices = root
        .child_mut("devices")
        .ok_or_else(|| VmError::invalid("no <devices>"))?;
    let mut disks = 0;
    for k in devices.kids.iter_mut() {
        let Node::El(d) = k else { continue };
        match d.name.as_str() {
            "disk" if d.attr("type") == Some("network") => {
                let Some(src) = d.child_mut("source") else {
                    continue;
                };
                if src.attr("protocol") != Some("nbd") {
                    continue;
                }
                src.kids
                    .retain(|k| !matches!(k, Node::El(e) if e.name == "host"));
                src.kids.insert(
                    0,
                    Node::El(El::new(
                        "host",
                        &[("name", to.nbd_host.as_str()), ("port", port.as_str())],
                    )),
                );
                disks += 1;
            }
            "graphics" if d.attr("type") == Some("vnc") => {
                for (k, v) in d.attrs.iter_mut() {
                    if k == "listen" {
                        *v = to.console.clone();
                    }
                }
                for l in d.kids.iter_mut() {
                    if let Node::El(l) = l
                        && l.name == "listen"
                        && l.attr("type") == Some("address")
                    {
                        for (k, v) in l.attrs.iter_mut() {
                            if k == "address" {
                                *v = to.console.clone();
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if disks != meta.volumes.len() {
        return invalid(format!(
            "found {} NBD disks, the VM has {} volumes",
            disks,
            meta.volumes.len()
        ));
    }
    Ok(el_xml(&root))
}

// ---- virsh ----------------------------------------------------------------

/// A domain libvirt has on this node.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Domain {
    pub name: String,
    /// libvirt's id while it runs.
    pub id: Option<u32>,
    pub state: DomState,
}

/// A domain this node runs for raidish, as `/api/v1/vm/local` reports it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LocalVm {
    #[serde(flatten)]
    pub domain: Domain,
    pub meta: DomMeta,
    pub vcpus: Option<u32>,
    pub memory: Option<crate::types::Bytes>,
    /// CPU time used, seconds.
    pub cpu_s: Option<f64>,
    /// The VNC console's port on 127.0.0.1.
    pub vnc_port: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct Virsh {
    pub bin: String,
    pub uri: String,
}

impl Virsh {
    async fn run(&self, args: &[&str]) -> Result<String, VmError> {
        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.arg("-q").arg("-c").arg(&self.uri).args(args);
        let out = crate::proc::ran(cmd).await.map_err(VmError::Host)?;
        if !out.ok() {
            let err = out.stderr.trim().trim_start_matches("error: ");
            return Err(VmError::Virsh {
                cmd: args.join(" "),
                reason: err.replace('\n', "; "),
            });
        }
        Ok(out.stdout)
    }

    /// Every domain libvirt knows of on this node.
    pub async fn list(&self) -> Result<Vec<Domain>, VmError> {
        Ok(parse_list(&self.run(&["list", "--all"]).await?))
    }

    /// raidish's metadata on `name`; None: not one of ours.
    pub async fn meta(&self, name: &str) -> Option<DomMeta> {
        let xml = self.run(&["metadata", name, "--uri", META_NS]).await.ok()?;
        parse_meta(&xml)
    }

    /// Start `xml` as a transient domain.
    pub async fn create(&self, name: &str, xml: &str) -> Result<(), VmError> {
        let path =
            std::env::temp_dir().join(format!("raidish-vm-{}-{}.xml", name, std::process::id()));
        tokio::fs::write(&path, xml)
            .await
            .map_err(|e| VmError::Host(format!("{}: {}", path.display(), e)))?;
        let r = self.run(&["create", &path.to_string_lossy()]).await;
        let _ = tokio::fs::remove_file(&path).await;
        r.map(|_| ())
    }

    pub async fn shutdown(&self, name: &str) -> Result<(), VmError> {
        self.run(&["shutdown", name]).await.map(|_| ())
    }
    pub async fn destroy(&self, name: &str) -> Result<(), VmError> {
        self.run(&["destroy", name]).await.map(|_| ())
    }
    pub async fn reboot(&self, name: &str) -> Result<(), VmError> {
        self.run(&["reboot", name]).await.map(|_| ())
    }
    pub async fn reset(&self, name: &str) -> Result<(), VmError> {
        self.run(&["reset", name]).await.map(|_| ())
    }

    /// A running domain's XML as it can be migrated.
    /// Running domain `name` as it runs: its CPU expanded to what the
    /// guest really has (`--migratable` strips that back to the model).
    pub async fn dumpxml_live(&self, name: &str) -> Result<String, VmError> {
        self.run(&["dumpxml", name]).await
    }

    pub async fn dumpxml_migratable(&self, name: &str) -> Result<String, VmError> {
        self.run(&["dumpxml", "--migratable", name]).await
    }

    /// Running domain `name` as its moves are judged
    /// (`crate::vm_check::judged_domain`).
    pub async fn dumpxml_judged(&self, name: &str) -> Result<String, VmError> {
        let live = self.dumpxml_live(name).await?;
        Ok(match self.dumpxml_migratable(name).await {
            Ok(mig) => crate::vm_check::judged_domain(&live, &mig),
            Err(_) => live,
        })
    }

    /// Replace raidish's metadata on running domain `name`.
    pub async fn set_meta(&self, name: &str, meta: &DomMeta) -> Result<(), VmError> {
        self.run(&[
            "metadata",
            name,
            "--uri",
            META_NS,
            "--key",
            "raidish",
            "--set",
            &meta_xml(meta),
            "--live",
        ])
        .await
        .map(|_| ())
    }

    /// Live-migrate `name` to `to`'s libvirt (peer to peer) as `xml`;
    /// aborted after `timeout`.
    pub async fn migrate(
        &self,
        name: &str,
        xml: &str,
        to: &Incoming,
        timeout: Duration,
    ) -> Result<(), VmError> {
        let uri = to
            .uri
            .as_deref()
            .ok_or_else(|| VmError::Host("no libvirt URI for the target".into()))?;
        let host = to.migrate_host.as_deref();
        let path = std::env::temp_dir().join(format!(
            "raidish-migrate-{}-{}.xml",
            name,
            std::process::id()
        ));
        tokio::fs::write(&path, xml)
            .await
            .map_err(|e| VmError::Host(format!("{}: {}", path.display(), e)))?;
        let path_s = path.to_string_lossy().into_owned();
        let migrateuri = host.map(|h| {
            if h.contains(':') {
                format!("tcp://[{}]", h)
            } else {
                format!("tcp://{}", h)
            }
        });
        // Always over TLS: libvirt to libvirt (the URI) and qemu's stream.
        let mut args = vec![
            "migrate", "--live", "--p2p", "--tls", "--xml", &path_s, name, uri,
        ];
        if let Some(m) = &migrateuri {
            args.extend(["--migrateuri", m.as_str()]);
        }
        let r = tokio::time::timeout(timeout, self.run(&args)).await;
        let _ = tokio::fs::remove_file(&path).await;
        match r {
            // libvirt's reason, without the command line.
            Ok(Err(VmError::Virsh { reason, .. })) => Err(VmError::Virsh {
                cmd: "migrate".into(),
                reason,
            }),
            Ok(r) => r.map(|_| ()),
            Err(_) => {
                let _ = self.run(&["domjobabort", name]).await;
                Err(VmError::Timeout(timeout))
            }
        }
    }

    /// `virsh version`, as it prints it.
    pub async fn version(&self) -> Result<String, VmError> {
        self.run(&["version"]).await
    }

    /// `virsh domcapabilities`: what this host's qemu offers a guest
    /// (default arch and virt type).
    pub async fn domcapabilities(&self) -> Result<String, VmError> {
        self.run(&["domcapabilities"]).await
    }

    /// `virsh capabilities`: the host, and the machine types per arch.
    pub async fn capabilities(&self) -> Result<String, VmError> {
        self.run(&["capabilities"]).await
    }

    /// The active networks.
    pub async fn networks(&self) -> Result<Vec<String>, VmError> {
        Ok(self
            .run(&["net-list", "--name"])
            .await?
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect())
    }

    /// `virsh` with `xml` in a temporary file, as `args` then that file.
    async fn with_file(&self, args: &[&str], xml: &str) -> Result<String, VmError> {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "raidish-virsh-{}-{}.xml",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        tokio::fs::write(&path, xml)
            .await
            .map_err(|e| VmError::Host(format!("{}: {}", path.display(), e)))?;
        let p = path.to_string_lossy().into_owned();
        let mut a = args.to_vec();
        a.push(&p);
        let r = self.run(&a).await;
        let _ = tokio::fs::remove_file(&path).await;
        r
    }

    /// Whether this host can give a guest the CPU `cpu_xml` (a `<cpu>`
    /// element, or a domain holding one) describes: `virsh
    /// hypervisor-cpu-compare`. Err with libvirt's reason if not.
    pub async fn cpu_compare(&self, cpu_xml: &str) -> Result<String, VmError> {
        let out = self
            .with_file(&["hypervisor-cpu-compare", "--error"], cpu_xml)
            .await?;
        Ok(out.trim().to_string())
    }

    /// The CPU every one of `domcaps` (each a `virsh domcapabilities`)
    /// can give a guest, migratable: `virsh hypervisor-cpu-baseline`.
    pub async fn cpu_baseline(&self, domcaps: &[String]) -> Result<String, VmError> {
        let all = domcaps.concat();
        self.with_file(&["hypervisor-cpu-baseline", "--migratable"], &all)
            .await
    }

    pub async fn vnc_port(&self, name: &str) -> Option<u16> {
        let out = self.run(&["vncdisplay", name]).await.ok()?;
        let display: u16 = out.trim().rsplit(':').next()?.parse().ok()?;
        5900u16.checked_add(display)
    }

    /// The domain with its details, if raidish started it (any node, any
    /// pool: see `VmRuntime::our_vm`).
    pub async fn local_vm(&self, d: Domain) -> Option<LocalVm> {
        let meta = self.meta(&d.name).await?;
        let info = self.run(&["dominfo", &d.name]).await.unwrap_or_default();
        let field = |k: &str| {
            info.lines()
                .find_map(|l| l.strip_prefix(k).map(|v| v.trim().to_string()))
        };
        let vnc_port = if d.id.is_some() {
            self.vnc_port(&d.name).await
        } else {
            None
        };
        Some(LocalVm {
            vcpus: field("CPU(s):").and_then(|v| v.parse().ok()),
            // `2097152 KiB`
            memory: field("Used memory:")
                .or_else(|| field("Max memory:"))
                .and_then(|v| {
                    let mut w = v.split_whitespace();
                    crate::vm_def::libvirt_memory(w.next()?, w.next())
                }),
            cpu_s: field("CPU time:").and_then(|v| v.trim_end_matches('s').parse().ok()),
            vnc_port,
            domain: d,
            meta,
        })
    }

    /// Every domain raidish started on this libvirt (any node's, any
    /// pool's: see `VmRuntime::local_vms`).
    pub async fn local_vms(&self) -> Result<Vec<LocalVm>, VmError> {
        let mut out = Vec::new();
        for d in self.list().await? {
            if let Some(v) = self.local_vm(d).await {
                out.push(v);
            }
        }
        Ok(out)
    }
}

/// `virsh -q list --all`: ` 3    name   running` / ` -    name   shut off`.
fn parse_list(out: &str) -> Vec<Domain> {
    out.lines()
        .filter_map(|l| {
            let mut parts = l.split_whitespace();
            let id = parts.next()?;
            let name = parts.next()?.to_string();
            let state = DomState::parse(&parts.collect::<Vec<_>>().join(" "));
            Some(Domain {
                name,
                id: id.parse().ok(),
                state,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "tests/vm_tests.rs"]
mod tests;
