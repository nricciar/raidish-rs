//! The parts of a VM definition (see `crate::vm`) the browser needs too:
//! what a domain XML configures, and the XML a new VM starts from. No XML
//! parser here (this builds for wasm); `crate::vm` does the parsing.

use crate::types::Bytes;
use serde::{Deserialize, Serialize};

/// The hardware a domain XML configures, for showing it (libvirt decides
/// what actually runs).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct VmHardware {
    pub vcpus: Option<u32>,
    pub memory: Option<Bytes>,
    pub arch: Option<String>,
    pub machine: Option<String>,
    /// `<os><boot dev=.../>`, in order.
    #[serde(default)]
    pub boot: Vec<String>,
    #[serde(default)]
    pub disks: Vec<VmDisk>,
    #[serde(default)]
    pub nics: Vec<VmNic>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct VmDisk {
    /// `disk`, `cdrom`, ...
    pub device: String,
    /// `vda`, `sda`, ...
    pub target: Option<String>,
    pub bus: Option<String>,
    /// The raidish volume, if it is one.
    pub volume: Option<String>,
    /// Otherwise, where it comes from (a path, a host...).
    pub source: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct VmNic {
    /// `network`, `bridge`, `user`, ...
    pub kind: String,
    /// The network or bridge.
    pub source: Option<String>,
    pub mac: Option<String>,
    pub model: Option<String>,
}

/// A libvirt domain's state, as `virsh list` shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomState {
    Running,
    Idle,
    Paused,
    InShutdown,
    ShutOff,
    Crashed,
    Suspended,
    /// Anything else libvirt says.
    Other(String),
}

impl DomState {
    pub fn parse(s: &str) -> Self {
        match s {
            "running" => DomState::Running,
            "idle" => DomState::Idle,
            "paused" => DomState::Paused,
            "in shutdown" => DomState::InShutdown,
            "shut off" => DomState::ShutOff,
            "crashed" => DomState::Crashed,
            "pmsuspended" => DomState::Suspended,
            s => DomState::Other(s.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            DomState::Running => "running",
            DomState::Idle => "idle",
            DomState::Paused => "paused",
            DomState::InShutdown => "in shutdown",
            DomState::ShutOff => "shut off",
            DomState::Crashed => "crashed",
            DomState::Suspended => "pmsuspended",
            DomState::Other(s) => s,
        }
    }
}

impl std::fmt::Display for DomState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DomState {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DomState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(DomState::parse(&String::deserialize(d)?))
    }
}

/// The raidish volumes a VM's XML uses, or why it can't be run.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VmDisks {
    /// `anchor` (its first raidish disk) places it; `volumes` are all of
    /// them, the anchor first.
    Usable {
        anchor: String,
        volumes: Vec<String>,
    },
    Unusable {
        error: String,
    },
}

impl VmDisks {
    /// From the raidish disks in XML order: unusable without one.
    pub fn of(volumes: Vec<String>) -> Self {
        match volumes.first() {
            Some(a) => VmDisks::Usable {
                anchor: a.clone(),
                volumes,
            },
            None => VmDisks::Unusable {
                error: "no raidish disk (<source file='raidish:VOLUME'/>)".into(),
            },
        }
    }

    pub fn anchor(&self) -> Option<&str> {
        match self {
            VmDisks::Usable { anchor, .. } => Some(anchor),
            VmDisks::Unusable { .. } => None,
        }
    }

    /// Every raidish volume (none if unusable).
    pub fn volumes(&self) -> &[String] {
        match self {
            VmDisks::Usable { volumes, .. } => volumes,
            VmDisks::Unusable { .. } => &[],
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            VmDisks::Usable { .. } => None,
            VmDisks::Unusable { error } => Some(error),
        }
    }
}

/// What the "new VM" form asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewVm {
    /// A valid VM name (`crate::types::VmName`).
    pub name: String,
    pub vcpus: u32,
    pub memory: Bytes,
    /// The volume for its disk (`<name>-disk0`, see `disk_volume`).
    pub disk: String,
    /// A volume holding an install ISO, attached as a cdrom.
    pub iso: Option<String>,
    /// A libvirt network for one virtio NIC; None: no network.
    pub network: Option<String>,
}

/// A libvirt memory amount: `n` in `unit` (KiB if none, as libvirt has
/// it). None: not a number, or a unit libvirt doesn't have.
pub fn libvirt_memory(n: &str, unit: Option<&str>) -> Option<Bytes> {
    let n: u64 = n.trim().parse().ok()?;
    let scale: u64 = match unit.unwrap_or("KiB") {
        "b" | "bytes" => 1,
        "KB" => 1000,
        "k" | "KiB" => 1 << 10,
        "MB" => 1_000_000,
        "M" | "MiB" => 1 << 20,
        "GB" => 1_000_000_000,
        "G" | "GiB" => 1 << 30,
        "TB" => 1_000_000_000_000,
        "T" | "TiB" => 1 << 40,
        _ => return None,
    };
    n.checked_mul(scale).map(Bytes::new)
}

/// The volume a new VM `name`'s disk gets.
pub fn disk_volume(name: &str) -> String {
    format!("{}-disk0", name)
}

/// The domain XML for a new VM: a q35 KVM guest with one virtio disk (and
/// the ISO as a cdrom), booting from the disk, else the cdrom. No graphics:
/// raidish adds the VNC console when it starts it. Names are limited to
/// `[A-Za-z0-9._-]` (volume and network names too, by the form), so nothing
/// needs escaping.
pub fn template(vm: &NewVm) -> String {
    let mut devices = format!(
        "    <disk type='file' device='disk'>\n      \
         <source file='raidish:{}'/>\n      \
         <target dev='vda' bus='virtio'/>\n    \
         </disk>\n",
        vm.disk
    );
    if let Some(iso) = &vm.iso {
        devices.push_str(&format!(
            "    <disk type='file' device='cdrom'>\n      \
             <source file='raidish:{}'/>\n      \
             <target dev='sda' bus='sata'/>\n      \
             <readonly/>\n    \
             </disk>\n",
            iso
        ));
    }
    if let Some(net) = &vm.network {
        devices.push_str(&format!(
            "    <interface type='network'>\n      \
             <source network='{}'/>\n      \
             <model type='virtio'/>\n    \
             </interface>\n",
            net
        ));
    }
    format!(
        "<domain type='kvm'>\n  \
         <name>{name}</name>\n  \
         <memory unit='MiB'>{mem}</memory>\n  \
         <vcpu>{cpus}</vcpu>\n  \
         <os>\n    \
         <type arch='x86_64' machine='q35'>hvm</type>\n    \
         <boot dev='hd'/>\n    \
         <boot dev='cdrom'/>\n  \
         </os>\n  \
         <features><acpi/><apic/></features>\n  \
         <cpu mode='host-passthrough'/>\n  \
         <clock offset='utc'/>\n  \
         <on_poweroff>destroy</on_poweroff>\n  \
         <on_reboot>restart</on_reboot>\n  \
         <on_crash>destroy</on_crash>\n  \
         <devices>\n\
         {devices}    \
         <video><model type='virtio'/></video>\n    \
         <input type='tablet' bus='usb'/>\n    \
         <serial type='pty'/>\n    \
         <console type='pty'/>\n    \
         <memballoon model='virtio'/>\n  \
         </devices>\n\
         </domain>\n",
        name = vm.name,
        mem = vm.memory.mib(),
        cpus = vm.vcpus,
    )
}

/// A VM as its XML defines it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VmSpec {
    pub name: String,
    /// The definition's change number (see `crate::vm_store`): a running
    /// VM started from an older one has a restart pending.
    pub change: u64,
    /// The raidish volumes it uses, or why the XML is unusable.
    #[serde(flatten)]
    pub disks: VmDisks,
    /// What the XML configures (as far as it could be read).
    #[serde(default)]
    pub hw: VmHardware,
    #[serde(skip)]
    pub xml: String,
}
