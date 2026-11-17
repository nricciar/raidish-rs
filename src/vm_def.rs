//! The parts of a VM definition (see `crate::vm`) the browser needs too:
//! what a domain XML configures, and the XML a new VM starts from. No XML
//! parser here (this builds for wasm); `crate::vm` does the parsing.

use serde::{Deserialize, Serialize};

/// The hardware a domain XML configures, for showing it (libvirt decides
/// what actually runs).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct VmHardware {
    pub vcpus: Option<u32>,
    pub memory_kib: Option<u64>,
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

/// What the "new VM" form asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewVm {
    /// A valid VM name (`crate::vm::valid_name`: letters, digits, `-_.`).
    pub name: String,
    pub vcpus: u32,
    pub memory_mib: u64,
    /// The volume for its disk (`<name>-disk0`, see `disk_volume`).
    pub disk: String,
    /// A volume holding an install ISO, attached as a cdrom.
    pub iso: Option<String>,
    /// A libvirt network for one virtio NIC; None: no network.
    pub network: Option<String>,
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
        mem = vm.memory_mib,
        cpus = vm.vcpus,
    )
}
