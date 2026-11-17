//! Virtual machines whose disks are raidish volumes, run through libvirt.
//!
//! A VM is a libvirt domain XML at `/vms/<name>.xml` in the pool (edit it
//! over NFS or the file manager; `raidish vm import` writes one from `virsh
//! dumpxml`). A disk is a raidish volume when its `<source>` says so:
//!
//! ```xml
//! <source file='raidish:vm1'/>                 <!-- or dev=, or -->
//! <source protocol='nbd' name='vm1'/>
//! ```
//!
//! Nothing about a VM lives in `/etc/libvirt` on any host. The node that
//! runs a VM's disks renders the XML (disks pointed at its own NBD export,
//! a VNC console on 127.0.0.1, raidish metadata) and starts it as a
//! *transient* domain, so libvirt forgets it once it stops.
//!
//! Where a VM runs follows its **anchor**, the first raidish disk: that
//! volume's placement settings (`VolumeProps`: nodes, `move_to`, enabled)
//! place the VM, and its other disks go wherever the anchor goes. Whether
//! it *should* run is one replicated bit (`ClusterState::vms`). Whether it
//! *does* is never stored: it is what libvirt on the anchor's node says.

use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use crate::vm_def::{VmDisk, VmHardware, VmNic};

/// Where VM definitions live in the pool.
pub const VM_DIR: &str = "vms";

/// The XML namespace of the metadata raidish puts on the domains it runs.
pub const META_NS: &str = "https://raidish.dev/xmlns/vm/1";

/// The `vm` section of the config. Absent: this node runs no VMs.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct VmConfig {
    /// libvirt connection (default qemu:///system).
    pub uri: Option<String>,
    /// The virsh binary (default virsh).
    pub virsh: Option<String>,
    /// How long a guest gets to shut down before it's destroyed (default 60).
    pub shutdown_timeout_s: Option<u64>,
}

impl VmConfig {
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_s.unwrap_or(60))
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
    pub poke: tokio::sync::Notify,
    /// Where qemu finds this node's NBD exports.
    pub nbd: (String, u16),
    /// VMs to shut down (destroyed after the timeout) and start again from
    /// their current XML.
    pub restart: std::sync::Mutex<std::collections::BTreeSet<String>>,
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && !name.starts_with('.')
}

pub fn spec_path(name: &str) -> PathBuf {
    Path::new(VM_DIR).join(format!("{}.xml", name))
}

/// A VM as its XML defines it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VmSpec {
    pub name: String,
    /// The XML file's change counter: a running VM started from an older
    /// one has a restart pending.
    pub change: u64,
    /// The volume that places the VM (its first raidish disk).
    pub anchor: String,
    /// Every raidish volume it uses, the anchor first.
    pub volumes: Vec<String>,
    /// Why the XML is unusable, if it is (then anchor/volumes are empty).
    pub error: Option<String>,
    /// What the XML configures (as far as it could be read).
    #[serde(default)]
    pub hw: VmHardware,
    #[serde(skip)]
    pub xml: String,
}

/// An element's text.
fn text(el: &El) -> String {
    el.kids
        .iter()
        .filter_map(|k| match k {
            Node::Raw(Event::Text(t)) => t.unescape().ok().map(|t| t.into_owned()),
            _ => None,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// `<memory unit='...'>n</memory>` in KiB (libvirt's units; KiB by default).
fn memory_kib(el: &El) -> Option<u64> {
    let n: u64 = text(el).parse().ok()?;
    let bytes: u64 = match el.attr("unit").unwrap_or("KiB") {
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
    n.checked_mul(bytes).map(|b| b / 1024)
}

fn hardware(root: &El) -> VmHardware {
    let mut hw = VmHardware {
        vcpus: root.child("vcpu").and_then(|v| text(v).parse().ok()),
        memory_kib: root
            .child("currentMemory")
            .or_else(|| root.child("memory"))
            .and_then(memory_kib),
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
            anchor: String::new(),
            volumes: Vec::new(),
            error: None,
            hw: VmHardware::default(),
            xml: xml.to_string(),
        };
        let root = parse_domain(xml);
        if let Ok(root) = &root {
            spec.hw = hardware(root);
        }
        match root.and_then(|root| raidish_disks(&root)) {
            Ok(vols) if vols.is_empty() => {
                spec.error = Some("no raidish disk (<source file='raidish:VOLUME'/>)".into())
            }
            Ok(vols) => {
                spec.anchor = vols[0].clone();
                spec.volumes = vols;
            }
            Err(e) => spec.error = Some(e),
        }
        spec
    }

    pub fn usable(&self) -> bool {
        self.error.is_none()
    }
}

/// Read every `/vms/*.xml` as of the newest committed root, reusing
/// `cache` entries whose file hasn't changed. No `/vms`: no VMs.
pub async fn load_specs<D: BlockDevice>(
    fs: &FileSystem<D>,
    cache: &mut BTreeMap<String, VmSpec>,
) -> Result<(), FileSystemError> {
    fs.refresh_root().await?;
    let index = match fs.ls(Path::new(VM_DIR)).await {
        Ok(i) => i,
        Err(FileSystemError::FileNotFound | FileSystemError::DirectoryNotFound) => {
            cache.clear();
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let mut seen = BTreeMap::new();
    for (file, entry) in &index.files {
        let Some(name) = file.strip_suffix(".xml") else {
            continue;
        };
        if !valid_name(name) {
            continue;
        }
        let dnode = fs.read_dnode(entry.id).await?;
        if dnode.kind != crate::inode::ObjType::File {
            continue;
        }
        let spec = match cache.remove(name) {
            Some(s) if s.change == dnode.change => s,
            _ => {
                let bytes = fs.read_file(&spec_path(name)).await?;
                VmSpec::parse(name, &String::from_utf8_lossy(&bytes), dnode.change)
            }
        };
        seen.insert(name.to_string(), spec);
    }
    *cache = seen;
    Ok(())
}

// ---- a tiny XML tree, enough to rewrite a domain --------------------------

#[derive(Clone, Debug)]
enum Node {
    El(El),
    /// Text, comments, CDATA... written back as they were read.
    Raw(Event<'static>),
}

#[derive(Clone, Debug, Default)]
struct El {
    name: String,
    attrs: Vec<(String, String)>,
    kids: Vec<Node>,
}

impl El {
    fn new(name: &str, attrs: &[(&str, &str)]) -> El {
        El {
            name: name.into(),
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            kids: Vec::new(),
        }
    }
    fn attr(&self, k: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
    fn local(&self) -> &str {
        self.name.rsplit(':').next().unwrap_or(&self.name)
    }
    fn child(&self, name: &str) -> Option<&El> {
        self.elements().find(|e| e.name == name)
    }
    fn child_mut(&mut self, name: &str) -> Option<&mut El> {
        self.kids.iter_mut().find_map(|k| match k {
            Node::El(e) if e.name == name => Some(e),
            _ => None,
        })
    }
    fn elements(&self) -> impl Iterator<Item = &El> {
        self.kids.iter().filter_map(|k| match k {
            Node::El(e) => Some(e),
            _ => None,
        })
    }
    fn remove(&mut self, name: &str) {
        self.kids
            .retain(|k| !matches!(k, Node::El(e) if e.name == name));
    }
}

fn start_el(e: &BytesStart) -> Result<El, String> {
    let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
    let mut attrs = Vec::new();
    for a in e.attributes() {
        let a = a.map_err(|e| e.to_string())?;
        attrs.push((
            String::from_utf8_lossy(a.key.as_ref()).into_owned(),
            a.unescape_value().map_err(|e| e.to_string())?.into_owned(),
        ));
    }
    Ok(El {
        name,
        attrs,
        kids: Vec::new(),
    })
}

/// The root element of `xml`.
fn parse_root(xml: &str) -> Result<El, String> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut stack: Vec<El> = Vec::new();
    let mut root = None;
    loop {
        let ev = reader
            .read_event()
            .map_err(|e| format!("XML at byte {}: {}", reader.buffer_position(), e))?;
        match ev {
            Event::Start(s) => stack.push(start_el(&s)?),
            Event::Empty(s) => {
                let el = start_el(&s)?;
                match stack.last_mut() {
                    Some(p) => p.kids.push(Node::El(el)),
                    None => root = Some(el),
                }
            }
            Event::End(_) => {
                let el = stack.pop().ok_or("XML: unbalanced end tag")?;
                match stack.last_mut() {
                    Some(p) => p.kids.push(Node::El(el)),
                    None => root = Some(el),
                }
            }
            Event::Eof => break,
            Event::Decl(_) | Event::DocType(_) | Event::PI(_) if stack.is_empty() => {}
            ev => {
                if let Some(p) = stack.last_mut() {
                    p.kids.push(Node::Raw(ev.into_owned()));
                }
            }
        }
    }
    if !stack.is_empty() {
        return Err("XML: unclosed element".into());
    }
    root.ok_or_else(|| "XML: no root element".into())
}

fn parse_domain(xml: &str) -> Result<El, String> {
    let root = parse_root(xml)?;
    if root.name != "domain" {
        return Err(format!("expected a <domain>, found <{}>", root.name));
    }
    Ok(root)
}

fn write_el(w: &mut quick_xml::Writer<Vec<u8>>, el: &El) -> std::io::Result<()> {
    let mut start = BytesStart::new(el.name.as_str());
    for (k, v) in &el.attrs {
        start.push_attribute((k.as_str(), v.as_str()));
    }
    if el.kids.is_empty() {
        return w.write_event(Event::Empty(start));
    }
    w.write_event(Event::Start(start))?;
    for k in &el.kids {
        match k {
            Node::El(e) => write_el(w, e)?,
            Node::Raw(ev) => w.write_event(ev.clone())?,
        }
    }
    w.write_event(Event::End(BytesEnd::new(el.name.as_str())))
}

fn to_string(el: &El) -> String {
    let mut w = quick_xml::Writer::new(Vec::new());
    write_el(&mut w, el).expect("writing to memory");
    String::from_utf8(w.into_inner()).expect("utf-8 in, utf-8 out")
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

/// Every raidish volume in the domain's disks, the anchor (first non-cdrom
/// one) first.
fn raidish_disks(root: &El) -> Result<Vec<String>, String> {
    let Some(devices) = root.child("devices") else {
        return Ok(Vec::new());
    };
    let mut disks: Vec<(bool, String)> = Vec::new();
    for d in devices.elements().filter(|e| e.name == "disk") {
        if let Some(v) = disk_volume(d) {
            if v.is_empty() || v.contains('/') {
                return Err(format!("bad volume name '{}'", v));
            }
            if disks.iter().any(|(_, x)| *x == v) {
                return Err(format!("volume '{}' is used twice", v));
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
/// VNC console on 127.0.0.1, and raidish's metadata in place of any other.
/// `owner` is the anchor's `vol/` lease owner: a domain whose owner isn't
/// the lease's current one is an orphan of an earlier raidish (its disks'
/// NBD connections go nowhere).
pub fn render(spec: &VmSpec, nbd: (&str, u16), owner: &str) -> Result<String, String> {
    let mut root = parse_domain(&spec.xml)?;
    let port = nbd.1.to_string();

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
    meta.kids.push(Node::El(El::new(
        "raidish:vm",
        &[
            ("xmlns:raidish", META_NS),
            ("anchor", &spec.anchor),
            ("volumes", &spec.volumes.join(",")),
            ("change", &spec.change.to_string()),
            ("owner", owner),
        ],
    )));
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
                Node::El(e) if matches!(e.name.as_str(), "driver" | "source" | "auth") => {}
                Node::El(e) => new.kids.push(Node::El(e.clone())),
                Node::Raw(_) => {}
            }
        }
        *disk = new;
    }
    // Console.
    devices.remove("graphics");
    let mut g = El::new(
        "graphics",
        &[
            ("type", "vnc"),
            ("port", "-1"),
            ("autoport", "yes"),
            ("listen", "127.0.0.1"),
        ],
    );
    g.kids.push(Node::El(El::new(
        "listen",
        &[("type", "address"), ("address", "127.0.0.1")],
    )));
    devices.kids.push(Node::El(g));
    Ok(to_string(&root))
}

/// What raidish's metadata on a running domain says.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DomMeta {
    pub anchor: String,
    pub volumes: Vec<String>,
    pub change: u64,
    /// The anchor's lease owner when it was started (see `render`).
    #[serde(default)]
    pub owner: String,
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
        owner: el.attr("owner").unwrap_or_default().to_string(),
    })
}

// ---- virsh ----------------------------------------------------------------

/// A domain libvirt has on this node.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Domain {
    pub name: String,
    /// libvirt's id while it runs.
    pub id: Option<u32>,
    /// `running`, `paused`, `in shutdown`, `shut off`, ...
    pub state: String,
}

/// A domain this node runs for raidish, as `/api/v1/vm/local` reports it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LocalVm {
    #[serde(flatten)]
    pub domain: Domain,
    pub meta: DomMeta,
    pub vcpus: Option<u32>,
    pub memory_kib: Option<u64>,
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
    async fn run(&self, args: &[&str]) -> Result<String, String> {
        let out = tokio::process::Command::new(&self.bin)
            .arg("-q")
            .arg("-c")
            .arg(&self.uri)
            .args(args)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| format!("{}: {}", self.bin, e))?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            let err = err.trim().trim_start_matches("error: ");
            return Err(format!(
                "virsh {}: {}",
                args.join(" "),
                err.replace('\n', "; ")
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Every domain libvirt knows of on this node.
    pub async fn list(&self) -> Result<Vec<Domain>, String> {
        Ok(parse_list(&self.run(&["list", "--all"]).await?))
    }

    /// raidish's metadata on `name`; None: not one of ours.
    pub async fn meta(&self, name: &str) -> Option<DomMeta> {
        let xml = self.run(&["metadata", name, "--uri", META_NS]).await.ok()?;
        parse_meta(&xml)
    }

    /// Start `xml` as a transient domain.
    pub async fn create(&self, name: &str, xml: &str) -> Result<(), String> {
        let path =
            std::env::temp_dir().join(format!("raidish-vm-{}-{}.xml", name, std::process::id()));
        tokio::fs::write(&path, xml)
            .await
            .map_err(|e| format!("{}: {}", path.display(), e))?;
        let r = self.run(&["create", &path.to_string_lossy()]).await;
        let _ = tokio::fs::remove_file(&path).await;
        r.map(|_| ())
    }

    pub async fn shutdown(&self, name: &str) -> Result<(), String> {
        self.run(&["shutdown", name]).await.map(|_| ())
    }
    pub async fn destroy(&self, name: &str) -> Result<(), String> {
        self.run(&["destroy", name]).await.map(|_| ())
    }
    pub async fn reboot(&self, name: &str) -> Result<(), String> {
        self.run(&["reboot", name]).await.map(|_| ())
    }
    pub async fn reset(&self, name: &str) -> Result<(), String> {
        self.run(&["reset", name]).await.map(|_| ())
    }

    pub async fn vnc_port(&self, name: &str) -> Option<u16> {
        let out = self.run(&["vncdisplay", name]).await.ok()?;
        let display: u16 = out.trim().rsplit(':').next()?.parse().ok()?;
        5900u16.checked_add(display)
    }

    /// The domain with its details, if it is one of ours.
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
            memory_kib: field("Used memory:")
                .or_else(|| field("Max memory:"))
                .and_then(|v| v.split_whitespace().next()?.parse().ok()),
            cpu_s: field("CPU time:").and_then(|v| v.trim_end_matches('s').parse().ok()),
            vnc_port,
            domain: d,
            meta,
        })
    }

    /// Every domain of ours on this node.
    pub async fn local_vms(&self) -> Result<Vec<LocalVm>, String> {
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
            let state = parts.collect::<Vec<_>>().join(" ");
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
