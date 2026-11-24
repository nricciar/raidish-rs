//! Can a VM move to a node, and how (MIG-4): live, cold (shut down here,
//! started there), or not at all. From the VM's definition and the two
//! nodes' facts (`crate::node_facts`), plus what the target says live
//! about the VM's CPU and its free memory (`Live`).

use crate::node_facts::NodeFacts;
use crate::types::Bytes;
use crate::vm::{El, parse_root};
use serde::{Deserialize, Serialize};

pub use crate::api_types::{Mode, Reason, ReasonLevel, Verdict};

/// A fresh (live) verdict for `node`.
fn new_verdict(node: &str) -> Verdict {
    Verdict {
        node: node.to_string(),
        mode: Mode::Live,
        reasons: Vec::new(),
    }
}

/// Recording reasons on a `Verdict` (plain data, `crate::api_types`).
trait Judge {
    fn no(&mut self, t: impl Into<String>);
    fn cold(&mut self, t: impl Into<String>);
    fn warn(&mut self, t: impl Into<String>);
}

impl Judge for Verdict {
    fn no(&mut self, t: impl Into<String>) {
        self.mode = Mode::No;
        self.reasons.push(Reason {
            level: ReasonLevel::No,
            text: t.into(),
        });
    }
    fn cold(&mut self, t: impl Into<String>) {
        self.mode = self.mode.min(Mode::Cold);
        self.reasons.push(Reason {
            level: ReasonLevel::Cold,
            text: t.into(),
        });
    }
    fn warn(&mut self, t: impl Into<String>) {
        self.reasons.push(Reason {
            level: ReasonLevel::Warn,
            text: t.into(),
        });
    }
}

/// What a VM's domain XML asks of a host.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Needs {
    /// `kvm` or `qemu`.
    pub virt_type: String,
    pub arch: Option<String>,
    pub machine: Option<String>,
    pub memory: Bytes,
    /// `host-passthrough`, `host-model`, `custom`, `maximum`; None: no
    /// `<cpu>` (qemu's default model, fine anywhere).
    pub cpu_mode: Option<String>,
    pub cpu_model: Option<String>,
    pub cpu_features: Vec<String>,
    /// The `<cpu>` element as written (for `cpu-compare`).
    pub cpu_xml: Option<String>,
    pub networks: Vec<String>,
    pub bridges: Vec<String>,
}

/// Read what `xml` (a domain) needs.
pub fn needs(xml: &str) -> Result<Needs, crate::vm::VmError> {
    let root = parse_root(xml)?;
    if root.name != "domain" {
        return Err(crate::vm::VmError::Invalid(format!(
            "expected a <domain>, got <{}>",
            root.name
        )));
    }
    let mut n = Needs {
        virt_type: root.attr("type").unwrap_or("kvm").to_string(),
        ..Default::default()
    };
    if let Some(t) = root.child("os").and_then(|o| o.child("type")) {
        n.arch = t.attr("arch").map(str::to_string);
        n.machine = t.attr("machine").map(str::to_string);
    }
    if let Some(m) = root.child("memory") {
        n.memory = crate::vm_def::libvirt_memory(&m.text(), m.attr("unit")).unwrap_or_default();
    }
    if let Some(c) = root.child("cpu") {
        n.cpu_mode = Some(c.attr("mode").unwrap_or("custom").to_string());
        n.cpu_model = c.child("model").map(El::text).filter(|m| !m.is_empty());
        n.cpu_features = c
            .elements()
            .filter(|e| e.name == "feature")
            .filter(|e| matches!(e.attr("policy"), None | Some("require") | Some("force")))
            .filter_map(|e| e.attr("name").map(str::to_string))
            .collect();
        n.cpu_xml = Some(crate::vm::el_xml(c));
    }
    if let Some(d) = root.child("devices") {
        for i in d.elements().filter(|e| e.name == "interface") {
            let src = i.child("source");
            match (i.attr("type"), src) {
                (Some("network"), Some(s)) => {
                    n.networks.extend(s.attr("network").map(str::to_string))
                }
                (Some("bridge"), Some(s)) => n.bridges.extend(s.attr("bridge").map(str::to_string)),
                _ => {}
            }
        }
    }
    Ok(n)
}

/// The CPU a guest gets with `host-model` on `facts`' host, as a `<cpu>`
/// another host can be asked about (`virsh hypervisor-cpu-compare`).
pub fn host_model_xml(facts: &NodeFacts) -> Option<String> {
    let cpu = &facts.libvirt.as_ref()?.cpu;
    if cpu.model.is_empty() {
        return None;
    }
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('\'', "&apos;")
    };
    let mut x = format!(
        "<cpu mode='custom' match='exact'><model fallback='forbid'>{}</model>",
        esc(&cpu.model)
    );
    if !cpu.vendor.is_empty() {
        x.push_str(&format!("<vendor>{}</vendor>", esc(&cpu.vendor)));
    }
    for f in &cpu.features {
        x.push_str(&format!("<feature policy='require' name='{}'/>", esc(f)));
    }
    x.push_str("</cpu>");
    Some(x)
}

/// The `<cpu>` to ask the target about for a VM running (or to run) on
/// `source`: its own if it names a model, else what host-model gives it
/// on the source. None: nothing to ask (no `<cpu>`, or passthrough).
pub fn cpu_to_compare(n: &Needs, source: Option<&NodeFacts>) -> Option<String> {
    match n.cpu_mode.as_deref() {
        None | Some("host-passthrough") | Some("maximum") => None,
        Some("host-model") => source.and_then(host_model_xml),
        Some(_) => n.cpu_xml.clone(),
    }
}

/// What a node is asked (`POST /api/v1/vm/check`): the VM's CPU (see
/// `cpu_to_compare`).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct LiveAsk {
    pub cpu: Option<String>,
}

/// What the target says now (`POST /api/v1/vm/check` on it).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct Live {
    /// `cpu-compare` of the VM's CPU there: Err with libvirt's reason.
    pub cpu: Option<Result<String, String>>,
    /// Its free memory, MiB.
    pub available: Option<Bytes>,
}

/// The verdict for moving a VM that needs `n` from `source` (None: it
/// isn't running anywhere, so a move is a cold start) to `target`.
/// `target` None: it hasn't reported its facts.
pub fn check(
    n: &Needs,
    to: &str,
    source: Option<&NodeFacts>,
    target: Option<&NodeFacts>,
    live: Option<&Live>,
) -> Verdict {
    let mut v = new_verdict(to);
    let Some(t) = target else {
        v.cold(format!(
            "{} hasn't reported its facts (its raidish is older, or down)",
            to
        ));
        return v;
    };
    let Some(lv) = &t.libvirt else {
        v.no(format!("{} runs no VMs (no `vm:` in its config)", to));
        return v;
    };
    if let Some(e) = &lv.error {
        v.no(format!("{}'s libvirt: {}", to, e));
        return v;
    }
    // Where it can run at all.
    if let Some(a) = &n.arch {
        if !t.host.arch.is_empty() && !arch_matches(a, &t.host.arch) {
            v.no(format!("it's {}, {} is {}", a, to, t.host.arch));
        }
    }
    if n.virt_type == "kvm" && (!t.host.kvm || (!lv.virt_type.is_empty() && lv.virt_type != "kvm"))
    {
        v.no(format!("{} has no KVM (/dev/kvm)", to));
    }
    if let Some(m) = &n.machine {
        if !lv.machines.is_empty() && !lv.machines.contains(m) {
            v.no(format!("{}'s qemu has no machine type '{}'", to, m));
        }
    }
    for net in &n.networks {
        if !lv.networks.contains(net) {
            v.no(format!("{} has no (active) libvirt network '{}'", to, net));
        }
    }
    for br in &n.bridges {
        if !lv.bridges.contains(br) {
            v.no(format!("{} has no bridge '{}'", to, br));
        }
    }
    match live.and_then(|l| l.available) {
        Some(free) if n.memory > free => v.warn(format!(
            "{} has {} MiB available, the VM wants {} MiB",
            to,
            free.mib(),
            n.memory.mib()
        )),
        None if n.memory > t.host.memory && t.host.memory > Bytes::default() => v.warn(format!(
            "{} has {} MiB of memory, the VM wants {} MiB",
            to,
            t.host.memory.mib(),
            n.memory.mib()
        )),
        _ => {}
    }

    // Live, from where it runs.
    let Some(s) = source else {
        return v;
    };
    if s.node == to {
        return v;
    }
    for (who, f) in [(&s.node, s), (&t.node, t)] {
        if !f.peer_io {
            v.cold(format!(
                "peer I/O is off on {} (no `tls:` in its config)",
                who
            ));
        }
    }
    match s.peers.get(to) {
        Some(p) => {
            match &p.libvirt {
                Some(pr) if !pr.ok => v.cold(format!(
                    "{} → {} libvirt TLS: {}",
                    s.node,
                    to,
                    pr.describe()
                )),
                Some(_) => {}
                None => v.cold(format!("{} has no libvirt to migrate from", s.node)),
            }
            if let Some(pr) = &p.peer_io {
                if !pr.ok {
                    v.cold(format!("{} → {}: {}", s.node, to, pr.describe()));
                }
            }
            if let Some(e) = &p.migrate_tls_error {
                v.cold(format!(
                    "the last live migration {} → {} failed on qemu's TLS ({}): {}'s /etc/pki/qemu needs its `raidish cert libvirt` certificates, keys readable by qemu's group (or migrate_tls_x509_cert_dir in its qemu.conf naming where they are)",
                    s.node, to, e, to
                ));
            }
        }
        None => v.cold(format!("{} hasn't checked {} yet", s.node, to)),
    }
    if let (Some(sv), Some(tv)) = (s.qemu_version(), t.qemu_version()) {
        if tv < sv {
            v.cold(format!(
                "{}'s qemu ({}) is older than {}'s ({})",
                to,
                t.libvirt
                    .as_ref()
                    .map(|l| l.qemu_version.as_str())
                    .unwrap_or(""),
                s.node,
                s.libvirt
                    .as_ref()
                    .map(|l| l.qemu_version.as_str())
                    .unwrap_or("")
            ));
        }
    }
    if n.cpu_features.iter().any(|f| f == "invtsc") {
        v.cold("its CPU has invtsc, which libvirt won't migrate");
    }
    match n.cpu_mode.as_deref() {
        Some("maximum") => v.cold("its CPU mode 'maximum' isn't migratable"),
        Some("host-passthrough") if s.host.cpu_model != t.host.cpu_model => v.cold(format!(
            "its CPU is host-passthrough, and {} ({}) and {} ({}) differ: give it a CPU model both have (raidish vm cpu-baseline)",
            s.node, s.host.cpu_model, to, t.host.cpu_model
        )),
        _ => {}
    }
    match live.and_then(|l| l.cpu.as_ref()) {
        Some(Err(e)) => v.cold(format!(
            "{} can't give it the CPU it has on {}: {} (raidish vm cpu-baseline)",
            to, s.node, e
        )),
        Some(Ok(_)) => {}
        None => {
            // No live answer: a guess from the stored facts.
            let model = match n.cpu_mode.as_deref() {
                Some("host-model") => s.libvirt.as_ref().map(|l| l.cpu.model.clone()),
                Some("custom") => n.cpu_model.clone(),
                _ => None,
            };
            if let Some(m) = model.filter(|m| !m.is_empty()) {
                if !lv.cpu_models.is_empty() && !lv.cpu_models.contains(&m) {
                    v.cold(format!("{} can't give a guest CPU model {}", to, m));
                }
            }
        }
    }
    v
}

fn arch_matches(domain: &str, host: &str) -> bool {
    domain == host || (domain == "aarch64" && host == "arm64")
}

#[cfg(test)]
#[path = "tests/vm_check_tests.rs"]
mod tests;
