//! `raidish vm ...`: VMs through any `listen` node's API (see `crate::vms_api`).

use crate::vm::{VmSpec, valid_name};
use crate::vms_api::VmView;
use clap::Subcommand;
use std::time::Duration;

#[derive(Subcommand)]
pub enum VmCommands {
    /// List VMs: where they run, and what libvirt says they're doing.
    List,
    /// Store a libvirt domain XML (`virsh dumpxml` output) as VM `name`.
    /// Its disks that are raidish volumes say so: `<source
    /// file='raidish:VOLUME'/>`. It's checked as it's stored: every disk
    /// a raidish volume no other VM uses, nothing that reaches the host.
    Import {
        name: String,
        xml: String,
        /// Replace VM `name`'s definition (default: refuse if there is one).
        #[arg(long)]
        replace: bool,
    },
    /// Print VM `name`'s definition (the XML).
    Show { name: String },
    /// Delete VM `name`'s definition (and stop it). Its volumes stay.
    Delete { name: String },
    /// It should run: the node with its disks starts it.
    Start { name: String },
    /// It should not run: shut it down (destroyed after the timeout).
    Stop { name: String },
    /// Stop it at once (no guest shutdown).
    Kill { name: String },
    /// Shut it down and start it again from its current XML.
    Restart { name: String },
    /// Reboot the guest (same domain, same XML).
    Reboot { name: String },
    /// Hard reset.
    Reset { name: String },
    /// Move it (its disks) to `node`: migrated live if both nodes can,
    /// else shut down here and started there (`vm check` says which).
    Move {
        name: String,
        node: String,
        /// Refuse unless it can move live.
        #[arg(long)]
        live: bool,
    },
    /// Where it can move, and how: live, cold (shut down, started there),
    /// or not at all, and why (every other node, or just `node`).
    Check { name: String, node: Option<String> },
    /// A CPU every node (or each of `--nodes`) can give a guest, to paste
    /// into a domain's XML so it can move live between them.
    CpuBaseline {
        /// Comma-separated node names (default: all that run VMs).
        #[arg(long)]
        nodes: Option<String>,
    },
}

/// `raidish node ...`.
#[derive(Subcommand)]
pub enum NodeCommands {
    /// What each node is (host, CPU, libvirt) and whether the others reach
    /// its libvirt over TLS: what live migration needs, and what's wrong.
    Facts { node: Option<String> },
}

struct Api {
    endpoints: Vec<String>,
    http: reqwest::Client,
}

impl Api {
    /// Send `req` (built against a base URL) to the first node that answers.
    async fn send(
        &self,
        req: impl Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, String> {
        let mut errors = Vec::new();
        for ep in &self.endpoints {
            match req(&self.http, ep.trim_end_matches('/')).send().await {
                Ok(r) if r.status().is_success() => return Ok(r),
                Ok(r) if r.status().is_client_error() => {
                    let s = r.status();
                    return Err(format!("{}: {}", s, r.text().await.unwrap_or_default()));
                }
                Ok(r) => {
                    let s = r.status();
                    errors.push(format!(
                        "{}: {} {}",
                        ep,
                        s,
                        r.text().await.unwrap_or_default()
                    ))
                }
                Err(e) => errors.push(format!("{}: {}", ep, e)),
            }
        }
        Err(if errors.is_empty() {
            "no cluster endpoints".into()
        } else {
            errors.join("; ")
        })
    }
}

pub async fn run(command: &VmCommands, endpoints: Vec<String>) -> Result<(), String> {
    let api = Api {
        endpoints,
        http: crate::tls::http_builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| e.to_string())?,
    };
    match command {
        VmCommands::List => {
            let vms: Vec<VmView> = api
                .send(|c, b| c.get(format!("{}/api/v1/vms", b)))
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            print_vms(&vms);
        }
        VmCommands::Show { name } => {
            let def: crate::vm_store::VmDef = api
                .send(|c, b| c.get(format!("{}/api/v1/vms/{}/definition", b, name)))
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            print!("{}", def.xml);
            if !def.xml.ends_with('\n') {
                println!();
            }
        }
        VmCommands::Delete { name } => {
            api.send(|c, b| c.delete(format!("{}/api/v1/vms/{}", b, name)))
                .await?;
            println!("VM '{}' deleted (its volumes stay)", name);
        }
        VmCommands::Import { name, xml, replace } => {
            if !valid_name(name) {
                return Err(format!(
                    "bad VM name '{}' (letters, digits, '-', '_', '.')",
                    name
                ));
            }
            let body = std::fs::read_to_string(xml).map_err(|e| format!("{}: {}", xml, e))?;
            let spec = VmSpec::parse(name, &body, 0);
            if let Some(e) = &spec.error {
                return Err(e.clone());
            }
            let def: crate::vm_store::VmDef = api
                .send(|c, b| {
                    c.put(format!("{}/api/v1/vms/{}?create={}", b, name, !replace))
                        .body(body.clone())
                })
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            println!(
                "VM '{}' defined (change {}): anchor volume '{}', volumes {}",
                name,
                def.change,
                spec.anchor,
                spec.volumes.join(", ")
            );
            println!(
                "if libvirt still has a domain '{}' defined on some host, undefine it there \
                 (raidish runs VMs as transient domains)",
                name
            );
        }
        VmCommands::Move { name, node, live } => {
            let view: crate::vms_api::CheckView = api
                .send(|c, b| {
                    c.post(format!("{}/api/v1/vms/{}/move", b, name))
                        .json(&serde_json::json!({ "node": node, "live": live }))
                })
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            match (view.verdicts.first(), &view.running_on) {
                (_, None) => println!("VM '{}' isn't running: it starts on {} next", name, node),
                (Some(v), Some(from)) if v.mode == crate::vm_check::Mode::Live => {
                    println!("moving VM '{}' from {} to {} live", name, from, node)
                }
                (Some(v), Some(from)) => println!(
                    "moving VM '{}' from {} to {} cold (shut down, started there): {}",
                    name,
                    from,
                    node,
                    v.why()
                ),
                (None, Some(from)) => println!("VM '{}' already runs on {}", name, from),
            }
            if let Some(v) = view.verdicts.first() {
                for r in v.reasons.iter().filter(|r| r.level == "warn") {
                    println!("  warning: {}", r.text);
                }
            }
        }
        VmCommands::Check { name, node } => {
            let q = node
                .as_ref()
                .map(|n| format!("?node={}", n))
                .unwrap_or_default();
            let view: crate::vms_api::CheckView = api
                .send(|c, b| c.get(format!("{}/api/v1/vms/{}/check{}", b, name, q)))
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            print_check(&view);
        }
        VmCommands::CpuBaseline { nodes } => {
            let q = nodes
                .as_ref()
                .map(|n| format!("?nodes={}", n))
                .unwrap_or_default();
            let xml = api
                .send(|c, b| c.get(format!("{}/api/v1/vm/cpu-baseline{}", b, q)))
                .await?
                .text()
                .await
                .map_err(|e| e.to_string())?;
            println!("{}", xml);
        }
        VmCommands::Start { name }
        | VmCommands::Stop { name }
        | VmCommands::Kill { name }
        | VmCommands::Restart { name }
        | VmCommands::Reboot { name }
        | VmCommands::Reset { name } => {
            let action = match command {
                VmCommands::Start { .. } => "start",
                VmCommands::Stop { .. } => "stop",
                VmCommands::Kill { .. } => "kill",
                VmCommands::Restart { .. } => "restart",
                VmCommands::Reboot { .. } => "reboot",
                _ => "reset",
            };
            api.send(|c, b| c.post(format!("{}/api/v1/vms/{}/{}", b, name, action)))
                .await?;
        }
    }
    Ok(())
}

fn print_check(view: &crate::vms_api::CheckView) {
    match &view.running_on {
        Some(n) => println!("VM '{}' runs on {}; moving it to:", view.vm, n),
        None => println!("VM '{}' isn't running; starting it on:", view.vm),
    }
    for v in &view.verdicts {
        let mode = match (v.mode, view.running_on.is_some()) {
            (crate::vm_check::Mode::No, _) => "no",
            (crate::vm_check::Mode::Cold, true) => "cold",
            (crate::vm_check::Mode::Live, true) => "live",
            (_, false) => "ok",
        };
        println!("  {:<10} {}", v.node, mode);
        for r in &v.reasons {
            // Not running: the live-only reasons don't matter.
            if r.level == "cold" && view.running_on.is_none() {
                continue;
            }
            println!("    {:<5} {}", r.level, r.text);
        }
    }
}

/// `raidish node ...` through any `listen` node's API.
pub async fn run_node(command: &NodeCommands, endpoints: Vec<String>) -> Result<(), String> {
    let api = Api {
        endpoints,
        http: crate::tls::http_builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| e.to_string())?,
    };
    match command {
        NodeCommands::Facts { node } => {
            let view: crate::vms_api::FactsView = api
                .send(|c, b| c.get(format!("{}/api/v1/nodes/facts", b)))
                .await?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            if let Some(n) = node {
                if !view.nodes.contains_key(n) {
                    return Err(format!("no facts from '{}' (yet)", n));
                }
            }
            for (name, f) in &view.nodes {
                if node.as_ref().is_some_and(|n| n != name) {
                    continue;
                }
                print_facts(f, &view);
            }
        }
    }
    Ok(())
}

fn ago(ms: u64) -> String {
    let s = crate::cluster::now_ms().saturating_sub(ms) / 1000;
    match s {
        0..=119 => format!("{}s ago", s),
        120..=7199 => format!("{}m ago", s / 60),
        _ => format!("{}h ago", s / 3600),
    }
}

fn print_facts(f: &crate::node_facts::NodeFacts, view: &crate::vms_api::FactsView) {
    let up = view.live.contains(&f.node);
    println!(
        "{} ({}){}  collected {}, raidish {}",
        f.node,
        f.host.hostname,
        if up { "" } else { "  DOWN: as it last was" },
        ago(f.collected_at),
        f.raidish
    );
    let h = &f.host;
    println!(
        "  host     {} {}, {} ({} threads), {} MiB, kvm {}",
        h.arch,
        h.kernel,
        if h.cpu_model.is_empty() {
            "?"
        } else {
            &h.cpu_model
        },
        h.cpu_threads,
        h.memory_mb,
        if h.kvm { "yes" } else { "no" }
    );
    println!(
        "  peer I/O {}",
        if f.peer_io {
            "on"
        } else {
            "off (no `tls:`): VMs move cold only"
        }
    );
    match &f.libvirt {
        None => println!("  libvirt  - (no `vm:` in its config: runs no VMs)"),
        Some(l) if l.error.is_some() => {
            println!("  libvirt  {}: {}", l.uri, l.error.as_deref().unwrap_or(""))
        }
        Some(l) => {
            println!(
                "  libvirt  {} (libvirt {}, qemu {}, {})",
                l.uri, l.libvirt_version, l.qemu_version, l.virt_type
            );
            println!(
                "  cpu      host-model {} ({} features); {} named models usable",
                if l.cpu.model.is_empty() {
                    "?"
                } else {
                    &l.cpu.model
                },
                l.cpu.features.len(),
                l.cpu_models.len()
            );
            println!(
                "  networks {}",
                if l.networks.is_empty() {
                    "-".into()
                } else {
                    l.networks.join(", ")
                }
            );
            println!(
                "  bridges  {}",
                if l.bridges.is_empty() {
                    "-".into()
                } else {
                    l.bridges.join(", ")
                }
            );
        }
    }
    // Its libvirt TLS, as the others reach it.
    let seen: Vec<(&String, &crate::node_facts::PeerFacts)> = view
        .nodes
        .iter()
        .filter(|(n, _)| *n != &f.node)
        .filter_map(|(n, o)| o.peers.get(&f.node).map(|p| (n, p)))
        .collect();
    if f.libvirt.is_some() && !seen.is_empty() {
        println!("  libvirt TLS, from the others:");
        for (from, p) in seen {
            match &p.libvirt {
                Some(pr) => {
                    let cert = pr
                        .cert
                        .as_ref()
                        .map(|c| format!(" [{} {}]", c.subject, c.sans.join(",")))
                        .unwrap_or_default();
                    println!(
                        "    from {:<8} {} ({}){}",
                        from,
                        pr.describe(),
                        p.host,
                        cert
                    );
                }
                None => println!("    from {:<8} - (runs no VMs: doesn't check)", from),
            }
            if let Some(e) = &p.migrate_tls_error {
                println!("    from {:<8} last migration failed on TLS: {}", from, e);
            }
            if let Some(pr) = p.peer_io.as_ref().filter(|p| !p.ok) {
                println!("    from {:<8} peer I/O: {}", from, pr.describe());
            }
        }
    }
    println!();
}

fn print_vms(vms: &[VmView]) {
    if vms.is_empty() {
        println!("no VMs (`vm import <name> <xml>` defines one)");
        return;
    }
    println!(
        "{:<16} {:<8} {:<10} {:<12} {:>5} {:>8}  VOLUMES / NOTES",
        "NAME", "WANT", "NODE", "STATE", "VCPU", "MEMORY"
    );
    for v in vms {
        let mut notes = Vec::new();
        if let Some(e) = &v.spec.error {
            notes.push(format!("error: {}", e));
        }
        if let Some(e) = &v.start_error {
            notes.push(format!("not starting: {}", e));
        }
        if let Some(e) = &v.unreachable {
            notes.push(format!("node unreachable: {}", e));
        }
        if v.restart_pending {
            notes.push("restart pending (XML changed)".into());
        }
        let state = match (&v.state, &v.unreachable) {
            (Some(s), _) => s.clone(),
            (None, Some(_)) => "unknown".into(),
            (None, None) => "stopped".into(),
        };
        println!(
            "{:<16} {:<8} {:<10} {:<12} {:>5} {:>8}  {}{}",
            v.spec.name,
            if v.want_running { "running" } else { "stopped" },
            v.node.as_deref().unwrap_or("-"),
            state,
            v.vcpus.map_or("-".into(), |n| n.to_string()),
            v.memory_kib
                .map_or("-".into(), |k| format!("{}M", k / 1024)),
            v.spec.volumes.join(","),
            if notes.is_empty() {
                String::new()
            } else {
                format!("  [{}]", notes.join("; "))
            }
        );
    }
}
