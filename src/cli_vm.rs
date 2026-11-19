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
    /// else shut down here and started there.
    Move { name: String, node: String },
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
        VmCommands::Move { name, node } => {
            api.send(|c, b| {
                c.post(format!("{}/api/v1/vms/{}/move", b, name))
                    .json(&serde_json::json!({ "node": node }))
            })
            .await?;
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
