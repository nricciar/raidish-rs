//! Live migration of VMs (`docs/design/mig-2-live-migration.md`).
//!
//! A VM asked to move (its anchor's `move_to`) from the node running it (A)
//! to one that takes migrations (B) is migrated live rather than shut down
//! and started again:
//!
//! 1. Its volumes are attached to A and B (`Request::Attach`): A keeps
//!    writing them (the agent places a migrating VM's volumes here), B
//!    exports them proxied to A (see `super::handoff`).
//! 2. B is asked whether it's ready (`POST /api/v1/vm/incoming/<name>`),
//!    and how to reach it.
//! 3. A's `virsh dumpxml --migratable` is pointed at B (`vm::retarget`) and
//!    `virsh migrate --live --p2p` sends the guest there.
//!
//! The agent loop then finishes it (see `super::Agent::poll_migrations`):
//! on success A detaches itself, so each volume is handed to B; once none
//! runs here, B is detached too. On failure both are detached and the VM
//! moves cold. B re-stamps the domain's metadata once it runs the volumes.

use crate::cluster::LeaseOp;
use crate::cluster::lease::LeaseClient;
use crate::vm::{DomMeta, Incoming, Virsh};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// How long B gets to export the volumes and say it's ready.
const READY_WAIT: Duration = Duration::from_secs(60);

/// A live migration from this node, under way.
pub(super) struct Migration {
    pub to: String,
    pub volumes: Vec<String>,
    pub started: Instant,
    pub task: JoinHandle<Result<(), String>>,
}

/// Migrated away: its volumes being handed to `to`.
pub(super) struct Migrated {
    pub to: String,
    pub volumes: Vec<String>,
    /// This node detached from them (so they're handed over).
    pub detached: bool,
}

/// Attach `volume` to `node` too, or (`on: false`) no longer.
pub(super) async fn attach(
    leases: &LeaseClient,
    volume: &str,
    node: &str,
    on: bool,
) -> Result<(), String> {
    leases
        .call(&LeaseOp::Attach {
            volume: volume.to_string(),
            node: node.to_string(),
            on,
        })
        .await
        .map(|_| ())
}

/// Everything a migration needs, taken from the agent when it starts.
pub(super) struct Job {
    pub leases: LeaseClient,
    pub http: reqwest::Client,
    /// B's API (`https://node1:8881`).
    pub url: String,
    pub name: String,
    pub volumes: Vec<String>,
    pub me: String,
    pub to: String,
    pub virsh: Virsh,
    /// The domain's metadata here.
    pub meta: DomMeta,
    pub timeout: Duration,
}

impl Job {
    /// Steps 1–3 above. Ok once the guest runs on B.
    pub async fn run(self) -> Result<(), String> {
        for node in [&self.me, &self.to] {
            for v in &self.volumes {
                attach(&self.leases, v, node, true)
                    .await
                    .map_err(|e| format!("attaching '{}' to {}: {}", v, node, e))?;
            }
        }
        let to = self.ready().await?;
        let xml = self.virsh.dumpxml_migratable(&self.name).await?;
        let meta = DomMeta {
            owner: String::new(),
            node: to.node.clone(),
            migrating: true,
            ..self.meta.clone()
        };
        let xml = crate::vm::retarget(&xml, &to, &meta)?;
        self.virsh
            .migrate(&self.name, &xml, &to, self.timeout)
            .await
    }

    /// B, once it exports every volume (proxied here).
    async fn ready(&self) -> Result<Incoming, String> {
        let url = format!("{}/api/v1/vm/incoming/{}", self.url, self.name);
        let deadline = Instant::now() + READY_WAIT;
        loop {
            let why = match self.http.post(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    return r
                        .json::<Incoming>()
                        .await
                        .map_err(|e| format!("{}: {}", self.to, e));
                }
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    let why = format!("{}: {}", status, body.trim());
                    if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
                        return Err(format!("{} takes no migrations: {}", self.to, why));
                    }
                    why
                }
                Err(e) => e.to_string(),
            };
            if Instant::now() >= deadline {
                return Err(format!("{} isn't ready: {}", self.to, why));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
