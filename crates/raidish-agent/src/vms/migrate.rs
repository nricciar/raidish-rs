//! Live migration of VMs (`docs/design/mig-2-live-migration.md`).
//!
//! A VM asked to move (its anchor's `move_to`) from the node running it (A)
//! to one that takes migrations (B) is migrated live rather than shut down
//! and started again:
//!
//! 0. Whether it can be is checked first (`crate::vm_check`, from both
//!    nodes' facts and what B says about the VM's CPU now): if not, it
//!    moves cold at once, saying why.
//! 1. Its volumes are attached to A and B (`Request::Attach`): A keeps
//!    writing them (the agent places a migrating VM's volumes here), B
//!    exports them proxied to A (see `crate::volumes::handoff`).
//! 2. B is asked whether it's ready (`POST /api/v1/vm/incoming/<name>`),
//!    and how to reach it.
//! 3. A's `virsh dumpxml --migratable` is pointed at B (`vm::retarget`) and
//!    `virsh migrate --live --p2p` sends the guest there.
//!
//! The agent loop then finishes it (see `crate::Agent::poll_migrations`):
//! on success A detaches itself, so each volume is handed to B; once none
//! runs here, B is detached too. On failure where the guest runs now is
//! asked first (`Whereabouts`): a migration can fail after the switchover
//! (its answer lost, a timeout at the very end), so the guest may be on B.
//! There: it's finished as if it had succeeded. Here or nowhere: both are
//! detached and the VM moves cold. Not here and B can't be asked: nothing
//! is done (and it isn't started here) until B answers. B re-stamps the
//! domain's metadata once it runs the volumes.

use crate::cluster::LeaseOp;
use crate::cluster::lease::LeaseClient;
use crate::committer::PoolAdmin;
use crate::node_facts::FactsHandle;
use crate::types::DatasetName;
use crate::vm::{DomMeta, Incoming, Virsh};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// How long B gets to export the volumes and say it's ready.
const READY_WAIT: Duration = Duration::from_secs(60);

/// A live migration from this node, under way.
pub(crate) struct Migration {
    pub to: String,
    pub volumes: Vec<DatasetName>,
    pub started: Instant,
    pub task: JoinHandle<Result<(), Failure>>,
}

/// Why a live migration didn't happen.
#[derive(Debug, Clone)]
pub(crate) enum Failure {
    /// The check said it can't be live (nothing was tried).
    NotLive(String),
    /// It was tried and failed.
    Failed(String),
}

impl From<String> for Failure {
    fn from(e: String) -> Self {
        Failure::Failed(e)
    }
}

impl From<crate::vm::VmError> for Failure {
    fn from(e: crate::vm::VmError) -> Self {
        Failure::Failed(e.to_string())
    }
}

/// Migrated away: its volumes being handed to `to`.
pub(crate) struct Migrated {
    pub to: String,
    pub volumes: Vec<DatasetName>,
    /// This node detached from them (so they're handed over).
    pub detached: bool,
}

/// A live migration that failed, until where its guest runs is known.
pub(crate) struct Unsettled {
    pub to: String,
    pub volumes: Vec<DatasetName>,
    pub why: String,
    /// It was said that B couldn't be asked (said once).
    pub told: bool,
    /// Found by its record, not by a job that ran here: the agent here
    /// restarted (or a stale snapshot showed an ended one).
    pub recovered: bool,
}

/// Where a failed migration's guest runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Whereabouts {
    /// Still here (or both: here wins, B destroys its copy once detached).
    Here,
    /// On B only: the switchover happened.
    There,
    /// Neither runs it.
    Neither,
    /// Not here, and B (or this node's libvirt) couldn't be asked.
    Unknown,
}

/// From whether it runs `here` and on B (`there`); None: couldn't be asked.
pub(crate) fn whereabouts(here: Option<bool>, there: Option<bool>) -> Whereabouts {
    match (here, there) {
        (Some(true), _) => Whereabouts::Here,
        (Some(false), Some(true)) => Whereabouts::There,
        (Some(false), Some(false)) => Whereabouts::Neither,
        _ => Whereabouts::Unknown,
    }
}

/// Does the node at `url` run VM `name` (`GET /api/v1/vm/local`)? None: it
/// couldn't be asked, or couldn't ask its libvirt.
pub(crate) async fn runs_there(http: &reqwest::Client, url: &str, name: &str) -> Option<bool> {
    let view: crate::vm::LocalView = http
        .get(format!("{}/api/v1/vm/local", url))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    if view.error.is_some() {
        return None;
    }
    Some(view.vms.iter().any(|v| v.name.as_str() == name))
}

/// Record that VM `vm` migrates from `from` to `to` (`LeaseOp::BeginMigration`).
pub(crate) async fn begin(
    leases: &LeaseClient,
    vm: &crate::types::VmName,
    from: &str,
    to: &str,
    volumes: &[DatasetName],
) -> Result<(), String> {
    recorded(leases.call(&LeaseOp::BeginMigration {
        vm: vm.clone(),
        from: from.to_string(),
        to: to.to_string(),
        volumes: volumes.to_vec(),
    }))
    .await
}

/// VM `vm`'s migration from `from` to `to` is over (`LeaseOp::EndMigration`).
pub(crate) async fn end(
    leases: &LeaseClient,
    vm: &crate::types::VmName,
    from: &str,
    to: &str,
) -> Result<(), String> {
    recorded(leases.call(&LeaseOp::EndMigration {
        vm: vm.clone(),
        from: from.to_string(),
        to: to.to_string(),
    }))
    .await
}

/// A record op's answer: `Refused` is an error too.
async fn recorded(
    call: impl std::future::Future<Output = Result<crate::cluster::state::Response, String>>,
) -> Result<(), String> {
    match call.await? {
        crate::cluster::state::Response::Refused(why) => Err(why),
        _ => Ok(()),
    }
}

/// Attach `volume` to `node` too, or (`on: false`) no longer.
pub(crate) async fn attach(
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
pub(crate) struct Job {
    pub leases: LeaseClient,
    pub http: reqwest::Client,
    /// B's API (`https://node1:8881`).
    pub url: String,
    /// B's host in that URL: its libvirt and qemu's stream, by default.
    pub host: String,
    pub name: crate::types::VmName,
    pub volumes: Vec<DatasetName>,
    pub me: String,
    pub to: String,
    pub virsh: Virsh,
    /// The domain's metadata here.
    pub meta: DomMeta,
    pub timeout: Duration,
    /// This node's facts, and the committer for the target's.
    pub facts: FactsHandle,
    pub committer: crate::committer::Admin,
}

impl Job {
    /// Steps 0–3 above. Ok once the guest runs on B.
    pub async fn run(self) -> Result<(), Failure> {
        // Judged by what it runs as (its CPU as the guest has it, see
        // `vm_check::judged_domain`).
        let live_xml = self.virsh.dumpxml_judged(&self.name).await?;
        let verdict = self.check(&live_xml).await;
        if verdict.mode != crate::vm_check::Mode::Live {
            return Err(Failure::NotLive(verdict.why()));
        }
        // Its intent first, so it's finished even if this agent restarts
        // or is gone (`state::recovery`): ended once both are detached.
        begin(&self.leases, &self.name, &self.me, &self.to, &self.volumes)
            .await
            .map_err(|e| format!("recording the migration: {}", e))?;
        for node in [&self.me, &self.to] {
            for v in &self.volumes {
                attach(&self.leases, v, node, true)
                    .await
                    .map_err(|e| format!("attaching '{}' to {}: {}", v, node, e))?;
            }
        }
        let to = self.ready().await?.resolved(&self.host);
        let xml = self.virsh.dumpxml_migratable(&self.name).await?;
        let meta = DomMeta {
            owner: crate::vm::DomOwner::Migrating,
            node: to.node.clone(),
            ..self.meta.clone()
        };
        let xml = crate::vm::retarget(&xml, &to, &meta)?;
        Ok(self
            .virsh
            .migrate(&self.name, &xml, &to, self.timeout)
            .await?)
    }

    /// Step 0: can it go live to B?
    async fn check(&self, xml: &str) -> crate::vm_check::Verdict {
        use crate::vm_check::{check, cpu_to_compare, needs};
        let needs = match needs(xml) {
            Ok(n) => n,
            Err(e) => {
                let mut n = crate::vm_check::Needs::default();
                eprintln!("agent: reading VM '{}': {}", self.name, e);
                n.virt_type = "kvm".into();
                n
            }
        };
        let me = self.facts.with_fresh_probe(&self.to).await;
        let target = match self.committer.node_facts().await {
            Ok(mut all) => all.remove(&self.to),
            Err(_) => None,
        };
        let live = ask_live(&self.http, &self.url, cpu_to_compare(&needs, me.as_ref())).await;
        check(
            &needs,
            &self.to,
            me.as_ref(),
            target.as_ref(),
            live.as_ref(),
        )
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

/// What the node at `url` says now about running a VM with CPU `cpu`
/// (`POST /api/v1/vm/check`). None: it couldn't be asked.
pub async fn ask_live(
    http: &reqwest::Client,
    url: &str,
    cpu: Option<String>,
) -> Option<crate::vm_check::Live> {
    let r = http
        .post(format!("{}/api/v1/vm/check", url))
        .json(&crate::vm_check::LiveAsk { cpu })
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json().await.ok()
}
