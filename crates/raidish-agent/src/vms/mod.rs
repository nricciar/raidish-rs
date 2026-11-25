//! The VMs on this node: starting the ones that belong here, stopping the
//! rest, orphans, arrivals, and live migration (`migrate`).

pub(crate) mod migrate;
pub(crate) mod plan;
pub(crate) mod state;

use crate::committer::PoolAdmin;
use crate::events::{EventKind, Level, Subject};
use crate::inode::{DatasetProps, Transport};
use crate::placement::Placement;
use crate::types::DatasetName;
use crate::vm_store::DefSpec;
use crate::volume::VolumeInfo;
use crate::volumes::VolumeView;
use crate::{Agent, peer_host, peer_url};
use state::{Recovery, StandInStep, StopStep, Stopping, VmState};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

/// What the VMs here ask of the volumes: where theirs go, and which must
/// stay where they are. Built by the VM side (`Agent::vm_demand`, then
/// [`VmDemand::saw`]); the volume side only reads it.
#[derive(Clone, Default)]
pub(crate) struct VmDemand {
    /// VM volumes, placed by their VM's anchor: its settings (exported
    /// over NBD for qemu, not attached to the host kernel) and its holder.
    pub(crate) place: BTreeMap<DatasetName, (DatasetProps, Option<String>)>,
    /// Volumes of VMs migrating away from here: kept here until they're
    /// gone (see `migrate`), whatever their placement says.
    pub(crate) migrating: BTreeSet<DatasetName>,
    /// Every volume a VM definition names (usable or not).
    pub(crate) disks: BTreeSet<DatasetName>,
    /// Volumes a VM of ours running here uses.
    pub(crate) in_use: BTreeSet<DatasetName>,
    /// libvirt couldn't be asked what runs here.
    pub(crate) libvirt_unknown: bool,
}

impl VmDemand {
    /// What libvirt runs here: its domains' volumes stay.
    pub(crate) fn saw(&mut self, doms: &Domains) {
        self.in_use = doms.in_use();
        self.libvirt_unknown = doms.listed.is_err();
    }

    /// Whether `name` is a VM's volume (exported for qemu, not attached).
    pub(crate) fn for_vm(&self, name: &DatasetName) -> bool {
        self.place.contains_key(name)
    }

    /// Whether `name` must keep running where it is: a VM here uses it, or
    /// it's a VM's and libvirt can't say whether one does.
    pub(crate) fn pinned(&self, name: &DatasetName) -> bool {
        self.in_use.contains(name) || (self.libvirt_unknown && self.for_vm(name))
    }
}

/// Whether VM `spec`'s volumes belong on this node.
pub(crate) fn anchor_here(p: &Placement, spec: &crate::vm::VmSpec) -> bool {
    spec.anchor().is_some_and(|n| {
        p.volume(n)
            .is_some_and(|a| p.target(n, &a.props).as_deref() == Some(p.me.as_str()))
    })
}

/// What libvirt runs on this node.
pub(crate) struct Domains {
    /// Every domain (Err: libvirt is unknown, so VM volumes stay where
    /// they are: they may be in use).
    pub(crate) listed: Result<Vec<crate::vm::Domain>, String>,
    /// This node's own, running.
    pub(crate) ours: Vec<crate::vm::LocalVm>,
}

impl Domains {
    /// Volumes a VM of ours uses.
    pub(crate) fn in_use(&self) -> BTreeSet<DatasetName> {
        self.ours
            .iter()
            .flat_map(|v| v.meta.volumes.iter().cloned())
            .collect()
    }
}

/// The agent's VM bookkeeping (the runtime is `Agent::vm`).
#[derive(Default)]
pub(crate) struct VmBook {
    /// The VM definitions as last read.
    pub(crate) specs: BTreeMap<crate::types::VmName, crate::vm::VmSpec>,
    /// What this node is doing with each VM, beyond what libvirt says (see
    /// `state`): none, it runs or not as libvirt says.
    pub(crate) states: BTreeMap<crate::types::VmName, VmState>,
}

impl VmBook {
    /// Whether VM `name` is leaving, or just left, by migration.
    pub(crate) fn leaving(&self, name: &crate::types::VmName) -> bool {
        self.states.get(name).is_some_and(VmState::leaving)
    }

    /// VM `name`'s stop, if it's being stopped here.
    pub(crate) fn stopping(&self, name: &crate::types::VmName) -> Option<&Stopping> {
        match self.states.get(name) {
            Some(VmState::Stopping(s)) => Some(s),
            _ => None,
        }
    }

    /// The VMs in states `pick` selects.
    fn named(&self, pick: impl Fn(&VmState) -> bool) -> Vec<crate::types::VmName> {
        self.states
            .iter()
            .filter(|(_, s)| pick(s))
            .map(|(n, _)| n.clone())
            .collect()
    }
}

impl Agent {
    /// The volumes here, as the VM side sees them (read only).
    pub(crate) fn volumes_here(&self) -> &dyn VolumeView {
        &self.shared.volumes
    }

    /// What the VMs ask of `volumes`, as far as their definitions and the
    /// migrations from here say (libvirt's part comes later, `VmDemand::saw`).
    pub(crate) fn vm_demand(
        &self,
        volumes: &BTreeMap<DatasetName, VolumeInfo>,
        state: &crate::cluster::state::ClusterState,
        now: u64,
    ) -> VmDemand {
        let mut place = BTreeMap::new();
        if self.vm.is_some() {
            for spec in self.vms.specs.values().filter(|s| s.usable()) {
                let Some((anchor_name, anchor)) =
                    spec.anchor().and_then(|a| Some((a, volumes.get(a)?)))
                else {
                    continue;
                };
                let props = DatasetProps {
                    automount: Some(Transport::Nbd),
                    ..anchor.props.clone()
                };
                let h = state.dataset_node(anchor_name, now).map(str::to_string);
                for v in spec.volumes() {
                    place
                        .entry(v.clone())
                        .or_insert_with(|| (props.clone(), h.clone()));
                }
            }
        }
        let migrating = self
            .vms
            .states
            .values()
            .filter_map(|s| match s {
                VmState::Migrating(m) => Some(m.volumes.iter().cloned()),
                _ => None,
            })
            .flatten()
            .collect();
        let disks = self
            .vms
            .specs
            .values()
            .flat_map(|s| s.volumes().iter().cloned())
            .collect();
        VmDemand {
            place,
            migrating,
            disks,
            ..Default::default()
        }
    }

    /// Which libvirt domains are this node's (those started for this pool,
    /// by its GUID, by this node), and the VM definitions. On a read error
    /// the last ones read are kept: losing them would look like every VM
    /// was deleted.
    pub(crate) async fn refresh_vms(&mut self) {
        let Some(vm) = &self.vm else {
            return;
        };
        if let Some(w) = &self.writer
            && let Some(sb) = w.fs.superblock().await
        {
            vm.set_pool(sb.pool_guid);
        }
        match self.committer.vms().await {
            Ok(defs) => {
                let mut specs: BTreeMap<crate::types::VmName, crate::vm::VmSpec> =
                    defs.into_iter().map(|(n, d)| (n, d.spec())).collect();
                crate::vm::mark_conflicts(&mut specs);
                self.vms.specs = specs;
            }
            Err(e) => eprintln!("agent: reading VM definitions: {}", e),
        }
    }

    /// What libvirt runs here.
    pub(crate) async fn domains(&self) -> Domains {
        // Not knowing which are ours yet counts as not knowing.
        let listed = match &self.vm {
            Some(vm) if vm.ident().is_none() => Err("the pool's GUID isn't known yet".to_string()),
            Some(vm) => vm.virsh.list().await.map_err(|e| {
                eprintln!("agent: {}", e);
                e.to_string()
            }),
            None => Ok(Vec::new()),
        };
        // Only this node's: whatever else shares its libvirt (other nodes,
        // other clusters, domains nobody stamped) is left alone.
        // A running domain whose owner libvirt couldn't tell (I-25) makes
        // the whole listing unknown: it may be ours, using volumes that
        // must stay, or one to start again.
        let mut listed = listed;
        let mut ours = Vec::new();
        if let (Some(vm), Ok(doms)) = (&self.vm, &listed) {
            let mut unknown = None;
            for d in doms.iter().filter(|d| d.id.is_some()) {
                match vm.our_vm(d.clone()).await {
                    Ok(Some(v)) => ours.push(v),
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("agent: who owns domain '{}' is unknown: {}", d.name, e);
                        unknown.get_or_insert(format!("domain '{}': {}", d.name, e));
                    }
                }
            }
            if let Some(e) = unknown {
                listed = Err(e);
            }
        }
        Domains { listed, ours }
    }

    /// This node's domains that shouldn't run here (any more): orphans are
    /// destroyed, VMs asked to move migrate live if they can, the rest shut
    /// down (destroyed after the timeout). VMs that just arrived by
    /// migration are made this node's.
    pub(crate) async fn stop_unwanted_vms(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        p: &Placement,
        ours: &[crate::vm::LocalVm],
    ) {
        // A failed migration is tried again only once the VM is asked to
        // move somewhere (else) again.
        let specs = &self.vms.specs;
        self.vms.states.retain(|vm, st| {
            let VmState::Stopping(s) = st else {
                return true;
            };
            let move_to = specs
                .get(vm)
                .and_then(|s| p.volume(s.anchor()?))
                .and_then(|a| a.props.move_to.as_deref());
            s.cold_to = state::keep_cold_to(s.cold_to.take(), move_to);
            !s.idle()
        });
        let now = std::time::Instant::now();
        let mut actions = Vec::new();
        for v in ours {
            let restarting = vm.restarting(&v.name);
            let book = &self.vms;
            let here = self.volumes_here();
            actions.extend(plan::plan_domain(
                p,
                book,
                here,
                v,
                restarting,
                vm.shutdown_timeout,
                now,
            ));
        }
        for a in actions {
            self.act_on_vm(vm, p, ours, a, now).await;
        }
        // Stopped: its domain is gone. (One moving cold whose shutdown
        // wasn't sent yet just goes: there was nothing here to stop.)
        let up = |n: &str| ours.iter().any(|v| v.name.as_str() == n);
        let gone = self.vms.named(|s| matches!(s, VmState::Stopping(_)));
        for n in gone.iter().filter(|n| !up(n)) {
            let sent = self
                .vms
                .stopping(n)
                .is_some_and(|s| s.shutdown_sent.is_some());
            self.vms.states.remove(n);
            if sent {
                self.note(
                    Level::Info,
                    EventKind::VmStopped,
                    Subject::vm(n),
                    format!("VM '{}' stopped on {}", n, self.node),
                );
            }
        }
        // Restarts are done once the old domain is gone (it's started again
        // by `start_wanted_vms`, from the current XML).
        vm.restarts_done(up);
    }

    /// Carry out one decision of `plan::plan_domain` for a domain in `ours`.
    async fn act_on_vm(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        p: &Placement,
        ours: &[crate::vm::LocalVm],
        action: plan::VmAction,
        now: std::time::Instant,
    ) {
        use plan::VmAction;
        let name = match &action {
            VmAction::Arrived(n)
            | VmAction::DestroyOrphan(n)
            | VmAction::DetachLeaked(n, _)
            | VmAction::Keep(n)
            | VmAction::Unwanted { vm: n, .. } => n.clone(),
        };
        let Some(v) = ours.iter().find(|v| v.name == name) else {
            return;
        };
        match action {
            VmAction::Arrived(_) => self.arrived(vm, v).await,
            VmAction::DestroyOrphan(_) => {
                if let Err(e) = vm.virsh.destroy(&name).await {
                    eprintln!("agent: {}", e);
                }
                self.note(
                    Level::Warn,
                    EventKind::VmOrphan,
                    Subject::vm(&name),
                    format!(
                        "VM '{}' was running on {} without its volumes held there; destroyed it",
                        name, self.node
                    ),
                );
                self.vms.states.remove(&name);
            }
            VmAction::DetachLeaked(_, volumes) => {
                for x in volumes {
                    let op = crate::cluster::LeaseOp::SetAttached {
                        volume: x.to_string(),
                        nodes: Vec::new(),
                    };
                    match self.leases.call(&op).await {
                        Ok(_) => self.note(
                            Level::Warn,
                            EventKind::VmMigrationRecovered,
                            Subject::vm(&name),
                            format!(
                                "volume '{}' of VM '{}' (running on {}) was attached to {} with \
                                 no migration under way; detached it from all",
                                x,
                                name,
                                self.node,
                                p.attached(&x).join(", ")
                            ),
                        ),
                        Err(e) => eprintln!("agent: detaching '{}': {}", x, e),
                    }
                }
            }
            VmAction::Keep(_) => {
                self.vms.states.remove(&name);
            }
            VmAction::Unwanted {
                migrate_to,
                stop: step,
                ..
            } => {
                if let Some(to) = migrate_to
                    && self.start_migration(vm, v, &to)
                {
                    return;
                }
                let stop = self.vms.stopping(&name).cloned().unwrap_or_default();
                match step {
                    StopStep::Shutdown => {
                        self.note(
                            Level::Info,
                            EventKind::VmStopping,
                            Subject::vm(&name),
                            format!("VM '{}' shutting down on {}", name, self.node),
                        );
                        if let Err(e) = vm.virsh.shutdown(&name).await {
                            eprintln!("agent: {}", e);
                        }
                        self.vms.states.insert(
                            name,
                            VmState::Stopping(Stopping {
                                shutdown_sent: Some(now),
                                ..stop
                            }),
                        );
                    }
                    StopStep::ForceOff => {
                        self.note(
                            Level::Warn,
                            EventKind::VmForcedOff,
                            Subject::vm(&name),
                            format!(
                                "VM '{}' didn't shut down in {:?}; forced it off",
                                name, vm.shutdown_timeout
                            ),
                        );
                        if let Err(e) = vm.virsh.destroy(&name).await {
                            eprintln!("agent: {}", e);
                        }
                    }
                    StopStep::Wait => {}
                }
            }
        }
    }

    /// Start the VMs that belong here once all their volumes run here, and
    /// publish why the others can't start.
    pub(crate) async fn start_wanted_vms(
        &self,
        vm: &Arc<crate::vm::VmRuntime>,
        p: &Placement,
        listed: &[crate::vm::Domain],
        ours: &[crate::vm::LocalVm],
    ) {
        let mut errors = BTreeMap::new();
        for spec in self.vms.specs.values() {
            if !p.state.vms.contains_key(spec.name.as_str()) || !anchor_here(p, spec) {
                continue;
            }
            // Leaving, or just left, by migration: it runs elsewhere even
            // where its domain is already gone from here.
            // Or a failed migration's guest may run elsewhere (unknown yet).
            if self.vms.leaving(&spec.name) {
                continue;
            }
            if let Some(e) = spec.error() {
                self.note_once(
                    Level::Warn,
                    EventKind::VmInvalid,
                    Subject::vm(&spec.name),
                    format!("VM '{}' can't start: {}", spec.name, e),
                );
                errors.insert(spec.name.to_string(), e.to_string());
                continue;
            }
            if let Some(d) = listed
                .iter()
                .find(|d| d.name.as_str() == spec.name.as_str() && d.id.is_some())
            {
                if !ours.iter().any(|v| v.name.as_str() == d.name) {
                    let e = format!(
                        "a domain '{}' this node didn't start is running on its libvirt",
                        d.name
                    );
                    self.note_once(
                        Level::Warn,
                        EventKind::VmConflict,
                        Subject::vm(&spec.name),
                        format!("VM '{}' can't start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.to_string(), e);
                } else {
                    self.events.clear(&Subject::vm(&spec.name));
                }
                continue;
            }
            let Some(anchor) = spec.anchor() else {
                continue;
            };
            if !spec.volumes().iter().all(|v| self.volumes_here().writes(v)) {
                continue; // its disks aren't all here yet
            }
            let Some(owner) = self.volumes_here().owner_of(anchor) else {
                continue;
            };
            let Some(ident) = vm.ident() else {
                continue;
            };
            let rendered =
                crate::vm::render(spec, (&vm.nbd.0, vm.nbd.1), &vm.console, &owner, &ident);
            let started = match rendered {
                Ok(xml) => vm.virsh.create(&spec.name, &xml).await,
                Err(e) => Err(e),
            };
            match started {
                Ok(()) => {
                    self.events.clear(&Subject::vm(&spec.name));
                    self.note(
                        Level::Info,
                        EventKind::VmStarted,
                        Subject::vm(&spec.name),
                        format!("VM '{}' started on {}", spec.name, self.node),
                    );
                }
                Err(e) => {
                    self.note_once(
                        Level::Error,
                        EventKind::VmStartFailed,
                        Subject::vm(&spec.name),
                        format!("VM '{}' failed to start on {}: {}", spec.name, self.node, e),
                    );
                    errors.insert(spec.name.to_string(), e.to_string());
                }
            }
        }
        self.state.write().unwrap().vm_errors = errors;
    }

    /// Destroy (no shutdown: there's no time) every VM of ours here using
    /// one of `volumes`.
    pub(crate) async fn destroy_vms_using(&mut self, volumes: &[DatasetName]) {
        let Some(vm) = self.vm.clone() else {
            return;
        };
        let doms = match vm.virsh.list().await {
            Ok(d) => d,
            Err(e) => {
                eprintln!(
                    "agent: the leases of {:?} are gone, but libvirt can't be asked what \
                     uses them: {}",
                    volumes, e
                );
                return;
            }
        };
        for d in doms.into_iter().filter(|d| d.id.is_some()) {
            // Whose it is unknown (I-25): it goes if its disks reach a lost
            // volume through this node's export, which is going anyway;
            // that fails safe without touching another node's domains.
            let uses = match vm.our_vm(d.clone()).await {
                Ok(Some(v)) => v.meta.volumes.iter().any(|x| volumes.contains(x)),
                Ok(None) => false,
                Err(e) => match vm.exported_volumes_of(&d.name, volumes).await {
                    Ok(used) => !used.is_empty(),
                    Err(e2) => {
                        eprintln!(
                            "agent: whether domain '{}' uses a lost volume is unknown: {}; {}",
                            d.name, e, e2
                        );
                        false
                    }
                },
            };
            if !uses {
                continue;
            }
            self.note(
                Level::Error,
                EventKind::VmKilled,
                Subject::vm(&d.name),
                format!(
                    "VM '{}' was killed on {}: its volume lease was lost",
                    d.name, self.node
                ),
            );
            if let Err(e) = vm.virsh.destroy(&d.name).await {
                eprintln!("agent: {}", e);
            }
            let name = d.name.as_str();
            if matches!(self.vms.states.get(name), Some(VmState::Stopping(_))) {
                self.vms.states.remove(name);
            }
        }
    }

    /// Migrate VM `v` live to `to` (`plan::migration_target` decided it
    /// should): true if the migration started, false to move it cold (`to`
    /// can't be reached).
    pub(crate) fn start_migration(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        v: &crate::vm::LocalVm,
        to: &str,
    ) -> bool {
        let name = &v.name;
        let Some(spec) = self.vms.specs.get(name) else {
            return false;
        };
        let to = to.to_string();
        let peers = &self.cluster.members.peers;
        let (Some(url), Some(host)) = (peer_url(peers, &to), peer_host(peers, &to)) else {
            return false;
        };
        let http = match crate::tls::http_client(Some(Duration::from_secs(10))) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("agent: migrating VM '{}': {}", name, e);
                return false;
            }
        };
        let job = migrate::Job {
            leases: self.leases.clone(),
            http,
            url,
            host,
            name: name.clone(),
            volumes: spec.volumes().to_vec(),
            me: self.node.clone(),
            to: to.clone(),
            virsh: vm.virsh.clone(),
            meta: v.meta.clone(),
            timeout: vm.migrate_timeout,
            facts: self.facts.clone(),
            committer: self.committer.clone(),
        };
        println!("agent: moving VM '{}' to {} (live, if it can be)", name, to);
        self.vms.states.insert(
            name.clone(),
            VmState::Migrating(migrate::Migration {
                to,
                volumes: spec.volumes().to_vec(),
                started: std::time::Instant::now(),
                task: tokio::spawn(job.run()),
            }),
        );
        true
    }

    /// Finish what migrations from here did (see `migrate`): hand the
    /// volumes of a VM that left to where it went, or put a failed one's
    /// back, to move cold.
    pub(crate) async fn poll_migrations(&mut self) {
        let done = self
            .vms
            .named(|s| matches!(s, VmState::Migrating(m) if m.task.is_finished()));
        for name in done {
            let Some(VmState::Migrating(m)) = self.vms.states.remove(&name) else {
                continue;
            };
            let result = match m.task.await {
                Ok(r) => r,
                Err(e) => Err(migrate::Failure::Failed(e.to_string())),
            };
            // A failure is noted once it's settled (`settle_failed`).
            match &result {
                Ok(()) => {
                    self.facts.note_migration(&m.to, &Ok(()));
                    self.note(
                        Level::Info,
                        EventKind::VmMigrated,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' migrated live from {} to {} in {:.1}s",
                            name,
                            self.node,
                            m.to,
                            m.started.elapsed().as_secs_f64()
                        ),
                    );
                }
                Err(migrate::Failure::NotLive(why)) => {
                    self.note(
                        Level::Info,
                        EventKind::VmMovingCold,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' moves cold from {} to {}: {}",
                            name, self.node, m.to, why
                        ),
                    );
                }
                Err(migrate::Failure::Failed(_)) => {}
            }
            let next = state::finished(m.to, m.volumes, result);
            self.vms.states.insert(name, next);
        }
        self.settle_failed().await;
        // Migrated: detach here (so each volume is handed over), then, once
        // none runs here, there too (back to their own placement).
        let names = self
            .vms
            .named(|s| matches!(s, VmState::HandingOffVolumes(_)));
        for name in names {
            let Some(VmState::HandingOffVolumes(mut m)) = self.vms.states.remove(&name) else {
                continue;
            };
            if !m.detached {
                let mut ok = true;
                for v in &m.volumes {
                    if let Err(e) = migrate::attach(&self.leases, v, &self.node, false).await {
                        eprintln!("agent: detaching '{}' from {}: {}", v, self.node, e);
                        ok = false;
                    }
                }
                m.detached = ok;
                self.vms.states.insert(name, VmState::HandingOffVolumes(m));
                continue;
            }
            if m.volumes.iter().any(|v| self.volumes_here().writes(v)) {
                self.vms.states.insert(name, VmState::HandingOffVolumes(m));
                continue;
            }
            let mut ok = true;
            for v in &m.volumes {
                if let Err(e) = migrate::attach(&self.leases, v, &m.to, false).await {
                    eprintln!("agent: detaching '{}' from {}: {}", v, m.to, e);
                    ok = false;
                }
            }
            // Detached from both: its record goes (tried again until it does).
            if ok {
                match migrate::end(&self.leases, &name, &self.node, &m.to).await {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("agent: ending VM '{}''s migration record: {}", name, e);
                        ok = false;
                    }
                }
            }
            if !ok {
                self.vms.states.insert(name, VmState::HandingOffVolumes(m));
            }
        }
    }

    /// Failed live migrations: where does the guest run now? It may have
    /// switched over before the failure (`migrate::Whereabouts`).
    pub(crate) async fn settle_failed(&mut self) {
        let names = self.vms.named(|s| matches!(s, VmState::Unsettled(_)));
        for name in names {
            let Some(VmState::Unsettled(u)) = self.vms.states.remove(&name) else {
                continue;
            };
            let here = match &self.vm {
                Some(vm) => vm.virsh.list().await.ok().map(|l| {
                    l.iter()
                        .any(|d| d.name.as_str() == name.as_str() && d.id.is_some())
                }),
                None => Some(false),
            };
            // Here, but libvirt may still be migrating it (a recovered
            // one: libvirtd outlives this agent): not settled until it
            // isn't, or that can be asked.
            if here == Some(true)
                && let Some(vm) = &self.vm
            {
                let job = vm.virsh.job_active(&name).await;
                if !matches!(job, Ok(false)) {
                    let why = match job {
                        Ok(_) => "libvirt is still migrating it".to_string(),
                        Err(e) => format!("whether libvirt still migrates it is unknown: {}", e),
                    };
                    self.note_once(
                        Level::Warn,
                        EventKind::VmMigrateFailed,
                        Subject::vm(&name),
                        format!(
                            "VM '{}''s migration from {} to {} isn't settled: {}",
                            name, self.node, u.to, why
                        ),
                    );
                    self.vms.states.insert(name, VmState::Unsettled(u));
                    continue;
                }
            }
            // B only matters if it isn't here.
            let there = if here == Some(false) {
                self.ask_runs_there(&u.to, &name).await
            } else {
                None
            };
            let w = migrate::whereabouts(here, there);
            match w {
                migrate::Whereabouts::Unknown => {
                    if !u.told {
                        self.note(
                            Level::Warn,
                            EventKind::VmMigrateFailed,
                            Subject::vm(&name),
                            format!(
                                "VM '{}' couldn't migrate live from {} to {} ({}) and no \
                                 longer runs on {}; {} can't be asked whether it runs there, \
                                 so it's left alone until it can",
                                name, self.node, u.to, u.why, self.node, u.to
                            ),
                        );
                    }
                }
                migrate::Whereabouts::There => {
                    self.facts.note_migration(&u.to, &Ok(()));
                    let msg = match u.recovered {
                        true => format!(
                            "VM '{}' runs on {}: finishing its migration from {} ({})",
                            name, u.to, self.node, u.why
                        ),
                        false => format!(
                            "VM '{}' migrated live from {} to {} (though {} said it failed: {})",
                            name, self.node, u.to, self.node, u.why
                        ),
                    };
                    self.note(Level::Info, EventKind::VmMigrated, Subject::vm(&name), msg);
                }
                migrate::Whereabouts::Here | migrate::Whereabouts::Neither => {
                    let at = if w == migrate::Whereabouts::Here {
                        format!("it still runs on {}", self.node)
                    } else {
                        "it runs nowhere".to_string()
                    };
                    self.facts.note_migration(&u.to, &Err(u.why.clone()));
                    self.note(
                        Level::Warn,
                        EventKind::VmMigrateFailed,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' couldn't migrate live from {} to {} ({}); {}; moving it cold",
                            name, self.node, u.to, u.why, at
                        ),
                    );
                    let mut ok = true;
                    for v in &u.volumes {
                        for node in [u.to.as_str(), self.node.as_str()] {
                            if let Err(e) = migrate::attach(&self.leases, v, node, false).await {
                                eprintln!("agent: detaching '{}' from {}: {}", v, node, e);
                                ok = false;
                            }
                        }
                    }
                    // Left recorded on failure: settled again once this
                    // VM's stop is over (`recover_migrations`).
                    if ok {
                        match migrate::end(&self.leases, &name, &self.node, &u.to).await {
                            Ok(()) => {}
                            Err(e) => {
                                eprintln!("agent: ending VM '{}''s migration record: {}", name, e)
                            }
                        }
                    }
                }
            }
            self.vms.states.insert(name, state::settled(u, w));
        }
    }

    /// Migrations recorded in the cluster state that nobody runs any more
    /// (see `state::recovery`): this node's own, after a restart, are
    /// settled again; a gone source's, if this node is their target, are
    /// finished here in its place.
    ///
    /// Decided on a linearizable read, not on this tick's view: that may
    /// lag behind the cluster (just restarted and still catching up, or
    /// just ended a record that isn't applied here yet), and a record
    /// that's over must not be finished again. The read is made only when
    /// the view shows one to finish.
    pub(crate) async fn recover_migrations(&mut self, p: &Placement) {
        let me = self.node.as_str();
        let states = &self.vms.states;
        let to_finish = |s: &crate::cluster::state::ClusterState, live: &BTreeSet<String>| {
            s.migrations.iter().any(|(n, r)| {
                state::recovery(r, me, live, states.contains_key(n)) != Recovery::Nothing
            })
        };
        if !to_finish(&p.state, &p.live) {
            return;
        }
        let fresh = match self
            .cluster
            .linearizable_state(Duration::from_secs(5))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("agent: reading the recorded migrations: {}", e);
                return;
            }
        };
        let now = crate::cluster::now_ms();
        let live: BTreeSet<String> = fresh.live_nodes(now).map(|(n, _)| n.to_string()).collect();
        for (name, rec) in &fresh.migrations {
            let has_state = self.vms.states.contains_key(name);
            match state::recovery(rec, &self.node, &live, has_state) {
                Recovery::Nothing => {}
                Recovery::Resettle => {
                    println!(
                        "agent: VM '{}''s migration from {} to {} is recorded but runs nowhere \
                         here: settling it",
                        name, rec.from, rec.to
                    );
                    self.vms.states.insert(
                        name.clone(),
                        VmState::Unsettled(migrate::Unsettled {
                            to: rec.to.clone(),
                            volumes: rec.volumes.clone(),
                            why: format!("interrupted: the agent on {} restarted", self.node),
                            told: false,
                            recovered: true,
                        }),
                    );
                }
                Recovery::StandIn => {
                    let step = state::stand_in_step(
                        rec,
                        &self.node,
                        |v| fresh.attached.get(v.as_str()).cloned().unwrap_or_default(),
                        |v| fresh.dataset_node(v, now).map(str::to_string),
                    );
                    match step {
                        StandInStep::Wait => {}
                        StandInStep::Detach(node, volumes) => {
                            for v in &volumes {
                                match migrate::attach(&self.leases, v, &node, false).await {
                                    Ok(()) => println!(
                                        "agent: VM '{}''s migration from {} (gone): detached \
                                         '{}' from {}",
                                        name, rec.from, v, node
                                    ),
                                    Err(e) => {
                                        eprintln!("agent: detaching '{}' from {}: {}", v, node, e)
                                    }
                                }
                            }
                        }
                        StandInStep::End => {
                            match migrate::end(&self.leases, name, &rec.from, &rec.to).await {
                                Ok(()) => {
                                    self.note(
                                        Level::Warn,
                                        EventKind::VmMigrationRecovered,
                                        Subject::vm(name),
                                        format!(
                                            "VM '{}''s migration from {} to {} was finished by {}: \
                                         {} is gone",
                                            name, rec.from, rec.to, self.node, rec.from
                                        ),
                                    );
                                }
                                Err(e) => eprintln!(
                                    "agent: ending VM '{}''s migration record: {}",
                                    name, e
                                ),
                            }
                        }
                    }
                }
            }
        }
    }

    /// Does `node` run VM `name`? None: it couldn't be asked.
    pub(crate) async fn ask_runs_there(
        &self,
        node: &str,
        name: &crate::types::VmName,
    ) -> Option<bool> {
        let url = peer_url(&self.cluster.members.peers, node)?;
        let http = crate::tls::http_client(Some(Duration::from_secs(5))).ok()?;
        migrate::runs_there(&http, &url, name).await
    }

    /// VM `v` migrated here and its volumes run here now: make it this
    /// node's (its metadata names this node's lease owner).
    pub(crate) async fn arrived(&mut self, vm: &Arc<crate::vm::VmRuntime>, v: &crate::vm::LocalVm) {
        let name = &v.name;
        let Some(owner) = self.volumes_here().owner_of(&v.meta.anchor) else {
            return;
        };
        let meta = crate::vm::DomMeta {
            owner: crate::vm::DomOwner::Lease(owner),
            node: self.node.clone(),
            ..v.meta.clone()
        };
        match vm.virsh.set_meta(name, &meta).await {
            Ok(()) => {
                self.note(
                    Level::Info,
                    EventKind::VmArrived,
                    Subject::vm(name),
                    format!("VM '{}' now runs on {} (migrated)", name, self.node),
                );
            }
            Err(e) => eprintln!("agent: taking migrated VM '{}' on: {}", name, e),
        }
    }
}
