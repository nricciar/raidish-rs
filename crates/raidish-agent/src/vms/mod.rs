//! The VMs on this node: starting the ones that belong here, stopping the
//! rest, orphans, arrivals, and live migration (`migrate`).

pub(crate) mod migrate;

use crate::committer::PoolAdmin;
use crate::events::{EventKind, Level, Subject};
use crate::placement::Placement;
use crate::types::DatasetName;
use crate::vm_store::DefSpec;
use crate::{Agent, peer_host, peer_url};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

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
    /// VMs asked to shut down here, and when.
    pub(crate) stopping: BTreeMap<crate::types::VmName, std::time::Instant>,
    /// Live migrations of VMs from here, under way (see `migrate`).
    pub(crate) migrations: BTreeMap<crate::types::VmName, migrate::Migration>,
    /// VMs migrated away whose volumes are being handed over.
    pub(crate) migrated: BTreeMap<crate::types::VmName, migrate::Migrated>,
    /// VMs whose live migration to a node failed: they move there cold.
    pub(crate) migrate_failed: BTreeMap<crate::types::VmName, String>,
    /// Failed live migrations whose guest's whereabouts aren't settled yet
    /// (see `Agent::settle_failed`).
    pub(crate) unsettled: BTreeMap<crate::types::VmName, migrate::Unsettled>,
}

impl Agent {
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
        self.vms.migrate_failed.retain(|vm, to| {
            specs
                .get(vm)
                .and_then(|s| p.volume(s.anchor()?))
                .is_some_and(|a| a.props.move_to.as_deref() == Some(to.as_str()))
        });
        for v in ours {
            let name = &v.name;
            if self.vms.migrations.contains_key(name) || self.vms.unsettled.contains_key(name) {
                continue; // leaving: see `poll_migrations`
            }
            // Migrated here: left alone while its volumes are proxied here
            // or being handed over, and made this node's once they run here.
            if v.meta.owner == crate::vm::DomOwner::Migrating {
                let vols = &self.shared.volumes;
                let incoming = v.meta.volumes.iter().all(|x| vols.has(x));
                let arrived = v.meta.volumes.iter().all(|x| vols.writes(x));
                if arrived {
                    self.arrived(vm, v).await;
                    continue;
                }
                if incoming {
                    continue;
                }
            }
            // Running without this process holding its disks, or started
            // under an earlier lease (an orphan of an earlier raidish here:
            // its NBD connections go nowhere, its writes would be refused).
            let vols = &self.shared.volumes;
            let current = vols
                .owned(&v.meta.anchor, |o| v.meta.owner.is(&o.lease.owner))
                .unwrap_or(false);
            if !current || !v.meta.volumes.iter().all(|x| vols.writes(x)) {
                eprintln!(
                    "agent: VM '{}' runs here without this node's hold on its volumes \
                     (an orphan); destroying it",
                    name
                );
                if let Err(e) = vm.virsh.destroy(name).await {
                    eprintln!("agent: {}", e);
                }
                self.events.warn(
                    EventKind::VmOrphan,
                    Subject::vm(name),
                    format!(
                        "VM '{}' was running on {} without its volumes held there; destroyed it",
                        name, self.node
                    ),
                );
                self.vms.stopping.remove(name);
                continue;
            }
            let wanted = p.state.vms.contains_key(name.as_str())
                && !vm.restarting(name)
                && self
                    .vms
                    .specs
                    .get(name)
                    .is_some_and(|s| s.usable() && p.anchor_here(s));
            if wanted {
                self.vms.stopping.remove(name);
                continue;
            }
            // Asked to move: live if it can be, instead of shutting down.
            if !self.vms.stopping.contains_key(name) && self.start_migration(vm, v, p) {
                continue;
            }
            match self.vms.stopping.get(name) {
                None => {
                    println!("agent: shutting VM '{}' down", name);
                    self.events.info(
                        EventKind::VmStopping,
                        Subject::vm(name),
                        format!("VM '{}' shutting down on {}", name, self.node),
                    );
                    if let Err(e) = vm.virsh.shutdown(name).await {
                        eprintln!("agent: {}", e);
                    }
                    self.vms
                        .stopping
                        .insert(name.clone(), std::time::Instant::now());
                }
                Some(t) if t.elapsed() > vm.shutdown_timeout => {
                    eprintln!(
                        "agent: VM '{}' didn't shut down in {:?}; destroying it",
                        name, vm.shutdown_timeout
                    );
                    self.events.warn(
                        EventKind::VmForcedOff,
                        Subject::vm(name),
                        format!(
                            "VM '{}' didn't shut down in {:?}; forced it off",
                            name, vm.shutdown_timeout
                        ),
                    );
                    if let Err(e) = vm.virsh.destroy(name).await {
                        eprintln!("agent: {}", e);
                    }
                }
                Some(_) => {}
            }
        }
        let up = |n: &str| ours.iter().any(|v| v.name.as_str() == n);
        for n in self.vms.stopping.keys().filter(|n| !up(n)) {
            self.events.info(
                EventKind::VmStopped,
                Subject::vm(n),
                format!("VM '{}' stopped on {}", n, self.node),
            );
        }
        self.vms.stopping.retain(|n, _| up(n));
        // Restarts are done once the old domain is gone (it's started again
        // by `start_wanted_vms`, from the current XML).
        vm.restarts_done(up);
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
            if !p.state.vms.contains_key(spec.name.as_str()) || !p.anchor_here(spec) {
                continue;
            }
            // Leaving, or just left, by migration: it runs elsewhere even
            // where its domain is already gone from here.
            // Or a failed migration's guest may run elsewhere (unknown yet).
            if self.vms.migrations.contains_key(&spec.name)
                || self.vms.migrated.contains_key(&spec.name)
                || self.vms.unsettled.contains_key(&spec.name)
            {
                continue;
            }
            if let Some(e) = spec.error() {
                self.events.once(
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
                    self.events.once(
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
            if !spec.volumes().iter().all(|v| self.shared.volumes.writes(v)) {
                continue; // its disks aren't all here yet
            }
            let Some(owner) = self.shared.volumes.owned(anchor, |o| o.lease.owner.clone()) else {
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
                    println!("agent: started VM '{}' here", spec.name);
                    self.events.clear(&Subject::vm(&spec.name));
                    self.events.info(
                        EventKind::VmStarted,
                        Subject::vm(&spec.name),
                        format!("VM '{}' started on {}", spec.name, self.node),
                    );
                }
                Err(e) => {
                    eprintln!("agent: starting VM '{}': {}", spec.name, e);
                    self.events.once(
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
            eprintln!(
                "agent: destroying VM '{}' (its volume lease is gone)",
                d.name
            );
            self.events.error(
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
            self.vms.stopping.remove(d.name.as_str());
        }
    }

    /// Migrate VM `v` live to where its anchor is asked to move, if it can
    /// be (see `migrate`): true if a migration started (or the VM is
    /// already leaving), false to move it cold.
    pub(crate) fn start_migration(
        &mut self,
        vm: &Arc<crate::vm::VmRuntime>,
        v: &crate::vm::LocalVm,
        p: &Placement,
    ) -> bool {
        let name = &v.name;
        if self.peer.is_none() || v.state != crate::vm::DomState::Running {
            return false;
        }
        let Some(spec) = self.vms.specs.get(name).filter(|s| s.usable()) else {
            return false;
        };
        let Some((anchor_name, anchor)) = spec.anchor().and_then(|a| Some((a, p.volume(a)?)))
        else {
            return false;
        };
        // An explicit move to a live node, the VM still wanted running
        // (not stopped, not restarted), all its disks here.
        let Some(to) = anchor.props.move_to.clone() else {
            return false;
        };
        if to == self.node
            || !p.live.contains(&to)
            || p.target(anchor_name, &anchor.props).as_deref() != Some(to.as_str())
            || !p.state.vms.contains_key(name.as_str())
            || vm.restarting(name)
            || !spec.volumes().iter().all(|x| self.shared.volumes.writes(x))
            || self.vms.migrate_failed.get(name) == Some(&to)
        {
            return false;
        }
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
        self.vms.migrations.insert(
            name.clone(),
            migrate::Migration {
                to,
                volumes: spec.volumes().to_vec(),
                started: std::time::Instant::now(),
                task: tokio::spawn(job.run()),
            },
        );
        true
    }

    /// Finish what migrations from here did (see `migrate`): hand the
    /// volumes of a VM that left to where it went, or put a failed one's
    /// back, to move cold.
    pub(crate) async fn poll_migrations(&mut self) {
        let done: Vec<crate::types::VmName> = self
            .vms
            .migrations
            .iter()
            .filter(|(_, m)| m.task.is_finished())
            .map(|(n, _)| n.clone())
            .collect();
        for name in done {
            let m = self.vms.migrations.remove(&name).unwrap();
            let result = match m.task.await {
                Ok(r) => r,
                Err(e) => Err(migrate::Failure::Failed(e.to_string())),
            };
            // A failure is noted once it's settled (`settle_failed`).
            if result.is_ok() {
                self.facts.note_migration(&m.to, &Ok(()));
            }
            match result {
                Ok(()) => {
                    println!(
                        "agent: migrated VM '{}' to {} in {:?}",
                        name,
                        m.to,
                        m.started.elapsed()
                    );
                    self.events.info(
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
                    self.vms.migrated.insert(
                        name,
                        migrate::Migrated {
                            to: m.to,
                            volumes: m.volumes,
                            detached: false,
                        },
                    );
                }
                Err(migrate::Failure::NotLive(why)) => {
                    println!(
                        "agent: VM '{}' can't move to {} live: {} (moving it cold)",
                        name, m.to, why
                    );
                    self.events.info(
                        EventKind::VmMovingCold,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' moves cold from {} to {}: {}",
                            name, self.node, m.to, why
                        ),
                    );
                    self.vms.migrate_failed.insert(name, m.to);
                }
                Err(migrate::Failure::Failed(e)) => {
                    self.vms.unsettled.insert(
                        name,
                        migrate::Unsettled {
                            to: m.to,
                            volumes: m.volumes,
                            why: e,
                            told: false,
                        },
                    );
                }
            }
        }
        self.settle_failed().await;
        // Migrated: detach here (so each volume is handed over), then, once
        // none runs here, there too (back to their own placement).
        let names: Vec<crate::types::VmName> = self.vms.migrated.keys().cloned().collect();
        for name in names {
            let mut m = self.vms.migrated.remove(&name).unwrap();
            if !m.detached {
                let mut ok = true;
                for v in &m.volumes {
                    if let Err(e) = migrate::attach(&self.leases, v, &self.node, false).await {
                        eprintln!("agent: detaching '{}' from {}: {}", v, self.node, e);
                        ok = false;
                    }
                }
                m.detached = ok;
                self.vms.migrated.insert(name, m);
                continue;
            }
            if m.volumes.iter().any(|v| self.shared.volumes.writes(v)) {
                self.vms.migrated.insert(name, m);
                continue;
            }
            let mut ok = true;
            for v in &m.volumes {
                if let Err(e) = migrate::attach(&self.leases, v, &m.to, false).await {
                    eprintln!("agent: detaching '{}' from {}: {}", v, m.to, e);
                    ok = false;
                }
            }
            if !ok {
                self.vms.migrated.insert(name, m);
            }
        }
    }

    /// Failed live migrations: where does the guest run now? It may have
    /// switched over before the failure (`migrate::Whereabouts`).
    pub(crate) async fn settle_failed(&mut self) {
        let names: Vec<crate::types::VmName> = self.vms.unsettled.keys().cloned().collect();
        for name in names {
            let mut u = self.vms.unsettled.remove(&name).unwrap();
            let here = match &self.vm {
                Some(vm) => vm.virsh.list().await.ok().map(|l| {
                    l.iter()
                        .any(|d| d.name.as_str() == name.as_str() && d.id.is_some())
                }),
                None => Some(false),
            };
            // B only matters if it isn't here.
            let there = if here == Some(false) {
                self.ask_runs_there(&u.to, &name).await
            } else {
                None
            };
            match migrate::whereabouts(here, there) {
                migrate::Whereabouts::Unknown => {
                    if !u.told {
                        u.told = true;
                        eprintln!(
                            "agent: migrating VM '{}' to {} failed ({}) and it isn't here: \
                             waiting for {} to say whether it runs there",
                            name, u.to, u.why, u.to
                        );
                        self.events.warn(
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
                    self.vms.unsettled.insert(name, u);
                }
                migrate::Whereabouts::There => {
                    println!(
                        "agent: migrating VM '{}' to {} failed ({}) but it runs there: \
                         handing its volumes over",
                        name, u.to, u.why
                    );
                    self.facts.note_migration(&u.to, &Ok(()));
                    self.events.info(
                        EventKind::VmMigrated,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' migrated live from {} to {} (though {} said it failed: {})",
                            name, self.node, u.to, self.node, u.why
                        ),
                    );
                    self.vms.migrated.insert(
                        name,
                        migrate::Migrated {
                            to: u.to,
                            volumes: u.volumes,
                            detached: false,
                        },
                    );
                }
                w @ (migrate::Whereabouts::Here | migrate::Whereabouts::Neither) => {
                    let at = if w == migrate::Whereabouts::Here {
                        format!("it still runs on {}", self.node)
                    } else {
                        "it runs nowhere".to_string()
                    };
                    eprintln!(
                        "agent: migrating VM '{}' to {} failed: {}; {} (moving it cold)",
                        name, u.to, u.why, at
                    );
                    self.facts.note_migration(&u.to, &Err(u.why.clone()));
                    self.events.warn(
                        EventKind::VmMigrateFailed,
                        Subject::vm(&name),
                        format!(
                            "VM '{}' couldn't migrate live from {} to {} ({}); {}; moving it cold",
                            name, self.node, u.to, u.why, at
                        ),
                    );
                    for v in &u.volumes {
                        for node in [u.to.as_str(), self.node.as_str()] {
                            if let Err(e) = migrate::attach(&self.leases, v, node, false).await {
                                eprintln!("agent: detaching '{}' from {}: {}", v, node, e);
                            }
                        }
                    }
                    self.vms.migrate_failed.insert(name, u.to);
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
        let Some(owner) = self
            .shared
            .volumes
            .owned(&v.meta.anchor, |o| o.lease.owner.clone())
        else {
            return;
        };
        let meta = crate::vm::DomMeta {
            owner: crate::vm::DomOwner::Lease(owner),
            node: self.node.clone(),
            ..v.meta.clone()
        };
        match vm.virsh.set_meta(name, &meta).await {
            Ok(()) => {
                println!("agent: VM '{}' migrated here", name);
                self.events.info(
                    EventKind::VmArrived,
                    Subject::vm(name),
                    format!("VM '{}' now runs on {} (migrated)", name, self.node),
                );
            }
            Err(e) => eprintln!("agent: taking migrated VM '{}' on: {}", name, e),
        }
    }
}
