//! The agent's decisions (`volumes::plan`, `vms::plan`) as tables: what a
//! tick does from what it sees, without a cluster, a pool or libvirt.

use crate::cluster::state::{ClusterState, Lease, MigrationRecord};
use crate::inode::{DatasetProps, Transport};
use crate::placement::Placement;
use crate::types::{DatasetName, Epoch, Holder, HolderRole, LeaseResource, VmName, VolumeInfo};
use crate::vm::{DomMeta, DomOwner, DomState, LocalVm, VmDisks, VmSpec};
use crate::vms::VmBook;
use crate::vms::plan::{VmAction, migration_target, plan_domain};
use crate::vms::state::{StopStep, Stopping, VmState};
use crate::volumes::VolumeView;
use crate::volumes::plan::{VolumeAction, plan_proxies, plan_volumes};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const ME: &str = "node-0";
const OWNER: &str = "agent:node-0";

fn dn(s: &str) -> DatasetName {
    s.parse().unwrap()
}

/// What the volume table here holds, as the plans see it.
#[derive(Default)]
struct Here {
    /// Written here: how it's exported, under which lease owner.
    written: BTreeMap<DatasetName, (Transport, String)>,
    proxied: BTreeMap<DatasetName, Transport>,
    /// Being handed to or from here.
    handing: BTreeSet<DatasetName>,
}

impl Here {
    fn writes(mut self, v: &str, t: Transport) -> Self {
        self.written.insert(dn(v), (t, OWNER.into()));
        self
    }
    fn proxies(mut self, v: &str, t: Transport) -> Self {
        self.proxied.insert(dn(v), t);
        self
    }
}

impl VolumeView for Here {
    fn has(&self, n: &DatasetName) -> bool {
        self.written.contains_key(n) || self.proxied.contains_key(n) || self.handing.contains(n)
    }
    fn writes(&self, n: &DatasetName) -> bool {
        self.written.contains_key(n)
    }
    fn owner_of(&self, n: &DatasetName) -> Option<String> {
        self.written.get(n).map(|(_, o)| o.clone())
    }
    fn running(&self) -> Vec<(DatasetName, Transport)> {
        self.written
            .iter()
            .map(|(n, (t, _))| (n.clone(), *t))
            .collect()
    }
    fn export_transport(&self, n: &DatasetName) -> Option<Transport> {
        self.written.get(n).map(|(t, _)| *t)
    }
    fn proxy_transport(&self, n: &DatasetName) -> Option<Transport> {
        self.proxied.get(n).copied()
    }
    fn proxied(&self) -> Vec<DatasetName> {
        self.proxied.keys().cloned().collect()
    }
}

/// A volume placed on `nodes` (first live wins), exported over NVMe.
fn vol(name: &str, nodes: &[&str]) -> VolumeInfo {
    VolumeInfo {
        name: dn(name),
        size_bytes: 1 << 20,
        volblocksize: 4096,
        props: DatasetProps {
            nodes: nodes.iter().map(|s| s.to_string()).collect(),
            automount: Some(Transport::Nvme),
            enabled: true,
            move_to: None,
            port: None,
        },
    }
}

/// This node's view: `live` nodes, these volumes, peer I/O on.
fn view(live: &[&str], vols: Vec<VolumeInfo>) -> Placement {
    Placement {
        me: ME.into(),
        state: ClusterState::default(),
        now: 1_000,
        live: live.iter().map(|s| s.to_string()).collect(),
        peer_on: true,
        volumes: vols.into_iter().map(|v| (v.name.clone(), v)).collect(),
        vm: Default::default(),
    }
}

/// `node`'s agent holds volume `v`'s lease.
fn held(p: &mut Placement, v: &str, node: &str) {
    p.state.leases.insert(
        LeaseResource::dataset(v),
        Lease {
            owner: format!("agent:{}", node),
            holder: Holder {
                role: HolderRole::Agent,
                node: Some(node.into()),
                ..Default::default()
            },
            epoch: Epoch::from(1),
            expires_ms: u64::MAX / 2,
            released: false,
        },
    );
}

fn attach(p: &mut Placement, v: &str, nodes: &[&str]) {
    p.state
        .attached
        .insert(v.into(), nodes.iter().map(|s| s.to_string()).collect());
}

// ---- volumes ----

#[test]
fn a_volume_that_belongs_here_and_runs_nowhere_is_started() {
    let p = view(&[ME, "node-1"], vec![vol("a", &[ME])]);
    assert_eq!(
        plan_volumes(&p, &Here::default()),
        vec![VolumeAction::Start {
            name: dn("a"),
            transport: Transport::Nvme,
            attach: true,
        }]
    );
    // Already written here: nothing to do.
    assert!(plan_volumes(&p, &Here::default().writes("a", Transport::Nvme)).is_empty());
    // Someone else still holds its lease: wait for them to let go.
    let mut p = p;
    held(&mut p, "a", "node-1");
    assert!(plan_volumes(&p, &Here::default()).is_empty());
}

#[test]
fn a_vms_volume_is_exported_not_attached() {
    let mut p = view(&[ME], vec![vol("a", &[ME])]);
    let nbd = DatasetProps {
        automount: Some(Transport::Nbd),
        ..p.volumes[&dn("a")].props.clone()
    };
    p.vm.place.insert(dn("a"), (nbd, None));
    assert_eq!(
        plan_volumes(&p, &Here::default()),
        vec![VolumeAction::Start {
            name: dn("a"),
            transport: Transport::Nbd,
            attach: false,
        }]
    );
}

#[test]
fn a_volume_going_elsewhere_is_handed_over_only_where_it_can_be() {
    let here = Here::default().writes("a", Transport::Nvme);
    // Placed on node-1, attached there too: handed over.
    let mut p = view(&[ME, "node-1"], vec![vol("a", &["node-1"])]);
    attach(&mut p, "a", &[ME, "node-1"]);
    p.state
        .attached
        .get_mut("a")
        .unwrap()
        .retain(|n| n == "node-1");
    let hand = vec![VolumeAction::HandOff {
        name: dn("a"),
        to: "node-1".into(),
    }];
    assert_eq!(plan_volumes(&p, &here), hand);
    // Asked to move there: handed over too.
    let mut q = view(&[ME, "node-1"], vec![vol("a", &[])]);
    q.volumes.get_mut(&dn("a")).unwrap().props.move_to = Some("node-1".into());
    assert_eq!(plan_volumes(&q, &here), hand);
    // Neither attached nor asked to move there: stopped (node-1 starts it).
    let r = view(&[ME, "node-1"], vec![vol("a", &["node-1"])]);
    assert_eq!(plan_volumes(&r, &here), vec![VolumeAction::Stop(dn("a"))]);
    // No peer I/O: stopped.
    p.peer_on = false;
    assert_eq!(plan_volumes(&p, &here), vec![VolumeAction::Stop(dn("a"))]);
    // Exported over another transport here than there: stopped.
    q.volumes.get_mut(&dn("a")).unwrap().props.automount = Some(Transport::Nbd);
    assert_eq!(plan_volumes(&q, &here), vec![VolumeAction::Stop(dn("a"))]);
}

#[test]
fn a_volume_is_stopped_when_gone_moved_away_or_reexported_unless_a_vm_pins_it() {
    let here = Here::default().writes("a", Transport::Nvme);
    // Deleted.
    let p = view(&[ME], vec![]);
    assert_eq!(plan_volumes(&p, &here), vec![VolumeAction::Stop(dn("a"))]);
    // Still here, but over another transport now: stopped (started over
    // it next tick, once its lease is let go).
    let mut q = view(&[ME], vec![vol("a", &[ME])]);
    q.volumes.get_mut(&dn("a")).unwrap().props.automount = Some(Transport::Nbd);
    assert_eq!(plan_volumes(&q, &here), vec![VolumeAction::Stop(dn("a"))]);
    // A VM here uses it: it stays, wherever it belongs.
    let mut r = view(&[ME, "node-1"], vec![vol("a", &["node-1"])]);
    r.vm.in_use.insert(dn("a"));
    assert!(plan_volumes(&r, &here).is_empty());
    // A VM's, and libvirt can't say whether one uses it: it stays too.
    let mut s = view(&[ME, "node-1"], vec![vol("a", &["node-1"])]);
    let props = s.volumes[&dn("a")].props.clone();
    s.vm.place.insert(dn("a"), (props, None));
    s.vm.libvirt_unknown = true;
    assert!(plan_volumes(&s, &here).is_empty());
}

#[test]
fn proxies_follow_attachment_writer_and_transport() {
    // Attached here and written on node-1: proxied here.
    let mut p = view(&[ME, "node-1"], vec![vol("a", &["node-1"])]);
    attach(&mut p, "a", &["node-1", ME]);
    let add = vec![VolumeAction::AddProxy {
        name: dn("a"),
        transport: Transport::Nvme,
        attach: true,
    }];
    // Nobody writes it yet: not yet (the kernel's reads would just wait).
    assert!(plan_proxies(&p, &Here::default()).is_empty());
    held(&mut p, "a", "node-1");
    assert_eq!(plan_proxies(&p, &Here::default()), add);
    // Already proxied, or written here: nothing to add.
    let proxied = Here::default().proxies("a", Transport::Nvme);
    assert!(plan_proxies(&p, &proxied).is_empty());
    assert!(plan_proxies(&p, &Here::default().writes("a", Transport::Nvme)).is_empty());
    // Now over another transport: removed (made again next tick).
    let nbd = Here::default().proxies("a", Transport::Nbd);
    assert_eq!(
        plan_proxies(&p, &nbd),
        vec![VolumeAction::RemoveProxy(dn("a"))]
    );
    // No longer attached here: removed.
    attach(&mut p, "a", &["node-1"]);
    assert_eq!(
        plan_proxies(&p, &proxied),
        vec![VolumeAction::RemoveProxy(dn("a"))]
    );
    // Deleted: removed.
    let q = view(&[ME, "node-1"], vec![]);
    assert_eq!(
        plan_proxies(&q, &proxied),
        vec![VolumeAction::RemoveProxy(dn("a"))]
    );
}

// ---- VMs ----

fn spec(name: &str, vols: &[&str]) -> VmSpec {
    VmSpec {
        name: vmn(name),
        change: 1,
        disks: VmDisks::of(vols.iter().map(|v| dn(v)).collect()),
        hw: Default::default(),
        xml: String::new(),
    }
}

/// Domain `name` running here on `vols`, started under `owner`.
fn dom(name: &str, vols: &[&str], owner: DomOwner) -> LocalVm {
    LocalVm {
        name: vmn(name),
        id: Some(1),
        state: DomState::Running,
        meta: DomMeta {
            anchor: dn(vols[0]),
            volumes: vols.iter().map(|v| dn(v)).collect(),
            change: 1,
            owner,
            pool: String::new(),
            node: ME.into(),
        },
        vcpus: None,
        memory: None,
        cpu_s: None,
        vnc_port: None,
    }
}

fn ours(name: &str, vols: &[&str]) -> LocalVm {
    dom(name, vols, DomOwner::Lease(OWNER.into()))
}

/// VM `vm` defined (on `vols`) and wanted running; its volumes placed
/// on `nodes`, written here.
fn vm_setup(vm: &str, vols: &[&str], nodes: &[&str]) -> (Placement, VmBook, Here) {
    let mut p = view(
        &[ME, "node-1"],
        vols.iter().map(|v| vol(v, nodes)).collect(),
    );
    p.state.vms.insert(vm.into(), 0);
    let mut book = VmBook::default();
    book.specs.insert(vmn(vm), spec(vm, vols));
    let mut here = Here::default();
    for v in vols {
        here = here.writes(v, Transport::Nbd);
    }
    (p, book, here)
}

const T: Duration = Duration::from_secs(60);

fn plan(p: &Placement, book: &VmBook, here: &Here, v: &LocalVm) -> Vec<VmAction> {
    plan_domain(p, book, here, v, false, T, Instant::now())
}

fn vmn(s: &str) -> VmName {
    s.parse().unwrap()
}

#[test]
fn a_vm_that_belongs_here_is_kept() {
    let (p, book, here) = vm_setup("web", &["w0", "w1"], &[ME]);
    assert_eq!(
        plan(&p, &book, &here, &ours("web", &["w0", "w1"])),
        vec![VmAction::Keep(vmn("web"))]
    );
}

#[test]
fn a_vm_without_its_volumes_held_here_is_an_orphan() {
    let (p, book, here) = vm_setup("web", &["w0", "w1"], &[ME]);
    let orphan = vec![VmAction::DestroyOrphan(vmn("web"))];
    // Started under an earlier lease (an earlier raidish here).
    let old = dom("web", &["w0", "w1"], DomOwner::Lease("agent:old".into()));
    assert_eq!(plan(&p, &book, &here, &old), orphan);
    // One of its disks isn't written here.
    let mut part = Here::default().writes("w0", Transport::Nbd);
    part.written.get_mut(&dn("w0")).unwrap().1 = OWNER.into();
    assert_eq!(plan(&p, &book, &part, &ours("web", &["w0", "w1"])), orphan);
}

#[test]
fn a_vm_migrating_here_waits_for_its_volumes_then_is_made_ours() {
    let (p, book, here) = vm_setup("web", &["w0"], &[ME]);
    let incoming = dom("web", &["w0"], DomOwner::Migrating);
    assert_eq!(
        plan(&p, &book, &here, &incoming),
        vec![VmAction::Arrived(vmn("web"))]
    );
    // Proxied here, or being handed over: left alone.
    let proxied = Here::default().proxies("w0", Transport::Nbd);
    assert!(plan(&p, &book, &proxied, &incoming).is_empty());
    // Neither: nothing of it here, so it's an orphan.
    assert_eq!(
        plan(&p, &book, &Here::default(), &incoming),
        vec![VmAction::DestroyOrphan(vmn("web"))]
    );
}

#[test]
fn a_vm_leaving_by_migration_is_left_alone() {
    let (p, mut book, here) = vm_setup("web", &["w0"], &["node-1"]);
    book.states.insert(
        vmn("web"),
        VmState::HandingOffVolumes(crate::vms::migrate::Migrated {
            to: "node-1".into(),
            volumes: vec![dn("w0")],
            detached: false,
        }),
    );
    assert!(plan(&p, &book, &here, &ours("web", &["w0"])).is_empty());
}

#[test]
fn a_vm_that_doesnt_belong_here_is_shut_down_then_forced_off() {
    let (p, mut book, here) = vm_setup("web", &["w0"], &["node-1"]);
    let v = ours("web", &["w0"]);
    let now = Instant::now();
    let step = |book: &VmBook| match plan_domain(&p, book, &here, &v, false, T, now).as_slice() {
        [
            VmAction::Unwanted {
                migrate_to, stop, ..
            },
        ] => (migrate_to.clone(), *stop),
        other => panic!("{:?}", other),
    };
    // Not asked to move: no migration, the shutdown is sent.
    assert_eq!(step(&book), (None, StopStep::Shutdown));
    let sent = |at: Instant| {
        VmState::Stopping(Stopping {
            shutdown_sent: Some(at),
            cold_to: None,
        })
    };
    book.states.insert(vmn("web"), sent(now));
    assert_eq!(step(&book), (None, StopStep::Wait));
    book.states
        .insert(vmn("web"), sent(now - T - Duration::from_secs(1)));
    assert_eq!(step(&book), (None, StopStep::ForceOff));
    // Stopped (not wanted any more): the same.
    let mut q = p.clone();
    q.state.vms.clear();
    book.states.clear();
    assert!(matches!(
        plan_domain(&q, &book, &here, &v, false, T, now).as_slice(),
        [VmAction::Unwanted {
            migrate_to: None,
            stop: StopStep::Shutdown,
            ..
        }]
    ));
    // Restarting: shut down (it's started again from its new XML).
    let (p, book, here) = vm_setup("web", &["w0"], &[ME]);
    assert!(matches!(
        plan_domain(&p, &book, &here, &v, true, T, now).as_slice(),
        [VmAction::Unwanted {
            migrate_to: None,
            stop: StopStep::Shutdown,
            ..
        }]
    ));
}

/// `vm_setup` with `web`'s anchor asked to move to node-1, which its
/// placement agrees with.
fn moving() -> (Placement, VmBook, Here) {
    let (mut p, book, here) = vm_setup("web", &["w0", "w1"], &[]);
    for v in ["w0", "w1"] {
        p.volumes.get_mut(&dn(v)).unwrap().props.move_to = Some("node-1".into());
    }
    (p, book, here)
}

#[test]
fn a_vm_asked_to_move_migrates_live_when_it_can() {
    let (p, book, here) = moving();
    let v = ours("web", &["w0", "w1"]);
    assert_eq!(
        plan(&p, &book, &here, &v),
        vec![VmAction::Unwanted {
            vm: vmn("web"),
            migrate_to: Some("node-1".into()),
            stop: StopStep::Shutdown,
        }]
    );
    let to = |p: &Placement, book: &VmBook, here: &Here, v: &LocalVm, cold: Option<&str>| {
        migration_target(p, book, here, v, false, cold)
    };
    assert_eq!(to(&p, &book, &here, &v, None).as_deref(), Some("node-1"));
    // Not live to a node it already failed to move to live.
    assert_eq!(to(&p, &book, &here, &v, Some("node-1")), None);
    assert!(matches!(
        plan(
            &p,
            &{
                VmBook {
                    specs: book.specs.clone(),
                    states: [(
                        vmn("web"),
                        VmState::Stopping(Stopping::cold("node-1".into())),
                    )]
                    .into(),
                }
            },
            &here,
            &v
        )
        .as_slice(),
        [VmAction::Unwanted {
            migrate_to: None,
            stop: StopStep::Shutdown,
            ..
        }]
    ));
    // Restarting, or stopped: not migrated.
    assert_eq!(migration_target(&p, &book, &here, &v, true, None), None);
    let mut q = p.clone();
    q.state.vms.clear();
    assert_eq!(to(&q, &book, &here, &v, None), None);
    // Not all its disks written here.
    let part = Here::default().writes("w0", Transport::Nbd);
    assert_eq!(to(&p, &book, &part, &v, None), None);
    // Paused, not running.
    let mut paused = v.clone();
    paused.state = DomState::Paused;
    assert_eq!(to(&p, &book, &here, &paused, None), None);
    // No peer I/O.
    let mut r = p.clone();
    r.peer_on = false;
    assert_eq!(to(&r, &book, &here, &v, None), None);
    // The target isn't live.
    let mut s = p.clone();
    s.live.remove("node-1");
    assert_eq!(to(&s, &book, &here, &v, None), None);
}

#[test]
fn a_running_vms_leaked_attachments_are_detached_unless_it_migrates() {
    let (mut p, book, here) = vm_setup("web", &["w0", "w1"], &[ME]);
    attach(&mut p, "w1", &[ME, "node-1"]);
    let v = ours("web", &["w0", "w1"]);
    assert_eq!(
        plan(&p, &book, &here, &v),
        vec![
            VmAction::DetachLeaked(vmn("web"), vec![dn("w1")]),
            VmAction::Keep(vmn("web")),
        ]
    );
    p.state.migrations.insert(
        vmn("web"),
        MigrationRecord {
            from: "node-1".into(),
            to: ME.into(),
            volumes: vec![dn("w0"), dn("w1")],
            since_ms: 0,
        },
    );
    assert_eq!(plan(&p, &book, &here, &v), vec![VmAction::Keep(vmn("web"))]);
}
