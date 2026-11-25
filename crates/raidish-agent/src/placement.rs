//! Where each volume belongs: the placement rules (`place`, `place_on`,
//! `place_attached`) and one tick's look at the cluster (`Placement`).

use crate::Agent;
use crate::inode::{DatasetProps, Transport};
use crate::types::DatasetName;
use crate::volume::VolumeInfo;
use std::collections::{BTreeMap, BTreeSet};

/// Which node should run a volume, given who runs it now (`holder`) and
/// which nodes are live. None: nowhere.
pub fn place(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
) -> Option<String> {
    props.automount?;
    place_on(props, holder, live)
}

/// Which node should write a volume attached to `attached` (see
/// `ClusterState::attached`; empty: not attached anywhere in particular,
/// so `place`). The writer is always one of the attached nodes: the one
/// writing it now while it stays attached and live (moving it would mean a
/// handoff for nothing), else the one it's asked to move to, else the
/// first live one.
pub fn place_attached(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
    attached: &[String],
) -> Option<String> {
    if attached.is_empty() {
        return place(props, holder, live);
    }
    props.automount?;
    if !props.enabled {
        return None;
    }
    let ok = |n: &str| live.contains(n) && attached.iter().any(|a| a == n);
    if let Some(m) = &props.move_to
        && ok(m)
    {
        return Some(m.clone());
    }
    if let Some(h) = holder
        && ok(h)
    {
        return Some(h.to_string());
    }
    attached.iter().find(|n| live.contains(*n)).cloned()
}

/// Which of the `live` nodes should run a dataset (enabled, by its node
/// list), given who runs it now.
pub fn place_on(
    props: &DatasetProps,
    holder: Option<&str>,
    live: &BTreeSet<String>,
) -> Option<String> {
    if !props.enabled {
        return None;
    }
    let allowed = |n: &str| props.nodes.is_empty() || props.nodes.iter().any(|x| x == n);
    if let Some(m) = &props.move_to
        && live.contains(m)
        && allowed(m)
    {
        return Some(m.clone());
    }
    if let Some(h) = holder
        && live.contains(h)
        && allowed(h)
    {
        return Some(h.to_string());
    }
    if props.nodes.is_empty() {
        // Any node: the lowest (node-2 before node-10).
        live.iter().min_by_key(|n| (n.len(), n.as_str())).cloned()
    } else {
        props.nodes.iter().find(|n| live.contains(*n)).cloned()
    }
}

/// Where one tick wants each volume, from one look at the cluster state.
pub(crate) struct Placement {
    /// This node.
    pub(crate) me: String,
    pub(crate) state: crate::cluster::state::ClusterState,
    pub(crate) now: u64,
    /// Nodes whose agents hold their `node/` lease.
    pub(crate) live: BTreeSet<String>,
    /// Peer I/O is on: volumes can be proxied and handed over.
    pub(crate) peer_on: bool,
    pub(crate) volumes: BTreeMap<DatasetName, VolumeInfo>,
    /// VM volumes: placed by their VM's anchor (its settings, over NBD, and
    /// its holder).
    pub(crate) vm_place: BTreeMap<DatasetName, (DatasetProps, Option<String>)>,
    /// Volumes of VMs migrating away from here: kept here until they're
    /// gone.
    pub(crate) migrating: BTreeSet<DatasetName>,
}

impl Placement {
    pub(crate) fn volume(&self, name: &DatasetName) -> Option<&VolumeInfo> {
        self.volumes.get(name)
    }

    /// The node whose lease on volume `name` is live, if any.
    pub(crate) fn holder(&self, name: &DatasetName) -> Option<String> {
        self.state.dataset_node(name, self.now).map(str::to_string)
    }

    /// The nodes volume `name` is attached to (only with peer I/O: without
    /// it there's no proxying, so no point).
    pub(crate) fn attached(&self, name: &DatasetName) -> &[String] {
        match self.peer_on {
            true => self
                .state
                .attached
                .get(name.as_str())
                .map_or(&[], |v| v.as_slice()),
            false => &[],
        }
    }

    /// Whether `name` is a VM's volume (exported for qemu, not attached).
    pub(crate) fn for_vm(&self, name: &DatasetName) -> bool {
        self.vm_place.contains_key(name)
    }

    /// The node volume `name` (with settings `own`) should run on.
    pub(crate) fn target(&self, name: &DatasetName, own: &DatasetProps) -> Option<String> {
        if self.migrating.contains(name) {
            return Some(self.me.clone());
        }
        let (props, holder) = match self.vm_place.get(name) {
            Some((p, h)) => (p, h.clone()),
            None => (own, self.holder(name)),
        };
        place_attached(props, holder.as_deref(), &self.live, self.attached(name))
    }

    /// How volume `name` (with settings `own`) is exported.
    pub(crate) fn transport(&self, name: &DatasetName, own: &DatasetProps) -> Option<Transport> {
        match self.vm_place.get(name) {
            Some((p, _)) => p.automount,
            None => own.automount,
        }
    }

    /// Whether VM `spec`'s volumes belong on this node.
    pub(crate) fn anchor_here(&self, spec: &crate::vm::VmSpec) -> bool {
        spec.anchor().is_some_and(|n| {
            self.volume(n)
                .is_some_and(|a| self.target(n, &a.props).as_deref() == Some(self.me.as_str()))
        })
    }
}

impl Agent {
    /// Where `volumes` belong, from one look at the cluster state.
    pub(crate) fn placement(&self, volumes: Vec<VolumeInfo>) -> Placement {
        let state = self.cluster.status().state;
        let now = crate::cluster::now_ms();
        let live = state.live_nodes(now).map(|(n, _)| n.to_string()).collect();
        let volumes: BTreeMap<DatasetName, VolumeInfo> =
            volumes.into_iter().map(|v| (v.name.clone(), v)).collect();
        // A VM's volumes are placed by its anchor's settings and holder, so
        // they all land on one node, exported over NBD for qemu (and not
        // attached to the host kernel).
        let mut vm_place = BTreeMap::new();
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
                    vm_place
                        .entry(v.clone())
                        .or_insert_with(|| (props.clone(), h.clone()));
                }
            }
        }
        // A migrating VM's volumes stay here until it's gone (see
        // `migrate`), whatever their placement says.
        let migrating = self
            .vms
            .migrations
            .values()
            .flat_map(|m| m.volumes.iter().cloned())
            .collect();
        Placement {
            me: self.node.clone(),
            state,
            now,
            live,
            peer_on: self.peer.is_some(),
            volumes,
            vm_place,
            migrating,
        }
    }
}
