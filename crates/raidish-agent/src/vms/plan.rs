//! What to do with each domain of ours running here, decided from one
//! tick's look at the cluster (`Placement`), the VM bookkeeping (`VmBook`)
//! and the volumes here (`VolumeView`): pure, so the decisions are tested
//! without libvirt. `Agent::stop_unwanted_vms` carries them out.

use super::state::{self, StopStep};
use super::{VmBook, anchor_here};
use crate::placement::Placement;
use crate::types::{DatasetName, VmName};
use crate::vm::{DomOwner, DomState, LocalVm};
use crate::volumes::VolumeView;
use std::time::{Duration, Instant};

/// One thing to do about a domain of ours here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VmAction {
    /// Migrated here, and its volumes run here now: make it this node's
    /// (its metadata names this node's lease owner).
    Arrived(VmName),
    /// Running without this node holding its disks, or under an earlier
    /// lease (an orphan of an earlier raidish here: its NBD connections go
    /// nowhere, its writes would be refused): destroy it.
    DestroyOrphan(VmName),
    /// Its volumes are attached elsewhere with no migration of it recorded
    /// (`state::leaked`): back to their own placement.
    DetachLeaked(VmName, Vec<DatasetName>),
    /// It belongs here: whatever stop it had is over.
    Keep(VmName),
    /// It doesn't belong here: live to `migrate_to` if that's set (and the
    /// migration can be started), else `stop`.
    Unwanted {
        vm: VmName,
        migrate_to: Option<String>,
        stop: StopStep,
    },
}

/// What one domain of ours (`v`) needs. Nothing while it's leaving by
/// migration, or while one that migrated here waits for its volumes.
/// `restarting`: a restart was asked for (it's shut down, then started
/// again); `now` and `shutdown_timeout`: for the stop step.
pub(crate) fn plan_domain(
    p: &Placement,
    book: &VmBook,
    here: &dyn VolumeView,
    v: &LocalVm,
    restarting: bool,
    shutdown_timeout: Duration,
    now: Instant,
) -> Vec<VmAction> {
    let name = &v.name;
    if book.leaving(name) {
        return Vec::new(); // see `poll_migrations`
    }
    // Migrated here: left alone while its volumes are proxied here or
    // being handed over, and made this node's once they run here.
    if v.meta.owner == DomOwner::Migrating {
        if v.meta.volumes.iter().all(|x| here.writes(x)) {
            return vec![VmAction::Arrived(name.clone())];
        }
        if v.meta.volumes.iter().all(|x| here.has(x)) {
            return Vec::new();
        }
    }
    let current = here
        .owner_of(&v.meta.anchor)
        .is_some_and(|o| v.meta.owner.is(&o));
    if !current || !v.meta.volumes.iter().all(|x| here.writes(x)) {
        return vec![VmAction::DestroyOrphan(name.clone())];
    }
    let mut out = Vec::new();
    let recorded = p.state.migrations.contains_key(name);
    let leaked = state::leaked(&v.meta.volumes, recorded, |x| !p.attached(x).is_empty());
    if !leaked.is_empty() {
        out.push(VmAction::DetachLeaked(name.clone(), leaked));
    }
    let wanted = p.state.vms.contains_key(name.as_str())
        && !restarting
        && book
            .specs
            .get(name)
            .is_some_and(|s| s.usable() && anchor_here(p, s));
    if wanted {
        out.push(VmAction::Keep(name.clone()));
        return out;
    }
    let stop = book.stopping(name).cloned().unwrap_or_default();
    // Asked to move: live if it can be, instead of shutting down (not once
    // the shutdown is sent).
    let migrate_to = match stop.shutdown_sent {
        None => migration_target(p, book, here, v, restarting, stop.cold_to.as_deref()),
        Some(_) => None,
    };
    out.push(VmAction::Unwanted {
        vm: name.clone(),
        migrate_to,
        stop: state::stop_step(stop.shutdown_sent, now, shutdown_timeout),
    });
    out
}

/// Where VM `v` should migrate live to, if it should: an explicit move of
/// its anchor (`move_to`) to a live node that its placement agrees with,
/// the VM still wanted running (not stopped, not `restarting`), peer I/O
/// on, all its disks written here, and not a node it already failed to
/// move to live (`cold_to`).
pub(crate) fn migration_target(
    p: &Placement,
    book: &VmBook,
    here: &dyn VolumeView,
    v: &LocalVm,
    restarting: bool,
    cold_to: Option<&str>,
) -> Option<String> {
    if !p.peer_on || v.state != DomState::Running {
        return None;
    }
    let spec = book.specs.get(&v.name).filter(|s| s.usable())?;
    let anchor_name = spec.anchor()?;
    let anchor = p.volume(anchor_name)?;
    let to = anchor.props.move_to.clone()?;
    let refused = to == p.me
        || !p.live.contains(&to)
        || p.target(anchor_name, &anchor.props).as_deref() != Some(to.as_str())
        || !p.state.vms.contains_key(v.name.as_str())
        || restarting
        || !spec.volumes().iter().all(|x| here.writes(x))
        || cold_to == Some(to.as_str());
    (!refused).then_some(to)
}
