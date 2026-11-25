//! What to do with the volumes here, decided from one tick's look at the
//! cluster (`Placement`) and the volume table (`VolumeView`): pure, so the
//! decisions are tested without a cluster. `Agent::act_on_volumes` carries
//! them out.

use super::VolumeView;
use crate::inode::Transport;
use crate::placement::Placement;
use crate::types::DatasetName;

/// One thing to do to a volume here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VolumeAction {
    /// Hand it to `to`, connections and all; if that can't be done, stop
    /// it here (the next node starts it).
    HandOff { name: DatasetName, to: String },
    /// Stop it here (the next node starts it).
    Stop(DatasetName),
    /// Run it here, exported over `transport`; `attach` its kernel device
    /// too (a VM's isn't: qemu is the NBD client).
    Start {
        name: DatasetName,
        transport: Transport,
        attach: bool,
    },
    /// Export it here, proxied to the node writing it.
    AddProxy {
        name: DatasetName,
        transport: Transport,
        attach: bool,
    },
    /// No longer export it here (written elsewhere).
    RemoveProxy(DatasetName),
}

/// The volumes written here that shouldn't be (handed over or stopped),
/// then those to start here.
///
/// A volume a VM here uses stays (`VmDemand::pinned`). One that goes to a
/// live node it's attached to (or asked to move to), over the transport
/// it's exported over here, is handed over; any other is stopped. One
/// stopped here this tick isn't started again until the next: its lease
/// is still ours in this view.
pub(crate) fn plan_volumes(p: &Placement, here: &dyn VolumeView) -> Vec<VolumeAction> {
    let mut out = Vec::new();
    for (name, t) in here.running() {
        let misplaced = match p.volume(&name) {
            None => true,
            Some(v) => {
                p.target(&name, &v.props).as_deref() != Some(p.me.as_str())
                    || p.transport(&name, &v.props) != Some(t)
            }
        };
        if !misplaced || p.vm.pinned(&name) {
            continue;
        }
        let hand_to = p.volume(&name).and_then(|v| {
            let to = p.target(&name, &v.props)?;
            let ok = p.peer_on
                && to != p.me
                && p.live.contains(&to)
                && p.transport(&name, &v.props) == here.export_transport(&name)
                && (p.attached(&name).contains(&to)
                    || v.props.move_to.as_deref() == Some(to.as_str()));
            ok.then_some(to)
        });
        out.push(match hand_to {
            Some(to) => VolumeAction::HandOff { name, to },
            None => VolumeAction::Stop(name),
        });
    }
    for v in p.volumes.values() {
        if here.writes(&v.name)
            || p.target(&v.name, &v.props).as_deref() != Some(p.me.as_str())
            // Someone still has it: wait for them to let go.
            || p.state.dataset_held(&v.name, p.now)
        {
            continue;
        }
        let Some(transport) = p.transport(&v.name, &v.props) else {
            continue;
        };
        out.push(VolumeAction::Start {
            name: v.name.clone(),
            transport,
            attach: !p.vm.for_vm(&v.name),
        });
    }
    out
}

/// The exports of volumes attached here and written elsewhere: added once
/// another node writes the volume (proxying to nobody, the kernel's first
/// reads would just wait), removed once it's no longer attached here, is
/// now over another transport (made again next tick), or is gone.
pub(crate) fn plan_proxies(p: &Placement, here: &dyn VolumeView) -> Vec<VolumeAction> {
    let mut out = Vec::new();
    for v in p.volumes.values() {
        let attached = p.attached(&v.name).contains(&p.me);
        let proxied = here.proxy_transport(&v.name);
        // Anything else there: written here, or being handed over.
        let slot = here.has(&v.name);
        let elsewhere = p.holder(&v.name).is_some_and(|h| h != p.me);
        let transport = p.transport(&v.name, &v.props);
        if attached && !slot && elsewhere {
            if let Some(transport) = transport {
                out.push(VolumeAction::AddProxy {
                    name: v.name.clone(),
                    transport,
                    attach: !p.vm.for_vm(&v.name),
                });
            }
        } else if proxied.is_some() && (!attached || proxied != transport) {
            out.push(VolumeAction::RemoveProxy(v.name.clone()));
        }
    }
    for name in here.proxied() {
        if p.volume(&name).is_none() {
            out.push(VolumeAction::RemoveProxy(name));
        }
    }
    out
}
