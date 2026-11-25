//! What this node is doing with each VM, beyond what libvirt says: one
//! [`VmState`] per VM, and the transitions between them as pure functions.
//!
//! A VM with no state is running, or not, as libvirt says; that isn't kept
//! here. The states and how a VM goes between them:
//!
//! ```text
//!   (running) ── asked to move, can be live ──▶ Migrating
//!   (running) ── not wanted here ─────────────▶ Stopping
//!   Migrating ── Ok ──────────────────────────▶ HandingOffVolumes
//!   Migrating ── NotLive ─────────────────────▶ Stopping (cold_to)
//!   Migrating ── Failed ──────────────────────▶ Unsettled
//!   Unsettled ── There ───────────────────────▶ HandingOffVolumes
//!   Unsettled ── Here / Neither ──────────────▶ Stopping (cold_to)
//!   Unsettled ── Unknown ─────────────────────▶ Unsettled (told)
//!   HandingOffVolumes ── both detached ───────▶ (none: it runs there)
//!   Stopping ── its domain is gone, or it's wanted here again ─▶ (none)
//! ```
//!
//! What survives a restart is the migration's record in the cluster state
//! (`ClusterState::migrations`, written before the volumes are attached to
//! both nodes, cleared once they're detached): [`recovery`] says who
//! finishes a migration nobody runs any more.

use super::migrate::{Failure, Migrated, Migration, Unsettled, Whereabouts};
use crate::cluster::state::MigrationRecord;
use crate::types::DatasetName;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// What this node is doing with one VM.
pub(crate) enum VmState {
    /// Shutting it down here (see [`Stopping`]).
    Stopping(Stopping),
    /// Migrating it live from here: the job runs (see `migrate`).
    Migrating(Migration),
    /// Migrated away: its volumes are being handed to where it went.
    HandingOffVolumes(Migrated),
    /// A live migration that failed, until where its guest runs is known.
    Unsettled(Unsettled),
}

impl VmState {
    /// Whether the VM is leaving, or has just left, by migration: its
    /// domain is left alone here and it isn't started here.
    pub(crate) fn leaving(&self) -> bool {
        !matches!(self, VmState::Stopping(_))
    }
}

/// A VM to shut down here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Stopping {
    /// When `virsh shutdown` was sent; None: it's still to be sent. It's
    /// forced off (`virsh destroy`) once `shutdown_timeout` has passed.
    pub(crate) shutdown_sent: Option<Instant>,
    /// It couldn't move live to this node (it was checked, or tried and
    /// it failed), so it moves there cold: it isn't tried live again while
    /// its anchor is asked to move there ([`keep_cold_to`]).
    pub(crate) cold_to: Option<String>,
}

impl Stopping {
    /// Moving cold to `to`: nothing sent yet.
    pub(crate) fn cold(to: String) -> Self {
        Stopping {
            shutdown_sent: None,
            cold_to: Some(to),
        }
    }

    /// Nothing sent and nothing to remember: the same as no state.
    pub(crate) fn idle(&self) -> bool {
        self.shutdown_sent.is_none() && self.cold_to.is_none()
    }
}

/// What a stop pass does with a VM that shouldn't run here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopStep {
    /// Send `virsh shutdown`.
    Shutdown,
    /// It had `timeout` to shut down: force it off.
    ForceOff,
    /// Give it time.
    Wait,
}

/// The stop pass for a VM whose shutdown was sent at `sent` (None: not
/// yet), at `now`.
pub(crate) fn stop_step(sent: Option<Instant>, now: Instant, timeout: Duration) -> StopStep {
    match sent {
        None => StopStep::Shutdown,
        Some(t) if now.saturating_duration_since(t) > timeout => StopStep::ForceOff,
        Some(_) => StopStep::Wait,
    }
}

/// `cold_to`, while the anchor is still asked to move there (`move_to`):
/// once it's asked to move somewhere else, or nowhere, live is tried again.
pub(crate) fn keep_cold_to(cold_to: Option<String>, move_to: Option<&str>) -> Option<String> {
    cold_to.filter(|to| move_to == Some(to.as_str()))
}

/// A migration to `to` (of `volumes`) finished with `result`.
pub(crate) fn finished(
    to: String,
    volumes: Vec<DatasetName>,
    result: Result<(), Failure>,
) -> VmState {
    match result {
        Ok(()) => VmState::HandingOffVolumes(Migrated {
            to,
            volumes,
            detached: false,
        }),
        Err(Failure::NotLive(_)) => VmState::Stopping(Stopping::cold(to)),
        Err(Failure::Failed(why)) => VmState::Unsettled(Unsettled {
            to,
            volumes,
            why,
            told: false,
            recovered: false,
        }),
    }
}

/// Failed migration `u`'s guest was found at `w`. Unknown: it stays
/// unsettled (`told`: the operator was told). Here or nowhere: it moves
/// cold (its volumes detached from both nodes first, by the caller).
pub(crate) fn settled(u: Unsettled, w: Whereabouts) -> VmState {
    match w {
        Whereabouts::Unknown => VmState::Unsettled(Unsettled { told: true, ..u }),
        Whereabouts::There => VmState::HandingOffVolumes(Migrated {
            to: u.to,
            volumes: u.volumes,
            detached: false,
        }),
        Whereabouts::Here | Whereabouts::Neither => VmState::Stopping(Stopping::cold(u.to)),
    }
}

/// What node `me` does about a recorded migration (`rec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// Nothing: it's under way, or it's someone else's to finish.
    Nothing,
    /// The source, with no job for it (it restarted): settle it again,
    /// as a failed migration (`Unsettled`): where does the guest run?
    Resettle,
    /// The target, its source gone (no live `node/` lease): finish it in
    /// the source's place ([`stand_in_step`]).
    StandIn,
}

/// Who finishes `rec`: its source when it holds no state for the VM
/// (`has_state`: a job, a handoff, a stop), its target only once the
/// source isn't `live`. Never the target while the source is live: it
/// can't tell an interrupted migration from one under way (the volumes
/// are attached to both before the guest moves).
pub(crate) fn recovery(
    rec: &MigrationRecord,
    me: &str,
    live: &BTreeSet<String>,
    has_state: bool,
) -> Recovery {
    if rec.from == me {
        match has_state {
            true => Recovery::Nothing,
            false => Recovery::Resettle,
        }
    } else if rec.to == me && !live.contains(&rec.from) {
        Recovery::StandIn
    } else {
        Recovery::Nothing
    }
}

/// The next step of finishing `rec` on its target (`me`) in place of its
/// gone source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StandInStep {
    /// Detach these volumes from `node`.
    Detach(String, Vec<DatasetName>),
    /// The source's `ds/` leases are still live: wait for them to lapse.
    Wait,
    /// Detached from both: end the record.
    End,
}

/// As the source would: detach it from the volumes, then, once it writes
/// none of them, detach this node too and end the record. Where the guest
/// runs doesn't matter: the source's agent is gone (whatever it ran can't
/// reach its disks), and the VM is started wherever its volumes go.
/// `attached` gives a volume's attached nodes, `writer` the node writing
/// it.
pub(crate) fn stand_in_step(
    rec: &MigrationRecord,
    me: &str,
    attached: impl Fn(&DatasetName) -> Vec<String>,
    writer: impl Fn(&DatasetName) -> Option<String>,
) -> StandInStep {
    let on = |node: &str| -> Vec<DatasetName> {
        rec.volumes
            .iter()
            .filter(|v| attached(v).iter().any(|n| n == node))
            .cloned()
            .collect()
    };
    let from = on(&rec.from);
    if !from.is_empty() {
        return StandInStep::Detach(rec.from.clone(), from);
    }
    if rec
        .volumes
        .iter()
        .any(|v| writer(v).as_deref() == Some(rec.from.as_str()))
    {
        return StandInStep::Wait;
    }
    let mine = on(me);
    if !mine.is_empty() {
        return StandInStep::Detach(me.to_string(), mine);
    }
    StandInStep::End
}

/// The safety net: a VM running here as this node's, writing all its
/// `volumes` here. With no migration of it `recorded`, none of them should
/// be attached anywhere (`attached`): those that are leaked (a migration
/// from before records, a lost `EndMigration`) and go back to their own
/// placement.
pub(crate) fn leaked(
    volumes: &[DatasetName],
    recorded: bool,
    attached: impl Fn(&DatasetName) -> bool,
) -> Vec<DatasetName> {
    if recorded {
        return Vec::new();
    }
    volumes.iter().filter(|v| attached(v)).cloned().collect()
}
