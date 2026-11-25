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

use super::migrate::{Failure, Migrated, Migration, Unsettled, Whereabouts};
use crate::types::DatasetName;
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
