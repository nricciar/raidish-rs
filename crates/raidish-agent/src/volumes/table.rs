//! What this node has of each volume, in one place ([`Volumes`]): the ones
//! it writes, the ones it exports for a writer elsewhere, the ones being
//! handed to or from it. Shared by the agent loop and the peer I/O
//! listener (the receiving end of a handoff), under one lock that is never
//! held across an `.await`. Each change is a method that checks, under that
//! lock, the slot's state and the writer generation it was made under, and
//! hands the slow work (closing an export, releasing a lease) back to the
//! caller to do on values moved out of the map.
//!
//! Before this, the same was kept in four maps (the agent's running
//! volumes, the listener's slots, the epochs written here, the epochs
//! handed from), plus the export servers' tables, kept in step by
//! convention: a transport recorded one way and served another, a proxy
//! put back for a writer that was gone, an export left behind (structural
//! triage I-27).

use crate::Lost;
use crate::api_types::{Attach, RunningVolume};
use crate::cluster::lease::HeldLease;
use crate::export::{BlockExporter, ExportHandle};
use crate::fs::FileSystem;
use crate::inode::Transport;
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::peer_io::{PeerExport, PeerIo};
use crate::types::{DatasetName, Epoch};
use crate::vdev::Pool;
use crate::volume_io::SwitchableIo;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Which of this node's writers (see `NodeWriter`) something was made
/// under: a new one each time it's opened. What a writer ran goes with it,
/// so a slot made under an earlier one is never taken for the current one's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WriterGen(u64);

impl WriterGen {
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        WriterGen(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// This node's writer, as what runs on it needs it.
#[derive(Clone)]
pub(crate) struct WriterParts {
    pub wgen: WriterGen,
    pub fs: Arc<FileSystem<Pool>>,
    pub nbd: Arc<NbdServer<Pool>>,
    pub nvme: Arc<NvmeTcpServer<Pool>>,
}

impl WriterParts {
    /// The server exporting over `t`.
    pub fn exporter(&self, t: Transport) -> &dyn BlockExporter {
        match t {
            Transport::Nbd => &*self.nbd,
            Transport::Nvme => &*self.nvme,
        }
    }

    /// Export `name`, written here, over `t`.
    pub async fn add_export(&self, name: &DatasetName, t: Transport) -> Result<Export, String> {
        self.exporter(t)
            .add_export(name)
            .await
            .map(|handle| Export::new(t, handle))
            .map_err(|e| e.to_string())
    }
}

/// A volume's export here, and the transport it's served over: one
/// value, so the two can't disagree.
#[derive(Debug)]
pub(crate) struct Export {
    pub transport: Transport,
    handle: ExportHandle,
}

impl Export {
    pub fn new(transport: Transport, handle: ExportHandle) -> Self {
        Export { transport, handle }
    }

    pub fn io(&self) -> &Arc<SwitchableIo> {
        self.handle.io()
    }

    pub fn peer_export(&self) -> Option<PeerExport> {
        self.handle.peer_export()
    }

    /// Take it down: its clients drain, then the last sync.
    pub async fn close(self) -> Result<(), String> {
        self.handle.close().await.map_err(|e| e.to_string())
    }
}

/// A volume this node writes, under its own `ds/` lease.
pub(crate) struct Owned {
    pub lease: Arc<HeldLease>,
    pub lost: Lost,
    /// Taken over in a clean handoff: the epoch of the writer it came from
    /// (what peer proxies are told, see `PeerExport::from`).
    pub handed_from: Option<Epoch>,
    pub export: Export,
    /// Its kernel device here.
    pub attach: Attach,
    /// When it was last attached, or tried (a failed attach is tried again
    /// `ATTACH_RETRY` later).
    pub attach_tried: Instant,
}

/// A volume exported here and written by another node.
pub(crate) struct Proxied {
    pub peer: Arc<PeerIo>,
    pub export: Export,
    pub attach: Attach,
}

impl Proxied {
    /// Let go of the writer for good: requests stuck retrying one that's
    /// gone fail at once, with no replacement backend to wait for, so the
    /// export's drain and last sync don't stall.
    pub fn retire(&self) {
        self.export.io().no_replacement();
        self.peer.retire();
    }

    /// Take its export down for good (its device, if any, detached first
    /// by whoever can).
    pub async fn take_down(self, volume: &DatasetName) {
        self.retire();
        if let Err(e) = self.export.close().await {
            eprintln!("agent: unexporting proxied '{}': {}", volume, e);
        }
    }
}

/// What this node has of one volume.
pub(crate) enum VolumeSlot {
    /// Written here.
    Running { wgen: WriterGen, vol: Owned },
    /// Handed over to this node, not yet picked up by the agent loop (it
    /// attaches the device, says so). Written here all the same: peers
    /// proxy to it, and it goes with its lease or its writer.
    Adopted { wgen: WriterGen, vol: Owned },
    /// Being handed from this node: the handoff holds what it was
    /// (`Owned`), and puts it back if the handoff is called off.
    HandingOff { wgen: WriterGen },
    /// Exported here, written by a peer.
    Proxied { wgen: WriterGen, proxy: Proxied },
    /// Being taken over by this node (a handoff to it, or started here):
    /// the [`TakeoverGuard`] holds the proxy it replaces, if any.
    TakingOver { wgen: WriterGen },
}

impl VolumeSlot {
    fn wgen(&self) -> WriterGen {
        match self {
            VolumeSlot::Running { wgen, .. }
            | VolumeSlot::Adopted { wgen, .. }
            | VolumeSlot::HandingOff { wgen, .. }
            | VolumeSlot::Proxied { wgen, .. }
            | VolumeSlot::TakingOver { wgen, .. } => *wgen,
        }
    }

    /// The volume as this node writes it (running or just handed over).
    fn owned(&self) -> Option<&Owned> {
        match self {
            VolumeSlot::Running { vol, .. } | VolumeSlot::Adopted { vol, .. } => Some(vol),
            _ => None,
        }
    }
}

/// Why a takeover couldn't claim a volume's slot (nothing was changed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Claim {
    /// That isn't this node's current writer (or there's none).
    StaleWriter,
    /// Written here already.
    Written,
    /// A handoff to or from here is under way.
    Busy,
    /// Proxied here over another transport: taken over, it'd be served
    /// over one while recorded as the other (I-27).
    Transport(Transport),
}

impl std::fmt::Display for Claim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Claim::StaleWriter => write!(f, "no writer here (or it was replaced)"),
            Claim::Written => write!(f, "already written here"),
            Claim::Busy => write!(f, "a handoff is under way"),
            Claim::Transport(t) => write!(f, "exported here over {}", t),
        }
    }
}

/// What a dropped writer ran, for the caller to stop.
pub(crate) struct Dropped {
    pub owned: Vec<(DatasetName, Owned)>,
    pub proxies: Vec<(DatasetName, Proxied)>,
}

struct Inner {
    writer: Option<WriterParts>,
    slots: BTreeMap<DatasetName, VolumeSlot>,
}

/// See the module docs.
pub(crate) struct Volumes(Mutex<Inner>);

impl Default for Volumes {
    fn default() -> Self {
        Volumes(Mutex::new(Inner {
            writer: None,
            slots: BTreeMap::new(),
        }))
    }
}

impl Volumes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.lock().unwrap()
    }

    // ---- the writer ----

    /// `w` is this node's writer from now on.
    pub fn set_writer(&self, w: WriterParts) {
        self.lock().writer = Some(w);
    }

    pub fn writer(&self) -> Option<WriterParts> {
        self.lock().writer.clone()
    }

    /// Writer `wgen` is gone: so is all that ran on it. From here on nothing
    /// can be made under it (a takeover finishing late is turned away, see
    /// [`TakeoverGuard::finish`]). What it ran is handed back to stop; the
    /// handoffs under way find their slot gone and undo themselves.
    pub fn drop_writer(&self, wgen: WriterGen) -> Dropped {
        let mut inner = self.lock();
        if inner.writer.as_ref().is_some_and(|w| w.wgen == wgen) {
            inner.writer = None;
        }
        let names: Vec<DatasetName> = inner
            .slots
            .iter()
            .filter(|(_, s)| s.wgen() == wgen)
            .map(|(n, _)| n.clone())
            .collect();
        let mut out = Dropped {
            owned: Vec::new(),
            proxies: Vec::new(),
        };
        for n in names {
            match inner.slots.remove(&n) {
                Some(VolumeSlot::Running { vol, .. } | VolumeSlot::Adopted { vol, .. }) => {
                    out.owned.push((n, vol))
                }
                Some(VolumeSlot::Proxied { proxy, .. }) => out.proxies.push((n, proxy)),
                _ => {}
            }
        }
        out
    }

    // ---- reading ----

    /// Whether this node has anything of `name` (writes, proxies, or is
    /// handing it to or from here).
    pub fn has(&self, name: &DatasetName) -> bool {
        self.lock().slots.contains_key(name)
    }

    /// Whether this node writes `name` (running, or handed over to it).
    pub fn writes(&self, name: &DatasetName) -> bool {
        self.lock()
            .slots
            .get(name)
            .is_some_and(|s| s.owned().is_some())
    }

    /// What `f` makes of `name` as this node writes it.
    pub fn owned<R>(&self, name: &DatasetName, f: impl FnOnce(&Owned) -> R) -> Option<R> {
        self.lock()
            .slots
            .get(name)
            .and_then(VolumeSlot::owned)
            .map(f)
    }

    /// The volumes running here (picked up by the loop), and their
    /// transports.
    pub fn running(&self) -> Vec<(DatasetName, Transport)> {
        self.lock()
            .slots
            .iter()
            .filter_map(|(n, s)| match s {
                VolumeSlot::Running { vol, .. } => Some((n.clone(), vol.export.transport)),
                _ => None,
            })
            .collect()
    }

    /// The volumes written here whose own lease is lost.
    pub fn lost(&self) -> Vec<DatasetName> {
        self.lock()
            .slots
            .iter()
            .filter(|(_, s)| s.owned().is_some_and(|v| v.lost.is_lost()))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// The running volumes whose attach failed at least `retry` ago.
    pub fn attach_due(&self, retry: Duration) -> Vec<(DatasetName, Transport)> {
        self.lock()
            .slots
            .iter()
            .filter_map(|(n, s)| match s {
                VolumeSlot::Running { vol, .. }
                    if matches!(vol.attach, Attach::Failed { .. })
                        && vol.attach_tried.elapsed() >= retry =>
                {
                    Some((n.clone(), vol.export.transport))
                }
                _ => None,
            })
            .collect()
    }

    /// The transport `name` is proxied here over, if it is.
    pub fn proxy_transport(&self, name: &DatasetName) -> Option<Transport> {
        match self.lock().slots.get(name) {
            Some(VolumeSlot::Proxied { proxy, .. }) => Some(proxy.export.transport),
            _ => None,
        }
    }

    pub fn proxied(&self) -> Vec<DatasetName> {
        self.lock()
            .slots
            .iter()
            .filter(|(_, s)| matches!(s, VolumeSlot::Proxied { .. }))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// What the dashboard shows: derived from the slots, never kept apart.
    pub fn published(&self) -> Vec<RunningVolume> {
        self.lock()
            .slots
            .iter()
            .filter_map(|(n, s)| {
                let (export, attach, proxied) = match s {
                    VolumeSlot::Running { vol, .. } | VolumeSlot::Adopted { vol, .. } => {
                        (&vol.export, &vol.attach, false)
                    }
                    VolumeSlot::Proxied { proxy, .. } => (&proxy.export, &proxy.attach, true),
                    _ => return None,
                };
                Some(RunningVolume {
                    name: n.clone(),
                    transport: export.transport,
                    attach: attach.clone(),
                    proxied,
                })
            })
            .collect()
    }

    /// `volume`'s export for a peer's proxy that reached it at `epoch`.
    pub fn peer_export(&self, volume: &DatasetName, epoch: Epoch) -> Result<PeerExport, String> {
        let inner = self.lock();
        let vol = inner
            .slots
            .get(volume)
            .and_then(VolumeSlot::owned)
            .ok_or_else(|| format!("{} isn't written here", volume))?;
        if vol.lease.epoch != epoch {
            return Err(format!(
                "{} is written here at epoch {}, not {}",
                volume, vol.lease.epoch, epoch
            ));
        }
        let from = vol.handed_from.unwrap_or(Epoch::NONE);
        vol.export
            .peer_export()
            .map(|e| PeerExport { from, ..e })
            .ok_or_else(|| format!("{} isn't exported here", volume))
    }

    /// What's there of `name`, by state (for tests).
    #[cfg(test)]
    pub fn state(&self, name: &DatasetName) -> Option<&'static str> {
        self.lock().slots.get(name).map(|s| match s {
            VolumeSlot::Running { .. } => "running",
            VolumeSlot::Adopted { .. } => "adopted",
            VolumeSlot::HandingOff { .. } => "handing off",
            VolumeSlot::Proxied { .. } => "proxied",
            VolumeSlot::TakingOver { .. } => "taking over",
        })
    }

    // ---- changes ----

    /// Change running `name`'s attach (the attach was just tried).
    pub fn set_attach(&self, name: &DatasetName, attach: Attach) {
        if let Some(VolumeSlot::Running { vol, .. } | VolumeSlot::Adopted { vol, .. }) =
            self.lock().slots.get_mut(name)
        {
            vol.attach = attach;
            vol.attach_tried = Instant::now();
        }
    }

    /// Take `name`, written here, out to stop it.
    pub fn take_owned(&self, name: &DatasetName) -> Option<Owned> {
        let mut inner = self.lock();
        match inner.slots.remove(name) {
            Some(VolumeSlot::Running { vol, .. } | VolumeSlot::Adopted { vol, .. }) => Some(vol),
            Some(other) => {
                inner.slots.insert(name.clone(), other);
                None
            }
            None => None,
        }
    }

    /// Every volume handed over since the last look is running now: their
    /// names, transports and attach (for the loop to attach the device,
    /// where it wasn't, and say so).
    pub fn pick_up(&self) -> Vec<(DatasetName, Transport, Attach)> {
        let mut inner = self.lock();
        let names: Vec<DatasetName> = inner
            .slots
            .iter()
            .filter(|(_, s)| matches!(s, VolumeSlot::Adopted { .. }))
            .map(|(n, _)| n.clone())
            .collect();
        let mut out = Vec::new();
        for n in names {
            if let Some(VolumeSlot::Adopted { wgen, vol }) = inner.slots.remove(&n) {
                out.push((n.clone(), vol.export.transport, vol.attach.clone()));
                inner.slots.insert(n, VolumeSlot::Running { wgen, vol });
            }
        }
        out
    }

    /// Start exporting `name` here, proxied: only while nothing else is
    /// there and `wgen` is the current writer.
    pub fn add_proxied(
        &self,
        name: &DatasetName,
        wgen: WriterGen,
        proxy: Proxied,
    ) -> Result<(), Proxied> {
        let mut inner = self.lock();
        if inner.slots.contains_key(name) || !current(&inner, wgen) {
            return Err(proxy);
        }
        inner
            .slots
            .insert(name.clone(), VolumeSlot::Proxied { wgen, proxy });
        Ok(())
    }

    /// Take the proxied export of `name` out, to take it down.
    pub fn take_proxied(&self, name: &DatasetName) -> Option<Proxied> {
        let mut inner = self.lock();
        match inner.slots.remove(name) {
            Some(VolumeSlot::Proxied { proxy, .. }) => Some(proxy),
            Some(other) => {
                inner.slots.insert(name.clone(), other);
                None
            }
            None => None,
        }
    }

    /// Hand running `name` from here: what it is moves to the caller, a
    /// marker stays (peers are no longer pointed at it, nothing else claims
    /// it). Ends in [`Volumes::finish_handoff`] or [`Volumes::abort_handoff`].
    pub fn begin_handoff(&self, name: &DatasetName, wgen: WriterGen) -> Option<Owned> {
        let mut inner = self.lock();
        match inner.slots.remove(name) {
            Some(VolumeSlot::Running { wgen: g, vol }) if g == wgen => {
                inner
                    .slots
                    .insert(name.clone(), VolumeSlot::HandingOff { wgen });
                Some(vol)
            }
            Some(other) => {
                inner.slots.insert(name.clone(), other);
                None
            }
            None => None,
        }
    }

    /// The handoff of `name` was called off: it runs here again, as it was
    /// (`vol`). Err: its writer went meanwhile, so `vol` is the caller's
    /// to stop.
    pub fn abort_handoff(
        &self,
        name: &DatasetName,
        wgen: WriterGen,
        vol: Owned,
    ) -> Result<(), Owned> {
        let mut inner = self.lock();
        let ours = matches!(inner.slots.get(name), Some(VolumeSlot::HandingOff { wgen: g, .. }) if *g == wgen);
        if !ours || !current(&inner, wgen) {
            if ours {
                inner.slots.remove(name);
            }
            return Err(vol);
        }
        inner
            .slots
            .insert(name.clone(), VolumeSlot::Running { wgen, vol });
        Ok(())
    }

    /// `name` was handed from here: nothing of it is left.
    pub fn finish_handoff(&self, name: &DatasetName, wgen: WriterGen) {
        let mut inner = self.lock();
        if matches!(inner.slots.get(name), Some(VolumeSlot::HandingOff { wgen: g, .. }) if *g == wgen)
        {
            inner.slots.remove(name);
        }
    }

    /// Claim `name` to take it over on writer `wgen`, to be served over
    /// `transport`: the proxied export there (if any) moves to the guard,
    /// and nothing else claims the volume until the guard is done.
    pub fn begin_takeover(
        &self,
        name: &DatasetName,
        wgen: WriterGen,
        transport: Transport,
    ) -> Result<TakeoverGuard<'_>, Claim> {
        let mut inner = self.lock();
        if !current(&inner, wgen) {
            return Err(Claim::StaleWriter);
        }
        let prior = match inner.slots.get(name) {
            None => None,
            Some(VolumeSlot::Proxied { proxy, .. }) if proxy.export.transport != transport => {
                return Err(Claim::Transport(proxy.export.transport));
            }
            Some(VolumeSlot::Proxied { .. }) => match inner.slots.remove(name) {
                Some(VolumeSlot::Proxied { proxy, .. }) => Some(proxy),
                _ => unreachable!("just matched"),
            },
            Some(VolumeSlot::Running { .. } | VolumeSlot::Adopted { .. }) => {
                return Err(Claim::Written);
            }
            Some(VolumeSlot::HandingOff { .. } | VolumeSlot::TakingOver { .. }) => {
                return Err(Claim::Busy);
            }
        };
        inner
            .slots
            .insert(name.clone(), VolumeSlot::TakingOver { wgen });
        Ok(TakeoverGuard {
            volumes: self,
            name: name.clone(),
            wgen,
            prior,
            done: false,
        })
    }
}

fn current(inner: &Inner, wgen: WriterGen) -> bool {
    inner.writer.as_ref().is_some_and(|w| w.wgen == wgen)
}

/// Whether what was taken over is the loop's to pick up (a handoff to this
/// node, see [`Volumes::pick_up`]) or running already (started by the
/// loop itself).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Took {
    Adopted,
    Running,
}

/// A takeover's claim on a volume ([`Volumes::begin_takeover`]), and the
/// proxied export it replaces (`prior`), which the takeover moves into what
/// it makes of the volume, or leaves here to be put back.
///
/// Ends in [`TakeoverGuard::finish`], or [`TakeoverGuard::abort`]: the
/// claim is given up and the proxy put back, if it still serves a writer
/// this node has (else, its writer gone, it's taken down: it would serve
/// nobody). Dropped without either (an error, a cancelled task), it aborts
/// all the same, taking a dead proxy down in the background: a takeover
/// can't leave a claim behind.
pub(crate) struct TakeoverGuard<'a> {
    volumes: &'a Volumes,
    name: DatasetName,
    wgen: WriterGen,
    pub prior: Option<Proxied>,
    done: bool,
}

impl TakeoverGuard<'_> {
    /// The takeover is done: `vol` is written here now (`took`). Err: the
    /// claim is gone (its writer was dropped meanwhile), so `vol` is the
    /// caller's to stop. `prior`, if still here, is let go of either way.
    pub fn finish(mut self, vol: Owned, took: Took) -> Result<(), Owned> {
        self.done = true;
        if let Some(p) = self.prior.take() {
            // Its export is `vol`'s now, or should have been.
            p.retire();
        }
        let mut inner = self.volumes.lock();
        let ours = matches!(
            inner.slots.get(&self.name),
            Some(VolumeSlot::TakingOver { wgen, .. }) if *wgen == self.wgen
        );
        if !ours || !current(&inner, self.wgen) {
            if ours {
                inner.slots.remove(&self.name);
            }
            return Err(vol);
        }
        let wgen = self.wgen;
        let slot = match took {
            Took::Adopted => VolumeSlot::Adopted { wgen, vol },
            Took::Running => VolumeSlot::Running { wgen, vol },
        };
        inner.slots.insert(self.name.clone(), slot);
        Ok(())
    }

    /// Give the claim up: the proxy goes back if it still serves this
    /// node's writer; otherwise it's returned, to take down.
    fn settle(&mut self) -> Option<Proxied> {
        self.done = true;
        let mut inner = self.volumes.lock();
        let ours = matches!(
            inner.slots.get(&self.name),
            Some(VolumeSlot::TakingOver { wgen, .. }) if *wgen == self.wgen
        );
        if ours {
            inner.slots.remove(&self.name);
        }
        match self.prior.take() {
            Some(proxy) if ours && current(&inner, self.wgen) => {
                inner.slots.insert(
                    self.name.clone(),
                    VolumeSlot::Proxied {
                        wgen: self.wgen,
                        proxy,
                    },
                );
                None
            }
            p => p,
        }
    }

    /// Give the claim up (see the type's docs), taking a dead proxy down
    /// before returning.
    pub async fn abort(mut self) {
        if let Some(p) = self.settle() {
            p.take_down(&self.name).await;
        }
    }
}

impl Drop for TakeoverGuard<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(p) = self.settle() {
            // Retired now; its export's handle drains it in the background.
            p.retire();
            drop(p);
        }
    }
}
