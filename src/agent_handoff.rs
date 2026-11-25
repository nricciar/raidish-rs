//! Volumes attached to several nodes: the proxied exports of the attached
//! nodes that don't write a volume, and handing a volume from the node
//! that writes it to another (`docs/design/mig-1-volume-handoff.md`).
//!
//! The agent loop (see `super::Agent::tick`) decides what should happen:
//! which volumes get a proxied export here, and when to hand one this node
//! writes to another node. The receiving end of a handoff runs outside that
//! loop, in the peer I/O listener ([`Shared::handoff`]), so what it needs
//! of this node lives in [`Shared`], and what it takes over is left in a
//! [`Slot`] for the loop to pick up.
//!
//! A handoff, A (the writer) to B:
//!
//! 1. A asks B: `RAIDISH-HANDOFF/1 <volume> <epoch> <transport>`. B pauses
//!    its proxied export (requests in flight finish, A still serves
//!    them; new ones wait) and answers `READY <owner>\t<holder>`: the lease
//!    owner and holder (JSON) it will hold the volume's lease as.
//! 2. A stops serving it: its export is removed (local clients and B's
//!    proxy connection drain), the dataset is synced and detached.
//! 3. A transfers the `ds/` lease to B's owner (`Request::Transfer`) and
//!    says `TRANSFERRED <epoch>`.
//! 4. B adopts the lease, attaches the dataset, switches its export to
//!    local I/O, unpauses, and answers `DONE`.
//!
//! B gives up and unpauses (its export proxying again) if A stops short
//! of step 3; A never stops serving before B is paused and ready.

use super::{Lost, hold};
use crate::cluster::lease::{HeldLease, LeaseClient, owner_id};
use crate::committer::dataset_resource;
use crate::export::BlockExporter;
use crate::fs::FileSystem;
use crate::inode::Transport;
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::peer_io::{PeerExport, PeerIo, PeerLines, PeerOut, PeerVolumes, read_line};
use crate::types::DatasetName;
use crate::types::Epoch;
use crate::vdev::Pool;
use crate::volume_io::{LocalIo, OwnedPaused, SwitchableIo};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// How long either end waits on the other for a reply that needs no work
/// (READY, DONE).
pub(super) const HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest the writer handing a volume over may take from the
/// receiver's READY to transferring the lease: its clients' drain
/// (`DRAIN_TIMEOUT`, 30 s) and the last syncs. Past it, the writer gives up
/// instead (the volume synced, its lease released).
pub(super) const SENDER_BUDGET: Duration = Duration::from_secs(60);
/// How long the receiver waits for TRANSFERRED: the sender's budget, then
/// the transfer itself (two lease calls of at most 5 s) with room to spare.
/// Waiting less, the receiver could give up first and the lease be handed
/// to an owner nobody renews: the volume down until it expired (structural
/// triage I-27).
pub(super) const TRANSFER_WAIT: Duration = Duration::from_secs(SENDER_BUDGET.as_secs() + 30);
/// How long a proxied request keeps retrying (the writer failing over
/// takes a lease TTL or so).
pub(super) const PROXY_RETRY: Duration = Duration::from_secs(20);

/// This node's writer, as the handoff handler needs it.
#[derive(Clone)]
pub(super) struct WriterParts {
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

    pub async fn add_export(&self, name: &DatasetName, t: Transport) -> Result<(), String> {
        self.exporter(t)
            .add_export(name)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn remove_export(&self, name: &DatasetName, t: Transport) -> Result<bool, String> {
        self.exporter(t)
            .remove_export(name)
            .await
            .map_err(|e| e.to_string())
    }
}

/// A volume exported here but written by another node.
pub(super) struct Proxy {
    pub peer: Arc<PeerIo>,
    pub io: Arc<SwitchableIo>,
    pub transport: Transport,
    /// The kernel device attached to the export, if any.
    pub device: Option<String>,
}

impl Proxy {
    /// Take its export down for good (`device`, if any, detached first by
    /// whoever can): retired, so requests stuck retrying a writer that's
    /// gone let go, with no replacement to wait for, so they and the
    /// export's last sync fail at once instead of stalling the caller.
    pub(super) async fn take_down(&self, volume: &DatasetName, w: Option<&WriterParts>) {
        self.io.no_replacement();
        self.peer.retire();
        if let Some(w) = w
            && let Err(e) = w.remove_export(volume, self.transport).await
        {
            eprintln!("agent: unexporting proxied '{}': {}", volume, e);
        }
    }
}

/// What this node has of a volume it doesn't (yet) run as its writer.
pub(super) enum Slot {
    Proxy(Proxy),
    /// A handoff to this node is under way.
    TakingOver,
    /// Handed over: this node writes it now, for the agent loop to pick up.
    Adopted {
        lease: Arc<HeldLease>,
        lost: Lost,
        transport: Transport,
        device: Option<String>,
    },
}

/// What the agent shares with its peer I/O listener.
pub(super) struct Shared {
    pub leases: LeaseClient,
    /// What this node's leases say of it.
    pub desc: Mutex<crate::types::Holder>,
    pub writer: Mutex<Option<WriterParts>>,
    pub slots: Mutex<BTreeMap<DatasetName, Slot>>,
    /// The volumes this node writes, by `ds/` lease epoch: what peers may
    /// proxy to.
    pub writing: Mutex<BTreeMap<DatasetName, Epoch>>,
    /// Volumes taken over here in a clean handoff: name → (the epoch this
    /// node writes it at, the epoch of the writer it came from). What peer
    /// proxies are told (see `PeerExport::from`).
    pub handed_from: Mutex<BTreeMap<DatasetName, (Epoch, Epoch)>>,
}

impl Shared {
    pub fn new(leases: LeaseClient) -> Self {
        Shared {
            leases,
            desc: Mutex::new(Default::default()),
            writer: Mutex::new(None),
            slots: Mutex::new(BTreeMap::new()),
            writing: Mutex::new(BTreeMap::new()),
            handed_from: Mutex::new(BTreeMap::new()),
        }
    }

    /// Put back what a failed takeover on writer `w` took: its claim on
    /// the slot, and the proxy if it had one. Only while the claim still
    /// stands and `w` is still this node's writer: a writer dropped meanwhile
    /// (`drop_proxies`) took the claim with it, and its proxies are dead.
    /// Such a proxy isn't put back (it would serve again, to nobody) but
    /// taken down here, where it's still at hand (structural triage I-27).
    pub(super) async fn restore(
        &self,
        volume: &DatasetName,
        proxy: Option<Proxy>,
        w: &WriterParts,
    ) {
        let dead = {
            let cur = self.writer.lock().unwrap();
            let live = cur.as_ref().is_some_and(|c| Arc::ptr_eq(&c.fs, &w.fs));
            let mut slots = self.slots.lock().unwrap();
            let ours = matches!(slots.get(volume), Some(Slot::TakingOver));
            if ours {
                slots.remove(volume);
            }
            match proxy {
                Some(p) if ours && live => {
                    slots.insert(volume.clone(), Slot::Proxy(p));
                    None
                }
                p => p,
            }
        };
        if let Some(p) = dead {
            p.take_down(volume, Some(w)).await;
        }
    }
}

/// Take volume `name` over on this node's writer `w`, holding its lease as
/// `owner` at `epoch` (granted no earlier than `asked`): attach the
/// dataset and serve the export locally, the paused proxied one if there
/// is one (`proxy`, `paused`), else a new one over `transport`. `from`:
/// the epoch of the writer handing it over.
#[allow(clippy::too_many_arguments)]
pub(super) async fn take_over(
    leases: &LeaseClient,
    w: &WriterParts,
    name: &DatasetName,
    transport: Transport,
    owner: &str,
    epoch: Epoch,
    asked: Instant,
    from: Epoch,
    proxy: Option<(&Proxy, &mut OwnedPaused)>,
) -> Result<(Arc<HeldLease>, Lost), String> {
    let lease = Arc::new(HeldLease::adopt(
        leases.clone(),
        dataset_resource(name),
        owner,
        epoch,
        asked,
    ));
    let lost = hold(&lease, None);
    let l = lease.clone();
    if let Err(e) =
        w.fs.attach_dataset_leased(
            name,
            lease.holding(),
            Some(lost.watch()),
            Some(Arc::new(move || l.fresh())),
        )
        .await
    {
        w.fs.abandon_dataset(name);
        lease.release().await;
        return Err(format!("attaching: {}", e));
    }
    match proxy {
        Some((p, paused)) => {
            // Taken over cleanly from its writer, which made what it
            // acknowledged durable; but writes an earlier, unclean writer
            // change lost are still owed a failed flush (audit A2), and so
            // are writes an earlier writer acknowledged that this proxy,
            // idle since, never found out about (I-22).
            if p.peer.owed_on_handoff(from) {
                p.io.poison();
            }
            paused.set(Arc::new(LocalIo::new(w.fs.clone(), name)));
            p.peer.retire();
        }
        None => {
            if let Err(e) = w.add_export(name, transport).await {
                w.fs.abandon_dataset(name);
                lease.release().await;
                return Err(format!("exporting: {}", e));
            }
        }
    }
    Ok((lease, lost))
}

#[async_trait]
impl PeerVolumes for Shared {
    fn export(&self, volume: &DatasetName, epoch: Epoch) -> Result<PeerExport, String> {
        match self.writing.lock().unwrap().get(volume) {
            Some(e) if *e == epoch => {}
            Some(e) => {
                return Err(format!(
                    "{} is written here at epoch {}, not {}",
                    volume, e, epoch
                ));
            }
            None => return Err(format!("{} isn't written here", volume)),
        }
        let w = self
            .writer
            .lock()
            .unwrap()
            .clone()
            .ok_or("no writer here")?;
        let from = match self.handed_from.lock().unwrap().get(volume) {
            Some(&(now, from)) if now == epoch => from,
            _ => Epoch::NONE,
        };
        [Transport::Nbd, Transport::Nvme]
            .into_iter()
            .find_map(|t| w.exporter(t).peer_export(volume))
            .map(|e| PeerExport { from, ..e })
            .ok_or_else(|| format!("{} isn't exported here", volume))
    }

    async fn handoff(
        &self,
        volume: &DatasetName,
        epoch: Epoch,
        transport: &str,
        mut rd: PeerLines,
        mut wr: PeerOut,
    ) {
        let asked = Instant::now();
        macro_rules! refuse {
            ($($why:tt)*) => {{
                let why = format!($($why)*);
                eprintln!("agent: not taking '{}' over: {}", volume, why);
                let _ = wr.write_all(format!("ERR {}\n", why).as_bytes()).await;
                let _ = wr.flush().await;
                return;
            }};
        }
        let Ok(transport) = transport.parse::<Transport>() else {
            refuse!("unknown transport '{}'", transport)
        };
        let Some(w) = self.writer.lock().unwrap().clone() else {
            refuse!("no writer here yet")
        };
        // Claim the volume's slot: nothing else happens to it meanwhile.
        let claimed = {
            let mut slots = self.slots.lock().unwrap();
            match slots.remove(volume) {
                Some(Slot::Proxy(p)) => {
                    slots.insert(volume.clone(), Slot::TakingOver);
                    Ok(Some(p))
                }
                None => {
                    slots.insert(volume.clone(), Slot::TakingOver);
                    Ok(None)
                }
                Some(other) => {
                    slots.insert(volume.clone(), other);
                    Err(())
                }
            }
        };
        let Ok(proxy) = claimed else { refuse!("busy") };
        if let Some(p) = &proxy
            && p.transport != transport
        {
            // Taken over, it would be served over the proxy's transport
            // while recorded as `transport`, and its export never removed.
            let t = p.transport;
            self.restore(volume, proxy, &w).await;
            refuse!("exported here over {}, not {}", t, transport);
        }
        if self.writing.lock().unwrap().contains_key(volume) {
            self.restore(volume, proxy, &w).await;
            refuse!("already written here");
        }
        // Requests in flight finish (the writer still serves them); new
        // ones wait until it's handed over, or not.
        let mut paused = match &proxy {
            Some(p) => Some(p.io.pause_owned().await),
            None => None,
        };
        let owner = owner_id();
        let desc = serde_json::to_string(&*self.desc.lock().unwrap()).unwrap_or_default();
        if wr
            .write_all(format!("READY {}\t{}\n", owner, desc).as_bytes())
            .await
            .is_err()
            || wr.flush().await.is_err()
        {
            // Unpaused first: taking a dead proxy down syncs through it.
            drop(paused);
            self.restore(volume, proxy, &w).await;
            return;
        }
        let line = tokio::time::timeout(TRANSFER_WAIT, read_line(&mut rd)).await;
        let new_epoch = match line {
            Ok(Ok(l)) => l
                .strip_prefix("TRANSFERRED ")
                .and_then(|e| e.parse::<Epoch>().ok()),
            _ => None,
        };
        let Some(new_epoch) = new_epoch else {
            drop(paused);
            self.restore(volume, proxy, &w).await;
            eprintln!(
                "agent: the handoff of '{}' (epoch {}) was called off",
                volume, epoch
            );
            return;
        };
        let pair = match (&proxy, paused.as_mut()) {
            (Some(p), Some(g)) => Some((p, g)),
            _ => None,
        };
        let taken = take_over(
            &self.leases,
            &w,
            volume,
            transport,
            &owner,
            new_epoch,
            asked,
            epoch,
            pair,
        )
        .await;
        match taken {
            Ok((lease, lost)) => {
                let device = proxy.as_ref().and_then(|p| p.device.clone());
                // Published only while `w` is still this node's writer, under
                // its lock: the agent clears it before it stops what ran on
                // it, so a takeover finishing after that is undone here,
                // where `w` is still at hand (audit A1).
                let orphaned = {
                    let cur = self.writer.lock().unwrap();
                    if cur.as_ref().is_some_and(|c| Arc::ptr_eq(&c.fs, &w.fs)) {
                        self.writing
                            .lock()
                            .unwrap()
                            .insert(volume.clone(), new_epoch);
                        self.handed_from
                            .lock()
                            .unwrap()
                            .insert(volume.clone(), (new_epoch, epoch));
                        self.slots.lock().unwrap().insert(
                            volume.clone(),
                            Slot::Adopted {
                                lease,
                                lost,
                                transport,
                                device,
                            },
                        );
                        None
                    } else {
                        Some(lease)
                    }
                };
                if let Some(lease) = orphaned {
                    // Writes acked on the proxy since the pause, and not
                    // flushed, go with the dataset: owed a failed flush.
                    if let Some(p) = &proxy {
                        p.io.poison();
                    }
                    drop(paused);
                    if let Err(e) = w.remove_export(volume, transport).await {
                        eprintln!("agent: unexporting '{}': {}", volume, e);
                    }
                    w.fs.abandon_dataset(volume);
                    lease.release().await;
                    self.restore(volume, None, &w).await;
                    refuse!("this node's writer went away meanwhile")
                }
                drop(paused);
                println!("agent: took volume '{}' over (epoch {})", volume, new_epoch);
                let _ = wr.write_all(b"DONE\n").await;
                let _ = wr.flush().await;
            }
            Err(e) => {
                drop(paused);
                self.restore(volume, proxy, &w).await;
                refuse!("{}", e)
            }
        }
    }
}
