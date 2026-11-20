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
use crate::types::Epoch;
use crate::vdev::Pool;
use crate::volume_io::{LocalIo, OwnedPaused, SwitchableIo};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// How long the receiving end waits for the writer to finish its part.
pub(super) const HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);
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

    pub async fn add_export(&self, name: &str, t: Transport) -> Result<(), String> {
        self.exporter(t)
            .add_export(name)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn remove_export(&self, name: &str, t: Transport) -> Result<bool, String> {
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
    pub slots: Mutex<BTreeMap<String, Slot>>,
    /// The volumes this node writes, by `ds/` lease epoch: what peers may
    /// proxy to.
    pub writing: Mutex<BTreeMap<String, Epoch>>,
}

impl Shared {
    pub fn new(leases: LeaseClient) -> Self {
        Shared {
            leases,
            desc: Mutex::new(Default::default()),
            writer: Mutex::new(None),
            slots: Mutex::new(BTreeMap::new()),
            writing: Mutex::new(BTreeMap::new()),
        }
    }

    /// Put back what a failed takeover took: the proxy, or nothing.
    fn restore(&self, volume: &str, proxy: Option<Proxy>) {
        let mut slots = self.slots.lock().unwrap();
        match proxy {
            Some(p) => slots.insert(volume.to_string(), Slot::Proxy(p)),
            None => slots.remove(volume),
        };
    }
}

/// Take volume `name` over on this node's writer `w`, holding its lease as
/// `owner` at `epoch` (granted no earlier than `asked`): attach the
/// dataset and serve the export locally, the paused proxied one if there
/// is one (`proxy`, `paused`), else a new one over `transport`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn take_over(
    leases: &LeaseClient,
    w: &WriterParts,
    name: &str,
    transport: Transport,
    owner: &str,
    epoch: Epoch,
    asked: Instant,
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
    fn export(&self, volume: &str, epoch: Epoch) -> Result<PeerExport, String> {
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
        [Transport::Nbd, Transport::Nvme]
            .into_iter()
            .find_map(|t| w.exporter(t).peer_export(volume))
            .ok_or_else(|| format!("{} isn't exported here", volume))
    }

    async fn handoff(
        &self,
        volume: &str,
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
                    slots.insert(volume.to_string(), Slot::TakingOver);
                    Ok(Some(p))
                }
                None => {
                    slots.insert(volume.to_string(), Slot::TakingOver);
                    Ok(None)
                }
                Some(other) => {
                    slots.insert(volume.to_string(), other);
                    Err(())
                }
            }
        };
        let Ok(proxy) = claimed else { refuse!("busy") };
        if self.writing.lock().unwrap().contains_key(volume) {
            self.restore(volume, proxy);
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
            self.restore(volume, proxy);
            return;
        }
        let line = tokio::time::timeout(HANDOFF_TIMEOUT, read_line(&mut rd)).await;
        let new_epoch = match line {
            Ok(Ok(l)) => l
                .strip_prefix("TRANSFERRED ")
                .and_then(|e| e.parse::<Epoch>().ok()),
            _ => None,
        };
        let Some(new_epoch) = new_epoch else {
            drop(paused);
            self.restore(volume, proxy);
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
            pair,
        )
        .await;
        match taken {
            Ok((lease, lost)) => {
                self.writing
                    .lock()
                    .unwrap()
                    .insert(volume.to_string(), new_epoch);
                let device = proxy.as_ref().and_then(|p| p.device.clone());
                self.slots.lock().unwrap().insert(
                    volume.to_string(),
                    Slot::Adopted {
                        lease,
                        lost,
                        transport,
                        device,
                    },
                );
                drop(paused);
                println!("agent: took volume '{}' over (epoch {})", volume, new_epoch);
                let _ = wr.write_all(b"DONE\n").await;
                let _ = wr.flush().await;
            }
            Err(e) => {
                drop(paused);
                self.restore(volume, proxy);
                refuse!("{}", e)
            }
        }
    }
}
