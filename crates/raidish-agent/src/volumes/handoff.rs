//! Volumes attached to several nodes: the proxied exports of the attached
//! nodes that don't write a volume, and handing a volume from the node
//! that writes it to another (`docs/design/mig-1-volume-handoff.md`).
//!
//! The agent loop (see `crate::Agent::tick`) decides what should happen:
//! which volumes get a proxied export here, and when to hand one this node
//! writes to another node. The receiving end of a handoff runs outside that
//! loop, in the peer I/O listener ([`Shared::handoff`]), so what it needs
//! of this node lives in [`Shared`], and what it takes over is left in
//! [`Volumes`] for the loop to pick up.
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

use super::ATTACH_RETRY;
use super::{Export, Owned, Took, Volumes, WriterParts};
use crate::Agent;
use crate::api_types::Attach;
use crate::cluster::lease::{HeldLease, LeaseClient, owner_id};
use crate::committer::dataset_resource;
use crate::events::{EventKind, Subject};
use crate::inode::Transport;
use crate::peer_io::{PeerExport, PeerLines, PeerOut, PeerVolumes, read_line};
use crate::types::Holder;
use crate::types::{DatasetName, Epoch};
use crate::volume_io::LocalIo;
use crate::{Lost, hold};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// How long either end waits on the other for a reply that needs no work
/// (READY, DONE).
pub(crate) const HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);
/// The longest the writer handing a volume over may take from the
/// receiver's READY to transferring the lease: its clients' drain
/// (`DRAIN_TIMEOUT`, 30 s) and the last syncs. Past it, the writer gives up
/// instead (the volume synced, its lease released).
pub(crate) const SENDER_BUDGET: Duration = Duration::from_secs(60);
/// How long the receiver waits for TRANSFERRED: the sender's budget, then
/// the transfer itself (two lease calls of at most 5 s) with room to spare.
/// Waiting less, the receiver could give up first and the lease be handed
/// to an owner nobody renews: the volume down until it expired (structural
/// triage I-27).
pub(crate) const TRANSFER_WAIT: Duration = Duration::from_secs(SENDER_BUDGET.as_secs() + 30);
/// How long a proxied request keeps retrying (the writer failing over
/// takes a lease TTL or so).
pub(crate) const PROXY_RETRY: Duration = Duration::from_secs(20);

/// What the agent shares with its peer I/O listener.
pub(crate) struct Shared {
    pub leases: LeaseClient,
    /// What this node's leases say of it.
    pub desc: Mutex<crate::types::Holder>,
    /// This node's writer, and what it has of each volume.
    pub volumes: Volumes,
}

impl Shared {
    pub fn new(leases: LeaseClient) -> Self {
        Shared {
            leases,
            desc: Mutex::new(Default::default()),
            volumes: Volumes::default(),
        }
    }
}

/// Hold volume `name`'s lease as `owner` at `epoch` (granted no earlier
/// than `asked`) and attach its dataset on writer `w`: the first half of
/// taking it over (the export is the caller's).
pub(crate) async fn take_lease(
    leases: &LeaseClient,
    w: &WriterParts,
    name: &DatasetName,
    owner: &str,
    epoch: Epoch,
    asked: Instant,
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
    Ok((lease, lost))
}

#[async_trait]
impl PeerVolumes for Shared {
    fn export(&self, volume: &DatasetName, epoch: Epoch) -> Result<PeerExport, String> {
        self.volumes.peer_export(volume, epoch)
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
        let Some(w) = self.volumes.writer() else {
            refuse!("no writer here yet")
        };
        // Claim the volume's slot: nothing else happens to it meanwhile.
        // Refused, nothing was changed (a proxy over another transport
        // stays as it is: taken over, it would be served over the proxy's
        // while recorded as `transport`, its export never removed).
        let mut guard = match self.volumes.begin_takeover(volume, w.wgen, transport) {
            Ok(g) => g,
            Err(e) => refuse!("{}", e),
        };
        // Requests in flight finish (the writer still serves them); new
        // ones wait until it's handed over, or not.
        let mut paused = match &guard.prior {
            Some(p) => Some(p.export.io().pause_owned().await),
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
            guard.abort().await;
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
            guard.abort().await;
            eprintln!(
                "agent: the handoff of '{}' (epoch {}) was called off",
                volume, epoch
            );
            return;
        };
        let (lease, lost) =
            match take_lease(&self.leases, &w, volume, &owner, new_epoch, asked).await {
                Ok(l) => l,
                Err(e) => {
                    drop(paused);
                    guard.abort().await;
                    refuse!("{}", e)
                }
            };
        // Serve it from here: the paused proxied export switched to local
        // I/O (its clients keep their connections), or a new one.
        let had_proxy = guard.prior.is_some();
        let (export, attach): (Export, Attach) = match (guard.prior.take(), paused.as_mut()) {
            (Some(p), Some(g)) => {
                // Taken over cleanly from its writer, which made what it
                // acknowledged durable; but writes an earlier, unclean
                // writer change lost are still owed a failed flush (audit
                // A2), and so are writes an earlier writer acknowledged that
                // this proxy, idle since, never found out about (I-22).
                if p.peer.owed_on_handoff(epoch) {
                    p.export.io().poison();
                }
                g.set(Arc::new(LocalIo::new(w.fs.clone(), volume)));
                p.peer.retire();
                (p.export, p.attach)
            }
            (prior, _) => {
                // No proxy (one is always paused when there is).
                guard.prior = prior;
                match w.add_export(volume, transport).await {
                    Ok(e) => (e, Attach::Exported),
                    Err(e) => {
                        drop(paused);
                        w.fs.abandon_dataset(volume);
                        lease.release().await;
                        guard.abort().await;
                        refuse!("exporting: {}", e)
                    }
                }
            }
        };
        let vol = Owned {
            lease,
            lost,
            handed_from: Some(epoch),
            export,
            attach,
            attach_tried: Instant::now(),
        };
        // Published only while `w` is still this node's writer: dropping it
        // turns a takeover finishing after that away, undone here, where
        // `w` is still at hand (audit A1).
        if let Err(vol) = guard.finish(vol, Took::Adopted) {
            // Writes acked on the proxy since the pause, and not flushed,
            // go with the dataset: owed a failed flush.
            if had_proxy {
                vol.export.io().poison();
            }
            drop(paused);
            if let Err(e) = vol.export.close().await {
                eprintln!("agent: unexporting '{}': {}", volume, e);
            }
            w.fs.abandon_dataset(volume);
            vol.lease.release().await;
            refuse!("this node's writer went away meanwhile")
        }
        drop(paused);
        println!("agent: took volume '{}' over (epoch {})", volume, new_epoch);
        let _ = wr.write_all(b"DONE\n").await;
        let _ = wr.flush().await;
    }
}

impl Agent {
    /// Hand volume `name`, which this node writes, to node `to` (see
    /// `handoff`). Ok(true): it's not run here any more (handed over, or
    /// stopped cleanly when the handoff failed after this node let go);
    /// Ok(false): no peer I/O, stop it instead; Err: still run here.
    pub(crate) async fn hand_off(&mut self, name: &DatasetName, to: &str) -> Result<bool, String> {
        use tokio::io::{AsyncWriteExt, BufReader};
        let Some(p) = &self.peer else {
            return Ok(false);
        };
        let (connector, port) = (p.connector.clone(), p.port);
        let addr = self.peer_addr(to)?;
        let (epoch, transport) = self
            .shared
            .volumes
            .owned(name, |o| (o.lease.epoch, o.export.transport))
            .ok_or("not running here")?;
        let parts = self
            .writer
            .as_ref()
            .map(|w| w.parts.clone())
            .ok_or("no writer here")?;
        let tls = crate::peer_io::dial(&connector, &addr, port).await?;
        let (rd, mut wr) = tokio::io::split(tls);
        let mut rd = BufReader::new(rd);
        let ask = format!(
            "{} {} {} {}\n",
            crate::peer_io::HANDOFF,
            name,
            epoch,
            transport
        );
        wr.write_all(ask.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        wr.flush().await.map_err(|e| e.to_string())?;
        let answer = tokio::time::timeout(HANDOFF_TIMEOUT, crate::peer_io::read_line(&mut rd))
            .await
            .map_err(|_| format!("{} didn't answer", to))?
            .map_err(|e| format!("{}: {}", to, e))?;
        let Some((owner, desc)) = answer
            .strip_prefix("READY ")
            .and_then(|r| r.split_once('\t'))
        else {
            return Err(format!("{} said: {}", to, answer));
        };
        let desc: Holder =
            serde_json::from_str(desc).map_err(|e| format!("{}'s holder: {}", to, e))?;
        let owner = owner.to_string();
        // From here it's the handoff's: peers aren't pointed at it any more,
        // nothing else claims it, and it comes back whole if called off.
        let Some(vol) = self.shared.volumes.begin_handoff(name, parts.wgen) else {
            return Err("not running here any more".into());
        };
        println!("agent: handing volume '{}' to {}", name, to);
        let started = std::time::Instant::now();

        // Stop serving it here: clients (the peer's proxy too) drain, then
        // the last sync.
        let Owned {
            lease,
            lost,
            handed_from,
            export,
            attach,
            attach_tried,
        } = vol;
        if attach.device().is_some()
            && let Err(e) = self.detach(name, transport).await
        {
            eprintln!("agent: detaching '{}': {}", name, e);
        }
        if let Err(e) = export.close().await {
            eprintln!("agent: unexporting '{}': {}", name, e);
        }
        if let Err(e) = parts.fs.detach_dataset(name).await {
            // Not all of it is committed: keep it, call the handoff off.
            let _ = wr.write_all(b"ABORT\n").await;
            let _ = wr.flush().await;
            let export = match parts.add_export(name, transport).await {
                Ok(x) => x,
                Err(e2) => {
                    // Not served, it can't run here: let go, and whoever's
                    // next replays its intent log.
                    parts.fs.abandon_dataset(name);
                    lease.release().await;
                    self.shared.volumes.finish_handoff(name, parts.wgen);
                    return Err(format!(
                        "last sync: {}; exporting it again: {}; stopped here",
                        e, e2
                    ));
                }
            };
            let (attach, attach_tried) = match attach.device() {
                // Detached for the handoff: attached again right away.
                Some(_) => (
                    Attach::Failed {
                        error: "detached for a handoff that failed".into(),
                    },
                    std::time::Instant::now()
                        .checked_sub(ATTACH_RETRY)
                        .unwrap_or_else(std::time::Instant::now),
                ),
                None => (attach, attach_tried),
            };
            let vol = Owned {
                lease,
                lost,
                handed_from,
                export,
                attach,
                attach_tried,
            };
            if let Err(vol) = self.shared.volumes.abort_handoff(name, parts.wgen, vol) {
                self.wind_down(name, vol, true).await;
            }
            return Err(format!("last sync: {}", e));
        }
        self.shared.volumes.finish_handoff(name, parts.wgen);

        if started.elapsed() > SENDER_BUDGET {
            // The receiver stops waiting not long after: transferred now,
            // the lease could go to an owner nobody renews. Synced, so let
            // go of it instead: whoever's next starts it (I-27).
            let _ = wr.write_all(b"ABORT\n").await;
            let _ = wr.flush().await;
            lease.release().await;
            self.events.warn(
                EventKind::VolumeHandoffFailed,
                Subject::volume(name),
                format!(
                    "Volume '{}' took too long to hand from {} to {} ({:?}); stopped here instead",
                    name,
                    self.node,
                    to,
                    started.elapsed()
                ),
            );
            return Ok(true);
        }
        let mut transferred = lease.transfer(&owner, desc.clone()).await;
        if let Err(crate::cluster::lease::TransferError::NoAnswer(_)) = &transferred {
            // No answer may still have gone through: the same transfer
            // again says which.
            transferred = lease.transfer(&owner, desc.clone()).await;
        }
        let new_epoch = match transferred {
            Ok(e) => e,
            Err(e) => {
                // Synced and let go of: whoever's next starts it.
                let _ = wr.write_all(b"ABORT\n").await;
                let _ = wr.flush().await;
                lease.release().await;
                self.events.warn(
                    EventKind::VolumeHandoffFailed,
                    Subject::volume(name),
                    format!(
                        "Volume '{}' couldn't be handed from {} to {} ({}); stopped here instead",
                        name, self.node, to, e
                    ),
                );
                return Ok(true);
            }
        };
        let _ = wr
            .write_all(format!("TRANSFERRED {}\n", new_epoch).as_bytes())
            .await;
        let _ = wr.flush().await;
        let done = tokio::time::timeout(HANDOFF_TIMEOUT, crate::peer_io::read_line(&mut rd)).await;
        match done {
            Ok(Ok(l)) if l == "DONE" => {
                println!(
                    "agent: handed volume '{}' to {} in {:?}",
                    name,
                    to,
                    started.elapsed()
                );
                self.events.info(
                    EventKind::VolumeHandedOff,
                    Subject::volume(name),
                    format!("Volume '{}' was handed from {} to {}", name, self.node, to),
                );
            }
            other => {
                // Its lease is theirs either way: if they couldn't take it,
                // they let go of it and it starts wherever it goes next.
                eprintln!(
                    "agent: {} didn't confirm taking '{}' over: {:?}",
                    to, name, other
                );
            }
        }
        Ok(true)
    }
}
