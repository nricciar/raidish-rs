//! The volumes on this node: the ones it writes, exports and attaches, the
//! ones it exports proxied to a writer elsewhere, and handing them between
//! nodes (`handoff`). What it has of each is one table (`table`).

pub(crate) mod handoff;
mod table;

pub(crate) use table::*;

use crate::cluster::lease::HeldLease;
use crate::committer::PoolAdmin;
use crate::committer::dataset_resource;
use crate::events::{EventKind, Level, Subject};
use crate::inode::{DatasetProps, Transport};
use crate::placement::Placement;
use crate::types::DatasetName;
use crate::vms::Domains;
use crate::volume::VolumeInfo;
use crate::{Agent, Attach, StartError, hold, peer_host};
use std::sync::Arc;
use std::time::Duration;

/// How long after a failed attach the agent tries again.
pub(crate) const ATTACH_RETRY: Duration = Duration::from_secs(10);

impl Agent {
    /// Stop what no longer belongs here (deleted, disabled, moved, changed
    /// transport) once no VM here uses it, handing it over where it can
    /// be; start what belongs here.
    pub(crate) async fn reconcile_volumes(&mut self, p: &Placement, doms: &Domains) {
        let in_use = doms.in_use();
        let running = self.shared.volumes.running();
        let stop: Vec<DatasetName> = running
            .iter()
            .filter(|(name, t)| match p.volume(name) {
                None => true,
                Some(v) => {
                    p.target(name, &v.props).as_deref() != Some(&self.node)
                        || p.transport(name, &v.props) != Some(*t)
                }
            })
            .filter(|(name, _)| !in_use.contains(name))
            // Libvirt unknown: VM volumes stay where they are, they may be
            // in use.
            .filter(|(name, _)| doms.listed.is_ok() || !p.for_vm(name))
            .map(|(n, _)| n.clone())
            .collect();
        for name in stop {
            // Going to a live node it's attached to (or asked to move to):
            // hand it over, connections and all. Otherwise stop it here
            // and let the next node start it.
            if let Some(v) = p.volume(&name)
                && let Some(to) = p.target(&name, &v.props)
                && p.peer_on
                && to != self.node
                && p.live.contains(&to)
                && p.transport(&name, &v.props)
                    == self.shared.volumes.owned(&name, |o| o.export.transport)
                && (p.attached(&name).contains(&to)
                    || v.props.move_to.as_deref() == Some(to.as_str()))
            {
                match self.hand_off(&name, &to).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("agent: handing '{}' to {}: {}", name, to, e);
                        continue; // still the writer: again next tick
                    }
                }
                if !self.shared.volumes.writes(&name) {
                    continue;
                }
            }
            println!("agent: stopping volume '{}' here", name);
            self.stop(&name, true).await;
            self.events.info(
                EventKind::VolumeStopped,
                Subject::volume(&name),
                format!("Volume '{}' stopped on {}", name, self.node),
            );
        }
        for v in p.volumes.values() {
            if self.shared.volumes.writes(&v.name) {
                continue;
            }
            if p.target(&v.name, &v.props).as_deref() != Some(&self.node) {
                continue;
            }
            if p.state.dataset_held(&v.name, p.now) {
                continue; // someone still has it: wait for them to let go
            }
            let Some(t) = p.transport(&v.name, &v.props) else {
                continue;
            };
            if let Err(e) = self.start(v, t, !p.for_vm(&v.name)).await {
                eprintln!("agent: starting volume '{}': {}", v.name, e);
                let StartError::Failed(e) = e else {
                    continue;
                };
                self.events.once(
                    Level::Error,
                    EventKind::VolumeStartFailed,
                    Subject::volume(&v.name),
                    format!(
                        "Volume '{}' failed to start on {}: {}",
                        v.name, self.node, e
                    ),
                );
            }
        }
    }

    /// Exports of volumes attached here and written elsewhere.
    pub(crate) async fn reconcile_proxies(&mut self, p: &Placement) {
        for v in p.volumes.values() {
            let here = p.attached(&v.name).contains(&self.node);
            // Anything else there: written here, or being handed over.
            let proxied = self.shared.volumes.proxy_transport(&v.name);
            let slot = self.shared.volumes.has(&v.name);
            // Only once another node writes it: proxying to nobody, the
            // kernel's first reads (attaching) would just wait.
            let elsewhere = p.holder(&v.name).is_some_and(|h| h != self.node);
            if here && !slot && elsewhere {
                let Some(t) = p.transport(&v.name, &v.props) else {
                    continue;
                };
                if let Err(e) = self.add_proxy(v, t, !p.for_vm(&v.name)).await {
                    eprintln!("agent: proxying volume '{}': {}", v.name, e);
                }
            } else if proxied.is_some() && (!here || proxied != p.transport(&v.name, &v.props)) {
                // Gone from here, or now over another transport
                // (made again next tick).
                self.remove_proxy(&v.name).await;
            }
        }
        let gone: Vec<DatasetName> = self
            .shared
            .volumes
            .proxied()
            .into_iter()
            .filter(|n| p.volume(n).is_none())
            .collect();
        for name in gone {
            self.remove_proxy(&name).await;
        }
    }

    /// Run volume `v` here, exported over `transport`; `attach` the
    /// kernel device too (a VM's disks aren't: qemu is the NBD client).
    pub(crate) async fn start(
        &mut self,
        v: &VolumeInfo,
        transport: Transport,
        attach: bool,
    ) -> Result<(), StartError> {
        let parts = self
            .ensure_writer()
            .await
            .map_err(StartError::Failed)?
            .parts
            .clone();
        // Exported here already, proxied to a writer that's gone: that
        // export is taken over (its clients keep their connections). Over
        // another transport it can't be (it would be served over one while
        // recorded as the other, and never removed): it goes first (I-27).
        if self
            .shared
            .volumes
            .proxy_transport(&v.name)
            .is_some_and(|t| t != transport)
        {
            self.remove_proxy(&v.name).await;
        }
        let mut guard = self
            .shared
            .volumes
            .begin_takeover(&v.name, parts.wgen, transport)
            .map_err(|e| StartError::Busy(e.to_string()))?;
        let lease =
            match HeldLease::acquire(self.leases.clone(), dataset_resource(&v.name), self.desc())
                .await
            {
                Ok(l) => Arc::new(l),
                Err(e) => {
                    guard.abort().await;
                    return Err(StartError::Busy(e));
                }
            };
        let lost = hold(&lease, None);
        let started: Result<(Export, Option<Attach>), String> = async {
            let l = lease.clone();
            parts
                .fs
                .attach_dataset_leased(
                    &v.name,
                    lease.holding(),
                    Some(lost.watch()),
                    Some(Arc::new(move || l.fresh())),
                )
                .await
                .map_err(|e| e.to_string())?;
            match guard.prior.take() {
                Some(p) => {
                    // Its writer is gone (that's why this node starts it):
                    // requests stuck retrying it let go now and wait for
                    // the local backend, or the pause would wait them out.
                    // Whatever it acknowledged and nobody flushed went with
                    // it (only its intent log is replayed): say so at the
                    // next flush (audit A2).
                    p.peer.retire();
                    let mut paused = p.export.io().pause_owned().await;
                    // Asked once paused: a write acknowledged while the
                    // pause drained requests counts too.
                    if p.peer.unflushed() {
                        p.export.io().poison();
                    }
                    paused.set(Arc::new(crate::volume_io::LocalIo::new(
                        parts.fs.clone(),
                        &v.name,
                    )));
                    p.peer.retire();
                    drop(paused);
                    Ok((p.export, Some(p.attach)))
                }
                None => Ok((parts.add_export(&v.name, transport).await?, None)),
            }
        }
        .await;
        let (export, prior_attach) = match started {
            Ok(x) => x,
            Err(e) => {
                parts.fs.abandon_dataset(&v.name);
                lease.release().await;
                guard.abort().await;
                return Err(StartError::Failed(e));
            }
        };
        let needs_attach = prior_attach.is_none() && attach;
        let vol = Owned {
            lease,
            lost,
            handed_from: None,
            export,
            attach: prior_attach.unwrap_or_default(),
            attach_tried: std::time::Instant::now(),
        };
        // Written here from now on (peers may proxy to it); the device next.
        if let Err(vol) = guard.finish(vol, Took::Running) {
            self.wind_down(&v.name, vol, true).await;
            return Err(StartError::Busy("this node's writer went away".into()));
        }
        let attached = if needs_attach {
            let a = Attach::of(self.attach(&v.name, transport).await);
            self.shared.volumes.set_attach(&v.name, a.clone());
            a
        } else {
            self.shared
                .volumes
                .owned(&v.name, |o| o.attach.clone())
                .unwrap_or_default()
        };
        println!(
            "agent: running volume '{}' here ({}{})",
            v.name,
            transport,
            attached.on()
        );
        self.events.clear(&Subject::volume(&v.name));
        self.events.info(
            EventKind::VolumeStarted,
            Subject::volume(&v.name),
            format!(
                "Volume '{}' started on {} ({}{})",
                v.name,
                self.node,
                transport,
                attached.on()
            ),
        );
        if let Some(n) = attached.error() {
            eprintln!(
                "agent: volume '{}' exported but not attached: {}",
                v.name, n
            );
            self.events.warn(
                EventKind::VolumeNotAttached,
                Subject::volume(&v.name),
                format!(
                    "Volume '{}' is exported but not attached on {}: {}",
                    v.name, self.node, n
                ),
            );
        }
        // Moved here on request: done, clear the request.
        if v.props.move_to.as_deref() == Some(&self.node) {
            let props = DatasetProps {
                move_to: None,
                ..v.props.clone()
            };
            if let Err(e) = self.committer.set_props(&v.name, &props).await {
                eprintln!("agent: clearing the move of '{}': {}", v.name, e);
            }
        }
        Ok(())
    }

    /// Attach again the kernel devices of running volumes whose attach
    /// failed (the helper or a kernel module was missing, a transient
    /// error), so whatever waits for the device (see `crate::csi`) gets it.
    pub(crate) async fn retry_attach(&mut self) {
        for (name, t) in self.shared.volumes.attach_due(ATTACH_RETRY) {
            let attach = Attach::of(self.attach(&name, t).await);
            if attach.error().is_none() {
                println!("agent: attached volume '{}'{}", name, attach.on());
            }
            self.shared.volumes.set_attach(&name, attach);
        }
    }

    /// Stop running `name`: detach, unexport, and (`clean`) sync and release
    /// the lease; otherwise (lease lost) drop it without writing.
    pub(crate) async fn stop(&mut self, name: &DatasetName, clean: bool) {
        let Some(v) = self.shared.volumes.take_owned(name) else {
            return;
        };
        self.wind_down(name, v, clean).await;
    }

    /// Stop volume `name`, out of `Volumes` already (see `stop`). Takes
    /// only `&self`, so several can go at once.
    pub(crate) async fn wind_down(&self, name: &DatasetName, v: Owned, clean: bool) {
        if v.attach.device().is_some()
            && let Err(e) = self.detach(name, v.export.transport).await
        {
            eprintln!("agent: detaching '{}': {}", name, e);
        }
        let removed = v.export.close().await;
        if let Some(w) = &self.writer {
            if clean {
                if let Err(e) = removed {
                    eprintln!("agent: unexporting '{}': {}", name, e);
                }
                if let Err(e) = w.fs.detach_dataset(name).await {
                    // Its intent log is replayed once the lease is released.
                    eprintln!("agent: last sync of '{}': {}", name, e);
                    w.fs.abandon_dataset(name);
                }
            } else {
                w.fs.abandon_dataset(name);
            }
        }
        if clean {
            v.lease.release().await;
        }
    }

    /// Where node `node`'s peer I/O is: the host of its disk URLs (the
    /// name its certificate is issued for).
    pub(crate) fn peer_addr(&self, node: &str) -> Result<String, String> {
        peer_host(&self.cluster.members.peers, node)
            .ok_or_else(|| format!("{} isn't a known peer", node))
    }

    /// Where volume `name`'s writer is, looked up live each time it's asked.
    pub(crate) fn locator(&self, name: &DatasetName, port: u16) -> Arc<dyn crate::peer_io::Locate> {
        let cluster = self.cluster.clone();
        let resource = dataset_resource(name);
        let me = self.node.clone();
        let peers = self.cluster.members.peers.clone();
        Arc::new(move || {
            let state = cluster.status().state;
            let now = crate::cluster::now_ms();
            let l = state.live_lease(&resource, now)?;
            let node = l.holder.node.clone()?;
            if node == me {
                return None; // written here: nothing to proxy to
            }
            Some((peer_host(&peers, &node)?, port, l.epoch))
        })
    }

    /// Put what was handed to this node (see `handoff`) with what it runs.
    pub(crate) async fn adopt_handed_over(&mut self, volumes: &[VolumeInfo]) {
        for (name, transport, attach) in self.shared.volumes.pick_up() {
            let vm_disk = self.vms.specs.values().any(|s| s.volumes().contains(&name));
            if attach.device().is_none() && !vm_disk {
                let a = Attach::of(self.attach(&name, transport).await);
                self.shared.volumes.set_attach(&name, a);
            }
            self.events.info(
                EventKind::VolumeHandedOver,
                Subject::volume(&name),
                format!("Volume '{}' was handed over to {}", name, self.node),
            );
            if let Some(v) = volumes.iter().find(|v| v.name == name)
                && v.props.move_to.as_deref() == Some(&self.node)
            {
                let props = DatasetProps {
                    move_to: None,
                    ..v.props.clone()
                };
                if let Err(e) = self.committer.set_props(&name, &props).await {
                    eprintln!("agent: clearing the move of '{}': {}", name, e);
                }
            }
        }
    }

    /// Export volume `v` here over `t`, its I/O proxied to whichever node
    /// writes it; `attach` the kernel device too.
    pub(crate) async fn add_proxy(
        &mut self,
        v: &VolumeInfo,
        t: Transport,
        attach: bool,
    ) -> Result<(), String> {
        let Some(p) = &self.peer else {
            return Ok(());
        };
        let (connector, port) = (p.connector.clone(), p.port);
        let parts = self.ensure_writer().await?.parts.clone();
        let locate = self.locator(&v.name, port);
        let peer = Arc::new(
            crate::peer_io::PeerIo::new(&v.name, connector, locate).retry_for(handoff::PROXY_RETRY),
        );
        let handle = parts
            .exporter(t)
            .add_export_io(&v.name, v.size_bytes, peer.clone())
            .map_err(|e| e.to_string())?;
        let proxy = Proxied {
            peer,
            export: Export::new(t, handle),
            attach: Attach::Exported,
        };
        let proxy = if attach {
            match self.attach(&v.name, t).await {
                Ok(d) => Proxied {
                    attach: Attach::device_of(d),
                    ..proxy
                },
                Err(e) => {
                    // Not exported without its device: the next tick tries
                    // the whole thing again.
                    proxy.take_down(&v.name).await;
                    return Err(format!("attaching: {}", e));
                }
            }
        } else {
            proxy
        };
        if let Err(proxy) = self.shared.volumes.add_proxied(&v.name, parts.wgen, proxy) {
            // Claimed meanwhile (a handoff to here), or its writer's gone.
            self.take_down_proxy(&v.name, proxy).await;
            return Err("taken meanwhile".into());
        }
        println!(
            "agent: exporting volume '{}' here ({}), proxied to its writer",
            v.name, t
        );
        Ok(())
    }

    /// Stop exporting the proxied volume `name` here.
    pub(crate) async fn remove_proxy(&mut self, name: &DatasetName) {
        let Some(proxy) = self.shared.volumes.take_proxied(name) else {
            return;
        };
        self.take_down_proxy(name, proxy).await;
        println!("agent: no longer exporting proxied volume '{}' here", name);
    }

    /// Stop serving `proxy` (out of `Volumes` already): its device, then
    /// its export (`Proxied::take_down`). Retired first: unexported while
    /// it still retried a writer that's gone, the export's drain and last
    /// sync waited `PROXY_RETRY` per request, the agent loop stuck behind
    /// them (structural triage, `remove_proxy`).
    pub(crate) async fn take_down_proxy(&self, name: &DatasetName, proxy: Proxied) {
        proxy.retire();
        if proxy.attach.device().is_some()
            && let Err(e) = self.detach(name, proxy.export.transport).await
        {
            eprintln!("agent: detaching proxied '{}': {}", name, e);
        }
        proxy.take_down(name).await;
    }
}
