//! The service datasets (S3, SQS, etcd) this node serves (see
//! `raidish_services::frontend`): placing, starting and stopping them, in
//! their own task ([`ServiceController`]), apart from the volumes and VMs.
//!
//! They run on this node's writer, which the task watches (`NodeSupervisor::
//! writers`): what ran on one writer goes with it, by its generation (a
//! writer dropped and opened again between two looks shows only as a new
//! generation, never as None), before anything starts on the next.

use crate::cluster::Cluster;
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::PoolAdmin;
use crate::committer::dataset_resource;
use crate::events::{EventKind, EventSink, Level, Subject};
use crate::frontend::Frontend;
use crate::placement::place_on;
use crate::types::{DatasetName, Holder, LeaseResource};
use crate::volumes::WriterParts;
use crate::{Agent, Lost, StartError, TICK, hold, note, note_once};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{Notify, watch};

/// A service dataset this node runs (see `crate::frontend`).
pub(crate) struct RunningService {
    pub(crate) lease: Arc<HeldLease>,
    pub(crate) lost: Lost,
    pub(crate) format: crate::dataset::DatasetFormat,
}

/// What runs here, and on which writer.
pub(crate) struct Served {
    pub(crate) node: String,
    pub(crate) frontend: Arc<Frontend>,
    pub(crate) events: EventSink,
    /// The writer what runs here runs on.
    pub(crate) ran_on: Option<WriterParts>,
    pub(crate) running: BTreeMap<DatasetName, RunningService>,
}

/// The services' task: see the module docs.
pub(crate) struct ServiceController {
    served: Served,
    cluster: Arc<Cluster>,
    leases: LeaseClient,
    committer: crate::committer::Admin,
    /// What this node's leases say of it.
    desc: Holder,
    writers: watch::Receiver<Option<WriterParts>>,
    /// Whether this node holds its node lease (it places nothing without).
    placing: watch::Receiver<bool>,
    /// Poked when a service's lease is lost.
    wake: Arc<Notify>,
}

impl std::ops::Deref for ServiceController {
    type Target = Served;
    fn deref(&self) -> &Served {
        &self.served
    }
}

impl std::ops::DerefMut for ServiceController {
    fn deref_mut(&mut self) -> &mut Served {
        &mut self.served
    }
}

impl ServiceController {
    pub(crate) fn new(agent: &Agent, frontend: Arc<Frontend>) -> Self {
        ServiceController {
            served: Served {
                node: agent.node.clone(),
                frontend,
                events: agent.events.clone(),
                ran_on: None,
                running: BTreeMap::new(),
            },
            cluster: agent.cluster.clone(),
            leases: agent.leases.clone(),
            committer: agent.committer.clone(),
            desc: agent.desc(),
            writers: agent.sup.writers(),
            placing: agent.sup.placing(),
            wake: Arc::new(Notify::new()),
        }
    }

    /// Every `TICK`, or once the writer changes or a lease is lost.
    pub(crate) async fn run(mut self) {
        loop {
            self.tick().await;
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                _ = self.wake.notified() => {}
                changed = self.writers.changed() => {
                    if changed.is_err() {
                        return; // the agent is gone
                    }
                }
            }
        }
    }

    async fn tick(&mut self) {
        let writer = self.writers.borrow_and_update().clone();
        self.served.follow(writer).await;
        self.served.drop_lost().await;
        if *self.placing.borrow() {
            self.place().await;
        }
    }
}

impl Served {
    /// What runs here follows this node's writer (`writer`): once it's
    /// another generation, or none, what ran on the old one goes first
    /// (unclean: that writer's lease is lost, it can't write).
    pub(crate) async fn follow(&mut self, writer: Option<WriterParts>) {
        let wgen = |w: &Option<WriterParts>| w.as_ref().map(|w| w.wgen);
        if wgen(&writer) == wgen(&self.ran_on) {
            return;
        }
        let names: Vec<DatasetName> = self.running.keys().cloned().collect();
        for name in names {
            self.stop(&name, false).await;
        }
        self.ran_on = writer;
    }

    /// Stop serving what lost its lease.
    pub(crate) async fn drop_lost(&mut self) {
        let lost: Vec<DatasetName> = self
            .running
            .iter()
            .filter(|(_, r)| r.lost.is_lost())
            .map(|(n, _)| n.clone())
            .collect();
        for name in lost {
            let f = self.format(&name);
            let label = crate::events::label_of(f);
            note(
                &self.events,
                Level::Error,
                EventKind::DatasetLost,
                Subject::dataset(f, &name),
                format!(
                    "{} '{}' stopped on {}: its lease was lost",
                    label, name, self.node
                ),
            );
            self.stop(&name, false).await;
        }
    }
}

impl ServiceController {
    /// Place, start and stop the service datasets, and tell the front
    /// ends where each runs.
    async fn place(&mut self) {
        let Some(w) = self.ran_on.clone() else {
            return; // no writer to run them on (yet)
        };
        let Ok(datasets) = self.committer.datasets().await else {
            return;
        };
        let state = self.cluster.status().state;
        let now = crate::cluster::now_ms();
        // The live nodes that serve, and where.
        let nodes: BTreeMap<String, String> = state
            .live_nodes(now)
            .filter_map(|(n, _)| {
                let addr = state
                    .live_lease(&LeaseResource::node(n), now)?
                    .holder
                    .addr
                    .clone()?;
                Some((n.to_string(), addr))
            })
            .collect();
        let serving: BTreeSet<String> = nodes.keys().cloned().collect();
        let mut routes = BTreeMap::new();
        let mut wanted = BTreeMap::new();
        for d in datasets.iter().filter(|d| d.format.is_service()) {
            let holder = state
                .live_lease(&dataset_resource(&d.name), now)
                .map(|l| &l.holder);
            let owner = holder.and_then(|h| {
                Some(crate::frontend::Owner {
                    node: h.node.clone()?,
                    addr: h.addr.clone()?,
                })
            });
            routes.insert(
                d.name.to_string(),
                crate::frontend::Route {
                    format: d.format,
                    port: d.props.port,
                    created_ms: d.created_ms,
                    owner,
                },
            );
            let target = place_on(&d.props, holder.and_then(|h| h.node.as_deref()), &serving);
            if target.as_deref() == Some(&self.node) {
                wanted.insert(d.name.clone(), (d.format, holder.is_some()));
            }
        }
        self.frontend.set_routes(routes, nodes);
        let stop: Vec<DatasetName> = self
            .running
            .keys()
            .filter(|n| !wanted.contains_key(*n))
            .cloned()
            .collect();
        for name in stop {
            println!("agent: stopping dataset '{}' here", name);
            let f = self.format(&name);
            let label = crate::events::label_of(f);
            self.stop(&name, true).await;
            note(
                &self.events,
                Level::Info,
                EventKind::DatasetStopped,
                Subject::dataset(f, &name),
                format!("{} '{}' stopped on {}", label, name, self.node),
            );
        }
        for (name, (format, held)) in wanted {
            if self.running.contains_key(&name) || held {
                continue; // running, or someone still has it
            }
            let label = crate::events::format_label(format);
            let subject = Subject::dataset(Some(format), &name);
            match self.start(&w, &name, format).await {
                Ok(()) => {
                    self.events.clear(&subject);
                    note(
                        &self.events,
                        Level::Info,
                        EventKind::DatasetStarted,
                        subject,
                        format!("{} '{}' started on {}", label, name, self.node),
                    );
                }
                Err(StartError::Busy(e)) => {
                    eprintln!("agent: starting dataset '{}': {}", name, e);
                }
                Err(StartError::Failed(e)) => {
                    note_once(
                        &self.events,
                        Level::Error,
                        EventKind::DatasetStartFailed,
                        subject,
                        format!(
                            "{} '{}' failed to start on {}: {}",
                            label, name, self.node, e
                        ),
                    );
                }
            }
        }
    }

    /// Serve `name` here, on writer `w`.
    async fn start(
        &mut self,
        w: &WriterParts,
        name: &DatasetName,
        format: crate::dataset::DatasetFormat,
    ) -> Result<(), StartError> {
        let lease = Arc::new(
            HeldLease::acquire(
                self.leases.clone(),
                dataset_resource(name),
                self.desc.clone(),
            )
            .await
            .map_err(StartError::Busy)?,
        );
        let lost = hold(&lease, None, &self.wake);
        let started = async {
            let l = lease.clone();
            w.fs.attach_dataset_leased(
                name,
                lease.holding(),
                Some(lost.watch()),
                Some(Arc::new(move || l.fresh())),
            )
            .await
            .map_err(|e| e.to_string())?;
            self.frontend
                .serve_local(name, format, w.fs.clone(), lease.epoch)
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        if let Err(e) = started {
            self.frontend.stop_local(name);
            w.fs.abandon_dataset(name);
            lease.release().await;
            return Err(StartError::Failed(e));
        }
        self.running.insert(
            name.clone(),
            RunningService {
                lease,
                lost,
                format,
            },
        );
        Ok(())
    }
}

impl Served {
    /// Service dataset `name`'s format, if it runs here.
    fn format(&self, name: &DatasetName) -> Option<crate::dataset::DatasetFormat> {
        self.running.get(name).map(|r| r.format)
    }

    /// Stop serving `name`; `clean`: sync it and give its lease back
    /// (otherwise the lease, or the writer's, is lost: drop it, writing
    /// nothing).
    async fn stop(&mut self, name: &DatasetName, clean: bool) {
        let Some(r) = self.running.remove(name) else {
            return;
        };
        self.frontend.stop_local(name);
        if let Some(w) = &self.ran_on {
            if clean {
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
            r.lease.release().await;
        }
    }
}
