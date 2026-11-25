//! The service datasets (S3, SQS, etcd) this node serves (see
//! `raidish_services::frontend`): placing, starting and stopping them.

use crate::cluster::lease::HeldLease;
use crate::committer::PoolAdmin;
use crate::committer::dataset_resource;
use crate::events::{EventKind, Level, Subject};
use crate::placement::place_on;
use crate::types::DatasetName;
use crate::types::LeaseResource;
use crate::{Agent, Lost, StartError, hold};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// A service dataset this agent runs (see `crate::frontend`).
pub(crate) struct RunningService {
    pub(crate) lease: Arc<HeldLease>,
    pub(crate) lost: Lost,
    pub(crate) format: crate::dataset::DatasetFormat,
}

/// The services (S3, SQS, etcd datasets) this node serves.
#[derive(Default)]
pub(crate) struct Services {
    /// This node's service front ends (None: it serves none).
    pub(crate) frontend: Option<Arc<crate::frontend::Frontend>>,
    pub(crate) running: BTreeMap<DatasetName, RunningService>,
}

impl Agent {
    /// Place, start and stop the service datasets (see `crate::frontend`),
    /// and tell the front ends where each runs.
    pub(crate) async fn tick_services(
        &mut self,
        state: &crate::cluster::state::ClusterState,
        now: u64,
        live: &BTreeSet<String>,
    ) {
        let Some(fe) = self.services.frontend.clone() else {
            return;
        };
        let Ok(datasets) = self.committer.datasets().await else {
            return;
        };
        // The live nodes that serve, and where.
        let nodes: BTreeMap<String, String> = live
            .iter()
            .filter_map(|n| {
                let addr = state
                    .live_lease(&LeaseResource::node(n), now)?
                    .holder
                    .addr
                    .clone()?;
                Some((n.clone(), addr))
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
        fe.set_routes(routes, nodes);
        let stop: Vec<DatasetName> = self
            .services
            .running
            .keys()
            .filter(|n| !wanted.contains_key(*n))
            .cloned()
            .collect();
        for name in stop {
            println!("agent: stopping dataset '{}' here", name);
            let f = self.service_format(&name);
            let label = crate::events::label_of(f);
            self.stop_service(&name, true).await;
            self.events.info(
                EventKind::DatasetStopped,
                Subject::dataset(f, &name),
                format!("{} '{}' stopped on {}", label, name, self.node),
            );
        }
        for (name, (format, held)) in wanted {
            if self.services.running.contains_key(&name) || held {
                continue; // running, or someone still has it
            }
            let label = crate::events::format_label(format);
            let subject = Subject::dataset(Some(format), &name);
            match self.start_service(&fe, &name, format).await {
                Ok(()) => {
                    println!("agent: serving {} dataset '{}' here", format.name(), name);
                    self.events.clear(&subject);
                    self.events.info(
                        EventKind::DatasetStarted,
                        subject,
                        format!("{} '{}' started on {}", label, name, self.node),
                    );
                }
                Err(StartError::Busy(e)) => {
                    eprintln!("agent: starting dataset '{}': {}", name, e);
                }
                Err(StartError::Failed(e)) => {
                    eprintln!("agent: starting dataset '{}': {}", name, e);
                    self.events.once(
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

    pub(crate) async fn start_service(
        &mut self,
        fe: &Arc<crate::frontend::Frontend>,
        name: &DatasetName,
        format: crate::dataset::DatasetFormat,
    ) -> Result<(), StartError> {
        let lease = Arc::new(
            HeldLease::acquire(self.leases.clone(), dataset_resource(name), self.desc())
                .await
                .map_err(StartError::Busy)?,
        );
        let lost = hold(&lease, None);
        let started = async {
            let w = self.ensure_writer().await?;
            let l = lease.clone();
            w.fs.attach_dataset_leased(
                name,
                lease.holding(),
                Some(lost.watch()),
                Some(Arc::new(move || l.fresh())),
            )
            .await
            .map_err(|e| e.to_string())?;
            fe.serve_local(name, format, w.fs.clone(), lease.epoch)
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        if let Err(e) = started {
            fe.stop_local(name);
            if let Some(w) = &self.writer {
                w.fs.abandon_dataset(name);
            }
            lease.release().await;
            return Err(StartError::Failed(e));
        }
        self.services.running.insert(
            name.clone(),
            RunningService {
                lease,
                lost,
                format,
            },
        );
        Ok(())
    }

    /// Service dataset `name`'s format, if it runs here.
    pub(crate) fn service_format(
        &self,
        name: &DatasetName,
    ) -> Option<crate::dataset::DatasetFormat> {
        self.services.running.get(name).map(|r| r.format)
    }

    /// Stop serving service dataset `name`; `clean`: sync it and give its
    /// lease back (otherwise the lease is lost: drop it, writing nothing).
    pub(crate) async fn stop_service(&mut self, name: &DatasetName, clean: bool) {
        let Some(r) = self.services.running.remove(name) else {
            return;
        };
        if let Some(fe) = &self.services.frontend {
            fe.stop_local(name);
        }
        if let Some(w) = &self.writer {
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
