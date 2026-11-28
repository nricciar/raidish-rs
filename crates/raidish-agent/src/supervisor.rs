//! The node lease and this node's writer ([`NodeSupervisor`]): the writer
//! (`NodeWriter`) is its lease, the pool in writer role, the export
//! servers and the healer of lent space. What runs on it learns of it from
//! the volume table (`Volumes::set_writer`) and a `watch`
//! ([`NodeSupervisor::writers`]: the service task's).

use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::committer::{CommitClient, PoolAccess};
use crate::fs::FileSystem;
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::types::{Holder, LeaseResource};
use crate::volumes::handoff::Shared;
use crate::volumes::{self, WriterParts};
use crate::{AutomountConfig, Lost, WriterSlot, hold};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// How often a node checks whether space lent to it needs a resilver.
pub(crate) const HEAL_INTERVAL: Duration = Duration::from_secs(10);

/// This node's volume writer: its lease, the pool in writer role, the
/// export servers.
pub(crate) struct NodeWriter {
    /// Held (and renewed) for as long as this writer exists.
    pub(crate) _lease: Arc<HeldLease>,
    pub(crate) lost: Lost,
    /// Its file system and block servers (`w.fs`, `w.exporter(t)`), and
    /// its generation (`w.wgen`).
    pub(crate) parts: volumes::WriterParts,
    pub(crate) tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for NodeWriter {
    type Target = volumes::WriterParts;
    fn deref(&self) -> &volumes::WriterParts {
        &self.parts
    }
}

impl Drop for NodeWriter {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// What opening the writer takes.
pub(crate) struct WriterConf {
    pub node: String,
    pub leases: LeaseClient,
    /// Where the writer's commits go.
    pub endpoints: Vec<String>,
    pub access: PoolAccess,
    pub config: AutomountConfig,
    /// Where this node's file API finds the writer.
    pub files: WriterSlot,
    pub shared: Arc<Shared>,
}

/// Keeps the node lease and opens this node's writer; lets go of a writer
/// whose lease is lost (`take_lost_writer`) for the agent loop to fence
/// what ran on it.
pub(crate) struct NodeSupervisor {
    conf: WriterConf,
    node_lease: Option<(Arc<HeldLease>, Lost)>,
    writer: Option<NodeWriter>,
    /// The writer, for what runs on it outside the agent loop.
    writer_tx: watch::Sender<Option<WriterParts>>,
    /// Whether the node lease is held: this node places things.
    placing_tx: watch::Sender<bool>,
}

impl NodeSupervisor {
    pub(crate) fn new(conf: WriterConf) -> Self {
        NodeSupervisor {
            conf,
            node_lease: None,
            writer: None,
            writer_tx: watch::Sender::new(None),
            placing_tx: watch::Sender::new(false),
        }
    }

    /// This node's writer as it changes (None: there's none). Watched, a
    /// drop and reopen between two looks shows only as a new generation.
    pub(crate) fn writers(&self) -> watch::Receiver<Option<WriterParts>> {
        self.writer_tx.subscribe()
    }

    /// Whether this node holds its node lease.
    pub(crate) fn placing(&self) -> watch::Receiver<bool> {
        self.placing_tx.subscribe()
    }

    fn desc(&self) -> Holder {
        self.conf.shared.desc.lock().unwrap().clone()
    }

    /// Hold `node/<name>` (taken again once lost): whether it's held.
    pub(crate) async fn ensure_node_lease(&mut self) -> bool {
        if self
            .node_lease
            .as_ref()
            .is_none_or(|(_, lost)| lost.is_lost())
        {
            let resource = LeaseResource::node(&self.conf.node);
            self.node_lease =
                match HeldLease::acquire(self.conf.leases.clone(), resource, self.desc()).await {
                    Ok(l) => {
                        let l = Arc::new(l);
                        let lost = hold(&l, None, self.conf.shared.volumes.wake());
                        Some((l, lost))
                    }
                    Err(e) => {
                        eprintln!("agent: {}", e);
                        None
                    }
                };
        }
        let held = self.node_lease.is_some();
        self.placing_tx
            .send_if_modified(|p| std::mem::replace(p, held) != held);
        held
    }

    /// This node's writer, if it has one.
    pub(crate) fn writer(&self) -> Option<&NodeWriter> {
        self.writer.as_ref()
    }

    /// The writer, once its lease is lost: no longer this node's (the file
    /// API and the watchers are told), and the caller's to fence what ran
    /// on it before the next is opened (`Agent::drop_lost`).
    pub(crate) fn take_lost_writer(&mut self) -> Option<NodeWriter> {
        if !self.writer.as_ref().is_some_and(|w| w.lost.is_lost()) {
            return None;
        }
        let w = self.writer.take()?;
        *self.conf.files.write().unwrap() = None;
        self.writer_tx.send_replace(None);
        Some(w)
    }

    /// This node's writer, opened if there's none.
    pub(crate) async fn ensure_writer(&mut self) -> Result<&NodeWriter, String> {
        if self.writer.is_none() {
            let w = self.open_writer().await?;
            *self.conf.files.write().unwrap() = Some(w.fs.clone());
            self.conf.shared.volumes.set_writer(w.parts.clone());
            self.writer_tx.send_replace(Some(w.parts.clone()));
            self.writer = Some(w);
        }
        Ok(self.writer.as_ref().unwrap())
    }

    async fn open_writer(&self) -> Result<NodeWriter, String> {
        let c = &self.conf;
        let resource = LeaseResource::writer(&c.node);
        let lease = Arc::new(HeldLease::acquire(c.leases.clone(), resource, self.desc()).await?);
        let sink = CommitClient::new(c.endpoints.clone()).as_writer(lease.lease_ref());
        // Once the writer lease is gone, commits stop waiting for the
        // committer (it would answer "retry" for as long as nobody else
        // takes the lease, which for `writer/<node>` is forever).
        let lost = hold(&lease, Some(sink.cancel.clone()), c.shared.volumes.wake());
        let fs = match FileSystem::load_writer(c.access.open().await, Arc::new(sink)).await {
            Ok(fs) => Arc::new(fs),
            Err(e) => {
                // Let go now, so the next tick can take it again (dropping
                // it would too, a little later).
                lease.release().await;
                return Err(format!("opening the pool: {}", e));
            }
        };
        let bind = c.config.bind();
        let nbd_port = c.config.nbd_port();
        let nvme_port = c.config.nvme_port();
        // Bind synchronously before advertising the writer (H-14): a just-dropped
        // writer's aborted tasks may take a moment to release their sockets, so
        // retry `AddrInUse` briefly; any persistent failure fails `open_writer`.
        let nbd_listener = match bind_listener(&bind, nbd_port).await {
            Ok(l) => l,
            Err(e) => {
                lease.release().await;
                return Err(format!("binding NBD on {}:{}: {}", bind, nbd_port, e));
            }
        };
        let nvme_listener = match bind_listener(&bind, nvme_port).await {
            Ok(l) => l,
            Err(e) => {
                drop(nbd_listener);
                lease.release().await;
                return Err(format!("binding NVMe/TCP on {}:{}: {}", bind, nvme_port, e));
            }
        };
        let nbd = Arc::new(NbdServer::with_bind(fs.clone(), bind.clone(), nbd_port));
        let nvme = Arc::new(NvmeTcpServer::with_bind(fs.clone(), bind, nvme_port));
        let mut tasks = Vec::new();
        let n = nbd.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = n.serve(nbd_listener).await {
                eprintln!("agent: NBD server: {}", e);
            }
        }));
        let n = nvme.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = n.serve(nvme_listener).await {
                eprintln!("agent: NVMe/TCP target: {}", e);
            }
        }));
        tasks.push(crate::committer::health::spawn_lent_healer(
            fs.clone(),
            HEAL_INTERVAL,
            "agent",
        ));
        Ok(NodeWriter {
            _lease: lease,
            lost,
            parts: volumes::WriterParts {
                wgen: volumes::WriterGen::next(),
                fs,
                nbd,
                nvme,
            },
            tasks,
        })
    }
}

/// Bind `(addr, port)`, retrying `AddrInUse` briefly so a just-dropped
/// writer's aborted listener tasks have time to drop their sockets (H-14).
pub(crate) async fn bind_listener(
    addr: &str,
    port: u16,
) -> std::io::Result<tokio::net::TcpListener> {
    let mut attempts = 0u32;
    loop {
        match tokio::net::TcpListener::bind((addr, port)).await {
            Ok(l) => return Ok(l),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && attempts < 20 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
