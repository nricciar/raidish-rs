//! This node's writer (`NodeWriter`): its lease, the pool in writer role,
//! the export servers and the healer of lent space.

use crate::cluster::lease::HeldLease;
use crate::committer::CommitClient;
use crate::fs::FileSystem;
use crate::nbd::NbdServer;
use crate::nvme::NvmeTcpServer;
use crate::types::LeaseResource;
use crate::volumes;
use crate::{Agent, Lost, hold};
use std::sync::Arc;
use std::time::Duration;

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

impl Agent {
    pub(crate) async fn ensure_writer(&mut self) -> Result<&NodeWriter, String> {
        if self.writer.is_none() {
            let resource = LeaseResource::writer(&self.node);
            let lease =
                Arc::new(HeldLease::acquire(self.leases.clone(), resource, self.desc()).await?);
            let sink = CommitClient::new(self.endpoints.clone()).as_writer(lease.lease_ref());
            // Once the writer lease is gone, commits stop waiting for the
            // committer (it would answer "retry" for as long as nobody
            // else takes the lease, which for `writer/<node>` is forever).
            let lost = hold(&lease, Some(sink.cancel.clone()));
            let fs = match FileSystem::load_writer(self.access.open().await, Arc::new(sink)).await {
                Ok(fs) => Arc::new(fs),
                Err(e) => {
                    // Let go now, so the next tick can take it again
                    // (dropping it would too, a little later).
                    lease.release().await;
                    return Err(format!("opening the pool: {}", e));
                }
            };
            let bind = self.config.bind();
            let nbd = Arc::new(NbdServer::with_bind(
                fs.clone(),
                bind.clone(),
                self.config.nbd_port(),
            ));
            let nvme = Arc::new(NvmeTcpServer::with_bind(
                fs.clone(),
                bind,
                self.config.nvme_port(),
            ));
            let mut tasks = Vec::new();
            let n = nbd.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = n.run().await {
                    eprintln!("agent: NBD server: {}", e);
                }
            }));
            let n = nvme.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = n.run().await {
                    eprintln!("agent: NVMe/TCP target: {}", e);
                }
            }));
            // The committer's health monitor resilvers the pool, except the
            // space lent here: its writes are fenced out of it. This node
            // repairs that itself (see `FileSystem::heal_lent`).
            let healer = fs.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(HEAL_INTERVAL).await;
                    match healer.heal_lent().await {
                        Ok(Some(r)) if r.repaired() > 0 || !r.is_clean() => {
                            eprintln!("agent: lent space: {}", r);
                        }
                        Ok(_) => {}
                        Err(e) => eprintln!("agent: healing lent space: {}", e),
                    }
                }
            }));
            *self.files.write().unwrap() = Some(fs.clone());
            let parts = volumes::WriterParts {
                wgen: volumes::WriterGen::next(),
                fs,
                nbd,
                nvme,
            };
            self.shared.volumes.set_writer(parts.clone());
            self.writer = Some(NodeWriter {
                _lease: lease,
                lost,
                parts,
                tasks,
            });
        }
        Ok(self.writer.as_ref().unwrap())
    }
}
