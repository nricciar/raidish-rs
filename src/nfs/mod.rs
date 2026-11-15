//! An NFS server for the pool, on every `listen` node.
//!
//! Versions 3 (`v3`) and 4.1 (`v4`), over the shared `backend`, which
//! knows nothing of any one protocol version: file handles, attributes,
//! writes at any offset and who may write which file. `rpc` and `xdr` are
//! the wire layers.
//!
//! Every node serves the same file system with the same file handles, so a
//! client can mount any of them, and a hyperconverged host can mount its own
//! node (`127.0.0.1`) and keep running when another node fails:
//!
//! ```text
//! mount -t nfs -o vers=3,proto=tcp,port=2049,mountport=2049,nolock 127.0.0.1:/ /mnt/pool
//! ```
//!
//! There is no locking protocol (NLM) yet, hence `nolock`. NFS has no
//! authentication of its own here (`AUTH_SYS` is taken at its word): keep
//! the port on 127.0.0.1, the default, or a trusted network.

pub mod backend;
pub mod peers;
pub mod rpc;
pub mod v3;
pub mod v4;
pub mod xdr;

use crate::disk::BlockDevice;
use backend::Backend;
use serde::Deserialize;
use std::sync::Arc;

/// The `nfs` section of the config.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct NfsConfig {
    /// Address to listen on (default 127.0.0.1).
    pub bind: Option<String>,
    /// NFS, MOUNT and the portmapper all on this port (default 2049).
    pub port: Option<u16>,
    /// The name or address clients reach this node's NFS server at, told
    /// to NFSv4 clients (`fs_locations`) along with the other nodes' (the
    /// hosts in the pool config). Default: the host name. Nothing is told
    /// while `bind` is a loopback address.
    pub advertise: Option<String>,
}

impl NfsConfig {
    /// Whether clients on other machines can reach this server.
    pub fn reachable(&self) -> bool {
        let bind = self.bind.as_deref().unwrap_or("127.0.0.1");
        !(bind.starts_with("127.") || bind == "::1" || bind == "localhost")
    }

    /// This node's address for `fs_locations`.
    pub fn advertised(&self) -> Option<String> {
        if let Some(a) = &self.advertise {
            return Some(a.clone());
        }
        std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
    }

    pub fn addr(&self) -> String {
        format!(
            "{}:{}",
            self.bind.as_deref().unwrap_or("127.0.0.1"),
            self.port.unwrap_or(2049)
        )
    }
}

/// Every program on the port: NFS 3 and 4, MOUNT, the portmapper.
struct Programs<D: BlockDevice> {
    v3: v3::Server<D>,
    v4: Arc<v4::Server<D>>,
}

#[async_trait::async_trait]
impl<D: BlockDevice + 'static> rpc::Dispatch for Programs<D> {
    async fn call(&self, call: &rpc::Call, args: &mut xdr::Reader<'_>) -> rpc::Reply {
        use v3::{MOUNT_PROG, NFS_PROG, PMAP_PROG};
        match (call.prog, call.vers) {
            (NFS_PROG, 3) => self.v3.nfs(call, args).await,
            (NFS_PROG, 4) => self.v4.call(call, args).await,
            (NFS_PROG, _) => rpc::Reply::ProgMismatch(3, 4),
            (MOUNT_PROG, 3) => self.v3.mount(call, args).await,
            (MOUNT_PROG, _) => rpc::Reply::ProgMismatch(3, 3),
            (PMAP_PROG, 2) => self.v3.pmap(call, args),
            (PMAP_PROG, _) => rpc::Reply::ProgMismatch(2, 2),
            _ => rpc::Reply::ProgUnavail,
        }
    }
}

/// Serve `backend` over NFS (3 and 4.1) at `addr`. Returns the bound
/// address; runs (with the housekeeping) until the runtime stops.
/// `cluster`: where the NFSv4 state owner is (None: this node owns its
/// state). Returns the bound address and the NFSv4 server.
pub async fn start<D: BlockDevice + 'static>(
    addr: &str,
    backend: Arc<Backend<D>>,
    cluster: Option<Arc<dyn v4::V4Cluster>>,
) -> std::io::Result<(std::net::SocketAddr, Arc<v4::Server<D>>)> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let v4 = Arc::new(match cluster {
        Some(c) => v4::Server::with_cluster(backend.clone(), backend.node(), c),
        None => v4::Server::new(backend.clone(), backend.node()),
    });
    let returned = v4.clone();
    let programs = Arc::new(Programs {
        v3: v3::Server::new(backend.clone(), local.port()),
        v4: v4.clone(),
    });
    tokio::spawn(rpc::serve(listener, programs));
    tokio::spawn(async move {
        let mut ticks = 0u64;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            backend.tidy().await;
            ticks += 1;
            if ticks % 4 == 0 {
                v4.tidy().await;
            }
        }
    });
    Ok((local, returned))
}

#[cfg(test)]
#[path = "../tests/nfs_tests.rs"]
mod tests;
