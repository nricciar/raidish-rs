//! Reaching the other nodes' NFS servers: who holds a file (`backend::Peers`)
//! or the NFSv4 state (`v4::V4Cluster`) comes from this node's copy of the
//! cluster's leases, and requests go over the node API
//! (`POST /api/v1/nfs/forward` and `/api/v1/nfs/v4`, node certificates
//! only), as bincode. The NFSv4 state owner is whoever holds the `nfs4`
//! lease (`own_v4_state`), and keeps its client records in the cluster.

use super::backend::{Backend, Forward, Forwarded, Peers, lease_node};
use super::v4::{self, V4Cluster, V4Forward, V4Forwarded};
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::cluster::state::{NFS4, Response};
use crate::cluster::{Cluster, LeaseOp, now_ms};
use crate::committer::object_resource;
use crate::disk::FileId;
use crate::vdev::Pool;
use async_trait::async_trait;
use rocket::data::{Data, ToByteUnit};
use rocket::http::Status;
use rocket::{State, post};
use std::sync::Arc;
use std::time::Duration;

/// Largest forwarded request (a 1 MiB write and its framing, with room).
const MAX_FORWARD: u64 = 8 << 20;

pub struct ClusterPeers {
    cluster: Arc<Cluster>,
    http: reqwest::Client,
    leases: LeaseClient,
    /// The `nfs4` lease (owner, epoch), while this node holds it.
    nfs4: std::sync::Mutex<Option<(String, u64)>>,
    /// Every node's NFS address (see `with_locations`), by node id.
    locations: Vec<(u64, String)>,
    /// The NFS port (the same on every node).
    port: u16,
    /// Those of them resolved to IP addresses so far.
    resolved: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

impl ClusterPeers {
    /// `leases`: where lease requests go (this node's own API).
    pub fn new(cluster: Arc<Cluster>, leases: LeaseClient) -> Self {
        ClusterPeers {
            cluster,
            http: crate::tls::http_builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
            leases,
            nfs4: Default::default(),
            locations: Vec::new(),
            port: 2049,
            resolved: Default::default(),
        }
    }

    /// Tell NFSv4 clients every node's address: this node's (`me`) and
    /// the other members' hosts (from their URLs in the pool config), all
    /// serving NFS on `port`.
    pub fn with_locations(mut self, me: Option<String>, port: u16) -> Self {
        let mut all: Vec<(u64, String)> = me
            .into_iter()
            .map(|h| (self.cluster.members.me, h))
            .collect();
        for (id, url) in &self.cluster.members.peers {
            if let Some(host) = url::Url::parse(url)
                .ok()
                .and_then(|u| u.host_str().map(String::from))
                && !all.iter().any(|(_, h)| *h == host)
            {
                all.push((*id, host));
            }
        }
        self.locations = all;
        self.port = port;
        self
    }

    /// `name` as an IP address if it resolves (cached), else as is.
    fn resolve(&self, name: &str) -> String {
        use std::net::ToSocketAddrs;
        if name.parse::<std::net::IpAddr>().is_ok() {
            return name.to_string();
        }
        let mut resolved = self.resolved.lock().unwrap();
        if let Some(ip) = resolved.get(name) {
            return ip.clone();
        }
        match (name, self.port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
        {
            Some(a) => {
                let ip = a.ip().to_string();
                resolved.insert(name.to_string(), ip.clone());
                ip
            }
            None => name.to_string(),
        }
    }

    /// The clients the cluster has recorded (owner strings).
    pub fn records(&self) -> Vec<Vec<u8>> {
        self.cluster
            .nfs_clients()
            .iter()
            .filter_map(|h| unhex(h))
            .collect()
    }

    async fn post<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        node: &str,
        path: &str,
        req: &T,
    ) -> Result<R, String> {
        let url = self
            .url_of(node)
            .ok_or_else(|| format!("no address for {}", node))?;
        let body = bincode::serialize(req).map_err(|e| e.to_string())?;
        let resp = self
            .http
            .post(format!("{}{}", url.trim_end_matches('/'), path))
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("{}: {}", node, resp.status()));
        }
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        bincode::deserialize(&bytes).map_err(|e| e.to_string())
    }

    /// The base URL of node `name` (`node-<id>`).
    fn url_of(&self, name: &str) -> Option<String> {
        let id: u64 = name.strip_prefix("node-")?.parse().ok()?;
        self.cluster.members.peers.get(&id).cloned()
    }
}

#[async_trait]
impl Peers for ClusterPeers {
    fn holder(&self, id: FileId) -> Option<String> {
        let l = self.cluster.lease_state(&object_resource(id))?;
        if l.released || l.expires_ms <= now_ms() {
            return None;
        }
        lease_node(&l.desc).map(String::from)
    }

    async fn forward(&self, node: &str, req: &Forward) -> Result<Forwarded, String> {
        self.post(node, "/api/v1/nfs/forward", req).await
    }
}

#[async_trait]
impl V4Cluster for ClusterPeers {
    fn owner(&self) -> Option<String> {
        let l = self.cluster.lease_state(NFS4)?;
        if l.released || l.expires_ms <= now_ms() {
            return None;
        }
        l.desc.strip_prefix("nfs4 ").map(String::from)
    }

    async fn forward(&self, node: &str, req: &V4Forward) -> Result<V4Forwarded, String> {
        self.post(node, "/api/v1/nfs/v4", req).await
    }

    /// As IP addresses where they resolve (a client's kernel may not be
    /// able to look names up), names otherwise.
    fn locations(&self) -> Vec<String> {
        self.locations
            .iter()
            .map(|(_, h)| self.resolve(h))
            .collect()
    }

    fn devices(&self) -> Vec<v4::Device> {
        self.locations
            .iter()
            .map(|(id, h)| v4::Device {
                id: *id,
                node: format!("node-{}", id),
                addr: self.resolve(h),
                port: self.port,
            })
            .collect()
    }

    async fn record(&self, client: &[u8], present: bool) -> Result<(), String> {
        let Some((owner, epoch)) = self.nfs4.lock().unwrap().clone() else {
            return Err("not the NFSv4 state owner".into());
        };
        let op = LeaseOp::NfsClient {
            client: hex(client),
            present,
            nfs_owner: owner,
            nfs_epoch: epoch,
        };
        match self.leases.call(&op).await? {
            Response::Recorded => Ok(()),
            other => Err(format!("{:?}", other)),
        }
    }
}

/// Hold the `nfs4` lease whenever this node can, and own the NFSv4 state
/// while it does (handing it back if the lease is lost).
pub async fn own_v4_state(server: Arc<v4::Server<Pool>>, peers: Arc<ClusterPeers>, node: String) {
    loop {
        let held =
            match HeldLease::acquire(peers.leases.clone(), NFS4, &format!("nfs4 {}", node)).await {
                Ok(h) => Arc::new(h),
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
        *peers.nfs4.lock().unwrap() = Some((held.owner.clone(), held.epoch));
        let lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = lost.clone();
        held.hold(move |msg| {
            eprintln!("nfs: {}; handing the NFSv4 state over", msg);
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let records = peers.records();
        println!(
            "nfs: this node owns the NFSv4 state (epoch {}; {} client(s) may reclaim)",
            held.epoch,
            records.len()
        );
        server.become_owner(records);
        while !lost.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        server.become_follower();
        *peers.nfs4.lock().unwrap() = None;
    }
}

/// This node's NFS server: the shared backend and the NFSv4 server.
pub type NfsServers = (Arc<Backend<Pool>>, Arc<v4::Server<Pool>>);

/// This node's NFS server, if it runs one (for forwarded requests).
pub struct NfsForwarding(pub Option<NfsServers>);

#[post("/api/v1/nfs/forward", data = "<body>")]
async fn forward(
    _auth: crate::auth::IsNode,
    body: Data<'_>,
    st: &State<NfsForwarding>,
) -> Result<Vec<u8>, (Status, String)> {
    let Some((backend, _)) = &st.0 else {
        return Err((Status::ServiceUnavailable, "no NFS server here".into()));
    };
    let bytes = body
        .open(MAX_FORWARD.bytes())
        .into_bytes()
        .await
        .map_err(|e| (Status::BadRequest, e.to_string()))?;
    if !bytes.is_complete() {
        return Err((
            Status::PayloadTooLarge,
            "forwarded request too large".into(),
        ));
    }
    let req: Forward =
        bincode::deserialize(&bytes).map_err(|e| (Status::BadRequest, e.to_string()))?;
    let reply = backend.serve_forward(req).await;
    bincode::serialize(&reply).map_err(|e| (Status::InternalServerError, e.to_string()))
}

#[post("/api/v1/nfs/v4", data = "<body>")]
async fn forward_v4(
    _auth: crate::auth::IsNode,
    body: Data<'_>,
    st: &State<NfsForwarding>,
) -> Result<Vec<u8>, (Status, String)> {
    let Some((_, server)) = &st.0 else {
        return Err((Status::ServiceUnavailable, "no NFS server here".into()));
    };
    let bytes = body
        .open(MAX_FORWARD.bytes())
        .into_bytes()
        .await
        .map_err(|e| (Status::BadRequest, e.to_string()))?;
    if !bytes.is_complete() {
        return Err((
            Status::PayloadTooLarge,
            "forwarded request too large".into(),
        ));
    }
    let req: V4Forward =
        bincode::deserialize(&bytes).map_err(|e| (Status::BadRequest, e.to_string()))?;
    let reply = server.serve_forward(req).await;
    bincode::serialize(&reply).map_err(|e| (Status::InternalServerError, e.to_string()))
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![forward, forward_v4]
}
