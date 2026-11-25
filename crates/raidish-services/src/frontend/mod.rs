//! The service front ends: etcd (`crate::etcd`), S3 (`crate::s3`) and SQS
//! (`crate::sqs`) datasets, reachable through every node.
//!
//! **Where a dataset runs.** Like a volume: on one node at a time, the
//! holder of its `ds/<name>` lease, placed by the agents
//! (`raidish::agent`) among the live nodes with a `services` section, by
//! the dataset's `nodes` setting. That node runs it as its writer
//! (intent log, lent space) and serves it.
//!
//! **How clients reach it.** Through any node:
//!
//! * etcd: every node listens on each etcd dataset's port, TLS with a
//!   client certificate from the cluster's CA required (its role: admin
//!   and node may write, readonly only reads). A connection to a node that
//!   doesn't run the dataset is forwarded to the one that does: over TLS
//!   between the nodes (this node's certificate), starting with a
//!   `RAIDISH-FWD/1 <dataset> <role>` line (the client's role, believed only
//!   from a node) so it's never forwarded again.
//! * S3 and SQS: every node listens on one HTTPS port for each (requests
//!   are signed with access keys made from tokens, see [`keys`]). A
//!   request for a bucket or queue another node runs is proxied there over
//!   TLS, headers (`Host` too) and body unchanged, so its signature still
//!   holds; the proxied request carries `x-raidish-forwarded` so it's
//!   never proxied again. Listing and creating buckets and queues is
//!   answered by any node.
//!
//! Who runs what comes from the leases (the holder's description names
//! its address: `agent <node> @<address>`), read live by each agent tick.

pub mod http;
pub mod keys;
pub mod routes;

use crate::committer::PoolAdmin;
use crate::dataset::DatasetFormat;
use crate::etcd::server::{Caller, EtcdConn};
use crate::etcd::store::EtcdStore;
use crate::fs::{FileSystem, FileSystemError};
use crate::pki::Role;
use crate::types::Epoch;
use crate::vdev::Pool;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// TLS for the service ports.
pub struct ServiceTls {
    /// S3 and SQS: HTTPS with this node's certificate (requests are
    /// authenticated by their signatures).
    pub https: TlsAcceptor,
    /// etcd: a client certificate from the cluster's CA is required.
    pub etcd: TlsAcceptor,
    /// To other nodes (proxying), as this node.
    pub connector: TlsConnector,
}

impl ServiceTls {
    pub fn new(cfg: &crate::tls::TlsConfig) -> Result<Self, String> {
        Ok(ServiceTls {
            https: TlsAcceptor::from(Arc::new(cfg.server_config(false, &[b"h2", b"http/1.1"])?)),
            etcd: TlsAcceptor::from(Arc::new(cfg.server_config(true, &[b"h2"])?)),
            connector: TlsConnector::from(Arc::new(cfg.client_config()?)),
        })
    }

    /// A TLS connection to `addr:port`, as this node.
    pub async fn connect(
        &self,
        addr: &str,
        port: u16,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let sock = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect((addr, port)))
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
        let _ = sock.set_nodelay(true);
        let name = rustls::pki_types::ServerName::try_from(addr.to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        tokio::time::timeout(Duration::from_secs(10), self.connector.connect(name, sock))
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
    }
}

pub use crate::tls::peer_role;

/// What a forwarded etcd connection starts with, with this forwarding's
/// version. Bump it with any change to the line.
pub const FORWARD_PREAMBLE: &[u8] = b"RAIDISH-FWD/1 ";
/// The header marking a proxied S3 / SQS request.
pub const FORWARDED_HEADER: &str = "x-raidish-forwarded";

/// A configured port: unset is `default`, 0 is off (None).
fn on(port: Option<u16>, default: u16) -> Option<u16> {
    match port.unwrap_or(default) {
        0 => None,
        p => Some(p),
    }
}

/// The `services` section of the config.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ServicesConfig {
    /// Address the service ports listen on (default 0.0.0.0).
    pub bind: Option<String>,
    /// How other nodes and clients reach this node (default: the host
    /// name).
    pub advertise: Option<String>,
    /// The S3 port (default 9000; 0: none).
    pub s3_port: Option<u16>,
    /// Buckets as host names: `<bucket>.<s3_domain>` (as well as paths).
    pub s3_domain: Option<String>,
    /// The SQS port (default 9324; 0: none).
    pub sqs_port: Option<u16>,
    /// The region S3 and SQS requests are signed for (default us-east-1).
    pub region: Option<String>,
}

impl ServicesConfig {
    pub fn bind(&self) -> String {
        self.bind.clone().unwrap_or_else(|| "0.0.0.0".into())
    }
    /// The S3 port; None: no S3 here.
    pub fn s3_port(&self) -> Option<u16> {
        on(self.s3_port, 9000)
    }
    /// The SQS port; None: no SQS here.
    pub fn sqs_port(&self) -> Option<u16> {
        on(self.sqs_port, 9324)
    }
    pub fn region(&self) -> String {
        self.region.clone().unwrap_or_else(|| "us-east-1".into())
    }
    /// This node's address, as others reach it.
    pub fn advertised(&self, node: &str) -> String {
        if let Some(a) = &self.advertise {
            return a.clone();
        }
        std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| node.to_string())
    }
}

/// Who runs a dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub node: String,
    /// Its advertised address.
    pub addr: String,
}

/// A service dataset, as the cluster has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub format: DatasetFormat,
    /// An etcd dataset's port.
    pub port: Option<u16>,
    pub created_ms: u64,
    pub owner: Option<Owner>,
}

/// Where a dataset a request names is served, as this node sees it.
pub enum Located<T> {
    /// Here: what `Frontend::locate`'s `pick` took from it.
    Here(T),
    /// At another node: pass the request on.
    There(Owner),
    /// It exists, but isn't served anywhere this request can go right now
    /// (starting up, moving, or already passed on once).
    Unavailable,
    /// No such dataset (of that format).
    Missing,
}

/// A dataset served here.
pub enum Local {
    Etcd {
        store: Arc<EtcdStore<Pool>>,
        /// Connections for its gRPC server.
        conns: mpsc::Sender<std::io::Result<EtcdConn>>,
    },
    S3(Arc<crate::s3::Bucket<Pool>>),
    Queue(Arc<crate::sqs::Queue<Pool>>),
}

impl Local {
    pub fn bucket(&self) -> Option<Arc<crate::s3::Bucket<Pool>>> {
        match self {
            Local::S3(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn queue(&self) -> Option<Arc<crate::sqs::Queue<Pool>>> {
        match self {
            Local::Queue(q) => Some(q.clone()),
            _ => None,
        }
    }

    /// An etcd dataset's connection queue.
    pub fn etcd_conns(&self) -> Option<mpsc::Sender<std::io::Result<EtcdConn>>> {
        match self {
            Local::Etcd { conns, .. } => Some(conns.clone()),
            _ => None,
        }
    }
}

impl Local {
    fn stop(&self) {
        match self {
            Local::Etcd { store, .. } => store.stop(),
            Local::S3(b) => b.stop(),
            Local::Queue(q) => q.stop(),
        }
    }
}

#[derive(Default)]
struct Routing {
    routes: BTreeMap<String, Route>,
    /// The live nodes serving: name → address.
    nodes: BTreeMap<String, String>,
}

/// This node's front ends.
pub struct Frontend {
    node: String,
    config: ServicesConfig,
    tls: ServiceTls,
    /// Checks S3/SQS requests' access keys (see [`keys`]).
    auth: keys::ServiceAuth,
    committer: crate::committer::Admin,
    routing: RwLock<Routing>,
    local: RwLock<BTreeMap<String, Arc<Local>>>,
    etcd_ports: Mutex<BTreeMap<u16, tokio::task::JoinHandle<()>>>,
    /// Signalled when the routes change (a waiting CreateBucket...).
    changed: tokio::sync::Notify,
}

/// An etcd member id for a node.
pub fn member_id(node: &str) -> u64 {
    u64::from_le_bytes(
        blake3::hash(node.as_bytes()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    )
}

impl Frontend {
    /// The services and their ports.
    pub fn config(&self) -> &ServicesConfig {
        &self.config
    }

    pub fn tls(&self) -> &ServiceTls {
        &self.tls
    }

    /// Checks S3/SQS requests' access keys.
    pub fn auth(&self) -> &keys::ServiceAuth {
        &self.auth
    }

    /// The pool's metadata operations (buckets and queues are datasets).
    pub fn committer(&self) -> &crate::committer::Admin {
        &self.committer
    }

    pub fn new(
        node: String,
        config: ServicesConfig,
        auth: keys::ServiceAuth,
        tls: ServiceTls,
        committer: crate::committer::Admin,
    ) -> Arc<Self> {
        Arc::new(Frontend {
            node,
            config,
            tls,
            auth,
            committer,
            routing: Default::default(),
            local: Default::default(),
            etcd_ports: Default::default(),
            changed: Default::default(),
        })
    }

    /// This node's address.
    pub fn addr(&self) -> String {
        self.config.advertised(&self.node)
    }

    /// Start the S3 and SQS listeners.
    pub async fn start(self: &Arc<Self>) -> std::io::Result<()> {
        let bind = self.config.bind();
        if let Some(port) = self.config.s3_port() {
            let l = TcpListener::bind((bind.as_str(), port)).await?;
            println!("s3: serving on {}", l.local_addr()?);
            tokio::spawn(crate::s3::http::serve(self.clone(), l));
        }
        if let Some(port) = self.config.sqs_port() {
            let l = TcpListener::bind((bind.as_str(), port)).await?;
            println!("sqs: serving on {}", l.local_addr()?);
            tokio::spawn(crate::sqs::http::serve(self.clone(), l));
        }
        Ok(())
    }

    /// The service datasets and the nodes serving, as of now.
    pub fn set_routes(
        self: &Arc<Self>,
        routes: BTreeMap<String, Route>,
        nodes: BTreeMap<String, String>,
    ) {
        let ports: Vec<u16> = routes
            .values()
            .filter(|r| r.format == DatasetFormat::Etcd)
            .filter_map(|r| r.port)
            .collect();
        {
            let mut g = self.routing.write().unwrap();
            if g.routes == routes && g.nodes == nodes {
                return self.listen_etcd(&ports);
            }
            g.routes = routes;
            g.nodes = nodes;
        }
        self.changed.notify_waiters();
        self.listen_etcd(&ports);
    }

    pub fn route(&self, name: &str) -> Option<Route> {
        self.routing.read().unwrap().routes.get(name).cloned()
    }

    pub fn routes(&self) -> BTreeMap<String, Route> {
        self.routing.read().unwrap().routes.clone()
    }

    pub fn nodes(&self) -> BTreeMap<String, String> {
        self.routing.read().unwrap().nodes.clone()
    }

    pub fn local(&self, name: &str) -> Option<Arc<Local>> {
        self.local.read().unwrap().get(name).cloned()
    }

    /// Where `name` (a `format` dataset) is served: here (`pick` takes
    /// what the request needs from it), at another node, nowhere right
    /// now, or not at all. `forwarded`: a node already passed this request
    /// on, so it isn't passed on again (it would only bounce).
    pub fn locate<T>(
        &self,
        name: &str,
        format: DatasetFormat,
        forwarded: bool,
        pick: impl FnOnce(&Local) -> Option<T>,
    ) -> Located<T> {
        if let Some(t) = self.local(name).as_deref().and_then(pick) {
            return Located::Here(t);
        }
        let Some(route) = self.route(name).filter(|r| r.format == format) else {
            return Located::Missing;
        };
        match route.owner {
            Some(o) if o.node != self.node && !forwarded => Located::There(o),
            _ => Located::Unavailable,
        }
    }

    /// Listen on every etcd dataset's port (and no others).
    fn listen_etcd(self: &Arc<Self>, ports: &[u16]) {
        let mut l = self.etcd_ports.lock().unwrap();
        l.retain(|p, task| {
            let keep = ports.contains(p);
            if !keep {
                task.abort();
            }
            keep
        });
        for &port in ports {
            if l.contains_key(&port) {
                continue;
            }
            let fe = self.clone();
            l.insert(
                port,
                tokio::spawn(async move {
                    let bind = fe.config.bind();
                    let listener = crate::net::bind_retrying(&bind, port, "etcd").await;
                    println!("etcd: listening on {}:{}", bind, port);
                    crate::net::accept_loop(listener, "etcd", move |sock, _| {
                        let fe = fe.clone();
                        async move { fe.etcd_conn(port, sock).await }
                    })
                    .await
                }),
            );
        }
    }

    /// A connection to an etcd port: served here, or forwarded.
    async fn etcd_conn(&self, port: u16, sock: TcpStream) {
        let Ok(tls) = crate::net::tls_accept(&self.tls.etcd, sock).await else {
            return;
        };
        let Some(peer) = peer_role(&tls) else {
            return; // no raidish role: nothing allowed
        };
        let (io, forwarded) = match read_preamble(tls).await {
            Ok(x) => x,
            Err(_) => return,
        };
        // Forwarded by a node, for the client it's forwarding.
        let role = match forwarded {
            Some(_) if peer != Role::Node => return,
            Some(r) => r,
            None => peer,
        };
        // The etcd dataset on this port.
        let Some(name) = self
            .routing
            .read()
            .unwrap()
            .routes
            .iter()
            .find(|(_, r)| r.format == DatasetFormat::Etcd && r.port == Some(port))
            .map(|(n, _)| n.clone())
        else {
            return;
        };
        let owner = match self.locate(
            &name,
            DatasetFormat::Etcd,
            forwarded.is_some(),
            Local::etcd_conns,
        ) {
            Located::Here(conns) => {
                let _ = conns.send(Ok(EtcdConn::new(io, Caller { role }))).await;
                return;
            }
            Located::There(owner) => owner,
            // Not (or no longer) here, or nowhere yet: the client reconnects.
            Located::Unavailable | Located::Missing => return,
        };
        let Ok(mut up) = self.tls.connect(&owner.addr, port).await else {
            return;
        };
        let line = format!(
            "{}{} {}\n",
            String::from_utf8_lossy(FORWARD_PREAMBLE),
            name,
            role
        );
        if up.write_all(line.as_bytes()).await.is_err() {
            return;
        }
        let mut io = io;
        let _ = tokio::io::copy_bidirectional(&mut io, &mut up).await;
    }

    /// Serve dataset `name` here, from `fs` (this node's writer, which runs
    /// it under its `ds/<name>` lease of epoch `epoch`).
    pub async fn serve_local(
        self: &Arc<Self>,
        name: &str,
        format: DatasetFormat,
        fs: Arc<FileSystem<Pool>>,
        epoch: Epoch,
    ) -> Result<(), FileSystemError> {
        let local = match format {
            DatasetFormat::Etcd => {
                let store = EtcdStore::open(fs, name, member_id(&self.node), epoch.get())
                    .await
                    .map_err(|e| FileSystemError::Unavailable(e.to_string()))?;
                let port = self.route(name).and_then(|r| r.port).unwrap_or(0);
                let urls = self
                    .nodes()
                    .values()
                    .map(|a| format!("https://{}:{}", a, port))
                    .collect();
                let (tx, rx) = mpsc::channel(64);
                let router = crate::etcd::server::EtcdServer {
                    store: store.clone(),
                    node: self.node.clone(),
                    client_urls: urls,
                }
                .router();
                let mut stopped = store.stopped();
                let label = name.to_string();
                tokio::spawn(async move {
                    let r = router
                        .serve_with_incoming_shutdown(
                            tokio_stream::wrappers::ReceiverStream::<std::io::Result<EtcdConn>>::new(rx),
                            async move {
                                while !*stopped.borrow_and_update() {
                                    if stopped.changed().await.is_err() {
                                        return;
                                    }
                                }
                            },
                        )
                        .await;
                    if let Err(e) = r {
                        eprintln!("etcd '{}': {}", label, e);
                    }
                });
                Local::Etcd { store, conns: tx }
            }
            DatasetFormat::S3 => Local::S3(crate::s3::Bucket::open(fs, name).await?),
            DatasetFormat::Queue => Local::Queue(crate::sqs::Queue::open(fs, name).await?),
            f => {
                return Err(FileSystemError::InvalidArgument(format!(
                    "{} datasets aren't services",
                    f.name()
                )));
            }
        };
        if let Some(old) = self
            .local
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::new(local))
        {
            old.stop();
        }
        self.changed.notify_waiters();
        Ok(())
    }

    /// Stop serving `name` here.
    pub fn stop_local(&self, name: &str) {
        if let Some(l) = self.local.write().unwrap().remove(name) {
            l.stop();
        }
    }

    /// Delete service dataset `name`: stopped everywhere (disabled, and
    /// waited for), then deleted. False: there's none. On failure it's
    /// served again.
    pub async fn delete_service(&self, name: &str) -> Result<bool, String> {
        let Some(props) = self
            .committer
            .datasets()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .find(|d| d.name == name)
            .map(|d| d.props)
        else {
            return Ok(false);
        };
        let disabled = crate::inode::DatasetProps {
            enabled: false,
            ..props.clone()
        };
        // Enabled again unless deleted, however this ends: disabled and
        // left so (an error, or the request cancelled), it stayed down for
        // good (I-35).
        let mut reenable = Reenable {
            committer: self.committer.clone(),
            name: name.to_string(),
            props: Some(props.clone()),
        };
        self.committer
            .set_props(name, &disabled)
            .await
            .map_err(|e| e.to_string())?;
        let mut last = String::new();
        for _ in 0..30 {
            self.wait_routes(2, |routes| {
                routes.get(name).is_none_or(|r| r.owner.is_none())
            })
            .await;
            match self.committer.delete_dataset(name).await {
                Ok(()) => {
                    // Gone from this node's routes now (the agents' next
                    // round says so everywhere).
                    reenable.props = None;
                    self.routing.write().unwrap().routes.remove(name);
                    self.changed.notify_waiters();
                    return Ok(true);
                }
                Err(e) => last = e.to_string(),
            }
        }
        reenable.props = None;
        let _ = self.committer.set_props(name, &props).await;
        Err(format!("deleting {}: {}", name, last))
    }

    /// Wait (up to `secs`) until `cond` holds of the routes.
    pub async fn wait_routes(
        &self,
        secs: u64,
        cond: impl Fn(&BTreeMap<String, Route>) -> bool,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let wait = self.changed.notified();
            if cond(&self.routing.read().unwrap().routes) {
                return true;
            }
            if tokio::time::timeout_at(deadline, wait).await.is_err() {
                return cond(&self.routing.read().unwrap().routes);
            }
        }
    }
}

/// Puts a service's props back (enabling it again) when dropped with
/// `props` set: `delete_service` disabled it and didn't get to delete it.
struct Reenable {
    committer: crate::committer::Admin,
    name: String,
    props: Option<crate::inode::DatasetProps>,
}

impl Drop for Reenable {
    fn drop(&mut self) {
        let Some(props) = self.props.take() else {
            return;
        };
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            eprintln!("services: '{}' left disabled (no runtime)", self.name);
            return;
        };
        let (committer, name) = (self.committer.clone(), self.name.clone());
        rt.spawn(async move {
            if let Err(e) = committer.set_props(&name, &props).await {
                eprintln!("services: enabling '{}' again: {}", name, e);
            }
        });
    }
}

/// A connection with some bytes already read off it, given back first.
pub struct Prefixed<S> {
    pre: Vec<u8>,
    pos: usize,
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos < self.pre.len() {
            let n = (self.pre.len() - self.pos).min(buf.remaining());
            let pos = self.pos;
            buf.put_slice(&self.pre[pos..pos + n]);
            self.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Read a connection's start: the forwarding line (and the role it
/// names), or the client's own first bytes (given back). Clients speak
/// first (HTTP/2's preface is longer than the preamble).
async fn read_preamble<S: AsyncRead + Unpin>(
    mut sock: S,
) -> std::io::Result<(Prefixed<S>, Option<Role>)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut buf = vec![0u8; FORWARD_PREAMBLE.len()];
    tokio::time::timeout_at(deadline, sock.read_exact(&mut buf))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
    if buf != FORWARD_PREAMBLE {
        // Another version's forwarding line: refused, not handed to the
        // service as a client's bytes.
        if buf.starts_with(b"RAIDISH-FWD") {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                eprintln!(
                    "services: refusing a forwarded connection that starts {:?}: this node \
                     speaks {:?} (every node must run the same raidish)",
                    String::from_utf8_lossy(&buf),
                    String::from_utf8_lossy(FORWARD_PREAMBLE)
                )
            });
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        return Ok((
            Prefixed {
                pre: buf,
                pos: 0,
                inner: sock,
            },
            None,
        ));
    }
    // The rest of the line: the dataset and the client's role.
    let mut line = Vec::new();
    loop {
        let b = tokio::time::timeout_at(deadline, sock.read_u8())
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
        if b == b'\n' {
            break;
        }
        line.push(b);
        if line.len() > 300 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
    }
    let line = String::from_utf8_lossy(&line).into_owned();
    let role: Role = line
        .rsplit(' ')
        .next()
        .unwrap_or("")
        .parse()
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
    Ok((
        Prefixed {
            pre: Vec::new(),
            pos: 0,
            inner: sock,
        },
        Some(role),
    ))
}

/// A CA, a node certificate (`raidish_auth::testing::Pki`), a service
/// key and a frontend using them.
#[cfg(test)]
pub mod testing {
    use super::*;

    pub struct Fixture {
        pub pki: raidish_auth::testing::Pki,
        pub service_key: Vec<u8>,
    }

    /// The PKI's fields and methods (`dir`, `tls`, `verifier`, `http`).
    impl std::ops::Deref for Fixture {
        type Target = raidish_auth::testing::Pki;
        fn deref(&self) -> &Self::Target {
            &self.pki
        }
    }

    impl Fixture {
        pub fn new(revoked: Vec<String>) -> Self {
            Fixture {
                pki: raidish_auth::testing::Pki::new(revoked),
                service_key: vec![7; 32],
            }
        }

        pub fn frontend(&self) -> Arc<Frontend> {
            self.frontend_with(Default::default())
        }

        /// `frontend`, with `slot` as its committer.
        pub fn frontend_with(&self, slot: crate::committer::CommitterSlot) -> Arc<Frontend> {
            let config = ServicesConfig {
                advertise: Some("127.0.0.1".into()),
                ..Default::default()
            };
            let k = self.service_key.clone();
            let auth = keys::ServiceAuth::new(
                "us-east-1".into(),
                self.verifier.clone(),
                Arc::new(move || Some(k.clone())),
            );
            Frontend::new(
                "n1".into(),
                config,
                auth,
                ServiceTls::new(&self.tls).unwrap(),
                crate::committer::Admin::new(slot, crate::committer::CommitClient::new(vec![])),
            )
        }

        /// An access key and secret with `role`.
        pub fn key(&self, role: Role) -> (String, String) {
            keys::mint(
                &self.service_key,
                &keys::AccessKey {
                    id: keys::random_id(),
                    role,
                    exp: None,
                    internal: false,
                },
            )
            .unwrap()
        }
    }
}
