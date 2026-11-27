//! The accept loop: TLS (or not), then hyper serving an axum `Router`,
//! each request carrying its connection's [`Peer`] and address.

use super::{Connections, LiveCert, LiveTrust, Peer};
use axum::Router;
use axum::extract::ConnectInfo;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceExt;

/// What a TLS server swaps and closes at runtime.
#[derive(Clone)]
pub struct Server {
    pub cert: Arc<LiveCert>,
    pub trust: Arc<LiveTrust>,
    pub conns: Arc<Connections>,
}

impl Server {
    /// `conns`: the registry a revocation closes connections through
    /// (the node's `crate::trust::Trust`'s; a fresh one if None).
    pub fn new(
        cert: Arc<LiveCert>,
        trust: Arc<LiveTrust>,
        conns: Option<Arc<Connections>>,
    ) -> Server {
        Server {
            cert,
            trust,
            conns: conns.unwrap_or_default(),
        }
    }

    /// From a `tls:` config: its certificate, and its bundle for clients
    /// (who may present a certificate, or come with a token instead).
    pub fn from_config(tls: &crate::tls::TlsConfig) -> Result<Server, String> {
        let (chain, key) = tls
            .identity()?
            .ok_or("tls: serving needs this node's cert.pem and key.pem")?;
        Ok(Server::new(
            LiveCert::new(chain, key)?,
            LiveTrust::new(tls.roots()?, false, tls.trust.clone())?,
            tls.trust.as_ref().map(|t| t.connections().clone()),
        ))
    }

    /// The rustls config: built once, reading `cert` and `trust` per
    /// handshake.
    pub fn tls_config(&self) -> Arc<rustls::ServerConfig> {
        crate::tls::install_provider();
        let mut c = rustls::ServerConfig::builder()
            .with_client_cert_verifier(self.trust.clone())
            .with_cert_resolver(self.cert.clone());
        c.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Arc::new(c)
    }
}

/// How long open connections get to finish their requests once the server
/// stops, before they're dropped.
pub const GRACE: std::time::Duration = std::time::Duration::from_secs(3);

/// Serve `app` on `listener` until `shutdown` resolves: HTTPS with `tls`,
/// else plain HTTP. Then it stops accepting, and each open connection
/// finishes what it's serving and closes (dropped after [`GRACE`]);
/// returns once they all have.
pub async fn serve(
    listener: TcpListener,
    tls: Option<Server>,
    app: Router,
    shutdown: impl Future<Output = ()>,
) {
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let accepting = async move {
        match tls {
            Some(server) => {
                let acceptor = tokio_rustls::TlsAcceptor::from(server.tls_config());
                let conns = server.conns.clone();
                crate::net::accept_loop(listener, "https", move |sock, addr| {
                    let acceptor = acceptor.clone();
                    let conns = conns.clone();
                    let app = app.clone();
                    let stop = stop.clone();
                    async move {
                        let Ok(tls) = crate::net::tls_accept(&acceptor, sock).await else {
                            return;
                        };
                        let peer: Option<Peer> = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first())
                            .and_then(|d| conns.register(d.clone().into_owned()));
                        connection(tls, addr, peer, app, stop).await;
                    }
                })
                .await
            }
            None => {
                crate::net::accept_loop(listener, "http", move |sock, addr| {
                    connection(sock, addr, None, app.clone(), stop.clone())
                })
                .await
            }
        }
    };
    tokio::select! {
        _ = accepting => {}
        _ = shutdown => {}
    }
    let _ = stop_tx.send(true);
    // Every connection holds a receiver: closed when the last one ends.
    let _ = tokio::time::timeout(GRACE * 2, stop_tx.closed()).await;
}

async fn connection<S>(
    io: S,
    addr: SocketAddr,
    peer: Option<Peer>,
    app: Router,
    mut stop: tokio::sync::watch::Receiver<bool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let p = peer.clone();
    let svc = hyper::service::service_fn(move |mut req: hyper::Request<Incoming>| {
        if let Some(p) = &p {
            req.extensions_mut().insert(p.clone());
        }
        req.extensions_mut().insert(ConnectInfo(addr));
        app.clone().oneshot(req)
    });
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), svc);
    tokio::pin!(conn);
    let revoked = async {
        match &peer {
            Some(p) => p.closed().await,
            None => std::future::pending().await,
        }
    };
    // Dropping the connection future closes the socket; requests still
    // running on it are dropped with it.
    tokio::select! {
        _ = conn.as_mut() => return,
        _ = revoked => return,
        _ = stop.wait_for(|s| *s) => {}
    }
    // The server is stopping: finish what's in flight, then close.
    conn.as_mut().graceful_shutdown();
    let _ = tokio::time::timeout(GRACE, conn).await;
}
