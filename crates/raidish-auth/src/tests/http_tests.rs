//! The HTTP-1 spike's checks: live certificate and trust swaps, the peer
//! certificate in requests, a WebSocket route, and closing a serial's
//! connections and streams.

use crate::http::{LiveIdentity, LiveTrust, Peer, Server, serve};
use crate::pki::{Ca, Role};
use crate::testing::Pki;
use crate::tls::TlsConfig;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Router};
use futures_util::{SinkExt, StreamExt};
use raidish_types::pki::Serial;
use rustls::pki_types::{CertificateDer, ServerName};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::Connector;

async fn whoami(peer: Option<Extension<Peer>>) -> String {
    match peer {
        Some(Extension(p)) => p.serial().to_string(),
        None => "anonymous".into(),
    }
}

async fn ws(up: WebSocketUpgrade, Extension(peer): Extension<Peer>) -> Response {
    up.on_upgrade(move |s| echo(s, peer))
}

/// Echo until the client goes or the peer's connections are closed.
async fn echo(mut s: WebSocket, peer: Peer) {
    loop {
        tokio::select! {
            m = s.recv() => match m {
                Some(Ok(Message::Text(t))) => {
                    if s.send(Message::Text(t)).await.is_err() {
                        return;
                    }
                }
                Some(Ok(_)) => {}
                _ => return,
            },
            _ = peer.closed() => {
                let _ = s.send(Message::Close(None)).await;
                return;
            }
        }
    }
}

fn app() -> Router {
    Router::new()
        .route("/whoami", get(whoami))
        .route("/ws", get(ws))
}

fn tls(dir: &Path) -> TlsConfig {
    TlsConfig {
        dir: Some(dir.to_string_lossy().into()),
        ..Default::default()
    }
}

/// A client trusting `roots_dir`'s CA and presenting `who`'s certificate.
fn client_cfg(roots_dir: &Path, who: &Path) -> Arc<rustls::ClientConfig> {
    crate::tls::install_provider();
    let mut roots = rustls::RootCertStore::empty();
    for c in tls(roots_dir).roots().unwrap() {
        roots.add(c).unwrap();
    }
    let (chain, key) = tls(who).identity().unwrap().unwrap();
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .unwrap(),
    )
}

fn http(cfg: &rustls::ClientConfig) -> reqwest::Client {
    reqwest::Client::builder()
        .use_preconfigured_tls(cfg.clone())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

fn serial_of(dir: &Path) -> Serial {
    let (chain, _) = tls(dir).identity().unwrap().unwrap();
    crate::ca::facts(&chain[0]).unwrap().serial
}

struct Running {
    pki: Pki,
    server: Server,
    port: u16,
}

impl Running {
    fn dir(&self, name: &str) -> std::path::PathBuf {
        self.pki.dir.join(name)
    }
    fn url(&self, path: &str) -> String {
        format!("https://127.0.0.1:{}{}", self.port, path)
    }
    fn cfg(&self, who: &str) -> Arc<rustls::ClientConfig> {
        client_cfg(&self.dir("n1"), &self.dir(who))
    }
    /// A TLS connection as `who`, nothing sent yet.
    async fn raw(&self, who: &str) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        let sock = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .unwrap();
        tokio_rustls::TlsConnector::from(self.cfg(who))
            .connect(ServerName::try_from("127.0.0.1").unwrap(), sock)
            .await
            .unwrap()
    }
    async fn ws(
        &self,
        who: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let url = format!("wss://127.0.0.1:{}/ws", self.port);
        let (s, _) = tokio_tungstenite::connect_async_tls_with_config(
            url,
            None,
            false,
            Some(Connector::Rustls(self.cfg(who))),
        )
        .await
        .unwrap();
        s
    }
}

async fn start() -> Running {
    let pki = Pki::new(vec![]);
    let (chain, key) = pki.tls.identity().unwrap().unwrap();
    let server = Server::new(
        LiveIdentity::new(chain, key).unwrap(),
        LiveTrust::new(pki.tls.roots().unwrap(), false, None).unwrap(),
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(serve(
        listener,
        Some(server.clone()),
        app(),
        std::future::pending(),
    ));
    Running { pki, server, port }
}

/// One HTTP/1.1 GET over an open TLS stream: the status line.
async fn get_over<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    s: &mut S,
    path: &str,
) -> String {
    s.write_all(format!("GET {} HTTP/1.1\r\nHost: x\r\n\r\n", path).as_bytes())
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_cert_reaches_the_handler() {
    let r = start().await;
    let got = http(&r.cfg("admin"))
        .get(r.url("/whoami"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(got, serial_of(&r.dir("admin")).to_string());

    // Over HTTP/2 too (ALPN h2), with the same peer.
    let mut h2 = (*r.cfg("admin")).clone();
    h2.alpn_protocols = vec![b"h2".to_vec()];
    let resp = http(&h2).get(r.url("/whoami")).send().await.unwrap();
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert_eq!(
        resp.text().await.unwrap(),
        serial_of(&r.dir("admin")).to_string()
    );

    // No client cert: still served (a token would authenticate it).
    let anon = rustls::ClientConfig::builder()
        .with_root_certificates({
            let mut s = rustls::RootCertStore::empty();
            for c in r.pki.tls.roots().unwrap() {
                s.add(c).unwrap();
            }
            s
        })
        .with_no_client_auth();
    let got = http(&anon)
        .get(r.url("/whoami"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(got, "anonymous");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_cert_swaps_for_new_connections_only() {
    let r = start().await;
    let ca = Ca::load(&r.pki.dir).unwrap();
    ca.issue("n1b", Role::Node, &[], None, false).unwrap();

    let leaf =
        |s: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>| -> CertificateDer<'static> {
            s.get_ref().1.peer_certificates().unwrap()[0]
                .clone()
                .into_owned()
        };
    let mut before = r.raw("admin").await;
    assert_eq!(leaf(&before), r.server.cert.leaf());
    let old = leaf(&before);

    let (chain, key) = tls(&r.dir("n1b")).identity().unwrap().unwrap();
    r.server.cert.set(chain.clone(), key).unwrap();

    let after = r.raw("admin").await;
    assert_eq!(leaf(&after), chain[0]);
    assert_ne!(leaf(&after), old);
    // The connection opened before the swap still works, on the old cert.
    assert_eq!(get_over(&mut before, "/whoami").await, "HTTP/1.1 200 OK");
}

#[tokio::test(flavor = "multi_thread")]
async fn trust_swaps_at_runtime() {
    let r = start().await;
    let other = tempfile::tempdir().unwrap();
    Ca::init(other.path(), "other", None, false).unwrap();
    Ca::load(other.path())
        .unwrap()
        .issue("stranger", Role::Admin, &[], None, false)
        .unwrap();
    let stranger = http(&client_cfg(&r.dir("n1"), &other.path().join("stranger")));

    assert!(stranger.get(r.url("/whoami")).send().await.is_err());

    let mut roots = r.pki.tls.roots().unwrap();
    roots.extend(tls(&other.path().join("stranger")).roots().unwrap());
    r.server.trust.set(roots).unwrap();
    let got = stranger
        .get(r.url("/whoami"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(got, serial_of(&other.path().join("stranger")).to_string());

    // And back: refused again.
    r.server.trust.set(r.pki.tls.roots().unwrap()).unwrap();
    let fresh = http(&client_cfg(&r.dir("n1"), &other.path().join("stranger")));
    assert!(fresh.get(r.url("/whoami")).send().await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_a_serial_ends_its_connections_and_websockets() {
    let r = start().await;
    let admin = serial_of(&r.dir("admin"));
    let node = serial_of(&r.dir("n1"));

    let mut a_ws = r.ws("admin").await;
    let mut n_ws = r.ws("n1").await;
    let mut a_idle = r.raw("admin").await;
    // The idle connection registers once its handshake is done server-side.
    for _ in 0..50 {
        if r.server.conns.count(&admin) == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        r.server.conns.count(&admin),
        2,
        "the WebSocket and the idle connection"
    );

    a_ws.send("hi".into()).await.unwrap();
    assert_eq!(
        a_ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "hi"
    );

    assert_eq!(r.server.conns.close(&admin), 2);

    // The WebSocket gets a close frame and ends.
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match a_ws.next().await {
                Some(Ok(m)) if m.is_close() => return,
                Some(Ok(_)) => {}
                _ => return,
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "admin's WebSocket still open");

    // The idle connection is closed by the server.
    let mut b = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), a_idle.read(&mut b))
        .await
        .expect("admin's idle connection still open");
    assert!(matches!(n, Ok(0) | Err(_)));

    // Nobody else's.
    n_ws.send("still here".into()).await.unwrap();
    assert_eq!(
        n_ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "still here"
    );
    for _ in 0..50 {
        if r.server.conns.count(&admin) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(r.server.conns.count(&admin), 0);
    assert_eq!(r.server.conns.count(&node), 1);

    // A new connection with the same cert still gets in: closing isn't
    // refusing (that's the verifier's job, CA-2).
    let got = http(&r.cfg("admin"))
        .get(r.url("/whoami"))
        .send()
        .await
        .unwrap();
    assert!(got.status().is_success());
}
