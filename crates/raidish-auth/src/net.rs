//! Serving TCP: one accept loop for every listener (NBD, NVMe/TCP, peer
//! I/O, NFS, S3/SQS, etcd), so each gets the same handling of what goes
//! wrong around a connection rather than its own.

use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};

/// `http://host:port` of the `listen` node serving a remote disk path
/// (`http://host:port/api/v1/disks/N`, or `ws://`), None for local paths.
pub fn disk_server_base(path: &str) -> Option<String> {
    let url = url::Url::parse(path).ok()?;
    let scheme = match url.scheme() {
        "http" | "ws" => "http",
        "https" | "wss" => "https",
        _ => return None,
    };
    let host = url.host_str()?;
    Some(match url.port() {
        Some(p) => format!("{}://{}:{}", scheme, host, p),
        None => format!("{}://{}", scheme, host),
    })
}

/// A connection's budget for requests in flight: how many, and how many
/// bytes of payload they hold. A server takes a permit (`admit`) before it
/// reads or spawns the next request and the task holds it until it has
/// replied, so a client that pipelines without limit just stops being
/// read (TCP pushes back) instead of growing tasks and buffers without
/// bound.
pub struct InFlight {
    requests: std::sync::Arc<tokio::sync::Semaphore>,
    bytes: std::sync::Arc<tokio::sync::Semaphore>,
    max_bytes: u32,
}

/// One admitted request's share of an [`InFlight`] budget, given back on drop.
pub struct Admitted {
    _request: tokio::sync::OwnedSemaphorePermit,
    _bytes: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl InFlight {
    /// At most `requests` at a time, holding at most `bytes` between them.
    pub fn new(requests: usize, bytes: u32) -> Self {
        InFlight {
            requests: std::sync::Arc::new(tokio::sync::Semaphore::new(requests)),
            bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(bytes as usize)),
            max_bytes: bytes,
        }
    }

    /// Wait for room for one more request holding `bytes`. A request
    /// larger than the whole budget still runs, once it has it all.
    pub async fn admit(&self, bytes: usize) -> Admitted {
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");
        let n = bytes.min(self.max_bytes as usize) as u32;
        let bytes = match n {
            0 => None,
            n => Some(
                self.bytes
                    .clone()
                    .acquire_many_owned(n)
                    .await
                    .expect("never closed"),
            ),
        };
        Admitted {
            _request: request,
            _bytes: bytes,
        }
    }

    /// How many more requests it would admit now.
    pub fn available(&self) -> usize {
        self.requests.available_permits()
    }
}

/// How long a TLS handshake may take before the connection is dropped.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The first pause after a failed accept, doubled each time it fails
/// again, up to `MAX_PAUSE`.
pub(crate) const MIN_PAUSE: Duration = Duration::from_millis(50);
pub(crate) const MAX_PAUSE: Duration = Duration::from_secs(1);

/// Accept connections on `listener` for as long as the task runs, each
/// handed to `handle` in a task of its own (`TCP_NODELAY` set: every
/// protocol served here is request/response).
///
/// A failed accept never ends the loop: most are about the system, not
/// the listener (out of file descriptors, a connection reset before it
/// was taken), and clear up. It pauses instead, longer each time in a
/// row, so running out of descriptors doesn't turn into a busy loop.
pub async fn accept_loop<F, Fut>(listener: TcpListener, label: &str, handle: F)
where
    F: Fn(TcpStream, SocketAddr) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    accepting(listener, label, handle).await
}

/// Where connections come from (a `TcpListener`; tests feed their own).
pub(crate) trait Accept {
    type Conn;
    fn accept(&mut self) -> impl Future<Output = std::io::Result<(Self::Conn, SocketAddr)>>;
}

impl Accept for TcpListener {
    type Conn = TcpStream;
    async fn accept(&mut self) -> std::io::Result<(TcpStream, SocketAddr)> {
        let (sock, peer) = TcpListener::accept(self).await?;
        let _ = sock.set_nodelay(true);
        Ok((sock, peer))
    }
}

/// `accept_loop` over any [`Accept`].
pub(crate) async fn accepting<A, F, Fut>(mut from: A, label: &str, handle: F)
where
    A: Accept,
    F: Fn(A::Conn, SocketAddr) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut pause = MIN_PAUSE;
    loop {
        match from.accept().await {
            Ok((conn, peer)) => {
                pause = MIN_PAUSE;
                tokio::spawn(handle(conn, peer));
            }
            // One connection's trouble (gone before it was taken): the
            // next is fine.
            Err(e) if per_connection(&e) => {}
            Err(e) => {
                eprintln!("{}: accept: {} (pausing {:?})", label, e, pause);
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(MAX_PAUSE);
            }
        }
    }
}

/// An accept error about the one connection, not the listener or the
/// system.
fn per_connection(e: &std::io::Error) -> bool {
    use std::io::ErrorKind as K;
    matches!(
        e.kind(),
        K::ConnectionAborted | K::ConnectionReset | K::Interrupted | K::WouldBlock
    )
}

/// The TLS handshake on `sock`, given [`HANDSHAKE_TIMEOUT`].
pub async fn tls_accept(
    acceptor: &tokio_rustls::TlsAcceptor,
    sock: TcpStream,
) -> std::io::Result<tokio_rustls::server::TlsStream<TcpStream>> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(sock))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake"))?
}

/// Bind `bind:port`, retrying every few seconds until it can (the port
/// may be busy for a moment: an earlier listener still closing).
pub async fn bind_retrying(bind: &str, port: u16, label: &str) -> TcpListener {
    loop {
        match TcpListener::bind((bind, port)).await {
            Ok(l) => return l,
            Err(e) => {
                eprintln!("{}: listening on {}:{}: {}", label, bind, port, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/net_tests.rs"]
mod tests;

/// The most keys a [`LogThrottle`] remembers.
const MAX_KEYS: usize = 1024;

/// Says whether a repeated log message may be logged now: once per `every`
/// per key, counting what it held back meanwhile.
pub struct LogThrottle {
    every: Duration,
    last: std::sync::Mutex<std::collections::HashMap<String, (Instant, u64)>>,
}

impl LogThrottle {
    pub fn new(every: Duration) -> Self {
        LogThrottle {
            every,
            last: Default::default(),
        }
    }

    /// `Some(n)`: log it now, `n` more having been held back since it last
    /// was. `None`: hold it back.
    pub fn allow(&self, key: &str) -> Option<u64> {
        let now = Instant::now();
        let mut m = self.last.lock().unwrap();
        // Forget keys gone quiet, so a stream of distinct ones can't grow
        // the map without bound.
        if m.len() >= MAX_KEYS {
            m.retain(|_, (t, _)| now.duration_since(*t) < self.every);
        }
        // Still full (that many distinct keys in one `every`): hold back
        // new ones rather than remember them.
        if m.len() >= MAX_KEYS && !m.contains_key(key) {
            return None;
        }
        match m.get_mut(key) {
            Some((t, held)) if now.duration_since(*t) < self.every => {
                *held += 1;
                None
            }
            Some((t, held)) => {
                *t = now;
                Some(std::mem::take(held))
            }
            None => {
                m.insert(key.to_string(), (now, 0));
                Some(0)
            }
        }
    }
}

/// What asking one endpoint came to (see [`try_each`]).
pub enum Answer<T, E> {
    /// Its answer.
    Got(T),
    /// A definite refusal: the others would say the same.
    Refused(E),
    /// No answer from it (why): ask the next.
    Unreachable(String),
}

/// Asking every endpoint came to nothing.
#[derive(Debug)]
pub enum Tried<E> {
    Refused(E),
    /// None answered: each one's reason.
    Nobody(Vec<String>),
}

impl<E: std::fmt::Display> std::fmt::Display for Tried<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tried::Refused(e) => e.fmt(f),
            Tried::Nobody(why) if why.is_empty() => f.write_str("no cluster endpoints"),
            Tried::Nobody(why) => f.write_str(&why.join("; ")),
        }
    }
}

/// Ask `endpoints` in turn until one answers (or refuses).
pub async fn try_each<'a, T, E, F, Fut>(
    endpoints: impl IntoIterator<Item = &'a str>,
    mut ask: F,
) -> Result<T, Tried<E>>
where
    F: FnMut(&'a str) -> Fut,
    Fut: Future<Output = Answer<T, E>>,
{
    let mut why = Vec::new();
    for ep in endpoints {
        match ask(ep).await {
            Answer::Got(v) => return Ok(v),
            Answer::Refused(e) => return Err(Tried::Refused(e)),
            Answer::Unreachable(m) => why.push(format!("{}: {}", ep, m)),
        }
    }
    Err(Tried::Nobody(why))
}

/// A stream that fails (reads and writes) once its peer is revoked: what
/// a long-lived session over a TLS connection is wrapped in, so a
/// revocation ends it (`crate::trust`). Without a registration, a plain
/// stream.
///
/// Reads and writes each wait on the revocation with their own future, so
/// each keeps its own waker. With one, whichever polled last would hold it:
/// split in two (`tokio::io::split`) with replies written from other tasks
/// (NBD), a revocation would wake a writer that had already finished and
/// leave the parked reader, and the idle session, open.
pub struct Revocable<S> {
    io: S,
    read_closed: Option<Closed>,
    write_closed: Option<Closed>,
    ended: bool,
}

type Closed = std::pin::Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

#[derive(Clone, Copy)]
enum Side {
    Read,
    Write,
}

impl<S> Revocable<S> {
    /// `io`, ended when `peer` is closed (None: never).
    pub fn new(io: S, peer: Option<crate::http::Peer>) -> Self {
        let closed = |p: &crate::http::Peer| {
            let p = p.clone();
            Box::pin(async move { p.closed().await }) as Closed
        };
        Revocable {
            io,
            read_closed: peer.as_ref().map(closed),
            write_closed: peer.as_ref().map(closed),
            ended: false,
        }
    }

    fn check(&mut self, side: Side, cx: &mut std::task::Context<'_>) -> std::io::Result<()> {
        let closed = match side {
            Side::Read => &mut self.read_closed,
            Side::Write => &mut self.write_closed,
        };
        if !self.ended
            && let Some(c) = closed
            && c.as_mut().poll(cx).is_ready()
        {
            self.ended = true;
        }
        if self.ended {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "the peer's certificate was revoked",
            ));
        }
        Ok(())
    }
}

/// The server side of a TLS connection, ended when its client's
/// certificate is revoked (registered in `conns`; None: never).
pub fn revocable<T>(
    tls: tokio_rustls::server::TlsStream<T>,
    conns: Option<&crate::http::Connections>,
) -> Revocable<tokio_rustls::server::TlsStream<T>> {
    let peer = conns.and_then(|c| {
        let der = tls.get_ref().1.peer_certificates()?.first()?.clone();
        c.register(der.into_owned())
    });
    Revocable::new(tls, peer)
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Revocable<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Err(e) = self.check(Side::Read, cx) {
            return std::task::Poll::Ready(Err(e));
        }
        std::pin::Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Revocable<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Err(e) = self.check(Side::Write, cx) {
            return std::task::Poll::Ready(Err(e));
        }
        std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Err(e) = self.check(Side::Write, cx) {
            return std::task::Poll::Ready(Err(e));
        }
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
