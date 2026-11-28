//! Serving HTTP(S) with axum (HTTP-1, `docs/ca-service-plan.md` §5, D1).
//!
//! * [`LiveIdentity`] / [`LiveTrust`]: the server certificate and the client
//!   verifier, swappable while serving (renewal, trust changes). A swap
//!   applies from the next handshake; open connections keep theirs.
//! * [`Peer`]: the client's certificate, in every request's extensions,
//!   taken from the connection it came on.
//! * [`Connections`]: every connection, and every stream upgraded off one
//!   (WebSockets), under its peer's serial, so revoking a serial can end
//!   them all. A WebSocket handler holds its `Peer` and watches
//!   [`Peer::closed`] (an upgrade takes the socket out of hyper).
//! * [`serve`]: one listener, TLS or plain, an axum `Router` behind it.
//! * [`Json`]: JSON in (whatever the `Content-Type`) and out.
//!
//! The guards in `crate::auth` are extractors over what this puts in each
//! request: the [`Peer`], and the [`crate::auth::AuthState`] layer.

mod conns;
mod live;
mod serve;

pub use conns::{Connections, Peer};
pub use live::{LiveIdentity, LiveTrust};
pub use serve::{Server, serve};

use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::path::PathBuf;

/// What a route answers when it can't: a status and a one-line reason.
pub type Refusal = (StatusCode, String);

/// The most a JSON (or other buffered) body may be.
pub const BODY_LIMIT: usize = 64 << 20;

/// A JSON request body or response. Unlike `axum::Json`, a body is taken
/// as JSON whatever its `Content-Type` says (as Rocket did: the command
/// line and the nodes don't all set one).
#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Json<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Json<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T, S> FromRequest<S> for Json<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Refusal;

    async fn from_request(req: Request, state: &S) -> Result<Self, Refusal> {
        let bytes = axum::body::Bytes::from_request(req, state)
            .await
            .map_err(|e| (e.status(), e.body_text()))?;
        serde_json::from_slice(&bytes)
            .map(Json)
            .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, format!("bad JSON: {}", e)))
    }
}

impl<T: serde::Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

/// `router` made ready to serve: every route sees `auth` (who's asking,
/// see `crate::auth`), and buffered bodies are capped at [`BODY_LIMIT`]
/// (streamed ones, read from `axum::body::Body`, aren't).
pub fn finish(router: axum::Router, auth: crate::auth::AuthState) -> axum::Router {
    router
        .layer(axum::Extension(std::sync::Arc::new(auth)))
        .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT))
}

/// Bind `addr` and serve `app` there until `shutdown` (TLS with `tls`).
/// Listening when this returns.
pub async fn spawn(
    addr: impl tokio::net::ToSocketAddrs,
    tls: Option<Server>,
    app: axum::Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let at = listener.local_addr()?;
    tokio::spawn(serve(listener, tls, app, shutdown));
    Ok(at)
}

/// A body read whole, at most `limit` bytes (413 beyond).
pub async fn read_body(body: axum::body::Body, limit: usize) -> Result<axum::body::Bytes, Refusal> {
    axum::body::to_bytes(body, limit).await.map_err(|e| {
        let too_big = std::error::Error::source(&e)
            .is_some_and(|s| s.is::<http_body_util::LengthLimitError>());
        if too_big {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("body larger than {} bytes", limit),
            )
        } else {
            (StatusCode::BAD_REQUEST, format!("reading the body: {}", e))
        }
    })
}

/// A body as an `AsyncRead`, for streaming it.
pub fn body_reader(body: axum::body::Body) -> impl tokio::io::AsyncRead + Send + Unpin + 'static {
    use futures_util::TryStreamExt;
    tokio_util::io::StreamReader::new(body.into_data_stream().map_err(std::io::Error::other))
}

/// A `{*path}` capture as a relative path, by the rules Rocket had for one:
/// empty segments are skipped, `..` drops the one before, and a segment
/// starting with `.` or `*`, or ending with `:`, `<` or `>`, is refused.
pub fn path_buf(path: &str) -> Result<PathBuf, Refusal> {
    let mut buf = PathBuf::new();
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        let bad = seg.starts_with('.') && seg != ".."
            || seg.starts_with('*')
            || seg.ends_with(':')
            || seg.ends_with('<')
            || seg.ends_with('>')
            || seg.contains('\\');
        if bad {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("bad path segment '{}'", seg),
            ));
        }
        if seg == ".." {
            buf.pop();
        } else {
            buf.push(seg);
        }
    }
    Ok(buf)
}

/// A request's `?…` query as `T`, or 400 saying why not.
pub struct Query<T>(pub T);

impl<T, S> axum::extract::FromRequestParts<S> for Query<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Refusal;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _: &S,
    ) -> Result<Self, Refusal> {
        serde_urlencoded::from_str(parts.uri.query().unwrap_or(""))
            .map(Query)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad query: {}", e)))
    }
}

#[cfg(test)]
#[path = "../tests/http_tests.rs"]
mod tests;
