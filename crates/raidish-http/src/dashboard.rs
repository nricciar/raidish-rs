//! The web dashboard's server side: its static files (embedded into release
//! builds by `build.rs`) and `GET /api/v1/pool`, which tells it where the
//! pool's disks are. Everything else it reads itself, over `/ws`.

use crate::inspect::PoolMap;
use axum::extract::State;
use axum::http::{StatusCode as Status, Uri, header};
use axum::response::{IntoResponse, Response};
use raidish_auth::http::Json;
use std::path::PathBuf;
use std::sync::Arc;

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

/// This node's view of the pool (None: no pool configured).
pub struct DashboardState {
    pub pool: Option<PoolMap>,
}

/// `/` and `GET /api/v1/pool`. The other files are the app's fallback,
/// [`asset`], so they only answer paths no route does.
pub fn router(st: Arc<DashboardState>) -> axum::Router {
    axum::Router::new()
        .route("/", axum::routing::get(index))
        .route("/api/v1/pool", axum::routing::get(pool))
        .with_state(st)
}

/// GET /api/v1/pool - the pool layout and where each disk is served.
async fn pool(
    _auth: crate::auth::CanRead,
    State(state): State<Arc<DashboardState>>,
) -> Result<Json<PoolMap>, Status> {
    state.pool.clone().map(Json).ok_or(Status::NOT_FOUND)
}

async fn index() -> Asset {
    lookup("index.html").unwrap_or_else(|| Asset {
        mime: "text/html; charset=utf-8".into(),
        body: NOT_BUNDLED.as_bytes().to_vec(),
        cache: false,
    })
}

/// Any other GET: a bundled file, or 404.
pub async fn asset(method: axum::http::Method, uri: Uri) -> Response {
    if method != axum::http::Method::GET && method != axum::http::Method::HEAD {
        return Status::NOT_FOUND.into_response();
    }
    let found = raidish_auth::http::path_buf(uri.path())
        .ok()
        .and_then(|p| lookup(p.to_str()?));
    match found {
        Some(a) => a.into_response(),
        None => Status::NOT_FOUND.into_response(),
    }
}

pub struct Asset {
    mime: String,
    body: Vec<u8>,
    /// Trunk names its js/wasm/css by content hash: cache those forever.
    cache: bool,
}

impl IntoResponse for Asset {
    fn into_response(self) -> Response {
        let cache = if self.cache {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        (
            [
                (header::CONTENT_TYPE, self.mime),
                (header::CACHE_CONTROL, cache.to_string()),
            ],
            self.body,
        )
            .into_response()
    }
}

/// The type of a file the web build makes, by its extension.
fn mime_of(ext: &str) -> &'static str {
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript",
        "wasm" => "application/wasm",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "txt" => "text/plain; charset=utf-8",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn lookup(path: &str) -> Option<Asset> {
    let cache = path != "index.html";
    if let Some((_, mime, bytes)) = embedded::ASSETS.iter().find(|(p, _, _)| *p == path) {
        return Some(Asset {
            mime: mime.to_string(),
            body: bytes.to_vec(),
            cache,
        });
    }
    // Not bundled (a debug build): serve a `trunk build` output if present.
    if !embedded::ASSETS.is_empty() {
        return None;
    }
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../web/dist")
        .join(path);
    let body = std::fs::read(&file).ok()?;
    let mime = mime_of(file.extension().and_then(|e| e.to_str()).unwrap_or("")).to_string();
    Some(Asset {
        mime,
        body,
        cache: false,
    })
}

const NOT_BUNDLED: &str = "<!doctype html><meta charset=utf-8><title>raidish</title>
<body style=\"font-family:system-ui;max-width:40em;margin:4em auto;line-height:1.5\">
<h1>raidish</h1>
<p>This build doesn't include the web dashboard (only <code>--release</code> builds bundle it).</p>
<p>For development, run <code>trunk serve</code> in <code>web/</code> and open
<a href=\"http://127.0.0.1:8080/\">http://127.0.0.1:8080/</a>
(it proxies to this node), or <code>trunk build</code> there and reload this page.</p>";
