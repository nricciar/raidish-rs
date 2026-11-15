//! The web dashboard's server side: its static files (embedded into release
//! builds by `build.rs`) and `GET /api/v1/pool`, which tells it where the
//! pool's disks are. Everything else it reads itself, over `/ws`.

use crate::inspect::PoolMap;
use rocket::http::{ContentType, Header, Status};
use rocket::response::{self, Responder, Response};
use rocket::serde::json::Json;
use rocket::{Request, State};
use std::io::Cursor;
use std::path::PathBuf;

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

/// This node's view of the pool (None: no pool configured).
pub struct DashboardState {
    pub pool: Option<PoolMap>,
}

pub fn routes() -> Vec<rocket::Route> {
    routes![index, asset, pool]
}

/// GET /api/v1/pool - the pool layout and where each disk is served.
#[get("/api/v1/pool")]
fn pool(
    _auth: crate::auth::CanRead,
    state: &State<DashboardState>,
) -> Result<Json<PoolMap>, Status> {
    state.pool.clone().map(Json).ok_or(Status::NotFound)
}

#[get("/")]
fn index() -> Asset {
    lookup("index.html").unwrap_or_else(|| Asset {
        mime: ContentType::HTML,
        body: NOT_BUNDLED.as_bytes().to_vec(),
        cache: false,
    })
}

// Ranked below every API route, so it only answers paths nothing else does.
#[get("/<path..>", rank = 20)]
fn asset(path: PathBuf) -> Option<Asset> {
    lookup(path.to_str()?)
}

pub struct Asset {
    mime: ContentType,
    body: Vec<u8>,
    /// Trunk names its js/wasm/css by content hash: cache those forever.
    cache: bool,
}

impl<'r> Responder<'r, 'static> for Asset {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let cache = if self.cache {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        Response::build()
            .header(self.mime)
            .header(Header::new("Cache-Control", cache))
            .sized_body(self.body.len(), Cursor::new(self.body))
            .ok()
    }
}

fn lookup(path: &str) -> Option<Asset> {
    let cache = path != "index.html";
    if let Some((_, mime, bytes)) = embedded::ASSETS.iter().find(|(p, _, _)| *p == path) {
        return Some(Asset {
            mime: ContentType::parse_flexible(mime).unwrap_or(ContentType::Binary),
            body: bytes.to_vec(),
            cache,
        });
    }
    // Not bundled (a debug build): serve a `trunk build` output if present.
    if !embedded::ASSETS.is_empty() {
        return None;
    }
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("web/dist")
        .join(path);
    let body = std::fs::read(&file).ok()?;
    let mime = file
        .extension()
        .and_then(|e| ContentType::from_extension(e.to_str()?))
        .unwrap_or(ContentType::Binary);
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
