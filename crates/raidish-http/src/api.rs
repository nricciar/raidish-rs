use crate::cluster::Cluster;
use crate::committer::PoolAdmin;
use crate::disk::BLOCK_SIZE;
use crate::disks::DiskSet;
use crate::fenced_disks::{Expiry, FencedDisks};
use crate::fs::FileSystem;
use crate::vdev::Pool;
use crate::websocket::{WsOp, WsRequest, WsStatus, encode_response};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{FromRef, FromRequestParts, Path, State};
use axum::http::StatusCode as Status;
use axum::http::request::Parts;
use axum::response::Response;
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use raidish_auth::http::{Json, Query};
use std::sync::Arc;

/// What this disk server fences writes with (see `crate::fence`).
pub type EpochFloor = crate::fence::ServerFence;

/// The served disks: reached only through their fence.
pub struct DiskServer {
    disks: Arc<FencedDisks>,
}

/// An HTTP status for a refused block request.
fn http_status(s: WsStatus) -> Status {
    match s {
        WsStatus::ErrorNotFound => Status::NOT_FOUND,
        WsStatus::InvalidRequest => Status::BAD_REQUEST,
        WsStatus::StaleEpoch | WsStatus::Fenced => Status::CONFLICT,
        WsStatus::Forbidden => Status::FORBIDDEN,
        WsStatus::NotReady => Status::SERVICE_UNAVAILABLE,
        WsStatus::Expired => Status::REQUEST_TIMEOUT,
        WsStatus::Success => Status::OK,
    }
}

/// Start an HTTP/WebSocket server that exposes `disks` (by their index in
/// the set). Only local disks are served; remote ones are refused. `pool`,
/// if configured, backs the read-only file endpoint. `cluster` is this
/// node's control-plane member (its routes share the port).
#[allow(clippy::too_many_arguments)]
pub async fn listen(
    disks: Arc<DiskSet>,
    pool: Option<Pool>,
    address: &str,
    port: u16,
    cluster: Arc<Cluster>,
    floor: Arc<EpochFloor>,
    pool_map: Option<crate::inspect::PoolMap>,
    committer: crate::committer::CommitterSlot,
    agent: crate::agent::AgentState,
    files: crate::files_api::WriterSlot,
    tls: Option<&crate::tls::TlsConfig>,
    auth: crate::auth::AuthState,
    nfs: Option<crate::nfs::peers::NfsServers>,
    vms: Option<Arc<crate::vm::VmRuntime>>,
    facts: crate::node_facts::FactsHandle,
    events: crate::events::EventSink,
    ca: Arc<raidish_ca::CaService>,
) {
    // Serving blocks doesn't need a formatted pool (that's how a remote
    // client formats it in the first place).
    let disks = Arc::new(FencedDisks::new(disks.caddies().clone(), floor.clone()));
    // Read-only: this process only serves raw blocks; the pool's writer is
    // whoever formats/serves it remotely.
    let pool = pool.map(Arc::new);
    match pool.clone() {
        Some(pool) => {
            let fence = floor.clone();
            tokio::spawn(async move { refresh_fence_map(pool, fence).await });
        }
        // No pool to map: every block is the pool's.
        None => floor.mark_unmapped(),
    }
    {
        let (cluster, fence) = (cluster.clone(), floor.clone());
        tokio::spawn(async move { sync_fence(cluster, fence).await });
    }
    let read_only = match pool {
        None => None,
        Some(pool) => match FileSystem::load_readonly(pool).await {
            Ok(fs) => Some(Arc::new(fs)),
            Err(e) => {
                println!("serving raw disks only ({})", e);
                None
            }
        },
    };
    let files = crate::files_api::FilesState {
        node: cluster.name.clone(),
        writer: files,
        read_only,
        // This node's own member (followers forward to the leader).
        leases: Arc::new(crate::cluster::lease::LeaseClient::local(cluster.clone())),
        upload_limit: crate::files_api::UPLOAD_LIMIT,
    };
    // Volume management is done by the committer: ask every node for it.
    let me = format!("{}://127.0.0.1:{}", crate::tls::scheme(), port);
    let mut endpoints = vec![me];
    endpoints.extend(cluster.members.peers.values().cloned());
    let volumes = crate::volumes_api::VolumesState {
        admin: crate::committer::Admin::new(
            committer.clone(),
            crate::committer::CommitClient::new(endpoints),
        ),
    };
    let server = Arc::new(DiskServer { disks });
    let volumes = Arc::new(volumes);
    let app = router(Routes {
        server,
        files: Arc::new(files),
        volumes: volumes.clone(),
        vms: Arc::new(crate::vms_api::VmsState::new(vms, facts)),
        cluster: cluster.clone(),
        ca,
        committer,
        agent,
        events,
        dashboard: Arc::new(crate::dashboard::DashboardState { pool: pool_map }),
        nfs: crate::nfs::peers::NfsForwarding(nfs),
    });
    let app = raidish_auth::http::finish(app, auth);
    let tls = tls.map(|t| raidish_auth::http::Server::from_config(t).expect("tls"));
    let listener = crate::net::bind_retrying(address, port, "api").await;
    // Until asked to stop (SIGINT or SIGTERM), then Raft is stopped at
    // once: a leader whose heartbeats still get through keeps the followers
    // from electing anyone while the lease requests they forward to it
    // fail, long enough for their leases to run out. Stopped, it goes
    // quiet and the others elect a new leader within an election timeout.
    raidish_auth::http::serve(listener, tls, app, stop_signal()).await;
    let _ = cluster.raft.shutdown().await;
}

/// SIGINT or SIGTERM.
async fn stop_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    println!("shutting down");
}

/// Everything the node's routes use.
pub struct Routes {
    pub server: Arc<DiskServer>,
    pub files: Arc<crate::files_api::FilesState>,
    pub volumes: Arc<crate::volumes_api::VolumesState>,
    pub vms: Arc<crate::vms_api::VmsState>,
    pub cluster: Arc<Cluster>,
    pub ca: Arc<raidish_ca::CaService>,
    pub committer: crate::committer::CommitterSlot,
    pub agent: crate::agent::AgentState,
    pub events: crate::events::EventSink,
    pub dashboard: Arc<crate::dashboard::DashboardState>,
    pub nfs: crate::nfs::peers::NfsForwarding,
}

/// The node's routes, all of them (`raidish_auth::http::finish` makes it
/// servable).
pub fn router(r: Routes) -> axum::Router {
    let disks = DiskRoutes {
        server: r.server,
        agent: r.agent.clone(),
        volumes: r.volumes.clone(),
    };
    axum::Router::new()
        .merge(raidish_auth::auth::router())
        .merge(disk_router(disks))
        .merge(crate::cluster::routes::router(r.cluster.clone()))
        .merge(raidish_ca::routes::router(r.ca))
        .merge(crate::frontend::routes::router(r.cluster.clone()))
        .merge(crate::committer::routes::router(r.committer))
        .merge(crate::files_api::router(r.files))
        .merge(crate::volumes_api::router(r.volumes.clone()))
        .merge(crate::vms_api::router(crate::vms_api::VmsCtx {
            vms: r.vms,
            volumes: r.volumes,
            cluster: r.cluster,
            agent: r.agent,
            events: r.events,
        }))
        .merge(crate::nfs::peers::router(r.nfs))
        .merge(crate::dashboard::router(r.dashboard))
        .fallback(crate::dashboard::asset)
}

/// What the routes in this module use.
#[derive(Clone)]
pub struct DiskRoutes {
    pub server: Arc<DiskServer>,
    pub agent: crate::agent::AgentState,
    pub volumes: Arc<crate::volumes_api::VolumesState>,
}

impl FromRef<DiskRoutes> for Arc<DiskServer> {
    fn from_ref(r: &DiskRoutes) -> Self {
        r.server.clone()
    }
}

pub fn disk_router(r: DiskRoutes) -> axum::Router {
    axum::Router::new()
        .route(
            "/api/v1/disks/{disk_index}/blocks/{block_id}",
            get(get_block).put(put_block),
        )
        .route("/api/v1/disks", get(list_disks))
        .route("/api/v1/disks/{disk_index}/flush", post(flush_disks))
        .route("/ws", get(ws_endpoint))
        .route("/api/v1/agent/status", get(agent_status))
        .route("/api/v1/events", get(activity))
        .with_state(r)
}

use crate::events::EventQuery;

/// HTTP: GET /api/v1/events - the cluster's activity log (see
/// `crate::events`), from whichever node is the committer.
async fn activity(
    _auth: crate::auth::CanRead,
    Query(q): Query<EventQuery>,
    State(r): State<DiskRoutes>,
) -> Result<Json<crate::events::EventPage>, (Status, String)> {
    r.volumes
        .admin
        .events(&q)
        .await
        .map(Json)
        .map_err(|e| (Status::SERVICE_UNAVAILABLE, e.to_string()))
}

/// HTTP: GET /api/v1/agent/status - the volumes this node runs.
async fn agent_status(
    _auth: crate::auth::CanRead,
    State(r): State<DiskRoutes>,
) -> Json<crate::agent::AgentStatus> {
    Json(r.agent.read().unwrap().clone())
}

/// The fence's startup barrier, its cluster half (audit R1b): the
/// grants as of one linearizable read. Retried until the cluster answers.
pub(crate) async fn sync_fence(cluster: Arc<Cluster>, fence: Arc<EpochFloor>) {
    loop {
        match cluster
            .linearizable_state(std::time::Duration::from_secs(10))
            .await
        {
            Ok(state) => {
                crate::cluster::store::observe(&*fence, &state);
                fence.mark_synced();
                return;
            }
            Err(e) => eprintln!(
                "fence: waiting for the cluster before admitting writes: {}",
                e
            ),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Keep the fencing domain map in step with the pool's layout: known once
/// the pool is formatted, and changed by a reformat. Until the superblock
/// has been read (or found missing on a readable pool: not formatted yet),
/// the map isn't known and writes wait (audit R1b).
async fn refresh_fence_map(pool: Arc<Pool>, fence: Arc<EpochFloor>) {
    let mut last: Option<(u64, u64)> = None;
    loop {
        match crate::fs::probe_superblock_of(&*pool).await {
            Ok(Some(sb)) => {
                let key = (sb.pool_guid, sb.data_start);
                if last != Some(key) {
                    let layout = crate::metaslab::MetaslabLayout::from_superblock(&sb);
                    fence.set_map(Some(crate::fence::FenceMap::new(
                        pool.topology().clone(),
                        layout,
                    )));
                    last = Some(key);
                }
            }
            // Readable, and no pool on it (until one is formatted). Whether
            // that's to be believed is the fence's call: not once anything
            // has been lent (`fence::Ready`).
            Ok(None) if !fence.has_map() => fence.mark_unmapped(),
            Ok(None) | Err(crate::fs::Unreadable) => {}
        }
        tokio::time::sleep(std::time::Duration::from_secs(if last.is_some() {
            10
        } else {
            2
        }))
        .await;
    }
}

/// The `X-Raidish-Epoch` header of an HTTP block write (`Epoch::NONE` if
/// absent). A header that isn't an epoch is a bad request, not "no lease".
pub struct HeaderEpoch(crate::types::Epoch);

impl<S: Send + Sync> FromRequestParts<S> for HeaderEpoch {
    type Rejection = Status;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Status> {
        match parts.headers.get("X-Raidish-Epoch") {
            None => Ok(HeaderEpoch(crate::types::Epoch::NONE)),
            Some(v) => v
                .to_str()
                .ok()
                .and_then(|v| v.parse().ok())
                .map(HeaderEpoch)
                .ok_or(Status::BAD_REQUEST),
        }
    }
}

/// HTTP: GET /api/v1/disks/{disk_index}/blocks/{block_id}
/// Reads a block directly from a specific disk in the array
async fn get_block(
    _auth: crate::auth::CanRead,
    Path((disk_index, block_id)): Path<(usize, u64)>,
    State(server): State<Arc<DiskServer>>,
) -> Result<Vec<u8>, Status> {
    let r = server
        .disks
        .validate_read(disk_index, block_id, 1)
        .map_err(http_status)?;
    server.disks.read(r, None).await.map_err(|s| match s {
        WsStatus::ErrorNotFound => Status::INTERNAL_SERVER_ERROR,
        s => http_status(s),
    })
}

/// HTTP: PUT /api/v1/disks/{disk_index}/blocks/{block_id}
/// Writes a block directly to a specific disk in the array
async fn put_block(
    _auth: crate::auth::CanWrite,
    Path((disk_index, block_id)): Path<(usize, u64)>,
    epoch: HeaderEpoch,
    State(server): State<Arc<DiskServer>>,
    data: axum::body::Body,
) -> Result<Status, Status> {
    if !server.disks.is_local(disk_index) {
        return Err(Status::NOT_FOUND);
    }
    let bytes = raidish_auth::http::read_body(data, 10 << 20)
        .await
        .map_err(|_| Status::BAD_REQUEST)?;
    if bytes.len() != BLOCK_SIZE {
        return Err(Status::BAD_REQUEST);
    }
    // Admitted only once the whole block is here, under its stripe lock,
    // right before it's written: a write that sat streaming its body can't
    // be admitted under an epoch that has since been fenced out.
    let w = server
        .disks
        .validate_write(disk_index, block_id, 1, &bytes)
        .map_err(http_status)?;
    server
        .disks
        .write(w, crate::fence::Stamp::Pool(epoch.0), &Expiry::never())
        .await
        .map_err(|s| match s {
            WsStatus::ErrorNotFound => Status::INTERNAL_SERVER_ERROR,
            s => http_status(s),
        })?;
    Ok(Status::OK)
}

/// HTTP: GET /api/v1/disks - List disk indices and their types (local/remote)
async fn list_disks(_auth: crate::auth::CanRead, State(server): State<Arc<DiskServer>>) -> String {
    let disk_info: Vec<_> = (0..server.disks.disk_count())
        .map(|i| {
            serde_json::json!({
                "index": i,
                "is_local": server.disks.is_local(i),
                "servable": server.disks.is_local(i)
            })
        })
        .collect();

    serde_json::to_string(&disk_info).unwrap()
}

/// HTTP: POST /api/v1/flush - Flush all local disks
async fn flush_disks(
    _auth: crate::auth::CanWrite,
    Path(disk_index): Path<usize>,
    epoch: HeaderEpoch,
    State(server): State<Arc<DiskServer>>,
) -> Status {
    let _ = epoch;
    match server.disks.flush(disk_index).await {
        Ok(()) => Status::OK,
        Err(WsStatus::ErrorNotFound) if server.disks.is_local(disk_index) => {
            Status::INTERNAL_SERVER_ERROR
        }
        Err(s) => http_status(s),
    }
}

/// WebSocket endpoint for block I/O (protocol in `crate::websocket`).
///
/// Requests are handled concurrently and answered as they complete, so a
/// client can keep many requests in flight on one connection.
/// Most requests one connection may have being served at once; beyond that
/// the server stops reading, which pushes back on the client.
const WS_MAX_IN_FLIGHT: usize = 256;

/// `?v=`: the block protocol the client speaks.
#[derive(serde::Deserialize, Default)]
struct Version {
    v: Option<u32>,
}

async fn ws_endpoint(
    who: crate::auth::Authed,
    peer: Option<axum::Extension<raidish_auth::http::Peer>>,
    ws: WebSocketUpgrade,
    Query(Version { v }): Query<Version>,
    State(server): State<Arc<DiskServer>>,
) -> Result<Response, (Status, String)> {
    use crate::websocket::BLOCK_PROTOCOL;
    if v != Some(BLOCK_PROTOCOL) {
        let asked = v.map_or("no block protocol version".to_string(), |v| {
            format!("block protocol {}", v)
        });
        return Err((
            Status::BAD_REQUEST,
            format!("{} asked for; this node speaks {}", asked, BLOCK_PROTOCOL),
        ));
    }
    let can_write = who.can_write();
    let disks = server.disks.clone();
    let peer = peer.map(|axum::Extension(p)| p);

    Ok(ws.on_upgrade(move |stream| async move {
        let (mut sink, mut source) = stream.split();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(WS_MAX_IN_FLIGHT);
        let permits = Arc::new(tokio::sync::Semaphore::new(WS_MAX_IN_FLIGHT));
        // Set once the connection is gone: its client has given up on
        // whatever it still has in flight (audit D5b).
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let writer = {
            let closed = closed.clone();
            tokio::spawn(async move {
                while let Some(response) = rx.recv().await {
                    if sink.send(Message::Binary(response.into())).await.is_err() {
                        break;
                    }
                }
                closed.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        // Until the client goes, or the peer's connections are closed (its
        // certificate revoked: `raidish_auth::http::Connections`).
        let revoked = async {
            match &peer {
                Some(p) => p.closed().await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(revoked);
        loop {
            let message = tokio::select! {
                m = source.next() => m,
                _ = &mut revoked => break,
            };
            let Some(message) = message else { break };
            match message {
                Ok(Message::Binary(data)) => {
                    // The client's clock started before this: the wait
                    // for a permit counts against its budget.
                    let received = tokio::time::Instant::now();
                    let Ok(permit) = permits.clone().acquire_owned().await else {
                        break;
                    };
                    let (disks, tx, closed) = (disks.clone(), tx.clone(), closed.clone());
                    tokio::spawn(async move {
                        let reply =
                            handle_ws_message_as(&data, &disks, can_write, received, closed).await;
                        let _ = tx.send(reply).await;
                        drop(permit);
                    });
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }

        closed.store(true, std::sync::atomic::Ordering::SeqCst);
        drop(tx);
        let _ = writer.await;
    }))
}

/// Serve one encoded request against `disks` and encode the reply.
#[cfg(test)]
pub(crate) async fn handle_ws_message(data: &[u8], disks: &FencedDisks) -> Vec<u8> {
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    handle_ws_message_as(data, disks, true, tokio::time::Instant::now(), closed).await
}

/// One websocket request from a caller who may (or may not) write, which
/// arrived at `received` on a connection that sets `closed` when it ends.
/// A read-only caller's writes and flushes are refused, and its reads
/// carry no epoch: only writers may raise the fencing floor.
pub(crate) async fn handle_ws_message_as(
    data: &[u8],
    disks: &FencedDisks,
    can_write: bool,
    received: tokio::time::Instant,
    closed: Arc<std::sync::atomic::AtomicBool>,
) -> Vec<u8> {
    let mut owned;
    let mut data = data;
    if !can_write && data.len() >= crate::websocket::WS_REQUEST_HEADER_LEN {
        let id = u64::from_le_bytes(data[15..23].try_into().unwrap());
        if matches!(WsOp::try_from(data[0]), Ok(WsOp::Write | WsOp::Flush)) {
            return encode_response(WsStatus::Forbidden, id, &[]);
        }
        owned = data.to_vec();
        owned[23..31].fill(0);
        data = &owned;
    }
    let Some(request) = WsRequest::parse(data) else {
        // Echo the id if there's one, so the client's request fails instead
        // of waiting for a reply that never comes.
        let id = data
            .get(15..23)
            .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
        return encode_response(WsStatus::InvalidRequest, id, &[]);
    };
    let id = request.id();
    // Half the client's budget: the rest covers the reply's way back, and
    // the request's way here (which the server can't see).
    let deadline = request.budget().map(|b| received + b / 2);
    let expiry = Expiry::new(deadline, Some(closed));
    match serve_request(&request, disks, &expiry).await {
        Ok(payload) => encode_response(WsStatus::Success, id, &payload),
        Err(status) => encode_response(status, id, &[]),
    }
}

async fn serve_request(
    request: &WsRequest<'_>,
    disks: &FencedDisks,
    expiry: &Expiry,
) -> Result<Vec<u8>, WsStatus> {
    let op = request.op().ok_or(WsStatus::InvalidRequest)?;
    let disk = request.disk_index() as usize;
    let (block, count) = (request.block_id(), request.count());
    // Validated before anything is admitted: an admitted request raises
    // floors, and a malformed one must change nothing.
    match op {
        WsOp::Read => {
            let r = disks.validate_read(disk, block, count)?;
            let stamp = request.stamp().ok_or(WsStatus::InvalidRequest)?;
            disks.read(r, Some(stamp)).await
        }
        WsOp::Write => {
            let data = request.write_data().ok_or(WsStatus::InvalidRequest)?;
            let w = disks.validate_write(disk, block, count, data)?;
            let stamp = request.stamp().ok_or(WsStatus::InvalidRequest)?;
            disks.write(w, stamp, expiry).await?;
            Ok(Vec::new())
        }
        WsOp::Flush => {
            disks.flush(disk).await?;
            Ok(Vec::new())
        }
        WsOp::Info => Ok(disks.capacity_blocks(disk).await?.to_le_bytes().to_vec()),
    }
}

#[cfg(test)]
#[path = "tests/api_tests.rs"]
mod tests;
