use crate::cluster::Cluster;
use crate::committer::PoolAdmin;
use crate::disk::BLOCK_SIZE;
use crate::disks::DiskSet;
use crate::fenced_disks::{Expiry, FencedDisks};
use crate::fs::FileSystem;
use crate::vdev::Pool;
use crate::websocket::{WsOp, WsRequest, WsStatus, encode_response};
use rocket::data::{Data, ToByteUnit};
use rocket::figment::util::map;
use rocket::futures::{SinkExt, StreamExt};
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::{State, get, put};
use std::sync::Arc;
use ws::WebSocket;

/// What this disk server fences writes with (see `crate::fence`).
pub type EpochFloor = crate::fence::ServerFence;

/// The served disks: reached only through their fence.
pub struct DiskServer {
    disks: Arc<FencedDisks>,
}

/// An HTTP status for a refused block request.
fn http_status(s: WsStatus) -> Status {
    match s {
        WsStatus::ErrorNotFound => Status::NotFound,
        WsStatus::InvalidRequest => Status::BadRequest,
        WsStatus::StaleEpoch | WsStatus::Fenced => Status::Conflict,
        WsStatus::Forbidden => Status::Forbidden,
        WsStatus::NotReady => Status::ServiceUnavailable,
        WsStatus::Expired => Status::RequestTimeout,
        WsStatus::Success => Status::Ok,
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
    let server = DiskServer { disks };

    let mut figment = rocket::Config::figment()
        .merge(("port", port))
        .merge((
            "limits",
            map!["data-form" => "500 MiB", "file" => "500 MiB", "stream" => "500 MiB", "json" => "64 MiB", "upload" => "64 GiB"],
        ));
    if let Some(tls) = tls {
        let path = |p: Option<std::path::PathBuf>, what: &str| {
            p.map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| panic!("tls: no {} configured", what))
        };
        figment = figment
            .merge(("tls.certs", path(tls.cert_path(), "cert")))
            .merge(("tls.key", path(tls.key_path(), "key")))
            // Ask for a client certificate but don't insist: browsers
            // present a token instead (checked per route, `crate::auth`).
            .merge(("tls.mutual.ca_certs", path(tls.ca_path(), "ca")))
            .merge(("tls.mutual.mandatory", false));
    }

    rocket::build()
        .manage(auth)
        .manage(server)
        .manage(files)
        .manage(volumes)
        .manage(crate::vms_api::VmsState::new(vms, facts))
        .manage(cluster)
        .manage(committer)
        .manage(agent)
        .manage(events)
        .manage(crate::dashboard::DashboardState { pool: pool_map })
        .manage(crate::nfs::peers::NfsForwarding(nfs))
        .configure(figment)
        .register(
            "/",
            rocket::catchers![crate::auth::unauthorized, crate::auth::forbidden],
        )
        .mount("/", rocket::routes![crate::auth::whoami])
        .mount(
            "/",
            routes![
                get_block,
                put_block,
                list_disks,
                flush_disks,
                ws_endpoint,
                agent_status,
                activity
            ],
        )
        .mount("/", crate::cluster::routes::routes())
        .attach(crate::cluster::routes::stop_raft_on_shutdown())
        .mount("/", crate::frontend::routes::routes())
        .mount("/", crate::committer::routes::routes())
        .mount("/", crate::files_api::routes())
        .mount("/", crate::volumes_api::routes())
        .mount("/", crate::vms_api::routes())
        .mount("/", crate::nfs::peers::routes())
        .mount("/", crate::dashboard::routes())
        .launch()
        .await
        .unwrap();
}

use crate::events::EventQuery;

/// HTTP: GET /api/v1/events - the cluster's activity log (see
/// `crate::events`), from whichever node is the committer.
#[get("/api/v1/events?<q..>")]
async fn activity(
    _auth: crate::auth::CanRead,
    q: EventQuery,
    volumes: &State<crate::volumes_api::VolumesState>,
) -> Result<rocket::serde::json::Json<crate::events::EventPage>, (rocket::http::Status, String)> {
    volumes
        .admin
        .events(&q)
        .await
        .map(rocket::serde::json::Json)
        .map_err(|e| (rocket::http::Status::ServiceUnavailable, e.to_string()))
}

/// HTTP: GET /api/v1/agent/status - the volumes this node runs.
#[get("/api/v1/agent/status")]
async fn agent_status(
    _auth: crate::auth::CanRead,
    agent: &State<crate::agent::AgentState>,
) -> rocket::serde::json::Json<crate::agent::AgentStatus> {
    rocket::serde::json::Json(agent.read().unwrap().clone())
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
                crate::cluster::store::observe(&fence, &state);
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

#[rocket::async_trait]
impl<'r> FromRequest<'r> for HeaderEpoch {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, ()> {
        match req.headers().get_one("X-Raidish-Epoch") {
            None => Outcome::Success(HeaderEpoch(crate::types::Epoch::NONE)),
            Some(v) => match v.parse() {
                Ok(e) => Outcome::Success(HeaderEpoch(e)),
                Err(_) => Outcome::Error((Status::BadRequest, ())),
            },
        }
    }
}

/// HTTP: GET /api/v1/disks/{disk_index}/blocks/{block_id}
/// Reads a block directly from a specific disk in the array
#[get("/api/v1/disks/<disk_index>/blocks/<block_id>")]
async fn get_block(
    _auth: crate::auth::CanRead,
    disk_index: usize,
    block_id: u64,
    server: &State<DiskServer>,
) -> Result<Vec<u8>, Status> {
    let r = server
        .disks
        .validate_read(disk_index, block_id, 1)
        .map_err(http_status)?;
    server.disks.read(r, None).await.map_err(|s| match s {
        WsStatus::ErrorNotFound => Status::InternalServerError,
        s => http_status(s),
    })
}

/// HTTP: PUT /api/v1/disks/{disk_index}/blocks/{block_id}
/// Writes a block directly to a specific disk in the array
#[put("/api/v1/disks/<disk_index>/blocks/<block_id>", data = "<data>")]
async fn put_block(
    _auth: crate::auth::CanWrite,
    disk_index: usize,
    block_id: u64,
    data: Data<'_>,
    epoch: HeaderEpoch,
    server: &State<DiskServer>,
) -> Result<Status, Status> {
    if !server.disks.is_local(disk_index) {
        return Err(Status::NotFound);
    }
    let bytes = data
        .open(10.mebibytes())
        .into_bytes()
        .await
        .map_err(|_| Status::BadRequest)?;
    if bytes.len() != BLOCK_SIZE {
        return Err(Status::BadRequest);
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
            WsStatus::ErrorNotFound => Status::InternalServerError,
            s => http_status(s),
        })?;
    Ok(Status::Ok)
}

/// HTTP: GET /api/v1/disks - List disk indices and their types (local/remote)
#[get("/api/v1/disks")]
async fn list_disks(_auth: crate::auth::CanRead, server: &State<DiskServer>) -> String {
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
#[post("/api/v1/disks/<disk_index>/flush")]
async fn flush_disks(
    _auth: crate::auth::CanWrite,
    disk_index: usize,
    epoch: HeaderEpoch,
    server: &State<DiskServer>,
) -> Status {
    let _ = epoch;
    match server.disks.flush(disk_index).await {
        Ok(()) => Status::Ok,
        Err(WsStatus::ErrorNotFound) if server.disks.is_local(disk_index) => {
            Status::InternalServerError
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

#[get("/ws?<v>")]
async fn ws_endpoint(
    who: crate::auth::Principal,
    ws: WebSocket,
    v: Option<u32>,
    server: &State<DiskServer>,
) -> Result<ws::Channel<'static>, (rocket::http::Status, String)> {
    use crate::websocket::BLOCK_PROTOCOL;
    if v != Some(BLOCK_PROTOCOL) {
        let asked = v.map_or("no block protocol version".to_string(), |v| {
            format!("block protocol {}", v)
        });
        return Err((
            rocket::http::Status::BadRequest,
            format!("{} asked for; this node speaks {}", asked, BLOCK_PROTOCOL),
        ));
    }
    let can_write = who.can_write();
    let disks = server.disks.clone();

    Ok(ws.channel(move |stream| {
        Box::pin(async move {
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
                        if sink.send(ws::Message::Binary(response)).await.is_err() {
                            break;
                        }
                    }
                    closed.store(true, std::sync::atomic::Ordering::SeqCst);
                })
            };

            while let Some(message) = source.next().await {
                match message {
                    Ok(ws::Message::Binary(data)) => {
                        // The client's clock started before this: the wait
                        // for a permit counts against its budget.
                        let received = tokio::time::Instant::now();
                        let Ok(permit) = permits.clone().acquire_owned().await else {
                            break;
                        };
                        let (disks, tx, closed) = (disks.clone(), tx.clone(), closed.clone());
                        tokio::spawn(async move {
                            let reply =
                                handle_ws_message_as(&data, &disks, can_write, received, closed)
                                    .await;
                            let _ = tx.send(reply).await;
                            drop(permit);
                        });
                    }
                    Ok(ws::Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }

            closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(tx);
            let _ = writer.await;
            Ok(())
        })
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
