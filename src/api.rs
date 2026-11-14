use crate::cluster::Cluster;
use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::disks::{Caddy, DiskSet};
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

pub struct DiskServer {
    disks: Arc<Vec<Caddy>>,
    floor: Arc<EpochFloor>,
}

impl DiskServer {
    fn local_disk(&self, disk_index: usize) -> Result<&Caddy, Status> {
        let disk = self.disks.get(disk_index).ok_or(Status::NotFound)?;
        if !disk.is_local() {
            return Err(Status::BadRequest);
        }
        Ok(disk)
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
    port: u32,
    cluster: Arc<Cluster>,
    floor: Arc<EpochFloor>,
    pool_map: Option<crate::inspect::PoolMap>,
    committer: crate::committer::CommitterSlot,
    agent: crate::agent::AgentState,
    files: crate::files_api::WriterSlot,
    tls: Option<&crate::tls::TlsConfig>,
    auth: crate::auth::AuthState,
    nfs: Option<crate::nfs::peers::NfsServers>,
) {
    // Serving blocks doesn't need a formatted pool (that's how a remote
    // client formats it in the first place).
    let disks = disks.disks.clone();
    // Read-only: this process only serves raw blocks; the pool's writer is
    // whoever formats/serves it remotely.
    let pool = pool.map(Arc::new);
    if let Some(pool) = pool.clone() {
        let fence = floor.clone();
        tokio::spawn(async move { refresh_fence_map(pool, fence).await });
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
        // Any node takes lease requests (followers forward to the leader).
        leases: Arc::new(crate::cluster::lease::LeaseClient::new(vec![format!(
            "{}://127.0.0.1:{}",
            crate::tls::scheme(),
            port
        )])),
    };
    let server = DiskServer { disks, floor };

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
        .manage(cluster)
        .manage(committer)
        .manage(agent)
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
                agent_status
            ],
        )
        .mount("/", crate::cluster::routes::routes())
        .mount("/", crate::committer::routes::routes())
        .mount("/", crate::files_api::routes())
        .mount("/", crate::nfs::peers::routes())
        .mount("/", crate::dashboard::routes())
        .launch()
        .await
        .unwrap();
}

/// HTTP: GET /api/v1/agent/status - the volumes this node runs.
#[get("/api/v1/agent/status")]
async fn agent_status(
    _auth: crate::auth::CanRead,
    agent: &State<crate::agent::AgentState>,
) -> rocket::serde::json::Json<crate::agent::AgentStatus> {
    rocket::serde::json::Json(agent.read().unwrap().clone())
}

/// Keep the fencing domain map in step with the pool's layout: known once
/// the pool is formatted, and changed by a reformat.
async fn refresh_fence_map(pool: Arc<Pool>, fence: Arc<EpochFloor>) {
    let mut last: Option<(u64, u64)> = None;
    loop {
        if let Some(sb) = crate::fs::read_superblock_of(&pool).await {
            let key = (sb.pool_guid, sb.data_start);
            if last != Some(key) {
                let layout = crate::metaslab::MetaslabLayout::from_superblock(&sb);
                fence.set_map(Some(crate::fence::FenceMap::new(
                    pool.topology.clone(),
                    layout,
                )));
                last = Some(key);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(if last.is_some() {
            10
        } else {
            2
        }))
        .await;
    }
}

/// The `X-Raidish-Epoch` header of an HTTP block write (0 if absent).
pub struct HeaderEpoch(u64);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for HeaderEpoch {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, ()> {
        let epoch = req
            .headers()
            .get_one("X-Raidish-Epoch")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Outcome::Success(HeaderEpoch(epoch))
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
    let mut buf = vec![0u8; BLOCK_SIZE];
    server
        .local_disk(disk_index)?
        .read_block(block_id, &mut buf)
        .await
        .map_err(|_| Status::InternalServerError)?;

    Ok(buf)
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
    if !server.floor.admit(disk_index, block_id, 1, epoch.0, true) {
        return Err(Status::Conflict);
    }
    let bytes = data
        .open(10.mebibytes())
        .into_bytes()
        .await
        .map_err(|_| Status::BadRequest)?;

    if bytes.len() != BLOCK_SIZE {
        return Err(Status::BadRequest);
    }

    let bytes = &bytes.as_slice().try_into().unwrap();
    server
        .local_disk(disk_index)?
        .write_block(block_id, bytes)
        .await
        .map_err(|_| Status::InternalServerError)?;

    Ok(Status::Ok)
}

/// HTTP: GET /api/v1/disks - List disk indices and their types (local/remote)
#[get("/api/v1/disks")]
async fn list_disks(_auth: crate::auth::CanRead, server: &State<DiskServer>) -> String {
    let disk_info: Vec<_> = (0..server.disks.len())
        .map(|i| {
            serde_json::json!({
                "index": i,
                "is_local": server.disks[i].is_local(),
                "servable": server.disks[i].is_local()
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
    match server.local_disk(disk_index) {
        Ok(disk) => match disk.flush().await {
            Ok(()) => Status::Ok,
            Err(_) => Status::InternalServerError,
        },
        Err(status) => status,
    }
}

/// WebSocket endpoint for block I/O (protocol in `crate::websocket`).
///
/// Requests are handled concurrently and answered as they complete, so a
/// client can keep many requests in flight on one connection.
/// Most requests one connection may have being served at once; beyond that
/// the server stops reading, which pushes back on the client.
const WS_MAX_IN_FLIGHT: usize = 256;

#[get("/ws")]
async fn ws_endpoint(
    who: crate::auth::Principal,
    ws: WebSocket,
    server: &State<DiskServer>,
) -> ws::Channel<'static> {
    let can_write = who.can_write();
    let disks = server.disks.clone();
    let floor = server.floor.clone();

    ws.channel(move |stream| {
        Box::pin(async move {
            let (mut sink, mut source) = stream.split();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(WS_MAX_IN_FLIGHT);
            let permits = Arc::new(tokio::sync::Semaphore::new(WS_MAX_IN_FLIGHT));

            let writer = tokio::spawn(async move {
                while let Some(response) = rx.recv().await {
                    if sink.send(ws::Message::Binary(response)).await.is_err() {
                        break;
                    }
                }
            });

            while let Some(message) = source.next().await {
                match message {
                    Ok(ws::Message::Binary(data)) => {
                        let Ok(permit) = permits.clone().acquire_owned().await else {
                            break;
                        };
                        let (disks, tx, floor) = (disks.clone(), tx.clone(), floor.clone());
                        tokio::spawn(async move {
                            let _ = tx
                                .send(handle_ws_message_as(&data, &disks, &floor, can_write).await)
                                .await;
                            drop(permit);
                        });
                    }
                    Ok(ws::Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }

            drop(tx);
            let _ = writer.await;
            Ok(())
        })
    })
}

/// Serve one encoded request against `disks` and encode the reply.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn handle_ws_message(
    data: &[u8],
    disks: &Arc<Vec<Caddy>>,
    floor: &EpochFloor,
) -> Vec<u8> {
    handle_ws_message_as(data, disks, floor, true).await
}

/// One websocket request from a caller who may (or may not) write. A
/// read-only caller's writes and flushes are refused, and its reads carry no
/// epoch: only writers may raise the fencing floor.
pub(crate) async fn handle_ws_message_as(
    data: &[u8],
    disks: &Arc<Vec<Caddy>>,
    floor: &EpochFloor,
    can_write: bool,
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
    match serve_request(&request, disks, floor).await {
        Ok(payload) => encode_response(WsStatus::Success, id, &payload),
        Err(status) => encode_response(status, id, &[]),
    }
}

async fn serve_request(
    request: &WsRequest<'_>,
    disks: &Arc<Vec<Caddy>>,
    floor: &EpochFloor,
) -> Result<Vec<u8>, WsStatus> {
    let op = request.op().ok_or(WsStatus::InvalidRequest)?;
    let disk_index = request.disk_index() as usize;
    let block = request.block_id();
    let count = request.count();
    // Reads raise the pool floor (a new committer's first reads fence the
    // old one out); only writes are refused (see `fence::ServerFence::admit`).
    if matches!(op, WsOp::Read | WsOp::Write)
        && !floor.admit(disk_index, block, count, request.epoch(), op == WsOp::Write)
    {
        return Err(WsStatus::StaleEpoch);
    }

    let disk = disks
        .get(disk_index)
        .filter(|d| d.is_local())
        .ok_or(WsStatus::ErrorNotFound)?;
    if count > crate::websocket::WS_MAX_BLOCKS {
        return Err(WsStatus::InvalidRequest);
    }

    match op {
        WsOp::Read => {
            let mut buffer = vec![0u8; count as usize * BLOCK_SIZE];
            disk.read_blocks(block, &mut buffer).await?;
            Ok(buffer)
        }
        WsOp::Write => {
            let data = request.write_data().ok_or(WsStatus::InvalidRequest)?;
            disk.write_blocks(block, data).await?;
            Ok(Vec::new())
        }
        WsOp::Flush => {
            disk.flush().await?;
            Ok(Vec::new())
        }
        WsOp::Info => Ok(disk.capacity_blocks().await?.to_le_bytes().to_vec()),
    }
}

#[cfg(test)]
#[path = "tests/api_tests.rs"]
mod tests;
