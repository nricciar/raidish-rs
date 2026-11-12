use crate::cluster::Cluster;
use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::disks::{Caddy, DiskSet};
use crate::fs::{FileSystem, FileSystemError};
use crate::vdev::Pool;
use crate::websocket::{WsOp, WsRequest, WsStatus, encode_response};
use rocket::data::{Data, ToByteUnit};
use rocket::figment::util::map;
use rocket::futures::{SinkExt, StreamExt};
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::{State, get, put};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use ws::WebSocket;

/// The newest pool lease epoch this disk server knows of, from the cluster
/// or from requests. Writes carrying an older epoch are refused.
#[derive(Debug, Default)]
pub struct EpochFloor(pub Arc<AtomicU64>);

impl EpochFloor {
    /// Note `epoch` and say whether a write carrying it may go ahead.
    pub fn admit(&self, epoch: u64) -> bool {
        epoch >= self.0.fetch_max(epoch, Ordering::SeqCst)
    }
}

pub struct DiskServer {
    disks: Arc<Vec<Caddy>>,
    /// Only present if a pool is configured and formatted.
    fs: Option<FileSystem<Pool>>,
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
pub async fn listen(
    disks: Arc<DiskSet>,
    pool: Option<Pool>,
    port: u32,
    cluster: Arc<Cluster>,
    floor: Arc<AtomicU64>,
    pool_map: Option<crate::inspect::PoolMap>,
) {
    // Serving blocks doesn't need a formatted pool (that's how a remote
    // client formats it in the first place).
    let disks = disks.disks.clone();
    // Read-only: this process only serves raw blocks; the pool's writer is
    // whoever formats/serves it remotely.
    let fs = match pool {
        None => None,
        Some(pool) => match FileSystem::load_readonly(pool).await {
            Ok(fs) => Some(fs),
            Err(e) => {
                println!("serving raw disks only ({})", e);
                None
            }
        },
    };
    let server = DiskServer {
        disks,
        fs,
        floor: Arc::new(EpochFloor(floor)),
    };

    rocket::build()
        .manage(server)
        .manage(cluster)
        .manage(crate::dashboard::DashboardState { pool: pool_map })
        .configure(
            rocket::Config::figment()
                .merge(("port", port))
                .merge((
                    "limits",
                    map!["data-form" => "500 MiB", "file" => "500 MiB", "stream" => "500 MiB", "json" => "64 MiB"],
                )),
        )
        .mount(
            "/",
            routes![
                get_block,
                put_block,
                list_disks,
                flush_disks,
                read_file,
                ws_endpoint
            ],
        )
        .mount("/", crate::cluster::routes::routes())
        .mount("/", crate::dashboard::routes())
        .launch()
        .await
        .unwrap();
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
    disk_index: usize,
    block_id: u64,
    data: Data<'_>,
    epoch: HeaderEpoch,
    server: &State<DiskServer>,
) -> Result<Status, Status> {
    if !server.floor.admit(epoch.0) {
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
async fn list_disks(server: &State<DiskServer>) -> String {
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
async fn flush_disks(disk_index: usize, epoch: HeaderEpoch, server: &State<DiskServer>) -> Status {
    if !server.floor.admit(epoch.0) {
        return Status::Conflict;
    }
    match server.local_disk(disk_index) {
        Ok(disk) => match disk.flush().await {
            Ok(()) => Status::Ok,
            Err(_) => Status::InternalServerError,
        },
        Err(status) => status,
    }
}

#[get("/api/v1/files/<path..>")]
async fn read_file(
    path: std::path::PathBuf,
    server: &State<DiskServer>,
) -> Result<Vec<u8>, Status> {
    let fs = server.fs.as_ref().ok_or(Status::ServiceUnavailable)?;
    match fs.read_file(path.as_path()).await {
        Ok(data) => Ok(data),
        Err(err) => match err {
            FileSystemError::FileNotFound => Err(Status::NotFound),
            FileSystemError::NotAFile => Err(Status::BadRequest),
            _ => {
                eprintln!("Error reading file '{}': {}", path.display(), err);
                Err(Status::InternalServerError)
            }
        },
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
async fn ws_endpoint(ws: WebSocket, server: &State<DiskServer>) -> ws::Channel<'static> {
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
                                .send(handle_ws_message(&data, &disks, &floor).await)
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
pub(crate) async fn handle_ws_message(
    data: &[u8],
    disks: &Arc<Vec<Caddy>>,
    floor: &EpochFloor,
) -> Vec<u8> {
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
    // Every request raises the floor (a new writer's first reads fence the
    // old one out), but only writes and flushes are refused.
    if !floor.admit(request.epoch()) && matches!(op, WsOp::Write | WsOp::Flush) {
        return Err(WsStatus::StaleEpoch);
    }
    let disk_index = request.disk_index() as usize;
    let block = request.block_id();
    let count = request.count();

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
