use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::disks::{Caddy, DiskSet};
use crate::fs::{FileSystem, FileSystemError};
use crate::vdev::Pool;
use crate::websocket::{WsOp, WsRequest, WsStatus, encode_response};
use etcd_client::{Client, GetOptions, LockOptions};
use rocket::data::{Data, ToByteUnit};
use rocket::figment::util::map;
use rocket::futures::{SinkExt, StreamExt};
use rocket::http::Status;
use rocket::{State, get, put};
use serde::Deserialize;
use std::sync::Arc;
use ws::WebSocket;

#[derive(Deserialize, Debug)]
pub struct EtcdConfig {
    pub enable: bool,
    pub etcd_nodes: Vec<String>,
}

pub struct DiskServer {
    disks: Arc<Vec<Caddy>>,
    /// Only present if a pool is configured and formatted.
    fs: Option<FileSystem<Pool>>,
    etcd_client: Option<Client>,
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
/// if configured, backs the read-only file endpoint.
pub async fn listen(
    disks: Arc<DiskSet>,
    pool: Option<Pool>,
    port: u32,
    etcd_config: Option<EtcdConfig>,
    id: &str,
) {
    {
        let etcd_client = match etcd_config {
            Some(etcd_config) if etcd_config.enable => {
                let mut etcd_cli = Client::connect(etcd_config.etcd_nodes, None).await.unwrap();

                // register us with etcd
                let lease_id = etcd_cli.lease_grant(10, None).await.unwrap().id();
                tokio::spawn(keepalive(etcd_cli.clone(), lease_id));

                register_node(&mut etcd_cli, id, lease_id).await.unwrap();

                tokio::spawn(elect_leader(etcd_cli.clone(), lease_id, id.to_string()));

                Some(etcd_cli)
            }
            _ => None,
        };

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
            etcd_client,
        };

        rocket::build()
            .manage(server)
            .configure(rocket::Config::figment().merge(("port", port)).merge((
                "limits",
                map!["data-form" => "500 MiB", "file" => "500 MiB", "stream" => "500 MiB"],
            )))
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
            .launch()
            .await
            .unwrap();
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
    server: &State<DiskServer>,
) -> Result<Status, Status> {
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
    if let Some(mut etcd) = server.etcd_client.clone() {
        if let Ok(resp) = etcd.leader("/myapp/nodes/leader").await
            && let Some(kv) = resp.kv()
        {
            println!("leader: {:?} = {:?}", kv.key_str(), kv.value_str());
        }
        if let Ok(resp) = etcd
            .get("/myapp/nodes/", Some(GetOptions::new().with_prefix()))
            .await
        {
            for kv in resp.kvs() {
                println!("kv: {:?} -- {:?}", kv.key_str(), kv.value_str());
            }
        }
    }

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
async fn flush_disks(disk_index: usize, server: &State<DiskServer>) -> Status {
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
                        let (disks, tx) = (disks.clone(), tx.clone());
                        tokio::spawn(async move {
                            let _ = tx.send(handle_ws_message(&data, &disks).await).await;
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
pub(crate) async fn handle_ws_message(data: &[u8], disks: &Arc<Vec<Caddy>>) -> Vec<u8> {
    let Some(request) = WsRequest::parse(data) else {
        // Echo the id if there's one, so the client's request fails instead
        // of waiting for a reply that never comes.
        let id = data
            .get(15..23)
            .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
        return encode_response(WsStatus::InvalidRequest, id, &[]);
    };
    let id = request.id();
    match serve_request(&request, disks).await {
        Ok(payload) => encode_response(WsStatus::Success, id, &payload),
        Err(status) => encode_response(status, id, &[]),
    }
}

async fn serve_request(
    request: &WsRequest<'_>,
    disks: &Arc<Vec<Caddy>>,
) -> Result<Vec<u8>, WsStatus> {
    let op = request.op().ok_or(WsStatus::InvalidRequest)?;
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

async fn keepalive(mut client: Client, lease_id: i64) {
    let (mut ka, _ks) = client.lease_keep_alive(lease_id).await.unwrap();

    while ka.keep_alive().await.is_ok() {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

    // If this exits, node is considered dead
}

async fn elect_leader(mut client: Client, lease_id: i64, value: String) {
    let resp = client
        .campaign("/myapp/nodes/leader", value, lease_id)
        .await
        .unwrap();
    let leader = resp.leader().unwrap();

    // if we are here this node is the node leader

    println!(
        "election name:{:?}, leaseId:{:?}",
        leader.name_str(),
        leader.lease()
    );
}

async fn register_node(
    client: &mut Client,
    node_id: &str,
    lease_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let lock_options = LockOptions::new().with_lease(lease_id);
    client
        .lock(format!("/myapp/nodes/{}", node_id), Some(lock_options))
        .await?;

    Ok(())
}

#[cfg(test)]
#[path = "tests/api_tests.rs"]
mod tests;
