use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::raidz::RaidZ;
use crate::stripe::Caddy;
use crate::websocket::{WsOp, WsRequest, WsResponseBuilder, WsStatus};
use etcd_client::{Client, GetOptions, LockOptions};
use rocket::data::{Data, ToByteUnit};
use rocket::figment::util::map;
use rocket::futures::{SinkExt, StreamExt};
use rocket::http::Status;
use rocket::{State, get, put};
use std::sync::Arc;
use ws::WebSocket;

pub struct RaidZServer {
    raidz: RaidZ,
}

impl RaidZ {
    /// Start an HTTP/WebSocket server that exposes all local disks in this RaidZ array
    /// Only local disks are exposed - remote disks are skipped
    pub async fn listen(self, port: u32) {
        /*let mut etcd_cli = Client::connect(
            ["http://127.0.0.1:12379","http://127.0.0.1:22379","http://127.0.0.1:32379"],
            None
        ).await.unwrap();
        self.etcd_client = Some(etcd_cli.clone());

        // register us with etcd
        let lease_id = etcd_cli.lease_grant(10, None).await.unwrap().id();
        tokio::spawn(keepalive(etcd_cli.clone(), lease_id));

        register_node(
            &mut etcd_cli,
            &self.id,
            lease_id
        ).await.unwrap();

        tokio::spawn(elect_leader(etcd_cli.clone(), lease_id, self.id.to_string()));*/

        let server = RaidZServer { raidz: self };

        rocket::build()
            .manage(server)
            .configure(rocket::Config::figment().merge(("port", port)).merge((
                "limits",
                map!["data-form" => "500 MiB", "file" => "500 MiB", "stream" => "500 MiB"],
            )))
            .mount(
                "/",
                routes![get_block, put_block, list_disks, flush_disks, ws_endpoint],
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
    server: &State<RaidZServer>,
) -> Result<Vec<u8>, Status> {
    // Check if disk_index is valid
    if disk_index >= server.raidz.stripe.disks.len() {
        return Err(Status::NotFound);
    }

    // Check if this is a local disk (not remote)
    if !server.raidz.is_local_disk(disk_index) {
        return Err(Status::BadRequest);
    }

    let mut buf = vec![0u8; BLOCK_SIZE];
    server.raidz.stripe.disks[disk_index]
        .read_block(block_id, &mut buf)
        .await
        .unwrap();

    Ok(buf)
}

/// HTTP: PUT /api/v1/disks/{disk_index}/blocks/{block_id}
/// Writes a block directly to a specific disk in the array
#[put("/api/v1/disks/<disk_index>/blocks/<block_id>", data = "<data>")]
async fn put_block(
    disk_index: usize,
    block_id: u64,
    data: Data<'_>,
    server: &State<RaidZServer>,
) -> Result<Status, Status> {
    let bytes = data
        .open(10.mebibytes())
        .into_bytes()
        .await
        .map_err(|_| Status::BadRequest)?;

    if bytes.len() != BLOCK_SIZE {
        return Err(Status::BadRequest);
    }

    // Check if disk_index is valid
    if disk_index >= server.raidz.stripe.disks.len() {
        return Err(Status::NotFound);
    }

    // Check if this is a local disk (not remote)
    if !server.raidz.is_local_disk(disk_index) {
        return Err(Status::BadRequest);
    }

    let bytes = &bytes.as_slice().try_into().unwrap();
    server.raidz.stripe.disks[disk_index]
        .write_block(block_id, &bytes)
        .await
        .unwrap();

    Ok(Status::Ok)
}

/// HTTP: GET /api/v1/disks - List disk indices and their types (local/remote)
#[get("/api/v1/disks")]
async fn list_disks(server: &State<RaidZServer>) -> String {
    /*let resp = node.etcd_client.clone().unwrap().leader("/myapp/nodes/leader").await.unwrap();
    let kv = resp.kv().unwrap();
    println!("key is {:?}", kv.key_str());
    println!("value is {:?}", kv.value_str());

    let resp = node.etcd_client.clone().unwrap()
        .get(
            "/myapp/nodes/",
            Some(GetOptions::new().with_prefix())
        )
        .await.unwrap();
    for kv in resp.kvs() {
        println!("kv: {:?} -- {:?}", kv.key_str(), kv.value_str());
    }*/

    let disk_info: Vec<_> = (0..server.raidz.stripe.disks.len())
        .map(|i| {
            serde_json::json!({
                "index": i,
                "is_local": server.raidz.is_local_disk(i),
                "servable": server.raidz.is_local_disk(i)
            })
        })
        .collect();

    serde_json::to_string(&disk_info).unwrap()
}

/// HTTP: POST /api/v1/flush - Flush all local disks
#[post("/api/v1/disks/<disk_index>/flush")]
async fn flush_disks(disk_index: usize, server: &State<RaidZServer>) -> Status {
    // Check if disk_index is valid
    if disk_index >= server.raidz.stripe.disks.len() {
        return Status::NotFound;
    }

    // Check if this is a local disk (not remote)
    if !server.raidz.is_local_disk(disk_index) {
        return Status::BadRequest;
    }

    server.raidz.stripe.disks[disk_index].flush().await.unwrap();

    Status::Ok
}

/// WebSocket endpoint for efficient block I/O
/// Message format (binary):
///   Request:  [op:1][disk_index:2][block_id:8][data:0/4096]
///     op=0: read, op=1: write
///   Response: [status:1][data:0/4096]
///     status=0: success, status=1: error (not found/not local), status=2: invalid request
#[get("/ws")]
async fn ws_endpoint(ws: WebSocket, server: &State<RaidZServer>) -> ws::Channel<'static> {
    let disks = server.raidz.stripe.disks.clone();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            while let Some(message) = stream.next().await {
                if let Ok(ws::Message::Binary(data)) = message {
                    let response = match handle_ws_message(&data, &disks).await {
                        Ok(resp) => resp,
                        Err(err) => WsResponseBuilder::error(err),
                    };
                    let _ = stream.send(ws::Message::Binary(response)).await;
                }
            }
            Ok(())
        })
    })
}

async fn handle_ws_message(data: &[u8], disks: &Arc<Vec<Caddy>>) -> Result<Vec<u8>, WsStatus> {
    let request = WsRequest::parse(data)?;

    let op = request.op()?;

    let disk_index = request.disk_index() as usize;
    let block_id = request.block_id();

    // Validate disk access
    if disk_index >= disks.len() || !disks[disk_index].is_local() {
        return Err(WsStatus::ErrorNotFound);
    }

    match op {
        WsOp::Read => {
            let mut buffer = [0u8; BLOCK_SIZE];
            disks[disk_index].read_block(block_id, &mut buffer).await?;
            Ok(WsResponseBuilder::with_data(&buffer))
        }
        WsOp::Write => {
            let write_data = request.write_data().ok_or(WsStatus::InvalidRequest)?;

            let block_array: &[u8; BLOCK_SIZE] = match write_data.try_into() {
                Ok(arr) => arr,
                Err(_) => return Err(WsStatus::InvalidRequest),
            };

            disks[disk_index].write_block(block_id, block_array).await?;

            Ok(WsResponseBuilder::success())
        }
        WsOp::Flush => {
            disks[disk_index].flush().await?;
            Ok(WsResponseBuilder::success())
        }
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
