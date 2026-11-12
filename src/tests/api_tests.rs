use super::*;
use crate::disk::{BLOCK_SIZE, Disk, DiskError};
use crate::test_util::{TestDiskServer, TestPool, fast_disk_options};
use crate::websocket::{WsOp, WsStatus, decode_response, encode_request};

async fn local_disks(pool: &TestPool) -> Arc<Vec<Caddy>> {
    let mut disks = Vec::new();
    for p in &pool.paths {
        let p = p.to_str().unwrap();
        disks.push(Caddy::Healthy(p.to_string(), Disk::open(p).await.unwrap()));
    }
    Arc::new(disks)
}

async fn send(
    disks: &Arc<Vec<Caddy>>,
    floor: &EpochFloor,
    op: WsOp,
    block: u64,
    epoch: u64,
    data: Option<&[u8]>,
) -> (WsStatus, Vec<u8>) {
    let req = encode_request(op, 0, block, 1, 42, epoch, data);
    let resp = handle_ws_message(&req, disks, floor).await;
    let (status, id) = decode_response(&resp).unwrap();
    assert_eq!(id, 42);
    (
        WsStatus::try_from(status).unwrap(),
        resp[crate::websocket::WS_RESPONSE_HEADER_LEN..].to_vec(),
    )
}

#[tokio::test]
async fn stale_epoch_writes_are_refused() {
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let floor = EpochFloor::default();
    let a = vec![0xAAu8; BLOCK_SIZE];
    let b = vec![0xBBu8; BLOCK_SIZE];

    // Writer at epoch 5.
    let (s, _) = send(&disks, &floor, WsOp::Write, 7, 5, Some(&a)).await;
    assert_eq!(s, WsStatus::Success);

    // A new writer (epoch 6) shows up; its first read raises the floor.
    let (s, data) = send(&disks, &floor, WsOp::Read, 7, 6, None).await;
    assert_eq!(s, WsStatus::Success);
    assert_eq!(data, a);

    // The old writer is now fenced: writes and flushes are refused, the
    // block is untouched. Reads still work.
    let (s, _) = send(&disks, &floor, WsOp::Write, 7, 5, Some(&b)).await;
    assert_eq!(s, WsStatus::StaleEpoch);
    let (s, _) = send(&disks, &floor, WsOp::Flush, 0, 5, None).await;
    assert_eq!(s, WsStatus::StaleEpoch);
    let (s, _) = send(&disks, &floor, WsOp::Write, 7, 0, Some(&b)).await;
    assert_eq!(s, WsStatus::StaleEpoch, "no epoch at all is older still");
    let (s, data) = send(&disks, &floor, WsOp::Read, 7, 5, None).await;
    assert_eq!(s, WsStatus::Success);
    assert_eq!(data, a);

    // The new writer can write.
    let (s, _) = send(&disks, &floor, WsOp::Write, 7, 6, Some(&b)).await;
    assert_eq!(s, WsStatus::Success);
    let (_, data) = send(&disks, &floor, WsOp::Read, 7, 6, None).await;
    assert_eq!(data, b);
}

#[tokio::test]
async fn floor_from_the_cluster_fences_too() {
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let floor = EpochFloor::default();
    // The cluster applied a pool grant at epoch 3 (no request seen yet).
    floor.0.fetch_max(3, Ordering::SeqCst);
    let a = vec![1u8; BLOCK_SIZE];
    let (s, _) = send(&disks, &floor, WsOp::Write, 1, 2, Some(&a)).await;
    assert_eq!(s, WsStatus::StaleEpoch);
    let (s, _) = send(&disks, &floor, WsOp::Write, 1, 3, Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}

#[tokio::test]
async fn unfenced_server_accepts_epoch_zero() {
    // No leases anywhere: the old single-writer setup keeps working.
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let floor = EpochFloor::default();
    let a = vec![9u8; BLOCK_SIZE];
    let (s, _) = send(&disks, &floor, WsOp::Write, 3, 0, Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}

#[tokio::test]
async fn remote_client_sees_stale_epoch() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    // Someone with a newer lease than ours (this process sends epoch 0).
    server.floor.0.fetch_max(1, Ordering::SeqCst);
    let disk = Disk::open_with(&server.url(0), fast_disk_options())
        .await
        .unwrap();
    let err = disk.write_block(5, &[0u8; BLOCK_SIZE]).await.unwrap_err();
    assert!(matches!(err, DiskError::StaleEpoch), "got {}", err);
    let mut buf = vec![0u8; BLOCK_SIZE];
    disk.read_block(5, &mut buf).await.unwrap();
}

#[test]
fn old_protocol_headers_are_rejected() {
    // A 23-byte (pre-epoch) read request is too short to parse.
    let mut req = encode_request(WsOp::Read, 0, 0, 1, 7, 0, None);
    req.truncate(23);
    assert!(WsRequest::parse(&req).is_none());
}
