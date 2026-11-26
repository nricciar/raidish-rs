use super::*;
use crate::coords::*;
use crate::disk::{BLOCK_SIZE, Disk, DiskError, RawDisk};
use crate::test_util::{TestDiskServer, TestPool, fast_disk_options};
use crate::types::{Epoch, ep};
use crate::websocket::{WsOp, WsStatus, decode_response, encode_request};

use crate::disks::Caddy;

/// The pool's disks, served behind a fence that has done its startup.
async fn local_disks(pool: &TestPool) -> FencedDisks {
    let mut disks = Vec::new();
    for p in &pool.paths {
        let p = p.to_str().unwrap();
        disks.push(Caddy::Healthy(p.to_string(), Disk::open(p).await.unwrap()));
    }
    FencedDisks::new(Arc::new(disks), Arc::new(EpochFloor::ready()))
}

async fn send(
    disks: &FencedDisks,
    op: WsOp,
    block: u64,
    epoch: Epoch,
    data: Option<&[u8]>,
) -> (WsStatus, Vec<u8>) {
    let req = encode_request(
        op,
        0,
        block,
        1,
        42,
        crate::fence::Stamp::Pool(epoch),
        std::time::Duration::ZERO,
        data,
    );
    let resp = handle_ws_message(&req, disks).await;
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
    let a = vec![0xAAu8; BLOCK_SIZE];
    let b = vec![0xBBu8; BLOCK_SIZE];

    // Writer at epoch 5.
    let (s, _) = send(&disks, WsOp::Write, 7, ep(5), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);

    // A new writer (epoch 6) shows up; its first read raises the floor.
    let (s, data) = send(&disks, WsOp::Read, 7, ep(6), None).await;
    assert_eq!(s, WsStatus::Success);
    assert_eq!(data, a);

    // The old writer is now fenced: writes are refused, the block is
    // untouched. Reads still work, and so do flushes (they only make
    // admitted writes durable).
    let (s, _) = send(&disks, WsOp::Write, 7, ep(5), Some(&b)).await;
    assert_eq!(s, WsStatus::StaleEpoch);
    let (s, _) = send(&disks, WsOp::Flush, 0, ep(5), None).await;
    assert_eq!(s, WsStatus::Success);
    let (s, _) = send(&disks, WsOp::Write, 7, ep(0), Some(&b)).await;
    assert_eq!(s, WsStatus::StaleEpoch, "no epoch at all is older still");
    let (s, data) = send(&disks, WsOp::Read, 7, ep(5), None).await;
    assert_eq!(s, WsStatus::Success);
    assert_eq!(data, a);

    // The new writer can write.
    let (s, _) = send(&disks, WsOp::Write, 7, ep(6), Some(&b)).await;
    assert_eq!(s, WsStatus::Success);
    let (_, data) = send(&disks, WsOp::Read, 7, ep(6), None).await;
    assert_eq!(data, b);
}

#[tokio::test]
async fn floor_from_the_cluster_fences_too() {
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    // The cluster applied a pool grant at epoch 3 (no request seen yet).
    disks.floor().raise_pool(ep(3));
    let a = vec![1u8; BLOCK_SIZE];
    let (s, _) = send(&disks, WsOp::Write, 1, ep(2), Some(&a)).await;
    assert_eq!(s, WsStatus::StaleEpoch);
    let (s, _) = send(&disks, WsOp::Write, 1, ep(3), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}

#[tokio::test]
async fn unfenced_server_accepts_epoch_zero() {
    // No leases anywhere: the old single-writer setup keeps working.
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let a = vec![9u8; BLOCK_SIZE];
    let (s, _) = send(&disks, WsOp::Write, 3, ep(0), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}

#[tokio::test]
async fn remote_client_sees_stale_epoch() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    // Someone with a newer lease than ours (this process sends epoch 0).
    server.floor.raise_pool(ep(1));
    let disk = Disk::open_with(&server.url(0), fast_disk_options())
        .await
        .unwrap();
    let err = disk
        .write_block(RawDiskBlock::new(5), &[0u8; BLOCK_SIZE])
        .await
        .unwrap_err();
    assert!(matches!(err, DiskError::StaleEpoch), "got {}", err);
    let mut buf = vec![0u8; BLOCK_SIZE];
    disk.read_block(
        RawDiskBlock::new(5),
        <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap(),
    )
    .await
    .unwrap();
}

/// A refused pool epoch stops the pool that sent it, not every pool in the
/// process: a committer and a writer in one process fence apart.
#[tokio::test]
async fn a_stale_pool_epoch_stops_only_its_own_pool() {
    use crate::fence::ClientFence;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    static OLD: AtomicBool = AtomicBool::new(false);
    static NEW: AtomicBool = AtomicBool::new(false);
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    server.floor.raise_pool(ep(2));
    let url = server.url(0);
    let open = |epoch, hook: fn()| {
        let url = url.clone();
        async move {
            let fence = Arc::new(ClientFence::default());
            fence.set_pool_epoch(ep(epoch));
            fence.on_stale(hook);
            crate::disks::DiskSet::new_fenced(&[url.as_str()], fast_disk_options(), fence).await
        }
    };
    let old = open(1, || OLD.store(true, SeqCst)).await;
    let new = open(2, || NEW.store(true, SeqCst)).await;
    let block = [[0u8; BLOCK_SIZE]];
    let err = old
        .write_shards(PoolDiskIdx::new(0).unwrap(), DiskShard::new(5), &block)
        .await
        .unwrap_err();
    assert!(matches!(err, DiskError::StaleEpoch), "got {}", err);
    new.write_shards(PoolDiskIdx::new(0).unwrap(), DiskShard::new(5), &block)
        .await
        .unwrap();
    assert!(OLD.load(SeqCst), "the refused pool's hook ran");
    assert!(!NEW.load(SeqCst), "the other pool's didn't");
}

/// Disk URLs ask for this block protocol (keeping any query they have).
#[test]
fn disk_urls_ask_for_this_block_protocol() {
    use crate::websocket::{BLOCK_PROTOCOL, versioned};
    let v = BLOCK_PROTOCOL;
    assert_eq!(
        versioned("wss://n1:8882/ws"),
        format!("wss://n1:8882/ws?v={}", v)
    );
    assert_eq!(
        versioned("wss://n1/ws?a=b"),
        format!("wss://n1/ws?a=b&v={}", v)
    );
}

#[test]
fn old_protocol_headers_are_rejected() {
    // A 23-byte (pre-epoch) read request is too short to parse.
    let mut req = encode_request(
        WsOp::Read,
        0,
        0,
        1,
        7,
        crate::fence::Stamp::Pool(ep(0)),
        std::time::Duration::ZERO,
        None,
    );
    req.truncate(23);
    assert!(WsRequest::parse(&req).is_none());
}

#[tokio::test]
async fn readonly_connections_cannot_write_or_fence() {
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let a = vec![0xAAu8; BLOCK_SIZE];
    let ro = |op, block, epoch, data: Option<&[u8]>| {
        let req = encode_request(
            op,
            0,
            block,
            1,
            9,
            crate::fence::Stamp::Pool(epoch),
            std::time::Duration::ZERO,
            data,
        );
        let disks = &disks;
        async move {
            let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let now = tokio::time::Instant::now();
            let resp = handle_ws_message_as(&req, disks, false, now, closed).await;
            let (status, id) = decode_response(&resp).unwrap();
            assert_eq!(id, 9);
            WsStatus::try_from(status).unwrap()
        }
    };

    let (s, _) = send(&disks, WsOp::Write, 7, ep(5), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
    assert_eq!(
        ro(WsOp::Write, 7, ep(99), Some(&a)).await,
        WsStatus::Forbidden
    );
    assert_eq!(ro(WsOp::Flush, 0, ep(99), None).await, WsStatus::Forbidden);
    // A read claiming a huge epoch doesn't raise the floor...
    assert_eq!(ro(WsOp::Read, 7, ep(1_000), None).await, WsStatus::Success);
    // ...so the real writer at epoch 5 is not fenced out.
    let (s, _) = send(&disks, WsOp::Write, 7, ep(5), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}

#[tokio::test]
async fn malformed_requests_dont_raise_the_floor() {
    let pool = TestPool::new().await;
    let disks = local_disks(&pool).await;
    let a = vec![1u8; BLOCK_SIZE];
    let huge = ep(u64::MAX);
    // Unknown disk, a count past the limit, data not matching the count:
    // refused before admission, so none of them fences anyone out.
    let bad_disk = encode_request(
        WsOp::Write,
        99,
        1,
        1,
        1,
        crate::fence::Stamp::Pool(huge),
        std::time::Duration::ZERO,
        Some(&a),
    );
    let bad_count = encode_request(
        WsOp::Read,
        0,
        1,
        u32::MAX,
        2,
        crate::fence::Stamp::Pool(huge),
        std::time::Duration::ZERO,
        None,
    );
    let short = encode_request(
        WsOp::Write,
        0,
        1,
        2,
        3,
        crate::fence::Stamp::Pool(huge),
        std::time::Duration::ZERO,
        Some(&a),
    );
    for req in [bad_disk, bad_count, short] {
        let (status, _) = decode_response(&handle_ws_message(&req, &disks).await).unwrap();
        assert_ne!(WsStatus::try_from(status).unwrap(), WsStatus::Success);
    }
    let (s, _) = send(&disks, WsOp::Write, 1, ep(1), Some(&a)).await;
    assert_eq!(s, WsStatus::Success);
}
