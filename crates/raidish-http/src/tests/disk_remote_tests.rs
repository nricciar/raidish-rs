//! Remote disks (`raidish_core::disk::RemoteDisk`) against an in-process
//! disk server (`crate::test_util::TestDiskServer`): timeouts,
//! reconnects, fencing. Local disks are tested with the storage core.

use crate::coords::*;
use crate::disk::*;
use crate::test_util::create_test_disk_file;

use crate::test_util::{ServerMode, TestDiskServer, TestPool, fast_disk_options, wait_until};
use std::time::{Duration, Instant};

async fn remote_disk(server: &TestDiskServer, idx: usize) -> Disk {
    Disk::open_with(&server.url(idx), fast_disk_options())
        .await
        .unwrap()
}

#[tokio::test]
async fn remote_read_write_roundtrip() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 2).await;
    assert!(disk.is_online());

    let data: Vec<u8> = (0..BLOCK_SIZE * 3).map(|i| (i % 251) as u8).collect();
    disk.write_blocks(
        RawDiskBlock::new(10),
        crate::disk::as_blocks(&data).unwrap(),
    )
    .await
    .unwrap();
    let mut back = vec![0u8; data.len()];
    disk.read_blocks(
        RawDiskBlock::new(10),
        crate::disk::as_blocks_mut(&mut back).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(back, data);
    assert_eq!(
        disk.capacity_blocks().await.unwrap(),
        crate::test_util::TOTAL_BLOCKS
    );
}

#[tokio::test]
async fn remote_disk_fails_fast_and_reconnects() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 0).await;
    let mut reconnects = disk.reconnects().unwrap();
    let gen0 = *reconnects.borrow_and_update();

    server.stop();
    assert!(
        wait_until(2, || !disk.is_online()).await,
        "never noticed the drop"
    );
    let mut buf = vec![0u8; BLOCK_SIZE];
    let t = Instant::now();
    assert!(
        disk.read_block(
            RawDiskBlock::new(0),
            <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap()
        )
        .await
        .is_err()
    );
    assert!(
        t.elapsed() < Duration::from_millis(100),
        "offline disk should fail fast"
    );

    server.restart().await;
    assert!(
        wait_until(5, || disk.is_online()).await,
        "never reconnected"
    );
    assert!(*reconnects.borrow_and_update() > gen0);
    disk.write_block(RawDiskBlock::new(3), &[7u8; BLOCK_SIZE])
        .await
        .unwrap();
    disk.read_block(
        RawDiskBlock::new(3),
        <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(buf, vec![7u8; BLOCK_SIZE]);
}

/// A reconnect loop that finds a live connection keeps it. It used to
/// connect again and replace it, and the replaced connection's close then
/// started another loop, which replaced that one, and so on, for good.
/// Such a loop starts when a request finds the disk offline just as a
/// reconnect completes (the flag cleared before the new connection was
/// installed): `force_reconnect` starts one outright.
#[tokio::test]
async fn a_second_reconnect_loop_keeps_the_live_connection() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 0).await;
    let reconnects = disk.reconnects().unwrap();
    let gen0 = *reconnects.borrow();

    disk.force_reconnect();
    // Many reconnect delays (`fast_disk_options`: 20 ms up to 200 ms).
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(disk.is_online());
    assert_eq!(*reconnects.borrow(), gen0, "replaced a live connection");

    // The real thing still reconnects.
    server.stop();
    assert!(wait_until(2, || !disk.is_online()).await);
    server.restart().await;
    assert!(
        wait_until(5, || disk.is_online()).await,
        "never reconnected"
    );
    let after = *reconnects.borrow();
    assert!(after > gen0);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(*reconnects.borrow(), after, "kept reconnecting");
}

#[tokio::test]
async fn remote_disk_times_out_on_a_silent_server() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 1).await;

    server.set_mode(ServerMode::Silent);
    let t = Instant::now();
    let mut buf = vec![0u8; BLOCK_SIZE];
    let err = disk
        .read_block(
            RawDiskBlock::new(0),
            <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DiskError::Timeout), "got {}", err);
    assert!(t.elapsed() < Duration::from_secs(2));

    server.set_mode(ServerMode::Normal);
    assert!(wait_until(5, || disk.is_online()).await);
    disk.read_block(
        RawDiskBlock::new(0),
        <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_connection_to_a_frozen_server_is_dropped() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 1).await;
    // Pings keep a healthy idle connection alive past the idle timeout.
    tokio::time::sleep(fast_disk_options().idle_timeout * 2).await;
    assert!(disk.is_online(), "healthy idle connection was dropped");

    server.set_mode(ServerMode::Frozen);
    assert!(
        wait_until(3, || !disk.is_online()).await,
        "frozen server not detected"
    );
    server.set_mode(ServerMode::Normal);
    assert!(wait_until(5, || disk.is_online()).await);
}

#[tokio::test]
async fn remote_disk_opens_while_server_is_down() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    server.stop();
    let disk = remote_disk(&server, 4).await;
    assert!(!disk.is_online());
    server.restart().await;
    assert!(wait_until(5, || disk.is_online()).await);
    let mut buf = vec![0u8; BLOCK_SIZE];
    disk.read_block(
        RawDiskBlock::new(0),
        <&mut crate::disk::Block>::try_from(&mut (buf)[..]).unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn raidz_survives_a_hung_disk_within_the_timeout() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let raid = pool.dev_remote(&server, &[4]).await;

    server.set_mode(ServerMode::Silent);
    let data: Vec<u8> = (0..BLOCK_SIZE * 30).map(|i| (i % 249) as u8).collect();
    let t = Instant::now();
    raid.write_blocks(PoolLba::new(300), crate::disk::as_blocks(&data).unwrap())
        .await
        .unwrap();
    raid.flush().await.unwrap();
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "hung disk stalled the write"
    );

    let mut back = vec![0u8; data.len()];
    raid.read_blocks(
        PoolLba::new(300),
        crate::disk::as_blocks_mut(&mut back).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(back, data);
}

#[test]
fn byte_buffers_are_blocks_only_when_whole() {
    let mut bytes = vec![7u8; 2 * BLOCK_SIZE];
    assert_eq!(as_blocks(&bytes).unwrap().len(), 2);
    assert_eq!(as_blocks_mut(&mut bytes).unwrap().len(), 2);
    assert!(as_blocks(&bytes[..BLOCK_SIZE + 1]).is_err());
    assert!(as_blocks_mut(&mut bytes[1..]).is_err());
    assert_eq!(zeroed_blocks(3).as_flattened().len(), 3 * BLOCK_SIZE);
}

#[test]
fn block_pointers_refuse_what_they_cant_describe() {
    use crate::blkptr::BlockPtr;
    use crate::types::Txg;
    let data = vec![1u8; BLOCK_SIZE + 1];
    let p = BlockPtr::new(PoolLba::new(10), 2, &data, Txg::from_raw(5)).unwrap();
    assert_eq!(
        (p.lba, p.asize, p.lsize),
        (PoolLba::new(10), 2, BLOCK_SIZE as u32 + 1)
    );
    assert_eq!(p.checksum, *blake3::hash(&data).as_bytes());
    // More data than its blocks hold, or sizes past u32.
    assert!(BlockPtr::new(PoolLba::new(10), 1, &data, Txg::from_raw(5)).is_err());
    assert!(
        BlockPtr::new(
            PoolLba::new(10),
            u32::MAX as u64 + 1,
            &data,
            Txg::from_raw(5)
        )
        .is_err()
    );
}

/// D6d: a missing local disk is an error, not a new empty disk.
#[tokio::test]
async fn a_missing_disk_file_isnt_created() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("typo");
    let r = Disk::open(path.to_str().unwrap()).await;
    assert!(r.is_err(), "opened a disk that doesn't exist");
    assert!(!path.exists(), "the open created the file");
}

fn locked() -> DiskOptions {
    DiskOptions {
        lock: true,
        ..DiskOptions::default()
    }
}

/// D6d: a locked disk file can't be opened (locked) again until its open
/// is gone; the size is the file's.
#[tokio::test]
async fn a_locked_disk_file_is_held_exclusively() {
    let (_f, path) = create_test_disk_file().await;
    let path = path.to_str().unwrap();
    let first = Disk::open_with(path, locked()).await.unwrap();
    assert_eq!(
        first.capacity_blocks().await.unwrap(),
        100 * 1024 * 1024 / BLOCK_SIZE as u64
    );
    let second = Disk::open_with(path, locked()).await;
    let e = second.expect_err("opened a locked disk twice");
    assert!(e.to_string().contains("in use"), "{}", e);
    // A clone shares the open, and its lock.
    let clone = first.clone();
    drop(first);
    assert!(Disk::open_with(path, locked()).await.is_err());
    drop(clone);
    Disk::open_with(path, locked()).await.unwrap();
}

/// D6d: a pool spec over this node's served disks and others shares the
/// served (locked) opens rather than failing to lock them again.
#[tokio::test]
async fn a_pool_shares_the_served_disks_opens() {
    let (_a, a) = create_test_disk_file().await;
    let (_b, b) = create_test_disk_file().await;
    let (a, b) = (a.to_str().unwrap(), b.to_str().unwrap());
    let served = crate::disks::DiskSet::new_with(&[a], locked()).await;
    assert!(served.disk(PoolDiskIdx::new(0).unwrap()).is_local());
    let pool = crate::disks::DiskSet::new_sharing(&[a, b], locked(), &served).await;
    assert!(
        pool.disk(PoolDiskIdx::new(0).unwrap()).is_local(),
        "the served disk wasn't shared"
    );
    assert!(pool.disk(PoolDiskIdx::new(1).unwrap()).is_local());
    // The other one is locked by the pool's own open.
    assert!(Disk::open_with(b, locked()).await.is_err());
}
