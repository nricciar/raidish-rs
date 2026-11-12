use super::*;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::fs::File;

/// Helper to create a 100MB temporary file for testing
async fn create_test_disk_file() -> (NamedTempFile, PathBuf) {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path().to_path_buf();

    // Allocate 100MB (similar to fallocate)
    let size = 100 * 1024 * 1024; // 100MB
    let file = File::create(&path).await.expect("Failed to open temp file");

    // Write zeros to allocate space
    file.set_len(size).await.expect("Failed to set file length");
    file.sync_all().await.expect("Failed to sync file");

    (temp_file, path)
}

#[tokio::test]
async fn test_open_local_disk() {
    let (_temp_file, path) = create_test_disk_file().await;

    let disk = Disk::open(path.to_str().unwrap())
        .await
        .expect("Failed to open disk");

    assert!(disk.is_local(), "Disk should be local");
}

#[tokio::test]
async fn test_read_write_single_block() {
    let (_temp_file, path) = create_test_disk_file().await;

    let disk = Disk::open(path.to_str().unwrap())
        .await
        .expect("Failed to open disk");

    // Create test data
    let mut write_buf = [0u8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        write_buf[i] = (i % 256) as u8;
    }

    // Write block
    disk.write_block(0, &write_buf)
        .await
        .expect("Failed to write block");

    // Flush to ensure data is written
    disk.flush().await.expect("Failed to flush");

    // Read block back
    let mut read_buf = [0u8; BLOCK_SIZE];
    disk.read_block(0, &mut read_buf)
        .await
        .expect("Failed to read block");

    // Verify data matches
    assert_eq!(write_buf, read_buf, "Written and read data should match");
}

#[tokio::test]
async fn test_write_read_multiple_blocks() {
    let (_temp_file, path) = create_test_disk_file().await;

    let disk = Disk::open(path.to_str().unwrap())
        .await
        .expect("Failed to open disk");

    // Write different patterns to different blocks
    for block_id in 0..10 {
        let mut write_buf = [0u8; BLOCK_SIZE];
        for i in 0..BLOCK_SIZE {
            write_buf[i] = ((block_id + i) % 256) as u8;
        }

        disk.write_block(block_id as u64, &write_buf)
            .await
            .expect("Failed to write block");
    }

    disk.flush().await.expect("Failed to flush");

    // Read back and verify each block
    for block_id in 0..10 {
        let mut read_buf = [0u8; BLOCK_SIZE];
        disk.read_block(block_id, &mut read_buf)
            .await
            .expect("Failed to read block");

        // Verify pattern
        for i in 0..BLOCK_SIZE {
            let expected = ((block_id + i as u64) % 256) as u8;
            assert_eq!(
                read_buf[i], expected,
                "Mismatch at block {} offset {}",
                block_id, i
            );
        }
    }
}

#[tokio::test]
async fn test_overwrite_block() {
    let (_temp_file, path) = create_test_disk_file().await;

    let disk = Disk::open(path.to_str().unwrap())
        .await
        .expect("Failed to open disk");

    // Write initial data
    let write_buf1 = [0xAAu8; BLOCK_SIZE];
    disk.write_block(5, &write_buf1)
        .await
        .expect("Failed to write first time");

    // Overwrite with different data
    let write_buf2 = [0x55u8; BLOCK_SIZE];
    disk.write_block(5, &write_buf2)
        .await
        .expect("Failed to write second time");

    disk.flush().await.expect("Failed to flush");

    // Read back
    let mut read_buf = [0u8; BLOCK_SIZE];
    disk.read_block(5, &mut read_buf)
        .await
        .expect("Failed to read block");

    // Should match second write
    assert_eq!(
        read_buf, write_buf2,
        "Block should contain overwritten data"
    );
}

#[tokio::test]
async fn test_non_sequential_block_access() {
    let (_temp_file, path) = create_test_disk_file().await;

    let disk = Disk::open(path.to_str().unwrap())
        .await
        .expect("Failed to open disk");

    // Write to blocks in non-sequential order
    let block_ids = [100, 50, 75, 25, 90];

    for &block_id in &block_ids {
        let write_buf = [block_id as u8; BLOCK_SIZE];
        disk.write_block(block_id, &write_buf)
            .await
            .expect("Failed to write block");
    }

    disk.flush().await.expect("Failed to flush");

    // Read back in different order
    for &block_id in &[25, 50, 75, 90, 100] {
        let mut read_buf = [0u8; BLOCK_SIZE];
        disk.read_block(block_id, &mut read_buf)
            .await
            .expect("Failed to read block");

        assert!(
            read_buf.iter().all(|&b| b == block_id as u8),
            "Block {} data mismatch",
            block_id
        );
    }
}

#[tokio::test]
async fn test_flush_persistence() {
    let (_temp_file, path) = create_test_disk_file().await;
    let path_str = path.to_str().unwrap().to_string();

    // Write data and flush
    {
        let disk = Disk::open(&path_str).await.expect("Failed to open disk");

        let write_buf = [0x42u8; BLOCK_SIZE];
        disk.write_block(10, &write_buf)
            .await
            .expect("Failed to write block");

        disk.flush().await.expect("Failed to flush");
    } // disk dropped here

    // Re-open and verify data persisted
    {
        let disk = Disk::open(&path_str).await.expect("Failed to re-open disk");

        let mut read_buf = [0u8; BLOCK_SIZE];
        disk.read_block(10, &mut read_buf)
            .await
            .expect("Failed to read block");

        assert!(
            read_buf.iter().all(|&b| b == 0x42),
            "Data should persist after flush and re-open"
        );
    }
}

// ---- remote (websocket) disks ----

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
    disk.write_blocks(10, &data).await.unwrap();
    let mut back = vec![0u8; data.len()];
    disk.read_blocks(10, &mut back).await.unwrap();
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
    assert!(disk.read_block(0, &mut buf).await.is_err());
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
    disk.write_block(3, &[7u8; BLOCK_SIZE]).await.unwrap();
    disk.read_block(3, &mut buf).await.unwrap();
    assert_eq!(buf, vec![7u8; BLOCK_SIZE]);
}

#[tokio::test]
async fn remote_disk_times_out_on_a_silent_server() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let disk = remote_disk(&server, 1).await;

    server.set_mode(ServerMode::Silent);
    let t = Instant::now();
    let mut buf = vec![0u8; BLOCK_SIZE];
    let err = disk.read_block(0, &mut buf).await.unwrap_err();
    assert!(matches!(err, DiskError::Timeout), "got {}", err);
    assert!(t.elapsed() < Duration::from_secs(2));

    server.set_mode(ServerMode::Normal);
    assert!(wait_until(5, || disk.is_online()).await);
    disk.read_block(0, &mut buf).await.unwrap();
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
    disk.read_block(0, &mut buf).await.unwrap();
}

#[tokio::test]
async fn raidz_survives_a_hung_disk_within_the_timeout() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let raid = pool.dev_remote(&server, &[4]).await;

    server.set_mode(ServerMode::Silent);
    let data: Vec<u8> = (0..BLOCK_SIZE * 30).map(|i| (i % 249) as u8).collect();
    let t = Instant::now();
    raid.write_blocks(300, &data).await.unwrap();
    raid.flush().await.unwrap();
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "hung disk stalled the write"
    );

    let mut back = vec![0u8; data.len()];
    raid.read_blocks(300, &mut back).await.unwrap();
    assert_eq!(back, data);
}
