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
