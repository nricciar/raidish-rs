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

/// Helper to create a test RaidZ array with 5 disks
async fn create_test_raidz() -> (RaidZ, Vec<NamedTempFile>) {
    let mut temp_files = Vec::new();
    let mut paths = Vec::new();

    for _ in 0..5 {
        let (temp_file, path) = create_test_disk_file().await;
        temp_files.push(temp_file);
        paths.push(path);
    }

    let stripe = Stripe::new(
        [
            paths[0].to_str().unwrap(),
            paths[1].to_str().unwrap(),
            paths[2].to_str().unwrap(),
            paths[3].to_str().unwrap(),
            paths[4].to_str().unwrap(),
        ],
        DATA_SHARDS,
    )
    .await;

    let raidz = RaidZ::new(stripe).await;
    (raidz, temp_files)
}

#[tokio::test]
async fn test_raidz_creation() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Verify that all disks are local
    for i in 0..5 {
        assert!(raidz.is_local_disk(i), "Disk {} should be local", i);
    }
}

#[tokio::test]
async fn test_basic_read_write() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Write a single block
    let write_buf = [0x42u8; BLOCK_SIZE];
    raidz.write_block(0, &write_buf)
        .await
        .expect("Failed to write block");
    
    // Flush to ensure data is written with parity
    raidz.flush().await.expect("Failed to flush");
    
    // Read the block back
    let mut read_buf = [0u8; BLOCK_SIZE];
    raidz.read_block(0, &mut read_buf)
        .await
        .expect("Failed to read block");
    
    assert_eq!(read_buf, write_buf, "Read data should match written data");
}

#[tokio::test]
async fn test_multiple_blocks_in_single_stripe() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Write 3 blocks (fills one stripe)
    let blocks = [
        [0xAAu8; BLOCK_SIZE],
        [0xBBu8; BLOCK_SIZE],
        [0xCCu8; BLOCK_SIZE],
    ];
    
    for (i, block) in blocks.iter().enumerate() {
        raidz.write_block(i as u64, block)
            .await
            .expect("Failed to write block");
    }
    
    raidz.flush().await.expect("Failed to flush");
    
    // Read back and verify
    for (i, expected) in blocks.iter().enumerate() {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz.read_block(i as u64, &mut read_buf)
            .await
            .expect("Failed to read block");
        
        assert_eq!(&read_buf, expected, "Block {} mismatch", i);
    }
}

#[tokio::test]
async fn test_read_before_flush() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Write data but don't flush
    let write_buf = [0x99u8; BLOCK_SIZE];
    raidz.write_block(5, &write_buf)
        .await
        .expect("Failed to write block");
    
    // Should be able to read from the write buffer
    let mut read_buf = [0u8; BLOCK_SIZE];
    raidz.read_block(5, &mut read_buf)
        .await
        .expect("Failed to read from write buffer");
    
    assert_eq!(read_buf, write_buf, "Should read from write buffer before flush");
}

#[tokio::test]
async fn test_stripe_buffer_completeness() {
    let mut buffer = StripeBuffer::new();
    
    assert!(!buffer.is_complete(), "New buffer should not be complete");
    assert!(!buffer.has_shard(0));
    assert!(!buffer.has_shard(1));
    assert!(!buffer.has_shard(2));
    
    // Add first shard
    let data1 = [0x11u8; BLOCK_SIZE];
    buffer.set_shard(0, &data1);
    assert!(buffer.has_shard(0));
    assert!(!buffer.is_complete());
    
    // Add second shard
    let data2 = [0x22u8; BLOCK_SIZE];
    buffer.set_shard(1, &data2);
    assert!(buffer.has_shard(1));
    assert!(!buffer.is_complete());
    
    // Add third shard - now complete
    let data3 = [0x33u8; BLOCK_SIZE];
    buffer.set_shard(2, &data3);
    assert!(buffer.has_shard(2));
    assert!(buffer.is_complete(), "Buffer should be complete with all 3 shards");
}

#[tokio::test]
async fn test_stripe_buffer_get_shard() {
    let mut buffer = StripeBuffer::new();
    
    let data = [0x42u8; BLOCK_SIZE];
    buffer.set_shard(1, &data);
    
    // Should be able to get the shard we just set
    let mut read_buf = [0u8; BLOCK_SIZE];
    assert!(buffer.get_shard(1, &mut read_buf), "Should find shard 1");
    assert_eq!(read_buf, data);
    
    // Should not be able to get a shard we didn't set
    let mut read_buf2 = [0xFFu8; BLOCK_SIZE];
    assert!(!buffer.get_shard(0, &mut read_buf2), "Should not find shard 0");
}

#[tokio::test]
async fn test_large_sequential_write() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Write 30 blocks (10 stripes)
    for block_id in 0..30 {
        let write_buf = [(block_id % 256) as u8; BLOCK_SIZE];
        raidz.write_block(block_id, &write_buf)
            .await
            .expect("Failed to write block");
    }
    
    raidz.flush().await.expect("Failed to flush");
    
    // Verify all blocks
    for block_id in 0..30 {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz.read_block(block_id, &mut read_buf)
            .await
            .expect("Failed to read block");
        
        let expected = [(block_id % 256) as u8; BLOCK_SIZE];
        assert_eq!(read_buf, expected, "Block {} mismatch", block_id);
    }
}

#[tokio::test]
async fn test_random_access_pattern() {
    let (raidz, _temp_files) = create_test_raidz().await;
    
    // Write in random order
    let block_ids = [7, 2, 15, 4, 11, 0, 8, 3, 12, 6];
    
    for &block_id in &block_ids {
        let write_buf = [block_id as u8; BLOCK_SIZE];
        raidz.write_block(block_id, &write_buf)
            .await
            .expect("Failed to write block");
    }
    
    raidz.flush().await.expect("Failed to flush");
    
    // Read in different random order
    for &block_id in &[0, 3, 6, 2, 8, 4, 12, 7, 15, 11] {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz.read_block(block_id, &mut read_buf)
            .await
            .expect("Failed to read block");
        
        assert!(
            read_buf.iter().all(|&b| b == block_id as u8),
            "Block {} mismatch",
            block_id
        );
    }
}