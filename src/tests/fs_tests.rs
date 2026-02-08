use super::*;
use crate::raidz::RaidZ;
use crate::stripe::Stripe;
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
async fn create_test_fs() -> (FileSystem<RaidZ>, Vec<NamedTempFile>) {
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

    let total_blocks = (100 * 1024 * 1024) / BLOCK_SIZE;

    let _ = FileSystem::format(raidz, total_blocks as u64)
        .await
        .unwrap();

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

    let fs = FileSystem::load(raidz).await.unwrap();

    (fs, temp_files)
}

#[tokio::test]
async fn test_multiple_files_in_directory() {
    let (fs, _temp_files) = create_test_fs().await;

    // Create a directory
    fs.mkdir(Path::new("/files")).await.unwrap();

    // Write multiple files
    for i in 0..5 {
        let path = PathBuf::from(format!("/files/file{}.txt", i));
        let data = format!("Content of file {}", i);
        fs.write_file(&path, data.as_bytes()).await.unwrap();
    }

    // Verify all files can be read back
    for i in 0..5 {
        let path = PathBuf::from(format!("/files/file{}.txt", i));
        let data = fs.read_file(&path).await.unwrap();
        let expected = format!("Content of file {}", i);
        assert_eq!(data, expected.as_bytes());
    }
}

#[tokio::test]
async fn test_nested_directory_creation() {
    let (fs, _temp_files) = create_test_fs().await;

    // Create nested directories one at a time
    let paths = vec!["/level1", "/level1/level2", "/level1/level2/level3"];

    for path in paths {
        fs.mkdir(Path::new(path)).await.unwrap();
    }

    // Create a file in the deepest directory
    let file_path = Path::new("/level1/level2/level3/deep.txt");
    fs.write_file(file_path, b"Deep content").await.unwrap();

    // Verify file is readable
    let data = fs.read_file(file_path).await.unwrap();
    assert_eq!(data, b"Deep content");
}
