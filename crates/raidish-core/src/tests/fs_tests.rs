use super::*;
use crate::role::CommitterFs;
use crate::vdev::Pool;
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
async fn create_test_fs() -> (CommitterFs<Pool>, Vec<NamedTempFile>) {
    let mut temp_files = Vec::new();
    let mut paths = Vec::new();

    for _ in 0..5 {
        let (temp_file, path) = create_test_disk_file().await;
        temp_files.push(temp_file);
        paths.push(path);
    }

    let p: Vec<&str> = paths.iter().map(|p| p.to_str().unwrap()).collect();
    let disks = std::sync::Arc::new(crate::disks::DiskSet::new(&p).await);
    let raidz = crate::vdev::Pool::raidz(disks, 2);

    let total_blocks = (100 * 1024 * 1024) / BLOCK_SIZE;

    let _ = FileSystem::format(raidz, total_blocks as u64)
        .await
        .unwrap();

    let p: Vec<&str> = paths.iter().map(|p| p.to_str().unwrap()).collect();
    let disks = std::sync::Arc::new(crate::disks::DiskSet::new(&p).await);
    let raidz = crate::vdev::Pool::raidz(disks, 2);

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

#[test]
fn uberblocks_validate_what_they_claim() {
    use crate::types::Txg;
    let good = Uberblock {
        magic: UBERBLOCK_MAGIC,
        version: UBERBLOCK_VERSION,
        txg: Txg::from_raw(7),
        timestamp: 0,
        objects: crate::object::TreeRoot {
            ptr: None,
            levels: 0,
        },
        degraded: Vec::new(),
        next_object: 10,
        datasets: FileId::from_raw(2),
    };
    assert!(good.validate(Txg::from_raw(7)).is_ok());
    // Written in another TXG's label than it says.
    assert!(good.validate(Txg::from_raw(8)).is_err());
    let bad = [
        Uberblock {
            magic: 0,
            ..good.clone()
        },
        Uberblock {
            version: 0,
            ..good.clone()
        },
        Uberblock {
            datasets: FileId::from_raw(0),
            ..good.clone()
        },
        Uberblock {
            next_object: 2,
            ..good.clone()
        },
    ];
    for b in bad {
        assert!(b.validate(Txg::from_raw(7)).is_err(), "{:?}", b);
    }
}

/// A label's TXG is covered by its checksum: a flipped bit there is
/// caught, not taken for an older or newer copy.
#[test]
fn label_checksum_covers_the_txg() {
    use crate::types::Txg;
    let payload = b"space map".to_vec();
    let header = BlockHeader {
        magic: LABEL_MAGIC,
        txg: Txg::from_raw(12),
        checksum: label_checksum(Txg::from_raw(12), &payload),
        payload_len: payload.len() as u32,
    };
    let mut buf = vec![0u8; BLOCK_SIZE];
    let hb = bincode::serialize(&header).unwrap();
    buf[..hb.len()].copy_from_slice(&hb);
    buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + payload.len()].copy_from_slice(&payload);
    assert_eq!(
        parse_label(&buf).unwrap(),
        (Txg::from_raw(12), payload.len())
    );
    let flipped = BlockHeader {
        txg: Txg::from_raw(12 ^ 1 << 3),
        ..header
    };
    let hb = bincode::serialize(&flipped).unwrap();
    buf[..hb.len()].copy_from_slice(&hb);
    assert!(matches!(
        parse_label(&buf),
        Err(FileSystemError::ChecksumMismatch)
    ));
}

/// No pool value exists without a superblock and an uberblock (I-8): a
/// blank pool, and one whose uberblock ring is gone (a format that never
/// wrote one), fail to load, read-only too, rather than coming back
/// half-loaded.
#[tokio::test]
async fn a_pool_without_an_uberblock_does_not_load() {
    use crate::test_util::TestPool;
    let pool = TestPool::new().await;
    assert!(matches!(
        FileSystem::load(pool.dev().await).await,
        Err(FileSystemError::NotFormatted)
    ));

    let header = pool.format().await.header;
    let dev = pool.dev().await;
    let zero = [0u8; BLOCK_SIZE];
    for i in 0..header.uberblock_blocks() {
        dev.write_block(header.uberblock_start + i, &zero)
            .await
            .unwrap();
    }
    dev.flush().await.unwrap();
    assert!(read_superblock_of(&dev).await.is_some(), "superblock kept");
    assert!(matches!(
        FileSystem::load(dev).await,
        Err(FileSystemError::NotFormatted)
    ));
    assert!(matches!(
        FileSystem::load_readonly(pool.dev().await).await,
        Err(FileSystemError::NotFormatted)
    ));
}
