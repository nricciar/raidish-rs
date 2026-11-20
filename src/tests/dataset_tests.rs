use super::*;
use crate::test_util::TestPool;
use std::path::Path;

#[tokio::test]
async fn format_makes_the_default_posix_dataset() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let index = fs.datasets().await.unwrap();
    assert_eq!(index.datasets.len(), 1);
    let e = &index.datasets[DEFAULT_FS];
    assert_eq!(e.format, DatasetFormat::Posix);
    assert_eq!(e.root, fs.root_dir());
    assert_ne!(fs.root_dir(), fs.datasets_object());
    let ub = fs.uberblock().await.unwrap();
    assert_eq!(ub.datasets, fs.datasets_object());
    assert_eq!(ub.version, crate::fs::UBERBLOCK_VERSION);
    // The root directory is its own parent.
    assert_eq!(
        fs.read_dnode(fs.root_dir()).await.unwrap().parent,
        Some(fs.root_dir())
    );
}

#[tokio::test]
async fn reload_finds_the_same_root() {
    let pool = TestPool::new().await;
    let (datasets, root) = {
        let fs = pool.format().await;
        fs.write_file(Path::new("/hello"), b"hi").await.unwrap();
        fs.sync().await.unwrap();
        (fs.datasets_object(), fs.root_dir())
    };
    let fs = pool.load().await;
    assert_eq!(fs.datasets_object(), datasets);
    assert_eq!(fs.root_dir(), root);
    assert_eq!(fs.read_file(Path::new("/hello")).await.unwrap(), b"hi");
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn the_dataset_directory_is_not_in_the_namespace() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let ds = fs.read_dnode(fs.datasets_object()).await.unwrap();
    assert_eq!(ds.kind(), ObjType::Datasets);
    // Not a directory: path lookups can't walk into it.
    assert!(fs.read_dir(&ds).await.is_err());
    let root = fs.read_dnode(fs.root_dir()).await.unwrap();
    let index = fs.read_dir(&root).await.unwrap();
    assert!(index.files.values().all(|e| e.id != fs.datasets_object()));
}

#[tokio::test]
async fn fsck_reports_a_missing_default_dataset() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    {
        let _tx = fs.begin_tx().await;
        fs.write_datasets(&DatasetIndex::default()).await.unwrap();
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(!report.is_clean());
    let text = report.to_string();
    assert!(text.contains("no 'fs' dataset"), "{}", text);
}

#[tokio::test]
async fn a_volume_is_a_block_dataset_not_an_entry_in_root() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let id = fs
        .create_block("vol", 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let e = fs.dataset_entry("vol").await.unwrap();
    assert_eq!((e.root, e.format), (id, DatasetFormat::Block));
    let d = fs.read_dnode(id).await.unwrap();
    assert_eq!(d.kind(), ObjType::Volume);
    assert_eq!(d.parent, Some(fs.datasets_object()));
    // Not in `/`; a file of the same name is something else.
    assert!(fs.ls(Path::new("")).await.unwrap().files.is_empty());
    fs.write_file(Path::new("/vol"), b"a file").await.unwrap();
    fs.block_write("vol", 0, b"a block").await.unwrap();
    let mut buf = [0u8; 7];
    fs.block_read("vol", 0, &mut buf).await.unwrap();
    assert_eq!(&buf, b"a block");
    assert_eq!(fs.read_file(Path::new("/vol")).await.unwrap(), b"a file");
    // A posix dataset isn't a volume.
    assert!(fs.block_read(DEFAULT_FS, 0, &mut buf).await.is_err());
    assert!(fs.delete_volume(DEFAULT_FS).await.is_err());
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn dataset_names_are_checked() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let vbs = fs.default_volblocksize();
    for bad in ["", "a@b", "a:b", "a/b", ".", ".."] {
        assert!(
            fs.create_block(bad, 1 << 20, vbs).await.is_err(),
            "{:?}",
            bad
        );
    }
    assert!(matches!(
        fs.create_block(DEFAULT_FS, 1 << 20, vbs).await,
        Err(FileSystemError::AlreadyExists)
    ));
    fs.create_block("v", 1 << 20, vbs).await.unwrap();
    assert!(matches!(
        fs.create_block("v", 1 << 20, vbs).await,
        Err(FileSystemError::AlreadyExists)
    ));
}

#[tokio::test]
async fn snapshots_and_clones_are_datasets_too() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    fs.block_write("vol", 0, b"before").await.unwrap();
    fs.sync().await.unwrap();
    fs.create_snapshot("vol", "s1").await.unwrap();
    fs.clone_snapshot("vol", "s1", "copy").await.unwrap();
    let e = fs.dataset_entry("copy").await.unwrap();
    assert_eq!(e.format, DatasetFormat::Block);
    let mut buf = [0u8; 6];
    fs.block_read("copy", 0, &mut buf).await.unwrap();
    assert_eq!(&buf, b"before");
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);

    // Survives a reload; deleting the clone takes its entry away.
    drop(fs);
    let fs = pool.load().await;
    fs.delete_volume("copy").await.unwrap();
    assert!(fs.dataset_entry("copy").await.is_err());
    assert_eq!(fs.list_volumes().await.unwrap().len(), 1);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[test]
fn the_lease_is_per_dataset() {
    assert_eq!(
        crate::committer::dataset_resource("vol").to_string(),
        "ds/vol"
    );
}
