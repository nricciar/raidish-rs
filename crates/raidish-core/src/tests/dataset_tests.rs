use super::*;
use crate::test_util::TestPool;
use crate::test_util::ds;
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
    let ub = fs.uberblock().await;
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
    let report = fs.fsck().await.unwrap();
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
        let mut tx = fs.begin_tx().await;
        fs.write_datasets(&mut tx, &DatasetIndex::default())
            .await
            .unwrap();
        tx.applied();
    }
    let report = fs.fsck().await.unwrap();
    assert!(!report.is_clean());
    let text = report.to_string();
    assert!(text.contains("no 'fs' dataset"), "{}", text);
}

#[tokio::test]
async fn a_volume_is_a_block_dataset_not_an_entry_in_root() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let id = fs
        .create_block(&ds("vol"), 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let e = fs.dataset_entry(&ds("vol")).await.unwrap();
    assert_eq!((e.root, e.format), (id, DatasetFormat::Block));
    let d = fs.read_dnode(id).await.unwrap();
    assert_eq!(d.kind(), ObjType::Volume);
    assert_eq!(d.parent, Some(fs.datasets_object()));
    // Not in `/`; a file of the same name is something else.
    assert!(fs.ls(Path::new("")).await.unwrap().files.is_empty());
    fs.write_file(Path::new("/vol"), b"a file").await.unwrap();
    fs.block_write(&ds("vol"), 0, b"a block").await.unwrap();
    let mut buf = [0u8; 7];
    fs.block_read(&ds("vol"), 0, &mut buf).await.unwrap();
    assert_eq!(&buf, b"a block");
    assert_eq!(fs.read_file(Path::new("/vol")).await.unwrap(), b"a file");
    // A posix dataset isn't a volume.
    assert!(fs.block_read(&ds(DEFAULT_FS), 0, &mut buf).await.is_err());
    assert!(fs.delete_volume(&ds(DEFAULT_FS)).await.is_err());
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn dataset_names_are_checked() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let vbs = fs.default_volblocksize();
    // Bad names don't get this far: they don't parse.
    for bad in ["", "a@b", "a:b", "a/b", ".", ".."] {
        assert!(
            crate::types::DatasetName::try_from(bad).is_err(),
            "{:?}",
            bad
        );
    }
    assert!(matches!(
        fs.create_block(&ds(DEFAULT_FS), 1 << 20, vbs).await,
        Err(FileSystemError::AlreadyExists)
    ));
    fs.create_block(&ds("v"), 1 << 20, vbs).await.unwrap();
    assert!(matches!(
        fs.create_block(&ds("v"), 1 << 20, vbs).await,
        Err(FileSystemError::AlreadyExists)
    ));
}

#[tokio::test]
async fn snapshots_and_clones_are_datasets_too() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block(&ds("vol"), 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    fs.block_write(&ds("vol"), 0, b"before").await.unwrap();
    fs.sync().await.unwrap();
    fs.create_snapshot(&ds("vol"), "s1").await.unwrap();
    fs.clone_snapshot(&ds("vol"), "s1", &ds("copy"))
        .await
        .unwrap();
    let e = fs.dataset_entry(&ds("copy")).await.unwrap();
    assert_eq!(e.format, DatasetFormat::Block);
    let mut buf = [0u8; 6];
    fs.block_read(&ds("copy"), 0, &mut buf).await.unwrap();
    assert_eq!(&buf, b"before");
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);

    // Survives a reload; deleting the clone takes its entry away.
    drop(fs);
    let fs = pool.load().await;
    fs.delete_volume(&ds("copy")).await.unwrap();
    assert!(fs.dataset_entry(&ds("copy")).await.is_err());
    assert_eq!(fs.list_volumes().await.unwrap().len(), 1);
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[test]
fn the_lease_is_per_dataset() {
    assert_eq!(
        crate::types::LeaseResource::dataset("vol").to_string(),
        "ds/vol"
    );
}

/// S4b: a sync that doesn't fold the intent log (the holes-only retry
/// after `NoSpace`) leaves the dnode's log as it is, even while `vol.log`
/// says none (a replay clears it before its commit): the log still holds
/// writes that sync didn't commit.
#[tokio::test]
async fn a_sync_without_folding_keeps_the_dnodes_log() {
    use crate::inode::{DatasetLog, LogRegion};
    use crate::types::{Holding, LeaseRef, ep, res};
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block(&ds("v"), 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let vol = fs.open_dataset(&ds("v")).await.unwrap();
    let log = DatasetLog {
        regions: vec![LogRegion {
            start: 1 << 30,
            len: 8,
            guid: 1,
        }],
        from_seq: 0,
        writer: LeaseRef::new(res("writer/1"), ep(1)),
        ds: Holding::new("o", ep(1)),
    };
    *vol.log.lock().unwrap() = Some(log.clone());
    let mut tx = fs.begin_tx().await;
    fs.update_dataset_dnode(&mut tx, &vol, vol.obj.root_ptr())
        .await
        .unwrap();

    // As a replay leaves it: the log cleared in memory, records dirty.
    *vol.log.lock().unwrap() = None;
    let holes: std::collections::BTreeMap<u64, crate::object::Dirty> =
        [(0, crate::object::Dirty::Hole)].into_iter().collect();
    fs.sync_dataset(&mut tx, &vol, &holes, false).await.unwrap();
    let head_log = |d: crate::inode::Dnode| d.head().and_then(|h| h.log.clone());
    assert_eq!(head_log(fs.read_dnode(vol.id).await.unwrap()), Some(log));
    // Folding (everything synced): the log goes.
    fs.sync_dataset(&mut tx, &vol, &holes, true).await.unwrap();
    tx.applied();
    assert_eq!(head_log(fs.read_dnode(vol.id).await.unwrap()), None);
}
