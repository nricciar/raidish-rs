use super::*;
use crate::inode::{ObjType, SetAttrs};
use crate::test_util::TestPool;
use crate::vdev::Pool;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 5) as u8).wrapping_mul(37).wrapping_add(seed))
        .collect()
}

/// Commit enough TXGs that every deferred free is released.
async fn settle(fs: &FileSystem<Pool>) {
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
}

async fn clean(fs: &FileSystem<Pool>) {
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    settle(fs).await;
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

/// Writes at any offset (in the middle, past the end, sparse), read back
/// before and after the sync and after a reload.
#[tokio::test]
async fn writes_at_any_offset() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let base = pattern(400_000, 1);
    fs.write_file(Path::new("/f"), &base).await.unwrap();
    let bs = fs.file_blksz() as u64;

    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    let mut expect = base.clone();
    // Inside one record, across a record boundary, and past the end.
    for (at, len, seed) in [
        (10u64, 100usize, 2u8),
        (bs - 50, 300, 3),
        (390_000, 20_000, 4),
    ] {
        let data = pattern(len, seed);
        fs.file_write(&f, at, &data).await.unwrap();
        let end = at as usize + len;
        if expect.len() < end {
            expect.resize(end, 0);
        }
        expect[at as usize..end].copy_from_slice(&data);
    }
    // Sparse: far past the end; the gap reads as zeros.
    let far = 5 * bs + 7;
    fs.file_write(&f, far, b"tail").await.unwrap();
    expect.resize(far as usize, 0);
    expect.extend_from_slice(b"tail");
    assert_eq!(f.size(), expect.len() as u64);

    let mut buf = vec![0u8; expect.len()];
    assert_eq!(fs.file_read(&f, 0, &mut buf).await.unwrap(), expect.len());
    assert_eq!(buf, expect, "before the sync");
    // Not visible to path reads until synced.
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), base);

    fs.close_file(&f).await.unwrap();
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), expect);
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), expect);
    clean(&fs).await;
}

/// A small write to a large file rewrites only the record it touches.
#[tokio::test]
async fn a_small_write_rewrites_one_record() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let bs = fs.file_blksz() as usize;
    fs.write_file(Path::new("/f"), &pattern(10 * bs, 5))
        .await
        .unwrap();
    let (_, before) = fs.stat_file(Path::new("/f")).await.unwrap();
    let before = fs.tree_leaves(&before.tree).await.unwrap();

    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    fs.file_write(&f, 3 * bs as u64 + 10, b"x").await.unwrap();
    fs.close_file(&f).await.unwrap();

    let (_, after) = fs.stat_file(Path::new("/f")).await.unwrap();
    let after = fs.tree_leaves(&after.tree).await.unwrap();
    let changed: Vec<u64> = before
        .iter()
        .zip(&after)
        .filter(|(a, b)| a.1 != b.1)
        .map(|(a, _)| a.0)
        .collect();
    assert_eq!(changed, vec![3]);
    clean(&fs).await;
}

#[tokio::test]
async fn truncate_cuts_and_extends() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let data = pattern(300_000, 6);
    fs.write_file(Path::new("/f"), &data).await.unwrap();

    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    fs.file_truncate(&f, 1000).await.unwrap();
    fs.file_sync(&f).await.unwrap();
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), data[..1000]);

    // Growing alone (no record to write) still reaches the dnode.
    fs.file_truncate(&f, 5000).await.unwrap();
    fs.close_file(&f).await.unwrap();
    let mut expect = data[..1000].to_vec();
    expect.resize(5000, 0);
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), expect);
    clean(&fs).await;
}

/// Writes made on a file that was replaced or deleted meanwhile are
/// dropped, never written over the new contents (or bringing it back).
#[tokio::test]
async fn writes_to_a_replaced_or_deleted_file_are_dropped() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/f"), &pattern(50_000, 7))
        .await
        .unwrap();
    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    fs.file_write(&f, 0, b"stale").await.unwrap();
    fs.write_file(Path::new("/f"), b"replaced").await.unwrap();
    // The commit carrying the stale writes reports the conflict.
    assert!(matches!(
        fs.file_sync(&f).await,
        Err(FileSystemError::Conflict(_))
    ));
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), b"replaced");

    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    fs.file_write(&f, 0, b"gone").await.unwrap();
    fs.delete(Path::new("/f")).await.unwrap();
    fs.sync().await.unwrap();
    assert!(fs.stat(Path::new("/f")).await.is_err());
    clean(&fs).await;
}

#[tokio::test]
async fn rename_moves_and_replaces() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/a")).await.unwrap();
    fs.mkdir(Path::new("/b")).await.unwrap();
    let id = fs.write_file(Path::new("/a/f"), b"one").await.unwrap();
    fs.write_file(Path::new("/b/g"), &pattern(200_000, 8))
        .await
        .unwrap();

    // Across directories, then over an existing file (which is freed).
    assert_eq!(
        fs.rename(Path::new("/a/f"), Path::new("/b/f"))
            .await
            .unwrap(),
        id
    );
    assert!(fs.stat(Path::new("/a/f")).await.is_err());
    fs.rename(Path::new("/b/f"), Path::new("/b/g"))
        .await
        .unwrap();
    assert_eq!(fs.read_file(Path::new("/b/g")).await.unwrap(), b"one");
    assert_eq!(fs.file_id(Path::new("/b/g")).await.unwrap(), id);
    // Within one directory.
    fs.rename(Path::new("/b/g"), Path::new("/b/h"))
        .await
        .unwrap();
    assert_eq!(
        fs.ls(Path::new("/b"))
            .await
            .unwrap()
            .files
            .keys()
            .collect::<Vec<_>>(),
        ["h"]
    );
    // A directory, with its contents.
    fs.rename(Path::new("/b"), Path::new("/a/b")).await.unwrap();
    assert_eq!(fs.read_file(Path::new("/a/b/h")).await.unwrap(), b"one");
    clean(&fs).await;
}

#[tokio::test]
async fn rename_refuses_what_it_must() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/d")).await.unwrap();
    fs.mkdir(Path::new("/d/sub")).await.unwrap();
    fs.mkdir(Path::new("/full")).await.unwrap();
    fs.write_file(Path::new("/full/x"), b"x").await.unwrap();
    fs.write_file(Path::new("/f"), b"f").await.unwrap();
    fs.create_block("vol", 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    let refused = |r: Result<FileId, FileSystemError>| assert!(r.is_err(), "{:?}", r);

    refused(fs.rename(Path::new("/d"), Path::new("/d/sub/d")).await);
    refused(fs.rename(Path::new("/d"), Path::new("/full")).await);
    refused(fs.rename(Path::new("/f"), Path::new("/d")).await);
    refused(fs.rename(Path::new("/d"), Path::new("/f")).await);
    refused(fs.rename(Path::new("/vol"), Path::new("/vol2")).await);
    refused(fs.rename(Path::new("/f"), Path::new("/vol")).await);
    refused(fs.rename(Path::new("/nope"), Path::new("/x")).await);
    // A directory over an empty one is fine.
    fs.mkdir(Path::new("/empty")).await.unwrap();
    fs.rename(Path::new("/d"), Path::new("/empty"))
        .await
        .unwrap();
    assert_eq!(
        fs.stat(Path::new("/empty/sub")).await.unwrap().1.kind,
        ObjType::Directory
    );
    clean(&fs).await;
}

#[tokio::test]
async fn attributes_persist() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/f"), b"x").await.unwrap();
    let (_, before) = fs.stat(Path::new("/f")).await.unwrap();
    assert_eq!(before.attrs.mode, 0o644);
    let set = SetAttrs {
        mode: Some(0o600),
        uid: Some(1000),
        gid: Some(100),
        mtime_ns: Some(42),
    };
    fs.set_attrs(Path::new("/f"), &set).await.unwrap();
    drop(fs);
    let fs = pool.load().await;
    let (_, d) = fs.stat(Path::new("/f")).await.unwrap();
    assert_eq!(
        (d.attrs.mode, d.attrs.uid, d.attrs.gid, d.attrs.mtime_ns),
        (0o600, 1000, 100, 42)
    );
    assert!(d.attrs.ctime_ns >= before.attrs.ctime_ns);
    // Writing bumps the modification time.
    let f = fs.open_file(Path::new("/f"), None).await.unwrap();
    fs.file_write(&f, 1, b"y").await.unwrap();
    fs.close_file(&f).await.unwrap();
    assert!(fs.stat(Path::new("/f")).await.unwrap().1.attrs.mtime_ns > 42);
}
