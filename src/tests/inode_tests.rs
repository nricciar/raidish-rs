use super::*;
use crate::test_util::TestPool;
use std::path::Path;

fn content(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(29).wrapping_add(seed))
        .collect()
}

/// Enough objects to need several table records (and a table indirect
/// level), all readable after a reload, and fsck clean.
#[tokio::test]
async fn object_table_grows_past_one_record() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    assert_eq!(fs.objects.levels(), 0);
    fs.mkdir(Path::new("/d")).await.unwrap();
    let n = 3 * DNODES_PER_LEAF as usize;
    for i in 0..n {
        let p = format!("/d/f{i}");
        fs.write_file(Path::new(&p), &content(i as u8, 100 + i))
            .await
            .unwrap();
    }
    assert!(fs.objects.levels() >= 1);
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(fs.ls(Path::new("/d")).await.unwrap().files.len(), n);
    for i in [0, n / 2, n - 1] {
        let p = format!("/d/f{i}");
        assert_eq!(
            fs.read_file(Path::new(&p)).await.unwrap(),
            content(i as u8, 100 + i)
        );
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn replacing_a_file_keeps_its_object_number() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let id = fs.write_file(Path::new("/f"), b"one").await.unwrap();
    let again = fs
        .write_file(Path::new("/f"), &content(1, 500_000))
        .await
        .unwrap();
    assert_eq!(id, again);
    let (_, d) = fs.stat_file(Path::new("/f")).await.unwrap();
    assert_eq!(d.size, 500_000);
    assert_eq!(
        fs.read_file(Path::new("/f")).await.unwrap(),
        content(1, 500_000)
    );
    // Replacing a directory's contents never touches the directory.
    let root_before = fs.read_dnode(ROOT_OBJECT).await.unwrap();
    fs.write_file(Path::new("/f"), b"three").await.unwrap();
    assert_eq!(fs.read_dnode(ROOT_OBJECT).await.unwrap(), root_before);
}

/// One record needs no indirect block: the root points at it.
#[tokio::test]
async fn small_files_have_no_indirect_block() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/small"), b"vm config")
        .await
        .unwrap();
    let (_, d) = fs.stat_file(Path::new("/small")).await.unwrap();
    assert_eq!(d.tree.levels, 0);
    assert_eq!(fs.tree_extents(&d.tree).await.unwrap().len(), 1);
    let big = content(2, 3 * fs.file_blksz() as usize + 5);
    fs.write_file(Path::new("/big"), &big).await.unwrap();
    let (_, d) = fs.stat_file(Path::new("/big")).await.unwrap();
    assert_eq!(d.tree.levels, 1);
    assert_eq!(fs.read_file(Path::new("/big")).await.unwrap(), big);
    // An empty file takes nothing.
    fs.write_file(Path::new("/empty"), b"").await.unwrap();
    let (_, d) = fs.stat_file(Path::new("/empty")).await.unwrap();
    assert!(d.tree.ptr.is_none());
    assert!(fs.read_file(Path::new("/empty")).await.unwrap().is_empty());
}

#[tokio::test]
async fn deleting_a_directory_needs_it_empty() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/d")).await.unwrap();
    fs.write_file(Path::new("/d/f"), b"x").await.unwrap();
    assert!(fs.delete(Path::new("/d")).await.is_err());
    fs.delete(Path::new("/d/f")).await.unwrap();
    fs.delete(Path::new("/d")).await.unwrap();
    assert!(fs.ls(Path::new("")).await.unwrap().files.is_empty());
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

/// fsck reports an object nothing links to, and an entry for an object
/// that doesn't exist.
#[tokio::test]
async fn fsck_reports_namespace_damage() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/f"), b"x").await.unwrap();
    {
        let _tx = fs.begin_tx().await;
        let stray = fs.alloc_object();
        fs.write_dnode(stray, Some(Dnode::file(fs.file_blksz())))
            .await
            .unwrap();
        fs.commit_locked().await.unwrap();
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(!report.is_clean());
    assert!(
        report.errors.iter().any(|e| e.contains("isn't linked")),
        "{}",
        report
    );

    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/f"), b"x").await.unwrap();
    {
        let _tx = fs.begin_tx().await;
        let (id, _) = fs.stat(Path::new("/f")).await.unwrap();
        fs.write_dnode(id, None).await.unwrap();
        fs.commit_locked().await.unwrap();
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(
        report.errors.iter().any(|e| e.contains("doesn't exist")),
        "{}",
        report
    );
}
