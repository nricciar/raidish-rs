//! `kv` datasets as datasets: durability, snapshots and clones, send /
//! receive, fsck, crashes.

use super::*;
use crate::disk::BlockDevice;
use crate::test_util::TestPool;
use std::ops::Bound;

async fn fill<D: BlockDevice>(fs: &FileSystem<D>, name: &str, n: u64, tag: &str) {
    for i in 0..n {
        let k = format!("k{:05}", i);
        let v = format!("{}-{}-{}", tag, i, "x".repeat((i % 50) as usize));
        fs.kv_put(name, k.as_bytes(), v.as_bytes()).await.unwrap();
    }
}

async fn all<D: BlockDevice>(fs: &FileSystem<D>, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    fs.kv_range(name, Bound::Unbounded, Bound::Unbounded, usize::MAX)
        .await
        .unwrap()
}

async fn clean<D: BlockDevice>(fs: &FileSystem<D>) {
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn survives_a_reload_and_is_a_tree_dataset() {
    let pool = TestPool::new().await;
    let before = {
        let fs = pool.format().await;
        fs.create_kv("kv").await.unwrap();
        let e = fs.dataset_entry("kv").await.unwrap();
        assert_eq!(e.format, DatasetFormat::Kv);
        assert!(e.format.is_tree());
        fill(&fs, "kv", 3000, "a").await;
        fs.sync().await.unwrap();
        all(&fs, "kv").await
    };
    let fs = pool.load().await;
    assert_eq!(all(&fs, "kv").await, before);
    assert_eq!(fs.kv_len("kv").await.unwrap(), 3000);
    clean(&fs).await;
    // Uncommitted changes don't survive a crash (no sync).
    fs.kv_put("kv", b"k00000", b"changed").await.unwrap();
    drop(fs);
    let fs = pool.load().await;
    assert_eq!(all(&fs, "kv").await, before);
}

/// Rewrite a committed tree (splits, merges, new and freed nodes), then
/// crash after `k` device writes of its commit.
/// Returns whether it crashed, the rewritten contents, and the writes the
/// commit made.
async fn crash_rewrite(pool: &TestPool, k: i64) -> (bool, Vec<(Vec<u8>, Vec<u8>)>, i64) {
    let (fs, faults) = pool.format_faulty().await;
    fs.create_kv_with("kv", fs.volblock_align(), 1 << 16)
        .await
        .unwrap();
    fill(&fs, "kv", 1500, "a").await;
    fs.sync().await.unwrap();
    fill(&fs, "kv", 1500, "b").await;
    for i in 0..400 {
        fs.kv_delete("kv", format!("k{:05}", i * 3).as_bytes())
            .await
            .unwrap();
    }
    let after = all(&fs, "kv").await;
    faults.fail_after(k);
    let failed = fs.sync().await.is_err();
    (failed && faults.tripped(), after, k - faults.remaining())
}

#[tokio::test]
async fn a_crash_mid_commit_keeps_a_whole_tree() {
    let before = {
        let pool = TestPool::new().await;
        let fs = pool.format().await;
        fs.create_kv_with("kv", fs.volblock_align(), 1 << 16)
            .await
            .unwrap();
        fill(&fs, "kv", 1500, "a").await;
        all(&fs, "kv").await
    };
    // How many writes the commit takes.
    let (_, _, total) = crash_rewrite(&TestPool::new().await, i64::MAX).await;
    assert!(total > 4, "commit took {} writes", total);
    let mut old = 0;
    for k in [0, 1, total / 4, total / 2, total - 1, total] {
        let pool = TestPool::new().await;
        let (crashed, after, _) = crash_rewrite(&pool, k).await;
        let fs = pool.load().await;
        let got = all(&fs, "kv").await;
        if crashed {
            assert!(
                got == before || got == after,
                "k={}/{}: torn tree",
                k,
                total
            );
            old += (got == before) as usize;
        } else {
            assert!(got == after, "k={}/{}", k, total);
        }
        clean(&fs).await;
    }
    assert!(old > 0, "no crash point kept the old tree");
}

#[tokio::test]
async fn snapshots_clones_and_rollback() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_kv("kv").await.unwrap();
    fill(&fs, "kv", 1500, "a").await;
    fs.sync().await.unwrap();
    let a = all(&fs, "kv").await;
    fs.create_snapshot("kv", "s1").await.unwrap();
    fill(&fs, "kv", 2500, "b").await;
    fs.kv_delete("kv", b"k00007").await.unwrap();
    fs.sync().await.unwrap();
    let b = all(&fs, "kv").await;
    assert_ne!(a, b);

    fs.clone_snapshot("kv", "s1", "copy").await.unwrap();
    assert_eq!(
        fs.dataset_entry("copy").await.unwrap().format,
        DatasetFormat::Kv
    );
    assert_eq!(all(&fs, "copy").await, a);
    // The clone and its origin diverge independently.
    fs.kv_put("copy", b"only-in-copy", b"1").await.unwrap();
    fs.sync().await.unwrap();
    assert_eq!(fs.kv_get("kv", b"only-in-copy").await.unwrap(), None);
    clean(&fs).await;

    fs.delete_dataset("copy").await.unwrap();
    fs.rollback_volume("kv").await.unwrap();
    assert_eq!(all(&fs, "kv").await, a);
    clean(&fs).await;
}

#[tokio::test]
async fn send_and_receive() {
    let src = TestPool::new().await;
    let fs = src.format().await;
    fs.create_kv("kv").await.unwrap();
    fill(&fs, "kv", 1200, "a").await;
    fs.sync().await.unwrap();
    fs.create_snapshot("kv", "s1").await.unwrap();
    let s1 = all(&fs, "kv").await;
    fill(&fs, "kv", 1800, "b").await;
    fs.kv_put("kv", b"big", &vec![7u8; 300_000]).await.unwrap();
    fs.sync().await.unwrap();
    fs.create_snapshot("kv", "s2").await.unwrap();
    let s2 = all(&fs, "kv").await;

    let mut full = Vec::new();
    fs.send_snapshot("kv", "s1", None, &mut full).await.unwrap();
    let mut incr = Vec::new();
    fs.send_snapshot("kv", "s2", Some("s1"), &mut incr)
        .await
        .unwrap();

    let dst = TestPool::new().await;
    let other = dst.format().await;
    other
        .recv_snapshot("kv", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert_eq!(
        other.dataset_entry("kv").await.unwrap().format,
        DatasetFormat::Kv
    );
    assert_eq!(all(&other, "kv").await, s1);
    other
        .recv_snapshot("kv", &mut incr.as_slice(), false)
        .await
        .unwrap();
    assert_eq!(all(&other, "kv").await, s2);
    clean(&other).await;
    // A kv stream doesn't land on a volume.
    other
        .create_block("vol", 1 << 20, other.default_volblocksize())
        .await
        .unwrap();
    other.create_snapshot("vol", "s1").await.unwrap();
    assert!(
        other
            .recv_snapshot("vol", &mut incr.as_slice(), true)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn fsck_finds_a_damaged_tree() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_kv_with("kv", fs.volblock_align(), 1 << 16)
        .await
        .unwrap();
    fill(&fs, "kv", 800, "a").await;
    fs.sync().await.unwrap();
    clean(&fs).await;
    // Overwrite a leaf with a well-formed node holding keys out of order:
    // its checksum is fine (a commit wrote it), its contents aren't.
    let ds = fs.open_kv("kv").await.unwrap();
    let leaf = {
        let t = crate::btree::Txn::begin(&fs, &ds.obj).await.unwrap();
        let mut id = t.header.root;
        loop {
            match t.node(id).await.unwrap() {
                crate::btree::Node::Branch { children, .. } => id = children[0],
                crate::btree::Node::Leaf(_) => break id,
            }
        }
    };
    let bad = crate::btree::Node::Leaf(vec![
        (b"zzz".to_vec(), crate::btree::Val::Inline(b"1".to_vec())),
        (b"aaa".to_vec(), crate::btree::Val::Inline(b"2".to_vec())),
    ]);
    let body = bincode::serialize(&bad).unwrap();
    let mut rec = (body.len() as u32).to_le_bytes().to_vec();
    rec.extend(body);
    ds.obj.put_record(leaf, crate::object::Dirty::Data(rec));
    // A leaked record too.
    ds.obj.put_record(
        70_000 % (1 << 16),
        crate::object::Dirty::Data(vec![1, 2, 3]),
    );
    fs.sync().await.unwrap();
    let report = fs.fsck(false).await.unwrap();
    let text = report.to_string();
    assert!(!report.is_clean());
    assert!(text.contains("out of order"), "{}", text);
    assert!(text.contains("leaked"), "{}", text);
}

#[tokio::test]
async fn names_and_shapes_are_checked() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    assert!(fs.create_kv("a@b").await.is_err());
    assert!(fs.create_kv(crate::dataset::DEFAULT_FS).await.is_err());
    assert!(fs.create_kv_with("x", 1000, 1 << 10).await.is_err());
    assert!(
        fs.create_kv_with("x", fs.volblock_align(), 1)
            .await
            .is_err()
    );
    fs.create_kv("x").await.unwrap();
    assert!(matches!(
        fs.create_kv("x").await,
        Err(FileSystemError::AlreadyExists)
    ));
    // Not a volume.
    assert!(fs.delete_volume("x").await.is_err());
    assert_eq!(fs.list_volumes().await.unwrap().len(), 0);
}
