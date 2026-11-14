use super::*;
use crate::test_util::{TestPool, VOLBLOCK_ALIGN};
use crate::vdev::Pool;
use std::path::Path;

const MB: u64 = 1024 * 1024;
// Small records: an 8 MiB volume has two levels of indirects.
const VOL: u64 = 8 * MB;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 3) as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

async fn allocated(fs: &FileSystem<Pool>) -> u64 {
    let mut used = 0;
    for ms in fs.metaslabs.iter() {
        let ms = ms.read().await;
        used += ms.block_count - ms.free_blocks() - ms.pending_free_blocks();
    }
    used
}

/// Commit enough TXGs that every deferred free is released.
async fn settle(fs: &FileSystem<Pool>) {
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
}

async fn read(fs: &FileSystem<Pool>, name: &str, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = match name.split_once('@') {
        Some((v, s)) => fs.snapshot_read(v, s, 0, &mut buf).await.unwrap(),
        None => fs.block_read(name, 0, &mut buf).await.unwrap(),
    };
    buf.truncate(n);
    buf
}

async fn clean(fs: &FileSystem<Pool>) {
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Write `data` at `offset` of `want` too (what the volume should hold).
async fn write(fs: &FileSystem<Pool>, want: &mut [u8], offset: usize, data: &[u8]) {
    fs.block_write("vol", offset as u64, data).await.unwrap();
    want[offset..offset + data.len()].copy_from_slice(data);
}

async fn volume(fs: &FileSystem<Pool>) -> Vec<u8> {
    fs.create_block("vol", VOL, VOLBLOCK_ALIGN).await.unwrap();
    let want = pattern(VOL as usize, 1);
    fs.block_write("vol", 0, &want).await.unwrap();
    fs.sync().await.unwrap();
    want
}

/// A snapshot keeps the volume as it was; the volume moves on.
#[tokio::test]
async fn a_snapshot_keeps_the_contents() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let mut want = volume(&fs).await;
    let at_snap = want.clone();
    fs.create_snapshot("vol", "s1").await.unwrap();
    write(&fs, &mut want, 100_000, &pattern(2 * MB as usize, 2)).await;
    fs.block_zero("vol", 5 * MB, MB as usize).await.unwrap();
    want[5 * MB as usize..6 * MB as usize].fill(0);
    fs.sync().await.unwrap();

    assert_eq!(read(&fs, "vol@s1", VOL as usize).await, at_snap);
    assert_eq!(read(&fs, "vol", VOL as usize).await, want);
    assert!(matches!(
        fs.create_snapshot("vol", "s1").await,
        Err(FileSystemError::AlreadyExists)
    ));
    assert!(fs.create_snapshot("vol", "no/slash").await.is_err());
    clean(&fs).await;

    // And after reopening the pool.
    drop(fs);
    let fs = pool.load().await;
    assert_eq!(read(&fs, "vol@s1", VOL as usize).await, at_snap);
    assert_eq!(read(&fs, "vol", VOL as usize).await, want);
    let list = fs.list_snapshots("vol").await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "vol@s1");
    // What the overwrites and the zeroing replaced is the snapshot's alone.
    assert!(
        list[0].unique_bytes >= 3 * MB && list[0].unique_bytes < 4 * MB,
        "{}",
        list[0].unique_bytes
    );
    clean(&fs).await;
}

/// Destroying snapshots, in any order, frees exactly what only they had:
/// fsck finds no leak and nothing referenced-but-free, and once they and
/// the volume are gone, the space is back.
#[tokio::test]
async fn destroying_snapshots_frees_exactly_their_blocks() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let baseline = allocated(&fs).await;
    let mut want = volume(&fs).await;
    let mut snaps = Vec::new();
    for (i, off) in [0usize, 1_500_000, 3_000_000, 600_000]
        .into_iter()
        .enumerate()
    {
        let name = format!("s{}", i);
        fs.create_snapshot("vol", &name).await.unwrap();
        snaps.push((name, want.clone()));
        write(&fs, &mut want, off, &pattern(MB as usize, 10 + i as u8)).await;
        fs.sync().await.unwrap();
    }
    // The middle ones first, then the oldest, then the newest.
    for victim in ["s2", "s1", "s0"] {
        fs.destroy_snapshot("vol", victim).await.unwrap();
        snaps.retain(|(n, _)| n != victim);
        settle(&fs).await;
        clean(&fs).await;
        for (n, data) in &snaps {
            assert_eq!(&read(&fs, &format!("vol@{}", n), VOL as usize).await, data);
        }
        assert_eq!(read(&fs, "vol", VOL as usize).await, want);
    }
    assert!(
        fs.delete(Path::new("vol")).await.is_err(),
        "it has a snapshot"
    );
    fs.destroy_snapshot("vol", "s3").await.unwrap();
    assert!(matches!(
        fs.destroy_snapshot("vol", "s3").await,
        Err(FileSystemError::FileNotFound)
    ));
    fs.delete(Path::new("vol")).await.unwrap();
    settle(&fs).await;
    clean(&fs).await;
    assert_eq!(allocated(&fs).await, baseline);
}

/// A clone starts as its snapshot and goes its own way; the snapshot
/// can't go while the clone exists; deleting the clone frees only what it
/// wrote.
#[tokio::test]
async fn clones_share_until_written() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let baseline = allocated(&fs).await;
    let mut want = volume(&fs).await;
    let at_snap = want.clone();
    fs.create_snapshot("vol", "s").await.unwrap();
    let before_clone = allocated(&fs).await;
    fs.clone_snapshot("vol", "s", "copy").await.unwrap();
    assert!(
        allocated(&fs).await - before_clone < 100,
        "a clone copies nothing"
    );
    assert_eq!(read(&fs, "copy", VOL as usize).await, at_snap);

    let mut theirs = at_snap.clone();
    let data = pattern(MB as usize, 7);
    fs.block_write("copy", 2 * MB, &data).await.unwrap();
    theirs[2 * MB as usize..3 * MB as usize].copy_from_slice(&data);
    write(&fs, &mut want, 4 * MB as usize, &pattern(MB as usize, 8)).await;
    fs.sync().await.unwrap();
    assert_eq!(read(&fs, "copy", VOL as usize).await, theirs);
    assert_eq!(read(&fs, "vol", VOL as usize).await, want);
    assert_eq!(read(&fs, "vol@s", VOL as usize).await, at_snap);
    clean(&fs).await;

    // Snapshots of a clone work the same.
    fs.create_snapshot("copy", "c1").await.unwrap();
    fs.block_write("copy", 0, &pattern(MB as usize, 9))
        .await
        .unwrap();
    fs.sync().await.unwrap();
    assert_eq!(read(&fs, "copy@c1", VOL as usize).await, theirs);
    let list = fs.list_snapshots("vol").await.unwrap();
    assert_eq!(list[0].clones, vec!["copy".to_string()]);

    assert!(matches!(
        fs.destroy_snapshot("vol", "s").await,
        Err(FileSystemError::InvalidArgument(_))
    ));
    fs.destroy_snapshot("copy", "c1").await.unwrap();
    fs.delete(Path::new("copy")).await.unwrap();
    settle(&fs).await;
    clean(&fs).await;
    assert_eq!(read(&fs, "vol@s", VOL as usize).await, at_snap);
    fs.destroy_snapshot("vol", "s").await.unwrap();
    fs.delete(Path::new("vol")).await.unwrap();
    settle(&fs).await;
    clean(&fs).await;
    assert_eq!(allocated(&fs).await, baseline);
}

/// Rolling back discards every write since the newest snapshot, unsynced
/// ones too, and frees what they took.
#[tokio::test]
async fn rollback_returns_to_the_newest_snapshot() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let mut want = volume(&fs).await;
    assert!(fs.rollback_volume("vol").await.is_err(), "no snapshots");
    fs.create_snapshot("vol", "s").await.unwrap();
    let at_snap = want.clone();
    settle(&fs).await;
    let held = allocated(&fs).await;
    write(&fs, &mut want, 0, &pattern(3 * MB as usize, 4)).await;
    fs.sync().await.unwrap();
    write(&fs, &mut want, 5 * MB as usize, &pattern(MB as usize, 5)).await; // unsynced
    fs.rollback_volume("vol").await.unwrap();
    assert_eq!(read(&fs, "vol", VOL as usize).await, at_snap);
    settle(&fs).await;
    assert_eq!(allocated(&fs).await, held);
    clean(&fs).await;
    // And it goes on from there.
    fs.block_write("vol", 0, &pattern(4096, 6)).await.unwrap();
    fs.sync().await.unwrap();
    assert_eq!(read(&fs, "vol@s", VOL as usize).await, at_snap);
    clean(&fs).await;
}

/// Scrub reads shared records once and finds nothing wrong.
#[tokio::test]
async fn scrub_walks_snapshots() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let mut want = volume(&fs).await;
    fs.create_snapshot("vol", "s").await.unwrap();
    write(&fs, &mut want, 0, &pattern(MB as usize, 3)).await;
    fs.sync().await.unwrap();
    let report = fs.scrub(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    // Volume (8 MiB) plus the overwritten MiB, once each, plus metadata.
    let records = VOL / VOLBLOCK_ALIGN as u64;
    let extra = MB.div_ceil(VOLBLOCK_ALIGN as u64);
    assert!(
        report.records >= records + extra && report.records < records + extra + 40,
        "{} records",
        report.records
    );
}
