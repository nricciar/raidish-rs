use super::*;
use crate::test_util::{TestPool, VOLBLOCK_ALIGN};
use crate::vdev::Pool;

const MB: u64 = 1024 * 1024;
// Small records: two levels of indirects, so whole subtrees can differ.
const VOL: u64 = 8 * MB;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 3) as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

async fn read(fs: &FileSystem<Pool>, name: &str) -> Vec<u8> {
    let mut buf = vec![0xEEu8; VOL as usize];
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

async fn allocated(fs: &FileSystem<Pool>) -> u64 {
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
    let mut used = 0;
    for ms in fs.metaslabs.iter() {
        let ms = ms.read().await;
        used += ms.block_count - ms.free_blocks() - ms.pending_free_blocks();
    }
    used
}

async fn send(fs: &FileSystem<Pool>, snap: &str, from: Option<&str>) -> Vec<u8> {
    let mut out = Vec::new();
    fs.send_snapshot("vol", snap, from, &mut out).await.unwrap();
    out
}

/// A source volume with snapshot s1 of a sparse first version, then
/// changes (overwrites, a new region, a zeroed range spanning whole
/// indirect subtrees) and snapshot s2.
async fn source() -> (TestPool, FileSystem<Pool>) {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", VOL, VOLBLOCK_ALIGN).await.unwrap();
    fs.block_write("vol", 0, &pattern(6 * MB as usize, 1))
        .await
        .unwrap();
    fs.create_snapshot("vol", "s1").await.unwrap();
    fs.block_write("vol", 500_000, &pattern(MB as usize, 2))
        .await
        .unwrap();
    fs.block_write("vol", 7 * MB, &pattern(MB as usize / 2, 3))
        .await
        .unwrap();
    // 512 records of 12 KiB = 6 MiB per level-1 subtree: this clears the
    // whole second one.
    fs.block_zero("vol", 2 * MB, 4 * MB as usize).await.unwrap();
    fs.create_snapshot("vol", "s2").await.unwrap();
    // And the head goes on (not part of any stream).
    fs.block_write("vol", 0, &pattern(4096, 4)).await.unwrap();
    fs.sync().await.unwrap();
    (pool, fs)
}

/// A full stream, then an incremental one, make the same snapshots (same
/// GUIDs) in another pool; the incremental carries only what changed.
#[tokio::test]
async fn full_then_incremental() {
    let (_sp, src) = source().await;
    let full = send(&src, "s1", None).await;
    let incr = send(&src, "s2", Some("s1")).await;
    assert!(
        incr.len() < full.len() / 2,
        "incremental {} vs full {}",
        incr.len(),
        full.len()
    );

    let dp = TestPool::new().await;
    let dst = dp.format().await;
    let r = dst
        .recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert!(!r.incremental);
    assert_eq!(read(&dst, "vol@s1").await, read(&src, "vol@s1").await);
    assert_eq!(read(&dst, "vol").await, read(&src, "vol@s1").await);
    let r = dst
        .recv_snapshot("vol", &mut incr.as_slice(), false)
        .await
        .unwrap();
    assert!(r.incremental && r.hole_records > 0, "{}", r);
    assert_eq!(read(&dst, "vol@s2").await, read(&src, "vol@s2").await);
    assert_eq!(read(&dst, "vol").await, read(&src, "vol@s2").await);
    let guids = |l: Vec<crate::snapshot::SnapshotInfo>| -> Vec<(String, u64)> {
        l.into_iter().map(|s| (s.name, s.guid)).collect()
    };
    assert_eq!(
        guids(dst.list_snapshots("vol").await.unwrap()),
        guids(src.list_snapshots("vol").await.unwrap())
    );
    clean(&dst).await;
    clean(&src).await;

    // Again: already there.
    assert!(matches!(
        dst.recv_snapshot("vol", &mut incr.as_slice(), false).await,
        Err(FileSystemError::AlreadyExists)
    ));
    // Into the same pool under another name: a copy.
    src.recv_snapshot("copy", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert_eq!(read(&src, "copy").await, read(&src, "vol@s1").await);
    clean(&src).await;
}

/// An incremental needs its base as the target's newest snapshot, and an
/// untouched target (or `force`, which rolls it back).
#[tokio::test]
async fn incremental_needs_its_base() {
    let (_sp, src) = source().await;
    let full = send(&src, "s1", None).await;
    let incr = send(&src, "s2", Some("s1")).await;
    let dp = TestPool::new().await;
    let dst = dp.format().await;
    assert!(
        dst.recv_snapshot("vol", &mut incr.as_slice(), false)
            .await
            .is_err(),
        "no such volume"
    );
    dst.recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert!(
        dst.recv_snapshot("vol", &mut full.as_slice(), false)
            .await
            .is_err(),
        "a full stream wants a new volume"
    );
    dst.block_write("vol", 0, &pattern(4096, 9)).await.unwrap();
    assert!(
        dst.recv_snapshot("vol", &mut incr.as_slice(), false)
            .await
            .is_err(),
        "written since s1"
    );
    dst.recv_snapshot("vol", &mut incr.as_slice(), true)
        .await
        .unwrap();
    assert_eq!(read(&dst, "vol").await, read(&src, "vol@s2").await);

    // A volume whose newest snapshot isn't the base.
    let dp2 = TestPool::new().await;
    let other = dp2.format().await;
    other
        .recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    other.create_snapshot("vol", "later").await.unwrap();
    let e = other
        .recv_snapshot("vol", &mut incr.as_slice(), false)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("newest"), "{}", e);
    clean(&dst).await;
    clean(&other).await;
}

/// A damaged or cut-short stream changes nothing: an incremental's target
/// is rolled back to its base, a full one's volume deleted.
#[tokio::test]
async fn bad_streams_leave_nothing_behind() {
    let (_sp, src) = source().await;
    let full = send(&src, "s1", None).await;
    let incr = send(&src, "s2", Some("s1")).await;
    let dp = TestPool::new().await;
    let dst = dp.format().await;
    let baseline = allocated(&dst).await;

    // Cut short, and corrupted in the data.
    let short = &full[..full.len() / 2];
    assert!(
        dst.recv_snapshot("vol", &mut &short[..], false)
            .await
            .is_err()
    );
    let mut flipped = full.clone();
    let at = flipped.len() / 2;
    flipped[at] ^= 0x40;
    assert!(
        dst.recv_snapshot("vol", &mut flipped.as_slice(), false)
            .await
            .is_err()
    );
    assert!(dst.list_volumes().await.unwrap().is_empty());
    assert_eq!(allocated(&dst).await, baseline);
    clean(&dst).await;

    dst.recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    let before = read(&dst, "vol").await;
    let mut flipped = incr.clone();
    let at = flipped.len() - 100;
    flipped[at] ^= 1;
    assert!(
        dst.recv_snapshot("vol", &mut flipped.as_slice(), false)
            .await
            .is_err()
    );
    assert_eq!(read(&dst, "vol").await, before);
    assert_eq!(dst.list_snapshots("vol").await.unwrap().len(), 1);
    clean(&dst).await;
    // And the good one still goes on.
    dst.recv_snapshot("vol", &mut incr.as_slice(), false)
        .await
        .unwrap();
    assert_eq!(read(&dst, "vol@s2").await, read(&src, "vol@s2").await);
    assert!(
        dst.recv_snapshot("vol", &mut &b"not a stream at all"[..], false)
            .await
            .is_err()
    );
    clean(&dst).await;
}

/// Sending checks its arguments.
#[tokio::test]
async fn send_checks_snapshots() {
    let (_sp, src) = source().await;
    let mut out = Vec::new();
    assert!(
        src.send_snapshot("vol", "nope", None, &mut out)
            .await
            .is_err()
    );
    assert!(
        src.send_snapshot("vol", "s1", Some("s2"), &mut out)
            .await
            .is_err(),
        "the base must be older"
    );
}

/// The preflight a client asks before streaming: it changes nothing, and
/// answers what a receive would.
#[tokio::test]
async fn recv_check_answers_without_changing_anything() {
    let (_sp, src) = source().await;
    let full = send(&src, "s1", None).await;
    let incr = send(&src, "s2", Some("s1")).await;
    let mut r = full.as_slice();
    let (header, raw) = peek_header(&mut r).await.unwrap();
    assert_eq!([raw.as_slice(), r].concat(), full, "nothing lost");
    let (incr_header, _) = peek_header(&mut incr.as_slice()).await.unwrap();

    let dp = TestPool::new().await;
    let dst = dp.format().await;
    dst.recv_check("vol", &header, false).await.unwrap();
    assert!(
        dst.list_volumes().await.unwrap().is_empty(),
        "a check creates nothing"
    );
    assert!(dst.recv_check("vol", &incr_header, false).await.is_err());
    dst.recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert!(
        dst.recv_check("vol", &header, false).await.is_err(),
        "exists"
    );
    dst.recv_check("vol", &incr_header, false).await.unwrap();
    dst.block_write("vol", 0, &pattern(4096, 9)).await.unwrap();
    assert!(dst.recv_check("vol", &incr_header, false).await.is_err());
    dst.recv_check("vol", &incr_header, true).await.unwrap();
    // Still written to (not rolled back by the check).
    let mut b = vec![0u8; 4096];
    dst.block_read("vol", 0, &mut b).await.unwrap();
    assert_eq!(b, pattern(4096, 9));
}

/// A received snapshot keeps the sender's creation time.
#[tokio::test]
async fn received_snapshots_keep_their_dates() {
    let (_sp, src) = source().await;
    let full = send(&src, "s1", None).await;
    let dp = TestPool::new().await;
    let dst = dp.format().await;
    dst.recv_snapshot("vol", &mut full.as_slice(), false)
        .await
        .unwrap();
    assert_eq!(
        dst.list_snapshots("vol").await.unwrap()[0].created_ns,
        src.list_snapshots("vol").await.unwrap()[0].created_ns
    );
}
