use super::*;
use crate::coords::PoolLba;
use crate::fs::LabelState;
use crate::role::CommitterFs;
use crate::test_util::ds;
use crate::test_util::{TestPool, check_files, write_files};
use crate::types::Txg;
use crate::vdev::Pool;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// First record of a file.
async fn first_record(fs: &CommitterFs<Pool>, path: &str) -> crate::blkptr::BlockPtr {
    let (_, d) = fs.stat_file(Path::new(path)).await.unwrap();
    fs.tree_leaves(&d.tree).await.unwrap()[0].1.clone()
}

#[tokio::test]
async fn scrub_finds_and_repairs_bad_parity() {
    let pool = TestPool::new().await;
    let lba = {
        let fs = pool.format().await;
        write_files(&fs, "f", 1, 2).await;
        first_record(&fs, "/f0").await.lba
    };
    // Parity is never read on a healthy read, so only a scrub notices.
    pool.corrupt(3, TestPool::stripe_offset(lba));
    pool.corrupt(4, TestPool::stripe_offset(lba + 3));

    {
        let fs = FileSystem::load_readonly(pool.dev().await).await.unwrap();
        check_files(&fs, "f", 1, 2).await;
        let dry = fs.scrub().await.unwrap(); // read-only: only a check
        assert!(!dry.repair);
        assert_eq!(dry.shards.mismatched[3], 1, "{}", dry);
        assert_eq!(dry.shards.mismatched[4], 1, "{}", dry);
        assert_eq!(dry.repaired(), 0);
    }

    let fs = pool.load().await;
    let report = fs.scrub_repair().await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert_eq!(report.shards.repaired[3], 1);
    assert_eq!(report.shards.repaired[4], 1);
    assert_eq!(fs.scrub_repair().await.unwrap().mismatched(), 0);
}

#[tokio::test]
async fn scrub_repairs_corrupt_data_and_labels() {
    let pool = TestPool::new().await;
    let lba = {
        let fs = pool.format().await;
        write_files(&fs, "f", 1, 1).await;
        first_record(&fs, "/f0").await.lba
    };
    pool.corrupt(2, TestPool::locate(lba + 2).1); // a data shard
    pool.corrupt(0, TestPool::stripe_offset(PoolLba::new(0))); // superblock mirror 0
    pool.corrupt(
        4,
        TestPool::stripe_offset(PoolLba::new(crate::test_util::UBERBLOCK_START)),
    ); // uberblock parity

    let fs = pool.load().await;
    let report = fs.scrub_repair().await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert!(report.shards.repaired[2] >= 1, "{}", report);
    assert!(report.shards.repaired[0] >= 1, "{}", report);
    assert!(report.shards.repaired[4] >= 1, "{}", report);
    assert_eq!(fs.scrub_repair().await.unwrap().mismatched(), 0);
    drop(fs);

    // Everything is whole again: two disks can go.
    let fs = FileSystem::load(pool.dev_without(&[1, 3]).await)
        .await
        .unwrap();
    check_files(&fs, "f", 1, 1).await;
}

#[tokio::test]
async fn replaced_disk_is_fenced_then_rebuilt() {
    let pool = TestPool::new().await;
    {
        let fs = pool.format().await;
        write_files(&fs, "f", 1, 3).await;
    }
    // Swap disk 2 for a blank one while the pool is offline.
    std::fs::File::create(&pool.paths[2])
        .unwrap()
        .set_len(crate::test_util::DISK_BYTES)
        .unwrap();

    {
        let fs = pool.load().await;
        let stripe = &fs.dev.pool().disks();
        assert_eq!(fs.degraded_disks()[2], Some(Txg::ZERO));
        assert!(stripe.is_fenced(crate::test_util::disk(2)));
        check_files(&fs, "f", 1, 3).await; // served around the blank disk

        let report = fs.resilver().await.unwrap();
        assert!(report.is_clean(), "{}", report);
        assert_eq!(report.cleared, vec![2]);
        assert!(!stripe.is_fenced(crate::test_util::disk(2)));
        assert_eq!(
            fs.check_disk_label(crate::test_util::disk(2)).await,
            LabelState::Ok
        );
    }

    let fs = FileSystem::load(pool.dev_without(&[0, 4]).await)
        .await
        .unwrap();
    assert_eq!(fs.degraded_disks(), vec![None; crate::test_util::DISKS]);
    check_files(&fs, "f", 1, 3).await;
}

/// I-21: a stripe with buffered (unflushed) writes is skipped by the
/// scan: it'll be rewritten whole on flush, so nothing on disk was checked
/// for it. A resilver that skipped one can't bring a disk back in sync.
#[tokio::test]
async fn a_resilver_that_skipped_a_stripe_clears_nothing() {
    let fp = TestPool::new().await.faulty();
    {
        let fs = fp.format().await;
        write_files(&fs, "f", 1, 3).await;
    }
    std::fs::File::create(&fp.pool.paths[2])
        .unwrap()
        .set_len(crate::test_util::DISK_BYTES)
        .unwrap();

    let fs = Arc::new(fp.load().await);
    assert_eq!(fs.degraded_disks()[2], Some(Txg::ZERO));
    // The vdevs see it too (I-20: a read-modify-write won't read it).
    let disks = fs.dev.pool().disks().clone();
    assert!(disks.may_lag(crate::test_util::disk(2)) && !disks.may_lag(crate::test_util::disk(0)));
    let lba = first_record(&fs, "/f0").await.lba;
    let mut block = [0u8; BLOCK_SIZE];
    fs.dev.read_block(lba, &mut block).await.unwrap();

    // The walk reaches the record, then a write of its block (as it is)
    // comes in: its stripe is buffered while the walk checks it.
    let at = TestPool::stripe_offset(lba) / BLOCK_SIZE as u64;
    for d in [0, 1, 3, 4] {
        fp.disk(d).hold_reads(Some(at..at + 1));
    }
    let scan = tokio::spawn({
        let fs = fs.clone();
        async move { fs.resilver().await }
    });
    fp.disk(0).wait_reads_held(1).await;
    fs.dev.write_block(lba, &block).await.unwrap();
    for d in [0, 1, 3, 4] {
        fp.disk(d).hold_reads(None);
    }
    let report = scan.await.unwrap().unwrap();
    assert!(
        report.shards.skipped > 0 && report.cleared.is_empty() && !report.certifies(),
        "{}",
        report
    );
    assert!(fs.degraded_disks()[2].is_some());

    fs.dev.flush().await.unwrap();
    let report = fs.resilver().await.unwrap();
    assert_eq!(report.cleared, vec![2], "{}", report);
    assert!(!disks.may_lag(crate::test_util::disk(2)));
    drop(report);
    let fs = Arc::into_inner(fs).unwrap();
    drop(fs);
    fp.disk(0).set_offline(true);
    fp.disk(4).set_offline(true);
    let fs = fp.load().await;
    check_files(&fs, "f", 1, 3).await;
}

#[tokio::test]
async fn scrub_runs_safely_alongside_volume_writes() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let vbs = crate::test_util::DEFAULT_VOLBLOCKSIZE as u64;
    fs.create_block(&ds("vol"), 64 * vbs, vbs as u32)
        .await
        .unwrap();

    let writer = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for round in 0..40u64 {
                for slot in 0..16u64 {
                    let fill = (round * 16 + slot) as u8;
                    let at = ((slot * 7 + round) % 64) * vbs;
                    fs.block_write(&ds("vol"), at, &vec![fill; vbs as usize])
                        .await
                        .unwrap();
                }
                fs.sync().await.unwrap();
            }
        })
    };
    let mut scans = 0;
    while !writer.is_finished() {
        let r = fs.scrub_repair().await.unwrap();
        assert!(r.is_clean(), "{}", r);
        assert_eq!(r.mismatched(), 0, "scrub found damage mid-write: {}", r);
        scans += 1;
    }
    writer.await.unwrap();
    assert!(scans > 1);
    assert_eq!(fs.scan_hold.load(Ordering::SeqCst), 0);
    fs.sync().await.unwrap();
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());

    // Every record holds a uniform fill: none was torn by a scan rewrite.
    let mut buf = vec![0u8; vbs as usize];
    for i in 0..64 {
        fs.block_read(&ds("vol"), i * vbs, &mut buf).await.unwrap();
        assert!(buf.iter().all(|&b| b == buf[0]), "record {} torn", i);
    }
    let r = fs.scrub_repair().await.unwrap();
    assert_eq!(r.mismatched(), 0, "{}", r);
}
