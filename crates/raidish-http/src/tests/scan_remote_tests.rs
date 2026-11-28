//! Scrub and resilver of pools with disks behind a disk server
//! (`crate::test_util::TestDiskServer`), and the health monitor
//! (`crate::committer::health`). The local-disk cases are with the
//! storage core (`raidish_core::scan`).

use crate::committer::health::spawn_health_monitor;
use crate::coords::*;
use crate::disk::PoolDevice;
use crate::fs::FileSystem;
use crate::test_util::{
    TOTAL_BLOCKS, TestDiskServer, TestPool, check_files, content, wait_until, write_files,
};
use crate::vdev::Pool;
use raidish_core::role::CommitterFs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

async fn format_remote(
    pool: &TestPool,
    server: &TestDiskServer,
    remote: &[usize],
) -> CommitterFs<Pool> {
    FileSystem::format(pool.dev_remote(server, remote).await, TOTAL_BLOCKS)
        .await
        .unwrap()
}

#[tokio::test]
async fn resilver_restores_redundancy_after_an_outage() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    {
        let fs = format_remote(&pool, &server, &[1]).await;
        write_files(&fs, "a", 1, 4).await;

        // Data disk 1 drops out while the pool keeps changing.
        server.stop();
        let disk1 = &fs.dev.pool().disks();
        assert!(wait_until(3, || !disk1.is_online(PoolDiskIdx::new(1).unwrap())).await);
        write_files(&fs, "a", 2, 2).await; // overwrite some
        write_files(&fs, "b", 2, 3).await; // and add more
        fs.sync().await.unwrap();
        let since = fs.degraded_disks()[1].expect("disk 1 should be degraded");
        assert!(
            since > 1,
            "only the outage needs rebuilding, not TXG {}",
            since
        );
        // Nothing to resilver while it's away.
        assert!(!fs.needs_resilver());

        server.restart().await;
        assert!(wait_until(5, || disk1.is_online(PoolDiskIdx::new(1).unwrap())).await);
        assert!(fs.needs_resilver());
        let report = fs.resilver().await.unwrap();
        println!("{}", report);
        assert!(report.is_clean(), "{}", report);
        assert_eq!(report.cleared, vec![1]);
        assert!(report.shards.repaired[1] > 0);
        for d in [0, 2, 3, 4] {
            assert_eq!(
                report.shards.mismatched[d], 0,
                "disk {} touched: {}",
                d, report
            );
        }
        assert_eq!(fs.degraded_disks(), vec![None; crate::test_util::DISKS]);

        // A second pass finds nothing left to fix.
        let again = fs.scrub_repair().await.unwrap();
        assert_eq!(again.mismatched(), 0, "{}", again);
    }

    // Now lose two *other* disks: only possible if disk 1 really is current.
    let fs = FileSystem::load(pool.dev_without(&[0, 3]).await)
        .await
        .unwrap();
    assert_eq!(fs.degraded_disks(), vec![None; crate::test_util::DISKS]);
    check_files(&fs, "a", 2, 2).await;
    check_files(&fs, "b", 2, 3).await;
    let a2 = "/a2";
    assert_eq!(
        fs.read_file(Path::new(a2)).await.unwrap(),
        content(a2, 1, 150_000 + 2 * 7_001)
    );
}

#[tokio::test]
async fn resilver_skips_what_did_not_change() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let fs = format_remote(&pool, &server, &[4]).await;
    write_files(&fs, "old", 1, 8).await;
    let full = fs.scrub_repair().await.unwrap();

    server.stop();
    let stripe = &fs.dev.pool().disks();
    assert!(wait_until(3, || !stripe.is_online(PoolDiskIdx::new(4).unwrap())).await);
    write_files(&fs, "new", 1, 1).await;
    server.restart().await;
    assert!(wait_until(5, || stripe.is_online(PoolDiskIdx::new(4).unwrap())).await);

    let report = fs.resilver().await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert_eq!(report.cleared, vec![4]);
    assert!(report.pruned > 0);
    assert!(
        report.records * 3 < full.records,
        "resilver read {} records, a full scrub {}",
        report.records,
        full.records
    );
    check_files(&fs, "new", 1, 1).await;
}

#[tokio::test]
async fn health_monitor_resilvers_a_returning_disk() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let fs = Arc::new(format_remote(&pool, &server, &[3]).await);
    let monitor = spawn_health_monitor(fs.clone(), Duration::from_millis(100), None);

    server.stop();
    let stripe = &fs.dev.pool().disks();
    assert!(wait_until(3, || !stripe.is_online(PoolDiskIdx::new(3).unwrap())).await);
    write_files(&fs, "m", 1, 2).await;
    assert!(fs.degraded_disks()[3].is_some());

    server.restart().await;
    let fs2 = fs.clone();
    assert!(
        wait_until(10, move || fs2.degraded_disks()[3].is_none()).await,
        "monitor never resilvered disk 3"
    );
    monitor.abort();
    drop(fs);

    let fs = FileSystem::load(pool.dev_without(&[0, 4]).await)
        .await
        .unwrap();
    check_files(&fs, "m", 1, 2).await;
}

#[tokio::test]
async fn resilver_only_touches_the_disk_that_was_away() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let fs = format_remote(&pool, &server, &[4]).await;
    write_files(&fs, "x", 1, 1).await;
    fs.sync().await.unwrap();
    server.stop();
    let stripe = &fs.dev.pool().disks();
    assert!(wait_until(3, || !stripe.is_online(PoolDiskIdx::new(4).unwrap())).await);
    write_files(&fs, "y", 1, 1).await;
    fs.sync().await.unwrap();
    drop(fs);
    server.restart().await;

    let fs = FileSystem::load(pool.dev_remote(&server, &[4]).await)
        .await
        .unwrap();
    assert!(fs.degraded_disks()[4].is_some());
    let report = fs.resilver().await.unwrap();
    assert_eq!(report.cleared, vec![4], "{}", report);
    for d in 0..4 {
        assert_eq!(
            report.shards.mismatched[d], 0,
            "disk {} touched: {}",
            d, report
        );
    }
}
