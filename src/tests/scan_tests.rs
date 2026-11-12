use super::*;
use crate::inode::InodeType;
use crate::test_util::{TOTAL_BLOCKS, TestDiskServer, TestPool, wait_until};
use crate::vdev::Pool;
use std::path::Path;

fn content(name: &str, generation: u8, len: usize) -> Vec<u8> {
    let seed = blake3::hash(name.as_bytes()).as_bytes()[0];
    (0..len)
        .map(|i| {
            (i as u8)
                .wrapping_mul(31)
                .wrapping_add(seed)
                .wrapping_add(generation)
        })
        .collect()
}

async fn write_files(fs: &FileSystem<Pool>, prefix: &str, generation: u8, n: usize) {
    for i in 0..n {
        let name = format!("/{}{}", prefix, i);
        fs.write_file(
            Path::new(&name),
            &content(&name, generation, 150_000 + i * 7_001),
        )
        .await
        .unwrap();
    }
}

async fn check_files(fs: &FileSystem<Pool>, prefix: &str, generation: u8, n: usize) {
    for i in 0..n {
        let name = format!("/{}{}", prefix, i);
        let data = fs.read_file(Path::new(&name)).await.unwrap();
        assert!(
            data == content(&name, generation, 150_000 + i * 7_001),
            "{} has wrong contents",
            name
        );
    }
}

/// First record of a file.
async fn first_record(fs: &FileSystem<Pool>, path: &str) -> crate::blkptr::BlockPtr {
    let r = fs.get_file_at_path(Path::new(path)).await.unwrap();
    match fs.read_inode(&r).await.unwrap().inode_type {
        InodeType::File { blocks, .. } => blocks[0].clone(),
        _ => unreachable!(),
    }
}

async fn format_remote(
    pool: &TestPool,
    server: &TestDiskServer,
    remote: &[usize],
) -> FileSystem<Pool> {
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
        let disk1 = &fs.dev.pool().unwrap().disks;
        assert!(wait_until(3, || !disk1.is_online(1)).await);
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
        assert!(wait_until(5, || disk1.is_online(1)).await);
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
        let again = fs.scrub(true).await.unwrap();
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
    let full = fs.scrub(true).await.unwrap();

    server.stop();
    let stripe = &fs.dev.pool().unwrap().disks;
    assert!(wait_until(3, || !stripe.is_online(4)).await);
    write_files(&fs, "new", 1, 1).await;
    server.restart().await;
    assert!(wait_until(5, || stripe.is_online(4)).await);

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
        let dry = fs.scrub(true).await.unwrap(); // read-only: never repairs
        assert!(!dry.repair);
        assert_eq!(dry.shards.mismatched[3], 1, "{}", dry);
        assert_eq!(dry.shards.mismatched[4], 1, "{}", dry);
        assert_eq!(dry.repaired(), 0);
    }

    let fs = pool.load().await;
    let report = fs.scrub(true).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert_eq!(report.shards.repaired[3], 1);
    assert_eq!(report.shards.repaired[4], 1);
    assert_eq!(fs.scrub(true).await.unwrap().mismatched(), 0);
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
    pool.corrupt(0, TestPool::stripe_offset(0)); // superblock mirror 0
    pool.corrupt(
        4,
        TestPool::stripe_offset(crate::test_util::UBERBLOCK_START),
    ); // uberblock parity

    let fs = pool.load().await;
    let report = fs.scrub(true).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert!(report.shards.repaired[2] >= 1, "{}", report);
    assert!(report.shards.repaired[0] >= 1, "{}", report);
    assert!(report.shards.repaired[4] >= 1, "{}", report);
    assert_eq!(fs.scrub(true).await.unwrap().mismatched(), 0);
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
        let stripe = &fs.dev.pool().unwrap().disks;
        assert_eq!(fs.degraded_disks()[2], Some(0));
        assert!(stripe.is_fenced(2));
        check_files(&fs, "f", 1, 3).await; // served around the blank disk

        let report = fs.resilver().await.unwrap();
        assert!(report.is_clean(), "{}", report);
        assert_eq!(report.cleared, vec![2]);
        assert!(!stripe.is_fenced(2));
        assert_eq!(fs.check_disk_label(2).await, LabelState::Ok);
    }

    let fs = FileSystem::load(pool.dev_without(&[0, 4]).await)
        .await
        .unwrap();
    assert_eq!(fs.degraded_disks(), vec![None; crate::test_util::DISKS]);
    check_files(&fs, "f", 1, 3).await;
}

#[tokio::test]
async fn scrub_runs_safely_alongside_volume_writes() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let vbs = crate::test_util::DEFAULT_VOLBLOCKSIZE as u64;
    fs.create_block("vol", 64 * vbs, vbs as u32).await.unwrap();

    let writer = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for round in 0..40u64 {
                for slot in 0..16u64 {
                    let fill = (round * 16 + slot) as u8;
                    let at = ((slot * 7 + round) % 64) * vbs;
                    fs.block_write("vol", at, &vec![fill; vbs as usize])
                        .await
                        .unwrap();
                }
                fs.sync().await.unwrap();
            }
        })
    };
    let mut scans = 0;
    while !writer.is_finished() {
        let r = fs.scrub(true).await.unwrap();
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
        fs.block_read("vol", i * vbs, &mut buf).await.unwrap();
        assert!(buf.iter().all(|&b| b == buf[0]), "record {} torn", i);
    }
    let r = fs.scrub(true).await.unwrap();
    assert_eq!(r.mismatched(), 0, "{}", r);
}

#[tokio::test]
async fn health_monitor_resilvers_a_returning_disk() {
    let pool = TestPool::new().await;
    let server = TestDiskServer::start(&pool.paths).await;
    let fs = Arc::new(format_remote(&pool, &server, &[3]).await);
    let monitor = spawn_health_monitor(fs.clone(), Duration::from_millis(100));

    server.stop();
    let stripe = &fs.dev.pool().unwrap().disks;
    assert!(wait_until(3, || !stripe.is_online(3)).await);
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
    let stripe = &fs.dev.pool().unwrap().disks;
    assert!(wait_until(3, || !stripe.is_online(4)).await);
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
