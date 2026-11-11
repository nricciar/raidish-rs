//! Crash consistency: the committed on-disk state must be valid at any
//! instant, even while operations and commits run concurrently.

use super::*;
use crate::test_util::TestPool;
use std::path::PathBuf;
use std::sync::Arc;

/// Load an independent view of whatever is committed on disk right now and
/// check it as if the process had crashed at this moment.
async fn check_snapshot<D: crate::disk::BlockDevice + Send + Sync + 'static>(
    pool: &TestPool,
    writer: &FileSystem<D>,
) -> Result<(), String> {
    // Holding the writer's transaction lock means no device writes are in
    // progress, so the disks hold exactly the last committed state: an exact
    // image of what a crash right now would leave behind.
    let _quiesce = writer.begin_tx().await;
    let snap = match FileSystem::load_readonly(pool.dev().await).await {
        Ok(fs) => fs,
        Err(e) => return Err(format!("snapshot failed to load: {e}")),
    };
    let orphans = snap
        .find_orphaned_blocks()
        .await
        .map_err(|e| format!("walk: {e}"))?;
    if !orphans.is_empty() {
        return Err(format!("leaked blocks in committed state: {:?}", orphans));
    }
    let index = snap
        .ls(Path::new("/d"))
        .await
        .map_err(|e| format!("ls: {e}"))?;
    for name in index.files.keys() {
        let path = PathBuf::from(format!("/d/{name}"));
        let data = snap
            .read_file(&path)
            .await
            .map_err(|e| format!("read {name}: {e}"))?;
        let expect = data.first().copied().unwrap_or(0);
        if !data.iter().all(|&b| b == expect) {
            return Err(format!("{name} has mixed contents"));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow (~30 s); run with: cargo test -- --ignored"]
async fn metadata_ops_and_background_commits_stay_consistent() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    fs.mkdir(Path::new("/d")).await.unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Background commits, like the NBD/NVMe servers' 5 s sync, but constant.
    let syncer = {
        let (fs, stop) = (fs.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                fs.sync().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    // Writers overwriting files: each overwrite frees the old version.
    let mut writers = Vec::new();
    for w in 0..3u8 {
        let fs = fs.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..60u32 {
                let name = PathBuf::from(format!("/d/w{w}-{}", i % 4));
                let fill = (w * 60 + (i % 200) as u8).max(1);
                fs.write_file(&name, &vec![fill; 20_000 + (i as usize % 7) * 9000])
                    .await
                    .unwrap();
            }
        }));
    }

    let mut problems = Vec::new();
    for _ in 0..25 {
        if let Err(e) = check_snapshot(&pool, &fs).await {
            problems.push(e);
        }
        tokio::task::yield_now().await;
    }
    for w in writers {
        w.await.unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    syncer.await.unwrap();
    fs.sync().await.unwrap();
    if let Err(e) = check_snapshot(&pool, &fs).await {
        problems.push(format!("final: {e}"));
    }
    assert!(
        problems.is_empty(),
        "{} bad snapshots, first: {}",
        problems.len(),
        problems[0]
    );
}

/// Each 4 KiB slot is only ever written with a uniform byte, so in any
/// committed state every slot must be uniform (some old or new version).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volume_writes_and_commits_stay_consistent() {
    let pool = TestPool::new().await;
    let fs = Arc::new(pool.format().await);
    let size = 8 * 1024 * 1024u64;
    fs.create_block("vol", size, crate::volume::DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let syncer = {
        let (fs, stop) = (fs.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                fs.sync().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let mut writers = Vec::new();
    for w in 0..4u64 {
        let fs = fs.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..400u64 {
                let slot = (i * 7 + w * 131) % (size / 4096);
                let fill = ((w * 400 + i) % 250 + 1) as u8;
                if i % 13 == 0 {
                    fs.block_zero("vol", slot * 4096, 4096).await.unwrap();
                } else {
                    fs.block_write("vol", slot * 4096, &[fill; 4096])
                        .await
                        .unwrap();
                }
            }
        }));
    }

    let mut problems = Vec::new();
    for _ in 0..20 {
        let quiesce = fs.begin_tx().await;
        let snap = FileSystem::load_readonly(pool.dev().await).await;
        drop(quiesce);
        match snap {
            Err(e) => problems.push(format!("load: {e}")),
            Ok(snap) => {
                if let Ok(o) = snap.find_orphaned_blocks().await
                    && !o.is_empty()
                {
                    problems.push(format!("leaked: {:?}", o));
                }
                let mut buf = vec![0u8; size as usize];
                match snap.block_read("vol", 0, &mut buf).await {
                    Err(e) => problems.push(format!("read: {e}")),
                    Ok(_) => {
                        for (n, s) in buf.chunks(4096).enumerate() {
                            if !s.iter().all(|&b| b == s[0]) {
                                problems.push(format!("slot {n} torn"));
                                break;
                            }
                        }
                    }
                }
            }
        }
        tokio::task::yield_now().await;
    }
    for w in writers {
        w.await.unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    syncer.await.unwrap();
    assert!(
        problems.is_empty(),
        "{} problems, first: {}",
        problems.len(),
        problems[0]
    );
}

async fn write_generation<D: crate::disk::BlockDevice + Send + Sync + 'static>(
    fs: &FileSystem<D>,
    generation: u8,
) {
    fs.write_file(
        &PathBuf::from(format!("/d/gen{generation}")),
        &vec![generation; 150_000],
    )
    .await
    .unwrap();
    fs.block_write(
        "vol",
        generation as u64 * 1_000_000,
        &vec![generation; 200_000],
    )
    .await
    .unwrap();
    fs.sync().await.unwrap();
}

async fn check_generations<D: crate::disk::BlockDevice + Send + Sync + 'static>(
    fs: &FileSystem<D>,
    upto: u8,
) {
    for g in 1..=upto {
        let f = fs
            .read_file(&PathBuf::from(format!("/d/gen{g}")))
            .await
            .unwrap();
        assert!(
            f.len() == 150_000 && f.iter().all(|&b| b == g),
            "file gen{g}"
        );
        let mut buf = vec![0u8; 200_000];
        fs.block_read("vol", g as u64 * 1_000_000, &mut buf)
            .await
            .unwrap();
        assert!(buf.iter().all(|&b| b == g), "volume gen{g}");
    }
    let orphans = fs.find_orphaned_blocks().await.unwrap();
    assert!(
        orphans.is_empty(),
        "orphans after gen {upto}: {:?}",
        orphans
    );
}

#[tokio::test]
async fn degraded_operation_and_stale_disk_return() {
    let pool = TestPool::new().await;
    {
        let fs = pool.format().await;
        fs.mkdir(Path::new("/d")).await.unwrap();
        fs.create_block("vol", 64 << 20, crate::volume::DEFAULT_VOLBLOCKSIZE)
            .await
            .unwrap();
        write_generation(&fs, 1).await;
    }
    // Data disk 1 gone: read everything, keep writing.
    {
        let fs = FileSystem::load(pool.dev_without(&[1]).await)
            .await
            .unwrap();
        check_generations(&fs, 1).await;
        write_generation(&fs, 2).await;
        check_generations(&fs, 2).await;
    }
    // Parity disk 4 gone as well as data disk 1 (two failures).
    {
        let fs = FileSystem::load(pool.dev_without(&[1, 4]).await)
            .await
            .unwrap();
        check_generations(&fs, 2).await;
        write_generation(&fs, 3).await;
        check_generations(&fs, 3).await;
    }
    // Both come back holding stale contents. Everything must still read
    // correctly and new writes must not clobber live data.
    {
        let fs = pool.load().await;
        let degraded = fs.degraded_disks();
        assert!(degraded[1].is_some() && degraded[4].is_some(), "{:?}", degraded);
        check_generations(&fs, 3).await;
        write_generation(&fs, 4).await;
        check_generations(&fs, 4).await;

        // Resilver brings both back in sync...
        let report = fs.resilver().await.unwrap();
        assert!(report.is_clean(), "{}", report);
        assert_eq!(report.cleared, vec![1, 4], "{}", report);
        assert!(report.shards.repaired[1] > 0 && report.shards.repaired[4] > 0, "{report}");
        for d in [0, 2, 3] {
            assert_eq!(report.shards.mismatched[d], 0, "disk {d} touched: {report}");
        }
    }
    // ...so the pool now survives losing the *other* two disks.
    let fs = FileSystem::load(pool.dev_without(&[0, 3]).await)
        .await
        .unwrap();
    check_generations(&fs, 4).await;
}

/// One fixed workload of metadata and volume operations.
async fn workload<D: crate::disk::BlockDevice + Send + Sync + 'static>(
    fs: &FileSystem<D>,
) -> Result<(), crate::fs::FileSystemError> {
    let file = |n: u32, v: u8| {
        (
            PathBuf::from(format!("/d/f{n}")),
            vec![v; 30_000 + n as usize * 7000],
        )
    };
    fs.mkdir(Path::new("/d")).await?;
    for n in 0..3 {
        let (p, d) = file(n, 10 + n as u8);
        fs.write_file(&p, &d).await?;
    }
    fs.create_block("vol", 4 << 20, crate::volume::DEFAULT_VOLBLOCKSIZE)
        .await?;
    for slot in 0..40u64 {
        fs.block_write("vol", slot * 9 * 4096, &[(slot % 200 + 1) as u8; 4096])
            .await?;
    }
    fs.sync().await?;
    for n in 0..3 {
        let (p, d) = file(n, 20 + n as u8);
        fs.write_file(&p, &d).await?;
    }
    fs.delete(Path::new("/d/f1")).await?;
    // A scrub in the middle: crashing in its commits must be harmless too.
    let r = fs.scrub(true).await?;
    assert!(r.is_clean(), "{}", r);
    for slot in 0..40u64 {
        if slot % 3 == 0 {
            fs.block_zero("vol", slot * 9 * 4096, 4096).await?;
        } else {
            fs.block_write("vol", slot * 9 * 4096, &[(slot % 200 + 101) as u8; 4096])
                .await?;
        }
    }
    fs.sync().await?;
    Ok(())
}

/// Crash after exactly k device writes, for k across the whole workload
/// (including inside commits), then reboot and check the pool.
#[tokio::test]
#[ignore = "slow (~2 min); run with: cargo test -- --ignored"]
async fn crash_at_every_point_of_a_workload() {
    // How many writes does the workload take?
    let total = {
        let pool = TestPool::new().await;
        let (fs, faults) = pool.format_faulty().await;
        faults.fail_after(i64::MAX);
        workload(&fs).await.unwrap();
        (i64::MAX - faults_remaining(&faults)) as usize
    };
    assert!(total > 50, "workload too small ({total} writes)");

    let step = (total / 80).max(1);
    let mut checked = 0;
    for k in (0..=total).step_by(step) {
        let pool = TestPool::new().await;
        {
            let (fs, faults) = pool.format_faulty().await;
            faults.fail_after(k as i64);
            let r = workload(&fs).await;
            assert!(r.is_err() == faults.tripped() || k == total, "k={k}: {r:?}");
            // Crash: drop everything that wasn't written.
        }
        let fs = FileSystem::load(pool.dev().await)
            .await
            .unwrap_or_else(|e| panic!("k={k}/{total}: pool failed to load after crash: {e}"));
        let orphans = fs.find_orphaned_blocks().await.unwrap();
        assert!(orphans.is_empty(), "k={k}/{total}: leaked {:?}", orphans);
        // Every reachable shard (data and parity) and label is consistent.
        let scrub = fs.scrub(false).await.unwrap();
        assert!(
            scrub.is_clean() && scrub.mismatched() == 0,
            "k={k}/{total}: {scrub}"
        );
        if let Ok(index) = fs.ls(Path::new("/d")).await {
            for name in index.files.keys() {
                let data = fs
                    .read_file(&PathBuf::from(format!("/d/{name}")))
                    .await
                    .unwrap_or_else(|e| panic!("k={k}: read {name}: {e}"));
                assert!(data.iter().all(|&b| b == data[0]), "k={k}: {name} torn");
            }
        }
        if fs.open_volume("vol").await.is_ok() {
            let mut buf = vec![0u8; 4 << 20];
            fs.block_read("vol", 0, &mut buf)
                .await
                .unwrap_or_else(|e| panic!("k={k}: volume read: {e}"));
            for (n, s) in buf.chunks(4096).enumerate() {
                assert!(s.iter().all(|&b| b == s[0]), "k={k}: volume slot {n} torn");
            }
        }
        // And it's still a working pool.
        fs.write_file(Path::new("/after"), &[7u8; 5000])
            .await
            .unwrap_or_else(|e| panic!("k={k}: write after crash: {e}"));
        checked += 1;
    }
    assert!(checked >= 40);
}

fn faults_remaining(f: &crate::test_util::FaultSwitch) -> i64 {
    f.remaining()
}
