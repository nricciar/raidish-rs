//! The per-member-disk fault harness (`crate::disk::fault`), and audit
//! reproductions written against it (see docs/audit-triage.md).
//!
//! Reproductions of bugs not fixed yet are `#[ignore = "audit <ID>"]`: they
//! fail today and document the bug; un-ignore them with the fix.

use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority, dataset_resource};
use crate::disk::{BLOCK_SIZE, Block, BlockDevice, Disk};
use crate::disks::DISK_LABEL_BLOCKS;
use crate::fs::FileSystem;
use crate::test_util::{FaultDisk, FaultError, FaultPool, FaultStats, TestPool};
use crate::types::{Epoch, Holding, LeaseRef};
use crate::vdev::Pool;
use std::sync::Arc;

const MB: usize = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn blk(v: u8) -> Block {
    [v; BLOCK_SIZE]
}

/// One fault disk over a fresh 1 MiB file.
fn one_disk() -> (tempfile::NamedTempFile, crate::test_util::FaultHandle, Disk) {
    let f = tempfile::NamedTempFile::new().unwrap();
    f.as_file().set_len(MB as u64).unwrap();
    let h = FaultDisk::open(f.path()).unwrap();
    let d = Disk::faulty(h.clone());
    (f, h, d)
}

async fn read(d: &Disk, b: u64) -> u8 {
    let mut buf = blk(0xEE);
    d.read_block(b, &mut buf).await.unwrap();
    assert!(buf.iter().all(|&x| x == buf[0]), "torn block {}", b);
    buf[0]
}

/// What's in the backing file, bypassing the fault layer (and its cache).
fn on_file(f: &tempfile::NamedTempFile, b: u64) -> u8 {
    use std::os::unix::fs::FileExt;
    let mut buf = [0u8; 1];
    f.as_file()
        .read_exact_at(&mut buf, b * BLOCK_SIZE as u64)
        .unwrap();
    buf[0]
}

// ---- the harness itself ----

#[tokio::test]
async fn write_failures_count_down_and_carry_the_error() {
    let (_f, h, d) = one_disk();
    h.fail_writes_after(2, FaultError::StaleEpoch);
    d.write_block(1, &blk(1)).await.unwrap();
    d.write_block(2, &blk(2)).await.unwrap();
    assert!(matches!(
        d.write_block(3, &blk(3)).await,
        Err(crate::disk::DiskError::StaleEpoch)
    ));
    assert!(d.write_block(4, &blk(4)).await.is_err());
    assert_eq!((read(&d, 2).await, read(&d, 3).await), (2, 0));
    h.fail_writes(None);
    d.write_block(3, &blk(3)).await.unwrap();
    assert_eq!(read(&d, 3).await, 3);
    let s = h.stats();
    assert_eq!((s.writes, s.failed_writes), (5, 2));

    h.fail_writes(Some(FaultError::Timeout));
    assert!(matches!(
        d.write_block(5, &blk(5)).await,
        Err(crate::disk::DiskError::Timeout)
    ));
}

#[tokio::test]
async fn faults_can_target_a_block_range() {
    let (_f, h, d) = one_disk();
    h.target_blocks(Some(10..20));
    h.fail_writes(Some(FaultError::Io));
    d.write_block(9, &blk(9)).await.unwrap();
    d.write_block(20, &blk(20)).await.unwrap();
    assert!(d.write_block(10, &blk(1)).await.is_err());
    // A run overlapping the range is targeted as a whole.
    assert!(d.write_blocks(5, &[blk(1); 6]).await.is_err());
    assert_eq!(read(&d, 9).await, 9);
}

#[tokio::test]
async fn flush_failures_and_offline() {
    let (_f, h, d) = one_disk();
    h.fail_flushes(Some(FaultError::Io));
    assert!(d.flush().await.is_err());
    h.fail_flushes(None);
    d.flush().await.unwrap();

    d.write_block(1, &blk(1)).await.unwrap();
    h.set_offline(true);
    assert!(!d.is_online());
    let mut buf = blk(0);
    assert!(d.read_block(1, &mut buf).await.is_err());
    assert!(d.write_block(1, &blk(2)).await.is_err());
    assert!(d.flush().await.is_err());
    assert!(d.capacity_blocks().await.is_err());
    h.heal();
    assert!(d.is_online());
    assert_eq!(read(&d, 1).await, 1);
    assert_eq!(d.capacity_blocks().await.unwrap(), (MB / BLOCK_SIZE) as u64);
    assert_eq!(
        h.stats(),
        FaultStats {
            reads: 1,
            writes: 2,
            failed_writes: 1,
            dropped_writes: 0,
            flushes: 3,
            failed_flushes: 2,
        }
    );
}

#[tokio::test]
async fn dropped_writes_are_acked_and_lost() {
    let (_f, h, d) = one_disk();
    d.write_block(1, &blk(1)).await.unwrap();
    h.drop_writes(true);
    d.write_block(1, &blk(2)).await.unwrap();
    d.flush().await.unwrap();
    assert_eq!(read(&d, 1).await, 1);
    assert_eq!(h.stats().dropped_writes, 1);
}

#[tokio::test]
async fn the_write_cache_is_lost_on_a_power_cut_unless_flushed() {
    let (f, h, d) = one_disk();
    h.cache_writes(true);
    d.write_block(1, &blk(1)).await.unwrap();
    assert_eq!(read(&d, 1).await, 1, "reads see the cache");
    assert_eq!(on_file(&f, 1), 0);
    d.flush().await.unwrap();
    assert_eq!(on_file(&f, 1), 1);

    d.write_block(1, &blk(2)).await.unwrap();
    d.write_block(2, &blk(2)).await.unwrap();
    // A failed flush persists nothing.
    h.fail_flushes(Some(FaultError::Io));
    assert!(d.flush().await.is_err());
    h.power_cut();
    assert_eq!((read(&d, 1).await, read(&d, 2).await), (1, 0));

    // Turning the cache off writes it back.
    h.fail_flushes(None);
    d.write_block(3, &blk(3)).await.unwrap();
    h.cache_writes(false);
    assert_eq!(on_file(&f, 3), 3);
}

#[tokio::test]
async fn held_writes_land_in_the_order_released() {
    let (_f, h, d) = one_disk();
    let d = Arc::new(d);
    h.hold_writes(true);
    let (d1, d2) = (d.clone(), d.clone());
    let first = tokio::spawn(async move { d1.write_block(1, &blk(1)).await });
    h.wait_held(1).await;
    let second = tokio::spawn(async move { d2.write_block(1, &blk(2)).await });
    h.wait_held(2).await;
    assert_eq!(read(&d, 1).await, 0, "nothing landed yet");
    // The later write lands first; the earlier one overwrites it.
    assert!(h.release_newest());
    second.await.unwrap().unwrap();
    assert_eq!(read(&d, 1).await, 2);
    assert!(h.release_oldest());
    first.await.unwrap().unwrap();
    assert_eq!(read(&d, 1).await, 1);
    assert!(!h.release_oldest());

    // A write whose caller is gone still lands when released.
    let d3 = d.clone();
    let gone = tokio::spawn(async move { d3.write_block(5, &blk(5)).await });
    h.wait_held(1).await;
    gone.abort();
    let _ = gone.await;
    h.hold_writes(false);
    h.release_all();
    assert_eq!((h.held(), read(&d, 5).await), (0, 5));
}

#[tokio::test]
async fn deferred_writes_reply_now_and_land_later() {
    let (_f, h, d) = one_disk();
    h.defer_writes(Some(Some(FaultError::Timeout)));
    assert!(d.write_block(1, &blk(1)).await.is_err());
    h.defer_writes(None);
    d.write_block(1, &blk(2)).await.unwrap();
    assert_eq!((h.deferred(), read(&d, 1).await), (1, 2));
    // The timed-out write lands over the newer one.
    assert_eq!(h.land_deferred(), 1);
    assert_eq!(read(&d, 1).await, 1);

    h.defer_writes(Some(None));
    d.write_block(2, &blk(2)).await.unwrap();
    assert_eq!(h.discard_deferred(), 1);
    assert_eq!(read(&d, 2).await, 0);
}

#[tokio::test]
async fn snapshot_and_restore_roll_the_image_back() {
    let (_f, h, d) = one_disk();
    d.write_block(1, &blk(1)).await.unwrap();
    h.snapshot();
    d.write_block(1, &blk(2)).await.unwrap();
    d.write_block(1, &blk(3)).await.unwrap();
    d.write_blocks(7, &[blk(7); 3]).await.unwrap();
    assert_eq!(h.restore(), 4);
    assert_eq!(read(&d, 1).await, 1);
    assert_eq!(read(&d, 8).await, 0);
}

#[tokio::test]
async fn a_fault_pool_reopens_with_its_faults_in_place() {
    for fp in [FaultPool::mirror(2).await, FaultPool::raidz(1, 3).await] {
        let fs = fp.format().await;
        fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
            .await
            .unwrap();
        fs.block_write("a", 0, &pattern(100_000, 1)).await.unwrap();
        fs.sync().await.unwrap();
        drop(fs);

        // One member gone, across a reopen: still readable, and it's
        // reported down.
        fp.disk(0).set_offline(true);
        let fs = fp.load().await;
        assert!(!fs.dev.disks().is_online(0));
        let mut buf = vec![0u8; 100_000];
        fs.block_read("a", 0, &mut buf).await.unwrap();
        assert_eq!(buf, pattern(100_000, 1));
        assert!(fp.disk(1).stats().reads > 0);
        drop(fs);
        fp.heal();
        let fs = fp.load().await;
        assert!(fs.fsck(false).await.unwrap().is_clean());
    }
}

// ---- audit reproductions ----

/// D1, at the vdev: disk 0 misses a write, disk 1 takes it but then can't
/// flush it. No member holds the data durably, yet the mirror reports the
/// flush done (before the D1 fix, it succeeded if *any* member's flush did).
#[tokio::test]
async fn d1_mirror_flush_with_no_durable_copy_fails() {
    let fp = FaultPool::mirror(2).await;
    let dev = fp.dev();
    fp.disk(0).fail_writes(Some(FaultError::Io));
    dev.write_block(100, &blk(7)).await.unwrap();
    fp.disk(0).fail_writes(None);
    fp.disk(1).fail_flushes(Some(FaultError::Io));
    assert!(
        dev.flush().await.is_err(),
        "flush acked with zero durable copies"
    );
}

/// D1: a flush given up on midway (its future dropped) leaves the writes
/// it took pending: a later flush must still cover them.
#[tokio::test]
async fn d1_a_cancelled_flush_keeps_its_writes_pending() {
    use crate::disks::WriteAcks;
    let acks = WriteAcks::new(1);
    acks.record(0b01);
    let stuck = acks.flush(std::future::pending::<Result<u64, crate::disk::DiskError>>);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), stuck)
            .await
            .is_err()
    );
    assert!(
        acks.flush(|| async { Ok(0b10) }).await.is_err(),
        "the write disk 0 took is still owed a flush of disk 0"
    );
    acks.flush(|| async { Ok(0b01) }).await.unwrap();
    acks.flush(|| async { Ok(0) }).await.unwrap();
}

/// D1: a repair (one disk's copy rewritten) doesn't fail the vdev's flush
/// when that disk can't flush, but it's reported lost to whoever asks, once.
#[tokio::test]
async fn d1_a_repair_a_disk_cant_flush_is_reported_lost() {
    let fp = FaultPool::mirror(2).await;
    let dev = fp.dev();
    let disks = dev.pool().unwrap().disks().clone();
    disks.repair_shards(1, 100, &[blk(7)]).await.unwrap();
    assert!(disks.take_repairs_lost(), "not flushed yet");
    dev.flush().await.unwrap();
    assert!(!disks.take_repairs_lost(), "flushed");

    disks.repair_shards(1, 100, &[blk(8)]).await.unwrap();
    fp.disk(1).fail_flushes(Some(FaultError::Io));
    dev.flush().await.unwrap();
    assert!(disks.take_repairs_lost(), "its disk's flush failed");
    assert!(!disks.take_repairs_lost(), "reported once");
}

/// Two flushes of a disk overlapping both cover the same repair: that's
/// one repair flushed, not two (the count went below zero, and every scan
/// after reported repairs lost).
#[tokio::test]
async fn d1_overlapping_flushes_count_a_repair_once() {
    let fp = FaultPool::mirror(2).await;
    let disks = fp.dev().pool().unwrap().disks().clone();
    disks.repair_shards(1, 100, &[blk(7)]).await.unwrap();
    fp.disk(1).hold_flushes(true);
    let (x, y, ()) = tokio::join!(disks.flush_disk(1), disks.flush_disk(1), async {
        fp.disk(1).wait_flushes_held(2).await;
        fp.disk(1).hold_flushes(false);
    });
    x.unwrap();
    y.unwrap();
    assert!(!disks.take_repairs_lost(), "flushed");
    disks.repair_shards(1, 100, &[blk(8)]).await.unwrap();
    assert!(disks.take_repairs_lost(), "a new repair, not flushed yet");
    disks.flush_disk(1).await.unwrap();
    assert!(!disks.take_repairs_lost(), "and that one flushed");
}

/// D1 on raidz1 over 3 disks: a full stripe that disk 0 missed is durable
/// only if both other disks flush it; one flush failing must fail the flush.
#[tokio::test]
async fn d1_raidz_flush_needs_data_many_durable_shards() {
    let fp = FaultPool::raidz(1, 3).await;
    let dev = fp.dev();
    dev.flush().await.unwrap();
    fp.disk(0).fail_writes(Some(FaultError::Io));
    dev.write_blocks(0, &[blk(3), blk(4)]).await.unwrap();
    fp.disk(0).fail_writes(None);
    fp.disk(1).fail_flushes(Some(FaultError::Io));
    assert!(dev.flush().await.is_err());
    // Still pending, so a flush once disk 1 recovers covers it.
    fp.disk(1).fail_flushes(None);
    dev.flush().await.unwrap();
    // A flush with only the missing disk failing is fine: the stripe was
    // never on it.
    fp.disk(0).fail_flushes(Some(FaultError::Io));
    dev.write_blocks(2, &[blk(5), blk(6)]).await.unwrap();
    dev.flush().await.unwrap();
}

/// D1, end to end: disk 1 has a volatile write cache. Disk 0 fails every
/// write, disk 1 fails its flush, then loses power. Whatever `sync` acked
/// must survive (before the D1 fix it returned Ok and the data was gone).
#[tokio::test]
async fn d1_acked_sync_survives_a_power_cut() {
    let fp = FaultPool::mirror(2).await;
    let fs = fp.format().await;
    fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    fs.sync().await.unwrap();

    fp.disk(1).cache_writes(true);
    fp.disk(0).fail_writes(Some(FaultError::Io));
    fp.disk(1).fail_flushes(Some(FaultError::Io));
    let data = pattern(200_000, 9);
    fs.block_write("a", 0, &data).await.unwrap();
    let acked = fs.sync().await.is_ok();
    drop(fs);
    fp.disk(1).power_cut();
    fp.disk(1).cache_writes(false);
    fp.heal();

    if acked {
        let fs = fp.load().await;
        let mut buf = vec![0u8; data.len()];
        let r = fs.block_read("a", 0, &mut buf).await;
        assert!(
            r.is_ok() && buf == data,
            "sync acked data no disk persisted ({:?})",
            r.err()
        );
    }
}

/// R2a: one member refusing a write for its epoch fails the whole write
/// (mirror and raidz), and doesn't count the member as having missed data.
#[tokio::test]
async fn r2a_a_fencing_refusal_fails_the_op_and_isnt_a_miss() {
    for fp in [FaultPool::mirror(2).await, FaultPool::raidz(1, 3).await] {
        let dev = fp.dev();
        fp.disk(1).fail_writes(Some(FaultError::StaleEpoch));
        let r = dev.write_blocks(0, &[blk(1), blk(2)]).await;
        assert!(
            matches!(r, Err(crate::disk::DiskError::StaleEpoch)),
            "{:?}",
            r.err()
        );
        assert_eq!(dev.disks().miss_count(1), 0);
        fp.disk(1).fail_writes(None);
        fp.disk(1).fail_flushes(Some(FaultError::StaleEpoch));
        assert!(matches!(
            dev.flush().await,
            Err(crate::disk::DiskError::StaleEpoch)
        ));
        assert_eq!(dev.disks().miss_count(1), 0);
    }
}

/// MiB `i` of volume "a" holds `pattern(MB, i + 1)`, for every `i < upto`.
async fn d2_check(fs: &FileSystem<Pool>, upto: u8) {
    for i in 0..upto {
        let mut buf = vec![0u8; MB];
        let r = fs.block_read("a", i as u64 * MB as u64, &mut buf).await;
        assert!(
            r.is_ok() && buf == pattern(MB, i + 1),
            "MiB {} after the stale disk returned ({:?})",
            i,
            r.err()
        );
    }
}

/// D2: mirror disk 1 drops out (snapshotted first), misses several commits,
/// and comes back with its old image: a valid label, stale space maps and
/// uberblocks. The pool must not take any of its metadata: every commit
/// made meanwhile is there on reopen, and writing on reuses no live space.
///
/// Passes today: the uberblock ring (`read_label_versions`) and space-map
/// slots (`read_slot`) read every copy and take the newest TXG, so the
/// stale disk's versions lose even though it isn't fenced. Kept as the
/// regression test for D2.
#[tokio::test]
async fn d2_a_stale_returning_mirror_disk_is_not_read_for_metadata() {
    let fp = FaultPool::mirror(2).await;
    let fs = fp.format().await;
    fs.create_block("a", 16 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    fs.block_write("a", 0, &pattern(MB, 1)).await.unwrap();
    fs.sync().await.unwrap();
    drop(fs);

    fp.disk(1).snapshot();
    fp.disk(1).set_offline(true);
    let fs = fp.load().await;
    for i in 1..6u8 {
        fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
            .await
            .unwrap();
        fs.sync().await.unwrap();
    }
    let degraded = fs.degraded_disks();
    assert!(degraded[1].is_some(), "{:?}", degraded);
    drop(fs);

    // It comes back as it was before the outage. (Offline, nothing reached
    // it, so this rolls back nothing; it's what makes sure of that.)
    fp.disk(1).restore();
    fp.heal();
    let reads = fp.disk(1).stats().reads;
    let fs = fp.load().await;
    d2_check(&fs, 6).await;
    assert!(
        fp.disk(1).stats().reads > reads + 10,
        "the stale disk serves reads (it isn't fenced)"
    );

    // More commits on top: a stale space map would hand out live blocks.
    for i in 6..12u8 {
        fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
            .await
            .unwrap();
        fs.sync().await.unwrap();
    }
    d2_check(&fs, 12).await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    drop(fs);

    // And with the good disk gone, the resilvered stale one has it all.
    let fs = fp.load().await;
    fs.resilver().await.unwrap();
    drop(fs);
    fp.disk(0).set_offline(true);
    let fs = fp.load().await;
    d2_check(&fs, 12).await;
}

/// D2, the case the test above misses: every space-map slot already holds
/// a valid label on the stale disk (several commits before the outage), so
/// a read served by it parses fine at an old TXG. The loader must still see
/// the newer copy on the other disk, whichever member serves the read.
#[tokio::test]
async fn d2_stale_slots_that_parse_dont_win() {
    for stale in 0..2usize {
        let fp = FaultPool::mirror(2).await;
        let fs = fp.format().await;
        fs.create_block("a", 32 * MB as u64, fs.default_volblocksize())
            .await
            .unwrap();
        for i in 0..4u8 {
            fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
                .await
                .unwrap();
            fs.sync().await.unwrap();
        }
        drop(fs);

        fp.disk(stale).snapshot();
        fp.disk(stale).set_offline(true);
        let fs = fp.load().await;
        for i in 4..10u8 {
            fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
                .await
                .unwrap();
            fs.sync().await.unwrap();
        }
        drop(fs);
        fp.disk(stale).restore();
        fp.heal();

        let fs = fp.load().await;
        for i in 10..20u8 {
            fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
                .await
                .unwrap();
            fs.sync().await.unwrap();
        }
        d2_check(&fs, 20).await;
        let report = fs.fsck(false).await.unwrap();
        assert!(report.is_clean(), "stale disk {}: {}", stale, report);
    }
}

/// D3: on a 3-way mirror, two members miss a run of commits together and
/// come back agreeing on their old labels. A repairing scrub must keep the
/// one newer copy, not "fix" it to the stale majority: afterwards the pool
/// still has everything with only the formerly stale members left.
#[tokio::test]
async fn d3_a_stale_majority_doesnt_win_label_repair() {
    let fp = FaultPool::mirror(3).await;
    let fs = fp.format().await;
    fs.create_block("a", 32 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    for i in 0..3u8 {
        fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
            .await
            .unwrap();
        fs.sync().await.unwrap();
    }
    drop(fs);

    for d in [1, 2] {
        fp.disk(d).snapshot();
        fp.disk(d).set_offline(true);
    }
    let fs = fp.load().await;
    for i in 3..8u8 {
        fs.block_write("a", i as u64 * MB as u64, &pattern(MB, i + 1))
            .await
            .unwrap();
        fs.sync().await.unwrap();
    }
    drop(fs);
    for d in [1, 2] {
        fp.disk(d).restore();
    }
    fp.heal();

    let fs = fp.load().await;
    let report = fs.scrub(true).await.unwrap();
    assert!(report.label_unrecoverable.is_empty(), "{}", report);
    d2_check(&fs, 8).await;
    drop(fs);

    // The labels the scrub settled on are the new ones: the old majority,
    // reopened alone, has every commit.
    fp.disk(0).set_offline(true);
    let fs = fp.load().await;
    d2_check(&fs, 8).await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

// ---- ZIL ----

async fn lease(auth: &LocalAuthority, resource: impl std::fmt::Display, owner: &str) -> Epoch {
    let resource = crate::types::res(resource);
    let op = LeaseOp::Acquire {
        resource: resource.clone(),
        owner: owner.into(),
        holder: crate::types::Holder::default().with_detail(format!("test {}", owner)),
        ttl_ms: 600_000,
    };
    match auth.propose(op).await.unwrap() {
        Response::Granted { epoch, .. } => epoch,
        r => panic!("{} not granted: {:?}", resource, r),
    }
}

async fn release(auth: &LocalAuthority, resource: impl std::fmt::Display, owner: &str, e: Epoch) {
    let op = LeaseOp::Release {
        resource: crate::types::res(resource),
        holder: Holding::new(owner, e),
    };
    assert_eq!(auth.propose(op).await.unwrap(), Response::Released);
}

struct ZilSetup {
    fp: FaultPool,
    auth: Arc<LocalAuthority>,
    committer: Arc<Committer<Pool>>,
}

/// A 2-way mirror with volume "a" and a committer, every disk faultable.
async fn zil_setup() -> ZilSetup {
    let fp = FaultPool::mirror(2).await;
    let fs = fp.format().await;
    fs.create_block("a", 8 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    drop(fs);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, &POOL, "c").await;
    let fs = Arc::new(fp.load().await);
    let committer =
        Arc::new(Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await);
    ZilSetup {
        fp,
        auth,
        committer,
    }
}

struct ZilWriter {
    fs: FileSystem<Pool>,
    epoch: Epoch,
    ds_epoch: Epoch,
}

impl ZilWriter {
    async fn start(s: &ZilSetup) -> Self {
        let epoch = lease(&s.auth, "writer/1", "1").await;
        let sink = Arc::new(DirectSink {
            committer: s.committer.clone(),
            writer: LeaseRef::new(crate::types::res("writer/1"), epoch),
        });
        let fs = FileSystem::load_writer(s.fp.dev(), sink).await.unwrap();
        let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
        fs.attach_dataset_leased("a", Holding::new("o1", ds_epoch), None, None)
            .await
            .unwrap();
        ZilWriter {
            fs,
            epoch,
            ds_epoch,
        }
    }

    /// The process dies: its leases lapse, nothing is detached.
    async fn die(self, s: &ZilSetup) {
        drop(self.fs);
        release(&s.auth, &dataset_resource("a"), "o1", self.ds_epoch).await;
        release(&s.auth, "writer/1", "1", self.epoch).await;
    }

    fn cursor(&self) -> crate::zil::LogCursor {
        let vol = self
            .fs
            .open_datasets
            .lock()
            .unwrap()
            .get("a")
            .cloned()
            .unwrap();
        vol.cursor.lock().unwrap().clone().expect("a log region")
    }
}

/// Hand-built commits against the committer: overlapping claims are
/// refused (S6c-2), and a commit that only moves the intent log on (the
/// tree unchanged) is acknowledged when resent, not refused (S6b).
#[tokio::test]
async fn committer_claims_are_exact_and_log_only_resends_are_acked() {
    use crate::inode::{DatasetLog, LogRegion};
    use crate::types::Extent;
    use crate::writer::{CommitError, DatasetCommit};
    let s = zil_setup().await;
    let c = &s.committer;
    let e1 = lease(&s.auth, "writer/1", "1").await;
    let w1 = LeaseRef::new(crate::types::res("writer/1"), e1);
    let ds = Holding::new("o1", lease(&s.auth, &dataset_resource("a"), "o1").await);
    let lent = c.lend(&w1, 64).await.unwrap();
    let free = *lent.free.iter().max_by_key(|e| e.len).unwrap();
    assert!(free.len >= 16, "{:?}", lent.free);
    let root = c.fs().open_dataset("a").await.unwrap().obj.root_ptr();
    let commit = |allocs: Vec<Extent>, log: Option<DatasetLog>| DatasetCommit {
        dataset: "a".into(),
        writer: w1.clone(),
        ds: ds.clone(),
        base_root: root.clone(),
        new_root: root.clone(),
        allocs,
        frees: vec![],
        free_births: vec![],
        missed: vec![],
        birth_txg: lent.open_txg,
        log,
        min_birth: None,
    };

    let overlapping = commit(
        vec![Extent::new(free.start, 4), Extent::new(free.start + 2, 4)],
        None,
    );
    let r = c.commit(&overlapping).await;
    assert!(
        matches!(&r, Err(CommitError::Refused { msg, .. }) if msg.contains("overlap")),
        "{:?}",
        r
    );

    let region = LogRegion {
        start: free.start,
        len: 8,
        guid: 7,
    };
    let log = DatasetLog {
        regions: vec![region.clone()],
        from_seq: 0,
        writer: w1.clone(),
        ds: ds.clone(),
    };
    let log_only = commit(vec![crate::zil::region_alloc(&region)], Some(log.clone()));
    c.commit(&log_only).await.unwrap();
    // Its answer lost, the writer resends it: already applied.
    c.commit(&log_only).await.unwrap();
    let vol = c.fs().open_dataset("a").await.unwrap();
    assert_eq!(*vol.log.lock().unwrap(), Some(log));
}

/// S4a: a flush that falls back to a commit (here: volume "b" has no log
/// yet) writes none of the appends it planned for the others. Written and
/// never flushed, such a slot could be lost while later records after it
/// were flushed, and replay stops at the gap. (The commit folds "a" here,
/// so its log moves on to a new region either way; when it doesn't, the
/// planned appends are put back and nothing more is logged after them.)
#[tokio::test]
async fn s4a_a_commit_fallback_writes_no_unflushed_log_records() {
    let s = zil_setup().await;
    s.committer
        .fs()
        .create_block("b", 8 * MB as u64, s.committer.fs().default_volblocksize())
        .await
        .unwrap();
    let w = ZilWriter::start(&s).await;
    let ds_b = lease(&s.auth, &dataset_resource("b"), "o1").await;
    w.fs.attach_dataset_leased("b", Holding::new("o1", ds_b), None, None)
        .await
        .unwrap();
    w.fs.block_write("a", 0, &pattern(100_000, 1))
        .await
        .unwrap();
    w.fs.flush_for("a").await.unwrap(); // a's log is registered
    let c = w.cursor();

    // Watch a's next log slot (writes there are counted as dropped).
    let start = c.region.start + c.used + DISK_LABEL_BLOCKS;
    for d in 0..2 {
        s.fp.disk(d)
            .target_blocks(Some(start..start + c.region.len - c.used));
        s.fp.disk(d).drop_writes(true);
    }
    w.fs.block_write("a", 0, &pattern(50_000, 2)).await.unwrap();
    w.fs.block_write("b", 0, &pattern(50_000, 3)).await.unwrap();
    w.fs.flush_for("b").await.unwrap();
    for d in 0..2 {
        assert_eq!(
            s.fp.disk(d).stats().dropped_writes,
            0,
            "a's planned record was written (disk {})",
            d
        );
    }
    s.fp.heal();
    w.fs.flush_for("a").await.unwrap();
    let mut want = pattern(100_000, 1);
    want[..50_000].copy_from_slice(&pattern(50_000, 2));
    assert_eq!(read_a(&w.fs, want.len()).await, want);
}

async fn txg(s: &ZilSetup) -> u64 {
    s.committer.fs().uberblock().await.unwrap().txg.get()
}

async fn read_a(fs: &FileSystem<Pool>, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    let n = fs.block_read("a", 0, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

/// The log record of an acked flush lost on one mirror member (dropped by
/// its cache) is read from the other on replay, after the writer crashes.
#[tokio::test]
async fn zil_a_record_one_member_lost_is_replayed_from_the_other() {
    let s = zil_setup().await;
    let w = ZilWriter::start(&s).await;
    w.fs.block_write("a", 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap(); // commits: registers the log
    let c = w.cursor();

    // Disk 0 loses (only) the next log record's blocks, without a word.
    let start = c.region.start + c.used + DISK_LABEL_BLOCKS;
    s.fp.disk(0)
        .target_blocks(Some(start..start + c.region.len - c.used));
    s.fp.disk(0).drop_writes(true);
    let before = txg(&s).await;
    w.fs.block_write("a", 0, &pattern(50_000, 2)).await.unwrap();
    w.fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "acked from the log");
    assert!(s.fp.disk(0).stats().dropped_writes > 0);
    s.fp.heal();

    w.die(&s).await;
    let logs = s.committer.fs().dataset_logs().await.unwrap();
    assert_eq!(logs.len(), 1);
    let n = s
        .committer
        .fs()
        .replay_dataset_log(&logs[0].0, &logs[0].1)
        .await
        .unwrap();
    assert!(n > 0, "the record was replayed");
    let mut want = pattern(300_000, 1);
    want[..50_000].copy_from_slice(&pattern(50_000, 2));
    let fresh = s.fp.load().await;
    assert_eq!(read_a(&fresh, want.len()).await, want);
}

/// A log write still in flight to disk 0 when the writer crashes (D5b: a
/// timed-out remote write left running) lands after the replay committed
/// and the space was reused. It must not clobber live data: with disk 1
/// gone, disk 0 alone must still read back every acked write.
///
/// Passes today (the reused space doesn't overlap the old record here);
/// kept as a D5b regression test, not a proof that D5b can't happen.
#[tokio::test]
async fn zil_an_in_flight_log_write_landing_after_replay_clobbers_nothing() {
    let s = zil_setup().await;
    let w = ZilWriter::start(&s).await;
    w.fs.block_write("a", 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap(); // commits: registers the log
    w.fs.block_write("a", 0, &pattern(10_000, 2)).await.unwrap();
    w.fs.flush().await.unwrap(); // logged

    // The next record's write to disk 0 hangs; the writer crashes waiting.
    s.fp.disk(0).hold_writes(true);
    w.fs.block_write("a", 0, &pattern(10_000, 3)).await.unwrap();
    let w = Arc::new(w);
    let flusher = {
        let w = w.clone();
        tokio::spawn(async move { w.fs.flush().await })
    };
    s.fp.disk(0).wait_held(1).await;
    s.fp.disk(0).hold_writes(false);
    flusher.abort();
    let _ = flusher.await;
    let w = Arc::try_unwrap(w).ok().expect("flusher gone");
    w.die(&s).await;

    // The committer replays the log and commits; later commits reuse space.
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
    let w = ZilWriter::start(&s).await;
    let mut want = read_a(&w.fs, 4 * MB).await;
    for i in 0..8u8 {
        let at = (i as usize) * 400_000;
        let data = pattern(400_000, 10 + i);
        w.fs.block_write("a", at as u64, &data).await.unwrap();
        w.fs.sync().await.unwrap();
        want[at..at + data.len()].copy_from_slice(&data);
    }
    w.die(&s).await;

    // Now the stale record lands on disk 0.
    assert_eq!(s.fp.disk(0).held(), 1);
    s.fp.disk(0).release_all();
    s.fp.disk(1).set_offline(true);
    let fresh = s.fp.load().await;
    assert_eq!(read_a(&fresh, want.len()).await, want);
    let report = fresh.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Smoke check that faults on raidz members aim where the test says.
#[tokio::test]
async fn raidz_survives_one_member_failing_every_write() {
    let fp = TestPool::raidz(1, 3).await.faulty();
    let fs = fp.format().await;
    fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    fp.disk(2).fail_writes(Some(FaultError::Io));
    fs.block_write("a", 0, &pattern(200_000, 4)).await.unwrap();
    fs.sync().await.unwrap();
    assert!(fs.degraded_disks()[2].is_some());
    drop(fs);
    fp.heal();
    let fs = fp.load().await;
    assert_eq!(read_a(&fs, 200_000).await, pattern(200_000, 4));
    fs.resilver().await.unwrap();
    assert!(fs.degraded_disks()[2].is_none());
    drop(fs);
    fp.disk(0).set_offline(true);
    let fs = fp.load().await;
    assert_eq!(read_a(&fs, 200_000).await, pattern(200_000, 4));
}

/// An authority whose answers can be lost: the op is applied, the caller
/// hears nothing (a leader that committed it and then died).
struct LossyAuthority {
    inner: Arc<LocalAuthority>,
    /// Answers to let through before losing any.
    pass: std::sync::atomic::AtomicUsize,
    lose: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Authority for LossyAuthority {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        let r = self.inner.propose(op).await;
        let pass = self.pass.load(std::sync::atomic::Ordering::SeqCst);
        if pass > 0 {
            self.pass
                .store(pass - 1, std::sync::atomic::Ordering::SeqCst);
            return r;
        }
        let lose = self.lose.load(std::sync::atomic::Ordering::SeqCst);
        if lose > 0 {
            self.lose
                .store(lose - 1, std::sync::atomic::Ordering::SeqCst);
            return Err("answer lost".into());
        }
        r
    }
    fn state(&self) -> crate::cluster::state::ClusterState {
        self.inner.state()
    }
}

/// R4d: a lend or a return whose answer was lost doesn't wedge the
/// metaslab. A return is resent naming the same lend (answered as the
/// first time); a lend in doubt is settled by `reclaim` and taken back.
#[tokio::test]
async fn r4d_lost_answers_to_a_lend_or_return_dont_wedge_the_metaslab() {
    use crate::cluster::state::MetaslabGrant;
    use std::sync::atomic::Ordering::SeqCst;
    let fp = FaultPool::mirror(2).await;
    drop(fp.format().await);
    let inner = Arc::new(LocalAuthority::default());
    let epoch = lease(&inner, &POOL, "c").await;
    let auth = Arc::new(LossyAuthority {
        inner: inner.clone(),
        pass: Default::default(),
        lose: Default::default(),
    });
    let fs = Arc::new(fp.load().await);
    let c = Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await;
    let e1 = lease(&inner, "writer/1", "1").await;
    let w1 = LeaseRef::new(crate::types::res("writer/1"), e1);
    let grant = |idx| inner.state().metaslabs.get(&idx).cloned();

    // A return whose answer is lost, sent again.
    let lent = c.lend(&w1, 64).await.unwrap();
    auth.lose.store(1, SeqCst);
    assert!(c.give_back(&w1, lent.idx).await.is_err());
    assert!(matches!(
        grant(lent.idx),
        Some(MetaslabGrant::Returned { .. })
    ));
    c.give_back(&w1, lent.idx).await.unwrap();
    assert!(!c.fs().lent_metaslabs().await.contains(&lent.idx.get()));

    // A lend whose answer is lost: it went through, the writer never heard.
    auth.lose.store(1, SeqCst);
    assert!(c.lend(&w1, 64).await.is_err());
    let idx = *inner
        .state()
        .metaslabs
        .iter()
        .find(|(_, g)| matches!(g, MetaslabGrant::Lent { .. }))
        .expect("the lend went through")
        .0;
    assert!(
        c.fs().lent_metaslabs().await.contains(&idx.get()),
        "kept out of use"
    );
    c.reclaim().await;
    assert!(matches!(grant(idx), Some(MetaslabGrant::Returned { .. })));
    assert!(!c.fs().lent_metaslabs().await.contains(&idx.get()));
    // And it lends again.
    let again = c.lend(&w1, 64).await.unwrap();
    c.give_back(&w1, again.idx).await.unwrap();

    // Both answers lost: the lend's, then (once `reclaim` settled the
    // lend) the return's. The resent return still names the lend, and
    // the metaslab comes back as returned, stamped with the return's
    // epoch, not as a lend that never happened.
    auth.lose.store(1, SeqCst);
    assert!(c.lend(&w1, 64).await.is_err());
    let (&idx, lend) = inner
        .state()
        .metaslabs
        .iter()
        .find(|(_, g)| matches!(g, MetaslabGrant::Lent { .. }))
        .map(|(i, g)| (i, g.epoch()))
        .expect("the lend went through");
    auth.pass.store(1, SeqCst);
    auth.lose.store(1, SeqCst);
    c.reclaim().await;
    assert!(
        matches!(grant(idx), Some(MetaslabGrant::Returned { from, .. }) if from == lend),
        "returned"
    );
    c.reclaim().await;
    let Some(MetaslabGrant::Returned { epoch: back, .. }) = grant(idx) else {
        panic!("still returned")
    };
    assert!(!c.fs().lent_metaslabs().await.contains(&idx.get()));
    let layout = c.fs().layout;
    let lba = layout.data_start + idx.index() as u64 * layout.metaslab_size_blocks;
    let fence = c.fs().dev.pool().unwrap().disks().fence();
    assert_eq!(
        fence.epoch_for(Some(0), lba + crate::disks::DISK_LABEL_BLOCKS, 1),
        crate::fence::Stamp::Returned(back),
        "taken back as returned"
    );
}

/// S6a: a commit whose dataset lease is lost while it waits for the tx is
/// refused, not applied on a check made before the wait.
#[tokio::test]
async fn s6a_a_lease_lost_while_waiting_for_the_tx_refuses_the_commit() {
    use crate::inode::{DatasetLog, LogRegion};
    use crate::writer::{CommitError, DatasetCommit};
    let s = zil_setup().await;
    let c = s.committer.clone();
    let e1 = lease(&s.auth, "writer/1", "1").await;
    let w1 = LeaseRef::new(crate::types::res("writer/1"), e1);
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    let ds = Holding::new("o1", ds_epoch);
    let lent = c.lend(&w1, 64).await.unwrap();
    let free = *lent.free.iter().max_by_key(|e| e.len).unwrap();
    let root = c.fs().open_dataset("a").await.unwrap().obj.root_ptr();
    let region = LogRegion {
        start: free.start,
        len: 8,
        guid: 9,
    };
    let commit = DatasetCommit {
        dataset: "a".into(),
        writer: w1.clone(),
        ds: ds.clone(),
        base_root: root.clone(),
        new_root: root,
        allocs: vec![crate::zil::region_alloc(&region)],
        frees: vec![],
        free_births: vec![],
        missed: vec![],
        birth_txg: lent.open_txg,
        log: Some(DatasetLog {
            regions: vec![region],
            from_seq: 0,
            writer: w1.clone(),
            ds,
        }),
        min_birth: None,
    };
    let tx = c.fs().begin_tx().await;
    let pending = {
        let c = c.clone();
        let commit = commit.clone();
        tokio::spawn(async move { c.commit(&commit).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
    lease(&s.auth, &dataset_resource("a"), "o2").await;
    drop(tx);
    let r = pending.await.unwrap();
    assert!(matches!(r, Err(CommitError::Refused { .. })), "{:?}", r);
    assert_eq!(
        *c.fs().open_dataset("a").await.unwrap().log.lock().unwrap(),
        None
    );
}

// ---- S5a/b: an operation that fails partway leaves nothing behind ----

/// Everything a test can see of the pool: the tree under `/` (names and
/// contents), the volumes and their snapshots, and how many records fsck
/// walks (a dnode written and left unreferenced still counts).
async fn s5_seen(fs: &FileSystem<Pool>) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let mut dirs = vec![std::path::PathBuf::from("/")];
    while let Some(d) = dirs.pop() {
        for (name, e) in fs.ls(&d).await.unwrap().files {
            let p = d.join(&name);
            match fs.read_file(&p).await {
                Ok(data) => writeln!(out, "{} {:?} {}", p.display(), e.kind, data.len()).unwrap(),
                Err(_) => {
                    writeln!(out, "{} {:?}", p.display(), e.kind).unwrap();
                    dirs.push(p);
                }
            }
        }
    }
    for v in fs.list_volumes().await.unwrap() {
        writeln!(out, "volume {} {}", v.name, v.size_bytes).unwrap();
        for s in fs.list_snapshots(&v.name).await.unwrap() {
            writeln!(out, "  snapshot {}", s.name).unwrap();
        }
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    writeln!(out, "records {}", report.records).unwrap();
    out
}

/// Run `op` with every disk failing from its `n`th write on, for each `n`
/// until it succeeds. Each failure must leave the pool as `before` (seen
/// after healing, so a commit takes whatever the failure left); success
/// must change it.
async fn s5_every_failure_point<O>(fp: &FaultPool, fs: &FileSystem<Pool>, op: O)
where
    O: for<'a> Fn(
        &'a FileSystem<Pool>,
    ) -> futures::future::BoxFuture<'a, Result<(), crate::fs::FileSystemError>>,
{
    let before = s5_seen(fs).await;
    for n in 0..500u64 {
        for d in &fp.disks {
            d.fail_writes_after(n, FaultError::Io);
        }
        let r = op(fs).await;
        fp.heal();
        match r {
            Ok(()) => {
                assert!(n > 0, "the op wrote nothing: this tests nothing");
                assert_ne!(s5_seen(fs).await, before, "the op changed nothing");
                return;
            }
            Err(e) => {
                let seen = s5_seen(fs).await;
                assert_eq!(seen, before, "failing at write {} ({}) left a change", n, e);
            }
        }
    }
    panic!("the op never succeeded");
}

/// A small tree: `/a`, `/d/b`, `/d/e/` and volume `vol`, committed.
async fn s5_pool() -> (FaultPool, FileSystem<Pool>) {
    let fp = FaultPool::mirror(2).await;
    let fs = fp.format().await;
    fs.write_file("/a".as_ref(), &pattern(3000, 1))
        .await
        .unwrap();
    fs.mkdir("/d".as_ref()).await.unwrap();
    fs.write_file("/d/b".as_ref(), &pattern(20_000, 2))
        .await
        .unwrap();
    fs.mkdir("/d/e".as_ref()).await.unwrap();
    fs.create_block("vol", 4 * MB as u64, crate::test_util::VOLBLOCK_ALIGN)
        .await
        .unwrap();
    fs.block_write("vol", 0, &pattern(MB, 3)).await.unwrap();
    fs.sync().await.unwrap();
    (fp, fs)
}

macro_rules! s5_op {
    (|$fs:ident, $tx:ident| $body:expr) => {
        |$fs: &FileSystem<Pool>| -> futures::future::BoxFuture<'_, Result<(), crate::fs::FileSystemError>> {
            Box::pin(async move {
                // The op alone, under its tx: not the commit after it (a
                // failed commit leaves what it took, by design).
                let mut tx = $fs.begin_tx().await;
                let $tx = &mut tx;
                let r = $body.map(|_| ());
                $tx.check(r).await
            })
        }
    };
}

/// `write_file` without its commit.
async fn s5_write(
    fs: &FileSystem<Pool>,
    tx: &mut crate::tx::Tx,
    path: &str,
    data: &[u8],
) -> Result<(), crate::fs::FileSystemError> {
    let l = fs.lookup(path.as_ref()).await?;
    let current = fs.current_file(&l).await?;
    let blksz = fs.file_blksz();
    let tree = {
        let mut allocs = tx.allocs();
        fs.write_tree(blksz as usize, data, &mut allocs.ptrs)
            .await?
    };
    fs.set_file_contents(tx, l, current, blksz, data.len() as u64, tree)
        .await
        .map(|_| ())
}

/// S5a: a new file (data, its dnode, then its directory).
#[tokio::test]
async fn s5a_a_failed_create_leaves_nothing() {
    let (fp, fs) = s5_pool().await;
    s5_every_failure_point(
        &fp,
        &fs,
        s5_op!(|fs, tx| s5_write(fs, tx, "/d/new", &pattern(9000, 4)).await),
    )
    .await;
}

/// S5a: unlinking a file (its dnode and tree go, then the directory).
#[tokio::test]
async fn s5a_a_failed_delete_leaves_the_file() {
    let (fp, fs) = s5_pool().await;
    s5_every_failure_point(
        &fp,
        &fs,
        s5_op!(|fs, tx| fs.delete_locked(tx, "/d/b".as_ref()).await),
    )
    .await;
}

/// S5a: a rename over an existing file (two directories and the target).
#[tokio::test]
async fn s5a_a_failed_rename_over_a_target_leaves_both() {
    let (fp, fs) = s5_pool().await;
    s5_every_failure_point(
        &fp,
        &fs,
        s5_op!(|fs, tx| fs.rename_locked(tx, "/a".as_ref(), "/d/b".as_ref()).await),
    )
    .await;
}

/// S5a: a directory moved under another.
#[tokio::test]
async fn s5a_a_failed_directory_move_leaves_it() {
    let (fp, fs) = s5_pool().await;
    s5_every_failure_point(
        &fp,
        &fs,
        s5_op!(|fs, tx| fs.rename_locked(tx, "/d/e".as_ref(), "/e2".as_ref()).await),
    )
    .await;
}

/// S5b: deleting a dataset.
#[tokio::test]
async fn s5b_a_failed_dataset_delete_leaves_it() {
    let (fp, fs) = s5_pool().await;
    s5_every_failure_point(
        &fp,
        &fs,
        s5_op!(|fs, tx| fs.delete_dataset_locked(tx, "vol").await),
    )
    .await;
}

/// S5a: an op given up on partway (its future dropped while a write is
/// stuck) is undone before its tx lets the commit lock go, so no commit
/// takes the half of it already done.
#[tokio::test]
async fn s5a_a_cancelled_op_leaves_nothing() {
    let (fp, fs) = s5_pool().await;
    let before = s5_seen(&fs).await;
    for d in &fp.disks {
        d.hold_writes(true);
    }
    let op = async {
        let mut tx = fs.begin_tx().await;
        // The dnode goes, then the directory write sticks.
        fs.delete_locked(&mut tx, "/d/b".as_ref()).await
    };
    let r = tokio::time::timeout(std::time::Duration::from_millis(200), op).await;
    assert!(r.is_err(), "the op wasn't held up: {:?}", r);
    assert!(fp.disks.iter().any(|d| d.held() > 0));
    fp.heal();
    for d in &fp.disks {
        d.release_all();
    }
    // The undo runs in a task holding the lock: once it's free, it's done.
    drop(fs.begin_tx().await);
    assert_eq!(s5_seen(&fs).await, before);
}

/// S5a: a failed op undoes only itself. An op applied before it in the
/// same open TXG (the committer stages several writers' commits so, for
/// one group commit) stays for that commit.
#[tokio::test]
async fn s5a_a_failed_op_keeps_the_ops_applied_before_it() {
    let (fp, fs) = s5_pool().await;
    let mut tx = fs.begin_tx().await;
    let r = s5_write(&fs, &mut tx, "/p", &pattern(5000, 6)).await;
    tx.check(r).await.unwrap();
    let mut failed = false;
    for n in 0..50 {
        for d in &fp.disks {
            d.fail_writes_after(n, FaultError::Io);
        }
        let r = fs.delete_locked(&mut tx, "/d/b".as_ref()).await;
        let r = tx.check(r).await;
        fp.heal();
        if r.is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "the delete never failed");
    fs.commit_locked(&mut tx).await.unwrap();
    drop(tx);
    let fs = fp.load().await;
    assert_eq!(fs.read_file("/p".as_ref()).await.unwrap(), pattern(5000, 6));
    assert_eq!(
        fs.read_file("/d/b".as_ref()).await.unwrap(),
        pattern(20_000, 2)
    );
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// S5a: a rollback given up on partway (its future dropped while waiting
/// for a metaslab lock) leaves what it hadn't undone yet for the tx's drop,
/// which finishes it before the lock goes.
#[tokio::test]
async fn s5a_a_cancelled_rollback_is_finished_by_the_drop() {
    let (fp, fs) = s5_pool().await;
    let before = s5_seen(&fs).await;
    let mut tx = fs.begin_tx().await;
    for d in &fp.disks {
        d.fail_writes(Some(FaultError::Io));
    }
    // The dnode goes and its tree is freed, then the directory write fails.
    let r = fs.delete_locked(&mut tx, "/d/b".as_ref()).await;
    assert!(r.is_err());
    fp.heal();
    // Undoing the frees needs these.
    let mut held = Vec::new();
    for ms in fs.metaslabs.iter() {
        held.push(ms.clone().write_owned().await);
    }
    let rollback = tokio::time::timeout(std::time::Duration::from_millis(100), tx.check(r)).await;
    assert!(rollback.is_err(), "the rollback wasn't held up");
    drop(held);
    drop(tx);
    drop(fs.begin_tx().await);
    assert_eq!(s5_seen(&fs).await, before);
}

// ---- D4: what a resilver may clear ----

/// D4: a miss reported while a resilver walks (a writer's commit marking
/// the disk, for a write the walk's snapshot doesn't have) keeps the disk
/// degraded: the resilver didn't rebuild that write.
#[tokio::test]
async fn d4_a_miss_reported_during_the_scan_keeps_the_disk_degraded() {
    let fp = FaultPool::mirror(2).await;
    let fs = Arc::new(fp.format().await);
    fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    fs.sync().await.unwrap();
    // Disk 1 misses a commit's writes: degraded, with records to rebuild.
    fp.disk(1).fail_writes(Some(FaultError::Io));
    fs.block_write("a", 0, &pattern(MB, 1)).await.unwrap();
    fs.sync().await.unwrap();
    fp.heal();
    let since = fs.degraded_disks()[1].expect("disk 1 degraded");

    // The resilver's repair writes to disk 1 hang: it's mid-walk. (Only
    // writes to the data area: its first commit's labels go through.)
    let data_start = fs.layout.data_start + DISK_LABEL_BLOCKS;
    fp.disk(1).target_blocks(Some(data_start..u64::MAX));
    fp.disk(1).hold_writes(true);
    let scan = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.resilver().await })
    };
    while fp.disk(1).held() == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // A writer's commit reports a miss on disk 1 meanwhile.
    {
        let _tx = fs.begin_tx().await;
        fs.mark_degraded(1, since);
    }
    fp.disk(1).hold_writes(false);
    fp.disk(1).release_all();
    let report = scan.await.unwrap().unwrap();
    assert!(report.repaired() > 0, "{}", report);
    assert!(!report.cleared.contains(&1), "{}", report);
    assert!(fs.degraded_disks()[1].is_some());
    // A resilver with nothing new reported clears it.
    let report = fs.resilver().await.unwrap();
    assert!(report.cleared.contains(&1), "{}", report);
}

/// What raw block `b` of `disk`'s file holds, past the fault layer.
fn raw_block(fp: &FaultPool, disk: usize, b: u64) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(fp.disk(disk).path()).unwrap();
    let mut buf = vec![0u8; BLOCK_SIZE];
    f.read_exact_at(&mut buf, b * BLOCK_SIZE as u64).unwrap();
    buf
}

/// D4: intent log records are outside the tree, so a scan has to check
/// them itself. A disk that *counts* a miss never leaves a live record
/// short (the flush commits instead, folding it into the tree), but one
/// lost silently (a write cache dropped, say) is on one copy of two until
/// replay. A scrub finds and rewrites it.
#[tokio::test]
async fn d4_a_scrub_rewrites_a_live_log_record_a_disk_lost() {
    let s = zil_setup().await;
    let w = ZilWriter::start(&s).await;
    w.fs.block_write("a", 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap(); // commits: registers the log
    let c = w.cursor();
    // Disk 0 loses the next record's blocks, without a word.
    let start = c.region.start + c.used + DISK_LABEL_BLOCKS;
    s.fp.disk(0)
        .target_blocks(Some(start..start + c.region.len - c.used));
    s.fp.disk(0).drop_writes(true);
    let before = txg(&s).await;
    w.fs.block_write("a", 0, &pattern(50_000, 2)).await.unwrap();
    w.fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "acked from the log");
    s.fp.heal();
    let used = w.cursor().used;
    assert!(used > c.used, "a record was logged");
    let record = |disk| {
        (c.used..used)
            .map(|b| raw_block(&s.fp, disk, c.region.start + b + DISK_LABEL_BLOCKS))
            .collect::<Vec<_>>()
    };
    assert_ne!(record(0), record(1), "disk 0 lost the record");

    let report = s.committer.fs().scrub(true).await.unwrap();
    assert!(report.log_records > 0, "{}", report);
    assert!(report.repaired() > 0, "{}", report);
    assert_eq!(record(0), record(1), "the record is still on one copy");
    w.die(&s).await;
}

/// D4: a writer's misses count as reported only once a commit carrying
/// them is accepted. A refused one (here: its volume lease was lost) is
/// never applied, so the next commit must report them again.
#[tokio::test]
async fn d4_misses_a_refused_commit_carried_are_reported_again() {
    let s = zil_setup().await;
    let w = ZilWriter::start(&s).await;
    w.fs.block_write("a", 0, &pattern(100_000, 1))
        .await
        .unwrap();
    w.fs.sync().await.unwrap();
    // The volume's lease moves on; then the writer's next sync, whose
    // writes disk 1 fails, is refused before anything is applied.
    release(&s.auth, &dataset_resource("a"), "o1", w.ds_epoch).await;
    lease(&s.auth, &dataset_resource("a"), "o2").await;
    s.fp.disk(1).fail_writes(Some(FaultError::Io));
    w.fs.block_write("a", 0, &pattern(100_000, 2))
        .await
        .unwrap();
    let r = w.fs.sync().await;
    s.fp.heal();
    assert!(r.is_err(), "the sync was refused");
    let fs = s.committer.fs();
    assert!(fs.degraded_disks()[1].is_none(), "refused: nothing applied");
    // Its next accepted commit, whatever it is, reports the miss.
    w.fs.mkdir("/x".as_ref()).await.unwrap();
    assert!(
        fs.degraded_disks()[1].is_some(),
        "the miss the refused commit carried was lost"
    );
}

/// D3: a writable rewind while one mirror member is offline. That member
/// comes back still holding the discarded TXGs' uberblocks. A plain import
/// must stay on the rewound lineage, and a repairing scrub must settle the
/// ring on the rewind's tombstones, not copy the discarded uberblocks back:
/// afterwards the returned member alone opens the rewound state.
#[tokio::test]
async fn d3_a_member_that_missed_a_rewind_doesnt_bring_it_back() {
    d3_missed_rewind(FaultPool::mirror(2).await).await;
    d3_missed_rewind(FaultPool::raidz(1, 3).await).await;
}

async fn d3_missed_rewind(fp: FaultPool) {
    use crate::fs::{LoadOptions, Rewind};
    use std::path::Path;
    let fs = fp.format().await;
    let a1 = pattern(200_000, 1);
    fs.write_file(Path::new("/a"), &a1).await.unwrap();
    let good = fs.uberblock().await.unwrap().txg;
    fs.write_file(Path::new("/a"), &pattern(200_000, 2))
        .await
        .unwrap();
    fs.write_file(Path::new("/b"), &pattern(50_000, 2))
        .await
        .unwrap();
    let newest = fs.uberblock().await.unwrap().txg;
    assert!(newest > good);
    drop(fs);

    fp.disk(1).set_offline(true);
    let fs = FileSystem::load_with(
        fp.dev(),
        LoadOptions {
            read_only: false,
            rewind: Rewind::Txg(good),
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    drop(fs);
    fp.heal();

    let fs = fp.load().await;
    assert!(fs.uberblock().await.unwrap().txg > newest);
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    assert!(fs.read_file(Path::new("/b")).await.is_err());
    let report = fs.scrub(true).await.unwrap();
    assert!(report.label_unrecoverable.is_empty(), "{}", report);
    drop(fs);

    // Disk 1 alone: its ring now holds the rewind, not what it discarded.
    fp.disk(0).set_offline(true);
    let fs = fp.load().await;
    assert!(fs.uberblock().await.unwrap().txg > newest);
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    assert!(fs.read_file(Path::new("/b")).await.is_err());
    let ring: Vec<_> = fs.uberblock_ring().await.iter().map(|u| u.txg).collect();
    assert!(
        ring.iter().all(|&t| t <= good || t > newest),
        "discarded TXGs importable again: {:?}",
        ring
    );
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// A resilver cancelled mid-walk lets go of the deferred frees it held
/// back: otherwise no freed space is ever reused again.
#[tokio::test]
async fn a_cancelled_resilver_releases_its_free_hold() {
    use std::sync::atomic::Ordering;
    let fp = FaultPool::mirror(2).await;
    let fs = fp.format().await;
    fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    fs.sync().await.unwrap();
    fp.disk(1).fail_writes(Some(FaultError::Io));
    fs.block_write("a", 0, &pattern(MB, 1)).await.unwrap();
    fs.sync().await.unwrap();
    fp.heal();

    let data_start = fs.layout.data_start + DISK_LABEL_BLOCKS;
    fp.disk(1).target_blocks(Some(data_start..u64::MAX));
    fp.disk(1).hold_writes(true);
    {
        let scan = fs.resilver();
        tokio::pin!(scan);
        tokio::select! {
            _ = &mut scan => panic!("the resilver finished with its repairs held"),
            _ = async {
                while fp.disk(1).held() == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            } => {}
        }
        assert_eq!(fs.scan_hold.load(Ordering::SeqCst), 1);
    }
    assert_eq!(fs.scan_hold.load(Ordering::SeqCst), 0, "the hold leaked");
    fp.disk(1).hold_writes(false);
    fp.disk(1).release_all();
    let report = fs.resilver().await.unwrap();
    assert!(report.cleared.contains(&1), "{}", report);
}

/// D3/D4: a degraded member's copies of the label regions don't vote, and
/// are rewritten to match. That includes slots never written: leftovers a
/// stale member holds in one are zeroed, not passed over as blank (mirror)
/// or reported unrecoverable, keeping the disk degraded forever (raidz).
#[tokio::test]
async fn a_stale_members_leftovers_in_a_blank_label_slot_are_zeroed() {
    use std::os::unix::fs::FileExt;
    for (fp, width) in [
        (FaultPool::mirror(2).await, 1),
        (FaultPool::raidz(1, 3).await, 2),
    ] {
        let fs = fp.format().await;
        fs.create_block("a", 4 * MB as u64, fs.default_volblocksize())
            .await
            .unwrap();
        fs.sync().await.unwrap();
        fp.disk(1).fail_writes(Some(FaultError::Io));
        fs.block_write("a", 0, &pattern(MB, 1)).await.unwrap();
        fs.sync().await.unwrap();
        fp.heal();
        assert!(fs.degraded_disks()[1].is_some());

        // The last label block every member has as zeros: never written.
        let first = DISK_LABEL_BLOCKS + fs.layout.spacemap_start / width;
        let end = DISK_LABEL_BLOCKS + fs.layout.data_start / width;
        let zero = vec![0u8; BLOCK_SIZE];
        let blank = (first..end)
            .rev()
            .find(|&b| (0..fp.disks.len()).all(|d| raw_block(&fp, d, b) == zero))
            .expect("a never-written label block");
        std::fs::OpenOptions::new()
            .write(true)
            .open(fp.disk(1).path())
            .unwrap()
            .write_all_at(&[0x5Au8; BLOCK_SIZE], blank * BLOCK_SIZE as u64)
            .unwrap();

        let report = fs.resilver().await.unwrap();
        assert!(report.label_unrecoverable.is_empty(), "{}", report);
        assert!(report.cleared.contains(&1), "{}", report);
        assert_eq!(raw_block(&fp, 1, blank), zero, "leftovers kept");
    }
}
