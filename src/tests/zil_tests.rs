//! Volume intent logs: flushes acknowledged without the committer, replayed
//! after the writer goes away.

use super::*;
use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority, volume_resource};
use crate::test_util::TestPool;
use crate::vdev::Pool;
use std::sync::atomic::{AtomicBool, Ordering};

const MB: usize = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn lease(auth: &LocalAuthority, resource: &str, owner: &str) -> u64 {
    let op = LeaseOp::Acquire {
        resource: resource.into(),
        owner: owner.into(),
        desc: format!("test {}", owner),
        ttl_ms: 600_000,
    };
    match auth.propose(op).await.unwrap() {
        Response::Granted { epoch, .. } => epoch,
        r => panic!("{} not granted: {:?}", resource, r),
    }
}

async fn release(auth: &LocalAuthority, resource: &str, owner: &str, epoch: u64) {
    let op = LeaseOp::Release {
        resource: resource.into(),
        owner: owner.into(),
        epoch,
    };
    assert_eq!(auth.propose(op).await.unwrap(), Response::Released);
}

struct Setup {
    pool: TestPool,
    auth: Arc<LocalAuthority>,
    committer: Arc<Committer<Pool>>,
}

async fn setup() -> Setup {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("a", 8 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    drop(fs);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, POOL, "c").await;
    let fs = Arc::new(pool.load().await);
    let committer =
        Arc::new(Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await);
    Setup {
        pool,
        auth,
        committer,
    }
}

struct Writer {
    fs: FileSystem<Pool>,
    id: String,
    epoch: u64,
    vol_owner: String,
    vol_epoch: u64,
}

impl Writer {
    async fn die(self, s: &Setup) {
        // No detach: the process is gone, its leases lapse.
        drop(self.fs);
        release(
            &s.auth,
            &volume_resource("a"),
            &self.vol_owner,
            self.vol_epoch,
        )
        .await;
        release(
            &s.auth,
            &format!("writer/{}", self.id),
            &self.id,
            self.epoch,
        )
        .await;
    }
}

/// A writer running volume "a" (`fresh`: its lease check).
async fn writer(s: &Setup, id: &str, fresh: Option<crate::writer::LeaseCheck>) -> Writer {
    let resource = format!("writer/{}", id);
    let epoch = lease(&s.auth, &resource, id).await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: resource,
        epoch,
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    let vol_owner = format!("o{}", id);
    let vol_epoch = lease(&s.auth, &volume_resource("a"), &vol_owner).await;
    fs.attach_volume_leased("a", &vol_owner, vol_epoch, None, fresh)
        .await
        .unwrap();
    Writer {
        fs,
        id: id.into(),
        epoch,
        vol_owner,
        vol_epoch,
    }
}

async fn txg(s: &Setup) -> u64 {
    s.committer.fs.uberblock().await.unwrap().txg
}

async fn read(fs: &FileSystem<Pool>, offset: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = fs.block_read("a", offset, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

async fn cursor(fs: &FileSystem<Pool>) -> Option<LogCursor> {
    fs.open_volume("a")
        .await
        .unwrap()
        .cursor
        .lock()
        .unwrap()
        .clone()
}

async fn assert_clean(s: &Setup) {
    let fs = s.pool.load().await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn a_flush_is_acknowledged_from_the_log_and_replayed_after_a_crash() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;

    // The first flush commits (no log yet) and registers a region.
    w.fs.block_write("a", 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let c = cursor(&w.fs)
        .await
        .expect("a log region after the first commit");
    assert_eq!((c.used, c.seq), (0, 0));

    // Later flushes only append to the log: no commit.
    let before = txg(&s).await;
    w.fs.block_write("a", 100_000, &pattern(200_000, 2))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_zero("a", 1_000_000, 500_000).await.unwrap();
    w.fs.block_write("a", 2_000_000, &pattern(70_001, 3))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "flushes didn't commit");
    assert_eq!(cursor(&w.fs).await.unwrap().seq, 2);

    let mut want = vec![0u8; 3 * MB];
    want[..300_000].copy_from_slice(&pattern(300_000, 1));
    want[100_000..300_000].copy_from_slice(&pattern(200_000, 2));
    want[2_000_000..2_070_001].copy_from_slice(&pattern(70_001, 3));
    assert_eq!(read(&w.fs, 0, want.len()).await, want);

    // The writer dies. What it acknowledged isn't in the tree...
    w.die(&s).await;
    let fresh = s.pool.load().await;
    assert_ne!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    // ...until the committer notices and replays the log.
    s.committer.reclaim().await;
    assert!(s.committer.fs.volume_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test]
async fn the_next_holder_claims_the_volume_and_sees_the_log() {
    let s = setup().await;
    let w1 = writer(&s, "1", None).await;
    w1.fs
        .block_write("a", 0, &pattern(100_000, 1))
        .await
        .unwrap();
    w1.fs.flush().await.unwrap(); // commits, registers the log
    w1.fs
        .block_write("a", 50_000, &pattern(400_000, 2))
        .await
        .unwrap();
    w1.fs.flush().await.unwrap(); // logged only
    w1.die(&s).await;

    // A new writer takes the volume over before the committer's reclaim
    // loop has run: attaching replays the log first.
    let w2 = writer(&s, "2", None).await;
    let mut want = pattern(100_000, 1);
    want.resize(450_000, 0);
    want[50_000..450_000].copy_from_slice(&pattern(400_000, 2));
    assert_eq!(read(&w2.fs, 0, want.len()).await, want);

    // It carries on with a log of its own, and a clean detach drops it.
    w2.fs.block_write("a", 0, &pattern(1000, 3)).await.unwrap();
    w2.fs.flush().await.unwrap();
    w2.fs
        .block_write("a", 4000, &pattern(1000, 4))
        .await
        .unwrap();
    w2.fs.flush().await.unwrap();
    w2.fs.detach_volume("a").await.unwrap();
    assert!(s.committer.fs.volume_logs().await.unwrap().is_empty());
    want[..1000].copy_from_slice(&pattern(1000, 3));
    want[4000..5000].copy_from_slice(&pattern(1000, 4));
    w2.fs.return_metaslabs().await;
    drop(w2);
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test]
async fn a_commit_rotates_the_log_and_the_space_adds_up() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write("a", 0, &pattern(50_000, 1)).await.unwrap();
    w.fs.flush().await.unwrap();
    let first = cursor(&w.fs).await.unwrap().region;
    w.fs.block_write("a", 0, &pattern(50_000, 2)).await.unwrap();
    w.fs.flush().await.unwrap();

    // The regular commit folds the log into the tree and starts a new one.
    let before = txg(&s).await;
    w.fs.sync().await.unwrap();
    assert!(txg(&s).await > before);
    let second = cursor(&w.fs).await.unwrap();
    assert_ne!(second.region, first);
    assert_eq!(second.seq, 0);
    // A live log is in use: no leak, nothing in it handed out twice.
    assert_clean(&s).await;

    w.fs.detach_volume("a").await.unwrap();
    assert!(cursor(&w.fs).await.is_none());
    assert!(s.committer.fs.volume_logs().await.unwrap().is_empty());
    w.fs.return_metaslabs().await;
    assert_clean(&s).await;
}

#[tokio::test]
async fn a_full_log_falls_back_to_a_commit() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write("a", 0, &pattern(1000, 1)).await.unwrap();
    w.fs.flush().await.unwrap();
    let region = cursor(&w.fs).await.unwrap().region;

    // More than the region holds, in one flush.
    let big = pattern(region.len as usize * crate::disk::BLOCK_SIZE + 1, 2);
    let before = txg(&s).await;
    w.fs.block_write("a", 0, &big).await.unwrap();
    w.fs.flush().await.unwrap();
    assert!(txg(&s).await > before, "committed instead");
    assert_eq!(cursor(&w.fs).await.unwrap().seq, 0, "with a fresh log");
    assert_eq!(read(&w.fs, 0, big.len()).await, big);
}

#[tokio::test]
async fn no_acknowledgement_without_a_fresh_lease() {
    let s = setup().await;
    let ok = Arc::new(AtomicBool::new(true));
    let flag = ok.clone();
    let w = writer(&s, "1", Some(Arc::new(move || flag.load(Ordering::SeqCst)))).await;
    w.fs.block_write("a", 0, &pattern(1000, 1)).await.unwrap();
    w.fs.flush().await.unwrap();

    // The lease can't be confirmed (renewals failing): flushes aren't acked.
    ok.store(false, Ordering::SeqCst);
    w.fs.block_write("a", 0, &pattern(1000, 2)).await.unwrap();
    assert!(matches!(
        w.fs.flush().await,
        Err(FileSystemError::Unavailable(_))
    ));
    // A retry has nothing new to log, but still isn't acknowledged.
    assert!(matches!(
        w.fs.flush().await,
        Err(FileSystemError::Unavailable(_))
    ));

    // Once the lease is confirmed again, the retry logs and acknowledges it.
    ok.store(true, Ordering::SeqCst);
    let seq = cursor(&w.fs).await.unwrap().seq;
    w.fs.flush().await.unwrap();
    assert_eq!(cursor(&w.fs).await.unwrap().seq, seq + 1);
    w.die(&s).await;
    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, 1000).await, pattern(1000, 2));
}

/// Make the `vol/a` lease look as if it expired `ago_ms` ago.
fn expire_volume_lease(s: &Setup, ago_ms: u64) {
    let mut state = s.auth.0.lock().unwrap();
    let l = state.leases.get_mut(&volume_resource("a")).unwrap();
    l.expires_ms = crate::cluster::now_ms() - ago_ms;
}

#[tokio::test]
async fn a_lapsed_lease_is_taken_over_before_its_log_is_replayed() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write("a", 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_write("a", 0, &pattern(1000, 2)).await.unwrap();
    w.fs.flush().await.unwrap();
    let mut want = pattern(300_000, 1);
    want[..1000].copy_from_slice(&pattern(1000, 2));

    // Only just expired: its holder may still renew it. Leave it be.
    expire_volume_lease(&s, 1000);
    s.committer.reclaim().await;
    assert_eq!(s.committer.fs.volume_logs().await.unwrap().len(), 1);

    // Lapsed for good (the writer is partitioned away, not dead).
    expire_volume_lease(&s, 60_000);
    s.committer.reclaim().await;
    assert!(s.committer.fs.volume_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);

    // The partition heals: the old holder can't renew its lease and go on
    // acknowledging flushes into a log nobody will replay.
    let renew = LeaseOp::Renew {
        resource: volume_resource("a"),
        owner: w.vol_owner.clone(),
        epoch: w.vol_epoch,
        ttl_ms: 10_000,
    };
    assert_eq!(s.auth.propose(renew).await.unwrap(), Response::Lost);
    // And the committer let go of it again: the next node can have it.
    let next = lease(&s.auth, &volume_resource("a"), "o2").await;
    assert!(next > w.vol_epoch);
    assert_clean(&s).await;
}

#[tokio::test]
async fn replay_stops_at_the_end_of_the_log() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write("a", 0, &pattern(1000, 1)).await.unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_write("a", 0, &pattern(1000, 2)).await.unwrap();
    w.fs.flush().await.unwrap();
    // Leftovers past the last record: a record from another log (wrong
    // guid), then garbage.
    let c = cursor(&w.fs).await.unwrap();
    let foreign = encode(
        c.region.guid ^ 1,
        c.seq,
        &[Entry::Data(0, pattern(48 * 1024, 9))],
    )
    .unwrap();
    w.fs.write_record_blocks(c.region.start + c.used, &foreign)
        .await
        .unwrap();
    let after = c.used + w.fs.align_blocks(foreign.len().div_ceil(BLOCK_SIZE) as u64);
    w.fs.write_record_blocks(c.region.start + after, &pattern(8192, 7))
        .await
        .unwrap();
    w.fs.dev.flush().await.unwrap();
    w.die(&s).await;

    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, 1000).await, pattern(1000, 2));
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test]
async fn replay_rebuilds_log_blocks_a_disk_missed() {
    let s = setup().await;
    // This writer can't reach disk 1: its log records miss that shard, and
    // the disk still holds whatever was there before.
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: resource,
        epoch,
    });
    let fs = FileSystem::load_writer(s.pool.dev_without(&[1]).await, sink)
        .await
        .unwrap();
    let vol_epoch = lease(&s.auth, &volume_resource("a"), "o1").await;
    fs.attach_volume("a", "o1", vol_epoch).await.unwrap();
    fs.block_write("a", 0, &pattern(10_000, 1)).await.unwrap();
    fs.flush().await.unwrap();
    let data = pattern(300_000, 2);
    fs.block_write("a", 0, &data).await.unwrap();
    fs.flush().await.unwrap();
    assert_eq!(cursor(&fs).await.unwrap().seq, 1, "logged");
    drop(fs);
    release(&s.auth, &volume_resource("a"), "o1", vol_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;

    // The committer sees every disk, disk 1 stale: parity rebuilds it.
    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, data.len()).await, data);
}

/// Holds every volume commit until released (a committer failing over).
struct Gate {
    inner: DirectSink<Pool>,
    open: tokio::sync::watch::Receiver<bool>,
    waiting: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::writer::CommitSink for Gate {
    fn writer(&self) -> (String, u64) {
        self.inner.writer()
    }
    async fn lease_metaslab(
        &self,
        want: u64,
    ) -> Result<crate::writer::LentMetaslab, crate::writer::CommitError> {
        self.inner.lease_metaslab(want).await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), crate::writer::CommitError> {
        self.inner.return_metaslab(idx).await
    }
    async fn commit(
        &self,
        c: crate::writer::VolumeCommit,
    ) -> Result<crate::writer::Committed, crate::writer::CommitError> {
        let mut open = self.open.clone();
        if !*open.borrow() {
            self.waiting.fetch_add(1, Ordering::SeqCst);
            let _ = open.wait_for(|o| *o).await;
        }
        self.inner.commit(c).await
    }
    async fn commit_fs(
        &self,
        c: crate::writer::FsCommit,
    ) -> Result<crate::writer::FsCommitted, crate::writer::CommitError> {
        self.inner.commit_fs(c).await
    }
    async fn claim_volume(
        &self,
        volume: &str,
        vol_owner: &str,
        vol_epoch: u64,
    ) -> Result<(), crate::writer::CommitError> {
        self.inner.claim_volume(volume, vol_owner, vol_epoch).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushes_carry_on_while_a_commit_waits_for_the_committer() {
    let s = setup().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let (open_tx, open_rx) = tokio::sync::watch::channel(true);
    let sink = Arc::new(Gate {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: resource,
            epoch,
        },
        open: open_rx,
        waiting: Default::default(),
    });
    let fs = Arc::new(
        FileSystem::load_writer(s.pool.dev().await, sink.clone())
            .await
            .unwrap(),
    );
    let vol_epoch = lease(&s.auth, &volume_resource("a"), "o1").await;
    fs.attach_volume("a", "o1", vol_epoch).await.unwrap();
    fs.block_write("a", 0, &pattern(100_000, 1)).await.unwrap();
    fs.flush().await.unwrap(); // commits: log registered

    // B is written and logged, then a commit starts and gets stuck.
    fs.block_write("a", 0, &pattern(50_000, 2)).await.unwrap();
    fs.flush().await.unwrap();
    open_tx.send_replace(false);
    let folding = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.sync().await })
    };
    while sink.waiting.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Meanwhile C is written, after the commit took its records. Flushes
    // don't wait for the commit.
    fs.block_write("a", 200_000, &pattern(60_000, 3))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), fs.flush())
        .await
        .expect("a flush doesn't wait for the stuck commit")
        .unwrap();

    // The committer comes back: the commit lands (B in the tree, the log
    // now [current from the cut, new region]).
    open_tx.send_replace(true);
    folding.await.unwrap().unwrap();
    assert_eq!(
        s.committer.fs.volume_logs().await.unwrap()[0]
            .1
            .regions
            .len(),
        2
    );

    // The writer dies before committing C: replay finds it in the old
    // region's tail.
    drop(fs);
    release(&s.auth, &volume_resource("a"), "o1", vol_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    let mut want = pattern(100_000, 1);
    want[..50_000].copy_from_slice(&pattern(50_000, 2));
    want.resize(260_000, 0);
    want[200_000..].copy_from_slice(&pattern(60_000, 3));
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test]
async fn a_faulted_pool_keeps_the_log_for_later() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write("a", 0, &pattern(1000, 1)).await.unwrap();
    w.fs.flush().await.unwrap();
    let data = pattern(200_000, 2);
    w.fs.block_write("a", 0, &data).await.unwrap();
    w.fs.flush().await.unwrap();
    w.die(&s).await;

    // More disks need a resilver than raidz2 can cover: unreadable records
    // would look like the end of the log, so nothing is replayed (or dropped).
    let disks = s.committer.fs.disk_count();
    for d in 0..3 {
        s.committer.fs.mark_degraded(d, 1);
    }
    s.committer.reclaim().await;
    assert_eq!(s.committer.fs.volume_logs().await.unwrap().len(), 1);

    // Resilvered: now it's replayed.
    *s.committer.fs.degraded.lock().unwrap() = vec![None; disks];
    s.committer.reclaim().await;
    assert!(s.committer.fs.volume_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, data.len()).await, data);
}

#[tokio::test]
async fn a_writer_heals_what_a_disk_missed_in_its_lent_space() {
    let s = setup().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = || {
        Arc::new(DirectSink {
            committer: s.committer.clone(),
            writer: resource.clone(),
            epoch,
        })
    };
    // Written and committed while disk 1 was unreachable: its shards there
    // are stale, and the commit marks it degraded.
    let a = FileSystem::load_writer(s.pool.dev_without(&[1]).await, sink())
        .await
        .unwrap();
    let vol_epoch = lease(&s.auth, &volume_resource("a"), "o1").await;
    a.attach_volume("a", "o1", vol_epoch).await.unwrap();
    a.block_write("a", 0, &pattern(400_000, 1)).await.unwrap();
    a.sync().await.unwrap();
    assert!(s.committer.fs.degraded_disks()[1].is_some());
    let lent = a.writer.as_ref().unwrap().lent.lock().unwrap().clone();
    assert!(!lent.is_empty());

    // The same writer, once it reaches disk 1 again, repairs its lent space.
    let b = FileSystem::load_writer(s.pool.dev().await, sink())
        .await
        .unwrap();
    b.writer.as_ref().unwrap().lent.lock().unwrap().extend(lent);
    let report = b.heal_lent().await.unwrap().expect("a heal pass");
    assert!(report.repaired() > 0, "{}", report);
    assert!(report.is_clean(), "{}", report);

    // Nothing left for the committer to find.
    let scrub = s.committer.fs.scrub(false).await.unwrap();
    assert_eq!(scrub.mismatched(), 0, "{}", scrub);
}

#[tokio::test]
async fn a_failed_disk_flush_leaves_no_gap_before_later_records() {
    use crate::test_util::FaultyDev;
    let s = setup().await;
    let epoch = lease(&s.auth, "writer/1", "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: "writer/1".into(),
        epoch,
    });
    let (dev, switch) = FaultyDev::new(s.pool.dev().await);
    let fs = FileSystem::load_writer(dev, sink).await.unwrap();
    let vol_epoch = lease(&s.auth, &volume_resource("a"), "o1").await;
    fs.attach_volume_leased("a", "o1", vol_epoch, None, None)
        .await
        .unwrap();
    let cursor = |fs: &FileSystem<FaultyDev<Pool>>| {
        let vol = fs.volumes.lock().unwrap().get("a").cloned().unwrap();
        vol.cursor.lock().unwrap().clone()
    };

    fs.block_write("a", 0, &pattern(300_000, 1)).await.unwrap();
    fs.flush().await.unwrap();

    // The disk flush fails: the record just written may not have made it.
    fs.block_write("a", 0, &pattern(1000, 2)).await.unwrap();
    let c = cursor(&fs).expect("a log region");
    switch.fail_flushes(true);
    assert!(fs.flush().await.is_err());
    switch.fail_flushes(false);
    // Say it didn't.
    fs.write_record_blocks(c.region.start + c.used, &vec![0u8; BLOCK_SIZE])
        .await
        .unwrap();

    // A later flush is acknowledged...
    fs.block_write("a", 1000, &pattern(1000, 3)).await.unwrap();
    fs.flush().await.unwrap();

    // ...and survives the writer dying, gap or not.
    drop(fs);
    release(&s.auth, &volume_resource("a"), "o1", vol_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    s.committer.reclaim().await;
    let mut want = pattern(300_000, 1);
    want[..1000].copy_from_slice(&pattern(1000, 2));
    want[1000..2000].copy_from_slice(&pattern(1000, 3));
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    assert_clean(&s).await;
}
