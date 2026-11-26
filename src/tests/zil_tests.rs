//! Dataset intent logs: flushes acknowledged without the committer, replayed
//! after the writer goes away.

use crate::cluster::LeaseOp;
use crate::cluster::state::{POOL, Response};
use crate::committer::{Authority, Committer, DirectSink, LocalAuthority, dataset_resource};
use crate::coords::*;
use crate::disk::{BLOCK_SIZE, PoolDevice};
use crate::fs::{FileSystem, FileSystemError};
use crate::test_util::TestPool;
use crate::test_util::dsn;
use crate::types::Txg;
use crate::types::{Epoch, Holding, LeaseRef, MetaslabIdx};
use crate::vdev::Pool;
use crate::zil::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const MB: usize = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

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

async fn release(
    auth: &LocalAuthority,
    resource: impl std::fmt::Display,
    owner: &str,
    epoch: Epoch,
) {
    let resource = crate::types::res(resource);
    let op = LeaseOp::Release {
        resource: resource.clone(),
        holder: Holding::new(owner, epoch),
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
    fs.create_block(&dsn("a"), 8 * MB as u64, fs.default_volblocksize())
        .await
        .unwrap();
    drop(fs);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, &POOL, "c").await;
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
    epoch: Epoch,
    ds_owner: String,
    ds_epoch: Epoch,
}

impl Writer {
    async fn die(self, s: &Setup) {
        // No detach: the process is gone, its leases lapse.
        drop(self.fs);
        release(
            &s.auth,
            &dataset_resource("a"),
            &self.ds_owner,
            self.ds_epoch,
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
        writer: LeaseRef::new(crate::types::res(&resource), epoch),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    let ds_owner = format!("o{}", id);
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), &ds_owner).await;
    fs.attach_dataset_leased(
        &dsn("a"),
        Holding::new(ds_owner.clone(), ds_epoch),
        None,
        fresh,
    )
    .await
    .unwrap();
    Writer {
        fs,
        id: id.into(),
        epoch,
        ds_owner,
        ds_epoch,
    }
}

async fn txg(s: &Setup) -> u64 {
    (s.committer.fs().uberblock().await.unwrap().txg).get()
}

async fn read(fs: &FileSystem<Pool>, offset: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = fs.block_read(&dsn("a"), offset, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

async fn cursor(fs: &FileSystem<Pool>) -> Option<LogCursor> {
    fs.open_dataset(&dsn("a"))
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
    w.fs.block_write(&dsn("a"), 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let c = cursor(&w.fs)
        .await
        .expect("a log region after the first commit");
    assert_eq!((c.used, c.seq), (0, 0));

    // Later flushes only append to the log: no commit.
    let before = txg(&s).await;
    w.fs.block_write(&dsn("a"), 100_000, &pattern(200_000, 2))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_zero(&dsn("a"), 1_000_000, 500_000)
        .await
        .unwrap();
    w.fs.block_write(&dsn("a"), 2_000_000, &pattern(70_001, 3))
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
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
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
        .block_write(&dsn("a"), 0, &pattern(100_000, 1))
        .await
        .unwrap();
    w1.fs.flush().await.unwrap(); // commits, registers the log
    w1.fs
        .block_write(&dsn("a"), 50_000, &pattern(400_000, 2))
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
    w2.fs
        .block_write(&dsn("a"), 0, &pattern(1000, 3))
        .await
        .unwrap();
    w2.fs.flush().await.unwrap();
    w2.fs
        .block_write(&dsn("a"), 4000, &pattern(1000, 4))
        .await
        .unwrap();
    w2.fs.flush().await.unwrap();
    w2.fs.detach_dataset(&dsn("a")).await.unwrap();
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
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
    w.fs.block_write(&dsn("a"), 0, &pattern(50_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let first = cursor(&w.fs).await.unwrap().region;
    w.fs.block_write(&dsn("a"), 0, &pattern(50_000, 2))
        .await
        .unwrap();
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

    w.fs.detach_dataset(&dsn("a")).await.unwrap();
    // Gone from the writer, log cursor and all: it can't even be opened
    // there any more (structural triage I-23).
    assert!(w.fs.open_dataset(&dsn("a")).await.is_err());
    assert!(!w.fs.open_datasets.lock().unwrap().contains_key("a"));
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
    w.fs.return_metaslabs().await;
    assert_clean(&s).await;
}

#[tokio::test]
async fn a_full_log_falls_back_to_a_commit() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let region = cursor(&w.fs).await.unwrap().region;

    // More than the region holds, in one flush.
    let big = pattern(region.len as usize * crate::disk::BLOCK_SIZE + 1, 2);
    let before = txg(&s).await;
    w.fs.block_write(&dsn("a"), 0, &big).await.unwrap();
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
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();

    // The lease can't be confirmed (renewals failing): flushes aren't acked.
    ok.store(false, Ordering::SeqCst);
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 2))
        .await
        .unwrap();
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

/// Make the `ds/a` lease look as if it expired `ago_ms` ago.
fn expire_volume_lease(s: &Setup, ago_ms: u64) {
    let mut state = s.auth.0.lock().unwrap();
    let l = state.leases.get_mut(&dataset_resource("a")).unwrap();
    l.expires_ms = crate::cluster::now_ms() - ago_ms;
}

#[tokio::test]
async fn a_lapsed_lease_is_taken_over_before_its_log_is_replayed() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write(&dsn("a"), 0, &pattern(300_000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 2))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let mut want = pattern(300_000, 1);
    want[..1000].copy_from_slice(&pattern(1000, 2));

    // Only just expired: its holder may still renew it. Leave it be.
    expire_volume_lease(&s, 1000);
    s.committer.reclaim().await;
    assert_eq!(s.committer.fs().dataset_logs().await.unwrap().len(), 1);

    // Lapsed for good (the writer is partitioned away, not dead).
    expire_volume_lease(&s, 60_000);
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);

    // The partition heals: the old holder can't renew its lease and go on
    // acknowledging flushes into a log nobody will replay.
    let renew = LeaseOp::Renew {
        resource: dataset_resource("a"),
        holder: Holding::new(w.ds_owner.clone(), w.ds_epoch),
        ttl_ms: 10_000,
    };
    assert_eq!(s.auth.propose(renew).await.unwrap(), Response::Lost);
    // And the committer let go of it again: the next node can have it.
    let next = lease(&s.auth, &dataset_resource("a"), "o2").await;
    assert!(next > w.ds_epoch);
    assert_clean(&s).await;
}

#[tokio::test]
async fn replay_stops_at_the_end_of_the_log() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 2))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    // Leftovers past the last record: a record from another log (wrong
    // guid), then garbage.
    let c = cursor(&w.fs).await.unwrap();
    let foreign = encode_data_record(c.region.guid ^ 1, c.seq, 0, pattern(48 * 1024, 9)).unwrap();
    w.fs.write_record_blocks(PoolLba::new(c.region.start + c.used), &foreign)
        .await
        .unwrap();
    let after = c.used + w.fs.align_blocks(foreign.len().div_ceil(BLOCK_SIZE) as u64);
    w.fs.write_record_blocks(PoolLba::new(c.region.start + after), &pattern(8192, 7))
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
async fn a_flush_some_disk_missed_commits_and_reports_the_miss() {
    let s = setup().await;
    // This writer can't reach disks 1 and 3: its records are on three of
    // five disks, all raidz2 has to spare. Logged, they'd be lost with one
    // more disk (its node, say): a replay meanwhile would take an unreadable
    // record for the end of the log, and the committer, never told of the
    // misses, would count those disks healthy and go ahead.
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res(&resource), epoch),
    });
    let fs = FileSystem::load_writer(s.pool.dev_without(&[1, 3]).await, sink)
        .await
        .unwrap();
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    fs.block_write(&dsn("a"), 0, &pattern(10_000, 1))
        .await
        .unwrap();
    fs.flush().await.unwrap(); // commits: log registered
    let data = pattern(300_000, 2);
    for (i, chunk) in data.chunks(50_000).enumerate() {
        let before = txg(&s).await;
        fs.block_write(&dsn("a"), (i * 50_000) as u64, chunk)
            .await
            .unwrap();
        fs.flush().await.unwrap();
        assert!(txg(&s).await > before, "flush {} committed", i);
    }
    // The committer knows, durably.
    let fresh = s.pool.load().await;
    let degraded = fresh.degraded_disks();
    assert!(
        degraded[1].is_some() && degraded[3].is_some(),
        "{:?}",
        degraded
    );
    // Every acknowledged write is in the tree: nothing waits on a replay.
    assert_eq!(read(&fresh, 0, data.len()).await, data);
    drop(fresh);
    drop(fs);
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());

    // A writer that reaches every disk acknowledges from the log again.
    let w = writer(&s, "2", None).await;
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 3))
        .await
        .unwrap();
    w.fs.flush().await.unwrap(); // commits: log registered
    let before = txg(&s).await;
    w.fs.block_write(&dsn("a"), 4096, &pattern(1000, 4))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "logged");
    assert_eq!(cursor(&w.fs).await.unwrap().seq, 1);
}

/// Holds every volume commit until released (a committer failing over).
struct Gate {
    inner: DirectSink<Pool>,
    open: tokio::sync::watch::Receiver<bool>,
    waiting: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::writer::CommitSink for Gate {
    fn writer(&self) -> LeaseRef {
        self.inner.writer()
    }
    async fn lease_metaslab(
        &self,
        want: u64,
    ) -> Result<crate::writer::LentMetaslab, crate::writer::CommitError> {
        self.inner.lease_metaslab(want).await
    }
    async fn return_metaslab(&self, idx: MetaslabIdx) -> Result<(), crate::writer::CommitError> {
        self.inner.return_metaslab(idx).await
    }
    async fn commit(
        &self,
        c: crate::writer::DatasetCommit,
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
    async fn claim_dataset(
        &self,
        volume: &crate::types::DatasetName,
        ds: &Holding,
    ) -> Result<(), crate::writer::CommitError> {
        self.inner.claim_dataset(volume, ds).await
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
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        },
        open: open_rx,
        waiting: Default::default(),
    });
    let fs = Arc::new(
        FileSystem::load_writer(s.pool.dev().await, sink.clone())
            .await
            .unwrap(),
    );
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    fs.block_write(&dsn("a"), 0, &pattern(100_000, 1))
        .await
        .unwrap();
    fs.flush().await.unwrap(); // commits: log registered

    // B is written and logged, then a commit starts and gets stuck.
    fs.block_write(&dsn("a"), 0, &pattern(50_000, 2))
        .await
        .unwrap();
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
    fs.block_write(&dsn("a"), 200_000, &pattern(60_000, 3))
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
        s.committer.fs().dataset_logs().await.unwrap()[0]
            .1
            .regions
            .len(),
        2
    );

    // The writer dies before committing C: replay finds it in the old
    // region's tail.
    drop(fs);
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
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
    w.fs.block_write(&dsn("a"), 0, &pattern(1000, 1))
        .await
        .unwrap();
    w.fs.flush().await.unwrap();
    let data = pattern(200_000, 2);
    w.fs.block_write(&dsn("a"), 0, &data).await.unwrap();
    w.fs.flush().await.unwrap();
    w.die(&s).await;

    // More disks need a resilver than raidz2 can cover: unreadable records
    // would look like the end of the log, so nothing is replayed (or dropped).
    let disks = s.committer.fs().disk_count();
    for d in 0..3 {
        s.committer
            .fs()
            .mark_degraded(PoolDiskIdx::new(d).unwrap(), Txg::from_raw(1));
    }
    s.committer.reclaim().await;
    assert_eq!(s.committer.fs().dataset_logs().await.unwrap().len(), 1);

    // Resilvered: now it's replayed.
    *s.committer.fs().degraded.lock().unwrap() = vec![None; disks];
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
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
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        })
    };
    // Written and committed while disk 1 was unreachable: its shards there
    // are stale, and the commit marks it degraded.
    let a = FileSystem::load_writer(s.pool.dev_without(&[1]).await, sink())
        .await
        .unwrap();
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    a.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    a.block_write(&dsn("a"), 0, &pattern(400_000, 1))
        .await
        .unwrap();
    a.sync().await.unwrap();
    assert!(s.committer.fs().degraded_disks()[1].is_some());
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
    let scrub = s.committer.fs().scrub(false).await.unwrap();
    assert_eq!(scrub.mismatched(), 0, "{}", scrub);
}

#[tokio::test]
async fn a_failed_disk_flush_leaves_no_gap_before_later_records() {
    use crate::test_util::FaultyDev;
    let s = setup().await;
    let epoch = lease(&s.auth, "writer/1", "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res("writer/1"), epoch),
    });
    let (dev, switch) = FaultyDev::new(s.pool.dev().await);
    let fs = FileSystem::load_writer(dev, sink).await.unwrap();
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset_leased(&dsn("a"), Holding::new("o1", ds_epoch), None, None)
        .await
        .unwrap();
    let cursor = |fs: &FileSystem<FaultyDev<Pool>>| {
        let vol = fs.open_datasets.lock().unwrap().get("a").cloned().unwrap();
        vol.cursor.lock().unwrap().clone()
    };

    fs.block_write(&dsn("a"), 0, &pattern(300_000, 1))
        .await
        .unwrap();
    fs.flush().await.unwrap();

    // The disk flush fails: the record just written may not have made it.
    fs.block_write(&dsn("a"), 0, &pattern(1000, 2))
        .await
        .unwrap();
    let c = cursor(&fs).expect("a log region");
    switch.fail_flushes(true);
    assert!(fs.flush().await.is_err());
    switch.fail_flushes(false);
    // Say it didn't.
    fs.write_record_blocks(
        PoolLba::new(c.region.start + c.used),
        &vec![0u8; BLOCK_SIZE],
    )
    .await
    .unwrap();

    // A later flush is acknowledged...
    fs.block_write(&dsn("a"), 1000, &pattern(1000, 3))
        .await
        .unwrap();
    fs.flush().await.unwrap();

    // ...and survives the writer dying, gap or not.
    drop(fs);
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
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

/// A tiny deterministic generator (no rand dependency in tests).
fn lcg(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed >> 33
}

#[tokio::test]
async fn small_writes_log_only_the_bytes_that_changed() {
    let s = setup().await;
    let w = writer(&s, "1", None).await;
    let bs = w.fs.open_dataset(&dsn("a")).await.unwrap().blksz;
    let size = 3 * MB;
    let mut want = vec![0u8; size];

    // A committed base, and a registered log.
    want[..MB].copy_from_slice(&pattern(MB, 1));
    w.fs.block_write(&dsn("a"), 0, &want[..MB]).await.unwrap();
    w.fs.flush().await.unwrap();
    let before = txg(&s).await;

    // Small writes, each flushed: over committed data, overlapping each
    // other, and into records that were never written (zeros under them).
    let mut seed = 7u64;
    let flushes = 60;
    for i in 0..flushes {
        let len = 1 + (lcg(&mut seed) as usize % 2000);
        let off = match i % 3 {
            0 => lcg(&mut seed) as usize % (MB - len),
            1 => 2 * MB + lcg(&mut seed) as usize % (MB - len),
            // Overlaps the previous write's record.
            _ => (lcg(&mut seed) as usize % (MB / bs)) * bs + bs / 3,
        };
        let data = pattern(len, i as u8 + 10);
        w.fs.block_write(&dsn("a"), off as u64, &data)
            .await
            .unwrap();
        want[off..off + len].copy_from_slice(&data);
        w.fs.flush().await.unwrap();
    }
    // A record zeroed whole, then written into: a hole, then bytes over it.
    w.fs.block_zero(&dsn("a"), (4 * bs) as u64, bs)
        .await
        .unwrap();
    want[4 * bs..5 * bs].fill(0);
    w.fs.block_write(&dsn("a"), (4 * bs + 100) as u64, &pattern(300, 99))
        .await
        .unwrap();
    want[4 * bs + 100..4 * bs + 400].copy_from_slice(&pattern(300, 99));
    // Most of a record: logged whole.
    w.fs.block_write(&dsn("a"), (8 * bs + 10) as u64, &pattern(bs - 20, 98))
        .await
        .unwrap();
    want[8 * bs + 10..9 * bs - 10].copy_from_slice(&pattern(bs - 20, 98));
    w.fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "every flush went to the log");

    // Whole records would take at least a record's blocks per flush.
    let used = cursor(&w.fs).await.unwrap().used;
    let record_blocks = bs.div_ceil(crate::disk::BLOCK_SIZE) as u64;
    assert!(
        used < flushes * record_blocks / 3,
        "log used {} blocks for {} small flushes",
        used,
        flushes
    );
    assert_eq!(read(&w.fs, 0, size).await, want);

    w.die(&s).await;
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, size).await, want);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_patch_written_during_a_stuck_commit_applies_over_what_it_synced() {
    let s = setup().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let (open_tx, open_rx) = tokio::sync::watch::channel(true);
    let sink = Arc::new(Gate {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        },
        open: open_rx,
        waiting: Default::default(),
    });
    let fs = Arc::new(
        FileSystem::load_writer(s.pool.dev().await, sink.clone())
            .await
            .unwrap(),
    );
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    let mut want = pattern(100_000, 1);
    fs.block_write(&dsn("a"), 0, &want).await.unwrap();
    fs.flush().await.unwrap(); // commits: log registered

    // B: a few bytes, logged as a patch; then a commit takes the record
    // and gets stuck.
    fs.block_write(&dsn("a"), 1000, &pattern(10, 2))
        .await
        .unwrap();
    want[1000..1010].copy_from_slice(&pattern(10, 2));
    fs.flush().await.unwrap();
    open_tx.send_replace(false);
    let folding = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.sync().await })
    };
    while sink.waiting.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // C: more bytes of the same record, over the copy being synced.
    fs.block_write(&dsn("a"), 3000, &pattern(20, 3))
        .await
        .unwrap();
    want[3000..3020].copy_from_slice(&pattern(20, 3));
    fs.flush().await.unwrap();

    open_tx.send_replace(true);
    folding.await.unwrap().unwrap();

    // The writer dies with C only in the log: replay patches it onto the
    // committed record (which has B).
    drop(fs);
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    assert_eq!(read(&fresh, 0, want.len()).await, want);
    drop(fresh);
    assert_clean(&s).await;
}

#[test]
fn unlogged_ranges_merge() {
    use crate::object::Unlogged;
    let mut u = Unlogged::Ranges(Vec::new());
    u.add(10, 20);
    u.add(30, 40);
    u.add(20, 25); // touches the first
    assert_eq!(u, Unlogged::Ranges(vec![(10, 25), (30, 40)]));
    u.add(5, 35); // swallows both
    assert_eq!(u, Unlogged::Ranges(vec![(5, 40)]));
    u.add(0, 1);
    assert_eq!(u, Unlogged::Ranges(vec![(0, 1), (5, 40)]));
    assert_eq!(u.bytes(), Some(36));
    u.merge(&Unlogged::Ranges(vec![(100, 110)]));
    assert_eq!(u, Unlogged::Ranges(vec![(0, 1), (5, 40), (100, 110)]));
    // Too many pieces: the whole record.
    for i in 0..20 {
        u.add(200 + i * 10, 201 + i * 10);
    }
    assert_eq!(u, Unlogged::Whole);
    u.add(0, 1);
    assert_eq!(u, Unlogged::Whole);
    let mut v = Unlogged::Ranges(vec![(1, 2)]);
    v.merge(&Unlogged::Whole);
    assert_eq!(v, Unlogged::Whole);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filling_log_is_folded_early_by_the_sync_loop() {
    let s = setup().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res(&resource), epoch),
    });
    let fs = Arc::new(
        FileSystem::load_writer(s.pool.dev().await, sink)
            .await
            .unwrap(),
    );
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    fs.block_write(&dsn("a"), 0, &pattern(1000, 1))
        .await
        .unwrap();
    fs.flush().await.unwrap(); // commits: log registered
    // The loop's first tick runs at once; the next would be SYNC_INTERVAL
    // away, far longer than this test waits.
    crate::volume_io::ensure_sync_loop(&fs, "test");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let first = cursor(&fs).await.unwrap().region;

    // Flushes, each logged whole, until the region passes the mark.
    let chunk = 256 * 1024;
    let mut want = vec![0u8; 8 * MB];
    want[..1000].copy_from_slice(&pattern(1000, 1));
    let mut i = 0usize;
    let before = txg(&s).await;
    loop {
        let c = cursor(&fs).await.unwrap();
        if c.region != first {
            break; // already folded
        }
        if c.used * 100 >= c.region.len * crate::zil::FOLD_AT_PERCENT {
            break;
        }
        let off = (i * chunk) % (8 * MB);
        let data = pattern(chunk, i as u8 + 2);
        fs.block_write(&dsn("a"), off as u64, &data).await.unwrap();
        want[off..off + chunk].copy_from_slice(&data);
        fs.flush().await.unwrap();
        assert_eq!(txg(&s).await, before, "a flush fell back to a commit");
        i += 1;
        assert!(i < 10_000, "the log never filled");
    }

    // The sync loop folds without waiting for its next tick.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while cursor(&fs).await.unwrap().region == first {
        assert!(std::time::Instant::now() < deadline, "no early fold");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(read(&fs, 0, want.len()).await, want);
    fs.detach_dataset(&dsn("a")).await.unwrap();
    fs.return_metaslabs().await;
    drop(fs);
    release(&s.auth, &dataset_resource("a"), "o1", ds_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    assert_clean(&s).await;
}

// ---- keyed (kv) datasets: the log carries operations ----

/// A pool with kv dataset "k" (small nodes, so trees split), and its
/// committer.
async fn setup_kv() -> Setup {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_kv_with(&dsn("k"), fs.volblock_align(), 1 << 16)
        .await
        .unwrap();
    drop(fs);
    let auth = Arc::new(LocalAuthority::default());
    let epoch = lease(&auth, &POOL, "c").await;
    let fs = Arc::new(pool.load().await);
    let committer =
        Arc::new(Committer::new(fs, auth.clone(), "c".into(), epoch, "n0".into()).await);
    Setup {
        pool,
        auth,
        committer,
    }
}

async fn kv_all(fs: &FileSystem<Pool>) -> Vec<(Vec<u8>, Vec<u8>)> {
    fs.kv_range(
        &dsn("k"),
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        usize::MAX,
    )
    .await
    .unwrap()
}

fn model_vec(m: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

async fn kv_cursor(fs: &FileSystem<Pool>) -> Option<LogCursor> {
    fs.open_dataset(&dsn("k"))
        .await
        .unwrap()
        .cursor
        .lock()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn kv_flushes_log_operations_and_replay_after_a_crash() {
    let s = setup_kv().await;
    let id = "1";
    let wepoch = lease(&s.auth, "writer/1", id).await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res("writer/1"), wepoch),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    let ds_epoch = lease(&s.auth, &dataset_resource("k"), "o1").await;
    fs.attach_dataset(&dsn("k"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();

    // A committed base (deep enough to split on later puts), and a log.
    let mut model = BTreeMap::new();
    for i in 0..600u32 {
        let k = format!("base/{:05}", i).into_bytes();
        let v = pattern(40 + (i as usize % 90), i as u8);
        fs.kv_put(&dsn("k"), &k, &v).await.unwrap();
        model.insert(k, v);
    }
    fs.flush().await.unwrap();
    assert!(
        kv_cursor(&fs).await.is_some(),
        "a log after the first commit"
    );
    let before = txg(&s).await;

    // Small operations, each flushed (acknowledged): puts that split
    // nodes, replacements, deletes, a large value in overflow pages.
    let mut seed = 11u64;
    // (The test pool's log region holds about 80 of them.)
    let flushes = 60u64;
    for n in 0..flushes {
        let i = lcg(&mut seed) % 700;
        let k = format!("base/{:05}", i).into_bytes();
        if n % 4 == 3 {
            fs.kv_delete(&dsn("k"), &k).await.unwrap();
            model.remove(&k);
        } else {
            let v = pattern(10 + (lcg(&mut seed) as usize % 200), n as u8);
            fs.kv_put(&dsn("k"), &k, &v).await.unwrap();
            model.insert(k, v);
        }
        fs.flush().await.unwrap();
    }
    let big = pattern(100_000, 77);
    fs.kv_put(&dsn("k"), b"big", &big).await.unwrap();
    model.insert(b"big".to_vec(), big);
    fs.flush().await.unwrap();
    assert_eq!(txg(&s).await, before, "every flush went to the log");

    // Operations are small: about one log unit per flush, not the nodes
    // (a leaf and the header at least) each put rewrote.
    let used = kv_cursor(&fs).await.unwrap().used;
    let unit = fs.align_blocks(1);
    let node_blocks = (fs.volblock_align() as usize).div_ceil(crate::disk::BLOCK_SIZE) as u64;
    assert!(
        used <= (flushes + 1) * unit + 100_000u64.div_ceil(crate::disk::BLOCK_SIZE as u64) + unit,
        "log used {} blocks for {} flushes",
        used,
        flushes
    );
    assert!(used < (flushes + 1) * 2 * node_blocks);
    assert_eq!(kv_all(&fs).await, model_vec(&model));

    // The writer dies: the tree doesn't have the acknowledged operations...
    drop(fs);
    release(&s.auth, &dataset_resource("k"), "o1", ds_epoch).await;
    release(&s.auth, "writer/1", id, wepoch).await;
    let fresh = s.pool.load().await;
    assert_ne!(kv_all(&fresh).await, model_vec(&model));
    drop(fresh);
    // ...until the committer replays them through the tree.
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());
    let fresh = s.pool.load().await;
    assert_eq!(kv_all(&fresh).await, model_vec(&model));
    assert_eq!(fresh.kv_len(&dsn("k")).await.unwrap(), model.len() as u64);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_operations_a_stuck_fold_took_replay_again_harmlessly() {
    let s = setup_kv().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let (open_tx, open_rx) = tokio::sync::watch::channel(true);
    let sink = Arc::new(Gate {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: LeaseRef::new(crate::types::res(&resource), epoch),
        },
        open: open_rx,
        waiting: Default::default(),
    });
    let fs = Arc::new(
        FileSystem::load_writer(s.pool.dev().await, sink.clone())
            .await
            .unwrap(),
    );
    let ds_epoch = lease(&s.auth, &dataset_resource("k"), "o1").await;
    fs.attach_dataset(&dsn("k"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    let mut model = BTreeMap::new();
    for i in 0..400u32 {
        let k = format!("a/{:04}", i).into_bytes();
        fs.kv_put(&dsn("k"), &k, &k).await.unwrap();
        model.insert(k.clone(), k);
    }
    fs.flush().await.unwrap(); // commits: log registered

    // B: operations applied but not yet logged when a commit takes their
    // records and gets stuck. A value grown then shrunk again, keys
    // inserted and deleted: replayed again over the committed tree, they
    // reshape it on the way.
    for i in 0..60u32 {
        let k = format!("a/{:04}", i * 5).into_bytes();
        fs.kv_put(&dsn("k"), &k, &pattern(700, i as u8))
            .await
            .unwrap();
        fs.kv_put(&dsn("k"), &k, b"short").await.unwrap();
        model.insert(k, b"short".to_vec());
        let n = format!("b/{:04}", i).into_bytes();
        fs.kv_put(&dsn("k"), &n, &n).await.unwrap();
        fs.kv_delete(&dsn("k"), &n).await.unwrap();
    }
    open_tx.send_replace(false);
    let folding = {
        let fs = fs.clone();
        tokio::spawn(async move { fs.sync().await })
    };
    while sink.waiting.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // C, after the cut. This flush logs B (its commit hasn't landed) and C.
    for i in 0..80u32 {
        let k = format!("c/{:04}", i).into_bytes();
        fs.kv_put(&dsn("k"), &k, &pattern(30, i as u8))
            .await
            .unwrap();
        model.insert(k, pattern(30, i as u8));
    }
    fs.kv_delete(&dsn("k"), b"a/0001").await.unwrap();
    model.remove(&b"a/0001"[..]);
    tokio::time::timeout(std::time::Duration::from_secs(2), fs.flush())
        .await
        .expect("a flush doesn't wait for the stuck commit")
        .unwrap();

    open_tx.send_replace(true);
    folding.await.unwrap().unwrap();
    assert_eq!(kv_all(&fs).await, model_vec(&model));

    // The writer dies with C only in the log (after B again).
    drop(fs);
    release(&s.auth, &dataset_resource("k"), "o1", ds_epoch).await;
    release(&s.auth, "writer/1", "1", epoch).await;
    s.committer.reclaim().await;
    let fresh = s.pool.load().await;
    assert_eq!(kv_all(&fresh).await, model_vec(&model));
    assert_eq!(fresh.kv_len(&dsn("k")).await.unwrap(), model.len() as u64);
    drop(fresh);
    assert_clean(&s).await;
}

#[tokio::test]
async fn committed_kv_operations_leave_the_queue() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_kv(&dsn("k")).await.unwrap();
    for i in 0..50u32 {
        fs.kv_put(&dsn("k"), &i.to_be_bytes(), b"v").await.unwrap();
    }
    // No writer: nothing logs them, a commit has them.
    let ds = fs.open_dataset(&dsn("k")).await.unwrap();
    assert_eq!(ds.obj.dirty.lock().unwrap().ops.len(), 50);
    fs.sync().await.unwrap();
    assert!(ds.obj.dirty.lock().unwrap().ops.is_empty());
    // A delete of nothing changes nothing, so it queues nothing.
    fs.kv_delete(&dsn("k"), b"absent").await.unwrap();
    assert!(ds.obj.dirty.lock().unwrap().ops.is_empty());
}

#[tokio::test]
async fn misses_no_commit_carried_are_reported_on_their_own() {
    let s = setup().await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res(&resource), epoch),
    });
    let fs = FileSystem::load_writer(s.pool.dev_without(&[2]).await, sink)
        .await
        .unwrap();
    let ds_epoch = lease(&s.auth, &dataset_resource("a"), "o1").await;
    fs.attach_dataset(&dsn("a"), Holding::new("o1", ds_epoch))
        .await
        .unwrap();
    fs.block_write(&dsn("a"), 0, &pattern(10_000, 1))
        .await
        .unwrap();
    fs.flush().await.unwrap(); // commits (and reports disk 2)
    let w = fs.writer.clone().unwrap();
    let vol = fs.open_dataset(&dsn("a")).await.unwrap();
    // Nothing unreported: nothing to commit.
    let before = txg(&s).await;
    fs.writer_report_misses(&w, &vol).await.unwrap();
    assert_eq!(txg(&s).await, before);
    // A write disk 2 misses, that no commit carries (the free slot after
    // the log's last record: the next append overwrites it).
    let c = cursor(&fs).await.unwrap();
    fs.write_record_blocks(PoolLba::new(c.region.start + c.used), &vec![0u8; 4096])
        .await
        .unwrap();
    assert!(fs.unaccounted_misses()[2]);
    *s.committer.fs().degraded.lock().unwrap() = vec![None; s.committer.fs().disk_count()];
    fs.writer_report_misses(&w, &vol).await.unwrap();
    assert!(txg(&s).await > before, "committed");
    assert!(!fs.unaccounted_misses()[2]);
    assert!(s.committer.fs().degraded_disks()[2].is_some());
    // The log is as it was: flushes carry on into it.
    assert_eq!(cursor(&fs).await.unwrap(), c);
    assert_eq!(s.committer.fs().dataset_logs().await.unwrap().len(), 1);
    let fresh = s.pool.load().await;
    assert!(fresh.degraded_disks()[2].is_some());
}

/// After the committer replays a dead writer's kv log, the next writer to
/// take the dataset carries on from the replayed tree.
#[tokio::test]
async fn kv_the_next_writer_carries_on_after_a_replay() {
    let s = setup_kv().await;
    let mut model = BTreeMap::new();
    {
        let wepoch = lease(&s.auth, "writer/1", "1").await;
        let sink = Arc::new(DirectSink {
            committer: s.committer.clone(),
            writer: LeaseRef::new(crate::types::res("writer/1"), wepoch),
        });
        let fs = FileSystem::load_writer(s.pool.dev().await, sink)
            .await
            .unwrap();
        let ds_epoch = lease(&s.auth, &dataset_resource("k"), "o1").await;
        fs.attach_dataset(&dsn("k"), Holding::new("o1", ds_epoch))
            .await
            .unwrap();
        for i in 0..400u32 {
            let k = format!("base/{:05}", i).into_bytes();
            let v = pattern(40 + (i as usize % 90), i as u8);
            fs.kv_put(&dsn("k"), &k, &v).await.unwrap();
            model.insert(k, v);
        }
        fs.flush().await.unwrap();
        // Acknowledged from the log only, then the writer dies.
        for i in 0..60u32 {
            let k = format!("log/{:05}", i).into_bytes();
            let v = pattern(30, i as u8);
            fs.kv_put(&dsn("k"), &k, &v).await.unwrap();
            model.insert(k, v);
            fs.flush().await.unwrap();
        }
        drop(fs);
        release(&s.auth, &dataset_resource("k"), "o1", ds_epoch).await;
        release(&s.auth, "writer/1", "1", wepoch).await;
    }

    // A second writer, open before the replay (like every `listen` node's).
    let wepoch = lease(&s.auth, "writer/2", "2").await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: LeaseRef::new(crate::types::res("writer/2"), wepoch),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    s.committer.reclaim().await;
    assert!(s.committer.fs().dataset_logs().await.unwrap().is_empty());

    let ds_epoch = lease(&s.auth, &dataset_resource("k"), "o2").await;
    fs.attach_dataset(&dsn("k"), Holding::new("o2", ds_epoch))
        .await
        .unwrap();
    assert_eq!(kv_all(&fs).await, model_vec(&model));
    for i in 0..50u32 {
        let k = format!("next/{:05}", i).into_bytes();
        let v = pattern(20, i as u8);
        fs.kv_put(&dsn("k"), &k, &v).await.unwrap();
        model.insert(k, v);
    }
    fs.sync().await.expect("the next writer's sync");
    let fresh = s.pool.load().await;
    assert_eq!(kv_all(&fresh).await, model_vec(&model));
    drop(fresh);
    assert_clean(&s).await;
}
