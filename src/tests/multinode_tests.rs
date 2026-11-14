//! Several writers on one pool: a committer and volume writers, in process.

use super::*;
use crate::cluster::state::Response;
use crate::fence::FenceMap;
use crate::test_util::{TestDiskServer, TestPool};
use crate::vdev::Pool;

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

async fn setup(volumes: &[&str]) -> Setup {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    for v in volumes {
        fs.create_block(v, 8 * MB as u64, fs.default_volblocksize())
            .await
            .unwrap();
    }
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
    resource: String,
    epoch: u64,
}

async fn writer(s: &Setup, dev: Pool, id: &str) -> Writer {
    let resource = format!("writer/{}", id);
    let epoch = lease(&s.auth, &resource, id).await;
    let sink = Arc::new(DirectSink {
        committer: s.committer.clone(),
        writer: resource.clone(),
        epoch,
    });
    Writer {
        fs: FileSystem::load_writer(dev, sink).await.unwrap(),
        resource,
        epoch,
    }
}

/// Take `vol/<name>` as `owner` and run it on `w`.
async fn attach(s: &Setup, w: &Writer, vol: &str, owner: &str) -> u64 {
    let epoch = lease(&s.auth, &volume_resource(vol), owner).await;
    w.fs.attach_volume(vol, owner, epoch).await.unwrap();
    epoch
}

async fn read(fs: &FileSystem<Pool>, vol: &str, offset: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = fs.block_read(vol, offset, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

#[tokio::test]
async fn two_writers_two_volumes() {
    let s = setup(&["a", "b"]).await;
    let w1 = writer(&s, s.pool.dev().await, "1").await;
    let w2 = writer(&s, s.pool.dev().await, "2").await;
    attach(&s, &w1, "a", "o1").await;
    attach(&s, &w2, "b", "o2").await;

    let da = pattern(3 * MB, 1);
    let db = pattern(2 * MB + 12345, 2);
    let (r1, r2) = tokio::join!(
        async {
            w1.fs.block_write("a", 0, &da).await?;
            w1.fs.sync().await
        },
        async {
            w2.fs.block_write("b", 4096, &db).await?;
            w2.fs.sync().await
        }
    );
    r1.unwrap();
    r2.unwrap();
    // Overwrite part of each (the replaced records are freed by the
    // committer), and sync a few times.
    let patch = pattern(200_000, 9);
    for round in 0..3u64 {
        w1.fs
            .block_write("a", 1000 + round * 4096, &patch)
            .await
            .unwrap();
        w2.fs
            .block_write("b", 50_000 + round, &patch)
            .await
            .unwrap();
        w1.fs.sync().await.unwrap();
        w2.fs.sync().await.unwrap();
    }
    let mut want_a = da.clone();
    let mut want_b = vec![0u8; 4096];
    want_b.extend_from_slice(&db);
    for round in 0..3usize {
        want_a[1000 + round * 4096..][..patch.len()].copy_from_slice(&patch);
        want_b[50_000 + round..][..patch.len()].copy_from_slice(&patch);
    }
    assert_eq!(read(&w1.fs, "a", 0, want_a.len()).await, want_a);
    assert_eq!(read(&w2.fs, "b", 0, want_b.len()).await, want_b);
    assert!(!s.committer.fs.lent_metaslabs().await.is_empty());

    // A running volume's intent log keeps its metaslab lent; once detached,
    // everything goes back.
    w1.fs.return_metaslabs().await;
    assert!(!s.committer.fs.lent_metaslabs().await.is_empty());
    w1.fs.detach_volume("a").await.unwrap();
    w2.fs.detach_volume("b").await.unwrap();
    w1.fs.return_metaslabs().await;
    w2.fs.return_metaslabs().await;
    assert!(s.committer.fs.lent_metaslabs().await.is_empty());
    drop((w1, w2));

    // Everything is in the committed tree, and the space maps agree.
    let fs = s.pool.load().await;
    assert_eq!(read(&fs, "a", 0, want_a.len()).await, want_a);
    assert_eq!(read(&fs, "b", 0, want_b.len()).await, want_b);
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Wraps a sink and keeps the last commit, to resend it.
struct Recording {
    inner: DirectSink<Pool>,
    last: std::sync::Mutex<Option<VolumeCommit>>,
    last_fs: std::sync::Mutex<Option<FsCommit>>,
}

#[async_trait]
impl CommitSink for Recording {
    fn writer(&self) -> (String, u64) {
        self.inner.writer()
    }
    async fn lease_metaslab(&self, want: u64) -> Result<LentMetaslab, CommitError> {
        self.inner.lease_metaslab(want).await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError> {
        self.inner.return_metaslab(idx).await
    }
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError> {
        *self.last.lock().unwrap() = Some(c.clone());
        self.inner.commit(c).await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        *self.last_fs.lock().unwrap() = Some(c.clone());
        self.inner.commit_fs(c).await
    }
    async fn claim_volume(
        &self,
        volume: &str,
        vol_owner: &str,
        vol_epoch: u64,
    ) -> Result<(), CommitError> {
        self.inner.claim_volume(volume, vol_owner, vol_epoch).await
    }
}

#[tokio::test]
async fn commits_are_compare_and_swap() {
    let s = setup(&["a"]).await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(Recording {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: resource,
            epoch,
        },
        last: Default::default(),
        last_fs: Default::default(),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink.clone())
        .await
        .unwrap();
    let vol_epoch = lease(&s.auth, &volume_resource("a"), "o").await;
    fs.attach_volume("a", "o", vol_epoch).await.unwrap();
    fs.block_write("a", 0, &pattern(300_000, 3)).await.unwrap();
    fs.sync().await.unwrap();
    let first = sink.last.lock().unwrap().clone().unwrap();

    // Resent (the answer got lost): acknowledged, nothing changes.
    let again = s.committer.commit(&first).await.unwrap();
    assert!(again.txg > 0);
    // Built on a root that has moved on: refused.
    fs.block_write("a", 0, &pattern(1000, 4)).await.unwrap();
    fs.sync().await.unwrap();
    // The first commit again, now that the root has moved on: refused.
    assert!(matches!(
        s.committer.commit(&first).await,
        Err(CommitError::Refused(_))
    ));
    // Space outside what was lent: refused.
    let mut wild = sink.last.lock().unwrap().clone().unwrap();
    wild.base_root = wild.new_root.clone();
    wild.allocs = vec![(s.committer.fs.layout.data_start, 1)];
    assert!(matches!(
        s.committer.commit(&wild).await,
        Err(CommitError::Refused(_))
    ));
    let mut want = pattern(300_000, 3);
    want[..1000].copy_from_slice(&pattern(1000, 4));
    assert_eq!(read(&fs, "a", 0, want.len()).await, want);
}

#[tokio::test]
async fn a_writer_without_its_leases_cannot_commit() {
    let s = setup(&["a"]).await;
    let w = writer(&s, s.pool.dev().await, "1").await;
    let vol_epoch = attach(&s, &w, "a", "o").await;
    w.fs.block_write("a", 0, &pattern(100_000, 1))
        .await
        .unwrap();
    w.fs.sync().await.unwrap();

    // The volume lease is released (moved elsewhere): the next sync is refused.
    release(&s.auth, &volume_resource("a"), "o", vol_epoch).await;
    w.fs.block_write("a", 0, &pattern(100_000, 2))
        .await
        .unwrap();
    assert!(w.fs.sync().await.is_err());

    // The writer itself goes away: the committer takes its metaslabs back.
    assert!(!s.committer.fs.lent_metaslabs().await.is_empty());
    release(&s.auth, &w.resource, "1", w.epoch).await;
    s.committer.reclaim().await;
    assert!(s.committer.fs.lent_metaslabs().await.is_empty());

    // What was committed is what's there.
    let fs = s.pool.load().await;
    assert_eq!(read(&fs, "a", 0, 100_000).await, pattern(100_000, 1));
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

/// Feed the disk server what the cluster knows (a real one learns it through Raft).
fn sync_fence(server: &TestDiskServer, s: &Setup) {
    let state = s.auth.state();
    server.floor.observe(
        state.pool_epoch(),
        state
            .metaslabs
            .iter()
            .map(|(&i, g)| (i, g.writer.is_some(), g.epoch)),
    );
}

#[tokio::test]
async fn a_writer_that_lost_its_metaslab_is_fenced() {
    let s = setup(&["a"]).await;
    let server = TestDiskServer::start(&s.pool.paths).await;
    server.floor.set_map(Some(FenceMap::new(
        s.pool.topology.clone(),
        s.committer.fs.layout,
    )));
    let all: Vec<usize> = (0..s.pool.paths.len()).collect();

    // Writer A runs the volume over the network.
    let a = writer(&s, s.pool.dev_remote(&server, &all).await, "a").await;
    let a_vol = attach(&s, &a, "a", "oa").await;
    let first = pattern(400_000, 1);
    a.fs.block_write("a", 0, &first).await.unwrap();
    a.fs.sync().await.unwrap();
    sync_fence(&server, &s);

    // A stalls; the cluster decides it's gone: its leases lapse and the
    // committer takes its metaslabs back.
    release(&s.auth, &a.resource, "a", a.epoch).await;
    release(&s.auth, &volume_resource("a"), "oa", a_vol).await;
    s.committer.reclaim().await;
    assert!(s.committer.fs.lent_metaslabs().await.is_empty());
    sync_fence(&server, &s);

    // A wakes up and writes into the metaslab it still thinks it has: the
    // disk servers refuse.
    a.fs.block_write("a", 0, &pattern(400_000, 2))
        .await
        .unwrap();
    assert!(a.fs.sync().await.is_err());

    // Writer B takes the volume over and finds A's committed data.
    let b = writer(&s, s.pool.dev_remote(&server, &all).await, "b").await;
    attach(&s, &b, "a", "ob").await;
    assert_eq!(read(&b.fs, "a", 0, first.len()).await, first);
    let second = pattern(10_000, 3);
    b.fs.block_write("a", 0, &second).await.unwrap();
    b.fs.sync().await.unwrap();
    b.fs.return_metaslabs().await;

    let fs = s.pool.load().await;
    let mut want = first.clone();
    want[..second.len()].copy_from_slice(&second);
    assert_eq!(read(&fs, "a", 0, want.len()).await, want);
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

/// An authority whose view of the cluster lags (a committer on a Raft
/// follower): proposals go through, `state()` shows an older snapshot.
struct Lagging {
    inner: Arc<LocalAuthority>,
    view: std::sync::Mutex<ClusterState>,
}

#[async_trait]
impl Authority for Lagging {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        self.inner.propose(op).await
    }
    fn state(&self) -> ClusterState {
        self.view.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn a_lagging_view_doesnt_take_back_fresh_lends() {
    let s = setup(&["a"]).await;
    // The writer's and volume's leases are visible; the lends to come aren't.
    let w_epoch = lease(&s.auth, "writer/1", "1").await;
    let v_epoch = lease(&s.auth, &volume_resource("a"), "o").await;
    let lagging = Arc::new(Lagging {
        inner: s.auth.clone(),
        view: std::sync::Mutex::new(s.auth.state()),
    });
    let pool_epoch = s.auth.state().pool_epoch();
    let committer = Arc::new(
        Committer::new(
            s.committer.fs.clone(),
            lagging.clone(),
            "c".into(),
            pool_epoch,
            "n0".into(),
        )
        .await,
    );
    let sink = Arc::new(DirectSink {
        committer: committer.clone(),
        writer: "writer/1".into(),
        epoch: w_epoch,
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    fs.attach_volume("a", "o", v_epoch).await.unwrap();
    fs.block_write("a", 0, &pattern(200_000, 5)).await.unwrap();
    fs.sync().await.unwrap();
    committer.reclaim().await;
    assert!(
        !committer.fs.lent_metaslabs().await.is_empty(),
        "not taken back"
    );
    fs.block_write("a", 0, &pattern(1000, 6)).await.unwrap();
    fs.sync().await.unwrap();
}

/// Resends `Retry` answers until there is a definite one, like
/// `CommitClient` does for a writer (a hard mount).
struct Retrying(DirectSink<Pool>);

#[async_trait]
impl CommitSink for Retrying {
    fn writer(&self) -> (String, u64) {
        self.0.writer()
    }
    async fn lease_metaslab(&self, want: u64) -> Result<LentMetaslab, CommitError> {
        self.0.lease_metaslab(want).await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError> {
        self.0.return_metaslab(idx).await
    }
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError> {
        loop {
            match self.0.commit(c.clone()).await {
                Err(CommitError::Retry(_)) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                other => return other,
            }
        }
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        self.0.commit_fs(c).await
    }
    async fn claim_volume(
        &self,
        volume: &str,
        vol_owner: &str,
        vol_epoch: u64,
    ) -> Result<(), CommitError> {
        self.0.claim_volume(volume, vol_owner, vol_epoch).await
    }
}

#[tokio::test]
async fn a_sync_stops_waiting_once_its_volume_lease_is_lost() {
    let s = setup(&["a"]).await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(Retrying(DirectSink {
        committer: s.committer.clone(),
        writer: resource,
        epoch,
    }));
    let fs = FileSystem::load_writer(s.pool.dev().await, sink)
        .await
        .unwrap();
    // A short volume lease, which lapses without anyone taking it over: the
    // committer answers "retry" (it could still be renewed), forever.
    let op = LeaseOp::Acquire {
        resource: volume_resource("a"),
        owner: "o".into(),
        desc: "test o".into(),
        ttl_ms: 2_000,
    };
    let Response::Granted { epoch: v_epoch, .. } = s.auth.propose(op).await.unwrap() else {
        panic!("volume lease not granted");
    };
    let (lost, rx) = tokio::sync::watch::channel(false);
    fs.attach_volume_until("a", "o", v_epoch, Some(rx))
        .await
        .unwrap();
    fs.block_write("a", 0, &pattern(100_000, 1)).await.unwrap();
    fs.sync().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
    fs.block_write("a", 0, &pattern(100_000, 2)).await.unwrap();
    let fs = Arc::new(fs);
    let syncing = tokio::spawn({
        let fs = fs.clone();
        async move { fs.sync().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!syncing.is_finished(), "the committer should say retry");

    // The holder notices the lease is gone: the sync gives up.
    lost.send_replace(true);
    let r = tokio::time::timeout(std::time::Duration::from_secs(5), syncing)
        .await
        .expect("sync still waiting after the lease was lost")
        .unwrap();
    assert!(r.is_err());
    fs.abandon_volume("a");

    // Only what was committed is there.
    let fs = s.pool.load().await;
    assert_eq!(read(&fs, "a", 0, 100_000).await, pattern(100_000, 1));
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

// ---- files and directories from writers ----

use crate::writer::FsOp;
use std::path::Path;

#[tokio::test]
async fn two_writers_write_files_concurrently() {
    let s = setup(&[]).await;
    let w1 = writer(&s, s.pool.dev().await, "1").await;
    let w2 = writer(&s, s.pool.dev().await, "2").await;
    w1.fs.mkdir(Path::new("shared")).await.unwrap();
    w2.fs.mkdir(Path::new("two")).await.unwrap();

    // Same directory and different ones, at the same time.
    let (r1, r2) = tokio::join!(
        async {
            for i in 0..4u8 {
                let p = format!("shared/a{}", i);
                w1.fs
                    .write_file(Path::new(&p), &pattern(300_000, i))
                    .await?;
            }
            w1.fs.write_file(Path::new("one"), &pattern(MB, 40)).await
        },
        async {
            for i in 0..4u8 {
                let p = format!("shared/b{}", i);
                w2.fs
                    .write_file(Path::new(&p), &pattern(200_000, 10 + i))
                    .await?;
            }
            w2.fs
                .write_file(Path::new("two/x"), &pattern(MB + 7, 41))
                .await
        }
    );
    r1.unwrap();
    r2.unwrap();

    // Overwrite one of the other writer's files, and delete another.
    w2.fs.reload_root_index().await.unwrap();
    w2.fs
        .write_file(Path::new("shared/a0"), &pattern(5000, 99))
        .await
        .unwrap();
    w1.fs.delete(Path::new("shared/b3")).await.unwrap();

    let check = async |fs: &FileSystem<Pool>| {
        assert_eq!(
            fs.read_file(Path::new("shared/a0")).await.unwrap(),
            pattern(5000, 99)
        );
        for i in 1..4u8 {
            let p = format!("shared/a{}", i);
            assert_eq!(
                fs.read_file(Path::new(&p)).await.unwrap(),
                pattern(300_000, i)
            );
        }
        for i in 0..3u8 {
            let p = format!("shared/b{}", i);
            assert_eq!(
                fs.read_file(Path::new(&p)).await.unwrap(),
                pattern(200_000, 10 + i)
            );
        }
        assert!(fs.read_file(Path::new("shared/b3")).await.is_err());
        assert_eq!(
            fs.read_file(Path::new("one")).await.unwrap(),
            pattern(MB, 40)
        );
        assert_eq!(
            fs.read_file(Path::new("two/x")).await.unwrap(),
            pattern(MB + 7, 41)
        );
    };
    w1.fs.reload_root_index().await.unwrap();
    w2.fs.reload_root_index().await.unwrap();
    check(&w1.fs).await;
    check(&w2.fs).await;

    w1.fs.return_metaslabs().await;
    w2.fs.return_metaslabs().await;
    drop((w1, w2));
    let fs = s.pool.load().await;
    check(&fs).await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn file_commits_are_compare_and_swap() {
    let s = setup(&["vol"]).await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(Recording {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: resource.clone(),
            epoch,
        },
        last: Default::default(),
        last_fs: Default::default(),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink.clone())
        .await
        .unwrap();
    fs.write_file(Path::new("f"), &pattern(100_000, 1))
        .await
        .unwrap();
    let first = sink.last_fs.lock().unwrap().clone().unwrap();

    // Resent (the answer got lost): acknowledged, nothing changes.
    s.committer.commit_fs(&first).await.unwrap();
    assert_eq!(
        s.committer.fs.read_file(Path::new("f")).await.unwrap(),
        pattern(100_000, 1)
    );

    // Someone else replaces it: the first write, prepared against "absent",
    // is now refused rather than clobbering the newer file.
    fs.write_file(Path::new("f"), &pattern(2000, 2))
        .await
        .unwrap();
    assert!(matches!(
        s.committer.commit_fs(&first).await,
        Err(CommitError::Refused(_))
    ));
    // A delete prepared against the first version: refused too.
    let stale_delete = FsCommit {
        op: FsOp::Delete {
            path: "f".into(),
            base: Some((
                0,
                crate::object::TreeRoot {
                    ptr: None,
                    levels: 0,
                },
            )),
        },
        ..first.clone()
    };
    assert!(matches!(
        s.committer.commit_fs(&stale_delete).await,
        Err(CommitError::Refused(_))
    ));
    // Space outside what was lent: refused.
    let mut wild = first.clone();
    if let FsOp::WriteFile {
        path, allocs, base, ..
    } = &mut wild.op
    {
        *path = "g".into();
        *base = None;
        allocs[0].0 = s.committer.fs.layout.data_start;
    }
    assert!(matches!(
        s.committer.commit_fs(&wild).await,
        Err(CommitError::Refused(_))
    ));
    // Volumes aren't deleted this way.
    assert!(fs.delete(Path::new("vol")).await.is_err());

    assert_eq!(
        fs.read_file(Path::new("f")).await.unwrap(),
        pattern(2000, 2)
    );
    fs.return_metaslabs().await;
    drop(fs);
    let fs = s.pool.load().await;
    assert_eq!(
        fs.read_file(Path::new("f")).await.unwrap(),
        pattern(2000, 2)
    );
    assert!(fs.read_file(Path::new("g")).await.is_err());
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Writes at an offset from a writer: the committer applies them with a
/// compare-and-swap on the tree they were made on; a resend is
/// acknowledged, a stale one refused, and the writer's own sync of writes
/// made on a file replaced meanwhile fails with a conflict.
#[tokio::test]
async fn offset_writes_are_compare_and_swap() {
    let s = setup(&[]).await;
    let resource = "writer/1".to_string();
    let epoch = lease(&s.auth, &resource, "1").await;
    let sink = Arc::new(Recording {
        inner: DirectSink {
            committer: s.committer.clone(),
            writer: resource.clone(),
            epoch,
        },
        last: Default::default(),
        last_fs: Default::default(),
    });
    let fs = FileSystem::load_writer(s.pool.dev().await, sink.clone())
        .await
        .unwrap();
    let mut expect = pattern(200_000, 1);
    fs.write_file(Path::new("f"), &expect).await.unwrap();

    let f = fs.open_file(Path::new("f"), None).await.unwrap();
    fs.file_write(&f, 150_000, b"middle").await.unwrap();
    fs.file_sync(&f).await.unwrap();
    expect[150_000..150_006].copy_from_slice(b"middle");
    let update = sink.last_fs.lock().unwrap().clone().unwrap();
    assert!(matches!(update.op, FsOp::UpdateFile { .. }));
    assert_eq!(
        s.committer.fs.read_file(Path::new("f")).await.unwrap(),
        expect
    );

    // Resent: acknowledged, nothing changes.
    s.committer.commit_fs(&update).await.unwrap();
    assert_eq!(
        s.committer.fs.read_file(Path::new("f")).await.unwrap(),
        expect
    );

    // Replaced by someone else: the old update is refused, and writes
    // made on the old contents are dropped, not written over the new.
    s.committer
        .fs
        .write_file(Path::new("f"), b"theirs")
        .await
        .unwrap();
    assert!(matches!(
        s.committer.commit_fs(&update).await,
        Err(CommitError::Refused(_))
    ));
    fs.file_write(&f, 0, b"mine").await.unwrap();
    assert!(matches!(
        fs.file_sync(&f).await,
        Err(FileSystemError::Conflict(_))
    ));
    assert_eq!(
        s.committer.fs.read_file(Path::new("f")).await.unwrap(),
        b"theirs"
    );

    fs.return_metaslabs().await;
    drop(fs);
    let fs = s.pool.load().await;
    assert_eq!(fs.read_file(Path::new("f")).await.unwrap(), b"theirs");
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

/// Lends like a real sink, but keeps namespace commits instead of sending
/// them (the writer is told "retry", so it keeps the space out of use).
struct Holding {
    inner: DirectSink<Pool>,
    held: std::sync::Mutex<Vec<FsCommit>>,
}

#[async_trait]
impl CommitSink for Holding {
    fn writer(&self) -> (String, u64) {
        self.inner.writer()
    }
    async fn lease_metaslab(&self, want: u64) -> Result<LentMetaslab, CommitError> {
        self.inner.lease_metaslab(want).await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError> {
        self.inner.return_metaslab(idx).await
    }
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError> {
        self.inner.commit(c).await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        self.held.lock().unwrap().push(c);
        Err(CommitError::Retry("held".into()))
    }
    async fn claim_volume(
        &self,
        volume: &str,
        vol_owner: &str,
        vol_epoch: u64,
    ) -> Result<(), CommitError> {
        self.inner.claim_volume(volume, vol_owner, vol_epoch).await
    }
}

#[tokio::test]
async fn concurrent_commits_share_a_txg() {
    let s = setup(&[]).await;
    let mut held = Vec::new();
    let mut writers = Vec::new();
    for id in 0..3u8 {
        let resource = format!("writer/{}", id);
        let epoch = lease(&s.auth, &resource, &id.to_string()).await;
        let sink = Arc::new(Holding {
            inner: DirectSink {
                committer: s.committer.clone(),
                writer: resource,
                epoch,
            },
            held: Default::default(),
        });
        let fs = FileSystem::load_writer(s.pool.dev().await, sink.clone())
            .await
            .unwrap();
        for f in 0..2u8 {
            let p = format!("w{}f{}", id, f);
            assert!(
                fs.write_file(Path::new(&p), &pattern(50_000, id * 10 + f))
                    .await
                    .is_err()
            );
        }
        assert!(fs.mkdir(Path::new(&format!("d{}", id))).await.is_err());
        held.extend(sink.held.lock().unwrap().drain(..));
        writers.push(fs);
    }
    assert_eq!(held.len(), 9);

    // All of them arrive while a commit is in progress: they apply one after
    // another, then share a single TXG.
    let before = s.committer.fs.uberblock().await.unwrap().txg;
    let busy = s.committer.fs.begin_tx().await;
    let commits = futures::future::join_all(held.iter().map(|c| s.committer.commit_fs(c)));
    let (results, ()) = tokio::join!(commits, async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(busy);
    });
    let txgs: Vec<u64> = results.into_iter().map(|r| r.unwrap().txg).collect();
    let after = s.committer.fs.uberblock().await.unwrap().txg;
    assert_eq!(after - before, 1, "9 commits took {} TXGs", after - before);
    assert!(txgs.iter().all(|&t| t == after));

    for fs in &writers {
        fs.return_metaslabs().await;
    }
    drop(writers);
    let fs = s.pool.load().await;
    for id in 0..3u8 {
        for f in 0..2u8 {
            let p = format!("w{}f{}", id, f);
            assert_eq!(
                fs.read_file(Path::new(&p)).await.unwrap(),
                pattern(50_000, id * 10 + f)
            );
        }
        fs.ls(Path::new(&format!("d{}", id))).await.unwrap();
    }
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

// ---- streaming writes, object numbers, object leases ----

use crate::writer::ObjLease;

/// Write `data` through a `FileWriter` in uneven chunks.
async fn stream(
    fs: &FileSystem<Pool>,
    path: &str,
    data: &[u8],
    lease: Option<ObjLease>,
) -> Result<u64, FileSystemError> {
    let mut f = fs.file_writer(Path::new(path), lease).await?;
    for chunk in data.chunks(100_003) {
        f.write(chunk).await?;
    }
    assert_eq!(f.len(), data.len() as u64);
    f.finish().await
}

#[tokio::test]
async fn a_writer_streams_a_large_file() {
    let s = setup(&[]).await;
    let w1 = writer(&s, s.pool.dev().await, "1").await;
    let w2 = writer(&s, s.pool.dev().await, "2").await;
    let big = pattern(5 * MB + 17, 1);

    let mut f = w1.fs.file_writer(Path::new("big"), None).await.unwrap();
    let (first, rest) = big.split_at(3 * MB);
    for chunk in first.chunks(65_536) {
        f.write(chunk).await.unwrap();
    }
    // Mid-upload: giving metaslabs back keeps the ones this file uses (the
    // committer would otherwise lend its records' space to someone else).
    w1.fs.return_metaslabs().await;
    assert!(!s.committer.fs.lent_metaslabs().await.is_empty());
    // Another writer gets on with its own files meanwhile.
    w2.fs
        .write_file(Path::new("small"), &pattern(70_000, 2))
        .await
        .unwrap();
    for chunk in rest.chunks(65_536) {
        f.write(chunk).await.unwrap();
    }
    f.finish().await.unwrap();

    assert_eq!(w1.fs.read_file(Path::new("big")).await.unwrap(), big);
    w1.fs.return_metaslabs().await;
    w2.fs.return_metaslabs().await;
    assert!(s.committer.fs.lent_metaslabs().await.is_empty());
    drop((w1, w2));
    let fs = s.pool.load().await;
    assert_eq!(fs.read_file(Path::new("big")).await.unwrap(), big);
    assert_eq!(
        fs.read_file(Path::new("small")).await.unwrap(),
        pattern(70_000, 2)
    );
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn a_stale_upload_is_refused_and_its_space_released() {
    let s = setup(&[]).await;
    let w1 = writer(&s, s.pool.dev().await, "1").await;
    let w2 = writer(&s, s.pool.dev().await, "2").await;

    let mut f = w1.fs.file_writer(Path::new("g"), None).await.unwrap();
    f.write(&pattern(MB, 1)).await.unwrap();
    // Someone else creates it first.
    w2.fs
        .write_file(Path::new("g"), &pattern(1000, 2))
        .await
        .unwrap();
    assert!(matches!(
        f.finish().await,
        Err(FileSystemError::Conflict(_))
    ));
    // An abandoned upload gives its space back too.
    let mut f = w1.fs.file_writer(Path::new("h"), None).await.unwrap();
    f.write(&pattern(MB, 3)).await.unwrap();
    f.abort().await;

    w1.fs.reload_root_index().await.unwrap();
    assert_eq!(
        w1.fs.read_file(Path::new("g")).await.unwrap(),
        pattern(1000, 2)
    );
    assert!(w1.fs.read_file(Path::new("h")).await.is_err());
    w1.fs.return_metaslabs().await;
    w2.fs.return_metaslabs().await;
    drop((w1, w2));
    let fs = s.pool.load().await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn object_numbers_are_pool_wide_and_never_reused() {
    let s = setup(&["vol"]).await;
    let w = writer(&s, s.pool.dev().await, "1").await;
    w.fs.mkdir(Path::new("a")).await.unwrap();
    w.fs.mkdir(Path::new("b")).await.unwrap();
    let mut ids = vec![
        s.committer.fs.file_id(Path::new("vol")).await.ok(),
        w.fs.create_file(Path::new("a/x")).await.ok(),
        w.fs.create_file(Path::new("b/x")).await.ok(),
        w.fs.create_file(Path::new("x")).await.ok(),
    ]
    .into_iter()
    .map(Option::unwrap)
    .collect::<Vec<_>>();
    // Same name in different directories: different objects.
    let n = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), n);

    // Creating an existing file just returns it.
    let ax = w.fs.file_id(Path::new("a/x")).await.unwrap();
    assert_eq!(w.fs.create_file(Path::new("a/x")).await.unwrap(), ax);
    // Replacing keeps the number; delete + create gets a new one.
    w.fs.write_file(Path::new("a/x"), b"hello").await.unwrap();
    assert_eq!(w.fs.file_id(Path::new("a/x")).await.unwrap(), ax);
    w.fs.delete(Path::new("a/x")).await.unwrap();
    let again = w.fs.create_file(Path::new("a/x")).await.unwrap();
    assert!(again > *ids.iter().max().unwrap());

    // The counter survives a reopen (it's in the uberblock).
    w.fs.return_metaslabs().await;
    drop(w);
    drop(s.committer);
    let fs = s.pool.load().await;
    let next = fs.create_file(Path::new("after")).await.unwrap();
    assert!(next > again);
}

#[tokio::test]
async fn an_object_lease_keeps_others_out() {
    let s = setup(&[]).await;
    let w1 = writer(&s, s.pool.dev().await, "1").await;
    let w2 = writer(&s, s.pool.dev().await, "2").await;

    let id = w1.fs.create_file(Path::new("f")).await.unwrap();
    let res = object_resource(id);
    let epoch = lease(&s.auth, &res, "up1").await;
    let held = ObjLease {
        owner: "up1".into(),
        epoch,
    };
    let data = pattern(2 * MB + 5, 7);
    let mut f = w1
        .fs
        .file_writer(Path::new("f"), Some(held.clone()))
        .await
        .unwrap();
    f.write(&data).await.unwrap();
    // While it's open for writing, no one else may change it.
    assert!(matches!(
        w2.fs.write_file(Path::new("f"), b"mine").await,
        Err(FileSystemError::Conflict(_))
    ));
    assert!(w2.fs.delete(Path::new("f")).await.is_err());
    // Others' files are fine.
    w2.fs.write_file(Path::new("other"), b"ok").await.unwrap();
    assert_eq!(f.finish().await.unwrap(), id);

    // A write whose lease was lost meanwhile is refused.
    let mut f = w1
        .fs
        .file_writer(Path::new("f"), Some(held.clone()))
        .await
        .unwrap();
    f.write(&pattern(MB, 8)).await.unwrap();
    release(&s.auth, &res, "up1", epoch).await;
    assert!(matches!(
        f.finish().await,
        Err(FileSystemError::Conflict(_))
    ));
    // Released: anyone may write it again.
    w2.fs
        .write_file(Path::new("f"), &pattern(3000, 9))
        .await
        .unwrap();

    w1.fs.reload_root_index().await.unwrap();
    assert_eq!(
        w1.fs.read_file(Path::new("f")).await.unwrap(),
        pattern(3000, 9)
    );
    w1.fs.return_metaslabs().await;
    w2.fs.return_metaslabs().await;
    drop((w1, w2));
    let fs = s.pool.load().await;
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn a_standalone_pool_streams_files_too() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let big = pattern(4 * MB + 3, 5);
    let id = stream(&fs, "big", &big, None).await.unwrap();
    assert_eq!(fs.read_file(Path::new("big")).await.unwrap(), big);
    // Replaced while a second upload was in progress: that one is refused.
    let mut f = fs.file_writer(Path::new("big"), None).await.unwrap();
    f.write(&pattern(MB, 6)).await.unwrap();
    fs.write_file(Path::new("big"), b"tiny").await.unwrap();
    assert!(matches!(
        f.finish().await,
        Err(FileSystemError::Conflict(_))
    ));
    assert_eq!(fs.file_id(Path::new("big")).await.unwrap(), id);
    drop(fs);
    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("big")).await.unwrap(), b"tiny");
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[test]
fn a_grant_this_view_hasnt_seen_yet_is_retried_not_refused() {
    use crate::cluster::state::{ClusterState, Lease};
    let mut state = ClusterState::default();
    let lease = |owner: &str, epoch, released| Lease {
        owner: owner.into(),
        desc: String::new(),
        epoch,
        expires_ms: 0,
        released,
    };
    // The previous holder's (expired) grant is all this node knows of yet.
    state.leases.insert("vol/a".into(), lease("old", 5, false));
    assert!(matches!(
        check_lease(&state, "vol/a", Some("new"), 7),
        Err(CommitError::Retry(_))
    ));
    state.leases.insert("vol/a".into(), lease("old", 5, true));
    assert!(matches!(
        check_lease(&state, "vol/a", Some("new"), 7),
        Err(CommitError::Retry(_))
    ));
    // Taken over by a newer grant, or this grant released: lost for good.
    state
        .leases
        .insert("vol/a".into(), lease("newer", 9, false));
    assert!(matches!(
        check_lease(&state, "vol/a", Some("new"), 7),
        Err(CommitError::Refused(_))
    ));
    state.leases.insert("vol/a".into(), lease("new", 7, true));
    assert!(matches!(
        check_lease(&state, "vol/a", Some("new"), 7),
        Err(CommitError::Refused(_))
    ));
}

#[test]
fn an_orphaned_log_is_replayed_only_once_its_lease_is_gone() {
    use crate::cluster::state::Lease;
    let log = crate::inode::VolumeLog {
        regions: Vec::new(),
        from_seq: 0,
        writer: "writer/1".into(),
        writer_epoch: 3,
        vol_owner: "o".into(),
        vol_epoch: 7,
    };
    let now = 1_000_000;
    let lease = |owner: &str, epoch: u64, expires_ms: u64, released: bool| Lease {
        owner: owner.into(),
        desc: String::new(),
        epoch,
        expires_ms,
        released,
    };
    let at = |l: Option<Lease>| orphan_state(l.as_ref(), &log, now);

    // Not visible yet, or the view lags behind the log's grant (a follower,
    // or a new committer): the holder may well be live.
    assert_eq!(at(None), Orphan::Live);
    assert_eq!(at(Some(lease("x", 5, 0, true))), Orphan::Live);
    assert_eq!(at(Some(lease("o", 6, 0, false))), Orphan::Live);
    // Held, or only just expired (still renewable).
    assert_eq!(at(Some(lease("o", 7, now + 5000, false))), Orphan::Live);
    assert_eq!(at(Some(lease("o", 7, now - 1000, false))), Orphan::Live);
    // Taken over, or released by its holder.
    assert_eq!(at(Some(lease("p", 8, now + 5000, false))), Orphan::Gone);
    assert_eq!(at(Some(lease("o", 7, now + 5000, true))), Orphan::Gone);
    // Lapsed past the grace period: renewable until someone takes it.
    assert_eq!(at(Some(lease("o", 7, now - 60_000, false))), Orphan::Lapsed);
}
