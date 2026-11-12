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
