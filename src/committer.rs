//! The committer: the `pool` lease holder in a multi-writer pool (see
//! `crate::writer`). It lends metaslabs to volume writers, applies their
//! commits, and does the pool's metadata operations (create, delete, set
//! volume settings). `listen` runs one on whichever node wins the `pool`
//! lease; the others keep trying, so it fails over.

use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::cluster::state::{ClusterState, POOL, Response};
use crate::cluster::{Cluster, LeaseOp, now_ms};
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::VolumeProps;
use crate::volume::VolumeInfo;
use crate::writer::{CommitError, CommitSink, Committed, LentMetaslab, VolumeCommit};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Where the committer gets leases and the cluster's view from.
#[async_trait]
pub trait Authority: Send + Sync {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String>;
    /// This node's applied cluster state.
    fn state(&self) -> ClusterState;
}

#[async_trait]
impl Authority for Cluster {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        self.lease(&op, false).await
    }
    fn state(&self) -> ClusterState {
        self.status().state
    }
}

/// A cluster state machine in memory (tests, and a pool without a cluster).
#[derive(Default)]
pub struct LocalAuthority(pub std::sync::Mutex<ClusterState>);

#[async_trait]
impl Authority for LocalAuthority {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        Ok(self.0.lock().unwrap().apply(&op.stamp(now_ms())))
    }
    fn state(&self) -> ClusterState {
        self.0.lock().unwrap().clone()
    }
}

/// Is `resource` held by `owner` at `epoch`? `Err(Refused)` if it never
/// will be again (taken over, released), `Err(Retry)` if it's not visible
/// or has lapsed but may still be renewed.
fn check_lease(
    state: &ClusterState,
    resource: &str,
    owner: Option<&str>,
    epoch: u64,
) -> Result<(), CommitError> {
    if state.holds(resource, owner, epoch, now_ms()) {
        return Ok(());
    }
    match state.leases.get(resource) {
        Some(l) if l.epoch > epoch || l.released || owner.is_some_and(|o| o != l.owner) => Err(
            CommitError::Refused(format!("{} lease lost (epoch {})", resource, epoch)),
        ),
        _ => Err(CommitError::Retry(format!("{} lease not live", resource))),
    }
}

pub fn volume_resource(name: &str) -> String {
    format!("vol/{}", name)
}

pub struct Committer<D: BlockDevice> {
    pub fs: Arc<FileSystem<D>>,
    auth: Arc<dyn Authority>,
    owner: String,
    epoch: u64,
    /// The node it runs on (for status).
    pub node: String,
    /// Metaslab → (writer, writer epoch) it's lent to. The committer's own
    /// record is authoritative: this node's view of the cluster may not show
    /// a lend it just made yet.
    lends: std::sync::Mutex<std::collections::HashMap<u32, (String, u64)>>,
}

/// A writer's lease must have lapsed this long before its metaslabs are
/// taken back (this node's view of renewals may lag a little).
const RECLAIM_GRACE_MS: u64 = 3000;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CommitterInfo {
    pub node: String,
    pub epoch: u64,
}

impl<D: BlockDevice> Committer<D> {
    /// Become the committer of `fs`, which was opened writable under the
    /// `pool` lease (`owner`, `epoch`).
    pub async fn new(
        fs: Arc<FileSystem<D>>,
        auth: Arc<dyn Authority>,
        owner: String,
        epoch: u64,
        node: String,
    ) -> Self {
        if let Some(pool) = fs.dev.pool() {
            pool.disks.fence.set_pool_epoch(epoch);
        }
        let state = auth.state();
        let lends: std::collections::HashMap<u32, (String, u64)> = state
            .metaslabs
            .iter()
            .filter_map(|(&i, g)| g.writer.clone().map(|w| (i, (w, g.writer_epoch))))
            .collect();
        fs.mark_lent(lends.keys().copied()).await;
        Committer {
            fs,
            auth,
            owner,
            epoch,
            node,
            lends: std::sync::Mutex::new(lends),
        }
    }

    pub fn info(&self) -> CommitterInfo {
        CommitterInfo {
            node: self.node.clone(),
            epoch: self.epoch,
        }
    }

    /// Lend `writer` a metaslab, preferably with `want_blocks` free.
    pub async fn lend(
        &self,
        writer: &str,
        writer_epoch: u64,
        want_blocks: u64,
    ) -> Result<LentMetaslab, CommitError> {
        check_lease(&self.auth.state(), writer, None, writer_epoch)?;
        let useful = 4 * self.fs.max_record_blocks();
        let size = self.fs.layout.metaslab_size_blocks;
        let min_free = want_blocks.min(size / 2).max(useful);
        let picked = match self.fs.lend_pick(min_free).await {
            Some(p) => Some(p),
            None => self.fs.lend_pick(useful).await,
        };
        let Some((idx, free)) = picked else {
            return Err(CommitError::Refused(
                "no metaslab with free space to lend".into(),
            ));
        };
        let op = LeaseOp::Lend {
            idx,
            writer: writer.into(),
            writer_epoch,
            pool_owner: self.owner.clone(),
            pool_epoch: self.epoch,
        };
        match self.auth.propose(op).await {
            Ok(Response::Fenced { epoch }) => {
                self.lends
                    .lock()
                    .unwrap()
                    .insert(idx, (writer.to_string(), writer_epoch));
                Ok(LentMetaslab {
                    idx,
                    epoch,
                    free,
                    open_txg: self
                        .fs
                        .open_txg()
                        .await
                        .map_err(|e| CommitError::Retry(e.to_string()))?,
                })
            }
            Ok(Response::Refused(m)) => {
                self.fs.lend_abort(idx).await;
                Err(CommitError::Refused(m))
            }
            other => {
                self.fs.lend_abort(idx).await;
                Err(CommitError::Retry(format!("lend: {:?}", other)))
            }
        }
    }

    async fn take_back(&self, idx: u32) -> Result<(), CommitError> {
        let op = LeaseOp::Return {
            idx,
            pool_owner: self.owner.clone(),
            pool_epoch: self.epoch,
        };
        match self.auth.propose(op).await {
            Ok(Response::Fenced { epoch }) => {
                self.lends.lock().unwrap().remove(&idx);
                self.fs.unlend(idx, epoch).await;
                Ok(())
            }
            Ok(Response::Refused(m)) => Err(CommitError::Refused(m)),
            other => Err(CommitError::Retry(format!("return: {:?}", other))),
        }
    }

    /// A writer gives metaslab `idx` back.
    pub async fn give_back(
        &self,
        writer: &str,
        writer_epoch: u64,
        idx: u32,
    ) -> Result<(), CommitError> {
        let lent = self.lends.lock().unwrap().get(&idx).cloned();
        match lent {
            Some((w, e)) if w == writer && e == writer_epoch => self.take_back(idx).await,
            // Already back (a resend): fine.
            None => Ok(()),
            Some(_) => Err(CommitError::Refused(format!(
                "metaslab {} isn't lent to {}",
                idx, writer
            ))),
        }
    }

    pub async fn commit(&self, c: &VolumeCommit) -> Result<Committed, CommitError> {
        let state = self.auth.state();
        check_lease(&state, &c.writer, None, c.writer_epoch)?;
        check_lease(
            &state,
            &volume_resource(&c.volume),
            Some(&c.vol_owner),
            c.vol_epoch,
        )?;
        let lends = self.lends.lock().unwrap().clone();
        let txg = self
            .fs
            .apply_volume_commit(c, |idx| {
                lends
                    .get(&idx)
                    .is_some_and(|(w, e)| *w == c.writer && *e == c.writer_epoch)
            })
            .await?;
        Ok(Committed { txg })
    }

    /// Take back metaslabs whose writer is gone (its lease taken over,
    /// released, or expired).
    pub async fn reclaim(&self) {
        let state = self.auth.state();
        let now = now_ms();
        let lends = self.lends.lock().unwrap().clone();
        for (idx, (writer, epoch)) in lends {
            let gone = match state.leases.get(&writer) {
                Some(l) => l.epoch != epoch || l.released || l.expires_ms + RECLAIM_GRACE_MS < now,
                // Not visible here yet: a lend this node's view hasn't caught up with.
                None => false,
            };
            if gone && let Err(e) = self.take_back(idx).await {
                eprintln!("committer: taking back metaslab {}: {}", idx, e);
            }
        }
    }

    pub async fn volumes(&self) -> Result<Vec<VolumeInfo>, FileSystemError> {
        self.fs.list_volumes().await
    }

    pub async fn create_volume(&self, name: &str, size_bytes: u64) -> Result<(), FileSystemError> {
        let vbs = self.fs.default_volblocksize();
        self.fs
            .create_block(name, size_bytes, vbs)
            .await
            .map(|_| ())
    }

    fn in_use(&self, name: &str) -> Option<String> {
        let state = self.auth.state();
        state
            .leases
            .get(&volume_resource(name))
            .filter(|l| !l.released && l.expires_ms > now_ms())
            .map(|l| l.desc.clone())
    }

    pub async fn delete_volume(&self, name: &str) -> Result<(), FileSystemError> {
        if let Some(by) = self.in_use(name) {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is running ({}); disable or stop it first",
                name, by
            )));
        }
        self.fs.delete(std::path::Path::new(name)).await?;
        self.fs.close_volume(name);
        self.fs.sync().await
    }

    pub async fn set_props(&self, name: &str, props: VolumeProps) -> Result<(), FileSystemError> {
        self.fs.set_volume_props(name, props).await
    }
}

// ---- writers talking to a committer ----

/// A writer's sink for a committer in the same process (tests).
pub struct DirectSink<D: BlockDevice> {
    pub committer: Arc<Committer<D>>,
    pub writer: String,
    pub epoch: u64,
}

#[async_trait]
impl<D: BlockDevice + 'static> CommitSink for DirectSink<D> {
    fn writer(&self) -> (String, u64) {
        (self.writer.clone(), self.epoch)
    }
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError> {
        self.committer
            .lend(&self.writer, self.epoch, want_blocks)
            .await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError> {
        self.committer
            .give_back(&self.writer, self.epoch, idx)
            .await
    }
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError> {
        self.committer.commit(&c).await
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LendRequest {
    pub writer: String,
    pub writer_epoch: u64,
    pub want_blocks: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReturnRequest {
    pub writer: String,
    pub writer_epoch: u64,
    pub idx: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateVolume {
    pub name: String,
    pub size_bytes: u64,
}

/// Talks to whichever `listen` node is the committer.
#[derive(Clone)]
pub struct CommitClient {
    endpoints: Vec<String>,
    http: reqwest::Client,
    writer: Option<(String, u64)>,
    /// Set when the writer gives up (its lease is gone): stop resending.
    pub cancel: Arc<AtomicBool>,
}

const ROUND_BACKOFF: Duration = Duration::from_millis(500);

impl CommitClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        CommitClient {
            endpoints: endpoints
                .into_iter()
                .map(|e| e.trim_end_matches('/').to_string())
                .collect(),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("http client"),
            writer: None,
            cancel: Default::default(),
        }
    }

    /// The same client, committing as `writer` at `epoch`.
    pub fn as_writer(mut self, writer: &str, epoch: u64) -> Self {
        self.writer = Some((writer.to_string(), epoch));
        self
    }

    /// One pass over the endpoints: the first definite answer, else the
    /// reasons it couldn't get one.
    async fn once<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&(impl Serialize + Sync)>,
    ) -> Result<T, CommitError> {
        let mut why = Vec::new();
        for ep in &self.endpoints {
            let mut req = self.http.request(method.clone(), format!("{}{}", ep, path));
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send().await {
                Ok(resp) => match resp.json::<Result<T, CommitError>>().await {
                    Ok(Ok(v)) => return Ok(v),
                    Ok(Err(CommitError::Refused(m))) => return Err(CommitError::Refused(m)),
                    Ok(Err(CommitError::Retry(m))) => why.push(format!("{}: {}", ep, m)),
                    Err(e) => why.push(format!("{}: {}", ep, e)),
                },
                Err(e) => why.push(format!("{}: {}", ep, e)),
            }
        }
        Err(CommitError::Retry(if why.is_empty() {
            "no endpoints".into()
        } else {
            why.join("; ")
        }))
    }

    /// `once`, retried for `rounds` passes (None: until cancelled).
    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&(impl Serialize + Sync)>,
        rounds: Option<usize>,
    ) -> Result<T, CommitError> {
        let mut n = 0;
        loop {
            match self.once(method.clone(), path, body).await {
                Err(CommitError::Retry(m)) => {
                    n += 1;
                    if rounds.is_some_and(|r| n >= r) || self.cancel.load(Ordering::SeqCst) {
                        return Err(CommitError::Retry(m));
                    }
                    if n == 1 || n % 20 == 0 {
                        eprintln!("committer unavailable (will retry): {}", m);
                    }
                    tokio::time::sleep(ROUND_BACKOFF).await;
                }
                other => return other,
            }
        }
    }

    pub async fn committer(&self) -> Option<CommitterInfo> {
        self.once::<CommitterInfo>(reqwest::Method::GET, "/api/v1/pool/committer", None::<&()>)
            .await
            .ok()
    }

    pub async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/volumes",
            None::<&()>,
            Some(10),
        )
        .await
    }

    pub async fn create_volume(&self, name: &str, size_bytes: u64) -> Result<(), CommitError> {
        let body = CreateVolume {
            name: name.into(),
            size_bytes,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/volumes",
            Some(&body),
            Some(10),
        )
        .await
    }

    pub async fn delete_volume(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}", name);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    pub async fn set_props(&self, name: &str, props: &VolumeProps) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/props", name);
        self.call(reqwest::Method::PUT, &path, Some(props), Some(10))
            .await
    }
}

#[async_trait]
impl CommitSink for CommitClient {
    fn writer(&self) -> (String, u64) {
        self.writer.clone().unwrap_or_default()
    }
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError> {
        let (writer, writer_epoch) = self.writer();
        let body = LendRequest {
            writer,
            writer_epoch,
            want_blocks,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/lend",
            Some(&body),
            Some(20),
        )
        .await
    }
    async fn return_metaslab(&self, idx: u32) -> Result<(), CommitError> {
        let (writer, writer_epoch) = self.writer();
        let body = ReturnRequest {
            writer,
            writer_epoch,
            idx,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/return",
            Some(&body),
            Some(20),
        )
        .await
    }
    async fn commit(&self, c: VolumeCommit) -> Result<Committed, CommitError> {
        // A flush waits for the committer as long as it takes (a hard mount),
        // unless the writer gives up.
        self.call(reqwest::Method::POST, "/api/v1/pool/commit", Some(&c), None)
            .await
    }
}

// ---- HTTP routes, served by every `listen` node ----

/// This node's committer, while it is one.
pub type CommitterSlot = Arc<tokio::sync::RwLock<Option<Arc<Committer<crate::vdev::Pool>>>>>;

pub mod routes {
    use super::*;
    use rocket::serde::json::Json;
    use rocket::{State, delete, get, post, put};

    fn not_committer<T>() -> Json<Result<T, CommitError>> {
        Json(Err(CommitError::Retry(
            "this node isn't the committer".into(),
        )))
    }

    async fn current(slot: &CommitterSlot) -> Option<Arc<Committer<crate::vdev::Pool>>> {
        slot.read().await.clone()
    }

    fn fs_err<T>(r: Result<T, FileSystemError>) -> Json<Result<T, CommitError>> {
        Json(r.map_err(|e| CommitError::Refused(e.to_string())))
    }

    #[get("/api/v1/pool/committer")]
    pub async fn committer(
        slot: &State<CommitterSlot>,
    ) -> Json<Result<CommitterInfo, CommitError>> {
        match current(slot).await {
            Some(c) => Json(Ok(c.info())),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/lend", data = "<req>")]
    pub async fn lend(
        req: Json<LendRequest>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<LentMetaslab, CommitError>> {
        match current(slot).await {
            Some(c) => Json(c.lend(&req.writer, req.writer_epoch, req.want_blocks).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/return", data = "<req>")]
    pub async fn give_back(
        req: Json<ReturnRequest>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => Json(c.give_back(&req.writer, req.writer_epoch, req.idx).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/commit", data = "<req>")]
    pub async fn commit(
        req: Json<VolumeCommit>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Committed, CommitError>> {
        match current(slot).await {
            Some(c) => Json(c.commit(&req).await),
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/volumes")]
    pub async fn volumes(
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Vec<VolumeInfo>, CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.volumes().await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes", data = "<req>")]
    pub async fn create(
        req: Json<CreateVolume>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.create_volume(&req.name, req.size_bytes).await),
            None => not_committer(),
        }
    }

    #[delete("/api/v1/pool/volumes/<name>")]
    pub async fn remove(name: &str, slot: &State<CommitterSlot>) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.delete_volume(name).await),
            None => not_committer(),
        }
    }

    #[put("/api/v1/pool/volumes/<name>/props", data = "<props>")]
    pub async fn props(
        name: &str,
        props: Json<VolumeProps>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.set_props(name, props.into_inner()).await),
            None => not_committer(),
        }
    }

    pub fn routes() -> Vec<rocket::Route> {
        rocket::routes![
            committer, lend, give_back, commit, volumes, create, remove, props
        ]
    }
}

// ---- the committer service in `listen` ----

/// How `listen` opens the pool as committer (or writer): the pool spec
/// with this node's own disks reached through its own disk server, so
/// every write is fenced the same way.
#[derive(Clone)]
pub struct PoolAccess {
    pub spec: crate::config::PoolSpec,
    pub opts: crate::disk::DiskOptions,
}

impl PoolAccess {
    pub async fn open(&self) -> crate::vdev::Pool {
        self.spec.open(self.opts).await
    }
}

/// Contend for the `pool` lease forever; while holding it, be the committer.
pub async fn run(
    slot: CommitterSlot,
    cluster: Arc<Cluster>,
    access: PoolAccess,
    leases: LeaseClient,
    node: String,
) {
    loop {
        let held =
            match HeldLease::acquire(leases.clone(), POOL, &format!("committer {}", node)).await {
                Ok(h) => Arc::new(h),
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
        let pool = access.open().await;
        pool.disks.fence.set_pool_epoch(held.epoch);
        let fs = match FileSystem::load(pool).await {
            Ok(fs) => Arc::new(fs),
            Err(e) => {
                // Not formatted yet (or unreadable): let `format` have the lease.
                eprintln!("committer: pool not available ({}); retrying", e);
                held.release().await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let lost = Arc::new(AtomicBool::new(false));
        let flag = lost.clone();
        held.hold(move |msg| {
            eprintln!("committer: {}; stepping down", msg);
            flag.store(true, Ordering::SeqCst);
        });
        let committer = Arc::new(
            Committer::new(
                fs.clone(),
                cluster.clone(),
                held.owner.clone(),
                held.epoch,
                node.clone(),
            )
            .await,
        );
        println!(
            "committer: this node commits the pool (epoch {})",
            held.epoch
        );
        crate::scan::spawn_health_monitor(fs.clone(), Duration::from_secs(30));
        *slot.write().await = Some(committer.clone());
        while !lost.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(2)).await;
            committer.reclaim().await;
        }
        *slot.write().await = None;
    }
}

#[cfg(test)]
#[path = "tests/multinode_tests.rs"]
mod tests;
