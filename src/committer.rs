//! The committer: the `pool` lease holder in a multi-writer pool (see
//! `crate::writer`). It lends metaslabs to writers, applies their commits,
//! and does the pool's metadata operations (create and delete datasets,
//! change their settings, snapshots). `listen` runs one on whichever node wins the `pool`
//! lease; the others keep trying, so it fails over.

use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::cluster::state::{ClusterState, POOL, Response};
use crate::cluster::{Cluster, LeaseOp, now_ms};
use crate::dataset::DatasetInfo;
use crate::disk::BlockDevice;
use crate::events::{EventLog, Held, Level, NewEvent, Observed};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::DatasetProps;
use crate::send::StreamReport;
use crate::snapshot::SnapshotInfo;
use crate::volume::VolumeInfo;
use crate::writer::{
    CommitError, CommitSink, Committed, DatasetCommit, FsCommit, FsCommitted, LentMetaslab,
};
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
    /// The Raft leader's id.
    fn leader(&self) -> Option<u64> {
        None
    }
    /// Which node serves each pool disk (by name).
    fn disk_nodes(&self) -> std::collections::BTreeMap<usize, String> {
        Default::default()
    }
}

#[async_trait]
impl Authority for Cluster {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        self.lease(&op, false).await
    }
    fn state(&self) -> ClusterState {
        self.status().state
    }
    fn leader(&self) -> Option<u64> {
        self.status().leader
    }
    fn disk_nodes(&self) -> std::collections::BTreeMap<usize, String> {
        self.members
            .disks
            .iter()
            .flat_map(|(id, disks)| {
                disks
                    .iter()
                    .map(move |d| (*d, crate::cluster::members::node_name(*id)))
            })
            .collect()
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
        // Taken over since, or this very grant released / someone else's.
        Some(l)
            if l.epoch > epoch
                || (l.epoch == epoch && (l.released || owner.is_some_and(|o| o != l.owner))) =>
        {
            Err(CommitError::Refused(format!(
                "{} lease lost (epoch {})",
                resource, epoch
            )))
        }
        // An older grant (whoever's, released or not): this view hasn't
        // caught up with this one yet. Or it lapsed but may be renewed.
        _ => Err(CommitError::Retry(format!("{} lease not live", resource))),
    }
}

fn reserved(name: &str) -> FileSystemError {
    FileSystemError::InvalidArgument(format!("'{}' is the pool's own", name))
}

/// Names starting with `.` are the pool's (see `crate::events`).
fn check_not_reserved(name: &str) -> Result<(), FileSystemError> {
    if name.starts_with('.') {
        return Err(FileSystemError::InvalidArgument(format!(
            "bad name '{}': names starting with '.' are reserved",
            name
        )));
    }
    Ok(())
}

/// A settings change, described ("disabled, moving to node-2"); empty if
/// nothing an admin would care about changed (a node clearing a move it
/// made).
pub fn describe_props(old: &DatasetProps, new: &DatasetProps) -> String {
    let mut out = Vec::new();
    if old.enabled != new.enabled {
        out.push(if new.enabled { "enabled" } else { "disabled" }.to_string());
    }
    if new.move_to.is_some() && new.move_to != old.move_to {
        out.push(format!(
            "moving to {}",
            new.move_to.as_deref().unwrap_or("")
        ));
    }
    if old.nodes != new.nodes {
        out.push(if new.nodes.is_empty() {
            "may run on any node".to_string()
        } else {
            format!("nodes {}", new.nodes.join(", "))
        });
    }
    if old.automount != new.automount {
        out.push(match new.automount {
            Some(t) => format!("automount {}", t),
            None => "automount off".to_string(),
        });
    }
    if old.port != new.port {
        out.push(match new.port {
            Some(p) => format!("port {}", p),
            None => "no port".to_string(),
        });
    }
    out.join(", ")
}

/// `Disk 2 (node1)`, from a pool disk's path (`https://node1:7000/...`);
/// the committer reaches its own node's disks over loopback (`me`).
fn disk_label(d: usize, path: &str, me: &str) -> String {
    match path.split_once("://") {
        Some((_, rest)) => {
            let host = match rest.split([':', '/']).next().unwrap_or(rest) {
                "127.0.0.1" | "localhost" | "[" => me,
                h => h,
            };
            format!("Disk {} ({})", d, host)
        }
        None => format!("Disk {} ({})", d, me),
    }
}

pub fn dataset_resource(name: &str) -> String {
    format!("ds/{}", name)
}

/// The lease held on a file while it is open for writing.
pub fn object_resource(id: crate::disk::FileId) -> String {
    format!("obj/{}", id)
}

/// May `c` change object `id`? With an `obj/<id>` lease: only if it's
/// held. Without one: only if no one else holds it.
fn object_lease_ok(
    state: &ClusterState,
    c: &FsCommit,
    id: crate::disk::FileId,
) -> Result<(), CommitError> {
    let resource = object_resource(id);
    if let Some(l) = &c.lease {
        return check_lease(state, &resource, Some(&l.owner), l.epoch);
    }
    match state.leases.get(&resource) {
        Some(l) if !l.released && l.expires_ms > now_ms() => Err(CommitError::Refused(format!(
            "object {} is open for writing ({})",
            id, l.desc
        ))),
        _ => Ok(()),
    }
}

/// Where an intent log's `ds/<name>` lease stands, for replaying it.
#[derive(Debug, PartialEq, Eq)]
enum Orphan {
    /// Held (or this view hasn't caught up with its grant yet): keep it.
    Live,
    /// Taken over or released: nothing more can be acknowledged; replay.
    Gone,
    /// Expired, but its holder could still renew it: fence it, then replay.
    Lapsed,
}

/// Like `check_lease`: an older epoch in this view means the view lags (a
/// committer on a Raft follower, or just after a failover), not that the
/// log's holder is gone.
fn orphan_state(
    lease: Option<&crate::cluster::state::Lease>,
    l: &crate::inode::DatasetLog,
    now: u64,
) -> Orphan {
    match lease {
        None => Orphan::Live,
        Some(lease) if lease.epoch < l.ds_epoch => Orphan::Live,
        Some(lease) if lease.epoch > l.ds_epoch => Orphan::Gone,
        Some(lease) if lease.owner != l.ds_owner || lease.released => Orphan::Gone,
        Some(lease) if lease.expires_ms + RECLAIM_GRACE_MS < now => Orphan::Lapsed,
        Some(_) => Orphan::Live,
    }
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
    /// The activity log (opened, or made, on first use).
    events: tokio::sync::OnceCell<Arc<EventLog<D>>>,
    /// The VM definitions (opened, or made, on first use).
    vms: tokio::sync::OnceCell<Arc<crate::vm_store::VmStore<D>>>,
    /// What `observe` last saw (outer None: not read from the log yet).
    seen: tokio::sync::Mutex<Option<Option<Observed>>>,
    /// Which pool disks were online at the last `observe` (in memory: a
    /// new committer reports the ones offline once).
    disks_online: std::sync::Mutex<Option<Vec<bool>>>,
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
        // Writers' views of the TXG may lag a previous committer's: from
        // here on, blocks are born after any snapshot it took.
        if let Ok(t) = fs.open_txg().await {
            fs.raise_birth_floor(t);
        }
        Committer {
            fs,
            auth,
            owner,
            epoch,
            node,
            lends: std::sync::Mutex::new(lends),
            events: Default::default(),
            vms: Default::default(),
            seen: Default::default(),
            disks_online: Default::default(),
        }
    }

    // ---- the activity log (see `crate::events`) ----

    /// The pool's activity log.
    pub async fn events(&self) -> Result<Arc<EventLog<D>>, FileSystemError> {
        self.events
            .get_or_try_init(|| EventLog::open(self.fs.clone()))
            .await
            .cloned()
    }

    /// The pool's VM definitions (see `crate::vm_store`).
    pub async fn vm_store(&self) -> Result<Arc<crate::vm_store::VmStore<D>>, FileSystemError> {
        self.vms
            .get_or_try_init(|| crate::vm_store::VmStore::open(self.fs.clone()))
            .await
            .cloned()
    }

    /// Add events reported by nodes.
    pub async fn record(&self, events: Vec<NewEvent>) -> Result<Vec<u64>, FileSystemError> {
        self.events().await?.append(events, None).await
    }

    /// Record something that happened here (a change asked of the
    /// committer, by `by`). A log that can't be written isn't the
    /// change's failure: it's only reported.
    pub async fn note(
        &self,
        level: Level,
        kind: &str,
        subject: String,
        message: String,
        by: Option<String>,
    ) {
        let e = NewEvent {
            ts_ms: now_ms(),
            node: self.node.clone(),
            level,
            kind: kind.into(),
            subject: Some(subject),
            message,
            by,
        };
        if let Err(err) = self.record(vec![e]).await {
            eprintln!("committer: recording an event: {}", err);
        }
    }

    /// Look at the cluster and the pool's disks, and record what changed
    /// since the last look (see `crate::events::diff`).
    pub async fn observe(&self) -> Result<(), FileSystemError> {
        let log = self.events().await?;
        let mut seen = self.seen.lock().await;
        if seen.is_none() {
            *seen = Some(log.seen().await?);
        }
        let state = self.auth.state();
        let now = now_ms();
        let me = Held {
            node: self.node.clone(),
            epoch: self.epoch,
        };
        let new = crate::events::observe(&state, now, me, self.auth.leader());
        let old = seen.as_ref().and_then(|s| s.as_ref());
        let mut noticed = Vec::new();
        if old != Some(&new) {
            let formats: std::collections::HashMap<String, crate::dataset::DatasetFormat> = self
                .fs
                .datasets()
                .await?
                .datasets
                .into_iter()
                .map(|(n, e)| (n, e.format))
                .collect();
            let label = |d: &str| {
                formats
                    .get(d)
                    .map_or("Dataset", |f| crate::events::format_label(*f))
                    .to_string()
            };
            noticed = crate::events::diff(old, &new, &state, &label);
        }
        noticed.extend(self.disk_changes());
        let changed = old != Some(&new);
        if noticed.is_empty() && !changed {
            return Ok(());
        }
        let events = noticed
            .into_iter()
            .map(|(level, kind, subject, message)| NewEvent {
                ts_ms: now,
                node: self.node.clone(),
                level,
                kind: kind.into(),
                subject: Some(subject),
                message,
                by: None,
            })
            .collect();
        log.append(events, changed.then_some(&new)).await?;
        *seen = Some(Some(new));
        Ok(())
    }

    /// Pool disks that went offline or came back since the last look.
    fn disk_changes(&self) -> Vec<crate::events::Noticed> {
        let Some(pool) = self.fs.dev.pool() else {
            return Vec::new();
        };
        let now: Vec<bool> = (0..pool.disks.len())
            .map(|d| pool.disks.is_online(d))
            .collect();
        let nodes = self.auth.disk_nodes();
        let mut last = self.disks_online.lock().unwrap();
        let mut out = Vec::new();
        for (d, &online) in now.iter().enumerate() {
            let before = last.as_ref().and_then(|l| l.get(d).copied());
            let name = match nodes.get(&d) {
                Some(n) => format!("Disk {} ({})", d, n),
                None => disk_label(d, pool.disks.disks[d].path(), &self.node),
            };
            match (before, online) {
                (Some(true), false) | (None, false) => out.push((
                    Level::Error,
                    "disk.offline",
                    format!("disk/{}", d),
                    format!("{} is offline", name),
                )),
                (Some(false), true) => out.push((
                    Level::Info,
                    "disk.online",
                    format!("disk/{}", d),
                    format!("{} is back online", name),
                )),
                _ => {}
            }
        }
        *last = Some(now);
        out
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

    pub async fn commit(&self, c: &DatasetCommit) -> Result<Committed, CommitError> {
        let state = self.auth.state();
        check_lease(&state, &c.writer, None, c.writer_epoch)?;
        check_lease(
            &state,
            &dataset_resource(&c.dataset),
            Some(&c.ds_owner),
            c.ds_epoch,
        )?;
        let lends = self.lends.lock().unwrap().clone();
        let txg = self
            .fs
            .apply_dataset_commit(c, |idx| {
                lends
                    .get(&idx)
                    .is_some_and(|(w, e)| *w == c.writer && *e == c.writer_epoch)
            })
            .await?;
        Ok(Committed { txg })
    }

    /// Apply a writer's file or directory change.
    pub async fn commit_fs(&self, c: &FsCommit) -> Result<FsCommitted, CommitError> {
        let state = self.auth.state();
        check_lease(&state, &c.writer, None, c.writer_epoch)?;
        let lends = self.lends.lock().unwrap().clone();
        self.fs
            .apply_fs_commit(
                c,
                |idx| {
                    lends
                        .get(&idx)
                        .is_some_and(|(w, e)| *w == c.writer && *e == c.writer_epoch)
                },
                |id| object_lease_ok(&state, c, id),
            )
            .await
    }

    /// Get `volume` ready for the holder of its `ds/<name>` lease: replay
    /// whatever intent log an earlier holder left (it can't add to it any
    /// more: its lease is gone).
    pub async fn claim_dataset(
        &self,
        volume: &str,
        ds_owner: &str,
        ds_epoch: u64,
    ) -> Result<(), CommitError> {
        check_lease(
            &self.auth.state(),
            &dataset_resource(volume),
            Some(ds_owner),
            ds_epoch,
        )?;
        let vol = self
            .fs
            .open_dataset(volume)
            .await
            .map_err(|e| CommitError::Refused(format!("volume '{}': {}", volume, e)))?;
        let log = vol.log.lock().unwrap().clone();
        if let Some(l) = log
            && (l.ds_owner != ds_owner || l.ds_epoch != ds_epoch)
        {
            self.fs
                .replay_dataset_log(volume, &l)
                .await
                .map_err(|e| CommitError::Retry(format!("replaying '{}': {}", volume, e)))?;
        }
        Ok(())
    }

    /// Replay the intent logs of volumes whose writer's lease is gone.
    async fn replay_orphaned_logs(&self) {
        let Ok(logs) = self.fs.dataset_logs().await else {
            return;
        };
        let state = self.auth.state();
        let now = now_ms();
        for (name, l) in logs {
            let resource = dataset_resource(&name);
            let fence = match orphan_state(state.leases.get(&resource), &l, now) {
                Orphan::Live => continue,
                Orphan::Gone => None,
                Orphan::Lapsed => match self.fence_lapsed(&resource, &l).await {
                    Some(f) => Some(f),
                    None => continue,
                },
            };
            if let Err(e) = self.fs.replay_dataset_log(&name, &l).await {
                eprintln!("committer: replaying the log of '{}': {}", name, e);
            }
            // The log's holder is superseded either way (a failed replay is
            // retried next time as `Gone`); let the next node have it.
            if let Some(epoch) = fence {
                let op = LeaseOp::Release {
                    resource: resource.clone(),
                    owner: self.replay_owner(),
                    epoch,
                };
                if let Err(e) = self.auth.propose(op).await {
                    eprintln!("committer: releasing {}: {}", resource, e);
                }
            }
        }
    }

    /// Who holds a `ds/<name>` lease while the committer replays its log.
    fn replay_owner(&self) -> String {
        format!("{} (log replay)", self.owner)
    }

    /// Take over a lapsed `ds/<name>` lease, so its holder can't renew it
    /// (a lapsed lease can be renewed until someone else is granted it) and
    /// go on acknowledging flushes into a log that's been replayed. Returns
    /// the epoch granted, or None if the lease is live again.
    async fn fence_lapsed(&self, resource: &str, l: &crate::inode::DatasetLog) -> Option<u64> {
        let op = LeaseOp::Acquire {
            resource: resource.to_string(),
            owner: self.replay_owner(),
            desc: format!("committer: replaying {}'s intent log", l.writer),
            ttl_ms: crate::cluster::lease::LEASE_TTL.as_millis() as u64,
        };
        match self.auth.propose(op).await {
            // A newer epoch than the log's: its holder's renewals fail from
            // now on, and its last one was sent over a TTL ago (the lease
            // had expired when this was granted), so it can't be
            // acknowledging anything any more.
            Ok(Response::Granted { epoch, .. }) if epoch > l.ds_epoch => Some(epoch),
            Ok(_) => None,
            Err(e) => {
                eprintln!("committer: fencing {}: {}", resource, e);
                None
            }
        }
    }

    /// Take back metaslabs whose writer is gone (its lease taken over,
    /// released, or expired), after replaying any intent log it left.
    pub async fn reclaim(&self) {
        self.replay_orphaned_logs().await;
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
        check_not_reserved(name)?;
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
            .get(&dataset_resource(name))
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
        if let Some(vm) = self.vm_store().await?.user_of(name).await? {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is VM '{}''s disk; delete the VM (or take the disk out of it) first",
                name, vm
            )));
        }
        self.fs.delete_volume(name).await?;
        self.fs.sync().await
    }

    pub async fn set_props(&self, name: &str, props: DatasetProps) -> Result<(), FileSystemError> {
        if let Ok(e) = self.fs.dataset_entry(name).await {
            if e.format.is_system() {
                return Err(reserved(name));
            }
            if e.format == crate::dataset::DatasetFormat::Etcd {
                self.check_etcd_port(name, &props).await?;
            }
        }
        self.fs.set_dataset_props(name, props).await
    }

    /// Dataset `name`'s kind ("Volume"...) and settings, for describing a
    /// change to them.
    pub async fn props_of(&self, name: &str) -> Option<(&'static str, DatasetProps)> {
        let e = self.fs.dataset_entry(name).await.ok()?;
        let d = self.fs.read_dnode(e.root).await.ok()?;
        Some((
            crate::events::format_label(e.format),
            d.props.unwrap_or_default(),
        ))
    }

    // ---- other datasets: kv, and the services (etcd, s3, queue) ----

    pub async fn datasets(&self) -> Result<Vec<DatasetInfo>, FileSystemError> {
        self.fs.list_btree_datasets().await
    }

    /// An etcd dataset needs a port of its own.
    async fn check_etcd_port(
        &self,
        name: &str,
        props: &DatasetProps,
    ) -> Result<(), FileSystemError> {
        let Some(port) = props.port else {
            return Err(FileSystemError::InvalidArgument(
                "an etcd dataset needs a port (its clients' port on every node)".into(),
            ));
        };
        for d in self.fs.list_btree_datasets().await? {
            if d.name != name
                && d.format == crate::dataset::DatasetFormat::Etcd
                && d.props.port == Some(port)
            {
                return Err(FileSystemError::InvalidArgument(format!(
                    "port {} is etcd dataset '{}'s",
                    port, d.name
                )));
            }
        }
        Ok(())
    }

    pub async fn create_dataset(&self, req: &CreateDataset) -> Result<(), FileSystemError> {
        use crate::dataset::DatasetFormat;
        check_not_reserved(&req.name)?;
        match req.format {
            DatasetFormat::Kv => {
                self.fs.create_kv(&req.name).await?;
                return self
                    .fs
                    .set_dataset_props(&req.name, req.props.clone())
                    .await;
            }
            DatasetFormat::Etcd => self.check_etcd_port(&req.name, &req.props).await?,
            DatasetFormat::S3 => check_bucket_name(&req.name)?,
            DatasetFormat::Queue => check_queue_name(&req.name)?,
            f => {
                return Err(FileSystemError::InvalidArgument(format!(
                    "{} datasets are made otherwise",
                    f.name()
                )));
            }
        }
        self.fs
            .create_service(&req.name, req.format, req.props.clone())
            .await
            .map(|_| ())
    }

    /// Delete dataset `name` (not a volume: see `delete_volume`).
    pub async fn delete_dataset(&self, name: &str) -> Result<(), FileSystemError> {
        let e = self.fs.dataset_entry(name).await?;
        if e.format.is_system() {
            return Err(reserved(name));
        }
        if !e.format.is_btree() {
            return Err(FileSystemError::InvalidArgument(format!(
                "'{}' is a {} dataset",
                name,
                e.format.name()
            )));
        }
        if let Some(by) = self.in_use(name) {
            return Err(FileSystemError::InvalidArgument(format!(
                "dataset '{}' is running ({}); disable it first",
                name, by
            )));
        }
        self.fs.delete_dataset(name).await?;
        self.fs.sync().await
    }

    // ---- snapshots (see `crate::snapshot`) ----

    pub async fn snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, FileSystemError> {
        self.fs.list_snapshots(name).await
    }

    /// Snapshot `name` as last committed (a running volume's writes since
    /// its writer's last commit aren't in it). Returns its TXG.
    pub async fn create_snapshot(&self, name: &str, snap: &str) -> Result<u64, FileSystemError> {
        self.fs.create_snapshot(name, snap).await
    }

    pub async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), FileSystemError> {
        self.fs.destroy_snapshot(name, snap).await
    }

    pub async fn clone_snapshot(
        &self,
        name: &str,
        snap: &str,
        new: &str,
    ) -> Result<(), FileSystemError> {
        self.fs.clone_snapshot(name, snap, new).await.map(|_| ())
    }

    /// Receive a send stream into volume `name` (see `crate::send`); it
    /// must not be running.
    pub async fn recv<R: tokio::io::AsyncRead + Unpin + Send>(
        &self,
        name: &str,
        r: &mut R,
        force: bool,
    ) -> Result<StreamReport, FileSystemError> {
        if let Some(by) = self.in_use(name) {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is running ({}); disable or stop it first",
                name, by
            )));
        }
        self.fs.recv_snapshot(name, r, force).await
    }

    /// Whether a stream with `header` would be received into `name`.
    pub async fn recv_check(
        &self,
        name: &str,
        header: &crate::send::SendHeader,
        force: bool,
    ) -> Result<(), FileSystemError> {
        if let Some(by) = self.in_use(name) {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is running ({}); disable or stop it first",
                name, by
            )));
        }
        self.fs.recv_check(name, header, force).await
    }

    /// Roll `name` back to its newest snapshot (it must not be running).
    pub async fn rollback(&self, name: &str) -> Result<(), FileSystemError> {
        if let Some(by) = self.in_use(name) {
            return Err(FileSystemError::InvalidArgument(format!(
                "volume '{}' is running ({}); disable or stop it first",
                name, by
            )));
        }
        self.fs.rollback_volume(name).await
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
    async fn commit(&self, c: DatasetCommit) -> Result<Committed, CommitError> {
        self.committer.commit(&c).await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        self.committer.commit_fs(&c).await
    }
    async fn claim_dataset(
        &self,
        volume: &str,
        ds_owner: &str,
        ds_epoch: u64,
    ) -> Result<(), CommitError> {
        self.committer
            .claim_dataset(volume, ds_owner, ds_epoch)
            .await
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
pub struct ClaimRequest {
    pub dataset: String,
    pub ds_owner: String,
    pub ds_epoch: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateVolume {
    pub name: String,
    pub size_bytes: u64,
}

/// A dataset to make (see `Committer::create_dataset`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateDataset {
    pub name: String,
    pub format: crate::dataset::DatasetFormat,
    #[serde(default)]
    pub props: DatasetProps,
}

/// S3's rules for bucket names (those that work as host names).
pub fn check_bucket_name(name: &str) -> Result<(), FileSystemError> {
    let ok = (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name.as_bytes()[name.len() - 1].is_ascii_alphanumeric()
        && !name.contains("..")
        && name.parse::<std::net::Ipv4Addr>().is_err();
    if !ok {
        return Err(FileSystemError::InvalidArgument(format!(
            "bad bucket name '{}' (3 to 63 lowercase letters, digits, '.' and '-')",
            name
        )));
    }
    Ok(())
}

/// SQS's rules for queue names.
pub fn check_queue_name(name: &str) -> Result<(), FileSystemError> {
    if name.ends_with(".fifo") {
        return Err(FileSystemError::InvalidArgument(
            "FIFO queues aren't supported".into(),
        ));
    }
    let ok = (1..=80).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !ok {
        return Err(FileSystemError::InvalidArgument(format!(
            "bad queue name '{}' (1 to 80 letters, digits, '-' and '_')",
            name
        )));
    }
    Ok(())
}

/// A snapshot or clone to make.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NewName {
    pub name: String,
}

/// A VM definition to store (`PUT /api/v1/pool/vms`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PutVm {
    pub name: String,
    pub xml: String,
    /// Refuse if there is one.
    #[serde(default)]
    pub create: bool,
    /// Refuse unless the one there is this change.
    #[serde(default)]
    pub change: Option<u64>,
}

/// Talks to whichever `listen` node is the committer.
#[derive(Clone)]
pub struct CommitClient {
    endpoints: Vec<String>,
    http: reqwest::Client,
    writer: Option<(String, u64)>,
    /// The admin this node asks for (see `crate::auth::Actor`).
    on_behalf: Option<String>,
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
            http: crate::tls::http_builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("http client"),
            writer: None,
            on_behalf: None,
            cancel: Default::default(),
        }
    }

    /// The same client, asking on behalf of `who` (an admin's request
    /// passed on: the activity log says it was theirs).
    pub fn on_behalf(&self, who: &crate::auth::Principal) -> Self {
        let mut c = self.clone();
        c.on_behalf = Some(who.name.clone());
        c
    }

    /// Report events (to whichever node is the committer), one try.
    pub async fn report_events(&self, events: &[NewEvent]) -> Result<Vec<u64>, CommitError> {
        self.once(reqwest::Method::POST, "/api/v1/pool/events", Some(&events))
            .await
    }

    /// A page of the activity log.
    pub async fn events(
        &self,
        q: &crate::events::EventQuery,
    ) -> Result<crate::events::EventPage, CommitError> {
        let mut qs = Vec::new();
        if let Some(a) = q.after {
            qs.push(format!("after={}", a));
        }
        if let Some(b) = q.before {
            qs.push(format!("before={}", b));
        }
        if let Some(l) = q.limit {
            qs.push(format!("limit={}", l));
        }
        let path = format!("/api/v1/pool/events?{}", qs.join("&"));
        self.once(reqwest::Method::GET, &path, None::<&()>).await
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
            if let Some(who) = &self.on_behalf {
                req = req.header(crate::auth::ON_BEHALF, who);
            }
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send().await {
                Ok(resp) => match resp.json::<Result<T, CommitError>>().await {
                    Ok(Ok(v)) => return Ok(v),
                    Ok(Err(CommitError::Refused(m))) => return Err(CommitError::Refused(m)),
                    Ok(Err(CommitError::Retry(m))) => why.push(format!("{}: {}", ep, m)),
                    Ok(Err(e)) => return Err(e),
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

    /// Every VM definition.
    pub async fn vms(
        &self,
    ) -> Result<std::collections::BTreeMap<String, crate::vm_store::VmDef>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/vms",
            None::<&()>,
            Some(10),
        )
        .await
    }

    /// Store VM `req.name`'s definition (see `crate::vm_store::VmStore::put`).
    pub async fn put_vm(&self, req: &PutVm) -> Result<crate::vm_store::VmDef, CommitError> {
        self.call(
            reqwest::Method::PUT,
            "/api/v1/pool/vms",
            Some(req),
            Some(10),
        )
        .await
    }

    /// Delete VM `name`'s definition (`change`: only if it's that one).
    pub async fn delete_vm(&self, name: &str, change: Option<u64>) -> Result<(), CommitError> {
        let mut path = format!("/api/v1/pool/vms/{}", name);
        if let Some(c) = change {
            path.push_str(&format!("?change={}", c));
        }
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    pub async fn datasets(&self) -> Result<Vec<DatasetInfo>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/datasets",
            None::<&()>,
            Some(10),
        )
        .await
    }

    pub async fn create_dataset(&self, req: &CreateDataset) -> Result<(), CommitError> {
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/datasets",
            Some(req),
            Some(10),
        )
        .await
    }

    pub async fn delete_dataset(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/datasets/{}", name);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    pub async fn set_props(&self, name: &str, props: &DatasetProps) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/props", name);
        self.call(reqwest::Method::PUT, &path, Some(props), Some(10))
            .await
    }

    pub async fn snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots", name);
        self.call(reqwest::Method::GET, &path, None::<&()>, Some(10))
            .await
    }

    pub async fn create_snapshot(&self, name: &str, snap: &str) -> Result<u64, CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots", name);
        let body = NewName { name: snap.into() };
        self.call(reqwest::Method::POST, &path, Some(&body), Some(10))
            .await
    }

    pub async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots/{}", name, snap);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    pub async fn clone_snapshot(
        &self,
        name: &str,
        snap: &str,
        new: &str,
    ) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots/{}/clone", name, snap);
        let body = NewName { name: new.into() };
        self.call(reqwest::Method::POST, &path, Some(&body), Some(10))
            .await
    }

    /// Stream `r` (a send stream) to the committer, received into volume
    /// `name`. Not retried: a stream can't be sent twice.
    pub async fn recv<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        name: &str,
        force: bool,
        mut r: R,
    ) -> Result<StreamReport, CommitError> {
        use tokio::io::AsyncReadExt;
        // Asked first, so a refusal comes back as such (not as a stream
        // cut off midway).
        let (header, raw) = crate::send::peek_header(&mut r)
            .await
            .map_err(|e| CommitError::Refused(e.to_string()))?;
        let check = format!("/api/v1/pool/volumes/{}/recv/check?force={}", name, force);
        self.call::<()>(reqwest::Method::POST, &check, Some(&header), Some(10))
            .await?;
        let r = std::io::Cursor::new(raw).chain(r);
        // Find the committer first: the stream goes to it alone.
        let mut found = None;
        for ep in &self.endpoints {
            let url = format!("{}/api/v1/pool/committer", ep);
            if let Ok(resp) = self.http.get(&url).send().await
                && let Ok(Ok(_)) = resp.json::<Result<CommitterInfo, CommitError>>().await
            {
                found = Some(ep.clone());
                break;
            }
        }
        let ep = found.ok_or_else(|| CommitError::Retry("no committer found".into()))?;
        let chunks = futures::stream::unfold(Some(r), |r| async move {
            let mut r = r?;
            let mut buf = vec![0u8; 1 << 20];
            match r.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok::<_, std::io::Error>(buf), Some(r)))
                }
                Err(e) => Some((Err(e), None)),
            }
        });
        // No overall timeout: a stream takes as long as it takes.
        let http = crate::tls::http_builder()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .map_err(|e| CommitError::Retry(e.to_string()))?;
        let url = format!("{}/api/v1/pool/volumes/{}/recv?force={}", ep, name, force);
        let resp = http
            .post(url)
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await
            .map_err(|e| CommitError::Retry(format!("{}: {}", ep, e)))?;
        resp.json::<Result<StreamReport, CommitError>>()
            .await
            .map_err(|e| CommitError::Retry(format!("{}: {}", ep, e)))?
    }

    pub async fn rollback(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/rollback", name);
        self.call(reqwest::Method::POST, &path, None::<&()>, Some(10))
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
    async fn commit(&self, c: DatasetCommit) -> Result<Committed, CommitError> {
        // A flush waits for the committer as long as it takes (a hard mount),
        // unless the writer gives up.
        self.call(reqwest::Method::POST, "/api/v1/pool/commit", Some(&c), None)
            .await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        // A namespace op is a single call from a client: give up after a
        // while instead of hanging it (the outcome is then unknown, and a
        // resend is acknowledged if it was applied).
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/commit_fs",
            Some(&c),
            Some(20),
        )
        .await
    }
    async fn claim_dataset(
        &self,
        volume: &str,
        ds_owner: &str,
        ds_epoch: u64,
    ) -> Result<(), CommitError> {
        let body = ClaimRequest {
            dataset: volume.into(),
            ds_owner: ds_owner.into(),
            ds_epoch,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/claim",
            Some(&body),
            Some(20),
        )
        .await
    }
}

// ---- HTTP routes, served by every `listen` node ----

/// This node's committer, while it is one.
pub type CommitterSlot = Arc<tokio::sync::RwLock<Option<Arc<Committer<crate::vdev::Pool>>>>>;

pub mod routes {
    use super::*;
    use crate::auth::Actor;
    use crate::events::EventQuery;
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

    /// `volume/<name>` (or whatever kind it is).
    async fn subject_of(c: &Committer<crate::vdev::Pool>, name: &str) -> String {
        let label = c.props_of(name).await.map_or("Dataset", |p| p.0);
        format!("{}/{}", label.to_lowercase(), name)
    }

    #[get("/api/v1/pool/committer")]
    pub async fn committer(
        _auth: crate::auth::CanRead,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<CommitterInfo, CommitError>> {
        match current(slot).await {
            Some(c) => Json(Ok(c.info())),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/lend", data = "<req>")]
    pub async fn lend(
        _auth: crate::auth::CanWrite,
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
        _auth: crate::auth::CanWrite,
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
        _auth: crate::auth::CanWrite,
        req: Json<DatasetCommit>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Committed, CommitError>> {
        match current(slot).await {
            Some(c) => Json(c.commit(&req).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/commit_fs", data = "<req>")]
    pub async fn commit_fs(
        _auth: crate::auth::CanWrite,
        req: Json<FsCommit>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<FsCommitted, CommitError>> {
        match current(slot).await {
            Some(c) => Json(c.commit_fs(&req).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/claim", data = "<req>")]
    pub async fn claim(
        _auth: crate::auth::CanWrite,
        req: Json<ClaimRequest>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => Json(
                c.claim_dataset(&req.dataset, &req.ds_owner, req.ds_epoch)
                    .await,
            ),
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/volumes")]
    pub async fn volumes(
        _auth: crate::auth::CanRead,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Vec<VolumeInfo>, CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.volumes().await),
            None => not_committer(),
        }
    }

    /// Record `message` about `subject` if `r` succeeded.
    async fn noted<T>(
        c: &Committer<crate::vdev::Pool>,
        r: Result<T, FileSystemError>,
        by: Actor,
        kind: &str,
        subject: String,
        message: impl FnOnce(&T) -> String,
    ) -> Json<Result<T, CommitError>> {
        if let Ok(v) = &r {
            let m = message(v);
            c.note(Level::Info, kind, subject, m, by.0).await;
        }
        fs_err(r)
    }

    #[post("/api/v1/pool/volumes", data = "<req>")]
    pub async fn create(
        _auth: crate::auth::CanWrite,
        by: Actor,
        req: Json<CreateVolume>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.create_volume(&req.name, req.size_bytes).await;
                noted(
                    &c,
                    r,
                    by,
                    "volume.created",
                    format!("volume/{}", req.name),
                    |_| {
                        format!(
                            "Volume '{}' created ({})",
                            req.name,
                            crate::events::bytes(req.size_bytes)
                        )
                    },
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[delete("/api/v1/pool/volumes/<name>")]
    pub async fn remove(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.delete_volume(name).await;
                noted(
                    &c,
                    r,
                    by,
                    "volume.deleted",
                    format!("volume/{}", name),
                    |_| format!("Volume '{}' deleted", name),
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[put("/api/v1/pool/volumes/<name>/props", data = "<props>")]
    pub async fn props(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        props: Json<DatasetProps>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let before = c.props_of(name).await;
                let new = props.into_inner();
                let r = c.set_props(name, new.clone()).await;
                let what = match &before {
                    Some((label, old)) => {
                        let d = describe_props(old, &new);
                        (!d.is_empty()).then_some((*label, d))
                    }
                    None => None,
                };
                match what {
                    Some((label, d)) => {
                        noted(
                            &c,
                            r,
                            by,
                            "dataset.settings",
                            format!("{}/{}", label.to_lowercase(), name),
                            |_| format!("{} '{}': {}", label, name, d),
                        )
                        .await
                    }
                    None => fs_err(r),
                }
            }
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/events?<q..>")]
    pub async fn events(
        _auth: crate::auth::CanRead,
        q: EventQuery,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<crate::events::EventPage, CommitError>> {
        match current(slot).await {
            Some(c) => match c.events().await {
                Ok(log) => fs_err(log.read(&q).await),
                Err(e) => fs_err(Err(e)),
            },
            None => not_committer(),
        }
    }

    /// Nodes report what happened on them.
    #[post("/api/v1/pool/events", data = "<events>")]
    pub async fn report_events(
        _auth: crate::auth::IsNode,
        events: Json<Vec<NewEvent>>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Vec<u64>, CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.record(events.into_inner()).await),
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/vms")]
    pub async fn vms(
        _auth: crate::auth::CanRead,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<std::collections::BTreeMap<String, crate::vm_store::VmDef>, CommitError>> {
        match current(slot).await {
            Some(c) => match c.vm_store().await {
                Ok(st) => fs_err(st.list().await),
                Err(e) => fs_err(Err(e)),
            },
            None => not_committer(),
        }
    }

    #[put("/api/v1/pool/vms", data = "<req>")]
    pub async fn put_vm(
        _auth: crate::auth::CanWrite,
        by: Actor,
        req: Json<PutVm>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<crate::vm_store::VmDef, CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = match c.vm_store().await {
                    Ok(st) => st.put(&req.name, &req.xml, req.create, req.change).await,
                    Err(e) => Err(e),
                };
                let kind = if req.create {
                    "vm.defined"
                } else {
                    "vm.changed"
                };
                noted(&c, r, by, kind, format!("vm/{}", req.name), |d| {
                    format!("VM '{}' definition stored (change {})", d.name, d.change)
                })
                .await
            }
            None => not_committer(),
        }
    }

    #[delete("/api/v1/pool/vms/<name>?<change>")]
    pub async fn delete_vm(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        change: Option<u64>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = match c.vm_store().await {
                    Ok(st) => st.delete(name, change).await,
                    Err(e) => Err(e),
                };
                noted(&c, r, by, "vm.deleted", format!("vm/{}", name), |_| {
                    format!("VM '{}' deleted", name)
                })
                .await
            }
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/datasets")]
    pub async fn datasets(
        _auth: crate::auth::CanRead,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Vec<DatasetInfo>, CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.datasets().await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/datasets", data = "<req>")]
    pub async fn create_dataset(
        _auth: crate::auth::CanWrite,
        by: Actor,
        req: Json<CreateDataset>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.create_dataset(&req).await;
                let label = crate::events::format_label(req.format);
                noted(
                    &c,
                    r,
                    by,
                    "dataset.created",
                    format!("{}/{}", label.to_lowercase(), req.name),
                    |_| format!("{} '{}' created", label, req.name),
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[delete("/api/v1/pool/datasets/<name>")]
    pub async fn delete_dataset(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let label = c.props_of(name).await.map_or("Dataset", |p| p.0);
                let r = c.delete_dataset(name).await;
                noted(
                    &c,
                    r,
                    by,
                    "dataset.deleted",
                    format!("{}/{}", label.to_lowercase(), name),
                    |_| format!("{} '{}' deleted", label, name),
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[get("/api/v1/pool/volumes/<name>/snapshots")]
    pub async fn snapshots(
        _auth: crate::auth::CanRead,
        name: &str,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<Vec<SnapshotInfo>, CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.snapshots(name).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes/<name>/snapshots", data = "<req>")]
    pub async fn snapshot(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        req: Json<NewName>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<u64, CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.create_snapshot(name, &req.name).await;
                noted(
                    &c,
                    r,
                    by,
                    "snapshot.created",
                    subject_of(&c, name).await,
                    |_| format!("Snapshot '{}@{}' taken", name, req.name),
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[delete("/api/v1/pool/volumes/<name>/snapshots/<snap>")]
    pub async fn destroy_snapshot(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        snap: &str,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.destroy_snapshot(name, snap).await;
                noted(
                    &c,
                    r,
                    by,
                    "snapshot.destroyed",
                    subject_of(&c, name).await,
                    |_| format!("Snapshot '{}@{}' destroyed", name, snap),
                )
                .await
            }
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes/<name>/snapshots/<snap>/clone", data = "<req>")]
    pub async fn clone_snapshot(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        snap: &str,
        req: Json<NewName>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.clone_snapshot(name, snap, &req.name).await;
                let subject = subject_of(&c, &req.name).await;
                noted(&c, r, by, "snapshot.cloned", subject, |_| {
                    format!("'{}' cloned from '{}@{}'", req.name, name, snap)
                })
                .await
            }
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes/<name>/recv?<force>", data = "<data>")]
    pub async fn recv(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        force: Option<bool>,
        data: rocket::Data<'_>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<StreamReport, CommitError>> {
        match current(slot).await {
            Some(c) => {
                let mut stream = data.open(rocket::data::ByteUnit::max_value());
                let r = c.recv(name, &mut stream, force.unwrap_or(false)).await;
                let subject = subject_of(&c, name).await;
                noted(&c, r, by, "snapshot.received", subject, |rep| {
                    format!(
                        "'{}' received {} ({})",
                        name,
                        rep.snapshot,
                        crate::events::bytes(rep.data_bytes)
                    )
                })
                .await
            }
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes/<name>/recv/check?<force>", data = "<header>")]
    pub async fn recv_check(
        _auth: crate::auth::CanWrite,
        name: &str,
        force: Option<bool>,
        header: Json<crate::send::SendHeader>,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => fs_err(c.recv_check(name, &header, force.unwrap_or(false)).await),
            None => not_committer(),
        }
    }

    #[post("/api/v1/pool/volumes/<name>/rollback")]
    pub async fn rollback(
        _auth: crate::auth::CanWrite,
        by: Actor,
        name: &str,
        slot: &State<CommitterSlot>,
    ) -> Json<Result<(), CommitError>> {
        match current(slot).await {
            Some(c) => {
                let r = c.rollback(name).await;
                noted(
                    &c,
                    r,
                    by,
                    "snapshot.rollback",
                    subject_of(&c, name).await,
                    |_| format!("'{}' rolled back to its newest snapshot", name),
                )
                .await
            }
            None => not_committer(),
        }
    }

    pub fn routes() -> Vec<rocket::Route> {
        rocket::routes![
            committer,
            lend,
            give_back,
            commit,
            commit_fs,
            claim,
            volumes,
            create,
            remove,
            props,
            datasets,
            create_dataset,
            delete_dataset,
            snapshots,
            snapshot,
            destroy_snapshot,
            clone_snapshot,
            rollback,
            recv,
            recv_check,
            events,
            report_events,
            vms,
            put_vm,
            delete_vm
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
    events: crate::events::EventSink,
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
        let monitor = crate::scan::spawn_health_monitor(
            fs.clone(),
            Duration::from_secs(30),
            Some(events.clone()),
        );
        *slot.write().await = Some(committer.clone());
        let mut observe_failing = false;
        while !lost.load(Ordering::SeqCst) {
            match committer.observe().await {
                Ok(()) => observe_failing = false,
                Err(e) if !observe_failing => {
                    eprintln!("committer: activity log: {}", e);
                    observe_failing = true;
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            committer.reclaim().await;
        }
        *slot.write().await = None;
        // It holds the pool (and its disk connections) for as long as it
        // runs, and this one may no longer write: the next term opens the
        // pool afresh with its own.
        monitor.abort();
    }
}

#[cfg(test)]
#[path = "tests/multinode_tests.rs"]
mod tests;
