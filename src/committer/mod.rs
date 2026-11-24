//! The committer: the `pool` lease holder in a multi-writer pool (see
//! `crate::writer`). It lends metaslabs to writers, applies their commits,
//! and does the pool's metadata operations (create and delete datasets,
//! change their settings, snapshots). `listen` runs one on whichever node wins the `pool`
//! lease; the others keep trying, so it fails over.

pub use crate::api_types::{CreateVolume, NewName};
use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::cluster::state::MetaslabGrant;
use crate::cluster::state::{ClusterState, POOL, Response};
use crate::cluster::{Cluster, LeaseOp, now_ms};
use crate::dataset::{DatasetFormat, DatasetInfo};
use crate::disk::BlockDevice;
use crate::events::{EventKind, EventLog, Held, Level, NewEvent, Observed, Subject};
use crate::fs::{FileSystem, FileSystemError};
use crate::inode::DatasetProps;
use crate::send::StreamReport;
use crate::snapshot::SnapshotInfo;
use crate::types::ErrorKind;
use crate::types::MetaslabIdx;
use crate::types::{Epoch, Holder, HolderRole, Holding, LeaseRef, LeaseResource, Txg};
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

/// A cluster state machine in memory (tests).
#[cfg(test)]
#[derive(Default)]
pub struct LocalAuthority(pub std::sync::Mutex<ClusterState>);

#[cfg(test)]
#[async_trait]
impl Authority for LocalAuthority {
    async fn propose(&self, op: LeaseOp) -> Result<Response, String> {
        Ok(self.0.lock().unwrap().apply(&op.stamp(now_ms())))
    }
    fn state(&self) -> ClusterState {
        self.0.lock().unwrap().clone()
    }
}

/// Is `resource` held by `holder`? See `check_lease`.
fn check_held(
    state: &ClusterState,
    resource: &LeaseResource,
    holder: &Holding,
) -> Result<(), CommitError> {
    if state.holds(resource, holder, now_ms()) {
        return Ok(());
    }
    check_lease(state, resource, Some(&holder.owner), holder.epoch)
}

/// Is `lease` live (whoever holds it)? See `check_lease`.
fn check_live(state: &ClusterState, lease: &LeaseRef) -> Result<(), CommitError> {
    if state.is_live(lease, now_ms()) {
        return Ok(());
    }
    check_lease(state, &lease.resource, None, lease.epoch)
}

/// Why `resource` isn't held (by `owner`, if given) at `epoch`:
/// `Err(Refused)` if it never will be again (taken over, released),
/// `Err(Retry)` if it's not visible or has lapsed but may still be renewed.
fn check_lease(
    state: &ClusterState,
    resource: &LeaseResource,
    owner: Option<&str>,
    epoch: Epoch,
) -> Result<(), CommitError> {
    match state.leases.get(resource) {
        // Taken over since, or this very grant released / someone else's.
        Some(l)
            if l.epoch > epoch
                || (l.epoch == epoch && (l.released || owner.is_some_and(|o| o != l.owner))) =>
        {
            Err(CommitError::refused(
                ErrorKind::Conflict,
                format!("{} lease lost (epoch {})", resource, epoch),
            ))
        }
        // An older grant (whoever's, released or not): this view hasn't
        // caught up with this one yet. Or it lapsed but may be renewed.
        _ => Err(CommitError::Retry(format!("{} lease not live", resource))),
    }
}

fn reserved(name: &str) -> FileSystemError {
    FileSystemError::InvalidArgument(format!("'{}' is the pool's own", name))
}

/// A name a user may give a new dataset (`.` names are the pool's: see
/// `crate::types::DatasetName`).
fn check_not_reserved(name: &str) -> Result<(), FileSystemError> {
    crate::types::DatasetName::user(name)?;
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

pub fn dataset_resource(name: &str) -> LeaseResource {
    LeaseResource::dataset(name)
}

/// The lease held on a file while it is open for writing.
pub fn object_resource(id: crate::disk::FileId) -> LeaseResource {
    LeaseResource::Object(id)
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
        return check_held(state, &resource, l);
    }
    match state.leases.get(&resource) {
        Some(l) if l.is_live(now_ms()) => Err(CommitError::refused(
            ErrorKind::Conflict,
            format!("object {} is open for writing ({})", id, l.holder),
        )),
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
        Some(lease) if lease.epoch < l.ds.epoch => Orphan::Live,
        Some(lease) if lease.epoch > l.ds.epoch => Orphan::Gone,
        Some(lease) if lease.owner != l.ds.owner || lease.released => Orphan::Gone,
        Some(lease) if lease.expires_ms.saturating_add(RECLAIM_GRACE_MS) < now => Orphan::Lapsed,
        Some(_) => Orphan::Live,
    }
}

pub struct Committer<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    auth: Arc<dyn Authority>,
    /// Its `pool` lease.
    pool: Holding,
    /// The node it runs on (for status).
    node: String,
    /// Metaslab → its lend. The committer's own record is authoritative:
    /// this node's view of the cluster may not show a lend it just made yet.
    lends: std::sync::Mutex<std::collections::HashMap<MetaslabIdx, Lend>>,
    /// Per metaslab, the epoch of the newest grant (lend or return) the
    /// cluster answered this committer with: what its next `Lend` or
    /// `Return` of it names (see `LeaseOp::Lend`). This node's applied
    /// state may lag its own ops; epochs only grow, so the newer one wins.
    grants: std::sync::Mutex<std::collections::HashMap<MetaslabIdx, Epoch>>,
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

/// A metaslab lent (or maybe lent) to a writer. A `Return` always names
/// the lend's own epoch, however often it's sent: a resend after a lost
/// answer is answered as the first time (audit R4d). Only `Active` and
/// `Returning` have one, so nothing else can be named.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Lend {
    /// The `Lend` (naming `after`) got no answer, so it may have gone
    /// through. The writer was never told: `reclaim` sends it again to
    /// find out (`settle_lend`), then takes it back.
    InDoubt { writer: LeaseRef, after: Epoch },
    /// Lent at `epoch`, and the writer knows.
    Active { writer: LeaseRef, epoch: Epoch },
    /// Its `Return` was sent, maybe without an answer: `reclaim` sends it
    /// again, live writer or not.
    Returning { writer: LeaseRef, epoch: Epoch },
}

impl Lend {
    fn writer(&self) -> &LeaseRef {
        match self {
            Lend::InDoubt { writer, .. }
            | Lend::Active { writer, .. }
            | Lend::Returning { writer, .. } => writer,
        }
    }
}

/// A writer's lease must have lapsed this long before its metaslabs are
/// taken back (this node's view of renewals may lag a little).
const RECLAIM_GRACE_MS: u64 = 3000;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CommitterInfo {
    pub node: String,
    pub epoch: Epoch,
}

impl<D: BlockDevice> Committer<D> {
    /// Become the committer of `fs`, which was opened writable under the
    /// `pool` lease (`owner`, `epoch`).
    pub async fn new(
        fs: Arc<FileSystem<D>>,
        auth: Arc<dyn Authority>,
        owner: String,
        epoch: Epoch,
        node: String,
    ) -> Self {
        if let Some(pool) = fs.dev.pool() {
            pool.disks().fence().set_pool_epoch(epoch);
        }
        let state = auth.state();
        let lends: std::collections::HashMap<MetaslabIdx, Lend> = state
            .metaslabs
            .iter()
            .filter_map(|(&i, g)| match g {
                MetaslabGrant::Lent { writer, epoch, .. } => Some((
                    i,
                    Lend::Active {
                        writer: writer.clone(),
                        epoch: *epoch,
                    },
                )),
                MetaslabGrant::Returned { .. } => None,
            })
            .collect();
        let grants = state
            .metaslabs
            .iter()
            .map(|(&i, g)| (i, g.epoch()))
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
            pool: Holding::new(owner, epoch),
            node,
            lends: std::sync::Mutex::new(lends),
            grants: std::sync::Mutex::new(grants),
            events: Default::default(),
            vms: Default::default(),
            seen: Default::default(),
            disks_online: Default::default(),
        }
    }

    // ---- the activity log (see `crate::events`) ----

    /// The pool it commits.
    #[cfg(test)]
    pub fn fs(&self) -> &Arc<FileSystem<D>> {
        &self.fs
    }

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
        kind: EventKind,
        subject: Subject,
        message: String,
        by: Option<String>,
    ) {
        let e = NewEvent {
            ts_ms: now_ms(),
            node: self.node.clone(),
            level,
            kind,
            subject,
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
            epoch: self.pool.epoch,
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
            noticed = crate::events::diff(old, &new, &state, &|d| formats.get(d).copied());
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
                kind,
                subject,
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
        let now: Vec<bool> = (0..pool.disks().len())
            .map(|d| pool.disks().is_online(d))
            .collect();
        let nodes = self.auth.disk_nodes();
        let mut last = self.disks_online.lock().unwrap();
        let mut out = Vec::new();
        for (d, &online) in now.iter().enumerate() {
            let before = last.as_ref().and_then(|l| l.get(d).copied());
            let name = match nodes.get(&d) {
                Some(n) => format!("Disk {} ({})", d, n),
                None => disk_label(d, pool.disks().disk(d).path(), &self.node),
            };
            match (before, online) {
                (Some(true), false) | (None, false) => out.push((
                    Level::Error,
                    EventKind::DiskOffline,
                    Subject::Disk(d),
                    format!("{} is offline", name),
                )),
                (Some(false), true) => out.push((
                    Level::Info,
                    EventKind::DiskOnline,
                    Subject::Disk(d),
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
            epoch: self.pool.epoch,
        }
    }

    /// Lend `writer` a metaslab, preferably with `want_blocks` free.
    pub async fn lend(
        &self,
        writer: &LeaseRef,
        want_blocks: u64,
    ) -> Result<LentMetaslab, CommitError> {
        check_live(&self.auth.state(), writer)?;
        let useful = 4 * self.fs.max_record_blocks();
        let size = self.fs.layout.metaslab_size_blocks;
        let min_free = want_blocks.min(size / 2).max(useful);
        let picked = match self.fs.lend_pick(min_free).await {
            Some(p) => Some(p),
            None => self.fs.lend_pick(useful).await,
        };
        let Some((idx, free)) = picked else {
            return Err(CommitError::refused(
                ErrorKind::NoSpace,
                "no metaslab with free space to lend",
            ));
        };
        let after = self.grant_epoch(idx);
        let op = LeaseOp::Lend {
            idx,
            writer: writer.clone(),
            pool: self.pool.clone(),
            after,
        };
        match self.auth.propose(op).await {
            Ok(Response::Fenced { epoch }) => {
                self.grants.lock().unwrap().insert(idx, epoch);
                self.lends.lock().unwrap().insert(
                    idx,
                    Lend::Active {
                        writer: writer.clone(),
                        epoch,
                    },
                );
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
                Err(CommitError::refused(ErrorKind::Conflict, m))
            }
            other => {
                // It may have gone through: the metaslab stays out of use
                // here (it may be the writer's on the disk servers), and
                // `reclaim` finds out and takes it back (audit R4d).
                self.lends.lock().unwrap().insert(
                    idx,
                    Lend::InDoubt {
                        writer: writer.clone(),
                        after,
                    },
                );
                Err(CommitError::Retry(format!("lend: {:?}", other)))
            }
        }
    }

    /// Settle a lend in doubt: the same `Lend` again is answered as the
    /// first time if it went through. It's then `Active` (recorded here:
    /// settled once is settled, or a later lost answer, say to the return,
    /// would send it again, now refused, and abort a lend that happened);
    /// otherwise it's dropped and the metaslab is back in use here. The
    /// lend's epoch, or None.
    async fn settle_lend(
        &self,
        idx: MetaslabIdx,
        writer: &LeaseRef,
        after: Epoch,
    ) -> Result<Option<Epoch>, CommitError> {
        let settled = self.settle_lend_once(idx, writer, after).await?;
        let mut lends = self.lends.lock().unwrap();
        let ours = |l: &Lend| matches!(l, Lend::InDoubt { writer: w, after: a } if w == writer && *a == after);
        if lends.get(&idx).is_some_and(ours) {
            match settled {
                Some(epoch) => {
                    lends.insert(
                        idx,
                        Lend::Active {
                            writer: writer.clone(),
                            epoch,
                        },
                    );
                }
                None => {
                    lends.remove(&idx);
                }
            }
        }
        Ok(settled)
    }

    async fn settle_lend_once(
        &self,
        idx: MetaslabIdx,
        writer: &LeaseRef,
        after: Epoch,
    ) -> Result<Option<Epoch>, CommitError> {
        let op = LeaseOp::Lend {
            idx,
            writer: writer.clone(),
            pool: self.pool.clone(),
            after,
        };
        match self.auth.propose(op).await {
            Ok(Response::Fenced { epoch }) => {
                self.grants.lock().unwrap().insert(idx, epoch);
                Ok(Some(epoch))
            }
            Ok(Response::Refused(m)) => {
                // A resend of a lend that went through is answered as the
                // first time, so this one didn't, unless the state says
                // otherwise (this node's view may be all there is to ask).
                let applied = self.auth.state().metaslabs.get(&idx).and_then(|g| match g {
                    MetaslabGrant::Lent {
                        writer: w,
                        epoch,
                        from,
                    } if w == writer && *from == after => Some(*epoch),
                    _ => None,
                });
                if let Some(epoch) = applied {
                    return Ok(Some(epoch));
                }
                eprintln!("committer: metaslab {} wasn't lent after all: {}", idx, m);
                self.fs.lend_abort(idx).await;
                Ok(None)
            }
            other => Err(CommitError::Retry(format!("lend in doubt: {:?}", other))),
        }
    }

    async fn take_back(&self, idx: MetaslabIdx) -> Result<(), CommitError> {
        let Some(l) = self.lends.lock().unwrap().get(&idx).cloned() else {
            return Ok(());
        };
        let (writer, lend) = match l {
            Lend::Active { writer, epoch } | Lend::Returning { writer, epoch } => (writer, epoch),
            Lend::InDoubt { writer, after } => match self.settle_lend(idx, &writer, after).await? {
                Some(epoch) => (writer, epoch),
                None => return Ok(()),
            },
        };
        self.lends.lock().unwrap().insert(
            idx,
            Lend::Returning {
                writer,
                epoch: lend,
            },
        );
        let op = LeaseOp::Return {
            idx,
            pool: self.pool.clone(),
            lend,
        };
        match self.auth.propose(op).await {
            Ok(Response::Fenced { epoch }) => {
                self.grants.lock().unwrap().insert(idx, epoch);
                self.lends.lock().unwrap().remove(&idx);
                self.fs.unlend(idx, epoch).await;
                Ok(())
            }
            Ok(Response::Refused(m)) => Err(CommitError::refused(ErrorKind::Conflict, m)),
            other => Err(CommitError::Retry(format!("return: {:?}", other))),
        }
    }

    /// The epoch of metaslab `idx`'s current grant, as far as this
    /// committer knows: the newer of its own last answer and the applied
    /// state.
    fn grant_epoch(&self, idx: MetaslabIdx) -> Epoch {
        let seen = self
            .auth
            .state()
            .metaslabs
            .get(&idx)
            .map_or(Epoch::NONE, |g| g.epoch());
        let ours = self
            .grants
            .lock()
            .unwrap()
            .get(&idx)
            .copied()
            .unwrap_or(Epoch::NONE);
        seen.max(ours)
    }

    /// Whether metaslab `idx` is lent to `writer` right now.
    fn lent_to(&self, idx: MetaslabIdx, writer: &LeaseRef) -> bool {
        // A lend in doubt was never told to the writer, and one being
        // returned is going: neither is its.
        matches!(
            self.lends.lock().unwrap().get(&idx),
            Some(Lend::Active { writer: w, .. }) if w == writer
        )
    }

    /// A writer gives metaslab `idx` back.
    pub async fn give_back(&self, writer: &LeaseRef, idx: MetaslabIdx) -> Result<(), CommitError> {
        let lent = self.lends.lock().unwrap().get(&idx).cloned();
        match lent.as_ref().map(Lend::writer) {
            Some(w) if w == writer => self.take_back(idx).await,
            // Already back (a resend): fine.
            None => Ok(()),
            Some(_) => Err(CommitError::refused(
                ErrorKind::Conflict,
                format!("metaslab {} isn't lent to {}", idx, writer),
            )),
        }
    }

    pub async fn commit(&self, c: &DatasetCommit) -> Result<Committed, CommitError> {
        let leases_ok = || {
            let state = self.auth.state();
            check_live(&state, &c.writer)?;
            check_held(&state, &dataset_resource(&c.dataset), &c.ds)
        };
        // Early, to refuse before waiting; again under the tx, which is
        // the check that counts (it takes the tx: audit S6a).
        leases_ok()?;
        let leases_ok = |_: &crate::tx::Tx| leases_ok();
        // Asked live, while the allocations are claimed (under the metaslab
        // locks): a snapshot taken here could still say a metaslab is this
        // writer's after it was taken back and lent to another (audit S6a).
        let txg = self
            .fs
            .apply_dataset_commit(c, leases_ok, |idx| self.lent_to(idx, &c.writer))
            .await?;
        Ok(Committed { txg })
    }

    /// Apply a writer's file or directory change.
    pub async fn commit_fs(&self, c: &FsCommit) -> Result<FsCommitted, CommitError> {
        let leases_ok = || check_live(&self.auth.state(), &c.writer);
        leases_ok()?;
        // The `obj/<id>` leases, as of under the tx (a state taken then).
        let state = std::sync::Mutex::new(None::<ClusterState>);
        self.fs
            .apply_fs_commit(
                c,
                |_: &crate::tx::Tx| {
                    let s = self.auth.state();
                    check_live(&s, &c.writer)?;
                    *state.lock().unwrap() = Some(s);
                    Ok(())
                },
                |idx| self.lent_to(idx, &c.writer),
                |id| match state.lock().unwrap().as_ref() {
                    Some(s) => object_lease_ok(s, c, id),
                    None => object_lease_ok(&self.auth.state(), c, id),
                },
            )
            .await
    }

    pub async fn claim_dataset(&self, volume: &str, ds: &Holding) -> Result<(), CommitError> {
        check_held(&self.auth.state(), &dataset_resource(volume), ds)?;
        let vol = self
            .fs
            .open_dataset(volume)
            .await
            .map_err(|e| CommitError::failed(format!("volume '{}'", volume), &e))?;
        let log = vol.log.lock().unwrap().clone();
        if let Some(l) = log
            && l.ds != *ds
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
                    holder: Holding::new(self.replay_owner(), epoch),
                };
                if let Err(e) = self.auth.propose(op).await {
                    eprintln!("committer: releasing {}: {}", resource, e);
                }
            }
        }
    }

    /// Who holds a `ds/<name>` lease while the committer replays its log.
    fn replay_owner(&self) -> String {
        format!("{} (log replay)", self.pool.owner)
    }

    /// Take over a lapsed `ds/<name>` lease, so its holder can't renew it
    /// (a lapsed lease can be renewed until someone else is granted it) and
    /// go on acknowledging flushes into a log that's been replayed. Returns
    /// the epoch granted, or None if the lease is live again.
    async fn fence_lapsed(
        &self,
        resource: &LeaseResource,
        l: &crate::inode::DatasetLog,
    ) -> Option<Epoch> {
        let op = LeaseOp::Acquire {
            resource: resource.clone(),
            owner: self.replay_owner(),
            holder: Holder::new(HolderRole::Replay, Some(&self.node))
                .with_detail(format!("{}'s intent log", l.writer.resource)),
            ttl_ms: crate::cluster::lease::LEASE_TTL.as_millis() as u64,
        };
        match self.auth.propose(op).await {
            // A newer epoch than the log's: its holder's renewals fail from
            // now on, and its last one was sent over a TTL ago (the lease
            // had expired when this was granted), so it can't be
            // acknowledging anything any more.
            Ok(Response::Granted { epoch, .. }) if epoch > l.ds.epoch => Some(epoch),
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
        for (idx, l) in lends {
            // A lend in doubt goes back at once (its writer wasn't told), and
            // a return that got no answer is sent again.
            let gone = match &l {
                Lend::InDoubt { .. } | Lend::Returning { .. } => true,
                Lend::Active { writer, .. } => match state.leases.get(&writer.resource) {
                    Some(l) => {
                        l.epoch != writer.epoch
                            || l.released
                            || l.expires_ms.saturating_add(RECLAIM_GRACE_MS) < now
                    }
                    // Not visible here yet: a lend this node's view hasn't caught up with.
                    None => false,
                },
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
            .live_lease(&dataset_resource(name), now_ms())
            .map(|l| l.holder.to_string())
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
    /// Dataset `name`'s format and props.
    pub async fn props_of(&self, name: &str) -> Option<(DatasetFormat, DatasetProps)> {
        let e = self.fs.dataset_entry(name).await.ok()?;
        let d = self.fs.read_dnode(e.root).await.ok()?;
        Some((e.format, d.props().cloned().unwrap_or_default()))
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
            DatasetFormat::S3 => drop(crate::types::BucketName::try_from(req.name.as_str())?),
            DatasetFormat::Queue => drop(crate::types::QueueName::try_from(req.name.as_str())?),
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
    pub async fn create_snapshot(&self, name: &str, snap: &str) -> Result<Txg, FileSystemError> {
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
#[cfg(test)]
pub struct DirectSink<D: BlockDevice> {
    pub committer: Arc<Committer<D>>,
    pub writer: LeaseRef,
}

#[cfg(test)]
#[async_trait]
impl<D: BlockDevice + 'static> CommitSink for DirectSink<D> {
    fn writer(&self) -> LeaseRef {
        self.writer.clone()
    }
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError> {
        self.committer.lend(&self.writer, want_blocks).await
    }
    async fn return_metaslab(&self, idx: MetaslabIdx) -> Result<(), CommitError> {
        self.committer.give_back(&self.writer, idx).await
    }
    async fn commit(&self, c: DatasetCommit) -> Result<Committed, CommitError> {
        self.committer.commit(&c).await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        self.committer.commit_fs(&c).await
    }
    async fn claim_dataset(&self, volume: &str, ds: &Holding) -> Result<(), CommitError> {
        self.committer.claim_dataset(volume, ds).await
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LendRequest {
    pub writer: LeaseRef,
    pub want_blocks: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReturnRequest {
    pub writer: LeaseRef,
    pub idx: MetaslabIdx,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClaimRequest {
    pub dataset: String,
    pub ds: Holding,
}

/// A dataset to make (see `Committer::create_dataset`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateDataset {
    pub name: crate::types::DatasetName,

    pub format: crate::dataset::DatasetFormat,
    #[serde(default)]
    pub props: DatasetProps,
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

pub mod admin;
mod client;
pub mod health;
pub use admin::{Admin, LocalAdmin, PoolAdmin};
pub use client::CommitClient;

// ---- HTTP routes, served by every `listen` node ----

/// This node's committer, while it is one.
pub type CommitterSlot = Arc<tokio::sync::RwLock<Option<Arc<Committer<crate::vdev::Pool>>>>>;

pub mod routes;

// ---- the committer service in `listen` ----

/// How `listen` opens the pool as committer (or writer): the pool spec
/// with this node's own disks reached through its own disk server, so
/// every write is fenced the same way.
#[derive(Clone)]
pub struct PoolAccess {
    pub spec: crate::vdev::PoolSpec,
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
        let held = match HeldLease::acquire(
            leases.clone(),
            POOL,
            Holder::new(HolderRole::Committer, Some(&node)),
        )
        .await
        {
            Ok(h) => Arc::new(h),
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        // Renewing from the start: loading can take longer than a TTL, and
        // a lease that lapsed meanwhile could be granted to another
        // committer while this one goes on to commit (audit R3b).
        let lost = Arc::new(AtomicBool::new(false));
        let flag = lost.clone();
        held.hold(move |msg| {
            eprintln!("committer: {}; stepping down", msg);
            flag.store(true, Ordering::SeqCst);
        });
        let pool = access.open().await;
        pool.disks().fence().set_pool_epoch(held.epoch);
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
        if lost.load(Ordering::SeqCst) {
            continue;
        }
        // The lends and grants it starts from, as of now: the lease may
        // have been granted through the leader, ahead of what this node
        // has applied (audit R2b).
        if let Err(e) = cluster.linearizable_state(Duration::from_secs(10)).await {
            eprintln!(
                "committer: no linearizable read of the cluster ({}); retrying",
                e
            );
            held.release().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        // The read can take its whole timeout: the lease may have lapsed.
        if lost.load(Ordering::SeqCst) {
            continue;
        }
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
        if lost.load(Ordering::SeqCst) {
            continue;
        }
        println!(
            "committer: this node commits the pool (epoch {})",
            held.epoch
        );
        let monitor =
            health::spawn_health_monitor(fs.clone(), Duration::from_secs(30), Some(events.clone()));
        *slot.write().await = Some(committer.clone());
        let mut observe_failing = false;
        let fenced_out = || fs.dev.pool().is_some_and(|p| p.disks().fence().is_stale());
        let mut ticked = false;
        while !lost.load(Ordering::SeqCst) {
            if fenced_out() {
                // A disk server refused our pool epoch: a newer committer
                // has the pool, whatever our lease renewals say (audit R2a).
                eprintln!("committer: a disk refused our pool epoch; stepping down");
                held.release().await;
                break;
            }
            if fs.uberblock_in_doubt() {
                // An uberblock write failed partway: this pool can't commit
                // until it's reopened. The next term opens it afresh
                // (structural triage I-2).
                eprintln!("committer: an uberblock write failed partway; stepping down");
                held.release().await;
                break;
            }
            // Each tick after the first, once the checks above passed: a
            // committer stepping down doesn't reclaim on its way out.
            if ticked {
                committer.reclaim().await;
            }
            ticked = true;
            match committer.observe().await {
                Ok(()) => observe_failing = false,
                Err(e) if !observe_failing => {
                    eprintln!("committer: activity log: {}", e);
                    observe_failing = true;
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        *slot.write().await = None;
        // It holds the pool (and its disk connections) for as long as it
        // runs, and this one may no longer write: the next term opens the
        // pool afresh with its own.
        monitor.abort();
    }
}

#[cfg(test)]
#[path = "../tests/multinode_tests.rs"]
mod tests;
