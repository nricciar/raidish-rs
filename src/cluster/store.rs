//! Raft log + state machine, kept in memory and persisted to a few small
//! JSON files in the node's data directory (not on the pool: the control
//! plane has to work while the pool is degraded or not formatted yet).
//!
//! Everything here is tiny (a handful of leases, a log that's purged after
//! every snapshot), so every change rewrites the affected file atomically
//! (write a temp file, fsync, rename).

use super::TypeConfig;
use super::state::{ClusterState, Response};
use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

type NodeId = u64;
type Node = openraft::BasicNode;
type SErr = StorageError<NodeId>;

#[derive(Serialize, Deserialize, Default, Clone)]
struct StateMachine {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, Node>,
    state: ClusterState,
}

#[derive(Serialize, Deserialize, Default)]
struct LogFile {
    last_purged: Option<LogId<NodeId>>,
    entries: Vec<Entry<TypeConfig>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Node>,
    data: Vec<u8>,
}

struct Inner {
    dir: PathBuf,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    sm: StateMachine,
    snapshot: Option<StoredSnapshot>,
    snapshot_seq: u64,
}

/// Shared handle; the log reader and snapshot builder are clones of it.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Inner>>,
    /// The applied state, for readers outside Raft (status, fencing).
    view: Arc<RwLock<ClusterState>>,
    /// This node's disk server fencing, fed every applied state.
    fence: Arc<crate::fence::ServerFence>,
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, SErr> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| StorageIOError::read(&e).into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageIOError::read(&e).into()),
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(dir)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

impl Inner {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn save_log(&self) -> Result<(), SErr> {
        let file = LogFile {
            last_purged: self.last_purged,
            entries: self.log.values().cloned().collect(),
        };
        write_json(&self.path("log.json"), &file).map_err(|e| StorageIOError::write_logs(&e).into())
    }

    fn save_sm(&self) -> Result<(), SErr> {
        write_json(&self.path("state.json"), &self.sm)
            .map_err(|e| StorageIOError::write_state_machine(&e).into())
    }
}

impl Store {
    /// Open (or create) the store in `dir`.
    pub fn open(dir: &Path, fence: Arc<crate::fence::ServerFence>) -> Result<Self, String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {}", dir.display(), e))?;
        let err = |e: SErr| format!("{}: {}", dir.display(), e);
        let vote = read_json(&dir.join("vote.json")).map_err(err)?;
        let committed = read_json(&dir.join("committed.json"))
            .map_err(err)?
            .flatten();
        let log: LogFile = read_json(&dir.join("log.json"))
            .map_err(err)?
            .unwrap_or_default();
        let sm: StateMachine = read_json(&dir.join("state.json"))
            .map_err(err)?
            .unwrap_or_default();
        let snapshot: Option<StoredSnapshot> =
            read_json(&dir.join("snapshot.json")).map_err(err)?;
        observe(&fence, &sm.state);
        Ok(Store {
            view: Arc::new(RwLock::new(sm.state.clone())),
            fence,
            inner: Arc::new(Mutex::new(Inner {
                dir: dir.to_path_buf(),
                vote,
                committed,
                last_purged: log.last_purged,
                log: log
                    .entries
                    .into_iter()
                    .map(|e| (e.log_id.index, e))
                    .collect(),
                sm,
                snapshot,
                snapshot_seq: 0,
            })),
        })
    }

    /// The NFSv4 client records of the applied state.
    pub fn nfs_clients(&self) -> Vec<String> {
        self.view
            .read()
            .unwrap()
            .nfs_clients
            .keys()
            .cloned()
            .collect()
    }

    /// One lease of the applied state (cheaper than `state()`).
    pub fn lease(&self, resource: &str) -> Option<super::state::Lease> {
        self.view.read().unwrap().leases.get(resource).cloned()
    }

    /// The applied cluster state.
    pub fn state(&self) -> ClusterState {
        self.view.read().unwrap().clone()
    }

    fn publish(&self, state: &ClusterState) {
        observe(&self.fence, state);
        *self.view.write().unwrap() = state.clone();
    }
}

fn observe(fence: &crate::fence::ServerFence, state: &ClusterState) {
    fence.observe(
        state.pool_epoch(),
        state
            .metaslabs
            .iter()
            .map(|(&i, g)| (i, g.writer.is_some(), g.epoch)),
    );
}

impl RaftLogReader<TypeConfig> for Store {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, SErr> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Store {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, SErr> {
        let mut inner = self.inner.lock().unwrap();
        let data = serde_json::to_vec(&inner.sm.state)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        inner.snapshot_seq += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.sm.last_applied,
            last_membership: inner.sm.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                inner.sm.last_applied.map_or(0, |l| l.index),
                inner.snapshot_seq
            ),
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        write_json(&inner.path("snapshot.json"), &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        inner.snapshot = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Store {
    type LogReader = Store;
    type SnapshotBuilder = Store;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), SErr> {
        let mut inner = self.inner.lock().unwrap();
        write_json(&inner.path("vote.json"), vote).map_err(|e| StorageIOError::write_vote(&e))?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, SErr> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), SErr> {
        let mut inner = self.inner.lock().unwrap();
        write_json(&inner.path("committed.json"), &committed)
            .map_err(|e| StorageIOError::write(&e))?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, SErr> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, SErr> {
        let inner = self.inner.lock().unwrap();
        let last = inner.log.values().next_back().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last.or(inner.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), SErr>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut inner = self.inner.lock().unwrap();
        for e in entries {
            inner.log.insert(e.log_id.index, e);
        }
        inner.save_log()
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let mut inner = self.inner.lock().unwrap();
        inner.log.split_off(&log_id.index);
        inner.save_log()
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let mut inner = self.inner.lock().unwrap();
        inner.log = inner.log.split_off(&(log_id.index + 1));
        inner.last_purged = Some(log_id);
        inner.save_log()
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), SErr> {
        let inner = self.inner.lock().unwrap();
        Ok((inner.sm.last_applied, inner.sm.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, SErr> {
        let mut inner = self.inner.lock().unwrap();
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            inner.sm.last_applied = Some(e.log_id);
            out.push(match &e.payload {
                EntryPayload::Blank => Response::None,
                EntryPayload::Normal(req) => inner.sm.state.apply(req),
                EntryPayload::Membership(m) => {
                    inner.sm.membership = StoredMembership::new(Some(e.log_id), m.clone());
                    Response::None
                }
            });
        }
        // Persist before anyone (a disk server's fence) acts on the result.
        inner.save_sm()?;
        self.publish(&inner.sm.state);
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SErr> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SErr> {
        let data = snapshot.into_inner();
        let state: ClusterState = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.inner.lock().unwrap();
        inner.sm = StateMachine {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            state,
        };
        inner.save_sm()?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        write_json(&inner.path("snapshot.json"), &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        inner.snapshot = Some(stored);
        self.publish(&inner.sm.state);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, SErr> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
