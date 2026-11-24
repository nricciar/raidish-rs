//! The cluster's activity log: what happened, for admins (nodes joining
//! and leaving, volumes, services and VMs starting and stopping, disks
//! dropping out, resilvers, what admins changed). Not every file write:
//! a level of detail that shows how the cluster is running.
//!
//! The log is one system dataset per pool (`.events`, format `events`):
//! a B+tree of `E<seq>` → event, written by the committer only, so events
//! have one order. Where events come from:
//!
//! - The committer watches the cluster state ([`observe`], [`diff`]):
//!   nodes joining, leaving or going quiet, a dataset whose node went
//!   away, committer takeovers, Raft leader changes. What it last saw is
//!   kept in the log too (`S`), so a new committer reports what changed
//!   while there was none.
//! - The committer's own routes record the pool changes they make, and
//!   who asked.
//! - Each node reports what it does and what fails there (an
//!   [`EventSink`], sent to the committer in batches; kept in memory while
//!   there is none).
//!
//! Old events go: past [`MAX_EVENTS`] or [`MAX_AGE_MS`].

use crate::btree::Txn;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::types::Epoch;
use crate::types::{HolderRole, LeaseResource};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

/// The log's dataset.
pub const EVENTS_DATASET: &str = ".events";
/// Events kept, at most.
pub const MAX_EVENTS: u64 = 50_000;
/// Events older than this go.
pub const MAX_AGE_MS: u64 = 30 * 24 * 3600 * 1000;
/// Events one read returns, at most.
pub const MAX_PAGE: usize = 1000;

const META: &[u8] = b"M";
const SEEN: &[u8] = b"S";

pub use crate::api_types::{Event, EventKind, EventPage, Level, NewEvent, Subject};

/// What to read (`GET /api/v1/events`).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, rocket::FromForm)]
pub struct EventQuery {
    /// Events numbered after this, oldest first.
    pub after: Option<u64>,
    /// Without `after`: the newest events numbered before this (None: the
    /// newest).
    pub before: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    next_seq: u64,
}

/// The event number in key `k` (see [`key`]).
fn seq_of(k: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(k.strip_prefix(b"E")?.try_into().ok()?))
}

fn key(seq: u64) -> Vec<u8> {
    let mut k = b"E".to_vec();
    k.extend(seq.to_be_bytes());
    k
}

use crate::service_tree::enc_json as json;

fn parse<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T, FileSystemError> {
    crate::service_tree::dec_json(b, "events record")
}

/// The log, opened on the committer's pool.
pub struct EventLog<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    ds: Arc<Dataset>,
    /// Appends one at a time (they number events).
    append: tokio::sync::Mutex<()>,
}

impl<D: BlockDevice> EventLog<D> {
    /// Open the pool's log, made empty if it has none yet.
    pub async fn open(fs: Arc<FileSystem<D>>) -> Result<Arc<Self>, FileSystemError> {
        let ds =
            crate::service_tree::ensure_btree(&fs, EVENTS_DATASET, DatasetFormat::Events).await?;
        Ok(Arc::new(EventLog {
            fs,
            ds,
            append: Default::default(),
        }))
    }

    /// Add `events` (and, if given, what the committer last saw), number
    /// them, drop what's too old, and commit. Returns their numbers.
    pub async fn append(
        &self,
        events: Vec<NewEvent>,
        seen: Option<&Observed>,
    ) -> Result<Vec<u64>, FileSystemError> {
        if events.is_empty() && seen.is_none() {
            return Ok(Vec::new());
        }
        let _one = self.append.lock().await;
        let seqs = {
            let _w = self.ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &self.ds.obj).await?;
            let mut meta: Meta = match t.get(META).await? {
                Some(b) => parse(&b)?,
                None => Meta::default(),
            };
            let mut seqs = Vec::with_capacity(events.len());
            for event in events {
                let seq = meta.next_seq.max(1);
                meta.next_seq = seq + 1;
                t.put(&key(seq), &json(&Event { seq, event })?).await?;
                seqs.push(seq);
            }
            t.put(META, &json(&meta)?).await?;
            if let Some(s) = seen {
                t.put(SEEN, &json(s)?).await?;
            }
            prune(&mut t, crate::fs::now_ms()).await?;
            t.apply_logged()?;
            seqs
        };
        self.fs.sync().await?;
        Ok(seqs)
    }

    /// What the committer last saw (None: nothing yet, a new log).
    pub async fn seen(&self) -> Result<Option<Observed>, FileSystemError> {
        let _r = self.ds.ops.read().await;
        let t = Txn::begin(&self.fs, &self.ds.obj).await?;
        // One kept by an older version that no longer reads: start over
        // from what's seen now.
        Ok(t.get(SEEN).await?.and_then(|b| parse(&b).ok()))
    }

    /// Read a page (see [`EventQuery`]).
    pub async fn read(&self, q: &EventQuery) -> Result<EventPage, FileSystemError> {
        let limit = q.limit.unwrap_or(200).clamp(1, MAX_PAGE);
        let _r = self.ds.ops.read().await;
        let t = Txn::begin(&self.fs, &self.ds.obj).await?;
        let next = match t.get(META).await? {
            Some(b) => parse::<Meta>(&b)?.next_seq.max(1),
            None => 1,
        };
        let first = t
            .range(Bound::Included(b"E"), Bound::Excluded(b"F"), 1)
            .await?
            .first()
            .and_then(|(k, _)| seq_of(k))
            .unwrap_or(0);
        // Numbers are consecutive (only the oldest go), so the newest
        // `limit` before `n` start at `n - limit`.
        let (from, to) = match (q.after, q.before) {
            (Some(a), _) => (a.saturating_add(1), next),
            (None, b) => {
                let to = b.unwrap_or(next).min(next);
                (to.saturating_sub(limit as u64).max(1), to)
            }
        };
        let (from, to) = (key(from), key(to));
        let events = t
            .range(
                Bound::Included(from.as_slice()),
                Bound::Excluded(to.as_slice()),
                limit,
            )
            .await?
            .iter()
            // One this version can't read (written by another): skip it
            // rather than lose the whole page.
            .filter_map(|(k, v)| match parse(v) {
                Ok(e) => Some(e),
                Err(e) => {
                    eprintln!("events: skipping unreadable event {:?}: {}", seq_of(k), e);
                    None
                }
            })
            .collect();
        Ok(EventPage {
            events,
            next,
            first,
        })
    }
}

/// Drop the oldest events past the limits (a few at a time: each append
/// does some).
async fn prune<D: BlockDevice>(t: &mut Txn<'_, D>, now: u64) -> Result<(), FileSystemError> {
    // Besides the events: `M` and `S`.
    let events = t.header.entries.saturating_sub(2);
    let over = events.saturating_sub(MAX_EVENTS);
    let scan = (over as usize).clamp(64, 4096);
    let oldest = t
        .range(Bound::Included(b"E"), Bound::Excluded(b"F"), scan)
        .await?;
    for (i, (k, v)) in oldest.into_iter().enumerate() {
        let old = parse::<Event>(&v).map_or(true, |e| e.event.ts_ms + MAX_AGE_MS < now);
        if (i as u64) < over || old {
            t.delete(&k).await?;
        } else {
            break;
        }
    }
    Ok(())
}

// ---- what the committer sees ----

/// A lease, as last seen.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Held {
    pub node: String,
    pub epoch: Epoch,
}

/// What the committer last saw of the cluster: just enough to tell what
/// changed since.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Observed {
    /// Every node ever seen → its lease epoch and whether it was live.
    pub nodes: BTreeMap<String, (Epoch, bool)>,
    /// Running datasets → who runs them.
    pub datasets: BTreeMap<String, Held>,
    pub committer: Option<Held>,
    /// The Raft leader's id.
    pub leader: Option<u64>,
}

/// What `state` shows at `now`. Nodes stay known once seen.
pub fn observe(
    state: &crate::cluster::state::ClusterState,
    now: u64,
    committer: Held,
    leader: Option<u64>,
) -> Observed {
    let mut o = Observed {
        committer: Some(committer),
        leader,
        ..Default::default()
    };
    for (r, l) in &state.leases {
        let live = l.is_live(now);
        match r {
            LeaseResource::Node(n) => {
                o.nodes.insert(n.clone(), (l.epoch, live));
            }
            // The committer replaying a log isn't running it.
            LeaseResource::Dataset(d) if live && l.holder.role != HolderRole::Replay => {
                o.datasets.insert(
                    d.clone(),
                    Held {
                        node: l.holder.node.clone().unwrap_or_default(),
                        epoch: l.epoch,
                    },
                );
            }
            _ => {}
        }
    }
    o
}

/// An event the committer noticed: (level, kind, subject, message).
pub type Noticed = (Level, EventKind, Subject, String);

/// What changed from `old` to `new`, as events. `state` says how leases
/// ended; `format` says what format a dataset is, if known.
pub fn diff(
    old: Option<&Observed>,
    new: &Observed,
    state: &crate::cluster::state::ClusterState,
    format: &dyn Fn(&str) -> Option<DatasetFormat>,
) -> Vec<Noticed> {
    let mut out = Vec::new();
    let me = new.committer.clone().expect("observed by a committer");
    let Some(old) = old else {
        out.push((
            Level::Info,
            EventKind::EventsStarted,
            Subject::Pool,
            format!("Activity log started (committer on {})", me.node),
        ));
        return out;
    };
    match &old.committer {
        Some(c) if c == &me => {}
        Some(c) if c.node == me.node => out.push((
            Level::Info,
            EventKind::CommitterStarted,
            Subject::node(&me.node),
            format!("Committer restarted on {}", me.node),
        )),
        Some(c) => out.push((
            Level::Warn,
            EventKind::CommitterMoved,
            Subject::node(&me.node),
            format!("{} took over as committer from {}", me.node, c.node),
        )),
        None => out.push((
            Level::Info,
            EventKind::CommitterStarted,
            Subject::node(&me.node),
            format!("{} is the committer", me.node),
        )),
    }
    // Nodes.
    for (n, &(epoch, live)) in &new.nodes {
        let subject = Subject::node(n);
        match old.nodes.get(n) {
            None | Some((_, false)) if live => out.push((
                Level::Info,
                EventKind::NodeUp,
                subject,
                format!("Node {} joined the cluster", n),
            )),
            Some(&(e, true)) if live && e != epoch => out.push((
                Level::Warn,
                EventKind::NodeRestarted,
                subject,
                format!("Node {} restarted", n),
            )),
            Some((_, true)) if !live => {
                let released = state
                    .leases
                    .get(&LeaseResource::node(n))
                    .is_some_and(|l| l.released && l.epoch == epoch);
                if released {
                    out.push((
                        Level::Info,
                        EventKind::NodeLeft,
                        subject,
                        format!("Node {} left the cluster", n),
                    ));
                } else {
                    out.push((
                        Level::Error,
                        EventKind::NodeDown,
                        subject,
                        format!("Node {} stopped responding", n),
                    ));
                }
            }
            _ => {}
        }
    }
    // Datasets whose node let go without saying so (its agent reports
    // clean starts and stops itself).
    let live_nodes: BTreeSet<&String> = new
        .nodes
        .iter()
        .filter(|(_, (_, live))| *live)
        .map(|(n, _)| n)
        .collect();
    for (d, was) in &old.datasets {
        let f = format(d);
        let what = label_of(f);
        let subject = Subject::dataset(f, d);
        match new.datasets.get(d) {
            Some(now) if now.epoch == was.epoch => {}
            Some(now) => {
                if !live_nodes.contains(&was.node) && now.node != was.node {
                    out.push((
                        Level::Warn,
                        EventKind::DatasetFailover,
                        subject,
                        format!(
                            "{} '{}' moved from {} to {} ({} is down)",
                            what, d, was.node, now.node, was.node
                        ),
                    ));
                }
            }
            None => {
                let lease = state.leases.get(&LeaseResource::dataset(d));
                let clean = lease.is_none_or(|l| l.released || l.epoch != was.epoch);
                // A live node reports a lease it lost itself.
                if !clean && !live_nodes.contains(&was.node) {
                    out.push((
                        Level::Warn,
                        EventKind::DatasetLost,
                        subject,
                        format!("{} '{}' stopped: {} is down", what, d, was.node),
                    ));
                }
            }
        }
    }
    if new.leader != old.leader {
        match &new.leader {
            Some(id) => out.push((
                Level::Info,
                EventKind::RaftLeader,
                Subject::Cluster,
                format!(
                    "Raft leader is now {}",
                    crate::cluster::members::node_name(*id)
                ),
            )),
            None => out.push((
                Level::Warn,
                EventKind::RaftLeader,
                Subject::Cluster,
                "The cluster has no Raft leader".into(),
            )),
        }
    }
    out
}

/// What a dataset is, for messages ("Volume", "Bucket"...).
/// A dataset of format `f` (None: not known), for messages.
pub fn label_of(f: Option<DatasetFormat>) -> &'static str {
    f.map_or("Dataset", format_label)
}

pub fn format_label(f: DatasetFormat) -> &'static str {
    match f {
        DatasetFormat::Block => "Volume",
        DatasetFormat::S3 => "Bucket",
        DatasetFormat::Queue => "Queue",
        DatasetFormat::Etcd => "Etcd",
        DatasetFormat::Kv => "Kv",
        DatasetFormat::Posix => "Filesystem",
        DatasetFormat::Events => "Events",
        DatasetFormat::Vms => "VM definitions",
    }
}

/// `1.5 GiB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{} B", n)
    } else if v.fract() == 0.0 {
        format!("{} {}", v, UNITS[u])
    } else {
        format!("{:.1} {}", v, UNITS[u])
    }
}

// ---- reporting from a node ----

/// Events kept in memory while there's no committer to take them.
const SINK_CAP: usize = 5000;
/// Events sent at once.
const BATCH: usize = 500;

struct SinkState {
    queue: VecDeque<NewEvent>,
    /// Dropped (the queue was full) since the last send.
    dropped: u64,
    /// For `once`: the message last reported, by subject.
    last: HashMap<Subject, String>,
}

/// Where a node reports events: queued here, sent to the committer by
/// [`EventSink::run`].
#[derive(Clone)]
pub struct EventSink {
    node: String,
    state: Arc<std::sync::Mutex<SinkState>>,
    wake: Arc<tokio::sync::Notify>,
}

impl EventSink {
    pub fn new(node: &str) -> Self {
        EventSink {
            node: node.to_string(),
            state: Arc::new(std::sync::Mutex::new(SinkState {
                queue: VecDeque::new(),
                dropped: 0,
                last: HashMap::new(),
            })),
            wake: Default::default(),
        }
    }

    /// Report an event (now, on this node).
    pub fn emit(
        &self,
        level: Level,
        kind: EventKind,
        subject: Subject,
        message: impl Into<String>,
        by: Option<String>,
    ) {
        let e = NewEvent {
            ts_ms: crate::fs::now_ms(),
            node: self.node.clone(),
            level,
            kind,
            subject,
            message: message.into(),
            by,
        };
        let mut s = self.state.lock().unwrap();
        if s.queue.len() >= SINK_CAP {
            s.queue.pop_front();
            s.dropped += 1;
        }
        s.queue.push_back(e);
        drop(s);
        self.wake.notify_one();
    }

    pub fn info(&self, kind: EventKind, subject: Subject, message: impl Into<String>) {
        self.emit(Level::Info, kind, subject, message, None);
    }

    pub fn warn(&self, kind: EventKind, subject: Subject, message: impl Into<String>) {
        self.emit(Level::Warn, kind, subject, message, None);
    }

    pub fn error(&self, kind: EventKind, subject: Subject, message: impl Into<String>) {
        self.emit(Level::Error, kind, subject, message, None);
    }

    /// Report a condition of `subject` that's retried (a VM that won't
    /// start, every tick): once, and again only if its message changes.
    /// Returns whether it was reported.
    pub fn once(
        &self,
        level: Level,
        kind: EventKind,
        subject: Subject,
        message: impl Into<String>,
    ) -> bool {
        let message = message.into();
        {
            let mut s = self.state.lock().unwrap();
            if s.last.get(&subject) == Some(&message) {
                return false;
            }
            s.last.insert(subject.clone(), message.clone());
        }
        self.emit(level, kind, subject, message, None);
        true
    }

    /// The condition of `subject` is over (reported again next time).
    /// Returns whether it was on.
    pub fn clear(&self, subject: &Subject) -> bool {
        self.state.lock().unwrap().last.remove(subject).is_some()
    }

    /// Take up to `n` queued events (and a note of any dropped).
    fn take(&self, n: usize) -> Vec<NewEvent> {
        let mut s = self.state.lock().unwrap();
        let mut out = Vec::new();
        if s.dropped > 0 {
            out.push(NewEvent {
                ts_ms: crate::fs::now_ms(),
                node: self.node.clone(),
                level: Level::Warn,
                kind: EventKind::EventsDropped,
                subject: Subject::node(&self.node),
                message: format!(
                    "{} events from {} were dropped while no committer was reachable",
                    s.dropped, self.node
                ),
                by: None,
            });
            s.dropped = 0;
        }
        let k = n.min(s.queue.len());
        out.extend(s.queue.drain(..k));
        out
    }

    /// Put back events that couldn't be sent (in front: they're older).
    fn put_back(&self, events: Vec<NewEvent>) {
        let mut s = self.state.lock().unwrap();
        for e in events.into_iter().rev() {
            if s.queue.len() >= SINK_CAP {
                s.dropped += 1;
            } else {
                s.queue.push_front(e);
            }
        }
    }

    /// Queued, not yet sent.
    #[cfg(test)]
    pub fn pending(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }

    /// Send what's queued, forever, through `send` (to the committer).
    pub async fn run<F, Fut>(self, send: F)
    where
        F: Fn(Vec<NewEvent>) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        loop {
            let batch = self.take(BATCH);
            if batch.is_empty() {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(5), self.wake.notified())
                        .await;
                // Gather what else comes in at once.
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                continue;
            }
            if send(batch.clone()).await.is_err() {
                self.put_back(batch);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/events_tests.rs"]
mod tests;
