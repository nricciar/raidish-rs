//! The slow volume work, in the background: handing a volume from here
//! (`handoff::HandOff`) and stopping one cleanly ([`stop_clean`]), each up
//! to its clients' drain (30 s) and the last sync. The agent loop spawns
//! them ([`Background`]), skips their volumes while they run, and collects
//! them when they're done: one slow volume holds up neither the rest nor
//! the VMs.
//!
//! Each holds its volume's slot as a marker (`table::Marker`: `HandingOff`,
//! `Stopping`) until it ends, however it ends, and `select!`s its waits
//! against the volume's lease and the writer's: lost, it lets go at once
//! (without writing), and the writer's fence ([`Background::join`]) isn't
//! held up.

use super::table::Marker;
use super::{Owned, WriterGen, WriterParts};
use crate::attach::Attacher;
use crate::events::{EventKind, EventSink, Level, Subject};
use crate::types::DatasetName;
use crate::{Lost, note, or_lost};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// What the background work needs of the agent.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub node: String,
    pub shared: Arc<super::handoff::Shared>,
    pub attacher: Arc<dyn Attacher>,
    pub events: EventSink,
}

/// What a background task does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Work {
    HandOff { to: String },
    Stop,
}

struct Task {
    name: DatasetName,
    wgen: WriterGen,
    work: Work,
    /// Err: it's still run here (a handoff that didn't start), or what
    /// went wrong on the way.
    handle: JoinHandle<Result<(), String>>,
}

/// The volume work under way in the background.
#[derive(Default)]
pub(crate) struct Background {
    tasks: Vec<Task>,
}

impl Background {
    /// Whether `name` has work under way.
    pub(crate) fn busy(&self, name: &DatasetName) -> bool {
        self.tasks.iter().any(|t| &t.name == name)
    }

    pub(crate) fn spawn(
        &mut self,
        name: &DatasetName,
        wgen: WriterGen,
        work: Work,
        f: impl Future<Output = Result<(), String>> + Send + 'static,
    ) {
        self.tasks.push(Task {
            name: name.clone(),
            wgen,
            work,
            handle: tokio::spawn(f),
        });
    }

    /// What finished since the last look.
    pub(crate) async fn collect(&mut self) -> Vec<(DatasetName, Work, Result<(), String>)> {
        let (done, running) = std::mem::take(&mut self.tasks)
            .into_iter()
            .partition(|t: &Task| t.handle.is_finished());
        self.tasks = running;
        let mut out = Vec::new();
        for t in done {
            out.push((t.name, t.work, flatten(t.handle.await)));
        }
        out
    }

    /// Wait for all that runs on writer `wgen` (its lease is lost: they end
    /// at once), logging how each went.
    pub(crate) async fn join(&mut self, wgen: WriterGen) {
        let (mine, rest) = std::mem::take(&mut self.tasks)
            .into_iter()
            .partition(|t: &Task| t.wgen == wgen);
        self.tasks = rest;
        for t in mine {
            if let Err(e) = flatten(t.handle.await) {
                eprintln!("agent: {:?} of '{}': {}", t.work, t.name, e);
            }
        }
    }
}

fn flatten(r: Result<Result<(), String>, tokio::task::JoinError>) -> Result<(), String> {
    r.unwrap_or_else(|e| Err(format!("its task failed: {}", e)))
}

/// Stop running `name` (`v`, out of the table, `marker` in its place):
/// detach its device, close its export (its clients drain), sync and
/// detach the dataset, release the lease. Its lease or the writer's lost
/// on the way: dropped without writing, the lease not released.
pub(crate) async fn stop_clean(
    cx: Ctx,
    w: WriterParts,
    writer_lost: Lost,
    name: DatasetName,
    v: Owned,
    marker: Marker,
) -> Result<(), String> {
    let Owned {
        lease,
        lost,
        export,
        attach,
        ..
    } = v;
    if attach.device().is_some()
        && let Err(e) = cx.attacher.detach(&name, export.transport).await
    {
        eprintln!("agent: detaching '{}': {}", name, e);
    }
    // Cut short, the export drains in the background (`ExportHandle`).
    let synced = match or_lost(export.close(), &lost, &writer_lost).await {
        Some(closed) => {
            if let Err(e) = closed {
                eprintln!("agent: unexporting '{}': {}", name, e);
            }
            or_lost(w.fs.detach_dataset(&name), &lost, &writer_lost).await
        }
        None => None,
    };
    match synced {
        Some(synced) => {
            if let Err(e) = synced {
                // Its intent log is replayed once the lease is released.
                eprintln!("agent: last sync of '{}': {}", name, e);
                w.fs.abandon_dataset(&name);
            }
            lease.release().await;
            note(
                &cx.events,
                Level::Info,
                EventKind::VolumeStopped,
                Subject::volume(&name),
                format!("Volume '{}' stopped on {}", name, cx.node),
            );
        }
        None => {
            w.fs.abandon_dataset(&name);
            lost_on_the_way(&cx, &name, &lost);
        }
    }
    drop(marker);
    Ok(())
}

/// Say so when volume `name`'s own lease was lost while it was stopped or
/// handed off (a lost writer is said once, for all, by its fence).
pub(crate) fn lost_on_the_way(cx: &Ctx, name: &DatasetName, lost: &Lost) {
    if lost.is_lost() {
        note(
            &cx.events,
            Level::Error,
            EventKind::VolumeLost,
            Subject::volume(name),
            format!(
                "Volume '{}' stopped on {}: its lease was lost",
                name, cx.node
            ),
        );
    }
}
