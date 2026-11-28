//! Keeping a served pool healthy (see `spawn_health_monitor`): the
//! storage core's scrub and resilver (`crate::scan`), driven by disk
//! reconnects and reported to the activity log (`crate::events`).

use crate::disk::PoolDevice;
use crate::events::{EventKind, Subject};
use crate::fs::LabelState;
use crate::types::Txg;
use raidish_core::coords::PoolDiskIdx;
use raidish_core::role::{CommitterFs, WriterFs};
use std::sync::Arc;
use std::time::Duration;

/// A volume writer's half of keeping the pool healthy: the committer's
/// health monitor resilvers the pool except the space lent to writers (its
/// writes are fenced out of it), so each writer repairs its own lent space
/// every `interval` (see `FileSystem::heal_lent`). `who` prefixes the log.
pub fn spawn_lent_healer<D: PoolDevice + 'static>(
    fs: Arc<WriterFs<D>>,
    interval: Duration,
    who: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match fs.heal_lent().await {
                Ok(Some(r)) if r.repaired() > 0 || !r.is_clean() => {
                    eprintln!("{}: lent space: {}", who, r);
                }
                Ok(_) => {}
                Err(e) => eprintln!("{}: healing lent space: {}", who, e),
            }
        }
    })
}

/// Keep a served pool healthy: whenever a disk (re)connects, check its label
/// (a replaced disk is fenced and fully resilvered), and whenever an online
/// disk is degraded, resilver it. Also re-checks periodically, since misses
/// are only noticed at commit time.
pub fn spawn_health_monitor<D: PoolDevice + 'static>(
    fs: Arc<CommitterFs<D>>,
    interval: Duration,
    events: Option<crate::events::EventSink>,
) -> tokio::task::JoinHandle<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PoolDiskIdx>();
    {
        let pool = fs.dev.pool();
        for d in pool.disks().indexes() {
            if let Some(mut events) = pool.disks().disk(d).reconnects() {
                let tx = tx.clone();
                tokio::spawn(async move {
                    while events.changed().await.is_ok() {
                        if tx.send(d).is_err() {
                            return;
                        }
                    }
                });
            }
        }
    }
    drop(tx);

    tokio::spawn(async move {
        // A resilver that can't finish clean (say, an unreadable record) is
        // retried with backoff instead of rescanning every interval.
        let mut wait = interval;
        loop {
            if fs.needs_resilver() {
                match fs.resilver().await {
                    Ok(report) => {
                        if report.since > Txg::ZERO || !report.cleared.is_empty() {
                            eprintln!("{}", report);
                            if let Some(ev) = &events {
                                ev.clear(&Subject::Pool);
                                let line = report.summary();
                                // Clean but not certifying (a stripe being
                                // written, labels only on degraded disks):
                                // nothing cleared yet, retried.
                                if report.certifies() {
                                    ev.info(
                                        EventKind::ResilverDone,
                                        Subject::Pool,
                                        format!("Resilver finished: {}", line),
                                    );
                                } else {
                                    ev.warn(
                                        EventKind::ResilverIncomplete,
                                        Subject::Pool,
                                        format!("Resilver incomplete (will retry): {}", line),
                                    );
                                }
                            }
                        }
                        wait = if report.is_clean() {
                            interval
                        } else {
                            (wait * 2).min(interval * 20)
                        };
                    }
                    Err(e) => {
                        eprintln!("resilver failed: {}", e);
                        if let Some(ev) = &events {
                            ev.once(
                                crate::events::Level::Error,
                                EventKind::ResilverFailed,
                                Subject::Pool,
                                format!("Resilver failed: {}", e),
                            );
                        }
                        wait = (wait * 2).min(interval * 20);
                    }
                }
            }
            tokio::select! {
                Some(d) = rx.recv() => {
                    wait = interval;
                    if fs.adopt_disk(d).await == LabelState::Foreign {
                        // Persist the degraded mark before resilvering.
                        let _ = fs.commit_txg().await;
                    }
                }
                _ = tokio::time::sleep(wait) => {}
            }
        }
    })
}
