//! "Sealed" is loud (CA-3c, `docs/ca-service-plan.md`): every member's
//! seal asked live (`GET /api/v1/pki/status?all=true`), a warning when no
//! node is unsealed and something runs out before one is, and the CA's
//! events in the activity log.
//!
//! The activity log is `raidish-committer`'s; the CA sees it only through
//! [`CaEvents`], which `raidish listen` implements over its `EventSink`.

use crate::now_ms;
use crate::service::CaService;
use raidish_types::api_types::{EventKind, Level, Subject};
use raidish_types::pki::{NodeSeal, PkiState, PkiStatus, ProfileKind};
use raidish_types::types::NodeId;
use std::collections::BTreeMap;
use std::time::Duration;

const DAY_MS: u64 = 86_400_000;
/// Warned about this long before a node cert or the CRL runs out.
pub const WARN_BEFORE_MS: u64 = 10 * DAY_MS;
/// How long a member gets to answer `?all=true`.
const ASK_TIMEOUT: Duration = Duration::from_secs(3);

/// Where the CA's events go (the activity log).
pub trait CaEvents: Send + Sync {
    fn emit(
        &self,
        level: Level,
        kind: EventKind,
        subject: Subject,
        message: String,
        by: Option<String>,
    );
    /// A condition of `subject`: reported once, and again only if its
    /// message changes. Whether it was reported.
    fn once(&self, level: Level, kind: EventKind, subject: Subject, message: String) -> bool;
    /// The condition of `subject` is over. Whether it was on.
    fn clear(&self, subject: &Subject) -> bool;
}

fn day(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "?".into())
}

/// When each member's node certificate runs out (its latest live one).
pub fn node_certs_end(pki: &PkiState, now: u64) -> BTreeMap<NodeId, u64> {
    let mut end = BTreeMap::new();
    for (serial, c) in &pki.certs {
        if c.identity.profile != ProfileKind::Node || !pki.is_live(serial, now) {
            continue;
        }
        if let Some(m) = c.identity.member {
            let e = end.entry(m).or_insert(0);
            *e = (*e).max(c.not_after_ms);
        }
    }
    end
}

/// `member`'s node certificate, if it runs out within [`WARN_BEFORE_MS`].
pub fn node_cert_due(pki: &PkiState, member: NodeId, now: u64) -> Option<String> {
    let end = *node_certs_end(pki, now).get(&member)?;
    (end.saturating_sub(now) < WARN_BEFORE_MS)
        .then(|| format!("{}'s certificate runs out on {}", member, day(end)))
}

/// The CRL, if it runs out within [`WARN_BEFORE_MS`].
pub fn crl_due(pki: &PkiState, now: u64) -> Option<String> {
    let crl = pki.crl.as_ref()?;
    (crl.next_update_ms.saturating_sub(now) < WARN_BEFORE_MS)
        .then(|| format!("the CRL runs out on {}", day(crl.next_update_ms)))
}

/// The warning for a cluster where no node is unsealed (`all`: every
/// member answered), if something runs out before one is.
pub fn unsealed_none(pki: &PkiState, all: bool, now: u64) -> Option<String> {
    let mut due: Vec<String> = node_certs_end(pki, now)
        .keys()
        .filter_map(|m| node_cert_due(pki, *m, now))
        .collect();
    due.extend(crl_due(pki, now));
    if due.is_empty() {
        return None;
    }
    Some(format!(
        "no {}node is unsealed, and {}: unseal one (`raidish ca unseal`) to renew and re-sign",
        if all { "" } else { "reachable " },
        due.join(", ")
    ))
}

fn reason(what: String) -> String {
    format!(
        "{}, and no node is unsealed to renew it: unseal one (`raidish ca unseal`)",
        what
    )
}

impl CaService {
    /// Every member's seal, asked now (this node's from itself), in
    /// member order.
    ///
    /// A member that says it's unsealed but hasn't applied the CA's latest
    /// seal yet counts as sealed: its DEK goes when it does.
    pub async fn ask_all(&self) -> Vec<NodeSeal> {
        let epoch = self.current().await.map_or(0, |p| p.seal.epoch);
        let me = NodeSeal {
            node: self.cluster().name.clone(),
            reachable: true,
            unsealed: self.unsealed(),
            unsealed_by: self.unsealed_by(),
            error: None,
        };
        let peers = self.cluster().members.peers.iter().map(|(id, base)| {
            let url = format!("{}/api/v1/pki/status", base);
            let http = self.http();
            async move {
                let asked = async {
                    let r = http.get(&url).send().await.map_err(|e| e.to_string())?;
                    if !r.status().is_success() {
                        return Err(format!("{}", r.status()));
                    }
                    r.json::<PkiStatus>().await.map_err(|e| e.to_string())
                };
                let r = match tokio::time::timeout(ASK_TIMEOUT, asked).await {
                    Ok(r) => r,
                    Err(_) => Err("no answer in 3 s".into()),
                };
                match r {
                    Ok(s) => {
                        let current = s.pki.as_ref().is_some_and(|p| p.seal_epoch >= epoch);
                        (
                            *id,
                            NodeSeal {
                                node: s.node,
                                reachable: true,
                                unsealed: s.unsealed && current,
                                unsealed_by: s.unsealed_by.filter(|_| current),
                                error: None,
                            },
                        )
                    }
                    Err(e) => (
                        *id,
                        NodeSeal {
                            node: id.to_string(),
                            reachable: false,
                            unsealed: false,
                            unsealed_by: None,
                            error: Some(e),
                        },
                    ),
                }
            }
        });
        let mut all: Vec<(NodeId, NodeSeal)> = futures::future::join_all(peers).await;
        all.push((self.cluster().members.me, me));
        all.sort_by_key(|(id, _)| *id);
        all.into_iter().map(|(_, s)| s).collect()
    }

    /// `GET /api/v1/pki/status?all=true`: this node's status, every
    /// member's seal, and the warning for a cluster none of them can sign
    /// for (in place of each node's own CRL warning).
    pub async fn status_all(&self) -> PkiStatus {
        let mut s = self.status_of(false).await;
        s.nodes = self.ask_all().await;
        if !s.nodes.iter().any(|n| n.unsealed)
            && let Some(pki) = self.current().await
        {
            let all = s.nodes.iter().all(|n| n.reachable);
            s.warnings.extend(unsealed_none(&pki, all, now_ms()));
        }
        s
    }

    /// From the CA's loop, on a sealed node: warn the activity log
    /// (`ca.unsealed_none`, once) if no node is unsealed and this node's
    /// own certificate runs out soon. The lowest-numbered member that
    /// answers speaks for the CRL, so it isn't said three times.
    pub(crate) async fn check_unsealed_none(&self) {
        let Some(events) = self.events.clone() else {
            return;
        };
        let me = self.cluster().members.me;
        let mine = Subject::node(&self.cluster().name);
        let crl = Subject::Cluster;
        let Some(pki) = self.cluster().pki() else {
            return;
        };
        let now = now_ms();
        let own = node_cert_due(&pki, me, now);
        let crl_msg = crl_due(&pki, now);
        let warned = self
            .warned_unsealed_none
            .load(std::sync::atomic::Ordering::Relaxed);
        if self.unsealed() || (own.is_none() && crl_msg.is_none()) {
            if warned {
                events.clear(&mine);
                events.clear(&crl);
                self.warned_unsealed_none
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }
        let nodes = self.ask_all().await;
        if nodes.iter().any(|n| n.unsealed) {
            if warned {
                events.clear(&mine);
                events.clear(&crl);
                self.warned_unsealed_none
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }
        let speaks_for_crl = nodes
            .iter()
            .find(|n| n.reachable)
            .is_some_and(|n| n.node == self.cluster().name);
        let mut said = false;
        if let Some(w) = own {
            events.once(Level::Warn, EventKind::CaUnsealedNone, mine, reason(w));
            said = true;
        }
        if let Some(w) = crl_msg.filter(|_| speaks_for_crl) {
            events.once(Level::Warn, EventKind::CaUnsealedNone, crl, reason(w));
            said = true;
        }
        if said {
            self.warned_unsealed_none
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// An event, if there's somewhere to send it.
    pub(crate) fn event(
        &self,
        kind: EventKind,
        subject: Subject,
        message: String,
        by: Option<String>,
    ) {
        if let Some(e) = &self.events {
            e.emit(Level::Info, kind, subject, message, by);
        }
    }

    /// This node, as an event's subject.
    pub(crate) fn me_subject(&self) -> Subject {
        Subject::node(&self.cluster().name)
    }
}

/// Once the CA is in Raft: if this node is sealed, say so at start, and
/// warn if no other node is unsealed either. A few seconds' grace first,
/// for a node key or an unseal-once file to do its work.
pub(crate) async fn announce(ca: std::sync::Weak<CaService>) {
    loop {
        let Some(c) = ca.upgrade() else { return };
        if c.cluster().pki().is_some() && !c.is_staged().await {
            break;
        }
        drop(c);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let Some(ca) = ca.upgrade() else { return };
    if ca.unsealed() {
        return;
    }
    println!("ca: sealed: signing waits for an unsealed node (raidish ca unseal)");
    let nodes = ca.ask_all().await;
    if nodes.iter().any(|n| n.unsealed) {
        return;
    }
    let all = nodes.iter().all(|n| n.reachable);
    let pki = ca.cluster().pki();
    match pki.and_then(|p| unsealed_none(&p, all, now_ms())) {
        Some(w) => eprintln!("ca: WARNING: {}", w),
        None => eprintln!(
            "ca: WARNING: no {}node is unsealed: nothing is signed (joins, renewals, tokens) \
             until one is",
            if all { "" } else { "reachable " }
        ),
    }
}

#[cfg(test)]
#[path = "tests/loud_tests.rs"]
mod tests;
