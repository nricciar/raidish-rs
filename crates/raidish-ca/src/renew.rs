//! A node renews its own certificate (CA-4a, `docs/ca-service-plan.md`):
//! at 2/3 of its life, at once if its issuer isn't the active one any
//! more, or when asked (`POST /api/v1/pki/self/renew`).
//!
//! A new key and CSR each time. Signed in-process on an unsealed node,
//! else by a peer over mTLS with the current certificate (`/renew`). The
//! files are swapped together (`raidish_auth::tls::swap_identity`), then
//! the process's [`LiveIdentity`]: every acceptor and connector presents
//! the new one from the next handshake on, and what is open stays open.
//!
//! It checks at start, when the CA changes, hourly, and when the next
//! renewal is due. A failure retries after a minute (less for a short-lived
//! cert), doubling to an hour, and says so once (`ca.renew_failed`).

use crate::api::{Issued, RenewRequest};
use crate::now_ms;
use crate::service::CaService;
use raidish_auth::http::LiveIdentity;
use raidish_types::api_types::{EventKind, Level};
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::pem::PemObject;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use zeroize::Zeroizing;

/// Checked at least this often, due or not.
const CHECK_EVERY: Duration = Duration::from_secs(3600);
const BACKOFF_MAX: Duration = Duration::from_secs(3600);

/// This node's own certificate: what it serves and presents, and where
/// it's kept.
#[derive(Clone)]
pub struct NodeIdentity {
    pub live: Arc<LiveIdentity>,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// The renewer's state, beside the service.
#[derive(Default)]
pub(crate) struct Renewer {
    /// Renew at the next check, due or not.
    pub(crate) force: AtomicBool,
    /// A `ca.renew_failed` is on.
    failing: AtomicBool,
    /// "Not on record" was said.
    said_unknown: AtomicBool,
}

/// The client a node renews through a peer with: the certificate it
/// presents is the one renewed, so never a pooled connection or a resumed
/// TLS session, which would carry the one before.
pub(crate) fn client(tls: Option<Arc<rustls::ClientConfig>>) -> Result<reqwest::Client, String> {
    let b = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .connect_timeout(raidish_auth::tls::CONNECT_TIMEOUT)
        .timeout(Duration::from_secs(20));
    let b = match tls {
        Some(c) => {
            let mut c = (*c).clone();
            c.resumption = rustls::client::Resumption::disabled();
            b.use_preconfigured_tls(c)
        }
        None => b,
    };
    b.build().map_err(|e| format!("HTTP client: {}", e))
}

pub(crate) fn start(ca: &Arc<CaService>) {
    if ca.identity.is_some() {
        tokio::spawn(run(Arc::downgrade(ca), ca.renew_wake.clone()));
    }
}

async fn run(ca: Weak<CaService>, wake: Arc<tokio::sync::Notify>) {
    let mut backoff: Option<Duration> = None;
    loop {
        let wait = {
            let Some(ca) = ca.upgrade() else { return };
            match ca.renew_once().await {
                Ok(next) => {
                    backoff = None;
                    next.min(CHECK_EVERY)
                }
                Err((e, first)) => {
                    let b = match backoff {
                        None => first,
                        Some(b) => (b * 2).min(BACKOFF_MAX),
                    };
                    backoff = Some(b);
                    ca.renew_failed(&e, b);
                    b
                }
            }
        };
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// When a certificate from `not_before_ms` to `not_after_ms` is due: 2/3
/// through what's usable of it (its `notBefore` is backdated).
pub fn due_at(not_before_ms: u64, not_after_ms: u64) -> u64 {
    let start = not_before_ms + raidish_auth::ca::BACKDATE_MS.min(not_after_ms - not_before_ms);
    start + (not_after_ms.saturating_sub(start)) * 2 / 3
}

impl CaService {
    /// Ask for a renewal now (the route): at once, due or not.
    pub fn renew_now(&self) -> Result<(), crate::CaError> {
        if self.identity.is_none() {
            return Err(crate::CaError::new(
                409,
                "this node has no certificate of its own to renew",
            ));
        }
        self.renewer.force.store(true, Ordering::SeqCst);
        self.renew_wake.notify_one();
        Ok(())
    }

    /// Renew if due. How long until the next check; on failure, why and
    /// the first retry's wait.
    async fn renew_once(&self) -> Result<Duration, (String, Duration)> {
        let Some(id) = &self.identity else {
            return Ok(CHECK_EVERY);
        };
        let leaf = id.live.leaf();
        let facts = raidish_auth::ca::facts(&leaf).map_err(|e| (e, Duration::from_secs(60)))?;
        let usable = facts
            .not_after_ms
            .saturating_sub(facts.not_before_ms + raidish_auth::ca::BACKDATE_MS);
        let first = Duration::from_millis((usable / 10).clamp(1000, 60_000));
        let Some(pki) = self.cluster().pki() else {
            return Ok(CHECK_EVERY);
        };
        if self.is_staged().await {
            return Ok(first);
        }
        let now = now_ms();
        let force = self.renewer.force.swap(false, Ordering::SeqCst);
        let Some(rec) = pki.certs.get(&facts.serial) else {
            // Just renewed through a peer, and this view is behind; or a
            // certificate this CA never issued (it's left alone).
            if !self.renewer.said_unknown.swap(true, Ordering::Relaxed) {
                println!(
                    "ca: this node's certificate ({}) isn't on record: not renewed",
                    facts.serial
                );
            }
            if force {
                return Err(("this node's certificate isn't on record".into(), first));
            }
            return Ok(first);
        };
        self.renewer.said_unknown.store(false, Ordering::Relaxed);
        let retired = pki.active_issuer().is_some_and(|(a, _)| a != rec.issuer);
        let at = due_at(facts.not_before_ms, facts.not_after_ms);
        if !force && !retired && now < at {
            return Ok(Duration::from_millis(at - now));
        }
        let why = match (force, retired) {
            (true, _) => "asked",
            (_, true) => "its issuer is retired",
            _ => "due",
        };
        // Asked for, it's retried until it's done.
        let again = |e: String| {
            if force {
                self.renewer.force.store(true, Ordering::SeqCst);
            }
            (e, first)
        };
        let (issued, key_pem) = self.renew_self(&leaf).await.map_err(again)?;
        self.install(id, &issued.cert_pem, &key_pem)
            .map_err(again)?;
        println!(
            "ca: renewed this node's certificate ({}): {} → {}",
            why, facts.serial, issued.serial
        );
        if self.renewer.failing.swap(false, Ordering::Relaxed) {
            self.event(
                EventKind::CaRenewed,
                self.me_subject(),
                format!(
                    "{}'s certificate renewed ({})",
                    self.cluster().name,
                    issued.serial
                ),
                None,
            );
        }
        // The new one's due time, from the top.
        Ok(Duration::ZERO)
    }

    /// A certificate in place of `leaf`, from this node if it can sign,
    /// else from a peer.
    async fn renew_self(&self, leaf: &[u8]) -> Result<(Issued, Zeroizing<String>), String> {
        // The CN asked for is ignored: the identity is the record's.
        let (key_pem, csr_pem) = raidish_auth::ca::generate_csr(&self.cluster().name)?;
        let issued = if self.unsealed() {
            match self.renew(leaf, &csr_pem).await {
                Ok(i) => Some(i),
                Err(e) if e.status == 503 => None,
                Err(e) => return Err(e.msg),
            }
        } else {
            None
        };
        let issued = match issued {
            Some(i) => i,
            None => self.renew_through_peers(&csr_pem).await?,
        };
        Ok((issued, key_pem))
    }

    async fn renew_through_peers(&self, csr_pem: &str) -> Result<Issued, String> {
        let req = RenewRequest {
            csr_pem: csr_pem.to_string(),
        };
        let mut errs = Vec::new();
        for (node, base) in self.peer_urls() {
            let url = format!("{}/api/v1/pki/renew", base.trim_end_matches('/'));
            match self.renew_http.post(&url).json(&req).send().await {
                Ok(r) if r.status().is_success() => {
                    return r.json().await.map_err(|e| format!("{}: {}", url, e));
                }
                Ok(r) => {
                    let s = r.status();
                    let body = r.text().await.unwrap_or_default();
                    // 503: that one can't sign either; anything else would
                    // be the same everywhere.
                    if s.as_u16() != 503 {
                        return Err(format!("{}: {}: {}", node, s, body.trim()));
                    }
                    errs.push(format!("{}: {}", node, body.trim()));
                }
                Err(e) => errs.push(format!("{}: {}", node, e)),
            }
        }
        Err(if errs.is_empty() {
            "sealed, and no peers to ask".into()
        } else {
            format!("no node could sign: {}", errs.join("; "))
        })
    }

    /// The files, then what's served and presented.
    fn install(&self, id: &NodeIdentity, cert_pem: &str, key_pem: &str) -> Result<(), String> {
        let chain = raidish_auth::ca::pem_certs(cert_pem)?;
        let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
            .map_err(|e| format!("the new key: {}", e))?;
        raidish_auth::tls::swap_identity(&id.cert_path, &id.key_path, cert_pem, key_pem)?;
        id.live.set(chain, key)
    }

    fn renew_failed(&self, e: &str, retry: Duration) {
        eprintln!(
            "ca: renewing this node's certificate: {} (again in {}s)",
            e,
            retry.as_secs()
        );
        if !self.renewer.failing.swap(true, Ordering::Relaxed)
            && let Some(events) = &self.events
        {
            events.emit(
                Level::Warn,
                EventKind::CaRenewFailed,
                self.me_subject(),
                format!(
                    "{}'s certificate isn't renewing: {}",
                    self.cluster().name,
                    e
                ),
                None,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn due_two_thirds_through_the_usable_life() {
        let h = raidish_auth::ca::BACKDATE_MS;
        assert_eq!(due_at(0, h + 30_000), h + 20_000);
        // Shorter than the backdate (not one of ours): due at its end.
        assert_eq!(due_at(0, 3_000), 3_000);
    }
}
