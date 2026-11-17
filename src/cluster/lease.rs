//! Client side of leases: what a writing command (`nbd`, `block`, `fsck
//! --repair`, ...) uses to become the pool's one writer.
//!
//! The holder renews every `ttl / 3`, and every [`RETRY_PAUSE`] after a
//! failed renewal (a leader election, typically). If it can't renew for most
//! of a TTL it assumes the lease is gone and stops (see [`HeldLease::hold`]).
//! The disk servers fence it in any case: once anyone uses a newer epoch,
//! writes with the old one are refused.

use super::LeaseOp;
use super::state::Response;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const LEASE_TTL: Duration = Duration::from_secs(10);

/// How soon a failed renewal is tried again.
const RETRY_PAUSE: Duration = Duration::from_millis(500);

/// Talks to whichever `listen` node answers (non-leaders forward).
#[derive(Clone)]
pub struct LeaseClient {
    endpoints: Vec<String>,
    http: reqwest::Client,
}

impl LeaseClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        LeaseClient {
            endpoints: endpoints
                .into_iter()
                .map(|e| e.trim_end_matches('/').to_string())
                .collect(),
            http: crate::tls::http_builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .build()
                .expect("http client"),
        }
    }

    /// Send `op` to the first endpoint that answers.
    pub async fn call(&self, op: &LeaseOp) -> Result<Response, String> {
        let mut errors = Vec::new();
        for ep in &self.endpoints {
            let url = format!("{}/api/v1/cluster/lease", ep);
            match self.http.post(&url).json(op).send().await {
                Ok(resp) => match resp.json::<Result<Response, String>>().await {
                    Ok(Ok(r)) => return Ok(r),
                    Ok(Err(e)) => errors.push(format!("{}: {}", ep, e)),
                    Err(e) => errors.push(format!("{}: {}", ep, e)),
                },
                Err(e) => errors.push(format!("{}: {}", ep, e)),
            }
        }
        Err(if errors.is_empty() {
            "no cluster endpoints".into()
        } else {
            errors.join("; ")
        })
    }

    /// Fetch `/api/v1/cluster/status` from the first node that answers.
    pub async fn status(&self) -> Result<super::Status, String> {
        let mut errors = Vec::new();
        for ep in &self.endpoints {
            let url = format!("{}/api/v1/cluster/status", ep);
            match self.http.get(&url).send().await {
                Ok(resp) => match resp.json().await {
                    Ok(s) => return Ok(s),
                    Err(e) => errors.push(format!("{}: {}", ep, e)),
                },
                Err(e) => errors.push(format!("{}: {}", ep, e)),
            }
        }
        Err(errors.join("; "))
    }
}

/// An owner id unique to this process.
pub fn owner_id() -> String {
    let host = std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".into());
    format!("{}:{}:{}", host, std::process::id(), super::now_ms())
}

pub struct HeldLease {
    pub client: LeaseClient,
    pub resource: String,
    pub owner: String,
    pub epoch: u64,
    /// Set by `release`: the renew loop stops quietly.
    released: std::sync::atomic::AtomicBool,
    /// When the last successful grant or renewal was *sent* (the lease is
    /// good for a TTL from then, at the earliest).
    renewed: std::sync::Mutex<Instant>,
}

impl HeldLease {
    /// Take `resource`, or say who has it.
    pub async fn acquire(
        client: LeaseClient,
        resource: &str,
        desc: &str,
    ) -> Result<HeldLease, String> {
        let owner = owner_id();
        let sent = Instant::now();
        let op = LeaseOp::Acquire {
            resource: resource.into(),
            owner: owner.clone(),
            desc: desc.into(),
            ttl_ms: LEASE_TTL.as_millis() as u64,
        };
        match client.call(&op).await? {
            Response::Granted { epoch, .. } => Ok(HeldLease {
                client,
                resource: resource.into(),
                owner,
                epoch,
                released: Default::default(),
                renewed: std::sync::Mutex::new(sent),
            }),
            Response::Held {
                owner,
                desc,
                expires_ms,
            } => Err(format!(
                "{} is held by {} ({}) for another {:.1}s",
                resource,
                desc,
                owner,
                expires_ms.saturating_sub(super::now_ms()) as f64 / 1000.0
            )),
            other => Err(format!("unexpected lease reply: {:?}", other)),
        }
    }

    /// Keep renewing in the background, for as long as anyone holds this
    /// `HeldLease`: once the last `Arc` is dropped the lease is released
    /// (whoever let go of it, a failed start included, needn't remember
    /// to). `on_lost` runs (once) when the lease is lost or can't be
    /// renewed for most of a TTL.
    pub fn hold(self: &Arc<Self>, on_lost: impl FnOnce(String) + Send + 'static) {
        let weak = Arc::downgrade(self);
        let (client, resource, owner, epoch) = (
            self.client.clone(),
            self.resource.clone(),
            self.owner.clone(),
            self.epoch,
        );
        tokio::spawn(async move {
            let ttl = LEASE_TTL;
            // Conservative: the lease counts from when the request was sent.
            let mut valid_from = Instant::now();
            let mut pause = ttl / 3;
            loop {
                tokio::time::sleep(pause).await;
                let Some(me) = weak.upgrade() else {
                    // Dropped without `release`: give it back rather than
                    // renew a lease nobody uses (it would block everyone
                    // else, this process's own next attempt included).
                    let op = LeaseOp::Release {
                        resource,
                        owner,
                        epoch,
                    };
                    if let Err(e) = client.call(&op).await {
                        eprintln!("lease: release failed (it will expire): {}", e);
                    }
                    return;
                };
                if me.released.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let sent = Instant::now();
                let op = LeaseOp::Renew {
                    resource: me.resource.clone(),
                    owner: me.owner.clone(),
                    epoch: me.epoch,
                    ttl_ms: ttl.as_millis() as u64,
                };
                // Never wait on a renewal past the point where the lease
                // has to count as gone.
                let left = (ttl * 4 / 5).saturating_sub(valid_from.elapsed());
                let result = match tokio::time::timeout(left, me.client.call(&op)).await {
                    Ok(r) => r,
                    Err(_) => Err("timed out".to_string()),
                };
                match result {
                    Ok(Response::Granted { epoch, .. }) if epoch == me.epoch => {
                        valid_from = sent;
                        *me.renewed.lock().unwrap() = sent;
                        pause = ttl / 3;
                    }
                    Ok(_) if me.released.load(std::sync::atomic::Ordering::SeqCst) => return,
                    Ok(other) => {
                        on_lost(format!("lease on {} lost ({:?})", me.resource, other));
                        return;
                    }
                    Err(e) => {
                        if valid_from.elapsed() >= ttl * 4 / 5 {
                            on_lost(format!(
                                "lease on {} could not be renewed: {}",
                                me.resource, e
                            ));
                            return;
                        }
                        // Most likely the leader has just gone and the
                        // others are electing a new one: keep asking, often,
                        // so the renewal lands as soon as there is one.
                        eprintln!("lease: renew failed (will retry): {}", e);
                        pause = RETRY_PAUSE;
                    }
                }
            }
        });
    }

    /// Is the lease provably still held? True while the last renewal is
    /// well within a TTL old, leaving a third of it as margin for clock rate
    /// differences and in-flight writes: whatever this holder does while
    /// it's true happens before anyone else can be granted the lease.
    pub fn fresh(&self) -> bool {
        self.renewed.lock().unwrap().elapsed() < LEASE_TTL * 2 / 3
    }

    pub async fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let op = LeaseOp::Release {
            resource: self.resource.clone(),
            owner: self.owner.clone(),
            epoch: self.epoch,
        };
        if let Err(e) = self.client.call(&op).await {
            eprintln!("lease: release failed (it will expire): {}", e);
        }
    }
}
