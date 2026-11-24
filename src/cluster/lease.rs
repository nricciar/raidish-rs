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
use crate::types::{Epoch, Holder, Holding, LeaseRef, LeaseResource};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub const LEASE_TTL: Duration = Duration::from_secs(10);

/// How soon a failed renewal is tried again.
const RETRY_PAUSE: Duration = Duration::from_millis(500);

/// The longest one renewal attempt is waited on (several fit in the time a
/// lease has left once a renewal is due).
const RENEW_ATTEMPT: Duration = Duration::from_millis(2500);

/// Where leases are granted: the cluster's Raft state machine, in
/// process (`Cluster`, which forwards to the leader) or over HTTP
/// (`HttpLeases`, to any `listen` node).
#[async_trait::async_trait]
pub trait LeaseAuthority: Send + Sync {
    async fn call(&self, op: &LeaseOp) -> Result<Response, String>;
    async fn status(&self) -> Result<super::Status, String>;
}

/// Longest one call is waited on, in process as over HTTP (the HTTP
/// client's own timeout).
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

#[async_trait::async_trait]
impl LeaseAuthority for super::Cluster {
    async fn call(&self, op: &LeaseOp) -> Result<Response, String> {
        tokio::time::timeout(CALL_TIMEOUT, self.lease(op, false))
            .await
            .unwrap_or_else(|_| Err(format!("no answer in {:?}", CALL_TIMEOUT)))
    }
    async fn status(&self) -> Result<super::Status, String> {
        Ok(super::Cluster::status(self))
    }
}

/// A handle on a [`LeaseAuthority`]: what lease holders keep.
#[derive(Clone)]
pub struct LeaseClient(Arc<dyn LeaseAuthority>);

impl LeaseClient {
    /// Over HTTP, to `endpoints` (any `listen` nodes).
    pub fn new(endpoints: Vec<String>) -> Self {
        LeaseClient(Arc::new(HttpLeases::new(endpoints)))
    }

    /// This process's own cluster member: no HTTP to itself.
    pub fn local(cluster: Arc<super::Cluster>) -> Self {
        LeaseClient(cluster)
    }

    pub async fn call(&self, op: &LeaseOp) -> Result<Response, String> {
        self.0.call(op).await
    }

    pub async fn status(&self) -> Result<super::Status, String> {
        self.0.status().await
    }
}

/// Talks to whichever `listen` node answers (non-leaders forward).
pub struct HttpLeases {
    endpoints: Vec<String>,
    http: reqwest::Client,
    /// The endpoint that answered last: asked first next time. Usually
    /// the leader, which answers without forwarding, so it wins.
    preferred: AtomicUsize,
}

/// How long a call waits on the preferred endpoint before asking the
/// others as well.
const HEDGE_AFTER: Duration = Duration::from_millis(200);

impl HttpLeases {
    pub fn new(endpoints: Vec<String>) -> Self {
        HttpLeases {
            endpoints: endpoints
                .into_iter()
                .map(|e| e.trim_end_matches('/').to_string())
                .collect(),
            http: crate::tls::http_client(Some(CALL_TIMEOUT)).expect("http client"),
            preferred: AtomicUsize::new(0),
        }
    }

    /// `op` sent to endpoint `i`.
    async fn ask(&self, i: usize, op: &LeaseOp) -> (usize, Result<Response, String>) {
        let ep = &self.endpoints[i];
        let url = format!("{}/api/v1/cluster/lease", ep);
        let r = match self.http.post(&url).json(op).send().await {
            Ok(resp) => match resp.json::<Result<Response, String>>().await {
                Ok(r) => r,
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        };
        (i, r.map_err(|e| format!("{}: {}", ep, e)))
    }
}

#[async_trait::async_trait]
impl LeaseAuthority for HttpLeases {
    /// Ask the endpoint that answered last; if it fails, or hasn't
    /// answered within `HEDGE_AFTER`, ask the rest too, and take the first
    /// answer. One node that's slow or half gone (shutting down, swapping)
    /// mustn't hold the answer up: tried in turn, it took the whole of a
    /// renewal's time, and a lease that was fine counted as lost. Asking
    /// every node every time (as this once did) had each follower forward
    /// a copy to the leader: every op applied up to once per node. Each op
    /// is idempotent (a renewal, an acquire by the same owner, a release),
    /// so the duplicates a hedge can still cause are harmless.
    async fn call(&self, op: &LeaseOp) -> Result<Response, String> {
        use futures::stream::{FuturesUnordered, StreamExt};
        let n = self.endpoints.len();
        if n == 0 {
            return Err("no cluster endpoints".into());
        }
        let first = self.preferred.load(Ordering::Relaxed) % n;
        let mut pending = FuturesUnordered::new();
        pending.push(self.ask(first, op));
        let mut others = (1..n).map(|k| (first + k) % n);
        let hedge = tokio::time::sleep(HEDGE_AFTER);
        tokio::pin!(hedge);
        let mut hedged = n == 1;
        let mut errors = Vec::new();
        loop {
            tokio::select! {
                Some((i, r)) = pending.next() => match r {
                    Ok(r) => {
                        self.preferred.store(i, Ordering::Relaxed);
                        return Ok(r);
                    }
                    Err(e) => {
                        errors.push(e);
                        if !hedged {
                            hedged = true;
                            pending.extend(others.by_ref().map(|i| self.ask(i, op)));
                        }
                        if pending.is_empty() {
                            return Err(errors.join("; "));
                        }
                    }
                },
                _ = &mut hedge, if !hedged => {
                    hedged = true;
                    pending.extend(others.by_ref().map(|i| self.ask(i, op)));
                }
            }
        }
    }

    /// Fetch `/api/v1/cluster/status` from the first node that answers.
    async fn status(&self) -> Result<super::Status, String> {
        use crate::net::{Answer, try_each};
        try_each(self.endpoints.iter().map(String::as_str), |ep| async move {
            let url = format!("{}/api/v1/cluster/status", ep);
            match self.http.get(&url).send().await {
                Ok(resp) => match resp.json().await {
                    Ok(s) => Answer::Got(s),
                    Err(e) => Answer::Unreachable(e.to_string()),
                },
                Err(e) => Answer::Unreachable(e.to_string()),
            }
        })
        .await
        .map_err(|e: crate::net::Tried<String>| e.to_string())
    }
}

/// An owner id unique to this call: two leases taken in the same
/// millisecond by one process must not look like one holder, nor two
/// processes with one host name and pid (a restarted container is pid 1
/// again) if the clock went back between them (audit R4c).
pub fn owner_id() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let host = std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".into());
    format!(
        "{}:{}:{}.{}.{:016x}",
        host,
        std::process::id(),
        super::now_ms(),
        n,
        crate::fs::process_seed()
    )
}

/// Where a [`HeldLease`] stands, from this holder's side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum HoldState {
    Held = 0,
    /// A `transfer` is in flight: a renewal refused meanwhile (the transfer
    /// went through first) isn't a lost lease.
    HandingOff = 1,
    /// Let go (`release`, or a transfer went through): the renew loop
    /// stops quietly.
    Released = 2,
    /// A transfer may have gone through (no answer), or the lease is gone:
    /// not `fresh`. The next renewal settles it: granted, it's `Held`
    /// again; refused, it's lost (`on_lost`). The same transfer may be
    /// tried again meanwhile.
    InDoubt = 3,
    /// Renewal was refused, or couldn't be had in time: the lease is gone
    /// (`on_lost` has run). Never `fresh` again, so nothing more is acked
    /// under it while the last renewal is still recent (structural triage
    /// I-15).
    Lost = 4,
}

/// A [`HoldState`] shared with the renew loop.
#[derive(Default)]
struct AtomicHoldState(std::sync::atomic::AtomicU8);

impl AtomicHoldState {
    fn get(&self) -> HoldState {
        match self.0.load(std::sync::atomic::Ordering::SeqCst) {
            0 => HoldState::Held,
            1 => HoldState::HandingOff,
            3 => HoldState::InDoubt,
            4 => HoldState::Lost,
            _ => HoldState::Released,
        }
    }

    fn set(&self, s: HoldState) {
        self.0.store(s as u8, std::sync::atomic::Ordering::SeqCst);
    }

    /// Move `from` → `to` if it's still `from`.
    fn advance(&self, from: HoldState, to: HoldState) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.0
            .compare_exchange(from as u8, to as u8, SeqCst, SeqCst)
            .is_ok()
    }
}

pub struct HeldLease {
    pub client: LeaseClient,
    pub resource: LeaseResource,
    pub owner: String,
    pub epoch: Epoch,
    state: AtomicHoldState,
    /// When the last successful grant or renewal was *sent* (the lease is
    /// good for a TTL from then, at the earliest).
    renewed: std::sync::Mutex<Instant>,
}

/// Why [`HeldLease::transfer`] didn't hand the lease over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// The lease is gone (lost or expired): nobody holds it as this holder.
    Lost(String),
    /// Refused for good (released, being handed off, not the holder): this
    /// holder still has it, unless it was released.
    Refused(String),
    /// No answer: it may have gone through. The same transfer again says
    /// which (repeated, it succeeds with the same epoch).
    NoAnswer(String),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::Lost(m) | TransferError::Refused(m) | TransferError::NoAnswer(m) => {
                f.write_str(m)
            }
        }
    }
}

impl HeldLease {
    /// This lease, as its holder: what a request made under it carries.
    pub fn holding(&self) -> Holding {
        Holding::new(self.owner.clone(), self.epoch)
    }

    /// This lease, by resource and epoch.
    pub fn lease_ref(&self) -> LeaseRef {
        LeaseRef::new(self.resource.clone(), self.epoch)
    }

    /// Take `resource`, or say who has it.
    pub async fn acquire(
        client: LeaseClient,
        resource: LeaseResource,
        holder: Holder,
    ) -> Result<HeldLease, String> {
        let owner = owner_id();
        let sent = Instant::now();
        let op = LeaseOp::Acquire {
            resource: resource.clone(),
            owner: owner.clone(),
            holder,
            ttl_ms: LEASE_TTL.as_millis() as u64,
        };
        match client.call(&op).await? {
            Response::Granted { epoch, .. } => {
                Ok(HeldLease::adopt(client, resource, &owner, epoch, sent))
            }
            Response::Held {
                owner,
                holder,
                expires_ms,
            } => Err(format!(
                "{} is held by {} ({}) for another {:.1}s",
                resource,
                holder,
                owner,
                expires_ms.saturating_sub(super::now_ms()) as f64 / 1000.0
            )),
            other => Err(format!("unexpected lease reply: {:?}", other)),
        }
    }

    /// A lease this process was granted some other way: handed over by
    /// its previous holder ([`HeldLease::transfer`] to `owner`, which this
    /// process chose). `sent` is no later than when the grant was made (when
    /// this process asked for it, say): the lease counts from then.
    pub fn adopt(
        client: LeaseClient,
        resource: LeaseResource,
        owner: &str,
        epoch: Epoch,
        sent: Instant,
    ) -> HeldLease {
        HeldLease {
            client,
            resource,
            owner: owner.into(),
            epoch,
            state: Default::default(),
            renewed: std::sync::Mutex::new(sent),
        }
    }

    /// Hand this lease straight to `to_owner` (see `Request::Transfer`):
    /// it's never free in between. On success this holder has let go (its
    /// renewals stop quietly, `on_lost` doesn't run) and the new holder's
    /// epoch is returned, for it to [`HeldLease::adopt`]. See
    /// [`TransferError`] for what a failure leaves.
    pub async fn transfer(
        &self,
        to_owner: &str,
        to_holder: Holder,
    ) -> Result<Epoch, TransferError> {
        if !self.state.advance(HoldState::Held, HoldState::HandingOff)
            && !self
                .state
                .advance(HoldState::InDoubt, HoldState::HandingOff)
        {
            return Err(TransferError::Refused(match self.state.get() {
                HoldState::HandingOff => format!("lease on {} is being handed off", self.resource),
                HoldState::Lost => format!("lease on {} was lost", self.resource),
                _ => format!("lease on {} was released", self.resource),
            }));
        }
        let op = LeaseOp::Transfer {
            resource: self.resource.clone(),
            from: Holding::new(self.owner.clone(), self.epoch),
            to_owner: to_owner.into(),
            to_holder,
            ttl_ms: LEASE_TTL.as_millis() as u64,
        };
        let r = self.client.call(&op).await;
        match r {
            Ok(Response::Granted { epoch, .. }) => {
                self.state.set(HoldState::Released);
                Ok(epoch)
            }
            other => {
                // Back to held only if it certainly didn't go through, and
                // unless `release` ran meanwhile. Lost, or no answer (it may
                // have): in doubt, so not `fresh`, until a renewal says.
                let back = match &other {
                    Ok(Response::Refused(_)) => HoldState::Held,
                    _ => HoldState::InDoubt,
                };
                self.state.advance(HoldState::HandingOff, back);
                Err(match other {
                    Ok(Response::Lost) => {
                        TransferError::Lost(format!("lease on {} was lost", self.resource))
                    }
                    Ok(Response::Refused(why)) => TransferError::Refused(why),
                    Ok(other) => {
                        TransferError::NoAnswer(format!("unexpected lease reply: {:?}", other))
                    }
                    Err(e) => TransferError::NoAnswer(e),
                })
            }
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
            // Conservative: the lease counts from when the request that
            // granted it was sent (not from now: a holder that took a while
            // between acquiring and holding has less left, audit R3a).
            let mut valid_from = *weak
                .upgrade()
                .map(|me| *me.renewed.lock().unwrap())
                .get_or_insert_with(Instant::now);
            // The first renewal a third of a TTL after the grant was sent,
            // not after now (audit R3a).
            let mut pause = (ttl / 3).saturating_sub(valid_from.elapsed());
            loop {
                tokio::time::sleep(pause).await;
                let Some(me) = weak.upgrade() else {
                    // Dropped without `release`: give it back rather than
                    // renew a lease nobody uses (it would block everyone
                    // else, this process's own next attempt included).
                    let op = LeaseOp::Release {
                        resource,
                        holder: Holding::new(owner, epoch),
                    };
                    if let Err(e) = client.call(&op).await {
                        eprintln!("lease: release failed (it will expire): {}", e);
                    }
                    return;
                };
                if me.state.get() == HoldState::Released {
                    return;
                }
                let sent = Instant::now();
                let op = LeaseOp::Renew {
                    resource: me.resource.clone(),
                    holder: Holding::new(me.owner.clone(), me.epoch),
                    ttl_ms: ttl.as_millis() as u64,
                };
                // Never wait on a renewal past the point where the lease
                // has to count as gone, nor on one attempt for long: a
                // renewal lost on the way (a leader election) is sent again
                // while there's still time.
                let left = (ttl * 4 / 5).saturating_sub(valid_from.elapsed());
                let attempt = left.min(RENEW_ATTEMPT);
                let result = match tokio::time::timeout(attempt, me.client.call(&op)).await {
                    Ok(r) => r,
                    Err(_) => Err("timed out".to_string()),
                };
                match result {
                    Ok(Response::Granted { epoch, .. }) if epoch == me.epoch => {
                        valid_from = sent;
                        *me.renewed.lock().unwrap() = sent;
                        pause = ttl / 3;
                        // Renewed as this holder: a transfer in doubt didn't
                        // go through.
                        me.state.advance(HoldState::InDoubt, HoldState::Held);
                    }
                    Ok(_) => match me.state.get() {
                        HoldState::Released | HoldState::Lost => return,
                        // Refused while a transfer is in flight: it went
                        // through (`transfer` then lets go), or it didn't
                        // and the next renewal finds out.
                        HoldState::HandingOff => pause = RETRY_PAUSE,
                        HoldState::Held | HoldState::InDoubt => {
                            me.state.set(HoldState::Lost);
                            on_lost(format!("lease on {} lost ({:?})", me.resource, result));
                            return;
                        }
                    },
                    Err(e) => {
                        if valid_from.elapsed() >= ttl * 4 / 5 {
                            // Let go of meanwhile (released, or handed off
                            // and released): nothing was lost.
                            if matches!(me.state.get(), HoldState::Released | HoldState::Lost) {
                                return;
                            }
                            me.state.set(HoldState::Lost);
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
    /// Never while it's being handed off or once released: the new holder
    /// may be acting already (audit R3b).
    pub fn fresh(&self) -> bool {
        self.state.get() == HoldState::Held
            && self.renewed.lock().unwrap().elapsed() < LEASE_TTL * 2 / 3
    }

    pub async fn release(&self) {
        self.state.set(HoldState::Released);
        let op = LeaseOp::Release {
            resource: self.resource.clone(),
            holder: Holding::new(self.owner.clone(), self.epoch),
        };
        if let Err(e) = self.client.call(&op).await {
            eprintln!("lease: release failed (it will expire): {}", e);
        }
    }
}
