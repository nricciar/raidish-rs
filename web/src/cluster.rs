//! `GET /api/v1/cluster/status` (the same answer `raidish cluster status`
//! prints).

pub use raidish::cluster::state::Lease;

/// The cluster's status, and when we got it.
#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    pub answer: raidish::cluster::Status,
    /// Our clock when the answer arrived.
    pub fetched_ms: f64,
}

impl std::ops::Deref for Status {
    type Target = raidish::cluster::Status;
    fn deref(&self) -> &Self::Target {
        &self.answer
    }
}

impl Status {
    /// Milliseconds until a lease expires, in the node's clock.
    pub fn remaining_ms(&self, lease: &Lease, now_ms: f64) -> f64 {
        let node_now = self.now_ms as f64 + (now_ms - self.fetched_ms);
        lease.expires_ms as f64 - node_now
    }
}

pub async fn fetch() -> Result<Status, String> {
    let resp = crate::session::get("/api/v1/cluster/status")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(Status {
        answer: resp.json().await.map_err(|e| e.to_string())?,
        fetched_ms: crate::session::now_ms(),
    })
}
