//! `GET /api/v1/cluster/status` (the same answer `raidish cluster status`
//! prints).

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Member {
    pub id: u64,
    /// None for the node that answered.
    pub addr: Option<String>,
    pub disks: Vec<usize>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Lease {
    pub owner: String,
    pub desc: String,
    pub epoch: u64,
    pub expires_ms: u64,
    pub released: bool,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Default)]
pub struct State {
    pub epoch: u64,
    pub leases: BTreeMap<String, Lease>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Status {
    pub node: String,
    pub id: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub members: Vec<Member>,
    pub fingerprint: String,
    pub state: State,
    /// The answering node's clock.
    pub now_ms: u64,
    /// Our clock when the answer arrived (not part of the response).
    #[serde(skip)]
    pub fetched_ms: f64,
}

impl Status {
    /// The node serving pool disk `disk`.
    pub fn owner_of(&self, disk: usize) -> Option<&Member> {
        self.members.iter().find(|m| m.disks.contains(&disk))
    }

    /// Milliseconds until a lease expires, in the node's clock.
    pub fn remaining_ms(&self, lease: &Lease, now_ms: f64) -> f64 {
        let node_now = self.now_ms as f64 + (now_ms - self.fetched_ms);
        lease.expires_ms as f64 - node_now
    }
}

pub async fn fetch() -> Result<Status, String> {
    let resp = gloo_net::http::Request::get("/api/v1/cluster/status")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let mut status: Status = resp.json().await.map_err(|e| e.to_string())?;
    status.fetched_ms = crate::session::now_ms();
    Ok(status)
}
