//! Cluster membership, derived from the pool config: every remote disk URL
//! names a `listen` node, and local paths are this node. There's nothing
//! else to configure.
//!
//! * Disks are grouped by node (`scheme://host:port`), so a node serving
//!   several disks is one member.
//! * A node's id is the lowest pool position of its disks. Every node lists
//!   the same disks in the same order (local or by URL), so they all compute
//!   the same ids without exchanging anything.
//! * A URL pointing at this machine and this node's port is also this node.
//! * A pool of only local disks is a cluster of one.
//!
//! Membership records no addresses. Each node knows the others from its own
//! config, and nobody has to know its own address as the others see it.

use crate::config::disk_server_base;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    /// This node's id.
    pub me: u64,
    /// The other nodes: id → base URL.
    pub peers: BTreeMap<u64, String>,
    /// Pool disk positions of every node (this one too).
    pub disks: BTreeMap<u64, Vec<usize>>,
    /// Identifies the layout. Nodes whose configs disagree (different order,
    /// different grouping) have different fingerprints and refuse each other.
    pub fingerprint: String,
}

impl Members {
    /// A cluster of one (no pool configured).
    pub fn solo() -> Self {
        Members {
            me: 0,
            peers: BTreeMap::new(),
            disks: BTreeMap::from([(0, vec![])]),
            fingerprint: "solo".into(),
        }
    }

    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.disks.keys().copied()
    }
}

/// Derive the members from the pool's disk `paths` (in pool order).
/// `layout` is anything that pins down the pool's shape (it goes into the
/// fingerprint). `is_me(base)` says whether a URL base is this node.
pub fn derive(
    paths: &[String],
    layout: &str,
    is_me: impl Fn(&str) -> bool,
) -> Result<Members, String> {
    // Owner of each disk: None for this node, else the node's base URL.
    let owners: Vec<Option<String>> = paths
        .iter()
        .map(|p| disk_server_base(p).filter(|b| !is_me(b)))
        .collect();

    let mut disks: BTreeMap<Option<&str>, Vec<usize>> = BTreeMap::new();
    for (i, o) in owners.iter().enumerate() {
        disks.entry(o.as_deref()).or_default().push(i);
    }
    let Some(mine) = disks.get(&None) else {
        return Err(
            "none of the pool's disks are on this node (list this node's disks as local \
             paths, or check --port)"
                .into(),
        );
    };
    let me = mine[0] as u64;

    let mut members = Members {
        me,
        peers: BTreeMap::new(),
        disks: BTreeMap::new(),
        fingerprint: String::new(),
    };
    for (owner, idx) in &disks {
        let id = idx[0] as u64;
        members.disks.insert(id, idx.clone());
        if let Some(base) = owner {
            members.peers.insert(id, base.to_string());
        }
    }

    // Same on every node: which node owns each disk, by id.
    let ids: Vec<u64> = owners
        .iter()
        .map(|o| disks[&o.as_deref()][0] as u64)
        .collect();
    let h = blake3::hash(format!("{:?}|{}", ids, layout).as_bytes());
    members.fingerprint = h.to_hex()[..16].to_string();
    Ok(members)
}

/// Is `base` (`http://host:port`) this machine on `port`? The host is ours
/// if it's loopback or we can bind one of its addresses.
pub fn is_local_base(base: &str, port: u16) -> bool {
    let Ok(url) = url::Url::parse(base) else {
        return false;
    };
    if url.port_or_known_default() != Some(port) {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    use std::net::ToSocketAddrs;
    let Ok(addrs) = (host, port).to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|a| a.ip().is_loopback() || std::net::TcpListener::bind((a.ip(), 0)).is_ok())
}
