//! Raft RPCs between `listen` nodes: JSON over the same HTTP port that
//! serves the disks (routes in `cluster::routes`).

use super::TypeConfig;
use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::sync::Arc;

type NodeId = u64;

/// Peers are looked up in this node's own view of the pool (membership
/// records no addresses).
#[derive(Clone)]
pub struct Network {
    pub http: reqwest::Client,
    pub peers: Arc<BTreeMap<NodeId, String>>,
    pub fingerprint: String,
}

pub struct Conn {
    target: NodeId,
    addr: Option<String>,
    http: reqwest::Client,
    fingerprint: String,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Conn {
        Conn {
            target,
            addr: self.peers.get(&target).cloned(),
            http: self.http.clone(),
            fingerprint: self.fingerprint.clone(),
        }
    }
}

impl Conn {
    async fn call<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
        option: &RPCOption,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let Some(addr) = &self.addr else {
            let e = std::io::Error::other(format!("no address for node {}", self.target));
            return Err(RPCError::Unreachable(Unreachable::new(&e)));
        };
        let url = format!("{}{}", addr, path);
        let resp = self
            .http
            .post(&url)
            .timeout(option.hard_ttl())
            .header(super::FINGERPRINT_HEADER, &self.fingerprint)
            .header(super::TO_HEADER, self.target.to_string())
            .json(req)
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    RPCError::Unreachable(Unreachable::new(&e))
                } else {
                    RPCError::Network(NetworkError::new(&e))
                }
            })?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            let e = std::io::Error::other(format!(
                "{} refused us: its pool config disagrees with ours (disk order?)",
                addr
            ));
            return Err(RPCError::Unreachable(Unreachable::new(&e)));
        }
        let result: Result<Resp, RaftError<NodeId, E>> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call("/api/v1/raft/append", &rpc, &option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.call("/api/v1/raft/snapshot", &rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call("/api/v1/raft/vote", &rpc, &option).await
    }
}
