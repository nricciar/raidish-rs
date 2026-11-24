//! `raidish csi`: a Container Storage Interface driver, so Kubernetes pods
//! can use raidish volumes as persistent volumes.
//!
//! It runs on every node next to `listen` (as root: it formats and mounts),
//! and serves the Identity, Controller and Node services on one unix socket.
//! The CSI sidecars (external-provisioner, external-attacher,
//! node-driver-registrar) and kubelet are its clients.
//!
//! It adds no machinery of its own. The node agent (see `raidish::agent`)
//! already runs a volume on the first live node of its `nodes` list,
//! exports it and attaches the kernel device, so:
//!
//! * CreateVolume / DeleteVolume create and delete a raidish volume (the
//!   CSI volume id is its name). A new volume isn't placed anywhere.
//! * ControllerPublishVolume(node) sets `nodes: [node]`, `automount: nvme`
//!   and enables it. That node's agent takes the volume's `ds/` lease and
//!   attaches it over NVMe/TCP. Since the list names only that node, the
//!   volume never fails over somewhere its pod isn't.
//! * ControllerUnpublishVolume disables it, and waits for that node to let
//!   the lease go (or for it to expire, if that node is dead).
//! * NodeStageVolume waits for this node's agent to report the device,
//!   formats it if it's blank and mounts it at the staging path;
//!   NodePublishVolume bind-mounts that into the pod. A raw block volume
//!   (`volumeMode: Block`) skips staging: NodePublishVolume bind-mounts the
//!   device itself onto the file kubelet hands it.
//!
//! A filesystem volume is single-node (ReadWriteOnce / ReadWriteOncePod):
//! Kubernetes' attach/detach controller won't publish one to a second node
//! while it's still published to another, and the `ds/` lease would refuse
//! a second writer anyway.
//!
//! A raw block volume may also be ReadWriteMany (MULTI_NODE_MULTI_WRITER),
//! for a VM's disk while it migrates between nodes. ControllerPublishVolume
//! then attaches it to the node as well (`ClusterState::attached`, one node
//! at a time) instead of replacing its node, and ControllerUnpublishVolume
//! detaches it from just that node. Every attached node exports it and
//! attaches its device: one writes it, the others proxy their I/O to that
//! one, and when the writer's node is detached it hands the volume to
//! another attached node with no I/O lost (see `raidish::agent::handoff`).
//! That needs peer I/O (every node with TLS); without it, the volume runs
//! on its first node only and the others never see a device.

pub(crate) use raidish_auth::tls;
pub(crate) use raidish_cluster as cluster;
pub(crate) use raidish_committer as committer;
pub(crate) use raidish_core::{inode, volume, writer};
pub(crate) use raidish_types::{api_types, types};
pub(crate) use raidish_vm::proc;

pub mod pb {
    #![allow(clippy::all, dead_code)]
    tonic::include_proto!("csi.v1");
}

use crate::committer::CommitClient;
use crate::committer::PoolAdmin;
use crate::inode::Transport;
use crate::writer::CommitError;
use pb::controller_server::{Controller, ControllerServer};
use pb::identity_server::{Identity, IdentityServer};
use pb::node_server::{Node, NodeServer};
use pb::volume_capability::AccessType;
use pb::volume_capability::access_mode::Mode;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status};

/// The driver's name (`provisioner:` in a StorageClass).
pub const DRIVER_NAME: &str = "csi.raidish.io";

/// Volumes are created in whole MiB.
const SIZE_UNIT: u64 = 1 << 20;
/// Size of a volume asked for with no capacity range.
const DEFAULT_SIZE: u64 = 1 << 30;
/// How long NodeStageVolume waits for the agent to attach the device before
/// failing (kubelet calls it again).
const ATTACH_WAIT: Duration = Duration::from_secs(90);
/// How long ControllerUnpublishVolume waits for the volume to stop.
const RELEASE_WAIT: Duration = Duration::from_secs(45);

type Rpc<T> = Result<Response<T>, Status>;

#[derive(Clone)]
struct Driver {
    /// This node's name in the cluster (`node-0`): the CSI node id.
    node: String,
    /// Every `listen` node (the committer and lease endpoints).
    endpoints: Vec<String>,
    committer: CommitClient,
    /// This node's `listen` API (its agent's status).
    agent: String,
    http: reqwest::Client,
}

fn commit_status(e: CommitError) -> Status {
    use crate::types::ErrorKind as K;
    let msg = e.to_string();
    match e.kind() {
        K::NotFound => Status::not_found(msg),
        K::Exists => Status::already_exists(msg),
        K::Invalid | K::NotDir | K::NotFile => Status::invalid_argument(msg),
        K::Conflict | K::NotEmpty => Status::failed_precondition(msg),
        K::NoSpace => Status::resource_exhausted(msg),
        K::Unavailable => Status::unavailable(msg),
        K::Forbidden => Status::permission_denied(msg),
        K::Internal => Status::internal(msg),
    }
}

/// The same rule as everywhere else (`crate::types::DatasetName`), so a
/// volume made any other way can be used here too.
fn valid_name(name: &str) -> Result<crate::types::DatasetName, Status> {
    crate::types::DatasetName::user(name).map_err(|e| Status::invalid_argument(e.to_string()))
}

/// Whether `cap` asks for a volume on several nodes at once.
fn multi_node(cap: &pb::VolumeCapability) -> bool {
    cap.access_mode
        .map(|m| Mode::try_from(m.mode).unwrap_or(Mode::Unknown))
        == Some(Mode::MultiNodeMultiWriter)
}

/// Access modes we can honour: one node at a time, or (raw block only)
/// several, each exporting it and one of them writing it.
fn supported(cap: &pb::VolumeCapability) -> Result<(), String> {
    let mode = cap
        .access_mode
        .map(|m| Mode::try_from(m.mode).unwrap_or(Mode::Unknown))
        .unwrap_or(Mode::Unknown);
    match mode {
        Mode::SingleNodeWriter
        | Mode::SingleNodeReaderOnly
        | Mode::SingleNodeSingleWriter
        | Mode::SingleNodeMultiWriter => {}
        Mode::MultiNodeMultiWriter => {
            // A filesystem mounted on two nodes at once would be torn up:
            // only a raw device (a VM's disk moving between nodes) is.
            if !matches!(cap.access_type, Some(AccessType::Block(_))) {
                return Err(format!(
                    "access mode {} is for raw block volumes only (volumeMode: Block)",
                    mode.as_str_name()
                ));
            }
        }
        m => {
            return Err(format!(
                "access mode {} isn't supported (single node, or multi-node \
                 multi-writer raw block)",
                m.as_str_name()
            ));
        }
    }
    match &cap.access_type {
        Some(AccessType::Mount(m)) => {
            let fs = fs_type(m);
            if !matches!(fs, "ext4" | "xfs") {
                return Err(format!("filesystem '{}' isn't supported (ext4, xfs)", fs));
            }
            Ok(())
        }
        Some(AccessType::Block(_)) => Ok(()),
        None => Err("no access type".into()),
    }
}

fn fs_type(m: &pb::volume_capability::MountVolume) -> &str {
    if m.fs_type.is_empty() {
        "ext4"
    } else {
        &m.fs_type
    }
}

/// Run a command; its stderr on failure.
async fn run(cmd: &str, args: &[&str]) -> Result<String, String> {
    let mut c = tokio::process::Command::new(cmd);
    c.args(args);
    crate::proc::run(c).await
}

/// Is something mounted exactly at `path`?
async fn is_mounted(path: &str) -> bool {
    run("findmnt", &["-rn", "--mountpoint", path])
        .await
        .is_ok_and(|s| !s.is_empty())
}

async fn unmount(path: &str) -> Result<(), Status> {
    if is_mounted(path).await {
        run("umount", &[path]).await.map_err(Status::internal)?;
    }
    Ok(())
}

impl Driver {
    fn leases(&self) -> crate::cluster::lease::LeaseClient {
        crate::cluster::lease::LeaseClient::new(self.endpoints.clone())
    }

    /// The nodes `name` is attached to (see `ClusterState::attached`).
    async fn attached(&self, name: &str) -> Result<Vec<String>, Status> {
        let status = self.leases().status().await.map_err(Status::unavailable)?;
        Ok(status.state.attached.get(name).cloned().unwrap_or_default())
    }

    /// Attach `name` to `node` too, or no longer.
    async fn attach(&self, name: &str, node: &str, on: bool) -> Result<(), Status> {
        let op = crate::cluster::LeaseOp::Attach {
            volume: name.to_string(),
            node: node.to_string(),
            on,
        };
        self.leases()
            .call(&op)
            .await
            .map(|_| ())
            .map_err(Status::unavailable)
    }

    /// Publish `v` to `node` as well as wherever else it's published: a
    /// multi-node raw block volume. Every node it's attached to exports it;
    /// one of them writes it and the others proxy to that one, and the
    /// writer hands it over (connections and all) when it's unpublished
    /// there (see `raidish::agent`).
    async fn publish_shared(
        &self,
        v: &crate::volume::VolumeInfo,
        node: &str,
    ) -> Result<(), Status> {
        let attached = self.attached(&v.name).await?;
        let mut props = v.props.clone();
        let running = props.enabled && props.automount.is_some();
        // Published to one node (before it was asked for on several): that
        // node stays first, its writer.
        if attached.is_empty()
            && running
            && let Some(first) = props.nodes.first()
            && first != node
        {
            self.attach(&v.name, first, true).await?;
        }
        if !attached.iter().any(|n| n == node) {
            println!("csi: attaching volume '{}' to {} too", v.name, node);
            self.attach(&v.name, node, true).await?;
        }
        if !running || props.automount != Some(Transport::Nvme) {
            if !running {
                props.nodes = vec![node.to_string()];
            }
            props.automount = Some(Transport::Nvme);
            props.enabled = true;
            props.move_to = None;
            println!("csi: publishing volume '{}' to {}", v.name, node);
            self.committer
                .set_props(&v.name, &props)
                .await
                .map_err(commit_status)?;
        }
        Ok(())
    }

    async fn volume(&self, name: &str) -> Result<Option<crate::volume::VolumeInfo>, Status> {
        let vols = self.committer.volumes().await.map_err(commit_status)?;
        Ok(vols.into_iter().find(|v| v.name == name))
    }

    /// Who runs `name` now (its live `ds/` lease's node), if anyone.
    async fn holder(&self, name: &str) -> Result<Option<String>, Status> {
        let status = crate::cluster::lease::LeaseClient::new(self.endpoints.clone())
            .status()
            .await
            .map_err(Status::unavailable)?;
        let now = crate::cluster::now_ms();
        Ok(status.state.dataset_node(name, now).map(str::to_string))
    }

    /// The kernel device this node's agent attached `name` on, or why
    /// there isn't one (yet).
    async fn local_device(&self, name: &str) -> Result<String, String> {
        let status: crate::api_types::AgentStatus = self
            .http
            .get(format!("{}/api/v1/agent/status", self.agent))
            .send()
            .await
            .map_err(|e| format!("asking the local agent: {}", e))?
            .json()
            .await
            .map_err(|e| format!("asking the local agent: {}", e))?;
        let Some(v) = status.volumes.into_iter().find(|v| v.name == name) else {
            return Err("not running on this node yet".into());
        };
        use crate::api_types::Attach;
        match v.attach {
            Attach::Attached { device } if Path::new(&device).exists() => Ok(device),
            Attach::Attached { device } => Err(format!("{} doesn't exist (yet)", device)),
            Attach::Failed { error } => Err(format!("attach failed: {}", error)),
            Attach::Exported => Err(
                "exported, but the agent attaches no devices (automount.helper is none?)".into(),
            ),
        }
    }

    async fn wait_device(&self, name: &str) -> Result<String, Status> {
        let start = Instant::now();
        let mut last = String::new();
        while start.elapsed() < ATTACH_WAIT {
            match self.local_device(name).await {
                Ok(d) => return Ok(d),
                Err(e) => last = e,
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(Status::deadline_exceeded(format!(
            "volume '{}' isn't attached on {}: {}",
            name, self.node, last
        )))
    }
}

#[tonic::async_trait]
impl Identity for Driver {
    async fn get_plugin_info(
        &self,
        _: Request<pb::GetPluginInfoRequest>,
    ) -> Rpc<pb::GetPluginInfoResponse> {
        Ok(Response::new(pb::GetPluginInfoResponse {
            name: DRIVER_NAME.into(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<pb::GetPluginCapabilitiesRequest>,
    ) -> Rpc<pb::GetPluginCapabilitiesResponse> {
        use pb::plugin_capability::{Service, Type, service};
        Ok(Response::new(pb::GetPluginCapabilitiesResponse {
            capabilities: vec![pb::PluginCapability {
                r#type: Some(Type::Service(Service {
                    r#type: service::Type::ControllerService as i32,
                })),
            }],
        }))
    }

    async fn probe(&self, _: Request<pb::ProbeRequest>) -> Rpc<pb::ProbeResponse> {
        let ready = self.committer.committer().await.is_some();
        Ok(Response::new(pb::ProbeResponse { ready: Some(ready) }))
    }
}

#[tonic::async_trait]
impl Controller for Driver {
    async fn create_volume(
        &self,
        req: Request<pb::CreateVolumeRequest>,
    ) -> Rpc<pb::CreateVolumeResponse> {
        let req = req.into_inner();
        let name = valid_name(&req.name)?;
        if req.volume_content_source.is_some() {
            return Err(Status::invalid_argument(
                "volume content sources aren't supported",
            ));
        }
        for cap in &req.volume_capabilities {
            supported(cap).map_err(Status::invalid_argument)?;
        }
        let (required, limit) = req
            .capacity_range
            .map(|r| (r.required_bytes.max(0) as u64, r.limit_bytes.max(0) as u64))
            .unwrap_or((0, 0));
        let mut size = if required == 0 {
            DEFAULT_SIZE
        } else {
            required
        };
        size = size.div_ceil(SIZE_UNIT) * SIZE_UNIT;
        if limit != 0 && size > limit {
            size = limit;
        }
        if size < required || size == 0 {
            return Err(Status::out_of_range(format!(
                "can't fit {} bytes in whole MiB under the limit {}",
                required, limit
            )));
        }
        match self.volume(&req.name).await? {
            // Retried request: same volume.
            Some(v) if v.size_bytes >= required && (limit == 0 || v.size_bytes <= limit) => {
                size = v.size_bytes;
            }
            Some(v) => {
                return Err(Status::already_exists(format!(
                    "volume '{}' exists with {} bytes",
                    req.name, v.size_bytes
                )));
            }
            None => {
                println!("csi: creating volume '{}' ({} bytes)", req.name, size);
                self.committer
                    .create_volume(&name, size)
                    .await
                    .map_err(commit_status)?;
            }
        }
        Ok(Response::new(pb::CreateVolumeResponse {
            volume: Some(pb::Volume {
                capacity_bytes: size as i64,
                volume_id: req.name,
                ..Default::default()
            }),
        }))
    }

    async fn delete_volume(
        &self,
        req: Request<pb::DeleteVolumeRequest>,
    ) -> Rpc<pb::DeleteVolumeResponse> {
        let id = req.into_inner().volume_id;
        valid_name(&id)?;
        if self.volume(&id).await?.is_some() {
            println!("csi: deleting volume '{}'", id);
            self.committer
                .delete_volume(&id)
                .await
                .map_err(commit_status)?;
        }
        Ok(Response::new(pb::DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        req: Request<pb::ControllerPublishVolumeRequest>,
    ) -> Rpc<pb::ControllerPublishVolumeResponse> {
        let req = req.into_inner();
        valid_name(&req.volume_id)?;
        if req.node_id.is_empty() {
            return Err(Status::invalid_argument("no node id"));
        }
        if let Some(cap) = &req.volume_capability {
            supported(cap).map_err(Status::invalid_argument)?;
        }
        let Some(v) = self.volume(&req.volume_id).await? else {
            return Err(Status::not_found(format!("no volume '{}'", req.volume_id)));
        };
        if req.volume_capability.as_ref().is_some_and(multi_node) {
            self.publish_shared(&v, &req.node_id).await?;
            return Ok(Response::new(Default::default()));
        }
        let attached = self.attached(&req.volume_id).await?;
        if !attached.is_empty() && attached != [req.node_id.clone()] {
            return Err(Status::failed_precondition(format!(
                "volume '{}' is published to {}",
                req.volume_id,
                attached.join(",")
            )));
        }
        let mut props = v.props.clone();
        let want = vec![req.node_id.clone()];
        if props.enabled && props.automount == Some(Transport::Nvme) && props.nodes == want {
            return Ok(Response::new(Default::default())); // already
        }
        if props.enabled
            && props.automount.is_some()
            && !props.nodes.is_empty()
            && props.nodes != want
        {
            return Err(Status::failed_precondition(format!(
                "volume '{}' is published to {}",
                req.volume_id,
                props.nodes.join(",")
            )));
        }
        props.nodes = want;
        props.automount = Some(Transport::Nvme);
        props.enabled = true;
        props.move_to = None;
        println!(
            "csi: publishing volume '{}' to {}",
            req.volume_id, req.node_id
        );
        self.committer
            .set_props(&req.volume_id, &props)
            .await
            .map_err(commit_status)?;
        Ok(Response::new(Default::default()))
    }

    async fn controller_unpublish_volume(
        &self,
        req: Request<pb::ControllerUnpublishVolumeRequest>,
    ) -> Rpc<pb::ControllerUnpublishVolumeResponse> {
        let req = req.into_inner();
        valid_name(&req.volume_id)?;
        let Some(v) = self.volume(&req.volume_id).await? else {
            return Ok(Response::new(Default::default())); // gone: nothing to undo
        };
        let attached = self.attached(&req.volume_id).await?;
        let shared = attached.contains(&req.node_id);
        // Unpublished from the last of several nodes, or from its one node.
        let last = match shared {
            true => attached.len() == 1,
            false => {
                attached.is_empty()
                    && (req.node_id.is_empty() || v.props.nodes == [req.node_id.clone()])
            }
        };
        // Stopped before it's detached from its last node: detached first,
        // it would fall back to its own placement and start somewhere.
        if last && v.props.enabled {
            let mut props = v.props.clone();
            props.enabled = false;
            println!(
                "csi: unpublishing volume '{}' from {}",
                req.volume_id, req.node_id
            );
            self.committer
                .set_props(&req.volume_id, &props)
                .await
                .map_err(commit_status)?;
        }
        // Still published elsewhere: if this node writes it, it hands it to
        // one of them.
        if shared {
            println!(
                "csi: detaching volume '{}' from {}",
                req.volume_id, req.node_id
            );
            self.attach(&req.volume_id, &req.node_id, false).await?;
        }
        // Done once nobody (or not that node) runs it any more.
        let start = Instant::now();
        loop {
            match self.holder(&req.volume_id).await? {
                None => break,
                Some(h) if !req.node_id.is_empty() && h != req.node_id => break,
                Some(h) if start.elapsed() > RELEASE_WAIT => {
                    return Err(Status::deadline_exceeded(format!(
                        "volume '{}' still runs on {}",
                        req.volume_id, h
                    )));
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        Ok(Response::new(Default::default()))
    }

    async fn validate_volume_capabilities(
        &self,
        req: Request<pb::ValidateVolumeCapabilitiesRequest>,
    ) -> Rpc<pb::ValidateVolumeCapabilitiesResponse> {
        let req = req.into_inner();
        valid_name(&req.volume_id)?;
        if self.volume(&req.volume_id).await?.is_none() {
            return Err(Status::not_found(format!("no volume '{}'", req.volume_id)));
        }
        if let Some(e) = req
            .volume_capabilities
            .iter()
            .find_map(|c| supported(c).err())
        {
            return Ok(Response::new(pb::ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message: e,
            }));
        }
        Ok(Response::new(pb::ValidateVolumeCapabilitiesResponse {
            confirmed: Some(pb::validate_volume_capabilities_response::Confirmed {
                volume_context: req.volume_context,
                volume_capabilities: req.volume_capabilities,
                parameters: req.parameters,
                ..Default::default()
            }),
            message: String::new(),
        }))
    }

    async fn controller_get_capabilities(
        &self,
        _: Request<pb::ControllerGetCapabilitiesRequest>,
    ) -> Rpc<pb::ControllerGetCapabilitiesResponse> {
        use pb::controller_service_capability::{Rpc as R, Type, rpc};
        let caps = [
            rpc::Type::CreateDeleteVolume,
            rpc::Type::PublishUnpublishVolume,
        ]
        .into_iter()
        .map(|t| pb::ControllerServiceCapability {
            r#type: Some(Type::Rpc(R { r#type: t as i32 })),
        })
        .collect();
        Ok(Response::new(pb::ControllerGetCapabilitiesResponse {
            capabilities: caps,
        }))
    }

    async fn list_volumes(
        &self,
        _: Request<pb::ListVolumesRequest>,
    ) -> Rpc<pb::ListVolumesResponse> {
        Err(Status::unimplemented("ListVolumes"))
    }
    async fn controller_list_volume_health(
        &self,
        _: Request<pb::ControllerListVolumeHealthRequest>,
    ) -> Rpc<pb::ControllerListVolumeHealthResponse> {
        Err(Status::unimplemented("ControllerListVolumeHealth"))
    }
    async fn controller_get_volume_health(
        &self,
        _: Request<pb::ControllerGetVolumeHealthRequest>,
    ) -> Rpc<pb::ControllerGetVolumeHealthResponse> {
        Err(Status::unimplemented("ControllerGetVolumeHealth"))
    }
    async fn get_capacity(
        &self,
        _: Request<pb::GetCapacityRequest>,
    ) -> Rpc<pb::GetCapacityResponse> {
        Err(Status::unimplemented("GetCapacity"))
    }
    async fn create_snapshot(
        &self,
        _: Request<pb::CreateSnapshotRequest>,
    ) -> Rpc<pb::CreateSnapshotResponse> {
        Err(Status::unimplemented("CreateSnapshot"))
    }
    async fn delete_snapshot(
        &self,
        _: Request<pb::DeleteSnapshotRequest>,
    ) -> Rpc<pb::DeleteSnapshotResponse> {
        Err(Status::unimplemented("DeleteSnapshot"))
    }
    async fn list_snapshots(
        &self,
        _: Request<pb::ListSnapshotsRequest>,
    ) -> Rpc<pb::ListSnapshotsResponse> {
        Err(Status::unimplemented("ListSnapshots"))
    }
    async fn get_snapshot(
        &self,
        _: Request<pb::GetSnapshotRequest>,
    ) -> Rpc<pb::GetSnapshotResponse> {
        Err(Status::unimplemented("GetSnapshot"))
    }
    async fn controller_expand_volume(
        &self,
        _: Request<pb::ControllerExpandVolumeRequest>,
    ) -> Rpc<pb::ControllerExpandVolumeResponse> {
        Err(Status::unimplemented("ControllerExpandVolume"))
    }
    async fn controller_get_volume(
        &self,
        _: Request<pb::ControllerGetVolumeRequest>,
    ) -> Rpc<pb::ControllerGetVolumeResponse> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }
    async fn controller_modify_volume(
        &self,
        _: Request<pb::ControllerModifyVolumeRequest>,
    ) -> Rpc<pb::ControllerModifyVolumeResponse> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
    async fn controller_get_node_info(
        &self,
        _: Request<pb::ControllerGetNodeInfoRequest>,
    ) -> Rpc<pb::ControllerGetNodeInfoResponse> {
        Err(Status::unimplemented("ControllerGetNodeInfo"))
    }
}

#[tonic::async_trait]
impl Node for Driver {
    async fn node_stage_volume(
        &self,
        req: Request<pb::NodeStageVolumeRequest>,
    ) -> Rpc<pb::NodeStageVolumeResponse> {
        let req = req.into_inner();
        valid_name(&req.volume_id)?;
        let cap = req
            .volume_capability
            .ok_or_else(|| Status::invalid_argument("no volume capability"))?;
        supported(&cap).map_err(Status::invalid_argument)?;
        let target = req.staging_target_path.as_str();
        if target.is_empty() {
            return Err(Status::invalid_argument("no staging path"));
        }
        let dev = self.wait_device(&req.volume_id).await?;
        // A raw block volume is used straight from the device (NodePublish).
        let Some(AccessType::Mount(m)) = &cap.access_type else {
            return Ok(Response::new(pb::NodeStageVolumeResponse {}));
        };
        if is_mounted(target).await {
            return Ok(Response::new(pb::NodeStageVolumeResponse {}));
        }
        let fs = fs_type(m);
        // blkid exits 2 when it finds no signature at all: a new volume.
        let mut blkid = tokio::process::Command::new("blkid");
        blkid.args(["-p", "-s", "TYPE", "-o", "value", &dev]);
        match crate::proc::ran(blkid).await {
            Ok(o) if o.code == Some(2) => {
                println!("csi: formatting '{}' ({}) as {}", req.volume_id, dev, fs);
                let mkfs = format!("mkfs.{}", fs);
                let args: &[&str] = match fs {
                    "ext4" => &["-q", "-F", "-E", "nodiscard", &dev],
                    _ => &["-q", "-K", &dev],
                };
                run(&mkfs, args).await.map_err(Status::internal)?;
            }
            Ok(o) if o.ok() => {
                let found = o.stdout.trim().to_string();
                if found != fs {
                    return Err(Status::failed_precondition(format!(
                        "volume '{}' holds {} (asked for {})",
                        req.volume_id,
                        if found.is_empty() {
                            "something else"
                        } else {
                            &found
                        },
                        fs
                    )));
                }
                // Repair what an unclean stop (or an I/O error that made the
                // kernel abort the journal) left before mounting, like a boot
                // does. e2fsck -p: 0 clean, 1 fixed, 2 fixed (reboot advised:
                // nothing has it mounted); worse needs a human.
                if fs == "ext4" {
                    let mut e2fsck = tokio::process::Command::new("e2fsck");
                    e2fsck.args(["-p", &dev]);
                    let out = crate::proc::ran(e2fsck).await.map_err(Status::internal)?;
                    match out.code {
                        Some(0) => {}
                        Some(1 | 2) => println!(
                            "csi: e2fsck repaired '{}': {}",
                            req.volume_id,
                            out.stdout.trim()
                        ),
                        _ => {
                            return Err(Status::internal(format!(
                                "e2fsck {} needs manual repair: {}{}",
                                dev,
                                out.stdout.trim(),
                                out.stderr.trim()
                            )));
                        }
                    }
                }
            }
            Ok(o) => {
                return Err(Status::internal(format!(
                    "blkid {}: {}",
                    dev,
                    o.stderr.trim()
                )));
            }
            Err(e) => return Err(Status::internal(e)),
        }
        std::fs::create_dir_all(target).map_err(|e| Status::internal(e.to_string()))?;
        let mut args = vec!["-t", fs];
        let opts = m.mount_flags.join(",");
        if !opts.is_empty() {
            args.extend(["-o", &opts]);
        }
        args.extend([dev.as_str(), target]);
        run("mount", &args).await.map_err(Status::internal)?;
        println!("csi: staged '{}' ({}) at {}", req.volume_id, dev, target);
        Ok(Response::new(pb::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<pb::NodeUnstageVolumeRequest>,
    ) -> Rpc<pb::NodeUnstageVolumeResponse> {
        let req = req.into_inner();
        unmount(&req.staging_target_path).await?;
        println!("csi: unstaged '{}'", req.volume_id);
        Ok(Response::new(pb::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        req: Request<pb::NodePublishVolumeRequest>,
    ) -> Rpc<pb::NodePublishVolumeResponse> {
        let req = req.into_inner();
        valid_name(&req.volume_id)?;
        let cap = req
            .volume_capability
            .ok_or_else(|| Status::invalid_argument("no volume capability"))?;
        supported(&cap).map_err(Status::invalid_argument)?;
        let target = req.target_path.as_str();
        if target.is_empty() {
            return Err(Status::invalid_argument("no target path"));
        }
        if is_mounted(target).await {
            return Ok(Response::new(pb::NodePublishVolumeResponse {}));
        }
        let source = match &cap.access_type {
            Some(AccessType::Block(_)) => {
                let dev = self.wait_device(&req.volume_id).await?;
                if let Some(p) = Path::new(target).parent() {
                    std::fs::create_dir_all(p).map_err(|e| Status::internal(e.to_string()))?;
                }
                std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(target)
                    .map_err(|e| Status::internal(e.to_string()))?;
                dev
            }
            _ => {
                if req.staging_target_path.is_empty() || !is_mounted(&req.staging_target_path).await
                {
                    return Err(Status::failed_precondition("volume isn't staged"));
                }
                std::fs::create_dir_all(target).map_err(|e| Status::internal(e.to_string()))?;
                req.staging_target_path.clone()
            }
        };
        let opts = if req.readonly { "bind,ro" } else { "bind" };
        run("mount", &["-o", opts, &source, target])
            .await
            .map_err(Status::internal)?;
        println!("csi: published '{}' at {}", req.volume_id, target);
        Ok(Response::new(pb::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<pb::NodeUnpublishVolumeRequest>,
    ) -> Rpc<pb::NodeUnpublishVolumeResponse> {
        let req = req.into_inner();
        let target = req.target_path.as_str();
        unmount(target).await?;
        let p = Path::new(target);
        let _ = if p.is_dir() {
            std::fs::remove_dir(p)
        } else {
            std::fs::remove_file(p)
        };
        println!("csi: unpublished '{}' from {}", req.volume_id, target);
        Ok(Response::new(pb::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        req: Request<pb::NodeGetVolumeStatsRequest>,
    ) -> Rpc<pb::NodeGetVolumeStatsResponse> {
        let path = req.into_inner().volume_path;
        if !is_mounted(&path).await {
            return Err(Status::not_found(format!("{} isn't mounted", path)));
        }
        use pb::volume_usage::Unit;
        // A raw block volume is published as the device itself: its size is
        // all there is to report (df would describe the /dev it came from).
        use std::os::unix::fs::FileTypeExt;
        if std::fs::metadata(&path).is_ok_and(|m| m.file_type().is_block_device()) {
            let out = run("blockdev", &["--getsize64", &path])
                .await
                .map_err(Status::internal)?;
            let total: i64 = out
                .trim()
                .parse()
                .map_err(|_| Status::internal(format!("blockdev: {}", out)))?;
            return Ok(Response::new(pb::NodeGetVolumeStatsResponse {
                usage: vec![pb::VolumeUsage {
                    total,
                    unit: Unit::Bytes as i32,
                    ..Default::default()
                }],
            }));
        }
        let out = run(
            "df",
            &["-B1", "--output=size,used,avail,itotal,iused,iavail", &path],
        )
        .await
        .map_err(Status::internal)?;
        let n: Vec<i64> = out
            .lines()
            .last()
            .unwrap_or("")
            .split_whitespace()
            .filter_map(|x| x.parse().ok())
            .collect();
        if n.len() != 6 {
            return Err(Status::internal(format!("df: {}", out)));
        }
        Ok(Response::new(pb::NodeGetVolumeStatsResponse {
            usage: vec![
                pb::VolumeUsage {
                    total: n[0],
                    used: n[1],
                    available: n[2],
                    unit: Unit::Bytes as i32,
                },
                pb::VolumeUsage {
                    total: n[3],
                    used: n[4],
                    available: n[5],
                    unit: Unit::Inodes as i32,
                },
            ],
        }))
    }

    async fn node_get_volume_health(
        &self,
        _: Request<pb::NodeGetVolumeHealthRequest>,
    ) -> Rpc<pb::NodeGetVolumeHealthResponse> {
        Err(Status::unimplemented("NodeGetVolumeHealth"))
    }
    async fn node_get_storage_health(
        &self,
        _: Request<pb::NodeGetStorageHealthRequest>,
    ) -> Rpc<pb::NodeGetStorageHealthResponse> {
        Err(Status::unimplemented("NodeGetStorageHealth"))
    }
    async fn node_expand_volume(
        &self,
        _: Request<pb::NodeExpandVolumeRequest>,
    ) -> Rpc<pb::NodeExpandVolumeResponse> {
        Err(Status::unimplemented("NodeExpandVolume"))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<pb::NodeGetCapabilitiesRequest>,
    ) -> Rpc<pb::NodeGetCapabilitiesResponse> {
        use pb::node_service_capability::{Rpc as R, Type, rpc};
        let caps = [rpc::Type::StageUnstageVolume, rpc::Type::GetVolumeStats]
            .into_iter()
            .map(|t| pb::NodeServiceCapability {
                r#type: Some(Type::Rpc(R { r#type: t as i32 })),
            })
            .collect();
        Ok(Response::new(pb::NodeGetCapabilitiesResponse {
            capabilities: caps,
        }))
    }

    async fn node_get_info(
        &self,
        _: Request<pb::NodeGetInfoRequest>,
    ) -> Rpc<pb::NodeGetInfoResponse> {
        Ok(Response::new(pb::NodeGetInfoResponse {
            node_id: self.node.clone(),
            ..Default::default()
        }))
    }
}

/// Serve the driver on the unix socket `endpoint` (`unix:///path` or a path).
pub async fn serve(
    endpoint: &str,
    node: String,
    agent: Option<String>,
    endpoints: Vec<String>,
) -> Result<(), String> {
    if endpoints.is_empty() {
        return Err("no cluster to ask: the pool has no remote disks".into());
    }
    let path = endpoint.strip_prefix("unix://").unwrap_or(endpoint);
    if let Some(dir) = Path::new(path).parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {}", dir.display(), e))?;
    }
    let _ = std::fs::remove_file(path);
    let uds = tokio::net::UnixListener::bind(path).map_err(|e| format!("{}: {}", path, e))?;
    let driver = Driver {
        node: node.clone(),
        committer: CommitClient::new(endpoints.clone()),
        endpoints,
        agent: agent
            .unwrap_or_else(|| format!("{}://127.0.0.1:8881", crate::tls::scheme()))
            .trim_end_matches('/')
            .to_string(),
        http: crate::tls::http_client(Some(Duration::from_secs(10)))?,
    };
    println!("csi: {} on {} as {}", DRIVER_NAME, path, node);
    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(driver.clone()))
        .add_service(ControllerServer::new(driver.clone()))
        .add_service(NodeServer::new(driver))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(uds))
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
#[path = "tests/csi_tests.rs"]
mod tests;
