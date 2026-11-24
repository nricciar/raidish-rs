# raidish-csi

The Kubernetes Container Storage Interface (`csi.raidish.io`) driver for [RAIDish](../../README.md).

`raidish-csi` serves the CSI Identity, Controller, and Node gRPC services on a Unix domain socket, allowing Kubernetes pods and virtual machines to consume RAIDish volumes as PersistentVolumes. It runs on each node alongside `raidish listen` (as root, to format and mount block devices) and delegates placement, NVMe/TCP export ([`raidish-block`](../raidish-block/)), and kernel attachment to the existing RAIDish committer ([`raidish-committer`](../raidish-committer/)) and node agent.

## Scope

**What's in this crate:**
- Generated `tonic` gRPC server bindings for `proto/csi/csi.proto` (compiled by `build.rs` using `protoc-bin-vendored`, with no system `protoc` required)
- **Identity service**: `GetPluginInfo`, `GetPluginCapabilities`, and `Probe`
- **Controller service**: `CreateVolume`, `DeleteVolume`, `ControllerPublishVolume`, `ControllerUnpublishVolume`, and `ValidateVolumeCapabilities`
- **Node service**: `NodeStageVolume`, `NodeUnstageVolume`, `NodePublishVolume`, `NodeUnpublishVolume`, `NodeGetVolumeStats`, and `NodeGetInfo`
- Support for `ext4` and `xfs` filesystem volumes (single-node access modes) and raw `volumeMode: Block` volumes (including `ReadWriteMany` for live VM migration)
- Driver entry point (`serve`) invoked by `raidish csi`

## How It Works

`raidish-csi` adds no separate storage control plane—each CSI RPC drives the committer and local node agent directly:

```text
kubelet / CSI sidecars ──(gRPC over UDS)──► raidish-csi
                                              ├──► CommitClient (create/delete/place volumes)
                                              ├──► LeaseClient  (RWX shared attachment list)
                                              └──► Local agent  (discover attached NVMe device)
```

| CSI RPC | What it does |
|---|---|
| `CreateVolume` / `DeleteVolume` | Creates or deletes a RAIDish volume through the committer (volume ID = volume name; sized in whole MiB, 1 GiB default). New volumes start disabled. |
| `ControllerPublishVolume` | For single-node volumes, sets `nodes: [node]`, `automount: nvme`, and enables the volume so that node's agent claims the `ds/<name>` lease and attaches it. For `ReadWriteMany` raw block volumes, adds the node to `ClusterState::attached` instead. |
| `ControllerUnpublishVolume` | Disables the volume (or removes the node from `ClusterState::attached`) and waits for the node to release its lease or detach. |
| `NodeStageVolume` / `NodeUnstageVolume` | Polls the local agent's status endpoint until the NVMe/TCP block device appears, formats it with `ext4` or `xfs` if blank, and mounts/unmounts the staging path. |
| `NodePublishVolume` / `NodeUnpublishVolume` | Bind-mounts the staged directory into the pod, or—for `volumeMode: Block`—bind-mounts the raw device directly onto kubelet's target file (skipping staging). |

### Access Modes

- **Filesystem volumes (`ext4`, `xfs`)**: Single-node only (`ReadWriteOnce`, `ReadWriteOncePod`). Because `nodes` is pinned to the single published node, the volume will not fail over away from its pod.
- **Raw block volumes (`volumeMode: Block`)**: Single-node or `ReadWriteMany` (`MULTI_NODE_MULTI_WRITER`, e.g., for KubeVirt live migration). With TLS / peer I/O enabled, every attached node exports and attaches the device—one node writes it while the others proxy over peer I/O, handing off cleanly when the writer node detaches.

## Running the Driver

On each Kubernetes node (alongside `raidish listen`, with standard CSI sidecars `external-provisioner`, `external-attacher`, and `node-driver-registrar`):

```bash
sudo raidish --config /etc/raidish/client.yaml csi \
  --node node-0 \
  --endpoint /var/lib/kubelet/plugins/csi.raidish.io/csi.sock
```

Minimal `StorageClass`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: raidish
provisioner: csi.raidish.io
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
```

## Building & Testing

From the repository root (`build.rs` uses `protoc-bin-vendored`, so no external `protoc` installation is needed):

```bash
# Check build
cargo check -p raidish-csi

# Run the raidish-csi unit test suite
cargo test -p raidish-csi
```
