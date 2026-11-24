# raidish-block

Block device exports and cross-node volume I/O for [RAIDish](../../README.md).

`raidish-block` exposes [RAIDish](../../README.md) copy-on-write volumes (from [`raidish-core`](../raidish-core/)) over **NBD** and **NVMe/TCP**, and provides the mTLS **peer I/O** proxy (using [`raidish-auth`](../raidish-auth/)) used when a volume is attached on a non-writer node (such as during live VM migration and volume handoff).

## Scope

**What's in this crate:**
- Volume I/O abstraction (`VolumeIo`), local filesystem backend (`LocalIo`), and live-swappable backend wrapper (`SwitchableIo`)
- Shared runtime export lifecycle (`BlockExporter`, `ExportTable`, `ExportLife`) for adding/removing exports and draining active connections cleanly
- Multi-export **NBD** server (`nbd`, default port `10809`) with fixed-newstyle negotiation, concurrent requests, `FLUSH`, `TRIM`, and `WRITE_ZEROES`
- Multi-subsystem **NVMe/TCP** target (`nvme`, default port `4420`) with 4 KiB LBAs, Fabrics Connect, admin + I/O queues, `FLUSH`, `WRITE_ZEROES`, and Dataset Management (`DSM`)
- Mutual-TLS **peer I/O** proxy and server (`peer_io`, default port `10810`, node certificates only) for forwarding block I/O to the volume's active writer node

## Architecture & Module Map

Client block protocols talk only to `SwitchableIo`, allowing a volume's backend to move between proxied (`PeerIo`) and local (`LocalIo`) without dropping live client connections:

```text
NBD (10809) / NVMe-TCP (4420)  ──►  ExportTable / SwitchableIo
                                          ├──► LocalIo  ──► FileSystem (raidish-core)
                                          └──► PeerIo   ──► mTLS (10810) ──► writer node
```

| Module | Purpose |
|---|---|
| `volume_io` | `VolumeIo` trait, `LocalIo<D>` filesystem adapter, `SwitchableIo` pause/swap wrapper, and shared periodic sync loop |
| `export` | `BlockExporter` trait, `ExportTable<E>`, and `ExportLife` graceful connection draining on export removal |
| `nbd` | Fixed-newstyle NBD server (`NBD_PORT = 10809`), concurrent handle-matched request loop, and `NbdServer` |
| `nvme` | NVMe/TCP target (`NVME_PORT = 4420`, NQN `nqn.2024-01.com.raidish:nvme:<volume>`), queue pairs, and `NvmeTcpServer` |
| `peer_io` | mTLS node-to-node I/O proxy (`PeerIo`) and listener (`DEFAULT_PORT = 10810`), epoch-checked preamble, and handoff framing |

## How It Works

### Dynamic Exports & Live Handoff

Both `NbdServer` and `NvmeTcpServer` implement `BlockExporter`: volumes are added and removed while the server runs. Removing an export stops new connections, drains in-flight requests (up to 30 s), and syncs before returning.

Because exports hold their backend inside a `SwitchableIo`, volume handoff can pause I/O, swap `PeerIo` for `LocalIo` (or vice versa), and resume—clients experience only a brief pause rather than a disconnect.

### NBD & NVMe/TCP Behavior

- **Loopback by default**: Both listeners bind `127.0.0.1` unless configured otherwise (neither protocol carries RAIDish authentication of its own).
- **Durability**: Writes acknowledge once buffered in the volume's dirty records (volatile write cache). `FLUSH` persists through the volume's intent log (`zil`) or a TXG commit, and a background sync loop periodically commits dirty volumes.
- **Space reclamation**: `TRIM` / `WRITE_ZEROES` (NBD) and `DSM` / `WRITE_ZEROES` (NVMe) deallocate full records back to the pool.
- **Debugging**: Set `RAIDISH_NVME_TRACE=1` to log incoming NVMe/TCP commands.

### Peer I/O

When a volume is exported on a node that does not hold its `ds/<name>` lease, `PeerIo` connects over mTLS (requiring a `node` certificate) to the active writer on port `10810`, verifies the writer's lease epoch in the preamble, and multiplexes concurrent block requests over the NBD transmission framing. If the writer fails over without a clean handoff while unflushed writes were acknowledged, `PeerIo` fails the next `FLUSH` rather than falsely acknowledging durability.

## CLI Usage

### Cluster Placement & Auto-Mount

In a cluster, each `listen` node's agent automatically exports and attaches volumes placed on it (configured via `automount:` in the node config):

```bash
# Create a 20 GiB volume and auto-mount it over NVMe/TCP (or nbd) on the first live node
raidish volume create vm0 21474836480
raidish volume set vm0 --nodes node-0,node-1 --automount nvme
raidish volume list

# Explicitly move a running volume to another node
raidish volume move vm0 node-1
```

Node config overrides (all optional; set `helper: none` to export without attaching a kernel device):

```yaml
automount:
  bind: 127.0.0.1
  nbd_port: 10809
  nvme_port: 4420
  helper: /usr/local/sbin/raidish-attach
```

### Standalone Exports & Client Attach

To run a single export manually without the cluster agent:

```bash
# Create a 1 GiB volume and export it over NBD (10809) or NVMe/TCP (4420)
raidish --config remote-config.yaml block vdisk0 1073741824
raidish --config remote-config.yaml nbd vdisk0
# or:
raidish --config remote-config.yaml nvme vdisk0
```

Connecting from a Linux host (default ports):

```bash
# NBD
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N vdisk0

# NVMe/TCP
sudo modprobe nvme-tcp
sudo nvme connect -t tcp -a 127.0.0.1 -s 4420 -n nqn.2024-01.com.raidish:nvme:vdisk0
sudo nvme disconnect -n nqn.2024-01.com.raidish:nvme:vdisk0
```

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-block

# Run the raidish-block unit test suite
cargo test -p raidish-block
```
