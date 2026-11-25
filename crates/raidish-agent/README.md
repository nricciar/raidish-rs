# raidish-agent

The per-node placement, workload, and migration controller for [RAIDish](../../README.md).

Every `listen` node runs an `Agent` that holds the node's liveness (`node/<name>`) and writer (`writer/<name>`) leases, reconciles which volumes, service datasets, and VMs should run locally, manages block exports and kernel attachments, and coordinates zero-downtime volume handoffs and live VM migrations.

## Scope

**What's in this crate:**
- Node and writer supervision (`supervisor`): maintains `node/<name>` and `writer/<name>` leases (via [`raidish-cluster`](../raidish-cluster/)), opens the pool in writer mode ([`raidish-core`](../raidish-core/)), hosts the NBD and NVMe/TCP servers ([`raidish-block`](../raidish-block/)), and heals degraded lent metaslabs
- Sticky placement rules (`placement`): places volumes, service datasets, and VMs on the first live preferred node without automatic fail-back
- Volume reconciliation & zero-downtime handoff (`volumes`): acquires `ds/<name>` leases, manages local exports and mTLS peer-I/O proxies, runs slow drains/handoffs in the background, and tears down exports immediately on lease loss
- Kernel device attachment (`attach`): invokes the `raidish-attach` root helper with timeouts and retries for NBD and NVMe/TCP kernel initiators
- Service dataset controller (`services`): runs S3, SQS, and `etcd` datasets ([`raidish-services`](../raidish-services/)) in a dedicated task tied to the node's writer generation
- VM reconciliation & live migration (`vms`): starts/stops transient libvirt domains ([`raidish-vm`](../raidish-vm/)), cleans up orphans, destroys VMs immediately if their disk leases are lost, and orchestrates `qemu+tls://` live migrations with cold fallback
- Host capability probing & migration checks (`node_facts`, `vm_check`): collects local host/CPU/QEMU/network facts and probes peer `:16514` libvirt TLS, reporting live vs. cold migration verdicts

## Architecture & Module Map

```text
Agent::run (2 s tick + wake/poke)
  ├── supervisor  ──► node/<name> & writer/<name> leases, FileSystem writer, NBD/NVMe servers
  ├── placement   ──► computes desired volumes, proxies, and VMs for this node
  ├── volumes     ──► local exports, PeerIo proxies, background drains & volume handoff
  ├── attach      ──► raidish-attach helper (kernel NBD / NVMe devices)
  ├── vms         ──► transient libvirt domains, orphan cleanup, live/cold migration
  └── services    ──► independent ServiceController task (S3, SQS, etcd frontends)
```

| Module | Purpose |
|---|---|
| `lib` | `Agent`, `AutomountConfig`, `WriterSlot`, main 2 s reconciliation loop (`Agent::run`), and immediate lease-loss teardown (`drop_lost`) |
| `supervisor` | `NodeSupervisor` / `NodeWriter`: node & writer leases, writer-mode pool, NBD/NVMe servers, and lent-space resilvering |
| `placement` | `place`, `place_on`, `place_attached`, and per-tick `Placement` calculation |
| `volumes` | Volume table (`table`), reconciliation (`mod`, `plan`), background drains (`background`), and mTLS volume handoff (`handoff`) |
| `attach` | `Attacher` trait and `raidish-attach` subprocess wrapper with 60 s timeout and retry |
| `services` | `ServiceController`: independent placement/lifecycle loop for S3, SQS, and `etcd` datasets |
| `vms` | VM reconciliation (`mod`, `plan`, `state`) and live/cold migration orchestration (`migrate`) |
| `node_facts` | `Collector`: probes host, CPU, QEMU/libvirt, networks, and peer `:16514` libvirt TLS; stores facts in `.vms` via [`raidish-committer`](../raidish-committer/) |
| `vm_check` | Evaluates VM definitions against source/target `NodeFacts` and live CPU/memory checks to produce a `Verdict` (`Live`, `Cold`, or refused) |

## How It Works

### Placement & Lease Safety

Each agent renews a `node/<name>` lease for liveness. A volume or service runs on the first live node in its `nodes` list (or any live node if empty) and stays there while that node remains live; moving it requires an explicit `volume move` or `vm move`. Actual ownership is enforced by the dataset's `ds/<name>` lease.

If a volume's lease or the node's `writer/<name>` lease lapses (for example, during a network partition), `drop_lost()` immediately destroys any VM backed by those volumes before winding down exports and proxies, preventing a fenced guest from continuing to run with I/O errors while another node starts it.

### Volume Handoff & Live VM Migration

When TLS is configured, the agent starts a peer I/O listener (`peer_bind:peer_port`, default `0.0.0.0:10810`, `node` certificates only):
- **Shared attachments & handoff**: Non-writer nodes export attached volumes backed by `PeerIo` proxied to the active writer; handing a volume over commits dirty state, transfers the `ds/<name>` lease, and swaps backends under live connections.
- **VM placement & migration**: A VM follows its **anchor** volume (its first RAIDish disk). Before a move, `vm_check` verifies host compatibility using `NodeFacts` (including libvirt TLS on `<host>:16514`); if live migration is supported, the target attaches the VM's disks via peer I/O, runs `virsh migrate --live --p2p --tls`, and takes over the volumes with no I/O lost.

### Configuration & Inspection

Configure local block export and kernel attachment behavior in a `listen` node's config (all fields optional):

```yaml
automount:
  bind: 127.0.0.1                       # default (exports + local initiator)
  nbd_port: 10809                       # default
  nvme_port: 4420                       # default
  helper: /usr/local/sbin/raidish-attach # set `none` to export without kernel attach
  peer_bind: 0.0.0.0                    # default (mTLS peer I/O & handoff)
  peer_port: 10810                      # default
```

Useful inspection commands and endpoints:

```bash
raidish node facts              # collected host, QEMU, network, and libvirt TLS facts
raidish vm check <vm> [node]    # live / cold / refused verdict per target node
# Per-node running volumes and attached devices: GET /api/v1/agent/status
```

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-agent

# Run the raidish-agent unit test suite
cargo test -p raidish-agent
```
