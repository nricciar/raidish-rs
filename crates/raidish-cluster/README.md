# raidish-cluster

The embedded Raft control plane and lease client for [RAIDish](../../README.md).

Every `listen` node runs a member of an `openraft` group over the same HTTP(S) port that serves its disks—there is no separate coordination service to deploy. The group replicates `ClusterState` (from [`raidish-types`](../raidish-types/)) to issue epoch-stamped leases for the committer, volume/file writers, node liveness, metaslab lends, and NFSv4 state.

## Scope

**What's in this crate:**
- Embedded `openraft` node lifecycle (`Cluster::start`), leader forwarding for writes, and linearizable read-index queries (`linearizable_state`)
- Deterministic membership derived directly from the pool's disk list (`members::derive`), with layout fingerprinting to reject misconfigured peers
- Safe bootstrap and wiped-node recovery: fresh nodes only form a new group when all peers are fresh, and a node that lost its `data_dir` catches up non-voting before participating in elections
- Local JSON-backed Raft log, vote, state machine, and snapshot store (`Store`) kept outside the storage pool, notifying a `FenceObserver` on every applied grant
- HTTP Raft RPC transport (`Network`) and Rocket routes (`routes`) with protocol-version and layout-fingerprint guards
- Lease client (`LeaseClient`, `HttpLeases`) and automatic background renewal handle (`HeldLease`) with hedged HTTP calls and failover tolerance

## Module Map

| Module | Purpose |
|---|---|
| `lib` (`Cluster`) | `openraft` setup, bootstrap/catch-up logic, config-mismatch checks, leader forwarding, and `linearizable_state` |
| `members` | Derives `Members` (node IDs, peer base URLs, disk ownership, layout fingerprint) from the pool's disk list |
| `store` | File-backed `openraft` log/state-machine/snapshot persistence (atomic temp-file + fsync + rename) and `FenceObserver` updates |
| `network` | `openraft` `RaftNetwork` implementation sending JSON RPCs over `reqwest` with layout/protocol headers |
| `routes` | Rocket endpoints for Raft RPCs and lease/status/read-index requests, plus `SameLayout` guard and `stop_raft_on_shutdown` fairing |
| `lease` | `LeaseAuthority`, `LeaseClient` (in-process or hedged `HttpLeases`), and `HeldLease` background renewal loop |
| `state` | Re-export of [`raidish_types::cluster_state`](../raidish-types/src/cluster_state.rs) |

## How It Works

### Membership from the Pool Config

Cluster membership needs no separate configuration:
- Disks are grouped by `scheme://host:port`; local paths belong to the current node.
- Each node's ID is the lowest pool position of its disks (`node-0`, `node-1`, ...), so every node computes the same IDs independently.
- A pool with only local disks runs as a single-node cluster.
- Every node must list the pool's disks in the same order. Raft RPCs carry layout fingerprint, target ID, and Raft protocol headers (`X-Raidish-Layout`, `X-Raidish-To`, `X-Raidish-Raft`), and a node whose config disagrees with peers refuses Raft traffic instead of disrupting elections.

### Leases & Fencing

`HeldLease` acquires a lease (`LEASE_TTL` = 10 s) and renews it every `ttl / 3`, retrying through brief leader elections. If renewal fails for most of a TTL, the holder steps down. Every lease grant and metaslab lend receives a monotonically increasing `Epoch`; whenever `Store` applies a state update, it feeds the new epochs to the local `FenceObserver` so disk servers immediately reject writes carrying stale epochs.

### Persistent State (`data_dir`)

Raft state (`version`, `vote.json`, `committed.json`, `log.json`, `state.json`, `snapshot.json`) is stored outside the pool (default `~/.raidish/<name>`, configurable via `cluster.data_dir`) so the control plane stays available even when the pool is degraded or unformatted.

If a node starts with an empty `data_dir` while peers already have committed state, it writes a `catching-up` marker, refuses votes, and waits until it has replicated up to the leader's read index before voting again.

### HTTP Routes

Mounted by `routes::routes()` on each `listen` node's HTTP(S) server:

| Endpoint | Role | Purpose |
|---|---|---|
| `POST /api/v1/raft/append` | `node` | `AppendEntries` RPC (guarded by `SameLayout`) |
| `POST /api/v1/raft/vote` | `node` | `RequestVote` RPC (guarded by `SameLayout` and catch-up vote floor) |
| `POST /api/v1/raft/snapshot` | `node` | `InstallSnapshot` RPC (guarded by `SameLayout`) |
| `GET /api/v1/cluster/read_index` | `node` | Leader's confirmed commit index for linearizable reads |
| `POST /api/v1/cluster/lease` | `admin` / `node` | Propose a `LeaseOp` (followers forward to the leader) |
| `GET /api/v1/cluster/status` | `readonly`+ | Node ID, leader, term, membership, fingerprint, and applied state |

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-cluster

# Run the raidish-cluster unit test suite
cargo test -p raidish-cluster
```
