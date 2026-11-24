# raidish-committer

The pool committer, health monitor, and activity log for [RAIDish](../../README.md).

In a multi-writer RAIDish pool, one node at a time holds the `pool` lease and acts as the **committer**—the sole writer of pool metadata (inodes, dataset directory, space maps, uberblocks, and labels). Every `listen` node runs the committer loop so the role fails over automatically if the active node stops, loses its lease, or is fenced out.

## Scope

**What's in this crate:**
- Committer service (`Committer`, `run`) that contends for the `pool` lease, lends/reclaims metaslabs, applies writer commits (`DatasetCommit`, `FsCommit`), and replays orphaned intent logs
- Unified pool administration interface (`PoolAdmin`) implemented locally (`LocalAdmin`), over HTTP (`CommitClient`), and transparently across both (`Admin`)
- Rocket HTTP routes (`/api/v1/pool/*`, `routes`) mounted by every `listen` node; non-committers return `CommitError::Retry` so clients fail over to the active committer
- Automatic disk health monitoring and resilvering (`health::spawn_health_monitor`) driven by disk reconnects and periodic checks
- Cluster activity log (`events::EventLog`, `EventSink`) persisted in the `.events` B+tree system dataset
- Single-object write leases (`object_lease`) bridging `raidish-core`'s `ObjLease` with `raidish-cluster`'s `HeldLease` for HTTP file uploads and NFS writers

## Architecture & Module Map

Callers talk to `PoolAdmin` (or `CommitClient` for writer storage ops) without needing to track which node currently holds the `pool` lease:

```text
Writers / API / CLI  ──►  Admin / CommitClient  ──►  /api/v1/pool/* (routes)
                                                            │
                                                     Committer<Pool> (holds `pool` lease)
                                                       ├── FileSystem (metadata, TXGs, lends)
                                                       ├── health (reconnect adopt + resilver)
                                                       └── EventLog (`.events` dataset)
```

| Module | Purpose |
|---|---|
| `lib` (`Committer`, `run`) | `pool` lease acquisition/failover loop, metaslab lending and reclamation, writer commit verification, orphan ZIL replay, and `CommitClient` re-export |
| `admin` | `PoolAdmin` trait plus `LocalAdmin`, `Admin` (local-or-HTTP), and audit logging for datasets, volumes, snapshots, VMs, node facts, and events |
| `routes` | Rocket endpoints under `/api/v1/pool/*` (`Here` guard returning `Retry` when this node isn't the committer) |
| `health` | `spawn_health_monitor`: watches disk reconnects, adopts replaced disks, and runs resilvers with backoff |
| `events` | `.events` B+tree activity log (up to 50,000 events / 30 days), cluster-state diffing (`observe`/`diff`), and batched node reporting (`EventSink`) |
| `object_lease` | `ObjectLeases` / `ObjectGuard` adapters acquiring and renewing `obj/<id>` leases for file writers |

## How It Works

### Failover & Fencing

`run()` loops continuously trying to acquire the `pool` lease. Once acquired, it:
1. Starts renewing the lease immediately, opens the pool with its fencing epoch, and performs a linearizable Raft read (`linearizable_state`) so its view of lent metaslabs is up to date.
2. Publishes itself into `CommitterSlot`, spawns `health::spawn_health_monitor`, and ticks every 2 s to reclaim expired lends/logs and record cluster state transitions in `.events`.
3. Steps down immediately (clearing `CommitterSlot` and aborting the monitor) if the lease is lost, a disk server rejects its pool epoch, or an uberblock write is left in doubt.

### Metaslab Lending & Writer Commits

Writers do not modify pool metadata directly. Instead they request metaslabs (`POST /api/v1/pool/lend`), write data and indirect blocks straight to disks under the lend's fencing epoch, and submit `DatasetCommit` or `FsCommit` payloads (`POST /api/v1/pool/commit`, `/commit_fs`). `Committer` re-validates the writer's lease, dataset/object lease, and metaslab ownership under the metadata transaction lock (`Tx`) before committing. Metaslabs belonging to a writer whose lease lapsed past `RECLAIM_GRACE_MS` (3 s) are fenced back via `LeaseOp::Return` and reclaimed.

### Health Monitor & Activity Log

- **Health monitor**: subscribes to disk reconnect notifications, adopts foreign/replaced disks (marking them degraded so they rebuild), and runs TXG-bounded resilvers with exponential backoff if a run is incomplete or errors.
- **Activity log**: stored in `.events` and written exclusively by the committer so events have a single sequence order. The committer persists its last observed cluster snapshot alongside the events, so a new committer after failover still reports what changed while no committer was active.

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-committer

# Run the raidish-committer unit test suite
cargo test -p raidish-committer
```
