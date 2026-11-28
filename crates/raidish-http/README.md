# raidish-http

The node server (`raidish listen`) and HTTP(S) API for [RAIDish](../../README.md).

`raidish-http` is the top-level server assembly crate on `axum`: it parses a node's YAML configuration, starts and wires together every subsystem (fenced disk server, Raft, cluster CA, committer, agent, NFS, and S3/SQS/`etcd`), mounts all `/api/v1/*` and WebSocket routes, and embeds the `web/` dashboard into release builds.

## Scope

**What's in this crate:**
- Node configuration (`config`): YAML config parsing for pool topologies, TLS/auth/CA, Raft, NFS, S3/SQS/`etcd` services, automount, and VMs
- Node startup & wiring (`server::listen`): assembles [`raidish-core`](../raidish-core/), [`raidish-auth`](../raidish-auth/), [`raidish-cluster`](../raidish-cluster/), [`raidish-ca`](../raidish-ca/), [`raidish-committer`](../raidish-committer/), [`raidish-agent`](../raidish-agent/), [`raidish-nfs`](../raidish-nfs/), and [`raidish-services`](../raidish-services/) into one running node
- Fenced disk server (`fenced_disks`, `api`): serves local disks over HTTP and `/ws` behind block-range validation, startup readiness barrier, and stripe-locked epoch fencing
- Node HTTP(S) APIs (`files_api`, `volumes_api`, `vms_api`, `api`): streaming and random-access file API, volume/snapshot management, VM definitions/lifecycle/VNC console proxy, agent status, and activity log
- Dashboard hosting & UI bundling (`dashboard`, `build.rs`): serves the Yew dashboard (`web/`), embeds release assets via `trunk`, and exposes `GET /api/v1/pool`
- End-to-end integration test suites (`src/tests/`): cluster, multi-node, fault injection, ZIL, remote disk/scan, and API tests

## Architecture & Module Map

```text
raidish listen (server::listen)
  ├── config (YAML -> PoolSpec, TLS, Raft, CA, NFS, services, automount, VM)
  ├── background services (Raft, CaService, committer, agent, NFS, S3/SQS/etcd)
  └── axum HTTP(S) + WS router (api::router)
        ├── local routes: /ws, /api/v1/{disks,files,dir,stat,volumes,vms,agent,events,pool}, /
        └── mounted crate routers: auth, cluster/raft, ca/pki, committer, services, nfs/peers
```

| Module | Purpose |
|---|---|
| `server` | `listen()`: validates config, initializes TLS/trust/CA/Raft/committer/agent/NFS/services, and runs the `axum` server until shutdown |
| `config` | `Config`, `PoolSpec`, and YAML deserialization for pool layouts and every subsystem's config block |
| `api`, `api_error` | `api::router()` assembling all routes, `/ws` block protocol endpoint, disk HTTP endpoints, `/api/v1/agent/status`, `/api/v1/events`, and `ErrorKind` → HTTP status mapping |
| `fenced_disks` | `FencedDisks`: single gate for local disk I/O enforcing range shape, startup fence readiness, and stripe-locked epoch ordering |
| `files_api` | Streaming uploads/downloads, range `GET`, offset `PATCH` (under `obj/<id>` lease), `DELETE`, `rename`, `stat`, `attrs`, and directory listing/creation |
| `volumes_api` | Volume, property, snapshot, clone, and rollback HTTP endpoints (routed to the active committer via `Admin`) |
| `vms_api` | VM XML CRUD, live state queries, start/stop/restart/reboot/reset/kill, live/cold move, migration checks, node facts, and WebSocket VNC console proxy |
| `dashboard` | Serves embedded release UI assets (or `web/dist` on disk in debug builds) and `GET /api/v1/pool` |

## HTTP API & Dashboard

Every `listen` node exposes the same API surface so clients can talk to any node:

| Route prefix | Purpose |
|---|---|
| `/ws`, `/api/v1/disks/*` | Binary WebSocket block protocol and disk listing/flush |
| `/api/v1/{files,dir,stat,rename,attrs}/*` | Filesystem dataset HTTP API |
| `/api/v1/volumes/*` | Volume and snapshot management |
| `/api/v1/vms/*` | VM definitions, lifecycle, live migration, and VNC console |
| `/api/v1/agent/status`, `/api/v1/events` | Per-node running volumes/devices and cluster activity log |
| `/`, `/api/v1/pool` | Web dashboard SPA and pool topology map |
| `/api/v1/{raft,cluster,pki,pool,services,nfs}/*` | Mounted routes from `raidish-cluster`, `raidish-ca`, `raidish-committer`, `raidish-services`, and `raidish-nfs` |

### Dashboard Build (`build.rs`)

Release builds automatically run `trunk build --release` in `web/` (requiring `trunk` and the `wasm32-unknown-unknown` target) and embed the compiled assets into the binary. Debug builds—and release builds with `RAIDISH_SKIP_UI=1`—skip bundling and serve `web/dist` from disk if present.

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-http

# Run the integration and API test suites (filter by suite name as needed)
cargo test -p raidish-http
cargo test -p raidish-http cluster_tests

# Release build without bundling the web dashboard
RAIDISH_SKIP_UI=1 cargo build --release -p raidish-http
```
