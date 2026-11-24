# raidish-types

The shared plain-data crate for [RAIDish](../../README.md).

`raidish-types` defines the core identifiers, validated names, HTTP API DTOs, VM configuration models, and deterministic cluster lease state shared across the workspace. It has no I/O or server dependencies and compiles both natively and to `wasm32-unknown-unknown`.

## Scope

**What's in this crate:**
- Strongly typed identifiers and units (`Epoch`, `Txg`, `FileId`, `NodeId`, `MetaslabIdx`, `Bytes`, `Extent`) to avoid mixing bare integers
- Validated resource names (`DatasetName`, `SnapshotName`, `SnapshotRef`, `VmName`, `BucketName`, `QueueName`) that enforce rules on parse and deserialization
- Shared storage and fencing primitives (`DatasetFormat`, `DatasetProps`, `ObjType`, `Transport`, `Domain`, `Grant`, `LeaseResource`)
- Wire DTOs for the HTTP API: events, volumes, files, auth (`Role`, `Principal`), agent status, node facts, and VM views
- Deterministic replicated cluster state (`ClusterState`, `LeaseOp`, `Lease`, `MetaslabGrant`) applied by Raft replicas
- Portable VM hardware descriptions, domain states, and starter XML generation (`vm_def`)

## Module Map

| Module | Purpose |
|---|---|
| `types` | Core newtypes (`Epoch`, `Txg`, `FileId`, `NodeId`, `Bytes`), validated names, dataset/volume metadata, and fencing domains/grants |
| `api_types` | Request and response DTOs for the HTTP API (events, files, volumes, VMs, auth principals/roles, agent status, host/migration facts) |
| `cluster_state` | Deterministic Raft state machine data and transitions: resource leases, monotonically increasing fencing epochs, metaslab lends, and cluster status views |
| `vm_def` | Portable VM hardware specs (`VmHardware`, `VmDisk`, `VmNic`), `DomState`, and new-VM domain XML templates (no XML parser dependency) |

## Quick Example

Parsing validated identifiers and stepping the deterministic cluster lease state:

```rust
use raidish_types::cluster_state::{ClusterState, LeaseOp, Request, Response};
use raidish_types::types::{DatasetName, Holder, HolderRole, LeaseResource, SnapshotRef};

// Validated names check syntax on construction and serde deserialization
let ds = DatasetName::user("vm-0")?;
let snap: SnapshotRef = "vm-0@2026-11-18T10:00".parse()?;
assert_eq!(snap.volume, ds);

// Acquire a lease against the replicated cluster state machine
let mut state = ClusterState::default();
let resp = state.apply(&Request {
    now_ms: 1_000,
    op: LeaseOp::Acquire {
        resource: LeaseResource::dataset("vm-0"),
        owner: "node-0".into(),
        holder: Holder::new(HolderRole::Agent, Some("node-0")),
        ttl_ms: 10_000,
    },
});
assert!(matches!(resp, Response::Granted { .. }));
```

## Building & Testing

The workspace uses the Rust toolchain pinned in [`rust-toolchain.toml`](../../rust-toolchain.toml) (nightly, with `wasm32-unknown-unknown`). From the repository root:

```bash
# Check native build
cargo check -p raidish-types

# Run the raidish-types unit test suite
cargo test -p raidish-types

# Verify the wasm32 build used by the web dashboard
cargo check -p raidish-types --target wasm32-unknown-unknown
```
