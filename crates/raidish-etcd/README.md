# raidish-etcd

An etcd v3 gRPC server backed by a [RAIDish](../../README.md) B+tree dataset.

`raidish-etcd` implements etcd's multi-version key-value store and gRPC API (reporting as etcd `3.5.17`) on top of an `etcd`-format dataset in [`raidish-core`](../raidish-core/), allowing `etcdctl`, standard client libraries, and etcd consumers such as Kubernetes to run against a RAIDish pool unchanged.

## Scope

**What's in this crate:**
- Generated `tonic` gRPC bindings for etcd v3.5 protos (`proto/etcd/`, compiled by `build.rs` with `protoc-bin-vendored`, requiring no system `protoc`)
- MVCC keyspace (`store::EtcdStore`) supporting global revisions, historical range reads, atomic `Txn` compare/then/else (up to 128 ops), batched compaction, and TTL leases with key expiration
- `tonic` gRPC router (`server::EtcdServer::router`) implementing **KV**, **Watch** (history replay, live streams, progress notifications), **Lease**, and status/member endpoints for **Maintenance**, **Cluster**, and **Auth**
- Connection-level role enforcement (`EtcdConn` / `Caller`) allowing read-only certificates to query/watch while requiring write roles for mutations

## Module Map

| Module | Purpose |
|---|---|
| `store` | `EtcdStore<D>`: MVCC B+tree storage engine, revisioning, transactions, compaction, lease lifecycle/expiration, and watch event log |
| `server` | `EtcdServer<D>`: `tonic` gRPC implementations (`KV`, `Watch`, `Lease`, `Maintenance`, `Cluster`, `Auth`), watch batching, and `EtcdConn` caller role propagation |
| `pb` | Generated `mvccpb`, `authpb`, and `etcdserverpb` protobuf types and gRPC server/client stubs |

## How It Works

### B+Tree Keyspace Layout

All state for an etcd instance lives inside a single RAIDish B+tree dataset under disjoint prefixes (keys in `H` are byte-escaped so a key's revision history stays contiguous and prefix-free):

```text
M rev / M compact                  current revision and compaction floor
K <key>                            current live key metadata & value
H <esc key> 0x00 0x01 <main> <sub> historical revision (put or tombstone)
R <main> <sub>                     revision event log replayed to watchers
L <lease id>                       lease record (TTL)
A <lease id> <key>                 key attached to a lease
```

### Transactions, Durability & Watches

- Every mutating request (`Put`, `DeleteRange`, `Txn`, `LeaseRevoke`) executes as a single atomic `btree::Txn`, logged as one intent-log entry, and is acknowledged only once durable (`FileSystem::flush`).
- Reads and watchers wait for any visible write to become durable before returning or streaming events, ensuring nothing a crash could roll back is ever observed.
- Limits follow etcd defaults: `MAX_REQUEST_BYTES = 1.5 MiB`, `MAX_TXN_OPS = 128`, `MIN_LEASE_TTL = 2 s`.

### Placement & Failover

One node serves an `etcd` dataset at a time under its `ds/<name>` lease (peer nodes forward client connections to it via [`raidish-services`](../raidish-services/)). Lease countdowns live in memory on the active server; on failover, the new server restarts every persisted lease's TTL afresh, matching etcd's behavior across leader changes.

## CLI Usage

Create an `etcd` dataset with its dedicated client port, then connect with `etcdctl` using a client certificate issued by the cluster CA (`admin`/`node` for writes, `readonly` for reads/watches):

```bash
# Create the dataset (every node listens on port 2379 and forwards to the holder)
raidish dataset create cfg --format etcd --port 2379

# Read and write with standard etcdctl
etcdctl --endpoints=https://node0:2379 \
  --cacert pki/ca.pem --cert pki/admin/cert.pem --key pki/admin/key.pem \
  put /hello world
etcdctl --endpoints=https://node0:2379 \
  --cacert pki/ca.pem --cert pki/admin/cert.pem --key pki/admin/key.pem \
  get /hello
```

## Building & Testing

From the repository root (`build.rs` uses `protoc-bin-vendored`, so no external `protoc` installation is needed):

```bash
# Check build
cargo check -p raidish-etcd

# Run the raidish-etcd unit test suite
cargo test -p raidish-etcd
```
