# raidish-nfs

The NFSv3, NFSv4.1, and NFSv4.2 server for [RAIDish](../../README.md).

`raidish-nfs` serves the pool's filesystem dataset ([`raidish-core`](../raidish-core/)) on every `listen` node over a single TCP port (default `2049`, multiplexing NFS, MOUNT, and portmapper). Every node serves the same files under the same file handles, so clients can mount any node—or trunk across multiple nodes—and keep working through node failovers (coordinated via [`raidish-cluster`](../raidish-cluster/) leases).

## Scope

**What's in this crate:**
- ONC RPC v2 over TCP with record marking and concurrent request execution (`rpc`), plus an XDR encoder/decoder (`xdr`)
- Shared protocol-agnostic backend (`backend`): stable `(pool GUID, object ID)` file handles, unstable writes with per-process write verifiers, and per-file `obj/<id>` write leasing
- **NFSv3** (`v3`): RFC 1813 file operations, MOUNT v3, and portmapper v2 on the same TCP port
- **NFSv4.1 & 4.2** (`v4`): `SEQUENCE` sessions with reply caching, opens with share reservations, byte-range locks, cluster-wide state failover with grace period, session trunking (`fs_locations`), pNFS flexfiles layouts (RFC 8435), and NFSv4.2 `SEEK` / `DEALLOCATE`
- Node-to-node forwarding (`peers`): Rocket endpoints (`POST /api/v1/nfs/forward` and `/api/v1/nfs/v4`, `node` certificates only) routing file writes to the active `obj/<id>` holder and stateful v4 ops to the `nfs4` lease owner

## Architecture & Module Map

```text
TCP :2049 (rpc / xdr)
  ├──► v3 (NFSv3 / MOUNT / PMAP)  ──┐
  │                                 ├──► backend (handles, open files, obj/<id> leases) ──► FileSystem (raidish-core)
  └──► v4 (NFSv4.1 / 4.2 / pNFS)  ──┘         │
            │                                 └──► peers (forward dirty-file ops to writer node)
            └──► peers (forward stateful ops to `nfs4` lease owner)
```

| Module | Purpose |
|---|---|
| `lib` | `NfsConfig` (`bind`, `port`, `advertise`), program multiplexing (`Programs`), and `start()` server + background tidy loop |
| `rpc` | ONC RPC v2 framing over TCP (`MAX_RECORD = 4 MiB`), `AUTH_NONE` / `AUTH_SYS` parsing, and concurrent XID-matched replies |
| `xdr` | Big-endian 4-byte-aligned XDR `Reader` / `Writer` |
| `backend` | Shared pool operations by object ID, stable file handles, unstable writes + verifiers, idle-file commit/release, and peer forwarding hook |
| `v3` | NFSv3, MOUNT v3, and portmapper v2 handlers |
| `v4` | NFSv4.1/4.2 compound dispatcher (`mod`), attributes (`attrs`), sessions/opens/locks/grace state (`state`), and op handlers (`ops/{session,open,read,write,lock,layout}`) |
| `peers` | `nfs4` lease ownership loop (`own_v4_state`) and bincode-over-HTTPS peer forwarding routes |

## How It Works

### File Handles & Single-Writer Leasing

File handles encode the pool GUID and the object's never-reused `FileId`, so handles remain valid across restarts and across all nodes. The first write to a file acquires its `obj/<id>` lease; while that node holds uncommitted data, other nodes forward operations affecting that file (`READ`, `WRITE`, `COMMIT`, attributes, truncate) to it and return its write verifier. Idle files are committed and released automatically.

### NFSv4 State, Failover & pNFS

- **Cluster state owner**: One node holds the `nfs4` lease and owns sessions, opens, and locks across the cluster, recording active client owners in Raft so clients reclaim locks during a grace period after failover. Plain data requests (`READ`/`WRITE`/`COMMIT`) run directly on the receiving node once the state owner validates the session slot.
- **pNFS flexfiles**: On multi-node clusters, every node's NFSv3 server acts as a data server with identical handles. Read layouts list all nodes as mirrors; read-write layouts point clients at the active writer node.
- **NFSv4.2**: `SEEK` (`SEEK_DATA`/`SEEK_HOLE`) and `DEALLOCATE` (`fallocate --punch-hole`) operate on copy-on-write records; `ALLOCATE` is refused.

### Mounting & Limitations

By default NFS binds `127.0.0.1:2049` (`nfs:` in the node config). Because `AUTH_SYS` identities are trusted as sent and there is no Kerberos/RPCSEC_GSS authentication, keep the port on loopback or a trusted network.

```bash
# NFSv4.2 (add trunkdiscovery,max_connect=3 on a multi-node cluster)
mount -t nfs -o vers=4.2 127.0.0.1:/ /mnt/pool

# NFSv3 (no NLM lock manager; mount with nolock)
mount -t nfs -o vers=3,proto=tcp,port=2049,mountport=2049,mountproto=tcp,nolock \
  127.0.0.1:/ /mnt/pool
```

Not supported: symlinks, hard links, device nodes, NFSv3 NLM locking, and NFSv4 delegations.

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-nfs

# Run the raidish-nfs unit test suite
cargo test -p raidish-nfs
```
