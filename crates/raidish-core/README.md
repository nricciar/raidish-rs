# raidish-core

The storage engine crate for [RAIDish](../../README.md).

`raidish-core` implements the on-disk format, RAID pool topologies, copy-on-write object store, datasets (filesystems, block volumes, key-value stores), snapshots, replication, and pool repair on top of [`raidish-types`](../raidish-types/). It compiles both natively and to `wasm32-unknown-unknown`.

## Scope

**What's in this crate:**
- Disk backends (local files, native WebSocket remote disks, browser WebSocket disks) and epoch-based write fencing
- Composable vdev topologies: RAID-Z (up to parity 3 via Reed-Solomon), N-way mirrors, and stripes
- Copy-on-write object trees with parent-stored BLAKE3 checksums and TXG-synced metaslab allocation
- Datasets: POSIX-style directories/files (`fs`), sparse virtual block volumes (`block`), and B+tree key-value stores (`kv`)
- Multi-writer coordination primitives (lent metaslabs, committer/writer handoff) and a per-dataset intent log (ZIL)
- Copy-on-write snapshots, clones, and full/incremental send/receive streams
- Scrub, TXG-bounded resilver, `fsck` / salvage, and read-only pool inspection

## Architecture & Module Map

Data flows down from datasets through the copy-on-write object layer and vdev tree to member disks:

```text
Datasets (fs / volume / kv)  ──►  Object Trees & Inodes  ──►  Metaslab Allocator & TXGs
                                                                        │
                                  Leaf Disks (local / WS)  ◄──  Vdev Pool (raidz / mirror / stripe)
```

| Area | Key modules | Purpose |
|---|---|---|
| **Disks & Fencing** | `disk`, `disks`, `coords`, `websocket`, `fence` | `RawDisk` (one disk) and `PoolDevice` (a pool) traits over `4096`-byte blocks, typed block coordinates and disk indexes (`coords`), local/WebSocket backends (`disk_ws`, `disk_wasm`), and per-domain epoch fencing |
| **Vdevs & Topologies** | `vdev`, `raidz`, `mirror`, `stripe` | `Pool` and `Topology` combining RAID-Z (`reed-solomon-simd`), mirrors, and stripes with per-leaf health tracking |
| **Allocator & COW Store** | `blkptr`, `object`, `metaslab`, `tx` | Checksummed `BlockPtr`s, sparse indirect trees, A/B metaslab space maps, and transactional metadata updates |
| **Filesystem & Datasets** | `fs`, `inode`, `dataset`, `open_file`, `file_writer` | `FileSystem<D>`, dnode object table, directory namespace, random-access and streaming file writes |
| **Volumes & KV** | `volume`, `btree`, `kv` | Fixed-size sparse block volumes and ordered B+tree key-value datasets |
| **Multi-Writer & Durability** | `writer`, `zil` | Writer-side allocation into lent metaslabs and fast-flush intent logging |
| **Snapshots & Replication** | `snapshot`, `send` | Zero-copy snapshots/clones and full/incremental replication streams (`send` is native-only) |
| **Maintenance & Inspection** | `scan`, `fsck`, `inspect` | Online scrub/resilver, consistency check & repair, and portable read-only views |

## Quick Example

Formatting a local 3-disk RAID-Z1 pool and writing/reading a file:

```rust
use std::path::Path;
use std::sync::Arc;
use raidish_core::disk::BLOCK_SIZE;
use raidish_core::disks::DiskSet;
use raidish_core::fs::FileSystem;
use raidish_core::vdev::Pool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Backing files (or http(s):// URLs to remote disk servers)
    let paths = ["disks/d1", "disks/d2", "disks/d3"];
    let disks = Arc::new(DiskSet::new(&paths).await);
    let pool = Pool::raidz(disks, 1); // 2 data + 1 parity

    let total_blocks = (100 * 1024 * 1024) / BLOCK_SIZE as u64;
    let fs = FileSystem::format(pool, total_blocks).await?;

    fs.mkdir(Path::new("/docs")).await?;
    fs.write_file(Path::new("/docs/hello.txt"), b"hello raidish").await?;

    let data = fs.read_file(Path::new("/docs/hello.txt")).await?;
    assert_eq!(data, b"hello raidish");
    Ok(())
}
```

Use `FileSystem::load(pool)` (or `FileSystem::load_readonly(pool)` for safe inspection alongside a running writer) to reopen an existing pool.

## Building & Testing

The workspace uses the Rust toolchain pinned in [`rust-toolchain.toml`](../../rust-toolchain.toml) (nightly, with `wasm32-unknown-unknown`). From the repository root:

```bash
# Check native build
cargo check -p raidish-core

# Run the raidish-core unit test suite
cargo test -p raidish-core

# Verify the wasm32 build used by the web dashboard
cargo check -p raidish-core --target wasm32-unknown-unknown
```