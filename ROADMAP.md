# raidish 1.0 Roadmap

## Context
`TODO` is a rough 1.0 wish list. Some items are concrete fixes, some are aspirational, and several are tangled together. For example, "no plaintext", libvirt TLS, live migration and maintenance mode are really one dependency chain. The goal is a set of workstreams, each made of tasks that can be shipped separately, with their dependencies stated and a proposed cut for what is in 1.0 and what comes after. The task IDs (`VM-1`, `SEC-3`, …) are there so tasks can be referenced directly (e.g. "do VM-2").

## Where the code stands today (what shaped this plan)
- **VMs & live migration**
  - Domain XMLs live in the `.vms` system dataset (`src/vm_store.rs`, B+tree of `V<name>` keys written by the committer; `N<node>` keys hold node facts) and run as transient domains via `virsh create` (`src/vm.rs`).
  - Placement follows the "anchor" disk's `ds/` lease. **Live migration is implemented** (`src/agent_handoff.rs`, `src/agent_migrate.rs`, `src/peer_io.rs`, `src/volume_io.rs`, `src/vm_check.rs`): `POST /api/v1/vms/<name>/move` (and `raidish vm move`) checks readiness, attaches the VM's volumes to both nodes with mTLS peer I/O proxying, runs `virsh migrate --live --p2p --tls`, and atomically hands off the `ds/` lease to the target (falling back to cold move unless `--live` is required). Verified on `docker-compose.migrate.yml` and a 3-node DigitalOcean cluster (`deploy/do/SETUP.md` §10).
  - Node facts (`src/node_facts.rs`, `GET /api/v1/nodes/facts`) now report host CPU/memory/KVM, libvirt/qemu versions, `domcapabilities`, active networks and Linux bridges, and peer libvirt/peer-I/O TLS probes.
  - In the create form (`web/src/vm_new.rs`), the disk is still always created new, the ISO must be a raidish volume, and the network is still free text (not yet wired to `nodes/facts`).
- **Plaintext today**
  - NBD (`127.0.0.1:10809`), NVMe/TCP (`:4420`) and NFS (`:2049`, AUTH_SYS taken on trust, write permissions not enforced) are still plaintext.
  - The main API still falls back to plain HTTP with no auth when `tls:` is unset.
  - Local VM control uses `qemu:///system`; cross-node libvirt probing and live migration are **TLS-only** (`qemu+tls://<host>/system` and qemu `/etc/pki/qemu`, issued by `raidish cert libvirt`; `qemu+tcp` is rejected on config load).
  - Node-to-node block proxying (`peer_io`, default `:10810`), S3, SQS and etcd are TLS-only.
- **Events**
  - Events are a durable log in `.events` (`src/events.rs`).
  - The UI has a collapsible activity footer (`web/src/activity.rs`) that polls `GET /api/v1/events` every 3 s (not pushed over WS/SSE yet, no toast popups).
  - SQS isn't used inside raidish.
- **etcd & Kubernetes**
  - KV, Txn, Watch, Lease, Compact, plus `Status`/`Alarm`/`Defragment`/`MemberList`/`AuthStatus` are implemented and **validated with a 3-node k3s cluster** (`deploy/do/README.md`, `deploy/do/SETUP.md`), including node restarts and degraded-pool committer failovers.
  - A **Kubernetes CSI driver** (`raidish csi`, `src/csi.rs`, `deploy/do/csi.yaml`) is implemented and tested on k3s for both `ReadWriteOnce` ext4 and `ReadWriteOnce`/`ReadWriteMany` raw block PVCs (using MIG-1 peer I/O and atomic handoff).
  - `Hash`, `HashKV`, `Snapshot`, `MoveLeader`, `Downgrade`, and most of `Auth`/`Cluster` remain unimplemented.
- **Shutdown**
  - SIGTERM only stops Raft (`stop_raft_on_shutdown`). Leases expire (~10 s) rather than being released.
  - VMs aren't shut down/migrated and datasets aren't detached (`FileSystem::detach_dataset` in `src/writer.rs` is unused at shutdown). There is no drain or cordon.
- **send/recv**
  - Goes through stdout/stdin pipes only (with committer `/api/v1/pool/volumes/<name>/recv` on the receive side). There is no network transport, pull mode, scheduling or retention.
- **API & UI**
  - Consistently `/api/v1` with shared wire types (`src/api_types.rs`) and unified error mapping (`src/api_error.rs`), but gaps remain between CLI, API and UI:
    - Datasets are still only reachable through committer-internal `/api/v1/pool/datasets` (service keys have `/api/v1/services/keys`).
    - `scrub`, `fsck`, `resilver`, `defrag`, and `salvage` are CLI-only.
    - PKI is CLI-only.
    - `send` has no API.
  - The UI has Overview (with `VmHosts` readiness panel), VMs (list, detail page with live/cold move dialog and readiness reasons, console, create/import), and Files (including the Volumes panel), plus the activity footer.

---

## Workstreams and tasks

### A. Foundations (do first, everything else builds on these)
- **API-1 API conventions doc + resource map.** Write down the public resource tree (`/api/v1/{nodes,disks,volumes,datasets,services,vms,networks,events,replication,pki}`), the verb and permission conventions, the error shape (building on `src/api_error.rs` / `src/api_types.rs`), and long-running operations as "jobs" (for migrate, scrub, send). Every later task follows it.
- **NODE-1 Node capability/preflight report.** *(done: `src/node_facts.rs`, `raidish node facts`, `GET /api/v1/nodes/facts`, `web/src/hosts.rs`, `docs/design/mig-4-readiness.md`)* Each node collects facts and stores them in `.vms` (`N<node>` keys via the committer):
  - host CPU, memory, kernel, arch, and `/dev/kvm`;
  - libvirt and qemu versions, `domcapabilities`, and machine types;
  - active libvirt networks and Linux bridges;
  - peer libvirt TLS, peer I/O, and last qemu migrate-TLS probes with actionable hints.

  This primitive feeds MIG-4 (done), VM-3 (network dropdown), and MAINT-2.
- **EVT-1 Push events to the UI.** Stream `.events` over a websocket or SSE (`GET /api/v1/events/stream`). Change `web/src/activity.rs` from polling to subscribing, with a vCenter-style toast/footer alert for important kinds (`node.down`, `disk.offline`, `vm.start_failed`, …).

### B. VM create/edit UX (quick, concrete wins)
- **VM-1 Volume usage index.** An API that lists the volumes not referenced by any VM in `.vms` (and not mounted or leased elsewhere). It defines what "unused" means.
- **VM-2 Disk picker.** The form chooses between "create new (size)" and "use existing unused volume" (from VM-1). This means updating `vm_def::NewVm` and `vm_create_simple` (`web/src/api.rs`).
- **VM-3 Network picker.** A dropdown of libvirt networks/bridges that exist on *every* node allowed to run the VM, taken from NODE-1 (`GET /api/v1/nodes/facts`). A network missing on some nodes shows a warning, not just a hidden option. Optionally, an allow-list `vm.networks:` in config.
- **VM-4 ISO from the filesystem.** Let the cdrom point at a file in a raidish filesystem dataset:
  - export it as a read-only NBD export of a file, reusing `src/nbd.rs`;
  - the picker browses the files API and still also offers "unused volume".

  This gives you an ISO library for free, and S3 later (PLAT-4).

### C. Security: "no plaintext anywhere"
- **SEC-1 Secure-by-default mode.** Refuse to start without `tls:` unless an explicit `insecure_dev: true` is set. Add a startup audit that logs every listener and its security. Make certs expire by default and add `raidish cert renew`/rotation (expiry warning in `src/server.rs` and `cert issue --force` / `cert trust` / `untrust` already exist).
- **SEC-2 NFS RPC-with-TLS (RFC 9289).**
  - Implement the AUTH_TLS probe and STARTTLS upgrade in `src/nfs/rpc.rs`, which works with the Linux client's `xprtsec=tls|mtls` via tlshd.
  - Under mTLS, map the client cert to an identity and squash or override the AUTH_SYS uid.
  - Enforce write permissions on the server (`src/nfs/v3.rs` gap).
  - Add a config option to require TLS on NFS.
- **SEC-3 Block protocols over TLS.** *(note: node-to-node block proxying in `src/peer_io.rs` is already mTLS-only; client-facing NBD/NVMe exports remain plaintext on loopback)*
  - NBD: TLS via NBD_OPT_STARTTLS, which qemu supports with `tls-creds-x509`. `render()` emits the matching `<source><tls/>`.
  - NVMe/TCP: TLS 1.3 PSK/TP-8011 and/or DH-HMAC-CHAP.
  - This is required before any of these listen beyond loopback (PLAT-2 in guest VMs, NET-3).
- **SEC-4 Libvirt TLS with raidish PKI.** *(done: `raidish cert libvirt`, `src/pki.rs`, `deploy/do/SETUP.md` §10; checked by NODE-1's probes; migrations are TLS-only)* `raidish cert libvirt` issues server and client certs in libvirt's `/etc/pki/libvirt*` layout plus qemu `/etc/pki/qemu` certs. Documented in `deploy/do/SETUP.md` §10; NODE-1 reports misconfigurations with hints.

### D. Live migration (needs SEC-4 and NODE-1)
- **MIG-1 Volume handoff during migration.** *(done: `docs/design/mig-1-volume-handoff.md`, `src/volume_io.rs`, `src/peer_io.rs`, `src/agent_handoff.rs`, `src/cluster/state.rs`, `src/cluster/lease.rs`, `src/csi.rs`)* Implemented via option (a) (proxying to the single writer, preserving fencing and single-writer invariants):
  - Atomic `Request::Transfer` lease handoff (`HeldLease::transfer`/`adopt`).
  - `VolumeIo` + `SwitchableIo` pause/switch layer under both NBD and NVMe exports.
  - mTLS node-to-node `PeerIo` (`:10810`, protocol `/2` with epoch checks and unclean-writer flush failure tracking).
  - Cluster-state attachment set (`ClusterState::attached`, `Request::Attach`/`SetAttached`, `raidish cluster attach`), proxied exports, and zero-downtime handoff (also used by `volume move` and CSI `ReadWriteMany` raw block volumes).
- **MIG-2 Migration orchestration.** *(done: `docs/design/mig-2-live-migration.md`, `src/agent_migrate.rs`, `src/agent.rs`, `src/vm.rs`, `src/vms_api.rs`)*
  - Triggered via existing `POST /api/v1/vms/<name>/move` (one way to move a VM, live when readiness allows): attaches volumes to source and target, prepares target (`POST /api/v1/vm/incoming/<name>`), retargets `virsh dumpxml --migratable` (`--xml`), runs `virsh migrate --live --p2p --tls` to `qemu+tls://<host>/system`, hands off volumes via MIG-1, and re-stamps metadata live on the target.
  - On pre-check failure or migration failure, falls back to cold move (unless `live: true` / `--live` is set, which refuses with 409). Emits events.
  - Verified on `docker-compose.migrate.yml` and 3-node droplets.
- **MIG-3 Migrate in CLI and UI.** *(mostly done; progress UI remaining)*
  - Done: `raidish vm move <name> <node> [--live]` (`src/cli_vm.rs`) and the VM page Move dialog (`web/src/vm_page.rs`, showing live vs cold per target node and reasons why not).
  - Remaining: live migration progress indicator in CLI and UI beyond activity log events (driven by jobs once API-1 lands).
- **MIG-4 Readiness check.** *(done: `src/vm_check.rs`, `GET /api/v1/vms/<name>/check`, `POST /api/v1/vm/check`, `POST /api/v1/vm/probe/<node>`, `GET /api/v1/vm/cpu-baseline`, `raidish vm check`, `raidish vm cpu-baseline`, `web/src/vm_page.rs`, `web/src/hosts.rs`, `docs/design/mig-4-readiness.md`)* Per VM and destination: libvirt reachability, arch/KVM/machine type, matching network/bridge, CPU compatibility via `virsh hypervisor-cpu-compare` (judged against the running guest's CPU), qemu version, peer I/O, and live TLS probes (all from NODE-1). Shown in the UI (`VmHosts` and VM page) and enforced before every `move`.

### E. Maintenance mode
- **MAINT-1 Graceful shutdown.** On SIGTERM:
  - live-migrate or stop VMs (using MIG-2 where possible, cold move otherwise);
  - flush and `detach_dataset` for owned volumes and datasets;
  - explicitly release `ds/` and `node/` leases;
  - stop the committer cleanly.

  It must fit inside the systemd `TimeoutStopSec` in `hack/raidish.service`, which may need raising.
- **MAINT-2 Cordon/drain.** A node `maintenance` flag in cluster state: no new placements, and existing VMs and volumes are moved off (live-migrated/handed off via MIG-1/MIG-2 when possible). `raidish node drain|uncordon` plus API and UI. `systemctl stop` performs a drain and then exits.
- **MAINT-3 Disk maintenance.** Take a disk offline or replace it gracefully, ahead of resilver. The CLI tools probably cover most of this; it needs an API and UI (ties into API-2).

### F. API unification (incremental, alongside everything else)
- **API-2 Close the gaps table.** Public `/api/v1/datasets`, services, PKI (cert/token issue), and pool tools (scrub/fsck/resilver as jobs). *(Already done: `vm import` via `PUT /api/v1/vms/<name>`, service keys via `POST /api/v1/services/keys`.)*
- **API-3 CLI as an API client.** Every CLI command goes through the API (local or remote), so "anything from anywhere" holds automatically. *(Partially done: `vm` and `node` commands already use the HTTP API; `volume`/`dataset` use the committer API when one runs; pool tools, `cert`, and `send` remain local/direct.)*
- **API-4 UI pages.** Datasets/Services, Cluster/Nodes (with maintenance), Replication, Events history. *(Already present: Volumes panel under Files, VMs + VM detail page, `VmHosts` readiness card on Overview, activity footer.)* These build on API-2.

### G. Replication and tunnelling
- **REP-1 Native send/recv transport.** A cluster-to-cluster stream over mTLS HTTPS (push and pull), reusing `src/send.rs`'s `AsyncRead`/`AsyncWrite` and the existing `/recv` route. Cross-cluster trust works by pairing (exchange CA or pairing token).
- **REP-2 Replication policies.** Scheduled snapshot, then incremental send, with retention on both sides, as a job with events. API and UI ("back up volume X to cluster Y nightly").
- **REP-3 NAT-traversing transport.** *(aspirational)* A pluggable transport under REP-1. Evaluate iroh (Rust, QUIC, hole punching plus relays), WireGuard/boringtun, and the tox tunnel you linked. Recommend spiking iroh first since it's native Rust. UI for pairing and tunnel setup.
- **REP-4 S3 versioning & CRDT storage layout.** *(post-1.0; see `docs/multi-master-plan.md`)* Extend S3 buckets (`src/s3/mod.rs`) with content-addressed refcounted chunks (`B <blake3>`), immutable versions + tombstones with HLC/`(origin_site, origin_seq)` LWW ordering, a bucket-local oplog (`L <seq>`) + peer cursors (`C <peer>`), and split-independent Merkle range-hashes in `src/btree.rs` branch nodes.
- **REP-5 S3 oplog replication & bidirectional Merkle reconcile.** *(post-1.0; depends on REP-1, REP-4)* Active-active multi-region/multi-cloud S3 sync over REP-1: continuous zstd-compressed, chunk-deduped oplog streaming (sliding window, `origin_site` loop prevention) paired with lock-free local CoW snapshot + bidirectional Merkle range-hash reconciliation for bootstrapping, outage catch-up, and anti-entropy.
- **REP-6 Multi-cloud failover ergonomics.** *(post-1.0; depends on REP-5)* Readiness gate (`GET /api/v1/replication/ready`) so recovering clusters catch up before DNS re-adds them, optional bounded 404 peer read-through (`read_through: true`) to smooth over the ~1 s replication window, and replication lag/status observability.
- **REP-7 Opt-in multi-master POSIX datasets.** *(post-1.0 / exploratory; depends on REP-5 and per-dataset POSIX snapshots)* Explicit per-dataset replication modes (`local` strict POSIX default, `replicated-ro`, opt-in `multi-master`) reusing the S3 chunk + CRDT reconcile engine, with a Move-Tree CRDT for renames, `CLOSE`/idle whole-file versioning over NFS, loud refusal (`ENOLCK`) or explicit opt-in for zone-local locks, and `.conflict-<site>` visibility.

### H. Platform: dogfood KV, bus, S3 and etcd
- **PLAT-1 Kubernetes on raidish (validation).** *(done on 3-node k3s droplets: `deploy/do/README.md`, `deploy/do/SETUP.md`; automated integration test & in-VM k8s optional)*
  - Validated with 3 k3s servers pointing at raidish's `k8s` etcd dataset over mTLS, including node restarts and degraded-pool committer failovers (uncovered and fixed the in-doubt commit wedge, per-dataset flushes, hedged lease calls, and etcd lease-expiry / compaction `prev_kv` bugs).
  - Remaining if wanted: running k8s inside 3 pinned raidish VMs (vs directly on the cluster nodes) and an automated CI/compose integration test.
- **PLAT-2 CSI driver for raidish volumes.** *(done for host k8s nodes: `src/csi.rs`, `raidish csi`, `deploy/do/csi.yaml`, `deploy/do/SETUP.md` §8, `src/tests/csi_tests.rs`)*
  - Implemented and verified on k3s over loopback NVMe/TCP (`127.0.0.1`): dynamic provisioning, `ReadWriteOnce` ext4 filesystem volumes, `ReadWriteOnce` raw block volumes, and `ReadWriteMany` (`MULTI_NODE_MULTI_WRITER`) raw block volumes backed by MIG-1 peer I/O and atomic handoff.
  - Remaining (post-1.0): exposing CSI into guest k8s VMs over VM networks (requires SEC-3 and NET-2).
- **PLAT-3 Container registry.** Run `distribution` (registry:2) with its S3 driver pointed at raidish S3, packaged as a raidish "service". This answers both "datastore for docker images" and "a use for S3".
- **PLAT-4 More S3 uses.** An ISO and cloud-image library (VM-4 can read from a bucket), and an S3 replication target for REP-2.
- **PLAT-5 Events to the bus.** Optionally mirror `.events` into an SQS queue so external tooling can subscribe. This puts the message bus to real use without making it load-bearing.

### I. VM networking (mostly post-1.0)
- **NET-1 Managed networks.** *(1.0)* raidish defines networks in cluster state and creates them as identically named libvirt networks on every node. This removes the "same network name on all hosts" migration problem by construction, and VM-3 then lists these.
- **NET-2 Private overlay networks.** A per-VM private network by default, plus joinable shared private networks across nodes (VXLAN/Geneve over the cluster's links, encrypted with WireGuard or IPsec to stay "no plaintext"). A per-network toggle exposes raidish endpoints (NFS, S3, NVMe) on it.
- **NET-3 Cross-cluster private networks.** Reuse the REP-3 tunnel layer.
- **NET-4 Public IPs.** A MetalLB-style BGP/ARP advertiser for VM and pod IPs.

---

## Proposed ordering and 1.0 cut
| Milestone | Tasks | Status / Notes |
|---|---|---|
| M1: Foundations + quick wins | API-1, ~~NODE-1~~, EVT-1, VM-1..4, MAINT-1 | **NODE-1 done.** Remaining items are independent and can run in parallel |
| M2: Lock it down | SEC-1..3, ~~SEC-4~~, API-2 (ongoing) | **SEC-4 done.** SEC-2 and SEC-3 are the largest remaining |
| M3: Live migration | NET-1, ~~MIG-1~~, ~~MIG-2~~, MIG-3 (progress UI), ~~MIG-4~~ | **MIG-1, MIG-2, MIG-4 done; MIG-3 CLI/UI move done** (progress UI left). NET-1 remains |
| M4: Maintenance | MAINT-2, MAINT-3, API-4 | Unblocked now that live migration (MIG-1/2/4) is done |
| M5: Replication | REP-1, REP-2, API-3 | |
| M6: Dogfood | ~~PLAT-1~~, ~~PLAT-2 (host)~~, PLAT-3, PLAT-5 | **PLAT-1 & host PLAT-2 done** on 3-node k3s droplets; PLAT-3 and PLAT-5 remain |
| Post-1.0 | REP-3, REP-4..7, PLAT-2 (guest VMs), PLAT-4, NET-2..4 | Aspirational, multi-master S3/POSIX replication (`docs/multi-master-plan.md`), or depend on overlay networking / block TLS |

Key dependency chains:
- ~~NODE-1~~ → VM-3, ~~MIG-4~~
- ~~SEC-4~~ + ~~MIG-1~~ → ~~MIG-2~~ → MAINT-2 (live)
- SEC-3 → PLAT-2 (from guest VMs) / NET-2 exposure
- REP-1 → REP-3 → NET-3
- REP-1 + REP-4 → REP-5 → REP-6, REP-7

## Traceability and testing
- Every line of `TODO` maps to at least one task ID. Mapping:

  | TODO lines | Task IDs |
  |---|---|
  | 1–5 | VM-1..4 |
  | 6 | SEC-2 |
  | 7 | SEC-4 |
  | 8 | SEC-1, SEC-3 |
  | 9 | MIG-1..3 |
  | 10–12 | NODE-1, MIG-4 |
  | 13–14 | REP-1..3 |
  | 15–18 | NET-1..4 |
  | 20 | EVT-1, PLAT-5 |
  | 21–26 | PLAT-1..3, NET-4 |
  | 27 | PLAT-3, PLAT-4 |
  | 28–29 | API-1..4 |
  | 30–32 | MAINT-1..3 |
- Each task, once picked up, gets verified on the docker-compose 3-node cluster (`docker-compose.yml` / `docker-compose.migrate.yml`) and/or the 3-node droplet cluster (`deploy/do/`): live migration, k3s/etcd, and CSI are already verified there; drain across nodes and an NFS mount with `xprtsec=mtls` from a client container remain to be verified as those tasks land.
