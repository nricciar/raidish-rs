# raidish-vm

Libvirt virtual machine support and definition storage for [RAIDish](../../README.md).

`raidish-vm` validates, renders, stores, and runs libvirt domains whose disks are RAIDish volumes. Nothing about a VM lives in `/etc/libvirt` on any host: definitions are stored in the pool's `.vms` dataset (via [`raidish-core`](../raidish-core/)), rendered per host to point at that node's NBD exports ([`raidish-block`](../raidish-block/)), and started as transient domains through `virsh`.

## Scope

**What's in this crate:**
- Preserving XML tree parser and serializer (`xml`, built on `quick-xml`) used for libvirt domain XML and S3 request/response bodies
- Strict domain XML validation (`vm`): guarantees every disk is a RAIDish volume (or empty CD-ROM) and blocks host access (host files/devices, passthrough, scripts, `<qemu:commandline>`, direct kernel boot, firmware outside `/usr/share`)
- Transient domain rendering (`vm::render`) and live-migration XML retargeting (`vm::retarget`), rewriting RAIDish disks to NBD, wiring loopback VNC consoles, and stamping pool/node ownership metadata
- Async `virsh` wrapper (`vm::Virsh`) for domain lifecycle, inspection, CPU compatibility checks, and TLS-only live migration (`qemu+tls://`)
- Pool-backed VM and node-facts store (`vm_store::VmStore`) in the `.vms` B+tree dataset, enforcing volume exclusivity and monotonic change numbers
- Async host command helper (`proc`) with `kill_on_drop` semantics

## Module Map

| Module | Purpose |
|---|---|
| `vm` | `VmConfig`, `VmRuntime`, domain XML validation (`parse_spec`), transient rendering (`render`), migration retargeting (`retarget`), ownership metadata (`DomMeta`), and `Virsh` |
| `vm_store` | `VmStore<D>` over the `.vms` B+tree system dataset storing validated VM definitions (`V<name>`) and host capability facts (`N<node>`) |
| `xml` | Round-trip DOM-style XML tree (`El`, `Node`, `parse_root`, `el_xml`) keeping untouched XML verbatim |
| `proc` | Async `tokio::process::Command` wrappers (`ran`, `run`) returning trimmed stdout or formatted stderr errors |

## How It Works

### Disks, Validation & Transient Rendering

A domain XML marks a RAIDish volume on its disk `<source>`:

```xml
<disk type='file' device='disk'>
  <source file='raidish:vm1'/>          <!-- or protocol='nbd' name='vm1' -->
  <target dev='vda' bus='virtio'/>
</disk>
```

Because libvirt runs domains with host privileges, every definition is validated when stored and again before launch to ensure the guest stays isolated. When starting a VM, `vm::render` drops host-specific boilerplate from `virsh dumpxml` (`<emulator>`, `<seclabel>`, `<resource>`, graphics, pty paths), rewrites each volume source to the node's NBD export, adds a local VNC console, and stamps RAIDish ownership metadata (`https://raidish.dev/xmlns/vm/1`) so a node only acts on domains it owns.

### The `.vms` Store

`VmStore` persists definitions in the `.vms` B+tree dataset (written exclusively by the committer):
- Each save checks that every referenced RAIDish volume exists and is not claimed by another VM.
- A store-wide monotonic `change` counter prevents stale edits (optimistic concurrency) and detects when a running domain's XML is out of date.
- Host capability snapshots (`NodeFacts`) live alongside VM definitions under `N<node>`.

### Configuration

Add `vm: {}` to a `listen` node's config to enable VMs with defaults (all fields are optional overrides):

```yaml
vm:
  uri: qemu:///system       # default
  shutdown_timeout_s: 60    # default before destroy
  console_listen: 127.0.0.1 # default (unpassworded VNC; keep local)
  migrate_timeout_s: 300    # default; live migration is qemu+tls:// only
  # nbd_host / nbd_port     # override if libvirt is on another network stack
  # migrate_uri / migrate_host
```

## CLI Usage

```bash
# Import a domain XML (with <source file='raidish:VOLUME'/>) and start it
raidish vm import myvm myvm.xml        # --replace to update an existing definition
raidish vm start myvm
raidish vm list

# Check migration compatibility across nodes, then move (live if supported, else cold)
raidish vm check myvm
raidish vm move myvm node-2            # --live refuses a cold fallback

# Lifecycle and heterogeneous-CPU baseline helper
raidish vm restart myvm                # applies pending XML edits
raidish vm stop myvm
raidish vm cpu-baseline                # <cpu> element common to all VM hosts
```

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-vm

# Run the raidish-vm unit test suite
cargo test -p raidish-vm
```
