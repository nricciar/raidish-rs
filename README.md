# RAIDish

a distributed raidz/mirror filesystem proof of concept

## Setup

step 1) run a server with our disks

```
make setup-etcd-cluster
mkdir disks/
fallocate -l 1G disks/disk1
fallocate -l 1G disks/disk2
fallocate -l 1G disks/disk3
fallocate -l 1G disks/disk4
fallocate -l 1G disks/disk5
cargo run -- --config example-config.yaml listen node-1
```

etcd is optional: set `etcd_config.enable: false` in the config to run without it.
`listen` serves the raw disks on port 8881 (`--port` to change). It serves the
config's `disks` list (or, without one, the pool's disks), so a node can also
serve a single disk with `disks: [disks/disk1]`. A remote disk is addressed as
`http://host:port/api/v1/disks/<index in that node's list>`.

step 2) format the pool from a client

```
// format disks with filesystem (sized from the disks' capacity)
cargo run -- --config remote-config.yaml format
// display blockmap info to the console
cargo run -- --config remote-config.yaml map
```

## Pool layouts

The `pool` section of the config describes how the disks are combined:

```yaml
# 3 disks, raidz1 (2 data + 1 parity)
pool:
  raidz1: [disks/d1, disks/d2, disks/d3]

# 5 disks, raidz2 (3 data + 2 parity)
pool:
  raidz2: [disks/d1, disks/d2, disks/d3, disks/d4, disks/d5]

# any parity
pool:
  raidz: { parity: 3, disks: [d1, d2, d3, d4, d5, d6] }

# n-way mirror
pool:
  mirror: [disks/d1, disks/d2, disks/d3]

# 4 disks on 4 nodes, two mirrors striped together (RAID10)
pool:
  stripe:
    - mirror: [http://node1:8881/api/v1/disks/0, http://node2:8881/api/v1/disks/0]
    - mirror: [http://node3:8881/api/v1/disks/0, http://node4:8881/api/v1/disks/0]
```

A `stripe` can mix raidz groups, mirrors and plain disks (a plain disk has no
redundancy). Stripes can't be nested. Data is interleaved across stripe members
in chunks of one record (~120 KiB). A stripe survives as many failed disks as
its weakest member. `status` shows the layout and each disk's health.

Records are aligned to the pool's width: the number of data shards of a raidz
group, 1 for a mirror, and the least common multiple across a stripe's members.
That width can be at most 30 blocks. The layout is recorded in the superblock,
and a pool won't open with a different one. An old-style config with five
`disks` and no `pool` is still a raidz2 pool.

## Commands

```
// format the pool (--blocks N to override the size)
format
// load filesystem and display root file index
load
// write file to the file index
write path/to/file path/to/dest
// read file from the file index (written to ./output.png)
read path/to/file
// make a directory, list a directory, delete a file/volume
mkdir path
ls path
delete path
// show a root entry's inode
inode name
// display the block map / metaslab usage / orphaned blocks (if any)
map
metaslab
orphaned
// create a copy-on-write volume (virtual block device)
block name size
// export a volume over NBD (port 10809) or NVMe/TCP (port 4420)
nbd name
nvme name
// disk health: online, label, degraded since which TXG
status
// verify every block (data and parity) and the labels, repair what's wrong
scrub [--dry-run]
// rebuild only what degraded disks missed while they were away
resilver
// serve this node's disks (port 8881)
listen [--port N] id
```

`load`, `read`, `ls`, `inode`, `map`, `metaslab`, `orphaned`, `status` and `scrub --dry-run`
open the pool read-only, so they're safe to run against a pool that another process is serving.

## Disk failures, resilver and scrub

Remote disks reconnect on their own. Every request has a timeout (10 s, or
`disk_timeout_ms` in the config). An idle connection is pinged, and dropped if
it goes quiet. While a disk is away, requests to it fail fast and parity covers
for it. A disk whose server is down at startup opens offline and joins once the
server is back.

Every write a disk misses is recorded, and the pool remembers the disk as
*degraded since TXG n*. `resilver` rebuilds that disk. Because the tree is
copy-on-write, it only reads what was written since TXG n and skips the rest.
`nbd` and `nvme` resilver automatically when a degraded disk comes back.

Each member disk carries a label with the pool's id in its first two blocks. A
disk without a matching label (blank, replaced or foreign) is *fenced*: it gets
no reads, because its contents can't be trusted. It then gets a full resilver.

`scrub` checks every reachable block against its checksum, and checks every
parity shard against the data, which normal reads never look at. It also checks
the superblock, uberblocks and space maps. It repairs whatever is wrong, and a
degraded disk is back in sync once the scrub passes.

Scrub and resilver are safe while the pool is in use. They do hold back reuse
of freed space until they finish, and they stop if the pool gets close to full.

## Virtual Block Devices

```
// create a 20 GiB volume
cargo run -- --config remote-config.yaml block vm0 21474836480
```

## NBD

step 1) run the NBD server for a volume

```
cargo run -- --config remote-config.yaml block vdisk0 104857600
cargo run -- --config remote-config.yaml nbd vdisk0
```

step 2) use with nbd

```
// connect /dev/nbd0 to our nbd server
sudo nbd-client localhost 10809 /dev/nbd0

// interact with block device
sudo mkfs.ext4 /dev/nbd0
sudo file -s /dev/nbd0
sudo blkid /dev/nbd0
sudo mount -o discard /dev/nbd0 path/to/mount
sudo umount path/to/mount

// disconnect /dev/nbd0 interface
sudo nbd-client -d /dev/nbd0
```

### Running a VM

QEMU has its own NBD client:

```
// write a disk image onto the volume
qemu-img convert -n -f qcow2 -O raw debian-12-nocloud-amd64.qcow2 nbd://127.0.0.1:10809
// boot it
qemu-system-x86_64 -enable-kvm -cpu host -m 2048 -smp 2 \
  -drive file=nbd://127.0.0.1:10809,format=raw,if=virtio,cache=none,discard=unmap \
  -nic user,model=virtio-net-pci -nographic
```

## NVMe/TCP

There's no discovery controller, so connect to the subsystem directly.

step 1) run the NVMe/TCP target for a volume

```
cargo run -- --config remote-config.yaml block vdisk0 1073741824
cargo run -- --config remote-config.yaml nvme vdisk0
```

step 2) connect with the kernel initiator (needs `nvme-cli`)

```
sudo modprobe nvme-tcp
sudo nvme connect -t tcp -a 127.0.0.1 -s 4420 -n nqn.2024-01.com.raidish:nvme:vdisk0

// find the device: the model is "RAIDISH NVMe-TCP Volume"
sudo nvme list

// disconnect
sudo nvme disconnect -n nqn.2024-01.com.raidish:nvme:vdisk0
```

NVMe device names (`/dev/nvmeXnY`) are assigned dynamically. If your machine has
NVMe drives of its own, check `nvme list` before running `mkfs` on anything.

Set `RAIDISH_NVME_TRACE=1` to log every command the target handles.

## Testing

```
cargo test
// slow crash-consistency tests (crash at ~80 points of a workload, ~2 min)
cargo test -- --ignored
```

`hack/raidish-nbd-test` and `hack/raidish-nvme-test` wrap the few privileged steps
(connect, mkfs, mount, umount, fsck, fstrim) for end-to-end tests against the kernel
initiators. They only touch `/dev/nbd15` and the raidish NVMe namespace, and mount with
`nosuid,nodev,noexec`. Install them root-owned with a single sudo rule each:

```
sudo install -o root -g root -m 0755 hack/raidish-nbd-test /usr/local/sbin/
echo "$(whoami) ALL=(root) NOPASSWD: /usr/local/sbin/raidish-nbd-test" | sudo tee /etc/sudoers.d/raidish-nbd-test
sudo chmod 0440 /etc/sudoers.d/raidish-nbd-test
```

(The same for `raidish-nvme-test`.)

## Limitations

* **One writer per pool.** Nothing yet stops two processes from writing the same pool
  (e.g. `block` from the CLI while `nbd`/`nvme` is serving), and that will corrupt it.
  Stop the server first, or use the read-only commands.
* **No authentication** on the disk server's HTTP/websocket ports.
* Every fsync costs a full TXG commit (~5 ms); there's no intent log yet.
* Volumes live in the root directory, and deleting a directory leaks its contents.
* Pools formatted before `RAIDISHV14` (configurable layouts) must be reformatted.
* Self-heal by checksum tries at most two bad disks per record, so raidz3 only
  gets its third parity shard's worth of protection from missing (offline)
  disks, not from silently corrupt ones.
* The superblock and uberblocks live on the first stripe member (usually),
  so they are only as redundant as that member.

## TODO

* pool exclusivity (writer lease) and multi-node allocation via metaslab leases
* fsck `--repair` (leaks, referenced-but-free blocks), rewind import to an older
  uberblock, rebuilding space maps from the tree, `salvage` of readable data
* fragmentation: contiguous allocation of sequential records, coalesced
  multi-record reads, `defrag` for volumes
* intent log (ZIL) for low-latency fsync
* snapshots and clones
* permissions
* NVMe/TCP: discovery, digests, host authentication
