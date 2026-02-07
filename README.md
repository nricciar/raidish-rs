# RAIDish

a distributed raidz2 like filesystem proof of concept

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

## Usage

```
// format disks with filesystem
cargo run -- --config remote-config.yaml format
// display blockmap info to the console
cargo run -- --config remote-config.yaml map
```

## Other Commands

```
// load filesystem and display root file index
load
// write file to root file index
write path/to/file path/to/dest
// read file from file index
read path/to/file
// display orphaned blocks (if any)
orphaned
// create a virtual block device
block name size
// expose virtual block device over nbd
nbd name size
```

## NBD

step 1) run NBD server for virtual block device

```
// create virtual block device (100MB)
cargo run -- --config remote-config.yaml block vdisk0 104857600
cargo run -- --config remote-config.yaml nbd vdisk0 104857600
```

step 2) use with nbd

```
// connect /dev/nbd0 to our nbd server
sudo nbd-client localhost 10809 /dev/nbd0

// interact with block device
sudo mkfs.ext4 /dev/nbd0
sudo file -s /dev/nbd0
sudo blkid /dev/nbd0
sudo mount /dev/nbd0 path/to/mount
sudo umount path/to/mount

// disconnect /dev/nbd0 interface
sudo nbd-client -d /dev/nbd0
```

## TODO

* virtual block devices (partially implemented)
* permissions
* snapshots
