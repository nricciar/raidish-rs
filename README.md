# RAIDish

a distributed raidz2 like filesystem proof of concept

## Setup

```
mkdir disks/
fallocate -l 1G disks/disk1
fallocate -l 1G disks/disk2
fallocate -l 1G disks/disk3
fallocate -l 1G disks/disk4
fallocate -l 1G disks/disk5
```

## Usage

```
// format filesystem
cargo run format
// display blockmap info to the console
cargo run map
// load filesystem and display root file index
cargo run load
// write file to root file index
cargo run write path/to/file
// read file from file index
cargo run read file
// display orphaned blocks (if any)
cargo run orphaned
// start a http/websocket API server for filesystem
cargo run listen
// create a virtual block device
cargo run block name size
// expose virtual block device over nbd
cargo run nbd name size
```

## TODO

* virtual block devices (partially implemented)
* directories
* snapshots
