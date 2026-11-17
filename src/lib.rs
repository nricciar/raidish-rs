//! RAIDish: a distributed raidz/mirror filesystem.
//!
//! The storage core (pool, filesystem, volumes, scrub/fsck, inspection) is
//! portable and also builds for `wasm32-unknown-unknown`, where it reads a
//! pool over the `listen` nodes' websocket block protocol (the web
//! dashboard). The servers and the command line are native only.

// On wasm most of the crate is only reachable through `inspect`.
#![cfg_attr(target_arch = "wasm32", allow(dead_code, unused_imports))]

#[cfg(not(target_arch = "wasm32"))]
#[macro_use]
extern crate rocket;

#[cfg(not(target_arch = "wasm32"))]
pub mod agent;
#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
pub mod auth;
pub mod blkptr;
pub mod btree;
#[cfg(not(target_arch = "wasm32"))]
pub mod cli;
#[cfg(not(target_arch = "wasm32"))]
mod cli_pki;
#[cfg(not(target_arch = "wasm32"))]
mod cli_vm;
#[cfg(not(target_arch = "wasm32"))]
mod cluster;
#[cfg(not(target_arch = "wasm32"))]
pub mod committer;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
mod dashboard;
pub mod dataset;
pub mod disk;
pub mod disks;
#[cfg(not(target_arch = "wasm32"))]
pub mod etcd;
#[cfg(not(target_arch = "wasm32"))]
pub mod events;
pub mod fence;
pub mod file_writer;
#[cfg(not(target_arch = "wasm32"))]
pub mod files_api;
#[cfg(not(target_arch = "wasm32"))]
pub mod frontend;
pub mod fs;
mod fsck;
pub mod inode;
pub mod inspect;
pub mod kv;
mod metaslab;
mod mirror;
#[cfg(not(target_arch = "wasm32"))]
mod nbd;
#[cfg(not(target_arch = "wasm32"))]
pub mod nfs;
#[cfg(not(target_arch = "wasm32"))]
mod nvme;
mod object;
pub mod open_file;
#[cfg(not(target_arch = "wasm32"))]
pub mod pki;
mod raidz;
mod rt;
#[cfg(not(target_arch = "wasm32"))]
pub mod s3;
mod scan;
#[cfg(not(target_arch = "wasm32"))]
pub mod send;
#[cfg(not(target_arch = "wasm32"))]
pub mod sigv4;
pub mod snapshot;
#[cfg(not(target_arch = "wasm32"))]
pub mod sqs;
mod stripe;
#[cfg(not(target_arch = "wasm32"))]
pub mod tls;
mod trace;
#[cfg(not(target_arch = "wasm32"))]
mod ui;
pub mod vdev;
#[cfg(not(target_arch = "wasm32"))]
pub mod vm;
pub mod vm_def;
#[cfg(not(target_arch = "wasm32"))]
mod vms_api;
mod volume;
#[cfg(not(target_arch = "wasm32"))]
mod volumes_api;
pub mod websocket;
pub mod writer;
pub mod zil;

#[cfg(test)]
#[path = "tests/util.rs"]
mod test_util;
