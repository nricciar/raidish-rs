//! RAIDish: a distributed raidz/mirror filesystem.
//!
//! The storage core (pool, filesystem, volumes, scrub/fsck, inspection) is
//! portable and also builds for `wasm32-unknown-unknown`, where it reads a
//! pool over the `listen` nodes' websocket block protocol (the web
//! dashboard). The servers and the command line are native only.

#[cfg(not(target_arch = "wasm32"))]
#[macro_use]
extern crate rocket;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod agent;
#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod api_error;
pub mod api_types;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod auth;
pub(crate) mod blkptr;
pub(crate) mod btree;
#[cfg(not(target_arch = "wasm32"))]
pub mod cli;
#[cfg(not(target_arch = "wasm32"))]
mod cli_pki;
#[cfg(not(target_arch = "wasm32"))]
mod cli_vm;
pub mod cluster;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod committer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod config;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod csi;
#[cfg(not(target_arch = "wasm32"))]
mod dashboard;
pub(crate) mod dataset;
pub mod disk;
pub mod disks;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod etcd;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod events;
#[cfg(not(target_arch = "wasm32"))]
mod export;
pub(crate) mod fence;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod fenced_disks;
pub(crate) mod file_writer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod files_api;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod frontend;
pub mod fs;
mod fsck;
pub mod inode;
pub mod inspect;
pub(crate) mod kv;
mod metaslab;
mod mirror;
#[cfg(not(target_arch = "wasm32"))]
mod nbd;
#[cfg(not(target_arch = "wasm32"))]
mod net;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod nfs;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod node_facts;
#[cfg(not(target_arch = "wasm32"))]
mod nvme;
mod object;
#[cfg(not(target_arch = "wasm32"))]
mod object_lease;
pub(crate) mod open_file;
#[cfg(not(target_arch = "wasm32"))]
mod peer_io;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod pki;
#[cfg(not(target_arch = "wasm32"))]
mod proc;
mod raidz;
mod rt;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod s3;
mod scan;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod send;
#[cfg(not(target_arch = "wasm32"))]
mod server;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod service_tree;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sigv4;
pub(crate) mod snapshot;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod sqs;
mod stripe;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod tls;
mod trace;
pub(crate) mod tx;
pub mod types;
#[cfg(not(target_arch = "wasm32"))]
mod ui;
pub mod vdev;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod vm;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod vm_check;
pub mod vm_def;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod vm_store;
#[cfg(not(target_arch = "wasm32"))]
mod vms_api;
mod volume;
#[cfg(not(target_arch = "wasm32"))]
mod volume_io;
#[cfg(not(target_arch = "wasm32"))]
mod volumes_api;
pub(crate) mod websocket;
pub(crate) mod writer;
#[cfg(not(target_arch = "wasm32"))]
mod xml;
pub(crate) mod zil;

#[cfg(test)]
#[path = "tests/fault_tests.rs"]
mod fault_tests;
#[cfg(test)]
#[path = "tests/util.rs"]
mod test_util;
