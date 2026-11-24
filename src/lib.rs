//! RAIDish: a distributed raidz/mirror filesystem.
//!
//! The storage core (pool, filesystem, volumes, scrub/fsck, inspection) is
//! portable and also builds for `wasm32-unknown-unknown`, where it reads a
//! pool over the `listen` nodes' websocket block protocol (the web
//! dashboard). The servers and the command line are native only.

pub use raidish_types::{api_types, types, vm_def};

// The storage core.
#[cfg(test)]
pub(crate) use raidish_core::blkptr;
pub(crate) use raidish_core::dataset;
pub use raidish_core::disk;
pub use raidish_core::disks;
pub use raidish_core::fence;
pub use raidish_core::fs;
pub use raidish_core::inode;
pub use raidish_core::inspect;
pub(crate) use raidish_core::metaslab;
pub(crate) use raidish_core::object;
pub(crate) use raidish_core::open_file;
#[cfg(test)]
pub(crate) use raidish_core::raidz;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_core::send;
pub(crate) use raidish_core::snapshot;
#[cfg(test)]
pub(crate) use raidish_core::tx;
pub use raidish_core::vdev;
pub(crate) use raidish_core::volume;
pub(crate) use raidish_core::websocket;
pub(crate) use raidish_core::writer;
#[cfg(test)]
pub(crate) use raidish_core::zil;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod agent;
#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod api_error;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_auth::auth;
#[cfg(not(target_arch = "wasm32"))]
pub mod cli;
#[cfg(not(target_arch = "wasm32"))]
mod cli_pki;
#[cfg(not(target_arch = "wasm32"))]
mod cli_vm;
pub use raidish_cluster as cluster;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_committer as committer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod config;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_csi as csi;
#[cfg(not(target_arch = "wasm32"))]
mod dashboard;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_block::export;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_committer::events;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod fenced_disks;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod files_api;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_auth::net;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_block::nbd;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_nfs as nfs;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_services::frontend;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod node_facts;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_auth::pki;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_block::nvme;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_block::peer_io;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_committer::object_lease;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_vm::proc;
#[cfg(not(target_arch = "wasm32"))]
mod server;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_auth::tls;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_vm::vm;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod vm_check;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_vm::vm_store;
#[cfg(not(target_arch = "wasm32"))]
mod vms_api;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use raidish_block::volume_io;
#[cfg(not(target_arch = "wasm32"))]
mod volumes_api;

#[cfg(test)]
#[path = "tests/cluster_tests.rs"]
mod cluster_tests;
#[cfg(test)]
#[path = "tests/disk_remote_tests.rs"]
mod disk_remote_tests;
#[cfg(test)]
#[path = "tests/fault_tests.rs"]
mod fault_tests;
#[cfg(test)]
#[path = "tests/multinode_tests.rs"]
mod multinode_tests;
#[cfg(test)]
#[path = "tests/scan_remote_tests.rs"]
mod scan_remote_tests;
#[cfg(test)]
#[path = "tests/util.rs"]
mod test_util;
#[cfg(test)]
#[path = "tests/zil_tests.rs"]
mod zil_tests;
