//! The node's server (`raidish listen`): its config, startup, and its
//! HTTP(S) API on axum (HTTP-1, `docs/ca-service-plan.md` §5): the disks
//! and their WebSocket, files, volumes, VMs, the dashboard and its UI
//! bundle (`build.rs`), with every other crate's routes mounted beside
//! them (`api::router`). The planned `raidish-server` of
//! `docs/workspace-split-plan.md`.
//!
//! The serving primitives (live TLS, the peer per request, connections by
//! serial) are in `raidish_auth::http`, so the crates with routes of their
//! own use them too.

pub use raidish_types::{api_types, types, vm_def};

// The storage core.
#[cfg(test)]
pub(crate) use raidish_core::blkptr;
pub use raidish_core::coords;
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
#[cfg(test)]
pub(crate) use raidish_core::tx;
pub use raidish_core::vdev;
pub(crate) use raidish_core::websocket;
pub(crate) use raidish_core::writer;
#[cfg(test)]
pub(crate) use raidish_core::zil;

pub(crate) use raidish_agent as agent;
pub mod api;
pub mod api_error;
pub(crate) use raidish_auth::auth;
pub use raidish_cluster as cluster;
pub(crate) use raidish_committer as committer;
pub mod config;
pub mod dashboard;
pub(crate) use raidish_committer::events;
pub mod fenced_disks;
pub mod files_api;
pub(crate) use raidish_agent::node_facts;
pub(crate) use raidish_auth::net;
pub(crate) use raidish_auth::pki;
pub(crate) use raidish_committer::object_lease;
pub(crate) use raidish_nfs as nfs;
pub(crate) use raidish_services::frontend;
pub mod server;
pub(crate) use raidish_agent::vm_check;
pub(crate) use raidish_auth::tls;
pub(crate) use raidish_vm::vm;
pub(crate) use raidish_vm::vm_store;
pub mod vms_api;
#[cfg(test)]
pub(crate) use raidish_block::volume_io;
pub mod volumes_api;

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
