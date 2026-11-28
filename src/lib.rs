//! RAIDish: a distributed raidz/mirror filesystem. This crate is the
//! `raidish` command line; the node it runs with `listen` is
//! `raidish-http`, and the rest is in the crates under `crates/`.

pub use raidish_types::{api_types, types, vm_def};

// The storage core.
pub use raidish_core::coords;
pub(crate) use raidish_core::dataset;
pub use raidish_core::disk;
pub use raidish_core::disks;
pub use raidish_core::fence;
pub use raidish_core::fs;
pub use raidish_core::inode;
pub use raidish_core::inspect;
pub use raidish_core::role;
pub(crate) use raidish_core::send;
pub(crate) use raidish_core::snapshot;
pub use raidish_core::vdev;
pub(crate) use raidish_core::volume;
pub(crate) use raidish_core::writer;

pub(crate) use raidish_auth::auth;
pub mod cli;
mod cli_ca;
mod cli_pki;
mod cli_vm;
pub(crate) use raidish_agent::node_facts;
pub(crate) use raidish_agent::vm_check;
pub(crate) use raidish_auth::net;
pub(crate) use raidish_auth::pki;
pub(crate) use raidish_auth::tls;
pub(crate) use raidish_block::nbd;
pub(crate) use raidish_block::nvme;
pub use raidish_cluster as cluster;
pub(crate) use raidish_committer as committer;
pub(crate) use raidish_csi as csi;
pub(crate) use raidish_http::config;
pub(crate) use raidish_http::server;
pub(crate) use raidish_http::vms_api;
pub(crate) use raidish_services::frontend;
pub(crate) use raidish_vm::vm;
pub(crate) use raidish_vm::vm_store;
