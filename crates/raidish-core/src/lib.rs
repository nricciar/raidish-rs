//! The RAIDish storage core: disks (local, remote over the websocket block
//! protocol, and in the browser), raidz/mirror vdevs, the copy-on-write
//! object store, the filesystem, volumes, snapshots, send/receive, scrub,
//! fsck and inspection.
//!
//! Builds natively and for `wasm32-unknown-unknown` (the web dashboard
//! reads a pool through it).

// The plain data, where the storage code has always found it.
pub use raidish_types::{api_types, types, vm_def};

pub mod blkptr;
pub mod btree;
pub mod coords;
pub mod dataset;
pub mod disk;
pub mod disks;
pub mod fence;
pub mod file_writer;
pub mod fs;
pub mod fsck;
pub mod inode;
pub mod inspect;
pub mod kv;
pub mod metaslab;
pub mod mirror;
pub mod object;
pub mod open_file;
pub mod raidz;
pub mod role;
pub mod rt;
pub mod scan;
#[cfg(not(target_arch = "wasm32"))]
pub mod send;
#[cfg(not(target_arch = "wasm32"))]
pub mod service_tree;
pub mod snapshot;
pub mod stripe;
pub mod trace;
pub mod tx;
#[cfg(not(target_arch = "wasm32"))]
pub mod ui;
pub mod vdev;
pub mod volume;
pub mod websocket;
pub mod writer;
pub mod zil;

/// Test fixtures (pools on sparse files, fault injection) for this
/// crate's tests and, with the `testutil` feature, other crates'.
#[cfg(any(test, feature = "testutil"))]
#[path = "tests/util.rs"]
pub mod test_util;
