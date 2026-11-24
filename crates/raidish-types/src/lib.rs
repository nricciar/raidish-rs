//! RAIDish's plain data: the identifiers and names (`types`), VM
//! definitions (`vm_def`), what the HTTP API sends and takes
//! (`api_types`) and the cluster's replicated state (`cluster_state`).
//!
//! No I/O and no server dependencies: the storage core, the servers and
//! the web dashboard (wasm32) all build on it.

pub mod api_types;
pub mod cluster_state;
pub mod types;
pub mod vm_def;
