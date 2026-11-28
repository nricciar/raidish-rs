//! RAIDish's plain data: the identifiers and names (`types`), VM
//! definitions (`vm_def`), what the HTTP API sends and takes
//! (`api_types`), the cluster's replicated state (`cluster_state`) and
//! the cluster CA's part of it (`pki`).
//!
//! No I/O and no server dependencies: the storage core, the servers and
//! the web dashboard (wasm32) all build on it.

pub mod api_types;
pub mod cluster_state;
pub mod pki;
pub mod types;
pub mod vm_def;
