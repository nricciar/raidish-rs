//! Identity and transport for RAIDish nodes and clients: the cluster PKI
//! (`pki`, offline; `ca` and `envelope`, the managed CA's X.509 and sealing), TLS setup and HTTP clients (`tls`), who a request is from and
//! what it may do (`auth`, request extractors), serving HTTP(S) with axum
//! (`http`), and the shared TCP accept loop (`net`).

pub(crate) use raidish_types::api_types;

pub mod auth;
pub mod ca;
pub mod envelope;
pub mod http;
pub mod net;
pub mod pki;
pub mod tls;

/// A throwaway PKI for tests (here and, with `testutil`, in other crates).
#[cfg(any(test, feature = "testutil"))]
pub mod testing;
