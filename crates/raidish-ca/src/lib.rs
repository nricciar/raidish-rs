//! The cluster's certificate authority, as a service of every `listen` node
//! (see `docs/ca-service-plan.md`).
//!
//! * [`init`]: `raidish ca init` on node 0, before anything runs: the root,
//!   the issuer, the key slot, node 0's and the operator's certificates and
//!   one join token per peer, all **staged** in node 0's data dir
//!   (`pki_bootstrap.json`), since there is no Raft yet to put them in.
//! * [`CaService`]: what a node does with it. Staged, it answers from (and
//!   records into) the staged state until the cluster has a leader, then
//!   proposes it as `PkiOp::Bootstrap` and deletes the file. After that the
//!   CA lives in Raft (`ClusterState::pki`). A node holding the cluster DEK
//!   (unsealed) signs; a sealed one passes token-authenticated requests on.
//! * [`join`]: `raidish join`, the client side of `POST /api/v1/pki/join`:
//!   it pins the root by the fingerprint in the token before sending it.
//! * [`routes`]: the `/api/v1/pki/*` endpoints.

pub mod api;
pub mod crl;
pub mod init;
pub mod join;
pub mod loud;
pub mod renew;
mod revoke;
pub mod routes;
mod seal;
mod service;
pub mod token;
pub mod token_signer;

pub use loud::CaEvents;
pub use renew::NodeIdentity;
pub use seal::NodeKeySource;
pub use service::{CaError, CaOptions, CaService};

use std::path::{Path, PathBuf};

/// Node 0's staged CA, before the first quorum (in its Raft data dir).
pub const BOOTSTRAP_FILE: &str = raidish_cluster::store::CA_STAGED_FILE;

pub fn bootstrap_path(data_dir: &Path) -> PathBuf {
    data_dir.join(BOOTSTRAP_FILE)
}

/// The one-shot DEK `ca init` leaves for node 0's first start (hex), so
/// the documented flow needs no `ca unseal`. Read and removed at startup.
pub fn unseal_once_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(raidish_cluster::store::CA_DIR)
        .join("unseal-once")
}

/// Write `bytes` to `path` all at once (temp file and rename), with
/// `mode`, making its directory if need be.
pub fn write_file(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let say = |e: std::io::Error| format!("{}: {}", path.display(), e);
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(say)?;
    }
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(&tmp)
        .map_err(say)?;
    f.write_all(bytes).map_err(say)?;
    f.sync_all().map_err(say)?;
    drop(f);
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode)).map_err(say)?;
    std::fs::rename(&tmp, path).map_err(say)
}

/// A certificate bundle as `tls.dir` holds it.
pub struct Bundle {
    /// The root (what to trust).
    pub ca_pem: String,
    /// The leaf, then its issuer.
    pub cert_pem: String,
    pub key_pem: zeroize::Zeroizing<String>,
}

impl Bundle {
    /// Write `ca.pem`, `cert.pem` and `key.pem` (`0600`) to the given paths.
    pub fn write(&self, ca: &Path, cert: &Path, key: &Path) -> Result<(), String> {
        write_file(key, self.key_pem.as_bytes(), 0o600)?;
        write_file(cert, self.cert_pem.as_bytes(), 0o644)?;
        write_file(ca, self.ca_pem.as_bytes(), 0o644)
    }

    /// Write the bundle into `dir` (`ca.pem`, `cert.pem`, `key.pem`).
    pub fn write_dir(&self, dir: &Path) -> Result<(), String> {
        self.write(
            &dir.join("ca.pem"),
            &dir.join("cert.pem"),
            &dir.join("key.pem"),
        )
    }
}

/// Node certificates' names: a host and what every node is also reached
/// by on itself, without repeats.
pub fn node_sans(hosts: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for h in hosts
        .iter()
        .map(String::as_str)
        .chain(["localhost", "127.0.0.1"])
    {
        let h = h.trim().to_ascii_lowercase();
        if !h.is_empty() && !out.contains(&h) {
            out.push(h);
        }
    }
    out
}

/// The host of a base URL (`https://node1:8881` → `node1`).
pub fn url_host(base: &str) -> Option<String> {
    let u = url::Url::parse(base).ok()?;
    let h = u.host_str()?;
    Some(
        h.trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase(),
    )
}

/// A CRL's DER as PEM.
pub fn crl_pem(der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::from("-----BEGIN X509 CRL-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str("-----END X509 CRL-----\n");
    out
}

pub(crate) fn now_ms() -> u64 {
    raidish_types::types::now_ms()
}

#[cfg(test)]
#[path = "tests/cluster_tests.rs"]
mod cluster_tests;
