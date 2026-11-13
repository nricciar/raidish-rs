//! TLS for everything a process says over HTTP(S) and websockets.
//!
//! ```yaml
//! tls:
//!   dir: /etc/raidish/tls     # ca.pem, cert.pem, key.pem (a `cert issue` bundle)
//!   # or name the files:  ca: …  cert: …  key: …
//! ```
//!
//! With a `tls:` section, a `listen` node serves HTTPS only and requires a
//! client certificate or a token on every request (see `crate::auth`), and
//! every client in the process (Raft, leases, the committer, remote disks)
//! verifies servers against `ca.pem` and presents `cert.pem`. Without one,
//! nothing changes: plain HTTP, no authentication.
//!
//! The client side is one process-wide rustls config, set by [`init`] at
//! startup, so the many places that open connections need no plumbing.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

#[derive(Deserialize, Debug, Clone, Default)]
pub struct TlsConfig {
    /// A directory holding `ca.pem`, `cert.pem` and `key.pem`.
    pub dir: Option<String>,
    /// The trust bundle (overrides `dir/ca.pem`).
    pub ca: Option<String>,
    /// This process's certificate (overrides `dir/cert.pem`).
    pub cert: Option<String>,
    /// Its private key (overrides `dir/key.pem`).
    pub key: Option<String>,
}

fn expand(p: &str) -> PathBuf {
    match (p.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => PathBuf::from(home).join(rest),
        _ => PathBuf::from(p),
    }
}

impl TlsConfig {
    fn path(&self, explicit: &Option<String>, file: &str) -> Option<PathBuf> {
        match (explicit, &self.dir) {
            (Some(p), _) => Some(expand(p)),
            (None, Some(d)) => Some(expand(d).join(file)),
            _ => None,
        }
    }
    pub fn ca_path(&self) -> Option<PathBuf> {
        self.path(&self.ca, "ca.pem")
    }
    pub fn cert_path(&self) -> Option<PathBuf> {
        self.path(&self.cert, "cert.pem")
    }
    pub fn key_path(&self) -> Option<PathBuf> {
        self.path(&self.key, "key.pem")
    }

    /// The trusted CAs (DER).
    pub fn roots(&self) -> Result<Vec<CertificateDer<'static>>, String> {
        let p = self
            .ca_path()
            .ok_or("tls: set `dir` or `ca` (the CA bundle)")?;
        let certs: Vec<_> = CertificateDer::pem_file_iter(&p)
            .map_err(|e| format!("{}: {}", p.display(), e))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("{}: {}", p.display(), e))?;
        if certs.is_empty() {
            return Err(format!("{}: no certificates", p.display()));
        }
        Ok(certs)
    }

    /// This process's certificate chain and key, if it has one (a client
    /// may have just a CA bundle; it then can't authenticate with a cert).
    pub fn identity(
        &self,
    ) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>, String> {
        let (Some(cert), Some(key)) = (self.cert_path(), self.key_path()) else {
            return Ok(None);
        };
        if self.cert.is_none() && !cert.exists() {
            return Ok(None);
        }
        let chain: Vec<_> = CertificateDer::pem_file_iter(&cert)
            .map_err(|e| format!("{}: {}", cert.display(), e))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("{}: {}", cert.display(), e))?;
        let key =
            PrivateKeyDer::from_pem_file(&key).map_err(|e| format!("{}: {}", key.display(), e))?;
        Ok(Some((chain, key)))
    }

    /// A rustls client config: trust the bundle, present the identity.
    pub fn client_config(&self) -> Result<rustls::ClientConfig, String> {
        install_provider();
        let mut roots = rustls::RootCertStore::empty();
        for c in self.roots()? {
            roots.add(c).map_err(|e| format!("tls: bad CA: {}", e))?;
        }
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        match self.identity()? {
            Some((chain, key)) => builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("tls: {}", e)),
            None => Ok(builder.with_no_client_auth()),
        }
    }
}

/// Use ring for rustls (the one crypto backend we build).
pub fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

static CLIENT: OnceLock<Option<Arc<rustls::ClientConfig>>> = OnceLock::new();

/// Set the process's TLS client config (once, at startup). `None`: plain
/// HTTP. Disk URLs must use `https://` with TLS and `http://` without.
pub fn init(cfg: Option<&TlsConfig>) -> Result<(), String> {
    let client = match cfg {
        Some(c) => Some(Arc::new(c.client_config()?)),
        None => None,
    };
    CLIENT
        .set(client)
        .map_err(|_| "tls: already initialised".to_string())
}

/// The process's client config, None without TLS.
pub fn client() -> Option<Arc<rustls::ClientConfig>> {
    CLIENT.get().cloned().flatten()
}

pub fn enabled() -> bool {
    client().is_some()
}

/// `https` with TLS, else `http`.
pub fn scheme() -> &'static str {
    if enabled() { "https" } else { "http" }
}

/// A reqwest client builder that speaks the process's TLS.
pub fn http_builder() -> reqwest::ClientBuilder {
    let b = reqwest::Client::builder();
    match client() {
        Some(c) => b.use_preconfigured_tls((*c).clone()),
        None => b,
    }
}

/// The websocket connector for `connect_async_tls_with_config`.
pub fn ws_connector() -> Option<tokio_tungstenite::Connector> {
    client().map(tokio_tungstenite::Connector::Rustls)
}

/// Check that `urls` match the TLS setting (https with TLS, http without),
/// so a half-converted config fails early with a clear message.
pub fn check_urls<'a>(urls: impl IntoIterator<Item = &'a String>) -> Result<(), String> {
    let want = scheme();
    for u in urls {
        let Some(base) = crate::config::disk_server_base(u) else {
            continue;
        };
        if !base.starts_with(&format!("{}://", want)) {
            return Err(if enabled() {
                format!("tls is configured, so '{}' must use https://", u)
            } else {
                format!("'{}' uses https:// but the config has no `tls:` section", u)
            });
        }
    }
    Ok(())
}
