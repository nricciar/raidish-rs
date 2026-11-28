//! TLS for everything a process says over HTTP(S) and websockets.
//!
//! ```yaml
//! tls:
//!   dir: /etc/raidish/tls     # ca.pem, cert.pem, key.pem (from `raidish join`)
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
    /// The node's live revocations (not configured: `listen` sets it), so
    /// every verifier made from this config refuses what's revoked.
    #[serde(skip)]
    pub trust: Option<Arc<crate::trust::Trust>>,
    /// The identity as it is now (not configured: [`init`] makes it from
    /// the files), so every acceptor and connector made from this config
    /// follows a renewal (CA-4a). None: the files, as read.
    #[serde(skip)]
    pub live: Option<Arc<crate::http::LiveIdentity>>,
}

/// `p`, with a leading `~/` as `$HOME/`.
pub fn expand(p: &str) -> PathBuf {
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
        let server_verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("tls: {}", e))?;
        let builder = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(CheckedServer {
                inner: server_verifier,
                trust: self.trust.clone(),
            }));
        if let Some(live) = &self.live {
            return Ok(builder.with_client_cert_resolver(live.clone()));
        }
        match self.identity()? {
            Some((chain, key)) => builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("tls: {}", e)),
            None => Ok(builder.with_no_client_auth()),
        }
    }
}

impl TlsConfig {
    /// A rustls server config with this process's certificate: clients
    /// must (`require_client`) or may present a certificate chaining to
    /// the bundle. `alpn`: the protocols offered.
    pub fn server_config(
        &self,
        require_client: bool,
        alpn: &[&[u8]],
    ) -> Result<rustls::ServerConfig, String> {
        install_provider();
        let mut roots = rustls::RootCertStore::empty();
        for c in self.roots()? {
            roots.add(c).map_err(|e| format!("tls: bad CA: {}", e))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
        let verifier = if require_client {
            verifier.build()
        } else {
            verifier.allow_unauthenticated().build()
        }
        .map_err(|e| format!("tls: {}", e))?;
        let verifier = Arc::new(CheckedClient {
            inner: verifier,
            trust: self.trust.clone(),
        });
        let b = rustls::ServerConfig::builder().with_client_cert_verifier(verifier);
        let mut c = match &self.live {
            Some(live) => b.with_cert_resolver(live.clone()),
            None => {
                let (chain, key) = self
                    .identity()?
                    .ok_or("tls: serving needs this node's cert.pem and key.pem")?;
                b.with_single_cert(chain, key)
                    .map_err(|e| format!("tls: {}", e))?
            }
        };
        c.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        Ok(c)
    }
}

// ---- revocation, on top of the chain checks (see `crate::trust`) ------------

/// What the process's own client config ([`init`]) checks servers against:
/// set by `listen` once the node has its [`crate::trust::Trust`] (the
/// client config is made before, and is one per process anyway).
static CLIENT_TRUST: OnceLock<Arc<crate::trust::Trust>> = OnceLock::new();

/// Refuse revoked servers from the process's client config from now on.
/// Only the first call counts (one node per process).
pub fn check_servers_against(trust: Arc<crate::trust::Trust>) {
    let _ = CLIENT_TRUST.set(trust);
}

/// `der` (chain already checked) against `trust`, as a TLS error.
pub fn check_revoked(
    trust: Option<&crate::trust::Trust>,
    der: &CertificateDer<'_>,
) -> Result<(), rustls::Error> {
    match trust {
        Some(t) => t
            .check_leaf(der)
            .map_err(|_| rustls::Error::InvalidCertificate(rustls::CertificateError::Revoked)),
        None => Ok(()),
    }
}

/// A client verifier that also refuses what's revoked.
#[derive(Debug)]
struct CheckedClient {
    inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    trust: Option<Arc<crate::trust::Trust>>,
}

impl rustls::server::danger::ClientCertVerifier for CheckedClient {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }
    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        let ok = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        check_revoked(self.trust.as_deref(), end_entity)?;
        Ok(ok)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// A server verifier that also refuses what's revoked: its config's
/// trust, else the process's ([`check_servers_against`]).
#[derive(Debug)]
struct CheckedServer {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    trust: Option<Arc<crate::trust::Trust>>,
}

impl rustls::client::danger::ServerCertVerifier for CheckedServer {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let ok =
            self.inner
                .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)?;
        check_revoked(
            self.trust.as_deref().or(CLIENT_TRUST.get().map(|t| &**t)),
            end_entity,
        )?;
        Ok(ok)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Use ring for rustls (the one crypto backend we build).
pub fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

static CLIENT: OnceLock<Option<Arc<rustls::ClientConfig>>> = OnceLock::new();
static IDENTITY: OnceLock<Arc<crate::http::LiveIdentity>> = OnceLock::new();

/// Set the process's TLS client config (once, at startup). `None`: plain
/// HTTP. Disk URLs must use `https://` with TLS and `http://` without.
/// With a certificate, it's presented through the process's
/// [`identity`], so a renewal reaches this client too.
pub fn init(cfg: Option<&TlsConfig>) -> Result<(), String> {
    let client = match cfg {
        Some(c) => {
            let mut c = c.clone();
            if let (Some(cert), Some(key)) = (c.cert_path(), c.key_path())
                && let Some(what) = finish_swap(&cert, &key)
            {
                eprintln!("tls: {}", what);
            }
            if c.live.is_none()
                && let Some((chain, key)) = c.identity()?
            {
                let live = crate::http::LiveIdentity::new(chain, key)?;
                let _ = IDENTITY.set(live.clone());
                c.live = Some(live);
            }
            Some(Arc::new(c.client_config()?))
        }
        None => None,
    };
    CLIENT
        .set(client)
        .map_err(|_| "tls: already initialised".to_string())
}

fn new_path(p: &std::path::Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(".new");
    PathBuf::from(s)
}

fn tmp_path(p: &std::path::Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// `path` whole or not at all (a `.tmp` beside it, renamed).
fn write_new(path: &std::path::Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let tmp = tmp_path(path);
    let say = |e: std::io::Error| format!("{}: {}", tmp.display(), e);
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(&tmp)
        .map_err(say)?;
    f.write_all(bytes).map_err(say)?;
    f.sync_all().map_err(say)?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode)).map_err(say)?;
    std::fs::rename(&tmp, path).map_err(say)
}

fn sync_dir(path: &std::path::Path) {
    if let Some(d) = path.parent().filter(|d| !d.as_os_str().is_empty())
        && let Ok(f) = std::fs::File::open(d)
    {
        let _ = f.sync_all();
    }
}

/// Replace the certificate and key files together (a renewal). Two files
/// can't be renamed at once, so: both written as `.new` (each there only
/// once whole), the key's first, then the key renamed, then the
/// certificate. [`finish_swap`] completes (or undoes)
/// one cut short, so a crash never leaves a key with another's
/// certificate.
pub fn swap_identity(
    cert: &std::path::Path,
    key: &std::path::Path,
    cert_pem: &str,
    key_pem: &str,
) -> Result<(), String> {
    let (cert_new, key_new) = (new_path(cert), new_path(key));
    write_new(&key_new, key_pem.as_bytes(), 0o600)?;
    write_new(&cert_new, cert_pem.as_bytes(), 0o644)?;
    sync_dir(cert);
    std::fs::rename(&key_new, key).map_err(|e| format!("{}: {}", key.display(), e))?;
    sync_dir(key);
    std::fs::rename(&cert_new, cert).map_err(|e| format!("{}: {}", cert.display(), e))?;
    sync_dir(cert);
    Ok(())
}

/// Finish a [`swap_identity`] a crash cut short: both `.new` there, both
/// go in; the certificate's alone (the key went in), it goes in; the key's
/// alone (the certificate never got written), it's dropped. What it did.
pub fn finish_swap(cert: &std::path::Path, key: &std::path::Path) -> Option<String> {
    let (cert_new, key_new) = (new_path(cert), new_path(key));
    // Half-written: never one of the pair.
    let _ = std::fs::remove_file(tmp_path(&key_new));
    let _ = std::fs::remove_file(tmp_path(&cert_new));
    match (key_new.exists(), cert_new.exists()) {
        (true, true) => {
            std::fs::rename(&key_new, key).ok()?;
            std::fs::rename(&cert_new, cert).ok()?;
            Some(format!("{}: finished a renewal cut short", cert.display()))
        }
        (false, true) => {
            std::fs::rename(&cert_new, cert).ok()?;
            Some(format!("{}: finished a renewal cut short", cert.display()))
        }
        (true, false) => {
            std::fs::remove_file(&key_new).ok()?;
            Some(format!(
                "{}: dropped a renewal cut short",
                key_new.display()
            ))
        }
        (false, false) => None,
    }
}

/// The process's own certificate, live (set by [`init`] when it has one):
/// `listen` serves it and renews it in place.
pub fn identity() -> Option<Arc<crate::http::LiveIdentity>> {
    IDENTITY.get().cloned()
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
fn http_builder() -> reqwest::ClientBuilder {
    let b = reqwest::Client::builder();
    match client() {
        Some(c) => b.use_preconfigured_tls((*c).clone()),
        None => b,
    }
}

/// How long any API client waits to connect to a node.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// An HTTP client for talking to `listen` nodes (this process's TLS
/// identity, if it has one): `CONNECT_TIMEOUT` to connect, and `timeout`
/// for the whole of a request (None: as long as it takes, for a stream).
pub fn http_client(timeout: Option<std::time::Duration>) -> Result<reqwest::Client, String> {
    let b = http_builder().connect_timeout(CONNECT_TIMEOUT);
    match timeout {
        Some(t) => b.timeout(t),
        None => b,
    }
    .build()
    .map_err(|e| format!("HTTP client: {}", e))
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
        let Some(base) = crate::net::disk_server_base(u) else {
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

/// The role of a TLS connection's client certificate.
pub fn peer_role<S>(tls: &tokio_rustls::server::TlsStream<S>) -> Option<crate::pki::Role> {
    let der = tls.get_ref().1.peer_certificates()?.first()?;
    crate::pki::inspect_der(der).ok()?.role
}

#[cfg(test)]
mod swap_tests {
    use super::*;

    fn read(p: &std::path::Path) -> String {
        std::fs::read_to_string(p).unwrap()
    }

    #[test]
    fn a_swap_cut_short_is_finished_or_dropped_never_mixed() {
        let d = tempfile::tempdir().unwrap();
        let (cert, key) = (d.path().join("cert.pem"), d.path().join("key.pem"));
        swap_identity(&cert, &key, "c1", "k1").unwrap();
        assert_eq!((read(&cert), read(&key)), ("c1".into(), "k1".into()));
        assert!(finish_swap(&cert, &key).is_none());
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&key).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        // Both written, neither in: both go in.
        std::fs::write(new_path(&key), "k2").unwrap();
        std::fs::write(new_path(&cert), "c2").unwrap();
        assert!(finish_swap(&cert, &key).is_some());
        assert_eq!((read(&cert), read(&key)), ("c2".into(), "k2".into()));
        // The key in, the certificate not yet: it goes in.
        std::fs::write(&key, "k3").unwrap();
        std::fs::write(new_path(&cert), "c3").unwrap();
        assert!(finish_swap(&cert, &key).is_some());
        assert_eq!((read(&cert), read(&key)), ("c3".into(), "k3".into()));
        // Only the new key written: dropped, the pair as it was.
        std::fs::write(new_path(&key), "k4").unwrap();
        assert!(finish_swap(&cert, &key).is_some());
        assert_eq!((read(&cert), read(&key)), ("c3".into(), "k3".into()));
        assert!(!new_path(&key).exists() && !new_path(&cert).exists());
    }
}
