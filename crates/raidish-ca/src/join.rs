//! `raidish join`: get this node's certificate with a join token.
//!
//! The joiner has no CA bundle yet. It fetches `/api/v1/pki/ca.pem` without
//! verifying the server, and keeps it only if its SHA-256 is the
//! fingerprint in the token; then it talks to the node with ordinary TLS
//! verification against that root. A wrong or spoofed node fails before
//! the token is sent.

use crate::Bundle;
use crate::api::{Issued, JoinRequest};
use crate::token::JoinToken;
use raidish_auth::ca::{fingerprint, pem_certs};
use raidish_auth::envelope::ct_eq;
use raidish_types::types::NodeId;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Accepts any server certificate (still checking the handshake was signed
/// by its key), and keeps it to look at.
#[derive(Debug)]
struct Unverified {
    seen: Mutex<Option<Vec<u8>>>,
    algs: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        *self.seen.lock().unwrap() = Some(end_entity.to_vec());
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    raidish_auth::tls::install_provider();
    rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
}

fn client(tls: rustls::ClientConfig) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .connect_timeout(raidish_auth::tls::CONNECT_TIMEOUT)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())
}

/// The root at `base`, checked against the token's fingerprint, and the
/// server's own certificate as it presented it.
pub async fn pinned_root(base: &str, token: &JoinToken) -> Result<(String, Vec<u8>), String> {
    if !base.starts_with("https://") {
        return Err(format!("{}: joining takes https://", base));
    }
    let verifier = Arc::new(Unverified {
        seen: Mutex::new(None),
        algs: provider().signature_verification_algorithms,
    });
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    let pem = client(tls)?
        .get(format!("{}/api/v1/pki/ca.pem", base))
        .send()
        .await
        .map_err(|e| format!("{}: {}", base, e))?
        .error_for_status()
        .map_err(|e| format!("{}: {}", base, e))?
        .text()
        .await
        .map_err(|e| format!("{}: {}", base, e))?;
    let certs = pem_certs(&pem).map_err(|e| format!("{}: its CA: {}", base, e))?;
    let [root] = &certs[..] else {
        return Err(format!("{}: expected one root certificate", base));
    };
    if !ct_eq(&fingerprint(root).0, &token.root.0) {
        return Err(format!(
            "{} serves another CA (root {}) than the token names ({}): not sending the token. \
             Wrong address, an old token, or someone in between.",
            base,
            fingerprint(root),
            token.root
        ));
    }
    let seen = verifier
        .seen
        .lock()
        .unwrap()
        .take()
        .ok_or("no server certificate")?;
    Ok((pem, seen))
}

/// What `join` got.
pub struct Joined {
    pub bundle: Bundle,
    pub serial: raidish_types::pki::Serial,
    pub not_after_ms: u64,
}

/// Join the cluster at `base` as `member` (this node's id and layout
/// fingerprint, from its own config): a new key, signed. Nothing is
/// written; the caller puts the bundle in `tls.dir`.
pub async fn join(
    base: &str,
    token: &JoinToken,
    member: NodeId,
    layout_fingerprint: &str,
) -> Result<Joined, String> {
    let base = base.trim_end_matches('/');
    let (ca_pem, server_cert) = pinned_root(base, token).await?;
    // Say plainly if the address won't pass verification, rather than as
    // a TLS error.
    let host = crate::url_host(base).ok_or_else(|| format!("{}: no host", base))?;
    let info = raidish_auth::pki::inspect_der(&server_cert)?;
    if !info.sans.iter().any(|s| s.eq_ignore_ascii_case(&host)) {
        return Err(format!(
            "{}'s certificate is for {} — not '{}'. Join with one of those addresses, or \
             reissue node 0's with `raidish ca init --san {}`.",
            base,
            info.sans.join(", "),
            host,
            host
        ));
    }
    let mut roots = rustls::RootCertStore::empty();
    for c in pem_certs(&ca_pem)? {
        roots.add(c).map_err(|e| e.to_string())?;
    }
    let _ = provider();
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let (key_pem, csr_pem) = raidish_auth::ca::generate_csr(&member.name())?;
    let req = JoinRequest {
        id: token.id.clone(),
        secret: token.secret_b64().to_string(),
        member,
        fingerprint: layout_fingerprint.to_string(),
        csr_pem,
    };
    let resp = client(tls)?
        .post(format!("{}/api/v1/pki/join", base))
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("{}: {}", base, e))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("{}: {}: {}", base, status, text.trim()));
    }
    let issued: Issued = resp.json().await.map_err(|e| format!("{}: {}", base, e))?;
    if issued.ca_pem.trim() != ca_pem.trim() {
        return Err("the answer names another root than the pinned one".into());
    }
    Ok(Joined {
        bundle: Bundle {
            ca_pem,
            cert_pem: issued.cert_pem,
            key_pem,
        },
        serial: issued.serial,
        not_after_ms: issued.not_after_ms,
    })
}
