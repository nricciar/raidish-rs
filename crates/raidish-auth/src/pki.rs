//! Reading certificates: what a certificate says ([`CertInfo`]), for the
//! auth layer and `ca status`, and where a host's libvirt files go
//! ([`LIBVIRT_FILES`], for `ca issue --profile libvirt`).
//!
//! Identities: Subject CN is the name and OU the role (`node`, `admin` or
//! `readonly`). The cluster's CA issues them (`crate::ca`, run by
//! `raidish-ca`); tests use an offline one ([`Ca`], with `testutil`).

use ::time::OffsetDateTime;
use x509_parser::prelude::*;

pub use crate::api_types::Role;
#[cfg(any(test, feature = "testutil"))]
pub use crate::fixture_ca::Ca;

/// What a PEM certificate says, for `ca status` and the auth layer.
#[derive(Debug, Clone)]
pub struct CertInfo {
    pub name: String,
    pub role: Option<Role>,
    /// The subject's OU as written (a role, or `libvirt`).
    pub ou: Option<String>,
    pub is_ca: bool,
    pub sans: Vec<String>,
    pub issuer: String,
    pub not_after: OffsetDateTime,
    /// blake3 of the DER, first 16 hex digits.
    pub fingerprint: String,
    pub der: Vec<u8>,
}

impl CertInfo {
    /// Expires at the RFC 5280 "never" date (or later than year 9000).
    pub fn never_expires(&self) -> bool {
        self.not_after.year() >= 9000
    }

    pub fn expiry(&self) -> String {
        if self.never_expires() {
            "never".into()
        } else {
            format!("{}", self.not_after.date())
        }
    }
}

/// Parse one DER certificate.
pub fn inspect_der(der: &[u8]) -> Result<CertInfo, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("bad certificate: {}", e))?;
    let subject = cert.subject();
    let name = subject
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .unwrap_or("")
        .to_string();
    let ou = subject
        .iter_organizational_unit()
        .next()
        .and_then(|a| a.as_str().ok())
        .map(|s| s.to_string());
    let role = ou.as_deref().and_then(|s| s.parse().ok());
    let is_ca = cert
        .basic_constraints()
        .ok()
        .flatten()
        .map(|b| b.value.ca)
        .unwrap_or(false);
    let mut sans = Vec::new();
    if let Ok(Some(ext)) = cert.subject_alternative_name() {
        for n in &ext.value.general_names {
            match n {
                GeneralName::DNSName(d) => sans.push(d.to_string()),
                GeneralName::IPAddress(ip) => sans.push(match ip.len() {
                    4 => std::net::IpAddr::from(<[u8; 4]>::try_from(*ip).unwrap()).to_string(),
                    16 => std::net::IpAddr::from(<[u8; 16]>::try_from(*ip).unwrap()).to_string(),
                    _ => format!("{:?}", ip),
                }),
                other => sans.push(format!("{:?}", other)),
            }
        }
    }
    let issuer = cert
        .issuer()
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .unwrap_or("")
        .to_string();
    Ok(CertInfo {
        name,
        role,
        ou,
        is_ca,
        sans,
        issuer,
        not_after: cert.validity().not_after.to_datetime(),
        fingerprint: blake3::hash(der).to_hex()[..16].to_string(),
        der: der.to_vec(),
    })
}

/// Every certificate in a PEM file.
pub fn read_pem_certs(pem: &str) -> Result<Vec<Vec<u8>>, String> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .map(|c| c.map(|c| c.to_vec()).map_err(|e| format!("bad PEM: {}", e)))
        .collect()
}

/// Where a host's libvirt certificate goes (`ca issue --profile libvirt`;
/// the fixture's `Ca::issue_libvirt`): host `name`'s files, relative to the
/// directory it writes (`<pki>/libvirt/<name>/`), and whether each is a
/// key. The tree under it goes to `/` on that host.
pub const LIBVIRT_FILES: &[(&str, bool)] = &[
    ("etc/pki/CA/cacert.pem", false),
    ("etc/pki/libvirt/servercert.pem", false),
    ("etc/pki/libvirt/private/serverkey.pem", true),
    ("etc/pki/libvirt/clientcert.pem", false),
    ("etc/pki/libvirt/private/clientkey.pem", true),
    ("etc/pki/qemu/ca-cert.pem", false),
    ("etc/pki/qemu/server-cert.pem", false),
    ("etc/pki/qemu/server-key.pem", true),
    ("etc/pki/qemu/client-cert.pem", false),
    ("etc/pki/qemu/client-key.pem", true),
];

/// A token's lifetime: `90d`, `12h`, `30m`, `45s` or plain seconds.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let (num, mult) = match s.chars().last() {
        Some('d') => (&s[..s.len() - 1], 86400),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('s') => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    num.parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(|| format!("bad duration '{}' (try 90d, 12h, 30m)", s))
}

#[cfg(test)]
#[path = "tests/pki_tests.rs"]
mod tests;
