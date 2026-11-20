//! The cluster's private CA: everything `raidish cert …` does.
//!
//! A PKI directory holds:
//!
//! * `ca.key`: the CA's private key. Only `cert` commands need it, so it can
//!   live on an offline machine.
//! * `ca.pem`: the **trust bundle**: this CA's certificate, plus any other CA
//!   being rotated in or out (`cert trust`). Nodes and clients trust every
//!   CA in it.
//! * `<name>/{ca.pem,cert.pem,key.pem}`: one bundle per issued identity,
//!   ready to copy to a node or client as its `tls.dir`.
//!
//! Identities: Subject CN is the name and OU the role (`node`, `admin` or
//! `readonly`). Node certs are server and client certs and always cover
//! `localhost` and `127.0.0.1`, because a node calls itself. All keys are
//! ECDSA P-256, which browsers and rustls accept.
//!
//! `cert libvirt` issues a host's libvirt certificate (OU `libvirt`, which
//! is no raidish role: it opens nothing of raidish's) laid out the way
//! libvirtd and qemu read theirs, for TLS between nodes' libvirt and for
//! qemu's migration stream (`<dir>/libvirt/<name>/etc/pki/...`).
//!
//! Certificates don't expire by default: they get the RFC 5280 "no
//! well-defined expiration" date, 9999-12-31 23:59:59Z. Pass `days` to
//! choose a lifetime.

use ::time::OffsetDateTime;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use std::fmt;
use std::path::{Path, PathBuf};
use x509_parser::prelude::*;

pub use crate::api_types::Role;

/// What a PEM certificate says, for `cert status` and the auth layer.
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

fn err(e: impl fmt::Display) -> String {
    e.to_string()
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

fn to_pem(der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

/// Write `contents` to `path`; `secret` files get mode 0600. Refuses to
/// replace an existing file unless `force`.
fn write_file(path: &Path, contents: &str, secret: bool, force: bool) -> Result<(), String> {
    if path.exists() && !force {
        return Err(format!(
            "{} already exists (pass --force to replace it)",
            path.display()
        ));
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {}", dir.display(), e))?;
    }
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(if secret { 0o600 } else { 0o644 })
        .open(path)
        .map_err(|e| format!("{}: {}", path.display(), e))?;
    f.write_all(contents.as_bytes())
        .map_err(|e| format!("{}: {}", path.display(), e))
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {}", path.display(), e))
}

fn validity(params: &mut CertificateParams, days: Option<u32>) {
    let now = OffsetDateTime::now_utc();
    // A little slack for clocks that are slightly behind.
    params.not_before = now - ::time::Duration::hours(1);
    params.not_after = match days {
        Some(d) => now + ::time::Duration::days(d as i64),
        None => rcgen::date_time_ymd(9999, 12, 31) + ::time::Duration::seconds(86399),
    };
}

fn random_serial() -> Result<SerialNumber, String> {
    let mut bytes = [0u8; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
        .map_err(|_| "no randomness".to_string())?;
    bytes[0] &= 0x7f; // keep it positive
    Ok(SerialNumber::from_slice(&bytes))
}

/// The CA in a PKI directory, ready to sign.
pub struct Ca {
    dir: PathBuf,
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl Ca {
    pub fn bundle_path(dir: &Path) -> PathBuf {
        dir.join("ca.pem")
    }

    /// Create a new CA in `dir` (`ca.key` and the trust bundle `ca.pem`).
    pub fn init(
        dir: &Path,
        name: &str,
        days: Option<u32>,
        force: bool,
    ) -> Result<CertInfo, String> {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, name);
        dn.push(DnType::OrganizationName, "raidish");
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        params.serial_number = Some(random_serial()?);
        validity(&mut params, days);
        let cert = params.self_signed(&key).map_err(err)?;

        let key_path = dir.join("ca.key");
        if key_path.exists() && !force {
            return Err(format!(
                "{} already exists (pass --force to replace the CA; every certificate it issued \
                 stops being trusted once it leaves the bundle)",
                key_path.display()
            ));
        }
        write_file(&key_path, &key.serialize_pem(), true, true)?;
        write_file(&Self::bundle_path(dir), &cert.pem(), false, true)?;
        inspect_der(cert.der())
    }

    /// Load the CA from `dir`: `ca.key`, and the certificate in `ca.pem`
    /// whose public key matches it.
    pub fn load(dir: &Path) -> Result<Ca, String> {
        let key = KeyPair::from_pem(&read(&dir.join("ca.key"))?).map_err(err)?;
        let spki = key.public_key_der();
        let bundle = read(&Self::bundle_path(dir))?;
        for der in read_pem_certs(&bundle)? {
            let (_, parsed) = X509Certificate::from_der(&der).map_err(err)?;
            if parsed.public_key().raw == spki.as_slice() {
                let der = rustls::pki_types::CertificateDer::from(der);
                // Re-sign the CA's own params: children only reference its
                // name and key id, which are the same.
                let params = CertificateParams::from_ca_cert_der(&der).map_err(err)?;
                let cert = params.self_signed(&key).map_err(err)?;
                return Ok(Ca {
                    dir: dir.to_path_buf(),
                    cert,
                    key,
                });
            }
        }
        Err(format!(
            "no certificate in {} matches ca.key",
            Self::bundle_path(dir).display()
        ))
    }

    /// Issue a `role` certificate for `name` and write its bundle to
    /// `<dir>/<name>/`. Node certs also cover `sans` (plus `localhost`,
    /// `127.0.0.1` and `name` itself).
    pub fn issue(
        &self,
        name: &str,
        role: Role,
        sans: &[String],
        days: Option<u32>,
        force: bool,
    ) -> Result<CertInfo, String> {
        if name.is_empty() || name.contains('/') || name.starts_with('.') {
            return Err(format!("bad name '{}'", name));
        }
        let mut alt = Vec::new();
        if role == Role::Node {
            for s in std::iter::once(name)
                .chain(sans.iter().map(|s| s.as_str()))
                .chain(["localhost", "127.0.0.1"])
            {
                if !alt.iter().any(|a: &String| a == s) {
                    alt.push(s.to_string());
                }
            }
        }
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
        params.subject_alt_names = alt
            .iter()
            .map(|s| match s.parse::<std::net::IpAddr>() {
                Ok(ip) => Ok(SanType::IpAddress(ip)),
                Err(_) => s.as_str().try_into().map(SanType::DnsName).map_err(err),
            })
            .collect::<Result<_, String>>()?;
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, name);
        dn.push(DnType::OrganizationalUnitName, role.as_str());
        dn.push(DnType::OrganizationName, "raidish");
        params.distinguished_name = dn;
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = match role {
            Role::Node => vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
            _ => vec![ExtendedKeyUsagePurpose::ClientAuth],
        };
        params.use_authority_key_identifier_extension = true;
        params.serial_number = Some(random_serial()?);
        validity(&mut params, days);

        let out = self.dir.join(name);
        let cert_path = out.join("cert.pem");
        if cert_path.exists() && !force {
            return Err(format!(
                "{} already exists (pass --force to reissue)",
                cert_path.display()
            ));
        }
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
        let cert = params.signed_by(&key, &self.cert, &self.key).map_err(err)?;
        write_file(&out.join("key.pem"), &key.serialize_pem(), true, true)?;
        write_file(&cert_path, &cert.pem(), false, true)?;
        write_file(
            &out.join("ca.pem"),
            &read(&Self::bundle_path(&self.dir))?,
            false,
            true,
        )?;
        inspect_der(cert.der())
    }
}

/// Where `Ca::issue_libvirt` puts host `name`'s files, relative to the
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

impl Ca {
    /// Issue host `name`'s libvirt certificate: server and client (libvirtd
    /// takes connections and, migrating peer to peer, makes them; so does
    /// qemu's migration stream), for `name` and `sans`, written under
    /// `<dir>/libvirt/<name>/` as `LIBVIRT_FILES` lays out: copy its `etc`
    /// to `/` on that host.
    pub fn issue_libvirt(
        &self,
        name: &str,
        sans: &[String],
        days: Option<u32>,
        force: bool,
    ) -> Result<CertInfo, String> {
        if name.is_empty() || name.contains('/') || name.starts_with('.') {
            return Err(format!("bad name '{}'", name));
        }
        let out = self.dir.join("libvirt").join(name);
        let first = out.join(LIBVIRT_FILES[1].0);
        if first.exists() && !force {
            return Err(format!(
                "{} already exists (pass --force to reissue)",
                first.display()
            ));
        }
        let mut alt: Vec<String> = Vec::new();
        for s in std::iter::once(name).chain(sans.iter().map(|s| s.as_str())) {
            if !alt.iter().any(|a| a == s) {
                alt.push(s.to_string());
            }
        }
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
        params.subject_alt_names = alt
            .iter()
            .map(|s| match s.parse::<std::net::IpAddr>() {
                Ok(ip) => Ok(SanType::IpAddress(ip)),
                Err(_) => s.as_str().try_into().map(SanType::DnsName).map_err(err),
            })
            .collect::<Result<_, String>>()?;
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, name);
        dn.push(DnType::OrganizationalUnitName, "libvirt");
        dn.push(DnType::OrganizationName, "raidish");
        params.distinguished_name = dn;
        params.is_ca = IsCa::ExplicitNoCa;
        // gnutls (libvirt, qemu) wants KeyEncipherment on a server cert
        // even for ECDSA keys it can't use it with.
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.use_authority_key_identifier_extension = true;
        params.serial_number = Some(random_serial()?);
        validity(&mut params, days);
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
        let cert = params.signed_by(&key, &self.cert, &self.key).map_err(err)?;
        let ca = read(&Self::bundle_path(&self.dir))?;
        let (cert_pem, key_pem) = (cert.pem(), key.serialize_pem());
        for (path, secret) in LIBVIRT_FILES {
            let is_ca = path.ends_with("/cacert.pem") || path.ends_with("/ca-cert.pem");
            let body = match (*secret, is_ca) {
                (true, _) => &key_pem,
                (false, true) => &ca,
                (false, false) => &cert_pem,
            };
            write_file(&out.join(path), body, *secret, true)?;
        }
        // The tree is unpacked as is on the host: its directories must be
        // the 0755 /etc/pki's are, whatever this umask made them.
        #[cfg(unix)]
        for (path, _) in LIBVIRT_FILES {
            use std::os::unix::fs::PermissionsExt;
            let mut d = out.join(path);
            while d.pop() && d != out {
                std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o755))
                    .map_err(err)?;
            }
        }
        inspect_der(cert.der())
    }
}

/// Add the CAs in `pem` to `dir`'s trust bundle (and every issued bundle in
/// it). Returns the CAs added.
pub fn trust(dir: &Path, pem: &str) -> Result<Vec<CertInfo>, String> {
    let bundle_path = Ca::bundle_path(dir);
    let mut certs = read_pem_certs(&read(&bundle_path)?)?;
    let mut added = Vec::new();
    for der in read_pem_certs(pem)? {
        let info = inspect_der(&der)?;
        if !info.is_ca {
            return Err(format!("'{}' is not a CA certificate", info.name));
        }
        if !certs.contains(&der) {
            certs.push(der);
            added.push(info);
        }
    }
    write_bundle(dir, &certs)?;
    Ok(added)
}

/// Remove CAs (by fingerprint or name) from `dir`'s trust bundle. Refuses to
/// remove the CA that `ca.key` belongs to.
pub fn untrust(dir: &Path, which: &str) -> Result<Vec<CertInfo>, String> {
    let bundle_path = Ca::bundle_path(dir);
    let own = Ca::load(dir).ok().map(|ca| ca.key.public_key_der());
    let mut keep = Vec::new();
    let mut removed = Vec::new();
    for der in read_pem_certs(&read(&bundle_path)?)? {
        let info = inspect_der(&der)?;
        if info.fingerprint == which || info.name == which {
            let (_, parsed) = X509Certificate::from_der(&der).map_err(err)?;
            if own.as_deref() == Some(parsed.public_key().raw) {
                return Err(format!(
                    "'{}' is this directory's own CA (ca.key); it can't be untrusted here",
                    info.name
                ));
            }
            removed.push(info);
        } else {
            keep.push(der);
        }
    }
    if removed.is_empty() {
        return Err(format!("no CA '{}' in {}", which, bundle_path.display()));
    }
    write_bundle(dir, &keep)?;
    Ok(removed)
}

/// Rewrite `ca.pem` and every issued bundle's copy of it.
fn write_bundle(dir: &Path, certs: &[Vec<u8>]) -> Result<(), String> {
    let pem: String = certs.iter().map(|d| to_pem(d)).collect();
    write_file(&Ca::bundle_path(dir), &pem, false, true)?;
    for entry in std::fs::read_dir(dir).map_err(err)? {
        let p = entry.map_err(err)?.path();
        if p.is_dir() && p.join("cert.pem").exists() {
            write_file(&p.join("ca.pem"), &pem, false, true)?;
        }
    }
    Ok(())
}

/// Every certificate in a PKI directory: the trust bundle, then each
/// issued identity, by name.
pub fn status(dir: &Path) -> Result<(Vec<CertInfo>, Vec<CertInfo>), String> {
    let cas = read_pem_certs(&read(&Ca::bundle_path(dir))?)?
        .iter()
        .map(|d| inspect_der(d))
        .collect::<Result<Vec<_>, _>>()?;
    let mut issued = Vec::new();
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(err)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("cert.pem").exists())
        .collect();
    dirs.sort();
    for p in dirs {
        for der in read_pem_certs(&read(&p.join("cert.pem"))?)? {
            issued.push(inspect_der(&der)?);
        }
    }
    Ok((cas, issued))
}

/// The hosts of a config's remote disk URLs, in pool order: the names
/// `cert issue-nodes` issues node certs for.
pub fn hosts_in(paths: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in paths {
        let Some(base) = crate::config::disk_server_base(p) else {
            continue;
        };
        let Ok(url) = url::Url::parse(&base) else {
            continue;
        };
        if let Some(h) = url.host_str() {
            let h = h.trim_start_matches('[').trim_end_matches(']').to_string();
            if !out.contains(&h) {
                out.push(h);
            }
        }
    }
    out
}

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
