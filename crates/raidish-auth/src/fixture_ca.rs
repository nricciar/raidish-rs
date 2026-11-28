//! The old offline CA, kept as a test fixture: a CA in a directory
//! that signs node, admin and libvirt bundles with no cluster behind it.
//! The cluster's own CA is `crate::ca`, run by `raidish-ca`.

use crate::pki::{CertInfo, LIBVIRT_FILES, Role, inspect_der, read_pem_certs};
use ::time::OffsetDateTime;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use std::fmt;
use std::path::{Path, PathBuf};
use x509_parser::prelude::*;

fn err(e: impl fmt::Display) -> String {
    e.to_string()
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
