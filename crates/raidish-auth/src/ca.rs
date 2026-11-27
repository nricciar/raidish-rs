//! The managed CA's X.509 (see `docs/ca-service-plan.md`): a root
//! (`pathLen=1`, kept offline), issuers under it (`pathLen=0`, their keys
//! sealed in Raft), leaves signed from CSRs, and the [`X509Checker`] the
//! Raft state machine checks CA certificates with.
//!
//! A CSR is only proof that the requester holds a key: its subject, SANs and
//! extensions are ignored, and the certificate is built from a
//! [`LeafSpec`] the signer derived. All keys are ECDSA P-256.

use ::time::OffsetDateTime;
use raidish_types::pki::{
    CertChecker, CertFacts, CertFingerprint, CrlFacts, IssuerId, KeyId, ProfileKind,
    RevocationReason, Serial,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use x509_parser::prelude::*;
use zeroize::Zeroizing;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// The certificates in a PEM string (DER), in order.
pub fn pem_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("bad PEM: {}", e))
}

pub fn first_der(pem: &str) -> Result<CertificateDer<'static>, String> {
    pem_certs(pem)?
        .into_iter()
        .next()
        .ok_or_else(|| "no certificate in PEM".to_string())
}

pub fn fingerprint(der: &[u8]) -> CertFingerprint {
    CertFingerprint(crate::envelope::sha256(der))
}

fn ms(t: OffsetDateTime) -> u64 {
    (t.unix_timestamp_nanos() / 1_000_000).max(0) as u64
}

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn random_serial() -> Result<SerialNumber, String> {
    let mut b: [u8; 16] = crate::envelope::random()?;
    b[0] &= 0x7f; // positive
    b[0] |= 0x01; // and 16 bytes long, never shorter
    Ok(SerialNumber::from_slice(&b))
}

/// What a certificate says, beyond `pki::CertInfo`: for the registry and the
/// checks.
pub fn facts(der: &[u8]) -> Result<CertFacts, String> {
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| format!("bad certificate: {}", e))?;
    let mut ski = None;
    for ext in cert.extensions() {
        if let ParsedExtension::SubjectKeyIdentifier(k) = ext.parsed_extension() {
            ski = Some(KeyId(k.0.to_vec()));
        }
    }
    let v = cert.validity();
    Ok(CertFacts {
        serial: Serial::from_be_bytes(cert.raw_serial()),
        ski: ski.unwrap_or(KeyId(Vec::new())),
        not_before_ms: ms(v.not_before.to_datetime()),
        not_after_ms: ms(v.not_after.to_datetime()),
        fingerprint: fingerprint(der),
    })
}

/// A certificate's Authority Key Identifier, if it has one.
pub fn authority_key_id(der: &[u8]) -> Option<KeyId> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    cert.extensions()
        .iter()
        .find_map(|ext| match ext.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(a) => {
                a.key_identifier.as_ref().map(|k| KeyId(k.0.to_vec()))
            }
            _ => None,
        })
}

fn ca_params(name: &str, path_len: u8, days: u32) -> Result<CertificateParams, String> {
    let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    dn.push(DnType::OrganizationName, "raidish");
    params.distinguished_name = dn;
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(path_len));
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.serial_number = Some(random_serial()?);
    let t = now();
    params.not_before = t - ::time::Duration::hours(1);
    params.not_after = t + ::time::Duration::days(days as i64);
    Ok(params)
}

/// A CA's cert and key in the form rcgen signs with. The certificate is
/// re-made from the real one's parameters (same name, same key id), which
/// is all a child references.
fn as_issuer(cert_pem: &str, key_pem: &str) -> Result<(rcgen::Certificate, KeyPair), String> {
    let key = KeyPair::from_pem(key_pem).map_err(err)?;
    let der = first_der(cert_pem)?;
    let params = CertificateParams::from_ca_cert_der(&der).map_err(err)?;
    let cert = params.self_signed(&key).map_err(err)?;
    Ok((cert, key))
}

/// A new root CA: its certificate and key (PEM), `pathLen=1`.
pub fn create_root(name: &str, days: u32) -> Result<(String, Zeroizing<String>), String> {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
    let cert = ca_params(name, 1, days)?.self_signed(&key).map_err(err)?;
    Ok((cert.pem(), Zeroizing::new(key.serialize_pem())))
}

/// A new issuer under the root (`pathLen=0`, at most as long as the root).
pub fn create_issuer(
    name: &str,
    root_pem: &str,
    root_key_pem: &str,
    days: u32,
) -> Result<(String, Zeroizing<String>), String> {
    let (root, root_key) = as_issuer(root_pem, root_key_pem)?;
    let root_end = facts(&first_der(root_pem)?)?.not_after_ms;
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
    let mut params = ca_params(name, 0, days)?;
    params.use_authority_key_identifier_extension = true;
    clamp_end(&mut params, root_end);
    let cert = params.signed_by(&key, &root, &root_key).map_err(err)?;
    Ok((cert.pem(), Zeroizing::new(key.serialize_pem())))
}

fn clamp_end(params: &mut CertificateParams, end_ms: u64) {
    let end = OffsetDateTime::from_unix_timestamp_nanos(end_ms as i128 * 1_000_000)
        .unwrap_or(params.not_after);
    if params.not_after > end {
        params.not_after = end;
    }
}

/// A new key and a CSR for it: what a joining node or a client makes
/// before asking to be signed. The key never leaves the caller.
pub fn generate_csr(cn: &str) -> Result<(Zeroizing<String>, String), String> {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(err)?;
    let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    params.distinguished_name = dn;
    let csr = params.serialize_request(&key).map_err(err)?;
    Ok((Zeroizing::new(key.serialize_pem()), csr.pem().map_err(err)?))
}

/// The certificate a signer makes, decided by the signer.
#[derive(Clone, Debug)]
pub struct LeafSpec {
    pub profile: ProfileKind,
    pub cn: String,
    pub sans: Vec<String>,
    pub days: u32,
}

/// A certificate signed.
#[derive(Clone, Debug)]
pub struct Issued {
    pub cert_pem: String,
    pub serial: Serial,
    pub not_before_ms: u64,
    pub not_after_ms: u64,
}

/// An issuer, unsealed: signs leaves.
pub struct Signer {
    pub id: IssuerId,
    pub cert_pem: String,
    not_after_ms: u64,
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl Signer {
    pub fn new(id: IssuerId, cert_pem: &str, key_pem: &str) -> Result<Signer, String> {
        let (cert, key) = as_issuer(cert_pem, key_pem)?;
        Ok(Signer {
            id,
            cert_pem: cert_pem.to_string(),
            not_after_ms: facts(&first_der(cert_pem)?)?.not_after_ms,
            cert,
            key,
        })
    }

    /// Sign `csr_pem`'s key as `spec` says (nothing else of the CSR is
    /// used). The certificate ends with the issuer at the latest.
    pub fn sign(&self, csr_pem: &str, spec: &LeafSpec) -> Result<Issued, String> {
        let mut csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|e| format!("bad CSR (or its signature): {}", e))?;
        if csr.public_key.algorithm() != &rcgen::PKCS_ECDSA_P256_SHA256 {
            return Err("only ECDSA P-256 keys are signed".into());
        }
        csr.params = leaf_params(spec)?;
        clamp_end(&mut csr.params, self.not_after_ms);
        let cert = csr.signed_by(&self.cert, &self.key).map_err(err)?;
        let f = facts(cert.der())?;
        Ok(Issued {
            cert_pem: cert.pem(),
            serial: f.serial,
            not_before_ms: f.not_before_ms,
            not_after_ms: f.not_after_ms,
        })
    }

    /// A CRL listing `revoked` (serial, reason, when), number `number`,
    /// current from now until `next_update_ms` (DER).
    pub fn sign_crl(
        &self,
        revoked: &[(Serial, RevocationReason, u64)],
        number: u64,
        next_update_ms: u64,
    ) -> Result<(Vec<u8>, CrlFacts), String> {
        let at = |ms: u64| {
            OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000).map_err(err)
        };
        let revoked_certs = revoked
            .iter()
            .map(|(serial, reason, at_ms)| {
                Ok(rcgen::RevokedCertParams {
                    serial_number: SerialNumber::from_slice(&serial_bytes(serial)?),
                    revocation_time: at(*at_ms)?,
                    reason_code: Some(match reason {
                        RevocationReason::Unspecified => rcgen::RevocationReason::Unspecified,
                        RevocationReason::KeyCompromise => rcgen::RevocationReason::KeyCompromise,
                        RevocationReason::Superseded => rcgen::RevocationReason::Superseded,
                        RevocationReason::CessationOfOperation => {
                            rcgen::RevocationReason::CessationOfOperation
                        }
                    }),
                    invalidity_date: None,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        // Whole seconds: what the CRL can say, so the record matches it.
        let this = now().replace_nanosecond(0).map_err(err)?;
        let params = rcgen::CertificateRevocationListParams {
            this_update: this,
            next_update: at(next_update_ms - next_update_ms % 1000)?,
            crl_number: SerialNumber::from_slice(&number.to_be_bytes()),
            issuing_distribution_point: None,
            revoked_certs,
            key_identifier_method: self.cert.params().key_identifier_method.clone(),
        };
        let crl = params.signed_by(&self.cert, &self.key).map_err(err)?;
        let der = crl.der().to_vec();
        let facts = crl_facts(&der)?;
        Ok((der, facts))
    }

    /// A leaf, its chain (leaf then this issuer) as one PEM: what
    /// `cert.pem` holds, so peers can build the path to the root.
    pub fn chain(&self, leaf_pem: &str) -> String {
        format!("{}{}", leaf_pem, self.cert_pem)
    }
}

fn leaf_params(spec: &LeafSpec) -> Result<CertificateParams, String> {
    let p = spec.profile.spec();
    let mut params = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
    params.subject_alt_names = spec
        .sans
        .iter()
        .map(|s| match s.parse::<std::net::IpAddr>() {
            Ok(ip) => Ok(SanType::IpAddress(ip)),
            Err(_) => s
                .as_str()
                .try_into()
                .map(SanType::DnsName)
                .map_err(|e| format!("SAN '{}': {}", s, e)),
        })
        .collect::<Result<_, String>>()?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, spec.cn.as_str());
    dn.push(DnType::OrganizationalUnitName, p.ou);
    dn.push(DnType::OrganizationName, "raidish");
    params.distinguished_name = dn;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    if p.key_encipherment {
        params.key_usages.push(KeyUsagePurpose::KeyEncipherment);
    }
    params.extended_key_usages = Vec::new();
    if p.server_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
    }
    if p.client_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
    }
    if p.code_signing {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::CodeSigning);
    }
    params.use_authority_key_identifier_extension = true;
    params.serial_number = Some(random_serial()?);
    let t = now();
    // A little slack for clocks slightly behind.
    params.not_before = t - ::time::Duration::hours(1);
    params.not_after = t + ::time::Duration::days(spec.days as i64);
    Ok(params)
}

// ---- checking CA certificates for the state machine ------------------------------

/// The X.509 [`CertChecker`] every Raft replica applies CA ops with.
/// Deterministic: it reads only the certificates and the time it's given.
pub struct X509Checker;

const ECDSA_SHA256: &str = "1.2.840.10045.4.3.2";

fn parse(der: &[u8]) -> Result<X509Certificate<'_>, String> {
    X509Certificate::from_der(der)
        .map(|(_, c)| c)
        .map_err(|e| format!("bad certificate: {}", e))
}

fn check_ca_constraints(cert: &X509Certificate<'_>, path_len: u8) -> Result<(), String> {
    match cert.basic_constraints().map_err(err)?.map(|b| b.value) {
        Some(b) if b.ca && b.path_len_constraint == Some(path_len as u32) => Ok(()),
        Some(b) if b.ca => Err(format!(
            "a CA with path length {:?}, not {}",
            b.path_len_constraint, path_len
        )),
        _ => Err("not a CA certificate".into()),
    }
}

fn check_valid_at(cert: &X509Certificate<'_>, at_ms: u64) -> Result<(), String> {
    let v = cert.validity();
    let (nb, na) = (
        ms(v.not_before.to_datetime()),
        ms(v.not_after.to_datetime()),
    );
    if at_ms < nb || at_ms >= na {
        return Err("not valid at that time".into());
    }
    Ok(())
}

/// Is `cert` signed by `by`'s key (ECDSA P-256 / SHA-256, all the CA makes)?
fn check_signed_by(cert: &X509Certificate<'_>, by: &X509Certificate<'_>) -> Result<(), String> {
    if cert.signature_algorithm.algorithm.to_id_string() != ECDSA_SHA256 {
        return Err("not signed with ECDSA P-256 / SHA-256".into());
    }
    if cert.issuer().as_raw() != by.subject().as_raw() {
        return Err("issued under another name".into());
    }
    ring::signature::UnparsedPublicKey::new(
        &ring::signature::ECDSA_P256_SHA256_ASN1,
        &by.public_key().subject_public_key.data,
    )
    .verify(cert.tbs_certificate.as_ref(), &cert.signature_value.data)
    .map_err(|_| "bad signature".to_string())
}

/// OU `token-signer`, not a CA, and codeSigning its only EKU: a cert that
/// signs tokens and can do nothing in TLS.
pub fn check_token_signer_cert(cert: &X509Certificate<'_>) -> Result<(), String> {
    let ou: Vec<&str> = cert
        .subject()
        .iter_organizational_unit()
        .filter_map(|a| a.as_str().ok())
        .collect();
    if ou != [ProfileKind::TokenSigner.spec().ou] {
        return Err(format!("OU {:?}, not token-signer", ou));
    }
    if cert.is_ca() {
        return Err("a CA certificate".into());
    }
    let eku = cert
        .extended_key_usage()
        .map_err(err)?
        .ok_or("no extended key usage")?
        .value;
    let only_code_signing = eku.code_signing
        && !(eku.any
            || eku.server_auth
            || eku.client_auth
            || eku.email_protection
            || eku.time_stamping
            || eku.ocsp_signing
            || !eku.other.is_empty());
    if !only_code_signing {
        return Err("its extended key usage isn't codeSigning alone".into());
    }
    Ok(())
}

fn serial_bytes(s: &Serial) -> Result<Vec<u8>, String> {
    let h = s.as_str();
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(h.get(i..i + 2).unwrap_or("x"), 16))
        .collect::<Result<_, _>>()
        .map_err(|_| format!("bad serial {}", s))
}

/// A CRL's number and times (its signature unchecked).
pub fn crl_facts(der: &[u8]) -> Result<CrlFacts, String> {
    let (_, crl) =
        CertificateRevocationList::from_der(der).map_err(|e| format!("bad CRL: {}", e))?;
    let number = crl.crl_number().ok_or("the CRL has no number")?;
    if number.bits() > 64 {
        return Err("CRL number out of range".into());
    }
    Ok(CrlFacts {
        number: number.to_u64_digits().first().copied().unwrap_or(0),
        this_update_ms: ms(crl.last_update().to_datetime()),
        next_update_ms: ms(crl
            .next_update()
            .ok_or("the CRL has no next update")?
            .to_datetime()),
    })
}

/// The serials a CRL lists.
pub fn crl_serials(der: &[u8]) -> Result<Vec<Serial>, String> {
    let (_, crl) =
        CertificateRevocationList::from_der(der).map_err(|e| format!("bad CRL: {}", e))?;
    Ok(crl
        .iter_revoked_certificates()
        .map(|r| Serial::from_be_bytes(r.raw_serial()))
        .collect())
}

impl CertChecker for X509Checker {
    fn anchor(&self, pem: &str, at_ms: u64) -> Result<CertFacts, String> {
        let der = first_der(pem)?;
        let cert = parse(&der)?;
        check_signed_by(&cert, &cert).map_err(|e| format!("not self-signed: {}", e))?;
        check_ca_constraints(&cert, 1)?;
        check_valid_at(&cert, at_ms)?;
        facts(&der)
    }

    fn ca(
        &self,
        pem: &str,
        parent_pem: &str,
        path_len: u8,
        at_ms: u64,
    ) -> Result<CertFacts, String> {
        let der = first_der(pem)?;
        let parent_der = first_der(parent_pem)?;
        let (cert, parent) = (parse(&der)?, parse(&parent_der)?);
        check_signed_by(&cert, &parent)?;
        check_ca_constraints(&cert, path_len)?;
        check_valid_at(&cert, at_ms)?;
        facts(&der)
    }

    fn token_signer(
        &self,
        chain_pem: &str,
        issuer_pem: &str,
        at_ms: u64,
    ) -> Result<CertFacts, String> {
        let chain = pem_certs(chain_pem)?;
        let [leaf_der, carried] = &chain[..] else {
            return Err("the chain must be the signer then its issuer".into());
        };
        let issuer_der = first_der(issuer_pem)?;
        if carried.as_ref() != issuer_der.as_ref() {
            return Err("the chain's issuer isn't the active issuer".into());
        }
        let (leaf, issuer) = (parse(leaf_der)?, parse(&issuer_der)?);
        check_signed_by(&leaf, &issuer)?;
        check_valid_at(&leaf, at_ms)?;
        check_token_signer_cert(&leaf)?;
        facts(leaf_der)
    }

    fn crl(&self, der: &[u8], issuer_pem: &str) -> Result<CrlFacts, String> {
        let (_, crl) =
            CertificateRevocationList::from_der(der).map_err(|e| format!("bad CRL: {}", e))?;
        let issuer_der = first_der(issuer_pem)?;
        let issuer = parse(&issuer_der)?;
        if crl.signature_algorithm.algorithm.to_id_string() != ECDSA_SHA256 {
            return Err("not signed with ECDSA P-256 / SHA-256".into());
        }
        if crl.issuer().as_raw() != issuer.subject().as_raw() {
            return Err("issued under another name".into());
        }
        ring::signature::UnparsedPublicKey::new(
            &ring::signature::ECDSA_P256_SHA256_ASN1,
            &issuer.public_key().subject_public_key.data,
        )
        .verify(crl.tbs_cert_list.as_ref(), &crl.signature_value.data)
        .map_err(|_| "bad signature".to_string())?;
        crl_facts(der)
    }
}

#[cfg(test)]
#[path = "tests/ca_tests.rs"]
mod tests;
