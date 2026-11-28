//! The admin's side of issuer rotation (CA-4d): what `ca rotate-issuer`
//! and `ca revoke-issuer` do with the root, on the admin's machine. The
//! root's key is opened from `root.enc` only for this, and never sent.

use crate::api::NewIssuerRequest;
use raidish_auth::ca::{self, Signer};
use raidish_auth::envelope::{self, RootEnvelope};
use raidish_types::pki::{CertFingerprint, IssuerId, RevocationReason, Serial};
use zeroize::Zeroizing;

/// The root, opened: its certificate and key.
pub struct Root {
    pub cert_pem: String,
    key_pem: Zeroizing<String>,
    pub not_after_ms: u64,
}

impl Root {
    /// Open `root.enc` (its JSON) with the root key from `ca init`, if
    /// it's the root of the CA whose fingerprint is `anchor`.
    pub fn open(
        envelope_json: &[u8],
        root_key: &str,
        anchor: &CertFingerprint,
    ) -> Result<Root, String> {
        let env: RootEnvelope =
            serde_json::from_slice(envelope_json).map_err(|e| format!("root.enc: {}", e))?;
        let fp = ca::fingerprint(&ca::first_der(&env.cert_pem)?);
        if !envelope::ct_eq(&fp.0, &anchor.0) {
            return Err(format!(
                "root.enc holds another CA's root ({}) than the cluster's ({})",
                fp, anchor
            ));
        }
        let key = envelope::parse_recovery_key(root_key.trim())
            .map_err(|e| format!("the root key: {}", e))?;
        let key_pem = env.open(&key)?;
        let not_after_ms = ca::facts(&ca::first_der(&env.cert_pem)?)?.not_after_ms;
        Ok(Root {
            cert_pem: env.cert_pem,
            key_pem,
            not_after_ms,
        })
    }

    /// A new issuer `id` under the root, `days` long (at most the root's
    /// life): what `POST /api/v1/pki/issuers` takes.
    pub fn issuer(&self, id: IssuerId, days: u32) -> Result<NewIssuerRequest, String> {
        let (cert_pem, key_pem) = ca::create_issuer(
            &format!("raidish issuer {}", id.0),
            &self.cert_pem,
            &self.key_pem,
            days,
        )?;
        Ok(NewIssuerRequest {
            id,
            cert_pem,
            key_pem: key_pem.to_string(),
        })
    }

    /// The root's CRL (PEM): `revoked` issuers (serial, when), number
    /// `number`, current until the root runs out (nothing re-signs it on
    /// a schedule).
    pub fn crl(&self, revoked: &[(Serial, u64)], number: u64) -> Result<String, String> {
        let signer = Signer::new(IssuerId(0), &self.cert_pem, &self.key_pem)?;
        let list: Vec<_> = revoked
            .iter()
            .map(|(s, at)| (s.clone(), RevocationReason::Superseded, *at))
            .collect();
        let (der, _) = signer.sign_crl(&list, number, self.not_after_ms)?;
        Ok(crate::crl_pem(&der))
    }
}
