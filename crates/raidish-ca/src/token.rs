//! Join tokens: `rdj1.<root sha256>.<id>.<secret>`.
//!
//! The root's fingerprint lets the joiner check who it's talking to before
//! it trusts anything (it has no CA bundle yet); the id names the
//! enrollment in `PkiState`, which holds only the secret's hash.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use raidish_types::pki::{CertFingerprint, TokenId};
use zeroize::Zeroizing;

const PREFIX: &str = "rdj1";

pub struct JoinToken {
    pub root: CertFingerprint,
    pub id: TokenId,
    pub secret: Zeroizing<Vec<u8>>,
}

impl JoinToken {
    /// A token with a fresh id and secret.
    pub fn generate(root: CertFingerprint) -> Result<JoinToken, String> {
        let id: [u8; 8] = raidish_auth::envelope::random()?;
        let secret: [u8; 32] = raidish_auth::envelope::random()?;
        Ok(JoinToken {
            root,
            id: TokenId::from_bytes(id),
            secret: Zeroizing::new(secret.to_vec()),
        })
    }

    pub fn secret_b64(&self) -> Zeroizing<String> {
        Zeroizing::new(B64.encode(&*self.secret))
    }

    pub fn secret_sha256(&self) -> [u8; 32] {
        raidish_auth::envelope::sha256(&self.secret)
    }

    pub fn encode(&self) -> Zeroizing<String> {
        Zeroizing::new(format!(
            "{}.{}.{}.{}",
            PREFIX,
            B64.encode(self.root.0),
            self.id,
            *self.secret_b64()
        ))
    }

    pub fn parse(s: &str) -> Result<JoinToken, String> {
        let bad = || "not a join token (they look like rdj1.…)".to_string();
        let mut parts = s.trim().split('.');
        if parts.next() != Some(PREFIX) {
            return Err(bad());
        }
        let (Some(root), Some(id), Some(secret), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(bad());
        };
        let root: [u8; 32] = B64
            .decode(root)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or_else(bad)?;
        Ok(JoinToken {
            root: CertFingerprint(root),
            id: TokenId::parse(id)?,
            secret: Zeroizing::new(decode_secret(secret)?),
        })
    }
}

/// A token's secret as sent in a request.
pub fn decode_secret(s: &str) -> Result<Vec<u8>, String> {
    let b = B64.decode(s).map_err(|_| "bad token secret".to_string())?;
    if b.len() != 32 {
        return Err("bad token secret".into());
    }
    Ok(b)
}
