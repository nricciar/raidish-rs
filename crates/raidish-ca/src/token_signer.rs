//! The token signer (CA-2c, D6): the one identity `rdt2` tokens are signed
//! with. An unsealed node makes it when there is none, when it was revoked,
//! when its issuer is no longer the active one, or once two thirds of its
//! life are gone (checked with the CRL, `crate::crl`). Its key is sealed
//! under the DEK in Raft, so any unsealed node can mint tokens.
//!
//! Tokens carry their signer's chain, so those from a replaced signer stay
//! good until they expire; revoking the signer (`ca revoke <serial>`) ends
//! them all, and a new one is made.

use crate::api::{TokenCreated, TokenRequest};
use crate::now_ms;
use crate::service::{CaError, CaService};
use raidish_auth::ca::LeafSpec;
use raidish_auth::envelope;
use raidish_cluster::state::{LeaseOp, Response};
use raidish_types::pki::{PkiOp, PkiState, ProfileKind, TokenSignerRecord};
use zeroize::Zeroizing;

/// The token signer's CN.
pub const CN: &str = "raidish-tokens";

/// Whether `pki` needs a new token signer at `now`.
pub fn due(pki: &PkiState, now: u64) -> bool {
    let Some((_, issuer)) = pki.active_issuer() else {
        return false;
    };
    let Some(t) = &pki.token_signer else {
        return true;
    };
    let life_ms = pki.params(ProfileKind::TokenSigner).default_days as u64 * 86_400_000;
    pki.is_revoked(&t.serial)
        || !signed_by(&t.chain_pem, &issuer.cert_pem)
        || t.not_after_ms.saturating_sub(now) < life_ms / 3
}

/// Whether `chain`'s last certificate is `issuer`'s.
fn signed_by(chain: &str, issuer: &str) -> bool {
    match (
        raidish_auth::ca::pem_certs(chain),
        raidish_auth::ca::pem_certs(issuer),
    ) {
        (Ok(c), Ok(i)) => c.len() == 2 && i.first().is_some_and(|i| c[1] == *i),
        _ => false,
    }
}

/// The token signer, unsealed: what mints `rdt2` tokens.
pub struct TokenKey {
    pub chain_pem: String,
    pub key_pem: Zeroizing<String>,
}

impl CaService {
    /// Make and record a token signer if this node can and one is due.
    pub(crate) async fn refresh_token_signer(&self) -> Result<bool, CaError> {
        if !self.unsealed() || self.is_staged().await {
            return Ok(false);
        }
        if !self.cluster().pki().is_some_and(|p| due(&p, now_ms())) {
            return Ok(false);
        }
        // Not from a view behind this node's own commits (see
        // `refresh_crl`).
        let pki = self.current_linearizable().await?;
        if !due(&pki, now_ms()) {
            return Ok(false);
        }
        let dek = self.dek_at(&pki)?;
        let signer = self.signer(&pki)?;
        let (key_pem, csr) =
            raidish_auth::ca::generate_csr(CN).map_err(|e| CaError::new(500, e))?;
        let issued = signer
            .sign(
                &csr,
                &LeafSpec {
                    profile: ProfileKind::TokenSigner,
                    cn: CN.into(),
                    sans: vec![],
                    days: pki.params(ProfileKind::TokenSigner).default_days,
                },
            )
            .map_err(|e| CaError::new(500, e))?;
        let record = TokenSignerRecord {
            chain_pem: signer.chain(&issued.cert_pem),
            serial: issued.serial.clone(),
            not_after_ms: issued.not_after_ms,
            sealed_key: envelope::seal(
                &dek,
                &envelope::token_signer_aad(&pki.anchor.fingerprint, &issued.serial),
                key_pem.as_bytes(),
            )
            .map_err(|e| CaError::new(500, e))?,
        };
        match self
            .cluster()
            .lease(
                &LeaseOp::Pki(PkiOp::SetTokenSigner(Box::new(record)).at_epoch(pki.seal.epoch)),
                false,
            )
            .await
        {
            Ok(Response::Recorded) => {
                println!("ca: token signer {} made", issued.serial);
                Ok(true)
            }
            Ok(Response::Refused(e)) => Err(CaError::new(409, e)),
            Ok(other) => Err(CaError::new(500, format!("unexpected answer {:?}", other))),
            Err(e) => Err(CaError::new(503, e)),
        }
    }

    /// Mint an `rdt2` token (`POST /api/v1/pki/tokens`).
    pub async fn create_token(
        &self,
        caller: &[u8],
        req: TokenRequest,
    ) -> Result<TokenCreated, CaError> {
        use rustls::pki_types::{PrivatePkcs8KeyDer, pem::PemObject};
        let role: raidish_auth::pki::Role =
            req.role.parse().map_err(|e: String| CaError::new(400, e))?;
        if role == raidish_auth::pki::Role::Node {
            return Err(CaError::new(
                400,
                "tokens can't carry the node role (nodes use certificates)",
            ));
        }
        if req.sub.trim().is_empty() {
            return Err(CaError::new(400, "a token is for someone: sub is empty"));
        }
        if req.ttl_s.is_some_and(|t| t <= 0) {
            return Err(CaError::new(400, "ttl_s must be positive"));
        }
        let pki = self.for_caller(caller).await?;
        let key = self.token_key(&pki)?;
        let chain =
            raidish_auth::ca::pem_certs(&key.chain_pem).map_err(|e| CaError::new(500, e))?;
        let pkcs8 = PrivatePkcs8KeyDer::from_pem_slice(key.key_pem.as_bytes())
            .map_err(|e| CaError::new(500, format!("the token signer's key: {}", e)))?;
        let (token, claims) = raidish_auth::auth::create_token_rdt2(
            &req.sub,
            role,
            req.ttl_s,
            &chain,
            pkcs8.secret_pkcs8_der(),
        )
        .map_err(|e| CaError::new(500, e))?;
        Ok(TokenCreated {
            token,
            id: raidish_types::pki::TokenId::parse(&claims.id).map_err(|e| CaError::new(500, e))?,
            sub: claims.sub,
            role: claims.role,
            exp: claims.exp.unwrap_or_default(),
        })
    }

    /// The token signer, its key opened (503 when sealed or none yet).
    pub(crate) fn token_key(&self, pki: &PkiState) -> Result<TokenKey, CaError> {
        let dek = self.dek_at(pki).map_err(|e| match e.status {
            503 => CaError::new(
                503,
                "this node is sealed (it can't mint tokens): ask another node",
            ),
            _ => e,
        })?;
        let t = pki
            .token_signer
            .as_ref()
            .ok_or_else(|| CaError::new(503, "no token signer yet: an unsealed node makes one"))?;
        if pki.is_revoked(&t.serial) {
            return Err(CaError::new(
                503,
                "the token signer was revoked: a new one is on its way",
            ));
        }
        let key = envelope::open(
            &dek,
            &envelope::token_signer_aad(&pki.anchor.fingerprint, &t.serial),
            &t.sealed_key,
        )
        .map_err(|e| CaError::new(500, format!("the token signer's key: {}", e)))?;
        let key_pem = Zeroizing::new(
            String::from_utf8(key.to_vec()).map_err(|_| CaError::new(500, "not PEM"))?,
        );
        Ok(TokenKey {
            chain_pem: t.chain_pem.clone(),
            key_pem,
        })
    }
}

#[cfg(test)]
#[path = "tests/token_signer_tests.rs"]
mod tests;
