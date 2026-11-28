//! Listing and revoking (CA-2a): nothing here signs, so a sealed node
//! does it too. What's revoked is refused everywhere as soon as it's
//! applied (`raidish_auth::trust`).

use crate::api::{RevokeCertRequest, RevokeTokenRequest};
use crate::now_ms;
use crate::service::{CaError, CaService};
use raidish_types::pki::{CertListing, PkiOp, ProfileKind, RevocationReason, Serial, TokenId};

impl CaService {
    /// Every certificate on record, oldest first: live, expired (until
    /// pruned) and revoked.
    pub async fn list_certs(&self) -> Result<Vec<CertListing>, CaError> {
        let pki = self.current_linearizable().await?;
        let mut certs: Vec<CertListing> = pki
            .certs
            .iter()
            .map(|(serial, cert)| CertListing {
                serial: serial.clone(),
                cert: cert.clone(),
                revoked: pki.revoked_certs.get(serial).cloned(),
            })
            .collect();
        certs.sort_by_key(|c| (c.cert.not_before_ms, c.serial.clone()));
        Ok(certs)
    }

    /// Revoke the certificate `serial`. `caller`: the certificate the
    /// request came with, if any (it can't revoke itself: the answer
    /// would never arrive).
    pub async fn revoke_cert(
        &self,
        serial: Serial,
        req: RevokeCertRequest,
        caller: Option<&Serial>,
    ) -> Result<(), CaError> {
        let pki = self.current_linearizable().await?;
        if pki.is_revoked(&serial) {
            return Ok(());
        }
        // The token signer: every `rdt2` token with it. An unsealed node
        // makes the next one.
        if pki
            .token_signer
            .as_ref()
            .is_some_and(|t| t.serial == serial)
        {
            return self
                .commit(PkiOp::RevokeCert {
                    serial,
                    reason: req.reason.unwrap_or(RevocationReason::Unspecified),
                })
                .await;
        }
        let cert = pki
            .certs
            .get(&serial)
            .ok_or_else(|| CaError::new(404, format!("no certificate {} on record", serial)))?;
        if caller == Some(&serial) {
            return Err(CaError::conflict(
                "that's the certificate this request came with: revoke it with another one",
            ));
        }
        let now = now_ms();
        if cert.identity.profile == ProfileKind::Node
            && let Some(member) = cert.identity.member
            && pki.is_live(&serial, now)
            && !req.force
        {
            let others = pki
                .member_certs(member, now)
                .filter(|(s, c)| **s != serial && c.identity.profile == ProfileKind::Node)
                .count();
            if others == 0 {
                return Err(CaError::conflict(format!(
                    "{} is {}'s only node certificate: revoking it cuts that node off \
                     the cluster until it joins again (with a `ca token create --replace` \
                     token). Again with --force to do it",
                    serial, member
                )));
            }
        }
        self.commit(PkiOp::RevokeCert {
            serial,
            reason: req.reason.unwrap_or(RevocationReason::Unspecified),
        })
        .await
    }

    /// Revoke the token (or access key) `id`.
    pub async fn revoke_token(&self, id: TokenId, req: RevokeTokenRequest) -> Result<(), CaError> {
        self.current().await.ok_or_else(CaError::no_ca)?;
        if let Some(exp) = req.expires_ms
            && exp <= now_ms()
        {
            return Err(CaError::bad(
                "that token has expired already: nothing to revoke",
            ));
        }
        self.commit(PkiOp::RevokeToken {
            id,
            expires_ms: req.expires_ms,
        })
        .await
    }
}
