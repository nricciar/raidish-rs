//! Issuer rotation (CA-4d, `docs/ca-service-plan.md`): the root stays
//! with the admin. `ca rotate-issuer` makes the new issuer with it and
//! hands it to an unsealed node, which seals its key under the DEK and
//! makes it the active one. Everything that follows does so by itself:
//! nodes renew (their issuer isn't active, `crate::renew`), the token
//! signer and the CRL are made anew (`crate::token_signer`, `crate::crl`).
//!
//! `ca revoke-issuer` then revokes the old one once nothing of the
//! cluster's own is on it ([`PkiState::issuer_use`]), and sends the
//! root's CRL saying so, which the admin signs with the root.

use crate::api::{IssuerAdded, NewIssuerRequest};
use crate::now_ms;
use crate::service::{CaError, CaService};
use raidish_auth::ca::{X509Checker, facts, first_der, key_matches};
use raidish_auth::envelope;
use raidish_types::api_types::EventKind;
use raidish_types::pki::{
    CertChecker, IssuerId, IssuerRecord, IssuerStatus, IssuerUse, PkiOp, RootCrlRecord,
};
use rustls::pki_types::CertificateRevocationListDer;
use rustls::pki_types::pem::PemObject;
use zeroize::Zeroizing;

/// What's still on an issuer, said plainly.
pub fn describe(u: &IssuerUse) -> String {
    let mut what = Vec::new();
    if !u.nodes.is_empty() {
        what.push(format!(
            "the node certificates of {}",
            u.nodes
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if u.token_signer {
        what.push("the token signer".into());
    }
    if u.crl {
        what.push("the CRL".into());
    }
    what.join(", ")
}

impl CaService {
    /// `POST /api/v1/pki/issuers`.
    pub async fn add_issuer(
        &self,
        caller: &[u8],
        req: NewIssuerRequest,
        by: Option<String>,
    ) -> Result<IssuerAdded, CaError> {
        let key = Zeroizing::new(req.key_pem);
        if self.is_staged().await {
            return Err(CaError::conflict(
                "the CA is only staged here: rotate once the cluster has it",
            ));
        }
        let pki = self.for_caller(caller).await?;
        let dek = self.dek_at(&pki)?;
        let next = IssuerId(pki.issuers.keys().map(|i| i.0).max().unwrap_or(0) + 1);
        if req.id != next {
            return Err(CaError::conflict(format!(
                "the next issuer is {}, not {}: make it again",
                next, req.id
            )));
        }
        key_matches(&req.cert_pem, &key).map_err(CaError::bad)?;
        let f = facts(&first_der(&req.cert_pem).map_err(CaError::bad)?).map_err(CaError::bad)?;
        let record = IssuerRecord {
            cert_pem: req.cert_pem.clone(),
            serial: f.serial.clone(),
            ski: f.ski,
            not_after_ms: f.not_after_ms,
            sealed_key: envelope::seal(
                &dek,
                &envelope::issuer_aad(&pki.anchor.fingerprint, next),
                key.as_bytes(),
            )
            .map_err(CaError::internal)?,
            status: IssuerStatus::Active,
        };
        let retired: Vec<IssuerId> = pki
            .issuers
            .iter()
            .filter(|(_, r)| r.status == IssuerStatus::Active)
            .map(|(id, _)| *id)
            .collect();
        // The state machine checks it chains to the root.
        self.commit_at(
            &pki,
            PkiOp::AddIssuer {
                id: next,
                record: Box::new(record),
                activate: true,
            },
        )
        .await?;
        let msg = format!(
            "{} is the active issuer (serial {}){}",
            next,
            f.serial,
            match retired.as_slice() {
                [] => String::new(),
                r => format!(
                    "; {} retired",
                    r.iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        );
        println!("ca: {}", msg);
        self.event(
            EventKind::CaIssuerAdded,
            raidish_types::api_types::Subject::Cluster,
            msg,
            by,
        );
        Ok(IssuerAdded {
            id: next,
            serial: f.serial,
            not_after_ms: f.not_after_ms,
            retired,
        })
    }

    /// `GET /api/v1/pki/issuers/<id>`.
    pub async fn issuer_use(&self, id: IssuerId) -> Result<IssuerUse, CaError> {
        let pki = self.current_linearizable().await?;
        if !pki.issuers.contains_key(&id) {
            return Err(CaError::new(404, format!("no {}", id)));
        }
        Ok(pki.issuer_use(id, now_ms(), crate::token_signer::signed_by))
    }

    /// `POST /api/v1/pki/issuers/<id>/revoke`: refused while the
    /// cluster's own certificates are on it, unless `force`.
    pub async fn revoke_issuer(
        &self,
        id: IssuerId,
        force: bool,
        by: Option<String>,
    ) -> Result<IssuerUse, CaError> {
        let u = self.issuer_use(id).await?;
        if !force && !u.clear() {
            return Err(CaError::conflict(format!(
                "{} still has {}: wait for them to move to the active issuer (nodes renew \
                 by themselves), or force it",
                id,
                describe(&u)
            )));
        }
        self.commit(PkiOp::RevokeIssuer { id }).await?;
        let msg = format!(
            "{} revoked: its certificates stop working{}",
            id,
            match u.others.len() {
                0 => String::new(),
                n => format!(" ({} others besides the cluster's)", n),
            }
        );
        println!("ca: {}", msg);
        self.event(
            EventKind::CaIssuerRevoked,
            raidish_types::api_types::Subject::Cluster,
            msg,
            by,
        );
        Ok(u)
    }

    /// `POST /api/v1/pki/root-crl`: the root's CRL, signed by the admin.
    pub async fn set_root_crl(
        &self,
        caller: &[u8],
        crl_pem: &str,
        by: Option<String>,
    ) -> Result<RootCrlRecord, CaError> {
        let pki = self.for_caller(caller).await?;
        let der = CertificateRevocationListDer::from_pem_slice(crl_pem.as_bytes())
            .map_err(|e| CaError::bad(format!("bad CRL PEM: {}", e)))?
            .to_vec();
        let f = X509Checker
            .crl(&der, &pki.anchor.cert_pem)
            .map_err(|e| CaError::bad(format!("not the root's CRL: {}", e)))?;
        let record = RootCrlRecord {
            der,
            number: f.number,
            this_update_ms: f.this_update_ms,
            next_update_ms: f.next_update_ms,
        };
        self.commit(PkiOp::SetRootCrl(Box::new(record.clone())))
            .await?;
        self.event(
            EventKind::CaRootCrl,
            raidish_types::api_types::Subject::Cluster,
            format!("root CRL {} recorded", record.number),
            by,
        );
        Ok(record)
    }

    /// The root's CRL, as Raft has it.
    pub async fn root_crl(&self) -> Option<RootCrlRecord> {
        self.current().await.and_then(|p| p.root_crl.clone())
    }
}
