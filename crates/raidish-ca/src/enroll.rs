//! Client certificates by token (CA-4c, `docs/ca-service-plan.md`): an
//! admin makes a `Client` enrollment for a profile and a name, and whoever
//! holds the token gets that certificate for a key of their own
//! (`raidish enroll`).
//!
//! As `/join`: authenticated by the token alone, rate-limited, passed on
//! by a sealed node, consumed at the seal epoch its MAC was checked at.

use crate::api::{
    ClientEnrollmentCreated, ClientEnrollmentRequest, DEFAULT_TOKEN_TTL_SECS, EnrollRequest,
    Enrolled, MAX_TOKEN_TTL_SECS,
};
use crate::now_ms;
use crate::service::{CaError, CaService, answer, enrollment, sign};
use crate::token::JoinToken;
use raidish_auth::envelope;
use raidish_types::api_types::Principal;
use raidish_types::pki::{EnrollScope, Identity, PkiOp, ProfileKind};

/// The profiles a client enrolls as: client certificates an admin could
/// issue, with no host names (a server's are an admin's to approve).
pub fn enrollable(p: ProfileKind) -> bool {
    let spec = p.spec();
    p.admin_issuable() && spec.client_auth && !spec.server_auth
}

/// A certificate's name as an admin gives it.
pub(crate) fn check_cn(cn: &str) -> Result<String, CaError> {
    let cn = cn.trim().to_string();
    if cn.is_empty() || cn.len() > 64 || cn.chars().any(|c| c.is_control()) {
        return Err(CaError::bad(
            "a certificate needs a name (CN) of 1-64 characters",
        ));
    }
    Ok(cn)
}

impl CaService {
    /// `POST /api/v1/pki/enrollments/client`.
    pub async fn create_client_enrollment(
        &self,
        admin: &Principal,
        caller: &[u8],
        req: ClientEnrollmentRequest,
    ) -> Result<ClientEnrollmentCreated, CaError> {
        let pki = self.for_caller(caller).await?;
        let dek = self.dek_at(&pki)?;
        if !enrollable(req.profile) {
            return Err(CaError::bad(format!(
                "clients enroll as {}, not {}",
                ProfileKind::ALL
                    .into_iter()
                    .filter(|p| enrollable(*p))
                    .map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                req.profile
            )));
        }
        let cn = check_cn(&req.cn)?;
        let ttl = req.ttl_secs.unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        if ttl == 0 || ttl > MAX_TOKEN_TTL_SECS {
            return Err(CaError::bad(format!(
                "a token lasts up to {} days",
                MAX_TOKEN_TTL_SECS / 86400
            )));
        }
        let token = JoinToken::generate(pki.anchor.fingerprint).map_err(CaError::internal)?;
        let e = enrollment(
            &dek,
            &token,
            EnrollScope::Client {
                profile: req.profile,
                cn: cn.clone(),
            },
            now_ms() + ttl * 1000,
            &admin.name,
        );
        let expires_ms = e.expires_ms;
        self.commit_at(
            &pki,
            PkiOp::CreateEnrollment {
                id: token.id.clone(),
                enrollment: Box::new(e),
            },
        )
        .await?;
        println!(
            "ca: {} made enrollment token {} for '{}' ({})",
            admin.name, token.id, cn, req.profile
        );
        Ok(ClientEnrollmentCreated {
            id: token.id.clone(),
            profile: req.profile,
            cn,
            token: token.encode().to_string(),
            expires_ms,
        })
    }

    /// `POST /api/v1/pki/enroll`.
    pub async fn enroll(&self, req: EnrollRequest, forwarded: bool) -> Result<Enrolled, CaError> {
        let pki = self.current().await.ok_or_else(CaError::no_ca)?;
        if !self.unsealed() {
            if forwarded {
                return Err(CaError::sealed());
            }
            // The signer names its peers; this node's add the signer.
            let mut got: Enrolled = self.forward("enroll", &req).await?;
            for p in self.cluster().members.peers.values() {
                if !got.endpoints.contains(p) {
                    got.endpoints.push(p.clone());
                }
            }
            return Ok(got);
        }
        let unknown = || CaError::denied("unknown, expired or used-up enrollment token");
        let secret = crate::token::decode_secret(&req.secret).map_err(CaError::denied)?;
        let hash = envelope::sha256(&secret);
        let e = pki
            .enrollments
            .get(&req.id)
            .filter(|e| envelope::ct_eq(&e.secret_sha256, &hash))
            .filter(|e| e.expires_ms > now_ms())
            .ok_or_else(unknown)?;
        let dek = self.dek_at(&pki)?;
        if !envelope::enrollment_mac_ok(&dek, &req.id, e) {
            return Err(CaError::denied(
                "this token wasn't made by the cluster's signer (its MAC is wrong)",
            ));
        }
        let EnrollScope::Client { profile, cn } = &e.scope else {
            return Err(CaError::denied(
                "a node's join token: nodes join with `raidish join`",
            ));
        };
        // Checked when it was made, and again: the rules may be newer.
        if !enrollable(*profile) {
            return Err(CaError::denied(format!(
                "no enrollment issues {} certificates",
                profile
            )));
        }
        let identity = Identity {
            profile: *profile,
            cn: cn.clone(),
            sans: vec![],
            member: None,
        };
        let signer = self.signer(&pki)?;
        // Signed first: a bad CSR mustn't use the token up.
        let issued = sign(
            &signer,
            &req.csr_pem,
            &identity,
            pki.params(*profile).default_days,
            None,
        )?;
        self.commit_at(
            &pki,
            PkiOp::ConsumeEnrollment {
                id: req.id.clone(),
                secret_sha256: hash,
                member: None,
            },
        )
        .await?;
        self.record(&pki, &signer, &identity, &issued, None).await?;
        println!(
            "ca: '{}' enrolled ({}, certificate {})",
            identity.cn, identity.profile, issued.serial
        );
        Ok(Enrolled {
            issued: answer(&pki, &signer, issued),
            profile: identity.profile,
            cn: identity.cn,
            endpoints: self.cluster().members.peers.values().cloned().collect(),
        })
    }
}
