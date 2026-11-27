//! The CRL (CA-2b, `docs/ca-service-plan.md`): signed by the active
//! issuer on an unsealed node, kept in Raft (`PkiState::crl`), served by
//! every node.
//!
//! An unsealed node re-signs it when the CA's revocations change (it's
//! behind `PkiState::generation`), when the active issuer changes, and when
//! it has fewer than [`RESIGN_BEFORE_MS`] left. It checks when the CA
//! changes, when it's unsealed, and every hour. Several unsealed nodes may
//! race: `SetCrl` takes only a newer number, so the first wins and the
//! others' are refused.

use crate::now_ms;
use crate::service::{CaError, CaService};
use raidish_cluster::state::{LeaseOp, Response};
use raidish_types::pki::{CrlRecord, PkiOp, PkiState};
use std::sync::{Arc, Weak};
use std::time::Duration;

const DAY_MS: u64 = 86_400_000;
/// How long a CRL is current (`nextUpdate`).
pub const LIFE_MS: u64 = 30 * DAY_MS;
/// Re-signed once it has less than this left.
pub const RESIGN_BEFORE_MS: u64 = 10 * DAY_MS;
/// How often an unsealed node checks, changes or not.
const CHECK_EVERY: Duration = Duration::from_secs(3600);

/// Start `ca`'s re-signer (woken by `CaService`'s observer when the CA
/// changes).
pub(crate) fn start(ca: &Arc<CaService>) {
    tokio::spawn(run(Arc::downgrade(ca), ca.crl_wake.clone()));
}

async fn run(ca: Weak<CaService>, wake: Arc<tokio::sync::Notify>) {
    loop {
        {
            let Some(ca) = ca.upgrade() else { return };
            // The token signer first: making one is a change the CRL
            // then covers.
            if let Err(e) = ca.refresh_token_signer().await {
                eprintln!("ca: the token signer: {}", e);
            }
            if let Err(e) = ca.refresh_crl().await {
                eprintln!("ca: re-signing the CRL: {}", e);
            }
            ca.check_unsealed_none().await;
        }
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(CHECK_EVERY) => {}
        }
    }
}

/// Whether `pki`'s CRL needs signing anew at `now`.
pub fn due(pki: &PkiState, now: u64) -> bool {
    let Some((active, _)) = pki.active_issuer() else {
        return false;
    };
    match &pki.crl {
        None => true,
        Some(c) => {
            c.issuer != active
                || c.generation < pki.generation
                || c.next_update_ms.saturating_sub(now) < RESIGN_BEFORE_MS
        }
    }
}

/// What to say about the CRL on a node that can't re-sign it (sealed), if
/// anything: it runs out soon, or is out.
pub fn warning(pki: &PkiState, unsealed: bool, now: u64) -> Option<String> {
    let crl = pki.crl.as_ref()?;
    if unsealed || crl.next_update_ms.saturating_sub(now) >= RESIGN_BEFORE_MS {
        return None;
    }
    let what = if crl.next_update_ms <= now {
        "the CRL is out of date".to_string()
    } else {
        format!(
            "the CRL runs out in {} days",
            (crl.next_update_ms - now) / DAY_MS
        )
    };
    Some(format!(
        "{} and no unsealed node has re-signed it: unseal one (`raidish ca unseal`)",
        what
    ))
}

impl CaService {
    /// The CRL, as Raft has it.
    pub async fn crl(&self) -> Option<CrlRecord> {
        self.current().await.and_then(|p| p.crl.clone())
    }

    /// Sign and record a new CRL if this node can and one is due. Whether
    /// it did.
    pub(crate) async fn refresh_crl(&self) -> Result<bool, CaError> {
        if !self.unsealed() || self.is_staged().await {
            return Ok(false);
        }
        let now = now_ms();
        if !self.cluster().pki().is_some_and(|p| due(&p, now)) {
            return Ok(false);
        }
        // Due by this node's view, which can be behind what it just
        // committed (`lease` answers at commit): a CRL signed from it
        // would be refused, or behind, and nothing would wake it again.
        let pki = self.current_linearizable().await?;
        if !due(&pki, now) {
            return Ok(false);
        }
        let signer = self.signer(&pki)?;
        let revoked: Vec<_> = pki
            .revoked_certs
            .iter()
            .filter(|(s, _)| pki.certs.get(*s).is_some_and(|c| c.issuer == signer.id))
            .map(|(s, r)| (s.clone(), r.reason, r.at_ms))
            .collect();
        let number = pki
            .crl
            .as_ref()
            .filter(|c| c.issuer == signer.id)
            .map_or(1, |c| c.number + 1);
        let (der, facts) = signer
            .sign_crl(&revoked, number, now + LIFE_MS)
            .map_err(|e| CaError::new(500, e))?;
        let record = CrlRecord {
            issuer: signer.id,
            der,
            number: facts.number,
            this_update_ms: facts.this_update_ms,
            next_update_ms: facts.next_update_ms,
            generation: pki.generation,
        };
        match self
            .cluster()
            .lease(
                &LeaseOp::Pki(PkiOp::SetCrl(Box::new(record)).at_epoch(pki.seal.epoch)),
                false,
            )
            .await
        {
            Ok(Response::Recorded) => {
                println!("ca: CRL {} signed ({} revoked)", number, revoked.len());
                Ok(true)
            }
            // Another node's went in first.
            Ok(Response::Refused(_)) => Ok(false),
            Ok(other) => Err(CaError::new(500, format!("unexpected answer {:?}", other))),
            Err(e) => Err(CaError::new(503, e)),
        }
    }
}

#[cfg(test)]
#[path = "tests/crl_tests.rs"]
mod tests;
