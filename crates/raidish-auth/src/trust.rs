//! What this node refuses, live (CA-2a, `docs/ca-service-plan.md` §3.7):
//! revoked certificates, certificates of revoked issuers, revoked tokens.
//!
//! One [`Trust`] per node, fed by the cluster as revocations are applied
//! (`PkiObserver`), and read by every check of a peer: the TLS verifiers
//! (`crate::tls`, `crate::http::LiveTrust`), each request's caller
//! (`crate::auth`) and the S3/SQS keys. A revocation also ends the peer's
//! open connections and streams ([`Connections`]).

use crate::http::Connections;
use raidish_types::pki::{IssuerStatus, KeyId, PkiObserver, PkiState, Serial};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// The refused, as of the last state applied.
#[derive(Default, Debug)]
struct Revoked {
    serials: HashSet<Serial>,
    /// Revoked issuers' subject key ids: every leaf whose authority key id
    /// is one of these is refused, recorded or not.
    issuers: HashSet<KeyId>,
    /// Token (and access key) ids: the cluster's and the config's.
    tokens: HashSet<String>,
}

pub struct Trust {
    revoked: RwLock<Arc<Revoked>>,
    /// `auth.revoked` from the config: always refused.
    configured: Vec<String>,
    conns: Arc<Connections>,
}

impl std::fmt::Debug for Trust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Trust").finish_non_exhaustive()
    }
}

impl Trust {
    /// Refusing only the token ids `configured` (`auth.revoked`) until the
    /// cluster's CA is observed.
    pub fn new(configured: Vec<String>) -> Arc<Trust> {
        let revoked = Revoked {
            tokens: configured.iter().cloned().collect(),
            ..Default::default()
        };
        Arc::new(Trust {
            revoked: RwLock::new(Arc::new(revoked)),
            configured,
            conns: Connections::new(),
        })
    }

    /// This node's open connections and streams, by peer serial.
    pub fn connections(&self) -> &Arc<Connections> {
        &self.conns
    }

    fn current(&self) -> Arc<Revoked> {
        self.revoked.read().unwrap().clone()
    }

    /// Whether the certificate `der` (a leaf that has passed its chain
    /// check) is still good: not revoked, nor signed by a revoked issuer.
    pub fn check_leaf(&self, der: &[u8]) -> Result<(), String> {
        let r = self.current();
        if r.serials.is_empty() && r.issuers.is_empty() {
            return Ok(());
        }
        let serial = crate::ca::facts(der)?.serial;
        if r.serials.contains(&serial) {
            return Err(format!("certificate {} is revoked", serial));
        }
        if let Some(aki) = crate::ca::authority_key_id(der)
            && r.issuers.contains(&aki)
        {
            return Err(format!(
                "certificate {} was issued by a revoked issuer",
                serial
            ));
        }
        Ok(())
    }

    /// Whether token (or access key) `id` is revoked.
    pub fn token_revoked(&self, id: &str) -> bool {
        self.current().tokens.contains(id)
    }
}

impl PkiObserver for Trust {
    fn observe_pki(&self, pki: Option<&PkiState>) {
        let mut next = Revoked {
            tokens: self.configured.iter().cloned().collect(),
            ..Default::default()
        };
        if let Some(pki) = pki {
            next.serials.extend(pki.revoked_certs.keys().cloned());
            next.tokens
                .extend(pki.revoked_tokens.keys().map(|t| t.as_str().to_string()));
            for issuer in pki.issuers.values() {
                if matches!(issuer.status, IssuerStatus::Revoked { .. }) {
                    next.issuers.insert(issuer.ski.clone());
                    // Its own certificate too (a client never presents
                    // it, but a chain check might see it).
                    next.serials.insert(issuer.serial.clone());
                }
            }
        }
        let next = Arc::new(next);
        let prev = std::mem::replace(&mut *self.revoked.write().unwrap(), next.clone());
        // What's newly refused can't keep what it has open.
        for s in next.serials.difference(&prev.serials) {
            self.conns.close(s);
        }
        for ski in next.issuers.difference(&prev.issuers) {
            self.conns.close_issued_by(ski);
        }
    }
}

#[cfg(test)]
#[path = "tests/trust_tests.rs"]
mod tests;
