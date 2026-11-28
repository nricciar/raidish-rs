//! When a node makes a new token signer.

use super::*;
use raidish_types::pki::{Revocation, RevocationReason, SealedBlob, Serial};

const T0: u64 = 1_700_000_000_000;
const DAY_MS: u64 = 86_400_000;

/// Real PEM, so the chain's issuer can be compared.
fn pems() -> (String, String, String) {
    let (root, root_key) = raidish_auth::ca::create_root("r", 7300).unwrap();
    let (a, _) = raidish_auth::ca::create_issuer("a", &root, &root_key, 730).unwrap();
    let (b, _) = raidish_auth::ca::create_issuer("b", &root, &root_key, 730).unwrap();
    (root, a, b)
}

fn state(issuer: &str, signer: Option<(String, u64)>) -> PkiState {
    let mut p: PkiState = serde_json::from_value(serde_json::json!({
        "anchor": {"cert_pem": "ROOT", "fingerprint": "00".repeat(32)},
        "issuers": {"1": {
            "cert_pem": issuer, "serial": "02", "ski": "02",
            "not_after_ms": T0 + 700 * DAY_MS,
            "sealed_key": {"v": 1, "nonce": "00".repeat(12), "ct": ""},
            "status": "active"
        }},
        "seal": {"cluster_id": "00".repeat(16), "slots": [], "epoch": 0},
        "generation": 1
    }))
    .unwrap();
    p.token_signer = signer.map(|(chain, not_after_ms)| TokenSignerRecord {
        chain_pem: chain,
        serial: Serial::from_be_bytes(&[40]),
        not_after_ms,
        sealed_key: SealedBlob {
            v: 1,
            nonce: [0; 12],
            ct: vec![],
        },
    });
    p
}

#[test]
fn due_when_missing_revoked_from_another_issuer_or_two_thirds_gone() {
    let life = 730 * DAY_MS;
    // The leaf is a stand-in: only the chain's issuer is compared.
    let (leaf, active, old) = pems();
    assert!(due(&state(&active, None), T0), "none yet");
    let fresh = state(&active, Some((format!("{}{}", leaf, active), T0 + life)));
    assert!(!due(&fresh, T0));
    assert!(
        !due(&fresh, T0 + life * 2 / 3 - DAY_MS),
        "a third left, and a day"
    );
    assert!(
        due(&fresh, T0 + life * 2 / 3 + DAY_MS),
        "less than a third left"
    );
    let other = state(&active, Some((format!("{}{}", leaf, old), T0 + life)));
    assert!(due(&other, T0), "another issuer's");
    let mut revoked = fresh.clone();
    revoked.revoked_certs.insert(
        Serial::from_be_bytes(&[40]),
        Revocation {
            reason: RevocationReason::KeyCompromise,
            at_ms: T0,
            expires_ms: None,
        },
    );
    assert!(due(&revoked, T0), "revoked");
    // No active issuer: nothing to sign one with.
    let mut retired = state(&active, None);
    retired.issuers.clear();
    assert!(!due(&retired, T0));
}
