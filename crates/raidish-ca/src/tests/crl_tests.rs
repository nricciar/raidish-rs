//! When a CRL is due, and what a sealed node says about it.

use super::*;
use raidish_types::pki::IssuerId;

const T0: u64 = 1_700_000_000_000;

fn with_crl(generation: u64, crl_generation: u64, next: u64) -> PkiState {
    // The fixture from `ca init` would need a cluster: a state with one
    // active issuer is enough here.
    let mut p: PkiState = serde_json::from_value(serde_json::json!({
        "anchor": {"cert_pem": "ROOT", "fingerprint": "00".repeat(32)},
        "issuers": {"1": {
            "cert_pem": "ISSUER", "serial": "02", "ski": "02",
            "not_after_ms": T0 + 700 * DAY_MS,
            "sealed_key": {"v": 1, "nonce": "00".repeat(12), "ct": ""},
            "status": "active"
        }},
        "seal": {"cluster_id": "00".repeat(16), "slots": [], "epoch": 0},
        "generation": generation
    }))
    .unwrap();
    p.crl = Some(CrlRecord {
        issuer: IssuerId(1),
        der: vec![],
        number: 1,
        this_update_ms: T0,
        next_update_ms: next,
        generation: crl_generation,
    });
    p
}

#[test]
fn due_when_missing_behind_or_running_out() {
    let fresh = with_crl(5, 5, T0 + LIFE_MS);
    assert!(!due(&fresh, T0));
    assert!(!due(&fresh, T0 + 19 * DAY_MS), "11 days left");
    assert!(due(&fresh, T0 + 21 * DAY_MS), "9 days left");
    assert!(due(&with_crl(6, 5, T0 + LIFE_MS), T0), "a revocation since");
    let mut none = fresh.clone();
    none.crl = None;
    assert!(due(&none, T0));
    let mut other = fresh.clone();
    other.crl.as_mut().unwrap().issuer = IssuerId(9);
    assert!(due(&other, T0), "another issuer's");
}

#[test]
fn a_sealed_node_warns_when_it_runs_out() {
    let p = with_crl(5, 5, T0 + LIFE_MS);
    assert_eq!(warning(&p, false, T0), None);
    let w = warning(&p, false, T0 + 25 * DAY_MS).unwrap();
    assert!(w.contains("runs out in 5 days"), "{}", w);
    assert_eq!(warning(&p, true, T0 + 25 * DAY_MS), None, "it can re-sign");
    let w = warning(&p, false, T0 + 31 * DAY_MS).unwrap();
    assert!(w.contains("out of date"), "{}", w);
}
