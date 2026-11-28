//! What's warned about when no node is unsealed.

use super::*;
use raidish_types::pki::{
    CrlRecord, Identity, IssuedCert, IssuerId, Revocation, RevocationReason, Serial,
};

const T0: u64 = 1_700_000_000_000;

fn state() -> PkiState {
    serde_json::from_value(serde_json::json!({
        "anchor": {"cert_pem": "ROOT", "fingerprint": "00".repeat(32)},
        "issuers": {"1": {
            "cert_pem": "ISSUER", "serial": "02", "ski": "02",
            "not_after_ms": T0 + 700 * DAY_MS,
            "sealed_key": {"v": 1, "nonce": "00".repeat(12), "ct": ""},
            "status": "active"
        }},
        "seal": {"cluster_id": "00".repeat(16), "slots": [], "epoch": 0},
        "generation": 1
    }))
    .unwrap()
}

fn cert(p: &mut PkiState, serial: u8, member: u64, profile: ProfileKind, days: u64) {
    p.certs.insert(
        Serial::from_be_bytes(&[serial]),
        IssuedCert {
            identity: Identity {
                profile,
                cn: format!("node{}", member),
                sans: vec![],
                member: Some(NodeId::from_raw(member)),
            },
            issuer: IssuerId(1),
            not_before_ms: T0,
            not_after_ms: T0 + days * DAY_MS,
            renews: None,
        },
    );
}

#[test]
fn a_members_latest_live_node_cert_counts() {
    let mut p = state();
    let n1 = NodeId::from_raw(1);
    cert(&mut p, 1, 1, ProfileKind::Node, 5);
    assert!(
        node_cert_due(&p, n1, T0)
            .unwrap()
            .contains("node-1's certificate")
    );
    // Renewed: the new one counts, not the one it renewed.
    cert(&mut p, 2, 1, ProfileKind::Node, 90);
    assert_eq!(node_cert_due(&p, n1, T0), None);
    // A libvirt cert isn't the node's.
    cert(&mut p, 3, 2, ProfileKind::Libvirt, 3);
    assert_eq!(node_cert_due(&p, NodeId::from_raw(2), T0), None);
    // Revoked: as if it weren't there.
    p.revoked_certs.insert(
        Serial::from_be_bytes(&[2]),
        Revocation {
            reason: RevocationReason::Superseded,
            at_ms: T0,
            expires_ms: None,
        },
    );
    assert!(node_cert_due(&p, n1, T0).is_some());
    assert_eq!(unsealed_none(&state(), true, T0), None, "nothing runs out");
}

#[test]
fn the_warning_names_what_runs_out_and_whether_everyone_answered() {
    let mut p = state();
    cert(&mut p, 1, 0, ProfileKind::Node, 5);
    cert(&mut p, 2, 1, ProfileKind::Node, 90);
    p.crl = Some(CrlRecord {
        issuer: IssuerId(1),
        der: vec![],
        number: 1,
        this_update_ms: T0,
        next_update_ms: T0 + 3 * DAY_MS,
        generation: 1,
    });
    let w = unsealed_none(&p, true, T0).unwrap();
    assert!(w.starts_with("no node is unsealed"), "{}", w);
    assert!(
        w.contains("node-0's certificate") && w.contains("the CRL"),
        "{}",
        w
    );
    assert!(!w.contains("node-1"), "{}", w);
    assert!(
        unsealed_none(&p, false, T0)
            .unwrap()
            .starts_with("no reachable node")
    );
}
