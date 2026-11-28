# raidish-ca

The embedded cluster Certificate Authority for [RAIDish](../../README.md).

Every `listen` node runs `CaService` backed by replicated Raft state (`ClusterState::pki` via [`raidish-cluster`](../raidish-cluster/)) and X.509/envelope cryptography from [`raidish-auth`](../raidish-auth/). The root key stays offline in an encrypted `root.enc` envelope on the operator's machine, while an active intermediate issuer's private key is sealed under a cluster DEK in Raft—allowing any unsealed node to sign certificates, rotate CRLs, and mint `rdt2` bearer tokens while nodes renew their own certificates live without restarts.

## Scope

**What's in this crate:**
- Staged bootstrap (`init`): generates the offline root (`root.enc`), first intermediate issuer, key slots, node 0 + operator bundles, and per-peer join tokens before Raft forms, then promotes the staged state into Raft once a leader is elected
- Cluster CA service & HTTP API (`service`, `routes`, `api`): `CaService` and `/api/v1/pki/*` `axum` routes for joining, enrolling, signing, renewing, revoking, CRLs, issuers, sealing, and `rdt2` tokens
- Pin-verified onboarding (`join`, `enroll`, `token`): single-use `rdj1` tokens embedding the root SHA-256 fingerprint so joiners and clients verify the cluster root before sending any secret
- Live node self-renewal (`renew`): nodes automatically generate a new key/CSR at 2/3 of their certificate lifetime (default 30 days) or on issuer rotation and hot-swap `LiveIdentity` with no restart
- Sealing, key slots & rekeying (`seal`, `loud`): recovery-key, passphrase, and per-node auto-unseal (`systemd-creds` or file) slots, cluster-wide sealing, DEK rekeying, and activity-log warnings when all nodes are sealed near cert/CRL expiry
- Revocation, CRLs, token signer & issuer rotation (`revoke`, `crl`, `token_signer`, `issuers`, `rotate`): immediate cluster-wide revocation (dropping active connections), auto-resigned CRLs, rotating `rdt2` token signer, and offline-root intermediate issuer rotation

## Architecture & Module Map

```text
Offline root (`root.enc`, operator-held)
   └── signs intermediate issuer (key sealed under cluster DEK in Raft)
          └── CaService on each `listen` node (`/api/v1/pki/*`)
                 ├── unsealed node: signs join/enroll/renew/issue, CRLs, `rdt2` tokens
                 └── sealed node: serves reads/revocations, forwards token join/enroll
```

| Module | Purpose |
|---|---|
| `init` | `raidish ca init`: builds the offline root, first issuer, node 0 + admin bundles, peer join tokens, and staged `pki_bootstrap.json` |
| `service`, `routes`, `api` | `CaService` state machine/signer, `/api/v1/pki/*` `axum` handlers, and request/response DTOs |
| `join`, `enroll`, `token` | `rdj1` token format, root-fingerprint-pinned `raidish join` (nodes) and `raidish enroll` (clients) |
| `renew` | Automatic background node certificate renewal and in-place `LiveIdentity` swap |
| `seal`, `loud` | Unseal/seal/rekey/slot management, `NodeKeySource` auto-unseal, cluster-wide seal status, and expiry warnings |
| `revoke`, `crl` | Certificate listing/revocation (works even while sealed) and background CRL signing |
| `token_signer` | Rotating `raidish-tokens` signer identity for `rdt2` bearer tokens |
| `issuers`, `rotate` | Online issuer management (`PkiState::issuer_use`) and offline-root `rotate-issuer` / `revoke-issuer` |

## CLI Usage

### Bootstrapping a Cluster & Joining Nodes

Run `ca init` on node 0 before its first `listen`. It writes node 0's certificate files, an operator bundle (`pki-admin/`), and `root.enc`, stages the CA in node 0's data dir (with a one-shot unseal file for its first boot), and prints the recovery key plus a `join` command for each peer:

```bash
# On node 0 (keep root.enc and the printed recovery key off the cluster)
raidish -c node0.yaml ca init
raidish -c node0.yaml listen

# On each peer node before its first `listen`, using its printed join token
raidish -c node1.yaml join https://node0:8881 --token-stdin
raidish -c node1.yaml listen

# Inspect CA state, active issuer, and which nodes are unsealed
raidish ca status
```

### Issuing, Enrolling & Renewing Certificates

```bash
# Issue a certificate directly (profiles: admin, readonly, client, server, libvirt)
raidish ca issue alice --profile readonly --out ./alice-tls
raidish ca issue host1 --profile libvirt --member node-1 --san 10.0.0.2 --out ./libvirt-host1

# Or create a single-use enrollment token so a client generates its own private key locally
raidish ca token create --client --profile readonly --cn bob
raidish enroll https://node0:8881 --token-stdin --dir ~/.raidish/tls

# Re-join a reinstalled node (revokes its previous cert when the new one joins)
raidish ca token create --member node-1 --replace

# Renew your admin certificate in place, or trigger immediate renewal on running nodes
raidish ca renew
raidish ca renew --node
```

### Listing, Revoking & CRLs

Revocations apply immediately across all nodes (even if the CA is sealed) and close active connections using the revoked certificate:

```bash
raidish ca list                          # live certs (use --revoked or --all)
raidish ca revoke <serial-or-cn> --reason compromised
raidish ca crl --pem --out cluster.crl   # active issuer CRL (--root for root CRL)
```

### Sealing, Unsealing & Key Slots

After a full cluster restart, nodes start sealed—storage, existing mTLS, and issued tokens keep working, but signing waits until at least one node is unsealed:

```bash
# Unseal a node using a recovery key or passphrase (--all unseals every node)
raidish ca unseal

# Seal every node immediately (removes node-key slots unless --keep-node-keys is passed)
raidish ca seal

# Manage unseal slots
raidish ca slot list
raidish ca slot add --recovery-key --label "offsite-safe"
raidish ca slot add --node-key node-0    # prints systemd-creds setup for auto-unseal

# Replace the DEK and all slots if a slot secret has leaked
raidish ca rekey
```

For unattended restarts on a node, store a `--node-key` slot in an encrypted systemd credential (`LoadCredentialEncrypted=raidish-unseal`) and reference it in the node's config:

```yaml
ca:
  unseal_credential: raidish-unseal
```

### Rotating the Intermediate Issuer

Issuer rotation uses the offline `root.enc` + root key on the admin's machine. Once the new issuer is active, nodes renew onto it automatically (and CRLs + the token signer re-sign):

```bash
raidish ca rotate-issuer --root root.enc
# Once nodes have moved off the old issuer, revoke it with a signed root CRL:
raidish ca revoke-issuer issuer-1 --root root.enc
```

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-ca

# Run the raidish-ca unit and cluster test suite
cargo test -p raidish-ca
```
