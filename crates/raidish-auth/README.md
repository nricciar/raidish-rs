# raidish-auth

Identity and transport for [RAIDish](../../README.md) nodes and clients.

`raidish-auth` holds what the cluster's CA (`raidish-ca`) signs with and every process checks with: X.509 issuance and checking, sealing, rustls TLS configuration with live-swappable identity and trust, HTTP/WebSocket clients, stateless request authentication (mTLS client certificates and signed bearer tokens) with axum extractors, and the shared TCP accept loop used by RAIDish's protocol listeners.

## Scope

**What's in this crate:**
- The managed CA's X.509 side (`ca`): issuers under an offline root, CSR signing per profile, CRLs, the token signer's certificate, and fingerprints; and its sealing (`envelope`): the DEK, key slots, and `root.enc`
- Reading certificates (`pki`): `CertInfo`, `inspect_der`, and where a host's libvirt/qemu files go (`LIBVIRT_FILES`)
- Process-wide rustls client/server TLS setup (`TlsConfig`, a live identity renewals swap in place, `reqwest` HTTP clients, `tokio-tungstenite` WebSocket connectors, and peer role extraction)
- Stateless authentication via mTLS client certificates or bearer tokens (`rdt2`, signed by the CA's token signer; older `rdt1` ones are still accepted) through `Verifier`, with live revocation
- axum extractors enforcing role hierarchy (`Authed`, `CanRead`, `CanWrite`, `IsAdmin`, `IsAdminCert`, `IsNode`, `Actor`)
- Shared TCP listener utilities (`accept_loop`, `bind_retrying`, `tls_accept`) with backoff on transient accept errors and handshake timeouts
- With `testutil`: a throwaway offline CA (`pki::Ca`) and PKI (`testing`) for other crates' tests

## Module Map

| Module | Purpose |
|---|---|
| `ca` | X.509 for the cluster CA: issuers, CSR signing, CRLs, checks (`X509Checker`), `key_matches` |
| `envelope` | Sealing: AES-GCM under the DEK, key slots (recovery key, passphrase), the root's envelope |
| `pki` | Reading certificates (`CertInfo`, `inspect_der`, `read_pem_certs`), `LIBVIRT_FILES`, `parse_duration` |
| `tls` | `TlsConfig` loading (`dir` or explicit PEM paths), rustls configs, the live identity and its paired file swap (`swap_identity`), process-wide `tls::init`, and configured `reqwest` / WebSocket clients |
| `auth` | `rdt1`/`rdt2` bearer tokens, `Verifier` for certificates and tokens, revocation checks, and request extractors |
| `http` | Serving HTTP(S) with axum |
| `trust` | What a node refuses, live: revoked certificates, issuers and tokens, fed from Raft |
| `net` | Shared `accept_loop`, retry-on-bind helper, timed `tls_accept`, log throttling, and `disk_server_base` URL parsing |

## Certificates

The cluster's CA is `raidish ca` (the `raidish-ca` crate): `raidish ca init` on node 0, `raidish join` for the other nodes, `raidish ca issue` / `raidish enroll` for clients. Nodes renew their own certificates live; see the main [README](../../README.md#certificates).

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-auth

# Run the raidish-auth unit test suite
cargo test -p raidish-auth
```
