# raidish-auth

Identity and transport for [RAIDish](../../README.md) nodes and clients.

`raidish-auth` implements the cluster's private PKI, rustls TLS configuration and HTTP/WebSocket clients, stateless request authentication (mTLS client certificates and signed bearer tokens) with Rocket request guards, and the shared TCP accept loop used by RAIDish's protocol listeners.

## Scope

**What's in this crate:**
- Private ECDSA P-256 CA (`Ca`), per-identity certificate bundles (`node`, `admin`, `readonly`, plus libvirt/QEMU migration certs), and rolling CA trust-bundle management (`trust` / `untrust`)
- Process-wide rustls client/server TLS setup (`TlsConfig`, `reqwest` HTTP clients, `tokio-tungstenite` WebSocket connectors, and peer role extraction)
- Stateless authentication via mTLS client certificates or admin-signed `rdt1` bearer tokens (`create_token`, `Verifier`) with config-based revocation
- Rocket request guards enforcing role hierarchy (`Authed`, `CanRead`, `CanWrite`, `IsNode`, `Actor`)
- Shared TCP listener utilities (`accept_loop`, `bind_retrying`, `tls_accept`) with backoff on transient accept errors and handshake timeouts

## Module Map

| Module | Purpose |
|---|---|
| `pki` | Cluster CA initialization (`Ca::init` / `Ca::load`), issuing identity and libvirt certs, PEM/DER inspection, and CA trust-bundle rotation |
| `tls` | `TlsConfig` loading (`dir` or explicit PEM paths), rustls client/server configs, process-wide `tls::init`, and configured `reqwest` / WebSocket clients |
| `auth` | Admin-signed `rdt1` bearer tokens, `Verifier` for certificates and tokens, revocation checks, and Rocket request guards (`Authed`, `CanRead`, `CanWrite`, `IsNode`) |
| `net` | Shared `accept_loop`, retry-on-bind helper, timed `tls_accept`, log throttling, and `disk_server_base` URL parsing |

## Setting Up a Cluster PKI

Although the CLI lives in the root `raidish` crate, `raidish cert` is a thin wrapper around `raidish_auth::pki`. You can run it from any machine—`ca.key` never needs to be copied onto cluster nodes.

```bash
# 1. Create the CA (pki/ca.key + pki/ca.pem)
raidish cert init --dir pki

# 2. Issue a node cert for every host in the pool config's disk URLs
raidish -c cluster.yaml cert issue-nodes --dir pki
# ...or issue nodes individually (name, localhost, and 127.0.0.1 are always added as SANs)
raidish cert issue node0 --role node --san 10.0.0.10 --dir pki

# 3. Issue an admin bundle for the CLI (and optionally a readonly bundle)
raidish cert issue admin --role admin --dir pki

# 4. Verify the trust bundle, SANs, and expiry
raidish cert status --dir pki
```

Certificates do not expire by default; pass `--days <N>` on `init` or `issue` / `issue-nodes` to set a lifetime.

Each issued identity gets a self-contained directory `pki/<name>/` holding `ca.pem`, `cert.pem`, and `key.pem`. Copy each node's bundle to that host (e.g., `/etc/raidish/tls`) and enable TLS in its config using `https://` for remote disk URLs:

```yaml
tls:
  dir: /etc/raidish/tls
```

## Renewing & Rotating Certificates

**Renewing a node or client certificate** (expiring cert, new SAN, or replaced key):

```bash
# Reissue one certificate
raidish cert issue node0 --role node --days 365 --force --dir pki

# Or reissue all node certificates from the cluster config
raidish -c cluster.yaml cert issue-nodes --days 365 --force --dir pki
```

Nodes log a warning at startup if their certificate expires within 30 days. Restart nodes **one at a time** during any rotation so the cluster maintains quorum.

Reissue in place with `--force`, copy the updated bundle to the host, and restart that node.

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-auth

# Run the raidish-auth unit test suite
cargo test -p raidish-auth
```
