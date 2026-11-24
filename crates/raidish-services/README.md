# raidish-services

Network services over [RAIDish](../../README.md) B+tree datasets: **S3**, **SQS**, AWS Signature V4 authentication, and the multi-node **frontend** serving S3, SQS, and `etcd`.

Each bucket, queue, or `etcd` instance is a standalone dataset run by one writer node at a time under its `ds/<name>` lease. Every `listen` node exposes the service ports and transparently proxies or forwards requests to whichever node currently owns the target dataset.

## Scope

**What's in this crate:**
- **S3** (`s3`): one `s3` B+tree dataset per bucket with 4 MiB chunked object bodies, crash-safe atomic swaps, background garbage collection, path-style and virtual-hosted REST API (buckets, conditional/range GET, PUT, HEAD, DELETE, batch delete, copy, list v1/v2, multipart uploads, and checksums)
- **SQS** (`sqs`): one `queue` B+tree dataset per standard queue, AWS JSON 1.0 protocol (`X-Amz-Target: AmazonSQS.*`), long polling, visibility timeouts, batch ops, tags, purge, and cross-node dead-letter queue redrive (no FIFO or legacy XML query protocol)
- **AWS Signature V4** (`sigv4`): header and presigned-URL verification, `aws-chunked` streaming payload/trailer signatures, and outbound SigV4 request signing
- **Stateless access keys** (`frontend::keys`, `frontend::routes`): derives `RDK1…` S3/SQS credentials from RAIDish tokens or admin certs via `POST /api/v1/services/keys` with no per-user database
- **Multi-node frontend** (`frontend`): HTTPS (HTTP/1.1 + HTTP/2) listeners for S3 (default port `9000`) and SQS (default port `9324`), per-dataset mTLS listeners for `etcd`, and transparent peer proxying

## Architecture & Module Map

```text
HTTPS :9000 (S3) / :9324 (SQS) / mTLS (etcd)
        │
        ▼
  frontend (locate dataset holder via cluster leases)
        ├── local  ──► s3::Bucket / sqs::Queue / raidish-etcd (ds/<name> writer)
        └── remote ──► TLS proxy (S3/SQS) or mTLS forward (etcd) ──► owner node
```

| Module | Purpose |
|---|---|
| `s3` | `Bucket` B+tree storage engine (`mod`), object checksums (`checksum`), and S3 REST API (`http`) |
| `sqs` | `Queue` B+tree storage engine (`mod`) and AWS JSON 1.0 SQS API (`http`) |
| `sigv4` | AWS Signature Version 4 verification (headers, presigned URLs, `aws-chunked`) and outbound signing |
| `frontend` | `ServicesConfig`, `Frontend` routing/proxying (`mod`), shared `hyper` HTTPS server & payload reader (`http`), stateless `RDK1` key derivation (`keys`), and Rocket key-minting route (`routes`) |

## How It Works

### Routing & Transparent Proxying

Every node listens on the configured service ports and tracks which node owns each dataset from the cluster's `ds/<name>` leases:
- **S3 & SQS**: Requests for a bucket or queue owned by another node are proxied over TLS with headers (`Host` included) and body intact so SigV4 verification succeeds on the owner (`x-raidish-forwarded` prevents loops). Bucket/queue listing and creation can be served by any node.
- **etcd**: Every node listens on each `etcd` dataset's port requiring a client certificate from the cluster CA (`admin`/`node` may write; `readonly` reads only). Connections to a non-owner node are forwarded over mTLS with a `RAIDISH-FWD/1 <dataset> <role>` preamble.

### Stateless S3/SQS Access Keys

S3 and SQS clients authenticate with access keys minted from a RAIDish token or admin certificate (`raidish token s3-key` → `POST /api/v1/services/keys`):
- The access key ID (`RDK1<token_id><role><expiry>`) embeds its claims directly.
- The secret is recomputed on any node as `HMAC-SHA256(ClusterState::service_key, access_key_id)`.
- Revoking a token in `auth.revoked` immediately invalidates every S3/SQS key derived from it, and keys derived from a `readonly` token are restricted to read operations.

### Configuration

Enable S3/SQS listeners in a node's config (`0` disables a port; TLS must be enabled for service ports):

```yaml
services:
  bind: 0.0.0.0         # default
  # advertise: node0    # address peers use to proxy here (default: hostname)
  s3_port: 9000         # default (0 to disable)
  # s3_domain: s3.local # optional virtual-hosted bucket suffix
  sqs_port: 9324        # default (0 to disable)
  region: us-east-1     # default
```

## Building & Testing

From the repository root:

```bash
# Check build
cargo check -p raidish-services

# Run the raidish-services unit test suite
cargo test -p raidish-services
```
