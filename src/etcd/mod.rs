//! An etcd v3 server over a B+tree dataset (format `etcd`): the KV, Watch,
//! Lease, Maintenance (status), Cluster (member list) and Auth (status)
//! services of etcd's gRPC API, so etcdctl, the client libraries and
//! etcd's users (Kubernetes...) work against it unchanged.
//!
//! **Data model.** etcd's: a revision counter for the whole keyspace,
//! bumped by every transaction that changes keys; each key's
//! create / mod revision, version and lease; the history of every key
//! since the last compaction, readable at any of those revisions and
//! replayed to watchers. All of it lives in the dataset's B+tree:
//!
//! ```text
//!   M rev / M compact          the current revision, the compacted one
//!   K <key>                    the key as of now (live keys only)
//!   H <esc key> 0x00 0x01 <main> <sub>   the key as of revision main.sub
//!                              (a put or a tombstone), oldest first
//!   R <main> <sub>             what changed at main.sub (the key): the
//!                              event log watchers replay
//!   L <lease id>               a lease (its TTL)
//!   A <lease id> <key>         a key attached to a lease
//! ```
//!
//! `esc` keeps keys in order and makes them prefix-free (0x00 → 0x00 0xff,
//! then 0x00 0x01 ends the key), so a key's history is contiguous and a
//! range of keys is a range of history.
//!
//! **Transactions.** Each request that writes (a put, a delete, a txn, a
//! lease revoke) is one B+tree transaction (see `crate::btree::Txn`):
//! applied at once, logged as one intent log entry, replayed all or none.
//! It's acknowledged once durable (`FileSystem::flush`: through the
//! dataset's intent log on its writer). Reads wait for every write they
//! can see to be durable first, so nothing a crash could undo is ever
//! read or watched. Watch events go out in revision order, once durable.
//!
//! **Where it runs.** One node serves a dataset at a time, the holder of
//! its `ds/<name>` lease (see `crate::frontend`). Leases' remaining time
//! lives there, in memory: a new server starts every lease's TTL afresh,
//! as etcd does on a leader change.

pub mod server;
pub mod store;

/// etcd's protobuf messages and gRPC services (from its `.proto` files,
/// see `proto/`).
// Generated whole from etcd's protos: not every message is used.
#[allow(clippy::all, clippy::pedantic, dead_code)]
pub mod pb {
    pub mod mvccpb {
        tonic::include_proto!("mvccpb");
    }
    pub mod authpb {
        tonic::include_proto!("authpb");
    }
    pub mod etcdserverpb {
        tonic::include_proto!("etcdserverpb");
    }
}

/// The etcd version this server answers as (its API is that version's).
pub const ETCD_VERSION: &str = "3.5.17";

#[cfg(test)]
#[path = "../tests/etcd_tests.rs"]
mod tests;
