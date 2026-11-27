//! RAIDish's network services over service datasets: S3 (`s3`), SQS
//! (`sqs`), AWS Signature V4 for both (`sigv4`), and the frontend that
//! serves them and etcd on whichever node holds each dataset's lease
//! (`frontend`).

pub(crate) use raidish_auth::{auth, http, net, pki, tls};
pub(crate) use raidish_cluster as cluster;
pub(crate) use raidish_committer as committer;
#[cfg(test)]
pub(crate) use raidish_core::test_util;
pub(crate) use raidish_core::{btree, dataset, disk, fs, inode, kv, service_tree, vdev};
pub(crate) use raidish_etcd as etcd;
pub(crate) use raidish_types::types;
pub(crate) use raidish_vm::xml;

pub mod frontend;
pub mod s3;
pub mod sigv4;
pub mod sqs;
