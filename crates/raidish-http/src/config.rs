//! The YAML config: which disks this node serves, and the pool layout.
//!
//! ```yaml
//! # 3-disk raidz1
//! pool:
//!   raidz1: [disks/d1, disks/d2, disks/d3]
//!
//! # 4 disks: two 2-way mirrors, striped (RAID10)
//! pool:
//!   stripe:
//!     - mirror: [http://n1:8881/api/v1/disks/0, http://n2:8881/api/v1/disks/0]
//!     - mirror: [http://n3:8881/api/v1/disks/0, http://n4:8881/api/v1/disks/0]
//! ```
//!
//! `raidz: { parity: N, disks: [...] }` takes any parity. A `disks:` list
//! with no `pool:` is what `listen` serves; for compatibility, exactly five
//! of them with no `pool:` also make a raidz2 pool, the old fixed layout.

use crate::cluster::ClusterConfig;
use crate::disk::DiskOptions;
use crate::vdev::Topology;
use serde::Deserialize;

#[derive(Deserialize, Debug, Default)]
pub struct Config {
    /// Disks this node serves with `listen` (and, if there are exactly five
    /// and no `pool`, the legacy raidz2 pool).
    #[serde(default)]
    pub disks: Option<Vec<String>>,
    /// Nodes to ask, for a config without `pool:` (a client's, as
    /// `raidish enroll` writes it): the CA and other API commands use them.
    /// Commands that open the pool itself still need `pool:`.
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// The pool layout.
    #[serde(default, with = "serde_yaml::with::singleton_map_recursive")]
    pub pool: Option<VdevConfig>,
    /// Control-plane settings (see `crate::cluster`). The members themselves
    /// come from `pool`.
    pub cluster: Option<ClusterConfig>,
    /// Remote disk request timeout (default 10000).
    pub disk_timeout_ms: Option<u64>,
    /// How `listen` exports and attaches the volumes placed on it.
    #[serde(default)]
    pub automount: crate::agent::AutomountConfig,
    /// Certificates (see `crate::tls`). Without it: plain HTTP, no auth.
    pub tls: Option<crate::tls::TlsConfig>,
    /// Who may do what once `tls` is on (see `crate::auth`).
    #[serde(default)]
    pub auth: crate::auth::AuthConfig,
    /// Serve the pool over NFS from `listen` (see `crate::nfs`). Absent: no
    /// NFS server.
    pub nfs: Option<crate::nfs::NfsConfig>,
    /// Serve etcd, S3 and SQS datasets (see `crate::frontend`). Absent:
    /// this node serves none (it still forwards nothing either).
    pub services: Option<crate::frontend::ServicesConfig>,
    /// Run VMs whose disks are volumes, through libvirt (see `crate::vm`).
    /// Absent: this node runs none.
    pub vm: Option<crate::vm::VmConfig>,
    /// The cluster CA (see `raidish_ca`).
    #[serde(default)]
    pub ca: CaConfig,
}

/// ```yaml
/// ca:
///   unseal_once: false   # ignore `ca init`'s one-shot unseal file
///   # This node's own key (`raidish ca slot add --node-key`), to unseal
///   # itself at start. One of:
///   unseal_credential: raidish-unseal   # $CREDENTIALS_DIRECTORY/<name>
///   unseal_key: /etc/raidish/unseal.key # a plain file: a copy of the
///                                       # disk opens the CA
/// ```
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CaConfig {
    /// Unseal at start from the one-shot file `ca init` leaves (default on).
    #[serde(default = "yes")]
    pub unseal_once: bool,
    #[serde(default)]
    pub unseal_key: Option<std::path::PathBuf>,
    #[serde(default)]
    pub unseal_credential: Option<String>,
}

fn yes() -> bool {
    true
}

impl Default for CaConfig {
    fn default() -> Self {
        CaConfig {
            unseal_once: true,
            unseal_key: None,
            unseal_credential: None,
        }
    }
}

impl CaConfig {
    /// Where this node's own key is, if anywhere.
    pub fn node_key(&self) -> Result<Option<raidish_ca::NodeKeySource>, String> {
        use raidish_ca::NodeKeySource;
        match (&self.unseal_key, &self.unseal_credential) {
            (Some(_), Some(_)) => Err("ca: unseal_key or unseal_credential, not both".into()),
            (Some(p), None) => Ok(Some(NodeKeySource::File(p.clone()))),
            (None, Some(c)) if c.is_empty() || c.contains('/') => {
                Err(format!("ca.unseal_credential '{}': a credential's name", c))
            }
            (None, Some(c)) => Ok(Some(NodeKeySource::Credential(c.clone()))),
            (None, None) => Ok(None),
        }
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VdevConfig {
    Raidz { parity: usize, disks: Vec<String> },
    Raidz1(Vec<String>),
    Raidz2(Vec<String>),
    Raidz3(Vec<String>),
    Mirror(Vec<String>),
    Stripe(Vec<StripeMember>),
}

/// A stripe member: a plain disk or a raidz/mirror group.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum StripeMember {
    Disk(String),
    Vdev(VdevConfig),
}

pub use crate::vdev::PoolSpec;

impl VdevConfig {
    pub fn to_spec(&self) -> Result<PoolSpec, String> {
        let mut paths = Vec::new();
        let topology = self.topology(&mut paths)?;
        let mut seen = std::collections::HashSet::new();
        for p in &paths {
            if !seen.insert(p) {
                return Err(format!("disk '{}' is used more than once", p));
            }
        }
        topology.validate(paths.len())?;
        Ok(PoolSpec { paths, topology })
    }

    fn topology(&self, paths: &mut Vec<String>) -> Result<Topology, String> {
        fn number(paths: &mut Vec<String>, disks: &[String]) -> Vec<usize> {
            disks
                .iter()
                .map(|d| {
                    paths.push(d.clone());
                    paths.len() - 1
                })
                .collect()
        }
        Ok(match self {
            VdevConfig::Raidz { parity, disks } => Topology::RaidZ {
                parity: *parity,
                disks: number(paths, disks),
            },
            VdevConfig::Raidz1(disks) => Topology::RaidZ {
                parity: 1,
                disks: number(paths, disks),
            },
            VdevConfig::Raidz2(disks) => Topology::RaidZ {
                parity: 2,
                disks: number(paths, disks),
            },
            VdevConfig::Raidz3(disks) => Topology::RaidZ {
                parity: 3,
                disks: number(paths, disks),
            },
            VdevConfig::Mirror(disks) => Topology::Mirror {
                disks: number(paths, disks),
            },
            VdevConfig::Stripe(members) => Topology::Stripe {
                children: members
                    .iter()
                    .map(|m| match m {
                        StripeMember::Disk(d) => {
                            paths.push(d.clone());
                            Ok(Topology::Disk(paths.len() - 1))
                        }
                        StripeMember::Vdev(v) => v.topology(paths),
                    })
                    .collect::<Result<_, String>>()?,
            },
        })
    }
}

impl Config {
    pub fn parse(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml).map_err(|e| format!("Failed to parse config: {}", e))
    }

    /// The pool layout, if one is configured.
    pub fn pool_spec(&self) -> Result<Option<PoolSpec>, String> {
        match (&self.pool, &self.disks) {
            (Some(pool), _) => pool.to_spec().map(Some),
            (None, Some(disks)) if disks.len() == 5 => {
                VdevConfig::Raidz2(disks.clone()).to_spec().map(Some)
            }
            _ => Ok(None),
        }
    }

    /// The disks `listen` serves: `disks`, or else the pool's.
    pub fn serve_disks(&self) -> Result<Vec<String>, String> {
        match (&self.disks, self.pool_spec()?) {
            (Some(disks), _) if !disks.is_empty() => Ok(disks.clone()),
            (_, Some(spec)) => Ok(spec.paths),
            _ => Err("no disks configured (set `disks` or `pool`)".into()),
        }
    }

    /// The cluster a `listen` node on `port` belongs to, from the pool's
    /// disks (see `cluster::members`). A cluster of one without a pool.
    pub fn cluster_members(&self, port: u16) -> Result<crate::cluster::members::Members, String> {
        match self.pool_spec()? {
            None => Ok(crate::cluster::members::Members::solo()),
            Some(spec) => crate::cluster::members::derive(
                &spec.paths,
                &format!("{:?}", spec.topology),
                |base| crate::cluster::members::is_local_base(base, port),
            ),
        }
    }

    /// Where a dashboard finds each pool disk when it asks the `listen`
    /// node on `port`: that node's own disks by their index in
    /// `serve_disks`, the others by URL. None without a pool.
    pub fn pool_map(&self, port: u16) -> Result<Option<crate::inspect::PoolMap>, String> {
        self.pool_map_with(|base| crate::cluster::members::is_local_base(base, port))
    }

    /// `pool_map`, with `is_me(base)` deciding which URLs are this node.
    pub(crate) fn pool_map_with(
        &self,
        is_me: impl Fn(&str) -> bool,
    ) -> Result<Option<crate::inspect::PoolMap>, String> {
        use crate::inspect::{DiskAddr, PoolMap};
        let Some(spec) = self.pool_spec()? else {
            return Ok(None);
        };
        let serve = self.serve_disks()?;
        let disks = spec
            .paths
            .iter()
            .map(|path| match disk_server_base(path) {
                Some(base) if !is_me(&base) => Ok(DiskAddr::Url(path.clone())),
                // A URL naming this node: the index is its last segment.
                Some(_) => path
                    .rsplit('/')
                    .next()
                    .and_then(|i| i.parse().ok())
                    .map(DiskAddr::This)
                    .ok_or_else(|| format!("disk '{}' must end in a disk index", path)),
                None => serve
                    .iter()
                    .position(|s| s == path)
                    .map(|i| DiskAddr::This(i as u16))
                    .ok_or_else(|| format!("local disk '{}' is not served by this node", path)),
            })
            .collect::<Result<_, String>>()?;
        Ok(Some(PoolMap {
            topology: spec.topology,
            disks,
        }))
    }

    /// Where to ask for leases: every `listen` node that serves one of the
    /// pool's remote disks. Empty for a pool of local disks only.
    /// Node `name`'s Raft data dir (`cluster.data_dir/<name>`).
    pub fn data_dir(&self, name: &str) -> std::path::PathBuf {
        match self.cluster.as_ref().and_then(|c| c.data_dir.as_deref()) {
            Some(d) => crate::tls::expand(d).join(name),
            None => crate::cluster::default_data_dir(name),
        }
    }

    /// The nodes to ask: the pool's disk servers, else `endpoints`.
    pub fn lease_endpoints(&self) -> Result<Vec<String>, String> {
        if self.pool_spec()?.is_none() {
            return Ok(self
                .endpoints
                .iter()
                .map(|e| e.trim_end_matches('/').to_string())
                .collect());
        }
        let mut out: Vec<String> = Vec::new();
        for path in self.pool_spec()?.map(|s| s.paths).unwrap_or_default() {
            let Some(base) = disk_server_base(&path) else {
                continue;
            };
            if !out.contains(&base) {
                out.push(base);
            }
        }
        Ok(out)
    }

    pub fn disk_options(&self) -> DiskOptions {
        let mut opts = DiskOptions {
            lock: true,
            ..DiskOptions::default()
        };
        if let Some(ms) = self.disk_timeout_ms {
            opts.request_timeout = std::time::Duration::from_millis(ms);
        }
        opts
    }
}

pub use crate::net::disk_server_base;

#[cfg(test)]
#[path = "tests/config_tests.rs"]
mod tests;
