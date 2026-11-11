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

use crate::api::EtcdConfig;
use crate::disk::DiskOptions;
use crate::disks::DiskSet;
use crate::vdev::{Pool, Topology};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Debug, Default)]
pub struct Config {
    /// Disks this node serves with `listen` (and, if there are exactly five
    /// and no `pool`, the legacy raidz2 pool).
    #[serde(default)]
    pub disks: Option<Vec<String>>,
    /// The pool layout.
    #[serde(default, with = "serde_yaml::with::singleton_map_recursive")]
    pub pool: Option<VdevConfig>,
    pub etcd_config: Option<EtcdConfig>,
    /// Remote disk request timeout (default 10000).
    pub disk_timeout_ms: Option<u64>,
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

/// A pool layout with its disks numbered: `topology` refers to `paths` by index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSpec {
    pub paths: Vec<String>,
    pub topology: Topology,
}

impl PoolSpec {
    /// Open the disks and build the pool.
    pub async fn open(&self, opts: DiskOptions) -> Pool {
        let paths: Vec<&str> = self.paths.iter().map(|s| s.as_str()).collect();
        let disks = Arc::new(DiskSet::new_with(&paths, opts).await);
        self.on(disks)
    }

    /// Build the pool over already opened `disks` (in `paths` order).
    pub fn on(&self, disks: Arc<DiskSet>) -> Pool {
        Pool::new(disks, self.topology.clone()).expect("validated topology")
    }
}

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

    pub fn disk_options(&self) -> DiskOptions {
        let mut opts = DiskOptions::default();
        if let Some(ms) = self.disk_timeout_ms {
            opts.request_timeout = std::time::Duration::from_millis(ms);
        }
        opts
    }
}

#[cfg(test)]
#[path = "tests/config_tests.rs"]
mod tests;
