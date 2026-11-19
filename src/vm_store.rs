//! The cluster's VM definitions (see `crate::vm`): one system dataset per
//! pool (`.vms`, format `vms`), a B+tree of `V<name>` → definition,
//! written by the committer only. Next to them, `N<node>` → what that node
//! last reported about itself (`crate::node_facts`).
//!
//! Every definition is checked as it's stored, under one lock, so what's
//! stored always holds:
//!
//! * it parses, and holds only what keeps a guest inside its VM
//!   (`crate::vm::check_domain`);
//! * each raidish disk is an existing volume;
//! * no volume is two VMs' (two guests writing one disk corrupt it).
//!
//! It's reached only through the VM API (`crate::vms_api`, which passes
//! changes on to the committer): not through the file API or NFS, and its
//! dataset can't be listed, changed or deleted like a user's.
//!
//! A definition's `change` comes from one counter for the whole store, so
//! a VM deleted and defined again never reuses a number a running domain
//! was started from (see `VmSpec::change`).

use crate::btree::Txn;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::vm::{VmSpec, valid_name};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// The store's dataset.
pub const VMS_DATASET: &str = ".vms";
/// The longest definition stored.
pub const MAX_XML: usize = 1 << 20;
/// Definitions listed, at most (far more than a cluster runs).
const MAX_VMS: usize = 100_000;

const META: &[u8] = b"M";

/// A stored definition.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VmDef {
    pub name: String,
    pub xml: String,
    pub change: u64,
}

impl VmDef {
    pub fn spec(&self) -> VmSpec {
        VmSpec::parse(&self.name, &self.xml, self.change)
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    /// The last change number given out.
    change: u64,
}

fn key(name: &str) -> Vec<u8> {
    let mut k = b"V".to_vec();
    k.extend(name.as_bytes());
    k
}

fn json<T: Serialize>(v: &T) -> Result<Vec<u8>, FileSystemError> {
    serde_json::to_vec(v).map_err(|e| FileSystemError::InvalidArgument(e.to_string()))
}

fn parse<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T, FileSystemError> {
    serde_json::from_slice(b).map_err(|e| FileSystemError::InvalidArgument(format!("vms: {}", e)))
}

fn bad(m: String) -> FileSystemError {
    FileSystemError::InvalidArgument(m)
}

/// The store, opened on the committer's pool.
pub struct VmStore<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    ds: Arc<Dataset>,
    /// Changes one at a time: each checks against all the others.
    write: tokio::sync::Mutex<()>,
}

impl<D: BlockDevice> VmStore<D> {
    /// Open the pool's store, made empty if it has none yet.
    pub async fn open(fs: Arc<FileSystem<D>>) -> Result<Arc<Self>, FileSystemError> {
        if !fs.datasets().await?.datasets.contains_key(VMS_DATASET) {
            let blksz = fs.default_kv_blksz();
            match fs
                .create_btree(
                    VMS_DATASET,
                    DatasetFormat::Vms,
                    blksz,
                    crate::kv::NODE_CAPACITY,
                    Default::default(),
                )
                .await
            {
                Ok(_) | Err(FileSystemError::AlreadyExists) => {}
                Err(e) => return Err(e),
            }
        }
        let ds = fs.open_btree(VMS_DATASET, DatasetFormat::Vms).await?;
        Ok(Arc::new(VmStore {
            fs,
            ds,
            write: Default::default(),
        }))
    }

    async fn all(t: &Txn<'_, D>) -> Result<BTreeMap<String, VmDef>, FileSystemError> {
        t.range(Bound::Included(b"V"), Bound::Excluded(b"W"), MAX_VMS)
            .await?
            .iter()
            .map(|(_, v)| parse::<VmDef>(v).map(|d| (d.name.clone(), d)))
            .collect()
    }

    /// Every definition, by name.
    pub async fn list(&self) -> Result<BTreeMap<String, VmDef>, FileSystemError> {
        let _r = self.ds.ops.read().await;
        let t = Txn::begin(&self.fs, &self.ds.obj).await?;
        Self::all(&t).await
    }

    /// What each node last reported about itself, by name.
    pub async fn nodes(
        &self,
    ) -> Result<BTreeMap<String, crate::node_facts::NodeFacts>, FileSystemError> {
        let _r = self.ds.ops.read().await;
        let t = Txn::begin(&self.fs, &self.ds.obj).await?;
        t.range(Bound::Included(b"N"), Bound::Excluded(b"O"), MAX_VMS)
            .await?
            .iter()
            .map(|(_, v)| parse::<crate::node_facts::NodeFacts>(v).map(|f| (f.node.clone(), f)))
            .collect()
    }

    /// Store `facts` as what its node reports about itself.
    pub async fn put_node(
        &self,
        facts: &crate::node_facts::NodeFacts,
    ) -> Result<(), FileSystemError> {
        if facts.node.is_empty() || facts.node.len() > 64 {
            return Err(bad(format!("bad node name '{}'", facts.node)));
        }
        let mut k = b"N".to_vec();
        k.extend(facts.node.as_bytes());
        let _one = self.write.lock().await;
        {
            let _w = self.ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &self.ds.obj).await?;
            t.put(&k, &json(facts)?).await?;
            t.apply_logged()?;
        }
        self.fs.sync().await
    }

    /// The VM using `volume`, if one does.
    pub async fn user_of(&self, volume: &str) -> Result<Option<String>, FileSystemError> {
        Ok(self
            .list()
            .await?
            .into_values()
            .find(|d| d.spec().volumes.iter().any(|v| v == volume))
            .map(|d| d.name))
    }

    /// Store `xml` as VM `name`'s definition, once it checks out (see the
    /// module docs). `create`: refuse if there is one. `change`: refuse
    /// unless the one there is that change (it was changed since).
    pub async fn put(
        &self,
        name: &str,
        xml: &str,
        create: bool,
        change: Option<u64>,
    ) -> Result<VmDef, FileSystemError> {
        if !valid_name(name) {
            return Err(bad(format!(
                "bad VM name '{}' (letters, digits, '-', '_', '.')",
                name
            )));
        }
        if xml.len() > MAX_XML {
            return Err(bad(format!("a VM definition is at most {} bytes", MAX_XML)));
        }
        let spec = VmSpec::parse(name, xml, 0);
        if let Some(e) = spec.error {
            return Err(bad(format!("VM '{}': {}", name, e)));
        }
        for v in &spec.volumes {
            match self.fs.dataset_entry(v).await {
                Ok(e) if e.format == DatasetFormat::Block => {}
                Ok(e) => {
                    return Err(bad(format!(
                        "VM '{}': '{}' is a {} dataset, not a volume",
                        name,
                        v,
                        e.format.name()
                    )));
                }
                Err(_) => return Err(bad(format!("VM '{}': no volume '{}'", name, v))),
            }
        }

        let _one = self.write.lock().await;
        let def = {
            let _w = self.ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &self.ds.obj).await?;
            let all = Self::all(&t).await?;
            match (all.get(name), change) {
                (Some(_), _) if create => {
                    return Err(FileSystemError::Conflict(format!(
                        "VM '{}' already exists",
                        name
                    )));
                }
                (Some(d), Some(c)) if d.change != c => {
                    return Err(FileSystemError::Conflict(format!(
                        "VM '{}' was changed since (now change {}, not {}): reload it",
                        name, d.change, c
                    )));
                }
                (None, Some(_)) => {
                    return Err(FileSystemError::FileNotFound);
                }
                _ => {}
            }
            for other in all.values().filter(|d| d.name != name) {
                let theirs = other.spec().volumes;
                if let Some(v) = spec.volumes.iter().find(|v| theirs.contains(v)) {
                    return Err(FileSystemError::Conflict(format!(
                        "VM '{}': volume '{}' is VM '{}''s (a volume belongs to one VM)",
                        name, v, other.name
                    )));
                }
            }
            let mut meta: Meta = match t.get(META).await? {
                Some(b) => parse(&b)?,
                None => Meta::default(),
            };
            meta.change += 1;
            let def = VmDef {
                name: name.to_string(),
                xml: xml.to_string(),
                change: meta.change,
            };
            t.put(&key(name), &json(&def)?).await?;
            t.put(META, &json(&meta)?).await?;
            t.apply_logged()?;
            def
        };
        self.fs.sync().await?;
        Ok(def)
    }

    /// Delete VM `name`'s definition (`change`: only if it's that one).
    pub async fn delete(&self, name: &str, change: Option<u64>) -> Result<(), FileSystemError> {
        let _one = self.write.lock().await;
        {
            let _w = self.ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &self.ds.obj).await?;
            let Some(b) = t.get(&key(name)).await? else {
                return Err(FileSystemError::FileNotFound);
            };
            if let Some(c) = change {
                let d: VmDef = parse(&b)?;
                if d.change != c {
                    return Err(FileSystemError::Conflict(format!(
                        "VM '{}' was changed since (now change {}, not {})",
                        name, d.change, c
                    )));
                }
            }
            t.delete(&key(name)).await?;
            t.apply_logged()?;
        }
        self.fs.sync().await
    }
}

#[cfg(test)]
#[path = "tests/vm_store_tests.rs"]
mod tests;
