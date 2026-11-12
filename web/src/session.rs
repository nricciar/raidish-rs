//! The pool, opened by the browser itself: the raidish core, compiled to
//! wasm, reading the disks over the nodes' websocket block protocol.

use raidish::disk::{DiskOptions, IoStats};
use raidish::disks::DiskSet;
use raidish::fs::FileSystem;
use raidish::inspect::{Layout, MetaslabReport, ObjectReport, PoolMap, PoolReport};
use raidish::vdev::{Pool, Topology};
use std::rc::Rc;
use std::sync::Arc;

/// Connections to every disk of the pool (kept across refreshes).
pub struct Session {
    pub disks: Arc<DiskSet>,
    pub topology: Topology,
}

/// One read-only look at the pool.
pub struct Snapshot {
    pub fs: FileSystem<Pool>,
    pub report: PoolReport,
    pub metaslabs: Vec<MetaslabReport>,
}

pub struct LayoutData {
    pub layout: Layout,
    pub objects: Vec<ObjectReport>,
    pub txg: u64,
}

/// `Rc` compared by identity, for component props: cheap, and a new
/// snapshot is always a new allocation.
pub struct Shared<T>(pub Rc<T>);

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Shared(self.0.clone())
    }
}

impl<T> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl<T> std::ops::Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub fn origin() -> String {
    web_sys::window()
        .and_then(|w| w.location().origin().ok())
        .unwrap_or_default()
}

pub fn hostname() -> String {
    web_sys::window()
        .and_then(|w| w.location().hostname().ok())
        .unwrap_or_else(|| "localhost".into())
}

pub fn now_ms() -> f64 {
    js_sys::Date::now()
}

pub fn tab_hidden() -> bool {
    web_sys::window()
        .and_then(|w| w.document())
        .is_some_and(|d| d.hidden())
}

impl Session {
    /// Ask this node where the disks are, and connect to all of them.
    pub async fn open() -> Result<Session, String> {
        let resp = gloo_net::http::Request::get("/api/v1/pool")
            .send()
            .await
            .map_err(|e| format!("asking the node for its pool: {}", e))?;
        if resp.status() == 404 {
            return Err("this node has no pool configured".into());
        }
        if !resp.ok() {
            return Err(format!("GET /api/v1/pool: HTTP {}", resp.status()));
        }
        let map: PoolMap = resp
            .json()
            .await
            .map_err(|e| format!("GET /api/v1/pool: {}", e))?;
        let urls = map.urls(&origin());
        let paths: Vec<&str> = urls.iter().map(String::as_str).collect();
        // A dashboard should notice a node coming back quickly.
        let opts = DiskOptions {
            reconnect_max: std::time::Duration::from_secs(5),
            ..DiskOptions::default()
        };
        let disks = Arc::new(DiskSet::new_with(&paths, opts).await);
        Ok(Session {
            disks,
            topology: map.topology,
        })
    }

    /// Load the newest committed state, read-only.
    pub async fn snapshot(&self) -> Result<Snapshot, String> {
        let pool = Pool::new(self.disks.clone(), self.topology.clone())?;
        let fs = FileSystem::load_readonly(pool)
            .await
            .map_err(|e| format!("cannot open the pool: {}", e))?;
        let report = fs.pool_report().await;
        let metaslabs = fs.metaslab_reports().await;
        Ok(Snapshot {
            fs,
            report,
            metaslabs,
        })
    }

    pub fn io_stats(&self) -> Vec<IoStats> {
        self.disks
            .disks
            .iter()
            .map(|c| c.io_stats().unwrap_or_default())
            .collect()
    }

    pub fn online(&self) -> Vec<bool> {
        (0..self.disks.len())
            .map(|d| self.disks.is_online(d))
            .collect()
    }
}

impl Snapshot {
    pub async fn layout(&self) -> LayoutData {
        let (layout, objects) = self.fs.layout().await;
        LayoutData {
            layout,
            objects,
            txg: self.report.txg,
        }
    }
}
