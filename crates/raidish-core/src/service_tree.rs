//! What the pool's btree-backed services share: S3 buckets and SQS queues
//! (`ServiceTree`), the committer's own stores (`ensure_btree`), the
//! background loops they all run (`spawn_while_alive`), and how their
//! records are encoded.

use crate::btree::Txn;
use crate::dataset::{Dataset, DatasetFormat};
use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Proof that a dataset's operations lock (`Dataset::ops`) is held, for
/// reading or writing. Only a guard makes one, so a function taking it
/// can't be called outside the lock (audit X1).
pub struct OpsHeld<'a>(std::marker::PhantomData<&'a ()>);

impl<'a> OpsHeld<'a> {
    pub(crate) fn read(_g: &'a tokio::sync::RwLockReadGuard<'_, ()>) -> Self {
        OpsHeld(std::marker::PhantomData)
    }
}

/// Proof that a dataset's operations lock is held for writing: nothing
/// reads or writes the dataset meanwhile.
pub struct OpsWriteHeld<'a>(std::marker::PhantomData<&'a ()>);

impl<'a> OpsWriteHeld<'a> {
    pub fn of(_g: &'a tokio::sync::RwLockWriteGuard<'_, ()>) -> Self {
        OpsWriteHeld(std::marker::PhantomData)
    }
}

/// A service's errors: the pool's, and "not served here (any more)".
pub trait ServiceError: From<FileSystemError> {
    fn stopped() -> Self;
}

/// A dataset served from this node as one btree: stoppable, with writes
/// made durable before they're answered and reads that only ever see
/// durable writes.
pub struct ServiceTree<D: BlockDevice> {
    pub fs: Arc<FileSystem<D>>,
    pub name: String,
    format: DatasetFormat,
    stopped: AtomicBool,
    pub settler: Settler,
}

impl<D: BlockDevice + 'static> ServiceTree<D> {
    /// Serve `name` (a `format` dataset) of `fs`.
    pub async fn open(
        fs: Arc<FileSystem<D>>,
        name: &str,
        format: DatasetFormat,
    ) -> Result<Self, FileSystemError> {
        fs.open_btree(name, format).await?;
        Ok(ServiceTree {
            fs,
            name: name.to_string(),
            format,
            stopped: AtomicBool::new(false),
            settler: Settler::default(),
        })
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub async fn ds<E: ServiceError>(&self) -> Result<Arc<Dataset>, E> {
        if self.is_stopped() {
            return Err(E::stopped());
        }
        Ok(self.fs.open_btree(&self.name, self.format).await?)
    }

    /// Run a logged write, and make it durable.
    pub async fn write<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: ServiceError,
        F: for<'a, 'b> FnOnce(
            &'a mut Txn<'b, D>,
        ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
    {
        let ds = self.ds::<E>().await?;
        let out = {
            let _w = ds.ops.write().await;
            let mut t = Txn::begin(&self.fs, &ds.obj).await?;
            let out = f(&mut t).await?;
            if !t.is_empty() {
                t.apply_logged()?;
                self.settler.applied();
            }
            out
        };
        self.settler.settle(&self.fs, &self.name).await?;
        Ok(out)
    }

    /// Run a read (of durable state only).
    pub async fn read<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: ServiceError,
        F: for<'a> FnOnce(
            &'a Txn<'a, D>,
        ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
    {
        self.read_held(|t, _| f(t)).await
    }

    /// `read`, with proof the operations lock is held for the read.
    pub async fn read_held<T, E, F>(&self, f: F) -> Result<T, E>
    where
        E: ServiceError,
        F: for<'a> FnOnce(
            &'a Txn<'a, D>,
            &'a OpsHeld<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
    {
        let ds = self.ds::<E>().await?;
        let r = ds.ops.read().await;
        self.settler.settle(&self.fs, &self.name).await?;
        let t = Txn::begin(&self.fs, &ds.obj).await?;
        let held = OpsHeld::read(&r);
        f(&t, &held).await
    }
}

/// Where a store's writes are (applied, durable), so answers and reads
/// only ever reflect durable writes (as `crate::etcd::store` does).
#[derive(Default)]
pub struct Settler {
    applied: AtomicU64,
    durable: AtomicU64,
}

impl Settler {
    /// A write was applied (called with the dataset's operations
    /// write-locked).
    pub fn applied(&self) {
        self.applied.fetch_add(1, Ordering::SeqCst);
    }

    /// Make every applied write to dataset `name` durable.
    pub async fn settle<D: BlockDevice>(
        &self,
        fs: &FileSystem<D>,
        name: &str,
    ) -> Result<(), FileSystemError> {
        let target = self.applied.load(Ordering::SeqCst);
        if self.durable.load(Ordering::SeqCst) >= target {
            return Ok(());
        }
        fs.kv_settle().await?;
        fs.flush_for(name).await?;
        self.durable.fetch_max(target, Ordering::SeqCst);
        Ok(())
    }
}

/// Btree dataset `name` of `fs`, made empty if the pool has none yet (the
/// committer's own stores: `.vms`, `.events`).
pub async fn ensure_btree<D: BlockDevice>(
    fs: &FileSystem<D>,
    name: &str,
    format: DatasetFormat,
) -> Result<Arc<Dataset>, FileSystemError> {
    if !fs.datasets().await?.datasets.contains_key(name) {
        let blksz = fs.default_kv_blksz();
        match fs
            .create_btree(
                name,
                format,
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
    fs.open_btree(name, format).await
}

/// Something served that can be stopped.
pub trait Served: Send + Sync + 'static {
    fn is_stopped(&self) -> bool;
}

/// Run `work` on `svc` now and then every `period` (sooner when `wake` is
/// notified) for as long as it's alive and not stopped. The loop holds
/// `svc` only while `work` runs: a service nobody else holds is dropped,
/// and the loop ends.
pub fn spawn_while_alive<T, F, Fut>(
    svc: &Arc<T>,
    period: Duration,
    wake: Option<Arc<tokio::sync::Notify>>,
    work: F,
) where
    T: Served,
    F: Fn(Arc<T>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            let Some(s) = weak.upgrade() else { return };
            if s.is_stopped() {
                return;
            }
            work(s.clone()).await;
            // Stopped while it worked: don't wait out a period first.
            if s.is_stopped() {
                return;
            }
            drop(s); // not kept alive while waiting
            match &wake {
                Some(n) => {
                    let _ = tokio::time::timeout(period, n.notified()).await;
                }
                None => tokio::time::sleep(period).await,
            }
        }
    });
}

// ---- record encodings ----

/// A record in bincode (what the services' own records are).
pub fn enc_bin<T: Serialize>(v: &T) -> Vec<u8> {
    bincode::serialize(v).expect("encoding")
}

/// A bincode record; `what` names it if it's corrupt.
pub fn dec_bin<T: for<'a> Deserialize<'a>>(b: &[u8], what: &str) -> Result<T, FileSystemError> {
    bincode::deserialize(b).map_err(|e| corrupt(what, e))
}

/// A record in JSON (records an older or newer version may read too).
pub fn enc_json<T: Serialize>(v: &T) -> Result<Vec<u8>, FileSystemError> {
    serde_json::to_vec(v).map_err(|e| FileSystemError::Internal(e.to_string()))
}

/// A JSON record; `what` names it if it's corrupt.
pub fn dec_json<T: for<'a> Deserialize<'a>>(b: &[u8], what: &str) -> Result<T, FileSystemError> {
    serde_json::from_slice(b).map_err(|e| corrupt(what, e))
}

fn corrupt(what: &str, e: impl std::fmt::Display) -> FileSystemError {
    FileSystemError::Internal(format!("corrupt {}: {}", what, e))
}
