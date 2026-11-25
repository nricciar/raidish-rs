//! What the block exports (NBD, NVMe/TCP) do I/O through.
//!
//! An export doesn't call the file system itself: it calls a [`VolumeIo`],
//! today always [`LocalIo`] (this node writes the volume: the file system's
//! block calls). Holding it as a [`SwitchableIo`] lets the backend be
//! swapped under live connections: a volume handed from one node to another
//! (`docs/design/mig-1-volume-handoff.md`) goes from being proxied to the
//! writer to being written here, without a client noticing more than a
//! pause.

use crate::disk::BlockDevice;
use crate::fs::{FileSystem, FileSystemError};
use crate::types::DatasetName;
use async_trait::async_trait;
use std::any::Any;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

/// How long a request on a retired backend waits for its replacement
/// before it fails after all.
const REPLACE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Block I/O on one volume.
#[async_trait]
pub trait VolumeIo: Send + Sync {
    /// Read at `offset` into `buf`; how much was read (the rest, past the
    /// volume's data, is the caller's to zero).
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError>;
    /// Write `data` at `offset`; how much was written.
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError>;
    /// Zero (deallocate) `len` bytes at `offset`.
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError>;
    /// Make every completed write durable (a client's FLUSH).
    async fn flush(&self) -> Result<(), FileSystemError>;
    /// Commit everything written (a client hanging up, an export removed).
    async fn sync(&self) -> Result<(), FileSystemError>;
    /// Taken out of service (see `crate::peer_io::PeerIo::retire`): its
    /// errors from now on mean "send this to the next backend", which a
    /// [`SwitchableIo`] does.
    fn retired(&self) -> bool {
        false
    }
}

/// This node writes the volume: straight to the file system.
pub struct LocalIo<D: BlockDevice> {
    fs: Arc<FileSystem<D>>,
    name: DatasetName,
}

impl<D: BlockDevice> LocalIo<D> {
    pub fn new(fs: Arc<FileSystem<D>>, name: &DatasetName) -> Self {
        LocalIo {
            fs,
            name: name.clone(),
        }
    }
}

#[async_trait]
impl<D: BlockDevice + Send + Sync + 'static> VolumeIo for LocalIo<D> {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.fs.block_read(&self.name, offset, buf).await
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.fs.block_write(&self.name, offset, data).await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.fs
            .block_zero(&self.name, offset, len)
            .await
            .map(|_| ())
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        // Through the intent log on a writer (see `crate::zil`).
        self.fs.flush_for(&self.name).await
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.fs.sync_for(&self.name).await
    }
}

/// A [`VolumeIo`] whose backend can be swapped while requests run.
///
/// Every request holds the backend for as long as it runs. [`pause`]
/// waits for those in flight to finish and holds back any new ones (the
/// lock is fair: a waiting pause queues new requests behind it) until the
/// returned [`Paused`] is dropped, with or without a new backend set.
///
/// [`pause`]: SwitchableIo::pause
pub struct SwitchableIo {
    backend: Arc<RwLock<Arc<dyn VolumeIo>>>,
    /// The next flush fails (see `poison`).
    lost: std::sync::atomic::AtomicBool,
    /// Nothing will replace the backend (see `no_replacement`).
    last: std::sync::atomic::AtomicBool,
}

/// Requests are held back while this lives; see [`SwitchableIo::pause`].
#[cfg(test)]
pub struct Paused<'a> {
    backend: tokio::sync::RwLockWriteGuard<'a, Arc<dyn VolumeIo>>,
}

#[cfg(test)]
impl Paused<'_> {
    /// Serve the held-back requests, and all later ones, from `io`.
    pub fn set(&mut self, io: Arc<dyn VolumeIo>) {
        *self.backend = io;
    }
    /// The backend the held-back requests would go to now.
    pub fn current(&self) -> Arc<dyn VolumeIo> {
        self.backend.clone()
    }
}

impl SwitchableIo {
    pub fn new(io: Arc<dyn VolumeIo>) -> Self {
        SwitchableIo {
            backend: Arc::new(RwLock::new(io)),
            lost: Default::default(),
            last: Default::default(),
        }
    }

    /// The backend is the last this export has (it's being taken down):
    /// once it's retired, its errors are final rather than waited out for
    /// a replacement for up to `REPLACE_WAIT`. Taking a dead proxy's export
    /// down otherwise stalled whoever did it that long.
    pub fn no_replacement(&self) {
        self.last.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn waits_for_replacement(&self, io: &dyn VolumeIo, since: std::time::Instant) -> bool {
        io.retired()
            && !self.last.load(std::sync::atomic::Ordering::SeqCst)
            && since.elapsed() <= REPLACE_WAIT
    }

    /// Writes this export acknowledged may be lost (the backend that took
    /// them is gone, and its replacement didn't get them in a clean
    /// handoff): fail the next flush, so whoever relies on it learns
    /// (audit A2).
    pub fn poison(&self) {
        self.lost.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn take_lost(&self) -> Result<(), FileSystemError> {
        if self.lost.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(Self::lost_error());
        }
        Ok(())
    }

    /// `take_lost`, leaving the poison for a flush to take.
    fn peek_lost(&self) -> Result<(), FileSystemError> {
        if self.lost.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Self::lost_error());
        }
        Ok(())
    }

    fn lost_error() -> FileSystemError {
        FileSystemError::Unavailable(
            "writes acknowledged by the volume's previous writer were lost with it".into(),
        )
    }

    /// Hold new requests back once those in flight have finished.
    #[cfg(test)]
    pub async fn pause(&self) -> Paused<'_> {
        Paused {
            backend: self.backend.write().await,
        }
    }

    /// Hold new requests back once those in flight have finished, until
    /// the result is dropped (held across other work).
    pub async fn pause_owned(&self) -> OwnedPaused {
        OwnedPaused {
            backend: self.backend.clone().write_owned().await,
        }
    }

    /// Serve requests from `io` from now on (after those in flight).
    #[cfg(test)]
    pub async fn switch(&self, io: Arc<dyn VolumeIo>) {
        self.pause().await.set(io);
    }
}

/// Requests are held back while this lives; see
/// [`SwitchableIo::pause_owned`].
pub struct OwnedPaused {
    backend: OwnedRwLockWriteGuard<Arc<dyn VolumeIo>>,
}

impl OwnedPaused {
    /// Serve the held-back requests, and all later ones, from `io`.
    pub fn set(&mut self, io: Arc<dyn VolumeIo>) {
        *self.backend = io;
    }
}

impl SwitchableIo {
    /// Run `op` on the current backend; if it fails because that backend
    /// was retired, run it again on the one that replaces it (waiting, if
    /// a pause is under way, for the switch).
    async fn run<'a, T, F>(&'a self, mut op: F) -> Result<T, FileSystemError>
    where
        F: FnMut(Arc<dyn VolumeIo>) -> futures::future::BoxFuture<'a, Result<T, FileSystemError>>,
    {
        let since = std::time::Instant::now();
        loop {
            let backend = self.backend.read().await;
            let io = backend.clone();
            let r = op(io.clone()).await;
            if r.is_ok() || !self.waits_for_replacement(&*io, since) {
                return r;
            }
            drop(backend);
            // Not replaced yet (the pause that replaces it is still waiting
            // for its turn): don't spin.
            if Arc::ptr_eq(&*self.backend.read().await, &io) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

#[async_trait]
impl VolumeIo for SwitchableIo {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        // `run`, spelled out: the buffer is lent to one attempt at a time.
        let since = std::time::Instant::now();
        loop {
            let backend = self.backend.read().await;
            let io = backend.clone();
            let r = io.read(offset, buf).await;
            if r.is_ok() || !self.waits_for_replacement(&*io, since) {
                return r;
            }
            drop(backend);
            if Arc::ptr_eq(&*self.backend.read().await, &io) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        self.run(|io| Box::pin(async move { io.write(offset, data).await }))
            .await
    }
    async fn zero(&self, offset: u64, len: usize) -> Result<(), FileSystemError> {
        self.run(|io| Box::pin(async move { io.zero(offset, len).await }))
            .await
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.run(|io| Box::pin(async move { io.flush().await }))
            .await?;
        self.take_lost()
    }
    /// Reports the poison but leaves it: only a client's FLUSH learns it
    /// for good. A sync is what a client hanging up (`NBD_CMD_DISC`) or an
    /// export going away does, whose error nobody relying on the writes
    /// sees: taken there, the next client's flush would succeed over the
    /// lost writes (structural triage I-24).
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.run(|io| Box::pin(async move { io.sync().await }))
            .await?;
        self.peek_lost()
    }
}

// ---- the background sync ----

/// How often dirty volume data is committed without an explicit FLUSH.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// File systems that already have a background sync loop, by address. Weak,
/// so a dropped file system (and its loop) doesn't pin the entry.
static SYNC_LOOPS: StdMutex<Vec<Weak<dyn Any + Send + Sync>>> = StdMutex::new(Vec::new());

/// Start the background TXG sync for `fs` unless one is already running.
/// Shared by every NBD server and NVMe/TCP target on the same file system;
/// the loop ends once the file system is dropped.
pub fn ensure_sync_loop<D>(fs: &Arc<FileSystem<D>>, who: &'static str)
where
    D: BlockDevice + Send + Sync + 'static,
{
    {
        let mut loops = SYNC_LOOPS.lock().unwrap();
        loops.retain(|w| w.strong_count() > 0);
        let ptr = Arc::as_ptr(fs) as *const ();
        if loops.iter().any(|w| w.as_ptr() as *const () == ptr) {
            return;
        }
        let any: Arc<dyn Any + Send + Sync> = fs.clone();
        loops.push(Arc::downgrade(&any));
    }
    let weak = Arc::downgrade(fs);
    let fold = fs.fold_wanted.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SYNC_INTERVAL);
        loop {
            // Every SYNC_INTERVAL, or early when an intent log fills up.
            tokio::select! {
                _ = tick.tick() => {}
                _ = fold.wait() => {}
            }
            let Some(fs) = weak.upgrade() else { return };
            if fs.has_dirty_datasets()
                && let Err(e) = fs.sync().await
            {
                eprintln!("{}: background sync failed: {}", who, e);
            }
            // Only now: flushes still appending to the old region while the
            // fold ran don't ask for another one. If the fold didn't move
            // the log on, the next flush asks again.
            fold.take();
            drop(fs);
        }
    });
}

#[cfg(test)]
#[path = "tests/volume_io_tests.rs"]
mod tests;
