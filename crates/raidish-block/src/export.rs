//! Exporting volumes as block devices, whatever the protocol: the table
//! of exports both servers keep (`ExportTable`), how an export's
//! connections are wound down (`ExportLife`), and the contract the agent
//! drives them through (`BlockExporter`). NBD (`crate::nbd`) and
//! NVMe/TCP (`crate::nvme`) differ only in their wire protocol and in
//! what they call an export (a name, a subsystem NQN).

use crate::fs::FileSystemError;
use crate::peer_io::PeerExport;
use crate::volume_io::{SwitchableIo, VolumeIo};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, watch};

/// How long removing an export waits for its connections to wind down.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// The agent's view of a block server.
#[async_trait::async_trait]
pub trait BlockExporter: Send + Sync {
    /// Export volume `volume`, written by this node.
    async fn add_export(&self, volume: &str) -> Result<(), FileSystemError>;
    /// Export `volume`, `size` bytes, with its I/O going to `io`.
    fn add_export_io(
        &self,
        volume: &str,
        size: u64,
        io: Arc<dyn VolumeIo>,
    ) -> Result<(), FileSystemError>;
    /// The backend of `volume`'s export, to pause or switch it.
    fn export_io(&self, volume: &str) -> Option<Arc<SwitchableIo>>;
    /// `volume`'s export for another node's proxy (see `crate::peer_io`):
    /// its connections count as the export's, so removing it drains them.
    fn peer_export(&self, volume: &str) -> Option<PeerExport>;
    /// Stop exporting `volume`: new connections are refused, current ones
    /// closed once their in-flight requests finish, then everything is
    /// committed. False: it wasn't exported.
    async fn remove_export(&self, volume: &str) -> Result<bool, FileSystemError>;
    /// The exported volumes (what tests check).
    #[cfg(test)]
    fn exports(&self) -> Vec<String>;
}

/// One export, as the table sees it.
pub(crate) trait Exported: Send + Sync + 'static {
    fn volume(&self) -> &str;
    fn size(&self) -> u64;
    fn life(&self) -> &ExportLife;
    fn io(&self) -> &Arc<SwitchableIo>;
}

/// A server's exports, by its protocol's name for them (`key` of the
/// volume).
pub(crate) struct ExportTable<E> {
    by_key: StdMutex<BTreeMap<String, Arc<E>>>,
    key: fn(&str) -> String,
    /// Names the server in log messages.
    label: &'static str,
}

impl<E: Exported> ExportTable<E> {
    pub fn new(label: &'static str, key: fn(&str) -> String) -> Self {
        ExportTable {
            by_key: Default::default(),
            key,
            label,
        }
    }

    pub fn insert(&self, export: E) -> Result<(), FileSystemError> {
        let k = (self.key)(export.volume());
        let mut m = self.by_key.lock().unwrap();
        if m.contains_key(&k) {
            return Err(FileSystemError::AlreadyExists);
        }
        m.insert(k, Arc::new(export));
        Ok(())
    }

    /// By the protocol's name for it.
    pub fn by_key(&self, key: &str) -> Option<Arc<E>> {
        self.by_key.lock().unwrap().get(key).cloned()
    }

    /// The only export, if there is exactly one.
    pub fn only(&self) -> Option<Arc<E>> {
        let m = self.by_key.lock().unwrap();
        match m.len() {
            1 => m.values().next().cloned(),
            _ => None,
        }
    }

    pub fn get(&self, volume: &str) -> Option<Arc<E>> {
        self.by_key(&(self.key)(volume))
    }

    /// Every export, in key order.
    pub fn all(&self) -> Vec<Arc<E>> {
        self.by_key.lock().unwrap().values().cloned().collect()
    }

    /// The protocol's names for the exports, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.by_key.lock().unwrap().keys().cloned().collect()
    }

    #[cfg(test)]
    pub fn volumes(&self) -> Vec<String> {
        self.all().iter().map(|e| e.volume().to_string()).collect()
    }

    pub fn export_io(&self, volume: &str) -> Option<Arc<SwitchableIo>> {
        Some(self.get(volume)?.io().clone())
    }

    pub fn peer_export(&self, volume: &str) -> Option<PeerExport> {
        let e = self.get(volume)?;
        Some(PeerExport {
            size: e.size(),
            io: e.io().clone(),
            session: e.life().join()?,
            from: crate::types::Epoch::NONE,
        })
    }

    pub async fn remove(&self, volume: &str) -> Result<bool, FileSystemError> {
        let k = (self.key)(volume);
        let Some(e) = self.by_key(&k) else {
            return Ok(false);
        };
        // Kept in the table until its connections are gone: a new export
        // of the volume meanwhile would be written alongside them.
        e.life().close(&format!("{}: {}", self.label, k)).await;
        let synced = e.io().sync().await;
        let mut m = self.by_key.lock().unwrap();
        if m.get(&k).is_some_and(|x| Arc::ptr_eq(x, &e)) {
            m.remove(&k);
        }
        drop(m);
        synced?;
        Ok(true)
    }
}

/// Lifetime of one export as seen by its connections: they `join` it when
/// they attach and watch for `close`, which waits until they have all gone.
pub(crate) struct ExportLife {
    closed: watch::Sender<bool>,
    alive: StdMutex<Option<mpsc::Sender<()>>>,
    drained: Mutex<mpsc::Receiver<()>>,
}

/// A connection's hold on an export; dropping it lets `close` finish.
pub struct ExportSession {
    _alive: mpsc::Sender<()>,
    closed: watch::Receiver<bool>,
}

impl ExportSession {
    /// Another handle on the close signal, for a different task to wait on.
    pub(crate) fn close_signal(&self) -> watch::Receiver<bool> {
        self.closed.clone()
    }
}

/// Resolves once the export is closing (never, for `None`).
pub(crate) async fn export_closed(rx: &mut Option<watch::Receiver<bool>>) {
    match rx {
        // An error means the export itself is gone: closed too.
        Some(rx) => drop(rx.wait_for(|closed| *closed).await),
        None => std::future::pending().await,
    }
}

impl ExportLife {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        ExportLife {
            closed: watch::Sender::new(false),
            alive: StdMutex::new(Some(tx)),
            drained: Mutex::new(rx),
        }
    }

    /// Attach a connection; `None` once the export is closing.
    pub(crate) fn join(&self) -> Option<ExportSession> {
        let alive = self.alive.lock().unwrap().clone()?;
        Some(ExportSession {
            _alive: alive,
            closed: self.closed.subscribe(),
        })
    }

    /// Refuse new connections, tell current ones to stop, and wait (up to
    /// `DRAIN_TIMEOUT`) for them to finish their in-flight requests.
    pub(crate) async fn close(&self, what: &str) {
        self.alive.lock().unwrap().take();
        self.closed.send_replace(true);
        let mut drained = self.drained.lock().await;
        if tokio::time::timeout(DRAIN_TIMEOUT, drained.recv())
            .await
            .is_err()
        {
            eprintln!("{}: connections did not close in {:?}", what, DRAIN_TIMEOUT);
        }
    }
}
