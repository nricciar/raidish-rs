//! Exporting volumes as block devices, whatever the protocol: the table
//! of exports both servers keep (`ExportTable`), how an export's
//! connections are wound down (`ExportLife`), and the contract the agent
//! drives them through (`BlockExporter`), and the handle that owns one
//! (`ExportHandle`). NBD (`crate::nbd`) and
//! NVMe/TCP (`crate::nvme`) differ only in their wire protocol and in
//! what they call an export (a name, a subsystem NQN).

use crate::fs::FileSystemError;
use crate::peer_io::PeerExport;
use crate::types::DatasetName;
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
    /// Export volume `volume`, written by this node, for as long as the
    /// handle is held (see [`ExportHandle`]).
    async fn add_export(&self, volume: &DatasetName) -> Result<ExportHandle, FileSystemError>;
    /// Export `volume`, `size` bytes, with its I/O going to `io`.
    fn add_export_io(
        &self,
        volume: &DatasetName,
        size: u64,
        io: Arc<dyn VolumeIo>,
    ) -> Result<ExportHandle, FileSystemError>;
    /// The backend of `volume`'s export, to pause or switch it.
    fn export_io(&self, volume: &DatasetName) -> Option<Arc<SwitchableIo>>;
    /// `volume`'s export for another node's proxy (see `crate::peer_io`):
    /// its connections count as the export's, so removing it drains them.
    fn peer_export(&self, volume: &DatasetName) -> Option<PeerExport>;
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
    /// Shared with the handles, which take their own entry out.
    by_key: Arc<StdMutex<BTreeMap<String, Arc<E>>>>,
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

    /// Add `export`, for as long as the handle returned is held. Refused
    /// while the volume is exported, or still draining (see
    /// [`ExportHandle`]).
    pub fn insert(&self, export: E) -> Result<ExportHandle, FileSystemError> {
        let k = (self.key)(export.volume());
        let e = Arc::new(export);
        {
            let mut m = self.by_key.lock().unwrap();
            if m.contains_key(&k) {
                return Err(FileSystemError::AlreadyExists);
            }
            m.insert(k.clone(), e.clone());
        }
        let (map, mine, key) = (self.by_key.clone(), e.clone(), k.clone());
        Ok(ExportHandle {
            entry: Some(Entry {
                what: format!("{}: {}", self.label, k),
                export: e,
                unlist: Box::new(move || {
                    let mut m = map.lock().unwrap();
                    if m.get(&key).is_some_and(|x| Arc::ptr_eq(x, &mine)) {
                        m.remove(&key);
                    }
                }),
            }),
        })
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

    pub fn export_io(&self, volume: &DatasetName) -> Option<Arc<SwitchableIo>> {
        Some(self.get(volume)?.io().clone())
    }

    pub fn peer_export(&self, volume: &DatasetName) -> Option<PeerExport> {
        peer_export_of(&*self.get(volume)?)
    }
}

fn peer_export_of(e: &dyn Exported) -> Option<PeerExport> {
    Some(PeerExport {
        size: e.size(),
        io: e.io().clone(),
        session: e.life().join()?,
        from: crate::types::Epoch::NONE,
    })
}

/// One export, owned: it's served for as long as this is held.
///
/// [`ExportHandle::close`] takes it down: new connections are refused,
/// current ones closed once their in-flight requests finish, everything is
/// committed, and only then is it out of its server's table (a new export
/// of the volume meanwhile would be written alongside the old connections).
///
/// Dropped without `close`, it does the same in the background: refused and
/// signalled at once, drained, synced and unlisted by a task of its own.
/// Nothing else takes an export down, so none can be left behind by whoever
/// forgot it, nor taken down from under its owner.
#[must_use = "the export is taken down once its handle is dropped"]
pub struct ExportHandle {
    entry: Option<Entry>,
}

struct Entry {
    /// Names it in log messages (`nbd: <volume>`).
    what: String,
    export: Arc<dyn Exported>,
    /// Takes it out of its table (if it's still the one there).
    unlist: Box<dyn FnOnce() + Send + Sync>,
}

impl Entry {
    async fn close(self) -> Result<(), FileSystemError> {
        self.export.life().close(&self.what).await;
        let synced = self.export.io().sync().await;
        (self.unlist)();
        synced
    }
}

impl ExportHandle {
    fn entry(&self) -> &Entry {
        self.entry
            .as_ref()
            .expect("an export handle has its entry until closed")
    }

    /// Its backend, to pause, switch or poison.
    pub fn io(&self) -> &Arc<SwitchableIo> {
        self.entry().export.io()
    }

    /// It for another node's proxy (see `crate::peer_io`); None once closing.
    pub fn peer_export(&self) -> Option<PeerExport> {
        peer_export_of(&*self.entry().export)
    }

    /// Take it down (see the type's docs), and say whether its last sync
    /// went through.
    pub async fn close(mut self) -> Result<(), FileSystemError> {
        match self.entry.take() {
            Some(e) => e.close().await,
            None => Ok(()),
        }
    }
}

impl Drop for ExportHandle {
    fn drop(&mut self) {
        let Some(e) = self.entry.take() else {
            return;
        };
        // No new connections from now on, and the current ones are told.
        e.export.life().shut();
        eprintln!(
            "{}: export dropped unclosed; draining it in the background",
            e.what
        );
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    let what = e.what.clone();
                    if let Err(err) = e.close().await {
                        eprintln!("{}: last sync: {}", what, err);
                    }
                });
            }
            // No runtime: nothing runs its connections any more either.
            Err(_) => (e.unlist)(),
        }
    }
}

impl std::fmt::Debug for ExportHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.entry {
            Some(e) => write!(f, "ExportHandle({})", e.what),
            None => write!(f, "ExportHandle(closed)"),
        }
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

    /// Refuse new connections and tell current ones to stop.
    pub(crate) fn shut(&self) {
        self.alive.lock().unwrap().take();
        self.closed.send_replace(true);
    }

    /// `shut`, then wait (up to `DRAIN_TIMEOUT`) for the connections to
    /// finish their in-flight requests.
    pub(crate) async fn close(&self, what: &str) {
        self.shut();
        let mut drained = self.drained.lock().await;
        if tokio::time::timeout(DRAIN_TIMEOUT, drained.recv())
            .await
            .is_err()
        {
            eprintln!("{}: connections did not close in {:?}", what, DRAIN_TIMEOUT);
        }
    }
}
