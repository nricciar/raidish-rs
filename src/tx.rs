//! Metadata transactions (audit S5a/b).
//!
//! A [`Tx`] is the commit lock, held: metadata changes (the object table,
//! frees, the allocations they reference) take `&mut Tx`, so none can be
//! made outside it, and no commit can capture one half done. It journals
//! each change, so an operation that fails partway is undone back to where
//! it started ([`Tx::check`]) instead of leaving its
//! first steps for the next commit to persist. Dropped with changes still
//! journaled (a caller that gave up: a dropped future), it undoes them
//! before letting the lock go.
//!
//! What a commit took is never undone: once `commit_locked` starts, the
//! journal is forgotten. A commit that fails leaves its operations applied,
//! for the next one (its uberblock may even have landed on some disks).
//! Likewise `Tx::applied` keeps what's been done for a later commit.

use crate::blkptr::BlockPtr;
use crate::metaslab::{MetaslabLayout, MetaslabState};
use crate::object::{Dirty, ObjTree};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

type Metaslabs = Arc<Vec<Arc<RwLock<MetaslabState>>>>;

/// One journaled change, and how to undo it.
pub(crate) enum Undo {
    /// The object table's dirty record `leaf` was this before.
    Record { leaf: u64, prior: Option<Dirty> },
    /// An extent was freed (it's in its metaslab's `freeing`).
    Freed { start: u64, len: u64 },
    /// An extent was allocated for something the operation references.
    Allocated { start: u64, len: u64 },
    /// Put an in-memory value back.
    Restore(Box<dyn FnOnce() + Send + Sync>),
}

/// Where an operation started in a [`Tx`]'s journal.
#[derive(Clone, Copy, Debug)]
struct Savepoint(usize);

/// The commit lock, held, and the changes made under it since the last
/// commit (see the module docs).
pub(crate) struct Tx {
    guard: Option<OwnedMutexGuard<()>>,
    lock: Arc<Mutex<()>>,
    undo: Vec<Undo>,
    /// In a commit: nothing it does is journaled (or ever undone).
    committing: bool,
    objects: Arc<ObjTree>,
    metaslabs: Metaslabs,
    layout: MetaslabLayout,
}

impl Tx {
    pub(crate) async fn begin(
        lock: &Arc<Mutex<()>>,
        objects: Arc<ObjTree>,
        metaslabs: Metaslabs,
        layout: MetaslabLayout,
    ) -> Tx {
        let guard = lock.clone().lock_owned().await;
        Tx {
            guard: Some(guard),
            lock: lock.clone(),
            undo: Vec::new(),
            committing: false,
            objects,
            metaslabs,
            layout,
        }
    }

    /// Whether this is `lock`'s transaction (a pool's, not another's).
    pub(crate) fn is_of(&self, lock: &Arc<Mutex<()>>) -> bool {
        Arc::ptr_eq(&self.lock, lock)
    }

    pub(crate) fn journal(&mut self, u: Undo) {
        if !self.committing {
            self.undo.push(u);
        }
    }

    /// Journal a value's restoration: `f` puts it back.
    pub(crate) fn on_rollback(&mut self, f: impl FnOnce() + Send + Sync + 'static) {
        self.journal(Undo::Restore(Box::new(f)));
    }

    /// Journal extents allocated for what the operation references.
    pub(crate) fn allocated(&mut self, extents: impl IntoIterator<Item = (u64, u64)>) {
        for (start, len) in extents {
            if len > 0 {
                self.journal(Undo::Allocated { start, len });
            }
        }
    }

    /// Collect the pointers an op allocates: journaled when the
    /// collection drops, however the op ends (a dropped future included).
    pub(crate) fn allocs(&mut self) -> Allocs<'_> {
        Allocs {
            tx: self,
            ptrs: Vec::new(),
        }
    }

    /// The changes so far stand (a later commit takes them).
    pub(crate) fn applied(&mut self) {
        self.undo.clear();
    }

    /// A commit starts: what it takes stands, and nothing it does is
    /// journaled, until `end_commit`.
    pub(crate) fn begin_commit(&mut self) {
        self.undo.clear();
        self.committing = true;
    }

    pub(crate) fn end_commit(&mut self) {
        self.committing = false;
    }

    /// Undo everything since `sp`.
    /// Cancel safe: an entry leaves the journal only as it's applied, so
    /// a rollback given up on partway leaves the rest for `Drop`.
    async fn rollback_to(&mut self, sp: Savepoint) {
        undo_to(
            &mut self.undo,
            sp.0,
            &self.objects,
            &self.metaslabs,
            self.layout,
        )
        .await;
    }

    /// An operation's result: on an error, undo everything since the last
    /// commit (or `applied`); otherwise it stands (`applied`).
    pub(crate) async fn check<T, E>(&mut self, r: Result<T, E>) -> Result<T, E> {
        match r {
            Ok(v) => {
                self.applied();
                Ok(v)
            }
            Err(e) => {
                self.rollback_to(Savepoint(0)).await;
                Err(e)
            }
        }
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        if self.undo.is_empty() {
            return;
        }
        // Given up on with changes outstanding: undo them, and only then
        // let the lock go (it moves into the task), so no commit sees them.
        let mut undo = std::mem::take(&mut self.undo);
        let guard = self.guard.take();
        let (objects, metaslabs, layout) =
            (self.objects.clone(), self.metaslabs.clone(), self.layout);
        let task = async move {
            undo_to(&mut undo, 0, &objects, &metaslabs, layout).await;
            drop(guard);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                h.spawn(task);
            }
            Err(_) => eprintln!("tx: dropped outside a runtime; its changes stay"),
        }
    }
}

/// Pointers allocated under a [`Tx`] (see [`Tx::allocs`]).
pub(crate) struct Allocs<'t> {
    tx: &'t mut Tx,
    pub(crate) ptrs: Vec<BlockPtr>,
}

impl Drop for Allocs<'_> {
    fn drop(&mut self) {
        let ptrs = std::mem::take(&mut self.ptrs);
        self.tx
            .allocated(ptrs.iter().map(|p| (p.extent().start, p.extent().len)));
    }
}

/// An extent allocated and not written yet: given back if dropped armed
/// (a write whose future was dropped), so a cancelled write leaks nothing.
pub(crate) struct Unwritten {
    start: u64,
    len: u64,
    metaslabs: Metaslabs,
    layout: MetaslabLayout,
}

impl Unwritten {
    pub(crate) fn new(start: u64, len: u64, metaslabs: Metaslabs, layout: MetaslabLayout) -> Self {
        Unwritten {
            start,
            len,
            metaslabs,
            layout,
        }
    }

    /// The write finished (or its caller takes the extent back itself).
    pub(crate) fn disarm(mut self) {
        self.len = 0;
    }
}

impl Drop for Unwritten {
    fn drop(&mut self) {
        let (start, len) = (self.start, self.len);
        if len == 0 {
            return;
        }
        let Some(ms) = metaslab(&self.metaslabs, self.layout, start) else {
            return;
        };
        // Now if the metaslab is free (the caller's tx, if any, is still
        // held: it drops after this), else as soon as it is.
        if let Ok(mut m) = ms.try_write() {
            m.unallocate(start, len);
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                h.spawn(async move { ms.write().await.unallocate(start, len) });
            }
            Err(_) => eprintln!("unwritten {{{}:{}}}: dropped outside a runtime", start, len),
        }
    }
}

/// Undo `undo` back to its first `keep` entries, newest first. Each entry
/// is popped only once whatever it needs is locked, and then applied with
/// no await between: dropped at any await, what's left is still in `undo`.
async fn undo_to(
    undo: &mut Vec<Undo>,
    keep: usize,
    objects: &ObjTree,
    metaslabs: &Metaslabs,
    layout: MetaslabLayout,
) {
    while undo.len() > keep {
        let ms = match undo.last() {
            Some(Undo::Freed { start, .. } | Undo::Allocated { start, .. }) => {
                metaslab(metaslabs, layout, *start)
            }
            _ => None,
        };
        let mut locked = match &ms {
            Some(m) => Some(m.write().await),
            None => None,
        };
        match undo.pop().expect("checked non-empty") {
            Undo::Record { leaf, prior } => objects.restore_record(leaf, prior),
            Undo::Freed { start, len } => {
                if let Some(m) = locked.as_mut()
                    && !m.unfree(start, len)
                {
                    eprintln!("tx: undoing free {{{}:{}}}: not pending", start, len);
                }
            }
            Undo::Allocated { start, len } => {
                if let Some(m) = locked.as_mut() {
                    m.unallocate(start, len);
                }
            }
            Undo::Restore(f) => f(),
        }
    }
}

fn metaslab(
    metaslabs: &Metaslabs,
    layout: MetaslabLayout,
    block: u64,
) -> Option<Arc<RwLock<MetaslabState>>> {
    let idx = layout.metaslab_for_block(block)?;
    metaslabs.get(idx.index()).cloned()
}
