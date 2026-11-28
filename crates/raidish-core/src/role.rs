//! What a process may do with an open pool, in its type: a
//! `FileSystem<D, R>` is opened as one of three roles, and keeps it.
//!
//! * [`Committer`]: the process that owns the pool. It allocates, commits,
//!   repairs, and lends metaslabs to writers. A standalone (embedded,
//!   single-machine) pool is one too: nothing here needs a cluster.
//! * [`Writer`]: a volume writer. It writes the records of the volumes it
//!   runs into metaslabs a committer lends it, and has the committer
//!   commit them.
//! * [`ReadOnly`]: inspection. It never writes to the pool, so it is safe
//!   to point at a pool another process is serving.
//!
//! The constructors pick the role: `CommitterFs::format`/`load`,
//! `WriterFs::load_writer`, `ReadOnlyFs::load_readonly`.
//!
//! What only the owner of a pool may do is only there on `CommitterFs`:
//!
//! ```no_run
//! # use raidish_core::{role::CommitterFs, types::DatasetName, vdev::Pool};
//! async fn repair(fs: &CommitterFs<Pool>, vol: &DatasetName) {
//!     fs.scrub_repair().await.unwrap();
//!     fs.fsck_repair().await.unwrap();
//!     fs.create_snapshot(vol, "s").await.unwrap();
//! }
//! ```
//!
//! An inspection open can check, never repair:
//!
//! ```compile_fail,E0599
//! # use raidish_core::{role::ReadOnlyFs, vdev::Pool};
//! async fn repair(fs: &ReadOnlyFs<Pool>) {
//!     fs.scrub_repair().await.unwrap();
//! }
//! ```
//!
//! nor change anything (writing is [`Mutable`]'s):
//!
//! ```compile_fail,E0599
//! # use raidish_core::{role::ReadOnlyFs, vdev::Pool};
//! async fn write(fs: &ReadOnlyFs<Pool>) {
//!     fs.mkdir(std::path::Path::new("/d")).await.unwrap();
//! }
//! ```
//!
//! not even below the namespace, a record at a time:
//!
//! ```compile_fail,E0599
//! # use raidish_core::{role::ReadOnlyFs, vdev::Pool};
//! async fn write(fs: &ReadOnlyFs<Pool>) {
//!     fs.write_record(&[0u8; 4096]).await.unwrap();
//! }
//! ```
//!
//! and a volume writer leaves snapshots to the committer:
//!
//! ```compile_fail,E0599
//! # use raidish_core::{role::WriterFs, types::DatasetName, vdev::Pool};
//! async fn snap(fs: &WriterFs<Pool>, vol: &DatasetName) {
//!     fs.create_snapshot(vol, "s").await.unwrap();
//! }
//! ```

use crate::disk::PoolDevice;
use crate::fs::{Commits, FileSystem};
use crate::writer::WriterState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

/// A pool role (see the module docs). Sealed: these three are all there
/// is. A role is a value: the state only that role has lives in it.
pub trait Role: sealed::Sealed + Sized + Send + Sync + 'static {
    /// The writer's state, for the shared low-level paths (allocation,
    /// pinning lent space) that differ on a writer. Fixed by the role:
    /// `None` on every other, so each instantiation folds the check away.
    #[doc(hidden)]
    fn writer(&self) -> Option<&Arc<WriterState>> {
        None
    }

    /// `fs` as its own role's type (see [`View`]).
    #[doc(hidden)]
    fn view<D: PoolDevice>(fs: &FileSystem<D, Self>) -> View<'_, D>;
}

/// A pool as the type of the role it was opened as. Each role's `view`
/// returns its own arm, so in each instantiation a `match` on it has one
/// live arm: what differs by role is written once, in place, and the arm
/// can call what only that role has.
#[doc(hidden)]
pub enum View<'a, D: PoolDevice> {
    Committer(&'a CommitterFs<D>),
    Writer(&'a WriterFs<D>),
    ReadOnly(&'a ReadOnlyFs<D>),
}

/// [`View`] for a role that changes the pool: no read-only arm.
#[doc(hidden)]
pub enum MutView<'a, D: PoolDevice> {
    Committer(&'a CommitterFs<D>),
    Writer(&'a WriterFs<D>),
}

/// A role that changes the pool: [`Committer`] or [`Writer`].
pub trait Mutable: Role {
    /// `fs` as its own role's type (see [`MutView`]).
    #[doc(hidden)]
    fn view_mut<D: PoolDevice>(fs: &FileSystem<D, Self>) -> MutView<'_, D>;

    /// The commits this process has made, its own or through a committer.
    #[doc(hidden)]
    fn commits(&self) -> &Commits;
}

/// The process that owns and commits the pool. Standalone, it is all
/// there is (see the module docs): nothing here needs a cluster.
#[derive(Debug)]
pub struct Committer {
    /// Next pool-wide object number (persisted in every uberblock). Only
    /// the committer hands them out.
    pub(crate) next_object: AtomicU64,
    /// Set while an uberblock's copies are being written, and left set if
    /// that fails (or is cancelled): some copy may have landed, so that
    /// TXG may be what an import picks, and the space-map slots it names
    /// must not be rewritten. A retry would rewrite them for the same TXG,
    /// with different contents (structural triage I-2). From then on this
    /// pool commits nothing; reopening it reads what's really there. A
    /// committer steps down on it.
    pub(crate) uberblock_in_doubt: AtomicBool,
    /// Per disk: how many times it has been marked degraded (this process).
    /// A resilver clears a disk only if no mark came while it scanned: one
    /// that did is a write the scan's snapshot doesn't have (audit D4).
    /// Only a committer clears a disk, so only it counts.
    pub(crate) degraded_marks: std::sync::Mutex<Vec<u64>>,
    /// The lowest TXG new blocks may be stamped with (see
    /// `raise_birth_floor`).
    pub(crate) birth_floor: AtomicU64,
    pub(crate) commits: Commits,
}

impl Committer {
    /// A committer's state before the pool is read, for a pool of `disks`
    /// leaf disks.
    pub(crate) fn new(disks: usize) -> Self {
        Committer {
            next_object: AtomicU64::new(1),
            uberblock_in_doubt: AtomicBool::new(false),
            degraded_marks: std::sync::Mutex::new(vec![0; disks]),
            birth_floor: AtomicU64::new(0),
            commits: Commits::default(),
        }
    }
}

/// A volume writer, committing through a committer: its sink, the volumes
/// it runs and the metaslabs lent to it (see `crate::writer`).
pub struct Writer {
    pub(crate) state: Arc<WriterState>,
    pub(crate) commits: Commits,
}

/// Inspection only: no allocation or commit state. It never writes (no
/// commits, no self-heal repairs), so it is safe to point at a pool
/// another process is serving.
#[derive(Debug, Default)]
pub struct ReadOnly;

impl Role for Committer {
    fn view<D: PoolDevice>(fs: &FileSystem<D, Self>) -> View<'_, D> {
        View::Committer(fs)
    }
}

impl Role for Writer {
    fn writer(&self) -> Option<&Arc<WriterState>> {
        Some(&self.state)
    }

    fn view<D: PoolDevice>(fs: &FileSystem<D, Self>) -> View<'_, D> {
        View::Writer(fs)
    }
}

impl Role for ReadOnly {
    fn view<D: PoolDevice>(fs: &FileSystem<D, Self>) -> View<'_, D> {
        View::ReadOnly(fs)
    }
}

impl Mutable for Committer {
    fn view_mut<D: PoolDevice>(fs: &FileSystem<D, Self>) -> MutView<'_, D> {
        MutView::Committer(fs)
    }

    fn commits(&self) -> &Commits {
        &self.commits
    }
}

impl Mutable for Writer {
    fn view_mut<D: PoolDevice>(fs: &FileSystem<D, Self>) -> MutView<'_, D> {
        MutView::Writer(fs)
    }

    fn commits(&self) -> &Commits {
        &self.commits
    }
}

impl<D: PoolDevice, R: Role> FileSystem<D, R> {
    /// This pool as its role's type (see [`View`]).
    pub(crate) fn by_role(&self) -> View<'_, D> {
        R::view(self)
    }
}

impl<D: PoolDevice, R: Mutable> FileSystem<D, R> {
    /// This pool as its role's type (see [`MutView`]).
    pub(crate) fn mutable(&self) -> MutView<'_, D> {
        R::view_mut(self)
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Committer {}
    impl Sealed for super::Writer {}
    impl Sealed for super::ReadOnly {}
}

/// A pool opened by the process that owns it.
pub type CommitterFs<D> = crate::fs::FileSystem<D, Committer>;
/// A pool opened by a volume writer.
pub type WriterFs<D> = crate::fs::FileSystem<D, Writer>;
/// A pool opened for inspection.
pub type ReadOnlyFs<D> = crate::fs::FileSystem<D, ReadOnly>;
