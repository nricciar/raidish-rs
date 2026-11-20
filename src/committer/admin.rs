//! `PoolAdmin`: the pool's metadata operations (datasets, volumes,
//! snapshots, VM definitions, node facts, the activity log), as one
//! contract with three implementations:
//!
//! * `LocalAdmin`: this process's committer. It records what an admin
//!   changed in the activity log, so no caller can skip that.
//! * `CommitClient`: whichever node is the committer, over HTTP
//!   (`super::routes`, which are `LocalAdmin` again on that node).
//! * `Admin`: the committer here if this node is it, else over HTTP.

use super::*;
use crate::api_types::NodeFacts;
use crate::api_types::VmDef;
use crate::events::{EventPage, EventQuery};
use crate::send::SendHeader;
use crate::types::DatasetName;
use std::collections::BTreeMap;

/// The trait, and its `Admin` implementation (each method to `via()`),
/// from one list of signatures.
macro_rules! pool_admin {
    ($( $(#[$m:meta])* async fn $name:ident(&self $(, $arg:ident: $ty:ty)* $(,)? ) -> $ret:ty; )*) => {
        #[async_trait]
        pub trait PoolAdmin: Send + Sync {
            $( $(#[$m])* async fn $name(&self $(, $arg: $ty)*) -> $ret; )*
        }

        #[async_trait]
        impl PoolAdmin for Admin {
            $(
                async fn $name(&self $(, $arg: $ty)*) -> $ret {
                    match self.via().await {
                        // Ours stepping down (lease lost, slot not yet
                        // cleared): the new one may already be up.
                        Via::Here(a) => match a.$name($($arg),*).await {
                            Err(CommitError::Retry(_)) => self.there().$name($($arg),*).await,
                            r => r,
                        },
                        Via::There(c) => c.$name($($arg),*).await,
                    }
                }
            )*
        }
    };
}

pool_admin! {
    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError>;
    async fn create_volume(&self, name: &DatasetName, size_bytes: u64) -> Result<(), CommitError>;
    async fn delete_volume(&self, name: &str) -> Result<(), CommitError>;
    async fn set_props(&self, name: &str, props: &DatasetProps) -> Result<(), CommitError>;

    async fn datasets(&self) -> Result<Vec<DatasetInfo>, CommitError>;
    async fn create_dataset(&self, req: &CreateDataset) -> Result<(), CommitError>;
    async fn delete_dataset(&self, name: &str) -> Result<(), CommitError>;

    async fn snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, CommitError>;
    async fn create_snapshot(&self, name: &str, snap: &str) -> Result<Txg, CommitError>;
    async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), CommitError>;
    async fn clone_snapshot(&self, name: &str, snap: &str, new: &str) -> Result<(), CommitError>;
    /// Back to its newest snapshot.
    async fn rollback(&self, name: &str) -> Result<(), CommitError>;
    /// Whether a stream with `header` would be received into `name`.
    async fn recv_check(
        &self,
        name: &str,
        header: &SendHeader,
        force: bool,
    ) -> Result<(), CommitError>;

    async fn vms(&self) -> Result<BTreeMap<String, VmDef>, CommitError>;
    async fn put_vm(&self, req: &PutVm) -> Result<VmDef, CommitError>;
    /// `change`: refuse unless the stored one is that change.
    async fn delete_vm(&self, name: &str, change: Option<u64>) -> Result<(), CommitError>;

    async fn node_facts(&self) -> Result<BTreeMap<String, NodeFacts>, CommitError>;
    async fn put_node_facts(&self, facts: &NodeFacts) -> Result<(), CommitError>;

    async fn events(&self, q: &EventQuery) -> Result<EventPage, CommitError>;
    async fn report_events(&self, events: &[NewEvent]) -> Result<Vec<u64>, CommitError>;
}

// ---- in process ----

/// This process's committer, acting for `by` (None: the system itself).
pub struct LocalAdmin<D: BlockDevice> {
    pub committer: Arc<Committer<D>>,
    pub by: Option<String>,
}

impl<D: BlockDevice + 'static> LocalAdmin<D> {
    pub fn new(committer: Arc<Committer<D>>, by: Option<String>) -> Self {
        LocalAdmin { committer, by }
    }

    /// Record `message(v)` about `subject` if `r` succeeded.
    async fn noted<T>(
        &self,
        r: Result<T, FileSystemError>,
        kind: EventKind,
        subject: Subject,
        message: impl FnOnce(&T) -> String,
    ) -> Result<T, CommitError> {
        if let Ok(v) = &r {
            let m = message(v);
            self.committer
                .note(Level::Info, kind, subject, m, self.by.clone())
                .await;
        }
        r.map_err(CommitError::from)
    }

    /// Dataset `name`, as an event subject.
    async fn subject_of(&self, name: &str) -> Subject {
        Subject::dataset(self.committer.props_of(name).await.map(|p| p.0), name)
    }

    async fn vm_store(&self) -> Result<Arc<crate::vm_store::VmStore<D>>, CommitError> {
        Ok(self.committer.vm_store().await?)
    }

    /// Receive the send stream `r` into volume `name`. Not in the trait:
    /// a stream can't be sent twice, so there is no retrying it.
    pub async fn recv<R: tokio::io::AsyncRead + Unpin + Send>(
        &self,
        name: &str,
        r: &mut R,
        force: bool,
    ) -> Result<StreamReport, CommitError> {
        let r = self.committer.recv(name, r, force).await;
        let subject = self.subject_of(name).await;
        self.noted(r, EventKind::SnapshotReceived, subject, |rep| {
            format!(
                "'{}' received {} ({})",
                name,
                rep.snapshot,
                crate::events::bytes(rep.data_bytes)
            )
        })
        .await
    }
}

#[async_trait]
impl<D: BlockDevice + 'static> PoolAdmin for LocalAdmin<D> {
    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError> {
        Ok(self.committer.volumes().await?)
    }

    async fn create_volume(&self, name: &DatasetName, size_bytes: u64) -> Result<(), CommitError> {
        let r = self.committer.create_volume(name, size_bytes).await;
        self.noted(r, EventKind::VolumeCreated, Subject::volume(name), |_| {
            format!(
                "Volume '{}' created ({})",
                name,
                crate::events::bytes(size_bytes)
            )
        })
        .await
    }

    async fn delete_volume(&self, name: &str) -> Result<(), CommitError> {
        let r = self.committer.delete_volume(name).await;
        self.noted(r, EventKind::VolumeDeleted, Subject::volume(name), |_| {
            format!("Volume '{}' deleted", name)
        })
        .await
    }

    async fn set_props(&self, name: &str, props: &DatasetProps) -> Result<(), CommitError> {
        let before = self.committer.props_of(name).await;
        let r = self.committer.set_props(name, props.clone()).await;
        // Only a change worth saying is recorded.
        let what = before.and_then(|(f, old)| {
            let d = describe_props(&old, props);
            (!d.is_empty()).then_some((f, d))
        });
        match what {
            Some((f, d)) => {
                let label = crate::events::format_label(f);
                self.noted(
                    r,
                    EventKind::DatasetSettings,
                    Subject::dataset(Some(f), name),
                    |_| format!("{} '{}': {}", label, name, d),
                )
                .await
            }
            None => r.map_err(CommitError::from),
        }
    }

    async fn datasets(&self) -> Result<Vec<DatasetInfo>, CommitError> {
        Ok(self.committer.datasets().await?)
    }

    async fn create_dataset(&self, req: &CreateDataset) -> Result<(), CommitError> {
        let r = self.committer.create_dataset(req).await;
        let label = crate::events::format_label(req.format);
        self.noted(
            r,
            EventKind::DatasetCreated,
            Subject::dataset(Some(req.format), &req.name),
            |_| format!("{} '{}' created", label, req.name),
        )
        .await
    }

    async fn delete_dataset(&self, name: &str) -> Result<(), CommitError> {
        let f = self.committer.props_of(name).await.map(|p| p.0);
        let label = crate::events::label_of(f);
        let r = self.committer.delete_dataset(name).await;
        self.noted(
            r,
            EventKind::DatasetDeleted,
            Subject::dataset(f, name),
            |_| format!("{} '{}' deleted", label, name),
        )
        .await
    }

    async fn snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, CommitError> {
        Ok(self.committer.snapshots(name).await?)
    }

    async fn create_snapshot(&self, name: &str, snap: &str) -> Result<Txg, CommitError> {
        let r = self.committer.create_snapshot(name, snap).await;
        let subject = self.subject_of(name).await;
        self.noted(r, EventKind::SnapshotCreated, subject, |_| {
            format!("Snapshot '{}@{}' taken", name, snap)
        })
        .await
    }

    async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), CommitError> {
        let r = self.committer.destroy_snapshot(name, snap).await;
        let subject = self.subject_of(name).await;
        self.noted(r, EventKind::SnapshotDestroyed, subject, |_| {
            format!("Snapshot '{}@{}' destroyed", name, snap)
        })
        .await
    }

    async fn clone_snapshot(&self, name: &str, snap: &str, new: &str) -> Result<(), CommitError> {
        let r = self.committer.clone_snapshot(name, snap, new).await;
        let subject = self.subject_of(new).await;
        self.noted(r, EventKind::SnapshotCloned, subject, |_| {
            format!("'{}' cloned from '{}@{}'", new, name, snap)
        })
        .await
    }

    async fn rollback(&self, name: &str) -> Result<(), CommitError> {
        let r = self.committer.rollback(name).await;
        let subject = self.subject_of(name).await;
        self.noted(r, EventKind::SnapshotRollback, subject, |_| {
            format!("'{}' rolled back to its newest snapshot", name)
        })
        .await
    }

    async fn recv_check(
        &self,
        name: &str,
        header: &SendHeader,
        force: bool,
    ) -> Result<(), CommitError> {
        Ok(self.committer.recv_check(name, header, force).await?)
    }

    async fn vms(&self) -> Result<BTreeMap<String, VmDef>, CommitError> {
        Ok(self.vm_store().await?.list().await?)
    }

    async fn put_vm(&self, req: &PutVm) -> Result<VmDef, CommitError> {
        let r = self
            .vm_store()
            .await?
            .put(&req.name, &req.xml, req.create, req.change)
            .await;
        let kind = if req.create {
            EventKind::VmDefined
        } else {
            EventKind::VmChanged
        };
        self.noted(r, kind, Subject::vm(&req.name), |d| {
            format!("VM '{}' definition stored (change {})", d.name, d.change)
        })
        .await
    }

    async fn delete_vm(&self, name: &str, change: Option<u64>) -> Result<(), CommitError> {
        let r = self.vm_store().await?.delete(name, change).await;
        self.noted(r, EventKind::VmDeleted, Subject::vm(name), |_| {
            format!("VM '{}' deleted", name)
        })
        .await
    }

    async fn node_facts(&self) -> Result<BTreeMap<String, NodeFacts>, CommitError> {
        Ok(self.vm_store().await?.nodes().await?)
    }

    async fn put_node_facts(&self, facts: &NodeFacts) -> Result<(), CommitError> {
        Ok(self.vm_store().await?.put_node(facts).await?)
    }

    async fn events(&self, q: &EventQuery) -> Result<EventPage, CommitError> {
        Ok(self.committer.events().await?.read(q).await?)
    }

    async fn report_events(&self, events: &[NewEvent]) -> Result<Vec<u64>, CommitError> {
        Ok(self.committer.record(events.to_vec()).await?)
    }
}

// ---- here or there ----

/// The committer, wherever it is: in process while this node is it (no
/// HTTP to itself), else over HTTP.
#[derive(Clone)]
pub struct Admin {
    pub slot: CommitterSlot,
    pub client: CommitClient,
    by: Option<String>,
}

/// Which one a call goes to.
enum Via {
    Here(LocalAdmin<crate::vdev::Pool>),
    There(CommitClient),
}

impl Admin {
    pub fn new(slot: CommitterSlot, client: CommitClient) -> Self {
        Admin {
            slot,
            client,
            by: None,
        }
    }

    /// The same, acting for `who` (the activity log says it was theirs).
    pub fn on_behalf(&self, who: &crate::auth::Principal) -> Self {
        Admin {
            by: Some(who.name.clone()),
            ..self.clone()
        }
    }

    async fn via(&self) -> Via {
        match self.slot.read().await.clone() {
            Some(c) => Via::Here(LocalAdmin::new(c, self.by.clone())),
            None => Via::There(self.there()),
        }
    }

    /// The pool's committer over HTTP, acting for `by`.
    fn there(&self) -> CommitClient {
        match &self.by {
            Some(who) => self.client.on_behalf_of(who),
            None => self.client.clone(),
        }
    }
}
