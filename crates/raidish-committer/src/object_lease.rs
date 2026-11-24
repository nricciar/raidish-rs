//! Leases on single objects (`obj/<id>`): what a writer of one file
//! holds while it writes, whether the write came in over the files API
//! (`crate::files_api`) or NFS (`crate::nfs`).

use crate::cluster::lease::{HeldLease, LeaseClient};
use crate::types::{Holder, LeaseResource};
use crate::writer::ObjLease;
use async_trait::async_trait;
use std::sync::Arc;

/// A held `obj/<id>` lease.
#[async_trait]
pub trait ObjectGuard: Send + Sync {
    fn lease(&self) -> ObjLease;
    async fn release(&self);
}

/// Where uploads take their `obj/<id>` leases.
#[async_trait]
pub trait ObjectLeases: Send + Sync {
    /// Take `resource` (and keep renewing it until released), or say who has it.
    async fn take(
        &self,
        resource: LeaseResource,
        desc: Holder,
    ) -> Result<Box<dyn ObjectGuard>, String>;
}

struct Held(Arc<HeldLease>);

#[async_trait]
impl ObjectGuard for Held {
    fn lease(&self) -> ObjLease {
        ObjLease {
            owner: self.0.owner.clone(),
            epoch: self.0.epoch,
        }
    }
    async fn release(&self) {
        self.0.release().await
    }
}

#[async_trait]
impl ObjectLeases for LeaseClient {
    async fn take(
        &self,
        resource: LeaseResource,
        desc: Holder,
    ) -> Result<Box<dyn ObjectGuard>, String> {
        let held = Arc::new(HeldLease::acquire(self.clone(), resource, desc).await?);
        // If it's lost, the upload's commit is refused: nothing to do here.
        held.hold(|m| eprintln!("object lease: {}", m));
        Ok(Box::new(Held(held)))
    }
}
