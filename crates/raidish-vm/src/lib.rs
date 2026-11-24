//! VMs for RAIDish: libvirt domain XML (`xml`), what raidish accepts and
//! how it runs a domain on this host (`vm`, through `virsh`), the pool's
//! store of VM definitions (`vm_store`), and running helper programs
//! (`proc`).

pub(crate) use raidish_core::{btree, dataset, disk, fs, service_tree};
#[cfg(test)]
pub(crate) use raidish_core::{test_util, vdev};
pub(crate) use raidish_types::{api_types, types, vm_def};

pub mod proc;
pub mod vm;
pub mod vm_store;
pub mod xml;
