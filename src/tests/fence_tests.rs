use super::*;
use crate::metaslab::MetaslabLayout;

fn map() -> FenceMap {
    // raidz1 over 3 disks (2 data shards), metaslabs of 100 blocks from 40.
    FenceMap::new(
        Topology::RaidZ {
            parity: 1,
            disks: vec![0, 1, 2],
        },
        MetaslabLayout {
            metaslab_count: 4,
            metaslab_size_blocks: 100,
            spacemap_start: 20,
            spacemap_slot_blocks: 2,
            data_start: 40,
            align: 2,
        },
    )
}

/// A raw block (label area included) holding pool block `lba`.
fn raw(lba: u64) -> u64 {
    lba / 2 + DISK_LABEL_BLOCKS
}

#[test]
fn domains_follow_metaslabs() {
    let m = map();
    assert_eq!(m.domain_of(0, 0), Domain::Pool, "label");
    assert_eq!(m.domain_of(1, raw(39)), Domain::Pool, "space maps");
    assert_eq!(m.domain_of(2, raw(40)), Domain::Metaslab(0));
    assert_eq!(m.domain_of(0, raw(140)), Domain::Metaslab(1));
    assert_eq!(
        m.domain_of(0, raw(440)),
        Domain::Pool,
        "past the last metaslab"
    );
    assert_eq!(
        m.domains(0, raw(138), 2),
        vec![Domain::Metaslab(0), Domain::Metaslab(1)]
    );
}

fn fence() -> ServerFence {
    let f = ServerFence::default();
    f.set_map(Some(map()));
    f
}

#[test]
fn lent_metaslab_is_fenced_by_its_lend() {
    let f = fence();
    let ms1 = raw(140);
    // Committer at pool epoch 3 writes metadata and metaslab 1.
    f.observe(3, []);
    assert!(f.admit(0, DISK_LABEL_BLOCKS, 1, 3, true));
    assert!(f.admit(0, ms1, 1, 3, true));
    // Metaslab 1 is lent at epoch 4: the committer can't write it any more,
    // the writer can.
    f.observe(3, [(1, true, 4)]);
    assert!(!f.admit(0, ms1, 1, 3, true));
    assert!(f.admit(0, ms1, 1, 4, true));
    // The writer's epoch doesn't touch the pool domain or other metaslabs.
    assert_eq!(f.pool_floor(), 3);
    assert!(f.admit(0, raw(40), 1, 3, true));
    // A new committer (pool epoch 5) doesn't fence the writer...
    f.observe(5, []);
    assert!(f.admit(0, ms1, 1, 4, true));
    // ...but does fence the old committer everywhere else.
    assert!(!f.admit(0, raw(40), 1, 3, true));
    assert!(!f.admit(0, DISK_LABEL_BLOCKS, 1, 3, true));
    assert!(f.admit(0, raw(40), 1, 5, true));
    // Re-lent to someone else at 6: the first writer is out.
    f.observe(5, [(1, true, 6)]);
    assert!(!f.admit(0, ms1, 1, 4, true));
    assert!(f.admit(0, ms1, 1, 6, true));
    // Returned at 7: only the committer's return epoch (or newer) writes.
    f.observe(5, [(1, false, 7)]);
    assert!(!f.admit(0, ms1, 1, 6, true));
    assert!(!f.admit(0, ms1, 1, 5, true));
    assert!(f.admit(0, ms1, 1, 7, true));
}

#[test]
fn a_lagging_server_learns_epochs_from_writes() {
    let f = fence();
    let ms1 = raw(140);
    // The server hasn't applied the lend at 4 yet: the writer's write
    // still goes through and raises the floor.
    f.observe(3, []);
    assert!(f.admit(0, ms1, 1, 4, true));
    assert!(!f.admit(0, ms1, 1, 3, true), "the old committer is out");
    // Reads never refuse, and only pool-domain reads raise the floor.
    assert!(f.admit(0, ms1, 1, 9, false));
    assert_eq!(f.pool_floor(), 3);
    assert!(f.admit(0, DISK_LABEL_BLOCKS, 1, 9, false));
    assert_eq!(f.pool_floor(), 9);
}

#[test]
fn served_indices_map_to_pool_disks() {
    let f = fence();
    // This server's disk 0 is the pool's disk 2.
    f.set_served([(0, 2)].into_iter().collect());
    f.observe(1, [(0, true, 2)]);
    assert!(!f.admit(0, raw(40), 1, 1, true));
    // A disk this server serves outside the pool is the pool domain.
    assert!(f.admit(5, raw(40), 1, 1, true));
}

#[test]
fn client_epochs_by_domain() {
    let c = ClientFence::default();
    c.set_pool_epoch(3);
    assert_eq!(c.epoch_for(Some(0), raw(140), 1), (3, false));
    c.set_map(map());
    c.set_metaslab_epoch(1, 7);
    assert_eq!(c.epoch_for(Some(0), raw(140), 1), (7, true));
    assert_eq!(c.epoch_for(Some(0), raw(40), 1), (3, false));
    assert_eq!(c.epoch_for(Some(0), DISK_LABEL_BLOCKS, 1), (3, false));
    c.clear_metaslab(1);
    assert_eq!(c.epoch_for(Some(0), raw(140), 1), (3, false));
}
