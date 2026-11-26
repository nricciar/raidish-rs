use super::*;
use crate::disks::DISK_LABEL_BLOCKS;
use crate::metaslab::MetaslabLayout;
use crate::types::{ep, ms};

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
fn raw(lba: u64) -> RawDiskBlock {
    RawDiskBlock::new(lba / 2 + DISK_LABEL_BLOCKS)
}

/// Raw block `b` itself.
fn blk(b: u64) -> RawDiskBlock {
    RawDiskBlock::new(b)
}

/// Pool disk `i`.
fn d(i: usize) -> PoolDiskIdx {
    PoolDiskIdx::new(i).unwrap()
}

/// The server's disk `i`.
fn sd(i: u16) -> ServedDiskIdx {
    ServedDiskIdx::new(i)
}

#[test]
fn domains_follow_metaslabs() {
    let m = map();
    assert_eq!(m.domain_of(d(0), blk(0)), Domain::Pool, "label");
    assert_eq!(m.domain_of(d(1), raw(39)), Domain::Pool, "space maps");
    assert_eq!(m.domain_of(d(2), raw(40)), Domain::Metaslab(ms(0)));
    assert_eq!(m.domain_of(d(0), raw(140)), Domain::Metaslab(ms(1)));
    assert_eq!(
        m.domain_of(d(0), raw(440)),
        Domain::Pool,
        "past the last metaslab"
    );
    assert_eq!(
        m.domains(d(0), raw(138), 2),
        vec![Domain::Metaslab(ms(0)), Domain::Metaslab(ms(1))]
    );
}

/// R1a: a range running past the last block still names the domain it
/// starts in (it once wrapped to nothing: no domain, nothing checked).
#[test]
fn a_range_past_the_end_still_has_a_domain() {
    let m = map();
    assert_eq!(m.domains(d(0), blk(u64::MAX), 0), vec![Domain::Pool]);
    assert_eq!(m.domains(d(0), blk(u64::MAX - 1), 8).len(), 1);
    assert_eq!(
        m.domains(d(0), raw(140), 0),
        vec![Domain::Metaslab(ms(1))],
        "count 0 is one block"
    );
}

fn fence() -> ServerFence {
    let f = ServerFence::default();
    f.set_map(Some(map()));
    f
}

/// A write of one block at `block`, stamped `stamp`: admitted?
fn writes(f: &ServerFence, block: RawDiskBlock, stamp: Stamp) -> bool {
    f.admit_stamp(sd(0), block, 1, stamp, true) == Admit::Yes
}

fn lend(e: u64) -> Stamp {
    Stamp::Lent(ep(e))
}

fn pool(e: u64) -> Stamp {
    Stamp::Pool(ep(e))
}

fn returned(e: u64) -> Stamp {
    Stamp::Returned(ep(e))
}

#[test]
fn lent_metaslab_is_fenced_by_its_lend() {
    let f = fence();
    let ms1 = raw(140);
    // Committer at pool epoch 3 writes metadata and metaslab 1.
    f.observe(ep(3), []);
    assert!(writes(&f, blk(DISK_LABEL_BLOCKS), pool(3)));
    assert!(writes(&f, ms1, pool(3)));
    // Metaslab 1 is lent at epoch 4: the committer can't write it any more,
    // the writer can.
    f.observe(ep(3), [(ms(1), Grant::Lent(ep(4)))]);
    assert!(!writes(&f, ms1, pool(3)));
    assert!(writes(&f, ms1, lend(4)));
    // The writer's epoch doesn't touch the pool domain or other metaslabs.
    assert_eq!(f.pool_floor(), 3);
    assert!(writes(&f, raw(40), pool(3)));
    // A new committer (pool epoch 5) doesn't fence the writer...
    f.observe(ep(5), []);
    assert!(writes(&f, ms1, lend(4)));
    // ...but does fence the old committer everywhere else.
    assert!(!writes(&f, raw(40), pool(3)));
    assert!(!writes(&f, blk(DISK_LABEL_BLOCKS), pool(3)));
    assert!(writes(&f, raw(40), pool(5)));
    // Re-lent to someone else at 6: the first writer is out.
    f.observe(ep(5), [(ms(1), Grant::Lent(ep(6)))]);
    assert!(!writes(&f, ms1, lend(4)));
    assert!(writes(&f, ms1, lend(6)));
    // Returned at 7: only the committer's return epoch (or newer) writes.
    f.observe(ep(5), [(ms(1), Grant::Returned(ep(7)))]);
    assert!(!writes(&f, ms1, lend(6)));
    assert!(!writes(&f, ms1, pool(5)));
    assert!(writes(&f, ms1, returned(7)));
}

/// R2b: a committer whose pool epoch is newer than a lend must not write
/// into the lent metaslab, and its refused write must not raise the
/// metaslab's floor past the writer's lend.
#[test]
fn a_newer_pool_epoch_is_fenced_out_of_lent_space() {
    let f = fence();
    let ms1 = raw(140);
    f.observe(ep(3), [(ms(1), Grant::Lent(ep(4)))]);
    f.observe(ep(9), []);
    assert_eq!(f.admit_stamp(sd(0), ms1, 1, pool(9), true), Admit::Fenced);
    assert_eq!(f.admit_stamp(sd(0), ms1, 1, pool(2), true), Admit::Fenced);
    assert!(writes(&f, ms1, lend(4)), "the writer is still in");
    // An old lend is stale, not fenced (it's the lend that moved on).
    f.observe(ep(9), [(ms(1), Grant::Lent(ep(10)))]);
    assert_eq!(f.admit_stamp(sd(0), ms1, 1, lend(4), true), Admit::Stale);
    // A lend-stamped read doesn't raise the pool floor.
    assert!(f.admit_stamp(sd(0), blk(DISK_LABEL_BLOCKS), 1, lend(12), false) == Admit::Yes);
    assert_eq!(f.pool_floor(), 9);
}

#[test]
fn a_lagging_server_learns_epochs_from_writes() {
    let f = fence();
    let ms1 = raw(140);
    // The server hasn't applied the lend at 4 yet: the writer's write
    // still goes through and raises the floor.
    f.observe(ep(3), []);
    assert!(writes(&f, ms1, lend(4)));
    assert!(
        !f.admit(sd(0), ms1, 1, ep(3), true),
        "the old committer is out"
    );
    // Reads never refuse, and only pool-domain reads raise the floor.
    assert!(f.admit(sd(0), ms1, 1, ep(9), false));
    assert_eq!(f.pool_floor(), 3);
    assert!(f.admit(sd(0), blk(DISK_LABEL_BLOCKS), 1, ep(9), false));
    assert_eq!(f.pool_floor(), 9);
}

#[test]
fn served_indices_map_to_pool_disks() {
    let f = fence();
    // This server's disk 0 is the pool's disk 2.
    f.set_served([(sd(0), d(2))].into_iter().collect());
    f.observe(ep(1), [(ms(0), Grant::Lent(ep(2)))]);
    assert!(!f.admit(sd(0), raw(40), 1, ep(1), true));
    // A disk this server serves outside the pool is the pool domain.
    assert!(f.admit(sd(5), raw(40), 1, ep(1), true));
}

#[test]
fn client_epochs_by_domain() {
    let c = ClientFence::default();
    c.set_pool_epoch(ep(3));
    assert_eq!(c.epoch_for(Some(d(0)), raw(140), 1), Stamp::Pool(ep(3)));
    c.set_map(map());
    c.set_lent(ms(1), ep(7));
    assert_eq!(c.epoch_for(Some(d(0)), raw(140), 1), Stamp::Lent(ep(7)));
    assert_eq!(c.epoch_for(Some(d(0)), raw(40), 1), Stamp::Pool(ep(3)));
    assert_eq!(
        c.epoch_for(Some(d(0)), blk(DISK_LABEL_BLOCKS), 1),
        Stamp::Pool(ep(3))
    );
    c.clear_metaslab(ms(1));
    assert_eq!(c.epoch_for(Some(d(0)), raw(140), 1), Stamp::Pool(ep(3)));
}

/// R2b: the pool domain admits only pool stamps, however new a lend or
/// return epoch is; a return stamp is the committer's, so it's fenced out
/// of lent space like a pool stamp.
#[test]
fn stamps_must_fit_their_domain() {
    let f = fence();
    f.observe(ep(3), [(ms(1), Grant::Lent(ep(4)))]);
    let label = blk(DISK_LABEL_BLOCKS);
    assert_eq!(f.admit_stamp(sd(0), label, 1, lend(9), true), Admit::Fenced);
    assert_eq!(
        f.admit_stamp(sd(0), label, 1, returned(9), true),
        Admit::Fenced
    );
    assert_eq!(f.pool_floor(), 3, "nor raised the floor");
    assert_eq!(
        f.admit_stamp(sd(0), raw(140), 1, returned(9), true),
        Admit::Fenced
    );
    // Returned at 5: the committer's return stamp writes, an older one is
    // stale.
    f.observe(ep(3), [(ms(1), Grant::Returned(ep(5)))]);
    assert_eq!(
        f.admit_stamp(sd(0), raw(140), 1, returned(4), true),
        Admit::Stale
    );
    assert!(writes(&f, raw(140), returned(5)));
    // A write spanning two domains must be split: one stamp is wrong for
    // one of them.
    assert_eq!(
        f.admit_stamp(sd(0), raw(138), 2, returned(5), true),
        Admit::Yes
    );
    assert_eq!(
        f.admit_stamp(sd(0), raw(38), 2, returned(5), true),
        Admit::Fenced
    );
    assert_eq!(
        f.admit_stamp(sd(0), raw(38), 2, pool(3), true),
        Admit::Stale
    );
}

/// R2b: a client's request across domains whose stamps differ goes out as
/// one request per run, each with its own stamp, and nothing in a plain
/// metaslab is stamped with another's (newer) epoch.
#[test]
fn a_client_splits_requests_by_stamp() {
    let c = ClientFence::default();
    c.set_pool_epoch(ep(3));
    c.set_map(map());
    // One raw block holds 2 pool blocks here: metaslab 1 is raw(140) to
    // raw(238), 50 raw blocks.
    let (m0, m1, m2) = (raw(40), raw(140), raw(240));
    assert_eq!(c.stamps(Some(d(0)), m0, 150), vec![(m0, 150, pool(3))]);
    c.set_returned(ms(1), ep(7));
    assert_eq!(
        c.stamps(Some(d(0)), raw(38), 102),
        vec![
            (raw(38), 51, pool(3)),
            (m1, 50, returned(7)),
            (m2, 1, pool(3))
        ],
    );
    c.set_lent(ms(2), ep(8));
    assert_eq!(
        c.stamps(Some(d(0)), m1, 51),
        vec![(m1, 50, returned(7)), (m2, 1, lend(8))],
    );
    // A return older than the pool epoch: the pool stamp covers it.
    c.set_pool_epoch(ep(9));
    assert_eq!(c.stamps(Some(d(0)), m1, 50), vec![(m1, 50, pool(9))]);
    // Not a pool disk, or nothing granted: one request.
    assert_eq!(c.stamps(None, m1, 51), vec![(m1, 51, pool(9))]);
    // Zero blocks (and a range past the end) are still one request, of
    // what was asked.
    assert_eq!(c.stamps(Some(d(0)), m2, 0), vec![(m2, 0, lend(8))]);
    let runs = c.stamps(Some(d(0)), blk(u64::MAX - 1), 4);
    assert_eq!(runs.iter().map(|r| r.1).sum::<u32>(), 4);
}

/// I-10: views of the cluster's grants can arrive out of order (the
/// startup read and each Raft apply). An older one never takes a grant
/// back: a stale `Returned(11)` arriving after `Lent(12)` would let pool
/// stamps into the writer's space. Nor does a view that predates a lend
/// (the metaslab missing from it) drop the grant.
#[test]
fn an_older_view_never_takes_a_grant_back() {
    let f = fence();
    let ms0 = raw(40);
    f.observe(ep(10), [(ms(0), Grant::Lent(ep(12)))]);
    assert!(!writes(&f, ms0, pool(13)), "pool stamp in lent space");
    // The older view lands after.
    f.observe(ep(10), [(ms(0), Grant::Returned(ep(11)))]);
    assert!(!writes(&f, ms0, pool(13)), "an older view revoked the lend");
    assert!(writes(&f, ms0, lend(12)));
    // A view from before any lend.
    f.observe(ep(10), std::iter::empty());
    assert!(
        !writes(&f, ms0, pool(13)),
        "a view without the grant dropped it"
    );
    // A newer return does move it on.
    f.observe(ep(10), [(ms(0), Grant::Returned(ep(14)))]);
    assert!(writes(&f, ms0, returned(14)));
    assert!(!writes(&f, ms0, lend(12)), "a returned lend still writes");
}
