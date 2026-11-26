use super::*;

#[test]
fn a_raw_range_is_checked_at_its_end() {
    let last = i64::MAX as u64 / BLOCK_SIZE as u64;
    // Starts addressable, ends past it: the start alone isn't I-9.
    assert!(RawDiskBlock::new(last).range(2).is_none());
    assert!(RawDiskBlock::new(last - 1).range(1).is_some());
    assert!(RawDiskBlock::new(u64::MAX).range(1).is_none());
    let r = RawDiskBlock::new(3).range(2).unwrap();
    assert_eq!((r.byte_offset(), r.byte_len()), (3 * 4096, 2 * 4096));
    assert_eq!(r.end(), RawDiskBlock::new(5));
}

#[test]
fn shards_sit_after_the_label_area() {
    assert_eq!(
        DiskShard::new(0).to_raw(),
        Some(RawDiskBlock::new(DISK_LABEL_BLOCKS))
    );
    assert_eq!(DiskShard::new(u64::MAX).to_raw(), None);
    assert_eq!(RawDiskBlock::new(1).to_shard(), None);
    assert_eq!(
        RawDiskBlock::new(DISK_LABEL_BLOCKS + 7).to_shard(),
        Some(DiskShard::new(7))
    );
}

#[test]
fn stripes_and_lbas_convert_both_ways() {
    let (s, i) = VdevLba::new(10).stripe_of(3);
    assert_eq!((s, i), (StripeId::new(3), 1));
    assert_eq!(s.first_lba(3), VdevLba::new(9));
    assert_eq!(s.shard(), DiskShard::new(3));
}

#[test]
fn disk_indexes_stay_below_max_disks() {
    assert!(PoolDiskIdx::new(MAX_DISKS - 1).is_some());
    assert!(PoolDiskIdx::new(MAX_DISKS).is_none());
    let mut m = DiskMask::EMPTY;
    let (a, b) = (PoolDiskIdx::new(0).unwrap(), PoolDiskIdx::new(63).unwrap());
    m.set(a);
    m.set(b);
    assert!(m.contains(a) && m.contains(b));
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![a, b]);
    m.clear(a);
    assert_eq!(m.len(), 1);
    let at = AtomicDiskMask::default();
    at.set(b, true);
    assert!(at.contains(b) && !at.contains(a));
}
