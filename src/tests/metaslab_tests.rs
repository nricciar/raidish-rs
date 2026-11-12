use super::*;
use crate::test_util::{TOTAL_BLOCKS, TestPool};
use crate::vdev::Pool;

/// Alignment of the default test pool (raidz2: 3 data shards).
const ALIGN: u64 = crate::test_util::DATA_SHARDS as u64;
use std::path::{Path, PathBuf};

async fn free_map(fs: &FileSystem<Pool>) -> Vec<BTreeMap<u64, u64>> {
    let mut out = Vec::new();
    for ms in fs.metaslabs.iter() {
        out.push(ms.read().await.free_extents.clone());
    }
    out
}

async fn total_free(fs: &FileSystem<Pool>) -> (u64, u64) {
    let mut free = 0;
    let mut pending = 0;
    for ms in fs.metaslabs.iter() {
        let ms = ms.read().await;
        free += ms.free_blocks();
        pending += ms.pending_free_blocks();
    }
    (free, pending)
}

#[test]
fn layout_is_stripe_aligned_and_fits() {
    for total in [25_600u64, 500_000, 10_000_000, 100_000_000] {
        let l = MetaslabLayout::compute(total, crate::test_util::SPACEMAP_START, ALIGN);
        assert_eq!(l.metaslab_size_blocks % ALIGN, 0);
        assert_eq!(l.spacemap_slot_blocks % ALIGN, 0);
        assert_eq!(l.data_start % ALIGN, 0);
        assert!(l.metaslab_count > 0);
        assert!(l.data_start + l.metaslab_count * l.metaslab_size_blocks <= total);
        assert!(l.metaslab_size_blocks <= METASLAB_MAX_BLOCKS);
        // A slot must hold both bitmaps.
        let bitmap_bytes = l.metaslab_size_blocks.div_ceil(8);
        assert!(l.spacemap_slot_blocks * BLOCK_PAYLOAD_SIZE as u64 >= 2 * bitmap_bytes);
        // Slots of the last metaslab end where data begins.
        let last = l.slot_lba(l.metaslab_count as usize - 1, SPACEMAP_SLOTS - 1);
        assert_eq!(last + l.spacemap_slot_blocks, l.data_start);
    }
}

#[test]
fn encode_decode_round_trip() {
    let start = 3000;
    let mut ms = MetaslabState::new_empty(start, 1026, ALIGN);
    let a = ms.take_aligned(30).unwrap();
    let b = ms.take_aligned(60).unwrap();
    let c = ms.take_aligned(9).unwrap();
    ms.freeing.push(b.clone());
    ms.deferred.push_back((7, vec![c.clone()]));

    let bytes = ms.encode();
    let decoded = MetaslabState::decode(start, 1026, ALIGN, &bytes, 42);

    assert_eq!(decoded.free_extents, ms.free_extents);
    // freeing + deferred both come back as deferred, released at the given TXG.
    assert_eq!(decoded.deferred.len(), 1);
    let (release, extents) = &decoded.deferred[0];
    assert_eq!(*release, 42);
    let blocks: u64 = extents.iter().map(|e| e.len).sum();
    assert_eq!(blocks, b.len + c.len);
    // Allocated range is neither free nor deferred.
    assert!(!decoded.overlaps_released(a.start, a.len));
}

#[test]
fn insert_free_merges_neighbours() {
    let mut m = BTreeMap::new();
    MetaslabState::insert_free(&mut m, 10, 5);
    MetaslabState::insert_free(&mut m, 20, 5);
    MetaslabState::insert_free(&mut m, 15, 5);
    assert_eq!(m.into_iter().collect::<Vec<_>>(), vec![(10, 15)]);
}

#[tokio::test]
async fn txg_advances_and_survives_reload() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let t0 = fs.uberblock().await.unwrap().txg;
    assert_eq!(t0, 1, "format commits TXG 1");

    fs.write_file(Path::new("/a"), b"hello").await.unwrap();
    fs.write_file(Path::new("/b"), b"world").await.unwrap();
    let t1 = fs.uberblock().await.unwrap().txg;
    assert_eq!(t1, t0 + 2);
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(fs.uberblock().await.unwrap().txg, t1);
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), b"hello");
    assert_eq!(fs.read_file(Path::new("/b")).await.unwrap(), b"world");
}

#[tokio::test]
async fn allocation_does_no_io_until_commit() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let before = free_map(&fs).await;

    // Allocate without committing, then reload: the allocation must vanish.
    let got = fs.allocate_blocks_stripe_aligned(30).await.unwrap();
    assert_eq!(got.iter().map(|e| e.len).sum::<u64>(), 30);
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(free_map(&fs).await, before);
}

#[tokio::test]
async fn free_space_round_trips_through_reload() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;

    fs.mkdir(Path::new("/d")).await.unwrap();
    for i in 0..10 {
        let data = vec![i as u8; 10_000 + i * 997];
        fs.write_file(&PathBuf::from(format!("/d/f{}", i)), &data)
            .await
            .unwrap();
    }
    fs.write_file(Path::new("/d/f3"), b"overwritten")
        .await
        .unwrap();
    fs.delete(Path::new("/d/f5")).await.unwrap();

    let before = total_free(&fs).await;
    drop(fs);

    let fs = pool.load().await;
    // Space released in memory after the last commit is still recorded as
    // deferred on disk (it is persisted as free one TXG later), so a reload
    // may defer slightly more. Allocated space must match exactly.
    let after = total_free(&fs).await;
    assert_eq!(after.0 + after.1, before.0 + before.1);
    assert!(after.0 <= before.0);
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
    assert_eq!(
        fs.read_file(Path::new("/d/f3")).await.unwrap(),
        b"overwritten"
    );
    assert_eq!(
        fs.read_file(Path::new("/d/f9")).await.unwrap(),
        vec![9u8; 10_000 + 9 * 997]
    );
}

#[tokio::test]
async fn frees_are_deferred_for_txg_defer_size() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;

    let (free0, pending0) = total_free(&fs).await;
    fs.write_file(Path::new("/x"), &vec![1u8; 50_000])
        .await
        .unwrap();
    let (free1, _) = total_free(&fs).await;
    assert!(free1 < free0);

    // Overwrite: old data is freed in this TXG but must not be reusable yet.
    fs.write_file(Path::new("/x"), &vec![2u8; 50_000])
        .await
        .unwrap();
    let (_, pending) = total_free(&fs).await;
    assert!(pending > pending0, "old blocks should be deferred");

    // After TXG_DEFER_SIZE more commits, deferred space is released.
    for _ in 0..TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
    let (_, pending_after) = total_free(&fs).await;
    assert_eq!(pending_after, 0);
}

#[tokio::test]
async fn deferred_blocks_are_never_reallocated_early() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/x"), &vec![1u8; 20_000])
        .await
        .unwrap();

    let old = fs.get_file_at_path(Path::new("/x")).await.unwrap();
    let old_inode = fs.read_inode(&old).await.unwrap();
    let old_data = fs.content_extents(&old_inode.inode_type).await.unwrap();

    fs.delete(Path::new("/x")).await.unwrap();
    // Allocate nearly everything that's free; none may overlap the deferred blocks.
    let (free, _) = total_free(&fs).await;
    let got = fs
        .allocate_blocks_stripe_aligned(free / ALIGN * ALIGN)
        .await
        .unwrap();
    for g in &got {
        for o in &old_data {
            assert!(
                g.start + g.len <= o.start || o.start + o.len <= g.start,
                "reallocated deferred extent {:?} as {:?}",
                o,
                g
            );
        }
    }
}

#[tokio::test]
async fn uncommitted_slot_write_is_ignored() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/a"), b"data").await.unwrap();
    let committed = free_map(&fs).await;
    let txg = fs.uberblock().await.unwrap().txg;

    // Simulate a crash after writing the inactive slot for TXG+1 but before
    // the uberblock: claim metaslab 0 is completely free.
    let inactive = {
        let ms = fs.metaslabs[0].read().await;
        (ms.active_slot.unwrap() + 1) % SPACEMAP_SLOTS
    };
    let bogus = MetaslabState::new_empty(
        fs.layout.metaslab_start(0),
        fs.layout.metaslab_size_blocks,
        ALIGN,
    );
    fs.write_slot(fs.layout.slot_lba(0, inactive), txg + 1, &bogus.encode())
        .await
        .unwrap();
    fs.dev.flush().await.unwrap();
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(free_map(&fs).await, committed);
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), b"data");
}

#[tokio::test]
async fn multi_block_root_index_loads() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    // Enough root entries that the serialized index spans several blocks.
    for i in 0..120 {
        let name = format!("/a-fairly-long-file-name-to-grow-the-index-{:04}", i);
        fs.write_file(Path::new(&name), b"x").await.unwrap();
    }
    let root_bytes = crate::blkptr::object_size(&fs.root_ptr.lock().await);
    assert!(root_bytes > BLOCK_SIZE as u64);
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(fs.file_index.read().await.files.len(), 120);
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn unable_to_allocate_rolls_back() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let before = free_map(&fs).await;
    let err = fs.allocate_blocks_stripe_aligned(TOTAL_BLOCKS * 2).await;
    assert!(matches!(
        err,
        Err(FileSystemError::UnableToAllocateBlocks(_))
    ));
    assert_eq!(free_map(&fs).await, before);
}
