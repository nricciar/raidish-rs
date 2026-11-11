use super::*;
use crate::raidz::RaidZ;
use crate::test_util::TestPool;
use std::path::Path;

const MB: u64 = 1024 * 1024;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 3) as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

async fn allocated(fs: &FileSystem<RaidZ>) -> u64 {
    let mut used = 0;
    for ms in fs.metaslabs.iter() {
        let ms = ms.read().await;
        used += ms.block_count - ms.free_blocks() - ms.pending_free_blocks();
    }
    used
}

/// Commit enough TXGs that every deferred free is released.
async fn settle(fs: &FileSystem<RaidZ>) {
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
}

async fn read(fs: &FileSystem<RaidZ>, offset: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = fs.block_read("vol", offset, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

#[test]
fn levels_cover_the_volume() {
    assert_eq!(levels_for(1), 1);
    assert_eq!(levels_for(INDIRECT_FANOUT), 1);
    assert_eq!(levels_for(INDIRECT_FANOUT + 1), 2);
    assert_eq!(levels_for(INDIRECT_FANOUT * INDIRECT_FANOUT), 2);
    assert_eq!(levels_for(INDIRECT_FANOUT * INDIRECT_FANOUT + 1), 3);
}

#[tokio::test]
async fn create_validates_arguments() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    for vbs in [0, 4096, 16384, MAX_VOLBLOCKSIZE + VOLBLOCK_ALIGN] {
        assert!(matches!(
            fs.create_block("v", MB, vbs).await,
            Err(FileSystemError::InvalidArgument(_))
        ));
    }
    fs.create_block("v", MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    assert!(matches!(
        fs.create_block("v", MB, DEFAULT_VOLBLOCKSIZE).await,
        Err(FileSystemError::AlreadyExists)
    ));
}

#[tokio::test]
async fn new_volume_is_sparse_and_reads_zeros() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let before = allocated(&fs).await;

    // A volume far larger than the pool is fine while it's empty.
    fs.create_block("vol", 64 * 1024 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    settle(&fs).await;
    // Only the inode and root index changed size, not the data.
    assert!(allocated(&fs).await <= before + 12);

    assert!(read(&fs, 0, 100_000).await.iter().all(|&b| b == 0));
    assert!(
        read(&fs, 40 * 1024 * MB, 4096)
            .await
            .iter()
            .all(|&b| b == 0)
    );
}

#[tokio::test]
async fn writes_visible_before_and_after_sync_and_reload() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", 8 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let a = pattern(4096, 1); // aligned, inside one record
    let b = pattern(70_000, 2); // unaligned, spans records
    let c = pattern(DEFAULT_VOLBLOCKSIZE as usize, 3); // exactly one record
    fs.block_write("vol", 8192, &a).await.unwrap();
    fs.block_write("vol", 100_003, &b).await.unwrap();
    fs.block_write("vol", 10 * DEFAULT_VOLBLOCKSIZE as u64, &c)
        .await
        .unwrap();

    async fn check(fs: &FileSystem<RaidZ>, a: &[u8], b: &[u8], c: &[u8]) {
        assert_eq!(read(fs, 8192, a.len()).await, a);
        assert_eq!(read(fs, 100_003, b.len()).await, b);
        assert_eq!(read(fs, 10 * DEFAULT_VOLBLOCKSIZE as u64, c.len()).await, c);
        // Neighbours untouched.
        assert_eq!(read(fs, 100_000, 3).await, vec![0, 0, 0]);
        assert_eq!(read(fs, 8192 + 4096, 10).await, vec![0; 10]);
    }

    check(&fs, &a, &b, &c).await; // from dirty records
    fs.sync().await.unwrap();
    check(&fs, &a, &b, &c).await; // from the tree
    drop(fs);

    let fs = pool.load().await;
    check(&fs, &a, &b, &c).await; // from disk after reload
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn partial_writes_merge_across_txgs() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let mut expect = vec![0u8; 3 * DEFAULT_VOLBLOCKSIZE as usize];
    for (i, off) in [0usize, 5000, 40_000, 61_000, 100_001]
        .into_iter()
        .enumerate()
    {
        let chunk = pattern(9000, i as u8 + 10);
        expect[off..off + chunk.len()].copy_from_slice(&chunk);
        fs.block_write("vol", off as u64, &chunk).await.unwrap();
        fs.sync().await.unwrap(); // each lands in its own TXG
    }
    drop(fs);

    let fs = pool.load().await;
    assert_eq!(read(&fs, 0, expect.len()).await, expect);
}

#[tokio::test]
async fn writes_are_clipped_at_end_of_volume() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let size = DEFAULT_VOLBLOCKSIZE as u64 * 3 + 1000;
    fs.create_block("vol", size, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let tail = pattern(5000, 4);
    assert_eq!(
        fs.block_write("vol", size - 1000, &tail).await.unwrap(),
        1000
    );
    assert_eq!(fs.block_write("vol", size, &tail).await.unwrap(), 0);
    fs.sync().await.unwrap();
    assert_eq!(read(&fs, size - 1000, 5000).await, tail[..1000]);
}

#[tokio::test]
async fn overwrites_are_copy_on_write_and_free_old_space() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", 4 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let first = pattern(2 * MB as usize, 1);
    fs.block_write("vol", 0, &first).await.unwrap();
    fs.sync().await.unwrap();

    let vol = fs.open_volume("vol").await.unwrap();
    let old_leaf = fs
        .tree_lookup(&vol, vol.root(), 0, 3)
        .await
        .unwrap()
        .unwrap();

    settle(&fs).await;
    let used_once = allocated(&fs).await;

    // Overwrite everything: new locations, old space comes back.
    let second = pattern(2 * MB as usize, 2);
    fs.block_write("vol", 0, &second).await.unwrap();
    fs.sync().await.unwrap();
    let new_leaf = fs
        .tree_lookup(&vol, vol.root(), 0, 3)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        old_leaf.lba, new_leaf.lba,
        "record must move (copy on write)"
    );
    assert!(new_leaf.birth_txg > old_leaf.birth_txg);

    settle(&fs).await;
    assert_eq!(allocated(&fs).await, used_once, "no leak after overwrite");
    assert_eq!(read(&fs, 0, second.len()).await, second);
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn trim_and_zero_writes_become_holes() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    fs.create_block("vol", 4 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    settle(&fs).await;
    let empty = allocated(&fs).await;

    fs.block_write("vol", 0, &pattern(MB as usize, 7))
        .await
        .unwrap();
    settle(&fs).await;
    assert!(allocated(&fs).await > empty);

    // Trim the first half, write zeros over the second half.
    let half = MB as usize / 2;
    fs.block_zero("vol", 0, half).await.unwrap();
    fs.block_write("vol", half as u64, &vec![0u8; half])
        .await
        .unwrap();
    settle(&fs).await;

    assert!(read(&fs, 0, MB as usize).await.iter().all(|&b| b == 0));
    // Everything is a hole again; at most a partial-record tail stays allocated.
    assert!(allocated(&fs).await <= empty + 3 * 12);
}

#[tokio::test]
async fn multi_level_trees() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let vbs = VOLBLOCK_ALIGN as u64;
    // > FANOUT^2 records -> 3 levels, all sparse.
    let size = (INDIRECT_FANOUT * INDIRECT_FANOUT + 10) * vbs;
    fs.create_block("vol", size, VOLBLOCK_ALIGN).await.unwrap();
    assert_eq!(fs.open_volume("vol").await.unwrap().levels, 3);

    let records = [
        0,
        INDIRECT_FANOUT - 1,
        INDIRECT_FANOUT,
        INDIRECT_FANOUT * INDIRECT_FANOUT - 1,
        INDIRECT_FANOUT * INDIRECT_FANOUT + 9,
    ];
    for (i, r) in records.iter().enumerate() {
        fs.block_write("vol", r * vbs + 100, &pattern(500, i as u8))
            .await
            .unwrap();
    }
    fs.sync().await.unwrap();
    drop(fs);

    let fs = pool.load().await;
    for (i, r) in records.iter().enumerate() {
        assert_eq!(read(&fs, r * vbs + 100, 500).await, pattern(500, i as u8));
    }
    assert!(read(&fs, 7 * vbs, 100).await.iter().all(|&b| b == 0));
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_writers_and_readers() {
    let pool = TestPool::new().await;
    let fs = std::sync::Arc::new(pool.format().await);
    fs.create_block("vol", 16 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    // 64 tasks each write their own 4K-aligned slots, interleaved across
    // shared records, while syncs happen in the middle.
    let mut tasks = tokio::task::JoinSet::new();
    for t in 0..64u64 {
        let fs = fs.clone();
        tasks.spawn(async move {
            for i in 0..16u64 {
                let off = (i * 64 + t) * 4096;
                fs.block_write("vol", off, &pattern(4096, (t * 16 + i) as u8))
                    .await
                    .unwrap();
                if i % 5 == 0 {
                    let mut buf = vec![0u8; 4096];
                    fs.block_read("vol", off, &mut buf).await.unwrap();
                    assert_eq!(buf, pattern(4096, (t * 16 + i) as u8));
                }
            }
        });
    }
    let syncer = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for _ in 0..5 {
                fs.sync().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    syncer.await.unwrap();
    fs.sync().await.unwrap();

    for t in 0..64u64 {
        for i in 0..16u64 {
            let off = (i * 64 + t) * 4096;
            assert_eq!(
                read(&fs, off, 4096).await,
                pattern(4096, (t * 16 + i) as u8),
                "task {} write {}",
                t,
                i
            );
        }
    }
}

#[tokio::test]
async fn delete_frees_the_whole_tree() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let baseline = allocated(&fs).await;

    fs.create_block("vol", 8 * MB, VOLBLOCK_ALIGN)
        .await
        .unwrap();
    fs.block_write("vol", 0, &pattern(3 * MB as usize, 1))
        .await
        .unwrap();
    fs.block_write("vol", 7 * MB, &pattern(4096, 2))
        .await
        .unwrap();
    fs.sync().await.unwrap();

    fs.delete(Path::new("/vol")).await.unwrap();
    settle(&fs).await;
    assert_eq!(allocated(&fs).await, baseline);
    assert!(matches!(
        fs.block_read("vol", 0, &mut [0u8; 10]).await,
        Err(FileSystemError::FileNotFound)
    ));
}

#[tokio::test]
async fn corrupted_volume_record_self_heals() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let data = pattern(DEFAULT_VOLBLOCKSIZE as usize, 9);
    fs.block_write("vol", 0, &data).await.unwrap();
    fs.sync().await.unwrap();
    let vol = fs.open_volume("vol").await.unwrap();
    let leaf = fs
        .tree_lookup(&vol, vol.root(), 0, 0)
        .await
        .unwrap()
        .unwrap();
    drop(vol);
    drop(fs);

    let (disk, off) = TestPool::locate(leaf.lba + 5);
    pool.corrupt(disk, off);

    let fs = pool.load().await;
    assert_eq!(read(&fs, 0, data.len()).await, data);
}

/// The test pool holds ~96 MiB of data; two 60 MiB writes cannot both fit.
#[tokio::test]
async fn full_pool_fails_cleanly_without_leaking() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", 1024 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();

    let a = pattern(60 * MB as usize, 1);
    fs.block_write("vol", 0, &a).await.unwrap();
    fs.sync().await.unwrap();
    settle(&fs).await;
    let used_before = allocated(&fs).await;
    let free_before = fs.allocatable_blocks().await;

    let b = pattern(60 * MB as usize, 2);
    fs.block_write("vol", 200 * MB, &b).await.unwrap();
    assert!(matches!(
        fs.sync().await,
        Err(FileSystemError::UnableToAllocateBlocks(_))
    ));

    // Nothing allocated, nothing freed, data still served from memory.
    assert_eq!(
        allocated(&fs).await,
        used_before,
        "failed sync leaked space"
    );
    assert_eq!(fs.allocatable_blocks().await, free_before);
    assert!(read(&fs, 200 * MB, b.len()).await == b);
    assert!(read(&fs, 0, a.len()).await == a);
    // The pool itself is still consistent and committable.
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
    drop(fs);

    // B was never durable; A is intact.
    let fs = pool.load().await;
    assert!(read(&fs, 0, a.len()).await == a);
    assert!(read(&fs, 200 * MB, 4096).await.iter().all(|&x| x == 0));
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn trim_lets_a_full_pool_recover() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.create_block("vol", 1024 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let a = pattern(60 * MB as usize, 3);
    fs.block_write("vol", 0, &a).await.unwrap();
    fs.sync().await.unwrap();

    let b = pattern(60 * MB as usize, 4);
    fs.block_write("vol", 200 * MB, &b).await.unwrap();
    assert!(fs.sync().await.is_err());

    // The guest trims A. Holes commit on their own even though B can't fit
    // yet; once A's space clears the defer window, B goes through.
    fs.block_zero("vol", 0, a.len()).await.unwrap();
    let mut attempts = 0;
    while fs.sync().await.is_err() {
        attempts += 1;
        assert!(attempts < 6, "pool never recovered");
    }
    assert!(attempts >= 1, "B shouldn't fit until A's space is released");
    drop(fs);

    let fs = pool.load().await;
    assert!(read(&fs, 200 * MB, b.len()).await == b);
    assert!(read(&fs, 0, 4096).await.iter().all(|&x| x == 0));
    assert!(fs.find_orphaned_blocks().await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_flushes_share_commits() {
    let pool = TestPool::new().await;
    let fs = std::sync::Arc::new(pool.format().await);
    fs.create_block("vol", 64 * MB, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let txg_before = fs.uberblock().await.unwrap().txg;

    let mut tasks = tokio::task::JoinSet::new();
    for t in 0..32u64 {
        let fs = fs.clone();
        tasks.spawn(async move {
            fs.block_write("vol", t * MB, &pattern(64 * 1024, t as u8))
                .await
                .unwrap();
            // Like fsync: once this returns, the write must be durable.
            fs.sync().await.unwrap();
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    let commits = fs.uberblock().await.unwrap().txg - txg_before;
    assert!(commits < 32, "{} commits for 32 flushes", commits);
    drop(fs);

    let fs = pool.load().await;
    for t in 0..32u64 {
        assert!(
            read(&fs, t * MB, 64 * 1024).await == pattern(64 * 1024, t as u8),
            "write {t}"
        );
    }
}

#[test]
fn record_cache_is_bounded() {
    let mut c = RecordCache::default();
    let rec = Arc::new(vec![0u8; 48 * 1024]);
    let n = RECORD_CACHE_BYTES / rec.len() * 2;
    for i in 0..n as u64 {
        c.insert((i, 1), rec.clone());
    }
    assert!(c.bytes <= RECORD_CACHE_BYTES);
    assert_eq!(c.map.len(), c.order.len());
    assert!(c.get((0, 1)).is_none(), "oldest evicted");
    assert!(c.get((n as u64 - 1, 1)).is_some(), "newest kept");
    // Same lba, newer birth TXG: a different entry, never a stale hit.
    assert!(c.get((n as u64 - 1, 2)).is_none());
}

/// A sync that fails after caching new indirect blocks hands their space
/// back, and the retry (same TXG, so same birth_txg) reuses those LBAs for
/// different contents. That is safe only because every indirect written
/// re-inserts its cache entry; this guards that invariant.
#[tokio::test]
async fn failed_sync_retry_does_not_serve_stale_cached_indirects() {
    let pool = TestPool::new().await;
    let (fs, faults) = pool.format_faulty().await;
    fs.create_block("vol", 64 * MB, VOLBLOCK_ALIGN)
        .await
        .unwrap();
    let vbs = VOLBLOCK_ALIGN as usize;

    // Two levels: record 600 needs an L1 indirect and the root indirect.
    assert_eq!(fs.open_volume("vol").await.unwrap().levels, 2);
    let rec = 600u64;
    fs.block_write("vol", rec * vbs as u64, &pattern(vbs, 1))
        .await
        .unwrap();
    // Sync writes: the data record, then L1 (cached), then the root -> fail it.
    faults.fail_after(2);
    assert!(fs.sync().await.is_err());
    faults.heal();

    // Change the record so the retry's L1 has different contents.
    let second = pattern(vbs, 2);
    fs.block_write("vol", rec * vbs as u64, &second)
        .await
        .unwrap();
    fs.sync().await.unwrap();

    assert!(read_any(&fs, rec * vbs as u64, vbs).await == second);
}

async fn read_any<D: crate::disk::BlockDevice + Send + Sync + 'static>(
    fs: &FileSystem<D>,
    offset: u64,
    len: usize,
) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    fs.block_read("vol", offset, &mut buf).await.unwrap();
    buf
}
