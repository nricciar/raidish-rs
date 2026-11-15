use super::*;
use crate::disk::{BLOCK_SIZE, BlockDevice};
use crate::fs::{LoadOptions, Rewind};
use crate::test_util::{DEFAULT_VOLBLOCKSIZE, TestPool};
use crate::vdev::Pool;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn content(name: &str, generation: u8, len: usize) -> Vec<u8> {
    let seed = blake3::hash(name.as_bytes()).as_bytes()[0];
    (0..len)
        .map(|i| {
            (i as u8)
                .wrapping_mul(31)
                .wrapping_add(seed)
                .wrapping_add(generation)
        })
        .collect()
}

async fn file_blocks<D: BlockDevice>(fs: &FileSystem<D>, path: &str) -> Vec<BlockPtr> {
    let (_, d) = fs.stat_file(Path::new(path)).await.unwrap();
    let leaves = fs.tree_leaves(&d.tree).await.unwrap();
    leaves.into_iter().map(|(_, p)| p).collect()
}

/// Make the stripe holding `lba` unrecoverable: three of the five raidz2
/// shards corrupted.
fn destroy_stripe(pool: &TestPool, lba: u64) {
    for d in 0..3 {
        pool.corrupt(d, TestPool::stripe_offset(lba));
    }
}

/// Commit until every deferred free is released.
async fn settle<D: BlockDevice>(fs: &FileSystem<D>) {
    for _ in 0..=TXG_DEFER_SIZE {
        fs.commit_txg().await.unwrap();
    }
}

async fn workload(fs: &FileSystem<Pool>) {
    fs.mkdir(Path::new("/d")).await.unwrap();
    for i in 0..4 {
        let name = format!("/d/f{i}");
        fs.write_file(Path::new(&name), &content(&name, 1, 200_000 + i * 9_000))
            .await
            .unwrap();
    }
    fs.write_file(Path::new("/top"), &content("/top", 1, 5_000))
        .await
        .unwrap();
    fs.write_file(Path::new("/d/f1"), &content("/d/f1", 2, 70_000))
        .await
        .unwrap();
    fs.delete(Path::new("/d/f2")).await.unwrap();
    fs.create_block("vol", 4 << 20, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    fs.block_write("vol", 0, &content("vol", 1, 1 << 20))
        .await
        .unwrap();
    fs.block_write("vol", 3 << 20, &content("vol", 2, 300_000))
        .await
        .unwrap();
    fs.sync().await.unwrap();
}

#[tokio::test]
async fn clean_pool_is_clean() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    workload(&fs).await;

    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert!(report.leaks.is_empty() && report.referenced_free.is_empty());
    assert!(report.live_blocks > 0);

    let report = fs.fsck(true).await.unwrap();
    assert!(report.is_clean() && !report.leaks_repaired && !report.referenced_free_repaired);

    // And from a fresh, read-only import.
    drop(fs);
    let fs = FileSystem::load_readonly(pool.dev().await).await.unwrap();
    let report = fs.fsck(false).await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[tokio::test]
async fn leaks_are_found_and_freed() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    workload(&fs).await;
    settle(&fs).await;
    let before = fs.allocatable_blocks().await;

    // Allocate space nothing will ever reference, and commit it.
    let leaked = fs.allocate_contiguous(30).await.unwrap();
    fs.commit_txg().await.unwrap();

    let report = fs.fsck(false).await.unwrap();
    assert!(!report.is_clean());
    assert_eq!(report.leaks, vec![(leaked.start, leaked.len)]);
    assert_eq!(fs.find_orphaned_blocks().await.unwrap().len(), 1);

    let report = fs.fsck(true).await.unwrap();
    assert!(report.leaks_repaired && report.is_clean(), "{}", report);
    settle(&fs).await;
    assert_eq!(fs.allocatable_blocks().await, before);

    drop(fs);
    let fs = pool.load().await;
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

#[tokio::test]
async fn referenced_free_blocks_are_reallocated() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let data = content("/a", 1, 300_000);
    fs.write_file(Path::new("/a"), &data).await.unwrap();

    // Damage the space map: claim the file's second record is free.
    let victim = file_blocks(&fs, "/a").await[1].clone();
    fs.unallocate(victim.lba, victim.asize as u64).await;
    fs.commit_txg().await.unwrap();

    let report = fs.fsck(false).await.unwrap();
    assert_eq!(
        report.referenced_free,
        vec![(victim.lba, victim.asize as u64)]
    );
    let report = fs.fsck(true).await.unwrap();
    assert!(
        report.referenced_free_repaired && report.is_clean(),
        "{}",
        report
    );

    // Fill the pool; the file must survive.
    drop(fs);
    let fs = pool.load().await;
    let mut i = 0;
    while fs.allocatable_blocks().await > 2_000 {
        let name = format!("/fill{i}");
        if fs
            .write_file(Path::new(&name), &content(&name, 0, 1 << 20))
            .await
            .is_err()
        {
            break;
        }
        i += 1;
    }
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), data);
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

#[tokio::test]
async fn space_maps_are_rebuilt_from_the_tree() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    workload(&fs).await;
    settle(&fs).await;
    let free_before = fs.allocatable_blocks().await;
    let layout = fs.layout;
    drop(fs);

    // Destroy both slots of metaslab 0 (where everything was written).
    let mut offsets = std::collections::BTreeSet::new();
    for slot in 0..crate::metaslab::SPACEMAP_SLOTS {
        let lba = layout.slot_lba(0, slot);
        for b in 0..layout.spacemap_slot_blocks {
            offsets.insert(TestPool::stripe_offset(lba + b));
        }
    }
    for off in offsets {
        for d in 0..3 {
            pool.corrupt(d, off);
        }
    }

    let fs = pool.load().await;
    assert_eq!(fs.metaslabs[0].read().await.free_blocks(), 0);
    assert!(fs.allocatable_blocks().await < free_before);

    let report = fs.rebuild_spacemaps(false).await.unwrap();
    assert!(report.reclaimed > 0, "{}", report);
    settle(&fs).await;
    assert_eq!(fs.allocatable_blocks().await, free_before);
    assert!(fs.fsck(false).await.unwrap().is_clean());

    drop(fs);
    let fs = pool.load().await;
    assert_eq!(fs.allocatable_blocks().await, free_before);
    for i in [0, 1, 3] {
        let name = format!("/d/f{i}");
        let generation = if i == 1 { 2 } else { 1 };
        let len = if i == 1 { 70_000 } else { 200_000 + i * 9_000 };
        assert_eq!(
            fs.read_file(Path::new(&name)).await.unwrap(),
            content(&name, generation, len)
        );
    }
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

#[tokio::test]
async fn rewind_skips_damaged_txgs_and_sticks() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let a1 = content("/a", 1, 250_000);
    let b1 = content("/b", 1, 90_000);
    fs.write_file(Path::new("/a"), &a1).await.unwrap();
    fs.write_file(Path::new("/b"), &b1).await.unwrap();
    let good_txg = fs.uberblock().await.unwrap().txg;
    fs.write_file(Path::new("/a"), &content("/a", 2, 250_000))
        .await
        .unwrap();
    let newest = fs.uberblock().await.unwrap().txg;
    assert!(newest > good_txg);
    let damaged = file_blocks(&fs, "/a").await[0].lba;
    drop(fs);
    destroy_stripe(&pool, damaged);

    // A normal import opens the damaged TXG.
    let fs = pool.load().await;
    assert!(fs.read_file(Path::new("/a")).await.is_err());
    drop(fs);

    // An unknown TXG is refused.
    assert!(
        FileSystem::load_with(
            pool.dev().await,
            LoadOptions {
                read_only: true,
                rewind: Rewind::Txg(newest + 100),
            },
        )
        .await
        .is_err()
    );

    // Auto rewind finds the last intact state and commits past the ring.
    let fs = FileSystem::load_with(
        pool.dev().await,
        LoadOptions {
            read_only: false,
            rewind: Rewind::Auto,
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    assert_eq!(fs.read_file(Path::new("/b")).await.unwrap(), b1);
    assert!(fs.uberblock().await.unwrap().txg > newest);
    assert!(fs.fsck(false).await.unwrap().is_clean());
    // The discarded TXGs are gone from the ring, so nothing can go back to them.
    let ring: Vec<u64> = fs.uberblock_ring().await.iter().map(|u| u.txg).collect();
    assert!(
        ring.iter().all(|&t| t <= good_txg || t > newest),
        "discarded TXGs still importable: {:?}",
        ring
    );
    let c = content("/c", 1, 40_000);
    fs.write_file(Path::new("/c"), &c).await.unwrap();
    drop(fs);

    // A plain import now stays on the rewound lineage.
    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    assert_eq!(fs.read_file(Path::new("/c")).await.unwrap(), c);
    assert!(fs.fsck(false).await.unwrap().is_clean());
}

#[tokio::test]
async fn writable_rewind_to_a_damaged_tree_is_refused() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/d")).await.unwrap();
    // Several records: the file's tree has an indirect block.
    let a = content("/d/a", 1, 300_000);
    fs.write_file(Path::new("/d/a"), &a).await.unwrap();
    let txg = fs.uberblock().await.unwrap().txg;
    fs.write_file(Path::new("/top"), b"top").await.unwrap();
    // /d/a's indirect block is shared by both TXGs; destroy it.
    let (_, d) = fs.stat_file(Path::new("/d/a")).await.unwrap();
    assert!(d.tree.levels > 0);
    let indirect = d.tree.ptr.clone().unwrap();
    let a_blocks = file_blocks(&fs, "/d/a").await;
    drop(fs);
    destroy_stripe(&pool, indirect.lba);

    let opts = |read_only| LoadOptions {
        read_only,
        rewind: Rewind::Txg(txg),
    };
    // Writable: refused, rather than freeing /d/a's records (which nothing
    // readable references any more).
    assert!(
        FileSystem::load_with(pool.dev().await, opts(false))
            .await
            .is_err()
    );
    // Read-only still opens it for salvage, and nothing was freed.
    let fs = FileSystem::load_with(pool.dev().await, opts(true))
        .await
        .unwrap();
    assert!(fs.read_file(Path::new("/d/a")).await.is_err());
    drop(fs);
    let fs = pool.load().await;
    for p in &a_blocks {
        let ms = fs.layout.metaslab_for_block(p.lba).unwrap();
        assert!(
            !fs.metaslabs[ms]
                .read()
                .await
                .released_map()
                .contains_key(&p.lba),
            "/d/a's blocks were freed"
        );
    }
}

/// The object table lists every object, so an unreadable directory hides
/// no space: a writable rewind past one is safe and frees nothing live.
#[tokio::test]
async fn damaged_directory_hides_no_space() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/d")).await.unwrap();
    let a = content("/d/a", 1, 60_000);
    fs.write_file(Path::new("/d/a"), &a).await.unwrap();
    let (_, dir) = fs.stat(Path::new("/d")).await.unwrap();
    let dir_record = fs.tree_leaves(&dir.tree).await.unwrap()[0].1.clone();
    let a_blocks = file_blocks(&fs, "/d/a").await;
    drop(fs);
    // A one-block record may sit on any disk: wipe that stripe on all.
    for disk in 0..crate::test_util::DISKS {
        pool.corrupt(disk, TestPool::stripe_offset(dir_record.lba));
    }

    let fs = pool.load().await;
    let report = fs.fsck(true).await.unwrap();
    assert!(!report.incomplete, "{}", report);
    assert!(report.leaks.is_empty() && report.referenced_free.is_empty());
    assert!(!report.errors.is_empty(), "the directory is reported");
    for p in &a_blocks {
        let ms = fs.layout.metaslab_for_block(p.lba).unwrap();
        assert!(
            !fs.metaslabs[ms]
                .read()
                .await
                .released_map()
                .contains_key(&p.lba),
            "/d/a's blocks were freed"
        );
    }
}

#[tokio::test]
async fn rewind_to_an_explicit_txg_read_only() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let a1 = content("/a", 1, 50_000);
    fs.write_file(Path::new("/a"), &a1).await.unwrap();
    let txg = fs.uberblock().await.unwrap().txg;
    fs.write_file(Path::new("/a"), &content("/a", 2, 50_000))
        .await
        .unwrap();
    fs.write_file(Path::new("/b"), b"bee").await.unwrap();
    drop(fs);

    let fs = FileSystem::load_with(
        pool.dev().await,
        LoadOptions {
            read_only: true,
            rewind: Rewind::Txg(txg),
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.read_file(Path::new("/a")).await.unwrap(), a1);
    assert!(fs.read_file(Path::new("/b")).await.is_err());
    drop(fs);
    // Read-only rewind changed nothing.
    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/b")).await.unwrap(), b"bee");
}

#[tokio::test]
async fn salvage_copies_what_is_readable() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let big = content("/big", 1, 400_000);
    let small = content("/d/small", 1, 10_000);
    let vol = content("vol", 1, 1 << 20);
    fs.mkdir(Path::new("/d")).await.unwrap();
    fs.write_file(Path::new("/big"), &big).await.unwrap();
    fs.write_file(Path::new("/d/small"), &small).await.unwrap();
    fs.create_block("vol", 2 << 20, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    fs.block_write("vol", 512 << 10, &vol).await.unwrap();
    fs.sync().await.unwrap();
    let blocks = file_blocks(&fs, "/big").await;
    assert!(blocks.len() > 2);
    let lost = blocks[1].clone();
    drop(fs);
    destroy_stripe(&pool, lost.lba);

    let fs = FileSystem::load_readonly(pool.dev().await).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let report = fs.salvage(dir.path()).await.unwrap();
    assert!(!report.is_complete());

    let mut expect = big.clone();
    let off = blocks[0].lsize as usize;
    expect[off..off + lost.lsize as usize].fill(0);
    assert_eq!(std::fs::read(dir.path().join("big")).unwrap(), expect);
    assert_eq!(std::fs::read(dir.path().join("d/small")).unwrap(), small);
    let img = std::fs::read(dir.path().join("vol.img")).unwrap();
    assert_eq!(img.len(), 2 << 20);
    assert_eq!(&img[512 << 10..(512 << 10) + vol.len()], &vol[..]);
    assert!(img[..512 << 10].iter().all(|&b| b == 0));

    let damaged: Vec<_> = report.items.iter().filter(|i| !i.lost.is_empty()).collect();
    assert_eq!(damaged.len(), 1);
    assert_eq!(damaged[0].path, "/big");
    assert_eq!(damaged[0].lost, vec![(off as u64, lost.lsize as u64)]);
}

/// Counts `read_blocks` calls reaching the device.
struct CountingDev<D> {
    inner: D,
    reads: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl<D: BlockDevice> BlockDevice for CountingDev<D> {
    async fn flush(&self) -> Result<(), crate::disk::DiskError> {
        self.inner.flush().await
    }
    async fn read_block(&self, b: u64, buf: &mut [u8]) -> Result<(), crate::disk::DiskError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_block(b, buf).await
    }
    async fn read_blocks(&self, b: u64, buf: &mut [u8]) -> Result<(), crate::disk::DiskError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_blocks(b, buf).await
    }
    async fn write_block(
        &self,
        b: u64,
        buf: &[u8; BLOCK_SIZE],
    ) -> Result<(), crate::disk::DiskError> {
        self.inner.write_block(b, buf).await
    }
    async fn write_blocks(&self, b: u64, buf: &[u8]) -> Result<(), crate::disk::DiskError> {
        self.inner.write_blocks(b, buf).await
    }
    async fn reconstruct_candidates(
        &self,
        b: u64,
    ) -> Result<crate::disk::Candidates, crate::disk::DiskError> {
        self.inner.reconstruct_candidates(b).await
    }
    async fn repair_block(
        &self,
        b: u64,
        buf: &[u8; BLOCK_SIZE],
    ) -> Result<(), crate::disk::DiskError> {
        self.inner.repair_block(b, buf).await
    }
    fn pool(&self) -> Option<&Pool> {
        self.inner.pool()
    }
}

#[tokio::test]
async fn sequential_volume_writes_are_contiguous_and_read_coalesced() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let vbs = DEFAULT_VOLBLOCKSIZE as usize;
    let records = 64;
    fs.create_block("vol", (records * vbs) as u64, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    let data = content("vol", 3, records * vbs);
    fs.block_write("vol", 0, &data).await.unwrap();
    fs.sync().await.unwrap();
    let frag = fs.fragmentation("vol").await.unwrap();
    assert_eq!(frag.records, records as u64);
    assert_eq!(frag.breaks, 0, "{}", frag);
    drop(fs);

    // Fresh import (cold caches): a 21-record read is one data read (252
    // blocks) plus the indirect block, not one read per record.
    let reads = Arc::new(AtomicU64::new(0));
    let dev = CountingDev {
        inner: pool.dev().await,
        reads: reads.clone(),
    };
    let fs = FileSystem::load(dev).await.unwrap();
    fs.open_volume("vol").await.unwrap();
    reads.store(0, Ordering::SeqCst);
    let mut buf = vec![0u8; 21 * vbs];
    fs.block_read("vol", 0, &mut buf).await.unwrap();
    assert_eq!(buf, data[..21 * vbs]);
    assert!(
        reads.load(Ordering::SeqCst) <= 2,
        "{} reads",
        reads.load(Ordering::SeqCst)
    );
    drop(fs);

    // A corrupt record inside a coalesced read still self-heals.
    let ptr = {
        let fs = pool.load().await;
        let vol = fs.open_volume("vol").await.unwrap();
        fs.tree_leaves(&vol.obj.root()).await.unwrap()[30].1.clone()
    };
    let (disk, offset) = TestPool::locate(ptr.lba + 1);
    pool.corrupt(disk, offset);
    let fs = pool.load().await;
    let mut buf = vec![0u8; 21 * vbs];
    fs.block_read("vol", 21 * vbs as u64, &mut buf)
        .await
        .unwrap();
    assert_eq!(buf, data[21 * vbs..42 * vbs]);
}

#[tokio::test]
async fn defrag_makes_a_volume_contiguous() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let vbs = DEFAULT_VOLBLOCKSIZE as usize;
    let records = 64u64;
    fs.create_block("vol", records * vbs as u64, DEFAULT_VOLBLOCKSIZE)
        .await
        .unwrap();
    // Write records in a scrambled order, one commit each, with some files
    // in between, so they scatter.
    let mut expect = vec![0u8; records as usize * vbs];
    for k in 0..records {
        let idx = (k * 37) % records;
        let rec = content(&format!("r{idx}"), k as u8, vbs);
        expect[idx as usize * vbs..(idx as usize + 1) * vbs].copy_from_slice(&rec);
        fs.block_write("vol", idx * vbs as u64, &rec).await.unwrap();
        fs.sync().await.unwrap();
        if k % 8 == 0 {
            let name = format!("/f{k}");
            fs.write_file(Path::new(&name), &content(&name, 0, 20_000))
                .await
                .unwrap();
        }
    }
    let before = fs.fragmentation("vol").await.unwrap();
    assert!(before.breaks > 10, "{}", before);

    let (b, after) = fs.defrag("vol", 4096).await.unwrap();
    assert_eq!(b, before);
    assert_eq!(after.records, records);
    assert_eq!(after.breaks, 0, "{}", after);
    let mut buf = vec![0u8; expect.len()];
    fs.block_read("vol", 0, &mut buf).await.unwrap();
    assert_eq!(buf, expect);
    assert!(fs.fsck(false).await.unwrap().is_clean());

    // Already contiguous: nothing to do.
    let (_, again) = fs.defrag("vol", 4096).await.unwrap();
    assert_eq!(again, after);
    drop(fs);
    let fs = pool.load().await;
    let mut buf = vec![0u8; expect.len()];
    fs.block_read("vol", 0, &mut buf).await.unwrap();
    assert_eq!(buf, expect);
}

#[test]
fn interval_helpers() {
    let mut m = BTreeMap::new();
    assert!(insert_range(&mut m, 10, 5).is_empty());
    assert!(insert_range(&mut m, 15, 5).is_empty()); // adjacent: merged
    assert_eq!(m, BTreeMap::from([(10, 10)]));
    assert_eq!(insert_range(&mut m, 18, 4), vec![(18, 2)]);
    assert_eq!(m, BTreeMap::from([(10, 12)]));
    assert_eq!(clip(&m, 12, 15), BTreeMap::from([(12, 3)]));
    assert_eq!(complement(&m, 0, 30), BTreeMap::from([(0, 10), (22, 8)]));
    let other = BTreeMap::from([(5, 7), (20, 5)]);
    assert_eq!(intersect(&m, &other), vec![(10, 2), (20, 2)]);
}
