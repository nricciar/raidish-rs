//! The filesystem on every supported pool layout.

use super::*;
use crate::coords::RawDiskBlock;
use crate::disk::BLOCK_SIZE;
use crate::fs::FileSystem;
use crate::test_util::TestPool;
use crate::test_util::disk;
use crate::test_util::ds;
use crate::types::MetaslabIdx;
use std::path::Path;

fn raidz(parity: usize, disks: std::ops::Range<usize>) -> Topology {
    Topology::RaidZ {
        parity,
        disks: disks.collect(),
    }
}

fn mirror(disks: std::ops::Range<usize>) -> Topology {
    Topology::Mirror {
        disks: disks.collect(),
    }
}

fn stripe(children: Vec<Topology>) -> Topology {
    Topology::Stripe { children }
}

/// Every layout under test.
fn layouts() -> Vec<Topology> {
    vec![
        raidz(1, 0..3),
        raidz(2, 0..5),
        raidz(1, 0..5), // 4 data shards: records of 28 blocks
        mirror(0..2),
        mirror(0..3),
        stripe(vec![mirror(0..2), mirror(2..4)]),
        stripe(vec![raidz(1, 0..3), raidz(1, 3..6)]),
        // mixed widths: lcm(3, 2) = 6
        stripe(vec![raidz(1, 0..4), raidz(1, 4..7)]),
        stripe(vec![raidz(1, 0..3), mirror(3..5)]),
    ]
}

/// The redundant groups of a layout (leaf vdevs and what each tolerates).
fn groups(t: &Topology) -> Vec<(Vec<usize>, usize)> {
    match t {
        Topology::Stripe { children } => children.iter().flat_map(groups).collect(),
        t => vec![(t.leaves(), t.fault_tolerance())],
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 9) as u8)
        .collect()
}

/// Sizes span one block, odd sizes, several records and several stripe chunks.
const SIZES: [usize; 4] = [1, 7 * 4096 + 5, 200_000, 700_000];

async fn write_set(fs: &FileSystem<Pool>, prefix: &str, seed: u8) {
    for (i, len) in SIZES.iter().enumerate() {
        let path = format!("/{}{}", prefix, i);
        fs.write_file(Path::new(&path), &pattern(*len, seed + i as u8))
            .await
            .unwrap();
    }
    fs.sync().await.unwrap();
}

async fn check_set(fs: &FileSystem<Pool>, prefix: &str, seed: u8, what: &str) {
    for (i, len) in SIZES.iter().enumerate() {
        let path = format!("/{}{}", prefix, i);
        let data = fs
            .read_file(Path::new(&path))
            .await
            .unwrap_or_else(|e| panic!("{}: {}: {}", what, path, e));
        assert!(
            data == pattern(*len, seed + i as u8),
            "{}: {} differs",
            what,
            path
        );
    }
}

async fn read_set(fs: &FileSystem<Pool>, prefix: &str) -> bool {
    for i in 0..SIZES.len() {
        if fs
            .read_file(Path::new(&format!("/{}{}", prefix, i)))
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn default_pool_matches_the_old_fixed_layout() {
    let fs = TestPool::new().await.format().await;
    assert_eq!(fs.align, crate::test_util::DATA_SHARDS as u64);
    assert_eq!(fs.max_record_blocks(), crate::test_util::MAX_RECORD_BLOCKS);
    assert_eq!(fs.volblock_align(), crate::test_util::VOLBLOCK_ALIGN);
    assert_eq!(
        fs.default_volblocksize(),
        crate::test_util::DEFAULT_VOLBLOCKSIZE
    );
    assert_eq!(fs.max_volblocksize(), crate::test_util::MAX_VOLBLOCKSIZE);
    assert_eq!(
        fs.header.uberblock_start,
        PoolLba::new(crate::test_util::UBERBLOCK_START)
    );
    assert_eq!(
        fs.header.spacemap_start,
        PoolLba::new(crate::test_util::SPACEMAP_START)
    );
}

#[tokio::test]
async fn files_and_volumes_round_trip_on_every_layout() {
    for t in layouts() {
        let what = t.summary();
        let pool = TestPool::with_topology(t).await;
        {
            let fs = pool.format().await;
            write_set(&fs, "f", 1).await;
            let vbs = fs.default_volblocksize();
            assert!(vbs.is_multiple_of(fs.volblock_align()), "{}", what);
            fs.create_block(&ds("vol"), 8 << 20, vbs).await.unwrap();
            let data = pattern(3 * vbs as usize + 1234, 9);
            fs.block_write(&ds("vol"), 5000, &data).await.unwrap();
            fs.sync().await.unwrap();
        }
        let fs = pool.load().await;
        check_set(&fs, "f", 1, &what).await;
        let mut buf = vec![0u8; 3 * fs.default_volblocksize() as usize + 1234];
        fs.block_read(&ds("vol"), 5000, &mut buf).await.unwrap();
        assert!(buf == pattern(buf.len(), 9), "{}: volume differs", what);

        let report = fs.scrub(true).await.unwrap();
        assert!(report.is_clean(), "{}: {}", what, report);
        assert_eq!(report.mismatched(), 0, "{}: {}", what, report);
        assert!(
            report.label_unrecoverable.is_empty(),
            "{}: {}",
            what,
            report
        );
    }
}

#[tokio::test]
async fn every_layout_survives_the_failures_it_tolerates() {
    for t in layouts() {
        let what = t.summary();
        let groups = groups(&t);
        let pool = TestPool::with_topology(t).await;
        drop({
            let fs = pool.format().await;
            write_set(&fs, "f", 3).await;
            fs
        });

        // The most each group can lose, all at once.
        let lost: Vec<usize> = groups
            .iter()
            .flat_map(|(disks, ft)| disks[..*ft].to_vec())
            .collect();
        let fs = FileSystem::load_readonly(pool.dev_without(&lost).await)
            .await
            .unwrap_or_else(|e| panic!("{} without {:?}: {}", what, lost, e));
        check_set(&fs, "f", 3, &format!("{} without {:?}", what, lost)).await;
        drop(fs);

        // One more in the first group is too many.
        let (disks, ft) = &groups[0];
        let mut too_many = lost.clone();
        too_many.push(disks[*ft]);
        let readable = match FileSystem::load_readonly(pool.dev_without(&too_many).await).await {
            Ok(fs) => read_set(&fs, "f").await,
            Err(_) => false,
        };
        assert!(
            !readable,
            "{} still fully readable without {:?}",
            what, too_many
        );
    }
}

#[tokio::test]
async fn scrub_repairs_a_corrupted_disk_on_every_layout() {
    for t in layouts() {
        let what = t.summary();
        let (victim_group, _) = groups(&t).into_iter().find(|(_, ft)| *ft > 0).unwrap();
        let victim = victim_group[0];
        let pool = TestPool::with_topology(t).await;
        let fs = pool.format().await;
        write_set(&fs, "f", 5).await;
        fs.dev.flush().await.unwrap();

        // Flip every written block in the first 2000 of the disk.
        let mut flipped = 0;
        for b in 0..2000u64 {
            let off = (b + crate::disks::DISK_LABEL_BLOCKS) * BLOCK_SIZE as u64;
            if pool.read_raw(victim, off).iter().any(|&x| x != 0) {
                pool.corrupt(victim, off);
                flipped += 1;
            }
        }
        assert!(flipped > 0, "{}", what);

        check_set(&fs, "f", 5, &what).await;
        let report = fs.scrub(true).await.unwrap();
        assert!(report.is_clean(), "{}: {}", what, report);
        assert!(report.mismatched() > 0, "{}: {}", what, report);
        assert_eq!(
            report.repaired(),
            report.mismatched(),
            "{}: {}",
            what,
            report
        );
        for d in 0..pool.paths.len() {
            if d != victim {
                assert_eq!(
                    report.shards.mismatched[d], 0,
                    "{}: disk {}: {}",
                    what, d, report
                );
            }
        }
        let again = fs.scrub(true).await.unwrap();
        assert_eq!(again.mismatched(), 0, "{}: {}", what, again);
    }
}

#[tokio::test]
async fn resilver_brings_a_returning_disk_back_on_every_layout() {
    for t in layouts() {
        let what = t.summary();
        let (group, ft) = groups(&t).into_iter().find(|(_, ft)| *ft > 0).unwrap();
        let away = group[0];
        let pool = TestPool::with_topology(t).await;
        drop({
            let fs = pool.format().await;
            write_set(&fs, "a", 1).await;
            fs
        });

        // Disk `away` misses a batch of writes.
        {
            let fs = FileSystem::load(pool.dev_without(&[away]).await)
                .await
                .unwrap();
            write_set(&fs, "b", 2).await;
            assert!(fs.degraded_disks()[away].is_some(), "{}", what);
        }

        // Back: resilver it.
        {
            let fs = pool.load().await;
            assert!(fs.needs_resilver(), "{}", what);
            let report = fs.resilver().await.unwrap();
            assert!(report.is_clean(), "{}: {}", what, report);
            assert_eq!(report.cleared, vec![away], "{}: {}", what, report);
            assert_eq!(
                fs.degraded_disks(),
                vec![None; pool.paths.len()],
                "{}",
                what
            );
        }

        // Now the rest of its group may go: `away` must really be current.
        let others: Vec<usize> = group[1..=ft].to_vec();
        let fs = FileSystem::load_readonly(pool.dev_without(&others).await)
            .await
            .unwrap();
        check_set(&fs, "a", 1, &what).await;
        check_set(&fs, "b", 2, &what).await;
    }
}

#[tokio::test]
async fn opening_with_a_different_layout_is_refused() {
    let pool = TestPool::with_topology(mirror(0..3)).await;
    drop(pool.format().await);
    // Same disks, read as a stripe of three plain disks.
    let paths: Vec<&str> = pool.paths.iter().map(|p| p.to_str().unwrap()).collect();
    let disks = Arc::new(DiskSet::new(&paths).await);
    let other = Pool::new(
        disks,
        stripe(vec![
            Topology::Disk(0),
            Topology::Disk(1),
            Topology::Disk(2),
        ]),
    )
    .unwrap();
    match FileSystem::load(other).await {
        Err(crate::fs::FileSystemError::TopologyMismatch(_)) => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("opened a mirror as a stripe"),
    }
}

#[tokio::test]
async fn stripe_requests_crossing_chunks_are_split() {
    let pool = TestPool::with_topology(stripe(vec![mirror(0..2), mirror(2..4)])).await;
    let dev = pool.dev().await;
    // Chunks are 30 blocks: 20..80 touches chunks 0, 1 and 2.
    let data = pattern(60 * BLOCK_SIZE, 7);
    dev.write_blocks(PoolLba::new(20), crate::disk::as_blocks(&data).unwrap())
        .await
        .unwrap();
    dev.flush().await.unwrap();
    let mut back = vec![0u8; data.len()];
    dev.read_blocks(
        PoolLba::new(20),
        crate::disk::as_blocks_mut(&mut back).unwrap(),
    )
    .await
    .unwrap();
    assert!(back == data);
    let check = dev
        .verify_blocks(PoolLba::new(20), &data, false)
        .await
        .unwrap();
    assert_eq!(check.mismatched.iter().sum::<u64>(), 0);
    assert_eq!(check.stripes, 60);
    // Chunk 1 (blocks 30..60) went to the second mirror only.
    let off = crate::disks::DISK_LABEL_BLOCKS * BLOCK_SIZE as u64;
    assert_eq!(
        pool.read_raw(2, off),
        data[10 * BLOCK_SIZE..11 * BLOCK_SIZE]
    );
    assert_eq!(
        pool.read_raw(3, off),
        data[10 * BLOCK_SIZE..11 * BLOCK_SIZE]
    );
}

/// Every raw disk block a pool write touches maps back (through
/// `Topology::pool_range_of`) to the metaslab that was written: the disk
/// servers' fencing domains agree with where the vdevs put the data.
#[tokio::test]
async fn fence_domains_match_placement_on_every_layout() {
    use crate::disk::PoolDevice;
    use crate::fence::{Domain, FenceMap};
    for t in layouts() {
        let what = t.summary();
        let pool = TestPool::with_topology(t.clone()).await;
        let fs = pool.format().await;
        fs.dev.flush().await.unwrap();
        let layout = fs.layout;
        assert!(layout.metaslab_count >= 2, "{}", what);
        let disk_len = |d: usize| std::fs::metadata(&pool.paths[d]).unwrap().len();
        let snapshot = |d: usize| std::fs::read(&pool.paths[d]).unwrap();
        let before: Vec<Vec<u8>> = (0..t.leaves().len()).map(snapshot).collect();

        // Two records' worth at the start of metaslab 1, then one at its end.
        let k = 1;
        let rec = fs.max_record_blocks();
        let start = layout.metaslab_start(k);
        let end = start + layout.metaslab_size_blocks - rec;
        for at in [start, end] {
            let buf =
                vec![0xA5u8; (rec * 2).min(layout.metaslab_size_blocks) as usize * BLOCK_SIZE];
            let len = if at == start {
                buf.len()
            } else {
                rec as usize * BLOCK_SIZE
            };
            fs.dev
                .write_blocks(
                    PoolLba::new(at),
                    crate::disk::as_blocks(&buf[..len]).unwrap(),
                )
                .await
                .unwrap();
        }
        fs.dev.flush().await.unwrap();

        let map = FenceMap::new(t.clone(), layout);
        let mut changed = 0;
        for (d, before) in before.iter().enumerate() {
            let after = snapshot(d);
            let blocks = disk_len(d) / BLOCK_SIZE as u64;
            for b in 0..blocks {
                let r = b as usize * BLOCK_SIZE..(b as usize + 1) * BLOCK_SIZE;
                if before[r.clone()] != after[r] {
                    changed += 1;
                    assert_eq!(
                        map.domain_of(disk(d), RawDiskBlock::new(b)),
                        Domain::Metaslab(MetaslabIdx::of_index(k)),
                        "{}: disk {} raw block {}",
                        what,
                        d,
                        b
                    );
                }
            }
        }
        assert!(changed > 0, "{}", what);
        // Labels and headers are the pool's.
        assert_eq!(
            map.domain_of(disk(0), RawDiskBlock::new(0)),
            Domain::Pool,
            "{}",
            what
        );
        assert_eq!(
            map.domain_of(disk(0), RawDiskBlock::new(crate::disks::DISK_LABEL_BLOCKS)),
            Domain::Pool,
            "{}",
            what
        );
    }
}

/// D6b: raidz parity is at most 3.
#[test]
fn raidz_parity_is_at_most_three() {
    assert!(raidz(3, 0..6).validate(6).is_ok());
    let e = raidz(4, 0..8).validate(8).unwrap_err();
    assert!(e.contains("at most 3"), "{}", e);
}

/// A pool of one disk is a whole filesystem: files and volumes survive a
/// reload, scrub finds nothing, and a corrupted block reads as an error
/// (there is no copy to rebuild it from), never as wrong data.
#[tokio::test]
async fn a_single_disk_pool_is_a_whole_filesystem() {
    let t = stripe(vec![Topology::Disk(0)]);
    assert_eq!(t.fault_tolerance(), 0);
    let pool = TestPool::with_topology(t).await;
    let big = pattern(700_000, 42);
    {
        let fs = pool.format().await;
        write_set(&fs, "f", 1).await;
        fs.write_file(Path::new("/big"), &big).await.unwrap();
        let vbs = fs.default_volblocksize();
        fs.create_block(&ds("vol"), 8 << 20, vbs).await.unwrap();
        fs.block_write(&ds("vol"), 5000, &pattern(vbs as usize + 7, 9))
            .await
            .unwrap();
        fs.sync().await.unwrap();
    }

    // Reopened the way an embedder would: `Pool::single` over one disk.
    let path = pool.paths[0].to_str().unwrap().to_string();
    let disk = crate::disk::Disk::open(&path).await.unwrap();
    let fs = FileSystem::load(Pool::single(crate::disks::Caddy::Healthy(path, disk)))
        .await
        .unwrap();
    check_set(&fs, "f", 1, "single").await;
    let mut buf = vec![0u8; fs.default_volblocksize() as usize + 7];
    fs.block_read(&ds("vol"), 5000, &mut buf).await.unwrap();
    assert!(buf == pattern(buf.len(), 9), "volume differs");
    let report = fs.scrub(true).await.unwrap();
    assert!(report.is_clean(), "{}", report);
    assert_eq!(report.mismatched(), 0, "{}", report);

    // Corrupt one block of /big's data, found by its contents.
    let first = &big[..BLOCK_SIZE];
    let off = (0..20_000u64)
        .map(|b| b * BLOCK_SIZE as u64)
        .find(|&off| pool.read_raw(0, off) == first)
        .expect("/big's first block on disk");
    pool.corrupt(0, off);

    assert!(
        fs.read_file(Path::new("/big")).await.is_err(),
        "a corrupted block with no copy must not read"
    );
    check_set(&fs, "f", 1, "single, after /big was corrupted").await;
    let report = fs.scrub(true).await.unwrap();
    assert!(!report.is_clean(), "{}", report);
    assert_eq!(report.repaired(), 0, "{}", report);
}
