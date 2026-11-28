use super::*;
use crate::coords::{MemberIdx, PoolDiskIdx, VdevLba};
use crate::vdev::Vdev;

/// Pool disks `0..n`.
fn first(n: usize) -> Vec<PoolDiskIdx> {
    PoolDiskIdx::all(n).collect()
}
use crate::test_util::DISKS;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::fs::File;

/// Helper to create a 100MB temporary file for testing
async fn create_test_disk_file() -> (NamedTempFile, PathBuf) {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path().to_path_buf();

    // Allocate 100MB (similar to fallocate)
    let size = 100 * 1024 * 1024; // 100MB
    let file = File::create(&path).await.expect("Failed to open temp file");

    // Write zeros to allocate space
    file.set_len(size).await.expect("Failed to set file length");
    file.sync_all().await.expect("Failed to sync file");

    (temp_file, path)
}

/// Helper to create a test RaidZ array with 5 disks
async fn create_test_raidz() -> (RaidZ, Vec<NamedTempFile>) {
    let mut temp_files = Vec::new();
    let mut paths = Vec::new();

    for _ in 0..5 {
        let (temp_file, path) = create_test_disk_file().await;
        temp_files.push(temp_file);
        paths.push(path);
    }

    let p: Vec<&str> = paths.iter().map(|p| p.to_str().unwrap()).collect();
    let disks = std::sync::Arc::new(crate::disks::DiskSet::new(&p).await);
    let raidz = RaidZ::new(disks, first(5), 2);
    (raidz, temp_files)
}

#[tokio::test]
async fn test_raidz_creation() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Verify that all disks are local
    for i in 0..5 {
        assert!(
            raidz.is_local_disk(MemberIdx(i)),
            "Disk {} should be local",
            i
        );
    }
}

#[tokio::test]
async fn test_basic_read_write() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Write a single block
    let write_buf = [0x42u8; BLOCK_SIZE];
    raidz
        .write_block(VdevLba::new(0), &write_buf)
        .await
        .expect("Failed to write block");

    // Flush to ensure data is written with parity
    raidz.flush().await.expect("Failed to flush");

    // Read the block back
    let mut read_buf = [0u8; BLOCK_SIZE];
    raidz
        .read_block(VdevLba::new(0), &mut read_buf)
        .await
        .expect("Failed to read block");

    assert_eq!(read_buf, write_buf, "Read data should match written data");
}

#[tokio::test]
async fn test_multiple_blocks_in_single_stripe() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Write 3 blocks (fills one stripe)
    let blocks = [
        [0xAAu8; BLOCK_SIZE],
        [0xBBu8; BLOCK_SIZE],
        [0xCCu8; BLOCK_SIZE],
    ];

    for (i, block) in blocks.iter().enumerate() {
        raidz
            .write_block(VdevLba::new(i as u64), block)
            .await
            .expect("Failed to write block");
    }

    raidz.flush().await.expect("Failed to flush");

    // Read back and verify
    for (i, expected) in blocks.iter().enumerate() {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz
            .read_block(VdevLba::new(i as u64), &mut read_buf)
            .await
            .expect("Failed to read block");

        assert_eq!(&read_buf, expected, "Block {} mismatch", i);
    }
}

#[tokio::test]
async fn test_read_before_flush() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Write data but don't flush
    let write_buf = [0x99u8; BLOCK_SIZE];
    raidz
        .write_block(VdevLba::new(5), &write_buf)
        .await
        .expect("Failed to write block");

    // Should be able to read from the write buffer
    let mut read_buf = [0u8; BLOCK_SIZE];
    raidz
        .read_block(VdevLba::new(5), &mut read_buf)
        .await
        .expect("Failed to read from write buffer");

    assert_eq!(
        read_buf, write_buf,
        "Should read from write buffer before flush"
    );
}

#[tokio::test]
async fn test_stripe_buffer_completeness() {
    let mut buffer = StripeBuffer::new(3);

    assert!(!buffer.is_complete(), "New buffer should not be complete");
    assert!(!buffer.has_shard(0));
    assert!(!buffer.has_shard(1));
    assert!(!buffer.has_shard(2));

    // Add first shard
    let data1 = [0x11u8; BLOCK_SIZE];
    buffer.set_shard(0, &data1);
    assert!(buffer.has_shard(0));
    assert!(!buffer.is_complete());

    // Add second shard
    let data2 = [0x22u8; BLOCK_SIZE];
    buffer.set_shard(1, &data2);
    assert!(buffer.has_shard(1));
    assert!(!buffer.is_complete());

    // Add third shard - now complete
    let data3 = [0x33u8; BLOCK_SIZE];
    buffer.set_shard(2, &data3);
    assert!(buffer.has_shard(2));
    assert!(
        buffer.is_complete(),
        "Buffer should be complete with all 3 shards"
    );
}

#[tokio::test]
async fn test_stripe_buffer_get_shard() {
    let mut buffer = StripeBuffer::new(3);

    let data = [0x42u8; BLOCK_SIZE];
    buffer.set_shard(1, &data);

    // Should be able to get the shard we just set
    let mut read_buf = [0u8; BLOCK_SIZE];
    assert!(buffer.get_shard(1, &mut read_buf), "Should find shard 1");
    assert_eq!(read_buf, data);

    // Should not be able to get a shard we didn't set
    let mut read_buf2 = [0xFFu8; BLOCK_SIZE];
    assert!(
        !buffer.get_shard(0, &mut read_buf2),
        "Should not find shard 0"
    );
}

#[tokio::test]
async fn test_large_sequential_write() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Write 30 blocks (10 stripes)
    for block_id in 0..30 {
        let write_buf = [(block_id % 256) as u8; BLOCK_SIZE];
        raidz
            .write_block(VdevLba::new(block_id), &write_buf)
            .await
            .expect("Failed to write block");
    }

    raidz.flush().await.expect("Failed to flush");

    // Verify all blocks
    for block_id in 0..30 {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz
            .read_block(VdevLba::new(block_id), &mut read_buf)
            .await
            .expect("Failed to read block");

        let expected = [(block_id % 256) as u8; BLOCK_SIZE];
        assert_eq!(read_buf, expected, "Block {} mismatch", block_id);
    }
}

#[tokio::test]
async fn test_random_access_pattern() {
    let (raidz, _temp_files) = create_test_raidz().await;

    // Write in random order
    let block_ids = [7, 2, 15, 4, 11, 0, 8, 3, 12, 6];

    for &block_id in &block_ids {
        let write_buf = [block_id as u8; BLOCK_SIZE];
        raidz
            .write_block(VdevLba::new(block_id), &write_buf)
            .await
            .expect("Failed to write block");
    }

    raidz.flush().await.expect("Failed to flush");

    // Read in different random order
    for &block_id in &[0, 3, 6, 2, 8, 4, 12, 7, 15, 11] {
        let mut read_buf = [0u8; BLOCK_SIZE];
        raidz
            .read_block(VdevLba::new(block_id), &mut read_buf)
            .await
            .expect("Failed to read block");

        assert!(
            read_buf.iter().all(|&b| b == block_id as u8),
            "Block {} mismatch",
            block_id
        );
    }
}

fn pattern(blocks: usize, seed: u8) -> Vec<u8> {
    (0..blocks * BLOCK_SIZE)
        .map(|i| ((i / 7) as u8).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn range_io_round_trips_at_every_alignment() {
    let (raidz, _t) = create_test_raidz().await;
    // Every (start % 3, len % 3) combination, plus multi-stripe spans.
    let mut lba = 300;
    for start_off in 0..3u64 {
        for blocks in [1usize, 2, 3, 4, 5, 6, 7, 31] {
            let start = lba + start_off;
            let data = pattern(blocks, (start + blocks as u64) as u8);
            raidz
                .write_blocks(VdevLba::new(start), crate::disk::as_blocks(&data).unwrap())
                .await
                .unwrap();
            let mut got = vec![0u8; data.len()];
            raidz
                .read_blocks(
                    VdevLba::new(start),
                    crate::disk::as_blocks_mut(&mut got).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(got, data, "before flush: start {} blocks {}", start, blocks);

            raidz.flush().await.unwrap();
            let mut got = vec![0u8; data.len()];
            raidz
                .read_blocks(
                    VdevLba::new(start),
                    crate::disk::as_blocks_mut(&mut got).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(got, data, "after flush: start {} blocks {}", start, blocks);
            lba += 60;
        }
    }
}

#[tokio::test]
async fn full_stripe_write_supersedes_buffered_partial() {
    let (raidz, _t) = create_test_raidz().await;
    // Buffer a partial write to stripe 10, then overwrite the whole stripe.
    let stale = pattern(1, 1);
    raidz
        .write_block(VdevLba::new(30), stale.as_slice().try_into().unwrap())
        .await
        .unwrap();
    let fresh = pattern(3, 2);
    raidz
        .write_blocks(VdevLba::new(30), crate::disk::as_blocks(&fresh).unwrap())
        .await
        .unwrap();
    raidz.flush().await.unwrap();

    let mut got = vec![0u8; fresh.len()];
    raidz
        .read_blocks(
            VdevLba::new(30),
            crate::disk::as_blocks_mut(&mut got).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(got, fresh);
}

#[tokio::test]
async fn full_stripe_writes_have_valid_parity() {
    let (raidz, _t) = create_test_raidz().await;
    let data = pattern(12, 5);
    raidz
        .write_blocks(VdevLba::new(60), crate::disk::as_blocks(&data).unwrap())
        .await
        .unwrap();
    raidz.flush().await.unwrap();

    // Every way of rebuilding each data block from the other disks must
    // agree with what was written (i.e. parity is consistent).
    for i in 0..12u64 {
        let c = raidz
            .reconstruct_candidates(VdevLba::new(60 + i))
            .await
            .unwrap();
        let at = i as usize * BLOCK_SIZE;
        let rebuilt: Vec<_> = c.rebuilt.iter().flatten().collect();
        // Without each other disk, plus without just its own.
        assert_eq!(rebuilt.len(), DISKS, "block {}", 60 + i);
        for r in rebuilt {
            assert_eq!(&r[..], &data[at..at + BLOCK_SIZE], "block {}", 60 + i);
        }
    }
}

/// Single-block writes racing concurrent flushes on the same stripes: no
/// flush may trip over a stripe another one already wrote out, and no write
/// may land in a buffer that was already written out (and be lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_flushes_and_block_writes_lose_nothing() {
    let (raidz, _temp_files) = create_test_raidz().await;
    let raidz = std::sync::Arc::new(raidz);
    const BLOCKS: u64 = 12; // four stripes of three data shards
    for round in 0..40u8 {
        let mut tasks = Vec::new();
        for lba in 0..BLOCKS {
            let r = raidz.clone();
            tasks.push(tokio::spawn(async move {
                let fill = round.wrapping_mul(31).wrapping_add(lba as u8);
                r.write_block(VdevLba::new(lba), &[fill; BLOCK_SIZE])
                    .await
                    .unwrap();
            }));
            if lba % 3 == 0 {
                let r = raidz.clone();
                tasks.push(tokio::spawn(async move { r.flush().await.unwrap() }));
            }
        }
        for t in tasks {
            t.await.unwrap();
        }
        raidz.flush().await.unwrap();
        for lba in 0..BLOCKS {
            let mut buf = [0u8; BLOCK_SIZE];
            raidz.read_block(VdevLba::new(lba), &mut buf).await.unwrap();
            let fill = round.wrapping_mul(31).wrapping_add(lba as u8);
            assert!(buf.iter().all(|&b| b == fill), "round {round}, block {lba}");
        }
        // Everything reached the disks: nothing is left buffered.
        assert!(
            raidz.writes.is_empty(),
            "round {round}: stripes left buffered"
        );
    }
}

/// `masks_of` gives what filtering every mask did, in the same order.
#[test]
fn masks_of_is_every_mask_with_k_bits_in_order() {
    for n in 0..=12usize {
        for k in 0..=n {
            let old: Vec<u128> = (0u128..1 << n)
                .filter(|m| m.count_ones() as usize == k)
                .collect();
            assert_eq!(masks_of(n, k).collect::<Vec<_>>(), old, "n={} k={}", n, k);
        }
    }
    // No overflow at the widest pool.
    assert_eq!(masks_of(64, 61).count(), 41664); // C(64, 3)
}

/// D6b: label repair on a wide (valid) raidz group tries each way of
/// leaving `parity` shards out once: C(disks, parity) rebuilds.
#[test]
fn label_candidates_on_a_wide_raidz_are_each_rebuild_once() {
    let (data, parity) = (30, 1);
    let n = data + parity;
    // Inconsistent shards (the parity matches no data), so every rebuild
    // is tried.
    let mut shards: Vec<Shard> = (0..n).map(|i| [i as u8 + 1; BLOCK_SIZE]).collect();
    shards[n - 1] = [0xEE; BLOCK_SIZE];
    let present = vec![true; n];
    let candidates = label_candidates(data, parity, &shards, &present)
        .unwrap()
        .expect("inconsistent shards have candidates");
    assert_eq!(candidates.len(), n);
}

/// A read of a block still buffered in a stripe must not hold the stripe
/// map's shard lock while it waits for that stripe (a flush holds it across
/// disk I/O): a write to another stripe in the same shard would then block
/// its thread outright. On one thread that's a deadlock: the flush can
/// never finish.
#[test]
fn a_read_waiting_on_a_buffered_stripe_doesnt_block_the_map() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let files: Vec<NamedTempFile> = (0..3)
                .map(|_| {
                    let f = NamedTempFile::new().unwrap();
                    f.as_file().set_len(16 * 1024 * 1024).unwrap();
                    f
                })
                .collect();
            let handles: Vec<_> = files
                .iter()
                .map(|f| crate::test_util::FaultDisk::open(f.path()).unwrap())
                .collect();
            let caddies = handles
                .iter()
                .map(|h| {
                    crate::disks::Caddy::Healthy(
                        h.path().to_string_lossy().into_owned(),
                        crate::disk::Disk::faulty(h.clone()),
                    )
                })
                .collect();
            let disks = Arc::new(crate::disks::DiskSet::from_caddies(caddies));
            let raidz = Arc::new(RaidZ::new(disks, first(3), 1));
            let data = raidz.data as u64;
            // Stripe 0, half written: buffered.
            raidz
                .write_block(VdevLba::new(0), &[1u8; BLOCK_SIZE])
                .await
                .unwrap();

            for h in &handles {
                h.hold_writes(true);
            }
            let flush = tokio::spawn({
                let raidz = raidz.clone();
                async move { raidz.flush().await }
            });
            while handles.iter().map(|h| h.held()).sum::<usize>() == 0 {
                tokio::task::yield_now().await;
            }
            // Waits on stripe 0, which the flush holds.
            let read = tokio::spawn({
                let raidz = raidz.clone();
                async move {
                    let mut buf = [0u8; BLOCK_SIZE];
                    raidz
                        .read_block(VdevLba::new(0), &mut buf)
                        .await
                        .map(|_| buf[0])
                }
            });
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            // Inserts into many stripes, so into the shard the read held
            // (all but certainly: a few hundred shards at most).
            for other in 1..2000u64 {
                raidz
                    .write_block(VdevLba::new(other * data), &[2u8; BLOCK_SIZE])
                    .await
                    .unwrap();
            }
            for h in &handles {
                h.hold_writes(false);
                h.release_all();
            }
            flush.await.unwrap().unwrap();
            assert_eq!(read.await.unwrap().unwrap(), 1);
        });
        let _ = done.send(());
    });
    assert!(
        finished
            .recv_timeout(std::time::Duration::from_secs(20))
            .is_ok(),
        "deadlocked: the read held the map's shard while waiting"
    );
}

/// raidz`parity` over `n` small disks, each behind a fault switch.
fn fault_raidz(
    n: usize,
    parity: usize,
) -> (
    Arc<RaidZ>,
    Vec<crate::test_util::FaultHandle>,
    Vec<NamedTempFile>,
) {
    let files: Vec<NamedTempFile> = (0..n)
        .map(|_| {
            let f = NamedTempFile::new().unwrap();
            f.as_file().set_len(16 * 1024 * 1024).unwrap();
            f
        })
        .collect();
    let handles: Vec<_> = files
        .iter()
        .map(|f| crate::test_util::FaultDisk::open(f.path()).unwrap())
        .collect();
    let caddies = handles
        .iter()
        .map(|h| {
            crate::disks::Caddy::Healthy(
                h.path().to_string_lossy().into_owned(),
                crate::disk::Disk::faulty(h.clone()),
            )
        })
        .collect();
    let disks = Arc::new(crate::disks::DiskSet::from_caddies(caddies));
    (
        Arc::new(RaidZ::new(disks, first(n), parity)),
        handles,
        files,
    )
}

/// I-21: a partial write to one block of a stripe that a full-stripe write
/// is replacing, racing it. The partial write's flush reads the stripe's
/// other blocks to rebuild parity; read before the full write landed, they
/// were the old contents, written back over the full write's: its other
/// blocks lost. It must wait for the full write and read what that wrote.
#[tokio::test]
async fn a_partial_write_racing_a_full_stripe_write_keeps_its_neighbours() {
    let (raidz, handles, _f) = fault_raidz(3, 1);
    let full = pattern(2, 7); // stripe 0: blocks 0 and 1
    for h in &handles {
        h.hold_writes(true);
    }
    let whole = tokio::spawn({
        let raidz = raidz.clone();
        let full = full.clone();
        async move {
            raidz
                .write_blocks(VdevLba::new(0), crate::disk::as_blocks(&full).unwrap())
                .await
        }
    });
    for h in &handles {
        h.wait_held(1).await;
    }
    // Block 1 again (overlapping the full write there), then flushed.
    let partial = tokio::spawn({
        let raidz = raidz.clone();
        async move {
            raidz
                .write_block(VdevLba::new(1), &[9u8; BLOCK_SIZE])
                .await?;
            raidz.flush().await
        }
    });
    // Let it get as far as it can (to its own held writes, before).
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for h in &handles {
        h.hold_writes(false);
        h.release_all();
    }
    whole.await.unwrap().unwrap();
    partial.await.unwrap().unwrap();
    for h in &handles {
        h.release_all();
    }
    raidz.flush().await.unwrap();

    let mut b0 = [0u8; BLOCK_SIZE];
    raidz.read_block(VdevLba::new(0), &mut b0).await.unwrap();
    assert_eq!(&b0[..], &full[..BLOCK_SIZE], "block 0 lost the full write");
    let mut b1 = [0u8; BLOCK_SIZE];
    raidz.read_block(VdevLba::new(1), &mut b1).await.unwrap();
    assert!(b1.iter().all(|&b| b == 9), "block 1: the later write");
}

/// I-21: a full-stripe write that fails (too few disks took it) must not
/// throw away the partial writes buffered for that stripe before it: they
/// were acknowledged, and a later flush still writes them.
#[tokio::test]
async fn a_failed_full_stripe_write_keeps_the_buffered_partial() {
    let (raidz, handles, _f) = fault_raidz(3, 1);
    raidz
        .write_block(VdevLba::new(0), &[5u8; BLOCK_SIZE])
        .await
        .unwrap();
    for h in &handles[1..] {
        h.fail_writes(Some(crate::test_util::FaultError::Io));
    }
    let full = pattern(2, 3);
    assert!(
        raidz
            .write_blocks(VdevLba::new(0), crate::disk::as_blocks(&full).unwrap())
            .await
            .is_err()
    );
    for h in &handles {
        h.heal();
    }
    // Disks 1 and 2 missed it, so they lag (I-20) and leave too few
    // members to complete the stripe from: that flush must wait.
    assert!(raidz.flush().await.is_err());
    // Once they're found in sync again (what a resilver does), it goes.
    for d in first(3).into_iter().skip(1) {
        raidz.disks.caught_up(d, raidz.disks.miss_count(d));
    }
    raidz.flush().await.unwrap();
    let mut b0 = [0u8; BLOCK_SIZE];
    raidz.read_block(VdevLba::new(0), &mut b0).await.unwrap();
    assert!(
        b0.iter().all(|&b| b == 5),
        "the buffered write was dropped with the failed full write"
    );
}

/// I-20: a member missed a full-stripe write (its shard is the one from
/// before) and is back. A partial write to another block of that stripe
/// rebuilds parity from the stripe's other shards as on disk: read from
/// the stale member, the old block went into the new parity and the one
/// it missed was lost for good. It must be rebuilt from the members that
/// have it instead.
#[tokio::test]
async fn read_modify_write_doesnt_read_a_member_that_missed_writes() {
    let (raidz, handles, _f) = fault_raidz(3, 1);
    let old = pattern(2, 1);
    raidz
        .write_blocks(VdevLba::new(0), crate::disk::as_blocks(&old).unwrap())
        .await
        .unwrap();
    raidz.flush().await.unwrap();

    // Disk 1 (block 1's) misses the next version of the stripe.
    handles[1].fail_writes(Some(crate::test_util::FaultError::Io));
    let new = pattern(2, 2);
    raidz
        .write_blocks(VdevLba::new(0), crate::disk::as_blocks(&new).unwrap())
        .await
        .unwrap();
    raidz.flush().await.unwrap();
    handles[1].heal();
    assert!(raidz.disks.may_lag(first(2)[1]));

    raidz
        .write_block(VdevLba::new(0), &[7u8; BLOCK_SIZE])
        .await
        .unwrap();
    raidz.flush().await.unwrap();
    let mut b1 = [0u8; BLOCK_SIZE];
    raidz.read_block(VdevLba::new(1), &mut b1).await.unwrap();
    assert_eq!(
        &b1[..],
        &new[BLOCK_SIZE..],
        "block 1 went back to what the stale member had"
    );
    // And the stripe's parity agrees (block 1 rebuilt without disk 1).
    let c = raidz.reconstruct_candidates(VdevLba::new(1)).await.unwrap();
    for r in c.rebuilt.iter().flatten() {
        assert_eq!(&r[..], &new[BLOCK_SIZE..]);
    }
}

/// H-7: `RaidZ::read_block` must preserve fencing errors (`StaleEpoch`) and
/// classify `IncompleteStripe` as transient (`UnhealthyDisk`), not erase them
/// into permanent `IoError(InvalidData)`.
#[tokio::test]
async fn h7_read_block_preserves_fencing_and_incomplete_stripe_errors() {
    let (raidz, handles, _files) = fault_raidz(3, 1);
    let data = pattern(2, 5);
    raidz
        .write_blocks(VdevLba::new(0), crate::disk::as_blocks(&data).unwrap())
        .await
        .unwrap();
    raidz.flush().await.unwrap();

    // Case 1: primary shard returns StaleEpoch (e.g. a fencing error).
    handles[0].fail_reads(Some(crate::test_util::FaultError::StaleEpoch));
    let mut buf = [0u8; BLOCK_SIZE];
    let err = raidz
        .read_block(VdevLba::new(0), &mut buf)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::disk::DiskError::StaleEpoch),
        "expected StaleEpoch, got {:?}",
        err
    );
    handles[0].heal();

    // Case 2: primary shard fails with Io, sibling shard returns StaleEpoch.
    handles[0].fail_reads(Some(crate::test_util::FaultError::Io));
    handles[1].fail_reads(Some(crate::test_util::FaultError::StaleEpoch));
    let err = raidz
        .read_block(VdevLba::new(0), &mut buf)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::disk::DiskError::StaleEpoch),
        "expected StaleEpoch from sibling reconstruction, got {:?}",
        err
    );
    handles[0].heal();
    handles[1].heal();

    // Case 3: too many disks offline -> IncompleteStripe mapped to transient UnhealthyDisk.
    handles[0].set_offline(true);
    handles[1].set_offline(true);
    let err = raidz
        .read_block(VdevLba::new(0), &mut buf)
        .await
        .unwrap_err();
    assert!(
        err.is_transient(),
        "IncompleteStripe must map to a transient DiskError, got {:?}",
        err
    );
    let fs_err = crate::fs::FileSystemError::DiskError(err);
    assert_eq!(fs_err.kind(), crate::types::ErrorKind::Unavailable);
}
