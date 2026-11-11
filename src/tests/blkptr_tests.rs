use super::*;
use crate::test_util::TestPool;
use std::path::Path;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn file_ptrs<D: BlockDevice>(fs: &FileSystem<D>, path: &str) -> Vec<BlockPtr> {
    let r = fs.get_file_at_path(Path::new(path)).await.unwrap();
    match fs.read_inode(&r).await.unwrap().inode_type {
        crate::inode::InodeType::File { blocks, .. } => blocks,
        _ => panic!("not a file"),
    }
}

#[tokio::test]
async fn objects_round_trip_with_aligned_records() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let txg = fs.open_txg().await.unwrap();

    let max = MAX_RECORD_BLOCKS as usize * BLOCK_SIZE;
    for len in [
        0,
        1,
        BLOCK_SIZE - 1,
        BLOCK_SIZE,
        BLOCK_SIZE + 1,
        max,
        max + 1,
        500_000,
    ] {
        let data = pattern(len, len as u8);
        let ptrs = fs.write_object(&data).await.unwrap();

        assert_eq!(object_size(&ptrs), len as u64);
        for p in &ptrs {
            assert_eq!(
                p.lba % DATA_SHARDS as u64,
                0,
                "record start must be stripe aligned"
            );
            assert_eq!(p.asize as u64 % DATA_SHARDS as u64, 0);
            assert!(p.asize as u64 <= MAX_RECORD_BLOCKS);
            assert!(p.data_blocks() <= p.asize as u64);
            assert_eq!(p.birth_txg, txg);
        }
        assert_eq!(fs.read_object(&ptrs).await.unwrap(), data, "len {}", len);
    }
}

#[tokio::test]
async fn data_blocks_hold_full_payloads() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    // Exactly 3 blocks of data should take exactly one stripe, no header overhead.
    let data = pattern(3 * BLOCK_SIZE, 7);
    fs.write_file(Path::new("/f"), &data).await.unwrap();
    let ptrs = file_ptrs(&fs, "/f").await;
    assert_eq!(ptrs.len(), 1);
    assert_eq!(ptrs[0].asize, 3);

    // And the raw disk block is the data itself.
    let (disk, off) = TestPool::locate(ptrs[0].lba);
    assert_eq!(pool.read_raw(disk, off), data[..BLOCK_SIZE]);
}

#[tokio::test]
async fn bit_rot_is_detected_healed_and_repaired() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let data = pattern(200_000, 3);
    fs.write_file(Path::new("/f"), &data).await.unwrap();
    let ptrs = file_ptrs(&fs, "/f").await;
    drop(fs);

    let victim = ptrs[1].lba + 4;
    let (disk, off) = TestPool::locate(victim);
    let good = pool.read_raw(disk, off);
    pool.corrupt(disk, off);
    assert_ne!(pool.read_raw(disk, off), good);

    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), data);
    // Self-heal rewrote the bad shard in place.
    assert_eq!(pool.read_raw(disk, off), good);
}

#[tokio::test]
async fn multiple_bad_blocks_in_one_record_heal() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let data = pattern(100_000, 9);
    fs.write_file(Path::new("/f"), &data).await.unwrap();
    let ptrs = file_ptrs(&fs, "/f").await;
    drop(fs);

    // Two blocks in different stripes of the same record.
    for lba in [ptrs[0].lba, ptrs[0].lba + 4] {
        let (disk, off) = TestPool::locate(lba);
        pool.corrupt(disk, off);
    }

    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/f")).await.unwrap(), data);
}

#[tokio::test]
async fn unrecoverable_corruption_is_reported() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.write_file(Path::new("/f"), &pattern(10_000, 1))
        .await
        .unwrap();
    let ptrs = file_ptrs(&fs, "/f").await;
    drop(fs);

    // Corrupt a data block and both parity shards of its stripe.
    let lba = ptrs[0].lba;
    let (disk, off) = TestPool::locate(lba);
    pool.corrupt(disk, off);
    for parity_disk in DATA_SHARDS..crate::stripe::DISKS {
        pool.corrupt(parity_disk, TestPool::stripe_offset(lba));
    }

    let fs = pool.load().await;
    assert!(matches!(
        fs.read_file(Path::new("/f")).await,
        Err(FileSystemError::ChecksumMismatch)
    ));
}

#[tokio::test]
async fn corrupted_metadata_heals_on_load() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/d")).await.unwrap();
    fs.write_file(Path::new("/d/f"), b"nested").await.unwrap();
    let root = fs.root_ptr.lock().await.clone();
    drop(fs);

    // Corrupt the root index itself.
    let (disk, off) = TestPool::locate(root[0].lba);
    pool.corrupt(disk, off);

    let fs = pool.load().await;
    assert_eq!(fs.read_file(Path::new("/d/f")).await.unwrap(), b"nested");
}
