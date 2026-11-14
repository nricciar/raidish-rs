use super::*;
use crate::test_util::TestPool;
use crate::test_util::VOLBLOCK_ALIGN;
use crate::vdev::Pool;

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i / 3) as u8).wrapping_mul(13).wrapping_add(seed))
        .collect()
}

async fn allocated(fs: &FileSystem<Pool>) -> u64 {
    let mut used = 0;
    for ms in fs.metaslabs.iter() {
        let ms = ms.read().await;
        used += ms.block_count - ms.free_blocks() - ms.pending_free_blocks();
    }
    used
}

/// Commit enough TXGs that every deferred free is released.
async fn settle(fs: &FileSystem<Pool>) {
    for _ in 0..=crate::metaslab::TXG_DEFER_SIZE {
        fs.sync().await.unwrap();
    }
}

/// Sync `obj`'s dirty records into a new tree and commit, the way an
/// owner of the tree does: switch to the new root, free what it replaced.
async fn sync_obj(fs: &FileSystem<Pool>, obj: &ObjTree) {
    if let Some(taken) = obj.begin_sync() {
        let (mut allocated, mut replaced) = (Vec::new(), Vec::new());
        let root = fs
            .build_tree(obj, &taken, &mut allocated, &mut replaced)
            .await
            .unwrap();
        obj.set_root(root);
        for p in &replaced {
            fs.free(p.lba, p.asize as u64).await.unwrap();
        }
        obj.end_sync(&taken, Some(&taken));
    }
    fs.sync().await.unwrap();
}

async fn read(fs: &FileSystem<Pool>, obj: &ObjTree, offset: u64, len: usize) -> Vec<u8> {
    let mut buf = vec![0xEEu8; len];
    let n = fs.object_read(obj, offset, &mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

async fn tree_blocks(fs: &FileSystem<Pool>, obj: &ObjTree) -> u64 {
    let ext = fs.tree_extents(&obj.root()).await.unwrap();
    ext.iter().map(|e| e.len).sum()
}

fn growable(bs: usize) -> ObjTree {
    ObjTree::new(
        bs,
        Sizing::Growable,
        0,
        TreeRoot {
            ptr: None,
            levels: 1,
        },
    )
}

#[test]
fn levels_for_counts_indirect_levels() {
    assert_eq!(levels_for(0), 0);
    assert_eq!(levels_for(1), 0);
    assert_eq!(levels_for(2), 1);
    assert_eq!(levels_for(INDIRECT_FANOUT), 1);
    assert_eq!(levels_for(INDIRECT_FANOUT + 1), 2);
    assert_eq!(levels_for(INDIRECT_FANOUT * INDIRECT_FANOUT + 1), 3);
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

/// A growable object gains levels as writes go past what its tree
/// addresses, one or several at a time, and keeps everything written before.
#[tokio::test]
async fn growable_object_grows_its_tree() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let base = allocated(&fs).await;
    let bs = VOLBLOCK_ALIGN as usize;
    let obj = growable(bs);

    let a = pattern(bs + 100, 1);
    fs.object_modify(&obj, 0, a.len(), Some(&a)).await.unwrap();
    assert_eq!(obj.size(), a.len() as u64);
    sync_obj(&fs, &obj).await;
    assert_eq!(obj.levels(), 1);

    // Past one indirect block: one more level.
    let at_b = 600 * bs as u64 + 7;
    let b = pattern(300, 2);
    fs.object_modify(&obj, at_b, b.len(), Some(&b))
        .await
        .unwrap();
    // Readable before the sync, from the dirty records.
    assert_eq!(read(&fs, &obj, at_b, b.len()).await, b);
    sync_obj(&fs, &obj).await;
    assert_eq!(obj.levels(), 2);

    // Past FANOUT^2 records: a third level.
    let at_c = (INDIRECT_FANOUT * INDIRECT_FANOUT + 5) * bs as u64;
    let c = pattern(bs, 3);
    fs.object_modify(&obj, at_c, c.len(), Some(&c))
        .await
        .unwrap();
    sync_obj(&fs, &obj).await;
    assert_eq!(obj.levels(), 3);
    assert_eq!(obj.size(), at_c + c.len() as u64);

    assert_eq!(read(&fs, &obj, 0, a.len()).await, a);
    assert_eq!(read(&fs, &obj, at_b, b.len()).await, b);
    assert_eq!(read(&fs, &obj, at_c, c.len()).await, c);
    // Holes in between read as zeros; reads stop at the end.
    assert_eq!(read(&fs, &obj, 10 * bs as u64, 50).await, vec![0u8; 50]);
    assert!(read(&fs, &obj, at_c + c.len() as u64, 10).await.is_empty());

    // From one level straight to three.
    let jump = growable(bs);
    fs.object_modify(&jump, 0, 10, Some(&[9u8; 10]))
        .await
        .unwrap();
    sync_obj(&fs, &jump).await;
    fs.object_modify(&jump, at_c, c.len(), Some(&c))
        .await
        .unwrap();
    sync_obj(&fs, &jump).await;
    assert_eq!(jump.levels(), 3);
    assert_eq!(read(&fs, &jump, 0, 10).await, vec![9u8; 10]);
    assert_eq!(read(&fs, &jump, at_c, c.len()).await, c);

    // Nothing leaked: what's allocated is exactly the two trees.
    settle(&fs).await;
    assert_eq!(
        allocated(&fs).await,
        base + tree_blocks(&fs, &obj).await + tree_blocks(&fs, &jump).await
    );
}

/// The committed tree never aliases indices past what it addresses.
#[tokio::test]
async fn lookups_past_the_tree_are_holes() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let bs = VOLBLOCK_ALIGN as usize;
    let obj = growable(bs);
    fs.object_modify(&obj, 0, bs, Some(&pattern(bs, 4)))
        .await
        .unwrap();
    sync_obj(&fs, &obj).await;
    let root = obj.root();
    assert!(fs.tree_lookup(&obj, &root, 0, 0).await.unwrap().is_some());
    // Index 512 would land in slot 0 again if it wrapped.
    let past = fs.tree_lookup(&obj, &root, 0, INDIRECT_FANOUT).await;
    assert!(past.unwrap().is_none());
}

/// Truncating zeroes the tail of the last record, frees every record past
/// it, and growing again reads zeros there.
#[tokio::test]
async fn truncate_shrinks_and_frees() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    settle(&fs).await;
    let base = allocated(&fs).await;
    let bs = VOLBLOCK_ALIGN as usize;
    let obj = growable(bs);

    let data = pattern(20 * bs, 5);
    fs.object_modify(&obj, 0, data.len(), Some(&data))
        .await
        .unwrap();
    sync_obj(&fs, &obj).await;
    // Dirty (not yet synced) records past the cut go too.
    fs.object_modify(&obj, 25 * bs as u64, 10, Some(&[1u8; 10]))
        .await
        .unwrap();
    let full = tree_blocks(&fs, &obj).await;

    let cut = 3 * bs as u64 + 100;
    fs.object_truncate(&obj, cut).await.unwrap();
    assert_eq!(obj.size(), cut);
    assert_eq!(
        read(&fs, &obj, 0, 2 * data.len()).await,
        data[..cut as usize]
    );
    sync_obj(&fs, &obj).await;
    assert_eq!(fs.tree_leaves(&obj.root()).await.unwrap().len(), 4);
    assert!(tree_blocks(&fs, &obj).await < full);

    fs.object_truncate(&obj, 30 * bs as u64).await.unwrap();
    let after = read(&fs, &obj, 0, 30 * bs).await;
    assert_eq!(after.len(), 30 * bs);
    assert_eq!(&after[..cut as usize], &data[..cut as usize]);
    assert!(after[cut as usize..].iter().all(|&b| b == 0));

    sync_obj(&fs, &obj).await;
    settle(&fs).await;
    assert_eq!(allocated(&fs).await, base + tree_blocks(&fs, &obj).await);

    // To zero: nothing left.
    fs.object_truncate(&obj, 0).await.unwrap();
    sync_obj(&fs, &obj).await;
    assert!(obj.root().ptr.is_none());
    settle(&fs).await;
    assert_eq!(allocated(&fs).await, base);
}

#[tokio::test]
async fn fixed_objects_cut_writes_and_refuse_truncate() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let bs = VOLBLOCK_ALIGN as usize;
    let size = 2 * bs as u64;
    let obj = ObjTree::new(
        bs,
        Sizing::Fixed,
        size,
        TreeRoot {
            ptr: None,
            levels: 1,
        },
    );
    let data = pattern(3 * bs, 6);
    let n = fs
        .object_modify(&obj, bs as u64, data.len(), Some(&data))
        .await
        .unwrap();
    assert_eq!(n, bs);
    assert_eq!(obj.size(), size);
    assert!(fs.object_truncate(&obj, bs as u64).await.is_err());
}
