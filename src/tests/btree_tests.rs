//! The B+tree against a `BTreeMap` model, through `kv` datasets with the
//! smallest nodes the test pool allows (so trees get deep quickly).

use crate::disk::BlockDevice;
use crate::fs::FileSystem;
use crate::test_util::{TestPool, VOLBLOCK_ALIGN};
use crate::vdev::Pool;
use std::collections::BTreeMap;
use std::ops::Bound;

/// A small deterministic generator (no RNG dependency).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64, width: usize) -> Vec<u8> {
    let mut k = format!("key/{:08}/", i).into_bytes();
    k.resize(width.max(k.len()), b'.');
    k
}

fn value(i: u64, generation: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|j| (i.wrapping_mul(31) ^ generation.wrapping_mul(7) ^ j as u64) as u8)
        .collect()
}

async fn small_kv(pool: &TestPool, name: &str) -> FileSystem<Pool> {
    let fs = pool.format().await;
    fs.create_kv_with(name, VOLBLOCK_ALIGN, 1 << 20)
        .await
        .unwrap();
    fs
}

/// The committed tree is sound and holds exactly `model`.
async fn verify<D: BlockDevice>(
    fs: &FileSystem<D>,
    name: &str,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
) {
    fs.sync().await.unwrap();
    let id = fs.dataset_id(name).await.unwrap();
    let d = fs.read_dnode(id).await.unwrap();
    let report = fs.check_kv_tree(&d).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.entries, model.len() as u64);
    assert_eq!(fs.kv_len(name).await.unwrap(), model.len() as u64);
    let all = fs
        .kv_range(name, Bound::Unbounded, Bound::Unbounded, usize::MAX)
        .await
        .unwrap();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(all.len(), want.len());
    assert!(all == want, "range scan differs from the model");
}

#[tokio::test]
async fn random_puts_and_deletes_match_a_model() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let mut model = BTreeMap::new();
    let mut rng = Rng(0x9e3779b97f4a7c15);
    for round in 0..6u64 {
        for _ in 0..1500 {
            let i = rng.below(3000);
            let k = key(i, 8 + rng.below(120) as usize);
            if rng.below(4) == 0 {
                let had = model.remove(&k).is_some();
                assert_eq!(fs.kv_delete("t", &k).await.unwrap(), had);
            } else {
                let v = value(i, round, rng.below(300) as usize);
                let had = model.insert(k.clone(), v.clone()).is_some();
                assert_eq!(fs.kv_put("t", &k, &v).await.unwrap(), had);
            }
        }
        verify(&fs, "t", &model).await;
    }
    // Spot checks by key.
    for (k, v) in model.iter().step_by(37) {
        assert_eq!(fs.kv_get("t", k).await.unwrap().as_ref(), Some(v));
    }
    assert_eq!(fs.kv_get("t", b"absent").await.unwrap(), None);
}

#[tokio::test]
async fn a_deep_tree_shrinks_back_to_nothing() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let mut model = BTreeMap::new();
    // Long keys: few per node, so branches split too.
    for i in 0..2500 {
        let k = key(i, 600);
        let v = value(i, 0, 8);
        fs.kv_put("t", &k, &v).await.unwrap();
        model.insert(k, v);
    }
    verify(&fs, "t", &model).await;
    let t = {
        let ds = fs.open_kv("t").await.unwrap();
        crate::btree::Txn::begin(&fs, &ds.obj).await.unwrap().header
    };
    assert!(t.height >= 2, "height {}", t.height);
    // Delete in an order that empties nodes from both ends and the middle.
    let mut keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    let mut rng = Rng(42);
    for i in (1..keys.len()).rev() {
        keys.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for (n, k) in keys.iter().enumerate() {
        assert!(fs.kv_delete("t", k).await.unwrap());
        model.remove(k);
        if n % 600 == 0 {
            verify(&fs, "t", &model).await;
        }
    }
    verify(&fs, "t", &model).await;
    let h = {
        let ds = fs.open_kv("t").await.unwrap();
        crate::btree::Txn::begin(&fs, &ds.obj).await.unwrap().header
    };
    assert_eq!((h.root, h.height, h.entries), (0, 0, 0));
    // Every node's record was freed (only the header is left).
    let id = fs.dataset_id("t").await.unwrap();
    let d = fs.read_dnode(id).await.unwrap();
    let leaves = fs.tree_leaves(&d.tree).await.unwrap();
    assert_eq!(leaves.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![0]);
}

#[tokio::test]
async fn large_values_go_to_overflow_pages() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let mut model = BTreeMap::new();
    for (i, len) in [0usize, 1, 700, 768, 769, 12_000, 12_288, 100_000, 1 << 20]
        .into_iter()
        .enumerate()
    {
        let k = key(i as u64, 10);
        let v = value(i as u64, 1, len);
        fs.kv_put("t", &k, &v).await.unwrap();
        model.insert(k, v);
    }
    verify(&fs, "t", &model).await;
    // Replacing and deleting large values frees their pages.
    let big = key(8, 10);
    fs.kv_put("t", &big, b"small now").await.unwrap();
    model.insert(big.clone(), b"small now".to_vec());
    fs.kv_delete("t", &key(7, 10)).await.unwrap();
    model.remove(&key(7, 10));
    verify(&fs, "t", &model).await;
}

#[tokio::test]
async fn ranges_and_prefixes() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let mut model = BTreeMap::new();
    for dir in ["a", "b", "b/c", "ba", "c"] {
        for i in 0..400 {
            let k = format!("{}/{:04}", dir, i).into_bytes();
            fs.kv_put("t", &k, &k).await.unwrap();
            model.insert(k.clone(), k);
        }
    }
    // A prefix, paged.
    let mut got = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = fs
            .kv_scan_prefix("t", b"b/", after.as_deref(), 77)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        after = Some(page.last().unwrap().0.clone());
        got.extend(page.into_iter().map(|(k, _)| k));
    }
    let want: Vec<Vec<u8>> = model
        .keys()
        .filter(|k| k.starts_with(b"b/"))
        .cloned()
        .collect();
    assert_eq!(got, want);
    assert_eq!(want.len(), 800);
    // Bounds of every kind.
    let r = fs
        .kv_range(
            "t",
            Bound::Excluded(b"a/0399"),
            Bound::Included(b"b/0001"),
            usize::MAX,
        )
        .await
        .unwrap();
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![&b"b/0000"[..], &b"b/0001"[..]]);
    assert_eq!(crate::kv::prefix_end(b"a\xff\xff"), Some(b"b".to_vec()));
    assert_eq!(crate::kv::prefix_end(b"\xff"), None);
}

#[tokio::test]
async fn limits_are_enforced() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let max_key = (VOLBLOCK_ALIGN as usize - 4) / 16;
    assert!(fs.kv_put("t", b"", b"v").await.is_err());
    assert!(
        fs.kv_put("t", &vec![b'k'; max_key + 1], b"v")
            .await
            .is_err()
    );
    fs.kv_put("t", &vec![b'k'; max_key], b"v").await.unwrap();
    assert!(
        fs.kv_put("t", b"big", &vec![0; crate::btree::MAX_VALUE + 1])
            .await
            .is_err()
    );
    // A volume isn't a kv dataset, and the other way round.
    fs.create_block("vol", 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    assert!(fs.kv_get("vol", b"k").await.is_err());
    assert!(fs.block_read("t", 0, &mut [0u8; 8]).await.is_err());
}
