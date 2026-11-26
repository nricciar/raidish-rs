//! The B+tree against a `BTreeMap` model, through `kv` datasets with the
//! smallest nodes the test pool allows (so trees get deep quickly).

use crate::disk::PoolDevice;
use crate::fs::FileSystem;
use crate::test_util::ds;
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
    let name = &ds(name);
    let fs = pool.format().await;
    fs.create_kv_with(name, VOLBLOCK_ALIGN, 1 << 20)
        .await
        .unwrap();
    fs
}

/// The committed tree is sound and holds exactly `model`.
async fn verify<D: PoolDevice>(fs: &FileSystem<D>, name: &str, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let name = &ds(name);
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
                assert_eq!(fs.kv_delete(&ds("t"), &k).await.unwrap(), had);
            } else {
                let v = value(i, round, rng.below(300) as usize);
                let had = model.insert(k.clone(), v.clone()).is_some();
                assert_eq!(fs.kv_put(&ds("t"), &k, &v).await.unwrap(), had);
            }
        }
        verify(&fs, "t", &model).await;
    }
    // Spot checks by key.
    for (k, v) in model.iter().step_by(37) {
        assert_eq!(fs.kv_get(&ds("t"), k).await.unwrap().as_ref(), Some(v));
    }
    assert_eq!(fs.kv_get(&ds("t"), b"absent").await.unwrap(), None);
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
        fs.kv_put(&ds("t"), &k, &v).await.unwrap();
        model.insert(k, v);
    }
    verify(&fs, "t", &model).await;
    let t = {
        let ds = fs.open_kv(&ds("t")).await.unwrap();
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
        assert!(fs.kv_delete(&ds("t"), k).await.unwrap());
        model.remove(k);
        if n % 600 == 0 {
            verify(&fs, "t", &model).await;
        }
    }
    verify(&fs, "t", &model).await;
    let h = {
        let ds = fs.open_kv(&ds("t")).await.unwrap();
        crate::btree::Txn::begin(&fs, &ds.obj).await.unwrap().header
    };
    assert_eq!((h.root, h.height, h.entries), (None, 0, 0));
    // Every node's record was freed: what's left is the header and the
    // pages the spilled free ids are listed in, and every other id is free.
    let id = fs.dataset_id(&ds("t")).await.unwrap();
    let d = fs.read_dnode(id).await.unwrap();
    let leaves = fs.tree_leaves(&d.tree).await.unwrap();
    assert_eq!(leaves[0].0, 0);
    let lists = leaves.len() as u64 - 1;
    assert_eq!(h.free_chain.is_some(), lists > 0);
    assert_eq!(h.free_total + lists, h.next_id - 1);
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
        fs.kv_put(&ds("t"), &k, &v).await.unwrap();
        model.insert(k, v);
    }
    verify(&fs, "t", &model).await;
    // Replacing and deleting large values frees their pages.
    let big = key(8, 10);
    fs.kv_put(&ds("t"), &big, b"small now").await.unwrap();
    model.insert(big.clone(), b"small now".to_vec());
    fs.kv_delete(&ds("t"), &key(7, 10)).await.unwrap();
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
            fs.kv_put(&ds("t"), &k, &k).await.unwrap();
            model.insert(k.clone(), k);
        }
    }
    // A prefix, paged.
    let mut got = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = fs
            .kv_scan_prefix(&ds("t"), b"b/", after.as_deref(), 77)
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
            &ds("t"),
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
    assert!(fs.kv_put(&ds("t"), b"", b"v").await.is_err());
    assert!(
        fs.kv_put(&ds("t"), &vec![b'k'; max_key + 1], b"v")
            .await
            .is_err()
    );
    fs.kv_put(&ds("t"), &vec![b'k'; max_key], b"v")
        .await
        .unwrap();
    assert!(
        fs.kv_put(&ds("t"), b"big", &vec![0; crate::btree::MAX_VALUE + 1])
            .await
            .is_err()
    );
    // A volume isn't a kv dataset, and the other way round.
    fs.create_block(&ds("vol"), 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    assert!(fs.kv_get(&ds("vol"), b"k").await.is_err());
    assert!(fs.block_read(&ds("t"), 0, &mut [0u8; 8]).await.is_err());
}

#[test]
fn decode_takes_all_of_a_record_or_nothing() {
    use super::{decode, encode};
    let mut buf = encode(&(7u64, 9u32)).unwrap();
    assert_eq!(decode::<(u64, u32)>(&buf, "t").unwrap(), (7, 9));
    // Longer than the record says it is: trailing bytes are refused.
    let longer = (buf.len() - 4 + 2) as u32;
    buf[..4].copy_from_slice(&longer.to_le_bytes());
    buf.extend([0, 0]);
    assert!(decode::<(u64, u32)>(&buf, "t").is_err());
    // A length past the buffer.
    let mut short = encode(&1u64).unwrap();
    short[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode::<u64>(&short, "t").is_err());
}

/// Overflow pages that are all zeros are committed as holes: they must
/// read back as zeros (and pass the tree check), not as missing pages.
#[tokio::test]
async fn zero_overflow_pages_read_back() {
    let pool = TestPool::new().await;
    let fs = small_kv(&pool, "t").await;
    let mut model = BTreeMap::new();
    let mut middle = value(1, 0, 200_000);
    middle[40_000..120_000].fill(0);
    let mut tail = value(2, 0, 100_000);
    tail[50_000..].fill(0);
    for (i, v) in [vec![0u8; 100_000], middle, tail].into_iter().enumerate() {
        let k = key(i as u64, 10);
        fs.kv_put(&ds("t"), &k, &v).await.unwrap();
        model.insert(k, v);
    }
    verify(&fs, "t", &model).await;
    for (k, v) in &model {
        assert_eq!(fs.kv_get(&ds("t"), k).await.unwrap().as_ref(), Some(v));
    }
}

/// S6c-1: a long-lived tree whose large values keep changing reuses the
/// ids it frees: `next_id` stays near the most it held at once (here a few
/// hundred), far below what the churn wrote in all (tens of thousands of
/// pages), and the tree check finds every id used or free.
#[tokio::test]
async fn churning_large_values_reuses_freed_ids() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let capacity = 4096;
    fs.create_kv_with(&ds("t"), VOLBLOCK_ALIGN, capacity)
        .await
        .unwrap();
    let blksz = VOLBLOCK_ALIGN as usize;
    let mut model = BTreeMap::new();
    let mut rng = Rng(7);
    let mut pages_written = 0;
    for round in 0..2000u64 {
        let k = key(rng.below(8), 10);
        if rng.below(5) == 0 {
            fs.kv_delete(&ds("t"), &k).await.unwrap();
            model.remove(&k);
        } else {
            let len = blksz + rng.below(30 * blksz as u64) as usize;
            pages_written += len.div_ceil(blksz);
            let v = value(round, round, len);
            fs.kv_put(&ds("t"), &k, &v)
                .await
                .unwrap_or_else(|e| panic!("round {}: {}", round, e));
            model.insert(k, v);
        }
        if round % 500 == 499 {
            verify(&fs, "t", &model).await;
        }
    }
    verify(&fs, "t", &model).await;
    let h = {
        let ds = fs.open_kv(&ds("t")).await.unwrap();
        crate::btree::Txn::begin(&fs, &ds.obj).await.unwrap().header
    };
    assert!(pages_written > 5 * capacity as usize);
    // 8 values of at most 31 pages, an id list each, a replacement in
    // flight, the leaves and the free lists' pages.
    assert!(h.next_id < 600, "next_id {} after churn", h.next_id);
}

/// S6c-1: freeing far more ids at once than the header's free list holds
/// (deleting a big set of large values) keeps them all, spilled into id
/// lists, and filling the tree again reuses them rather than new ids.
#[tokio::test]
async fn ids_freed_in_bulk_are_all_kept_for_reuse() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    let capacity = 4096;
    fs.create_kv_with(&ds("t"), VOLBLOCK_ALIGN, capacity)
        .await
        .unwrap();
    let blksz = VOLBLOCK_ALIGN as usize;
    let mut first_next_id = None;
    for cycle in 0..6u64 {
        let mut model = BTreeMap::new();
        for i in 0..40 {
            let (k, v) = (key(cycle * 100 + i, 10), value(i, cycle, 30 * blksz));
            fs.kv_put(&ds("t"), &k, &v)
                .await
                .unwrap_or_else(|e| panic!("cycle {} put {}: {}", cycle, i, e));
            model.insert(k, v);
        }
        verify(&fs, "t", &model).await;
        let next_id = {
            let ds = fs.open_kv(&ds("t")).await.unwrap();
            crate::btree::Txn::begin(&fs, &ds.obj)
                .await
                .unwrap()
                .header
                .next_id
        };
        let first = *first_next_id.get_or_insert(next_id);
        assert!(
            next_id <= first + 64,
            "cycle {}: next_id {} (first fill: {})",
            cycle,
            next_id,
            first
        );
        for k in model.keys() {
            fs.kv_delete(&ds("t"), k).await.unwrap();
        }
        verify(&fs, "t", &BTreeMap::new()).await;
    }
}

/// Nodes' keys and values are serde byte strings: the encoding must stay
/// exactly what the derived `Vec<u8>` encoding wrote (existing pools).
#[test]
fn node_encoding_matches_the_vec_u8_encoding() {
    use super::{Node, Val, decode, encode, size_of};
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    enum OldVal {
        Inline(Vec<u8>),
        Overflow { index: u64, len: u64 },
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    enum OldNode {
        Leaf(Vec<(Vec<u8>, OldVal)>),
        Branch {
            keys: Vec<Vec<u8>>,
            children: Vec<u64>,
        },
    }
    let leaf = Node::Leaf(vec![
        (
            b"/registry/pods/default/a".to_vec(),
            Val::Inline(vec![0, 1, 2, 255]),
        ),
        (vec![], Val::Inline(vec![])),
        (
            b"k".to_vec(),
            Val::Overflow {
                index: 7,
                len: 1 << 20,
            },
        ),
    ]);
    let old_leaf = OldNode::Leaf(vec![
        (
            b"/registry/pods/default/a".to_vec(),
            OldVal::Inline(vec![0, 1, 2, 255]),
        ),
        (vec![], OldVal::Inline(vec![])),
        (
            b"k".to_vec(),
            OldVal::Overflow {
                index: 7,
                len: 1 << 20,
            },
        ),
    ]);
    let branch = Node::Branch {
        keys: vec![b"m".to_vec(), vec![9; 300]],
        children: vec![1, 2, 3],
    };
    let old_branch = OldNode::Branch {
        keys: vec![b"m".to_vec(), vec![9; 300]],
        children: vec![1, 2, 3],
    };
    for (new, old) in [(leaf, old_leaf), (branch, old_branch)] {
        let a = encode(&new).unwrap();
        let b = encode(&old).unwrap();
        assert_eq!(a, b);
        assert_eq!(size_of(&new), a.len());
        assert_eq!(decode::<Node>(&b, "n").unwrap(), new);
        assert_eq!(decode::<OldNode>(&a, "n").unwrap(), old);
    }
}
