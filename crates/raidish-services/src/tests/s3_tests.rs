//! S3 buckets as stores: objects, listings, multipart uploads, bodies
//! deleted once unused, reloads.

use super::*;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::vdev::Pool;
use raidish_core::role::{Committer, CommitterFs};

async fn bucket(pool: &TestPool) -> (Arc<CommitterFs<Pool>>, Arc<Bucket<Pool, Committer>>) {
    let fs = Arc::new(pool.format().await);
    fs.create_service(&ds("b"), DatasetFormat::S3, Default::default())
        .await
        .unwrap();
    let b = Bucket::open(fs.clone(), &ds("b")).await.unwrap();
    (fs, b)
}

fn data(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31) ^ seed).collect()
}

async fn put(b: &Arc<Bucket<Pool, Committer>>, key: &str, body: &[u8]) -> ObjectMeta {
    let mut w = b.new_body(Some(body.len() as u64)).await.unwrap();
    // In uneven pieces, as a network delivers them.
    for piece in body.chunks(777_777) {
        w.write(piece).await.unwrap();
    }
    let (seg, md5) = w.finish().await.unwrap();
    let meta = ObjectMeta {
        size: seg.size,
        etag: hex::encode(md5),
        last_modified_ms: 1,
        content_type: Some("application/octet-stream".into()),
        headers: vec![],
        user: vec![("k".into(), "v".into())],
        segments: vec![seg],
    };
    b.put_object(key, meta.clone()).await.unwrap();
    meta
}

async fn get(b: &Arc<Bucket<Pool, Committer>>, key: &str) -> Option<Vec<u8>> {
    let (meta, _hold) = b.head_held(key).await.unwrap()?;
    let mut out = Vec::new();
    for s in &meta.segments {
        let mut chunk = 0;
        while (out.len() as u64) < meta.size {
            match b.read_chunk(s.body, chunk).await.unwrap() {
                Some(c) => out.extend(c),
                None => break,
            }
            chunk += 1;
        }
    }
    Some(out)
}

/// Body chunks in the tree.
async fn chunks(fs: &CommitterFs<Pool>) -> usize {
    let ds = fs.open_btree(&ds("b"), DatasetFormat::S3).await.unwrap();
    let t = Txn::begin(fs, &ds.obj).await.unwrap();
    t.range_raw(Bound::Included(b"B"), Bound::Excluded(b"C"), usize::MAX)
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn objects_small_and_large() {
    let pool = TestPool::new().await;
    let (fs, b) = bucket(&pool).await;
    let small = data(1000, 1);
    let large = data(CHUNK * 2 + 12345, 2);
    let m = put(&b, "small", &small).await;
    assert_eq!(m.etag, hex::encode(Md5::digest(&small)));
    put(&b, "dir/large", &large).await;
    put(&b, "empty", b"").await;
    assert_eq!(get(&b, "small").await.unwrap(), small);
    assert_eq!(get(&b, "dir/large").await.unwrap(), large);
    assert_eq!(get(&b, "empty").await.unwrap(), b"");
    assert_eq!(get(&b, "nope").await, None);
    assert_eq!(chunks(&fs).await, 1 + 3);
    // Replaced, then deleted: the old bodies go.
    put(&b, "dir/large", b"now small").await;
    assert!(b.delete_object("small").await.unwrap());
    assert!(!b.delete_object("small").await.unwrap());
    b.collect().await.unwrap();
    assert_eq!(chunks(&fs).await, 1);
    assert_eq!(
        b.head_held("dir/large")
            .await
            .map(|o| o.map(|(m, _)| m))
            .unwrap()
            .unwrap()
            .size,
        9
    );
}

#[tokio::test]
async fn bodies_being_read_stay() {
    let pool = TestPool::new().await;
    let (fs, b) = bucket(&pool).await;
    let m = put(&b, "k", &data(5000, 3)).await;
    let (_, hold) = b.head_held("k").await.unwrap().unwrap();
    b.delete_object("k").await.unwrap();
    b.collect().await.unwrap();
    // Still readable by whoever holds it.
    assert!(b.read_chunk(m.segments[0].body, 0).await.unwrap().is_some());
    drop(hold);
    b.collect().await.unwrap();
    assert_eq!(chunks(&fs).await, 0);
}

#[tokio::test]
async fn listings_with_prefixes_and_delimiters() {
    let pool = TestPool::new().await;
    let (_fs, b) = bucket(&pool).await;
    for k in [
        "a.txt",
        "photos/2023/1.jpg",
        "photos/2023/2.jpg",
        "photos/2024/1.jpg",
        "photos/x.jpg",
        "videos/v.mp4",
        "z",
    ] {
        put(&b, k, k.as_bytes()).await;
    }
    let keys = |l: &Listing| l.objects.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    let l = b.list("", None, Some("/"), 1000).await.unwrap();
    assert_eq!(keys(&l), vec!["a.txt", "z"]);
    assert_eq!(l.prefixes, vec!["photos/", "videos/"]);
    let l = b.list("photos/", None, Some("/"), 1000).await.unwrap();
    assert_eq!(keys(&l), vec!["photos/x.jpg"]);
    assert_eq!(l.prefixes, vec!["photos/2023/", "photos/2024/"]);
    // A marker after the prefix but not under it (or not on a character
    // boundary of it) is just a key to list after, not a panic.
    for marker in ["q/", "photos", "é/", "photos\u{e9}/"] {
        b.list("photos/", Some(marker), Some("/"), 1000)
            .await
            .unwrap();
    }
    // The prefix itself as the marker (audit X2: once an underflow): all
    // of it, as with no marker.
    let l = b
        .list("photos/", Some("photos/"), Some("/"), 1000)
        .await
        .unwrap();
    assert_eq!(keys(&l), vec!["photos/x.jpg"]);
    assert_eq!(l.prefixes, vec!["photos/2023/", "photos/2024/"]);
    // After a rolled-up prefix: past everything under it.
    let l = b
        .list("photos/", Some("photos/2023/"), Some("/"), 1000)
        .await
        .unwrap();
    assert_eq!(keys(&l), vec!["photos/x.jpg"]);
    assert_eq!(l.prefixes, vec!["photos/2024/"]);
    let l = b
        .list("phot\u{e9}s/", Some("photos/"), Some("/"), 1000)
        .await
        .unwrap();
    assert!(l.objects.is_empty());
    // Paged: two at a time, prefixes counting.
    let mut seen = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let l = b.list("", after.as_deref(), Some("/"), 2).await.unwrap();
        seen.extend(keys(&l));
        seen.extend(l.prefixes.clone());
        if !l.truncated {
            break;
        }
        after = l.last.clone();
    }
    seen.sort();
    assert_eq!(seen, vec!["a.txt", "photos/", "videos/", "z"]);
    // No delimiter: everything, in order.
    let l = b.list("photos/", None, None, 1000).await.unwrap();
    assert_eq!(l.objects.len(), 4);
    let l = b
        .list("", Some("photos/2024/1.jpg"), None, 2)
        .await
        .unwrap();
    assert_eq!(keys(&l), vec!["photos/x.jpg", "videos/v.mp4"]);
    assert!(l.truncated);
}

#[tokio::test]
async fn multipart_uploads() {
    let pool = TestPool::new().await;
    let (fs, b) = bucket(&pool).await;
    let template = ObjectMeta {
        size: 0,
        etag: String::new(),
        last_modified_ms: 0,
        content_type: Some("text/plain".into()),
        headers: vec![],
        user: vec![],
        segments: vec![],
    };
    let id = b.create_upload("big", template.clone()).await.unwrap();
    let mut parts = Vec::new();
    for (n, len) in [
        (1u32, MIN_PART as usize),
        (2, MIN_PART as usize + 1),
        (3, 10),
    ] {
        let body = data(len, n as u8);
        let mut w = b.new_body(Some(len as u64)).await.unwrap();
        w.write(&body).await.unwrap();
        let (seg, md5) = w.finish().await.unwrap();
        b.put_part(
            &id,
            n,
            Part {
                body: seg.body,
                size: seg.size,
                md5,
                last_modified_ms: 1,
            },
        )
        .await
        .unwrap();
        parts.push((n, body, hex::encode(md5)));
    }
    // Part 2 again: replaces it.
    let body2 = data(MIN_PART as usize, 9);
    let mut w = b.new_body(None).await.unwrap();
    w.write(&body2).await.unwrap();
    let (seg, md5) = w.finish().await.unwrap();
    b.put_part(
        &id,
        2,
        Part {
            body: seg.body,
            size: seg.size,
            md5,
            last_modified_ms: 2,
        },
    )
    .await
    .unwrap();
    parts[1] = (2, body2, hex::encode(md5));
    let listed = b.parts(&id, 0, 1000).await.unwrap();
    assert_eq!(
        listed.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(b.uploads(None, 10).await.unwrap().len(), 1);
    // Refused: out of order, a wrong ETag, a small part before the last.
    let etags = |ns: &[usize]| {
        ns.iter()
            .map(|&i| (parts[i].0, parts[i].2.clone()))
            .collect::<Vec<_>>()
    };
    assert!(matches!(
        b.complete(&id, etags(&[1, 0])).await,
        Err(S3Error::InvalidPartOrder)
    ));
    assert!(matches!(
        b.complete(&id, vec![(1, "bad".into())]).await,
        Err(S3Error::InvalidPart(_))
    ));
    assert!(matches!(
        b.complete(&id, etags(&[2, 2])).await,
        Err(S3Error::InvalidPartOrder)
    ));
    let (key, meta) = b.complete(&id, etags(&[0, 1, 2])).await.unwrap();
    assert_eq!(key, "big");
    assert!(meta.etag.ends_with("-3"));
    let mut want = Vec::new();
    for p in &parts {
        want.extend_from_slice(&p.1);
    }
    assert_eq!(meta.size, want.len() as u64);
    assert_eq!(get(&b, "big").await.unwrap(), want);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    assert!(matches!(b.upload(&id).await, Err(S3Error::NoSuchUpload)));
    // An aborted upload leaves nothing.
    let id = b.create_upload("gone", template).await.unwrap();
    let mut w = b.new_body(None).await.unwrap();
    w.write(&data(100, 1)).await.unwrap();
    let (seg, md5) = w.finish().await.unwrap();
    b.put_part(
        &id,
        1,
        Part {
            body: seg.body,
            size: seg.size,
            md5,
            last_modified_ms: 1,
        },
    )
    .await
    .unwrap();
    b.abort(&id).await.unwrap();
    assert!(!b.is_empty().await.unwrap());
    b.delete_object("big").await.unwrap();
    assert!(b.is_empty().await.unwrap());
    b.collect().await.unwrap();
    assert_eq!(chunks(&fs).await, 0);
}

#[tokio::test]
async fn half_written_bodies_are_dropped_on_reopen() {
    let pool = TestPool::new().await;
    {
        let (fs, b) = bucket(&pool).await;
        put(&b, "kept", &data(CHUNK + 1, 5)).await;
        let mut w = b.new_body(None).await.unwrap();
        w.write(&data(CHUNK + 10, 6)).await.unwrap();
        // The server goes away mid-upload (its chunk so far committed).
        fs.sync().await.unwrap();
        b.stop();
    }
    let fs = Arc::new(pool.load().await);
    assert_eq!(chunks(&fs).await, 2 + 1);
    let b = Bucket::open(fs.clone(), &ds("b")).await.unwrap();
    b.collect().await.unwrap();
    assert_eq!(chunks(&fs).await, 2);
    assert_eq!(get(&b, "kept").await.unwrap(), data(CHUNK + 1, 5));
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}
