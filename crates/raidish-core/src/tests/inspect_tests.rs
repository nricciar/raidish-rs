use super::*;
use crate::test_util::TestPool;
use std::path::Path;

fn span(start: u64, len: u64, kind: BlockKind, owner: Option<u32>) -> Span {
    Span {
        start,
        len,
        kind,
        owner,
    }
}

#[test]
fn flatten_resolves_overlaps_by_priority() {
    let spans = vec![
        span(0, 100, BlockKind::Free, None),
        span(10, 5, BlockKind::Data, Some(0)),
        span(0, 1, BlockKind::Superblock, None),
    ];
    let flat = flatten(spans, 100);
    assert_eq!(
        flat,
        vec![
            span(0, 1, BlockKind::Superblock, None),
            span(1, 9, BlockKind::Free, None),
            span(10, 5, BlockKind::Data, Some(0)),
            span(15, 85, BlockKind::Free, None),
        ]
    );
}

#[test]
fn buckets_count_kinds_and_owners() {
    let layout = Layout {
        total_blocks: 100,
        spans: flatten(
            vec![
                span(0, 30, BlockKind::Data, Some(0)),
                span(30, 10, BlockKind::Data, Some(1)),
                span(50, 50, BlockKind::Free, None),
            ],
            100,
        ),
        owners: vec!["/a".into(), "/b".into()],
    };
    let b = layout.buckets(0, 100, 4);
    assert_eq!(b.len(), 4);
    assert_eq!(b[0].counts[kind_index(BlockKind::Data)], 25);
    assert_eq!(b[0].owner, Some(0));
    // 5 of /a, 10 of /b, 10 unattributed.
    assert_eq!(b[1].counts[kind_index(BlockKind::Data)], 15);
    assert_eq!(b[1].counts[kind_index(BlockKind::Other)], 10);
    assert_eq!(b[1].owner, Some(1));
    assert_eq!(b[3].dominant(), BlockKind::Free);
    assert_eq!(b[3].fill(), 0.0);
    for bucket in &b {
        assert_eq!(bucket.counts.iter().sum::<u64>(), bucket.len);
    }

    // A zoomed window smaller than the bucket count: one block per cell.
    let z = layout.buckets(28, 4, 100);
    assert_eq!(z.len(), 4);
    assert_eq!(
        z.iter().map(|b| b.owner).collect::<Vec<_>>(),
        [Some(0), Some(0), Some(1), Some(1)]
    );
    assert_eq!(layout.span_at(31).unwrap().owner, Some(1));
    assert!(layout.span_at(45).is_none());
}

#[tokio::test]
async fn reports_a_formatted_pool() {
    let pool = TestPool::new().await;
    let fs = pool.format().await;
    fs.mkdir(Path::new("/dir")).await.unwrap();
    fs.write_file(Path::new("/dir/hello"), &vec![7u8; 100_000])
        .await
        .unwrap();
    fs.create_block("vol", 1 << 20, fs.default_volblocksize())
        .await
        .unwrap();
    fs.block_write("vol", 0, &vec![1u8; 64 * 1024])
        .await
        .unwrap();
    fs.sync().await.unwrap();

    let report = fs.pool_report().await;
    assert_eq!(report.health, Health::Online);
    assert_eq!(report.fault_tolerance, 2);
    assert_eq!(report.failures_left, 2);
    assert_eq!(report.disks.len(), 5);
    assert!(report.disks.iter().all(|d| d.online && d.local));
    assert!(report.txg > 0);
    assert!(report.used_blocks > 0);
    assert_eq!(
        report.used_blocks + report.free_blocks + report.deferred_blocks,
        fs.metaslab_reports()
            .await
            .iter()
            .map(|m| m.blocks)
            .sum::<u64>()
    );

    let (layout, objects) = fs.layout().await;
    let paths: Vec<_> = objects.iter().map(|o| (o.path.as_str(), o.kind)).collect();
    assert_eq!(
        paths,
        [
            ("/dir", ObjectKind::Directory),
            ("/dir/hello", ObjectKind::File),
            ("/vol", ObjectKind::Volume)
        ]
    );
    let hello = &objects[1];
    assert_eq!(hello.size_bytes, 100_000);
    assert!(hello.allocated_blocks >= 100_000 / 4096);
    let totals = layout.totals();
    assert_eq!(totals.iter().sum::<u64>(), layout.total_blocks);
    assert_eq!(
        totals[kind_index(BlockKind::Superblock)],
        fs.header.superblock_blocks
    );
    // Every object's data shows up in the map under its name.
    let owner = layout
        .owners
        .iter()
        .position(|o| o == "/dir/hello")
        .unwrap() as u32;
    assert!(
        layout
            .spans
            .iter()
            .any(|s| s.owner == Some(owner) && s.kind == BlockKind::Data)
    );

    let (records, _) = fs.volume_fragmentation("/vol").await.unwrap();
    assert!(records > 0);
}

#[tokio::test]
async fn missing_disks_degrade_then_fault() {
    let pool = TestPool::new().await;
    drop(pool.format().await);
    let fs = FileSystem::load_readonly(pool.dev_without(&[1]).await)
        .await
        .unwrap();
    let r = fs.pool_report().await;
    assert_eq!(r.health, Health::Degraded);
    assert_eq!(r.failures_left, 1);
    assert!(!r.disks[1].online);

    let fs = FileSystem::load_readonly(pool.dev_without(&[1, 3]).await)
        .await
        .unwrap();
    assert_eq!(fs.pool_report().await.failures_left, 0);
}
