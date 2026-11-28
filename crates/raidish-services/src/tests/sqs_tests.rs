//! SQS queues as stores: sends, receives, visibility, receipts, delays,
//! dead letters, retention, purges, reloads.

use super::*;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::vdev::Pool;
use raidish_core::role::{Committer, CommitterFs};

async fn queue(pool: &TestPool) -> (Arc<CommitterFs<Pool>>, Arc<Queue<Pool, Committer>>) {
    let fs = Arc::new(pool.format().await);
    fs.create_service(&ds("q"), DatasetFormat::Queue, Default::default())
        .await
        .unwrap();
    let q = Queue::open(fs.clone(), &ds("q")).await.unwrap();
    (fs, q)
}

fn msg(body: &str) -> NewMsg {
    NewMsg {
        body: body.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn send_receive_delete() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    let sent = q.send(vec![msg("a"), msg("b"), msg("")]).await.unwrap();
    assert!(sent[0].is_ok() && sent[1].is_ok());
    assert!(matches!(
        sent[2],
        Err(SqsError::Invalid("MissingParameter", _))
    ));
    assert_eq!(sent[0].as_ref().unwrap().md5_body, md5_hex(b"a"));
    assert_eq!(q.meta().await.unwrap().messages, 2);

    let (got, dead) = q.receive(10, Some(30)).await.unwrap();
    assert!(dead.is_empty());
    let bodies: Vec<_> = got.iter().map(|r| r.msg.body.as_str()).collect();
    assert_eq!(bodies, ["a", "b"]);
    assert_eq!(got[0].msg.receive_count, 1);
    // Hidden now.
    assert!(q.receive(10, None).await.unwrap().0.is_empty());
    assert_eq!(
        q.counts().await.unwrap(),
        Counts {
            visible: 0,
            inflight: 2,
            delayed: 0
        }
    );

    // Back to visible, received again: the first receipt is stale.
    let old = got[0].receipt.clone();
    q.change_visibility(vec![(old.clone(), 0)]).await.unwrap()[0]
        .clone()
        .unwrap();
    let (again, _) = q.receive(1, Some(30)).await.unwrap();
    assert_eq!(again[0].msg.body, "a");
    assert_eq!(again[0].msg.receive_count, 2);
    q.delete(vec![old.clone()]).await.unwrap()[0]
        .clone()
        .unwrap();
    assert_eq!(
        q.meta().await.unwrap().messages,
        2,
        "a stale receipt deletes nothing"
    );
    assert!(matches!(
        q.change_visibility(vec![(old, 5)]).await.unwrap()[0],
        Err(SqsError::ReceiptHandleIsInvalid(_))
    ));
    q.delete(vec![again[0].receipt.clone(), got[1].receipt.clone()])
        .await
        .unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 0);
    assert_eq!(
        q.counts().await.unwrap(),
        Counts {
            visible: 0,
            inflight: 0,
            delayed: 0
        }
    );
    assert!(matches!(
        q.delete(vec!["junk".into()]).await.unwrap()[0],
        Err(SqsError::ReceiptHandleIsInvalid(_))
    ));
}

#[tokio::test]
async fn delays_waits_and_attributes() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.send(vec![NewMsg {
        delay: Some(1),
        ..msg("later")
    }])
    .await
    .unwrap();
    assert_eq!(
        q.counts().await.unwrap(),
        Counts {
            visible: 0,
            inflight: 0,
            delayed: 1
        }
    );
    assert!(q.receive(1, None).await.unwrap().0.is_empty());
    // A long poll sees it once due.
    let t = std::time::Instant::now();
    let (got, _) = q.receive_wait(1, None, Some(5)).await.unwrap();
    assert_eq!(got[0].msg.body, "later");
    assert!(t.elapsed() < Duration::from_secs(4));

    // A long poll woken by a send.
    let q2 = q.clone();
    let waiter = tokio::spawn(async move { q2.receive_wait(1, None, Some(10)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    q.send(vec![msg("now")]).await.unwrap();
    let (got, _) = waiter.await.unwrap().unwrap();
    assert_eq!(got[0].msg.body, "now");

    // Attributes: checked, all or nothing.
    q.set_attrs(vec![("VisibilityTimeout".into(), "5".into())])
        .await
        .unwrap();
    assert!(
        q.set_attrs(vec![
            ("DelaySeconds".into(), "3".into()),
            ("VisibilityTimeout".into(), "99999".into())
        ])
        .await
        .is_err()
    );
    let a = q.meta().await.unwrap().attrs;
    assert_eq!((a.visibility_timeout, a.delay_seconds), (5, 0));
    assert!(
        q.set_attrs(vec![("FifoQueue".into(), "true".into())])
            .await
            .is_err()
    );
    // Message attributes, checked.
    let bad = NewMsg {
        attrs: vec![(
            "n".into(),
            MsgAttr {
                data_type: "Number".into(),
                string: Some("x".into()),
                binary: None,
            },
        )],
        ..msg("m")
    };
    assert!(q.send(vec![bad]).await.unwrap()[0].is_err());
    assert!(q.send(vec![msg("\u{0}")]).await.unwrap()[0].is_err());
}

#[tokio::test]
async fn dead_letters_retention_purge() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.set_attrs(vec![(
        "RedrivePolicy".into(),
        r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":"2"}"#
            .into(),
    )])
    .await
    .unwrap();
    q.send(vec![msg("poison")]).await.unwrap();
    for n in 1..=2 {
        let (got, dead) = q.receive(1, Some(0)).await.unwrap();
        assert_eq!((got.len(), dead.len(), got[0].msg.receive_count), (1, 0, n));
    }
    let (got, dead) = q.receive(1, Some(0)).await.unwrap();
    assert_eq!((got.len(), dead.len()), (0, 1));
    // Hidden while it moves; gone once moved.
    assert!(q.receive(1, Some(0)).await.unwrap().1.is_empty());
    q.remove(Receipt {
        seq: dead[0].seq,
        nonce: dead[0].msg.nonce,
    })
    .await
    .unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 0);

    // Retention: old messages go (in the background, and on receive).
    let old = crate::cluster::now_ms() - 400_000 * 1000;
    q.send(vec![
        NewMsg {
            sent_ms: Some(old),
            ..msg("old1")
        },
        NewMsg {
            sent_ms: Some(old),
            ..msg("old2")
        },
        msg("new"),
    ])
    .await
    .unwrap();
    assert_eq!(q.sweep().await.unwrap(), 2);
    let (got, _) = q.receive(10, None).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].msg.body, "new");

    for i in 0..700 {
        q.send(vec![msg(&format!("m{}", i))]).await.unwrap();
    }
    q.purge().await.unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 0);
    assert_eq!(
        q.counts().await.unwrap(),
        Counts {
            visible: 0,
            inflight: 0,
            delayed: 0
        }
    );
}

#[tokio::test]
async fn survives_a_reload() {
    let pool = TestPool::new().await;
    let (fs, q) = queue(&pool).await;
    q.set_attrs(vec![("DelaySeconds".into(), "0".into())])
        .await
        .unwrap();
    q.send(vec![msg("kept"), msg("taken")]).await.unwrap();
    let (got, _) = q.receive(1, Some(600)).await.unwrap();
    assert_eq!(got[0].msg.body, "kept");
    q.stop();
    drop(q);
    drop(fs);
    let fs = Arc::new(pool.load().await);
    let q = Queue::open(fs.clone(), &ds("q")).await.unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 2);
    let (got2, _) = q.receive(10, None).await.unwrap();
    // "kept" is still in flight.
    assert_eq!(got2.len(), 1);
    assert_eq!(got2[0].msg.body, "taken");
    q.delete(vec![got[0].receipt.clone()]).await.unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 1);
    let report = fs.fsck().await.unwrap();
    assert!(report.is_clean(), "{}", report);
}

#[test]
fn attribute_md5() {
    // As SQS computes it: sorted by name, lengths big-endian.
    let attrs = vec![(
        "a".to_string(),
        MsgAttr {
            data_type: "String".into(),
            string: Some("b".into()),
            binary: None,
        },
    )];
    let mut raw = Vec::new();
    raw.extend(1u32.to_be_bytes());
    raw.extend(b"a");
    raw.extend(6u32.to_be_bytes());
    raw.extend(b"String");
    raw.push(1);
    raw.extend(1u32.to_be_bytes());
    raw.extend(b"b");
    assert_eq!(md5_of_attrs(&attrs).unwrap(), md5_hex(&raw));
    assert_eq!(md5_of_attrs(&[]), None);
}

/// Q1: with a visibility timeout of 0, a message is visible again at once,
/// but one receive returns it once, not again further on in its scan.
#[tokio::test]
async fn a_zero_visibility_receive_returns_each_message_once() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.send(vec![msg("a"), msg("b"), msg("c")]).await.unwrap();
    let (got, _) = q.receive(10, Some(0)).await.unwrap();
    let mut bodies: Vec<_> = got.iter().map(|r| r.msg.body.as_str()).collect();
    bodies.sort_unstable();
    assert_eq!(bodies, ["a", "b", "c"]);
    assert!(got.iter().all(|r| r.msg.receive_count == 1));
    // Still visible: the next receive gets them all again.
    assert_eq!(q.receive(10, Some(0)).await.unwrap().0.len(), 3);
}

/// Q1: a receipt naming nonce 0 (what a never-received message has) is
/// invalid: it can't delete or hide a message nobody received.
#[tokio::test]
async fn a_receipt_with_nonce_zero_is_refused() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.send(vec![msg("unseen")]).await.unwrap();
    let forged = Receipt { seq: 0, nonce: 0 }.encode();
    assert!(matches!(
        Receipt::decode(&forged),
        Err(SqsError::ReceiptHandleIsInvalid(_))
    ));
    let r = q.delete(vec![forged.clone()]).await.unwrap();
    assert!(r[0].is_err(), "a forged receipt deleted a message");
    let r = q.change_visibility(vec![(forged, 300)]).await.unwrap();
    assert!(r[0].is_err(), "a forged receipt hid a message");
    assert_eq!(q.meta().await.unwrap().messages, 1);
    let (got, _) = q.receive(1, None).await.unwrap();
    assert_eq!(got[0].msg.body, "unseen");
}

/// Q1: once a message is taken for a dead-letter move, its last
/// consumer's receipt no longer deletes it or changes its visibility; the
/// move's own removal still works.
#[tokio::test]
async fn a_dead_letter_move_ends_the_last_receipt() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.set_attrs(vec![(
        "RedrivePolicy".into(),
        r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":"1"}"#
            .into(),
    )])
    .await
    .unwrap();
    q.send(vec![msg("poison")]).await.unwrap();
    let (got, _) = q.receive(1, Some(0)).await.unwrap();
    let last = got[0].receipt.clone();
    let (_, dead) = q.receive(1, Some(0)).await.unwrap();
    assert_eq!(dead.len(), 1);
    let r = q.change_visibility(vec![(last.clone(), 0)]).await.unwrap();
    assert!(r[0].is_err(), "the old receipt still changes visibility");
    q.delete(vec![last]).await.unwrap();
    assert_eq!(
        q.meta().await.unwrap().messages,
        1,
        "the old receipt deleted it"
    );
    q.remove(Receipt {
        seq: dead[0].seq,
        nonce: dead[0].msg.nonce,
    })
    .await
    .unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 0);
}

/// I-34: the retention and receive-count arithmetic saturates, whatever a
/// message's history says. A `sent_ms` near `u64::MAX` used to overflow
/// `sent_ms + retention` (a panic in a debug build; in release, a wrap
/// that read as long expired), and a count at `u32::MAX` wrapped to 0.
#[tokio::test]
async fn a_message_history_at_the_limits_doesnt_overflow() {
    let pool = TestPool::new().await;
    let (_fs, q) = queue(&pool).await;
    q.send(vec![NewMsg {
        sent_ms: Some(u64::MAX - 10),
        receive_count: u32::MAX,
        ..msg("edge")
    }])
    .await
    .unwrap()
    .remove(0)
    .unwrap();
    q.sweep().await.unwrap();
    assert_eq!(q.meta().await.unwrap().messages, 1, "swept as expired");
    let (got, _) = q.receive(1, Some(0)).await.unwrap();
    assert_eq!(got[0].msg.body, "edge");
    assert_eq!(got[0].msg.receive_count, u32::MAX);
}
