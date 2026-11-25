//! The SQS JSON API over HTTP, signed: queues, messages, batches,
//! attributes, dead-letter moves, errors.

use super::*;
use crate::frontend::testing::Fixture;
use crate::frontend::{Owner, Route};
use crate::pki::Role;
use crate::test_util::TestPool;
use crate::test_util::ds;
use crate::types::ep;
use std::collections::BTreeMap;

struct Sqs {
    addr: String,
    http: reqwest::Client,
    key: (String, String),
    fx: Fixture,
    _fe: Arc<Frontend>,
    _fs: Arc<crate::fs::FileSystem<Pool>>,
}

async fn server(pool: &TestPool) -> Sqs {
    let fs = Arc::new(pool.format().await);
    let fx = Fixture::new(vec![]);
    let fe = fx.frontend();
    let mut routes = BTreeMap::new();
    for q in ["q", "dlq"] {
        fs.create_service(&ds(q), DatasetFormat::Queue, Default::default())
            .await
            .unwrap();
        routes.insert(
            q.to_string(),
            Route {
                format: DatasetFormat::Queue,
                port: None,
                created_ms: 1,
                owner: Some(Owner {
                    node: "n1".into(),
                    addr: "127.0.0.1".into(),
                }),
            },
        );
    }
    fe.set_routes(routes, BTreeMap::new());
    for q in ["q", "dlq"] {
        fe.serve_local(&ds(q), DatasetFormat::Queue, fs.clone(), ep(1))
            .await
            .unwrap();
    }
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(serve(fe.clone(), l));
    Sqs {
        addr,
        http: fx.http(),
        key: fx.key(Role::Admin),
        fx,
        _fe: fe,
        _fs: fs,
    }
}

impl Sqs {
    async fn raw(&self, target: &str, body: &Value, sign: bool) -> (u16, String, Value) {
        let body = body.to_string().into_bytes();
        let uri: hyper::Uri = format!("https://{}/", self.addr).parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("host", HeaderValue::from_str(&self.addr).unwrap());
        h.insert(
            "content-type",
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        h.insert(
            "x-amz-target",
            HeaderValue::from_str(&format!("AmazonSQS.{}", target)).unwrap(),
        );
        let signed = if sign {
            sigv4::sign(
                &self.key.0,
                &self.key.1,
                "us-east-1",
                "sqs",
                &Method::POST,
                &uri,
                &h,
                &body,
            )
        } else {
            Vec::new()
        };
        let mut rb = self.http.post(format!("https://{}/", self.addr));
        for (k, v) in &h {
            if k != "host" {
                rb = rb.header(k.as_str(), v.to_str().unwrap());
            }
        }
        for (k, v) in signed {
            rb = rb.header(k, v);
        }
        let r = rb.body(body).send().await.unwrap();
        let status = r.status().as_u16();
        let qe = r
            .headers()
            .get("x-amzn-query-error")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        let v = r.json().await.unwrap_or(json!({}));
        (status, qe, v)
    }

    async fn ok(&self, target: &str, body: Value) -> Value {
        let (st, _, v) = self.raw(target, &body, true).await;
        assert_eq!(st, 200, "{} {}", target, v);
        v
    }

    async fn err(&self, target: &str, body: Value) -> (u16, String) {
        let (st, _, v) = self.raw(target, &body, true).await;
        assert_ne!(st, 200, "{} {}", target, v);
        (st, v["__type"].as_str().unwrap_or("").to_string())
    }

    fn url(&self, q: &str) -> String {
        format!("https://{}/{}/{}", self.addr, ACCOUNT, q)
    }
}

#[tokio::test]
async fn queues_and_messages() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let q = s.url("q");
    let v = s.ok("GetQueueUrl", json!({"QueueName": "q"})).await;
    assert_eq!(v["QueueUrl"], q);
    let v = s.ok("ListQueues", json!({})).await;
    assert_eq!(v["QueueUrls"].as_array().unwrap().len(), 2);
    let v = s.ok("ListQueues", json!({"QueueNamePrefix": "d"})).await;
    assert_eq!(v["QueueUrls"], json!([s.url("dlq")]));
    let v = s.ok("ListQueues", json!({"MaxResults": 1})).await;
    assert!(v["NextToken"].is_string());

    let v = s
        .ok(
            "SendMessage",
            json!({
                "QueueUrl": q,
                "MessageBody": "hello",
                "MessageAttributes": {
                    "color": {"DataType": "String", "StringValue": "blue"},
                    "blob": {"DataType": "Binary", "BinaryValue": "AAEC"},
                },
            }),
        )
        .await;
    assert_eq!(v["MD5OfMessageBody"], md5_hex(b"hello"));
    let attrs_md5 = v["MD5OfMessageAttributes"].as_str().unwrap().to_string();
    let v = s
        .ok(
            "ReceiveMessage",
            json!({
                "QueueUrl": q,
                "MaxNumberOfMessages": 10,
                "MessageAttributeNames": ["All"],
                "MessageSystemAttributeNames": ["All"],
            }),
        )
        .await;
    let m = &v["Messages"][0];
    assert_eq!(m["Body"], "hello");
    assert_eq!(m["MD5OfMessageAttributes"], attrs_md5.as_str());
    assert_eq!(m["MessageAttributes"]["blob"]["BinaryValue"], "AAEC");
    assert_eq!(m["Attributes"]["ApproximateReceiveCount"], "1");
    assert_eq!(m["Attributes"]["SenderId"], s.key.0.as_str());
    let receipt = m["ReceiptHandle"].as_str().unwrap().to_string();
    // Only the attributes asked for.
    s.ok(
        "ChangeMessageVisibility",
        json!({"QueueUrl": q, "ReceiptHandle": receipt, "VisibilityTimeout": 0}),
    )
    .await;
    let v = s
        .ok(
            "ReceiveMessage",
            json!({"QueueUrl": q, "MessageAttributeNames": ["col.*", "color"]}),
        )
        .await;
    let m = &v["Messages"][0];
    assert!(m.get("Attributes").is_none());
    assert_eq!(m["MessageAttributes"].as_object().unwrap().len(), 1);
    let receipt = m["ReceiptHandle"].as_str().unwrap().to_string();
    s.ok(
        "DeleteMessage",
        json!({"QueueUrl": q, "ReceiptHandle": receipt}),
    )
    .await;

    // Batches.
    let v = s
        .ok(
            "SendMessageBatch",
            json!({"QueueUrl": q, "Entries": [
                {"Id": "a", "MessageBody": "1"},
                {"Id": "b", "MessageBody": "2", "DelaySeconds": 0},
                {"Id": "c", "MessageBody": ""},
            ]}),
        )
        .await;
    assert_eq!(v["Successful"].as_array().unwrap().len(), 2);
    assert_eq!(v["Failed"][0]["Id"], "c");
    let (_, kind) = s
        .err("SendMessageBatch", json!({"QueueUrl": q, "Entries": [{"Id": "a", "MessageBody": "1"}, {"Id": "a", "MessageBody": "2"}]}))
        .await;
    assert!(kind.ends_with("BatchEntryIdsNotDistinct"), "{}", kind);
    let v = s
        .ok(
            "ReceiveMessage",
            json!({"QueueUrl": q, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 1}),
        )
        .await;
    let entries: Vec<Value> = v["Messages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, m)| json!({"Id": format!("e{}", i), "ReceiptHandle": m["ReceiptHandle"]}))
        .collect();
    assert_eq!(entries.len(), 2);
    let v = s
        .ok(
            "DeleteMessageBatch",
            json!({"QueueUrl": q, "Entries": entries}),
        )
        .await;
    assert_eq!(v["Successful"].as_array().unwrap().len(), 2);
    // Nothing left: an empty answer after the wait.
    let t = std::time::Instant::now();
    let v = s
        .ok(
            "ReceiveMessage",
            json!({"QueueUrl": q, "WaitTimeSeconds": 1}),
        )
        .await;
    assert!(v.get("Messages").is_none());
    assert!(t.elapsed() >= std::time::Duration::from_millis(900));

    // Attributes and tags.
    s.ok(
        "SetQueueAttributes",
        json!({"QueueUrl": q, "Attributes": {"VisibilityTimeout": "45"}}),
    )
    .await;
    let v = s
        .ok(
            "GetQueueAttributes",
            json!({"QueueUrl": q, "AttributeNames": ["All"]}),
        )
        .await;
    assert_eq!(v["Attributes"]["VisibilityTimeout"], "45");
    assert_eq!(
        v["Attributes"]["QueueArn"],
        "arn:aws:sqs:us-east-1:000000000000:q"
    );
    assert_eq!(v["Attributes"]["ApproximateNumberOfMessages"], "0");
    let (_, kind) = s
        .err(
            "SetQueueAttributes",
            json!({"QueueUrl": q, "Attributes": {"VisibilityTimeout": "-1"}}),
        )
        .await;
    assert!(kind.ends_with("InvalidAttributeValue"), "{}", kind);
    s.ok("TagQueue", json!({"QueueUrl": q, "Tags": {"team": "x"}}))
        .await;
    assert_eq!(
        s.ok("ListQueueTags", json!({"QueueUrl": q})).await["Tags"]["team"],
        "x"
    );
    s.ok("UntagQueue", json!({"QueueUrl": q, "TagKeys": ["team"]}))
        .await;
    assert!(
        s.ok("ListQueueTags", json!({"QueueUrl": q}))
            .await
            .get("Tags")
            .is_none()
    );
    s.ok("PurgeQueue", json!({"QueueUrl": q})).await;
}

#[tokio::test]
async fn dead_letters_and_errors() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let q = s.url("q");
    let dlq = s.url("dlq");
    let (_, kind) = s
        .err("SetQueueAttributes", json!({"QueueUrl": q, "Attributes": {"RedrivePolicy": r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:nope","maxReceiveCount":1}"#}}))
        .await;
    assert!(kind.ends_with("InvalidParameterValue"));
    s.ok("SetQueueAttributes", json!({"QueueUrl": q, "Attributes": {"RedrivePolicy": r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":1}"#}})).await;
    s.ok("SendMessage", json!({"QueueUrl": q, "MessageBody": "poison", "MessageAttributes": {"k": {"DataType": "Number", "StringValue": "7"}}})).await;
    let v = s
        .ok(
            "ReceiveMessage",
            json!({"QueueUrl": q, "VisibilityTimeout": 0}),
        )
        .await;
    assert_eq!(v["Messages"][0]["Body"], "poison");
    // Received too often: moved.
    let v = s.ok("ReceiveMessage", json!({"QueueUrl": q})).await;
    assert!(v.get("Messages").is_none());
    let v = s
        .ok("ReceiveMessage", json!({"QueueUrl": dlq, "WaitTimeSeconds": 5, "AttributeNames": ["All"], "MessageAttributeNames": ["All"]}))
        .await;
    let m = &v["Messages"][0];
    assert_eq!(m["Body"], "poison");
    assert_eq!(m["MessageAttributes"]["k"]["StringValue"], "7");
    assert_eq!(
        m["Attributes"]["DeadLetterQueueSourceArn"],
        "arn:aws:sqs:us-east-1:000000000000:q"
    );
    let v = s.ok("GetQueueAttributes", json!({"QueueUrl": q, "AttributeNames": ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]})).await;
    assert_eq!(
        v["Attributes"]["ApproximateNumberOfMessagesNotVisible"],
        "0"
    );
    let v = s
        .ok("ListDeadLetterSourceQueues", json!({"QueueUrl": dlq}))
        .await;
    assert_eq!(v["queueUrls"], json!([q]));

    // Errors.
    let (st, qe, v) = s
        .raw("GetQueueUrl", &json!({"QueueName": "nope"}), true)
        .await;
    assert_eq!(st, 400);
    assert_eq!(v["__type"], "com.amazonaws.sqs#QueueDoesNotExist");
    assert_eq!(qe, "AWS.SimpleQueueService.NonExistentQueue;Sender");
    let (st, _, _) = s.raw("ListQueues", &json!({}), false).await;
    assert_eq!(st, 403);
    let (_, kind) = s
        .err(
            "SendMessage",
            json!({"QueueUrl": q, "MessageBody": "x", "MessageGroupId": "g"}),
        )
        .await;
    assert!(kind.ends_with("InvalidParameterValue"));
    let (_, kind) = s
        .err(
            "DeleteMessage",
            json!({"QueueUrl": q, "ReceiptHandle": "bad!"}),
        )
        .await;
    assert!(kind.ends_with("ReceiptHandleIsInvalid"), "{}", kind);
    let (_, kind) = s
        .err(
            "ReceiveMessage",
            json!({"QueueUrl": q, "MaxNumberOfMessages": 11}),
        )
        .await;
    assert!(kind.ends_with("InvalidParameterValue"));
    let (_, kind) = s.err("CreateQueue", json!({"QueueName": "x.fifo"})).await;
    assert!(kind.ends_with("InvalidParameterValue"));
    // The same queue again, same attributes: its URL.
    let v = s
        .ok(
            "CreateQueue",
            json!({"QueueName": "q", "Attributes": {"VisibilityTimeout": "30"}}),
        )
        .await;
    assert_eq!(v["QueueUrl"], q);
    let (_, kind) = s
        .err(
            "CreateQueue",
            json!({"QueueName": "q", "Attributes": {"VisibilityTimeout": "31"}}),
        )
        .await;
    assert!(kind.ends_with("QueueNameExists"), "{}", kind);
    // A readonly key looks, and changes nothing (a receive does).
    let mut ro = Sqs {
        key: s.fx.key(Role::Readonly),
        ..s
    };
    ro.ok(
        "GetQueueAttributes",
        json!({"QueueUrl": q, "AttributeNames": ["All"]}),
    )
    .await;
    let (st, kind) = ro.err("ReceiveMessage", json!({"QueueUrl": q})).await;
    assert_eq!((st, kind.as_str()), (403, "com.amazonaws.sqs#AccessDenied"));
    ro.key = ro.fx.key(Role::Admin);
}

/// I-34: a message's history (`RaidishMoved`) is a dead-letter move's
/// alone. A client's SendMessage carrying one is sent as any other (sent
/// now, never received); `u64::MAX` there used to overflow the retention
/// arithmetic, and `u32::MAX` receives the count. The move's own action
/// is refused to a client's key, admin or not.
#[tokio::test]
async fn a_clients_message_history_is_ignored() {
    let pool = TestPool::new().await;
    let s = server(&pool).await;
    let q = s.url("q");
    let forged = json!({
        "QueueUrl": q,
        "MessageBody": "forged",
        "RaidishMoved": {"SentMs": u64::MAX, "ReceiveCount": u32::MAX, "From": "elsewhere"},
    });
    s.ok("SendMessage", forged.clone()).await;
    let (st, kind) = s.err(super::MOVE_MESSAGE, forged).await;
    assert_eq!(st, 403);
    assert!(kind.ends_with("AccessDenied"), "{}", kind);

    let before = crate::types::now_ms();
    let v = s
        .ok(
            "ReceiveMessage",
            json!({"QueueUrl": q, "AttributeNames": ["All"]}),
        )
        .await;
    let m = &v["Messages"][0];
    assert_eq!(m["Body"], "forged");
    assert_eq!(m["Attributes"]["ApproximateReceiveCount"], "1");
    let sent: u64 = m["Attributes"]["SentTimestamp"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(sent <= before, "sent at {}", sent);
    assert!(m["Attributes"].get("DeadLetterQueueSourceArn").is_none());
}
