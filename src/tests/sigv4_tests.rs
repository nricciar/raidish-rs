//! SigV4 against AWS's published examples, and our own signatures.

use super::*;
fn config() -> StaticKeys {
    StaticKeys {
        region: "us-east-1".into(),
        keys: vec![(
            "AKIAIOSFODNN7EXAMPLE".into(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        )],
    }
}

fn at_2013() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z")
        .unwrap()
        .into()
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.append(
            hyper::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    h
}

/// "GET Object" from the S3 SigV4 documentation.
#[test]
fn aws_example_get_object() {
    let h = headers(&[
        ("host", "examplebucket.s3.amazonaws.com"),
        ("range", "bytes=0-9"),
        ("x-amz-content-sha256", EMPTY_SHA256),
        ("x-amz-date", "20130524T000000Z"),
        (
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        ),
    ]);
    let uri: Uri = "/test.txt".parse().unwrap();
    let s = verify_at(&config(), "s3", &Method::GET, &uri, &h, None, at_2013()).unwrap();
    assert_eq!(s.payload, Payload::Sha256(EMPTY_SHA256.into()));
    // Any change breaks it.
    let uri2: Uri = "/test.txu".parse().unwrap();
    assert_eq!(
        verify_at(&config(), "s3", &Method::GET, &uri2, &h, None, at_2013()).unwrap_err(),
        AuthError::Mismatch
    );
    // Too far from its time.
    assert_eq!(
        verify(&config(), "s3", &Method::GET, &uri, &h, None).unwrap_err(),
        AuthError::Skewed
    );
}

/// "GET Bucket lifecycle" (a query parameter with no value) and "list
/// objects" (sorted parameters).
#[test]
fn aws_example_queries() {
    let h = headers(&[
        ("host", "examplebucket.s3.amazonaws.com"),
        ("x-amz-content-sha256", EMPTY_SHA256),
        ("x-amz-date", "20130524T000000Z"),
        (
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
        ),
    ]);
    let uri: Uri = "/?lifecycle".parse().unwrap();
    verify_at(&config(), "s3", &Method::GET, &uri, &h, None, at_2013()).unwrap();
    let mut h2 = h.clone();
    h2.insert(
        "authorization",
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
            .parse()
            .unwrap(),
    );
    let uri: Uri = "/?max-keys=2&prefix=J".parse().unwrap();
    verify_at(&config(), "s3", &Method::GET, &uri, &h2, None, at_2013()).unwrap();
}

/// The presigned URL example.
#[test]
fn aws_example_presigned() {
    let h = headers(&[("host", "examplebucket.s3.amazonaws.com")]);
    let uri: Uri = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404".parse().unwrap();
    let s = verify_at(&config(), "s3", &Method::GET, &uri, &h, None, at_2013()).unwrap();
    assert_eq!(s.payload, Payload::Unsigned);
    let late = at_2013() + chrono::Duration::seconds(86_401);
    assert_eq!(
        verify_at(&config(), "s3", &Method::GET, &uri, &h, None, late).unwrap_err(),
        AuthError::Expired
    );
}

/// The `aws-chunked` example: the seed signature, then the chunks'.
#[test]
fn aws_example_chunked() {
    let h = headers(&[
        ("host", "s3.amazonaws.com"),
        ("x-amz-date", "20130524T000000Z"),
        ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ("x-amz-content-sha256", STREAMING),
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", "66560"),
        ("content-length", "66824"),
        (
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
        ),
    ]);
    let uri: Uri = "/examplebucket/chunkObject.txt".parse().unwrap();
    let mut s = verify_at(&config(), "s3", &Method::PUT, &uri, &h, None, at_2013()).unwrap();
    assert_eq!(s.payload, Payload::Chunked { trailer: false });
    let a64k = Sha256::digest(vec![b'a'; 65536]);
    assert!(s.chunks.chunk(
        "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
        &a64k
    ));
    assert!(s.chunks.chunk(
        "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
        &Sha256::digest(vec![b'a'; 1024])
    ));
    assert!(s.chunks.chunk(
        "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
        &Sha256::digest(b"")
    ));
}

#[test]
fn our_signatures_verify() {
    let c = config();
    let uri: Uri = "http://node1:9324/000000000000/q?x=a%20b".parse().unwrap();
    let mut h = headers(&[
        ("host", "node1:9324"),
        ("x-amz-target", "AmazonSQS.SendMessage"),
    ]);
    let body = br#"{"QueueUrl":"x"}"#;
    for (k, v) in sign(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "us-east-1",
        "sqs",
        &Method::POST,
        &uri,
        &h,
        body,
    ) {
        h.insert(
            hyper::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    let hash = hex::encode(Sha256::digest(body));
    verify(&c, "sqs", &Method::POST, &uri, &h, Some(&hash)).unwrap();
    // Signed for another service.
    assert!(matches!(
        verify(&c, "s3", &Method::POST, &uri, &h, Some(&hash)),
        Err(AuthError::Malformed(_))
    ));
}
