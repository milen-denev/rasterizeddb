//! Integration tests for the S3 client against a real S3-compatible server.
//!
//! Skipped unless `RDB_S3_TEST_ENDPOINT` is set, so an ordinary `cargo test`
//! does not need a network service. To run them:
//!
//! ```text
//! docker run -d --rm -p 19000:9000 \
//!     -e MINIO_ROOT_USER=testkey -e MINIO_ROOT_PASSWORD=testsecret123 \
//!     quay.io/minio/minio:latest server /data
//! curl -s -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
//!     --user testkey:testsecret123 http://127.0.0.1:19000/rdbtest
//!
//! RDB_S3_TEST_ENDPOINT=http://127.0.0.1:19000 \
//! RDB_S3_TEST_BUCKET=rdbtest \
//! AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret123 \
//!     cargo test -p rasterizeddb_core --features s3_backup --test s3_backup_minio
//! ```
//!
//! What these cover that the unit tests cannot: that the SigV4 signature this
//! crate computes is one a server actually accepts, and that the hand-rolled
//! HTTP/1.1 framing survives real responses.

#![cfg(feature = "s3_backup")]

use std::sync::Arc;

use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint, S3Client, S3Config};

/// Builds a client from the environment, or `None` when the suite should skip.
fn client() -> Option<Arc<S3Client>> {
    let endpoint = std::env::var("RDB_S3_TEST_ENDPOINT").ok()?;
    let bucket = std::env::var("RDB_S3_TEST_BUCKET").unwrap_or_else(|_| "rdbtest".to_string());
    let credentials = Credentials::from_env()
        .expect("RDB_S3_TEST_ENDPOINT is set, so AWS_ACCESS_KEY_ID/SECRET must be too");

    Some(Arc::new(
        S3Client::new(S3Config {
            endpoint: Endpoint::parse(&endpoint).expect("RDB_S3_TEST_ENDPOINT must be a URL"),
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            bucket,
            credentials,
            // MinIO does not do virtual-host buckets without extra DNS setup,
            // and path style is what every non-AWS implementation supports.
            path_style: true,
            max_attempts: 3,
        })
        .expect("the client is constructible"),
    ))
}

macro_rules! s3_test {
    ($name:ident, $client:ident, $body:block) => {
        #[tokio::test]
        async fn $name() {
            let Some($client) = client() else {
                eprintln!(
                    "skipping {}: RDB_S3_TEST_ENDPOINT is not set",
                    stringify!($name)
                );
                return;
            };
            $body
        }
    };
}

/// Exercises the TLS path against real AWS.
///
/// MinIO is reached over plain HTTP, so without this the whole encrypted
/// transport — the rustls client config, the `webpki-roots` trust anchors, the
/// handshake, and virtual-host bucket addressing — would never execute before
/// production did it first.
///
/// It uses the well-known example credentials, so it needs no secrets and
/// cannot touch anything: AWS rejects the key and the assertion is that it
/// rejected it *as an S3 error*, which means the connection, the request
/// framing and the response parsing all worked. Signature correctness is what
/// the MinIO tests above establish.
///
/// Opt-in via `RDB_S3_TLS_PROBE=1`, because a test suite should not reach out
/// to a third party unless asked.
#[tokio::test]
async fn reaches_real_s3_over_tls() {
    if std::env::var("RDB_S3_TLS_PROBE").as_deref() != Ok("1") {
        eprintln!("skipping reaches_real_s3_over_tls: set RDB_S3_TLS_PROBE=1 to run it");
        return;
    }

    let client = S3Client::new(S3Config {
        endpoint: Endpoint::parse("https://s3.us-east-1.amazonaws.com").expect("endpoint"),
        region: "us-east-1".to_string(),
        bucket: "rdb-tls-probe-does-not-exist".to_string(),
        credentials: Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        },
        // Virtual-host addressing, which is what AWS itself uses and which the
        // MinIO tests never exercise.
        path_style: false,
        max_attempts: 1,
    })
    .expect("client");

    let err = match client.get_object("probe").await {
        Err(e) => e,
        Ok(_) => panic!("a bucket that does not exist must not return an object"),
    };

    // Anything that got a status code back proves the encrypted round trip.
    // A connection or certificate failure would surface as a different kind.
    assert!(
        matches!(
            err.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
        ),
        "expected an S3 rejection over TLS, got {:?}: {err}",
        err.kind()
    );
    assert!(
        err.to_string().contains("returned 40"),
        "expected a parsed 4xx from S3, got: {err}"
    );
}

s3_test!(round_trips_a_small_object, client, {
    let key = "it/small.bin".to_string();
    let body = b"the quick brown fox".to_vec();

    client.put_object(&key, &body).await.expect("put");
    assert_eq!(client.get_object(&key).await.expect("get"), body);

    client.delete_objects(&[key.clone()]).await.expect("delete");
    let err = client.get_object(&key).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{err}");
});

s3_test!(round_trips_an_empty_object, client, {
    // The empty body takes the other signing path, where the payload hash is
    // the digest of nothing rather than UNSIGNED-PAYLOAD.
    let key = "it/empty.bin".to_string();
    client.put_object(&key, &[]).await.expect("put");
    assert!(client.get_object(&key).await.expect("get").is_empty());
    client.delete_objects(&[key]).await.expect("delete");
});

s3_test!(round_trips_a_key_needing_escaping, client, {
    // Path encoding has to match the signature exactly or the request is
    // rejected, and these are the characters where encoders disagree.
    let key = "it/a b+c~d/e=f&g.bin".to_string();
    let body = b"escaped".to_vec();
    client.put_object(&key, &body).await.expect("put");
    assert_eq!(client.get_object(&key).await.expect("get"), body);
    client.delete_objects(&[key]).await.expect("delete");
});

s3_test!(round_trips_a_multipart_upload, client, {
    let key = "it/multipart.bin".to_string();

    // Three parts, each above S3's 5 MiB minimum for a non-final part.
    let part_size = 5 * 1024 * 1024;
    let upload_id = client.create_multipart_upload(&key).await.expect("create");

    let mut expected = Vec::new();
    let mut parts = Vec::new();
    for number in 1..=3u32 {
        let body = vec![number as u8; part_size];
        expected.extend_from_slice(&body);
        let etag = client
            .upload_part(&key, &upload_id, number, &body)
            .await
            .expect("upload part");
        parts.push((number, etag));
    }

    client
        .complete_multipart_upload(&key, &upload_id, &parts)
        .await
        .expect("complete");

    // Streamed rather than buffered, which is the path a restore takes.
    let mut fetched = Vec::new();
    let total = client
        .get_object_streaming(&key, |chunk| {
            fetched.extend_from_slice(chunk);
            Ok(())
        })
        .await
        .expect("streaming get");

    assert_eq!(total, expected.len() as u64);
    assert_eq!(fetched, expected);

    client.delete_objects(&[key]).await.expect("delete");
});

s3_test!(aborts_a_multipart_upload, client, {
    let key = "it/aborted.bin".to_string();
    let upload_id = client.create_multipart_upload(&key).await.expect("create");
    client
        .upload_part(&key, &upload_id, 1, &vec![0u8; 5 * 1024 * 1024])
        .await
        .expect("upload part");
    client
        .abort_multipart_upload(&key, &upload_id)
        .await
        .expect("abort");

    // Aborting twice is not an error: the second call finds it already gone,
    // which is the state the caller wanted.
    client
        .abort_multipart_upload(&key, &upload_id)
        .await
        .expect("second abort");
});

s3_test!(lists_and_abandons_orphaned_multipart_uploads, client, {
    // A process killed mid-snapshot leaves parts behind that S3 bills for until
    // something reaps them. The service clears them at start-up, which needs
    // both halves of this to work.
    // Its own prefix, because this test aborts everything it finds and the
    // other tests here run concurrently — sharing `it/` would have it cancel
    // their uploads out from under them.
    let prefix = "orphans/";
    let key = format!("{prefix}orphan.bin");
    let upload_id = client.create_multipart_upload(&key).await.expect("create");
    client
        .upload_part(&key, &upload_id, 1, &vec![9u8; 5 * 1024 * 1024])
        .await
        .expect("upload part");

    let listed = client.list_multipart_uploads(prefix).await.expect("list");
    assert!(
        listed.iter().any(|(k, id)| k == &key && id == &upload_id),
        "the in-flight upload should be listed, got {listed:?}"
    );
    assert!(
        listed.iter().all(|(k, _)| k.starts_with(prefix)),
        "the listing must be filtered to the prefix, got {listed:?}"
    );

    for (k, id) in &listed {
        client.abort_multipart_upload(k, id).await.expect("abort");
    }

    let after = client
        .list_multipart_uploads(prefix)
        .await
        .expect("list again");
    assert!(
        !after.iter().any(|(k, _)| k == &key),
        "the upload should be gone, got {after:?}"
    );
});

s3_test!(deletes_many_keys_in_one_request, client, {
    let keys: Vec<String> = (0..25).map(|i| format!("it/batch/{i:03}.bin")).collect();
    for key in &keys {
        client.put_object(key, b"x").await.expect("put");
    }

    client.delete_objects(&keys).await.expect("batch delete");

    for key in &keys {
        let err = client.get_object(key).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{key}: {err}");
    }
});

s3_test!(reports_a_missing_object_as_not_found, client, {
    let err = client
        .get_object("it/definitely-not-here.bin")
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{err}");
});

s3_test!(reuses_pooled_connections, client, {
    // Sequential requests on one client should not each pay a handshake. The
    // failure mode a reused connection introduces is a response read against a
    // stream that still holds the previous body, which shows up as garbage
    // here rather than as a timing difference.
    for i in 0..10u8 {
        let key = format!("it/pool/{i}.bin");
        let body = vec![i; 1024];
        client.put_object(&key, &body).await.expect("put");
        assert_eq!(client.get_object(&key).await.expect("get"), body);
    }
    let keys: Vec<String> = (0..10).map(|i| format!("it/pool/{i}.bin")).collect();
    client.delete_objects(&keys).await.expect("delete");
});
