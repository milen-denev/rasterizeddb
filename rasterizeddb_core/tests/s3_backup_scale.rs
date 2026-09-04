//! Volume test for the snapshot upload and restore download paths.
//!
//! The correctness suites deliberately use small databases, so the largest
//! object they produce is a few megabytes and the multipart path is only ever
//! exercised with three parts. This one pushes enough *incompressible* data
//! through to force a real multipart upload — dozens of parts, concurrent, with
//! the compressor and the uploader actually contending — and then reads it all
//! back and compares it byte for byte.
//!
//! Incompressible on purpose: repetitive data would shrink to a single part and
//! prove nothing about the path that matters.
//!
//! Opt-in, because it moves hundreds of megabytes:
//!
//! ```text
//! RDB_S3_SCALE_MB=256 RDB_S3_TEST_ENDPOINT=http://127.0.0.1:19000 \
//! RDB_S3_TEST_BUCKET=rdbtest \
//! AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret123 \
//!   cargo test -p rasterizeddb_core --features s3_backup --test s3_backup_scale -- --nocapture
//! ```

#![cfg(feature = "s3_backup")]

use rasterizeddb_core::core::backup::manifest::RecoveryTarget;
use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint};
use rasterizeddb_core::core::backup::{BackupConfig, BackupService};

/// Part size the test forces, so a moderate payload still produces many parts.
const PART_SIZE: usize = 5 * 1024 * 1024;

fn config(prefix: &str) -> Option<BackupConfig> {
    let endpoint = std::env::var("RDB_S3_TEST_ENDPOINT").ok()?;
    Some(BackupConfig {
        bucket: std::env::var("RDB_S3_TEST_BUCKET").unwrap_or_else(|_| "rdbtest".to_string()),
        prefix: format!("scale/{}/{prefix}", fastrand::u64(..)),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        endpoint: Endpoint::parse(&endpoint).expect("endpoint must be a URL"),
        credentials: Credentials::from_env().expect("AWS credentials must be set"),
        path_style: true,
        // Level 1: the point is to move bytes, not to measure the compressor.
        snapshot_level: 1,
        segment_level: 1,
        compression_threads: 2,
        long_range: false,
        part_size: PART_SIZE,
        upload_concurrency: 4,
        max_attempts: 3,
        snapshot_interval_secs: 0,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
    })
}

/// Deterministic pseudo-random bytes: incompressible, and reproducible so a
/// mismatch can be localised.
fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut state = seed | 1;
    for chunk in out.chunks_mut(8) {
        // xorshift64*, enough to defeat a compressor and cheap to generate.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_snapshot_uploads_and_restores_byte_for_byte() {
    let Ok(megabytes) = std::env::var("RDB_S3_SCALE_MB") else {
        eprintln!(
            "skipping scale test: set RDB_S3_SCALE_MB=256 (and RDB_S3_TEST_ENDPOINT) to run it"
        );
        return;
    };
    let megabytes: usize = megabytes.parse().expect("RDB_S3_SCALE_MB must be a number");
    let Some(config) = config("large") else {
        eprintln!("skipping scale test: RDB_S3_TEST_ENDPOINT is not set");
        return;
    };

    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    // A handful of files rather than one, so entry framing is exercised at size
    // too, and one deliberately empty.
    let file_count = 6usize;
    let per_file = megabytes * 1024 * 1024 / file_count;
    let mut expected = Vec::new();
    for i in 0..file_count {
        let name = format!("table{i}.db");
        let bytes = if i == file_count - 1 {
            Vec::new()
        } else {
            incompressible(i as u64 + 1, per_file)
        };
        std::fs::write(live.join(&name), &bytes).unwrap();
        expected.push((name, bytes));
    }
    let raw: usize = expected.iter().map(|(_, b)| b.len()).sum();

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    let started = std::time::Instant::now();
    let report = service.snapshot_now().await.expect("snapshot");
    let upload_secs = started.elapsed().as_secs_f64();

    let parts = report.compressed_bytes.div_ceil(PART_SIZE as u64);
    eprintln!(
        "SCALE uploaded {} MiB raw -> {} MiB compressed in {:.1}s ({:.0} MiB/s), ~{} parts",
        raw / (1024 * 1024),
        report.compressed_bytes / (1024 * 1024),
        upload_secs,
        (raw as f64 / (1024.0 * 1024.0)) / upload_secs.max(0.001),
        parts
    );
    assert!(
        parts > 3,
        "the payload did not force a real multipart upload ({parts} parts); \
         raise RDB_S3_SCALE_MB"
    );

    let target = root.join("restored");
    let started = std::time::Instant::now();
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");
    let download_secs = started.elapsed().as_secs_f64();
    eprintln!(
        "SCALE restored in {:.1}s ({:.0} MiB/s)",
        download_secs,
        (raw as f64 / (1024.0 * 1024.0)) / download_secs.max(0.001)
    );

    // Byte for byte, not by length: a multipart upload that assembled its parts
    // in the wrong order produces an object of exactly the right size.
    for (name, bytes) in &expected {
        let restored = std::fs::read(target.join(name))
            .unwrap_or_else(|e| panic!("{name} missing from the restore: {e}"));
        assert_eq!(
            restored.len(),
            bytes.len(),
            "{name} came back the wrong length"
        );
        assert!(
            restored == *bytes,
            "{name} came back with different contents — parts may have been assembled out of order"
        );
    }
}
