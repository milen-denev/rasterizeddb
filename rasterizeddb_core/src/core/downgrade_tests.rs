//! What a build of `HEAD` (the last commit) would make of the files this
//! build writes: for every new on-disk artifact, either it reads it exactly as
//! intended, or it refuses it safely. Never a misread.
//!
//! An old binary cannot be run here, so each test carries a copy of the
//! relevant `HEAD` reader rule (quoted from `git show HEAD:<file>`) and applies
//! it to bytes this build produced.

use crate::core::mvcc::status::StatusLog;
use crate::core::mvcc::TxOutcome;

/// `HEAD`'s `StatusLog::open`: the file is a plain bitmap, two bits per id —
/// `0b01` committed, `0b10` aborted, `0b00` in progress (and so aborted, for
/// an id below the highest recorded, after a restart).
fn head_status_answer(bytes: &[u8], tx: u64) -> TxOutcome {
    let byte = bytes.get((tx / 4) as usize).copied().unwrap_or(0);
    match (byte >> ((tx % 4) * 2)) & 0b11 {
        0b01 => TxOutcome::Committed,
        0b10 => TxOutcome::Aborted,
        _ => TxOutcome::InProgress,
    }
}

/// The status file stays a bitmap however the log settles and forgets, so a
/// `HEAD` build reads every id a row can still carry exactly as this build
/// answers it. Ids below the forgotten horizon differ (this build says
/// committed, the file still says aborted) — and no row carries those, which
/// is the forget contract; `HEAD` answering "aborted" for them is the
/// conservative answer anyway. The `.settled` sidecar is a file `HEAD` never
/// opens.
#[test]
fn a_head_build_reads_a_settled_and_forgetting_status_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db_transactions.status");
    let log = StatusLog::open_with(&path, true).unwrap();
    log.set_persist_step(1);
    let n = 20_000u64;
    let outcome_of = |tx: u64| {
        if tx % 13 == 0 {
            TxOutcome::Aborted
        } else {
            TxOutcome::Committed
        }
    };
    for tx in 2..n {
        log.record(tx, outcome_of(tx));
    }
    log.flush().unwrap();
    let settled = log.settle_below(16_384);
    assert!(settled > 0, "nothing settled; the test exercises nothing");
    let forgotten = log.forget_below(8_192);
    assert!(forgotten > 0, "nothing forgotten; the test exercises nothing");
    log.flush().unwrap();
    assert!(path.with_file_name("db_transactions.status.settled").exists());

    let bytes = std::fs::read(&path).unwrap();
    assert_ne!(
        bytes.get(..8),
        Some(&crate::core::mvcc::status::REWRITTEN_FORMAT_MAGIC[..]),
        "the status file was rewritten in a format HEAD would read as bits"
    );
    for tx in 2..n {
        let head = head_status_answer(&bytes, tx);
        // What the file says is what was recorded, forgotten or not.
        assert_eq!(head, outcome_of(tx), "id {tx}: the status file lost an outcome");
        if tx >= forgotten {
            assert_eq!(head, log.outcome(tx), "id {tx}: HEAD would answer differently");
        }
    }
}

/// `HEAD`'s `DeltaBatch::decode` accepts exactly version 1 and ignores
/// anything after the records. A batch carrying an `aborted` list must not be
/// version 1, or `HEAD` would honour an in-doubt commit the list overrides.
#[cfg(feature = "kv_storage")]
#[test]
fn a_head_build_refuses_a_kv_batch_it_would_misread() {
    use crate::core::storage_providers::kv::layout::{DeltaBatch, Record, RecordKind};
    let head_accepts = |bytes: &[u8]| u16::from_le_bytes(bytes[4..6].try_into().unwrap()) == 1;
    let batch = |aborted: Vec<(u64, u64)>| DeltaBatch {
        seq: 7,
        base_gen: 1,
        committed: vec![42],
        records: vec![Record {
            txn: 42,
            kind: RecordKind::Write,
            offset: 0,
            payload: b"row".to_vec(),
        }],
        aborted,
    };

    let plain = batch(Vec::new()).encode();
    assert!(head_accepts(&plain), "a batch without aborts must stay readable by HEAD");
    let with_aborts = batch(vec![(41, 6)]).encode();
    assert!(!head_accepts(&with_aborts), "HEAD would read this batch and ignore its aborts");

    // This build reads both.
    assert!(DeltaBatch::decode(&plain).unwrap().unwrap().aborted.is_empty());
    assert_eq!(DeltaBatch::decode(&with_aborts).unwrap().unwrap().aborted, vec![(41, 6)]);
}

/// `HEAD`'s filter loader accepts exactly format version 3 and rebuilds on
/// anything else; this build writes 5. A refusal is a rebuild from the table,
/// never a misread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_head_build_refuses_this_builds_filter_sidecar() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_string_lossy().to_string();
    let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
    for sql in [
        "CREATE TABLE f (id UBIGINT, v VARCHAR);",
        "INSERT INTO f (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    ] {
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap();
        crate::core::rql::executor::execute(purpose, db.clone()).await;
    }
    let table = db.table_for_tests("f").unwrap();
    table.wait_filters_ready().await.unwrap();
    table.checkpoint_filters().await.unwrap();
    let flt = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy() == "f_flt.db")
        .expect("a filter sidecar");
    let bytes = std::fs::read(flt.path()).unwrap();
    assert_eq!(&bytes[..8], b"RDBFLT01");
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    assert_ne!(version, 3, "HEAD would read this sidecar as its own format");
    let _ = crate::core::wal::remove_for(&dir_str);
}

/// `HEAD`'s `TableSchema::load` reads the `_schema.db` header and nothing after
/// it: "if schema_len < TABLE_SIZE as u64 { … NotFound … }", then
/// `read_data_into_buffer` of exactly `TABLE_SIZE` (`129 * 2 + 1`) bytes at
/// offset 0 into `TableSchema::from_vec`. So the incarnation record this build
/// appends after the header (C7) is invisible to it, and it reads the header
/// exactly as written.
#[tokio::test]
async fn a_head_build_reads_a_schema_file_with_an_incarnation_record() {
    use crate::core::row::schema::TableSchema;
    use crate::core::storage_providers::file_sync::LocalStorageProvider;
    use crate::core::storage_providers::traits::StorageIO;
    const HEAD_TABLE_SIZE: usize = 129 * 2 + 1;

    let dir = tempfile::tempdir().unwrap();
    let io = rclite::Arc::new(
        LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("down_schema.db")).await,
    );
    let mut schema = TableSchema::new("down".into(), true);
    schema.set_primary_key("id".into());
    schema.save(io.clone()).await.unwrap();

    let len = io.get_len().await as usize;
    assert!(len > HEAD_TABLE_SIZE, "no incarnation record was written; nothing to test");
    let mut bytes = vec![0u8; len];
    io.read_data_into_buffer(&mut 0, &mut bytes).await.unwrap();

    let head = TableSchema::from_vec(&bytes[..HEAD_TABLE_SIZE]).expect("HEAD reads the header");
    assert_eq!(head.name, "down");
    assert_eq!(head.primary_key.as_deref(), Some("id"));
    assert!(head.is_immutable);
}
