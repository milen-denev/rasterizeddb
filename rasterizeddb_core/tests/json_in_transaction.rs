//! A JSON document's shadow rows belong to the transaction that wrote them.
//!
//! They used to commit on their own — each nested object its own autocommit,
//! its own flushes — which made a document with N nested objects N+1
//! transactions, and left the shadow rows behind when the statement that
//! caused them rolled back. Now they join the caller's transaction, and this
//! file guards the two things that has to get right.
//!
//! The first is the one that fails silently. A key no document has carried
//! before adds a column to the shadow table, and adding a column rewrites the
//! table: every row read, the table dropped and recreated, every row written
//! back. Read frozen, that rewrite would not see the rows the *same*
//! transaction had written moments before — and the drop would take them. The
//! document would come back missing, from a statement that reported success.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        rql::{
            executor::{execute, execute_in_txn},
            json_map,
            lexer_s1::recognize_query_purpose,
        },
    },
};
use rclite::Arc;

/// Runs a test body on a thread with a stack big enough for the engine.
///
/// `execute()` builds a very large async state machine, and in a debug build
/// one statement's frame overruns the 2 MiB a thread gets by default — the
/// server itself runs on a deliberately-set 32 MiB stack for exactly this
/// reason (`main.rs`, `STACK_SIZE`). A `#[tokio::test]` gets no such favour,
/// and `block_on` drives the future on the *calling* thread, so raising only
/// the runtime's `thread_stack_size` would not help: the thread that overflows
/// is the test's own. Both are raised here, so the test passes for anyone who
/// runs it rather than only with `RUST_MIN_STACK` set in the environment.
fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
    // Every test in this file is about the shredded mapping — shadow rows
    // joining the caller's transaction, a rewrite triggered by a new key, keys
    // that collide as column names. A document is stored as bytes by default
    // now, which has none of those mechanics, so the mapping under test has to
    // be turned on. Process-wide, and each test file is its own process.
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
}

async fn run(db: &Arc<Database>, q: &str) -> String {
    let purpose = recognize_query_purpose(q).expect("recognized");
    String::from_utf8_lossy(&execute(purpose, db.clone()).await).to_string()
}

async fn run_in(db: &Arc<Database>, q: &str, tx: rasterizeddb_core::core::mvcc::TxId) -> String {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute_in_txn(purpose, db.clone(), tx).await;
    assert_ne!(out.first(), Some(&3), "{} failed: {}", q, String::from_utf8_lossy(&out[1..]));
    String::from_utf8_lossy(&out).to_string()
}

fn fresh_db(tag: &str) -> String {
    let dir = format!("/tmp/rdb_json_in_txn_{}_{}", tag, std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

#[test]
fn documents_written_in_one_transaction_survive_a_new_key_rewrite() {
    on_a_big_stack(documents_written_in_one_transaction_survive_a_new_key_rewrite_body());
}

async fn documents_written_in_one_transaction_survive_a_new_key_rewrite_body() {
    init_globals();
    let dir = fresh_db("rewrite");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    let created = run(&db, "CREATE TABLE j (id INT NOT NULL, doc JSONB NULL)").await;
    assert!(!created.to_lowercase().contains("error"), "create failed: {}", created);

    let tx = db.transactions.begin();
    // Establishes the shadow table with one key, and the nested table with one.
    run_in(&db, r#"INSERT INTO j (id, doc) VALUES (1, '{"k1":1,"o":{"a":1}}')"#, tx).await;
    // `k2` is new to the shadow table and `b` is new to the nested one: two
    // rewrites, both while row 1's shadow rows are uncommitted.
    run_in(
        &db,
        r#"INSERT INTO j (id, doc) VALUES (2, '{"k1":2,"k2":"x","o":{"a":2,"b":3}}')"#,
        tx,
    )
    .await;
    db.sync_durable().expect("durable");
    db.commit_transaction(tx);
    db.transactions.flush().expect("flushed");

    let one = run(&db, "SELECT doc FROM j WHERE id = 1").await;
    for expected in ["\"k1\":1", "\"a\":1"] {
        assert!(one.contains(expected), "row 1 lost {} across the rewrite: {}", expected, one);
    }
    let two = run(&db, "SELECT doc FROM j WHERE id = 2").await;
    for expected in ["\"k1\":2", "\"k2\":\"x\"", "\"a\":2", "\"b\":3"] {
        assert!(two.contains(expected), "row 2 missing {}: {}", expected, two);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_rolled_back_json_insert_leaves_no_shadow_rows() {
    on_a_big_stack(a_rolled_back_json_insert_leaves_no_shadow_rows_body());
}

async fn a_rolled_back_json_insert_leaves_no_shadow_rows_body() {
    init_globals();
    let dir = fresh_db("rollback");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE j (id INT NOT NULL, doc JSONB NULL)").await;
    let shadow = json_map::shadow_table_name("j", "doc");

    let tx = db.transactions.begin();
    run_in(&db, r#"INSERT INTO j (id, doc) VALUES (1, '{"k":1}')"#, tx).await;
    db.abort_transaction(tx);
    db.transactions.flush().expect("flushed");

    // The parent row is gone, and so is the document's row — it was written
    // by the same transaction, not by one of its own that had already
    // committed.
    let parent = run(&db, "SELECT id FROM j").await;
    assert!(!parent.contains('1'), "the rolled-back parent row is still visible: {}", parent);
    let orphans = run(&db, &format!("SELECT __json_id FROM {}", shadow)).await;
    assert!(
        !orphans.contains('-'),
        "the rolled-back document left a shadow row behind (a UUID is visible): {}",
        orphans
    );

    // And a committed one is there, through the same path.
    run(&db, r#"INSERT INTO j (id, doc) VALUES (2, '{"k":2}')"#).await;
    let kept = run(&db, "SELECT doc FROM j WHERE id = 2").await;
    assert!(kept.contains("\"k\":2"), "the committed document did not come back: {}", kept);

    let _ = std::fs::remove_dir_all(&dir);
}

/// JSON keys are case-sensitive; column names are not. `date` and `Date`
/// used to resolve to one column, which the second spelling's `INSERT` then
/// could not name — the import of a real database failed on it at row 28 of a
/// table, with "schema mismatch" and nothing about why.
#[test]
fn keys_differing_only_in_case_get_distinct_columns() {
    on_a_big_stack(keys_differing_only_in_case_get_distinct_columns_body());
}

async fn keys_differing_only_in_case_get_distinct_columns_body() {
    init_globals();
    let dir = fresh_db("case");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE j (id INT NOT NULL, doc JSONB NULL)").await;

    run(&db, r#"INSERT INTO j (id, doc) VALUES (1, '{"date":"2026-01-01"}')"#).await;
    let second = run(&db, r#"INSERT INTO j (id, doc) VALUES (2, '{"Date":"2026-02-02"}')"#).await;
    assert!(
        !second.to_lowercase().contains("mismatch") && !second.to_lowercase().contains("not found"),
        "a key differing only in case from an existing one was refused: {}",
        second
    );
    // Both spellings in one document: neither may overwrite the other.
    run(&db, r#"INSERT INTO j (id, doc) VALUES (3, '{"date":"a","Date":"b"}')"#).await;

    let one = run(&db, "SELECT doc FROM j WHERE id = 1").await;
    assert!(one.contains("2026-01-01"), "row 1: {}", one);
    let two = run(&db, "SELECT doc FROM j WHERE id = 2").await;
    assert!(two.contains("2026-02-02"), "row 2 lost its value: {}", two);
    let three = run(&db, "SELECT doc FROM j WHERE id = 3").await;
    assert!(
        three.contains("\"a\"") && three.contains("\"b\""),
        "row 3 should hold both values: {}",
        three
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A JSON key whose name contains a SQL keyword — `values_json` here — names
/// the table that backs it, and the `INSERT` parser found `VALUES` *inside
/// that name* rather than after the column list. The column names were then
/// parsed as the tuple and the statement failed as "schema mismatch"; a
/// real import stopped on exactly this document.
#[test]
fn a_key_containing_a_keyword_does_not_confuse_the_insert_parser() {
    on_a_big_stack(a_key_containing_a_keyword_does_not_confuse_the_insert_parser_body());
}

async fn a_key_containing_a_keyword_does_not_confuse_the_insert_parser_body() {
    init_globals();
    let dir = fresh_db("keyword");
    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE v (id INT NOT NULL, config JSONB NULL)").await;

    let inserted = run(
        &db,
        r#"INSERT INTO v (id, config) VALUES (1, '{"title": "Pie chart", "labels_json": ["Soap", "Shampoo"], "values_json": [11203.75, 10696.66], "select": {"from": 1, "where": [1, 2]}}')"#,
    )
    .await;
    assert!(
        !inserted.to_lowercase().contains("mismatch"),
        "a document with keyword-like keys was refused: {}",
        inserted
    );
    let back = run(&db, "SELECT config FROM v WHERE id = 1").await;
    for expected in [
        "\"values_json\":[11203.75,10696.66]",
        "\"labels_json\":[\"Soap\",\"Shampoo\"]",
        "\"where\":[1,2]",
        "\"from\":1",
    ] {
        assert!(back.contains(expected), "{} missing from the document: {}", expected, back);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
