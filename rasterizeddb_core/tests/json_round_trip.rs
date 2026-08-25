//! A JSON document must come back as the value that went in.
//!
//! A JSON column is shredded into a shadow table, one column per key, and
//! rebuilt on read. The rebuild has to restore each value's *type*, not just
//! its text — a client asked for JSON and compares against JSON. The failure
//! this file guards is silent: the document comes back well-formed and
//! plausible, with `[1,2]` turned into `["1","2"]`.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
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
    // These guard the *shredded* mapping's type fidelity — that `[1,2]` does
    // not come back as `["1","2"]` once it has been taken apart into columns
    // and put back together. A document is stored as bytes by default now and
    // has no such step to get wrong; `tests/jsonb_native.rs` and the compat
    // suite cover that. The mapping still ships, so it still has to hold.
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
}

async fn run(db: &Arc<Database>, q: &str) -> String {
    String::from_utf8_lossy(&run_bytes(db, q).await).to_string()
}

async fn run_bytes(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    execute(purpose, db.clone()).await
}

#[test]
fn every_json_value_keeps_its_type_across_a_round_trip() {
    on_a_big_stack(every_json_value_keeps_its_type_across_a_round_trip_body());
}

async fn every_json_value_keeps_its_type_across_a_round_trip_body() {
    init_globals();
    let dir = format!("/tmp/rdb_json_round_trip_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    let created = run(&db, "CREATE TABLE j (id INT NOT NULL, doc JSONB NULL)").await;
    assert!(
        !created.to_lowercase().contains("error"),
        "create failed: {}",
        created
    );

    // Each of these was, at some point, stored as something else: a numeric
    // array as `TEXT[]` (so `[1,2]` read back `["1","2"]`), an array of arrays
    // flattened to strings, and a nested array under a key lost entirely.
    let document = r#"{"n":1,"s":"a","b":true,"nums":[1,2],"strs":["x","y"],"bools":[true,false],"mixed":[1,"a"],"empty":[],"deep":[[1,2],[3]],"obj":{"m":3}}"#;
    run(
        &db,
        &format!("INSERT INTO j (id, doc) VALUES (1, '{}')", document),
    )
    .await;

    let out = run(&db, "SELECT doc FROM j").await;

    for expected in [
        "\"nums\":[1,2]",
        "\"strs\":[\"x\",\"y\"]",
        "\"bools\":[true,false]",
        "\"mixed\":[1,\"a\"]",
        "\"empty\":[]",
        "\"deep\":[[1,2],[3]]",
        "\"obj\":{\"m\":3}",
        "\"n\":1",
        "\"b\":true",
    ] {
        assert!(
            out.contains(expected),
            "{} missing from the document read back: {}",
            expected,
            out
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_scaled_numeric_accepts_null_like_any_other_column() {
    on_a_big_stack(a_scaled_numeric_accepts_null_like_any_other_column_body());
}

async fn a_scaled_numeric_accepts_null_like_any_other_column_body() {
    // `numeric(18,8)` holding NULL is ordinary, and the scaled-decimal path
    // did not recognise NULL as anything but an unparsable number — so the row
    // could not be built and the statement came back as "Cannot insert row due
    // to schema mismatch", naming neither the column nor the value. It stopped
    // an import of a real table dead on its first row.
    init_globals();
    let dir = format!("/tmp/rdb_scaled_null_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE m (id INT NOT NULL, cost NUMERIC(18, 8))").await;

    for sql in [
        "INSERT INTO m (id, cost) VALUES (1, NULL)",
        "INSERT INTO m (id) VALUES (2)",
        "INSERT INTO m (id, cost) VALUES (3, 1.25)",
    ] {
        let out = run(&db, sql).await;
        assert!(
            !out.to_lowercase().contains("mismatch") && !out.to_lowercase().contains("error"),
            "{} was refused: {}",
            sql,
            out
        );
    }

    // Decoded rather than string-matched: the response is rows of bytes, and a
    // DECIMAL is an i128 of units scaled by the column's declared places — so
    // 1.25 at scale 8 is 125_000_000.
    let response = run_bytes(&db, "SELECT id, cost FROM m").await;
    let rows = rasterizeddb_core::core::row::row::vec_into_rows(&response[1..]).expect("rows");
    assert_eq!(rows.len(), 3, "a row went missing");

    let stored: Vec<i128> = rows
        .iter()
        .map(|row| {
            let bytes = row.columns[1].data.into_slice();
            let mut buf = [0u8; 16];
            let n = bytes.len().min(16);
            buf[..n].copy_from_slice(&bytes[..n]);
            i128::from_le_bytes(buf)
        })
        .collect();
    assert_eq!(stored[0], 0, "an explicit NULL should store nothing");
    assert_eq!(stored[1], 0, "an omitted column should store nothing");
    assert_eq!(stored[2], 125_000_000, "the real value was lost");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_unique_column_holds_any_number_of_nulls() {
    on_a_big_stack(a_unique_column_holds_any_number_of_nulls_body());
}

async fn a_unique_column_holds_any_number_of_nulls_body() {
    // PostgreSQL's default is NULLS DISTINCT: a NULL conflicts with nothing,
    // however often it repeats. This engine tested for NULL by asking whether
    // the stored value was empty — true for a TEXT column, never true for a
    // fixed-width one, where a NULL is that type's zeroes. So two rows with a
    // NULL uuid looked like two copies of the same value: a nullable UNIQUE
    // column could hold exactly one NULL, and importing a table PostgreSQL was
    // perfectly happy with was refused as "already holds duplicate values".
    init_globals();
    let dir = format!("/tmp/rdb_unique_nulls_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);

    for (table, ddl) in [
        ("inline", "CREATE TABLE inline (id INT NOT NULL, ref UUID NULL UNIQUE)"),
        ("keyed", "CREATE TABLE keyed (id INT NOT NULL, ref UUID NULL, UNIQUE (ref))"),
    ] {
        run(&db, ddl).await;
        for id in 1..=3 {
            let out = run(&db, &format!("INSERT INTO {} (id, ref) VALUES ({}, NULL)", table, id)).await;
            assert!(
                !out.to_lowercase().contains("duplicate"),
                "{}: NULL number {} was refused: {}",
                table,
                id,
                out
            );
        }

        // A real repeat is still a repeat.
        run(&db, &format!(
            "INSERT INTO {} (id, ref) VALUES (9, '11111111-1111-1111-1111-111111111111')",
            table
        )).await;
        let clash = run(&db, &format!(
            "INSERT INTO {} (id, ref) VALUES (10, '11111111-1111-1111-1111-111111111111')",
            table
        )).await;
        assert!(
            clash.to_lowercase().contains("duplicate"),
            "{}: a real duplicate was accepted: {}",
            table,
            clash
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
