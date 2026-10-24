//! A JSON column that stores its document rather than shredding it.
//!
//! The shredded mapping is still available behind `json_shadow_tables`; this
//! file is about the default. What it has to prove is that a document goes in
//! and the *same* document comes back — including the shapes shredding could
//! not hold, which is the whole reason the default changed.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
    },
};
use rclite::Arc;

/// See the note in `json_in_transaction.rs`: tokio's default 2 MiB thread
/// stack is not enough for `execute` in a debug build.
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
}

async fn run(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        panic!("{} failed: {}", q, String::from_utf8_lossy(&out[1..]));
    }
    out
}

async fn try_run(db: &Arc<Database>, q: &str) -> Result<(), String> {
    let purpose = recognize_query_purpose(q).ok_or("not recognized")?;
    let out = execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        return Err(String::from_utf8_lossy(&out[1..]).to_string());
    }
    Ok(())
}

fn fresh(tag: &str) -> String {
    let dir = format!("/tmp/rdb_jsonb_native_{}_{}", tag, std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// Whether the response carries `expected` stored as `core::jsonb` encodes it.
///
/// `execute` returns the row as bytes, so what is asserted here is the
/// *storage*: the exact blob the encoder produces appears in the row. What a
/// client sees is a different layer and is covered by the compat suite, which
/// goes over the wire.
fn stores_document(response: &[u8], expected: &str) -> bool {
    let value: serde_json::Value = serde_json::from_str(expected).expect("expected parses");
    let encoded = rasterizeddb_core::core::jsonb::encode(&value);
    response
        .windows(encoded.len())
        .any(|window| window == encoded.as_slice())
}

fn assert_stores(response: &[u8], expected: &str) {
    assert!(
        stores_document(response, expected),
        "the row does not hold {} as it encodes",
        expected
    );
}

#[test]
fn documents_of_every_shape_round_trip() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("shapes");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(&db, "CREATE TABLE t (id INT NOT NULL, doc JSONB NULL)").await;

        // Each of these was refused, mangled, or exploded into tables by the
        // shredded mapping.
        let documents = [
            r#"{"a":1,"b":"two","c":true,"d":null}"#,
            r#"{"nums":[1,2,3],"mixed":[1,"a",true],"deep":[[1,2],[3]]}"#,
            r#"{"date":"lower","Date":"upper"}"#,
            r#"{"QA Comments":"spaces","values_json":[1.5],"select":{"from":1}}"#,
            r#"{"empty_obj":{},"empty_arr":[]}"#,
            r#"{"big":123456789012345678901234567890,"precise":0.12345678901234567890123}"#,
            r#"[1,2,3]"#,
            r#"[]"#,
            r#"{}"#,
            // A bare scalar: the shredded mapping refused these outright.
            r#"42"#,
            r#""just a string""#,
            r#"true"#,
        ];

        for (index, document) in documents.iter().enumerate() {
            let escaped = document.replace('\'', "''");
            run(
                &db,
                &format!("INSERT INTO t (id, doc) VALUES ({}, '{}')", index, escaped),
            )
            .await;
        }

        for (index, document) in documents.iter().enumerate() {
            let got = run(&db, &format!("SELECT doc FROM t WHERE id = {}", index)).await;
            assert!(
                stores_document(&got, document),
                "row {} is not stored as {}",
                index,
                document
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// A document whose keys are data is the case shredding could not hold: it grew
/// a column per key and rewrote the table for each one. Here it is one write.
#[test]
fn a_document_keyed_by_data_costs_one_write() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("mapshaped");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(
            &db,
            "CREATE TABLE stats (id INT NOT NULL, column_stats JSONB NULL)",
        )
        .await;

        // 200 rows, every one carrying keys no other row has.
        for row in 0..200 {
            let document = format!(
                r#"{{"col_{}_{}":{{"nulls":{},"distinct":{}}},"col_{}_b":{{"nulls":0}}}}"#,
                row,
                row,
                row,
                row * 3,
                row
            );
            run(
                &db,
                &format!(
                    "INSERT INTO stats (id, column_stats) VALUES ({}, '{}')",
                    row, document
                ),
            )
            .await;
        }

        // Shredding this made a table per nested object and a column per key.
        // The whole point is that the database is still two tables' worth of
        // files.
        let files = std::fs::read_dir(&dir).expect("dir").count();
        assert!(
            files < 40,
            "a map-shaped column produced {} files; it should not create tables per key",
            files
        );

        let got = run(&db, "SELECT column_stats FROM stats WHERE id = 199").await;
        assert_stores(
            &got,
            r#"{"col_199_199":{"nulls":199,"distinct":597},"col_199_b":{"nulls":0}}"#,
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn a_null_document_stays_null_and_bad_json_is_refused() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("nulls");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(&db, "CREATE TABLE t (id INT NOT NULL, doc JSONB NULL)").await;

        run(&db, "INSERT INTO t (id, doc) VALUES (1, NULL)").await;
        let got = run(&db, "SELECT doc FROM t WHERE id = 1").await;
        assert!(
            !stores_document(&got, "{}"),
            "a NULL document came back as a document"
        );

        // Text that is not a document must be refused at the statement, not
        // stored and discovered on the way out.
        let refused = try_run(&db, "INSERT INTO t (id, doc) VALUES (2, 'not json at all')").await;
        assert!(refused.is_err(), "invalid JSON was accepted");

        // And the refusal left nothing behind.
        let after = run(&db, "SELECT id FROM t WHERE id = 2").await;
        assert_eq!(after.first(), Some(&2), "the query itself should succeed");
        assert!(after.len() < 40, "a refused row was stored anyway");

        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// An UPDATE assigns a document through the same encoding path as an INSERT.
#[test]
fn an_update_replaces_the_document() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("update");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(&db, "CREATE TABLE t (id INT NOT NULL, doc JSONB NULL)").await;
        run(&db, r#"INSERT INTO t (id, doc) VALUES (1, '{"v":1}')"#).await;
        run(
            &db,
            r#"UPDATE t SET doc = '{"v":2,"added":"yes"}' WHERE id = 1"#,
        )
        .await;
        let got = run(&db, "SELECT doc FROM t WHERE id = 1").await;
        assert_stores(&got, r#"{"v":2,"added":"yes"}"#);
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// `->` and `->>` reach into a stored document from SQL. Shredding exposed a
/// document's fields only through the internal shadow table, under a hashed
/// name a client could not be expected to know; these are what PostgreSQL
/// clients actually write.
#[test]
fn a_path_operator_reads_into_a_document() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("paths");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(&db, "CREATE TABLE t (id INT NOT NULL, doc JSONB NULL)").await;
        run(
            &db,
            r#"INSERT INTO t (id, doc) VALUES (1, '{"status":"live","hits":42,"nested":{"a":[10,20]},"flag":true}')"#,
        )
        .await;

        // `->>` yields text: a string unquoted, anything else as its JSON.
        let text = |bytes: Vec<u8>| String::from_utf8_lossy(&bytes).to_string();

        let got = text(run(&db, "SELECT doc->>'status' FROM t WHERE id = 1").await);
        assert!(
            got.contains("live") && !got.contains("\"live\""),
            "->> on a string: {:?}",
            got
        );

        let got = text(run(&db, "SELECT doc->>'hits' FROM t WHERE id = 1").await);
        assert!(got.contains("42"), "->> on a number: {:?}", got);

        let got = text(run(&db, "SELECT doc->>'flag' FROM t WHERE id = 1").await);
        assert!(got.contains("true"), "->> on a bool: {:?}", got);

        // Chained, and into an array.
        let got = text(run(&db, "SELECT doc->'nested'->'a'->>1 FROM t WHERE id = 1").await);
        assert!(got.contains("20"), "chained path: {:?}", got);

        // `#>>` takes the whole path at once.
        let got = text(run(&db, "SELECT doc#>>'{nested,a,0}' FROM t WHERE id = 1").await);
        assert!(got.contains("10"), "#>> path: {:?}", got);

        // A key that is not there is NULL, not an error.
        let out = run(&db, "SELECT doc->>'missing' FROM t WHERE id = 1").await;
        assert_eq!(
            out.first(),
            Some(&2),
            "a missing key should not fail the query"
        );

        // `->` keeps it a document, so the bytes are still the encoded form.
        let got = run(&db, "SELECT doc->'nested' FROM t WHERE id = 1").await;
        assert_stores(&got, r#"{"a":[10,20]}"#);

        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// A path in a `WHERE` clause. Shredding made a document's fields queryable
/// only through the internal shadow table, under a hashed name; this is the
/// form a PostgreSQL client actually writes.
#[test]
fn a_path_operator_filters_rows() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("wherepath");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(
            &db,
            "CREATE TABLE t (id INT NOT NULL, tag TEXT NULL, doc JSONB NULL)",
        )
        .await;
        for (id, status, hits) in [(1, "live", 42), (2, "dead", 7), (3, "live", 99)] {
            run(
                &db,
                &format!(
                    r#"INSERT INTO t (id, tag, doc) VALUES ({}, 'row-{}', '{{"status":"{}","hits":{},"nested":{{"k":"{}"}}}}')"#,
                    id, id, status, hits, status
                ),
            )
            .await;
        }

        let tags = |bytes: Vec<u8>| {
            let text = String::from_utf8_lossy(&bytes).to_string();
            let mut found: Vec<String> = Vec::new();
            for n in 1..=3 {
                if text.contains(&format!("row-{}", n)) {
                    found.push(format!("row-{}", n));
                }
            }
            found
        };

        // Equality on a text value reached through a path.
        let got = tags(run(&db, "SELECT tag FROM t WHERE doc->>'status' = 'live'").await);
        assert_eq!(got, vec!["row-1", "row-3"], "->> equality");

        let got = tags(run(&db, "SELECT tag FROM t WHERE doc->>'status' = 'dead'").await);
        assert_eq!(got, vec!["row-2"], "->> equality, other value");

        // A nested path.
        let got = tags(run(&db, "SELECT tag FROM t WHERE doc->'nested'->>'k' = 'dead'").await);
        assert_eq!(got, vec!["row-2"], "nested path");

        // The `#>>` form reaches the same value.
        let got = tags(run(&db, "SELECT tag FROM t WHERE doc#>>'{nested,k}' = 'live'").await);
        assert_eq!(got, vec!["row-1", "row-3"], "#>> path");

        // A key no document has matches nothing, rather than failing.
        let got = tags(run(&db, "SELECT tag FROM t WHERE doc->>'missing' = 'x'").await);
        assert!(
            got.is_empty(),
            "a missing key should match nothing: {:?}",
            got
        );

        // Combined with an ordinary predicate.
        let got = tags(
            run(
                &db,
                "SELECT tag FROM t WHERE doc->>'status' = 'live' AND id > 1",
            )
            .await,
        );
        assert_eq!(
            got,
            vec!["row-3"],
            "path predicate combined with a column one"
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// `CREATE INDEX ON t ((doc->>'k'))` records that a path is worth indexing.
///
/// Deliberately not automatic: a document keyed by data has unboundedly many
/// paths, so indexing every path one happens to contain would rebuild, in the
/// index, the unbounded growth that made shredding unusable. Only the queries
/// say which path matters, so an operator names it.
#[test]
fn an_index_over_a_path_is_declared_and_survives_a_reopen() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("declareindex");
        {
            let db = Arc::new(Database::new_with_backend(&dir, false).await);
            run(&db, "CREATE TABLE t (id INT NOT NULL, doc JSONB NULL)").await;
            run(
                &db,
                r#"INSERT INTO t (id, doc) VALUES (1, '{"status":"live"}')"#,
            )
            .await;
            run(&db, "CREATE INDEX ix_status ON t ((doc->>'status'))").await;

            let declared = {
                let entry = db.table_for_tests("t").expect("table");
                entry.schema.path_indexes.clone()
            };
            assert_eq!(declared.len(), 1, "the declaration was not recorded");
            assert_eq!(declared[0].name, "ix_status");
            assert_eq!(declared[0].path, "doc->'status'");
        }

        // Reopened: a declaration lives in a sidecar, not in memory.
        {
            let db = Arc::new(Database::new_with_backend(&dir, false).await);
            let declared = {
                let entry = db.table_for_tests("t").expect("table after reopen");
                entry.schema.path_indexes.clone()
            };
            assert_eq!(
                declared.len(),
                1,
                "the declaration did not survive the reopen"
            );

            // Rows still read correctly with a declaration present.
            let got = run(&db, "SELECT id FROM t WHERE doc->>'status' = 'live'").await;
            assert_eq!(got.first(), Some(&2));

            // Dropping it takes it away, and that survives too.
            run(&db, "DROP INDEX ix_status").await;
            let entry = db.table_for_tests("t").expect("table");
            assert!(
                entry.schema.path_indexes.is_empty(),
                "the drop was not recorded"
            );
        }
        {
            let db = Arc::new(Database::new_with_backend(&dir, false).await);
            let entry = db.table_for_tests("t").expect("table");
            assert!(
                entry.schema.path_indexes.is_empty(),
                "the drop did not survive the reopen"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// The refusals, which have to say why rather than accepting and doing nothing.
#[test]
fn an_index_that_cannot_be_built_is_refused_with_a_reason() {
    on_a_big_stack(async {
        init_globals();
        let dir = fresh("refuseindex");
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(
            &db,
            "CREATE TABLE t (id INT NOT NULL, name TEXT NULL, doc JSONB NULL)",
        )
        .await;

        // `->` yields a document, whose type differs row to row — see
        // `path_index`. Indexing it could prune rows that do match.
        let refused = try_run(&db, "CREATE INDEX ix ON t ((doc->'status'))").await;
        let message = refused.expect_err("-> should be refused");
        assert!(message.contains("->>"), "{}", message);

        // A path into something that is not a document.
        let refused = try_run(&db, "CREATE INDEX ix ON t ((name->>'k'))").await;
        assert!(
            refused.is_err(),
            "a path into a non-document should be refused"
        );

        // An index over an ordinary column is not claimed here at all: the
        // engine maintains one over every eligible column already, and over the
        // Postgres wire the statement is answered as an accepted no-op. What
        // matters is that it is not mistaken for a path index — that is the
        // compat suite's `an_ordinary_index_is_still_accepted`.
        assert!(
            recognize_query_purpose("CREATE INDEX ix_name ON t (name)").is_none(),
            "an ordinary index should not be claimed as a path index"
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}
