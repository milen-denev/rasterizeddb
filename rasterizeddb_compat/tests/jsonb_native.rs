//! A stored JSON document, seen the way a client sees it.
//!
//! `json_columns.rs` covers the shredded mapping, which is still available
//! behind `--json-shadow-tables`. This file covers the default: the document is
//! stored as bytes and rebuilt on the way out. What matters is that a driver
//! that asked for `jsonb` gets `jsonb` — the type it is advertised as, and a
//! value it can decode — for documents the shredded mapping could not hold.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// The document stored for `id`, decoded **as JSON by the driver**.
///
/// Decoding into `serde_json::Value` rather than `String` is the assertion that
/// matters: sqlx checks the column's advertised type against the Rust type it
/// was asked for, so this only succeeds if the column really is `jsonb` on the
/// wire.
async fn document(pool: &sqlx::PgPool, id: i32) -> serde_json::Value {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT doc FROM docs WHERE id = {}", id)))
        .fetch_one(pool)
        .await
        .expect("select should succeed");
    row.try_get("doc").expect("doc decodes as JSON")
}

#[tokio::test]
async fn documents_the_shredded_mapping_could_not_hold_round_trip() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");

    // Every one of these was refused, renamed, flattened or exploded into
    // tables by shredding.
    let documents = [
        // Keys differing only in case: one column held both, so one was lost.
        (1, serde_json::json!({"date": "lower", "Date": "upper"})),
        // Keys that are not column names.
        (2, serde_json::json!({"QA Comments": 1, "a.b.c": 2, "": 3, "ключ": 4})),
        // A key containing a SQL keyword — this one stopped a real import.
        (3, serde_json::json!({"values_json": [11203.75, 10696.66], "select": {"from": 1}})),
        // Mixed and nested arrays.
        (4, serde_json::json!({"mixed": [1, "a", true, null], "deep": [[1, 2], [3]]})),
        // A bare scalar: refused outright before.
        (5, serde_json::json!(42)),
        (6, serde_json::json!("just a string")),
        // Deeper than the old nesting limit.
        (7, serde_json::json!({"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":"deep"}}}}}}}}})),
        (8, serde_json::json!({})),
        (9, serde_json::json!([])),
    ];

    for (id, value) in &documents {
        let literal = value.to_string().replace('\'', "''");
        run(&pool, &format!("INSERT INTO docs (id, doc) VALUES ({}, '{}')", id, literal))
            .await
            .unwrap_or_else(|e| panic!("insert {} failed: {}", id, e));
    }

    for (id, value) in &documents {
        assert_eq!(&document(&pool, *id).await, value, "document {} changed", id);
    }
}

/// A map-shaped document — keys that are data — is the case that made shredding
/// unusable. Two hundred rows, no two sharing a key.
#[tokio::test]
async fn a_document_keyed_by_data_is_stored_and_read_back() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");

    for row in 0..200 {
        let value = serde_json::json!({
            format!("col_{}", row): {"nulls": row, "distinct": row * 3},
        });
        let literal = value.to_string().replace('\'', "''");
        run(&pool, &format!("INSERT INTO docs (id, doc) VALUES ({}, '{}')", row, literal))
            .await
            .unwrap_or_else(|e| panic!("row {} failed: {}", row, e));
    }

    let last = document(&pool, 199).await;
    assert_eq!(last, serde_json::json!({"col_199": {"nulls": 199, "distinct": 597}}));

    let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM docs")
        .fetch_one(&pool)
        .await
        .expect("count")
        .try_get("n")
        .expect("n");
    assert_eq!(count, 200);
}

#[tokio::test]
async fn a_null_document_is_null_and_invalid_json_is_refused() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");

    run(&pool, "INSERT INTO docs (id, doc) VALUES (1, NULL)").await.expect("null insert");
    let row = sqlx::query("SELECT doc FROM docs WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    let value: Option<serde_json::Value> = row.try_get("doc").expect("nullable jsonb");
    assert!(value.is_none(), "a NULL document came back as {:?}", value);

    let refused = run(&pool, "INSERT INTO docs (id, doc) VALUES (2, 'not json')").await;
    assert!(refused.is_err(), "text that is not a document was accepted");
}

/// The column has to be advertised as `jsonb`, or a typed driver refuses it —
/// which is exactly what happened when JSON columns were reported as `uuid`
/// and then as `text`.
#[tokio::test]
async fn the_column_is_advertised_as_jsonb() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");
    run(&pool, r#"INSERT INTO docs (id, doc) VALUES (1, '{"k":1}')"#).await.expect("insert");

    let row = sqlx::query("SELECT doc FROM docs WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    use sqlx::Column;
    assert_eq!(
        row.column(0).type_info().to_string().to_lowercase(),
        "jsonb",
        "the column must be advertised as jsonb"
    );

    // `information_schema.columns` is deliberately not checked here: it
    // returns its rows with an empty RowDescription, so no field of it can be
    // read by name. That is a pre-existing gap in the catalogue views, not a
    // JSON one — a plain INT column shows it too.
}

/// `->` and `->>` over the wire, which is the whole point of them: shredding
/// exposed a document's fields only through an internal table named by a hash,
/// so no client could reach them. These are what sqlx and sea-orm emit.
#[tokio::test]
async fn path_operators_work_for_a_client() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");
    for (id, status, hits) in [(1, "live", 42), (2, "dead", 7), (3, "live", 99)] {
        run(
            &pool,
            &format!(
                r#"INSERT INTO docs (id, doc) VALUES ({}, '{{"status":"{}","hits":{},"nested":{{"k":"{}"}}}}')"#,
                id, status, hits, status
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", id, e));
    }

    // Projected as text.
    let status: String = sqlx::query("SELECT doc->>'status' AS s FROM docs WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select")
        .try_get("s")
        .expect("s");
    assert_eq!(status, "live");

    // Filtered on.
    let ids: Vec<i32> = sqlx::query("SELECT id FROM docs WHERE doc->>'status' = 'live'")
        .fetch_all(&pool)
        .await
        .expect("filter")
        .iter()
        .map(|r| r.try_get::<i32, _>("id").expect("id"))
        .collect();
    assert_eq!(ids, vec![1, 3]);

    // A nested path, and the `#>>` spelling of the same thing.
    let ids: Vec<i32> = sqlx::query("SELECT id FROM docs WHERE doc->'nested'->>'k' = 'dead'")
        .fetch_all(&pool)
        .await
        .expect("nested filter")
        .iter()
        .map(|r| r.try_get::<i32, _>("id").expect("id"))
        .collect();
    assert_eq!(ids, vec![2]);

    let ids: Vec<i32> = sqlx::query("SELECT id FROM docs WHERE doc#>>'{nested,k}' = 'dead'")
        .fetch_all(&pool)
        .await
        .expect("#>> filter")
        .iter()
        .map(|r| r.try_get::<i32, _>("id").expect("id"))
        .collect();
    assert_eq!(ids, vec![2]);

    // `->` keeps it a document, so a driver decodes it as one.
    let inner: serde_json::Value = sqlx::query("SELECT doc->'nested' AS n FROM docs WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("-> select")
        .try_get("n")
        .expect("n decodes as JSON");
    assert_eq!(inner, serde_json::json!({"k": "live"}));
}

/// An index over a document path, over the wire.
#[tokio::test]
async fn an_index_over_a_path_is_accepted_and_an_ordinary_one_still_is_too() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, name TEXT, doc JSONB)").await.expect("create");
    run(&pool, r#"INSERT INTO docs (id, doc) VALUES (1, '{"status":"live"}')"#).await.expect("insert");

    run(&pool, "CREATE INDEX ix_status ON docs ((doc->>'status'))")
        .await
        .expect("a path index should be accepted");
    // Idempotent, as a migration that re-runs needs.
    run(&pool, "CREATE INDEX IF NOT EXISTS ix_status ON docs ((doc->>'status'))")
        .await
        .expect("IF NOT EXISTS should be idempotent");

    // Queries keep working with a declaration present.
    let ids: Vec<i32> = sqlx::query("SELECT id FROM docs WHERE doc->>'status' = 'live'")
        .fetch_all(&pool)
        .await
        .expect("filter")
        .iter()
        .map(|r| r.try_get::<i32, _>("id").expect("id"))
        .collect();
    assert_eq!(ids, vec![1]);

    // An ordinary index is still the accepted no-op it was — the guard for
    // path indexes must not have taken it over.
    run(&pool, "CREATE INDEX ix_name ON docs (name)")
        .await
        .expect("an ordinary index should still be accepted");

    run(&pool, "DROP INDEX ix_status").await.expect("drop");
    // Dropping one this engine never stored is not an error either.
    run(&pool, "DROP INDEX ix_name").await.expect("drop ordinary");

    // `->` is refused, with the reason.
    let refused = run(&pool, "CREATE INDEX ix_bad ON docs ((doc->'status'))").await;
    let message = refused.expect_err("-> should be refused");
    assert!(message.to_lowercase().contains("->>"), "{}", message);
}

/// `SELECT COUNT(*) FROM (SELECT …) AS sub_query` — how sea-orm writes a
/// paginated count — materialises the inner query into a real table, so every
/// column of its select list needs a type that table can hold. A `JSONB`
/// column had none, and the whole statement failed rather than just that
/// column: a page count over any table with a document column was impossible.
#[tokio::test]
async fn a_paginated_count_over_a_table_with_a_document_works() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE cases (id INT NOT NULL, name TEXT, doc JSONB)")
        .await
        .expect("create");
    // The first row leaves the document NULL deliberately — the column's type
    // used to be sampled from the first row alone, so a NULL there decided it.
    run(&pool, "INSERT INTO cases (id, name, doc) VALUES (1, 'a', NULL)").await.expect("i1");
    run(&pool, r#"INSERT INTO cases (id, name, doc) VALUES (2, 'b', '{"k":"v"}')"#).await.expect("i2");
    run(&pool, r#"INSERT INTO cases (id, name, doc) VALUES (3, 'c', '{"k":"w"}')"#).await.expect("i3");

    let count: i64 = sqlx::query(
        "SELECT COUNT(*) AS num_items FROM (SELECT id, name, doc FROM cases) AS sub_query",
    )
    .fetch_one(&pool)
    .await
    .expect("a paginated count must work over a document column")
    .try_get("num_items")
    .expect("num_items");
    assert_eq!(count, 3);

    // With a predicate, which is what a real page count carries.
    let count: i64 = sqlx::query(
        "SELECT COUNT(*) AS num_items FROM (SELECT id, doc FROM cases WHERE id > 1) AS sub_query",
    )
    .fetch_one(&pool)
    .await
    .expect("count with a predicate")
    .try_get("num_items")
    .expect("num_items");
    assert_eq!(count, 2);

    // The documents still read back correctly afterwards — materialising a
    // subquery must not disturb the table it read.
    let doc: serde_json::Value = sqlx::query("SELECT doc FROM cases WHERE id = 2")
        .fetch_one(&pool)
        .await
        .expect("select")
        .try_get("doc")
        .expect("doc");
    assert_eq!(doc, serde_json::json!({"k": "v"}));
}

/// `RETURNING` a document column, including when it is NULL.
///
/// This sits where two changes meet: a stored document is rebuilt for
/// `RETURNING` by `hydrate_json_in_returning`, and the assignment path was
/// separately taught to emit a real NULL rather than the empty placeholder. A
/// document column has to come back as a document either way — and a NULL one
/// as NULL, not as `{}`, which is what an empty value would decode to.
#[tokio::test]
async fn returning_a_document_column_gives_back_the_document() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, name TEXT, doc JSONB)")
        .await
        .expect("create");

    // INSERT … RETURNING
    let row = sqlx::query(
        r#"INSERT INTO docs (id, name, doc) VALUES (1, 'a', '{"k":"v"}') RETURNING id, doc"#,
    )
    .fetch_one(&pool)
    .await
    .expect("insert returning");
    let doc: serde_json::Value = row.try_get("doc").expect("doc decodes as JSON");
    assert_eq!(doc, serde_json::json!({"k": "v"}));

    // UPDATE … RETURNING, assigning the document.
    let row = sqlx::query(r#"UPDATE docs SET doc = '{"k":"w"}' WHERE id = 1 RETURNING doc"#)
        .fetch_one(&pool)
        .await
        .expect("update returning");
    let doc: serde_json::Value = row.try_get("doc").expect("doc");
    assert_eq!(doc, serde_json::json!({"k": "w"}));

    // UPDATE … RETURNING where the document is *not* the assigned column.
    let row = sqlx::query("UPDATE docs SET name = 'b' WHERE id = 1 RETURNING name, doc")
        .fetch_one(&pool)
        .await
        .expect("update returning an untouched document");
    let doc: serde_json::Value = row.try_get("doc").expect("doc");
    assert_eq!(doc, serde_json::json!({"k": "w"}), "an untouched document must survive");

    // A NULL document comes back as NULL, not as an empty value that would
    // decode to `{}`.
    run(&pool, "INSERT INTO docs (id, name, doc) VALUES (2, 'c', NULL)").await.expect("insert null");
    let row = sqlx::query("UPDATE docs SET name = 'd' WHERE id = 2 RETURNING name, doc")
        .fetch_one(&pool)
        .await
        .expect("update returning a null document");
    let doc: Option<serde_json::Value> = row.try_get("doc").expect("nullable doc");
    assert!(doc.is_none(), "a NULL document came back as {:?}", doc);
}

/// A derived table, in NATIVE mode — the shape `paginate()` wraps every select
/// in, so it is not exotic.
///
/// Materialising a derived table samples the returned rows to choose the temp
/// relation's column types, then writes each value back as a SQL literal. Both
/// steps have to keep the document's type: the shredded equivalent of this
/// test failed at each of them in turn.
#[tokio::test]
async fn a_derived_table_carries_a_document_through() {
    let (_s, pool) = server().await;
    _ = run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.unwrap();
    _ = run(
        &pool,
        r#"INSERT INTO docs (id, doc) VALUES (1, '{"k":"v","n":[1,2]}')"#,
    )
    .await.unwrap();

    let expected = serde_json::json!({"k": "v", "n": [1, 2]});
    for sql in [
        "SELECT sub.doc FROM (SELECT doc FROM docs WHERE id = 1) AS sub",
        "SELECT sub.doc, sub.id FROM (SELECT id, doc FROM docs) AS sub",
        "SELECT count(*) AS n FROM (SELECT doc FROM docs) AS sub",
    ] {
        let row = sqlx::query(sql)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
        if sql.starts_with("SELECT count") {
            assert_eq!(row.try_get::<i64, _>("n").expect("n"), 1);
            continue;
        }
        let doc: Option<serde_json::Value> = row
            .try_get("doc")
            .unwrap_or_else(|e| panic!("{} could not decode doc: {}", sql, e));
        assert_eq!(doc, Some(expected.clone()), "{} lost the document", sql);
    }
}
