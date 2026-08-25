//! JSON columns, mapped onto native tables.
//!
//! The point of the mapping is that a document stops being opaque: its fields
//! become typed columns that the query engine, the comparison evaluator and the
//! filter index can all see into. These tests check both halves — that a
//! document round-trips, and that its fields are genuinely queryable as their
//! own types.

use rasterizeddb_compat::TestServer;
use sqlx::Row;

/// Every test here is about the *shredded* mapping, which is no longer the
/// default — a document is stored as bytes unless this is on. The switch is
/// process-wide and each test file is its own process, so setting it here
/// covers this file and nothing else.
///
/// `jsonb_native.rs` is the same suite for the default.
async fn server() -> (TestServer, sqlx::PgPool) {
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// The document stored for `id`, parsed back into JSON.
async fn document(pool: &sqlx::PgPool, table: &str, id: i32) -> serde_json::Value {
    let row = sqlx::query(&format!("SELECT doc FROM {} WHERE id = {}", table, id))
        .fetch_one(pool)
        .await
        .expect("select should succeed");
    // Decoded as a document, not as text. A JSON column is advertised as
    // `jsonb`, and a driver checks that against the Rust type it was asked for
    // — so a `String` is refused. That refusal is the fix working: advertised
    // as `text` (or, before that, as the `uuid` it is stored as) an application
    // could not read the column at all.
    row.try_get("doc").expect("doc column")
}

/// The table a JSON column is backed by, named the way the engine names it.
fn shadow_of(parent: &str, column: &str) -> String {
    rasterizeddb_core::core::rql::json_map::shadow_table_name(parent, column)
}

async fn with_document(pool: &sqlx::PgPool, json: &str) {
    run(pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");
    run(pool, &format!("INSERT INTO docs (id, doc) VALUES (1, '{}')", json))
        .await
        .expect("insert");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_round_trips_with_its_types_intact() {
    let (_s, pool) = server().await;
    with_document(
        &pool,
        r#"{"name":"acme","hits":42,"ratio":1.5,"active":true,"tags":["a","b"]}"#,
    )
    .await;

    let doc = document(&pool, "docs", 1).await;
    assert_eq!(doc["name"], serde_json::json!("acme"));
    assert_eq!(doc["hits"], serde_json::json!(42), "an integer stays a number");
    assert_eq!(doc["ratio"], serde_json::json!(1.5));
    assert_eq!(doc["active"], serde_json::json!(true), "a boolean stays a boolean");
    assert_eq!(doc["tags"], serde_json::json!(["a", "b"]));
}

/// The whole reason for the mapping: the fields are real columns.
#[tokio::test(flavor = "multi_thread")]
async fn the_fields_are_queryable_as_native_columns() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");
    for (id, json) in [
        (1, r#"{"name":"acme","hits":42,"seen":"2026-08-20T14:30:00Z","ip":"10.0.0.1"}"#),
        (2, r#"{"name":"globex","hits":7,"seen":"2025-01-01T00:00:00Z","ip":"192.168.0.5"}"#),
    ] {
        run(&pool, &format!("INSERT INTO docs (id, doc) VALUES ({}, '{}')", id, json))
            .await
            .expect("insert");
    }

    let shadow = shadow_of("docs", "doc");

    // A number compares as a number, not as text.
    let names: Vec<String> = sqlx::query(&format!("SELECT name FROM {} WHERE hits > 10", shadow))
        .fetch_all(&pool)
        .await
        .expect("numeric predicate")
        .iter()
        .map(|r| r.try_get::<String, _>("name").unwrap())
        .collect();
    assert_eq!(names, vec!["acme"]);

    // A date string became a timestamp, so a range works.
    let count = sqlx::query(&format!("SELECT name FROM {} WHERE seen > '2026-01-01'", shadow))
        .fetch_all(&pool)
        .await
        .expect("timestamp predicate")
        .len();
    assert_eq!(count, 1);

    // An address string became an address.
    let count = sqlx::query(&format!("SELECT name FROM {} WHERE ip = '10.0.0.1'", shadow))
        .fetch_all(&pool)
        .await
        .expect("inet predicate")
        .len();
    assert_eq!(count, 1);
}

/// A key with no column yet gets one, so a document with new fields extends
/// the table rather than being rejected.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_key_adds_a_column() {
    let (_s, pool) = server().await;
    with_document(&pool, r#"{"a":1}"#).await;

    run(&pool, r#"INSERT INTO docs (id, doc) VALUES (2, '{"a":2,"b":"added"}')"#)
        .await
        .expect("insert with a new key");

    assert_eq!(document(&pool, "docs", 2).await["b"], serde_json::json!("added"));
    // The earlier document is unaffected and simply has no `b`.
    assert_eq!(document(&pool, "docs", 1).await.get("b"), None);
}

/// Refusing a conflicting type would make a column's type depend on whichever
/// document happened to arrive first.
#[tokio::test(flavor = "multi_thread")]
async fn a_conflicting_type_widens_the_column_and_keeps_the_values() {
    let (_s, pool) = server().await;
    with_document(&pool, r#"{"n":10,"keep":"a"}"#).await;

    // An integer column meeting a fraction becomes a double.
    run(&pool, r#"INSERT INTO docs (id, doc) VALUES (2, '{"n":2.5,"keep":"b"}')"#)
        .await
        .expect("insert a fraction");
    assert_eq!(document(&pool, "docs", 1).await["n"], serde_json::json!(10.0));
    assert_eq!(document(&pool, "docs", 2).await["n"], serde_json::json!(2.5));
    assert_eq!(
        document(&pool, "docs", 1).await["keep"],
        serde_json::json!("a"),
        "the other columns are untouched by the widening"
    );

    // Anything genuinely incompatible becomes text.
    run(&pool, r#"INSERT INTO docs (id, doc) VALUES (3, '{"n":"text now","keep":"c"}')"#)
        .await
        .expect("insert a string");
    assert_eq!(document(&pool, "docs", 3).await["n"], serde_json::json!("text now"));
    assert_eq!(
        document(&pool, "docs", 2).await["n"],
        serde_json::json!("2.5"),
        "the earlier values survive as their text form"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_object_becomes_a_table_of_its_own() {
    let (_s, pool) = server().await;
    with_document(&pool, r#"{"outer":"x","inner":{"a":1,"b":"deep"}}"#).await;

    let doc = document(&pool, "docs", 1).await;
    assert_eq!(doc["outer"], serde_json::json!("x"));
    assert_eq!(
        doc["inner"],
        serde_json::json!({"a": 1, "b": "deep"}),
        "the nesting survives the round trip"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_document_stays_null() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");
    run(&pool, "INSERT INTO docs (id, doc) VALUES (1, NULL)").await.expect("insert");

    let row = sqlx::query("SELECT doc FROM docs WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    let doc: Option<String> = row.try_get("doc").expect("doc column");
    assert_eq!(doc, None);
}

/// A `null` inside a document leaves the column absent rather than storing a
/// value that is indistinguishable from a real one.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_field_is_absent_rather_than_stored() {
    let (_s, pool) = server().await;
    with_document(&pool, r#"{"a":1,"b":null}"#).await;

    let doc = document(&pool, "docs", 1).await;
    assert_eq!(doc["a"], serde_json::json!(1));
    assert_eq!(doc.get("b"), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bare_value_is_stored_as_itself() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE docs (id INT NOT NULL, doc JSONB)").await.expect("create");

    // A bare scalar used to be refused, on the reasoning that a shadow table
    // makes fields into columns and elements into rows and a scalar has
    // neither. The element path already stored scalars in typed value columns,
    // though, so the representation existed and only the top level lacked it —
    // and a scalar is the ordinary content of some JSON columns, an attachment
    // holding a file id being the case that forced this.
    run(&pool, "INSERT INTO docs (id, doc) VALUES (1, '42')")
        .await
        .expect("a bare number is a JSON value");
    assert_eq!(document(&pool, "docs", 1).await, serde_json::json!(42));

    // Malformed JSON is still not a document.
    let err = run(&pool, "INSERT INTO docs (id, doc) VALUES (2, '{not json}')")
        .await
        .expect_err("malformed JSON");
    assert!(err.to_ascii_lowercase().contains("json"), "{}", err);
}

/// Two JSON columns must not share a table, however similar their names.
#[tokio::test(flavor = "multi_thread")]
async fn two_json_columns_get_separate_tables() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE two (id INT NOT NULL, a JSONB, b JSONB)").await.expect("create");
    run(&pool, r#"INSERT INTO two (id, a, b) VALUES (1, '{"x":1}', '{"y":2}')"#)
        .await
        .expect("insert");

    let row = sqlx::query("SELECT a, b FROM two WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    // `jsonb`, so both decode as documents. See `document` above.
    let a: serde_json::Value = row.try_get("a").expect("a");
    let b: serde_json::Value = row.try_get("b").expect("b");

    assert_eq!(a, serde_json::json!({"x": 1}));
    assert_eq!(b, serde_json::json!({"y": 2}));
    assert_ne!(shadow_of("two", "a"), shadow_of("two", "b"));
}

/// A string that merely looks numeric must stay text: the timestamp parser
/// accepts a bare integer as an epoch, and some IP parsers accept one too.
#[tokio::test(flavor = "multi_thread")]
async fn strings_that_resemble_other_types_stay_strings() {
    let (_s, pool) = server().await;
    with_document(&pool, r#"{"a":"12345","b":"42","c":"not a date"}"#).await;

    let doc = document(&pool, "docs", 1).await;
    assert_eq!(doc["a"], serde_json::json!("12345"));
    assert_eq!(doc["b"], serde_json::json!("42"));
    assert_eq!(doc["c"], serde_json::json!("not a date"));
}

/// An `UPDATE` assigning a document must store it, exactly as an `INSERT` does.
///
/// It used to report `UPDATE 1` and leave the column reading back as `{}`: the
/// document went to the `UUID` column as raw text, because the mapping was
/// wired into the INSERT arm only. The row's previous contents were gone and
/// nothing said so — a silent total loss of the value, which is worse than
/// refusing the statement would have been.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_stores_the_document_it_assigns() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ud (id INT NOT NULL, doc JSONB)").await.expect("create");
    run(&pool, r#"INSERT INTO ud (id, doc) VALUES (1, '{"a":1,"b":"x"}')"#).await.expect("insert");
    assert_eq!(document(&pool, "ud", 1).await["a"], serde_json::json!(1));

    run(&pool, r#"UPDATE ud SET doc = '{"a":2,"c":true}' WHERE id = 1"#).await.expect("update");
    let doc = document(&pool, "ud", 1).await;
    assert_eq!(doc["a"], serde_json::json!(2), "the new value must be stored");
    assert_eq!(doc["c"], serde_json::json!(true), "a key new to this document must appear");
    assert!(doc.get("b").is_none(), "a key the new document drops must be gone: {}", doc);

    // A field the mapping turned into a column is still queryable as that
    // column after an update, which is the whole point of the mapping.
    let n = sqlx::query("SELECT id FROM ud WHERE id = 1").fetch_all(&pool).await.expect("select");
    assert_eq!(n.len(), 1);

    // Updating a row that has no document yet works too — the first assignment
    // is what creates the shadow row.
    run(&pool, "INSERT INTO ud (id) VALUES (2)").await.expect("insert without a document");
    run(&pool, r#"UPDATE ud SET doc = '{"z":9}' WHERE id = 2"#).await.expect("update");
    assert_eq!(document(&pool, "ud", 2).await["z"], serde_json::json!(9));
}

/// An `UPDATE` assigning a top-level array stores it, and a bare scalar is
/// refused — matching `INSERT` in both directions.
///
/// The pair matters more than either half: a value accepted in one position and
/// refused in the other is how the earlier `{}` corruption stayed invisible.
/// This test previously asserted that arrays were refused; array shredding
/// makes them representable, so what it pins now is that `UPDATE` and `INSERT`
/// still agree, not any particular verdict.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_and_an_insert_agree_on_what_a_document_may_be() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ur (id INT NOT NULL, doc JSONB)").await.expect("create");
    run(&pool, r#"INSERT INTO ur (id, doc) VALUES (1, '{"a":1}')"#).await.expect("insert");

    // A top-level array maps its elements to rows, so it round-trips through
    // both statements.
    for value in ["[]", "[1,2,3]", r#"["alpha","beta"]"#] {
        run(&pool, &format!("UPDATE ur SET doc = '{}' WHERE id = 1", value))
            .await
            .unwrap_or_else(|e| panic!("UPDATE {} was refused: {}", value, e));
        assert_eq!(
            document(&pool, "ur", 1).await,
            serde_json::from_str::<serde_json::Value>(value).unwrap(),
            "{} must round-trip through UPDATE",
            value
        );
    }

    // An array of objects is the shape that matters most in practice — a list
    // of uniform records.
    let records = r#"[{"a":1,"b":"x"},{"a":2,"b":"y"}]"#;
    run(&pool, &format!("UPDATE ur SET doc = '{}' WHERE id = 1", records)).await.expect("records");
    assert_eq!(
        document(&pool, "ur", 1).await,
        serde_json::from_str::<serde_json::Value>(records).unwrap()
    );

    // A bare scalar round-trips through both statements too, and reads back as
    // the scalar rather than as an object or a one-element array.
    let mut next_id = 9;
    for scalar in ["42", r#""text""#, "true", "-1.5", r#""""#] {
        run(&pool, &format!("UPDATE ur SET doc = '{}' WHERE id = 1", scalar))
            .await
            .unwrap_or_else(|e| panic!("UPDATE {} was refused: {}", scalar, e));
        assert_eq!(
            document(&pool, "ur", 1).await,
            serde_json::from_str::<serde_json::Value>(scalar).unwrap(),
            "{} must round-trip through UPDATE",
            scalar
        );

        run(&pool, &format!("INSERT INTO ur (id, doc) VALUES ({}, '{}')", next_id, scalar))
            .await
            .unwrap_or_else(|e| panic!("INSERT {} was refused: {}", scalar, e));
        assert_eq!(
            document(&pool, "ur", next_id).await,
            serde_json::from_str::<serde_json::Value>(scalar).unwrap(),
            "{} must round-trip through INSERT",
            scalar
        );
        next_id += 1;
    }

    // And a document still reads back as a document after all of that.
    run(&pool, &format!("UPDATE ur SET doc = '{}' WHERE id = 1", records)).await.expect("restore");
    assert_eq!(
        document(&pool, "ur", 1).await,
        serde_json::from_str::<serde_json::Value>(records).unwrap()
    );
}