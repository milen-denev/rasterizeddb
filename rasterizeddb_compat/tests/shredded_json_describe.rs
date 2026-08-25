//! A JSON column in the SHREDDED mapping, described beside an aggregate.
//!
//! Shredded mode stores a JSON column as a `UUID` pointing at the shadow
//! table its document was mapped into, and `reported_type_of` exists so the
//! client is told `jsonb` — the type it declared and the type of the bytes it
//! actually receives. Its comment says every path that describes a column has
//! to agree on that. They do not: the describe path returns early for an
//! aggregate plan and builds column types from the plan, where a plain column
//! reports its RAW storage type. The same column then describes as `jsonb` in
//! a plain select and as `uuid` beside a `COUNT(*)`.
//!
//! This whole file runs in shredded mode. That mode is a process-global set
//! once, and each test binary is its own process, so it must live in a file
//! of its own — which is also why this class of bug was invisible: every
//! other test in the suite runs NATIVE, where the column's own type is
//! already `JSONB` and the gap cannot appear.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

/// Set before the server starts, and before any table is created.
fn use_shredded_mapping() {
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    assert!(
        rasterizeddb_core::json_shadow_tables(),
        "this file must run with the shredded JSON mapping"
    );
}

async fn server() -> (TestServer, sqlx::PgPool) {
    use_shredded_mapping();
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// Both failures this pinned are fixed; it now guards them.
///
///   1. the value was mis-encoded — the metadata that feed the formatter said
///      `uuid`, so the document was rendered as a UUID;
///   2. beside an aggregate the column was DESCRIBED as `uuid` — the
///      production error, measured independently against the deployed server.
#[tokio::test]
async fn a_shredded_json_column_describes_as_jsonb_everywhere() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE \"case\" (id INT PRIMARY KEY, title TEXT NOT NULL, selected_branches JSONB)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO \"case\" (id, title, selected_branches) VALUES (1, 'a', NULL)")
        .await
        .expect("insert");

    // Plain select — correct today.
    let row = sqlx::query("SELECT selected_branches FROM \"case\" LIMIT 1")
        .fetch_one(&pool)
        .await
        .expect("plain select");
    assert_eq!(
        row.try_get::<Option<serde_json::Value>, _>("selected_branches")
            .expect("a plain select describes the column as jsonb"),
        None
    );

    // Beside a cast and beside an expression — both correct today, measured
    // against the production server before being pinned here.
    for sql in [
        "SELECT selected_branches, id + 1 AS n FROM \"case\" LIMIT 1",
        "SELECT selected_branches, title AS t FROM \"case\" LIMIT 1",
    ] {
        let row = sqlx::query(sql).fetch_one(&pool).await.expect(sql);
        assert_eq!(
            row.try_get::<Option<serde_json::Value>, _>("selected_branches")
                .unwrap_or_else(|e| panic!("{}: {}", sql, e)),
            None
        );
    }

    // Beside an aggregate — the production failure. The row's bytes are the
    // document either way; only the advertised type is wrong.
    let row = sqlx::query(
        "SELECT selected_branches, count(*) AS n FROM \"case\" GROUP BY selected_branches LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("the grouped select runs");
    assert_eq!(
        row.try_get::<Option<serde_json::Value>, _>("selected_branches")
            .expect("an aggregate must not change how the column is described"),
        None,
        "described as its raw storage type beside an aggregate"
    );
}

/// The other half, and the reason the test above cannot stand alone: it
/// inserts a NULL document, so every assertion expects `None` — which is also
/// what a *lost* document returns. This one stores a real document and demands
/// it back.
///
/// It matters because fixing only the advertised type converts a loud client
/// error into a silent NULL: the driver stops complaining and the application
/// reads a row whose document has vanished. Storage is correct throughout;
/// only what reaches the client is wrong.
#[tokio::test]
async fn a_shredded_document_survives_a_constant_in_the_select_list() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE cases (id INT NOT NULL, title TEXT, sb JSONB)")
        .await
        .expect("create");
    run(
        &pool,
        r#"INSERT INTO cases (id, title, sb) VALUES (1, 'a', '{"k":"v"}')"#,
    )
    .await
    .expect("insert");

    let expected = serde_json::json!({"k": "v"});
    for sql in [
        "SELECT sb FROM cases",
        "SELECT sb, title FROM cases",
        "SELECT sb, 1 AS one FROM cases",
        "SELECT 1 AS one, sb FROM cases",
        "SELECT sb, title || 'x' AS t FROM cases",
        "SELECT sb, count(*) AS n FROM cases GROUP BY sb",
    ] {
        let row = sqlx::query(sql)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
        let doc: Option<serde_json::Value> = row
            .try_get("sb")
            .unwrap_or_else(|e| panic!("{} could not decode sb: {}", sql, e));
        assert_eq!(doc, Some(expected.clone()), "{} lost the document", sql);
    }
}

/// Through a JOIN — the shape the reporting application actually uses, and the
/// one still broken after the single-table paths were fixed.
///
/// The join path builds its own output columns (`JoinPlan::output_columns`)
/// from each item's raw `SchemaField`, so it never consulted `reported_type`
/// and never hydrated the document either. A plain two-table select of a
/// shredded column therefore failed where the same column read straight from
/// its table succeeded.
#[tokio::test]
async fn a_shredded_document_survives_a_join() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE model (id INT NOT NULL, name TEXT)")
        .await
        .expect("create model");
    run(
        &pool,
        "CREATE TABLE item (id INT NOT NULL, model_id INT, doc JSONB)",
    )
    .await
    .expect("create item");
    run(&pool, "INSERT INTO model (id, name) VALUES (1, 'm')")
        .await
        .expect("insert model");
    run(
        &pool,
        r#"INSERT INTO item (id, model_id, doc) VALUES (1, 1, '{"k":"v"}')"#,
    )
    .await
    .expect("insert item");

    let expected = serde_json::json!({"k": "v"});
    for sql in [
        // No aggregate, no computed item, no GROUP BY — just the join.
        "SELECT i.doc FROM item i INNER JOIN model m ON i.model_id = m.id",
        "SELECT i.doc, m.name FROM item i INNER JOIN model m ON i.model_id = m.id",
        "SELECT i.id, i.doc, m.name FROM item i INNER JOIN model m ON i.model_id = m.id",
        "SELECT i.doc, count(*) AS n FROM item i INNER JOIN model m ON i.model_id = m.id GROUP BY i.doc",
    ] {
        let row = sqlx::query(sql)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
        let doc: Option<serde_json::Value> = row
            .try_get("doc")
            .unwrap_or_else(|e| panic!("{} could not decode doc: {}", sql, e));
        assert_eq!(doc, Some(expected.clone()), "{} lost the document", sql);
    }
}
