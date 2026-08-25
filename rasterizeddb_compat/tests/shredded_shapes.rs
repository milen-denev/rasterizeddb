//! Query shapes over a SHREDDED JSON column.
//!
//! A shredded JSON column is stored as a `UUID` pointing at its shadow table.
//! Every path that describes a column has to say `jsonb`, and every path that
//! formats one has to send the document — and the two have to come from the
//! same source, or the client is told one thing and handed another. This file
//! walks the shapes an application actually emits; the whole file runs
//! shredded (a process-global, hence a file of its own).

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

fn shredded() {
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    assert!(rasterizeddb_core::json_shadow_tables());
}

async fn fixture() -> (TestServer, sqlx::PgPool) {
    shredded();
    let s = TestServer::start().await.expect("start");
    let pool = s.connect_sqlx().await.expect("connect");
    for ddl in [
        "CREATE TABLE \"case\" (id INT PRIMARY KEY, domain_id INT NOT NULL, title TEXT NOT NULL, selected_branches JSONB, created_at TIMESTAMPTZ)",
        "CREATE TABLE case_model (id INT PRIMARY KEY, name TEXT NOT NULL)",
    ] {
        pool.execute(sqlx::raw_sql(ddl)).await.expect(ddl);
    }
    pool.execute(sqlx::raw_sql("INSERT INTO case_model (id, name) VALUES (1, 'm')")).await.unwrap();
    pool.execute(sqlx::raw_sql("INSERT INTO \"case\" (id, domain_id, title, selected_branches, created_at) VALUES (1, 1, 'a', '{\"b\":[1]}', '2026-08-24 10:00:00')")).await.unwrap();
    pool.execute(sqlx::raw_sql("INSERT INTO \"case\" (id, domain_id, title, selected_branches, created_at) VALUES (2, 1, 'b', NULL, '2026-08-24 11:00:00')")).await.unwrap();
    (s, pool)
}

/// The shapes an application emits, each reading the document back.
#[tokio::test]
async fn every_ordinary_shape_returns_the_document() {
    let (_s, pool) = fixture().await;
    let expected = serde_json::json!({"b": [1]});

    for (label, sql) in [
        ("plain", "SELECT selected_branches FROM \"case\" WHERE id = 1"),
        ("all columns", "SELECT id, domain_id, title, selected_branches, created_at FROM \"case\" WHERE id = 1"),
        ("where", "SELECT id, selected_branches FROM \"case\" WHERE domain_id = 1 AND id = 1"),
        ("order by", "SELECT id, selected_branches FROM \"case\" WHERE id = 1 ORDER BY created_at DESC"),
        ("paged", "SELECT id, selected_branches FROM \"case\" WHERE id = 1 ORDER BY id ASC LIMIT 1 OFFSET 0"),
        ("expression item", "SELECT selected_branches, id + 1 AS n FROM \"case\" WHERE id = 1"),
        ("aggregate", "SELECT selected_branches, count(*) AS n FROM \"case\" WHERE id = 1 GROUP BY selected_branches"),
        // A join projects its own output columns rather than going through the
        // single-table projection, so it needed both halves of its own: the
        // executor hydrates the reference into the document, and the plan's
        // output columns are advertised through `reported_type_of`. Without
        // the second, this said `uuid` and every typed client refused the row
        // — while the identical single-relation select was correct.
        ("join", "SELECT c.id, c.selected_branches FROM \"case\" c JOIN case_model m ON m.id = c.domain_id WHERE c.id = 1"),
    ] {
        let row = sqlx::query(sql)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", label, e));
        let got: Option<serde_json::Value> = row
            .try_get("selected_branches")
            .unwrap_or_else(|e| panic!("{}: {}", label, e));
        assert_eq!(got, Some(expected.clone()), "{}", label);
    }

    // A NULL document is NULL through the same shapes, not an empty object.
    let got: Option<serde_json::Value> =
        sqlx::query("SELECT c.selected_branches FROM \"case\" c JOIN case_model m ON m.id = c.domain_id WHERE c.id = 2")
            .fetch_one(&pool)
            .await
            .expect("join, null document")
            .try_get("selected_branches")
            .expect("null decodes");
    assert_eq!(got, None);
}

/// A derived table still disagrees with itself: the description comes from
/// describing the inner query (correctly `jsonb`), while the value comes from
/// the temporary relation the derived table was materialised into, whose
/// column is text — so the document arrives with no `jsonb` version byte and
/// the client reports "unsupported JSONB format version 123", 123 being `{`.
/// Same root as every other bug in this file: a description and a payload
/// built from two different sources.
#[tokio::test]
async fn a_derived_table_returns_the_document() {
    let (_s, pool) = fixture().await;
    let got: Option<serde_json::Value> = sqlx::query(
        "SELECT sub.selected_branches FROM (SELECT selected_branches FROM \"case\" WHERE id = 1) AS sub",
    )
    .fetch_one(&pool)
    .await
    .expect("derived table")
    .try_get("selected_branches")
    .expect("derived decodes");
    assert_eq!(got, Some(serde_json::json!({"b": [1]})));
}
