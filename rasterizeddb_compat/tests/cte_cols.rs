//! Two defects found while checking the reported ones.
//!
//! 1. A `WITH … SELECT` describes as **no columns** over the extended
//!    protocol. The rows arrive, so the client sees the right row count and
//!    cannot read a single field — the same silent shape the `SHOW BACKUPS`
//!    describe bug had.
//! 2. `UPDATE … WHERE false RETURNING id` fails to compile.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE due (id INT PRIMARY KEY, at TIMESTAMPTZ NOT NULL)",
        "INSERT INTO due (id, at) VALUES (1, '2000-01-01 00:00:00+00')",
        "INSERT INTO due (id, at) VALUES (2, '2099-01-01 00:00:00+00')",
    ] {
        sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect(sql);
    }
    (server, pool)
}

/// Every CTE shape must hand the client its columns, not just its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cte_describes_its_columns() {
    let (_server, pool) = seeded().await;
    for (label, sql) in [
        ("plain select", "SELECT id FROM due WHERE id = 2"),
        ("derived table", "SELECT id FROM (SELECT id FROM due WHERE id = 2) d"),
        ("cte", "WITH d AS (SELECT id FROM due WHERE id = 2) SELECT id FROM d"),
        (
            "cte two columns",
            "WITH d AS (SELECT id, at FROM due WHERE id = 2) SELECT id, at FROM d",
        ),
    ] {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        let names: Vec<String> = rows
            .first()
            .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
        assert!(
            !names.is_empty(),
            "{label}: {} row(s) came back with no columns — the client cannot read any field: {sql}",
            rows.len()
        );
    }
}

/// A bare boolean literal is a predicate on its own.
///
/// sea-orm renders an empty `Condition::all()` as `WHERE TRUE` and an empty
/// `Condition::any()` as `WHERE FALSE`, so a filter chain built from an empty
/// list produced a hard query failure rather than "everything" or "nothing".
/// The row counts are asserted, not just the absence of an error: a predicate
/// that parses and then selects the wrong side is the worse outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bare_boolean_literal_is_a_predicate() {
    let (_server, pool) = seeded().await;

    // Two rows exist, so TRUE means both and FALSE means neither.
    for (label, sql, want) in [
        ("select true", "SELECT id FROM due WHERE true", 2),
        ("select false", "SELECT id FROM due WHERE false", 0),
        ("select TRUE caps", "SELECT id FROM due WHERE TRUE", 2),
        ("true and match", "SELECT id FROM due WHERE true AND id = 1", 1),
        ("false and match", "SELECT id FROM due WHERE false AND id = 1", 0),
        ("true or match", "SELECT id FROM due WHERE false OR id = 1", 1),
        ("returning true", "UPDATE due SET at='2026-01-01 00:00:00+00' WHERE true RETURNING id", 2),
        ("returning false", "UPDATE due SET at='2026-01-01 00:00:00+00' WHERE false RETURNING id", 0),
    ] {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{label}: {sql}: {e}"));
        assert_eq!(rows.len(), want, "{label}: {sql}");
    }

    // A DELETE with the identity element must not empty the table.
    sqlx::query(AssertSqlSafe("DELETE FROM due WHERE false"))
        .execute(&pool)
        .await
        .expect("delete where false");
    let left = sqlx::query(AssertSqlSafe("SELECT id FROM due"))
        .fetch_all(&pool)
        .await
        .expect("count");
    assert_eq!(left.len(), 2, "DELETE ... WHERE false must delete nothing");
}
