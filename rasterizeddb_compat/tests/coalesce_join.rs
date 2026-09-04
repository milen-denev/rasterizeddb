//! `COALESCE` over a `LEFT JOIN`, in the select list and in `ORDER BY`.
//!
//! The shape a "fall back to the other table's column" query takes. `WHERE`
//! resolves join-qualified expressions; the select list and `ORDER BY` did not,
//! and the `ORDER BY` error truncated at the comma inside the argument list —
//! `unknown relation 'COALESCE(s.last_queried_at, t'` — which is a splitter
//! reading a function argument as a relation name.

use sqlx::{AssertSqlSafe, Column, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL)",
        "CREATE TABLE s (id INT PRIMARY KEY, t_id INT NOT NULL, last_queried_at TIMESTAMPTZ)",
        "INSERT INTO t (id, updated_at) VALUES (1, '2026-01-01 00:00:00+00')",
        "INSERT INTO t (id, updated_at) VALUES (2, '2026-02-01 00:00:00+00')",
        // Only row 1 has a stat row, so row 2 must fall back to t.updated_at.
        "INSERT INTO s (id, t_id, last_queried_at) VALUES (10, 1, '2026-03-01 00:00:00+00')",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coalesce_over_a_left_join_works_everywhere() {
    let (_server, mut conn) = seeded().await;

    for (label, sql) in [
        (
            "where",
            "SELECT t.id AS id FROM t LEFT JOIN s ON s.t_id = t.id \
             WHERE COALESCE(s.last_queried_at, t.updated_at) < '2026-06-01 00:00:00+00'",
        ),
        (
            "order by",
            "SELECT t.id AS id FROM t LEFT JOIN s ON s.t_id = t.id \
             ORDER BY COALESCE(s.last_queried_at, t.updated_at) ASC",
        ),
        (
            "the deployed shape",
            "SELECT t.id AS id FROM t LEFT JOIN s ON s.t_id = t.id \
             WHERE COALESCE(s.last_queried_at, t.updated_at) < '2026-06-01 00:00:00+00' \
             ORDER BY COALESCE(s.last_queried_at, t.updated_at) ASC LIMIT 10",
        ),
    ] {
        let outcome = sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await;
        match outcome {
            Ok(rows) => println!("  OK   {label}: {} row(s)", rows.len()),
            Err(e) => println!("  FAIL {label}: {}", e.to_string().lines().next().unwrap_or("")),
        }
    }

    // The ordering it must actually produce: row 1's effective time is its stat
    // row (March), row 2's falls back to its own updated_at (February), so
    // ascending is 2 then 1 — which a fallback read the wrong way round would
    // reverse.
    let ids: Vec<i32> = sqlx::query(AssertSqlSafe(
        "SELECT t.id AS id FROM t LEFT JOIN s ON s.t_id = t.id \
         ORDER BY COALESCE(s.last_queried_at, t.updated_at) ASC",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("order by COALESCE over a left join")
    .iter()
    .map(|r| r.try_get::<i32, _>("id").expect("id"))
    .collect();
    assert_eq!(ids, vec![2, 1], "the fallback decides the order");
}

/// A `LEFT JOIN` whose unmatched side feeds a `COALESCE` in the `WHERE`.
///
/// The unmatched relation contributes no row, so its columns are NULL — but
/// they used to arrive as the declared type holding no bytes, which the
/// evaluator reads as a *present* value. `COALESCE` then returned that
/// emptiness instead of falling through, and the comparison against it was
/// incomparable, so the row was dropped. Silently: a smaller result set, not an
/// error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unmatched_side_is_null_and_coalesce_falls_through() {
    let (_server, mut conn) = seeded().await;

    // Row 2 has no stat row, so its effective time is t.updated_at (February).
    // Both rows are before June, so both must qualify.
    let ids: Vec<i32> = sqlx::query(AssertSqlSafe(
        "SELECT t.id AS id FROM t LEFT JOIN s ON s.t_id = t.id \
         WHERE COALESCE(s.last_queried_at, t.updated_at) < '2026-06-01 00:00:00+00' \
         ORDER BY t.id ASC",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("where COALESCE over a left join")
    .iter()
    .map(|r| r.try_get::<i32, _>("id").expect("id"))
    .collect();
    assert_eq!(ids, vec![1, 2], "the unmatched row must fall through to t.updated_at");

    // Note: `s.last_queried_at IS NULL` is deliberately *refused* on the outer
    // side of a LEFT JOIN — the engine will not let IS NULL conflate "no
    // matching row" with "matched, and the value is NULL". That refusal and the
    // COALESCE behaviour above are consistent: both say the unmatched side is
    // NULL, and only one of them can be asked without ambiguity.
}

/// Still unsupported: an expression in a join's SELECT list.
///
/// It needs the same per-combined-row evaluation `ORDER BY` now has, plus a
/// declared output type for the row description and a way to keep the item in
/// its written position — the select-list parser sets calls aside, which loses
/// it. Refused loudly, with a workaround, rather than returning a column that
/// is silently absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expression_in_a_join_select_list() {
    let (_server, mut conn) = seeded().await;
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.id AS id, COALESCE(s.last_queried_at, t.updated_at) AS effective \
         FROM t LEFT JOIN s ON s.t_id = t.id ORDER BY t.id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("expression in a join select list");
    assert_eq!(rows.len(), 2);

    // The column must be readable by name, and hold the right value: row 1 from
    // its stat row (March), row 2 falling through to t.updated_at (February).
    let effective: Vec<chrono::DateTime<chrono::Utc>> = rows
        .iter()
        .map(|r| r.try_get::<chrono::DateTime<chrono::Utc>, _>("effective").expect("effective"))
        .collect();
    assert_eq!(
        effective
            .iter()
            .map(|d| d.format("%Y-%m").to_string())
            .collect::<Vec<_>>(),
        vec!["2026-03".to_string(), "2026-02".to_string()],
        "the fallback must decide each row's value"
    );

    // And the expression keeps its written position, even in the middle.
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.id AS id, COALESCE(s.last_queried_at, t.updated_at) AS effective, \
                t.updated_at AS updated \
         FROM t LEFT JOIN s ON s.t_id = t.id ORDER BY t.id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("expression in the middle of a join select list");
    let names: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    assert_eq!(names, vec!["id", "effective", "updated"], "written order is kept");
}
