//! `COUNT(*) FILTER (WHERE …)` over a join.
//!
//! The shape PMS's scheduler and case dashboards use: several aggregates over
//! one grouped join, each counting a different subset. It was refused outright,
//! which 500s both pages as soon as a single pool row exists.
//!
//! The clause names columns by relation, and the fold sees only the projected
//! row — so each referenced column has to be added to the projection and the
//! predicate rewritten to address it by position. The counts are asserted, not
//! just the absence of an error: a filter that resolved to the wrong column
//! would still return a number.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE pool (id INT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE task (id INT PRIMARY KEY, pool_id INT NOT NULL, status TEXT NOT NULL, \
         assigned_at TIMESTAMPTZ, closed_at TIMESTAMPTZ)",
        "INSERT INTO pool (id, name) VALUES (1, 'alpha')",
        "INSERT INTO pool (id, name) VALUES (2, 'beta')",
        // pool 1: 3 tasks — 2 assigned, 1 closed, 1 queued.
        "INSERT INTO task (id, pool_id, status, assigned_at, closed_at) VALUES (10, 1, 'done', '2026-01-02 00:00:00+00', '2026-01-03 00:00:00+00')",
        "INSERT INTO task (id, pool_id, status, assigned_at, closed_at) VALUES (11, 1, 'busy', '2026-01-02 00:00:00+00', NULL)",
        "INSERT INTO task (id, pool_id, status, assigned_at, closed_at) VALUES (12, 1, 'queued', NULL, NULL)",
        // pool 2: 1 task, queued.
        "INSERT INTO task (id, pool_id, status, assigned_at, closed_at) VALUES (13, 2, 'queued', NULL, NULL)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_aggregates_over_a_grouped_join() {
    let (_server, mut conn) = seeded().await;

    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                COUNT(*) AS total, \
                COUNT(*) FILTER (WHERE t.assigned_at IS NOT NULL) AS taken, \
                COUNT(*) FILTER (WHERE t.closed_at IS NOT NULL) AS completed, \
                COUNT(*) FILTER (WHERE t.status = 'queued') AS queued \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("filtered aggregates over a join must work");

    assert_eq!(rows.len(), 2, "two pools");
    let got: Vec<(i32, i64, i64, i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get("pool_id").expect("pool_id"),
                r.try_get("total").expect("total"),
                r.try_get("taken").expect("taken"),
                r.try_get("completed").expect("completed"),
                r.try_get("queued").expect("queued"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, 3, 2, 1, 1), (2, 1, 0, 0, 1)],
        "each filter must count its own subset"
    );
}

/// A filter naming a column on the *other* side of the join, which is the
/// reason the clause cannot simply be folded into the WHERE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_may_name_the_other_relation() {
    let (_server, mut conn) = seeded().await;
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                COUNT(*) AS total, \
                COUNT(*) FILTER (WHERE p.name = 'alpha') AS in_alpha \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("a filter on the joined relation");
    let got: Vec<(i32, i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get("pool_id").expect("pool_id"),
                r.try_get("total").expect("total"),
                r.try_get("in_alpha").expect("in_alpha"),
            )
        })
        .collect();
    assert_eq!(got, vec![(1, 3, 3), (2, 1, 0)]);
}

/// `MIN(x) FILTER (…)`, and an unfiltered aggregate beside a filtered one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_min_beside_an_unfiltered_count() {
    let (_server, mut conn) = seeded().await;
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                COUNT(*) AS total, \
                MIN(t.id) FILTER (WHERE t.status = 'queued') AS first_queued \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("a filtered MIN over a join");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<i64, _>("total").expect("total"), 3);
    assert_eq!(rows[0].try_get::<i32, _>("first_queued").expect("min"), 12);
    assert_eq!(rows[1].try_get::<i32, _>("first_queued").expect("min"), 13);
}

/// PMS's `scheduler_pool_ops_stats` shape: several filtered aggregates over one
/// grouped join, which is the part that 500s the page.
///
/// `EXTRACT(EPOCH FROM …)` is covered by `extract_epoch.rs`. It is deliberately
/// *not* wrapped in an aggregate here: `MAX(EXTRACT(EPOCH FROM (a - b)))` is an
/// aggregate whose argument is an expression, which the join's fold does not
/// support — it resolves an aggregate argument to a projected column. That is a
/// third gap, tracked separately below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scheduler_pool_stats_shape() {
    let (_server, mut conn) = seeded().await;

    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                COUNT(*) FILTER (WHERE t.assigned_at IS NOT NULL) AS taken, \
                COUNT(*) FILTER (WHERE t.closed_at IS NOT NULL) AS completed, \
                MIN(t.assigned_at) FILTER (WHERE t.status = 'busy') AS oldest_busy, \
                MIN(t.id) FILTER (WHERE t.status = 'queued') AS first_queued \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         WHERE p.name IS NOT NULL \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("the scheduler pool stats shape must run");

    assert_eq!(rows.len(), 2, "two pools");
    assert_eq!(rows[0].try_get::<i64, _>("taken").expect("taken"), 2);
    assert_eq!(rows[0].try_get::<i64, _>("completed").expect("completed"), 1);
    assert_eq!(rows[0].try_get::<i32, _>("first_queued").expect("first"), 12);
    assert_eq!(rows[1].try_get::<i64, _>("taken").expect("taken"), 0);
}

/// Open: an aggregate whose argument is an expression, over a join.
///
/// The fold addresses an aggregate's argument as a projected column, so an
/// expression there has nowhere to live yet. It needs the value projected as a
/// computed column first — the machinery the select-list expressions use — and
/// then the aggregate pointed at that position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_over_an_expression_argument() {
    let (_server, mut conn) = seeded().await;
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                MAX(EXTRACT(EPOCH FROM (t.closed_at - t.assigned_at))) AS longest \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("an aggregate over an expression argument");
    assert_eq!(rows[0].try_get::<f64, _>("longest").expect("longest"), 86_400.0);
}

/// A `FILTER` clause is a whole boolean expression, not one comparison.
///
/// Every clause PMS writes is an `AND` of two or three tests. A
/// single-comparison parser refused those outright — and parsed an `OR` as one
/// comparison whose right-hand side was the rest of the clause, which counted
/// **nothing** and returned it as a number. The second is the dangerous one, so
/// the counts are asserted rather than the absence of an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_clause_may_be_a_boolean_expression() {
    let (_server, mut conn) = seeded().await;

    let rows = sqlx::query(AssertSqlSafe(
        "SELECT t.pool_id AS pool_id, \
                COUNT(*) FILTER (WHERE t.assigned_at IS NOT NULL AND t.status = 'done') AS done_and_taken, \
                COUNT(*) FILTER (WHERE t.status = 'done' OR t.status = 'queued') AS done_or_queued, \
                COUNT(*) FILTER (WHERE t.assigned_at IS NOT NULL \
                                   AND t.closed_at IS NOT NULL \
                                   AND t.closed_at >= t.assigned_at) AS closed_after_taken \
         FROM task t JOIN pool p ON p.id = t.pool_id \
         GROUP BY t.pool_id ORDER BY t.pool_id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("a boolean FILTER clause over a join");

    let got: Vec<(i32, i64, i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get("pool_id").expect("pool_id"),
                r.try_get("done_and_taken").expect("and"),
                r.try_get("done_or_queued").expect("or"),
                r.try_get("closed_after_taken").expect("three"),
            )
        })
        .collect();
    // Pool 1: task 10 is done+assigned; done|queued is tasks 10 and 12; task 10
    // is the only one closed after being assigned.
    // Pool 2: only task 13, queued.
    assert_eq!(got, vec![(1, 1, 2, 1), (2, 0, 1, 0)]);
}

/// PMS's construct verbatim: `(AGG(EXTRACT(EPOCH FROM (a - b))) FILTER (WHERE
/// …))::float8`, over a LEFT JOIN, grouped.
///
/// Five separate things at once — a cast around a filtered aggregate, a
/// multi-term FILTER, EXTRACT, an aggregate whose argument is an expression,
/// and an outer side that is often NULL. Each was its own gap; this is the
/// shape that has to work for `/app/cases` to load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_deployed_dashboard_construct() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE sp (id INT PRIMARY KEY, domain_id INT NOT NULL)",
        "CREATE TABLE spc (id INT PRIMARY KEY, pool_id INT NOT NULL, case_id INT NOT NULL, \
         created_at TIMESTAMPTZ NOT NULL, assigned_at TIMESTAMPTZ)",
        "CREATE TABLE cse (id INT PRIMARY KEY, closed_at TIMESTAMPTZ)",
        "INSERT INTO sp (id, domain_id) VALUES (1, 7)",
        // Taken after 60s, closed 120s after being assigned.
        "INSERT INTO cse (id, closed_at) VALUES (100, '2026-01-01 00:03:00+00')",
        "INSERT INTO spc (id, pool_id, case_id, created_at, assigned_at) VALUES \
         (10, 1, 100, '2026-01-01 00:00:00+00', '2026-01-01 00:01:00+00')",
        // Taken after 30s, never closed — the unmatched outer side.
        "INSERT INTO spc (id, pool_id, case_id, created_at, assigned_at) VALUES \
         (11, 1, 999, '2026-01-01 00:00:00+00', '2026-01-01 00:00:30+00')",
        // Never taken at all.
        "INSERT INTO spc (id, pool_id, case_id, created_at, assigned_at) VALUES \
         (12, 1, 998, '2026-01-01 00:00:00+00', NULL)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }

    let row = sqlx::query(AssertSqlSafe(
        "SELECT spc.pool_id AS pool_id, \
                COUNT(*) FILTER (WHERE spc.assigned_at IS NOT NULL) AS taken, \
                (AVG(EXTRACT(EPOCH FROM (spc.assigned_at - spc.created_at))) \
                    FILTER (WHERE spc.assigned_at IS NOT NULL \
                              AND spc.assigned_at >= spc.created_at))::float8 AS avg_take_seconds, \
                (MIN(EXTRACT(EPOCH FROM (cse.closed_at - spc.assigned_at))) \
                    FILTER (WHERE spc.assigned_at IS NOT NULL \
                              AND cse.closed_at IS NOT NULL \
                              AND cse.closed_at >= spc.assigned_at))::float8 AS fastest_finish_seconds \
         FROM spc \
         JOIN sp ON sp.id = spc.pool_id \
         LEFT JOIN cse ON cse.id = spc.case_id \
         WHERE sp.domain_id = 7 \
         GROUP BY spc.pool_id",
    ))
    .fetch_one(&mut conn)
    .await
    .expect("the deployed dashboard construct must run");

    assert_eq!(row.try_get::<i64, _>("taken").expect("taken"), 2);
    // Take times are 60s and 30s; row 12 was never assigned and the filter
    // excludes it, so the average is 45 rather than 30.
    assert_eq!(
        row.try_get::<f64, _>("avg_take_seconds").expect("avg"),
        45.0,
        "the filter must exclude the untaken row from the average"
    );
    // Only row 10 was ever closed: 120 seconds after it was assigned. Row 11's
    // unmatched `closed_at` is NULL and must not contribute a zero.
    assert_eq!(
        row.try_get::<f64, _>("fastest_finish_seconds").expect("fastest"),
        120.0,
        "an unmatched outer side must not count as a finish"
    );
}
