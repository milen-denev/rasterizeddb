//! A predicate with more comparisons than the compiled program can index.
//!
//! `EvalOp::Value` carried the comparison's index as a `u16`, written with
//! `index as u16`. Past 65,535 that truncates, so the indices wrapped to
//! 0, 1, 2 … and the boolean program became a *valid-looking* program over the
//! wrong comparisons. Nothing caught it: every wrapped index resolves to a
//! result that exists, so the "plan and results disagree, fall back" guard in
//! `evaluate_comparison_results` never fired. The predicate silently stopped
//! discriminating and the query returned every row.
//!
//! It was reachable from ordinary SQL rather than only from a pathological
//! literal list. A correlated `EXISTS` is rewritten to `IN`, and the inner
//! query is materialised one literal per inner row — so
//! `EXISTS (SELECT 1 FROM t WHERE t.k = outer.k)` over a table of more than
//! 65,535 rows returned every outer row, and `NOT EXISTS` returned every outer
//! row too. Both cannot be true. On the deployment this was found on, that
//! made a migration's "which rows are missing" query report all of them, every
//! hour, for twelve hours.
//!
//! The boundary was exact: 65,535 correct, 65,536 wrong.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

/// Two rows, one of which matches, so a working predicate answers 1 and a
/// dropped one answers 2. The difference between "filtered" and "ignored" has
/// to be visible in the count, not just in an error.
async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE outer_t (oid INT, note VARCHAR)",
        "INSERT INTO outer_t (oid, note) VALUES (5, 'present'), (999999999, 'absent')",
    ] {
        sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect(sql);
    }
    (server, pool)
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    let row = sqlx::query(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{}", &sql[..sql.len().min(120)]));
    row.try_get::<i64, _>(0).expect("count column")
}

/// The literal form, tested either side of the old boundary.
///
/// 65,535 passed before the fix as well; it is here so a regression that moves
/// the limit rather than removing it cannot pass by only testing the far side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_list_past_the_index_width_still_filters() {
    let (_server, pool) = seeded().await;

    for n in [65_535usize, 65_536, 70_000] {
        let mut sql = String::with_capacity(n * 7 + 64);
        sql.push_str("SELECT COUNT(*) FROM outer_t o WHERE o.oid IN (");
        for i in 1..=n {
            if i > 1 {
                sql.push(',');
            }
            sql.push_str(&i.to_string());
        }
        sql.push(')');

        assert_eq!(
            count(&pool, &sql).await,
            1,
            "an IN list of {n} values stopped filtering; \
             2 means the predicate was dropped, not that it matched"
        );
    }
}

/// The form that made this reachable in practice.
///
/// The inner table has to exceed the old boundary for the rewrite to produce
/// more comparisons than a `u16` could index, so this is deliberately a large
/// insert rather than a handful of rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_correlated_exists_over_a_large_table_still_filters() {
    let (_server, pool) = seeded().await;
    sqlx::query(AssertSqlSafe("CREATE TABLE inner_t (id INT)"))
        .execute(&pool)
        .await
        .expect("create inner");

    // 70,000 rows: enough to pass 65,535, written in batches so no single
    // statement is unreasonable.
    const ROWS: usize = 70_000;
    const BATCH: usize = 5_000;
    let mut next = 1usize;
    while next <= ROWS {
        let last = (next + BATCH - 1).min(ROWS);
        let mut sql = String::with_capacity(BATCH * 10 + 64);
        sql.push_str("INSERT INTO inner_t (id) VALUES ");
        for i in next..=last {
            if i > next {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&i.to_string());
            sql.push(')');
        }
        sqlx::query(AssertSqlSafe(sql.as_str())).execute(&pool).await.expect("seed inner");
        next = last + 1;
    }

    // `oid` 5 is in `inner_t`; 999999999 is not. Exactly one row qualifies for
    // each predicate, and the two must not agree — before the fix both
    // answered 2, which is the tell that the subquery contributed nothing.
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outer_t o \
             WHERE EXISTS (SELECT 1 FROM inner_t i WHERE i.id = o.oid)",
        )
        .await,
        1,
        "correlated EXISTS over a table larger than the old index width was ignored"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outer_t o \
             WHERE NOT EXISTS (SELECT 1 FROM inner_t i WHERE i.id = o.oid)",
        )
        .await,
        1,
        "correlated NOT EXISTS over a large table was ignored"
    );

    // The explicit form someone would reach for as a workaround. It goes
    // through the same materialisation, so it broke the same way — and was
    // briefly, wrongly, reported as unaffected because it had only been tried
    // with a one-row inner query.
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outer_t o WHERE o.oid IN (SELECT i.id FROM inner_t i)",
        )
        .await,
        1,
        "IN (SELECT …) over a large table was ignored"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM outer_t o WHERE o.oid NOT IN (SELECT i.id FROM inner_t i)",
        )
        .await,
        1,
        "NOT IN (SELECT …) over a large table was ignored"
    );
}
