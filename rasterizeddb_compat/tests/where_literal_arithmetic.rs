//! Literal arithmetic in a WHERE comparison over a NUMERIC column, and the
//! per-row recheck of `UPDATE … FROM` / `DELETE … USING` that is written the
//! same way.
//!
//! `v < 0.1234 * 100` on a NUMERIC column failed with "operand size does not
//! match the column type being evaluated" whenever the comparison was in
//! parentheses: the constant folder split `(v < 0.1234 * 100)` at its `<`
//! into `(v ` and ` 0.1234 * 100)`, neither of which folds, and the
//! evaluator compared the arithmetic's result to the column at the wrong
//! width. A join write's recheck is `AND (<terms>)`, so its recheck failed,
//! and the write fell back to the key alone, with no recheck, silently.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i64> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect()
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> u64 {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

#[tokio::test(flavor = "multi_thread")]
async fn literal_arithmetic_compares_with_a_numeric_column() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for ty in ["NUMERIC(10,4)", "INT", "DOUBLE PRECISION"] {
        run(&pool, "DROP TABLE IF EXISTS w").await;
        run(&pool, &format!("CREATE TABLE w (id BIGINT PRIMARY KEY, v {ty}, flag INT)")).await;
        run(&pool, "INSERT INTO w VALUES (1, 5, 0), (2, 12, 0), (3, 30, 0)").await;
        for (pred, want) in [
            ("v < 0.1234 * 100", vec![1, 2]),
            ("v < 5 * 4", vec![1, 2]),
            ("v > 12 - 0.000001", vec![2, 3]),
            ("v <= 2.5 * 4.8", vec![1, 2]),
            ("(v < 100 / 4) AND (v > 2 * 3)", vec![2]),
        ] {
            assert_eq!(
                ids(&pool, &format!("SELECT id FROM w WHERE id > 0 AND ({pred}) ORDER BY id")).await,
                want,
                "{ty}: WHERE ({pred})"
            );
        }
        assert_eq!(run(&pool, "UPDATE w SET flag = 1 WHERE id > 0 AND (v < 0.1234 * 100)").await, 2, "{ty}");
        assert_eq!(ids(&pool, "SELECT id FROM w WHERE flag = 1 ORDER BY id").await, [1, 2], "{ty}");
    }
}

/// The recheck carries the source's values into literal arithmetic, and
/// runs: the right rows are written, in UPDATE … FROM and DELETE … USING.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_writes_recheck_with_literal_arithmetic_runs() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v NUMERIC(10,4), flag INT);
         INSERT INTO t VALUES (1, 5, 0), (2, 12, 0), (3, 30, 0), (4, 0.5, 0);
         CREATE TABLE s (id BIGINT PRIMARY KEY, w NUMERIC(10,4), w2 NUMERIC(10,4));
         INSERT INTO s VALUES (1, 0.1234, 5), (2, 0.1, 12), (3, 0.2, 31), (4, 0.001, 0.5);",
    )
    .await;
    // 5 < 12.34, 12 < 10 no, 30 < 20 no, 0.5 < 0.1 no.
    assert_eq!(run(&pool, "UPDATE t SET flag = 1 FROM s WHERE t.id = s.id AND t.v < s.w * 100").await, 1);
    // 5 > 4.999999, 12 > 11.999999, 30 > 30.999999 no, 0.5 > 0.499999.
    assert_eq!(run(&pool, "UPDATE t SET flag = flag + 10 FROM s WHERE t.id = s.id AND t.v > s.w2 - 0.000001").await, 3);
    assert_eq!(ids(&pool, "SELECT flag::BIGINT FROM t ORDER BY id").await, [11, 10, 0, 10]);
    // Only 0.5 < 0.5 * 0.5 + 1.
    assert_eq!(run(&pool, "DELETE FROM t USING s WHERE t.id = s.id AND t.v < s.w2 * 0.5 + 1").await, 1);
    assert_eq!(ids(&pool, "SELECT id FROM t ORDER BY id").await, [1, 2, 3]);
}
