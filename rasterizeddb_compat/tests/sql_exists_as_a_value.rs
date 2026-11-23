//! An EXISTS used as a value — under a cast or arithmetic — rather than as
//! a condition. A correlated one was refused ("a correlated EXISTS used as
//! a value … is not supported"); an uncorrelated one, folded to the
//! predicate `1 = 1`, lost its meaning to the operators around it
//! (`EXISTS (…)::int + 1` read as `1 = 1::int + 1`, false, where PostgreSQL
//! answers 2). Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exists_under_a_cast_or_arithmetic_is_its_value() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE t2 (id int PRIMARY KEY, x int, v int); CREATE TABLE u2 (id int PRIMARY KEY, tid int); \
         INSERT INTO t2 VALUES (1,1,0),(2,6,0),(3,NULL,0); INSERT INTO u2 VALUES (1,2),(2,2),(3,3)"
            .to_string(),
    )))
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT id, x - EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id)::int FROM t2 ORDER BY id", v(&["1|1", "2|5", "3|NULL"])),
        ("SELECT id, EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id)::int + 10 FROM t2 ORDER BY id", v(&["1|10", "2|11", "3|11"])),
        ("SELECT id, (NOT EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id))::int FROM t2 ORDER BY id", v(&["1|1", "2|0", "3|0"])),
        ("SELECT id, EXISTS (SELECT 1 FROM u2 WHERE u2.tid > t2.id)::int * 5 FROM t2 ORDER BY id", v(&["1|5", "2|5", "3|0"])),
        // Uncorrelated, run once and folded: the cast and the arithmetic
        // still apply to its truth value.
        ("SELECT EXISTS (SELECT 1 FROM u2)::int", v(&["1"])),
        ("SELECT id, EXISTS (SELECT 1 FROM u2)::int + 1 FROM t2 ORDER BY id", v(&["1|2", "2|2", "3|2"])),
        ("SELECT id, EXISTS (SELECT 1 FROM u2 WHERE u2.tid = 99)::int + 1 FROM t2 ORDER BY id", v(&["1|1", "2|1", "3|1"])),
        // As a condition, unchanged.
        ("SELECT id FROM t2 WHERE EXISTS (SELECT 1 FROM u2) AND id < 3 ORDER BY id", v(&["1", "2"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    assert_eq!(
        rows(&pool, "UPDATE t2 SET v = x - EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id)::int RETURNING id, v").await,
        v(&["1|1", "2|5", "3|NULL"])
    );
    pool.execute("UPDATE t2 SET v = EXISTS (SELECT 1 FROM u2 WHERE u2.tid = t2.id)::int + 100 WHERE id < 3")
        .await
        .unwrap();
    assert_eq!(rows(&pool, "SELECT id, v FROM t2 ORDER BY id").await, v(&["1|100", "2|101", "3|NULL"]));
}
