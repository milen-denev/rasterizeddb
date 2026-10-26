//! Integer literals a column's type cannot hold, and integer arithmetic
//! between literals in a `WHERE`.
//!
//! `smallint_col = 40001` was refused ("literal cannot be compared against a
//! I16 column") where PostgreSQL answers it, and `id <= 500000 + 100` was
//! evaluated per row with no index, ~170 times slower than `id <= 500100`.
//! Both are answered now. Every answer here is checked against the counts the
//! test computes itself, on a table past the filter index's floor and again
//! with a B-tree on the compared column: a planner that took an out-of-range
//! literal for "no candidates" would prune rows and report fewer, with no
//! error anywhere.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

const N: i64 = 6_000;

/// The row's `s`: -1000..=999, NULL on every seventh row.
fn s_of(id: i64) -> Option<i64> {
    (id % 7 != 0).then(|| id % 2000 - 1000)
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get::<i64, _>(0)
}

async fn check_all(pool: &sqlx::PgPool, label: &str) {
    let non_null = (1..=N).filter(|id| s_of(*id).is_some()).count() as i64;
    let s_is = |v: i64| (1..=N).filter(|id| s_of(*id) == Some(v)).count() as i64;
    for (predicate, want) in [
        // Out of the column's range, as literals and as folded arithmetic.
        ("s = 40001", 0),
        ("s <> 40001", non_null),
        ("s < 40001", non_null),
        ("s <= 40001", non_null),
        ("s > 40001", 0),
        ("s >= -40001", non_null),
        ("s < -40001", 0),
        ("40001 > s", non_null),
        ("s = 40000 + 1", 0),
        ("s < 40000 + 1", non_null),
        ("s > -40000 - 1", non_null),
        // In range, folded.
        ("s = 3 - 8", s_is(-5)),
        ("s = -1000 + 0", s_is(-1000)),
        ("s >= 990 + 9", s_is(999)),
        // The primary key, folded: a range and a point.
        ("id >= 3000 AND id <= 3000 + 100", 101),
        ("id = 2999 + 1", 1),
        ("id < 2 * 3", 5),
        ("id > 6000 - 10 AND s IS NOT NULL", (N - 9..=N).filter(|id| s_of(*id).is_some()).count() as i64),
    ] {
        let sql = format!("SELECT COUNT(*) FROM sm WHERE {predicate}");
        assert_eq!(count(pool, &sql).await, want, "{label}: {predicate}");
    }
    // int4 + int4 beyond int4 is PostgreSQL's error, folded or not.
    let overflow = sqlx::query("SELECT COUNT(*) FROM sm WHERE id = 2000000000 + 2000000000")
        .fetch_one(pool)
        .await
        .expect_err("int4 overflow must be an error");
    assert!(overflow.to_string().contains("out of range"), "{label}: {overflow}");
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_literals_and_literal_arithmetic_answer_as_postgres_does() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    sqlx::query("CREATE TABLE sm (id INT PRIMARY KEY, s SMALLINT)").execute(&pool).await.expect("create");
    let ids: Vec<i64> = (1..=N).collect();
    for chunk in ids.chunks(1000) {
        let values: Vec<String> = chunk
            .iter()
            .map(|id| match s_of(*id) {
                Some(s) => format!("({id}, {s})"),
                None => format!("({id}, NULL)"),
            })
            .collect();
        sqlx::query(AssertSqlSafe(format!("INSERT INTO sm (id, s) VALUES {}", values.join(","))))
            .execute(&pool)
            .await
            .expect("insert");
    }
    check_all(&pool, "no index").await;
    sqlx::query("CREATE INDEX sm_s ON sm (s)").execute(&pool).await.expect("index");
    check_all(&pool, "btree on s").await;

    // Folded, a bound with arithmetic is an index range like any other: the
    // scan reads the hundred-odd candidates the primary key names, not the
    // table, which is the whole point of folding it.
    sqlx::query("RESET QUERY STATS").execute(&pool).await.expect("reset");
    // With the `;` pgbench and many drivers send: left on the last operand,
    // it kept this from folding.
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM sm WHERE id >= 4000 AND id <= 4000 + 100;").await, 101);
    let recent = sqlx::query("SHOW QUERIES").fetch_all(&pool).await.expect("show queries");
    let mine = recent
        .iter()
        .find(|r| r.get::<String, _>("statement").contains("4000 + 100"))
        .expect("the range is in the log");
    let scanned = mine.get::<i64, _>("rows_scanned");
    assert!(scanned <= 200, "the folded range read {scanned} of {N} rows: the index was not used");
}
