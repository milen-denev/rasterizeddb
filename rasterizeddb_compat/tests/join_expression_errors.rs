//! An expression that fails is the statement's error, never a NULL.
//!
//! The join projection and the join ORDER BY logged a failing expression and
//! carried on with NULL: `UPDATE t4 SET s = w.a + 1 FROM w …` stored NULL and
//! reported success, a join SELECT returned NULL as data, and a join ORDER BY
//! sorted the failed rows as NULLs. A CHECK that could not be evaluated for a
//! row was logged and the row stored unchecked. Each now fails (or, where the
//! value is exact, answers it), and a failure that is not about NUMERIC — a
//! division by zero — fails the same way.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0).unwrap_or_else(|| "NULL".into()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_expression_fails_the_statement() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE w (id INT PRIMARY KEY, a NUMERIC(20,12));
         CREATE TABLE t4 (id INT PRIMARY KEY, s NUMERIC(20,12));
         CREATE TABLE f (id INT PRIMARY KEY, add NUMERIC(20,12));
         CREATE TABLE u (id INT PRIMARY KEY, r NUMERIC(18,4));
         INSERT INTO w VALUES (1, 0.123456789123), (2, 1.5);
         INSERT INTO t4 VALUES (1, 0), (2, 0);
         INSERT INTO f VALUES (1, 0.000000000005), (2, 0.00005);
         INSERT INTO u VALUES (1, 0), (2, 0);",
    ))
    .await
    .expect("setup");

    // The four repros: each stores or answers the exact value.
    pool.execute("UPDATE t4 SET s = w.a + 1 FROM w WHERE w.id = t4.id").await.expect("update from");
    assert_eq!(texts(&pool, "SELECT s::text FROM t4 ORDER BY id").await, ["1.123456789123", "2.500000000000"]);
    pool.execute("UPDATE u SET r = f.add * 2 FROM f WHERE f.id = u.id").await.expect("update from");
    assert_eq!(texts(&pool, "SELECT r::text FROM u ORDER BY id").await, ["0.0000", "0.0001"]);
    assert_eq!(
        texts(&pool, "SELECT (w.a + 1)::text FROM t4 JOIN w ON w.id = t4.id ORDER BY t4.id").await,
        ["1.123456789123", "2.500000000000"]
    );
    let ids: Vec<i32> = sqlx::query_scalar("SELECT t4.id FROM t4 JOIN w ON w.id = t4.id ORDER BY w.a + 1 DESC")
        .fetch_all(&pool)
        .await
        .expect("order by");
    assert_eq!(ids, [2, 1]);

    // A failure that is not NUMERIC's: in a join's select list, its ORDER BY,
    // and its aggregate; and in an UPDATE … FROM.
    for sql in [
        "SELECT t4.id, 1 / (t4.id - 1) FROM t4 JOIN w ON w.id = t4.id",
        "SELECT t4.id FROM t4 JOIN w ON w.id = t4.id ORDER BY 1 / (t4.id - 1)",
        "SELECT sum(1 / (t4.id - 1)) FROM t4 JOIN w ON w.id = t4.id",
        "UPDATE t4 SET s = 1 / (w.id - 1) FROM w WHERE w.id = t4.id",
    ] {
        let simple = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await;
        let extended = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await;
        for (protocol, result) in [("simple", simple), ("extended", extended)] {
            let e = result.expect_err(sql);
            assert_eq!(code(&e), "22012", "{protocol}: {sql}: {e}");
        }
    }
    assert_eq!(texts(&pool, "SELECT s::text FROM t4 ORDER BY id").await, ["1.123456789123", "2.500000000000"]);

    // A CHECK that cannot be evaluated for the row fails it.
    pool.execute("CREATE TABLE c (id INT PRIMARY KEY, n INT, CHECK (10 / n > 0))").await.expect("create");
    pool.execute("INSERT INTO c VALUES (1, 5)").await.expect("evaluable");
    let e = pool.execute("INSERT INTO c VALUES (2, 0)").await.expect_err("division by zero");
    assert_eq!(code(&e), "22012", "{e}");
    let e = pool.execute("UPDATE c SET n = 0 WHERE id = 1").await.expect_err("division by zero");
    assert_eq!(code(&e), "22012", "{e}");
    assert_eq!(texts(&pool, "SELECT n::text FROM c").await, ["5"]);
}
