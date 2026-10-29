//! A word or a qualifier the statement cannot place is an error, never
//! ignored.
//!
//! The single-table readers took the first word after `FROM` as the relation
//! and the next as its alias, and dropped everything up to the first clause
//! keyword they knew: `SELECT … FROM t WHER id = 1` returned every row, as did
//! `FROM t LIMT 5` and `FROM t ODER BY id`. And they read `q.col` as `col`
//! whatever `q` was, so `DELETE FROM t WHERE nosuch.id = 1` deleted t's row 1
//! and `WHERE nosuch.v > 0 OR id = 2` read t's own `v`. PostgreSQL refuses
//! all of them: 42601 for the word, 42P01 ("missing FROM-clause entry") for
//! the qualifier. Each is checked through the simple and the extended
//! protocol and inside a CTE, and the table is checked untouched.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn rows(pool: &sqlx::PgPool) -> Vec<(i32, i32)> {
    sqlx::query("SELECT id, v FROM t ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_words_and_qualifiers_are_refused() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("setup");
    let before = rows(&pool).await;

    let cases: &[(&str, &str)] = &[
        ("SELECT id FROM t WHER id = 1", "42601"),
        ("SELECT id FROM t x WHER x.id = 1", "42601"),
        ("SELECT id FROM t LIMT 5", "42601"),
        ("SELECT id FROM t ODER BY id", "42601"),
        ("SELECT id FROM t WHERE id = 1 ODER BY id", "42601"),
        ("SELECT id FROM t WHERE id = 1 ANDD v = 2", "42601"),
        ("SELECT id FROM t GROUPBY id", "42601"),
        ("SELECT id FROM t WHERE nosuch.id = 1", "42P01"),
        ("SELECT id FROM t WHERE nosuch.v > 0 OR id = 2", "42P01"),
        ("SELECT nosuch.id FROM t", "42P01"),
        ("SELECT id FROM t ORDER BY nosuch.id", "42P01"),
        ("SELECT id FROM t AS q WHERE t.id = 1", "42P01"),
        ("WITH c AS (SELECT id FROM t WHER id = 1) SELECT count(*) FROM c", "42601"),
        ("WITH c AS (SELECT id FROM t WHERE nosuch.id = 1) SELECT count(*) FROM c", "42P01"),
        ("DELETE FROM t WHER id = 1", "42601"),
        ("DELETE FROM t WHERE nosuch.id = 1", "42P01"),
        ("DELETE FROM t AS x WHERE t.id = 1", "42P01"),
        ("UPDATE t SET v = 0 WHERE nosuch.id = 2", "42P01"),
        ("UPDATE t SET v = nosuch.v WHERE id = 2", "42P01"),
    ];
    for (sql, want) in cases {
        let simple = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await;
        let extended = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await;
        for (protocol, result) in [("simple", simple), ("extended", extended)] {
            match result {
                Ok(rows) => panic!("{protocol}: {sql} answered {} rows", rows.len()),
                Err(e) => assert_eq!(code(&e), *want, "{protocol}: {sql}: {e}"),
            }
        }
    }
    assert_eq!(rows(&pool).await, before, "a refused statement changed a row");

    // What is right still runs: the table's name, its alias, quoted and
    // schema-qualified names, a subquery's own relation.
    for (sql, want) in [
        ("SELECT t.id FROM t WHERE t.v > 10 ORDER BY t.id", vec![2, 3]),
        ("SELECT x.id FROM t AS x WHERE x.v > 10 ORDER BY x.id", vec![2, 3]),
        ("SELECT x.id FROM t x WHERE x.v > 10 ORDER BY x.id", vec![2, 3]),
        (r#"SELECT "t"."id" FROM "t" WHERE "t"."v" > 10 ORDER BY "t"."id""#, vec![2, 3]),
        ("SELECT id FROM public.t WHERE t.id = 2", vec![2]),
        ("SELECT id FROM t WHERE id IN (SELECT o.id FROM t o WHERE o.v > 10) ORDER BY id", vec![2, 3]),
    ] {
        let got: Vec<i32> = sqlx::query_scalar(AssertSqlSafe(sql.to_string()))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got, want, "{sql}");
    }
    pool.execute("UPDATE t AS x SET v = x.v + 1 WHERE x.id = 2").await.expect("aliased update");
    pool.execute("DELETE FROM t AS x WHERE x.id = 3").await.expect("aliased delete");
    assert_eq!(rows(&pool).await, vec![(1, 10), (2, 21)]);
}
