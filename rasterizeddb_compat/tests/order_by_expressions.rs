//! `ORDER BY` an expression written without parentheses.
//!
//! `ORDER BY n * 2 + 1` was split at its first space and refused as
//! "ORDER BY 'n' expects ASC or DESC, found '*'", while `ORDER BY (n * 2 + 1)`
//! sorted. Found by rasterizeddb-eb's partitioning twin. Expected orders are
//! PostgreSQL 17's: NULLs last ascending, first descending.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn an_unparenthesised_expression_sorts() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT NOT NULL, n INT);
         INSERT INTO t VALUES (1, 3), (2, 1), (3, 2), (4, NULL), (5, 5);",
    ))
    .await
    .expect("schema");
    let ids = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| r.get::<i32, _>(0))
                .collect::<Vec<i32>>()
        }
    };
    assert_eq!(ids("SELECT id FROM t ORDER BY n * 2 + 1").await, vec![2, 3, 1, 5, 4]);
    assert_eq!(ids("SELECT id FROM t ORDER BY n * 2 + 1 DESC").await, vec![4, 5, 1, 3, 2]);
    assert_eq!(ids("SELECT id FROM t ORDER BY n * 2 + 1 DESC NULLS LAST").await, vec![5, 1, 3, 2, 4]);
    assert_eq!(ids("SELECT id FROM t ORDER BY 10 - n, id").await, vec![5, 1, 3, 2, 4]);
}

/// A function call in `WHERE` is evaluated, as in the select list:
/// `COALESCE(s, 'z') = 'z'` was refused as an unknown column `COALESCE`.
#[tokio::test(flavor = "multi_thread")]
async fn a_function_call_in_where_is_evaluated() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT NOT NULL, s TEXT, n INT);
         INSERT INTO t VALUES (1, 'a', 3), (2, 'B', 1), (3, 'a', -2), (4, NULL, NULL), (5, 'c', 5);",
    ))
    .await
    .expect("schema");
    let cases: [(&str, Vec<i32>); 7] = [
        ("SELECT id FROM t WHERE COALESCE(s, 'z') = 'z' ORDER BY id", vec![4]),
        ("SELECT id FROM t WHERE UPPER(s) = 'A' ORDER BY id", vec![1, 3]),
        ("SELECT id FROM t WHERE lower(s) = 'b' ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE ABS(n) = 2 ORDER BY id", vec![3]),
        ("SELECT id FROM t WHERE NOT (UPPER(s) = 'A') ORDER BY id", vec![2, 5]),
        ("SELECT id FROM t WHERE LENGTH(s) = 1 AND n > 2 ORDER BY id", vec![1, 5]),
        ("SELECT id FROM t WHERE COALESCE(n, 0) IN (0, 5) ORDER BY id", vec![4, 5]),
    ];
    for (sql, want) in cases {
        let got: Vec<i32> = sqlx::query(sql)
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}
