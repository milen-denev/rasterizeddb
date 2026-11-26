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

/// A grouped query sorted by an aggregate, or a grouping key, that its
/// select list does not hold, as PostgreSQL sorts one: computed beside the
/// items over a derived table (`distinct_on::rewrite_grouped_order`). It was
/// refused, "ORDER BY term 'count(*)' is not in the SELECT list" (42601).
#[tokio::test(flavor = "multi_thread")]
async fn a_grouped_query_sorts_by_what_it_does_not_select() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, k TEXT NOT NULL, n INT);
         CREATE TABLE gl (k TEXT NOT NULL PRIMARY KEY, label TEXT);
         INSERT INTO g VALUES (1, 'a', 5), (2, 'b', 1), (3, 'b', 2), (4, 'c', 9), (5, 'b', NULL), (6, 'a', 1);
         INSERT INTO gl VALUES ('a', 'Alpha'), ('b', 'Beta'), ('c', 'Gamma');",
    ))
    .await
    .expect("schema");
    let rows = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::raw_sql(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| (0..r.len()).map(|i| r.try_get_unchecked::<Option<String>, _>(i).unwrap().unwrap_or_else(|| "NULL".into())).collect::<Vec<_>>().join("|"))
                .collect::<Vec<String>>()
        }
    };
    assert_eq!(rows("SELECT k FROM g GROUP BY k ORDER BY count(*) DESC").await, ["b", "a", "c"]);
    assert_eq!(rows("SELECT k FROM g GROUP BY k ORDER BY count(*) DESC LIMIT 1").await, ["b"]);
    assert_eq!(rows("SELECT k FROM g GROUP BY k ORDER BY sum(n) DESC NULLS LAST, k LIMIT 2 OFFSET 1").await, ["a", "b"]);
    assert_eq!(rows("SELECT k, count(*) AS c FROM g GROUP BY k ORDER BY max(id)").await, ["c|1", "b|3", "a|2"]);
    assert_eq!(rows("SELECT count(*) FROM g GROUP BY k ORDER BY k DESC").await, ["1", "3", "2"]);
    assert_eq!(rows("SELECT upper(k) AS u FROM g GROUP BY k HAVING count(*) > 1 ORDER BY min(n) NULLS FIRST, u").await, ["A", "B"]);
    assert_eq!(
        rows("SELECT gl.label FROM g JOIN gl ON g.k = gl.k GROUP BY gl.label ORDER BY count(*) DESC, gl.label").await,
        ["Beta", "Alpha", "Gamma"]
    );
    assert_eq!(rows("WITH w AS (SELECT k, n FROM g WHERE n IS NOT NULL) SELECT k FROM w GROUP BY k ORDER BY sum(n)").await, ["b", "a", "c"]);
    // One whose every sort term is an item is answered as it was.
    assert_eq!(rows("SELECT k, count(*) FROM g GROUP BY k ORDER BY count(*) DESC, k").await, ["b|3", "a|2", "c|1"]);
    // And so is one sorted by an aggregate the list has under a cast alone
    // (the Rust ORM's `CAST(SUM(x) AS BIGINT)` selected, `SUM(x)` sorted
    // by, with a HAVING): left as written, no derived table.
    assert_eq!(
        rows("SELECT k, CAST(SUM(n) AS BIGINT) FROM g GROUP BY k HAVING COUNT(*) >= 2 ORDER BY SUM(n) DESC").await,
        ["a|6", "b|3"]
    );
    assert_eq!(rows("SELECT k, sum(n)::bigint AS s FROM g GROUP BY k ORDER BY sum(n) DESC NULLS LAST, k").await, ["c|9", "a|6", "b|3"]);
}

