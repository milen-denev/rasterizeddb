//! Branches of `CASE`, `COALESCE`, `GREATEST` and `LEAST` that PostgreSQL
//! cannot reconcile are refused at plan time with SQLSTATE 42804 ("CASE types
//! integer and text cannot be matched"). They used to resolve to the first
//! branch's type and send the other branches' bytes under it.
//!
//! An untyped quoted literal adopts the other branches' type, so those shapes
//! stay valid — and are checked here alongside the refusals.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_branch_types_are_refused_with_42804() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE m (id INT NOT NULL, i INT, s TEXT, b BOOLEAN)")
        .await
        .unwrap();
    pool.execute("INSERT INTO m (id, i, s, b) VALUES (1, 5, 'x', true), (2, NULL, NULL, NULL)")
        .await
        .unwrap();

    for expr in [
        "CASE WHEN id = 1 THEN i ELSE s END",
        "CASE WHEN id = 1 THEN 1 ELSE s END",
        "CASE WHEN id = 1 THEN s ELSE 1 END",
        "CASE WHEN id = 1 THEN b ELSE 1 END",
        "CASE WHEN id = 1 THEN 1.5 ELSE s END",
        "COALESCE(i, s)",
        "COALESCE(s, 0)",
        "GREATEST(i, s)",
        "LEAST(b, i)",
    ] {
        let sql = format!("SELECT {expr} AS v FROM m");
        match sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await {
            Ok(rows) => panic!("{expr} answered {} rows instead of refusing", rows.len()),
            Err(sqlx::Error::Database(e)) => {
                assert_eq!(e.code().as_deref(), Some("42804"), "{expr}: {}", e.message());
                assert!(e.message().contains("cannot be matched"), "{expr}: {}", e.message());
            }
            Err(e) => panic!("{expr}: {e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_literal_adopts_the_other_branches_type() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE m (id INT NOT NULL, i INT, s TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO m (id, i, s) VALUES (1, 5, 'x'), (2, NULL, NULL)")
        .await
        .unwrap();
    let text = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            let mut v: Vec<Option<String>> = sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| r.get::<Option<String>, _>(0))
                .collect();
            v.sort();
            v
        }
    };
    assert_eq!(
        text("SELECT CASE WHEN id = 1 THEN 'a' ELSE 1::text END FROM m").await,
        vec![Some("1".to_string()), Some("a".to_string())]
    );
    assert_eq!(
        text("SELECT COALESCE(s, 'none') FROM m").await,
        vec![Some("none".to_string()), Some("x".to_string())]
    );
    let mut ints: Vec<Option<i32>> = sqlx::query("SELECT CASE WHEN id = 1 THEN i ELSE '7' END FROM m")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<Option<i32>, _>(0))
        .collect();
    ints.sort();
    assert_eq!(ints, vec![Some(5), Some(7)]);
    let mut ints: Vec<Option<i32>> = sqlx::query("SELECT COALESCE(i, '0') FROM m")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<Option<i32>, _>(0))
        .collect();
    ints.sort();
    assert_eq!(ints, vec![Some(0), Some(5)]);
}
