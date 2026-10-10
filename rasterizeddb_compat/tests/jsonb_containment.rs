//! `jsonb @>` and `<@` in `WHERE`, answered as PostgreSQL answers them.
//!
//! Two rules the engine got wrong, both silently: numbers compared as JSON
//! values, so `{"a":1.0}` did not contain `{"a":1}`; and a scalar on the right
//! was found anywhere, so `{"a":[1,2]}` contained `{"a":1}` and `[1,[2]]`
//! contained `[2]`. Each returned a different set of rows and no error.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn containment_filters_rows_as_postgres_does() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE d (id INT PRIMARY KEY, data JSONB)"))
        .await
        .expect("create");
    for (id, doc) in [
        (1, r#"{"a": 1.0}"#),
        (2, r#"{"a": [1, 2]}"#),
        (3, r#"[1, [2]]"#),
        (4, r#"[1, 2]"#),
        (5, r#"{"a": 1, "b": {"c": 2.50}}"#),
        (6, r#"{"a": "1"}"#),
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO d (id, data) VALUES ({}, '{}')",
            id, doc
        ))))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }

    let mut wrong = Vec::new();
    for (predicate, expected) in [
        (r#"data @> '{"a": 1}'"#, vec![1, 5]),
        (r#"data @> '{"a": 1.00}'"#, vec![1, 5]),
        (r#"data @> '{"a": [1]}'"#, vec![2]),
        (r#"data @> '[2]'"#, vec![4]),
        (r#"data @> '2'"#, vec![4]),
        (r#"data @> '[[2]]'"#, vec![3]),
        (r#"data @> '{"b": {"c": 2.5}}'"#, vec![5]),
        (r#"data @> '{"a": "1"}'"#, vec![6]),
        (r#"data <@ '{"a": 1, "b": {"c": 2.5, "d": 0}, "z": 0}'"#, vec![1, 5]),
    ] {
        let sql = format!("SELECT id FROM d WHERE {} ORDER BY id", predicate);
        match sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&pool).await {
            Ok(rows) => {
                let got: Vec<i32> = rows.iter().map(|r| r.get::<i32, _>(0)).collect();
                if got != expected {
                    wrong.push(format!("{}: got {:?}, want {:?}", predicate, got, expected));
                }
            }
            Err(e) => wrong.push(format!("{}: error {}", predicate, e)),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
