//! A primary key's columns are NOT NULL, however the key is declared.
//!
//! `CREATE TABLE cpk (a INT, b INT, PRIMARY KEY (a, b))` left both columns
//! nullable, and `INSERT INTO cpk VALUES (NULL, 1)` was stored. PostgreSQL
//! refuses it with 23502.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn primary_key_columns_refuse_null() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for (table, ddl, nulls) in [
        ("cpk", "CREATE TABLE cpk (a INT, b INT, c INT, PRIMARY KEY (a, b))", vec!["(NULL, 1, 1)", "(1, NULL, 1)"]),
        ("cpk2", "CREATE TABLE cpk2 (PRIMARY KEY (b, a), a BIGINT, b TEXT, c INT)", vec!["(NULL, 'x', 1)", "(1, NULL, 1)"]),
        ("cpk3", "CREATE TABLE cpk3 (a INT, b INT, c INT, CONSTRAINT k PRIMARY KEY (a))", vec!["(NULL, 1, 1)"]),
    ] {
        pool.execute(AssertSqlSafe(ddl)).await.expect(ddl);
        for v in nulls {
            let sql = format!("INSERT INTO {table} (a, b, c) VALUES {v}");
            let e = pool.execute(AssertSqlSafe(sql.clone())).await.expect_err(&sql);
            assert_eq!(code(&e), "23502", "{sql}: {e}");
        }
        let ok = if table == "cpk2" { "(1, 'x', NULL)" } else { "(1, 1, NULL)" };
        pool.execute(AssertSqlSafe(format!("INSERT INTO {table} (a, b, c) VALUES {ok}"))).await.expect("non-key NULL");
        let n: i64 = sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(n, 1, "{table}: a row with a NULL key column was stored");
        // An UPDATE cannot set one to NULL either.
        let e = pool
            .execute(AssertSqlSafe(format!("UPDATE {table} SET a = NULL")))
            .await
            .expect_err("UPDATE to a NULL key column");
        assert_eq!(code(&e), "23502", "{table}: {e}");
        // The column a non-key NULL went to still takes one.
        let nulls: i64 = sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table} WHERE c IS NULL")))
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(nulls, 1);
    }
}
