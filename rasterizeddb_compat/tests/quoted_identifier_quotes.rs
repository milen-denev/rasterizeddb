//! A quoted identifier holding a double quote (`"a""b"`, the name `a"b`).
//!
//! Every identifier reader in the engine dropped the name at the first inner
//! quote: `CREATE TABLE "t"" x ""y" (…)` reported success and created a table
//! named `t`, and a column so named failed with an unrelated type error. Until
//! the readers share one that reads `""` as a quote, such a statement is
//! refused (0A000) before anything is created, renamed or dropped under a
//! different name. A doubled quote inside a string is untouched.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

struct Root(String);

impl Root {
    fn new() -> Self {
        let r = std::env::temp_dir().join(format!("quoted_identifier_quotes_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn tables(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT relname::text FROM pg_class WHERE relname IN ('t', 'q', 'y', 't\" x \"y', 'keep') ORDER BY 1")
        .fetch_all(pool)
        .await
        .expect("catalogue")
}

#[tokio::test(flavor = "multi_thread")]
async fn an_identifier_holding_a_quote_is_refused_not_truncated() {
    let dir = Root::new();
    {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        pool.execute(sqlx::raw_sql(
            "CREATE TABLE keep (id BIGINT PRIMARY KEY, t TEXT); INSERT INTO keep VALUES (1, 'say \"\"hi\"\"');",
        ))
        .await
        .unwrap();
        for sql in [
            r#"CREATE TABLE "t"" x ""y" (id BIGINT PRIMARY KEY, v BIGINT)"#,
            r#"CREATE TABLE q (id BIGINT PRIMARY KEY, "a""b" TEXT)"#,
            r#"INSERT INTO "t"" x ""y" VALUES (1, 1)"#,
            r#"INSERT INTO keep (id, "t""") VALUES (2, 'x')"#,
            r#"SELECT "t""" FROM keep"#,
            r#"SELECT id AS "a""b" FROM keep"#,
            r#"UPDATE keep SET "t""" = 'x'"#,
            r#"ALTER TABLE keep ADD COLUMN "c""d" BIGINT"#,
            r#"ALTER TABLE keep RENAME COLUMN t TO "e""f""#,
            r#"ALTER TABLE keep ADD CONSTRAINT "k""1" UNIQUE (t)"#,
            r#"CREATE INDEX "i""x" ON keep (t)"#,
            r#"DROP TABLE "t"" x ""y""#,
            r#"DROP TABLE "keep""""#,
        ] {
            let e = sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect_err(sql);
            assert_eq!(code(&e), "0A000", "{sql}: {e}");
        }
        assert_eq!(tables(&pool).await, ["keep"]);
        // A doubled quote in a string is a string's business.
        let t: String = sqlx::query_scalar(r#"SELECT t FROM "keep" WHERE 'a""b' = 'a""b'"#).fetch_one(&pool).await.unwrap();
        assert_eq!(t, r#"say ""hi"""#);
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT column_name::text FROM information_schema.columns WHERE table_name = 'keep' ORDER BY 1")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(columns, ["id", "t"]);
    }
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(tables(&pool).await, ["keep"]);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM keep").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 1);
}
