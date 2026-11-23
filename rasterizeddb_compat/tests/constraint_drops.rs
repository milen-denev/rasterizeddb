//! Dropping a constraint, and refusing to drop the index one owns.
//!
//! `DROP INDEX t_pkey`, `DROP INDEX t_email_key` and `ALTER TABLE t DROP
//! CONSTRAINT t_pkey` all reported success and removed nothing: the index
//! answered to no handler and fell through to "an index this engine never
//! stored", and the primary key, kept in the schema header rather than a
//! constraint sidecar, matched no name. The client was told the key was gone
//! while it went on refusing rows.
//!
//! PostgreSQL refuses the first two (2BP01, "constraint … requires it") and
//! does the third: the key and its index go, the column keeps NOT NULL.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.map(|_| ())
}

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

async fn indexes(pool: &sqlx::PgPool, table: &str) -> Vec<String> {
    let mut v: Vec<String> = sqlx::query(AssertSqlSafe(format!(
        "SELECT indexname FROM pg_indexes WHERE tablename = '{table}'"
    )))
    .fetch_all(pool)
    .await
    .expect("pg_indexes")
    .iter()
    .map(|r| r.get::<String, _>(0))
    .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn a_constraint_index_is_refused_and_the_constraint_drops() {
    let root = format!("/var/tmp/constraint_drops_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    {
        let server = TestServer::start_file_backed(&root).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        run(&pool, "CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)").await.expect("create");
        run(&pool, "INSERT INTO t (id, email) VALUES (1, 'a')").await.expect("insert");
        assert_eq!(indexes(&pool, "t").await, vec!["t_email_key", "t_pkey"]);

        // The indexes a constraint owns are refused, as PostgreSQL refuses
        // them, and the constraints keep working.
        for name in ["t_pkey", "t_email_key"] {
            let e = run(&pool, &format!("DROP INDEX {name}")).await.expect_err("a constraint's index");
            assert_eq!(sqlstate(&e).as_deref(), Some("2BP01"), "{name}: {e}");
            assert!(e.to_string().contains("requires it"), "{e}");
        }
        let dup = run(&pool, "INSERT INTO t (id, email) VALUES (1, 'b')").await.expect_err("the key holds");
        assert_eq!(sqlstate(&dup).as_deref(), Some("23505"), "{dup}");
        assert_eq!(indexes(&pool, "t").await, vec!["t_email_key", "t_pkey"]);

        // The constraint itself drops, and takes its index with it.
        run(&pool, "ALTER TABLE t DROP CONSTRAINT t_pkey").await.expect("drop the key");
        assert_eq!(indexes(&pool, "t").await, vec!["t_email_key"]);
        run(&pool, "INSERT INTO t (id, email) VALUES (1, 'b')").await.expect("a duplicate id now goes in");
        // NOT NULL stays, as in PostgreSQL.
        let null = run(&pool, "INSERT INTO t (id, email) VALUES (NULL, 'c')").await.expect_err("NOT NULL holds");
        assert_eq!(sqlstate(&null).as_deref(), Some("23502"), "{null}");
        // The UNIQUE column was untouched by that.
        let dup_email = run(&pool, "INSERT INTO t (id, email) VALUES (2, 'a')").await.expect_err("email unique");
        assert_eq!(sqlstate(&dup_email).as_deref(), Some("23505"), "{dup_email}");
    }
    // And it stays dropped across a restart.
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(indexes(&pool, "t").await, vec!["t_email_key"]);
    run(&pool, "INSERT INTO t (id, email) VALUES (1, 'd')").await.expect("still no key after a restart");
}

/// An index nobody stored is still a no-op to drop: CREATE INDEX accepts
/// partial and expression indexes it does not build, and a migration that
/// drops one must not fail.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_index_is_still_a_no_op() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE u (id INT PRIMARY KEY, n INT)").await.expect("create");
    run(&pool, "CREATE INDEX u_expr ON u ((n + 1))").await.expect("accepted, not built");
    run(&pool, "DROP INDEX u_expr").await.expect("dropping it is a no-op");
    run(&pool, "DROP INDEX IF EXISTS never_was").await.expect("if exists");
}
