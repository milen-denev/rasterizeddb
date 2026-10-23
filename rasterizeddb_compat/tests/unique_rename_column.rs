//! A unique constraint survives `ALTER TABLE … RENAME COLUMN` of a column it
//! names, either as a key column or in a partial index's predicate.
//!
//! PostgreSQL keys constraints by column number, so a rename changes nothing
//! about what they enforce. Every expected answer here is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str) {
    match sqlx::query(AssertSqlSafe(sql)).execute(pool).await {
        Ok(_) => panic!("{sql}: PostgreSQL rejects this with 23505, but it succeeded"),
        Err(sqlx::Error::Database(d)) => {
            assert_eq!(d.code().as_deref(), Some("23505"), "{sql}: {d}")
        }
        Err(e) => panic!("{sql}: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_unique_column_keeps_its_constraint_through_a_rename() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE u (id INT, email TEXT UNIQUE)").await;
    exec(&pool, "INSERT INTO u VALUES (1, 'a')").await;
    exec(&pool, "ALTER TABLE u RENAME COLUMN email TO mail").await;
    refused(&pool, "INSERT INTO u VALUES (2, 'a')").await;
    exec(&pool, "INSERT INTO u VALUES (3, 'b')").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_constraint_and_a_partial_index_keep_theirs() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE m (org INT, slug TEXT, deleted_at TEXT, UNIQUE (org, slug))").await;
    exec(&pool, "CREATE TABLE p (id INT, email TEXT, deleted_at TEXT)").await;
    exec(&pool, "CREATE UNIQUE INDEX p_live ON p (email) WHERE deleted_at IS NULL").await;
    exec(&pool, "INSERT INTO m VALUES (1, 'x', NULL)").await;
    exec(&pool, "INSERT INTO p VALUES (1, 'a', NULL)").await;

    // A key column of a composite constraint.
    exec(&pool, "ALTER TABLE m RENAME COLUMN slug TO handle").await;
    refused(&pool, "INSERT INTO m VALUES (1, 'x', NULL)").await;
    exec(&pool, "INSERT INTO m VALUES (2, 'x', NULL)").await;

    // The key column of a partial index, then the column its predicate reads.
    exec(&pool, "ALTER TABLE p RENAME COLUMN email TO mail").await;
    refused(&pool, "INSERT INTO p VALUES (2, 'a', NULL)").await;
    exec(&pool, "ALTER TABLE p RENAME COLUMN deleted_at TO gone_at").await;
    refused(&pool, "INSERT INTO p VALUES (3, 'a', NULL)").await;
    // Outside the predicate the key is free, as before the renames.
    exec(&pool, "INSERT INTO p VALUES (4, 'a', 'yesterday')").await;
}
