//! Two answers a client reads before it reads any rows.
//!
//! A table whose only uniqueness is composite has its index from the moment
//! it is created: pg_indexes listed none until a restart, a single-column key
//! was listed at once. And a subquery's failure keeps its own SQLSTATE: a
//! refusal inside `IN (…)` reached the client as 42601 instead of 0A000.
//! Expected answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_is_indexed_from_creation() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE u (a INT NOT NULL, b INT NOT NULL, UNIQUE (a, b));
         CREATE TABLE p (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b));",
    ))
    .await
    .expect("schema");
    for (table, want) in [("u", "u_a_b_key"), ("p", "p_pkey")] {
        let names: Vec<String> = sqlx::query("SELECT indexname FROM pg_indexes WHERE tablename = $1")
            .bind(table)
            .fetch_all(&pool)
            .await
            .expect("pg_indexes")
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(names.len(), 1, "{table}: {names:?}");
        assert_eq!(names[0], want, "{table}");
    }
    // And it is enforced, as before.
    pool.execute("INSERT INTO u VALUES (1, 2)").await.expect("first");
    let e = pool.execute("INSERT INTO u VALUES (1, 2)").await.expect_err("duplicate");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23505"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subquery_failure_keeps_its_own_code() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE t (id INT); CREATE TABLE s (id INT);"))
        .await
        .expect("schema");
    let e = pool
        .execute("SELECT id FROM t WHERE id IN (SELECT id FROM s FOR UPDATE NOWAIT SKIP LOCKED)")
        .await
        .expect_err("an unsupported locking clause");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("0A000"), "{e}");
    let e = pool
        .execute("SELECT id FROM t WHERE id IN (SELECT id FROM no_such_table)")
        .await
        .expect_err("a missing relation");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42P01"), "{e}");
}
