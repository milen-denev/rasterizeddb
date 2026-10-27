//! `UPDATE … FROM`: an assignment's source references resolve to the source.
//!
//! An assigned expression was parsed against the target's schema and written
//! into each per-row `UPDATE` as text. A per-row `UPDATE` names one relation,
//! so `s.big` there read the target's own `big`: `SET v = s.big / 100.0`
//! stored `t.big / 100.0` (0.00) where PostgreSQL stores the source's value
//! (1.23), and said nothing.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

/// A file-backed server's root, removed on drop (after the server, which is
/// declared after it).
struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("update_from_source_refs_{}_{tag}", std::process::id()));
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

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0).unwrap_or_else(|| "NULL".into()))
        .collect()
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> u64 {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

/// The target `t` and the source `s` share the column names `big` and `x`;
/// `t`'s are zero and five, `s`'s are 123 and seven.
async fn seed(pool: &sqlx::PgPool, partitioning: &str) {
    run(
        pool,
        &format!(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, big BIGINT, x INT, v NUMERIC(12,2), w INT){partitioning}"
        ),
    )
    .await;
    run(pool, "CREATE TABLE s (id BIGINT PRIMARY KEY, big BIGINT, x INT)").await;
    run(pool, "INSERT INTO t VALUES (1, 0, 5, 0, 0), (2, 0, 5, 0, 0), (3, 0, 5, 0, 0)").await;
    run(pool, "INSERT INTO s VALUES (1, 123, 7), (2, 456, 7)").await;
}

async fn source_refs_resolve_to_the_source(partitioning: &str) {
    // File-backed: partitioned tables need it.
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    seed(&pool, partitioning).await;

    // A source column of the target's own name, alone in an expression.
    let n = run(&pool, "UPDATE t SET v = s.big / 100.0 FROM s WHERE t.id = s.id").await;
    assert_eq!(n, 2);
    assert_eq!(
        texts(&pool, "SELECT v::text FROM t ORDER BY id").await,
        ["1.23", "4.56", "0.00"],
        "{partitioning}: `s.big` read the target's big"
    );

    // Both relations' `x` in one expression, and a second assignment reading
    // the source alone.
    run(&pool, "UPDATE t SET w = t.x * 10 + s.x, v = s.x - t.x FROM s WHERE t.id = s.id").await;
    assert_eq!(
        texts(&pool, "SELECT w::text || ' ' || v::text FROM t ORDER BY id").await,
        ["57 2.00", "57 2.00", "0 0.00"],
        "{partitioning}: t.x and s.x were confused"
    );

    // Aliased: the alias is the qualifier, the name is not.
    run(&pool, "UPDATE t AS tt SET w = src.big + tt.x FROM s AS src WHERE tt.id = src.id AND src.big > 200").await;
    assert_eq!(
        texts(&pool, "SELECT w::text FROM t ORDER BY id").await,
        ["57", "461", "0"],
        "{partitioning}: an aliased source reference read the target"
    );

    // Unchanged: a target reference keeps reading the target row as written.
    run(&pool, "UPDATE t SET w = t.w + 1 FROM s WHERE t.id = s.id").await;
    assert_eq!(texts(&pool, "SELECT w::text FROM t ORDER BY id").await, ["58", "462", "0"]);
    assert_eq!(
        texts(&pool, "SELECT big::text || ' ' || x::text FROM t ORDER BY id").await,
        ["0 5", "0 5", "0 5"],
        "{partitioning}: the target's own columns changed"
    );

    // A NULL source value is NULL in the expression, not a zero.
    run(&pool, "INSERT INTO s VALUES (3, NULL, NULL)").await;
    run(&pool, "UPDATE t SET v = s.big / 100.0, w = COALESCE(s.x, -1) + t.x FROM s WHERE t.id = s.id AND s.id = 3").await;
    assert_eq!(
        texts(&pool, "SELECT COALESCE(v::text, 'NULL') || ' ' || w::text FROM t WHERE id = 3").await,
        ["NULL 4"],
        "{partitioning}: a NULL source value was read as a value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_references_resolve_to_the_source_on_a_plain_target() {
    source_refs_resolve_to_the_source("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_references_resolve_to_the_source_on_a_hash_partitioned_target() {
    source_refs_resolve_to_the_source(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
