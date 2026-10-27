//! What `UPDATE … FROM` and `DELETE … USING` write into each per-row
//! statement in place of a source reference.
//!
//! - The joined value was spliced in bare, beside whatever the text had
//!   there: `t.x-s.neg` with `-3` became `x--3`, `--` began a comment, and
//!   the rest of the statement (its other assignments and its `WHERE`) was
//!   dropped, so every row was updated. In a recheck it commented out a `)`.
//! - Only a bare `s.col` was a reference: `"s".big` and `s."big"` read the
//!   target's `big`.
//! - A reference inside a string literal was substituted, and a substituted
//!   string was scanned again.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("join_write_substitution_{}_{tag}", std::process::id()));
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

async fn run(pool: &sqlx::PgPool, sql: &str) -> u64 {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
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

async fn substitution(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let p = partitioning;
    run(
        &pool,
        &format!(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, f DOUBLE PRECISION, n NUMERIC(10,2), w BIGINT, \
             label TEXT, v BIGINT, big BIGINT, neg BIGINT, l TEXT){p}"
        ),
    )
    .await;
    run(
        &pool,
        "CREATE TABLE s (id BIGINT PRIMARY KEY, neg BIGINT, f DOUBLE PRECISION, n NUMERIC(10,2), l TEXT, x BIGINT, big BIGINT)",
    )
    .await;
    run(
        &pool,
        "INSERT INTO t VALUES (1, 5, 1.5, 2.25, 0, 'a', 10, 0, 0, 'tl'), (2, 5, 1.5, 2.25, 0, 'b', 10, 0, 0, 'tl'), (3, 5, 1.5, 2.25, 0, 'c', 10, 0, 0, 'tl')",
    )
    .await;
    run(
        &pool,
        "INSERT INTO s VALUES (1, -3, -0.5, -1.25, 'it''s -- /* s.x */ fine', 7, 123), (2, -3, -0.5, -1.25, 'q', 7, 456)",
    )
    .await;
    let all = |cols: &str| format!("SELECT {cols} FROM t ORDER BY id");

    // A negative after `-`, then another assignment and the WHERE. The
    // target has a `neg` of its own, so this is the per-row expression.
    let n = run(&pool, "UPDATE t SET w = t.x-s.neg, label = 'z' FROM s WHERE t.id = s.id AND s.id = 1").await;
    assert_eq!(n, 1, "{p}: the WHERE was commented out");
    assert_eq!(texts(&pool, &all("w::text || ' ' || label")).await, ["8 z", "0 b", "0 c"], "{p}");

    // Floats and numerics after `-`.
    run(&pool, "UPDATE t SET f = t.f-s.f, n = t.n-s.n FROM s WHERE t.id = s.id AND s.id = 1").await;
    assert_eq!(texts(&pool, &all("f::text || ' ' || n::text")).await, ["2 3.50", "1.5 2.25", "1.5 2.25"], "{p}");

    // A string holding `--`, `/*`, a quote and `s.x`, beside another
    // reference; and `s.x` inside a literal of the statement itself. The
    // target has `l` and `x` of its own, so this is the per-row expression.
    run(
        &pool,
        "UPDATE t SET label = 'see s.x: ' || s.l || ' / ' || CAST(s.x AS TEXT) FROM s WHERE t.id = s.id AND s.id = 1",
    )
    .await;
    assert_eq!(texts(&pool, "SELECT label FROM t WHERE id = 1").await, ["see s.x: it's -- /* s.x */ fine / 7"], "{p}");

    // Quoted qualifiers, where the target has a column of the same name.
    run(&pool, r#"UPDATE t SET v = "s".big + 1, w = s."big" - 1, x = "s"."big" * 2 FROM s WHERE t.id = s.id AND s.id = 2"#)
        .await;
    assert_eq!(texts(&pool, "SELECT v::text || ' ' || w::text || ' ' || x::text FROM t WHERE id = 2").await, ["457 455 912"], "{p}");

    // `-` before a negative in the recheck: id 1, 10 + 2 > 8 + 3; id 2,
    // 457 + 2 > 455 + 3; id 3 joins no source row.
    let n = run(&pool, "UPDATE t SET big = 1 FROM s WHERE t.id = s.id AND t.v + 2 > t.w-s.neg").await;
    assert_eq!(n, 2, "{p}: the recheck");
    assert_eq!(texts(&pool, &all("big::text")).await, ["1", "1", "0"], "{p}");
    let n = run(&pool, "DELETE FROM t USING s WHERE t.id = s.id AND t.v > t.x-s.neg").await;
    // id 1: 10 > 5 + 3 yes; id 2: 457 > 912 + 3 no.
    assert_eq!(n, 1, "{p}: DELETE … USING's recheck");
    assert_eq!(texts(&pool, &all("id::text")).await, ["2", "3"], "{p}");
}

#[tokio::test(flavor = "multi_thread")]
async fn source_values_are_substituted_whole_on_a_plain_target() {
    substitution("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn source_values_are_substituted_whole_on_a_hash_partitioned_target() {
    substitution(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
