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

/// `2147483647 + 1` overflows int4, PostgreSQL's 22003, which the evaluator
/// raises against an INT column. The decimal folder folded it into a value
/// first, and the comparison answered rows.
///
/// Against a BIGINT column the evaluator itself computes the literals at the
/// column's width and answers (PostgreSQL still raises): not the folder's,
/// and left as it was.
#[tokio::test(flavor = "multi_thread")]
async fn int4_overflow_in_a_where_literal_is_an_error() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE o (id BIGINT PRIMARY KEY, b BIGINT, i INT); INSERT INTO o VALUES (1, 4294967296, 7)").await;
    for sql in [
        "SELECT id FROM o WHERE i < 2147483647 + 1",
        "SELECT id FROM o WHERE id > 0 AND (i < 2147483647 + 1)",
        "SELECT id FROM o WHERE i < 0.5 + 2147483647 * 2",
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22003"), "{sql}: {e}");
    }
    // Integer division folds, truncating; int8 arithmetic does not overflow.
    assert_eq!(texts(&pool, "SELECT id::text FROM o WHERE b = 8589934592 / 2").await, ["1"]);
    assert_eq!(texts(&pool, "SELECT id::text FROM o WHERE id > 0 AND (i = 15 / 2)").await, ["1"]);
}

/// The shapes a parenthesised substitution broke: a value inside `IN (…)`,
/// `NOT IN`, `BETWEEN`, `LIKE`, `ILIKE` and `IS DISTINCT FROM`, which the
/// predicate parser takes bare and not parenthesised. `IN`, `NOT IN` and
/// `IS DISTINCT FROM` in UPDATE … FROM's recheck and DELETE … USING's; all
/// of them in a correlated subquery; positive and negative values; and `-`
/// before a negative. (A join condition `rt.v BETWEEN rs.lo AND rs.hi` or
/// `rt.name LIKE rs.name` is refused by the join itself, before any
/// substitution: "is not a comparison".)
async fn predicate_shapes(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "shapes_plain" } else { "shapes_hash" });
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let p = partitioning;
    run(
        &pool,
        &format!("CREATE TABLE rt (id BIGINT PRIMARY KEY, v BIGINT, name TEXT, lo BIGINT, hi BIGINT, flag BIGINT){p}"),
    )
    .await;
    run(
        &pool,
        "CREATE TABLE rs (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, lo BIGINT, hi BIGINT, name TEXT, neg BIGINT);
         CREATE TABLE c (cid BIGINT PRIMARY KEY, tid BIGINT, v BIGINT, name TEXT);",
    )
    .await;
    run(
        &pool,
        "INSERT INTO rt VALUES (1, 5, 'Apple', 0, 10, 0), (2, -3, 'apricot', -5, 0, 0), (3, 7, 'Banana', 6, 8, 0), \
         (4, 100, 'x', 0, 0, 0)",
    )
    .await;
    run(
        &pool,
        "INSERT INTO rs VALUES (1, 5, 6, 0, 10, 'Ap%', -3), (2, -3, 9, -5, -1, 'AP%', -3), (3, 1, 2, 6, 8, 'B%', -3), \
         (4, 1, 2, 0, 1, 'zz', -3)",
    )
    .await;
    run(&pool, "INSERT INTO c VALUES (1, 1, 5, 'Apple'), (3, 3, 7, 'Banana'), (5, -3, -3, 'neg')").await;
    let flagged = |pool: sqlx::PgPool| async move {
        texts(&pool, "SELECT id::text FROM rt WHERE flag = 1 ORDER BY id").await
    };
    for (cond, want) in [
        ("rt.v IN (rs.a, rs.b)", &["1", "2"][..]),
        ("rt.v NOT IN (rs.a)", &["3", "4"]),
        ("rt.v IN (rs.neg, 99)", &["2"]),
        ("rt.v IS DISTINCT FROM rs.a", &["3", "4"]),
        ("rt.v > rt.lo-rs.neg", &["1", "4"]),
        ("(SELECT COUNT(*) FROM c WHERE c.tid IN (rt.id, 99)) > 0", &["1", "3"]),
        ("(SELECT COUNT(*) FROM c WHERE c.tid IN (rt.v, 99)) > 0", &["2"]),
        ("(SELECT COUNT(*) FROM c WHERE c.v BETWEEN rt.lo AND rt.hi) > 0", &["1", "2", "3"]),
        ("(SELECT COUNT(*) FROM c WHERE c.name LIKE rt.name) > 0", &["1", "3"]),
        ("(SELECT COUNT(*) FROM c WHERE c.name ILIKE rt.name) > 0", &["1", "3"]),
        ("(SELECT COUNT(*) FROM c WHERE c.tid IS DISTINCT FROM rt.id AND c.tid NOT IN (rt.id)) > 2", &["2", "4"]),
    ] {
        run(&pool, "UPDATE rt SET flag = 0").await;
        let n = run(&pool, &format!("UPDATE rt SET flag = 1 FROM rs WHERE rt.id = rs.id AND {cond}")).await;
        assert_eq!(flagged(pool.clone()).await, want, "{p} UPDATE … FROM … AND {cond}");
        assert_eq!(n as usize, want.len(), "{p} UPDATE … FROM … AND {cond}: the count");
        // DELETE … USING, rolled back so every shape sees the same rows. It
        // takes no correlated subquery in its condition (refused before any
        // substitution).
        if cond.starts_with("(SELECT") {
            continue;
        }
        let mut c = pool.acquire().await.unwrap();
        c.execute("BEGIN").await.unwrap();
        let deleted: Vec<String> = sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM rt USING rs WHERE rt.id = rs.id AND {cond} RETURNING rt.id"
        )))
        .fetch_all(&mut *c)
        .await
        .unwrap_or_else(|e| panic!("{p} DELETE … USING … AND {cond}: {e}"))
        .iter()
        .map(|r| r.get::<i64, _>(0).to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
        assert_eq!(deleted, want, "{p} DELETE … USING … AND {cond}");
        c.execute("ROLLBACK").await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn substituted_values_work_in_every_predicate_shape_on_a_plain_target() {
    predicate_shapes("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn substituted_values_work_in_every_predicate_shape_on_a_hash_partitioned_target() {
    predicate_shapes(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
