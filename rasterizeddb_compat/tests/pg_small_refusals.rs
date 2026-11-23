//! Three small refusals PostgreSQL does not make.
//!
//! - A braced UUID, `'{6ba7b810-…}'`, which `uuid_in` accepts, was refused
//!   as input and as a comparison literal.
//! - A number on the left of column arithmetic, `12 = v * 1`, failed with
//!   "unexpected Op(*) after the end of the expression"; `v * 1 = 12` worked.
//! - A second constraint of a taken name answered 42P07 (duplicate_table);
//!   PostgreSQL's is 42710 (duplicate_object).

use rasterizeddb_compat::TestServer;
use sqlx::Executor;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_braced_uuid_is_one() {
    let (_server, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE uu (id UUID PRIMARY KEY, n BIGINT);
         INSERT INTO uu VALUES ('{6ba7b810-9dad-11d1-80b4-00c04fd430c8}', 1);",
    ))
    .await
    .unwrap();

    let id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM uu WHERE id = '{6ba7b810-9dad-11d1-80b4-00c04fd430c8}'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id.to_string(), "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    let n: i64 = sqlx::query_scalar("SELECT n FROM uu WHERE id = '{6BA7B810-9DAD-11D1-80B4-00C04FD430C8}'::uuid")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
    // An unbalanced brace is still refused.
    let e = sqlx::query("INSERT INTO uu VALUES ('{6ba7b810-9dad-11d1-80b4-00c04fd430c9', 2)")
        .execute(&pool)
        .await
        .expect_err("half a brace");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22P02"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_number_left_of_column_arithmetic_compares() {
    let (_server, pool) = server().await;
    pool.execute(sqlx::raw_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO t VALUES (1, 12), (2, 3);"))
        .await
        .unwrap();
    for (sql, want) in [
        ("SELECT id FROM t WHERE 12 = v * 1 ORDER BY id", vec![1i64]),
        ("SELECT id FROM t WHERE 12 <> v * 1 ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE 10 < v + 0 ORDER BY id", vec![1]),
        ("SELECT id FROM t WHERE 6 >= v * 2 ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE 10 > v + 0 ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE 12 <= v * 1 ORDER BY id", vec![1]),
        ("SELECT id FROM t WHERE 12 != v * 1 ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE 2 > v - 1 ORDER BY id", vec![]),
        ("SELECT id FROM t WHERE 3 > v - 1 ORDER BY id", vec![2]),
        ("SELECT id FROM t WHERE 12 = v * 1 AND 2 < v - 0 ORDER BY id", vec![1]),
    ] {
        let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql)).fetch_all(&pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(ids, want, "{sql}");
    }
    // A path into a document is not arithmetic: left as written, so the
    // refusal names the operands in their own order.
    pool.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, doc JSONB)").await.unwrap();
    let e = sqlx::query("SELECT id FROM d WHERE 1 = doc->'a'").fetch_all(&pool).await.expect_err("integer = jsonb");
    assert!(e.to_string().contains("integer = jsonb"), "{e}");
    let n = sqlx::query("UPDATE t SET v = 0 WHERE 3 = v * 1").execute(&pool).await.unwrap().rows_affected();
    assert_eq!(n, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_taken_constraint_name_is_duplicate_object() {
    let (_server, pool) = server().await;
    pool.execute("CREATE TABLE c (id BIGINT, a BIGINT, CONSTRAINT ck CHECK (a > 0))").await.unwrap();
    let e = sqlx::query("ALTER TABLE c ADD CONSTRAINT ck CHECK (a < 10)")
        .execute(&pool)
        .await
        .expect_err("a taken constraint name");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42710"), "{e}");
}
