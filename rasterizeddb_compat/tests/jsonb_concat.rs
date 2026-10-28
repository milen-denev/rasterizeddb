//! `||` over `jsonb`, PostgreSQL's way: a document beside a document, or
//! beside a written string (an `unknown` literal, which takes the document's
//! type), is `jsonb` concatenation.
//!
//! It was text concatenation of the document's encoded bytes: `SET doc =
//! doc || '{"b":1}'` reported success and stored the old document with text
//! trailing it, which read back unchanged, and `SET doc = '{"a":1}' ||
//! '{"b":1}'` stored text no reader could open ("JSONB cannot be rendered as
//! text", or NULL on the wire). Every value here is read back, and again after
//! a restart.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("jsonb_concat_{}_{tag}", std::process::id()));
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

async fn run(pool: &sqlx::PgPool, sql: &str) -> u64 {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

/// Each row's document and text, as the client is shown them.
async fn rows(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query("SELECT id, doc::text AS d, t FROM jc ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read back")
        .iter()
        .map(|r| {
            format!(
                "{} {} {}",
                r.get::<i64, _>("id"),
                r.get::<Option<String>, _>("d").unwrap_or_else(|| "NULL".into()),
                r.get::<Option<String>, _>("t").unwrap_or_else(|| "NULL".into())
            )
        })
        .collect()
}

async fn concatenation(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let want = [
        r#"1 {"a": 1, "b": 1.000000000000000000001, "z": 1} {"a": 1, "b": 1.000000000000000000001}"#,
        r#"2 [1, 2, 3, {"k": 1}] [1]"#,
        r#"3 {"q": 0, "s": [true]} NULL"#,
        r#"4 [{"o": 1}, 5] NULL"#,
    ];
    {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        let p = partitioning;
        run(&pool, &format!("CREATE TABLE jc (id BIGINT PRIMARY KEY, doc JSONB, t TEXT, n BIGINT){p}")).await;
        run(&pool, "CREATE TABLE js (id BIGINT PRIMARY KEY, d JSONB)").await;
        run(
            &pool,
            r#"INSERT INTO jc VALUES (1, '{"a":1}', NULL, 1), (2, '[1]', NULL, 2), (3, '{"q":0}', NULL, 3), (4, '{"o":1}', NULL, 4)"#,
        )
        .await;
        run(&pool, r#"INSERT INTO js VALUES (1, '{"s":[true]}')"#).await;

        // An object merges, the right side winning; a number keeps its digits.
        assert_eq!(run(&pool, r#"UPDATE jc SET doc = doc || '{"b":1.000000000000000000001}' WHERE id = 1"#).await, 1);
        // The literal on the left, and a document assigned to text as its JSON
        // (the row as it was before this statement).
        assert_eq!(run(&pool, r#"UPDATE jc SET doc = '{"z":1}' || doc, t = doc::text WHERE id = 1"#).await, 1);
        // Arrays concatenate; a scalar or an object is appended as an element.
        assert_eq!(run(&pool, "UPDATE jc SET doc = doc || '[2]', t = doc WHERE id = 2").await, 1);
        assert_eq!(run(&pool, "UPDATE jc SET doc = doc || '3' WHERE id = 2").await, 1);
        assert_eq!(run(&pool, r#"UPDATE jc SET doc = doc || '{"k":1}' WHERE id = 2"#).await, 1);
        // An object and a scalar make a two-element array.
        assert_eq!(run(&pool, "UPDATE jc SET doc = doc || '5' WHERE id = 4").await, 1);
        // A document from a joined source, and a literal through UPDATE … FROM,
        // where the right side's `q` wins.
        assert_eq!(run(&pool, "UPDATE jc SET doc = jc.doc || js.d FROM js WHERE jc.id = 3 AND js.id = 1").await, 1);
        assert_eq!(run(&pool, r#"UPDATE jc SET doc = '{"q":1}' || jc.doc FROM js WHERE jc.id = 3 AND js.id = 1"#).await, 1);
        assert_eq!(rows(&pool).await, want, "{p}");

        // The same, computed in a SELECT and described as jsonb.
        let r = sqlx::query(r#"SELECT doc || '{"x":0}' AS d FROM jc WHERE id = 3"#).fetch_one(&pool).await.unwrap();
        assert_eq!(r.get::<serde_json::Value, _>("d"), serde_json::json!({"q": 0, "s": [true], "x": 0}), "{p}");

        // Never stored: text that is not a document, a string concatenation
        // of two literals that is not one, and a number.
        for (sql, want_code) in [
            ("UPDATE jc SET doc = doc || 'abc' WHERE id = 1", "22P02"),
            (r#"UPDATE jc SET doc = '{"a":1}' || '{"b":1}' WHERE id = 1"#, "22P02"),
            (r#"INSERT INTO jc VALUES (9, '{"a":1}' || '{"b":1}', NULL, 9)"#, "22P02"),
            ("UPDATE jc SET doc = n + 1 WHERE id = 1", "42804"),
            ("UPDATE jc SET doc = jc.doc || 'abc' FROM js WHERE jc.id = 1 AND js.id = 1", "22P02"),
        ] {
            let e = sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect_err(sql);
            assert_eq!(code(&e), want_code, "{p}: {sql}: {e}");
        }
        assert_eq!(rows(&pool).await, want, "{p}: a refused write changed a row");
    }
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(rows(&pool).await, want, "{partitioning}: after a restart");
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonb_concatenation_on_a_plain_table() {
    concatenation("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonb_concatenation_on_a_hash_partitioned_table() {
    concatenation(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
