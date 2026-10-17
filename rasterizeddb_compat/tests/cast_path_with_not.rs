//! A cast JSON path in a clause that also holds a `NOT` kept its cast.
//!
//! The `NOT` rewrite renders the clause back to text, and that text was lexed
//! again with the cast now after the key literal: `doc ->> 'tier' ::int`. A
//! cast on a literal is dropped by design, so the path was compared as text
//! against a number and matched nothing. Every such query returned zero rows
//! and no error. The answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn a_cast_json_path_and_a_not_find_their_rows() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE jn (id INT NOT NULL, qty INT NOT NULL, active BOOLEAN NOT NULL, doc JSONB NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        r#"INSERT INTO jn (id, qty, active, doc) VALUES
           (1, 5, true,  '{"tier": 2, "ok": false}'),
           (2, 6, true,  '{"tier": 2, "ok": false}'),
           (3, 7, false, '{"tier": 2, "ok": true}'),
           (4, 8, true,  '{"tier": 3, "ok": false}')"#,
    ))
    .await
    .expect("seed");
    let mut wrong = Vec::new();
    for (clause, want) in [
        ("(doc->>'tier')::int = 2", 3i64),
        ("(doc->>'tier')::int = 2 AND NOT (qty = 5)", 2),
        ("(doc->>'tier')::int = 2 AND NOT (qty = 5 OR qty = 6)", 1),
        ("(doc->>'tier')::int = 2 AND NOT active", 1),
        ("NOT (qty = 5) AND (doc->>'tier')::int = 2", 2),
        ("(doc->>'ok')::boolean = false AND NOT (qty = 5)", 2),
        ("NOT ((doc->>'tier')::int = 2)", 1),
        ("(doc->>'tier')::int BETWEEN 2 AND 9 AND NOT (qty > 7 OR qty < 6)", 2),
        ("(doc->>'tier')::int = 2 AND qty != 5", 2),
        ("doc->>'tier' = '2' AND NOT (qty = 5)", 2),
    ] {
        let sql = format!("SELECT count(*) AS n FROM jn WHERE {clause}");
        let got: i64 = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get("n");
        if got != want {
            wrong.push(format!("{clause}: got {got}, PostgreSQL {want}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
