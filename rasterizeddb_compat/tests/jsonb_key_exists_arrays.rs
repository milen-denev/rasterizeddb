//! `?`, `?|` and `?&` with PostgreSQL's semantics.
//!
//! `?` was rewritten to `data->'k' <> ''`, which looks up an object key and so
//! answered `false` for a string element of a top-level array — PostgreSQL's
//! `'["a","b"]' ? 'a'` is true. `?|` and `?&` were refused by name. Expected
//! rows are worked out from PostgreSQL's rules, not from this engine.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn ids(pool: &sqlx::PgPool, predicate: &str) -> Result<Vec<i32>, sqlx::Error> {
    let mut out: Vec<i32> = sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM j WHERE {predicate}"
    )))
    .fetch_all(pool)
    .await?
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect();
    out.sort();
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn key_existence_matches_postgres() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE j (id INT PRIMARY KEY, data JSONB)")
        .await
        .unwrap();
    for (id, doc) in [
        (1, r#"'{"a": 1, "b": null}'"#),
        (2, r#"'["a", "b"]'"#),
        // "a" only nested: not a top-level element.
        (3, r#"'["x", ["a"], {"a": 1}]'"#),
        // A bare string is its own one-element pseudo-array.
        (4, r#"'"a"'"#),
        // Numbers are not keys.
        (5, r#"'[1, 2]'"#),
        (6, "NULL"),
        (7, r#"'{"c": {"a": 1}}'"#),
        (8, r#"'{"1": true}'"#),
        (9, r#"'{"it''s": 1}'"#),
    ] {
        let sql = format!("INSERT INTO j (id, data) VALUES ({id}, {doc})");
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.clone())))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("data ? 'a'", vec![1, 2, 4]),
        ("data ? 'b'", vec![1, 2]),
        ("data ? '1'", vec![8]),
        ("data ? 'zz'", vec![]),
        ("data ? 'it''s'", vec![9]),
        ("data ? 'a' AND id > 1", vec![2, 4]),
        ("data ? 'zz' OR id = 5", vec![5]),
        ("data ?| array['a', 'zz']", vec![1, 2, 4]),
        ("data ?| ARRAY['zz']", vec![]),
        ("data ?| '{b,c}'", vec![1, 2, 7]),
        ("data ?| array['it''s']", vec![9]),
        ("data ?| array['b']::text[]", vec![1, 2]),
        ("data ?& array['a', 'b']", vec![1, 2]),
        ("data ?& '{a}'", vec![1, 2, 4]),
        ("data ?& array['a', 'zz']", vec![]),
        // An empty list: `?|` holds for nothing, `?&` for every document.
        ("data ?| '{}'", vec![]),
        ("data ?& '{}'", vec![1, 2, 3, 4, 5, 7, 8, 9]),
        // NULL keys are skipped by both.
        ("data ?& array['a', NULL]", vec![1, 2, 4]),
        ("data ?| array[NULL, 'b']", vec![1, 2]),
    ] {
        match ids(&pool, predicate).await {
            Ok(got) if got == expected => {}
            Ok(got) => wrong.push(format!("{predicate}: got {got:?}, want {expected:?}")),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
