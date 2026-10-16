//! JSONB predicates over NULL documents, missing paths and bad literals,
//! against PostgreSQL's answers.
//!
//! PostgreSQL: `NULL @> x` is NULL, and so is `doc->'missing' @> x`; a WHERE
//! clause drops the row either way, and the other rows come back. Here both
//! used to fail the whole statement with "operation is not supported for this
//! column type", because the NULL's empty bytes reached the containment walk
//! and every comparison runs before the `AND` that guards it is applied. And a
//! right operand that is not JSON was kept as raw text, so its answer depended
//! on the row, where PostgreSQL refuses it up front with 22P02.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE d (id INT PRIMARY KEY, doc JSONB)"))
        .await
        .expect("create");
    for (id, doc) in [
        (1, r#"'{"a":1,"tags":["x","y"],"s":"str","n":5,"z":null,"o":{"k":1}}'"#),
        (2, "NULL"),
        (3, r#"'{"a":2}'"#),
        (4, "'[1,2]'"),
        (5, r#"'{"tags":["y"],"s":""}'"#),
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO d (id, doc) VALUES ({}, {})",
            id, doc
        ))))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<String, String> {
    match sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.get::<i32, _>("id").to_string())
            .collect::<Vec<_>>()
            .join(",")),
        Err(e) => Err(e.to_string()),
    }
}

/// Every answer is PostgreSQL 16's for the same table.
#[tokio::test]
async fn null_documents_and_missing_paths_match_nothing() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (clause, want) in [
        (r#"doc @> '{"a":1}'"#, "1"),
        (r#"doc @> '{"a":1}' OR id = 3"#, "1,3"),
        (r#"id = 2 OR doc @> '{"a":2}'"#, "2,3"),
        (r#"doc <@ '{"a":2,"b":3}'"#, "3"),
        (r#"doc @> '[1]'"#, "4"),
        (r#"doc->'tags' @> '["x"]'"#, "1"),
        (r#"doc->'tags' @> '["y"]' OR id = 3"#, "1,3,5"),
        (r#"doc->'tags' <@ '["x","y","q"]'"#, "1,5"),
        (r#"doc->'o'->'k' @> '1'"#, "1"),
        ("doc ? 'a'", "1,3"),
        ("doc ?| '{a,tags}'", "1,3,5"),
        ("doc ?| array['q','tags']", "1,5"),
        ("doc ?| '{q,r}'", ""),
        ("doc ?& '{a,tags}'", "1"),
        ("doc ?& array['tags']", "1,5"),
        ("doc ?& '{}'", "1,3,4,5"),
        ("doc->'o' ?| '{k,q}'", "1"),
        ("doc->'o' ?& '{k,q}'", ""),
        // `->>` on each kind of leaf.
        ("doc->>'s' = 'str'", "1"),
        ("doc->>'n' = '5'", "1"),
        ("doc->>'a' = '2'", "3"),
        // JSON null under `->>` is SQL NULL, so it equals nothing.
        ("doc->>'z' = 'null'", ""),
    ] {
        let sql = format!("SELECT id FROM d WHERE {} ORDER BY id", clause);
        let got = ids(&pool, &sql).await;
        if got != Ok(want.to_string()) {
            wrong.push(format!("{}: got {:?}, want {:?}", clause, got, want));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// PostgreSQL returns no row for `NOT (doc @> x)` when `doc` is NULL. Here the
/// form is refused outright rather than planned, which keeps a NULL from ever
/// turning into a match; this pins that, so whoever teaches the rewrite to
/// complement `@>` sees the NULL row has to stay out.
#[tokio::test]
async fn not_around_containment_never_returns_the_null_row() {
    let (_s, pool) = server().await;
    for clause in [r#"NOT (doc @> '{"a":1}')"#, r#"NOT (doc->'tags' @> '["x"]')"#] {
        let sql = format!("SELECT id FROM d WHERE {} ORDER BY id", clause);
        match ids(&pool, &sql).await {
            Err(_) => {}
            Ok(got) => assert!(
                !got.split(',').any(|id| id == "2"),
                "{} returned the NULL row: {}",
                clause,
                got
            ),
        }
    }
}

/// `doc @> '{bad'` is 22P02 "invalid input syntax for type json" in
/// PostgreSQL, before any row is read — including on a table of nothing but
/// objects, where the raw-text literal used to answer "no rows" silently.
#[tokio::test]
async fn an_invalid_json_literal_is_refused_with_22p02() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        r#"CREATE TABLE objs (id INT PRIMARY KEY, doc JSONB);
           INSERT INTO objs (id, doc) VALUES (1, '{"a":1}');
           INSERT INTO objs (id, doc) VALUES (2, '{"b":2}')"#,
    ))
    .await
    .expect("seed");
    for sql in [
        "SELECT id FROM objs WHERE doc @> '{bad'",
        "SELECT id FROM objs WHERE doc <@ 'not json'",
        "SELECT id FROM d WHERE doc @> '{bad'",
        "SELECT id FROM d WHERE doc->'tags' @> '[1,'",
    ] {
        let err = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&pool)
            .await
            .expect_err(sql);
        let db = err.as_database_error().unwrap_or_else(|| panic!("{}: {}", sql, err));
        assert_eq!(db.code().as_deref(), Some("22P02"), "{}: {}", sql, db.message());
        assert!(
            db.message().contains("invalid input syntax for type json"),
            "{}: {}",
            sql,
            db.message()
        );
    }
}
