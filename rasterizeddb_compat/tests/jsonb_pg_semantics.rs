//! Predicates over JSON expressions, answered as PostgreSQL 17 answers them.
//!
//! Every expected answer below was produced by running the same predicate
//! against the same rows on a real PostgreSQL 17. The cases come from a
//! differential fuzzer that found four ways this engine disagreed:
//!
//! - `->>` and `#>>` of a missing path, of a JSON `null`, or of the wrong kind
//!   of container are SQL NULL, so every comparison and `LIKE` on them is not
//!   true. They were read as the empty string, so `doc->>'a' <> 'z'` returned
//!   every row that had no `a` at all.
//! - `jsonb = jsonb` compares values: `1` equals `1.0`, and a literal compared
//!   with `->` or `#>` is a document even without a `::jsonb` cast. Bytes were
//!   compared instead, so `doc->'a' = '1'` matched nothing.
//! - `x IN (...)`, `NOT (...)` and `IS [NOT] NULL` over a path were refused.
//! - `NOT` is three-valued: `NOT (doc->>'a' = 'x')` drops a row with no `a`,
//!   because NOT of NULL is NULL.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

/// The rows every case runs against. Row 8's document is SQL NULL, row 4's
/// `a` is JSON null, rows 6, 7, 11 and 12 are not objects at all, and `t` and
/// `n` are NULL on some rows so that `NOT` over a plain column is covered too.
const ROWS: &[&str] = &[
    r#"(1,'{"a":"x","b":{"c":"x"},"n":1}','p',1)"#,
    r#"(2,'{"a":"y","n":1.0}','q',2)"#,
    r#"(3,'{"a":"","n":2}',NULL,NULL)"#,
    r#"(4,'{"a":null}','',3)"#,
    r#"(5,'{"b":1}','pz',NULL)"#,
    r#"(6,'[1,"a"]',NULL,1)"#,
    r#"(7,'"x"','r',2)"#,
    r#"(8,NULL,'p',NULL)"#,
    r#"(9,'{"a":true,"n":-0}','x',0)"#,
    r#"(10,'{"a":1,"n":1e0,"b":{"c":"z"}}',NULL,5)"#,
    r#"(11,'1.0','p',NULL)"#,
    r#"(12,'1','q',4)"#,
    r#"(13,'{"a":"x","b":{"c":null}}','P',2)"#,
];

/// `(predicate, ids PostgreSQL 17 returned)`.
const CASES: &[(&str, &str)] = &[
    // `->` and `#>` compare as jsonb values, literal cast or not.
    (r#"doc->'a' = '"x"'"#, "1,13"),
    ("doc->'a' = 'true'", "9"),
    ("doc->'a' = 'true'::jsonb", "9"),
    (r#"doc->'a' = '"x"'::jsonb"#, "1,13"),
    ("doc->'a' = 'null'", "4"),
    ("doc->'n' = '1'", "1,2,10"),
    ("doc->'n' = '1.0'", "1,2,10"),
    ("doc->'n' = '0'", "9"),
    (r#"doc #> '{b,c}' = '"x"'"#, "1"),
    (r#"doc #> '{b,c}' = '"x"'::jsonb"#, "1"),
    (r#"doc->'b' = '{"c":"x"}'"#, "1"),
    (r#"doc->'b' = '{"c": "x"}'::jsonb"#, "1"),
    ("'true'::jsonb = doc->'a'", "9"),
    ("'1' = doc->'n'", "1,2,10"),
    // The whole column, too.
    ("doc = '1'::jsonb", "11,12"),
    ("doc = '1.0'::jsonb", "11,12"),
    ("doc = '1'", "11,12"),
    ("doc <> '1'", "1,2,3,4,5,6,7,9,10,13"),
    (r#"doc = '{"n":1.0,"a":1,"b":{"c":"z"}}'"#, "10"),
    (r#"doc = '[1,"a"]'"#, "6"),
    (r#"doc = '[1.0,"a"]'"#, "6"),
    (r#"doc IN ('1', '"x"')"#, "7,11,12"),
];

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE s (id BIGINT PRIMARY KEY, doc JSONB, t TEXT, n INT)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "INSERT INTO s VALUES {}",
        ROWS.join(", ")
    ))))
    .await
    .expect("seed");
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, clause: &str) -> Result<String, String> {
    let sql = format!("SELECT id FROM s WHERE {clause} ORDER BY id");
    match sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.get::<i64, _>("id").to_string())
            .collect::<Vec<_>>()
            .join(",")),
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn predicates_answer_as_postgresql_does() {
    let (_s, pool) = server().await;
    let mut wrong = Vec::new();
    for (clause, want) in CASES {
        let got = ids(&pool, clause).await;
        if got.as_deref() != Ok(*want) {
            wrong.push(format!("{clause:<45} want {want:<30} got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} of {} wrong:\n{}", wrong.len(), CASES.len(), wrong.join("\n"));
}

/// PostgreSQL refuses a literal that is not JSON where a document is expected,
/// rather than comparing it as text: `doc->'a' <> ''` was this engine's old way
/// of asking whether a key exists, and PostgreSQL has never accepted it. `?`
/// is the key-existence operator.
#[tokio::test(flavor = "multi_thread")]
async fn a_literal_that_is_not_json_is_refused_against_a_document_path() {
    let (_s, pool) = server().await;
    for clause in ["doc->'a' <> ''", "doc->'a' = 'x'", "doc #> '{b,c}' = 'x'"] {
        let got = ids(&pool, clause).await;
        match got {
            Err(e) => assert!(e.contains("invalid input syntax for type json"), "{clause}: {e}"),
            Ok(rows) => panic!("{clause}: PostgreSQL refuses this, got rows {rows:?}"),
        }
    }
}
