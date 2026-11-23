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
    // A NULL from `->>` / `#>>` makes every comparison and LIKE not true.
    ("doc->>'a' = ''", "3"),
    ("doc->>'a' <> 'z'", "1,2,3,9,10,13"),
    ("doc->>'a' != 'z'", "1,2,3,9,10,13"),
    ("doc->>'a' < 'z'", "1,2,3,9,10,13"),
    ("doc->>'a' >= ''", "1,2,3,9,10,13"),
    ("doc->>'a' LIKE '%'", "1,2,3,9,10,13"),
    ("doc->>'a' LIKE 'x%'", "1,13"),
    ("doc->>'a' ILIKE 'X'", "1,13"),
    ("doc #>> '{b,c}' <> 'q'", "1,10"),
    ("doc #>> '{b,c}' = 'x'", "1"),
    ("doc->'b'->>'c' = ''", ""),
    ("doc->'b'->>'c' <> ''", "1,10"),
    ("doc->>'a' = 'x'", "1,13"),
    ("doc->>'zz' >= ''", ""),
    ("doc->>'a' = 'true'", "9"),
    // `->` and `#>` compare as jsonb values, literal cast or not.
    (r#"doc->'a' = '"x"'"#, "1,13"),
    ("doc->'a' = 'true'", "9"),
    ("doc->'a' = 'true'::jsonb", "9"),
    (r#"doc->'a' = '"x"'::jsonb"#, "1,13"),
    ("doc->'a' = 'null'", "4"),
    ("doc->'a' <> 'null'", "1,2,3,9,10,13"),
    (r#"doc->'a' <> '"x"'"#, "2,3,4,9,10"),
    ("doc->'n' = '1'", "1,2,10"),
    ("doc->'n' = '1.0'", "1,2,10"),
    ("doc->'n' <> '1'", "3,9"),
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
    // IN, NOT IN and BETWEEN over a path.
    ("doc->>'a' IN ('x','y')", "1,2,13"),
    ("doc->>'a' IN ('x')", "1,13"),
    ("doc->>'a' IN ('x', NULL)", "1,13"),
    ("doc->>'a' NOT IN ('x')", "2,3,9,10"),
    ("doc->>'a' NOT IN ('x', 'y', '')", "9,10"),
    ("doc->>'a' NOT IN ('x', NULL)", ""),
    ("(doc->>'a') IN ('x')", "1,13"),
    ("doc #>> '{b,c}' IN ('x','z')", "1,10"),
    ("doc->'b'->>'c' NOT IN ('x')", "10"),
    ("doc->>'a' BETWEEN 'a' AND 'y'", "1,2,9,13"),
    ("doc->>'a' NOT BETWEEN 'a' AND 'x'", "2,3,10"),
    // NOT is three-valued: NOT of NULL is NULL, and the row is dropped.
    ("NOT (doc->>'a' = 'x')", "2,3,9,10"),
    ("NOT doc->>'a' = 'x'", "2,3,9,10"),
    ("NOT (doc->>'a' <> 'x')", "1,13"),
    ("NOT (doc ? 'a')", "5,7,11,12"),
    ("NOT doc ? 'a'", "5,7,11,12"),
    (r#"NOT (doc @> '{"a":"x"}')"#, "2,3,4,5,6,7,9,10,11,12"),
    ("NOT (doc->>'a' = 'x' OR doc->>'a' = 'y')", "3,9,10"),
    ("NOT (doc->>'a' = 'x' AND doc->'n' = '1')", "2,3,9,10"),
    ("NOT NOT (doc->>'a' = 'x')", "1,13"),
    ("NOT (doc->>'a' LIKE 'x%')", "2,3,9,10"),
    ("doc->>'a' NOT LIKE 'x%'", "2,3,9,10"),
    ("doc->>'a' NOT ILIKE 'X%'", "2,3,9,10"),
    ("NOT (doc->>'a' IN ('x'))", "2,3,9,10"),
    ("NOT (doc->>'a' NOT IN ('x'))", "1,13"),
    ("NOT (doc->>'a' IS NULL)", "1,2,3,9,10,13"),
    ("NOT (doc->>'a' BETWEEN 'a' AND 'x')", "2,3,10"),
    ("NOT (id = 1)", "2,3,4,5,6,7,8,9,10,11,12,13"),
    ("NOT id = 1", "2,3,4,5,6,7,8,9,10,11,12,13"),
    ("NOT (id > 3 AND id < 10)", "1,2,3,10,11,12,13"),
    ("id > 2 AND NOT (doc->'n' = '1')", "3,9"),
    ("NOT (doc->'a' = 'true')", "1,2,3,4,10,13"),
    ("NOT (doc->'n' = '1') OR id = 1", "1,3,9"),
    ("NOT (doc ?| array['a','b'])", "7,11,12"),
    (r#"NOT (doc @> '{"a":"x"}' AND doc->>'a' = 'x')"#, "2,3,4,5,6,7,9,10,11,12"),
    // IS [NOT] NULL over a path.
    ("doc->>'a' IS NULL", "4,5,6,7,8,11,12"),
    ("doc->>'a' IS NOT NULL", "1,2,3,9,10,13"),
    ("doc->'a' IS NULL", "5,6,7,8,11,12"),
    ("doc->'a' IS NOT NULL", "1,2,3,4,9,10,13"),
    ("doc #>> '{b,c}' IS NULL", "2,3,4,5,6,7,8,9,11,12,13"),
    ("doc #> '{b,c}' IS NULL", "2,3,4,5,6,7,8,9,11,12"),
    ("doc->'b'->>'c' IS NOT NULL", "1,10"),
    ("doc->>'a' ISNULL", "4,5,6,7,8,11,12"),
    ("doc->>'a' NOTNULL", "1,2,3,9,10,13"),
    ("id > 0 AND doc->>'a' IS NULL", "4,5,6,7,8,11,12"),
    ("(doc->>'a') IS NULL", "4,5,6,7,8,11,12"),
    ("(doc->>'a') IS NOT NULL", "1,2,3,9,10,13"),
    ("doc IS NULL", "8"),
    ("doc IS NOT NULL", "1,2,3,4,5,6,7,9,10,11,12,13"),
    // NOT over plain nullable columns.
    ("NOT (t = 'p')", "2,4,5,7,9,12,13"),
    ("NOT t = 'p'", "2,4,5,7,9,12,13"),
    ("NOT (t LIKE 'p%')", "2,4,7,9,12,13"),
    ("t NOT LIKE 'p%'", "2,4,7,9,12,13"),
    ("NOT (t ILIKE 'p')", "2,4,5,7,9,12"),
    ("t NOT ILIKE 'p'", "2,4,5,7,9,12"),
    ("NOT (t IN ('p','q'))", "4,5,7,9,13"),
    ("NOT (t NOT IN ('p'))", "1,8,11"),
    ("NOT (t IS NULL)", "1,2,4,5,7,8,9,11,12,13"),
    ("NOT (t IS NOT NULL)", "3,6,10"),
    ("NOT (n BETWEEN 1 AND 2)", "4,9,10,12"),
    ("NOT (n > 1 OR t = 'p')", "9"),
    ("NOT (n > 1 AND t = 'p')", "1,2,4,5,6,7,9,12,13"),
    ("NOT (n IS DISTINCT FROM 2)", "2,7,13"),
    ("NOT (t SIMILAR TO 'p%')", "2,4,7,9,12,13"),
    ("NOT (n = 1) AND NOT (t = 'q')", "4,7,9,13"),
    ("(NOT (n = 1)) OR t = 'x'", "2,4,7,9,10,12,13"),
    ("NOT ((n = 1 OR n = 2) AND t = 'p')", "2,4,5,7,9,10,12,13"),
    ("NOT (n + 1 > 2)", "1,6,9"),
    ("NOT (t = 'p') IS TRUE", "2,3,4,5,6,7,9,10,12,13"),
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

/// A document compared with a number, a boolean or a non-text column is a type
/// error in PostgreSQL, not a comparison. Read as a document instead, the
/// bytes of `1` begin with the tag of JSON `false`, and those of `2.5` with
/// the tag of `null`, so `doc->'a' = 1` matched `{"a":false}`. PostgreSQL 17
/// refuses every case below with "operator does not exist: jsonb = integer"
/// (or `numeric`, `boolean`, `text`, `bigint`, or `integer = jsonb` when the
/// document is on the right).
#[tokio::test(flavor = "multi_thread")]
async fn a_document_against_a_number_or_a_non_text_column_is_refused() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        r#"INSERT INTO s VALUES (20,'{"a":false,"p":null}','x',1),(21,'{"a":true,"p":2.5}','y',2)"#,
    ))
    .await
    .expect("seed");
    let mut wrong = Vec::new();
    // `(clause, the operator PostgreSQL names)`. Under a NOT the engine has
    // already rewritten `=` to `<>`, and `true` to `1` (PostgreSQL says
    // `jsonb = boolean`), so for those only the refusal itself is checked.
    for (clause, operator) in [
        ("doc->'a' = 1", "jsonb = integer"),
        ("doc->'a' = 2", "jsonb = integer"),
        ("doc->'a' <> 1", "jsonb <> integer"),
        ("doc->'a' != 1", "jsonb <> integer"),
        ("doc->'p' = 2.5", "jsonb = numeric"),
        ("doc->'p' = 0.0", "jsonb = numeric"),
        ("doc->'a' = true", ""),
        ("doc->'a' > 0", "jsonb > integer"),
        ("doc->'a' IN (1, 2)", "jsonb = integer"),
        ("doc->'a' NOT IN (1, 2)", "jsonb <> integer"),
        ("doc #> '{a}' = 1", "jsonb = integer"),
        ("1 = doc->'a'", "integer = jsonb"),
        ("doc->'a' = n", "jsonb = integer"),
        ("n = doc->'a'", "integer = jsonb"),
        ("doc->'a' = n + 1", "jsonb = integer"),
        ("doc->'a' = t", "jsonb = text"),
        ("doc = n", "jsonb = integer"),
        ("doc = 1", "jsonb = integer"),
        ("NOT (doc->'a' = 1)", ""),
    ] {
        match ids(&pool, clause).await {
            Err(e) if e.contains(&format!("operator does not exist: {operator}")) => {}
            got => wrong.push(format!("{clause:<25} PostgreSQL refuses this ({operator}), got {got:?}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // A document against a document or a quoted literal is still a comparison.
    assert_eq!(ids(&pool, "doc->'a' = 'false'").await.as_deref(), Ok("20"));
    assert_eq!(ids(&pool, "doc->'a' = doc->'a' AND id >= 20").await.as_deref(), Ok("20,21"));
}

/// `IS NULL` over a path in the select list is a boolean, not the extracted
/// text. PostgreSQL 17: `SELECT id, doc->>'a' IS NULL FROM s ORDER BY id`.
#[tokio::test(flavor = "multi_thread")]
async fn is_null_over_a_path_in_the_select_list_is_a_boolean() {
    let (_s, pool) = server().await;
    let want = "1|f 2|f 3|f 4|t 5|t 6|t 7|t 8|t 9|f 10|f 11|t 12|t 13|f";
    let rows = sqlx::query("SELECT id, doc->>'a' IS NULL AS missing FROM s ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("query");
    let got = rows
        .iter()
        .map(|r| {
            let missing: bool = r.get("missing");
            format!("{}|{}", r.get::<i64, _>("id"), if missing { "t" } else { "f" })
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(got, want);

    let want = "1|t|f 2|t|f 3|t|f 4|t|t 5|f|t";
    let rows = sqlx::query(
        "SELECT id, doc->'a' IS NOT NULL AS present, (doc->>'a') IS NULL AS missing \
         FROM s WHERE id < 6 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("query");
    let got = rows
        .iter()
        .map(|r| {
            let flag = |b: bool| if b { "t" } else { "f" };
            format!(
                "{}|{}|{}",
                r.get::<i64, _>("id"),
                flag(r.get("present")),
                flag(r.get("missing"))
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(got, want);
}
