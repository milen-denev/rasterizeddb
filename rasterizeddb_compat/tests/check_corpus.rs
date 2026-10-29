//! CHECK constraints as `pg_dump` and ORMs write them.
//!
//! `pg_dump` renders every check in a canonical form: doubled parentheses,
//! casts on literals, `IN (…)` as `= ANY (ARRAY[…])`, `LIKE` as `~~`. That
//! form was refused at CREATE TABLE (42601 on '['), and DDL is not
//! transactional, so a `psql -f dump.sql` restore lost those tables and their
//! rows. Every form here must be accepted and enforced, with SQL's NULL
//! semantics (a NULL result passes), on a plain and on a hash-partitioned
//! table; a form nothing here can evaluate is refused with 0A000.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

/// Columns every case's table has.
const COLUMNS: &str =
    "id INT PRIMARY KEY, status TEXT, vs VARCHAR(20), x TEXT, n INT, price NUMERIC(10,2), ts TIMESTAMP, doc JSONB, flag BOOLEAN";

/// (who writes it, the check, rows as `status, vs, x, n, price, ts, doc, flag`
/// with whether the row is stored)
type Case = (&'static str, &'static str, &'static [(&'static str, bool)]);

const NULLS: &str = "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL";

const CASES: &[Case] = &[
    // pg_dump's canonical forms.
    ("pg_dump IN", "((status = ANY (ARRAY['a'::text, 'b'::text])))", &[
        ("'a', NULL, NULL, NULL, NULL, NULL, NULL, NULL", true),
        ("'c', NULL, NULL, NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump varchar IN", "(((vs)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[])))", &[
        ("NULL, 'b', NULL, NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, 'z', NULL, NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump NOT IN", "((status <> ALL (ARRAY['x'::text, 'y'::text])))", &[
        ("'a', NULL, NULL, NULL, NULL, NULL, NULL, NULL", true),
        ("'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump numeric", "((price > (0)::numeric))", &[
        ("NULL, NULL, NULL, NULL, 1.50, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump OR", "(((n IS NULL) OR (n > 0)))", &[
        ("NULL, NULL, NULL, 3, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, -3, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump timestamp", "((ts > '2000-01-01 00:00:00'::timestamp without time zone))", &[
        ("NULL, NULL, NULL, NULL, NULL, '2020-01-01 00:00:00', NULL, NULL", true),
        ("NULL, NULL, NULL, NULL, NULL, '1990-01-01 00:00:00', NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump LIKE", "((x ~~ '%@%'::text))", &[
        ("NULL, NULL, 'a@b', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'ab', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump ILIKE", "((x ~~* 'A%'::text))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'bc', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump NOT LIKE", "((x !~~ 'z%'::text))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'zz', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump char_length", "((char_length(x) <= 3))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'abcd', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump btrim", "((btrim(x) <> ''::text))", &[
        ("NULL, NULL, ' a ', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, '   ', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump jsonb_typeof", "((jsonb_typeof(doc) = 'object'::text))", &[
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"k\": 1}', NULL", true),
        ("NULL, NULL, NULL, NULL, NULL, NULL, '[1]', NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump ?", "((doc ? 'k'::text))", &[
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"k\": 1}', NULL", true),
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"j\": 1}', NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump @>", "((doc @> '{\"k\": 1}'::jsonb))", &[
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"k\": 1, \"j\": 2}', NULL", true),
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"k\": 2}', NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump ->>", "(((doc ->> 'k'::text) IS NOT NULL))", &[
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"k\": 1}', NULL", true),
        ("NULL, NULL, NULL, NULL, NULL, NULL, '{\"j\": 1}', NULL", false),
    ]),
    ("CASE value", "((CASE WHEN (n > 10) THEN price ELSE (100)::numeric END > (5)::numeric))", &[
        ("NULL, NULL, NULL, 20, 6, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, 20, 4, NULL, NULL, NULL", false),
        ("NULL, NULL, NULL, 1, 4, NULL, NULL, NULL", true),
    ]),
    ("pg_dump IS TRUE", "(((n > 0) IS TRUE))", &[
        ("NULL, NULL, NULL, 2, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, -2, NULL, NULL, NULL, NULL", false),
        // `IS TRUE` of NULL is FALSE: refused.
        (NULLS, false),
    ]),
    ("pg_dump IS DISTINCT FROM", "((x IS DISTINCT FROM 'zz'::text))", &[
        ("NULL, NULL, 'a', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'zz', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pg_dump boolean", "((flag IS NOT FALSE))", &[
        ("NULL, NULL, NULL, NULL, NULL, NULL, NULL, true", true),
        ("NULL, NULL, NULL, NULL, NULL, NULL, NULL, false", false),
        (NULLS, true),
    ]),
    // Django.
    ("Django", "(\"n\" >= 0)", &[
        ("NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, -1, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("Django range", "((\"price\" > 0 AND \"price\" < 100))", &[
        ("NULL, NULL, NULL, NULL, 50, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, NULL, 100, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    // Rails.
    ("Rails", "(char_length(status) <= 5)", &[
        ("'abc', NULL, NULL, NULL, NULL, NULL, NULL, NULL", true),
        ("'abcdef', NULL, NULL, NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    // SQLAlchemy's non-native enum and boolean.
    ("SQLAlchemy enum", "(status IN ('a', 'b'))", &[
        ("'b', NULL, NULL, NULL, NULL, NULL, NULL, NULL", true),
        ("'c', NULL, NULL, NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("SQLAlchemy boolean", "(n IN (0, 1))", &[
        ("NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, 2, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    // sea-orm.
    ("sea-orm", "(\"x\" <> '')", &[
        ("NULL, NULL, 'a', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, '', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("NOT LIKE written", "(x NOT LIKE 'z%')", &[
        ("NULL, NULL, 'a', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'za', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("SIMILAR TO", "(x SIMILAR TO '[a-z]+')", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'a1', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("COALESCE", "(COALESCE(n, 0) >= 0)", &[
        ("NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, NULL, -1, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
];

struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn corpus(partitioning: &str) {
    // File-backed: a partitioned table needs one.
    let dir = std::env::temp_dir().join(format!("check_corpus_{}_{}", std::process::id(), partitioning.len()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("root");
    let root = Root(dir.to_string_lossy().into_owned());
    let server = TestServer::start_file_backed(&root.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let mut wrong = Vec::new();
    for (n, (who, check, rows)) in CASES.iter().enumerate() {
        let table = format!("c{n}");
        // Declared at CREATE TABLE on even cases, by ALTER on odd ones.
        let declared = if n % 2 == 0 {
            pool.execute(AssertSqlSafe(format!(
                "CREATE TABLE {table} ({COLUMNS}, CONSTRAINT {table}_c CHECK {check}){partitioning}"
            )))
            .await
        } else {
            pool.execute(AssertSqlSafe(format!("CREATE TABLE {table} ({COLUMNS}){partitioning}")))
                .await
                .expect("create");
            pool.execute(AssertSqlSafe(format!("ALTER TABLE {table} ADD CONSTRAINT {table}_c CHECK {check}"))).await
        };
        if let Err(e) = declared {
            wrong.push(format!("{who}: {check}: declared: {e}"));
            continue;
        }
        for (i, (values, stored)) in rows.iter().enumerate() {
            let sql = format!("INSERT INTO {table} VALUES ({i}, {values})");
            match (pool.execute(AssertSqlSafe(sql.clone())).await, stored) {
                (Ok(_), true) => {}
                (Err(e), false) if code(&e) == "23514" => {}
                (got, _) => wrong.push(format!("{who}: {sql}: {:?}", got.map(|_| "stored").map_err(|e| e.to_string()))),
            }
        }
    }
    assert!(wrong.is_empty(), "{partitioning}: {} wrong:\n{}", wrong.len(), wrong.join("\n"));

    // What nothing here evaluates is refused where it is declared, with 0A000.
    // A CASE whose branches are themselves conditions is not read yet.
    for check in [
        "((x ~ '^[a-z]+$'::text))",
        "(no_such_function(n) > 0)",
        "(CASE WHEN (n > 10) THEN (price > (5)::numeric) ELSE true END)",
    ] {
        let e = pool
            .execute(AssertSqlSafe(format!("CREATE TABLE r ({COLUMNS}, CHECK {check}){partitioning}")))
            .await
            .expect_err(check);
        assert_eq!(code(&e), "0A000", "{check}: {e}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn check_forms_from_dumps_and_orms_on_a_plain_table() {
    corpus("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn check_forms_from_dumps_and_orms_on_a_hash_partitioned_table() {
    corpus(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
