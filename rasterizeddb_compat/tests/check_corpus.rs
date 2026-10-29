//! CHECK constraints as `pg_dump` and ORMs write them.
//!
//! `pg_dump` renders every check in a canonical form: doubled parentheses,
//! casts on literals, `IN (…)` as `= ANY (ARRAY[…])`, `LIKE` as `~~`. That
//! form was refused at CREATE TABLE (42601 on '['), and DDL is not
//! transactional, so a `psql -f dump.sql` restore lost those tables and their
//! rows. Every form here must be accepted and enforced, with SQL's NULL
//! semantics (a NULL result passes), on a plain and on a hash-partitioned
//! table; a form nothing here can evaluate is refused with 0A000.
//!
//! A regular expression is translated from PostgreSQL's dialect into one
//! matched exactly as PostgreSQL matches it (`core::rql::pg_regex`, checked
//! against answers recorded from PostgreSQL 17): `.` takes a newline, `\d`
//! is ASCII, `[[:<:]]` is a word start. A pattern outside what is translated
//! does not refuse the table or its rows: the table is created, the
//! constraint is not enforced, and a WARNING says so. A pattern PostgreSQL
//! itself rejects is accepted at CREATE, as there, and refuses each row
//! (2201B).

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
    // POSIX regular expressions: pg_dump's SIMILAR TO, Django's __regex and
    // __iregex, and the email/slug checks written by hand.
    ("pg_dump SIMILAR TO", "(((x)::text ~ similar_to_escape('a%'::text)))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'bbc', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("Django __regex", "((\"x\")::text ~ '^[a-z]+$'::text)", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'ab1', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("Django __iregex", "((\"x\")::text ~* '^[a-z]+$'::text)", &[
        ("NULL, NULL, 'ABC', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'AB1', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("regex negated", "((x !~ '[0-9]'::text))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'a1', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("regex negated, any case", "((x !~* 'Z'::text))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'az', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("email", "(x ~ '^[^@\\s]+@[^@\\s]+\\.[a-z]{2,}$')", &[
        ("NULL, NULL, 'a@b.co', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'a@b', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    // The shapes a regex check is written in, each of which was refused at
    // CREATE (0A000), and what each pattern means in PostgreSQL rather than in
    // Rust's `regex`.
    ("pg_dump NOT regex", "((NOT (x ~ 'z'::text)))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'az', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("NOT regexp_like", "(NOT regexp_like(x, '^[0-9]+$'))", &[
        ("NULL, NULL, 'a1', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, '12', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("regex = false", "(((x ~ 'a'::text) = false))", &[
        ("NULL, NULL, 'b', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'ba', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("regex = true", "((x ~ 'a') = true)", &[
        ("NULL, NULL, 'ba', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'b', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("pattern from the column", "(('abc'::text ~ x))", &[
        ("NULL, NULL, 'b', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'z', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("COLLATE", "(((x COLLATE \"C\") ~ '^[a-z]+$'::text))", &[
        ("NULL, NULL, 'abc', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'ABC', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("~ ANY", "((x ~ ANY (ARRAY['^a'::text, '^b'::text])))", &[
        ("NULL, NULL, 'bx', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'cx', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    ("a parenthesis in brackets", "((x ~ '[)]'::text))", &[
        ("NULL, NULL, 'a)', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'a(', NULL, NULL, NULL, NULL, NULL", false),
    ]),
    // Unicode-aware in Rust, too big to compile: every INSERT was 2201B.
    ("word class with a count", "((x ~ '^\\w{1,30}$'::text))", &[
        ("NULL, NULL, 'abc_1', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'a b', NULL, NULL, NULL, NULL, NULL", false),
        (NULLS, true),
    ]),
    // `.` takes a newline unless the `n` flag says otherwise.
    ("dot and newline", "((x ~ '^a.b$'::text))", &[
        ("NULL, NULL, E'a\\nb', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'ab', NULL, NULL, NULL, NULL, NULL", false),
    ]),
    ("newline-sensitive", "(regexp_like(x, '^a.b$', 'n'))", &[
        ("NULL, NULL, 'a-b', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, E'a\\nb', NULL, NULL, NULL, NULL, NULL", false),
    ]),
    ("word start", "((x ~ '[[:<:]]ab'::text))", &[
        ("NULL, NULL, 'x ab', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, 'xab', NULL, NULL, NULL, NULL, NULL", false),
    ]),
    ("digit escape", "((x ~ '^\\d+$'::text))", &[
        ("NULL, NULL, '123', NULL, NULL, NULL, NULL, NULL", true),
        ("NULL, NULL, '12a', NULL, NULL, NULL, NULL, NULL", false),
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

/// A pattern outside what is translated exactly neither refuses the table nor
/// its rows: CREATE and ADD CONSTRAINT succeed with a WARNING naming the
/// constraint, and a row PostgreSQL would refuse is stored. A pattern
/// PostgreSQL rejects is accepted at CREATE, as there, and every row meeting
/// it is refused (2201B; an unknown flag 22023). A row whose answer depends on
/// the locale's classes for non-ASCII text (`é` under `[[:alpha:]]`) is not
/// judged: stored, where an ASCII one is.
#[tokio::test(flavor = "multi_thread")]
async fn a_pattern_outside_the_translated_subset_is_unenforced_and_said_so() {
    use std::sync::{Arc, Mutex};
    let server = TestServer::start().await.expect("server");
    let (client, mut connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    let notices: Arc<Mutex<Vec<(String, String, String)>>> = Arc::default();
    let sink = notices.clone();
    tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = message {
                sink.lock().unwrap().push((n.severity().to_string(), n.code().code().to_string(), n.message().to_string()));
            }
        }
    });
    let taken = || std::mem::take(&mut *notices.lock().unwrap());
    let sqlstate = |e: tokio_postgres::Error| e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string());

    // A back-reference at CREATE; `\x` by ALTER over a table with rows.
    client
        .batch_execute("CREATE TABLE b (id INT PRIMARY KEY, s TEXT, CONSTRAINT b_c CHECK (s ~ '(a)\\1'))")
        .await
        .expect("created despite the pattern");
    let warned = taken();
    assert!(
        warned.iter().any(|(severity, code, message)| severity == "WARNING"
            && code == "01000"
            && message.contains("\"b_c\"")
            && message.contains("not enforced")),
        "{warned:?}"
    );
    client.batch_execute("INSERT INTO b VALUES (1, 'zz')").await.expect("not judged");
    client
        .batch_execute("ALTER TABLE b ADD CONSTRAINT b_x CHECK (s ~ '\\x41')")
        .await
        .expect("added despite the pattern");
    let warned = taken();
    assert!(warned.iter().any(|(s, _, m)| s == "WARNING" && m.contains("\"b_x\"")), "{warned:?}");
    client.batch_execute("INSERT INTO b VALUES (2, 'zz')").await.expect("not judged");

    // What PostgreSQL rejects: the table, then each row.
    for (n, (pattern, code)) in [("(", "2201B"), ("[a", "2201B"), ("*a", "2201B"), ("a)", "2201B")].into_iter().enumerate() {
        client
            .batch_execute(&format!("CREATE TABLE i{n} (id INT PRIMARY KEY, s TEXT, CHECK (s ~ '{pattern}'))"))
            .await
            .unwrap_or_else(|e| panic!("{pattern}: {e}"));
        let e = client.batch_execute(&format!("INSERT INTO i{n} VALUES (1, 'x')")).await.expect_err(pattern);
        assert_eq!(sqlstate(e), code, "{pattern}");
    }
    client
        .batch_execute("CREATE TABLE g (id INT PRIMARY KEY, s TEXT, CHECK (regexp_like(s, 'a', 'g')))")
        .await
        .expect("g");
    let e = client.batch_execute("INSERT INTO g VALUES (1, 'a')").await.expect_err("g flag");
    assert_eq!(sqlstate(e), "22023");

    // Locale classes over non-ASCII text: not judged; over ASCII: judged.
    client
        .batch_execute("CREATE TABLE w (id INT PRIMARY KEY, s TEXT, CONSTRAINT w_c CHECK (s !~ '[[:alpha:]]'))")
        .await
        .expect("w");
    client.batch_execute("INSERT INTO w VALUES (1, '1é')").await.expect("not judged");
    client.batch_execute("INSERT INTO w VALUES (2, '12')").await.expect("judged, holds");
    let e = client.batch_execute("INSERT INTO w VALUES (3, '1a')").await.expect_err("judged, violated");
    assert_eq!(sqlstate(e), "23514");
    for (table, want) in [("b", vec![1, 2]), ("w", vec![1, 2])] {
        let stored: Vec<i32> = client
            .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
            .await
            .expect("read")
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(stored, want, "{table}");
    }
}
