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

/// The "not judged" rule is narrow and said once.
///
/// A check whose regular expressions stand under AND, OR and NOT alone is
/// judged with a locale-dependent answer UNKNOWN, which is exact:
/// `s ~ '^\w+$' AND n > 0` with ('héllo', -1) is FALSE whatever `\w` says of
/// 'é', and PostgreSQL refuses it; it was accepted. Only a check that reads
/// such an answer as a value (`… IS TRUE`) is not judged for the row, and
/// that is said once per statement as a WARNING naming the constraint, with
/// how many rows. A pattern or flags read from the row outside the
/// translated subset refuse the row (0A000; an invalid one 2201B), where
/// plain ASCII data was not judged. `character(n)` checks are kept
/// unenforced (said so), as this engine does not compare blank-padded: they
/// refused the 'AB ' PostgreSQL accepts. `SHOW CHECKS` lists every check
/// and whether it is enforced.
#[tokio::test(flavor = "multi_thread")]
async fn the_not_judged_rule_is_narrow_and_said_once() {
    use std::sync::{Arc, Mutex};
    let server = TestServer::start().await.expect("server");
    let (client, mut connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    let notices: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let sink = notices.clone();
    tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = message {
                sink.lock().unwrap().push((n.code().code().to_string(), n.message().to_string()));
            }
        }
    });
    let taken = || std::mem::take(&mut *notices.lock().unwrap());
    let outcome = |r: Result<(), tokio_postgres::Error>| match r {
        Ok(()) => "ok".to_string(),
        Err(e) => e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string()),
    };
    client
        .batch_execute(
            "CREATE TABLE ck (id INT PRIMARY KEY, s TEXT, n INT, CONSTRAINT ck_c CHECK (s ~ '^\\w+$' AND n > 0)); \
             CREATE TABLE nm (id INT PRIMARY KEY, s TEXT, n INT, CONSTRAINT nm_c CHECK ((s ~ '^\\w+$') IS TRUE OR n > 0)); \
             CREATE TABLE cp (id INT PRIMARY KEY, s TEXT, p TEXT, CHECK (s ~ p)); \
             CREATE TABLE cf (id INT PRIMARY KEY, s TEXT, f TEXT, CHECK (regexp_like(s, 'a', f))); \
             CREATE TABLE em (id INT PRIMARY KEY, s TEXT, CHECK (s ~ '[\\w-.]+@x'))",
        )
        .await
        .expect("setup");
    let _ = taken();
    let mut wrong = Vec::new();
    for (sql, want) in [
        // FALSE whatever the regex answers: judged. (PostgreSQL 17's answers.)
        ("INSERT INTO ck VALUES (1, 'héllo', -1)", "23514"),
        ("INSERT INTO ck VALUES (2, 'h llo', 1)", "23514"),
        ("INSERT INTO ck VALUES (3, 'hello', 1)", "ok"),
        // Read as a value: not judged (PostgreSQL accepts it: 'é' is \w).
        ("INSERT INTO nm VALUES (1, 'héllo', -1)", "ok"),
        ("INSERT INTO nm VALUES (2, 'h llo', -1)", "23514"),
        // From the row: outside the subset 0A000, invalid 2201B, else judged.
        ("INSERT INTO cp VALUES (1, 'aa', '^(a)\\1$')", "0A000"),
        ("INSERT INTO cp VALUES (2, 'aa', '^(a')", "2201B"),
        ("INSERT INTO cp VALUES (3, 'aa', '^a+$')", "ok"),
        ("INSERT INTO cp VALUES (4, 'ab', '^a+$')", "23514"),
        ("INSERT INTO cf VALUES (1, 'a', 'x')", "0A000"),
        ("INSERT INTO cf VALUES (2, 'A', 'i')", "ok"),
        // `-` after a class: PostgreSQL's invalid character range.
        ("INSERT INTO em VALUES (1, 'a@x')", "2201B"),
    ] {
        let got = outcome(client.batch_execute(sql).await);
        if got != want {
            wrong.push(format!("{sql}: {got}, want {want}"));
        }
    }
    let _ = taken();
    // Three rows not judged by one statement: one WARNING, with the count.
    client
        .batch_execute("INSERT INTO nm VALUES (10, 'é', -1), (11, 'ü', -1), (12, 'ß', -1)")
        .await
        .expect("not judged");
    let warned: Vec<(String, String)> =
        taken().into_iter().filter(|(_, m)| m.contains("was not judged")).collect();
    if !(warned.len() == 1
        && warned[0].0 == "01000"
        && warned[0].1.contains("\"nm_c\"")
        && warned[0].1.contains("\"nm\"")
        && warned[0].1.ends_with("(3 rows)"))
    {
        wrong.push(format!("not-judged warning: {warned:?}"));
    }

    // character(n): kept unenforced, said so, and the padded value stored.
    for (create, table) in [
        ("CREATE TABLE c1 (id INT PRIMARY KEY, tag character(3), CONSTRAINT c1_tag_check CHECK ((tag = ANY (ARRAY['AB'::bpchar, 'CD'::bpchar]))))", "c1"),
        ("CREATE TABLE c3 (id INT PRIMARY KEY, tag char(3) CHECK (tag IN ('AB', 'CD')))", "c3"),
    ] {
        client.batch_execute(create).await.expect(table);
        let warned = taken();
        if !warned.iter().any(|(c, m)| c == "01000" && m.contains("not enforced") && m.contains("character(n)")) {
            wrong.push(format!("{table}: no warning: {warned:?}"));
        }
        let got = outcome(client.batch_execute(&format!("INSERT INTO {table} VALUES (1, 'AB ')")).await);
        if got != "ok" {
            wrong.push(format!("{table}: 'AB ' refused {got}"));
        }
    }

    // Listed, with whether each is enforced.
    let listed: Vec<String> = client
        .simple_query("SHOW CHECKS")
        .await
        .expect("SHOW CHECKS")
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some(format!("{} {} {}", r.get(0).unwrap_or(""), r.get(1).unwrap_or(""), r.get(2).unwrap_or("")))
            }
            _ => None,
        })
        .collect();
    for want in ["ck ck_c enforced (ASCII-exact)", "nm nm_c enforced (ASCII-exact)", "c1 c1_tag_check not enforced"] {
        if !listed.iter().any(|l| l == want) {
            wrong.push(format!("SHOW CHECKS lacks {want:?}: {listed:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A check reading a `character(n)` column is kept unenforced (said so) on
/// every DDL path, and listed as such: CREATE TABLE with `tag::text`, a bare
/// `CHAR`, ALTER TABLE ADD CONSTRAINT on an existing CHAR(3) (alone and in an
/// OR), and ADD COLUMN … CHAR(3) CHECK over a table with rows. Each refused
/// the 'AB ' PostgreSQL accepts, and SHOW CHECKS said "enforced". And ADD
/// CONSTRAINT says when stored rows go unjudged; SHOW CHECKS says a check
/// over a locale-dependent pattern is enforced for ASCII text exactly.
#[tokio::test(flavor = "multi_thread")]
async fn character_n_checks_are_declined_on_every_ddl_path() {
    use std::sync::{Arc, Mutex};
    let server = TestServer::start().await.expect("server");
    let (client, mut connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    let notices: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = notices.clone();
    tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = message {
                sink.lock().unwrap().push(n.message().to_string());
            }
        }
    });
    let taken = || std::mem::take(&mut *notices.lock().unwrap());
    let mut wrong = Vec::new();
    client
        .batch_execute("CREATE TABLE c4 (id INT PRIMARY KEY, tag CHAR(3), q INT, tg2 TEXT); INSERT INTO c4 VALUES (9, 'CD', 1, NULL)")
        .await
        .expect("c4");
    let _ = taken();
    for (ddl, name) in [
        ("ALTER TABLE c4 ADD CONSTRAINT c4_t CHECK (tag IN ('AB', 'CD'))", "c4_t"),
        ("ALTER TABLE c4 ADD CONSTRAINT c4_o CHECK (q > 0 OR tag = 'AB')", "c4_o"),
        ("ALTER TABLE c4 ADD COLUMN tg3 CHAR(3) CHECK (tg3 IN ('X'))", "c4_tg3_check"),
        ("CREATE TABLE c5 (id INT PRIMARY KEY, tag CHAR(3) CHECK (tag::text IN ('AB', 'CD')), c1 CHAR CHECK (c1 IN ('a')))", "c5_tag_check"),
    ] {
        client.batch_execute(ddl).await.unwrap_or_else(|e| panic!("{ddl}: {e}"));
        let warned = taken();
        if !warned.iter().any(|m| m.contains(&format!("\"{name}\"")) && m.contains("character(n)")) {
            wrong.push(format!("{ddl}: no warning: {warned:?}"));
        }
    }
    for sql in ["INSERT INTO c4 VALUES (1, 'AB ', -1, NULL, 'X  ')", "INSERT INTO c5 VALUES (1, 'AB ', 'a')"] {
        if let Err(e) = client.batch_execute(sql).await {
            wrong.push(format!("{sql}: {e}"));
        }
    }
    // Stored rows a check leaves unjudged are said at ADD CONSTRAINT.
    client
        .batch_execute("CREATE TABLE w (id INT PRIMARY KEY, s TEXT, n INT); INSERT INTO w VALUES (1, 'é', 1), (2, 'ab', 1), (3, 'ü', 1)")
        .await
        .expect("w");
    let _ = taken();
    client.batch_execute("ALTER TABLE w ADD CONSTRAINT w_c CHECK (s ~ '^\\w+$' AND n > 0)").await.expect("w_c");
    let warned = taken();
    if !warned.iter().any(|m| m.contains("\"w_c\"") && m.contains("not judged for 2 existing rows")) {
        wrong.push(format!("ADD CONSTRAINT w_c: {warned:?}"));
    }
    let listed: Vec<String> = client
        .simple_query("SHOW CHECKS")
        .await
        .expect("SHOW CHECKS")
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(format!("{} {}", r.get(1).unwrap_or(""), r.get(2).unwrap_or(""))),
            _ => None,
        })
        .collect();
    for want in [
        "c4_t not enforced",
        "c4_o not enforced",
        "c4_tg3_check not enforced",
        "c5_tag_check not enforced",
        "c5_c1_check not enforced",
        "w_c enforced (ASCII-exact)",
    ] {
        if !listed.iter().any(|l| l == want) {
            wrong.push(format!("SHOW CHECKS lacks {want:?}: {listed:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A column's `character(n)` record follows it through later DDL: `RENAME
/// COLUMN` moves it, `DROP COLUMN` retires it, and `ALTER COLUMN … TYPE`
/// sets or retires it, declining the checks already stored on the column
/// when it becomes `character(n)`. Kept per column name and left alone, a
/// check added after `RENAME code TO tag` was enforced on a `character(n)`
/// column (refusing `'AB '`), one on a TEXT column that took a former
/// `character(n)` column's name was declined (accepting `'ZZ'`), and a
/// TEXT column's check stayed enforced after `TYPE CHAR(3)`.
///
/// Mutations: the marker's `move_key` in `rename_column`; its `retire` in
/// `retire_leftovers`; `bpchar_type_change` returning nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_character_n_record_follows_rename_drop_and_type_changes() {
    let root = format!("/var/tmp/check_corpus_bpchar_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let server = TestServer::start_file_backed(&root).await.expect("server");
    let (client, connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE m (id INT PRIMARY KEY, tag CHAR(3), q INT);
             INSERT INTO m VALUES (9, 'AB', 1);
             ALTER TABLE m DROP COLUMN tag;
             ALTER TABLE m ADD COLUMN tag TEXT;
             ALTER TABLE m ADD CONSTRAINT m_c CHECK (tag IN ('AB'));
             CREATE TABLE n (id INT PRIMARY KEY, tag CHAR(3), q INT);
             ALTER TABLE n ALTER COLUMN tag TYPE TEXT;
             ALTER TABLE n ADD CONSTRAINT n_c CHECK (tag IN ('AB'));
             CREATE TABLE o (id INT PRIMARY KEY, code CHAR(3), q INT);
             ALTER TABLE o RENAME COLUMN code TO tag;
             ALTER TABLE o ADD CONSTRAINT o_c CHECK (tag IN ('AB'));
             CREATE TABLE p (id INT PRIMARY KEY, tag CHAR(3), code TEXT);
             ALTER TABLE p RENAME COLUMN tag TO t2;
             ALTER TABLE p RENAME COLUMN code TO tag;
             ALTER TABLE p ADD CONSTRAINT p_c CHECK (tag IN ('AB'));
             CREATE TABLE l (id INT PRIMARY KEY, tag TEXT, CONSTRAINT l_c CHECK (tag IN ('AB', 'CD')));
             ALTER TABLE l ALTER COLUMN tag TYPE CHAR(3);",
        )
        .await
        .expect("ddl");
    let mut wrong = Vec::new();
    // TEXT columns: the check is enforced, as in PostgreSQL.
    for sql in [
        "INSERT INTO m (id, q, tag) VALUES (1, 1, 'ZZ')",
        "INSERT INTO n VALUES (1, 'ZZ', 1)",
        "INSERT INTO p VALUES (1, 'x', 'ZZ')",
    ] {
        match client.batch_execute(sql).await {
            Ok(()) => wrong.push(format!("{sql}: accepted")),
            Err(e) if e.code().map(|c| c.code()) == Some("23514") => {}
            Err(e) => wrong.push(format!("{sql}: {e}")),
        }
    }
    // character(n) columns: declined, so a blank-padded value PostgreSQL
    // accepts is not refused.
    for sql in ["INSERT INTO o VALUES (1, 'AB ', 1)", "INSERT INTO l VALUES (1, 'AB ')"] {
        if let Err(e) = client.batch_execute(sql).await {
            wrong.push(format!("{sql}: {e}"));
        }
    }
    let listed: Vec<String> = client
        .simple_query("SHOW CHECKS")
        .await
        .expect("SHOW CHECKS")
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(format!("{} {}", r.get(1).unwrap_or(""), r.get(2).unwrap_or(""))),
            _ => None,
        })
        .collect();
    for want in ["m_c enforced", "n_c enforced", "p_c enforced", "o_c not enforced", "l_c not enforced"] {
        if !listed.iter().any(|l| l == want) {
            wrong.push(format!("SHOW CHECKS lacks {want:?}: {listed:?}"));
        }
    }
    // And after a restart, from the records alone.
    drop(client);
    drop(server);
    let server = TestServer::start_file_backed(&root).await.expect("restart");
    let (client, connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("reconnect");
    tokio::spawn(connection);
    match client.batch_execute("INSERT INTO p VALUES (2, 'x', 'YY')").await {
        Err(e) if e.code().map(|c| c.code()) == Some("23514") => {}
        other => wrong.push(format!("after restart, p accepted 'YY': {other:?}")),
    }
    if let Err(e) = client.batch_execute("INSERT INTO o VALUES (2, 'AB ', 1)").await {
        wrong.push(format!("after restart, o: {e}"));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
