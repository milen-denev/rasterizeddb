//! A set operation over columns of different type families is refused, as
//! PostgreSQL 17 refuses it (42804, `UNION types text and bigint cannot be
//! matched`). It was answered: a bigint under a text description went out
//! blank, a text under an integer's as NULL, an integer under a date's as a
//! date of garbage (`323541-03-15`), and `SELECT name FROM a UNION ALL SELECT
//! big FROM b` lost the second row, each with no error.
//!
//! A string constant beside a typed column is read as that type, as
//! PostgreSQL resolves an untyped constant: `SELECT d FROM t UNION ALL SELECT
//! '2026-05-05'` was a date of garbage, `'false'` was true, a uuid or a
//! timestamp NULL.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in the
//! same order; its refusals by code and message (without the position).
//! Three things differ and are asserted as they are here, each a refusal
//! where PostgreSQL answers, never an answer where it refuses: a date beside
//! a timestamp (PostgreSQL makes the date a timestamp), a string constant
//! in the first branch beside a typed column, and the wording for a time
//! beside a date or a timestamp.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<String>, (String, String)> {
    match sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()),
        Err(e) => {
            let db = e.as_database_error().unwrap_or_else(|| panic!("`{sql}`: no database error: {e}"));
            Err((db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string()))
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn set_operations_match_type_families_as_postgres_does() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");

    // What answers: one family at each position, and a string constant read
    // as its column's type.
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE mx (id int PRIMARY KEY, i int, b bigint, n numeric(10,2), f double precision, t text, vc varchar(10), ch char(3), bo boolean, d date, ts timestamp, tz timestamptz, tm time, u uuid, j jsonb, by bytea, ia int[], ta text[])"#, vec![]),
        (r#"INSERT INTO mx VALUES (1, 7, 8, 1.5, 2.5, 'x', 'vc', 'c', true, '2026-01-02', '2026-01-02 03:04:05', '2026-01-02 03:04:05+00', '03:04:05', '11111111-1111-1111-1111-111111111111', '{"a":1}', '\x01', '{1,2}', '{p,q}')"#, vec![]),
        (r#"CREATE TABLE t32 (id int PRIMARY KEY, name text, n int)"#, vec![]),
        (r#"CREATE TABLE t64 (a bigint PRIMARY KEY, b bigint, c bigint, d bigint)"#, vec![]),
        (r#"INSERT INTO t32 VALUES (1, 'x', 7)"#, vec![]),
        (r#"INSERT INTO t64 VALUES (5000000000, 6000000000, 7000000000, 8000000000)"#, vec![]),
        (r#"SELECT i FROM mx UNION ALL SELECT b FROM mx"#, vec![r#"7"#, r#"8"#]),
        (r#"SELECT i FROM mx UNION ALL SELECT n FROM mx"#, vec![r#"7"#, r#"1.50"#]),
        (r#"SELECT i FROM mx UNION ALL SELECT f FROM mx"#, vec![r#"7"#, r#"2.5"#]),
        (r#"SELECT n FROM mx UNION ALL SELECT f FROM mx"#, vec![r#"1.5"#, r#"2.5"#]),
        (r#"SELECT t FROM mx UNION ALL SELECT vc FROM mx"#, vec![r#"x"#, r#"vc"#]),
        (r#"SELECT t FROM mx UNION ALL SELECT ch FROM mx"#, vec![r#"x"#, r#"c"#]),
        (r#"SELECT vc FROM mx UNION ALL SELECT ch FROM mx"#, vec![r#"vc"#, r#"c"#]),
        (r#"SELECT j FROM mx UNION ALL SELECT j FROM mx"#, vec![r#"{"a": 1}"#, r#"{"a": 1}"#]),
        (r#"SELECT ia FROM mx UNION ALL SELECT ARRAY[3,4]"#, vec![r#"{1,2}"#, r#"{3,4}"#]),
        (r#"SELECT * FROM mx UNION ALL SELECT * FROM mx"#, vec![r#"1|7|8|1.50|2.5|x|vc|c  |t|2026-01-02|2026-01-02 03:04:05|2026-01-02 03:04:05+00|03:04:05|11111111-1111-1111-1111-111111111111|{"a": 1}|\x01|{1,2}|{p,q}"#, r#"1|7|8|1.50|2.5|x|vc|c  |t|2026-01-02|2026-01-02 03:04:05|2026-01-02 03:04:05+00|03:04:05|11111111-1111-1111-1111-111111111111|{"a": 1}|\x01|{1,2}|{p,q}"#]),
        (r#"SELECT id, t FROM mx UNION ALL SELECT m.id, m.t FROM mx m JOIN mx o ON o.id = m.id"#, vec![r#"1|x"#, r#"1|x"#]),
        (r#"SELECT id, n FROM t32 UNION ALL SELECT a, b FROM t64 ORDER BY 1"#, vec![r#"1|7"#, r#"5000000000|6000000000"#]),
        (r#"SELECT id, name FROM t32 UNION ALL SELECT a, NULL FROM t64 UNION ALL SELECT 9, 'k' ORDER BY 1"#, vec![r#"1|x"#, r#"9|k"#, r#"5000000000|NULL"#]),
        (r#"SELECT name FROM t32 WHERE id = 99 UNION ALL SELECT name FROM t32"#, vec![r#"x"#]),
        (r#"SELECT i FROM mx UNION ALL SELECT '5'"#, vec![r#"7"#, r#"5"#]),
        (r#"SELECT d FROM mx UNION ALL SELECT '2026-05-05'"#, vec![r#"2026-01-02"#, r#"2026-05-05"#]),
        (r#"SELECT bo FROM mx UNION ALL SELECT 'false'"#, vec![r#"t"#, r#"f"#]),
        (r#"SELECT n FROM mx UNION ALL SELECT '2.75'"#, vec![r#"1.50"#, r#"2.75"#]),
        (r#"SELECT u FROM mx UNION ALL SELECT '22222222-2222-2222-2222-222222222222'"#, vec![r#"11111111-1111-1111-1111-111111111111"#, r#"22222222-2222-2222-2222-222222222222"#]),
        (r#"SELECT ts FROM mx UNION ALL SELECT '2026-05-05 01:02:03'"#, vec![r#"2026-01-02 03:04:05"#, r#"2026-05-05 01:02:03"#]),
        (r#"SELECT tz FROM mx UNION ALL SELECT '2026-05-05 01:02:03+00'"#, vec![r#"2026-01-02 03:04:05+00"#, r#"2026-05-05 01:02:03+00"#]),
        (r#"SELECT f FROM mx UNION ALL SELECT '1e3'"#, vec![r#"2.5"#, r#"1000"#]),
        (r#"SELECT b FROM mx UNION ALL SELECT '9000000000'"#, vec![r#"8"#, r#"9000000000"#]),
        (r#"SELECT tm FROM mx UNION ALL SELECT '10:11:12'"#, vec![r#"03:04:05"#, r#"10:11:12"#]),
        (r#"SELECT by FROM mx UNION ALL SELECT '\x02'"#, vec![r#"\x01"#, r#"\x02"#]),
        (r#"SELECT ia FROM mx UNION ALL SELECT '{3,4}'"#, vec![r#"{1,2}"#, r#"{3,4}"#]),
        (r#"SELECT ta FROM mx UNION ALL SELECT '{r,s}'"#, vec![r#"{p,q}"#, r#"{r,s}"#]),
        (r#"SELECT j FROM mx UNION ALL SELECT '{"b":2}'"#, vec![r#"{"a": 1}"#, r#"{"b": 2}"#]),
        (r#"SELECT i, d, t FROM mx UNION ALL SELECT '5', '2026-05-05', 'lit' UNION ALL SELECT NULL, NULL, NULL"#, vec![r#"7|2026-01-02|x"#, r#"5|2026-05-05|lit"#, r#"NULL|NULL|NULL"#]),
        (r#"SELECT d FROM mx EXCEPT SELECT '2026-01-02'"#, vec![]),
        (r#"SELECT bo FROM mx INTERSECT SELECT 'true'"#, vec![r#"t"#]),
        (r#"SELECT i FROM mx UNION ALL SELECT '5' FROM mx"#, vec![r#"7"#, r#"5"#]),
        (r#"SELECT d FROM mx UNION ALL SELECT '2026-05-05' FROM mx"#, vec![r#"2026-01-02"#, r#"2026-05-05"#]),
        (r#"SELECT i, d FROM mx UNION ALL SELECT '5' AS a, '2026-05-05' AS b FROM mx WHERE id = 1"#, vec![r#"7|2026-01-02"#, r#"5|2026-05-05"#]),
        (r#"SELECT i, t FROM mx UNION ALL SELECT NULL, 'total'"#, vec![r#"7|x"#, r#"NULL|total"#]),
        (r#"SELECT d::timestamp FROM mx UNION ALL SELECT ts FROM mx"#, vec![r#"2026-01-02 00:00:00"#, r#"2026-01-02 03:04:05"#]),
        (r#"SELECT d FROM mx UNION ALL SELECT ts::date FROM mx"#, vec![r#"2026-01-02"#, r#"2026-01-02"#]),
        (r#"SELECT t FROM mx UNION ALL SELECT i::text FROM mx"#, vec![r#"x"#, r#"7"#]),
        (r#"SELECT d FROM mx UNION ALL SELECT DATE '2026-03-03'"#, vec![r#"2026-01-02"#, r#"2026-03-03"#]),
        (r#"SELECT ts FROM mx UNION ALL SELECT TIMESTAMP '2026-03-03 01:00:00'"#, vec![r#"2026-01-02 03:04:05"#, r#"2026-03-03 01:00:00"#]),
        (r#"SELECT bo FROM mx UNION ALL SELECT i > 3 FROM mx"#, vec![r#"t"#, r#"t"#]),
        (r#"SELECT i FROM mx UNION ALL SELECT count(*) FROM mx"#, vec![r#"7"#, r#"1"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(run(&pool, sql).await, Ok(expected.iter().map(|r| r.to_string()).collect()), "{sql}");
    }

    // What is refused, in PostgreSQL's words: columns, a join's, a `*`'s and
    // a constant's, under UNION, INTERSECT and EXCEPT.
    let refused: Vec<(&str, &str)> = vec![
        (r#"SELECT i FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types integer and text cannot be matched"#),
        (r#"SELECT t FROM mx UNION ALL SELECT i FROM mx"#, r#"UNION types text and integer cannot be matched"#),
        (r#"SELECT i FROM mx UNION ALL SELECT bo FROM mx"#, r#"UNION types integer and boolean cannot be matched"#),
        (r#"SELECT bo FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types boolean and text cannot be matched"#),
        (r#"SELECT d FROM mx UNION ALL SELECT i FROM mx"#, r#"UNION types date and integer cannot be matched"#),
        (r#"SELECT d FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types date and text cannot be matched"#),
        (r#"SELECT t FROM mx UNION ALL SELECT d FROM mx"#, r#"UNION types text and date cannot be matched"#),
        (r#"SELECT ts FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types timestamp without time zone and text cannot be matched"#),
        (r#"SELECT tm FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types time without time zone and text cannot be matched"#),
        (r#"SELECT u FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types uuid and text cannot be matched"#),
        (r#"SELECT j FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types jsonb and text cannot be matched"#),
        (r#"SELECT by FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types bytea and text cannot be matched"#),
        (r#"SELECT bo FROM mx UNION ALL SELECT i FROM mx"#, r#"UNION types boolean and integer cannot be matched"#),
        (r#"SELECT n FROM mx UNION ALL SELECT t FROM mx"#, r#"UNION types numeric and text cannot be matched"#),
        (r#"SELECT f FROM mx UNION ALL SELECT bo FROM mx"#, r#"UNION types double precision and boolean cannot be matched"#),
        (r#"SELECT u FROM mx UNION ALL SELECT i FROM mx"#, r#"UNION types uuid and integer cannot be matched"#),
        (r#"SELECT name FROM t32 UNION ALL SELECT b FROM t64"#, r#"UNION types text and bigint cannot be matched"#),
        (r#"SELECT b FROM t64 UNION ALL SELECT name FROM t32"#, r#"UNION types bigint and text cannot be matched"#),
        (r#"SELECT id, name FROM t32 UNION ALL SELECT a, b FROM t64 ORDER BY 1"#, r#"UNION types text and bigint cannot be matched"#),
        (r#"SELECT name FROM t32 INTERSECT SELECT b FROM t64"#, r#"INTERSECT types text and bigint cannot be matched"#),
        (r#"SELECT id FROM t32 EXCEPT SELECT name FROM t32"#, r#"EXCEPT types integer and text cannot be matched"#),
        (r#"SELECT id, t FROM mx UNION ALL SELECT m.id, o.b FROM mx m JOIN mx o ON o.id = m.id"#, r#"UNION types text and bigint cannot be matched"#),
        (r#"SELECT t FROM mx UNION ALL SELECT 5"#, r#"UNION types text and integer cannot be matched"#),
        (r#"SELECT i FROM mx UNION ALL SELECT true"#, r#"UNION types integer and boolean cannot be matched"#),
        (r#"SELECT *, n FROM t32 UNION ALL SELECT a, b, c, d FROM t64"#, r#"UNION types text and bigint cannot be matched"#),
        (r#"SELECT * FROM t32 UNION ALL SELECT a, b, c FROM t64"#, r#"UNION types text and bigint cannot be matched"#),
    ];
    for (sql, message) in refused {
        assert_eq!(run(&pool, sql).await, Err(("42804".to_string(), message.to_string())), "{sql}");
    }
    // Inside a CTE and a derived table the same, under the name of what
    // failed.
    for sql in [
        "WITH u AS (SELECT name FROM t32 UNION ALL SELECT b FROM t64) SELECT count(*) FROM u",
        "SELECT count(*) FROM (SELECT id FROM t32 UNION ALL SELECT name FROM t32) q",
    ] {
        let (code, message) = run(&pool, sql).await.expect_err(sql);
        assert_eq!(code, "42804", "{sql}: {message}");
        assert!(message.ends_with("cannot be matched"), "{sql}: {message}");
    }
    // An array column's elements are not typed here: the family is told,
    // the element is not (`text[]` where PostgreSQL says `integer[]`).
    for sql in ["SELECT ia FROM mx UNION ALL SELECT t FROM mx", "SELECT ia FROM mx UNION ALL SELECT i FROM mx"] {
        assert_eq!(run(&pool, sql).await.expect_err(sql).0, "42804", "{sql}");
    }
    // A time beside a date or a timestamp: PostgreSQL's "could not convert".
    for sql in ["SELECT tm FROM mx UNION ALL SELECT ts FROM mx", "SELECT d FROM mx UNION ALL SELECT tm FROM mx"] {
        assert_eq!(run(&pool, sql).await.expect_err(sql).0, "42804", "{sql}");
    }

    // Refused here where PostgreSQL answers, and said so: never garbage.
    for sql in [
        "SELECT d FROM mx UNION ALL SELECT ts FROM mx",
        "SELECT d FROM mx UNION ALL SELECT tz FROM mx",
        "SELECT d FROM mx UNION ALL SELECT d::timestamp FROM mx",
        "SELECT '5' UNION ALL SELECT i FROM mx",
        "SELECT '2026-05-05' UNION ALL SELECT d FROM mx",
    ] {
        let (code, message) = run(&pool, sql).await.expect_err(sql);
        assert_eq!(code, "0A000", "{sql}: {message}");
    }
    // A constant that is no value of its column's type: PostgreSQL's 22P02.
    assert_eq!(run(&pool, "SELECT i FROM mx UNION ALL SELECT 'abc'").await.expect_err("abc").0, "22P02");

    // A refused statement leaves the tables as they were.
    assert_eq!(run(&pool, "SELECT count(*) FROM t32").await, Ok(vec!["1".to_string()]));
}
