//! Small SQL forms an ORM writes, each of which was an explicit error:
//!
//! - `CURRENT_TIME(p)`, `LOCALTIME(p)`, `CURRENT_TIMESTAMP(p)` and
//!   `LOCALTIMESTAMP(p)`, in an expression ("function current_time(integer)
//!   does not exist") and as a column DEFAULT (the CREATE TABLE refused);
//! - the interval units `mil`, `c`, `cent`, `dec` and `mm`;
//! - `date + time`, `time + date` and `date - time`, a `timestamp`;
//! - `convert_to(…, 'LATIN1')` and `convert_from(…, 'LATIN1')`;
//! - `ALTER TABLE … ADD IF NOT EXISTS CONSTRAINT …`, the word order some
//!   tools write (this engine's extension: PostgreSQL has neither order);
//! - `pg_typeof(x) = 'date'::regtype` ("operator does not exist: text =
//!   integer");
//! - a correlated subquery that reads its outer table again under another
//!   alias (`FROM u7 x WHERE x.g = u7.g`, 0A000).
//!
//! And one silent wrong answer met beside them: `sha256('\x616263')` hashed
//! the literal's eight characters, not the three bytes it spells.
//!
//! Every expectation but the `ADD IF NOT EXISTS` one is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn start(setup: &str) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    if !setup.is_empty() {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(setup.to_string()))).await.expect("setup");
    }
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_clock_keywords_take_a_precision() {
    let (_server, pool) = start("").await;
    for (sql, expected) in [
        (
            "SELECT pg_typeof(current_time(2)), pg_typeof(localtime(0)), pg_typeof(current_timestamp(3)), \
             pg_typeof(localtimestamp(1))",
            vec!["time with time zone|time without time zone|timestamp with time zone|timestamp without time zone"],
        ),
        // No fraction at precision 0: `HH:MM:SS`, with `+00` for the zoned.
        (
            "SELECT length(localtime(0)::text), length(current_time(0)::text), length(current_timestamp(0)::text), \
             length(localtimestamp(0)::text)",
            vec!["8|11|22|19"],
        ),
        ("SELECT localtime(0) = localtime(0), LOCALTIME(6) IS NOT NULL", vec!["t|t"]),
        // A precision past six is six.
        ("SELECT length(current_timestamp(3)::text) <= 26, length(current_time(9)::text) <= 18", vec!["t|t"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // As a DEFAULT: the table was refused.
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE dfl (id int, ts timestamptz DEFAULT CURRENT_TIMESTAMP(3), t time DEFAULT LOCALTIME(0),
                           l timestamp DEFAULT localtimestamp(2), c timetz DEFAULT current_time (1));
         INSERT INTO dfl (id) VALUES (1);",
    ))
    .await
    .expect("defaults with a precision");
    assert_eq!(
        rows(&pool, "SELECT length(ts::text) <= 26, length(t::text), length(l::text) <= 22, length(c::text) <= 13 FROM dfl").await,
        ["t|8|t|t"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn interval_units_have_their_abbreviations() {
    let (_server, pool) = start("").await;
    for (sql, expected) in [
        (
            "SELECT '0 s'::interval, '1 mil'::interval, '2 mils'::interval, '1 c'::interval, '3 cent'::interval, \
             '1 dec'::interval, '2 decs'::interval, '5 mm'::interval",
            vec!["00:00:00|1000 years|2000 years|100 years|300 years|10 years|20 years|00:05:00"],
        ),
        ("SELECT interval '1 dec 2 y 3 mon', interval '1.5 dec', interval '1 c 1 mil'", vec![
            "12 years 3 mons|15 years|1100 years",
        ]),
        // The ones that already read, unchanged.
        ("SELECT '5 s'::interval, '2 m'::interval, '3 h'::interval, '1 d'::interval, '2 w'::interval, interval '100 ms'", vec![
            "00:00:05|00:02:00|03:00:00|1 day|14 days|00:00:00.1",
        ]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_date_and_a_time_of_day_make_a_timestamp() {
    let (_server, pool) = start(
        "CREATE TABLE g3 (id int PRIMARY KEY, d date, t time);
         INSERT INTO g3 VALUES (1, '2026-01-02', '10:00'), (2, NULL, '11:30'), (3, '2026-02-03', NULL);",
    )
    .await;
    for (sql, expected) in [
        (
            "SELECT date '2026-01-02' + time '10:00', time '10:00' + date '2026-01-02', \
             pg_typeof(date '2026-01-02' + time '10:00')",
            vec!["2026-01-02 10:00:00|2026-01-02 10:00:00|timestamp without time zone"],
        ),
        ("SELECT '2026-01-02'::date + '10:00:00.5'::time, date '2026-12-31' + time '23:59:59'", vec![
            "2026-01-02 10:00:00.5|2026-12-31 23:59:59",
        ]),
        ("SELECT date '2026-01-02' - time '10:00', pg_typeof(date '2026-01-02' - time '10:00')", vec![
            "2026-01-01 14:00:00|timestamp without time zone",
        ]),
        ("SELECT id, d + t, t + d FROM g3 ORDER BY id", vec![
            "1|2026-01-02 10:00:00|2026-01-02 10:00:00",
            "2|NULL|NULL",
            "3|NULL|NULL",
        ]),
        ("SELECT id FROM g3 WHERE d + t > '2026-01-02 09:00' ORDER BY id", vec!["1"]),
        // A time of day and an interval, as before.
        ("SELECT time '10:00' + interval '1 hour', time '23:30' + interval '45 minutes'", vec!["11:00:00|00:15:00"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // There is no `time - date`.
    assert_eq!(code(&pool, "SELECT time '10:00' - date '2026-01-02'").await, "42883");
}

#[tokio::test(flavor = "multi_thread")]
async fn latin1_converts_and_a_hex_literal_is_its_bytes() {
    let (_server, pool) = start("").await;
    for (sql, expected) in [
        (
            r"SELECT convert_to('héllo', 'LATIN1'), convert_from('\x68e96c6c6f', 'LATIN1'), convert_to('abc', 'latin1'),
                     convert_from(convert_to('ÿé', 'ISO_8859_1'), 'ISO-8859-1')",
            vec![r"\x68e96c6c6f|héllo|\x616263|ÿé"],
        ),
        (r"SELECT convert_from('\x68c3a96c6c6f', 'UTF8'), convert_from('\x68c3a96c6c6f'::bytea, 'UTF8')", vec!["héllo|héllo"]),
        // `sha256('\x616263')` is the hash of `abc`. It hashed the literal's
        // own eight characters, and said nothing.
        (
            r"SELECT encode(sha256('\x616263'), 'hex'), encode(sha256('abc'), 'hex') = encode(sha256('\x616263'), 'hex'),
                     length(sha512('\x00ff'))",
            vec!["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad|t|64"],
        ),
        ("SELECT encode(sha256(''), 'hex')", vec!["e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // A character LATIN1 has no byte for: untranslatable_character.
    assert_eq!(code(&pool, "SELECT convert_to('€', 'LATIN1')").await, "22P05");
}

/// Not PostgreSQL's grammar, which has `IF NOT EXISTS` for neither order:
/// this engine takes it after `CONSTRAINT`, and now before it too.
#[tokio::test(flavor = "multi_thread")]
async fn add_if_not_exists_constraint_in_either_word_order() {
    let (_server, pool) = start("CREATE TABLE g3 (id int PRIMARY KEY, n int); INSERT INTO g3 VALUES (1, 5);").await;
    for sql in [
        "ALTER TABLE g3 ADD IF NOT EXISTS CONSTRAINT g3_n_pos CHECK (n > 0)",
        // Again: the constraint is there, and that is success.
        "ALTER TABLE g3 ADD IF NOT EXISTS CONSTRAINT g3_n_pos CHECK (n > 0)",
        "ALTER TABLE g3 ADD CONSTRAINT IF NOT EXISTS g3_n_pos CHECK (n > 0)",
        "ALTER TABLE g3 ADD IF NOT EXISTS CONSTRAINT g3_n_u UNIQUE (n)",
        "ALTER TABLE g3 ADD IF NOT EXISTS CONSTRAINT g3_n_u UNIQUE (n)",
        // A column still reads as one.
        "ALTER TABLE g3 ADD IF NOT EXISTS extra int",
        "ALTER TABLE g3 ADD COLUMN IF NOT EXISTS extra int",
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(&pool, "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'g3'::regclass ORDER BY conname").await,
        ["g3_n_pos|c", "g3_n_u|u", "g3_pkey|p"]
    );
    assert_eq!(code(&pool, "INSERT INTO g3 (id, n) VALUES (9, -1)").await, "23514");
    assert_eq!(code(&pool, "INSERT INTO g3 (id, n) VALUES (9, 5)").await, "23505");
    assert_eq!(rows(&pool, "SELECT extra FROM g3").await, ["NULL"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_typeof_compares_with_a_regtype() {
    let (_server, pool) = start(
        "CREATE TABLE g3 (id int PRIMARY KEY, d date, t time, n int, ts timestamptz);
         INSERT INTO g3 VALUES (1, '2026-01-02', '10:00', 5, '2026-01-02 03:04:05+00'), (2, NULL, NULL, 6, NULL);",
    )
    .await;
    for (sql, expected) in [
        (
            "SELECT pg_typeof(d) = 'date'::regtype, pg_typeof(n) = 'integer'::regtype, pg_typeof(n) = 'int4'::regtype, \
             pg_typeof(d) = 'text'::regtype FROM g3 WHERE id = 1",
            vec!["t|t|t|f"],
        ),
        (
            "SELECT pg_typeof(ts) = 'timestamptz'::regtype, pg_typeof(ts) = 'timestamp with time zone'::regtype, \
             'time'::regtype = pg_typeof(t), pg_typeof(t) <> 'date'::regtype FROM g3 WHERE id = 1",
            vec!["t|t|t|t"],
        ),
        // The type is the column's, whatever the row holds.
        ("SELECT id FROM g3 WHERE pg_typeof(d) = 'date'::regtype ORDER BY id", vec!["1", "2"]),
        (
            "SELECT id FROM g3 WHERE pg_typeof(n) IN ('bigint'::regtype, 'integer'::regtype) \
             AND pg_typeof(n) NOT IN ('text'::regtype) ORDER BY id",
            vec!["1", "2"],
        ),
        (
            "SELECT pg_typeof(n + 1) = 'int4'::regtype, pg_typeof(n::bigint) = 'int8'::regtype, \
             pg_typeof(1.5) = 'numeric'::regtype, pg_typeof('x'::text) = 'text'::regtype, \
             pg_typeof(true) = 'bool'::regtype FROM g3 WHERE id = 1",
            vec!["t|t|t|t|t"],
        ),
        ("SELECT CASE WHEN pg_typeof(d) = 'date'::regtype THEN 'd' ELSE 'x' END FROM g3 WHERE id = 1", vec!["d"]),
        // On its own a regtype is what it was: its name shown, its oid cast.
        ("SELECT 'date'::regtype, 'int4'::regtype::oid", vec!["date|23"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subquery_reads_its_outer_table_again_under_another_alias() {
    let (_server, pool) = start(
        "CREATE TABLE u7 (id int PRIMARY KEY, g int, v int);
         INSERT INTO u7 VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, NULL, 5);",
    )
    .await;
    for (sql, expected) in [
        ("SELECT id, (SELECT max(x.v) FROM u7 x WHERE x.g = u7.g) FROM u7 ORDER BY id", vec![
            "1|20", "2|20", "3|30", "4|NULL",
        ]),
        ("SELECT id FROM u7 WHERE v = (SELECT max(x.v) FROM u7 x WHERE x.g = u7.g) ORDER BY id", vec!["2", "3"]),
        ("SELECT id FROM u7 WHERE EXISTS (SELECT 1 FROM u7 x WHERE x.id = u7.id AND x.v > 15) ORDER BY id", vec!["2", "3"]),
        (
            "SELECT u7.id, l.m FROM u7, LATERAL (SELECT max(x.v) AS m FROM u7 x WHERE x.g = u7.g) l ORDER BY u7.id",
            vec!["1|20", "2|20", "3|30", "4|NULL"],
        ),
        (
            "SELECT u7.id, l.c FROM u7 CROSS JOIN LATERAL (SELECT count(*) AS c FROM u7 AS x WHERE x.g = u7.g) l ORDER BY 1",
            vec!["1|2", "2|2", "3|1", "4|0"],
        ),
        (
            "SELECT u7.id, l.c FROM u7 LEFT JOIN LATERAL \
             (SELECT count(*) AS c FROM u7 x JOIN u7 y ON y.id = x.id WHERE x.g = u7.g) l ON true ORDER BY 1",
            vec!["1|2", "2|2", "3|1", "4|0"],
        ),
        ("SELECT id, (SELECT count(*) FROM u7 x WHERE x.g = u7.g AND x.v < u7.v) FROM u7 ORDER BY id", vec![
            "1|0", "2|1", "3|0", "4|0",
        ]),
        ("SELECT id, (SELECT x.v FROM u7 x WHERE x.id = u7.id + 1) FROM u7 ORDER BY id", vec![
            "1|20", "2|30", "3|5", "4|NULL",
        ]),
        // Unaliased inside, the name is the inner table's: a count of the
        // whole group 1, the same for every outer row.
        (
            "SELECT u7.id, l.c FROM u7 CROSS JOIN LATERAL (SELECT count(*) AS c FROM u7 WHERE u7.g = 1) l ORDER BY 1",
            vec!["1|2", "2|2", "3|2", "4|2"],
        ),
        // The outer aliased, the inner bare.
        ("SELECT a.id, (SELECT count(*) FROM u7 WHERE u7.g = a.g) FROM u7 a ORDER BY a.id", vec![
            "1|2", "2|2", "3|1", "4|0",
        ]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}
