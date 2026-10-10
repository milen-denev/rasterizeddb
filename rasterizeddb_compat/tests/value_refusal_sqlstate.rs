//! A value a column cannot hold fails the statement with PostgreSQL's SQLSTATE
//! and a message naming the column and the value.
//!
//! Every refusal used to reach the client as XX000 "Cannot insert row due to
//! schema mismatch" — and several types (timestamp, uuid, inet, boolean, the
//! PostgreSQL wire types) did not refuse at all, storing an empty value or
//! `false` and reporting success.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(
        "CREATE TABLE v (id INTEGER NOT NULL, i INTEGER NULL, s SMALLINT NULL, b BIGINT NULL, \
         d DOUBLE PRECISION NULL, n NUMERIC(6,2) NULL, ok BOOLEAN NULL, u UUID NULL, \
         ts TIMESTAMP NULL, dt DATE NULL, ip INET NULL, j JSONB NULL, by BYTEA NULL, \
         iv INTERVAL NULL)",
    )
    .await
    .expect("create");
    pool.execute("INSERT INTO v (id) VALUES (1)").await.expect("seed");
    (server, pool)
}

/// `(column, literal, SQLSTATE, text the message must contain)`.
const CASES: &[(&str, &str, &str, &str)] = &[
    ("i", "'abc'", "22P02", r#"invalid input syntax for type integer: "abc""#),
    ("i", "99999999999", "22003", r#"value "99999999999" is out of range for type integer"#),
    ("s", "70000", "22003", "out of range for type smallint"),
    ("b", "'x1'", "22P02", "invalid input syntax for type bigint"),
    ("d", "'nan-ish'", "22P02", "invalid input syntax for type double precision"),
    ("n", "'abc'", "22P02", "invalid input syntax for type numeric"),
    ("ok", "'maybe'", "22P02", r#"invalid input syntax for type boolean: "maybe""#),
    ("u", "'nope'", "22P02", r#"invalid input syntax for type uuid: "nope""#),
    ("ts", "'not a date'", "22007", "invalid input syntax for type timestamp"),
    ("ts", "'2026-13-45 10:00:00'", "22008", "date/time field value out of range"),
    ("dt", "'2026-02-30'", "22008", "date/time field value out of range"),
    ("ts", "'2025-02-29 00:00:00'", "22008", "date/time field value out of range"),
    ("ip", "'999.1.1.1'", "22P02", "invalid input syntax for type inet"),
    ("j", "'{bad'", "22P02", "invalid input syntax for type json"),
    ("by", r"'\'", "22P02", "invalid input syntax for type bytea"),
    ("by", r"'\x0'", "22023", "invalid hexadecimal data: odd number of digits"),
    ("iv", "'forever'", "22007", "invalid input syntax for type interval"),
];

fn check(
    failures: &mut Vec<String>,
    what: &str,
    column: &str,
    result: Result<sqlx::postgres::PgQueryResult, sqlx::Error>,
    code: &str,
    text: &str,
) {
    match result {
        Ok(_) => failures.push(format!("{}: accepted", what)),
        Err(sqlx::Error::Database(e)) => {
            let got = e.code().map(|c| c.to_string()).unwrap_or_default();
            if got != code {
                failures.push(format!("{}: SQLSTATE {} (want {}): {}", what, got, code, e.message()));
            }
            if !e.message().contains(text) {
                failures.push(format!("{}: message {:?} lacks {:?}", what, e.message(), text));
            }
            if !e.message().contains(&format!("column \"{}\"", column)) {
                failures.push(format!("{}: message {:?} does not name the column", what, e.message()));
            }
        }
        Err(e) => failures.push(format!("{}: {}", what, e)),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_values_carry_postgres_sqlstates() {
    let (_s, pool) = setup().await;
    let mut failures = Vec::new();
    for (column, literal, code, text) in CASES {
        let single = format!("INSERT INTO v (id, {}) VALUES (2, {})", column, literal);
        let r = pool.execute(sqlx::raw_sql(AssertSqlSafe(single.clone()))).await;
        check(&mut failures, &single, column, r, code, text);

        let multi = format!("INSERT INTO v (id, {}) VALUES (3, NULL), (4, {})", column, literal);
        let r = pool.execute(sqlx::raw_sql(AssertSqlSafe(multi.clone()))).await;
        check(&mut failures, &multi, column, r, code, text);

        let update = format!("UPDATE v SET {} = {} WHERE id = 1", column, literal);
        let r = pool.execute(sqlx::raw_sql(AssertSqlSafe(update.clone()))).await;
        check(&mut failures, &update, column, r, code, text);
    }
    // Nothing a refused statement touched was stored.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM v")
        .fetch_one(&pool)
        .await
        .expect("count");
    if count != 1 {
        failures.push(format!("{} rows after only refused inserts", count));
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// `INSERT … SELECT` hands each selected value to the same encoder.
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_refusals_carry_the_same_sqlstate() {
    let (_s, pool) = setup().await;
    pool.execute("CREATE TABLE src (k INTEGER NOT NULL, t TEXT NOT NULL)").await.expect("src");
    pool.execute("INSERT INTO src (k, t) VALUES (1, 'abc'), (2, 'nope')").await.expect("seed src");
    let mut failures = Vec::new();
    for (column, key, code, text) in [
        ("i", 1, "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("u", 2, "22P02", r#"invalid input syntax for type uuid: "nope""#),
    ] {
        let sql = format!("INSERT INTO v (id, {}) SELECT 9, t FROM src WHERE k = {}", column, key);
        let r = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.clone()))).await;
        check(&mut failures, &sql, column, r, code, text);
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
