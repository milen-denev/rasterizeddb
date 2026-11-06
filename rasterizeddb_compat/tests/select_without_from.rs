//! A `SELECT` with no `FROM` (`rql::no_from`), and `EXTRACT` in a `WHERE`.
//! Every expectation is PostgreSQL 17's (`docker run --rm postgres:17`).
//!
//! Only `SELECT 1` was answered; `SELECT 2`, `SELECT 1 + 1`, `SELECT now()`,
//! `SELECT '2024-02-29'::date` and every other probe a client or an ORM sends
//! were "syntax error or unsupported query", and `(SELECT 7)` as a value was
//! "cannot parse subquery". `EXTRACT(HOUR FROM ts) = 9` in a `WHERE` was "unknown
//! column or unsupported keyword 'EXTRACT'", where `date_part` worked.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_select_without_from_is_answered_as_postgresql_answers_it() {
    let root = std::env::temp_dir().join(format!("select_without_from_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _dir = OnDisk(root.clone());
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.simple_query("CREATE TABLE ev (id INT, ts TIMESTAMPTZ)").await.unwrap();
    c.simple_query("INSERT INTO ev VALUES (1, '2024-01-01 09:15:00+00'), (2, '2024-01-01 10:15:00+00'), (3, '2024-01-02 09:59:00+00')")
        .await
        .unwrap();
    let cases: &[(&str, &[&str])] = &[
        ("SELECT 1", &["1"]),
        ("SELECT 2", &["2"]),
        ("SELECT 'a'", &["a"]),
        ("SELECT 1 AS x", &["1"]),
        ("SELECT 1, 2", &["1|2"]),
        ("SELECT -5", &["-5"]),
        ("SELECT 1 + 1", &["2"]),
        ("SELECT 0.1 + 0.2", &["0.3"]),
        ("SELECT 0.1::float8", &["0.1"]),
        ("SELECT 0.1::float8 + 0.2::float8", &["0.30000000000000004"]),
        ("SELECT '2024-02-29'::date", &["2024-02-29"]),
        ("SELECT DATE '2024-02-29'", &["2024-02-29"]),
        ("SELECT DATE '2024-02-29' + 1", &["2024-03-01"]),
        ("SELECT INTERVAL '1 day'", &["1 day"]),
        ("SELECT now() > '2020-01-01'", &["t"]),
        ("SELECT EXTRACT(YEAR FROM now()) > 2000", &["t"]),
        ("SELECT date_part('year', DATE '2024-02-29')", &["2024"]),
        ("SELECT EXTRACT(DOW FROM DATE '2024-02-29')", &["4"]),
        ("SELECT EXTRACT(HOUR FROM INTERVAL '25 hours')", &["25"]),
        ("SELECT NULL", &["NULL"]),
        ("SELECT NULL::int", &["NULL"]),
        ("SELECT 'a' || 'b'", &["ab"]),
        ("SELECT upper('abc'), length('hello')", &["ABC|5"]),
        ("SELECT round(2.567, 2), abs(-3)", &["2.57|3"]),
        ("SELECT CASE WHEN 1 > 0 THEN 'y' ELSE 'n' END", &["y"]),
        ("SELECT COALESCE(NULL, 3)", &["3"]),
        ("SELECT 1 WHERE 1 = 1", &["1"]),
        ("SELECT 1 WHERE 1 = 0", &[]),
        ("SELECT 1 LIMIT 0", &[]),
        ("SELECT 7 AS a ORDER BY 1", &["7"]),
        ("SELECT true, false", &["t|f"]),
        ("SELECT 10 % 3, 7 / 2, 7.0 / 2", &["1|3|3.5000000000000000"]),
        ("SELECT TIMESTAMP '2024-01-01 10:00:00'", &["2024-01-01 10:00:00"]),
        ("SELECT '1 day'::interval + INTERVAL '2 hours'", &["1 day 02:00:00"]),
        ("SELECT 'it''s'", &["it's"]),
        ("SELECT DISTINCT 5", &["5"]),
        ("SELECT (SELECT 7)", &["7"]),
        ("SELECT COUNT(*) FROM ev WHERE EXTRACT(HOUR FROM ts) = 9", &["2"]),
        ("SELECT id FROM ev WHERE EXTRACT(DAY FROM ts) = 1 AND EXTRACT(HOUR FROM ts) >= 10 ORDER BY id", &["2"]),
        ("SELECT id FROM ev WHERE EXTRACT(EPOCH FROM ts) > 1704100000 ORDER BY id", &["1", "2", "3"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    for (sql, code) in [("SELECT x", "42703"), ("SELECT 1/0", "22012")] {
        let e = c.simple_query(sql).await.expect_err(sql);
        assert_eq!(e.code().map(|c| c.code()), Some(code), "{sql}: {e}");
    }

    // The extended protocol: described by the same parse that answers it, so
    // a typed client decodes every column, binary results included.
    let row = c.query_one("SELECT 1 + 1 AS two, 0.1::float8 + 0.2::float8 AS f, now() > '2020-01-01' AS later, 'x' || 'y' AS s", &[])
        .await
        .expect("extended FROM-less SELECT");
    assert_eq!(row.get::<_, i32>("two"), 2);
    assert_eq!(row.get::<_, f64>("f"), 0.30000000000000004);
    assert!(row.get::<_, bool>("later"));
    assert_eq!(row.get::<_, String>("s"), "xy");
    let row = c.query_one("SELECT $1::int + 1 AS n", &[&41i32]).await.expect("a bound parameter");
    assert_eq!(row.get::<_, i32>("n"), 42);
    assert!(c.query("SELECT 1 WHERE 1 = 0", &[]).await.expect("no row").is_empty());
}
