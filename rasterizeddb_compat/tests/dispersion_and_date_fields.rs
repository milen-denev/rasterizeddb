//! Three refusals from rasterizeddb-33's second pass, all PostgreSQL-legal,
//! and two found while fixing them. Every expectation is PostgreSQL 17's
//! (`docker run --rm postgres:17`), output for output.
//!
//! - `SELECT DISTINCT a` in a derived table or a `WITH` could not name its
//!   column ("has a column this cannot name"): `DISTINCT` was read as part of
//!   the first item. `SELECT ALL a` failed as a column named `ALL a`.
//! - `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`
//!   did not exist. Exact inputs are `numeric`, as PostgreSQL's
//!   `numeric_stddev_internal`: exact sums, `select_div_scale`'s places, a
//!   rounded square root. Floats follow `float8_accum` step for step.
//! - `EXTRACT` read only `EPOCH`, and `date_part` did not exist. The calendar
//!   fields are `numeric` from `EXTRACT` (six places for `SECOND`), `double
//!   precision` from `date_part`, in UTC; a computed `GROUP BY` key shows its
//!   display scale, where it showed nine places.
//! - An interval written as a bare number (`'0'`, `'5'`) is seconds; it was
//!   refused as invalid input.

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
async fn dispersion_date_fields_and_derived_distinct_match_postgresql() {
    let root = std::env::temp_dir().join(format!("dispersion_date_fields_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _dir = OnDisk(root.clone());
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE plain_t (a INT, b INT)",
        "INSERT INTO plain_t VALUES (1,1),(1,2),(2,3)",
        "CREATE TABLE ev (id INT, user_id INT, revenue NUMERIC(10,2), f FLOAT8, u NUMERIC, ts TIMESTAMPTZ, d DATE, iv INTERVAL)",
        "CREATE TABLE pev (tenant_id BIGINT NOT NULL, id INT, revenue NUMERIC(10,2), f FLOAT8) PARTITION BY HASH (tenant_id)",
        "INSERT INTO ev VALUES (1,1,0,2.5,1.25,'2024-01-01 10:00:05.123456+00','2024-01-01','1 year 2 mons 3 days 04:05:06.5'),(2,1,10.50,3.5,1249.5,'2024-02-29 23:59:59+00','2023-12-31','-2 days 01:00:00'),(3,2,0,-2.5,-0.004,'2021-01-03 00:00:00+00','2021-01-03','45 minutes'),(4,2,20.00,0.5,99.995,'1999-12-31 12:30:00+00','2000-01-01','00:00:00'),(5,3,NULL,NULL,NULL,NULL,NULL,NULL),(6,3,5.25,1.25,2.675,'2026-11-06 07:08:09.5+00','2026-11-06','100 years')",
        "INSERT INTO pev VALUES (1,1,0,2.5),(1,2,10.50,3.5),(2,3,0,-2.5),(2,4,20.00,0.5),(3,5,NULL,NULL),(3,6,5.25,1.25)",
        "CREATE TABLE ti (id INT, iv INTERVAL)",
        "INSERT INTO ti VALUES (1, '0'), (2, '5'), (3, '1.5'), (4, '-3'), (5, '+7'), (6, ' 10 '), (7, '1 day 5')",
        "CREATE TABLE tz (ts TIMESTAMPTZ, n NUMERIC(10,2))",
        "INSERT INTO tz VALUES ('2024-01-01 10:00:05+00', 1.25), ('2024-01-02 10:00:05+00', 2.50), ('2024-01-08 11:00:05+00', 3.75)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT a FROM (SELECT DISTINCT a FROM plain_t) u ORDER BY a", &["1", "2"]),
        ("SELECT a, n FROM (SELECT DISTINCT a, b % 2 AS n FROM plain_t) u ORDER BY a, n", &["1|0", "1|1", "2|1"]),
        ("SELECT a FROM (SELECT ALL a FROM plain_t) u ORDER BY a", &["1", "1", "2"]),
        ("WITH w AS (SELECT DISTINCT a FROM plain_t) SELECT a FROM w ORDER BY a", &["1", "2"]),
        ("SELECT stddev(revenue), stddev_samp(revenue), stddev_pop(revenue), variance(revenue), var_samp(revenue), var_pop(revenue) FROM ev", &["8.3994047408134822|8.3994047408134822|7.5126559883971794|70.5500000000000000|70.5500000000000000|56.4400000000000000"]),
        ("SELECT stddev(f), stddev_pop(f), variance(f), var_pop(f) FROM ev", &["2.2940139493908926|2.0518284528683193|5.2625|4.21"]),
        ("SELECT stddev(u), variance(u), stddev(id), variance(id), var_pop(user_id) FROM ev", &["548.842189147937|301227.748588700000|1.8708286933869707|3.5000000000000000|0.66666666666666666667"]),
        ("SELECT user_id, stddev(revenue), variance(f) FROM ev GROUP BY user_id ORDER BY user_id", &["1|7.4246212024587490|0.5", "2|14.1421356237309505|4.5", "3|NULL|NULL"]),
        ("SELECT ROUND(STDDEV(revenue), 2), ROUND(VARIANCE(revenue)::numeric, 4) FROM ev", &["8.40|70.5500"]),
        ("SELECT stddev(revenue) FROM ev WHERE id = 1", &["NULL"]),
        ("SELECT stddev_pop(revenue), var_pop(revenue) FROM ev WHERE id = 1", &["0|0"]),
        ("SELECT stddev(revenue) FROM ev WHERE id > 100", &["NULL"]),
        ("SELECT stddev(DISTINCT user_id) FROM ev", &["1.00000000000000000000"]),
        ("SELECT user_id FROM ev GROUP BY user_id HAVING stddev(revenue) > 5 ORDER BY user_id", &["1", "2"]),
        ("SELECT stddev(revenue), variance(f) FROM pev", &["8.3994047408134822|5.2625"]),
        ("SELECT id, EXTRACT(YEAR FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(DAY FROM ts), EXTRACT(HOUR FROM ts), EXTRACT(MINUTE FROM ts), EXTRACT(SECOND FROM ts) FROM ev ORDER BY id", &["1|2024|1|1|10|0|5.123456", "2|2024|2|29|23|59|59.000000", "3|2021|1|3|0|0|0.000000", "4|1999|12|31|12|30|0.000000", "5|NULL|NULL|NULL|NULL|NULL|NULL", "6|2026|11|6|7|8|9.500000"]),
        ("SELECT id, EXTRACT(DOW FROM ts), EXTRACT(ISODOW FROM ts), EXTRACT(DOY FROM ts), EXTRACT(QUARTER FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(ISOYEAR FROM ts) FROM ev ORDER BY id", &["1|1|1|1|1|1|2024", "2|4|4|60|1|9|2024", "3|0|7|3|1|53|2020", "4|5|5|365|4|52|1999", "5|NULL|NULL|NULL|NULL|NULL|NULL", "6|5|5|310|4|45|2026"]),
        ("SELECT id, EXTRACT(MILLISECONDS FROM ts), EXTRACT(MICROSECONDS FROM ts), EXTRACT(DECADE FROM ts), EXTRACT(CENTURY FROM ts), EXTRACT(MILLENNIUM FROM ts) FROM ev ORDER BY id", &["1|5123.456|5123456|202|21|3", "2|59000.000|59000000|202|21|3", "3|0.000|0|202|21|3", "4|0.000|0|199|20|2", "5|NULL|NULL|NULL|NULL|NULL", "6|9500.000|9500000|202|21|3"]),
        ("SELECT id, date_part('dow', ts), date_part('year', ts), date_part('second', ts), date_part('epoch', ts), date_part('hour', d) FROM ev ORDER BY id", &["1|1|2024|5.123456|1704103205.123456|0", "2|4|2024|59|1709251199|0", "3|0|2021|0|1609632000|0", "4|5|1999|0|946643400|0", "5|NULL|NULL|NULL|NULL|NULL", "6|5|2026|9.5|1793948889.5|0"]),
        ("SELECT id, EXTRACT(YEAR FROM d), EXTRACT(DOW FROM d), EXTRACT(WEEK FROM d) FROM ev ORDER BY id", &["1|2024|1|1", "2|2023|0|52", "3|2021|0|53", "4|2000|6|52", "5|NULL|NULL|NULL", "6|2026|5|45"]),
        ("SELECT id, EXTRACT(YEAR FROM iv), EXTRACT(MONTH FROM iv), EXTRACT(DAY FROM iv), EXTRACT(HOUR FROM iv), EXTRACT(MINUTE FROM iv), EXTRACT(SECOND FROM iv) FROM ev ORDER BY id", &["1|1|2|3|4|5|6.500000", "2|0|0|-2|1|0|0.000000", "3|0|0|0|0|45|0.000000", "4|0|0|0|0|0|0.000000", "5|NULL|NULL|NULL|NULL|NULL|NULL", "6|100|0|0|0|0|0.000000"]),
        ("SELECT EXTRACT(DOW FROM ts) AS dow, COUNT(*) FROM ev GROUP BY dow ORDER BY dow", &["0|1", "1|1", "4|1", "5|2", "NULL|1"]),
        ("SELECT id FROM ev WHERE date_part('year', ts) = 2024 ORDER BY id", &["1", "2"]),
        ("SELECT ALL a FROM plain_t ORDER BY a", &["1", "1", "2"]),
        ("SELECT a FROM (SELECT ALL a FROM plain_t) u ORDER BY a", &["1", "1", "2"]),
        ("SELECT a % 2 AS k, COUNT(*) FROM plain_t GROUP BY k ORDER BY k", &["0|1", "1|2"]),
        ("SELECT EXTRACT(DOW FROM ts) AS dow, COUNT(*) FROM tz GROUP BY dow ORDER BY dow", &["1|2", "2|1"]),
        ("SELECT EXTRACT(SECOND FROM ts) AS s, COUNT(*) FROM tz GROUP BY s ORDER BY s", &["5.000000|3"]),
        ("SELECT round(n, 1) AS r, COUNT(*) FROM tz GROUP BY r ORDER BY r", &["1.3|1", "2.5|1", "3.8|1"]),
        ("SELECT n * 2 AS d, COUNT(*) FROM tz GROUP BY d ORDER BY d DESC", &["7.50|1", "5.00|1", "2.50|1"]),
        ("SELECT date_trunc('week', ts) AS w, COUNT(*) FROM tz GROUP BY w ORDER BY w", &["2024-01-01 00:00:00+00|2", "2024-01-08 00:00:00+00|1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    let refused: &[(&str, &str)] = &[
        ("SELECT stddev(ts) FROM ev", "function stddev(timestamp with time zone) does not exist"),
        ("SELECT EXTRACT(HOUR FROM d) FROM ev", "unit \"hour\" not supported for type date"),
        ("SELECT EXTRACT(DOW FROM iv) FROM ev", "unit \"dow\" not supported for type interval"),
        ("SELECT EXTRACT(FOO FROM ts) FROM ev", "unit \"foo\" not recognized for type timestamp with time zone"),
        ("SELECT stddev(ts) FROM tz", "function stddev(timestamp with time zone) does not exist"),
    ];
    for (sql, want) in refused {
        let e = c.simple_query(sql).await.expect_err(sql);
        let message = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string());
        assert_eq!(message, *want, "{sql}");
    }
}
