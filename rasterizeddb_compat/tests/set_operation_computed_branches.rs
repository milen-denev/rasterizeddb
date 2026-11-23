//! A set operation with a branch that has no `FROM` and computes its value
//! (`SELECT 9, round(2.567, 1) UNION ALL SELECT id, v FROM t`), as PostgreSQL
//! 17 answers it. Beside a NUMERIC column it was refused (0A000, "of a type
//! this cannot tell"): only a constant in such a branch had a type. An
//! expression is typed as the statement run alone types it, and its NUMERIC
//! cells are read at the scale that statement writes them at.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in the
//! same order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

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

#[tokio::test(flavor = "multi_thread")]
async fn a_computed_branch_with_no_from_answers_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE src (id int PRIMARY KEY, g text, v numeric(10,2), w numeric, b bigint, f double precision)"#, vec![]),
        (r#"INSERT INTO src VALUES (1, 'a', 1.50, 1.123456789012, 5000000000, 1.5), (2, 'a', 2.25, NULL, 7, 2.25), (3, 'b', NULL, 7, NULL, NULL)"#, vec![]),
        (r#"SELECT 9, round(2.567, 1) UNION ALL SELECT id, v FROM src ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.6"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, round(2.567, 1) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.6"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, round(2.567, 3) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.567"#]),
        (r#"SELECT id, w FROM src UNION ALL SELECT 9, round(2.567, 1) ORDER BY 1"#, vec![r#"1|1.123456789012"#, r#"2|NULL"#, r#"3|7"#, r#"9|2.6"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, 1.5 + 2.25 ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|3.75"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, 2 * 3 ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|6"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, 10 / 4.0 ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.5000000000000000"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, abs(-7.125) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|7.125"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, 2.5::numeric(10,4) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.5000"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, CAST(3 AS numeric) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|3"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, NULLIF(1.5, 1.5) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|NULL"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, COALESCE(NULL, 4.75) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|4.75"#]),
        (r#"SELECT id, v FROM src UNION SELECT 1, round(1.4999, 2) ORDER BY 1, 2"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#]),
        (r#"SELECT id, v FROM src EXCEPT SELECT 1, round(1.4999, 2) ORDER BY 1"#, vec![r#"2|2.25"#, r#"3|NULL"#]),
        (r#"SELECT id, v FROM src EXCEPT SELECT 2, 2.250 + 0 ORDER BY 1"#, vec![r#"1|1.50"#, r#"3|NULL"#]),
        (r#"SELECT id, w FROM src EXCEPT SELECT 3, 3.5 * 2 ORDER BY 1"#, vec![r#"1|1.123456789012"#, r#"2|NULL"#]),
        (r#"SELECT id, b FROM src UNION ALL SELECT 9, 1 + 1 ORDER BY 1"#, vec![r#"1|5000000000"#, r#"2|7"#, r#"3|NULL"#, r#"9|2"#]),
        (r#"SELECT id, b FROM src UNION ALL SELECT 9, 2.5 * 2 ORDER BY 1"#, vec![r#"1|5000000000"#, r#"2|7"#, r#"3|NULL"#, r#"9|5.0"#]),
        (r#"SELECT id, id FROM src UNION ALL SELECT 9, 3000000000 + 1 ORDER BY 1"#, vec![r#"1|1"#, r#"2|2"#, r#"3|3"#, r#"9|3000000001"#]),
        (r#"SELECT id, f FROM src UNION ALL SELECT 9, sqrt(16) ORDER BY 1"#, vec![r#"1|1.5"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|4"#]),
        (r#"SELECT id, f FROM src UNION ALL SELECT 9, round(2.567, 1) ORDER BY 1"#, vec![r#"1|1.5"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2.6"#]),
        (r#"SELECT id, g FROM src UNION ALL SELECT 9, upper('x') || 'y' ORDER BY 1"#, vec![r#"1|a"#, r#"2|a"#, r#"3|b"#, r#"9|Xy"#]),
        (r#"SELECT id, g FROM src UNION ALL SELECT 9, lower('Q') ORDER BY 1"#, vec![r#"1|a"#, r#"2|a"#, r#"3|b"#, r#"9|q"#]),
        (r#"SELECT 9 AS id, round(2.567, 1) AS r UNION ALL SELECT id, w FROM src ORDER BY 1"#, vec![r#"1|1.123456789012"#, r#"2|NULL"#, r#"3|7"#, r#"9|2.6"#]),
        (r#"SELECT 9 AS id, 1 + 1 AS r UNION ALL SELECT id, v FROM src ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|2"#]),
        (r#"SELECT 9, round(2.567, 1) UNION ALL SELECT 8, round(1.23456, 4) ORDER BY 1"#, vec![r#"8|1.2346"#, r#"9|2.6"#]),
        (r#"SELECT 9, round(2.567, 1) UNION SELECT 9, 2.6 ORDER BY 1"#, vec![r#"9|2.6"#]),
        (r#"SELECT sum(x) FROM (SELECT v AS x FROM src UNION ALL SELECT round(2.567, 1)) u"#, vec![r#"6.35"#]),
        (r#"SELECT x FROM (SELECT v AS x FROM src UNION ALL SELECT round(2.567, 2) UNION ALL SELECT 10 / 3.0) u ORDER BY 1"#, vec![r#"1.50"#, r#"2.25"#, r#"2.57"#, r#"3.3333333333333333"#, r#"NULL"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}
