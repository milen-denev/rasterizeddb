//! An expression over GROUP BY keys in the select list, against PostgreSQL
//! 17's answers.
//!
//! `SELECT r * 3 ... GROUP BY r`, `upper(name) ... GROUP BY name`, a CASE over
//! a key: one value per group, which PostgreSQL allows; refused as a column
//! that "must appear in GROUP BY" (42601, about '?column?'). It becomes a key
//! of its own, which groups nothing finer. An expression over a column that
//! is no key is still refused, now with PostgreSQL's 42803 and the column's
//! name.

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
async fn an_expression_over_grouping_keys_has_one_value_per_group() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE g1 (id int PRIMARY KEY, r int, name text, amt numeric(10,2));
         INSERT INTO g1 VALUES (1, 2, 'Ann', 1.50), (2, 3, 'bob', 2.25), (3, 3, 'Bob', 4.00), (4, NULL, NULL, 1.00);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT r * 3 FROM g1 GROUP BY r ORDER BY 1", &["6", "9", "NULL"]),
        ("SELECT id, r * 3 FROM g1 GROUP BY id, r ORDER BY 1", &["1|6", "2|9", "3|9", "4|NULL"]),
        ("SELECT r, r * 3, count(*) FROM g1 GROUP BY r ORDER BY 1", &["2|6|1", "3|9|2", "NULL|NULL|1"]),
        ("SELECT upper(name), count(*) FROM g1 GROUP BY name ORDER BY 1", &["ANN|1", "BOB|1", "BOB|1", "NULL|1"]),
        ("SELECT r + 1 AS nxt, sum(amt) FROM g1 GROUP BY r ORDER BY nxt DESC", &["NULL|1.00", "4|6.25", "3|1.50"]),
        ("SELECT r * 2, count(*) FROM g1 GROUP BY r HAVING count(*) > 1", &["6|2"]),
        ("SELECT coalesce(r, 0) * 10, count(*) FROM g1 GROUP BY r ORDER BY 1", &["0|1", "20|1", "30|2"]),
        ("SELECT name || '!', r FROM g1 GROUP BY name, r ORDER BY 1, 2", &["Ann!|2", "Bob!|3", "bob!|3", "NULL|NULL"]),
        ("SELECT id * 10, name FROM g1 GROUP BY id ORDER BY 1", &["10|Ann", "20|bob", "30|Bob", "40|NULL"]),
        ("SELECT CASE WHEN r > 2 THEN 'big' ELSE 'small' END, count(*) FROM g1 GROUP BY r ORDER BY 2, 1", &["small|1", "small|1", "big|2"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
    let refused: &[(&str, &str)] = &[
        ("SELECT r * 3 FROM g1 GROUP BY name", "42803"),
        ("SELECT amt * 2 FROM g1 GROUP BY r", "42803"),
    ];
    for (sql, code) in refused {
        let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(&pool).await.expect_err(sql);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some(*code), "{sql}");
    }
}
