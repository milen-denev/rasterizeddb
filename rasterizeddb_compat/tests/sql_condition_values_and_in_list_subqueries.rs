//! A condition used as a value, and a scalar subquery as an IN-list item.
//!
//! `(a = b) IS NULL`, `(a > 0) = true`, `false = (a = b)` and `(b + 0) IS
//! NULL` in a WHERE were refused ("syntax error at or near IS", "unexpected
//! Op("=")"); ORMs write them. `= false` keeps SQL's NULL: it is `NOT (P)`,
//! not `IS FALSE`, which differs under a NOT.
//!
//! `x IN ((SELECT …), 3)` was refused, and with a correlated subquery first
//! in the list it answered true for every row in a select list. `IN ((SELECT
//! …))` is PostgreSQL's `IN (SELECT …)`. Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"))
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn sorted(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let mut r = rows(pool, sql).await;
    r.sort();
    r
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_condition_tested_as_a_value() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE bv (id int PRIMARY KEY, a int, b int, s text, t text, f boolean); \
         INSERT INTO bv VALUES (1,1,1,'x','x',true),(2,1,2,'x','y',false),(3,NULL,1,NULL,'y',NULL),(4,1,NULL,'x',NULL,true),(5,3,2,'b','a',false)"
            .to_string(),
    )))
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT id FROM bv WHERE (a = b) IS NULL ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (a = b) IS NOT NULL ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE NOT ((a = b) IS NULL) ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE (s = t) IS NULL ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (s < t) IS NOT NULL ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE (s LIKE 'x%') IS NULL ORDER BY 1", v(&["3"])),
        ("SELECT id FROM bv WHERE (a = b AND s = t) IS NULL ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (a = b OR s = t) IS NOT NULL ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE (a IN (b, 5)) IS NULL ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (a > 0) = true ORDER BY 1", v(&["1", "2", "4", "5"])),
        ("SELECT id FROM bv WHERE (a = b) = false ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE (a = b) <> true ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE (a = b) != false ORDER BY 1", v(&["1"])),
        ("SELECT id FROM bv WHERE NOT ((a = b) = false) ORDER BY 1", v(&["1"])),
        ("SELECT id FROM bv WHERE NOT ((a = b) = true) ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE NOT ((a = b) <> false) ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE (s = t) = false ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE (s LIKE 'x%') = true ORDER BY 1", v(&["1", "2", "4"])),
        ("SELECT id FROM bv WHERE (s LIKE 'x%') = false ORDER BY 1", v(&["5"])),
        ("SELECT id FROM bv WHERE true = (a = b) ORDER BY 1", v(&["1"])),
        ("SELECT id FROM bv WHERE false = (a = b) ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE NOT (false = (a = b)) ORDER BY 1", v(&["1"])),
        ("SELECT id FROM bv WHERE (a = b) = false OR id = 4 ORDER BY 1", v(&["2", "4", "5"])),
        ("SELECT id FROM bv WHERE id > 1 AND (a = b) IS NULL ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (a = b) IS NULL OR (s = t) = true ORDER BY 1", v(&["1", "3", "4"])),
        ("SELECT id FROM bv WHERE (a IS NULL) = true ORDER BY 1", v(&["3"])),
        ("SELECT id FROM bv WHERE (a IS NULL) = false ORDER BY 1", v(&["1", "2", "4", "5"])),
        ("SELECT id FROM bv WHERE (f) = true ORDER BY 1", v(&["1", "4"])),
        ("SELECT id FROM bv WHERE f = false ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM bv WHERE (b + 0) IS NULL ORDER BY 1", v(&["4"])),
        ("SELECT id FROM bv WHERE (a * b) IS NOT NULL ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE NOT ((a - b) IS NULL) ORDER BY 1", v(&["1", "2", "5"])),
        ("SELECT id FROM bv WHERE (s) IS NULL ORDER BY 1", v(&["3"])),
        ("SELECT id FROM bv WHERE (a = b) IS UNKNOWN ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM bv WHERE (a = b) IS TRUE ORDER BY 1", v(&["1"])),
        ("SELECT id, CASE WHEN (a = b) IS NULL THEN 'n' WHEN (a = b) = false THEN 'f' ELSE 't' END FROM bv ORDER BY 1", v(&["1|t", "2|f", "3|n", "4|n", "5|f"])),
        ("SELECT id, (a = b) IS NULL, (a = b) IS NOT NULL, (a > 0) = true, (a = b) = false, (s = t) <> true, (b + 0) IS NULL FROM bv ORDER BY 1", v(&["1|f|t|t|f|f|f", "2|f|t|t|t|t|f", "3|t|f|NULL|NULL|NULL|f", "4|t|f|t|NULL|NULL|t", "5|f|t|t|t|t|f"])),
        ("SELECT count(*) FILTER (WHERE (a = b) IS NULL), count(*) FILTER (WHERE (a = b) = false) FROM bv", v(&["2|2"])),
        ("SELECT id FROM bv x WHERE EXISTS (SELECT 1 FROM bv y WHERE (y.a = x.b) = true AND y.id <> x.id) ORDER BY 1", v(&["1", "3"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    assert_eq!(sorted(&pool, "UPDATE bv SET t = t WHERE (a = b) IS NULL RETURNING id").await, v(&["3", "4"]));
    assert_eq!(sorted(&pool, "UPDATE bv SET b = b WHERE NOT ((a = b) = false) RETURNING id").await, v(&["1"]));
    assert_eq!(sorted(&pool, "DELETE FROM bv WHERE (a = b) = false RETURNING id").await, v(&["2", "5"]));
    assert_eq!(rows(&pool, "SELECT id FROM bv ORDER BY 1").await, v(&["1", "3", "4"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scalar_subquery_as_an_in_list_item() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE w4 (id int PRIMARY KEY, b int, s text); INSERT INTO w4 VALUES (1,1,'x'),(2,5,'y'),(3,NULL,NULL),(4,2,'x')"
            .to_string(),
    )))
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT id FROM w4 WHERE id IN ((SELECT max(id) FROM w4), 1) ORDER BY 1", v(&["1", "4"])),
        ("SELECT id FROM w4 WHERE id NOT IN ((SELECT min(id) FROM w4), 3) ORDER BY 1", v(&["2", "4"])),
        ("SELECT id FROM w4 WHERE b IN ((SELECT max(id) FROM w4), 1) ORDER BY 1", v(&["1"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4 WHERE id = 99), 1) ORDER BY 1", v(&["1"])),
        ("SELECT id FROM w4 WHERE id NOT IN ((SELECT b FROM w4 WHERE id = 99), 1) ORDER BY 1", v(&[])),
        ("SELECT id FROM w4 WHERE s IN ((SELECT max(s) FROM w4), 'z') ORDER BY 1", v(&["2"])),
        ("SELECT id FROM w4 WHERE id IN (3, (SELECT max(id) FROM w4)) ORDER BY 1", v(&["3", "4"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) ORDER BY 1", v(&["1", "3"])),
        ("SELECT id FROM w4 WHERE id NOT IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) ORDER BY 1", v(&["2", "4"])),
        ("SELECT id FROM w4 WHERE NOT (id IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3)) ORDER BY 1", v(&["2", "4"])),
        ("SELECT id, id IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) FROM w4 ORDER BY 1", v(&["1|t", "2|f", "3|t", "4|f"])),
        ("SELECT id, id NOT IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) FROM w4 ORDER BY 1", v(&["1|f", "2|t", "3|f", "4|t"])),
        ("SELECT id, id IN ((SELECT b FROM w4 x WHERE x.id = w4.id) + 0, 3) FROM w4 ORDER BY 1", v(&["1|t", "2|f", "3|t", "4|f"])),
        ("SELECT id, id IN ((SELECT max(id) FROM w4), 1) FROM w4 ORDER BY 1", v(&["1|t", "2|f", "3|f", "4|t"])),
        ("SELECT id, id IN ((SELECT 7), 2) FROM w4 ORDER BY 1", v(&["1|f", "2|t", "3|f", "4|f"])),
        ("SELECT id, id IN ((SELECT b FROM w4 x WHERE x.id = w4.id)) FROM w4 ORDER BY 1", v(&["1|t", "2|f", "3|NULL", "4|f"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4)) ORDER BY 1", v(&["1", "2"])),
        ("SELECT id FROM w4 WHERE id IN (((SELECT b FROM w4))) ORDER BY 1", v(&["1", "2"])),
        ("SELECT id FROM w4 WHERE id NOT IN ((SELECT b FROM w4 WHERE b IS NOT NULL)) ORDER BY 1", v(&["3", "4"])),
        ("SELECT id, id IN ((SELECT b FROM w4)) FROM w4 ORDER BY 1", v(&["1|t", "2|t", "3|NULL", "4|NULL"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4 x WHERE x.b = w4.id)) ORDER BY 1", v(&["1", "2"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT max(id) FROM w4), (SELECT min(id) FROM w4)) ORDER BY 1", v(&["1", "4"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4 x WHERE x.id = w4.id), (SELECT 4)) ORDER BY 1", v(&["1", "4"])),
        ("SELECT id FROM w4 WHERE id IN ((SELECT b FROM w4)) AND id IN ((SELECT max(id) FROM w4), 1) ORDER BY 1", v(&["1"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // More than one row from a list item is the scalar subquery's error.
    let e = sqlx::raw_sql("SELECT id FROM w4 WHERE id IN ((SELECT id FROM w4 WHERE id > 1), 1)").fetch_all(&pool).await;
    assert!(e.is_err(), "a two-row list item must be refused");
    assert_eq!(sorted(&pool, "UPDATE w4 SET s = s WHERE id IN ((SELECT max(id) FROM w4), 1) RETURNING id").await, v(&["1", "4"]));
    assert_eq!(
        sorted(&pool, "UPDATE w4 SET s = 'u' WHERE id IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) RETURNING id").await,
        v(&["1", "3"])
    );
    assert_eq!(
        sorted(&pool, "DELETE FROM w4 WHERE id NOT IN ((SELECT b FROM w4 x WHERE x.id = w4.id), 3) RETURNING id").await,
        v(&["2", "4"])
    );
    assert_eq!(rows(&pool, "SELECT id, s FROM w4 ORDER BY 1").await, v(&["1|u", "3|u"]));
}
