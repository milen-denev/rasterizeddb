//! INTERSECT over NUMERICs equal at different scales — 2.25 and 2.2500 — shows
//! the digits of the input PostgreSQL reads first: the one with fewer distinct
//! rows (the left on a tie). The left's were always shown, so `… INTERSECT
//! SELECT 2, 2.2500` printed 2.25 where PostgreSQL prints 2.2500. UNION and
//! EXCEPT keep the left's, as they did. Every expectation is PostgreSQL 17's
//! (its tables analysed, so its estimates are the counts).

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

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn intersect_shows_the_digits_of_the_input_with_fewer_distinct_rows() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE isrc (id int PRIMARY KEY, g text, v numeric(10,2)); \
         INSERT INTO isrc VALUES (1,'a',1.50),(2,'a',2.25),(3,'b',NULL); \
         CREATE TABLE big (id int PRIMARY KEY, v numeric(12,5)); \
         INSERT INTO big SELECT g, 2.25 FROM generate_series(1, 200) g"
            .to_string(),
    )))
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT id, v FROM isrc INTERSECT SELECT 2, 9.0 / 4 ORDER BY 1", v(&["2|2.2500000000000000"])),
        ("SELECT id, v FROM isrc INTERSECT SELECT 2, 2.2500 ORDER BY 1", v(&["2|2.2500"])),
        ("SELECT id, v FROM isrc INTERSECT SELECT id, v::numeric(12,5) FROM isrc WHERE id = 2 ORDER BY 1", v(&["2|2.25000"])),
        ("SELECT id, v FROM isrc INTERSECT ALL SELECT 2, 9.0 / 4 ORDER BY 1", v(&["2|2.2500000000000000"])),
        ("SELECT 2, 9.0 / 4 INTERSECT SELECT id, v FROM isrc ORDER BY 1", v(&["2|2.2500000000000000"])),
        // The same two inputs in either order: the one-row side's digits.
        ("SELECT id, v FROM isrc WHERE id = 2 INTERSECT SELECT id, v FROM big ORDER BY 1", v(&["2|2.25"])),
        ("SELECT id, v FROM big INTERSECT SELECT id, v FROM isrc WHERE id = 2 ORDER BY 1", v(&["2|2.25"])),
        // Distinct rows, not rows: big's one value against isrc's three.
        ("SELECT v FROM isrc INTERSECT SELECT v FROM big", v(&["2.25000"])),
        ("SELECT v FROM big INTERSECT SELECT v FROM isrc", v(&["2.25000"])),
        // A tie keeps the left's; chained, each step decides again.
        ("SELECT 2.25 AS v INTERSECT SELECT 2.2500", v(&["2.25"])),
        ("SELECT 2.2500 AS v INTERSECT SELECT 2.25", v(&["2.2500"])),
        ("SELECT g, v FROM isrc INTERSECT SELECT 'a', 2.2500 INTERSECT SELECT 'a', 2.250 ORDER BY 1", v(&["a|2.2500"])),
        // UNION and EXCEPT keep the left's, unchanged.
        ("SELECT id, v FROM isrc UNION SELECT 2, 9.0 / 4 ORDER BY 1, 2", v(&["1|1.50", "2|2.25", "3|NULL"])),
        ("SELECT 2, 9.0 / 4 UNION SELECT id, v FROM isrc ORDER BY 1, 2", v(&["1|1.50", "2|2.2500000000000000", "3|NULL"])),
        ("SELECT id, v FROM isrc EXCEPT SELECT 1, 1.5000 ORDER BY 1", v(&["2|2.25", "3|NULL"])),
        ("SELECT 2, 2.2500 EXCEPT SELECT id, v FROM isrc WHERE id = 1", v(&["2|2.2500"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}
