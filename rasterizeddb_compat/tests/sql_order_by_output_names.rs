//! ORDER BY a bare name or a position means the OUTPUT column, before any
//! input column of that name, as PostgreSQL reads it. A label that shadowed a
//! stored column — `SELECT -id AS id … ORDER BY id`, or `ORDER BY 1` — sorted
//! by the stored column: a silent wrong order, under LIMIT (top-N), DISTINCT,
//! DISTINCT ON and a window too. An expression or a qualified name still reads
//! the input. Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn text(row: &sqlx::postgres::PgRow) -> String {
    (0..row.len())
        .map(|i| {
            // A prepared statement's integers come back binary.
            let cell = match row.try_get::<Option<i32>, _>(i) {
                Ok(n) => n.map(|n| n.to_string()),
                Err(_) => row.try_get_unchecked::<Option<String>, _>(i).expect("text"),
            };
            cell.unwrap_or_else(|| "NULL".into())
        })
        .collect::<Vec<_>>()
        .join("|")
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"))
        .iter()
        .map(text)
        .collect()
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE neg (id int PRIMARY KEY, g text, n int); \
         INSERT INTO neg VALUES (1,'b',5),(2,'a',NULL),(3,'c',1)"
            .to_string(),
    )))
    .await
    .expect("setup");
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_label_or_a_position_sorts_by_the_output() {
    let (_server, pool) = setup().await;
    for (sql, expected) in [
        ("SELECT -id AS id FROM neg ORDER BY 1", v(&["-3", "-2", "-1"])),
        ("SELECT -id AS id FROM neg ORDER BY id", v(&["-3", "-2", "-1"])),
        ("SELECT -id AS id FROM neg ORDER BY \"id\"", v(&["-3", "-2", "-1"])),
        ("SELECT -id AS id, g FROM neg ORDER BY 1 DESC", v(&["-1|b", "-2|a", "-3|c"])),
        ("SELECT g AS id FROM neg ORDER BY 1", v(&["a", "b", "c"])),
        ("SELECT upper(g) AS id, id AS k FROM neg ORDER BY 1", v(&["A|2", "B|1", "C|3"])),
        ("SELECT g AS id, id AS g FROM neg ORDER BY g", v(&["b|1", "a|2", "c|3"])),
        ("SELECT g AS \"ID\" FROM neg ORDER BY \"ID\"", v(&["a", "b", "c"])),
        ("SELECT n AS id FROM neg ORDER BY id", v(&["1", "5", "NULL"])),
        ("SELECT n AS id FROM neg ORDER BY id DESC NULLS LAST", v(&["5", "1", "NULL"])),
        ("SELECT n * 2 AS n FROM neg ORDER BY n NULLS FIRST", v(&["NULL", "2", "10"])),
        ("SELECT -id AS id FROM neg WHERE id > 1 ORDER BY id", v(&["-3", "-2"])),
        ("SELECT * FROM (SELECT -id AS id FROM neg ORDER BY id) s", v(&["-3", "-2", "-1"])),
        // Top-N: a LIMIT over the scan's own sort.
        ("SELECT -id AS id FROM neg ORDER BY id LIMIT 2", v(&["-3", "-2"])),
        ("SELECT g AS id FROM neg ORDER BY id LIMIT 2", v(&["a", "b"])),
        ("SELECT g AS id, id AS g FROM neg ORDER BY g LIMIT 2", v(&["b|1", "a|2"])),
        // An expression, a qualified name, or a label no column has: as before.
        ("SELECT -id AS id FROM neg ORDER BY id + 0", v(&["-1", "-2", "-3"])),
        ("SELECT -id AS id FROM neg ORDER BY neg.id", v(&["-1", "-2", "-3"])),
        ("SELECT -id AS \"Id\" FROM neg ORDER BY id", v(&["-1", "-2", "-3"])),
        ("SELECT id, -id AS id2 FROM neg ORDER BY id2", v(&["3|-3", "2|-2", "1|-1"])),
        // A window.
        ("SELECT -id AS id, row_number() OVER (ORDER BY id) FROM neg ORDER BY id", v(&["-3|3", "-2|2", "-1|1"])),
        ("SELECT -id AS id, row_number() OVER (ORDER BY g) AS g FROM neg ORDER BY g", v(&["-2|1", "-1|2", "-3|3"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_sorts_by_the_output_too() {
    let (_server, pool) = setup().await;
    pool.execute("INSERT INTO neg VALUES (4,'A',2)").await.expect("insert");
    for (sql, expected) in [
        ("SELECT DISTINCT -id AS id FROM neg ORDER BY id", v(&["-4", "-3", "-2", "-1"])),
        ("SELECT DISTINCT g AS id FROM neg ORDER BY 1", v(&["A", "a", "b", "c"])),
        ("SELECT DISTINCT g AS id, id AS g FROM neg ORDER BY g", v(&["b|1", "a|2", "c|3", "A|4"])),
        // An expression key under DISTINCT was refused ("not found in
        // schema"); one that shadowed a column sorted by the column.
        ("SELECT DISTINCT upper(g) AS x FROM neg ORDER BY x", v(&["A", "B", "C"])),
        ("SELECT DISTINCT upper(g) AS x FROM neg ORDER BY 1 DESC", v(&["C", "B", "A"])),
        ("SELECT DISTINCT upper(g) AS g FROM neg ORDER BY g DESC", v(&["C", "B", "A"])),
        ("SELECT DISTINCT upper(g) AS id FROM neg ORDER BY id LIMIT 2", v(&["A", "B"])),
        ("SELECT DISTINCT ON (id) -id AS id FROM neg ORDER BY id", v(&["-4", "-3", "-2", "-1"])),
        ("SELECT DISTINCT ON (g) g AS id, id AS g FROM neg ORDER BY g, id DESC", v(&["b|1", "a|2", "c|3", "A|4"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // Still refused: a key the select list does not have.
    let err = sqlx::raw_sql("SELECT DISTINCT upper(g) AS x FROM neg ORDER BY x, n").fetch_all(&pool).await;
    assert!(err.is_err(), "a DISTINCT key outside the select list must be refused");
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_and_repeated_statements_sort_by_the_output() {
    let (_server, pool) = setup().await;
    // Prepared (extended protocol) and repeated, so a cached or learned
    // parse is the one answering by the last run.
    for (sql, expected) in [
        ("SELECT -id AS id FROM neg WHERE id > $1 ORDER BY id", v(&["-3", "-2", "-1"])),
        ("SELECT -id AS id FROM neg WHERE id > $1 ORDER BY 1 LIMIT 2", v(&["-3", "-2"])),
        ("SELECT g AS id, id AS g FROM neg WHERE id > $1 ORDER BY g", v(&["b|1", "a|2", "c|3"])),
        ("SELECT DISTINCT upper(g) AS g FROM neg WHERE id > $1 ORDER BY g DESC", v(&["C", "B", "A"])),
    ] {
        for _ in 0..3 {
            let got: Vec<String> = sqlx::query(AssertSqlSafe(sql.to_string()))
                .bind(0i32)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"))
                .iter()
                .map(text)
                .collect();
            assert_eq!(got, expected, "{sql}");
        }
        let plain = sql.replace("$1", "0");
        for _ in 0..2 {
            assert_eq!(rows(&pool, &plain).await, expected, "{plain}");
        }
    }
}
