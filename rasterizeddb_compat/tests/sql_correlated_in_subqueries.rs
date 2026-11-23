//! A correlated subquery after IN, a common shape, was refused ("subquery
//! failed: missing FROM-clause entry for table a"): an IN subquery is run
//! once and its rows substituted, which one reading the outer row cannot
//! be. It is answered as the EXISTS it means (`executor::
//! correlated_in_as_exists`), with PostgreSQL's NULL rule for NOT IN and
//! the three-valued answer where the IN is a value rather than the WHERE's
//! own condition. Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const SETUP: &str = "CREATE TABLE ca (id int PRIMARY KEY, g text, n int); \
     CREATE TABLE cb (id int PRIMARY KEY, g text, a_id int); \
     INSERT INTO ca VALUES (1,'a',0),(2,'a',0),(3,'b',0),(4,'n',0),(5,NULL,0); \
     INSERT INTO cb VALUES (10,'a',1),(11,'b',3),(12,'z',2),(13,'n',NULL),(14,'n',9)";

async fn start() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(SETUP.to_string()))).await.expect("setup");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    if !sql.contains("ORDER BY") {
        out.sort();
    }
    out
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_in_subquery_in_a_select() {
    let (_server, pool) = start().await;
    for (sql, expected) in [
        ("SELECT id FROM ca WHERE id IN (SELECT cb.a_id FROM cb WHERE cb.g = ca.g) ORDER BY 1", v(&["1", "3"])),
        ("SELECT id FROM ca a WHERE a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) ORDER BY 1", v(&["1", "3"])),
        // A self-join: the bare `id` is the outer row's, not the subquery's.
        ("SELECT id FROM ca a WHERE id IN (SELECT o.id FROM ca o WHERE o.g = a.g) ORDER BY 1", v(&["1", "2", "3", "4"])),
        // NOT IN: a NULL among the rows (group 'n') keeps 4 out; an empty
        // subquery (row 5's NULL g) lets 5 in.
        ("SELECT id FROM ca a WHERE a.id NOT IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) ORDER BY 1", v(&["2", "5"])),
        (
            "SELECT id FROM ca a WHERE a.id NOT IN (SELECT b.a_id FROM cb b WHERE b.g = a.g AND b.a_id IS NOT NULL) ORDER BY 1",
            v(&["2", "4", "5"]),
        ),
        // As values: all three.
        (
            "SELECT id, a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) AS m, \
             a.id NOT IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) AS nm FROM ca a ORDER BY 1",
            v(&["1|t|f", "2|f|t", "3|t|f", "4|NULL|NULL", "5|f|t"]),
        ),
        // Under a NOT: the NULL of row 4 stays not true.
        ("SELECT id FROM ca a WHERE NOT (a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g)) ORDER BY 1", v(&["2", "5"])),
        ("SELECT id FROM ca a WHERE a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) OR a.id = 5 ORDER BY 1", v(&["1", "3", "5"])),
        ("SELECT id FROM ca a WHERE id IN (SELECT o.id FROM ca o WHERE o.g = a.g) EXCEPT SELECT 2 ORDER BY 1", v(&["1", "3", "4"])),
        (
            "SELECT id FROM ca a WHERE a.id + 0 IN (SELECT b.a_id FROM cb b WHERE b.g = a.g AND b.id > a.id) ORDER BY 1",
            v(&["1", "3"]),
        ),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_in_subquery_in_an_update_and_a_delete() {
    let (_server, pool) = start().await;
    assert_eq!(
        rows(&pool, "UPDATE ca a SET n = 1 WHERE a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) RETURNING id").await,
        v(&["1", "3"])
    );
    assert_eq!(
        rows(&pool, "UPDATE ca SET n = 2 WHERE id NOT IN (SELECT cb.a_id FROM cb WHERE cb.g = ca.g) RETURNING id").await,
        v(&["2", "5"])
    );
    assert_eq!(rows(&pool, "SELECT id, n FROM ca ORDER BY 1").await, v(&["1|1", "2|2", "3|1", "4|0", "5|2"]));
    assert_eq!(
        rows(&pool, "DELETE FROM ca a WHERE a.id IN (SELECT b.a_id FROM cb b WHERE b.g = a.g) RETURNING id").await,
        v(&["1", "3"])
    );
    assert_eq!(
        rows(&pool, "DELETE FROM ca WHERE id NOT IN (SELECT cb.a_id FROM cb WHERE cb.g = ca.g) RETURNING id").await,
        v(&["2", "5"])
    );
    assert_eq!(rows(&pool, "SELECT id FROM ca ORDER BY 1").await, v(&["4"]));
}
