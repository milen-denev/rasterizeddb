//! An EXISTS, or an ARRAY(…), over a set operation. `EXISTS (SELECT 1 EXCEPT
//! SELECT 2)` was refused ("the SELECT inside EXISTS has no FROM clause");
//! `ARRAY(SELECT 2 UNION ALL SELECT 1 ORDER BY 1)` was taken for a scalar
//! subquery ("returned 2 rows"); a correlated EXISTS over a UNION — a branch
//! with FROM beside one without — was answered as a scalar over its first
//! branch. Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

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

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn uncorrelated_set_operations_under_exists_and_array() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for (sql, expected) in [
        ("SELECT EXISTS (SELECT 1 EXCEPT SELECT 1)", "f"),
        ("SELECT EXISTS (SELECT 1 EXCEPT SELECT 2)", "t"),
        ("SELECT EXISTS (SELECT 1 INTERSECT SELECT 1)", "t"),
        ("SELECT NOT EXISTS (SELECT 1 UNION SELECT 2)", "f"),
        ("SELECT EXISTS (SELECT 1 WHERE false)", "f"),
        ("SELECT ARRAY(SELECT 2 UNION ALL SELECT 1 ORDER BY 1)", "{1,2}"),
        ("SELECT ARRAY(SELECT 'b' UNION SELECT 'a' ORDER BY 1)", "{a,b}"),
        ("SELECT ARRAY(SELECT 1)", "{1}"),
        ("SELECT ARRAY(SELECT 1 WHERE false)", "{}"),
    ] {
        assert_eq!(rows(&pool, sql).await, v(&[expected]), "{sql}");
    }
    // `ORDER BY 1` over a table names the one select item; it was read
    // inside the aggregate as the constant 1 and the order was lost.
    pool.execute("CREATE TABLE z (x int, y int); INSERT INTO z VALUES (2,1),(1,2),(3,NULL)").await.expect("setup");
    for (sql, expected) in [
        ("SELECT ARRAY(SELECT x FROM z ORDER BY 1)", "{1,2,3}"),
        ("SELECT ARRAY(SELECT x * 10 FROM z ORDER BY 1 DESC NULLS LAST)", "{30,20,10}"),
        ("SELECT ARRAY(SELECT y FROM z ORDER BY 1 NULLS FIRST)", "{NULL,1,2}"),
        ("SELECT ARRAY(SELECT x FROM z ORDER BY y, 1)", "{2,1,3}"),
        ("SELECT ARRAY(SELECT x FROM (VALUES (2),(1)) v(x) WHERE x > 0 ORDER BY 1)", "{1,2}"),
    ] {
        assert_eq!(rows(&pool, sql).await, v(&[expected]), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_exists_over_a_union_asks_each_branch() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        "CREATE TABLE ca (id int PRIMARY KEY, g text, n int); CREATE TABLE cb (id int PRIMARY KEY, cid int); \
         INSERT INTO ca VALUES (1,'a',0),(2,'a',0),(3,'b',0),(4,'n',0),(5,NULL,0); INSERT INTO cb VALUES (1,3),(2,4)"
            .to_string(),
    )))
    .await
    .expect("setup");
    for (sql, expected) in [
        // A branch with FROM beside a FROM-less one.
        (
            "SELECT id FROM ca c WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION ALL SELECT 1 WHERE c.id = 5) ORDER BY 1",
            v(&["1", "2", "5"]),
        ),
        (
            "SELECT id FROM ca c WHERE NOT EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION ALL SELECT 1 WHERE c.id = 5) ORDER BY 1",
            v(&["3", "4"]),
        ),
        (
            "SELECT id FROM ca c WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION ALL SELECT 1 WHERE c.g = 'n') ORDER BY 1",
            v(&["1", "2", "4"]),
        ),
        // Both branches read a table, both correlated.
        (
            "SELECT id FROM ca c WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION SELECT 1 FROM cb WHERE cb.cid = c.id) ORDER BY 1",
            v(&["1", "2", "3", "4"]),
        ),
        (
            "SELECT id FROM ca c WHERE NOT EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION SELECT 1 FROM cb WHERE cb.cid = c.id) ORDER BY 1",
            v(&["5"]),
        ),
        // The union's own ORDER BY decides nothing; the FROM-less branch's
        // WHERE meets a NULL.
        (
            "SELECT id FROM ca c WHERE EXISTS (SELECT 1 FROM cb WHERE cb.cid = c.id UNION ALL SELECT 1 WHERE c.g IS NULL ORDER BY 1) ORDER BY 1",
            v(&["3", "4", "5"]),
        ),
        // As a value.
        (
            "SELECT id, EXISTS (SELECT 1 FROM cb WHERE cb.cid = c.id UNION ALL SELECT 1 WHERE c.id = 1) FROM ca c ORDER BY 1",
            v(&["1|t", "2|f", "3|t", "4|t", "5|f"]),
        ),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    pool.execute("UPDATE ca c SET n = 1 WHERE EXISTS (SELECT 1 FROM cb WHERE cb.cid = c.id UNION ALL SELECT 1 WHERE c.id = 1)")
        .await
        .expect("update");
    assert_eq!(rows(&pool, "SELECT id, n FROM ca ORDER BY 1").await, v(&["1|1", "2|0", "3|1", "4|1", "5|0"]));
    pool.execute(
        "DELETE FROM ca c WHERE NOT EXISTS (SELECT 1 FROM ca o WHERE o.g = c.g AND o.id <> c.id UNION ALL SELECT 1 FROM cb WHERE cb.cid = c.id)",
    )
    .await
    .expect("delete");
    assert_eq!(rows(&pool, "SELECT id FROM ca ORDER BY 1").await, v(&["1", "2", "3", "4"]));
}
