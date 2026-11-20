//! `x [NOT] IN (…)` whose `x` is a literal, a call or arithmetic rather than
//! a column: a syntax error in every statement before ("syntax error at or
//! near \"IN\""), which a subquery folded to its value (`excluded.id IN
//! (SELECT …)`) reached too. Each answer is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

async fn ids(c: &tokio_postgres::Client, sql: &str) -> Vec<i32> {
    c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).iter().map(|r| r.get(0)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn in_takes_any_value_on_its_left() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE iv (id int PRIMARY KEY, n int, s text); CREATE TABLE iv2 (k int PRIMARY KEY); \
         INSERT INTO iv VALUES (1, 1, 'a'), (2, 2, NULL); INSERT INTO iv2 VALUES (1), (3)",
    )
    .await
    .expect("setup");
    for (predicate, want) in [
        ("1 IN (1, 3)", vec![1, 2]),
        ("2 IN (1, 3)", vec![]),
        ("2 NOT IN (1, 3)", vec![1, 2]),
        // Unknown either way: a NULL in the list.
        ("2 NOT IN (1, NULL)", vec![]),
        ("NOT (2 NOT IN (1, NULL))", vec![]),
        ("'a' IN ('a', 'b') AND id = 1", vec![1]),
        ("1 IN (SELECT k FROM iv2)", vec![1, 2]),
        ("n + 0 IN (1, 3)", vec![1]),
        ("n + 0 NOT IN (1, 3)", vec![2]),
        ("upper(s) IN ('A')", vec![1]),
        ("upper(s) NOT IN ('A')", vec![]),
        ("(n * 2) IN (2, 4)", vec![1, 2]),
        // A column's own IN is the scan's, as before.
        ("id IN (1, 3)", vec![1]),
        ("id NOT IN (SELECT k FROM iv2)", vec![2]),
    ] {
        assert_eq!(ids(&c, &format!("SELECT id FROM iv WHERE {predicate} ORDER BY id")).await, want, "{predicate}");
    }
    c.batch_execute("DELETE FROM iv WHERE 5 IN (1, 3); UPDATE iv SET n = 20 WHERE 1 IN (1, 3) AND id = 1")
        .await
        .expect("writes");
    assert_eq!(ids(&c, "SELECT n FROM iv ORDER BY id").await, vec![20, 2]);
}
