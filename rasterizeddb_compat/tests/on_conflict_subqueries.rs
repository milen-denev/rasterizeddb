//! Subqueries in an `INSERT … ON CONFLICT … DO UPDATE`, which PostgreSQL
//! runs per conflict, over `excluded` and the row that conflicted. The
//! INSERT's own subquery pass ran a `WHERE EXISTS (… excluded.id)` alone
//! ("missing FROM-clause entry for table \"excluded\"") and folded a SET's
//! scalar subquery under a snapshot taken before the conflict. Each answer
//! is PostgreSQL 17's (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

/// The `n` the statement returned, `None` for no row.
async fn returned(c: &tokio_postgres::Client, sql: &str) -> Option<i32> {
    c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).first().map(|r| r.get(0))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conflicts_subqueries_read_excluded_and_the_row() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE ex (id int PRIMARY KEY, n int); CREATE TABLE ex2 (k int PRIMARY KEY, m int); \
         INSERT INTO ex VALUES (1, 1), (2, 2); INSERT INTO ex2 VALUES (1, 10), (3, 30)",
    )
    .await
    .expect("setup");
    let upsert = |id: i32, set: &str| {
        format!("INSERT INTO ex (id, n) VALUES ({id}, 5) ON CONFLICT (id) DO UPDATE SET {set} RETURNING n")
    };
    for (sql, want) in [
        (upsert(1, "n = 11 WHERE EXISTS (SELECT 1 FROM ex2 WHERE ex2.k = excluded.id)"), Some(11)),
        (upsert(2, "n = 12 WHERE EXISTS (SELECT 1 FROM ex2 WHERE ex2.k = excluded.id)"), None),
        (upsert(2, "n = 13 WHERE NOT EXISTS (SELECT 1 FROM ex2 WHERE ex2.k = excluded.id)"), Some(13)),
        (upsert(1, "n = (SELECT m FROM ex2 WHERE ex2.k = excluded.id) + excluded.n"), Some(15)),
        (upsert(1, "n = (SELECT max(n) FROM ex) + 1"), Some(16)),
        (upsert(1, "n = 30 WHERE excluded.id IN (SELECT k FROM ex2)"), Some(30)),
        (upsert(2, "n = 31 WHERE excluded.id IN (SELECT k FROM ex2)"), None),
        (upsert(1, "n = 32 WHERE ex.n IN (SELECT m FROM ex2)"), Some(32)),
    ] {
        assert_eq!(returned(&c, &sql).await, want, "{sql}");
    }
    // The rows' own subqueries are still flattened with the rows.
    assert_eq!(
        returned(&c, "INSERT INTO ex (id, n) VALUES ((SELECT max(k) FROM ex2), 1) ON CONFLICT (id) DO UPDATE SET n = 0 RETURNING n").await,
        Some(1)
    );
    assert_eq!(
        returned(&c, "INSERT INTO ex (id, n) VALUES ((SELECT max(k) FROM ex2), 1) ON CONFLICT (id) DO UPDATE SET n = (SELECT count(*) FROM ex)::int RETURNING n").await,
        Some(3)
    );
    let rows: Vec<(i32, i32)> =
        c.query("SELECT id, n FROM ex ORDER BY id", &[]).await.expect("rows").iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(rows, vec![(1, 32), (2, 13), (3, 3)]);
}
