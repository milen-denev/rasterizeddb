//! A join that would outgrow memory is a statement error (53200), not the end
//! of the server. Every join step built its whole result at once: a cross
//! join over a few hundred thousand rows asked for the full product, the
//! allocation failed and the process aborted, taking every session with it
//! (found on a live server: `SELECT count(*) FROM (SELECT 1 FROM a, a, a
//! LIMIT 1000000) x`). The budget is `RDB_JOIN_MEMORY_MB`, set low here so
//! small tables cross it.

use rasterizeddb_compat::TestServer;
use tokio_postgres::Client;

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_past_its_memory_budget_is_refused_and_the_server_lives() {
    // 1 MB: 262144 row-index slots, read once by the engine on first use.
    // SAFETY: set before the server (and any other thread) starts, in a test
    // binary of its own.
    unsafe { std::env::set_var("RDB_JOIN_MEMORY_MB", "1") };
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    let values: Vec<String> = (1..=300).map(|i| format!("({i}, {})", i % 2)).collect();
    c.batch_execute(&format!(
        "CREATE TABLE jt (id INT PRIMARY KEY, k INT); INSERT INTO jt VALUES {};",
        values.join(", ")
    ))
    .await
    .expect("setup");

    // Within the budget: answered as before. 300 x 300 = 90000 rows of two
    // slots each.
    let n: i64 = c.query_one("SELECT count(*) FROM jt a, jt b", &[]).await.expect("small cross join").get(0);
    assert_eq!(n, 90_000);

    for sql in [
        // A cross join of three: 27 million rows.
        "SELECT count(*) FROM jt a, jt b, jt c",
        "SELECT count(*) FROM jt a CROSS JOIN jt b CROSS JOIN jt c",
        // The shape that took a live server down.
        "SELECT count(*) FROM (SELECT 1 FROM jt a, jt b, jt c LIMIT 1000) x",
        // A key joining many to many: 150 x 150 per key value, twice, then
        // again with a third relation.
        "SELECT count(*) FROM jt a JOIN jt b ON a.k = b.k JOIN jt c ON b.k = c.k",
        // No usable key: every pair is tried against the condition.
        "SELECT count(*) FROM jt a JOIN jt b ON a.id < b.id JOIN jt c ON b.id < c.id",
    ] {
        let e = c.query_one(sql, &[]).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
        assert_eq!(db.code().code(), "53200", "{sql}: {}", db.message());
        assert!(db.message().contains("the join would hold more than"), "{sql}: {}", db.message());
        // Still serving, on the same session and a new one.
        let one: i32 = c.query_one("SELECT 1", &[]).await.expect("same session").get(0);
        assert_eq!(one, 1);
    }
    let fresh = client(&server).await;
    let n: i64 = fresh.query_one("SELECT count(*) FROM jt", &[]).await.expect("new session").get(0);
    assert_eq!(n, 300);
}
