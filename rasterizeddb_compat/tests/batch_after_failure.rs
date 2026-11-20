//! A multi-statement query after a failed statement runs all of its
//! statements. A `DROP TABLE` in one is deferred to the batch's end and
//! answered before the per-statement state was reset, so the previous
//! statement's failure still stood and the batch stopped after the drop:
//! `DROP TABLE t; SELECT 'y'` returned no `y` and no error.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_after_a_failure_runs_every_statement() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.batch_execute("CREATE TABLE bf (id int)").await.expect("create");
    assert!(c.simple_query("SELECT 1/0").await.is_err());
    let replies = c.simple_query("DROP TABLE bf; SELECT 'y'").await.expect("batch");
    let rows: Vec<String> = replies
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(rows, ["y"]);
    assert!(c.simple_query("SELECT 1 FROM bf").await.is_err(), "the table was dropped");
}
