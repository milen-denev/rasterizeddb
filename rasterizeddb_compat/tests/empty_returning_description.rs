//! A RETURNING that returns no rows still describes them.
//!
//! PostgreSQL answers a writing statement with RETURNING with a
//! RowDescription even when no row qualifies, then `INSERT 0 0` (or `UPDATE
//! 0`, `DELETE 0`). An `INSERT … SELECT … RETURNING` whose query found nothing
//! sent only the command tag, so a client that builds its columns from the
//! description saw no columns at all. Found by rasterizeddb-eb's twin; the
//! simple protocol is where it shows, so that is what this speaks.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_returning_still_sends_its_row_description() {
    let server = TestServer::start().await.expect("server");
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
        .batch_execute(
            "CREATE TABLE src (tenant_id BIGINT NOT NULL, n INT);
             CREATE TABLE rq (tenant_id BIGINT NOT NULL, id BIGSERIAL, n INT, PRIMARY KEY (tenant_id, id));
             CREATE TABLE jt (id INT PRIMARY KEY, tenant_id BIGINT NOT NULL, n INT);
             INSERT INTO src VALUES (2, 1), (8, 2), (11, 3);
             INSERT INTO jt VALUES (1, 2, 0), (2, 8, 0);",
        )
        .await
        .expect("schema");

    for sql in [
        "INSERT INTO rq (tenant_id, n) SELECT tenant_id, n FROM src WHERE tenant_id > 100 RETURNING id",
        "INSERT INTO rq (tenant_id, n) SELECT 5, 1 WHERE 1 = 0 RETURNING id",
        "UPDATE rq SET n = 0 WHERE tenant_id > 100 RETURNING id",
        "DELETE FROM rq WHERE tenant_id > 100 RETURNING id",
        // Through a join, which writes back row by row (found by eb's S19 twin).
        "UPDATE jt SET n = 1 FROM src WHERE jt.tenant_id = src.tenant_id AND src.tenant_id > 100 RETURNING jt.id",
        "DELETE FROM jt USING src WHERE jt.tenant_id = src.tenant_id AND src.tenant_id > 100 RETURNING jt.id",
    ] {
        let messages = client.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let described: Vec<String> = messages
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::RowDescription(columns) => {
                    Some(columns.iter().map(|c| c.name().to_string()).collect())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{sql}: no RowDescription in {} messages", messages.len()));
        assert_eq!(described, ["id"], "{sql}");
        assert!(!messages.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))), "{sql}: rows");
        assert!(
            messages.iter().any(|m| matches!(m, SimpleQueryMessage::CommandComplete(0))),
            "{sql}: no CommandComplete(0)"
        );
    }
}
