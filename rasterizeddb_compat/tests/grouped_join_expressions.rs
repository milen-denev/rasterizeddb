//! A grouped join whose select list computes over the grouping keys:
//! `upper(c.name)`, `c.id + 1`, `coalesce(c.name, '-') || '!'`. Each was
//! refused ("function 'upper' is not supported yet", "column 'id + 1'
//! not found"). The answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grouped_join_computes_over_its_keys() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE gcu (id int, name text); CREATE TABLE gord (oid int, customer_id int, total int);
         INSERT INTO gcu VALUES (1, 'a'), (2, 'b');
         INSERT INTO gord VALUES (10, 1, 5), (11, 1, 7), (12, 2, 1)",
    )
    .await
    .expect("setup");
    assert_eq!(
        rows(&c, "SELECT upper(c.name) n, count(*) FROM gcu c JOIN gord o ON o.customer_id = c.id GROUP BY c.name ORDER BY 1").await,
        ["A|2", "B|1"]
    );
    assert_eq!(
        rows(&c, "SELECT c.id + 1, sum(o.total) FROM gcu c JOIN gord o ON o.customer_id = c.id GROUP BY c.id ORDER BY 1").await,
        ["2|12", "3|1"]
    );
    assert_eq!(
        rows(&c, "SELECT c.id, coalesce(c.name, '-') || '!', count(*) FROM gcu c JOIN gord o ON o.customer_id = c.id GROUP BY c.id, c.name ORDER BY 1").await,
        ["1|a!|2", "2|b!|1"]
    );
    // A column that is no grouping key is PostgreSQL's 42803, inside an
    // expression as alone.
    let err = c
        .simple_query("SELECT upper(o.oid::text), count(*) FROM gcu c JOIN gord o ON o.customer_id = c.id GROUP BY c.name")
        .await
        .expect_err("not grouped");
    assert!(
        err.as_db_error().is_some_and(|d| d.message().contains("must appear in the GROUP BY clause")),
        "{err:?}"
    );
}
