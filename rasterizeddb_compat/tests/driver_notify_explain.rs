//! `LISTEN`/`NOTIFY`, `EXPLAIN`, the source of each result column in its
//! `RowDescription`, and the constraint an integrity error names: what
//! drivers read beyond rows. Each was missing (a syntax error, or table 0
//! column 0 and `text` for a `varchar(20)`, or no constraint field). The
//! expected answers are PostgreSQL 17's (`.claude/reviews/drivers`).

use futures_util::StreamExt;
use rasterizeddb_compat::TestServer;
use tokio_postgres::types::Type;
use tokio_postgres::{AsyncMessage, SimpleQueryMessage};

/// A client whose connection's notifications arrive on the returned queue.
async fn listening_client(
    server: &TestServer,
) -> (
    tokio_postgres::Client,
    tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
) {
    let (client, mut conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut messages = futures_util::stream::poll_fn(move |cx| conn.poll_message(cx));
        while let Some(Ok(message)) = messages.next().await {
            if let AsyncMessage::Notification(n) = message {
                let _ = tx.send(n);
            }
        }
    });
    (client, rx)
}

async fn next_note(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
) -> Option<(String, String)> {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .ok()
        .flatten()
        .map(|n| (n.channel().to_string(), n.payload().to_string()))
}

#[tokio::test(flavor = "multi_thread")]
async fn notifications_reach_listeners_when_their_transaction_commits() {
    let server = TestServer::start().await.expect("server");
    let (listener, mut notes) = listening_client(&server).await;
    let (sender, mut own) = listening_client(&server).await;
    listener
        .batch_execute("LISTEN jobs; LISTEN \"Mixed\"")
        .await
        .expect("listen");

    // Outside a block: at once, to an idle listener.
    sender
        .batch_execute("NOTIFY jobs, 'one'")
        .await
        .expect("notify");
    assert_eq!(
        next_note(&mut notes).await,
        Some(("jobs".into(), "one".into()))
    );

    // In a block: at its commit, once per distinct payload; a rolled-back
    // block sends nothing.
    sender
        .batch_execute("BEGIN; NOTIFY jobs, 'two'; NOTIFY jobs, 'two'; NOTIFY \"Mixed\", 'm'")
        .await
        .expect("in block");
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_millis(300), notes.recv())
            .await
            .ok()
            .flatten()
            .map(|n| n.payload().to_string()),
        None
    );
    sender.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(
        next_note(&mut notes).await,
        Some(("jobs".into(), "two".into()))
    );
    assert_eq!(
        next_note(&mut notes).await,
        Some(("Mixed".into(), "m".into()))
    );
    sender
        .batch_execute("BEGIN; NOTIFY jobs, 'lost'; ROLLBACK")
        .await
        .expect("rolled back");

    // The function form, and a listener hearing its own.
    sender
        .batch_execute("LISTEN jobs")
        .await
        .expect("listen own");
    let rows = sender
        .simple_query("SELECT pg_notify('jobs', 'three')")
        .await
        .expect("pg_notify");
    assert!(rows.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))));
    assert_eq!(
        next_note(&mut notes).await,
        Some(("jobs".into(), "three".into()))
    );
    assert_eq!(
        next_note(&mut own).await,
        Some(("jobs".into(), "three".into()))
    );

    // UNLISTEN: nothing more on that channel.
    listener
        .batch_execute("UNLISTEN *")
        .await
        .expect("unlisten");
    sender
        .batch_execute("NOTIFY jobs, 'four'")
        .await
        .expect("notify");
    assert_eq!(
        next_note(&mut own).await,
        Some(("jobs".into(), "four".into()))
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_millis(300), notes.recv())
            .await
            .ok()
            .flatten()
            .map(|n| n.payload().to_string()),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn columns_name_their_table_and_declared_type() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, name VARCHAR(20), price NUMERIC(10,2)); \
         INSERT INTO items VALUES (1, 'a', 1.50)",
    )
    .await
    .expect("setup");
    let oid: i64 = c
        .query_one("SELECT oid FROM pg_class WHERE relname = 'items'", &[])
        .await
        .expect("oid")
        .get(0);
    let stmt = c
        .prepare("SELECT id, name AS label, price, id + 1 FROM items")
        .await
        .expect("prepare");
    let columns = stmt.columns();
    assert_eq!(columns[0].table_oid(), Some(oid as u32));
    assert_eq!(columns[0].column_id(), Some(1));
    assert_eq!(columns[1].column_id(), Some(2));
    assert_eq!(*columns[1].type_(), Type::VARCHAR);
    assert_eq!(columns[2].column_id(), Some(3));
    // A computed column belongs to no table.
    assert_eq!(columns[3].table_oid(), None);
    let row = c.query_one(&stmt, &[]).await.expect("rows");
    assert_eq!(row.get::<_, String>(1), "a");

    // The constraint a unique violation names, as its own field.
    let e = c
        .batch_execute("INSERT INTO items VALUES (1, 'b', 2)")
        .await
        .expect_err("duplicate");
    let db = e.as_db_error().expect("a server error");
    assert_eq!(db.code().code(), "23505");
    assert_eq!(db.constraint(), Some("items_pkey"));
    assert!(
        db.message()
            .starts_with("duplicate key value violates unique constraint \"items_pkey\""),
        "{}",
        db.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_describes_the_engines_plan() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, x TEXT); CREATE TABLE b (aid BIGINT, n INT); \
         INSERT INTO a VALUES (1, 'p'), (2, 'q'); INSERT INTO b VALUES (1, 5), (2, 7)",
    )
    .await
    .expect("setup");
    let plan: Vec<String> = c
        .query(
            "EXPLAIN SELECT a.x FROM a JOIN b ON a.id = b.aid WHERE b.n > 6",
            &[],
        )
        .await
        .expect("explain")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert!(plan.iter().any(|l| l.contains("Hash Join")), "{plan:?}");
    assert!(plan.iter().any(|l| l.contains("a.id = b.aid")), "{plan:?}");
    assert!(
        plan.iter().any(|l| l.contains("Filter: (b.n > 6)")),
        "{plan:?}"
    );
    // A comma join plans as the explicit join: the WHERE equality is its
    // hash key, not a test of every pair, and the pin on `a` reaches `b`'s
    // scan across it.
    let plan: Vec<String> = c
        .query("EXPLAIN SELECT a.x FROM a, b WHERE a.id = b.aid AND a.id = 2", &[])
        .await
        .expect("explain a comma join")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert!(plan.iter().any(|l| l.contains("Inner join b: a.id = b.aid")), "{plan:?}");
    assert!(!plan.iter().any(|l| l.contains("every pair is tested")), "{plan:?}");
    assert!(plan.iter().any(|l| l.contains("Scan Filter:") && l.contains("aid = 2")), "{plan:?}");
    let analyzed: Vec<String> = c
        .query("EXPLAIN ANALYZE SELECT * FROM a WHERE id = 2", &[])
        .await
        .expect("explain analyze")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert!(
        analyzed.iter().any(|l| l == "Actual: 1 rows"),
        "{analyzed:?}"
    );
    assert!(
        analyzed.iter().any(|l| l.starts_with("Execution Time:")),
        "{analyzed:?}"
    );
    let e = c
        .simple_query("EXPLAIN (FORMAT JSON) SELECT 1")
        .await
        .expect_err("json");
    assert_eq!(e.code().map(|s| s.code()), Some("0A000"));
}
