//! `now()` is the transaction's start, `statement_timestamp()` the
//! statement's, `clock_timestamp()` the wall clock at each call — as
//! PostgreSQL 17 answers each case here (`docker run --rm postgres:17`).
//!
//! All of them read the wall clock per evaluation, so
//! `INSERT … VALUES (now()), (now())` stored a different time in each row, a
//! `DEFAULT now()` column disagreed with a `now()` written beside it, and two
//! statements of one `BEGIN` block saw two different "now"s.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The first row's cells, as text.
async fn one(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no row"))
}

/// Long enough that two wall-clock readings either side differ.
async fn pause() {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

const TABLE: &str = "CREATE TABLE nw (k INT PRIMARY KEY, t TIMESTAMPTZ DEFAULT now(), u TIMESTAMPTZ, \
     s TIMESTAMPTZ, c TIMESTAMPTZ, d TIMESTAMPTZ DEFAULT statement_timestamp(), w TIMESTAMPTZ DEFAULT clock_timestamp())";

fn values(range: std::ops::Range<i32>, item: &str) -> String {
    range.map(|k| format!("({k}, {item})")).collect::<Vec<_>>().join(", ")
}

#[tokio::test(flavor = "multi_thread")]
async fn every_row_of_a_statement_reads_one_now() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, TABLE).await;
    run(
        &c,
        &format!(
            "INSERT INTO nw (k, u, s, c) VALUES {}",
            values(0..200, "now(), statement_timestamp(), clock_timestamp()")
        ),
    )
    .await;
    // PostgreSQL 17: 1 | 1 | 1 | 1 | t | t | t, and clock_timestamp()
    // distinct across rows.
    assert_eq!(
        one(
            &c,
            "SELECT count(DISTINCT t), count(DISTINCT u), count(DISTINCT s), count(DISTINCT d) FROM nw"
        )
        .await,
        ["1", "1", "1", "1"]
    );
    assert_eq!(one(&c, "SELECT count(*) FROM nw WHERE t <> u OR u <> s OR s <> d").await, ["0"]);
    let distinct: i64 = one(&c, "SELECT count(DISTINCT c) FROM nw").await[0].parse().unwrap();
    assert!(distinct > 1, "clock_timestamp() is read per call: {distinct} distinct over 200 rows");
    let distinct: i64 = one(&c, "SELECT count(DISTINCT w) FROM nw").await[0].parse().unwrap();
    assert!(distinct > 1, "a clock_timestamp() default too: {distinct}");
    assert_eq!(one(&c, "SELECT count(*) FROM nw WHERE c < u OR w < u").await, ["0"]);

    // CURRENT_TIMESTAMP and transaction_timestamp() are now(), and under
    // autocommit so is statement_timestamp().
    run(&c, "INSERT INTO nw (k, u, s, c) VALUES (1000, CURRENT_TIMESTAMP, transaction_timestamp(), statement_timestamp())").await;
    assert_eq!(one(&c, "SELECT count(*) FROM nw WHERE k = 1000 AND (t <> u OR u <> s OR s <> c)").await, ["0"]);
    run(&c, "DELETE FROM nw WHERE k = 1000").await;

    // An UPDATE over every row writes one value.
    run(&c, "UPDATE nw SET u = now(), s = statement_timestamp()").await;
    assert_eq!(one(&c, "SELECT count(DISTINCT u), count(DISTINCT s), count(*) FROM nw WHERE u = s").await, ["1", "1", "200"]);

    // A multi-row upsert, both ways.
    run(&c, "CREATE TABLE up (k INT PRIMARY KEY, t TIMESTAMPTZ)").await;
    let upsert = format!("INSERT INTO up VALUES {} ON CONFLICT (k) DO UPDATE SET t = now()", values(0..50, "now()"));
    run(&c, &upsert).await;
    assert_eq!(one(&c, "SELECT count(DISTINCT t), count(*) FROM up").await, ["1", "50"]);
    pause().await;
    run(&c, &upsert).await;
    assert_eq!(one(&c, "SELECT count(DISTINCT t) FROM up").await, ["1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_block_reads_one_now_and_a_new_statement_timestamp_per_statement() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, TABLE).await;
    run(&c, "CREATE TABLE one (x INT)").await;
    run(&c, "INSERT INTO one VALUES (1)").await;
    let before = one(&c, "SELECT now() FROM one").await;
    pause().await;
    run(&c, "BEGIN").await;
    pause().await;
    run(&c, "INSERT INTO nw (k, u, s) VALUES (1, now(), statement_timestamp())").await;
    pause().await;
    run(&c, "INSERT INTO nw (k, u, s) VALUES (2, CURRENT_TIMESTAMP, statement_timestamp()), (3, transaction_timestamp(), statement_timestamp())").await;
    pause().await;
    let inside = one(&c, "SELECT now() FROM one").await;
    run(&c, "COMMIT").await;
    pause().await;
    let after = one(&c, "SELECT now() FROM one").await;

    // PostgreSQL 17: one now() for the block, defaults included; one
    // statement_timestamp() per statement.
    assert_eq!(one(&c, "SELECT count(DISTINCT t), count(DISTINCT u) FROM nw").await, ["1", "1"]);
    assert_eq!(one(&c, "SELECT count(*) FROM nw WHERE t <> u").await, ["0"]);
    assert_eq!(one(&c, "SELECT count(DISTINCT s), count(DISTINCT d) FROM nw").await, ["2", "2"]);
    assert_eq!(one(&c, "SELECT count(*) FROM nw WHERE s <= u").await, ["0"], "each statement began after the block");
    assert_eq!(one(&c, "SELECT u FROM nw WHERE k = 1").await, inside);
    let between = format!("SELECT count(*) FROM nw WHERE u > '{}' AND u < '{}'", before[0], after[0]);
    assert_eq!(one(&c, &between).await, ["3"], "{before:?} {inside:?} {after:?}");
}

/// A multi-statement query runs as one implicit transaction, and PostgreSQL
/// stamps the message: every statement of it reads one `now()` and one
/// `statement_timestamp()`.
#[tokio::test(flavor = "multi_thread")]
async fn the_statements_of_one_message_share_their_instants() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, TABLE).await;
    let mut batch = String::new();
    for k in 0..20 {
        batch.push_str(&format!("INSERT INTO nw (k, u, s) VALUES ({k}, now(), statement_timestamp()); "));
    }
    run(&c, &batch).await;
    assert_eq!(one(&c, "SELECT count(DISTINCT t), count(DISTINCT u), count(DISTINCT s), count(*) FROM nw").await, ["1", "1", "1", "20"]);
}
