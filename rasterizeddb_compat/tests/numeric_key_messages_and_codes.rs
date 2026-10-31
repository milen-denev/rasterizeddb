//! A duplicate key's message names a `NUMERIC(p, s)` value at its declared
//! scale, and two constructs this engine does not implement say so (0A000)
//! instead of misreporting themselves.
//!
//! * The key was rendered from its stored integer at the engine's nine
//!   places: a NUMERIC(8,3) 1.500 read `b = 0.0000015`, a value no row holds.
//!   PostgreSQL 17 reports `Key (a, b)=(1, 1.500) already exists.`
//! * `UPDATE … FROM … RETURNING` an expression over the FROM relation was
//!   42703, an undefined column, for columns that exist (PostgreSQL returns
//!   them); a scalar subquery reading the outer row was 42601 ("function ''
//!   is not supported yet") in a select list and 42703 in a predicate
//!   (PostgreSQL evaluates it per row).

use rasterizeddb_compat::TestServer;
use tokio_postgres::Client;

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

/// The statement's error, as (SQLSTATE, message).
async fn refusal(c: &Client, sql: &str) -> (String, String) {
    let err = c.simple_query(sql).await.expect_err(sql);
    let db = err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}"));
    (db.code().code().to_string(), db.message().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_key_is_named_at_its_declared_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE k5 (a INT, b NUMERIC(8,3), PRIMARY KEY (a, b))").await;
    run(&c, "INSERT INTO k5 VALUES (1, 1.5)").await;
    let (code, message) = refusal(&c, "INSERT INTO k5 VALUES (1, 1.5)").await;
    assert_eq!(code, "23505");
    assert!(message.contains("b = 1.500"), "{message}");

    // The same when a UNIQUE constraint is added over rows that break it.
    run(&c, "CREATE TABLE k6 (a INT, b NUMERIC(10,2))").await;
    run(&c, "INSERT INTO k6 VALUES (1, 2.50), (1, 2.5)").await;
    let (code, message) = refusal(&c, "ALTER TABLE k6 ADD CONSTRAINT k6_u UNIQUE (a, b)").await;
    assert_eq!(code, "23505");
    assert!(message.contains("(1, 2.50)"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn returning_over_the_from_list_is_unsupported_not_undefined() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE u1 (id INT PRIMARY KEY, v INT)").await;
    run(&c, "CREATE TABLE u2 (id INT PRIMARY KEY, z INT)").await;
    run(&c, "INSERT INTO u1 VALUES (1, 10), (2, 20)").await;
    run(&c, "INSERT INTO u2 VALUES (1, 0), (2, 1)").await;
    for sql in [
        "UPDATE u1 SET v = 5 FROM u2 WHERE u1.id = u2.id RETURNING u1.v / u2.z",
        "UPDATE u1 SET v = 5 FROM u2 WHERE u1.id = u2.id RETURNING u2.z",
        "UPDATE u1 SET v = 5 FROM u2 WHERE u1.id = u2.id RETURNING v / z",
        "DELETE FROM u1 USING u2 WHERE u1.id = u2.id RETURNING u2.z",
    ] {
        assert_eq!(refusal(&c, sql).await.0, "0A000", "{sql}");
    }
    // A name no relation has is still an undefined column.
    assert_eq!(
        refusal(&c, "UPDATE u1 SET v = 5 FROM u2 WHERE u1.id = u2.id RETURNING nosuch").await.0,
        "42703"
    );
    // The target's own columns are returned as before.
    let rows = c.simple_query("UPDATE u1 SET v = 5 FROM u2 WHERE u1.id = u2.id RETURNING v * 2").await.unwrap();
    assert_eq!(rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_scalar_subquery_is_unsupported_not_a_syntax_error() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE u1 (id INT PRIMARY KEY, v INT)").await;
    run(&c, "CREATE TABLE u2 (id INT PRIMARY KEY, z INT)").await;
    run(&c, "INSERT INTO u1 VALUES (1, 10), (2, 20)").await;
    run(&c, "INSERT INTO u2 VALUES (1, 0), (2, 1)").await;
    for sql in [
        "SELECT id, (SELECT z FROM u2 WHERE u2.id = u1.id) FROM u1 ORDER BY id",
        "SELECT id, (SELECT max(z) FROM u2 WHERE u2.id = u1.id) AS m FROM u1 ORDER BY id",
        "SELECT id FROM u1 WHERE v > (SELECT z FROM u2 WHERE u2.id = u1.id) ORDER BY id",
    ] {
        assert_eq!(refusal(&c, sql).await.0, "0A000", "{sql}");
    }
    // An uncorrelated one is still answered.
    let rows = c.simple_query("SELECT id, (SELECT max(z) FROM u2) FROM u1 ORDER BY id").await.unwrap();
    assert_eq!(rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count(), 2);
}
