//! A database you create has to be one you can open.
//!
//! Roles are per database, and a database name is resolved *before* its
//! connection is authenticated — so a database with no role catalogue refuses
//! every login. Nothing gave a newly created database one, so `CREATE DATABASE`
//! and `IMPORT … INTO` both produced a database that no account could reach:
//! the name resolved, the password was right, and the answer was "password
//! authentication failed".
//!
//! This is the regression test for that. It is also what an operator hits
//! first: an import that succeeds and a client that then cannot read it.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::pgwire::TlsMode;
use rasterizeddb_core::auth::AuthMethod;
use sqlx::Row;

#[tokio::test]
async fn a_created_database_accepts_the_accounts_that_could_create_it() {
    // A password-authenticated server, deliberately: under trust
    // authentication the role catalogue is never consulted, so a test that used
    // the default would pass whether or not a created database has one.
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("s3cret"))
        .await
        .expect("server should start");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&server.url_as("rasterized", Some("s3cret")))
        .await
        .expect("should connect");

    sqlx::query("CREATE DATABASE made_here")
        .execute(&pool)
        .await
        .expect("create database");

    // The same credentials, against the database just created.
    let other = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&server.url_for("made_here", "rasterized", Some("s3cret")))
        .await
        .expect("the account that created the database should be able to open it");

    sqlx::query("CREATE TABLE t (id INT NOT NULL, doc JSONB)")
        .execute(&other)
        .await
        .expect("create table in the new database");
    sqlx::query(r#"INSERT INTO t (id, doc) VALUES (1, '{"k":"v"}')"#)
        .execute(&other)
        .await
        .expect("insert");

    let doc: serde_json::Value = sqlx::query("SELECT doc FROM t WHERE id = 1")
        .fetch_one(&other)
        .await
        .expect("select")
        .try_get("doc")
        .expect("doc");
    assert_eq!(doc, serde_json::json!({"k": "v"}));

    // And the two are separate databases, not one seen twice.
    let here = sqlx::query("SELECT id FROM t").fetch_all(&pool).await;
    assert!(here.is_err(), "the table should not exist in the original database");
}
