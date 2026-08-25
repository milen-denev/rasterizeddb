//! Several databases behind one listener, driven by a real driver.
//!
//! The point of every test here is what a client sees over the wire: which
//! database the name in its connection string reaches, and that two of them do
//! not leak into each other.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::core::database::DEFAULT_DATABASE_NAME;
use rasterizeddb_core::pgwire::TlsMode;

async fn connect(url: &str) -> Result<sqlx::PgPool, String> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .map_err(|e| e.to_string())
}

/// The name in the connection string has to select a database, not be ignored.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_that_does_not_exist_is_refused() {
    let server = TestServer::start().await.expect("server should start");

    let err = connect(&server.url_for("nonexistent", "rasterized", None))
        .await
        .expect_err("an unknown database must not be served the default one");

    assert!(
        err.contains("does not exist"),
        "the refusal should name the problem: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn create_database_makes_a_new_one_connectable() {
    let server = TestServer::start().await.expect("server should start");
    let pool = connect(&server.url()).await.expect("the default database should accept");

    sqlx::query("CREATE DATABASE analytics")
        .execute(&pool)
        .await
        .expect("CREATE DATABASE should succeed");

    connect(&server.url_for("analytics", "rasterized", None))
        .await
        .expect("the created database should accept connections");
}

/// The whole point of separate databases: a table in one is not a table in the
/// other, even under the same name.
#[tokio::test(flavor = "multi_thread")]
async fn two_databases_do_not_share_tables() {
    let server = TestServer::start().await.expect("server should start");
    let default = connect(&server.url()).await.expect("default should accept");

    sqlx::query("CREATE DATABASE staging")
        .execute(&default)
        .await
        .expect("CREATE DATABASE should succeed");

    let staging = connect(&server.url_for("staging", "rasterized", None))
        .await
        .expect("staging should accept");

    for (pool, value) in [(&default, 1i32), (&staging, 2i32)] {
        sqlx::query("CREATE TABLE items (id INT NOT NULL)")
            .execute(pool)
            .await
            .expect("each database should take its own CREATE TABLE");
        sqlx::query(&format!("INSERT INTO items (id) VALUES ({})", value))
            .execute(pool)
            .await
            .expect("insert should succeed");
    }

    // Same table name, same clause, different database: if the query cache were
    // keyed by table name alone these would answer each other.
    for (pool, expected) in [(&default, 1i32), (&staging, 2i32)] {
        for _ in 0..3 {
            let rows: Vec<(i32,)> = sqlx::query_as("SELECT id FROM items")
                .fetch_all(pool)
                .await
                .expect("select should succeed");
            assert_eq!(
                rows,
                vec![(expected,)],
                "each database must answer with its own rows"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_database_stops_accepting_connections() {
    let server = TestServer::start().await.expect("server should start");
    let default = connect(&server.url()).await.expect("default should accept");

    sqlx::query("CREATE DATABASE scratch").execute(&default).await.unwrap();

    let scratch = connect(&server.url_for("scratch", "rasterized", None))
        .await
        .expect("scratch should accept");

    // While a session is attached, the drop must be refused rather than pulling
    // the files out from under it.
    let err = sqlx::query("DROP DATABASE scratch")
        .execute(&default)
        .await
        .expect_err("a database in use must not be dropped");
    assert!(err.to_string().contains("being accessed"), "{}", err);

    scratch.close().await;

    sqlx::query("DROP DATABASE scratch")
        .execute(&default)
        .await
        .expect("dropping should succeed once the session has gone");

    connect(&server.url_for("scratch", "rasterized", None))
        .await
        .expect_err("a dropped database must not accept connections");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_database_cannot_be_dropped() {
    let server = TestServer::start().await.expect("server should start");
    let pool = connect(&server.url()).await.expect("default should accept");

    sqlx::query(&format!("DROP DATABASE {}", DEFAULT_DATABASE_NAME))
        .execute(&pool)
        .await
        .expect_err("the default database must not be droppable");
}

/// Roles live in the database that holds them, so an account created in one is
/// not an account in another.
#[tokio::test(flavor = "multi_thread")]
async fn roles_do_not_cross_databases() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");

    let default = connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("the bootstrap role should reach the default database");

    sqlx::query("CREATE DATABASE tenant").execute(&default).await.unwrap();
    sqlx::query("CREATE USER alice PASSWORD 'wonderland'")
        .execute(&default)
        .await
        .expect("CREATE USER should succeed");

    connect(&server.url_as("alice", Some("wonderland")))
        .await
        .expect("alice should reach the database she was created in");

    let err = connect(&server.url_for("tenant", "alice", Some("wonderland")))
        .await
        .expect_err("alice does not exist in the database she was not created in");
    assert!(
        err.contains("password authentication failed"),
        "an account missing from a database is refused as any unknown user is: {}",
        err
    );

    // A new database still gets the bootstrap role, or it would be unreachable.
    connect(&server.url_for("tenant", "rasterized", Some("boot")))
        .await
        .expect("a created database should be reachable with the bootstrap role");
}
