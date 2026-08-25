//! The `pg_catalog` and `information_schema` relations, as a driver reads them.
//!
//! These are what schema introspection runs on, and the extended query protocol
//! asks what a statement returns *before* running it. A catalogue view does not
//! exist until it is materialised, so the describe path could not resolve one
//! and answered `NoData` — then `Execute` sent rows anyway. sqlx surfaced that
//! as rows whose columns could not be read by name: the values were right and
//! `try_get("data_type")` was `ColumnNotFound`, so any ORM introspecting the
//! schema broke on it.
//!
//! The expected column names here are written out rather than read from the
//! engine's own list. They are fixed by PostgreSQL, so an independent spelling
//! is the oracle — a shared list would agree with itself even when both are
//! wrong.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Column, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE docs (id INT NOT NULL, body TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    (server, pool)
}

/// The column names a `SELECT *` announces, in order.
async fn described(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect("query should succeed");
    let first = rows.first().expect("expected at least one row");
    first.columns().iter().map(|c| c.name().to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_catalogue_view_announces_its_columns() {
    let (_server, pool) = server().await;

    for (sql, expected) in [
        (
            "SELECT * FROM information_schema.columns WHERE table_name = 'docs'",
            vec![
                "table_catalog", "table_schema", "table_name", "column_name",
                "ordinal_position", "data_type", "is_nullable",
            ],
        ),
        (
            "SELECT * FROM information_schema.tables WHERE table_name = 'docs'",
            vec!["table_catalog", "table_schema", "table_name", "table_type"],
        ),
        (
            "SELECT * FROM pg_class",
            vec!["oid", "relname", "relnamespace", "relkind", "relsize", "reltuples"],
        ),
        ("SELECT * FROM pg_namespace", vec!["oid", "nspname"]),
        (
            "SELECT * FROM pg_stat_user_tables",
            vec!["relid", "schemaname", "relname", "n_live_tup", "n_dead_tup"],
        ),
        (
            "SELECT * FROM pg_database",
            vec![
                "oid", "datname", "datdba", "encoding", "datcollate", "datctype",
                "datistemplate", "datallowconn", "datconnlimit",
            ],
        ),
    ] {
        assert_eq!(described(&pool, sql).await, expected, "for {sql:?}");
    }
}

/// The point of the description: a field can be read by name. This is the
/// assertion a schema-introspecting ORM actually makes.
#[tokio::test(flavor = "multi_thread")]
async fn introspection_columns_can_be_read_by_name() {
    let (_server, pool) = server().await;

    let rows = sqlx::query(
        "SELECT * FROM information_schema.columns WHERE table_name = 'docs' \
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("query");
    assert_eq!(rows.len(), 2, "docs has two columns");

    let names: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("column_name").expect("read by name"))
        .collect();
    assert_eq!(names, vec!["id", "body"]);

    let types: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("data_type").expect("read by name"))
        .collect();
    assert_eq!(types, vec!["integer", "text"]);

    // `id` is NOT NULL and `body` is not.
    let nullable: Vec<String> = rows
        .iter()
        .map(|r| r.try_get::<String, _>("is_nullable").expect("read by name"))
        .collect();
    assert_eq!(nullable, vec!["NO", "YES"]);

    // information_schema.tables, the relation a schema dump lists first.
    let row = sqlx::query(
        "SELECT table_name, table_type FROM information_schema.tables \
         WHERE table_name = 'docs'",
    )
    .fetch_one(&pool)
    .await
    .expect("tables");
    assert_eq!(row.try_get::<String, _>("table_name").expect("by name"), "docs");
    assert_eq!(row.try_get::<String, _>("table_type").expect("by name"), "BASE TABLE");
}

/// An explicit column list is described as written, not as the whole relation.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_column_list_is_described_as_written() {
    let (_server, pool) = server().await;

    assert_eq!(
        described(&pool, "SELECT relname, relkind FROM pg_class").await,
        vec!["relname", "relkind"]
    );
    assert_eq!(
        described(
            &pool,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'docs'"
        )
        .await,
        vec!["column_name"]
    );
}

/// "tables" and "columns" are ordinary words, so they are matched only when
/// qualified. A user relation named `tables` must still be its own table.
#[tokio::test(flavor = "multi_thread")]
async fn a_user_table_named_tables_is_not_the_catalogue_view() {
    let (_server, pool) = server().await;

    sqlx::query("CREATE TABLE tables (id INT NOT NULL, label TEXT)")
        .execute(&pool)
        .await
        .expect("a user table may be called tables");
    sqlx::query("INSERT INTO tables (id, label) VALUES (1, 'mine')")
        .execute(&pool)
        .await
        .expect("insert");

    let row = sqlx::query("SELECT id, label FROM tables").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<String, _>("label").expect("by name"), "mine");
}
