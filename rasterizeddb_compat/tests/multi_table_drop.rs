//! `DROP TABLE a, b`, against PostgreSQL 17's answers.
//!
//! It was refused ("dropping several tables in one statement is not
//! supported yet"), so a migration's `DROP TABLE IF EXISTS a, b` failed. It
//! is one statement now: every name checked before anything is dropped, a
//! foreign key between two listed tables going with them.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

async fn error_of(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    let d = e.as_database_error().expect("a database error");
    (d.code().map(|c| c.to_string()).unwrap_or_default(), d.message().to_string())
}

async fn exists(pool: &sqlx::PgPool, table: &str) -> bool {
    sqlx::raw_sql(AssertSqlSafe(format!("SELECT count(*) FROM \"{table}\""))).execute(pool).await.is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn several_tables_drop_together_or_not_at_all() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE m1 (a int); CREATE TABLE m2 (a int); CREATE TABLE m3 (a int); CREATE TABLE \"m,4\" (a int);").await;
    run(&pool, "DROP TABLE m1, m2").await;
    assert!(!exists(&pool, "m1").await && !exists(&pool, "m2").await);
    // A missing name fails the statement before anything is dropped.
    assert_eq!(
        error_of(&pool, "DROP TABLE m3, missing_table").await,
        ("42P01".to_string(), "table \"missing_table\" does not exist".to_string())
    );
    assert!(exists(&pool, "m3").await);
    // IF EXISTS skips it; a quoted name may hold a comma.
    run(&pool, "DROP TABLE IF EXISTS m3, missing_table, \"m,4\"").await;
    assert!(!exists(&pool, "m3").await && !exists(&pool, "m,4").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_key_between_listed_tables_goes_with_them() {
    let (_s, pool) = server().await;
    // Either order: the dependent is being dropped too.
    for order in ["mp, mc", "mc, mp"] {
        run(&pool, "CREATE TABLE mp (id int PRIMARY KEY); CREATE TABLE mc (id int, p int REFERENCES mp(id));").await;
        run(&pool, &format!("DROP TABLE {order}")).await;
        assert!(!exists(&pool, "mp").await && !exists(&pool, "mc").await, "{order}");
    }
    // A dependent outside the list refuses it whole, without CASCADE …
    run(&pool, "CREATE TABLE mp (id int PRIMARY KEY); CREATE TABLE mc (id int, p int REFERENCES mp(id)); CREATE TABLE mo (id int);")
        .await;
    assert_eq!(error_of(&pool, "DROP TABLE mp, mo").await.0, "2BP01");
    assert!(exists(&pool, "mp").await && exists(&pool, "mo").await);
    // … and with it drops only the key, never the dependent table.
    run(&pool, "DROP TABLE mp, mo CASCADE").await;
    assert!(!exists(&pool, "mp").await && !exists(&pool, "mo").await && exists(&pool, "mc").await);
    run(&pool, "INSERT INTO mc VALUES (1, 999)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn each_dropped_table_takes_its_sequences() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ms (id serial PRIMARY KEY, v int); CREATE TABLE mt (id serial PRIMARY KEY); INSERT INTO ms (v) VALUES (1), (2);").await;
    run(&pool, "DROP TABLE ms, mt").await;
    run(&pool, "CREATE TABLE ms (id serial PRIMARY KEY, v int)").await;
    let id: i32 = sqlx::query("INSERT INTO ms (v) VALUES (5) RETURNING id").fetch_one(&pool).await.expect("insert").get(0);
    assert_eq!(id, 1);
}
