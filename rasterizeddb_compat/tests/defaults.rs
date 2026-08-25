//! Column defaults.
//!
//! A default is stored as the expression it was written as and evaluated when a
//! row is inserted, which is what makes `DEFAULT CURRENT_TIMESTAMP` mean the
//! time of the insert rather than the time of the `CREATE TABLE`.

use rasterizeddb_compat::{TestServer, probes};
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn text_of(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(sql)
        .fetch_all(pool)
        .await
        .expect("select should succeed")
        .iter()
        .map(|r| r.try_get::<String, _>(0).expect("text column"))
        .collect()
}

async fn ints_of(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(sql)
        .fetch_all(pool)
        .await
        .expect("select should succeed")
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("int column"))
        .collect()
}

async fn with_defaults(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE d (\
           id INTEGER NOT NULL, \
           status TEXT NOT NULL DEFAULT 'pending', \
           hits INTEGER NOT NULL DEFAULT 0, \
           ok BOOL NOT NULL DEFAULT false, \
           made TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .await
    .expect("create with defaults");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_omitted_column_takes_its_default() {
    let (_s, pool) = server().await;
    with_defaults(&pool).await;

    run(&pool, "INSERT INTO d (id) VALUES (1)").await.expect("insert");

    assert_eq!(text_of(&pool, "SELECT status FROM d WHERE id = 1").await, vec!["pending"]);
    assert_eq!(ints_of(&pool, "SELECT hits FROM d WHERE id = 1").await, vec![0]);
    assert_eq!(
        ints_of(&pool, "SELECT id FROM d WHERE ok = false").await,
        vec![1],
        "a boolean default applies too"
    );
    assert_eq!(
        ints_of(&pool, "SELECT id FROM d WHERE made IS NOT NULL").await,
        vec![1],
        "CURRENT_TIMESTAMP produced a value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_supplied_value_wins_over_the_default() {
    let (_s, pool) = server().await;
    with_defaults(&pool).await;

    run(&pool, "INSERT INTO d (id, status) VALUES (2, 'active')").await.expect("insert");
    assert_eq!(text_of(&pool, "SELECT status FROM d WHERE id = 2").await, vec!["active"]);
}

/// `VALUES (…, DEFAULT, …)` means the same as leaving the column out.
#[tokio::test(flavor = "multi_thread")]
async fn the_default_keyword_in_a_values_list_is_honoured() {
    let (_s, pool) = server().await;
    with_defaults(&pool).await;

    run(&pool, "INSERT INTO d (id, status, hits) VALUES (3, DEFAULT, 7)")
        .await
        .expect("insert");

    assert_eq!(text_of(&pool, "SELECT status FROM d WHERE id = 3").await, vec!["pending"]);
    assert_eq!(
        ints_of(&pool, "SELECT hits FROM d WHERE id = 3").await,
        vec![7],
        "the explicitly supplied value is unaffected"
    );
}

/// This is what makes `ADD COLUMN … NOT NULL` possible at all: the existing
/// rows have something to be filled with.
#[tokio::test(flavor = "multi_thread")]
async fn add_column_not_null_with_a_default_fills_existing_rows() {
    let (_s, pool) = server().await;
    with_defaults(&pool).await;
    run(&pool, "INSERT INTO d (id) VALUES (1)").await.expect("insert");
    run(&pool, "INSERT INTO d (id) VALUES (2)").await.expect("insert");

    run(&pool, "ALTER TABLE d ADD COLUMN tier TEXT NOT NULL DEFAULT 'free'")
        .await
        .expect("add column with a default");

    assert_eq!(
        text_of(&pool, "SELECT tier FROM d ORDER BY id ASC").await,
        vec!["free", "free"],
        "rows that predate the column take its default"
    );
}

/// Changing a default affects later inserts only; it does not rewrite rows.
#[tokio::test(flavor = "multi_thread")]
async fn set_default_applies_to_later_inserts_only() {
    let (_s, pool) = server().await;
    with_defaults(&pool).await;
    run(&pool, "INSERT INTO d (id) VALUES (1)").await.expect("insert");

    run(&pool, "ALTER TABLE d ALTER COLUMN status SET DEFAULT 'queued'")
        .await
        .expect("set default");
    run(&pool, "INSERT INTO d (id) VALUES (2)").await.expect("insert");

    assert_eq!(
        text_of(&pool, "SELECT status FROM d ORDER BY id ASC").await,
        vec!["pending", "queued"],
        "the existing row keeps the value it was written with"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_default_leaves_the_column_null() {
    let (_s, pool) = server().await;

    run(&pool, "CREATE TABLE e (id INTEGER NOT NULL, note TEXT DEFAULT 'x')")
        .await
        .expect("create");
    run(&pool, "INSERT INTO e (id) VALUES (1)").await.expect("insert");
    run(&pool, "ALTER TABLE e ALTER COLUMN note DROP DEFAULT").await.expect("drop default");
    run(&pool, "INSERT INTO e (id) VALUES (2)").await.expect("insert");

    let rows = sqlx::query("SELECT note FROM e ORDER BY id ASC")
        .fetch_all(&pool)
        .await
        .expect("select");
    let notes: Vec<Option<String>> =
        rows.iter().map(|r| r.try_get("note").expect("note")).collect();
    assert_eq!(notes, vec![Some("x".to_string()), None]);
}

/// A default that cannot be evaluated must be refused when it is declared, not
/// discovered on the first insert.
#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_default_expression_is_refused_at_declaration() {
    let (_s, pool) = server().await;

    let err = run(&pool, "CREATE TABLE f (id INTEGER NOT NULL, x INTEGER DEFAULT some_func(1))")
        .await
        .expect_err("should be refused");
    assert!(err.contains("not supported"), "{}", err);
}

/// A column whose *name* contains the keyword must not be read as a clause.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_named_after_the_keyword_still_works() {
    let (_s, pool) = server().await;

    // The compat schema already has `domain.default_subscription`, which would
    // fail to parse if the keyword matched inside an identifier.
    run(&pool, "CREATE TABLE g (id INTEGER NOT NULL, default_value TEXT, is_default BOOL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO g (id, default_value) VALUES (1, 'v')").await.expect("insert");
    assert_eq!(text_of(&pool, "SELECT default_value FROM g WHERE id = 1").await, vec!["v"]);
}
