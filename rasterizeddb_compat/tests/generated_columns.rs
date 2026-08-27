//! `GENERATED ALWAYS AS (…) STORED` columns.
//!
//! The clause used to be recognised and discarded: the table was created, the
//! column existed, and every row read NULL for it with nothing reported. A
//! `SUM` over such a column returned nothing and looked like an empty table,
//! which is why the value is asserted here rather than only the absence of an
//! error.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// The schema the tests share: a total computed from three inputs, one of
/// which has a default so an omitted column still reaches the expression.
async fn orders(pool: &sqlx::PgPool) {
    run(
        pool,
        "CREATE TABLE billing (
            id INTEGER NOT NULL,
            subtotal NUMERIC(12,4) NOT NULL,
            tax NUMERIC(12,4) NOT NULL,
            fee NUMERIC(12,4) DEFAULT 1.0000,
            total NUMERIC(12,4) GENERATED ALWAYS AS (subtotal + tax + fee) STORED
        )",
    )
    .await
    .expect("create should succeed");
}

async fn total_of(pool: &sqlx::PgPool, id: i32) -> Option<Decimal> {
    sqlx::query(AssertSqlSafe(format!("SELECT total FROM billing WHERE id = {}", id)))
        .fetch_one(pool)
        .await
        .expect("select should succeed")
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_is_computed_from_the_row() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert should succeed");

    // 2.5 + 3.5 + the default fee of 1.
    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(70, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_supplied_input_is_used_in_place_of_the_default() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax, fee) VALUES (2, 10.0, 20.0, 5.0)")
        .await
        .expect("insert should succeed");

    assert_eq!(total_of(&pool, 2).await, Some(Decimal::new(350, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_computed_value_is_stored_and_aggregates_over_it() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO billing (id, subtotal, tax, fee) VALUES (2, 10.0, 20.0, 5.0)")
        .await
        .expect("insert");

    // `STORED` means the value is a column like any other, so an aggregate
    // reads it without re-evaluating anything. This is the assertion that
    // failed before: the sum was NULL.
    let sum: Decimal = sqlx::query(AssertSqlSafe("SELECT SUM(total) FROM billing"))
        .fetch_one(&pool)
        .await
        .expect("sum should succeed")
        .get(0);
    assert_eq!(sum, Decimal::new(420, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_supplied_value_is_refused() {
    let (_s, pool) = server().await;
    orders(&pool).await;

    // PostgreSQL's own error. Accepting the value and overruling it would let a
    // client believe it had set the column.
    let error = run(
        &pool,
        "INSERT INTO billing (id, subtotal, tax, total) VALUES (3, 1.0, 1.0, 99.0)",
    )
    .await
    .expect_err("a value for a generated column should be refused");
    assert!(error.contains("generated column"), "unexpected error: {}", error);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_an_input_is_refused_rather_than_leaving_the_total_stale() {
    let (_s, pool) = server().await;
    orders(&pool).await;
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");

    let error = run(&pool, "UPDATE billing SET subtotal = 100.0 WHERE id = 1")
        .await
        .expect_err("updating an input of a generated column should be refused");
    assert!(error.contains("stale"), "unexpected error: {}", error);

    // The row is untouched, so the refusal cost nothing.
    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(70, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_a_column_the_expression_does_not_name_still_works() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE notes (
            id INTEGER NOT NULL,
            a NUMERIC(12,4) NOT NULL,
            b NUMERIC(12,4) NOT NULL,
            memo TEXT,
            total NUMERIC(12,4) GENERATED ALWAYS AS (a + b) STORED
        )",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO notes (id, a, b, memo) VALUES (1, 2.0, 3.0, 'before')")
        .await
        .expect("insert");

    run(&pool, "UPDATE notes SET memo = 'after' WHERE id = 1")
        .await
        .expect("a column the expression does not name is not an input");

    let row = sqlx::query(AssertSqlSafe("SELECT memo, total FROM notes WHERE id = 1"))
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<Option<String>, _>(0).as_deref(), Some("after"));
    assert_eq!(row.get::<Option<Decimal>, _>(1), Some(Decimal::new(50, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_survives_adding_another_column() {
    let (_s, pool) = server().await;
    orders(&pool).await;
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert");

    // `ADD COLUMN` rewrites the table. A generation expression lost in the
    // rewrite would leave the column present and permanently NULL from then on.
    run(&pool, "ALTER TABLE billing ADD COLUMN note TEXT").await.expect("add column");
    run(&pool, "INSERT INTO billing (id, subtotal, tax) VALUES (2, 1.0, 1.0)")
        .await
        .expect("insert after the rewrite");

    assert_eq!(total_of(&pool, 1).await, Some(Decimal::new(70, 1)), "the existing row");
    assert_eq!(total_of(&pool, 2).await, Some(Decimal::new(30, 1)), "a row written after");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_input_makes_the_whole_expression_null() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE partial (
            id INTEGER NOT NULL,
            a NUMERIC(12,4),
            b NUMERIC(12,4) NOT NULL,
            total NUMERIC(12,4) GENERATED ALWAYS AS (a + b) STORED
        )",
    )
    .await
    .expect("create");

    // As in PostgreSQL: NULL propagates rather than being treated as zero,
    // which would report a total that looks right and is not.
    run(&pool, "INSERT INTO partial (id, b) VALUES (1, 5.0)").await.expect("insert");
    let total: Option<Decimal> =
        sqlx::query(AssertSqlSafe("SELECT total FROM partial WHERE id = 1"))
            .fetch_one(&pool)
            .await
            .expect("select")
            .get(0);
    assert_eq!(total, None);
}
