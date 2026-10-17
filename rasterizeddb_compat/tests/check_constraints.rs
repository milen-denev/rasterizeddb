//! `CHECK` constraints.
//!
//! The clause used to be recognised as a keyword and discarded: the table was
//! created, the constraint was reported as accepted, and a row violating it was
//! written and answered with `INSERT 0 1`.
//!
//! PostgreSQL rejects a row only when a check evaluates to **false**. A check
//! that evaluates to NULL — because a column it reads is NULL — passes, which
//! is the opposite of how a `WHERE` clause treats NULL and is the case most
//! likely to be got wrong.

use rasterizeddb_compat::TestServer;
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

async fn ids(pool: &sqlx::PgPool, table: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!("SELECT id FROM {} ORDER BY id", table)))
        .fetch_all(pool)
        .await
        .expect("select")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_range_check_refuses_a_row_outside_it() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE orders (
            id INTEGER NOT NULL,
            status SMALLINT NOT NULL CHECK (status BETWEEN 1 AND 10)
        )",
    )
    .await
    .expect("create");

    run(&pool, "INSERT INTO orders (id, status) VALUES (1, 5)").await.expect("in range");
    run(&pool, "INSERT INTO orders (id, status) VALUES (2, 1)").await.expect("lower bound");
    run(&pool, "INSERT INTO orders (id, status) VALUES (3, 10)").await.expect("upper bound");

    for (id, status) in [(4, 0), (5, 11), (6, 99)] {
        run(&pool, &format!("INSERT INTO orders (id, status) VALUES ({}, {})", id, status))
            .await
            .unwrap_err();
    }

    // Only the three valid rows are there.
    assert_eq!(ids(&pool, "orders").await, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_error_names_the_constraint() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER NOT NULL CHECK (v > 0))",
    )
    .await
    .expect("create");

    let error = run(&pool, "INSERT INTO t (id, v) VALUES (1, -5)")
        .await
        .expect_err("a negative value should be refused");
    assert!(error.contains("check constraint"), "unexpected error: {}", error);
    assert!(error.contains("t_v_check"), "the constraint should be named: {}", error);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_check_over_a_null_column_passes() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE n (id INTEGER NOT NULL, v INTEGER CHECK (v > 0))")
        .await
        .expect("create");

    // NULL is not greater than zero, and it is not less than or equal to it
    // either: the check evaluates to NULL, and PostgreSQL lets the row in.
    run(&pool, "INSERT INTO n (id) VALUES (1)").await.expect("an absent value passes");
    run(&pool, "INSERT INTO n (id, v) VALUES (2, 5)").await.expect("a valid value passes");
    run(&pool, "INSERT INTO n (id, v) VALUES (3, -1)")
        .await
        .expect_err("an invalid value is refused");

    assert_eq!(ids(&pool, "n").await, vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_level_check_may_name_several_columns() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE r (
            id INTEGER NOT NULL,
            lo INTEGER NOT NULL,
            hi INTEGER NOT NULL,
            CHECK (lo < hi)
        )",
    )
    .await
    .expect("create");

    run(&pool, "INSERT INTO r (id, lo, hi) VALUES (1, 1, 10)").await.expect("ordered");
    run(&pool, "INSERT INTO r (id, lo, hi) VALUES (2, 10, 1)")
        .await
        .expect_err("reversed bounds should be refused");
    assert_eq!(ids(&pool, "r").await, vec![1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_named_constraint_keeps_its_name() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE named (
            id INTEGER NOT NULL,
            v INTEGER NOT NULL,
            CONSTRAINT v_must_be_small CHECK (v < 100)
        )",
    )
    .await
    .expect("create");

    let error = run(&pool, "INSERT INTO named (id, v) VALUES (1, 500)")
        .await
        .expect_err("should be refused");
    assert!(error.contains("v_must_be_small"), "unexpected error: {}", error);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_check_sees_the_value_a_default_supplied() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE d (id INTEGER NOT NULL, v INTEGER DEFAULT -1 CHECK (v > 0))",
    )
    .await
    .expect("create");

    // The check runs against the row as it will be stored, so a default that
    // violates it is caught — checking the statement's literals would not see
    // the value at all.
    run(&pool, "INSERT INTO d (id) VALUES (1)")
        .await
        .expect_err("a default violating the check should be refused");
    assert!(ids(&pool, "d").await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_check_survives_adding_a_column() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE keep (id INTEGER NOT NULL, v INTEGER NOT NULL CHECK (v > 0))")
        .await
        .expect("create");
    run(&pool, "INSERT INTO keep (id, v) VALUES (1, 5)").await.expect("insert");

    // `ADD COLUMN` rewrites the table; a constraint lost in the rewrite would
    // stop being enforced with nothing said.
    run(&pool, "ALTER TABLE keep ADD COLUMN note TEXT").await.expect("add column");
    run(&pool, "INSERT INTO keep (id, v) VALUES (2, -3)")
        .await
        .expect_err("the check should still be enforced after the rewrite");
    assert_eq!(ids(&pool, "keep").await, vec![1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wide_schema_enforces_its_status_range() {
    use rasterizeddb_compat::wide_types;
    let (_s, pool) = server().await;
    wide_types::apply(&pool).await.expect("schema should apply");

    // `order_status SMALLINT NOT NULL CHECK (order_status BETWEEN 1 AND 10)`.
    let error = sqlx::query(AssertSqlSafe(
        "INSERT INTO test_ecommerce_orders \
         (order_id, tenant_id, order_number, customer_id, store_code, order_status, subtotal) \
         VALUES ('99999999-9999-4999-8999-999999999999', 1, 'ORD-BAD', 1, 'ST001', 99, 1.0)",
    ))
    .execute(&pool)
    .await
    .expect_err("status 99 should be refused");
    assert!(
        error.to_string().contains("check constraint"),
        "unexpected error: {}",
        error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_that_violates_a_single_column_check_is_refused() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE u (id INTEGER NOT NULL, status SMALLINT NOT NULL CHECK (status BETWEEN 1 AND 10))",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO u (id, status) VALUES (1, 5)").await.expect("insert");

    run(&pool, "UPDATE u SET status = 99 WHERE id = 1")
        .await
        .expect_err("an update outside the range should be refused");
    run(&pool, "UPDATE u SET status = 7 WHERE id = 1")
        .await
        .expect("an update inside the range should succeed");

    let status: i16 = sqlx::query(AssertSqlSafe("SELECT status FROM u WHERE id = 1"))
        .fetch_one(&pool)
        .await
        .expect("select")
        .get(0);
    assert_eq!(status, 7, "the refused update must not have landed");
}

/// A check reading a column the statement does not assign is **not** enforced
/// on update.
///
/// Judging it would need the row's current value for the other half, which the
/// update plan has not read. Pinned as a test rather than left undiscovered:
/// the alternative — evaluating against a value it does not have — would reject
/// rows that are perfectly valid.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_column_check_is_not_enforced_on_a_partial_update() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE m (id INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL, CHECK (lo < hi))",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO m (id, lo, hi) VALUES (1, 1, 10)").await.expect("insert");

    // PostgreSQL would refuse this. Here it lands, because `hi` is not in the
    // SET list and the check cannot be judged from the assignments alone.
    run(&pool, "UPDATE m SET lo = 50 WHERE id = 1").await.expect("the gap this pins");

    // Assigning both halves *is* judged.
    run(&pool, "UPDATE m SET lo = 50, hi = 20 WHERE id = 1")
        .await
        .expect_err("both columns assigned, so the check applies");
}

/// A check on a column assigned an expression of the row, `SET q = q + 1`.
/// The update plan holds an empty placeholder for such a column until the row
/// is read, and judging the check against that refused every one of them. It
/// is judged against each row's new value instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_check_on_a_column_assigned_an_expression_is_judged_per_row() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE e (id INTEGER NOT NULL, q INTEGER CHECK (q >= 0))")
        .await
        .expect("create");
    run(&pool, "INSERT INTO e (id, q) VALUES (1, 3)").await.expect("insert 1");
    run(&pool, "INSERT INTO e (id, q) VALUES (2, 8)").await.expect("insert 2");

    // PG 17: UPDATE 2, then an error for row 1 (-1) that leaves row 2 alone.
    run(&pool, "UPDATE e SET q = q + 1").await.expect("both rows stay valid");
    let error = run(&pool, "UPDATE e SET q = q - 5")
        .await
        .expect_err("row 1 would hold -1");
    assert!(error.contains("violates check constraint"), "unexpected error: {}", error);
    run(&pool, "UPDATE e SET q = COALESCE(q, 0) - 4 WHERE id = 1")
        .await
        .expect("row 1 holds 0");

    let rows: Vec<(i32, i32)> = sqlx::query(AssertSqlSafe("SELECT id, q FROM e ORDER BY id"))
        .fetch_all(&pool)
        .await
        .expect("select")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 0), (2, 9)]);
}
