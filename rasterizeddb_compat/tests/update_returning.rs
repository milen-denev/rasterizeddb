//! `UPDATE … SET … RETURNING` must report the value the row holds after the
//! write — through both protocols.
//!
//! A production app (sea-orm, extended protocol, quoted identifiers, a bound
//! parameter in the WHERE) ran `UPDATE global_settings SET value = value
//! WHERE key = $1 RETURNING id, key, value` and got `value = ""` while the
//! stored row still read the true value — a silent wrong answer, with the
//! unassigned columns of the same row coming back correct. The same statement
//! over the simple protocol with a literal predicate was right. These tests
//! pin every cell of that matrix, including the two that were never run: the
//! extended protocol with a literal right-hand side, and whether
//! `SET a = b` *stores* correctly (its RETURNING and its storage demonstrably
//! can disagree, so each is asserted on its own).

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn seed(pool: &sqlx::PgPool) {
    // The production shape exactly: uuid primary key, UNIQUE on the predicate
    // column. The unique constraint is part of the repro on purpose — the
    // UPDATE path re-checks constraints, and that check has its own history.
    run(pool, "CREATE TABLE global_settings (id UUID PRIMARY KEY, key TEXT NOT NULL UNIQUE, value TEXT NOT NULL)")
        .await
        .expect("create");
    run(pool, "INSERT INTO global_settings (id, key, value) VALUES ('f8f6d4d4-fde2-4f9d-a6c2-4d17bc177a86', 'next_invoice_number', '58')")
        .await
        .expect("insert");
}

/// The failing production cell: extended protocol, bound parameter in the
/// WHERE, the assigned column's own name on the right of SET, quoted the way
/// sea-orm writes it.
#[tokio::test]
async fn extended_set_self_returns_the_stored_value() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    let row = sqlx::query(
        "UPDATE \"global_settings\" SET \"value\" = \"value\" WHERE \"key\" = $1 \
         RETURNING \"id\", \"key\", \"value\"",
    )
    .bind("next_invoice_number")
    .fetch_one(&pool)
    .await
    .expect("the update must return its row");

    assert_eq!(
        row.try_get::<sqlx::types::Uuid, _>("id").expect("id").to_string(),
        "f8f6d4d4-fde2-4f9d-a6c2-4d17bc177a86"
    );
    assert_eq!(row.try_get::<String, _>("key").expect("key"), "next_invoice_number");
    assert_eq!(
        row.try_get::<String, _>("value").expect("value"),
        "58",
        "RETURNING of a self-assigned column must be its value, not empty"
    );

    // Storage was never in question in production, but pin it anyway.
    let value: String = sqlx::query("SELECT value FROM global_settings WHERE key = $1")
        .bind("next_invoice_number")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("value")
        .expect("value");
    assert_eq!(value, "58");
}

/// The unrun cell that separates "extended RETURNING is wrong for any
/// assigned column" from "extended plus a column-reference RHS specifically".
#[tokio::test]
async fn extended_set_literal_returns_the_new_value() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    let value: String = sqlx::query(
        "UPDATE global_settings SET value = '59' WHERE key = $1 RETURNING value",
    )
    .bind("next_invoice_number")
    .fetch_one(&pool)
    .await
    .expect("the update must return its row")
    .try_get("value")
    .expect("value");
    assert_eq!(value, "59", "RETURNING of a literal assignment");
}

/// The control row of the matrix: the same self-assignment over the simple
/// protocol with a literal predicate, which production observed to be right.
#[tokio::test]
async fn simple_set_self_returns_the_stored_value() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    let rows = pool
        .fetch_all(sqlx::raw_sql(
            "UPDATE global_settings SET value = value \
             WHERE key = 'next_invoice_number' RETURNING id, key, value",
        ))
        .await
        .expect("simple-protocol update must return its row");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].try_get::<String, _>("value").expect("value"),
        "58",
        "simple protocol, self-assigned column"
    );
}

/// `SET a = b` must STORE b's value — three production backfill migrations
/// depend on it, and `SET value = value` cannot prove it: a column-reference
/// RHS that resolved to empty and wrote it would be indistinguishable from a
/// correct write of an unchanged value. The re-SELECT is asserted separately
/// from the RETURNING because the two are known to be able to disagree.
#[tokio::test]
async fn set_column_to_other_column_stores_and_returns_it() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT NOT NULL, a TEXT, b TEXT)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO t (id, a, b) VALUES (1, '', 'hello')")
        .await
        .expect("insert");

    // Extended protocol, the migrations' shape.
    let returned: String = sqlx::query("UPDATE t SET a = b WHERE id = $1 RETURNING a")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("update must return its row")
        .try_get("a")
        .expect("a");
    assert_eq!(returned, "hello", "RETURNING of the cross-column assignment");

    // The stored value, asserted independently of RETURNING.
    let row = sqlx::query("SELECT a, b FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("read back");
    assert_eq!(row.try_get::<String, _>("a").expect("a"), "hello", "a STORES b's value");
    assert_eq!(row.try_get::<String, _>("b").expect("b"), "hello", "b is untouched");

    // And over the simple protocol, on a fresh row.
    run(&pool, "INSERT INTO t (id, a, b) VALUES (2, '', 'world')")
        .await
        .expect("insert");
    let rows = pool
        .fetch_all(sqlx::raw_sql("UPDATE t SET a = b WHERE id = 2 RETURNING a"))
        .await
        .expect("simple-protocol update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<String, _>("a").expect("a"), "world");
    let stored: String = sqlx::query("SELECT a FROM t WHERE id = 2")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("a")
        .expect("a");
    assert_eq!(stored, "world", "simple-protocol cross-column assignment stores");
}

/// `SET a = b RETURNING a` where `b IS NULL` must report NULL — not the empty
/// string. The overlay used to have no arm for a NULL evaluation, so the
/// assignment's empty placeholder leaked through: "" where the write path was
/// about to store NULL and set its companion. The same class of silent wrong
/// answer as the production bug, one row over.
#[tokio::test]
async fn set_column_to_a_null_column_returns_null() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT NOT NULL, a TEXT, b TEXT)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO t (id, a, b) VALUES (1, 'old', NULL)")
        .await
        .expect("insert");

    let returned: Option<String> = sqlx::query("UPDATE t SET a = b WHERE id = $1 RETURNING a")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("update must return its row")
        .try_get("a")
        .expect("a");
    assert_eq!(returned, None, "RETURNING must report NULL, not \"\"");

    // The stored value agrees, asserted independently.
    let stored: Option<String> = sqlx::query("SELECT a FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("a")
        .expect("a");
    assert_eq!(stored, None, "a stores NULL");
}

/// `RETURNING` an expression: the counter-allocation shape — reserve a block
/// by advancing the counter and learn the start of the reserved range from
/// the same statement. The expression evaluates against the row AFTER the
/// update, as PostgreSQL's RETURNING does.
#[tokio::test]
async fn returning_an_expression_reports_the_post_update_value() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ut (id INT NOT NULL, next_reference_number BIGINT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO ut (id, next_reference_number) VALUES (1, 58)")
        .await
        .expect("insert");

    let row = sqlx::query(
        "UPDATE ut SET next_reference_number = next_reference_number + 1 WHERE id = $1 \
         RETURNING (next_reference_number - 1) AS allocated",
    )
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("RETURNING an expression must work");
    assert_eq!(
        row.try_get::<i64, _>("allocated").expect("allocated"),
        58,
        "the reserved number: post-update 59, minus one"
    );

    // Storage advanced, asserted independently of RETURNING.
    let stored: i64 = sqlx::query("SELECT next_reference_number FROM ut WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("next_reference_number")
        .expect("value");
    assert_eq!(stored, 59);

    // A mixed list: a stored column beside the expression, and an aliased
    // plain column — all named the way the client asked.
    let row = sqlx::query(
        "UPDATE ut SET next_reference_number = next_reference_number + 10 WHERE id = $1 \
         RETURNING id, (next_reference_number - 10) AS allocated, next_reference_number AS reserved_through",
    )
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("mixed RETURNING list");
    assert_eq!(row.try_get::<i32, _>("id").expect("id"), 1);
    assert_eq!(row.try_get::<i64, _>("allocated").expect("allocated"), 59);
    assert_eq!(row.try_get::<i64, _>("reserved_through").expect("reserved"), 69);
}

/// `RETURNING (counter - 1) AS allocated` on an **int4** column must be
/// `int4`, as it is on PostgreSQL. Every integer literal parses as `i64`
/// here — there is nothing in `1` to say otherwise — so the subtraction
/// widened to `bigint`, and a client asking for `Option<i32>` was told its
/// type did not match. The value has to be re-encoded to the type it is
/// described as too: the arithmetic runs in `i64`, and eight bytes under an
/// `int4` OID is not a wrong number but an unreadable one.
#[tokio::test]
async fn a_computed_returning_keeps_the_columns_width() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE counters (id INT NOT NULL, next_reference_number INT NOT NULL, big BIGINT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO counters (id, next_reference_number, big) VALUES (1, 58, 900)")
        .await
        .expect("insert");

    // int4 column -> int4 result, decodable as i32.
    let row = sqlx::query(
        "UPDATE counters SET next_reference_number = next_reference_number + 1 WHERE id = $1 \
         RETURNING (next_reference_number - 1) AS allocated",
    )
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("the counter allocation must work");
    assert_eq!(
        row.try_get::<Option<i32>, _>("allocated").expect("allocated is int4"),
        Some(58)
    );

    // A bigint column still yields bigint — narrowing must not go too far.
    let row = sqlx::query("UPDATE counters SET big = big + 1 WHERE id = $1 RETURNING (big - 1) AS b")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("bigint arithmetic");
    assert_eq!(row.try_get::<Option<i64>, _>("b").expect("b is int8"), Some(900));

    // And in a plain SELECT, where the description and the value come from
    // different places and so have to be checked separately.
    let row = sqlx::query("SELECT (next_reference_number - 1) AS allocated, (big - 1) AS b FROM counters WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select computed columns");
    assert_eq!(row.try_get::<Option<i32>, _>("allocated").expect("int4"), Some(58));
    assert_eq!(row.try_get::<Option<i64>, _>("b").expect("int8"), Some(900));
}
