//! A computed `RETURNING` expression containing a bound parameter.
//!
//! From the audit path's `next_entry_order`:
//!
//! ```sql
//! UPDATE user_table SET next_row_entry_order = next_row_entry_order + $3
//! WHERE id = $1 AND domain_id = $2
//! RETURNING next_row_entry_order - $3 AS next_order
//! ```
//!
//! The expression is evaluated to produce the returned value, and arithmetic
//! widens: a subtraction over an `int4` is done in `i64`. A value of the wrong
//! width under a declared type is not a wrong number, it is an unreadable one —
//! and a *zero-length* one under a fixed-width type is a client panic rather
//! than an error, because sqlx's fixed-width decoders do not length-check.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// The statement verbatim in shape, on `BIGINT` as the audit table has it.
#[tokio::test]
async fn next_entry_order_returns_its_computed_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table (id INT PRIMARY KEY, domain_id INT NOT NULL, \
         next_row_entry_order BIGINT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO user_table (id, domain_id, next_row_entry_order) VALUES (1, 9, 100)",
    ))
    .await
    .expect("seed");

    let row = sqlx::query(
        "UPDATE user_table SET next_row_entry_order = next_row_entry_order + $3 \
         WHERE id = $1 AND domain_id = $2 \
         RETURNING next_row_entry_order - $3 AS next_order",
    )
    .bind(1i32)
    .bind(9i32)
    .bind(5i64)
    .fetch_one(&pool)
    .await
    .expect("the reservation statement must run");

    // The value before the increment: 100.
    assert_eq!(
        row.get::<Option<i64>, _>("next_order"),
        Some(100),
        "RETURNING must report the computed value, not an empty one"
    );

    let after: i64 = sqlx::query("SELECT next_row_entry_order FROM user_table WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back")
        .get("next_row_entry_order");
    assert_eq!(after, 105, "and the stored value is incremented");
}

/// The same over `INT`, where arithmetic widening to `i64` and a declared
/// four-byte type meet.
///
/// Ignored: records an open bug in the *description*, not the value. Written as
/// a literal, `n - 5` on an `int4` column is correctly advertised as `int4`.
/// With a bound parameter, `n - $1` is advertised as `int8`, so a client asking
/// for `i32` is told its own type is wrong:
///
///   mismatched types; Rust type `i32` (as SQL type `INT4`) is not compatible
///   with SQL type `INT8`
///
/// The value is right in both cases — this is loud rather than silent, and it
/// does not affect a `BIGINT` counter, which is the shape the audit path uses.
#[ignore = "open: a computed RETURNING with a bound parameter is typed int8 over an int4 column"]
#[tokio::test]
async fn a_computed_returning_over_int4_is_readable() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, n INT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, n) VALUES (1, 100)"))
        .await
        .expect("seed");

    let row = sqlx::query("UPDATE t SET n = n + $2 WHERE id = $1 RETURNING n - $2 AS before, n AS now")
        .bind(1i32)
        .bind(5i32)
        .fetch_one(&pool)
        .await
        .expect("computed returning over int4");

    assert_eq!(row.get::<Option<i32>, _>("before"), Some(100));
    assert_eq!(row.get::<Option<i32>, _>("now"), Some(105));
}

/// Repeated, the way a reservation counter is actually used — each call takes
/// the next block, so a wrong or empty answer compounds.
#[tokio::test]
async fn repeated_reservations_each_return_their_own_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table (id INT PRIMARY KEY, domain_id INT NOT NULL, \
         next_row_entry_order BIGINT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO user_table (id, domain_id, next_row_entry_order) VALUES (1, 9, 1)",
    ))
    .await
    .expect("seed");

    let mut seen = Vec::new();
    for _ in 0..5 {
        let row = sqlx::query(
            "UPDATE user_table SET next_row_entry_order = next_row_entry_order + $3 \
             WHERE id = $1 AND domain_id = $2 \
             RETURNING next_row_entry_order - $3 AS next_order",
        )
        .bind(1i32)
        .bind(9i32)
        .bind(2i64)
        .fetch_one(&pool)
        .await
        .expect("reservation");
        seen.push(row.get::<Option<i64>, _>("next_order"));
    }

    assert_eq!(
        seen,
        vec![Some(1), Some(3), Some(5), Some(7), Some(9)],
        "each reservation returns the value before its own increment"
    );
}
