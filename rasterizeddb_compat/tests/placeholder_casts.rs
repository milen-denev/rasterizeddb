//! A cast applied to a *bound placeholder*: `VALUES ($1::text)`.
//!
//! This is the shape pms's Ultra companion writer emits — every user value is
//! bound as a string and cast in SQL to the column's native type, so that one
//! text bridge serves every type instead of per-type value juggling. The value
//! is genuinely a parameter; only the placeholder carries the cast.
//!
//! That distinction matters: "it binds its parameters, so it cannot be
//! producing cast literals" is false for this shape, and it is how the stored
//! `'etststest'::text` went unexplained. Substituting `$1` into the statement
//! text before the cast is resolved reconstitutes exactly the literal form.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// The single-row insert, `native_codec.rs:260`.
#[tokio::test]
async fn a_cast_on_a_bound_placeholder_stores_the_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_native (row_id INT PRIMARY KEY, c_a TEXT NOT NULL, c_b BIGINT NOT NULL)",
    ))
    .await
    .expect("create");

    sqlx::query("INSERT INTO utrn_native (row_id, c_a, c_b) VALUES ($1, $2::text, $3::bigint)")
        .bind(1i32)
        .bind("etststest")
        .bind("42")
        .execute(&pool)
        .await
        .expect("insert");

    let row = sqlx::query("SELECT c_a, c_b FROM utrn_native WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<String, _>("c_a"), "etststest");
    assert_eq!(row.get::<i64, _>("c_b"), 42);
}

/// The update, `native_codec.rs:395`.
#[tokio::test]
async fn a_cast_on_a_placeholder_in_an_update_stores_the_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_upd (row_id INT PRIMARY KEY, c_a TEXT NOT NULL)",
    ))
    .await
    .expect("create");
    sqlx::query("INSERT INTO utrn_upd (row_id, c_a) VALUES ($1, $2::text)")
        .bind(1i32)
        .bind("before")
        .execute(&pool)
        .await
        .expect("insert");

    sqlx::query("UPDATE utrn_upd SET \"c_a\" = $1::text WHERE row_id = $2")
        .bind("after")
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("update");

    let row = sqlx::query("SELECT c_a FROM utrn_upd WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<String, _>("c_a"), "after");
}

/// A user is allowed to type `::`, and a quote.
///
/// The repair this bug will need keys on the shape `'<value>'::<type>`, so the
/// engine must not confuse a value that merely *contains* that punctuation with
/// a cast it applied itself.
#[tokio::test]
async fn values_that_contain_punctuation_survive_the_cast_form() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_punct (row_id INT PRIMARY KEY, c_a TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    for (id, value) in [
        (1i32, "x::text"),
        (2, "'quoted'"),
        (3, "'already'::text"),
        (4, "a::b::c"),
        (5, "it's fine"),
        (6, ""),
    ] {
        sqlx::query("INSERT INTO utrn_punct (row_id, c_a) VALUES ($1, $2::text)")
            .bind(id)
            .bind(value)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("insert {:?}: {}", value, e));

        let row = sqlx::query("SELECT c_a FROM utrn_punct WHERE row_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("select {:?}: {}", value, e));
        assert_eq!(
            row.get::<String, _>("c_a"),
            value,
            "a value containing punctuation must round-trip unchanged"
        );
    }
}
