//! A literal carrying an explicit cast, e.g. `'x'::text`.
//!
//! PostgreSQL parses `'x'::text` as the string `x`. Clients and query builders
//! emit that form freely — it is how a value is pinned to a type when the
//! parameter's type is otherwise ambiguous. Stored verbatim it is not a value
//! at all: the quotes and the cast become part of the data, and every later
//! read returns `'x'::text` where the application wrote `x`.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

#[tokio::test]
async fn a_text_literal_with_a_cast_stores_the_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_probe (id INT PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    pool.execute(sqlx::raw_sql(
        "INSERT INTO utrn_probe (id, a, b) VALUES (1, 'etststest'::text, 'st'::text)",
    ))
    .await
    .expect("insert");

    let row = sqlx::query("SELECT a, b FROM utrn_probe WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");

    assert_eq!(row.get::<String, _>("a"), "etststest");
    assert_eq!(row.get::<String, _>("b"), "st");
}

#[tokio::test]
async fn casts_on_other_types_store_the_value() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE cast_kinds (id INT PRIMARY KEY, n INT NOT NULL, t TEXT NOT NULL, f DOUBLE PRECISION NOT NULL)",
    ))
    .await
    .expect("create");

    pool.execute(sqlx::raw_sql(
        "INSERT INTO cast_kinds (id, n, t, f) VALUES (1, 42::int4, 'x y'::varchar, 1.5::float8)",
    ))
    .await
    .expect("insert");

    let row = sqlx::query("SELECT n, t, f FROM cast_kinds WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");

    assert_eq!(row.get::<i32, _>("n"), 42);
    assert_eq!(row.get::<String, _>("t"), "x y");
    assert_eq!(row.get::<f64, _>("f"), 1.5);
}

/// The same form in an `UPDATE` assignment and in a `WHERE`.
#[tokio::test]
async fn casts_work_in_update_and_where() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE cast_upd (id INT PRIMARY KEY, a TEXT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql("INSERT INTO cast_upd (id, a) VALUES (1, 'one')"))
        .await
        .expect("insert");

    pool.execute(sqlx::raw_sql("UPDATE cast_upd SET a = 'two'::text WHERE id = 1"))
        .await
        .expect("update");

    let row = sqlx::query("SELECT a FROM cast_upd WHERE a = 'two'::text")
        .fetch_one(&pool)
        .await
        .expect("select by cast literal");
    assert_eq!(row.get::<String, _>("a"), "two");
}

/// The stripper must not damage a value that merely *contains* the punctuation.
///
/// A user is allowed to type `::` and quotes. These go in as inline literals —
/// the path the stripper actually runs on — so this is where that guard belongs.
#[tokio::test]
async fn inline_values_containing_punctuation_are_left_alone() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE punct (id INT PRIMARY KEY, a TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    // (id, SQL literal as written, what must be stored)
    for (id, literal, expected) in [
        (1i32, "'x::text'", "x::text"),
        (2, "'a::b::c'", "a::b::c"),
        (3, "'already'::text", "already"),
        (4, "'x::text'::text", "x::text"),
        (5, "'::'", "::"),
        (6, "''", ""),
    ] {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO punct (id, a) VALUES ({}, {})",
            id, literal
        )))
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", literal, e));

        let row = sqlx::query("SELECT a FROM punct WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("select {}: {}", literal, e));
        assert_eq!(
            row.get::<String, _>("a"),
            expected,
            "{} must store {:?}",
            literal,
            expected
        );
    }
}
