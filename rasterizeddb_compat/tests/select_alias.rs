//! `SELECT col AS name` — the result column must carry the alias.
//!
//! A client addresses columns by the name it asked for. If the alias is not
//! applied, `row.get("n")` fails with `ColumnNotFound("n")` — an error about
//! the client's own query, naming nothing that leads to the cause.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// Ignored: records an open bug.
///
/// `SELECT next_row_entry_order AS n FROM t` returns the column named
/// `next_row_entry_order`, so `row.get("n")` fails with `ColumnNotFound("n")`.
/// It happens with and without bound parameters — sqlx prepares either way, so
/// the description a driver caches is what governs.
///
/// What I established, so the next attempt does not repeat it:
///
/// - the *execute* path already has the right label. Instrumented, its select
///   items come through as `items=1 visible=1 labels=["n"]`, and taking the
///   label from there changes nothing the client sees.
/// - the *describe* path names columns from the schema — `f.name` out of
///   `requested_row_fetch.columns_fetching_data` — but patching that branch
///   also changed nothing, and a trace showed it is never reached for this
///   statement. Some earlier branch of `describe_resolved` returns first, and
///   finding which one is where the fix starts.
///
/// Aliases *do* work over a join and over an aggregate, both of which build
/// their descriptions from select items. It is the plain single-table path
/// that drops them.
#[ignore = "open: a column alias is dropped on a single-table SELECT"]
#[tokio::test]
async fn a_column_alias_names_the_result_column() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, next_row_entry_order BIGINT NOT NULL, label TEXT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO t (id, next_row_entry_order, label) VALUES (1, 42, 'x')",
    ))
    .await
    .expect("seed");

    let row = sqlx::query("SELECT next_row_entry_order AS n FROM t WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select with alias");
    assert_eq!(
        row.try_get::<i64, _>("n").expect("the alias must name the column"),
        42
    );

    // Several at once, including one that renames to another column's name.
    let row = sqlx::query("SELECT id AS pk, label AS title, next_row_entry_order AS ord FROM t WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select with several aliases");
    assert_eq!(row.try_get::<i32, _>("pk").expect("pk"), 1);
    assert_eq!(row.try_get::<String, _>("title").expect("title"), "x");
    assert_eq!(row.try_get::<i64, _>("ord").expect("ord"), 42);

    // Without AS, which is also legal.
    let row = sqlx::query("SELECT label title FROM t WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select with a bare alias");
    assert_eq!(row.try_get::<String, _>("title").expect("bare alias"), "x");
}
