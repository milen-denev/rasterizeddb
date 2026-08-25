//! An expression over two joined relations — `COALESCE(s.a, t.b)`.
//!
//! A qualified reference is resolved by splitting it at its dot, so handing a
//! whole function call to that resolver takes everything before the last dot
//! as a relation name:
//!
//!   unknown relation 'COALESCE(s.last_queried_at, t' in
//!   'COALESCE(s.last_queried_at, t.updated_at)'
//!
//! From production, in the auto-tier worker.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, updated_at TIMESTAMP NOT NULL, name TEXT NOT NULL)",
    ))
    .await
    .expect("create t");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE s (id INT PRIMARY KEY, t_id INT NOT NULL, last_queried_at TIMESTAMP)",
    ))
    .await
    .expect("create s");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO t (id, updated_at, name) VALUES (1, '2024-01-01 00:00:00', 'a')",
    ))
    .await
    .expect("seed t");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO t (id, updated_at, name) VALUES (2, '2024-02-02 00:00:00', 'b')",
    ))
    .await
    .expect("seed t2");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO s (id, t_id, last_queried_at) VALUES (1, 1, '2024-03-03 00:00:00')",
    ))
    .await
    .expect("seed s");
    pool.execute(sqlx::raw_sql("INSERT INTO s (id, t_id, last_queried_at) VALUES (2, 2, NULL)"))
        .await
        .expect("seed s2");
}

async fn probe(pool: &sqlx::PgPool, label: &str, sql: &str) -> Result<String, String> {
    match sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await {
        Ok(rows) => {
            let out = rows
                .iter()
                .map(|r| r.try_get::<i32, _>("id").map(|v| v.to_string()).unwrap_or("?".into()))
                .collect::<Vec<_>>()
                .join(",");
            println!("{:<34} -> ok [{}]", label, out);
            Ok(out)
        }
        Err(e) => {
            let msg = e.to_string().lines().next().unwrap_or("").to_string();
            println!("{:<34} -> {}", label, msg);
            Err(msg)
        }
    }
}

/// An expression spanning two joined relations, in `WHERE` and `ORDER BY`.
///
/// From `ultra_auto_worker`'s demote query, which reported
/// `unknown relation 'COALESCE(s.last_queried_at, t'` hourly in production.
///
/// The `WHERE` half was the worse of the two and is fixed. It returned the
/// **empty set** where rows matched, silently, for two independent reasons:
///
/// 1. `compare` reads the right-hand bytes as the left-hand *type*, so a
///    nineteen-byte timestamp literal was read as an eight-byte timestamp.
///    Both sides are now read as the same type.
/// 2. A NULL reaching a post-join predicate looked like a present value
///    holding its type's zeroes, so `COALESCE(a, b)` returned those zeroes
///    instead of falling through to `b`. The join now fetches each column's
///    `__null_` companion, carries the marker into the combined row, and the
///    evaluator honours it.
///
/// `ORDER BY` over the same expression still fails; see the test below.
#[tokio::test]
async fn coalesce_over_two_relations() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    // Row 1: s.last_queried_at = 2024-03-03, so COALESCE yields it.
    // Row 2: s.last_queried_at IS NULL, so COALESCE falls through to
    //        t.updated_at = 2024-02-02. Both are after 2024-01-15.
    let matched = probe(
        &pool,
        "WHERE COALESCE over two relations",
        "SELECT t.id AS id FROM t JOIN s ON s.t_id = t.id \
         WHERE COALESCE(s.last_queried_at, t.updated_at) > '2024-01-15 00:00:00' \
         ORDER BY t.id",
    )
    .await;
    assert_eq!(matched, Ok("1,2".to_string()), "both rows match the predicate");

    // The same with the columns also projected, which is a different fetch.
    let with_cols = probe(
        &pool,
        "WHERE COALESCE, columns projected",
        "SELECT t.id AS id, s.last_queried_at AS lq, t.updated_at AS up \
         FROM t JOIN s ON s.t_id = t.id \
         WHERE COALESCE(s.last_queried_at, t.updated_at) > '2024-01-15 00:00:00' \
         ORDER BY t.id",
    )
    .await;
    assert_eq!(with_cols, Ok("1,2".to_string()));

    // A NULL still reads as a NULL either side of the join.
    assert_eq!(
        probe(&pool, "IS NULL through the join",
            "SELECT t.id AS id FROM t JOIN s ON s.t_id = t.id WHERE s.last_queried_at IS NULL").await,
        Ok("2".to_string())
    );
    // And COALESCE never yields NULL when its fallback is NOT NULL.
    assert_eq!(
        probe(&pool, "COALESCE IS NULL", 
            "SELECT t.id AS id FROM t JOIN s ON s.t_id = t.id \
             WHERE COALESCE(s.last_queried_at, t.updated_at) IS NULL").await,
        Ok(String::new())
    );

    // A predicate on a plain column is unchanged: only row 2's t.updated_at
    // is after the cutoff.
    assert_eq!(
        probe(&pool, "plain column control",
            "SELECT t.id AS id FROM t JOIN s ON s.t_id = t.id \
             WHERE t.updated_at > '2024-01-15 00:00:00'").await,
        Ok("2".to_string())
    );
}

/// `ORDER BY` over an expression spanning two relations.
///
/// Ignored: still open. The term is handed to the qualified-column resolver,
/// which splits it at its last dot and calls what precedes that a relation:
/// `unknown relation 'COALESCE(s.last_queried_at, t'`. Sorting by an
/// expression the SELECT list does not carry needs the post-scan
/// evaluate-sort-page path that single-table queries use, extended to a join.
#[ignore = "open: ORDER BY over a cross-relation expression"]
#[tokio::test]
async fn order_by_an_expression_over_two_relations() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let ordered = probe(
        &pool,
        "ORDER BY COALESCE",
        "SELECT t.id AS id FROM t JOIN s ON s.t_id = t.id \
         ORDER BY COALESCE(s.last_queried_at, t.updated_at) ASC",
    )
    .await;
    assert_eq!(ordered, Ok("2,1".to_string()), "2024-02-02 sorts before 2024-03-03");
}
