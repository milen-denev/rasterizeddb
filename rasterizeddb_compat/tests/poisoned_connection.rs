//! A failed statement must not poison the pooled connection it ran on.
//!
//! The chain this reproduces, end to end:
//!
//! 1. A statement fails while being *prepared*. sqlx's prepare is one batch —
//!    `Parse, Describe, Sync` — so an error at Parse leaves a `Describe` still
//!    queued behind it.
//! 2. A server that answers it anyway puts a `ParameterDescription` into the
//!    stream that the client never asked for.
//! 3. The client reads that stray message as part of its *next* query on the
//!    same connection, decoding a parameter count against the wrong buffer.
//! 4. `ParameterDescription::decode_body` reads a `u16` count and then does
//!    `get_u32()` that many times with no length check, so an overshooting
//!    count exhausts the buffer exactly: "advance out of bounds: the len is 0
//!    but advancing by 4" — a panic, not an error.
//!
//! Which is why the failure outlives any fix to the statement that failed, is
//! intermittent rather than per-request, and cannot be reproduced from the
//! shape of the failing statement alone: it needs the connection to have been
//! poisoned first.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table (id INT PRIMARY KEY, domain_id INT NOT NULL, \
         storage_mode INT NOT NULL, effective_storage_tier INT NOT NULL, \
         updated_at TIMESTAMP NOT NULL)",
    ))
    .await
    .expect("create user_table");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table_query_stat (user_table_id INT PRIMARY KEY, \
         last_queried_at TIMESTAMP)",
    ))
    .await
    .expect("create stat");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO user_table (id, domain_id, storage_mode, effective_storage_tier, updated_at) \
         VALUES (1, 9, 2, 1, '2024-01-01 00:00:00')",
    ))
    .await
    .expect("seed");
}

/// The demote query, verbatim in shape.
///
/// It used to fail — `ORDER BY COALESCE(...)` over a join was unsupported —
/// and this file used it as its convenient failing statement. It works now, so
/// it is kept as an assertion that it does, and the poisoning is provoked by
/// [`POISONING`] instead. What this file is really about is what the connection
/// does after *any* failed prepare, not about this statement.
const DEMOTE: &str = "SELECT t.id AS id, t.domain_id AS domain_id \
     FROM user_table t \
     LEFT JOIN user_table_query_stat s ON s.user_table_id = t.id \
     WHERE t.storage_mode = 2 AND t.effective_storage_tier = 1 \
       AND COALESCE(s.last_queried_at, t.updated_at) < $1 \
     ORDER BY COALESCE(s.last_queried_at, t.updated_at) ASC \
     LIMIT $2";

/// A statement that fails while being prepared, and will keep failing: it names
/// a column that does not exist. Carries a parameter, because the bug is in the
/// `ParameterDescription` the failed prepare leaves behind.
const POISONING: &str = "SELECT t.no_such_column FROM user_table t WHERE t.id = $1";

#[tokio::test]
async fn a_failed_prepare_does_not_poison_the_connection() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    // Pin one connection, so "the next query" is on the same socket the
    // failing statement ran on — which is what a pool does.
    let mut conn = pool.acquire().await.expect("acquire");

    for round in 1..=3 {
        let failed = sqlx::query(POISONING)
            .bind(1i32)
            .fetch_all(&mut *conn)
            .await;
        assert!(failed.is_err(), "round {}: this statement is expected to fail", round);

        // Now the connection must still work. Parameters matter: this is the
        // path that reads a ParameterDescription.
        let row = sqlx::query("SELECT id, domain_id FROM user_table WHERE id = $1")
            .bind(1i32)
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("round {}: connection poisoned: {}", round, e));

        assert_eq!(row.get::<i32, _>("id"), 1, "round {}", round);
        assert_eq!(row.get::<i32, _>("domain_id"), 9, "round {}", round);

        // And a differently-shaped statement, forcing a fresh prepare.
        let n: i64 = sqlx::query("SELECT COUNT(*) AS c FROM user_table WHERE domain_id = $1")
            .bind(9i32)
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("round {}: second statement failed: {}", round, e))
            .get("c");
        assert_eq!(n, 1, "round {}", round);
    }
}

/// The same thing through the pool rather than a pinned connection, which is
/// how the worker and the request handlers actually share sockets.
#[tokio::test]
async fn a_pool_survives_a_repeatedly_failing_statement() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    for round in 1..=5 {
        let _ = sqlx::query(POISONING).bind(1i32).fetch_all(&pool).await;

        let row = sqlx::query("SELECT id, domain_id FROM user_table WHERE id = $1")
            .bind(1i32)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("round {}: pool query failed after the error: {}", round, e));
        assert_eq!(row.get::<i32, _>("domain_id"), 9);
    }
}

/// The demote query itself, which this file used to rely on failing.
///
/// `ORDER BY COALESCE(s.a, t.b)` over a `LEFT JOIN` is evaluated over the
/// combined row now, so the statement runs. Asserted here so that if it ever
/// regresses to an error, the failure names the query rather than showing up as
/// a connection-poisoning test that mysteriously passes for the wrong reason.
#[tokio::test]
async fn the_demote_query_runs() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let rows = sqlx::query(DEMOTE)
        .bind(
            chrono::NaiveDateTime::parse_from_str("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        )
        .bind(10i64)
        .fetch_all(&pool)
        .await
        .expect("the demote query should run");
    // The one seeded row has updated_at 2024-01-01, before the bound cutoff,
    // and no stat row — so it falls through to `t.updated_at` and qualifies.
    assert_eq!(rows.len(), 1, "the unmatched row must fall through and qualify");
}
