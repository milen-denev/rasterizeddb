//! A `UNIQUE` constraint is re-checked only when the statement writes to it.
//!
//! An `UPDATE … SET status = …` that never mentions `id` was rejected for a
//! duplicate `id`, on a table where every `id` was unique — a valid statement
//! refused, repeatedly, in a background worker. Re-validating a column the
//! statement does not assign can only produce a false positive: the row keeps
//! the value it already had, and that value already satisfied the constraint.
//!
//! The constraint still has to hold when the statement *does* write to it,
//! which is the other half of this file.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Row;

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

async fn fixture(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE j (id UUID NOT NULL, status TEXT, attempts INTEGER)")
        .await
        .expect("create");
    run(pool, "ALTER TABLE j ADD CONSTRAINT j_id_key UNIQUE (id)").await.expect("constraint");
    for i in 0..25 {
        run(
            pool,
            &format!(
                "INSERT INTO j (id, status, attempts) VALUES ('00000000-0000-4000-8000-{:012x}', 'pending', 0)",
                i
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", i, e));
    }
}

#[tokio::test]
async fn an_update_that_does_not_touch_the_key_is_not_rejected() {
    let (_s, pool) = server().await;
    fixture(&pool).await;

    // The shape the worker runs: many rows, none of them assigning `id`.
    for round in 0..6 {
        run(
            &pool,
            &format!(
                "UPDATE j SET status = 'processing{}', attempts = attempts + 1 \
                 WHERE id IN (SELECT id FROM j ORDER BY attempts ASC LIMIT 16384) RETURNING *",
                round
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("round {} was rejected: {}", round, e));
    }

    let n: i64 = sqlx::query("SELECT COUNT(*) AS n FROM j WHERE status = 'processing5'")
        .fetch_one(&pool)
        .await
        .expect("count")
        .try_get("n")
        .expect("n");
    assert_eq!(n, 25, "every row should have been updated");
}

/// The constraint still holds where it means something.
#[tokio::test]
async fn the_constraint_still_holds_when_the_key_is_written() {
    let (_s, pool) = server().await;
    fixture(&pool).await;

    // Assigning a key that another row already holds must be refused.
    let refused = run(
        &pool,
        "UPDATE j SET id = '00000000-0000-4000-8000-000000000000' \
         WHERE id = '00000000-0000-4000-8000-000000000001'",
    )
    .await;
    assert!(refused.is_err(), "a real duplicate must still be refused");

    // Inserting a duplicate must be refused too.
    let refused = run(
        &pool,
        "INSERT INTO j (id, status, attempts) VALUES ('00000000-0000-4000-8000-000000000000', 'x', 0)",
    )
    .await;
    assert!(refused.is_err(), "a duplicate insert must still be refused");

    // Assigning a key nothing else holds is fine, and the row keeps it.
    run(
        &pool,
        "UPDATE j SET id = '11111111-0000-4000-8000-000000000000' \
         WHERE id = '00000000-0000-4000-8000-000000000002'",
    )
    .await
    .expect("a free key may be assigned");

    let n: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM j WHERE id = '11111111-0000-4000-8000-000000000000'",
    )
    .fetch_one(&pool)
    .await
    .expect("count")
    .try_get("n")
    .expect("n");
    assert_eq!(n, 1);
}
