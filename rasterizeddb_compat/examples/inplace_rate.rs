//! Measures how often the in-place column-update fast path is actually taken,
//! against traffic arriving through the wire rather than through `Table`.
//!
//! The fast path is only worth its latch and its gate if it fires. This reports
//! the hit rate for the two shapes that matter: an autocommit `UPDATE` with
//! nothing else running, and the same statement inside an explicit transaction.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;

async fn run(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = TestServer::start().await?;
    let pool = server.connect_sqlx().await?;

    // `amount` is fixed-width, which is what the fast path requires; `note` is
    // a string, which it must always decline.
    run(&pool, "CREATE TABLE m (id INT NOT NULL, amount BIGINT, note TEXT)").await;
    for id in 1..=200 {
        run(&pool, &format!("INSERT INTO m (id, amount, note) VALUES ({}, {}, 'x')", id, id)).await;
    }

    let table = server.database().table_for_tests("m").expect("table").clone();

    let before = table.in_place_column_updates();
    for round in 0..200i64 {
        run(&pool, &format!("UPDATE m SET amount = {} WHERE id = {}", round, round + 1)).await;
    }
    let autocommit = table.in_place_column_updates() - before;

    let before = table.in_place_column_updates();
    for round in 0..200i64 {
        use sqlx::Connection;
        let mut c = sqlx::PgConnection::connect(&server.url()).await?;
        sqlx::query("BEGIN").execute(&mut c).await?;
        sqlx::query(AssertSqlSafe(format!("UPDATE m SET amount = {} WHERE id = {}", round, round + 1)))
            .execute(&mut c)
            .await?;
        sqlx::query("COMMIT").execute(&mut c).await?;
    }
    let in_block = table.in_place_column_updates() - before;

    let before = table.in_place_column_updates();
    for round in 0..200i64 {
        run(&pool, &format!("UPDATE m SET note = 'r{}' WHERE id = {}", round, round + 1)).await;
    }
    let string_update = table.in_place_column_updates() - before;

    println!("in-place hit rate over 200 updates each:");
    println!("  autocommit, fixed-width column : {:>3} / 200", autocommit);
    println!("  inside BEGIN, fixed-width      : {:>3} / 200", in_block);
    println!("  autocommit, string column      : {:>3} / 200  (must be 0)", string_update);
    Ok(())
}
