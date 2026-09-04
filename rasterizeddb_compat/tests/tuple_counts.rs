//! What `pg_stat_user_tables` says about live and dead tuples.
//!
//! These two columns are read by every PostgreSQL-shaped monitoring tool to
//! decide whether a table needs vacuuming, and those tools never see this
//! engine's own `SHOW TABLE STATS`. So they are the only numbers such a tool
//! gets, and an error in them is invisible from the outside.
//!
//! The bug this pins ran in the reassuring direction. `n_dead_tup` was derived
//! from dead *bytes*, and incremental vacuum never reclaims pointer *slots* —
//! the filter index refers to them as stable addresses — so a table can
//! accumulate thousands of dead slots per live row while its dead bytes stay
//! near zero. Measured on a live node, a heartbeat table with 8 live rows in
//! 38,030 slots reported a 1.5% dead ratio against a true 99.98%.
//!
//! An update through the wire protocol always relocates its row and so always
//! costs a slot, which is what makes the amplification reproducible here.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// `(n_live_tup, n_dead_tup)` for one table.
async fn tuples(pool: &sqlx::PgPool, table: &str) -> (i64, i64) {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = '{}'",
        table
    )))
    .fetch_one(pool)
    .await
    .expect("pg_stat_user_tables should answer");
    (row.get("n_live_tup"), row.get("n_dead_tup"))
}

/// The slot count the engine reports for the same table.
async fn slots(pool: &sqlx::PgPool, table: &str) -> i64 {
    sqlx::query(AssertSqlSafe("SHOW TABLES"))
        .fetch_all(pool)
        .await
        .expect("SHOW TABLES")
        .iter()
        .find(|r| r.get::<String, _>("table_name") == table)
        .map(|r| r.get::<i64, _>("row_slots"))
        .expect("the table is listed")
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_heavy_table_does_not_report_itself_healthy() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE lease (id INT PRIMARY KEY, holder TEXT NOT NULL)")
        .await
        .expect("create");
    for id in 1..=4 {
        run(&pool, &format!("INSERT INTO lease (id, holder) VALUES ({}, 'a')", id))
            .await
            .expect("insert");
    }

    // A table that has only ever been inserted into is all live and no dead.
    // This is the case a naive "assume every slot is dead until a sweep proves
    // otherwise" would have broken, and it would have had Postgres tooling
    // vacuuming a healthy table on no evidence.
    let (live, dead) = tuples(&pool, "lease").await;
    assert_eq!((live, dead), (4, 0), "four inserts, nothing dead");

    // Now the heartbeat shape: the same rows renewed over and over. Each value
    // is longer than the last so the row must relocate, which costs a slot.
    for round in 0..60 {
        for id in 1..=4 {
            run(
                &pool,
                &format!(
                    "UPDATE lease SET holder = '{}' WHERE id = {}",
                    "h".repeat(round + 2),
                    id
                ),
            )
            .await
            .expect("update");
        }
    }

    let slots = slots(&pool, "lease").await;
    let (live, dead) = tuples(&pool, "lease").await;

    assert!(slots > 200, "the updates should have accumulated slots, got {}", slots);
    // The heart of it. Before the fix this reported roughly `slots` live and
    // almost nothing dead — a table 98% dead reporting itself as healthy.
    assert_eq!(live, 4, "four rows are live however many slots they have used");
    assert_eq!(
        dead,
        slots - 4,
        "every slot that is not one of the four live rows is dead"
    );
    // The invariant the studio's PostgreSQL path depends on: it derives a row
    // count as `n_live_tup + n_dead_tup`, so the two must sum to the slots.
    assert_eq!(live + dead, slots, "the two columns must account for every slot");
}

/// An empty table reports nothing rather than something.
///
/// The clamp is what guarantees this: with no slots, no id high-water mark can
/// produce a live row.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_table_reports_no_tuples() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE empty_t (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create");

    assert_eq!(tuples(&pool, "empty_t").await, (0, 0));
}
