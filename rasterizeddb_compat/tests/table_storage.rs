//! What `SHOW TABLES` and `SHOW TABLE STATS` report, as a driver reads them.
//!
//! Two things are pinned here. The first is the **column list**, which had no
//! coverage at all: the wire protocol describes a statement's columns before it
//! runs, and for these statements the description and the rows come from two
//! places in `core::introspect` — a `result_fields` list and a row builder.
//! Nothing but a test holds them together, and when they drift the values do
//! not merely get the wrong labels, they land under the wrong columns.
//!
//! The second is that a table says **what is holding it**. This server is
//! in-memory, so every table here should say so — and "it does not survive a
//! restart" is exactly the sort of fact a console must not have to infer.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Column, Executor, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} should succeed: {}", sql, e))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_reports_what_is_holding_it() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE parts (id INT PRIMARY KEY, name TEXT NOT NULL, note TEXT)")
        .await
        .expect("create");
    for id in 1..=4 {
        run(&pool, &format!("INSERT INTO parts (id, name, note) VALUES ({}, 'p{}', 'n')", id, id))
            .await
            .expect("insert");
    }

    let listed = rows(&pool, "SHOW TABLES").await;
    assert_eq!(
        listed[0].columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
        vec![
            "table_name",
            "storage",
            "row_slots",
            "columns",
            "data_bytes",
            "pointer_bytes",
            "index_bytes",
            "total_bytes",
            "reclaimable_bytes",
            "filters_ready",
            "immutable",
            "primary_key",
        ],
        "the described columns and the rows come from two lists that must agree"
    );

    let parts = listed
        .iter()
        .find(|r| r.get::<String, _>("table_name") == "parts")
        .expect("the table is listed");
    // This server holds its database in memory, and that is what it must say.
    // The value is one word from a fixed vocabulary because a console groups and
    // colours by it — see `DbStorageProvider::kind`.
    assert_eq!(parts.get::<String, _>("storage"), "memory");
    // The values land under the right names, which is the half of the drift a
    // column-name check on its own would not catch.
    assert_eq!(parts.get::<i64, _>("row_slots"), 4);
    assert_eq!(parts.get::<i64, _>("columns"), 3);
    assert!(parts.get::<i64, _>("total_bytes") > 0);
    assert_eq!(parts.get::<String, _>("primary_key"), "id");

    // --- and the same question in detail ------------------------------------
    let stats: Vec<(String, String)> = rows(&pool, "SHOW TABLE STATS parts")
        .await
        .iter()
        .map(|r| (r.get::<String, _>("metric"), r.get::<String, _>("value")))
        .collect();
    let metric = |name: &str| {
        stats
            .iter()
            .find(|(m, _)| m == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no `{}` in {:?}", name, stats.iter().map(|(m, _)| m).collect::<Vec<_>>()))
    };

    assert_eq!(metric("storage"), "memory");
    assert_eq!(metric("storage_location"), "(in process memory, not persisted)");
    // Figures that did not exist before: the measured average row, the
    // proportion of the table that is dead, and whether reclamation is running
    // for this table — which `SHOW CONFIG` can only answer as "per table".
    assert!(metric("avg_row_bytes").parse::<u64>().expect("a number") > 0);
    assert!(metric("dead_ratio").ends_with('%'), "read as a proportion, not as bytes");
    assert_eq!(metric("vacuum_enabled"), "true");
    assert_eq!(metric("check_constraints"), "0");
    assert_eq!(metric("path_indexes"), "0");
    assert_eq!(metric("vacuum_panics"), "0");
}

/// `SHOW STATS` answers whether this database's commits can still be trusted.
///
/// A poisoned log, a failed data-file flush and a panicking vacuum service
/// were each visible only as one log line at the moment they happened; these
/// metrics are how an operator asks afterwards. The failure states themselves
/// are driven in `core::introspect`'s tests — this pins that they reach the
/// statement a client runs.
#[tokio::test(flavor = "multi_thread")]
async fn database_stats_report_durability_health() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    run(&pool, "CREATE TABLE health (id INT PRIMARY KEY)").await.expect("create");

    let stats: Vec<(String, String)> = rows(&pool, "SHOW STATS")
        .await
        .iter()
        .map(|r| (r.get::<String, _>("metric"), r.get::<String, _>("value")))
        .collect();
    let metric = |name: &str| {
        stats
            .iter()
            .find(|(m, _)| m == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no `{}` in {:?}", name, stats.iter().map(|(m, _)| m).collect::<Vec<_>>()))
    };

    assert_eq!(metric("wal_poisoned"), "false");
    assert_eq!(metric("wal_poison_reason"), "");
    assert_eq!(metric("wal_checkpoints_declined"), "0");
    assert_eq!(metric("wal_checkpoint_failures"), "0");
    assert_eq!(metric("wal_last_checkpoint_error"), "");
    assert_eq!(metric("data_flush_failure"), "");
    assert_eq!(metric("vacuum_panics"), "0");
}
