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

/// `SHOW TABLE STATS`'s value for `metric` on `table`.
async fn metric(pool: &sqlx::PgPool, table: &str, metric: &str) -> String {
    sqlx::query(AssertSqlSafe(format!("SHOW TABLE STATS {}", table)))
        .fetch_all(pool)
        .await
        .expect("SHOW TABLE STATS")
        .iter()
        .find(|r| r.get::<String, _>("metric") == metric)
        .map(|r| r.get::<String, _>("value"))
        .unwrap_or_else(|| panic!("no `{}` metric", metric))
}

/// Deletes count before any sweep has run.
///
/// The unswept figure is the ids handed out, an upper bound that counted
/// every deleted row as live. The deletes this instance has seen now come
/// off it.
#[tokio::test(flavor = "multi_thread")]
async fn deletes_are_not_counted_live() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE del_t (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create");
    for id in 1..=10 {
        run(&pool, &format!("INSERT INTO del_t (id, v) VALUES ({}, 'x')", id))
            .await
            .expect("insert");
    }
    // Never swept: an insert-only table gives the service nothing to find.
    assert_eq!(metric(&pool, "del_t", "live_rows").await, "not measured");
    assert_eq!(metric(&pool, "del_t", "slots_per_live_row").await, "not measured");

    run(&pool, "DELETE FROM del_t WHERE id <= 4").await.expect("delete");
    assert_eq!(tuples(&pool, "del_t").await, (6, 4), "four of ten deleted");
    assert_eq!(slots(&pool, "del_t").await, 10);

    // The two shapes `partition_twin` compares, since the split itself
    // depends on when each side was swept: they must answer, not fail alike.
    let sum: i64 = sqlx::query(
        "SELECT n_live_tup + n_dead_tup AS slots FROM pg_stat_user_tables WHERE relname = 'del_t'",
    )
    .fetch_one(&pool)
    .await
    .expect("a sum of the two columns")
    .get("slots");
    assert_eq!(sum, 10);
    let counted: bool = sqlx::query("SELECT reltuples >= 0 AS counted FROM pg_class WHERE relname = 'del_t'")
        .fetch_one(&pool)
        .await
        .expect("a comparison on reltuples")
        .get("counted");
    assert!(counted);
}

/// After a sweep, `live` is what it counted, carried forward by the writes
/// since, and `SHOW TABLE STATS` reports it with the amplification it means.
#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_measures_live_rows_and_the_writes_after_it_move_them() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE beat (id INT PRIMARY KEY, holder TEXT NOT NULL)")
        .await
        .expect("create");
    for id in 1..=10 {
        run(&pool, &format!("INSERT INTO beat (id, holder) VALUES ({}, 'a')", id))
            .await
            .expect("insert");
    }
    for round in 0..20 {
        run(
            &pool,
            &format!("UPDATE beat SET holder = '{}' WHERE id <= 5", "h".repeat(round + 2)),
        )
        .await
        .expect("update");
    }
    run(&pool, "DELETE FROM beat WHERE id IN (9, 10)").await.expect("delete");
    run(&pool, "VACUUM beat").await.expect("vacuum");

    let slots_now = slots(&pool, "beat").await;
    assert!(slots_now >= 110, "the updates cost a slot each, got {}", slots_now);
    assert_eq!(metric(&pool, "beat", "live_rows").await, "8");
    assert_eq!(
        metric(&pool, "beat", "slots_per_live_row").await,
        format!("{:.1}", slots_now as f64 / 8.0)
    );
    assert_eq!(tuples(&pool, "beat").await, (8, slots_now - 8));

    // Writes after the sweep move the measured count, an update not at all.
    run(&pool, "INSERT INTO beat (id, holder) VALUES (11, 'b'), (12, 'b')")
        .await
        .expect("insert");
    run(&pool, "DELETE FROM beat WHERE id = 1").await.expect("delete");
    run(&pool, "UPDATE beat SET holder = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz' WHERE id = 2")
        .await
        .expect("update");
    assert_eq!(metric(&pool, "beat", "live_rows").await, "9");
    let slots_now = slots(&pool, "beat").await;
    assert_eq!(tuples(&pool, "beat").await, (9, slots_now - 9));

    // The figure is what COUNT(*) says, which is the point of measuring it.
    let counted: i64 = sqlx::query("SELECT COUNT(*) AS n FROM beat")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get("n");
    assert_eq!(counted, 9);
}

/// A delete an older snapshot can still see is dead, as PostgreSQL counts
/// it: the sweep cannot reclaim it yet, but it is not a row.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_held_by_an_open_snapshot_is_not_live() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(&pool, "CREATE TABLE held_t (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create");
    for id in 1..=6 {
        run(&pool, &format!("INSERT INTO held_t (id, v) VALUES ({}, 'x')", id))
            .await
            .expect("insert");
    }
    let mut reader = pool.acquire().await.expect("a second connection");
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin");
    let seen: i64 = sqlx::query("SELECT COUNT(*) AS n FROM held_t")
        .fetch_one(&mut *reader)
        .await
        .expect("count")
        .get("n");
    assert_eq!(seen, 6);

    run(&pool, "DELETE FROM held_t WHERE id <= 2").await.expect("delete");
    run(&pool, "VACUUM held_t").await.expect("vacuum");
    assert_eq!(metric(&pool, "held_t", "live_rows").await, "4");
    assert_eq!(tuples(&pool, "held_t").await, (4, 2));

    reader.execute("COMMIT").await.expect("commit");
}

/// A partitioned table is measured once every materialized partition has
/// been swept, each partition's count clamped to its own slots.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_is_measured_over_its_partitions() {
    // File-backed: a partitioned table needs one.
    struct Root(std::path::PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Root(std::env::temp_dir().join(format!("tuple_counts_ptc_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    std::fs::create_dir_all(&root.0).expect("root");
    let server = TestServer::start_file_backed(&root.0.to_string_lossy())
        .await
        .expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    run(
        &pool,
        "CREATE TABLE ptc (tenant_id BIGINT NOT NULL, id INT NOT NULL, v TEXT NOT NULL, \
         PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 4",
    )
    .await
    .expect("create");
    for t in 1..=8 {
        for id in 1..=3 {
            run(&pool, &format!("INSERT INTO ptc (tenant_id, id, v) VALUES ({}, {}, 'a')", t, id))
                .await
                .expect("insert");
        }
    }
    assert_eq!(metric(&pool, "ptc", "live_rows").await, "not measured");
    assert_eq!(tuples(&pool, "ptc").await, (24, 0));

    for round in 0..5 {
        run(&pool, &format!("UPDATE ptc SET v = '{}' WHERE id = 1", "u".repeat(round + 2)))
            .await
            .expect("update");
    }
    run(&pool, "DELETE FROM ptc WHERE id = 3").await.expect("delete");
    // Before the sweep: ids handed out less the deletes seen.
    let slots_now = slots(&pool, "ptc").await;
    assert_eq!(slots_now, 24 + 8 * 5, "one slot per row per update");
    assert_eq!(tuples(&pool, "ptc").await, (16, slots_now - 16));

    run(&pool, "VACUUM ptc").await.expect("vacuum");
    assert_eq!(metric(&pool, "ptc", "live_rows").await, "16");
    assert_eq!(tuples(&pool, "ptc").await, (16, slots_now - 16));

    run(&pool, "INSERT INTO ptc (tenant_id, id, v) VALUES (1, 9, 'b')").await.expect("insert");
    assert_eq!(metric(&pool, "ptc", "live_rows").await, "17");
}
