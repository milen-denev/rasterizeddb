//! Phase 3 P5b's measurements (docs/partitioning.md §15 phase 3): with
//! partitions opened on demand, what a start costs at N materialized
//! one-row partitions, and what each open and each closed partition costs
//! in file descriptors, resident memory, tokio tasks and idle wakeups, with
//! `ACTIVE` of them touched by a routed read. A measurement, not a check:
//! `#[ignore]`d, run by hand with `--ignored --nocapture`; `P5B_N` (default
//! 10,000, past phase 2's 2,048 cap, which this raises for its own directory
//! only) and `P5B_ACTIVE` (default 100) set the sizes. The numbers go in
//! "Phase 3 P5b as built". On tmpfs (the temp directory), which hides disk
//! cost: no fsync below is a disk's.
//!
//! Wakeups are the process's context switches over an idle window, as in
//! `partition_resident_tests`.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::core::database::{Database, Relation};
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

const IDLE: Duration = Duration::from_secs(10);

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn fds() -> usize {
    std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}

fn rss_kib() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4
}

fn context_switches() -> u64 {
    let mut total = 0;
    if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
        for t in tasks.flatten() {
            let status = std::fs::read_to_string(t.path().join("status")).unwrap_or_default();
            for line in status.lines() {
                if let Some(v) = line
                    .strip_prefix("voluntary_ctxt_switches:")
                    .or_else(|| line.strip_prefix("nonvoluntary_ctxt_switches:"))
                {
                    total += v.trim().parse::<u64>().unwrap_or(0);
                }
            }
        }
    }
    total
}

fn tasks() -> usize {
    tokio::runtime::Handle::current().metrics().num_alive_tasks()
}

async fn idle_switches_per_sec() -> f64 {
    let before = context_switches();
    tokio::time::sleep(IDLE).await;
    (context_switches() - before) as f64 / IDLE.as_secs_f64()
}

fn partitioned(db: &Database, name: &str) -> rclite::Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not partitioned"),
    }
}

async fn exec(db: &rclite::Arc<Database>, sql: &str) -> Vec<u8> {
    // Box::pin: the executor's future is large.
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap();
    let out = Box::pin(crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn lazy_resident_cost_and_startup() {
    let n = env_u32("P5B_N", 10_000);
    let active = env_u32("P5B_ACTIVE", 100).min(n);
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = tempfile::tempdir().unwrap();
    super::partitioned::cap_hooks::raise(base.path(), n.max(2048));
    let root = base.path().join("db");
    let root_s = path_str(&root);
    let populate_started = Instant::now();
    // Tenants by partition: the first tenant found for each, kept for the
    // routed reads below.
    let mut tenant_of: Vec<Option<i64>> = vec![None; n as usize];
    {
        let db = rclite::Arc::new(Database::new_named(&root_s, false, "lazy_resident").await);
        // Past the parser's 2,048 too: the statement's table, created with
        // `n` partitions directly.
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
            "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT)",
        )
        .unwrap();
        let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
        db.create_partitioned_table(ct, &["tenant_id"], n).await.unwrap();
        let t = partitioned(&db, "acct");
        let epoch = t.epoch().unwrap();
        let mut tenant = 0i64;
        let mut filled = 0u32;
        while filled < n {
            let pid = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map)[0];
            if tenant_of[pid as usize].is_none() {
                let qp = QueryPurpose::InsertRow(InsertRowData {
                    table_name: "acct".into(),
                    query: format!("INSERT INTO acct (tenant_id, n, label) VALUES ({tenant}, 1, 'x')"),
                    on_conflict_do_nothing: false,
                    on_conflict_target: Vec::new(),
                    on_conflict_update: None,
                });
                let row =
                    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0).unwrap();
                let tx = db.transactions.begin();
                t.insert_row_in_txn(row, tx).await.unwrap();
                db.transactions.commit(tx);
                tenant_of[pid as usize] = Some(tenant);
                filled += 1;
                // Bounded open set while populating, as a server with idle
                // close would keep it: otherwise 10,000 open partitions
                // would need 100,000 fds here.
                if filled % 256 == 0 {
                    t.close_idle().await.unwrap();
                }
            }
            tenant += 1;
        }
        t.close_idle().await.unwrap();
        db.sync_durable().unwrap();
        drop(epoch);
        drop(t);
        drop(db);
        crate::core::wal::remove_for(&root_s);
    }
    let populate = populate_started.elapsed();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (fd0, rss0, tasks0) = (fds(), rss_kib(), tasks());
    let idle0 = idle_switches_per_sec().await;

    // A start: nothing opens.
    let started = Instant::now();
    let db = rclite::Arc::new(Database::new_named(&root_s, false, "lazy_resident").await);
    let open = started.elapsed();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t = partitioned(&db, "acct");
    assert_eq!(t.materialized_pids().unwrap().len() as u32, n);
    assert_eq!(t.open_count(), 0);
    let (fd1, rss1, tasks1) = (fds(), rss_kib(), tasks());
    let idle1 = idle_switches_per_sec().await;

    // `active` routed reads, each opening its partition.
    let mut first_touch = Vec::with_capacity(active as usize);
    for pid in 0..active {
        let tenant = tenant_of[pid as usize].unwrap();
        let at = Instant::now();
        exec(&db, &format!("SELECT n FROM acct WHERE tenant_id = {tenant}")).await;
        first_touch.push(at.elapsed());
    }
    let mut warm = Vec::with_capacity(active as usize);
    for pid in 0..active {
        let tenant = tenant_of[pid as usize].unwrap();
        let at = Instant::now();
        exec(&db, &format!("SELECT n FROM acct WHERE tenant_id = {tenant}")).await;
        warm.push(at.elapsed());
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(t.open_count() as u32, active);
    let (fd2, rss2, tasks2) = (fds(), rss_kib(), tasks());
    let idle2 = idle_switches_per_sec().await;

    // Every one closed again.
    let close_started = Instant::now();
    let closed = t.close_idle().await.unwrap();
    let close_time = close_started.elapsed();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (fd3, rss3, tasks3) = (fds(), rss_kib(), tasks());

    drop(t);
    drop(db);
    crate::core::wal::remove_for(&root_s);

    let median = |v: &mut Vec<Duration>| {
        v.sort();
        v[v.len() / 2].as_secs_f64() * 1e6
    };
    let a = active as f64;
    println!(
        "LAZY N={n} ACTIVE={active} (tmpfs): populated in {:.1} s; start {:.3} s ({:.2} us/partition), \
         fds +{} rss +{} KiB tasks +{} idle {:.1}/s vs {:.1}/s baseline with none open; \
         after {active} routed reads: fds +{} ({:.2}/open partition), rss +{} KiB ({:.1} KiB/open partition), \
         tasks +{} ({:.2}/open partition), idle {:.1}/s ({:.3}/s per open partition); \
         first-touch median {:.0} us, warm median {:.0} us; \
         close of {closed} in {:.3} s, then fds +{} rss +{} KiB tasks +{} over the baseline",
        populate.as_secs_f64(),
        open.as_secs_f64(),
        open.as_secs_f64() * 1e6 / n as f64,
        fd1 as i64 - fd0 as i64,
        rss1 as i64 - rss0 as i64,
        tasks1 as i64 - tasks0 as i64,
        idle1,
        idle0,
        fd2 as i64 - fd1 as i64,
        (fd2 as f64 - fd1 as f64) / a,
        rss2 as i64 - rss1 as i64,
        (rss2 as f64 - rss1 as f64) / a,
        tasks2 as i64 - tasks1 as i64,
        (tasks2 as f64 - tasks1 as f64) / a,
        idle2,
        (idle2 - idle1) / a,
        median(&mut first_touch),
        median(&mut warm),
        close_time.as_secs_f64(),
        fd3 as i64 - fd0 as i64,
        rss3 as i64 - rss0 as i64,
        tasks3 as i64 - tasks0 as i64,
    );
}

/// What a `SHOW` statement takes, from parse to its rows.
async fn timed_show(db: &rclite::Arc<Database>, sql: &str) -> Duration {
    let statement = crate::core::introspect::parse(sql).unwrap().unwrap();
    let at = Instant::now();
    let out = crate::core::introspect::execute(&statement, db).await;
    let took = at.elapsed();
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    took
}

/// Waits for a partition's index load or build and filter backfill.
async fn settled(t: &crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>) {
    t.index_load_settled().await;
    while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// P5b-2's measurements (docs/partitioning.md "Phase 3 P5b-2 as built"):
/// `P5B2_N` partitions (default 1,024) of `P5B2_ROWS` rows each (default
/// 200, an index on `v`), under an open budget of `P5B2_BUDGET` (default 64)
/// with `index_storage = snapshot`:
///
/// - routed reads of every partition in turn, then the resident cost with
///   the budget full (fds, RSS, tasks, idle wakeups) and the evictions;
/// - catalog reads with every partition closed: the first after a start
///   (each closed partition's sizes read from its files once) against the
///   next (kept sizes), for `SHOW PARTITIONS` and `SHOW TABLES`;
/// - first touch: a routed read that opens its partition and the time to
///   its index ready, loading the snapshot the close sealed against
///   rebuilding (the seal skipped at close).
///
/// A measurement, `#[ignore]`d; tmpfs, so no fsync here is a disk's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn budget_resident_cost_catalog_reads_and_first_touch() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let n = env_u32("P5B2_N", 1024);
    let rows = env_u32("P5B2_ROWS", 200) as i64;
    let budget_n = env_u32("P5B2_BUDGET", 64) as usize;
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = tempfile::tempdir().unwrap();
    super::partitioned::cap_hooks::raise(base.path(), n.max(2048));
    let _snap = storage_override::set(base.path(), IndexStorage::Snapshot);
    let budget = crate::core::partition::budget::isolate_with(base.path(), budget_n, Some(0));
    let root = base.path().join("db");
    let root_s = path_str(&root);
    let mut tenant_of: Vec<Option<i64>> = vec![None; n as usize];
    let populate_started = Instant::now();
    {
        let db = rclite::Arc::new(Database::new_named(&root_s, false, "budget_resident").await);
        db.partition_maintenance().set_period(Duration::from_secs(3600));
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
            "CREATE TABLE acct (tenant_id BIGINT NOT NULL, v BIGINT NOT NULL, label TEXT)",
        )
        .unwrap();
        let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
        db.create_partitioned_table(ct, &["tenant_id"], n).await.unwrap();
        exec(&db, "CREATE INDEX acct_v ON acct (v)").await;
        let t = partitioned(&db, "acct");
        let epoch = t.epoch().unwrap();
        let mut tenant = 0i64;
        let mut filled = 0u32;
        while filled < n {
            let pid = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map)[0];
            if tenant_of[pid as usize].is_none() {
                let values: Vec<String> =
                    (0..rows).map(|i| format!("({tenant}, {}, 'x')", tenant * 1_000_000 + i)).collect();
                exec(&db, &format!("INSERT INTO acct (tenant_id, v, label) VALUES {}", values.join(", "))).await;
                tenant_of[pid as usize] = Some(tenant);
                filled += 1;
            }
            tenant += 1;
        }
        drop(epoch);
        // Every partition sealed by its close.
        for (_, part) in t.open_partitions_now() {
            settled(&part).await;
        }
        t.close_idle().await.unwrap();
        db.sync_durable().unwrap();
        drop(t);
        drop(db);
        crate::core::wal::remove_for(&root_s);
    }
    let populate = populate_started.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (fd0, rss0, tasks0) = (fds(), rss_kib(), tasks());
    let idle0 = idle_switches_per_sec().await;

    let db = rclite::Arc::new(Database::new_named(&root_s, false, "budget_resident").await);
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    let t = partitioned(&db, "acct");
    assert_eq!(t.open_count(), 0);

    // Catalog reads with every partition closed.
    let parts_first = timed_show(&db, "SHOW PARTITIONS acct").await;
    let mut parts_next = Vec::new();
    for _ in 0..5 {
        parts_next.push(timed_show(&db, "SHOW PARTITIONS acct").await);
    }
    let mut tables_next = Vec::new();
    for _ in 0..5 {
        tables_next.push(timed_show(&db, "SHOW TABLES").await);
    }

    // Routed reads of every partition in turn, under the budget.
    let evictions0 = budget.evictions();
    let mut touch = Vec::with_capacity(n as usize);
    for pid in 0..n {
        let tenant = tenant_of[pid as usize].unwrap();
        let at = Instant::now();
        exec(&db, &format!("SELECT v FROM acct WHERE tenant_id = {tenant} AND v = {}", tenant * 1_000_000 + 7)).await;
        touch.push(at.elapsed());
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let open_now = t.open_count();
    let (fd1, rss1, tasks1) = (fds(), rss_kib(), tasks());
    let idle1 = idle_switches_per_sec().await;
    let evicted = budget.evictions() - evictions0;

    // First touch to index ready: loaded from the close's snapshot, then
    // with the seal skipped at close (rebuilt).
    let sample: Vec<u32> = (0..n).step_by((n as usize / 32).max(1)).take(32).collect();
    let mut loaded = Vec::new();
    let mut origins_loaded = 0;
    t.close_idle().await.unwrap();
    for &pid in &sample {
        let at = Instant::now();
        let part = t.open_part(pid).await.unwrap();
        settled(&part).await;
        loaded.push(at.elapsed());
        origins_loaded += (part.index_origin() == Origin::Loaded) as usize;
    }
    // Written once each, so the next close must seal, and closed without it.
    crate::core::row::table::close_seal_hooks::set_skipped(true);
    for &pid in &sample {
        let tenant = tenant_of[pid as usize].unwrap();
        exec(&db, &format!("INSERT INTO acct (tenant_id, v, label) VALUES ({tenant}, -1, 'w')")).await;
    }
    t.close_idle().await.unwrap();
    crate::core::row::table::close_seal_hooks::set_skipped(false);
    let mut rebuilt = Vec::new();
    let mut origins_built = 0;
    for &pid in &sample {
        let at = Instant::now();
        let part = t.open_part(pid).await.unwrap();
        settled(&part).await;
        rebuilt.push(at.elapsed());
        origins_built += (part.index_origin() != Origin::Loaded) as usize;
    }
    t.close_idle().await.unwrap();
    drop(t);
    drop(db);
    crate::core::wal::remove_for(&root_s);
    crate::core::partition::budget::release(base.path());

    let median = |v: &mut Vec<Duration>| {
        v.sort();
        v[v.len() / 2].as_secs_f64() * 1e3
    };
    let p99 = |v: &mut Vec<Duration>| {
        v.sort();
        v[(v.len() * 99 / 100).min(v.len() - 1)].as_secs_f64() * 1e3
    };
    println!(
        "BUDGET N={n} ROWS={rows} BUDGET={budget_n} (tmpfs): populated in {:.1} s; \
         catalog with all closed: SHOW PARTITIONS first {:.2} ms, next median {:.2} ms; SHOW TABLES next median {:.2} ms; \
         {n} routed reads under the budget: median {:.2} ms p99 {:.2} ms, {evicted} evictions, {open_now} open after; \
         fds +{} rss +{} KiB tasks +{} idle {:.1}/s vs {:.1}/s with none open; \
         first touch to index ready over {} partitions: loaded median {:.2} ms ({origins_loaded} loaded), \
         rebuilt median {:.2} ms ({origins_built} rebuilt)",
        populate.as_secs_f64(),
        parts_first.as_secs_f64() * 1e3,
        median(&mut parts_next),
        median(&mut tables_next),
        median(&mut touch),
        p99(&mut touch),
        fd1 as i64 - fd0 as i64,
        rss1 as i64 - rss0 as i64,
        tasks1 as i64 - tasks0 as i64,
        idle1,
        idle0,
        sample.len(),
        median(&mut loaded),
        median(&mut rebuilt),
    );
}

/// The process's CPU time (user + system) so far.
fn cpu_time() -> Duration {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the command, which may hold spaces: utime and stime are
    // the 14th and 15th fields overall.
    let after = stat.rsplit_once(')').map(|(_, rest)| rest).unwrap_or("");
    let fields: Vec<&str> = after.split_whitespace().collect();
    let ticks: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0)
        + fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    Duration::from_millis(ticks * 1000 / hz)
}

/// The bytes this process caused to be read from and written to storage.
fn io_bytes() -> (u64, u64) {
    let io = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let get = |key: &str| {
        io.lines().find_map(|l| l.strip_prefix(key)).and_then(|v| v.trim().parse().ok()).unwrap_or(0)
    };
    (get("read_bytes:"), get("write_bytes:"))
}

/// P5b-2b's measurement (docs/partitioning.md "Phase 3 P5b-2b as built"):
/// the first unrouted read over `P5B2B_N` closed partitions (default 1,024)
/// of `P5B2B_ROWS` rows each (default 200, an index on `v`), every one
/// sealed by its close, with fan-out opens full (as before P5b-2b) against
/// scan-only, under `index_storage = rebuild` and `= snapshot`: the
/// statement, then everything its opens start until it settles (CPU, RSS,
/// bytes read and written), then closing them. `P5B2B_DIR` puts the
/// database there (an ext4 directory for the disk-bound part; the temp
/// directory, tmpfs, by default). A measurement, `#[ignore]`d.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn first_fan_out_cost_scan_only_against_full() {
    use crate::core::index_store::load::{settings::IndexStorage, storage_override};
    let n = env_u32("P5B2B_N", 1024);
    let rows = env_u32("P5B2B_ROWS", 200) as i64;
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = match std::env::var("P5B2B_DIR") {
        Ok(parent) => tempfile::tempdir_in(parent).unwrap(),
        Err(_) => tempfile::tempdir().unwrap(),
    };
    super::partitioned::cap_hooks::raise(base.path(), n.max(2048));
    let _budget = crate::core::partition::budget::isolate_with(base.path(), (n as usize) * 2, Some(0));
    for storage in [IndexStorage::Rebuild, IndexStorage::Snapshot] {
        let root = base.path().join(format!("db_{}", storage.name()));
        let root_s = path_str(&root);
        let _mode = storage_override::set(&root, storage);
        {
            let db = rclite::Arc::new(Database::new_named(&root_s, false, "fan_out_cost").await);
            db.partition_maintenance().set_period(Duration::from_secs(3600));
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
                "CREATE TABLE acct (tenant_id BIGINT NOT NULL, v BIGINT NOT NULL, label TEXT)",
            )
            .unwrap();
            let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
            db.create_partitioned_table(ct, &["tenant_id"], n).await.unwrap();
            exec(&db, "CREATE INDEX acct_v ON acct (v)").await;
            let t = partitioned(&db, "acct");
            let epoch = t.epoch().unwrap();
            let mut seen = vec![false; n as usize];
            let (mut tenant, mut filled) = (0i64, 0u32);
            while filled < n {
                let pid = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map)[0];
                if !seen[pid as usize] {
                    let values: Vec<String> = (0..rows).map(|i| format!("({tenant}, {}, 'x')", tenant * 1_000_000 + i)).collect();
                    exec(&db, &format!("INSERT INTO acct (tenant_id, v, label) VALUES {}", values.join(", "))).await;
                    seen[pid as usize] = true;
                    filled += 1;
                    if filled % 64 == 0 {
                        for (_, part) in t.open_partitions_now() {
                            settled(&part).await;
                        }
                        t.close_idle().await.unwrap();
                    }
                }
                tenant += 1;
            }
            for (_, part) in t.open_partitions_now() {
                settled(&part).await;
            }
            t.close_idle().await.unwrap();
            db.sync_durable().unwrap();
            drop(epoch);
            drop(t);
            drop(db);
            crate::core::wal::remove_for(&root_s);
        }
        // Three runs of each, interleaved, against the box's drift; medians.
        let mut results: Vec<(bool, f64, f64, f64, i64)> = Vec::new();
        for scan_only in [false, true, false, true, false, true] {
            if scan_only {
                crate::core::partition::table::scan_open_hooks::enable(&root);
            } else {
                crate::core::partition::table::scan_open_hooks::disable(&root);
            }
            let db = rclite::Arc::new(Database::new_named(&root_s, false, "fan_out_cost").await);
            db.partition_maintenance().set_period(Duration::from_secs(3600));
            let t = partitioned(&db, "acct");
            tokio::time::sleep(Duration::from_secs(1)).await;
            let (cpu0, rss0) = (cpu_time(), rss_kib());
            let at = Instant::now();
            exec(&db, "SELECT COUNT(*) FROM acct WHERE label = 'x'").await;
            let statement = at.elapsed();
            for (_, part) in t.open_partitions_now() {
                if !part.is_scan_only() {
                    settled(&part).await;
                }
            }
            let settled_at = at.elapsed();
            let (cpu1, rss1) = (cpu_time(), rss_kib());
            let closing = Instant::now();
            t.close_idle().await.unwrap();
            let close = closing.elapsed();
            results.push((
                scan_only,
                statement.as_secs_f64() * 1e3,
                settled_at.as_secs_f64() * 1e3,
                (cpu1 - cpu0).as_secs_f64() * 1e3 + close.as_secs_f64() * 0.0,
                rss1 as i64 - rss0 as i64,
            ));
            drop(t);
            drop(db);
            crate::core::wal::remove_for(&root_s);
        }
        for scan_only in [false, true] {
            let pick = |f: &dyn Fn(&(bool, f64, f64, f64, i64)) -> f64| {
                let mut v: Vec<f64> = results.iter().filter(|r| r.0 == scan_only).map(f).collect();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v[v.len() / 2]
            };
            println!(
                "FANOUT N={n} ROWS={rows} storage={} opens={}: statement median {:.1} ms, settled median {:.1} ms; CPU median {:.0} ms; RSS median +{:.0} KiB",
                storage.name(),
                if scan_only { "scan-only" } else { "full" },
                pick(&|r| r.1),
                pick(&|r| r.2),
                pick(&|r| r.3),
                pick(&|r| r.4 as f64),
            );
        }
        crate::core::partition::table::scan_open_hooks::enable(&root);
    }
}
