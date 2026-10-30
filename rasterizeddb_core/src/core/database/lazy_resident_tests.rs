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
        exec(
            &db,
            &format!(
                "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT) PARTITION BY HASH (tenant_id) PARTITIONS {n}"
            ),
        )
        .await;
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
