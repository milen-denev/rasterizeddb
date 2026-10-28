//! Resident cost of a partitioned table at N = 1024 (docs/partitioning.md
//! §7.1, §15 phase 3's measures): open time, file descriptors, resident
//! memory, tokio tasks and idle wakeups per materialized partition, with
//! every partition holding one row. A measurement, not a check: `#[ignore]`d,
//! run by hand with `--ignored --nocapture`, and its numbers recorded in
//! "Phase 3 P5a as built".
//!
//! Wakeups are counted as the process's context switches (voluntary and
//! not, every thread, `/proc/self/task/*/status`) over an idle window, with
//! the database open and, as the baseline, after it closed.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::core::database::{Database, Relation};
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

const N: u32 = 1024;
const IDLE: Duration = Duration::from_secs(10);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn resident_cost_per_partition_at_1024() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let root_s = path_str(&root);
    {
        let db = Database::new_named(&root_s, false, "resident").await;
        let sql = format!(
            "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT) PARTITION BY HASH (tenant_id) PARTITIONS {N}"
        );
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).unwrap();
        let db = rclite::Arc::new(db);
        let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
        assert!(!out.is_empty() && out[0] != 3, "{}", String::from_utf8_lossy(&out));
        let t = partitioned(&db, "acct");
        let mut tenant = 0i64;
        while t.totals().await.unwrap().materialized < N {
            let qp = QueryPurpose::InsertRow(InsertRowData {
                table_name: "acct".into(),
                query: format!("INSERT INTO acct (tenant_id, n, label) VALUES ({tenant}, 1, 'x')"),
                on_conflict_do_nothing: false,
                on_conflict_target: Vec::new(),
                on_conflict_update: None,
            });
            let row = crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0).unwrap();
            let tx = db.transactions.begin();
            t.insert_row_in_txn(row, tx).await.unwrap();
            db.transactions.commit(tx);
            tenant += 1;
        }
        db.sync_durable().unwrap();
        drop(t);
        drop(db);
        crate::core::wal::remove_for(&root_s);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (fd0, rss0, tasks0) = (fds(), rss_kib(), tasks());
    let idle0 = idle_switches_per_sec().await;

    let started = Instant::now();
    let db = Database::new_named(&root_s, false, "resident").await;
    let open = started.elapsed();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t = partitioned(&db, "acct");
    assert_eq!(t.totals().await.unwrap().materialized, N);
    let (fd1, rss1, tasks1) = (fds(), rss_kib(), tasks());
    let idle1 = idle_switches_per_sec().await;
    drop(t);
    drop(db);
    crate::core::wal::remove_for(&root_s);

    let n = N as f64;
    println!(
        "RESIDENT N={N}: open {:.3} s ({:.1} us/partition); fds +{} ({:.2}/partition); rss +{} KiB ({:.1} KiB/partition); \
         tasks +{} ({:.2}/partition); idle context switches {:.1}/s open vs {:.1}/s baseline ({:.3}/s per partition)",
        open.as_secs_f64(),
        open.as_secs_f64() * 1e6 / n,
        fd1 as i64 - fd0 as i64,
        (fd1 as f64 - fd0 as f64) / n,
        rss1 as i64 - rss0 as i64,
        (rss1 as f64 - rss0 as f64) / n,
        tasks1 as i64 - tasks0 as i64,
        (tasks1 as f64 - tasks0 as f64) / n,
        idle1,
        idle0,
        (idle1 - idle0) / n,
    );
}
