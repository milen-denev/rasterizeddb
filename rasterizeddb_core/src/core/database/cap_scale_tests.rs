//! Plan item 4's measurement: what a table declaring `CAP_N` partitions
//! (default 2²⁰) costs to CREATE, hold at rest, reopen, list, read through
//! and drop, with `CAP_TENANTS` of them written. A measurement, not a check:
//! `#[ignore]`d, run by hand with `--ignored --nocapture`. `CAP_DIR` picks the
//! directory (default the temp directory, tmpfs here; give an ext4 path for
//! disk cost). The numbers go in docs/partitioning.md "The cap: as measured".

use std::path::Path;
use std::time::{Duration, Instant};

use crate::core::database::{Database, Relation};
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn rss_kib() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4
}

fn dir_bytes(p: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            total += if md.is_dir() { dir_bytes(&e.path()) } else { md.len() };
        }
    }
    total
}

fn partitioned(db: &Database, name: &str) -> rclite::Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not partitioned"),
    }
}

async fn exec(db: &rclite::Arc<Database>, sql: &str) -> (Duration, Vec<u8>) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap();
    let at = Instant::now();
    let out = Box::pin(crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    let took = at.elapsed();
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    (took, out)
}

async fn show(db: &rclite::Arc<Database>, sql: &str) -> (Duration, usize) {
    let statement = crate::core::introspect::parse(sql).unwrap().unwrap();
    let at = Instant::now();
    let out = crate::core::introspect::execute(&statement, db).await;
    let took = at.elapsed();
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    (took, out.len())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn declared_partition_scale() {
    let n = env_u32("CAP_N", 1 << 20);
    let tenants = env_u32("CAP_TENANTS", 1_000) as i64;
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = match std::env::var("CAP_DIR") {
        Ok(d) => tempfile::tempdir_in(d).unwrap(),
        Err(_) => tempfile::tempdir().unwrap(),
    };
    super::partitioned::cap_hooks::raise(base.path(), n);
    let root = base.path().join("db");
    let root_s = root.to_string_lossy().into_owned();
    eprintln!("CAP_N={n} CAP_TENANTS={tenants} dir={}", base.path().display());

    let rss0 = rss_kib();
    {
        let db = rclite::Arc::new(Database::new_named(&root_s, false, "cap_scale").await);
        let rss_open = rss_kib();
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
            "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT)",
        )
        .unwrap();
        let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
        let at = Instant::now();
        db.create_partitioned_table(ct, &["tenant_id"], n).await.unwrap();
        let create = at.elapsed();
        let rss_created = rss_kib();
        eprintln!(
            "create: {:.1} ms, rss +{} KiB ({:.1} B per declared), map dir {} B",
            ms(create),
            rss_created as i64 - rss_open as i64,
            (rss_created as f64 - rss_open as f64) * 1024.0 / n as f64,
            dir_bytes(&root),
        );

        let t = partitioned(&db, "acct");
        let at = Instant::now();
        for tenant in 0..tenants {
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
            if tenant % 256 == 255 {
                t.close_idle().await.unwrap();
            }
        }
        let insert = at.elapsed();
        t.close_idle().await.unwrap();
        eprintln!(
            "insert {tenants} tenants: {:.1} ms ({:.2} ms each), rss {} KiB over open",
            ms(insert),
            ms(insert) / tenants as f64,
            rss_kib() as i64 - rss_open as i64
        );

        for sql in [
            "SELECT n FROM acct WHERE tenant_id = 7",
            "SELECT n FROM acct WHERE tenant_id = 7",
            "SELECT COUNT(*) FROM acct",
            "SELECT COUNT(*) FROM acct",
            "SELECT tenant_id FROM acct WHERE n = 2",
        ] {
            let (took, _) = exec(&db, sql).await;
            eprintln!("{sql}: {:.1} ms", ms(took));
        }
        for sql in ["SHOW PARTITIONS acct", "SHOW TABLES"] {
            match crate::core::introspect::parse(sql) {
                Some(Ok(_)) => {
                    let (took, bytes) = show(&db, sql).await;
                    eprintln!("{sql}: {:.1} ms, {bytes} B reply", ms(took));
                }
                other => eprintln!("{sql}: not a SHOW here ({:?})", other.map(|o| o.err())),
            }
        }
        eprintln!("on disk after writes: {} B", dir_bytes(&root));
    }
    let rss_closed = rss_kib();
    eprintln!("rss after drop: +{} KiB over start", rss_closed as i64 - rss0 as i64);

    {
        let at = Instant::now();
        let db = rclite::Arc::new(Database::new_named(&root_s, false, "cap_scale").await);
        let reopen = at.elapsed();
        eprintln!("reopen: {:.1} ms, rss {} KiB (was {rss_closed})", ms(reopen), rss_kib());
        for sql in ["SELECT n FROM acct WHERE tenant_id = 7", "SELECT COUNT(*) FROM acct"] {
            let (took, out) = exec(&db, sql).await;
            eprintln!("after reopen {sql}: {:.1} ms ({} B)", ms(took), out.len());
        }
        let (took, bytes) = show(&db, "SHOW PARTITIONS acct").await;
        eprintln!("after reopen SHOW PARTITIONS acct: {:.1} ms, {bytes} B reply", ms(took));
        eprintln!("after reopen and reads: rss {} KiB", rss_kib());
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
            "CREATE TABLE acct2 (tenant_id BIGINT NOT NULL, n INT NOT NULL)",
        )
        .unwrap();
        let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
        let refused = db.create_partitioned_table(ct, &["tenant_id"], 64).await;
        eprintln!("a second table past the cap: {:?}", refused.err().map(|e| e.to_string()));
        let (took, _) = exec(&db, "DROP TABLE acct").await;
        eprintln!("drop: {:.1} ms, on disk after {} B", ms(took), dir_bytes(&root));
    }
}
