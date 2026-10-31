//! The query cache across a second instance of the same files (phase 3 P5b
//! review): a partition closed and opened again, a plain table reloaded by
//! an `ALTER`, and a table dropped and created again under its name. Each
//! new instance climbs its cache generation by one per write from its own
//! floor, so it can reach a stamp an earlier instance left under the same
//! clause; the cache matches stamps exactly, so only a scope no other
//! instance shares keeps it from answering with the old instance's
//! pointers.
//!
//! Its own test binary: the cache is switched on per process
//! (`ENABLE_CACHE` is set once), and the other suites run with it off.
//!
//! Each case caches a clause at every generation the first instance reaches,
//! then writes through the second instance one row at a time and probes a
//! clause whenever the second instance's generation equals a stamp the first
//! left under it. Every probe must answer with every matching row.

use std::path::Path;

use rasterizeddb_core::core::database::{Database, Relation};
use rasterizeddb_core::core::partition::table::{Close, PartitionedTable};
use rclite::Arc;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn cache_on() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(512);
    rasterizeddb_core::cache::query_cache::init(10_000);
    let _ = rasterizeddb_core::core::processor::concurrent_processor::ENABLE_CACHE.set(true);
    assert!(rasterizeddb_core::cache::query_cache::enabled(), "the cache is off in this process");
}

async fn open(dir: &Path) -> Arc<Database> {
    cache_on();
    Arc::new(Database::try_new_named(&path_str(dir), false, "cache_reopen").await.unwrap())
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    rasterizeddb_core::core::wal::remove_for(&path_str(dir));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(rasterizeddb_core::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

async fn count(db: &Arc<Database>, sql: &str) -> usize {
    let out = exec(db, sql).await;
    assert_eq!(out.first(), Some(&2), "{sql}: not a row set");
    rasterizeddb_core::core::row::row::vec_into_rows(&out[1..]).unwrap().len()
}

fn pt(db: &Database) -> Arc<PartitionedTable> {
    match db.relation("pt") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("pt is not partitioned"),
    }
}

fn pid_of_tenant(t: &PartitionedTable, tenant: i64) -> u32 {
    let epoch = t.epoch().unwrap();
    let pids = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map);
    assert_eq!(pids.len(), 1);
    pids[0]
}

/// The probe loop shared by every case: `generation` reads the current
/// instance's generation, `insert(k)` writes one more matching row.
async fn probe_second_instance<G, I, Fg, Fi>(
    stamps: &[(u64, String)],
    first_max: u64,
    total: &mut usize,
    select: impl Fn(&str) -> String,
    db: &Arc<Database>,
    mut generation: G,
    mut insert: I,
) -> (usize, Vec<String>)
where
    G: FnMut() -> Fg,
    Fg: std::future::Future<Output = u64>,
    I: FnMut(i64) -> Fi,
    Fi: std::future::Future<Output = ()>,
{
    let mut stale = Vec::new();
    let mut probed = 0;
    for k in 0..400i64 {
        let g = generation().await;
        if let Some((_, clause)) = stamps.iter().find(|(s, _)| *s == g) {
            probed += 1;
            let n = count(db, &select(clause)).await;
            if n != *total {
                stale.push(format!("generation {g}: `{clause}` returned {n} rows, {total} match"));
            }
        }
        if g > first_max {
            break;
        }
        insert(k).await;
        *total += 1;
    }
    (probed, stale)
}

/// A partition closed and opened again: the reopened instance must never
/// be served the closed one's entries. (At 8bdcc88c: 40 of 40 probes
/// returned the old pointer sets.)
///
/// Mutation: `instance_scope` returning the scope unqualified.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopened_partition_is_never_served_the_closed_instances_cache_entries() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 4",
    )
    .await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let generation = |t: Arc<PartitionedTable>| async move { t.open_part(p).await.unwrap().cache_generation() };
    let mut total = 0usize;
    let mut stamps: Vec<(u64, String)> = Vec::new();
    for i in 1..=40i64 {
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES (1, {i}, 7)")).await;
        total += 1;
        let g = generation(t.clone()).await;
        let clause = format!("tenant_id = 1 AND v = 7 AND id < {}", 100_000 + i);
        assert_eq!(count(&db, &format!("SELECT tenant_id, id, v FROM pt WHERE {clause}")).await, total);
        stamps.push((g, clause));
    }
    let first_max = generation(t.clone()).await;
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let (probed, stale) = probe_second_instance(
        &stamps,
        first_max,
        &mut total,
        |c| format!("SELECT tenant_id, id, v FROM pt WHERE {c}"),
        &db,
        || generation(t.clone()),
        |k| {
            let db = db.clone();
            async move {
                exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES (1, {}, 7)", 10_000 + k)).await;
            }
        },
    )
    .await;
    drop(t);
    close(&dir, db);
    assert!(stale.is_empty(), "stale cache answers after a close and reopen ({probed} probed):\n{}", stale.join("\n"));
}

/// The same across a plain table's `ALTER` reload and across `DROP` then
/// `CREATE` under the name: a later instance never answers from an earlier
/// one's entries, whether or not their generations meet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reloaded_or_recreated_plain_table_is_never_served_the_old_instances_cache_entries() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    for how in ["ALTER TABLE pl ADD COLUMN w BIGINT", "DROP TABLE pl"] {
        exec(&db, "CREATE TABLE pl (id BIGINT PRIMARY KEY, v BIGINT)").await;
        let generation = |db: Arc<Database>| async move { db.table_for_tests("pl").unwrap().cache_generation() };
        let mut total = 0usize;
        let mut stamps: Vec<(u64, String)> = Vec::new();
        for i in 1..=40i64 {
            exec(&db, &format!("INSERT INTO pl (id, v) VALUES ({i}, 7)")).await;
            total += 1;
            let g = generation(db.clone()).await;
            let clause = format!("v = 7 AND id < {}", 100_000 + i);
            assert_eq!(count(&db, &format!("SELECT id, v FROM pl WHERE {clause}")).await, total);
            stamps.push((g, clause));
        }
        let first_max = generation(db.clone()).await;
        exec(&db, how).await;
        if how.starts_with("DROP") {
            // The same rows again, under a new table of the name.
            exec(&db, "CREATE TABLE pl (id BIGINT PRIMARY KEY, v BIGINT)").await;
            total = 0;
            for i in 1..=40i64 {
                exec(&db, &format!("INSERT INTO pl (id, v) VALUES ({}, 7)", 50_000 + i)).await;
                total += 1;
            }
        }
        let (_probed, stale) = probe_second_instance(
            &stamps,
            first_max,
            &mut total,
            |c| format!("SELECT id, v FROM pl WHERE {c}"),
            &db,
            || generation(db.clone()),
            |k| {
                let db = db.clone();
                async move {
                    exec(&db, &format!("INSERT INTO pl (id, v) VALUES ({}, 7)", 10_000 + k)).await;
                }
            },
        )
        .await;
        assert!(stale.is_empty(), "stale cache answers after `{how}`:\n{}", stale.join("\n"));
        exec(&db, "DROP TABLE pl").await;
    }
    close(&dir, db);
}
