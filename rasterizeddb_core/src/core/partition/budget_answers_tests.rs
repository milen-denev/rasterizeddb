//! Answers never depend on the open budget (P5b-2a review B's probe, made a
//! test): one script of reads and writes over partitioned tables, joins,
//! foreign keys both ways, `ON CONFLICT` and `RETURNING`, run under a budget
//! of one open partition and under an unbounded one, answers compared byte
//! for byte, and the catalogs' row figures compared, before and after a
//! restart and across a DROP and re-create.

use std::path::Path;

use rclite::Arc;

use super::table::PartitionedTable;
use crate::core::database::{Database, Relation};

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&path_str(dir), false, "budget_answers").await.unwrap())
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let rewritten = crate::core::rql::lexer_alter::unique_index_as_constraint(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(String::from_utf8_lossy(&out).into_owned());
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn table(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not partitioned"),
    }
}

fn pid_of_tenant(t: &PartitionedTable, tenant: i64) -> u32 {
    let epoch = t.epoch().unwrap();
    let pids = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map);
    assert_eq!(pids.len(), 1);
    pids[0]
}

async fn sizes_of(t: &PartitionedTable) -> (u64, u64, u64, Vec<(u32, &'static str, u64)>, u64) {
    let tot = t.totals().await.unwrap();
    let parts = t.partitions().await.unwrap().into_iter().map(|p| (p.pid, p.state, p.slots)).collect();
    (tot.data, tot.pointers, tot.slots, parts, t.rows_len().unwrap())
}

const SCRIPT: &[&str] = &[
    "CREATE TABLE parent (pid BIGINT PRIMARY KEY, name TEXT)",
    "CREATE TABLE pt (tenant_id BIGINT NOT NULL REFERENCES parent(pid), id BIGINT NOT NULL, v BIGINT, s TEXT, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 16",
    "CREATE TABLE pu (tenant_id BIGINT NOT NULL, k BIGINT NOT NULL, w BIGINT, PRIMARY KEY (tenant_id, k)) PARTITION BY HASH (tenant_id) PARTITIONS 16",
    "CREATE TABLE pk1 (tenant_id BIGINT PRIMARY KEY, label TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 16",
    "INSERT INTO pk1 (tenant_id, label) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j'),(11,'k'),(12,'l')",
    "CREATE TABLE child (cid BIGINT PRIMARY KEY, t BIGINT REFERENCES pk1(tenant_id), i BIGINT)",
    "INSERT INTO parent (pid, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j'),(11,'k'),(12,'l'),(13,'m'),(14,'n'),(15,'o'),(16,'p'),(17,'q'),(18,'r'),(19,'s'),(20,'t')",
    "@populate",
    "SELECT COUNT(*) FROM pt",
    "SELECT tenant_id, id, v FROM pt WHERE tenant_id = 7 ORDER BY id",
    "SELECT tenant_id, COUNT(*), SUM(v), MIN(v), MAX(v) FROM pt GROUP BY tenant_id ORDER BY tenant_id",
    "SELECT tenant_id, id, v FROM pt WHERE v % 7 = 3 ORDER BY tenant_id, id",
    "SELECT tenant_id, id FROM pt ORDER BY v DESC LIMIT 7",
    "SELECT p.tenant_id, p.id, u.w FROM pt p JOIN pu u ON p.tenant_id = u.tenant_id AND p.id = u.k ORDER BY p.tenant_id, p.id",
    "SELECT p.tenant_id, COUNT(*) FROM pt p JOIN pu u ON p.tenant_id = u.tenant_id WHERE p.tenant_id = 5 GROUP BY p.tenant_id",
    "SELECT a.name, COUNT(*) FROM parent a JOIN pt p ON a.pid = p.tenant_id GROUP BY a.name ORDER BY a.name",
    "SELECT a.pid, p.id FROM parent a LEFT JOIN pt p ON a.pid = p.tenant_id AND p.id = 3 ORDER BY a.pid",
    "UPDATE pt SET v = v + 1000 WHERE tenant_id = 3 AND id = 2 RETURNING tenant_id, id, v",
    "UPDATE pt SET v = v + 1 WHERE v % 5 = 0 RETURNING tenant_id, id, v",
    "DELETE FROM pt WHERE tenant_id = 4 AND id > 6 RETURNING tenant_id, id",
    "DELETE FROM pt WHERE v % 11 = 1 RETURNING tenant_id, id, v",
    "INSERT INTO pt (tenant_id, id, v, s) VALUES (2, 1, 999, 'x') ON CONFLICT (tenant_id, id) DO UPDATE SET v = EXCLUDED.v RETURNING tenant_id, id, v",
    "INSERT INTO pt (tenant_id, id, v, s) VALUES (2, 100, 1, 'y'), (9, 1, 2, 'y'), (13, 200, 3, 'y') ON CONFLICT (tenant_id, id) DO UPDATE SET v = pt.v + EXCLUDED.v RETURNING tenant_id, id, v",
    "INSERT INTO pt (tenant_id, id, v, s) VALUES (6, 1, 5, 'z') ON CONFLICT DO NOTHING RETURNING tenant_id, id",
    "INSERT INTO pt (tenant_id, id, v) VALUES (99, 1, 1)",
    "INSERT INTO child (cid, t, i) VALUES (1, 5, 3), (2, 8, 4), (3, 12, 1)",
    "INSERT INTO child (cid, t, i) VALUES (4, 55, 999)",
    "DELETE FROM pk1 WHERE tenant_id = 8",
    "DELETE FROM pk1 WHERE tenant_id = 9 RETURNING tenant_id, label",
    "UPDATE child SET t = 77 WHERE cid = 1",
    "UPDATE child SET t = 2 WHERE cid = 1 RETURNING cid, t",
    "INSERT INTO child (cid, t, i) VALUES (5, 3, 0) ON CONFLICT (cid) DO UPDATE SET t = EXCLUDED.t RETURNING cid, t",
    "SELECT c.cid, k.label FROM child c JOIN pk1 k ON c.t = k.tenant_id ORDER BY c.cid",
    "DELETE FROM pt WHERE tenant_id = 8 AND id = 4",
    "DELETE FROM parent WHERE pid = 12",
    "UPDATE pt SET id = 77 WHERE tenant_id = 12 AND id = 1",
    "INSERT INTO pu (tenant_id, k, w) SELECT tenant_id, id + 1000, v FROM pt WHERE tenant_id IN (1, 2, 3)",
    "UPDATE pu SET w = w * 2 FROM pt WHERE pu.tenant_id = pt.tenant_id AND pu.k = pt.id RETURNING pu.tenant_id, pu.k, pu.w",
    "DELETE FROM pu USING pt WHERE pu.tenant_id = pt.tenant_id AND pu.k = pt.id AND pt.v > 500 RETURNING pu.tenant_id, pu.k",
    "SELECT tenant_id, k, w FROM pu ORDER BY tenant_id, k",
    "SELECT tenant_id, id, v, s FROM pt ORDER BY tenant_id, id",
    "SELECT COUNT(DISTINCT v), AVG(v) FROM pt",
    "SELECT tenant_id FROM pt WHERE id IN (SELECT k FROM pu WHERE w > 10) ORDER BY tenant_id",
    "SELECT cid, t, i FROM child ORDER BY cid",
];

async fn run_script(db: &Arc<Database>) -> Vec<(String, Result<Vec<u8>, String>)> {
    let mut out = Vec::new();
    for sql in SCRIPT {
        if *sql == "@populate" {
            for t in 1..=20i64 {
                let vals: Vec<String> = (1..=12i64).map(|i| format!("({t}, {i}, {}, 's{i}')", t * 37 + i * 13)).collect();
                let s = format!("INSERT INTO pt (tenant_id, id, v, s) VALUES {}", vals.join(", "));
                out.push((s.clone(), try_exec(db, &s).await));
                let vals: Vec<String> = (1..=6i64).map(|i| format!("({t}, {i}, {})", t * 3 + i)).collect();
                let s = format!("INSERT INTO pu (tenant_id, k, w) VALUES {}", vals.join(", "));
                out.push((s.clone(), try_exec(db, &s).await));
            }
            continue;
        }
        out.push((sql.to_string(), try_exec(db, sql).await));
    }
    out
}

/// A budget of one against an unbounded one: every statement's answer byte
/// for byte the same, and the catalogs' row figures, before and after a
/// restart and across a DROP and re-create.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_never_depend_on_the_open_budget() {
    let small = tempfile::tempdir().unwrap();
    let big = tempfile::tempdir().unwrap();
    let b_small = super::budget::isolate_with(small.path(), 1, Some(0));
    let _ = &b_small;
    super::budget::isolate_with(big.path(), 100_000, Some(0));
    let ds = open(small.path()).await;
    let db = open(big.path()).await;
    let a = run_script(&ds).await;
    let b = run_script(&db).await;
    let mut diffs = 0;
    for ((sql, x), (_, y)) in a.iter().zip(b.iter()) {
        let same = match (x, y) {
            (Ok(x), Ok(y)) => x == y,
            (Err(x), Err(y)) => x == y,
            _ => false,
        };
        if !same {
            diffs += 1;
            eprintln!("budget answers differ: {sql}\n  small: {:?}\n  big:   {:?}", x.as_ref().map(|v| v.len()).map_err(|e| e.clone()), y.as_ref().map(|v| v.len()).map_err(|e| e.clone()));
        }
    }
    for name in ["pt", "pu"] {
        let (s, l) = (sizes_of(&table(&ds, name)).await, sizes_of(&table(&db, name)).await);
        assert_eq!(s.3, l.3, "{name}: SHOW PARTITIONS slots");
        assert_eq!((s.0, s.1, s.2, s.4), (l.0, l.1, l.2, l.4), "{name}: totals");
    }
    // After a restart (first catalog read stats the files).
    close(small.path(), ds);
    close(big.path(), db);
    let ds = open(small.path()).await;
    let db = open(big.path()).await;
    for name in ["pt", "pu"] {
        let (s, l) = (sizes_of(&table(&ds, name)).await, sizes_of(&table(&db, name)).await);
        assert_eq!(s.3, l.3, "{name}: SHOW PARTITIONS slots after restart");
    }
    for sql in ["SELECT tenant_id, id, v, s FROM pt ORDER BY tenant_id, id", "SELECT tenant_id, k, w FROM pu ORDER BY tenant_id, k"] {
        assert_eq!(try_exec(&ds, sql).await, try_exec(&db, sql).await, "after restart: {sql}");
    }
    // DROP and recreate under the same name: no kept size survives.
    for d in [&ds, &db] {
        let _ = try_exec(d, "DROP TABLE child").await;
        exec(d, "DROP TABLE pu").await;
        exec(d, "CREATE TABLE pu (tenant_id BIGINT NOT NULL, k BIGINT NOT NULL, w BIGINT, PRIMARY KEY (tenant_id, k)) PARTITION BY HASH (tenant_id) PARTITIONS 16").await;
        exec(d, "INSERT INTO pu (tenant_id, k, w) VALUES (1, 1, 1)").await;
    }
    let (s, l) = (sizes_of(&table(&ds, "pu")).await, sizes_of(&table(&db, "pu")).await);
    assert_eq!(s.3, l.3, "pu recreated: SHOW PARTITIONS slots");
    assert_eq!(diffs, 0, "{diffs} statements answered differently under a budget of one");
    close(small.path(), ds);
    close(big.path(), db);
    super::budget::release(small.path());
    super::budget::release(big.path());
}

