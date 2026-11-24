//! Reads on a key/value-store table against the same table on files
//! (docs/kv-partitions.md): a primary-key point read, an indexed lookup, a
//! primary-key range, and on a hash-partitioned twin a routed point read, a
//! routed indexed lookup and an unrouted indexed one. Each table is created
//! on files and placed on a store (`STORAGE PLACE TABLE … AT 'kv:bench'`),
//! loaded with the same rows, then the database is reopened and every query
//! timed in process, the query cache off.
//!
//! ```text
//! kv_table_bench --dir D [--rows N] [--iters K] [--s3 ENDPOINT,BUCKET] [--reuse] [--only NAME] [--backend file|kv]
//!   cargo run --release -p rasterizeddb_core --example kv_table_bench -- --dir /mnt/…/kvb
//! ```
//!
//! The store is a local directory store under D unless `--s3` names one
//! (credentials from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, path
//! addressing). Per query: the median and p90 microseconds, and the bytes
//! the process read through `read(2)` per query (`/proc/self/io` `rchar`),
//! which a scan of a store file shows and a read of a few rows does not.

use std::time::Instant;

use rclite::Arc;

use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::rql::executor;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;

struct Args {
    dir: String,
    rows: u64,
    iters: u64,
    s3: Option<(String, String)>,
    /// Keep a database a previous run loaded.
    reuse: bool,
    /// Only the queries whose name contains this, only on this backend.
    only: Option<String>,
    backend: Option<String>,
}

fn args() -> Args {
    let mut a = Args { dir: String::new(), rows: 100_000, iters: 300, s3: None, reuse: false, only: None, backend: None };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--dir" => a.dir = value(),
            "--rows" => a.rows = value().parse().expect("--rows"),
            "--iters" => a.iters = value().parse().expect("--iters"),
            "--reuse" => a.reuse = true,
            "--only" => a.only = Some(value()),
            "--backend" => a.backend = Some(value()),
            "--s3" => {
                let v = value();
                let (e, b) = v.split_once(',').expect("--s3 ENDPOINT,BUCKET");
                a.s3 = Some((e.to_string(), b.to_string()));
            }
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!a.dir.is_empty(), "--dir is required");
    a
}

async fn run(db: &Arc<Database>, sql: &str) -> usize {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
    let out = executor::execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        panic!("{sql}: {}", String::from_utf8_lossy(&out[1..]));
    }
    out.len()
}

async fn storage(db: &Arc<Database>, sql: &str) {
    rasterizeddb_core::core::storage::sql::execute(sql, db).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("rchar: ").and_then(|v| v.trim().parse().ok())))
        .unwrap_or(0)
}

const BACKENDS: [&str; 2] = ["file", "kv"];

async fn load(db: &Arc<Database>, a: &Args) {
    match &a.s3 {
        Some((endpoint, bucket)) => {
            storage(
                db,
                &format!(
                    "STORAGE ADD STORE IF NOT EXISTS bench S3 WITH (ENDPOINT = '{endpoint}', BUCKET = '{bucket}', \
                     PREFIX = 'kvbench', ADDRESSING = 'path')"
                ),
            )
            .await
        }
        None => {
            let kv = format!("{}/store", a.dir);
            std::fs::create_dir_all(&kv).unwrap();
            storage(db, &format!("STORAGE ADD STORE IF NOT EXISTS bench DIRECTORY WITH (PATH = '{kv}', PREFIX = 'bench')"))
                .await
        }
    }
    for b in BACKENDS {
        if b == "kv" {
            storage(db, "STORAGE PLACE TABLE t_kv AT 'kv:bench'").await;
            storage(db, "STORAGE PLACE TABLE p_kv AT 'kv:bench'").await;
        }
        run(db, &format!("CREATE TABLE t_{b} (id BIGINT PRIMARY KEY, tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT)"))
            .await;
        run(db, &format!("CREATE INDEX t_{b}_n ON t_{b} (n)")).await;
        run(
            db,
            &format!(
                "CREATE TABLE p_{b} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT NOT NULL, label TEXT, \
                 PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 64"
            ),
        )
        .await;
        run(db, &format!("CREATE INDEX p_{b}_n ON p_{b} (n)")).await;
        let started = Instant::now();
        let mut i = 0u64;
        while i < a.rows {
            let end = (i + 1000).min(a.rows);
            let values: Vec<String> =
                (i..end).map(|k| format!("({k}, {}, {}, 'label-{k}')", k % 500, k % 1000)).collect();
            let values = values.join(", ");
            run(db, &format!("INSERT INTO t_{b} (id, tenant_id, n, label) VALUES {values}")).await;
            run(db, &format!("INSERT INTO p_{b} (id, tenant_id, n, label) VALUES {values}")).await;
            i = end;
        }
        println!("LOAD {b}: {} rows into t_{b} and p_{b} in {:.1} s", a.rows, started.elapsed().as_secs_f64());
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let a = args();
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(8);
    let _ = rasterizeddb_core::BATCH_SIZE.set(10_000);
    let _ = rasterizeddb_core::cache::query_cache::ENABLE_CACHE.set(false);
    let root = format!("{}/db", a.dir);
    std::fs::create_dir_all(&root).unwrap();
    if !a.reuse {
        let db = Arc::new(Database::new(&root).await);
        load(&db, &a).await;
        let _ = db.seal_for_shutdown(std::time::Duration::from_secs(120)).await;
        drop(db);
    }
    let opened = Instant::now();
    let db = Arc::new(Database::new(&root).await);
    println!("OPEN {:.2} s", opened.elapsed().as_secs_f64());

    let queries: [(&str, fn(&str, u64) -> String); 6] = [
        ("pk point", |b, k| format!("SELECT id, label FROM t_{b} WHERE id = {k}")),
        ("index n= (100 rows)", |b, k| format!("SELECT id FROM t_{b} WHERE n = {}", k % 1000)),
        ("pk range (100 rows)", |b, k| format!("SELECT id FROM t_{b} WHERE id >= {k} AND id < {}", k + 100)),
        ("part routed point", |b, k| format!("SELECT id, label FROM p_{b} WHERE tenant_id = {} AND id = {k}", k % 500)),
        ("part routed + index", |b, k| format!("SELECT id FROM p_{b} WHERE tenant_id = {} AND n = {}", k % 500, k % 1000)),
        ("part fan-out index", |b, k| format!("SELECT id FROM p_{b} WHERE n = {}", k % 1000)),
    ];
    println!("{:<22} {:>6} {:>10} {:>10} {:>14}", "query", "table", "p50 us", "p90 us", "read B/query");
    for (name, sql) in queries {
        if a.only.as_ref().is_some_and(|o| !name.contains(o.as_str())) {
            continue;
        }
        for b in BACKENDS {
            if a.backend.as_ref().is_some_and(|x| x != b) {
                continue;
            }
            // A first pass opens what the query reaches; then the timed pass.
            for k in 0..20 {
                run(&db, &sql(b, (k * 7919) % a.rows)).await;
            }
            let before = rchar();
            let mut times: Vec<u128> = Vec::with_capacity(a.iters as usize);
            for k in 0..a.iters {
                let key = (k * 104_729 + 13) % a.rows;
                let s = sql(b, key);
                let t = Instant::now();
                run(&db, &s).await;
                times.push(t.elapsed().as_micros());
            }
            let read = (rchar() - before) / a.iters;
            times.sort_unstable();
            let p = |q: f64| times[((times.len() as f64 - 1.0) * q) as usize];
            println!("{name:<22} {b:>6} {:>10} {:>10} {read:>14}", p(0.5), p(0.9));
        }
    }
}
