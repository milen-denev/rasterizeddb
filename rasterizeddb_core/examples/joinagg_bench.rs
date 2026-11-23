// Join and GROUP BY memory/time driver: one query per process, so the peak RSS
// it prints (VmHWM) belongs to that query alone.
//
//   joinagg_bench load --dir /var/tmp/x --facts 1000000 --dims 100000
//   joinagg_bench run  --dir /var/tmp/x --query j1|j2|j3|g1|g2|g3 [--iters 3]
//
// `facts(id, dim_id, g1k, g100k, amount)` with `g1k = id % 1000` and
// `g100k = id % 100000`; `dims(id, name, region)` with 100 regions. Every
// column is NOT NULL except `facts.note`, which is NULL on every other row.
//
// Peak RSS is reported after the database is opened and before the query runs
// (`base`) and after it (`peak`), so `peak - base` is what the query added.

use std::time::Instant;

use rclite::Arc;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

fn arg(name: &str) -> Option<String> {
    let argv: Vec<String> = std::env::args().collect();
    argv.iter()
        .position(|a| a == name)
        .and_then(|i| argv.get(i + 1).cloned())
}

fn hwm_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let db = db.clone();
    let sql = sql.to_string();
    // On a runtime worker, as the server runs a statement.
    tokio::spawn(async move {
        let purpose = recognize_query_purpose(&sql).expect("recognize");
        executor::execute(purpose, db).await
    })
    .await
    .expect("query task")
}

fn query(name: &str) -> &'static str {
    match name {
        // 1M x 100k inner join, 1M output rows.
        "j1" => "SELECT f.id, f.amount, d.name FROM facts f JOIN dims d ON d.id = f.dim_id",
        // The same join, aggregated by a column of the one side.
        "j2" => "SELECT d.region, SUM(f.amount), COUNT(*) FROM facts f JOIN dims d ON d.id = f.dim_id GROUP BY d.region",
        // The same join, sorted on an expression and paged.
        // (`ORDER BY a + b` over a join is refused by the planner; COALESCE
        // takes the same expression-sort path.)
        "j3" => "SELECT f.id, d.name FROM facts f JOIN dims d ON d.id = f.dim_id ORDER BY COALESCE(f.note, d.region) DESC, f.id LIMIT 10",
        // A column sort over the join, paged.
        "j5" => "SELECT f.id, d.name FROM facts f JOIN dims d ON d.id = f.dim_id ORDER BY d.name, f.id LIMIT 10",
        // The small side on the left: 100 dims joined to 1M facts.
        "j6" => "SELECT d.id, f.amount FROM dims d JOIN facts f ON f.dim_id = d.id WHERE d.id < 100",
        // DISTINCT over the join.
        "j4" => "SELECT DISTINCT d.region, f.g1k FROM facts f JOIN dims d ON d.id = f.dim_id",
        // GROUP BY, 1k groups and 100k groups.
        "g1" => "SELECT g1k, SUM(amount), COUNT(*), MAX(amount) FROM facts GROUP BY g1k",
        "g2" => "SELECT g100k, SUM(amount), COUNT(*), MAX(amount) FROM facts GROUP BY g100k",
        // A nullable column in the fold.
        "g3" => "SELECT g1k, COUNT(note), AVG(amount) FROM facts GROUP BY g1k",
        other => panic!("unknown query {other}"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mode = std::env::args().nth(1).expect("load|run");
    let dir = arg("--dir").expect("--dir");
    let _ = MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = BATCH_SIZE.get_or_init(|| 4096);
    rasterizeddb_core::set_synchronous_commit(false);
    let database = Arc::new(Database::new(&dir).await);

    match mode.as_str() {
        "load" => {
            let facts: u64 = arg("--facts").map_or(1_000_000, |v| v.parse().unwrap());
            let dims: u64 = arg("--dims").map_or(100_000, |v| v.parse().unwrap());
            exec(&database, "CREATE TABLE facts (id BIGINT NOT NULL, dim_id BIGINT NOT NULL, g1k BIGINT NOT NULL, g100k BIGINT NOT NULL, amount BIGINT NOT NULL, note BIGINT)").await;
            exec(&database, "CREATE TABLE dims (id BIGINT NOT NULL, name TEXT NOT NULL, region BIGINT NOT NULL)").await;
            let t0 = Instant::now();
            let clients = 8u64;
            let mut tasks = Vec::new();
            for c in 0..clients {
                let db = database.clone();
                tasks.push(tokio::spawn(async move {
                    let mut i = c;
                    while i < dims {
                        let sql = format!(
                            "INSERT INTO dims (id, name, region) VALUES ({}, 'dim_{}', {})",
                            i, i, i % 100
                        );
                        let r = exec(&db, &sql).await;
                        assert_eq!(r.first(), Some(&0), "dims insert failed");
                        i += clients;
                    }
                    let mut i = c;
                    while i < facts {
                        let note = if i % 2 == 0 { (i % 7).to_string() } else { "NULL".to_string() };
                        let sql = format!(
                            "INSERT INTO facts (id, dim_id, g1k, g100k, amount, note) VALUES ({}, {}, {}, {}, {}, {})",
                            i,
                            (i * 7919) % dims,
                            i % 1000,
                            i % 100_000,
                            (i * 31) % 10_007,
                            note
                        );
                        let r = exec(&db, &sql).await;
                        assert_eq!(r.first(), Some(&0), "facts insert failed");
                        i += clients;
                    }
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
            eprintln!("loaded {} facts, {} dims in {:.1?}", facts, dims, t0.elapsed());
        }
        "run" => {
            let name = arg("--query").expect("--query");
            let iters: usize = arg("--iters").map_or(3, |v| v.parse().unwrap());
            let sql = query(&name);
            let base = hwm_kb();
            let mut times = Vec::new();
            let mut bytes = 0usize;
            let mut checksum = 0u64;
            for _ in 0..iters {
                let t = Instant::now();
                let out = exec(&database, sql).await;
                times.push(t.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(out.first(), Some(&2), "{}: {}", name, String::from_utf8_lossy(&out[1..]));
                bytes = out.len();
                // Order-insensitive: the rows' hashes summed.
                checksum = rasterizeddb_core::core::row::row::serialized_rows(&out[1..])
                    .map(|r| {
                        use std::hash::{BuildHasher, Hasher};
                        let mut h = std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default().build_hasher();
                        h.write(r.expect("row"));
                        h.finish()
                    })
                    .fold(0u64, u64::wrapping_add);
            }
            times.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "query={} result_bytes={} checksum={:016x} min_ms={:.0} median_ms={:.0} base_rss_mb={} peak_rss_mb={} added_mb={}",
                name,
                bytes,
                checksum,
                times[0],
                times[times.len() / 2],
                base / 1024,
                hwm_kb() / 1024,
                (hwm_kb().saturating_sub(base)) / 1024
            );
        }
        _ => panic!("load|run"),
    }
}
