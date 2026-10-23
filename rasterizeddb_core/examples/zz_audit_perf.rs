//! zz_audit_perf — TEMPORARY audit harness for the query-execution audit.
//! Delete before the audit finishes. Not part of the project.
//!
//!   zz_audit_perf write --dir D --rows N [--nrows N]
//!   zz_audit_perf read  --dir D --workload W [--table bench|bench_n] [--iters K]
//!
//! Counts global-allocator calls (System) per query and reads VmHWM/VmRSS so a
//! query's peak memory shows as the high-water-mark delta over the opened DB.
//! NOTE: MemoryBlock heap allocations go through mimalloc in memory_pool.rs and
//! are NOT counted here; only Vec/Box/String/HashMap through the global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rclite::Arc;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        row::row::vec_into_rows,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static BIG: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        if l.size() >= 1 << 20 {
            BIG.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new as u64, Ordering::Relaxed);
        if new >= 1 << 20 {
            BIG.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn proc_kb(key: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            return rest
                .trim_start_matches(':')
                .split_whitespace()
                .next()
                .and_then(|k| k.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

struct Args {
    mode: String,
    dir: String,
    rows: usize,
    nrows: usize,
    iters: usize,
    workload: String,
    table: String,
    threads: usize,
    batch: usize,
    cache: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::new(),
        dir: String::new(),
        rows: 200_000,
        nrows: 200_000,
        iters: 5,
        workload: "point".to_string(),
        table: "bench".to_string(),
        threads: 16,
        batch: 10_000,
        cache: false,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let mut next = || {
            i += 1;
            argv.get(i).cloned().expect("missing value")
        };
        match arg {
            "write" | "read" => a.mode = arg.to_string(),
            "--dir" => a.dir = next(),
            "--rows" => a.rows = next().parse().unwrap(),
            "--nrows" => a.nrows = next().parse().unwrap(),
            "--iters" => a.iters = next().parse().unwrap(),
            "--workload" => a.workload = next(),
            "--table" => a.table = next(),
            "--threads" => a.threads = next().parse().unwrap(),
            "--batch" => a.batch = next().parse().unwrap(),
            "--cache" => a.cache = next() == "on",
            other => panic!("unknown argument {other}"),
        }
        i += 1;
    }
    assert!(!a.mode.is_empty() && !a.dir.is_empty(), "usage");
    a
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();
    let _ = MAX_PERMITS_THREADS.get_or_init(|| args.threads);
    let _ = BATCH_SIZE.get_or_init(|| args.batch);
    let _ = ENABLE_CACHE.get_or_init(|| args.cache);
    if args.cache {
        rasterizeddb_core::cache::query_cache::init(1024);
    }
    rasterizeddb_core::set_synchronous_commit(false);

    let t_open = Instant::now();
    let database = Arc::new(Database::new(&args.dir).await);
    eprintln!("open took {:.2?}", t_open.elapsed());

    match args.mode.as_str() {
        "write" => write_mode(&database, &args).await,
        "read" => read_mode(&database, &args).await,
        _ => unreachable!(),
    }
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    executor::execute(purpose, db.clone()).await
}

fn row_tuple(i: usize) -> String {
    let id = (i as u64) + 1;
    let age = (i % 90) as i32;
    let salary = 30_000.0 + ((i % 200_000) as f64);
    format!(
        "({}, {}, {}, 'employee_{}', 'dept_{}')",
        id,
        age,
        salary,
        id,
        i % 32
    )
}

async fn fill(db: &Arc<Database>, table: &str, rows: usize) {
    let t0 = Instant::now();
    const PER: usize = 500;
    let mut i = 0;
    while i < rows {
        let end = (i + PER).min(rows);
        let tuples: Vec<String> = (i..end).map(row_tuple).collect();
        let sql = format!(
            "INSERT INTO {} (id, age, salary, name, dept) VALUES {};",
            table,
            tuples.join(", ")
        );
        let r = exec(db, &sql).await;
        if r.first() == Some(&3) {
            eprintln!("insert failed: {}", String::from_utf8_lossy(&r[1..]));
            std::process::exit(1);
        }
        i = end;
    }
    eprintln!(
        "filled {} with {} rows in {:.2?}",
        table,
        rows,
        t0.elapsed()
    );
}

async fn write_mode(db: &Arc<Database>, args: &Args) {
    for sql in [
        "CREATE TABLE IF NOT EXISTS bench (id UBIGINT NOT NULL, age INTEGER NOT NULL, salary DOUBLE NOT NULL, name VARCHAR NOT NULL, dept VARCHAR NOT NULL);",
        "CREATE TABLE IF NOT EXISTS bench_n (id UBIGINT, age INTEGER, salary DOUBLE, name VARCHAR, dept VARCHAR);",
        "CREATE TABLE IF NOT EXISTS depts (dept VARCHAR NOT NULL, region VARCHAR NOT NULL);",
    ] {
        let r = exec(db, sql).await;
        if r.first() == Some(&3) {
            eprintln!("ddl failed: {}", String::from_utf8_lossy(&r[1..]));
            std::process::exit(1);
        }
    }
    for d in 0..32 {
        let r = exec(
            db,
            &format!(
                "INSERT INTO depts (dept, region) VALUES ('dept_{}', 'region_{}');",
                d,
                d % 4
            ),
        )
        .await;
        if r.first() == Some(&3) {
            eprintln!("insert failed: {}", String::from_utf8_lossy(&r[1..]));
            std::process::exit(1);
        }
    }
    fill(db, "bench", args.rows).await;
    if args.nrows > 0 {
        fill(db, "bench_n", args.nrows).await;
    }
    // Let the filter maintenance service checkpoint before exit.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
}

fn query_for(w: &str, t: &str, i: u64, rows: usize) -> String {
    let r = rows.max(1) as u64;
    match w {
        "point" => format!(
            "SELECT id, age, salary, name, dept FROM {t} WHERE id = {}",
            (i * 7919) % r + 1
        ),
        "point_narrow" => format!("SELECT id FROM {t} WHERE id = {}", (i * 7919) % r + 1),
        "range_limit" => format!("SELECT id, name FROM {t} WHERE salary > 100000.0 LIMIT 10"),
        "range_late_limit" => format!(
            "SELECT id, name FROM {t} WHERE id >= {} LIMIT 10",
            r * 3 / 4
        ),
        "limit_only" => format!("SELECT id, name FROM {t} LIMIT 10"),
        "topn" => format!("SELECT id, name FROM {t} ORDER BY name LIMIT 10"),
        "topn_id" => format!("SELECT id, name FROM {t} ORDER BY id LIMIT 10"),
        "orderby_full" => format!("SELECT id FROM {t} ORDER BY age"),
        "groupby" => format!("SELECT dept, COUNT(*) FROM {t} GROUP BY dept"),
        "sum" => format!("SELECT SUM(salary) FROM {t}"),
        "count" => format!("SELECT COUNT(*) FROM {t}"),
        "count_where" => format!("SELECT COUNT(*) FROM {t} WHERE age > 44"),
        "distinct" => format!("SELECT DISTINCT dept FROM {t}"),
        "join" => format!(
            "SELECT b.id, d.region FROM {t} b JOIN depts d ON b.dept = d.dept WHERE b.age = 42"
        ),
        "join_all" => format!("SELECT b.id, d.region FROM {t} b JOIN depts d ON b.dept = d.dept"),
        "scan" => format!("SELECT id, age, salary, name FROM {t} WHERE age > 44"),
        "narrow" => format!("SELECT id, salary FROM {t} WHERE age = 42 AND salary > 30000.0"),
        "neq" => format!("SELECT id FROM {t} WHERE age != 42"),
        "in100" | "in1000" => {
            let n = if w == "in100" { 100 } else { 1000 };
            let list: Vec<String> = (0..n).map(|k| ((k * 97) % r + 1).to_string()).collect();
            format!("SELECT id FROM {t} WHERE id IN ({})", list.join(","))
        }
        "like_prefix" => format!("SELECT id FROM {t} WHERE name LIKE 'employee_1234%'"),
        "like_suffix" => format!("SELECT id, name FROM {t} WHERE name LIKE '%42'"),
        "colcmp" => format!("SELECT id FROM {t} WHERE age = id"),
        "update50k" => format!("UPDATE {t} SET age = age + 1 WHERE id <= 50000"),
        other => panic!("unknown workload {other}"),
    }
}

async fn read_mode(db: &Arc<Database>, args: &Args) {
    let workloads: Vec<&str> = if args.workload == "all" {
        vec![
            "point",
            "range_limit",
            "range_late_limit",
            "limit_only",
            "topn",
            "orderby_full",
            "groupby",
            "sum",
            "count",
            "distinct",
            "join",
            "scan",
            "narrow",
            "neq",
            "in100",
            "in1000",
            "like_prefix",
            "like_suffix",
        ]
    } else {
        args.workload.split(',').collect()
    };

    // Touch the table once so the file-open cost is not in the first sample.
    let _ = exec(db, &format!("SELECT id FROM {} WHERE id = 1", args.table)).await;
    if let Some(t) = db.tables.get(&args.table).map(|t| t.value().clone()) {
        let slots = t.io_pointers().unwrap().get_len().await
            / rasterizeddb_core::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64;
        let rows_len = t.io_rows().unwrap().get_len().await;
        eprintln!("table {} slots={} rows_file_bytes={}", args.table, slots, rows_len);
    }
    let hwm_open = proc_kb("VmHWM");
    let rss_open = proc_kb("VmRSS");

    for w in workloads {
        let mut times: Vec<f64> = Vec::new();
        let mut allocs: Vec<u64> = Vec::new();
        let mut bytes: Vec<u64> = Vec::new();
        let mut bigs: Vec<u64> = Vec::new();
        let mut out_len = 0usize;
        let mut rows_back = 0usize;
        let mut err: Option<String> = None;
        for i in 0..args.iters {
            let sql = query_for(w, &args.table, i as u64, args.rows);
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let b0 = BYTES.load(Ordering::Relaxed);
            let g0 = BIG.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let r = exec(db, &sql).await;
            let dt = t0.elapsed().as_secs_f64() * 1000.0;
            allocs.push(ALLOCS.load(Ordering::Relaxed) - a0);
            bytes.push(BYTES.load(Ordering::Relaxed) - b0);
            bigs.push(BIG.load(Ordering::Relaxed) - g0);
            times.push(dt);
            out_len = r.len();
            if r.first() == Some(&3) {
                err = Some(String::from_utf8_lossy(&r[1..]).to_string());
            } else if r.first() == Some(&2) {
                rows_back = vec_into_rows(&r[1..]).map(|v| v.len()).unwrap_or(usize::MAX);
            }
        }
        let hwm = proc_kb("VmHWM");
        let rss = proc_kb("VmRSS");
        let warm: Vec<f64> = times.iter().skip(1).copied().collect();
        let warm_avg = if warm.is_empty() {
            times[0]
        } else {
            warm.iter().sum::<f64>() / warm.len() as f64
        };
        let warm_min = warm.iter().copied().fold(f64::INFINITY, f64::min);
        let a_avg = allocs.iter().skip(1).sum::<u64>() as f64 / (allocs.len().max(2) - 1) as f64;
        let b_avg = bytes.iter().skip(1).sum::<u64>() as f64 / (bytes.len().max(2) - 1) as f64;
        let g_avg = bigs.iter().skip(1).sum::<u64>() as f64 / (bigs.len().max(2) - 1) as f64;
        println!(
            "{:<18} {:<8} cold={:>8.2}ms warm_avg={:>8.2}ms warm_min={:>8.2}ms rows={:<7} bytes={:<9} allocs/q={:>9.0} allocB/q={:>12.0} big(>=1MiB)/q={:>4.1} hwm_delta={:>7}kB rss_delta={:>7}kB{}",
            w,
            args.table,
            times[0],
            warm_avg,
            if warm_min.is_finite() { warm_min } else { times[0] },
            rows_back,
            out_len,
            a_avg,
            b_avg,
            g_avg,
            hwm.saturating_sub(hwm_open),
            rss as i64 - rss_open as i64,
            err.map(|e| format!(" ERROR={e}")).unwrap_or_default()
        );
    }
}
