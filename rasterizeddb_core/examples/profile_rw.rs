// Read/write profiling driver, split into two processes so one `perf record`
// covers one path only.
//
//   profile_rw write --dir /path/to/db --rows 200000 [--sync on|off]
//   profile_rw read  --dir /path/to/db --iters 20 --workload scan|point|narrow|agg|like|all
//
// `write` creates the table if it is missing and appends `--rows` rows through
// the SQL INSERT path — the same path a client statement takes, minus the wire
// protocol. `read` reopens that database and runs the chosen query shape in a
// loop. Both print wall-clock totals so the sample counts in the profile can be
// read against real time.

use std::time::Instant;

use rclite::Arc;

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        rql::{executor, lexer_s1::recognize_query_purpose},
    },
};

struct Args {
    mode: String,
    dir: String,
    rows: usize,
    iters: usize,
    workload: String,
    sync: bool,
    threads: usize,
    batch: usize,
    cache: bool,
    tune_alloc: bool,
    via_task: bool,
    clients: usize,
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::new(),
        dir: String::new(),
        rows: 200_000,
        iters: 20,
        workload: "scan".to_string(),
        sync: true,
        threads: 8,
        batch: 4096,
        cache: false,
        tune_alloc: false,
        via_task: false,
        clients: 1,
    };

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let mut next = || {
            i += 1;
            argv.get(i).cloned().unwrap_or_else(|| {
                eprintln!("missing value for argument");
                std::process::exit(2)
            })
        };
        match arg {
            "write" | "read" => a.mode = arg.to_string(),
            "--dir" => a.dir = next(),
            "--rows" => a.rows = next().parse().unwrap(),
            "--iters" => a.iters = next().parse().unwrap(),
            "--workload" => a.workload = next(),
            "--sync" => a.sync = next() == "on",
            "--threads" => a.threads = next().parse().unwrap(),
            "--batch" => a.batch = next().parse().unwrap(),
            "--cache" => a.cache = next() == "on",
            "--tune-alloc" => a.tune_alloc = next() == "on",
            "--via-task" => a.via_task = next() == "on",
            "--clients" => a.clients = next().parse().unwrap(),
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    if a.mode.is_empty() || a.dir.is_empty() {
        eprintln!(
            "usage: profile_rw <write|read> --dir PATH [--rows N] [--iters N] [--workload scan|point|narrow|agg|like|all] [--sync on|off] [--cache on|off] [--tune-alloc on|off]"
        );
        std::process::exit(2);
    }
    a
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();

    // What the server does at startup, so a profile of this driver is a profile
    // of the server's allocator behaviour. `--tune-alloc off` is the A/B.
    if args.tune_alloc {
        rasterizeddb_core::tune_allocator();
    }

    let _ = MAX_PERMITS_THREADS.get_or_init(|| args.threads);
    let _ = BATCH_SIZE.get_or_init(|| args.batch);
    let _ = ENABLE_CACHE.get_or_init(|| args.cache);
    rasterizeddb_core::set_synchronous_commit(args.sync);

    let database = Arc::new(Database::new(&args.dir).await);

    match args.mode.as_str() {
        "write" => write_mode(&database, &args).await,
        "read" => read_mode(&database, &args).await,
        _ => unreachable!(),
    }
}

async fn write_mode(database: &Arc<Database>, args: &Args) {
    exec(
        database,
        r##"CREATE TABLE IF NOT EXISTS bench (
            id UBIGINT,
            age INTEGER,
            salary DOUBLE,
            name VARCHAR,
            dept VARCHAR
        );"##,
    )
    .await;

    eprintln!(
        "write: {} rows over {} client(s), synchronous_commit={}",
        args.rows,
        args.clients,
        if args.sync { "on" } else { "off" }
    );
    let t0 = Instant::now();
    let per_client = args.rows / args.clients.max(1);
    let mut clients = Vec::with_capacity(args.clients);
    for c in 0..args.clients {
        let db = database.clone();
        let first = c * per_client;
        clients.push(tokio::spawn(async move {
            for i in first..first + per_client {
                let id = (i as u64) + 1;
                let age = (i % 90) as i32;
                let salary = 30_000.0 + ((i % 200_000) as f64);
                let sql = format!(
                    "INSERT INTO bench (id, age, salary, name, dept) VALUES ({}, {}, {}, 'employee_{}', 'dept_{}');",
                    id,
                    age,
                    salary,
                    id,
                    i % 32
                );
                exec(&db, &sql).await;
            }
        }));
    }
    for c in clients {
        c.await.expect("client task");
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "write done: {} rows in {:.2?} ({:.0} rows/s, {:.3} ms/row)",
        args.rows,
        elapsed,
        args.rows as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / args.rows as f64
    );
}

async fn read_mode(database: &Arc<Database>, args: &Args) {
    let workloads: Vec<&str> = if args.workload == "all" {
        vec![
            "scan",
            "point",
            "narrow",
            "agg",
            "like",
            "count",
            "topn",
            "topn_narrow",
        ]
    } else {
        vec![args.workload.as_str()]
    };

    for w in workloads {
        // Warm up: first touch of a table pays for opening its files.
        let _ = run(database, &query_for(w, 1, args.rows), args).await;

        let t0 = Instant::now();
        let mut bytes = 0usize;
        if args.clients <= 1 {
            for i in 0..args.iters {
                let sql = query_for(w, i as u64, args.rows);
                bytes += run(database, &sql, args).await.len();
            }
        } else {
            // Several statements in flight at once, which is the server's shape
            // and the case where the allocator's arenas matter.
            let per_client = args.iters / args.clients;
            let mut clients = Vec::with_capacity(args.clients);
            for c in 0..args.clients {
                let db = database.clone();
                let workload = w.to_string();
                let rows = args.rows;
                clients.push(tokio::spawn(async move {
                    let mut n = 0usize;
                    for i in 0..per_client {
                        let sql = query_for(&workload, (c * per_client + i) as u64, rows);
                        let purpose = recognize_query_purpose(&sql).expect("recognize");
                        n += executor::execute(purpose, db.clone()).await.len();
                    }
                    n
                }));
            }
            for c in clients {
                bytes += c.await.expect("client task");
            }
        }
        let elapsed = t0.elapsed();
        eprintln!(
            "read[{}]: {} iterations in {:.2?} ({:.2?} avg, {} bytes)",
            w,
            args.iters,
            elapsed,
            elapsed / (args.iters.max(1) as u32),
            bytes
        );
    }
}

async fn run(db: &Arc<Database>, sql: &str, args: &Args) -> Vec<u8> {
    if args.via_task {
        exec_on_worker(db, sql).await
    } else {
        exec(db, sql).await
    }
}

fn query_for(workload: &str, i: u64, rows: usize) -> String {
    // Every predicate below is selectivity-stable: it keeps the same fraction
    // of the table at any `--rows`, so the profiles compare across sizes.
    match workload {
        // Full scan, ~50% of rows returned: the payload path as much as the filter.
        "scan" => "SELECT id, age, salary, name FROM bench WHERE age > 44".to_string(),
        // Single-row lookup, a different row each time so a cache cannot serve it.
        "point" => format!(
            "SELECT id, age, salary, name, dept FROM bench WHERE id = {}",
            (i % rows.max(1) as u64) + 1
        ),
        // Full scan returning ~1%: the cost is the filter, not the payload.
        "narrow" => "SELECT id, salary FROM bench WHERE age = 42 AND salary > 30000.0".to_string(),
        // Aggregate over ~50% of the table.
        "agg" => "SELECT COUNT(*), AVG(salary) FROM bench WHERE age > 44".to_string(),
        // String predicate: the SIMD compare path.
        "like" => "SELECT id, name FROM bench WHERE name LIKE '%42'".to_string(),
        // A full pass that fetches no row payload at all: every slot is walked
        // and visibility-tested, and nothing else happens. This is the floor
        // cost of touching a table, and on the 192.168.1.7 node it is ~600 ns
        // per slot — 580 ms to count a million rows.
        "count" => "SELECT COUNT(*) FROM bench".to_string(),
        // `ORDER BY … LIMIT k`. The sort key defeats the early-exit that makes
        // a bare `LIMIT` cheap, so this pays the full pass above and then keeps
        // only k rows. Measured on that node: 1.5 ms without the ORDER BY,
        // ~860 ms with it, on the same million-row table.
        "topn" => "SELECT id, age, salary, name FROM bench ORDER BY name LIMIT 200".to_string(),
        // The same shape with the payload narrowed to the sort key alone. If
        // the cost were the payload rather than the pass, this would be much
        // cheaper than `topn`; on the live node it was only ~9 % cheaper, which
        // is what sent this profiling run looking at the pass instead.
        "topn_narrow" => "SELECT name FROM bench ORDER BY name LIMIT 200".to_string(),
        other => {
            eprintln!("unknown workload: {other}");
            std::process::exit(2)
        }
    }
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    executor::execute(purpose, db.clone()).await
}

/// Runs the statement on a runtime worker rather than on `block_on`'s thread.
///
/// The server never executes on the main thread, and glibc gives a worker its
/// own arena — whose heap is capped at 64 MiB, so an allocation larger than
/// that is mmap'ed however the mmap threshold is set. Whether allocator tuning
/// reaches the server at all turns on this.
async fn exec_on_worker(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let db = db.clone();
    let sql = sql.to_string();
    tokio::spawn(async move {
        let purpose = recognize_query_purpose(&sql).expect("recognize");
        executor::execute(purpose, db).await
    })
    .await
    .expect("query task")
}
