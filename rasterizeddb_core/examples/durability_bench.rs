// Commit-path measurements for the durability work. Run against a real disk
// (e.g. /var/tmp on ext4), never tmpfs: there fsync is nearly free and every
// number below means nothing.
//
//   durability_bench mixed  --dir D [--writers 16] [--readers 4] [--workers 4] [--secs 5]
//       N clients INSERT one row per statement (synchronous_commit on) while M
//       clients run point SELECTs; prints read latency p50/p99/max and the
//       write rate. Shows whether committers parked in fsync starve readers.
//   durability_bench insert --dir D [--rows 10000] [--iters 10]
//       One multi-row INSERT of --rows rows, --iters times; per-statement time.
//   durability_bench appends --dir D [--secs 3] [--payload 100]
//       Raw WAL appends (record + commit marker, sync deferred, never taken):
//       appends/sec, i.e. the synchronous_commit = off write path.
//       --shape row: begin + log + commit_txn_deferred (a row write);
//       --shape single: log_and_commit_deferred (an outcome record).

use std::time::{Duration, Instant};

use rclite::Arc;

use rasterizeddb_core::core::{
    database::Database,
    processor::concurrent_processor::ENABLE_CACHE,
    rql::{executor, lexer_s1::recognize_query_purpose},
};

struct Args {
    mode: String,
    dir: String,
    writers: usize,
    readers: usize,
    workers: usize,
    secs: u64,
    rows: usize,
    iters: usize,
    payload: usize,
    shape: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::new(),
        dir: String::new(),
        writers: 16,
        readers: 4,
        workers: 4,
        secs: 5,
        rows: 10_000,
        iters: 10,
        payload: 100,
        shape: "row".to_string(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].clone();
        let mut next = || {
            i += 1;
            argv.get(i).cloned().expect("missing value")
        };
        match arg.as_str() {
            "mixed" | "insert" | "appends" => a.mode = arg,
            "--dir" => a.dir = next(),
            "--writers" => a.writers = next().parse().unwrap(),
            "--readers" => a.readers = next().parse().unwrap(),
            "--workers" => a.workers = next().parse().unwrap(),
            "--secs" => a.secs = next().parse().unwrap(),
            "--rows" => a.rows = next().parse().unwrap(),
            "--iters" => a.iters = next().parse().unwrap(),
            "--payload" => a.payload = next().parse().unwrap(),
            "--shape" => a.shape = next(),
            other => panic!("unknown argument {other}"),
        }
        i += 1;
    }
    assert!(!a.mode.is_empty() && !a.dir.is_empty(), "usage: see the header");
    a
}

fn main() {
    let args = parse_args();
    let _ = ENABLE_CACHE.set(false);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        match args.mode.as_str() {
            "mixed" => mixed(&args).await,
            "insert" => insert(&args).await,
            "appends" => appends(&args),
            _ => unreachable!(),
        }
    });
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).expect("recognize");
    executor::execute(purpose, db.clone()).await
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

async fn mixed(args: &Args) {
    rasterizeddb_core::set_synchronous_commit(true);
    let db = Arc::new(Database::new(&args.dir).await);
    exec(&db, "CREATE TABLE IF NOT EXISTS r (id UBIGINT, name VARCHAR);").await;
    exec(&db, "CREATE TABLE IF NOT EXISTS w (id UBIGINT, name VARCHAR);").await;
    let seed: Vec<String> = (1..=1000).map(|i| format!("({i}, 'n{i}')")).collect();
    exec(&db, &format!("INSERT INTO r (id, name) VALUES {};", seed.join(","))).await;

    let stop = Instant::now() + Duration::from_secs(args.secs);
    let mut writers = Vec::new();
    for c in 0..args.writers {
        let db = db.clone();
        writers.push(tokio::spawn(async move {
            let mut n = 0u64;
            while Instant::now() < stop {
                let id = (c as u64) << 32 | n;
                exec(&db, &format!("INSERT INTO w (id, name) VALUES ({id}, 'x{id}');")).await;
                n += 1;
            }
            n
        }));
    }
    let mut readers = Vec::new();
    for c in 0..args.readers {
        let db = db.clone();
        readers.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut i = c as u64;
            while Instant::now() < stop {
                let t = Instant::now();
                exec(&db, &format!("SELECT id, name FROM r WHERE id = {};", i % 1000 + 1)).await;
                lat.push(t.elapsed());
                i += 7;
            }
            lat
        }));
    }
    let mut writes = 0;
    for w in writers {
        writes += w.await.unwrap();
    }
    let mut lat = Vec::new();
    for r in readers {
        lat.extend(r.await.unwrap());
    }
    lat.sort();
    println!(
        "mixed: writers={} readers={} workers={} | writes {:.0}/s | reads {} p50 {:?} p99 {:?} max {:?}",
        args.writers,
        args.readers,
        args.workers,
        writes as f64 / args.secs as f64,
        lat.len(),
        pct(&lat, 0.50),
        pct(&lat, 0.99),
        lat.last().copied().unwrap_or_default()
    );
}

async fn insert(args: &Args) {
    rasterizeddb_core::set_synchronous_commit(true);
    let db = Arc::new(Database::new(&args.dir).await);
    exec(&db, "CREATE TABLE IF NOT EXISTS m (id UBIGINT, a INTEGER, name VARCHAR);").await;
    let mut times = Vec::new();
    for it in 0..args.iters {
        let base = (it * args.rows) as u64;
        let values: Vec<String> = (0..args.rows as u64)
            .map(|i| format!("({}, {}, 'name_{}')", base + i, i % 97, i))
            .collect();
        let sql = format!("INSERT INTO m (id, a, name) VALUES {};", values.join(","));
        let t = Instant::now();
        let out = exec(&db, &sql).await;
        times.push(t.elapsed());
        assert_eq!(out.first(), Some(&0u8), "insert failed: {:?}", String::from_utf8_lossy(&out));
    }
    let mut sorted = times.clone();
    sorted.sort();
    println!(
        "insert: {} rows x {} | median {:?} min {:?} all {:?}",
        args.rows,
        args.iters,
        pct(&sorted, 0.5),
        sorted[0],
        times
    );
}

fn appends(args: &Args) {
    use rasterizeddb_core::core::wal::Wal;
    std::fs::create_dir_all(&args.dir).unwrap();
    let path = std::path::Path::new(&args.dir).join("bench.wal");
    let _ = std::fs::remove_file(&path);
    let wal = Wal::open(&path).unwrap();
    let payload = vec![0xA5u8; args.payload];
    let stop = Instant::now() + Duration::from_secs(args.secs);
    let mut n = 0u64;
    let mut off = 0u64;
    let started = Instant::now();
    // Bounded on bytes as well as time: the disk is shared and nearly full.
    while Instant::now() < stop && n < 1_000_000 {
        for _ in 0..256 {
            if args.shape == "single" {
                wal.log_and_commit_deferred(1, off, &payload).unwrap();
            } else {
                let txn = wal.begin();
                wal.log(txn, 1, off, &payload).unwrap();
                wal.commit_txn_deferred(txn).unwrap();
            }
            off += payload.len() as u64;
            n += 1;
        }
    }
    let el = started.elapsed();
    println!(
        "appends[{}]: payload {} B | {} in {:?} | {:.0}/s",
        args.shape,
        args.payload,
        n,
        el,
        n as f64 / el.as_secs_f64()
    );
    drop(wal);
    let _ = std::fs::remove_file(&path);
}
