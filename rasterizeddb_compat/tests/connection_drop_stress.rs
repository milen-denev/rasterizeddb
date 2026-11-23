//! Stress for the one-off "server closed the connection mid multi-row INSERT"
//! (sqlx `UnexpectedEof`) seen once in a full compat run.
//!
//! Ignored and bounded: run it deliberately, against both backends, with the
//! server's log captured —
//!
//! ```text
//! RUST_LOG=warn STRESS_SECS=900 STRESS_ROWS=1500000 \
//!   cargo test --release -p rasterizeddb_compat --test connection_drop_stress \
//!   -- --ignored --nocapture --test-threads 1 2> server.log
//! ```
//!
//! Several sessions at once: three (`STRESS_INSERTERS`) insert 5,000-tuple statements into a
//! PRIMARY KEY table (one in twenty is a 50,000-tuple, ~2 MB statement), one
//! scans and sorts, one updates, one runs scans under a short
//! `statement_timeout`, and one opens raw sockets and abandons them
//! mid-scan. Any session other than the abandoning one that sees its
//! connection closed under it counts as a drop, and the test fails on any.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Default)]
struct Tally {
    statements: AtomicU64,
    rows: AtomicU64,
    expected_errors: AtomicU64,
    other_errors: AtomicU64,
    drops: AtomicU64,
}

/// A closed connection, as sqlx reports it. Everything else is an ordinary
/// statement error the session survives.
fn is_drop(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("unexpectedeof")
        || m.contains("expected to read")
        || m.contains("connection closed")
        || m.contains("broken pipe")
        || m.contains("connection reset")
        || m.contains("pool timed out")
}

async fn record(tally: &Tally, who: &str, sql_head: &str, result: Result<u64, sqlx::Error>, expected: &[&str]) -> bool {
    tally.statements.fetch_add(1, Ordering::Relaxed);
    match result {
        Ok(n) => {
            tally.rows.fetch_add(n, Ordering::Relaxed);
            true
        }
        Err(e) => {
            let text = e.to_string();
            if is_drop(&text) {
                tally.drops.fetch_add(1, Ordering::Relaxed);
                eprintln!("STRESS DROP [{who}] after {sql_head:.60}: {text}");
                false
            } else if expected.iter().any(|x| text.contains(x)) {
                tally.expected_errors.fetch_add(1, Ordering::Relaxed);
                true
            } else {
                tally.other_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("STRESS ERROR [{who}] {sql_head:.60}: {text}");
                true
            }
        }
    }
}

async fn connect(url: &str) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(url).await.expect("connect")
}

async fn run(server: &TestServer, label: &str) {
    let secs = env_u64("STRESS_SECS", 600);
    let target_rows = env_u64("STRESS_ROWS", 1_000_000);
    let url = server.url();
    let mut setup = connect(&url).await;
    setup
        .execute("CREATE TABLE p (id BIGINT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");
    drop(setup);

    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Tally::default());
    let inserted = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();

    let inserters = env_u64("STRESS_INSERTERS", 3);
    for w in 0..inserters {
        let (url, stop, tally, inserted) = (url.clone(), stop.clone(), tally.clone(), inserted.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let mut next = w * 100_000_000;
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) && inserted.load(Ordering::Relaxed) < target_rows {
                k += 1;
                let tuples = if k % 20 == 0 { 50_000 } else { 5_000 };
                let values = (next..next + tuples)
                    .map(|i| format!("({i}, 'tag{} {}', {i})", i % 977, "x".repeat((i % 7) as usize * 5)))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!("INSERT INTO p (id, tag, n) VALUES {values}");
                let r = conn.execute(AssertSqlSafe(sql)).await.map(|r| r.rows_affected());
                if record(&tally, "insert", "INSERT", r, &["could not serialize", "40001"]).await {
                    next += tuples;
                    inserted.fetch_add(tuples, Ordering::Relaxed);
                } else {
                    conn = connect(&url).await;
                }
            }
        }));
    }

    {
        let (url, stop, tally) = (url.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let queries = [
                "SELECT COUNT(*) FROM p WHERE tag LIKE '%zzq%'",
                "SELECT id, n FROM p ORDER BY n DESC LIMIT 50",
                "SELECT id, tag FROM p WHERE id IN (5, 500, 50000, 100000005) ORDER BY id",
                "SELECT n % 13 AS k, COUNT(*) FROM p GROUP BY n % 13",
            ];
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let sql = queries[i % queries.len()];
                i += 1;
                let r = sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await.map(|v| v.len() as u64);
                if !record(&tally, "scan", sql, r, &[]).await {
                    conn = connect(&url).await;
                }
            }
        }));
    }

    {
        let (url, stop, tally) = (url.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                // A 500-row range in what the first inserter has written.
                let start = (k * 7_919) % 1_000_000;
                let sql = format!("UPDATE p SET n = n + 1 WHERE id >= {start} AND id < {}", start + 500);
                let r = conn.execute(AssertSqlSafe(sql)).await.map(|r| r.rows_affected());
                if !record(&tally, "update", "UPDATE", r, &["could not serialize", "40001", "deadlock"]).await {
                    conn = connect(&url).await;
                }
            }
        }));
    }

    {
        let (url, stop, tally) = (url.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let _ = conn.execute("SET statement_timeout = 40").await;
            while !stop.load(Ordering::Relaxed) {
                let r = sqlx::query("SELECT id FROM p WHERE tag LIKE '%never%' ORDER BY n")
                    .fetch_all(&mut conn)
                    .await
                    .map(|v| v.len() as u64);
                if !record(&tally, "timeout", "timed scan", r, &["statement timeout", "57014"]).await {
                    conn = connect(&url).await;
                    let _ = conn.execute("SET statement_timeout = 40").await;
                }
            }
        }));
    }

    {
        // Raw sockets abandoned mid-scan: the closes these cause are expected,
        // and are what the other sessions must survive.
        let (stop, port) = (stop.clone(), server.port());
        let database = url.rsplit('/').next().unwrap().split('?').next().unwrap().to_string();
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let db = database.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    use std::io::Write;
                    let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) else { return };
                    let mut params = Vec::new();
                    for (k, v) in [("user", "rasterized"), ("database", db.as_str())] {
                        params.extend_from_slice(k.as_bytes());
                        params.push(0);
                        params.extend_from_slice(v.as_bytes());
                        params.push(0);
                    }
                    params.push(0);
                    let mut startup = ((params.len() + 8) as i32).to_be_bytes().to_vec();
                    startup.extend_from_slice(&196_608i32.to_be_bytes());
                    startup.extend_from_slice(&params);
                    let _ = s.write_all(&startup);
                    std::thread::sleep(Duration::from_millis(50));
                    let q = b"SELECT id FROM p WHERE tag LIKE '%gone%'\0";
                    let mut msg = vec![b'Q'];
                    msg.extend_from_slice(&((q.len() + 4) as i32).to_be_bytes());
                    msg.extend_from_slice(q);
                    let _ = s.write_all(&msg);
                    std::thread::sleep(Duration::from_millis(30));
                })
                .await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }));
    }

    let started = Instant::now();
    while Instant::now() < deadline && inserted.load(Ordering::Relaxed) < target_rows {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!(
            "[{label}] {:>5.0?} inserted {} statements {} rows-touched {} expected-errors {} other-errors {} drops {}",
            started.elapsed(),
            inserted.load(Ordering::Relaxed),
            tally.statements.load(Ordering::Relaxed),
            tally.rows.load(Ordering::Relaxed),
            tally.expected_errors.load(Ordering::Relaxed),
            tally.other_errors.load(Ordering::Relaxed),
            tally.drops.load(Ordering::Relaxed),
        );
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(120), t).await;
    }
    println!(
        "[{label}] DONE in {:.0?}: inserted {} statements {} other-errors {} drops {}",
        started.elapsed(),
        inserted.load(Ordering::Relaxed),
        tally.statements.load(Ordering::Relaxed),
        tally.other_errors.load(Ordering::Relaxed),
        tally.drops.load(Ordering::Relaxed),
    );
    assert_eq!(tally.drops.load(Ordering::Relaxed), 0, "[{label}] sessions saw their connection closed");
}

#[ignore = "stress: run deliberately, see the module docs"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn in_memory_server_keeps_its_connections() {
    let server = TestServer::start().await.expect("start");
    run(&server, "memory").await;
}

#[ignore = "stress: run deliberately, see the module docs"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn file_backed_server_keeps_its_connections() {
    let root = format!("/var/tmp/connection_drop_stress_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    struct Remove(String);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Remove(root.clone());
    let server = TestServer::start_file_backed(&root).await.expect("start");
    run(&server, "file").await;
}
