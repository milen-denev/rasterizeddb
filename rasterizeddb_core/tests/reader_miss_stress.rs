//! The concurrent reader miss: a reader must never see fewer rows than are
//! committed and never deleted.
//!
//! A fixed set of `N` rows is created up front and never deleted. Writers then
//! update them (fixed-width, which may go in place, and variable-width, which
//! relocates), insert and delete churn rows outside the set, and run vacuum,
//! settle and forget sweeps. Readers check the invariant constantly: a count
//! of the set is `N`, a scan returns all `N` ids exactly once, and a point
//! lookup of any one finds it.
//!
//! Bounded and seeded; `RDB_MISS_SECS` (default 3) sets the duration,
//! `RDB_MISS_MODE` picks `memory` or `file` (default both), `RDB_MISS_HOT=0`
//! spreads the writes, `RDB_MISS_CACHE=1` turns the query cache on.
//!
//! **The mechanism it pins** (found 2026-10-09, after a month open): the
//! scan's reclaimable gate — which skips versions vacuum may already have
//! freed — bounded its horizon by `oldest_active` alone. A reader's snapshot
//! keeps an updater that was running when it was taken invisible after it
//! commits; once the updater retires, `oldest_active` passes it. So for an
//! updated row the reader saw the *old* version as its visible one and the
//! gate called that version reclaimable (`xmax` below the horizon) and dropped
//! it, while the *new* version was rightly invisible: the row vanished from the
//! scan. The visibility rule itself said "visible" for the dropped pointer,
//! which is exactly what months of tracing reported. Fixed by bounding the
//! horizon by the oldest live snapshot (`VersionHorizon::sample`, the snapshot
//! registry in `mvcc/transaction.rs`). With the old bound this test misses
//! ~5 % of its reads; with the fix, none in half a million.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        processor::concurrent_processor::ENABLE_CACHE,
        row::row::vec_into_rows,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
    },
};
use rclite::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const N: u32 = 64;

fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

async fn exec(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    execute(purpose, db.clone()).await
}

async fn ok(db: &Arc<Database>, q: &str) {
    let out = exec(db, q).await;
    assert_ne!(out.first(), Some(&3), "{} failed: {}", q, String::from_utf8_lossy(&out[1..]));
}

/// Ids a SELECT returned, or `None` if it errored (errors are counted apart).
async fn ids(db: &Arc<Database>, q: &str) -> Option<Vec<i32>> {
    let out = exec(db, q).await;
    if out.first() != Some(&2) {
        return None;
    }
    let rows = vec_into_rows(&out[1..]).ok()?;
    Some(
        rows.iter()
            .map(|r| i32::from_le_bytes(r.columns[0].data.into_slice()[..4].try_into().unwrap()))
            .collect(),
    )
}

async fn count(db: &Arc<Database>, q: &str) -> Option<i64> {
    let out = exec(db, q).await;
    if out.first() != Some(&2) {
        return None;
    }
    let rows = vec_into_rows(&out[1..]).ok()?;
    let bytes = rows.first()?.columns.first()?.data.into_slice().to_vec();
    Some(match bytes.len() {
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        _ => -1,
    })
}

#[derive(Default)]
struct Tally {
    reads: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
    writes: AtomicU64,
    sweeps: AtomicU64,
}

async fn stress(db: Arc<Database>, secs: u64, label: &'static str) {
    ok(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, pad TEXT NOT NULL)").await;
    let values: Vec<String> = (0..N).map(|i| format!("({}, 0, 'p')", i)).collect();
    ok(&db, &format!("INSERT INTO t (id, v, pad) VALUES {}", values.join(", "))).await;

    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let tally = std::sync::Arc::new(Tally::default());
    let misses_log = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let mut tasks = Vec::new();

    // Writers: in-place-eligible and relocating updates of the fixed set.
    for w in 0..6u64 {
        let (db, stop, tally) = (db.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1) * 0x2545_F491;
            let mut xs = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            while !stop.load(Ordering::Relaxed) {
                // Hot mode: everyone on the same few rows, as the original
                // reproduction had it — contention, conflicts, retries.
                let id = if hot() { xs() % 2 } else { xs() % N as u64 };
                let q = if xs() % 2 == 0 {
                    format!("UPDATE t SET v = v + 1 WHERE id = {}", id)
                } else {
                    let len = 1 + xs() % 200;
                    format!("UPDATE t SET pad = '{}' WHERE id = {}", "x".repeat(len as usize), id)
                };
                let _ = exec(&db, &q).await;
                tally.writes.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // Churn outside the set.
    for w in 0..2u64 {
        let (db, stop, tally) = (db.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            let mut k = 1_000 + w as i64 * 1_000_000;
            while !stop.load(Ordering::Relaxed) {
                let _ = exec(&db, &format!("INSERT INTO t (id, v, pad) VALUES ({}, 1, 'c')", k)).await;
                let _ = exec(&db, &format!("DELETE FROM t WHERE id = {}", k)).await;
                k += 1;
                tally.writes.fetch_add(2, Ordering::Relaxed);
            }
        }));
    }
    // Vacuum, settle and forget.
    {
        let (db, stop, tally) = (db.clone(), stop.clone(), tally.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = exec(&db, "VACUUM t").await;
                db.transactions.settle_now();
                if let Some(table) = db.tables.get("t").map(|t| t.value().clone()) {
                    let _ = table.neutralize_aborted_now().await;
                }
                tally.sweeps.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }));
    }
    // Readers.
    for r in 0..6u64 {
        let (db, stop, tally, log) = (db.clone(), stop.clone(), tally.clone(), misses_log.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = 0xD1B5_4A32_D192_ED03u64 ^ (r + 3) * 0x9876_5431;
            let mut xs = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            while !stop.load(Ordering::Relaxed) {
                let which = xs() % 4;
                let pick = |r: u64| if hot() { r % 2 } else { r % N as u64 };
                let (q, verdict): (String, Option<String>) = match which {
                    0 => {
                        let q = format!("SELECT COUNT(*) FROM t WHERE id < {}", N);
                        match count(&db, &q).await {
                            None => (q, Some("error".into())),
                            Some(n) if n == N as i64 => (q, None),
                            Some(n) => (q, Some(format!("count {}", n))),
                        }
                    }
                    1 => {
                        let q = format!("SELECT id FROM t WHERE id < {}", N);
                        match ids(&db, &q).await {
                            None => (q, Some("error".into())),
                            Some(mut got) => {
                                got.sort();
                                let want: Vec<i32> = (0..N as i32).collect();
                                if got == want {
                                    (q, None)
                                } else {
                                    let missing: Vec<i32> =
                                        want.iter().copied().filter(|i| !got.contains(i)).collect();
                                    (q, Some(format!("{} rows, missing {:?}", got.len(), missing)))
                                }
                            }
                        }
                    }
                    2 => {
                        let id = pick(xs());
                        let q = format!("SELECT id FROM t WHERE id = {}", id);
                        match ids(&db, &q).await {
                            None => (q, Some("error".into())),
                            Some(got) if got.len() == 1 => (q, None),
                            Some(got) => (q, Some(format!("{} rows", got.len()))),
                        }
                    }
                    _ => {
                        let id = pick(xs());
                        // A non-key predicate: no index fast path.
                        let q = format!("SELECT id FROM t WHERE v >= 0 AND id = {}", id);
                        match ids(&db, &q).await {
                            None => (q, Some("error".into())),
                            Some(got) if got.len() == 1 => (q, None),
                            Some(got) => (q, Some(format!("{} rows", got.len()))),
                        }
                    }
                };
                tally.reads.fetch_add(1, Ordering::Relaxed);
                if let Some(v) = verdict {
                    if v == "error" {
                        tally.errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        tally.misses.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut log = log.lock();
                    if log.len() < 40 {
                        log.push(format!("{} -> {}", q, v));
                    }
                }
            }
        }));
    }

    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    let log = misses_log.lock().clone();
    eprintln!(
        "[{}] {} reads, {} misses, {} read errors, {} writes, {} sweeps",
        label,
        tally.reads.load(Ordering::Relaxed),
        tally.misses.load(Ordering::Relaxed),
        tally.errors.load(Ordering::Relaxed),
        tally.writes.load(Ordering::Relaxed),
        tally.sweeps.load(Ordering::Relaxed),
    );
    for line in &log {
        eprintln!("[{}]   {}", label, line);
    }
    assert_eq!(
        tally.misses.load(Ordering::Relaxed),
        0,
        "[{}] a reader missed rows that exist: {:?}",
        label,
        log
    );
}

/// Hot by default — everyone on the same two rows, the shape that reproduced
/// it. `RDB_MISS_HOT=0` spreads the writes over the whole set.
fn hot() -> bool {
    std::env::var("RDB_MISS_HOT").map_or(true, |v| v != "0")
}

/// `RDB_MISS_CACHE=1` runs with the query cache on, as production does.
fn init_cache() {
    let on = std::env::var("RDB_MISS_CACHE").is_ok_and(|v| v == "1");
    if on && ENABLE_CACHE.set(true).is_ok() {
        rasterizeddb_core::cache::query_cache::init(10_000);
    }
    ENABLE_CACHE.get_or_init(|| false);
}

fn secs() -> u64 {
    std::env::var("RDB_MISS_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

fn mode_enabled(mode: &str) -> bool {
    std::env::var("RDB_MISS_MODE").map_or(true, |m| m == mode)
}

#[test]
fn a_reader_never_misses_a_row_in_memory() {
    if !mode_enabled("memory") {
        return;
    }
    MAX_PERMITS_THREADS.get_or_init(|| 8);
    BATCH_SIZE.get_or_init(|| 1_000);
    init_cache();
    on_a_big_stack(async {
        let db = Arc::new(Database::new_named("", true, "missmem").await);
        stress(db, secs(), "memory").await;
    });
}

#[test]
fn a_reader_never_misses_a_row_on_files() {
    if !mode_enabled("file") {
        return;
    }
    MAX_PERMITS_THREADS.get_or_init(|| 8);
    BATCH_SIZE.get_or_init(|| 1_000);
    init_cache();
    on_a_big_stack(async {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::new_named(dir.path().to_str().unwrap(), false, "missfile").await);
        stress(db, secs(), "file").await;
        drop(dir);
    });
}
