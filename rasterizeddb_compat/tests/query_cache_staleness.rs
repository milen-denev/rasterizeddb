//! The pointer cache, with the cache actually ON.
//!
//! Production runs `--enable-cache true`; the test harness sets
//! `ENABLE_CACHE=false`, so nothing in the suite exercised this path at all.
//! The cache holds the pointers matching a PREDICATE, and an entry is retired
//! by the table's cache generation — which a WRITE bumps. A transaction's
//! COMMIT bumps nothing. So a scan running while a writer is mid-transaction
//! caches the pre-write pointer set under the current generation; when that
//! writer commits, the set is wrong in both directions and every later reader
//! presenting the same generation sees nothing at all.
//!
//! `ENABLE_CACHE` is a process-global set once, so this lives in its own file.

use rasterizeddb_compat::TestServer;
use sqlx::Executor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn cache_on() {
    // Must win the race against the harness's `init_globals`, which sets it
    // false; both use the same `OnceLock`, so first writer wins.
    let _ = rasterizeddb_core::cache::query_cache::ENABLE_CACHE.set(true);
    rasterizeddb_core::cache::query_cache::init(10_000);
}

/// Ignored: it FAILS, reproducing the open concurrent-reader defect in the
/// configuration production actually runs. Run with `-- --ignored`.
///
/// Measured with the cache ON: ~8 misses in 60 reads — the same rate as with
/// it off, which is how the cache was ruled out as the cause. Its value is
/// that it makes the production configuration testable at all: nothing else
/// in the suite turns `ENABLE_CACHE` on, so the whole cache fast path
/// (`query_cache::lookup` and the pointer sets it serves) has no coverage.
#[ignore = "open defect: reader misses an existing row (reproduced with the cache on)"]
#[tokio::test]
async fn a_reader_never_misses_a_row_while_others_update_it() {
    cache_on();
    let _ = rasterizeddb_core::VACUUM_ENABLED.set(false);
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE t (id INT PRIMARY KEY, title TEXT NOT NULL)"))
        .await
        .expect("create");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, title) VALUES (1, 'start')"))
        .await
        .expect("insert");

    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..8u32 {
        let pool = pool.clone();
        let stop = stop.clone();
        writers.push(tokio::spawn(async move {
            let mut n = 0u32;
            while !stop.load(Ordering::Relaxed) && n < 60 {
                let _ = sqlx::query("UPDATE t SET title = $1 WHERE id = 1")
                    .bind(format!("w{}_{}", w, n))
                    .execute(&pool)
                    .await;
                n += 1;
            }
        }));
    }

    let mut misses = 0;
    for _ in 0..60 {
        let n = sqlx::query("SELECT id FROM t WHERE id = 1")
            .fetch_all(&pool)
            .await
            .map(|r| r.len())
            .unwrap_or(99);
        if n == 0 {
            misses += 1;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        let _ = w.await;
    }

    assert_eq!(misses, 0, "a reader missed a row that exists, with the cache on");
}
