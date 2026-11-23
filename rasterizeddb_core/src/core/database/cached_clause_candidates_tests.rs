//! A read the query cache already answers skips building its candidates
//! (`Table::candidates_unless_cached`), and answers exactly as it did.
//!
//! The clause is the shape that made the build expensive: a primary-key
//! prefix (`tenant_id = … AND month = …`) and a column outside the key, so
//! the only index answer is every row under the prefix.
//!
//! The query cache's switch is set once per process and the rest of the lib
//! turns it off, so the work runs in a child process that turns it on first,
//! as `partition_seam_tests::partitions_have_distinct_cache_scopes` does.

use std::sync::atomic::Ordering;

use rclite::Arc;

use super::*;
use crate::core::row::table::CANDIDATES_SKIPPED_FOR_CACHE;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;

const CHILD_ENV: &str = "RDB_CACHED_CLAUSE_CANDIDATES_CHILD";

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
    let reply = executor::execute(purpose, db.clone()).await;
    assert_ne!(reply.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&reply));
    reply
}

fn skipped() -> u64 {
    CANDIDATES_SKIPPED_FOR_CACHE.load(Ordering::Relaxed)
}

#[test]
#[cfg(unix)]
fn a_cached_clause_skips_its_candidates_and_answers_the_same() {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args([
            "--exact",
            "core::database::cached_clause_candidates_tests::cached_clause_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn the cache child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "cache child failed or did not run (status {:?})\n--- stdout\n{}\n--- stderr\n{}",
        out.status,
        stdout,
        stderr.chars().rev().take(4000).collect::<String>().chars().rev().collect::<String>()
    );
}

/// The child half of the test above. A no-op unless the parent set the
/// environment variable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_clause_child() {
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }
    crate::cache::query_cache::init(1024);
    let _ = crate::cache::query_cache::ENABLE_CACHE.set(true);
    assert!(crate::cache::query_cache::enabled(), "the child could not turn the cache on");

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(relation_dispatch_tests::open(dir.path()).await);
    run(
        &db,
        "CREATE TABLE ev (tenant_id BIGINT NOT NULL, month INT NOT NULL, event_id BIGINT NOT NULL, \
         session_id TEXT, PRIMARY KEY (tenant_id, month, event_id))",
    )
    .await;
    for (tenant, month) in [(6, 1), (7, 1), (7, 2)] {
        let values: Vec<String> = (0..1500)
            .map(|e| format!("({tenant}, {month}, {e}, 's-{}')", e % 700))
            .collect();
        run(&db, &format!("INSERT INTO ev VALUES {}", values.join(", "))).await;
    }

    let select = "SELECT event_id FROM ev WHERE session_id = 's-42' AND tenant_id = 7 AND month = 1 LIMIT 100";
    let count = "SELECT COUNT(*) FROM ev WHERE session_id = 's-42' AND tenant_id = 7 AND month = 1";
    let ordered = "SELECT event_id FROM ev WHERE session_id = 's-42' AND tenant_id = 7 AND month = 1 \
                   ORDER BY event_id DESC LIMIT 2";

    // The first read is a miss and builds its candidates; the second is
    // served from the entry the first stored, without them.
    let (hits0, _, _) = crate::cache::query_cache::stats();
    let first = run(&db, select).await;
    let before = skipped();
    let second = run(&db, select).await;
    assert_eq!(second, first, "a hit without candidates answered differently");
    assert!(skipped() > before, "the hit still built its candidates");
    let (hits1, _, _) = crate::cache::query_cache::stats();
    assert!(hits1 > hits0, "the cache never took part; the test proves nothing");

    // A write retires the entry: the next read is a miss, builds its
    // candidates and sees the new row; the one after is a hit again.
    run(&db, "INSERT INTO ev VALUES (7, 1, 5000, 's-42')").await;
    let before = skipped();
    let third = run(&db, select).await;
    assert_eq!(skipped(), before, "a read the write retired skipped its candidates");
    assert_ne!(third, first, "the read after the write missed the new row");
    let fourth = run(&db, select).await;
    assert_eq!(fourth, third, "the hit after the write answered differently");
    assert!(skipped() > before, "the hit after the write still built its candidates");

    // A count is served the same way.
    let counted = run(&db, count).await;
    let before = skipped();
    assert_eq!(run(&db, count).await, counted, "a counted hit answered differently");
    assert!(skipped() > before, "the counted hit still built its candidates");

    // An ordered read keeps them: the processor may decline its hit and
    // rescan, and the rescan needs them.
    let sorted = run(&db, ordered).await;
    let before = skipped();
    assert_eq!(run(&db, ordered).await, sorted, "an ordered read answered differently");
    assert_eq!(skipped(), before, "an ordered read skipped its candidates");

    let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("a statement still holds the database"));
    relation_dispatch_tests::close(db, dir.path());
}
