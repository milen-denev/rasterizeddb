//! V10 case 6's stand-in for slice B3a (docs/persistent-indexes.md §7,
//! "Describe and catalog reads take no lock"; Q3): the catalogue's figures
//! come through `schema_view`'s stats accessor, which takes no table lock,
//! so `pg_class` and `pg_stat_user_tables` answer at once while another
//! block holds the table in ACCESS EXCLUSIVE mode with a write pending. B4
//! activates the real case, with a DDL's AE.

use std::time::{Duration, Instant};

use super::owner_tests::{ok, session};
use crate::core::mvcc::owner::OwnerRelease;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;

/// Generous for a debug build on a loaded box; a catalogue read that
/// waited for the lock would take `LOCK_WAIT_TIMEOUT` (10 s) and fail.
const PROMPT: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_figures_answer_during_a_held_access_exclusive() {
    let db = crate::replication::test_support::in_memory_database().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE held (id INT, v TEXT)").await;
    ok(
        &db,
        &mut s,
        "INSERT INTO held (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    let block = db.transactions.begin();
    let release = OwnerRelease::for_root(&db.row_locks, block);
    for sql in [
        "LOCK TABLE held IN ACCESS EXCLUSIVE MODE",
        "INSERT INTO held (id, v) VALUES (4, 'd')",
    ] {
        let out =
            executor::execute_in_txn(recognize_query_purpose(sql).unwrap(), db.clone(), block)
                .await;
        assert!(
            !out.is_empty() && out[0] != 3,
            "{sql}: {}",
            String::from_utf8_lossy(&out)
        );
    }
    assert!(
        db.row_locks.owner_holds_tables(block),
        "the block holds no table lock"
    );

    for sql in [
        "SELECT relname, reltuples FROM pg_class WHERE relname = 'held'",
        "SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname = 'held'",
    ] {
        let started = Instant::now();
        let rows = ok(&db, &mut s, sql).await;
        let took = started.elapsed();
        assert_eq!(rows, 1, "{sql}");
        assert!(took < PROMPT, "{sql} took {took:?} behind the held lock");
    }
    let started = Instant::now();
    let totals = db
        .schema_view("held")
        .unwrap()
        .stats()
        .totals()
        .await
        .unwrap();
    assert!(started.elapsed() < PROMPT);
    assert!(totals.slots >= 3 && totals.bytes > 0, "{totals:?}");

    db.commit_transaction(block);
    drop(release);
}
