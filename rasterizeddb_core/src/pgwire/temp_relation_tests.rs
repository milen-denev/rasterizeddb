//! What a statement-scoped relation costs in durable syncs.
//!
//! Every CTE and derived table is materialised into a real `__cte_…` relation
//! that is created, filled and dropped inside its statement. Those relations
//! are memory-backed and pool-exempt (`Database::temp_seed`, `pool_place`,
//! `pool_forget`); without that, each one creates and fsyncs five real files,
//! deletes them again and rewrites `db_storage.cfg` twice. On ext4 that is
//! ~130 ms per materialisation against ~5 ms. The suites run on tmpfs, where
//! the difference is under a millisecond and every result is still right, so
//! only a sync count can catch the guard coming undone.
//!
//! Driven through pgwire because that is where a `WITH` is materialised; the
//! executor alone does not parse one.

use super::*;
use crate::core::sync_audit::{self, SyncEvent};
use tokio::io::AsyncReadExt;

async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    db: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    sql: &str,
) {
    Box::pin(execute_single_statement(db, state, stream, sql, &[], false))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Reads one statement's reply and returns its data rows, panicking on an
/// `ErrorResponse`: a materialisation that failed would sync nothing and pass
/// every count below.
async fn data_rows<C: AsyncRead + Unpin>(client: &mut C, sql: &str) -> usize {
    let mut rows = 0;
    loop {
        let kind = client.read_u8().await.unwrap();
        let len = client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => rows += 1,
            b'C' => return rows,
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

/// The two ways the guard comes undone, each with a file of its own: a temp
/// relation given real files, or one recorded in the pool's placement map.
/// Deterministic, since nothing in the background ever touches either, so it
/// holds on every attempt.
fn assert_stays_in_memory(what: &str, events: &[SyncEvent]) {
    let leaked: Vec<_> = events
        .iter()
        .filter(|e| {
            let p = e.path.to_string_lossy();
            p.contains(crate::core::rql::cte::TEMP_TABLE_PREFIX) || p.ends_with("db_storage.cfg")
        })
        .collect();
    assert!(
        leaked.is_empty(),
        "{what}: a statement-scoped relation reached the disk: {leaked:#?}"
    );
}

/// Within the floor as measured: at most `max_log` syncs of the database's
/// own log (the read-only autocommit's; the catalogue tombstone the drop
/// used to write is gone with the row, `a_materialised_relation_is_never_catalogued`)
/// and nothing else.
fn within_floor(events: &[SyncEvent], max_log: usize) -> bool {
    let log = events.iter().filter(|e| e.tag == "wal.log").count();
    log <= max_log && events.iter().all(|e| e.tag == "wal.log")
}

/// Reruns allowed when the process-wide log writer's timer lands a sync of
/// the root log inside the window. It never syncs anything else, so a
/// regression exceeds the floor on every attempt.
const MAX_ATTEMPTS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
async fn a_materialised_relation_never_syncs_a_file_of_its_own() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(10_000);
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let root_s = root.to_string_lossy().into_owned();

    let db = Arc::new(Database::new_named(&root_s, false, "pgtempsync").await);
    let catalog = DatabaseCatalog::single(db.clone());
    let auth = AuthConfig {
        method: crate::auth::AuthMethod::Trust,
        ..AuthConfig::default()
    };
    let principal = Principal {
        name: "rasterized".to_string(),
        superuser: false,
        authenticated: false,
    };
    let mut state = SessionState::new(
        catalog,
        db.name.clone(),
        auth,
        principal,
        ConnectionLimits::default(),
    );
    // Large enough for every reply, so nothing has to drain it meanwhile.
    let (mut server, mut client) = tokio::io::duplex(1 << 20);

    for sql in [
        "CREATE TABLE t (id INT, label TEXT)",
        "INSERT INTO t (id, label) VALUES (1, 'a')",
        "INSERT INTO t (id, label) VALUES (2, 'b')",
    ] {
        run(&db, &mut state, &mut server, sql).await;
        data_rows(&mut client, sql).await;
    }

    // (statement, its data rows, the most root-log syncs it may cost)
    let statements = [
        ("WITH s AS (SELECT id FROM t) SELECT id FROM s", 2, 1),
        // What sea-orm's `paginate()` emits for every paginated list view.
        ("SELECT COUNT(*) FROM (SELECT id FROM t) AS sub", 1, 1),
    ];

    // Warm-up. The first drop of any table creates the catalogue's schema
    // sidecars once; that is the catalogue's cost, not the relation's, and
    // counting it would make the bounds depend on test order.
    for (sql, want, _) in statements {
        run(&db, &mut state, &mut server, sql).await;
        assert_eq!(data_rows(&mut client, sql).await, want, "{sql}");
    }

    let watch = sync_audit::watch(&root);
    for (sql, want, max_log) in statements {
        let mut seen = Vec::new();
        let mut ok = false;
        for _ in 0..MAX_ATTEMPTS {
            let _ = watch.take_events();
            run(&db, &mut state, &mut server, sql).await;
            let events = watch.take_events();
            assert_eq!(data_rows(&mut client, sql).await, want, "{sql}");
            assert_stays_in_memory(sql, &events);
            if within_floor(&events, max_log) {
                ok = true;
                break;
            }
            seen.push(events);
        }
        assert!(
            ok,
            "{sql}: every attempt exceeded {max_log} root-log syncs or synced \
             something else, a new per-statement durable side effect: {seen:#?}"
        );
    }
    drop(watch);

    drop(state);
    drop(db);
    crate::core::wal::remove_for(&root_s);
}

/// A statement's own relation is not catalogued: no row written when it is
/// created, none searched for when it is dropped. Both used to happen. The
/// row was a logged, synced write per statement and its tombstone stayed for
/// good, so the catalogue grew one dead row per CTE or derived-table query
/// ever run and every later search read past all of them (8.3 to 8.8 ms over
/// 1,500 queries on ext4). The search loaded the catalogue's schema, whose
/// ten sidecars the first drop in a database created with a directory sync
/// each: ~90 ms on that database's first such query, which the test above
/// warms past.
#[tokio::test(flavor = "multi_thread")]
async fn a_materialised_relation_is_never_catalogued() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(10_000);
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let root_s = root.to_string_lossy().into_owned();

    let db = Arc::new(Database::new_named(&root_s, false, "pgtempcat").await);
    let catalog = DatabaseCatalog::single(db.clone());
    let auth = AuthConfig {
        method: crate::auth::AuthMethod::Trust,
        ..AuthConfig::default()
    };
    let principal = Principal {
        name: "rasterized".to_string(),
        superuser: false,
        authenticated: false,
    };
    let mut state = SessionState::new(catalog, db.name.clone(), auth, principal, ConnectionLimits::default());
    let (mut server, mut client) = tokio::io::duplex(1 << 20);

    for sql in ["CREATE TABLE t (id INT, label TEXT)", "INSERT INTO t (id, label) VALUES (1, 'a')"] {
        run(&db, &mut state, &mut server, sql).await;
        data_rows(&mut client, sql).await;
    }
    let catalogue = root.join("db_data_pointers.db");
    let catalogue_len = || std::fs::metadata(&catalogue).map(|m| m.len()).unwrap_or(0);
    let before = catalogue_len();

    // The first in this database, unwarmed: nothing but the log syncs, and
    // no catalogue sidecar is created.
    let sql = "SELECT COUNT(*) FROM (SELECT id FROM t) AS sub";
    let watch = sync_audit::watch(&root);
    let mut seen = Vec::new();
    let mut ok = false;
    for attempt in 0..MAX_ATTEMPTS {
        let _ = watch.take_events();
        run(&db, &mut state, &mut server, sql).await;
        let events = watch.take_events();
        assert_eq!(data_rows(&mut client, sql).await, 1, "{sql}");
        assert_stays_in_memory(sql, &events);
        let catalogue_files: Vec<_> =
            events.iter().filter(|e| e.path.to_string_lossy().contains("db_data_schema_")).collect();
        assert!(
            catalogue_files.is_empty(),
            "attempt {attempt}: a statement's relation touched the catalogue's sidecars: {catalogue_files:#?}"
        );
        if within_floor(&events, 1) {
            ok = true;
            break;
        }
        seen.push(events);
    }
    drop(watch);
    assert!(ok, "{sql}: synced more than the read's own log record: {seen:#?}");

    for _ in 0..20 {
        for sql in ["WITH s AS (SELECT id FROM t) SELECT id FROM s", "SELECT COUNT(*) FROM (SELECT id FROM t) AS sub"] {
            run(&db, &mut state, &mut server, sql).await;
            data_rows(&mut client, sql).await;
        }
    }
    assert_eq!(catalogue_len(), before, "a statement's relation left rows in the catalogue");
    assert!(
        db.table_names().iter().all(|n| !n.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)),
        "a statement's relation outlived it"
    );

    drop(state);
    drop(db);
    crate::core::wal::remove_for(&root_s);
}

const CACHE_CHILD_ENV: &str = "RDB_TEMP_RELATION_CACHE_CHILD";

/// With the query cache on, a derived-table query is served from the entry
/// its inner scan left, and the relation it is materialised into never asks
/// the cache. That relation is created under a fresh name per statement, so
/// its lookup could only miss and its entry only evict a useful one: each
/// such query counted a hit (the inner scan) and a miss (the relation's) in
/// step, which is how this was seen. Run in a child process: the cache
/// switch is process-wide and every other library test runs with it off.
#[test]
#[cfg(unix)]
fn a_materialised_relation_does_not_use_the_query_cache() {
    if std::env::var(CACHE_CHILD_ENV).is_ok() {
        return;
    }
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args(["--exact", "pgwire::temp_relation_tests::temp_relation_cache_child", "--nocapture", "--test-threads=1"])
        .env(CACHE_CHILD_ENV, "1")
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

/// The child half of [`a_materialised_relation_does_not_use_the_query_cache`].
/// A no-op unless the parent set the environment variable.
#[tokio::test(flavor = "multi_thread")]
async fn temp_relation_cache_child() {
    if std::env::var(CACHE_CHILD_ENV).is_err() {
        return;
    }
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(10_000);
    crate::cache::query_cache::init(1024);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(true);
    assert!(crate::cache::query_cache::enabled(), "the child could not turn the cache on");

    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let root_s = root.to_string_lossy().into_owned();
    let db = Arc::new(Database::new_named(&root_s, false, "pgtempcache").await);
    let catalog = DatabaseCatalog::single(db.clone());
    let auth = AuthConfig {
        method: crate::auth::AuthMethod::Trust,
        ..AuthConfig::default()
    };
    let principal = Principal {
        name: "rasterized".to_string(),
        superuser: false,
        authenticated: false,
    };
    let mut state = SessionState::new(catalog, db.name.clone(), auth, principal, ConnectionLimits::default());
    let (mut server, mut client) = tokio::io::duplex(1 << 20);
    for sql in [
        "CREATE TABLE ev (id INT PRIMARY KEY, tenant INT, u INT, revenue INT)",
        "INSERT INTO ev VALUES (1, 7, 1, 10), (2, 7, 2, 20), (3, 8, 1, 30), (4, 7, 1, 5)",
    ] {
        run(&db, &mut state, &mut server, sql).await;
        data_rows(&mut client, sql).await;
    }

    let sql = "SELECT u, revenue FROM (SELECT u, SUM(revenue) AS revenue FROM ev WHERE tenant = 7 GROUP BY u) s \
               ORDER BY revenue DESC LIMIT 10";
    // The first fills the inner scan's entry.
    run(&db, &mut state, &mut server, sql).await;
    assert_eq!(data_rows(&mut client, sql).await, 2, "{sql}");

    let (hits0, misses0, _) = crate::cache::query_cache::stats();
    const RUNS: u64 = 5;
    for _ in 0..RUNS {
        run(&db, &mut state, &mut server, sql).await;
        assert_eq!(data_rows(&mut client, sql).await, 2, "{sql}");
    }
    let (hits1, misses1, _) = crate::cache::query_cache::stats();
    assert!(hits1 - hits0 >= RUNS, "the inner scan was not served from the cache ({} hits)", hits1 - hits0);
    assert_eq!(
        misses1 - misses0,
        0,
        "a statement's relation consulted the query cache: a miss per query, an entry nothing can read"
    );

    drop(state);
    drop(db);
    crate::core::wal::remove_for(&root_s);
}
