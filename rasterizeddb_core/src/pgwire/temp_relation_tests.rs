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
/// own log (the read-only autocommit's, plus the durable catalogue tombstone
/// the drop writes) and nothing else.
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
        ("WITH s AS (SELECT id FROM t) SELECT id FROM s", 2, 2),
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
