//! Table locks live on the wire (slice B3b, docs/persistent-indexes.md §7
//! "As built (B3b)"): DML and SELECT wait behind `LOCK TABLE … ACCESS
//! EXCLUSIVE` and proceed when it ends, a SELECT does not wait behind ROW
//! EXCLUSIVE, `lock_timeout` bounds a wait with 55P03 and `0` bounds nothing,
//! cancellation ends a wait with 57014 and leaves no queue entry behind, a
//! crossed wait between two blocks ends in one 40P01, an autocommit
//! statement's reply is sent only after its locks are released, and a
//! partitioned table is locked by its logical name.
//!
//! Driven through `execute_recorded` as `owner_tests` is; each waiter runs
//! on a task of its own so the test can watch it wait.

use std::time::{Duration, Instant};

use super::owner_tests::{Session, ok, send, session};
use super::*;
use tokio::io::AsyncReadExt;

/// Long enough for a statement that is not blocked to have finished on a
/// loaded debug build.
const SETTLE: Duration = Duration::from_millis(400);
/// The bound on anything that should finish.
const BOUND: Duration = Duration::from_secs(20);

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

type Running = tokio::task::JoinHandle<(Session, Result<usize, String>, Duration)>;

/// Runs `sql` on `s` in a task of its own: the session comes back with the
/// outcome and how long it took.
fn spawn(db: &Arc<Database>, s: Session, sql: &str) -> Running {
    let (db, sql) = (db.clone(), sql.to_string());
    tokio::spawn(async move {
        let mut s = s;
        let started = Instant::now();
        let out = send(&db, &mut s, &sql).await;
        (s, out, started.elapsed())
    })
}

async fn finish(what: &str, running: Running) -> (Session, Result<usize, String>, Duration) {
    tokio::time::timeout(BOUND, running)
        .await
        .unwrap_or_else(|_| panic!("{what}: did not finish within {BOUND:?}"))
        .expect("the task panicked")
}

async fn seeded(db: &Arc<Database>) -> Session {
    let mut s = session(db);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
        "CREATE TABLE u (id INT PRIMARY KEY, v TEXT)",
        "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO u (id, v) VALUES (1, 'a')",
    ] {
        ok(db, &mut s, sql).await;
    }
    s
}

/// A block holding `LOCK TABLE t IN <mode> MODE`.
async fn holder(db: &Arc<Database>, table: &str, mode: &str) -> Session {
    let mut h = session(db);
    ok(db, &mut h, "BEGIN").await;
    ok(db, &mut h, &format!("LOCK TABLE {table} IN {mode} MODE")).await;
    h
}

fn sqlstate_of(error: &str) -> &str {
    // The ErrorResponse body: `S…\0C<code>\0M…`.
    error
        .split('\0')
        .find_map(|field| field.strip_prefix('C'))
        .unwrap_or("")
}

/// DML waits behind a held `LOCK TABLE … ACCESS EXCLUSIVE` instead of
/// failing at once, and proceeds the moment the block ends: INSERT, UPDATE
/// and DELETE alike, each to its result.
///
/// Mutation: `Database::lock_table` grants ROW EXCLUSIVE without asking (the
/// DML runs beside the AE and finishes while it is held).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dml_waits_behind_lock_table_access_exclusive_and_proceeds() {
    let db = db().await;
    let mut setup = seeded(&db).await;
    for (sql, check, want) in [
        ("INSERT INTO t (id, v) VALUES (3, 'c')", "SELECT id FROM t WHERE id = 3", 1),
        ("UPDATE t SET v = 'z' WHERE id = 1", "SELECT id FROM t WHERE v = 'z'", 1),
        ("DELETE FROM t WHERE id = 2", "SELECT id FROM t WHERE id = 2", 0),
    ] {
        let mut h = holder(&db, "t", "ACCESS EXCLUSIVE").await;
        let waiter = spawn(&db, session(&db), sql);
        tokio::time::sleep(SETTLE).await;
        assert!(!waiter.is_finished(), "{sql}: ran beside a held ACCESS EXCLUSIVE");
        ok(&db, &mut h, "COMMIT").await;
        let (_, out, _) = finish(sql, waiter).await;
        assert_eq!(out.unwrap_or_else(|e| panic!("{sql}: {e}")), 0, "{sql}");
        assert_eq!(ok(&db, &mut setup, check).await, want, "{sql}: its write did not land");
    }
}

/// A SELECT waits behind ACCESS EXCLUSIVE too, and reads after it ends.
///
/// Mutation: the SELECT arm and the statement set take no lock for a read
/// (`lock_table` returns at once for ACCESS SHARE).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_waits_behind_access_exclusive() {
    let db = db().await;
    let _setup = seeded(&db).await;
    let mut h = holder(&db, "t", "ACCESS EXCLUSIVE").await;
    let waiter = spawn(&db, session(&db), "SELECT id, v FROM t WHERE id = 1");
    tokio::time::sleep(SETTLE).await;
    assert!(!waiter.is_finished(), "a SELECT read beside a held ACCESS EXCLUSIVE");
    ok(&db, &mut h, "COMMIT").await;
    let (_, out, _) = finish("SELECT", waiter).await;
    assert_eq!(out.unwrap(), 1);
}

/// A SELECT never waits behind ROW EXCLUSIVE: neither a block's write in
/// progress nor `LOCK TABLE … IN ROW EXCLUSIVE MODE`.
///
/// Mutation: reads take SHARE (which conflicts with ROW EXCLUSIVE) instead
/// of ACCESS SHARE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_does_not_wait_behind_row_exclusive() {
    let db = db().await;
    let _setup = seeded(&db).await;
    let mut writer = session(&db);
    ok(&db, &mut writer, "BEGIN").await;
    ok(&db, &mut writer, "INSERT INTO t (id, v) VALUES (9, 'w')").await;
    let mut lock = holder(&db, "t", "ROW EXCLUSIVE").await;
    let reader = spawn(&db, session(&db), "SELECT id FROM t WHERE id = 1");
    let (_, out, took) = finish("SELECT beside ROW EXCLUSIVE", reader).await;
    assert_eq!(out.unwrap(), 1);
    assert!(took < SETTLE * 5, "the SELECT took {took:?} beside ROW EXCLUSIVE");
    ok(&db, &mut writer, "COMMIT").await;
    ok(&db, &mut lock, "COMMIT").await;
}

/// `lock_timeout` set: a wait fails with 55P03 once it has lasted that long,
/// and not before. Its queue entry is gone with it: an ACCESS EXCLUSIVE
/// asked with NOWAIT after the holder ends is granted, where a stale queued
/// request would hold it back (rule (a)). `SHOW` reports the value set,
/// `RESET` puts back `0`.
///
/// Mutation: the wait ignores the session's `lock_timeout` (the statement
/// signal carries none), and the test's bound expires instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_fails_a_wait_with_55p03_after_its_bound() {
    let db = db().await;
    let _setup = seeded(&db).await;
    let mut h = holder(&db, "t", "ACCESS EXCLUSIVE").await;
    let mut w = session(&db);
    ok(&db, &mut w, "SET lock_timeout = 300").await;
    assert_eq!(show(&db, &mut w, "lock_timeout").await, "300ms");
    let waiter = spawn(&db, w, "SELECT id FROM t");
    let (mut w, out, took) = finish("SELECT under lock_timeout", waiter).await;
    let e = out.expect_err("the wait outlasted lock_timeout and succeeded");
    assert_eq!(sqlstate_of(&e), "55P03", "{e}");
    assert!(took >= Duration::from_millis(300), "failed after {took:?}, before the bound");
    assert!(took < Duration::from_secs(5), "failed after {took:?}");
    ok(&db, &mut h, "COMMIT").await;
    ok(&db, &mut w, "RESET lock_timeout").await;
    assert_eq!(show(&db, &mut w, "lock_timeout").await, "0");
    assert_nothing_queued(&db, "t").await;
}

/// `lock_timeout = 0` means no bound (PostgreSQL's default, decision 12):
/// a wait past `deadlock_timeout` and past several polls keeps waiting, and
/// completes when the holder ends.
///
/// Mutation: `0` read as a one-second bound (the session passes 1000 when
/// unset); the wait fails with 55P03 before the holder ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_zero_waits_unbounded() {
    let db = db().await;
    let _setup = seeded(&db).await;
    let mut h = holder(&db, "t", "ACCESS EXCLUSIVE").await;
    let mut w = session(&db);
    ok(&db, &mut w, "SET lock_timeout = 0").await;
    let waiter = spawn(&db, w, "INSERT INTO t (id, v) VALUES (5, 'e')");
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(!waiter.is_finished(), "the unbounded wait ended before the holder did");
    ok(&db, &mut h, "COMMIT").await;
    let (_, out, took) = finish("INSERT under lock_timeout = 0", waiter).await;
    assert_eq!(out.unwrap_or_else(|e| panic!("{e}")), 0);
    assert!(took >= Duration::from_millis(1600));
}

/// Cancellation ends a wait with 57014 (here `statement_timeout`, the same
/// signal a CancelRequest and a departed client trip), and the waiter's
/// queue entry is removed: an ACCESS EXCLUSIVE asked with NOWAIT once the
/// holder ends is granted.
///
/// Mutations: the wait ignores `cancel::tripped()` (it waits for the
/// holder); a cancelled wait returns without abandoning its request (the
/// NOWAIT is refused by the stale entry).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_wait_gives_57014_and_leaves_no_queue_entry() {
    let db = db().await;
    let _setup = seeded(&db).await;
    let mut h = holder(&db, "t", "ACCESS EXCLUSIVE").await;
    let mut w = session(&db);
    ok(&db, &mut w, "BEGIN").await;
    ok(&db, &mut w, "SET statement_timeout = 300").await;
    let waiter = spawn(&db, w, "SELECT id FROM t");
    let (mut w, out, took) = finish("SELECT under statement_timeout", waiter).await;
    let e = out.expect_err("the cancelled wait succeeded");
    assert_eq!(sqlstate_of(&e), "57014", "{e}");
    assert!(took < Duration::from_secs(5), "cancelled after {took:?}");
    ok(&db, &mut h, "COMMIT").await;
    // The block that waited is still open (an error does not end it before
    // B6); its owner holds nothing on `t` and awaits nothing.
    assert_nothing_queued(&db, "t").await;
    let _ = send(&db, &mut w, "ROLLBACK").await;
}

/// Two blocks crossing their waits: exactly one gets 40P01, about a second
/// in (`deadlock_timeout`), and once it rolls back the other proceeds. With
/// `LOCK TABLE` (strong requests) and with plain reads under held locks.
///
/// Mutation: the wait's deadlock check never finds a cycle (both wait until
/// the test's bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadlock_between_two_blocks_fails_exactly_one() {
    let db = db().await;
    let _setup = seeded(&db).await;
    for (second_a, second_b) in [
        ("LOCK TABLE u IN ACCESS EXCLUSIVE MODE", "LOCK TABLE t IN ACCESS EXCLUSIVE MODE"),
        ("SELECT id FROM u", "SELECT id FROM t"),
    ] {
        let a = holder(&db, "t", "ACCESS EXCLUSIVE").await;
        let b = holder(&db, "u", "ACCESS EXCLUSIVE").await;
        let ra = spawn(&db, a, second_a);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rb = spawn(&db, b, second_b);
        // Whichever fails is rolled back by its client, which frees the other.
        let started = Instant::now();
        let mut pending = vec![("a", ra), ("b", rb)];
        let mut outcomes = Vec::new();
        while !pending.is_empty() {
            let i = loop {
                if let Some(i) = pending.iter().position(|(_, h)| h.is_finished()) {
                    break i;
                }
                assert!(started.elapsed() < BOUND, "{second_a} / {second_b}: neither finished");
                tokio::time::sleep(Duration::from_millis(20)).await;
            };
            let (who, h) = pending.remove(i);
            let (mut s, out, took) = h.await.unwrap();
            let failed = out.is_err();
            if let Err(e) = &out {
                assert_eq!(sqlstate_of(e), "40P01", "{who}: {e}");
            }
            let _ = send(&db, &mut s, if failed { "ROLLBACK" } else { "COMMIT" }).await;
            outcomes.push((who, failed, took));
        }
        let victims = outcomes.iter().filter(|(_, failed, _)| *failed).count();
        assert_eq!(victims, 1, "{second_a} / {second_b}: {outcomes:?}");
        let first = outcomes[0].2;
        assert!(first < Duration::from_secs(6), "the cycle took {first:?} to break");
    }
}

/// The release-before-reply ordering: an autocommit statement's reply
/// (CommandComplete, or an ErrorResponse, which is flushed at once) reaches
/// the client only after its owner has released its table locks, so the
/// client's next step, here `LOCK TABLE … NOWAIT` on another connection
/// sent the moment the reply is read, never meets the statement's own lock.
/// The statement holds its release back 400 ms (a test hook), which with
/// the reply sent first leaves the NOWAIT a wide window to be refused.
///
/// Mutation: `hold_reply` never holds (the reply goes out inside O's scope).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_is_sent_only_after_its_statements_locks_are_released() {
    let db = db().await;
    let _setup = seeded(&db).await;
    for sql in [
        "SELECT id FROM t WHERE id = 1 /* rdb_release_delay_ms=400 */",
        "SELECT id / 0 FROM t WHERE id = 1 /* rdb_release_delay_ms=400 */",
        "INSERT INTO t (id, v) VALUES (40, 'x') /* rdb_release_delay_ms=400 */",
    ] {
        let s = session(&db);
        let Session { mut state, mut server, mut client } = s;
        let (db2, stmt) = (db.clone(), sql.to_string());
        let running = tokio::spawn(async move {
            Box::pin(execute_recorded(&db2, &mut state, &mut server, &stmt, &[], false, None))
                .await
                .unwrap();
        });
        // Read until the reply's end arrives.
        loop {
            let kind = client.read_u8().await.unwrap();
            let len = client.read_i32().await.unwrap() as usize;
            let mut body = vec![0u8; len - 4];
            client.read_exact(&mut body).await.unwrap();
            if kind == b'C' || kind == b'E' {
                break;
            }
        }
        // The client's next step, at once.
        let mut next = session(&db);
        ok(&db, &mut next, "BEGIN").await;
        let got = send(&db, &mut next, "LOCK TABLE t IN ACCESS EXCLUSIVE MODE NOWAIT").await;
        assert!(
            got.is_ok(),
            "{sql}: the client's next statement met its previous statement's lock: {got:?}"
        );
        ok(&db, &mut next, "COMMIT").await;
        tokio::time::timeout(BOUND, running).await.unwrap().unwrap();
    }
}

/// A partitioned table is locked by its logical name: `LOCK TABLE` on it
/// blocks DML on any of its partitions (a routed INSERT, a full-key UPDATE)
/// and a read routed to one partition, which all proceed after it ends.
///
/// Mutation: `Database::lock_table` takes nothing for a partitioned name (the
/// routed INSERT runs on its partition beside the table's AE).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_table_on_a_partitioned_table_blocks_its_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let db = Arc::new(Database::new_named(&dir.path().to_string_lossy(), false, "tl_part").await);
    let mut setup = session(&db);
    ok(
        &db,
        &mut setup,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 4",
    )
    .await;
    ok(&db, &mut setup, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30)").await;
    for sql in [
        "INSERT INTO pt (tenant_id, id, v) VALUES (7, 1, 70)",
        "UPDATE pt SET v = 11 WHERE tenant_id = 1 AND id = 1",
        "SELECT v FROM pt WHERE tenant_id = 2 AND id = 1",
    ] {
        let mut h = holder(&db, "pt", "ACCESS EXCLUSIVE").await;
        let waiter = spawn(&db, session(&db), sql);
        tokio::time::sleep(SETTLE).await;
        assert!(!waiter.is_finished(), "{sql}: ran on a partition beside the table's ACCESS EXCLUSIVE");
        ok(&db, &mut h, "COMMIT").await;
        let (_, out, _) = finish(sql, waiter).await;
        out.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    drop(setup);
    drop(db);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
}

/// What `SHOW name` answers.
async fn show(db: &Arc<Database>, s: &mut Session, name: &str) -> String {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, &format!("SHOW {name}"), &[], false, None))
        .await
        .unwrap();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        if kind == b'D' {
            // DataRow: i16 count, i32 length, bytes.
            let n = i32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
            let value = String::from_utf8_lossy(&body[6..6 + n]).to_string();
            // Drain to CommandComplete.
            loop {
                let kind = s.client.read_u8().await.unwrap();
                let len = s.client.read_i32().await.unwrap() as usize;
                let mut rest = vec![0u8; len - 4];
                s.client.read_exact(&mut rest).await.unwrap();
                if kind == b'C' {
                    return value;
                }
            }
        }
        if kind == b'E' {
            panic!("SHOW {name}: {}", String::from_utf8_lossy(&body));
        }
    }
}

/// Nothing holds or awaits `table` any more: an ACCESS EXCLUSIVE asked with
/// NOWAIT by a fresh block is granted (a stale queued request, weak or
/// strong, would refuse it by rule (a)).
async fn assert_nothing_queued(db: &Arc<Database>, table: &str) {
    let mut probe = session(db);
    ok(db, &mut probe, "BEGIN").await;
    let got = send(db, &mut probe, &format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")).await;
    assert!(got.is_ok(), "a request was left queued on {table}: {got:?}");
    ok(db, &mut probe, "COMMIT").await;
}

/// `lock_timeout` from the StartupMessage, both spellings, as
/// `statement_timeout` is read.
#[test]
fn lock_timeout_is_read_from_the_startup_message() {
    let mut p = HashMap::new();
    p.insert("options".to_string(), "-c lock_timeout=5s -c statement_timeout=1000".to_string());
    assert_eq!(startup_lock_timeout(&p), Some(5000));
    assert_eq!(startup_statement_timeout(&p), Some(1000));
    let mut p = HashMap::new();
    p.insert("lock_timeout".to_string(), "250".to_string());
    assert_eq!(startup_lock_timeout(&p), Some(250));
    assert_eq!(startup_lock_timeout(&HashMap::new()), None);
}
