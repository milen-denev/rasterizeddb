//! Lazy open and exclusive close (docs/partitioning.md §7.2, phase 3 P5b),
//! through `Database` and SQL.
//!
//! What these pin, each against the mechanism that keeps it:
//!
//! - a start opens no partition, and an access opens only what it reaches;
//! - a closed partition answers every read and write as an open one does,
//!   in one process and after a restart, and reports the same sizes;
//! - I13 across a close: a closed partition keeps its pin, so an aborted
//!   row stays hidden however far every other table forgets, until the
//!   partition itself is swept, which a forget round does by opening it;
//! - a close never takes an instance a statement holds, and an open waits
//!   for a close in flight (I14);
//! - a partition that does not reopen fails the statement, never answering
//!   as if it were empty;
//! - a schema change validates the rows of closed partitions too, and its
//!   metadata-only test counts them;
//! - `SET CONFIG vacuum.enabled` reaches partitions opened later;
//! - closes racing writers, readers, forget sweeps, fan-outs and DDL lose
//!   and mix nothing.

use std::path::Path;
use std::time::Duration;

use rclite::Arc;

use super::table::{Close, PartitionedTable};
use crate::core::database::{Database, Relation};
use crate::core::mvcc::transaction::TxId;

const CREATE: &str = "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, \
                      PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 8";
const TENANTS: i64 = 16;
const IDS: i64 = 4;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&path_str(dir), false, "lazy").await.unwrap())
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn restart(dir: &Path, db: Arc<Database>) -> Arc<Database> {
    close(dir, db);
    open(dir).await
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    // `CREATE UNIQUE INDEX` reaches the executor as a constraint, as pgwire
    // hands it over.
    let rewritten = crate::core::rql::lexer_alter::unique_index_as_constraint(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(String::from_utf8_lossy(&out).into_owned());
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn exec_in(db: &Arc<Database>, tx: TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
}

fn decode_i64s(out: &[u8], columns: usize) -> Result<Vec<Vec<i64>>, String> {
    if out.first() != Some(&2) {
        return Err(format!("not a row set: {}", String::from_utf8_lossy(out)));
    }
    let rows = crate::core::row::row::vec_into_rows(&out[1..]).map_err(|e| format!("{e:?}"))?;
    Ok(rows
        .iter()
        .map(|r| {
            (0..columns)
                .map(|c| i64::from_le_bytes(r.columns[c].data.into_slice().try_into().unwrap_or([0xFF; 8])))
                .collect()
        })
        .collect())
}

/// `(tenant_id, id, v)` of every row `SELECT … WHERE <clause>` returns,
/// sorted; an error as `Err`.
async fn try_rows(db: &Arc<Database>, clause: &str) -> Result<Vec<(i64, i64, i64)>, String> {
    let sql = if clause.is_empty() {
        "SELECT tenant_id, id, v FROM pt".to_string()
    } else {
        format!("SELECT tenant_id, id, v FROM pt WHERE {clause}")
    };
    let out = try_exec(db, &sql).await?;
    let mut v: Vec<(i64, i64, i64)> = decode_i64s(&out, 3)?.into_iter().map(|r| (r[0], r[1], r[2])).collect();
    v.sort();
    Ok(v)
}

async fn rows(db: &Arc<Database>, clause: &str) -> Vec<(i64, i64, i64)> {
    try_rows(db, clause).await.unwrap_or_else(|e| panic!("WHERE {clause}: {e}"))
}

async fn set_config(db: &Arc<Database>, sql: &str) {
    let statement = crate::core::introspect::parse(sql).unwrap().unwrap();
    let out = crate::core::introspect::execute(&statement, db).await;
    assert_eq!(out.first(), Some(&2), "{sql}: {}", String::from_utf8_lossy(&out));
}

fn pt(db: &Database) -> Arc<PartitionedTable> {
    match db.relation("pt") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("pt is not partitioned"),
    }
}

fn pid_of_tenant(t: &PartitionedTable, tenant: i64) -> u32 {
    let epoch = t.epoch().unwrap();
    let pids = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map);
    assert_eq!(pids.len(), 1, "tenant {tenant} routes to {pids:?}");
    pids[0]
}

/// Every `(tenant, id, v)` the populated table starts with.
fn expected() -> Vec<(i64, i64, i64)> {
    let mut v: Vec<(i64, i64, i64)> =
        (1..=TENANTS).flat_map(|t| (1..=IDS).map(move |i| (t, i, t * 100 + i))).collect();
    v.sort();
    v
}

async fn populated(db: &Arc<Database>) {
    exec(db, CREATE).await;
    let values: Vec<String> = expected().iter().map(|(t, i, v)| format!("({t}, {i}, {v})")).collect();
    exec(db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
}

fn advance(db: &Database) {
    for _ in 0..9_000 {
        let tx = db.transactions.begin();
        db.transactions.commit(tx);
    }
    db.transactions.settle_now();
}

async fn sweep_plain(db: &Database) {
    for name in db.table_names() {
        if let Some(t) = db.table_for_tests(&name)
            && !t.is_shell()
        {
            t.neutralize_aborted_now().await.unwrap();
        }
    }
}

/// Sweeps every plain table and every materialized partition but `skip`
/// (each closed one opened for its sweep and closed again), then forgets as
/// far as the registry allows.
async fn forget_except(db: &Database, skip: &[u32]) -> TxId {
    advance(db);
    sweep_plain(db).await;
    let t = pt(db);
    let board = t.pin_board().unwrap();
    for pid in t.materialized_pids().unwrap() {
        if !skip.contains(&pid) {
            board.sweep(pid).await.unwrap();
        }
    }
    db.transactions.try_forget()
}

/// A start opens no partition; a routed read opens the one it reaches, a
/// write the one it writes, an unrouted read the rest; each answers as
/// before the restart.
///
/// Mutation: `open_live` opening every materialized partition (the phase 2
/// behaviour): partitions are open before any access.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_opens_nothing_and_an_access_opens_only_what_it_reaches() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    let materialized = t.materialized_pids().unwrap();
    assert!(materialized.len() > 4, "only {} partitions hold a tenant", materialized.len());
    assert_eq!(t.open_count(), 0, "a start opened partitions");

    let p3 = pid_of_tenant(&t, 3);
    assert_eq!(rows(&db, "tenant_id = 3").await, expected().into_iter().filter(|r| r.0 == 3).collect::<Vec<_>>());
    let open: Vec<u32> = t.open_partitions_now().into_iter().map(|(pid, _)| pid).collect();
    assert_eq!(open, vec![p3], "a routed read opened more than its partition");

    let other = (1..=TENANTS).find(|x| pid_of_tenant(&t, *x) != p3).unwrap();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({other}, 99, 1)")).await;
    assert_eq!(t.open_count(), 2, "a routed write opened more than its partition");

    let mut want = expected();
    want.push((other, 99, 1));
    want.sort();
    assert_eq!(rows(&db, "").await, want);
    assert_eq!(t.open_count(), materialized.len(), "a fan-out left a partition unread");
    close(dir.path(), db);
}

/// Every partition closed between statements: each read and write answers
/// as with every partition open, the sizes reported are the same, and so
/// is everything after a restart.
///
/// Mutation: `closed_lens` leaving out the staged `.tmp` (or the totals
/// skipping closed partitions): the sizes differ once closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_partition_answers_like_an_open_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let open_totals = t.totals().await.unwrap();
    let open_parts = t.partitions().await.unwrap();
    let materialized = t.materialized_pids().unwrap().len();
    assert_eq!(t.close_idle().await.unwrap(), materialized);
    assert_eq!(t.open_count(), 0);
    assert_eq!(t.totals().await.unwrap(), open_totals, "a closed table reports other sizes");
    assert_eq!(t.partitions().await.unwrap(), open_parts, "a closed table reports other partitions");

    let mut want = expected();
    assert_eq!(rows(&db, "").await, want);
    t.close_idle().await.unwrap();
    exec(&db, "UPDATE pt SET v = v + 1000 WHERE tenant_id = 2").await;
    for r in want.iter_mut().filter(|r| r.0 == 2) {
        r.2 += 1000;
    }
    t.close_idle().await.unwrap();
    exec(&db, "DELETE FROM pt WHERE tenant_id = 4 AND id = 1").await;
    want.retain(|r| *r != (4, 1, 401));
    t.close_idle().await.unwrap();
    exec(&db, "UPDATE pt SET v = 7 WHERE id = 3").await;
    for r in want.iter_mut().filter(|r| r.1 == 3) {
        r.2 = 7;
    }
    t.close_idle().await.unwrap();
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (5, 50, 5), (6, 60, 6)").await;
    want.extend([(5, 50, 5), (6, 60, 6)]);
    want.sort();
    t.close_idle().await.unwrap();
    assert_eq!(rows(&db, "").await, want);
    t.close_idle().await.unwrap();
    for tenant in [2, 4, 5, 6] {
        let mine: Vec<_> = want.iter().copied().filter(|r| r.0 == tenant).collect();
        assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, mine);
        t.close_idle().await.unwrap();
    }
    let (opens, closes) = t.open_close_counts();
    assert!(opens >= materialized as u64 && closes >= 2 * materialized as u64, "opens {opens}, closes {closes}");
    drop(t);
    let db = restart(dir.path(), db).await;
    assert_eq!(rows(&db, "").await, want, "after a restart");
    close(dir.path(), db);
}

/// I13 across a close (§7.2): an aborted write in `p`, `p` closed, every
/// other table swept and forgotten past: the horizon stops below the
/// aborted id and the row stays hidden when `p` reopens, in one process and
/// after a restart; `p`'s own sweep (which opens it) lets the horizon go.
///
/// Mutation: a close clearing its partition's pin (`Parts::close` applying
/// a clean `SWEPT`): forgetting passes the aborted id and the row returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aborted_row_stays_hidden_across_a_close_a_forget_and_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 999, -1)").await;
    db.abort_transaction(x);
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let tenant_one: Vec<_> = expected().into_iter().filter(|r| r.0 == 1).collect();
    assert_eq!(rows(&db, "tenant_id = 1").await, tenant_one);
    assert_eq!(t.close_idle().await.unwrap(), t.materialized_pids().unwrap().len());
    assert!(t.pin_of(p).await.unwrap().is_some(), "a closed partition lost its pin");

    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= x, "forgetting passed {x} (to {forgotten}) while closed partition {p} was unswept");
    assert_eq!(t.open_count(), 0, "a sweep left a partition open that it opened");
    assert_eq!(rows(&db, "tenant_id = 1").await, tenant_one, "the aborted row came back after a close");
    t.close_idle().await.unwrap();
    drop(t);

    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= x, "after a restart, forgetting passed {x} (to {forgotten})");
    assert_eq!(rows(&db, "tenant_id = 1").await, tenant_one, "after a restart");
    t.close_idle().await.unwrap();

    // `p` swept while closed: opened for it, closed again, and then the
    // horizon passes.
    t.pin_board().unwrap().sweep(p).await.unwrap();
    assert_eq!(t.open_count(), 0, "the sweep left its partition open");
    let forgotten = db.transactions.try_forget();
    assert!(forgotten > x, "with {p} swept forgetting still stops at {forgotten}");
    assert_eq!(rows(&db, "tenant_id = 1").await, tenant_one);
    drop(t);
    close(dir.path(), db);
}

/// A forget round sweeps a closed, pinned partition by opening it, and
/// closes it again afterwards: forgetting is not held back by partitions
/// nothing reads (every one closed after a restart, say).
///
/// Mutations: `sweep_all` skipping closed partitions (the pins stay, and
/// forgetting with them); not closing what it opened (partitions stay
/// open).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forget_round_sweeps_closed_partitions_and_closes_them_again() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 999, -1)").await;
    db.abort_transaction(x);
    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    assert_eq!(t.open_count(), 0);
    advance(&db);
    sweep_plain(&db).await;
    let board = t.pin_board().unwrap();
    board.set_step(1);
    for _ in 0..8 {
        db.partition_maintenance().run_board_round().await;
        let mut pinned = 0;
        for pid in t.materialized_pids().unwrap() {
            pinned += t.pin_of(pid).await.unwrap().is_some() as usize;
        }
        if pinned == 0 {
            break;
        }
    }
    for pid in t.materialized_pids().unwrap() {
        assert_eq!(t.pin_of(pid).await.unwrap(), None, "partition {pid} is still pinned after the forget rounds");
    }
    assert_eq!(t.open_count(), 0, "the forget rounds left partitions open");
    let forgotten = db.transactions.try_forget();
    assert!(forgotten > x, "forgetting stopped at {forgotten}, below {x}");
    let tenant_one: Vec<_> = expected().into_iter().filter(|r| r.0 == 1).collect();
    assert_eq!(rows(&db, "tenant_id = 1").await, tenant_one, "the aborted row came back");
    drop(t);
    close(dir.path(), db);
}

/// A close takes nothing a statement holds: with a clone outstanding it
/// leaves the partition open; once that is dropped it closes.
///
/// Mutation: `Parts::close` without the strong-count check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_leaves_an_instance_in_use_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 7);
    t.close_idle().await.unwrap();
    let held = t.open_part(p).await.unwrap();
    assert_eq!(t.close_partition(p).await.unwrap(), Close::InUse);
    assert_eq!(t.close_idle().await.unwrap(), 0);
    assert_eq!(t.open_count(), 1);
    assert!(!held.is_retired(), "a close retired an instance a statement holds");
    drop(held);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::NotOpen);
    assert_eq!(t.open_count(), 0);
    drop(t);
    close(dir.path(), db);
}

/// I14: an access to a partition whose close is in flight waits for it,
/// then opens the files afresh; it never opens a second instance beside the
/// one the close still owns.
///
/// Mutation: `Parts::open_slow` without the lifecycle lock: the read opens
/// a new instance while the close is parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_waits_for_a_close_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 9);
    // Something staged for the close to land.
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (9, 77, 77)").await;
    let (reached, resume) = super::table::lifecycle_hooks::park_close(dir.path(), p);
    let closer = {
        let t = t.clone();
        tokio::spawn(async move { t.close_partition(p).await })
    };
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("the close never parked");
    let reader = {
        let db = db.clone();
        tokio::spawn(async move { rows(&db, "tenant_id = 9").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!reader.is_finished(), "a read opened partition {p} while its close was in flight");
    assert!(
        t.open_partitions_now().iter().all(|(pid, _)| *pid != p),
        "an instance of {p} was opened beside the closing one"
    );
    resume.notify_one();
    assert_eq!(closer.await.unwrap().unwrap(), Close::Closed);
    let mut want: Vec<_> = expected().into_iter().filter(|r| r.0 == 9).collect();
    want.push((9, 77, 77));
    want.sort();
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), reader).await.unwrap().unwrap(), want);
    drop(t);
    close(dir.path(), db);
}

/// A closed partition whose pointer file is gone fails every statement that
/// reaches it, routed or fanned out, read or write: never an empty answer
/// (a provider would create the missing file), never a fan-out that leaves
/// it out.
///
/// Mutations: `open_instance` without `require_existing` (the partition
/// reads as empty); the fan-out skipping a partition that fails to open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_that_cannot_reopen_fails_the_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 11);
    t.close_idle().await.unwrap();
    let [_, pointers] = super::files::data_files(t.uid(), p);
    std::fs::remove_file(dir.path().join(&pointers)).unwrap();
    for clause in ["tenant_id = 11", "", "v > 0"] {
        match try_rows(&db, clause).await {
            Err(e) => assert!(e.contains(&pointers) || e.contains("missing") || e.contains("not found") || e.contains("No such"), "WHERE {clause}: {e}"),
            Ok(got) => panic!("WHERE {clause} answered {} rows with partition {p} gone", got.len()),
        }
    }
    assert!(try_exec(&db, "SELECT COUNT(*) FROM pt").await.is_err(), "COUNT answered with a partition gone");
    assert!(try_exec(&db, "UPDATE pt SET v = 0 WHERE tenant_id = 11").await.is_err());
    assert!(try_exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (11, 500, 1)").await.is_err());
    assert!(!dir.path().join(&pointers).exists(), "an open created the missing file");
    drop(t);
    close(dir.path(), db);
}

/// A schema change validates the rows of closed partitions, and counts
/// them for its metadata-only test: a duplicate in a closed partition fails
/// `CREATE UNIQUE INDEX`, and `ADD COLUMN` on a table whose partitions are
/// all closed is refused as a populated table's is.
///
/// Mutations: `row_tables_held` over the open partitions only (the index is
/// built over the duplicate); `rows_len` counting open partitions only (the
/// ADD COLUMN is accepted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_change_reaches_closed_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 30, 303)").await;
    let t = pt(&db);
    // Refused with the partitions open: the populated rule.
    let open_refusal = try_exec(&db, "ALTER TABLE pt ADD COLUMN z BIGINT").await.expect_err("ADD COLUMN on a populated table");
    t.close_idle().await.unwrap();
    let closed_refusal =
        try_exec(&db, "ALTER TABLE pt ADD COLUMN z BIGINT").await.expect_err("ADD COLUMN with every partition closed was accepted");
    assert_eq!(closed_refusal, open_refusal);
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let dup = try_exec(&db, "CREATE UNIQUE INDEX pt_tv ON pt (tenant_id, v)").await;
    assert!(dup.is_err(), "a unique index was built over a duplicate in a closed partition");
    let t = pt(&db);
    t.close_idle().await.unwrap();
    // Without the duplicate it is built, and enforced in a partition closed
    // since.
    exec(&db, "DELETE FROM pt WHERE tenant_id = 3 AND id = 30").await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    exec(&db, "CREATE UNIQUE INDEX pt_tv ON pt (tenant_id, v)").await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    assert!(try_exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 31, 303)").await.is_err());
    assert_eq!(rows(&db, "").await, expected());
    drop(t);
    close(dir.path(), db);
}

/// `SET CONFIG vacuum.enabled = off` holds for a partition closed and
/// opened again.
///
/// Mutation: `Parts::apply_vacuum` doing nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_vacuum_setting_reaches_partitions_opened_later() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let p = pid_of_tenant(&t, 2);
    let _ = t.open_part(p).await.unwrap();
    set_config(&db, "SET CONFIG vacuum.enabled = off").await;
    t.close_idle().await.unwrap();
    for pid in t.materialized_pids().unwrap() {
        assert!(!t.open_part(pid).await.unwrap().vacuum_enabled().unwrap(), "partition {pid} reopened with vacuum on");
    }
    t.close_idle().await.unwrap();
    set_config(&db, "SET CONFIG vacuum.enabled = on").await;
    assert!(t.open_part(p).await.unwrap().vacuum_enabled().unwrap());
    drop(t);
    close(dir.path(), db);
}

/// A fan-out whose partitions close under it between two partition reads
/// reads every one, opening those it reaches closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_reads_every_partition_while_they_close_under_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let closer = t.clone();
    let hook: crate::core::rql::row_source::test_hooks::Hook = std::sync::Arc::new(move || {
        let closer = closer.clone();
        Box::pin(async move {
            closer.close_idle().await.unwrap();
        })
    });
    crate::core::rql::row_source::test_hooks::set_after_first(t.uid(), Some(hook));
    let got = rows(&db, "").await;
    crate::core::rql::row_source::test_hooks::set_after_first(t.uid(), None);
    assert_eq!(got, expected());
    drop(t);
    close(dir.path(), db);
}

/// Writers, readers, forget sweeps and a closer closing every idle
/// partition over and over: every acknowledged row is stored once, in one
/// process and after a restart, and every read in between saw a whole
/// table. Bounded: 4 writers of 120 rows each.
///
/// Mutation: `Parts::close` without the strong-count check: an instance a
/// writer still holds is retired and a second one opens beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn closes_racing_writers_readers_and_sweeps_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4i64 {
        let db = db.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..120i64 {
                let tenant = 1 + (w * 120 + i) % TENANTS;
                let id = 1000 + w * 1000 + i;
                exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, {id}, {id})")).await;
                if i % 10 == 0 {
                    exec(&db, &format!("UPDATE pt SET v = v + 1 WHERE tenant_id = {tenant} AND id = {id}")).await;
                }
            }
        }));
    }
    let closer = {
        let (t, stop) = (t.clone(), stop.clone());
        tokio::spawn(async move {
            let mut closed = 0;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                closed += t.close_idle().await.unwrap();
                tokio::task::yield_now().await;
            }
            closed
        })
    };
    let reader = {
        let (db, stop) = (db.clone(), stop.clone());
        tokio::spawn(async move {
            let mut reads = 0;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let all = rows(&db, "").await;
                // The initial rows are never touched: each read sees them all.
                let base = expected();
                let kept: Vec<_> = all.iter().copied().filter(|r| r.1 <= IDS).collect();
                assert_eq!(kept, base, "a read under the closes lost rows");
                reads += 1;
            }
            reads
        })
    };
    let sweeper = {
        let (t, stop) = (t.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                t.sweep_pins_now().await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    for w in writers {
        w.await.unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    let closed = closer.await.unwrap();
    let reads = reader.await.unwrap();
    sweeper.await.unwrap();
    assert!(closed > 8, "the closer closed only {closed} partitions");
    assert!(reads > 0);
    let mut want = expected();
    for w in 0..4i64 {
        for i in 0..120i64 {
            let tenant = 1 + (w * 120 + i) % TENANTS;
            let id = 1000 + w * 1000 + i;
            want.push((tenant, id, id + (i % 10 == 0) as i64));
        }
    }
    want.sort();
    assert_eq!(rows(&db, "").await, want);
    drop(t);
    let db = restart(dir.path(), db).await;
    assert_eq!(rows(&db, "").await, want, "after a restart");
    close(dir.path(), db);
}

/// Schema changes while a closer closes every idle partition over and over:
/// each change lands on every partition, open or closed, and every row
/// stays; a close of the instance a change replaced does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_changes_racing_closes_keep_one_schema_and_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closer = {
        let (db, stop) = (db.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                // The current instance each time: a schema change replaces it.
                let _ = pt(&db).close_idle().await;
                tokio::task::yield_now().await;
            }
        })
    };
    for ddl in [
        "CREATE INDEX pt_v ON pt (v)",
        "ALTER TABLE pt ADD CONSTRAINT v_pos CHECK (v > 0)",
        "ALTER TABLE pt RENAME COLUMN v TO w",
        "ALTER TABLE pt RENAME COLUMN w TO v",
        "ALTER TABLE pt DROP CONSTRAINT v_pos",
        "DROP INDEX pt_v",
        "ALTER TABLE pt ALTER COLUMN v SET DEFAULT 5",
    ] {
        exec(&db, ddl).await;
        if !ddl.contains("TO w") {
            assert_eq!(rows(&db, "").await, expected(), "after `{ddl}`");
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    closer.await.unwrap();
    let t = pt(&db);
    let shell = crate::core::index::shape_of(&t.shell().schema);
    for (pid, part) in t.all_partitions().await.unwrap() {
        assert_eq!(crate::core::index::shape_of(&part.schema), shell, "partition {pid} holds another schema");
        assert!(!part.is_retired(), "partition {pid} is a retired instance");
    }
    exec(&db, "INSERT INTO pt (tenant_id, id) VALUES (1, 500)").await;
    assert_eq!(rows(&db, "id = 500").await, vec![(1, 500, 5)]);
    drop(t);
    let db = restart(dir.path(), db).await;
    let mut want = expected();
    want.push((1, 500, 5));
    want.sort();
    assert_eq!(rows(&db, "").await, want, "after a restart");
    close(dir.path(), db);
}

/// A table instance a schema change replaced opens and closes nothing: a
/// background job still holding it (a forget round, a closer) neither opens
/// a second instance of a file set beside the new table's (I14), with the
/// old schema, nor closes one.
///
/// Mutation: `Parts::refuse_retired` doing nothing: the old instance opens
/// a closed partition with the old schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_instance_opens_and_closes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let old = pt(&db);
    let (p_open, p_closed) = (pid_of_tenant(&old, 1), pid_of_tenant(&old, 2));
    assert_ne!(p_open, p_closed);
    old.close_idle().await.unwrap();
    let _ = old.open_part(p_open).await.unwrap();
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let new = pt(&db);
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(old.open_part(p_closed).await.is_err(), "a replaced instance opened a partition");
    assert_eq!(old.close_partition(p_open).await.unwrap(), Close::Retired);
    assert!(new.open_partitions_now().iter().any(|(pid, _)| *pid == p_open), "the new instance lost its open partition");
    assert_eq!(rows(&db, "").await, expected());
    drop((old, new));
    close(dir.path(), db);
}

/// An operator's `rdb_index_checkpoint` reaches every materialized
/// partition, opening the closed ones; a shutdown's seal only the open ones.
///
/// Mutation: `seal_targets` naming the open partitions for the checkpoint
/// too: it seals none of a table whose partitions are closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_checkpoint_reaches_closed_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let materialized = t.materialized_pids().unwrap().len();
    t.close_idle().await.unwrap();
    let report = db.index_checkpoint("pt", None).await.unwrap();
    assert_eq!(report.sealed.len(), materialized, "the checkpoint left closed partitions out: {:?}", report.sealed.len());
    t.close_idle().await.unwrap();
    let shutdown = db.seal_for_shutdown(Duration::from_secs(30)).await;
    assert!(
        shutdown.sealed.iter().all(|s| !s.name.contains("partition")) && shutdown.skipped.iter().all(|n| !n.contains("partition")),
        "a shutdown opened a closed partition to seal it"
    );
    assert_eq!(t.open_count(), 0);
    drop(t);
    close(dir.path(), db);
}

/// Opening a closed partition when the process is out of file descriptors
/// (EMFILE, injected at the provider's open) fails the statement with
/// SQLSTATE 53000, never a panic; a fan-out that fails part-way closes the
/// partitions it opened for itself; and once descriptors are back the next
/// statement answers in full. (At 8bdcc88c the provider's `unwrap` panicked
/// the statement, and the partitions a fan-out had opened stayed open.)
///
/// Mutations: `Table::open_partition` back on the panicking `create_new`
/// (the statement panics); `out_of_resources` not naming the failure (it is
/// no 53000); `FanOut::each` not closing what it opened (partitions stay
/// open after the failed fan-out).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_out_of_file_descriptors_fails_the_statement_with_53000() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let materialized = t.materialized_pids().unwrap().len();
    t.close_idle().await.unwrap();
    let root = path_str(dir.path());
    // Two partitions open (two files each), then every open fails.
    crate::core::storage_providers::file_sync::open_test_hooks::fail_after(&root, 4);
    let fanned = try_rows(&db, "").await.expect_err("a fan-out over closed partitions answered with no descriptors left");
    assert!(fanned.contains("insufficient resources"), "{fanned}");
    assert!(!fanned.contains("panicked"), "{fanned}");
    assert_eq!(crate::pgwire::sqlstate_for(&fanned), "53000", "{fanned}");
    assert_eq!(t.open_count(), 0, "the failed fan-out left the partitions it opened open");
    let routed = try_rows(&db, "tenant_id = 3").await.expect_err("a routed read opened with no descriptors left");
    assert_eq!(crate::pgwire::sqlstate_for(&routed), "53000", "{routed}");
    let written = try_exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 99, 1)").await.expect_err("a write opened");
    assert_eq!(crate::pgwire::sqlstate_for(&written), "53000", "{written}");
    crate::core::storage_providers::file_sync::open_test_hooks::clear(&root);
    assert_eq!(rows(&db, "").await, expected(), "after descriptors came back");
    assert_eq!(t.open_count(), materialized);
    drop(t);
    close(dir.path(), db);
}

/// A reopened partition whose filter backfill cannot read a row keeps that
/// row a candidate: a query for its value still finds it once the backfill
/// is done. (At 8bdcc88c the backfill skipped the slot and then cleared the
/// unindexed prefix, so the filter pruned the row: a silent miss on every
/// reopen.)
///
/// Mutation: `backfill_into` clearing the prefix whatever it could not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_the_backfill_cannot_read_stays_a_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 1",
    )
    .await;
    // Past the filter's `min_table_rows` (4096), below which it builds no
    // mask and prunes nothing. Row i sits at slot i - 1 of the one partition.
    for batch in 0..12i64 {
        let values: Vec<String> =
            (batch * 500 + 1..=batch * 500 + 500).map(|i| format!("({}, {i}, {})", i % 7, 100_000 + i)).collect();
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    }
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let root = path_str(dir.path());
    // Row 3001 (v = 103001) is unreadable to the reopen's backfill.
    crate::core::row::table::backfill_test_hooks::fail_slot(&root, 3000);
    let part = t.open_part(0).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !part.filters_ready().unwrap() {
        assert!(std::time::Instant::now() < deadline, "the backfill never finished");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    crate::core::row::table::backfill_test_hooks::clear_failing(&root);
    drop(part);
    assert_eq!(
        rows(&db, "v = 103001").await,
        vec![(3001 % 7, 3001, 103001)],
        "the filter pruned a row its backfill could not read"
    );
    assert_eq!(rows(&db, "v = 103002").await, vec![(3002 % 7, 3002, 103002)]);
    drop(t);
    close(dir.path(), db);
}

/// The catalog never reports a closed partition's filter or indexes ready:
/// with every partition closed `SHOW TABLE`'s `filters_ready` and
/// `pg_indexes`' readiness are false, and true again once all are open
/// and built.
///
/// Mutation: the readiness folds over the open partitions only (true when
/// none is open).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_partitions_are_never_reported_ready() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let figures = crate::core::introspect::row_figures_for_tests(&db, "pt").await.unwrap();
    assert!(!figures.0, "filters reported ready with every partition closed");
    let ready = crate::core::rql::catalog_views::index_ready_for_tests(&db, "pt").unwrap();
    assert_eq!(ready, Some(false), "indexes reported ready with every partition closed");
    let _ = rows(&db, "").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let filters = crate::core::introspect::row_figures_for_tests(&db, "pt").await.unwrap().0;
        let indexes = crate::core::rql::catalog_views::index_ready_for_tests(&db, "pt").unwrap();
        if filters && indexes == Some(true) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never ready with every partition open: {filters} {indexes:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(t);
    close(dir.path(), db);
}
