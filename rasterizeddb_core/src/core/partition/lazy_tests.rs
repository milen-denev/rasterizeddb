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

/// `(tenant_id, id, v, <column>)` of every row, sorted: a column an ALTER
/// added, read with a default so a row it did not reach reads wrong.
async fn with_column(db: &Arc<Database>, column: &str) -> Vec<(i64, i64, i64, i64)> {
    let out = exec(db, &format!("SELECT tenant_id, id, v, {column} FROM pt")).await;
    let mut v: Vec<(i64, i64, i64, i64)> =
        decode_i64s(&out, 4).unwrap().into_iter().map(|r| (r[0], r[1], r[2], r[3])).collect();
    v.sort();
    v
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
    // Which partitions the fan-out read, not how many are open after it: a
    // forget round may sweep-open a pinned partition and close it again
    // once the fan-out lets go of it.
    let _ = crate::core::rql::row_source::test_hooks::take_reads(t.uid());
    assert_eq!(rows(&db, "").await, want);
    let mut read = crate::core::rql::row_source::test_hooks::take_reads(t.uid());
    read.sort();
    read.dedup();
    assert_eq!(read, materialized, "a fan-out left a partition unread");
    close(dir.path(), db);
}

/// A table's totals without the map log, which a forget round may append to
/// at any time.
fn partition_sizes(t: super::table::Totals) -> (u64, u64, u64, u64, u64, u32, u32) {
    (t.data, t.pointers, t.filters, t.slots, t.schema, t.materialized, t.declared)
}

/// Every partition closed between statements: each read and write answers
/// as with every partition open, the sizes reported are the same, and so
/// is everything after a restart.
///
/// Since P5b-2 a close seals (its index store gains a snapshot), so the
/// sizes compared are the closed table's against what the same files report
/// once reopened, and against a fresh read of their metadata after a
/// restart: the sizes a close keeps for the catalogs (`Slot::sizes`) are the
/// files' own.
///
/// Mutation: `closed_lens` leaving out the staged `.tmp` (or the totals
/// skipping closed partitions): the sizes differ once closed. (P5b-2) The
/// close keeping the sizes the instance had before its seal: they differ
/// from the files' after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_partition_answers_like_an_open_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    let open_totals = t.totals().await.unwrap();
    let materialized = t.materialized_pids().unwrap().len();
    assert_eq!(t.close_idle().await.unwrap(), materialized);
    assert_eq!(t.open_count(), 0);
    let closed_totals = t.totals().await.unwrap();
    let closed_parts = t.partitions().await.unwrap();
    // The rows and pointers are what they were open; the seal added only
    // index-store bytes.
    assert_eq!(
        (closed_totals.data, closed_totals.pointers, closed_totals.slots, closed_totals.materialized),
        (open_totals.data, open_totals.pointers, open_totals.slots, open_totals.materialized),
        "a closed table reports other sizes"
    );
    // A restart reads every closed partition's sizes from its files: the
    // same as the close kept.
    drop(t);
    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    // (The map log is left out: a forget round may append to it meanwhile.)
    assert_eq!(partition_sizes(t.totals().await.unwrap()), partition_sizes(closed_totals), "the sizes a close kept are not the files'");
    assert_eq!(t.partitions().await.unwrap(), closed_parts, "the partitions a close kept are not the files'");
    // And the instances report the same once reopened.
    assert_eq!(rows(&db, "").await, expected());
    assert_eq!(t.open_count(), materialized);
    assert_eq!(partition_sizes(t.totals().await.unwrap()), partition_sizes(closed_totals), "an open table reports other sizes than closed");
    assert_eq!(t.partitions().await.unwrap(), closed_parts, "an open table reports other partitions than closed");

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
/// `CREATE UNIQUE INDEX`, and `ADD COLUMN … DEFAULT` on a table whose
/// partitions are all closed rewrites every row as a populated table's does
/// (phase 6 P8), so each reads the default.
///
/// Mutations: `row_tables_held` over the open partitions only (the index is
/// built over the duplicate); `rows_len_exact` counting open partitions only
/// (the ADD COLUMN takes the empty table's metadata-only path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_change_reaches_closed_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    populated(&db).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 30, 303)").await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    exec(&db, "ALTER TABLE pt ADD COLUMN z BIGINT DEFAULT 9").await;
    let mut all = expected();
    all.push((3, 30, 303));
    all.sort();
    assert_eq!(with_column(&db, "z").await, all.iter().map(|&(t, i, v)| (t, i, v, 9)).collect::<Vec<_>>());
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
    // Its own budget: an open out of descriptors evicts before its retry,
    // and from the process's budget that would close other tests' partitions.
    let _budget = super::budget::isolate_with(dir.path(), super::budget::MAX_OPEN, Some(0));
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

/// Running out of file descriptors in the open's existence check, before
/// any provider is built, is `53000` as well. (At a2cb4589 the check
/// rewrapped the error with its path, dropping the errno, so the statement
/// failed with an internal error, review A round 2.)
///
/// Mutation: `require_existing` rewrapping EMFILE like any other failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_out_of_file_descriptors_in_the_existence_check_is_53000() {
    let dir = tempfile::tempdir().unwrap();
    // Its own budget: an open out of descriptors evicts before its retry,
    // and from the process's budget that would close other tests' partitions.
    let _budget = super::budget::isolate_with(dir.path(), super::budget::MAX_OPEN, Some(0));
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    super::files::existence_test_hooks::fail(dir.path());
    let routed = try_rows(&db, "tenant_id = 3").await.expect_err("a routed read opened with no descriptors left");
    assert!(routed.contains("insufficient resources"), "{routed}");
    assert_eq!(crate::pgwire::sqlstate_for(&routed), "53000", "{routed}");
    let written = try_exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 99, 1)").await.expect_err("a write opened");
    assert_eq!(crate::pgwire::sqlstate_for(&written), "53000", "{written}");
    super::files::existence_test_hooks::clear(dir.path());
    assert_eq!(t.open_count(), 0);
    assert_eq!(rows(&db, "").await, expected(), "after descriptors came back");
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
    // A fan-out opens scan-only (P5b-2b): its partitions' indexes are not
    // built, and not reported ready.
    let ready = crate::core::rql::catalog_views::index_ready_for_tests(&db, "pt").unwrap();
    assert_eq!(ready, Some(false), "indexes reported ready on scan-only partitions");
    // Upgraded (what a write, a DDL or a VACUUM reaches), they build.
    drop(t.all_partitions().await.unwrap());
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

/// The descriptors this process holds on files under `dir`.
fn fds_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out: Vec<_> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .flatten()
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .filter(|p| p.starts_with(dir))
        .collect();
    out.sort();
    out
}

/// An open that fails between its two providers (the pointer file's open,
/// then out of descriptors for the rows file's) leaves nothing behind: no
/// descriptor on the partition's files, and no log registration other than
/// the ones every successful open of the partition makes, which a close
/// keeps on purpose (§7.2: a close never unregisters, so a checkpoint after
/// it still syncs the files by path; dropping one would let a checkpoint
/// skip records already logged under the id, I16). Review A round 2 asked
/// whether the first provider's registration leaked: it is the startup
/// directory registration's own entry, unchanged.
///
/// Mutation: the failed open keeping the pointer provider alive (its
/// descriptors stay).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_failing_between_its_providers_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    // Its own budget: an open out of descriptors evicts before its retry,
    // and from the process's budget that would close other tests' partitions.
    let _budget = super::budget::isolate_with(dir.path(), super::budget::MAX_OPEN, Some(0));
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let root = path_str(dir.path());
    let wal = crate::core::wal::wal_for(&root).unwrap();
    let registered = || {
        let mut files: Vec<_> =
            wal.registered_files().into_iter().filter(|(_, p)| p.starts_with(dir.path())).collect();
        files.sort();
        files
    };
    let (files_before, fds_before) = (registered(), fds_under(dir.path()));
    let pid = t.materialized_pids().unwrap()[0];
    for _ in 0..20 {
        // The pointer file's provider opens; the rows file's fails.
        crate::core::storage_providers::file_sync::open_test_hooks::fail_after(&root, 1);
        let failed = t.open_part(pid).await.err().expect("the open succeeded with no descriptors left");
        crate::core::storage_providers::file_sync::open_test_hooks::clear(&root);
        assert_eq!(crate::pgwire::sqlstate_for(&failed.to_string()), "53000", "{failed}");
    }
    assert_eq!(fds_under(dir.path()), fds_before, "a failed open kept descriptors on the partition's files");
    assert_eq!(registered(), files_before, "a failed open left a registration");
    assert_eq!(t.open_count(), 0);
    // What a successful open and close leave, for comparison: the same, once
    // the open's one-shot tasks (its index build, which holds the providers
    // while it reads) have ended.
    drop(t.open_part(pid).await.unwrap());
    t.close_idle().await.unwrap();
    assert_eq!(registered(), files_before);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fds_under(dir.path()) != fds_before {
        assert!(std::time::Instant::now() < deadline, "a closed partition kept its descriptors");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(t);
    close(dir.path(), db);
}

// ------------------------------------------------------------------ P5b-2
// The open budget, its CLOCK and idle close (`partition::budget`).

fn open_pids(t: &PartitionedTable) -> Vec<u32> {
    let mut pids: Vec<u32> = t.open_partitions_now().into_iter().map(|(p, _)| p).collect();
    pids.sort();
    pids
}

/// Tenants routing to `n` distinct partitions, one tenant each, in pid order.
fn tenants_of_distinct_partitions(t: &PartitionedTable, n: usize, tenants: i64) -> Vec<(i64, u32)> {
    let mut seen = std::collections::BTreeMap::new();
    for tenant in 1..=tenants {
        seen.entry(pid_of_tenant(t, tenant)).or_insert(tenant);
    }
    let out: Vec<(i64, u32)> = seen.into_iter().map(|(p, tenant)| (tenant, p)).take(n).collect();
    assert_eq!(out.len(), n, "only {} partitions are reached by tenants 1..={tenants}", out.len());
    out
}

/// Routed reads of every partition in turn under a budget of 2: after each
/// statement at most two partitions are open, every answer is whole, and
/// the budget closed the rest to make room.
///
/// Mutation: `Budget::make_room` returning at once (every partition stays
/// open).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_budget_closes_what_nobody_holds_to_open_more() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    // No forget round sweep-opening partitions under the test's counts.
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let materialized = t.materialized_pids().unwrap().len();
    // The populating INSERT keeps a ticket on every partition it writes
    // until it commits, so its opens went past the budget: not counted.
    let (evictions, overruns) = (budget.evictions(), budget.overruns());
    for round in 0..3 {
        for tenant in 1..=TENANTS {
            let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == tenant).collect();
            assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, mine, "round {round}, tenant {tenant}");
            assert!(t.open_count() <= 2, "{} partitions open under a budget of 2", t.open_count());
            assert_eq!(budget.open(), t.open_count(), "the budget's count is not the slots'");
        }
    }
    let evicted = (budget.evictions() - evictions) as usize;
    assert!(evicted >= 3 * materialized - 2, "only {evicted} evictions");
    assert_eq!(budget.overruns(), overruns);
    assert!(super::table::instance_count::violations(dir.path()).is_empty());
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A partition a statement holds is never closed for the budget: an open
/// that finds the budget full and nothing else to close opens past it
/// (counted as an overrun), and the held instance stays the one in its slot.
///
/// Mutation: `Parts::close_held` without its strong-count check (the held
/// instance is closed under its holder).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_budget_never_closes_a_partition_in_use() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 1, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let overruns = budget.overruns();
    let pairs = tenants_of_distinct_partitions(&t, 3, TENANTS);
    let ((_, held_pid), (other_tenant, other_pid), (third_tenant, third_pid)) = (pairs[0], pairs[1], pairs[2]);
    let held = t.open_part(held_pid).await.unwrap();
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == other_tenant).collect();
    assert_eq!(rows(&db, &format!("tenant_id = {other_tenant}")).await, mine);
    assert_eq!(open_pids(&t), {
        let mut v = vec![held_pid, other_pid];
        v.sort();
        v
    });
    assert_eq!(budget.overruns(), overruns + 1, "the open did not go past the full budget");
    let again = t.open_part(held_pid).await.unwrap();
    assert!(Arc::ptr_eq(&held, &again), "the held partition was replaced");
    drop((held, again));
    // Released, both are what the next open closes to get back under it.
    let theirs: Vec<_> = expected().into_iter().filter(|r| r.0 == third_tenant).collect();
    assert_eq!(rows(&db, &format!("tenant_id = {third_tenant}")).await, theirs);
    assert_eq!(open_pids(&t), vec![third_pid]);
    assert!(super::table::instance_count::violations(dir.path()).is_empty());
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A fan-out wider than the budget reads every partition, and leaves the
/// budget's count equal to what is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_wider_than_the_budget_reads_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    let p1 = pid_of_tenant(&t, 1);
    for _ in 0..3 {
        t.close_idle().await.unwrap();
        assert_eq!(rows(&db, "").await, expected());
        assert_eq!(budget.open(), t.open_count());
        // The next open closes its share of the overrun, and the board
        // rounds trim the rest: what the fan-out holds is released by now.
        assert_eq!(t.close_partition(p1).await.unwrap(), Close::Closed);
        let before = t.open_count();
        assert_eq!(rows(&db, "tenant_id = 1").await, expected().into_iter().filter(|r| r.0 == 1).collect::<Vec<_>>());
        assert_eq!(t.open_count(), (before + 1 - super::budget::CLOSES_PER_OPEN).max(2));
        let board = t.pin_board().unwrap().clone();
        while t.open_count() > 2 {
            assert!(board.trim_budget().await > 0, "a board round trimmed nothing of an overrun");
        }
        assert_eq!(budget.open(), t.open_count());
    }
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Scan resistance: partitions a fan-out opened enter the CLOCK with their
/// reference bits clear, so the next open that needs room closes one of
/// them before any partition a routed access keeps using. Three rounds over
/// different partitions of a 32-partition table, so a CLOCK that set the
/// bit on a fan-out's open (and offered whichever came first after one lap)
/// is caught with high probability.
///
/// Mutation: `open_part_scan` setting the reference bit, as a routed open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_is_evicted_before_what_routed_reads_keep_using() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 5, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 32",
    )
    .await;
    let values: Vec<String> = (1..=200i64).map(|t| format!("({t}, 1, {t})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    let parts = tenants_of_distinct_partitions(&t, 21, 200);
    let overruns = budget.overruns();
    for round in 0..3 {
        t.close_idle().await.unwrap();
        let group = &parts[round * 7..round * 7 + 7];
        let (hot, rest) = group.split_at(3);
        let (scanned, later) = rest.split_at(2);
        for &(tenant, _) in hot {
            assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, vec![(tenant, 1, tenant)]);
        }
        let list = scanned.iter().map(|(tenant, _)| tenant.to_string()).collect::<Vec<_>>().join(", ");
        assert_eq!(rows(&db, &format!("tenant_id IN ({list})")).await.len(), 2);
        assert_eq!(t.open_count(), 5);
        for &(tenant, _) in later {
            assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, vec![(tenant, 1, tenant)]);
            // The routed workload keeps using its partitions (the hand
            // clears their bits as it passes).
            for &(tenant, _) in hot {
                assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, vec![(tenant, 1, tenant)]);
            }
        }
        let open = open_pids(&t);
        for &(tenant, pid) in hot {
            assert!(open.contains(&pid), "round {round}: tenant {tenant}'s routed partition {pid} was evicted before the fan-out's ({open:?})");
        }
        for &(_, pid) in scanned {
            assert!(!open.contains(&pid), "round {round}: the fan-out's partition {pid} outlived routed ones ({open:?})");
        }
    }
    assert_eq!(budget.overruns(), overruns);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Idle close (§7.2): a board round closes every open partition no routed
/// access touched for a whole period, and keeps the touched ones, and none
/// a statement holds.
///
/// Mutation: `Parts::close_idle_round` ignoring the reference bit (the
/// touched partition closes too).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_close_closes_only_what_no_routed_access_touched() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(1));
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let pairs = tenants_of_distinct_partitions(&t, 3, TENANTS);
    let ((busy, busy_pid), (held_tenant, held_pid), (_, idle_pid)) = (pairs[0], pairs[1], pairs[2]);
    // Every partition opened by a fan-out (bits clear), then one held.
    assert_eq!(rows(&db, "").await, expected());
    let materialized = t.materialized_pids().unwrap().len();
    assert_eq!(t.open_count(), materialized);
    let held = t.open_part_scan(held_pid).await.unwrap();
    // The scheduler's board rounds run idle rounds too; this test drives
    // more of them.
    let board = t.pin_board().unwrap().clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        // The busy tenant read throughout.
        let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == busy).collect();
        assert_eq!(rows(&db, &format!("tenant_id = {busy}")).await, mine);
        board.close_idle_round().await;
        if !open_pids(&t).contains(&idle_pid) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "idle close never closed partition {idle_pid}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let open = open_pids(&t);
    assert!(open.contains(&busy_pid), "a partition read every round was closed as idle ({open:?})");
    assert!(open.contains(&held_pid), "a held partition was closed as idle ({open:?})");
    assert_eq!(open.len(), 2, "idle partitions stayed open: {open:?}");
    drop(held);
    let _ = held_tenant;
    drop(board);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// I14 under the open budget (§16): an open/close storm at a budget of one
/// partition, under unique-key writers that race each other for the same
/// keys, routed readers, fan-outs and idle rounds every second. Every
/// acknowledged row is stored once; of two writers inserting one key,
/// exactly one is acknowledged and the other refused as a duplicate; no
/// read misses a row; and no file set ever has two live instances (counted
/// at every open and close). Again after a restart. Bounded: 4 writers of
/// 120 rows, 2 of them racing for 40 shared keys.
///
/// Mutation: `Parts::open_slow` putting the instance before the lifecycle
/// lock is taken (two opens of a pid build two instances).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_open_close_storm_at_budget_one_keeps_one_instance_and_every_row() {
    use std::sync::atomic::{AtomicBool, Ordering as O};
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 1, Some(1));
    let db = open(dir.path()).await;
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4i64 {
        let db = db.clone();
        writers.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            for i in 0..120i64 {
                let tenant = 1 + (w * 120 + i) % TENANTS;
                let id = 5000 + w * 1000 + i;
                exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, {id}, {id})")).await;
                acked.push((tenant, id, id));
                // A key every writer pair 0/1 races for: one of them wins.
                if w < 2 && i < 40 {
                    let (tenant, id) = (1 + i % TENANTS, 9000 + i);
                    match try_exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, {id}, {w})")).await {
                        Ok(_) => acked.push((tenant, id, w)),
                        Err(e) => assert_eq!(crate::pgwire::sqlstate_for(&e), "23505", "a racing insert failed otherwise: {e}"),
                    }
                }
                // An existing key: always refused.
                let dup = try_exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, 1, 0)")).await;
                let e = dup.expect_err("a duplicate of a populated key was acknowledged");
                assert_eq!(crate::pgwire::sqlstate_for(&e), "23505", "{e}");
            }
            acked
        }));
    }
    let mut readers = Vec::new();
    for r in 0..2i64 {
        let (db, stop) = (db.clone(), stop.clone());
        readers.push(tokio::spawn(async move {
            let mut reads = 0u64;
            let mut tenant = 1 + r;
            while !stop.load(O::Acquire) {
                let base: Vec<_> = expected().into_iter().filter(|x| x.0 == tenant).collect();
                let got: Vec<_> = rows(&db, &format!("tenant_id = {tenant}")).await.into_iter().filter(|x| x.1 <= IDS).collect();
                assert_eq!(got, base, "a routed read under the storm lost rows");
                tenant = 1 + (tenant + 3) % TENANTS;
                reads += 1;
            }
            reads
        }));
    }
    let fan = {
        let (db, stop) = (db.clone(), stop.clone());
        tokio::spawn(async move {
            let mut reads = 0u64;
            while !stop.load(O::Acquire) {
                let kept: Vec<_> = rows(&db, "").await.into_iter().filter(|x| x.1 <= IDS).collect();
                assert_eq!(kept, expected(), "a fan-out under the storm lost rows");
                reads += 1;
            }
            reads
        })
    };
    let idle = {
        let (board, stop) = (t.pin_board().unwrap().clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(O::Acquire) {
                board.close_idle_round().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };
    // And a closer closing whatever nobody holds, over and over: the storm.
    let closer = {
        let (t, stop) = (t.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(O::Acquire) {
                t.close_idle().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let (opens_before, closes_before) = t.open_close_counts();
    let mut want = expected();
    for w in writers {
        want.extend(w.await.unwrap());
    }
    stop.store(true, O::Release);
    for r in readers {
        assert!(r.await.unwrap() > 0);
    }
    assert!(fan.await.unwrap() > 0);
    idle.await.unwrap();
    closer.await.unwrap();
    want.sort();
    // One of each racing pair won.
    for i in 0..40i64 {
        assert_eq!(want.iter().filter(|x| x.1 == 9000 + i).count(), 1, "key {} acknowledged other than once", 9000 + i);
    }
    assert_eq!(rows(&db, "").await, want, "the storm lost or duplicated rows");
    let (opens, closes) = t.open_close_counts();
    let (opens, closes) = (opens - opens_before, closes - closes_before);
    assert!(opens > 50 && closes > 50, "{opens} opens, {closes} closes: not a storm");
    // Every open at a budget of one needed room: some was made by closing.
    assert!(budget.evictions() > 0, "the budget never closed a partition");
    let violations = super::table::instance_count::violations(dir.path());
    assert!(violations.is_empty(), "{violations:?}");
    drop(t);
    let db = restart(dir.path(), db).await;
    assert_eq!(rows(&db, "").await, want, "after a restart");
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Waits for `t`'s index load or build and its filter backfill to finish.
async fn settled(t: &crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    tokio::time::timeout(Duration::from_secs(60), t.index_load_settled()).await.expect("the index load never settled");
    while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(std::time::Instant::now() < deadline, "the filter or the indexes never became ready");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Idle close with the persistent-index snapshot (§7.2 point 3, P5b-2): a
/// partition closed after writes seals as it closes, and its next open, in
/// the same process or after a restart, loads its indexes from that
/// snapshot instead of rebuilding them, and answers through them as a scan
/// does; a reopen that writes nothing closes without writing a new one.
/// A write after the reopen makes the next close seal again.
///
/// Mutation: the close not sealing (`close_seal_hooks`, or
/// `close_exclusive(false)`): the reopen rebuilds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_closed_after_writes_reopens_from_the_snapshot_its_close_sealed() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let dir = tempfile::tempdir().unwrap();
    let _snap = storage_override::set(dir.path(), IndexStorage::Snapshot);
    let _budget = super::budget::isolate_with(dir.path(), super::budget::MAX_OPEN, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    let p = pid_of_tenant(&t, 3);
    let stem = super::files::data_stem(t.uid(), p);
    let ixs = dir.path().join(format!("{stem}.ixs"));
    assert_eq!(t.close_idle().await.unwrap(), t.materialized_pids().unwrap().len());
    let sealed = std::fs::metadata(&ixs).expect("the close sealed no snapshot");

    let check = |db: Arc<Database>| async move {
        for v in [301i64, 302, 1604, 77777, 999] {
            let by_index = rows(&db, &format!("v = {v}")).await;
            let by_scan = rows(&db, &format!("v + 0 = {v}")).await;
            assert_eq!(by_index, by_scan, "v = {v}");
        }
    };
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    assert_eq!(part.index_origin(), Origin::Loaded, "the reopen rebuilt rather than load the close's snapshot");
    drop(part);
    check(db.clone()).await;
    // A reopen that wrote nothing closes without a new snapshot.
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let after = std::fs::metadata(&ixs).unwrap();
    assert_eq!(
        (after.len(), after.modified().unwrap()),
        (sealed.len(), sealed.modified().unwrap()),
        "a read-only reopen's close wrote a new snapshot"
    );
    // A write, then a close: sealed again, and loaded again.
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 77, 77777)").await;
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    drop(part);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    assert_eq!(part.index_origin(), Origin::Loaded, "the close after a write sealed nothing the reopen loads");
    drop(part);
    assert_eq!(rows(&db, "v = 77777").await, vec![(3, 77, 77777)]);
    // After a restart, closed partitions load what their closes sealed.
    t.close_idle().await.unwrap();
    drop(t);
    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    assert_eq!(part.index_origin(), Origin::Loaded, "after a restart the partition rebuilt");
    drop(part);
    check(db.clone()).await;
    assert_eq!(rows(&db, "v = 77777").await, vec![(3, 77, 77777)]);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// C5's open-time scan of legacy `CHAR` keys runs for plain tables only; a
/// partition never carries the canonical flag (documented there). Reopening
/// partitions, by an explicit close and by the open budget's eviction (and
/// after the seal a close now writes), keeps that: every reopened instance
/// reads its `CHAR` keys as values, flag clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopened_partition_never_takes_the_legacy_char_canonical_flag() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, code CHAR(4) NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, code)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 8",
    )
    .await;
    let values: Vec<String> = (1..=TENANTS).map(|t| format!("({t}, 'c{t}', {t})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, code, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    for round in 0..2 {
        t.close_idle().await.unwrap();
        // Routed reads past the budget: each open evicts another.
        for tenant in 1..=TENANTS {
            let out = exec(&db, &format!("SELECT v FROM pt WHERE tenant_id = {tenant} AND code = 'c{tenant}'")).await;
            assert_eq!(decode_i64s(&out, 1).unwrap(), vec![vec![tenant]], "round {round}, tenant {tenant}");
            for (pid, part) in t.open_partitions_now() {
                assert!(!part.legacy_keys_canonical, "round {round}: partition {pid} took the canonical flag");
            }
        }
    }
    drop(t);
    let db = restart(dir.path(), db).await;
    let t = pt(&db);
    for (_, part) in t.all_partitions().await.unwrap() {
        assert!(!part.legacy_keys_canonical, "a partition opened after a restart took the canonical flag");
    }
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Out of descriptors at an open (§7.1): the open closes idle partitions
/// and tries once more, and the statement succeeds when that freed enough;
/// with nothing idle to close it fails with `53000`, as before.
///
/// Mutation: `Parts::open_slow` returning the error without evicting and
/// retrying (the routed read fails).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_out_of_descriptors_closes_idle_partitions_and_retries() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), super::budget::MAX_OPEN, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    let pairs = tenants_of_distinct_partitions(&t, 4, TENANTS);
    // Three partitions open and idle.
    for &(tenant, _) in &pairs[..3] {
        assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await.len() as i64, IDS);
    }
    assert_eq!(t.open_count(), 3);
    let root = path_str(dir.path());
    let evictions = budget.evictions();
    // The next provider open fails once, as a full descriptor table would.
    crate::core::storage_providers::file_sync::open_test_hooks::fail_next(&root, 1);
    let (tenant, pid) = pairs[3];
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == tenant).collect();
    assert_eq!(rows(&db, &format!("tenant_id = {tenant}")).await, mine, "the retried open did not answer");
    assert!(budget.evictions() > evictions, "nothing was closed before the retry");
    assert!(open_pids(&t).contains(&pid));
    // With every other partition held, nothing frees: 53000.
    let held: Vec<_> = open_pids(&t).into_iter().filter(|p| *p != pid).collect();
    let mut holds = Vec::new();
    for p in &held {
        holds.push(t.open_part(*p).await.unwrap());
    }
    holds.push(t.open_part(pid).await.unwrap());
    let closed_tenant = (1..=TENANTS).find(|x| !open_pids(&t).contains(&pid_of_tenant(&t, *x))).unwrap();
    crate::core::storage_providers::file_sync::open_test_hooks::fail_next(&root, 1);
    let failed = try_rows(&db, &format!("tenant_id = {closed_tenant}")).await.expect_err("an open with nothing to free answered");
    assert_eq!(crate::pgwire::sqlstate_for(&failed), "53000", "{failed}");
    crate::core::storage_providers::file_sync::open_test_hooks::clear(&root);
    drop(holds);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

// ---------------------------------------------------------- P5b-2a reviews

/// The catalogs' sizes and the rows length, as one tuple: (data, pointers,
/// slots, SHOW PARTITIONS' (pid, state, slots), rows_len).
async fn sizes_of(t: &PartitionedTable) -> (u64, u64, u64, Vec<(u32, &'static str, u64)>, u64) {
    let tot = t.totals().await.unwrap();
    let parts = t.partitions().await.unwrap().into_iter().map(|p| (p.pid, p.state, p.slots)).collect();
    (tot.data, tot.pointers, tot.slots, parts, t.rows_len().unwrap())
}

/// Review B's probe: during a sealing close (the instance out of its slot)
/// the catalogs and the rows length read what the files hold, not the
/// previous close's kept sizes. (At 7ba631c9 they read the sizes the
/// partition had before it was reopened and written.)
///
/// Mutation: `Slot::put` not clearing the kept sizes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_window_reads_the_files_not_the_previous_close() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 10_000, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1)").await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    t.close_idle().await.unwrap();
    let _first = sizes_of(&t).await;
    let values: Vec<String> = (2..=300).map(|i| format!("(1, {i}, {i})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let (reached, resume) = super::table::lifecycle_hooks::park_close(dir.path(), p);
    let closer = {
        let t = t.clone();
        tokio::spawn(async move { t.close_partition(p).await })
    };
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("parked");
    let during = sizes_of(&t).await;
    resume.notify_one();
    assert_eq!(closer.await.unwrap().unwrap(), Close::Closed);
    let after = sizes_of(&t).await;
    assert_eq!((during.0, during.1, during.2, during.4), (after.0, after.1, after.2, after.4), "the close window's sizes");
    assert_eq!(during.3, after.3, "the close window's partitions");
    assert_eq!(after.2, 300, "the kept slots are not the partition's");
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Parks the next close of `p`, runs `sql` while it is parked, then lets
/// the close go; the statement's outcome.
async fn during_a_close(db: &Arc<Database>, t: &Arc<PartitionedTable>, dir: &Path, p: u32, sql: &str) -> Result<Vec<u8>, String> {
    let (reached, resume) = super::table::lifecycle_hooks::park_close(dir, p);
    let closer = {
        let t = t.clone();
        tokio::spawn(async move { t.close_partition(p).await })
    };
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("parked");
    let statement = {
        let (db, sql) = (db.clone(), sql.to_string());
        tokio::spawn(async move { try_exec(&db, &sql).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    resume.notify_one();
    assert_eq!(closer.await.unwrap().unwrap(), Close::Closed);
    tokio::time::timeout(Duration::from_secs(20), statement).await.expect("the statement finished").unwrap()
}

/// Review B's B1: a partition emptied and closed (kept sizes zero), then
/// written: during its next close `ALTER TABLE … ADD COLUMN … DEFAULT` must
/// take the populated table's path, a rewrite of every row (phase 6 P8), so
/// each reads the default. (At 7ba631c9 the kept zero made it metadata-only;
/// it was refused on a populated table until P8.)
///
/// Mutations: the ALTER arm's emptiness check under its lock reading kept
/// sizes (`rows_len_held` back on `rows_len` with the cache).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_during_a_close_window_rewrites_a_populated_table() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 10_000, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 1",
    )
    .await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1)").await;
    exec(&db, "DELETE FROM pt WHERE tenant_id = 1").await;
    let _ = try_exec(&db, "VACUUM FULL pt").await;
    t.close_idle().await.unwrap();
    let _ = sizes_of(&t).await;
    let values: Vec<String> = (2..=50).map(|i| format!("(1, {i}, {i})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let alter = during_a_close(&db, &t, dir.path(), p, "ALTER TABLE pt ADD COLUMN z BIGINT DEFAULT 4").await;
    alter.expect("ADD COLUMN in a close window");
    assert_eq!(with_column(&db, "z").await, (2..=50).map(|i| (1, i, i, 4)).collect::<Vec<_>>());
    drop(t);
    let db = restart(dir.path(), db).await;
    assert_eq!(with_column(&db, "z").await, (2..=50).map(|i| (1, i, i, 4)).collect::<Vec<_>>());
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A's B1, the other way in: a materializing open that runs out of
/// descriptors leaves the partition materialized, closed and empty, whose
/// sizes a catalog read keeps as zero; two rows written, then during their
/// close `ADD COLUMN w … DEFAULT 7` must rewrite both (phase 6 P8), so each
/// reads 7, after a restart too. (At 7ba631c9 the kept zero made it
/// metadata-only; it was refused on a populated table until P8.)
///
/// Mutation: the kept sizes not cleared when a close takes the instance
/// out, with the DDL check reading them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_kept_sizes_never_let_an_alter_through() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 10_000, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let root = path_str(dir.path());
    // Materialized (its files created), then its open fails.
    crate::core::storage_providers::file_sync::open_test_hooks::fail_next(&root, 1);
    let failed = try_exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1)").await.expect_err("the open succeeded");
    assert_eq!(crate::pgwire::sqlstate_for(&failed), "53000", "{failed}");
    assert!(t.materialized_pids().unwrap().contains(&p));
    assert_eq!(t.open_count(), 0);
    // A catalog read keeps its (empty) sizes.
    assert_eq!(sizes_of(&t).await.4, 0);
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1), (1, 2, 2)").await;
    let alter = during_a_close(&db, &t, dir.path(), p, "ALTER TABLE pt ADD COLUMN w BIGINT DEFAULT 7").await;
    alter.expect("ADD COLUMN from zero kept sizes");
    assert_eq!(with_column(&db, "w").await, vec![(1, 1, 1, 7), (1, 2, 2, 7)]);
    assert!(sizes_of(&pt(&db)).await.4 > 0);
    drop(t);
    let db = restart(dir.path(), db).await;
    assert_eq!(with_column(&db, "w").await, vec![(1, 1, 1, 7), (1, 2, 2, 7)]);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review B's S2, reproduced by review A: the CLOCK upgrades an entry of a
/// table that is dropped meanwhile, so the upgrade is its last reference;
/// dropping it under the ring's guard ran the table's token drops, which
/// lock the same ring, and the open making room hung for good holding its
/// own lifecycle lock. The pause after the upgrade widens the window.
///
/// Mutation: the CLOCK dropping a passed-over instance under the guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_clock_never_drops_the_last_reference_to_a_table_under_its_ring() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    for name in ["a", "b"] {
        exec(
            &db,
            &format!(
                "CREATE TABLE {name} (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)) \
                 PARTITION BY HASH (tenant_id) PARTITIONS 4"
            ),
        )
        .await;
    }
    let a = match db.relation("a") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!(),
    };
    let values: Vec<String> = (1..=8).map(|i| format!("({i}, 1)")).collect();
    exec(&db, &format!("INSERT INTO a (tenant_id, id) VALUES {}", values.join(", "))).await;
    a.close_idle().await.unwrap();
    // Two of a's partitions open and touched: the budget is full of a.
    let pairs = tenants_of_distinct_partitions(&a, 2, 8);
    for &(tenant, _) in &pairs {
        exec(&db, &format!("SELECT id FROM a WHERE tenant_id = {tenant}")).await;
    }
    assert_eq!(a.open_count(), 2);
    drop(a);
    let (reached, resume) = super::budget::victim_hooks::park(dir.path());
    let insert = {
        let db = db.clone();
        tokio::spawn(async move { try_exec(&db, "INSERT INTO b (tenant_id, id) VALUES (1, 1)").await })
    };
    tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(10)).expect("the CLOCK never upgraded an entry"))
        .await
        .unwrap();
    exec(&db, "DROP TABLE a").await;
    // A watchdog off the runtime: a deadlock in the CLOCK blocks worker
    // threads one by one until no timer fires, so the test aborts the
    // binary with a message rather than hang.
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let finished = finished.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(30));
            if !finished.load(std::sync::atomic::Ordering::Acquire) {
                eprintln!("the_clock_never_drops_the_last_reference_to_a_table_under_its_ring: the insert hung in the CLOCK (a deadlock)");
                std::process::abort();
            }
        });
    }
    resume.send(()).unwrap();
    let done = tokio::time::timeout(Duration::from_secs(20), insert).await.expect("the insert hung in the CLOCK").unwrap();
    finished.store(true, std::sync::atomic::Ordering::Release);
    assert!(done.is_ok(), "{done:?}");
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review B's S1: an open that finds the budget overrun closes at most
/// `CLOSES_PER_OPEN` partitions, each a sealing close inside its statement;
/// the board rounds trim the rest. (At 7ba631c9 the next statement closed
/// the whole overrun: 7.4 s for a single-row INSERT after a 200-tenant one
/// on ext4.)
///
/// Mutation: `make_room` without the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_closes_a_few_and_the_board_rounds_trim_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    let materialized = t.materialized_pids().unwrap().len();
    // The populating INSERT held every partition it wrote: all open.
    assert_eq!(t.open_count(), materialized, "the multi-tenant INSERT did not overrun");
    let p = pid_of_tenant(&t, 1);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let (before, evictions) = (t.open_count(), budget.evictions());
    assert_eq!(rows(&db, "tenant_id = 1").await, expected().into_iter().filter(|r| r.0 == 1).collect::<Vec<_>>());
    assert_eq!(budget.evictions() - evictions, super::budget::CLOSES_PER_OPEN as u64, "the open closed other than its share");
    assert_eq!(t.open_count(), before - super::budget::CLOSES_PER_OPEN + 1);
    let board = t.pin_board().unwrap().clone();
    let mut rounds = 0;
    while t.open_count() > 2 {
        let closed = board.trim_budget().await;
        assert!(closed <= super::budget::TRIM_PER_ROUND);
        rounds += 1;
        assert!(rounds < 10, "the board rounds never trimmed the overrun");
    }
    assert_eq!(budget.open(), t.open_count());
    drop(board);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A's S2: a closing partition stays counted in the budget until its
/// instance, and so its descriptors, are gone.
///
/// Mutation: the token released when the close takes the instance out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closing_partition_stays_counted_until_its_descriptors_go() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let open_before = budget.open();
    let (reached, resume) = super::table::lifecycle_hooks::park_close(dir.path(), p);
    let closer = {
        let t = t.clone();
        tokio::spawn(async move { t.close_partition(p).await })
    };
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("parked");
    assert_eq!(budget.open(), open_before, "a partition mid-close stopped counting");
    resume.notify_one();
    assert_eq!(closer.await.unwrap().unwrap(), Close::Closed);
    assert_eq!(budget.open(), open_before - 1);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A's S1: a close runs on a task of its own, so a caller dropped
/// mid-close (a cancelled statement) leaves the close to finish under its
/// lifecycle lock: an open of the partition waits for it, and the close
/// counts.
///
/// Mutation: the close body run inline in its caller (`close_detached`
/// without the task).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_whose_caller_is_dropped_still_finishes_under_its_lock() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let (_, closes_before) = t.open_close_counts();
    let (reached, resume) = super::table::lifecycle_hooks::park_close(dir.path(), p);
    let caller = {
        let t = t.clone();
        tokio::spawn(async move { t.close_partition(p).await })
    };
    tokio::time::timeout(Duration::from_secs(10), reached.notified()).await.expect("parked");
    caller.abort();
    let _ = caller.await;
    let opener = {
        let t = t.clone();
        tokio::spawn(async move { t.open_part(p).await.map(|_| ()) })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!opener.is_finished(), "an open ran while the dropped caller's close was still under way");
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), opener).await.expect("the open never ran").unwrap().unwrap();
    assert_eq!(t.open_close_counts().1, closes_before + 1, "the dropped caller's close did not finish");
    assert!(super::table::instance_count::violations(dir.path()).is_empty());
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Under `index_storage = rebuild` the seal at close still buys the next
/// open its filter: the snapshot's FILTER section is loaded (and vouched
/// for) instead of backfilled, though the indexes rebuild. So the seal is
/// kept in that mode.
///
/// Mutation: the close not sealing (the reopen backfills; nothing vouched).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_rebuild_the_seal_at_close_buys_the_reopen_its_filter() {
    use crate::core::index_store::load::{settings::IndexStorage, storage_override};
    let dir = tempfile::tempdir().unwrap();
    let _rebuild = storage_override::set(dir.path(), IndexStorage::Rebuild);
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    let p = pid_of_tenant(&t, 1);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    assert!(part.gate().vouched(), "the reopen did not load the filter its close sealed");
    drop(part);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A's second race on the kept sizes: a catalog read that read a
/// closed partition's files, then was overtaken by an open, writes and a
/// close, must not keep what it read over the close's final sizes. (At
/// 7ba631c9 `closed_sizes` dropped its lock between its check and its
/// store, and the stale read stayed until the next open and close.)
///
/// Mutation: `closed_sizes` keeping its read whatever happened meanwhile
/// (no epoch check).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catalog_read_overtaken_by_a_close_never_keeps_its_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    t.close_idle().await.unwrap();
    drop(t);
    // After a restart nothing is kept: the next read reads the files.
    let db = restart(dir.path(), db).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let (reached, resume) = super::table::kept_sizes_hooks::park(dir.path(), p);
    let reader = {
        let t = t.clone();
        tokio::spawn(async move { t.totals().await.unwrap() })
    };
    tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(10)).expect("the read never reached the files"))
        .await
        .unwrap();
    // Meanwhile: opened, written, closed.
    let values: Vec<String> = (100..400).map(|i| format!("(1, {i}, {i})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    let final_sizes = t.totals().await.unwrap();
    resume.send(()).unwrap();
    let _stale = reader.await.unwrap();
    assert_eq!(t.totals().await.unwrap(), final_sizes, "a read overtaken by a close kept its sizes over the close's");
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// An open refused because its partition is not materialized closes
/// nothing for the budget (review B: `make_room` ran before the refusal).
///
/// Mutation: `make_room` before the not-materialized refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_refused_as_unmaterialized_evicts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 1, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1)").await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let unmaterialized = (0..8u32).find(|q| !t.materialized_pids().unwrap().contains(q)).unwrap();
    assert_eq!(open_pids(&t), vec![p]);
    let evictions = budget.evictions();
    assert!(t.open_part(unmaterialized).await.is_err(), "an unmaterialized partition opened");
    assert_eq!(budget.evictions(), evictions, "a refused open closed a partition for the budget");
    assert_eq!(open_pids(&t), vec![p]);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

// ------------------------------------------------------------------ P5b-2b
// Scan-only fan-out opens.

/// A populated table with an index on `v`, under `index_storage =
/// snapshot`, every partition closed by a sealing close (so each has a
/// snapshot with INDEX sections), the scheduler parked.
async fn sealed_and_closed(dir: &Path) -> (Arc<Database>, crate::core::index_store::load::storage_override::Guard) {
    use crate::core::index_store::load::{settings::IndexStorage, storage_override};
    let snap = storage_override::set(dir, IndexStorage::Snapshot);
    // These tests are of deferral: no proven load starts at a scan-only open.
    super::table::scan_open_hooks::defer_loads(dir);
    let db = open(dir).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    assert_eq!(t.close_idle().await.unwrap(), t.materialized_pids().unwrap().len());
    (db, snap)
}

/// A fan-out that only reads opens every partition scan-only (P5b-2b):
/// no index load or build starts, the answers are whole, and closing them
/// again writes nothing, so a routed open afterwards loads the snapshot the
/// writes' closes sealed.
///
/// Mutations: the fan-out's open made full (the indexes load or build);
/// a scan-only close sealing (its transient ticket marks the snapshot
/// `DIRTY`, and the next open rebuilds).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_opens_scan_only_and_its_close_writes_nothing() {
    use crate::core::index_store::load::Origin;
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let open = t.open_partitions_now();
    assert_eq!(open.len(), t.materialized_pids().unwrap().len());
    tokio::time::sleep(Duration::from_millis(300)).await;
    for (pid, part) in &open {
        assert!(part.is_scan_only(), "partition {pid} opened full by a fan-out");
        assert_eq!(part.index_origin(), Origin::Pending, "partition {pid}'s indexes started on a fan-out's open");
    }
    drop(open);
    t.close_idle().await.unwrap();
    let p = pid_of_tenant(&t, 3);
    let part = t.open_part(p).await.unwrap();
    settled(&part).await;
    assert_eq!(part.index_origin(), Origin::Loaded, "a scan-only close spoiled the snapshot");
    drop(part);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A routed read reaching a scan-only instance upgrades it in place: the
/// same instance, its indexes loaded, every answer through them the same as
/// a scan's, while a concurrent fan-out keeps reading.
///
/// Mutation: `planned` not upgrading the instance a routed read reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_routed_read_upgrades_a_scan_only_instance_in_place() {
    use crate::core::index_store::load::Origin;
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let p = pid_of_tenant(&t, 3);
    let before = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(before.is_scan_only());
    let scanner = {
        let db = db.clone();
        tokio::spawn(async move {
            for _ in 0..20 {
                assert_eq!(rows(&db, "").await, expected(), "a fan-out racing an upgrade lost rows");
            }
        })
    };
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == 3).collect();
    assert_eq!(rows(&db, "tenant_id = 3").await, mine);
    let after = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(Arc::ptr_eq(&before, &after), "the upgrade reopened the partition");
    assert!(!after.is_scan_only(), "a routed read left the instance scan-only");
    settled(&after).await;
    assert_eq!(after.index_origin(), Origin::Loaded);
    for v in [301i64, 302, 303, 304] {
        assert_eq!(rows(&db, &format!("v = {v}")).await, rows(&db, &format!("v + 0 = {v}")).await, "v = {v}");
    }
    scanner.await.unwrap();
    drop((before, after));
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A write reaching a scan-only instance upgrades it before its ticket,
/// whatever path it took (here a row written straight through the instance
/// a fan-out opened, which no routing step upgrades): the load's barrier is
/// up before the write, so the loaded index holds the new row too.
///
/// Mutation: `enter_write` not upgrading the instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_upgrades_a_scan_only_instance_before_its_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let p = pid_of_tenant(&t, 3);
    let part = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(part.is_scan_only());
    let qp = crate::core::rql::lexer_s1::QueryPurpose::InsertRow(crate::core::rql::lexer_s1::InsertRowData {
        table_name: "pt".into(),
        query: "INSERT INTO pt (tenant_id, id, v) VALUES (3, 77, 99977)".into(),
        on_conflict_do_nothing: false,
        on_conflict_target: Vec::new(),
        on_conflict_update: None,
    });
    let row = crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0).unwrap();
    let tx = db.transactions.begin();
    part.insert_row_in_txn(row, tx).await.unwrap();
    db.transactions.commit(tx);
    assert!(!part.is_scan_only(), "a write left the instance scan-only");
    settled(&part).await;
    assert_eq!(rows(&db, "v = 99977").await, vec![(3, 77, 99977)], "the index missed a row written at the upgrade");
    drop(part);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A scan-only instance whose file set something wrote with no write path
/// before its upgrade (a forget sweep's neutralizing, a vacuum tick: a
/// ticket, no new slot) still loads the snapshot its open proved, and the
/// routed read that upgrades it waits for that load rather than scan
/// (review B of P5b-2b: a sweep's ticket demoted every deferred load to a
/// build, and the first routed read after a fan-out scanned).
///
/// Mutation: the deferred load demoted whenever the word left `Clean`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upgrade_after_a_ticket_without_new_slots_still_loads() {
    use crate::core::index_store::load::Origin;
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let p = pid_of_tenant(&t, 3);
    let part = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(part.is_scan_only());
    // A ticket on the word with no write path: what a sweep or a vacuum takes.
    {
        let _ticket = crate::core::index_store::gate::ticket(part.gate(), None).await.unwrap();
    }
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == 3).collect();
    assert_eq!(rows(&db, "tenant_id = 3").await, mine);
    settled(&part).await;
    assert_eq!(part.index_origin(), Origin::Loaded, "a ticket without new slots demoted the deferred load");
    for v in [301i64, 302, 303, 304] {
        assert_eq!(rows(&db, &format!("v = {v}")).await, rows(&db, &format!("v + 0 = {v}")).await, "v = {v}");
    }
    drop(part);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A scan-only instance whose slots were renumbered after its open (what a
/// compaction does) does not load the snapshot its open proved: its indexes
/// build from the rows as they are.
///
/// Mutation: the deferred load kept whatever happened after the open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upgrade_after_a_renumbering_builds_rather_than_loads() {
    use crate::core::index_store::load::Origin;
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let p = pid_of_tenant(&t, 3);
    let part = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(part.is_scan_only());
    // A renumbering's epoch moves, as `Table::compact` moves it.
    {
        let ticket = crate::core::index_store::gate::test_mint();
        part.data().unwrap().indexes.as_ref().unwrap().invalidate(crate::core::index_store::gate::Proof::from(&ticket));
    }
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == 3).collect();
    assert_eq!(rows(&db, "tenant_id = 3").await, mine);
    settled(&part).await;
    assert_ne!(part.index_origin(), Origin::Loaded, "a renumbered file set loaded the snapshot its open had proved");
    for v in [301i64, 302, 303, 304] {
        assert_eq!(rows(&db, &format!("v = {v}")).await, rows(&db, &format!("v + 0 = {v}")).await, "v = {v}");
    }
    drop(part);
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Shutdown seals the open partitions, but not a scan-only one, whose
/// seal's transient ticket would spoil the snapshot its next open loads;
/// and nothing else (its unit's idle checkpoint, which acts only on a
/// `DIRTY` word) spoils it while it is open.
///
/// Mutation: shutdown sealing scan-only partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_and_idle_checkpoints_leave_scan_only_partitions_alone() {
    use crate::core::index_store::load::Origin;
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let _idle = crate::core::row::table::table_seal::idle_override::set(dir.path(), Duration::from_millis(50));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    // The scheduler running again, for the idle checkpoint's looks.
    db.partition_maintenance().set_period(Duration::from_millis(20));
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let report = db.seal_for_shutdown(Duration::from_secs(30)).await;
    let _ = report;
    drop(t);
    let db = restart(dir.path(), db).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    let t = pt(&db);
    for pid in t.materialized_pids().unwrap() {
        let part = t.open_part(pid).await.unwrap();
        settled(&part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid}'s snapshot was spoiled while it was scan-only");
    }
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A round 2's N1: an instance referenced again as its close drops
/// it stays counted in the budget, with its descriptors, until the last
/// reference goes.
///
/// Mutation: the token released whatever the instance's references.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_kept_past_its_close_stays_counted() {
    let dir = tempfile::tempdir().unwrap();
    let budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let open_before = budget.open();
    super::table::close_leak_hooks::keep(dir.path(), p);
    assert_eq!(t.close_partition(p).await.unwrap(), Close::Closed);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(budget.open(), open_before, "a partition still referenced stopped counting");
    super::table::close_leak_hooks::release(dir.path());
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while budget.open() != open_before - 1 {
        assert!(std::time::Instant::now() < deadline, "the token never went with the last reference");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// What the open budget assumes of descriptors, against what `/proc/self/fd`
/// counts on the files under the test's own directory: an open partition
/// holds exactly `PER_PARTITION` (the budget subtracts that per open
/// partition from the descriptors it counts in use), and the count the
/// budget reads sees every descriptor a plain table holds. Settled, a plain
/// table holds twenty (four providers, five each), whatever its indexes:
/// review B of P5b-2b measured 40 with its primary key's index and 20 more
/// per added index, which was the instances each DDL replaced still held
/// for up to two seconds (fixed in P5b-3, `schema_changes_give_a_plain_
/// tables_descriptors_back_at_once`). The budget counts rather than charges
/// all the same: what a plain table holds in passing is not a constant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_budget_counts_what_tables_and_partitions_hold() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    let held = |name: &str| fds_under(dir.path()).iter().filter(|p| p.file_name().is_some_and(|f| f.to_string_lossy().starts_with(name))).count() as u64;
    exec(&db, "CREATE TABLE plain (id BIGINT PRIMARY KEY, v BIGINT)").await;
    exec(&db, "CREATE INDEX plain_v ON plain (v)").await;
    exec(&db, "INSERT INTO plain (id, v) VALUES (1, 1), (2, 2)").await;
    exec(&db, &"SELECT id FROM plain WHERE v = 1".to_string()).await;
    exec(&db, CREATE).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 1)").await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let stem = super::files::data_stem(t.uid(), p);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(held(&stem), super::budget::PER_PARTITION, "an open partition does not hold what the budget assumes");
    let plain = held("plain");
    assert_eq!(plain, 20, "a plain table with two indexes holds {plain} descriptors, settled");
    assert!(
        super::budget::descriptors_in_use() >= fds_under(dir.path()).len() as u64,
        "the budget's count misses descriptors this process holds"
    );
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// In snapshot mode, a fan-out's open whose snapshot proves loadable loads
/// it at once while the open budget has room (P5b-2b review B: deferring a
/// cheap load made every first routed read after a fan-out pay it alone,
/// where full opens load every partition at once); with the budget past
/// half full, it stays scan-only and defers the load.
///
/// Mutation: a proven load never started at a scan-only open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_loads_a_proven_snapshot_while_the_budget_has_room() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let dir = tempfile::tempdir().unwrap();
    let _snap = storage_override::set(dir.path(), IndexStorage::Snapshot);
    let budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    t.close_idle().await.unwrap();
    assert_eq!(rows(&db, "").await, expected());
    for (pid, part) in t.open_partitions_now() {
        assert!(!part.is_scan_only(), "partition {pid}: a proven snapshot deferred with the budget's room");
        settled(&part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid}");
    }
    let _ = budget;
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
    // Past half the budget, the same fan-out stays scan-only.
    let dir = tempfile::tempdir().unwrap();
    let _snap = storage_override::set(dir.path(), IndexStorage::Snapshot);
    let _budget = super::budget::isolate_with(dir.path(), 2, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    t.close_idle().await.unwrap();
    assert_eq!(rows(&db, "").await, expected());
    let scan_only = t.open_partitions_now().iter().filter(|(_, p)| p.is_scan_only()).count();
    assert!(scan_only >= t.materialized_pids().unwrap().len() - 1, "only {scan_only} scan-only past half the budget");
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A fan-out's eager proven load needs evidence of routed traffic (P5b-3):
/// after a start, where no routed access has reached any partition yet, a
/// fan-out with all the budget's room leaves every partition scan-only;
/// once a routed read has reached one, the next fan-out loads that one's
/// snapshot at once and still defers the rest. The evidence outlives the
/// partition's close.
///
/// Mutation: the eager load not asking for routed evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_loads_at_once_only_where_routed_traffic_went() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let dir = tempfile::tempdir().unwrap();
    let _snap = storage_override::set(dir.path(), IndexStorage::Snapshot);
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let db = open(dir.path()).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    populated(&db).await;
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let t = pt(&db);
    for (_, part) in t.open_partitions_now() {
        settled(&part).await;
    }
    t.close_idle().await.unwrap();
    drop(t);
    let db = restart(dir.path(), db).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let opened = t.open_partitions_now();
    assert_eq!(opened.len(), t.materialized_pids().unwrap().len());
    for (pid, part) in &opened {
        assert!(part.is_scan_only(), "partition {pid}: loaded at once with no routed access since the start");
    }
    drop(opened);
    let routed = pid_of_tenant(&t, 3);
    assert_eq!(rows(&db, "tenant_id = 3").await.len(), IDS as usize);
    for (_, part) in t.open_partitions_now() {
        if !part.is_scan_only() {
            settled(&part).await;
        }
    }
    t.close_idle().await.unwrap();
    assert!(t.open_partitions_now().is_empty());
    assert_eq!(rows(&db, "").await, expected());
    for (pid, part) in t.open_partitions_now() {
        if pid == routed {
            assert!(!part.is_scan_only(), "partition {pid}: routed before, its proven snapshot deferred");
            settled(&part).await;
            assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid}");
        } else {
            assert!(part.is_scan_only(), "partition {pid}: loaded at once with no routed access");
        }
    }
    drop(t);
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// Review A round 2 of P5b-2b: a scan-only partition's deferred load can
/// run on a word already `Dirty` (a sweep or a vacuum tick wrote first),
/// where a write's ticket takes the fast path and writes while the load is
/// in flight. A load is a union, so the index still answers as a scan: the
/// load held (`load::hooks::hold`) while an INSERT, an UPDATE of the indexed
/// column, a DELETE and a vacuum run, then every probe through the index
/// equals the scan, a duplicate key is refused, and so after a restart. Run
/// in memory residency by the lib suite and in disk residency by its
/// `RASTERIZEDDB_INDEX_RESIDENCY=disk` run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_during_a_deferred_load_on_a_dirty_word_keep_the_index_whole() {
    let dir = tempfile::tempdir().unwrap();
    let _budget = super::budget::isolate_with(dir.path(), 64, Some(0));
    let (db, _snap) = sealed_and_closed(dir.path()).await;
    let t = pt(&db);
    assert_eq!(rows(&db, "").await, expected());
    let p = pid_of_tenant(&t, 3);
    let part = t.open_partitions_now().into_iter().find(|(q, _)| *q == p).unwrap().1;
    assert!(part.is_scan_only());
    // The word made `Dirty` with no write path, as a sweep makes it.
    {
        let _ticket = crate::core::index_store::gate::ticket(part.gate(), None).await.unwrap();
    }
    let hold = crate::core::index_store::load::hooks::hold(dir.path());
    // A routed read upgrades the instance; its load waits in the hold (the
    // read waits `load_wait`, then scans).
    let mine: Vec<_> = expected().into_iter().filter(|r| r.0 == 3).collect();
    assert_eq!(rows(&db, "tenant_id = 3").await, mine);
    assert!(part.gate().loads().loading(), "the deferred load did not start");
    let writer = {
        let (db, part) = (db.clone(), part.clone());
        tokio::spawn(async move {
            exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 50, 99950)").await;
            exec(&db, "UPDATE pt SET v = 99951 WHERE tenant_id = 3 AND id = 1").await;
            exec(&db, "DELETE FROM pt WHERE tenant_id = 3 AND id = 2").await;
            let _ = part.vacuum_now().await;
        })
    };
    // Writes on a `Dirty` word do not wait for the load; give them time,
    // then let the load go whatever they did.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while !writer.is_finished() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    drop(hold);
    writer.await.unwrap();
    settled(&part).await;
    drop(part);
    let check = |db: Arc<Database>| async move {
        for v in [99950i64, 99951, 301, 302, 303, 304, 1601] {
            assert_eq!(rows(&db, &format!("v = {v}")).await, rows(&db, &format!("v + 0 = {v}")).await, "v = {v}");
        }
        assert_eq!(rows(&db, "v = 99951").await, vec![(3, 1, 99951)]);
        assert!(rows(&db, "v = 302").await.is_empty(), "a deleted row read through the index");
        for dup in ["(3, 50, 0)", "(3, 1, 0)", "(3, 3, 0)"] {
            let e = try_exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {dup}")).await.expect_err("a duplicate key was stored");
            assert_eq!(crate::pgwire::sqlstate_for(&e), "23505", "{dup}: {e}");
        }
    };
    check(db.clone()).await;
    drop(t);
    let db = restart(dir.path(), db).await;
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    check(db.clone()).await;
    close(dir.path(), db);
    super::budget::release(dir.path());
}

/// A database with no partitioned table runs no scheduler, so no board
/// round refreshes the open budget's count of descriptors; plain tables'
/// opens do, once it is due (P5b-2b review: the count stayed the start's,
/// and a server holding only plain tables stopped raising its soft limit as
/// they opened). Twenty plain tables opened, a period waited, one more
/// opened: the count moves by what they hold.
///
/// Mutation: `note_plain_open` not refreshing the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_tables_opening_keep_the_descriptor_count_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    let _ = super::budget::descriptors_in_use();
    tokio::time::sleep(2 * super::budget::RECOUNT_EVERY + Duration::from_millis(100)).await;
    let before = super::budget::descriptors_in_use();
    for i in 0..20 {
        exec(&db, &format!("CREATE TABLE plain{i} (id BIGINT PRIMARY KEY, v BIGINT)")).await;
        exec(&db, &format!("INSERT INTO plain{i} (id, v) VALUES (1, 1)")).await;
    }
    tokio::time::sleep(super::budget::RECOUNT_EVERY + Duration::from_millis(100)).await;
    // Another test's board round may recount meanwhile in a full run; what
    // this one needs is a count taken after the last open, holding them.
    let last_open = std::time::Instant::now();
    exec(&db, "CREATE TABLE plain_last (id BIGINT PRIMARY KEY, v BIGINT)").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while super::budget::counted_at().is_none_or(|t| t < last_open) {
        assert!(
            std::time::Instant::now() < deadline,
            "no recount after twenty plain tables opened: the count stayed {} (was {before})",
            super::budget::descriptors_in_use()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The claim is stamped before its listing lands.
    while super::budget::descriptors_in_use() == before {
        assert!(
            std::time::Instant::now() < deadline,
            "the recount after twenty plain tables opened did not move from {before}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(db.partitioned_table("plain0").is_none());
    close(dir.path(), db);
}


/// The descriptors held on files under `dir` whose names start with
/// `prefix`, a deleted file's included.
fn fds_named(dir: &Path, prefix: &str) -> usize {
    fds_under(dir)
        .iter()
        .filter(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with(prefix)))
        .count()
}

/// Polls until `count()` is `want`, for at most `within`; the last count.
async fn count_returns_to(count: impl Fn() -> usize, want: usize, within: Duration) -> usize {
    let deadline = std::time::Instant::now() + within;
    loop {
        let now = count();
        if now == want || std::time::Instant::now() >= deadline {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A schema change gives the replaced instance's descriptors back at once
/// (P5b-3). Review B's probe read a plain table's descriptors as growing
/// with every schema change, 40 to 180 over six; measured to the end, they
/// never grew: the instance a change replaced held its providers until its
/// service task's next two-second poll, and a rewrite's two instances
/// (its build and its reopen) on the old, deleted files the same. A burst of
/// ALTERs (a migration) held twenty per statement per table until the polls
/// caught up. Now the drop wakes the task, and what is left is the vacuum
/// service's quarter-second slice after the retirement.
///
/// Ten changes, rewrites among them: after each, the table's descriptors are
/// back to what it held before within a second. Then a restart holds the
/// same.
///
/// Mutation: the drop not waking the task (`services_released`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_changes_give_a_plain_tables_descriptors_back_at_once() {
    let dir = tempfile::tempdir().unwrap();
    super::budget::isolate_with(dir.path(), 100_000, Some(0));
    let db = open(dir.path()).await;
    exec(&db, "CREATE TABLE q (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)").await;
    exec(&db, "INSERT INTO q (id, v, s) VALUES (1, 1, 'a'), (2, 2, 'b')").await;
    exec(&db, "SELECT * FROM q WHERE v = 1").await;
    // Past any poll the creation left behind, under either version.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let base = fds_named(dir.path(), "q");
    assert!(base > 0, "the table holds no descriptors");
    for sql in [
        "CREATE INDEX q_v0 ON q (v)",
        "ALTER TABLE q ADD COLUMN c1 BIGINT",
        "ALTER TABLE q RENAME COLUMN s TO s2",
        "ALTER TABLE q ALTER COLUMN v SET DEFAULT 5",
        "ALTER TABLE q ADD COLUMN c4 BIGINT",
        "ALTER TABLE q DROP COLUMN c1",
        "DROP INDEX q_v0",
        "CREATE INDEX q_v7 ON q (v)",
        "ALTER TABLE q RENAME COLUMN s2 TO s",
        "ALTER TABLE q ADD COLUMN c9 BIGINT",
    ] {
        exec(&db, sql).await;
        exec(&db, "SELECT * FROM q WHERE v = 1").await;
        let now = count_returns_to(|| fds_named(dir.path(), "q"), base, Duration::from_secs(1)).await;
        assert_eq!(
            now,
            base,
            "a second after {sql} the table held {now} descriptors, {base} before: {:?}",
            fds_under(dir.path())
        );
    }
    let db = restart(dir.path(), db).await;
    exec(&db, "SELECT * FROM q WHERE v = 1").await;
    let now = count_returns_to(|| fds_named(dir.path(), "q"), base, Duration::from_secs(1)).await;
    assert_eq!(now, base, "after a restart the table held {now} descriptors, {base} before");
    close(dir.path(), db);
}

/// A partitioned table's schema changes leave its open partitions' and its
/// shell's descriptors where they were (P5b-3, the same check as the plain
/// table's): a partition opens `External`, with no service task of its own,
/// and a metadata change reloads it in place. A pin, not a fix: this held
/// before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_changes_leave_a_partitioned_tables_descriptors_alone() {
    let dir = tempfile::tempdir().unwrap();
    super::budget::isolate_with(dir.path(), 100_000, Some(0));
    let db = open(dir.path()).await;
    populated(&db).await;
    exec(&db, "SELECT * FROM pt").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let opened = open_pids(&pt(&db));
    let base = fds_under(dir.path()).len();
    for sql in [
        "CREATE INDEX pt_v0 ON pt (v)",
        "ALTER TABLE pt RENAME COLUMN v TO v2",
        "ALTER TABLE pt ALTER COLUMN v2 SET DEFAULT 5",
        "ALTER TABLE pt ADD COLUMN c3 BIGINT DEFAULT NULL",
        "DROP INDEX pt_v0",
        "ALTER TABLE pt RENAME COLUMN v2 TO v",
    ] {
        // A change this engine refuses on a partitioned table changes
        // nothing either; the count is what is checked.
        let _ = try_exec(&db, sql).await;
        exec(&db, "SELECT * FROM pt").await;
        let now = count_returns_to(|| fds_under(dir.path()).len(), base, Duration::from_secs(1)).await;
        assert_eq!(now, base, "a second after {sql} {now} descriptors were held, {base} before");
        assert_eq!(open_pids(&pt(&db)), opened, "{sql} changed which partitions are open");
    }
    close(dir.path(), db);
}
