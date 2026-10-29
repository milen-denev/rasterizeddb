//! Forget pins (docs/partitioning.md §7.2 point 1, §16 I13) and shared
//! maintenance (§7.3), through `Database` and SQL.
//!
//! The trap these pin: below the database's forgotten horizon every id
//! reads as committed, so a partition whose records still carry an aborted
//! transaction's stamp must hold that horizon down until it is swept, or the
//! aborted rows come back. Since phase 3 P5a nothing but the slot's pin does
//! (a partition takes no place in the forget registry), so each test
//! sweeps every other table, forces forgetting as far as it will go, and
//! asserts both that the horizon stopped below the aborted id and that the
//! aborted row stays invisible; the last step sweeps the pinned partition
//! too and asserts the horizon then passes, so the stop was the pin's.

use std::path::Path;
use std::time::Duration;

use rclite::Arc;

use crate::core::database::{Database, Relation};
use crate::core::mvcc::transaction::TxId;
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::storage_providers::traits::StorageIO;

const CREATE: &str =
    "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4";

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&path_str(dir), false, "pins").await.unwrap())
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

async fn exec_in(db: &Arc<Database>, tx: TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
}

/// The ids `SELECT id FROM pt WHERE <clause>` returns, sorted.
async fn ids(db: &Arc<Database>, clause: &str) -> Vec<i64> {
    let out = exec(db, &format!("SELECT id FROM pt WHERE {clause}")).await;
    assert_eq!(out[0], 2, "not a row set");
    let mut v: Vec<i64> = crate::core::row::row::vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
        .collect();
    v.sort();
    v
}

fn pt(db: &Database) -> rclite::Arc<PartitionedTable> {
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

fn materialized(t: &PartitionedTable) -> Vec<u32> {
    let epoch = t.epoch().unwrap();
    t.materialized(&epoch, &Route::All).unwrap().into_iter().map(|(pid, _)| pid).collect()
}

/// Ends enough transactions for the settle bound to move well past every
/// id so far, and settles.
fn advance(db: &Database) {
    for _ in 0..9_000 {
        let tx = db.transactions.begin();
        db.transactions.commit(tx);
    }
    db.transactions.settle_now();
}

/// Sweeps every plain table (the catalogue's included), so none of them
/// holds the horizon.
async fn sweep_plain(db: &Database) {
    for name in db.table_names() {
        if let Some(t) = db.table_for_tests(&name)
            && !t.is_shell()
        {
            t.neutralize_aborted_now().await.unwrap();
        }
    }
}

/// Sweeps every materialized partition of `pt` but `skip`, then forgets as
/// far as the registry allows. Returns the forgotten horizon.
async fn forget_except(db: &Database, skip: &[u32]) -> TxId {
    advance(db);
    sweep_plain(db).await;
    let t = pt(db);
    let board = t.pin_board().unwrap();
    for pid in materialized(&t) {
        if !skip.contains(&pid) {
            board.sweep(pid).await.unwrap();
        }
    }
    db.transactions.try_forget()
}

/// Committed rows in tenants 1 to 8, then one row of tenant 1 inserted
/// under a transaction that aborts: its id.
async fn populated_with_an_abort(db: &Arc<Database>) -> TxId {
    exec(db, CREATE).await;
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let x = db.transactions.begin();
    exec_in(db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 999, 'aborted')").await;
    db.abort_transaction(x);
    assert_eq!(ids(db, "tenant_id = 1").await, vec![1]);
    x
}

/// I13, in one process: a partition whose aborted stamps are unswept holds
/// the forgotten horizon below them however far every other table forgets,
/// and its aborted row stays invisible; swept, it lets go.
///
/// Mutation: `PinBoard::materialized` not lowering the table's handle (a
/// new partition's pin left uncovered): the horizon passes the aborted id
/// with the partition unswept, and the row comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unswept_partition_pins_forgetting_below_its_aborted_rows() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    let x = populated_with_an_abort(&db).await;
    let p = pid_of_tenant(&pt(&db), 1);
    // Before any partition is swept: the handle covers every materialized
    // partition's pin from its MATERIALIZE on.
    advance(&db);
    sweep_plain(&db).await;
    let forgotten = db.transactions.try_forget();
    assert!(forgotten <= x, "with no partition swept, forgotten below {forgotten}, past aborted {x}");

    for round in 0..3 {
        let forgotten = forget_except(&db, &[p]).await;
        assert!(forgotten <= x, "round {round}: forgotten below {forgotten}, past aborted {x}, with its partition unswept");
        assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "round {round}: the aborted row came back");
        assert!(pt(&db).pin_board().unwrap().handle_horizon() <= x);
    }
    // Swept too: nothing pins, and the horizon passes it.
    let forgotten = forget_except(&db, &[]).await;
    assert!(forgotten > x, "with every table swept, forgetting stopped at {forgotten}, below {x}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back once forgotten");
    assert_eq!(ids(&db, "id > 0").await, (1..=8).collect::<Vec<_>>());
    close(&dir, db);
}

/// I13 across a restart: the pins are the map log's, replayed. A partition
/// whose last pin record is its `MATERIALIZE` still pins after the restart,
/// whatever the other partitions' `SWEPT`s and the plain tables let go.
///
/// Mutation: `PinBoard::new` setting the handle to `TxId::MAX` ("every
/// partition clean on restart"): the restarted database forgets past the
/// aborted id and the row comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pins_survive_a_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    let x = populated_with_an_abort(&db).await;
    let p = pid_of_tenant(&pt(&db), 1);
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= x);
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = open(&dir).await;
    let t = pt(&db);
    assert!(t.pin_of(p).await.unwrap().is_some_and(|pin| pin <= x), "partition {p} lost its pin in the restart");
    for other in materialized(&t).into_iter().filter(|&q| q != p) {
        // The others' `SWEPT`s replayed: each pins at its sweep's bound, or
        // nothing when it was clean, never below where it was.
        assert!(t.pin_of(other).await.unwrap().is_none_or(|pin| pin > x), "partition {other} pins at or below {x}");
    }
    drop(t);
    for round in 0..2 {
        let forgotten = forget_except(&db, &[p]).await;
        assert!(forgotten <= x, "after the restart, round {round}: forgotten below {forgotten}, past {x}");
        assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "after the restart, the aborted row came back");
    }
    let forgotten = forget_except(&db, &[]).await;
    assert!(forgotten > x, "after the restart, with every table swept, forgetting stopped at {forgotten}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1]);
    close(&dir, db);
}

/// The first-write edge (§7.2): a partition swept clean pins nothing; its
/// next write, by a transaction that then aborts, pins it again before the
/// stamp, durably (the pin survives a restart), and the aborted row stays
/// invisible however far the rest forgets.
///
/// Mutation: `PinBoard::first_write` only setting the slot pinned, no
/// `DIRTY` and no lowered handle: forgetting passes the aborted id and the
/// row comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_write_after_a_clean_sweep_pins_again() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, CREATE).await;
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    forget_except(&db, &[]).await;
    assert_eq!(t.pin_of(p).await.unwrap(), None, "a partition of committed rows only did not sweep clean");
    assert!(!t.pin_board().unwrap().watch(p).unwrap().is_pinned());
    assert_eq!(t.pin_board().unwrap().handle_horizon(), TxId::MAX, "every partition is clean, yet the table pins");

    let y = db.transactions.begin();
    exec_in(&db, y, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 777, 'aborted')").await;
    let pin = t.pin_of(p).await.unwrap().expect("the first write after a clean sweep left the partition unpinned");
    assert!(pin <= y, "pinned at {pin}, above the writer {y}");
    assert!(t.pin_board().unwrap().handle_horizon() <= pin);
    db.abort_transaction(y);
    drop(t);
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= y, "forgotten below {forgotten}, past the aborted {y}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back");
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = open(&dir).await;
    assert!(pt(&db).pin_of(p).await.unwrap().is_some_and(|pin| pin <= y), "the DIRTY edge did not survive a restart");
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= y, "after the restart: forgotten below {forgotten}, past {y}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "after the restart, the aborted row came back");
    let forgotten = forget_except(&db, &[]).await;
    assert!(forgotten > y);
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1]);
    close(&dir, db);
}

/// A sweep is clean only if no writer was inside or entered while it ran
/// (the write watch's proof): a writer in flight leaves the pin at the
/// sweep's bound, and the next sweep, with nobody inside, clears it.
///
/// Mutation: the clean test in `PinBoard::sweep` not requiring
/// `active0 == 0`: the first sweep clears the pin under the writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_with_a_writer_inside_is_not_clean() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 'c')").await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    let board = t.pin_board().unwrap().clone();
    let watch = board.watch(p).unwrap().clone();
    advance(&db);
    {
        let _inside = watch.enter().await.unwrap();
        let r = board.sweep(p).await.unwrap();
        let bound = r.advanced_to.expect("the sweep did not advance");
        assert_eq!(t.pin_of(p).await.unwrap(), Some(bound), "a sweep with a writer inside cleared the pin");
        assert!(watch.is_pinned());
    }
    advance(&db);
    let r = board.sweep(p).await.unwrap();
    assert!(r.advanced_to.is_some());
    assert_eq!(t.pin_of(p).await.unwrap(), None, "a sweep with nobody inside left the pin");
    drop((t, board, watch));
    close(&dir, db);
}

/// Shared maintenance (§7.3): an `External` partition runs no task of its
/// own. Reopened with 64 materialized partitions, the database has fewer
/// tasks than partitions once the open's one-shot work ends (before phase 3
/// P5a, about two per partition).
///
/// Mutation: `open_partition` spawning the providers' service for an
/// `External` partition too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_partitions_spawn_no_tasks_of_their_own() {
    const N: u32 = 64;
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    {
        let db = open(&dir).await;
        exec(&db, &format!("CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS {N}")).await;
        let t = pt(&db);
        let mut tenant = 0i64;
        while (materialized(&t).len() as u32) < N {
            exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, {tenant}, 'c')")).await;
            tenant += 1;
        }
        db.sync_durable().unwrap();
        drop(t);
        close(&dir, db);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let tasks = || tokio::runtime::Handle::current().metrics().num_alive_tasks();
    let before = tasks();
    let db = open(&dir).await;
    assert_eq!(materialized(&pt(&db)).len() as u32, N);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let added = tasks().saturating_sub(before);
    assert!(added < (N / 2) as usize, "{added} tasks for {N} open partitions and the plain tables");
    close(&dir, db);
}

/// The scheduler services what its partitions' writes push: after writes,
/// its rounds run the idle checkpoint that each partition's own watcher
/// used to (the file set's word ends `Clean`), and the units then leave the
/// round until the next write.
///
/// Mutation: no idle sealer built for an `External` partition: the word
/// stays `Dirty`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scheduler_runs_the_idle_checkpoint_and_then_rests() {
    use crate::core::index_store::gate_word::GateState;
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    let _idle = crate::core::row::table::table_seal::idle_override::set(&dir, Duration::from_millis(50));
    let db = open(&dir).await;
    // Rounds only when this test drives them.
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')").await;
    let t = pt(&db);
    let epoch = t.epoch().unwrap();
    let parts = t.materialized(&epoch, &Route::All).unwrap();
    assert!(!parts.is_empty());
    let sched = db.partition_maintenance().clone();
    assert!(sched.queued_units().await > 0, "the writes pushed nothing");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        sched.run_round().await;
        let all_clean = parts.iter().all(|(_, p)| p.gate().word().load().state() == GateState::Clean);
        if all_clean && sched.queued_units().await == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "after {} rounds: words {:?}, {} units still in the round",
            sched.rounds(),
            parts.iter().map(|(pid, p)| (*pid, p.gate().word().load().state())).collect::<Vec<_>>(),
            sched.queued_units().await
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    // A write pushes its partition again.
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 4, 'd')").await;
    assert!(sched.queued_units().await > 0, "a write after the rest pushed nothing");
    drop((t, epoch, parts));
    close(&dir, db);
}

/// Each materialized partition's pin, by pid.
async fn pins(t: &PartitionedTable) -> Vec<(u32, Option<TxId>)> {
    let mut out = Vec::new();
    for pid in materialized(t) {
        out.push((pid, t.pin_of(pid).await.unwrap()));
    }
    out
}

fn map_generations(dir: &Path, uid: u64) -> Vec<u64> {
    let mut gens: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|n| match crate::core::partition::files::parse_name(&n)? {
            crate::core::partition::files::PartFile::Map { uid: u, generation } if u == uid && !n.ends_with(".tmp") => {
                Some(generation)
            }
            _ => None,
        })
        .collect();
    gens.sort();
    gens
}

/// Map-log generations (§3.2): a compaction writes the state into the next
/// generation, seals it and deletes the old one; records appended after it
/// land in the new generation; a restart opens the highest sealed one with
/// every pin as it was, so an aborted row the old generation pinned stays
/// pinned (and invisible) through two compactions and a restart.
///
/// Mutation: the compaction leaving out the records captured since its
/// clone: its replay check refuses the new generation, and `compact` errs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_keeps_every_record_and_the_pins_through_a_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    let x = populated_with_an_abort(&db).await;
    let t = pt(&db);
    let uid = t.uid();
    let p = pid_of_tenant(&t, 1);
    forget_except(&db, &[p]).await;
    let board = t.pin_board().unwrap().clone();
    assert_eq!(board.generation(), 0);

    // Writes while the compaction is held between its snapshot and its
    // switch: the other partitions are clean, so each one's first write
    // appends a DIRTY to the old generation after the clone, which the new
    // one must take from the capture (left out, those partitions would
    // reopen clean under unswept stamps).
    let others: Vec<u32> = materialized(&t).into_iter().filter(|&q| q != p).collect();
    for q in &others {
        assert_eq!(t.pin_of(*q).await.unwrap(), None, "partition {q} did not sweep clean");
    }
    let (reached, resume) = super::pins::test_hooks::arm(&dir);
    let compacting = {
        let board = board.clone();
        tokio::spawn(async move { board.compact().await })
    };
    reached.notified().await;
    for tenant in 100..140i64 {
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, {tenant}, 'w')")).await;
    }
    for q in &others {
        assert!(t.pin_of(*q).await.unwrap().is_some(), "partition {q} took a write and stayed clean");
    }
    resume.notify_one();
    assert!(compacting.await.unwrap().unwrap(), "the first compaction did not run");
    assert_eq!(board.generation(), 1);
    assert_eq!(map_generations(&dir, uid), vec![1], "the old generation was not deleted");
    let replay = board.log.replay().await.unwrap();
    assert_eq!(replay.state.snapshot(), board.state.lock().await.snapshot(), "generation 1 does not replay to the state");
    assert!(replay.state.sealed());

    forget_except(&db, &[p]).await;
    assert!(board.compact().await.unwrap());
    assert_eq!(board.generation(), 2);
    assert_eq!(map_generations(&dir, uid), vec![2]);
    let rows = ids(&db, "id > 0").await;
    let pins_now = pins(&t).await;
    assert!(pins_now.iter().find(|(q, _)| *q == p).is_some_and(|(_, pin)| pin.is_some_and(|v| v <= x)), "the unswept partition's pin moved");
    drop((t, board));
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = open(&dir).await;
    let t = pt(&db);
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(t.pin_board().unwrap().generation(), 2);
    assert_eq!(pins(&t).await, pins_now, "the pins changed across the restart");
    assert_eq!(ids(&db, "id > 0").await, rows);
    drop(t);
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= x, "after compactions and a restart, forgotten below {forgotten}, past {x}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back");
    // Appends after the restart go to generation 2, and a third compaction
    // follows from there.
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (500, 500, 'late')").await;
    let board = pt(&db).pin_board().unwrap().clone();
    assert!(board.compact().await.unwrap());
    assert_eq!(map_generations(&dir, uid), vec![3]);
    drop(board);
    close(&dir, db);
}

/// An unfinished compaction (a higher generation with no `SEALED`) is never
/// opened: the generation before it stays live, and the reaper deletes the
/// leftover at the next start. A sealed one with its old generation still
/// there (a crash before the delete) opens, and the old one is reaped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unfinished_compaction_is_ignored_and_reaped() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    populated_with_an_abort(&db).await;
    let t = pt(&db);
    let uid = t.uid();
    let rows = ids(&db, "id > 0").await;
    let want = pins(&t).await;
    // A generation 1 holding the opening records only, as a compaction that
    // died before `SEALED` leaves it.
    {
        let board = t.pin_board().unwrap();
        let state = board.state.lock().await.clone();
        crate::core::partition::files::create_map_generation(&dir, uid, 1).unwrap();
        let io = std::sync::Arc::new(db.db_io_pointers.create_new(crate::core::partition::files::map_file(uid, 1)).await);
        let log = crate::core::partition::files::MapLogFile::create(io).await.unwrap();
        log.append(&crate::core::partition::log::Record::Header(state.header().clone())).await.unwrap();
        log.append(&crate::core::partition::log::Record::Locations(state.locations().to_vec())).await.unwrap();
        log.append_durable(&crate::core::partition::log::Record::Snapshot(state.snapshot())).await.unwrap();
    }
    drop(t);
    db.sync_durable().unwrap();
    close(&dir, db);
    let db = open(&dir).await;
    let t = pt(&db);
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(t.pin_board().unwrap().generation(), 0, "an unsealed generation was opened");
    assert_eq!(map_generations(&dir, uid), vec![0], "the unfinished generation was not reaped");
    assert_eq!(ids(&db, "id > 0").await, rows);
    assert_eq!(pins(&t).await, want);
    // A sealed generation 1 beside generation 0: copy 0 back in after a
    // compaction, as a crash between the switch and the delete leaves it.
    let board = t.pin_board().unwrap().clone();
    let zero = std::fs::read(dir.join(crate::core::partition::files::map_file(uid, 0))).unwrap();
    assert!(board.compact().await.unwrap());
    drop((t, board));
    std::fs::write(dir.join(crate::core::partition::files::map_file(uid, 0)), zero).unwrap();
    db.sync_durable().unwrap();
    close(&dir, db);
    let db = open(&dir).await;
    let t = pt(&db);
    assert_eq!(t.pin_board().unwrap().generation(), 1, "the sealed generation was not chosen");
    assert_eq!(map_generations(&dir, uid), vec![1], "the old generation was not reaped");
    assert_eq!(ids(&db, "id > 0").await, rows);
    assert_eq!(pins(&t).await, want);
    drop(t);
    close(&dir, db);
}

/// `SERIAL` high-water marks persisted in `__pt_<uid>_seq.db` (§10): after
/// a restart the first insert resumes from the reservation, seeding nothing
/// from the partitions (no scan); values never repeat across restarts, and
/// a restart leaves a gap of at most one step.
///
/// Mutations: `SerialSource::resume` always declining (the scan comes
/// back); `cover` never raising the reservation (values repeat after a
/// restart).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_high_water_survives_restarts_without_a_scan() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, "CREATE TABLE ser (tenant_id BIGINT NOT NULL, a SERIAL, b BIGSERIAL, body TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 8").await;
    let ser = |db: &Database| match db.relation("ser") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("ser is not partitioned"),
    };
    let values = |out: Vec<u8>| -> Vec<(i32, i64)> {
        let mut v: Vec<(i32, i64)> = crate::core::row::row::vec_into_rows(&out[1..])
            .unwrap()
            .iter()
            .map(|r| {
                (
                    i32::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()),
                    i64::from_le_bytes(r.columns[1].data.into_slice().try_into().unwrap()),
                )
            })
            .collect();
        v.sort();
        v
    };
    for tenant in 1..=5i64 {
        exec(&db, &format!("INSERT INTO ser (tenant_id, body) VALUES ({tenant}, 'x')")).await;
    }
    let t = ser(&db);
    assert!(t.serial_reserved("ser_a_seq").is_some_and(|r| r >= 5), "{:?}", t.serial_reserved("ser_a_seq"));
    drop(t);
    db.sync_durable().unwrap();
    close(&dir, db);
    let mut last = 5i64;
    for round in 0..3 {
        let db = open(&dir).await;
        let reserved = ser(&db).serial_reserved("ser_a_seq").expect("no reservation after a restart");
        assert!(reserved >= last, "round {round}: reserved {reserved} below the last value {last}");
        for tenant in 0..3i64 {
            exec(&db, &format!("INSERT INTO ser (tenant_id, body) VALUES ({}, 'y')", 10 + tenant)).await;
        }
        assert_eq!(ser(&db).serial_max_scans(), 0, "round {round}: the first insert after a restart scanned the partitions");
        let got = values(exec(&db, "SELECT a, b FROM ser").await);
        let a: Vec<i64> = got.iter().map(|(a, _)| *a as i64).collect();
        let mut dedup = a.clone();
        dedup.dedup();
        assert_eq!(dedup, a, "round {round}: a SERIAL value repeated: {a:?}");
        let b: Vec<i64> = { let mut b: Vec<i64> = got.iter().map(|(_, b)| *b).collect(); b.sort(); b };
        let mut bd = b.clone();
        bd.dedup();
        assert_eq!(bd, b, "round {round}: a BIGSERIAL value repeated: {b:?}");
        let newest = *a.last().unwrap();
        assert!(newest > last, "round {round}: no value above {last}");
        assert!(newest <= last + super::serial::STEP + 3, "round {round}: a gap wider than one step ({last} to {newest})");
        last = newest;
        db.sync_durable().unwrap();
        close(&dir, db);
    }
}

/// A sweep that finds a stamp at or above its bound is not clean, even with
/// no writer inside: a transaction still running wrote it, and may abort.
/// When it does, the pin the sweep left keeps its row hidden.
///
/// Mutation: `neutralize_aborted_from` not counting stamps at or above the
/// bound (`newer` always 0): the sweep clears the pin under the running
/// writer's stamp, forgetting passes it once it aborts, and the row comes
/// back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_that_sees_a_running_writers_stamp_is_not_clean() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, CREATE).await;
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    forget_except(&db, &[]).await;
    assert_eq!(t.pin_of(p).await.unwrap(), None);
    let w = db.transactions.begin();
    exec_in(&db, w, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 555, 'running')").await;
    advance(&db);
    let r = t.pin_board().unwrap().sweep(p).await.unwrap();
    let bound = r.advanced_to.expect("the sweep did not advance");
    assert!(bound <= w, "the bound {bound} passed the running writer {w}");
    assert!(r.newer > 0, "the sweep saw no stamp at or above its bound: {r:?}");
    assert_eq!(t.pin_of(p).await.unwrap(), Some(bound), "a sweep over a running writer's stamp was clean");
    db.abort_transaction(w);
    drop(t);
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= w, "forgotten below {forgotten}, past the aborted {w}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back");
    let forgotten = forget_except(&db, &[]).await;
    assert!(forgotten > w);
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1]);
    close(&dir, db);
}

/// A table that cannot open (a partition's file missing, I9) has unknown
/// pins: it holds the forgotten horizon where it was for as long as it stays
/// broken, so once the file is back its aborted rows are still hidden.
///
/// Mutation: no `broken_pin` for a table that failed to open: the broken
/// table pins nothing, forgetting passes the aborted id, and after the
/// repair the row comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broken_table_pins_until_it_opens() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    let x = populated_with_an_abort(&db).await;
    let t = pt(&db);
    let (uid, p) = (t.uid(), pid_of_tenant(&t, 1));
    let other = materialized(&t).into_iter().find(|&q| q != p).expect("a second partition");
    drop(t);
    db.sync_durable().unwrap();
    close(&dir, db);

    let victim = dir.join(&crate::core::partition::files::data_files(uid, other)[0]);
    let aside = base.path().join("aside");
    std::fs::rename(&victim, &aside).unwrap();
    let db = open(&dir).await;
    assert!(pt(&db).broken().is_some(), "the table opened without a partition's file");
    assert_eq!(db.partition_maintenance().broken_pins(), vec!["pt".to_string()], "the broken table's hold is not named");
    advance(&db);
    sweep_plain(&db).await;
    let forgotten = db.transactions.try_forget();
    assert!(forgotten <= x, "a broken table let forgetting pass the aborted {x} (forgotten below {forgotten})");
    db.sync_durable().unwrap();
    close(&dir, db);

    std::fs::rename(&aside, &victim).unwrap();
    let db = open(&dir).await;
    assert!(pt(&db).broken().is_none(), "{:?}", pt(&db).broken());
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back after the repair");
    close(&dir, db);
}

// ---------------------------------------------------------------------------
// Round 2: supervision, the queue, SERIAL renames and clean shutdowns.
// ---------------------------------------------------------------------------

/// The materialized partition holding `tenant`, as its `Table`.
fn part_of(t: &PartitionedTable, tenant: i64) -> rclite::Arc<crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>> {
    let pid = pid_of_tenant(t, tenant);
    let epoch = t.epoch().unwrap();
    t.materialized(&epoch, &Route::All).unwrap().into_iter().find(|(p, _)| *p == pid).expect("materialized").1
}

/// Two tenants in two different partitions.
fn two_tenants(t: &PartitionedTable) -> (i64, i64) {
    let a = 1;
    let b = (2..100).find(|&b| pid_of_tenant(t, b) != pid_of_tenant(t, a)).unwrap();
    (a, b)
}

/// A panic in one partition's maintenance (its vacuum tick, or the service
/// around it) is caught and counted, and the scheduler's task goes on: a
/// later DELETE in another partition is still swept.
///
/// Mutations: the vacuum tick's catch in `ExternalUnit::service_once`
/// removed (the tick's panic is then the scheduler's, `stats.panics` stays
/// 0); the scheduler's per-unit catch removed (the injected service panic
/// ends the unit round, and the task with it, so rounds stop).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_partition_never_stops_the_scheduler() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    crate::core::row::table::table_maintenance::test_hooks::set_interval(&dir, Duration::from_millis(1));
    let db = open(&dir).await;
    let sched = db.partition_maintenance().clone();
    sched.set_period(Duration::from_millis(20));
    exec(&db, CREATE).await;
    let t = pt(&db);
    let (a, b) = two_tenants(&t);
    for id in 0..80i64 {
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({a}, {id}, 'a'), ({b}, {id}, 'b')")).await;
    }
    let (pa, pb) = (part_of(&t, a), part_of(&t, b));
    pa.vacuum_stats().unwrap().inject_panics.store(1, std::sync::atomic::Ordering::Release);
    crate::core::row::table::table_maintenance::test_hooks::panic_once(&dir);
    exec(&db, &format!("UPDATE pt SET v = 'x' WHERE tenant_id = {a}")).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while pa.vacuum_stats().unwrap().panics.load(std::sync::atomic::Ordering::Acquire) == 0
        || sched.panics() == 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the panics were never caught: tick {} service {}",
            pa.vacuum_stats().unwrap().panics.load(std::sync::atomic::Ordering::Acquire),
            sched.panics()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rounds = sched.rounds();
    let runs = pb.vacuum_stats().unwrap().runs.load(std::sync::atomic::Ordering::Acquire);
    exec(&db, &format!("DELETE FROM pt WHERE tenant_id = {b}")).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while pb.vacuum_stats().unwrap().runs.load(std::sync::atomic::Ordering::Acquire) == runs {
        assert!(
            std::time::Instant::now() < deadline,
            "after the panics the other partition's DELETE was never swept ({} rounds since, {} panics)",
            sched.rounds() - rounds,
            sched.panics()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(sched.rounds() > rounds, "the scheduler stopped running rounds");
    drop((t, pa, pb));
    close(&dir, db);
}

/// A write that a round services in its middle, and lets go as settled,
/// pushes its partition again when it ends: what it changed (its mutation
/// count, bumped at the statement's end) is news.
///
/// Mutation: no push when the write guard drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_a_round_serviced_midway_is_pushed_again_at_its_end() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    // Idle checkpoints at once, so no pending seal keeps the unit in the
    // round and hides a dropped push.
    let _idle = crate::core::row::table::table_seal::idle_override::set(&dir, Duration::from_millis(0));
    crate::core::row::table::table_maintenance::test_hooks::set_interval(&dir, Duration::from_millis(1));
    let db = open(&dir).await;
    let sched = db.partition_maintenance().clone();
    sched.set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 'a')").await;
    let t = pt(&db);
    let watch = t.pin_board().unwrap().watch(pid_of_tenant(&t, 1)).unwrap().clone();
    let settle = |sched: StdArcScheduler| async move {
        for _ in 0..200 {
            sched.run_round().await;
            if sched.queued_units().await == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the units never settled");
    };
    settle(sched.clone()).await;
    // A statement in flight: entered (and pushed), serviced and let go
    // while it runs, then done.
    let inside = watch.enter().await.unwrap();
    sched.run_round().await;
    assert_eq!(sched.queued_units().await, 0, "the round kept a settled unit");
    drop(inside);
    assert!(sched.queued_units().await > 0, "the write's end pushed nothing");
    drop((t, watch));
    close(&dir, db);
}

type StdArcScheduler = std::sync::Arc<crate::core::partition::maintenance::Scheduler>;

/// A partition with a few UPDATEs, below the vacuum's sweep threshold,
/// settles and leaves the round instead of being serviced every round.
///
/// Mutation: `VacuumService::settled` without its below-threshold clause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_below_the_sweep_threshold_settles() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    let _idle = crate::core::row::table::table_seal::idle_override::set(&dir, Duration::from_millis(0));
    crate::core::row::table::table_maintenance::test_hooks::set_interval(&dir, Duration::from_millis(1));
    let db = open(&dir).await;
    let sched = db.partition_maintenance().clone();
    sched.set_period(Duration::from_secs(3600));
    exec(&db, CREATE).await;
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c')").await;
    let settle = |what: &'static str| {
        let sched = sched.clone();
        async move {
            for _ in 0..100 {
                sched.run_round().await;
                if sched.queued_units().await == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("{what}");
        }
    };
    // The first tick scans whatever is there; settled after it.
    settle("the freshly written partition never settled").await;
    // One dead stamp, far below the threshold: the next tick is not due,
    // and the partition must settle anyway.
    exec(&db, "UPDATE pt SET v = 'x' WHERE tenant_id = 1 AND id = 2").await;
    settle("a partition below the sweep threshold never settled").await;
    close(&dir, db);
}

/// A writer that enters after the sweep armed, stamps behind its scan and
/// leaves before it ends: the sweep's second look at `seq` keeps it from
/// being clean, so the stamp stays pinned and, once its transaction aborts,
/// hidden.
///
/// Mutation: the sweep's clean test not re-reading `seq` (`clean = true`
/// once `pinned` is cleared).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_behind_the_scan_keeps_the_sweep_unclean() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, CREATE).await;
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    advance(&db);
    let board = t.pin_board().unwrap().clone();
    let (reached, resume) = crate::core::vacuum::neutralize_test_hooks::arm(&dir);
    let sweep = {
        let board = board.clone();
        tokio::spawn(async move { board.sweep(p).await })
    };
    reached.notified().await;
    let w = db.transactions.begin();
    exec_in(&db, w, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 444, 'behind')").await;
    resume.notify_one();
    let report = sweep.await.unwrap().unwrap();
    let bound = report.advanced_to.expect("the sweep did not advance");
    assert_eq!(report.newer, 0, "the writer was not behind the scan: {report:?}");
    assert_eq!(t.pin_of(p).await.unwrap(), Some(bound), "a sweep a writer entered meanwhile was clean");
    db.abort_transaction(w);
    drop((t, board));
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= w, "forgotten below {forgotten}, past the aborted {w}");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back");
    close(&dir, db);
}

/// Startup holds forgetting off from the plain tables' registration until
/// the partitioned tables have registered: a plain table's vacuum forgetting
/// in that window would pass an aborted stamp a partition still holds.
///
/// Mutation: the startup `forget_pause` dropped before the partitioned
/// tables open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forgetting_waits_for_the_partitioned_tables_at_startup() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    let x = populated_with_an_abort(&db).await;
    exec(&db, "CREATE TABLE plain (id BIGINT NOT NULL, v TEXT)").await;
    exec(&db, "INSERT INTO plain (id, v) VALUES (1, 'p')").await;
    advance(&db);
    db.transactions.status().flush_all().unwrap();
    db.sync_durable().unwrap();
    close(&dir, db);

    crate::core::database::partitioned::startup_hooks::forget_before_partitioned_open(&dir);
    let db = open(&dir).await;
    let forgotten = db.transactions.status().forgotten_below();
    assert!(forgotten <= x, "startup forgot below {forgotten}, past the aborted {x}, before the partitions registered");
    assert_eq!(ids(&db, "tenant_id = 1").await, vec![1], "the aborted row came back");
    close(&dir, db);
}

async fn serial_ids(db: &Arc<Database>, table: &str) -> Vec<i64> {
    let out = exec(db, &format!("SELECT id FROM {table}")).await;
    let mut v: Vec<i64> = crate::core::row::row::vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
        .collect();
    v.sort();
    v
}

fn assert_distinct(v: &[i64], what: &str) {
    let mut d = v.to_vec();
    d.dedup();
    assert_eq!(d.len(), v.len(), "{what}: a SERIAL value repeated: {v:?}");
}

/// A `SERIAL` whose table is renamed away and back: the entry the first
/// name left would be stale (the counter ran on under the second name), so
/// it is forgotten at the rename, and the restart, clean or not, never
/// resumes below a value handed out. Also renamed away only.
///
/// Mutation: `SerialSource::retain_read` keeping every entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_serial_renamed_away_and_back_never_repeats_a_value() {
    for (back, clean) in [(true, true), (true, false), (false, true), (false, false)] {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("db");
        let db = open(&dir).await;
        exec(&db, "CREATE TABLE sa (tenant_id BIGINT NOT NULL, id BIGSERIAL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4").await;
        exec(&db, "INSERT INTO sa (tenant_id, v) VALUES (1, 'a'), (2, 'a'), (3, 'a')").await;
        exec(&db, "ALTER TABLE sa RENAME TO sb").await;
        let rows: Vec<String> = (0..1200).map(|i| format!("({}, 'b')", i % 7)).collect();
        exec(&db, &format!("INSERT INTO sb (tenant_id, v) VALUES {}", rows.join(", "))).await;
        let name = if back {
            exec(&db, "ALTER TABLE sb RENAME TO sa").await;
            "sa"
        } else {
            "sb"
        };
        if clean {
            db.record_clean_shutdown().await;
        }
        db.sync_durable().unwrap();
        close(&dir, db);
        let db = open(&dir).await;
        exec(&db, &format!("INSERT INTO {name} (tenant_id, v) VALUES (5, 'after'), (6, 'after')")).await;
        let got = serial_ids(&db, name).await;
        assert_eq!(got.len(), 1205);
        assert_distinct(&got, &format!("renamed {}, {} restart", if back { "away and back" } else { "away" }, if clean { "clean" } else { "crash-like" }));
        close(&dir, db);
    }
}

/// A clean shutdown records the exact `SERIAL` high-water: the restart
/// resumes with no gap, as PostgreSQL's sequences do. (A restart without it,
/// as after a crash, skips at most a step:
/// `serial_high_water_survives_restarts_without_a_scan`.)
///
/// Mutation: `record_exact` writing nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_shutdown_leaves_no_serial_gap() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, "CREATE TABLE sc (tenant_id BIGINT NOT NULL, id BIGSERIAL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4").await;
    exec(&db, "INSERT INTO sc (tenant_id, v) VALUES (1, 'a'), (2, 'a'), (3, 'a')").await;
    db.record_clean_shutdown().await;
    close(&dir, db);
    let db = open(&dir).await;
    exec(&db, "INSERT INTO sc (tenant_id, v) VALUES (4, 'b')").await;
    db.record_clean_shutdown().await;
    close(&dir, db);
    let db = open(&dir).await;
    exec(&db, "INSERT INTO sc (tenant_id, v) VALUES (5, 'c')").await;
    assert_eq!(serial_ids(&db, "sc").await, vec![1, 2, 3, 4, 5], "a clean restart left a gap");
    assert_eq!(match db.relation("sc") { Some(Relation::Partitioned(t)) => t.serial_max_scans(), _ => 99 }, 0);
    close(&dir, db);
}

/// A compaction picks a generation number no file has: a leftover the
/// reaper keeps (one that does not replay) is never written over or
/// deleted.
///
/// Mutation: `next_map_generation` ignoring the files (live + 1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_never_reuses_a_generation_number() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    populated_with_an_abort(&db).await;
    let t = pt(&db);
    let uid = t.uid();
    let leftover = dir.join(crate::core::partition::files::map_file(uid, 1));
    std::fs::write(&leftover, b"not a map log").unwrap();
    let board = t.pin_board().unwrap().clone();
    assert!(board.compact().await.unwrap());
    assert_eq!(board.generation(), 2, "the compaction did not step over the leftover");
    assert_eq!(std::fs::read(&leftover).unwrap(), b"not a map log", "the leftover was touched");
    assert!(board.compact().await.unwrap());
    assert_eq!(board.generation(), 3);
    drop((t, board));
    close(&dir, db);
}

/// A unit stuck in its service holds up neither the other units of its
/// round nor any table's forget round: the boards run on their own task.
///
/// Mutation: the board round run after the unit round in one task (the
/// first design): with a unit parked, board rounds stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_partition_holds_up_no_forget_round() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    let db = open(&dir).await;
    let sched = db.partition_maintenance().clone();
    sched.set_period(Duration::from_millis(20));
    exec(&db, CREATE).await;
    let release = crate::core::row::table::table_maintenance::test_hooks::park_once(&dir);
    exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 1, 'a')").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (units, boards) = (sched.rounds(), sched.board_rounds());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (units_after, boards_after) = (sched.rounds(), sched.board_rounds());
    release.notify_one();
    assert_eq!(units_after, units, "the parked unit's round finished while it was parked");
    assert!(boards_after >= boards + 5, "board rounds stopped behind a parked unit ({boards} then {boards_after})");
    close(&dir, db);
}

/// A clean shutdown's exact write with an insert still running: the insert
/// takes the next value after the counters were read and before the file
/// is written. It must raise the reservation above that value (it sees the
/// lowered entry, and waits for the write), or a restart from the file would
/// hand the value out again.
///
/// Mutation: `record_exact` lowering its in-memory entries only after the
/// file write (the first ordering): the insert sees the old, high entry,
/// raises nothing, and the restart repeats its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_during_the_clean_shutdown_write_never_repeats_after_a_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    exec(&db, "CREATE TABLE sx (tenant_id BIGINT NOT NULL, id BIGSERIAL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4").await;
    exec(&db, "INSERT INTO sx (tenant_id, v) VALUES (1, 'a'), (2, 'a'), (3, 'a')").await;
    let (reached, resume) = super::serial::test_hooks::arm(&dir);
    let recording = {
        let db = db.clone();
        tokio::spawn(async move { db.record_clean_shutdown().await })
    };
    reached.notified().await;
    let inserting = {
        let db = db.clone();
        tokio::spawn(async move { exec(&db, "INSERT INTO sx (tenant_id, v) VALUES (4, 'during')").await })
    };
    // Long enough for the insert to take its value and reach its cover.
    tokio::time::sleep(Duration::from_millis(300)).await;
    resume.notify_one();
    recording.await.unwrap();
    inserting.await.unwrap();
    assert_eq!(serial_ids(&db, "sx").await, vec![1, 2, 3, 4]);
    // No second clean write: the restart reads what the file holds now.
    db.sync_durable().unwrap();
    close(&dir, db);
    let db = open(&dir).await;
    exec(&db, "INSERT INTO sx (tenant_id, v) VALUES (5, 'after')").await;
    let got = serial_ids(&db, "sx").await;
    assert_eq!(got.len(), 5);
    assert_distinct(&got, "an insert during the clean-shutdown write, then a restart");
    close(&dir, db);
}

/// Who takes from a shared sequence first in a process.
#[derive(Debug, Clone, Copy)]
enum SharedOrder {
    /// The plain table, seeding the counter from its own rows, then the
    /// partitioned one.
    PlainFirst,
    /// The partitioned table, from its reservation, then the plain one.
    PartitionedFirst,
    /// Only the plain table.
    PlainOnly,
}

/// A `SERIAL` sequence a plain table shares through a hand-written
/// `nextval` default: whichever table seeds the counter in a process, the
/// partitioned table's reservation stays above every value it holds, across
/// clean and crash-like restarts, so its first insert after a restart never
/// repeats one.
///
/// From the rebase review's probes (`probe_shared_sequence_stale_reservation`
/// and `probe_shared_sequence_clean_shutdown_lowers`). The plain table
/// seeding first meant `seed_sequences` never resumed the partitioned one,
/// so nothing set its `tracked` and its reservation was never raised for the
/// rest of the process (repeats `[2, 3]` after clean restarts, `[1026,
/// 1027]` after crash-like ones); and a clean shutdown lowered the
/// reservation to a counter the plain table had seeded below the
/// partitioned table's values (3 lowered to 1, and 2 handed out again).
///
/// The plain table's own values can still collide with the partitioned
/// table's, and a plain table that seeds the counter below values the
/// partitioned one holds makes both draw them again (it seeds from its own
/// maximum: `sequence::follow_rename` documents that sharing). That predates
/// P5a and is kept out of the way here (the plain table's maximum is raised
/// to the partitioned table's, except in the plain-only case, which needs
/// the low seed and has the partitioned table take nothing meanwhile); only
/// the partitioned table's values are asserted.
///
/// Mutations: `SerialSource::open` starting `tracked` false (plain-first
/// repeats, clean and crash-like); `record_exact` lowering entries not
/// seeded here (plain-only, clean: the reservation falls below the stored
/// values, and they repeat).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_sequence_never_repeats_a_partitioned_tables_values() {
    use SharedOrder::*;
    for order in [PlainFirst, PartitionedFirst, PlainOnly] {
        for clean in [true, false] {
            let what = format!("{order:?}, {} restarts", if clean { "clean" } else { "crash-like" });
            let base = tempfile::tempdir().unwrap();
            let dir = base.path().join("db");
            let db = open(&dir).await;
            exec(&db, "CREATE TABLE sa (tenant_id BIGINT NOT NULL, id BIGSERIAL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4").await;
            exec(&db, "CREATE TABLE sb (id BIGINT DEFAULT nextval('sa_id_seq'), v TEXT)").await;
            exec(&db, "INSERT INTO sa (tenant_id, v) VALUES (1, 'start1'), (2, 'start1'), (3, 'start1')").await;
            if !matches!(order, PlainOnly) {
                // The plain table's maximum at the partitioned one's, so its
                // seed next start is not below values the partitioned table
                // already holds: that older defect (both tables then draw
                // from a counter below them) is not what this pins, and
                // would repeat values within start 2 already.
                exec(&db, "INSERT INTO sb (id, v) VALUES (3, 'explicit')").await;
            }
            let at: &Path = &dir;
            let restart = |db: Arc<Database>| async move {
                if clean {
                    db.record_clean_shutdown().await;
                }
                db.sync_durable().unwrap();
                close(at, db);
                open(at).await
            };
            let reserved = |db: &Database| match db.relation("sa") {
                Some(Relation::Partitioned(t)) => t.serial_reserved("sa_id_seq"),
                _ => None,
            };
            let db = restart(db).await;

            // Enough partitioned rows to pass a whole reservation step.
            let many: Vec<String> = (0..1100).map(|i| format!("({}, 'start2')", i % 7)).collect();
            let many = format!("INSERT INTO sa (tenant_id, v) VALUES {}", many.join(", "));
            let mut expected = 3;
            match order {
                PlainFirst => {
                    exec(&db, "INSERT INTO sb (v) VALUES ('b')").await;
                    exec(&db, &many).await;
                    expected += 1100;
                }
                PartitionedFirst => {
                    exec(&db, &many).await;
                    exec(&db, "INSERT INTO sb (v) VALUES ('b')").await;
                    expected += 1100;
                }
                PlainOnly => {
                    exec(&db, "INSERT INTO sb (v) VALUES ('b')").await;
                }
            }
            let held = serial_ids(&db, "sa").await;
            let max = *held.last().unwrap();
            if clean {
                db.record_clean_shutdown().await;
            }
            let on_disk = reserved(&db);
            assert!(
                on_disk.is_some_and(|r| r >= max),
                "{what}: the reservation {on_disk:?} is below the partitioned table's {max}"
            );
            let db = restart(db).await;

            exec(&db, "INSERT INTO sa (tenant_id, v) VALUES (5, 'start3'), (6, 'start3')").await;
            let got = serial_ids(&db, "sa").await;
            assert_eq!(got.len(), expected + 2, "{what}");
            assert_distinct(&got, &what);
            close(&dir, db);
        }
    }
}

/// The log syncs one forget round of eight pinned partitions takes, with
/// nothing to rewrite (`clean`: every stamp committed) and with an aborted
/// row in every partition (each sweep's rewrites synced first, as
/// `neutralize_aborted_from` makes them durable). Counted from the WAL's own
/// sync site, with the scheduler's rounds and the log writer's held off the
/// directory.
async fn forget_round_syncs(aborted: bool) -> (usize, usize) {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    let _held = crate::core::log_writer::test_hooks::hold(base.path());
    let db = open(&dir).await;
    // Before any board registers, so the scheduler's first sleep is this.
    db.partition_maintenance().set_period(Duration::from_secs(3600));
    exec(&db, "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 8").await;
    let values: Vec<String> = (1..=64i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let t = pt(&db);
    assert_eq!(materialized(&t).len(), 8);
    if aborted {
        let x = db.transactions.begin();
        let values: Vec<String> = (1..=64i64).map(|t| format!("({t}, {}, 'aborted')", 1000 + t)).collect();
        exec_in(&db, x, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
        db.abort_transaction(x);
    }
    advance(&db);
    db.sync_durable().unwrap();
    let board = t.pin_board().unwrap().clone();
    board.set_step(1);
    let watch = crate::core::sync_audit::watch(&dir);
    let advanced = board.forget_round(8).await;
    let syncs = watch.take().iter().filter(|(tag, _)| *tag == "wal.log").count();
    drop(watch);
    drop((t, board));
    close(&dir, db);
    (advanced, syncs)
}

/// A forget round's `SWEPT`s are one map-log transaction: one log sync for
/// the round, however many partitions advanced, on top of the syncs each
/// sweep's own rewrites take. Written ahead of the log, a `SWEPT` cannot be
/// left to group commit (a transaction holding an in-place write syncs at
/// its commit), so unbatched a round of eight took eight.
///
/// Mutation: `forget_round` sweeping each partition with its own `sweep`
/// (the unbatched form): eight syncs for a clean round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forget_round_takes_one_map_log_sync() {
    let (advanced, clean) = forget_round_syncs(false).await;
    assert_eq!(advanced, 8, "not every pinned partition advanced");
    let (advanced, rewriting) = forget_round_syncs(true).await;
    assert_eq!(advanced, 8, "not every pinned partition advanced");
    eprintln!("log syncs for one forget round of 8 partitions: {clean} with nothing to rewrite, {rewriting} with an aborted row in each");
    assert_eq!(clean, 1, "a clean round took {clean} log syncs");
}
