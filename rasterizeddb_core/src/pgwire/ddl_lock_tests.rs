//! DDL under ACCESS EXCLUSIVE on the wire (slice B4, docs/persistent-indexes.md
//! §7 "As built (B4)"): CREATE INDEX waits for a block that read the table,
//! as in PostgreSQL; DROP waits for its readers and the statements queued
//! behind it get 42P01, even with a CREATE of the name queued after them;
//! crossed RENAMEs serialise with no 40P01, and a RENAME and a CREATE of its
//! target serialise; a parent's DDL locks the FK child whose sidecar it
//! rewrites, and ADD FOREIGN KEY locks the parent against its writers but not
//! its readers (finding L11); plain VACUUM runs beside writers while VACUUM
//! FULL waits for them, and a bare VACUUM gives each table back as it goes
//! (finding L6).
//!
//! Driven through `execute_recorded` as `table_lock_tests` is.

use std::time::{Duration, Instant};

use super::owner_tests::{Session, ok, send, session};
use super::*;

const SETTLE: Duration = Duration::from_millis(400);
const BOUND: Duration = Duration::from_secs(20);

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

type Running = tokio::task::JoinHandle<(Session, Result<usize, String>, Duration)>;

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

fn sqlstate_of(error: &str) -> &str {
    error
        .split('\0')
        .find_map(|field| field.strip_prefix('C'))
        .unwrap_or("")
}

async fn setup(db: &Arc<Database>, statements: &[&str]) -> Session {
    let mut s = session(db);
    for sql in statements {
        ok(db, &mut s, sql).await;
    }
    s
}

/// A block that has read `table`: it holds ACCESS SHARE to its end.
async fn reader(db: &Arc<Database>, table: &str) -> Session {
    let mut r = session(db);
    ok(db, &mut r, "BEGIN").await;
    ok(db, &mut r, &format!("SELECT * FROM {table}")).await;
    r
}

/// CREATE INDEX waits for a block that read the table (its ACCESS SHARE
/// lasts to the block's end, PostgreSQL's semantics) and builds once the
/// block ends; the reader's own reads go on meanwhile.
///
/// Mutations: `Database::lock_table` grants ACCESS EXCLUSIVE without asking
/// (the CREATE INDEX runs beside the reader; with the debug assertion in
/// `reload_with` it fails there instead); the declaration written before
/// the arm's `table_for` (it sits on disk while the DDL waits).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_waits_for_a_reader_holding_access_share() {
    let db = db().await;
    let _s = setup(
        &db,
        &["CREATE TABLE t (id INT PRIMARY KEY, v INT)", "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)"],
    )
    .await;
    let mut r = reader(&db, "t").await;
    let ddl = spawn(&db, session(&db), "CREATE INDEX t_v ON t (v)");
    tokio::time::sleep(SETTLE).await;
    assert!(!ddl.is_finished(), "CREATE INDEX ran beside a block holding ACCESS SHARE");
    // The holder keeps reading: rule (b), it already holds the table.
    assert_eq!(ok(&db, &mut r, "SELECT id FROM t WHERE v = 20").await, 1);
    // The sidecar on disk, not the open schema: a declaration written
    // before the lock would sit there while the DDL waited (§7 step 3).
    async fn on_disk(db: &Arc<Database>) -> bool {
        let io = db.table_for_tests("t").unwrap().io_schema.clone();
        crate::core::index::decl::load(io)
            .await
            .iter()
            .any(|i| i.name == "t_v" && !i.is_deleted)
    }
    assert!(!on_disk(&db).await, "the declaration was written before the lock was held");
    ok(&db, &mut r, "COMMIT").await;
    let (_, out, _) = finish("CREATE INDEX", ddl).await;
    out.expect("CREATE INDEX after the reader");
    assert!(on_disk(&db).await, "the index is not declared after the reload");
    let open = db.schema_view("t").unwrap().schema().secondary_indexes.iter().any(|i| i.name == "t_v");
    assert!(open, "the reloaded instance does not carry the index");
}

/// DROP waits for its readers; a SELECT that arrives while it waits queues
/// behind it and gets 42P01 once the DROP is done, and a CREATE of the same
/// name that asks after that SELECT queues behind it, so the SELECT never
/// meets the new table: 42P01 by the lock order, then the CREATE succeeds.
///
/// Mutation: `lock_create_target` takes no lock (the CREATE runs at once
/// while the old table still exists and fails with 42P07).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_waits_for_readers_and_its_waiters_get_42p01() {
    let db = db().await;
    let _s = setup(&db, &["CREATE TABLE t (id INT PRIMARY KEY, v TEXT)", "INSERT INTO t (id, v) VALUES (1, 'a')"]).await;
    let mut r = reader(&db, "t").await;
    let drop_t = spawn(&db, session(&db), "DROP TABLE t");
    tokio::time::sleep(SETTLE).await;
    assert!(!drop_t.is_finished(), "DROP ran beside a reader");
    let late = spawn(&db, session(&db), "SELECT id FROM t WHERE id = 1");
    tokio::time::sleep(SETTLE).await;
    let create = spawn(&db, session(&db), "CREATE TABLE t (id INT PRIMARY KEY, w TEXT)");
    tokio::time::sleep(SETTLE).await;
    assert!(!late.is_finished(), "a SELECT passed a waiting DROP");
    assert!(!create.is_finished(), "a CREATE of the name ran while its DROP waited");

    ok(&db, &mut r, "COMMIT").await;
    let (_, dropped, _) = finish("DROP", drop_t).await;
    dropped.expect("the DROP after its reader");
    let (_, read, _) = finish("the queued SELECT", late).await;
    let e = read.expect_err("the SELECT queued behind the DROP read a table");
    assert_eq!(sqlstate_of(&e), "42P01", "{e}");
    let (_, created, _) = finish("CREATE", create).await;
    created.expect("the CREATE after the DROP");
    let mut s = session(&db);
    ok(&db, &mut s, "INSERT INTO t (id, w) VALUES (5, 'new')").await;
    assert_eq!(ok(&db, &mut s, "SELECT w FROM t").await, 1);
}

/// RENAME a TO b and RENAME b TO a, concurrent: each takes both names in
/// name order, so the second queues behind the first instead of holding its
/// old name while it waits for its new one. Both succeed, one after the
/// other, with no 40P01.
///
/// Mutation: `ddl_lock_extras` returns `None` for ALTER (the statement takes
/// only its old name; the new one late): the second RENAME starts with
/// nothing to wait on and fails, its old name not existing yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crossed_renames_serialise_without_a_deadlock() {
    let db = db().await;
    let _s = setup(&db, &["CREATE TABLE a (id INT PRIMARY KEY)", "INSERT INTO a (id) VALUES (1)"]).await;
    let mut r = reader(&db, "a").await;
    let first = spawn(&db, session(&db), "ALTER TABLE a RENAME TO b");
    tokio::time::sleep(SETTLE).await;
    let second = spawn(&db, session(&db), "ALTER TABLE b RENAME TO a");
    tokio::time::sleep(SETTLE).await;
    assert!(!first.is_finished() && !second.is_finished(), "a RENAME ran beside the reader");
    ok(&db, &mut r, "COMMIT").await;
    let (_, one, _) = finish("RENAME a TO b", first).await;
    let (_, two, _) = finish("RENAME b TO a", second).await;
    one.expect("RENAME a TO b");
    two.expect("RENAME b TO a, after the first");
    assert!(db.has_table("a") && !db.has_table("b"), "the renames did not run one after the other");
    let mut s = session(&db);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM a").await, 1);
}

/// A RENAME holds its new name from the start (name order puts `b` before
/// `z`), so a CREATE of that name waits for it, then finds the name taken:
/// one of the two fails with 42P07, never both succeed, and the RENAME is
/// not refused for a name another statement is creating.
///
/// Mutation: `lock_create_target` takes no lock (the CREATE takes the name
/// while the RENAME waits, and the RENAME fails).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rename_and_a_create_of_its_target_serialise() {
    let db = db().await;
    let _s = setup(&db, &["CREATE TABLE z (id INT PRIMARY KEY)", "INSERT INTO z (id) VALUES (1)"]).await;
    let mut r = reader(&db, "z").await;
    let rename = spawn(&db, session(&db), "ALTER TABLE z RENAME TO b");
    tokio::time::sleep(SETTLE).await;
    let create = spawn(&db, session(&db), "CREATE TABLE b (id INT PRIMARY KEY, other TEXT)");
    tokio::time::sleep(SETTLE).await;
    assert!(!create.is_finished(), "the CREATE took a name a waiting RENAME holds");
    ok(&db, &mut r, "COMMIT").await;
    let (_, renamed, _) = finish("RENAME", rename).await;
    renamed.expect("the RENAME");
    let (_, created, _) = finish("CREATE", create).await;
    let e = created.expect_err("both the RENAME and the CREATE of its target succeeded");
    assert_eq!(sqlstate_of(&e), "42P07", "{e}");
    let mut s = session(&db);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM b").await, 1, "b is not the renamed table");
}

/// Finding L11. A parent's DDL that rewrites a child's FK sidecar (RENAME
/// COLUMN of the referenced column) takes ACCESS EXCLUSIVE on the child
/// too, so it waits for the child's readers before writing anything; and
/// ADD FOREIGN KEY takes SHARE on the parent, so it waits for a parent
/// writer (a DELETE in flight) but not for a parent reader.
///
/// Mutation: `alter_lock_set` leaves the FK children out (the RENAME COLUMN
/// runs beside the child's reader; with the debug assertion in
/// `table_held` it fails there instead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fk_ddl_locks_the_other_side() {
    let db = db().await;
    let _s = setup(
        &db,
        &[
            "CREATE TABLE p (id INT PRIMARY KEY, k INT)",
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p (id))",
            "CREATE TABLE c2 (id INT PRIMARY KEY, pid INT)",
            "INSERT INTO p (id, k) VALUES (1, 1), (99, 9)",
            "INSERT INTO c (id, pid) VALUES (1, 1)",
        ],
    )
    .await;

    // The child's reader holds back the parent's RENAME COLUMN.
    let mut r = reader(&db, "c").await;
    let rename = spawn(&db, session(&db), "ALTER TABLE p RENAME COLUMN id TO pk");
    tokio::time::sleep(SETTLE).await;
    assert!(!rename.is_finished(), "the parent's DDL rewrote a child another block is reading");
    ok(&db, &mut r, "COMMIT").await;
    let (_, out, _) = finish("RENAME COLUMN", rename).await;
    out.expect("RENAME COLUMN after the child's reader");
    let parent_column = db.schema_view("c").unwrap().schema().foreign_keys[0].parent_column.clone();
    assert_eq!(parent_column, "pk", "the child's key does not follow the renamed column");

    // A parent reader does not hold back ADD FOREIGN KEY...
    let mut pr = reader(&db, "p").await;
    let add = spawn(&db, session(&db), "ALTER TABLE c2 ADD CONSTRAINT c2_p FOREIGN KEY (pid) REFERENCES p (pk)");
    let (_, out, took) = finish("ADD FOREIGN KEY beside a parent reader", add).await;
    out.expect("ADD FOREIGN KEY beside a parent reader");
    assert!(took < SETTLE * 5, "ADD FOREIGN KEY waited {took:?} for a parent reader");
    ok(&db, &mut pr, "COMMIT").await;
    let mut s = session(&db);
    ok(&db, &mut s, "ALTER TABLE c2 DROP CONSTRAINT c2_p").await;

    // ...but a parent writer does.
    let mut w = session(&db);
    ok(&db, &mut w, "BEGIN").await;
    ok(&db, &mut w, "DELETE FROM p WHERE pk = 99").await;
    let add = spawn(&db, session(&db), "ALTER TABLE c2 ADD CONSTRAINT c2_p FOREIGN KEY (pid) REFERENCES p (pk)");
    tokio::time::sleep(SETTLE).await;
    assert!(!add.is_finished(), "ADD FOREIGN KEY ran beside a parent DELETE in flight");
    ok(&db, &mut w, "COMMIT").await;
    let (_, out, _) = finish("ADD FOREIGN KEY after the parent writer", add).await;
    out.expect("ADD FOREIGN KEY after the parent writer");
}

/// Finding L6. Plain VACUUM takes ROW EXCLUSIVE, which a block's INSERT in
/// flight does not conflict with: it finishes beside it. VACUUM FULL takes
/// ACCESS EXCLUSIVE and waits for the block, then runs.
///
/// Mutation: `vacuum_mode` gives VACUUM FULL ROW EXCLUSIVE (it runs beside
/// the writer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_vacuum_runs_beside_a_writer_and_vacuum_full_waits() {
    let db = db().await;
    let _s = setup(&db, &["CREATE TABLE t (id INT PRIMARY KEY, v TEXT)", "INSERT INTO t (id, v) VALUES (1, 'a')"]).await;
    let mut w = session(&db);
    ok(&db, &mut w, "BEGIN").await;
    ok(&db, &mut w, "INSERT INTO t (id, v) VALUES (2, 'b')").await;

    let plain = spawn(&db, session(&db), "VACUUM t");
    let (_, out, took) = finish("VACUUM beside a writer", plain).await;
    out.expect("VACUUM beside a writer");
    assert!(took < SETTLE * 5, "plain VACUUM waited {took:?} for a writer");

    let full = spawn(&db, session(&db), "VACUUM FULL t");
    tokio::time::sleep(SETTLE).await;
    assert!(!full.is_finished(), "VACUUM FULL ran beside a writer");
    ok(&db, &mut w, "COMMIT").await;
    let (_, out, _) = finish("VACUUM FULL after the writer", full).await;
    out.expect("VACUUM FULL after the writer");
    let mut s = session(&db);
    assert_eq!(ok(&db, &mut s, "SELECT id FROM t").await, 2);
}

/// A bare VACUUM takes each table under an owner of its own and gives it
/// back when it moves on: while it waits on the last table (held by a
/// block), the tables it has done are free, so another session takes ACCESS
/// EXCLUSIVE on one at once.
///
/// Mutation: `VacuumOwner`'s drop does not release its owner (the done
/// table stays locked until the VACUUM ends; NOWAIT fails with 55P03).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bare_vacuum_gives_each_table_back() {
    let db = db().await;
    let _s = setup(
        &db,
        &["CREATE TABLE va (id INT PRIMARY KEY)", "CREATE TABLE vz (id INT PRIMARY KEY)", "INSERT INTO va (id) VALUES (1)"],
    )
    .await;
    // Tables are vacuumed in name order: `va` first, then a wait on `vz`.
    let mut h = session(&db);
    ok(&db, &mut h, "BEGIN").await;
    ok(&db, &mut h, "LOCK TABLE vz IN ACCESS EXCLUSIVE MODE").await;
    let vacuum = spawn(&db, session(&db), "VACUUM");
    tokio::time::sleep(SETTLE).await;
    assert!(!vacuum.is_finished(), "VACUUM did not wait on the locked table");
    let mut other = session(&db);
    ok(&db, &mut other, "BEGIN").await;
    let took = send(&db, &mut other, "LOCK TABLE va IN ACCESS EXCLUSIVE MODE NOWAIT").await;
    took.expect("a table the bare VACUUM had finished was still locked");
    ok(&db, &mut other, "COMMIT").await;
    ok(&db, &mut h, "COMMIT").await;
    let (_, out, _) = finish("VACUUM", vacuum).await;
    out.expect("VACUUM after the block");
}
