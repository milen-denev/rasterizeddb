//! What a column carries with it through `RENAME COLUMN`.
//!
//! Several per-column declarations live in sidecars keyed by the column's
//! *name* — its `DEFAULT`, its generation expression, its NUMERIC scale. A
//! rename that only rewrote the field left each of them filed under the old
//! name, so the renamed column silently lost them: an `INSERT` that omitted it
//! stored NULL where PostgreSQL stores the default, and nothing reported an
//! error. And a record left under a name that later came back — the retype
//! dance (add tmp, copy, drop original, rename tmp back) does exactly that —
//! was picked up by the new column, which then defaulted to the *dropped*
//! column's value.
//!
//! Driven through the executor, as the server drives it, with a reopen of the
//! database where the record's durability is the question.

use crate::core::database::Database;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "renamecoltests").await)
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

fn is_error(response: &[u8]) -> bool {
    response.first() == Some(&3)
}

#[track_caller]
fn expect_ok(response: &[u8], sql: &str) {
    if is_error(response) {
        panic!(
            "`{}` failed: {}",
            sql,
            String::from_utf8_lossy(&response[1..])
        );
    }
}

async fn ok(database: &Arc<Database>, sql: &str) {
    expect_ok(&run(database, sql).await, sql);
}

async fn rows(database: &Arc<Database>, sql: &str) -> Vec<Row> {
    let response = run(database, sql).await;
    expect_ok(&response, sql);
    vec_into_rows(&response[1..]).expect("a decodable result set")
}

/// An integer cell, widened; `None` for SQL NULL.
fn int(row: &Row, column: usize) -> Option<i64> {
    let cell = &row.columns[column];
    if cell.column_type == crate::core::db_type::DbType::NULL {
        return None;
    }
    let bytes = cell.data.into_slice();
    Some(match bytes.len() {
        0 => return None,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        n => panic!("an integer cell of {} bytes", n),
    })
}

/// Every value of one integer column of `sql`'s result, in row order.
async fn ints(database: &Arc<Database>, sql: &str) -> Vec<Option<i64>> {
    rows(database, sql).await.iter().map(|r| int(r, 0)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_column_keeps_its_default() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (a INT DEFAULT 7, b INT)").await;
    ok(&db, "ALTER TABLE t RENAME COLUMN a TO c").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT c FROM t WHERE b = 1").await,
        vec![Some(7)],
        "the renamed column lost its DEFAULT"
    );

    // And back again: the record follows every rename, not only the first.
    ok(&db, "ALTER TABLE t RENAME COLUMN c TO a").await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t WHERE b = 2").await,
        vec![Some(7)],
        "renaming the column back lost its DEFAULT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_default_survives_a_restart() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path()).await;
        ok(&db, "CREATE TABLE t (a INT DEFAULT 7, b INT)").await;
        ok(&db, "ALTER TABLE t RENAME COLUMN a TO c").await;
        drop(db);
    }
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT c FROM t WHERE b = 1").await,
        vec![Some(7)],
        "the renamed column's DEFAULT did not survive a restart"
    );
    // The old name holds nothing: a column added under it later is a new
    // column, with no default of its own.
    ok(&db, "ALTER TABLE t ADD COLUMN a INT").await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t WHERE b = 2").await,
        vec![None],
        "a column added under the renamed column's old name inherited its DEFAULT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_column_type_keeps_the_default_through_a_later_rename() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (a INT DEFAULT 7, b INT)").await;
    // A change of width is refused on this branch (every row would need a
    // rewrite); naming the type the column already has is accepted as a
    // no-op. Either way the column's default must still be there afterwards,
    // and must follow a later rename — as in PostgreSQL.
    let widened = run(&db, "ALTER TABLE t ALTER COLUMN a TYPE BIGINT").await;
    assert!(
        is_error(&widened),
        "widening is refused on this branch; if it is now supported, this test must check \
         that the DEFAULT survives the widening itself, not only the rename after it"
    );
    // The refused ALTER left the default in place.
    ok(&db, "INSERT INTO t (b) VALUES (0)").await;
    assert_eq!(ints(&db, "SELECT a FROM t WHERE b = 0").await, vec![Some(7)]);
    ok(&db, "ALTER TABLE t ALTER COLUMN a TYPE INT").await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    assert_eq!(ints(&db, "SELECT a FROM t WHERE b = 2").await, vec![Some(7)]);
    ok(&db, "ALTER TABLE t RENAME COLUMN a TO c").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT c FROM t WHERE b = 1").await,
        vec![Some(7)],
        "ALTER COLUMN TYPE then RENAME lost the DEFAULT"
    );
}

/// The retype dance an ORM migration runs in place of `ALTER COLUMN TYPE`:
/// add a temporary column, copy, drop the original, rename the temporary one
/// to the original name. In PostgreSQL the result has the *temporary* column's
/// default — the original's went with it when it was dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_retype_dance_ends_with_the_new_columns_default() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    // With a default on the temporary column: that one is the result's.
    ok(&db, "CREATE TABLE t (a INT DEFAULT 7, b INT)").await;
    ok(&db, "INSERT INTO t (a, b) VALUES (5, 0)").await;
    ok(&db, "ALTER TABLE t ADD COLUMN a_tmp BIGINT DEFAULT 9").await;
    ok(&db, "UPDATE t SET a_tmp = a").await;
    ok(&db, "ALTER TABLE t DROP COLUMN a").await;
    ok(&db, "ALTER TABLE t RENAME COLUMN a_tmp TO a").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t ORDER BY b").await,
        vec![Some(5), Some(9)],
        "after the retype dance the column defaulted to the dropped column's DEFAULT, \
         not the one it was added with"
    );

    // Without one: the dropped column's default must not come back.
    ok(&db, "CREATE TABLE u (a INT DEFAULT 7, b INT)").await;
    ok(&db, "ALTER TABLE u ADD COLUMN a_tmp BIGINT").await;
    ok(&db, "ALTER TABLE u DROP COLUMN a").await;
    ok(&db, "ALTER TABLE u RENAME COLUMN a_tmp TO a").await;
    ok(&db, "INSERT INTO u (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM u WHERE b = 1").await,
        vec![None],
        "a dropped column's DEFAULT came back on the column renamed onto its name"
    );

    // And across a restart, where the sidecar is replayed from the file.
    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    ok(&db, "INSERT INTO u (b) VALUES (2)").await;
    assert_eq!(ints(&db, "SELECT a FROM t WHERE b = 2").await, vec![Some(9)]);
    assert_eq!(ints(&db, "SELECT a FROM u WHERE b = 2").await, vec![None]);
}

/// `DROP COLUMN` then `ADD COLUMN` under the same name is a new column, which
/// in PostgreSQL has no default unless it is given one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_columns_default_does_not_reach_a_new_column_of_its_name() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (a INT DEFAULT 7, b INT)").await;
    ok(&db, "ALTER TABLE t DROP COLUMN a").await;
    ok(&db, "ALTER TABLE t ADD COLUMN a INT").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t WHERE b = 1").await,
        vec![None],
        "a re-added column inherited the dropped column's DEFAULT"
    );

    // With a row in the table, `ADD COLUMN` rewrites it rather than appending
    // a field — a separate path, carrying the table's defaults across by name.
    ok(&db, "CREATE TABLE t2 (a INT DEFAULT 7, b INT)").await;
    ok(&db, "INSERT INTO t2 (a, b) VALUES (1, 0)").await;
    ok(&db, "ALTER TABLE t2 DROP COLUMN a").await;
    ok(&db, "ALTER TABLE t2 ADD COLUMN a INT").await;
    ok(&db, "INSERT INTO t2 (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t2 ORDER BY b").await,
        vec![None, None],
        "a re-added column of a populated table inherited the dropped column's DEFAULT"
    );
}

/// A generated column's expression is filed under the column's name, the same
/// way a default is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_generated_column_is_still_generated() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(
        &db,
        "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED, k INT)",
    )
    .await;
    ok(&db, "ALTER TABLE g RENAME COLUMN b TO d").await;
    ok(&db, "INSERT INTO g (a, k) VALUES (3, 1)").await;
    assert_eq!(
        ints(&db, "SELECT d FROM g WHERE k = 1").await,
        vec![Some(6)],
        "the renamed generated column stopped being computed"
    );
    // PostgreSQL refuses a value for a generated column.
    assert!(
        is_error(&run(&db, "INSERT INTO g (a, d, k) VALUES (1, 100, 2)").await),
        "an INSERT supplying the renamed generated column was accepted"
    );

    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO g (a, k) VALUES (4, 3)").await;
    assert_eq!(
        ints(&db, "SELECT d FROM g WHERE k = 3").await,
        vec![Some(8)],
        "the renamed generated column's expression did not survive a restart"
    );
}

/// A NUMERIC(p,s) column's scale is filed under its name. Lost by a rename, the
/// stored integers were read at the engine's default scale: `12.34` stopped
/// matching `= 12.34`, with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_numeric_column_keeps_its_scale() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE n (p NUMERIC(10,2), k INT)").await;
    ok(&db, "INSERT INTO n (p, k) VALUES (12.34, 1)").await;
    assert_eq!(ints(&db, "SELECT k FROM n WHERE p = 12.34").await, vec![Some(1)]);
    ok(&db, "ALTER TABLE n RENAME COLUMN p TO q").await;
    assert_eq!(
        ints(&db, "SELECT k FROM n WHERE q = 12.34").await,
        vec![Some(1)],
        "the renamed NUMERIC column lost its scale"
    );
    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(
        ints(&db, "SELECT k FROM n WHERE q = 12.34").await,
        vec![Some(1)],
        "the renamed NUMERIC column's scale did not survive a restart"
    );
}

/// The rename and the drop themselves must outlast a restart. Both overwrite a
/// FIELDS record in place, and that record went in as a logged append: written
/// around the log, the replay at every open put the original back, so the
/// column came back under its old name, and a dropped column came back alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_and_drop_column_survive_a_restart() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path()).await;
        ok(&db, "CREATE TABLE r (a INT, b INT, z INT)").await;
        ok(&db, "INSERT INTO r (a, b, z) VALUES (1, 2, 3)").await;
        ok(&db, "ALTER TABLE r RENAME COLUMN a TO c").await;
        ok(&db, "ALTER TABLE r DROP COLUMN z").await;
        drop(db);
    }
    let db = open(dir.path()).await;
    assert_eq!(
        ints(&db, "SELECT c FROM r").await,
        vec![Some(1)],
        "RENAME COLUMN was undone by a restart"
    );
    assert!(
        is_error(&run(&db, "SELECT a FROM r").await),
        "the old column name still resolved after the restart"
    );
    assert!(
        is_error(&run(&db, "SELECT z FROM r").await),
        "DROP COLUMN was undone by a restart: the dropped column is back"
    );
}

/// A table written before drops retired their declarations can still hold a
/// default under a name no live column has. A rename onto that name must not
/// hand it to the renamed column.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rename_onto_a_name_with_a_leftover_default_does_not_inherit_it() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (x INT, b INT)").await;
    // The leftover, written as an older engine's `DROP COLUMN c` left it.
    let io = db.table_for_tests("t").expect("table t").io_schema.clone();
    crate::core::rql::column_default::append(
        io,
        &crate::core::rql::column_default::ColumnDefault {
            column: "c".to_string(),
            expression: "5".to_string(),
            is_deleted: false,
        },
    )
    .await
    .unwrap();
    db.reload_table("t").await.unwrap();

    ok(&db, "ALTER TABLE t RENAME COLUMN x TO c").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT c FROM t WHERE b = 1").await,
        vec![None],
        "the renamed column picked up a default left under its new name"
    );
}

/// Every value of `sql`'s result, two integer columns per row.
async fn int_pairs(database: &Arc<Database>, sql: &str) -> Vec<(Option<i64>, Option<i64>)> {
    rows(database, sql)
        .await
        .iter()
        .map(|r| (int(r, 0), int(r, 1)))
        .collect()
}

/// A `SERIAL` column's default reads `nextval('t_a_seq')`, and PostgreSQL keeps
/// that name through `RENAME COLUMN a TO c`. The table must still own the
/// counter afterwards: `DROP TABLE` forgets it, so a recreated `t` starts at 1.
/// Recognised by the column the default was filed under, the moved default
/// stopped naming the table's own sequence, and the new `t` started at 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_table_forgets_the_serial_of_a_renamed_column() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (a SERIAL, b INT)").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    ok(&db, "ALTER TABLE t RENAME COLUMN a TO c").await;
    ok(&db, "INSERT INTO t (b) VALUES (3)").await;
    assert_eq!(
        ints(&db, "SELECT c FROM t WHERE b = 3").await,
        vec![Some(3)],
        "the renamed SERIAL column stopped counting"
    );
    ok(&db, "DROP TABLE t").await;
    ok(&db, "CREATE TABLE t (a SERIAL, b INT)").await;
    ok(&db, "INSERT INTO t (b) VALUES (10)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t WHERE b = 10").await,
        vec![Some(1)],
        "the dropped table's counter outlived it: the recreated table did not start at 1"
    );
}

/// `RENAME TABLE` moves the table's `SERIAL` counters to the new name, so a
/// table created under the old name has a counter of its own. After a column
/// rename the counter stayed behind, the two tables shared it, and after a
/// restart the renamed table stored a key it already held — with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_table_takes_the_serial_of_a_renamed_column() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path()).await;
        ok(&db, "CREATE TABLE s (a SERIAL, b INT)").await;
        ok(&db, "INSERT INTO s (b) VALUES (1)").await;
        ok(&db, "INSERT INTO s (b) VALUES (2)").await;
        ok(&db, "ALTER TABLE s RENAME COLUMN a TO c").await;
        ok(&db, "ALTER TABLE s RENAME TO s2").await;
        ok(&db, "CREATE TABLE s (a SERIAL, b INT)").await;
        ok(&db, "INSERT INTO s (b) VALUES (10)").await;
        ok(&db, "INSERT INTO s2 (b) VALUES (3)").await;
        assert_eq!(
            ints(&db, "SELECT a FROM s WHERE b = 10").await,
            vec![Some(1)],
            "the new table took over the renamed table's counter"
        );
        assert_eq!(ints(&db, "SELECT c FROM s2 WHERE b = 3").await, vec![Some(3)]);
        drop(db);
    }
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO s (b) VALUES (11)").await;
    ok(&db, "INSERT INTO s2 (b) VALUES (4)").await;
    assert_eq!(ints(&db, "SELECT a FROM s WHERE b = 11").await, vec![Some(2)]);
    assert_eq!(
        int_pairs(&db, "SELECT c, b FROM s2 ORDER BY b").await,
        vec![
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(4), Some(4))
        ],
        "the renamed table's keys are not 1..4 after a restart"
    );
}

/// A composite `UNIQUE (a, b)` names its columns in a sidecar. Left naming
/// `a` after `RENAME COLUMN a TO x`, it no longer matched any column and was
/// silently not enforced: a duplicate pair was stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_composite_unique_key_follows_a_renamed_column() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE u (a INT, b INT, k INT, UNIQUE (a, b))").await;
    ok(&db, "INSERT INTO u (a, b, k) VALUES (1, 1, 1)").await;
    assert!(is_error(&run(&db, "INSERT INTO u (a, b, k) VALUES (1, 1, 2)").await));
    ok(&db, "ALTER TABLE u RENAME COLUMN a TO x").await;
    assert!(
        is_error(&run(&db, "INSERT INTO u (x, b, k) VALUES (1, 1, 3)").await),
        "after RENAME COLUMN the composite UNIQUE key was not enforced"
    );
    ok(&db, "INSERT INTO u (x, b, k) VALUES (1, 2, 4)").await;

    drop(db);
    let db = open(dir.path()).await;
    assert!(
        is_error(&run(&db, "INSERT INTO u (x, b, k) VALUES (1, 1, 5)").await),
        "after a restart the renamed composite UNIQUE key was not enforced"
    );
    assert_eq!(ints(&db, "SELECT k FROM u ORDER BY k").await, vec![Some(1), Some(4)]);
}

/// A foreign key names its column, and its parent's, in the child's sidecar.
/// Renaming the child's column left the key naming nothing, so orphans were
/// accepted; renaming the parent's made every `DELETE` from the parent fail
/// with `column "id" does not exist`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_key_follows_a_renamed_column_on_either_side() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE p (id INT, v INT)").await;
    ok(&db, "CREATE TABLE ch (id INT, pid INT REFERENCES p(id))").await;
    ok(&db, "INSERT INTO p (id, v) VALUES (1, 0)").await;
    ok(&db, "INSERT INTO p (id, v) VALUES (2, 0)").await;
    ok(&db, "INSERT INTO p (id, v) VALUES (3, 0)").await;
    ok(&db, "INSERT INTO ch (id, pid) VALUES (1, 1)").await;

    ok(&db, "ALTER TABLE ch RENAME COLUMN pid TO q").await;
    assert!(
        is_error(&run(&db, "INSERT INTO ch (id, q) VALUES (2, 99)").await),
        "a child row referencing no parent was accepted after its column was renamed"
    );
    ok(&db, "INSERT INTO ch (id, q) VALUES (3, 2)").await;

    ok(&db, "ALTER TABLE p RENAME COLUMN id TO pk").await;
    ok(&db, "DELETE FROM p WHERE pk = 3").await;
    assert!(
        is_error(&run(&db, "DELETE FROM p WHERE pk = 1").await),
        "a referenced parent row was deleted after the parent's column was renamed"
    );
    assert!(is_error(&run(&db, "INSERT INTO ch (id, q) VALUES (4, 3)").await));

    drop(db);
    let db = open(dir.path()).await;
    assert!(
        is_error(&run(&db, "INSERT INTO ch (id, q) VALUES (5, 98)").await),
        "after a restart the renamed foreign key was not enforced"
    );
    assert!(is_error(&run(&db, "DELETE FROM p WHERE pk = 2").await));
    assert_eq!(ints(&db, "SELECT pk FROM p ORDER BY pk").await, vec![Some(1), Some(2)]);
    assert_eq!(ints(&db, "SELECT id FROM ch ORDER BY id").await, vec![Some(1), Some(3)]);
}

/// A `CHECK` is stored as the text written, which names the column. After a
/// rename it named nothing, failed to compile, and was skipped with a log line:
/// every row it forbids was accepted. A string literal spelling the old name is
/// a value, not a reference, and must not be rewritten.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_check_constraint_follows_a_renamed_column() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE c3 (a INT CHECK (a > 0), k INT)").await;
    ok(&db, "CREATE TABLE c4 (a TEXT, k INT, CHECK (a <> 'a'))").await;
    ok(&db, "ALTER TABLE c3 RENAME COLUMN a TO x").await;
    ok(&db, "ALTER TABLE c4 RENAME COLUMN a TO x").await;
    assert!(
        is_error(&run(&db, "INSERT INTO c3 (x, k) VALUES (-1, 1)").await),
        "the CHECK on a renamed column was not enforced"
    );
    ok(&db, "INSERT INTO c3 (x, k) VALUES (5, 2)").await;
    assert!(
        is_error(&run(&db, "INSERT INTO c4 (x, k) VALUES ('a', 1)").await),
        "the CHECK on a renamed column was not enforced"
    );
    ok(&db, "INSERT INTO c4 (x, k) VALUES ('x', 2)").await;

    drop(db);
    let db = open(dir.path()).await;
    assert!(
        is_error(&run(&db, "INSERT INTO c3 (x, k) VALUES (-2, 3)").await),
        "after a restart the CHECK on a renamed column was not enforced"
    );
    assert_eq!(ints(&db, "SELECT k FROM c3").await, vec![Some(2)]);
    assert_eq!(ints(&db, "SELECT k FROM c4").await, vec![Some(2)]);
}

/// A generated column's expression names its inputs. Renaming an input left
/// the expression naming a column that no longer exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_generated_column_follows_its_renamed_input() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(
        &db,
        "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED, k INT)",
    )
    .await;
    ok(&db, "ALTER TABLE g RENAME COLUMN a TO x").await;
    let inserted = run(&db, "INSERT INTO g (x, k) VALUES (3, 1)").await;
    expect_ok(&inserted, "INSERT INTO g (x, k) VALUES (3, 1)");
    assert_eq!(
        ints(&db, "SELECT b FROM g WHERE k = 1").await,
        vec![Some(6)],
        "the generated column stopped reading its renamed input"
    );
    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO g (x, k) VALUES (4, 2)").await;
    assert_eq!(ints(&db, "SELECT b FROM g WHERE k = 2").await, vec![Some(8)]);
}

/// Seeds a declaration under `column` the way an older engine's `DROP COLUMN`
/// left it, or a power loss between a drop's separately durable writes.
async fn seed_leftover_default(db: &Arc<Database>, table: &str, column: &str, expression: &str) {
    let io = db.table_for_tests(table).expect("table").io_schema.clone();
    crate::core::rql::column_default::append(
        io,
        &crate::core::rql::column_default::ColumnDefault {
            column: column.to_string(),
            expression: expression.to_string(),
            is_deleted: false,
        },
    )
    .await
    .unwrap();
    db.reload_table(table).await.unwrap();
}

/// `ADD COLUMN a` with no `DEFAULT` is a column with no default, whatever a
/// dead column of that name left in the sidecar — on both paths, the empty
/// table's append and the populated table's rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_column_does_not_inherit_a_leftover_default() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE t (b INT)").await;
    seed_leftover_default(&db, "t", "a", "5").await;
    ok(&db, "ALTER TABLE t ADD COLUMN a INT").await;
    ok(&db, "INSERT INTO t (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t WHERE b = 1").await,
        vec![None],
        "a column added to an empty table inherited a leftover DEFAULT"
    );

    ok(&db, "CREATE TABLE t2 (b INT)").await;
    ok(&db, "INSERT INTO t2 (b) VALUES (0)").await;
    seed_leftover_default(&db, "t2", "a", "5").await;
    ok(&db, "ALTER TABLE t2 ADD COLUMN a INT").await;
    ok(&db, "INSERT INTO t2 (b) VALUES (1)").await;
    assert_eq!(
        ints(&db, "SELECT a FROM t2 ORDER BY b").await,
        vec![None, None],
        "a column added to a populated table inherited a leftover DEFAULT"
    );

    drop(db);
    let db = open(dir.path()).await;
    ok(&db, "INSERT INTO t (b) VALUES (2)").await;
    ok(&db, "INSERT INTO t2 (b) VALUES (2)").await;
    assert_eq!(ints(&db, "SELECT a FROM t WHERE b = 2").await, vec![None]);
    assert_eq!(ints(&db, "SELECT a FROM t2 WHERE b = 2").await, vec![None]);
}

/// A NUMERIC column's scale goes with it on `RENAME` and `DROP`: a new column
/// added under the old name is unconstrained, and must not round to the old
/// column's two places.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_added_under_a_numeric_columns_old_name_keeps_its_own_scale() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE n (p NUMERIC(10,2), k INT)").await;
    ok(&db, "ALTER TABLE n RENAME COLUMN p TO q").await;
    ok(&db, "ALTER TABLE n ADD COLUMN p NUMERIC").await;
    ok(&db, "INSERT INTO n (p, k) VALUES (1.2345, 1)").await;

    ok(&db, "CREATE TABLE m (p NUMERIC(10,2), k INT)").await;
    ok(&db, "ALTER TABLE m DROP COLUMN p").await;
    ok(&db, "ALTER TABLE m ADD COLUMN p NUMERIC").await;
    ok(&db, "INSERT INTO m (p, k) VALUES (1.2345, 1)").await;

    for table in ["n", "m"] {
        assert_eq!(
            ints(&db, &format!("SELECT k FROM {} WHERE p = 1.2345", table)).await,
            vec![Some(1)],
            "{}: the new column was rounded to the old column's scale",
            table
        );
        assert!(ints(&db, &format!("SELECT k FROM {} WHERE p = 1.23", table)).await.is_empty());
    }
    drop(db);
    let db = open(dir.path()).await;
    for table in ["n", "m"] {
        assert_eq!(
            ints(&db, &format!("SELECT k FROM {} WHERE p = 1.2345", table)).await,
            vec![Some(1)],
            "{}: after a restart the new column read at the old column's scale",
            table
        );
    }
}

/// A generated column renamed away, or dropped, takes its expression along: a
/// plain column added under the old name is not generated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_column_added_under_a_generated_columns_old_name_is_plain() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(
        &db,
        "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED, k INT)",
    )
    .await;
    ok(&db, "ALTER TABLE g RENAME COLUMN b TO d").await;
    ok(&db, "ALTER TABLE g ADD COLUMN b INT").await;
    ok(&db, "INSERT INTO g (a, b, k) VALUES (3, 100, 1)").await;

    ok(
        &db,
        "CREATE TABLE h (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED, k INT)",
    )
    .await;
    ok(&db, "ALTER TABLE h DROP COLUMN b").await;
    ok(&db, "ALTER TABLE h ADD COLUMN b INT").await;
    ok(&db, "INSERT INTO h (a, b, k) VALUES (3, 100, 1)").await;

    assert_eq!(
        int_pairs(&db, "SELECT b, d FROM g WHERE k = 1").await,
        vec![(Some(100), Some(6))]
    );
    assert_eq!(
        ints(&db, "SELECT b FROM h WHERE k = 1").await,
        vec![Some(100)],
        "a column added under a dropped generated column's name was generated"
    );
}

/// `ALTER COLUMN … TYPE NUMERIC(p, s)` on a NUMERIC column is not a same-type
/// no-op when the scale moves. It used to report success and keep the old
/// scale, so every later value was rounded to it. An empty table takes the new
/// scale; a populated one is refused, since its stored integers are scaled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_column_type_numeric_scale_is_not_a_no_op() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;

    ok(&db, "CREATE TABLE n (p NUMERIC(10,2), k INT)").await;
    ok(&db, "ALTER TABLE n ALTER COLUMN p TYPE NUMERIC(12,4)").await;
    ok(&db, "INSERT INTO n (p, k) VALUES (1.2345, 1)").await;
    assert_eq!(
        ints(&db, "SELECT k FROM n WHERE p = 1.2345").await,
        vec![Some(1)],
        "ALTER COLUMN TYPE NUMERIC(12,4) kept the old scale"
    );
    assert!(ints(&db, "SELECT k FROM n WHERE p = 1.23").await.is_empty());
    // The same declaration again changes nothing, and is accepted.
    ok(&db, "ALTER TABLE n ALTER COLUMN p TYPE NUMERIC(12,4)").await;
    // The table holds a row now: moving the scale would reinterpret it.
    assert!(
        is_error(&run(&db, "ALTER TABLE n ALTER COLUMN p TYPE NUMERIC(12,2)").await),
        "a scale change on a populated table was accepted"
    );
    assert!(is_error(&run(&db, "ALTER TABLE n ALTER COLUMN p TYPE NUMERIC").await));
    assert_eq!(ints(&db, "SELECT k FROM n WHERE p = 1.2345").await, vec![Some(1)]);

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(
        ints(&db, "SELECT k FROM n WHERE p = 1.2345").await,
        vec![Some(1)],
        "the new scale did not survive a restart"
    );
}

/// Dropping a constraint is a tombstone written over a logged record. Written
/// around the log, the replay at the next open put the record back, and the
/// dropped constraint was enforced again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_constraint_stays_dropped_across_a_restart() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path()).await;
        ok(&db, "CREATE TABLE u (a INT, b INT, CONSTRAINT u_ab UNIQUE (a, b))").await;
        ok(&db, "INSERT INTO u (a, b) VALUES (1, 1)").await;
        ok(&db, "ALTER TABLE u DROP CONSTRAINT u_ab").await;

        ok(&db, "CREATE TABLE v (a INT UNIQUE, b INT)").await;
        ok(&db, "INSERT INTO v (a, b) VALUES (1, 1)").await;
        ok(&db, "ALTER TABLE v DROP CONSTRAINT v_a_key").await;

        ok(&db, "CREATE TABLE p (id INT)").await;
        ok(
            &db,
            "CREATE TABLE ch (id INT, pid INT, CONSTRAINT fk_ch FOREIGN KEY (pid) REFERENCES p(id))",
        )
        .await;
        ok(&db, "ALTER TABLE ch DROP CONSTRAINT fk_ch").await;

        // Each takes effect at once.
        ok(&db, "INSERT INTO u (a, b) VALUES (1, 1)").await;
        ok(&db, "INSERT INTO v (a, b) VALUES (1, 2)").await;
        ok(&db, "INSERT INTO ch (id, pid) VALUES (1, 99)").await;
        drop(db);
    }
    let db = open(dir.path()).await;
    let u = run(&db, "INSERT INTO u (a, b) VALUES (1, 1)").await;
    let v = run(&db, "INSERT INTO v (a, b) VALUES (1, 3)").await;
    let ch = run(&db, "INSERT INTO ch (id, pid) VALUES (2, 98)").await;
    assert!(!is_error(&u), "the dropped composite UNIQUE came back: {}", String::from_utf8_lossy(&u[1..]));
    assert!(!is_error(&v), "the dropped inline UNIQUE came back: {}", String::from_utf8_lossy(&v[1..]));
    assert!(!is_error(&ch), "the dropped FOREIGN KEY came back: {}", String::from_utf8_lossy(&ch[1..]));
}

/// What the expression rewrite treats as a reference to the renamed column,
/// and what it must leave alone.
#[test]
fn expression_rewrite_renames_references_and_nothing_else() {
    use crate::core::rql::executor::rename_column_references as rewrite;
    let r = |e: &str| rewrite(e, "t", "a", "x");
    // A reference, in any case; a longer word sharing the prefix is not one.
    assert_eq!(r("a > 0 AND ab < A"), "x > 0 AND ab < x");
    assert_eq!(r("(a*2)+a_b"), "(x*2)+a_b");
    // Values, comments and dollar-quoted bodies are not references.
    assert_eq!(r("a <> 'a' AND a <> 'it''s a'"), "x <> 'a' AND x <> 'it''s a'");
    assert_eq!(r("a <> E'\\'a'"), "x <> E'\\'a'");
    assert_eq!(r("a <> $$a$$ AND a <> $q$ a $q$"), "x <> $$a$$ AND x <> $q$ a $q$");
    assert_eq!(r("a > 0 -- a\n"), "x > 0 -- a\n");
    assert_eq!(r("a > 0 /* a */"), "x > 0 /* a */");
    // A function name, a cast's type, another table's column: not references.
    assert_eq!(r("a(a) > 0"), "a(x) > 0");
    assert_eq!(r("b::a = a"), "b::a = x");
    assert_eq!(r("u.a = a AND a.b = 1"), "u.a = x AND a.b = 1");
    // Qualified by the table itself, or quoted: references.
    assert_eq!(r("t.a = 1 AND T . a = 2"), "t.x = 1 AND T . x = 2");
    assert_eq!(r("\"a\" > 0 AND \"A\" > 0 AND \"ab\" > 0"), "\"x\" > 0 AND \"x\" > 0 AND \"ab\" > 0");
    // Numbers are not words, even when they contain the letter.
    assert_eq!(r("a > 1e5 AND a < 0xa"), "x > 1e5 AND x < 0xa");
    // A new name that is not a plain identifier is quoted.
    assert_eq!(rewrite("a > 0", "t", "a", "my col"), "\"my col\" > 0");
}
