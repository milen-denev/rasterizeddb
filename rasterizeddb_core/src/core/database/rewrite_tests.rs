//! A DDL's writes to one table, all or nothing (slice B5, `rewrite.rs`):
//! each multi-write ALTER arm, failed after all its writes, leaves the
//! table's schema exactly as it was, before and after a reopen, and the
//! statement then runs again cleanly; a rewrite failed part-way through its
//! fill leaves the old rows and shape. The power-cut case is
//! `power_loss::twin_tests::rewrite_crash_tests`.

use std::path::Path;

use rclite::Arc;

use super::relation_dispatch_tests::open;
use super::*;
use crate::core::power_loss::failpoint;

/// Closes `db` so the directory can be opened again in this process.
fn shut(db: Arc<Database>, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&root.to_string_lossy());
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

/// Everything a metadata ALTER can change in a table's schema, as text.
fn fingerprint(db: &Database, table: &str) -> String {
    let view = db.schema_view(table).unwrap_or_else(|| panic!("{table} is missing"));
    let s = view.schema();
    let fields: Vec<String> = s
        .fields
        .iter()
        .map(|f| format!("{}:{:?}:{}", f.name, f.db_type, if f.is_deleted { "dead" } else { "live" }))
        .collect();
    let defaults: Vec<String> = s
        .defaults
        .iter()
        .filter(|d| !d.is_deleted)
        .map(|d| format!("{}={}", d.column, d.expression))
        .collect();
    let keys: Vec<String> = s.unique_keys.iter().filter(|k| !k.is_deleted).map(|k| k.describe()).collect();
    let modifiers: Vec<String> = s
        .type_modifiers
        .iter()
        .filter(|m| !m.is_deleted)
        .map(|m| format!("{}({},{})", m.column, m.precision, m.scale))
        .collect();
    format!("fields {fields:?}\ndefaults {defaults:?}\nkeys {keys:?}\nmodifiers {modifiers:?}")
}

fn failing(dir: &Path, label: &str) -> failpoint::Guard {
    failpoint::arm(
        &format!("test.rewrite.{label}"),
        dir,
        1,
        std::sync::Arc::new(|_hit: &failpoint::Hit<'_>| {
            failpoint::Outcome::Fail(std::io::Error::other("injected failure"))
        }),
    )
}

/// Each multi-write ALTER arm B5 moved into the reload (ADD COLUMN on an
/// empty table, DROP COLUMN with a partial unique index on the column,
/// RENAME COLUMN, SET NOT NULL, a NUMERIC scale change, and RENAME TO's
/// sequence defaults), failed after every one of its writes: the statement
/// reports the failure, the table's schema is what it was, in the open
/// instance and after a reopen, no intent is left, and the same statement
/// then succeeds (nothing half-applied stands in its way as "already
/// exists" or "does not exist").
///
/// Mutation: the restore puts nothing back (`restore_from_backup` returns
/// at once); every arm's writes stay and the fingerprints differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_metadata_alter_writes_nothing() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let db = Arc::new(open(root).await);
    for sql in [
        "CREATE TABLE m_add (id INT PRIMARY KEY)",
        "CREATE TABLE m_drop (id INT PRIMARY KEY, d INT DEFAULT 4, e INT)",
        "ALTER TABLE m_drop ADD CONSTRAINT m_drop_d UNIQUE (d) WHERE e > 0",
        "CREATE TABLE m_ren (id INT PRIMARY KEY, d INT DEFAULT 4)",
        "CREATE TABLE m_nn (id INT PRIMARY KEY, d INT)",
        "INSERT INTO m_nn (id, d) VALUES (1, 1)",
        "CREATE TABLE m_num (id INT PRIMARY KEY, d NUMERIC(10,2))",
        "CREATE TABLE m_seq (id SERIAL PRIMARY KEY, x INT)",
    ] {
        exec(&db, sql).await.unwrap();
    }
    let cases: &[(&str, &str)] = &[
        ("m_add", "ALTER TABLE m_add ADD COLUMN c NUMERIC(10,2) DEFAULT 1.5"),
        ("m_drop", "ALTER TABLE m_drop DROP COLUMN d"),
        ("m_ren", "ALTER TABLE m_ren RENAME COLUMN d TO dd"),
        ("m_nn", "ALTER TABLE m_nn ALTER COLUMN d SET NOT NULL"),
        ("m_num", "ALTER TABLE m_num ALTER COLUMN d TYPE NUMERIC(12,4)"),
    ];
    let mut before = Vec::new();
    for (table, sql) in cases {
        let was = fingerprint(&db, table);
        let guard = failing(root, "metadata_after_work");
        let out = exec(&db, sql).await;
        assert_eq!(guard.fired(), 1, "{sql}: the failure point was not reached: {out:?}\n{was}");
        drop(guard);
        assert!(out.is_err(), "{sql}: reported success over an injected failure");
        assert_eq!(fingerprint(&db, table), was, "{sql}: a failed statement left writes behind");
        before.push(was);
    }
    // RENAME TO: the rename is its own crash-safe step; its sequence defaults
    // follow as one reload, all or none.
    let seq_was = fingerprint(&db, "m_seq");
    let guard = failing(root, "metadata_after_work");
    let out = exec(&db, "ALTER TABLE m_seq RENAME TO m_seq2").await;
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert!(out.is_err(), "RENAME TO reported success over an injected failure");
    assert_eq!(
        fingerprint(&db, "m_seq2"),
        seq_was,
        "a failed sequence-default follow left some defaults moved"
    );

    // Through a reopen, which also finds no intent to act on.
    shut(db, root);
    let leftover: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("__rewrite_") || n.starts_with(rewrite::BACKUP_PREFIX))
        .collect();
    assert!(leftover.is_empty(), "a failed statement left {leftover:?}");
    let db = Arc::new(open(root).await);
    for ((table, sql), was) in cases.iter().zip(&before) {
        assert_eq!(&fingerprint(&db, table), was, "{sql}: the reopened table differs");
    }
    assert_eq!(fingerprint(&db, "m_seq2"), seq_was);

    // And each statement runs cleanly now.
    for (table, sql) in cases {
        exec(&db, sql).await.unwrap_or_else(|e| panic!("{e}"));
        let _ = table;
    }
    shut(db, root);
}

/// A rewrite failed part-way through writing the new table's rows: the
/// statement fails, and the table has its old shape and every old row, in
/// the open instance and after a reopen; the ALTER then succeeds.
///
/// Mutation: the restore puts nothing back; the table opens new-shaped with
/// one row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_rewrite_leaves_the_old_table() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let db = Arc::new(open(root).await);
    exec(&db, "CREATE TABLE r_fail (id INT PRIMARY KEY, v TEXT)").await.unwrap();
    for i in 0..10 {
        exec(&db, &format!("INSERT INTO r_fail (id, v) VALUES ({i}, 'v{i}')")).await.unwrap();
    }
    let count = |db: &Database| {
        let t = db.table_for_tests("r_fail").unwrap().clone();
        (t.schema.fields.iter().filter(|f| !f.is_deleted).count(), t)
    };
    let (columns_before, _) = count(&db);
    let guard = failing(root, "mid_fill");
    let out = exec(&db, "ALTER TABLE r_fail ADD COLUMN extra INT DEFAULT 1").await;
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert!(out.is_err(), "the rewrite reported success over an injected failure");
    let check = |db: &Database| {
        let (columns, t) = count(db);
        assert_eq!(columns, columns_before, "the failed rewrite changed the table's shape");
        t
    };
    let rows = |t: Arc<Table<DbStorageProvider>>| async move {
        let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&t.schema.fields);
        t.query_row("", &t.schema.fields.to_vec(), Table::<DbStorageProvider>::build_fetch_for(&t.schema.fields, &[]), fetch, None, None)
            .await
            .unwrap()
            .len()
    };
    assert_eq!(rows(check(&db)).await, 10, "the failed rewrite lost rows");
    shut(db, root);
    let db = Arc::new(open(root).await);
    assert_eq!(rows(check(&db)).await, 10, "the failed rewrite lost rows across a reopen");
    exec(&db, "ALTER TABLE r_fail ADD COLUMN extra INT DEFAULT 1").await.unwrap();
    let (columns, t) = count(&db);
    assert!(columns > columns_before);
    assert_eq!(rows(t).await, 10);
    shut(db, root);
}

/// `ALTER COLUMN … SET NOT NULL` enforces NOT NULL and keeps the column's
/// default. It used to be parsed as `DROP DEFAULT` (the bare `SET` that
/// `SET DEFAULT` leaves once its clause is split off), so it dropped the
/// default, enforced nothing and reported success.
///
/// Mutation: the lexer matches any detail starting with `SET` again; the
/// NULL insert after it is accepted and the default is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_not_null_enforces_and_keeps_the_default() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let db = Arc::new(open(root).await);
    exec(&db, "CREATE TABLE nn (id INT PRIMARY KEY, d INT DEFAULT 4)").await.unwrap();
    exec(&db, "INSERT INTO nn (id, d) VALUES (1, NULL)").await.unwrap();
    let e = exec(&db, "ALTER TABLE nn ALTER COLUMN d SET NOT NULL").await.expect_err("SET NOT NULL over a NULL");
    assert!(e.contains("null values"), "{e}");
    exec(&db, "DELETE FROM nn WHERE id = 1").await.unwrap();
    exec(&db, "INSERT INTO nn (id, d) VALUES (2, 3)").await.unwrap();
    exec(&db, "ALTER TABLE nn ALTER COLUMN d SET NOT NULL").await.unwrap();
    assert!(
        fingerprint(&db, "nn").contains("\"d=4\""),
        "SET NOT NULL dropped the default:\n{}",
        fingerprint(&db, "nn")
    );
    assert!(
        !fingerprint(&db, "nn").contains("__null_d:BOOL:live"),
        "SET NOT NULL left the column nullable:\n{}",
        fingerprint(&db, "nn")
    );
    exec(&db, "INSERT INTO nn (id) VALUES (3)").await.unwrap();
    shut(db, root);
}

/// The `ALTER COLUMN … TYPE … USING` silent-drop guard still holds with the
/// rewrites moved into the reload: a same-type restatement carrying a USING
/// transform is refused rather than reported as a no-op with the transform
/// never run, and the rows are untouched; the same restatement without USING
/// (what sea-orm sends) is still accepted.
///
/// Mutation: the USING refusal removed; the statement succeeds and the
/// values are not multiplied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_type_using_is_still_refused_not_dropped() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let db = Arc::new(open(root).await);
    exec(&db, "CREATE TABLE u (id INT PRIMARY KEY, v INT)").await.unwrap();
    exec(&db, "INSERT INTO u (id, v) VALUES (1, 2)").await.unwrap();
    let was = fingerprint(&db, "u");
    let e = exec(&db, "ALTER TABLE u ALTER COLUMN v TYPE INT USING (v * 100)")
        .await
        .expect_err("a USING transform was accepted and not run");
    assert!(e.contains("USING"), "{e}");
    assert_eq!(fingerprint(&db, "u"), was);
    exec(&db, "ALTER TABLE u ALTER COLUMN v TYPE INT").await.expect("a same-type restatement");
    shut(db, root);
}

/// A rewrite keeps the table's declared PRIMARY KEY in its schema header.
/// It used to recreate the table with none, so after an `ADD COLUMN` on a
/// table with rows a client asking which column is the key got no answer.
///
/// Mutation: the rewrite's definition carries `primary_key: None` again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rewrite_keeps_the_primary_key() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path();
    let db = Arc::new(open(root).await);
    exec(&db, "CREATE TABLE pk (id INT PRIMARY KEY, v INT)").await.unwrap();
    exec(&db, "INSERT INTO pk (id, v) VALUES (1, 2)").await.unwrap();
    let key = |db: &Database| db.schema_view("pk").unwrap().schema().primary_key.clone();
    assert_eq!(key(&db).as_deref(), Some("id"));
    exec(&db, "ALTER TABLE pk ADD COLUMN extra INT DEFAULT 3").await.unwrap();
    assert_eq!(key(&db).as_deref(), Some("id"), "the rewrite dropped the PRIMARY KEY");
    shut(db, root);
}
