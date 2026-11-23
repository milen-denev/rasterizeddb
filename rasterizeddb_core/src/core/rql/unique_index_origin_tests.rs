//! A unique key records whether `CREATE UNIQUE INDEX` made it or `ADD
//! CONSTRAINT … UNIQUE` declared it (`UniqueKey::from_index`), and answers as
//! PostgreSQL 17 does for each:
//!
//! - `DROP INDEX` drops an index; of a constraint's index it is 2BP01;
//! - `ALTER TABLE … DROP CONSTRAINT` and `RENAME CONSTRAINT` of an index are
//!   42704 (a no-op under `IF EXISTS`), never 2BP01 for its foreign keys;
//! - a foreign key onto the index's column keeps it (2BP01) unless
//!   `CASCADE`, which drops the key;
//! - `pg_constraint` has no row for an index, and `pg_indexes` lists it.
//!
//! The mark survives a reopen and `ALTER INDEX … RENAME`, on a plain and a
//! partitioned table. Before it, every unique key was a constraint, so
//! `DROP INDEX` of one `CREATE UNIQUE INDEX` made was always refused.

use std::path::Path;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::vec_into_rows;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "uidx_origin").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

/// As a front end runs it: `CREATE UNIQUE INDEX` rewritten into the key it is
/// kept as first. The error is the engine's text, read for its SQLSTATE as
/// the wire reads it.
async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let rewritten = crate::core::rql::lexer_alter::unique_index_as_constraint(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(String::from_utf8_lossy(out.get(1..).unwrap_or_default()).into_owned());
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The statement fails with `code`, as `sqlstate_for` reports its text.
async fn fails(db: &Arc<Database>, sql: &str, code: &str) -> String {
    let e = try_exec(db, sql).await.err().unwrap_or_else(|| panic!("{sql} must fail with {code}"));
    assert_eq!(crate::pgwire::sqlstate_for(&e), code, "{sql}: {e}");
    e
}

/// `(name, from_index)` of `table`'s live unique keys, sorted.
fn keys(db: &Database, table: &str) -> Vec<(String, bool)> {
    let schema = match db.partitioned_table(table) {
        Some(pt) => pt.shell().schema.clone(),
        None => db.table_for_tests(table).expect("a table").schema.clone(),
    };
    // A composite primary key is a key too, named `<table>_pkey`: not one of
    // these.
    let mut out: Vec<(String, bool)> = schema
        .unique_keys
        .iter()
        .filter(|k| !k.is_deleted && !k.name.ends_with("_pkey"))
        .map(|k| (k.name.clone(), k.from_index))
        .collect();
    out.sort();
    out
}

/// `table`'s foreign keys' names, sorted.
fn foreign_keys(db: &Database, table: &str) -> Vec<String> {
    let schema = db.table_for_tests(table).expect("a table").schema.clone();
    let mut out: Vec<String> = schema.foreign_keys.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
    out.sort();
    out
}

/// One catalogue view's rows, each as `(column, text)` pairs.
async fn view(db: &Arc<Database>, name: &str) -> Vec<Vec<(String, String)>> {
    let temp = super::build_alone(name, db, FROZEN_TX, None).await.unwrap();
    let out = exec(db, &format!("SELECT * FROM {temp}")).await;
    assert_eq!(out.first(), Some(&2u8), "{name}: {}", String::from_utf8_lossy(&out));
    let fields = db.table_for_tests(&temp).unwrap().schema.fields.clone();
    let rows = vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|row| {
            fields
                .iter()
                .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                .map(|f| {
                    let bytes = row.get_column(&f.name, &fields).unwrap();
                    let bytes = bytes.into_slice();
                    let text = match f.db_type {
                        DbType::STRING => String::from_utf8(bytes.to_vec()).unwrap(),
                        DbType::I64 => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
                        DbType::I32 => i32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
                        _ => format!("{bytes:?}"),
                    };
                    (f.name.clone(), text)
                })
                .collect()
        })
        .collect();
    crate::core::rql::cte::drop_temps(db, &[temp], FROZEN_TX).await;
    rows
}

fn get<'r>(row: &'r [(String, String)], column: &str) -> &'r str {
    row.iter().find(|(n, _)| n == column).map(|(_, v)| v.as_str()).unwrap_or_default()
}

/// The names one view lists in `column`, sorted.
async fn listed(db: &Arc<Database>, view_name: &str, column: &str, among: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = view(db, view_name)
        .await
        .iter()
        .map(|r| get(r, column).to_string())
        .filter(|n| among.contains(&n.as_str()))
        .collect();
    out.sort();
    out
}

/// On a plain and a partitioned table: the index and the constraint keep
/// their kinds across a reopen and an `ALTER INDEX … RENAME`, and each
/// answers its own DROP alone.
///
/// Mutations: the mark not written (`into_vec` leaving `ORIGIN_AT` zero):
/// the reopened index is a constraint and `DROP INDEX` is 2BP01; the DROP
/// CONSTRAINT arm not skipping an index: `DROP CONSTRAINT ix` succeeds;
/// `remove_matching` ignoring its filter in DROP CONSTRAINT: not reachable by
/// name alone, covered by the IF EXISTS no-op leaving the index.
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_index_answers_drop_index_and_keeps_its_mark_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d).await;
    exec(&db, "CREATE TABLE pl (a BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT, e BIGINT, f BIGINT, PRIMARY KEY (a, c))")
        .await;
    exec(
        &db,
        "CREATE TABLE pk (a BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT, e BIGINT, f BIGINT, PRIMARY KEY (a, c)) \
         PARTITION BY HASH (a) PARTITIONS 4",
    )
    .await;
    for t in ["pl", "pk"] {
        exec(&db, &format!("INSERT INTO {t} (a, c, d, e) VALUES (1, 10, 100, 1000), (2, 20, 200, 2000)")).await;
        exec(&db, &format!("CREATE UNIQUE INDEX {t}_ix ON {t} (a, d)")).await;
        exec(&db, &format!("CREATE UNIQUE INDEX IF NOT EXISTS \"{t} ix é\" ON {t} (a, c, d)")).await;
        exec(&db, &format!("ALTER TABLE {t} ADD CONSTRAINT {t}_con UNIQUE (a, e)")).await;
        // A partial one is an index too, as it always was.
        exec(&db, &format!("CREATE UNIQUE INDEX {t}_part ON {t} (a, f) WHERE f > 0")).await;
    }
    close(d, db);
    let db = open(d).await;
    for t in ["pl", "pk"] {
        assert_eq!(
            keys(&db, t),
            vec![
                (format!("{t} ix é"), true),
                (format!("{t}_con"), false),
                (format!("{t}_ix"), true),
                (format!("{t}_part"), true),
            ],
            "{t}: the marks after a reopen"
        );
        // PostgreSQL: constraint "pl_ix" of relation "pl" does not exist.
        let e = fails(&db, &format!("ALTER TABLE {t} DROP CONSTRAINT {t}_ix"), "42704").await;
        assert!(e.contains(&format!("constraint \"{t}_ix\" of relation \"{t}\" does not exist")), "{t}: {e}");
        exec(&db, &format!("ALTER TABLE {t} DROP CONSTRAINT IF EXISTS {t}_ix")).await;
        fails(&db, &format!("ALTER TABLE {t} RENAME CONSTRAINT {t}_ix TO {t}_y"), "42704").await;
        // PostgreSQL: cannot drop index pl_con because constraint pl_con on
        // table pl requires it.
        let e = fails(&db, &format!("DROP INDEX {t}_con"), "2BP01").await;
        assert!(e.contains(&format!("because constraint {t}_con on table {t} requires it")), "{t}: {e}");
        fails(&db, &format!("DROP INDEX {t}_con CASCADE"), "2BP01").await;
        assert_eq!(keys(&db, t).len(), 4, "{t}: a refused drop dropped something");
        // All enforced, the partial one among the rows its predicate holds
        // for only (the mark and the predicate share the rewritten clause:
        // read in the wrong order, the predicate was lost and the index held
        // over every row).
        fails(&db, &format!("INSERT INTO {t} (a, c, d, e) VALUES (1, 11, 100, 5)"), "23505").await;
        fails(&db, &format!("INSERT INTO {t} (a, c, d, e) VALUES (1, 12, 5, 1000)"), "23505").await;
        exec(&db, &format!("INSERT INTO {t} (a, c, f) VALUES (1, 30, -1), (1, 31, -1), (1, 32, 7)")).await;
        fails(&db, &format!("INSERT INTO {t} (a, c, f) VALUES (1, 33, 7)"), "23505").await;
        // Renamed as an index, it stays one.
        exec(&db, &format!("ALTER TABLE {t} RENAME INDEX {t}_ix TO {t}_ix2")).await;
    }
    close(d, db);
    let db = open(d).await;
    for t in ["pl", "pk"] {
        assert!(keys(&db, t).contains(&(format!("{t}_ix2"), true)), "{t}: {:?} after the rename", keys(&db, t));
        exec(&db, &format!("DROP INDEX {t}_ix2")).await;
        exec(&db, &format!("DROP INDEX \"{t} ix é\" RESTRICT")).await;
        exec(&db, &format!("DROP INDEX {t}_part")).await;
        // The index is gone, so the row it refused goes in; the constraint
        // stays, and goes by its own DROP.
        exec(&db, &format!("INSERT INTO {t} (a, c, d, e) VALUES (1, 11, 100, 5)")).await;
        assert_eq!(keys(&db, t), vec![(format!("{t}_con"), false)], "{t}");
        exec(&db, &format!("ALTER TABLE {t} DROP CONSTRAINT {t}_con")).await;
        assert!(keys(&db, t).is_empty(), "{t}");
    }
    close(d, db);
    let db = open(d).await;
    for t in ["pl", "pk"] {
        assert!(keys(&db, t).is_empty(), "{t}: a dropped key came back after a reopen: {:?}", keys(&db, t));
    }
    close(d, db);
}

/// A foreign key onto a unique index's column: `DROP INDEX` is 2BP01 without
/// `CASCADE` (RESTRICT too) and drops the key with it; `DROP CONSTRAINT` of
/// the index is 42704, not the 2BP01 its key would give a constraint.
///
/// Mutation: the dependents check skipped in `execute_drop_unique_index`:
/// the plain DROP INDEX succeeds and leaves the key enforced against a
/// column nothing keeps unique.
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_key_keeps_a_unique_index_unless_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d).await;
    exec(&db, "CREATE TABLE p (id BIGINT PRIMARY KEY, k BIGINT)").await;
    exec(&db, "CREATE UNIQUE INDEX p_k_ix ON p (k)").await;
    exec(&db, "CREATE TABLE ch (id BIGINT PRIMARY KEY, pk BIGINT, CONSTRAINT ch_pk_fkey FOREIGN KEY (pk) REFERENCES p (k))").await;
    exec(&db, "INSERT INTO p (id, k) VALUES (1, 10)").await;
    exec(&db, "INSERT INTO ch (id, pk) VALUES (1, 10)").await;
    close(d, db);
    let db = open(d).await;
    // PostgreSQL: cannot drop index p_k_ix because other objects depend on it.
    let e = fails(&db, "DROP INDEX p_k_ix", "2BP01").await;
    assert!(e.contains("cannot drop index p_k_ix because other objects depend on it"), "{e}");
    assert!(e.contains("constraint ch_pk_fkey on table ch depends on index p_k_ix"), "{e}");
    fails(&db, "DROP INDEX p_k_ix RESTRICT", "2BP01").await;
    fails(&db, "ALTER TABLE p DROP CONSTRAINT p_k_ix", "42704").await;
    assert_eq!(keys(&db, "p"), vec![("p_k_ix".to_string(), true)]);
    assert_eq!(foreign_keys(&db, "ch"), vec!["ch_pk_fkey".to_string()]);
    fails(&db, "INSERT INTO ch (id, pk) VALUES (2, 99)", "23503").await;

    exec(&db, "DROP INDEX p_k_ix CASCADE").await;
    assert!(keys(&db, "p").is_empty());
    assert!(foreign_keys(&db, "ch").is_empty(), "CASCADE left {:?}", foreign_keys(&db, "ch"));
    // Neither is enforced any more.
    exec(&db, "INSERT INTO ch (id, pk) VALUES (2, 99)").await;
    exec(&db, "INSERT INTO p (id, k) VALUES (2, 10)").await;
    close(d, db);
    let db = open(d).await;
    assert!(keys(&db, "p").is_empty() && foreign_keys(&db, "ch").is_empty(), "the CASCADE came back after a reopen");
    close(d, db);
}

/// `pg_constraint` lists the constraint and not the index, as PostgreSQL
/// does; `pg_indexes` lists both, each as `CREATE UNIQUE INDEX`.
///
/// Mutation: `table_indexes` making a constraint of every full unique key
/// again: `pl_ix` is in `pg_constraint`.
#[tokio::test(flavor = "multi_thread")]
async fn the_catalogues_list_a_unique_index_as_an_index_only() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d).await;
    exec(&db, "CREATE TABLE pl (a BIGINT PRIMARY KEY, b BIGINT, c BIGINT)").await;
    exec(&db, "CREATE UNIQUE INDEX pl_ix ON pl (b)").await;
    exec(&db, "ALTER TABLE pl ADD CONSTRAINT pl_con UNIQUE (c)").await;
    close(d, db);
    let db = open(d).await;
    let among = ["pl_ix", "pl_con", "pl_pkey"];
    assert_eq!(listed(&db, "pg_constraint", "conname", &among).await, ["pl_con", "pl_pkey"]);
    assert_eq!(listed(&db, "pg_indexes", "indexname", &among).await, ["pl_con", "pl_ix", "pl_pkey"]);
    let indexes = view(&db, "pg_indexes").await;
    let def = indexes.iter().find(|r| get(r, "indexname") == "pl_ix").map(|r| get(r, "indexdef").to_string());
    assert_eq!(def.as_deref(), Some("CREATE UNIQUE INDEX pl_ix ON public.pl USING btree (b)"));
    close(d, db);
}
