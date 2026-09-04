//! Per-table storage backends, end to end.
//!
//! `placement_tests.rs` covers a table's files landing on the directory the pool
//! chose. These cover the next step: one database holding tables that are not in
//! a directory at all, and the file-backed features declining to act on them.

use itertools::Itertools;
use rclite::Arc;
use smallvec::SmallVec;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::row::row::{ColumnFetchingData, RowFetch};
use crate::core::row::schema::{SchemaCalculator, SchemaField};
use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};

use super::TableBackend;

fn create_statement(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![
            CreateColumnData {
                name: "id".to_string(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "label".to_string(),
                data_type: DbType::STRING,
            },
        ],
        unique_columns: Vec::new(),
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_keys: Vec::new(),
        type_modifiers: Vec::new(),
        generated: Vec::new(),
        checks: Vec::new(),
        primary_key: None,
        if_not_exists: false,
        is_immutable: false,
    }
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "backendtests").await)
}

async fn insert(db: &Arc<Database>, table: &str, id: i32, label: &str) {
    let sql = format!(
        "INSERT INTO {} (id, label) VALUES ({}, '{}')",
        table, id, label
    );
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let result = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(!result.is_empty(), "insert produced no result: {sql}");
    db.sync_durable().expect("sync");
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator;
    let mut cols = SmallVec::new();
    for f in fields
        .iter()
        .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
    {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    RowFetch {
        columns_fetching_data: cols,
    }
}

async fn row_count(db: &Arc<Database>, table: &str) -> usize {
    let handle = db.tables.get(table).expect("table is not in the live map");
    let fields = handle.schema.fields.to_vec();
    handle
        .query_row(
            "",
            &fields,
            full_fetch(&handle.schema.fields),
            full_fetch(&handle.schema.fields),
            None,
            None,
        )
        .await
        .unwrap()
        .len()
}

/// Files whose name begins with `table`, anywhere under `dir`.
fn files_for(dir: &std::path::Path, table: &str) -> Vec<String> {
    fn walk(dir: &std::path::Path, table: &str, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                walk(&path, table, out);
            } else if name.starts_with(table) {
                out.push(name);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, table, &mut out);
    out.sort();
    out
}

fn write_kv_config(dir: &std::path::Path, store_dir: &std::path::Path) {
    // Tab-separated, like `db_storage.cfg`.
    let text = format!(
        "# test stores\nversion\t1\nstore\tlocal\tdir\t{}\trdb\n",
        store_dir.to_string_lossy()
    );
    std::fs::write(dir.join(super::kv_registry::KV_FILE), text).unwrap();
}

// ---------------------------------------------------------------------------
// The spec
// ---------------------------------------------------------------------------

#[test]
fn a_bare_path_is_still_a_directory() {
    // Every `db_storage.cfg` written before backends existed holds bare paths,
    // and must keep meaning what it meant.
    assert_eq!(
        TableBackend::parse("/mnt/disk2"),
        TableBackend::Directory("/mnt/disk2".to_string())
    );
    // A drive letter is not a scheme. Reading `C:` as one would turn every
    // Windows placement into an unknown backend.
    assert!(matches!(
        TableBackend::parse("C:\\data\\rdb"),
        TableBackend::Directory(_)
    ));
}

#[test]
fn the_other_backends_round_trip_through_their_spec() {
    for spec in ["memory:", "kv:local", "kv:archive-2"] {
        let parsed = TableBackend::parse(spec);
        assert_eq!(
            parsed.as_spec(),
            spec,
            "{spec} did not survive a round trip"
        );
        assert!(!parsed.is_file_based(), "{spec} claimed to be file-backed");
        assert!(parsed.directory().is_none(), "{spec} produced a directory");
    }
    assert_eq!(
        TableBackend::parse("KV:local"),
        TableBackend::KeyValue("local".to_string())
    );
    assert_eq!(TableBackend::parse("MEMORY:"), TableBackend::Memory);
    // A scheme with nothing after it is not a store name; it is a path.
    assert!(matches!(
        TableBackend::parse("kv:"),
        TableBackend::Directory(_)
    ));
}

// ---------------------------------------------------------------------------
// Tables that are not in a directory
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_table_placed_in_memory_writes_no_files() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    db.storage
        .pin_backend("scratch", &TableBackend::Memory)
        .unwrap();
    db.add_table(create_statement("scratch")).await.unwrap();
    insert(&db, "scratch", 1, "one").await;
    insert(&db, "scratch", 2, "two").await;

    assert_eq!(row_count(&db, "scratch").await, 2);
    assert_eq!(
        files_for(root.path(), "scratch"),
        Vec::<String>::new(),
        "an in-memory table put files in the database directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_placed_on_a_key_value_store_survives_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    {
        let db = open(root.path()).await;
        db.storage
            .pin_backend("archived", &TableBackend::KeyValue("local".to_string()))
            .unwrap();
        db.add_table(create_statement("archived")).await.unwrap();
        for id in 0..25 {
            insert(&db, "archived", id, &format!("row-{id}")).await;
        }
        assert_eq!(row_count(&db, "archived").await, 25);
    }

    assert_eq!(
        files_for(root.path(), "archived"),
        Vec::<String>::new(),
        "the table's bytes should be in the store, not the database directory"
    );
    assert!(
        std::fs::read_dir(store.path()).unwrap().next().is_some(),
        "the store directory is empty, so nothing was written to it"
    );

    let db = open(root.path()).await;
    assert_eq!(
        row_count(&db, "archived").await,
        25,
        "the rows did not survive the restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_database_holds_all_three_backends_at_once() {
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    let db = open(root.path()).await;
    db.storage
        .pin_backend(
            "kept",
            &TableBackend::Directory(root.path().to_string_lossy().into_owned()),
        )
        .unwrap();
    db.storage
        .pin_backend("scratch", &TableBackend::Memory)
        .unwrap();
    db.storage
        .pin_backend("archived", &TableBackend::KeyValue("local".to_string()))
        .unwrap();

    for table in ["kept", "scratch", "archived"] {
        db.add_table(create_statement(table)).await.unwrap();
        insert(&db, table, 1, table).await;
        insert(&db, table, 2, table).await;
    }

    for table in ["kept", "scratch", "archived"] {
        assert_eq!(row_count(&db, table).await, 2, "{table} did not read back");
    }
    assert!(
        !files_for(root.path(), "kept").is_empty(),
        "the file table wrote nothing"
    );
    assert!(files_for(root.path(), "scratch").is_empty());
    assert!(files_for(root.path(), "archived").is_empty());
}

// ---------------------------------------------------------------------------
// The file-backed features decline
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_that_has_no_files_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;
    db.storage
        .add_location(
            &second.path().to_string_lossy(),
            super::LocationRole::Primary,
            1,
            true,
        )
        .unwrap();

    db.storage
        .pin_backend("scratch", &TableBackend::Memory)
        .unwrap();
    db.add_table(create_statement("scratch")).await.unwrap();

    let error = super::relocate::move_table(&db, "scratch", &second.path().to_string_lossy())
        .await
        .expect_err("a table with no files was moved between directories");
    assert!(
        error.contains("in-memory") && error.contains("no files"),
        "the refusal should say why, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn placing_a_table_on_a_store_that_is_not_configured_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let error = super::sql::execute("STORAGE PLACE TABLE t AT 'kv:nowhere'", &db)
        .await
        .expect_err("a pin to an unconfigured store was accepted");
    assert!(
        error.contains("nowhere"),
        "the refusal should name the store, got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_whose_store_vanished_is_absent_rather_than_empty() {
    // The failure this guards: the table opens against nothing, answers every
    // query with no rows, and reads exactly like a table someone emptied.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    {
        let db = open(root.path()).await;
        db.storage
            .pin_backend("archived", &TableBackend::KeyValue("local".to_string()))
            .unwrap();
        db.add_table(create_statement("archived")).await.unwrap();
        insert(&db, "archived", 1, "one").await;
    }

    std::fs::remove_file(root.path().join(super::kv_registry::KV_FILE)).unwrap();
    let db = open(root.path()).await;
    assert!(
        db.tables.get("archived").is_none(),
        "a table whose store is gone must not open as an empty one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_reused_on_a_different_backend_does_not_inherit_the_old_one() {
    // The seed for a non-directory table is cached, because these backends have
    // identity — an in-memory table's buffers live in the registry its seed
    // created. A name outlives that: drop the table, pin the name somewhere
    // else, recreate it, and a cache keyed on the name alone hands back a seed
    // for the backend the placement no longer names. Nothing later notices,
    // because every read of the wrongly-built table is consistent with itself.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    {
        let db = open(root.path()).await;

        db.storage
            .pin_backend("twice", &TableBackend::Memory)
            .unwrap();
        db.add_table(create_statement("twice")).await.unwrap();
        insert(&db, "twice", 1, "in-memory").await;
        assert_eq!(row_count(&db, "twice").await, 1);

        db.drop_table("twice").await.unwrap();

        // Same name, different backend.
        db.storage
            .pin_backend("twice", &TableBackend::KeyValue("local".to_string()))
            .unwrap();
        db.add_table(create_statement("twice")).await.unwrap();
        insert(&db, "twice", 2, "persistent").await;
        assert_eq!(row_count(&db, "twice").await, 1);
    }

    // The proof is the restart. Built on the stale in-memory seed, the row was
    // never in the store and comes back to nothing.
    let db = open(root.path()).await;
    assert_eq!(
        row_count(&db, "twice").await,
        1,
        "the recreated table was built on the backend it used to be on, so its \
         rows were never persisted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_seed_cached_before_the_placement_changed_is_not_reused() {
    // The other half of the same hazard, and the half no drop can clean up
    // after: a seed can be cached for a table that does not exist yet, because
    // several paths resolve a name before anything is created. `table_bytes` is
    // one, and a rebalance calls it. Re-pin the name afterwards and a cache
    // keyed on the name alone hands back the seed for the old placement.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    {
        let db = open(root.path()).await;
        db.storage
            .pin_backend("later", &TableBackend::Memory)
            .unwrap();
        // Resolves the name, and caches a seed, without creating anything.
        let _ = db.table_bytes("later").await;

        db.storage
            .pin_backend("later", &TableBackend::KeyValue("local".to_string()))
            .unwrap();
        db.add_table(create_statement("later")).await.unwrap();
        insert(&db, "later", 1, "persistent").await;
        assert_eq!(row_count(&db, "later").await, 1);
    }

    let db = open(root.path()).await;
    assert_eq!(
        row_count(&db, "later").await,
        1,
        "the table was built on the seed cached before the placement changed, so \
         its rows were never persisted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_carries_the_backend_across() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    db.storage
        .pin_backend("before", &TableBackend::Memory)
        .unwrap();
    assert_eq!(db.storage.backend_of("before"), Some(TableBackend::Memory));

    db.storage.rename("before", "after");
    assert_eq!(
        db.storage.backend_of("after"),
        Some(TableBackend::Memory),
        "a rename lost the backend, so the table would resolve to the database directory"
    );
    assert!(db.storage.backend_of("before").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_directory_placement_survives_the_pool_file() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = open(root.path()).await;
        db.storage
            .pin_backend("scratch", &TableBackend::Memory)
            .unwrap();
        db.storage
            .pin_backend("archived", &TableBackend::KeyValue("local".to_string()))
            .unwrap();
    }
    let manager = super::StorageManager::open(&root.path().to_string_lossy());
    assert_eq!(manager.backend_of("scratch"), Some(TableBackend::Memory));
    assert_eq!(
        manager.backend_of("archived"),
        Some(TableBackend::KeyValue("local".to_string()))
    );
    // And neither is mistaken for a directory by the paths that expect one.
    assert!(manager.location_of("scratch").is_none());
    assert!(manager.location_of("archived").is_none());
}
