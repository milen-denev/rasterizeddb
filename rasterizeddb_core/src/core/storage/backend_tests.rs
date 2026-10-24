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
    let handle = db.table_for_tests(table).expect("table is not in the live map");
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
async fn a_rebalance_leaves_a_table_that_has_no_files_alone() {
    // A rebalance balances *disks*. A table that occupies none of one is not a
    // candidate: counting it would have the policy move a table whose bytes are
    // not where it thinks they are. `STORAGE MOVE TABLE` still converts it —
    // that is a thing an operator asked for by name.
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
    db.storage.set_policy(super::PlacementPolicy::RoundRobin);

    db.storage
        .pin_backend("scratch", &TableBackend::Memory)
        .unwrap();
    db.add_table(create_statement("scratch")).await.unwrap();
    insert(&db, "scratch", 1, "one").await;

    let lines = super::relocate::rebalance(&db, false).await.unwrap();
    assert!(
        !lines.iter().any(|line| line.contains("scratch")),
        "a rebalance planned a move for a table with no files: {lines:?}"
    );
    assert_eq!(db.storage.backend_of("scratch"), Some(TableBackend::Memory));
    assert_eq!(row_count(&db, "scratch").await, 1);
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
        db.table_for_tests("archived").is_none(),
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

// ---------------------------------------------------------------------------
// Converting a table from one backend to another
// ---------------------------------------------------------------------------

/// Runs a `STORAGE ...` statement and returns its lines.
async fn storage(db: &Arc<Database>, sql: &str) -> Vec<String> {
    super::sql::execute(sql, db)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_converts_between_every_backend_and_keeps_its_rows() {
    // The whole point of the backend being a spec: one statement moves a table
    // from files to memory to a key/value store and back, and the rows are the
    // same rows at every step. The copy is `StorageIO` to `StorageIO`, so the
    // thing that could break is not the bytes — it is a seed resolved from the
    // placement it had *before* the flip, which reads as a table that arrives
    // empty.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    let db = open(root.path()).await;
    db.add_table(create_statement("wander")).await.unwrap();
    for id in 0..12 {
        insert(&db, "wander", id, &format!("row-{id}")).await;
    }
    assert_eq!(row_count(&db, "wander").await, 12);
    assert!(
        !files_for(root.path(), "wander").is_empty(),
        "the table did not start on files"
    );

    let root_path = root.path().to_string_lossy().into_owned();
    for (target, expected) in [
        ("memory:", TableBackend::Memory),
        ("kv:local", TableBackend::KeyValue("local".to_string())),
        ("memory:", TableBackend::Memory),
        (
            root_path.as_str(),
            TableBackend::Directory(root_path.clone()),
        ),
        ("kv:local", TableBackend::KeyValue("local".to_string())),
    ] {
        let lines = storage(&db, &format!("STORAGE MOVE TABLE wander TO '{target}'")).await;
        assert!(
            db.has_table("wander"),
            "after moving to {target} the table is not in the live map; move said {lines:?}"
        );
        assert!(
            lines
                .first()
                .is_some_and(|line| line.starts_with("moved wander")),
            "moving to {target} reported {lines:?}"
        );
        assert_eq!(
            db.storage.backend_of("wander"),
            Some(expected.clone()),
            "the placement did not follow the move to {target}"
        );
        assert_eq!(
            row_count(&db, "wander").await,
            12,
            "the rows did not survive the move to {target}"
        );
    }

    // It ends on the store, so the restart is what proves the last copy was
    // real rather than a live handle still serving the old bytes.
    assert_eq!(
        files_for(root.path(), "wander"),
        Vec::<String>::new(),
        "files were left in the database directory"
    );
    drop(db);
    let db = open(root.path()).await;
    assert_eq!(
        row_count(&db, "wander").await,
        12,
        "the rows did not survive the restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_off_files_deletes_the_files_it_left() {
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    let db = open(root.path()).await;
    db.add_table(create_statement("emigrant")).await.unwrap();
    insert(&db, "emigrant", 1, "one").await;
    assert!(!files_for(root.path(), "emigrant").is_empty());

    storage(&db, "STORAGE MOVE TABLE emigrant TO 'kv:local'").await;

    assert_eq!(
        files_for(root.path(), "emigrant"),
        Vec::<String>::new(),
        "the originals were left behind, so the next open would find two copies"
    );
    assert_eq!(row_count(&db, "emigrant").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_move_says_which_file_backed_features_the_table_lost() {
    // The part of a conversion that is invisible afterwards. Rows read back
    // identically on every backend, and the first hint that a table is no
    // longer in any backup would otherwise be a restore that does not have it.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    let db = open(root.path()).await;
    db.add_table(create_statement("told")).await.unwrap();
    insert(&db, "told", 1, "one").await;

    let lines = storage(&db, "STORAGE MOVE TABLE told TO 'kv:local'").await;
    assert!(
        lines.iter().any(|line| line.contains("backup")),
        "moving off files said nothing about backups: {lines:?}"
    );

    let lines = storage(&db, "STORAGE MOVE TABLE told TO 'memory:'").await;
    assert!(
        lines
            .iter()
            .any(|line| line.contains("does not survive a restart")),
        "moving into memory said nothing about losing the rows: {lines:?}"
    );

    let back = root.path().to_string_lossy().into_owned();
    let lines = storage(&db, &format!("STORAGE MOVE TABLE told TO '{back}'")).await;
    assert!(
        lines.iter().any(|line| line.contains("file-backed")),
        "moving back onto files said nothing about regaining them: {lines:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_onto_a_store_that_is_not_configured_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;
    db.add_table(create_statement("staying")).await.unwrap();
    insert(&db, "staying", 1, "one").await;

    let error = super::sql::execute("STORAGE MOVE TABLE staying TO 'kv:nowhere'", &db)
        .await
        .expect_err("a move onto an unconfigured store was accepted");
    assert!(
        error.contains("nowhere"),
        "the refusal should name the store, got: {error}"
    );
    // And the table is untouched: the check happens before anything is copied
    // and before the handle leaves the live map.
    assert_eq!(row_count(&db, "staying").await, 1);
}

// ---------------------------------------------------------------------------
// Defining stores from SQL
// ---------------------------------------------------------------------------

fn kv_config_text(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join(super::kv_registry::KV_FILE)).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_added_by_sql_is_usable_and_survives_a_restart() {
    // The reason this has to write the file at all: a store that exists only in
    // this process is one every table placed on it stops finding at the next
    // start-up — and a placement naming a store that is gone stops the whole
    // database opening.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();

    {
        let db = open(root.path()).await;
        let lines = storage(
            &db,
            &format!(
                "STORAGE ADD STORE fast DIRECTORY WITH (PATH = '{}', PREFIX = 'rdb', \
                 CHUNK_SIZE = '1MiB', CACHE_BYTES = '8MiB')",
                store.path().to_string_lossy()
            ),
        )
        .await;
        assert!(lines[0].contains("fast"), "{lines:?}");

        storage(&db, "STORAGE PLACE TABLE ledger AT 'kv:fast'").await;
        db.add_table(create_statement("ledger")).await.unwrap();
        insert(&db, "ledger", 1, "one").await;
        assert_eq!(row_count(&db, "ledger").await, 1);
    }

    let text = kv_config_text(root.path());
    assert!(
        text.contains("version\t1"),
        "the file has no version line: {text:?}"
    );
    assert!(
        text.contains("store\tfast\tdir\t"),
        "the store was not written: {text:?}"
    );
    assert!(
        text.contains("chunk_size=1048576"),
        "the settings were not written: {text:?}"
    );

    let db = open(root.path()).await;
    assert_eq!(
        row_count(&db, "ledger").await,
        1,
        "the store did not come back, so its table did not either"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_s3_store_round_trips_through_the_file_without_being_contacted() {
    // Defining an S3 store must not need the network: the definition is
    // configuration, and it is proved by the first table placed on it. This
    // pins that, and that every field survives the file.
    let root = tempfile::tempdir().unwrap();
    {
        let db = open(root.path()).await;
        storage(
            &db,
            "STORAGE ADD STORE archive S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
             REGION = 'us-east-1', BUCKET = 'rasterized', PREFIX = 'rdb/archive', \
             ADDRESSING = 'path', PAGE_SIZE = '32KiB')",
        )
        .await;
    }

    let db = open(root.path()).await;
    let stores = db.kv.list();
    let archive = stores
        .iter()
        .find(|s| s.name == "archive")
        .expect("the store did not come back");
    assert_eq!(archive.kind, "s3");
    assert_eq!(archive.endpoint, "http://127.0.0.1:9000");
    assert_eq!(archive.region, "us-east-1");
    assert_eq!(archive.bucket, "rasterized");
    assert_eq!(archive.prefix, "rdb/archive");
    assert_eq!(archive.addressing, "path");
    assert_eq!(archive.settings, "page_size=32768");
    assert!(!archive.opened, "defining a store must not open it");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_keeps_its_credentials_out_of_the_readable_config() {
    // A store may carry its own credentials, and where they land is the point:
    // `db_kv.cfg` is rewritten by the server, quoted in errors and readable by
    // anyone who can list the directory, so a secret in it is a secret in the
    // logs. They go to a second file, mode 0600.
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let lines = storage(
        &db,
        "STORAGE ADD STORE vault S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         BUCKET = 'b', ACCESS_KEY_ID = 'AKIAEXAMPLE', SECRET_ACCESS_KEY = 'sh-hunter2', \
         SESSION_TOKEN = 'tok-abc')",
    )
    .await;
    assert!(
        !lines.iter().any(|line| line.contains("hunter2")),
        "the statement echoed the secret back: {lines:?}"
    );

    let config = kv_config_text(root.path());
    assert!(
        config.contains("store\tvault\ts3\t"),
        "the store was not written: {config:?}"
    );
    assert!(
        !config.contains("hunter2"),
        "the secret reached {}: {config:?}",
        super::kv_registry::KV_FILE
    );
    assert!(
        !config.contains("AKIAEXAMPLE"),
        "the key id reached {}",
        super::kv_registry::KV_FILE
    );

    let secrets_path = root.path().join(super::kv_registry::KV_SECRETS_FILE);
    let secrets = std::fs::read_to_string(&secrets_path).expect("the secrets file was not written");
    assert!(
        secrets.contains("hunter2"),
        "the secret was not stored, so the store cannot sign"
    );
    assert!(
        secrets.contains("tok-abc"),
        "the session token was not stored"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&secrets_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the secrets file is readable by someone else");
    }

    // And nothing that reports on stores reports the secret.
    let shown = storage(&db, "SHOW STORAGE").await;
    assert!(
        !shown.iter().any(|line| line.contains("hunter2")),
        "{shown:?}"
    );
    assert!(
        shown.iter().any(|line| line.contains("store credentials")),
        "SHOW STORAGE did not say the store carries its own: {shown:?}"
    );
    assert!(
        format!("{:?}", db.kv).find("hunter2").is_none(),
        "Debug printed the secret"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_survive_a_restart_and_can_be_rotated() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = open(root.path()).await;
        storage(
            &db,
            "STORAGE ADD STORE vault S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
             BUCKET = 'b', ACCESS_KEY_ID = 'first-id', SECRET_ACCESS_KEY = 'first-secret')",
        )
        .await;
    }

    let db = open(root.path()).await;
    assert!(
        db.kv.has_credentials("vault"),
        "the credentials did not survive the restart"
    );
    let listed = db.kv.list();
    assert_eq!(listed[0].credentials, "store");

    // Rotating a key moves no bytes, so unlike an address it is allowed while
    // tables are placed on the store — which is the case an operator with a
    // leaked key is actually in.
    storage(&db, "STORAGE PLACE TABLE ledger AT 'kv:vault'").await;
    let lines = storage(
        &db,
        "STORAGE ALTER STORE vault SET ACCESS_KEY_ID = 'second-id', \
         SECRET_ACCESS_KEY = 'second-secret'",
    )
    .await;
    assert!(lines[0].contains("credentials"), "{lines:?}");

    let secrets =
        std::fs::read_to_string(root.path().join(super::kv_registry::KV_SECRETS_FILE)).unwrap();
    assert!(
        secrets.contains("second-secret"),
        "the rotation was not written"
    );
    assert!(
        !secrets.contains("first-secret"),
        "the old secret is still on disk"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn half_a_credential_is_refused_rather_than_stored() {
    // A secret with no key id cannot sign anything. Storing it would leave a
    // store that looks configured and fails at its first request, by which time
    // the failure reads as the endpoint being wrong.
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    for (options, expected) in [
        ("ACCESS_KEY_ID = 'only-id'", "SECRET_ACCESS_KEY"),
        ("SECRET_ACCESS_KEY = 'only-secret'", "ACCESS_KEY_ID"),
        ("SESSION_TOKEN = 'only-token'", "ACCESS_KEY_ID"),
    ] {
        // `expect_err`, not `unwrap_or_else`: refusing this statement *is* the
        // behaviour under test, so an `Err` is the pass. Written the other way
        // round, the assertion fired exactly when the code was right — it
        // panicked with the refusal message it had just been handed.
        let error = super::sql::execute(
            &format!(
                "STORAGE ADD STORE partial S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
                 BUCKET = 'b', {options})"
            ),
            &db,
        )
        .await
        .expect_err(&format!("{options} should have been refused"));
        // And the message has to name the half that is missing. `expected` was
        // bound and never read, so the test would have passed on any refusal at
        // all — including one that named the wrong field, which is the version
        // of this an operator cannot act on.
        assert!(
            error.contains(expected),
            "{options} was refused without naming the missing {expected}: {error}"
        );
    }
    assert!(
        !db.kv.has("partial"),
        "a half credential defined a store anyway"
    );
}

/// A geometry the store could never open with is refused by the statement.
///
/// `KvConfig::validate` catches it, but only when a provider is opened — which
/// is the first time a table is placed on the store, long after the statement
/// that got it wrong. In between, everything reported success: the `ADD STORE`
/// returned ok, `SHOW STORAGE STORES` listed the store, and tables placed on it
/// appeared to accept writes and reads. The only tells were `opened = f` and one
/// line in the server log.
///
/// 4096000 is the real value that caused it: decimal four million, which looks
/// like 4MB and is not a whole number of 65,536-byte pages. 4194304 — 4MiB — is.
#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_size_that_is_not_whole_pages_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let error = super::sql::execute(
        "STORAGE ADD STORE cold S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         BUCKET = 'b', CHUNK_SIZE = '4096000')",
        &db,
    )
    .await
    .expect_err("a chunk size that is not whole pages was accepted");

    // The message has to name the setting and offer a way out: the cause is
    // almost always a decimal/binary mix-up, and the operator cannot tell from
    // "invalid" which direction to round.
    assert!(
        error.contains("CHUNK_SIZE"),
        "the setting is not named: {error}"
    );
    assert!(
        error.contains("4194304"),
        "the nearest valid size is not offered: {error}"
    );
    assert!(
        !db.kv.has("cold"),
        "the store was defined despite being unopenable"
    );

    // The same size written as 4MiB is exactly 64 pages and must be accepted,
    // so the check refuses the mistake rather than the feature.
    super::sql::execute(
        "STORAGE ADD STORE warm S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         BUCKET = 'b', CHUNK_SIZE = '4MiB')",
        &db,
    )
    .await
    .expect("4MiB is a whole number of pages and must be accepted");
    assert!(db.kv.has("warm"), "a valid store was not defined");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_directory_store_refuses_credentials_rather_than_ignoring_them() {
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let error = super::sql::execute(
        &format!(
            "STORAGE ADD STORE local DIRECTORY WITH (PATH = '{}', \
             ACCESS_KEY_ID = 'k', SECRET_ACCESS_KEY = 's')",
            store.path().to_string_lossy()
        ),
        &db,
    )
    .await
    .expect_err("a directory store accepted credentials it can never use");
    assert!(error.contains("signs nothing"), "got: {error}");
    assert!(!error.contains('s') || !error.contains("SECRET_ACCESS_KEY = 's'"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_with_a_table_on_it_cannot_be_dropped_or_repointed() {
    // Both are the same hazard from two directions. Dropping the store leaves a
    // placement naming nothing, which stops the database opening; repointing it
    // leaves the table reading a prefix that has none of its bytes — and that
    // one is silent.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    storage(
        &db,
        &format!(
            "STORAGE ADD STORE busy DIRECTORY WITH (PATH = '{}')",
            store.path().to_string_lossy()
        ),
    )
    .await;
    storage(&db, "STORAGE PLACE TABLE books AT 'kv:busy'").await;
    db.add_table(create_statement("books")).await.unwrap();
    insert(&db, "books", 1, "one").await;

    let error = super::sql::execute("STORAGE DROP STORE busy", &db)
        .await
        .expect_err("a store with a table on it was dropped");
    assert!(
        error.contains("books"),
        "the refusal should name the table, got: {error}"
    );

    let error = super::sql::execute(
        &format!(
            "STORAGE ALTER STORE busy SET PATH = '{}'",
            elsewhere.path().to_string_lossy()
        ),
        &db,
    )
    .await
    .expect_err("a store under a live table was repointed");
    assert!(
        error.contains("books"),
        "the refusal should name the table, got: {error}"
    );

    // A tuning change is allowed, because it cannot move where the bytes are.
    let lines = storage(&db, "STORAGE ALTER STORE busy SET CACHE_BYTES = '16MiB'").await;
    assert!(lines[0].contains("cache_bytes"), "{lines:?}");
    assert!(
        lines.iter().any(|line| line.contains("already open")),
        "an open store's settings changed with no word about when they apply: {lines:?}"
    );

    // And once the table has moved off, the store is droppable.
    storage(
        &db,
        &format!(
            "STORAGE MOVE TABLE books TO '{}'",
            root.path().to_string_lossy()
        ),
    )
    .await;
    storage(&db, "STORAGE DROP STORE busy").await;
    assert!(!db.kv.has("busy"));
    assert_eq!(row_count(&db, "books").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_this_build_cannot_open_survives_a_rewrite() {
    // The trap this closes: a kind we do not understand used to parse into a
    // stub that kept only its name, so one `STORAGE ADD STORE` rewrote every
    // such store in the file as an empty line — and the tables on them stopped
    // opening, on a server that had never been asked to touch them.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(super::kv_registry::KV_FILE),
        "version\t1\nstore\tfuture\tazure\thttps://acct.blob.core.windows.net\tvault\trdb\n",
    )
    .unwrap();

    let db = open(root.path()).await;
    storage(
        &db,
        &format!(
            "STORAGE ADD STORE now DIRECTORY WITH (PATH = '{}')",
            store.path().to_string_lossy()
        ),
    )
    .await;

    let text = kv_config_text(root.path());
    assert!(
        text.contains("store\tfuture\tazure\thttps://acct.blob.core.windows.net\tvault\trdb"),
        "the unreadable store lost its fields when the file was rewritten: {text:?}"
    );
    assert!(text.contains("store\tnow\tdir\t"), "{text:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_store_file_is_never_rewritten() {
    // The registry is inert both when the file is absent and when it cannot be
    // parsed, and the two must not be confused by anything that writes: an
    // `ADD STORE` onto a file we failed to read would replace an operator's
    // stores with the single one they just typed.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    // No version line, which is how a file written with spaces instead of tabs
    // fails.
    let broken = "store archive s3 https://example.invalid us-east-1 bucket\n";
    std::fs::write(root.path().join(super::kv_registry::KV_FILE), broken).unwrap();

    let db = open(root.path()).await;
    let error = super::sql::execute(
        &format!(
            "STORAGE ADD STORE new DIRECTORY WITH (PATH = '{}')",
            store.path().to_string_lossy()
        ),
        &db,
    )
    .await
    .expect_err("a store was added on top of a file that could not be read");
    assert!(error.contains("could not be read"), "got: {error}");
    assert_eq!(
        kv_config_text(root.path()),
        broken,
        "the unreadable file was rewritten"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn show_storage_stores_reports_what_was_defined() {
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    assert!(super::sql::is_structured("SHOW STORAGE STORES"));
    storage(
        &db,
        &format!(
            "STORAGE ADD STORE reported DIRECTORY WITH (PATH = '{}', PREFIX = 'p')",
            store.path().to_string_lossy()
        ),
    )
    .await;

    let fields = super::sql::structured_fields("SHOW STORAGE STORES").unwrap();
    assert_eq!(fields[0].0, "store");
    let bytes = super::sql::execute_structured("SHOW STORAGE STORES", &db).unwrap();
    assert!(bytes.len() > 1, "SHOW STORAGE STORES returned no rows");

    // And the prose form lists it beside the directories, because "where are
    // this database's bytes" has had two answers since a table could be on a
    // store.
    let lines = storage(&db, "SHOW STORAGE").await;
    assert!(
        lines.iter().any(|line| line.starts_with("kv:reported")),
        "SHOW STORAGE did not mention the store: {lines:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_converted_into_memory_does_not_wedge_the_next_start_up() {
    // `MOVE TABLE ... TO 'memory:'` makes a state that used to need a
    // hand-written placement to reach: a catalogue entry for a table whose
    // bytes are gone by the time anything reads them. Whatever the answer is,
    // the database has to open — a scratch table taking the server down with it
    // is a far worse trade than an empty one.
    let root = tempfile::tempdir().unwrap();
    {
        let db = open(root.path()).await;
        db.add_table(create_statement("volatile")).await.unwrap();
        insert(&db, "volatile", 1, "one").await;
        storage(&db, "STORAGE MOVE TABLE volatile TO 'memory:'").await;
        assert_eq!(row_count(&db, "volatile").await, 1);
    }

    let db = open(root.path()).await;
    assert_eq!(
        db.storage.backend_of("volatile"),
        Some(TableBackend::Memory),
        "the placement should still say where the operator put it"
    );
    if db.table_for_tests("volatile").is_some() {
        assert_eq!(
            row_count(&db, "volatile").await,
            0,
            "the rows came back from memory"
        );
    }
}

/// Runs a statement through the executor, as a client would.
///
/// Deliberately without a `sync_durable`: a client does not issue one, and a
/// move that only works on a table whose writes have all reached the main file
/// works in a test and not in production.
async fn sql(db: &Arc<Database>, statement: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(statement)
        .unwrap_or_else(|| panic!("could not parse {statement}"));
    let result = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(
        !result.is_empty(),
        "statement produced no result: {statement}"
    );
}

/// The bytes of one of a table's sidecars, or an empty vec if it has none.
async fn sidecar(db: &Arc<Database>, table: &str, suffix: &str) -> Vec<u8> {
    use crate::core::storage_providers::traits::StorageIO;
    let seed = crate::core::database::seed_for_tests(db, table).await;
    let io = seed.create_new(format!("{table}{suffix}")).await;
    let len = io.get_len().await;
    if len == 0 {
        return Vec::new();
    }
    let mut buffer = vec![0u8; len as usize];
    io.read_data_into_buffer(&mut 0, &mut buffer)
        .await
        .expect("read the sidecar");
    buffer
}

#[tokio::test(flavor = "multi_thread")]
async fn a_move_carries_every_sidecar_verbatim() {
    // A table is not its rows. Its CHECK constraints, generated columns and
    // path indexes are files beside the rows, and a move that loses one — or
    // fills it with another file's bytes — produces a table that reads
    // correctly and enforces nothing. That failure is invisible to any test
    // that counts rows.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    write_kv_config(root.path(), store.path());

    let db = open(root.path()).await;
    db.storage
        .add_location(
            &second.path().to_string_lossy(),
            super::LocationRole::Primary,
            1,
            true,
        )
        .unwrap();
    // Defined the way an operator would, rather than with the bare defaults:
    // a prefix with a path separator in it and a chunk size that is not the
    // default are both things a real store has and a test store usually does
    // not.
    storage(
        &db,
        &format!(
            "STORAGE ADD STORE tuned DIRECTORY WITH (PATH = '{}', PREFIX = 'rdb/archive', \
             CHUNK_SIZE = '1MiB')",
            store.path().to_string_lossy()
        ),
    )
    .await;
    sql(
        &db,
        "CREATE TABLE checked (id INT PRIMARY KEY, qty INT NOT NULL CHECK (qty > 0))",
    )
    .await;
    sql(&db, "INSERT INTO checked (id, qty) VALUES (1, 5)").await;

    let suffixes = ["_schema.db", "_schema_FIELDS.db", "_schema_CHK.db"];
    let mut before = Vec::new();
    for suffix in suffixes {
        let bytes = sidecar(&db, "checked", suffix).await;
        assert!(
            !bytes.is_empty(),
            "checked{suffix} is empty before any move"
        );
        before.push(bytes);
    }

    for (next, target) in [
        "kv:tuned".to_string(),
        "kv:local".to_string(),
        second.path().to_string_lossy().into_owned(),
        "memory:".to_string(),
        root.path().to_string_lossy().into_owned(),
    ]
    .into_iter()
    .enumerate()
    {
        let next = next + 2;
        storage(&db, &format!("STORAGE MOVE TABLE checked TO '{target}'")).await;
        for (index, suffix) in suffixes.iter().enumerate() {
            assert_eq!(
                sidecar(&db, "checked", suffix).await,
                before[index],
                "checked{suffix} changed on the way to {target}"
            );
        }
        // The bytes being right is not the constraint being enforced: the
        // sidecar is read by the *reopened* table, and a reopen that resolves
        // it to the wrong file reads a valid record out of another file's
        // bytes. Only a write that should fail proves the difference.
        let violating = format!("INSERT INTO checked (id, qty) VALUES ({next}, -1)");
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&violating).unwrap();
        let result = crate::core::rql::executor::execute(purpose, db.clone()).await;
        let text = String::from_utf8_lossy(&result).to_string();
        assert!(
            text.to_lowercase().contains("check"),
            "on {target} the CHECK constraint was not enforced: {text}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn add_store_if_not_exists_is_issuable_on_every_start_up() {
    // What a deployment that configures its own store needs: no good moment
    // exists to decide whether this is the first start, and wrapping the
    // statement in a SHOW and a branch is a race as well as a nuisance.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let define = "STORAGE ADD STORE IF NOT EXISTS archive S3 WITH \
         (ENDPOINT = 'http://127.0.0.1:9000', REGION = 'us-east-1', BUCKET = 'b', \
          PREFIX = 'rdb/archive', ADDRESSING = 'path')";
    let first = storage(&db, define).await;
    assert!(first[0].contains("added"), "{first:?}");
    let second = storage(&db, define).await;
    assert!(
        second[0].contains("already exists"),
        "a re-issue was not a no-op: {second:?}"
    );
    assert_eq!(
        db.kv.names(),
        vec!["archive".to_string()],
        "the re-issue defined a second store"
    );

    // A definition that disagrees is refused rather than silently leaving the
    // old address in place — that is the case where a deployment has been
    // repointed and nobody would find out until a read came back empty.
    let error = super::sql::execute(
        "STORAGE ADD STORE IF NOT EXISTS archive S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         REGION = 'us-east-1', BUCKET = 'somewhere-else', PREFIX = 'rdb/archive', \
         ADDRESSING = 'path')",
        &db,
    )
    .await
    .expect_err("a changed bucket was accepted as 'already exists'");
    assert!(
        error.contains("BUCKET"),
        "the refusal should name the field, got: {error}"
    );

    // Credentials are still applied on the idempotent path: re-issuing with a
    // key from the environment is how a deployment rotates one.
    let rotated = storage(
        &db,
        "STORAGE ADD STORE IF NOT EXISTS archive S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         REGION = 'us-east-1', BUCKET = 'b', PREFIX = 'rdb/archive', ADDRESSING = 'path', \
         ACCESS_KEY_ID = 'rotated-id', SECRET_ACCESS_KEY = 'rotated-secret')",
    )
    .await;
    assert!(
        rotated
            .iter()
            .any(|line| line.contains("credentials were replaced")),
        "{rotated:?}"
    );
    assert!(db.kv.has_credentials("archive"));

    // And it works for a directory store too, which is what a test deployment
    // uses before it has a bucket.
    let define_dir = format!(
        "STORAGE ADD STORE IF NOT EXISTS local DIRECTORY WITH (PATH = '{}')",
        store.path().to_string_lossy()
    );
    storage(&db, &define_dir).await;
    let again = storage(&db, &define_dir).await;
    assert!(again[0].contains("already exists"), "{again:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_credential_containing_punctuation_survives_the_parser() {
    // `WITH (...)` used to split on every comma and to find its own keyword
    // with `rfind`, neither of which looked inside a quoted value. A secret is
    // exactly the kind of value that contains punctuation nobody thought about,
    // and the failure is silent: the value is truncated, the remainder is read
    // as another option, and the store is configured with a key that will never
    // authenticate.
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let secret = "a,b WITH c/d+e=";
    storage(
        &db,
        &format!(
            "STORAGE ADD STORE awkward S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
             BUCKET = 'b', ACCESS_KEY_ID = 'AKIA,X', SECRET_ACCESS_KEY = '{secret}')"
        ),
    )
    .await;

    let secrets =
        std::fs::read_to_string(root.path().join(super::kv_registry::KV_SECRETS_FILE)).unwrap();
    assert!(
        secrets.contains(secret),
        "the secret was mangled on the way in, so the store cannot ever authenticate"
    );
    assert!(
        secrets.contains("AKIA,X"),
        "the key id was truncated at its comma"
    );
    // The endpoint must not have been shifted by the ` WITH ` inside the value.
    let listed = db.kv.list();
    assert_eq!(listed[0].endpoint, "http://127.0.0.1:9000");
    assert_eq!(listed[0].bucket, "b");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unclosed_quote_is_refused_without_quoting_the_value() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    let error = super::sql::execute(
        "STORAGE ADD STORE broken S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
         BUCKET = 'b', SECRET_ACCESS_KEY = 'never-log-me)",
        &db,
    )
    .await
    .expect_err("an unterminated quote was accepted");
    assert!(error.contains("not closed"), "got: {error}");
    assert!(
        !error.contains("never-log-me"),
        "the parse error put the secret in the message, and an error message is a log line"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rotating_a_key_reaches_disk_even_while_the_store_is_open() {
    // The difference between "rotation needs a restart" and "rotation needs a
    // restart *and* a re-issue after it". The running server keeps signing with
    // what it opened with, but the new key has to be what the next start reads
    // — otherwise a rotation performed during an incident is silently undone by
    // the restart that was supposed to apply it.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    // A directory store, because opening an S3 one needs a network. What is
    // under test is the registry's persistence, which does not know the kind.
    storage(
        &db,
        &format!(
            "STORAGE ADD STORE live DIRECTORY WITH (PATH = '{}')",
            store.path().to_string_lossy()
        ),
    )
    .await;
    storage(&db, "STORAGE PLACE TABLE held AT 'kv:live'").await;
    db.add_table(create_statement("held")).await.unwrap();
    insert(&db, "held", 1, "one").await;
    assert!(
        db.kv.is_open("live"),
        "the store should be open, or this proves nothing"
    );

    db.kv
        .set_credentials(
            "live",
            Some(super::kv_registry::StoreCredentials {
                access_key: "rotated-id".to_string(),
                secret_key: "rotated-secret".to_string(),
                session_token: None,
            }),
        )
        .expect("rotating while open");

    let secrets =
        std::fs::read_to_string(root.path().join(super::kv_registry::KV_SECRETS_FILE)).unwrap();
    assert!(
        secrets.contains("rotated-secret"),
        "the rotation did not reach disk, so the next restart would use the old key"
    );

    // And it is still there after a reopen: one restart applies it, with no
    // second statement needed.
    drop(db);
    let db = open(root.path()).await;
    assert!(db.kv.has_credentials("live"));
    assert_eq!(row_count(&db, "held").await, 1);
}

// The `ADD STORE` half of this check lives in `kv_sql::check_tunable_geometry`
// and is covered by `a_chunk_size_that_cannot_open_is_refused` above. What
// follows covers `ALTER STORE`, which does not go through `split_options` and
// so is not reached by that check at all — the registry validates the merged
// entry instead, which is also the only place that can see a stored
// `PAGE_SIZE` the statement did not mention.

#[tokio::test(flavor = "multi_thread")]
async fn altering_one_size_is_judged_against_the_other() {
    // `CHUNK_SIZE` and `PAGE_SIZE` are only valid as a pair, so an ALTER of one
    // has to be checked beside whatever the other already is — not against the
    // change in isolation, and not against the defaults.
    let root = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    storage(
        &db,
        &format!(
            "STORAGE ADD STORE paired DIRECTORY WITH (PATH = '{}', CHUNK_SIZE = '4MiB', \
             PAGE_SIZE = '64KiB')",
            store.path().to_string_lossy()
        ),
    )
    .await;

    // 96 KiB divides neither 4 MiB nor anything else here.
    let error = super::sql::execute("STORAGE ALTER STORE paired SET PAGE_SIZE = '96KiB'", &db)
        .await
        .expect_err("a page size that does not divide the stored chunk size was accepted");
    assert!(error.contains("multiple"), "got: {error}");

    // And the store keeps the settings it had — a refused ALTER must not leave
    // a half-applied one behind.
    let listed = db.kv.list();
    assert!(
        listed[0].settings.contains("page_size=65536"),
        "the refused change was applied anyway: {}",
        listed[0].settings
    );

    // A page size that does divide it is accepted.
    storage(&db, "STORAGE ALTER STORE paired SET PAGE_SIZE = '128KiB'").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_storage_statements_are_refused_not_misread() {
    // `REBALANCE PARTITIONS` used to run a whole-database REBALANCE and report
    // success, and `MOVE PARTITION t …` read as a table named "PARTITION".
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;
    for sql in [
        "STORAGE REBALANCE PARTITIONS",
        "STORAGE REBALANCE PARTITIONS DRY RUN",
        "STORAGE MOVE PARTITION t FOR (k = 42) TO '/nonexistent'",
    ] {
        let error = super::sql::execute(sql, &db)
            .await
            .expect_err(&format!("{sql} was accepted"));
        assert!(error.contains("not supported"), "{sql}: {error}");
    }
    super::sql::execute("STORAGE REBALANCE DRY RUN", &db)
        .await
        .expect("a plain dry run still works");
}
