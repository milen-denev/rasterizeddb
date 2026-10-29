//! A `CREATE TABLE` that fails before its catalogue row (`create_intent`):
//! nothing is catalogued, its files go, and what was decided before it stays.
//! A `STORAGE PLACE TABLE` pin outlives it (the retry lands where the pin
//! says); a placement the `CREATE` chose itself goes with its files. And a
//! schema or field record that cannot be written fails the `CREATE`, on a
//! local directory and on a key/value store, rather than leaving a table that
//! loses a column, or its whole schema, at the next open.

use std::path::Path;

use rclite::Arc;

use super::Database;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&path_str(dir), false, "create_failure").await.unwrap())
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

async fn count(db: &Arc<Database>, table: &str) -> usize {
    let out = exec(db, &format!("SELECT * FROM {table}")).await.unwrap();
    assert_eq!(out.first(), Some(&2), "not a row set: {}", String::from_utf8_lossy(&out));
    crate::core::row::row::vec_into_rows(&out[1..]).unwrap().len()
}

/// `table`'s files in `dir` that hold anything (an empty one is reused).
fn files_of(dir: &Path, table: &str) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.metadata().is_ok_and(|m| m.len() > 0))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("{table}.")) || n.starts_with(&format!("{table}_")))
        .collect();
    out.sort();
    out
}

#[cfg(feature = "storage_pools")]
fn add_disk(db: &Arc<Database>, disk: &Path) -> String {
    std::fs::create_dir_all(disk).unwrap();
    db.storage
        .add_location(&path_str(disk), crate::core::storage::LocationRole::Primary, 1, true)
        .unwrap()
}

/// The store `local`, a directory under `base`, for the database at `root`.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
fn write_kv_config(root: &Path, store: &Path) {
    std::fs::create_dir_all(store).unwrap();
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(
        root.join(crate::core::storage::kv_registry::KV_FILE),
        format!("# test stores\nversion\t1\nstore\tlocal\tdir\t{}\trdb\n", path_str(store)),
    )
    .unwrap();
}

/// A CREATE that fails at its catalogue row keeps a pin set before it (a
/// directory and a key/value store), and the retry lands there; one that
/// chose its own placement forgets it. On one thread, so the injected
/// failure reaches the CREATE.
///
/// Mutation: `create_intent::discard` forgetting the placement whatever the
/// intent says (the pin's spec is gone and the retry lands in the root).
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
#[tokio::test(flavor = "current_thread")]
async fn a_failed_create_keeps_a_pin_and_forgets_its_own_placement() {
    use crate::core::database::test_hooks::FAIL_CATALOGUE_ROW;
    use crate::core::storage::TableBackend;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let disk = base.path().join("disk2");
    write_kv_config(&root, &base.path().join("kvstore"));
    let db = open(&root).await;
    let pin = add_disk(&db, &disk);
    db.storage.pin("t", &pin).unwrap();
    db.storage.pin_backend("k", &TableBackend::KeyValue("local".to_string())).unwrap();

    for (name, spec) in [("t", Some(pin.clone())), ("k", Some("kv:local".to_string())), ("u", None)] {
        FAIL_CATALOGUE_ROW.with(|f| f.set(true));
        let failed = exec(&db, &format!("CREATE TABLE {name} (id BIGINT NOT NULL, c BIGINT CHECK (c > 0))")).await;
        assert!(!FAIL_CATALOGUE_ROW.with(|f| f.replace(false)), "{name}: the CREATE never reached its catalogue row");
        assert!(failed.is_err(), "{name}: a CREATE whose catalogue row failed succeeded");
        assert!(!db.has_table(name), "{name}: the failed CREATE left a table");
        assert_eq!(db.storage.spec_of(name), spec, "{name}: the failed CREATE changed the placement");
    }
    assert_eq!(files_of(&disk, "t"), Vec::<String>::new(), "the failed CREATE kept files on the pinned disk");
    assert_eq!(files_of(&root, "t"), Vec::<String>::new());
    assert_eq!(files_of(&root, "u"), Vec::<String>::new());

    // The retries land where the pins say.
    for name in ["t", "k", "u"] {
        exec(&db, &format!("CREATE TABLE {name} (id BIGINT NOT NULL, c BIGINT CHECK (c > 0))")).await.unwrap();
        exec(&db, &format!("INSERT INTO {name} (id, c) VALUES (1, 1)")).await.unwrap();
    }
    assert_eq!(db.storage.location_of("t").as_deref(), Some(pin.as_str()), "the retry did not land on the pin");
    assert!(matches!(db.storage.backend_of("k"), Some(TableBackend::KeyValue(s)) if s == "local"));
    assert!(!files_of(&disk, "t").is_empty(), "the retry wrote nothing on the pinned disk");
    close(&root, db);
    let db = open(&root).await;
    for name in ["t", "k", "u"] {
        assert_eq!(count(&db, name).await, 1, "{name} lost its row across a restart");
    }
    close(&root, db);
}

/// A CREATE whose schema header or a field record cannot be written (a full
/// disk, a file-size limit) fails, leaves nothing catalogued and no file in
/// the way, and the CREATE runs again. It used to succeed: the write's
/// failure was logged, the live schema kept the column, and the next open
/// read a table without it, or without a schema at all.
///
/// Mutation: `TableSchema::save` or `SchemaField::save_field` back to an
/// append whose failure is only logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_whose_schema_cannot_be_written_creates_nothing() {
    use crate::core::storage_providers::file_sync::failed_appends::FAIL_APPEND_TO;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let db = open(&root).await;
    // Names no other test uses: the hook is process-wide.
    for (name, file) in [("cfail_header", "_schema.db"), ("cfail_field", "_schema_FIELDS.db")] {
        let target = format!("{name}{file}");
        FAIL_APPEND_TO.lock().push(target.clone());
        let failed = exec(&db, &format!("CREATE TABLE {name} (id BIGINT NOT NULL, v BIGINT)")).await;
        let armed = FAIL_APPEND_TO.lock().iter().any(|t| *t == target);
        FAIL_APPEND_TO.lock().retain(|t| *t != target);
        assert!(!armed, "{name}: the CREATE never appended to {file}");
        assert!(failed.is_err(), "{name}: a CREATE whose {file} could not be written succeeded");
        assert!(!db.has_table(name), "{name}: the failed CREATE left a table");
        assert_eq!(files_of(&root, name), Vec::<String>::new(), "{name}: the failed CREATE kept its files");
        exec(&db, &format!("CREATE TABLE {name} (id BIGINT NOT NULL, v BIGINT)")).await.unwrap();
        exec(&db, &format!("INSERT INTO {name} (id, v) VALUES (1, 2)")).await.unwrap();
    }
    close(&root, db);
    let db = open(&root).await;
    for name in ["cfail_header", "cfail_field"] {
        assert_eq!(count(&db, name).await, 1, "{name} lost its row across a restart");
        let t = db.table_held(name).unwrap();
        let columns: Vec<String> = t.schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
        assert!(columns.iter().any(|c| c == "v"), "{name} reopened without its column v: {columns:?}");
    }
    close(&root, db);
}

/// A CREATE on a key/value store that refuses its puts fails, rather than
/// succeeding over a schema the store never held: the schema and field puts
/// used to be logged on failure and dropped. Once the store takes puts
/// again the CREATE runs, and its rows survive a restart. Skipped where the
/// store cannot be made read-only (running as root).
///
/// Mutation: the same as above, the saves' failure only logged.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_on_a_store_that_refuses_its_puts_creates_nothing() {
    use crate::core::storage::TableBackend;
    use std::os::unix::fs::PermissionsExt;
    fn set_mode(dir: &Path, read_only: bool) {
        for entry in walk(dir) {
            let mode = match (entry.is_dir(), read_only) {
                (true, true) => 0o555,
                (true, false) => 0o755,
                (false, true) => 0o444,
                (false, false) => 0o644,
            };
            let _ = std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(mode));
        }
    }
    fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut out = vec![dir.to_path_buf()];
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.filter_map(|e| e.ok()) {
                let p = e.path();
                if p.is_dir() {
                    out.extend(walk(&p));
                } else {
                    out.push(p);
                }
            }
        }
        out
    }
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let store = base.path().join("kvstore");
    write_kv_config(&root, &store);
    let db = open(&root).await;
    // The store's family opened by a table already on it, so the refused
    // puts are the CREATE's own.
    db.storage.pin_backend("warm", &TableBackend::KeyValue("local".to_string())).unwrap();
    exec(&db, "CREATE TABLE warm (id BIGINT NOT NULL, v BIGINT)").await.unwrap();
    db.storage.pin_backend("k", &TableBackend::KeyValue("local".to_string())).unwrap();

    set_mode(&store, true);
    if std::fs::write(store.join("probe"), b"x").is_ok() {
        set_mode(&store, false);
        eprintln!("the store cannot be made read-only here (root?); skipped");
        close(&root, db);
        return;
    }
    let failed = exec(&db, "CREATE TABLE k (id BIGINT NOT NULL, v BIGINT)").await;
    set_mode(&store, false);
    assert!(failed.is_err(), "a CREATE whose store refused its puts succeeded");
    assert!(!db.has_table("k"), "the failed CREATE left a table");
    assert_eq!(db.storage.spec_of("k").as_deref(), Some("kv:local"), "the failed CREATE forgot the pin");

    exec(&db, "CREATE TABLE k (id BIGINT NOT NULL, v BIGINT)").await.unwrap();
    for i in 0..3 {
        exec(&db, &format!("INSERT INTO k (id, v) VALUES ({i}, {i})")).await.unwrap();
    }
    close(&root, db);
    let db = open(&root).await;
    assert_eq!(count(&db, "k").await, 3, "the CREATE after the store recovered lost its rows across a restart");
    assert!(db.has_table("warm"));
    close(&root, db);
}

/// `STORAGE PLACE TABLE` is refused while a statement holds the name (a
/// CREATE between its name check and its install), and runs once it is
/// released: a pin set meanwhile landed after the CREATE's files had gone
/// where the old placement said.
///
/// Mutation: `place_table` no longer taking the name's claim.
#[cfg(feature = "storage_pools")]
#[tokio::test]
async fn a_placement_waits_for_no_statement_holding_the_name() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let disk = base.path().join("disk2");
    let db = open(&root).await;
    let pin = add_disk(&db, &disk);
    let sql = format!("STORAGE PLACE TABLE racing AT '{pin}'");
    let held = db.claim_name("racing").unwrap();
    let refused = crate::core::storage::sql::execute(&sql, &db).await;
    assert!(refused.is_err(), "a placement went in while a statement held the name: {refused:?}");
    assert_eq!(db.storage.spec_of("racing"), None);
    drop(held);
    crate::core::storage::sql::execute(&sql, &db).await.unwrap();
    assert_eq!(db.storage.spec_of("racing").as_deref(), Some(pin.as_str()));
    close(&root, db);
}

/// An ALTER that rewrites the table and cannot write its new schema (a field
/// record refused) fails and puts the old files back: the columns and rows
/// are as they were, across a restart, and the ALTER runs again. It reported
/// success over an empty schema, every row with no column.
///
/// Mutation: `Database::create_files` no longer refusing a table whose
/// schema could not be written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rewrite_whose_schema_cannot_be_written_puts_the_table_back() {
    use crate::core::storage_providers::file_sync::failed_appends::FAIL_APPEND_TO;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let db = open(&root).await;
    let name = "rwfail_t";
    exec(&db, &format!("CREATE TABLE {name} (id BIGINT NOT NULL, v BIGINT)")).await.unwrap();
    exec(&db, &format!("INSERT INTO {name} (id, v) VALUES (1, 10), (2, 20), (3, 30)")).await.unwrap();
    let columns = |db: &Arc<Database>| -> Vec<String> {
        db.table_held(name).unwrap().schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect()
    };
    let before = columns(&db);
    let target = format!("{name}_schema_FIELDS.db");
    FAIL_APPEND_TO.lock().push(target.clone());
    let failed = exec(&db, &format!("ALTER TABLE {name} ADD COLUMN extra BIGINT DEFAULT 5")).await;
    let armed = FAIL_APPEND_TO.lock().iter().any(|t| *t == target);
    FAIL_APPEND_TO.lock().retain(|t| *t != target);
    assert!(!armed, "the rewrite never appended a field record");
    assert!(failed.is_err(), "a rewrite whose field record could not be written succeeded");
    assert_eq!(columns(&db), before, "the failed rewrite changed the columns");
    assert_eq!(count(&db, name).await, 3);
    close(&root, db);
    let db = open(&root).await;
    assert_eq!(columns(&db), before, "the failed rewrite's columns came back after a restart");
    let out = exec(&db, &format!("SELECT v FROM {name} WHERE id = 2")).await.unwrap();
    let rows = crate::core::row::row::vec_into_rows(&out[1..]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[0].data.into_slice(), 20i64.to_le_bytes().as_slice(), "the row's value changed");
    exec(&db, &format!("ALTER TABLE {name} ADD COLUMN extra BIGINT DEFAULT 5")).await.unwrap();
    assert!(columns(&db).iter().any(|c| c == "extra"), "the retried ALTER added nothing");
    assert_eq!(count(&db, name).await, 3);
    close(&root, db);
}

/// A first open whose catalogue schema cannot be written (a full disk)
/// returns an error, as `try_new_named` promises, rather than panicking in
/// the catalogue's `save` after `add_field` dropped the field that failed;
/// the next open, the disk recovered, makes the catalogue and works.
///
/// Mutation: `load_db_data_schema` back to `add_field`, the failure logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_open_that_cannot_write_its_catalogue_is_an_error() {
    use crate::core::storage_providers::file_sync::failed_appends::FAIL_APPEND_TO;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    // This directory's own catalogue only: the hook is process-wide.
    let target = format!(
        "{}/db/db_data_schema_FIELDS.db",
        base.path().file_name().unwrap().to_string_lossy()
    );
    FAIL_APPEND_TO.lock().push(target.clone());
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let opened = Database::try_new_named(&path_str(&root), false, "create_failure").await;
    let armed = FAIL_APPEND_TO.lock().iter().any(|t| *t == target);
    FAIL_APPEND_TO.lock().retain(|t| *t != target);
    crate::core::wal::remove_for(&path_str(&root));
    assert!(!armed, "the first open never wrote the catalogue's fields");
    assert!(opened.is_err(), "a first open whose catalogue could not be written opened");
    drop(opened);

    let db = open(&root).await;
    exec(&db, "CREATE TABLE after_bootstrap (id BIGINT NOT NULL)").await.unwrap();
    exec(&db, "INSERT INTO after_bootstrap (id) VALUES (1)").await.unwrap();
    close(&root, db);
    let db = open(&root).await;
    assert_eq!(count(&db, "after_bootstrap").await, 1);
    close(&root, db);
}
