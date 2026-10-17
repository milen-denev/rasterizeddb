//! A log that recovery refuses must refuse the database, not be stepped over.
//!
//! Ported from feat/partitioning d9679e2. `open_and_recover` refuses some logs
//! outright and leaves them on disk: one it cannot open or read, or whose
//! directory it cannot list (on feat/partitioning also one naming a file id
//! two files claim, which this branch does not detect yet, so the refusal here
//! is an unreadable log). Startup used to log that and carry on without a log.
//! The database then took writes onto files the unreplayed records still
//! described, and once the condition was cleared the next open replayed those
//! older records over the newer bytes and reported nothing: acknowledged rows
//! gone, earlier ones back.
//!
//! The scenario is built from real files. A first session commits row 1, a
//! second commits row 2; the second's file images go into a hand-written log
//! and the data files are put back as they were after the first, which is the
//! disk after a crash that left row 2 only in the log. Recovery is then made
//! to refuse the log as it refuses an unreadable one, through a path-keyed
//! test hook rather than mode 000, which refuses nothing to root and would let
//! every test here pass without testing anything. Each open runs on its own
//! thread and runtime, because a panicking open has to be observed rather than
//! end the test.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::wal::{self, DEFAULT_WAL_NAME, Wal};

const TABLE: &str = "stale_t";
/// The table's row file.
const ROW_FILE: &str = "stale_t.db";

/// Makes recovery refuse the log at `dir`, as it refuses one it cannot read,
/// whatever the uid, until restored or dropped.
struct Unreadable(std::path::PathBuf);

impl Unreadable {
    fn log_in(dir: &Path) -> Unreadable {
        assert!(dir.join(DEFAULT_WAL_NAME).exists(), "no log to refuse in {}", dir.display());
        wal::test_hooks::REFUSE_RECOVERY.lock().push(dir.to_path_buf());
        Unreadable(dir.to_path_buf())
    }
    fn restore(&self) {
        wal::test_hooks::REFUSE_RECOVERY
            .lock()
            .retain(|refused| refused != &self.0);
    }
}

impl Drop for Unreadable {
    fn drop(&mut self) {
        self.restore();
    }
}

/// Runs `f` on a fresh thread and runtime; `Err` carries the panic message.
fn session<F, Fut, T>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T>,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(f());
        rt.shutdown_timeout(std::time::Duration::from_secs(5));
        out
    })
    .join()
    .map_err(|payload| {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default()
    })
}

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let purpose = recognize_query_purpose(sql).expect("parse");
    executor::execute(purpose, db.clone()).await
}

async fn exec_ok(db: &Arc<Database>, sql: &str) {
    let out = exec(db, sql).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
}

async fn ids(db: &Arc<Database>) -> BTreeSet<u64> {
    let res = exec(db, &format!("SELECT id, v FROM {TABLE} WHERE id > 0")).await;
    assert_eq!(res.first(), Some(&2u8), "{}", String::from_utf8_lossy(&res));
    let rows = crate::core::row::row::vec_into_rows(&res[1..]).expect("decode rows");
    let table = db.tables.get(TABLE).expect("table").clone();
    let schema = &table.schema.fields;
    rows.iter()
        .map(|row| {
            u64::from_le_bytes(
                row.get_column("id", schema)
                    .expect("id")
                    .into_slice()
                    .try_into()
                    .expect("u64"),
            )
        })
        .collect()
}

async fn open(dir: &str) -> Arc<Database> {
    Arc::new(Database::new(dir).await)
}

/// Every regular file but the log and its mark, by name.
fn image(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        if !entry.file_type().unwrap().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(DEFAULT_WAL_NAME) {
            continue;
        }
        out.insert(name, std::fs::read(entry.path()).unwrap());
    }
    out
}

/// The log only: its sequence mark stays, as a checkpoint leaves it.
fn wipe_log(dir: &Path) {
    let _ = std::fs::remove_file(dir.join(DEFAULT_WAL_NAME));
}

/// Leaves `dir` as a crash would after row 2 committed to the log only.
/// Returns the log's bytes.
fn crashed_with_row_two_in_the_log(dir: &Path) -> Vec<u8> {
    let dir_str = dir.to_string_lossy().into_owned();
    let d = dir_str.clone();
    session(move || async move {
        let db = open(&d).await;
        exec_ok(&db, &format!("CREATE TABLE {TABLE} (id UBIGINT, v VARCHAR);")).await;
        exec_ok(&db, &format!("INSERT INTO {TABLE} (id, v) VALUES (1, 'one');")).await;
    })
    .unwrap();
    let _ = wal::remove_for(&dir_str);
    let first = image(dir);

    let d = dir_str.clone();
    session(move || async move {
        let db = open(&d).await;
        exec_ok(&db, &format!("INSERT INTO {TABLE} (id, v) VALUES (2, 'two');")).await;
        // A checkpoint, so the files hold row 2 and the log nothing.
        let _ = wal::wal_for(&d).expect("a log").checkpoint();
    })
    .unwrap();
    let _ = wal::remove_for(&dir_str);
    let second = image(dir);
    assert!(second.contains_key(ROW_FILE), "no {ROW_FILE} in {:?}", second.keys());

    // The second session's writes, as whole-file images, into a fresh log.
    wipe_log(dir);
    {
        let w = Wal::open(dir.join(DEFAULT_WAL_NAME)).unwrap();
        for (name, bytes) in &second {
            if first.get(name) == Some(bytes) {
                continue;
            }
            w.log_and_commit(wal::file_id_for_name(name), 0, bytes).unwrap();
        }
        assert_ne!(first.get(ROW_FILE), second.get(ROW_FILE), "row 2 never reached the row file");
    }
    // And the data files as the first session left them.
    for name in second.keys() {
        match first.get(name) {
            Some(bytes) => std::fs::write(dir.join(name), bytes).unwrap(),
            None => std::fs::remove_file(dir.join(name)).unwrap(),
        }
    }
    std::fs::read(dir.join(DEFAULT_WAL_NAME)).unwrap()
}

/// The bug itself, end to end: an open over a log recovery refused must not
/// succeed, and clearing the condition afterwards must replay onto the files
/// the log describes, not onto newer ones.
#[test]
fn a_refused_log_refuses_the_open_and_is_replayed_once_cleared() {
    init();
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_").tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let dir_str = dir.to_string_lossy().into_owned();
    let logged = crashed_with_row_two_in_the_log(&dir);
    let row_file_before = std::fs::read(dir.join(ROW_FILE)).unwrap();

    // Recovery refuses this log. Before the fix the database opened anyway,
    // with no log, and took row 3 onto the bytes row 2's records describe.
    let unreadable = Unreadable::log_in(&dir);
    let d = dir_str.clone();
    let refused = session(move || async move {
        let db = open(&d).await;
        exec_ok(&db, &format!("INSERT INTO {TABLE} (id, v) VALUES (3, 'three');")).await;
    });
    let _ = wal::remove_for(&dir_str);
    let accepted_row_three = refused.is_ok();
    unreadable.restore();

    if !accepted_row_three {
        // Untouched, for the operator who has to settle it.
        assert_eq!(std::fs::read(dir.join(DEFAULT_WAL_NAME)).unwrap(), logged, "the log changed");
        assert_eq!(std::fs::read(dir.join(ROW_FILE)).unwrap(), row_file_before, "a data file changed");
    }

    // The operator settles it: the log is readable again.
    drop(unreadable);
    let d = dir_str.clone();
    let seen = session(move || async move {
        let db = open(&d).await;
        ids(&db).await
    })
    .unwrap();
    let _ = wal::remove_for(&dir_str);

    if accepted_row_three {
        assert!(
            seen.contains(&3),
            "row 3 was acknowledged by an open that ran without its log, and the \
             later replay of that log destroyed it: the table now holds {seen:?}"
        );
    }
    let message = refused.err().expect("the database opened over a log its recovery refused");
    assert!(message.contains(&dir_str), "the refusal does not name the directory: {message}");
    assert!(
        message.contains("WAL recovery failed") && message.contains("ermission denied"),
        "the refusal does not say why: {message}"
    );
    assert_eq!(seen, BTreeSet::from([1, 2]), "replay after the fix did not restore row 2");
}

/// A directory holding a log recovery refuses: an unreadable one, with a
/// committed record for `a.db` in it. Returns the log's bytes and the guard
/// that keeps it refused.
fn plant_refused_log(dir: &Path) -> (Vec<u8>, Unreadable) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("a.db"), b"aaaa").unwrap();
    std::fs::write(dir.join("b.db"), b"bbbb").unwrap();
    let w = Wal::open(dir.join(DEFAULT_WAL_NAME)).unwrap();
    w.log_and_commit(wal::file_id_for_name("a.db"), 0, b"XX").unwrap();
    drop(w);
    let bytes = std::fs::read(dir.join(DEFAULT_WAL_NAME)).unwrap();
    (bytes, Unreadable::log_in(dir))
}

/// The log at `dir`, read with the refusal lifted.
fn log_bytes(dir: &Path, guard: &Unreadable) -> Vec<u8> {
    guard.restore();
    std::fs::read(dir.join(DEFAULT_WAL_NAME)).unwrap()
}

/// A second location whose log recovery refuses stops the pool from opening,
/// as the root's does, and leaves the log and the files as they were.
#[cfg(feature = "storage_pools")]
#[test]
fn a_location_whose_log_is_refused_refuses_the_pool() {
    use crate::core::storage::{LocationRole, StorageManager};
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_pool_").tempdir().unwrap();
    let root = tmp.path().join("root");
    let disk = tmp.path().join("disk");
    let (root_s, disk_s) = (
        root.to_string_lossy().into_owned(),
        disk.to_string_lossy().into_owned(),
    );
    std::fs::create_dir_all(&root).unwrap();
    {
        let pool = StorageManager::open(&root_s);
        pool.add_location(&disk_s, LocationRole::Primary, 1, true).unwrap();
    }
    let _ = wal::remove_for(&disk_s);
    let _ = std::fs::remove_file(disk.join(DEFAULT_WAL_NAME));
    let (logged, guard) = plant_refused_log(&disk);

    let r = root_s.clone();
    let refused = session(move || async move {
        let _ = StorageManager::open(&r);
    });
    let _ = wal::remove_for(&disk_s);
    let message = refused.err().expect("a pool opened over a location whose log recovery refused");
    assert!(message.contains(&disk_s), "the refusal does not name the location: {message}");
    assert_eq!(log_bytes(&disk, &guard), logged, "the log changed");
    assert_eq!(std::fs::read(disk.join("a.db")).unwrap(), b"aaaa");
    assert_eq!(std::fs::read(disk.join("b.db")).unwrap(), b"bbbb");
}

/// Adding a directory whose log recovery refuses is refused on the spot, and
/// the location is not recorded, so the next start does not trip over it.
#[cfg(feature = "storage_pools")]
#[test]
fn adding_a_location_whose_log_is_refused_is_refused() {
    use crate::core::storage::{LocationRole, StorageManager};
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_add_").tempdir().unwrap();
    let root = tmp.path().join("root");
    let disk = tmp.path().join("disk");
    let (root_s, disk_s) = (
        root.to_string_lossy().into_owned(),
        disk.to_string_lossy().into_owned(),
    );
    std::fs::create_dir_all(&root).unwrap();
    let (logged, guard) = plant_refused_log(&disk);

    let (r, d) = (root_s.clone(), disk_s.clone());
    let (added, paths) = session(move || async move {
        let pool = StorageManager::open(&r);
        let added = pool.add_location(&d, LocationRole::Primary, 1, true);
        (added, pool.all_paths())
    })
    .unwrap();
    let _ = wal::remove_for(&disk_s);
    let message = added.err().expect("a location whose log recovery refused was added");
    assert!(message.contains(&disk_s), "{message}");
    assert!(!paths.contains(&disk_s), "the refused location was recorded: {paths:?}");
    assert_eq!(log_bytes(&disk, &guard), logged, "the log changed");

    // And a restart, with the cause gone, does not know the location.
    drop(guard);
    let r = root_s.clone();
    let paths = session(move || async move { StorageManager::open(&r).all_paths() }).unwrap();
    assert!(!paths.contains(&disk_s), "the refused location was persisted: {paths:?}");
}

/// `CREATE DATABASE` adopting a directory whose log recovery refuses answers
/// with an error instead of panicking out of the session, leaves the log as it
/// was and registers nothing, and the catalogue goes on serving.
#[test]
fn create_database_over_a_refused_log_is_an_error() {
    use crate::core::database::catalog::DatabaseCatalog;
    init();
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_create_").tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let root_s = root.to_string_lossy().into_owned();
    let adopted = root.join("adopted");
    let adopted_s = adopted.to_string_lossy().into_owned();

    let (r, a) = (root_s.clone(), adopted.clone());
    let (created, planted, known, other) = session(move || async move {
        let catalog = DatabaseCatalog::open(Some(&r), "recref_default").await.expect("catalogue");
        // Planted after the open, so the catalogue's scan does not meet it.
        let planted = plant_refused_log(&a);
        let created = catalog.create("adopted").await.map(|_| ());
        let known = catalog.get("adopted").is_some();
        let other = catalog.create("fresh").await.map(|_| ());
        (created, planted, known, other)
    })
    .expect("CREATE DATABASE panicked rather than returning its refusal");
    let (logged, guard) = planted;
    let registered = wal::wal_for(&adopted_s).is_some();
    for dir in [root_s.clone(), adopted_s.clone(), root.join("fresh").to_string_lossy().into_owned()] {
        let _ = wal::remove_for(&dir);
    }

    let message = created.expect_err("a database was created over a log its recovery refused");
    assert!(message.contains(&adopted_s), "the refusal does not name the directory: {message}");
    assert!(!known, "the refused database is in the catalogue");
    assert!(!registered, "the refused directory's log is registered");
    assert_eq!(log_bytes(&adopted, &guard), logged, "the log changed");
    other.expect("the catalogue stopped serving after a refusal");
}

/// The same when the refusal is a second location's, which comes after the
/// root's log was replayed and installed: that log is uninstalled again, so a
/// retry once the cause is cleared recovers the directory rather than finding
/// a registered log and skipping it.
#[cfg(feature = "storage_pools")]
#[test]
fn a_refused_location_uninstalls_the_root_log() {
    use crate::core::database::catalog::DatabaseCatalog;
    use crate::core::storage::{LocationRole, StorageManager};
    init();
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_root_").tempdir().unwrap();
    let root = tmp.path().join("cat");
    let adopted = root.join("pooled");
    let disk = tmp.path().join("disk");
    let (root_s, adopted_s, disk_s) = (
        root.to_string_lossy().into_owned(),
        adopted.to_string_lossy().into_owned(),
        disk.to_string_lossy().into_owned(),
    );
    std::fs::create_dir_all(&adopted).unwrap();
    {
        let pool = StorageManager::open(&adopted_s);
        pool.add_location(&disk_s, LocationRole::Primary, 1, true).unwrap();
    }
    let _ = wal::remove_for(&disk_s);
    let _ = std::fs::remove_file(disk.join(DEFAULT_WAL_NAME));
    let (logged, guard) = plant_refused_log(&disk);

    let r = root_s.clone();
    let created = session(move || async move {
        let catalog = DatabaseCatalog::open(Some(&r), "recref_default").await.expect("catalogue");
        catalog.create("pooled").await.map(|_| ())
    })
    .expect("CREATE DATABASE panicked rather than returning its refusal");
    let root_log = wal::wal_for(&adopted_s).is_some();
    let disk_log = wal::wal_for(&disk_s).is_some();
    for dir in [&root_s, &adopted_s, &disk_s] {
        let _ = wal::remove_for(dir);
    }

    let message = created.expect_err("a database was created over a location its recovery refused");
    assert!(message.contains(&disk_s), "the refusal does not name the location: {message}");
    assert!(!root_log, "the refused database's root log is still registered");
    assert!(!disk_log, "the refused location's log is registered");
    assert_eq!(log_bytes(&disk, &guard), logged, "the log changed");
}

/// At startup a sub-database whose log recovery refuses fails the catalogue's
/// open with an error naming it, which the server reports before exiting,
/// rather than a panic.
#[test]
fn a_refused_sub_database_fails_the_catalogue_open_with_an_error() {
    use crate::core::database::catalog::DatabaseCatalog;
    init();
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_sub_").tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let root_s = root.to_string_lossy().into_owned();
    let sub = root.join("sub");
    let sub_s = sub.to_string_lossy().into_owned();
    let (logged, guard) = plant_refused_log(&sub);
    // What makes the scan take the directory for a database.
    std::fs::write(sub.join("db_data_schema.db"), b"").unwrap();

    let r = root_s.clone();
    let opened = session(move || async move {
        DatabaseCatalog::open(Some(&r), "recref_default").await.map(|_| ())
    })
    .expect("the catalogue panicked rather than returning the refusal");
    for dir in [&root_s, &sub_s] {
        let _ = wal::remove_for(dir);
    }

    let message = opened.expect_err("the catalogue opened a database over a log its recovery refused");
    assert!(message.contains(&sub_s), "the refusal does not name the directory: {message}");
    assert_eq!(log_bytes(&sub, &guard), logged, "the log changed");
}

/// A pool refused over its second location uninstalls the log it had just
/// replayed and installed for its first: nothing will write there, and a log
/// left registered would make the next open skip that directory's recovery.
#[cfg(feature = "storage_pools")]
#[test]
fn a_refused_pool_uninstalls_the_logs_it_installed() {
    use crate::core::storage::{LocationRole, StorageManager};
    let tmp = tempfile::Builder::new().prefix("rastdb_recref_two_").tempdir().unwrap();
    let root = tmp.path().join("root");
    let good = tmp.path().join("good");
    let bad = tmp.path().join("bad");
    let (root_s, good_s, bad_s) = (
        root.to_string_lossy().into_owned(),
        good.to_string_lossy().into_owned(),
        bad.to_string_lossy().into_owned(),
    );
    std::fs::create_dir_all(&root).unwrap();
    {
        let pool = StorageManager::open(&root_s);
        pool.add_location(&good_s, LocationRole::Primary, 1, true).unwrap();
        pool.add_location(&bad_s, LocationRole::Primary, 1, true).unwrap();
    }
    for dir in [&good_s, &bad_s] {
        let _ = wal::remove_for(dir);
    }
    let _ = std::fs::remove_file(bad.join(DEFAULT_WAL_NAME));
    let (_logged, _guard) = plant_refused_log(&bad);

    let r = root_s.clone();
    let opened = session(move || async move {
        StorageManager::try_open(&r).map(|_| ())
    })
    .expect("try_open panicked rather than returning its refusal");
    let good_log = wal::wal_for(&good_s).is_some();
    for dir in [&good_s, &bad_s] {
        let _ = wal::remove_for(dir);
    }
    let message = opened.expect_err("a pool opened over a location whose log recovery refused");
    assert!(message.contains(&bad_s), "{message}");
    assert!(!good_log, "the first location's log stayed registered after the pool was refused");
}
