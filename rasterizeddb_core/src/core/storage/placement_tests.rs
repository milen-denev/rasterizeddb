//! End-to-end tests: a real database, real files, and where they land.
//!
//! The unit tests in `tests.rs` cover the pool's own bookkeeping. These cover
//! the thing that bookkeeping exists for — that a table placed on a second disk
//! has its bytes on that disk, still opens from there after a restart, and
//! reads back the rows that were written to it.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};
use crate::core::storage_providers::traits::StorageIO;

use super::{LocationRole, PlacementPolicy};

fn create_statement(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![
            CreateColumnData { name: "id".to_string(), data_type: DbType::I32 },
            CreateColumnData { name: "label".to_string(), data_type: DbType::STRING },
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

/// Behind an `Arc` from the start, because the executor takes one and every
/// test here runs statements through it.
async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "placementtests").await)
}

/// Files whose name begins with `table`, in `dir`.
fn files_for(dir: &std::path::Path, table: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with(table) && name.ends_with(".db"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

async fn insert(db: &Arc<Database>, table: &str, id: i32, label: &str) {
    let sql = format!("INSERT INTO {} (id, label) VALUES ({}, '{}')", table, id, label);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let result = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert!(!result.is_empty(), "insert produced no result");
    // Through the log, so the bytes are actually in the files the assertions
    // below read with `std::fs`.
    db.sync_durable().expect("sync");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_is_created_in_the_location_the_policy_chose() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let db = open(root.path()).await;
    db.storage
        .add_location(&second.path().to_string_lossy(), LocationRole::Primary, 1, true)
        .unwrap();
    // Pinned rather than left to a policy: this test is about the files
    // following the placement, not about how the placement was reached.
    db.storage.pin("elsewhere", &second.path().to_string_lossy()).unwrap();

    db.add_table(create_statement("elsewhere")).await.unwrap();

    assert_eq!(
        files_for(root.path(), "elsewhere"),
        Vec::<String>::new(),
        "the database directory should hold none of the table's files"
    );
    let placed = files_for(second.path(), "elsewhere");
    assert!(
        placed.contains(&"elsewhere.db".to_string())
            && placed.contains(&"elsewhere_pointers.db".to_string())
            && placed.contains(&"elsewhere_schema.db".to_string()),
        "the second location should hold the table's files, found {:?}",
        placed
    );
}

/// The failure this feature is most likely to produce, and the one that reports
/// no error: the placement is lost or ignored at start-up, the table opens from
/// the database directory, and every row is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_placed_table_still_holds_its_rows_after_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    {
        let db = open(root.path()).await;
        db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
        db.storage.pin("moved", &second_path).unwrap();
        db.add_table(create_statement("moved")).await.unwrap();
        insert(&db, "moved", 1, "first").await;
        insert(&db, "moved", 2, "second").await;
    }

    let db = open(root.path()).await;
    assert_eq!(
        db.storage.location_of("moved").as_deref(),
        Some(second_path.as_str()),
        "the placement must come back from the pool file"
    );
    let table = db.tables.get("moved").expect("the table should be in the catalogue").clone();
    assert_eq!(
        StorageIO::get_location(&*table.io_rows).as_deref(),
        Some(second_path.as_str()),
        "the reopened table must read from the directory its files are in"
    );
    assert!(
        StorageIO::get_len(&*table.io_rows).await > 0,
        "the reopened table read an empty row file, which is what a lost placement looks like"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backup_location_gets_a_byte_identical_copy() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();

    {
        let db = open(root.path()).await;
        db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();
        // Reopened so the catalogue's own files are mirrored too: the mirror is
        // decided when a file is opened, and these were opened before the
        // backup location existed.
        drop(db);

        let db = open(root.path()).await;
        db.add_table(create_statement("mirrored")).await.unwrap();
        insert(&db, "mirrored", 1, "alpha").await;
        insert(&db, "mirrored", 2, "beta").await;
    }

    for name in ["mirrored.db", "mirrored_pointers.db", "mirrored_schema.db"] {
        let primary = std::fs::read(root.path().join(name)).unwrap_or_default();
        let copy = std::fs::read(backup.path().join(name)).unwrap_or_default();
        assert!(!primary.is_empty(), "{name} is empty in the database directory");
        assert_eq!(
            primary, copy,
            "{name} in the backup location is not byte-identical to the primary"
        );
    }

    // The catalogue too, or the backup directory would hold every table and no
    // list of them — and would not open as a database.
    let catalogue = std::fs::read(backup.path().join("db_data_rows.db")).unwrap_or_default();
    assert!(!catalogue.is_empty(), "the backup location has no copy of the catalogue");
}

/// A backup location added while the server is running applies to the next
/// table created, without a restart. The catalogue's own files do not — their
/// providers were built before the location existed — which is why
/// `STORAGE ADD LOCATION` says so and why the flags are applied before the
/// catalogue opens.
#[tokio::test(flavor = "multi_thread")]
async fn a_backup_added_at_runtime_covers_the_next_table() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.add_table(create_statement("before_backup")).await.unwrap();
    insert(&db, "before_backup", 1, "unprotected").await;

    db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();

    db.add_table(create_statement("after_backup")).await.unwrap();
    insert(&db, "after_backup", 1, "protected").await;

    let primary = std::fs::read(root.path().join("after_backup.db")).unwrap();
    let copy = std::fs::read(backup.path().join("after_backup.db")).unwrap_or_default();
    assert!(!primary.is_empty());
    assert_eq!(primary, copy, "a table created after the backup was added is not mirrored");

    // And the table opened earlier is not, which the reporting must not claim
    // otherwise.
    assert_eq!(
        db.tables.get("before_backup").unwrap().io_rows.backup_count(),
        0,
        "a table already open cannot have acquired a backup copy"
    );
    assert!(
        db.tables.get("after_backup").unwrap().io_rows.backup_count() > 0,
        "a table created after the backup was added should have one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backup_directory_opens_as_a_database_of_its_own() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();

    {
        let db = open(root.path()).await;
        db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();
        drop(db);

        let db = open(root.path()).await;
        db.add_table(create_statement("survivor")).await.unwrap();
        insert(&db, "survivor", 7, "still here").await;
    }

    // The point of a mirrored copy: point the engine at the backup directory
    // and it is a database, not an archive that needs restoring first. The pool
    // file is removed so the copy does not try to mirror back into a location
    // that is now its own.
    let _ = std::fs::remove_file(backup.path().join(super::POOL_FILE));
    let recovered = open(backup.path()).await;
    let table = recovered
        .tables
        .get("survivor")
        .expect("the backup copy should list the table");
    assert!(
        StorageIO::get_len(&*table.io_rows).await > 0,
        "the backup copy lists the table but holds none of its rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_takes_its_files_with_it() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.add_table(create_statement("travelling")).await.unwrap();
    insert(&db, "travelling", 1, "before").await;

    let before = std::fs::read(root.path().join("travelling.db")).unwrap();
    assert!(!before.is_empty());

    let copied = db.move_table_to("travelling", &second_path).await.unwrap();
    assert!(copied > 0, "a move that copied nothing has not moved anything");

    assert_eq!(
        files_for(root.path(), "travelling"),
        Vec::<String>::new(),
        "the original files should be gone once the move commits"
    );
    let after = std::fs::read(second.path().join("travelling.db")).unwrap();
    assert_eq!(before, after, "the moved row file is not the one that was there");

    assert_eq!(db.storage.location_of("travelling").as_deref(), Some(second_path.as_str()));

    // Still writable at its new address, which a move that left a stale handle
    // in the table map would not be.
    insert(&db, "travelling", 2, "after").await;
    let table = db.tables.get("travelling").unwrap().clone();
    assert_eq!(StorageIO::get_location(&*table.io_rows).as_deref(), Some(second_path.as_str()));
}

/// A backup directory is flat: a table on the root and the same table on a
/// second disk both mirror to one file in it. Sweeping the source's files after
/// a move therefore has to leave the backup alone, or the move deletes the copy
/// it just made — the primary survives and its mirror silently does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_move_keeps_the_backup_copy() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();
    let backup_path = backup.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();

    db.add_table(create_statement("shuttled")).await.unwrap();
    insert(&db, "shuttled", 1, "row").await;
    assert!(
        !std::fs::read(backup.path().join("shuttled.db")).unwrap_or_default().is_empty(),
        "the table should be mirrored before it moves"
    );

    db.move_table_to("shuttled", &second_path).await.unwrap();

    let primary = std::fs::read(second.path().join("shuttled.db")).unwrap();
    let copy = std::fs::read(backup.path().join("shuttled.db")).unwrap_or_default();
    assert!(!primary.is_empty(), "the moved table has no rows at its destination");
    assert_eq!(
        primary, copy,
        "the backup copy did not survive the move: a moved table must not lose its mirror"
    );

    // And it keeps being mirrored from its new home.
    insert(&db, "shuttled", 2, "after").await;
    let primary = std::fs::read(second.path().join("shuttled.db")).unwrap();
    let copy = std::fs::read(backup.path().join("shuttled.db")).unwrap_or_default();
    assert_eq!(primary, copy, "writes after the move are not reaching the backup");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_move_into_a_backup_location_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();
    db.add_table(create_statement("staying")).await.unwrap();

    let refused = db.move_table_to("staying", &backup_path).await.unwrap_err();
    assert!(
        refused.to_string().contains("backup"),
        "the refusal should say why: {refused}"
    );
    assert!(
        !files_for(root.path(), "staying").is_empty(),
        "a refused move must leave the table where it was"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_placed_table_removes_its_files_and_its_placement() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.storage.pin("temporary", &second_path).unwrap();
    db.add_table(create_statement("temporary")).await.unwrap();
    assert!(!files_for(second.path(), "temporary").is_empty());

    assert!(db.drop_table("temporary").await.unwrap());

    assert_eq!(
        files_for(second.path(), "temporary"),
        Vec::<String>::new(),
        "a dropped table's files must go from the directory they are actually in"
    );
    assert!(
        db.storage.location_of("temporary").is_none(),
        "a dropped table should leave no placement behind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_table_keeps_its_location() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.storage.pin("before", &second_path).unwrap();
    db.add_table(create_statement("before")).await.unwrap();

    db.rename_table("before", "after").await.unwrap();

    assert_eq!(db.storage.location_of("after").as_deref(), Some(second_path.as_str()));
    assert!(db.storage.location_of("before").is_none());
    assert!(
        !files_for(second.path(), "after").is_empty(),
        "the renamed table's files should be where the original's were"
    );
    assert_eq!(
        files_for(root.path(), "after"),
        Vec::<String>::new(),
        "a rename must not relocate a table to the database directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn free_space_placement_prefers_the_roomier_location() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let db = open(root.path()).await;
    db.storage
        .add_location(&second.path().to_string_lossy(), LocationRole::Primary, 1, true)
        .unwrap();
    db.storage.set_policy(PlacementPolicy::FreeSpace);

    // Both directories are almost certainly on the same filesystem here, so
    // which one wins is not something to assert. What is worth asserting is
    // that the decision is *made* — a policy that silently fell back to the
    // root would pass a weaker test.
    let chosen = db.storage.would_place().expect("a file-backed database always places somewhere");
    assert!(
        chosen == root.path().to_string_lossy()
            || chosen == second.path().to_string_lossy(),
        "free-space placement chose {chosen}, which is neither location"
    );

    // Weight is the tie-breaker an operator has, so it must actually move the
    // decision when free space is equal.
    db.storage
        .set_weight(&second.path().to_string_lossy(), 1_000)
        .unwrap();
    assert_eq!(
        db.storage.would_place().as_deref(),
        Some(second.path().to_string_lossy().as_ref()),
        "a thousandfold weight should decide a tie"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebalance_evacuates_a_disabled_location() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.storage.pin("draining", &second_path).unwrap();
    db.add_table(create_statement("draining")).await.unwrap();
    insert(&db, "draining", 1, "row").await;

    db.storage.set_enabled(&second_path, false).unwrap();

    let lines = super::relocate::rebalance(&db, false).await.unwrap();

    assert_eq!(
        db.storage.location_of("draining").as_deref(),
        Some(root.path().to_string_lossy().as_ref()),
        "a disabled location should be drained, not merely frozen: {lines:?}"
    );
    assert_eq!(
        files_for(second.path(), "draining"),
        Vec::<String>::new(),
        "the drained files should be gone from the disabled location"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_moves_nothing() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage.add_location(&second_path, LocationRole::Primary, 1, true).unwrap();
    db.storage.pin("still", &second_path).unwrap();
    db.add_table(create_statement("still")).await.unwrap();
    db.storage.set_enabled(&second_path, false).unwrap();

    let lines = super::relocate::rebalance(&db, true).await.unwrap();

    assert!(
        lines.iter().any(|line| line.contains("still")),
        "a dry run should name what it would move: {lines:?}"
    );
    assert_eq!(
        db.storage.location_of("still").as_deref(),
        Some(second_path.as_str()),
        "a dry run must not move anything"
    );
}

// ---------------------------------------------------------------------------
// The SQL surface, end to end
// ---------------------------------------------------------------------------
//
// These go through `recognize_query_purpose` and the executor rather than
// calling the pool directly, because the wiring between them is its own way to
// fail: a statement the lexer does not claim reaches the query path and comes
// back as "syntax error or unsupported query", and nothing else here would
// notice.

/// Runs a statement and returns the lines it produced.
async fn run(db: &Arc<Database>, sql: &str) -> Vec<String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("no statement kind recognised for {sql:?}"));
    let bytes = crate::core::rql::executor::execute(purpose, db.clone()).await;
    decode(&bytes)
}

/// Pulls the text out of a result the executor built.
///
/// The encoding is a leading marker byte and then serialized rows; only the
/// first column of each is read, which is all the text forms produce.
fn decode(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    text.split(|c: char| c.is_control())
        .map(|part| part.trim().to_string())
        .filter(|part| part.len() > 2 && part.chars().any(|c| c.is_ascii_alphabetic()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_statements_reach_the_storage_surface_rather_than_the_query_path() {
    use crate::core::rql::lexer_s1::{QueryPurpose, recognize_query_purpose};

    for sql in [
        "STORAGE ADD LOCATION '/mnt/disk2'",
        "STORAGE SET POLICY = 'free-space'",
        "STORAGE MOVE TABLE users TO '/mnt/disk2'",
        "SHOW STORAGE",
        "SHOW STORAGE LOCATIONS",
        "show storage placements;",
    ] {
        assert!(
            matches!(recognize_query_purpose(sql), Some(QueryPurpose::Storage(_))),
            "{sql:?} did not reach the storage surface"
        );
    }

    // And the near misses stay where they belong.
    assert!(matches!(
        recognize_query_purpose("SELECT * FROM storage"),
        Some(QueryPurpose::QueryRows(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_location_added_in_sql_takes_the_next_table() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;

    let added = run(&db, &format!("STORAGE ADD LOCATION '{}'", second_path)).await;
    assert!(
        added.iter().any(|line| line.contains(&second_path)),
        "adding a location should confirm it: {added:?}"
    );

    // Weighted far above the root, so the choice is not a coin toss between two
    // directories on the same filesystem.
    run(&db, &format!("STORAGE ALTER LOCATION '{}' SET WEIGHT = 100000", second_path)).await;
    run(&db, "STORAGE SET POLICY = 'free-space'").await;

    db.add_table(create_statement("chosen")).await.unwrap();

    assert_eq!(
        db.storage.location_of("chosen").as_deref(),
        Some(second_path.as_str()),
        "the policy set in SQL should have decided where the table went"
    );
    assert!(!files_for(second.path(), "chosen").is_empty());

    let shown = run(&db, "SHOW STORAGE").await;
    assert!(
        shown.iter().any(|line| line.contains("free-space")),
        "SHOW STORAGE should report the policy: {shown:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_statement_says_what_is_wrong_with_it() {
    let root = tempfile::tempdir().unwrap();
    let db = open(root.path()).await;

    for (sql, expected) in [
        ("STORAGE SET POLICY = 'sideways'", "unknown policy"),
        ("STORAGE ADD LOCATION 'data/relative'", "absolute"),
        ("STORAGE SET MIN_FREE = 'lots'", "not a size"),
        ("STORAGE DEFENESTRATE", "unknown STORAGE statement"),
    ] {
        let lines = run(&db, sql).await;
        let joined = lines.join(" ");
        assert!(
            joined.contains(expected),
            "{sql:?} should have been refused with {expected:?}, got {joined:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_through_sql_takes_its_rows_with_it() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    run(&db, &format!("STORAGE ADD LOCATION '{}'", second_path)).await;
    db.add_table(create_statement("relocated")).await.unwrap();
    insert(&db, "relocated", 1, "carried").await;

    let before = std::fs::read(root.path().join("relocated.db")).unwrap();
    let moved = run(&db, &format!("STORAGE MOVE TABLE relocated TO '{}'", second_path)).await;
    assert!(
        moved.iter().any(|line| line.contains("moved relocated")),
        "the move should report what it did: {moved:?}"
    );

    let after = std::fs::read(second.path().join("relocated.db")).unwrap();
    assert_eq!(before, after, "the moved rows are not the ones that were there");

    // And the table still answers, which a move that left a stale handle behind
    // would not.
    insert(&db, "relocated", 2, "after the move").await;
}
