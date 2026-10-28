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
    let sql = format!(
        "INSERT INTO {} (id, label) VALUES ({}, '{}')",
        table, id, label
    );
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
        .add_location(
            &second.path().to_string_lossy(),
            LocationRole::Primary,
            1,
            true,
        )
        .unwrap();
    // Pinned rather than left to a policy: this test is about the files
    // following the placement, not about how the placement was reached.
    db.storage
        .pin("elsewhere", &second.path().to_string_lossy())
        .unwrap();

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
        db.storage
            .add_location(&second_path, LocationRole::Primary, 1, true)
            .unwrap();
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
    let table = db.table_for_tests("moved")
        .expect("the table should be in the catalogue")
        .clone();
    assert_eq!(
        StorageIO::get_location(&*table.data().unwrap().io_rows).as_deref(),
        Some(second_path.as_str()),
        "the reopened table must read from the directory its files are in"
    );
    assert!(
        StorageIO::get_len(&*table.data().unwrap().io_rows).await > 0,
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
        db.storage
            .add_location(&backup_path, LocationRole::Backup, 1, true)
            .unwrap();
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
        assert!(
            !primary.is_empty(),
            "{name} is empty in the database directory"
        );
        assert_eq!(
            primary, copy,
            "{name} in the backup location is not byte-identical to the primary"
        );
    }

    // The catalogue too, or the backup directory would hold every table and no
    // list of them — and would not open as a database.
    let catalogue = std::fs::read(backup.path().join("db_data_rows.db")).unwrap_or_default();
    assert!(
        !catalogue.is_empty(),
        "the backup location has no copy of the catalogue"
    );
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
    db.add_table(create_statement("before_backup"))
        .await
        .unwrap();
    insert(&db, "before_backup", 1, "unprotected").await;

    db.storage
        .add_location(&backup_path, LocationRole::Backup, 1, true)
        .unwrap();

    db.add_table(create_statement("after_backup"))
        .await
        .unwrap();
    insert(&db, "after_backup", 1, "protected").await;

    let primary = std::fs::read(root.path().join("after_backup.db")).unwrap();
    let copy = std::fs::read(backup.path().join("after_backup.db")).unwrap_or_default();
    assert!(!primary.is_empty());
    assert_eq!(
        primary, copy,
        "a table created after the backup was added is not mirrored"
    );

    // And the table opened earlier is not, which the reporting must not claim
    // otherwise.
    assert_eq!(
        db.table_for_tests("before_backup")
            .unwrap()
            .io_rows()
            .unwrap()
            .backup_count(),
        0,
        "a table already open cannot have acquired a backup copy"
    );
    assert!(
        db.table_for_tests("after_backup")
            .unwrap()
            .io_rows()
            .unwrap()
            .backup_count()
            > 0,
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
        db.storage
            .add_location(&backup_path, LocationRole::Backup, 1, true)
            .unwrap();
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
    let table = recovered.table_for_tests("survivor")
        .expect("the backup copy should list the table");
    assert!(
        StorageIO::get_len(&*table.data().unwrap().io_rows).await > 0,
        "the backup copy lists the table but holds none of its rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_a_table_takes_its_files_with_it() {
    let root = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let second_path = second.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
    db.add_table(create_statement("travelling")).await.unwrap();
    insert(&db, "travelling", 1, "before").await;

    let before = std::fs::read(root.path().join("travelling.db")).unwrap();
    assert!(!before.is_empty());

    let copied = db.move_table_to("travelling", &second_path).await.unwrap();
    assert!(
        copied > 0,
        "a move that copied nothing has not moved anything"
    );

    assert_eq!(
        files_for(root.path(), "travelling"),
        Vec::<String>::new(),
        "the original files should be gone once the move commits"
    );
    let after = std::fs::read(second.path().join("travelling.db")).unwrap();
    assert_eq!(
        before, after,
        "the moved row file is not the one that was there"
    );

    assert_eq!(
        db.storage.location_of("travelling").as_deref(),
        Some(second_path.as_str())
    );

    // Still writable at its new address, which a move that left a stale handle
    // in the table map would not be.
    insert(&db, "travelling", 2, "after").await;
    let table = db.table_for_tests("travelling").unwrap().clone();
    assert_eq!(
        StorageIO::get_location(&*table.data().unwrap().io_rows).as_deref(),
        Some(second_path.as_str())
    );
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
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
    db.storage
        .add_location(&backup_path, LocationRole::Backup, 1, true)
        .unwrap();

    db.add_table(create_statement("shuttled")).await.unwrap();
    insert(&db, "shuttled", 1, "row").await;
    assert!(
        !std::fs::read(backup.path().join("shuttled.db"))
            .unwrap_or_default()
            .is_empty(),
        "the table should be mirrored before it moves"
    );

    db.move_table_to("shuttled", &second_path).await.unwrap();

    let primary = std::fs::read(second.path().join("shuttled.db")).unwrap();
    let copy = std::fs::read(backup.path().join("shuttled.db")).unwrap_or_default();
    assert!(
        !primary.is_empty(),
        "the moved table has no rows at its destination"
    );
    assert_eq!(
        primary, copy,
        "the backup copy did not survive the move: a moved table must not lose its mirror"
    );

    // And it keeps being mirrored from its new home.
    insert(&db, "shuttled", 2, "after").await;
    let primary = std::fs::read(second.path().join("shuttled.db")).unwrap();
    let copy = std::fs::read(backup.path().join("shuttled.db")).unwrap_or_default();
    assert_eq!(
        primary, copy,
        "writes after the move are not reaching the backup"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_move_into_a_backup_location_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();

    let db = open(root.path()).await;
    db.storage
        .add_location(&backup_path, LocationRole::Backup, 1, true)
        .unwrap();
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
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
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
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
    db.storage.pin("before", &second_path).unwrap();
    db.add_table(create_statement("before")).await.unwrap();

    db.rename_table("before", "after").await.unwrap();

    assert_eq!(
        db.storage.location_of("after").as_deref(),
        Some(second_path.as_str())
    );
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
        .add_location(
            &second.path().to_string_lossy(),
            LocationRole::Primary,
            1,
            true,
        )
        .unwrap();
    db.storage.set_policy(PlacementPolicy::FreeSpace);

    // Both directories are almost certainly on the same filesystem here, so
    // which one wins is not something to assert. What is worth asserting is
    // that the decision is *made* — a policy that silently fell back to the
    // root would pass a weaker test.
    let chosen = db
        .storage
        .would_place()
        .expect("a file-backed database always places somewhere");
    assert!(
        chosen == root.path().to_string_lossy() || chosen == second.path().to_string_lossy(),
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
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
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
    db.storage
        .add_location(&second_path, LocationRole::Primary, 1, true)
        .unwrap();
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
    run(
        &db,
        &format!(
            "STORAGE ALTER LOCATION '{}' SET WEIGHT = 100000",
            second_path
        ),
    )
    .await;
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
    let moved = run(
        &db,
        &format!("STORAGE MOVE TABLE relocated TO '{}'", second_path),
    )
    .await;
    assert!(
        moved.iter().any(|line| line.contains("moved relocated")),
        "the move should report what it did: {moved:?}"
    );

    let after = std::fs::read(second.path().join("relocated.db")).unwrap();
    assert_eq!(
        before, after,
        "the moved rows are not the ones that were there"
    );

    // And the table still answers, which a move that left a stale handle behind
    // would not.
    insert(&db, "relocated", 2, "after the move").await;
}

async fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

/// On a mirrored table an UPDATE that fails leaves no transaction behind in
/// the log and the backup as the primary is. The mirror answered the
/// primary's `defers_in_place_writes` while writing every backup at once,
/// and had no `abort_txn`: `UPDATE mq SET m = 7, n = 5000 WHERE id < 10`
/// overwrote one row in place and failed 23505; the primary's rows were
/// unchanged, the backup's were not, the log held the aborted transaction
/// as pending for good, and no checkpoint ran again.
///
/// Mutations: the mirror answering the primary's `defers_in_place_writes`
/// leaves the backup changed; its `abort_txn` removed leaves the
/// transaction pending.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_update_on_a_mirrored_table_leaves_nothing_pending_or_diverged() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();
    let root_path = root.path().to_string_lossy().into_owned();
    {
        let db = open(root.path()).await;
        db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();
        drop(db);
        crate::core::wal::remove_for(&root_path);
        crate::core::wal::remove_for(&backup_path);
    }
    let db = open(root.path()).await;
    exec(&db, "CREATE TABLE mq (id BIGINT PRIMARY KEY, n BIGINT UNIQUE, m BIGINT, CHECK (m + id < 100))").await.unwrap();
    let values: Vec<String> = (0..20).map(|i| format!("({i}, {i}, 0)")).collect();
    exec(&db, &format!("INSERT INTO mq (id, n, m) VALUES {}", values.join(", "))).await.unwrap();
    db.sync_durable().unwrap();
    let wal = crate::core::wal::wal_for(&root_path).expect("the root's log");
    assert_eq!(wal.pending_transactions(), 0);
    let table = db.table_for_tests("mq").unwrap();
    let in_place = table.in_place_column_updates();

    let e = exec(&db, "UPDATE mq SET m = 7, n = 5000 WHERE id < 10").await.expect_err("n repeats");
    assert!(e.contains("duplicate value"), "{e}");
    assert_eq!(table.in_place_column_updates(), in_place, "a mirrored table overwrote in place");
    assert_eq!(wal.pending_transactions(), 0, "the failed UPDATE's transaction is still pending in the log");
    assert!(wal.checkpoint().unwrap(), "a checkpoint declined after the failed UPDATE");
    // A failure that abandons the storage transaction after rows were
    // written (a CHECK only a later row breaks): the abort must reach the
    // primary's log, or the transaction stays pending.
    let e = exec(&db, "UPDATE mq SET m = 90 WHERE id < 15").await.expect_err("row 10 breaks the CHECK");
    assert!(e.contains("check constraint"), "{e}");
    assert_eq!(wal.pending_transactions(), 0, "the aborted UPDATE's transaction is still pending in the log");
    assert!(wal.checkpoint().unwrap(), "a checkpoint declined after the aborted UPDATE");

    // A successful one still works, and the copy keeps up.
    exec(&db, "UPDATE mq SET m = 8 WHERE id = 3").await.unwrap();
    db.sync_durable().unwrap();
    drop(table);
    drop(db);
    crate::core::wal::remove_for(&root_path);
    crate::core::wal::remove_for(&backup_path);
    for name in ["mq.db", "mq_pointers.db"] {
        let primary = std::fs::read(root.path().join(name)).unwrap();
        let copy = std::fs::read(backup.path().join(name)).unwrap();
        assert_eq!(primary.len(), copy.len(), "{name}: the backup's length differs from the primary's");
        assert!(primary == copy, "{name}: the backup differs from the primary");
    }
}

/// A database with `backup` attached as a backup location, reopened so every
/// table it makes is mirrored.
async fn mirrored(root: &std::path::Path, backup: &std::path::Path) -> Arc<Database> {
    let root_path = root.to_string_lossy().into_owned();
    let backup_path = backup.to_string_lossy().into_owned();
    {
        let db = open(root).await;
        db.storage.add_location(&backup_path, LocationRole::Backup, 1, true).unwrap();
        drop(db);
        crate::core::wal::remove_for(&root_path);
        crate::core::wal::remove_for(&backup_path);
    }
    open(root).await
}

async fn close(db: Arc<Database>, root: &std::path::Path, backup: &std::path::Path) {
    db.sync_durable().unwrap();
    drop(db);
    crate::core::wal::remove_for(&root.to_string_lossy());
    crate::core::wal::remove_for(&backup.to_string_lossy());
}

fn assert_same(root: &std::path::Path, backup: &std::path::Path, names: &[&str], when: &str) {
    for name in names {
        let primary = std::fs::read(root.join(name)).unwrap();
        let copy = std::fs::read(backup.join(name)).unwrap();
        assert_eq!(primary.len(), copy.len(), "{when}: {name}: the backup's length differs from the primary's");
        let first = primary.iter().zip(&copy).position(|(a, b)| a != b);
        assert!(first.is_none(), "{when}: {name}: the backup differs from the primary at byte {first:?}");
    }
}

/// A transaction's in-place writes to a rows file are committed through its
/// pointers file: a compaction (VACUUM FULL), an insert into an extent a
/// VACUUM reclaimed, and a whole-row rewrite. Copies staged per file were
/// never made for them, and the backup's rows file kept its old bytes at
/// the same length, which the length-only check at open does not see.
#[tokio::test(flavor = "multi_thread")]
async fn rows_file_writes_committed_through_the_pointers_file_reach_the_backup() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().into_owned();
    let db = mirrored(root.path(), backup.path()).await;
    for t in ["mf", "mr", "mw"] {
        exec(&db, &format!("CREATE TABLE {t} (id BIGINT, label TEXT)")).await.unwrap();
        let values: Vec<String> = (0..200).map(|i| format!("({i}, 'label-{i:04}')")).collect();
        exec(&db, &format!("INSERT INTO {t} (id, label) VALUES {}", values.join(", "))).await.unwrap();
    }
    // Compaction.
    exec(&db, "DELETE FROM mf WHERE id % 3 = 0").await.unwrap();
    exec(&db, "VACUUM mf").await.unwrap();
    exec(&db, "VACUUM FULL mf").await.unwrap();
    // Inserts into reclaimed extents.
    exec(&db, "DELETE FROM mr WHERE id % 2 = 0").await.unwrap();
    exec(&db, "VACUUM mr").await.unwrap();
    db.sync_durable().unwrap();
    let mr_len = std::fs::metadata(root.path().join("mr.db")).unwrap().len();
    let values: Vec<String> = (1000..1060).map(|i| format!("({i}, 'fresh-{i:04}')")).collect();
    exec(&db, &format!("INSERT INTO mr (id, label) VALUES {}", values.join(", "))).await.unwrap();
    // A whole-row rewrite of the same length, outside any transaction.
    {
        use crate::core::row::row::{ColumnWritePayload, RowWrite};
        use crate::core::row::row_pointer::RowPointerIterator;
        let table = db.table_for_tests("mw").unwrap();
        let mut it = RowPointerIterator::new(table.io_pointers().unwrap().clone()).await.unwrap();
        for _ in 0..5 {
            let (pos, mut pointer) = it.next_row_pointer_with_offset().await.unwrap().unwrap();
            let mut rw = RowWrite { columns_writing_data: smallvec::SmallVec::new() };
            for (bytes, order, ty) in [
                (7i64.to_le_bytes().to_vec(), 0u32, DbType::I64),
                (b"label-ZZZZ".to_vec(), 1, DbType::STRING),
            ] {
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(&bytes);
                let size = ty.get_size();
                rw.columns_writing_data.push(ColumnWritePayload { data: mb, write_order: order, column_type: ty, size });
            }
            pointer
                .update_with_pointer_record_pos(
                    table.io_pointers().unwrap().clone(),
                    table.io_rows().unwrap().clone(),
                    pos,
                    &table.data().unwrap().ids.last_row_id,
                    &table.data().unwrap().len,
                    #[cfg(feature = "enable_long_row")]
                    0,
                    &rw,
                    None,
                    None,
                    crate::core::mvcc::transaction::FROZEN_TX,
                )
                .await
                .unwrap();
        }
    }
    assert_eq!(crate::core::storage::mirror::pending_copies_in(&root_path), 0, "copies were left waiting");
    close(db, root.path(), backup.path()).await;
    let contains = |name: &str, what: &[u8]| {
        let bytes = std::fs::read(root.path().join(name)).unwrap();
        bytes.windows(what.len()).filter(|w| *w == what).count()
    };
    assert_eq!(contains("mw.db", b"label-ZZZZ"), 5, "the whole-row rewrites did not land in place");
    assert_eq!(contains("mr.db", b"fresh-10"), 60, "the inserts did not land");
    assert_eq!(
        std::fs::metadata(root.path().join("mr.db")).unwrap().len(),
        mr_len,
        "the inserts were appended, not written into reclaimed extents"
    );
    let names = ["mf.db", "mf_pointers.db", "mr.db", "mr_pointers.db", "mw.db", "mw_pointers.db"];
    assert_same(root.path(), backup.path(), &names, "before a restart");
    let db = open(root.path()).await;
    close(db, root.path(), backup.path()).await;
    assert_same(root.path(), backup.path(), &names, "after a restart");
}

/// The primary committed and the process died before the copy: the backup
/// is the same length with older bytes. The mark made before the commit
/// tells the next open to rebuild it.
#[tokio::test(flavor = "multi_thread")]
async fn a_copy_cut_off_after_the_primary_committed_is_rebuilt_at_open() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let db = mirrored(root.path(), backup.path()).await;
    exec(&db, "CREATE TABLE mc (id BIGINT, label TEXT)").await.unwrap();
    let values: Vec<String> = (0..200).map(|i| format!("({i}, 'label-{i:04}')")).collect();
    exec(&db, &format!("INSERT INTO mc (id, label) VALUES {}", values.join(", "))).await.unwrap();
    exec(&db, "DELETE FROM mc WHERE id % 2 = 0").await.unwrap();
    exec(&db, "VACUUM mc").await.unwrap();
    db.sync_durable().unwrap();
    let root_path = root.path().to_string_lossy().into_owned();
    crate::core::storage::mirror::test_hooks::CUT_OFF_COPY.lock().push(root_path.clone());
    let values: Vec<String> = (1000..1060).map(|i| format!("({i}, 'fresh-{i:04}')")).collect();
    exec(&db, &format!("INSERT INTO mc (id, label) VALUES {}", values.join(", "))).await.unwrap();
    let armed = crate::core::storage::mirror::test_hooks::CUT_OFF_COPY.lock().iter().any(|l| *l == root_path);
    assert!(!armed, "the INSERT committed no in-place write, so nothing was cut off");
    close(db, root.path(), backup.path()).await;
    let primary = std::fs::read(root.path().join("mc.db")).unwrap();
    let copy = std::fs::read(backup.path().join("mc.db")).unwrap();
    assert_eq!(primary.len(), copy.len(), "the reclaimed-extent insert grew the file");
    // Each row is its own commit, and only the first one's copy was cut off.
    assert!(primary != copy, "the scenario left nothing for the open to find");
    let listing: Vec<String> =
        std::fs::read_dir(backup.path()).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    assert!(backup.path().join("__mirror_behind.mc.db").exists(), "no mark says the backup is behind: {listing:?}");

    let db = open(root.path()).await;
    close(db, root.path(), backup.path()).await;
    assert_same(root.path(), backup.path(), &["mc.db", "mc_pointers.db"], "after the open");
    assert!(!backup.path().join("__mirror_behind.mc.db").exists(), "the mark outlived the rebuild");
}

fn marks_in(backup: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(backup)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("__mirror_behind."))
        .collect()
}

/// The table's own files, which every one of these tests compares.
fn table_files(dir: &std::path::Path, table: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("{table}.")) || n.starts_with(&format!("{table}_")))
        .filter(|n| n.ends_with(".db"))
        .collect();
    names.sort();
    names
}

/// A backup write that fails during a copy: `write_data` only logged it, so
/// the copy counted as made, its mark was removed, and the backup stayed the
/// same length with older bytes. The mark now stays, and the next open
/// rebuilds the copy.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_backup_write_keeps_the_mark_and_is_rebuilt_at_open() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();
    let db = mirrored(root.path(), backup.path()).await;
    exec(&db, "CREATE TABLE mx (id BIGINT, label TEXT)").await.unwrap();
    let values: Vec<String> = (0..200).map(|i| format!("({i}, 'label-{i:04}')")).collect();
    exec(&db, &format!("INSERT INTO mx (id, label) VALUES {}", values.join(", "))).await.unwrap();
    exec(&db, "DELETE FROM mx WHERE id % 2 = 0").await.unwrap();
    exec(&db, "VACUUM mx").await.unwrap();
    db.sync_durable().unwrap();
    assert!(marks_in(backup.path()).is_empty());

    crate::core::storage_providers::file_sync::failed_appends::FAIL_INPLACE_IN.lock().push(backup_path.clone());
    let values: Vec<String> = (1000..1060).map(|i| format!("({i}, 'fresh-{i:04}')")).collect();
    exec(&db, &format!("INSERT INTO mx (id, label) VALUES {}", values.join(", "))).await.unwrap();
    let armed = crate::core::storage_providers::file_sync::failed_appends::FAIL_INPLACE_IN
        .lock()
        .iter()
        .any(|l| *l == backup_path);
    assert!(!armed, "the INSERT wrote nothing in place to the backup, so nothing failed");
    close(db, root.path(), backup.path()).await;
    let marks = marks_in(backup.path());
    assert!(!marks.is_empty(), "the failed backup write left no mark");
    let names = table_files(root.path(), "mx");
    let differs = names.iter().any(|n| std::fs::read(root.path().join(n)).unwrap() != std::fs::read(backup.path().join(n)).unwrap());
    assert!(differs, "the failed write left nothing for the open to find: {marks:?}");

    let db = open(root.path()).await;
    close(db, root.path(), backup.path()).await;
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    assert_same(root.path(), backup.path(), &names, "after the open");
    assert!(marks_in(backup.path()).is_empty(), "the mark outlived the rebuild");
}

/// A transaction of more than 4096 scattered in-place writes copies one
/// span over most of the file. Nothing ordered its copy against another
/// commit's: a chunk read before that commit landed after that commit's own
/// copy, and the backup lost the later write (for a pointers file, a
/// deleted row back on failover). Driven on the provider: DELETE and UPDATE
/// serialise on the table's write lock, but an INSERT into a reclaimed
/// extent does not, and the mirror's order must not rest on that. The big
/// copy is parked between its read and its write while the other commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_copy_read_before_a_later_commit_does_not_land_after_it() {
    use crate::core::storage::mirror::{MirrorStorageProvider, test_hooks};
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().into_owned();
    let backup_path = backup.path().to_string_lossy().into_owned();
    {
        let (wal, _) = crate::core::wal::open_and_recover(&root_path).unwrap();
        crate::core::wal::install_for(&root_path, wal);
        let io = MirrorStorageProvider::attach(&root_path, std::slice::from_ref(&backup_path), "mz.db").await;
        io.append_data(&vec![0u8; 128 * 1024], true).await;
        io.commit_durable(io.begin_txn()).await.unwrap();

        // Every 16th byte: never touching, so past 4096 ranges they collapse.
        let txn = io.begin_txn();
        let mut big = 0;
        for i in 0..5000u64 {
            big = io.write_data_txn(txn, i * 16 + 1, &[0xAA]).await;
        }
        assert_ne!(big, 0, "the writes were not logged, so nothing waits to be copied");
        let parked = std::sync::Arc::new(tokio::sync::Notify::new());
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        *test_hooks::PARK_AFTER_READ.lock() = Some((root_path.clone(), parked.clone(), resume.clone()));
        let waiting_before = test_hooks::waiting_in(&root_path);
        let first = tokio::spawn({
            let io = io.clone();
            async move { io.commit_durable(big).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), parked.notified())
            .await
            .expect("the big copy never parked");

        // Inside the span the parked copy has already read.
        let txn = io.begin_txn();
        let small = io.write_data_txn(txn, 8, &[0xBB]).await;
        let second = tokio::spawn({
            let io = io.clone();
            async move { io.commit_durable(small).await }
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !second.is_finished() && test_hooks::waiting_in(&root_path) == waiting_before {
            assert!(std::time::Instant::now() < deadline, "the second commit neither finished nor waited");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        resume.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(crate::core::storage::mirror::pending_copies_in(&root_path), 0);
    }
    crate::core::wal::remove_for(&root_path);
    let primary = std::fs::read(root.path().join("mz.db")).unwrap();
    assert_eq!(primary[8], 0xBB, "the second commit's byte is not in the primary");
    assert_eq!(primary[17], 0xAA, "the first commit's bytes are not in the primary");
    assert_same(root.path(), backup.path(), &["mz.db"], "after both copies");
    assert!(marks_in(backup.path()).is_empty(), "a mark was left: {:?}", marks_in(backup.path()));
}

/// Without a log the primary writes in place at once and so do the
/// backups; one that fails leaves a mark, and the next attach rebuilds it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_immediate_backup_write_leaves_a_mark_that_the_next_attach_honours() {
    use crate::core::storage::mirror::MirrorStorageProvider;
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().into_owned();
    let backup_path = backup.path().to_string_lossy().into_owned();
    {
        let io = MirrorStorageProvider::attach(&root_path, std::slice::from_ref(&backup_path), "mi.db").await;
        io.append_data(&vec![0u8; 4096], true).await;
        io.commit_durable(io.begin_txn()).await.unwrap();
        crate::core::storage_providers::file_sync::failed_appends::FAIL_INPLACE_IN.lock().push(backup_path.clone());
        let token = io.write_data_txn(io.begin_txn(), 100, &[0xCC; 8]).await;
        assert_eq!(token, 0, "the primary has a log, so this is not the immediate path");
        io.commit_durable(token).await.unwrap();
    }
    assert!(marks_in(backup.path()).contains(&"__mirror_behind.mi.db".to_string()), "{:?}", marks_in(backup.path()));
    let primary = std::fs::read(root.path().join("mi.db")).unwrap();
    assert_eq!(&primary[100..108], &[0xCC; 8]);
    assert_ne!(primary, std::fs::read(backup.path().join("mi.db")).unwrap(), "the failed write reached the backup");
    drop(MirrorStorageProvider::attach(&root_path, std::slice::from_ref(&backup_path), "mi.db").await);
    assert_same(root.path(), backup.path(), &["mi.db"], "after the next attach");
    assert!(marks_in(backup.path()).is_empty());
}

/// `write_data_seek` wrote each backup with the backup's own
/// `write_data_seek`, whose failure nothing saw: the backup kept older
/// bytes at the same length and no mark said so. It goes through
/// `write_backup_or_mark` now, as every immediate write does.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_seek_write_to_a_backup_leaves_a_mark() {
    use crate::core::storage::mirror::MirrorStorageProvider;
    use std::io::SeekFrom;
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let root_path = root.path().to_string_lossy().into_owned();
    let backup_path = backup.path().to_string_lossy().into_owned();
    {
        let io = MirrorStorageProvider::attach(&root_path, std::slice::from_ref(&backup_path), "ms.db").await;
        io.append_data(&vec![0u8; 4096], true).await;
        io.commit_durable(io.begin_txn()).await.unwrap();
        // A seek write that succeeds reaches the backup, with no mark.
        io.write_data_seek(SeekFrom::Start(10), &[0xAA; 4]).await;
        io.write_data_seek(SeekFrom::End(-8), &[0xBB; 4]).await;
        assert!(marks_in(backup.path()).is_empty(), "{:?}", marks_in(backup.path()));
        let copy = std::fs::read(backup.path().join("ms.db")).unwrap();
        assert_eq!((&copy[10..14], &copy[4088..4092]), (&[0xAA; 4][..], &[0xBB; 4][..]));

        crate::core::storage_providers::file_sync::failed_appends::FAIL_INPLACE_IN.lock().push(backup_path.clone());
        io.write_data_seek(SeekFrom::Start(100), &[0xCC; 8]).await;
    }
    assert!(marks_in(backup.path()).contains(&"__mirror_behind.ms.db".to_string()), "{:?}", marks_in(backup.path()));
    let primary = std::fs::read(root.path().join("ms.db")).unwrap();
    assert_eq!(&primary[100..108], &[0xCC; 8]);
    assert_ne!(primary, std::fs::read(backup.path().join("ms.db")).unwrap(), "the failed write reached the backup");
    drop(MirrorStorageProvider::attach(&root_path, std::slice::from_ref(&backup_path), "ms.db").await);
    assert_same(root.path(), backup.path(), &["ms.db"], "after the next attach");
    assert!(marks_in(backup.path()).is_empty());
}
