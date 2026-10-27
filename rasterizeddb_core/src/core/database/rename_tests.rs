//! Tests for `ALTER TABLE ... RENAME TO`.
//!
//! The interesting cases here are not "does the name change" but what a crash
//! at each step leaves behind, so most of these drive the phases individually
//! rather than calling `rename_table` and asserting the happy path.

use super::*;
use crate::core::row::schema::TableSchema;
use crate::core::rql::lexer_ct::CreateColumnData;

async fn open_db(dir: &std::path::Path) -> Database {
    Database::new_named(&dir.to_string_lossy(), false, "renametests").await
}

async fn create(db: &Database, name: &str) {
    let create = CreateTable {
        table_name: name.to_string(),
        columns: vec![CreateColumnData {
            name: "id".to_string(),
            data_type: DbType::I32,
        }],
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
    };
    db.add_table(create).await.expect("create table");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rename_file_list_matches_what_a_table_creates() {
    // The list is the thing most likely to fall out of date, and the cost of it
    // doing so is silent: a sidecar added later and not added here would simply
    // be left behind, and the renamed table would come back missing whatever it
    // held. So this compares the list against the files a real table actually
    // produces rather than against a second hand-written list.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "filelist").await;

    // Reopening is what makes the lazily-created sidecars exist: several of
    // them are only opened when a schema is *loaded*, so a table that has been
    // created and never reopened has fewer files on disk than the same table
    // after a restart. Asserting against the freshly-created set is how three
    // sidecars — CHK, GEN and PIX — sat outside this list unnoticed: they were
    // never on disk while this test was looking.
    let table = db.table_for_tests("filelist").expect("table").clone();
    let schema_io = db
        .db_io_pointers
        .create_new("filelist_schema.db".to_string())
        .await;
    let _ = TableSchema::load(Arc::new(schema_io))
        .await
        .expect("load the schema back");
    for suffix in [
        "_schema_FK.db",
        "_schema_DEF.db",
        "_schema_UK.db",
        "_schema_JSON.db",
    ] {
        let io = db
            .db_io_pointers
            .create_new(format!("filelist{}", suffix))
            .await;
        io.append_data(b"x", true).await;
    }
    drop(table);

    let mut on_disk: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("filelist"))
        .map(|n| n.trim_start_matches("filelist").to_string())
        // `.tmp` is scratch from `create_temp`, not part of the table's durable
        // state. It is deliberately *not* copied: a temp file present at rename
        // time is a half-written rewrite, and carrying it across would hand the
        // new table a corrupt spare rather than leaving the mess behind.
        .filter(|n| !n.ends_with(".tmp"))
        .collect();
    on_disk.sort();

    let mut known: Vec<String> = RENAMED_SUFFIXES.iter().map(|s| s.to_string()).collect();
    known.sort();

    for suffix in &on_disk {
        assert!(
            known.contains(suffix),
            "a table creates {}{} but the rename list does not move it, so a rename \
             would silently leave it behind",
            "filelist",
            suffix
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_moves_the_table_and_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;

    db.rename_table("role", "roles").await.expect("rename");

    assert!(db.has_table("roles"), "the new name is not live");
    assert!(
        !db.has_table("role"),
        "the old name is still live"
    );

    // The catalogue is what survives a restart, so it is what the assertion has
    // to be about.
    assert!(
        db.catalogue_contains("roles").await,
        "the catalogue lost the new name"
    );
    assert!(
        !db.catalogue_contains("role").await,
        "the old catalogue row was not tombstoned, so the table comes back twice"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_renamed_table_survives_a_reopen_under_its_new_name() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "role").await;
        db.rename_table("role", "roles").await.expect("rename");
    }

    // The point of the whole ordering: what the catalogue says is what comes
    // back. An in-memory rename that does not survive here is not a rename.
    let db = open_db(dir.path()).await;
    assert!(
        db.has_table("roles"),
        "the rename did not survive a reopen"
    );
    assert!(
        !db.has_table("role"),
        "the old name came back, so the table exists twice after a restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_record_carries_the_new_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;
    db.rename_table("role", "roles").await.expect("rename");

    let schema_io = Arc::new(
        db.db_io_pointers
            .create_new("roles_schema.db".to_string())
            .await,
    );
    let schema = TableSchema::load(schema_io).await.expect("load schema");
    assert_eq!(
        schema.name, "roles",
        "the schema still names the old table, so two tables share a cache scope"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_onto_an_existing_name_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;
    create(&db, "roles").await;

    let err = db.rename_table("role", "roles").await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    // Refusing has to leave both intact — a refusal that half-ran would be
    // worse than the unsupported error this replaced.
    assert!(db.has_table("role"));
    assert!(db.has_table("roles"));
}

#[tokio::test(flavor = "multi_thread")]
async fn renaming_a_table_that_is_not_there_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    let err = db.rename_table("absent", "present").await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_to_the_same_name_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;
    db.rename_table("role", "role").await.expect("no-op rename");
    assert!(db.has_table("role"));
    assert!(db.catalogue_contains("role").await);
}

async fn insert_id(db: &Database, table: &str, value: i32) {
    let table = db.table_for_tests(table).expect("table").clone();
    let mut mb = MEMORY_POOL.acquire(4);
    mb.into_slice_mut().copy_from_slice(&value.to_le_bytes());
    let row = RowWrite {
        columns_writing_data: smallvec![ColumnWritePayload {
            column_type: DbType::I32,
            size: 4,
            data: mb,
            write_order: 0,
        }],
    };
    table.insert_row(row).await.expect("insert");
}

fn fetch_all(fields: &SmallVec<[SchemaField; 20]>) -> crate::core::row::row::RowFetch {
    crate::core::row::row::RowFetch {
        columns_fetching_data: fields
            .iter()
            .map(|f| crate::core::row::row::ColumnFetchingData {
                column_offset: f.offset as u32,
                column_type: f.db_type.clone(),
                size: f.size,
                schema_id: f.write_order,
            })
            .collect(),
    }
}

async fn read_ids(db: &Database, table: &str) -> Vec<i32> {
    let table = db.table_for_tests(table).expect("table").clone();
    let rows = table
        .query_row(
            "",
            &table.schema.fields.to_vec(),
            fetch_all(&table.schema.fields),
            fetch_all(&table.schema.fields),
            None,
            None,
        )
        .await
        .expect("query");
    rows.iter()
        .map(|r| i32::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rows_come_with_the_table() {
    // The assertion the whole feature stands on. Every other test here can pass
    // with an empty file copied to a new name: the catalogue would say the
    // right thing, the schema would say the right thing, and every row would be
    // gone. `.db` is the file that carries them and the one an early draft of
    // the suffix list omitted.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "role").await;
        for i in 1..=25 {
            insert_id(&db, "role", i).await;
        }
        assert_eq!(read_ids(&db, "role").await.len(), 25, "setup did not land");

        db.rename_table("role", "roles").await.expect("rename");

        let mut ids = read_ids(&db, "roles").await;
        ids.sort();
        assert_eq!(
            ids,
            (1..=25).collect::<Vec<i32>>(),
            "the rows did not come across with the table"
        );
    }

    // And across a restart, which is where a copy that never reached the disk
    // would show up.
    let db = open_db(dir.path()).await;
    let mut ids = read_ids(&db, "roles").await;
    ids.sort();
    assert_eq!(
        ids,
        (1..=25).collect::<Vec<i32>>(),
        "the copied rows did not survive a reopen"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_finishes_a_rename_that_reached_the_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "role").await;
        for i in 1..=5 {
            insert_id(&db, "role", i).await;
        }

        // A crash in the window the ordering deliberately produces: the new
        // catalogue row is written, the old one is not yet tombstoned. Both
        // names exist and both read.
        db.record_rename_intent("role", "roles")
            .await
            .expect("intent");
        db.copy_table_files("role", "roles").await.expect("copy");
        db.rewrite_schema_name("roles").await.expect("schema");
        db.remember_in_catalogue("roles").await.expect("catalogue");
        assert!(db.catalogue_contains("role").await);
        assert!(db.catalogue_contains("roles").await);
    }

    // Reopening completes it: the catalogue recorded the commit, so recovery
    // finishes rather than unwinds.
    let db = open_db(dir.path()).await;
    assert!(
        db.has_table("roles"),
        "recovery lost the renamed table"
    );
    assert!(
        !db.has_table("role"),
        "recovery left the duplicate in place, so the table exists twice"
    );
    let mut ids = read_ids(&db, "roles").await;
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5],
        "recovery kept the name but lost the rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_abandons_a_rename_that_never_reached_the_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "role").await;
        for i in 1..=5 {
            insert_id(&db, "role", i).await;
        }

        // A crash before the commit point: files copied, catalogue untouched.
        db.record_rename_intent("role", "roles")
            .await
            .expect("intent");
        db.copy_table_files("role", "roles").await.expect("copy");
    }

    // The catalogue names only the original, so that is the truth and the copy
    // belongs to nobody.
    let db = open_db(dir.path()).await;
    assert!(
        db.has_table("role"),
        "recovery discarded the original after a rename that never committed"
    );
    assert!(
        !db.has_table("roles"),
        "an uncommitted rename produced a visible table"
    );
    let mut ids = read_ids(&db, "role").await;
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5],
        "the original lost rows to a failed rename"
    );

    // The orphaned copy is gone rather than left to accumulate.
    let leftover = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("roles"));
    assert!(!leftover, "the abandoned copy was left on disk");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_successful_rename_leaves_no_files_behind() {
    // The abandoned-rename test already checks for orphans, but the *successful*
    // path is the one that runs every time, so a leak there accumulates a whole
    // table's worth of files per rename rather than per crash.
    //
    // The prefix check has to be exact-with-separator: `role` is a prefix of
    // `roles`, so a naive `starts_with("role")` would match the renamed table's
    // own files and pass while every original was still on disk.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;
    for i in 1..=10 {
        insert_id(&db, "role", i).await;
    }
    db.rename_table("role", "roles").await.expect("rename");

    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("role.") || n.starts_with("role_"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "the original's files survived the rename: {:?}",
        leftovers
    );

    // And the replacement really is there, so the assertion above cannot pass by
    // the rename having done nothing at all.
    let mut ids = read_ids(&db, "roles").await;
    ids.sort();
    assert_eq!(ids, (1..=10).collect::<Vec<i32>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_table_removes_every_file_it_created() {
    // `drop_table` releases the five handles a `Table` holds and nothing else,
    // but a table owns ten files: the `_schema_*` sidecars are opened lazily and
    // so were never in a handle to release. They survived the drop.
    //
    // The same list the rename moves is the list a drop has to delete, which is
    // what makes one guard cover both directions: a sidecar added later now
    // fails here as well as there.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "gone").await;
    for i in 1..=10 {
        insert_id(&db, "gone", i).await;
    }

    assert!(
        db.drop_table("gone").await.expect("drop"),
        "the table was not dropped"
    );

    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("gone.") || n.starts_with("gone_"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "DROP TABLE left files behind: {:?}",
        leftovers
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_table_takes_its_json_shadow_tables_with_it() {
    use crate::core::rql::json_map;

    // A shadow table is storage the engine created to back a JSON column. It is
    // owned by the parent's column, not by the user, so nothing will ever drop
    // it by name — when the parent goes it becomes unreachable garbage that
    // still shows up in the catalogue as a table.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;

    let shadow = json_map::shadow_table_name("owner", "doc");
    let mut create_owner = CreateTable {
        table_name: "owner".to_string(),
        columns: vec![CreateColumnData {
            name: "id".to_string(),
            data_type: DbType::I32,
        }],
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
    };
    create_owner.json_columns.push(json_map::JsonColumn {
        column: "doc".to_string(),
        shadow_table: shadow.clone(),
        is_deleted: false,
    });
    db.add_table(create_owner).await.expect("create owner");

    // The executor creates the shadow lazily on first write; standing in for it
    // here so the drop has something to find.
    create(&db, &shadow).await;
    assert!(
        db.has_table(&shadow),
        "setup did not create the shadow"
    );

    db.drop_table("owner").await.expect("drop");

    assert!(
        !db.has_table(&shadow),
        "the JSON shadow table outlived the table that owns it"
    );
    assert!(
        !db.catalogue_contains(&shadow).await,
        "the shadow's catalogue row survived, so it comes back as a table on the next open"
    );
    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| {
            n.starts_with(&format!("{}.", shadow)) || n.starts_with(&format!("{}_", shadow))
        })
        .collect();
    assert!(
        leftovers.is_empty(),
        "the shadow's files survived: {:?}",
        leftovers
    );
}

// ---------------------------------------------------------------------------
// Intents recovery must not act on
// ---------------------------------------------------------------------------

/// Opens `dir` with `intent` hand-written as the rename intent, twice (the
/// second open is where a reaper would have taken files the first one
/// orphaned), and returns the database.
async fn reopen_with_intent(dir: &std::path::Path, intent: &[u8]) -> Database {
    crate::core::wal::remove_for(&dir.to_string_lossy());
    std::fs::write(dir.join(super::RENAME_INTENT_FILE), intent).unwrap();
    {
        let _db = open_db(dir).await;
    }
    crate::core::wal::remove_for(&dir.to_string_lossy());
    open_db(dir).await
}

fn intent_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("__rename."))
        .collect();
    names.sort();
    names
}

async fn assert_intact(db: &Database, dir: &std::path::Path, table: &str, rows: &[i32], label: &str) {
    assert!(db.has_table(table), "{label}: the table {table} is gone");
    assert!(db.catalogue_contains(table).await, "{label}: {table}'s catalogue row was tombstoned");
    assert!(
        dir.join(format!("{table}.db")).exists(),
        "{label}: {table}'s files were deleted"
    );
    let mut ids = read_ids(db, table).await;
    ids.sort();
    assert_eq!(ids, rows, "{label}: {table} lost rows");
}

/// A power cut tore the intent of `RENAME pt TO pt2` after five bytes:
/// `pt\npt`, which the older two-line form read as "rename pt to pt". Both
/// catalogue lookups found the one row, so recovery took the rename for
/// committed, tombstoned the only row and discarded `pt`'s files.
///
/// Mutation: `parse_intent` accepting the two-line form again (and the
/// degenerate check gone) loses `pt`.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_intent_naming_one_table_twice_does_not_drop_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "pt").await;
        for i in 1..=4 {
            insert_id(&db, "pt", i).await;
        }
        db.sync_durable().unwrap();
    }
    let db = reopen_with_intent(dir.path(), b"pt\npt").await;
    assert_intact(&db, dir.path(), "pt", &[1, 2, 3, 4], "torn pt\\npt").await;
    assert_eq!(
        intent_files(dir.path()),
        vec!["__rename.intent.bad-1".to_string()],
        "a malformed intent is kept aside for an operator, not deleted or left in place"
    );
}

/// The checksummed form naming one table twice is no rename either, and the
/// same tombstone would follow from it.
#[tokio::test(flavor = "multi_thread")]
async fn a_checksummed_intent_naming_one_table_twice_does_not_drop_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "pt").await;
        for i in 1..=3 {
            insert_id(&db, "pt", i).await;
        }
        db.sync_durable().unwrap();
    }
    let db = reopen_with_intent(dir.path(), super::encode_intent("pt", "pt").as_bytes()).await;
    assert_intact(&db, dir.path(), "pt", &[1, 2, 3], "checksummed pt -> pt").await;
}

/// A two-line intent is indistinguishable from a checksummed one torn just
/// before its sum: `orders_archive\norders` may be the start of a rename of
/// `orders_archive` to `orders_x`. Read as written, both rows exist, so
/// recovery would tombstone `orders_archive` and discard its files. It is
/// refused, and both tables stay.
#[tokio::test(flavor = "multi_thread")]
async fn a_two_line_intent_is_not_acted_on() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "orders").await;
        create(&db, "orders_archive").await;
        insert_id(&db, "orders", 1).await;
        insert_id(&db, "orders_archive", 2).await;
        insert_id(&db, "orders_archive", 3).await;
        db.sync_durable().unwrap();
    }
    let db = reopen_with_intent(dir.path(), b"orders_archive\norders").await;
    assert_intact(&db, dir.path(), "orders", &[1], "two-line intent").await;
    assert_intact(&db, dir.path(), "orders_archive", &[2, 3], "two-line intent").await;
}

/// Torn anywhere, a checksummed intent is refused: every prefix of one, and
/// one with empty names.
#[test]
fn no_prefix_of_an_intent_parses() {
    let intent = super::encode_intent("orders_archive", "orders");
    for cut in 0..intent.len() {
        assert_eq!(super::parse_intent(&intent[..cut]), None, "a prefix of {cut} bytes parsed");
    }
    assert!(super::parse_intent(&intent).is_some());
    assert_eq!(super::parse_intent(&super::encode_intent("", "t")), None);
    assert_eq!(super::parse_intent(&super::encode_intent("t", "")), None);
    assert_eq!(super::parse_intent(&super::encode_intent("t", "t")), None);
}

// ---------------------------------------------------------------------------
// One intent at a time
// ---------------------------------------------------------------------------

/// Eight sessions renaming their own tables back and forth at once. There is
/// one `__rename.intent` and one `__rename.partial`: unserialised, one
/// rename installed another's bytes or deleted its live intent, and one
/// failed with "No such file or directory".
///
/// Mutation: `rename_serial` not taken fails a rename.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_renames_each_finish() {
    const SESSIONS: usize = 8;
    const ROUNDS: usize = 6;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db(dir.path()).await);
    for i in 0..SESSIONS {
        create(&db, &format!("ra{i}")).await;
        insert_id(&db, &format!("ra{i}"), i as i32).await;
    }
    let mut tasks = Vec::new();
    for i in 0..SESSIONS {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..ROUNDS {
                let (from, to) = if round % 2 == 0 { ("ra", "rb") } else { ("rb", "ra") };
                db.rename_table(&format!("{from}{i}"), &format!("{to}{i}"))
                    .await
                    .map_err(|e| format!("could not rename {from}{i} to {to}{i} (round {round}): {e}"))?;
            }
            Ok::<(), String>(())
        }));
    }
    let mut failures = Vec::new();
    for t in tasks {
        if let Err(e) = t.await.unwrap() {
            failures.push(e);
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
    for i in 0..SESSIONS {
        assert_eq!(read_ids(&db, &format!("ra{i}")).await, vec![i as i32]);
    }
    assert_eq!(intent_files(dir.path()), Vec::<String>::new(), "an intent outlived every rename");
}

/// The intent on disk while a rename is past its copy is that rename's, with
/// a second rename started meanwhile: it waits for the first to finish
/// rather than writing its own over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_rename_waits_for_the_first_intent() {
    use crate::core::power_loss::failpoint;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db(dir.path()).await);
    create(&db, "first").await;
    create(&db, "second").await;
    let seen = std::sync::Arc::new(parking_lot::Mutex::new(None::<String>));
    let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
    let parked_tx = parking_lot::Mutex::new(parked_tx);
    let intent = dir.path().join(super::RENAME_INTENT_FILE);
    let action: failpoint::Action = {
        let seen = seen.clone();
        std::sync::Arc::new(move |_hit| {
            let _ = parked_tx.lock().send(());
            std::thread::sleep(std::time::Duration::from_millis(400));
            *seen.lock() = std::fs::read_to_string(&intent).ok();
            failpoint::Outcome::Continue
        })
    };
    let guard = failpoint::arm("test.rename.after_copy", dir.path(), 1, action);
    let first = tokio::spawn({
        let db = db.clone();
        async move { db.rename_table("first", "first_moved").await }
    });
    tokio::task::spawn_blocking(move || parked_rx.recv_timeout(std::time::Duration::from_secs(30)))
        .await
        .unwrap()
        .expect("the first rename never reached its copy");
    let second = db.rename_table("second", "second_moved").await;
    first.await.unwrap().expect("first rename");
    second.expect("second rename");
    assert_eq!(guard.fired(), 1);
    drop(guard);
    assert_eq!(
        seen.lock().as_deref().and_then(super::parse_intent),
        Some(("first".to_string(), "first_moved".to_string())),
        "the first rename's intent was replaced while it ran"
    );
    assert!(db.has_table("first_moved") && db.has_table("second_moved"));
}

/// An intent whose deletion fails outlives the rename, so both names are
/// held until the next open finishes it: a table created under the old name
/// meanwhile would be what that open tombstones.
///
/// Mutation: the rename not holding the names lets the CREATE through.
#[tokio::test(flavor = "current_thread")]
async fn a_rename_whose_intent_stays_holds_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "kept").await;
    super::test_hooks::FAIL_INTENT_CLEAR.with(|f| f.set(true));
    db.rename_table("kept", "kept2").await.expect("the rename itself succeeds");
    assert!(!super::test_hooks::FAIL_INTENT_CLEAR.with(|f| f.get()), "the intent was never cleared");
    assert_eq!(intent_files(dir.path()), vec!["__rename.intent".to_string()]);
    let create_old = CreateTable {
        table_name: "kept".to_string(),
        columns: vec![CreateColumnData { name: "id".to_string(), data_type: DbType::I32 }],
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
    };
    assert!(db.add_table(create_old).await.is_err(), "a table was created under a name the kept intent names");
    // The next open finishes the rename and frees the name.
    drop(db);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
    let db = open_db(dir.path()).await;
    assert!(db.has_table("kept2") && !db.has_table("kept"));
    assert_eq!(intent_files(dir.path()), Vec::<String>::new());
}

/// The intent holds one name per line: a name with a line break is refused
/// before anything is written.
#[tokio::test(flavor = "multi_thread")]
async fn a_rename_intent_refuses_control_characters() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "plain").await;
    for bad in ["plain\nplain2", "tab\there", "cr\r"] {
        let e = db.record_rename_intent("plain", bad).await.expect_err(bad);
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{bad:?}: {e}");
        assert!(db.rename_table("plain", bad).await.is_err(), "{bad:?}");
    }
    assert_eq!(intent_files(dir.path()), Vec::<String>::new());
    assert!(db.has_table("plain"));
}
