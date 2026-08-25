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

    // Touch every sidecar so the lazily-created ones exist.
    let table = db.tables.get("filelist").expect("table").clone();
    for suffix in ["_schema_FK.db", "_schema_DEF.db", "_schema_UK.db", "_schema_JSON.db"] {
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

    assert!(db.tables.contains_key("roles"), "the new name is not live");
    assert!(!db.tables.contains_key("role"), "the old name is still live");

    // The catalogue is what survives a restart, so it is what the assertion has
    // to be about.
    assert!(db.catalogue_contains("roles").await, "the catalogue lost the new name");
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
    assert!(db.tables.contains_key("roles"), "the rename did not survive a reopen");
    assert!(
        !db.tables.contains_key("role"),
        "the old name came back, so the table exists twice after a restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_record_carries_the_new_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "role").await;
    db.rename_table("role", "roles").await.expect("rename");

    let schema_io = Arc::new(db.db_io_pointers.create_new("roles_schema.db".to_string()).await);
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
    assert!(db.tables.contains_key("role"));
    assert!(db.tables.contains_key("roles"));
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
    assert!(db.tables.contains_key("role"));
    assert!(db.catalogue_contains("role").await);
}

async fn insert_id(db: &Database, table: &str, value: i32) {
    let table = db.tables.get(table).expect("table").clone();
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
    let table = db.tables.get(table).expect("table").clone();
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
        db.record_rename_intent("role", "roles").await.expect("intent");
        db.copy_table_files("role", "roles").await.expect("copy");
        db.rewrite_schema_name("roles").await.expect("schema");
        db.remember_in_catalogue("roles").await.expect("catalogue");
        assert!(db.catalogue_contains("role").await);
        assert!(db.catalogue_contains("roles").await);
    }

    // Reopening completes it: the catalogue recorded the commit, so recovery
    // finishes rather than unwinds.
    let db = open_db(dir.path()).await;
    assert!(db.tables.contains_key("roles"), "recovery lost the renamed table");
    assert!(
        !db.tables.contains_key("role"),
        "recovery left the duplicate in place, so the table exists twice"
    );
    let mut ids = read_ids(&db, "roles").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "recovery kept the name but lost the rows");
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
        db.record_rename_intent("role", "roles").await.expect("intent");
        db.copy_table_files("role", "roles").await.expect("copy");
    }

    // The catalogue names only the original, so that is the truth and the copy
    // belongs to nobody.
    let db = open_db(dir.path()).await;
    assert!(
        db.tables.contains_key("role"),
        "recovery discarded the original after a rename that never committed"
    );
    assert!(
        !db.tables.contains_key("roles"),
        "an uncommitted rename produced a visible table"
    );
    let mut ids = read_ids(&db, "role").await;
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "the original lost rows to a failed rename");

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

    assert!(db.drop_table("gone").await.expect("drop"), "the table was not dropped");

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
        columns: vec![CreateColumnData { name: "id".to_string(), data_type: DbType::I32 }],
        unique_columns: Vec::new(),
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_keys: Vec::new(),
        type_modifiers: Vec::new(),
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
    assert!(db.tables.contains_key(&shadow), "setup did not create the shadow");

    db.drop_table("owner").await.expect("drop");

    assert!(
        !db.tables.contains_key(&shadow),
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
        .filter(|n| n.starts_with(&format!("{}.", shadow)) || n.starts_with(&format!("{}_", shadow)))
        .collect();
    assert!(leftovers.is_empty(), "the shadow's files survived: {:?}", leftovers);
}
