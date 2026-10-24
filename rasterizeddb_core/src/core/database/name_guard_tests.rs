//! Tests for which names a table may take, and for sizing a table without
//! touching its directory.
//!
//! A table's name is the stem of every file it owns, and until these guards
//! nothing checked that two stems stayed apart: `foo_schema`'s rows file is
//! `foo`'s schema, and dropping either deleted the other's. Every failure was
//! silent — the create succeeded and the damage showed after a reopen — so the
//! tests reopen before they look.

use super::*;
use crate::core::rql::lexer_ct::CreateColumnData;

async fn open_db(dir: &std::path::Path) -> Database {
    Database::new_named(&dir.to_string_lossy(), false, "nameguardtests").await
}

fn create_stmt(name: &str) -> CreateTable {
    CreateTable {
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
    }
}

async fn create(db: &Database, name: &str) {
    db.add_table(create_stmt(name)).await.expect("create table");
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

/// The table's column names and its ids, or `None` when it did not come back.
async fn contents(db: &Database, table: &str) -> Option<(Vec<String>, Vec<i32>)> {
    let table = db.table_for_tests(table)?.clone();
    let columns = table.schema.fields.iter().map(|f| f.name.clone()).collect();
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
        .ok()?;
    let mut ids: Vec<i32> = rows
        .iter()
        .filter_map(|r| r.columns.first())
        .filter_map(|c| c.data.into_slice().try_into().ok().map(i32::from_le_bytes))
        .collect();
    ids.sort();
    Some((columns, ids))
}

fn intact(ids: std::ops::RangeInclusive<i32>) -> Option<(Vec<String>, Vec<i32>)> {
    Some((vec!["id".to_string()], ids.collect()))
}

fn listing(dir: &std::path::Path) -> Vec<(String, u64)> {
    let mut files: Vec<(String, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| {
            let len = e.metadata().map(|m| m.len()).unwrap_or(0);
            (e.file_name().to_string_lossy().to_string(), len)
        })
        .collect();
    files.sort();
    files
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_named_after_another_tables_file_is_refused() {
    // Both orders, because the damage differs: `foo_schema` after `foo` let a
    // drop of `foo_schema` delete `foo`'s schema, and `foo` after `foo_seq`
    // wrote its sequence into `foo_seq`'s rows.
    for (first, second) in [
        ("foo", "foo_seq"),
        ("foo_seq", "foo"),
        ("foo", "foo_schema"),
        ("foo_schema", "foo"),
        ("foo", "foo_pointers"),
        ("foo", "foo_flt"),
        ("foo", "foo_schema_FIELDS"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open_db(dir.path()).await;
            create(&db, first).await;
            for i in 1..=10 {
                insert_id(&db, first, i).await;
            }

            let err = db.add_table(create_stmt(second)).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::AlreadyExists,
                "{first} then {second}"
            );
            assert!(
                err.to_string().contains(&format!("\"{}\"", first)),
                "the refusal should name the table it collides with: {err}"
            );
            assert!(!db.has_table(second));
            assert!(
                !db.catalogue_contains(second).await,
                "a refused create left a catalogue row for {second}"
            );
        }

        let db = open_db(dir.path()).await;
        assert_eq!(
            contents(&db, first).await,
            intact(1..=10),
            "{first} was damaged by an attempt to create {second}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_named_after_the_catalogues_files_is_refused() {
    // `db_data` used to come back with the catalogue's schema in place of its
    // own, its rows' pointers written into the catalogue's pointer file.
    for name in [
        "db_data",
        "db_data_rows",
        "db_data_schema",
        "db_data_pointers",
    ] {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open_db(dir.path()).await;
            create(&db, "keep").await;
            for i in 1..=5 {
                insert_id(&db, "keep", i).await;
            }
            let err = db.add_table(create_stmt(name)).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{name}");
            assert!(err.to_string().contains("catalogue"), "{err}");
        }

        let db = open_db(dir.path()).await;
        let mut names: Vec<String> = db.table_names();
        names.sort();
        assert_eq!(names, vec!["keep".to_string()], "after trying {name}");
        assert_eq!(contents(&db, "keep").await, intact(1..=5));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rename_onto_another_tables_files_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "foo").await;
        create(&db, "bar").await;
        for i in 1..=5 {
            insert_id(&db, "foo", i).await;
            insert_id(&db, "bar", i + 100).await;
        }

        for (from, to) in [
            ("bar", "foo_seq"),
            ("bar", "foo_schema"),
            // Onto its own sidecar: the copy writes `foo.db` over `foo_seq.db`
            // before it has copied `foo_seq.db` anywhere.
            ("foo", "foo_seq"),
            ("bar", "db_data"),
            ("bar", "db_data_rows"),
        ] {
            let err = db.rename_table(from, to).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{from} -> {to}");
            assert!(db.has_table(from), "{from} -> {to} lost {from}");
            assert!(!db.has_table(to));
        }
    }

    let db = open_db(dir.path()).await;
    assert_eq!(contents(&db, "foo").await, intact(1..=5));
    assert_eq!(contents(&db, "bar").await, intact(101..=105));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leftover_file_is_not_inherited() {
    // What a crash between a DROP's tombstone and its file deletion leaves: no
    // table owns the bytes, and a new table of that name would open them as its
    // own rows.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "live").await;
    std::fs::write(dir.path().join("ghost.db"), b"stale rows").unwrap();
    std::fs::write(dir.path().join("spook_seq.db.tmp"), b"staged").unwrap();

    for name in ["ghost", "spook"] {
        let err = db.add_table(create_stmt(name)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{name}");
        assert!(err.to_string().contains("belongs to no table"), "{err}");
        assert!(!db.catalogue_contains(name).await);

        let err = db.rename_table("live", name).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "rename to {name}");
        assert!(db.has_table("live"));
    }

    // Untouched: refusing is all the guard does.
    assert_eq!(
        std::fs::read(dir.path().join("ghost.db")).unwrap(),
        b"stale rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn names_that_are_not_plain_file_stems_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    // A sibling the `../` case would land in, so its escape is observable.
    let db_dir = dir.path().join("db");
    {
        let db = open_db(&db_dir).await;
        create(&db, "x").await;
        for i in 1..=5 {
            insert_id(&db, "x", i).await;
        }

        // `x.db`'s sidecars are named with every `.db` stripped, so its columns
        // went into `x`'s field list: `x` came back with two columns and three
        // of its five rows.
        for name in [
            "x.db",
            "a.dbx",
            "../escape",
            "sub/dir",
            "back\\slash",
            "__pt_0000000000000001_00000000",
            "__pt_",
            "",
        ] {
            let err = db.add_table(create_stmt(name)).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "create {name:?}");
            let err = db.rename_table("x", name).await.unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidInput,
                "rename to {name:?}"
            );
        }
        assert!(
            db.add_table(create_stmt("__pt_x"))
                .await
                .unwrap_err()
                .to_string()
                .contains("partitioned"),
            "the reason should say what the prefix is for"
        );
    }

    let db = open_db(&db_dir).await;
    assert_eq!(contents(&db, "x").await, intact(1..=5));
    assert!(
        !dir.path().join("escape.db").exists(),
        "a table's files escaped the database directory"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_prefixed_name_is_refused_as_a_destination_only() {
    // No such table can be created any more, but one made before the guard can
    // still be in a catalogue, and renaming it is the only way off the prefix:
    // its files are ordinary `<name><suffix>` ones, which the copy finds.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "plain").await;
        let err = db.rename_table("plain", "__pt_plain").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(db.has_table("plain"));

        db.add_table_unguarded(create_stmt("__pt_legacy"))
            .await
            .expect("a table as an older build would have made it");
        for i in 1..=5 {
            insert_id(&db, "__pt_legacy", i).await;
        }
        db.rename_table("__pt_legacy", "legacy")
            .await
            .expect("renaming off the reserved prefix");
    }

    let db = open_db(dir.path()).await;
    assert!(!db.has_table("__pt_legacy"));
    assert_eq!(contents(&db, "legacy").await, intact(1..=5));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_too_long_for_the_schema_record_is_refused() {
    // 240 bytes: past NAME_MAX once `_schema_FIELDS.db.tmp` is added. The
    // create panicked opening its files after writing the catalogue row, and
    // every reopen then panicked the same way. 129 is the first length the
    // schema record would have truncated.
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path()).await;
        create(&db, "keep").await;
        insert_id(&db, "keep", 1).await;
        for len in [129, 240] {
            let name = "b".repeat(len);
            let err = db.add_table(create_stmt(&name)).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{len} bytes");
            assert!(!db.catalogue_contains(&name).await, "{len} bytes");
        }
        // The longest name still allowed is a working table.
        create(&db, &"c".repeat(MAX_TABLE_NAME_BYTES)).await;
    }

    let db = open_db(dir.path()).await;
    assert_eq!(contents(&db, "keep").await, intact(1..=1));
    assert!(db.has_table(&"c".repeat(MAX_TABLE_NAME_BYTES)));
    assert_eq!(db.table_count(), 2);
}

#[test]
fn the_longest_name_fits_in_a_file_name() {
    let longest = RENAMED_SUFFIXES.iter().map(|s| s.len()).max().unwrap();
    assert!(MAX_TABLE_NAME_BYTES + longest + ".tmp".len() <= 255);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_named_after_an_engine_file_is_refused() {
    // `temp.db` is the compaction scratch, truncated on use and left out of
    // every S3 snapshot by name: a table `temp` lost its rows to both.
    for (name, owner) in [("temp", "compaction"), ("CONFIG_TABLE", "configuration")] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).await;
        create(&db, "keep").await;
        let err = db.add_table(create_stmt(name)).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{name}");
        assert!(err.to_string().contains(owner), "{err}");
        assert!(!db.catalogue_contains(name).await, "{name}");
        let err = db.rename_table("keep", name).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "rename to {name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_can_ask_before_it_drops() {
    // An ALTER rewrite drops the table and creates it again; a refusal of the
    // create comes after every row is released. `check_recreatable` is the
    // question to ask first, and it must agree with what the create will say.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    create(&db, "plain").await;
    insert_id(&db, "plain", 1).await;
    db.check_recreatable("plain")
        .expect("a table's own files are not leftovers");

    db.add_table_unguarded(create_stmt("__pt_legacy")).await.unwrap();
    db.add_table_unguarded(create_stmt("pair")).await.unwrap();
    db.add_table_unguarded(create_stmt("pair_seq")).await.unwrap();
    for name in ["__pt_legacy", "pair", "pair_seq"] {
        assert!(db.check_recreatable(name).is_err(), "{name}");
    }

    // And the drop-then-create it vouched for goes through.
    assert!(db.drop_table("plain").await.unwrap());
    create(&db, "plain").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rewriting_alter_on_a_legacy_name_refuses_before_the_drop() {
    // A table an older build accepted under a name `add_table` now refuses.
    // ADD COLUMN on a populated table rewrites it: drop, then create again. The
    // statement must fail while the rows are still there, not after.
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(open_db(dir.path()).await);
    db.add_table_unguarded(create_stmt("__pt_legacy")).await.unwrap();
    for i in 1..=3 {
        insert_id(&db, "__pt_legacy", i).await;
    }

    let sql = "ALTER TABLE __pt_legacy ADD COLUMN extra INT";
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).expect("parse");
    let reply = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert_eq!(reply.first(), Some(&3u8), "the rewrite must be refused");

    assert_eq!(contents(&db, "__pt_legacy").await, intact(1..=3));
    drop(db);
    let db = open_db(dir.path()).await;
    assert_eq!(contents(&db, "__pt_legacy").await, intact(1..=3), "after reopen");
}

#[tokio::test(flavor = "multi_thread")]
async fn names_that_only_share_a_prefix_still_work() {
    // The guard must not be a `starts_with`: `role` and `roles` share no file.
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;
    for name in [
        "foo",
        "foos",
        "foo_s",
        "foo_seqs",
        "foo_schemas",
        "seq",
        "db_datas",
        "data",
    ] {
        create(&db, name).await;
    }
    db.rename_table("foos", "foo_sequel").await.expect("rename");

    // A drop leaves nothing behind for the leftover check to trip on, so the
    // drop-and-recreate an ALTER rewrite does still works.
    insert_id(&db, "foo", 1).await;
    assert!(db.drop_table("foo").await.unwrap());
    create(&db, "foo").await;
    assert_eq!(contents(&db, "foo").await, intact(1..=0));
}

#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn sizing_a_table_creates_no_files() {
    // `table_bytes` opened every possible file to ask its length, which made
    // all of them: a STORAGE REBALANCE DRY RUN created nine empty sidecars and
    // their `.tmp`s for each table, and fifteen for a name with no table.
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(open_db(dir.path()).await);
    create(&db, "sized").await;
    for i in 1..=5 {
        insert_id(&db, "sized", i).await;
    }

    // Staged bytes nothing has folded yet. A freshly written table's `.tmp`s
    // are all empty, so without this the sum would pass with them left out.
    std::fs::write(dir.path().join("sized_schema_FK.db.tmp"), b"staged").unwrap();

    let before = listing(dir.path());
    let bytes = db.table_bytes("sized").await;
    let _ = crate::core::storage::relocate::rebalance(&db, true).await;
    let _ = db.table_bytes("absent").await;
    let after = listing(dir.path());
    assert_eq!(before, after, "sizing a table changed its directory");

    // The same number an open would have reported: every file the table owns,
    // staged bytes included. Exact-with-separator, so `sized` cannot match a
    // longer name's files.
    let expected: u64 = before
        .iter()
        .filter(|(n, _)| n.starts_with("sized.") || n.starts_with("sized_"))
        .map(|(_, len)| len)
        .sum();
    assert!(expected > 6);
    assert_eq!(bytes, expected);
}

#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn sizing_an_in_memory_table_registers_nothing() {
    let db = Database::new_named("", true, "nameguardmemory").await;
    create(&db, "sized").await;
    for i in 1..=5 {
        insert_id(&db, "sized", i).await;
    }
    let DbStorageProvider::Memory(memory) = &*db.db_io_pointers else {
        panic!("an in-memory database should have an in-memory catalogue");
    };
    let rows = memory.stored_len("sized.db").expect("rows file");
    let pointers = memory.stored_len("sized_pointers.db").expect("pointers");
    assert!(rows > 0 && pointers > 0);

    let bytes = db.table_bytes("sized").await;
    assert!(bytes >= rows + pointers, "{bytes} < {rows} + {pointers}");
    assert_eq!(
        memory.stored_len("sized_schema_FK.db"),
        None,
        "sizing registered a file the table never had"
    );
}

#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_backups_copies_do_not_block_a_recreate() {
    // A drop sweeps the enabled backups only, so a disabled one keeps its
    // copies. Counted as leftovers, they refused the create an ALTER rewrite
    // makes right after its drop, and the table was gone for good.
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let backup_path = backup.path().to_string_lossy().into_owned();
    let db = open_db(root.path()).await;
    db.storage
        .add_location(
            &backup_path,
            crate::core::storage::LocationRole::Backup,
            1,
            true,
        )
        .unwrap();
    create(&db, "t").await;
    insert_id(&db, "t", 1).await;
    db.storage.set_enabled(&backup_path, false).unwrap();

    db.check_recreatable("t").expect("before the drop");
    assert!(db.drop_table("t").await.unwrap());
    assert!(
        listing(backup.path()).iter().any(|(n, _)| n.starts_with("t_") || n.starts_with("t.")),
        "the disabled backup should still hold t's copies, or this tests nothing"
    );
    db.add_table(create_stmt("t"))
        .await
        .expect("recreating after the drop");
    insert_id(&db, "t", 2).await;
    assert_eq!(contents(&db, "t").await, intact(2..=2));
}
