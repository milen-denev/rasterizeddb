//! Partitioned tables through `Database` (S4): CREATE writes only
//! `__pt_<uid>_*` files and a `clusters = uid` row, a reopen resolves the
//! table as partitioned with its schema and rows, an older binary finds no
//! schema under the logical name (I18), two live rows sharing a partitioned
//! name or uid refuse the start, the per-database cap, the in-memory and
//! mirrored refusals, DROP of healthy and broken tables, and the forget
//! horizon. API level, on disk in small temp directories; no SQL creates a
//! partitioned table yet.

use std::path::Path;

use rclite::Arc;

use super::*;
use crate::core::mvcc::transaction::{FROZEN_TX, TxId};
use crate::core::partition::files::{self, PartFile};
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(root: &Path) -> Database {
    init();
    Database::new_named(&path_str(root), false, "parttests").await
}

async fn try_open(root: &Path) -> Result<Database, String> {
    init();
    Database::try_new_named(&path_str(root), false, "parttests").await
}

/// Closes `db` so the directory can be opened again in this process.
fn close(db: Database, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&path_str(root));
}

fn col(name: &str, data_type: DbType) -> CreateColumnData {
    CreateColumnData {
        name: name.into(),
        data_type,
    }
}

fn create_table(name: &str, columns: Vec<CreateColumnData>) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns,
        is_immutable: false,
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_columns: Vec::new(),
        type_modifiers: Vec::new(),
        unique_keys: Vec::new(),
        primary_key: None,
        if_not_exists: false,
        generated: Vec::new(),
        checks: Vec::new(),
    }
}

fn acct(name: &str) -> CreateTable {
    create_table(
        name,
        vec![col("tenant_id", DbType::I64), col("n", DbType::I32), col("label", DbType::STRING)],
    )
}

/// `sql` through the real CREATE parser.
fn parsed(sql: &str) -> CreateTable {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap_or_else(|| panic!("not a CREATE: {sql}"))
}

async fn create(db: &Database, name: &str, partitions: u32) -> u64 {
    db.create_partitioned_table(acct(name), &["tenant_id"], partitions)
        .await
        .unwrap_or_else(|e| panic!("creating {name}: {e}"));
    partitioned(db, name).uid()
}

fn partitioned(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        Some(Relation::Plain(_)) => panic!("{name} resolved as a plain table"),
        None => panic!("{name} does not resolve"),
    }
}

/// The live catalogue rows, read as startup reads them.
async fn catalogue(db: &Database) -> Vec<(String, u64)> {
    let schema = load_db_data_schema(db.db_io_schema.clone()).await;
    let mut iterator = RowPointerIterator::new(db.db_io_pointers.clone()).await.unwrap();
    let mut rows = load_db_tables(&schema, &mut iterator, db.db_io_rows.clone()).await;
    rows.sort();
    rows
}

fn files_in(root: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn uid_files(root: &Path, uid: u64) -> Vec<String> {
    files_in(root)
        .into_iter()
        .filter(|n| n.starts_with(&format!("{}{uid:016x}_", files::PREFIX)))
        .collect()
}

fn logical_files(root: &Path, name: &str) -> Vec<String> {
    files_in(root).into_iter().filter(|n| n.starts_with(name)).collect()
}

/// What a reopen must keep of a schema.
fn digest(s: &TableSchema) -> String {
    let fields: Vec<String> = s
        .fields
        .iter()
        .filter(|f| !f.is_deleted)
        .map(|f| format!("{}:{:?}:{}:{}:{:?}", f.name, f.db_type, f.write_order, f.is_unique, f.scale))
        .collect();
    let keys: Vec<(String, Vec<String>)> = s
        .unique_keys
        .iter()
        .filter(|k| !k.is_deleted)
        .map(|k| (k.name.clone(), k.columns.clone()))
        .collect();
    format!(
        "{} {fields:?} pk={:?} uk={keys:?} defaults={} checks={} typmods={}",
        s.name,
        s.primary_key,
        s.defaults.len(),
        s.checks.len(),
        s.type_modifiers.len()
    )
}

fn row(t: &PartitionedTable, tenant: i64, n: i32) -> crate::core::row::row::RowWrite {
    let qp = QueryPurpose::InsertRow(InsertRowData {
        table_name: t.name().to_string(),
        query: format!("INSERT INTO {} (tenant_id, n, label) VALUES ({tenant}, {n}, 'x')", t.name()),
        on_conflict_do_nothing: false,
        on_conflict_target: Vec::new(),
        on_conflict_update: None,
    });
    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0)
        .expect("the insert parser refused a plain row")
}

async fn insert(t: &PartitionedTable, tenant: i64, n: i32, tx: TxId) {
    t.insert_row_in_txn(row(t, tenant, n), tx).await.unwrap();
}

/// Rows with `n >= 0` across every materialized partition.
async fn count(t: &PartitionedTable) -> usize {
    let epoch = t.epoch().unwrap();
    let mut total = 0;
    for (_, p) in t.materialized(&epoch, &Route::All).unwrap() {
        let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&p.schema.fields);
        total += p
            .query_row("n >= 0", &p.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
            .await
            .unwrap()
            .len();
    }
    total
}

fn data_pids(root: &Path, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = files_in(root)
        .iter()
        .filter_map(|n| match files::parse_name(n)? {
            PartFile::Data { uid: u, pid } if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort();
    pids.dedup();
    pids
}

/// CREATE adds only `__pt_<uid>_*` files (the parent set and schema
/// sidecars, no partition yet) and one `clusters = uid` row; nothing starts
/// with the logical name, before or after a reopen, and the reopened table
/// resolves as partitioned with an equal schema (I8, §3.1, §12).
#[tokio::test(flavor = "multi_thread")]
async fn create_writes_only_uid_files_and_a_uid_row() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let before = files_in(root);
    let ct = parsed(
        "CREATE TABLE acct (tenant_id BIGINT NOT NULL, n INT NOT NULL, amt NUMERIC(10,2), \
         label TEXT DEFAULT 'x', CHECK (n > 0), PRIMARY KEY (tenant_id, n))",
    );
    db.create_partitioned_table(ct, &["tenant_id"], 8).await.unwrap();
    let t = partitioned(&db, "acct");
    let uid = t.uid();
    assert_eq!(t.broken(), None);
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()), "the shell is not in tables");

    let added: Vec<String> = files_in(root).into_iter().filter(|n| !before.contains(n)).collect();
    for name in &added {
        match files::parse_name(name) {
            Some(PartFile::Data { .. }) => panic!("CREATE materialized a partition: {name}"),
            Some(f) if f.uid() == uid => {}
            _ => panic!("CREATE added a file outside uid {uid:016x}: {name}"),
        }
    }
    for name in files::parent_files(uid) {
        assert!(added.contains(&name), "missing parent file {name}");
    }
    assert_eq!(logical_files(root, "acct"), Vec::<String>::new());
    assert_eq!(catalogue(&db).await, vec![("acct".to_string(), uid)]);
    let declared = digest(&t.shell().schema);
    assert!(declared.contains("amt:DECIMAL") && declared.contains("checks=1"), "{declared}");

    drop(t);
    close(db, root);
    let db = open(root).await;
    let t = partitioned(&db, "acct");
    assert_eq!(t.uid(), uid);
    assert_eq!(t.broken(), None);
    assert_eq!(digest(&t.shell().schema), declared);
    assert_eq!(digest(db.relation("acct").unwrap().schema()), declared);
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()));
    assert_eq!(logical_files(root, "acct"), Vec::<String>::new(), "the reopen created files under the logical name");
    drop(t);
    close(db, root);
}

/// Rows written through the table are there after a reopen, in the same
/// partitions: startup opens every materialized partition.
#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_keeps_rows_and_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 8).await;
    let t = partitioned(&db, "acct");
    for tenant in 0..24 {
        insert(&t, tenant, tenant as i32, FROZEN_TX).await;
    }
    db.sync_durable().unwrap();
    let pids = data_pids(root, uid);
    assert!(pids.len() > 1, "24 tenants landed in {pids:?}");
    assert_eq!(count(&t).await, 24);
    drop(t);
    close(db, root);

    let db = open(root).await;
    let t = partitioned(&db, "acct");
    assert_eq!(t.totals().await.unwrap().materialized as usize, pids.len());
    assert_eq!(count(&t).await, 24);
    drop(t);
    close(db, root);
}

/// I18, the automated half: a binary that ignores `clusters` opens a
/// partitioned row as `Table::new(name)`, finds no schema under the name
/// and marks the table unreadable, so it is left out (missing, not empty).
/// The open below is that rule, run over this build's files.
#[tokio::test(flavor = "multi_thread")]
async fn an_older_binary_finds_no_schema_under_the_logical_name() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "acct", 4).await;
    insert(&partitioned(&db, "acct"), 7, 1, FROZEN_TX).await;
    db.sync_durable().unwrap();
    close(db, root);

    let db = open(root).await;
    let rows = catalogue(&db).await;
    assert_eq!(rows.len(), 1);
    for (name, _clusters_ignored) in rows {
        let old = Table::new(&name, db.db_io_pointers.clone(), vec![]).await;
        assert!(
            old.schema_unreadable,
            "an older binary would open \"{name}\" as a plain table with {} columns",
            old.schema.fields.len()
        );
    }
    close(db, root);
}

/// Two live rows sharing a name refuse the start when either is a
/// partitioned table's, naming both; so do two rows sharing a uid. Two plain
/// rows under one name are only logged (user decision): they share files and
/// open as one table.
#[tokio::test(flavor = "multi_thread")]
async fn duplicate_catalogue_rows_refuse_only_with_a_partitioned_row() {
    // A plain table written over a partitioned one, as an older binary would.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 4).await;
    db.write_catalogue_row("acct", 0).await.unwrap();
    db.sync_durable().unwrap();
    close(db, root);
    let refusal = try_open(root).await.err().expect("a duplicated partitioned name opened");
    assert!(refusal.contains("\"acct\""), "{refusal}");
    assert!(refusal.contains("clusters = 0 ") && refusal.contains(&format!("{uid:#018x}")), "{refusal}");
    crate::core::wal::remove_for(&path_str(root));

    // Two names claiming one uid.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 4).await;
    db.write_catalogue_row("acct_copy", uid).await.unwrap();
    db.sync_durable().unwrap();
    close(db, root);
    let refusal = try_open(root).await.err().expect("two rows sharing a uid opened");
    assert!(refusal.contains("\"acct\"") && refusal.contains("\"acct_copy\""), "{refusal}");
    crate::core::wal::remove_for(&path_str(root));

    // A legacy plain duplicate: logged, and the table opens.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("plain")).await.unwrap();
    db.write_catalogue_row("plain", 0).await.unwrap();
    db.sync_durable().unwrap();
    close(db, root);
    let db = try_open(root).await.expect("a plain duplicate refused the start");
    assert!(matches!(db.relation("plain"), Some(Relation::Plain(t)) if !t.is_shell()));
    close(db, root);
}

/// 2,048 declared partitions per database: two tables of 1024 fit, a third
/// partition does not, and a DROP gives the room back. Another database is
/// counted on its own.
#[tokio::test(flavor = "multi_thread")]
async fn the_cap_counts_declared_partitions_per_database() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "t1", 1024).await;
    create(&db, "t2", 1024).await;
    let refused = db.create_partitioned_table(acct("t3"), &["tenant_id"], 1).await.unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::Unsupported, "{refused}");
    assert!(refused.to_string().contains("not supported"), "{refused}");
    assert!(db.relation("t3").is_none());
    assert_eq!(catalogue(&db).await.len(), 2, "the refused CREATE wrote a row");

    let other_dir = tempfile::tempdir().unwrap();
    let other = open(other_dir.path()).await;
    create(&other, "t1", 1024).await;
    close(other, other_dir.path());

    for bad in [0, MAX_DECLARED_PER_DATABASE_FOR_TESTS + 1] {
        let e = db.create_partitioned_table(acct("t4"), &["tenant_id"], bad).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "PARTITIONS {bad}: {e}");
    }

    assert!(db.drop_table("t1").await.unwrap());
    create(&db, "t3", 1).await;
    close(db, root);
}

const MAX_DECLARED_PER_DATABASE_FOR_TESTS: u32 = crate::core::partition::table::MAX_DECLARED_PER_DATABASE;

/// An in-memory database refuses PARTITION BY, and writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_in_memory_database_refuses_partitioning() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new_named(&path_str(dir.path()), true, "parttests_mem").await;
    let e = db.create_partitioned_table(acct("acct"), &["tenant_id"], 4).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{e}");
    assert!(e.to_string().contains("in-memory"), "{e}");
    assert!(db.relation("acct").is_none());
    assert!(catalogue(&db).await.is_empty());
}

/// A pool with a backup mirror refuses PARTITION BY at CREATE, and a table
/// created before the mirror was added opens broken rather than unmirrored,
/// without touching the backup; the plain tables beside it open.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn a_mirrored_pool_refuses_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let backup = dir.path().join("backup");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&backup).unwrap();
    let db = open(&root).await;
    let uid = create(&db, "early", 4).await;
    db.add_table(acct("side")).await.unwrap();
    db.storage
        .add_location(&path_str(&backup), crate::core::storage::LocationRole::Backup, 1, true)
        .unwrap();
    let e = db.create_partitioned_table(acct("acct"), &["tenant_id"], 4).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{e}");
    assert!(e.to_string().contains("backup mirrors"), "{e}");
    assert!(db.relation("acct").is_none());
    let rows: Vec<String> = catalogue(&db).await.into_iter().map(|(n, _)| n).collect();
    assert!(!rows.contains(&"acct".to_string()), "the refused CREATE wrote a row");
    close(db, &root);

    let db = open(&root).await;
    let early = partitioned(&db, "early");
    assert!(early.broken().is_some_and(|w| w.contains("backup mirrors")), "{:?}", early.broken());
    assert!(db.table_for_tests("early").is_none(), "a refused table's stand-in shell reached tables");
    assert!(matches!(db.relation("side"), Some(Relation::Plain(_))));
    assert_eq!(uid_files(&backup, uid), Vec::<String>::new(), "opening wrote into the backup");
    drop(early);
    close(db, &root);
}

/// DROP removes every `__pt_<uid>_*` file, partitions included, and the
/// row; the name resolves to nothing, now and after a reopen, and is free
/// for a new table under a new uid. A second DROP finds nothing. A statement
/// still holding the table refuses to write: it neither reports success on a
/// dropped table nor materializes a partition after the files went.
#[tokio::test(flavor = "multi_thread")]
async fn drop_removes_every_uid_file_and_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 8).await;
    let t = partitioned(&db, "acct");
    for tenant in 0..3 {
        insert(&t, tenant, 1, FROZEN_TX).await;
    }
    db.sync_durable().unwrap();
    let materialized = data_pids(root, uid);
    assert!(!materialized.is_empty() && materialized.len() < 8, "{materialized:?}");
    let held = t;

    assert!(db.drop_table("acct").await.unwrap(), "the drop found no table");
    assert_eq!(uid_files(root, uid), Vec::<String>::new(), "files outlived the drop");
    assert!(db.relation("acct").is_none() && db.table_for_tests("acct").is_none());
    assert!(catalogue(&db).await.is_empty(), "the row outlived the drop");
    assert!(!db.drop_table("acct").await.unwrap(), "a second drop found a table");
    // Tenants over every partition, so some land where none was materialized.
    for tenant in 0..24 {
        let e = held.insert_row_in_txn(row(&held, tenant, 2), FROZEN_TX).await;
        assert!(e.is_err(), "tenant {tenant}: an insert through a held handle succeeded after the drop");
    }
    assert_eq!(uid_files(root, uid), Vec::<String>::new(), "a held handle wrote files after the drop");
    drop(held);
    close(db, root);

    let db = open(root).await;
    assert!(db.relation("acct").is_none(), "the dropped table came back");
    let again = create(&db, "acct", 4).await;
    assert_ne!(again, uid);
    assert_eq!(logical_files(root, "acct"), Vec::<String>::new());
    close(db, root);
}

/// A partitioned table whose map log is gone opens broken, with its shell
/// in `tables`; one whose schema is gone opens broken with no shell. Neither
/// missing file is recreated (I9), neither stops the plain table beside it,
/// the name stays taken, and DROP removes both.
#[tokio::test(flavor = "multi_thread")]
async fn broken_tables_open_loudly_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let no_map = create(&db, "no_map", 4).await;
    let no_schema = create(&db, "no_schema", 4).await;
    db.add_table(acct("side")).await.unwrap();
    close(db, root);
    let map_file = files::map_file(no_map, 0);
    let schema_file = format!("{}.db", files::schema_stem(no_schema));
    std::fs::remove_file(root.join(&map_file)).unwrap();
    std::fs::remove_file(root.join(&schema_file)).unwrap();

    let db = open(root).await;
    assert!(matches!(db.relation("side"), Some(Relation::Plain(t)) if !t.is_shell()));
    let t = partitioned(&db, "no_map");
    assert!(t.broken().is_some(), "a table with no map log opened");
    assert!(db.table_for_tests("no_map").is_some_and(|s| s.is_shell()));
    let t2 = partitioned(&db, "no_schema");
    assert!(t2.broken().is_some_and(|w| w.contains("schema")), "{:?}", t2.broken());
    assert!(db.table_for_tests("no_schema").is_none(), "a shell with no schema reached tables");
    assert!(!root.join(&map_file).exists(), "opening recreated the map log");
    assert!(!root.join(&schema_file).exists(), "opening recreated the schema file");
    drop((t, t2));

    for name in ["no_map", "no_schema"] {
        let e = db.add_table(acct(name)).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{name}: {e}");
        let e = db.rename_table("side", name).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "rename onto {name}: {e}");
    }
    for (name, uid) in [("no_map", no_map), ("no_schema", no_schema)] {
        assert!(db.drop_table(name).await.unwrap(), "{name}: the drop found no table");
        assert_eq!(uid_files(root, uid), Vec::<String>::new(), "{name}'s files outlived the drop");
    }
    let rows: Vec<String> = catalogue(&db).await.into_iter().map(|(n, _)| n).collect();
    assert_eq!(rows, ["side"]);
    close(db, root);
}

/// A CREATE that fails before its catalogue row leaves no file, no row and
/// no map entry, and the name stays free.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_failing_before_its_row_leaves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let before = files_in(root);
    // The key is checked against the schema as written, after the files.
    let e = db.create_partitioned_table(acct("acct"), &["nope"], 4).await.unwrap_err();
    assert!(e.to_string().contains("nope"), "{e}");
    let left: Vec<String> = files_in(root)
        .into_iter()
        .filter(|n| !before.contains(n) && n.starts_with(files::PREFIX))
        .collect();
    assert_eq!(left, Vec::<String>::new(), "a failed CREATE left files");
    assert!(catalogue(&db).await.is_empty());
    assert!(db.relation("acct").is_none());
    create(&db, "acct", 4).await;
    close(db, root);
}

/// The refusals around a partitioned name: FKs both ways, a nullable or
/// generated key, shredded JSON, a key of 0 or 4 columns, an existing name,
/// a held name, and the rewrite paths that would drop it and create a plain
/// table in its place.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_names_are_guarded() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "acct", 4).await;
    let rows_before = catalogue(&db).await;

    let fk = |parent: &str| crate::core::rql::foreign_key::ForeignKey {
        name: format!("fk_{parent}"),
        column: "n".into(),
        parent_table: parent.into(),
        parent_column: "n".into(),
        on_delete: crate::core::rql::foreign_key::ReferentialAction::NoAction,
        on_update: crate::core::rql::foreign_key::ReferentialAction::NoAction,
        is_deleted: false,
    };
    // Phase 3 P4: a foreign key onto a partitioned parent must reference a
    // unique key of it, so its whole partition key (§1b); `n` is neither.
    let mut child = acct("child");
    child.foreign_keys.push(fk("acct"));
    let e = db.add_table(child).await.unwrap_err();
    assert!(e.to_string().contains("there is no unique constraint matching given keys"), "{e}");
    // A partitioned child of a plain parent is accepted (a plain parent's
    // column need not be unique, as on the plain path).
    db.add_table(acct("parent")).await.unwrap();
    let mut pchild = acct("pchild");
    pchild.foreign_keys.push(fk("parent"));
    db.create_partitioned_table(pchild, &["tenant_id"], 4).await.unwrap();
    // Dropped again, so the catalogue below holds the rows it held before.
    assert!(db.drop_table("pchild").await.unwrap());
    // A partitioned table under a name a plain FK already points at, by a
    // column that is not its partition key: its checks could not route.
    let mut orphan = acct("orphan_child");
    orphan.foreign_keys.push(fk("ghost"));
    db.add_table(orphan).await.unwrap();
    let e = db.create_partitioned_table(acct("ghost"), &["tenant_id"], 4).await.unwrap_err();
    assert!(e.to_string().contains("there is no unique constraint matching given keys"), "{e}");

    let mut nullable = acct("nullable");
    nullable
        .columns
        .push(col(&crate::core::rql::nullable::flag_column_name("tenant_id"), DbType::U8));
    let e = db.create_partitioned_table(nullable, &["tenant_id"], 4).await.unwrap_err();
    assert!(e.to_string().contains("nullable"), "{e}");

    let mut generated = acct("generated");
    generated.generated.push(crate::core::rql::generated::GeneratedColumn {
        column: "tenant_id".into(),
        expression: "n + 1".into(),
        is_deleted: false,
    });
    let e = db.create_partitioned_table(generated, &["tenant_id"], 4).await.unwrap_err();
    assert!(e.to_string().contains("is generated"), "{e}");

    let mut json = acct("json");
    json.json_columns.push(crate::core::rql::json_map::JsonColumn {
        column: "label".into(),
        shadow_table: "json_label_shadow".into(),
        is_deleted: false,
    });
    let e = db.create_partitioned_table(json, &["tenant_id"], 4).await.unwrap_err();
    assert!(e.to_string().contains("JSON columns"), "{e}");

    for key in [&["tenant_id", "n", "label", "tenant_id"][..], &[][..]] {
        let e = db.create_partitioned_table(acct("arity"), key, 4).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{key:?}: {e}");
        assert!(e.to_string().contains("it takes 1 to 3"), "{key:?}: {e}");
    }

    let e = db.create_partitioned_table(acct("acct"), &["tenant_id"], 4).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    let e = db.create_partitioned_table(acct("parent"), &["tenant_id"], 4).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    let e = db.add_table(acct("acct")).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");

    {
        let _held = db.claim_name("busy").unwrap();
        assert!(db.add_table(acct("busy")).await.is_err(), "a held name was created");
        assert!(db.create_partitioned_table(acct("busy"), &["tenant_id"], 4).await.is_err());
    }
    db.add_table(acct("busy")).await.unwrap();

    let e = db.check_recreatable("acct").unwrap_err();
    assert!(e.to_string().contains("partitioned"), "{e}");
    // A reload of a partitioned table reopens it whole, under its name and
    // uid (phase 3 P1), never as a plain table over `<name>*` files.
    let uid = partitioned(&db, "acct").uid();
    db.reload_table("acct").await.unwrap();
    assert_eq!(partitioned(&db, "acct").uid(), uid);
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()), "a reload removed the shell");
    assert_eq!(logical_files(root, "acct_"), Vec::<String>::new(), "a reload made files under the logical name");

    let partitioned_rows: Vec<_> = catalogue(&db).await.into_iter().filter(|(_, c)| *c != 0).collect();
    assert_eq!(partitioned_rows, rows_before, "a refused CREATE wrote a partitioned row");
    close(db, root);
}

/// The forget horizon advances with a partitioned table open: the shell
/// takes no place in the registry, and its partitions sweep for themselves.
#[tokio::test(flavor = "multi_thread")]
async fn forgetting_advances_with_a_partitioned_table_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    let manager = db.transactions.clone();
    for id in 0..300i64 {
        let tx = manager.begin();
        insert(&t, id % 16, id as i32, tx).await;
        if id % 3 == 0 {
            manager.abort(tx);
        } else {
            manager.commit(tx);
        }
    }
    for _ in 0..9_000 {
        let tx = manager.begin();
        manager.commit(tx);
    }
    manager.settle_now();

    let mut swept = 0;
    for name in db.table_names() {
        let table = db.table_for_tests(&name).unwrap();
        if !table.is_shell() {
            table.neutralize_aborted_now().await.unwrap();
        }
    }
    let epoch = t.epoch().unwrap();
    for (_, p) in t.materialized(&epoch, &Route::All).unwrap() {
        if p.neutralize_aborted_now().await.unwrap().advanced_to.is_some() {
            swept += 1;
        }
    }
    assert!(swept > 0, "no partition's sweep advanced");
    assert!(
        manager.status().forgotten_below() > 0,
        "the forget horizon is pinned with a partitioned table open"
    );
    drop((t, epoch));
    close(db, root);
}

/// §1b: every UNIQUE and PRIMARY KEY of a partitioned table contains every
/// key column, whichever field of the CREATE carries it (inline UNIQUE,
/// table-level constraint, the PRIMARY KEY header). A refusal writes no row
/// and no file; a constraint containing the key is accepted.
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_key_without_every_key_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let before = files_in(root);

    let one = &["tenant_id"][..];
    let two = &["tenant_id", "n"][..];
    let mut header_only = acct("u_header");
    header_only.primary_key = Some("n".into());
    let refused: Vec<(&str, CreateTable, &[&str])> = vec![
        (
            "PRIMARY KEY (n)",
            parsed("CREATE TABLE u_pk (tenant_id BIGINT NOT NULL, n INT NOT NULL, PRIMARY KEY (n))"),
            one,
        ),
        (
            "inline UNIQUE",
            parsed("CREATE TABLE u_inline (tenant_id BIGINT NOT NULL, n INT NOT NULL UNIQUE)"),
            one,
        ),
        // Only the leading key column: still a duplicate across partitions.
        (
            "UNIQUE (tenant_id) under a two-column key",
            parsed("CREATE TABLE u_lead (tenant_id BIGINT NOT NULL, n INT NOT NULL, UNIQUE (tenant_id))"),
            two,
        ),
        ("the PRIMARY KEY header alone", header_only, one),
    ];
    for (what, ct, key) in refused {
        let name = ct.table_name.clone();
        let e = db.create_partitioned_table(ct, key, 4).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Unsupported, "{what}: {e}");
        assert!(e.to_string().contains("partition key column"), "{what}: {e}");
        assert!(db.relation(&name).is_none(), "{what}: the refused table resolves");
    }
    // Partition files only: reading the catalogue opens its own sidecars.
    let added: Vec<String> = files_in(root)
        .into_iter()
        .filter(|n| !before.contains(n) && n.starts_with(files::PREFIX))
        .collect();
    assert_eq!(added, Vec::<String>::new(), "a refused CREATE wrote files");
    assert!(catalogue(&db).await.is_empty(), "a refused CREATE wrote a row");

    db.create_partitioned_table(
        parsed("CREATE TABLE u_ok (tenant_id BIGINT NOT NULL, n INT NOT NULL, PRIMARY KEY (tenant_id, n))"),
        two,
        4,
    )
    .await
    .unwrap();
    db.create_partitioned_table(
        parsed("CREATE TABLE u_ok1 (tenant_id BIGINT NOT NULL UNIQUE, n INT NOT NULL)"),
        one,
        4,
    )
    .await
    .unwrap();
    close(db, root);
}

/// A RENAME claims both its names for its whole run: a held name on either
/// side refuses it before anything is copied or written.
#[tokio::test(flavor = "multi_thread")]
async fn a_rename_claims_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("src")).await.unwrap();
    for held in ["dst", "src"] {
        let _claim = db.claim_name(held).unwrap();
        let e = db.rename_table("src", "dst").await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "holding {held}: {e}");
        assert!(e.to_string().contains("by another statement"), "holding {held}: {e}");
        assert!(matches!(db.relation("src"), Some(Relation::Plain(_))), "holding {held}");
        assert!(db.relation("dst").is_none(), "holding {held}");
    }
    let rows: Vec<String> = catalogue(&db).await.into_iter().map(|(n, _)| n).collect();
    assert_eq!(rows, ["src"]);
    db.rename_table("src", "dst").await.unwrap();
    close(db, root);
}

/// A RENAME onto a name a partitioned CREATE is between its files and its
/// row is refused: that CREATE's name check is long past and its files are
/// not `<name>*`, so only the claim stands between them. Without it both
/// commit, two live rows name the table, and the next start refuses.
#[tokio::test(flavor = "multi_thread")]
async fn a_rename_cannot_land_inside_a_partitioned_create() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("race_src")).await.unwrap();
    let parked = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    super::test_hooks::PARK_PARTITIONED_CREATE
        .lock()
        .unwrap()
        .push(("race_acct".into(), parked.clone(), release.clone()));

    let (created, renamed) = tokio::join!(
        db.create_partitioned_table(acct("race_acct"), &["tenant_id"], 4),
        async {
            parked.notified().await;
            let renamed = db.rename_table("race_src", "race_acct").await;
            release.notify_one();
            renamed
        }
    );
    created.unwrap();
    let e = renamed.expect_err("a rename committed inside a partitioned CREATE of its name");
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    let uid = partitioned(&db, "race_acct").uid();
    assert!(db.table_for_tests("race_acct").is_some_and(|s| s.is_shell()));
    assert!(matches!(db.relation("race_src"), Some(Relation::Plain(_))));
    assert_eq!(
        catalogue(&db).await,
        vec![("race_acct".to_string(), uid), ("race_src".to_string(), 0)]
    );
    close(db, root);

    let db = try_open(root).await.expect("the database no longer opens");
    assert_eq!(partitioned(&db, "race_acct").uid(), uid);
    assert!(matches!(db.relation("race_src"), Some(Relation::Plain(_))));
    close(db, root);
}

/// A name with a storage placement recorded is taken for a partitioned
/// CREATE even with no table in the maps: a reload, MOVE or plain DROP of a
/// key/value or memory table has it out of `tables`, with no `<name>*` file
/// on disk to refuse the name.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_placement_takes_the_name() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.storage
        .pin_backend("placed", &crate::core::storage::TableBackend::Memory)
        .unwrap();
    assert!(db.table_for_tests("placed").is_none());
    let e = db.create_partitioned_table(acct("placed"), &["tenant_id"], 4).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    assert!(e.to_string().contains("placement"), "{e}");
    assert!(db.relation("placed").is_none());
    assert!(catalogue(&db).await.is_empty(), "the refused CREATE wrote a row");
    close(db, root);
}
