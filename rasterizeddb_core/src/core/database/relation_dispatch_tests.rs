//! Name-keyed `Database` paths resolve a name through `relation()` before
//! they touch `tables`, files or `seed_in` (S5, `docs/partitioning.md` §3.3,
//! I8): structurally, by reading the `impl Database` blocks, and
//! behaviourally, each path on a partitioned table either answers for the
//! whole table or refuses, never from the shell's emptiness, and the shell
//! stays in `tables` after a refusal.
//!
//! The helpers here are shared with `reaper_tests`.

use std::path::Path;

use rclite::Arc;

use super::*;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
use crate::core::row::seam_tests::{matching, strip};

pub(super) fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

pub(super) fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

pub(super) async fn open(root: &Path) -> Database {
    init();
    Database::new_named(&path_str(root), false, "dispatchtests").await
}

/// Closes `db` so the directory can be opened again in this process.
pub(super) fn close(db: Database, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&path_str(root));
}

pub(super) fn acct(name: &str) -> CreateTable {
    let col = |name: &str, data_type: DbType| CreateColumnData {
        name: name.into(),
        data_type,
    };
    CreateTable {
        table_name: name.to_string(),
        columns: vec![col("tenant_id", DbType::I64), col("n", DbType::I32), col("label", DbType::STRING)],
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

pub(super) async fn create(db: &Database, name: &str, partitions: u32) -> u64 {
    db.create_partitioned_table(acct(name), &["tenant_id"], partitions)
        .await
        .unwrap_or_else(|e| panic!("creating {name}: {e}"));
    partitioned(db, name).uid()
}

pub(super) fn partitioned(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        Some(Relation::Plain(_)) => panic!("{name} resolved as a plain table"),
        None => panic!("{name} does not resolve"),
    }
}

fn row_for(table: &str, fields: &[SchemaField], tenant: i64, n: i32) -> crate::core::row::row::RowWrite {
    let qp = QueryPurpose::InsertRow(InsertRowData {
        table_name: table.to_string(),
        query: format!("INSERT INTO {table} (tenant_id, n, label) VALUES ({tenant}, {n}, 'x')"),
        on_conflict_do_nothing: false,
        on_conflict_target: Vec::new(),
        on_conflict_update: None,
    });
    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, fields, &[], &[], 0)
        .expect("the insert parser refused a plain row")
}

/// One row into a partitioned table, frozen.
pub(super) async fn insert(t: &PartitionedTable, tenant: i64, n: i32) {
    let row = row_for(t.name(), &t.shell().schema.fields, tenant, n);
    t.insert_row_in_txn(row, FROZEN_TX).await.unwrap();
}

/// One row into a plain table created from [`acct`].
pub(super) async fn insert_plain(db: &Database, name: &str, tenant: i64, n: i32) {
    let table = db.table_for_tests(name).unwrap().clone();
    let row = row_for(name, &table.schema.fields, tenant, n);
    table.insert_row(row).await.unwrap();
}

async fn rows_of(table: &Table<DbStorageProvider>) -> usize {
    let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&table.schema.fields);
    table
        .query_row("n >= 0", &table.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
        .await
        .unwrap()
        .len()
}

/// Rows across every materialized partition.
pub(super) async fn count(t: &PartitionedTable) -> usize {
    let epoch = t.epoch().unwrap();
    let mut total = 0;
    for (_, p) in t.materialized(&epoch, &Route::All).unwrap() {
        total += rows_of(&p).await;
    }
    total
}

pub(super) async fn count_plain(db: &Database, name: &str) -> usize {
    rows_of(&db.table_for_tests(name).unwrap().clone()).await
}

pub(super) fn files_in(root: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

pub(super) fn logical_files(root: &Path, name: &str) -> Vec<String> {
    files_in(root).into_iter().filter(|n| n.starts_with(name)).collect()
}

// ---------------------------------------------------------------------------
// The structural scan.
// ---------------------------------------------------------------------------

/// A name-keyed fn of `impl Database`.
#[derive(Debug)]
struct NameKeyed {
    name: String,
    /// Byte offset in the body of the first `relation`/`refuse_partitioned`
    /// call, if any.
    dispatch: Option<usize>,
    /// Byte offset of the first touch of the map, a file or a seed.
    touch: Option<usize>,
}

const NAME_PARAMS: [&str; 4] = ["table_name: &str", "name: &str", "old_name: &str", "new_name: &str"];
const DISPATCH: [&str; 2] = ["self.relation(", "self.refuse_partitioned("];
/// The map is touched through the slot primitives (`slots.rs`), or by name
/// in `slots.rs` itself.
const TOUCH: [&str; 11] = [
    "self.table_slots",
    "self.partitioned_tables",
    "self.has_table(",
    "self.table_unlocked(",
    "self.take_table(",
    "self.install_table(",
    "seed_in(",
    "Table::new",
    "sweep_files(",
    "discard_files(",
    "pool_place(",
];

/// Name-keyed fns that need no dispatch of their own, with why.
const ALLOWLIST: &[(&str, &str)] = &[
    ("new_named", "`name` is the database's"),
    ("try_new_named", "`name` is the database's"),
    ("forget_seed", "drops cached seeds by name; a partitioned table has none"),
    ("drop_table", "delegates to drop_table_to_depth, which dispatches"),
    ("forget_in_catalogue", "the row tombstone both drop paths share, after their dispatch"),
    ("forget_dropped_files", "the WAL barrier for the plain table drop_table_to_depth already resolved and removed"),
    ("abandon_copy", "delegates to discard_table_files, which dispatches"),
    ("table_files_conflict", "delegates to table_files_conflict_beside"),
    ("table_files_conflict_beside", "the name guard: a pass over every relation, plain and partitioned, by design"),
    ("remember_in_catalogue", "a row write; rename dispatches before it"),
    ("write_catalogue_row", "a row write with the cluster value its caller chose"),
    ("catalogue_contains", "test-only catalogue read"),
    ("claim_name", "an in-memory claim on the name, whatever it names"),
    ("drop_partitioned", "the partitioned branch of drop_table_to_depth's dispatch"),
    ("open_partitioned", "startup: builds the partitioned table relation() resolves"),
    ("refuse_held_name", "an in-memory check of the names a held rename keeps; touches no table"),
    ("hold_intent", "records a rename's two names in memory; rename dispatches before it"),
    ("relation", "the dispatch itself"),
    ("refuse_partitioned", "the dispatch itself"),
    ("resolve", "the name-to-key rule every slot primitive's lookup goes through"),
    ("schema_view", "a slot primitive: asks the partitioned map itself, as relation() does"),
    ("slot", "a slot primitive: the raw slot, for B3b's table_for, which dispatches"),
    ("partitioned_table", "a slot primitive: the partitioned map's lookup"),
    ("is_partitioned", "a slot primitive: the partitioned map's lookup"),
    ("has_table", "a slot primitive: what tables.contains_key answered; its callers dispatch"),
    ("table_unlocked", "a slot primitive: the escape hatch; its callers dispatch"),
    ("table_for_tests", "a slot primitive: never called by the engine"),
    ("take_table", "a slot primitive: its callers dispatch"),
    ("take_partitioned", "a slot primitive: drop_partitioned's removal, after its dispatch"),
];

/// Every fn of the `impl Database {` blocks in `src` that takes one of
/// [`NAME_PARAMS`].
fn name_keyed(src: &str) -> Vec<NameKeyed> {
    let code = strip(src);
    let bytes = code.as_bytes();
    let header = "impl Database {";
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = code[from..].find(header) {
        let open = from + at + header.len() - 1;
        let close = matching(bytes, open);
        let mut k = open + 1;
        while k < close {
            match bytes[k] {
                b'{' => {
                    k = matching(bytes, k) + 1;
                    continue;
                }
                b'f' if code[k..].starts_with("fn ")
                    && !(bytes[k - 1].is_ascii_alphanumeric() || bytes[k - 1] == b'_') =>
                {
                    let name_end = k + 3 + code[k + 3..].find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap();
                    let name = code[k + 3..name_end].to_string();
                    let params_open = name_end + code[name_end..].find('(').unwrap();
                    let params_close = matching(bytes, params_open);
                    // Whitespace-normalised, so a parameter list over several
                    // lines reads like one.
                    let params = code[params_open + 1..params_close].split_whitespace().collect::<Vec<_>>().join(" ");
                    let body_open = params_close + code[params_close..].find('{').unwrap();
                    let body_close = matching(bytes, body_open);
                    let body = &code[body_open + 1..body_close];
                    let keyed = params.split(',').map(str::trim).any(|p| NAME_PARAMS.contains(&p));
                    if keyed {
                        let first = |needles: &[&str]| needles.iter().filter_map(|n| body.find(n)).min();
                        out.push(NameKeyed {
                            name,
                            dispatch: first(&DISPATCH),
                            touch: first(&TOUCH),
                        });
                    }
                    k = body_close + 1;
                    continue;
                }
                _ => {}
            }
            k += 1;
        }
        from = close;
    }
    out
}

/// Those of `fns` neither dispatching before their first touch nor
/// allowlisted.
fn undispatched(fns: &[NameKeyed]) -> Vec<String> {
    fns.iter()
        .filter(|f| !ALLOWLIST.iter().any(|(n, _)| *n == f.name))
        .filter(|f| match (f.dispatch, f.touch) {
            (None, _) => true,
            (Some(d), Some(t)) => t < d,
            (Some(_), None) => false,
        })
        .map(|f| f.name.clone())
        .collect()
}

/// I8 for `impl Database`: every fn taking a table name calls
/// `self.relation(` or `self.refuse_partitioned(` before its first touch of
/// `tables`, a seed or a file, or is allowlisted with a reason. Such a fn
/// that went by name would find the shell's empty `<name>*` file set, or
/// replace the shell with a plain table.
#[test]
fn every_name_keyed_database_fn_resolves_the_relation_first() {
    let mut fns = name_keyed(include_str!("mod.rs"));
    fns.extend(name_keyed(include_str!("partitioned.rs")));
    fns.extend(name_keyed(include_str!("relation.rs")));
    fns.extend(name_keyed(include_str!("slots.rs")));

    // A reader that found nothing would pass vacuously; and a multi-line
    // parameter list must still be read.
    for known in [
        "table_bytes",
        "move_table_to_backend",
        "rename_table",
        "reload_table",
        "drop_table_to_depth",
        "check_recreatable",
        "copy_table_files",
        "rewrite_schema_name",
        "open_renamed_table",
        "record_rename_intent",
        "discard_table_files",
        "drop_partitioned",
    ] {
        assert!(fns.iter().any(|f| f.name == known), "the reader missed `{known}`: {fns:?}");
    }
    let stale: Vec<&str> =
        ALLOWLIST.iter().map(|(n, _)| *n).filter(|n| !fns.iter().any(|f| f.name == *n)).collect();
    assert!(stale.is_empty(), "allowlisted fns that no longer exist or take no name: {stale:?}");

    let missing = undispatched(&fns);
    assert!(
        missing.is_empty(),
        "these name-keyed `impl Database` fns touch `tables`, a seed or a file before resolving the \
         relation, and are not allowlisted: {missing:?}"
    );
}

/// The reader itself: a fn that touches the map first is found, whatever
/// its parameter layout, and a dispatching one is not.
#[test]
fn the_reader_finds_a_name_keyed_fn_that_skips_the_dispatch() {
    let src = r#"
impl Database {
    /// fn hidden(&self, name: &str) { self.table_slots }
    pub async fn sizes(
        &self,
        table_name: &str,
    ) -> u64 {
        let s = "{ self.relation( }";
        let seed = seed_in(&self.db_io_pointers, table_name).await;
        self.relation(table_name);
        0
    }
    fn checked(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        self.refuse_partitioned(old_name, "x")?;
        self.take_table(old_name);
        Ok(())
    }
    fn unrelated(&self, depth: usize) {
        self.table_slots.clear();
    }
}
"#;
    let fns = name_keyed(src);
    let names: Vec<&str> = fns.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["sizes", "checked"]);
    assert_eq!(undispatched(&fns), ["sizes"]);
}

// ---------------------------------------------------------------------------
// Behaviour.
// ---------------------------------------------------------------------------

fn refused(what: &str, e: &str) {
    assert!(e.contains("partitioned") && e.contains("not supported"), "{what} erred without the refusal: {e}");
}

/// `table_bytes` of a partitioned table is its partitions' total, not the
/// sum of `<name>*` files (there are none), and leaves no file under the
/// logical name.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn table_bytes_of_a_partitioned_table_is_its_partitions_total() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "acct", 8).await;
    let t = partitioned(&db, "acct");
    for tenant in 0..12 {
        insert(&t, tenant, 1).await;
    }
    db.sync_durable().unwrap();
    let totals = t.totals().await.unwrap();
    assert!(totals.bytes > 0 && totals.materialized > 1, "{totals:?}");
    assert_eq!(db.table_bytes("acct").await, totals.bytes);
    assert_eq!(logical_files(root, "acct"), Vec::<String>::new(), "table_bytes created files under the name");
    drop(t);
    close(db, root);
}

/// `vacuum_all` reports a partitioned table once, under its logical name,
/// summed over its partitions; and fails, naming the table, on a shell with
/// nothing behind it, rather than logging it and returning the rest.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_all_fans_out_and_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    create(&db, "acct", 8).await;
    db.add_table(acct("side")).await.unwrap();
    let t = partitioned(&db, "acct");
    for tenant in 0..12 {
        insert(&t, tenant, 1).await;
    }
    insert_plain(&db, "side", 1, 1).await;

    let reports = db.vacuum_all().await.unwrap();
    let names: Vec<&str> = reports.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["acct", "side"]);
    let (_, report) = &reports[0];
    assert_eq!(report.slots, t.totals().await.unwrap().slots, "{report:?}");
    assert_eq!(report.live_rows, 12, "{report:?}");
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()));

    // A shell with no partitioned table behind it.
    let mut schema = t.shell().schema.clone();
    schema.name = "orphan".into();
    db.install_table(
        "orphan".into(),
        Arc::new(Table::partition_shell(t.shell().io_schema.clone(), schema)),
    );
    let e = db.vacuum_all().await.unwrap_err().to_string();
    assert!(e.contains("orphan"), "{e}");
    refused("vacuum_all over an orphan shell", &e);
    db.take_table("orphan");
    drop(t);
    close(db, root);
}

/// A partitioned table whose schema could not be read has no shell in
/// `tables`; the name-keyed paths still refuse it as partitioned rather than
/// reporting it missing, and `vacuum_all` fails on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_shell_less_partitioned_table_is_refused_not_missing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 4).await;
    db.add_table(acct("side")).await.unwrap();
    close(db, root);
    std::fs::remove_file(root.join(format!("{}.db", crate::core::partition::files::schema_stem(uid)))).unwrap();

    let db = open(root).await;
    assert!(db.table_for_tests("acct").is_none() && db.is_partitioned("acct"));
    refused("rename of acct", &db.rename_table("acct", "acct2").await.unwrap_err().to_string());
    refused("reload of acct", &db.reload_table("acct").await.unwrap_err().to_string());
    refused("check_recreatable(acct)", &db.check_recreatable("acct").unwrap_err().to_string());
    #[cfg(feature = "storage_pools")]
    refused("move of acct", &db.move_table_to("acct", "memory:").await.unwrap_err().to_string());
    let e = db.vacuum_all().await.unwrap_err().to_string();
    assert!(e.contains("acct"), "{e}");
    // A plain table whose files `acct` would own under an older binary.
    let e = db.add_table(acct("acct_schema")).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::AlreadyExists, "{e}");
    assert!(db.relation("acct2").is_none() && db.table_for_tests("side").is_some());
    close(db, root);
}

/// The STORAGE statements: MOVE and PLACE refuse a partitioned table (even
/// toward the root, where a placement-less table would read as already
/// there), REBALANCE never plans its shell, which has no files and would
/// sort first, and SHOW STORAGE PLACEMENTS lists it rather than erring.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn storage_statements_skip_or_refuse_a_partitioned_table() {
    let dir = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let root = dir.path();
    let second_path = path_str(second.path());
    let db = Arc::new(open(root).await);
    create(&db, "acct", 4).await;
    for name in ["side_a", "side_b"] {
        db.add_table(acct(name)).await.unwrap();
        insert_plain(&db, name, 1, 1).await;
    }
    let run = |sql: String| {
        let db = db.clone();
        async move { crate::core::storage::sql::execute(&sql, &db).await }
    };

    for target in ["memory:".to_string(), path_str(root), second_path.clone()] {
        let e = run(format!("STORAGE MOVE TABLE acct TO '{target}'")).await.unwrap_err();
        refused(&format!("MOVE TABLE to {target}"), &e);
    }
    refused("PLACE TABLE", &run(format!("STORAGE PLACE TABLE acct AT '{second_path}'")).await.unwrap_err());

    run(format!("STORAGE ADD LOCATION '{second_path}'")).await.unwrap();
    run("STORAGE SET POLICY = 'round-robin'".to_string()).await.unwrap();
    let plan = run("STORAGE REBALANCE DRY RUN".to_string()).await.unwrap();
    assert!(plan.iter().any(|l| l.contains("side_")), "the plan moves no plain table: {plan:?}");
    assert!(!plan.iter().any(|l| l.contains("acct")), "the plan moves the shell: {plan:?}");
    // Through the relocation entry too, not only the statement.
    let e = crate::core::storage::relocate::move_table(
        &db,
        "acct",
        &crate::core::storage::TableBackend::Directory(second_path.clone()),
    )
    .await
    .unwrap_err();
    refused("relocate::move_table", &e);

    crate::core::storage::sql::execute_structured("SHOW STORAGE PLACEMENTS", &db)
        .unwrap_or_else(|e| panic!("SHOW STORAGE PLACEMENTS erred over a partitioned table: {e}"));
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()), "a refusal removed the shell");
    assert!(db.storage.spec_of("acct").is_none(), "a refused statement placed the partitioned table");
    assert_eq!(logical_files(root, "acct"), Vec::<String>::new());
    assert_eq!(logical_files(second.path(), "acct"), Vec::<String>::new());
}

/// The VACUUM statement, bare, named and FULL, vacuums a partitioned
/// table's materialized partitions (S11), never its shell: before S11 it
/// refused here, and a bare VACUUM vacuuming the shell failed outright
/// (finding F2). The shell stays in `tables`, no file appears under the
/// logical name, and the row survives every pass.
#[tokio::test(flavor = "multi_thread")]
async fn the_vacuum_statement_vacuums_a_partitioned_tables_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = Arc::new(open(root).await);
    create(&db, "acct", 4).await;
    db.add_table(acct("side")).await.unwrap();
    insert(&partitioned(&db, "acct"), 1, 1).await;
    for statement in ["VACUUM", "VACUUM acct", "VACUUM FULL", "VACUUM FULL acct", "VACUUM side"] {
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(statement).unwrap();
        let reply = crate::core::rql::executor::execute(purpose, db.clone()).await;
        assert_eq!(reply.first(), Some(&0u8), "{statement}: {}", String::from_utf8_lossy(&reply[1..]));
        assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()), "{statement} replaced the shell");
        assert_eq!(logical_files(root, "acct"), Vec::<String>::new(), "{statement}");
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose("SELECT COUNT(*) FROM acct").unwrap();
        let reply = crate::core::rql::executor::execute(purpose, db.clone()).await;
        let rows = crate::core::row::row::vec_into_rows(&reply[1..]).unwrap();
        let n = i64::from_le_bytes(rows[0].columns[0].data.into_slice().try_into().unwrap());
        assert_eq!(n, 1, "{statement} lost the row");
    }
}

/// ALTER and CREATE INDEX on a partitioned table refuse with 0A000's
/// wording, write nothing to its schema file, and leave the shell in
/// `tables` (reload_table refuses before it removes the entry).
#[tokio::test(flavor = "multi_thread")]
async fn alter_on_a_partitioned_table_refuses_and_keeps_the_shell() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = Arc::new(open(root).await);
    let uid = create(&db, "acct", 4).await;
    let schema_file = root.join(format!("{}.db", crate::core::partition::files::schema_stem(uid)));
    let before = std::fs::read(&schema_file).unwrap();
    for statement in [
        "ALTER TABLE acct ADD COLUMN x INT",
        "ALTER TABLE acct RENAME TO acct2",
        "ALTER TABLE acct RENAME COLUMN n TO m",
        "CREATE INDEX ix_acct ON acct (n)",
    ] {
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(statement).unwrap();
        let reply = crate::core::rql::executor::execute(purpose, db.clone()).await;
        assert_eq!(reply.first(), Some(&3u8), "{statement} answered on a partitioned table");
        refused(statement, &String::from_utf8_lossy(&reply[1..]));
        assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()), "{statement} removed the shell");
    }
    assert_eq!(std::fs::read(&schema_file).unwrap(), before, "a refused DDL wrote the schema file");
    refused("reload_table", &db.reload_table("acct").await.unwrap_err().to_string());
    assert!(db.table_for_tests("acct").is_some_and(|s| s.is_shell()));
    assert!(db.relation("acct2").is_none());
}
