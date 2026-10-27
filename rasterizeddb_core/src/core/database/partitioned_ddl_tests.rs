//! Partitioned schema changes below SQL (docs/partitioning.md §11, "Phase 3
//! P1 as built"): one reload reopens every open partition under the one new
//! schema, moves `schema_generation` once and the shape epoch only for a
//! new shape; validation reads every partition; the uniqueness rule holds;
//! and a partition's index snapshot from before the change is never loaded
//! after it.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use rclite::Arc;

use super::Database;
use crate::core::index_store::load::Origin;
use crate::core::index_store::load::settings::IndexStorage;
use crate::core::index_store::load::storage_override;
use crate::core::partition::route::Route;
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "pddl").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let rewritten = crate::core::rql::lexer_alter::unique_index_as_constraint(sql);
    let sql = rewritten.as_deref().unwrap_or(sql);
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{e}"))
}

fn count(out: &[u8]) -> usize {
    assert_eq!(out.first(), Some(&2), "not a row set: {}", String::from_utf8_lossy(out));
    crate::core::row::row::vec_into_rows(&out[1..]).unwrap_or_default().len()
}

fn partitions(db: &Database, name: &str) -> Vec<(u32, Arc<DbTable>)> {
    let pt = db.partitioned_table(name).expect("a partitioned table");
    let epoch = pt.epoch().expect("it opens");
    pt.materialized(&epoch, &Route::All).expect("its partitions")
}

/// The schema a client can tell apart: names, types, write orders,
/// tombstones, the declared indexes and constraints.
fn schema_text(schema: &crate::core::row::schema::TableSchema) -> String {
    let fields: Vec<String> = schema
        .fields
        .iter()
        .map(|f| format!("{}:{:?}:{}:{}", f.name, f.db_type, f.write_order, f.is_deleted))
        .collect();
    format!(
        "{} {fields:?} idx {:?} uk {:?} chk {:?} def {:?}",
        schema.name,
        schema.secondary_indexes.iter().map(|i| (&i.name, i.is_deleted)).collect::<Vec<_>>(),
        schema.unique_keys.iter().map(|k| (&k.name, &k.columns, k.is_deleted)).collect::<Vec<_>>(),
        schema.checks.iter().map(|c| (&c.name, c.is_deleted)).collect::<Vec<_>>(),
        schema.defaults.iter().map(|d| (&d.column, &d.expression, d.is_deleted)).collect::<Vec<_>>(),
    )
}

/// Every open partition holds the shell's schema, and none of `old` is
/// still open: each was retired and replaced.
fn assert_one_schema(db: &Database, name: &str, old: &[(u32, Arc<DbTable>)], at: &str) -> Vec<(u32, Arc<DbTable>)> {
    let pt = db.partitioned_table(name).expect("a partitioned table");
    assert!(pt.broken().is_none(), "{at}: {:?}", pt.broken());
    let shell = schema_text(&pt.shell().schema);
    let now = partitions(db, name);
    assert_eq!(
        now.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        old.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        "{at}: the open partitions changed"
    );
    for ((pid, new), (_, before)) in now.iter().zip(old) {
        assert_eq!(schema_text(&new.schema), shell, "{at}: partition {pid} holds another schema than the shell");
        assert!(!Arc::ptr_eq(new, before), "{at}: partition {pid} was not reopened");
        assert!(before.is_retired(), "{at}: partition {pid}'s old instance was not retired");
    }
    now
}

fn generation(db: &Database) -> u64 {
    db.schema_generation.load(Ordering::Acquire)
}

async fn index_names(t: &DbTable) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(view) = t.indexes().expect("a partition's data") {
            let names: Vec<String> = view.list().iter().map(|i| i.decl.name.clone()).collect();
            return names;
        }
        assert!(Instant::now() < deadline, "the indexes never appeared");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn populated(dir: &Path) -> Arc<Database> {
    let db = open(dir).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, s TEXT, \
         PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 8",
    )
    .await;
    let values: Vec<String> = (0..60i64).map(|i| format!("({}, {i}, {}, 'w{}')", i % 5 + 1, i * 2, i % 3)).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v, s) VALUES {}", values.join(", "))).await;
    db
}

/// One schema change reopens every open partition under the new schema,
/// retiring each old instance; `schema_generation` moves by one reload's
/// two bumps, however many partitions there are; the shape epoch stays for
/// an index and moves for a renamed column; a partition materialized
/// afterwards takes the new schema too.
///
/// Mutations: the reopen keeps one partition's old instance (fails "was not
/// reopened" / "another schema"); the shell's sidecars written after the
/// partitions reopen (the new index and CHECK are missing in memory);
/// `swap_partitioned` swapping the shell without the shape check (the
/// shape epoch stays after RENAME COLUMN).
#[tokio::test(flavor = "multi_thread")]
async fn a_schema_change_reopens_every_open_partition_under_one_schema() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = populated(d).await;
    let before = partitions(&db, "pt");
    assert!(before.len() >= 3 && before.len() < 8, "{} partitions open", before.len());

    let (generation0, shape, slot) = (generation(&db), db.shape_epoch(), db.slot_shape_epoch("pt"));
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    let after = assert_one_schema(&db, "pt", &before, "CREATE INDEX");
    assert_eq!(generation(&db), generation0 + 2, "schema_generation moves once per DDL, never per partition");
    assert_eq!((db.shape_epoch(), db.slot_shape_epoch("pt")), (shape, slot), "an index keeps the shape");
    for (pid, part) in &after {
        assert!(index_names(part).await.iter().any(|n| n == "pt_v"), "partition {pid} has no pt_v");
    }

    exec(&db, "ALTER TABLE pt ADD CONSTRAINT pt_c CHECK (v >= 0)").await;
    let after = assert_one_schema(&db, "pt", &after, "ADD CHECK");
    for tenant in 1..=5 {
        let refused = try_exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({tenant}, 1000, -1)")).await;
        assert!(refused.is_err(), "tenant {tenant}: a row breaking the new CHECK was stored");
    }

    let (generation0, shape, slot) = (generation(&db), db.shape_epoch(), db.slot_shape_epoch("pt"));
    exec(&db, "ALTER TABLE pt RENAME COLUMN v TO w").await;
    let after = assert_one_schema(&db, "pt", &after, "RENAME COLUMN");
    assert_eq!(generation(&db), generation0 + 2);
    assert!(db.shape_epoch() > shape, "a renamed column must move the shape epoch");
    assert_ne!(db.slot_shape_epoch("pt"), slot, "a renamed column must move the table's shape epoch");
    for tenant in 1..=5 {
        let out = exec(&db, &format!("SELECT w FROM pt WHERE tenant_id = {tenant} AND w >= 0")).await;
        assert_eq!(count(&out), 12, "tenant {tenant}");
    }

    // A partition materialized after the change takes the new schema.
    let mut tenant = 6;
    while partitions(&db, "pt").len() == after.len() {
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, w) VALUES ({tenant}, 1, 5)")).await;
        tenant += 1;
        assert!(tenant < 200, "no new partition materialized");
    }
    let shell = schema_text(&db.partitioned_table("pt").unwrap().shell().schema);
    for (pid, part) in partitions(&db, "pt") {
        assert_eq!(schema_text(&part.schema), shell, "partition {pid}");
        assert!(index_names(&part).await.iter().any(|n| n == "pt_v"), "partition {pid} has no pt_v");
    }
    close(d, db);
}

/// Validation reads every partition: a CHECK, NOT NULL or UNIQUE that one
/// partition's rows alone break is refused, whichever partition it is, and
/// nothing is written.
///
/// Mutation: the validation's fan-out skips one partition (the CHECK and
/// NOT NULL probes run as a fan-out SELECT; UNIQUE reads
/// `row_tables_held`): the change is accepted over that partition's row.
#[tokio::test(flavor = "multi_thread")]
async fn validation_reads_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = populated(d).await;
    let parts = partitions(&db, "pt");
    // One tenant per open partition.
    let mut tenants: Vec<(u32, i64)> = Vec::new();
    for tenant in 1..=5i64 {
        let pt = db.partitioned_table("pt").unwrap();
        let route = pt.route_where(&format!("tenant_id = {tenant}")).unwrap();
        let epoch = pt.epoch().unwrap();
        let pid = pt.materialized(&epoch, &route).unwrap()[0].0;
        if !tenants.iter().any(|(p, _)| *p == pid) {
            tenants.push((pid, tenant));
        }
    }
    assert_eq!(tenants.len(), parts.len(), "a tenant per open partition");
    let shell_before = schema_text(&db.partitioned_table("pt").unwrap().shell().schema);
    for (pid, tenant) in tenants {
        let at = format!("partition {pid} (tenant {tenant})");
        exec(&db, &format!("UPDATE pt SET v = -7, s = NULL WHERE tenant_id = {tenant} AND id = {}", tenant - 1)).await;
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v, s) VALUES ({tenant}, 900, 1, 'a'), ({tenant}, 901, 1, 'b')")).await;
        for ddl in [
            "ALTER TABLE pt ADD CONSTRAINT pt_c CHECK (v >= 0)",
            "ALTER TABLE pt ALTER COLUMN s SET NOT NULL",
            "ALTER TABLE pt ADD CONSTRAINT pt_u UNIQUE (tenant_id, v)",
        ] {
            assert!(try_exec(&db, ddl).await.is_err(), "{at}: `{ddl}` was accepted over the partition's rows");
            assert_eq!(schema_text(&db.partitioned_table("pt").unwrap().shell().schema), shell_before, "{at}: `{ddl}`");
        }
        exec(&db, &format!("UPDATE pt SET v = 1, s = 'w' WHERE tenant_id = {tenant} AND id = {}", tenant - 1)).await;
        exec(&db, &format!("DELETE FROM pt WHERE tenant_id = {tenant} AND id >= 900")).await;
    }
    exec(&db, "UPDATE pt SET s = 'x' WHERE s IS NULL").await;
    exec(&db, "ALTER TABLE pt ADD CONSTRAINT pt_c CHECK (v >= 0)").await;
    exec(&db, "ALTER TABLE pt ALTER COLUMN s SET NOT NULL").await;
    exec(&db, "ALTER TABLE pt ADD CONSTRAINT pt_u UNIQUE (tenant_id, id, s)").await;
    close(d, db);
}

/// A UNIQUE or PRIMARY KEY without every key column is refused (§1b), by
/// `ADD CONSTRAINT` and `CREATE UNIQUE INDEX`, partial or not, with the
/// rule in the reason and nothing written; with every key column it is
/// accepted. Mutation: the rule off in `partitioned_alter_refusal`.
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_key_without_every_key_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d).await;
    exec(
        &db,
        "CREATE TABLE pk2 (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT, PRIMARY KEY (a, b, c)) \
         PARTITION BY HASH (a, b) PARTITIONS 4",
    )
    .await;
    exec(&db, "INSERT INTO pk2 (a, b, c) VALUES (1, 1, 1), (1, 2, 2), (2, 1, 3)").await;
    let shell = schema_text(&db.partitioned_table("pk2").unwrap().shell().schema);
    for ddl in [
        "ALTER TABLE pk2 ADD CONSTRAINT u1 UNIQUE (a, c)",
        "ALTER TABLE pk2 ADD CONSTRAINT u2 UNIQUE (c)",
        "ALTER TABLE pk2 ADD UNIQUE (b, c)",
        "CREATE UNIQUE INDEX u3 ON pk2 (a, c)",
        "CREATE UNIQUE INDEX u4 ON pk2 (c) WHERE c > 0",
        "CREATE UNIQUE INDEX u5 ON pk2 (a)",
    ] {
        let e = try_exec(&db, ddl).await.expect_err(ddl);
        assert!(
            e.contains("does not contain partition key column") && e.contains("not supported"),
            "`{ddl}`: {e}"
        );
        assert_eq!(schema_text(&db.partitioned_table("pk2").unwrap().shell().schema), shell, "`{ddl}` wrote");
    }
    exec(&db, "ALTER TABLE pk2 ADD CONSTRAINT ok1 UNIQUE (b, a, c)").await;
    exec(&db, "CREATE UNIQUE INDEX ok2 ON pk2 (a, b) WHERE c > 100").await;
    assert!(try_exec(&db, "INSERT INTO pk2 (a, b, c) VALUES (1, 1, 1)").await.is_err());
    close(d, db);
}

/// With `index_storage = snapshot`: partitions that loaded their indexes
/// from a snapshot taken before a CREATE INDEX never load that snapshot
/// after it, so after a crash the new index is built from rows and answers
/// as a scan; after a clean shutdown under the new shape they load again.
/// (A changed index count is refused by the load plan as well as by the
/// identity; the next test keeps the count.)
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_snapshot_from_before_a_schema_change_is_never_loaded_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    {
        let db = populated(d).await;
        for (_, part) in partitions(&db, "pt") {
            settled(&part).await;
        }
        let report = db.seal_for_shutdown(Duration::from_secs(60)).await;
        assert!(report.skipped.is_empty() && report.sealed.iter().all(|s| s.outcome.is_clean()));
        close(d, db);
    }
    let db = open(d).await;
    let parts = partitions(&db, "pt");
    for (pid, part) in &parts {
        settled(part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid} did not load before the change");
    }
    exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
    for (pid, part) in partitions(&db, "pt") {
        settled(&part).await;
        assert_ne!(part.index_origin(), Origin::Loaded, "partition {pid} loaded a snapshot from before CREATE INDEX");
    }
    // A crash: no shutdown seal. The snapshots on disk are the old ones.
    close(d, db);
    let db = open(d).await;
    for (pid, part) in partitions(&db, "pt") {
        settled(&part).await;
        assert_ne!(part.index_origin(), Origin::Loaded, "partition {pid} loaded a stale snapshot after a crash");
        assert!(index_names(&part).await.iter().any(|n| n == "pt_v"), "partition {pid} has no pt_v");
    }
    for v in [0i64, 14, 58, 119, 7] {
        let indexed = count(&exec(&db, &format!("SELECT id FROM pt WHERE v = {v}")).await);
        let scanned = count(&exec(&db, &format!("SELECT id FROM pt WHERE v + 0 = {v}")).await);
        assert_eq!(indexed, scanned, "v = {v}");
    }
    // Sealed under the new shape, they load again.
    let report = db.seal_for_shutdown(Duration::from_secs(60)).await;
    assert!(report.skipped.is_empty() && report.sealed.iter().all(|s| s.outcome.is_clean()));
    close(d, db);
    let db = open(d).await;
    for (pid, part) in partitions(&db, "pt") {
        settled(&part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid} did not load under the new shape");
    }
    close(d, db);
}

/// The same, with the index count kept: a snapshot sealed with an index
/// on `v`, then that index dropped and one on `s` created, then a crash.
/// Only the identity's shape tells the old sections from the new index's,
/// so the partitions must rebuild, and the index on `s` answer as a scan.
///
/// Mutation: the snapshot identity unchanged by the index change (the index
/// list left out of `shape_hash`): the partitions load `v`'s sections as
/// the index on `s`.
#[tokio::test(flavor = "multi_thread")]
async fn a_swapped_index_never_loads_the_old_index_sections() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let _snap = storage_override::set(d, IndexStorage::Snapshot);
    {
        let db = populated(d).await;
        exec(&db, "CREATE INDEX pt_v ON pt (v)").await;
        for (_, part) in partitions(&db, "pt") {
            settled(&part).await;
        }
        let report = db.seal_for_shutdown(Duration::from_secs(60)).await;
        assert!(report.skipped.is_empty() && report.sealed.iter().all(|s| s.outcome.is_clean()));
        close(d, db);
    }
    let db = open(d).await;
    for (pid, part) in partitions(&db, "pt") {
        settled(&part).await;
        assert_eq!(part.index_origin(), Origin::Loaded, "partition {pid} did not load before the change");
    }
    exec(&db, "DROP INDEX pt_v").await;
    exec(&db, "CREATE INDEX pt_s ON pt (s)").await;
    for (_, part) in partitions(&db, "pt") {
        settled(&part).await;
    }
    close(d, db);
    let db = open(d).await;
    for (pid, part) in partitions(&db, "pt") {
        settled(&part).await;
        assert_ne!(part.index_origin(), Origin::Loaded, "partition {pid} loaded the sections of a dropped index");
    }
    for s in ["w0", "w1", "w2", "nope"] {
        let indexed = count(&exec(&db, &format!("SELECT id FROM pt WHERE s = '{s}'")).await);
        let scanned = count(&exec(&db, &format!("SELECT id FROM pt WHERE s || '' = '{s}'")).await);
        assert_eq!(indexed, scanned, "s = {s}");
    }
    close(d, db);
}

async fn settled(t: &DbTable) {
    let deadline = Instant::now() + Duration::from_secs(60);
    tokio::time::timeout(Duration::from_secs(60), t.index_load_settled()).await.expect("the index load never settled");
    while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(Instant::now() < deadline, "the filter or the indexes never became ready");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
