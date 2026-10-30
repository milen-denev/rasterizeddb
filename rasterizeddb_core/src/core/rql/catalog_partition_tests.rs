//! A partitioned table in the catalogue views and the introspection
//! statements (S8, `docs/partitioning.md` §14): it is listed once under its
//! logical name, with sizes and counts that are running totals over its
//! partitions, and reports what a plain twin given the same rows reports.
//! Its partitions are never listed; its indexes come from its schema; `SHOW
//! PARTITIONS` lists the map. Insert-only: phase 2a has no partitioned
//! DELETE or UPDATE yet.

use std::path::Path;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::db_type::DbType;
use crate::core::introspect;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::partition::files::{self, PartFile};
use crate::core::partition::table::PartitionedTable;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose, recognize_query_purpose};

const ROWS: i64 = 24;
const PARTITIONS: u32 = 8;
const DDL: &str = "(tenant_id BIGINT NOT NULL, n INT NOT NULL, label TEXT, PRIMARY KEY (tenant_id, n))";

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(root: &Path) -> Arc<Database> {
    init();
    Arc::new(Database::new_named(&path_str(root), false, "catparts").await)
}

fn close(db: Arc<Database>, root: &Path) {
    drop(db);
    crate::core::wal::remove_for(&path_str(root));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    crate::core::rql::executor::execute(purpose, db.clone()).await
}

async fn exec_ok(db: &Arc<Database>, sql: &str) {
    let out = exec(db, sql).await;
    assert!(out.first().is_some_and(|b| *b != 3), "{sql} failed: {}", String::from_utf8_lossy(&out));
}

fn partitioned(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not a partitioned table"),
    }
}

/// `pt` partitioned and `twin` plain, the same columns and key, the same
/// `ROWS` rows.
async fn twins(db: &Arc<Database>) -> Arc<PartitionedTable> {
    let sql = format!("CREATE TABLE pt {DDL}");
    let purpose = recognize_query_purpose(&sql).unwrap();
    let ct = crate::core::rql::lexer_ct::create_table_from_query_purpose(&purpose).unwrap();
    db.create_partitioned_table(ct, &["tenant_id"], PARTITIONS).await.unwrap();
    exec_ok(db, &format!("CREATE TABLE twin {DDL}")).await;
    let pt = partitioned(db, "pt");
    for tenant in 0..ROWS {
        let query = format!("INSERT INTO pt (tenant_id, n, label) VALUES ({tenant}, {tenant}, 'row {tenant}')");
        let qp = QueryPurpose::InsertRow(InsertRowData {
            table_name: "pt".to_string(),
            query: query.clone(),
            on_conflict_do_nothing: false,
            on_conflict_target: Vec::new(),
            on_conflict_update: None,
        });
        let row = crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &pt.shell().schema.fields, &[], &[], 0).unwrap();
        pt.insert_row_in_txn(row, FROZEN_TX).await.unwrap();
        exec_ok(db, &query.replace("INTO pt", "INTO twin")).await;
    }
    pt
}

/// Waits out the filter backfill every reopened table starts in the
/// background. It ends in a checkpoint written before the table reports
/// ready, so until then each partition's filter file grows under the
/// figures a test compares: a snapshot of the totals taken first would
/// disagree with a view read a moment later.
async fn settle(db: &Database, pt: &PartitionedTable) {
    let epoch = pt.epoch().unwrap();
    for (_, part) in pt.materialized(&epoch, &crate::core::partition::Route::All).await.unwrap() {
        part.wait_filters_ready().await.unwrap();
    }
    let twin = db.table_for_tests("twin").unwrap().clone();
    twin.wait_filters_ready().await.unwrap();
}

/// The pids with partition files in `root`.
fn materialized_on_disk(root: &Path, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|e| match files::parse_name(&e.unwrap().file_name().into_string().unwrap())? {
            PartFile::Data { uid: u, pid } if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort();
    pids.dedup();
    pids
}

/// One column of an introspection or catalogue row, as text.
fn cell(row: &Row, index: usize, db_type: &DbType) -> String {
    let bytes = row.columns[index].data.into_slice();
    match db_type {
        DbType::STRING => String::from_utf8(bytes.to_vec()).unwrap(),
        DbType::I64 => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        DbType::BOOL => (bytes[0] != 0).to_string(),
        other => panic!("no rendering for {other:?}"),
    }
}

/// An introspection statement's rows, each as `column -> text`.
async fn show(db: &Arc<Database>, sql: &str) -> Result<Vec<Vec<(String, String)>>, String> {
    let statement = introspect::parse(sql).unwrap().unwrap();
    let fields = introspect::result_fields(&statement).unwrap();
    let out = introspect::execute(&statement, db).await;
    match out.first() {
        Some(2) => {}
        Some(3) => return Err(String::from_utf8_lossy(&out[1..]).into_owned()),
        other => panic!("{sql}: unexpected reply {other:?}"),
    }
    Ok(vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|row| {
            fields
                .iter()
                .enumerate()
                .map(|(i, (name, t))| (name.clone(), cell(row, i, t)))
                .collect()
        })
        .collect())
}

fn get<'a>(row: &'a [(String, String)], column: &str) -> &'a str {
    row.iter()
        .find(|(n, _)| n == column)
        .map(|(_, v)| v.as_str())
        .unwrap_or_else(|| panic!("no column {column} in {row:?}"))
}

/// The row of a `SHOW TABLES`-shaped result for `table`, asserting there is
/// exactly one.
fn only<'a>(rows: &'a [Vec<(String, String)>], key: &str, table: &str) -> &'a [(String, String)] {
    let found: Vec<_> = rows.iter().filter(|r| get(r, key) == table).collect();
    assert_eq!(found.len(), 1, "{table} listed {} times: {rows:?}", found.len());
    found[0]
}

/// `SHOW TABLE STATS t` as `metric -> value`.
async fn stats(db: &Arc<Database>, table: &str) -> Vec<(String, String)> {
    show(db, &format!("SHOW TABLE STATS {table}"))
        .await
        .unwrap()
        .into_iter()
        .map(|r| (get(&r, "metric").to_string(), get(&r, "value").to_string()))
        .collect()
}

/// A catalogue view's rows, each as `column -> text`, built as pgwire builds
/// it and read back through a SELECT of its temporary relation.
async fn view(db: &Arc<Database>, name: &str) -> Vec<Vec<(String, String)>> {
    let temp = super::build(name, db, FROZEN_TX, None).await.unwrap();
    let out = exec(db, &format!("SELECT * FROM {temp}")).await;
    assert_eq!(out.first(), Some(&2u8), "{name}: {}", String::from_utf8_lossy(&out));
    let fields = db.table_for_tests(&temp).unwrap().schema.fields.clone();
    let rows = vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|row| {
            fields
                .iter()
                .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                .map(|f| {
                    let bytes = row.get_column(&f.name, &fields).unwrap();
                    let bytes = bytes.into_slice();
                    let text = match f.db_type {
                        DbType::STRING => String::from_utf8(bytes.to_vec()).unwrap(),
                        DbType::I64 => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
                        DbType::BOOL => (bytes[0] != 0).to_string(),
                        ref other => panic!("no rendering for {other:?}"),
                    };
                    (f.name.clone(), text)
                })
                .collect()
        })
        .collect();
    crate::core::rql::cte::drop_temps(db, &[temp], FROZEN_TX).await;
    rows
}

/// A view's rows for one table, with `pt` written as `twin` and the columns
/// that are identities (oids derived from names) left out, sorted: what must
/// be equal between the twins.
fn twin_rows(rows: &[Vec<(String, String)>], key: &str, table: &str, skip: &[&str]) -> Vec<Vec<(String, String)>> {
    let mut out: Vec<Vec<(String, String)>> = rows
        .iter()
        .filter(|r| get(r, key) == table)
        .map(|r| {
            r.iter()
                .filter(|(n, _)| !skip.contains(&n.as_str()) && n != key)
                .map(|(n, v)| (n.clone(), v.replace("pt", "twin")))
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// I8's catalogue half, insert-only: the partitioned table is listed once
/// in `pg_class` (relkind `'r'`), `pg_stat_user_tables`,
/// `information_schema.tables` and `.columns`, and `SHOW TABLES`, never a
/// partition; its live and dead counts equal its plain twin's; its sizes are
/// the sum over its partitions plus its parent files; its implicit PK index
/// is in `pg_index` and `pg_indexes` as the twin's is.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_reports_what_its_plain_twin_does() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = twins(&db).await.uid();
    // Read after a reopen: a plain table created by SQL builds its indexes
    // before its unique keys are recorded, so until a restart the twin's
    // `pg_indexes` lacks the PK index the partitioned table (built from the
    // schema) lists. The reopen also shows the totals are rebuilt at open.
    db.sync_durable().unwrap();
    close(db, root);
    let db = open(root).await;
    let pt = partitioned(&db, "pt");
    settle(&db, &pt).await;
    let on_disk = materialized_on_disk(root, uid);
    assert!(on_disk.len() > 1, "{ROWS} tenants landed in partitions {on_disk:?}");

    let totals = pt.totals().await.unwrap();
    assert_eq!(totals.materialized as usize, on_disk.len());
    let partitions = pt.partitions().await.unwrap();
    let partition_bytes: u64 = partitions.iter().map(|p| p.bytes).sum();
    assert_eq!(totals.data + totals.pointers + totals.filters, partition_bytes);
    assert!(totals.schema > 0 && totals.map > 0 && totals.ids == 8, "parent files: {totals:?}");
    let expected_bytes = partition_bytes + totals.schema + totals.map + totals.ids;

    // pg_class: once each, relkind 'r', the twin's tuple count, and the size
    // of every file the table has.
    let class = view(&db, "pg_class").await;
    for r in &class {
        assert!(!get(r, "relname").starts_with(files::PREFIX), "a partition file is listed: {r:?}");
    }
    let pt_class = only(&class, "relname", "pt");
    let twin_class = only(&class, "relname", "twin");
    assert_eq!(get(pt_class, "relkind"), "r");
    assert_eq!(get(pt_class, "reltuples"), ROWS.to_string());
    assert_eq!(get(pt_class, "reltuples"), get(twin_class, "reltuples"));
    assert_eq!(get(pt_class, "relsize"), expected_bytes.to_string());

    let stat = view(&db, "pg_stat_user_tables").await;
    let pt_stat = only(&stat, "relname", "pt");
    let twin_stat = only(&stat, "relname", "twin");
    assert_eq!(get(pt_stat, "n_live_tup"), ROWS.to_string());
    for column in ["n_live_tup", "n_dead_tup"] {
        assert_eq!(get(pt_stat, column), get(twin_stat, column), "{column}");
    }

    let tables = view(&db, "tables").await;
    only(&tables, "table_name", "pt");
    assert!(tables.iter().all(|r| !get(r, "table_name").starts_with("__")), "{tables:?}");
    let columns = view(&db, "columns").await;
    assert_eq!(
        twin_rows(&columns, "table_name", "pt", &[]),
        twin_rows(&columns, "table_name", "twin", &[])
    );
    assert!(!twin_rows(&columns, "table_name", "pt", &[]).is_empty());

    // The implicit index of the composite PK, as the twin has it. `pg_index`
    // lists single-column keys and declared indexes only, so here it is
    // empty for both; `declared_indexes_are_listed_from_the_schema` fills it.
    let index = view(&db, "pg_index").await;
    let pt_oid = get(pt_class, "oid").to_string();
    let twin_oid = get(twin_class, "oid").to_string();
    let pt_index = twin_rows(&index, "indrelid", &pt_oid, &["indexrelid"]);
    assert_eq!(pt_index, twin_rows(&index, "indrelid", &twin_oid, &["indexrelid"]));
    let indexes = view(&db, "pg_indexes").await;
    let pt_indexes = twin_rows(&indexes, "tablename", "pt", &[]);
    assert!(
        pt_indexes.iter().any(|r| get(r, "indexdef").contains("UNIQUE")),
        "pg_indexes lists no PK for pt: {indexes:?}"
    );
    assert_eq!(pt_indexes, twin_rows(&indexes, "tablename", "twin", &[]));

    // SHOW TABLES: once, the twin's slots and pointer bytes, and the total.
    let shown = show(&db, "SHOW TABLES").await.unwrap();
    assert!(shown.iter().all(|r| !get(r, "table_name").starts_with("__")), "{shown:?}");
    let pt_row = only(&shown, "table_name", "pt");
    let twin_row = only(&shown, "table_name", "twin");
    for column in ["storage", "row_slots", "columns", "pointer_bytes", "data_bytes", "primary_key"] {
        assert_eq!(get(pt_row, column), get(twin_row, column), "SHOW TABLES {column}");
    }
    assert_eq!(get(pt_row, "row_slots"), ROWS.to_string());
    assert_eq!(get(pt_row, "total_bytes"), expected_bytes.to_string());

    let pt_stats = stats(&db, "pt").await;
    let twin_stats = stats(&db, "twin").await;
    let metric = |s: &[(String, String)], m: &str| {
        s.iter()
            .find(|(k, _)| k == m)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no metric {m}"))
    };
    for m in ["row_slots", "last_row_id", "pointer_bytes", "data_bytes", "columns"] {
        assert_eq!(metric(&pt_stats, m), metric(&twin_stats, m), "SHOW TABLE STATS {m}");
    }
    assert_eq!(metric(&pt_stats, "total_bytes"), expected_bytes.to_string());
    assert_eq!(metric(&pt_stats, "partition_key"), "tenant_id");
    assert_eq!(metric(&pt_stats, "partitions_declared"), PARTITIONS.to_string());
    assert_eq!(metric(&pt_stats, "partitions_materialized"), on_disk.len().to_string());
    assert!(!twin_stats.iter().any(|(k, _)| k == "partitions_declared"));

    // The database's totals count every partitioned file once.
    let db_stats: Vec<(String, String)> = show(&db, "SHOW DATABASE STATS")
        .await
        .unwrap()
        .into_iter()
        .map(|r| (get(&r, "metric").to_string(), get(&r, "value").to_string()))
        .collect();
    assert_eq!(metric(&db_stats, "partition_map_bytes"), totals.map.to_string());
    assert_eq!(metric(&db_stats, "tables"), "2");

    drop((pt, pt_class, twin_class));
    close(db, root);
}

/// `SHOW PARTITIONS t`: every declared partition by pid, ranges tiling the
/// hash space, materialized exactly where files are, sizes summing to the
/// partitions' share of the table's. Refused for a plain table and a name
/// nothing has.
#[tokio::test(flavor = "multi_thread")]
async fn show_partitions_lists_the_map() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let pt = twins(&db).await;
    let on_disk = materialized_on_disk(root, pt.uid());
    let totals = pt.totals().await.unwrap();

    let rows = show(&db, "SHOW PARTITIONS pt").await.unwrap();
    assert_eq!(rows.len(), PARTITIONS as usize);
    let mut next_lo = "0x0000000000000000".to_string();
    let (mut slots, mut bytes) = (0u64, 0u64);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(get(r, "pid"), i.to_string());
        assert_eq!(get(r, "lo"), next_lo, "partition {i} does not start where {} ended", i.max(1) - 1);
        next_lo = get(r, "hi").to_string();
        let materialized = on_disk.contains(&(i as u32));
        assert_eq!(get(r, "state"), if materialized { "materialized" } else { "unmaterialized" });
        assert_eq!(get(r, "location"), if materialized { path_str(root) } else { String::new() });
        let (s, b): (u64, u64) = (get(r, "row_slots").parse().unwrap(), get(r, "total_bytes").parse().unwrap());
        if !materialized {
            assert_eq!((s, b), (0, 0));
        }
        slots += s;
        bytes += b;
    }
    assert_eq!(next_lo, "0x10000000000000000", "the last partition does not end the hash space");
    assert_eq!(slots, ROWS as u64);
    assert_eq!(bytes, totals.data + totals.pointers + totals.filters);

    let e = show(&db, "SHOW PARTITIONS twin").await.unwrap_err();
    assert!(e.contains("not partitioned"), "{e}");
    let e = show(&db, "SHOW PARTITIONS nosuch").await.unwrap_err();
    assert!(e.contains("does not exist"), "{e}");
    assert!(introspect::parse("SHOW PARTITIONS").unwrap().is_err());
    assert!(introspect::parse("SHOW PARTITIONS a b").unwrap().is_err());
    assert_eq!(
        introspect::parse("show partitions \"public\".\"pt\";").unwrap().unwrap(),
        introspect::Introspect::Partitions("pt".to_string())
    );

    drop(pt);
    close(db, root);
}

/// `SET CONFIG vacuum.enabled` reaches every materialized partition: the
/// shell has no rows to reclaim, so stopping at it would report the setting
/// applied while every partition kept its old one.
#[tokio::test(flavor = "multi_thread")]
async fn set_config_vacuum_reaches_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let pt = twins(&db).await;
    let epoch = pt.epoch().unwrap();
    let parts = pt.materialized(&epoch, &crate::core::partition::Route::All).await.unwrap();
    assert!(parts.len() > 1);
    let twin = db.table_for_tests("twin").unwrap().clone();

    for enabled in [false, true] {
        let reply = show(&db, &format!("SET CONFIG vacuum.enabled = {enabled}")).await.unwrap();
        let reply = get(&reply[0], "result").to_string();
        assert!(reply.contains(&format!("({} partitions)", parts.len())), "{reply}");
        for (pid, part) in &parts {
            assert_eq!(part.vacuum_enabled().unwrap(), enabled, "partition {pid}");
        }
        assert_eq!(twin.vacuum_enabled().unwrap(), enabled);
        let s = stats(&db, "pt").await;
        assert!(s.contains(&("vacuum_enabled".to_string(), enabled.to_string())), "{s:?}");
    }

    drop((pt, parts, twin));
    close(db, root);
}

/// A partitioned table's declared indexes come from its shell's schema, the
/// same declarations every partition builds from, and are listed as a plain
/// twin's are: never "no indexes" because the shell builds none.
#[tokio::test(flavor = "multi_thread")]
async fn declared_indexes_are_listed_from_the_schema() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let pt = twins(&db).await;
    exec_ok(&db, "CREATE INDEX twin_by_n ON twin USING btree (n)").await;
    // No SQL path declares an index on a partitioned table in phase 2a, so
    // the declaration is written as CREATE INDEX would, and read at reopen.
    let n = pt.shell().schema.fields.iter().find(|f| f.name == "n").unwrap().write_order as u32;
    crate::core::index::decl::append(
        pt.shell().io_schema.clone(),
        &crate::core::index::IndexDecl {
            name: "pt_by_n".to_string(),
            method: crate::core::index::Method::BTree,
            unique: false,
            columns: vec![n],
            is_deleted: false,
        },
    )
    .await
    .unwrap();
    db.sync_durable().unwrap();
    drop(pt);
    close(db, root);

    let db = open(root).await;
    let pt = partitioned(&db, "pt");
    assert!(
        pt.shell().schema.secondary_indexes.iter().any(|d| d.name == "pt_by_n"),
        "the declaration did not survive the reopen"
    );
    let indexes = view(&db, "pg_indexes").await;
    let listed = twin_rows(&indexes, "tablename", "pt", &[]);
    assert!(
        listed.iter().any(|r| get(r, "indexname") == "twin_by_n"),
        "pg_indexes lacks the declared index: {indexes:?}"
    );
    assert_eq!(listed, twin_rows(&indexes, "tablename", "twin", &[]));

    // Ready once every partition has built its own.
    let class = view(&db, "pg_class").await;
    let pt_oid = get(only(&class, "relname", "pt"), "oid").to_string();
    let twin_oid = get(only(&class, "relname", "twin"), "oid").to_string();
    let mut ready = false;
    for _ in 0..200 {
        let index = view(&db, "pg_index").await;
        let pt_index = twin_rows(&index, "indrelid", &pt_oid, &["indexrelid"]);
        let twin_index = twin_rows(&index, "indrelid", &twin_oid, &["indexrelid"]);
        assert_eq!(pt_index.len(), twin_index.len(), "{index:?}");
        if pt_index == twin_index && pt_index.iter().all(|r| get(r, "indisready") == "true") {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ready, "pt's pg_index rows never matched the twin's, ready");

    drop(pt);
    close(db, root);
}

/// A partitioned table that cannot be opened is neither left out nor shown
/// empty: every view and statement that needs its rows or its schema
/// refuses with the reason, whether its shell reached `tables` (map log
/// gone) or not (schema gone), while the views that need only its name or
/// a schema it has still list it.
#[tokio::test(flavor = "multi_thread")]
async fn a_broken_partitioned_table_refuses_rather_than_vanishing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for (gone, shelled) in [("map", true), ("schema", false)] {
        let db = open(root).await;
        twins(&db).await;
        let uid = partitioned(&db, "pt").uid();
        db.sync_durable().unwrap();
        close(db, root);
        let file = match gone {
            "map" => files::map_file(uid, 0),
            _ => format!("{}.db", files::schema_stem(uid)),
        };
        std::fs::remove_file(root.join(&file)).unwrap();

        let db = open(root).await;
        let pt = partitioned(&db, "pt");
        assert!(pt.broken().is_some(), "{gone}: opened");
        assert_eq!(db.has_table("pt"), shelled, "{gone}: {:?}", pt.broken());
        let refused = |what: &str, e: Result<String, String>| {
            let e = e.expect_err(&format!("{gone}: {what} answered for a broken table"));
            assert!(e.contains("cannot be opened"), "{gone}: {what} erred without the reason: {e}");
        };
        for v in ["pg_class", "pg_stat_user_tables", "pg_indexes", "pg_index"] {
            refused(v, super::build(v, &db, FROZEN_TX, None).await);
        }
        for statement in ["SHOW TABLES", "SHOW DATABASE STATS", "SHOW TABLE STATS pt", "SHOW PARTITIONS pt"] {
            refused(statement, show(&db, statement).await.map(|_| String::new()));
        }
        let v = view(&db, "tables").await;
        only(&v, "table_name", "pt");
        if shelled {
            assert!(!twin_rows(&view(&db, "columns").await, "table_name", "pt", &[]).is_empty());
            show(&db, "SHOW SCHEMA MAP").await.unwrap();
        } else {
            refused("columns", super::build("columns", &db, FROZEN_TX, None).await);
            refused("SHOW COLUMNS", show(&db, "SHOW COLUMNS FROM pt").await.map(|_| String::new()));
            // Its edges are unknown; a map drawn without them is wrong.
            refused("SHOW SCHEMA MAP", show(&db, "SHOW SCHEMA MAP").await.map(|_| String::new()));
        }

        // Dropped, the rest of the catalogue answers again.
        drop(pt);
        assert!(db.drop_table("pt").await.unwrap());
        exec_ok(&db, "DROP TABLE twin").await;
        let v = view(&db, "pg_class").await;
        assert!(v.iter().all(|r| get(r, "relname") != "pt"), "{v:?}");
        close(db, root);
    }
}
