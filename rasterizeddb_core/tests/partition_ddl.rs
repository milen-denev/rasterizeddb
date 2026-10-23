//! `CREATE TABLE … PARTITION BY HASH` through the SQL front door
//! (`docs/partitioning.md` §2, §1.4, §1.5, §1b; phase 2a).
//!
//! The supported shape creates a partitioned table: a catalogue row with a
//! uid, `__pt_<uid>_*` files and no file under the logical name. Every other
//! partitioning shape, and every key the rules refuse, is refused with a
//! reason the wire reports as 0A000, and writes nothing. DDL that would
//! change a partitioned table's schema is refused before it writes.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        row::schema::TableSchema,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
        storage_providers::traits::StorageIO,
    },
};
use rclite::Arc;

/// Runs a test body on a thread with a stack big enough for the engine; see
/// `table_lifecycle.rs` for why both the thread and the runtime need it.
fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
}

fn path_str(p: &std::path::Path) -> String {
    p.to_str().expect("a UTF-8 path").to_string()
}

/// The engine's answer: `Ok(())` for success, `Err(message)` for status 3.
/// A `CREATE UNIQUE INDEX` is first rewritten into the `ALTER TABLE … ADD
/// CONSTRAINT` pgwire turns it into, since the executor alone does not take it.
async fn run(db: &Arc<Database>, q: &str) -> Result<(), String> {
    let rewritten = rasterizeddb_core::core::rql::lexer_alter::unique_index_as_constraint(q);
    let q = rewritten.as_deref().unwrap_or(q);
    let Some(purpose) = recognize_query_purpose(q) else {
        return Err(format!("not recognised: {q}"));
    };
    let out = execute(purpose, db.clone()).await;
    if out.first() == Some(&3) {
        Err(String::from_utf8_lossy(&out[1..]).into_owned())
    } else {
        Ok(())
    }
}

/// The table catalogue's length on disk. A CREATE appends a row to it, so an
/// unchanged length means no row was written.
async fn catalogue_len(db: &Database) -> u64 {
    db.db_io_pointers.get_len().await + db.db_io_rows.get_len().await
}

/// Files in the database directory whose names start with `prefix`.
fn files_starting(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .expect("read the database directory")
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with(prefix))
        .collect();
    out.sort();
    out
}

/// Asserts `message` maps to SQLSTATE 0A000. Mirrors the order of
/// `pgwire::sqlstate_for`: "not supported" must be there, and none of the
/// needles that function tests first may be.
fn assert_0a000(sql: &str, message: &str) {
    let m = message.to_ascii_lowercase();
    assert!(m.contains("not supported"), "{sql} is not 0A000: {message}");
    for earlier in [
        "canceling statement",
        "deadlock detected",
        "could not serialize access",
        "duplicate key value",
        "duplicate value for unique",
        "violates",
        "permission denied",
        "does not exist",
        "table not found",
        "already exists",
        "out of range",
        "invalid input syntax",
        "could not parse",
        "cannot parse",
        "division by zero",
        "statement too complex",
    ] {
        assert!(!m.contains(earlier), "{sql}: \"{earlier}\" maps its message away from 0A000: {message}");
    }
    assert!(!(m.contains("not null") && m.contains("null value")), "{sql}: {message}");
}

/// The shell's schema as its file holds it, for comparing before and after a
/// refused statement: a statement that writes a sidecar and is refused only
/// at the reload leaves the in-memory schema as it was, so only the file
/// shows the write.
async fn on_disk_schema(db: &Database, table: &str) -> String {
    let io = db.tables.get(table).expect("the shell").io_schema.clone();
    let s = TableSchema::load(io).await.expect("the schema loads");
    format!(
        "fields={:?} fks={} uks={} idx={} paths={} defaults={} checks={}",
        s.fields
            .iter()
            .map(|f| format!("{}:{:?}:{}:{}", f.name, f.db_type, f.write_order, f.is_deleted))
            .collect::<Vec<_>>(),
        s.foreign_keys.len(),
        s.unique_keys.len(),
        s.secondary_indexes.len(),
        s.path_indexes.len(),
        s.defaults.len(),
        s.checks.len(),
    )
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    init_globals();
    Arc::new(Database::new_with_backend(&path_str(dir), false).await)
}

/// Each accepted shape creates a partitioned table: one catalogue row, a
/// nonzero uid, the declared number of partitions (64 by default), the key
/// as written, and no file under the logical name.
#[test]
fn accepted_shapes_create_partitioned_tables() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        for (name, sql, partitions, arity) in [
            (
                "pd_one",
                "CREATE TABLE pd_one (tenant_id BIGINT NOT NULL, n INT) PARTITION BY HASH (tenant_id)",
                64,
                1,
            ),
            (
                "pd_two",
                "CREATE TABLE pd_two (tenant_id BIGINT NOT NULL, g INT NOT NULL, asset_id INT NOT NULL, \
                 PRIMARY KEY (tenant_id, g, asset_id)) PARTITION BY HASH (tenant_id, g) PARTITIONS 16",
                16,
                2,
            ),
            (
                "pd_three",
                "create table if not exists pd_three (a int not null, b uuid not null, c text not null, \
                 unique (c, b, a)) partition by hash (a, b, c) partitions 3;",
                3,
                3,
            ),
            (
                "pd_quoted",
                "CREATE TABLE \"pd_quoted\" (\"k\" VARCHAR(10) NOT NULL UNIQUE) PARTITION BY HASH (\"k\") \
                 PARTITIONS 1",
                1,
                1,
            ),
            (
                "pd_serial",
                "CREATE TABLE pd_serial (id BIGSERIAL PRIMARY KEY, body TEXT) PARTITION BY HASH (id)",
                64,
                1,
            ),
            (
                "pd_text",
                "CREATE TABLE pd_text (k TEXT NOT NULL) PARTITION BY HASH (k) PARTITIONS 2",
                2,
                1,
            ),
        ] {
            let before = catalogue_len(&db).await;
            run(&db, sql).await.unwrap_or_else(|e| panic!("{sql} was refused: {e}"));
            assert!(catalogue_len(&db).await > before, "{sql} wrote no catalogue row");
            let table = db
                .partitioned
                .get(name)
                .unwrap_or_else(|| panic!("{sql} did not create a partitioned table"))
                .clone();
            assert_ne!(table.uid(), 0, "{sql}");
            assert!(table.broken().is_none(), "{sql}: {:?}", table.broken());
            assert_eq!(table.epoch().expect("the map").map.len(), partitions, "{sql}");
            assert_eq!(table.spec().expect("the key").arity(), arity, "{sql}");
            assert!(db.tables.get(name).is_some_and(|t| t.schema.name == name), "{sql}: no shell");
            assert_eq!(files_starting(dir.path(), name), Vec::<String>::new(), "{sql}");
            assert!(
                !files_starting(dir.path(), &format!("__pt_{:016x}_", table.uid())).is_empty(),
                "{sql}: no __pt_ files"
            );
        }
        // The key's write orders are the named columns', in the order named.
        let two = db.partitioned.get("pd_two").unwrap().clone();
        let orders: Vec<u32> = two.spec().unwrap().levels().iter().map(|l| l.write_order).collect();
        let shell = db.tables.get("pd_two").unwrap().clone();
        let order_of =
            |n: &str| shell.schema.fields.iter().find(|f| f.name == n).unwrap().write_order as u32;
        assert_eq!(orders, vec![order_of("tenant_id"), order_of("g")]);
    });
}

/// Every refused shape says why, maps to 0A000, and leaves the catalogue,
/// both maps and the directory as they were. Covers the parser's refusals
/// and the database's (nullability, uniqueness, FKs, generated keys, the
/// arity and type rules).
#[test]
fn refused_shapes_write_nothing() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        run(&db, "CREATE TABLE pd_parent (id INT PRIMARY KEY)").await.expect("parent");

        for (sql, names) in [
            ("CREATE TABLE pr (k INT) PARTITION BY HASH (k)", "nullable"),
            ("CREATE TABLE pr (k INT NOT NULL, id INT, PRIMARY KEY (id)) PARTITION BY HASH (k)", "UNIQUE"),
            ("CREATE TABLE pr (k INT NOT NULL, id INT UNIQUE) PARTITION BY HASH (k)", "UNIQUE"),
            ("CREATE TABLE pr (k INT NOT NULL, id INT NOT NULL, UNIQUE (id)) PARTITION BY HASH (k)", "UNIQUE"),
            (
                "CREATE TABLE pr (k INT NOT NULL, a INT NOT NULL, \
                 k2 INT GENERATED ALWAYS AS (a + 1) STORED NOT NULL) PARTITION BY HASH (k2)",
                "generated",
            ),
            (
                "CREATE TABLE pr (k INT NOT NULL REFERENCES pd_parent(id)) PARTITION BY HASH (k)",
                "FOREIGN KEY",
            ),
            (
                "CREATE TABLE pr (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, d INT NOT NULL) \
                 PARTITION BY HASH (a, b, c, d)",
                "4 columns",
            ),
            ("CREATE TABLE pr (k DOUBLE PRECISION NOT NULL) PARTITION BY HASH (k)", "F64"),
            ("CREATE TABLE pr (k NUMERIC(10, 2) NOT NULL) PARTITION BY HASH (k)", "NUMERIC"),
            ("CREATE TABLE pr (k BOOLEAN NOT NULL) PARTITION BY HASH (k)", "BOOL"),
            ("CREATE TABLE pr (k JSONB NOT NULL) PARTITION BY HASH (k)", "JSON"),
            ("CREATE TABLE pr (k CITEXT NOT NULL) PARTITION BY HASH (k)", "CITEXT"),
            ("CREATE TABLE pr (k BPCHAR NOT NULL) PARTITION BY HASH (k)", "BPCHAR"),
            ("CREATE TABLE pr (k NAME NOT NULL) PARTITION BY HASH (k)", "NAME"),
            ("CREATE TABLE pr (k CHAR(4) NOT NULL) PARTITION BY HASH (k)", "CHAR(4)"),
            ("CREATE TABLE pr (k VARCHAR(10) COLLATE \"C\" NOT NULL) PARTITION BY HASH (k)", "COLLATE"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY RANGE (k)", "RANGE"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY LIST (k)", "LIST"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (k) COLOCATE WITH pd_parent", "COLOCATE"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (k) WITH (fillfactor = 70)", "WITH"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (k) TABLESPACE fast", "TABLESPACE"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 0", "PARTITIONS 0"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 4096", "PARTITIONS 4096"),
            ("CREATE TABLE pr (k INT NOT NULL) PARTITION BY HASH (lower(k))", "column names"),
            ("CREATE TABLE pr (k INT NOT NULL) INHERITS (pd_parent)", "INHERITS"),
            ("CREATE TABLE pr PARTITION OF pd_parent FOR VALUES WITH (MODULUS 2, REMAINDER 0)", "PARTITION OF"),
            ("CREATE TABLE pr (k INT NOT NULL) INHERITS (pd_parent) PARTITION BY HASH (k)", "INHERITS"),
            ("CREATE TABLE public.pr (k INT NOT NULL) PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE __pt_x (k INT NOT NULL) PARTITION BY HASH (k)", "reserved"),
        ] {
            let len_before = catalogue_len(&db).await;
            let tables_before = db.tables.len();
            let files_before = files_starting(dir.path(), "");
            let reason = run(&db, sql).await.expect_err(&format!("{sql} was accepted"));
            assert!(reason.contains(names), "{sql} was refused for the wrong reason: {reason}");
            // The reserved-prefix refusal is the plain CREATE's own: an
            // invalid name, not a missing feature.
            if names != "reserved" {
                assert_0a000(sql, &reason);
            }
            assert_eq!(catalogue_len(&db).await, len_before, "{sql} wrote a catalogue row");
            assert_eq!(db.tables.len(), tables_before, "{sql} registered a table");
            assert!(db.partitioned.is_empty(), "{sql} registered a partitioned table");
            assert_eq!(files_starting(dir.path(), ""), files_before, "{sql} left files");
        }
    });
}

/// §1b: with key `(tenant_id, asset_group_id)`, `PRIMARY KEY (tenant_id,
/// asset_id)` would let equal keys sit in two partitions. Refused, naming
/// the alternative; the key's columns plus the constraint's is accepted.
#[test]
fn a_unique_key_must_contain_every_key_column() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        let sql = "CREATE TABLE assets (tenant_id BIGINT NOT NULL, asset_group_id INT NOT NULL, \
                   asset_id INT NOT NULL, PRIMARY KEY (tenant_id, asset_id)) \
                   PARTITION BY HASH (tenant_id, asset_group_id)";
        let reason = run(&db, sql).await.expect_err("accepted a primary key without asset_group_id");
        assert!(reason.contains("\"asset_group_id\""), "{reason}");
        assert!(reason.contains("UNIQUE (tenant_id, asset_group_id, asset_id)"), "{reason}");
        assert_0a000(sql, &reason);
        assert!(db.partitioned.is_empty());

        // Only the leading key column is not enough either.
        let sql = "CREATE TABLE assets (tenant_id BIGINT NOT NULL, asset_group_id INT NOT NULL, \
                   UNIQUE (tenant_id)) PARTITION BY HASH (tenant_id, asset_group_id)";
        assert_0a000(sql, &run(&db, sql).await.expect_err("accepted UNIQUE (tenant_id)"));

        run(
            &db,
            "CREATE TABLE assets (tenant_id BIGINT NOT NULL, asset_group_id INT NOT NULL, \
             asset_id INT NOT NULL, PRIMARY KEY (tenant_id, asset_group_id, asset_id)) \
             PARTITION BY HASH (tenant_id, asset_group_id)",
        )
        .await
        .expect("a primary key holding every key column");
        assert!(db.partitioned.contains_key("assets"));
    });
}

/// `IF NOT EXISTS` over an existing partitioned table succeeds and writes
/// nothing, with or without a clause; without `IF NOT EXISTS` it is refused.
#[test]
fn if_not_exists_on_a_partitioned_table_writes_nothing() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        let sql = "CREATE TABLE IF NOT EXISTS pd_ine (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 4";
        run(&db, sql).await.expect("created");
        let uid = db.partitioned.get("pd_ine").unwrap().uid();
        let len = catalogue_len(&db).await;
        let files = files_starting(dir.path(), "");
        for again in [
            sql,
            "CREATE TABLE IF NOT EXISTS pd_ine (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 8",
            "CREATE TABLE IF NOT EXISTS pd_ine (k INT NOT NULL)",
        ] {
            run(&db, again).await.unwrap_or_else(|e| panic!("{again}: {e}"));
            assert_eq!(catalogue_len(&db).await, len, "{again} wrote a row");
            assert_eq!(files_starting(dir.path(), ""), files, "{again} wrote files");
            assert_eq!(db.partitioned.get("pd_ine").unwrap().uid(), uid, "{again}");
        }
        for again in [
            "CREATE TABLE pd_ine (k INT NOT NULL) PARTITION BY HASH (k)",
            "CREATE TABLE pd_ine (k INT NOT NULL)",
        ] {
            let e = run(&db, again).await.expect_err(&format!("{again} was accepted"));
            assert!(e.to_ascii_lowercase().contains("already exists"), "{again}: {e}");
            assert_eq!(catalogue_len(&db).await, len, "{again} wrote a row");
        }
    });
}

/// FKs are refused both ways (phase 2a): a plain table cannot reference a
/// partitioned one, at CREATE or through ALTER, and the refused ALTER leaves
/// the child's FK sidecar empty.
#[test]
fn foreign_keys_onto_a_partitioned_table_are_refused() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        run(&db, "CREATE TABLE pd_fk_parent (id INT NOT NULL UNIQUE) PARTITION BY HASH (id) PARTITIONS 2")
            .await
            .expect("partitioned parent");

        let sql = "CREATE TABLE pd_fk_child (id INT, parent_id INT REFERENCES pd_fk_parent(id))";
        let len = catalogue_len(&db).await;
        let e = run(&db, sql).await.expect_err("a CREATE referencing a partitioned table");
        assert_0a000(sql, &e);
        assert!(db.tables.get("pd_fk_child").is_none());
        assert_eq!(catalogue_len(&db).await, len);

        run(&db, "CREATE TABLE pd_fk_child (id INT, parent_id INT)").await.expect("plain child");
        let before = on_disk_schema(&db, "pd_fk_child").await;
        for sql in [
            "ALTER TABLE pd_fk_child ADD CONSTRAINT c_fk FOREIGN KEY (parent_id) REFERENCES pd_fk_parent(id)",
            "ALTER TABLE pd_fk_child ADD FOREIGN KEY (parent_id) REFERENCES pd_fk_parent (id)",
            "ALTER TABLE pd_fk_child ADD CONSTRAINT c_fk2 FOREIGN KEY (parent_id) REFERENCES PD_FK_PARENT(id)",
        ] {
            let e = run(&db, sql).await.expect_err(&format!("{sql} was accepted"));
            assert_0a000(sql, &e);
            assert!(e.contains("pd_fk_parent") || e.contains("PD_FK_PARENT"), "{sql}: {e}");
        }
        assert_eq!(on_disk_schema(&db, "pd_fk_child").await, before, "a refused ALTER wrote the FK");
        assert!(db.tables.get("pd_fk_child").unwrap().schema.foreign_keys.is_empty());
    });
}

/// ALTER, RENAME and CREATE/DROP INDEX on a partitioned table are refused
/// with 0A000 before they write: its shell's schema file is unchanged.
#[test]
fn schema_ddl_on_a_partitioned_table_is_refused_before_it_writes() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let db = open(dir.path()).await;
        run(&db, "CREATE TABLE pd_plain (id INT PRIMARY KEY)").await.expect("plain");
        run(
            &db,
            "CREATE TABLE pd_alt (k INT NOT NULL, n INT, doc JSONB) PARTITION BY HASH (k) PARTITIONS 2",
        )
        .await
        .expect("partitioned");
        let before = on_disk_schema(&db, "pd_alt").await;
        let len = catalogue_len(&db).await;
        for sql in [
            "ALTER TABLE pd_alt ADD COLUMN x INT",
            "ALTER TABLE pd_alt RENAME COLUMN n TO m",
            "ALTER TABLE pd_alt DROP COLUMN n",
            "ALTER TABLE pd_alt ALTER COLUMN n SET DEFAULT 5",
            "ALTER TABLE pd_alt ALTER COLUMN n SET NOT NULL",
            "ALTER TABLE pd_alt ADD CONSTRAINT pd_alt_u UNIQUE (k, n)",
            "ALTER TABLE pd_alt ADD CONSTRAINT pd_alt_fk FOREIGN KEY (n) REFERENCES pd_plain(id)",
            "ALTER TABLE pd_alt ADD CONSTRAINT pd_alt_c CHECK (n > 0)",
            "ALTER TABLE IF EXISTS pd_alt ADD COLUMN y INT",
            "ALTER TABLE pd_alt RENAME TO pd_alt2",
            "ALTER TABLE pd_plain RENAME TO pd_alt",
            "CREATE INDEX pd_alt_n ON pd_alt (n)",
            "CREATE UNIQUE INDEX pd_alt_kn ON pd_alt (k, n)",
            "CREATE INDEX pd_alt_doc ON pd_alt ((doc->>'a'))",
        ] {
            let e = run(&db, sql).await.expect_err(&format!("{sql} was accepted"));
            assert_0a000(sql, &e);
            assert!(e.contains("partitioned"), "{sql}: {e}");
            assert_eq!(on_disk_schema(&db, "pd_alt").await, before, "{sql} wrote the shell's schema");
            assert!(db.partitioned.contains_key("pd_alt"), "{sql}");
            assert!(db.tables.get("pd_plain").is_some(), "{sql} moved the plain table");
        }
        assert_eq!(catalogue_len(&db).await, len, "a refused statement wrote a catalogue row");
    });
}

/// The cap is 2,048 declared partitions per database, not per process: a
/// full database refuses one more, a second database beside it does not, and
/// a DROP frees the room.
#[test]
fn the_cap_is_per_database() {
    on_a_big_stack(async {
        let dir_a = tempfile::tempdir().expect("dir");
        let dir_b = tempfile::tempdir().expect("dir");
        let a = open(dir_a.path()).await;
        let b = open(dir_b.path()).await;
        run(&a, "CREATE TABLE pd_full (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 2048")
            .await
            .expect("2048 in one table");
        let sql = "CREATE TABLE pd_more (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 1";
        let len = catalogue_len(&a).await;
        let e = run(&a, sql).await.expect_err("a 2049th partition");
        assert!(e.contains("2048"), "{e}");
        assert_0a000(sql, &e);
        assert_eq!(catalogue_len(&a).await, len);
        run(&b, "CREATE TABLE pd_full (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 2048")
            .await
            .expect("another database has its own cap");
        run(&a, "DROP TABLE pd_full").await.expect("drop");
        run(&a, sql).await.expect("room after the drop");
    });
}

/// An in-memory database refuses PARTITION BY (phase 2a) and creates
/// nothing.
#[test]
fn an_in_memory_database_refuses_partition_by() {
    on_a_big_stack(async {
        init_globals();
        let dir = tempfile::tempdir().expect("dir");
        let db = Arc::new(Database::new_with_backend(&path_str(dir.path()), true).await);
        let sql = "CREATE TABLE pd_mem (k INT NOT NULL) PARTITION BY HASH (k)";
        let e = run(&db, sql).await.expect_err("an in-memory partitioned table");
        assert!(e.contains("in-memory"), "{e}");
        assert_0a000(sql, &e);
        assert!(db.tables.get("pd_mem").is_none() && db.partitioned.is_empty());
    });
}

/// A pool with a backup mirror refuses PARTITION BY (phase 2a).
#[cfg(feature = "storage_pools")]
#[test]
fn a_mirrored_pool_refuses_partition_by() {
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path().join("root");
        let backup = dir.path().join("backup");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&backup).unwrap();
        let db = open(&root).await;
        db.storage
            .add_location(
                &path_str(&backup),
                rasterizeddb_core::core::storage::LocationRole::Backup,
                1,
                true,
            )
            .expect("a backup location");
        let sql = "CREATE TABLE pd_mirror (k INT NOT NULL) PARTITION BY HASH (k)";
        let len = catalogue_len(&db).await;
        let e = run(&db, sql).await.expect_err("a mirrored partitioned table");
        assert!(e.contains("backup mirrors"), "{e}");
        assert_0a000(sql, &e);
        assert_eq!(catalogue_len(&db).await, len);
        assert!(db.partitioned.is_empty());
        assert_eq!(files_starting(&backup, "__pt_"), Vec::<String>::new());
    });
}
