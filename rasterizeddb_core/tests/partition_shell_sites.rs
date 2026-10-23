//! The I8 site catalogue (`docs/partitioning.md` §16 I8, §3.3): the shell
//! never serves data.
//!
//! A partitioned table's shell sits in `Database.tables` under the logical
//! name, with a schema and no rows. Every name-keyed statement against it
//! must give a correct answer or an error, never an empty result or a
//! subset; afterwards the shell is still there, still the partitioned
//! table's, its schema on disk unchanged, and no `<logical>*` file has
//! appeared in the database directory (a path that rebuilt the table from
//! `<name>*` files would make one).
//!
//! Each site names its expected outcome. "Like the twin" means the statement
//! answers exactly what it answers on `site_q`, a plain table with the same
//! columns, constraints and rows, or fails with the same SQLSTATE. The twin
//! is given the rows the partitioned table accepts: none until S9 builds
//! INSERT, after which the data-reading sites are compared with rows in
//! them. A site that reads rows is `Refused` today; S10 and S11 move theirs
//! to `LikeTwin`.

mod partition_wire;

use std::path::Path;

use partition_wire::*;
use rasterizeddb_core::core::{
    database::Database, row::schema::TableSchema, storage_providers::traits::StorageIO,
};

const T: &str = "site_p";
const TWIN: &str = "site_q";
const DDL: &str = "(tenant_id BIGINT NOT NULL, asset_group_id INT NOT NULL, asset_id INT NOT NULL, n INT, \
                   label TEXT, PRIMARY KEY (tenant_id, asset_group_id, asset_id))";
const PARTITIONS: u32 = 8;
const PK_INDEX: &str = "site_p_tenant_id_asset_group_id_asset_id_key";

/// What a site must do.
#[derive(Clone, Copy)]
enum Expect {
    /// Refused as not built yet: 0A000, "not supported".
    Refused,
    /// The same outcome as on the twin: equal answers, or the same SQLSTATE.
    LikeTwin,
    /// Fails with this SQLSTATE (the statement is wrong on any table).
    Error(&'static str),
    /// Succeeds; the check reads the answer.
    Check(fn(&Reply, &Database) -> Result<(), String>),
}

struct Site {
    sql: &'static str,
    expect: Expect,
    /// Run before and after `sql` on both sessions, e.g. BEGIN / ROLLBACK.
    around: Option<(&'static str, &'static str)>,
    /// Who changes this expectation, or why it is what it is.
    note: &'static str,
}

const fn site(sql: &'static str, expect: Expect, note: &'static str) -> Site {
    Site { sql, expect, around: None, note }
}

const fn in_block(sql: &'static str, expect: Expect, note: &'static str) -> Site {
    Site { sql, expect, around: Some(("BEGIN", "ROLLBACK")), note }
}

fn rows_text(r: &Reply) -> String {
    format!("{:?}", r.rows)
}

fn show_tables(r: &Reply, _: &Database) -> Result<(), String> {
    let name = r.columns.iter().position(|c| c == "table_name").ok_or("no table_name column")?;
    let columns = r.columns.iter().position(|c| c == "columns").ok_or("no columns column")?;
    let mine: Vec<_> = r.rows.iter().filter(|row| row[name].as_deref() == Some(T)).collect();
    if mine.len() != 1 {
        return Err(format!("{T} listed {} times: {}", mine.len(), rows_text(r)));
    }
    if mine[0][columns].as_deref() != Some("5") {
        return Err(format!("{T} has {:?} columns", mine[0][columns]));
    }
    if r.rows.iter().any(|row| row[name].as_deref().is_some_and(|n| n.starts_with("__pt_"))) {
        return Err(format!("a partition is listed as a table: {}", rows_text(r)));
    }
    Ok(())
}

fn table_stats(r: &Reply, _: &Database) -> Result<(), String> {
    let get = |metric: &str| {
        r.rows.iter().find(|row| row[0].as_deref() == Some(metric)).and_then(|row| row[1].clone())
    };
    if get("table").as_deref() != Some(T) {
        return Err(format!("table is {:?}", get("table")));
    }
    if get("partition_key").as_deref() != Some("tenant_id, asset_group_id") {
        return Err(format!("partition_key is {:?}", get("partition_key")));
    }
    if get("partitions_declared").as_deref() != Some(&PARTITIONS.to_string()) {
        return Err(format!("partitions_declared is {:?}", get("partitions_declared")));
    }
    if get("columns").as_deref() != Some("5") {
        return Err(format!("columns is {:?}", get("columns")));
    }
    Ok(())
}

fn show_partitions(r: &Reply, _: &Database) -> Result<(), String> {
    if r.rows.len() != PARTITIONS as usize {
        return Err(format!("{} partitions listed, {PARTITIONS} declared", r.rows.len()));
    }
    Ok(())
}

/// `SHOW STORAGE` counts every table, the partitioned one once.
fn show_storage(r: &Reply, db: &Database) -> Result<(), String> {
    let line = r
        .rows
        .iter()
        .filter_map(|row| row[0].clone())
        .find(|l| l.contains("table(s)"))
        .ok_or_else(|| format!("no table count: {}", rows_text(r)))?;
    let count: usize = line
        .split(" table(s)")
        .next()
        .and_then(|head| head.rsplit(' ').next())
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| format!("unreadable count in {line}"))?;
    if count != db.tables.len() {
        return Err(format!("SHOW STORAGE counts {count} tables, the database has {}: {line}", db.tables.len()));
    }
    Ok(())
}

fn rebalance(r: &Reply, _: &Database) -> Result<(), String> {
    if r.rows.is_empty() {
        return Err("an empty report".into());
    }
    if rows_text(r).contains(T) {
        return Err(format!("a rebalance proposes moving {T}: {}", rows_text(r)));
    }
    Ok(())
}

/// The declared primary key's index is listed (declared indexes in
/// `pg_indexes`, I8). Not compared with the twin: see finding F1 in
/// `partition_twin.rs`.
fn pg_indexes(r: &Reply, _: &Database) -> Result<(), String> {
    if r.rows.len() != 1 || !rows_text(r).contains(PK_INDEX) {
        return Err(format!("expected {PK_INDEX} alone: {}", rows_text(r)));
    }
    Ok(())
}

/// Every site, in the order run. The `DROP` sites are last.
const SITES: &[Site] = &[
    // CREATE.
    site(
        "CREATE TABLE IF NOT EXISTS site_p (tenant_id BIGINT NOT NULL, asset_group_id INT NOT NULL, \
         asset_id INT NOT NULL, PRIMARY KEY (tenant_id, asset_group_id, asset_id))",
        Expect::Check(|_, _| Ok(())),
        "an existing name: success, nothing written",
    ),
    site(
        "CREATE TABLE IF NOT EXISTS site_p (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 2",
        Expect::Check(|_, _| Ok(())),
        "an existing name: success, nothing written",
    ),
    site("CREATE TABLE site_p (k INT NOT NULL)", Expect::Error("42P07"), "duplicate name"),
    site("CREATE TABLE site_p (k INT NOT NULL) PARTITION BY HASH (k)", Expect::Error("42P07"), "duplicate name"),
    // Every ALTER verb, with and without IF EXISTS: refused before writing.
    site("ALTER TABLE IF EXISTS site_p ADD COLUMN x INT", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE IF EXISTS site_p DROP COLUMN label", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p ADD COLUMN x INT", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p ADD COLUMN IF NOT EXISTS n INT", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p DROP COLUMN label", Expect::Refused, "§11"),
    site("ALTER TABLE site_p DROP COLUMN IF EXISTS nope", Expect::Refused, "§11"),
    site("ALTER TABLE site_p RENAME COLUMN label TO tag", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p RENAME TO site_p2", Expect::Refused, "I25, phase 3"),
    site("ALTER TABLE site_q RENAME TO site_p", Expect::Refused, "onto the partitioned name: refused first"),
    site("ALTER TABLE site_p ALTER COLUMN n TYPE BIGINT", Expect::Refused, "§11"),
    site("ALTER TABLE site_p ALTER COLUMN n TYPE BIGINT USING n::bigint", Expect::Refused, "§11"),
    site("ALTER TABLE site_p ALTER COLUMN n SET DEFAULT 5", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p ALTER COLUMN n DROP DEFAULT", Expect::Refused, "§11, phase 3"),
    site("ALTER TABLE site_p ALTER COLUMN n SET NOT NULL", Expect::Refused, "§11"),
    site("ALTER TABLE site_p ALTER COLUMN n DROP NOT NULL", Expect::Refused, "§11"),
    site("ALTER TABLE site_p ALTER COLUMN n SET STATISTICS 100", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p ADD CONSTRAINT site_p_u UNIQUE (tenant_id, asset_group_id, n)", Expect::Refused, "§11"),
    site("ALTER TABLE site_p ADD CONSTRAINT site_p_c CHECK (n > 0)", Expect::Refused, "§11"),
    site(
        "ALTER TABLE site_p ADD CONSTRAINT site_p_fk FOREIGN KEY (n) REFERENCES site_parent(id)",
        Expect::Refused,
        "FKs refused both ways in 2a",
    ),
    site("ALTER TABLE site_p ADD PRIMARY KEY (tenant_id, asset_group_id)", Expect::Refused, "§11"),
    site("ALTER TABLE site_p DROP CONSTRAINT site_p_tenant_id_asset_group_id_asset_id_key", Expect::Refused, "§11"),
    site("ALTER TABLE site_p OWNER TO rasterized", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p SET (fillfactor = 70)", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p SET SCHEMA other", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p ENABLE ROW LEVEL SECURITY", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p DISABLE TRIGGER ALL", Expect::Refused, "a no-op on a plain table"),
    site("ALTER TABLE site_p PARTITION BY HASH (tenant_id) PARTITIONS 4", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p SPLIT KEY (tenant_id = 1) INTO 4", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p ISOLATE KEY (tenant_id = 1)", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p MERGE PARTITIONS FOR (tenant_id = 1), FOR (tenant_id = 2)", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p DETACH PARTITION site_q", Expect::Refused, "never"),
    // Indexes.
    site("CREATE INDEX site_p_n ON site_p (n)", Expect::Refused, "reaches reload_table; §11"),
    site("CREATE INDEX IF NOT EXISTS site_p_n ON site_p (n)", Expect::Refused, "§11"),
    site("CREATE UNIQUE INDEX site_p_kn ON site_p (tenant_id, asset_group_id, n)", Expect::Refused, "§11"),
    site("CREATE INDEX site_p_l ON site_p USING hash (label)", Expect::Refused, "§11"),
    site(
        "DROP INDEX site_p_tenant_id_asset_group_id_asset_id_key",
        Expect::LikeTwin,
        "an implicit index: a no-op OK on both (so is any unknown name; PostgreSQL errs)",
    ),
    // Maintenance.
    site("VACUUM site_p", Expect::Refused, "S11: VACUUM fan-out"),
    site("VACUUM FULL site_p", Expect::Refused, "S11"),
    site("VACUUM ANALYZE site_p", Expect::Refused, "S11"),
    site(
        "VACUUM",
        Expect::Refused,
        "finding F2: bare VACUUM fails in any database holding a partitioned table, after vacuuming the \
         tables before it in map order; S11 makes it succeed with site_p in the report",
    ),
    site("VACUUM FULL", Expect::Refused, "finding F2; S11"),
    site("ANALYZE site_p", Expect::LikeTwin, "a no-op on both"),
    site("REINDEX TABLE site_p", Expect::LikeTwin, "not a statement here"),
    site("CLUSTER site_p", Expect::LikeTwin, "not a statement here"),
    site("TRUNCATE site_p", Expect::LikeTwin, "not a statement here"),
    site("COMMENT ON TABLE site_p IS 'x'", Expect::LikeTwin, "a no-op on both"),
    site("GRANT SELECT ON site_p TO PUBLIC", Expect::LikeTwin, "not a statement here"),
    // Locks.
    site("LOCK TABLE site_p", Expect::LikeTwin, "outside a block: 25P01 on both"),
    in_block(
        "LOCK TABLE site_p IN SHARE MODE",
        Expect::LikeTwin,
        "a name lock; once S9/S11 write, it must block a partition's writer",
    ),
    in_block("LOCK TABLE site_p IN ACCESS EXCLUSIVE MODE", Expect::LikeTwin, "as above"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR UPDATE", Expect::Refused, "phase 3"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR SHARE", Expect::Refused, "phase 3"),
    in_block("SELECT * FROM site_p FOR UPDATE SKIP LOCKED", Expect::Refused, "phase 3"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR UPDATE NOWAIT", Expect::Refused, "phase 3"),
    // COPY is not built for any table; the partitioned one fails alike.
    site("COPY site_p TO STDOUT", Expect::LikeTwin, "no COPY here"),
    site("COPY site_p (tenant_id, n) TO STDOUT", Expect::LikeTwin, "no COPY here"),
    site("COPY site_p FROM STDIN", Expect::LikeTwin, "no COPY here"),
    site("COPY (SELECT * FROM site_p) TO STDOUT", Expect::LikeTwin, "no COPY here"),
    // Storage.
    site("STORAGE MOVE TABLE site_p TO '/nonexistent'", Expect::Refused, "a partition moves with MOVE PARTITION"),
    site("STORAGE REBALANCE DRY RUN", Expect::Check(rebalance), "never proposes the shell"),
    site("STORAGE REBALANCE", Expect::Check(rebalance), "never moves the shell"),
    site("SHOW STORAGE", Expect::Check(show_storage), "counts the logical table once"),
    // Introspection.
    site("SHOW TABLES", Expect::Check(show_tables), "listed once, partitions never"),
    site("SHOW TABLE STATS site_p", Expect::Check(table_stats), "S8"),
    site("SHOW PARTITIONS site_p", Expect::Check(show_partitions), "S8"),
    site("DESCRIBE site_p", Expect::LikeTwin, "from the shell's schema"),
    site("SHOW COLUMNS FROM site_p", Expect::LikeTwin, "from the shell's schema"),
    site("EXPLAIN SELECT * FROM site_p", Expect::LikeTwin, "not a statement here"),
    // Every pg_* view and information_schema relation.
    site("SELECT relname, relkind, reltuples FROM pg_class WHERE relname = 'site_p'", Expect::LikeTwin, "S8"),
    site(
        "SELECT schemaname, relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = 'site_p'",
        Expect::LikeTwin,
        "S8",
    ),
    site("SELECT * FROM pg_indexes WHERE tablename = 'site_p'", Expect::Check(pg_indexes), "S8; F1"),
    site(
        "SELECT i.indisunique FROM pg_index i JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'site_p'",
        Expect::LikeTwin,
        "S8",
    ),
    site("SELECT * FROM pg_inherits", Expect::LikeTwin, "partitions are not inheritance children"),
    site("SELECT nspname FROM pg_namespace", Expect::LikeTwin, ""),
    site("SELECT datname FROM pg_database", Expect::LikeTwin, ""),
    site("SELECT * FROM pg_replication_slots", Expect::LikeTwin, ""),
    site(
        "SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema.columns \
         WHERE table_name = 'site_p' ORDER BY ordinal_position",
        Expect::LikeTwin,
        "S8",
    ),
    site("SELECT * FROM information_schema.tables WHERE table_name = 'site_p'", Expect::LikeTwin, "S8"),
    site("SELECT * FROM pg_tables WHERE tablename = 'site_p'", Expect::LikeTwin, "not a view here"),
    site("SELECT * FROM pg_constraint", Expect::LikeTwin, "not a view here"),
    site("SELECT * FROM pg_attribute", Expect::LikeTwin, "not a view here"),
    site("SELECT pg_total_relation_size('site_p')", Expect::LikeTwin, "not a function here"),
    // FKs, both directions.
    site(
        "CREATE TABLE site_child (id INT, tid BIGINT REFERENCES site_p(tenant_id))",
        Expect::Refused,
        "FKs onto a partitioned table: phase 3",
    ),
    site(
        "ALTER TABLE site_parent ADD CONSTRAINT site_parent_fk FOREIGN KEY (id) REFERENCES site_p(tenant_id)",
        Expect::Refused,
        "phase 3",
    ),
    site(
        "CREATE TABLE site_fk (k INT NOT NULL REFERENCES site_parent(id)) PARTITION BY HASH (k)",
        Expect::Refused,
        "FKs from a partitioned table: refused in 2a",
    ),
    // The data statements, through each front door.
    site("SELECT * FROM site_p", Expect::Refused, "S10"),
    site("SELECT COUNT(*) FROM site_p", Expect::Refused, "S10"),
    site("SELECT * FROM site_parent WHERE id IN (SELECT n FROM site_p)", Expect::Error("42601"), "F3; S10"),
    site("SELECT n FROM site_p UNION SELECT id FROM site_parent", Expect::Refused, "S10"),
    site("INSERT INTO site_p (tenant_id, asset_group_id, asset_id) VALUES (1, 1, 1)", Expect::Refused, "S9"),
    site("INSERT INTO site_parent (id) SELECT asset_id FROM site_p", Expect::Refused, "S10"),
    site("UPDATE site_p SET n = 1 WHERE tenant_id = 1", Expect::Refused, "S11"),
    site("DELETE FROM site_p WHERE tenant_id = 1", Expect::Refused, "S11"),
    site("DELETE FROM site_p", Expect::Refused, "S11 (upstream now runs a DELETE with no WHERE)"),
    site("DELETE FROM site_p WHERE n IS NULL OR n IS NOT NULL", Expect::Refused, "S11"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1", Expect::Refused, "S10"),
    Site {
        sql: "SELECT * FROM site_p WHERE tenant_id = 1",
        expect: Expect::Refused,
        around: Some(("BEGIN ISOLATION LEVEL REPEATABLE READ", "ROLLBACK")),
        note: "S10",
    },
];

/// The shell's schema as its file holds it.
async fn on_disk_schema(db: &Database, table: &str) -> String {
    let io = db.tables.get(table).expect("the shell").io_schema.clone();
    let s = TableSchema::load(io).await.expect("the schema loads");
    format!(
        "fields={:?} fks={} uks={} idx={} paths={} defaults={} checks={}",
        s.fields.iter().map(|f| format!("{}:{:?}:{}:{}", f.name, f.db_type, f.write_order, f.is_deleted)).collect::<Vec<_>>(),
        s.foreign_keys.len(),
        s.unique_keys.len(),
        s.secondary_indexes.len(),
        s.path_indexes.len(),
        s.defaults.len(),
        s.checks.len(),
    )
}

async fn catalogue_len(db: &Database) -> u64 {
    db.db_io_pointers.get_len().await + db.db_io_rows.get_len().await
}

/// What must hold after every site: the partitioned table is the same one,
/// its shell is in `tables` with no data and an unchanged schema file, and
/// no file under its logical name exists.
struct Invariant {
    uid: u64,
    schema: String,
}

fn check_invariant(rt: &tokio::runtime::Runtime, db: &Database, dir: &Path, inv: &Invariant, after: &str) {
    let table = db.partitioned.get(T).map(|t| t.value().clone());
    let table = table.unwrap_or_else(|| panic!("after `{after}`: {T} is no longer partitioned"));
    assert_eq!(table.uid(), inv.uid, "after `{after}`: a different partitioned table");
    assert!(table.broken().is_none(), "after `{after}`: {:?}", table.broken());
    let shell = db.tables.get(T).map(|t| t.value().clone());
    let shell = shell.unwrap_or_else(|| panic!("after `{after}`: the shell left Database.tables"));
    assert_eq!(shell.schema.name, T, "after `{after}`");
    assert!(shell.io_rows().is_err(), "after `{after}`: the entry under {T} has row storage");
    assert_eq!(rt.block_on(on_disk_schema(db, T)), inv.schema, "after `{after}`: the shell's schema file changed");
    assert_eq!(files_of_table(dir, T), Vec::<String>::new(), "after `{after}`: files under the logical name");
}

fn same_outcome(p: &Outcome, q: &Outcome) -> bool {
    let unname = |s: &str| s.replace(TWIN, T);
    match (p, q) {
        (Err(a), Err(b)) => a.code == b.code,
        (Ok(a), Ok(b)) => {
            let rows = |r: &Reply| {
                let mut v: Vec<Vec<Option<String>>> =
                    r.rows.iter().map(|row| row.iter().map(|c| c.as_deref().map(unname)).collect()).collect();
                v.sort();
                v
            };
            a.columns == b.columns
                && rows(a) == rows(b)
                && a.tags.iter().map(|t| unname(&t.to_ascii_lowercase())).collect::<Vec<_>>()
                    == b.tags.iter().map(|t| unname(&t.to_ascii_lowercase())).collect::<Vec<_>>()
        }
        _ => false,
    }
}

/// Runs `sql` (and its `around`) on a session.
fn run_site(c: &mut Client, site: &Site, sql: &str) -> Outcome {
    if let Some((before, _)) = site.around {
        c.ok(before);
    }
    let out = c.simple(sql);
    if let Some((_, after)) = site.around {
        let _ = c.simple(after);
    }
    out
}

#[test]
fn every_site_is_correct_or_an_error() {
    let dir = tempfile::tempdir().expect("dir");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        c.ok(&format!("CREATE TABLE {T} {DDL} PARTITION BY HASH (tenant_id, asset_group_id) PARTITIONS {PARTITIONS}"));
        c.ok(&format!("CREATE TABLE {TWIN} {DDL}"));
        c.ok("CREATE TABLE site_parent (id INT NOT NULL PRIMARY KEY)");
        c.ok("INSERT INTO site_parent (id) VALUES (1), (2), (3)");
        // The same rows into both, as far as the partitioned table takes them.
        for row in ["(1, 1, 1, 10, 'a')", "(1, 2, 2, 20, 'b')", "(2, 1, 3, NULL, NULL)", "(7, 3, 4, 40, 'd')"] {
            let sql = format!("INSERT INTO {T} (tenant_id, asset_group_id, asset_id, n, label) VALUES {row}");
            match c.simple(&sql) {
                Ok(_) => {
                    c.ok(&format!("INSERT INTO {TWIN} (tenant_id, asset_group_id, asset_id, n, label) VALUES {row}"));
                }
                Err(e) => assert!(e.is_unsupported(), "{sql}: {e}"),
            }
        }
    }
    let db = server.db.clone();
    let inv = Invariant {
        uid: db.partitioned.get(T).expect("partitioned").uid(),
        schema: rt.block_on(on_disk_schema(&db, T)),
    };
    check_invariant(&rt, &db, dir.path(), &inv, "setup");

    let mut cp = server.connect();
    let mut cq = server.connect();
    let mut wrong = Vec::new();
    for site in SITES {
        let catalogue_before = rt.block_on(catalogue_len(&db));
        let p = run_site(&mut cp, site, site.sql);
        let verdict: Result<(), String> = match site.expect {
            Expect::Refused => match &p {
                Err(e) if e.is_unsupported() => Ok(()),
                other => Err(format!("expected a 0A000 refusal, got {other:?}")),
            },
            Expect::Error(code) => match &p {
                Err(e) if e.code == code => Ok(()),
                other => Err(format!("expected {code}, got {other:?}")),
            },
            Expect::Check(check) => match &p {
                Ok(reply) => check(reply, &db),
                Err(e) => Err(format!("failed: {e}")),
            },
            Expect::LikeTwin => {
                let twin_sql = site.sql.replace(T, TWIN);
                let q = run_site(&mut cq, site, &twin_sql);
                if same_outcome(&p, &q) {
                    Ok(())
                } else {
                    Err(format!("differs from the twin:\n    partitioned: {p:?}\n    twin:        {q:?}"))
                }
            }
        };
        if let Err(why) = verdict {
            wrong.push(format!("`{}` ({}): {why}", site.sql, site.note));
        }
        // A refused or failed statement writes no catalogue row.
        if p.is_err() {
            assert_eq!(rt.block_on(catalogue_len(&db)), catalogue_before, "`{}` failed and wrote a catalogue row", site.sql);
        }
        check_invariant(&rt, &db, dir.path(), &inv, site.sql);
    }
    assert!(wrong.is_empty(), "sites that answered wrongly:\n  {}", wrong.join("\n  "));

    // Across a restart (the startup path, `seed_sequences`, the reaper) the
    // partitioned table is the same one.
    drop(cp);
    drop(cq);
    drop(db);
    server = server.restart();
    let db = server.db.clone();
    check_invariant(&rt, &db, dir.path(), &inv, "a restart");
    drop(db);
    drop(server);
}

/// `seed_sequences` runs at startup over every table. On a shell it cannot
/// read a maximum (plan §0.5); the table must still open as the partitioned
/// table it is, and a SERIAL insert is refused, not numbered from 1, until S9
/// fans the seeding out.
#[test]
fn a_serial_partitioned_table_survives_seed_sequences() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        c.ok("CREATE TABLE site_serial (id BIGSERIAL PRIMARY KEY, body TEXT) PARTITION BY HASH (id) PARTITIONS 4");
    }
    let uid = server.db.partitioned.get("site_serial").expect("partitioned").uid();
    server = server.restart();
    assert_eq!(server.db.partitioned.get("site_serial").map(|t| t.uid()), Some(uid));
    assert!(server.db.tables.get("site_serial").is_some_and(|t| t.io_rows().is_err()), "the shell");
    let mut c = server.connect();
    let e = c.simple("INSERT INTO site_serial (body) VALUES ('x')").expect_err("S9 builds INSERT");
    assert!(e.is_unsupported(), "{e}");
    assert_eq!(files_of_table(dir.path(), "site_serial"), Vec::<String>::new());
    drop(c);
    drop(server);
}

/// `DROP TABLE IF EXISTS` removes a partitioned table whole: its shell, its
/// map entry, its `__pt_<uid>_*` files; a second one is a no-op, a plain
/// `DROP` then fails, and an `ALTER … IF EXISTS` on the gone name is a no-op.
#[test]
fn drop_if_exists_removes_the_whole_table() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE site_drop {DDL} PARTITION BY HASH (tenant_id) PARTITIONS 4"));
    let uid = server.db.partitioned.get("site_drop").expect("partitioned").uid();
    let prefix = format!("__pt_{uid:016x}_");
    assert!(!files_starting(dir.path(), &prefix).is_empty(), "the table's own files");
    c.ok("DROP TABLE IF EXISTS site_drop");
    assert!(server.db.partitioned.get("site_drop").is_none());
    assert!(server.db.tables.get("site_drop").is_none());
    assert_eq!(files_starting(dir.path(), &prefix), Vec::<String>::new());
    assert_eq!(files_of_table(dir.path(), "site_drop"), Vec::<String>::new());
    c.ok("DROP TABLE IF EXISTS site_drop");
    let e = c.simple("DROP TABLE site_drop").expect_err("dropped already");
    assert_eq!(e.code, "42P01", "{e}");
    c.ok("ALTER TABLE IF EXISTS site_drop ADD COLUMN x INT");
    assert!(server.db.tables.get("site_drop").is_none(), "an ALTER IF EXISTS recreated it");
    assert_eq!(files_of_table(dir.path(), "site_drop"), Vec::<String>::new());
    drop(c);
    drop(server);
}
