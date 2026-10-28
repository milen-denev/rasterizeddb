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
//! is given the same rows (INSERT routes since S9), so the data-reading
//! sites compare over rows. The reading sites are `LikeTwin` since S10, and
//! the writing and maintenance ones since S11, RETURNING since S17, the
//! ON CONFLICT ones since S18, joins (with a partitioned source of
//! UPDATE … FROM / DELETE … USING) since S19, and row locks and a
//! partitioned target of UPDATE … FROM / DELETE … USING since phase 3 P3.

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
// PostgreSQL's name for a primary key's index, as upstream gives it since
// fec2dd9 (it was `<table>_<columns>_key`).
const PK_INDEX: &str = "site_p_pkey";

/// What a site must do.
#[derive(Clone, Copy)]
enum Expect {
    /// Refused as not built yet: 0A000, "not supported".
    Refused,
    /// The same outcome as on the twin: equal answers, or the same SQLSTATE.
    LikeTwin,
    /// [`Expect::LikeTwin`], and the twin succeeded with a row (or a write's
    /// tag): a statement that fails on both tables alike compares nothing.
    LikeTwinAnswered,
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
    /// Not empty: `sql` is prepared (its Describe compared too) and executed
    /// once per entry with these text parameters, each execution compared.
    runs: &'static [&'static [&'static str]],
    /// The column names the prepared statement's Describe must give.
    columns: Option<&'static [&'static str]>,
}

const fn site(sql: &'static str, expect: Expect, note: &'static str) -> Site {
    Site { sql, expect, around: None, note, runs: &[], columns: None }
}

const fn in_block(sql: &'static str, expect: Expect, note: &'static str) -> Site {
    Site { sql, expect, around: Some(("BEGIN", "ROLLBACK")), note, runs: &[], columns: None }
}

/// A prepared `SELECT`, run past its learned parse (the third run on answers
/// from the cache, S15): every Describe and execution like the twin's, rows
/// included.
const fn prepared(
    sql: &'static str,
    runs: &'static [&'static [&'static str]],
    columns: Option<&'static [&'static str]>,
    note: &'static str,
) -> Site {
    Site { sql, expect: Expect::LikeTwinAnswered, around: None, note, runs, columns }
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
    if count != db.table_count() {
        return Err(format!("SHOW STORAGE counts {count} tables, the database has {}: {line}", db.table_count()));
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

/// `site_p_kn` is declared on the shell (and so on every partition), as a
/// unique key the writes enforce.
fn kn_declared(_: &Reply, db: &Database) -> Result<(), String> {
    let pt = db.partitioned_table(T).ok_or("not partitioned")?;
    let declared = pt.shell().schema.unique_keys.iter().any(|k| !k.is_deleted && k.name == "site_p_kn");
    if declared { Ok(()) } else { Err("site_p_kn is not declared".into()) }
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
    // ALTER (phase 3 P1, §11): the metadata forms answer like the twin, in
    // pairs that leave the table as it was; what would rewrite every row,
    // a key column's change and foreign keys are refused before writing.
    site("ALTER TABLE IF EXISTS site_p ADD COLUMN x INT", Expect::Refused, "populated: rewrites every row (physical ALTER)"),
    site("ALTER TABLE site_p ADD COLUMN x INT", Expect::Refused, "populated: physical ALTER"),
    site("ALTER TABLE site_p ADD COLUMN IF NOT EXISTS n INT", Expect::LikeTwin, "an existing column: a no-op on both"),
    site("ALTER TABLE site_p DROP COLUMN IF EXISTS nope", Expect::LikeTwin, "no such column: a no-op on both"),
    site("ALTER TABLE site_p DROP COLUMN tenant_id", Expect::Refused, "§1.5: a partition key column"),
    site("ALTER TABLE site_p RENAME COLUMN label TO tag", Expect::LikeTwin, "phase 3 P1"),
    site("SELECT tenant_id, tag FROM site_p WHERE tenant_id = 1", Expect::LikeTwinAnswered, "the renamed column"),
    site("ALTER TABLE site_p RENAME COLUMN tag TO label", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_p RENAME COLUMN asset_group_id TO grp", Expect::LikeTwin, "a key column: the hash never sees names"),
    site("SELECT * FROM site_p WHERE tenant_id = 1 AND grp = 2", Expect::LikeTwinAnswered, "routed on the renamed key"),
    site("ALTER TABLE site_p RENAME COLUMN grp TO asset_group_id", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_q RENAME TO site_p", Expect::Error("42710"), "onto the partitioned name: as onto a plain one"),
    site("ALTER TABLE site_p ALTER COLUMN n TYPE BIGINT", Expect::Refused, "physical ALTER"),
    site("ALTER TABLE site_p ALTER COLUMN tenant_id TYPE INT", Expect::Refused, "a partition key column"),
    site("ALTER TABLE site_p ALTER COLUMN n TYPE BIGINT USING n::bigint", Expect::Refused, "USING: refused on both"),
    site("ALTER TABLE site_p ALTER COLUMN n SET DEFAULT 5", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_p ALTER COLUMN n DROP DEFAULT", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_p ALTER COLUMN n SET NOT NULL", Expect::LikeTwin, "a NULL n: refused on both"),
    site("ALTER TABLE site_p ALTER COLUMN n DROP NOT NULL", Expect::LikeTwin, "already nullable: a no-op on both"),
    site("ALTER TABLE site_p ALTER COLUMN tenant_id DROP NOT NULL", Expect::Refused, "§1.5"),
    site("ALTER TABLE site_p ALTER COLUMN n SET STATISTICS 100", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p ADD CONSTRAINT site_p_c CHECK (n > 0)", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_p DROP CONSTRAINT site_p_c", Expect::LikeTwin, "phase 3 P1"),
    site("ALTER TABLE site_p ADD CHECK (n > 100)", Expect::LikeTwin, "violated by rows on both: 23514"),
    site("ALTER TABLE site_p ADD CONSTRAINT site_p_u2 UNIQUE (n)", Expect::Refused, "§1b: every key column"),
    site("ALTER TABLE site_p ADD COLUMN m NUMERIC(12, 3)", Expect::Refused, "populated: physical ALTER"),
    site(
        "ALTER TABLE site_p ADD COLUMN g INT GENERATED ALWAYS AS (n + 1) STORED",
        Expect::Refused,
        "populated: physical ALTER",
    ),
    site(
        "ALTER TABLE site_p ADD CONSTRAINT site_p_fk FOREIGN KEY (n) REFERENCES site_parent(id)",
        Expect::LikeTwin,
        "phase 3 P4: the rows validated in every partition",
    ),
    site("ALTER TABLE site_p DROP CONSTRAINT IF EXISTS site_p_fk", Expect::LikeTwin, "phase 3 P4"),
    site("ALTER TABLE site_p DROP CONSTRAINT nope", Expect::LikeTwin, "no such constraint: a no-op on both"),
    site("ALTER TABLE site_p OWNER TO rasterized", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p SET (fillfactor = 70)", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p SET SCHEMA other", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p ENABLE ROW LEVEL SECURITY", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p DISABLE TRIGGER ALL", Expect::LikeTwin, "a no-op on both"),
    site("ALTER TABLE site_p PARTITION BY HASH (tenant_id) PARTITIONS 4", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p SPLIT KEY (tenant_id = 1) INTO 4", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p ISOLATE KEY (tenant_id = 1)", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p MERGE PARTITIONS FOR (tenant_id = 1), FOR (tenant_id = 2)", Expect::Refused, "phase 5"),
    site("ALTER TABLE site_p DETACH PARTITION site_q", Expect::Refused, "never"),
    // Indexes (phase 3 P1). Index names are database-wide, so these run on
    // the partitioned table alone, created and dropped in pairs.
    site("CREATE INDEX site_p_n ON site_p (n)", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site("CREATE INDEX IF NOT EXISTS site_p_n ON site_p (n)", Expect::Check(|_, _| Ok(())), "IF NOT EXISTS"),
    site("SELECT * FROM site_p WHERE n = 20", Expect::LikeTwinAnswered, "through the index"),
    site("CREATE UNIQUE INDEX site_p_kn ON site_p (tenant_id, asset_group_id, n)", Expect::Check(kn_declared), "§1b"),
    site(
        "CREATE UNIQUE INDEX IF NOT EXISTS site_p_kn ON site_p (tenant_id, asset_group_id, n)",
        Expect::Check(kn_declared),
        "an existing name: a no-op",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n, label) VALUES (1, 1, 99, 10, 'dup')",
        Expect::Error("23505"),
        "site_p_kn enforced: (1, 1, 10) is taken",
    ),
    site("CREATE UNIQUE INDEX site_p_bad ON site_p (n)", Expect::Refused, "§1b: every key column"),
    site("CREATE UNIQUE INDEX IF NOT EXISTS site_p_bad ON site_p (n)", Expect::Refused, "§1b, IF NOT EXISTS too"),
    site(
        "ALTER TABLE site_p ADD CONSTRAINT IF NOT EXISTS site_p_bad UNIQUE (n)",
        Expect::Refused,
        "§1b, IF NOT EXISTS after CONSTRAINT too",
    ),
    site("CREATE INDEX site_p_l ON site_p USING hash (label)", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site("DROP INDEX site_p_l", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site("ALTER TABLE site_p DROP CONSTRAINT site_p_kn", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site("DROP INDEX site_p_n", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site("DROP INDEX IF EXISTS site_p_n", Expect::Check(|_, _| Ok(())), "phase 3 P1"),
    site(
        "DROP INDEX site_p_tenant_id_asset_group_id_asset_id_key",
        Expect::LikeTwin,
        "an implicit index: a no-op OK on both (so is any unknown name; PostgreSQL errs)",
    ),
    // Maintenance.
    site("VACUUM site_p", Expect::LikeTwin, "S11: every materialized partition"),
    site("VACUUM FULL site_p", Expect::LikeTwin, "S11"),
    site("VACUUM ANALYZE site_p", Expect::LikeTwin, "S11"),
    site(
        "VACUUM",
        Expect::LikeTwin,
        "finding F2, fixed in S11: bare VACUUM failed in any database holding a partitioned table, after \
         vacuuming the tables before it in map order; it now vacuums site_p's partitions too",
    ),
    site("VACUUM FULL", Expect::LikeTwin, "finding F2, fixed in S11"),
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
    // Row locks by the composite identifying key, under the logical name
    // (phase 3 P3).
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR UPDATE", Expect::LikeTwin, "P3"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR SHARE", Expect::LikeTwin, "P3"),
    in_block("SELECT * FROM site_p FOR UPDATE SKIP LOCKED", Expect::LikeTwin, "P3"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1 FOR UPDATE NOWAIT", Expect::LikeTwin, "P3"),
    in_block("SELECT asset_id, n FROM site_p WHERE asset_id = 1 FOR NO KEY UPDATE", Expect::LikeTwin, "P3: fanned out"),
    // COPY is not built for any table; the partitioned one fails alike.
    site("COPY site_p TO STDOUT", Expect::LikeTwin, "no COPY here"),
    site("COPY site_p (tenant_id, n) TO STDOUT", Expect::LikeTwin, "no COPY here"),
    site("COPY site_p FROM STDIN", Expect::LikeTwin, "no COPY here"),
    // Its query reads site_p (S10); COPY then answers as on the twin.
    site("COPY (SELECT * FROM site_p) TO STDOUT", Expect::LikeTwin, "S10"),
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
    // Prepared reads past the learned parse (S15): routed per execution, and
    // described by select-item alias (owner constraint 1).
    prepared(
        "SELECT * FROM site_p WHERE tenant_id = $1 AND asset_group_id = $2",
        &[&["7", "3"], &["1", "2"], &["2", "1"], &["1", "1"], &["7", "3"]],
        None,
        "S15: the learned parse, routed per execution",
    ),
    prepared(
        "SELECT COUNT(*) FROM site_p WHERE tenant_id = $1",
        &[&["7"], &["1"], &["2"], &["999"], &["1"]],
        None,
        "S15",
    ),
    prepared(
        "SELECT n AS v, tenant_id AS t FROM site_p WHERE tenant_id = $1",
        &[&["7"], &["1"], &["2"], &["1"]],
        Some(&["v", "t"]),
        "S15; aliases by select item (1099cb0)",
    ),
    prepared(
        "SELECT tenant_id AS a, tenant_id AS b FROM site_p WHERE tenant_id = $1",
        &[&["7"], &["1"], &["2"], &["1"]],
        Some(&["a", "b"]),
        "S15; each item labels one column",
    ),
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
    // Phase 3 P4: onto a partitioned table only by its whole partition key,
    // which `tenant_id` alone is not (the key is two columns): 42830.
    site(
        "CREATE TABLE site_child (id INT, tid BIGINT REFERENCES site_p(tenant_id))",
        Expect::Error("42830"),
        "phase 3 P4: not its partition key",
    ),
    site(
        "ALTER TABLE site_parent ADD CONSTRAINT site_parent_fk FOREIGN KEY (id) REFERENCES site_p(tenant_id)",
        Expect::Error("42830"),
        "phase 3 P4: not its partition key",
    ),
    site(
        "CREATE TABLE site_fk (k INT NOT NULL REFERENCES site_parent(id)) PARTITION BY HASH (k)",
        Expect::Check(|_, db| if db.is_partitioned("site_fk") { Ok(()) } else { Err("not created".into()) }),
        "phase 3 P4: FKs from a partitioned table",
    ),
    site(
        "DROP TABLE site_fk",
        Expect::Check(|_, db| if db.is_partitioned("site_fk") { Err("still there".into()) } else { Ok(()) }),
        "phase 3 P4",
    ),
    // The data statements, through each front door.
    site("SELECT * FROM site_p", Expect::LikeTwin, "S10: a fan-out"),
    site("SELECT * FROM site_p WHERE tenant_id = 1 AND asset_group_id = 2", Expect::LikeTwin, "S10: routed"),
    site("SELECT * FROM site_p WHERE tenant_id = 99 AND asset_group_id = 1", Expect::LikeTwin, "S10: an empty route"),
    site("SELECT COUNT(*) FROM site_p", Expect::LikeTwin, "S10"),
    site("SELECT MAX(n) FROM site_p WHERE tenant_id = 1 AND tenant_id = 2", Expect::LikeTwin, "S10: constant false"),
    site("SELECT * FROM site_p ORDER BY n DESC, asset_id LIMIT 2 OFFSET 1", Expect::LikeTwin, "S10; S12 makes it cheap"),
    site("SELECT * FROM site_parent WHERE id IN (SELECT n FROM site_p)", Expect::LikeTwin, "S10"),
    site("SELECT n FROM site_p UNION SELECT id FROM site_parent", Expect::LikeTwin, "S10"),
    site("WITH x AS (SELECT n FROM site_p) SELECT COUNT(*), SUM(n) FROM x", Expect::LikeTwin, "S10"),
    // What upstream's 4301045 made work on a plain table, which failed on
    // both before and so compared nothing: several DISTINCT aggregates, a
    // function call in WHERE (wrapping a key column too: no proof), and an
    // unparenthesised ORDER BY expression.
    site(
        "SELECT COUNT(DISTINCT tenant_id), COUNT(DISTINCT label), COUNT(*) FROM site_p",
        Expect::LikeTwinAnswered,
        "S10; native in the fold since 4301045",
    ),
    site("SELECT * FROM site_p WHERE COALESCE(n, 0) > 15", Expect::LikeTwinAnswered, "S10: a fan-out"),
    site("SELECT * FROM site_p WHERE UPPER(label) = 'B'", Expect::LikeTwinAnswered, "S10: a fan-out"),
    site("SELECT * FROM site_p WHERE ABS(tenant_id) = 1 AND ABS(asset_group_id) = 2", Expect::LikeTwinAnswered, "S10: no proof"),
    site(
        "SELECT * FROM site_p WHERE tenant_id = 1 AND asset_group_id = 2 AND COALESCE(label, '') = 'b'",
        Expect::LikeTwinAnswered,
        "S10: routed, the call still applied",
    ),
    site(
        "SELECT tenant_id, asset_id, n * 2 + 1 FROM site_p ORDER BY n * 2 + 1 DESC, asset_id",
        Expect::LikeTwinAnswered,
        "S10; unparenthesised since 4301045",
    ),
    in_block(
        "UPDATE site_p SET n = n + 1 WHERE LOWER(label) = 'a' OR ABS(tenant_id) = 7",
        Expect::LikeTwinAnswered,
        "S11: a fan-out, rolled back by the block",
    ),
    site(
        "SELECT p.n, s.id FROM site_p p JOIN site_parent s ON p.asset_id = s.id",
        Expect::LikeTwinAnswered,
        "S19: a join, the partitioned side fanned out",
    ),
    site(
        "SELECT s.id, p.n, p.label FROM site_parent s LEFT JOIN site_p p ON p.asset_id = s.id AND p.tenant_id = 1",
        Expect::LikeTwinAnswered,
        "S19: a LEFT JOIN, the partitioned side null-supplying, NULLs carried",
    ),
    site(
        "SELECT a.asset_id, b.asset_id FROM site_p a JOIN site_p b ON a.tenant_id = b.tenant_id \
         WHERE a.tenant_id = 1 AND a.asset_group_id = 2",
        Expect::LikeTwinAnswered,
        "S19: a self-join, one side routed",
    ),
    site(
        "SELECT COUNT(*), SUM(p.n) FROM site_p p JOIN site_parent s ON p.asset_id = s.id",
        Expect::LikeTwinAnswered,
        "S19: an aggregate over a join",
    ),
    // A duplicate of a setup row: 23505 on both since S9.
    site("INSERT INTO site_p (tenant_id, asset_group_id, asset_id) VALUES (1, 1, 1)", Expect::LikeTwin, "S9"),
    in_block(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (9, 9, 9, 1), (8, 1, 1, 2)",
        Expect::LikeTwin,
        "S9: a routed multi-row insert, rolled back by the block",
    ),
    site(
        "INSERT INTO site_parent (id) SELECT asset_id FROM site_p",
        Expect::LikeTwin,
        "S10: 23505 on both (site_parent holds ids 1 to 3), nothing written",
    ),
    site("UPDATE site_p SET n = 1 WHERE tenant_id = 1", Expect::LikeTwin, "S11: a fan-out over tenant 1's groups"),
    site(
        "UPDATE site_p SET n = n + 1 WHERE tenant_id = 1 AND asset_group_id = 2",
        Expect::LikeTwin,
        "S11: routed to one partition",
    ),
    site(
        "UPDATE site_p SET tenant_id = 1, asset_group_id = 1, label = 'k' WHERE tenant_id = 1 AND asset_group_id = 1",
        Expect::LikeTwin,
        "S11: a key column assigned the value the WHERE pins it to",
    ),
    site("UPDATE site_p SET tenant_id = 5 WHERE tenant_id = 1", Expect::Refused, "a key change: phase 6"),
    site("UPDATE site_p SET asset_group_id = asset_group_id + 1", Expect::Refused, "a key change: phase 6"),
    site(
        "UPDATE site_p SET n = 2 WHERE tenant_id = 1 RETURNING n",
        Expect::LikeTwinAnswered,
        "S17: RETURNING over a fan-out of tenant 1's groups",
    ),
    in_block(
        "UPDATE site_p SET n = n + 1 WHERE tenant_id = 1 AND asset_group_id = 2 RETURNING asset_id AS v, n * 2, label",
        Expect::LikeTwinAnswered,
        "S17: routed, an alias and an expression, rolled back by the block",
    ),
    site("DELETE FROM site_p WHERE tenant_id = 7 RETURNING n", Expect::LikeTwinAnswered, "S17: routed"),
    in_block(
        "DELETE FROM site_p WHERE n IS NOT NULL RETURNING *",
        Expect::LikeTwinAnswered,
        "S17: a fan-out, rolled back by the block",
    ),
    in_block(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n, label) VALUES (3, 1, 5, 50, 'e') RETURNING *",
        Expect::LikeTwinAnswered,
        "S17: rolled back by the block",
    ),
    in_block(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (3, 1, 6, 1), (4, 2, 7, NULL) \
         RETURNING asset_id, n, label",
        Expect::LikeTwinAnswered,
        "S17: a multi-row insert over two partitions, rolled back by the block",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id) VALUES (5, 1, 8) ON CONFLICT DO NOTHING RETURNING asset_id",
        Expect::LikeTwinAnswered,
        "S18: DO NOTHING … RETURNING, the row inserted",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (5, 1, 8, 99) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO NOTHING",
        Expect::LikeTwinAnswered,
        "S18: DO NOTHING over a stored key, INSERT 0 0",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (5, 1, 8, 99) \
         ON CONFLICT (asset_group_id, asset_id) DO NOTHING",
        Expect::Error("42P10"),
        "S18: a target leaving out a key column matches no unique key, as in PostgreSQL",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (1, 1, 1, 5) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET n = site_p.n + EXCLUDED.n, label = 'up' \
         RETURNING *",
        Expect::LikeTwinAnswered,
        "S18: DO UPDATE … RETURNING, the post-image",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (1, 1, 1, 6) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET n = EXCLUDED.n WHERE site_p.n > 1000 \
         RETURNING asset_id, n",
        Expect::LikeTwin,
        "S18: DO UPDATE … WHERE that declines: a description and no rows",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (1, 1, 1, 7) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, n = 7",
        Expect::LikeTwinAnswered,
        "S18: a key column assigned EXCLUDED's own value keeps the point",
    ),
    site(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id) VALUES (1, 1, 1) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id + 1",
        Expect::Refused,
        "S18: a DO UPDATE that changes a key value: phase 6",
    ),
    in_block(
        "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES (1, 1, 1, 1), (3, 3, 3, 3), (5, 1, 8, 5) \
         ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET n = EXCLUDED.n * 10 RETURNING asset_id, n",
        Expect::LikeTwinAnswered,
        "S18: a multi-row upsert over three partitions, rolled back by the block",
    ),
    Site {
        sql: "INSERT INTO site_p (tenant_id, asset_group_id, asset_id, n) VALUES ($1, $2, $3, $4) \
              ON CONFLICT (tenant_id, asset_group_id, asset_id) DO UPDATE SET n = EXCLUDED.n RETURNING asset_id, n",
        expect: Expect::LikeTwinAnswered,
        around: None,
        note: "S18: a prepared upsert, inserting then updating",
        runs: &[&["6", "1", "9", "1"], &["6", "1", "9", "2"], &["1", "1", "1", "3"]],
        columns: Some(&["asset_id", "n"]),
    },
    // A partitioned target, written back by its composite key (P3): in a
    // rolled-back block, so the twin's run starts from the same rows.
    in_block(
        "UPDATE site_p SET n = 3 FROM site_parent WHERE site_p.asset_id = site_parent.id \
         RETURNING site_p.tenant_id, site_p.asset_id, site_p.n",
        Expect::LikeTwinAnswered,
        "P3: UPDATE … FROM with a partitioned target",
    ),
    in_block(
        "DELETE FROM site_p USING site_parent WHERE site_p.asset_id = site_parent.id RETURNING site_p.asset_id",
        Expect::LikeTwinAnswered,
        "P3: DELETE … USING with a partitioned target",
    ),
    // A partitioned source writing the plain, shared site_parent: in a
    // rolled-back block, so the twin's run starts from the same rows.
    in_block(
        "UPDATE site_parent SET id = site_parent.id FROM site_p WHERE site_parent.id = site_p.asset_id \
         AND site_p.n IS NOT NULL RETURNING site_parent.id",
        Expect::LikeTwinAnswered,
        "S19: UPDATE … FROM a partitioned source",
    ),
    in_block(
        "DELETE FROM site_parent USING site_p WHERE site_parent.id = site_p.asset_id RETURNING site_parent.id",
        Expect::LikeTwinAnswered,
        "S19: DELETE … USING a partitioned source",
    ),
    in_block("UPDATE site_p SET n = 9 WHERE n IS NULL", Expect::LikeTwin, "S11: a fan-out in a block, rolled back"),
    site("DELETE FROM site_p WHERE tenant_id = 1", Expect::LikeTwin, "S11"),
    site("VACUUM FULL site_p", Expect::LikeTwin, "S11: over dead versions"),
    site("SELECT * FROM site_p", Expect::LikeTwin, "after VACUUM FULL"),
    site("DELETE FROM site_p WHERE n IS NULL OR n IS NOT NULL", Expect::LikeTwin, "S11"),
    site("DELETE FROM site_p", Expect::LikeTwin, "S11 (upstream runs a DELETE with no WHERE)"),
    in_block("SELECT * FROM site_p WHERE tenant_id = 1", Expect::LikeTwin, "S10"),
    Site {
        sql: "SELECT * FROM site_p WHERE tenant_id = 1",
        expect: Expect::LikeTwin,
        around: Some(("BEGIN ISOLATION LEVEL REPEATABLE READ", "ROLLBACK")),
        note: "S10",
        runs: &[],
        columns: None,
    },
];

/// The shell's schema as its file holds it.
async fn on_disk_schema(db: &Database, table: &str) -> String {
    let io = db.table_for_tests(table).expect("the shell").io_schema.clone();
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

fn schema_text(s: &TableSchema) -> String {
    format!(
        "fields={:?} fks={} uks={:?} idx={:?} paths={} defaults={:?} checks={:?}",
        s.fields.iter().map(|f| format!("{}:{:?}:{}:{}", f.name, f.db_type, f.write_order, f.is_deleted)).collect::<Vec<_>>(),
        s.foreign_keys.len(),
        s.unique_keys.iter().map(|k| (&k.name, k.is_deleted)).collect::<Vec<_>>(),
        s.secondary_indexes.iter().map(|i| (&i.name, i.is_deleted)).collect::<Vec<_>>(),
        s.path_indexes.len(),
        s.defaults.iter().map(|d| (&d.column, &d.expression, d.is_deleted)).collect::<Vec<_>>(),
        s.checks.iter().map(|c| (&c.name, c.is_deleted)).collect::<Vec<_>>(),
    )
}

/// The shell's schema file read back, in [`schema_text`]'s words.
async fn on_disk_text(db: &Database, table: &str) -> String {
    let io = db.table_for_tests(table).expect("the shell").io_schema.clone();
    schema_text(&TableSchema::load(io).await.expect("the schema loads"))
}

async fn catalogue_len(db: &Database) -> u64 {
    db.db_io_pointers.get_len().await + db.db_io_rows.get_len().await
}

/// What must hold after every site: the partitioned table is the same one,
/// its shell is in `tables` with no data and its schema file read back as
/// the shell holds it, every partition holds the shell's schema, and no
/// file under its logical name exists. A site that failed left the schema
/// file as it was (checked by the caller); the DDL sites come in pairs, so
/// after the last site the schema is the one the catalogue started with.
struct Invariant {
    uid: u64,
    schema: String,
}

fn check_invariant(rt: &tokio::runtime::Runtime, db: &Database, dir: &Path, inv: &Invariant, after: &str) {
    let table = db.partitioned_table(T);
    let table = table.unwrap_or_else(|| panic!("after `{after}`: {T} is no longer partitioned"));
    assert_eq!(table.uid(), inv.uid, "after `{after}`: a different partitioned table");
    assert!(table.broken().is_none(), "after `{after}`: {:?}", table.broken());
    let shell = db.table_for_tests(T);
    let shell = shell.unwrap_or_else(|| panic!("after `{after}`: the shell left Database.tables"));
    assert_eq!(shell.schema.name, T, "after `{after}`");
    assert!(shell.io_rows().is_err(), "after `{after}`: the entry under {T} has row storage");
    let _ = &inv.schema;
    let shell_text = schema_text(&shell.schema);
    assert_eq!(rt.block_on(on_disk_text(db, T)), shell_text, "after `{after}`: the shell's file and memory disagree");
    let epoch = table.epoch().expect("it opens");
    for (pid, part) in table.materialized(&epoch, &rasterizeddb_core::core::partition::Route::All).expect("partitions") {
        assert_eq!(schema_text(&part.schema), shell_text, "after `{after}`: partition {pid} holds another schema");
    }
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
                && a.types == b.types
                && rows(a) == rows(b)
                && a.tags.iter().map(|t| unname(&t.to_ascii_lowercase())).collect::<Vec<_>>()
                    == b.tags.iter().map(|t| unname(&t.to_ascii_lowercase())).collect::<Vec<_>>()
        }
        _ => false,
    }
}

/// `sql` with the partitioned table's name, as a whole word, replaced by the
/// twin's: `site_parent` starts with `site_p` and must stay itself.
fn to_twin(sql: &str) -> String {
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find(T) {
        let before = rest[..at].chars().next_back();
        let after = rest[at + T.len()..].chars().next();
        out.push_str(&rest[..at]);
        if before.is_some_and(word) || after.is_some_and(word) {
            out.push_str(T);
        } else {
            out.push_str(TWIN);
        }
        rest = &rest[at + T.len()..];
    }
    out.push_str(rest);
    out
}

/// Runs `sql` (and its `around`) on a session. A prepared site's outcome is
/// its Describe and every execution, in one reply: the first failure, or the
/// columns of the Describe followed by each execution's columns, rows and
/// tags, so every one of them is compared.
fn run_site(c: &mut Client, site: &Site, sql: &str) -> Outcome {
    if !site.runs.is_empty() {
        let described = c.prepare("site", sql);
        let mut whole = match described {
            Ok(r) => r,
            Err(e) => {
                c.close_prepared("site");
                return Err(e);
            }
        };
        if let Some(want) = site.columns {
            assert_eq!(whole.columns, want, "`{sql}`: the statement Describe");
        }
        for (i, params) in site.runs.iter().enumerate() {
            let params: Vec<Option<String>> = params.iter().map(|p| Some(p.to_string())).collect();
            match c.execute_prepared("site", &params) {
                Ok(r) => {
                    if let Some(want) = site.columns {
                        assert_eq!(r.columns, want, "`{sql}` run {i}: the portal Describe");
                    }
                    whole.columns.extend(r.columns.iter().map(|col| format!("{i}:{col}")));
                    whole.types.extend(r.types);
                    whole.rows.extend(r.rows.into_iter().map(|mut row| {
                        row.insert(0, Some(format!("run {i}")));
                        row
                    }));
                    whole.tags.extend(r.tags);
                }
                Err(e) => {
                    c.close_prepared("site");
                    return Err(e);
                }
            }
        }
        c.close_prepared("site");
        return Ok(whole);
    }
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
        // The same rows into both.
        for row in ["(1, 1, 1, 10, 'a')", "(1, 2, 2, 20, 'b')", "(2, 1, 3, NULL, NULL)", "(7, 3, 4, 40, 'd')"] {
            c.ok(&format!("INSERT INTO {T} (tenant_id, asset_group_id, asset_id, n, label) VALUES {row}"));
            c.ok(&format!("INSERT INTO {TWIN} (tenant_id, asset_group_id, asset_id, n, label) VALUES {row}"));
        }
    }
    let db = server.db.clone();
    let inv = Invariant {
        uid: db.partitioned_table(T).expect("partitioned").uid(),
        schema: rt.block_on(on_disk_schema(&db, T)),
    };
    check_invariant(&rt, &db, dir.path(), &inv, "setup");

    let mut cp = server.connect();
    let mut cq = server.connect();
    let mut wrong = Vec::new();
    for site in SITES {
        let catalogue_before = rt.block_on(catalogue_len(&db));
        let schema_before = rt.block_on(on_disk_text(&db, T));
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
            Expect::LikeTwin | Expect::LikeTwinAnswered => {
                let twin_sql = to_twin(site.sql);
                let q = run_site(&mut cq, site, &twin_sql);
                let answered = match &q {
                    Ok(r) => r.columns.is_empty() || !r.rows.is_empty(),
                    Err(_) => false,
                };
                if !same_outcome(&p, &q) {
                    Err(format!("differs from the twin:\n    partitioned: {p:?}\n    twin:        {q:?}"))
                } else if matches!(site.expect, Expect::LikeTwinAnswered) && !answered {
                    Err(format!("the twin answered nothing, so nothing was compared: {q:?}"))
                } else {
                    Ok(())
                }
            }
        };
        if let Err(why) = verdict {
            wrong.push(format!("`{}` ({}): {why}", site.sql, site.note));
        }
        // A refused or failed statement writes no catalogue row.
        if p.is_err() {
            assert_eq!(rt.block_on(catalogue_len(&db)), catalogue_before, "`{}` failed and wrote a catalogue row", site.sql);
            assert_eq!(rt.block_on(on_disk_text(&db, T)), schema_before, "`{}` failed and changed the schema", site.sql);
        }
        check_invariant(&rt, &db, dir.path(), &inv, site.sql);
    }
    assert!(wrong.is_empty(), "sites that answered wrongly:\n  {}", wrong.join("\n  "));
    // The DDL sites came in pairs: the schema is the one the catalogue
    // started with.
    assert_eq!(rt.block_on(on_disk_schema(&db, T)), inv.schema, "the DDL pairs left the schema changed");
    // What the LikeTwin writes stored is the twin's, row for row.
    assert_eq!(stored_rows(&db, T), stored_rows(&db, TWIN), "the partitioned table stores other rows");

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

/// `seed_sequences` on a partitioned table (plan fact 5): the sequence is
/// the logical table's, seeded from every partition. Through the shell it
/// read nothing, so the first insert after a restart took `id` 1 again; on
/// this table, keyed and unique on `id`, that is a duplicate-key failure.
#[test]
fn a_serial_partitioned_table_survives_seed_sequences() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        c.ok("CREATE TABLE site_serial (id BIGSERIAL PRIMARY KEY, body TEXT) PARTITION BY HASH (id) PARTITIONS 4");
        c.ok("INSERT INTO site_serial (body) VALUES ('a'), ('b')");
    }
    let uid = server.db.partitioned_table("site_serial").expect("partitioned").uid();
    server = server.restart();
    assert_eq!(server.db.partitioned_table("site_serial").map(|t| t.uid()), Some(uid));
    assert!(server.db.table_for_tests("site_serial").is_some_and(|t| t.io_rows().is_err()), "the shell");
    let mut c = server.connect();
    c.ok("INSERT INTO site_serial (body) VALUES ('c')");
    let mut ids: Vec<i64> = stored_rows(&server.db, "site_serial")
        .iter()
        .map(|r| {
            let id = &r.iter().find(|(n, _)| n == "id").expect("id").1;
            i64::from_le_bytes(id.as_slice().try_into().unwrap())
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
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
    let uid = server.db.partitioned_table("site_drop").expect("partitioned").uid();
    let prefix = format!("__pt_{uid:016x}_");
    assert!(!files_starting(dir.path(), &prefix).is_empty(), "the table's own files");
    c.ok("DROP TABLE IF EXISTS site_drop");
    assert!(server.db.partitioned_table("site_drop").is_none());
    assert!(server.db.table_for_tests("site_drop").is_none());
    assert_eq!(files_starting(dir.path(), &prefix), Vec::<String>::new());
    assert_eq!(files_of_table(dir.path(), "site_drop"), Vec::<String>::new());
    c.ok("DROP TABLE IF EXISTS site_drop");
    let e = c.simple("DROP TABLE site_drop").expect_err("dropped already");
    assert_eq!(e.code, "42P01", "{e}");
    c.ok("ALTER TABLE IF EXISTS site_drop ADD COLUMN x INT");
    assert!(server.db.table_for_tests("site_drop").is_none(), "an ALTER IF EXISTS recreated it");
    assert_eq!(files_of_table(dir.path(), "site_drop"), Vec::<String>::new());
    drop(c);
    drop(server);
}
