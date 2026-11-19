//! `MERGE BRANCH` and `SHOW BRANCH DIFF`: a three-way merge of a branch into
//! its parent (docs/branching.md §6).
//!
//! # Three sides
//!
//! The **base** is the branch as it was made (or as of its last merge), kept
//! closed beside it. The **branch** side is a fresh copy of the branch, taken
//! at the start of the merge so it is read at one point and can become the
//! next base once the merge commits. The **parent** is read and written inside
//! one transaction on the parent itself.
//!
//! A change is something the branch did relative to the base. It is applied to
//! the parent unless the parent changed the same thing differently, which is a
//! conflict: by default the merge then applies nothing and lists them.
//!
//! # Schema, then rows
//!
//! The branch's schema changes are the DDL it ran, recorded as it ran
//! (`branch::note_ddl`), and are replayed on the parent first, in the merge's
//! transaction. Afterwards every table the branch changed must look on the
//! parent exactly as it does on the branch — compared through
//! `information_schema.columns` and `pg_indexes` — or the merge rolls back:
//! replaying DDL the parent cannot take the same way is caught there rather
//! than merged half-right.
//!
//! Rows are compared as text, as a client reads them, and written as SQL
//! through pgwire's own path, so the parent's constraints, foreign keys,
//! indexes and triggers apply to every merged row as to any other write.
//! A table with a primary or unique key is compared row by row on it; one
//! with none, as a multiset of whole rows.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::native_session::{NativeSink, Said, read_messages};
use super::*;
use crate::core::database::catalog::branch::ConflictPolicy;

/// `SHOW BRANCHES`.
/// The tree columns come last, so a client reading the first eleven by
/// position reads what it always did.
pub(super) const BRANCH_COLUMNS: [(&str, DbType); 15] = [
    ("branch", DbType::STRING),
    ("parent", DbType::STRING),
    ("parent_exists", DbType::BOOL),
    ("point", DbType::STRING),
    ("parent_lsn", DbType::I64),
    ("method", DbType::STRING),
    ("created_at", DbType::STRING),
    ("merged_at", DbType::STRING),
    ("size_bytes", DbType::I64),
    ("base_bytes", DbType::I64),
    ("sessions", DbType::I64),
    ("depth", DbType::I64),
    ("path", DbType::STRING),
    ("children", DbType::I64),
    ("via", DbType::STRING),
];

/// `SHOW BRANCH DIFF` and `MERGE BRANCH`: one row per table with a change.
pub(super) const DIFF_COLUMNS: [(&str, DbType); 7] = [
    ("table_name", DbType::STRING),
    ("change", DbType::STRING),
    ("inserts", DbType::I64),
    ("updates", DbType::I64),
    ("deletes", DbType::I64),
    ("conflicts", DbType::I64),
    ("detail", DbType::STRING),
];

/// Rows per multi-row `INSERT` a merge writes.
const INSERT_BATCH: usize = 200;
/// Conflicts named in a refusal; the rest are counted.
const CONFLICTS_SHOWN: usize = 20;

/// One row of a diff.
pub(super) struct DiffRow {
    pub table: String,
    pub change: &'static str,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub conflicts: u64,
    pub detail: String,
}

/// What a merge or diff failed with: SQLSTATE and message.
pub(super) type Failure = (&'static str, String);

/// A session of this process's own on one database: statements run through
/// pgwire's path, the answer read back as text. Never seen by a client.
struct Internal {
    database: Arc<Database>,
    state: SessionState,
}

impl Internal {
    fn new(database: Arc<Database>) -> Self {
        let catalog = DatabaseCatalog::single_unmanaged(database.clone());
        let state = SessionState::new(
            catalog,
            database.name.clone(),
            AuthConfig::default(),
            Principal {
                name: "rdb_branch_merge".to_string(),
                superuser: true,
                authenticated: true,
            },
            ConnectionLimits::default(),
        );
        Self { database, state }
    }

    async fn run(&mut self, sql: &str) -> Result<Said, String> {
        let state = &mut self.state;
        state.message_us = crate::core::statement_clock::wall_us();
        state.error_in_batch = false;
        state.native_capture = false;
        state.native_result = None;
        let mut sink = NativeSink::default();
        let normalized = crate::core::sql_scan::normalize_string_literals(sql)?;
        let ran = Box::pin(execute_recorded(
            &self.database,
            state,
            &mut sink,
            &normalized,
            &[],
            false,
            None,
        ))
        .await;
        state.statement_failed = false;
        ran.map_err(|e| e.to_string())?;
        let said = read_messages(&sink.out)
            .ok_or_else(|| "internal error: a statement's answer could not be read".to_string())?;
        if let Some((code, message)) = &said.error {
            return Err(format!("{message} (SQLSTATE {code})"));
        }
        Ok(said)
    }

    /// Ends an open block without committing it.
    async fn rollback(&mut self) {
        if self.state.transaction.is_some() {
            let _ = self.run("ROLLBACK").await;
        }
    }
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn text(cell: &Option<Vec<u8>>) -> String {
    match cell {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => "NULL".to_string(),
    }
}

fn literal(oid: i32, cell: &Option<Vec<u8>>) -> String {
    match cell {
        None => "NULL".to_string(),
        Some(bytes) => crate::pg_params::text_param_literal(oid, &String::from_utf8_lossy(bytes)),
    }
}

/// The tables a merge considers: the database's own, not its internal
/// shadows (JSON column tables, partition storage), nor its role catalogue,
/// which a merge never carries. The `__rdb_*` catalogue tables of views,
/// sequences, functions and triggers are data, merged like any table.
fn user_tables(database: &Database) -> BTreeSet<String> {
    database
        .relation_names()
        .into_iter()
        .filter(|n| !n.starts_with("__") || n.starts_with("__rdb_"))
        .filter(|n| n != crate::auth::ROLE_TABLE)
        .filter(|n| n != KEYMAP_TABLE)
        .collect()
}

/// A table's shape as a client sees it: columns (name and type) in order, and
/// its indexes' definitions.
#[derive(Debug, Clone, PartialEq, Default)]
struct Shape {
    columns: Vec<(String, String)>,
    indexes: BTreeSet<String>,
    /// Columns `information_schema` reports `NOT NULL` (already part of
    /// each column's signature; kept apart to be asked).
    not_null: BTreeSet<String>,
}

impl Shape {
    fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(n, _)| n.clone()).collect()
    }
}

async fn shapes(
    session: &mut Internal,
    tables: &BTreeSet<String>,
) -> Result<BTreeMap<String, Shape>, String> {
    let mut out: BTreeMap<String, Shape> = tables
        .iter()
        .map(|t| (t.clone(), Shape::default()))
        .collect();
    let columns = session
        .run(
            "SELECT table_name, column_name, ordinal_position, data_type, is_nullable, \
             character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns",
        )
        .await?;
    let mut ordered: Vec<(String, i64, String, String, String)> = columns
        .rows
        .iter()
        .map(|r| {
            let at = |i: usize| r.get(i).cloned().flatten();
            (
                text(&at(0)),
                text(&at(2)).parse::<i64>().unwrap_or(0),
                text(&at(1)),
                format!(
                    "{} {} {} {} {}",
                    text(&at(3)),
                    text(&at(4)),
                    text(&at(5)),
                    text(&at(6)),
                    text(&at(7))
                ),
                text(&at(4)),
            )
        })
        .collect();
    ordered.sort();
    for (table, _, column, signature, nullable) in ordered {
        if let Some(shape) = out.get_mut(&table) {
            if nullable == "NO" {
                shape.not_null.insert(column.clone());
            }
            shape.columns.push((column, signature));
        }
    }
    let indexes = session
        .run("SELECT tablename, indexdef FROM pg_indexes")
        .await?;
    for r in &indexes.rows {
        let table = text(&r.first().cloned().flatten());
        if let Some(shape) = out.get_mut(&table) {
            shape.indexes.insert(text(&r.get(1).cloned().flatten()));
        }
    }
    Ok(out)
}

/// The columns identifying a row of `table`, as positions in `columns`: its
/// primary key, else a full (not partial) unique key, else none.
fn key_of(database: &Database, table: &str, columns: &[String]) -> Option<Vec<usize>> {
    let relation = database.relation(table)?;
    let schema = relation.schema();
    let position = |names: &[String]| -> Option<Vec<usize>> {
        names
            .iter()
            .map(|n| {
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(n.trim().trim_matches('"')))
            })
            .collect()
    };
    if let Some(pk) = &schema.primary_key {
        let names: Vec<String> = pk
            .trim_matches(|c| c == '(' || c == ')')
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if let Some(found) = position(&names) {
            return Some(found);
        }
    }
    for key in &schema.unique_keys {
        if !key.is_deleted
            && key.predicate.is_none()
            && let Some(found) = position(&key.columns)
        {
            return Some(found);
        }
    }
    schema
        .fields
        .iter()
        .find(|f| f.is_unique && !f.is_deleted)
        .and_then(|f| position(std::slice::from_ref(&f.name)))
}

/// Columns a write may not name: generated ones, which the table computes.
fn generated_of(database: &Database, table: &str) -> BTreeSet<String> {
    database
        .relation(table)
        .map(|r| {
            r.schema()
                .generated
                .iter()
                .filter(|g| !g.is_deleted)
                .map(|g| g.column.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Columns with a declared `DEFAULT`.
fn defaulted_of(database: &Database, table: &str) -> BTreeSet<String> {
    database
        .relation(table)
        .map(|r| {
            r.schema()
                .defaults
                .iter()
                .filter(|d| !d.is_deleted)
                .map(|d| d.column.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `column` of `table` is filled by a sequence when an insert
/// leaves it out: a `SERIAL`, or a `DEFAULT nextval(…)`.
fn sequence_filled(database: &Database, table: &str, column: &str) -> bool {
    database.relation(table).is_some_and(|r| {
        r.schema().defaults.iter().any(|d| {
            !d.is_deleted
                && d.column.eq_ignore_ascii_case(column)
                && d.expression.to_ascii_lowercase().contains("nextval(")
        })
    })
}

/// Tables `table` references by foreign key.
fn references_of(database: &Database, table: &str) -> BTreeSet<String> {
    database
        .relation(table)
        .map(|r| {
            r.schema()
                .foreign_keys
                .iter()
                .filter(|f| !f.is_deleted && f.parent_table != table)
                .map(|f| f.parent_table.clone())
                .collect()
        })
        .unwrap_or_default()
}

type Cell = Option<Vec<u8>>;

/// A table's rows, projected onto a column list.
struct Projected {
    rows: Vec<Vec<Cell>>,
    /// Which of the columns the side has at all.
    has: Vec<bool>,
}

async fn read_projected(
    session: &mut Internal,
    table: &str,
    columns: &[String],
) -> Result<(Projected, Vec<i32>), String> {
    read_query(session, &format!("SELECT * FROM {}", ident(table)), columns).await
}

/// Runs `sql`, a `SELECT *` of one table, and projects its rows onto
/// `columns`.
async fn read_query(
    session: &mut Internal,
    sql: &str,
    columns: &[String],
) -> Result<(Projected, Vec<i32>), String> {
    let said = session.run(sql).await?;
    let oids = said.columns.clone().unwrap_or_default();
    let at: Vec<Option<usize>> = columns
        .iter()
        .map(|c| said.names.iter().position(|n| n.eq_ignore_ascii_case(c)))
        .collect();
    let rows = said
        .rows
        .into_iter()
        .map(|row| {
            at.iter()
                .map(|i| i.and_then(|i| row.get(i).cloned().flatten()))
                .collect()
        })
        .collect();
    let projected_oids = at
        .iter()
        .map(|i| i.and_then(|i| oids.get(i).copied()).unwrap_or(0))
        .collect();
    Ok((
        Projected {
            rows,
            has: at.iter().map(Option::is_some).collect(),
        },
        projected_oids,
    ))
}

/// One table's merge: what the branch changed, what conflicts, and the
/// statements that apply it.
#[derive(Default)]
struct TablePlan {
    inserts: u64,
    updates: u64,
    deletes: u64,
    conflicts: Vec<String>,
    /// Inserts and updates, run parents-first.
    writes: Vec<String>,
    /// Deletes, run children-first.
    deletes_sql: Vec<String>,
    /// Columns the branch wrote that the parent does not have (it dropped
    /// them, or never had them: a branch re-parented past the branch that
    /// added them), left out of what is written.
    left_out: BTreeSet<String>,
    /// Rows the branch inserted into a table keyed on a sequence, to be
    /// inserted with a new key ([`TableInput::renumber`]).
    renumbered: Vec<Vec<Cell>>,
}

struct TableInput<'a> {
    table: &'a str,
    columns: &'a [String],
    oids: &'a [i32],
    key: Option<Vec<usize>>,
    generated: &'a BTreeSet<String>,
    /// Columns with a declared `DEFAULT`, lower-cased.
    defaulted: &'a BTreeSet<String>,
    base: Option<Projected>,
    branch: Projected,
    parent: Option<Projected>,
    policy: ConflictPolicy,
    /// The key is a sequence's: a row only the branch has is inserted with
    /// a key from the parent's sequence ([`TablePlan::renumbered`]), never
    /// matched to a parent row by the number it happens to share.
    renumber: bool,
}

fn condition(columns: &[String], oids: &[i32], positions: &[usize], row: &[Cell]) -> String {
    positions
        .iter()
        .map(|&i| match &row[i] {
            None => format!("{} IS NULL", ident(&columns[i])),
            v => format!("{} = {}", ident(&columns[i]), literal(oids[i], v)),
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn insert_statements(input: &TableInput<'_>, skip: &[bool], rows: &[Vec<Cell>]) -> Vec<String> {
    let writable: Vec<usize> = (0..input.columns.len())
        .filter(|&i| {
            !skip[i]
                && !input
                    .generated
                    .contains(&input.columns[i].to_ascii_lowercase())
        })
        .collect();
    let names = writable
        .iter()
        .map(|&i| ident(&input.columns[i]))
        .collect::<Vec<_>>()
        .join(", ");
    rows.chunks(INSERT_BATCH)
        .map(|chunk| {
            let values = chunk
                .iter()
                .map(|row| {
                    format!(
                        "({})",
                        writable
                            .iter()
                            .map(|&i| literal(input.oids[i], &row[i]))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "INSERT INTO {} ({}) VALUES {}",
                ident(input.table),
                names,
                values
            )
        })
        .collect()
}

fn describe_key(input: &TableInput<'_>, key: &[usize], row: &[Cell]) -> String {
    key.iter()
        .map(|&i| format!("{}={}", input.columns[i], text(&row[i])))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The three-way comparison of one table.
fn plan_table(input: TableInput<'_>) -> TablePlan {
    let mut plan = TablePlan::default();
    let n = input.columns.len();
    let base_has = input
        .base
        .as_ref()
        .map(|b| b.has.clone())
        .unwrap_or_else(|| vec![true; n]);
    let parent_has = input
        .parent
        .as_ref()
        .map(|p| p.has.clone())
        .unwrap_or_else(|| vec![true; n]);
    // Columns the base had and the parent has not: nothing the parent can
    // hold, so never written there. (A column only the branch has is one it
    // added, which a merge's replayed DDL gives the parent first.)
    let skip: Vec<bool> = (0..n).map(|i| base_has[i] && !parent_has[i]).collect();

    // A key repeated on any side (NULLs in a unique key) does not identify
    // rows: compared as whole rows instead.
    let key = input.key.clone().filter(|key| {
        let unique = |rows: &[Vec<Cell>]| {
            let mut seen = std::collections::HashSet::new();
            rows.iter()
                .all(|r| seen.insert(key.iter().map(|&i| r[i].clone()).collect::<Vec<_>>()))
        };
        unique(&input.branch.rows)
            && input.base.as_ref().is_none_or(|b| unique(&b.rows))
            && input.parent.as_ref().is_none_or(|p| unique(&p.rows))
    });

    match key {
        Some(key) => plan_keyed(&input, &key, &base_has, &parent_has, &skip, &mut plan),
        None => {
            if base_has.iter().any(|h| !h) {
                plan.conflicts.push(
                    "the branch changed the columns of a table with no primary or unique key, whose rows \
                     cannot be matched across the change"
                        .to_string(),
                );
            } else {
                plan_keyless(&input, &skip, &mut plan);
            }
        }
    }
    plan
}

fn plan_keyed(
    input: &TableInput<'_>,
    key: &[usize],
    base_has: &[bool],
    parent_has: &[bool],
    skip: &[bool],
    plan: &mut TablePlan,
) {
    let key_of = |r: &Vec<Cell>| key.iter().map(|&i| r[i].clone()).collect::<Vec<Cell>>();
    let index = |rows: &[Vec<Cell>]| -> HashMap<Vec<Cell>, usize> {
        rows.iter()
            .enumerate()
            .map(|(i, r)| (key_of(r), i))
            .collect()
    };
    let empty: Vec<Vec<Cell>> = Vec::new();
    let base_rows = input.base.as_ref().map_or(&empty, |b| &b.rows);
    let parent_rows = input.parent.as_ref().map_or(&empty, |p| &p.rows);
    let (base_at, branch_at, parent_at) = (
        index(base_rows),
        index(&input.branch.rows),
        index(parent_rows),
    );
    let mut keys: Vec<&Vec<Cell>> = base_at.keys().chain(branch_at.keys()).collect();
    keys.sort();
    keys.dedup();

    let mut inserts: Vec<Vec<Cell>> = Vec::new();
    for k in keys {
        let b = base_at.get(k).map(|&i| &base_rows[i]);
        let r = branch_at.get(k).map(|&i| &input.branch.rows[i]);
        let p = parent_at.get(k).map(|&i| &parent_rows[i]);
        // Where the base lacks a column (one the branch added), its value
        // is what adding the column gave the row: on a merge the parent has
        // it by now, from the replayed DDL. A diff runs before that, and
        // reads it as NULL — or, for a column with a DEFAULT, as the branch's
        // value, since the default cannot be worked out here: a row the
        // branch then changed in that column alone is not counted.
        let effective_base = b.map(|b| {
            (0..b.len())
                .map(|i| {
                    if base_has[i] {
                        b[i].clone()
                    } else if parent_has[i]
                        && let Some(p) = p
                    {
                        p[i].clone()
                    } else if input
                        .defaulted
                        .contains(&input.columns[i].to_ascii_lowercase())
                    {
                        r.map(|r| r[i].clone()).unwrap_or(None)
                    } else {
                        None
                    }
                })
                .collect::<Vec<Cell>>()
        });
        let effective_parent = p.map(|p| {
            (0..p.len())
                .map(|i| {
                    if parent_has[i] {
                        p[i].clone()
                    } else {
                        effective_base
                            .as_ref()
                            .map_or_else(|| r.and_then(|r| r[i].clone()), |b| b[i].clone())
                    }
                })
                .collect::<Vec<Cell>>()
        });
        let (eb, ep) = (effective_base.as_ref(), effective_parent.as_ref());
        if input.renumber
            && eb.is_none()
            && let Some(r) = r
        {
            plan.inserts += 1;
            note_left_out(input, skip, r, plan);
            plan.renumbered.push(r.clone());
            continue;
        }
        enum Act {
            Nothing,
            Apply,
            Conflict(&'static str),
        }
        let act = match (eb, r, ep) {
            (None, None, _) => Act::Nothing,
            (None, Some(r), None) => {
                let _ = r;
                Act::Apply
            }
            (None, Some(r), Some(p)) if r == p => Act::Nothing,
            (None, Some(_), Some(_)) => {
                Act::Conflict("inserted on both sides with different values")
            }
            (Some(_), None, None) => Act::Nothing,
            (Some(b), None, Some(p)) if p == b => Act::Apply,
            (Some(_), None, Some(_)) => {
                Act::Conflict("deleted on the branch, changed on the parent")
            }
            (Some(b), Some(r), None) if r == b => Act::Nothing,
            (Some(_), Some(_), None) => {
                Act::Conflict("changed on the branch, deleted on the parent")
            }
            (Some(b), Some(r), Some(_)) if r == b => Act::Nothing,
            (Some(_), Some(r), Some(p)) if r == p => Act::Nothing,
            (Some(b), Some(_), Some(p)) if p == b => Act::Apply,
            (Some(_), Some(_), Some(_)) => Act::Conflict("changed differently on both sides"),
        };
        let apply = match act {
            Act::Nothing => false,
            Act::Apply => true,
            Act::Conflict(why) => {
                let row = r
                    .or(b.map(|b| b as &Vec<Cell>))
                    .expect("a key comes from base or branch");
                plan.conflicts
                    .push(format!("{} ({})", describe_key(input, key, row), why));
                input.policy == ConflictPolicy::BranchWins
            }
        };
        if !apply {
            continue;
        }
        match (r, p) {
            (Some(r), None) => {
                plan.inserts += 1;
                note_left_out(input, skip, r, plan);
                inserts.push(r.clone());
            }
            (Some(r), Some(p)) => {
                let ep = ep.expect("parent row present");
                let sets: Vec<String> = (0..r.len())
                    .filter(|&i| !key.contains(&i))
                    .filter(|&i| {
                        !input
                            .generated
                            .contains(&input.columns[i].to_ascii_lowercase())
                    })
                    .filter(|&i| {
                        if skip[i] {
                            if r[i] != ep[i] {
                                plan.left_out.insert(input.columns[i].clone());
                            }
                            return false;
                        }
                        r[i] != ep[i] || !parent_has[i]
                    })
                    .map(|i| {
                        format!(
                            "{} = {}",
                            ident(&input.columns[i]),
                            literal(input.oids[i], &r[i])
                        )
                    })
                    .collect();
                if !sets.is_empty() {
                    plan.updates += 1;
                    plan.writes.push(format!(
                        "UPDATE {} SET {} WHERE {}",
                        ident(input.table),
                        sets.join(", "),
                        condition(input.columns, input.oids, key, p)
                    ));
                }
            }
            (None, Some(p)) => {
                plan.deletes += 1;
                plan.deletes_sql.push(format!(
                    "DELETE FROM {} WHERE {}",
                    ident(input.table),
                    condition(input.columns, input.oids, key, p)
                ));
            }
            (None, None) => {}
        }
    }
    plan.writes.extend(insert_statements(input, skip, &inserts));
}

/// Records the columns of `row` an insert leaves out for want of them on the
/// parent, when it holds anything there.
fn note_left_out(input: &TableInput<'_>, skip: &[bool], row: &[Cell], plan: &mut TablePlan) {
    for (i, cell) in row.iter().enumerate() {
        if skip[i] && cell.is_some() {
            plan.left_out.insert(input.columns[i].clone());
        }
    }
}

fn plan_keyless(input: &TableInput<'_>, skip: &[bool], plan: &mut TablePlan) {
    let count = |rows: &[Vec<Cell>]| -> BTreeMap<Vec<Cell>, u64> {
        let mut m = BTreeMap::new();
        for r in rows {
            *m.entry(r.clone()).or_insert(0) += 1;
        }
        m
    };
    let empty: Vec<Vec<Cell>> = Vec::new();
    let cb = count(input.base.as_ref().map_or(&empty, |b| &b.rows));
    let cr = count(&input.branch.rows);
    let cp = count(input.parent.as_ref().map_or(&empty, |p| &p.rows));
    // A row is found on the parent by the columns it has.
    let all: Vec<usize> = (0..input.columns.len()).filter(|&i| !skip[i]).collect();
    let mut inserts: Vec<Vec<Cell>> = Vec::new();
    let mut added: Vec<Vec<Cell>> = Vec::new();
    let mut tuples: Vec<&Vec<Cell>> = cb.keys().chain(cr.keys()).collect();
    tuples.sort();
    tuples.dedup();
    for t in tuples {
        let (b, r, p) = (
            cb.get(t).copied().unwrap_or(0),
            cr.get(t).copied().unwrap_or(0),
            cp.get(t).copied().unwrap_or(0),
        );
        if r == b || r == p {
            continue;
        }
        if p != b {
            plan.conflicts.push(format!(
                "a row with no key, held {b} time(s) at the branch point, {r} on the branch and {p} on the parent: ({})",
                t.iter().map(text).collect::<Vec<_>>().join(", ")
            ));
            if input.policy != ConflictPolicy::BranchWins {
                continue;
            }
        }
        note_left_out(input, skip, t, plan);
        if r > p {
            plan.inserts += r - p;
            for _ in 0..(r - p) {
                added.push(t.clone());
            }
        } else {
            // There is no way to delete some of identical rows: all go, and
            // the ones that stay are written back.
            plan.deletes += p - r;
            plan.deletes_sql.push(format!(
                "DELETE FROM {} WHERE {}",
                ident(input.table),
                condition(input.columns, input.oids, &all, t)
            ));
            for _ in 0..r {
                inserts.push(t.clone());
            }
        }
    }
    let statements = insert_statements(input, skip, &inserts);
    // Written back after the delete that made room for them.
    plan.deletes_sql.extend(statements);
    // Rows only added go with the other inserts, parents first.
    plan.writes.extend(insert_statements(input, skip, &added));
}

/// Orders tables so a table comes after the ones it references.
fn parents_first(tables: &[String], references: &HashMap<String, BTreeSet<String>>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut visiting = BTreeSet::new();
    fn visit(
        t: &String,
        tables: &[String],
        references: &HashMap<String, BTreeSet<String>>,
        visiting: &mut BTreeSet<String>,
        out: &mut Vec<String>,
    ) {
        if out.contains(t) || !visiting.insert(t.clone()) {
            return;
        }
        if let Some(parents) = references.get(t) {
            for p in parents {
                if tables.contains(p) {
                    visit(p, tables, references, visiting, out);
                }
            }
        }
        if !out.contains(t) {
            out.push(t.clone());
        }
    }
    for t in tables {
        visit(t, tables, references, &mut visiting, &mut out);
    }
    out
}

/// Rows per page of a paged comparison: what one round holds of each side.
#[cfg(not(test))]
const MERGE_PAGE: usize = 20_000;
/// A few, in tests: every merge a test runs then pages.
#[cfg(test)]
const MERGE_PAGE: usize = 3;

/// The block a merge (and a diff) reads in: every page at one snapshot, and
/// a write over a row another session changed meanwhile refused rather than
/// lost.
const SNAPSHOT_BLOCK: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ";

/// The three sessions a comparison reads.
struct Sides<'a> {
    base: &'a mut Internal,
    branch: &'a mut Internal,
    parent: &'a mut Internal,
}

/// What a pass over one table does besides comparing it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pass {
    /// Nothing: a diff.
    Count,
    /// Runs the deletes (a keyless table's rewrites among them).
    Deletes,
    /// Runs the updates and inserts.
    Writes,
}

/// One table, as every page of it is compared.
struct TableSpec {
    table: String,
    /// The branch's columns, which every side is read as.
    columns: Vec<String>,
    /// The columns identifying a row (`key_of`).
    key: Option<Vec<usize>>,
    /// The primary key, when the table is compared a page at a time on it;
    /// `None` reads each side whole.
    stream: Option<Vec<usize>>,
    generated: BTreeSet<String>,
    defaulted: BTreeSet<String>,
    in_base: bool,
    in_parent: bool,
    /// The key column, when the key is one column a sequence fills: rows
    /// the branch inserted are renumbered ([`TableInput::renumber`]).
    serial: Option<usize>,
    /// Columns referencing a table whose rows may be renumbered, and that
    /// table.
    refs: Vec<(usize, String)>,
    /// Which of `columns` the parent has.
    parent_has: Vec<bool>,
}

/// One table's totals over its pages.
#[derive(Default)]
struct TableTotals {
    inserts: u64,
    updates: u64,
    deletes: u64,
    conflicts: u64,
    /// The first conflicts, described; the rest are only counted.
    shown: Vec<String>,
    left_out: BTreeSet<String>,
}

/// Reads `table` from `session`, projected onto `columns`: whole, or with
/// `order` (key positions and the column types their bounds are written
/// as), the rows after `after` and through `upto` in key order, at most a
/// page. Whether the page came back full is the third value: rows past it
/// in the range may remain.
async fn read_range(
    session: &mut Internal,
    table: &str,
    columns: &[String],
    order: Option<(&[usize], &[i32])>,
    after: Option<&[Cell]>,
    upto: Option<&[Cell]>,
) -> Result<(Projected, Vec<i32>, bool), String> {
    let Some((key, oids)) = order else {
        let (projected, oids) = read_projected(session, table, columns).await?;
        return Ok((projected, oids, false));
    };
    let names: Vec<String> = key.iter().map(|&i| ident(&columns[i])).collect();
    let tuple = |parts: Vec<String>| {
        if parts.len() == 1 {
            parts.into_iter().next().unwrap_or_default()
        } else {
            format!("({})", parts.join(", "))
        }
    };
    let lhs = tuple(names.clone());
    let bound = |row: &[Cell]| tuple(key.iter().map(|&i| literal(oids[i], &row[i])).collect());
    let mut conditions = Vec::new();
    if let Some(after) = after {
        conditions.push(format!("{lhs} > {}", bound(after)));
    }
    if let Some(upto) = upto {
        conditions.push(format!("{lhs} <= {}", bound(upto)));
    }
    let sql = format!(
        "SELECT * FROM {}{} ORDER BY {} LIMIT {MERGE_PAGE}",
        ident(table),
        if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        },
        names.join(", ")
    );
    let (projected, oids) = read_query(session, &sql, columns).await?;
    let full = projected.rows.len() >= MERGE_PAGE;
    Ok((projected, oids, full))
}

fn as_ref(o: &Option<(Vec<usize>, Vec<i32>)>) -> Option<(&[usize], &[i32])> {
    o.as_ref().map(|(k, x)| (k.as_slice(), x.as_slice()))
}

/// The table, in the parent, recording keys a merge renumbered: per branch
/// (by [`BranchMeta::merge_id`]), table and the key the branch knows the row
/// by, the key the parent gave it. Written in the merge's own transaction,
/// so it holds exactly what committed. Never merged, nor recorded as a
/// branch's DDL.
///
/// [`BranchMeta::merge_id`]: crate::core::database::catalog::branch::BranchMeta::merge_id
pub(crate) const KEYMAP_TABLE: &str = "__rdb_branch_keymap";

/// One table's renumbered keys, as text.
#[derive(Default)]
struct KeyMap {
    /// From earlier merges: the branch's key to the parent's.
    prior: HashMap<String, String>,
    /// The parent's keys of `prior`.
    prior_new: std::collections::HashSet<String>,
    /// From this merge.
    fresh: HashMap<String, String>,
}

impl KeyMap {
    fn get(&self, old: &str) -> Option<&String> {
        self.prior.get(old).or_else(|| self.fresh.get(old))
    }
}

/// Renumbered keys by table.
type KeyMaps = HashMap<String, KeyMap>;

fn key_text(cell: &Cell) -> Option<String> {
    cell.as_ref().map(|b| String::from_utf8_lossy(b).into_owned())
}

/// Rewrites, in a base or branch row, each column referencing a renumbered
/// table to the key the parent knows the row by.
fn translate_refs(row: &mut [Cell], refs: &[(usize, String)], maps: &KeyMaps) {
    for (i, table) in refs {
        if let Some(map) = maps.get(table)
            && let Some(old) = key_text(&row[*i])
            && let Some(new) = map.get(&old)
        {
            row[*i] = Some(new.clone().into_bytes());
        }
    }
}

/// What a merge does with the keys a sequence gave: whether it renumbers,
/// the keys known so far, and whose they are.
struct Renumbering<'a> {
    maps: &'a mut KeyMaps,
    branch_id: &'a str,
    /// Whether [`KEYMAP_TABLE`] exists on the parent yet.
    table_ready: bool,
}

/// Compares `spec`'s table a page at a time (or whole, unpaged), adding to
/// `totals` and running what `pass` runs on the parent.
///
/// A round reads the branch's next page, then the base's and the parent's
/// within it; a side whose page comes back full ends the round's range at
/// its last row, and the sides read before are read again up to there. So
/// every side holds each row of the range, the range's bound is the
/// engine's own ordering, and no round holds more than a page of a side.
///
/// A table keyed on a sequence: rows an earlier merge renumbered are left
/// out of the ranges (their keys differ between the sides) and compared
/// afterwards by their recorded keys; a row only the branch has is inserted
/// with a key from the parent's sequence.
async fn run_table(
    sides: &mut Sides<'_>,
    spec: &TableSpec,
    policy: ConflictPolicy,
    pass: Pass,
    totals: &mut TableTotals,
    renumbering: &mut Renumbering<'_>,
) -> Result<(), Failure> {
    let internal = |e: String| ("XX000", e);
    let t = spec.table.as_str();
    let cols = spec.columns.as_slice();
    let mut oids: Vec<i32> = Vec::new();
    let mut after: Option<Vec<Cell>> = None;
    loop {
        let order = |oids: &[i32]| -> Option<(Vec<usize>, Vec<i32>)> {
            spec.stream.as_ref().map(|k| (k.clone(), oids.to_vec()))
        };
        let o = order(&oids);
        let (mut r, r_oids, r_full) = read_range(sides.branch, t, cols, as_ref(&o), after.as_deref(), None)
            .await
            .map_err(internal)?;
        if oids.is_empty() {
            oids = r_oids.clone();
        }
        let o = order(&oids);
        let mut upto: Option<Vec<Cell>> = if r_full { r.rows.last().cloned() } else { None };
        let mut b = None;
        if spec.in_base {
            let (page, _, full) = read_range(sides.base, t, cols, as_ref(&o), after.as_deref(), upto.as_deref())
                .await
                .map_err(internal)?;
            if full {
                upto = page.rows.last().cloned();
                r = read_range(sides.branch, t, cols, as_ref(&o), after.as_deref(), upto.as_deref())
                    .await
                    .map_err(internal)?
                    .0;
            }
            b = Some(page);
        }
        let mut p = None;
        if spec.in_parent {
            let (page, _, full) = read_range(sides.parent, t, cols, as_ref(&o), after.as_deref(), upto.as_deref())
                .await
                .map_err(internal)?;
            if full {
                upto = page.rows.last().cloned();
                r = read_range(sides.branch, t, cols, as_ref(&o), after.as_deref(), upto.as_deref())
                    .await
                    .map_err(internal)?
                    .0;
                if spec.in_base {
                    b = Some(
                        read_range(sides.base, t, cols, as_ref(&o), after.as_deref(), upto.as_deref())
                            .await
                            .map_err(internal)?
                            .0,
                    );
                }
            }
            p = Some(page);
        }

        // Rows renumbered before are compared after the ranges, by key.
        if let Some(c) = spec.serial
            && let Some(map) = renumbering.maps.get(t)
            && !map.prior.is_empty()
        {
            let mine = |row: &Vec<Cell>| key_text(&row[c]).is_some_and(|k| map.prior.contains_key(&k));
            r.rows.retain(|row| !mine(row));
            if let Some(b) = &mut b {
                b.rows.retain(|row| !mine(row));
            }
            if let Some(p) = &mut p {
                p.rows.retain(|row| !key_text(&row[c]).is_some_and(|k| map.prior_new.contains(&k)));
            }
        }
        compare_page(sides, spec, policy, pass, totals, renumbering, &oids, b, r, p, true).await?;
        match upto {
            Some(bound) => after = Some(bound),
            None => break,
        }
    }

    // The rows renumbered before: base and branch read by the keys they
    // know them by, the parent by its own, and matched once the first two
    // are rewritten to the parent's.
    let Some(c) = spec.serial else { return Ok(()) };
    let pairs: Vec<(String, String)> = renumbering
        .maps
        .get(t)
        .map(|m| m.prior.iter().map(|(o, n)| (o.clone(), n.clone())).collect())
        .unwrap_or_default();
    let in_list = |keys: &[&String]| {
        keys.iter()
            .map(|k| literal(oids.get(c).copied().unwrap_or(0), &Some(k.as_bytes().to_vec())))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for chunk in pairs.chunks(MERGE_PAGE) {
        let olds: Vec<&String> = chunk.iter().map(|(o, _)| o).collect();
        let news: Vec<&String> = chunk.iter().map(|(_, n)| n).collect();
        let by = |keys: &[&String]| format!("SELECT * FROM {} WHERE {} IN ({})", ident(t), ident(&cols[c]), in_list(keys));
        let rewrite = |mut page: Projected, maps: &KeyMaps| {
            for row in &mut page.rows {
                if let Some(new) = key_text(&row[c]).and_then(|k| maps.get(t).and_then(|m| m.prior.get(&k)).cloned()) {
                    row[c] = Some(new.into_bytes());
                }
            }
            page
        };
        let r = rewrite(read_query(sides.branch, &by(&olds), cols).await.map_err(internal)?.0, renumbering.maps);
        let b = if spec.in_base {
            Some(rewrite(read_query(sides.base, &by(&olds), cols).await.map_err(internal)?.0, renumbering.maps))
        } else {
            None
        };
        let p = if spec.in_parent {
            Some(read_query(sides.parent, &by(&news), cols).await.map_err(internal)?.0)
        } else {
            None
        };
        compare_page(sides, spec, policy, pass, totals, renumbering, &oids, b, r, p, false).await?;
    }
    Ok(())
}

/// Plans one page of `spec`'s table, adds it to `totals`, and runs what
/// `pass` runs. `fresh_keys`: a row only the branch has may be renumbered
/// (false for rows matched by recorded keys, which are never new).
#[allow(clippy::too_many_arguments)]
async fn compare_page(
    sides: &mut Sides<'_>,
    spec: &TableSpec,
    policy: ConflictPolicy,
    pass: Pass,
    totals: &mut TableTotals,
    renumbering: &mut Renumbering<'_>,
    oids: &[i32],
    mut b: Option<Projected>,
    mut r: Projected,
    p: Option<Projected>,
    fresh_keys: bool,
) -> Result<(), Failure> {
    let t = spec.table.as_str();
    // The branch's rows as it holds them, for those inserted with new keys:
    // their references are rewritten once, at the insert, from the branch's
    // numbers — never from numbers already rewritten to the parent's, which
    // may be another branch row's.
    let raw: HashMap<String, Vec<Cell>> = match spec.serial {
        Some(c) if pass == Pass::Writes => r
            .rows
            .iter()
            .filter_map(|row| key_text(&row[c]).map(|k| (k, row.clone())))
            .collect(),
        _ => HashMap::new(),
    };
    // References to renumbered rows, as the parent knows them.
    if !spec.refs.is_empty() {
        for row in r.rows.iter_mut().chain(b.iter_mut().flat_map(|b| b.rows.iter_mut())) {
            translate_refs(row, &spec.refs, renumbering.maps);
        }
    }
    let plan = plan_table(TableInput {
        table: t,
        columns: &spec.columns,
        oids,
        key: spec.key.clone(),
        generated: &spec.generated,
        defaulted: &spec.defaulted,
        base: b,
        branch: r,
        parent: p,
        policy,
        renumber: spec.serial.is_some() && fresh_keys,
    });
    totals.inserts += plan.inserts;
    totals.updates += plan.updates;
    totals.deletes += plan.deletes;
    totals.conflicts += plan.conflicts.len() as u64;
    for c in plan.conflicts {
        if totals.shown.len() < CONFLICTS_SHOWN {
            totals.shown.push(c);
        }
    }
    totals.left_out.extend(plan.left_out);
    let run = match pass {
        Pass::Count => Vec::new(),
        Pass::Deletes => plan.deletes_sql,
        Pass::Writes => plan.writes,
    };
    for stmt in run {
        run_on_parent(sides.parent, &stmt).await?;
    }
    if pass == Pass::Writes && !plan.renumbered.is_empty() {
        let c = spec.serial.expect("renumbered rows come from a sequence-keyed table");
        let rows: Vec<Vec<Cell>> = plan
            .renumbered
            .into_iter()
            .map(|row| key_text(&row[c]).and_then(|k| raw.get(&k).cloned()).unwrap_or(row))
            .collect();
        insert_renumbered(sides.parent, spec, oids, rows, renumbering).await?;
    }
    Ok(())
}

async fn run_on_parent(parent: &mut Internal, stmt: &str) -> Result<Said, Failure> {
    parent.run(stmt).await.map_err(|e| {
        (
            "55000",
            format!(
                "merging failed on the parent ({e}) at `{}`; nothing was merged",
                stmt.chars().take(160).collect::<String>()
            ),
        )
    })
}

/// Inserts rows the branch added to a table keyed on a sequence, each with a
/// key the parent's sequence gives, and records the pairing (in
/// [`KEYMAP_TABLE`], in the merge's transaction, and in `renumbering` for
/// the rows referencing them that are merged after).
///
/// `rows` are as the branch holds them: their references are rewritten here,
/// once. A row referencing another of the same table that is inserted here
/// waits for that one, so it is written with its new key.
async fn insert_renumbered(
    parent: &mut Internal,
    spec: &TableSpec,
    oids: &[i32],
    rows: Vec<Vec<Cell>>,
    renumbering: &mut Renumbering<'_>,
) -> Result<(), Failure> {
    let t = spec.table.as_str();
    let c = spec.serial.expect("a renumbered table has a sequence key");
    let skip: Vec<bool> = (0..spec.columns.len())
        .map(|i| {
            i == c
                || spec.generated.contains(&spec.columns[i].to_ascii_lowercase())
                || !spec.parent_has.get(i).copied().unwrap_or(true)
        })
        .collect();
    let writable: Vec<usize> = (0..spec.columns.len()).filter(|&i| !skip[i]).collect();
    let self_refs: Vec<usize> = spec.refs.iter().filter(|(_, to)| to == t).map(|(i, _)| *i).collect();
    let mut pending: Vec<Vec<Cell>> = rows;
    let mut recorded: Vec<(String, String)> = Vec::new();
    while !pending.is_empty() {
        let waiting: std::collections::HashSet<String> = pending.iter().filter_map(|r| key_text(&r[c])).collect();
        let (ready, rest): (Vec<Vec<Cell>>, Vec<Vec<Cell>>) = pending.into_iter().partition(|row| {
            self_refs.iter().all(|&i| {
                key_text(&row[i]).is_none_or(|v| v == key_text(&row[c]).unwrap_or_default() || !waiting.contains(&v))
            })
        });
        if ready.is_empty() {
            return Err((
                "55000",
                format!(
                    "rows the branch inserted into {t} refer to each other in a cycle, so they cannot be given new \
                     keys one by one; nothing was merged"
                ),
            ));
        }
        for mut row in ready {
            let old = key_text(&row[c]).unwrap_or_default();
            translate_refs(&mut row, &spec.refs, renumbering.maps);
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
                ident(t),
                writable.iter().map(|&i| ident(&spec.columns[i])).collect::<Vec<_>>().join(", "),
                writable.iter().map(|&i| literal(oids[i], &row[i])).collect::<Vec<_>>().join(", "),
                ident(&spec.columns[c])
            );
            let said = run_on_parent(parent, &sql).await?;
            let new = said
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(key_text)
                .ok_or_else(|| ("XX000", format!("inserting into {t} returned no key")))?;
            renumbering.maps.entry(t.to_string()).or_default().fresh.insert(old.clone(), new.clone());
            recorded.push((old, new));
        }
        pending = rest;
    }
    if !renumbering.table_ready {
        run_on_parent(
            parent,
            &format!(
                "CREATE TABLE IF NOT EXISTS {KEYMAP_TABLE} (branch TEXT NOT NULL, tbl TEXT NOT NULL, old_key TEXT NOT \
                 NULL, new_key TEXT NOT NULL, PRIMARY KEY (branch, tbl, old_key))"
            ),
        )
        .await?;
        renumbering.table_ready = true;
    }
    let quote = |v: &str| format!("'{}'", v.replace('\'', "''"));
    for chunk in recorded.chunks(INSERT_BATCH) {
        let values = chunk
            .iter()
            .map(|(o, n)| format!("({}, {}, {}, {})", quote(renumbering.branch_id), quote(t), quote(o), quote(n)))
            .collect::<Vec<_>>()
            .join(", ");
        run_on_parent(parent, &format!("INSERT INTO {KEYMAP_TABLE} (branch, tbl, old_key, new_key) VALUES {values}")).await?;
    }
    Ok(())
}

/// Removes what merges of the branch `branch_id` recorded in `parent`'s
/// [`KEYMAP_TABLE`]: the branch is gone, and nothing reads them again.
/// Best effort — a row left behind is never read (no branch has its id).
pub(super) async fn forget_keys(parent: Arc<Database>, branch_id: &str) {
    if parent.relation(KEYMAP_TABLE).is_none() {
        return;
    }
    let mut session = Internal::new(parent);
    let _ = session
        .run(&format!(
            "DELETE FROM {KEYMAP_TABLE} WHERE branch = '{}'",
            branch_id.replace('\'', "''")
        ))
        .await;
}

/// The keys earlier merges of the branch `branch_id` renumbered, from the
/// parent's [`KEYMAP_TABLE`].
async fn load_keymaps(parent: &mut Internal, parent_db: &Database, branch_id: &str) -> Result<KeyMaps, String> {
    let mut maps = KeyMaps::new();
    if parent_db.relation(KEYMAP_TABLE).is_none() {
        return Ok(maps);
    }
    let said = parent
        .run(&format!(
            "SELECT tbl, old_key, new_key FROM {KEYMAP_TABLE} WHERE branch = '{}'",
            branch_id.replace('\'', "''")
        ))
        .await?;
    for row in &said.rows {
        let at = |i: usize| row.get(i).cloned().flatten().map(|b| String::from_utf8_lossy(&b).into_owned());
        if let (Some(t), Some(o), Some(n)) = (at(0), at(1), at(2)) {
            let map = maps.entry(t).or_default();
            map.prior_new.insert(n.clone());
            map.prior.insert(o, n);
        }
    }
    Ok(maps)
}

/// Diffs the branch `name` against its parent and base; with `apply`, also
/// merges it.
pub(super) async fn run(
    catalog: &Arc<DatabaseCatalog>,
    name: &str,
    into: Option<&str>,
    apply: Option<(ConflictPolicy, bool)>,
) -> Result<Vec<DiffRow>, Failure> {
    let meta = catalog
        .branch_meta(name)
        .ok_or_else(|| ("42704", format!("\"{name}\" is not a branch")))?;
    let parent_name = into.unwrap_or(&meta.parent).to_string();
    if parent_name != meta.parent {
        return Err((
            "0A000",
            format!(
                "branch \"{name}\" was made from \"{}\"; it can only be compared with or merged into that \
                 database",
                meta.parent
            ),
        ));
    }
    let parent_db = catalog.get(&parent_name).ok_or_else(|| {
        (
            "3D000",
            format!("the parent \"{parent_name}\" of branch \"{name}\" no longer exists"),
        )
    })?;
    let base_db = catalog.open_base(name).await.map_err(|e| ("55000", e))?;

    // The branch side: a merge reads a copy that becomes the next base; a
    // diff reads the branch as it is.
    let (next, branch_db) = match apply {
        Some(_) => {
            let (generation, db) = catalog.next_base(name).await.map_err(|e| ("58000", e))?;
            (Some(generation), db)
        }
        None => (
            None,
            catalog
                .get(name)
                .ok_or_else(|| ("3D000", format!("branch \"{name}\" does not exist")))?,
        ),
    };
    let ddl = catalog.branch_ddl(name);

    let outcome = merge_with(&base_db, &branch_db, &parent_db, &ddl, apply, &meta.merge_id(name)).await;
    match (outcome, next) {
        (Ok(rows), Some(generation)) => {
            if let Err(e) = catalog.adopt_base(name, generation, branch_db, ddl.len()) {
                crate::core::notice::push(
                    "WARNING",
                    "01000",
                    format!(
                        "the merge committed, but the branch's base could not be moved forward ({e}); a \
                         later merge of \"{name}\" would offer the same changes again"
                    ),
                );
            }
            Ok(rows)
        }
        (Ok(rows), None) => Ok(rows),
        (Err(e), Some(generation)) => {
            drop(branch_db);
            catalog.discard_base(name, generation);
            Err(e)
        }
        (Err(e), None) => Err(e),
    }
}

async fn merge_with(
    base_db: &Arc<Database>,
    branch_db: &Arc<Database>,
    parent_db: &Arc<Database>,
    ddl: &[String],
    apply: Option<(ConflictPolicy, bool)>,
    branch_id: &str,
) -> Result<Vec<DiffRow>, Failure> {
    let internal = |e: String| ("XX000", e);
    let mut base = Internal::new(base_db.clone());
    let mut branch = Internal::new(branch_db.clone());
    let mut parent = Internal::new(parent_db.clone());

    let base_tables = user_tables(base_db);
    let branch_tables = user_tables(branch_db);
    let parent_tables = user_tables(parent_db);
    let base_shapes = shapes(&mut base, &base_tables).await.map_err(internal)?;
    let branch_shapes = shapes(&mut branch, &branch_tables)
        .await
        .map_err(internal)?;
    let parent_shapes = shapes(&mut parent, &parent_tables)
        .await
        .map_err(internal)?;

    let mut rows: Vec<DiffRow> = Vec::new();
    let mut schema_conflicts: Vec<String> = Vec::new();
    let mut destructive: Vec<String> = Vec::new();
    // Tables whose shape the branch changed, and which the parent does not
    // already have.
    let mut needs_ddl: BTreeSet<String> = BTreeSet::new();
    let mut already: BTreeSet<String> = BTreeSet::new();

    let all: BTreeSet<String> = base_tables
        .iter()
        .chain(branch_tables.iter())
        .cloned()
        .collect();
    for table in &all {
        let (s, b, p) = (
            base_shapes.get(table),
            branch_shapes.get(table),
            parent_shapes.get(table),
        );
        if s == b {
            continue;
        }
        let detail = match (s, b) {
            (None, Some(b)) => format!("created ({} columns)", b.columns.len()),
            (Some(_), None) => {
                destructive.push(format!("table {table} dropped"));
                "dropped".to_string()
            }
            (Some(s), Some(b)) => {
                let (sn, bn) = (s.column_names(), b.column_names());
                let added: Vec<&String> = bn.iter().filter(|c| !sn.contains(c)).collect();
                let dropped: Vec<&String> = sn.iter().filter(|c| !bn.contains(c)).collect();
                let retyped: Vec<&String> = b
                    .columns
                    .iter()
                    .filter(|(n, t)| s.columns.iter().any(|(sn, st)| sn == n && st != t))
                    .map(|(n, _)| n)
                    .collect();
                for c in &dropped {
                    destructive.push(format!("column {table}.{c} dropped"));
                }
                let mut parts = Vec::new();
                let list =
                    |v: &[&String]| v.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
                if !added.is_empty() {
                    parts.push(format!("columns added: {}", list(&added)));
                }
                if !dropped.is_empty() {
                    parts.push(format!("columns dropped: {}", list(&dropped)));
                }
                if !retyped.is_empty() {
                    parts.push(format!("columns retyped: {}", list(&retyped)));
                }
                if s.indexes != b.indexes {
                    parts.push("indexes changed".to_string());
                }
                parts.join("; ")
            }
            (None, None) => continue,
        };
        let conflict = p != s && p != b;
        if p == b {
            already.insert(table.clone());
        } else if conflict {
            schema_conflicts.push(format!(
                "{table}: the parent changed this table's schema too"
            ));
        } else {
            needs_ddl.insert(table.clone());
        }
        rows.push(DiffRow {
            table: table.clone(),
            change: "schema",
            inserts: 0,
            updates: 0,
            deletes: 0,
            conflicts: u64::from(conflict),
            detail: if p == b {
                format!("{detail} (the parent already has it)")
            } else {
                detail
            },
        });
    }

    if !needs_ddl.is_empty() && ddl.is_empty() {
        schema_conflicts.push(format!(
            "the schema of {} changed on the branch, but no record of the statements that changed it was \
             kept (changed by a path that does not record them, or before this server recorded them); apply \
             those changes to the parent yourself, then merge",
            needs_ddl.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !needs_ddl.is_empty() && !already.is_empty() {
        schema_conflicts.push(format!(
            "the parent already has part of the branch's schema changes ({}) but not the rest ({}); the \
             recorded statements cannot be replayed over half of them",
            already.iter().cloned().collect::<Vec<_>>().join(", "),
            needs_ddl.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }

    let apply_ddl = !needs_ddl.is_empty();
    if let Some((_, allow_drop)) = apply {
        if !schema_conflicts.is_empty() {
            return Err((
                "55000",
                format!("cannot merge: {}", schema_conflicts.join("; ")),
            ));
        }
        if apply_ddl && !destructive.is_empty() && !allow_drop {
            return Err((
                "2BP01",
                format!(
                    "the merge would drop data on the parent ({}); a column renamed on the branch also \
                     reads as a drop. Add ALLOW DROP to merge anyway",
                    destructive.join(", ")
                ),
            ));
        }
        if apply_ddl {
            for stmt in ddl {
                // An index left behind by a merge that then rolled back is
                // harmless — the branch has it too — so it is the one kind
                // allowed to run outside the merge's atomicity.
                let upper = stmt.trim_start().to_ascii_uppercase();
                let index =
                    upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE UNIQUE INDEX");
                if crate::core::database::catalog::branch::is_recorded_ddl(stmt)
                    && !parent_db.block_ddl_covers(stmt)
                    && !index
                {
                    return Err((
                        "0A000",
                        format!(
                            "the branch's schema change `{}` cannot be undone by a rollback here, so a merge \
                             that failed after it would leave it applied; run it on the parent yourself, then \
                             merge",
                            stmt.chars().take(120).collect::<String>()
                        ),
                    ));
                }
            }
        }
        parent.run(SNAPSHOT_BLOCK).await.map_err(internal)?;
        if apply_ddl {
            for stmt in ddl {
                if let Err(e) = parent.run(stmt).await {
                    parent.rollback().await;
                    return Err((
                        "55000",
                        format!(
                            "the branch's schema change `{}` failed on the parent: {e}; nothing was merged",
                            stmt.chars().take(120).collect::<String>()
                        ),
                    ));
                }
            }
            // Every table the branch changed must now look as it does there.
            let now_tables = user_tables(parent_db);
            let now = match shapes(&mut parent, &now_tables).await {
                Ok(s) => s,
                Err(e) => {
                    parent.rollback().await;
                    return Err(internal(e));
                }
            };
            let differs: Vec<String> = needs_ddl
                .iter()
                .filter(|t| now.get(*t) != branch_shapes.get(*t))
                .cloned()
                .collect();
            if !differs.is_empty() {
                parent.rollback().await;
                return Err((
                    "55000",
                    format!(
                        "after replaying the branch's schema changes, {} still differ from the branch; nothing \
                         was merged",
                        differs.join(", ")
                    ),
                ));
            }
        }
    }

    // ---- rows ----
    let policy = apply.map(|(p, _)| p).unwrap_or(ConflictPolicy::Fail);
    let parent_now = if apply.is_some() {
        user_tables(parent_db)
    } else {
        parent_tables.clone()
    };
    // Each table as the parent has it now: as the branch has it once its
    // DDL replayed (checked above), else as it was.
    let parent_shape = |t: &String| -> Option<&Shape> {
        if apply_ddl && needs_ddl.contains(t) {
            branch_shapes.get(t)
        } else {
            parent_shapes.get(t)
        }
    };
    let mut specs: Vec<TableSpec> = Vec::new();
    let mut references: HashMap<String, BTreeSet<String>> = HashMap::new();
    // Tables keyed on one sequence-filled column, by that column's name.
    let serials: HashMap<String, String> = branch_tables
        .iter()
        .filter_map(|t| {
            let shape = branch_shapes.get(t)?;
            let columns = shape.column_names();
            let key = key_of(branch_db, t, &columns)?;
            let [c] = key.as_slice() else { return None };
            sequence_filled(branch_db, t, &columns[*c]).then(|| (t.clone(), columns[*c].clone()))
        })
        .collect();
    for table in &branch_tables {
        let Some(shape) = branch_shapes.get(table) else {
            continue;
        };
        if schema_conflicts
            .iter()
            .any(|c| c.starts_with(&format!("{table}:")))
        {
            continue;
        }
        let columns = shape.column_names();
        let in_base = base_tables.contains(table);
        let in_parent = parent_now.contains(table);
        // Paged on the key rows are matched by, when none of its columns
        // can be NULL (a NULL has no place in a range) and every side read
        // has them.
        let key = key_of(branch_db, table, &columns);
        let serial = serials
            .get(table)
            .and_then(|c| columns.iter().position(|n| n == c));
        let refs: Vec<(usize, String)> = branch_db
            .relation(table)
            .map(|r| r.schema().foreign_keys.clone())
            .unwrap_or_default()
            .iter()
            .filter(|f| !f.is_deleted)
            .filter(|f| {
                serials
                    .get(&f.parent_table)
                    .is_some_and(|c| c.eq_ignore_ascii_case(&f.parent_column))
            })
            .filter_map(|f| {
                columns
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(&f.column))
                    .map(|i| (i, f.parent_table.clone()))
            })
            .collect();
        // Read whole: a table whose rows reference each other by renumbered
        // keys (inserted in an order the references allow), and one whose
        // key holds such a reference (rewritten, it is no longer the key
        // the ranges are in).
        let whole = (serial.is_some() && refs.iter().any(|(_, to)| to == table))
            || key
                .as_ref()
                .is_some_and(|k| refs.iter().any(|(i, _)| k.contains(i)));
        let stream = key.clone().filter(|_| !whole).filter(|k| k.iter().all(|&i| shape.not_null.contains(&columns[i]))).filter(|pk| {
            let has = |shape: Option<&Shape>| {
                shape.is_some_and(|s| {
                    pk.iter()
                        .all(|&i| s.columns.iter().any(|(n, _)| n == &columns[i]))
                })
            };
            (!in_base || has(base_shapes.get(table))) && (!in_parent || has(parent_shape(table)))
        });
        references.insert(table.clone(), references_of(branch_db, table));
        specs.push(TableSpec {
            table: table.clone(),
            key,
            stream,
            generated: generated_of(branch_db, table),
            defaulted: defaulted_of(branch_db, table),
            parent_has: columns
                .iter()
                .map(|c| !in_parent || parent_shape(table).is_none_or(|s| s.columns.iter().any(|(n, _)| n == c)))
                .collect(),
            columns,
            in_base,
            in_parent,
            serial,
            refs,
        });
    }
    let names: Vec<String> = specs.iter().map(|s| s.table.clone()).collect();
    let order = parents_first(&names, &references);
    let by_name: HashMap<&String, &TableSpec> = specs.iter().map(|s| (&s.table, s)).collect();

    // A diff reads the live parent and branch, each at one point.
    if apply.is_none() {
        for session in [&mut parent, &mut branch] {
            session.run(SNAPSHOT_BLOCK).await.map_err(internal)?;
        }
    }
    let mut maps = match load_keymaps(&mut parent, parent_db, branch_id).await {
        Ok(m) => m,
        Err(e) => {
            parent.rollback().await;
            branch.rollback().await;
            return Err(internal(e));
        }
    };
    let mut renumbering = Renumbering {
        maps: &mut maps,
        branch_id,
        table_ready: parent_db.relation(KEYMAP_TABLE).is_some(),
    };
    let mut sides = Sides {
        base: &mut base,
        branch: &mut branch,
        parent: &mut parent,
    };
    // First pass, children first: every table compared whole, conflicts
    // counted, and on a merge the deletes run. Then, parents first, the
    // updates and inserts.
    let mut totals: HashMap<String, TableTotals> = HashMap::new();
    let mut failed: Option<Failure> = None;
    let first = if apply.is_some() { Pass::Deletes } else { Pass::Count };
    for t in order.iter().rev() {
        let mut total = TableTotals::default();
        if let Err(e) = run_table(&mut sides, by_name[t], policy, first, &mut total, &mut renumbering).await {
            failed = Some(e);
            break;
        }
        totals.insert(t.clone(), total);
    }
    let conflicted: u64 = totals.values().map(|t| t.conflicts).sum::<u64>() + schema_conflicts.len() as u64;
    if failed.is_none() && apply.is_some() && !(policy == ConflictPolicy::Fail && conflicted > 0) {
        for t in &order {
            let mut ignored = TableTotals::default();
            if let Err(e) = run_table(&mut sides, by_name[t], policy, Pass::Writes, &mut ignored, &mut renumbering).await {
                failed = Some(e);
                break;
            }
        }
    }
    if let Some(e) = failed {
        parent.rollback().await;
        branch.rollback().await;
        return Err(e);
    }

    let mut conflicts: Vec<String> = schema_conflicts.clone();
    for t in &order {
        let total = &totals[t];
        if total.inserts + total.updates + total.deletes == 0
            && total.conflicts == 0
            && total.left_out.is_empty()
        {
            continue;
        }
        conflicts.extend(total.shown.iter().map(|c| format!("{t}: {c}")));
        rows.push(DiffRow {
            table: t.clone(),
            change: "data",
            inserts: total.inserts,
            updates: total.updates,
            deletes: total.deletes,
            conflicts: total.conflicts,
            detail: match (total.shown.first(), total.left_out.is_empty()) {
                (Some(c), _) => c.clone(),
                (None, false) => format!(
                    "the parent has no column {}; the branch's values in it are left out",
                    total.left_out.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
                (None, true) => String::new(),
            },
        });
    }

    if apply.is_some() {
        if policy == ConflictPolicy::Fail && conflicted > 0 {
            parent.rollback().await;
            let shown: Vec<String> = conflicts.iter().take(CONFLICTS_SHOWN).cloned().collect();
            let more = (conflicted as usize).saturating_sub(shown.len());
            return Err((
                "40001",
                format!(
                    "merge conflict: {} change(s) were made differently on both sides, and nothing was merged: \
                     {}{}. SHOW BRANCH DIFF lists them; ON CONFLICT BRANCH WINS or PARENT WINS resolves them",
                    conflicted,
                    shown.join("; "),
                    if more > 0 {
                        format!("; and {more} more")
                    } else {
                        String::new()
                    }
                ),
            ));
        }
        if let Err(e) = parent.run("COMMIT").await {
            parent.rollback().await;
            return Err(("55000", format!("the merge could not commit: {e}")));
        }
    } else {
        parent.rollback().await;
        branch.rollback().await;
    }
    rows.sort_by(|a, b| a.table.cmp(&b.table).then(b.change.cmp(a.change)));
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(s: &str) -> Cell {
        Some(s.as_bytes().to_vec())
    }

    fn rows(v: &[&[&str]]) -> Projected {
        Projected {
            rows: v
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|c| if *c == "∅" { None } else { cell(c) })
                        .collect()
                })
                .collect(),
            has: vec![true; v.first().map_or(2, |r| r.len())],
        }
    }

    fn plan(
        base: &[&[&str]],
        branch: &[&[&str]],
        parent: &[&[&str]],
        key: bool,
        policy: ConflictPolicy,
    ) -> TablePlan {
        let columns = vec!["id".to_string(), "v".to_string()];
        let oids = vec![23, 25];
        let generated = BTreeSet::new();
        plan_table(TableInput {
            renumber: false,
            table: "t",
            columns: &columns,
            oids: &oids,
            key: key.then(|| vec![0]),
            generated: &generated,
            defaulted: &generated,
            base: Some(rows(base)),
            branch: rows(branch),
            parent: Some(rows(parent)),
            policy,
        })
    }

    #[test]
    fn a_change_only_the_branch_made_is_applied() {
        let p = plan(
            &[&["1", "a"], &["2", "b"], &["3", "c"]],
            &[&["1", "A"], &["3", "c"], &["4", "d"]],
            &[
                &["1", "a"],
                &["2", "b"],
                &["3", "c"],
                &["9", "parent's own"],
            ],
            true,
            ConflictPolicy::Fail,
        );
        assert_eq!((p.inserts, p.updates, p.deletes), (1, 1, 1));
        assert!(p.conflicts.is_empty());
        assert!(
            p.writes
                .iter()
                .any(|s| s == "UPDATE \"t\" SET \"v\" = 'A' WHERE \"id\" = 1"),
            "{:?}",
            p.writes
        );
        assert!(
            p.writes
                .iter()
                .any(|s| s == "INSERT INTO \"t\" (\"id\", \"v\") VALUES (4, 'd')"),
            "{:?}",
            p.writes
        );
        assert_eq!(
            p.deletes_sql,
            vec!["DELETE FROM \"t\" WHERE \"id\" = 2".to_string()]
        );
    }

    #[test]
    fn the_same_change_on_both_sides_is_not_a_conflict() {
        let p = plan(
            &[&["1", "a"]],
            &[&["1", "z"]],
            &[&["1", "z"]],
            true,
            ConflictPolicy::Fail,
        );
        assert_eq!(
            (p.inserts, p.updates, p.deletes, p.conflicts.len()),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn different_changes_conflict_and_the_policy_decides() {
        for (policy, writes) in [
            (ConflictPolicy::Fail, 0),
            (ConflictPolicy::ParentWins, 0),
            (ConflictPolicy::BranchWins, 1),
        ] {
            let p = plan(&[&["1", "a"]], &[&["1", "b"]], &[&["1", "c"]], true, policy);
            assert_eq!(p.conflicts.len(), 1, "{policy:?}");
            assert!(p.conflicts[0].contains("id=1"), "{:?}", p.conflicts);
            assert_eq!(p.writes.len(), writes, "{policy:?}");
        }
        // Deleted on one side, changed on the other.
        let p = plan(
            &[&["1", "a"]],
            &[],
            &[&["1", "c"]],
            true,
            ConflictPolicy::Fail,
        );
        assert_eq!(p.conflicts.len(), 1);
        let p = plan(
            &[&["1", "a"]],
            &[&["1", "b"]],
            &[],
            true,
            ConflictPolicy::Fail,
        );
        assert_eq!(p.conflicts.len(), 1);
        // Inserted on both with different values.
        let p = plan(
            &[],
            &[&["5", "x"]],
            &[&["5", "y"]],
            true,
            ConflictPolicy::Fail,
        );
        assert_eq!(p.conflicts.len(), 1);
    }

    #[test]
    fn rows_with_no_key_merge_as_a_multiset() {
        let p = plan(
            &[&["1", "a"], &["1", "a"], &["2", "b"]],
            &[&["1", "a"], &["2", "b"], &["2", "b"], &["3", "c"]],
            &[&["1", "a"], &["1", "a"], &["2", "b"], &["7", "parent"]],
            false,
            ConflictPolicy::Fail,
        );
        assert!(p.conflicts.is_empty(), "{:?}", p.conflicts);
        // One (1,a) removed: both go, one is written back.
        assert_eq!(p.deletes, 1);
        assert_eq!(p.inserts, 2);
        assert!(p.deletes_sql[0].starts_with("DELETE FROM \"t\" WHERE \"id\" = 1 AND \"v\" = 'a'"));
    }

    #[test]
    fn a_null_in_a_key_falls_back_to_whole_rows() {
        let p = plan(
            &[&["∅", "a"], &["∅", "b"]],
            &[&["∅", "a"]],
            &[&["∅", "a"], &["∅", "b"]],
            true,
            ConflictPolicy::Fail,
        );
        assert!(p.conflicts.is_empty());
        assert_eq!(p.deletes, 1);
        assert!(
            p.deletes_sql[0].contains("\"id\" IS NULL"),
            "{:?}",
            p.deletes_sql
        );
    }

    #[test]
    fn referenced_tables_come_first() {
        let mut refs = HashMap::new();
        refs.insert(
            "orders".to_string(),
            BTreeSet::from(["customers".to_string()]),
        );
        refs.insert("lines".to_string(), BTreeSet::from(["orders".to_string()]));
        let order = parents_first(
            &["lines".into(), "orders".into(), "customers".into()],
            &refs,
        );
        assert_eq!(order, vec!["customers", "orders", "lines"]);
    }
}
