//! PostgreSQL catalogue relations, built only when a statement names one.
//!
//! [`crate::core::introspect`] explains why the catalogue is not stored as
//! tables here: the schema lives in a per-table sidecar, the sizes live in the
//! filesystem, and materialising all three into rows costs the whole price of a
//! catalogue to answer questions that are a field read each.
//!
//! That argument is against keeping them. It is not an argument against
//! *producing* them when someone asks: an application written for PostgreSQL
//! reads `pg_stat_user_tables` rather than `SHOW TABLES`, and rewriting the
//! application is not always the caller's option. So a statement naming one of
//! these gets a temporary table built from the same field reads `SHOW` would
//! have made, and nothing is built for a statement that names none.
//!
//! The rows are a snapshot taken when the statement runs, which is what
//! PostgreSQL's statistics views are too.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;
use crate::core::rql::cte::Materialized;
use smallvec::SmallVec;

/// The catalogue relations this module can produce.
///
/// A slice rather than an array because `pg_stat_statements` is only one of
/// them in a build that records what runs. Without the feature the name is not
/// claimed at all, so a statement selecting from it fails the way PostgreSQL
/// fails when the extension is not installed — rather than returning an empty
/// relation, which a monitor would read as "this server has run nothing".
const VIEWS: &[&str] = &[
    #[cfg(feature = "query_stats")]
    "pg_stat_statements",
    "pg_stat_user_tables",
    "pg_class",
    "pg_namespace",
    "pg_inherits",
    "pg_replication_slots",
    "pg_index",
    "pg_indexes",
    "pg_database",
    "columns",
    "tables",
];

/// The columns each view is built with, in order.
///
/// The extended query protocol asks what a statement returns *before* running
/// it, and a catalogue view does not exist until it is materialised — so the
/// describe path cannot look its columns up the way it does for a real
/// relation. Answering from here lets it describe one without building it,
/// which matters because materialising at describe time would give `Describe`
/// side effects.
///
/// Without this every catalogue view answered `Describe` with `NoData` and then
/// sent `DataRow`s from `Execute` anyway. Drivers read that as a protocol
/// violation; sqlx surfaced it as rows whose columns could not be read by name,
/// so `SELECT * FROM information_schema.columns` returned the right values under
/// no names at all and any schema introspection failed.
///
/// `catalogue_columns_match_what_is_built` holds this list to what `build`
/// actually creates, so the two cannot drift.
pub fn columns_of(view: &str) -> Option<&'static [(&'static str, DbType)]> {
    #[cfg(feature = "query_stats")]
    use DbType::F64;
    use DbType::{BOOL, I64, STRING};
    Some(match view {
        // `pg_stat_statements`, as its extension defines it, minus the columns
        // this engine has no counterpart for. The block counters are the ones
        // dashboards select unconditionally, and are reported as zero rather
        // than omitted so that selecting them parses; `rows_scanned` and
        // `materialised` are this engine's additions, and are the two columns
        // that say *why* a statement is slow — one for scan-bound work and one
        // for the relations a CTE or derived table has to build first. Kept
        // apart rather than summed: they are different kinds of number, and a
        // statement can be expensive for either reason or both. See
        // `core::query_stats`.
        #[cfg(feature = "query_stats")]
        "pg_stat_statements" => &[
            ("userid", I64),
            ("dbid", I64),
            ("toplevel", BOOL),
            ("queryid", I64),
            ("query", STRING),
            ("calls", I64),
            ("total_exec_time", F64),
            ("min_exec_time", F64),
            ("max_exec_time", F64),
            ("mean_exec_time", F64),
            ("stddev_exec_time", F64),
            ("rows", I64),
            ("shared_blks_hit", I64),
            ("shared_blks_read", I64),
            ("rows_scanned", I64),
            ("materialised", I64),
            ("materialised_ms", F64),
        ],
        "pg_namespace" => &[("oid", I64), ("nspname", STRING)],
        "pg_class" => &[
            ("oid", I64),
            ("relname", STRING),
            ("relnamespace", I64),
            ("relkind", STRING),
            ("relsize", I64),
            ("reltuples", I64),
        ],
        "columns" => &[
            ("table_catalog", STRING),
            ("table_schema", STRING),
            ("table_name", STRING),
            ("column_name", STRING),
            ("ordinal_position", I64),
            ("data_type", STRING),
            ("is_nullable", STRING),
        ],
        "pg_stat_user_tables" => &[
            ("relid", I64),
            ("schemaname", STRING),
            ("relname", STRING),
            ("n_live_tup", I64),
            ("n_dead_tup", I64),
        ],
        "pg_replication_slots" => &[
            ("slot_name", STRING),
            ("plugin", STRING),
            ("slot_type", STRING),
            ("datoid", I64),
            ("database", STRING),
            ("temporary", BOOL),
            ("active", BOOL),
            ("active_pid", I64),
            ("xmin", I64),
            ("catalog_xmin", I64),
            ("restart_lsn", I64),
            ("confirmed_flush_lsn", I64),
        ],
        "pg_index" => &[
            ("indexrelid", I64),
            ("indrelid", I64),
            ("indnatts", I64),
            ("indisunique", BOOL),
            ("indisprimary", BOOL),
            ("indisvalid", BOOL),
            ("indisready", BOOL),
            ("indislive", BOOL),
            ("indpred", STRING),
            ("indexprs", STRING),
        ],
        "pg_indexes" => &[
            ("schemaname", STRING),
            ("tablename", STRING),
            ("indexname", STRING),
            ("tablespace", STRING),
            ("indexdef", STRING),
        ],
        "pg_database" => &[
            ("oid", I64),
            ("datname", STRING),
            ("datdba", I64),
            ("encoding", I64),
            ("datcollate", STRING),
            ("datctype", STRING),
            ("datistemplate", BOOL),
            ("datallowconn", BOOL),
            ("datconnlimit", I64),
        ],
        "tables" => &[
            ("table_catalog", STRING),
            ("table_schema", STRING),
            ("table_name", STRING),
            ("table_type", STRING),
        ],
        "pg_inherits" => &[("inhrelid", I64), ("inhparent", I64), ("inhseqno", I64)],
        _ => return None,
    })
}

/// The catalogue view a statement reads, if it reads one.
///
/// The same test [`materialize`] applies, so a statement that merely contains
/// the word does not match.
pub fn view_of_statement(sql: &str) -> Option<&'static str> {
    if !may_name_a_view(sql) {
        return None;
    }
    VIEWS.iter().copied().find(|view| names_relation(sql, view))
}

/// Whether `sql` could name any of [`VIEWS`]: each is `pg_…`, or is matched
/// only under an `information_schema.` or `pg_catalog.` qualifier
/// ([`needs_qualifier`]), so a statement holding neither `pg_` nor
/// `information_schema.`, in any case, names none. One pass, where
/// [`names_relation`] lowercases the whole statement once per view.
fn may_name_a_view(sql: &str) -> bool {
    let b = sql.as_bytes();
    let at = |word: &[u8]| {
        memchr::memchr2_iter(word[0], word[0].to_ascii_uppercase(), b)
            .any(|i| b.len() - i >= word.len() && b[i..i + word.len()].eq_ignore_ascii_case(word))
    };
    at(b"pg_") || at(b"information_schema.")
}

/// A schema equivalent to the one the materialised view will have, for
/// describing a statement without building it.
pub fn synthetic_schema(view: &str) -> Option<SmallVec<[SchemaField; 20]>> {
    let columns = columns_of(view)?;
    let mut fields: SmallVec<[SchemaField; 20]> = SmallVec::new();
    let mut offset = 0u64;
    for (order, (name, ty)) in columns.iter().enumerate() {
        let size = ty.get_size();
        fields.push(SchemaField::new(
            name.to_string(),
            ty.clone(),
            size,
            offset,
            order as u64,
            false,
        ));
        offset += size as u64;
    }
    Some(fields)
}

/// The one schema this engine has.
const SCHEMA: &str = "public";

/// Catalogue relations that are only recognised when they are qualified.
///
/// `columns` is `information_schema.columns`, and the bare word is far too
/// common to claim: it is a keyword in `SHOW COLUMNS FROM t`, and it is a
/// perfectly ordinary table name. Claiming it rewrote the keyword — `SHOW
/// COLUMNS FROM "rdb_roles"` became `SHOW __cat_columns_… FROM "rdb_roles"`,
/// which returned one empty column and left every client that asks a table for
/// its columns believing it had none.
const QUALIFIED_ONLY: [&str; 2] = ["columns", "tables"];

/// Whether `view` may only be matched with its schema in front of it.
fn needs_qualifier(view: &str) -> bool {
    QUALIFIED_ONLY.iter().any(|v| v.eq_ignore_ascii_case(view))
}

/// True when `name` is a catalogue relation, with or without a `pg_catalog.`
/// qualifier.
pub fn is_catalog_view(name: &str) -> bool {
    let trimmed = name.trim().trim_matches('"');
    let qualified = trimmed.contains('.');
    let bare = trimmed
        .rsplit('.')
        .next()
        .unwrap_or(trimmed)
        .trim_matches('"');
    VIEWS
        .iter()
        .any(|v| v.eq_ignore_ascii_case(bare) && (qualified || !needs_qualifier(v)))
}

/// Builds every catalogue relation the statement names and points it at them.
///
/// Returns `None` when the statement names none, which is the ordinary case and
/// costs one scan of the text.
pub async fn materialize(
    sql: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
    // Only `pg_database` needs it, and only a session has one: a `Database`
    // is owned by the catalogue and holds no way back up to it. `None` builds
    // the view with the current database as its only row, which is the truth
    // for a process serving one.
    catalog: Option<&Arc<crate::core::database::catalog::DatabaseCatalog>>,
) -> Result<Option<Materialized>, String> {
    if !may_name_a_view(sql) {
        return Ok(None);
    }
    let mut wanted: Vec<String> = Vec::new();
    for view in VIEWS.iter().copied() {
        if names_relation(sql, view) {
            wanted.push(view.to_string());
        }
    }
    if wanted.is_empty() {
        return Ok(None);
    }
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Catalog, tx, database);

    let mut temps: Vec<String> = Vec::new();
    let mut rewritten = sql.to_string();

    // `pg_total_relation_size(c.oid)` becomes `c.relsize`: the figure is read
    // when `pg_class` is built, where the storage layer is reachable, rather
    // than by a function the row evaluator has no way to answer.
    if wanted.iter().any(|v| v == "pg_class") {
        rewritten = rewrite_size_calls(&rewritten);
    }

    // Same treatment for the WAL position functions: `pg_current_wal_lsn()`
    // becomes the engine's own commit LSN as a literal, and
    // `pg_wal_lsn_diff(a, b)` becomes the subtraction it is. The row evaluator
    // has no way to reach the WAL, and a monitor's `SELECT` should not fail on
    // a function that is one field read away.
    if wanted.iter().any(|v| v == "pg_replication_slots") {
        rewritten = rewrite_wal_calls(&rewritten, database);
    }

    // `pg_get_expr(indpred, indrelid)` renders an index's partial predicate.
    // No index here has one, so the answer is always the empty predicate, and
    // a client filtering on it — `pg_get_expr(...) ILIKE '%<uuid>%'`, looking
    // for the partial index covering one value — correctly matches nothing.
    // Left alone the whole statement failed instead, which reads as "the index
    // could not be checked" rather than "there is no such index".
    if wanted.iter().any(|v| v == "pg_index") {
        rewritten = rewrite_get_expr(&rewritten);
    }

    for view in &wanted {
        match build(view, database, tx, catalog).await {
            Ok(temp) => {
                rewritten = replace_relation(&rewritten, view, &temp);
                temps.push(temp);
            }
            Err(e) => {
                // Whatever was built before the failure is dropped: the caller
                // is handed nothing by an `Err` and cannot clean up.
                crate::core::rql::cte::drop_temps(database, &temps, tx).await;
                return Err(e);
            }
        }
    }

    Ok(Some(Materialized { rewritten, temps }))
}

/// Whether `sql` uses `view` as a relation, rather than merely containing the
/// word.
fn names_relation(sql: &str, view: &str) -> bool {
    if needs_qualifier(view) {
        // Matched only as `information_schema.columns`, never as the bare word.
        return names_relation(sql, &format!("information_schema.{}", view))
            || names_relation(sql, &format!("pg_catalog.{}", view));
    }
    let lower = sql.to_ascii_lowercase();
    let mut from = 0usize;
    while let Some(at) = lower[from..].find(view) {
        let start = from + at;
        let end = start + view.len();
        let before_ok = start == 0 || {
            let b = lower.as_bytes()[start - 1];
            // A `pg_catalog.` qualifier is part of the name, not a different one.
            !(b.is_ascii_alphanumeric() || b == b'_')
        };
        let after_ok = end >= lower.len() || {
            let b = lower.as_bytes()[end];
            !(b.is_ascii_alphanumeric() || b == b'_')
        };
        if before_ok && after_ok {
            return true;
        }
        from = end;
    }
    false
}

/// Points every reference to `view` at `temp`, qualifier included.
///
/// Skips string literals: `WHERE relname = 'pg_class'` compares against the
/// *name* `pg_class`, and rewriting it would silently change what the statement
/// asks for.
fn replace_relation(sql: &str, view: &str, temp: &str) -> String {
    let bytes = sql.as_bytes();
    let lower = sql.to_ascii_lowercase();
    // Both schemas a catalogue relation is written under. The qualifier is
    // part of the name being replaced, not a separate token — leaving it behind
    // produced `information_schema.__cat_columns_…`, which names nothing.
    let qualified: [String; 2] = [
        format!("pg_catalog.{}", view),
        format!("information_schema.{}", view),
    ];
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        // A quoted literal is data; copy it through untouched.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        let matched = qualified
            .iter()
            .find(|q| lower[i..].starts_with(q.as_str()))
            .map(|q| q.len())
            .or_else(|| {
                // A qualifier-only view is never matched bare. See
                // `QUALIFIED_ONLY`.
                (!needs_qualifier(view) && lower[i..].starts_with(view)).then(|| view.len())
            });

        if let Some(len) = matched {
            let end = i + len;
            let before_ok = i == 0 || {
                let b = bytes[i - 1];
                !(b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
            };
            let after_ok = end >= bytes.len() || {
                let b = bytes[end];
                !(b.is_ascii_alphanumeric() || b == b'_')
            };
            if before_ok && after_ok {
                out.push_str(temp);
                i = end;
                continue;
            }
        }

        let ch = sql[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Turns `pg_total_relation_size(x.oid)` into `x.relsize`.
///
/// Only the `<alias>.oid` form is rewritten. Any other argument is left alone
/// and fails as an unsupported function, rather than being answered with a size
/// for a relation nobody named.
fn rewrite_size_calls(sql: &str) -> String {
    let mut out = sql.to_string();
    for func in ["pg_total_relation_size", "pg_relation_size"] {
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(at) = lower.find(func) else { break };
            let after = &out[at + func.len()..];
            let Some(open) = after.find('(') else { break };
            if !after[..open].trim().is_empty() {
                break;
            }
            let Some(close) = after.find(')') else { break };
            let arg = after[open + 1..close].trim();
            let Some(alias) = arg
                .strip_suffix(".oid")
                .or_else(|| arg.strip_suffix(".OID"))
            else {
                break;
            };
            out = format!(
                "{}{}.relsize{}",
                &out[..at],
                alias.trim(),
                &after[close + 1..]
            );
        }
    }
    out
}

/// Replaces the WAL position functions with values the evaluator can read.
///
/// `pg_current_wal_lsn()` becomes the engine's commit LSN — a real, monotonic
/// position, not a placeholder — and `pg_wal_lsn_diff(a, b)` becomes `(a - b)`.
/// Replaces every `pg_get_expr(...)` call with the empty string.
///
/// Empty rather than `NULL`: the row evaluator rejects a `NULL` literal in a
/// `WHERE`, and the two behave the same under the `LIKE`/`ILIKE` these calls
/// are almost always compared with — neither matches a pattern with anything
/// in it. Arguments are skipped by balancing parentheses, so a nested call
/// does not end the span early.
fn rewrite_get_expr(sql: &str) -> String {
    const NAME: &str = "pg_get_expr";
    let mut out = sql.to_string();
    let mut from = 0usize;

    loop {
        let lower = out.to_ascii_lowercase();
        let Some(at) = lower[from..].find(NAME).map(|i| i + from) else {
            break;
        };

        // Only a call, and only a whole word: `my_pg_get_expr` is a column.
        let before_ok = at == 0 || !is_ident_byte(out.as_bytes()[at - 1]);
        let after = &out[at + NAME.len()..];
        let open_rel = after.find('(').filter(|o| after[..*o].trim().is_empty());

        let (Some(open_rel), true) = (open_rel, before_ok) else {
            from = at + NAME.len();
            continue;
        };

        let start = at + NAME.len() + open_rel;
        let Some(end) = matching_paren(&out, start) else {
            from = at + NAME.len();
            continue;
        };

        out = format!("{}''{}", &out[..at], &out[end + 1..]);
        from = at + 2;
    }

    out
}

/// The index of the `)` closing the `(` at `open`, or `None` if unbalanced.
///
/// String literals are skipped so a parenthesis inside one cannot close the
/// call.
fn matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = open;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

/// A byte that can appear inside an identifier.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn rewrite_wal_calls(sql: &str, database: &Arc<Database>) -> String {
    let lsn = database
        .location
        .as_deref()
        .and_then(crate::core::wal::wal_for)
        .map(|w| w.commit_lsn())
        .unwrap_or(0);

    let mut out = sql.to_string();

    // The nullary one first, so the diff below sees a literal where its
    // argument was.
    loop {
        let lower = out.to_ascii_lowercase();
        let Some(at) = lower.find("pg_current_wal_lsn") else {
            break;
        };
        let after = &out[at + "pg_current_wal_lsn".len()..];
        let Some(open) = after.find('(') else { break };
        if !after[..open].trim().is_empty() {
            break;
        }
        let Some(close) = after[open..].find(')') else {
            break;
        };
        out = format!("{}{}{}", &out[..at], lsn, &after[open + close + 1..]);
    }

    loop {
        let lower = out.to_ascii_lowercase();
        let Some(at) = lower.find("pg_wal_lsn_diff") else {
            break;
        };
        let after = &out[at + "pg_wal_lsn_diff".len()..];
        let Some(open) = after.find('(') else { break };
        if !after[..open].trim().is_empty() {
            break;
        }
        let Some(close) = matching_paren_from(after, open) else {
            break;
        };
        let args = &after[open + 1..close];
        let Some(comma) = top_level_comma(args) else {
            break;
        };
        out = format!(
            "{}({} - {}){}",
            &out[..at],
            args[..comma].trim(),
            args[comma + 1..].trim(),
            &after[close + 1..]
        );
    }

    out
}

/// Index of the parenthesis matching the one at `open`, ignoring quoted text.
fn matching_paren_from(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match quote {
            Some(q) => {
                if *b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(*b),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

/// Index of the first comma at nesting depth zero.
fn top_level_comma(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    for (i, b) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if *b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(*b),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' if depth == 0 => return Some(i),
                _ => {}
            },
        }
    }
    None
}

/// Merges single-row `INSERT`s that share a target into multi-row ones.
///
/// Every row of a catalogue relation is generated as its own statement, which
/// keeps the builders simple. Executing them that way does not scale: the cost
/// is per statement, and a relation describing every column of every table has
/// thousands. Statements that agree up to `VALUES` are joined; anything that
/// does not fit the shape is passed through untouched, so a builder that emits
/// something unusual still works.
fn batch_inserts(inserts: &[String], per_statement: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut prefix: Option<&str> = None;
    let mut tuples: Vec<&str> = Vec::new();

    let flush = |out: &mut Vec<String>, prefix: Option<&str>, tuples: &mut Vec<&str>| {
        if let (Some(prefix), false) = (prefix, tuples.is_empty()) {
            out.push(format!("{}{}", prefix, tuples.join(", ")));
        }
        tuples.clear();
    };

    for insert in inserts {
        let split = insert.find(" VALUES ").map(|at| at + " VALUES ".len());
        let Some(at) = split else {
            flush(&mut out, prefix, &mut tuples);
            prefix = None;
            out.push(insert.clone());
            continue;
        };
        let (head, tail) = insert.split_at(at);
        if prefix != Some(head) || tuples.len() >= per_statement {
            flush(&mut out, prefix, &mut tuples);
            prefix = Some(head);
        }
        tuples.push(tail.trim());
    }
    flush(&mut out, prefix, &mut tuples);
    out
}

/// PostgreSQL's `information_schema` spelling of a column's type.
fn sql_type_name(db_type: &crate::core::db_type::DbType) -> &'static str {
    use crate::core::db_type::DbType;
    match db_type {
        DbType::BOOL => "boolean",
        DbType::I8 | DbType::I16 => "smallint",
        DbType::I32 => "integer",
        DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 => "bigint",
        // Wider than `bigint`; advertised as `numeric` on the wire too.
        DbType::U64 | DbType::I128 | DbType::U128 => "numeric",
        DbType::F32 => "real",
        DbType::F64 => "double precision",
        DbType::DATETIME => "timestamp with time zone",
        DbType::UUID => "uuid",
        DbType::INET => "inet",
        DbType::DECIMAL => "numeric",
        DbType::BYTEA => "bytea",
        DbType::ARRAY => "ARRAY",
        DbType::JSON | DbType::JSONB => "jsonb",
        _ => "text",
    }
}

/// A temporary table name for one view, unique to this statement.
fn temp_name(view: &str, database: &Arc<Database>) -> String {
    // The address of the database is stable for its lifetime and different for
    // two of them, so two statements in flight against different databases
    // cannot collide on a name.
    format!(
        "__cat_{}_{:x}",
        view,
        Arc::as_ptr(database) as usize as u64 ^ fastrand::u64(..)
    )
}

/// Builds one catalogue relation as a temporary table and returns its name.
async fn build(
    view: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
    catalog: Option<&Arc<crate::core::database::catalog::DatabaseCatalog>>,
) -> Result<String, String> {
    let temp = temp_name(view, database);

    // Names are collected first so no map guard is held across an await. A
    // partitioned table whose schema could not be read has no shell in
    // `tables`; it is listed too, so a view that needs its data or schema
    // refuses rather than answering without it.
    let tables: Vec<String> = database
        .relation_names()
        .into_iter()
        .filter(|name| !is_internal(name))
        .collect();

    let (create, inserts) = match view {
        "pg_namespace" => (
            format!("CREATE TABLE {} (oid BIGINT, nspname TEXT)", temp),
            vec![format!(
                "INSERT INTO {} (oid, nspname) VALUES ({}, '{}')",
                temp,
                oid_of(SCHEMA),
                SCHEMA
            )],
        ),
        "pg_class" => {
            let mut rows = Vec::with_capacity(tables.len());
            for name in &tables {
                // The size is read here, where the storage layer is reachable.
                // `pg_total_relation_size(c.oid)` is rewritten into a reference
                // to this column, so the figure comes from a `stat` rather than
                // from a function the row evaluator could not answer.
                let (bytes, tuples) = relation_size(database, name).await?;
                rows.push(format!(
                    "INSERT INTO {} (oid, relname, relnamespace, relkind, relsize, reltuples) \
                     VALUES ({}, '{}', {}, 'r', {}, {})",
                    temp,
                    oid_of(name),
                    escape(name),
                    oid_of(SCHEMA),
                    bytes,
                    tuples
                ));
            }
            (
                format!(
                    "CREATE TABLE {} (oid BIGINT, relname TEXT, relnamespace BIGINT, \
                     relkind TEXT, relsize BIGINT, reltuples BIGINT)",
                    temp
                ),
                rows,
            )
        }
        // `information_schema.columns`, which is how an application asks
        // whether a column exists before using it. Named `columns` here because
        // the qualifier is stripped before the lookup.
        "columns" => {
            let mut rows = Vec::new();
            for name in &tables {
                let Some(table) = database.schema_view(name) else {
                    if let Some(Relation::Partitioned(t)) = database.relation(name) {
                        return Err(t.broken().unwrap_or("its schema is unavailable").to_string());
                    }
                    continue;
                };
                let mut fields: Vec<_> = table
                    .schema()
                    .fields
                    .iter()
                    .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                    .cloned()
                    .collect();
                fields.sort_by_key(|f| f.write_order);
                for (position, field) in fields.iter().enumerate() {
                    // A column is nullable exactly when it has a companion
                    // recording that, which is the same test the writer uses.
                    let nullable = table.schema().fields.iter().any(|f| {
                        !f.is_deleted
                            && f.name.eq_ignore_ascii_case(
                                &crate::core::rql::nullable::flag_column_name(&field.name),
                            )
                    });
                    rows.push(format!(
                        "INSERT INTO {} (table_catalog, table_schema, table_name, column_name, \
                         ordinal_position, data_type, is_nullable) \
                         VALUES ('{}', '{}', '{}', '{}', {}, '{}', '{}')",
                        temp,
                        SCHEMA,
                        SCHEMA,
                        escape(name),
                        escape(&field.name),
                        position + 1,
                        // `character`, as PostgreSQL names `character(n)`
                        // (`type_modifier::TextTypmod`); the wire says
                        // `bpchar` for it too.
                        if matches!(field.text_typmod(), Some(crate::core::rql::type_modifier::TextTypmod::Bpchar(_))) {
                            "character"
                        } else {
                            sql_type_name(&field.db_type)
                        },
                        if nullable { "YES" } else { "NO" }
                    ));
                }
            }
            (
                format!(
                    "CREATE TABLE {} (table_catalog TEXT, table_schema TEXT, table_name TEXT, \
                     column_name TEXT, ordinal_position BIGINT, data_type TEXT, \
                     is_nullable TEXT)",
                    temp
                ),
                rows,
            )
        }
        // The four columns schema introspection actually reads —
        // `sea-orm-cli`, Diesel's `print-schema` and JDBC's `getTables` all
        // select them by name. The standard's remaining columns
        // (`self_referencing_column_name`, `is_insertable_into` and the rest)
        // are omitted rather than filled with plausible constants: a tool that
        // needs one should fail on its absence instead of reading an invented
        // answer.
        "tables" => {
            let mut rows = Vec::with_capacity(tables.len());
            for name in &tables {
                rows.push(format!(
                    "INSERT INTO {} (table_catalog, table_schema, table_name, table_type) \
                     VALUES ('{}', '{}', '{}', 'BASE TABLE')",
                    temp,
                    SCHEMA,
                    SCHEMA,
                    escape(name)
                ));
            }
            (
                format!(
                    "CREATE TABLE {} (table_catalog TEXT, table_schema TEXT, \
                     table_name TEXT, table_type TEXT)",
                    temp
                ),
                rows,
            )
        }
        // Times are milliseconds, which is what `pg_stat_statements` reports
        // and what every dashboard built on it divides and sorts by. The
        // engine keeps microseconds, so the conversion happens here rather
        // than in the caller's arithmetic.
        #[cfg(feature = "query_stats")]
        "pg_stat_statements" => {
            let stats = crate::core::query_stats::aggregated();
            let mut rows = Vec::with_capacity(stats.len());
            for agg in &stats {
                rows.push(format!(
                    "INSERT INTO {} (userid, dbid, toplevel, queryid, query, calls, \
                     total_exec_time, min_exec_time, max_exec_time, mean_exec_time, \
                     stddev_exec_time, rows, shared_blks_hit, shared_blks_read, rows_scanned, \
                     materialised, materialised_ms) \
                     VALUES (0, {}, TRUE, {}, '{}', {}, {}, {}, {}, {}, {}, {}, 0, 0, {}, {}, {})",
                    temp,
                    oid_of(&database.name),
                    // The whole 64-bit hash, read as signed — `queryid` is a
                    // `bigint` in PostgreSQL and half of them are negative
                    // there too.
                    agg.queryid as i64,
                    escape(&agg.query),
                    agg.calls,
                    ms(agg.total_us as f64),
                    ms(agg.min_us as f64),
                    ms(agg.max_us as f64),
                    ms(agg.mean_us()),
                    ms(agg.stddev_us()),
                    agg.rows,
                    agg.rows_scanned,
                    agg.materialised,
                    ms(agg.materialised_us as f64)
                ));
            }
            (
                format!(
                    "CREATE TABLE {} (userid BIGINT, dbid BIGINT, toplevel BOOL, \
                     queryid BIGINT, query TEXT, calls BIGINT, \
                     total_exec_time DOUBLE PRECISION, min_exec_time DOUBLE PRECISION, \
                     max_exec_time DOUBLE PRECISION, mean_exec_time DOUBLE PRECISION, \
                     stddev_exec_time DOUBLE PRECISION, rows BIGINT, \
                     shared_blks_hit BIGINT, shared_blks_read BIGINT, rows_scanned BIGINT, \
                     materialised BIGINT, materialised_ms DOUBLE PRECISION)",
                    temp
                ),
                rows,
            )
        }
        "pg_stat_user_tables" => {
            let mut rows = Vec::with_capacity(tables.len());
            for name in &tables {
                let (live, dead) = tuple_counts(database, name).await?;
                rows.push(format!(
                    "INSERT INTO {} (relid, schemaname, relname, n_live_tup, n_dead_tup) \
                     VALUES ({}, '{}', '{}', {}, {})",
                    temp,
                    oid_of(name),
                    SCHEMA,
                    escape(name),
                    live,
                    dead
                ));
            }
            (
                format!(
                    "CREATE TABLE {} (relid BIGINT, schemaname TEXT, relname TEXT, \
                     n_live_tup BIGINT, n_dead_tup BIGINT)",
                    temp
                ),
                rows,
            )
        }
        // Empty, and honestly so: this engine replicates statements between
        // its own nodes and has no PostgreSQL replication slots to report.
        // A monitor asking after one gets "there is no such slot" instead of
        // "relation does not exist", which is the true answer to its question.
        // The columns are PostgreSQL's, so a query that selects any of them
        // parses.
        "pg_replication_slots" => (
            format!(
                "CREATE TABLE {} (slot_name TEXT, plugin TEXT, slot_type TEXT, \
                 datoid BIGINT, database TEXT, temporary BOOL, active BOOL, \
                 active_pid BIGINT, xmin BIGINT, catalog_xmin BIGINT, \
                 restart_lsn BIGINT, confirmed_flush_lsn BIGINT)",
                temp
            ),
            Vec::new(),
        ),
        // One row per uniqueness constraint. This engine maintains its own
        // access paths and has no user-visible indexes, so the only entries are
        // the ones a client can reason about: what is unique, and what is the
        // key.
        "pg_index" => {
            let mut rows = Vec::new();
            for name in &tables {
                let Some(relation) = database.relation(name) else {
                    continue;
                };
                let schema = relation.schema();
                let relid = oid_of(name);

                let mut keys: Vec<(String, bool, bool)> = Vec::new();
                if let Some(primary) = schema.primary_key.as_deref() {
                    keys.push((primary.to_string(), true, true));
                }
                for field in schema.fields.iter() {
                    if field.is_unique
                        && !field.is_deleted
                        && !keys
                            .iter()
                            .any(|(n, _, _)| n.eq_ignore_ascii_case(&field.name))
                    {
                        keys.push((field.name.clone(), true, false));
                    }
                }
                // A partial key is unique only under its predicate, and this
                // view lists each column as unique outright, so it is left out
                // rather than reported as a guarantee it does not give.
                for key in schema.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_none()) {
                    if key.columns.len() == 1
                        && !keys
                            .iter()
                            .any(|(n, _, _)| n.eq_ignore_ascii_case(&key.columns[0]))
                    {
                        keys.push((key.columns[0].clone(), true, false));
                    }
                }

                // Indexes `CREATE INDEX` declared, each with its own
                // identity and its column count. Never "no indexes" for a
                // table whose indexes cannot be read: a migration tool would
                // re-issue every CREATE INDEX.
                if let Some((owned, ready)) = index_set(&relation)? {
                    let indexes = owned.view();
                    let mut declared: Vec<(String, usize)> = indexes
                        .list()
                        .iter()
                        .filter(|i| !i.implicit)
                        .map(|i| (i.decl.name.clone(), i.columns.len()))
                        .chain(indexes.gin().iter().map(|g| (g.decl.name.clone(), 1)))
                        .collect();
                    // A `CREATE INDEX` sharing another's columns shares its
                    // index; listed by its own name all the same.
                    for (index_name, unique, columns, _) in shared_declarations(&schema) {
                        if !unique
                            && !declared.iter().any(|(n, _)| n.eq_ignore_ascii_case(&index_name))
                            && indexes.list().iter().any(|i| i.path.is_none() && i.decl.columns == columns)
                        {
                            declared.push((index_name, columns.len()));
                        }
                    }
                    for (index_name, columns) in declared {
                        rows.push(format!(
                            "INSERT INTO {} (indexrelid, indrelid, indnatts, indisunique, \
                             indisprimary, indisvalid, indisready, indislive, \
                             indpred, indexprs) \
                             VALUES ({}, {}, {}, 'f', 'f', 't', '{}', 't', '', '')",
                            temp,
                            oid_of(&format!("{}.{}", name, index_name)),
                            relid,
                            columns,
                            if ready { "t" } else { "f" },
                        ));
                    }
                }

                for (column, unique, primary) in keys {
                    // The index has an identity of its own, distinct from the
                    // table's, so a client joining `pg_index` to `pg_class`
                    // does not find the table where it expects the index.
                    let indexrelid = oid_of(&format!("{}.{}", name, column));
                    rows.push(format!(
                        "INSERT INTO {} (indexrelid, indrelid, indnatts, indisunique, \
                         indisprimary, indisvalid, indisready, indislive, \
                         indpred, indexprs) \
                         VALUES ({}, {}, 1, {}, {}, 't', 't', 't', '', '')",
                        temp,
                        indexrelid,
                        relid,
                        if unique { "'t'" } else { "'f'" },
                        if primary { "'t'" } else { "'f'" },
                    ));
                }
            }
            (
                format!(
                    // `indpred` and `indexprs` are always empty: every index
                    // here is one plain column, never partial and never over an
                    // expression. They exist so a client that reads them finds
                    // the answer rather than an error.
                    "CREATE TABLE {} (indexrelid BIGINT, indrelid BIGINT, indnatts BIGINT, \
                     indisunique BOOL, indisprimary BOOL, indisvalid BOOL, indisready BOOL, \
                     indislive BOOL, indpred TEXT, indexprs TEXT)",
                    temp
                ),
                rows,
            )
        }
        // One row per index a table has: the implicit ones its `PRIMARY KEY`
        // and `UNIQUE` constraints bring, and the ones `CREATE INDEX`
        // declared, with the statement that would recreate each. This is the
        // view a migration tool reads to decide whether an index it wants
        // already exists.
        "pg_indexes" => {
            let mut rows = Vec::new();
            for name in &tables {
                let Some(relation) = database.relation(name) else {
                    continue;
                };
                let Some((owned, _)) = index_set(&relation)? else {
                    continue;
                };
                let indexes = owned.view();
                let schema = relation.schema();
                let column_name = |wo: u32| {
                    schema
                        .fields
                        .iter()
                        .find(|f| f.write_order as u32 == wo && !f.is_deleted)
                        .map(|f| f.name.clone())
                };
                let mut defs: Vec<(String, String)> = Vec::new();
                for index in indexes.list().iter() {
                    let columns: Vec<String> = match &index.path {
                        // An index over `doc->>'k'`, written as PostgreSQL
                        // writes an expression index: the expression in its
                        // own parentheses.
                        Some((document, steps)) => vec![format!(
                            "(({}))",
                            crate::core::jsonb::path_text(&column_name(*document).unwrap_or_default(), steps, true)
                        )],
                        None => index.columns.iter().filter_map(|(wo, _)| column_name(*wo)).collect(),
                    };
                    // A partial unique index is listed with its predicate, as
                    // PostgreSQL lists it. The index underneath holds every
                    // row; only the uniqueness is partial.
                    let predicate = schema
                        .unique_keys
                        .iter()
                        .find(|k| index.decl.unique && !k.is_deleted && k.name.eq_ignore_ascii_case(&index.decl.name))
                        .and_then(|k| k.predicate_display());
                    defs.push((
                        index.decl.name.clone(),
                        format!(
                            "CREATE {}INDEX {} ON {}.{} USING {} ({}){}",
                            if index.decl.unique { "UNIQUE " } else { "" },
                            index.decl.name,
                            SCHEMA,
                            name,
                            index.method().name(),
                            columns.join(", "),
                            predicate.map(|p| format!(" WHERE {}", p)).unwrap_or_default()
                        ),
                    ));
                }
                // A declaration over the columns of one already built shares
                // that index, so the loop above never met it: a second
                // `CREATE INDEX` on the same columns, a UNIQUE over indexed
                // columns, a primary key under an earlier `CREATE INDEX (id)`.
                // PostgreSQL lists each by its own name, and a migration tool
                // that cannot find its index or the table's pkey creates it
                // again or decides the key is gone.
                for (index_name, unique, columns, predicate) in shared_declarations(&schema) {
                    if defs.iter().any(|(listed, _)| listed.eq_ignore_ascii_case(&index_name)) {
                        continue;
                    }
                    let Some(built) = indexes.list().iter().find(|i| i.path.is_none() && i.decl.columns == columns)
                    else {
                        continue;
                    };
                    defs.push((
                        index_name.clone(),
                        format!(
                            "CREATE {}INDEX {} ON {}.{} USING {} ({}){}",
                            if unique { "UNIQUE " } else { "" },
                            index_name,
                            SCHEMA,
                            name,
                            built.method().name(),
                            columns.iter().filter_map(|wo| column_name(*wo)).collect::<Vec<_>>().join(", "),
                            predicate.map(|p| format!(" WHERE {}", p)).unwrap_or_default()
                        ),
                    ));
                }
                for g in indexes.gin().iter() {
                    let class = match g.opclass {
                        crate::core::jsonb::GinOpclass::Ops => "",
                        crate::core::jsonb::GinOpclass::PathOps => " jsonb_path_ops",
                    };
                    defs.push((
                        g.decl.name.clone(),
                        format!(
                            "CREATE INDEX {} ON {}.{} USING gin ({}{})",
                            g.decl.name,
                            SCHEMA,
                            name,
                            column_name(g.column).unwrap_or_default(),
                            class
                        ),
                    ));
                }
                for (index_name, def) in defs {
                    rows.push(format!(
                        "INSERT INTO {} (schemaname, tablename, indexname, tablespace, indexdef) \
                         VALUES ('{}', '{}', '{}', '', '{}')",
                        temp,
                        SCHEMA,
                        escape(name),
                        escape(&index_name),
                        escape(&def),
                    ));
                }
            }
            (
                format!(
                    "CREATE TABLE {} (schemaname TEXT, tablename TEXT, indexname TEXT, \
                     tablespace TEXT, indexdef TEXT)",
                    temp
                ),
                rows,
            )
        }
        "pg_database" => {
            // `datallowconn` and `datistemplate` are constants: every database
            // here accepts connections and none is a template.
            let names: Vec<String> = match catalog {
                Some(catalog) => catalog.names(),
                None => vec![database.name.clone()],
            };
            let rows = names
                .iter()
                .map(|name| {
                    format!(
                        "INSERT INTO {} (oid, datname, datdba, encoding, datcollate, \
                         datctype, datistemplate, datallowconn, datconnlimit) \
                         VALUES ({}, '{}', 10, 6, 'en_US.UTF-8', 'en_US.UTF-8', 'f', 't', -1)",
                        temp,
                        oid_of(name),
                        escape(name)
                    )
                })
                .collect();
            (
                format!(
                    "CREATE TABLE {} (oid BIGINT, datname TEXT, datdba BIGINT, \
                     encoding BIGINT, datcollate TEXT, datctype TEXT, \
                     datistemplate BOOL, datallowconn BOOL, datconnlimit BIGINT)",
                    temp
                ),
                rows,
            )
        }
        // Nothing inherits from anything here. A partitioned table is one
        // relation (relkind 'r' above) whose partitions no client can name, so
        // they are not children either. Produced empty rather than refused: a
        // query that anti-joins against it, which is how a caller excludes
        // partitions, then gets the right answer.
        "pg_inherits" => (
            format!(
                "CREATE TABLE {} (inhrelid BIGINT, inhparent BIGINT, inhseqno BIGINT)",
                temp
            ),
            Vec::new(),
        ),
        other => return Err(format!("'{}' is not a catalogue relation", other)),
    };

    crate::core::rql::executor::run_statement(&create, database, tx)
        .await
        .map_err(|e| format!("could not build {}: {}", view, e))?;
    // Filled in batches. A catalogue relation is one row per column of every
    // table — thousands for a real schema — and running one statement each cost
    // about a second and a half per thousand: `SELECT … FROM
    // information_schema.columns` took four and a half seconds, which a client
    // polling it feels as the whole database being slow. The rows are known and
    // identical in shape, so they go in a few statements instead of thousands.
    for statement in batch_inserts(&inserts, 200) {
        crate::core::rql::executor::run_statement(&statement, database, tx)
            .await
            .map_err(|e| format!("could not fill {}: {}", view, e))?;
    }
    Ok(temp)
}

/// Bytes one relation occupies, and its estimated tuple count.
///
/// Every file the table owns is counted — rows, pointers, schema sidecar,
/// filter checkpoint and sequence — which is what `pg_total_relation_size`
/// means: the relation and everything stored with it.
///
/// A partitioned table's figure is the running total over its materialized
/// partitions plus its parent files (schema, map log, ids): one length load
/// per file, never a scan. A table that cannot serve data (a broken
/// partitioned table, or a shell with no partitioned entry) errs rather than
/// reporting itself empty.
async fn relation_size(database: &Arc<Database>, name: &str) -> Result<(u64, i64), String> {
    let totals = totals_of(database, name).await?;
    Ok((totals.bytes, live_and_dead(totals.ids_handed_out, totals.slots).0))
}

/// One relation's figures through its [`SchemaView`]'s stats accessor: no
/// lock, no scan, and the schema and figures of one instance (Q3). `None`
/// from `schema_view` is a missing table, `(0, 0)`, unless it is a
/// partitioned table whose schema could not be read, which has no shell and
/// errs with its reason.
///
/// [`SchemaView`]: crate::core::database::SchemaView
async fn totals_of(database: &Arc<Database>, name: &str) -> Result<crate::core::database::RelationTotals, String> {
    match database.schema_view(name) {
        Some(view) => view.stats().totals().await.map_err(|e| e.to_string()),
        None => match database.relation(name) {
            Some(Relation::Partitioned(t)) => Err(t
                .check()
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "its schema is unavailable".to_string())),
            _ => Ok(Default::default()),
        },
    }
}

/// Live and dead tuple counts for one table.
///
/// Both are **estimates**, which is what PostgreSQL's are too — its own
/// documentation says these columns are approximate. Counting exactly would
/// mean a scan per table on every catalogue query; a caller that needs the
/// exact figure runs `SELECT COUNT(*)`, as it would against PostgreSQL.
///
/// ```text
/// slots = pointer file length / record length      (exact, and free)
/// live  = min(last_row_id, slots)                  (ids handed out, clamped)
/// dead  = slots - live
/// ```
///
/// # Why not from dead bytes
///
/// This used to derive `dead` as `last_dead_bytes / row_width`, and that is
/// blind to the failure these columns exist to reveal. Incremental vacuum never
/// reclaims pointer *slots* — the filter index refers to them as stable
/// addresses — so a table can accumulate thousands of dead slots per live row
/// while its dead *bytes* stay near zero. Measured on a live node: a heartbeat
/// table with **8 live rows in 38 030 slots** reported ~37 500 live and ~550
/// dead, a dead ratio of 1.5 % against a true 99.98 %.
///
/// That error ran in the reassuring direction, which is what made it serious.
/// `n_dead_tup` is what every PostgreSQL-shaped monitoring tool reads to decide
/// whether a table needs vacuuming, and those tools never see `SHOW TABLE
/// STATS` — so this number was the only one they got, and it told them the
/// table was healthy while it degraded.
///
/// # Why `last_row_id`
///
/// It is the id sequence's high-water mark, and an `UPDATE` does not draw from
/// it — pinned by `write_path_tests::updates_do_not_consume_row_ids`. So it is
/// at least the number of rows ever inserted, it is maintained by the write
/// path at no cost, and it needs no vacuum sweep to have run. That is the same
/// shape as PostgreSQL, where `n_live_tup` is maintained by the writes and
/// merely *corrected* by `ANALYZE`.
///
/// It errs by counting deleted rows as live, since their ids were still handed
/// out. That is the same direction as the old bug but bounded by the slot
/// count, where the old one was unbounded and uncorrectable.
///
/// # The clamp is load-bearing
///
/// `min(…, slots)` is not defensive. **Any path that advances the sequence
/// faster than it inserts rows** puts `last_row_id` above the slot count, and
/// compaction is a permanent one: `Table::compact` ends with
/// `last_row_id.fetch_max(…)`, which only ever raises, while the slot count
/// drops to the compacted total. A table compacted from 1 000 sparse ids to 10
/// slots keeps the mark at 1 000, and the clamp is what makes it report 10 —
/// the correct post-compaction answer. Historically, updates burning ids before
/// 2026-08-26 were another such path.
///
/// Without it, `live` exceeds `slots` and `dead` goes negative, which the
/// consumers of these two columns render as a negative dead-tuple count.
///
/// A partitioned table applies the same formula to its totals: slots summed
/// over the materialized partitions, and the ids its one shared sequence has
/// handed out, so it reports what a plain table with the same writes would.
/// Errs on a table that cannot serve data, never `(0, 0)`.
async fn tuple_counts(database: &Arc<Database>, name: &str) -> Result<(i64, i64), String> {
    let totals = totals_of(database, name).await?;
    Ok(live_and_dead(totals.ids_handed_out, totals.slots))
}

/// `live = min(ids handed out, slots)` and `dead = slots - live`; see
/// [`tuple_counts`] for why, and why the clamp is load-bearing.
fn live_and_dead(ids_handed_out: u64, slots: u64) -> (i64, i64) {
    let slots = slots as i64;
    let live = (ids_handed_out as i64).clamp(0, slots);
    (live, slots - live)
}

/// Every column index a schema declares, as `(name, unique, columns by
/// write_order, predicate)`, in the order and under the names
/// [`crate::core::index::for_schema`] builds them: `CREATE INDEX`, then the
/// primary key, inline UNIQUE columns and table-level keys. Gin and path
/// indexes are left out; each is always built under its own name.
fn shared_declarations(schema: &crate::core::row::schema::TableSchema) -> Vec<(String, bool, Vec<u32>, Option<String>)> {
    use crate::core::rql::unique_key::{implicit_key_name, pkey_name};
    let by_name = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .map(|f| f.write_order as u32)
    };
    let mut out = Vec::new();
    for decl in schema.secondary_indexes.iter().filter(|d| !d.is_deleted && d.method.opclass().is_none()) {
        out.push((decl.name.clone(), false, decl.columns.clone(), None));
    }
    let pk = schema.primary_key.as_deref().and_then(by_name);
    if let Some(pk) = pk {
        out.push((pkey_name(&schema.name), true, vec![pk], None));
    }
    // An inline `PRIMARY KEY` flags its column unique too; that is the
    // primary key's index, not a `<table>_<column>_key` of its own.
    for f in schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted && Some(f.write_order as u32) != pk) {
        out.push((implicit_key_name(&schema.name, &f.name), true, vec![f.write_order as u32], None));
    }
    for key in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
        if let Some(columns) = key.columns.iter().map(|c| by_name(c)).collect::<Option<Vec<u32>>>() {
            out.push((key.name.clone(), true, columns, key.predicate_display()));
        }
    }
    out
}

/// The indexes a table has and whether they are ready, or `None` when it has
/// none. A plain table's are its live ones. A partitioned table's are built
/// from its shell's schema (its declarations, and the implicit ones of its
/// `PRIMARY KEY` and `UNIQUE` constraints), since every partition builds its
/// own from the same schema and the shell builds none; they are ready once
/// every materialized partition's are. Errs on a table that cannot serve
/// data: "no indexes" would make a migration tool re-issue every one.
fn index_set(relation: &Relation) -> Result<Option<(std::sync::Arc<crate::core::index::TableIndexes>, bool)>, String> {
    match relation {
        Relation::Plain(table) => Ok(table
            .data()
            .map_err(|e| e.to_string())?
            .indexes
            .clone()
            .map(|indexes| {
                let ready = indexes.is_ready();
                (indexes, ready)
            })),
        Relation::Partitioned(table) => {
            table.check().map_err(|e| e.to_string())?;
            let indexes = crate::core::index::for_schema(&table.shell().schema);
            if indexes.is_empty() {
                return Ok(None);
            }
            let epoch = table.epoch().map_err(|e| e.to_string())?;
            let mut ready = true;
            for (_, partition) in table
                .materialized(&epoch, &crate::core::partition::Route::All)
                .map_err(|e| e.to_string())?
            {
                ready &= partition
                    .indexes()
                    .map_err(|e| e.to_string())?
                    .is_some_and(|i| i.is_ready());
            }
            Ok(Some((std::sync::Arc::new(indexes), ready)))
        }
    }
}

/// A stable identifier for a name, standing in for PostgreSQL's OID.
///
/// Derived from the name so it is the same across statements and across
/// restarts — a client that reads an oid from one query and joins on it in the
/// next gets the relation it asked for.
fn oid_of(name: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Kept positive and inside i32 range: `oid` is a 32-bit unsigned type in
    // PostgreSQL and a client may decode it as one.
    ((hash % 2_000_000_000) + 16_384) as i64
}

/// Tables the engine keeps for itself, which PostgreSQL's catalogue would not
/// list as user tables.
fn is_internal(name: &str) -> bool {
    name.starts_with("__") || name.starts_with(crate::core::rql::json_map::SHADOW_TABLE_PREFIX)
}

/// Microseconds as a millisecond literal a `DOUBLE PRECISION` column accepts.
///
/// Written out at fixed precision rather than through `{}`: an f64 formatted
/// the short way can come out as `1e-5`, and the value parser reads that as an
/// identifier rather than a number — a statement log with one sub-microsecond
/// entry in it would have failed the whole view.
#[cfg(feature = "query_stats")]
fn ms(micros: f64) -> String {
    format!("{:.6}", micros / 1_000.0)
}

fn escape(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(test)]
#[path = "catalog_partition_tests.rs"]
mod catalog_partition_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// The call is replaced wherever it appears, arguments and all, so a
    /// client filtering on a partial-index predicate gets "no such index"
    /// rather than a failed statement.
    #[test]
    fn pg_get_expr_becomes_an_empty_predicate() {
        assert_eq!(
            rewrite_get_expr("SELECT pg_get_expr(i.indpred, i.indrelid) FROM pg_index i"),
            "SELECT '' FROM pg_index i"
        );
        // The shape the application actually sends.
        assert_eq!(
            rewrite_get_expr(
                "SELECT 1 FROM pg_index i \
                 WHERE pg_get_expr(i.indpred, i.indrelid) ILIKE '%4806fcdf%'"
            ),
            "SELECT 1 FROM pg_index i WHERE '' ILIKE '%4806fcdf%'"
        );
    }

    /// A nested call must not end the span at the inner `)`, and a column that
    /// merely contains the name is not a call at all.
    #[test]
    fn the_rewrite_reads_the_whole_call_and_only_a_call() {
        assert_eq!(
            rewrite_get_expr("SELECT pg_get_expr(coalesce(a, b), c) FROM t"),
            "SELECT '' FROM t"
        );
        assert_eq!(
            rewrite_get_expr("SELECT my_pg_get_expr FROM t"),
            "SELECT my_pg_get_expr FROM t"
        );
        // Two of them, so the scan continues past the first.
        assert_eq!(
            rewrite_get_expr("SELECT pg_get_expr(a, b), pg_get_expr(c, d) FROM t"),
            "SELECT '', '' FROM t"
        );
    }

    #[test]
    fn only_a_statement_naming_a_view_is_recognised() {
        assert!(names_relation("SELECT * FROM pg_class", "pg_class"));
        assert!(names_relation(
            "SELECT * FROM pg_catalog.pg_class c",
            "pg_class"
        ));
        // A longer name that merely contains it is a different relation.
        assert!(!names_relation("SELECT * FROM pg_classroom", "pg_class"));
        assert!(!names_relation("SELECT * FROM my_pg_class", "pg_class"));
        assert!(!names_relation("SELECT * FROM users", "pg_class"));
    }

    #[test]
    fn a_reference_is_repointed_qualifier_and_all() {
        let out = replace_relation(
            "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname = 'pg_class'",
            "pg_class",
            "__cat_1",
        );
        assert!(
            out.starts_with("SELECT c.relname FROM __cat_1 c"),
            "{}",
            out
        );
        // The string literal is data, not a relation, so it stays as written.
        assert!(out.ends_with("'pg_class'"), "{}", out);
    }

    #[test]
    fn a_name_that_merely_contains_the_view_is_left_alone() {
        let out = replace_relation("SELECT * FROM pg_classroom", "pg_class", "__cat_1");
        assert_eq!(out, "SELECT * FROM pg_classroom");
    }

    #[test]
    fn a_size_call_becomes_a_column_reference() {
        // The size is read when pg_class is built, where the storage layer is
        // reachable; the row evaluator could not answer the function.
        assert_eq!(
            rewrite_size_calls("SELECT pg_total_relation_size(c.oid) FROM pg_class c"),
            "SELECT c.relsize FROM pg_class c"
        );
        assert_eq!(
            rewrite_size_calls("SELECT SUM(pg_relation_size(x.oid)) FROM pg_class x"),
            "SELECT SUM(x.relsize) FROM pg_class x"
        );
    }

    #[test]
    fn a_size_call_on_something_else_is_left_to_fail() {
        // Answering it would mean reporting a size for a relation nobody named.
        let sql = "SELECT pg_total_relation_size('users') FROM pg_class c";
        assert_eq!(rewrite_size_calls(sql), sql);
    }

    #[test]
    fn an_oid_is_stable_and_in_range() {
        // A client may decode `oid` as a 32-bit unsigned value, and a join
        // across two statements needs the same answer both times.
        assert_eq!(oid_of("users"), oid_of("users"));
        assert_ne!(oid_of("users"), oid_of("orders"));
        for name in [
            "users",
            "orders",
            "a",
            "",
            "user_table_row_archive_manifest",
        ] {
            let oid = oid_of(name);
            assert!(oid > 0 && oid < u32::MAX as i64, "{} -> {}", name, oid);
        }
    }

    #[test]
    fn the_engines_own_tables_are_not_user_tables() {
        assert!(is_internal("__cte_x_0_ab"));
        assert!(is_internal("__json_users_doc_1234"));
        assert!(!is_internal("users"));
    }

    #[test]
    fn every_listed_view_is_recognised_with_or_without_a_qualifier() {
        for view in VIEWS {
            // A qualifier-only view is deliberately *not* recognised bare:
            // `columns` is a keyword in `SHOW COLUMNS FROM t` and an ordinary
            // table name besides. See `QUALIFIED_ONLY`.
            assert_eq!(is_catalog_view(view), !needs_qualifier(view), "{}", view);
            assert!(is_catalog_view(&format!("pg_catalog.{}", view)), "{}", view);
            assert_eq!(
                is_catalog_view(&format!("\"{}\"", view)),
                !needs_qualifier(view),
                "{}",
                view
            );
        }
        assert!(!is_catalog_view("users"));
    }

    /// `may_name_a_view` relies on every view being `pg_…` or matched only
    /// under a qualifier. A view added outside that would be skipped by it.
    #[test]
    fn every_view_is_one_the_cheap_guard_can_see() {
        for view in VIEWS {
            assert!(view.starts_with("pg_") || needs_qualifier(view), "{view}");
            assert!(may_name_a_view(&format!("SELECT * FROM {}", if needs_qualifier(view) { format!("information_schema.{view}") } else { view.to_uppercase() })));
        }
        assert!(!may_name_a_view("UPDATE acc SET balance = 70 WHERE id = 10"));
        assert!(may_name_a_view("SELECT * FROM PG_CATALOG.tables"));
    }
}

#[cfg(test)]
mod batching_tests {
    use super::batch_inserts;

    #[test]
    fn statements_sharing_a_target_become_one() {
        let inserts = vec![
            "INSERT INTO t (a, b) VALUES (1, 'x')".to_string(),
            "INSERT INTO t (a, b) VALUES (2, 'y')".to_string(),
            "INSERT INTO t (a, b) VALUES (3, 'z')".to_string(),
        ];
        let out = batch_inserts(&inserts, 200);
        assert_eq!(
            out,
            vec!["INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y'), (3, 'z')"]
        );
    }

    #[test]
    fn a_batch_is_capped() {
        let inserts: Vec<String> = (0..5)
            .map(|i| format!("INSERT INTO t (a) VALUES ({})", i))
            .collect();
        assert_eq!(batch_inserts(&inserts, 2).len(), 3);
    }

    #[test]
    fn different_targets_are_not_merged() {
        let inserts = vec![
            "INSERT INTO a (x) VALUES (1)".to_string(),
            "INSERT INTO b (x) VALUES (2)".to_string(),
        ];
        assert_eq!(batch_inserts(&inserts, 200).len(), 2);
    }

    #[test]
    fn a_statement_of_another_shape_passes_through() {
        // A builder that emits something without a `VALUES` must still run.
        let inserts = vec!["INSERT INTO t SELECT * FROM other".to_string()];
        assert_eq!(batch_inserts(&inserts, 200), inserts);
    }

    #[test]
    fn nothing_in_nothing_out() {
        assert!(batch_inserts(&[], 200).is_empty());
    }
}

#[cfg(test)]
mod qualified_only_tests {
    use super::*;

    #[test]
    fn a_bare_columns_is_not_the_catalogue_relation() {
        // `SHOW COLUMNS FROM t` is the case that broke: the keyword was
        // rewritten to the temporary table's name, so the statement returned
        // one empty column and every client asking a table for its columns was
        // told it had none.
        assert!(!names_relation(
            "SHOW COLUMNS FROM \"rdb_roles\"",
            "columns"
        ));
        assert!(!names_relation("SELECT * FROM columns", "columns"));
        assert!(!is_catalog_view("columns"));
    }

    #[test]
    fn a_qualified_columns_still_is() {
        assert!(names_relation(
            "SELECT 1 FROM information_schema.columns WHERE table_name = 't'",
            "columns"
        ));
        assert!(is_catalog_view("information_schema.columns"));
    }

    #[test]
    fn the_keyword_survives_the_rewrite() {
        let sql = "SHOW COLUMNS FROM \"rdb_roles\"";
        assert_eq!(replace_relation(sql, "columns", "__cat_columns_1"), sql);
    }
}
