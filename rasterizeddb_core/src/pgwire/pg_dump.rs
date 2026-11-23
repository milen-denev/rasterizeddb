//! PostgreSQL's `pg_dump` against this server.
//!
//! `pg_dump` learns what to dump from about fifty catalogue queries, each
//! written for one server version and read back by column name
//! (`PQfnumber`). Most of them join catalogues this engine has no rows for
//! (`pg_opclass`, `pg_ts_config`, `pg_init_privs`, …) through shapes the
//! statement path does not read (`unnest('{…}'::oid[]) AS src(tbloid)`,
//! `acldefault(…)`, `pg_options_to_table(…)`). They are answered here
//! instead, from the same catalogue model `pg_class` and `pg_attribute` are
//! built from (`catalog_model`), when the session is `pg_dump`'s and the
//! statement is one of its catalogue queries ([`recognize`]). Everything
//! else it sends (`BEGIN`, the `SET`s, `LOCK TABLE`, `COPY … TO stdout`)
//! runs as any client's would.
//!
//! **The session.** `pg_dump` always sends `SET synchronize_seqscans TO off`
//! before its first catalogue query, whatever its options and its
//! `application_name`; nothing else here sets it. A session that did is
//! answered here ([`is_dump_session`]); any other reads the catalogue as
//! before, `psql`'s `\d` included.
//!
//! **Partitioned tables.** `pg_dump` dumps no rows for a relkind `'p'`
//! table: it expects them in the leaf partitions it finds through
//! `pg_inherits` (`makeTableDataInfo`, "data in partitions"), and the
//! partitions here are not relations a client can name. Answered as
//! `pg_class` answers everyone else, every partitioned table's rows would be
//! silently missing from the dump. So to `pg_dump` alone a partitioned table
//! is the table its rows are read from: relkind `'r'`, whose data step is
//! `COPY t TO stdout` over every partition, and its partitioning is the
//! engine's own spelling of the conversion, a post-data index
//! `CREATE INDEX t_partition_key ON public.t USING partition (k, …) WITH
//! (partitions = n)` (docs/partitioning.md, "Conversion as built"). A
//! restore into this server creates the table, loads its rows and then
//! converts it, key, partition count and all; PostgreSQL has no access method
//! `partition`, so a restore there fails that one statement and keeps a plain
//! table with every row (docs/postgres-import.md, "pg_dump").
//!
//! **Names.** A dump sets `search_path` to `''` before it creates anything,
//! so every relation an expression names must be schema-qualified as
//! PostgreSQL's own deparser qualifies it then: a sequence default is
//! `nextval('public.s'::regclass)`, a foreign key `REFERENCES public.t(…)`,
//! a view's `FROM public.t`.

use std::collections::HashMap;
use std::io;
use std::sync::Arc as StdArc;

use rclite::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::db_type::DbType;
use crate::core::rql::catalog_model::{self, Model, Rel};

use super::SessionState;

/// `pg_class`'s oid, the `tableoid` of every relation and index.
const PG_CLASS: i64 = 1259;
/// `pg_attrdef`'s.
const PG_ATTRDEF: i64 = 2604;
/// `pg_constraint`'s.
const PG_CONSTRAINT: i64 = 2606;
/// `pg_namespace`'s.
const PG_NAMESPACE: i64 = 2615;
/// `pg_type`'s.
const PG_TYPE: i64 = 1247;
/// `pg_enum`'s.
const PG_ENUM: i64 = 3501;

/// Whether this session is `pg_dump`'s: it turned `synchronize_seqscans`
/// off, which `pg_dump` does as it connects (`setup_connection`) and before
/// its first catalogue query.
pub(super) fn is_dump_session(state: &SessionState) -> bool {
    state
        .settings
        .get("synchronize_seqscans")
        .is_some_and(|v| matches!(v.trim().trim_matches('\'').to_ascii_lowercase().as_str(), "off" | "false" | "0" | "no"))
}

/// One of `pg_dump`'s catalogue queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Kind {
    /// A catalogue that holds nothing here: extensions, operators, casts,
    /// text-search objects, policies, publications, comments, …
    Empty,
    Roles,
    Namespaces,
    /// `getTables`: every relation `pg_dump` may dump.
    Tables,
    /// `getTypes`: the enum types.
    Types,
    /// `getTableAttrs`: the columns of the listed tables.
    Attributes,
    /// Their defaults (`pg_attrdef`).
    Defaults,
    /// Their `CHECK` constraints.
    Checks,
    /// `getIndexes`: indexes, primary and unique keys.
    Indexes,
    /// `getConstraints`: foreign keys.
    ForeignKeys,
    /// `getDependencies`: what orders the dump.
    Dependencies,
    /// `collectSequences`: every sequence's definition.
    Sequences,
    /// `SELECT last_value, is_called FROM <sequence>`.
    SequenceData(String),
    /// `pg_get_viewdef(<oid>)`.
    ViewDefinition(i64),
    /// An enum type's labels.
    EnumLabels(i64),
    /// Comments: only `public`'s, the one `initdb` gives it.
    Descriptions,
    /// `dumpDatabase`: the database's own row of `pg_database`.
    Database,
}

/// The statement as one lowercase line, `pg_catalog.` taken out: what the
/// recognisers match.
fn flatten(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut space = false;
    for ch in sql.chars() {
        if ch.is_whitespace() {
            space = true;
            continue;
        }
        if space && !out.is_empty() {
            out.push(' ');
        }
        space = false;
        out.extend(ch.to_lowercase());
    }
    out.replace("pg_catalog.", "")
}

/// The first number after `marker` in `flat`: `'16431'::oid`, or
/// `CAST('16431' AS oid)` as an `EXECUTE` binds a prepared statement's
/// parameter (`sql_prepare::bind`).
fn quoted_number_after(flat: &str, marker: &str) -> Option<i64> {
    let at = flat.find(marker)? + marker.len();
    let rest = flat[at..].trim_start();
    let rest = rest.strip_prefix("cast(").unwrap_or(rest);
    let rest = rest.trim_start_matches(['(', ' ', '\'']);
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Which of `pg_dump`'s catalogue queries `sql` is; `None` for anything
/// else, which runs as written.
pub(super) fn recognize(sql: &str) -> Option<Kind> {
    let flat = flatten(sql);
    let f = flat.trim_end_matches(';').trim_end();
    if !(f.starts_with("select ") || f.starts_with("with ")) {
        return None;
    }
    let has = |s: &str| f.contains(s);
    if f.starts_with("select last_value, is_called from ") {
        let name = sql.trim().trim_end_matches(';').split_whitespace().last()?;
        return Some(Kind::SequenceData(unqualified(name)));
    }
    if f.starts_with("select pg_get_viewdef(") {
        return quoted_number_after(f, "pg_get_viewdef(").map(Kind::ViewDefinition);
    }
    if has("from pg_enum where enumtypid") {
        return quoted_number_after(f, "enumtypid =").map(Kind::EnumLabels);
    }
    if f.starts_with("select oid, rolname from pg_roles") {
        return Some(Kind::Roles);
    }
    if has("from pg_namespace n") && has("nspacl") {
        return Some(Kind::Namespaces);
    }
    if has("from pg_class c") && has("relkind in (") && has("relpersistence") {
        return Some(Kind::Tables);
    }
    if has("from pg_type") && has("typrelkind") {
        return Some(Kind::Types);
    }
    if has("join pg_attribute a on (src.tbloid = a.attrelid)") {
        return Some(Kind::Attributes);
    }
    if has("join pg_attrdef a on (src.tbloid = a.adrelid)") {
        return Some(Kind::Defaults);
    }
    if has("join pg_constraint c on (src.tbloid = c.conrelid)") {
        if has("contype = 'c'") {
            return Some(Kind::Checks);
        }
        if has("contype = 'f'") {
            return Some(Kind::ForeignKeys);
        }
    }
    if has("join pg_index i on (src.tbloid = i.indrelid)") {
        return Some(Kind::Indexes);
    }
    if has("from pg_depend where deptype != 'p'") {
        return Some(Kind::Dependencies);
    }
    if has("from pg_sequence ") && has("seqrelid") {
        return Some(Kind::Sequences);
    }
    if has("from pg_description") {
        return Some(Kind::Descriptions);
    }
    if has("from pg_database where datname = current_database()") && has("datdba") {
        return Some(Kind::Database);
    }
    const EMPTY: &[&str] = &[
        "from pg_settings where name = 'restrict_nonsystem_relation_kind'",
        "from pg_extension x",
        "refclassid = 'pg_extension'::regclass",
        "from pg_proc p",
        "from pg_language",
        "from pg_operator",
        "from pg_am",
        "from pg_opclass",
        "from pg_opfamily",
        "from pg_ts_parser",
        "from pg_ts_template",
        "from pg_ts_dict",
        "from pg_ts_config",
        "from pg_foreign_data_wrapper",
        "from pg_foreign_server",
        "from pg_default_acl",
        "from pg_collation",
        "from pg_conversion",
        "from pg_cast",
        "from pg_transform",
        "from pg_inherits",
        "from pg_event_trigger",
        "from pg_partitioned_table",
        "from pg_statistic_ext",
        "join pg_trigger t",
        "from pg_rewrite",
        "join pg_policy pol",
        "from pg_publication",
        "from pg_publication_rel",
        "from pg_publication_namespace",
        "from pg_subscription",
        "with recursive w as",
        "from pg_largeobject_metadata",
        "where attacl is not null",
        "from pg_init_privs",
        "join pg_init_privs pip",
        "from pg_seclabels",
        "from pg_shseclabel",
        "from pg_db_role_setting",
    ];
    EMPTY.iter().any(|m| has(m)).then_some(Kind::Empty)
}

/// `public.s`, `"S"` or `s`, as the name it is (`s`, `S`).
fn unqualified(name: &str) -> String {
    let last = crate::core::sql_scan::split_top_level(name, b'.')
        .last()
        .map(|s| s.to_string())
        .unwrap_or_else(|| name.to_string());
    match last.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => last.to_ascii_lowercase(),
    }
}

// ------------------------------------------------------------------ names

/// Where the top-level `FROM` of a `SELECT` starts, past quotes and
/// parentheses; the end of the text when it has none.
fn top_level_from(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && bytes[i..].len() >= 4
                && bytes[i..i + 4].eq_ignore_ascii_case(b"from")
                && (i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]))
                && bytes.get(i + 4).is_none_or(|b| !crate::core::sql_scan::is_identifier_byte(*b)) =>
            {
                return i;
            }
            _ => {}
        }
        i += 1;
    }
    bytes.len()
}

/// The output column names of a `SELECT`, as PostgreSQL names them: an
/// alias, else a column's or a function's own name past any cast, else
/// `?column?`. `pg_dump` finds every column it reads by these names.
fn select_names(sql: &str) -> Vec<String> {
    let items = select_items(sql);
    if items.is_empty() {
        return vec!["?column?".to_string()];
    }
    items.into_iter().map(item_name).collect()
}

/// The items of a `SELECT`'s list; none for any other statement.
fn select_items(sql: &str) -> Vec<&str> {
    let text = sql.trim().trim_end_matches(';');
    if !text.get(..6).is_some_and(|h| h.eq_ignore_ascii_case("select")) {
        return Vec::new();
    }
    let body = &text[6..];
    let list = body[..top_level_from(body)].trim_start();
    let list = match list.get(..9) {
        Some(w) if w.eq_ignore_ascii_case("distinct ") => &list[9..],
        _ => list,
    };
    crate::core::sql_scan::split_top_level(list, b',')
}

/// Whether a `SELECT`'s every item is a `count(…)`: an aggregate over no
/// rows is one row of zeros, not no row. `pg_dump` counts the
/// subscriptions it may not read (`getSubscriptions`, when the session is
/// no superuser) and reads that row's first cell.
fn counts_only(sql: &str) -> bool {
    let items = select_items(sql);
    !items.is_empty() && items.iter().all(|i| i.trim().get(..6).is_some_and(|h| h.eq_ignore_ascii_case("count(")))
}

/// The top-level position of the last ` AS ` in `item`.
fn top_level_as(item: &str) -> Option<usize> {
    let bytes = item.as_bytes();
    let mut depth = 0usize;
    let mut found = None;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            b if depth == 0 && b.is_ascii_whitespace() => {
                if bytes.len() > i + 3
                    && bytes[i + 1..i + 3].eq_ignore_ascii_case(b"as")
                    && bytes[i + 3].is_ascii_whitespace()
                {
                    found = Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    found
}

fn item_name(item: &str) -> String {
    let item = item.trim();
    if let Some(at) = top_level_as(item) {
        let alias = item[at + 4..].trim();
        return match alias.strip_prefix('"').and_then(|a| a.strip_suffix('"')) {
            Some(quoted) => quoted.replace("\"\"", "\""),
            None => alias.to_ascii_lowercase(),
        };
    }
    // Past a cast: `oprcode::oid` is `oprcode`.
    let expr = match item.find("::") {
        Some(at) if !item[..at].contains('\'') || item.starts_with('\'') => &item[..at],
        _ => item,
    };
    let expr = expr.trim();
    let head = expr.split('(').next().unwrap_or(expr).trim();
    let is_name = !head.is_empty()
        && head
            .bytes()
            .all(|b| crate::core::sql_scan::is_identifier_byte(b) || b == b'.' || b == b'"')
        && !head.as_bytes()[0].is_ascii_digit();
    if !is_name || head.eq_ignore_ascii_case("null") || head.eq_ignore_ascii_case("true") || head.eq_ignore_ascii_case("false") {
        return "?column?".to_string();
    }
    let last = head.rsplit('.').next().unwrap_or(head);
    match last.strip_prefix('"').and_then(|a| a.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None if last.eq_ignore_ascii_case("case") => "case".to_string(),
        None => last.to_ascii_lowercase(),
    }
}

// ------------------------------------------------------------------ model

/// A sequence, as `pg_sequence` and the sequence's own row give it.
struct Sequence {
    oid: i64,
    name: String,
    data_type: String,
    start: i64,
    increment: i64,
    min: i64,
    max: i64,
    cache: i64,
    cycle: bool,
    last: Option<i64>,
    /// A `SERIAL`'s or an identity column's, which has no counter of its
    /// own until an insert after a restart seeds it from the column.
    implicit: bool,
    /// The column it belongs to (`OWNED BY`, a `SERIAL`'s or an identity
    /// column's): table oid, attnum, and whether it is an identity's.
    owner: Option<(i64, i64, bool)>,
}

/// What a dump session is answered from: the catalogue model, and what
/// `pg_dump` reads that `pg_class` does not hold.
pub(super) struct DumpModel {
    model: Model,
    sequences: Vec<Sequence>,
    /// A view's or materialized view's query by oid, its relations
    /// schema-qualified; and the oids of the relations it reads.
    views: HashMap<i64, (String, Vec<i64>)>,
    /// A partitioned table's declared partition count, by oid (`None`:
    /// unknown, its map did not replay).
    partitions: HashMap<i64, Option<u32>>,
    /// The database's name.
    database: String,
    /// What this database holds that a dump from here leaves out: its
    /// routines, triggers, domains and grants, which no catalogue query here
    /// answers yet.
    not_dumped: Vec<String>,
}

impl DumpModel {
    fn rel(&self, oid: i64) -> Option<&Rel> {
        self.model.rels.iter().find(|r| r.oid == oid)
    }

    fn rel_named(&self, name: &str) -> Option<&Rel> {
        self.model
            .rels
            .iter()
            .find(|r| r.name == name)
            .or_else(|| self.model.rels.iter().find(|r| r.name.eq_ignore_ascii_case(name)))
    }

    fn enum_name(&self, oid: i64) -> Option<(&str, bool)> {
        self.model.enums.iter().find_map(|e| {
            if e.oid() == oid {
                Some((e.name.as_str(), false))
            } else if e.oid() + 1 == oid {
                Some((e.name.as_str(), true))
            } else {
                None
            }
        })
    }
}

async fn build(database: &Arc<Database>, tx: crate::core::mvcc::transaction::TxId) -> Result<DumpModel, String> {
    use crate::core::rql::{catalog_views, sequence_catalog, views};
    let model = catalog_views::model_for(database, tx).await?;
    let view_tx = (tx != crate::core::mvcc::transaction::FROZEN_TX).then_some(tx);
    let defs = views::load(database, None, view_tx).await?;
    let explicit = sequence_catalog::load(database, None, view_tx).await?;
    let implicit = sequence_catalog::implicit_sequences(database);
    let column_of = |table: &str, column: &str| -> Option<(i64, i64, bool)> {
        let rel = model.rels.iter().find(|r| r.name.eq_ignore_ascii_case(table))?;
        let col = rel.columns.iter().find(|c| c.name.eq_ignore_ascii_case(column))?;
        Some((rel.oid, col.attnum, !col.identity.is_empty()))
    };
    let mut sequences = Vec::new();
    for row in catalog_views::sequence_rows(database, view_tx).await? {
        let is_implicit = !explicit.iter().any(|d| d.name == row.name);
        let owner = match explicit.iter().find(|d| d.name == row.name) {
            Some(def) => def
                .owned_by
                .rsplit_once('.')
                .and_then(|(table, column)| column_of(unqualified(table).as_str(), column)),
            None => implicit
                .iter()
                .find(|(s, _, _)| *s == row.name)
                .and_then(|(_, table, column)| column_of(table, column)),
        };
        sequences.push(Sequence {
            oid: catalog_model::oid_of(&row.name),
            name: row.name,
            data_type: row.data_type,
            start: row.start,
            increment: row.increment,
            min: row.min,
            max: row.max,
            cache: row.cache,
            cycle: row.cycle,
            last: row.last,
            implicit: is_implicit,
            owner,
        });
    }
    let names: Vec<String> = model.rels.iter().map(|r| r.name.clone()).collect();
    let mut view_map = HashMap::new();
    for def in &defs {
        let deps = def
            .deps
            .iter()
            .filter_map(|d| model.rels.iter().find(|r| r.name.eq_ignore_ascii_case(d)).map(|r| r.oid))
            .collect();
        view_map.insert(catalog_model::oid_of(&def.name), (qualify_relations(&def.definition, &names), deps));
    }
    let mut partitions = HashMap::new();
    for rel in model.rels.iter().filter(|r| r.kind == "p") {
        if let Some(Relation::Partitioned(pt)) = database.relation(&rel.name) {
            partitions.insert(rel.oid, pt.declared());
        }
    }
    let mut not_dumped: Vec<String> = Vec::new();
    for p in &model.procs {
        not_dumped.push(format!("{} {}", if p.kind == "p" { "procedure" } else { "function" }, p.name));
    }
    for t in crate::core::rql::triggers::load(database, None, view_tx).await.unwrap_or_default() {
        not_dumped.push(format!("trigger {} on {}", t.name, t.table));
    }
    for d in crate::core::rql::domains::load(database, None, view_tx).await.unwrap_or_default() {
        not_dumped.push(format!("domain {}", d.name));
    }
    if database.has_table(crate::core::rql::privileges::ACL_TABLE) {
        for a in crate::core::rql::privileges::load(database, view_tx).await.unwrap_or_default() {
            if a.privilege != "OWNER" {
                not_dumped.push(format!("GRANT {} ON {} TO {}", a.privilege, a.relation, a.grantee));
            }
        }
    }
    Ok(DumpModel { model, sequences, views: view_map, partitions, database: database.name.clone(), not_dumped })
}

/// The model this session's transaction sees, built once per transaction:
/// `pg_dump` reads the catalogue some fifty times inside one `REPEATABLE
/// READ` block.
async fn dump_model(database: &Arc<Database>, state: &mut SessionState) -> Result<StdArc<DumpModel>, String> {
    let tx = state.current_tx();
    if let (Some(tx), Some((held, model))) = (tx, &state.dump_model)
        && *held == tx
    {
        return Ok(model.clone());
    }
    let mut model = build(database, tx.unwrap_or(crate::core::mvcc::transaction::FROZEN_TX)).await?;
    // A `SERIAL`'s counter after a restart is seeded by the table's first
    // insert, from the column's largest value; until then the catalogue
    // reports no value. Dumped as such, the restored sequence would start
    // again at its first value, under rows that already hold it. So the
    // value is the one the next insert would seed from.
    for i in 0..model.sequences.len() {
        let q = &model.sequences[i];
        let (true, None, Some((table, attnum, _))) = (q.implicit, q.last, q.owner) else {
            continue;
        };
        let Some(rel) = model.rel(table) else { continue };
        let Some(column) = rel.columns.iter().find(|c| c.attnum == attnum) else { continue };
        let sql = format!("SELECT max({}) FROM {}", ident(&column.name), ident(&rel.name));
        if let Some(max) = scalar(database, state, &sql).await?.and_then(|v| v.parse::<i64>().ok()) {
            model.sequences[i].last = Some(max);
        }
    }
    let model = StdArc::new(model);
    state.dump_model = tx.map(|tx| (tx, model.clone()));
    Ok(model)
}

/// The first cell of what `sql` returns, run through this session's own
/// statement path (inside its transaction); `None` for no row or NULL.
async fn scalar(database: &Arc<Database>, state: &mut SessionState, sql: &str) -> Result<Option<String>, String> {
    let mut sink = std::io::Cursor::new(Vec::<u8>::new());
    let was_internal = std::mem::replace(&mut state.view_internal, true);
    let was_capturing = std::mem::replace(&mut state.native_capture, false);
    let kept_result = state.native_result.take();
    let ran = super::internal_run(Box::pin(super::execute_single_statement(database, state, &mut sink, sql, &[], true))).await;
    state.native_capture = was_capturing;
    state.native_result = kept_result;
    state.view_internal = was_internal;
    ran.map_err(|e| e.to_string())?;
    if state.statement_failed {
        return Err(state.statement_error.clone().unwrap_or_else(|| format!("{sql} failed")));
    }
    let bytes = sink.into_inner();
    let mut at = 0usize;
    while at + 5 <= bytes.len() {
        let len = u32::from_be_bytes([bytes[at + 1], bytes[at + 2], bytes[at + 3], bytes[at + 4]]) as usize;
        let end = (at + 1 + len).min(bytes.len());
        if bytes[at] == b'D' && end >= at + 11 {
            let cell = i32::from_be_bytes([bytes[at + 7], bytes[at + 8], bytes[at + 9], bytes[at + 10]]);
            if cell < 0 {
                return Ok(None);
            }
            let start = at + 11;
            let stop = (start + cell as usize).min(end);
            return Ok(Some(String::from_utf8_lossy(&bytes[start..stop]).into_owned()));
        }
        at = end.max(at + 5);
    }
    Ok(None)
}

// ------------------------------------------------------------- rendering

/// An identifier as PostgreSQL's deparser writes it: bare when it is a
/// lowercase name that is no reserved word, quoted otherwise.
fn ident(name: &str) -> String {
    const RESERVED: &[&str] = &[
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check",
        "collate", "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time",
        "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except",
        "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into",
        "lateral", "leading", "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or",
        "order", "placing", "primary", "references", "returning", "select", "session_user", "some", "symmetric",
        "system_user", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when",
        "where", "window", "with",
    ];
    let simple = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'$');
    if simple && !RESERVED.contains(&name) {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn qualified(name: &str) -> String {
    format!("public.{}", ident(name))
}

/// A column default as PostgreSQL's deparser gives it with an empty
/// `search_path`: each `nextval('s')` as `nextval('public.s'::regclass)`.
fn pg_default(expression: &str) -> String {
    let bytes = expression.as_bytes();
    let mut out = String::with_capacity(expression.len() + 24);
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len()).max(i + 1);
            continue;
        }
        let starts = i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]);
        if starts && bytes.len() >= i + 8 && bytes[i..i + 8].eq_ignore_ascii_case(b"nextval(") {
            let open = i + 7;
            if let Some(close) = crate::core::sql_scan::matching_close(expression, open) {
                let inner = expression[open + 1..close].trim();
                let inner = inner.split("::").next().unwrap_or(inner).trim();
                if let Some(name) = inner.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
                    let name = name.replace("''", "'");
                    let target = if name.contains('.') { name.clone() } else { qualified(&unqualified(&name)) };
                    out.push_str(&expression[copied..i]);
                    out.push_str(&format!("nextval('{}'::regclass)", target.replace('\'', "''")));
                    copied = close + 1;
                    i = close + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    out.push_str(&expression[copied..]);
    out
}

/// A foreign key's definition with its referenced table schema-qualified.
fn qualified_references(definition: &str, table: &str) -> String {
    let upper = definition.to_ascii_uppercase();
    let Some(at) = upper.find(" REFERENCES ") else {
        return definition.to_string();
    };
    let start = at + " REFERENCES ".len();
    let rest = &definition[start..];
    let end = if rest.starts_with('"') {
        rest[1..].find('"').map_or(rest.len(), |e| e + 2)
    } else {
        rest.find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$' || c == '.')).unwrap_or(rest.len())
    };
    format!("{}{}{}", &definition[..start], qualified(table), &rest[end..])
}

/// A view's query with each relation it names in a `FROM` list or a
/// `JOIN` schema-qualified, as PostgreSQL's deparser writes it under an
/// empty `search_path`, which a dump sets before it creates the view.
fn qualify_relations(sql: &str, relations: &[String]) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let mut copied = 0usize;
    // Per parenthesis depth: whether a `FROM` list is open there.
    let mut from_at: Vec<bool> = vec![false];
    // The last significant word or punctuation, lowercase.
    let mut previous = String::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if b == b'\'' {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len()).max(i + 1);
            previous = "'".to_string();
            continue;
        }
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'(' {
            from_at.push(false);
            previous = "(".to_string();
            i += 1;
            continue;
        }
        if b == b')' {
            if from_at.len() > 1 {
                from_at.pop();
            }
            previous = ")".to_string();
            i += 1;
            continue;
        }
        let quoted = b == b'"';
        if quoted || crate::core::sql_scan::is_identifier_byte(b) {
            let start = i;
            let (word, end) = if quoted {
                let mut j = i + 1;
                while j < bytes.len() {
                    if bytes[j] == b'"' {
                        if bytes.get(j + 1) == Some(&b'"') {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                let end = (j + 1).min(bytes.len());
                (sql[i + 1..end.saturating_sub(1).max(i + 1)].replace("\"\"", "\""), end)
            } else {
                let mut j = i;
                while j < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[j]) {
                    j += 1;
                }
                (sql[i..j].to_string(), j)
            };
            i = end;
            let lower = word.to_ascii_lowercase();
            let in_from = *from_at.last().unwrap_or(&false);
            let qualified_before = start > 0 && bytes[start - 1] == b'.';
            let qualified_after = bytes.get(end) == Some(&b'.');
            let call = sql[end..].trim_start().starts_with('(');
            let position = previous == "from" || previous == "join" || (previous == "," && in_from);
            if !quoted && !qualified_before {
                match lower.as_str() {
                    "from" => {
                        if let Some(top) = from_at.last_mut() {
                            *top = true;
                        }
                    }
                    "where" | "group" | "having" | "order" | "limit" | "offset" | "union" | "intersect" | "except"
                    | "window" | "fetch" | "for" | "returning" | "select" => {
                        if let Some(top) = from_at.last_mut() {
                            *top = false;
                        }
                    }
                    _ => {}
                }
            }
            if position && !qualified_before && !qualified_after && !call {
                let name = if quoted { word.clone() } else { lower.clone() };
                if relations.iter().any(|r| *r == name) {
                    out.push_str(&sql[copied..start]);
                    out.push_str(&qualified(&name));
                    copied = end;
                }
            }
            previous = if quoted { "\"".to_string() } else { lower };
            continue;
        }
        previous = (b as char).to_string();
        i += 1;
    }
    out.push_str(&sql[copied..]);
    out
}

/// A column's type as `format_type` writes it with an empty `search_path`.
fn type_name(dm: &DumpModel, type_oid: i64, typmod: i64) -> String {
    if let Some((name, array)) = dm.enum_name(type_oid) {
        return format!("{}{}", qualified(name), if array { "[]" } else { "" });
    }
    catalog_model::format_type(type_oid, Some(typmod)).unwrap_or_else(|| "text".to_string())
}

/// The partition key index `pg_dump` is shown on a partitioned table: its
/// oid, name and definition.
fn partition_index(dm: &DumpModel, rel: &Rel) -> Option<(i64, String, String)> {
    if rel.kind != "p" || rel.partition_key.is_empty() {
        return None;
    }
    let mut name = format!("{}_partition_key", rel.name);
    while rel.indexes.iter().any(|i| i.name == name) || dm.model.rels.iter().any(|r| r.name == name) {
        name.push('_');
    }
    let columns: Vec<String> = rel
        .partition_key
        .iter()
        .filter_map(|a| rel.columns.iter().find(|c| c.attnum == *a))
        .map(|c| ident(&c.name))
        .collect();
    let count = match dm.partitions.get(&rel.oid).copied().flatten() {
        Some(n) => format!(" WITH (partitions = {n})"),
        None => String::new(),
    };
    let definition = format!(
        "CREATE INDEX {} ON {} USING partition ({}){}",
        ident(&name),
        qualified(&rel.name),
        columns.join(", "),
        count
    );
    Some((catalog_model::index_oid(&rel.name, "__partition_key"), name, definition))
}

// ------------------------------------------------------------------- rows

type Cell = Option<String>;

fn s(v: impl ToString) -> Cell {
    Some(v.to_string())
}

fn flag(v: bool) -> Cell {
    Some(if v { "t" } else { "f" }.to_string())
}

/// The answer's rows: by column name, projected onto the statement's select
/// list; or by position, for a list whose names repeat.
enum Rows {
    Named(Vec<Vec<(&'static str, Cell)>>),
    Positional(Vec<Vec<Cell>>),
}

/// The oids an `unnest('{…}'::oid[])` lists.
fn listed_oids(sql: &str) -> Vec<i64> {
    let lower = sql.to_ascii_lowercase();
    let Some(at) = lower.find("unnest('{") else {
        return Vec::new();
    };
    let rest = &sql[at + "unnest('{".len()..];
    let end = rest.find('}').unwrap_or(rest.len());
    rest[..end].split(',').filter_map(|n| n.trim().parse().ok()).collect()
}

/// The relations `oids` names, in oid order, which is the order `pg_dump`
/// reads every per-table answer in.
fn listed<'a>(dm: &'a DumpModel, oids: &[i64]) -> Vec<&'a Rel> {
    let mut rels: Vec<&Rel> = dm.model.rels.iter().filter(|r| oids.contains(&r.oid)).collect();
    rels.sort_by_key(|r| r.oid);
    rels
}

fn attrdef_oid(rel: &Rel, column: &str) -> i64 {
    catalog_model::oid_of(&format!("{}.{}.attrdef", rel.name, column))
}

/// The defaults `pg_attrdef` holds for `rel`: (attnum, column name, text).
fn defaults_of(rel: &Rel) -> Vec<(i64, &str, String)> {
    if !matches!(rel.kind, "r" | "p") {
        return Vec::new();
    }
    rel.columns
        .iter()
        .filter(|c| c.identity.is_empty())
        .filter_map(|c| c.default.as_ref().map(|d| (c.attnum, c.name.as_str(), pg_default(d))))
        .collect()
}

fn rows_of(kind: &Kind, sql: &str, dm: &DumpModel) -> Rows {
    let mut rows: Vec<Vec<(&'static str, Cell)>> = Vec::new();
    match kind {
        Kind::Empty => {}
        Kind::Roles => {
            let mut roles: Vec<(i64, String)> = dm.model.roles.iter().map(|r| (r.oid, r.name.clone())).collect();
            if !roles.iter().any(|(o, _)| *o == catalog_model::DATABASE_OWNER_ROLE) {
                roles.push((catalog_model::DATABASE_OWNER_ROLE, "pg_database_owner".to_string()));
            }
            roles.sort();
            for (oid, name) in roles {
                rows.push(vec![("oid", s(oid)), ("rolname", Some(name))]);
            }
        }
        Kind::Namespaces => {
            let owner = catalog_model::DATABASE_OWNER_ROLE;
            for (oid, name, nspowner, acl) in [
                (catalog_model::NS_CATALOG, "pg_catalog", catalog_model::BOOTSTRAP_ROLE, None),
                // `public`'s standard ACL since PostgreSQL 15, which `pg_dump`
                // compares against and then writes nothing for.
                (
                    catalog_model::NS_PUBLIC,
                    "public",
                    owner,
                    Some("{pg_database_owner=UC/pg_database_owner,=U/pg_database_owner}".to_string()),
                ),
                (catalog_model::NS_INFORMATION_SCHEMA, "information_schema", catalog_model::BOOTSTRAP_ROLE, None),
            ] {
                rows.push(vec![
                    ("tableoid", s(PG_NAMESPACE)),
                    ("oid", s(oid)),
                    ("nspname", s(name)),
                    ("nspowner", s(nspowner)),
                    ("nspacl", acl),
                    ("acldefault", s("{}")),
                ]);
            }
        }
        Kind::Tables => {
            let mut rels: Vec<&Rel> = dm.model.rels.iter().collect();
            rels.sort_by_key(|r| r.oid);
            for rel in rels {
                let sequence = dm.sequences.iter().find(|q| rel.kind == "S" && q.oid == rel.oid);
                let owner = sequence.and_then(|q| q.owner);
                let checks = rel.constraints.iter().filter(|c| c.contype == "c").count();
                let has_index = !rel.indexes.is_empty() || partition_index(dm, rel).is_some();
                // A partitioned table is the table its rows are read from
                // (the module's comment says why).
                let kind = if rel.kind == "p" { "r" } else { rel.kind };
                rows.push(vec![
                    ("tableoid", s(PG_CLASS)),
                    ("oid", s(rel.oid)),
                    ("relname", s(&rel.name)),
                    ("relnamespace", s(catalog_model::NS_PUBLIC)),
                    ("relkind", s(kind)),
                    ("reltype", s(0)),
                    ("relowner", s(rel.owner)),
                    ("relchecks", s(checks)),
                    ("relhasindex", flag(has_index)),
                    ("relhasrules", flag(false)),
                    ("relpages", s(0)),
                    ("reltuples", s(if rel.kind == "S" { -1 } else { rel.tuples })),
                    ("relallvisible", s(0)),
                    ("relallfrozen", s(0)),
                    ("relhastriggers", flag(false)),
                    ("relpersistence", s("p")),
                    ("reloftype", s(0)),
                    ("relacl", None),
                    ("acldefault", None),
                    ("foreignserver", s(0)),
                    ("relfrozenxid", s(0)),
                    ("tfrozenxid", s(0)),
                    ("toid", s(0)),
                    ("toastpages", s(0)),
                    ("toast_reloptions", None),
                    ("owning_tab", owner.map(|o| o.0.to_string())),
                    ("owning_col", owner.map(|o| o.1.to_string())),
                    ("reltablespace", None),
                    ("relhasoids", flag(false)),
                    ("relispopulated", flag(true)),
                    ("relreplident", s("d")),
                    ("relrowsecurity", flag(false)),
                    ("relforcerowsecurity", flag(false)),
                    ("relminmxid", s(0)),
                    ("tminmxid", s(0)),
                    ("reloptions", None),
                    ("checkoption", None),
                    ("amname", matches!(kind, "r" | "m").then(|| "heap".to_string())),
                    ("is_identity_sequence", flag(owner.is_some_and(|o| o.2))),
                    ("ispartition", flag(false)),
                ]);
            }
        }
        Kind::Types => {
            let mut enums: Vec<_> = dm.model.enums.iter().collect();
            enums.sort_by_key(|e| e.oid());
            for e in enums {
                rows.push(vec![
                    ("tableoid", s(PG_TYPE)),
                    ("oid", s(e.oid())),
                    ("typname", s(&e.name)),
                    ("typnamespace", s(catalog_model::NS_PUBLIC)),
                    ("typacl", None),
                    ("acldefault", None),
                    ("typowner", s(catalog_model::BOOTSTRAP_ROLE)),
                    ("typelem", s(0)),
                    ("typrelid", s(0)),
                    ("typarray", s(e.oid() + 1)),
                    ("typrelkind", s(" ")),
                    ("typtype", s("e")),
                    ("typisdefined", flag(true)),
                    ("isarray", flag(false)),
                ]);
            }
        }
        Kind::EnumLabels(oid) => {
            if let Some(e) = dm.model.enums.iter().find(|e| e.oid() == *oid) {
                for label in &e.labels {
                    rows.push(vec![
                        ("oid", s(catalog_model::oid_of(&format!("{}.{label}", e.name)))),
                        ("enumlabel", s(label)),
                        ("tableoid", s(PG_ENUM)),
                    ]);
                }
            }
        }
        Kind::Attributes => {
            for rel in listed(dm, &listed_oids(sql)) {
                for c in &rel.columns {
                    let storage = if c.len == -1 { "x" } else { "p" };
                    rows.push(vec![
                        ("attrelid", s(rel.oid)),
                        ("attnum", s(c.attnum)),
                        ("attname", s(&c.name)),
                        ("attstattarget", None),
                        ("attstorage", s(storage)),
                        ("typstorage", s(storage)),
                        ("atthasdef", flag(c.identity.is_empty() && c.default.is_some() && matches!(rel.kind, "r" | "p"))),
                        ("attisdropped", flag(false)),
                        ("attlen", s(c.len)),
                        ("attalign", s("i")),
                        ("attislocal", flag(true)),
                        ("atttypname", s(type_name(dm, c.type_oid, c.typmod))),
                        ("attoptions", s("")),
                        ("attversion", None),
                        ("attcollation", s(0)),
                        ("attfdwoptions", s("")),
                        ("notnull_name", c.not_null.then(String::new)),
                        ("notnull_comment", None),
                        ("notnull_invalidoid", None),
                        ("notnull_noinherit", flag(false)),
                        ("notnull_islocal", flag(true)),
                        ("attcompression", s("")),
                        ("attidentity", s(c.identity)),
                        ("attmissingval", None),
                        ("attgenerated", s(c.generated)),
                    ]);
                }
            }
        }
        Kind::Defaults => {
            for rel in listed(dm, &listed_oids(sql)) {
                for (attnum, column, text) in defaults_of(rel) {
                    rows.push(vec![
                        ("tableoid", s(PG_ATTRDEF)),
                        ("oid", s(attrdef_oid(rel, column))),
                        ("adrelid", s(rel.oid)),
                        ("adnum", s(attnum)),
                        ("adsrc", Some(text)),
                    ]);
                }
            }
        }
        Kind::Checks => {
            for rel in listed(dm, &listed_oids(sql)) {
                let mut checks: Vec<_> = rel.constraints.iter().filter(|c| c.contype == "c").collect();
                checks.sort_by(|a, b| a.name.cmp(&b.name));
                for c in checks {
                    rows.push(vec![
                        ("tableoid", s(PG_CONSTRAINT)),
                        ("oid", s(c.oid)),
                        ("conrelid", s(rel.oid)),
                        ("conname", s(&c.name)),
                        ("consrc", s(&c.definition)),
                        ("conislocal", flag(true)),
                        ("convalidated", flag(true)),
                    ]);
                }
            }
        }
        Kind::Indexes => {
            for rel in listed(dm, &listed_oids(sql)) {
                let mut entries: Vec<Vec<(&'static str, Cell)>> = Vec::new();
                let mut names: Vec<String> = Vec::new();
                for index in &rel.indexes {
                    let constraint = rel
                        .constraints
                        .iter()
                        .find(|c| matches!(c.contype, "p" | "u") && c.index == index.oid);
                    let keys = index.keys.iter().map(i64::to_string).collect::<Vec<_>>().join(" ");
                    names.push(index.name.clone());
                    entries.push(index_row(
                        rel,
                        index.oid,
                        &index.name,
                        &index.definition,
                        &keys,
                        index.keys.len(),
                        constraint.map(|c| (c.contype, c.name.as_str(), c.oid, c.definition.as_str())),
                    ));
                }
                if let Some((oid, name, definition)) = partition_index(dm, rel) {
                    let keys = rel.partition_key.iter().map(i64::to_string).collect::<Vec<_>>().join(" ");
                    names.push(name.clone());
                    entries.push(index_row(rel, oid, &name, &definition, &keys, rel.partition_key.len(), None));
                }
                let mut order: Vec<usize> = (0..entries.len()).collect();
                order.sort_by(|a, b| names[*a].cmp(&names[*b]));
                let mut entries: Vec<Option<Vec<(&'static str, Cell)>>> = entries.into_iter().map(Some).collect();
                for at in order {
                    if let Some(row) = entries[at].take() {
                        rows.push(row);
                    }
                }
            }
        }
        Kind::ForeignKeys => {
            for rel in listed(dm, &listed_oids(sql)) {
                let mut keys: Vec<_> = rel.constraints.iter().filter(|c| c.contype == "f").collect();
                keys.sort_by(|a, b| a.name.cmp(&b.name));
                for c in keys {
                    let parent = dm.rel(c.foreign_table).map(|r| r.name.clone()).unwrap_or_default();
                    rows.push(vec![
                        ("tableoid", s(PG_CONSTRAINT)),
                        ("oid", s(c.oid)),
                        ("conrelid", s(rel.oid)),
                        ("conname", s(&c.name)),
                        ("confrelid", s(c.foreign_table)),
                        ("conindid", s(0)),
                        ("condef", s(qualified_references(&c.definition, &parent))),
                    ]);
                }
            }
        }
        Kind::Dependencies => {
            let mut deps: Vec<(i64, i64, i64, i64, &'static str)> = Vec::new();
            for (view, (_, reads)) in &dm.views {
                for dep in reads {
                    deps.push((PG_CLASS, *view, PG_CLASS, *dep, "n"));
                }
            }
            for rel in &dm.model.rels {
                for (_, column, text) in defaults_of(rel) {
                    for name in crate::core::rql::sequence_catalog::named_in(&text) {
                        if let Some(seq) = dm.rel_named(&unqualified(&name)).filter(|r| r.kind == "S") {
                            deps.push((PG_ATTRDEF, attrdef_oid(rel, column), PG_CLASS, seq.oid, "n"));
                        }
                    }
                }
            }
            for seq in &dm.sequences {
                if let Some((table, _, identity)) = seq.owner {
                    deps.push((PG_CLASS, seq.oid, PG_CLASS, table, if identity { "i" } else { "a" }));
                }
            }
            deps.sort();
            deps.dedup();
            for (classid, objid, refclassid, refobjid, deptype) in deps {
                rows.push(vec![
                    ("classid", s(classid)),
                    ("objid", s(objid)),
                    ("refclassid", s(refclassid)),
                    ("refobjid", s(refobjid)),
                    ("deptype", s(deptype)),
                ]);
            }
        }
        Kind::Sequences => {
            // By position: the list ends `NULL, 'f'` (no value read with the
            // definition) or `last_value, is_called`.
            let names = select_names(sql);
            let with_values = names.iter().any(|n| n == "last_value");
            let mut sequences: Vec<&Sequence> = dm.sequences.iter().collect();
            sequences.sort_by_key(|q| q.oid);
            return Rows::Positional(
                sequences
                    .into_iter()
                    .map(|q| {
                        vec![
                            s(q.oid),
                            s(&q.data_type),
                            s(q.start),
                            s(q.increment),
                            s(q.max),
                            s(q.min),
                            s(q.cache.max(1)),
                            flag(q.cycle),
                            if with_values { s(q.last.unwrap_or(q.start)) } else { None },
                            if with_values { flag(q.last.is_some()) } else { s("f") },
                        ]
                    })
                    .collect(),
            );
        }
        Kind::SequenceData(name) => {
            if let Some(q) = dm.sequences.iter().find(|q| q.name == *name || q.name.eq_ignore_ascii_case(name)) {
                rows.push(vec![("last_value", s(q.last.unwrap_or(q.start))), ("is_called", flag(q.last.is_some()))]);
            }
        }
        Kind::Database => {
            // As `pg_database` lists it (`catalog_views`).
            rows.push(vec![
                ("tableoid", s(1262)),
                ("oid", s(catalog_model::oid_of(&dm.database))),
                ("datname", s(&dm.database)),
                ("datdba", s(catalog_model::BOOTSTRAP_ROLE)),
                ("encoding", s("UTF8")),
                ("datcollate", s("en_US.UTF-8")),
                ("datctype", s("en_US.UTF-8")),
                ("datfrozenxid", s(0)),
                ("datacl", None),
                ("acldefault", None),
                ("datistemplate", flag(false)),
                ("datconnlimit", s(-1)),
                ("datminmxid", s(0)),
                ("datlocprovider", s("c")),
                ("datlocale", None),
                ("datcollversion", None),
                ("daticurules", None),
                ("tablespace", s("pg_default")),
                ("description", None),
            ]);
        }
        Kind::Descriptions => {
            // `public`'s comment as `initdb` sets it, which `pg_dump` leaves
            // out of a dump; with none, it writes `COMMENT ON SCHEMA public
            // IS ''` to clear the target's.
            rows.push(vec![
                ("description", s("standard public schema")),
                ("classoid", s(PG_NAMESPACE)),
                ("objoid", s(catalog_model::NS_PUBLIC)),
                ("objsubid", s(0)),
            ]);
        }
        Kind::ViewDefinition(oid) => {
            if let Some((definition, _)) = dm.views.get(oid) {
                let text = definition.trim().trim_end_matches(';').trim_end();
                rows.push(vec![("viewdef", s(format!("{text};")))]);
            }
        }
    }
    Rows::Named(rows)
}

fn index_row(
    rel: &Rel,
    oid: i64,
    name: &str,
    definition: &str,
    keys: &str,
    count: usize,
    constraint: Option<(&str, &str, i64, &str)>,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("tableoid", s(PG_CLASS)),
        ("oid", s(oid)),
        ("indrelid", s(rel.oid)),
        ("indexname", s(name)),
        ("relpages", s(0)),
        ("reltuples", s(0)),
        ("relallvisible", s(0)),
        ("relallfrozen", s(0)),
        ("indexdef", s(definition)),
        ("indkey", s(keys)),
        ("indisclustered", flag(false)),
        ("contype", constraint.map(|c| c.0.to_string())),
        ("conname", constraint.map(|c| c.1.to_string())),
        ("condeferrable", constraint.map(|_| "f".to_string())),
        ("condeferred", constraint.map(|_| "f".to_string())),
        ("contableoid", constraint.map(|_| PG_CONSTRAINT.to_string())),
        ("conoid", constraint.map(|c| c.2.to_string())),
        ("condef", constraint.map(|c| c.3.to_string())),
        ("indattnames", None),
        ("tablespace", None),
        ("indreloptions", None),
        ("indisreplident", flag(false)),
        ("parentidx", s(0)),
        ("indnkeyatts", s(count)),
        ("indnatts", s(count)),
        ("indstatcols", None),
        ("indstatvals", None),
        ("indnullsnotdistinct", flag(false)),
        ("conperiod", None),
    ]
}

/// The answer to `sql` as text cells, under the statement's own column
/// names.
fn answer_rows(kind: &Kind, sql: &str, dm: &DumpModel) -> (Vec<String>, Vec<Vec<Cell>>) {
    let mut names = select_names(sql);
    match rows_of(kind, sql, dm) {
        Rows::Named(rows) => {
            let rows = rows
                .into_iter()
                .map(|row| {
                    names
                        .iter()
                        .map(|n| row.iter().find(|(k, _)| k.eq_ignore_ascii_case(n)).and_then(|(_, v)| v.clone()))
                        .collect()
                })
                .collect();
            (names, rows)
        }
        Rows::Positional(rows) => {
            let width = rows.first().map_or(names.len(), Vec::len);
            names.resize(width, "?column?".to_string());
            (names, rows)
        }
    }
}

/// Answers one of `pg_dump`'s catalogue queries ([`recognize`]).
pub(super) async fn answer<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    sql: &str,
    kind: Kind,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (names, rows) = if kind == Kind::Empty {
        let names = select_names(sql);
        let rows = if counts_only(sql) { vec![vec![s(0); names.len()]] } else { Vec::new() };
        (names, rows)
    } else {
        let dm = match dump_model(database, state).await {
            Ok(dm) => dm,
            Err(e) => return super::fail_statement(state, stream, super::sqlstate_for(&e), &e).await,
        };
        // What the dump leaves out, said once, as it lists the relations.
        // `pg_dump` shows a server's notices only under `--verbose`;
        // docs/postgres-import.md lists them too.
        if kind == Kind::Tables && !dm.not_dumped.is_empty() {
            let message = format!(
                "this dump leaves out what pg_dump cannot read from RasterizedDB yet; recreate it after a restore: {}",
                dm.not_dumped.join(", ")
            );
            log::warn!("pg_dump of {}: {message}", dm.database);
            super::send_notice(stream, "01000", &message).await?;
        }
        answer_rows(&kind, sql, &dm)
    };
    if !omit_description {
        let fields: Vec<(String, DbType)> = names.iter().map(|n| (n.clone(), DbType::STRING)).collect();
        super::send_row_description(stream, &fields, &[]).await?;
    }
    for row in &rows {
        let cells: Vec<Option<&[u8]>> = row.iter().map(|c| c.as_deref().map(str::as_bytes)).collect();
        super::send_data_row(stream, &cells).await?;
    }
    super::send_command_complete(stream, &format!("SELECT {}", rows.len())).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_postgresql() {
        let names = select_names(
            "SELECT c.tableoid, c.oid, acldefault('r'::\"char\", c.relowner) AS acldefault, 0 AS relallfrozen, \
             oprcode::oid AS oprcode, prsstart::oid, fdwhandler::pg_catalog.regproc, \
             pg_catalog.format_type(t.oid, a.atttypmod) AS atttypname, format_type(seqtypid, NULL), NULL, 'f', \
             (d.deptype = 'i') IS TRUE AS is_identity_sequence FROM pg_class c",
        );
        assert_eq!(
            names,
            [
                "tableoid",
                "oid",
                "acldefault",
                "relallfrozen",
                "oprcode",
                "prsstart",
                "fdwhandler",
                "atttypname",
                "format_type",
                "?column?",
                "?column?",
                "is_identity_sequence"
            ]
        );
        assert_eq!(select_names("SELECT pg_catalog.pg_get_viewdef('16431'::pg_catalog.oid) AS viewdef"), ["viewdef"]);
        assert_eq!(select_names("SELECT DISTINCT attrelid FROM pg_attribute WHERE attacl IS NOT NULL"), ["attrelid"]);
    }

    #[test]
    fn defaults_name_their_sequences_qualified() {
        assert_eq!(pg_default("nextval('customers_id_seq')"), "nextval('public.customers_id_seq'::regclass)");
        assert_eq!(pg_default("NEXTVAL('Odd')"), "nextval('public.odd'::regclass)");
        assert_eq!(pg_default("nextval('\"Odd\"'::regclass)"), "nextval('public.\"Odd\"'::regclass)");
        assert_eq!(pg_default("nextval('public.s'::regclass)"), "nextval('public.s'::regclass)");
        assert_eq!(pg_default("'nextval(''x'')'"), "'nextval(''x'')'");
        assert_eq!(pg_default("now()"), "now()");
    }

    #[test]
    fn view_queries_name_relations_qualified() {
        let rels = vec!["customers".to_string(), "orders".to_string(), "Mixed".to_string()];
        assert_eq!(
            qualify_relations("SELECT id, email FROM customers WHERE active", &rels),
            "SELECT id, email FROM public.customers WHERE active"
        );
        assert_eq!(
            qualify_relations(
                "SELECT c.id FROM customers c JOIN orders o ON o.customer_id = c.id, \"Mixed\" WHERE c.id IN (SELECT customer_id FROM orders)",
                &rels
            ),
            "SELECT c.id FROM public.customers c JOIN public.orders o ON o.customer_id = c.id, public.\"Mixed\" WHERE c.id IN (SELECT customer_id FROM public.orders)"
        );
        // A column or a string that happens to share a name is left alone.
        assert_eq!(
            qualify_relations("SELECT orders, 'from customers' FROM other WHERE orders > 1", &rels),
            "SELECT orders, 'from customers' FROM other WHERE orders > 1"
        );
    }

    #[test]
    fn foreign_keys_reference_qualified_tables() {
        assert_eq!(
            qualified_references("FOREIGN KEY (\"customer_id\") REFERENCES \"customers\"(\"id\") ON DELETE CASCADE", "customers"),
            "FOREIGN KEY (\"customer_id\") REFERENCES public.customers(\"id\") ON DELETE CASCADE"
        );
        assert_eq!(
            qualified_references("FOREIGN KEY (a) REFERENCES Parent(id)", "Parent"),
            "FOREIGN KEY (a) REFERENCES public.\"Parent\"(id)"
        );
        // A composite key, its MATCH and its action's column list kept.
        assert_eq!(
            qualified_references(
                "FOREIGN KEY (\"a\", \"b\") REFERENCES \"p\"(\"x\", \"y\") MATCH FULL ON DELETE SET NULL (\"b\")",
                "p"
            ),
            "FOREIGN KEY (\"a\", \"b\") REFERENCES public.p(\"x\", \"y\") MATCH FULL ON DELETE SET NULL (\"b\")"
        );
    }

    /// Every catalogue query `pg_dump` 18 sent a PostgreSQL 17 server while
    /// dumping tables, a partitioned table, sequences and a view (its
    /// statement log), each recognised as the query it is.
    #[test]
    fn pg_dump_18_queries_are_recognised() {
        let cases: &[(&str, Kind)] = &[
            ("SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'", Kind::Empty),
            ("SELECT oid, rolname FROM pg_catalog.pg_roles ORDER BY 1", Kind::Roles),
            ("SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition FROM pg_extension x JOIN pg_namespace n ON n.oid = x.extnamespace", Kind::Empty),
            ("SELECT classid, objid, refobjid FROM pg_depend WHERE refclassid = 'pg_extension'::regclass AND deptype = 'e' ORDER BY 3", Kind::Empty),
            ("SELECT n.tableoid, n.oid, n.nspname, n.nspowner, n.nspacl, acldefault('n', n.nspowner) AS acldefault FROM pg_namespace n", Kind::Namespaces),
            ("SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, c.relpersistence FROM pg_class c\nLEFT JOIN pg_depend d ON (c.relkind = 'S')\nWHERE c.relkind IN ('r', 'S', 'v', 'c', 'm', 'f', 'p')\nORDER BY c.oid", Kind::Tables),
            ("SELECT p.tableoid, p.oid, p.proname FROM pg_proc p LEFT JOIN pg_init_privs pip ON (p.oid = pip.objoid) WHERE p.prokind <> 'a'", Kind::Empty),
            ("SELECT tableoid, oid, typname, typnamespace, typacl, CASE WHEN typrelid = 0 THEN ' '::\"char\" ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, typtype FROM pg_type", Kind::Types),
            ("SELECT tableoid, oid, amname, amtype, amhandler::pg_catalog.regproc AS amhandler FROM pg_am", Kind::Empty),
            ("SELECT inhrelid, inhparent FROM pg_inherits", Kind::Empty),
            ("SELECT\na.attrelid,\na.attnum\nFROM unnest('{16391,16405}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_attribute a ON (src.tbloid = a.attrelid) LEFT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)\nWHERE a.attnum > 0::pg_catalog.int2\nORDER BY a.attrelid, a.attnum", Kind::Attributes),
            ("SELECT a.tableoid, a.oid, adrelid, adnum, pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc\nFROM unnest('{16391,16405}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_attrdef a ON (src.tbloid = a.adrelid)\nORDER BY a.adrelid, a.adnum", Kind::Defaults),
            ("SELECT c.tableoid, c.oid, conrelid, conname, pg_catalog.pg_get_constraintdef(c.oid) AS consrc, conislocal, convalidated FROM unnest('{16391}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_constraint c ON (src.tbloid = c.conrelid)\nWHERE contype = 'c' ORDER BY c.conrelid, c.conname", Kind::Checks),
            ("SELECT partrelid FROM pg_partitioned_table WHERE\n(SELECT c.oid FROM pg_opclass c JOIN pg_am a ON c.opcmethod = a.oid\nWHERE opcname = 'enum_ops') = ANY(partclass)", Kind::Empty),
            ("SELECT t.tableoid, t.oid, i.indrelid FROM unnest('{16391}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_index i ON (src.tbloid = i.indrelid) JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid)", Kind::Indexes),
            ("SELECT c.tableoid, c.oid, conrelid, conname, confrelid, conindid, pg_catalog.pg_get_constraintdef(c.oid) AS condef\nFROM unnest('{16389}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_constraint c ON (src.tbloid = c.conrelid)\nWHERE contype = 'f' AND conparentid = 0 ORDER BY conrelid, conname", Kind::ForeignKeys),
            ("SELECT t.tgrelid, t.tgname FROM unnest('{16391}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_trigger t ON (src.tbloid = t.tgrelid)", Kind::Empty),
            ("SELECT classid, objid, refclassid, refobjid, deptype FROM pg_depend WHERE deptype != 'p' AND deptype != 'e'\nUNION ALL\nSELECT 'pg_opfamily'::regclass AS classid FROM pg_depend d, pg_amop o", Kind::Dependencies),
            ("SELECT DISTINCT attrelid FROM pg_attribute WHERE attacl IS NOT NULL", Kind::Empty),
            ("SELECT description, classoid, objoid, objsubid FROM pg_catalog.pg_description ORDER BY classoid, objoid, objsubid", Kind::Descriptions),
            ("SELECT seqrelid, format_type(seqtypid, NULL), seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, NULL, 'f' FROM pg_catalog.pg_sequence ORDER BY seqrelid", Kind::Sequences),
            ("SELECT at.attname, at.attacl, '{}' AS acldefault, pip.privtype, pip.initprivs FROM pg_catalog.pg_attribute at LEFT JOIN pg_catalog.pg_init_privs pip ON (at.attrelid = pip.objoid) WHERE at.attrelid = '6100'", Kind::Empty),
            ("SELECT pg_catalog.pg_get_viewdef('16431'::pg_catalog.oid) AS viewdef", Kind::ViewDefinition(16431)),
            ("SELECT last_value, is_called FROM public.customers_id_seq", Kind::SequenceData("customers_id_seq".to_string())),
            ("SELECT oid, enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = '17000'::pg_catalog.oid ORDER BY enumsortorder", Kind::EnumLabels(17000)),
            // As `EXECUTE dumpEnumType('17000')` binds its `PREPARE`.
            ("SELECT oid, enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = CAST('17000' AS pg_catalog.oid) ORDER BY enumsortorder", Kind::EnumLabels(17000)),
        ];
        for (sql, kind) in cases {
            assert_eq!(recognize(sql).as_ref(), Some(kind), "{sql}");
        }
        // What every other client sends runs as written.
        for sql in [
            "SELECT * FROM pg_class",
            "SELECT relname FROM pg_class c WHERE c.relkind IN ('r', 'p')",
            "SELECT pg_catalog.current_schemas(false)",
            "COPY public.t (a) TO stdout;",
            "LOCK TABLE public.t IN ACCESS SHARE MODE",
            "SELECT id FROM sales",
        ] {
            assert_eq!(recognize(sql), None, "{sql}");
        }
        assert_eq!(listed_oids("FROM unnest('{16391,16405, 7}'::pg_catalog.oid[]) AS src"), [16391, 16405, 7]);
    }
}
