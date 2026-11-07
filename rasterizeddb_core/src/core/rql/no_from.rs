//! A `SELECT` with no `FROM`: `SELECT 1 + 1`, `SELECT now()`, `SELECT
//! '2024-02-29'::date`, `SELECT EXTRACT(YEAR FROM now())`. One row, each item
//! evaluated with no columns to read, as PostgreSQL evaluates it.
//!
//! Only `SELECT 1` was answered (a special case in the wire layer); every
//! other such statement was "syntax error or unsupported query", which is what
//! a client's or an ORM's probe of the server got.
//!
//! Answered above the engine, as `rdb_index_checkpoint` is: the wire layer
//! describes it with [`NoFrom::columns`] and sends [`NoFrom::row`], both from
//! one parse, so the description and the values cannot disagree. A `WHERE`
//! must be constant, and `LIMIT`/`OFFSET` take constants; `ORDER BY` over one
//! row changes nothing. Anything else (`GROUP BY`, `INTO`, a set operation, an
//! aggregate) is not this shape and is left to the paths that handle or refuse
//! it.

use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};
use crate::core::rql::expr::{self, Expr, Value};
use crate::core::rql::join::find_keyword;

/// One parsed `SELECT` without `FROM`.
pub(crate) struct NoFrom {
    items: Vec<Item>,
    /// `false` when a constant `WHERE` is not true, or `LIMIT 0`, or an
    /// `OFFSET` past the one row: no row, the columns still described.
    emits: bool,
}

struct Item {
    label: String,
    expr: Expr,
    db_type: DbType,
    scale: Option<u32>,
}

/// Clauses after which the statement is not this shape.
const FOREIGN: &[&str] = &["INTO", "GROUP", "HAVING", "WINDOW", "UNION", "INTERSECT", "EXCEPT", "FOR", "FETCH"];

/// `None` when `stmt` is not a `SELECT` without `FROM`; `Some(Err)` when it is
/// one this cannot evaluate, with the reason.
pub(crate) fn parse(stmt: &str) -> Option<Result<NoFrom, String>> {
    let body = stmt.trim().trim_end_matches(';').trim_end();
    let head = body.get(..6)?;
    if !head.eq_ignore_ascii_case("SELECT") || !body[6..].starts_with(char::is_whitespace) {
        return None;
    }
    if find_keyword(body, "FROM", 6).is_some() || FOREIGN.iter().any(|k| find_keyword(body, k, 6).is_some()) {
        return None;
    }
    let rest = &body[6..];
    // The clause boundaries, in the order SQL writes them.
    let at = |k: &str| find_keyword(rest, k, 0);
    let (where_at, order_at, limit_at, offset_at) = (at("WHERE"), at("ORDER"), at("LIMIT"), at("OFFSET"));
    let ends: Vec<usize> = [where_at, order_at, limit_at, offset_at].into_iter().flatten().collect();
    let list_end = ends.iter().copied().min().unwrap_or(rest.len());
    let clause = |start: Option<usize>, word: &str| -> Option<&str> {
        let start = start? + word.len();
        let end = ends.iter().copied().filter(|e| *e >= start).min().unwrap_or(rest.len());
        Some(rest[start..end].trim())
    };
    let list = rest[..list_end].trim();
    if list.is_empty() {
        // `SELECT` alone is PostgreSQL's one row of no columns; nothing reads it.
        return None;
    }
    let distinct = list.get(..9).is_some_and(|h| h.eq_ignore_ascii_case("DISTINCT "));
    let list = if distinct { &list[9..] } else { list };
    let list = match list.get(..4) {
        Some(h) if h.eq_ignore_ascii_case("ALL ") => &list[4..],
        _ => list,
    };
    Some(build(list, clause(where_at, "WHERE"), clause(limit_at, "LIMIT"), clause(offset_at, "OFFSET")))
}

fn build(list: &str, condition: Option<&str>, limit: Option<&str>, offset: Option<&str>) -> Result<NoFrom, String> {
    let mut items = Vec::new();
    for raw in crate::core::rql::lexer_ct::split_definitions(list) {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("the SELECT list has an empty item".to_string());
        }
        if raw == "*" || raw.ends_with(".*") {
            return Err("SELECT * with no tables specified is not valid".to_string());
        }
        let (text, alias) = crate::core::rql::lexer_query::split_alias(raw);
        let text = typed_literals(text);
        // A comparison is a boolean value, which the expression grammar does
        // not read; tried once that has failed, as the select list does.
        let parsed = match expr::parse(&text, &[]) {
            Ok(parsed) => parsed,
            Err(e) => crate::core::rql::join::comparison_value(&text, &[])?.ok_or_else(|| no_column(e, &text))?,
        };
        if !expr::columns_of(&parsed).is_empty() {
            return Err(format!("column \"{}\" does not exist", text.trim()));
        }
        items.push(Item {
            label: alias.unwrap_or_else(|| crate::core::rql::join::pg_column_name(&text)),
            // A `timestamp` without time zone is described as one, so it is
            // shown without an offset, as a row expression's is.
            db_type: if expr::yields_timestamp(&parsed) { DbType::TIMESTAMP } else { expr::static_type(&parsed) },
            scale: expr::display_scale(&parsed),
            expr: parsed,
        });
    }
    let mut emits = true;
    if let Some(condition) = condition {
        let cond = expr::parse_cond(&typed_literals(condition), &[]).map_err(|e| no_column(e, condition))?;
        emits &= expr::eval_cond(&cond, &Row::default(), &[])?;
    }
    let count = |text: &str, what: &str| -> Result<Option<i64>, String> {
        if text.eq_ignore_ascii_case("ALL") {
            return Ok(None);
        }
        let words: Vec<&str> = text.split_whitespace().collect();
        let n = words.first().ok_or_else(|| format!("{what} needs a value"))?;
        n.parse::<i64>().map(Some).map_err(|_| format!("{what} must be a constant integer here, not '{text}'"))
    };
    if let Some(limit) = limit {
        if count(limit, "LIMIT")? == Some(0) {
            emits = false;
        }
    }
    if let Some(offset) = offset {
        if count(offset, "OFFSET")?.is_some_and(|n| n >= 1) {
            emits = false;
        }
    }
    Ok(NoFrom { items, emits })
}

/// An unknown name in a `SELECT` with no `FROM` is a column that does not
/// exist (42703), as PostgreSQL reports it, rather than the evaluator's own
/// words for an unresolved identifier.
fn no_column(e: String, text: &str) -> String {
    if e.contains("is neither a column nor a value") || e.contains("not found in schema") {
        format!("column \"{}\" does not exist", text.trim())
    } else {
        e
    }
}

/// `DATE '2024-02-29'`, `TIMESTAMP '…'`, `INTERVAL '1 day'`: SQL's typed
/// literal, as the cast it means (`'2024-02-29'::date`). Read outside quotes,
/// at word boundaries only.
fn typed_literals(text: &str) -> String {
    const TYPES: &[&str] = &["TIMESTAMPTZ", "TIMESTAMP", "INTERVAL", "DATE", "TIME"];
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 8);
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            out.push_str(&text[start..i]);
            continue;
        }
        let boundary = i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_' || bytes[i - 1] == b':');
        if boundary && c.is_ascii_alphabetic() {
            let word_end = bytes[i..].iter().position(|b| !(b.is_ascii_alphanumeric() || *b == b'_')).map_or(bytes.len(), |p| i + p);
            let word = &text[i..word_end];
            let after = &text[word_end..];
            let gap = after.len() - after.trim_start().len();
            if TYPES.iter().any(|t| t.eq_ignore_ascii_case(word)) && gap > 0 && after.trim_start().starts_with('\'') {
                // The literal's end, doubled quotes included.
                let lit_start = word_end + gap;
                let mut j = lit_start + 1;
                while j < bytes.len() {
                    if bytes[j] == b'\'' {
                        if bytes.get(j + 1) == Some(&b'\'') {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                if j < bytes.len() {
                    out.push_str(&text[lit_start..=j]);
                    out.push_str("::");
                    out.push_str(&word.to_ascii_lowercase());
                    i = j + 1;
                    continue;
                }
            }
            out.push_str(word);
            i = word_end;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    out
}

impl NoFrom {
    /// The columns, as `Describe` and the row description name and type them.
    pub(crate) fn columns(&self) -> Vec<(String, DbType)> {
        self.items.iter().map(|i| (i.label.clone(), i.db_type.clone())).collect()
    }

    /// Each column's NUMERIC scale: an expression's display scale, as a row
    /// expression's slot is sent at.
    pub(crate) fn scales(&self) -> Vec<u32> {
        self.items
            .iter()
            .map(|i| i.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE))
            .collect()
    }

    /// The one row (none when the `WHERE`, `LIMIT` or `OFFSET` says so), each
    /// value as a row expression's slot holds it: evaluated, coerced to the
    /// described type, at its display scale. A failed evaluation fails the
    /// statement.
    pub(crate) fn rows(&self) -> Result<Vec<Row>, String> {
        if !self.emits {
            return Ok(Vec::new());
        }
        let empty = Row::default();
        let mut row = Row::default();
        for (position, item) in self.items.iter().enumerate() {
            let value = expr::eval_output_planned(&item.expr, expr::output_is_wide(&item.expr), &empty, &[])
                .map(|v| expr::coerce_to_public(v, &item.db_type))
                .and_then(|v| expr::at_display_scale(v, item.scale))?;
            let (bytes, column_type) = match value {
                Value::Data { bytes, .. } if item.db_type == DbType::TIMESTAMP => (bytes, DbType::TIMESTAMP),
                Value::Data { bytes, db_type } => (bytes, db_type),
                Value::Null => (Vec::new(), DbType::NULL),
            };
            let mut data = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
            if !bytes.is_empty() {
                data.into_slice_mut().copy_from_slice(&bytes);
            }
            row.columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(position as u64),
                data,
                column_type,
            });
        }
        Ok(vec![row])
    }
}
