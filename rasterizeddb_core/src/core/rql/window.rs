//! Window functions: `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)`.
//!
//! A window function is not an aggregate. An aggregate folds many rows into
//! one; a window function keeps every row and adds a column computed from the
//! rows around it. So it runs after the scan and after `WHERE`, over the rows
//! that survived, and before `LIMIT`.
//!
//! # What it is for
//!
//! The shape the application uses is fairness:
//!
//! ```sql
//! SELECT id FROM (
//!     SELECT id, ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY created_at)
//!            AS domain_rank
//!     FROM task WHERE status = 'pending'
//! ) ranked
//! WHERE ranked.domain_rank <= 4
//! ```
//!
//! Numbering each tenant's rows independently and taking the first few of each
//! is "the first N per tenant", which `ORDER BY … LIMIT` cannot express — that
//! gives the first N overall, and one busy tenant then starves the rest.

use crate::core::db_type::DbType;
use crate::core::row::row::Row;
use crate::core::row::schema::SchemaField;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunc {
    /// 1, 2, 3 … within each partition, ties broken arbitrarily but stably.
    RowNumber,
    /// Ties share a number and the next value skips: 1, 1, 3.
    Rank,
    /// Ties share a number and the next value does not skip: 1, 1, 2.
    DenseRank,
}

impl WindowFunc {
    fn parse(name: &str) -> Option<Self> {
        Some(match name.trim().to_ascii_uppercase().as_str() {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            _ => return None,
        })
    }
}

/// One window function in a select list, resolved against the schema.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub func: WindowFunc,
    /// Rows are numbered independently within each distinct value of these.
    pub partition_by: Vec<SchemaField>,
    /// The order rows are numbered in, each with its own direction.
    pub order_by: Vec<(SchemaField, bool)>,
    pub label: String,
}

impl WindowSpec {
    /// Every column the window reads, so the scan fetches them.
    pub fn columns(&self) -> Vec<SchemaField> {
        let mut out = self.partition_by.clone();
        for (field, _) in &self.order_by {
            if !out.iter().any(|f| f.name == field.name) {
                out.push(field.clone());
            }
        }
        out
    }

    pub fn result_type(&self) -> DbType {
        DbType::I64
    }
}

/// Parses `<func>() OVER (…)`, or `None` when the item is not a window call.
///
/// The empty argument list is required: `ROW_NUMBER`, `RANK` and `DENSE_RANK`
/// take none, and a call with arguments is a different function that this does
/// not implement — refused rather than answered as though the arguments were
/// not there.
pub fn parse(item: &str, schema: &[SchemaField]) -> Option<Result<WindowSpec, String>> {
    let (expr, alias) = split_alias(item);

    let open = expr.find('(')?;
    let name = expr[..open].trim();
    let func = WindowFunc::parse(name)?;

    let close = match matching_paren(expr, open) {
        Some(c) => c,
        None => return Some(Err(format!("unterminated call in '{}'", item))),
    };
    if !expr[open + 1..close].trim().is_empty() {
        return Some(Err(format!("{}() takes no arguments", name)));
    }

    let rest = expr[close + 1..].trim();
    if !rest.to_ascii_uppercase().starts_with("OVER") {
        return Some(Err(format!(
            "{}() is a window function and needs an OVER clause",
            name
        )));
    }
    let after_over = rest["OVER".len()..].trim_start();
    let Some(window_open) = after_over.find('(') else {
        return Some(Err(format!("OVER needs a window definition in '{}'", item)));
    };
    let Some(window_close) = matching_paren(after_over, window_open) else {
        return Some(Err(format!("unterminated OVER clause in '{}'", item)));
    };
    if !after_over[window_close + 1..].trim().is_empty() {
        return Some(Err(format!(
            "'{}' is not part of a window definition",
            after_over[window_close + 1..].trim()
        )));
    }

    let body = after_over[window_open + 1..window_close].trim();
    Some(build(func, body, alias, name, schema))
}

fn build(
    func: WindowFunc,
    body: &str,
    alias: Option<String>,
    name: &str,
    schema: &[SchemaField],
) -> Result<WindowSpec, String> {
    let upper = body.to_ascii_uppercase();
    let partition_at = find_word(&upper, "PARTITION BY");
    let order_at = find_word(&upper, "ORDER BY");

    // A frame clause changes which rows the function sees. These three ignore
    // the frame, so accepting one silently would be harmless for them and a
    // trap for anything added later — it is refused instead.
    if find_word(&upper, "ROWS").is_some() || find_word(&upper, "RANGE").is_some() {
        return Err("a window frame (ROWS/RANGE) is not supported yet".to_string());
    }

    let resolve = |list: &str| -> Result<Vec<(SchemaField, bool)>, String> {
        let mut out = Vec::new();
        for raw in list.split(',') {
            let term = raw.trim();
            if term.is_empty() {
                continue;
            }
            let mut words = term.split_whitespace();
            let column = words.next().unwrap_or("");
            let descending = match words.next() {
                Some(d) if d.eq_ignore_ascii_case("DESC") => true,
                Some(d) if d.eq_ignore_ascii_case("ASC") => false,
                None => false,
                Some(other) => {
                    return Err(format!("'{}' is not a sort direction", other));
                }
            };
            let bare = column.rsplit('.').next().unwrap_or(column).trim_matches('"');
            let field = schema
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
                .ok_or_else(|| format!("Column '{}' not found in schema", column))?;
            out.push((field.clone(), descending));
        }
        Ok(out)
    };

    let (partition_text, order_text) = match (partition_at, order_at) {
        (Some(p), Some(o)) if o > p => (
            &body[p + "PARTITION BY".len()..o],
            &body[o + "ORDER BY".len()..],
        ),
        (Some(p), None) => (&body[p + "PARTITION BY".len()..], ""),
        (None, Some(o)) => ("", &body[o + "ORDER BY".len()..]),
        (None, None) => ("", ""),
        // `ORDER BY` before `PARTITION BY` is not valid SQL.
        (Some(_), Some(_)) => {
            return Err("PARTITION BY must come before ORDER BY".to_string())
        }
    };

    let partition_by = resolve(partition_text)?
        .into_iter()
        .map(|(f, _)| f)
        .collect();
    let order_by = resolve(order_text)?;

    Ok(WindowSpec {
        func,
        partition_by,
        order_by,
        label: alias.unwrap_or_else(|| name.to_ascii_lowercase()),
    })
}

/// The window value for each row, in the order the rows were given.
///
/// Rows are ranked within their partition, and the result is placed back at the
/// row's original position — the caller's row order is not disturbed, because
/// the statement's own `ORDER BY` decides that and runs later.
pub fn assign(rows: &[Row], spec: &WindowSpec) -> Vec<i64> {
    let key_of = |row: &Row, fields: &[SchemaField]| -> Vec<Vec<u8>> {
        fields
            .iter()
            .map(|f| {
                row.columns
                    .iter()
                    .find(|c| c.schema_id == f.write_order)
                    .map(|c| c.data.into_slice().to_vec())
                    .unwrap_or_default()
            })
            .collect()
    };

    // Group the row indices by partition, preserving first-seen order so the
    // result is deterministic for equal keys.
    let mut partitions: Vec<(Vec<Vec<u8>>, Vec<usize>)> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let key = key_of(row, &spec.partition_by);
        match partitions.iter_mut().find(|(k, _)| *k == key) {
            Some((_, members)) => members.push(index),
            None => partitions.push((key, vec![index])),
        }
    }

    let mut out = vec![0i64; rows.len()];
    for (_, mut members) in partitions {
        // A stable sort, so rows equal on every key keep the order they were
        // scanned in rather than an arbitrary one that could differ per run.
        members.sort_by(|a, b| {
            for (field, descending) in &spec.order_by {
                let lhs = rows[*a]
                    .columns
                    .iter()
                    .find(|c| c.schema_id == field.write_order)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let rhs = rows[*b]
                    .columns
                    .iter()
                    .find(|c| c.schema_id == field.write_order)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let ord = crate::core::processor::concurrent_processor::compare_bytes_as_type(
                    field.db_type.clone(),
                    lhs,
                    rhs,
                );
                let ord = if *descending { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        let same_as_previous = |a: usize, b: usize| -> bool {
            spec.order_by.iter().all(|(field, _)| {
                let lhs = rows[a]
                    .columns
                    .iter()
                    .find(|c| c.schema_id == field.write_order)
                    .map(|c| c.data.into_slice());
                let rhs = rows[b]
                    .columns
                    .iter()
                    .find(|c| c.schema_id == field.write_order)
                    .map(|c| c.data.into_slice());
                lhs == rhs
            })
        };

        let mut rank = 0i64;
        let mut dense = 0i64;
        for (position, index) in members.iter().enumerate() {
            let tied = position > 0 && same_as_previous(*index, members[position - 1]);
            if !tied {
                dense += 1;
                rank = position as i64 + 1;
            }
            out[*index] = match spec.func {
                WindowFunc::RowNumber => position as i64 + 1,
                // With no ORDER BY every row ties, which is what makes RANK
                // and DENSE_RANK 1 for all of them — as in PostgreSQL.
                WindowFunc::Rank => rank.max(1),
                WindowFunc::DenseRank => dense.max(1),
            };
        }
    }
    out
}

/// Splits a trailing alias off a window item.
fn split_alias(item: &str) -> (&str, Option<String>) {
    let Some(close) = item.rfind(')') else {
        return (item.trim(), None);
    };
    let tail = item[close + 1..].trim();
    if tail.is_empty() {
        return (item[..close + 1].trim(), None);
    }
    let mut words = tail.split_whitespace();
    let alias = match words.next() {
        Some(w) if w.eq_ignore_ascii_case("AS") => words.next(),
        Some(w) => Some(w),
        None => None,
    };
    (
        item[..close + 1].trim(),
        alias.map(|a| a.trim_matches('"').to_string()),
    )
}

fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Finds a keyword phrase, tolerating any whitespace between its words.
fn find_word(upper: &str, phrase: &str) -> Option<usize> {
    let words: Vec<&str> = phrase.split(' ').collect();
    let mut from = 0usize;
    while let Some(at) = upper[from..].find(words[0]) {
        let start = from + at;
        let mut cursor = start + words[0].len();
        let mut ok = start == 0
            || !upper.as_bytes()[start - 1].is_ascii_alphanumeric();
        for word in &words[1..] {
            let rest = &upper[cursor..];
            let trimmed = rest.trim_start();
            if trimmed.len() == rest.len() || !trimmed.starts_with(word) {
                ok = false;
                break;
            }
            cursor += (rest.len() - trimmed.len()) + word.len();
        }
        if ok {
            return Some(start);
        }
        from = start + words[0].len();
    }
    None
}
