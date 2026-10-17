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
use crate::core::rql::aggregate::AggFunc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunc {
    /// 1, 2, 3 … within each partition, ties broken arbitrarily but stably.
    RowNumber,
    /// Ties share a number and the next value skips: 1, 1, 3.
    Rank,
    /// Ties share a number and the next value does not skip: 1, 1, 2.
    DenseRank,
    /// `(rank - 1) / (partition rows - 1)`, in `[0, 1]`.
    PercentRank,
    /// The fraction of the partition at or before this row.
    CumeDist,
    /// The value `offset` rows earlier in the partition.
    Lag,
    /// The value `offset` rows later.
    Lead,
    /// The first, last or nth value of the frame.
    FirstValue,
    LastValue,
    NthValue,
    /// Aggregates computed over the frame rather than over a group.
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

impl WindowFunc {
    fn parse(name: &str) -> Option<Self> {
        Some(match name.trim().to_ascii_uppercase().as_str() {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            "PERCENT_RANK" => WindowFunc::PercentRank,
            "CUME_DIST" => WindowFunc::CumeDist,
            "LAG" => WindowFunc::Lag,
            "LEAD" => WindowFunc::Lead,
            "FIRST_VALUE" => WindowFunc::FirstValue,
            "LAST_VALUE" => WindowFunc::LastValue,
            "NTH_VALUE" => WindowFunc::NthValue,
            "SUM" => WindowFunc::Sum,
            "AVG" => WindowFunc::Avg,
            "COUNT" => WindowFunc::Count,
            "MIN" => WindowFunc::Min,
            "MAX" => WindowFunc::Max,
            _ => return None,
        })
    }

    /// How many arguments the function takes, as `(minimum, maximum)`.
    fn arity(&self) -> (usize, usize) {
        match self {
            WindowFunc::RowNumber
            | WindowFunc::Rank
            | WindowFunc::DenseRank
            | WindowFunc::PercentRank
            | WindowFunc::CumeDist => (0, 0),
            // `COUNT(*)` takes none and `COUNT(x)` takes one.
            WindowFunc::Count => (0, 1),
            WindowFunc::Lag | WindowFunc::Lead => (1, 2),
            WindowFunc::NthValue => (2, 2),
            _ => (1, 1),
        }
    }

    /// Whether the frame decides which rows the function sees.
    ///
    /// The ranking functions and `LAG`/`LEAD` address the partition directly
    /// and ignore any frame, exactly as PostgreSQL does — which is why a frame
    /// written alongside one of them is accepted rather than refused.
    fn honours_frame(&self) -> bool {
        !matches!(
            self,
            WindowFunc::RowNumber
                | WindowFunc::Rank
                | WindowFunc::DenseRank
                | WindowFunc::PercentRank
                | WindowFunc::CumeDist
                | WindowFunc::Lag
                | WindowFunc::Lead
        )
    }
}

/// Where a frame begins or ends, relative to the current row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMode {
    /// Counts rows.
    Rows,
    /// Counts *peer groups* — rows equal on the `ORDER BY` key.
    Range,
}

/// Which rows around the current one the function sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

impl Frame {
    /// The frame a window with no explicit clause gets.
    ///
    /// With an `ORDER BY` it is everything up to and including the current
    /// row's *peers*; without one it is the whole partition. Getting this
    /// wrong is not cosmetic — a running total and a partition total are
    /// different numbers, and both are spelled `SUM(x) OVER (…)`.
    fn default_for(has_order_by: bool) -> Frame {
        if has_order_by {
            Frame {
                mode: FrameMode::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            }
        } else {
            Frame {
                mode: FrameMode::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            }
        }
    }
}

/// One window function in a select list, resolved against the schema.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub func: WindowFunc,
    /// The column the function reads — `AVG(subtotal)`, `LAG(created_at)`.
    /// `None` for the ranking functions and `COUNT(*)`.
    pub argument: Option<SchemaField>,
    /// The second argument of `LAG`, `LEAD` and `NTH_VALUE`.
    pub offset: i64,
    /// Rows are numbered independently within each distinct value of these.
    pub partition_by: Vec<SchemaField>,
    /// The order rows are numbered in, each with its own direction.
    pub order_by: Vec<(SchemaField, bool)>,
    pub frame: Frame,
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
        if let Some(argument) = &self.argument {
            if !out.iter().any(|f| f.name == argument.name) {
                out.push(argument.clone());
            }
        }
        out
    }

    pub fn result_type(&self) -> DbType {
        let arg = self.argument.as_ref().map(|f| f.db_type.clone());
        match self.func {
            WindowFunc::RowNumber
            | WindowFunc::Rank
            | WindowFunc::DenseRank
            | WindowFunc::Count => DbType::I64,
            WindowFunc::PercentRank | WindowFunc::CumeDist => DbType::F64,
            // A value function returns whatever it was given.
            WindowFunc::Lag
            | WindowFunc::Lead
            | WindowFunc::FirstValue
            | WindowFunc::LastValue
            | WindowFunc::NthValue
            | WindowFunc::Min
            | WindowFunc::Max => arg.unwrap_or(DbType::I64),
            // The grouped aggregates' own rule, so `SUM(x)` and
            // `SUM(x) OVER ()` describe the same type to a client — they are
            // folded by the same accumulator (`numeric_fold`).
            WindowFunc::Sum => AggFunc::Sum.result_type(arg.as_ref()),
            WindowFunc::Avg => AggFunc::Avg.result_type(arg.as_ref()),
        }
    }

    /// The scale a `DECIMAL` result is stored at, which pgwire's
    /// `select_scales` must render it at.
    ///
    /// The grouped aggregate's rule, [`AggFunc::decimal_output_scale`]: a value
    /// read out of the column, and a `SUM`, `MIN` or `MAX` of its raw integers,
    /// stay at the column's declared scale; `AVG` divides at the engine scale,
    /// or the column's when wider. A non-decimal result has no scale and
    /// reports the engine's.
    pub fn decimal_output_scale(&self) -> u32 {
        let arg = self.argument.as_ref();
        match self.func {
            WindowFunc::Avg => AggFunc::Avg.decimal_output_scale(arg),
            WindowFunc::Sum
            | WindowFunc::Min
            | WindowFunc::Max
            | WindowFunc::Lag
            | WindowFunc::Lead
            | WindowFunc::FirstValue
            | WindowFunc::LastValue
            | WindowFunc::NthValue => AggFunc::Sum.decimal_output_scale(arg),
            _ => crate::core::value_codec::DECIMAL_SCALE,
        }
    }
}

/// Parses `<func>(args) OVER (…)`, or `None` when the item is not a window call.
///
/// The `OVER` is what distinguishes a window call from an aggregate: `AVG(x)`
/// folds a group and `AVG(x) OVER (…)` keeps every row. This has to run
/// **before** the aggregate rewriter, which would otherwise replace the call
/// with a synthetic column and leave `__agg_0 OVER (…)` behind — neither a
/// column nor a value.
pub fn parse(item: &str, schema: &[SchemaField]) -> Option<Result<WindowSpec, String>> {
    let (expr, alias) = split_alias(item);

    let open = expr.find('(')?;
    let name = expr[..open].trim();
    let func = WindowFunc::parse(name)?;

    let close = match matching_paren(expr, open) {
        Some(c) => c,
        None => return Some(Err(format!("unterminated call in '{}'", item))),
    };

    let rest = expr[close + 1..].trim();
    if !starts_with_word(rest, "OVER") {
        // Not a window call at all — a plain `AVG(x)` is an aggregate, and
        // saying so is the aggregate path's job, not this one's. Only the
        // functions that exist *solely* as window functions report the missing
        // clause themselves.
        return match func {
            WindowFunc::Sum
            | WindowFunc::Avg
            | WindowFunc::Count
            | WindowFunc::Min
            | WindowFunc::Max => None,
            _ => Some(Err(format!(
                "{}() is a window function and needs an OVER clause",
                name
            ))),
        };
    }

    let args_text = expr[open + 1..close].trim();
    let arguments: Vec<&str> = if args_text.is_empty() {
        Vec::new()
    } else {
        split_arguments(args_text)
    };
    let (min_args, max_args) = func.arity();
    // `COUNT(*)` is the no-argument form spelled with a star.
    let arguments: Vec<&str> = if arguments.len() == 1 && arguments[0] == "*" {
        Vec::new()
    } else {
        arguments
    };
    if arguments.len() < min_args || arguments.len() > max_args {
        return Some(Err(format!(
            "{}() takes {} argument(s), not {}",
            name.to_ascii_uppercase(),
            if min_args == max_args {
                min_args.to_string()
            } else {
                format!("{} to {}", min_args, max_args)
            },
            arguments.len()
        )));
    }

    let argument = match arguments.first() {
        None => None,
        Some(text) => match resolve_column(text, schema) {
            Ok(field) => Some(field),
            Err(e) => return Some(Err(e)),
        },
    };
    let offset = match arguments.get(1) {
        None => match func {
            // `LAG(x)` and `LEAD(x)` default to one row.
            WindowFunc::Lag | WindowFunc::Lead => 1,
            _ => 1,
        },
        Some(text) => match text.trim().parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                return Some(Err(format!(
                    "the second argument of {}() must be a constant number, not '{}'",
                    name.to_ascii_uppercase(),
                    text.trim()
                )));
            }
        },
    };

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
    Some(build(func, argument, offset, body, alias, name, schema))
}

/// Splits a call's arguments on commas that are not inside parentheses.
fn split_arguments(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (at, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(text[start..at].trim());
                start = at + 1;
            }
            _ => {}
        }
    }
    out.push(text[start..].trim());
    out
}

fn resolve_column(text: &str, schema: &[SchemaField]) -> Result<SchemaField, String> {
    let bare = text
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or(text)
        .trim_matches('"');
    schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
        .cloned()
        .ok_or_else(|| format!("Column '{}' not found in schema", text.trim()))
}

/// Whether `text` begins with `word` as a whole word.
fn starts_with_word(text: &str, word: &str) -> bool {
    let text = text.trim_start();
    if text.len() < word.len() || !text[..word.len()].eq_ignore_ascii_case(word) {
        return false;
    }
    text.as_bytes()
        .get(word.len())
        .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_')
}

fn build(
    func: WindowFunc,
    argument: Option<SchemaField>,
    offset: i64,
    body: &str,
    alias: Option<String>,
    name: &str,
    schema: &[SchemaField],
) -> Result<WindowSpec, String> {
    let upper = body.to_ascii_uppercase();
    let partition_at = find_word(&upper, "PARTITION BY");
    let order_at = find_word(&upper, "ORDER BY");
    let frame_at = find_word(&upper, "ROWS").or_else(|| find_word(&upper, "RANGE"));

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
            out.push((resolve_column(column, schema)?, descending));
        }
        Ok(out)
    };

    // The three clauses come in this order, and each ends where the next
    // begins. Cutting them by position rather than by splitting on keywords
    // keeps a column called `range` from ending the ORDER BY list.
    let end_of = |from: usize| -> usize {
        [order_at, frame_at]
            .into_iter()
            .flatten()
            .filter(|at| *at > from)
            .min()
            .unwrap_or(body.len())
    };
    let partition_text = match partition_at {
        Some(p) => &body[p + "PARTITION BY".len()..end_of(p)],
        None => "",
    };
    let order_text = match order_at {
        Some(o) => {
            if partition_at.is_some_and(|p| p > o) {
                return Err("PARTITION BY must come before ORDER BY".to_string());
            }
            &body[o + "ORDER BY".len()..frame_at.filter(|f| *f > o).unwrap_or(body.len())]
        }
        None => "",
    };

    let partition_by = resolve(partition_text)?
        .into_iter()
        .map(|(f, _)| f)
        .collect();
    let order_by = resolve(order_text)?;

    let frame = match frame_at {
        None => Frame::default_for(!order_by.is_empty()),
        Some(at) => parse_frame(&body[at..])?,
    };

    Ok(WindowSpec {
        func,
        argument,
        offset,
        partition_by,
        order_by,
        frame,
        label: alias.unwrap_or_else(|| name.to_ascii_lowercase()),
    })
}

/// `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`, and the single-bound forms.
fn parse_frame(text: &str) -> Result<Frame, String> {
    let words: Vec<String> = text
        .split_whitespace()
        .map(|w| w.to_ascii_uppercase())
        .collect();
    let mode = match words.first().map(String::as_str) {
        Some("ROWS") => FrameMode::Rows,
        Some("RANGE") => FrameMode::Range,
        _ => return Err(format!("'{}' is not a window frame", text.trim())),
    };
    let mut at = 1usize;

    // `ROWS n PRECEDING` is shorthand for `ROWS BETWEEN n PRECEDING AND CURRENT ROW`.
    let between = words.get(at).map(String::as_str) == Some("BETWEEN");
    if between {
        at += 1;
    }
    let start = parse_bound(&words, &mut at)?;
    let end = if between {
        if words.get(at).map(String::as_str) != Some("AND") {
            return Err("a BETWEEN frame needs AND between its bounds".to_string());
        }
        at += 1;
        parse_bound(&words, &mut at)?
    } else {
        FrameBound::CurrentRow
    };

    // `EXCLUDE` changes which rows the frame drops and is not implemented.
    // Refused rather than ignored: silently including the current row where the
    // query said to exclude it changes the number, not the shape.
    if words.get(at).is_some() {
        return Err(format!(
            "'{}' is not part of a window frame",
            words[at..].join(" ")
        ));
    }

    // A `RANGE` frame with an offset compares *values*, not row counts, and
    // needs the ORDER BY column's own arithmetic. Only the peer-based forms are
    // implemented; an offset is refused rather than answered as though it
    // counted rows, which would be a different set of rows and no error.
    if mode == FrameMode::Range
        && [start, end]
            .iter()
            .any(|b| matches!(b, FrameBound::Preceding(_) | FrameBound::Following(_)))
    {
        return Err(
            "a RANGE frame with an offset is not supported yet; use ROWS, or a frame              bounded by UNBOUNDED/CURRENT ROW"
                .to_string(),
        );
    }

    Ok(Frame { mode, start, end })
}

fn parse_bound(words: &[String], at: &mut usize) -> Result<FrameBound, String> {
    let first = words
        .get(*at)
        .ok_or_else(|| "a window frame is missing a bound".to_string())?
        .clone();
    match first.as_str() {
        "UNBOUNDED" => {
            *at += 1;
            match words.get(*at).map(String::as_str) {
                Some("PRECEDING") => {
                    *at += 1;
                    Ok(FrameBound::UnboundedPreceding)
                }
                Some("FOLLOWING") => {
                    *at += 1;
                    Ok(FrameBound::UnboundedFollowing)
                }
                _ => Err("UNBOUNDED must be followed by PRECEDING or FOLLOWING".to_string()),
            }
        }
        "CURRENT" => {
            *at += 1;
            if words.get(*at).map(String::as_str) != Some("ROW") {
                return Err("CURRENT must be followed by ROW".to_string());
            }
            *at += 1;
            Ok(FrameBound::CurrentRow)
        }
        number => {
            let n: u64 = number
                .parse()
                .map_err(|_| format!("'{}' is not a frame offset", number))?;
            *at += 1;
            match words.get(*at).map(String::as_str) {
                Some("PRECEDING") => {
                    *at += 1;
                    Ok(FrameBound::Preceding(n))
                }
                Some("FOLLOWING") => {
                    *at += 1;
                    Ok(FrameBound::Following(n))
                }
                _ => Err(format!(
                    "the frame offset {} must be followed by PRECEDING or FOLLOWING",
                    n
                )),
            }
        }
    }
}

/// A window function's result for one row: its bytes, or NULL.
pub type WindowValue = Option<Vec<u8>>;

/// The window value for each row, in the order the rows were given.
///
/// Rows are ranked within their partition, and the result is placed back at the
/// row's original position — the caller's row order is not disturbed, because
/// the statement's own `ORDER BY` decides that and runs later.
///
/// An error when an exact `SUM`/`AVG` leaves its result type — `bigint out of
/// range`, `numeric field overflow`, the grouped aggregate's wording. It used
/// to answer NULL, which a client cannot tell from an all-NULL frame.
pub fn assign(rows: &[Row], spec: &WindowSpec) -> Result<Vec<WindowValue>, String> {
    // A document partitions by its value, as `=` compares it: `{"a":1}` and
    // `{"a":1.0}` are one partition.
    let key_of = |row: &Row, fields: &[SchemaField]| -> Vec<Vec<u8>> {
        fields
            .iter()
            .map(|f| {
                row.columns
                    .iter()
                    .find(|c| u64::from(c.schema_id) == f.write_order)
                    .map(|c| {
                        let bytes = c.data.into_slice();
                        if f.db_type == DbType::JSONB && !bytes.is_empty() {
                            let mut key = Vec::new();
                            crate::core::jsonb::push_group_key(bytes, &mut key);
                            key
                        } else {
                            bytes.to_vec()
                        }
                    })
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

    let mut out: Vec<WindowValue> = vec![None; rows.len()];
    for (_, mut members) in partitions {
        // A stable sort, so rows equal on every key keep the order they were
        // scanned in rather than an arbitrary one that could differ per run.
        members.sort_by(|a, b| compare_by_order(rows, *a, *b, &spec.order_by));

        let peers_with_previous = |position: usize| -> bool {
            position > 0
                && compare_by_order(
                    rows,
                    members[position],
                    members[position - 1],
                    &spec.order_by,
                ) == std::cmp::Ordering::Equal
        };

        // The ranking numbers, computed once for the partition because three of
        // the functions need them and `PERCENT_RANK` needs the partition size.
        let count = members.len();
        let mut ranks: Vec<(i64, i64, i64)> = Vec::with_capacity(count);
        let mut rank = 1i64;
        let mut dense = 0i64;
        for position in 0..count {
            if !peers_with_previous(position) {
                dense += 1;
                rank = position as i64 + 1;
            }
            ranks.push((position as i64 + 1, rank, dense));
        }
        // How many rows are at or before each row's peer group, for CUME_DIST.
        let mut peer_end: Vec<usize> = vec![0; count];
        {
            let mut group_start = 0usize;
            for position in 0..=count {
                let ends = position == count || !peers_with_previous(position);
                if ends && position > 0 {
                    for slot in peer_end.iter_mut().take(position).skip(group_start) {
                        *slot = position;
                    }
                    group_start = position;
                }
            }
        }

        for (position, index) in members.iter().enumerate() {
            let (row_number, rank, dense) = ranks[position];
            out[*index] = match spec.func {
                WindowFunc::RowNumber => Some(row_number.to_le_bytes().to_vec()),
                // With no ORDER BY every row ties, which is what makes RANK
                // and DENSE_RANK 1 for all of them — as in PostgreSQL.
                WindowFunc::Rank => Some(rank.max(1).to_le_bytes().to_vec()),
                WindowFunc::DenseRank => Some(dense.max(1).to_le_bytes().to_vec()),
                WindowFunc::PercentRank => {
                    let value = if count <= 1 {
                        0.0
                    } else {
                        (rank.max(1) - 1) as f64 / (count - 1) as f64
                    };
                    Some(value.to_le_bytes().to_vec())
                }
                WindowFunc::CumeDist => Some(
                    (peer_end[position] as f64 / count as f64)
                        .to_le_bytes()
                        .to_vec(),
                ),
                // Addressed by position in the partition, not through the frame.
                WindowFunc::Lag | WindowFunc::Lead => {
                    let step = if spec.func == WindowFunc::Lag {
                        -spec.offset
                    } else {
                        spec.offset
                    };
                    let target = position as i64 + step;
                    if target < 0 || target >= count as i64 {
                        None
                    } else {
                        argument_bytes(rows, members[target as usize], spec)
                    }
                }
                _ => {
                    let (lo, hi) = frame_range(spec, position, count, &peers_with_previous);
                    if lo > hi {
                        // An empty frame: every aggregate but COUNT is NULL.
                        match spec.func {
                            WindowFunc::Count => Some(0i64.to_le_bytes().to_vec()),
                            _ => None,
                        }
                    } else {
                        evaluate_over(rows, &members[lo..=hi], spec)?
                    }
                }
            };
        }
    }

    // A `DECIMAL` value stays at the scale `WindowSpec::decimal_output_scale`
    // names — the column's for a value read out of it or a `SUM`, wider for an
    // `AVG` — and pgwire renders it there. It used to be lifted to the engine
    // scale here instead, which *truncated* a column wider than nine places,
    // and the grouped aggregate over the same rows printed a different number.
    Ok(out)
}

/// Compares two rows on a window's `ORDER BY` list.
fn compare_by_order(
    rows: &[Row],
    a: usize,
    b: usize,
    order_by: &[(SchemaField, bool)],
) -> std::cmp::Ordering {
    for (field, descending) in order_by {
        let lhs = rows[a]
            .columns
            .iter()
            .find(|c| u64::from(c.schema_id) == field.write_order)
            .map(|c| c.data.into_slice())
            .unwrap_or(&[]);
        let rhs = rows[b]
            .columns
            .iter()
            .find(|c| u64::from(c.schema_id) == field.write_order)
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
}

/// The frame's row range within the sorted partition, as inclusive positions.
///
/// `lo > hi` means the frame is empty, which is a real outcome — `ROWS BETWEEN
/// 3 FOLLOWING AND 5 FOLLOWING` near the end of a partition contains nothing.
fn frame_range(
    spec: &WindowSpec,
    position: usize,
    count: usize,
    peers_with_previous: &dyn Fn(usize) -> bool,
) -> (usize, usize) {
    if !spec.func.honours_frame() {
        return (0, count.saturating_sub(1));
    }

    // In RANGE mode a bound at the current row reaches the ends of its peer
    // group: the rows that tie with it on the ORDER BY key are part of the
    // same step. That is the difference between a running total that jumps at
    // each distinct value and one that jumps at each row.
    let peer_start = |from: usize| -> usize {
        let mut at = from;
        while at > 0 && peers_with_previous(at) {
            at -= 1;
        }
        at
    };
    let peer_finish = |from: usize| -> usize {
        let mut at = from;
        while at + 1 < count && peers_with_previous(at + 1) {
            at += 1;
        }
        at
    };

    let lo = match spec.frame.start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(n) => position.saturating_sub(n as usize),
        FrameBound::CurrentRow => match spec.frame.mode {
            FrameMode::Rows => position,
            FrameMode::Range => peer_start(position),
        },
        FrameBound::Following(n) => position.saturating_add(n as usize),
        FrameBound::UnboundedFollowing => count,
    };
    let hi = match spec.frame.end {
        FrameBound::UnboundedPreceding => 0usize.wrapping_sub(1),
        FrameBound::Preceding(n) => {
            if n as usize > position {
                // Every row before the frame's end precedes the partition.
                return (1, 0);
            }
            position - n as usize
        }
        FrameBound::CurrentRow => match spec.frame.mode {
            FrameMode::Rows => position,
            FrameMode::Range => peer_finish(position),
        },
        FrameBound::Following(n) => (position + n as usize).min(count.saturating_sub(1)),
        FrameBound::UnboundedFollowing => count.saturating_sub(1),
    };

    if count == 0 || lo >= count {
        return (1, 0);
    }
    (lo, hi.min(count.saturating_sub(1)))
}

/// The argument column's bytes for one row, or `None` when it holds nothing.
fn argument_bytes(rows: &[Row], index: usize, spec: &WindowSpec) -> Option<Vec<u8>> {
    let field = spec.argument.as_ref()?;
    let column = rows[index]
        .columns
        .iter()
        .find(|c| u64::from(c.schema_id) == field.write_order)?;
    // A NULL is typed NULL by the per-row retyping, while its bytes are the
    // type's zeroes: read as a value it was a 0 that counted towards an `AVG`.
    if column.column_type == DbType::NULL {
        return None;
    }
    let bytes = column.data.into_slice();
    // A variable-width column with no bytes is a NULL, not an empty value.
    if bytes.is_empty() && field.db_type.is_variable_width() {
        return None;
    }
    Some(bytes.to_vec())
}

/// The aggregate and value functions, over the rows the frame selected.
fn evaluate_over(
    rows: &[Row],
    frame: &[usize],
    spec: &WindowSpec,
) -> Result<WindowValue, String> {
    Ok(match spec.func {
        WindowFunc::Count => {
            let n = match spec.argument {
                // `COUNT(*)` counts rows; `COUNT(x)` counts non-NULL values.
                None => frame.len() as i64,
                Some(_) => frame
                    .iter()
                    .filter(|index| argument_bytes(rows, **index, spec).is_some())
                    .count() as i64,
            };
            Some(n.to_le_bytes().to_vec())
        }
        WindowFunc::FirstValue => frame
            .first()
            .and_then(|at| argument_bytes(rows, *at, spec)),
        WindowFunc::LastValue => frame.last().and_then(|at| argument_bytes(rows, *at, spec)),
        WindowFunc::NthValue => {
            // One-based, as in PostgreSQL.
            usize::try_from(spec.offset)
                .ok()
                .and_then(|n| n.checked_sub(1))
                .and_then(|at| frame.get(at))
                .and_then(|at| argument_bytes(rows, *at, spec))
        }
        WindowFunc::Min | WindowFunc::Max => {
            let Some(field) = spec.argument.as_ref() else {
                return Ok(None);
            };
            let mut best: Option<Vec<u8>> = None;
            for index in frame {
                let Some(bytes) = argument_bytes(rows, *index, spec) else {
                    continue;
                };
                best = Some(match best {
                    None => bytes,
                    Some(current) => {
                        let ord =
                            crate::core::processor::concurrent_processor::compare_bytes_as_type(
                                field.db_type.clone(),
                                &bytes,
                                &current,
                            );
                        let takes = if spec.func == WindowFunc::Min {
                            ord == std::cmp::Ordering::Less
                        } else {
                            ord == std::cmp::Ordering::Greater
                        };
                        if takes { bytes } else { current }
                    }
                });
            }
            best
        }
        WindowFunc::Sum | WindowFunc::Avg => numeric_fold(rows, frame, spec)?,
        // Handled by the caller, which addresses the partition rather than the
        // frame.
        _ => None,
    })
}

/// `SUM` and `AVG` over a frame.
///
/// Folded by the grouped aggregate's own accumulator, so the two cannot
/// disagree: exact types total in an `i128` and raise `bigint out of range` /
/// `numeric field overflow` when the answer leaves the result type, an exact
/// `AVG` rounds half away from zero at `AggFunc::decimal_output_scale`, and
/// floats total in an `f64`. An all-NULL or empty frame is NULL.
///
/// This used to be a second implementation that answered NULL on overflow and
/// truncated its average — two silent differences from `SUM(x)`/`AVG(x)`.
fn numeric_fold(rows: &[Row], frame: &[usize], spec: &WindowSpec) -> Result<WindowValue, String> {
    let Some(field) = spec.argument.as_ref() else {
        return Ok(None);
    };
    let func = match spec.func {
        WindowFunc::Sum => AggFunc::Sum,
        WindowFunc::Avg => AggFunc::Avg,
        _ => return Ok(None),
    };
    let mut acc = crate::core::rql::aggregate::Acc::new(func, Some(field));
    for index in frame {
        let Some(bytes) = argument_bytes(rows, *index, spec) else {
            continue;
        };
        acc.accumulate(Some(&bytes), Some(&field.db_type));
    }
    acc.finish_nullable(func, &spec.result_type())
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
        let mut ok = start == 0 || !upper.as_bytes()[start - 1].is_ascii_alphanumeric();
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
