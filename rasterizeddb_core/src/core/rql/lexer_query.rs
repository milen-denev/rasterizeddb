use log::debug;
use smallvec::SmallVec;

use crate::core::rql::aggregate::{AggFunc, SelectItem};
use crate::core::{
    processor::transformer::ComparerOperation,
    row::{
        common::simd_compare_strings,
        row::{ColumnFetchingData, RowFetch},
        schema::{SchemaCalculator, SchemaField},
    },
    rql::lexer_s1::QueryPurpose,
};

#[derive(Debug, Clone)]
pub struct QueryRows {
    pub table_name: String,
    pub query_row_fetch: RowFetch,
    pub requested_row_fetch: RowFetch,
    pub where_clause: String,
    /// Row cap handed to the scan. When `OFFSET` is present this already
    /// includes the skipped rows, because the scan cannot start part-way in.
    pub limit: Option<u64>,
    pub order_by: Option<String>,
    /// Rows to drop from the front of the result, from `OFFSET`.
    pub offset: Option<u64>,
    /// `SELECT DISTINCT`: duplicate result rows are dropped after the scan.
    ///
    /// Like an aggregate, this makes `LIMIT` and `OFFSET` apply to the
    /// deduplicated output, so neither is handed to the scan — they are taken
    /// from `written_limit` / `offset` instead.
    pub distinct: bool,
    /// `LIMIT` exactly as written, before `OFFSET` was folded into it for the
    /// scan. Used by the passes that run after the scan.
    pub written_limit: Option<u64>,
    /// Output positions holding a constant rather than a fetched column, with
    /// the value and type to emit. `SELECT 1` and a scalar subquery both
    /// produce these; the remaining positions consume the fetched columns in
    /// order.
    pub literal_slots: Vec<(usize, Vec<u8>, crate::core::db_type::DbType)>,
    /// Output positions filled by a nullary scalar function. Separate from
    /// The `FOR UPDATE`/`FOR SHARE` clause, when one was written.
    pub row_lock: Option<crate::core::rql::row_lock::RowLock>,
    /// Whether the `WHERE` clause reads a column the projection does not, so
    /// the scan may return more columns than the client asked for.
    pub predicate_needs_extra_columns: bool,
    /// Window functions, each filling one output slot. Computed after the scan
    /// over every row that survived `WHERE`.
    pub window_slots: Vec<(usize, crate::core::rql::window::WindowSpec)>,
    /// Expressions over the row's own columns, each filling one output slot.
    pub expr_slots: Vec<(
        usize,
        crate::core::rql::expr::Expr,
        crate::core::db_type::DbType,
    )>,
    /// `literal_slots` because these are evaluated per row rather than once.
    pub scalar_slots: Vec<(
        usize,
        crate::core::rql::aggregate::ScalarFunc,
        crate::core::db_type::DbType,
    )>,
    /// The `SELECT` list as written, in output order.
    pub select_items: Vec<crate::core::rql::aggregate::SelectItem>,
    /// For each visible output column, where its nullability companion sits in
    /// the fetched row — or `None` when the column cannot be `NULL`.
    ///
    /// The companions are fetched after the visible columns and trimmed off
    /// before the result is sent, the same way an unselected sort key is.
    pub null_flag_positions: Vec<Option<usize>>,
    /// Present when the SELECT list contains an aggregate call. The scan runs
    /// unchanged and the fold happens over the rows it returns.
    ///
    /// When this is set, `limit`, `offset` and `order_by` are deliberately
    /// **not** handed to the scan: all three apply to the aggregated output,
    /// which does not exist until the fold has run. Passing a limit down would
    /// also switch on early scan termination, silently truncating the input.
    pub aggregate: Option<crate::core::rql::aggregate::AggregatePlan>,
    /// `ORDER BY` keys the scan cannot sort by: expressions, select aliases,
    /// or plain columns carrying an explicit NULLS placement. When non-empty,
    /// the scan runs unsorted and unbounded and the executor evaluates each
    /// key per row, sorts, and pages. `(expr, descending, nulls_first)`.
    pub order_expressions: Vec<(crate::core::rql::expr::Expr, bool, Option<bool>)>,
    /// How many of `requested_row_fetch`'s columns the client asked for.
    ///
    /// Sorting reads the key out of the fetched row, so an `ORDER BY` over a
    /// column that is not selected has to fetch it anyway. It is appended after
    /// the visible ones and trimmed before the result is sent.
    pub visible_columns: usize,
}

/// Parses one select item into a plain column or an aggregate call.
///
/// The label an item is known by follows Postgres: an explicit `AS` name wins,
/// otherwise a column keeps its own name and a function takes the function's
/// name. The label is what `ORDER BY` may refer to.
/// Builds a [`SelectItem::Computed`] when `expr` is an expression wrapped
/// around one or more aggregate calls.
///
/// Returns `Ok(None)` when the item holds no aggregate at all, or when it is a
/// single bare aggregate call — both of which the existing paths handle.
///
/// The rewrite is textual: each aggregate call is replaced by a synthetic
/// column name, and the result is parsed by the ordinary expression parser
/// against a synthetic schema. That keeps one expression grammar in the engine
/// rather than a second one that only aggregates can use.
fn parse_computed_item(
    expr: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    alias: Option<&str>,
) -> Result<Option<SelectItem>, String> {
    let calls = find_aggregate_calls(expr);
    if calls.is_empty() {
        return Ok(None);
    }
    // A bare `SUM(x)` — one call spanning the whole item — is not computed.
    if calls.len() == 1 && calls[0].0 == 0 && calls[0].1 == expr.len() {
        return Ok(None);
    }

    let mut inputs: Vec<crate::core::rql::aggregate::ComputedInput> = Vec::new();
    let mut synth_schema: Vec<SchemaField> = Vec::new();
    let mut rewritten = String::with_capacity(expr.len());
    let mut cursor = 0usize;

    for (start, end) in &calls {
        rewritten.push_str(&expr[cursor..*start]);
        let call = &expr[*start..*end];
        let (func, arg) = parse_aggregate_call(call, schema)?;

        let slot = inputs.len();
        let ty = func.result_type(arg.as_ref().map(|f| &f.db_type));
        let name = format!(
            "{}{}",
            crate::core::rql::aggregate::COMPUTED_INPUT_PREFIX,
            slot
        );
        let mut synth_field = SchemaField::new(
            name.clone(),
            ty.clone(),
            ty.get_size(),
            0,
            slot as u64,
            false,
        );
        // SUM, MIN and MAX of a `NUMERIC(p, s)` column fold its stored
        // integers, so their result is at the column's scale; AVG divides at
        // the engine scale or wider (`AggFunc::decimal_output_scale`). Carried
        // here so the expression lifts it to the engine scale like a column
        // read — `MIN(v) + 1` over a `NUMERIC(10,2)` added 1 to 100 as 10^9
        // to 10^4.
        if matches!(ty, crate::core::db_type::DbType::DECIMAL) {
            synth_field.scale = Some(func.decimal_output_scale(arg.as_ref()));
        }
        synth_schema.push(synth_field);
        inputs.push(crate::core::rql::aggregate::ComputedInput { func, arg });
        rewritten.push_str(&name);
        cursor = *end;
    }
    rewritten.push_str(&expr[cursor..]);

    let parsed = crate::core::rql::expr::parse(&rewritten, &synth_schema)?;
    let db_type = crate::core::rql::expr::static_type(&parsed);

    Ok(Some(SelectItem::Computed {
        expr: parsed,
        inputs,
        db_type,
        label: alias
            .map(|a| a.to_string())
            .unwrap_or_else(|| "?column?".to_string()),
    }))
}

/// Splits `FUNC(args)` into the aggregate and its column argument.
/// Splits `COUNT(*) FILTER (WHERE …)` into the call and its filter.
///
/// The clause restricts which rows *this* aggregate sees, so it cannot be
/// folded into the statement's `WHERE` — a statement usually pairs a filtered
/// aggregate with an unfiltered one, and moving the test would change both.
pub(crate) fn split_aggregate_filter(item: &str) -> (Option<String>, Option<String>) {
    let upper = item.to_ascii_uppercase();
    let mut from = 0usize;
    while let Some(at) = upper[from..].find("FILTER") {
        let start = from + at;
        let before_ok = start == 0 || !upper.as_bytes()[start - 1].is_ascii_alphanumeric();
        let after = &item[start + "FILTER".len()..];
        let trimmed = after.trim_start();
        if before_ok && trimmed.starts_with('(') {
            let open = item.len() - trimmed.len();
            if let Some(close) = matching_paren_at(item, open) {
                let inside = item[open + 1..close].trim();
                if inside.len() >= 5 && inside[..5].eq_ignore_ascii_case("WHERE") {
                    // The item with the clause removed, so what follows it — a
                    // cast, an alias — stays attached to the call.
                    let rebuilt = format!("{}{}", item[..start].trim_end(), &item[close + 1..]);
                    return (Some(rebuilt), Some(inside[5..].trim().to_string()));
                }
            }
        }
        from = start + "FILTER".len();
    }
    (None, None)
}

fn parse_aggregate_call(
    call: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<(AggFunc, Option<SchemaField>), String> {
    let open = call
        .find('(')
        .ok_or_else(|| format!("'{}' is not a call", call))?;
    let close = call
        .rfind(')')
        .ok_or_else(|| format!("unterminated function call in '{}'", call))?;
    let name = call[..open].trim();
    let func =
        AggFunc::parse(name).ok_or_else(|| format!("function '{}' is not supported yet", name))?;
    let arg_text = call[open + 1..close].trim();

    if arg_text == "*" {
        if func != AggFunc::Count {
            return Err(format!("{}(*) is not valid", name.to_uppercase()));
        }
        return Ok((func, None));
    }
    let bare = arg_text
        .rsplit('.')
        .next()
        .unwrap_or(arg_text)
        .trim_matches('"');
    if let Some(field) = schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
    {
        return Ok((func, Some(field.clone())));
    }
    Err(format!("Column '{}' not found in schema", arg_text))
}

/// Byte ranges of every top-level aggregate call inside `expr`.
///
/// Nested aggregates are not looked for: SQL forbids them, and the outer call's
/// range already covers the text.
pub(crate) fn find_aggregate_calls(expr: &str) -> Vec<(usize, usize)> {
    let bytes = expr.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut in_quote = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_quote {
            if c == '\'' {
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            in_quote = true;
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let word = &expr[start..i];
            // The call's parenthesis may be separated by spaces: `MAX (x)`.
            let mut j = i;
            while j < bytes.len() && (bytes[j] as char).is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' && AggFunc::parse(word).is_some() {
                if let Some(close) = matching_paren_at(expr, j) {
                    out.push((start, close + 1));
                    i = close + 1;
                    continue;
                }
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Index of the `)` closing the `(` at `open`.
fn matching_paren_at(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut in_quote = false;
    for i in open..bytes.len() {
        let c = bytes[i] as char;
        if in_quote {
            if c == '\'' {
                in_quote = false;
            }
            continue;
        }
        match c {
            '\'' => in_quote = true,
            '(' => depth += 1,
            ')' => {
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

/// The fields an expression reads, with their NULL companions — the schema a
/// computed grouping key's evaluation sees. Built the same way as an
/// [`crate::core::rql::aggregate::AggregateFilter`]'s, for the same reason:
/// the fold has no schema of its own to tell a NULL from a zero.
fn expr_reads(
    expr: &crate::core::rql::expr::Expr,
    schema: &smallvec::SmallVec<[SchemaField; 20]>,
) -> Vec<SchemaField> {
    let mut fields: Vec<SchemaField> = Vec::new();
    for field in crate::core::rql::expr::columns_of(expr) {
        if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema) {
            if !fields.iter().any(|f| f.name == flag.name) {
                fields.push(flag.clone());
            }
        }
        if !fields.iter().any(|f| f.name == field.name) {
            fields.push(field);
        }
    }
    fields
}

fn parse_select_items(
    columns_part: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Vec<SelectItem>, String> {
    if columns_part.trim() == "*" {
        // Nullability companions are the engine's, not the client's.
        return Ok(crate::core::rql::nullable::visible_fields(schema)
            .into_iter()
            .map(|f| SelectItem::Column {
                label: f.name.clone(),
                field: f,
            })
            .collect());
    }

    let mut out = Vec::new();
    for raw in split_select_items(columns_part) {
        // `pg_column_size(t)` names the whole row. A row here is a fixed-width
        // record, so its stored width is the sum of the schema's field widths —
        // exact, and knowable without reading anything. `SUM(pg_column_size(t))`
        // is therefore that width times the number of rows, which the ordinary
        // aggregate path can answer.
        let rewritten_size = rewrite_row_size(raw.trim(), schema);
        let raw = rewritten_size.as_str();
        let item = raw.trim();
        if item.is_empty() {
            continue;
        }
        // `COUNT(*) FILTER (WHERE …)` is peeled off the item *before* anything
        // reads it as a call, because the cast that usually follows —
        // `COUNT(*) FILTER (…)::bigint` — otherwise sends the item down the
        // wrapped-aggregate path, which substituted the call and left the
        // clause behind as `__agg_0 FILTER (WHERE …)`: neither a column nor a
        // value.
        let (item_owned, filter_text) = split_aggregate_filter(item);
        // A cast around a filtered aggregate — `COUNT(*) FILTER (…)::bigint` —
        // would make the item an expression *around* a call, which is answered
        // by a different path that carries no filter. The cast is dropped: it
        // is a no-op on `COUNT`, and on a `SUM` it only widens what the column
        // already produces.
        let item_owned = item_owned.map(|text| {
            let (head, alias) = split_alias(&text);
            match crate::core::rql::expr::trailing_cast(head) {
                Some((call, _)) => match alias {
                    Some(alias) => format!("{} AS {}", call, alias),
                    None => call.to_string(),
                },
                None => text,
            }
        });
        let item = item_owned.as_deref().unwrap_or(item);

        let upper = item.to_ascii_uppercase();
        if upper.starts_with("DISTINCT ") || upper == "DISTINCT" {
            if starts_with_distinct_on(item["DISTINCT".len()..].trim_start()) {
                return Err("SELECT DISTINCT ON is not supported yet".to_string());
            }
            return Err("SELECT DISTINCT is not supported yet".to_string());
        }

        // `x IS [NOT] NULL` is a boolean. The alias split below read the `IS`
        // as an alias, so `doc->>'a' IS NULL` came back as the extracted text
        // in a column named `IS`. It is answered as the CASE it means, by the
        // expression evaluator, which reads a path's NULL as PostgreSQL does.
        if let Some((test, alias)) = null_test_item(item) {
            let parsed = crate::core::rql::expr::parse(
                &format!("CASE WHEN {} THEN TRUE ELSE FALSE END", test),
                schema,
            )?;
            out.push(SelectItem::RowExpr {
                db_type: crate::core::rql::expr::static_type(&parsed),
                expr: parsed,
                label: alias.unwrap_or_else(|| "?column?".to_string()),
            });
            continue;
        }

        // Split off a trailing alias. Only a bare `name` or `AS name` after the
        // expression counts, and the expression may itself contain spaces
        // inside its parentheses.
        let (expr, alias) = split_alias(item);

        let aggregate_filter = match &filter_text {
            None => None,
            Some(text) => {
                let condition = crate::core::rql::expr::parse_cond(text, schema)?;
                // The condition's columns, plus their NULL companions: the fold
                // has no schema of its own to tell a NULL from a zero.
                let mut fields: Vec<SchemaField> = Vec::new();
                for field in crate::core::rql::expr::cond_columns(&condition) {
                    if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema)
                    {
                        if !fields.iter().any(|f| f.name == flag.name) {
                            fields.push(flag.clone());
                        }
                    }
                    if !fields.iter().any(|f| f.name == field.name) {
                        fields.push(field);
                    }
                }
                Some(crate::core::rql::aggregate::AggregateFilter {
                    condition,
                    schema: fields,
                })
            }
        };

        // A window function looks like a call but is neither an aggregate nor a
        // scalar: it keeps every row and adds a column computed from the rows
        // around it.
        if let Some(outcome) = crate::core::rql::window::parse(item, schema) {
            let spec = outcome?;
            out.push(SelectItem::Window {
                db_type: spec.result_type(),
                label: spec.label.clone(),
                spec,
            });
            continue;
        }

        // Tried before the constant test below: that test reads a leading
        // quote as the whole item, so an expression *beginning* with a literal
        // was truncated to it.
        if crate::core::rql::expr::needs_evaluation(expr) || looks_like_row_expression(expr) {
            match crate::core::rql::expr::parse(expr, schema) {
                Ok(parsed) => {
                    let db_type = crate::core::rql::expr::static_type(&parsed);
                    out.push(SelectItem::RowExpr {
                        expr: parsed,
                        db_type,
                        label: alias.clone().unwrap_or_else(|| "?column?".to_string()),
                    });
                    continue;
                }
                // A type error is the statement's answer; falling through
                // would report the item as an unsupported construct instead.
                Err(e) if e.ends_with(" cannot be matched") => return Err(e),
                Err(_) => {}
            }
        }

        // A constant reads no column: `SELECT 1`, or whatever a scalar
        // subquery was replaced with.
        if let Some((bytes, db_type)) = parse_constant(expr) {
            out.push(SelectItem::Literal {
                bytes,
                db_type,
                label: alias.unwrap_or_else(|| "?column?".to_string()),
            });
            continue;
        }

        // An expression *over* aggregates — `COALESCE(MAX(x), 0) + 1` — is
        // neither a plain aggregate call nor a plain column. It is recognised
        // before the single-call path below, which would otherwise read
        // `COALESCE` as an unsupported aggregate function and refuse.
        //
        // A bare `MAX(x)` is deliberately left to that path: it needs no
        // expression machinery, and routing it here would change the output
        // type of every existing aggregate query.
        if let Some(computed) = parse_computed_item(expr, schema, alias.as_deref())? {
            out.push(computed);
            continue;
        }

        // An expression over the row's own columns — `a + b`, `COALESCE(a, 0)`.
        // Tried before the bare-column path below, which would otherwise report
        // the whole expression as a missing column name.
        if crate::core::rql::expr::needs_evaluation(expr)
            || crate::core::rql::lexer_query::looks_like_row_expression(expr)
        {
            if let Ok(parsed) = crate::core::rql::expr::parse(expr, schema) {
                let db_type = crate::core::rql::expr::static_type(&parsed);
                out.push(SelectItem::RowExpr {
                    expr: parsed,
                    db_type,
                    label: alias.unwrap_or_else(|| "?column?".to_string()),
                });
                continue;
            }
        }

        if let Some(open) = expr.find('(') {
            let func_name = expr[..open].trim();

            // A nullary scalar function occupies an output slot and reads no
            // column, like a constant — but is evaluated per row, so it cannot
            // be folded into one.
            if let Some((scalar, db_type)) =
                crate::core::rql::aggregate::ScalarFunc::parse(func_name)
            {
                let close = expr
                    .rfind(')')
                    .ok_or_else(|| format!("unterminated function call in '{}'", item))?;
                if !expr[open + 1..close].trim().is_empty() {
                    return Err(format!("{}() takes no arguments", func_name));
                }
                out.push(SelectItem::Scalar {
                    func: scalar,
                    db_type,
                    label: alias.unwrap_or_else(|| func_name.to_ascii_lowercase()),
                });
                continue;
            }

            let Some(func) = AggFunc::parse(func_name) else {
                return Err(format!("function '{}' is not supported yet", func_name));
            };
            let close = expr
                .rfind(')')
                .ok_or_else(|| format!("unterminated function call in '{}'", item))?;
            let arg_text = expr[open + 1..close].trim();

            let mut argument_expression: Option<crate::core::rql::aggregate::ArgExpression> = None;
            let arg = if arg_text == "*" {
                if func != AggFunc::Count {
                    return Err(format!("{}(*) is not valid", func_name.to_uppercase()));
                }
                None
            } else {
                let name = arg_text
                    .rsplit('.')
                    .next()
                    .unwrap_or(arg_text)
                    .trim_matches('"');
                match schema
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(name) && !f.is_deleted)
                {
                    Some(field) => Some(field.clone()),
                    // An argument that is computed rather than read:
                    // `SUM(CAST(NULLIF(data ->> 'y', '') AS NUMERIC))`, which is
                    // how an aggregate over a document field is written. Its
                    // type is known right here — the schema is in hand — so the
                    // result type is decided the same way a column's is, with
                    // no placeholder to be filled in later.
                    None => {
                        let parsed = crate::core::rql::expr::parse(arg_text, schema)
                            .map_err(|_| format!("Column '{}' not found in schema", arg_text))?;
                        let db_type = crate::core::rql::expr::static_type(&parsed);
                        let reads = expr_reads(&parsed, schema);
                        argument_expression = Some(crate::core::rql::aggregate::ArgExpression {
                            expr: parsed,
                            schema: reads,
                        });
                        Some(SchemaField::new(
                            arg_text.to_string(),
                            db_type.clone(),
                            db_type.get_size(),
                            0,
                            u64::MAX,
                            false,
                        ))
                    }
                }
            };

            out.push(SelectItem::Aggregate {
                func,
                arg,
                label: alias.unwrap_or_else(|| func.name().to_string()),
                filter: aggregate_filter,
                arg_expr: argument_expression,
            });
            continue;
        }

        // Say what is actually unsupported. A select item the resolver cannot
        // read is not automatically a missing column, and reporting it as one
        // sends the reader to check a schema that is fine — the same misleading
        // shape as an unsupported statement reported as "schema mismatch".
        // The whole item, not the alias-stripped `expr`: alias detection splits
        // on whitespace, so `CASE WHEN … END` arrives here as `expr = "CASE"`
        // with the rest read as an alias. Checking `expr` would miss every
        // multi-word construct — which is most of them.
        if let Some(reason) = unsupported_expression(item) {
            return Err(reason);
        }

        let name = expr.rsplit('.').next().unwrap_or(expr).trim_matches('"');
        if crate::core::rql::nullable::is_hidden(name) {
            return Err(format!("Column '{}' not found in schema", item));
        }
        let field = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name) && !f.is_deleted)
            .ok_or_else(|| format!("Column '{}' not found in schema", item))?;
        out.push(SelectItem::Column {
            label: alias.unwrap_or_else(|| field.name.clone()),
            field: field.clone(),
        });
    }

    if out.is_empty() {
        return Err("SELECT list is empty".to_string());
    }
    Ok(out)
}

/// Separates a select item from its output name.
/// Offset just past a multi-word clause keyword found at `at`.
///
/// The words may be separated by any whitespace, so the end cannot be computed
/// from their lengths alone: `ORDER BY` and `ORDER\n  BY` are the same clause
/// and end at different offsets.
fn after_phrase(sql: &str, at: usize, words: &[&str]) -> usize {
    let mut cursor = at;
    for word in words {
        let rest = &sql[cursor..];
        let trimmed = rest.trim_start();
        cursor += rest.len() - trimmed.len() + word.len();
    }
    cursor
}

/// Rewrites `pg_column_size(<relation>)` into the row's stored width.
///
/// Only the whole-row form is rewritten: `pg_column_size(<column>)` names a
/// value and is left for the expression evaluator, which can measure it.
fn rewrite_row_size(item: &str, schema: &SmallVec<[SchemaField; 20]>) -> String {
    const FUNC: &str = "pg_column_size";
    let lower = item.to_ascii_lowercase();
    let Some(at) = lower.find(FUNC) else {
        return item.to_string();
    };
    let after = &item[at + FUNC.len()..];
    let Some(open) = after.find('(') else {
        return item.to_string();
    };
    if !after[..open].trim().is_empty() {
        return item.to_string();
    }
    let Some(close) = after[open..].find(')').map(|c| open + c) else {
        return item.to_string();
    };
    let arg = after[open + 1..close].trim().trim_matches('"');
    let bare = arg.rsplit('.').next().unwrap_or(arg);

    // A real column: the evaluator measures the value itself.
    if schema.iter().any(|f| f.name.eq_ignore_ascii_case(bare)) {
        return item.to_string();
    }

    let width: u64 = schema
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .map(|f| f.size as u64)
        .sum();

    let before = &item[..at];
    let rest = &after[close + 1..];

    // `SUM(<width>)` over rows is `COUNT(*) * <width>`, which the aggregate
    // path can answer without an aggregate over an expression.
    let trimmed_before = before.trim_end();
    if trimmed_before.to_ascii_uppercase().ends_with("SUM(") {
        let head = &trimmed_before[..trimmed_before.len() - "SUM(".len()];
        let tail = rest.strip_prefix(')').unwrap_or(rest);
        return format!("{}COUNT(*) * {}{}", head, width, tail);
    }
    format!("{}{}{}", before, width, rest)
}

/// Whether an item looks like an expression rather than a column name.
///
/// `needs_evaluation` only recognises calls and casts, so a bare `a + b` needs
/// its own test: an operator at the top level is one.
pub(crate) fn looks_like_row_expression(text: &str) -> bool {
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
                b'+' | b'*' | b'/' | b'%' if depth == 0 && i > 0 => return true,
                // A `::` cast does too, and it is the only marker for one on a
                // parenthesised operand: `(id + 1)::bigint` hides its `+` at
                // depth 1, so without this the item was routed to the column
                // resolver and came back as "function '' is not supported yet".
                b':' if depth == 0 && bytes.get(i + 1) == Some(&b':') => return true,
                // `||` makes the item an expression as surely as `+` does.
                // Without this, `'utrn_' || replace(id, '-', '')` was taken for
                // the bare literal `'utrn_'` — every row answered with the
                // prefix and the rest of the expression silently discarded.
                b'|' if depth == 0 && bytes.get(i + 1) == Some(&b'|') => return true,
                // A `-` only counts when something can precede it, so a
                // negative literal is not mistaken for a subtraction.
                b'-' if depth == 0 && i > 0 => {
                    let prev = bytes[..i]
                        .iter()
                        .rposition(|c| !c.is_ascii_whitespace())
                        .map(|j| bytes[j]);
                    if matches!(prev, Some(c) if c.is_ascii_alphanumeric() || c == b'_' || c == b')' || c == b'"')
                    {
                        return true;
                    }
                }
                _ => {}
            },
        }
    }
    false
}

/// Whether `text` could name a column: a SQL identifier, not an operator or a
/// number.
fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Offset of a top-level ` AS ` in `item`, ignoring quotes and parentheses.
fn find_top_level_as(item: &str) -> Option<usize> {
    let bytes = item.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'A' | b'a' if depth == 0 => {
                    let is_word =
                        i + 2 <= bytes.len() && (bytes[i + 1] == b'S' || bytes[i + 1] == b's');
                    let before_ok = i > 0 && bytes[i - 1].is_ascii_whitespace();
                    let after_ok = i + 2 >= bytes.len() || bytes[i + 2].is_ascii_whitespace();
                    if is_word && before_ok && after_ok {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// A select item `<expr> IS [NOT] NULL [[AS] alias]`, as the test's text and
/// the alias, or `None` when the item is not that shape. A `CASE` block holds
/// its own `IS` and is left to the expression parser.
fn null_test_item(item: &str) -> Option<(String, Option<String>)> {
    if item.trim_start().to_ascii_uppercase().starts_with("CASE") {
        return None;
    }
    let at = crate::core::rql::join::find_keyword(item, "IS", 0)?;
    let operand = item[..at].trim();
    if operand.is_empty() {
        return None;
    }
    let words: Vec<&str> = item[at + 2..].split_whitespace().collect();
    let test_len = match words.as_slice() {
        [null, ..] if null.eq_ignore_ascii_case("NULL") => 1,
        [not, null, ..] if not.eq_ignore_ascii_case("NOT") && null.eq_ignore_ascii_case("NULL") => 2,
        _ => return None,
    };
    let alias = match &words[test_len..] {
        [] => None,
        [as_kw, name] if as_kw.eq_ignore_ascii_case("AS") => Some(name.trim_matches('"').to_string()),
        [name] if is_identifier(name.trim_matches('"')) => Some(name.trim_matches('"').to_string()),
        _ => return None,
    };
    Some((format!("{} IS {}", operand, words[..test_len].join(" ")), alias))
}

pub(crate) fn split_alias(item: &str) -> (&str, Option<String>) {
    // An explicit `AS` is unambiguous wherever it appears, so it is honoured
    // before any heuristic. Without this, an expression carrying a top-level
    // operator after its parentheses — `COALESCE(MAX(x), 0) + 1 AS next` — fell
    // through to splitting the whole item on whitespace, which cut the
    // expression at its first space and left `COALESCE(MAX(x),` behind.
    if let Some(at) = find_top_level_as(item) {
        let expr = item[..at].trim_end();
        let alias = item[at + 2..].trim().trim_matches('"');
        if !expr.is_empty() && !alias.is_empty() {
            return (expr, Some(alias.to_string()));
        }
    }

    // A whole `CASE … END` block with no alias is one expression. Without an
    // `AS` it fell through to the whitespace split below, which took `CASE`
    // as the expression and `WHEN` as its alias, so the item was reported as
    // unsupported.
    {
        let t = item.trim();
        let upper = t.to_ascii_uppercase();
        // Whole words only: `case_id case_end` is a column and its alias.
        let starts = upper.starts_with("CASE")
            && upper[4..].starts_with(|c: char| c.is_ascii_whitespace());
        let ends = upper.ends_with("END")
            && upper[..upper.len() - 3].ends_with(|c: char| c.is_ascii_whitespace());
        if starts && ends {
            return (t, None);
        }
    }

    // A quoted string literal is a single token however many spaces it holds.
    // Splitting on whitespace turned `'Zapier Integration'` into the expression
    // `'Zapier` plus an alias `Integration'`, so the item no longer looked like
    // a constant and came back as "column ''Zapier Integration'' not found in
    // schema" — a message naming the literal it had just mis-split.
    let trimmed = item.trim_start();
    if trimmed.starts_with('\'') {
        let offset = item.len() - trimmed.len();
        let bytes = trimmed.as_bytes();
        let mut i = 1usize;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                // `''` is an escaped quote, not the end of the literal.
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                break;
            }
            i += 1;
        }
        if i < bytes.len() {
            let end = offset + i + 1;
            let expr = &item[..end];
            let rest = item[end..].trim();
            let alias = if rest.is_empty() {
                None
            } else {
                let mut parts = rest.split_whitespace();
                match parts.next() {
                    Some(w) if w.eq_ignore_ascii_case("AS") => {
                        parts.next().map(|a| a.trim_matches('"').to_string())
                    }
                    // Only a name can follow a literal as its alias. An
                    // operator means the literal was the *start* of an
                    // expression: `'utrn_' || replace(id, '-', '')` was read as
                    // the literal `'utrn_'` aliased `||`, so every row answered
                    // with the prefix and the rest was silently discarded.
                    Some(w) if is_identifier(w.trim_matches('"')) => {
                        Some(w.trim_matches('"').to_string())
                    }
                    Some(_) => return (item.trim(), None),
                    None => None,
                }
            };
            return (expr, alias);
        }
    }

    // Everything after the closing parenthesis, or the whole item when there is
    // none, is where an alias can appear.
    let tail_start = item.rfind(')').map(|i| i + 1).unwrap_or(0);
    let tail = &item[tail_start..];

    // Nothing after the last `)` means there is no alias, and the whole item is
    // the expression. Without this the fall-through below split the item on
    // whitespace, so `COALESCE(SUM(b), 0)` became the expression
    // `COALESCE(SUM(b),` with the alias `0)` — reported to the client as
    // `Column '0)' not found in schema`, naming a column nobody wrote.
    if tail_start > 0 && tail.trim().is_empty() {
        return (item.trim_end(), None);
    }

    // A tail beginning with `::` is a cast, not an alias — it belongs to the
    // expression. Without this `COALESCE(x, 0)::bigint` came back as the
    // expression `COALESCE(x, 0)` aliased `::bigint`: the cast was silently
    // discarded and the column went out labelled with it.
    let trimmed_tail = tail.trim_start();
    if trimmed_tail.starts_with("::") {
        let cast_len = trimmed_tail
            .find(char::is_whitespace)
            .unwrap_or(trimmed_tail.len());
        let end = item.len() - trimmed_tail.len() + cast_len;
        let expr = item[..end].trim_end();
        let rest = item[end..].trim();
        let alias = if rest.is_empty() {
            None
        } else {
            let mut parts = rest.split_whitespace();
            match parts.next() {
                Some(w) if w.eq_ignore_ascii_case("AS") => {
                    parts.next().map(|a| a.trim_matches('"').to_string())
                }
                Some(w) if is_identifier(w.trim_matches('"')) => {
                    Some(w.trim_matches('"').to_string())
                }
                _ => return (item.trim(), None),
            }
        };
        return (expr, alias);
    }

    let mut words = tail.split_whitespace();

    match (words.next(), words.next()) {
        (Some(first), Some(second)) if first.eq_ignore_ascii_case("AS") => (
            item[..tail_start]
                .trim_end()
                .trim_end_matches(first)
                .trim_end(),
            Some(second.trim_matches('"').to_string()),
        ),
        (Some(first), None) if tail_start > 0 => {
            // `COUNT(x) total`
            (
                item[..tail_start].trim_end(),
                Some(first.trim_matches('"').to_string()),
            )
        }
        _ => {
            // No parentheses: `col AS name` or `col name`.
            let mut parts = item.split_whitespace();
            let head = parts.next().unwrap_or(item);
            let alias = match parts.next() {
                Some(w) if w.eq_ignore_ascii_case("AS") => {
                    parts.next().map(|a| a.trim_matches('"').to_string())
                }
                // Only a name can be an alias. An operator or a number is part
                // of the expression, so `a + 1` must not come back as the
                // column `a` aliased `+`.
                Some(w) if is_identifier(w.trim_matches('"')) => {
                    Some(w.trim_matches('"').to_string())
                }
                Some(_) => return (item.trim(), None),
                None => None,
            };
            (head, alias)
        }
    }
}

/// Parses a `HAVING` clause: the aggregate conditions the fold evaluates, and
/// the terms over plain columns that the caller moves into `WHERE`. See
/// [`crate::core::rql::having`], which the join planner shares.
fn parse_having(
    text: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<crate::core::rql::having::HavingSplit, String> {
    crate::core::rql::having::parse(text, &mut |call: &str| {
        match parse_select_items(call, schema)?.into_iter().next() {
            Some(item @ SelectItem::Aggregate { .. }) => Ok(item),
            _ => Err(format!("'{}' is not an aggregate HAVING can test", call)),
        }
    })
}

/// Whether grouping by `field` makes every group a single row of its relation,
/// so that the relation's other columns are functionally dependent on it.
///
/// True for a column that is `UNIQUE` (a `PRIMARY KEY` is too) and NOT NULL —
/// which here means it has no `__null_` companion. NOT NULL matters: NULLs are
/// distinct to a unique constraint and one group to the fold.
///
/// Uniqueness has to hold under the *fold's* equality, which is byte equality
/// of the key. Every uniqueness check that treats byte-identical values as
/// equal guarantees that; and the planner appends the dependent columns to the
/// group key rather than assuming them constant, so a uniqueness the data does
/// not actually have splits groups instead of merging rows.
///
/// Shared with the join planner, which applies it per relation.
pub(crate) fn determines_its_relation(
    field: &SchemaField,
    schema: &SmallVec<[SchemaField; 20]>,
) -> bool {
    field.is_unique
        && !field.is_deleted
        && crate::core::rql::nullable::flag_field(&field.name, schema).is_none()
}

/// The stored column a `GROUP BY` item names, when the item is exactly a
/// column reference — `status`, `t.status`, `"Status"`.
///
/// Only then. The item used to be cut at its first space and its qualifier
/// dropped, so `GROUP BY g % 2` grouped by `g`: five groups where there are
/// two, every count wrong, and no error.
fn plain_group_column<'s>(
    item: &str,
    schema: &'s SmallVec<[SchemaField; 20]>,
) -> Option<&'s SchemaField> {
    let item = item.trim();
    let bare = item.rsplit('.').next().unwrap_or(item);
    let name = bare.trim_matches('"');
    let qualifier_ok = item.len() == bare.len()
        || is_identifier(item[..item.len() - bare.len() - 1].trim_matches('"'));
    if !qualifier_ok || !(is_identifier(name) || (bare.starts_with('"') && bare.ends_with('"'))) {
        return None;
    }
    schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(name) && !f.is_deleted)
}

/// Resolves every `ORDER BY` key against the schema and renders the spec the
/// scan carries down to the sorter.
///
/// `None` when the statement has no `ORDER BY` at all, which is what tells the
/// scan it may stop early once `LIMIT` is met.
fn resolve_order_spec(
    terms: &[crate::core::rql::order_by::SortTerm],
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Option<String>, String> {
    if terms.is_empty() {
        return Ok(None);
    }
    let mut keys = Vec::with_capacity(terms.len());
    for term in terms {
        // This spec sorts inside the scan, which compares stored bytes and
        // cannot place NULLs; a placement it cannot honour is refused rather
        // than quietly dropped. (The expression-sort path handles it.)
        if term.nulls_first.is_some() {
            return Err(
                "ORDER BY ... NULLS FIRST/LAST is not supported in this position yet".to_string(),
            );
        }
        let bare = term
            .token
            .rsplit('.')
            .next()
            .unwrap_or(&term.token)
            .trim_matches('"');
        let field = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
            .ok_or_else(|| format!("ORDER BY field '{}' not found in schema", term.token))?;
        keys.push((field.name.clone(), term.descending));
    }
    Ok(Some(crate::core::rql::order_by::render_spec(&keys)))
}

/// Maps an `ORDER BY` term onto the label of a select item.
///
/// An aggregate query can only order by something it selected, since the input
/// rows no longer exist once the fold has run. Shared with the join path so
/// the two agree on what counts as naming an output — see
/// [`crate::core::rql::aggregate::resolve_order_label`].
fn resolve_output_label(token: &str, items: &[SelectItem]) -> Option<String> {
    crate::core::rql::aggregate::resolve_order_label(token, items)
}

/// Reads a select item that is a constant rather than a column reference.
///
/// Returns the stored bytes and the type to report. A quoted string is text; a
/// bare number is an integer or a double, following how the value was written.
fn parse_constant(expr: &str) -> Option<(Vec<u8>, crate::core::db_type::DbType)> {
    use crate::core::db_type::DbType;

    let text = expr.trim();
    if text.is_empty() {
        return None;
    }

    // `NULL` is stored as an empty value, the way every other NULL is here.
    // Without this a bare NULL in a select list was read as a column name and
    // reported as "column 'NULL' not found in schema".
    if text.eq_ignore_ascii_case("NULL") {
        return Some((Vec::new(), DbType::STRING));
    }
    if text.eq_ignore_ascii_case("TRUE") {
        return Some((vec![1u8], DbType::BOOL));
    }
    if text.eq_ignore_ascii_case("FALSE") {
        return Some((vec![0u8], DbType::BOOL));
    }

    if text.len() >= 2 && text.starts_with('\'') && text.ends_with('\'') {
        // `unquote_sql` rather than a slice: a quote inside the literal is
        // written doubled and has to collapse back to one, exactly as on the
        // insert path.
        let value = crate::core::value_codec::unquote_sql(text);
        return Some((value.as_bytes().to_vec(), DbType::STRING));
    }

    let first = text.as_bytes()[0];
    if !(first.is_ascii_digit() || ((first == b'-' || first == b'+') && text.len() > 1)) {
        return None;
    }

    if let Ok(v) = text.parse::<i64>() {
        return Some((v.to_le_bytes().to_vec(), DbType::I64));
    }
    if let Ok(v) = text.parse::<f64>() {
        return Some((v.to_le_bytes().to_vec(), DbType::F64));
    }
    None
}

/// Names the construct in a select item that is not supported, if it is one the
/// resolver would otherwise report as a missing column.
///
/// Every one of these is *valid* SQL that this engine does not implement. Left
/// to fall through, each is reported as `Column '<the whole expression>' not
/// found in schema` — which is wrong twice: the column usually exists, and the
/// problem is the construct wrapped around it. That message sends the reader to
/// audit a schema that is fine.
/// Whether what follows `DISTINCT` opens an `ON (...)` key list.
///
/// The word has to be `ON` in full and be followed by the parenthesis, so a
/// column actually named `on_call` or `online` is not mistaken for the
/// operator. Postgres allows whitespace between the two, so a bare `ON` with
/// the paren further along still counts.
fn starts_with_distinct_on(rest: &str) -> bool {
    let mut chars = rest.char_indices();
    let mut end = rest.len();
    for (i, c) in chars.by_ref() {
        if !c.is_ascii_alphabetic() {
            end = i;
            break;
        }
    }
    rest[..end].eq_ignore_ascii_case("ON") && rest[end..].trim_start().starts_with('(')
}

fn unsupported_expression(expr: &str) -> Option<String> {
    let upper = expr.to_ascii_uppercase();

    if upper.starts_with("CAST(") || upper.starts_with("CAST ") {
        return Some(format!(
            "'{}' uses CAST, which is not supported yet — not a missing column",
            expr.trim()
        ));
    }
    if upper.starts_with("CASE") && upper.contains("WHEN") {
        return Some(format!(
            "'{}' uses CASE WHEN, which is not supported yet — not a missing column",
            expr.trim()
        ));
    }
    if upper.contains(" OVER ") || upper.contains(" OVER(") {
        return Some(format!(
            "'{}' is a window function, which is not supported yet — not a missing column",
            expr.trim()
        ));
    }
    None
}

/// Splits a SELECT list on commas that are not inside parentheses or quotes.
fn split_select_items(list: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;

    for c in list.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    current.push(c);
                }
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(c);
                }
                ',' if depth == 0 => {
                    out.push(std::mem::take(&mut current));
                }
                _ => current.push(c),
            },
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

/// Extracts column names referenced in a WHERE clause
/// Uses simple string parsing to find potential column names by looking for schema field names
///
/// # Arguments
/// * `where_clause` - The WHERE clause string (without the WHERE keyword)
/// * `schema` - Vec<SchemaField> representing the table schema
///
/// # Returns
/// The column names found in the WHERE clause, in a fixed order
pub(crate) fn extract_where_columns(
    where_clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> std::collections::BTreeSet<String> {
    // Ordered, not hashed: the fetch built from this set came out in a
    // different column order on every parse of the same statement, so two
    // parses of one statement never compared equal reliably, and a prepared
    // statement's cached parse was refused at random.
    let mut where_columns = std::collections::BTreeSet::new();
    if where_clause.is_empty() {
        return where_columns;
    }

    let where_bytes = where_clause.as_bytes();

    // An underscore is part of an identifier, not a boundary around one. It was
    // treated as a separator here, so a column whose name is a prefix of
    // another's — `foo` beside `foo_id`, `context` beside `context_id` — was
    // read as present in a clause that never mentions it. Harmless while both
    // columns exist, since the extra one is merely fetched; fatal once the
    // shorter one is *dropped*, because the fetch then asks for a column the
    // schema no longer offers and the statement fails naming a column the
    // author never wrote.
    let is_word_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    // Check each schema field to see if it appears in the WHERE clause.
    // Tombstones are skipped: a dropped column cannot be referenced.
    for field in schema.iter().filter(|f| !f.is_deleted) {
        let field_bytes = field.name.as_bytes();
        if field_bytes.len() > where_bytes.len() {
            continue;
        }

        // Look for the column name in the WHERE clause using SIMD comparison.
        // `..=` so a clause ending in the column name is still matched.
        for i in 0..=where_bytes.len() - field_bytes.len() {
            let slice = &where_bytes[i..i + field_bytes.len()];
            if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
                let start_ok = i == 0 || !is_word_byte(where_bytes[i - 1]);
                let end_ok = i + field_bytes.len() == where_bytes.len()
                    || !is_word_byte(where_bytes[i + field_bytes.len()]);

                if start_ok && end_ok {
                    where_columns.insert(field.name.clone());
                    break; // Found this column, no need to keep searching
                }
            }
        }
    }

    where_columns
}

/// Parses a QueryPurpose::QueryRows variant and returns QueryRows with separate RowFetch structures
/// for query evaluation (WHERE clause columns) and result output (SELECT columns)
///
/// # Arguments
/// * `query_purpose` - QueryPurpose enum (should be QueryPurpose::QueryRows)  
/// * `schema` - Vec<SchemaField> representing the table schema
///
/// # Returns
/// QueryRows - Contains query_row_fetch (WHERE columns), requested_row_fetch (SELECT columns), and WHERE clause
pub fn row_fetch_from_select_query(
    query_purpose: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<QueryRows, String> {
    let sql = match query_purpose {
        QueryPurpose::QueryRows(qr_data) => qr_data.query.trim(),
        _ => return Err("Not a SELECT/QueryRows query".to_string()),
    };
    // `FETCH FIRST n ROWS ONLY`, `OFFSET n ROWS`, `LIMIT ALL` and `TAKE`/`SKIP`
    // all become the `LIMIT n OFFSET m` tail the clause split below searches
    // for. Without this the tail matches nothing and is dropped in silence,
    // which returns every row instead of a page.
    // `FOR UPDATE`/`FOR SHARE` is a locking clause, not part of the query the
    // scan runs. Split off before anything else looks at the statement: left in
    // place it reached the predicate tokenizer as the identifier `FOR`, which
    // panicked the worker and dropped the client's connection.
    let (sql, row_lock) = crate::core::rql::row_lock::split_locking_clause(sql)?;
    let sql = sql.as_str();

    let sql_owned = crate::core::tokenizer::pagination::normalize_pagination(sql)?;
    let sql = sql_owned.as_str();
    let sql_upper = sql.to_ascii_uppercase();
    let sql_bytes = sql_upper.as_bytes();
    // Matched as whole words against any whitespace, not against a literal
    // `"SELECT "` / `" FROM "`. A newline is as good a separator as a space,
    // and ORMs emit clauses on their own lines: `… now()\nFROM t` used to
    // report "Missing FROM in SELECT query" about a statement that plainly has
    // one.
    if !sql_upper.trim_start().starts_with("SELECT") {
        return Err("Not a SELECT query".to_string());
    }
    let select_end = sql_upper.find("SELECT").ok_or("Not a SELECT query")? + "SELECT".len();
    if !sql_bytes
        .get(select_end)
        .is_some_and(|c| c.is_ascii_whitespace())
    {
        return Err("Not a SELECT query".to_string());
    }

    let from_start = {
        let mut found = None;
        let mut quote: Option<u8> = None;
        // `EXTRACT(EPOCH FROM ts)` puts a `FROM` inside the select list, and it
        // is not the statement's. Splitting there took the rest of the call as
        // the relation and reported "relation does not exist" about a statement
        // whose actual `FROM` names a table that exists — an error naming
        // nothing the author wrote.
        let mut depth = 0i32;
        let mut i = select_end;
        while i + 4 <= sql_bytes.len() {
            let c = sql_bytes[i];
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    }
                }
                None => {
                    if c == b'\'' || c == b'"' {
                        quote = Some(c);
                    } else if c == b'(' {
                        depth += 1;
                    } else if c == b')' {
                        depth -= 1;
                    } else if depth == 0
                        && &sql_bytes[i..i + 4] == b"FROM"
                        && sql_bytes[i - 1].is_ascii_whitespace()
                        && sql_bytes
                            .get(i + 4)
                            .is_some_and(|n| n.is_ascii_whitespace())
                    {
                        found = Some(i);
                        break;
                    }
                }
            }
            i += 1;
        }
        found.ok_or("Missing FROM in SELECT query")?
    };

    let columns_part = sql[select_end..from_start].trim();

    let after_from = &sql[from_start + "FROM".len()..];
    let after_from_upper = &sql_upper[from_start + "FROM".len()..];

    // Find optional clauses (positions are within after_from).
    //
    // Matched as whole words against any whitespace. Searching for the literal
    // `" WHERE "` meant a statement that put the clause on its own line — which
    // every ORM and console does — matched nothing, so the predicate was
    // dropped and the scan returned **every row**. A filtered query answering
    // with the whole table is the worst shape of wrong there is: it looks like
    // a successful query.
    use crate::core::rql::join::{find_keyword, find_phrase};
    let where_pos = find_keyword(after_from, "WHERE", 0);
    let order_pos = find_phrase(after_from, &["ORDER", "BY"], 0);
    let limit_pos = find_keyword(after_from, "LIMIT", 0);
    let offset_pos = find_keyword(after_from, "OFFSET", 0);
    let group_pos = find_phrase(after_from, &["GROUP", "BY"], 0);
    let having_pos = find_keyword(after_from, "HAVING", 0);
    // A top-level `UNION` ends this branch. Without it the WHERE clause ran on
    // into `… UNION ALL SELECT …` and the predicate parser reported
    // "unsupported keyword 'UNION' in WHERE clause". The branches are combined
    // by `execute_union`, which reads the whole statement; this only has to
    // parse the first one.
    let union_pos = find_keyword(after_from, "UNION", 0);

    // Table name ends at the earliest clause marker.
    let table_end = [
        where_pos, order_pos, limit_pos, offset_pos, group_pos, having_pos, union_pos,
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(after_from.len());

    // A JOIN would silently be dropped otherwise: the relation name is taken
    // from the first token after FROM, so `FROM a JOIN b ON …` would scan `a`
    // alone and return a plausible but wrong result set. Refusing is the only
    // safe answer until joins are implemented.
    let from_clause_upper = &after_from_upper[..table_end];
    if from_clause_upper.contains(" JOIN ") || from_clause_upper.contains(",") {
        return Err("JOIN is not supported yet".to_string());
    }

    // `FROM users AS u` / `FROM users u`: the alias is consumed by the
    // qualified-name resolution in the SELECT list and WHERE clause, so only
    // the relation name is needed here.
    let table_part = after_from[..table_end]
        .trim()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('"');

    // WHERE clause is everything after WHERE up to ORDER BY / LIMIT / OFFSET.
    let where_raw = if let Some(wp) = where_pos {
        let start = wp + "WHERE".len();
        let end = [
            order_pos, limit_pos, offset_pos, group_pos, having_pos, union_pos,
        ]
        .into_iter()
        .flatten()
        .filter(|p| *p >= start)
        .min()
        .unwrap_or(after_from.len());
        after_from[start..end].trim()
    } else {
        ""
    };

    let group_by_raw = group_pos.map(|gp| {
        let start = after_phrase(after_from, gp, &["GROUP", "BY"]);
        let end = [having_pos, order_pos, limit_pos, offset_pos]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        after_from[start..end].trim()
    });

    let having_raw = having_pos.map(|hp| {
        let start = hp + "HAVING".len();
        let end = [order_pos, limit_pos, offset_pos]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        after_from[start..end].trim()
    });

    // Desugar `IN` / `BETWEEN` / `LIKE`, fold boolean literals, and resolve
    // qualified or quoted column names, so the evaluator only ever sees the
    // basic `column <op> value` grammar it implements.
    //
    // A HAVING term over plain grouping columns — `HAVING status = 'x'` — is
    // moved into WHERE first. Every row of a group carries the same bytes in a
    // plain GROUP BY column, so the term holds for the group exactly when it
    // holds for each of its rows. That is not true of a computed key, nor of a
    // query without GROUP BY (its one group exists even with no rows), and
    // both are refused rather than moved.
    let having_split = match having_raw {
        None => None,
        Some(text) => Some(parse_having(text, schema)?),
    };
    let mut where_combined: Option<String> = None;
    if let Some(split) = &having_split {
        if !split.row_terms.is_empty() {
            let Some(list) = group_by_raw else {
                return Err(format!(
                    "HAVING term '{}' tests no aggregate, and the query has no GROUP BY",
                    split.row_terms[0].text
                ));
            };
            let mut plain: Vec<&SchemaField> =
                crate::core::rql::join::split_top_level_commas(list)
                    .iter()
                    .filter_map(|item| plain_group_column(item, schema))
                    .collect();
            // Grouped by a unique NOT NULL column, every column is constant
            // within a group, so a term over any of them moves into WHERE.
            if plain.iter().any(|f| determines_its_relation(f, schema)) {
                plain = schema.iter().filter(|f| !f.is_deleted).collect();
            }
            let mut text = if where_raw.trim().is_empty() {
                String::new()
            } else {
                format!("({})", where_raw)
            };
            for term in &split.row_terms {
                for column in &term.columns {
                    let bare = column.rsplit('.').next().unwrap_or(column).trim_matches('"');
                    if !plain.iter().any(|f| f.name.eq_ignore_ascii_case(bare)) {
                        return Err(format!(
                            "column '{}' must appear in GROUP BY or be used in an aggregate function",
                            column
                        ));
                    }
                }
                if !text.is_empty() {
                    text.push_str(" AND ");
                }
                text.push('(');
                text.push_str(&term.text);
                text.push(')');
            }
            where_combined = Some(text);
        }
    }
    let where_raw = where_combined.as_deref().unwrap_or(where_raw);
    let where_owned = crate::core::tokenizer::predicate_rewrite::rewrite_where(where_raw, schema)?;
    let where_part = where_owned.as_str();

    // ORDER BY: every sort key, in the order written. Resolution is deferred,
    // because an aggregate query may order by a function result — `ORDER BY
    // COUNT(*)` names nothing in the schema.
    let mut order_terms: Vec<crate::core::rql::order_by::SortTerm> = match order_pos {
        None => Vec::new(),
        Some(op) => {
            let start = after_phrase(after_from, op, &["ORDER", "BY"]);
            let end = [limit_pos, offset_pos]
                .into_iter()
                .flatten()
                .filter(|p| *p >= start)
                .min()
                .unwrap_or(after_from.len());
            crate::core::rql::order_by::parse(&after_from[start..end])?
        }
    };

    // LIMIT value
    let limit = if let Some(lp) = limit_pos {
        let start = lp + "LIMIT".len();
        let end = [offset_pos]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        let expr = after_from[start..end].trim();
        if expr.is_empty() {
            return Err("LIMIT is present but no value was provided".to_string());
        }
        let token = expr.split_whitespace().next().unwrap_or("");
        let n = token
            .parse::<u64>()
            .map_err(|_| format!("Invalid LIMIT value '{}'", token))?;
        Some(n)
    } else {
        None
    };

    // OFFSET value. The engine's scan takes a row cap but not a starting
    // position, so the rows to skip are added to the cap here and dropped
    // after the scan; `LIMIT n OFFSET m` therefore fetches `n + m` rows.
    let offset = if let Some(op) = offset_pos {
        let start = op + "OFFSET".len();
        let expr = after_from[start..].trim();
        if expr.is_empty() {
            return Err("OFFSET is present but no value was provided".to_string());
        }
        let token = expr.split_whitespace().next().unwrap_or("");
        let n = token
            .parse::<u64>()
            .map_err(|_| format!("Invalid OFFSET value '{}'", token))?;
        Some(n)
    } else {
        None
    };

    // The scan has no starting position, so `OFFSET` is served by fetching the
    // skipped rows too and dropping them afterwards. The fold needs the limit
    // as written, before that adjustment.
    let original_limit = limit;
    let limit = match (limit, offset) {
        (Some(l), Some(o)) => Some(l.saturating_add(o)),
        (l, _) => l,
    };

    // Parse requested columns (SELECT columns).
    //
    // A select item may be qualified, quoted, and aliased — `u."name" AS who`.
    // All three are stripped down to the schema's canonical column name, since
    // that is what the fetch layer looks up. Aliases only affect the label sent
    // back in RowDescription, which the client does not depend on here.
    // `SELECT DISTINCT` is a post-pass over the fetched rows, so the keyword is
    // stripped here and the rest of the list parses normally.
    let columns_upper = columns_part.trim_start().to_ascii_uppercase();
    let (columns_part, distinct) = if columns_upper.starts_with("DISTINCT ") {
        let rest = columns_part.trim_start()["DISTINCT ".len()..].trim();
        // `DISTINCT ON (...)` is a different operator, not `DISTINCT` with a
        // parenthesised first column: it keeps one row per key rather than
        // dropping exact duplicates. Left to fall through, the `ON` reads as a
        // function call and the error blames a function named `on`, sending the
        // reader to look for something that does not exist.
        if starts_with_distinct_on(rest) {
            return Err(
                "SELECT DISTINCT ON is not supported yet — it keeps one row per key, \
                 which plain DISTINCT does not do, so it cannot be treated as one"
                    .to_string(),
            );
        }
        (rest, true)
    } else if columns_upper == "DISTINCT" {
        return Err("SELECT DISTINCT is missing its column list".to_string());
    } else {
        (columns_part, false)
    };

    let select_items = parse_select_items(columns_part, schema)?;

    // `ORDER BY 2` names the second select item. Resolved before anything below
    // looks at a sort key, so every path sees an ordinary one.
    crate::core::rql::order_by::resolve_ordinals(
        &mut order_terms,
        &select_items
            .iter()
            .map(|i| i.label().to_string())
            .collect::<Vec<_>>(),
    )?;
    let order_terms = order_terms;
    // A computed item folds aggregates too, so it takes the aggregate path —
    // otherwise its expression would be evaluated against raw scanned rows,
    // before there is any aggregate result for it to read.
    let has_aggregate = select_items.iter().any(|i| {
        matches!(
            i,
            SelectItem::Aggregate { .. } | SelectItem::Computed { .. }
        )
    });

    // A grouping key is usually a stored column, but PostgreSQL also accepts
    // a select-list alias — `GROUP BY snapshot_at` where `snapshot_at` labels
    // `date_trunc('minute', created_at)` — and the expression written out.
    // Neither has a scanned column to read, so those become computed keys the
    // fold evaluates per row.
    let mut computed_group_keys: Vec<crate::core::rql::aggregate::ComputedKey> = Vec::new();
    let mut group_by: Vec<SchemaField> = match group_by_raw {
        None => Vec::new(),
        Some(list) => {
            // Virtual write_orders sit above every real field's, so a
            // computed key can never shadow a scanned column.
            let mut next_virtual = schema.iter().map(|f| f.write_order).max().unwrap_or(0) + 1;
            let mut out = Vec::new();
            // Split at top level only: an expression key may hold commas of
            // its own, as `date_trunc('minute', created_at)` does.
            for item_text in crate::core::rql::join::split_top_level_commas(list) {
                let item = item_text.trim();
                if let Some(field) = plain_group_column(item, schema) {
                    out.push(field.clone());
                    continue;
                }

                // A name that is no column may label a select item.
                let labelled = select_items
                    .iter()
                    .position(|s| s.label().eq_ignore_ascii_case(item.trim_matches('"')));
                if let Some(position) = labelled {
                    match &select_items[position] {
                        // An alias of a plain column groups by that column.
                        SelectItem::Column { field, .. } => {
                            out.push(field.clone());
                            continue;
                        }
                        SelectItem::RowExpr {
                            expr,
                            db_type,
                            label,
                        } => {
                            let field = SchemaField::new(
                                label.clone(),
                                db_type.clone(),
                                db_type.get_size(),
                                0,
                                next_virtual,
                                false,
                            );
                            next_virtual += 1;
                            computed_group_keys.push(crate::core::rql::aggregate::ComputedKey {
                                write_order: field.write_order,
                                expr: expr.clone(),
                                schema: expr_reads(expr, schema),
                                item: Some(position),
                            });
                            out.push(field);
                            continue;
                        }
                        SelectItem::Aggregate { .. } | SelectItem::Computed { .. } => {
                            return Err(format!(
                                "aggregate functions are not allowed in GROUP BY ('{}')",
                                item
                            ));
                        }
                        _ => {
                            return Err(format!("cannot GROUP BY '{}'", item));
                        }
                    }
                }

                // The expression written out in the GROUP BY. When the select
                // list carries the same expression, the key echoes into that
                // item's output slot.
                if crate::core::rql::expr::needs_evaluation(item) || looks_like_row_expression(item)
                {
                    if let Ok(parsed) = crate::core::rql::expr::parse(item, schema) {
                        // `Expr` has no equality of its own; its Debug form is
                        // structural, which is what matching two parses needs.
                        let written = format!("{:?}", parsed);
                        let matching = select_items.iter().position(|s| match s {
                            SelectItem::RowExpr { expr, .. } => format!("{:?}", expr) == written,
                            _ => false,
                        });
                        let db_type = crate::core::rql::expr::static_type(&parsed);
                        let label = matching
                            .map(|p| select_items[p].label().to_string())
                            .unwrap_or_else(|| item.to_string());
                        let key_schema = expr_reads(&parsed, schema);
                        let field = SchemaField::new(
                            label,
                            db_type.clone(),
                            db_type.get_size(),
                            0,
                            next_virtual,
                            false,
                        );
                        next_virtual += 1;
                        computed_group_keys.push(crate::core::rql::aggregate::ComputedKey {
                            write_order: field.write_order,
                            expr: parsed,
                            schema: key_schema,
                            item: matching,
                        });
                        out.push(field);
                        continue;
                    }
                }

                return Err(format!("GROUP BY column '{}' not found in schema", item));
            }
            out
        }
    };

    // What makes a query grouped is a GROUP BY, not an aggregate in the select
    // list. `SELECT city FROM t GROUP BY city` is a DISTINCT and perfectly
    // legal, and so is putting the only aggregate in the HAVING —
    // `GROUP BY city HAVING count(*) > 1`. Both used to be rejected. The fold
    // needs no special case: it already emits the grouping key for a plain
    // column, so a plan whose items are all keys yields one row per group.
    let is_grouped = has_aggregate || !group_by.is_empty();

    // HAVING with nothing to group by is the whole table as one group, which
    // only means something if there is an aggregate to test.
    if having_raw.is_some() && !is_grouped {
        return Err("HAVING requires an aggregate in the SELECT list".to_string());
    }

    // Every non-aggregate select item of a grouped query must be a grouping
    // key; otherwise its value is not determined by the group.
    //
    // Unless the column is functionally dependent on the grouping (PostgreSQL
    // since 9.1): grouping by a column that is unique and NOT NULL makes each
    // group one row, so every other column of the relation is determined.
    // Django's `.annotate()` and Rails' `group(:id)` emit exactly
    // `SELECT a.id, a.name, COUNT(*) … GROUP BY a.id`, which was refused. Such a
    // column is appended to the group key rather than trusted to be constant:
    // the result is the same when the key really is unique, and still a
    // correct grouping if it ever is not.
    let determined_by_key = group_by
        .iter()
        .filter(|g| {
            !computed_group_keys
                .iter()
                .any(|k| k.write_order == g.write_order)
        })
        .any(|g| determines_its_relation(g, schema));
    if is_grouped {
        for (position, item) in select_items.iter().enumerate() {
            match item {
                SelectItem::Column { field, label } => {
                    if !group_by.iter().any(|g| g.write_order == field.write_order) {
                        if determined_by_key {
                            group_by.push(field.clone());
                            continue;
                        }
                        return Err(format!(
                            "column '{}' must appear in GROUP BY or be used in an aggregate",
                            label
                        ));
                    }
                }
                // A row expression is evaluated per input row, so in a
                // grouped query it is only determined when it is itself a
                // grouping key. Refused rather than emitted empty: an empty
                // value here is a silent wrong answer.
                SelectItem::RowExpr { label, .. } => {
                    if !computed_group_keys.iter().any(|k| k.item == Some(position)) {
                        return Err(format!(
                            "column '{}' must appear in GROUP BY or be used in an aggregate",
                            label
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    // Parsed above, before WHERE was rewritten; its plain-column terms are
    // already part of the WHERE clause.
    let having = having_split.and_then(|split| split.having);

    let aggregate = if is_grouped {
        Some(crate::core::rql::aggregate::AggregatePlan {
            items: select_items.clone(),
            group_by: group_by.clone(),
            computed_keys: computed_group_keys.clone(),
            having,
            order_by: {
                // A group can only be sorted by something the fold produced:
                // the input rows are gone by then.
                let mut keys = Vec::with_capacity(order_terms.len());
                for term in &order_terms {
                    // `NULLS FIRST`/`LAST` is carried through: the fold's
                    // sort reads NULL-ness from each output cell's type.
                    let label =
                        resolve_output_label(&term.token, &select_items).ok_or_else(|| {
                            format!("ORDER BY term '{}' is not in the SELECT list", term.token)
                        })?;
                    keys.push((label, term.descending, term.nulls_first));
                }
                keys
            },
            limit: original_limit,
            offset,
        })
    } else {
        None
    };

    // The scan fetches whatever the fold reads: every grouping key and every
    // aggregate argument.
    let mut literal_slots: Vec<(usize, Vec<u8>, crate::core::db_type::DbType)> = Vec::new();
    let mut scalar_slots: Vec<(
        usize,
        crate::core::rql::aggregate::ScalarFunc,
        crate::core::db_type::DbType,
    )> = Vec::new();
    for (position, item) in select_items.iter().enumerate() {
        if let SelectItem::Literal { bytes, db_type, .. } = item {
            literal_slots.push((position, bytes.clone(), db_type.clone()));
        }
        if let SelectItem::Scalar { func, db_type, .. } = item {
            scalar_slots.push((position, *func, db_type.clone()));
        }
    }

    // A statement of nothing but constants still needs the scan to produce one
    // row per input row, so it fetches the narrowest column available.
    let all_literals = !select_items.is_empty()
        && select_items
            .iter()
            .all(|i| matches!(i, SelectItem::Literal { .. } | SelectItem::Scalar { .. }));

    // A window reads columns the projection may not name — the partition and
    // order keys — and the scan is the only chance to fetch them.
    let mut window_slots: Vec<(usize, crate::core::rql::window::WindowSpec)> = Vec::new();
    for (position, item) in select_items.iter().enumerate() {
        if let SelectItem::Window { spec, .. } = item {
            window_slots.push((position, spec.clone()));
        }
    }

    // Row expressions occupy output slots no fetched column fills, and are
    // evaluated per row against the values the scan returned.
    let mut expr_slots: Vec<(
        usize,
        crate::core::rql::expr::Expr,
        crate::core::db_type::DbType,
    )> = Vec::new();
    for (position, item) in select_items.iter().enumerate() {
        if let SelectItem::RowExpr { expr, db_type, .. } = item {
            expr_slots.push((position, expr.clone(), db_type.clone()));
        }
    }

    let requested_columns: Vec<String> = if is_grouped {
        let mut cols: Vec<String> = Vec::new();
        for f in &group_by {
            // A computed key's field is virtual — no scan can fetch it. What
            // the scan must fetch is every column its expression reads.
            if let Some(key) = computed_group_keys
                .iter()
                .find(|k| k.write_order == f.write_order)
            {
                for kf in &key.schema {
                    if !cols.iter().any(|c| c == &kf.name) {
                        cols.push(kf.name.clone());
                    }
                }
                continue;
            }
            if !cols.iter().any(|c| c == &f.name) {
                cols.push(f.name.clone());
            }
        }
        for item in &select_items {
            match item {
                SelectItem::Aggregate {
                    arg,
                    filter,
                    arg_expr,
                    ..
                } => {
                    match arg_expr {
                        // A computed argument's `arg` names the expression, not
                        // a column — what has to be fetched is what it reads.
                        Some(argument) => {
                            for f in &argument.schema {
                                if !cols.iter().any(|c| c == &f.name) {
                                    cols.push(f.name.clone());
                                }
                            }
                        }
                        None => {
                            if let Some(f) = arg {
                                if !cols.iter().any(|c| c == &f.name) {
                                    cols.push(f.name.clone());
                                }
                            }
                        }
                    }
                    // `FILTER (WHERE status = …)` reads a column the aggregate
                    // itself does not, and the fold sees only what the scan
                    // returned — so without this the condition tested a column
                    // that was not there, read as NULL, and rejected every row.
                    if let Some(filter) = filter {
                        for f in &filter.schema {
                            if !cols.iter().any(|c| c == &f.name) {
                                cols.push(f.name.clone());
                            }
                        }
                    }
                }
                // A computed item's aggregates read columns too, and the scan
                // is the only chance to fetch them: the fold sees whatever the
                // scan returned, so a column missed here reads back as NULL.
                SelectItem::Computed { inputs, .. } => {
                    for input in inputs {
                        if let Some(f) = &input.arg {
                            if !cols.iter().any(|c| c == &f.name) {
                                cols.push(f.name.clone());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        // Every column a HAVING aggregate reads: its argument (or what a
        // computed argument reads) and what its FILTER reads.
        if let Some(h) = aggregate.as_ref().and_then(|a| a.having.as_ref()) {
            for item in &h.aggregates {
                if let SelectItem::Aggregate {
                    arg,
                    filter,
                    arg_expr,
                    ..
                } = item
                {
                    let mut names: Vec<&str> = Vec::new();
                    match arg_expr {
                        Some(argument) => names.extend(argument.schema.iter().map(|f| f.name.as_str())),
                        None => names.extend(arg.iter().map(|f| f.name.as_str())),
                    }
                    if let Some(filter) = filter {
                        names.extend(filter.schema.iter().map(|f| f.name.as_str()));
                    }
                    for name in names {
                        if !cols.iter().any(|c| c == name) {
                            cols.push(name.to_string());
                        }
                    }
                }
            }
        }
        // `SELECT COUNT(*)` reads no column at all, but the scan still has to
        // fetch something per row for the fold to have rows to count.
        if cols.is_empty() {
            if let Some(f) = schema.iter().min_by_key(|f| f.write_order) {
                cols.push(f.name.clone());
            }
        }
        cols
    } else if columns_part.trim() == "*" {
        crate::core::rql::nullable::visible_fields(schema)
            .into_iter()
            .map(|f| f.name)
            .collect()
    } else {
        let mut cols: Vec<String> = select_items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Column { field, .. } => Some(field.name.clone()),
                SelectItem::Literal { .. }
                | SelectItem::Scalar { .. }
                // A row expression fills its own output slot; the columns it
                // reads are added separately below.
                | SelectItem::RowExpr { .. }
                // A window fills its own slot; the columns it reads are added
                // to the fetch list below.
                | SelectItem::Window { .. } => None,
                SelectItem::Aggregate { .. } | SelectItem::Computed { .. } => {
                    unreachable!("aggregates handled above")
                }
            })
            .collect();
        // Every column a row expression reads has to be scanned: `eval` looks
        // them up in the fetched row, and one that was never fetched is
        // indistinguishable there from one that is NULL.
        for (_, spec) in &window_slots {
            for field in spec.columns() {
                if !cols.iter().any(|c| c == &field.name) {
                    cols.push(field.name.clone());
                }
            }
        }
        for item in &select_items {
            if let SelectItem::RowExpr { expr, .. } = item {
                for field in crate::core::rql::expr::columns_of(expr) {
                    if !cols.iter().any(|c| c == &field.name) {
                        cols.push(field.name.clone());
                    }
                    // The nullability companion too, or a NULL reads back as
                    // the column's zero value.
                    if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema)
                    {
                        if !cols.iter().any(|c| c == &flag.name) {
                            cols.push(flag.name.clone());
                        }
                    }
                }
            }
        }
        if all_literals {
            if let Some(f) = schema
                .iter()
                .filter(|f| !crate::core::rql::nullable::is_hidden(&f.name))
                .min_by_key(|f| f.size)
            {
                cols.push(f.name.clone());
            }
        }
        cols
    };

    // Extract columns needed for WHERE clause evaluation
    let where_columns = extract_where_columns(where_part, schema);

    let schema_calc = SchemaCalculator::default();

    // The sort key is read back out of the fetched row, so a column that is
    // ordered by but not selected still has to be fetched. It goes after the
    // visible columns and is trimmed off before the rows are sent.
    let visible_columns = requested_columns.len();

    // A predicate reading a column the projection does not means the scan has
    // to fetch it, and it comes back in the row alongside the requested ones.
    let predicate_needs_extra_columns =
        extract_where_columns(where_part, schema)
            .iter()
            .any(|name| {
                !requested_columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(name))
            });

    let mut requested_columns = requested_columns;

    // A nullable column's value alone cannot say whether it is `NULL` — the
    // stored bytes for a `NULL` are indistinguishable from a real zero or empty
    // string. Its companion is fetched alongside and trimmed off afterwards.
    let mut null_flag_positions: Vec<Option<usize>> = Vec::with_capacity(visible_columns);
    if aggregate.is_none() {
        for name in requested_columns.clone().iter() {
            match crate::core::rql::nullable::flag_field(name, schema) {
                None => null_flag_positions.push(None),
                Some(flag) => {
                    let pos = match requested_columns.iter().position(|c| c == &flag.name) {
                        Some(p) => p,
                        None => {
                            requested_columns.push(flag.name.clone());
                            requested_columns.len() - 1
                        }
                    };
                    null_flag_positions.push(Some(pos));
                }
            }
        }
    }

    // A sort key the scan can handle is a plain schema column with no explicit
    // NULLS placement; anything else — an expression, a select alias, or a
    // NULLS FIRST/LAST — is evaluated and sorted after the scan instead.
    let scan_can_sort = order_terms.iter().all(|term| {
        term.nulls_first.is_none() && {
            let bare = term
                .token
                .rsplit('.')
                .next()
                .unwrap_or(&term.token)
                .trim_matches('"');
            // A nullable key is allowed now: the scan fetches its `__null_x`
            // companion (below) and the sort reads it, so a stored NULL is no
            // longer byte-compared as the type's zeroes. Routing these to the
            // expression path instead meant the scan ran unbounded — it is
            // handed no limit there — so `ORDER BY <nullable> LIMIT 3` had to
            // materialise every matching row to return three.
            schema
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
        }
    });
    let mut order_expressions: Vec<(crate::core::rql::expr::Expr, bool, Option<bool>)> = Vec::new();

    // An aggregate query sorts, limits and offsets its own output after the
    // fold, so none of the three is handed to the scan. Passing a limit down
    // would additionally enable early scan termination, which would truncate
    // the *input* rows and silently produce a wrong aggregate.
    let (order_by, limit, offset) = if is_grouped {
        (None, None, None)
    } else if distinct {
        // Ordering still helps — deduplication preserves the first occurrence,
        // so a sorted input yields sorted output — but a row cap applied before
        // the duplicates are removed would return too few rows.
        let order_by = resolve_order_spec(&order_terms, schema)?;

        // A key outside the SELECT list cannot be fetched here: an extra column
        // would make otherwise identical rows distinct and defeat the
        // deduplication. The sort would then find nothing to read and return
        // the rows unordered, so the statement is refused instead — the same
        // answer PostgreSQL gives.
        if let Some(order_spec) = order_by.as_deref() {
            for (name, _) in crate::core::rql::order_by::parse_spec(order_spec) {
                if !requested_columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&name))
                {
                    return Err(format!(
                        "for SELECT DISTINCT, ORDER BY term '{}' must appear in the select list",
                        name
                    ));
                }
            }
        }

        // The offset is kept even though the limit is not: the executor
        // applies both *after* deduplicating (its `distinct` skip/take), which
        // is where PostgreSQL applies them. Passing `None` here dropped the
        // clause, so `DISTINCT … OFFSET n` silently returned page one.
        (order_by, None, offset)
    } else if scan_can_sort {
        let order_by = resolve_order_spec(&order_terms, schema)?;

        // Every sort key is read back out of the fetched row, so one that was
        // not selected still has to be fetched.
        if let Some(order_spec) = order_by.as_deref() {
            for (name, _) in crate::core::rql::order_by::parse_spec(order_spec) {
                // A nullable key is only sortable with its companion in the
                // row, so the companion is fetched too. Both ride after the
                // visible columns and are trimmed before the client sees the
                // row, exactly as an unselected sort key already was.
                if let Some(flag) = crate::core::rql::nullable::flag_field(&name, schema) {
                    if !requested_columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(&flag.name))
                    {
                        requested_columns.push(flag.name.clone());
                    }
                }
                if !requested_columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&name))
                {
                    requested_columns.push(name);
                }
            }
        }

        (order_by, limit, offset)
    } else {
        // `ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST` — the shape
        // an application sorting on a document field emits. The keys resolve
        // to expressions; the scan runs unsorted and UNBOUNDED (a limit would
        // cut rows before the sort decided which ones belong in the page),
        // and the executor evaluates, sorts and pages.
        for term in &order_terms {
            let bare = term
                .token
                .rsplit('.')
                .next()
                .unwrap_or(&term.token)
                .trim_matches('"');
            let expr = if let Some(field) = schema
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
            {
                crate::core::rql::expr::Expr::Column(field.clone())
            } else if let Some(item) = select_items
                .iter()
                .find(|s| s.label().eq_ignore_ascii_case(bare))
            {
                // A select alias sorts by what the item computes.
                match item {
                    SelectItem::Column { field, .. } => {
                        crate::core::rql::expr::Expr::Column(field.clone())
                    }
                    SelectItem::RowExpr { expr, .. } => expr.clone(),
                    _ => return Err(format!("ORDER BY '{}' cannot be sorted here", term.token)),
                }
            } else {
                crate::core::rql::expr::parse(&term.token, schema).map_err(|e| {
                    format!(
                        "ORDER BY term '{}' is not a column and could not be read as an expression: {}",
                        term.token, e
                    )
                })?
            };
            // The columns the key reads (with their NULL companions) ride
            // after the visible ones and are trimmed before the client sees
            // the row — the same trick a plain unselected sort key uses.
            for f in expr_reads(&expr, schema) {
                if !requested_columns
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&f.name))
                {
                    requested_columns.push(f.name.clone());
                }
            }
            order_expressions.push((expr, term.descending, term.nulls_first));
        }
        (None, None, offset)
    };

    // Build requested_row_fetch for SELECT columns
    let mut requested_columns_fetching_data = SmallVec::new();
    for col in &requested_columns {
        let field = schema
            .iter()
            .find(|f| {
                !f.is_deleted
                    && simd_compare_strings(
                        f.name.as_bytes(),
                        col.as_bytes(),
                        &ComparerOperation::Equals,
                    )
            })
            .ok_or_else(|| format!("Column '{}' not found in schema", col))?;
        let offset = schema_calc.calculate_schema_offset(&field.name, schema);
        requested_columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset.0,
            column_type: field.db_type.physical(),
            size: field.size,
            schema_id: offset.1,
        });
    }

    // Build query_row_fetch for WHERE clause columns
    let mut query_columns_fetching_data = SmallVec::new();
    for col_name in &where_columns {
        let field = schema
            .iter()
            .find(|f| {
                !f.is_deleted
                    && simd_compare_strings(
                        f.name.as_bytes(),
                        col_name.as_bytes(),
                        &ComparerOperation::Equals,
                    )
            })
            .ok_or_else(|| format!("WHERE column '{}' not found in schema", col_name))?;
        let offset = schema_calc.calculate_schema_offset(&field.name, schema);
        query_columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset.0,
            column_type: field.db_type.physical(),
            size: field.size,
            schema_id: offset.1,
        });
    }

    let query_rows = QueryRows {
        table_name: table_part.to_string(),
        query_row_fetch: RowFetch {
            columns_fetching_data: query_columns_fetching_data,
        },
        requested_row_fetch: RowFetch {
            columns_fetching_data: requested_columns_fetching_data,
        },
        where_clause: where_part.to_string(),
        limit,
        order_by,
        offset,
        visible_columns,
        order_expressions,
        aggregate,
        distinct,
        written_limit: original_limit,
        null_flag_positions,
        literal_slots,
        scalar_slots,
        expr_slots,
        window_slots,
        predicate_needs_extra_columns,
        row_lock,
        select_items,
    };

    // Debug, not info: this prints the whole `QueryRows` — every fetched
    // column, every select item, the full schema of each — for *every*
    // statement. At the default level it buried the log under kilobytes per
    // query, which is the opposite of useful when something needs diagnosing.
    debug!("Parsed QueryRows: {:?}", query_rows);

    Ok(query_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;

    #[test]
    fn an_expression_starting_with_a_literal_keeps_its_tail() {
        let schema = make_schema();
        let items = parse_select_items("'utrn_' || replace(name, '-', '')", &schema).unwrap();
        match &items[0] {
            SelectItem::RowExpr { .. } => {}
            other => panic!("expected an expression, got {:?}", other),
        }
        // The alias branch for quoted literals must still work.
        let (expr, alias) = split_alias("'Zapier Integration' AS vendor");
        assert_eq!(expr, "'Zapier Integration'");
        assert_eq!(alias.as_deref(), Some("vendor"));
        let (expr, alias) = split_alias("'plain literal'");
        assert_eq!(expr, "'plain literal'");
        assert_eq!(alias, None);
    }

    #[test]
    fn a_clause_on_its_own_line_is_still_found() {
        // Reported from the console: `SELECT … FROM "mytable"\nWHERE id = 1
        // LIMIT 100` returned 100 rows of a 20,006-row table — every row, up to
        // the limit. The clause split searched for the literal `" WHERE "`, so
        // a newline before the keyword matched nothing and the predicate was
        // dropped. A filtered query answering with the whole table is the worst
        // shape of wrong there is: it looks like success.
        let schema = make_schema();
        let purpose = QueryPurpose::QueryRows(crate::core::rql::lexer_s1::QueryRowsData {
            table_name: "users".to_string(),
            query: "SELECT \"id\", \"name\"\nFROM \"users\"\nWHERE id = 1\nLIMIT 100".to_string(),
        });
        let parsed = row_fetch_from_select_query(&purpose, &schema).unwrap();
        assert!(
            !parsed.where_clause.trim().is_empty(),
            "the predicate must survive a line break, got {:?}",
            parsed.where_clause
        );
        assert_eq!(parsed.limit, Some(100));
    }

    #[test]
    fn every_clause_keyword_survives_a_line_break() {
        let schema = make_schema();
        let purpose = QueryPurpose::QueryRows(crate::core::rql::lexer_s1::QueryRowsData {
            table_name: "users".to_string(),
            query: "SELECT age, COUNT(*)\nFROM users\nWHERE id > 1\nGROUP BY age\n\
                    ORDER BY age DESC\nLIMIT 5\nOFFSET 2"
                .to_string(),
        });
        let parsed = row_fetch_from_select_query(&purpose, &schema).unwrap();
        assert!(
            parsed.where_clause.contains('>'),
            "{:?}",
            parsed.where_clause
        );
        // A grouped query pages after the fold, so the limit rides on the
        // aggregate plan rather than on the scan.
        let plan = parsed.aggregate.as_ref().expect("GROUP BY must be found");
        assert_eq!(plan.limit, Some(5));
        assert_eq!(plan.offset, Some(2));
        assert!(!plan.group_by.is_empty(), "GROUP BY names a column");
        assert!(!plan.order_by.is_empty(), "ORDER BY must be found");
    }

    #[test]
    fn a_whole_row_size_becomes_the_records_width() {
        let schema = make_schema();
        // id(8) + name(32) + age(1) + email(64)
        let width: u64 = schema.iter().map(|f| f.size as u64).sum();

        // Summed over rows, it is that width times the row count.
        assert_eq!(
            rewrite_row_size("SUM(pg_column_size(t))", &schema),
            format!("COUNT(*) * {}", width)
        );
        assert_eq!(
            rewrite_row_size("COALESCE(SUM(pg_column_size(utr)), 0)", &schema),
            format!("COALESCE(COUNT(*) * {}, 0)", width)
        );
        // Unaggregated, it is the width itself.
        assert_eq!(
            rewrite_row_size("pg_column_size(t)", &schema),
            width.to_string()
        );
    }

    #[test]
    fn the_size_of_a_named_column_is_left_to_the_evaluator() {
        // That form measures one value, which the evaluator can do from the
        // row; rewriting it to the record width would answer a different
        // question.
        let schema = make_schema();
        let item = "pg_column_size(name)";
        assert_eq!(rewrite_row_size(item, &schema), item);
    }

    #[test]
    fn an_unaliased_expression_is_not_split_at_its_first_space() {
        // Reported from a running application as
        // `Column '0)' not found in schema` — a column name nobody wrote.
        // Alias detection took everything after the last `)` as the alias;
        // finding nothing there it fell through to splitting the whole item on
        // whitespace, so `COALESCE(SUM(b), 0)` became the expression
        // `COALESCE(SUM(b),` plus the alias `0)`.
        let (expr, alias) = split_alias("COALESCE(SUM(b), 0)");
        assert_eq!(expr, "COALESCE(SUM(b), 0)");
        assert_eq!(alias, None);

        // A real trailing alias still has to be found, with and without AS.
        let (expr, alias) = split_alias("COUNT(x) total");
        assert_eq!(expr, "COUNT(x)");
        assert_eq!(alias.as_deref(), Some("total"));
        let (expr, alias) = split_alias("COALESCE(SUM(b), 0) AS amount");
        assert_eq!(expr, "COALESCE(SUM(b), 0)");
        assert_eq!(alias.as_deref(), Some("amount"));

        // An operator is not a name, so an unaliased arithmetic item keeps all
        // of itself rather than aliasing the column `a` as `+`.
        let (expr, alias) = split_alias("a + 1");
        assert_eq!(expr, "a + 1");
        assert_eq!(alias, None);

        // A `::` cast belongs to the expression. It used to be read as the
        // alias, which discarded the cast *and* labelled the column `::bigint`
        // — the statement reported success and returned an uncast value.
        let (expr, alias) = split_alias("COALESCE(x, 0)::bigint");
        assert_eq!(expr, "COALESCE(x, 0)::bigint");
        assert_eq!(alias, None);

        let (expr, alias) = split_alias("COALESCE(x, 0)::bigint AS lag");
        assert_eq!(expr, "COALESCE(x, 0)::bigint");
        assert_eq!(alias.as_deref(), Some("lag"));

        let (expr, alias) = split_alias("(a + 1)::bigint total");
        assert_eq!(expr, "(a + 1)::bigint");
        assert_eq!(alias.as_deref(), Some("total"));
    }

    #[test]
    fn a_parenthesised_operand_under_a_cast_is_an_expression() {
        // `(a + 1)::bigint` hides its `+` at depth 1, so the only marker that
        // it is an expression rather than a column name is the cast. Without
        // it the item reached the column resolver and came back as
        // "function '' is not supported yet".
        assert!(looks_like_row_expression("(a + 1)::bigint"));
        assert!(looks_like_row_expression("a::bigint"));
        assert!(!looks_like_row_expression("plain_column"));
    }

    #[test]
    fn an_unaliased_expression_over_an_aggregate_parses() {
        let schema = make_schema();
        let items = parse_select_items("COALESCE(MAX(age), 0)", &schema).unwrap();
        assert!(matches!(items[0], SelectItem::Computed { .. }));
    }

    #[test]
    fn an_expression_over_an_aggregate_becomes_a_computed_item() {
        let schema = make_schema();
        let items = parse_select_items("COALESCE(MAX(age), 0) + 1 AS next", &schema).unwrap();
        assert_eq!(items.len(), 1);
        match &items[0] {
            SelectItem::Computed { inputs, label, .. } => {
                assert_eq!(label, "next");
                assert_eq!(inputs.len(), 1, "one aggregate was extracted");
                assert_eq!(inputs[0].func, AggFunc::Max);
                assert_eq!(inputs[0].arg.as_ref().unwrap().name, "age");
            }
            other => panic!("expected a computed item, got {:?}", other),
        }
    }

    #[test]
    fn a_bare_aggregate_is_not_rerouted_through_the_expression_path() {
        // Routing it there would change the reported type of every existing
        // aggregate query, so the plain path has to keep owning this shape.
        let schema = make_schema();
        let items = parse_select_items("MAX(age)", &schema).unwrap();
        assert!(matches!(items[0], SelectItem::Aggregate { .. }));
    }

    #[test]
    fn a_computed_item_pulls_out_every_aggregate_it_reads() {
        let schema = make_schema();
        let items = parse_select_items("SUM(age) / COUNT(*) AS mean", &schema).unwrap();
        match &items[0] {
            SelectItem::Computed { inputs, .. } => {
                assert_eq!(inputs.len(), 2);
                assert_eq!(inputs[0].func, AggFunc::Sum);
                assert_eq!(inputs[1].func, AggFunc::Count);
                assert!(inputs[1].arg.is_none(), "COUNT(*) reads no column");
            }
            other => panic!("expected a computed item, got {:?}", other),
        }
    }

    #[test]
    fn a_computed_item_takes_the_aggregate_path_and_fetches_its_columns() {
        // The failure this pins: if the scan does not fetch `age`, the fold
        // sees no value and the answer is silently wrong rather than an error.
        let schema = make_schema();
        let purpose = QueryPurpose::QueryRows(crate::core::rql::lexer_s1::QueryRowsData {
            table_name: "users".to_string(),
            query: "SELECT COALESCE(MAX(age), 0) + 1 AS next FROM users".to_string(),
        });
        let parsed = row_fetch_from_select_query(&purpose, &schema).unwrap();
        assert!(parsed.aggregate.is_some(), "must fold rather than project");
        let fetched: Vec<String> = parsed
            .requested_row_fetch
            .columns_fetching_data
            .iter()
            .map(|c| schema[c.schema_id as usize].name.clone())
            .collect();
        assert!(
            fetched.iter().any(|n| n == "age"),
            "the aggregate's column must be scanned, got {:?}",
            fetched
        );
    }

    fn make_schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 40,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "email".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 41,
                write_order: 3,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ]
    }

    #[test]
    fn test_select_all_columns() {
        let schema = make_schema();
        let query = "SELECT * FROM users WHERE age > 18";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data.len(),
            schema.len()
        );
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 1); // Only 'age' for WHERE clause
        assert_eq!(qr.where_clause, "age > 18");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_specific_columns() {
        let schema = make_schema();
        let query = "SELECT id, name FROM users WHERE name = 'John'";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 2);
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data[0].column_type,
            DbType::U64
        );
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 1); // Only 'name' for WHERE clause
        assert_eq!(qr.where_clause, "name = 'John'");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_no_where() {
        let schema = make_schema();
        let query = "SELECT id, email FROM users";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 2);
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 0); // No WHERE clause, so no query columns
        assert_eq!(qr.where_clause, "");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_column_not_found() {
        let schema = make_schema();
        let query = "SELECT id, not_a_column FROM users";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let result = row_fetch_from_select_query(&qp, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_simd_compare_strings_for_column_names() {
        let schema = make_schema();
        let col_name = "name";
        let col_bytes = col_name.as_bytes();
        let found = schema.iter().any(|f| {
            simd_compare_strings(f.name.as_bytes(), col_bytes, &ComparerOperation::Equals)
        });
        assert!(found);
        let not_found = schema.iter().any(|f| {
            simd_compare_strings(
                f.name.as_bytes(),
                b"not_a_column",
                &ComparerOperation::Equals,
            )
        });
        assert!(!not_found);
    }

    #[test]
    fn test_simd_compare_strings_for_where_clause() {
        // Simulate a WHERE clause comparison using SIMD
        let clause = "name = 'John'";
        let target = "name = 'John'";
        let result = simd_compare_strings(
            clause.as_bytes(),
            target.as_bytes(),
            &ComparerOperation::Equals,
        );
        assert!(result);
        let not_result =
            simd_compare_strings(clause.as_bytes(), b"age > 18", &ComparerOperation::Equals);
        assert!(!not_result);
    }

    #[test]
    fn test_query_vs_requested_columns() {
        let schema = make_schema();
        let query = "SELECT id, name, email FROM users WHERE age > 18 AND name CONTAINS 'John'";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        // Requested columns should be id, name, email (3 columns)
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 3);

        // Query columns should be age and name (2 columns from WHERE clause)
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 2);

        assert_eq!(qr.where_clause, "age > 18 AND name CONTAINS 'John'");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_complex_where_clause_column_extraction() {
        let schema = make_schema();
        let query = "SELECT name FROM users WHERE id = 999 AND age > 21";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        // Requested columns should be just name (1 column)
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 1);

        // Query columns should be id and age (2 columns from WHERE clause)
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 2);

        assert_eq!(qr.where_clause, "id = 999 AND age > 21");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_with_order_by_and_limit() {
        let schema = make_schema();
        let query = "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.where_clause, "age > 18");
        assert_eq!(qr.order_by.as_deref(), Some("name"));
        assert_eq!(qr.limit, Some(10));
    }

    #[test]
    fn test_select_with_limit_only() {
        let schema = make_schema();
        let query = "SELECT id FROM users LIMIT 5";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.where_clause, "");
        assert_eq!(qr.order_by, None);
        assert_eq!(qr.limit, Some(5));
    }

    #[test]
    fn test_select_with_order_by_unknown_column_should_fail() {
        let schema = make_schema();
        let query = "SELECT id FROM users ORDER BY not_a_column";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let result = row_fetch_from_select_query(&qp, &schema);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod distinct_on_detection {
    use super::starts_with_distinct_on;

    /// The operator, in the spacings Postgres accepts.
    #[test]
    fn the_operator_is_recognised() {
        assert!(starts_with_distinct_on("ON (id, v) id"));
        assert!(starts_with_distinct_on("on(id) id"));
        assert!(starts_with_distinct_on("ON   (id) id"));
    }

    /// A column whose name merely begins with those two letters is not it.
    ///
    /// This is the whole reason the check compares a full word rather than a
    /// prefix: `SELECT DISTINCT online, id` is a supported query, and reading
    /// it as the operator would refuse a statement that works.
    #[test]
    fn a_column_that_starts_with_on_is_not_the_operator() {
        assert!(!starts_with_distinct_on("online, id"));
        assert!(!starts_with_distinct_on("on_call"));
        assert!(!starts_with_distinct_on("only(x)"));
        // `ON` alone, with no key list, is a column named `on` — quoted or not,
        // it is not the operator, and refusing it would be wrong.
        assert!(!starts_with_distinct_on("on, id"));
        assert!(!starts_with_distinct_on("on"));
    }
}
