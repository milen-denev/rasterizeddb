//! Running a `MERGE` (`core::rql::merge` parses it).
//!
//! In one transaction (its own outside a block), in two phases, so the
//! matching sees the target as it was before any action:
//!
//! 1. every source row is read, and for each the target rows its `ON`
//!    condition matches; with `WHEN NOT MATCHED BY SOURCE` clauses, every
//!    target row no source row matched. A target row matched by two source
//!    rows is 21000, as PostgreSQL's "cannot affect row a second time".
//! 2. each pair, unmatched source row and unmatched target row takes the
//!    first clause of its kind whose `AND` condition holds, and its action
//!    is written as a statement of its own: by the target's primary key for
//!    an `UPDATE` or `DELETE`, so the target's triggers fire as for any
//!    write. `RETURNING` reports each written row, `merge_action()` as its
//!    action.

use super::trigger_exec::{cast_of, cast_of_oid, failure, field, key_columns, key_match, literal, query, query_in, quote_ident, visible_fields, Failure, Values};
use super::*;
use crate::core::rql::merge::{Action, Merge, WhenKind};

/// What an expression of the `MERGE` can name: the source row and the
/// target row, each by alias, and `merge_action()`.
struct Scope<'a> {
    merge: &'a Merge,
    source_names: &'a [String],
    source_types: &'a [u32],
    /// The source row, or `None` where there is none (a `NOT MATCHED BY
    /// SOURCE` row); `Some(None)`-like absence is a NULL row.
    source: Option<&'a [Option<String>]>,
    fields: &'a [crate::core::row::schema::SchemaField],
    /// The target row; `None` leaves target references as written (the `ON`
    /// condition, read against the target itself).
    target: Option<&'a Values>,
    /// Whether target references read NULL (an unmatched source row).
    target_null: bool,
    action: &'a str,
    /// Source references left as written (a description, read against the
    /// source itself).
    keep_source: bool,
}

impl Scope<'_> {
    fn source_value(&self, column: &str) -> Option<String> {
        if self.keep_source {
            return Some(format!("{}.{}", self.merge.source_alias, quote_ident(column)));
        }
        let at = self.source_names.iter().position(|n| n.eq_ignore_ascii_case(column))?;
        let cast = self.source_types.get(at).copied().and_then(cast_of_oid);
        let value = self.source.and_then(|row| row.get(at).cloned()).flatten();
        Some(literal(&value, cast))
    }

    fn target_value(&self, column: &str) -> Option<Option<String>> {
        let f = field(self.fields, column)?;
        if self.target_null {
            return Some(Some(literal(&None, cast_of(f))));
        }
        // No row: the reference stays as written (the `ON` condition).
        let Some(row) = self.target else {
            return Some(None);
        };
        let value = row.iter().find(|(n, _)| n.eq_ignore_ascii_case(column)).and_then(|(_, v)| v.clone());
        Some(Some(literal(&value, cast_of(f))))
    }

    fn is_target(&self, qualifier: &str) -> bool {
        qualifier == self.merge.target_alias || qualifier == self.merge.target
    }
}

/// `text` with source and target references written as literals (see
/// [`Scope`]), `merge_action()` as its action, and `s.*`/`t.*` as every
/// column.
fn substitute(text: &str, scope: &Scope<'_>) -> Result<String, Failure> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 32);
    let mut i = 0;
    let read_word = |at: usize| -> (String, usize, bool) {
        if bytes[at] == b'"' {
            let end = crate::core::sql_scan::quoted_end(bytes, at, b'"');
            (text[at + 1..end.saturating_sub(1).max(at + 1)].replace("\"\"", "\""), end, true)
        } else {
            let mut j = at;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            (text[at..j].to_ascii_lowercase(), j, false)
        }
    };
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            let end = crate::core::sql_scan::string_literal_end(bytes, i, false);
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
            let (word, end, _) = read_word(i);
            let after_dot = i > 0 && bytes[i - 1] == b'.';
            if after_dot {
                out.push_str(&text[i..end]);
                i = end;
                continue;
            }
            // `alias.col` or `alias.*`.
            if bytes.get(end) == Some(&b'.') && end + 1 < bytes.len() {
                let qualifier = word.clone();
                if bytes[end + 1] == b'*' {
                    let expanded = if qualifier == scope.merge.source_alias {
                        Some(scope.source_names.iter().map(|n| format!("{} AS {}", scope.source_value(n).unwrap_or_default(), quote_ident(n))).collect::<Vec<_>>().join(", "))
                    } else if scope.is_target(&qualifier) && (scope.target.is_some() || scope.target_null) {
                        Some(scope.fields.iter().map(|f| format!("{} AS {}", scope.target_value(&f.name).flatten().unwrap_or_default(), quote_ident(&f.name))).collect::<Vec<_>>().join(", "))
                    } else {
                        None
                    };
                    if let Some(expanded) = expanded {
                        out.push_str(&expanded);
                        i = end + 2;
                        continue;
                    }
                }
                if bytes[end + 1] == b'"' || bytes[end + 1].is_ascii_alphabetic() || bytes[end + 1] == b'_' {
                    let (column, column_end, _) = read_word(end + 1);
                    if qualifier == scope.merge.source_alias {
                        match scope.source_value(&column) {
                            Some(v) => out.push_str(&v),
                            None => return Err(("42703".into(), format!("column {qualifier}.{column} does not exist"))),
                        }
                        i = column_end;
                        continue;
                    }
                    if scope.is_target(&qualifier) {
                        match scope.target_value(&column) {
                            Some(Some(v)) => out.push_str(&v),
                            Some(None) => out.push_str(&text[i..column_end]),
                            None => return Err(("42703".into(), format!("column {qualifier}.{column} does not exist"))),
                        }
                        i = column_end;
                        continue;
                    }
                }
                out.push_str(&text[i..end]);
                i = end;
                continue;
            }
            if word == "merge_action" && text[end..].trim_start().starts_with('(') {
                let close = crate::core::sql_scan::matching_close(text, end + text[end..].find('(').unwrap_or(0)).unwrap_or(end);
                out.push_str(&format!("CAST('{}' AS text)", scope.action));
                i = close + 1;
                continue;
            }
            // A bare column: the source's or the target's, never both.
            if text[end..].trim_start().starts_with('(') {
                out.push_str(&text[i..end]);
                i = end;
                continue;
            }
            let in_source = scope.source_names.iter().any(|n| n.eq_ignore_ascii_case(&word));
            let in_target = field(scope.fields, &word).is_some();
            if in_source && in_target {
                return Err(("42702".into(), format!("column reference \"{word}\" is ambiguous")));
            }
            if in_source {
                out.push_str(&scope.source_value(&word).unwrap_or_default());
            } else if in_target {
                match scope.target_value(&word) {
                    Some(Some(v)) => out.push_str(&v),
                    _ => out.push_str(&text[i..end]),
                }
            } else {
                out.push_str(&text[i..end]);
            }
            i = end;
            continue;
        }
        let ch = text[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

async fn holds(database: &Arc<Database>, state: &mut SessionState, condition: &str) -> io::Result<Result<bool, Failure>> {
    Ok(match query(database, state, &format!("SELECT CASE WHEN {condition} THEN TRUE ELSE FALSE END")).await? {
        Ok(rows) => Ok(rows.rows.first().and_then(|r| r.first()).cloned().flatten().as_deref() == Some("t")),
        Err(e) => Err(failure(e)),
    })
}

/// Runs `merge` and answers it.
pub(super) async fn run_merge<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    merge: Merge,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let read_only = state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only);
    if read_only {
        return fail_statement(state, stream, "25006", &read_only_message("MERGE")).await;
    }
    let Some(view) = database.schema_view(&merge.target) else {
        return fail_statement(state, stream, "42P01", &format!("relation \"{}\" does not exist", merge.target)).await;
    };
    let schema = view.schema().clone();
    drop(view);
    if key_columns(&schema).is_none() && merge.clauses.iter().any(|c| matches!(c.action, Action::Update(_) | Action::Delete)) {
        return fail_statement(
            state,
            stream,
            "0A000",
            &format!("MERGE needs a primary key on \"{}\" to update or delete its rows here", merge.target),
        )
        .await;
    }
    let own_block = state.transaction.is_none();
    if own_block {
        if let Err(e) = super::view_statements::run_internal(database, state, "BEGIN").await? {
            return send_error_response(stream, sqlstate_for(&e), &e).await;
        }
    }
    let mut out = Vec::<u8>::new();
    let result = Box::pin(merge_rows(database, state, &mut out, &merge, &schema, result_formats, omit_description)).await?;
    if own_block {
        let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        let ended = super::view_statements::run_internal(database, state, end).await?;
        if result.is_ok() && let Err(e) = ended {
            return send_error_response(stream, sqlstate_for(&e), &e).await;
        }
    }
    match result {
        Ok(count) => {
            stream.write_all(&out).await?;
            send_command_complete(stream, &format!("MERGE {count}")).await
        }
        Err((code, message)) => {
            if state.transaction.is_some() {
                fail_statement(state, stream, &code, &message).await
            } else {
                send_error_response(stream, &code, &message).await
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn merge_rows(
    database: &Arc<Database>,
    state: &mut SessionState,
    out: &mut Vec<u8>,
    merge: &Merge,
    schema: &crate::core::row::schema::TableSchema,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<Result<u64, Failure>> {
    let fields = visible_fields(schema);
    let target = quote_ident(&merge.target);
    // Bare where it can be: a quoted alias keeps its quotes in this engine's
    // qualifier matching, so `"t"` was not the `t` of `t.id`.
    let target_alias = if merge.target_alias.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && merge.target_alias.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        merge.target_alias.clone()
    } else {
        quote_ident(&merge.target_alias)
    };
    let columns_list = fields.iter().map(|f| quote_ident(&f.name)).collect::<Vec<_>>().join(", ");

    // Phase 1: the source, and what each source row matches.
    let source = match query(database, state, &format!("SELECT * FROM {}", merge.source)).await? {
        Ok(rows) => rows,
        Err(e) => return Ok(Err(failure(e))),
    };
    let has_matched = merge.clauses.iter().any(|c| c.kind == WhenKind::Matched);
    let mut pairs: Vec<(usize, Values)> = Vec::new();
    let mut unmatched: Vec<usize> = Vec::new();
    let mut matched_keys: Vec<String> = Vec::new();
    for (at, row) in source.rows.iter().enumerate() {
        let scope = Scope {
            merge,
            source_names: &source.names,
            source_types: &source.types,
            source: Some(row),
            fields: &fields,
            target: None,
            target_null: false,
            action: "",
            keep_source: false,
        };
        let on = match substitute(&merge.on, &scope) {
            Ok(on) => on,
            Err(e) => return Ok(Err(e)),
        };
        let found = match query(database, state, &format!("SELECT {columns_list} FROM {target} AS {target_alias} WHERE {on}")).await? {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(failure(e))),
        };
        if found.rows.is_empty() {
            unmatched.push(at);
            continue;
        }
        for target_row in found.rows {
            let values: Values = fields.iter().map(|f| f.name.clone()).zip(target_row).collect();
            let key = key_match(schema, &fields, &values);
            if has_matched && matched_keys.contains(&key) {
                return Ok(Err((
                    "21000".into(),
                    "MERGE command cannot affect row a second time: more than one source row matches one target row".into(),
                )));
            }
            matched_keys.push(key);
            pairs.push((at, values));
        }
    }
    let mut by_source: Vec<Values> = Vec::new();
    if merge.clauses.iter().any(|c| c.kind == WhenKind::NotMatchedBySource) {
        let all = match query(database, state, &format!("SELECT {columns_list} FROM {target}")).await? {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(failure(e))),
        };
        for row in all.rows {
            let values: Values = fields.iter().map(|f| f.name.clone()).zip(row).collect();
            if !matched_keys.contains(&key_match(schema, &fields, &values)) {
                by_source.push(values);
            }
        }
    }

    // Phase 2: each row's first clause that holds, as a write of its own.
    let mut work: Vec<(WhenKind, Option<usize>, Option<Values>)> = Vec::new();
    work.extend(pairs.into_iter().map(|(at, values)| (WhenKind::Matched, Some(at), Some(values))));
    work.extend(unmatched.into_iter().map(|at| (WhenKind::NotMatchedByTarget, Some(at), None)));
    work.extend(by_source.into_iter().map(|values| (WhenKind::NotMatchedBySource, None, Some(values))));
    let mut count = 0u64;
    let mut described = false;
    for (kind, source_at, target_row) in work {
        let source_row = source_at.map(|at| source.rows[at].as_slice());
        let mut chosen = None;
        for clause in merge.clauses.iter().filter(|c| c.kind == kind) {
            let scope = Scope {
                merge,
                source_names: &source.names,
                source_types: &source.types,
                source: source_row,
                fields: &fields,
                target: target_row.as_ref(),
                target_null: target_row.is_none(),
                action: clause.action.word(),
                keep_source: false,
            };
            let applies = match &clause.condition {
                None => true,
                Some(condition) => {
                    let text = match substitute(condition, &scope) {
                        Ok(text) => text,
                        Err(e) => return Ok(Err(e)),
                    };
                    match holds(database, state, &text).await? {
                        Ok(holds) => holds,
                        Err(e) => return Ok(Err(e)),
                    }
                }
            };
            if applies {
                chosen = Some(clause);
                break;
            }
        }
        let Some(clause) = chosen else { continue };
        let scope = Scope {
            merge,
            source_names: &source.names,
            source_types: &source.types,
            source: source_row,
            fields: &fields,
            target: target_row.as_ref(),
            target_null: target_row.is_none(),
            action: clause.action.word(),
            keep_source: false,
        };
        let write = match &clause.action {
            Action::Nothing => continue,
            Action::Delete => {
                let Some(row) = &target_row else { continue };
                format!("DELETE FROM {target} WHERE {} RETURNING *", key_match(schema, &fields, row))
            }
            Action::Update(set) => {
                let Some(row) = &target_row else { continue };
                let mut items = Vec::new();
                for (column, expr) in set {
                    if field(&fields, column).is_none() {
                        return Ok(Err(("42703".into(), format!("column \"{column}\" of relation \"{}\" does not exist", merge.target))));
                    }
                    match substitute(expr, &scope) {
                        Ok(value) => items.push(format!("{} = {value}", quote_ident(column))),
                        Err(e) => return Ok(Err(e)),
                    }
                }
                format!("UPDATE {target} SET {} WHERE {} RETURNING *", items.join(", "), key_match(schema, &fields, row))
            }
            Action::Insert { columns, values } => match values {
                None => format!("INSERT INTO {target} DEFAULT VALUES RETURNING *"),
                Some(values) => {
                    let columns: Vec<String> = columns.clone().unwrap_or_else(|| fields.iter().map(|f| f.name.clone()).collect());
                    let mut items = Vec::new();
                    for value in values {
                        match substitute(value, &scope) {
                            Ok(v) => items.push(v),
                            Err(e) => return Ok(Err(e)),
                        }
                    }
                    format!(
                        "INSERT INTO {target} ({}) VALUES ({}) RETURNING *",
                        columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", "),
                        items.join(", ")
                    )
                }
            },
        };
        let stored = match query(database, state, &write).await? {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(failure(e))),
        };
        for row in stored.rows {
            count += 1;
            let Some(list) = &merge.returning else { continue };
            let after: Values = fields.iter().map(|f| f.name.clone()).zip(row).collect();
            let returned = Scope {
                merge,
                source_names: &source.names,
                source_types: &source.types,
                source: source_row,
                fields: &fields,
                target: Some(&after),
                target_null: false,
                action: clause.action.word(),
                keep_source: false,
            };
            let projected = match substitute(list, &returned) {
                Ok(p) => p,
                Err(e) => return Ok(Err(e)),
            };
            let rows = match query_in(database, state, &format!("SELECT {projected}"), result_formats).await? {
                Ok(rows) => rows,
                Err(e) => return Ok(Err(failure(e))),
            };
            if !described && !omit_description {
                if let Some(description) = &rows.description {
                    out.extend_from_slice(description);
                }
            }
            described = true;
            for message in &rows.data {
                out.extend_from_slice(message);
            }
        }
    }
    Ok(Ok(count))
}

/// What a `MERGE … RETURNING` is described as: its list as a query over the
/// source, each target column a NULL of its type and `merge_action()` text.
/// `None` without `RETURNING` (no rows) or when it cannot be built.
pub(super) fn describe_text(database: &Database, merge: &Merge) -> Option<String> {
    let list = merge.returning.as_ref()?;
    let view = database.schema_view(&merge.target)?;
    let fields = visible_fields(view.schema());
    drop(view);
    let scope = Scope {
        merge,
        source_names: &[],
        source_types: &[],
        source: None,
        fields: &fields,
        target: None,
        target_null: true,
        action: "UPDATE",
        keep_source: true,
    };
    let projected = substitute(list, &scope).ok()?;
    Some(format!("SELECT {projected} FROM {} LIMIT 0", merge.source))
}
