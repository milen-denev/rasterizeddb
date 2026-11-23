//! `CREATE TRIGGER`/`DROP TRIGGER`, and firing them (`core::rql::triggers`
//! keeps the rows and reads the PL/pgSQL).
//!
//! A write to a table with triggers runs here a row at a time, inside a
//! transaction (its own when the session is not in a block, so a trigger
//! that raises takes the whole statement back):
//!
//! 1. the rows it affects are read, with their `OLD` values and, for an
//!    `UPDATE`, the values its `SET` computes;
//! 2. `BEFORE … FOR EACH ROW` triggers run in name order, each over the
//!    `NEW` the previous one returned; `RETURN NULL` skips the row;
//! 3. the row is written as a statement of its own (by primary key for an
//!    `UPDATE` or `DELETE`), with the table's own triggers held off, and
//!    `RETURNING *` gives what was stored;
//! 4. `AFTER … FOR EACH ROW` triggers run over what was stored.
//!
//! Statement triggers run once, before and after. An `AFTER` row trigger
//! runs right after its row, not at the end of the statement as
//! PostgreSQL's queue does; the order differs only for a trigger that reads
//! the table it was fired by.

use super::*;
use crate::core::rql::triggers::{self, Event, PlBlock, PlStmt, Timing, TriggerDef, TriggerStatement};

pub(super) type Values = Vec<(String, Option<String>)>;

/// A statement's rows as text: column names, and each row's values.
pub(super) struct Rows {
    pub(super) names: Vec<String>,
    /// Each column's type OID, as the description gave it.
    pub(super) types: Vec<u32>,
    pub(super) rows: Vec<Vec<Option<String>>>,
    /// The `RowDescription` and `DataRow` messages as they came, for passing
    /// a `RETURNING` on.
    pub(super) description: Option<Vec<u8>>,
    pub(super) data: Vec<Vec<u8>>,
}

/// Every message of a run's output: its type and its whole bytes.
fn messages(bytes: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 5 <= bytes.len() {
        let len = u32::from_be_bytes([bytes[at + 1], bytes[at + 2], bytes[at + 3], bytes[at + 4]]) as usize;
        let end = (at + 1 + len).min(bytes.len());
        out.push((bytes[at], &bytes[at..end]));
        at = end.max(at + 5);
    }
    out
}

fn decode(output: &[u8]) -> Rows {
    let mut rows = Rows { names: Vec::new(), types: Vec::new(), rows: Vec::new(), description: None, data: Vec::new() };
    for (kind, message) in messages(output) {
        match kind {
            b'T' if rows.description.is_none() => {
                rows.description = Some(message.to_vec());
                let count = u16::from_be_bytes([message[5], message[6]]) as usize;
                let mut i = 7;
                for _ in 0..count {
                    let Some(nul) = message[i..].iter().position(|b| *b == 0) else { break };
                    rows.names.push(String::from_utf8_lossy(&message[i..i + nul]).into_owned());
                    let t = i + nul + 1 + 6;
                    rows.types.push(message.get(t..t + 4).map_or(0, |b| u32::from_be_bytes([b[0], b[1], b[2], b[3]])));
                    i += nul + 1 + 18;
                }
            }
            b'D' => {
                rows.data.push(message.to_vec());
                let count = u16::from_be_bytes([message[5], message[6]]) as usize;
                let mut i = 7;
                let mut row = Vec::with_capacity(count);
                for _ in 0..count {
                    let len = i32::from_be_bytes([message[i], message[i + 1], message[i + 2], message[i + 3]]);
                    i += 4;
                    if len < 0 {
                        row.push(None);
                    } else {
                        let len = len as usize;
                        row.push(Some(String::from_utf8_lossy(&message[i..i + len]).into_owned()));
                        i += len;
                    }
                }
                rows.rows.push(row);
            }
            _ => {}
        }
    }
    rows
}

/// Runs `sql` through the session's path and reads its rows (text form).
pub(super) async fn query(database: &Arc<Database>, state: &mut SessionState, sql: &str) -> io::Result<Result<Rows, String>> {
    query_in(database, state, sql, &[]).await
}

/// [`query`], its rows in `result_formats` (a `RETURNING` passed on to an
/// extended-protocol client is in the formats it bound).
pub(super) async fn query_in(
    database: &Arc<Database>,
    state: &mut SessionState,
    sql: &str,
    result_formats: &[i16],
) -> io::Result<Result<Rows, String>> {
    let mut sink = std::io::Cursor::new(Vec::<u8>::new());
    let was_internal = std::mem::replace(&mut state.view_internal, true);
    // Its rows to this buffer, never a native session's answer.
    let was_capturing = std::mem::replace(&mut state.native_capture, false);
    let kept_result = state.native_result.take();
    let ran =
        internal_run(Box::pin(execute_single_statement(database, state, &mut sink, sql, result_formats, false))).await;
    state.native_capture = was_capturing;
    state.native_result = kept_result;
    state.view_internal = was_internal;
    ran?;
    if state.statement_failed {
        return Ok(Err(state.statement_error.clone().unwrap_or_else(|| "statement failed".to_string())));
    }
    Ok(Ok(decode(sink.get_ref())))
}

/// An error raised while firing, with its SQLSTATE.
pub(super) type Failure = (String, String);

pub(super) fn failure(message: String) -> Failure {
    (sqlstate_for(&message).to_string(), message)
}

/// The cast a value of `field` is written with, so an expression reading it
/// computes on its type.
pub(super) fn cast_of(field: &crate::core::row::schema::SchemaField) -> Option<&'static str> {
    use crate::core::db_type::DbType;
    Some(match field.db_type {
        DbType::I8 | DbType::I16 => "smallint",
        DbType::I32 => "integer",
        DbType::I64 => "bigint",
        DbType::F32 => "real",
        DbType::F64 => "double precision",
        DbType::DECIMAL => "numeric",
        DbType::BOOL => "boolean",
        DbType::STRING if field.json_text => "json",
        DbType::STRING => "text",
        DbType::DATE => "date",
        DbType::DATETIME if field.without_time_zone => "timestamp",
        DbType::DATETIME => "timestamptz",
        DbType::TIME => "time",
        DbType::UUID => "uuid",
        DbType::JSONB => "jsonb",
        DbType::BYTEA => "bytea",
        DbType::INTERVAL => "interval",
        _ => return None,
    })
}

/// The cast a value of type `oid` is written with, from a `RowDescription`.
pub(super) fn cast_of_oid(oid: u32) -> Option<&'static str> {
    Some(match oid {
        16 => "boolean",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        700 => "real",
        701 => "double precision",
        1700 => "numeric",
        25 | 1043 | 1042 | 19 => "text",
        1082 => "date",
        1114 => "timestamp",
        1184 => "timestamptz",
        1083 => "time",
        2950 => "uuid",
        3802 => "jsonb",
        114 => "json",
        17 => "bytea",
        1186 => "interval",
        _ => return None,
    })
}

/// `value` as a literal of `cast`'s type.
pub(super) fn literal(value: &Option<String>, cast: Option<&str>) -> String {
    match value {
        None => match cast {
            Some(cast) => format!("CAST(NULL AS {cast})"),
            None => "NULL".to_string(),
        },
        Some(v) => {
            let quoted = format!("'{}'", v.replace('\'', "''"));
            match cast {
                Some("boolean") => if v == "t" || v.eq_ignore_ascii_case("true") { "TRUE".into() } else { "FALSE".into() },
                Some(cast) => format!("CAST({quoted} AS {cast})"),
                None => quoted,
            }
        }
    }
}

/// What a trigger function sees, besides its variables.
struct Context<'a> {
    trigger: &'a TriggerDef,
    event: Event,
    row_level: bool,
    timing: Timing,
    fields: &'a [crate::core::row::schema::SchemaField],
}

pub(super) fn field<'a>(fields: &'a [crate::core::row::schema::SchemaField], name: &str) -> Option<&'a crate::core::row::schema::SchemaField> {
    fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
}

/// `text` with `NEW.col`, `OLD.col`, the `TG_` variables and the function's
/// own variables written as literals, outside quotes.
fn substitute(
    text: &str,
    ctx: &Context<'_>,
    new: Option<&Values>,
    old: Option<&Values>,
    vars: &[(String, String, Option<String>)],
) -> Result<String, Failure> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 32);
    let mut i = 0;
    let value_of = |row: Option<&Values>, which: &str, column: &str| -> Result<String, Failure> {
        // `OLD` in an INSERT trigger and `NEW` in a DELETE one are NULL
        // records (PostgreSQL 11 on): a field of one reads NULL.
        let Some(row) = row else {
            return Ok(literal(&None, field(ctx.fields, column).and_then(cast_of)));
        };
        let _ = which;
        let (_, value) = row
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .ok_or_else(|| ("42703".to_string(), format!("record \"{which}\" has no field \"{column}\"")))?;
        Ok(literal(value, field(ctx.fields, column).and_then(cast_of)))
    };
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            let end = crate::core::sql_scan::string_literal_end(bytes, i, false);
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        if b == b'"' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let mut j = i;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            let word = text[i..j].to_ascii_lowercase();
            let after_dot = i > 0 && bytes[i - 1] == b'.';
            if !after_dot && (word == "new" || word == "old") && bytes.get(j) == Some(&b'.') {
                // `NEW.col` or `NEW."Col"`.
                let mut k = j + 1;
                let column = if bytes.get(k) == Some(&b'"') {
                    let end = crate::core::sql_scan::quoted_end(bytes, k, b'"');
                    let name = text[k + 1..end - 1].replace("\"\"", "\"");
                    k = end;
                    name
                } else {
                    let start = k;
                    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                        k += 1;
                    }
                    text[start..k].to_ascii_lowercase()
                };
                let row = if word == "new" { new } else { old };
                out.push_str(&value_of(row, &word.to_uppercase(), &column)?);
                i = k;
                continue;
            }
            if !after_dot && bytes.get(j) != Some(&b'(') {
                let tg = match word.as_str() {
                    "tg_op" => Some(format!("'{}'", ctx.event.word())),
                    "tg_name" => Some(format!("'{}'", ctx.trigger.name.replace('\'', "''"))),
                    "tg_when" => Some(if ctx.timing == Timing::Before { "'BEFORE'".into() } else { "'AFTER'".into() }),
                    "tg_level" => Some(if ctx.row_level { "'ROW'".into() } else { "'STATEMENT'".into() }),
                    "tg_table_name" | "tg_relname" => Some(format!("'{}'", ctx.trigger.table.replace('\'', "''"))),
                    "tg_table_schema" => Some("'public'".into()),
                    "tg_nargs" => Some(ctx.trigger.args.len().to_string()),
                    _ => None,
                };
                if let Some(tg) = tg {
                    out.push_str(&tg);
                    i = j;
                    continue;
                }
                if word == "tg_argv" && bytes.get(j) == Some(&b'[') {
                    let close = text[j..].find(']').map(|c| c + j).unwrap_or(j);
                    let n: usize = text[j + 1..close].trim().parse().unwrap_or(usize::MAX);
                    out.push_str(&literal(&ctx.trigger.args.get(n).cloned(), Some("text")));
                    i = close + 1;
                    continue;
                }
                if let Some((_, ty, value)) = vars.iter().find(|(name, _, _)| *name == word) {
                    if bytes.get(j) != Some(&b'.') {
                        if ty == RECORD {
                            return Err(("0A000".into(), format!("record \"{word}\" as a whole value is not supported yet")));
                        }
                        out.push_str(&variable_literal(ty, value));
                        i = j;
                        continue;
                    }
                    // `rec.field`, of the row the record holds.
                    if ty == RECORD {
                        let mut k = j + 1;
                        let column = if bytes.get(k) == Some(&b'"') {
                            let end = crate::core::sql_scan::quoted_end(bytes, k, b'"');
                            let name = text[k + 1..end - 1].replace("\"\"", "\"");
                            k = end;
                            name
                        } else {
                            let start = k;
                            while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                                k += 1;
                            }
                            text[start..k].to_ascii_lowercase()
                        };
                        let prefix = format!("{word}.");
                        let field = format!("{prefix}{column}");
                        match vars.iter().find(|(name, _, _)| *name == field) {
                            Some((_, ty, value)) => out.push_str(&variable_literal(ty, value)),
                            None if vars.iter().any(|(name, _, _)| name.starts_with(&prefix)) => {
                                return Err(("42703".into(), format!("record \"{word}\" has no field \"{column}\"")));
                            }
                            None => return Err(("55000".into(), format!("record \"{word}\" is not assigned yet"))),
                        }
                        i = k;
                        continue;
                    }
                }
            }
            out.push_str(&text[i..j]);
            i = j;
            continue;
        }
        let ch = text[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

/// What running a trigger function ended with.
enum Returned {
    /// `RETURN NEW` or `RETURN OLD` (a row), or `RETURN NULL` (`None`).
    Row(Option<Values>),
    /// The end of the body, or a bare `RETURN`.
    Nothing,
    /// `EXIT` (or `CONTINUE`, `exit` false) on its way to the loop it leaves.
    Leave { exit: bool },
}

/// The type a `record` variable is declared with. Its fields are variables
/// of their own, named `record.field`, set when a row is assigned to it.
const RECORD: &str = "record";

/// A variable's value as a literal of its type. An object-identifier type
/// (`regclass`, …) is read as the name it prints: `format('%s', part)` is
/// the relation's name, as in PostgreSQL, where the oid it would otherwise
/// become is not.
fn variable_literal(ty: &str, value: &Option<String>) -> String {
    match ty {
        "" => literal(value, None),
        // A text value is its quoted string, with no cast around it: `(v
        // LIKE '%x%')::text` did not read with one ("trailing text after
        // CAST(…)").
        "text" | "varchar" | "character varying" if value.is_some() => literal(value, None),
        "regclass" | "regtype" | "regproc" | "regprocedure" | "regnamespace" | "regrole" | "name" | "oid" => {
            literal(value, Some("text"))
        }
        ty => literal(value, Some(ty)),
    }
}

/// Puts one row into `targets`: a single `record` takes every column as its
/// fields, anything else takes the columns by position (NULL past the last).
fn assign_row(
    database: &Database,
    vars: &mut Vec<(String, String, Option<String>)>,
    targets: &[String],
    names: &[String],
    types: &[u32],
    row: Option<Vec<Option<String>>>,
) -> Result<(), Failure> {
    let declared = |vars: &[(String, String, Option<String>)], name: &str| vars.iter().position(|(n, _, _)| n == name);
    if let [target] = targets
        && declared(vars, target).is_some_and(|at| vars[at].1 == RECORD)
    {
        let prefix = format!("{target}.");
        vars.retain(|(name, _, _)| !name.starts_with(&prefix));
        let mut row = row.unwrap_or_else(|| vec![None; names.len()]);
        row.resize(names.len(), None);
        for ((name, oid), value) in names.iter().zip(types.iter().chain(std::iter::repeat(&0))).zip(row) {
            let cast = cast_of_oid(*oid).unwrap_or("").to_string();
            vars.push((format!("{prefix}{}", name.to_ascii_lowercase()), cast, value));
        }
        return Ok(());
    }
    let mut values = row.unwrap_or_default().into_iter();
    for target in targets {
        let Some(at) = declared(vars, target) else {
            return Err(("42601".into(), format!("\"{target}\" is not a known variable")));
        };
        if vars[at].1 == RECORD {
            return Err(("42601".into(), format!("record variable \"{target}\" cannot be part of a multiple-item INTO list")));
        }
        let value = values.next().flatten();
        // A `regclass` variable given an oid holds the relation it names,
        // which is what it prints as.
        vars[at].2 = match value.as_deref().map(str::parse::<i64>) {
            Some(Ok(oid)) if vars[at].1 == "regclass" => {
                let mut named: Option<String> = None;
                database.each_schema(|name, _| {
                    if named.is_none() && crate::core::rql::catalog_model::oid_of(name) == oid {
                        named = Some(name.to_string());
                    }
                });
                named.or(value)
            }
            _ => value,
        };
    }
    Ok(())
}

/// Whether an exception handler's `condition` takes an error of `code`:
/// `others` (all but a cancel or a failed assertion, as in PostgreSQL), an
/// SQLSTATE written out, or a condition's name.
fn condition_takes(condition: &str, code: &str) -> bool {
    if condition == "others" {
        return !matches!(code, "57014" | "P0004");
    }
    if condition.len() == 5 && condition.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
        return condition == code;
    }
    const NAMES: &[(&str, &str)] = &[
        ("feature_not_supported", "0A000"),
        ("invalid_parameter_value", "22023"),
        ("lock_not_available", "55P03"),
        ("deadlock_detected", "40P01"),
        ("serialization_failure", "40001"),
        ("unique_violation", "23505"),
        ("foreign_key_violation", "23503"),
        ("not_null_violation", "23502"),
        ("check_violation", "23514"),
        ("restrict_violation", "23001"),
        ("exclusion_violation", "23P01"),
        ("undefined_table", "42P01"),
        ("undefined_column", "42703"),
        ("undefined_object", "42704"),
        ("undefined_function", "42883"),
        ("duplicate_table", "42P07"),
        ("duplicate_column", "42701"),
        ("duplicate_object", "42710"),
        ("duplicate_function", "42723"),
        ("duplicate_schema", "42P06"),
        ("invalid_table_definition", "42P16"),
        ("invalid_column_reference", "42P10"),
        ("datatype_mismatch", "42804"),
        ("syntax_error", "42601"),
        ("insufficient_privilege", "42501"),
        ("wrong_object_type", "42809"),
        ("dependent_objects_still_exist", "2BP01"),
        ("object_in_use", "55006"),
        ("object_not_in_prerequisite_state", "55000"),
        ("division_by_zero", "22012"),
        ("numeric_value_out_of_range", "22003"),
        ("invalid_text_representation", "22P02"),
        ("string_data_right_truncation", "22001"),
        ("null_value_not_allowed", "22004"),
        ("invalid_datetime_format", "22007"),
        ("datetime_field_overflow", "22008"),
        ("data_exception", "22000"),
        ("raise_exception", "P0001"),
        ("no_data_found", "P0002"),
        ("too_many_rows", "P0003"),
        ("assert_failure", "P0004"),
        ("query_canceled", "57014"),
        ("read_only_sql_transaction", "25006"),
        ("in_failed_sql_transaction", "25P02"),
        ("internal_error", "XX000"),
        ("integrity_constraint_violation", "23000"),
    ];
    match NAMES.iter().find(|(name, _)| *name == condition) {
        // A class's own condition (`…000`) takes the whole class.
        Some((_, named)) if named.ends_with("000") => code.starts_with(&named[..2]),
        Some((_, named)) => *named == code,
        None => false,
    }
}

/// Whether `expr` is true, for an `IF`, a `WHILE` or a `WHEN`.
async fn is_true(database: &Arc<Database>, state: &mut SessionState, expr: &str) -> io::Result<Result<bool, Failure>> {
    Ok(Box::pin(value_of(database, state, &format!("CASE WHEN {expr} THEN TRUE ELSE FALSE END")))
        .await?
        .map(|v| v.is_some_and(|v| v == "t" || v == "true")))
}

/// One expression's value.
async fn value_of(
    database: &Arc<Database>,
    state: &mut SessionState,
    expr: &str,
) -> io::Result<Result<Option<String>, Failure>> {
    Ok(match query(database, state, &format!("SELECT {expr}")).await? {
        Ok(rows) => Ok(rows.rows.into_iter().next().and_then(|r| r.into_iter().next()).flatten()),
        Err(e) => Err(failure(e)),
    })
}

/// Runs `stmts`, the body of the trigger function or one of its branches.
#[allow(clippy::too_many_arguments)]
async fn run_statements(
    database: &Arc<Database>,
    state: &mut SessionState,
    ctx: &Context<'_>,
    stmts: &[PlStmt],
    new: &mut Option<Values>,
    old: &Option<Values>,
    vars: &mut Vec<(String, String, Option<String>)>,
) -> io::Result<Result<Option<Returned>, Failure>> {
    for stmt in stmts {
        match stmt {
            PlStmt::Null => {}
            PlStmt::Assign { target, expr } => {
                let text = match substitute(expr, ctx, new.as_ref(), old.as_ref(), vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                let value = match Box::pin(value_of(database, state, &text)).await? {
                    Ok(v) => v,
                    Err(e) => return Ok(Err(e)),
                };
                if let Some(column) = target.strip_prefix("new.") {
                    let column = column.trim_matches('"');
                    let Some(row) = new.as_mut() else {
                        return Ok(Err(failure(format!("record \"NEW\" is not assigned in a {} trigger", ctx.event.word()))));
                    };
                    match row.iter_mut().find(|(name, _)| name.eq_ignore_ascii_case(column)) {
                        Some(cell) => cell.1 = value,
                        None => return Ok(Err(("42703".into(), format!("record \"NEW\" has no field \"{column}\"")))),
                    }
                } else if target.starts_with("old.") {
                    return Ok(Err(("0A000".into(), "assigning to OLD is not supported".into())));
                } else {
                    match vars.iter_mut().find(|(name, _, _)| name == target) {
                        Some(var) => var.2 = value,
                        None => return Ok(Err(("42601".into(), format!("\"{target}\" is not a known variable")))),
                    }
                }
            }
            PlStmt::If { branches, otherwise } => {
                let mut taken: Option<&Vec<PlStmt>> = None;
                for (cond, body) in branches {
                    let text = match substitute(cond, ctx, new.as_ref(), old.as_ref(), vars) {
                        Ok(text) => text,
                        Err(e) => return Ok(Err(e)),
                    };
                    match Box::pin(value_of(database, state, &format!("CASE WHEN {text} THEN TRUE ELSE FALSE END"))).await? {
                        Ok(Some(v)) if v == "t" || v == "true" => {
                            taken = Some(body);
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => return Ok(Err(e)),
                    }
                }
                let body = taken.unwrap_or(otherwise);
                match Box::pin(run_statements(database, state, ctx, body, new, old, vars)).await? {
                    Ok(None) => {}
                    other => return Ok(other),
                }
            }
            PlStmt::Return(expr) => {
                let returned = match expr.as_deref().map(|e| e.trim().to_ascii_uppercase()) {
                    None => Returned::Nothing,
                    Some(e) if e == "NEW" => Returned::Row(new.clone()),
                    Some(e) if e == "OLD" => Returned::Row(old.clone()),
                    Some(e) if e == "NULL" => Returned::Row(None),
                    Some(other) => {
                        return Ok(Err(("0A000".into(), format!("a trigger function returns NEW, OLD or NULL, not {other}"))));
                    }
                };
                return Ok(Ok(Some(returned)));
            }
            PlStmt::Raise { level, format, args, errcode } => {
                let mut message = String::new();
                let mut values = Vec::new();
                for arg in args {
                    let text = match substitute(arg, ctx, new.as_ref(), old.as_ref(), vars) {
                        Ok(text) => text,
                        Err(e) => return Ok(Err(e)),
                    };
                    match Box::pin(value_of(database, state, &text)).await? {
                        Ok(v) => values.push(v.unwrap_or_else(|| "<NULL>".to_string())),
                        Err(e) => return Ok(Err(e)),
                    }
                }
                let mut next = values.into_iter();
                let mut chars = format.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '%' {
                        if chars.peek() == Some(&'%') {
                            chars.next();
                            message.push('%');
                        } else {
                            message.push_str(&next.next().unwrap_or_default());
                        }
                    } else {
                        message.push(c);
                    }
                }
                if level == "EXCEPTION" {
                    let code = errcode.clone().filter(|c| c.len() == 5).unwrap_or_else(|| "P0001".to_string());
                    return Ok(Err((code, message)));
                }
                let severity: &'static str = match level.as_str() {
                    "WARNING" => "WARNING",
                    "INFO" => "INFO",
                    "LOG" => "LOG",
                    "DEBUG" => "DEBUG",
                    _ => "NOTICE",
                };
                crate::core::notice::push(severity, "00000", message);
            }
            PlStmt::Block { body, handlers } => {
                // A block with handlers is PostgreSQL's subtransaction: in a
                // transaction block, a savepoint its failure rolls back to.
                // Outside one each statement commits on its own, so there
                // is nothing to take back; in an implicit block (a batch)
                // no savepoint can be had, and the error is the batch's.
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let in_block = state.transaction.is_some();
                let mut savepoint: Option<String> = None;
                if !handlers.is_empty() && state.transaction.as_ref().is_some_and(|t| !t.implicit) {
                    let name = format!("rdb_plpgsql_{}", NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
                    if Box::pin(super::view_statements::run_internal(database, state, &format!("SAVEPOINT {name}"))).await?.is_ok() {
                        savepoint = Some(name);
                    }
                }
                let batch_error = state.error_in_batch;
                match Box::pin(run_statements(database, state, ctx, body, new, old, vars)).await? {
                    Ok(result) => {
                        if let Some(name) = &savepoint {
                            let _ = Box::pin(super::view_statements::run_internal(database, state, &format!("RELEASE SAVEPOINT {name}"))).await?;
                        }
                        if result.is_some() {
                            return Ok(Ok(result));
                        }
                    }
                    Err((code, message)) => {
                        let handler = handlers
                            .iter()
                            .find(|(conditions, _)| conditions.iter().any(|c| condition_takes(c, &code)))
                            .filter(|_| savepoint.is_some() || !in_block);
                        let Some((_, stmts)) = handler else {
                            return Ok(Err((code, message)));
                        };
                        // The error was handled: it is not the statement's.
                        // Cleared before the savepoint is rolled back to,
                        // which would otherwise be reported as failing with
                        // it.
                        state.statement_failed = false;
                        state.statement_error = None;
                        if let Some(name) = &savepoint {
                            for sql in [format!("ROLLBACK TO SAVEPOINT {name}"), format!("RELEASE SAVEPOINT {name}")] {
                                if let Err(e) = Box::pin(super::view_statements::run_internal(database, state, &sql)).await? {
                                    return Ok(Err(failure(e)));
                                }
                            }
                        }
                        state.statement_failed = false;
                        state.statement_error = None;
                        state.error_in_batch = batch_error;
                        vars.push(("sqlstate".to_string(), "text".to_string(), Some(code)));
                        vars.push(("sqlerrm".to_string(), "text".to_string(), Some(message)));
                        let ran = Box::pin(run_statements(database, state, ctx, stmts, new, old, vars)).await?;
                        for name in ["sqlerrm", "sqlstate"] {
                            if let Some(at) = vars.iter().rposition(|(n, _, _)| n == name) {
                                vars.remove(at);
                            }
                        }
                        match ran {
                            Ok(None) => {}
                            other => return Ok(other),
                        }
                    }
                }
            }
            PlStmt::ForQuery { targets, query: source, dynamic, body } => {
                let text = match substitute(source, ctx, new.as_ref(), old.as_ref(), vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                let sql = if *dynamic {
                    match Box::pin(value_of(database, state, &text)).await? {
                        Ok(Some(sql)) => sql,
                        Ok(None) => return Ok(Err(("22004".into(), "query string argument of EXECUTE is null".into()))),
                        Err(e) => return Ok(Err(e)),
                    }
                } else {
                    text
                };
                // Read whole before the first iteration, so the body may
                // change what the query read.
                let found = match Box::pin(query(database, state, &sql)).await? {
                    Ok(found) => found,
                    Err(e) => return Ok(Err(failure(e))),
                };
                for row in found.rows {
                    if let Err(e) = assign_row(database, vars, targets, &found.names, &found.types, Some(row)) {
                        return Ok(Err(e));
                    }
                    match Box::pin(run_statements(database, state, ctx, body, new, old, vars)).await? {
                        Ok(None) | Ok(Some(Returned::Leave { exit: false })) => {}
                        Ok(Some(Returned::Leave { exit: true })) => break,
                        other => return Ok(other),
                    }
                }
            }
            PlStmt::ForRange { var, from, to, step, reverse, body } => {
                let mut bounds = [0i64; 3];
                for (slot, expr) in bounds.iter_mut().zip([Some(from), Some(to), step.as_ref()]) {
                    let Some(expr) = expr else {
                        *slot = 1;
                        continue;
                    };
                    let text = match substitute(expr, ctx, new.as_ref(), old.as_ref(), vars) {
                        Ok(text) => text,
                        Err(e) => return Ok(Err(e)),
                    };
                    *slot = match Box::pin(value_of(database, state, &text)).await? {
                        Ok(Some(v)) => match v.trim().parse::<i64>() {
                            Ok(n) => n,
                            Err(_) => return Ok(Err(("22P02".into(), format!("invalid input syntax for type integer: \"{v}\"")))),
                        },
                        Ok(None) => return Ok(Err(("22004".into(), "a bound of a FOR loop cannot be null".into()))),
                        Err(e) => return Ok(Err(e)),
                    };
                }
                let [from, to, step] = bounds;
                if step <= 0 {
                    return Ok(Err(("22023".into(), "BY value of FOR loop must be greater than zero".into())));
                }
                // The loop variable is the loop's own.
                vars.push((var.clone(), "integer".to_string(), None));
                let slot = vars.len() - 1;
                let mut i = from;
                let mut outcome = None;
                while if *reverse { i >= to } else { i <= to } {
                    vars[slot].2 = Some(i.to_string());
                    match Box::pin(run_statements(database, state, ctx, body, new, old, vars)).await? {
                        Ok(None) | Ok(Some(Returned::Leave { exit: false })) => {}
                        Ok(Some(Returned::Leave { exit: true })) => break,
                        other => {
                            outcome = Some(other);
                            break;
                        }
                    }
                    i = match if *reverse { i.checked_sub(step) } else { i.checked_add(step) } {
                        Some(next) => next,
                        None => break,
                    };
                }
                vars.remove(slot);
                if let Some(other) = outcome {
                    return Ok(other);
                }
            }
            PlStmt::While { cond, body } => loop {
                if let Some(cond) = cond {
                    let text = match substitute(cond, ctx, new.as_ref(), old.as_ref(), vars) {
                        Ok(text) => text,
                        Err(e) => return Ok(Err(e)),
                    };
                    match Box::pin(is_true(database, state, &text)).await? {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(e) => return Ok(Err(e)),
                    }
                }
                match Box::pin(run_statements(database, state, ctx, body, new, old, vars)).await? {
                    Ok(None) | Ok(Some(Returned::Leave { exit: false })) => {}
                    Ok(Some(Returned::Leave { exit: true })) => break,
                    other => return Ok(other),
                }
            },
            PlStmt::Leave { exit, when } => {
                let leaves = match when {
                    None => true,
                    Some(cond) => {
                        let text = match substitute(cond, ctx, new.as_ref(), old.as_ref(), vars) {
                            Ok(text) => text,
                            Err(e) => return Ok(Err(e)),
                        };
                        match Box::pin(is_true(database, state, &text)).await? {
                            Ok(leaves) => leaves,
                            Err(e) => return Ok(Err(e)),
                        }
                    }
                };
                if leaves {
                    return Ok(Ok(Some(Returned::Leave { exit: *exit })));
                }
            }
            PlStmt::SelectInto { targets, query: source } => {
                let text = match substitute(source, ctx, new.as_ref(), old.as_ref(), vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                let found = match Box::pin(query(database, state, &text)).await? {
                    Ok(found) => found,
                    Err(e) => return Ok(Err(failure(e))),
                };
                let first = found.rows.into_iter().next();
                if let Err(e) = assign_row(database, vars, targets, &found.names, &found.types, first) {
                    return Ok(Err(e));
                }
            }
            // `EXECUTE expr INTO target`: the dynamic query's first row.
            // Its targets are split off before the variables are written
            // in, or the target itself would be its own value.
            PlStmt::Sql(sql)
                if dynamic_execute(sql).is_some_and(|expr| crate::core::rql::join::find_keyword(expr, "INTO", 0).is_some()) =>
            {
                let expr = dynamic_execute(sql).expect("checked by the guard");
                if crate::core::rql::join::find_keyword(expr, "USING", 0).is_some() {
                    return Ok(Err(("0A000".into(), "EXECUTE … USING is not supported yet".into())));
                }
                let into = crate::core::rql::join::find_keyword(expr, "INTO", 0).expect("checked by the guard");
                let after = expr[into + 4..].trim();
                let after = if after.len() > 6 && after.as_bytes()[..6].eq_ignore_ascii_case(b"STRICT") && after.as_bytes()[6].is_ascii_whitespace() {
                    after[6..].trim_start()
                } else {
                    after
                };
                let targets: Vec<String> = after.split(',').map(|t| t.trim().to_ascii_lowercase()).filter(|t| !t.is_empty()).collect();
                let text = match substitute(expr[..into].trim(), ctx, new.as_ref(), old.as_ref(), vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                let dynamic = match Box::pin(value_of(database, state, &text)).await? {
                    Ok(Some(sql)) => sql,
                    Ok(None) => return Ok(Err(("22004".into(), "query string argument of EXECUTE is null".into()))),
                    Err(e) => return Ok(Err(e)),
                };
                let found = match Box::pin(query(database, state, &dynamic)).await? {
                    Ok(found) => found,
                    Err(e) => return Ok(Err(failure(e))),
                };
                let first = found.rows.into_iter().next();
                if let Err(e) = assign_row(database, vars, &targets, &found.names, &found.types, first) {
                    return Ok(Err(e));
                }
            }
            PlStmt::Perform(sql) | PlStmt::Sql(sql) => {
                let mut text = match substitute(sql, ctx, new.as_ref(), old.as_ref(), vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                // `EXECUTE expr`: PL/pgSQL's dynamic statement, the text the
                // expression computes (`EXECUTE format('…', x)`). Its
                // `USING` is refused rather than dropped.
                if matches!(stmt, PlStmt::Sql(_)) && let Some(expr) = dynamic_execute(&text) {
                    if crate::core::rql::join::find_keyword(expr, "USING", 0).is_some() {
                        return Ok(Err(("0A000".into(), "EXECUTE … USING is not supported yet".into())));
                    }
                    match Box::pin(value_of(database, state, expr)).await? {
                        Ok(Some(sql)) => text = sql,
                        Ok(None) => return Ok(Err(("22004".into(), "query string argument of EXECUTE is null".into()))),
                        Err(e) => return Ok(Err(e)),
                    }
                }
                if let Err(e) = Box::pin(super::view_statements::run_internal(database, state, &text)).await? {
                    return Ok(Err(failure(e)));
                }
                // A `DO` block's schema changes, for a branch's DDL log
                // (`branch_ddl`); a trigger's are its statement's business.
                if ctx.trigger.function == "inline_code_block" && super::schema_verb(&text) {
                    state.branch_ddl.push(text);
                }
            }
        }
    }
    Ok(Ok(None))
}

/// The expression of PL/pgSQL's `EXECUTE expr`, `None` for any other
/// statement.
fn dynamic_execute(text: &str) -> Option<&str> {
    let t = text.trim();
    (t.len() > 8 && t.as_bytes()[..7].eq_ignore_ascii_case(b"EXECUTE") && !crate::core::sql_scan::is_identifier_byte(t.as_bytes()[7]))
        .then(|| t[7..].trim())
}

/// The body of a `DO [LANGUAGE plpgsql] '…'` block (a dollar-quoted or
/// plain string); `None` for any other statement, 0A000 for another
/// language.
pub(super) fn parse_do(stmt: &str) -> Option<Result<String, Failure>> {
    let s = stmt.trim().trim_end_matches(';').trim_end();
    if !(s.len() > 3 && s.as_bytes()[..2].eq_ignore_ascii_case(b"DO") && s.as_bytes()[2].is_ascii_whitespace()) {
        return None;
    }
    let mut rest = s[2..].trim_start();
    let mut language: Option<String> = None;
    let mut take_language = |rest: &mut &str| {
        let r = rest.trim_start();
        if r.len() > 8 && r.as_bytes()[..8].eq_ignore_ascii_case(b"LANGUAGE") {
            let after = r[8..].trim_start();
            let len = after.bytes().take_while(|b| crate::core::sql_scan::is_identifier_byte(*b) || *b == b'\'').count();
            language = Some(after[..len].trim_matches('\'').to_ascii_lowercase());
            *rest = after[len..].trim_start();
        }
    };
    take_language(&mut rest);
    let bytes = rest.as_bytes();
    let (body, end) = if bytes.first() == Some(&b'$') {
        let tag = crate::core::sql_scan::dollar_tag_len(bytes, 0)?;
        let end = crate::core::sql_scan::dollar_quote_end(bytes, 0)?;
        (rest[tag..end - tag].to_string(), end)
    } else if bytes.first() == Some(&b'\'') {
        let end = crate::core::sql_scan::string_literal_end(bytes, 0, false);
        (rest[1..end - 1].replace("''", "'"), end)
    } else {
        return Some(Err(("42601".into(), "DO needs its code as a string".into())));
    };
    let mut tail = &rest[end..];
    take_language(&mut tail);
    if !tail.trim().is_empty() {
        return Some(Err(("42601".into(), format!("syntax error at or near \"{}\"", tail.trim()))));
    }
    match language.as_deref() {
        None | Some("plpgsql") => Some(Ok(body)),
        Some(other) => Some(Err(("0A000".into(), format!("language \"{other}\" is not supported in DO; only plpgsql is")))),
    }
}

/// Runs a `DO` block's PL/pgSQL, as an anonymous function with no
/// arguments: its `DECLARE`d variables, then its statements.
pub(super) async fn run_do(database: &Arc<Database>, state: &mut SessionState, body: &str) -> io::Result<Result<(), Failure>> {
    let block = match triggers::parse_plpgsql(body) {
        Ok(block) => block,
        Err((code, message)) => return Ok(Err((code.to_string(), message))),
    };
    // No trigger: nothing reads these but the `TG_` variables, which a
    // `DO` block has no use for.
    let anonymous = TriggerDef {
        name: String::new(),
        table: String::new(),
        timing: Timing::After,
        events: Vec::new(),
        update_columns: Vec::new(),
        row_level: false,
        when: String::new(),
        function: "inline_code_block".to_string(),
        args: Vec::new(),
    };
    let ctx = Context { trigger: &anonymous, event: Event::Insert, row_level: false, timing: Timing::After, fields: &[] };
    let mut vars: Vec<(String, String, Option<String>)> = Vec::new();
    for (name, ty, init) in &block.declarations {
        let value = match init {
            Some(expr) => {
                let text = match substitute(expr, &ctx, None, None, &vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                match Box::pin(value_of(database, state, &text)).await? {
                    Ok(v) => v,
                    Err(e) => return Ok(Err(e)),
                }
            }
            None => None,
        };
        vars.push((name.clone(), ty.clone(), value));
    }
    let mut new_row = None;
    match Box::pin(run_statements(database, state, &ctx, &block.body, &mut new_row, &None, &mut vars)).await? {
        Ok(Some(Returned::Row(Some(_)))) => Ok(Err(("42804".into(), "RETURN cannot have a parameter in a DO block".into()))),
        Ok(Some(Returned::Leave { exit })) => Ok(Err((
            "42601".into(),
            format!("{} cannot be used outside a loop", if exit { "EXIT" } else { "CONTINUE" }),
        ))),
        Ok(_) => Ok(Ok(())),
        Err(e) => Ok(Err(e)),
    }
}

/// Runs `trigger`'s function over a row: the `NEW` it returns (`None` to
/// skip the row), or for an `AFTER` or statement trigger nothing that
/// matters.
async fn fire_one(
    database: &Arc<Database>,
    state: &mut SessionState,
    trigger: &TriggerDef,
    block: &PlBlock,
    event: Event,
    fields: &[crate::core::row::schema::SchemaField],
    new: Option<Values>,
    old: Option<Values>,
) -> io::Result<Result<Option<Values>, Failure>> {
    let ctx = Context { trigger, event, row_level: trigger.row_level, timing: trigger.timing, fields };
    let mut vars: Vec<(String, String, Option<String>)> = Vec::new();
    for (name, ty, init) in &block.declarations {
        let value = match init {
            Some(expr) => {
                let text = match substitute(expr, &ctx, new.as_ref(), old.as_ref(), &vars) {
                    Ok(text) => text,
                    Err(e) => return Ok(Err(e)),
                };
                match Box::pin(value_of(database, state, &text)).await? {
                    Ok(v) => v,
                    Err(e) => return Ok(Err(e)),
                }
            }
            None => None,
        };
        vars.push((name.clone(), ty.clone(), value));
    }
    let mut new_row = new;
    let returned = match Box::pin(run_statements(database, state, &ctx, &block.body, &mut new_row, &old, &mut vars)).await? {
        Ok(r) => r,
        Err(e) => return Ok(Err(e)),
    };
    Ok(Ok(match (trigger.timing, trigger.row_level, returned) {
        (Timing::Before, true, Some(Returned::Row(row))) => row,
        (Timing::Before, true, _) => {
            return Ok(Err((
                "2F005".into(),
                format!("control reached end of trigger procedure without RETURN: {}()", trigger.function),
            )));
        }
        _ => new_row.or(old),
    }))
}

/// Whether `trigger` fires for this row: its `UPDATE OF` columns and its
/// `WHEN`.
async fn applies(
    database: &Arc<Database>,
    state: &mut SessionState,
    trigger: &TriggerDef,
    event: Event,
    assigned: &[String],
    fields: &[crate::core::row::schema::SchemaField],
    new: Option<&Values>,
    old: Option<&Values>,
) -> io::Result<Result<bool, Failure>> {
    if event == Event::Update
        && !trigger.update_columns.is_empty()
        && !trigger.update_columns.iter().any(|c| assigned.iter().any(|a| a.eq_ignore_ascii_case(c)))
    {
        return Ok(Ok(false));
    }
    if trigger.when.is_empty() {
        return Ok(Ok(true));
    }
    let ctx = Context { trigger, event, row_level: trigger.row_level, timing: trigger.timing, fields };
    let text = match substitute(&trigger.when, &ctx, new, old, &[]) {
        Ok(text) => text,
        Err(e) => return Ok(Err(e)),
    };
    Ok(match Box::pin(value_of(database, state, &format!("CASE WHEN {text} THEN TRUE ELSE FALSE END"))).await? {
        Ok(v) => Ok(v.as_deref() == Some("t")),
        Err(e) => Err(e),
    })
}

/// The table's triggers of `event`, each with its function's body read.
async fn triggers_for(
    database: &Arc<Database>,
    tx: Option<TxId>,
    table: &str,
    event: Event,
) -> Result<Vec<(TriggerDef, PlBlock)>, Failure> {
    let mut out = Vec::new();
    for trigger in triggers::load(database, Some(table), tx).await.map_err(failure)? {
        if !trigger.events.contains(&event) {
            continue;
        }
        let routine = crate::core::rql::functions::load(database, Some(&trigger.function), tx)
            .await
            .map_err(failure)?
            .into_iter()
            .find(|r| r.arg_types.is_empty() && r.return_type == "trigger")
            .ok_or_else(|| ("42883".to_string(), format!("function {}() does not exist", trigger.function)))?;
        if routine.language != "plpgsql" {
            return Err(("0A000".into(), format!("trigger function {}() must be LANGUAGE plpgsql", routine.name)));
        }
        let block = triggers::parse_plpgsql(&routine.body).map_err(|(c, m)| (c.to_string(), m))?;
        out.push((trigger, block));
    }
    Ok(out)
}

/// The columns that address one row: the primary key, or the first unique
/// key.
pub(super) fn key_columns(schema: &crate::core::row::schema::TableSchema) -> Option<Vec<String>> {
    if let Some(pk) = &schema.primary_key {
        let columns: Vec<String> = pk.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
        if !columns.is_empty() {
            return Some(columns);
        }
    }
    schema.unique_keys.first().map(|k| k.columns.clone())
}

/// A table's visible columns, in order.
pub(super) fn visible_fields(schema: &crate::core::row::schema::TableSchema) -> Vec<crate::core::row::schema::SchemaField> {
    let mut fields: Vec<_> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .cloned()
        .collect();
    fields.sort_by_key(|f| f.write_order);
    fields
}

pub(super) fn quote_ident(name: &str) -> String {
    crate::core::rql::views::quote_ident(name)
}

/// The statement's target table when it is an `INSERT`, `UPDATE` or
/// `DELETE` of a table that has triggers this session is not already
/// running for it.
pub(super) async fn triggered_write(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<(Event, String)> {
    if !database.has_table(triggers::TRIGGER_TABLE) {
        return None;
    }
    let first = stmt.trim_start().split(|c: char| c.is_whitespace()).next()?.to_ascii_uppercase();
    let event = match first.as_str() {
        "INSERT" => Event::Insert,
        "UPDATE" => Event::Update,
        "DELETE" => Event::Delete,
        "TRUNCATE" => Event::Truncate,
        _ => return None,
    };
    let table = if event == Event::Truncate {
        stmt.split_whitespace().nth(1).filter(|w| !w.eq_ignore_ascii_case("TABLE")).or_else(|| stmt.split_whitespace().nth(2))?
            .trim_end_matches(';')
            .to_string()
    } else {
        crate::core::rql::lock_set::relation_references(stmt)
            .into_iter()
            .find(|r| r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare)?
            .name
    };
    let table = crate::core::sql_scan::last_name_segment(&table).trim_matches('"').to_string();
    if state.triggers_suppressed.iter().any(|t| t == &table) {
        return None;
    }
    triggers::may_have_triggers(database, &table).await.then_some((event, table))
}

/// Runs the write `stmt` to `table` with its triggers (see the module
/// comment), and answers it.
pub(super) async fn run_triggered_write<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    event: Event,
    table: &str,
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    let all = match triggers_for(database, tx, table, event).await {
        Ok(all) => all,
        Err((code, message)) => return fail_statement(state, stream, &code, &message).await,
    };
    if all.is_empty() {
        // The registry said maybe; the catalogue says none for this event.
        state.triggers_suppressed.push(table.to_string());
        let ran = Box::pin(execute_single_statement(database, state, stream, stmt, result_formats, omit_description)).await;
        state.triggers_suppressed.pop();
        return ran;
    }
    let Some(view) = database.schema_view(table) else {
        return fail_statement(state, stream, "42P01", &format!("relation \"{table}\" does not exist")).await;
    };
    let schema = view.schema().clone();
    drop(view);
    let fields = visible_fields(&schema);

    // One transaction for all of it.
    let own_block = state.transaction.is_none();
    if own_block {
        if let Err(e) = super::view_statements::run_internal(database, state, "BEGIN").await? {
            return send_error_response(stream, sqlstate_for(&e), &e).await;
        }
    }
    let mut out = Vec::<u8>::new();
    let result = Box::pin(fire_write(database, state, &mut out, stmt, event, table, &schema, &fields, &all, result_formats, omit_description)).await?;
    if own_block {
        let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        let ended = super::view_statements::run_internal(database, state, end).await?;
        if result.is_ok() && let Err(e) = ended {
            return send_error_response(stream, sqlstate_for(&e), &e).await;
        }
    }
    match result {
        Ok(tag) => {
            stream.write_all(&out).await?;
            send_command_complete(stream, &tag).await
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

/// The rows of a write and what each trigger makes of them; the command
/// tag, or the failure that ends it. Rows a `RETURNING` reports are put in
/// `out`.
#[allow(clippy::too_many_arguments)]
async fn fire_write(
    database: &Arc<Database>,
    state: &mut SessionState,
    out: &mut Vec<u8>,
    stmt: &str,
    event: Event,
    table: &str,
    schema: &crate::core::row::schema::TableSchema,
    fields: &[crate::core::row::schema::SchemaField],
    all: &[(TriggerDef, PlBlock)],
    result_formats: &[i16],
    omit_description: bool,
) -> io::Result<Result<String, Failure>> {
    let statement_triggers = |timing: Timing| -> Vec<&(TriggerDef, PlBlock)> {
        all.iter().filter(|(t, _)| !t.row_level && t.timing == timing).collect()
    };
    let row_triggers = |timing: Timing| -> Vec<&(TriggerDef, PlBlock)> {
        all.iter().filter(|(t, _)| t.row_level && t.timing == timing).collect()
    };
    for (trigger, block) in statement_triggers(Timing::Before) {
        if let Err(e) = Box::pin(fire_one(database, state, trigger, block, event, fields, None, None)).await? {
            return Ok(Err(e));
        }
    }
    let quoted = quote_ident(table);
    let returning_list = crate::core::rql::lexer_ir::returning_split(stmt).map(|(_, tail)| tail.trim().to_string());
    let head = crate::core::rql::lexer_ir::returning_split(stmt).map(|(h, _)| h).unwrap_or(stmt).trim();

    // What the statement does to which rows: (OLD, NEW, assigned columns).
    let mut work: Vec<(Option<Values>, Option<Values>, Vec<String>)> = Vec::new();
    match event {
        Event::Truncate => {
            state.triggers_suppressed.push(table.to_string());
            let ran = super::view_statements::run_internal(database, state, stmt).await?;
            state.triggers_suppressed.pop();
            if let Err(e) = ran {
                return Ok(Err(failure(e)));
            }
            for (trigger, block) in statement_triggers(Timing::After) {
                if let Err(e) = Box::pin(fire_one(database, state, trigger, block, event, fields, None, None)).await? {
                    return Ok(Err(e));
                }
            }
            return Ok(Ok("TRUNCATE TABLE".to_string()));
        }
        Event::Insert => {
            if crate::core::rql::join::find_keyword(head, "CONFLICT", 0).is_some() {
                return Ok(Err(("0A000".into(), "INSERT … ON CONFLICT on a table with triggers is not supported yet".into())));
            }
            let tuples: Vec<Values> = match Box::pin(insert_rows(database, state, head, fields)).await? {
                Ok(tuples) => tuples,
                Err(e) => return Ok(Err(e)),
            };
            for tuple in tuples {
                let assigned = tuple.iter().map(|(c, _)| c.clone()).collect();
                // Every column, the ones written given, the rest NULL until
                // the engine fills them (a default, a serial).
                let new: Values = fields
                    .iter()
                    .map(|f| (f.name.clone(), tuple.iter().find(|(c, _)| c.eq_ignore_ascii_case(&f.name)).and_then(|(_, v)| v.clone())))
                    .collect();
                work.push((None, Some(new), assigned));
            }
        }
        Event::Update | Event::Delete => {
            if (event == Event::Update && crate::core::rql::join::find_keyword(head, "FROM", 0).is_some())
                || (event == Event::Delete && crate::core::rql::join::find_keyword(head, "USING", 0).is_some())
            {
                return Ok(Err(("0A000".into(), "UPDATE … FROM and DELETE … USING on a table with triggers are not supported yet".into())));
            }
            if key_columns(schema).is_none() {
                return Ok(Err((
                    "0A000".into(),
                    format!("table \"{table}\" needs a primary key for its UPDATE and DELETE triggers to address each row"),
                )));
            }
            let assignments = if event == Event::Update {
                crate::core::rql::lexer_ur::assignments_of_update(head).unwrap_or_default()
            } else {
                Vec::new()
            };
            let where_clause = crate::core::rql::join::find_keyword(head, "WHERE", 0).map(|at| head[at..].to_string()).unwrap_or_default();
            let mut select = String::from("SELECT ");
            select.push_str(&fields.iter().map(|f| quote_ident(&f.name)).collect::<Vec<_>>().join(", "));
            for (i, (_, expr)) in assignments.iter().enumerate() {
                select.push_str(&format!(", ({}) AS __new_{i}", expr));
            }
            select.push_str(&format!(" FROM {quoted} {where_clause}"));
            let rows = match query(database, state, &select).await? {
                Ok(rows) => rows,
                Err(e) => return Ok(Err(failure(e))),
            };
            let assigned: Vec<String> = assignments.iter().map(|(c, _)| c.trim_matches('"').to_string()).collect();
            for row in rows.rows {
                let old: Values = fields.iter().zip(row.iter()).map(|(f, v)| (f.name.clone(), v.clone())).collect();
                let new = (event == Event::Update).then(|| {
                    let mut new = old.clone();
                    for (i, column) in assigned.iter().enumerate() {
                        if let Some(cell) = new.iter_mut().find(|(n, _)| n.eq_ignore_ascii_case(column)) {
                            cell.1 = row.get(fields.len() + i).cloned().flatten();
                        }
                    }
                    new
                });
                work.push((Some(old), new, assigned.clone()));
            }
        }
    }

    let mut count = 0u64;
    let mut described = false;
    for (old, mut new, assigned) in work {
        // BEFORE row triggers, in name order, each over the last one's NEW.
        let mut skipped = false;
        for (trigger, block) in row_triggers(Timing::Before) {
            match Box::pin(applies(database, state, trigger, event, &assigned, fields, new.as_ref(), old.as_ref())).await? {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => return Ok(Err(e)),
            }
            let input = if event == Event::Delete { None } else { new.clone() };
            match Box::pin(fire_one(database, state, trigger, block, event, fields, input, old.clone())).await? {
                Ok(Some(row)) => {
                    if event != Event::Delete {
                        new = Some(row);
                    }
                }
                Ok(None) => {
                    skipped = true;
                    break;
                }
                Err(e) => return Ok(Err(e)),
            }
        }
        if skipped {
            continue;
        }
        // The write, with this table's triggers held off.
        let write = match (event, &old, &new) {
            (Event::Insert, _, Some(new)) => {
                // The columns the statement gave, or a trigger set; the rest
                // are the engine's to fill.
                let given: Vec<&(String, Option<String>)> = new
                    .iter()
                    .filter(|(c, v)| assigned.iter().any(|a| a.eq_ignore_ascii_case(c)) || v.is_some())
                    .collect();
                if given.is_empty() {
                    format!("INSERT INTO {quoted} DEFAULT VALUES RETURNING *")
                } else {
                    format!(
                        "INSERT INTO {quoted} ({}) VALUES ({}) RETURNING *",
                        given.iter().map(|(c, _)| quote_ident(c)).collect::<Vec<_>>().join(", "),
                        given.iter().map(|(c, v)| literal(v, field(fields, c).and_then(cast_of))).collect::<Vec<_>>().join(", ")
                    )
                }
            }
            (Event::Update, Some(old), Some(new)) => {
                let changed: Vec<String> = new
                    .iter()
                    .zip(old.iter())
                    .filter(|((c, n), (_, o))| n != o || assigned.iter().any(|a| a.eq_ignore_ascii_case(c)))
                    .map(|((c, v), _)| format!("{} = {}", quote_ident(c), literal(v, field(fields, c).and_then(cast_of))))
                    .collect();
                if changed.is_empty() {
                    format!("SELECT * FROM {quoted} WHERE {}", key_match(schema, fields, old))
                } else {
                    format!("UPDATE {quoted} SET {} WHERE {} RETURNING *", changed.join(", "), key_match(schema, fields, old))
                }
            }
            (Event::Delete, Some(old), _) => format!("DELETE FROM {quoted} WHERE {} RETURNING *", key_match(schema, fields, old)),
            _ => continue,
        };
        state.triggers_suppressed.push(table.to_string());
        let written = query(database, state, &write).await?;
        state.triggers_suppressed.pop();
        let stored = match written {
            Ok(rows) => rows,
            Err(e) => return Ok(Err(failure(e))),
        };
        for row in stored.rows {
            count += 1;
            let after: Values = stored.names.iter().cloned().zip(row.into_iter()).collect();
            let (after_new, after_old) = match event {
                Event::Delete => (None, Some(after.clone())),
                _ => (Some(after.clone()), old.clone()),
            };
            for (trigger, block) in row_triggers(Timing::After) {
                match Box::pin(applies(database, state, trigger, event, &assigned, fields, after_new.as_ref(), after_old.as_ref())).await? {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => return Ok(Err(e)),
                }
                if let Err(e) = Box::pin(fire_one(database, state, trigger, block, event, fields, after_new.clone(), after_old.clone())).await? {
                    return Ok(Err(e));
                }
            }
            // The statement's own RETURNING, computed over the stored row.
            if let Some(list) = &returning_list {
                let ctx_row: Values = after;
                let projected = match project(list, &ctx_row, fields) {
                    Ok(text) => text,
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
    }
    if returning_list.is_some() && !described && !omit_description {
        // No row: the description is still owed. Computed over no row.
        let empty: Values = fields.iter().map(|f| (f.name.clone(), None)).collect();
        if let Ok(projected) = project(returning_list.as_deref().unwrap_or("*"), &empty, fields) {
            if let Ok(rows) = query_in(database, state, &format!("SELECT {projected} WHERE FALSE"), result_formats).await? {
                if let Some(description) = rows.description {
                    out.extend_from_slice(&description);
                }
            }
        }
    }
    for (trigger, block) in statement_triggers(Timing::After) {
        if let Err(e) = Box::pin(fire_one(database, state, trigger, block, event, fields, None, None)).await? {
            return Ok(Err(e));
        }
    }
    Ok(Ok(match event {
        Event::Insert => format!("INSERT 0 {count}"),
        Event::Update => format!("UPDATE {count}"),
        Event::Delete => format!("DELETE {count}"),
        Event::Truncate => "TRUNCATE TABLE".to_string(),
    }))
}

/// `key = literal AND …` for the row `old`.
pub(super) fn key_match(schema: &crate::core::row::schema::TableSchema, fields: &[crate::core::row::schema::SchemaField], old: &Values) -> String {
    key_columns(schema)
        .unwrap_or_default()
        .iter()
        .map(|k| {
            let value = old.iter().find(|(c, _)| c.eq_ignore_ascii_case(k)).and_then(|(_, v)| v.clone());
            format!("{} = {}", quote_ident(k), literal(&value, field(fields, k).and_then(cast_of)))
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// A `RETURNING` list over the stored row: `*` as every column, and each
/// column name (bare, or qualified by the table) as its value.
fn project(list: &str, row: &Values, fields: &[crate::core::row::schema::SchemaField]) -> Result<String, Failure> {
    let lit = |c: &str| {
        let value = row.iter().find(|(n, _)| n.eq_ignore_ascii_case(c)).and_then(|(_, v)| v.clone());
        literal(&value, field(fields, c).and_then(cast_of))
    };
    let items: Vec<String> = crate::core::sql_scan::split_top_level(list, b',')
        .into_iter()
        .map(|item| {
            let item = item.trim();
            if item == "*" {
                return row.iter().map(|(c, _)| format!("{} AS {}", lit(c), quote_ident(c))).collect::<Vec<_>>().join(", ");
            }
            // The item's text with each column name replaced.
            let bytes = item.as_bytes();
            let mut out = String::new();
            let mut i = 0;
            let mut named: Option<String> = None;
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'\'' {
                    let end = crate::core::sql_scan::string_literal_end(bytes, i, false);
                    out.push_str(&item[i..end]);
                    i = end;
                    continue;
                }
                if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
                    let (word, end) = if b == b'"' {
                        let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
                        (item[i + 1..end - 1].replace("\"\"", "\""), end)
                    } else {
                        let mut j = i;
                        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                            j += 1;
                        }
                        (item[i..j].to_ascii_lowercase(), j)
                    };
                    // `t.col`: the qualifier goes with it.
                    if bytes.get(end) == Some(&b'.') {
                        i = end + 1;
                        continue;
                    }
                    let after_as = out.trim_end().to_ascii_uppercase().ends_with(" AS") || out.trim_end().eq_ignore_ascii_case("AS");
                    if !after_as && bytes.get(end) != Some(&b'(') && field(fields, &word).is_some() {
                        if named.is_none() && out.trim().is_empty() {
                            named = Some(word.clone());
                        }
                        out.push_str(&lit(&word));
                    } else {
                        out.push_str(&item[i..end]);
                    }
                    i = end;
                    continue;
                }
                let ch = item[i..].chars().next().expect("in bounds");
                out.push(ch);
                i += ch.len_utf8();
            }
            // A bare column keeps its name as the output's.
            let has_alias = crate::core::rql::join::find_keyword(item, "AS", 0).is_some();
            match named {
                Some(name) if !has_alias && out.trim() == lit(&name) => format!("{} AS {}", out.trim(), quote_ident(&name)),
                _ => out,
            }
        })
        .collect();
    Ok(items.join(", "))
}

/// An `INSERT`'s rows as `(column, value)` lists, the values computed (each
/// expression once): from `VALUES` tuples or from its query.
async fn insert_rows(
    database: &Arc<Database>,
    state: &mut SessionState,
    head: &str,
    fields: &[crate::core::row::schema::SchemaField],
) -> io::Result<Result<Vec<Values>, Failure>> {
    let all_columns: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
    // The column list, if any, and what follows it.
    let into = crate::core::rql::join::find_keyword(head, "INTO", 0).map(|a| a + 4).unwrap_or(0);
    let after_into = head[into..].trim_start();
    let name_end = after_into.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(after_into.len());
    let rest = after_into[name_end..].trim_start();
    let (columns, source) = if rest.starts_with('(') && !crate::core::sql_scan::starts_with_keyword(rest[1..].trim_start(), "SELECT") {
        let close = crate::core::sql_scan::matching_close(rest, 0).unwrap_or(rest.len() - 1);
        let columns: Vec<String> = crate::core::sql_scan::split_top_level(&rest[1..close], b',')
            .into_iter()
            .map(|c| {
                let c = c.trim();
                if c.starts_with('"') { c.trim_matches('"').replace("\"\"", "\"") } else { c.to_ascii_lowercase() }
            })
            .collect();
        (columns, rest[close + 1..].trim_start())
    } else {
        (all_columns.clone(), rest)
    };
    let upper = source.to_ascii_uppercase();
    if upper.starts_with("DEFAULT VALUES") {
        return Ok(Ok(vec![Vec::new()]));
    }
    let query_text = if upper.starts_with("VALUES") {
        // Each tuple's expressions, computed once, as a query.
        let tuples_text = source[6..].trim();
        let mut tuples = Vec::new();
        let mut at = 0;
        let bytes = tuples_text.as_bytes();
        while at < bytes.len() {
            if bytes[at] == b'(' {
                let close = crate::core::sql_scan::matching_close(tuples_text, at).unwrap_or(bytes.len() - 1);
                tuples.push(&tuples_text[at + 1..close]);
                at = close + 1;
            } else {
                at += 1;
            }
        }
        let mut selects = Vec::new();
        let mut defaults: Vec<Vec<bool>> = Vec::new();
        for tuple in tuples {
            let values = crate::core::sql_scan::split_top_level(tuple, b',');
            defaults.push(values.iter().map(|v| v.trim().eq_ignore_ascii_case("DEFAULT")).collect());
            let items: Vec<String> = values
                .iter()
                .zip(&columns)
                .map(|(v, c)| {
                    let v = v.trim();
                    if v.eq_ignore_ascii_case("DEFAULT") {
                        "NULL".to_string()
                    } else {
                        match field(fields, c).and_then(cast_of) {
                            Some(cast) => format!("CAST(({v}) AS {cast})"),
                            None => v.to_string(),
                        }
                    }
                })
                .collect();
            selects.push(format!("SELECT {}", items.join(", ")));
        }
        let mut out = Vec::new();
        for (select, defaulted) in selects.iter().zip(defaults) {
            let rows = match query(database, state, select).await? {
                Ok(rows) => rows,
                Err(e) => return Ok(Err(failure(e))),
            };
            let row = rows.rows.into_iter().next().unwrap_or_default();
            out.push(
                columns
                    .iter()
                    .zip(row)
                    .zip(defaulted)
                    .filter(|(_, d)| !d)
                    .map(|((c, v), _)| (c.clone(), v))
                    .collect(),
            );
        }
        return Ok(Ok(out));
    } else {
        source.to_string()
    };
    let rows = match query(database, state, &query_text).await? {
        Ok(rows) => rows,
        Err(e) => return Ok(Err(failure(e))),
    };
    Ok(Ok(rows.rows.into_iter().map(|row| columns.iter().cloned().zip(row).collect()).collect()))
}

/// `CREATE TRIGGER` and `DROP TRIGGER`.
pub(super) async fn handle_trigger_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<TriggerStatement, triggers::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let read_only = state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only);
    let tx = state.current_tx();
    match statement {
        TriggerStatement::Create { trigger, or_replace } => {
            if read_only {
                return fail_statement(state, stream, "25006", &read_only_message("CREATE TRIGGER")).await;
            }
            if !database.has_table(&trigger.table) {
                return fail_statement(state, stream, "42P01", &format!("relation \"{}\" does not exist", trigger.table)).await;
            }
            if matches!(crate::core::rql::views::lookup(database, &trigger.table, tx).await, Ok(Some(_))) {
                return fail_statement(state, stream, "42809", &format!("\"{}\" is a view", trigger.table)).await;
            }
            let routine = crate::core::rql::functions::load(database, Some(&trigger.function), tx)
                .await
                .unwrap_or_default()
                .into_iter()
                .find(|r| r.arg_types.is_empty());
            let Some(routine) = routine else {
                return fail_statement(state, stream, "42883", &format!("function {}() does not exist", trigger.function)).await;
            };
            if routine.return_type != "trigger" {
                return fail_statement(
                    state,
                    stream,
                    "42P17",
                    &format!("function {} must return type trigger", trigger.function),
                )
                .await;
            }
            // Read now, so a body this cannot run is refused here rather than
            // at the first write.
            if routine.language != "plpgsql" {
                return fail_statement(state, stream, "0A000", "a trigger function must be LANGUAGE plpgsql").await;
            }
            if let Err((code, message)) = triggers::parse_plpgsql(&routine.body) {
                return fail_statement(state, stream, code, &format!("trigger function {}(): {message}", routine.name)).await;
            }
            let existing = triggers::load(database, Some(&trigger.table), tx).await.unwrap_or_default();
            let exists = existing.iter().any(|t| t.name == trigger.name);
            if exists && !or_replace {
                return fail_statement(
                    state,
                    stream,
                    "42710",
                    &format!("trigger \"{}\" for relation \"{}\" already exists", trigger.name, trigger.table),
                )
                .await;
            }
            if trigger.events.contains(&Event::Update) || trigger.events.contains(&Event::Delete) {
                if trigger.row_level && database.schema_view(&trigger.table).is_some_and(|v| key_columns(v.schema()).is_none()) {
                    return fail_statement(
                        state,
                        stream,
                        "0A000",
                        &format!(
                            "table \"{}\" needs a primary key for an UPDATE or DELETE row trigger to address each row",
                            trigger.table
                        ),
                    )
                    .await;
                }
            }
            if let Err(e) = triggers::store(database, &trigger, exists, tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            send_command_complete(stream, "CREATE TRIGGER").await
        }
        TriggerStatement::Drop { name, table, if_exists } => {
            if read_only {
                return fail_statement(state, stream, "25006", &read_only_message("DROP TRIGGER")).await;
            }
            let found = triggers::load(database, Some(&table), tx).await.unwrap_or_default();
            match found.iter().find(|t| t.name == name) {
                Some(trigger) => {
                    if let Err(e) = triggers::remove(database, trigger, tx).await {
                        return fail_statement(state, stream, sqlstate_for(&e), &e).await;
                    }
                }
                None if if_exists => {
                    crate::core::notice::push(
                        "NOTICE",
                        "00000",
                        format!("trigger \"{name}\" for relation \"{table}\" does not exist, skipping"),
                    );
                }
                None if !database.has_table(&table) => {
                    return fail_statement(state, stream, "42P01", &format!("relation \"{table}\" does not exist")).await;
                }
                None => {
                    return fail_statement(
                        state,
                        stream,
                        "42704",
                        &format!("trigger \"{name}\" for table \"{table}\" does not exist"),
                    )
                    .await;
                }
            }
            send_command_complete(stream, "DROP TRIGGER").await
        }
    }
}

/// Table DDL that bears on triggers: a dropped table's triggers go with it,
/// a renamed table's follow it.
pub(super) async fn after_table_ddl(database: &Arc<Database>, tx: Option<TxId>, ddl: &crate::core::rql::views::TableDdl) {
    use crate::core::rql::views::TableDdl;
    // A renamed table keeps its owner and grants.
    if let TableDdl::Alter { name, rename_to: Some(to) } = ddl
        && database.has_table(to)
        && !database.has_table(name)
    {
        let _ = crate::core::rql::privileges::rename(database, name, to, tx).await;
    }
    if !database.has_table(triggers::TRIGGER_TABLE) {
        return;
    }
    match ddl {
        TableDdl::Drop { names, .. } => {
            for name in names {
                if !database.has_table(name) {
                    let _ = triggers::remove_table(database, name, tx).await;
                }
            }
        }
        TableDdl::Alter { name, rename_to: Some(to) } if database.has_table(to) && !database.has_table(name) => {
            for mut trigger in triggers::load(database, Some(name), tx).await.unwrap_or_default() {
                let _ = triggers::remove(database, &trigger, tx).await;
                trigger.table = to.clone();
                let _ = triggers::store(database, &trigger, false, tx).await;
            }
        }
        _ => {}
    }
}
