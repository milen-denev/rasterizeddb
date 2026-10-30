use log::warn;
use smallvec::SmallVec;

use crate::{
    core::{
        db_type::DbType,
        processor::transformer::ComparerOperation,
        row::{
            common::simd_compare_strings,
            row::{ColumnFetchingData, ColumnWritePayload, RowFetch},
            schema::{SchemaCalculator, SchemaField},
        },
        rql::lexer_s1::QueryPurpose,
    },
    memory_pool::{MEMORY_POOL, MemoryBlock},
};

pub struct UpdateRow {
    pub table_name: String,
    pub where_clause: String,
    pub query_row_fetch: RowFetch,
    pub updates: SmallVec<[ColumnWritePayload; 32]>,
    /// Assignments whose value depends on the row — `COALESCE(c, x)`,
    /// `CASE WHEN …`, a cast. Evaluated per row by the write path, since one
    /// value cannot stand for all of them.
    pub expressions: SmallVec<[(u32, crate::core::rql::expr::Expr); 8]>,
}

fn extract_where_columns(
    where_clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> std::collections::HashSet<String> {
    let mut where_columns = std::collections::HashSet::new();
    if where_clause.is_empty() {
        return where_columns;
    }

    let where_bytes = where_clause.as_bytes();
    // As in `lexer_query::extract_where_columns`: an underscore is part of an
    // identifier, so `foo` must not match inside `foo_id`, and a dropped column
    // cannot be referenced at all.
    let is_word_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for field in schema.iter().filter(|f| !f.is_deleted) {
        let field_bytes = field.name.as_bytes();
        if field_bytes.len() > where_bytes.len() {
            continue;
        }
        for i in 0..=where_bytes.len() - field_bytes.len() {
            let slice = &where_bytes[i..i + field_bytes.len()];
            if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
                let start_ok = i == 0 || !is_word_byte(where_bytes[i - 1]);
                let end_ok = i + field_bytes.len() == where_bytes.len()
                    || !is_word_byte(where_bytes[i + field_bytes.len()]);
                if start_ok && end_ok {
                    where_columns.insert(field.name.clone());
                    break;
                }
            }
        }
    }

    where_columns
}

#[inline(always)]
fn strip_surrounding_quotes(s: &str) -> &str {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        if s.len() >= 2 {
            return &s[1..s.len() - 1];
        }
    }
    s
}

/// Parses a scalar, reporting a bad literal instead of substituting a zero.
#[inline]
fn parse_scalar<T: std::str::FromStr>(value_str: &str, db_type: &DbType) -> Option<T> {
    match strip_surrounding_quotes(value_str).parse::<T>() {
        Ok(value) => Some(value),
        Err(_) => {
            warn!(
                "{:?} column cannot take the literal {:?}",
                db_type, value_str
            );
            None
        }
    }
}

#[inline(always)]
fn parse_postgres_bool(s: &str) -> Option<u8> {
    let lower = s.trim().to_ascii_lowercase();
    match lower.as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(1),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(0),
        _ => None,
    }
}

/// Encodes one SQL literal for `db_type`.
///
/// `None` means the literal is not a value of that type. It used to mean a
/// zero: every numeric arm ended in `unwrap_or_default()`, so
/// `UPDATE t SET n = 'abc'` on an integer column stored `0` and reported
/// success. Shares its contract with the `INSERT` encoder so the two cannot
/// drift apart on what they accept.
/// Offset of the parenthesis of a top-level function call, when the value is
/// one.
///
/// `None` for a plain literal, and for a parenthesised expression that does not
/// begin with a name.
fn top_level_call(value: &str) -> Option<usize> {
    let value = value.trim();
    if value.starts_with('\'') || value.starts_with('"') {
        return None;
    }
    let open = value.find('(')?;
    let name = value[..open].trim();
    if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some(open)
}

/// The scale a column was declared with, if it named one.
fn declared_scale(
    column: &str,
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
) -> Option<u32> {
    modifiers
        .iter()
        .find(|m| m.column.eq_ignore_ascii_case(column) && !m.is_deleted)
        .map(|m| m.scale as u32)
}

/// Encodes one `SET` value, honouring the column's declared scale.
///
/// `value_to_mb` yields an empty block for a literal it cannot parse, and the
/// caller pads a fixed-width column with zeroes — so `UPDATE t SET wide =
/// 9.8765432109876543` on a `numeric(38,16)` reported success and stored `0`.
/// A DECIMAL that fails to parse is named here instead.
///
/// The places past the scale round half away from zero and the declared
/// precision is enforced, as PostgreSQL's assignment does; `SET p = 2.345` on
/// a `NUMERIC(12,2)` stored 2.34, and any number of integer digits fitted.
fn encode_assignment(
    value_str: &str,
    field: &SchemaField,
    scale: Option<u32>,
    precision: Option<u32>,
) -> Result<MemoryBlock, String> {
    let trimmed = value_str.trim();
    if let (DbType::DECIMAL, Some(scale)) = (&field.db_type, scale) {
        if !trimmed.eq_ignore_ascii_case("NULL") {
            let text = trimmed.trim_matches('\'').trim_matches('"');
            let value =
                crate::core::value_codec::parse_decimal_rounded(text, scale).ok_or_else(|| {
                    format!(
                        "{} for column \"{}\"",
                        crate::core::value_codec::refusal_message(&DbType::DECIMAL, text),
                        field.name
                    )
                })?;
            crate::core::value_codec::check_numeric_precision(value, precision.unwrap_or(0), scale)?;
            let bytes = value.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            return Ok(mb);
        }
    }

    // PostgreSQL's reason, from which the wire layer derives its SQLSTATE.
    let encoded = value_to_mb(trimmed, &field.db_type).ok_or_else(|| {
        format!(
            "{} for column \"{}\"",
            crate::core::value_codec::refusal_message(&field.db_type, trimmed),
            field.name
        )
    })?;

    if field.db_type == DbType::DECIMAL
        && encoded.into_slice().is_empty()
        && !trimmed.eq_ignore_ascii_case("NULL")
    {
        return Err(format!(
            "'{}' is not a number that column '{}' can hold",
            trimmed.trim_matches('\'').trim_matches('"'),
            field.name
        ));
    }
    Ok(encoded)
}

fn value_to_mb(value_str: &str, db_type: &DbType) -> Option<MemoryBlock> {
    let value_str = value_str.trim();

    // "NULL" becomes empty.
    if value_str.eq_ignore_ascii_case("NULL") {
        return Some(MemoryBlock::default());
    }

    Some(match db_type {
        // `byteain`, from the literal as written: its quotes are undone once
        // (an `E'…'` string's escapes included), and input PostgreSQL refuses
        // fails the statement. Through the shared arm below, the quotes were
        // undone twice and a refused value was stored as zero bytes.
        DbType::BYTEA => {
            let bytes = match crate::core::value_codec::encode_bytea_sql(value_str) {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Refused BYTEA value {:?}: {}", value_str, e);
                    return None;
                }
            };
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        // Every type whose literal is text with a syntax of its own. Listed
        // alongside `BYTEA` and `ARRAY` rather than given an arm each, because
        // they all reach the same encoder — see `lexer_ir`'s matching arm.
        DbType::ARRAY
        | DbType::INTERVAL
        | DbType::MACADDR
        | DbType::MACADDR8
        | DbType::BIT
        | DbType::VARBIT
        | DbType::DATE
        | DbType::TIME
        | DbType::TIMETZ
        | DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE
        | DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => {
            // Doubled quotes are collapsed here, exactly as they are for
            // `STRING` below and in the `INSERT` path: a quote inside the
            // literal is written doubled, and `encode_literal` only strips the
            // outer pair. Without this an `UPDATE` stored `{"has ''quote''"}`
            // where the value was `{"has 'quote'"}`, reporting success.
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            match crate::core::value_codec::encode_literal(db_type, &stripped) {
                Some(bytes) => {
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                }
                // Refused, not stored empty — see the matching arm in `lexer_ir`.
                None => return None,
            }
        }
        // See the same arm in `lexer_ir`: a document is validated and encoded
        // here, because this is the last point that can still refuse it.
        DbType::JSONB => {
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            let encoded = crate::core::jsonb::encode_text(&stripped).ok()?;
            let mut mb = MEMORY_POOL.acquire(encoded.len());
            mb.into_slice_mut().copy_from_slice(&encoded);
            mb
        }
        DbType::STRING | DbType::CHAR | DbType::UNKNOWN | DbType::START | DbType::END => {
            // Doubled quotes are collapsed here too, or an UPDATE would
            // corrupt what an INSERT stored correctly.
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            let bytes = stripped.as_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(bytes);
            mb
        }
        DbType::I8 => {
            let v: i8 = parse_scalar::<i8>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I16 => {
            let v: i16 = parse_scalar::<i16>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I32 => {
            let v: i32 = parse_scalar::<i32>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I64 => {
            let v: i64 = parse_scalar::<i64>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I128 => {
            let v: i128 = parse_scalar::<i128>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U8 => {
            let v: u8 = parse_scalar::<u8>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U16 => {
            let v: u16 = parse_scalar::<u16>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U32 => {
            let v: u32 = parse_scalar::<u32>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U64 => {
            let v: u64 = parse_scalar::<u64>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U128 => {
            let v: u128 = parse_scalar::<u128>(value_str, db_type)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F32 => {
            // A float's range is PostgreSQL's: `'1e400'` is out of range, not infinity.
            let v: f32 = crate::core::value_codec::parse_float4(strip_surrounding_quotes(value_str))?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F64 => {
            // A float's range is PostgreSQL's: `'1e400'` is out of range, not infinity.
            let v: f64 = crate::core::value_codec::parse_float8(strip_surrounding_quotes(value_str))?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::BOOL => {
            let stripped = strip_surrounding_quotes(value_str);
            let v = parse_postgres_bool(stripped)?;
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        // Timestamps, UUIDs and addresses all arrive as quoted text and share
        // one encoder, so `UPDATE` and `INSERT` cannot drift apart on what they
        // accept. An unparsable literal yields an empty block rather than a
        // plausible-looking zero value.
        DbType::DATETIME | DbType::UUID | DbType::INET | DbType::DECIMAL => {
            match crate::core::value_codec::encode_literal(db_type, value_str) {
                Some(bytes) => {
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                }
                // Refused, not stored empty — see the matching arm in `lexer_ir`.
                None => return None,
            }
        }
        // Reporting-only: a JSON column's value is written as the UUID it
        // stores, through the arm above, never as this.
        DbType::JSON | DbType::BPCHAR(_) | DbType::NULL => MemoryBlock::default(),
    })
}

/// The `(column, literal)` pairs an `UPDATE` assigns, for constraint checks
/// that need the written values before the row is modified.
pub fn assignments_of_update(sql: &str) -> Option<Vec<(String, String)>> {
    let sql = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((head, _)) => head,
        None => sql,
    };
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("UPDATE") {
        return None;
    }

    let set_at = crate::core::rql::join::find_keyword(sql, "SET", 0)? + "SET".len();
    let end = crate::core::rql::join::find_keyword(sql, "WHERE", set_at).unwrap_or(sql.len());

    let mut out = Vec::new();
    for assignment in crate::core::rql::lexer_ct::split_definitions(&sql[set_at..end]) {
        let mut parts = assignment.splitn(2, '=');
        let column = parts.next()?.trim().trim_matches('"').to_string();
        let value = parts.next()?.trim().to_string();
        if !column.is_empty() {
            out.push((column, value));
        }
    }

    Some(out)
}

/// Attempts to parse a QueryPurpose::UpdateRow variant into an UpdateRow plan.
///
/// Supported form (simple):
/// `UPDATE table_name SET col1 = val1, col2 = val2 WHERE <expr>`
pub fn update_row_from_query_purpose(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Option<UpdateRow> {
    update_row_with_reason(qp, schema, &[]).0
}

/// Parses an `UPDATE`, also returning why it was rejected.
///
/// Without the reason an unsupported `WHERE` clause or `SET` expression reached
/// the client as "Cannot parse UPDATE query", which names nothing. See
/// `delete_row_with_reason`.
pub fn update_row_with_reason(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
) -> (Option<UpdateRow>, Option<String>) {
    let mut rejection: Option<String> = None;
    let parsed = parse_update_row(qp, schema, modifiers, &mut rejection);
    (parsed, rejection)
}

fn parse_update_row(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
    rejection: &mut Option<String>,
) -> Option<UpdateRow> {
    let sql = match qp {
        QueryPurpose::UpdateRow(sql) => sql.trim(),
        _ => return None,
    };

    // `RETURNING` is answered by the executor from the rows that matched, so it
    // is split off before the SET and WHERE clauses are read — otherwise its
    // column list would be parsed as part of the last predicate.
    let sql = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((head, _)) => head,
        None => sql,
    };

    let sql_upper = sql.to_ascii_uppercase();
    if !sql_upper.starts_with("UPDATE") {
        return None;
    }

    // Matched as whole words against any whitespace, not the literal `" SET "`
    // / `" WHERE "`. Every ORM formats a statement across lines, and
    // `UPDATE t\nSET c = 1\nWHERE …` failed to parse at all — reported as
    // "Cannot parse UPDATE query", naming nothing, while the identical
    // single-line statement worked.
    let set_pos = crate::core::rql::join::find_keyword(sql, "SET", 0)?;
    let before_set = sql[..set_pos].trim();
    let after_set = sql[set_pos + "SET".len()..].trim();

    // Table name is after UPDATE
    let mut parts = before_set.split_whitespace();
    parts.next()?; // UPDATE
    let table_name = parts.next()?.trim_matches('"');
    if table_name.is_empty() {
        return None;
    }

    // Split optional WHERE
    let where_pos = crate::core::rql::join::find_keyword(after_set, "WHERE", 0);
    let (set_clause, where_clause) = if let Some(pos) = where_pos {
        (
            after_set[..pos].trim(),
            after_set[pos + "WHERE".len()..].trim(),
        )
    } else {
        (after_set.trim(), "")
    };

    // Every qualifier names the target or its alias; one naming no relation
    // was dropped, so `SET v = 5 WHERE nosuch.id = 2` updated t's row 2.
    {
        let words: Vec<&str> = before_set.split_whitespace().collect();
        let mut allowed = crate::core::sql_scan::relation_qualifiers(table_name, None);
        match words.as_slice() {
            [_, _] => {}
            [_, _, alias] | [_, _, _, alias] if words.len() == 3 || words[2].eq_ignore_ascii_case("AS") => {
                // Aliased, the table is known only by the alias.
                allowed = crate::core::sql_scan::relation_qualifiers(table_name, Some(alias));
            }
            _ => {
                rejection.replace(format!("syntax error at or near \"{}\"", words.get(3).or(words.get(2)).unwrap_or(&"SET")));
                return None;
            }
        }
        let allowed: Vec<&str> = allowed.iter().map(String::as_str).collect();
        for text in [set_clause, where_clause] {
            if let Some(q) = crate::core::sql_scan::unknown_qualifier(text, &allowed) {
                rejection.replace(crate::core::sql_scan::missing_from_entry(&q));
                return None;
            }
        }
    }

    if set_clause.is_empty() {
        return None;
    }

    // Parse assignments
    let mut updates: SmallVec<[ColumnWritePayload; 32]> = SmallVec::new();
    let mut expressions: SmallVec<[(u32, crate::core::rql::expr::Expr); 8]> = SmallVec::new();
    // Split on top-level commas only: a comma inside `COALESCE(a, b)` or inside
    // a quoted string is data, not a separator between assignments.
    if set_clause.trim_end().ends_with(',') {
        rejection.replace("syntax error at or near \",\": an assignment is missing".to_string());
        return None;
    }
    for assignment in crate::core::rql::lexer_ct::split_definitions(set_clause) {
        let assignment = assignment.trim();
        if assignment.is_empty() {
            // `SET v = 0, WHERE …`: an empty assignment is a syntax error,
            // not one fewer.
            rejection.replace("syntax error at or near \",\": an assignment is missing".to_string());
            return None;
        }

        let mut kv = assignment.splitn(2, '=');
        let key = kv.next()?.trim().trim_matches('"');
        let value = kv.next()?.trim();
        if key.is_empty() {
            return None;
        }

        // A companion column belongs to the engine; a statement cannot assign
        // to it directly, and doing so would desynchronise it from its column.
        if crate::core::rql::nullable::is_hidden(key) {
            return None;
        }

        // Named rather than dropped silently: an assignment to a column that
        // does not exist — or to one that was dropped, which keeps its slot in
        // the row layout and so used to be found here — came back as the
        // unhelpful "Cannot parse UPDATE query".
        let Some(field) = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(key) && !f.is_deleted)
        else {
            rejection.replace(format!("column '{}' does not exist", key));
            return None;
        };
        let assigns_null = value.eq_ignore_ascii_case("NULL");

        // An expression whose value depends on the row is carried to the write
        // path rather than folded here. It must never fall through to
        // `value_to_mb`, which stores the text it was given: `SET t =
        // COALESCE(t, 'z')` wrote the literal characters "COALESCE(t, 'z')"
        // into the column and reported success.
        // A bare name that matches a column is an expression too: `SET a = b`
        // copies b's value per row. Without this it reached `value_to_mb`,
        // which parsed the *name* as a value — failing outright on a numeric
        // column, and storing the literal text "b" on a text one.
        let assigns_column = {
            let bare = value.trim().trim_matches('"');
            !bare.is_empty()
                && bare
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                && !bare.eq_ignore_ascii_case("NULL")
                && !bare.eq_ignore_ascii_case("TRUE")
                && !bare.eq_ignore_ascii_case("FALSE")
                && bare
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_alphabetic() || c == '_')
                && schema.iter().any(|f| {
                    f.name
                        .eq_ignore_ascii_case(bare.rsplit('.').next().unwrap_or(bare))
                })
        };

        // `needs_evaluation` recognises calls and casts; a bare `hits + 5` has
        // neither, so `SET hits = hits + 5` was taken for a literal and refused
        // with "I64 column cannot take the literal \"hits + 5\"". Every
        // endpoint-counter upsert in the application is that shape.
        if crate::core::rql::expr::needs_evaluation(value)
            || crate::core::rql::lexer_query::looks_like_row_expression(value)
            || assigns_column
        {
            match crate::core::rql::expr::parse(value, schema) {
                Ok(parsed) => {
                    expressions.push((field.write_order as u32, parsed));
                    // The column keeps its current bytes until the row is seen;
                    // the placeholder holds its slot in the payload list.
                    updates.push(ColumnWritePayload {
                        data: MemoryBlock::default(),
                        write_order: field.write_order as u32,
                        column_type: field.db_type.physical(),
                        size: field.size,
                    });
                    continue;
                }
                Err(reason) => {
                    rejection.replace(reason);
                    return None;
                }
            }
        }

        let data = if let Some(open) = top_level_call(value) {
            let name = value[..open].trim();
            match crate::core::rql::aggregate::ScalarFunc::parse(name) {
                Some((func, _)) => {
                    let close = value.rfind(')')?;
                    if !value[open + 1..close].trim().is_empty() {
                        return None;
                    }
                    let bytes = func.evaluate();
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                }
                // Refused rather than stored as text, and named so the client
                // learns which expression was the problem.
                None => {
                    rejection.replace(format!(
                        "the expression '{}' is not supported in a SET clause yet; refusing \
						 rather than storing it as text",
                        value
                    ));
                    return None;
                }
            }
        } else {
            match encode_assignment(
                value,
                field,
                declared_scale(&field.name, modifiers),
                crate::core::rql::type_modifier::precision_of(&field.name, modifiers),
            ) {
                // Held to a declared text length as an INSERT holds it
                // (`type_modifier::assign_bytes`): 22001 past it, and a
                // `character(n)` value without its trailing spaces.
                Ok(mb) => match crate::core::rql::type_modifier::assign_bytes(field, mb.into_slice()) {
                    Ok(None) => mb,
                    Ok(Some(bytes)) => {
                        let mut held = MEMORY_POOL.acquire(bytes.len());
                        held.into_slice_mut().copy_from_slice(&bytes);
                        held
                    }
                    Err(reason) => {
                        rejection.replace(reason);
                        return None;
                    }
                },
                Err(reason) => {
                    rejection.replace(reason);
                    return None;
                }
            }
        };

        // A fixed-width column has to fill its slot; `value_to_mb` yields an
        // empty block for NULL and for anything it could not parse.
        let data = if field.db_type.is_variable_width()
            || data.into_slice().len() == field.size as usize
        {
            data
        } else {
            let mut mb = MEMORY_POOL.acquire(field.size as usize);
            let slot = mb.into_slice_mut();
            slot.fill(0);
            let src = data.into_slice();
            let n = src.len().min(slot.len());
            slot[..n].copy_from_slice(&src[..n]);
            mb
        };

        updates.push(ColumnWritePayload {
            data,
            write_order: field.write_order as u32,
            column_type: field.db_type.physical(),
            size: field.size,
        });

        // Assigning to a nullable column also moves its companion, or a row set
        // to NULL and back would keep reading as NULL.
        if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema) {
            let bytes = crate::core::rql::nullable::flag_bytes(assigns_null);
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            updates.push(ColumnWritePayload {
                data: mb,
                write_order: flag.write_order as u32,
                column_type: flag.db_type.physical(),
                size: flag.size,
            });
        }
    }

    if updates.is_empty() {
        return None;
    }

    // `UPDATE` and `DELETE` evaluate their `WHERE` with the same machinery a
    // `SELECT` does, so it needs the same desugaring — quoted and qualified
    // names, `IN`/`BETWEEN`/`LIKE`, and the guards that keep a `NULL` row from
    // satisfying a comparison. Without it an ORM's `"users"."id" = …` reaches a
    // tokenizer that panics on the quote character.
    let where_clause: String =
        match crate::core::tokenizer::predicate_rewrite::rewrite_scan_where(where_clause, schema) {
            Ok(clause) => clause,
            Err(reason) => {
                rejection.replace(reason);
                return None;
            }
        };
    let where_clause = where_clause.as_str();

    // Build query_row_fetch for WHERE clause
    let where_columns = extract_where_columns(where_clause, schema);
    let schema_calc = SchemaCalculator::default();
    let mut query_columns_fetching_data = SmallVec::new();
    for col_name in &where_columns {
        let field = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(col_name.as_str()) && !f.is_deleted)?;
        let offset = schema_calc.calculate_schema_offset(&field.name, schema);
        query_columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset.0,
            column_type: field.db_type.physical(),
            size: field.size,
            schema_id: offset.1,
        });
    }

    Some(UpdateRow {
        table_name: table_name.to_string(),
        where_clause: where_clause.to_string(),
        query_row_fetch: RowFetch {
            columns_fetching_data: query_columns_fetching_data,
        },
        updates,
        expressions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
                typmod_packed: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 12,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
                typmod_packed: None,
            },
        ]
    }

    #[test]
    fn test_update_row_basic() {
        let schema = make_schema();
        let sql = r#"UPDATE users SET name = 'Alice' WHERE id = 1"#;
        let qp = QueryPurpose::UpdateRow(sql.to_string());
        let ur = update_row_from_query_purpose(&qp, &schema).expect("should parse");
        assert_eq!(ur.table_name, "users");
        assert_eq!(ur.where_clause, "id = 1");
        assert_eq!(ur.updates.len(), 1);
        assert_eq!(ur.updates[0].write_order, 1);
    }
}
