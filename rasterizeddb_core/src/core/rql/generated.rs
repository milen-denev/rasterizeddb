//! `GENERATED ALWAYS AS (expression) STORED` columns.
//!
//! A generated column is not written by the client. Its value is computed from
//! the other columns of the same row, at insert and again at every update that
//! touches one of its inputs, and stored like an ordinary column — which is
//! what `STORED` means and why reads pay nothing for it.
//!
//! # Why this is not a `DEFAULT`
//!
//! A default and a generation expression look alike and behave differently in
//! the two ways that matter:
//!
//! * A default applies only when the column is **omitted**. A generation
//!   expression applies always, and PostgreSQL *refuses* an `INSERT` that
//!   supplies a value for the column at all.
//! * A default is evaluated with nothing in scope. A generation expression
//!   reads the row it belongs to, so it can only be evaluated once the rest of
//!   the row is known.
//!
//! Folding the two together would have meant a generated column silently
//! keeping a stale value after an update to its inputs — the failure this
//! engine is most prone to, where nothing errors and the number is simply
//! wrong. The declaration therefore gets a sidecar of its own.
//!
//! The clause was previously *parsed and discarded*: `CREATE TABLE` succeeded,
//! the column existed, and every row's value for it was NULL. `SUM(total)` over
//! such a table returned nothing and reported no error.

use std::io;

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted record occupies: the column name, the expression, and a
/// tombstone. Fixed-stride, so the file reads as an array — the same shape
/// every other sidecar here uses.
pub const GENERATED_SIZE: u64 = 129 + 513 + 1;

/// Longest generation expression that can be stored.
///
/// Wider than a default's: a default is usually a constant, where a generation
/// expression names several columns and the arithmetic between them — the one
/// in the schema this was written for is 62 characters, and a wider table's
/// would be longer still.
pub const MAX_EXPRESSION_LEN: usize = 512;

/// A column's generation expression, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedColumn {
    pub column: String,
    pub expression: String,
    pub is_deleted: bool,
}

impl GeneratedColumn {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; GENERATED_SIZE as usize];

        let filtered: String = self
            .column
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let name = filtered.as_bytes();
        let name_len = name.len().min(128);
        out[..name_len].copy_from_slice(&name[..name_len]);

        let expression = self.expression.as_bytes();
        let expression_len = expression.len().min(MAX_EXPRESSION_LEN);
        out[129..129 + expression_len].copy_from_slice(&expression[..expression_len]);

        out[129 + 513] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != GENERATED_SIZE as usize {
            return Err(format!(
                "invalid generated-column record: expected {} bytes, got {}",
                GENERATED_SIZE,
                raw.len()
            ));
        }

        let read = |at: usize, len: usize| -> Result<String, String> {
            let end = raw[at..at + len]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(len - 1);
            std::str::from_utf8(&raw[at..at + end])
                .map(str::to_string)
                .map_err(|e| format!("invalid UTF-8 in generated-column record: {}", e))
        };

        Ok(GeneratedColumn {
            column: read(0, 129)?,
            expression: read(129, 513)?,
            is_deleted: raw[129 + 513] != 0,
        })
    }
}

fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "GEN.db")
}

/// Reads a table's generated columns.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<GeneratedColumn> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<GeneratedColumn> = Vec::new();
    if len < GENERATED_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; GENERATED_SIZE as usize];
    for i in 0..(len / GENERATED_SIZE) {
        let mut position = i * GENERATED_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        match GeneratedColumn::from_vec(&buffer) {
            // The newest record for a column wins, so a redefinition is an
            // append rather than a rewrite.
            Ok(entry) => {
                out.retain(|e| !e.column.eq_ignore_ascii_case(&entry.column));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable generated-column record: {}", e),
        }
    }

    out
}

/// Appends a record, superseding any earlier one for the same column.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, entry: &GeneratedColumn) -> io::Result<()> {
    if entry.expression.len() > MAX_EXPRESSION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a generation expression may be at most {} characters",
                MAX_EXPRESSION_LEN
            ),
        ));
    }
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&entry.into_vec(), true).await;
    Ok(())
}

/// The generation expression declared for `column`, if any.
pub fn expression_for<'a>(column: &str, generated: &'a [GeneratedColumn]) -> Option<&'a str> {
    generated
        .iter()
        .find(|g| g.column.eq_ignore_ascii_case(column) && !g.is_deleted)
        .map(|g| g.expression.as_str())
}

/// The literal an input column will actually hold once stored, for a
/// generation expression to read.
///
/// PostgreSQL computes a generated column after the row's other values have
/// been coerced to their columns' types, so the expression sees what is
/// stored. This read the literal the client wrote: `p NUMERIC(12,2)` given
/// 20.169 stored 20.17 while `g = p * 15` was computed from 20.169 — 302.54,
/// not 302.55 — and `WHERE g <> p * 15` found the row.
///
/// Only numbers are brought to their column here, a NUMERIC to its declared
/// scale (rounded) and an integer to a whole number (rounded); every other
/// type is stored as written, so its literal already is the stored value.
/// Text that does not evaluate is left for the insert to refuse in its own
/// words.
pub fn as_stored(literal: &str, field: &crate::core::row::schema::SchemaField) -> String {
    use crate::core::db_type::DbType;
    use crate::core::rql::expr::{self, Value};
    let numeric = matches!(
        field.db_type,
        DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::U8
            | DbType::U16
            | DbType::U32
            | DbType::U64
            | DbType::DECIMAL
    );
    let text = literal.trim();
    if !numeric || text.eq_ignore_ascii_case("NULL") {
        return literal.to_string();
    }
    let unquoted = text.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')).unwrap_or(text);
    let empty = crate::core::row::row::Row::default();
    let Ok(Value::Data { bytes, db_type }) = expr::parse(unquoted, &[])
        .and_then(|e| expr::eval(&e, &empty, &[]))
        .and_then(|v| expr::from_engine_scale(v, field))
    else {
        return literal.to_string();
    };
    match db_type {
        // `from_engine_scale` leaves it at the column's own scale, which the
        // engine-scale renderer would read a power of ten wrong.
        DbType::DECIMAL => match <[u8; 16]>::try_from(bytes.as_slice()) {
            Ok(raw) => crate::core::value_codec::format_decimal_scaled(
                i128::from_le_bytes(raw),
                field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
            ),
            Err(_) => literal.to_string(),
        },
        other => crate::core::rql::subquery::value_to_literal(&other, &bytes)
            .unwrap_or_else(|| literal.to_string()),
    }
}

/// Reads a `GENERATED ALWAYS AS (…) STORED` clause out of a column definition,
/// returning the definition with the clause removed and the expression.
///
/// `GENERATED … AS IDENTITY` is a different construct — a sequence, not a
/// computed value — and is left in place for the identity handling rather than
/// mistaken for this one.
pub fn split_clause(definition: &str) -> (String, Option<String>) {
    let upper = definition.to_ascii_uppercase();
    let Some(at) = find_keyword(&upper, "GENERATED") else {
        return (definition.to_string(), None);
    };
    let tail = &definition[at..];
    let tail_upper = &upper[at..];
    if tail_upper.contains("IDENTITY") {
        return (definition.to_string(), None);
    }
    // The expression is the parenthesised group after `AS`.
    let Some(open) = tail.find('(') else {
        return (definition.to_string(), None);
    };
    let mut depth = 0usize;
    let mut close = None;
    for (offset, c) in tail[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close) = close else {
        return (definition.to_string(), None);
    };

    let expression = tail[open + 1..close].trim().to_string();
    // Everything through the closing parenthesis goes, and so does a trailing
    // `STORED` or `VIRTUAL`.
    let mut rest = tail[close + 1..].trim_start();
    for word in ["STORED", "VIRTUAL"] {
        if rest.len() >= word.len() && rest[..word.len()].eq_ignore_ascii_case(word) {
            rest = rest[word.len()..].trim_start();
            break;
        }
    }
    let remainder = format!("{} {}", definition[..at].trim_end(), rest);
    (remainder.trim().to_string(), Some(expression))
}

/// Position of `keyword` as a whole word.
fn find_keyword(upper: &str, keyword: &str) -> Option<usize> {
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
    }
    let bytes = upper.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = upper[from..].find(keyword) {
        let at = from + offset;
        let before_ok = at == 0 || !is_word_byte(bytes[at - 1]);
        let after = at + keyword.len();
        let after_ok = after >= bytes.len() || !is_word_byte(bytes[after]);
        if before_ok && after_ok {
            return Some(at);
        }
        from = at + keyword.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips_through_its_bytes() {
        let entry = GeneratedColumn {
            column: "total_amount".to_string(),
            expression: "subtotal + tax_amount + shipping_fee - discount_amount".to_string(),
            is_deleted: false,
        };
        assert_eq!(GeneratedColumn::from_vec(&entry.into_vec()).unwrap(), entry);
    }

    #[test]
    fn the_clause_is_split_off_and_the_rest_survives() {
        let (rest, expression) = split_clause(
            "total_amount NUMERIC(12, 4) GENERATED ALWAYS AS (subtotal + tax_amount) STORED",
        );
        assert_eq!(rest, "total_amount NUMERIC(12, 4)");
        assert_eq!(expression.as_deref(), Some("subtotal + tax_amount"));
    }

    #[test]
    fn a_constraint_after_the_clause_is_kept() {
        let (rest, expression) =
            split_clause("t NUMERIC GENERATED ALWAYS AS (a * b) STORED NOT NULL");
        assert_eq!(rest, "t NUMERIC NOT NULL");
        assert_eq!(expression.as_deref(), Some("a * b"));
    }

    #[test]
    fn nested_parentheses_do_not_end_the_expression_early() {
        let (_, expression) =
            split_clause("t NUMERIC GENERATED ALWAYS AS ((a + b) * (c - d)) STORED");
        assert_eq!(expression.as_deref(), Some("(a + b) * (c - d)"));
    }

    #[test]
    fn an_identity_column_is_left_alone() {
        // A different construct entirely — a sequence, not a computed value.
        let definition = "id INTEGER GENERATED ALWAYS AS IDENTITY";
        let (rest, expression) = split_clause(definition);
        assert_eq!(rest, definition);
        assert!(expression.is_none());
    }

    #[test]
    fn a_column_named_after_the_keyword_is_not_mistaken_for_one() {
        let definition = "generated_at TIMESTAMPTZ NOT NULL";
        let (rest, expression) = split_clause(definition);
        assert_eq!(rest, definition);
        assert!(expression.is_none());
    }
}
