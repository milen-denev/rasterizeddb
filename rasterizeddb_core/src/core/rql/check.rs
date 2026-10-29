//! `CHECK` constraints.
//!
//! A check is a condition every row must satisfy. It was previously recognised
//! as a keyword and discarded — `CHECK (order_status BETWEEN 1 AND 10)` was
//! accepted at `CREATE TABLE`, and a row with `order_status = 99` was then
//! written and reported as success.
//!
//! # Where NULL goes
//!
//! PostgreSQL rejects a row only when a check evaluates to **false**. A check
//! that evaluates to NULL — because one of the columns it reads is NULL —
//! passes. That is the opposite of a `WHERE` clause, where NULL means the row
//! does not match, and it is the whole reason a `CHECK` is not simply the
//! predicate machinery pointed at one row.
//!
//! So a check whose referenced columns include a NULL is passed without being
//! evaluated, unless the check itself tests for NULL (`IS NULL`,
//! `IS NOT NULL`), in which case the test is the point and it is evaluated as
//! written. Those are the two readings that differ, and the second is rare
//! enough to be worth naming rather than guessing at.

use std::io;

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted record occupies: the constraint name, the expression,
/// and a tombstone. Fixed-stride, so the file reads as an array.
pub const CHECK_SIZE: u64 = 129 + 513 + 1;

/// Longest check expression that can be stored.
pub const MAX_EXPRESSION_LEN: usize = 512;

/// A table's check constraint, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraint {
    /// The name the constraint reports itself by. Derived from the table and
    /// column when the statement did not give one, as PostgreSQL derives it.
    pub name: String,
    pub expression: String,
    pub is_deleted: bool,
}

impl CheckConstraint {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; CHECK_SIZE as usize];

        // UTF-8, cut at a character boundary within 128 bytes: a name a
        // client gives (`"my check"`, `価格_chk`) is the name stored, so a DROP
        // by it finds it. Records written ASCII-filtered still read back as
        // written; the layout is unchanged.
        let name = self.name.as_bytes();
        let mut name_len = name.len().min(128);
        while !self.name.is_char_boundary(name_len) {
            name_len -= 1;
        }
        out[..name_len].copy_from_slice(&name[..name_len]);

        let expression = self.expression.as_bytes();
        let expression_len = expression.len().min(MAX_EXPRESSION_LEN);
        out[129..129 + expression_len].copy_from_slice(&expression[..expression_len]);

        out[129 + 513] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != CHECK_SIZE as usize {
            return Err(format!(
                "invalid check-constraint record: expected {} bytes, got {}",
                CHECK_SIZE,
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
                .map_err(|e| format!("invalid UTF-8 in check-constraint record: {}", e))
        };

        Ok(CheckConstraint {
            name: read(0, 129)?,
            expression: read(129, 513)?,
            is_deleted: raw[129 + 513] != 0,
        })
    }
}

fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "CHK.db")
}

/// Reads a table's check constraints.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<CheckConstraint> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<CheckConstraint> = Vec::new();
    if len < CHECK_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; CHECK_SIZE as usize];
    for i in 0..(len / CHECK_SIZE) {
        let mut position = i * CHECK_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        // A record of zeros is an append a power cut kept the length of and
        // dropped (`torn_tail::is_unwritten`), never a declaration.
        if crate::core::torn_tail::is_unwritten(&buffer) {
            continue;
        }
        match CheckConstraint::from_vec(&buffer) {
            Ok(entry) => {
                out.retain(|e| !e.name.eq_ignore_ascii_case(&entry.name));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable check-constraint record: {}", e),
        }
    }

    out
}

/// Appends a constraint, superseding any earlier one of the same name.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, entry: &CheckConstraint) -> io::Result<()> {
    let mut writes = crate::core::torn_tail::SidecarWrites::default();
    stage(&*schema_io, &mut writes, entry)?;
    writes.commit(&*schema_io).await
}

/// Adds `entry`'s record to `writes`, the statement's sidecar records, which
/// commit as one transaction (`torn_tail::SidecarWrites`): one log sync for
/// all of them, and after a cut all of them or none.
pub fn stage<S: StorageIO>(
    schema_io: &S,
    writes: &mut crate::core::torn_tail::SidecarWrites,
    entry: &CheckConstraint,
) -> io::Result<()> {
    if entry.expression.len() > MAX_EXPRESSION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a check expression may be at most {} characters",
                MAX_EXPRESSION_LEN
            ),
        ));
    }

    writes.add(crate::core::torn_tail::sidecar_name(schema_io, "CHK.db"), &entry.into_vec());
    Ok(())
}

/// Reads a `CHECK (…)` clause out of a definition, returning the definition
/// with the clause removed and the expression.
///
/// Handles both a column constraint (`status INT CHECK (status > 0)`) and a
/// table constraint (`CHECK (a < b)`), and the `CONSTRAINT name CHECK (…)`
/// spelling of either.
pub fn split_clause(definition: &str) -> (String, Option<String>) {
    let upper = definition.to_ascii_uppercase();
    let Some(at) = find_keyword(&upper, "CHECK") else {
        return (definition.to_string(), None);
    };
    let tail = &definition[at..];
    let Some(open) = tail.find('(') else {
        return (definition.to_string(), None);
    };
    // Nothing but whitespace may sit between the keyword and its parenthesis;
    // otherwise this is a column called `check_total` in some other construct.
    if !tail["CHECK".len()..open].trim().is_empty() {
        return (definition.to_string(), None);
    }

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
    let remainder = format!(
        "{} {}",
        definition[..at].trim_end(),
        tail[close + 1..].trim_start()
    );
    (remainder.trim().to_string(), Some(expression))
}

/// The name PostgreSQL would give an unnamed check on `table`.
pub fn derived_name(table: &str, column: Option<&str>, ordinal: usize) -> String {
    // Cut as every derived name is cut: the label survives.
    let label = crate::core::rql::unique_key::name_with_label;
    match column {
        Some(column) => label(
            &format!("{}_{}", table.to_ascii_lowercase(), column.to_ascii_lowercase()),
            "_check",
        ),
        None => label(&table.to_ascii_lowercase(), &format!("_check{ordinal}")),
    }
}

/// The name PostgreSQL gives an unnamed `ADD CHECK (…)` on `table`:
/// `<table>_<column>_check` when the expression reads exactly one of
/// `columns`, `<table>_check` when it reads none or several, and on a
/// collision (`taken`) the label numbered from 1 (`t_a_check1`, …), as
/// `ChooseConstraintName` does. A PG-written migration drops it by that
/// name; `<table>_check<n>`, which this used to give, it never finds.
/// Within the stored limit with the label whole, and bounded
/// (`unique_key::choose_name`): an error, never a spin.
pub fn unnamed_name(
    table: &str,
    expression: &str,
    columns: &[String],
    taken: impl Fn(&str) -> bool,
) -> Result<String, String> {
    let read = columns_read(expression, columns);
    let stem = match read.as_slice() {
        [one] => format!("{}_{}", table.to_ascii_lowercase(), one.to_ascii_lowercase()),
        _ => table.to_ascii_lowercase(),
    };
    crate::core::rql::unique_key::choose_name(&stem, "_check", taken)
}

/// The distinct `columns` an expression names, in the order first read:
/// identifiers outside string literals, a bare one matched without case, a
/// double-quoted one exactly.
fn columns_read(expression: &str, columns: &[String]) -> Vec<String> {
    let mut read: Vec<String> = Vec::new();
    let mut note = |found: Option<&String>| {
        if let Some(c) = found
            && !read.contains(c)
        {
            read.push(c.clone());
        }
    };
    let chars: Vec<char> = expression.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            // A string literal; `''` is a quote inside it.
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
        } else if c == '"' {
            let mut name = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '"' {
                    if chars.get(i + 1) == Some(&'"') {
                        name.push('"');
                        i += 2;
                        continue;
                    }
                    break;
                }
                name.push(chars[i]);
                i += 1;
            }
            i += 1;
            note(columns.iter().find(|col| **col == name));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            note(columns.iter().find(|col| col.eq_ignore_ascii_case(&word)));
        } else if c.is_ascii_digit() {
            // A number, `1e5` and `0x1f` included: never a column.
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.') {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    read
}

/// A check expression in the shape the predicate rewriter reads, from the
/// canonical form `pg_dump` writes and the forms ORMs generate.
///
/// `pg_dump` renders `status IN ('a', 'b')` as `((status = ANY
/// (ARRAY['a'::text, 'b'::text])))`, and for a varchar column as
/// `(((status)::text = ANY ((ARRAY['a'::character varying, …])::text[])))`;
/// a timestamp literal as `'…'::timestamp without time zone`. None of these
/// read, so a `psql -f dump.sql` restore refused those tables, and with DDL
/// outside the transaction lost them and their rows. Each is rewritten to
/// what it means:
/// - `x = ANY (ARRAY[…])` is `x IN (…)`, and `x <> ALL (ARRAY[…])` is
///   `x NOT IN (…)` (the same three-valued answer, NULL included), with an
///   array cast `(ARRAY[…])::text[]` dropped;
/// - a string literal's or a bare column's cast to `text`, `character
///   varying`, `varchar` or `bpchar` is dropped: a no-op on text;
/// - `timestamp without time zone` is `timestamp`, `timestamp with time
///   zone` is `timestamptz`, and so for `time`;
/// - the operators `~~`, `~~*`, `!~~`, `!~~*` are `LIKE`, `ILIKE`,
///   `NOT LIKE`, `NOT ILIKE`.
pub fn normalize_expression(expression: &str) -> String {
    let mut text = expression.to_string();
    for (from, to) in [
        ("::timestamp without time zone", "::timestamp"),
        ("::timestamp with time zone", "::timestamptz"),
        ("::time without time zone", "::time"),
        ("::time with time zone", "::timetz"),
    ] {
        text = replace_outside_quotes(&text, from, to);
    }
    // `pg_dump` writes LIKE as its operator: `~~`, `~~*` (ILIKE), and `!~~`,
    // `!~~*` for the negations. Longest first.
    for (from, to) in [(" !~~* ", " NOT ILIKE "), (" !~~ ", " NOT LIKE "), (" ~~* ", " ILIKE "), (" ~~ ", " LIKE ")] {
        text = replace_outside_quotes(&text, from, to);
    }
    let text = drop_text_casts(&text);
    let text = unwrap_bare_names(&text);
    let text = lower_any_all(&text);
    lower_regex_operators(&text)
}

/// `a ~ b`, `a ~* b`, `a !~ b`, `a !~* b` as `(regexp_like(a, b) = TRUE)`
/// (with flag `'i'` for the starred ones, `= FALSE` for the negations): the
/// boolean form the rewriter lifts to the evaluator. `pg_dump` writes every
/// `SIMILAR TO` check as `((x)::text ~ similar_to_escape('a%'::text))`, and
/// Django's `__regex`/`__iregex` constraints are these operators.
fn lower_regex_operators(text: &str) -> String {
    let mut out = text.to_string();
    // Bounded: each pass removes one operator.
    for _ in 0..64 {
        let bytes = out.as_bytes();
        let mut found = None;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                continue;
            }
            if bytes[i] == b'~' && bytes.get(i + 1) != Some(&b'~') && (i == 0 || bytes[i - 1] != b'~') {
                let negated = i > 0 && bytes[i - 1] == b'!';
                let start = if negated { i - 1 } else { i };
                let starred = bytes.get(i + 1) == Some(&b'*');
                let end = i + 1 + usize::from(starred);
                found = Some((start, end, negated, starred));
                break;
            }
            i += 1;
        }
        let Some((op_start, op_end, negated, starred)) = found else { break };
        let (Some(left), Some(right)) = (operand_before(&out, op_start), operand_after(&out, op_end)) else { break };
        let flags = if starred { ", 'i'" } else { "" };
        let truth = if negated { "FALSE" } else { "TRUE" };
        out = format!(
            "{}(regexp_like({}, {}{}) = {}){}",
            &out[..left],
            out[left..op_start].trim(),
            out[op_end..right].trim(),
            flags,
            truth,
            &out[right..]
        );
    }
    out
}

/// Where the operand ending just before `at` starts: a name, a string
/// literal, a parenthesised group or a call, with any `::type` cast.
fn operand_before(text: &str, at: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = at;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return None;
    }
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'"';
    match bytes[i - 1] {
        b')' => {
            // Back to the matching parenthesis, then a call's name.
            let mut depth = 0i32;
            while i > 0 {
                i -= 1;
                match bytes[i] {
                    b')' => depth += 1,
                    b'(' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            while i > 0 && word(bytes[i - 1]) {
                i -= 1;
            }
            Some(i)
        }
        b'\'' => {
            i -= 1;
            while i > 0 {
                i -= 1;
                if bytes[i] == b'\'' && !(i > 0 && bytes[i - 1] == b'\'') {
                    break;
                }
            }
            Some(i)
        }
        b if word(b) => {
            while i > 0 && (word(bytes[i - 1]) || bytes[i - 1] == b':') {
                i -= 1;
            }
            Some(i)
        }
        _ => None,
    }
}

/// Where the operand starting after `at` ends (see [`operand_before`]).
fn operand_after(text: &str, at: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = at;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'"';
    match bytes.get(i)? {
        b'\'' => i = crate::core::sql_scan::quoted_end(bytes, i, b'\''),
        b'(' => i = crate::core::sql_scan::matching_close(text, i)? + 1,
        b if word(*b) => {
            while i < bytes.len() && word(bytes[i]) {
                i += 1;
            }
            if bytes.get(i) == Some(&b'(') {
                i = crate::core::sql_scan::matching_close(text, i)? + 1;
            }
        }
        _ => return None,
    }
    // A cast on it.
    if text[i..].starts_with("::") {
        i += 2;
        while i < bytes.len() && (word(bytes[i]) || bytes[i] == b'[' || bytes[i] == b']') {
            i += 1;
        }
    }
    Some(i)
}

/// `(status)` as `status` where the parentheses hold one name and are not a
/// call's: what is left of `pg_dump`'s `(status)::text` once the cast is
/// dropped, which the rewriter does not read before `IN`.
fn unwrap_bare_names(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        if bytes[i] == b'(' {
            let called = out.trim_end().ends_with(|c: char| c.is_alphanumeric() || c == '_');
            if let Some(close) = text[i + 1..].find(')').map(|c| i + 1 + c) {
                let inner = text[i + 1..close].trim();
                let name = !inner.is_empty()
                    && (inner.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                        && inner.starts_with(|c: char| c.is_alphabetic() || c == '_')
                        || inner.len() >= 2 && inner.starts_with('"') && inner.ends_with('"') && !inner[1..inner.len() - 1].contains('"'));
                let keyword = ["NULL", "TRUE", "FALSE"].iter().any(|k| inner.eq_ignore_ascii_case(k));
                if name && !called && !keyword {
                    out.push_str(inner);
                    i = close + 1;
                    continue;
                }
            }
        }
        let ch = text[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// `haystack` with each occurrence of `from` outside string literals,
/// matched without regard to ASCII case, replaced by `to`.
fn replace_outside_quotes(haystack: &str, from: &str, to: &str) -> String {
    let bytes = haystack.as_bytes();
    let lower_from = from.to_ascii_lowercase();
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            out.push_str(&haystack[i..end]);
            i = end;
            continue;
        }
        if haystack[i..].len() >= from.len()
            && haystack.is_char_boundary(i + from.len())
            && haystack[i..i + from.len()].to_ascii_lowercase() == lower_from
        {
            out.push_str(to);
            i += from.len();
            continue;
        }
        let ch = haystack[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Drops `::text`-like casts (and an array's `::text[]`) after a string
/// literal, a bare or parenthesised column, or a parenthesised `ARRAY[…]`.
fn drop_text_casts(text: &str) -> String {
    const TEXTUAL: [&str; 5] = ["character varying", "varchar", "text", "bpchar", "character"];
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            out.push_str(&text[i..end]);
            i = end;
            continue;
        }
        if bytes[i] == b':' && bytes.get(i + 1) == Some(&b':') {
            let rest = &text[i + 2..];
            let lower = rest.to_ascii_lowercase();
            if let Some(name) = TEXTUAL.iter().find(|name| {
                lower.starts_with(*name)
                    && !lower[name.len()..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
            }) {
                // What the cast applies to must be text already: a literal,
                // a name, or a parenthesised one of those or an ARRAY.
                let before = out.trim_end();
                let castable = before.ends_with('\'')
                    || before.ends_with(')')
                    || before.ends_with(|c: char| c.is_alphanumeric() || c == '_' || c == '"');
                if castable {
                    let mut skip = i + 2 + name.len();
                    // `[]` of an array cast.
                    if text[skip..].starts_with("[]") {
                        skip += 2;
                    }
                    i = skip;
                    continue;
                }
            }
        }
        let ch = text[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// `x = ANY (ARRAY[a, b])` as `x IN (a, b)`, and `x <> ALL (ARRAY[a, b])`
/// as `x NOT IN (a, b)`; an extra pair of parentheses around the array, as
/// `pg_dump` writes with a cast, is taken too.
fn lower_any_all(text: &str) -> String {
    let mut out = text.to_string();
    for (op_kw, replacement) in [("= ANY", "IN"), ("<> ALL", "NOT IN"), ("!= ALL", "NOT IN")] {
        loop {
            let upper = out.to_ascii_uppercase();
            let Some(at) = find_outside_quotes(&upper, op_kw) else { break };
            let after = out[at + op_kw.len()..].trim_start();
            let after_at = out.len() - after.len();
            // `(ARRAY[...])` or `((ARRAY[...]))`
            let Some(open) = after.starts_with('(').then_some(after_at) else { break };
            let Some(close) = crate::core::sql_scan::matching_close(&out, open) else { break };
            let inner = out[open + 1..close].trim();
            let inner = inner
                .strip_prefix('(')
                .and_then(|i| i.strip_suffix(')'))
                .map(str::trim)
                .unwrap_or(inner);
            let upper_inner = inner.to_ascii_uppercase();
            let Some(items) = upper_inner
                .strip_prefix("ARRAY")
                .map(str::trim_start)
                .filter(|r| r.starts_with('[') && r.ends_with(']'))
                .map(|r| {
                    let start = inner.len() - r.len() + 1;
                    inner[start..inner.len() - 1].to_string()
                })
            else {
                break;
            };
            out = format!("{}{} ({}){}", &out[..at], replacement, items, &out[close + 1..]);
        }
    }
    out
}

/// Byte position of `needle` (upper case) in `upper` outside string literals.
fn find_outside_quotes(upper: &str, needle: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            continue;
        }
        if upper[i..].starts_with(needle) {
            return Some(i);
        }
        i += 1;
    }
    None
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
        let entry = CheckConstraint {
            name: "orders_status_check".to_string(),
            expression: "order_status BETWEEN 1 AND 10".to_string(),
            is_deleted: false,
        };
        assert_eq!(CheckConstraint::from_vec(&entry.into_vec()).unwrap(), entry);
    }

    #[test]
    fn a_column_check_is_split_off_and_the_rest_survives() {
        let (rest, expression) =
            split_clause("order_status SMALLINT NOT NULL CHECK (order_status BETWEEN 1 AND 10)");
        assert_eq!(rest, "order_status SMALLINT NOT NULL");
        assert_eq!(expression.as_deref(), Some("order_status BETWEEN 1 AND 10"));
    }

    #[test]
    fn a_constraint_after_the_check_is_kept() {
        let (rest, expression) = split_clause("v INT CHECK (v > 0) NOT NULL");
        assert_eq!(rest, "v INT NOT NULL");
        assert_eq!(expression.as_deref(), Some("v > 0"));
    }

    #[test]
    fn nested_parentheses_do_not_end_the_expression_early() {
        let (_, expression) = split_clause("v INT CHECK ((v > 0) AND (v < 10))");
        assert_eq!(expression.as_deref(), Some("(v > 0) AND (v < 10)"));
    }

    #[test]
    fn a_column_whose_name_contains_the_keyword_is_left_alone() {
        // `check_digit` is a column, not a constraint.
        let definition = "check_digit INTEGER NOT NULL";
        let (rest, expression) = split_clause(definition);
        assert_eq!(rest, definition);
        assert!(expression.is_none());
    }

    #[test]
    fn an_added_unnamed_check_is_named_as_postgres_names_it() {
        let columns: Vec<String> = ["a", "v", "Mixed"].iter().map(|c| c.to_string()).collect();
        let free = |_: &str| false;
        assert_eq!(unnamed_name("pl", "a > 0", &columns, free).unwrap(), "pl_a_check");
        assert_eq!(unnamed_name("pl", "a > 0 AND a < 9", &columns, free).unwrap(), "pl_a_check");
        assert_eq!(unnamed_name("pl", "a > v", &columns, free).unwrap(), "pl_check");
        assert_eq!(unnamed_name("pl", "1 = 1", &columns, free).unwrap(), "pl_check");
        // A literal spelling a column's name, a number with letters in it,
        // a function's name: none is a column.
        assert_eq!(unnamed_name("pl", "v > 1e5 OR 'a' = 'it''s a'", &columns, free).unwrap(), "pl_v_check");
        assert_eq!(unnamed_name("pl", "length(\"Mixed\") > 0", &columns, free).unwrap(), "pl_mixed_check");
        // A collision numbers the label, from 1.
        let taken = |n: &str| n == "pl_a_check" || n == "pl_a_check1";
        assert_eq!(unnamed_name("pl", "a > 0", &columns, taken).unwrap(), "pl_a_check2");
    }

    /// A stem at the stored limit keeps its label and number whole, so the
    /// names differ and the choice ends (it used to spin: the cut took the
    /// number off every candidate).
    #[test]
    fn a_long_table_names_distinct_checks_and_the_choice_ends() {
        let table = "t".repeat(125);
        let columns = vec!["a".to_string(), "c".repeat(120)];
        let mut taken: Vec<String> = Vec::new();
        for (expression, ends) in [
            ("a > 0", "_check"),
            ("a > 0", "_check1"),
            ("a > 0", "_check2"),
            (&*format!("{} > 0", "c".repeat(120)), "_check3"),
        ] {
            let name = unnamed_name(&table, expression, &columns, |n| taken.iter().any(|t| t == n)).unwrap();
            assert!(name.len() <= 128 && name.ends_with(ends), "{name}");
            assert!(!taken.contains(&name));
            taken.push(name);
        }
        // Every candidate taken: an error, not a spin.
        assert!(unnamed_name(&table, "a > 0", &columns, |_| true).is_err());
    }

    #[test]
    fn an_unnamed_check_takes_the_name_postgres_would_give_it() {
        assert_eq!(
            derived_name("orders", Some("status"), 0),
            "orders_status_check"
        );
        assert_eq!(derived_name("orders", None, 2), "orders_check2");
        // A long table's derived names keep their labels within the limit.
        let long = "t".repeat(125);
        for (name, label) in [(derived_name(&long, Some("a"), 0), "_check"), (derived_name(&long, None, 3), "_check3")] {
            assert!(name.len() <= 128 && name.ends_with(label), "{name}");
        }
    }
}
