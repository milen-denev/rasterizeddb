use log::{error, warn};
use smallvec::SmallVec;

use crate::core::row::row::{ColumnWritePayload, RowWrite};
use crate::core::row::schema::SchemaField;
use crate::core::{db_type::DbType, rql::lexer_s1::QueryPurpose};

use crate::memory_pool::{MEMORY_POOL, MemoryBlock};

/// Convert a string value to a MemoryBlock based on the database type
#[inline(always)]
/// Encodes one SQL literal for `db_type`.
///
/// `None` means the literal is not a value of that type. It used to mean a
/// zero: every numeric arm ended in `unwrap_or_default()`, so
/// `INSERT INTO t (n) VALUES ('abc')` on an integer column stored `0` and
/// reported success. Silently substituting a plausible value for one the caller
/// actually wrote is the worst available answer — worse than rejecting the
/// statement, and worse than storing nothing.
/// Encodes a literal for a column that declares a decimal scale.
///
/// A `NUMERIC(38,16)` column holds sixteen fractional digits; encoding at the
/// engine's default nine would drop the last seven and report success.
fn value_to_mb_scaled(
    value_str: &str,
    db_type: &DbType,
    scale: Option<u32>,
) -> Option<MemoryBlock> {
    let value_str = crate::core::rql::column_default::strip_redundant_cast(value_str.trim());
    if let (DbType::DECIMAL, Some(scale)) = (db_type, scale) {
        let text = value_str.trim().trim_matches('\'').trim_matches('"');
        // NULL is not a number to be parsed at a scale; it is the absence of
        // one, and `value_to_mb` below turns it into the empty block this
        // engine stores for NULL. Falling into `parse_decimal_scaled` with it
        // returned `None`, which the insert path reads as "this row could not
        // be built" — reported as "Cannot insert row due to schema mismatch",
        // naming neither the column nor the value. A `numeric(18,8)` column
        // holding NULL is ordinary, so this refused ordinary rows.
        if !text.eq_ignore_ascii_case("NULL") {
            let value = crate::core::value_codec::parse_decimal_scaled(text, scale)?;
            let bytes = value.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            return Some(mb);
        }
    }
    value_to_mb(value_str, db_type)
}

thread_local! {
    /// Why the last `insert_row_with_defaults` on this thread refused a value,
    /// in PostgreSQL's words. The function answers `None` for every failure,
    /// and its callers reported each one as a "schema mismatch" (XX000); this
    /// carries the real reason, and with it the real SQLSTATE, to the one
    /// caller that reports it. Same thread by construction: the build is
    /// synchronous and the caller reads it before its next `await`.
    static REFUSAL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Takes the reason the last insert build on this thread refused a value.
pub fn take_refusal() -> Option<String> {
    REFUSAL.with(|r| r.borrow_mut().take())
}

fn note_refusal(column: &str, db_type: &DbType, value: &str) {
    let message = format!(
        "{} for column \"{}\"",
        crate::core::value_codec::refusal_message(db_type, value),
        column
    );
    REFUSAL.with(|r| *r.borrow_mut() = Some(message));
}

fn value_to_mb(value_str: &str, db_type: &DbType) -> Option<MemoryBlock> {
    // `'x'::text` is a value, not the four tokens it looks like.
    //
    // A client pins a literal to a type this way whenever the parameter's type
    // would otherwise be ambiguous, and query builders emit it constantly.
    // PostgreSQL's parser resolves the cast and stores `x`. Here the quote
    // stripping below only fires when the text both starts *and* ends with a
    // quote, so a trailing cast left it alone and the whole token — quotes,
    // colons and type name — was stored as the value. Every later read then
    // returned `'x'::text` where the application had written `x`.
    //
    // The column already says what type the value is, so the cast says nothing
    // it does not; `strip_redundant_cast` removes only a cast applied to a
    // whole literal, and leaves anything it cannot vouch for to be parsed as
    // before.
    let value_str = crate::core::rql::column_default::strip_redundant_cast(value_str.trim());

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
        // PostgreSQL accepts many inputs for bool:
        // https://www.postgresql.org/docs/current/datatype-boolean.html
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "true" | "t" | "yes" | "y" | "on" | "1" => Some(1),
            "false" | "f" | "no" | "n" | "off" | "0" => Some(0),
            _ => None,
        }
    }

    // An unquoted NULL is the keyword; `'NULL'` is a four-character string.
    // Distinguishing them is why `parse_csv_values` keeps the quotes.
    if value_str.trim().eq_ignore_ascii_case("NULL") {
        // Fixed-width columns are padded to their slot by `fit_to_width`; a
        // STRING simply stores nothing. Neither is a true SQL NULL yet — see
        // `docs/postgres-compatibility.md` — but both beat storing the
        // four-character text "NULL".
        return Some(MemoryBlock::default());
    }

    Some(match db_type {
        DbType::I8 => {
            let val: i8 = parse_scalar::<i8>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I16 => {
            let val: i16 = parse_scalar::<i16>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I32 => {
            let val: i32 = parse_scalar::<i32>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I64 => {
            let val: i64 = parse_scalar::<i64>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I128 => {
            let val: i128 = parse_scalar::<i128>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U8 => {
            let val: u8 = parse_scalar::<u8>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U16 => {
            let val: u16 = parse_scalar::<u16>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U32 => {
            let val: u32 = parse_scalar::<u32>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U64 => {
            let val: u64 = parse_scalar::<u64>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U128 => {
            let val: u128 = parse_scalar::<u128>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F32 => {
            let val: f32 = parse_scalar::<f32>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F64 => {
            let val: f64 = parse_scalar::<f64>(value_str, db_type)?;
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::BOOL => {
            let stripped = strip_surrounding_quotes(value_str);
            // Refused, not defaulted: storing `false` for `'maybe'` was a wrong
            // value reported as success. PostgreSQL answers 22P02.
            let val_u8 = parse_postgres_bool(stripped)?;
            let bytes = val_u8.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        // Timestamps, UUIDs and addresses all arrive as quoted text and share
        // one encoder with the `UPDATE` path, so the two cannot drift apart on
        // what they accept. An unparsable literal yields an empty block rather
        // than a plausible-looking zero value.
        DbType::DATETIME | DbType::UUID | DbType::INET | DbType::DECIMAL => {
            match crate::core::value_codec::encode_literal(db_type, value_str) {
                Some(bytes) => {
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                }
                // Refused rather than stored empty: an empty block reads back
                // as a zero timestamp, a nil UUID or an empty value — a wrong
                // value reported as success.
                None => return None,
            }
        }
        DbType::STRING | DbType::CHAR => {
            // `unquote_sql` rather than `strip_surrounding_quotes`: the outer
            // pair is not the whole story, since a quote inside the literal is
            // written doubled and has to be collapsed back to one.
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            let b = stripped.as_bytes();
            let mut mb = MEMORY_POOL.acquire(b.len());
            mb.into_slice_mut().copy_from_slice(b);
            mb
        }
        // A document, stored as the bytes `core::jsonb` encodes it to. Parsed
        // here rather than anywhere later, because this is the last point at
        // which the statement can still be refused: bytes that are not a
        // document would otherwise be stored and read back as one.
        DbType::JSONB => {
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            let encoded = crate::core::jsonb::encode_text(&stripped).ok()?;
            let mut mb = MEMORY_POOL.acquire(encoded.len());
            mb.into_slice_mut().copy_from_slice(&encoded);
            mb
        }
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
        // Variable-width values with a syntax of their own: `\xdeadbeef` and
        // `{a,b,NULL}`. Both share the STRING storage mechanism.
        // Every type whose literal is text with a syntax of its own, sharing
        // one encoder so the `INSERT` and `UPDATE` paths cannot disagree about
        // what they accept.
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
            // `unquote_sql`, exactly as `STRING` above uses it, and for the
            // same reason: a quote inside the literal is written doubled and
            // has to be collapsed back to one. Merely stripping the outer pair
            // stored `{"has ''quote''"}` — an array element with two quotes
            // where the source had one. Nothing failed; the value was simply
            // no longer the value.
            let stripped = crate::core::value_codec::unquote_sql(value_str);
            match crate::core::value_codec::encode_literal(db_type, &stripped) {
                Some(bytes) => {
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                }
                // Refused rather than stored empty: an empty block reads back
                // as a zero timestamp, a nil UUID or an empty value — a wrong
                // value reported as success.
                None => return None,
            }
        }
        // Marker types carry no value. This used to panic, which made a column
        // of an unhandled type a way to kill the worker doing the insert.
        // Reporting-only: a JSON column is written as the UUID reference it
        // holds, never through this.
        DbType::JSON | DbType::UNKNOWN | DbType::NULL | DbType::START | DbType::END => {
            MemoryBlock::default()
        }
    })
}

/// Attempts to parse a QueryPurpose::InsertRow variant into a RowWrite object, using the provided schema.
/// Returns None if the QueryPurpose is not InsertRow or parsing fails.
/// Handles multi-line, whitespace, and trailing comma issues robustly.
/// The `(column, literal)` pairs an `INSERT` assigns, for constraint checks
/// that need the written values before the row is built.
///
/// Returns `None` when the statement does not have the shape this understands,
/// leaving the full parser to report why.
pub fn assignments_of_insert(sql: &str) -> Option<Vec<(String, String)>> {
    let sql = match returning_split(sql) {
        Some((head, _)) => head,
        None => sql,
    };
    let upper = sql.to_ascii_uppercase();
    if !upper.trim_start().starts_with("INSERT INTO") {
        return None;
    }

    let columns_open = sql.find('(')?;
    let columns_close = matching_paren(sql, columns_open)?;
    let columns: Vec<String> = sql[columns_open + 1..columns_close]
        .split(',')
        .map(|c| c.trim().trim_matches('"').to_string())
        .filter(|c| !c.is_empty())
        .collect();

    let values_at = find_word(&upper, "VALUES", columns_close)?;
    let values_open = sql[values_at..].find('(')? + values_at;
    let values_close = matching_paren(sql, values_open)?;
    let values = split_values(&sql[values_open + 1..values_close]);

    if columns.len() != values.len() {
        return None;
    }
    Some(columns.into_iter().zip(values).collect())
}

/// Index of the parenthesis closing the one at `open`, ignoring quoted text
/// of every kind — an escape string's `\'` or a dollar-quoted `)` included.
fn matching_paren(sql: &str, open: usize) -> Option<usize> {
    crate::core::sql_scan::matching_close(sql, open)
}

/// Rewrites `INSERT INTO t VALUES (…)` — no column list — into
/// `INSERT INTO t (a, b, …) VALUES (…)`, naming the table's visible columns in
/// declaration order. Returns `None` when the statement already has a column
/// list, or is not a `VALUES` insert.
///
/// SQL lets the list be omitted to mean "every column, in order"; every parser
/// downstream of here reads the list, and without this the first `(` after
/// the table name — the tuple itself — was taken for it, so the values were
/// looked up as column names and the statement failed as a schema mismatch.
///
/// Dropped columns keep their slot but are not columns, and a nullability
/// companion is the engine's, not the user's; neither is in the list. When
/// the tuple is shorter than the table, the list is cut to match: PostgreSQL
/// fills the trailing columns from their defaults, and naming only the leading
/// ones is how the single-row parser expresses exactly that.
pub fn supply_column_list(sql: &str, schema: &[SchemaField]) -> Option<String> {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO") {
        return None;
    }
    let values_at = find_word(&upper, "VALUES", "INSERT INTO".len())?;
    // A `(` before `VALUES` is the column list, already supplied.
    if trimmed[..values_at].contains('(') {
        return None;
    }

    let tuple_open = trimmed[values_at..].find('(')? + values_at;
    let tuple_close = matching_paren(trimmed, tuple_open)?;
    let arity = split_values(&trimmed[tuple_open + 1..tuple_close]).len();

    let mut visible: Vec<&SchemaField> = schema
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .collect();
    visible.sort_by_key(|f| f.write_order);
    let columns: Vec<String> = visible
        .iter()
        .take(arity.max(1))
        .map(|f| quote_identifier(&f.name))
        .collect();

    Some(format!(
        "{}({}) {}",
        &trimmed[..values_at],
        columns.join(", "),
        &trimmed[values_at..]
    ))
}

/// Position of `word` in `upper` at or after `from`, outside quotes and on a
/// word boundary, or `None`. The match ignores ASCII case, so `upper` may be
/// the statement as written.
fn find_word(upper: &str, word: &str, from: usize) -> Option<usize> {
    let bytes = upper.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = from;
    let mut in_string = false;
    let mut in_identifier = false;
    while i + word.len() <= bytes.len() {
        match bytes[i] {
            b'\'' if !in_identifier => in_string = !in_string,
            b'"' if !in_string => in_identifier = !in_identifier,
            // Compared as bytes: `upper[i..]` panics when `i` falls inside a
            // multi-byte character, which an unquoted identifier like `tëst`
            // puts outside every quote — it took the worker down mid-INSERT.
            //
            // Case-insensitive, so the original statement can be searched
            // without upper-casing a copy of it (`word` is upper case).
            _ if !in_string
                && !in_identifier
                && bytes
                    .get(i..i + word.len())
                    .is_some_and(|w| w.eq_ignore_ascii_case(word.as_bytes()))
                && (i == 0 || !is_word(bytes[i - 1]))
                && bytes.get(i + word.len()).map_or(true, |b| !is_word(*b)) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The name as it can appear in a column list: bare when it is a plain
/// identifier, double-quoted otherwise.
fn quote_identifier(name: &str) -> String {
    let plain = name
        .chars()
        .enumerate()
        .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
    if plain && !name.is_empty() {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Splits a VALUES list on commas at nesting depth zero; see
/// [`parse_csv_values`]. It tracked parentheses but not brackets, escape
/// strings or dollar quotes.
fn split_values(list: &str) -> Vec<String> {
    parse_csv_values(list).into_iter().map(str::to_string).collect()
}

/// Extracts the column list of a `RETURNING` clause, resolved against the schema.
///
/// `RETURNING` is how an ORM learns what a write actually stored — sea-orm's
/// `insert(...).exec()` reads the primary key back out of it and reports
/// `RecordNotInserted` when nothing comes back. Returns `None` when the
/// statement has no `RETURNING` clause, and an error when it names something
/// the table does not have.
/// Splits a statement at its `RETURNING` clause, if it has one, into the
/// statement proper and the column list.
///
/// The keyword is located outside string literals, so a value containing the
/// word "returning" is not mistaken for the clause.
/// Splits `INSERT INTO t (cols) SELECT …` into the text before the row source
/// and the query that supplies it.
///
/// `INSERT` has two row sources in SQL: a literal `VALUES` tuple list, and a
/// query. Only the first was implemented, and a statement using the second
/// reached the tuple parser, which reported "missing VALUES keyword" and
/// surfaced to the client as a schema mismatch — naming neither the real
/// problem nor the unsupported form.
///
/// Returning the two halves rather than executing anything keeps this a parse
/// concern: the caller runs the query and rebuilds the statement as a `VALUES`
/// list, so the ordinary insert path does the writing and there is no second
/// write path to keep in step.
///
/// `INSERT INTO t SELECT …` with no column list is split too, with the prefix
/// ending at the table name. That is told by the word right after the name,
/// not by where the first parenthesis is, so a parenthesis inside the query is
/// never mistaken for a column list. The caller's rewrite then names no
/// columns either, and the ordinary path supplies them from the schema as it
/// does for `INSERT INTO t VALUES (…)`.
///
/// `None` for an ordinary `VALUES` insert.
pub fn insert_select_source(sql: &str) -> Option<(&str, &str)> {
    const INSERT_INTO: &str = "INSERT INTO";
    let trimmed = sql.trim_start();
    // Byte comparisons throughout: slicing `&str` at a keyword's length panics
    // when that length falls inside a multi-byte character.
    if trimmed.len() < INSERT_INTO.len()
        || !trimmed.as_bytes()[..INSERT_INTO.len()].eq_ignore_ascii_case(INSERT_INTO.as_bytes())
    {
        return None;
    }

    // Matched as a whole word: a column list followed by a relation named
    // `selection` must not read as a query.
    let starts_word = |rest: &str, keyword: &str| {
        rest.len() >= keyword.len()
            && rest.as_bytes()[..keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
            && rest.as_bytes()
                .get(keyword.len())
                .map_or(true, |c| c.is_ascii_whitespace() || *c == b'(')
    };

    // The table name, which may be quoted or qualified, ends at whitespace or
    // at the column list's parenthesis. A query straight after it means there
    // is no column list.
    let bytes = trimmed.as_bytes();
    let mut at = INSERT_INTO.len();
    while bytes.get(at).is_some_and(|b| b.is_ascii_whitespace()) {
        at += 1;
    }
    while let Some(&b) = bytes.get(at) {
        if b == b'"' {
            at += 1;
            while bytes.get(at).is_some_and(|b| *b != b'"') {
                at += 1;
            }
            at += 1;
        } else if b.is_ascii_whitespace() || b == b'(' {
            break;
        } else {
            at += 1;
        }
    }
    let name_end = at.min(bytes.len());
    while bytes.get(at).is_some_and(|b| b.is_ascii_whitespace()) {
        at += 1;
    }
    if let Some(rest) = trimmed.get(at..) {
        if name_end > INSERT_INTO.len() && (starts_word(rest, "SELECT") || starts_word(rest, "WITH")) {
            return Some((&trimmed[..name_end], rest));
        }
    }

    let open = trimmed.find('(')?;
    // A row-source keyword before the first parenthesis means there is no
    // column list, so that parenthesis is part of the query.
    let head_upper = trimmed[..open].to_ascii_uppercase();
    if head_upper
        .split_whitespace()
        .any(|word| word == "SELECT" || word == "WITH" || word == "VALUES")
    {
        return None;
    }

    let close = matching_paren(trimmed, open)?;
    let after_columns = close + 1;
    let offset = trimmed[after_columns..].find(|c: char| !c.is_whitespace())? + after_columns;
    let rest = &trimmed[offset..];

    if !starts_word(rest, "SELECT") && !starts_word(rest, "WITH") {
        return None;
    }

    Some((&trimmed[..after_columns], rest))
}

/// A byte that can sit inside an identifier, so `RETURNING` next to one is part
/// of a longer name rather than the clause. `_` counts — `last_returning` is a
/// plausible column name — and so does `"`, since a column actually named
/// `returning` arrives quoted.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'"'
}

pub fn returning_split(sql: &str) -> Option<(&str, &str)> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        const KW: usize = "RETURNING".len();
        if i + KW <= bytes.len()
            && bytes[i..i + KW].eq_ignore_ascii_case(b"RETURNING")
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && (i + KW == bytes.len() || !is_ident_byte(bytes[i + KW]))
        {
            return Some((sql[..i].trim_end(), sql[i + KW..].trim()));
        }
        i += 1;
    }

    None
}

pub fn returning_columns(
    sql: &str,
    schema: &[SchemaField],
) -> Option<Result<Vec<SchemaField>, String>> {
    // Find the keyword outside of any string literal, so a value containing the
    // word "returning" cannot be mistaken for the clause.
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut kw_at = None;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            _ => {
                const KW: &[u8] = b"RETURNING";
                if i + KW.len() <= bytes.len()
                    && bytes[i..i + KW.len()].eq_ignore_ascii_case(KW)
                    && (i == 0 || !is_ident_byte(bytes[i - 1]))
                    && (i + KW.len() == bytes.len() || !is_ident_byte(bytes[i + KW.len()]))
                {
                    kw_at = Some(i + KW.len());
                    break;
                }
                i += 1;
            }
        }
    }

    let start = kw_at?;
    let list = sql[start..].trim().trim_end_matches(';').trim();
    if list.is_empty() {
        return Some(Err(
            "RETURNING is present but no columns were listed".to_string()
        ));
    }

    if list == "*" {
        // The same columns `SELECT *` shows, for the same reasons: a `__null_`
        // companion is bookkeeping the client has no name for, and a dropped
        // column keeps its slot rather than being removed, so both would
        // otherwise come back here after `SELECT *` had stopped returning them.
        let mut fields: Vec<SchemaField> = schema
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .cloned()
            .collect();
        fields.sort_by_key(|f| f.write_order);
        return Some(Ok(fields));
    }

    Some(parse_items(list, schema).map(|items| {
        // The compatibility view: every item as a `SchemaField`. A computed
        // item gets a synthetic one — its alias, its static type, a
        // write_order above every real field's — which is exactly what the
        // describe paths need (a name and a type) and deliberately NOT
        // fetchable: the row-building path resolves items itself.
        let mut virtual_order = schema.iter().map(|f| f.write_order).max().unwrap_or(0) + 1;
        items
            .into_iter()
            .map(|item| match item {
                ReturningItem::Column(f) => f,
                ReturningItem::Computed { alias, db_type, .. } => {
                    let field = SchemaField::new(
                        alias,
                        db_type.clone(),
                        db_type.get_size(),
                        0,
                        virtual_order,
                        false,
                    );
                    virtual_order += 1;
                    field
                }
            })
            .collect()
    }))
}

/// One item of a `RETURNING` list.
#[derive(Debug, Clone)]
pub enum ReturningItem {
    /// A stored column, echoed from the affected row.
    Column(SchemaField),
    /// An expression over the affected row — `(next_reference_number - 1) AS
    /// allocated`, the shape a counter-allocating UPDATE returns its reserved
    /// value with. Evaluated against the row as the statement leaves it.
    Computed {
        expr: crate::core::rql::expr::Expr,
        alias: String,
        db_type: DbType,
    },
}

/// The `RETURNING` list as items, or `None` when the statement has no clause.
pub fn returning_items(
    sql: &str,
    schema: &[SchemaField],
) -> Option<Result<Vec<ReturningItem>, String>> {
    let (_, list) = returning_split(sql)?;
    let list = list.trim().trim_end_matches(';').trim();
    if list.is_empty() {
        return Some(Err(
            "RETURNING is present but no columns were listed".to_string()
        ));
    }
    if list == "*" {
        let mut fields: Vec<SchemaField> = schema
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .cloned()
            .collect();
        fields.sort_by_key(|f| f.write_order);
        return Some(Ok(fields.into_iter().map(ReturningItem::Column).collect()));
    }
    Some(parse_items(list, schema))
}

fn parse_items(list: &str, schema: &[SchemaField]) -> Result<Vec<ReturningItem>, String> {
    let mut out = Vec::new();
    // Top-level commas only: `(a - 1) AS x, COALESCE(b, 0)` holds commas of
    // its own.
    for item in crate::core::rql::join::split_top_level_commas(list) {
        let item = item.trim();
        let (expr_text, alias) = crate::core::rql::lexer_query::split_alias(item);
        let expr_text = expr_text.trim();

        // A single bare name — possibly qualified or quoted, but holding no
        // operator — is a stored column. With an alias it becomes a computed
        // echo of itself, so the wire layer names it the way the client asked.
        let is_single_name = !expr_text.contains(|c: char| {
            c.is_whitespace() || matches!(c, '(' | ')' | '+' | '-' | '*' | '/' | '|' | ':' | ',')
        });
        if is_single_name {
            let name = expr_text
                .rsplit('.')
                .next()
                .unwrap_or(expr_text)
                .trim_matches('"');
            if let Some(f) = schema.iter().find(|f| {
                f.name.eq_ignore_ascii_case(name)
                    && !f.is_deleted
                    && !crate::core::rql::nullable::is_hidden(&f.name)
            }) {
                match alias {
                    Some(alias) => out.push(ReturningItem::Computed {
                        expr: crate::core::rql::expr::Expr::Column(f.clone()),
                        db_type: f.db_type.clone(),
                        alias,
                    }),
                    None => out.push(ReturningItem::Column(f.clone())),
                }
                continue;
            }
        }

        // Anything else is an expression over the affected row.
        match crate::core::rql::expr::parse(expr_text, schema) {
            Ok(expr) => {
                // The expression path must not become a way to address what
                // the column path refuses: a `__null_` companion is engine
                // bookkeeping, not data.
                if crate::core::rql::expr::columns_of(&expr)
                    .iter()
                    .any(|f| crate::core::rql::nullable::is_hidden(&f.name))
                {
                    return Err(format!("RETURNING column '{}' not found in schema", item));
                }
                let db_type = crate::core::rql::expr::static_type(&expr);
                out.push(ReturningItem::Computed {
                    expr,
                    db_type,
                    alias: alias.unwrap_or_else(|| "?column?".to_string()),
                });
            }
            Err(_) => {
                return Err(format!("RETURNING column '{}' not found in schema", item));
            }
        }
    }
    Ok(out)
}

/// Splits a `VALUES` tuple's contents into its values, each trimmed and
/// borrowed from `s`.
///
/// Quotes are kept: they are what distinguishes the keyword `NULL` from the
/// four-character string `'NULL'`, and each type's encoder strips them itself.
/// A trailing empty value is dropped (`1, ` is one value) while an empty one
/// between commas is kept, as it always was.
///
/// A comma separates values only at nesting depth zero — outside every string
/// (standard, escape, dollar-quoted), quoted identifier, `(…)` and `[…]`. It
/// used to track quotes alone, so `ARRAY[1,2]` was two values: three columns
/// given `(ARRAY[1,2], 3)` stored `ARRAY[1` and `2]` and reported success.
fn parse_csv_values(s: &str) -> Vec<&str> {
    crate::core::sql_scan::split_top_level(s, b',')
}

/// Pads or truncates a column value to the width its slot expects.
///
/// `STRING` is variable-width and is left alone; every other type occupies a
/// fixed slot, and `RowPointer::write_row` copies into it with
/// `copy_from_slice`, which panics on a length mismatch. Producing a
/// zero-filled value of the right width turns what would be a crashed worker
/// into a stored default.
fn fit_to_width(data: MemoryBlock, db_type: &DbType, width: usize, col_name: &str) -> MemoryBlock {
    if db_type.is_variable_width() || data.into_slice().len() == width {
        return data;
    }

    // An *empty* block is NULL, which is the designed representation: the slot
    // is zero-filled and the column's `__null_` companion carries the fact. It
    // is normal for every nullable column and needs no warning — it filled the
    // log on each insert that left one unset. A non-empty value of the wrong
    // width is a different matter and still worth reporting.
    if !data.into_slice().is_empty() {
        warn!(
            "Column {:?} of type {:?} produced {} bytes for a {}-byte slot; storing a zero value",
            col_name,
            db_type,
            data.into_slice().len(),
            width
        );
    }

    let mut mb = MEMORY_POOL.acquire(width);
    let slot = mb.into_slice_mut();
    slot.fill(0);
    let src = data.into_slice();
    let n = src.len().min(width);
    slot[..n].copy_from_slice(&src[..n]);
    mb
}

pub fn insert_row_from_query_purpose(
    qp: &QueryPurpose,
    schema: &[SchemaField],
) -> Option<RowWrite> {
    // No defaults, so no `nextval` can be reached and no database's sequence
    // is consulted: the id is never read.
    insert_row_with_defaults(qp, schema, &[], &[], 0)
}

/// As `insert_row_from_query_purpose`, but filling omitted columns from their
/// `DEFAULT` expressions.
/// The scale a column declared, or `None` when it declared none.
///
/// `None` rather than the default so the ordinary encode path is untouched:
/// a column with no modifier is written exactly as it was before this existed.
fn declared_scale(
    column: &str,
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
) -> Option<u32> {
    modifiers
        .iter()
        .find(|m| m.column.eq_ignore_ascii_case(column) && !m.is_deleted)
        .map(|m| m.scale as u32)
}

/// `database` is the id of the database the row is written into
/// ([`crate::core::database::Database::id`]): a `SERIAL` default draws from that
/// database's sequence, and from no other's.
pub fn insert_row_with_defaults(
    qp: &QueryPurpose,
    schema: &[SchemaField],
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
    database: u64,
) -> Option<RowWrite> {
    REFUSAL.with(|r| *r.borrow_mut() = None);
    if let QueryPurpose::InsertRow(sql) = qp {
        let sql = sql.query.as_str();
        // Trim leading whitespace and newlines
        let sql_trimmed =
            sql.trim_start_matches(|c: char| c.is_whitespace() || c == '\n' || c == '\r');
        // No upper-cased copy of the statement: it was made only to find two
        // keywords, and a wide INSERT is mostly value text. Both are matched
        // case-insensitively in place instead.
        if !sql_trimmed
            .as_bytes()
            .get(.."INSERT INTO".len())
            .is_some_and(|head| head.eq_ignore_ascii_case(b"INSERT INTO"))
        {
            error!(
                "INSERT parse failed: query does not start with INSERT INTO. prefix={:?}",
                sql_trimmed.chars().take(80).collect::<String>()
            );
            return None;
        }
        let sql = sql_trimmed;

        // --- Helper to extract parenthesis block robustly ---
        /// The first `open`…`close` group at or after `start`, ignoring
        /// delimiters inside quoted strings.
        ///
        /// The quote tracking is not a refinement: without it,
        /// `VALUES (1, 'a)b')` closed at the `)` *inside* the string, so the
        /// row was stored as `'a` — a silently truncated value from a
        /// well-formed statement, with no error anywhere.
        ///
        /// `start` is a byte offset and the walk is over bytes. It used to be
        /// `char_indices().skip(start)`, which skips `start` *characters*: every
        /// multi-byte character before the parenthesis — a table called
        /// `"tëst"`, a column called `"日本"` — moved the walk past it, so the
        /// tuple was taken for the column list or never found at all. The
        /// delimiters are ASCII, and a byte of a multi-byte UTF-8 character is
        /// never ASCII, so a byte walk cannot match inside one.
        fn extract_paren_block(
            s: &str,
            open: u8,
            close: u8,
            start: usize,
        ) -> Option<(usize, usize)> {
            let bytes = s.as_bytes();
            let mut depth = 0;
            let mut begin = None;
            let mut i = start;
            while i < bytes.len() {
                // Every kind of quoted run is stepped over whole, an escape
                // string's `\'` and a dollar-quoted body included.
                if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
                    i = end;
                    continue;
                }
                let c = bytes[i];
                if c == open {
                    if depth == 0 {
                        begin = Some(i);
                    }
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        return begin.map(|b| (b, i));
                    }
                }
                i += 1;
            }
            None
        }

        // Find columns block after "INSERT INTO table_name"; the statement
        // was just checked to start with it.
        let insert_kw = 0usize;
        // Skip table name: find first '(' after "INSERT INTO"
        let table_paren_start = match sql[insert_kw..].find('(') {
            Some(v) => v + insert_kw,
            None => {
                error!(
                    "INSERT parse failed: missing column list parentheses after INSERT INTO. prefix={:?}",
                    sql_trimmed.chars().take(200).collect::<String>()
                );
                return None;
            }
        };
        let (col_start, col_end) = match extract_paren_block(sql, b'(', b')', table_paren_start) {
            Some(v) => v,
            None => {
                error!(
                    "INSERT parse failed: could not extract column list parentheses block. prefix={:?}",
                    sql_trimmed.chars().take(200).collect::<String>()
                );
                return None;
            }
        };
        let columns_str = &sql[col_start + 1..col_end];

        // Find the VALUES keyword — after the column list, and as a word.
        // Searched from the start and as a bare substring, it matched inside
        // a table name: the shadow table for a JSON key called `values_json`
        // is named `…_values_json_…`, so the column list was taken for the
        // tuple and every column name was parsed as a value.
        // `find_word` matches case-insensitively, so the offset is already the
        // keyword's offset in `sql` — it used to be found a second time by
        // lower-casing the whole tail of the statement.
        let values_kw_real = match find_word(sql, "VALUES", col_end) {
            Some(v) => v,
            None => {
                error!(
                    "INSERT parse failed: missing VALUES keyword. prefix={:?}",
                    sql_trimmed.chars().take(200).collect::<String>()
                );
                return None;
            }
        };
        let values_paren_start = match sql[values_kw_real..].find('(') {
            Some(v) => v + values_kw_real,
            None => {
                error!(
                    "INSERT parse failed: missing VALUES(...) parentheses. prefix={:?}",
                    sql_trimmed.chars().take(220).collect::<String>()
                );
                return None;
            }
        };
        let (val_start, val_end) = match extract_paren_block(sql, b'(', b')', values_paren_start) {
            Some(v) => v,
            None => {
                error!(
                    "INSERT parse failed: could not extract VALUES parentheses block. prefix={:?}",
                    sql_trimmed.chars().take(220).collect::<String>()
                );
                return None;
            }
        };
        let values_str = &sql[val_start + 1..val_end];

        // Parse columns (simple split since column names shouldn't contain commas)
        let columns: SmallVec<[&str; 32]> = columns_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_matches('"').trim_matches('\''))
            .collect();

        // Parse values using the CSV parser that strips quotes
        let values = parse_csv_values(values_str);

        if columns.len() != values.len() {
            error!(
                "INSERT parse failed: columns/values count mismatch (columns={}, values={}). values_str_len={} columns_str_len={} sql_prefix={:?}",
                columns.len(),
                values.len(),
                values_str.len(),
                columns_str.len(),
                &sql_trimmed.chars().take(200).collect::<String>()
            );
            return None;
        }

        let mut columns_writing_data = SmallVec::<[ColumnWritePayload; 32]>::new();

        for (col_name, value) in columns.iter().zip(values.iter()) {
            // Find schema field
            // A dropped column keeps its slot in the row layout, so without the
            // tombstone check an `INSERT` naming it was accepted and the value
            // written into the dead slot — reported as success, readable by
            // nothing. PostgreSQL rejects the statement, and so does this.
            let Some(schema_field) = schema.iter().find(|f| f.name == *col_name && !f.is_deleted)
            else {
                error!(
                    "INSERT parse failed: column {:?} not found in schema (schema_fields={:?})",
                    col_name,
                    schema.iter().map(|f| f.name.as_str()).collect::<Vec<_>>()
                );
                return None;
            };
            // Convert value to MemoryBlock based on the database type.
            //
            // The row writer copies each column into a fixed-width slot, so a
            // block of the wrong width panics rather than failing the
            // statement. Any short block is therefore padded to the declared
            // width here — which is what a NULL or an unparsable literal
            // produces until real nullability lands.
            // `VALUES (…, DEFAULT, …)` means the same as leaving the column
            // out, so it is resolved through the same expression.
            let default_owned;
            let value: &str = if value.trim().eq_ignore_ascii_case("DEFAULT") {
                default_owned =
                    crate::core::rql::column_default::expression_for(col_name, defaults)
                        .and_then(|e| {
                            crate::core::rql::column_default::evaluate(
                                e,
                                &schema_field.db_type,
                                database,
                            )
                            .ok()
                        })
                        .unwrap_or_else(|| "NULL".to_string());
                &default_owned
            } else {
                value
            };

            // A function call in VALUES is evaluated, or refused. It must not
            // reach `value_to_mb`, which cannot parse it and yields an empty
            // block: `INSERT … VALUES (gen_random_uuid())` stored a zero UUID
            // and reported success.
            let value_owned;
            let value = if crate::core::rql::expr::needs_evaluation(value) {
                // There is no row yet, so only a constant-foldable expression
                // has meaning here — a nullary function or a literal.
                let expr = crate::core::rql::expr::parse(value, &[])
                    .map_err(|e| {
                        error!("INSERT parse failed: {}", e);
                    })
                    .ok()?;
                let empty = crate::core::row::row::Row {
                    position: 0,
                    length: 0,
                    columns: smallvec::SmallVec::new(),
                };
                let evaluated = crate::core::rql::expr::eval(&expr, &empty, &[])
                    .map_err(|e| {
                        error!("INSERT parse failed: {}", e);
                    })
                    .ok()?;
                value_owned = match evaluated {
                    crate::core::rql::expr::Value::Null => "NULL".to_string(),
                    crate::core::rql::expr::Value::Data { bytes, db_type } => {
                        crate::core::rql::subquery::value_to_literal(&db_type, &bytes)
                            .unwrap_or_else(|| "NULL".to_string())
                    }
                };
                &value_owned
            } else {
                value
            };

            let data = fit_to_width(
                value_to_mb_scaled(
                    value,
                    &schema_field.db_type,
                    declared_scale(&schema_field.name, modifiers),
                )
                .or_else(|| {
                    note_refusal(&schema_field.name, &schema_field.db_type, value);
                    None
                })?,
                &schema_field.db_type,
                schema_field.size as usize,
                col_name,
            );

            let payload = ColumnWritePayload {
                data,
                write_order: schema_field.write_order as u32,
                column_type: schema_field.db_type.physical(),
                size: schema_field.size as u32,
            };
            columns_writing_data.push(payload);
        }

        // Every column of the row has to be written, not just the ones the
        // statement named: the row is a fixed-width record, so a missing column
        // would shift every column after it. A column left out of an `INSERT`
        // is `NULL` if it can be, and its type's zero value otherwise.
        //
        // The nullability companions are filled in here too — they are never
        // named by the statement.
        use crate::core::rql::nullable;

        // A *dropped* column is filled too, and that is not an oversight: it
        // keeps its slot in the row layout — reclaiming the bytes would move
        // every column after it — so a row written without it is short and
        // every later column reads at the wrong offset. Skipping tombstones
        // here meant every `INSERT` after an `ALTER TABLE … DROP COLUMN`
        // produced a malformed row while reporting success: `count(*)` saw
        // them, the projection returned the wrong values or dropped the row.
        for field in schema.iter() {
            if columns_writing_data
                .iter()
                .any(|c| c.write_order as u64 == field.write_order)
            {
                continue;
            }

            let data = if field.is_deleted {
                // Nothing reads it, so the slot only has to be the right width.
                if field.db_type.is_variable_width() {
                    MemoryBlock::default()
                } else {
                    let mut mb = MEMORY_POOL.acquire(field.size as usize);
                    mb.into_slice_mut().fill(0);
                    mb
                }
            } else if nullable::is_hidden(&field.name) {
                // Set when the column it tracks was not supplied and has no
                // default, or was supplied as NULL.
                let tracked = &field.name[nullable::NULL_FLAG_PREFIX.len()..];
                let is_null = match columns.iter().position(|c| c.eq_ignore_ascii_case(tracked)) {
                    None => crate::core::rql::column_default::expression_for(tracked, defaults)
                        .map(|e| e.trim().eq_ignore_ascii_case("NULL"))
                        .unwrap_or(true),
                    Some(i) => values[i].trim().eq_ignore_ascii_case("NULL"),
                };
                let bytes = nullable::flag_bytes(is_null);
                let mut mb = MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(&bytes);
                mb
            } else if let Some(data) =
                crate::core::rql::column_default::expression_for(&field.name, defaults)
                    .and_then(|e| {
                        crate::core::rql::column_default::evaluate(e, &field.db_type, database).ok()
                    })
                    .filter(|literal| literal != "NULL")
                    .and_then(|literal| {
                        value_to_mb_scaled(
                            &literal,
                            &field.db_type,
                            declared_scale(&field.name, modifiers),
                        )
                    })
            {
                // The column was omitted but has a default, evaluated now — so
                // `DEFAULT CURRENT_TIMESTAMP` is the time of this insert rather
                // than of the declaration.
                fit_to_width(data, &field.db_type, field.size as usize, &field.name)
            } else if field.db_type.is_variable_width() {
                // Variable-width: an unsupplied string is simply empty.
                MemoryBlock::default()
            } else {
                // Fixed-width: the slot has to be filled, so it holds the
                // type's zero. This is expected rather than a fault, so it does
                // not go through `fit_to_width`'s warning path.
                let mut mb = MEMORY_POOL.acquire(field.size as usize);
                mb.into_slice_mut().fill(0);
                mb
            };

            columns_writing_data.push(ColumnWritePayload {
                data,
                write_order: field.write_order as u32,
                column_type: field.db_type.physical(),
                size: field.size,
            });
        }

        // Sort by write_order before creating the RowWrite
        columns_writing_data.sort_by_key(|col| col.write_order);
        Some(RowWrite {
            columns_writing_data,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    mod insert_select {
        use super::super::insert_select_source;

        #[test]
        fn finds_the_query_that_supplies_the_rows() {
            // The shape sea-orm emits for a data backfill, newlines included.
            let sql = "INSERT INTO context_case_model (context_id, case_model_id)\n\
                       SELECT c.id, c.case_model_id\n\
                       FROM context c\n\
                       WHERE c.case_model_id IS NOT NULL";
            let (prefix, select) =
                insert_select_source(sql).expect("should detect INSERT … SELECT");
            assert!(prefix.trim_end().ends_with(')'), "got {:?}", prefix);
            assert!(select.starts_with("SELECT c.id"), "got {:?}", select);
            assert!(select.contains("IS NOT NULL"));
        }

        #[test]
        fn an_ordinary_values_insert_is_not_a_query_source() {
            assert!(insert_select_source("INSERT INTO t (a, b) VALUES (1, 2)").is_none());
            assert!(insert_select_source("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)").is_none());
        }

        #[test]
        fn a_relation_whose_name_merely_starts_with_select_is_not_a_query() {
            // `SELECT` has to match as a whole word, or a column list followed
            // by something named `selection` reads as a row source.
            assert!(insert_select_source("INSERT INTO t (a) selection").is_none());
        }

        #[test]
        fn a_missing_column_list_ends_the_prefix_at_the_table_name() {
            // The parenthesis belongs to the query here, and the word after the
            // table name is what says so.
            let (prefix, source) =
                insert_select_source("INSERT INTO t SELECT count(*) FROM u").unwrap();
            assert_eq!(prefix, "INSERT INTO t");
            assert_eq!(source, "SELECT count(*) FROM u");
            let (prefix, source) =
                insert_select_source("INSERT INTO \"my t\"\n  WITH c AS (SELECT 1) SELECT * FROM c")
                    .unwrap();
            assert_eq!(prefix, "INSERT INTO \"my t\"");
            assert!(source.starts_with("WITH c"));
            // A relation whose name starts with the keyword is still not one.
            assert!(insert_select_source("INSERT INTO t selection").is_none());
            assert!(insert_select_source("INSERT INTO t VALUES ((SELECT 1))").is_none());
        }

        #[test]
        fn a_cte_row_source_is_recognised() {
            let sql = "INSERT INTO t (a) WITH c AS (SELECT 1) SELECT * FROM c";
            let (_, source) = insert_select_source(sql).expect("WITH is a row source too");
            assert!(source.starts_with("WITH"));
        }

        #[test]
        fn the_column_list_is_kept_whole() {
            let sql = "INSERT INTO t (a, b) SELECT x, y FROM u";
            let (prefix, _) = insert_select_source(sql).unwrap();
            assert_eq!(prefix, "INSERT INTO t (a, b)");
        }
    }

    use super::*;

    /// A JSON document is one value however many commas and braces it contains.
    #[test]
    fn assignments_keep_a_json_literal_whole() {
        let sql = r#"INSERT INTO docs (id, payload) VALUES (1, '{"name":"acme","hits":42,"tags":["a","b"]}')"#;
        let assignments = assignments_of_insert(sql).expect("should parse");
        assert_eq!(assignments.len(), 2, "{:?}", assignments);
        assert_eq!(assignments[0], ("id".to_string(), "1".to_string()));
        assert_eq!(
            assignments[1].1, r#"'{"name":"acme","hits":42,"tags":["a","b"]}'"#,
            "the document must survive as a single value"
        );
    }
    use crate::core::db_type::DbType;
    use crate::core::rql::lexer_s1::InsertRowData;

    #[test]
    fn test_insert_row_basic() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 4 + 8,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 2);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(row_write.columns_writing_data[0].size, 4);
        assert_eq!(row_write.columns_writing_data[1].size, 12);
    }

    #[test]
    fn test_insert_row_mismatched_columns() {
        let schema = vec![SchemaField {
            name: "id".to_string(),
            db_type: DbType::I32,
            size: 4,
            offset: 0,
            write_order: 0,
            is_unique: false,
            is_deleted: false,
            scale: None,
        }];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        assert!(insert_row_from_query_purpose(&qp, &schema).is_none());
    }

    #[test]
    fn test_insert_row_many_columns_full_check() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I16,
                size: 2,
                offset: 20,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "email".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 22,
                write_order: 3,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "active".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 54,
                write_order: 4,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "score".to_string(),
                db_type: DbType::F64,
                size: 8,
                offset: 55,
                write_order: 5,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "created_at".to_string(),
                db_type: DbType::I64,
                size: 8,
                offset: 63,
                write_order: 6,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "notes".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 71,
                write_order: 7,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];

        let sql = r#"INSERT INTO users (id, name, age, email, active, score, created_at, notes) VALUES (42, "Bob", 27, "bob@example.com", 1, 99.5, 1680000000, "Test user")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), schema.len());

        for (i, payload) in row_write.columns_writing_data.iter().enumerate() {
            let field = &schema[i];
            assert_eq!(
                payload.write_order, field.write_order as u32,
                "write_order mismatch at col {}",
                i
            );
            assert_eq!(
                payload.column_type, field.db_type,
                "db_type mismatch at col {}",
                i
            );
            assert_eq!(
                payload.size, field.size as u32,
                "size mismatch at col {}",
                i
            );
            // Check data bytes (match binary representation for numeric types)
            let expected_value: Vec<u8> = match field.db_type {
                DbType::I32 => 42i32.to_le_bytes().to_vec(),
                DbType::I16 => 27i16.to_le_bytes().to_vec(),
                DbType::U8 => [1u8].to_vec(),
                DbType::F64 => 99.5f64.to_le_bytes().to_vec(),
                DbType::I64 => 1680000000i64.to_le_bytes().to_vec(),
                DbType::STRING => match field.name.as_str() {
                    "name" => b"Bob".to_vec(),
                    "email" => b"bob@example.com".to_vec(),
                    "notes" => b"Test user".to_vec(),
                    _ => panic!("Unexpected STRING column name: {}", field.name),
                },
                _ => panic!("Unexpected column type: {:?}", field.db_type),
            };
            let actual_bytes = payload.data.into_slice();
            assert_eq!(
                actual_bytes,
                expected_value.as_slice(),
                "data bytes mismatch at col {}",
                i
            );
        }

        // Check that write_order is sorted
        let mut last_order = 0;
        for payload in &row_write.columns_writing_data {
            assert!(payload.write_order >= last_order, "write_order not sorted");
            last_order = payload.write_order;
        }
    }

    #[test]
    fn test_insert_row_column_order_mixed() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I16,
                size: 2,
                offset: 20,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];
        let sql = r#"INSERT INTO users (age, id, name) VALUES (30, 100, "Charlie")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 3);

        // Should be sorted by write_order: id (0), name (1), age (2)
        assert_eq!(row_write.columns_writing_data[0].write_order, 0);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            100i32.to_le_bytes()
        );

        assert_eq!(row_write.columns_writing_data[1].write_order, 1);
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[1].data.into_slice(),
            b"Charlie"
        );

        assert_eq!(row_write.columns_writing_data[2].write_order, 2);
        assert_eq!(row_write.columns_writing_data[2].column_type, DbType::I16);
        assert_eq!(
            row_write.columns_writing_data[2].data.into_slice(),
            30i16.to_le_bytes()
        );
    }

    #[test]
    fn test_insert_row_edge_cases() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "flag".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "desc".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 5,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];
        let sql = r#"INSERT INTO users (id, flag, desc) VALUES (0, 255, "Edge")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            0i32.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), [255u8]);
        assert_eq!(row_write.columns_writing_data[2].data.into_slice(), b"Edge");
    }

    #[test]
    fn test_insert_row_float_and_char() {
        let schema = vec![
            SchemaField {
                name: "score".to_string(),
                db_type: DbType::F64,
                size: 8,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "grade".to_string(),
                db_type: DbType::CHAR,
                size: 1,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];
        let sql = r#"INSERT INTO results (score, grade) VALUES (3.14, "A")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "results".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            3.14f64.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), b"A");
    }

    #[test]
    fn test_insert_row_with_whitespace_and_termination() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];
        // Leading/trailing whitespace, tabs, newlines, and semicolon termination
        let sqls = [
            "   \n\tINSERT INTO users (id, name) VALUES (123, \"Test\")\n\t  ",
            "\r\nINSERT INTO users (id, name) VALUES (456, \"User\");\n",
            "\t\n  INSERT INTO users (id, name) VALUES (789, \"Whitespace\")  ;  \n",
        ];
        let expected = [
            123i32.to_le_bytes(),
            456i32.to_le_bytes(),
            789i32.to_le_bytes(),
        ];
        let expected_names = [&b"Test"[..], &b"User"[..], &b"Whitespace"[..]];
        for ((sql, exp_id), exp_name) in sqls.iter().zip(expected.iter()).zip(expected_names.iter())
        {
            let qp = QueryPurpose::InsertRow(InsertRowData {
                query: sql.to_string(),
                table_name: "users".to_string(),
                on_conflict_do_nothing: false,
                on_conflict_update: None,
            });
            let row_write =
                insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
            assert_eq!(row_write.columns_writing_data[0].data.into_slice(), exp_id);
            assert_eq!(
                row_write.columns_writing_data[1].data.into_slice(),
                *exp_name
            );
        }
    }

    #[test]
    fn test_insert_row_bool_postgres_literals() {
        let schema = vec![
            SchemaField {
                name: "ok".to_string(),
                db_type: DbType::BOOL,
                size: 1,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "flag".to_string(),
                db_type: DbType::BOOL,
                size: 1,
                offset: 1,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];

        let sql = r#"INSERT INTO t (ok, flag) VALUES (true, 'f')"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "t".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 2);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::BOOL);
        assert_eq!(row_write.columns_writing_data[0].data.into_slice(), [1u8]);
        assert_eq!(row_write.columns_writing_data[1].column_type, DbType::BOOL);
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), [0u8]);

        let sql2 = r#"INSERT INTO t (ok, flag) VALUES (On, 0)"#;
        let qp2 = QueryPurpose::InsertRow(InsertRowData {
            query: sql2.to_string(),
            table_name: "t".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write2 =
            insert_row_from_query_purpose(&qp2, &schema).expect("Should parse successfully");
        assert_eq!(row_write2.columns_writing_data[0].data.into_slice(), [1u8]);
        assert_eq!(row_write2.columns_writing_data[1].data.into_slice(), [0u8]);
    }

    #[test]
    fn test_insert_row_employees_full_schema() {
        use crate::core::db_type::DbType;
        use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

        let schema = vec![
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
                name: "job_title".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 40,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "salary".to_string(),
                db_type: DbType::F32,
                size: 4,
                offset: 72,
                write_order: 3,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "department".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 76,
                write_order: 4,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 108,
                write_order: 5,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "manager".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 112,
                write_order: 6,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "location".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 144,
                write_order: 7,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "hire_date".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 176,
                write_order: 8,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "degree".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 184,
                write_order: 9,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "skills".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 216,
                write_order: 10,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "current_project".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 280,
                write_order: 11,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "performance_score".to_string(),
                db_type: DbType::F32,
                size: 4,
                offset: 312,
                write_order: 12,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "is_active".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 316,
                write_order: 13,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "created_at".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 317,
                write_order: 14,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "updated_at".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 325,
                write_order: 15,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "is_fired".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 333,
                write_order: 16,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ];

        let sql = r#"INSERT INTO employees (
            id, name, job_title, salary, department, age, manager, location, hire_date,
            degree, skills, current_project, performance_score, is_active,
            created_at, updated_at, is_fired
        ) VALUES (
            1, 'Alice Smith', 'Software Engineer', 120000.5, 'Engineering', 29, 'Bob Johnson', 'New York', 1680000000,
            'BSc Computer Science', 'Rust, SQL, Git', 'Migration Tool', 4.5, 1, 1680000001, 1680000002, 0
        )"#;

        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "employees".to_string(),
            on_conflict_do_nothing: false,
            on_conflict_update: None,
        });
        let row_write =
            super::insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), schema.len());

        // Check a few representative fields:
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::U64);
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            1u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[1].data.into_slice(),
            b"Alice Smith"
        );
        assert_eq!(
            row_write.columns_writing_data[2].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[2].data.into_slice(),
            b"Software Engineer"
        );
        assert_eq!(row_write.columns_writing_data[3].column_type, DbType::F32);
        assert_eq!(
            row_write.columns_writing_data[3].data.into_slice(),
            120000.5f32.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[4].data.into_slice(),
            b"Engineering"
        );
        assert_eq!(
            row_write.columns_writing_data[5].data.into_slice(),
            29i32.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[6].data.into_slice(),
            b"Bob Johnson"
        );
        assert_eq!(
            row_write.columns_writing_data[7].data.into_slice(),
            b"New York"
        );
        assert_eq!(
            row_write.columns_writing_data[8].data.into_slice(),
            1680000000u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[9].data.into_slice(),
            b"BSc Computer Science"
        );
        assert_eq!(
            row_write.columns_writing_data[10].data.into_slice(),
            b"Rust, SQL, Git"
        );
        assert_eq!(
            row_write.columns_writing_data[11].data.into_slice(),
            b"Migration Tool"
        );
        assert_eq!(
            row_write.columns_writing_data[12].data.into_slice(),
            4.5f32.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[13].data.into_slice(), [1u8]);
        assert_eq!(
            row_write.columns_writing_data[14].data.into_slice(),
            1680000001u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[15].data.into_slice(),
            1680000002u64.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[16].data.into_slice(), [0u8]);
    }
}

/// Splits a multi-tuple `INSERT … VALUES (…), (…), (…)` into one statement per
/// tuple.
///
/// Returns `None` when the statement has a single tuple and needs no splitting,
/// and `Some(Err)` when the `VALUES` clause could not be fully accounted for.
///
/// That last case is the point. The parser used to take the *first*
/// parenthesised group after `VALUES` and silently discard everything after its
/// closing paren, so a three-row insert wrote one row and reported
/// `INSERT 0 1` — technically not a lie, and very easy to miss. An ORM batching
/// writes lost rows with no error anywhere. Refusing a statement that was only
/// partly understood is strictly better than executing the part that was.
pub fn split_values_tuples(sql: &str) -> Option<Result<Vec<String>, String>> {
    let upper = sql.to_ascii_uppercase();
    // The `VALUES` that introduces the tuples, not one inside a string.
    let values_at = find_values_keyword(sql, &upper)?;

    let head = &sql[..values_at];
    let mut rest = sql[values_at + "VALUES".len()..].trim_start();

    let mut tuples: Vec<&str> = Vec::new();
    // A comma says another tuple is coming. If none does, the clause is
    // malformed and must be reported rather than quietly ending the list.
    let mut expecting_tuple = true;
    loop {
        if !rest.starts_with('(') {
            break;
        }
        let Some(close) = matching_paren(rest, 0) else {
            return Some(Err(
                "unbalanced parentheses in the VALUES clause".to_string()
            ));
        };
        tuples.push(&rest[..=close]);
        expecting_tuple = false;
        rest = rest[close + 1..].trim_start();
        if let Some(after_comma) = rest.strip_prefix(',') {
            rest = after_comma.trim_start();
            expecting_tuple = true;
            continue;
        }
        break;
    }

    if expecting_tuple {
        // Either no tuple at all — an `INSERT … SELECT`, which is a different
        // statement and not this function's business — or a comma with nothing
        // after it.
        if tuples.is_empty() {
            return None;
        }
        return Some(Err("trailing comma in the VALUES clause".to_string()));
    }

    // A group that opened and never closed leaves text the loop could not
    // consume. Reporting it is the whole point: the old parser took the first
    // tuple and discarded the rest without a word.
    if rest.starts_with('(') {
        return Some(Err(
            "unbalanced parentheses in the VALUES clause".to_string()
        ));
    }

    if tuples.len() <= 1 {
        return None;
    }

    // Whatever follows the tuples — `RETURNING …`, a trailing `;` — is carried
    // onto every statement so a RETURNING batch still reports every row.
    let tail = rest.trim_end_matches(';').trim();
    let mut out = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        let mut stmt = String::with_capacity(head.len() + tuple.len() + tail.len() + 16);
        stmt.push_str(head);
        stmt.push_str("VALUES ");
        stmt.push_str(tuple);
        if !tail.is_empty() {
            stmt.push(' ');
            stmt.push_str(tail);
        }
        out.push(stmt);
    }
    Some(Ok(out))
}

/// Byte offset of the `VALUES` keyword that introduces the tuple list.
///
/// Skips any occurrence inside a quoted string, so a row containing the word
/// does not derail the split.
fn find_values_keyword(sql: &str, upper: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            _ if !in_single && !in_double => {
                // Bytes, not `upper[i..]`, which panics inside a multi-byte
                // character of an unquoted identifier.
                if upper.as_bytes()[i..].starts_with(b"VALUES") {
                    // An identifier byte on either side means this is inside a
                    // name — `raw_values`, `order_values`, `valuesx` — not the
                    // keyword. `_`, `$` and non-ASCII bytes are identifier bytes
                    // too: matching inside `raw_values` split the statement at
                    // the column list, the tuple splitter gave up, and the
                    // single-row fallback silently kept only the first tuple.
                    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80;
                    let before_ok = i == 0 || !ident(bytes[i - 1]);
                    let after = i + "VALUES".len();
                    let after_ok = after >= bytes.len() || !ident(bytes[after]);
                    if before_ok && after_ok {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod multi_row_values {
    use super::*;

    fn split(sql: &str) -> Option<Result<Vec<String>, String>> {
        split_values_tuples(sql)
    }

    #[test]
    fn non_ascii_outside_quotes_does_not_panic_the_keyword_scanners() {
        // Each of these sliced `&str` at every byte and panicked inside `ë`.
        let sql = "INSERT INTO tëst (naïve, id) VALUES ('ü', 2), ('ø', 3)";
        let split = split("INSERT INTO tëst (naïve, id) VALUES ('ü', 2), ('ø', 3)")
            .expect("two tuples")
            .expect("splits");
        assert_eq!(split[1], "INSERT INTO tëst (naïve, id) VALUES ('ø', 3)");
        let upper = sql.to_ascii_uppercase();
        assert_eq!(find_word(&upper, "VALUES", 0), sql.find("VALUES"));
        assert!(insert_select_source("INSERT INTO t (a) aëëë").is_none());
        assert!(insert_select_source("INSERT INTë").is_none());
    }

    #[test]
    fn a_single_tuple_needs_no_splitting() {
        assert!(split("INSERT INTO t (a, b) VALUES (1, 'x')").is_none());
    }

    #[test]
    fn each_tuple_becomes_its_own_statement() {
        let out = split("INSERT INTO t (a, b) VALUES (1,'a'), (2,'b'), (3,'c')")
            .expect("multi-tuple")
            .expect("parsed");
        assert_eq!(
            out,
            vec![
                "INSERT INTO t (a, b) VALUES (1,'a')",
                "INSERT INTO t (a, b) VALUES (2,'b')",
                "INSERT INTO t (a, b) VALUES (3,'c')",
            ]
        );
    }

    /// A comma or a parenthesis inside a string is data, not structure.
    #[test]
    fn quoted_commas_and_parens_do_not_split_a_tuple() {
        let out = split("INSERT INTO t (a) VALUES ('x, y'), ('a)b')")
            .expect("multi-tuple")
            .expect("parsed");
        assert_eq!(out.len(), 2);
        assert!(out[0].ends_with("('x, y')"), "{}", out[0]);
        assert!(out[1].ends_with("('a)b')"), "{}", out[1]);
    }

    /// The word VALUES inside a string must not be mistaken for the keyword.
    #[test]
    fn the_keyword_is_not_matched_inside_a_string() {
        let out = split("INSERT INTO t (a, b) VALUES ('VALUES', 1), ('x', 2)")
            .expect("multi-tuple")
            .expect("parsed");
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("('VALUES', 1)"), "{}", out[0]);
    }

    /// Builds a schema the way a nullable column is actually stored: the column
    /// itself, its `__null_` companion, and a slot left behind by a DROP.
    fn returning_schema() -> Vec<SchemaField> {
        let mut fields = Vec::new();
        for (order, (name, deleted)) in [
            ("id", false),
            ("user_table_id", false),
            ("__null_user_table_id", false),
            ("gone", true),
        ]
        .into_iter()
        .enumerate()
        {
            let mut field =
                SchemaField::new(name.to_string(), DbType::I64, 8, 0, order as u64, false);
            field.is_deleted = deleted;
            fields.push(field);
        }
        fields
    }

    /// `RETURNING *` is the client asking for the row it just wrote, so it must
    /// show the same columns `SELECT *` does. It was returning the `__null_`
    /// companions and dropped slots as well, which a client then had to decode.
    #[test]
    fn returning_star_hides_what_select_star_hides() {
        let schema = returning_schema();
        let out = returning_columns("INSERT INTO t (id) VALUES (1) RETURNING *", &schema)
            .expect("clause found")
            .expect("resolved");
        let names: Vec<&str> = out.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["id", "user_table_id"]);
    }

    /// Naming the companion directly is not a way around that.
    #[test]
    fn a_hidden_companion_cannot_be_named_in_returning() {
        let schema = returning_schema();
        let out = returning_columns(
            "INSERT INTO t (id) VALUES (1) RETURNING __null_user_table_id",
            &schema,
        )
        .expect("clause found");
        assert!(
            out.is_err(),
            "a companion column is not addressable: {:?}",
            out
        );
    }

    /// `_` is an identifier byte, so a column whose name merely ends in the
    /// keyword is not a clause. The same boundary bug once split a WHERE.
    #[test]
    fn a_column_named_like_the_keyword_is_not_a_clause() {
        assert!(
            returning_split("INSERT INTO t (last_returning) VALUES (1)").is_none(),
            "matched inside an identifier"
        );
        assert!(
            returning_split("INSERT INTO t (\"returning\") VALUES (1)").is_none(),
            "matched a quoted identifier"
        );
        let (head, tail) =
            returning_split("INSERT INTO t (last_returning) VALUES (1) RETURNING id")
                .expect("the real clause is still found");
        assert!(head.ends_with("(1)"), "{}", head);
        assert_eq!(tail, "id");
    }

    /// A tail clause applies to every row, or a RETURNING batch would report
    /// only the last.
    #[test]
    fn a_returning_clause_is_carried_onto_every_statement() {
        let out = split("INSERT INTO t (a) VALUES (1), (2) RETURNING a")
            .expect("multi-tuple")
            .expect("parsed");
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|s| s.ends_with("RETURNING a")), "{:?}", out);
    }

    /// Refused rather than half-executed: the whole reason this exists.
    #[test]
    fn a_clause_that_cannot_be_accounted_for_is_refused() {
        assert!(
            split("INSERT INTO t (a) VALUES (1),")
                .expect("saw a comma")
                .is_err()
        );
        assert!(
            split("INSERT INTO t (a) VALUES (1), (2")
                .expect("saw two groups")
                .is_err(),
            "an unbalanced tuple must not be silently dropped"
        );
    }
}

#[cfg(test)]
mod column_list_supply {
    use super::*;

    fn field(name: &str, order: u64, deleted: bool) -> SchemaField {
        let mut f = SchemaField::new(name.to_string(), DbType::I64, 8, 0, order, false);
        f.is_deleted = deleted;
        f
    }

    fn schema() -> Vec<SchemaField> {
        // Declaration order is write order, not vector order.
        vec![
            field("name", 1, false),
            field("id", 0, false),
            field("__null_name", 2, false),
            field("legacy", 3, true),
            field("select", 4, false),
        ]
    }

    #[test]
    fn an_omitted_list_names_the_visible_columns_in_declaration_order() {
        let out = supply_column_list("INSERT INTO t VALUES (1, 'a', 2)", &schema()).unwrap();
        assert_eq!(out, "INSERT INTO t (id, name, select) VALUES (1, 'a', 2)");
    }

    #[test]
    fn a_short_tuple_names_only_the_leading_columns() {
        let out = supply_column_list("insert into t values (1)", &schema()).unwrap();
        assert_eq!(out, "insert into t (id) values (1)");
    }

    #[test]
    fn a_present_list_is_left_alone() {
        assert!(supply_column_list("INSERT INTO t (id) VALUES (1)", &schema()).is_none());
        assert!(supply_column_list("INSERT INTO t(id)VALUES(1)", &schema()).is_none());
    }

    #[test]
    fn other_row_sources_are_left_alone() {
        assert!(supply_column_list("INSERT INTO t SELECT * FROM u", &schema()).is_none());
        assert!(supply_column_list("UPDATE t SET a = 1", &schema()).is_none());
    }

    #[test]
    fn the_keyword_is_found_outside_quotes_and_on_a_boundary() {
        let out =
            supply_column_list("INSERT INTO \"values\" VALUES ('values', 'x')", &schema()).unwrap();
        assert_eq!(
            out,
            "INSERT INTO \"values\" (id, name) VALUES ('values', 'x')"
        );
        assert!(supply_column_list("INSERT INTO t VALUESX (1)", &schema()).is_none());
    }

    #[test]
    fn multi_row_and_returning_survive() {
        let out = supply_column_list(
            "INSERT INTO t VALUES (1, 'a'), (2, 'b') RETURNING id",
            &schema(),
        )
        .unwrap();
        assert_eq!(
            out,
            "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b') RETURNING id"
        );
    }

    #[test]
    fn an_awkward_name_is_quoted() {
        let s = vec![field("Order Id", 0, false)];
        let out = supply_column_list("INSERT INTO t VALUES (1)", &s).unwrap();
        assert_eq!(out, "INSERT INTO t (\"Order Id\") VALUES (1)");
    }
}

#[cfg(test)]
mod insert_parse_bench {
    use super::*;
    use crate::core::rql::lexer_s1::InsertRowData;

    fn field(name: &str, db_type: DbType, size: u32, order: u64) -> SchemaField {
        SchemaField {
            name: name.to_string(),
            db_type,
            size,
            offset: 0,
            write_order: order,
            is_unique: false,
            is_deleted: false,
            scale: None,
        }
    }

    /// A wide multi-row INSERT of text and JSON values, parsed the way the
    /// executor parses it: split into one statement per tuple, each parsed.
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored insert_parse_bench --nocapture`
    #[test]
    #[ignore]
    fn insert_parse_bench() {
        let schema = vec![
            field("id", DbType::I64, 8, 0),
            field("name", DbType::STRING, 12, 1),
            field("doc", DbType::STRING, 12, 2),
            field("note", DbType::STRING, 12, 3),
            field("qty", DbType::I32, 4, 4),
        ];
        let doc = format!(
            "{{\"payload\": \"{}\", \"tags\": [\"a\", \"b''c\"], \"n\": 1}}",
            "x".repeat(2000)
        );
        let mut sql = String::from("INSERT INTO items (id, name, doc, note, qty) VALUES ");
        for r in 0..100 {
            if r > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({}, 'name {} with O''Quote and ünïcode', '{}', '{}', {})",
                r,
                r,
                doc,
                "note, with (parens) ".repeat(20),
                r * 3
            ));
        }
        let tuples = split_values_tuples(&sql).unwrap().unwrap();
        let purposes: Vec<QueryPurpose> = tuples
            .iter()
            .map(|t| {
                QueryPurpose::InsertRow(InsertRowData {
                    query: t.clone(),
                    table_name: "items".to_string(),
                    on_conflict_do_nothing: false,
                    on_conflict_update: None,
                })
            })
            .collect();
        for qp in &purposes {
            assert!(insert_row_with_defaults(qp, &schema, &[], &[], 0).is_some());
        }
        let iters = 50u32;
        for round in 0..4 {
            let t = std::time::Instant::now();
            for _ in 0..iters {
                for qp in &purposes {
                    std::hint::black_box(insert_row_with_defaults(
                        std::hint::black_box(qp),
                        &schema,
                        &[],
                        &[],
                        0,
                    ));
                }
            }
            println!(
                "round {round}: {} tuples, {} B statement: {:?} per statement",
                purposes.len(),
                sql.len(),
                t.elapsed() / iters
            );
        }
    }
}

#[cfg(test)]
mod csv_values_reference {
    /// `parse_csv_values` as it was: a character at a time into owned strings.
    fn reference(s: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut current_value = String::new();
        let mut in_quotes = false;
        let mut quote_char = '"';
        for c in s.chars() {
            match c {
                '"' | '\'' if !in_quotes => {
                    in_quotes = true;
                    quote_char = c;
                    current_value.push(c);
                }
                c if c == quote_char && in_quotes => {
                    in_quotes = false;
                    current_value.push(c);
                }
                ',' if !in_quotes => {
                    values.push(current_value.trim().to_string());
                    current_value.clear();
                }
                _ => current_value.push(c),
            }
        }
        let trimmed = current_value.trim();
        if !trimmed.is_empty() {
            values.push(trimmed.to_string());
        }
        values
    }

    #[test]
    fn matches_the_reference() {
        let pieces = [
            // No brackets, `E` prefixes or `$`: the reference tracked quotes
            // alone, and nesting is what the rewrite changed on purpose.
            "'", "''", "\"", ",", ", ", " ", "a", "NULL", "'x,y'", "\"q,\"", "日本", "ü",
            "\u{2003}", "\n", "1.5", "'it''s'", "\\", "🙂",
        ];
        let mut state = 0x853c_49e6_748f_ea9bu64;
        for _ in 0..100_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let n = (state % 12) as usize;
            let mut s = String::new();
            let mut r = state;
            for _ in 0..n {
                r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                s.push_str(pieces[((r >> 33) % pieces.len() as u64) as usize]);
            }
            let got: Vec<String> = super::parse_csv_values(&s).into_iter().map(str::to_string).collect();
            assert_eq!(got, reference(&s), "{:?}", s);
        }
    }
}
