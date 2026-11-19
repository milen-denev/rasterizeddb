//! `COPY … FROM STDIN` and `COPY … TO STDOUT`, in the text, CSV and binary
//! formats, over the copy sub-protocol.
//!
//! Every driver's bulk path is COPY (Npgsql's `BeginBinaryImport`, pgx's
//! `CopyFrom`, psycopg's `copy`, JDBC's `CopyManager`); each was "syntax
//! error or unsupported query". Rows read from the client are written as
//! multi-row INSERTs through the session's own path (privileges, triggers,
//! defaults and checks as an INSERT has them), in one transaction: the
//! statement's own outside a block, so a row that fails leaves none of the
//! others behind, as PostgreSQL's COPY is one statement. Rows written to the
//! client are the statement's or the table's, as a SELECT reads them.
//!
//! A file or a program (`FROM '/path'`, `PROGRAM`) is the server's
//! filesystem, which a client of this one does not reach: refused (0A000).

use std::io;

use rclite::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

use super::trigger_exec::{Rows, query_in};
use super::view_statements::run_internal;
use super::{
    SessionState, fail_statement, read_frame_body, send_command_complete, send_error_response,
    send_message, sqlstate_for,
};
use crate::core::database::Database;

/// A COPY's data format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Text,
    Csv,
    Binary,
}

/// What `COPY` was asked.
#[derive(Debug, Clone)]
pub(crate) struct Copy {
    /// The table, or `None` for `COPY (query) TO`.
    pub table: Option<String>,
    pub query: Option<String>,
    pub columns: Vec<String>,
    pub from: bool,
    pub format: Format,
    pub delimiter: u8,
    pub null: String,
    pub header: bool,
    pub quote: u8,
    pub escape: u8,
    /// Columns `FORCE_QUOTE` names (`*` is every column).
    pub force_quote: Vec<String>,
    pub force_quote_all: bool,
}

/// `(sqlstate, message)`.
type Refusal = (&'static str, String);

fn refuse(code: &'static str, message: impl Into<String>) -> Refusal {
    (code, message.into())
}

/// The words and punctuation of a COPY statement, quotes kept.
fn tokens(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            let end = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            out.push(sql[i..end].to_string());
            i = end.max(i + 1);
            continue;
        }
        if matches!(b, b'(' | b')' | b',' | b';') {
            out.push((b as char).to_string());
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len()
            && !bytes[i].is_ascii_whitespace()
            && !matches!(bytes[i], b'(' | b')' | b',' | b';' | b'\'' | b'"')
        {
            i += 1;
        }
        out.push(sql[start..i].to_string());
    }
    out
}

fn is(token: Option<&String>, word: &str) -> bool {
    token.is_some_and(|t| t.eq_ignore_ascii_case(word))
}

/// A string option's value: `'x'` as `x`.
fn string_value(token: Option<&String>) -> Result<String, Refusal> {
    let t = token.ok_or_else(|| refuse("42601", "a COPY option is missing its value"))?;
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Ok(t[1..t.len() - 1].replace("''", "'"));
    }
    if t.starts_with("E'") || t.starts_with("e'") {
        return Ok(t[2..t.len() - 1].to_string());
    }
    Ok(t.clone())
}

fn single_byte(value: &str, what: &str) -> Result<u8, Refusal> {
    let bytes = value.as_bytes();
    match bytes {
        [b] => Ok(*b),
        _ => Err(refuse(
            "0A000",
            format!("COPY {what} must be a single one-byte character"),
        )),
    }
}

fn name_of(token: &str) -> String {
    let name = token.rsplit('.').next().unwrap_or(token);
    if name.starts_with('"') {
        name.trim_matches('"').replace("\"\"", "\"")
    } else {
        name.to_ascii_lowercase()
    }
}

/// `COPY …`, or `None` for a statement that is not one.
pub(crate) fn parse_copy(sql: &str) -> Option<Result<Copy, Refusal>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if !trimmed
        .get(..4)
        .is_some_and(|w| w.eq_ignore_ascii_case("COPY"))
        || trimmed
            .as_bytes()
            .get(4)
            .is_some_and(|b| !b.is_ascii_whitespace() && *b != b'(')
    {
        return None;
    }
    Some(parse(trimmed))
}

fn parse(sql: &str) -> Result<Copy, Refusal> {
    let mut copy = Copy {
        table: None,
        query: None,
        columns: Vec::new(),
        from: true,
        format: Format::Text,
        delimiter: b'\t',
        null: "\\N".to_string(),
        header: false,
        quote: b'"',
        escape: b'"',
        force_quote: Vec::new(),
        force_quote_all: false,
    };
    let rest = sql[4..].trim_start();
    // `COPY (query) TO …`.
    let after_target = if rest.starts_with('(') {
        let close = matching_paren(rest)
            .ok_or_else(|| refuse("42601", "COPY's query has no closing parenthesis"))?;
        copy.query = Some(rest[1..close].trim().to_string());
        &rest[close + 1..]
    } else {
        rest
    };
    let toks = tokens(after_target);
    let mut i = 0usize;
    if copy.query.is_none() {
        let name = toks
            .first()
            .ok_or_else(|| refuse("42601", "COPY names no table"))?;
        copy.table = Some(name_of(name));
        i = 1;
        if toks.get(i).map(String::as_str) == Some("(") {
            i += 1;
            while i < toks.len() && toks[i] != ")" {
                if toks[i] != "," {
                    copy.columns.push(name_of(&toks[i]));
                }
                i += 1;
            }
            i += 1;
        }
    }
    copy.from = if is(toks.get(i), "FROM") {
        true
    } else if is(toks.get(i), "TO") {
        false
    } else {
        return Err(refuse("42601", "COPY needs FROM STDIN or TO STDOUT"));
    };
    if copy.from && copy.query.is_some() {
        return Err(refuse("42601", "COPY FROM takes a table, not a query"));
    }
    i += 1;
    let endpoint = toks.get(i).cloned().unwrap_or_default();
    let expected = if copy.from { "STDIN" } else { "STDOUT" };
    if !endpoint.eq_ignore_ascii_case(expected) {
        return Err(refuse(
            "0A000",
            format!(
                "COPY {} a file or a program is not supported here: it reads the server's filesystem; use {}",
                if copy.from { "from" } else { "to" },
                expected
            ),
        ));
    }
    i += 1;
    if is(toks.get(i), "WITH") {
        i += 1;
    }
    let mut format_set = false;
    let mut null_set = false;
    let mut delimiter_set = false;
    if toks.get(i).map(String::as_str) == Some("(") {
        // `(FORMAT csv, HEADER, DELIMITER ',' …)`.
        i += 1;
        while i < toks.len() && toks[i] != ")" {
            let option = toks[i].to_ascii_uppercase();
            i += 1;
            // The value runs to the next `,` or `)` at this level.
            let mut value: Vec<String> = Vec::new();
            let mut depth = 0;
            while i < toks.len() && !(depth == 0 && (toks[i] == "," || toks[i] == ")")) {
                if toks[i] == "(" {
                    depth += 1;
                } else if toks[i] == ")" {
                    depth -= 1;
                }
                value.push(toks[i].clone());
                i += 1;
            }
            if toks.get(i).map(String::as_str) == Some(",") {
                i += 1;
            }
            let first = value.first();
            match option.as_str() {
                "FORMAT" => {
                    format_set = true;
                    copy.format = match string_value(first)?.to_ascii_lowercase().as_str() {
                        "text" => Format::Text,
                        "csv" => Format::Csv,
                        "binary" => Format::Binary,
                        other => {
                            return Err(refuse(
                                "22023",
                                format!("COPY format \"{other}\" not recognized"),
                            ));
                        }
                    };
                }
                "HEADER" => {
                    copy.header = match first.map(|v| v.trim_matches('\'').to_ascii_lowercase()) {
                        None => true,
                        Some(v) => match v.as_str() {
                            "true" | "on" | "1" => true,
                            "false" | "off" | "0" => false,
                            _ => {
                                return Err(refuse(
                                    "0A000",
                                    "COPY HEADER MATCH is not supported here",
                                ));
                            }
                        },
                    };
                }
                "DELIMITER" => {
                    delimiter_set = true;
                    copy.delimiter = single_byte(&string_value(first)?, "delimiter")?;
                }
                "NULL" => {
                    null_set = true;
                    copy.null = string_value(first)?;
                }
                "QUOTE" => copy.quote = single_byte(&string_value(first)?, "quote")?,
                "ESCAPE" => copy.escape = single_byte(&string_value(first)?, "escape")?,
                "ENCODING" => {
                    let encoding = string_value(first)?
                        .to_ascii_uppercase()
                        .replace(['-', '_'], "");
                    if encoding != "UTF8" {
                        return Err(refuse(
                            "0A000",
                            "COPY ENCODING other than UTF8 is not supported here",
                        ));
                    }
                }
                "FORCE_QUOTE" => {
                    if value.iter().any(|v| v == "*") {
                        copy.force_quote_all = true;
                    } else {
                        copy.force_quote = value
                            .iter()
                            .filter(|v| !matches!(v.as_str(), "(" | ")" | ","))
                            .map(|v| name_of(v))
                            .collect();
                    }
                }
                "FORCE_NOT_NULL" | "FORCE_NULL" | "FREEZE" => {}
                other => {
                    return Err(refuse(
                        "42601",
                        format!("option \"{}\" not recognized", other.to_ascii_lowercase()),
                    ));
                }
            }
        }
        i += 1;
    } else {
        // The pre-9.0 spellings: `BINARY`, `CSV HEADER`, `DELIMITER 'x'`, `NULL 'x'`.
        while i < toks.len() {
            let word = toks[i].to_ascii_uppercase();
            i += 1;
            match word.as_str() {
                "BINARY" => {
                    format_set = true;
                    copy.format = Format::Binary;
                }
                "CSV" => {
                    format_set = true;
                    copy.format = Format::Csv;
                }
                "HEADER" => copy.header = true,
                "DELIMITER" | "NULL" | "QUOTE" | "ESCAPE" => {
                    if is(toks.get(i), "AS") {
                        i += 1;
                    }
                    let value = string_value(toks.get(i))?;
                    i += 1;
                    match word.as_str() {
                        "DELIMITER" => {
                            delimiter_set = true;
                            copy.delimiter = single_byte(&value, "delimiter")?;
                        }
                        "NULL" => {
                            null_set = true;
                            copy.null = value;
                        }
                        "QUOTE" => copy.quote = single_byte(&value, "quote")?,
                        _ => copy.escape = single_byte(&value, "escape")?,
                    }
                }
                "FORCE" => {
                    // `FORCE QUOTE cols` / `FORCE NOT NULL cols`.
                    let quote = is(toks.get(i), "QUOTE");
                    while i < toks.len()
                        && !matches!(
                            toks[i].to_ascii_uppercase().as_str(),
                            "CSV" | "HEADER" | "DELIMITER" | "NULL" | "QUOTE" | "ESCAPE" | "BINARY"
                        )
                        || is(toks.get(i), "QUOTE")
                        || is(toks.get(i), "NOT")
                    {
                        if quote && toks[i] == "*" {
                            copy.force_quote_all = true;
                        }
                        i += 1;
                        if i >= toks.len() {
                            break;
                        }
                    }
                }
                "WHERE" => return Err(refuse("0A000", "COPY FROM … WHERE is not supported here")),
                _ => {
                    return Err(refuse(
                        "42601",
                        format!("syntax error at or near \"{}\"", toks[i - 1]),
                    ));
                }
            }
        }
    }
    if i < toks.len() && toks[i] != ";" {
        if is(toks.get(i), "WHERE") {
            return Err(refuse("0A000", "COPY FROM … WHERE is not supported here"));
        }
        return Err(refuse(
            "42601",
            format!("syntax error at or near \"{}\"", toks[i]),
        ));
    }
    let _ = format_set;
    if copy.format == Format::Csv {
        if !delimiter_set {
            copy.delimiter = b',';
        }
        if !null_set {
            copy.null = String::new();
        }
    }
    if copy.format == Format::Binary && (delimiter_set || null_set || copy.header) {
        return Err(refuse(
            "42601",
            "cannot specify DELIMITER, NULL or HEADER in BINARY mode",
        ));
    }
    Ok(copy)
}

fn matching_paren(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

// ------------------------------------------------------------------ input

/// One field as SQL: a quoted literal, or NULL.
fn literal(field: Option<&str>) -> String {
    match field {
        None => "NULL".to_string(),
        Some(text) => crate::auth::quote(text),
    }
}

/// The complete text-format rows at the start of `buf` (each its fields,
/// NULL as `None`), and the bytes they took; `\.` ends the data.
fn text_rows(
    buf: &[u8],
    copy: &Copy,
    at_end: bool,
) -> Result<(Vec<Vec<Option<String>>>, usize, bool), Refusal> {
    let mut rows = Vec::new();
    let mut at = 0usize;
    loop {
        let line_end = match memchr::memchr(b'\n', &buf[at..]) {
            Some(p) => at + p,
            None if at_end && at < buf.len() => buf.len(),
            None => break,
        };
        let mut line = &buf[at..line_end];
        let next = (line_end + 1).min(buf.len());
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if line == b"\\." {
            return Ok((rows, buf.len(), true));
        }
        let mut fields = Vec::new();
        let mut raw: Vec<u8> = Vec::new();
        let mut i = 0usize;
        let push = |raw: &mut Vec<u8>, fields: &mut Vec<Option<String>>| -> Result<(), Refusal> {
            let text = String::from_utf8(std::mem::take(raw))
                .map_err(|_| refuse("22021", "invalid byte sequence for encoding \"UTF8\""))?;
            fields.push(Some(text));
            Ok(())
        };
        // Whether the field as written was the NULL marker.
        let mut written: Vec<u8> = Vec::new();
        while i <= line.len() {
            if i == line.len() || line[i] == copy.delimiter {
                if written == copy.null.as_bytes() {
                    fields.push(None);
                    raw.clear();
                } else {
                    push(&mut raw, &mut fields)?;
                }
                written.clear();
                i += 1;
                continue;
            }
            let b = line[i];
            written.push(b);
            if b == b'\\' && i + 1 < line.len() {
                let c = line[i + 1];
                written.push(c);
                i += 2;
                match c {
                    b'b' => raw.push(8),
                    b'f' => raw.push(12),
                    b'n' => raw.push(b'\n'),
                    b'r' => raw.push(b'\r'),
                    b't' => raw.push(b'\t'),
                    b'v' => raw.push(11),
                    b'x' if i < line.len() && line[i].is_ascii_hexdigit() => {
                        let mut v = 0u8;
                        let mut n = 0;
                        while n < 2 && i < line.len() && line[i].is_ascii_hexdigit() {
                            v = v * 16 + (line[i] as char).to_digit(16).unwrap_or(0) as u8;
                            written.push(line[i]);
                            i += 1;
                            n += 1;
                        }
                        raw.push(v);
                    }
                    b'0'..=b'7' => {
                        let mut v = u32::from(c - b'0');
                        let mut n = 1;
                        while n < 3 && i < line.len() && (b'0'..=b'7').contains(&line[i]) {
                            v = v * 8 + u32::from(line[i] - b'0');
                            written.push(line[i]);
                            i += 1;
                            n += 1;
                        }
                        raw.push(v as u8);
                    }
                    other => raw.push(other),
                }
                continue;
            }
            raw.push(b);
            i += 1;
        }
        rows.push(fields);
        at = next;
        if at >= buf.len() {
            break;
        }
    }
    Ok((rows, at, false))
}

/// The complete CSV records at the start of `buf`, and the bytes they took.
fn csv_rows(
    buf: &[u8],
    copy: &Copy,
    at_end: bool,
) -> Result<(Vec<Vec<Option<String>>>, usize, bool), Refusal> {
    let mut rows = Vec::new();
    let mut at = 0usize;
    'records: while at < buf.len() {
        let mut fields: Vec<Option<String>> = Vec::new();
        let mut i = at;
        let mut field: Vec<u8> = Vec::new();
        let mut quoted = false;
        loop {
            if i >= buf.len() {
                if !at_end {
                    break 'records;
                }
                if quoted && field.is_empty() && fields.is_empty() {
                    break 'records;
                }
                // The last record, with no line end.
                fields.push(csv_field(field, quoted, copy)?);
                rows.push(fields);
                at = buf.len();
                break 'records;
            }
            let b = buf[i];
            if b == copy.quote {
                // A quoted run, to its closing quote.
                quoted = true;
                i += 1;
                loop {
                    if i >= buf.len() {
                        if at_end {
                            return Err(refuse("22P04", "unterminated CSV quoted field"));
                        }
                        break 'records;
                    }
                    let c = buf[i];
                    if c == copy.escape
                        && copy.escape != copy.quote
                        && buf.get(i + 1) == Some(&copy.quote)
                    {
                        field.push(copy.quote);
                        i += 2;
                        continue;
                    }
                    if c == copy.quote {
                        if copy.escape == copy.quote && buf.get(i + 1) == Some(&copy.quote) {
                            field.push(copy.quote);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    field.push(c);
                    i += 1;
                }
                continue;
            }
            if b == copy.delimiter {
                fields.push(csv_field(std::mem::take(&mut field), quoted, copy)?);
                quoted = false;
                i += 1;
                continue;
            }
            if b == b'\n' || b == b'\r' {
                fields.push(csv_field(std::mem::take(&mut field), quoted, copy)?);
                let mut next = i + 1;
                if b == b'\r' && buf.get(next) == Some(&b'\n') {
                    next += 1;
                }
                if fields.len() == 1 && fields[0].as_deref() == Some("\\.") && !quoted {
                    return Ok((rows, buf.len(), true));
                }
                rows.push(fields);
                at = next;
                continue 'records;
            }
            field.push(b);
            i += 1;
        }
    }
    Ok((rows, at, false))
}

fn csv_field(bytes: Vec<u8>, quoted: bool, copy: &Copy) -> Result<Option<String>, Refusal> {
    let text = String::from_utf8(bytes)
        .map_err(|_| refuse("22021", "invalid byte sequence for encoding \"UTF8\""))?;
    // An unquoted field equal to the NULL string is NULL; a quoted one never.
    if !quoted && text == copy.null {
        return Ok(None);
    }
    Ok(Some(text))
}

const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// The complete binary tuples at the start of `buf` (after its header),
/// each field's bytes; `true` at the trailer.
fn binary_rows(
    buf: &[u8],
    header_read: &mut bool,
) -> Result<(Vec<Vec<Option<Vec<u8>>>>, usize, bool), Refusal> {
    let mut at = 0usize;
    if !*header_read {
        if buf.len() < 19 {
            return Ok((Vec::new(), 0, false));
        }
        if &buf[..11] != BINARY_SIGNATURE {
            return Err(refuse("22P04", "COPY file signature not recognized"));
        }
        let ext = u32::from_be_bytes([buf[15], buf[16], buf[17], buf[18]]) as usize;
        if buf.len() < 19 + ext {
            return Ok((Vec::new(), 0, false));
        }
        at = 19 + ext;
        *header_read = true;
    }
    let mut rows = Vec::new();
    loop {
        if buf.len() < at + 2 {
            return Ok((rows, at, false));
        }
        let count = i16::from_be_bytes([buf[at], buf[at + 1]]);
        if count == -1 {
            return Ok((rows, buf.len(), true));
        }
        let mut i = at + 2;
        let mut fields = Vec::with_capacity(count.max(0) as usize);
        for _ in 0..count {
            if buf.len() < i + 4 {
                return Ok((rows, at, false));
            }
            let len = i32::from_be_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]);
            i += 4;
            if len < 0 {
                fields.push(None);
                continue;
            }
            let len = len as usize;
            if buf.len() < i + len {
                return Ok((rows, at, false));
            }
            fields.push(Some(buf[i..i + len].to_vec()));
            i += len;
        }
        rows.push(fields);
        at = i;
    }
}

/// The table's columns COPY names, or all of them, with each one's type oid.
fn target_columns(
    database: &Arc<Database>,
    table: &str,
    named: &[String],
) -> Result<Vec<(String, i64)>, Refusal> {
    let relation = database
        .relation(table)
        .ok_or_else(|| refuse("42P01", format!("relation \"{table}\" does not exist")))?;
    let schema = relation.schema();
    let mut fields: Vec<&crate::core::row::schema::SchemaField> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .collect();
    fields.sort_by_key(|f| f.write_order);
    let typed = |f: &crate::core::row::schema::SchemaField| {
        (
            f.name.clone(),
            crate::core::rql::catalog_model::column_type_oid(f, &schema),
        )
    };
    if named.is_empty() {
        return Ok(fields.iter().map(|f| typed(f)).collect());
    }
    named
        .iter()
        .map(|n| {
            fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(n))
                .map(|f| typed(f))
                .ok_or_else(|| {
                    refuse(
                        "42703",
                        format!("column \"{n}\" of relation \"{table}\" does not exist"),
                    )
                })
        })
        .collect()
}

/// Rows written per INSERT.
const BATCH: usize = 500;

/// `COPY … FROM STDIN`.
async fn copy_from<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    copy: &Copy,
) -> io::Result<Result<u64, Refusal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let table = copy.table.clone().unwrap_or_default();
    // A temporary table by the name the session writes.
    let real = state
        .temp_tables
        .iter()
        .find(|t| t.name == table)
        .map(|t| t.real.clone())
        .unwrap_or_else(|| table.clone());
    let columns = match target_columns(database, &real, &copy.columns) {
        Ok(c) => c,
        Err(e) => return Ok(Err(e)),
    };
    let binary = copy.format == Format::Binary;
    // CopyInResponse: the overall format and each column's.
    let mut response = vec![u8::from(binary)];
    response.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for _ in &columns {
        response.extend_from_slice(&i16::from(binary).to_be_bytes());
    }
    send_message(stream, b'G', &response).await?;
    state.copy_in_ran = true;

    let insert_head = format!(
        "INSERT INTO {} ({}) VALUES ",
        crate::core::rql::views::quote_ident(&table),
        columns
            .iter()
            .map(|(n, _)| crate::core::rql::views::quote_ident(n))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let own_block = state.transaction.is_none();
    let mut failure: Option<Refusal> = None;
    if own_block && let Err(e) = run_internal(database, state, "BEGIN").await? {
        failure = Some((sqlstate_for(&e), e));
    }
    let mut buf: Vec<u8> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut written = 0u64;
    let mut header_skipped = !copy.header;
    let mut binary_header = false;
    let mut ended = false;
    loop {
        // The client's messages, to its CopyDone or CopyFail.
        let mut tag = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(stream, &mut tag).await?;
        let payload = read_frame_body(stream, tag[0]).await?;
        let done = match tag[0] {
            b'd' => {
                if failure.is_none() && !ended {
                    buf.extend_from_slice(&payload);
                }
                false
            }
            b'c' => true,
            b'f' => {
                let message = String::from_utf8_lossy(&payload)
                    .trim_end_matches('\0')
                    .to_string();
                if failure.is_none() {
                    failure = Some(("57014", format!("COPY from stdin failed: {message}")));
                }
                true
            }
            // Flush and Sync are allowed, and ignored, mid-copy.
            b'H' | b'S' => false,
            other => {
                if failure.is_none() {
                    failure = Some((
                        "08P01",
                        format!("unexpected message type 0x{other:02X} during COPY from stdin"),
                    ));
                }
                true
            }
        };
        if failure.is_none() && !ended {
            let parsed = match copy.format {
                Format::Text => text_rows(&buf, copy, done).map(|(r, n, e)| {
                    (
                        r.into_iter()
                            .map(|row| row.into_iter().map(|f| f.map(String::into_bytes)).collect())
                            .collect(),
                        n,
                        e,
                    )
                }),
                Format::Csv => csv_rows(&buf, copy, done).map(|(r, n, e)| {
                    (
                        r.into_iter()
                            .map(|row| row.into_iter().map(|f| f.map(String::into_bytes)).collect())
                            .collect(),
                        n,
                        e,
                    )
                }),
                Format::Binary => binary_rows(&buf, &mut binary_header),
            };
            match parsed {
                Ok((rows, consumed, end)) => {
                    buf.drain(..consumed.min(buf.len()));
                    ended |= end;
                    for row in rows {
                        if !header_skipped {
                            header_skipped = true;
                            continue;
                        }
                        let row: Vec<Option<Vec<u8>>> = row;
                        if row.len() != columns.len() {
                            failure = Some((
                                "22P04",
                                if row.len() < columns.len() {
                                    format!("missing data for column \"{}\"", columns[row.len()].0)
                                } else {
                                    "extra data after last expected column".to_string()
                                },
                            ));
                            break;
                        }
                        let mut values = Vec::with_capacity(row.len());
                        for (field, (_, oid)) in row.into_iter().zip(columns.iter()) {
                            let value = if binary {
                                let param = crate::pg_params::BoundParam {
                                    value: field,
                                    format: 1,
                                    oid: *oid as i32,
                                };
                                match crate::pg_params::param_to_literal(&param) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        failure = Some(("22P03", e));
                                        break;
                                    }
                                }
                            } else {
                                literal(
                                    field
                                        .as_deref()
                                        .map(|b| std::str::from_utf8(b).unwrap_or_default()),
                                )
                            };
                            values.push(value);
                        }
                        if failure.is_some() {
                            break;
                        }
                        pending.push(format!("({})", values.join(", ")));
                        if pending.len() >= BATCH {
                            match insert(database, state, &insert_head, &mut pending).await? {
                                Ok(n) => written += n,
                                Err(e) => {
                                    failure = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => failure = Some(e),
            }
        }
        if done {
            break;
        }
    }
    if failure.is_none() && !pending.is_empty() {
        match insert(database, state, &insert_head, &mut pending).await? {
            Ok(n) => written += n,
            Err(e) => failure = Some(e),
        }
    }
    if failure.is_none() && binary && !ended {
        failure = Some((
            "22P04",
            "COPY binary data ended without its trailer".to_string(),
        ));
    }
    if own_block && state.transaction.is_some() {
        let end = if failure.is_none() {
            "COMMIT"
        } else {
            "ROLLBACK"
        };
        if let Err(e) = run_internal(database, state, end).await?
            && failure.is_none()
        {
            failure = Some((sqlstate_for(&e), e));
        }
    }
    Ok(match failure {
        None => Ok(written),
        Some(e) => Err(e),
    })
}

async fn insert(
    database: &Arc<Database>,
    state: &mut SessionState,
    head: &str,
    pending: &mut Vec<String>,
) -> io::Result<Result<u64, Refusal>> {
    let sql = format!("{head}{}", pending.join(", "));
    let count = pending.len() as u64;
    pending.clear();
    Ok(match run_internal(database, state, &sql).await? {
        Ok(_) => Ok(count),
        Err(e) => Err((sqlstate_for(&e), e)),
    })
}

// ------------------------------------------------------------------ output

fn text_field(value: Option<&str>, copy: &Copy, out: &mut Vec<u8>) {
    let Some(value) = value else {
        out.extend_from_slice(copy.null.as_bytes());
        return;
    };
    for &b in value.as_bytes() {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' if copy.delimiter == b'\t' => out.extend_from_slice(b"\\t"),
            8 => out.extend_from_slice(b"\\b"),
            12 => out.extend_from_slice(b"\\f"),
            11 => out.extend_from_slice(b"\\v"),
            b if b == copy.delimiter => {
                out.push(b'\\');
                out.push(b);
            }
            b => out.push(b),
        }
    }
}

fn csv_value(value: Option<&str>, force: bool, copy: &Copy, out: &mut Vec<u8>) {
    let Some(value) = value else {
        out.extend_from_slice(copy.null.as_bytes());
        return;
    };
    let needs = force
        || value.is_empty() && copy.null.is_empty()
        || value == copy.null
        || value
            .bytes()
            .any(|b| b == copy.delimiter || b == copy.quote || b == b'\n' || b == b'\r')
        || value == "\\.";
    if !needs {
        out.extend_from_slice(value.as_bytes());
        return;
    }
    out.push(copy.quote);
    for &b in value.as_bytes() {
        if b == copy.quote || b == copy.escape {
            out.push(copy.escape);
        }
        out.push(b);
    }
    out.push(copy.quote);
}

/// A DataRow's fields, as their bytes.
fn data_row_fields(message: &[u8]) -> Vec<Option<&[u8]>> {
    let count = u16::from_be_bytes([message[5], message[6]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut i = 7;
    for _ in 0..count {
        let len = i32::from_be_bytes([message[i], message[i + 1], message[i + 2], message[i + 3]]);
        i += 4;
        if len < 0 {
            out.push(None);
        } else {
            out.push(Some(&message[i..i + len as usize]));
            i += len as usize;
        }
    }
    out
}

/// `COPY … TO STDOUT`.
async fn copy_to<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    copy: &Copy,
) -> io::Result<Result<u64, Refusal>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let sql = match (&copy.query, &copy.table) {
        (Some(q), _) => q.clone(),
        (None, Some(t)) => {
            let list = if copy.columns.is_empty() {
                "*".to_string()
            } else {
                copy.columns
                    .iter()
                    .map(|c| crate::core::rql::views::quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!(
                "SELECT {list} FROM {}",
                crate::core::rql::views::quote_ident(t)
            )
        }
        (None, None) => return Ok(Err(refuse("42601", "COPY names no table"))),
    };
    let binary = copy.format == Format::Binary;
    let rows: Rows = match query_in(database, state, &sql, if binary { &[1] } else { &[] }).await? {
        Ok(rows) => rows,
        Err(e) => return Ok(Err((sqlstate_for(&e), e))),
    };
    if rows.description.is_none() {
        return Ok(Err(refuse(
            "0A000",
            "COPY query must have a RETURNING clause or return rows",
        )));
    }
    let width = rows.names.len();
    let mut response = vec![u8::from(binary)];
    response.extend_from_slice(&(width as i16).to_be_bytes());
    for _ in 0..width {
        response.extend_from_slice(&i16::from(binary).to_be_bytes());
    }
    send_message(stream, b'H', &response).await?;
    let mut out: Vec<u8> = Vec::new();
    if binary {
        out.extend_from_slice(BINARY_SIGNATURE);
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        send_message(stream, b'd', &out).await?;
        for message in &rows.data {
            out.clear();
            let fields = data_row_fields(message);
            out.extend_from_slice(&(fields.len() as i16).to_be_bytes());
            for field in fields {
                match field {
                    None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                    Some(bytes) => {
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(bytes);
                    }
                }
            }
            send_message(stream, b'd', &out).await?;
        }
        send_message(stream, b'd', &(-1i16).to_be_bytes()).await?;
    } else {
        let forced: Vec<bool> = rows
            .names
            .iter()
            .map(|n| {
                copy.force_quote_all || copy.force_quote.iter().any(|f| f.eq_ignore_ascii_case(n))
            })
            .collect();
        if copy.header {
            out.clear();
            for (i, name) in rows.names.iter().enumerate() {
                if i > 0 {
                    out.push(copy.delimiter);
                }
                if copy.format == Format::Csv {
                    csv_value(Some(name), false, copy, &mut out);
                } else {
                    text_field(Some(name), copy, &mut out);
                }
            }
            out.push(b'\n');
            send_message(stream, b'd', &out).await?;
        }
        for row in &rows.rows {
            out.clear();
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    out.push(copy.delimiter);
                }
                if copy.format == Format::Csv {
                    csv_value(
                        value.as_deref(),
                        forced.get(i).copied().unwrap_or(false),
                        copy,
                        &mut out,
                    );
                } else {
                    text_field(value.as_deref(), copy, &mut out);
                }
            }
            out.push(b'\n');
            send_message(stream, b'd', &out).await?;
        }
    }
    send_message(stream, b'c', &[]).await?;
    Ok(Ok(rows.rows.len() as u64))
}

/// Runs a `COPY`, replying `COPY n` or the error.
pub(super) async fn run_copy<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    copy: Copy,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if copy.from {
        let read_only = state
            .transaction
            .as_ref()
            .map(|t| t.read_only)
            .unwrap_or(state.default_read_only);
        if read_only {
            return fail_statement(
                state,
                stream,
                "25006",
                "cannot execute COPY FROM in a read-only transaction",
            )
            .await;
        }
    }
    let result = if copy.from {
        Box::pin(copy_from(database, state, stream, &copy)).await?
    } else {
        Box::pin(copy_to(database, state, stream, &copy)).await?
    };
    match result {
        Ok(n) => send_command_complete(stream, &format!("COPY {n}")).await,
        Err((code, message)) => {
            if state.transaction.is_some() {
                fail_statement(state, stream, code, &message).await
            } else {
                send_error_response(stream, code, &message).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(sql: &str) -> Copy {
        parse_copy(sql).unwrap().unwrap()
    }

    #[test]
    fn statements_parse_in_both_spellings() {
        let c = p("COPY np (id, s, n) FROM STDIN (FORMAT BINARY)");
        assert_eq!(
            (c.table.as_deref(), c.columns.len(), c.from, c.format),
            (Some("np"), 3, true, Format::Binary)
        );
        let c = p("COPY (SELECT id, s FROM np ORDER BY id) TO STDOUT");
        assert_eq!(
            (c.query.as_deref(), c.from, c.format),
            (
                Some("SELECT id, s FROM np ORDER BY id"),
                false,
                Format::Text
            )
        );
        let c = p("COPY t FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NA')");
        assert_eq!(
            (c.format, c.header, c.delimiter, c.null.as_str()),
            (Format::Csv, true, b';', "NA")
        );
        let c = p("COPY t TO STDOUT WITH CSV HEADER");
        assert_eq!(
            (c.format, c.header, c.delimiter, c.null.as_str()),
            (Format::Csv, true, b',', "")
        );
        let c = p("copy \"T\" from stdin delimiter as '|' null as ''");
        assert_eq!(
            (c.table.as_deref(), c.delimiter, c.null.as_str()),
            (Some("T"), b'|', "")
        );
        assert_eq!(
            parse_copy("COPY t FROM '/etc/passwd'")
                .unwrap()
                .unwrap_err()
                .0,
            "0A000"
        );
        assert!(parse_copy("COPYRIGHT").is_none());
    }

    #[test]
    fn text_rows_read_escapes_and_nulls() {
        let c = p("COPY t FROM STDIN");
        let (rows, used, end) = text_rows(b"1\ta\\tb\n2\t\\N\n3\tpartial", &c, false).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("a\tb".into())],
                vec![Some("2".into()), None]
            ]
        );
        assert_eq!((used, end), (12, false));
        let (rows, _, end) = text_rows(b"4\tx\\\\y\n\\.\n", &c, true).unwrap();
        assert_eq!(rows, vec![vec![Some("4".into()), Some("x\\y".into())]]);
        assert!(end);
    }

    #[test]
    fn csv_rows_keep_quoted_delimiters_and_lines() {
        let c = p("COPY t FROM STDIN (FORMAT csv)");
        let (rows, _, _) = csv_rows(b"1,\"a,b\",\n2,\"x\"\"y\nz\",\"\"\n", &c, true).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("a,b".into()), None],
                vec![
                    Some("2".into()),
                    Some("x\"y\nz".into()),
                    Some(String::new())
                ],
            ]
        );
        // An incomplete quoted record waits for more data.
        let (rows, used, _) = csv_rows(b"1,\"open\n", &c, false).unwrap();
        assert!(rows.is_empty());
        assert_eq!(used, 0);
    }

    #[test]
    fn output_escapes_round_trip() {
        let c = p("COPY t TO STDOUT");
        let mut out = Vec::new();
        text_field(Some("a\tb\\c\nd"), &c, &mut out);
        assert_eq!(out, b"a\\tb\\\\c\\nd");
        let c = p("COPY t TO STDOUT (FORMAT csv)");
        let mut out = Vec::new();
        csv_value(Some("x,\"y\""), false, &c, &mut out);
        assert_eq!(out, b"\"x,\"\"y\"\"\"");
        out.clear();
        csv_value(Some(""), false, &c, &mut out);
        assert_eq!(out, b"\"\"");
    }
}
