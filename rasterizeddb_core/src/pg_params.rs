//! Bind-parameter decoding and inlining for the PostgreSQL wire protocol.
//!
//! The engine's front end takes SQL *text*, while every real Postgres driver
//! (sqlx/sea-orm included) sends parameters out-of-band through Bind. Rather
//! than teach every lexer about placeholders, a bound portal is rendered back
//! into a single self-contained statement: each `$N` is replaced by the literal
//! its parameter denotes. Quoting is handled here so a value can never escape
//! into the statement as syntax.

use crate::core::db_type::DbType;

/// One value bound to a portal, exactly as it arrived on the wire.
#[derive(Clone, Debug)]
pub struct BoundParam {
    /// `None` is a SQL NULL (Bind encodes it as length -1).
    pub value: Option<Vec<u8>>,
    /// 0 = text, 1 = binary.
    pub format: i16,
    /// Type OID from Parse, or 0 when the client left it unspecified.
    pub oid: i32,
}

// Postgres type OIDs used on the parameter path.
pub const OID_BOOL: i32 = 16;
pub const OID_BYTEA: i32 = 17;
pub const OID_CHAR: i32 = 18;
pub const OID_NAME: i32 = 19;
pub const OID_INT8: i32 = 20;
pub const OID_INT2: i32 = 21;
pub const OID_INT4: i32 = 23;
pub const OID_TEXT: i32 = 25;
pub const OID_OID: i32 = 26;
pub const OID_FLOAT4: i32 = 700;
pub const OID_FLOAT8: i32 = 701;
pub const OID_BPCHAR: i32 = 1042;
pub const OID_VARCHAR: i32 = 1043;
pub const OID_DATE: i32 = 1082;
pub const OID_TIME: i32 = 1083;
pub const OID_TIMESTAMP: i32 = 1114;
pub const OID_TIMESTAMPTZ: i32 = 1184;
pub const OID_NUMERIC: i32 = 1700;
pub const OID_UUID: i32 = 2950;
pub const OID_JSON: i32 = 114;
pub const OID_JSONB: i32 = 3802;
pub const OID_TIMETZ: i32 = 1266;
pub const OID_INTERVAL: i32 = 1186;
pub const OID_INET: i32 = 869;
pub const OID_CIDR: i32 = 650;
pub const OID_MACADDR: i32 = 829;
pub const OID_MACADDR8: i32 = 774;
pub const OID_MONEY: i32 = 790;
pub const OID_BIT: i32 = 1560;
pub const OID_VARBIT: i32 = 1562;
pub const OID_POINT: i32 = 600;
pub const OID_LSEG: i32 = 601;
pub const OID_PATH: i32 = 602;
pub const OID_BOX: i32 = 603;
pub const OID_POLYGON: i32 = 604;
pub const OID_LINE: i32 = 628;
pub const OID_CIRCLE: i32 = 718;
pub const OID_INT4RANGE: i32 = 3904;
pub const OID_NUMRANGE: i32 = 3906;
pub const OID_TSRANGE: i32 = 3908;
pub const OID_TSTZRANGE: i32 = 3910;
pub const OID_DATERANGE: i32 = 3912;
pub const OID_INT8RANGE: i32 = 3926;

/// Postgres timestamps count microseconds from 2000-01-01, Unix epoch counts
/// seconds from 1970-01-01. This is the gap, in microseconds.
pub const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Maps a wire OID onto the storage type the engine will hold the value in.
pub fn oid_to_db_type(oid: i32) -> Option<DbType> {
    Some(match oid {
        OID_BOOL => DbType::BOOL,
        OID_INT2 => DbType::I16,
        OID_INT4 | OID_OID => DbType::I32,
        OID_INT8 => DbType::I64,
        OID_FLOAT4 => DbType::F32,
        OID_FLOAT8 => DbType::F64,
        OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME | OID_BYTEA => DbType::STRING,
        OID_CHAR => DbType::CHAR,
        OID_DATE | OID_TIME | OID_TIMESTAMP | OID_TIMESTAMPTZ => DbType::DATETIME,
        // A client that declares a parameter `jsonb` gets the stored form.
        // Without this the parameter was untyped and its document reached the
        // column as text.
        OID_JSON | OID_JSONB => DbType::JSONB,
        _ => return None,
    })
}

/// Rewrites `sql` with every `$N` replaced by the literal form of `params[N-1]`.
///
/// Placeholders inside string literals, quoted identifiers, dollar-quoted
/// bodies, and comments are left alone, so a statement that merely *mentions*
/// `$1` in text is not corrupted.
pub fn inline_params(sql: &str, params: &[BoundParam]) -> Result<String, String> {
    let bytes = sql.as_bytes();
    if params.is_empty() || memchr::memchr(b'$', bytes).is_none() {
        return Ok(sql.to_string());
    }

    // Everything but a placeholder is copied verbatim, so the text is copied
    // in runs: `copied` marks the start of the run not yet written, and only a
    // substitution flushes it. The scan itself jumps between the bytes that
    // can start something — a quote, a comment, a `$` — rather than visiting
    // every character; the body of a string literal, which is most of a wide
    // INSERT, is crossed with one `memchr`.
    let mut out = String::with_capacity(sql.len() + params.len() * 16);
    let mut copied = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i..].iter().position(|&b| SQL_SPECIAL[b as usize]) {
            Some(p) => i += p,
            None => break,
        }
        match bytes[i] {
            // Single-quoted string literal; '' is an escaped quote. After an
            // `E` prefix a backslash escapes the next byte as well, so `\'`
            // does not end it.
            b'\'' => i = string_literal_end(bytes, i, is_escape_string_quote(bytes, i)),
            // Quoted identifier; "" is an escaped quote.
            b'"' => i = quoted_end(bytes, i, b'"'),
            // Line comment, up to (not including) the newline.
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = memchr::memchr(b'\n', &bytes[i..]).map_or(bytes.len(), |p| i + p);
            }
            // Block comment, which Postgres allows to nest.
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = block_comment_end(bytes, i),
            b'$' => {
                // `$tag$ ... $tag$` dollar-quoted body — unless the `$` is
                // inside an identifier, where PostgreSQL allows it — or a `$N`
                // placeholder.
                let in_identifier = i > 0 && is_identifier_byte(bytes[i - 1]);
                if !in_identifier {
                    if let Some(end) = dollar_quote_end(bytes, i) {
                        i = end;
                        continue;
                    }
                }

                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j == i + 1 {
                    // A bare `$` that starts nothing; left as it is.
                    i += 1;
                    continue;
                }
                let n: usize = sql[i + 1..j]
                    .parse()
                    .map_err(|_| format!("invalid parameter placeholder '{}'", &sql[i..j]))?;
                if n == 0 || n > params.len() {
                    return Err(format!(
                        "bind message supplies {} parameters, but statement requires ${}",
                        params.len(),
                        n
                    ));
                }
                out.push_str(&sql[copied..i]);
                // `$n::timestamptz` / `$n::timestamp` on a value sent as untyped
                // text says how to read it, and the cast is consumed below:
                // read at that type now (`push_text_param_literal`), or a
                // `timestamp` column would drop the zone a `::timestamptz`
                // value asked to be converted by.
                let param = &params[n - 1];
                let cast_type = (param.oid == 0 && param.format == 0)
                    .then(|| placeholder_cast(bytes, j))
                    .flatten()
                    .and_then(|name| {
                        if name.eq_ignore_ascii_case("timestamptz") {
                            Some(OID_TIMESTAMPTZ)
                        } else if name.eq_ignore_ascii_case("timestamp") {
                            Some(OID_TIMESTAMP)
                        } else {
                            None
                        }
                    });
                match cast_type {
                    Some(oid) => push_param_literal(&mut out, &BoundParam { oid, ..param.clone() })?,
                    None => push_param_literal(&mut out, param)?,
                }
                i = j;
                let mut consumed_cast: Option<&str> = None;

                // A cast written on the placeholder goes with it.
                //
                // Clients pin a bound value's type in SQL — `$6::text`,
                // `$n::jsonb`, `$n::bigint` — whenever the parameter's type
                // would otherwise be ambiguous. A whole codec can be built on
                // it: bind everything as a string and let the cast say what it
                // is. That is correct SQL and PostgreSQL resolves it.
                //
                // Substituting the value here leaves the cast attached to a
                // literal, and every parser downstream of this point then sees
                // a token that is not the value: `'Jordan Patel'::text` stored
                // whole, quotes and all; `NULL::jsonb` failing the `NULL` test
                // that plain `NULL` passes, so a nullable document column
                // rejected the row as a schema mismatch; and `'{…}'::jsonb`
                // handed to a JSON parser that stopped at the leading quote.
                // Each of those was a separate report against a different
                // column type, and all of them are this one line.
                //
                // The cast is redundant once the value is inlined — the column
                // it is being written to, or compared against, supplies the
                // type. Only a cast *directly* on the placeholder is consumed;
                // one written on a column or an expression is left alone,
                // because there it can change what the statement means.
                if bytes.get(i) == Some(&b':') && bytes.get(i + 1) == Some(&b':') {
                    let mut k = i + 2;
                    while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                        k += 1;
                    }
                    let name_start = k;
                    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
                    {
                        k += 1;
                    }
                    // A bare type name only. A parameterised or array type —
                    // `numeric(10,2)`, `text[]` — is left in place rather than
                    // half-consumed, which would strand punctuation.
                    if k > name_start && !matches!(bytes.get(k), Some(b'(') | Some(b'[')) {
                        consumed_cast = Some(&sql[name_start..k]);
                        i = k;
                    }
                }
                // A value typed text that ends in a space keeps its type: a
                // `character(n)` column compares with text exactly, trailing
                // spaces kept, and with an untyped literal blank-padded
                // (`'ab  '` is `'ab'`). Typed by a `::text` cast, or by the
                // client declaring the parameter text (sqlx and
                // tokio-postgres bind strings so) with no cast to say
                // otherwise. Without the space the two readings agree.
                let typed_text = match consumed_cast {
                    Some(cast) => cast.eq_ignore_ascii_case("text"),
                    None => params[n - 1].oid == OID_TEXT,
                };
                if typed_text && params[n - 1].value.as_deref().is_some_and(|v| v.last() == Some(&b' ')) {
                    out.push_str("::text");
                }
                copied = i;
            }
            // A lone `-` or `/`.
            _ => i += 1,
        }
    }

    out.push_str(&sql[copied..]);
    Ok(out)
}

/// The text of each parameter `inline_params` writes unquoted because it is
/// untyped text that looks like a number: what the statement's text encoders
/// must store as sent rather than read as a constant written in the
/// statement (`value_codec::written_constant_text`).
pub fn untyped_number_texts(params: &[BoundParam]) -> Vec<String> {
    params
        .iter()
        .filter(|p| p.oid == 0 && p.format == 0)
        .filter_map(|p| std::str::from_utf8(p.value.as_deref()?).ok())
        .filter(|text| is_numeric_literal(text))
        .map(str::to_string)
        .collect()
}

/// The bare type name of a `::type` cast directly after a placeholder ending
/// at `at`, as `inline_params` reads one before consuming it.
fn placeholder_cast(bytes: &[u8], at: usize) -> Option<&str> {
    if bytes.get(at) != Some(&b':') || bytes.get(at + 1) != Some(&b':') {
        return None;
    }
    let mut k = at + 2;
    while k < bytes.len() && (bytes[k] as char).is_whitespace() {
        k += 1;
    }
    let start = k;
    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
        k += 1;
    }
    (k > start).then(|| std::str::from_utf8(&bytes[start..k]).ok()).flatten()
}

/// The bytes that can begin a construct `inline_params` must look inside or
/// step over. Every other byte — including every byte of a multi-byte UTF-8
/// character, all of which are >= 0x80 — is copied as it stands.
static SQL_SPECIAL: [bool; 256] = {
    let mut table = [false; 256];
    table[b'\'' as usize] = true;
    table[b'"' as usize] = true;
    table[b'-' as usize] = true;
    table[b'/' as usize] = true;
    table[b'$' as usize] = true;
    table
};

/// A byte that continues an identifier, so a `$` or a quote right after it
/// belongs to (or follows) a name rather than opening something.
#[inline]
fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// True when the `'` at `quote` opens an escape string: `E'…'` / `e'…'`, with
/// the `E` a word of its own rather than the end of an identifier.
#[inline]
fn is_escape_string_quote(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'E' | b'e')
        && (quote < 2 || !(is_identifier_byte(bytes[quote - 2]) || bytes[quote - 2] == b'$'))
}

/// Index just past the string literal opening at `start` (or the end of the
/// text when it is unterminated). In an escape string a backslash escapes
/// whatever byte follows it.
fn string_literal_end(bytes: &[u8], start: usize, escapes: bool) -> usize {
    let mut i = start + 1;
    loop {
        let found = if escapes {
            memchr::memchr2(b'\'', b'\\', &bytes[i..])
        } else {
            memchr::memchr(b'\'', &bytes[i..])
        };
        let Some(p) = found else {
            return bytes.len();
        };
        i += p;
        if bytes[i] == b'\\' {
            i = (i + 2).min(bytes.len());
            continue;
        }
        // A quote: doubled, it is an escaped quote and the literal goes on.
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return i + 1;
    }
}

/// Index just past the `quote`-delimited run opening at `start`, where a
/// doubled quote stands for itself.
fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while let Some(p) = memchr::memchr(quote, &bytes[i..]) {
        i += p;
        if bytes.get(i + 1) == Some(&quote) {
            i += 2;
            continue;
        }
        return i + 1;
    }
    bytes.len()
}

/// Index just past the (possibly nested) block comment opening at `start`.
fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start + 2;
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

#[cfg(test)]
#[inline]
fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// If a dollar-quoted string opens at `start`, returns the index just past its
/// closing tag. Returns `None` when this `$` is not a dollar quote.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        // A tag cannot begin with a digit; `$1` is a placeholder, not a tag.
        if j == start + 1 && bytes[j].is_ascii_digit() {
            return None;
        }
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None;
    }
    let tag = &bytes[start..=j];
    let mut k = j + 1;
    while k + tag.len() <= bytes.len() {
        if &bytes[k..k + tag.len()] == tag {
            return Some(k + tag.len());
        }
        k += 1;
    }
    // Unterminated: treat the rest of the statement as the body.
    Some(bytes.len())
}

/// Renders one bound parameter as a SQL literal.
pub fn param_to_literal(p: &BoundParam) -> Result<String, String> {
    let Some(raw) = p.value.as_deref() else {
        return Ok("NULL".to_string());
    };

    if p.format == 0 {
        let text = std::str::from_utf8(raw)
            .map_err(|_| "text parameter is not valid UTF-8".to_string())?;
        return Ok(text_param_literal(p.oid, text));
    }

    binary_param_literal(p.oid, raw)
}

/// [`param_to_literal`], appended to `out` — the form `inline_params` uses, so
/// the common text and string parameters are quoted straight into the
/// statement rather than into a `String` of their own first.
/// One bound value as `inline_params` writes it into the statement text.
pub fn param_literal(p: &BoundParam) -> Result<String, String> {
    let mut out = String::new();
    push_param_literal(&mut out, p)?;
    Ok(out)
}

fn push_param_literal(out: &mut String, p: &BoundParam) -> Result<(), String> {
    let Some(raw) = p.value.as_deref() else {
        out.push_str("NULL");
        return Ok(());
    };

    if p.format == 0 {
        let text = std::str::from_utf8(raw)
            .map_err(|_| "text parameter is not valid UTF-8".to_string())?;
        push_text_param_literal(out, p.oid, text);
        return Ok(());
    }

    match p.oid {
        OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME | OID_CHAR => {
            let s = std::str::from_utf8(raw)
                .map_err(|_| "binary text parameter is not valid UTF-8".to_string())?;
            push_quoted_literal(out, s);
            Ok(())
        }
        _ => {
            out.push_str(&binary_param_literal(p.oid, raw)?);
            Ok(())
        }
    }
}

/// Text-format parameters arrive already in their SQL rendering; only quoting
/// needs deciding, and that follows from the declared type.
fn text_param_literal(oid: i32, text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    push_text_param_literal(&mut out, oid, text);
    out
}

/// [`text_param_literal`], appended to `out`.
fn push_text_param_literal(out: &mut String, oid: i32, text: &str) {
    match oid {
        OID_BOOL => {
            let t = matches!(text, "t" | "true" | "TRUE" | "1" | "y" | "yes" | "on");
            out.push_str(if t { "1" } else { "0" });
        }
        OID_INT2 | OID_INT4 | OID_INT8 | OID_OID | OID_FLOAT4 | OID_FLOAT8 | OID_NUMERIC => {
            // Numeric shapes pass through; anything else is quoted so a
            // malformed value fails as a value rather than as syntax.
            if is_numeric_literal(text) {
                out.push_str(text);
            } else {
                push_quoted_literal(out, text);
            }
        }
        // A `timestamptz` or `timestamp` sent as text is read as that type
        // here, where the type is known, and inlined with no zone: what is
        // left means the same instant to whichever column or comparison
        // reads it. Inlined as written, the column decided: a `timestamp`
        // column dropped the `+02` of a `timestamptz` value (PostgreSQL
        // converts it, 08:00 for 10:00+02), and a `timestamptz` column
        // applied the zone of a `timestamp` value (PostgreSQL drops it).
        // Text that is not a timestamp (`infinity`) is left to the reader.
        OID_TIMESTAMPTZ | OID_TIMESTAMP => {
            let us = if oid == OID_TIMESTAMPTZ {
                crate::core::value_codec::parse_timestamp_us(text)
            } else {
                crate::core::value_codec::parse_timestamp_local_us(text)
            };
            // A `timestamptz` keeps an explicit `+00` (`zone_free_timestamp_literals`
            // says why): into a text column it is shown as PostgreSQL shows it.
            let rendered = us.map(|us| {
                let mut t = crate::core::value_codec::format_timestamp_us(us);
                if oid == OID_TIMESTAMPTZ {
                    t.push_str("+00");
                }
                t
            });
            match rendered {
                Some(t) if crate::core::value_codec::parse_timestamp_us(&t) == us => push_quoted_literal(out, &t),
                _ => push_quoted_literal(out, text),
            }
        }
        // An unspecified OID (0) is whatever the text looks like: sqlx sends
        // this for untyped placeholders.
        0 if is_numeric_literal(text) => out.push_str(text),
        _ => push_quoted_literal(out, text),
    }
}

fn binary_param_literal(oid: i32, raw: &[u8]) -> Result<String, String> {
    match oid {
        OID_BOOL => Ok(if raw.first().copied().unwrap_or(0) != 0 {
            "1"
        } else {
            "0"
        }
        .to_string()),
        OID_INT2 => Ok(be_i16(raw)?.to_string()),
        OID_INT4 | OID_OID => Ok(be_i32(raw)?.to_string()),
        OID_INT8 => Ok(be_i64(raw)?.to_string()),
        OID_FLOAT4 => Ok(fmt_f64(f32::from_bits(be_i32(raw)? as u32) as f64)),
        OID_FLOAT8 => Ok(fmt_f64(f64::from_bits(be_i64(raw)? as u64))),
        OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME | OID_CHAR => {
            let s = std::str::from_utf8(raw)
                .map_err(|_| "binary text parameter is not valid UTF-8".to_string())?;
            Ok(quote_literal(s))
        }
        OID_UUID => {
            if raw.len() != 16 {
                return Err("binary uuid parameter must be 16 bytes".to_string());
            }
            Ok(quote_literal(&format_uuid(raw)))
        }
        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            let us = be_i64(raw)? + PG_EPOCH_OFFSET_US;
            Ok(quote_literal(&format_timestamp_us(us)))
        }
        OID_DATE => {
            // Days since 2000-01-01.
            let days = be_i32(raw)? as i64;
            let us = days * 86_400_000_000 + PG_EPOCH_OFFSET_US;
            Ok(quote_literal(&format_timestamp_us(us)))
        }
        OID_TIME => {
            // Microseconds since midnight.
            let us = be_i64(raw)?;
            Ok(quote_literal(&format_time_us(us)))
        }
        OID_NUMERIC => Ok(decode_numeric(raw)?),
        // A `jsonb` parameter's binary form is a one-byte format version
        // followed by the document text; `json` is the text alone. The
        // outbound side already knew this — a document was rendered back to
        // the client this way — but the inbound side did not, so binding a
        // document as a parameter was refused outright with "unsupported
        // binary parameter type OID 3802", which is what every ORM sends for
        // a JSON column.
        OID_JSONB | OID_JSON => {
            let text = if oid == OID_JSONB {
                let (version, rest) = raw
                    .split_first()
                    .ok_or_else(|| "binary jsonb parameter is empty".to_string())?;
                if *version != 1 {
                    return Err(format!(
                        "unsupported jsonb binary format version {}",
                        version
                    ));
                }
                rest
            } else {
                raw
            };
            let s = std::str::from_utf8(text)
                .map_err(|_| "binary json parameter is not valid UTF-8".to_string())?;
            Ok(quote_literal(s))
        }
        OID_BYTEA => {
            // The shared nibble table rather than a `format!` per byte, which
            // allocated a String for every byte of the value.
            let mut s = Vec::with_capacity(raw.len() * 2 + 4);
            s.push(b'\'');
            crate::core::value_codec::format_bytea_into(raw, &mut s);
            s.push(b'\'');
            Ok(String::from_utf8(s).expect("a quote, \\x and hex digits are ASCII"))
        }
        // The types stored in PostgreSQL's own wire layout
        // (`pg_types`): a binary parameter *is* the stored form, so it is
        // rendered by the same formatter the result path uses, and the
        // literal then parses back to exactly those bytes — one codec, so the
        // binary and text binds of a value cannot store different bytes.
        OID_TIMETZ | OID_INTERVAL | OID_MACADDR | OID_MACADDR8 | OID_BIT | OID_VARBIT
        | OID_POINT | OID_LSEG | OID_PATH | OID_BOX | OID_POLYGON | OID_LINE | OID_CIRCLE
        | OID_INT4RANGE | OID_INT8RANGE | OID_NUMRANGE | OID_TSRANGE | OID_TSTZRANGE
        | OID_DATERANGE => {
            let text = wire_type_text(oid, raw)
                .ok_or_else(|| format!("binary parameter of type OID {} is malformed", oid))?;
            Ok(quote_literal(&text))
        }
        // `inet` / `cidr`: family (2 = IPv4, 3 = IPv6), prefix bits, the
        // is-cidr flag, the address length, then the address.
        OID_INET | OID_CIDR => {
            let text = inet_text(oid, raw)
                .ok_or_else(|| "binary inet parameter is malformed".to_string())?;
            Ok(quote_literal(&text))
        }
        // `money`: an `int8` count of cents (the scale `lc_monetary` gives
        // every locale this engine runs in), rendered as an exact amount.
        OID_MONEY => {
            let cents = be_i64(raw)?;
            let sign = if cents < 0 { "-" } else { "" };
            let magnitude = cents.unsigned_abs();
            Ok(format!("{}{}.{:02}", sign, magnitude / 100, magnitude % 100))
        }
        _ => match array_element_oid(oid) {
            Some(element) => binary_array_literal(element, raw),
            None => Err(format!("unsupported binary parameter type OID {}", oid)),
        },
    }
}

/// The text form of a value of one of the wire-layout types, from its binary
/// form — which is also its stored form.
fn wire_type_text(oid: i32, raw: &[u8]) -> Option<String> {
    use crate::core::pg_types as pg;
    let exact = |len: usize| (raw.len() == len).then_some(());
    match oid {
        OID_TIMETZ => {
            exact(pg::TIMETZ_LEN)?;
            pg::format_timetz(raw)
        }
        OID_INTERVAL => {
            exact(pg::INTERVAL_LEN)?;
            pg::format_interval(raw)
        }
        OID_MACADDR => {
            exact(pg::MACADDR_LEN)?;
            Some(pg::format_macaddr(raw))
        }
        OID_MACADDR8 => {
            exact(pg::MACADDR8_LEN)?;
            Some(pg::format_macaddr(raw))
        }
        OID_BIT | OID_VARBIT => pg::format_bit_string(raw),
        OID_POINT => {
            exact(pg::POINT_LEN)?;
            pg::format_point(raw)
        }
        OID_LSEG => {
            exact(pg::LSEG_LEN)?;
            pg::format_lseg(raw)
        }
        OID_BOX => {
            exact(pg::LSEG_LEN)?;
            pg::format_box(raw)
        }
        OID_LINE => {
            exact(pg::LINE_LEN)?;
            pg::format_line(raw)
        }
        OID_CIRCLE => {
            exact(24)?;
            pg::format_circle(raw)
        }
        OID_PATH => pg::format_path(raw),
        OID_POLYGON => pg::format_polygon(raw),
        OID_INT4RANGE => pg::format_range(&DbType::INT4RANGE, raw),
        OID_INT8RANGE => pg::format_range(&DbType::INT8RANGE, raw),
        OID_NUMRANGE => pg::format_range(&DbType::NUMRANGE, raw),
        OID_TSRANGE => pg::format_range(&DbType::TSRANGE, raw),
        OID_TSTZRANGE => pg::format_range(&DbType::TSTZRANGE, raw),
        OID_DATERANGE => pg::format_range(&DbType::DATERANGE, raw),
        _ => None,
    }
}

/// `inet` / `cidr` binary to `addr[/bits]`. A host address (all bits) is
/// written without its prefix for `inet`, as PostgreSQL prints it; `cidr`
/// always carries one.
fn inet_text(oid: i32, raw: &[u8]) -> Option<String> {
    let (&family, rest) = raw.split_first()?;
    let (&bits, rest) = rest.split_first()?;
    let (_is_cidr, rest) = rest.split_first()?;
    let (&len, addr) = rest.split_first()?;
    if addr.len() != len as usize {
        return None;
    }
    let (ip, max): (std::net::IpAddr, u8) = match (family, len) {
        (2, 4) => (std::net::Ipv4Addr::from(<[u8; 4]>::try_from(addr).ok()?).into(), 32),
        (3, 16) => (std::net::Ipv6Addr::from(<[u8; 16]>::try_from(addr).ok()?).into(), 128),
        _ => return None,
    };
    if bits > max {
        return None;
    }
    Some(if bits == max && oid == OID_INET {
        ip.to_string()
    } else {
        format!("{}/{}", ip, bits)
    })
}

/// The element type of an array OID, or `None` if this is not an array.
///
/// Listed rather than derived: PostgreSQL's array OIDs bear no arithmetic
/// relation to their element's, and guessing one would decode a value as
/// something it is not.
fn array_element_oid(oid: i32) -> Option<i32> {
    Some(match oid {
        1000 => OID_BOOL,
        1005 => OID_INT2,
        1007 => OID_INT4,
        1016 => OID_INT8,
        1009 => OID_TEXT,
        1015 => OID_VARCHAR,
        1014 => OID_BPCHAR,
        1021 => OID_FLOAT4,
        1022 => OID_FLOAT8,
        1231 => OID_NUMERIC,
        1115 => OID_TIMESTAMP,
        1185 => OID_TIMESTAMPTZ,
        1182 => OID_DATE,
        1183 => OID_TIME,
        2951 => OID_UUID,
        1001 => OID_BYTEA,
        1002 => OID_CHAR,
        1003 => OID_NAME,
        1028 => OID_OID,
        199 => OID_JSON,
        3807 => OID_JSONB,
        1041 => OID_INET,
        651 => OID_CIDR,
        1040 => OID_MACADDR,
        775 => OID_MACADDR8,
        791 => OID_MONEY,
        1187 => OID_INTERVAL,
        1270 => OID_TIMETZ,
        1561 => OID_BIT,
        1563 => OID_VARBIT,
        1017 => OID_POINT,
        1018 => OID_LSEG,
        1019 => OID_PATH,
        1020 => OID_BOX,
        1027 => OID_POLYGON,
        629 => OID_LINE,
        719 => OID_CIRCLE,
        3905 => OID_INT4RANGE,
        3927 => OID_INT8RANGE,
        3907 => OID_NUMRANGE,
        3909 => OID_TSRANGE,
        3911 => OID_TSTZRANGE,
        3913 => OID_DATERANGE,
        _ => return None,
    })
}

/// Renders a binary array parameter as this engine's array literal.
///
/// PostgreSQL's binary array is a header — dimension count, a null flag, the
/// element OID, then a length and lower bound per dimension — followed by each
/// element as a four-byte length (-1 for NULL) and that many bytes. Each
/// element is decoded by the same code that decodes it on its own, so an array
/// of a type gains nothing to go wrong that the type itself does not have.
///
/// Undecoded, a bound `uuid[]` reached the client as "unsupported binary
/// parameter type OID 2951" and the statement simply did not run.
fn binary_array_literal(element_oid: i32, raw: &[u8]) -> Result<String, String> {
    if raw.len() < 12 {
        return Err("binary array parameter is too short to hold its header".to_string());
    }
    let dimensions = i32::from_be_bytes(raw[0..4].try_into().expect("4 bytes"));
    if dimensions == 0 {
        return Ok("'{}'".to_string());
    }
    if dimensions != 1 {
        return Err(format!(
            "binary array parameter has {} dimensions; this engine stores one",
            dimensions
        ));
    }

    // The header's element OID is authoritative; the array OID only told us
    // what to expect.
    let stated = i32::from_be_bytes(raw[8..12].try_into().expect("4 bytes"));
    let element_oid = if stated != 0 { stated } else { element_oid };

    let mut at = 12;
    if raw.len() < at + 8 {
        return Err("binary array parameter is missing its dimension".to_string());
    }
    let count = i32::from_be_bytes(raw[at..at + 4].try_into().expect("4 bytes"));
    at += 8; // length and lower bound

    let mut parts: Vec<String> = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count.max(0) {
        if raw.len() < at + 4 {
            return Err("binary array parameter ends inside an element".to_string());
        }
        let len = i32::from_be_bytes(raw[at..at + 4].try_into().expect("4 bytes"));
        at += 4;
        if len < 0 {
            parts.push("NULL".to_string());
            continue;
        }
        let len = len as usize;
        if raw.len() < at + len {
            return Err("binary array parameter ends inside an element".to_string());
        }
        let decoded = binary_param_literal(element_oid, &raw[at..at + len])?;
        at += len;

        // The element decoders return SQL literals; an array literal quotes its
        // own elements, so the outer quoting is unwound first.
        let bare = unquote_sql_literal(&decoded);
        let mut quoted = String::with_capacity(bare.len() + 2);
        quoted.push('"');
        for c in bare.chars() {
            if c == '"' || c == '\\' {
                quoted.push('\\');
            }
            quoted.push(c);
        }
        quoted.push('"');
        parts.push(quoted);
    }

    Ok(quote_literal(&format!("{{{}}}", parts.join(","))))
}

/// Strips one layer of SQL quoting, collapsing doubled quotes.
fn unquote_sql_literal(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    trimmed.to_string()
}

pub(crate) fn is_numeric_literal(text: &str) -> bool {
    let t = text.trim();
    if t.is_empty() {
        return false;
    }
    let body = t.strip_prefix(['-', '+']).unwrap_or(t);
    if body.is_empty() {
        return false;
    }
    let mut seen_digit = false;
    let mut seen_dot = false;
    let mut seen_exp = false;
    let mut prev_exp = false;
    for c in body.chars() {
        match c {
            '0'..='9' => {
                seen_digit = true;
                prev_exp = false;
            }
            '.' if !seen_dot && !seen_exp => {
                seen_dot = true;
                prev_exp = false;
            }
            'e' | 'E' if seen_digit && !seen_exp => {
                seen_exp = true;
                prev_exp = true;
            }
            '-' | '+' if prev_exp => prev_exp = false,
            _ => return false,
        }
    }
    seen_digit
}

/// Single-quotes a value, doubling any quote it contains.
pub fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    push_quoted_literal(&mut out, s);
    out
}

/// [`quote_literal`], appended to `out`: the text between quotes is copied in
/// runs found with `memchr` rather than a character at a time.
pub fn push_quoted_literal(out: &mut String, s: &str) {
    out.reserve(s.len() + 2);
    out.push('\'');
    let mut start = 0usize;
    for at in memchr::memchr_iter(b'\'', s.as_bytes()) {
        // `at` is an ASCII quote, so both slices end on character boundaries.
        out.push_str(&s[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&s[start..]);
    out.push('\'');
}

fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.is_finite() && v.abs() < 1e15 {
        format!("{:.1}", v)
    } else {
        format!("{}", v)
    }
}

fn be_i16(raw: &[u8]) -> Result<i16, String> {
    if raw.len() != 2 {
        return Err("binary int2 parameter must be 2 bytes".to_string());
    }
    Ok(i16::from_be_bytes([raw[0], raw[1]]))
}

fn be_i32(raw: &[u8]) -> Result<i32, String> {
    if raw.len() != 4 {
        return Err("binary int4 parameter must be 4 bytes".to_string());
    }
    Ok(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn be_i64(raw: &[u8]) -> Result<i64, String> {
    if raw.len() != 8 {
        return Err("binary int8 parameter must be 8 bytes".to_string());
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(raw);
    Ok(i64::from_be_bytes(b))
}

/// Renders the canonical 8-4-4-4-12 form of a 16-byte UUID.
pub fn format_uuid(raw: &[u8]) -> String {
    debug_assert_eq!(raw.len(), 16);
    let mut s = String::with_capacity(36);
    for (i, b) in raw.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Parses a UUID's text form into 16 bytes: `value_codec::parse_uuid`, so a
/// bound parameter and a literal accept the same spellings (braces included).
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    crate::core::value_codec::parse_uuid(text)
}

/// Postgres binary `numeric`: i16 digit count, i16 weight, i16 sign,
/// i16 display scale, then base-10000 digits most-significant first.
fn decode_numeric(raw: &[u8]) -> Result<String, String> {
    if raw.len() < 8 {
        return Err("binary numeric parameter is too short".to_string());
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]) as i32;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = i16::from_be_bytes([raw[6], raw[7]]) as usize;

    if sign == 0xC000 {
        return Ok("'NaN'".to_string());
    }
    if raw.len() < 8 + ndigits * 2 {
        return Err("binary numeric parameter is truncated".to_string());
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|i| u16::from_be_bytes([raw[8 + i * 2], raw[9 + i * 2]]))
        .collect();

    // Integer part: base-10000 groups at weights >= 0.
    let mut int_part = String::new();
    for w in 0..=weight.max(-1) {
        if w > weight {
            break;
        }
        let d = digits.get(w as usize).copied().unwrap_or(0);
        if int_part.is_empty() {
            int_part.push_str(&d.to_string());
        } else {
            int_part.push_str(&format!("{:04}", d));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }

    // Fractional part: groups after the point, padded out to the display scale.
    let mut frac = String::new();
    let mut w = weight + 1;
    while frac.len() < dscale {
        let d = if w < 0 {
            0
        } else {
            digits.get(w as usize).copied().unwrap_or(0)
        };
        frac.push_str(&format!("{:04}", d));
        w += 1;
    }
    frac.truncate(dscale);

    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&int_part);
    if !frac.is_empty() {
        out.push('.');
        out.push_str(&frac);
    }
    Ok(out)
}

/// Renders microseconds-since-Unix-epoch as `YYYY-MM-DD HH:MM:SS[.ffffff]`.
pub fn format_timestamp_us(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let base = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    );
    if frac == 0 {
        base
    } else {
        format!("{}.{:06}", base, frac)
            .trim_end_matches('0')
            .to_string()
    }
}

fn format_time_us(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let base = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    );
    if frac == 0 {
        base
    } else {
        format!("{}.{:06}", base, frac)
            .trim_end_matches('0')
            .to_string()
    }
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to (y, m, d),
/// proleptic Gregorian, valid across the whole i64 range we care about.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Inverse of [`civil_from_days`].
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `timestamptz` value sent as text goes in as its UTC time and a
    /// `timestamp` one as its wall-clock time, neither with a zone left for
    /// the column to read its own way; `$n::timestamptz` on an untyped value
    /// says the same.
    #[test]
    fn a_timestamp_text_parameter_is_read_at_its_declared_type() {
        let zoned = "2024-01-01 10:00:00+02";
        assert_eq!(param_to_literal(&text(OID_TIMESTAMPTZ, zoned)).unwrap(), "'2024-01-01 08:00:00+00'");
        assert_eq!(param_to_literal(&text(OID_TIMESTAMP, zoned)).unwrap(), "'2024-01-01 10:00:00'");
        assert_eq!(param_to_literal(&text(OID_TIMESTAMPTZ, "infinity")).unwrap(), "'infinity'");
        assert_eq!(
            inline_params("SELECT $1::timestamptz, $2::timestamp, $3::text", &[text(0, zoned), text(0, zoned), text(0, zoned)])
                .unwrap(),
            "SELECT '2024-01-01 08:00:00+00', '2024-01-01 10:00:00', '2024-01-01 10:00:00+02'"
        );
    }

    fn text(oid: i32, s: &str) -> BoundParam {
        BoundParam {
            value: Some(s.as_bytes().to_vec()),
            format: 0,
            oid,
        }
    }
    fn binary(oid: i32, b: Vec<u8>) -> BoundParam {
        BoundParam {
            value: Some(b),
            format: 1,
            oid,
        }
    }
    fn null(oid: i32) -> BoundParam {
        BoundParam {
            value: None,
            format: 1,
            oid,
        }
    }

    #[test]
    fn inlines_positional_params_in_order() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2";
        let out = inline_params(sql, &[text(OID_INT4, "7"), text(OID_TEXT, "hi")]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 7 AND b = 'hi'");
    }

    #[test]
    fn reuses_a_param_referenced_twice() {
        let out = inline_params("SELECT $1, $1", &[text(OID_INT4, "3")]).unwrap();
        assert_eq!(out, "SELECT 3, 3");
    }

    #[test]
    fn param_beyond_ten_is_read_as_one_number() {
        let params: Vec<BoundParam> = (1..=12).map(|i| text(OID_INT4, &i.to_string())).collect();
        let out = inline_params("SELECT $12, $1", &params).unwrap();
        assert_eq!(out, "SELECT 12, 1");
    }

    #[test]
    fn leaves_placeholders_inside_literals_alone() {
        let out = inline_params("SELECT '$1', \"$1\", $1", &[text(OID_INT4, "9")]).unwrap();
        assert_eq!(out, "SELECT '$1', \"$1\", 9");
    }

    #[test]
    fn leaves_placeholders_inside_comments_alone() {
        let out = inline_params("SELECT $1 -- $2\n, $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, "SELECT 5 -- $2\n, 5");
        let out = inline_params("SELECT /* $2 */ $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, "SELECT /* $2 */ 5");
    }

    #[test]
    fn leaves_dollar_quoted_bodies_alone() {
        let out = inline_params("SELECT $tag$ $1 $tag$, $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, "SELECT $tag$ $1 $tag$, 5");
    }

    #[test]
    fn leaves_placeholders_inside_escape_strings_alone() {
        // In `E'…'` a backslash escapes the next character, so `\'` does not
        // end the literal. Read as a standard string, the literal ended there
        // and the `$1` that is still inside it was substituted — a parameter
        // value spliced into the middle of a string constant.
        let out = inline_params(r"SELECT E'it\'s $1', $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, r"SELECT E'it\'s $1', 5");
        let out = inline_params(r"SELECT e'\\', $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, r"SELECT e'\\', 5");
        let out = inline_params(r"SELECT E'a''b\' $1' || $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, r"SELECT E'a''b\' $1' || 5");
        // An identifier ending in E is not an escape-string prefix: a standard
        // string follows, where a backslash is just a backslash.
        let out = inline_params(r"SELECT name'\' , $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, r"SELECT name'\' , 5");
    }

    #[test]
    fn a_dollar_inside_an_identifier_does_not_open_a_dollar_quote() {
        // `a$b$` is an identifier (PostgreSQL allows `$` after the first
        // character). Read as the opening `$b$` of a dollar quote, it swallowed
        // the rest of the statement and the placeholders in it stayed unbound.
        let out = inline_params("SELECT a$b$ FROM t WHERE x = $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, "SELECT a$b$ FROM t WHERE x = 5");
        let out = inline_params("SELECT $$ $1 $$, $1", &[text(OID_INT4, "5")]).unwrap();
        assert_eq!(out, "SELECT $$ $1 $$, 5");
    }

    #[test]
    fn escapes_quotes_in_text_params() {
        let out = inline_params("SELECT $1", &[text(OID_TEXT, "O'Brien")]).unwrap();
        assert_eq!(out, "SELECT 'O''Brien'");
    }

    #[test]
    fn a_quote_in_a_param_cannot_terminate_the_statement() {
        let out = inline_params(
            "SELECT * FROM t WHERE name = $1",
            &[text(OID_TEXT, "x' OR '1'='1")],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = 'x'' OR ''1''=''1'");
    }

    #[test]
    fn null_param_renders_as_null_keyword() {
        let out = inline_params("SELECT $1", &[null(OID_TEXT)]).unwrap();
        assert_eq!(out, "SELECT NULL");
    }

    #[test]
    fn missing_param_is_an_error_not_a_panic() {
        assert!(inline_params("SELECT $2", &[text(OID_INT4, "1")]).is_err());
        assert!(inline_params("SELECT $0", &[text(OID_INT4, "1")]).is_err());
    }

    #[test]
    fn decodes_binary_integers() {
        assert_eq!(
            param_to_literal(&binary(OID_INT2, 300i16.to_be_bytes().to_vec())).unwrap(),
            "300"
        );
        assert_eq!(
            param_to_literal(&binary(OID_INT4, (-7i32).to_be_bytes().to_vec())).unwrap(),
            "-7"
        );
        assert_eq!(
            param_to_literal(&binary(OID_INT8, 9_000_000_000i64.to_be_bytes().to_vec())).unwrap(),
            "9000000000"
        );
    }

    #[test]
    fn decodes_binary_floats() {
        assert_eq!(
            param_to_literal(&binary(OID_FLOAT8, 1.5f64.to_be_bytes().to_vec())).unwrap(),
            "1.5"
        );
        assert_eq!(
            param_to_literal(&binary(OID_FLOAT4, 2.0f32.to_be_bytes().to_vec())).unwrap(),
            "2.0"
        );
    }

    #[test]
    fn decodes_binary_bool() {
        assert_eq!(param_to_literal(&binary(OID_BOOL, vec![1])).unwrap(), "1");
        assert_eq!(param_to_literal(&binary(OID_BOOL, vec![0])).unwrap(), "0");
    }

    #[test]
    fn decodes_binary_uuid_to_canonical_text() {
        let raw: Vec<u8> = (0u8..16).collect();
        assert_eq!(
            param_to_literal(&binary(OID_UUID, raw)).unwrap(),
            "'00010203-0405-0607-0809-0a0b0c0d0e0f'"
        );
    }

    #[test]
    fn uuid_text_round_trips_through_bytes() {
        let s = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        let bytes = parse_uuid(s).unwrap();
        assert_eq!(format_uuid(&bytes), s);
        assert_eq!(parse_uuid(&s.replace('-', "")).unwrap(), bytes);
        assert!(parse_uuid("not-a-uuid").is_none());
        assert!(parse_uuid("3f2504e0-4f89-11d3-9a0c-0305e82c33").is_none());
    }

    #[test]
    fn decodes_binary_timestamp_from_the_2000_epoch() {
        // 2000-01-01 00:00:00 is exactly zero in Postgres' timestamp encoding.
        assert_eq!(
            param_to_literal(&binary(OID_TIMESTAMPTZ, 0i64.to_be_bytes().to_vec())).unwrap(),
            "'2000-01-01 00:00:00'"
        );
        // One day and 1.5 seconds later.
        let us = 86_400_000_000i64 + 1_500_000;
        assert_eq!(
            param_to_literal(&binary(OID_TIMESTAMP, us.to_be_bytes().to_vec())).unwrap(),
            "'2000-01-02 00:00:01.5'"
        );
    }

    #[test]
    fn decodes_binary_date_as_midnight() {
        assert_eq!(
            param_to_literal(&binary(OID_DATE, 0i32.to_be_bytes().to_vec())).unwrap(),
            "'2000-01-01 00:00:00'"
        );
    }

    #[test]
    fn decodes_binary_numeric() {
        // 1234.5678 => ndigits 3, weight 0, sign +, dscale 4, digits [1234, 5678, 0]
        let mut raw = Vec::new();
        raw.extend_from_slice(&3i16.to_be_bytes());
        raw.extend_from_slice(&0i16.to_be_bytes());
        raw.extend_from_slice(&0u16.to_be_bytes());
        raw.extend_from_slice(&4i16.to_be_bytes());
        raw.extend_from_slice(&1234u16.to_be_bytes());
        raw.extend_from_slice(&5678u16.to_be_bytes());
        raw.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(
            param_to_literal(&binary(OID_NUMERIC, raw)).unwrap(),
            "1234.5678"
        );
    }

    #[test]
    fn decodes_negative_binary_numeric() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i16.to_be_bytes());
        raw.extend_from_slice(&0i16.to_be_bytes());
        raw.extend_from_slice(&0x4000u16.to_be_bytes());
        raw.extend_from_slice(&2i16.to_be_bytes());
        raw.extend_from_slice(&42u16.to_be_bytes());
        assert_eq!(
            param_to_literal(&binary(OID_NUMERIC, raw)).unwrap(),
            "-42.00"
        );
    }

    #[test]
    fn text_bool_params_become_engine_booleans() {
        assert_eq!(param_to_literal(&text(OID_BOOL, "t")).unwrap(), "1");
        assert_eq!(param_to_literal(&text(OID_BOOL, "f")).unwrap(), "0");
    }

    #[test]
    fn civil_day_conversion_round_trips() {
        for &(y, m, d) in &[
            (1970, 1, 1),
            (2000, 1, 1),
            (2026, 8, 20),
            (1999, 12, 31),
            (2400, 2, 29),
        ] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), (y, m, d), "{}-{}-{}", y, m, d);
        }
    }
}

#[cfg(test)]
mod binary_array_tests {
    use super::*;

    /// Builds PostgreSQL's binary array wire form.
    fn array(element_oid: i32, elements: &[Option<Vec<u8>>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes()); // one dimension
        out.extend_from_slice(&0i32.to_be_bytes()); // no null bitmap flag
        out.extend_from_slice(&element_oid.to_be_bytes());
        out.extend_from_slice(&(elements.len() as i32).to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        for element in elements {
            match element {
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(bytes) => {
                    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    #[test]
    fn a_uuid_array_decodes() {
        // OID 2951. Undecoded it reached the client as "unsupported binary
        // parameter type OID 2951" and the statement never ran.
        let one = [0x11u8; 16];
        let two = [0x22u8; 16];
        let raw = array(OID_UUID, &[Some(one.to_vec()), Some(two.to_vec())]);
        let literal = binary_param_literal(2951, &raw).expect("uuid[] decodes");
        assert!(
            literal.contains("11111111-1111-1111-1111-111111111111"),
            "{}",
            literal
        );
        assert!(
            literal.contains("22222222-2222-2222-2222-222222222222"),
            "{}",
            literal
        );
        assert!(
            literal.starts_with('\'') && literal.ends_with('\''),
            "{}",
            literal
        );
    }

    #[test]
    fn an_int_array_keeps_its_order_and_nulls() {
        let raw = array(
            OID_INT4,
            &[
                Some(1i32.to_be_bytes().to_vec()),
                None,
                Some(3i32.to_be_bytes().to_vec()),
            ],
        );
        assert_eq!(
            binary_param_literal(1007, &raw).unwrap(),
            "'{\"1\",NULL,\"3\"}'"
        );
    }

    #[test]
    fn an_empty_array_is_empty() {
        let raw = {
            let mut out = Vec::new();
            out.extend_from_slice(&0i32.to_be_bytes());
            out.extend_from_slice(&0i32.to_be_bytes());
            out.extend_from_slice(&OID_TEXT.to_be_bytes());
            out
        };
        assert_eq!(binary_param_literal(1009, &raw).unwrap(), "'{}'");
    }

    #[test]
    fn a_text_element_containing_a_quote_survives() {
        let raw = array(OID_TEXT, &[Some(b"has \"quote\"".to_vec())]);
        let literal = binary_param_literal(1009, &raw).unwrap();
        assert!(literal.contains("\\\"quote\\\""), "{}", literal);
    }

    #[test]
    fn money_inet_and_cidr_decode_to_their_text_forms() {
        assert_eq!(binary_param_literal(OID_MONEY, &(-123_456i64).to_be_bytes()).unwrap(), "-1234.56");
        assert_eq!(binary_param_literal(OID_MONEY, &5i64.to_be_bytes()).unwrap(), "0.05");
        assert_eq!(binary_param_literal(OID_INET, &[2, 32, 0, 4, 10, 1, 2, 3]).unwrap(), "'10.1.2.3'");
        assert_eq!(binary_param_literal(OID_CIDR, &[2, 32, 1, 4, 10, 1, 2, 3]).unwrap(), "'10.1.2.3/32'");
        assert!(binary_param_literal(OID_INET, &[2, 33, 0, 4, 10, 1, 2, 3]).is_err());
        assert!(binary_param_literal(OID_POINT, &[0; 15]).is_err(), "wrong width is refused");
    }

    #[test]
    fn arrays_of_the_wire_layout_types_decode_element_by_element() {
        // inet[]: two addresses.
        let raw = array(OID_INET, &[Some(vec![2, 32, 0, 4, 10, 0, 0, 1]), Some(vec![2, 8, 0, 4, 10, 0, 0, 0])]);
        assert_eq!(binary_param_literal(1041, &raw).unwrap(), "'{\"10.0.0.1\",\"10.0.0.0/8\"}'");
        // date[] with a NULL.
        let raw = array(OID_DATE, &[Some(0i32.to_be_bytes().to_vec()), None]);
        assert_eq!(binary_param_literal(1182, &raw).unwrap(), "'{\"2000-01-01 00:00:00\",NULL}'");
        // point[].
        let point: Vec<u8> = [1.5f64, -2.0].iter().flat_map(|v| v.to_be_bytes()).collect();
        let raw = array(OID_POINT, &[Some(point)]);
        assert_eq!(binary_param_literal(1017, &raw).unwrap(), "'{\"(1.5,-2)\"}'");
        // jsonb[]: the version byte is unwrapped per element, and the
        // document's own quotes are escaped inside the array literal.
        let raw = array(OID_JSONB, &[Some([vec![1u8], b"{\"k\":1}".to_vec()].concat())]);
        assert_eq!(binary_param_literal(3807, &raw).unwrap(), "'{\"{\\\"k\\\":1}\"}'");
        // interval[].
        let iv = [60_000_000i64.to_be_bytes().to_vec(), 0i32.to_be_bytes().to_vec(), 0i32.to_be_bytes().to_vec()].concat();
        let raw = array(OID_INTERVAL, &[Some(iv)]);
        assert!(binary_param_literal(1187, &raw).unwrap().contains("00:01:00"));
    }

    #[test]
    fn a_type_that_is_not_an_array_is_still_refused() {
        assert!(binary_param_literal(9999, b"x").is_err());
    }
}

/// The byte-at-a-time `inline_params` this module shipped with — plus the
/// E-string and identifier-`$` fixes — kept to check the run-copying rewrite
/// against.
#[cfg(test)]
mod inline_reference {
    use super::*;

    fn inline_params_reference(sql: &str, params: &[BoundParam]) -> Result<String, String> {
        if params.is_empty() || !sql.as_bytes().contains(&b'$') {
            return Ok(sql.to_string());
        }

        let bytes = sql.as_bytes();
        let mut out = String::with_capacity(sql.len() + params.len() * 16);
        let mut i = 0usize;

        while i < bytes.len() {
            let c = bytes[i];
            match c {
                // Single-quoted string literal; '' is an escaped quote.
                b'\'' => {
                    let escapes = is_escape_string_quote(bytes, i);
                    let start = i;
                    i += 1;
                    while i < bytes.len() {
                        if escapes && bytes[i] == b'\\' {
                            i = (i + 2).min(bytes.len());
                            continue;
                        }
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
                    out.push_str(&sql[start..i]);
                }
                // Quoted identifier; "" is an escaped quote.
                b'"' => {
                    let start = i;
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == b'"' {
                            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                                i += 2;
                                continue;
                            }
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    out.push_str(&sql[start..i]);
                }
                // Line comment.
                b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    let start = i;
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                    out.push_str(&sql[start..i]);
                }
                // Block comment, which Postgres allows to nest.
                b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                    let start = i;
                    let mut depth = 1usize;
                    i += 2;
                    while i < bytes.len() && depth > 0 {
                        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                            depth += 1;
                            i += 2;
                        } else if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                            depth -= 1;
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    out.push_str(&sql[start..i]);
                }
                b'$' => {
                    // `$tag$ ... $tag$` dollar-quoted body, or a `$N` placeholder.
                    if let Some(end) = (i == 0 || !is_identifier_byte(bytes[i - 1]))
                        .then(|| dollar_quote_end(bytes, i))
                        .flatten()
                    {
                        out.push_str(&sql[i..end]);
                        i = end;
                        continue;
                    }

                    let mut j = i + 1;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j == i + 1 {
                        // A bare `$` that starts nothing; emit it unchanged.
                        out.push('$');
                        i += 1;
                        continue;
                    }
                    let n: usize = sql[i + 1..j]
                        .parse()
                        .map_err(|_| format!("invalid parameter placeholder '{}'", &sql[i..j]))?;
                    if n == 0 || n > params.len() {
                        return Err(format!(
                            "bind message supplies {} parameters, but statement requires ${}",
                            params.len(),
                            n
                        ));
                    }
                    out.push_str(&param_to_literal(&params[n - 1])?);
                    i = j;

                    // A cast written on the placeholder goes with it.
                    //
                    // Clients pin a bound value's type in SQL — `$6::text`,
                    // `$n::jsonb`, `$n::bigint` — whenever the parameter's type
                    // would otherwise be ambiguous. A whole codec can be built on
                    // it: bind everything as a string and let the cast say what it
                    // is. That is correct SQL and PostgreSQL resolves it.
                    //
                    // Substituting the value here leaves the cast attached to a
                    // literal, and every parser downstream of this point then sees
                    // a token that is not the value: `'Jordan Patel'::text` stored
                    // whole, quotes and all; `NULL::jsonb` failing the `NULL` test
                    // that plain `NULL` passes, so a nullable document column
                    // rejected the row as a schema mismatch; and `'{…}'::jsonb`
                    // handed to a JSON parser that stopped at the leading quote.
                    // Each of those was a separate report against a different
                    // column type, and all of them are this one line.
                    //
                    // The cast is redundant once the value is inlined — the column
                    // it is being written to, or compared against, supplies the
                    // type. Only a cast *directly* on the placeholder is consumed;
                    // one written on a column or an expression is left alone,
                    // because there it can change what the statement means.
                    if bytes.get(i) == Some(&b':') && bytes.get(i + 1) == Some(&b':') {
                        let mut k = i + 2;
                        while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                            k += 1;
                        }
                        let name_start = k;
                        while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
                        {
                            k += 1;
                        }
                        // A bare type name only. A parameterised or array type —
                        // `numeric(10,2)`, `text[]` — is left in place rather than
                        // half-consumed, which would strand punctuation.
                        if k > name_start && !matches!(bytes.get(k), Some(b'(') | Some(b'[')) {
                            i = k;
                        }
                    }
                }
                _ => {
                    // Copy the whole UTF-8 sequence, not just the lead byte.
                    let ch_len = utf8_len(c);
                    let end = (i + ch_len).min(bytes.len());
                    out.push_str(&sql[i..end]);
                    i = end;
                }
            }
        }

        Ok(out)
    }

    /// The old `quote_literal`, a character at a time.
    fn quote_literal_reference(s: &str) -> String {
        let mut out = String::with_capacity(s.len() + 2);
        out.push('\'');
        for c in s.chars() {
            if c == '\'' {
                out.push('\'');
            }
            out.push(c);
        }
        out.push('\'');
        out
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
            items[(self.next() % items.len() as u64) as usize]
        }
    }

    const FRAGMENTS: &[&str] = &[
        "SELECT ",
        "INSERT INTO t (a, b) VALUES (",
        ", ",
        ") ",
        " WHERE x = ",
        "$1",
        "$2",
        "$3",
        "$4",
        "$12",
        "$0",
        "$1::text",
        "$2::jsonb",
        "$3 :: int",
        "$1::numeric(10,2)",
        "$2::text[]",
        "'plain'",
        "'it''s $1'",
        "''",
        "'",
        r"E'a\'b $1'",
        r"e'\\'",
        r"E'x''y\n$2'",
        r"name'\' ",
        r"E'trailing\",
        "\"col $1\"",
        "\"a\"\"b\"",
        "\"",
        "-- c $1\n",
        "--",
        "/* $1 /* $2 */ */",
        "/* open",
        "$$ $1 $$",
        "$tag$ $2 $tag$",
        "$tag$ open",
        "a$b$",
        "x$1",
        "$",
        "$ ",
        "-",
        "/",
        "- 1",
        "/ 2",
        "ü",
        "日本",
        "🙂",
        "\n",
        " ",
        "(",
        ")",
        "café$1",
        "E",
        "e",
        r"_E'\'",
        "1E'z'",
        "$9999999999999999999999",
    ];

    fn params() -> Vec<BoundParam> {
        let t = |oid: i32, s: &str| BoundParam {
            value: Some(s.as_bytes().to_vec()),
            format: 0,
            oid,
        };
        vec![
            t(OID_TEXT, "O'Brien ''q'' 日本"),
            t(0, "42"),
            t(OID_INT4, "not a number"),
            BoundParam {
                value: None,
                format: 0,
                oid: OID_TEXT,
            },
            t(OID_BOOL, "true"),
            BoundParam {
                value: Some(b"bin'ary".to_vec()),
                format: 1,
                oid: OID_VARCHAR,
            },
            BoundParam {
                value: Some(vec![0, 1, 0xab, 0xff]),
                format: 1,
                oid: OID_BYTEA,
            },
            t(OID_JSONB, "{\"k\": \"v'\"}"),
            t(OID_NUMERIC, "-1.5e3"),
            t(OID_TEXT, ""),
            t(OID_TEXT, "$1 -- /* '"),
            t(OID_TEXT, "''''"),
            BoundParam {
                value: Some(vec![0xff, 0xfe]),
                format: 0,
                oid: OID_TEXT,
            },
        ]
    }

    #[test]
    fn matches_the_reference_over_generated_statements() {
        let params = params();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for round in 0..50_000 {
            let n = 1 + (rng.next() % 14) as usize;
            let mut sql = String::new();
            for _ in 0..n {
                sql.push_str(rng.pick(FRAGMENTS));
            }
            // Fewer parameters sometimes, so out-of-range errors are compared;
            // all of them otherwise, including the invalid UTF-8 one.
            let take = match round % 7 {
                0 => 2,
                1 => params.len(),
                _ => params.len() - 1,
            };
            let p = &params[..take];
            assert_eq!(
                inline_params(&sql, p),
                inline_params_reference(&sql, p),
                "statement: {:?}",
                sql
            );
        }
    }

    #[test]
    fn quoting_matches_the_reference() {
        let mut rng = Rng(7);
        for _ in 0..5_000 {
            let n = (rng.next() % 10) as usize;
            let mut s = String::new();
            for _ in 0..n {
                s.push_str(rng.pick(&["'", "''", "a", "日本", " ", "\\", "🙂", "\"", "x'y"]));
            }
            assert_eq!(quote_literal(&s), quote_literal_reference(&s), "{:?}", s);
        }
    }

    #[test]
    fn bytea_matches_the_per_byte_format() {
        let raw: Vec<u8> = (0..=255u8).collect();
        let mut expected = String::from("'\\x");
        for b in &raw {
            expected.push_str(&format!("{:02x}", b));
        }
        expected.push('\'');
        assert_eq!(binary_param_literal(OID_BYTEA, &raw).unwrap(), expected);
        assert_eq!(binary_param_literal(OID_BYTEA, &[]).unwrap(), "'\\x'");
    }

    /// Release-mode timing of the rewrite against the reference, on a wide
    /// multi-row INSERT of text and JSON values, bound and as literal text:
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored inline_params_bench --nocapture`
    #[test]
    #[ignore]
    fn inline_params_bench() {
        let mut sql = String::from("INSERT INTO t (id, name, doc, note) VALUES ");
        let mut params = Vec::new();
        let doc = format!("{{\"payload\": \"{}\", \"n\": [1, 2, 3]}}", "x".repeat(400));
        for r in 0..200usize {
            if r > 0 {
                sql.push_str(", ");
            }
            let base = r * 4;
            sql.push_str(&format!(
                "(${}, ${}, ${}::jsonb, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4
            ));
            params.push(BoundParam {
                value: Some((r as i32).to_be_bytes().to_vec()),
                format: 1,
                oid: OID_INT4,
            });
            params.push(BoundParam {
                value: Some(format!("name {} with O'Quote and ünïcode", r).into_bytes()),
                format: 0,
                oid: OID_TEXT,
            });
            params.push(BoundParam {
                value: Some(doc.clone().into_bytes()),
                format: 0,
                oid: OID_JSONB,
            });
            params.push(BoundParam {
                value: Some("y".repeat(300).into_bytes()),
                format: 0,
                oid: OID_TEXT,
            });
        }
        assert_eq!(
            inline_params(&sql, &params),
            inline_params_reference(&sql, &params)
        );
        // The same statement with every value already a literal and one
        // placeholder left: the shape where the scan, not the quoting, is the
        // work.
        let literal_sql = format!("{} RETURNING $1", inline_params(&sql, &params).unwrap());
        let one = [BoundParam {
            value: Some(b"1".to_vec()),
            format: 0,
            oid: 0,
        }];

        let iters = 300u32;
        for round in 0..4 {
            let t = std::time::Instant::now();
            for _ in 0..iters {
                std::hint::black_box(
                    inline_params_reference(std::hint::black_box(&sql), &params).unwrap(),
                );
            }
            let old_bound = t.elapsed() / iters;
            let t = std::time::Instant::now();
            for _ in 0..iters {
                std::hint::black_box(inline_params(std::hint::black_box(&sql), &params).unwrap());
            }
            let new_bound = t.elapsed() / iters;
            let t = std::time::Instant::now();
            for _ in 0..iters {
                std::hint::black_box(
                    inline_params_reference(std::hint::black_box(&literal_sql), &one).unwrap(),
                );
            }
            let old_lit = t.elapsed() / iters;
            let t = std::time::Instant::now();
            for _ in 0..iters {
                std::hint::black_box(
                    inline_params(std::hint::black_box(&literal_sql), &one).unwrap(),
                );
            }
            let new_lit = t.elapsed() / iters;
            println!(
                "round {round}: 800 bound params -> {} B: old {:?} new {:?} | {} B of literals: old {:?} new {:?}",
                literal_sql.len(),
                old_bound,
                new_bound,
                literal_sql.len(),
                old_lit,
                new_lit,
            );
        }
    }
}
