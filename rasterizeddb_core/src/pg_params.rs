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
                let start = i;
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
                if let Some(end) = dollar_quote_end(bytes, i) {
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
                    while k < bytes.len()
                        && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
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

/// Text-format parameters arrive already in their SQL rendering; only quoting
/// needs deciding, and that follows from the declared type.
fn text_param_literal(oid: i32, text: &str) -> String {
    match oid {
        OID_BOOL => {
            let t = matches!(text, "t" | "true" | "TRUE" | "1" | "y" | "yes" | "on");
            if t { "1".to_string() } else { "0".to_string() }
        }
        OID_INT2 | OID_INT4 | OID_INT8 | OID_OID | OID_FLOAT4 | OID_FLOAT8 | OID_NUMERIC => {
            // Numeric shapes pass through; anything else is quoted so a
            // malformed value fails as a value rather than as syntax.
            if is_numeric_literal(text) {
                text.to_string()
            } else {
                quote_literal(text)
            }
        }
        // An unspecified OID (0) is whatever the text looks like: sqlx sends
        // this for untyped placeholders.
        0 if is_numeric_literal(text) => text.to_string(),
        _ => quote_literal(text),
    }
}

fn binary_param_literal(oid: i32, raw: &[u8]) -> Result<String, String> {
    match oid {
        OID_BOOL => Ok(if raw.first().copied().unwrap_or(0) != 0 { "1" } else { "0" }.to_string()),
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
            let mut s = String::with_capacity(raw.len() * 2 + 4);
            s.push_str("'\\x");
            for b in raw {
                s.push_str(&format!("{:02x}", b));
            }
            s.push('\'');
            Ok(s)
        }
        _ => match array_element_oid(oid) {
            Some(element) => binary_array_literal(element, raw),
            None => Err(format!("unsupported binary parameter type OID {}", oid)),
        },
    }
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

fn is_numeric_literal(text: &str) -> bool {
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

/// Parses the canonical UUID text form (hyphens optional) into 16 bytes.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut n = 0usize;
    let mut hi: Option<u8> = None;
    for c in text.chars() {
        if c == '-' {
            continue;
        }
        let d = c.to_digit(16)? as u8;
        match hi {
            None => hi = Some(d),
            Some(h) => {
                if n >= 16 {
                    return None;
                }
                out[n] = (h << 4) | d;
                n += 1;
                hi = None;
            }
        }
    }
    if n == 16 && hi.is_none() { Some(out) } else { None }
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
        format!("{}.{:06}", base, frac).trim_end_matches('0').to_string()
    }
}

fn format_time_us(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let base = format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60);
    if frac == 0 {
        base
    } else {
        format!("{}.{:06}", base, frac).trim_end_matches('0').to_string()
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

    fn text(oid: i32, s: &str) -> BoundParam {
        BoundParam { value: Some(s.as_bytes().to_vec()), format: 0, oid }
    }
    fn binary(oid: i32, b: Vec<u8>) -> BoundParam {
        BoundParam { value: Some(b), format: 1, oid }
    }
    fn null(oid: i32) -> BoundParam {
        BoundParam { value: None, format: 1, oid }
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
        assert_eq!(param_to_literal(&binary(OID_INT2, 300i16.to_be_bytes().to_vec())).unwrap(), "300");
        assert_eq!(param_to_literal(&binary(OID_INT4, (-7i32).to_be_bytes().to_vec())).unwrap(), "-7");
        assert_eq!(
            param_to_literal(&binary(OID_INT8, 9_000_000_000i64.to_be_bytes().to_vec())).unwrap(),
            "9000000000"
        );
    }

    #[test]
    fn decodes_binary_floats() {
        assert_eq!(param_to_literal(&binary(OID_FLOAT8, 1.5f64.to_be_bytes().to_vec())).unwrap(), "1.5");
        assert_eq!(param_to_literal(&binary(OID_FLOAT4, 2.0f32.to_be_bytes().to_vec())).unwrap(), "2.0");
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
        assert_eq!(param_to_literal(&binary(OID_NUMERIC, raw)).unwrap(), "1234.5678");
    }

    #[test]
    fn decodes_negative_binary_numeric() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i16.to_be_bytes());
        raw.extend_from_slice(&0i16.to_be_bytes());
        raw.extend_from_slice(&0x4000u16.to_be_bytes());
        raw.extend_from_slice(&2i16.to_be_bytes());
        raw.extend_from_slice(&42u16.to_be_bytes());
        assert_eq!(param_to_literal(&binary(OID_NUMERIC, raw)).unwrap(), "-42.00");
    }

    #[test]
    fn text_bool_params_become_engine_booleans() {
        assert_eq!(param_to_literal(&text(OID_BOOL, "t")).unwrap(), "1");
        assert_eq!(param_to_literal(&text(OID_BOOL, "f")).unwrap(), "0");
    }

    #[test]
    fn civil_day_conversion_round_trips() {
        for &(y, m, d) in &[(1970, 1, 1), (2000, 1, 1), (2026, 8, 20), (1999, 12, 31), (2400, 2, 29)] {
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
        assert!(literal.contains("11111111-1111-1111-1111-111111111111"), "{}", literal);
        assert!(literal.contains("22222222-2222-2222-2222-222222222222"), "{}", literal);
        assert!(literal.starts_with('\'') && literal.ends_with('\''), "{}", literal);
    }

    #[test]
    fn an_int_array_keeps_its_order_and_nulls() {
        let raw = array(
            OID_INT4,
            &[Some(1i32.to_be_bytes().to_vec()), None, Some(3i32.to_be_bytes().to_vec())],
        );
        assert_eq!(binary_param_literal(1007, &raw).unwrap(), "'{\"1\",NULL,\"3\"}'");
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
    fn a_type_that_is_not_an_array_is_still_refused() {
        assert!(binary_param_literal(9999, b"x").is_err());
    }
}
