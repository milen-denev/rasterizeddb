//! What the engine answers, in MySQL's encoding: column definitions, text
//! and binary rows, OK / EOF / ERR packets, and errors renumbered.
//!
//! The session asks the engine for text-format results, so every value
//! arrives as PostgreSQL's text for its type. MySQL's text protocol is text
//! too, and differs in a few spellings: booleans are `1`/`0`, binary data is
//! raw bytes rather than `\x…` hex, and a time zone offset is never shown.
//! The binary protocol (prepared statements) is built from the same text.

use crate::pgwire::proxied::{PgField, PgNotice};

use super::packet::{Packets, put_lenenc_int, put_lenenc_str};
use super::translate::MyError;

// Column types.
#[allow(dead_code)]
pub(crate) const T_DECIMAL: u8 = 0;
pub(crate) const T_TINY: u8 = 1;
pub(crate) const T_SHORT: u8 = 2;
pub(crate) const T_LONG: u8 = 3;
pub(crate) const T_FLOAT: u8 = 4;
pub(crate) const T_DOUBLE: u8 = 5;
pub(crate) const T_NULL: u8 = 6;
pub(crate) const T_TIMESTAMP: u8 = 7;
pub(crate) const T_LONGLONG: u8 = 8;
pub(crate) const T_INT24: u8 = 9;
pub(crate) const T_DATE: u8 = 10;
pub(crate) const T_TIME: u8 = 11;
pub(crate) const T_DATETIME: u8 = 12;
pub(crate) const T_YEAR: u8 = 13;
#[allow(dead_code)]
pub(crate) const T_VARCHAR: u8 = 15;
#[allow(dead_code)]
pub(crate) const T_BIT: u8 = 16;
pub(crate) const T_JSON: u8 = 245;
pub(crate) const T_NEWDECIMAL: u8 = 246;
#[allow(dead_code)]
pub(crate) const T_ENUM: u8 = 247;
#[allow(dead_code)]
pub(crate) const T_SET: u8 = 248;
pub(crate) const T_TINY_BLOB: u8 = 249;
pub(crate) const T_MEDIUM_BLOB: u8 = 250;
pub(crate) const T_LONG_BLOB: u8 = 251;
pub(crate) const T_BLOB: u8 = 252;
pub(crate) const T_VAR_STRING: u8 = 253;
pub(crate) const T_STRING: u8 = 254;
#[allow(dead_code)]
pub(crate) const T_GEOMETRY: u8 = 255;

// Column flags.
const F_BLOB: u16 = 16;
const F_BINARY: u16 = 128;
const F_TIMESTAMP: u16 = 1024;
const F_NUM: u16 = 32768;

/// `binary`, what numbers, dates and blobs are tagged with.
pub(crate) const CHARSET_BINARY: u16 = 63;
/// `utf8mb4_bin`: text compares as bytes here, and this says so.
pub(crate) const CHARSET_UTF8MB4_BIN: u16 = 46;

// Server status flags.
pub(crate) const STATUS_IN_TRANS: u16 = 1;
pub(crate) const STATUS_AUTOCOMMIT: u16 = 2;
pub(crate) const STATUS_MORE_RESULTS: u16 = 8;
pub(crate) const STATUS_NO_BACKSLASH_ESCAPES: u16 = 512;

/// How a value of a column is converted from the engine's text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Conv {
    Plain,
    Bool,
    Bytea,
    /// A time with a zone offset to drop.
    Zoned,
}

#[derive(Clone, Debug)]
pub(crate) struct MyColumn {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) type_code: u8,
    pub(crate) charset: u16,
    pub(crate) length: u32,
    pub(crate) flags: u16,
    pub(crate) decimals: u8,
    pub(crate) conv: Conv,
}

impl MyColumn {
    /// A text column of a locally built result (`SHOW …`).
    pub(crate) fn text(name: &str) -> Self {
        MyColumn {
            name: name.to_string(),
            table: String::new(),
            type_code: T_VAR_STRING,
            charset: CHARSET_UTF8MB4_BIN,
            length: 256,
            flags: 0,
            decimals: 0,
            conv: Conv::Plain,
        }
    }

    /// An integer column of a locally built result.
    pub(crate) fn int(name: &str) -> Self {
        MyColumn {
            name: name.to_string(),
            table: String::new(),
            type_code: T_LONGLONG,
            charset: CHARSET_BINARY,
            length: 21,
            flags: F_NUM | F_BINARY,
            decimals: 0,
            conv: Conv::Plain,
        }
    }
}

/// The MySQL column for an engine column.
pub(crate) fn column_for(field: &PgField) -> MyColumn {
    let (type_code, charset, length, flags, decimals, conv) = match field.type_oid {
        16 => (T_TINY, CHARSET_BINARY, 1, F_NUM | F_BINARY, 0, Conv::Bool),
        21 => (T_SHORT, CHARSET_BINARY, 6, F_NUM | F_BINARY, 0, Conv::Plain),
        23 | 26 => (T_LONG, CHARSET_BINARY, 11, F_NUM | F_BINARY, 0, Conv::Plain),
        20 => (
            T_LONGLONG,
            CHARSET_BINARY,
            20,
            F_NUM | F_BINARY,
            0,
            Conv::Plain,
        ),
        700 => (
            T_FLOAT,
            CHARSET_BINARY,
            12,
            F_NUM | F_BINARY,
            31,
            Conv::Plain,
        ),
        701 => (
            T_DOUBLE,
            CHARSET_BINARY,
            22,
            F_NUM | F_BINARY,
            31,
            Conv::Plain,
        ),
        1700 => {
            let (precision, scale) = if field.type_modifier >= 4 {
                let m = field.type_modifier - 4;
                ((m >> 16) & 0xffff, m & 0xffff)
            } else {
                (65, 30)
            };
            (
                T_NEWDECIMAL,
                CHARSET_BINARY,
                (precision + 2) as u32,
                F_NUM | F_BINARY,
                scale as u8,
                Conv::Plain,
            )
        }
        1082 => (T_DATE, CHARSET_BINARY, 10, F_BINARY, 0, Conv::Plain),
        1114 => (T_DATETIME, CHARSET_BINARY, 26, F_BINARY, 6, Conv::Plain),
        1184 => (
            T_TIMESTAMP,
            CHARSET_BINARY,
            26,
            F_BINARY | F_TIMESTAMP,
            6,
            Conv::Zoned,
        ),
        1083 => (T_TIME, CHARSET_BINARY, 15, F_BINARY, 6, Conv::Plain),
        1266 => (T_TIME, CHARSET_BINARY, 15, F_BINARY, 6, Conv::Zoned),
        17 => (
            T_BLOB,
            CHARSET_BINARY,
            65535,
            F_BLOB | F_BINARY,
            0,
            Conv::Bytea,
        ),
        114 | 3802 => (
            T_JSON,
            CHARSET_BINARY,
            u32::MAX,
            F_BLOB | F_BINARY,
            0,
            Conv::Plain,
        ),
        2950 => (T_STRING, CHARSET_UTF8MB4_BIN, 144, 0, 0, Conv::Plain),
        1043 | 1042 if field.type_modifier > 4 => (
            T_VAR_STRING,
            CHARSET_UTF8MB4_BIN,
            ((field.type_modifier - 4) as u32).saturating_mul(4),
            0,
            0,
            Conv::Plain,
        ),
        _ => (
            T_VAR_STRING,
            CHARSET_UTF8MB4_BIN,
            262_140,
            0,
            0,
            Conv::Plain,
        ),
    };
    MyColumn {
        name: field.name.clone(),
        table: String::new(),
        type_code,
        charset,
        length,
        flags,
        decimals,
        conv,
    }
}

/// A column definition packet (Protocol::ColumnDefinition41).
pub(crate) fn write_column(io: &mut Packets, schema: &str, column: &MyColumn) {
    let at = io.begin();
    let out = io.buf();
    put_lenenc_str(out, b"def");
    put_lenenc_str(out, schema.as_bytes());
    put_lenenc_str(out, column.table.as_bytes());
    put_lenenc_str(out, column.table.as_bytes());
    put_lenenc_str(out, column.name.as_bytes());
    put_lenenc_str(out, column.name.as_bytes());
    out.push(0x0c);
    out.extend_from_slice(&column.charset.to_le_bytes());
    out.extend_from_slice(&column.length.to_le_bytes());
    out.push(column.type_code);
    out.extend_from_slice(&column.flags.to_le_bytes());
    out.push(column.decimals);
    out.extend_from_slice(&[0, 0]);
    io.end(at);
}

/// The value MySQL's text protocol carries for an engine text value.
pub(crate) fn text_value<'a>(conv: Conv, value: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
    use std::borrow::Cow;
    match conv {
        Conv::Plain => Cow::Borrowed(value),
        Conv::Bool => Cow::Borrowed(match value {
            b"t" | b"true" => b"1",
            b"f" | b"false" => b"0",
            other => other,
        }),
        Conv::Bytea => match value.strip_prefix(b"\\x") {
            Some(hex) => Cow::Owned(decode_hex(hex)),
            None => Cow::Borrowed(value),
        },
        Conv::Zoned => Cow::Borrowed(strip_zone(value)),
    }
}

fn decode_hex(hex: &[u8]) -> Vec<u8> {
    let digit = |b: u8| match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    };
    hex.chunks(2)
        .filter(|p| p.len() == 2)
        .map(|p| digit(p[0]) << 4 | digit(p[1]))
        .collect()
}

/// `2024-01-02 03:04:05.5+02` → `2024-01-02 03:04:05.5`.
fn strip_zone(value: &[u8]) -> &[u8] {
    // The offset starts at the last '+' or '-' after the time's colons.
    let Some(colon) = value.iter().position(|b| *b == b':') else {
        return value;
    };
    match value[colon..]
        .iter()
        .rposition(|b| *b == b'+' || *b == b'-')
    {
        Some(at) => &value[..colon + at],
        None => value,
    }
}

/// A text-protocol row.
pub(crate) fn write_text_row(io: &mut Packets, columns: &[MyColumn], cells: &[Option<&[u8]>]) {
    let at = io.begin();
    let out = io.buf();
    for (k, cell) in cells.iter().enumerate() {
        match cell {
            None => out.push(0xfb),
            Some(v) => {
                let conv = columns.get(k).map(|c| c.conv).unwrap_or(Conv::Plain);
                put_lenenc_str(out, &text_value(conv, v));
            }
        }
    }
    io.end(at);
}

fn parse_date(s: &str) -> Option<(u16, u8, u8)> {
    let mut parts = s.splitn(3, '-');
    let y = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let d = parts.next()?.get(..2)?.parse().ok()?;
    Some((y, m, d))
}

fn parse_time(s: &str) -> Option<(u8, u8, u8, u32)> {
    let mut parts = s.splitn(3, ':');
    let h = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let rest = parts.next()?;
    let (sec, frac) = match rest.split_once('.') {
        Some((s, f)) => (s, f),
        None => (rest, ""),
    };
    let sec = sec.parse().ok()?;
    let mut micros = 0u32;
    if !frac.is_empty() {
        let digits: String = frac
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .take(6)
            .collect();
        micros = format!("{:0<6}", digits).parse().ok()?;
    }
    Some((h, m, sec, micros))
}

/// One value in the binary protocol's encoding for `column`'s type.
fn put_binary_value(out: &mut Vec<u8>, column: &MyColumn, value: &[u8]) -> Result<(), String> {
    let text = || std::str::from_utf8(value).map_err(|_| "a value is not text".to_string());
    let bad = |what: &str| {
        format!(
            "{what} {:?} cannot be sent in MySQL's binary encoding",
            String::from_utf8_lossy(value)
        )
    };
    match column.type_code {
        T_TINY => {
            let v: i8 = match text_value(column.conv, value).as_ref() {
                b"1" => 1,
                b"0" => 0,
                other => std::str::from_utf8(other)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| bad("the integer"))?,
            };
            out.push(v as u8);
        }
        T_SHORT => out.extend_from_slice(
            &text()?
                .parse::<i16>()
                .map_err(|_| bad("the integer"))?
                .to_le_bytes(),
        ),
        T_LONG => out.extend_from_slice(
            &text()?
                .parse::<i32>()
                .map_err(|_| bad("the integer"))?
                .to_le_bytes(),
        ),
        T_LONGLONG => out.extend_from_slice(
            &text()?
                .parse::<i64>()
                .map_err(|_| bad("the integer"))?
                .to_le_bytes(),
        ),
        T_FLOAT => {
            let v = parse_float(text()?).ok_or_else(|| bad("the number"))? as f32;
            out.extend_from_slice(&v.to_le_bytes());
        }
        T_DOUBLE => out.extend_from_slice(
            &parse_float(text()?)
                .ok_or_else(|| bad("the number"))?
                .to_le_bytes(),
        ),
        T_DATE => {
            let (y, m, d) = parse_date(text()?).ok_or_else(|| bad("the date"))?;
            out.push(4);
            out.extend_from_slice(&y.to_le_bytes());
            out.push(m);
            out.push(d);
        }
        T_DATETIME | T_TIMESTAMP => {
            let s = std::str::from_utf8(strip_zone(value)).map_err(|_| bad("the time"))?;
            let (date, time) = s.split_once(' ').unwrap_or((s, "00:00:00"));
            let (y, m, d) = parse_date(date).ok_or_else(|| bad("the time"))?;
            let (h, mi, sec, micros) = parse_time(time).ok_or_else(|| bad("the time"))?;
            out.push(if micros > 0 { 11 } else { 7 });
            out.extend_from_slice(&y.to_le_bytes());
            out.extend_from_slice(&[m, d, h, mi, sec]);
            if micros > 0 {
                out.extend_from_slice(&micros.to_le_bytes());
            }
        }
        T_TIME => {
            let s = std::str::from_utf8(strip_zone(value)).map_err(|_| bad("the time"))?;
            let (h, mi, sec, micros) = parse_time(s).ok_or_else(|| bad("the time"))?;
            let (days, h) = (h as u32 / 24, h % 24);
            out.push(if micros > 0 { 12 } else { 8 });
            out.push(0);
            out.extend_from_slice(&days.to_le_bytes());
            out.extend_from_slice(&[h, mi, sec]);
            if micros > 0 {
                out.extend_from_slice(&micros.to_le_bytes());
            }
        }
        _ => put_lenenc_str(out, &text_value(column.conv, value)),
    }
    Ok(())
}

fn parse_float(s: &str) -> Option<f64> {
    match s {
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse().ok(),
    }
}

/// A binary-protocol row.
pub(crate) fn write_binary_row(
    io: &mut Packets,
    columns: &[MyColumn],
    cells: &[Option<&[u8]>],
) -> Result<(), String> {
    let at = io.begin();
    let out = io.buf();
    out.push(0);
    let bitmap_at = out.len();
    let bitmap_len = (cells.len() + 7 + 2) / 8;
    out.resize(bitmap_at + bitmap_len, 0);
    for (k, cell) in cells.iter().enumerate() {
        match cell {
            None => {
                let bit = k + 2;
                out[bitmap_at + bit / 8] |= 1 << (bit % 8);
            }
            Some(v) => {
                let column = columns
                    .get(k)
                    .cloned()
                    .unwrap_or_else(|| MyColumn::text(""));
                if let Err(e) = put_binary_value(out, &column, v) {
                    // The row is not sent at all: the error that follows
                    // ends the result.
                    out.truncate(at);
                    return Err(e);
                }
            }
        }
    }
    io.end(at);
    Ok(())
}

/// An OK packet. `eof` marks the one that ends a result set under
/// CLIENT_DEPRECATE_EOF.
pub(crate) fn write_ok(
    io: &mut Packets,
    affected: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
    info: &str,
    eof: bool,
) {
    let at = io.begin();
    let out = io.buf();
    out.push(if eof { 0xfe } else { 0x00 });
    put_lenenc_int(out, affected);
    put_lenenc_int(out, insert_id);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&warnings.to_le_bytes());
    out.extend_from_slice(info.as_bytes());
    io.end(at);
}

/// An EOF packet (before CLIENT_DEPRECATE_EOF).
pub(crate) fn write_eof(io: &mut Packets, status: u16, warnings: u16) {
    let at = io.begin();
    let out = io.buf();
    out.push(0xfe);
    out.extend_from_slice(&warnings.to_le_bytes());
    out.extend_from_slice(&status.to_le_bytes());
    io.end(at);
}

pub(crate) fn write_err(io: &mut Packets, error: &MyError) {
    let at = io.begin();
    let out = io.buf();
    out.push(0xff);
    out.extend_from_slice(&error.code.to_le_bytes());
    out.push(b'#');
    out.extend_from_slice(error.state.as_bytes());
    out.extend_from_slice(error.message.as_bytes());
    io.end(at);
}

/// An engine error, in MySQL's numbering. The PostgreSQL code is kept at the
/// end of the message, so nothing it said is lost.
pub(crate) fn error_from_engine(notice: &PgNotice) -> MyError {
    let (code, state): (u16, &'static str) = match notice.code.as_str() {
        "23505" => (1062, "23000"),
        "23502" => (1048, "23000"),
        "23503" => (1452, "23000"),
        "23514" => (3819, "HY000"),
        "42P01" => (1146, "42S02"),
        "42703" => (1054, "42S22"),
        "42601" => (1064, "42000"),
        "42P07" => (1050, "42S01"),
        "42701" => (1060, "42S21"),
        "42704" => (1091, "42000"),
        "42883" => (1305, "42000"),
        "42501" => (1142, "42000"),
        "3D000" => (1049, "42000"),
        "42P04" => (1007, "HY000"),
        "40001" | "40P01" => (1213, "40001"),
        "55P03" => (1205, "HY000"),
        "57014" => (1317, "70100"),
        "22001" => (1406, "22001"),
        "22003" => (1264, "22003"),
        "22012" => (1365, "22012"),
        "22P02" | "22007" | "22008" => (1366, "HY000"),
        "25P02" => (1105, "HY000"),
        "25006" => (1792, "25006"),
        "0A000" => (1235, "42000"),
        "28P01" | "28000" => (1045, "28000"),
        "53300" => (1040, "08004"),
        "57P01" | "57P05" => (1053, "08S01"),
        "25P03" => (1205, "HY000"),
        _ => (1105, "HY000"),
    };
    let mut message = notice.message.clone();
    if let Some(detail) = &notice.detail {
        message.push_str(": ");
        message.push_str(detail);
    }
    if !notice.code.is_empty() {
        message.push_str(&format!(" (SQLSTATE {})", notice.code));
    }
    MyError::new(code, state, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_values() {
        assert_eq!(text_value(Conv::Bool, b"t").as_ref(), b"1");
        assert_eq!(text_value(Conv::Bytea, b"\\x00ff").as_ref(), &[0u8, 0xff]);
        assert_eq!(
            text_value(Conv::Zoned, b"2024-01-02 03:04:05.5+02").as_ref(),
            b"2024-01-02 03:04:05.5"
        );
        assert_eq!(
            text_value(Conv::Zoned, b"2024-01-02 03:04:05-05:30").as_ref(),
            b"2024-01-02 03:04:05"
        );
        assert_eq!(
            text_value(Conv::Zoned, b"10:00:00+00").as_ref(),
            b"10:00:00"
        );
    }

    #[test]
    fn binary_dates() {
        let column = MyColumn {
            type_code: T_DATETIME,
            ..MyColumn::text("t")
        };
        let mut out = Vec::new();
        put_binary_value(&mut out, &column, b"2024-01-02 03:04:05.000250").unwrap();
        assert_eq!(out, vec![11, 0xe8, 0x07, 1, 2, 3, 4, 5, 0xfa, 0, 0, 0]);
        let column = MyColumn {
            type_code: T_TIME,
            ..MyColumn::text("t")
        };
        let mut out = Vec::new();
        put_binary_value(&mut out, &column, b"25:00:01").unwrap();
        assert_eq!(out, vec![8, 0, 1, 0, 0, 0, 1, 0, 1]);
    }
}
