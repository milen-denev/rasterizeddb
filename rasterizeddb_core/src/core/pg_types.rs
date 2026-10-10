//! Text ⇄ storage encodings for the PostgreSQL types the engine gained beyond
//! its original scalar set: intervals, MAC addresses, bit strings, the split of
//! `DATE`/`TIME`/`TIMETZ` out of `DATETIME`, the seven geometric types and the
//! six built-in range types.
//!
//! # Why these store the wire format and [`super::value_codec`]'s types do not
//!
//! The older types are stored in whatever layout makes `memcmp` reproduce their
//! ordering, because the fingerprint index and the zone maps compare raw column
//! bytes. None of the types here has an ordering worth indexing — no query
//! plans a range scan over a polygon — so that constraint buys nothing, and the
//! opposite choice pays: storing **exactly the bytes PostgreSQL puts on the
//! wire** makes the binary result path a copy, and a copy cannot disagree with
//! the text path about what the value is.
//!
//! That is not a hypothetical worry in this engine. A `NUMERIC` reached one
//! client multiplied by a power of ten because the text renderer and the binary
//! renderer each derived the value separately and only one of them knew the
//! column's scale. Every type in this module derives the *text* form from the
//! stored bytes, so there is one source and the two protocols cannot fork.
//!
//! Where a type is fixed-width, it is stored inline in the row like `UUID`;
//! where it is not, [`DbType::physical`] maps it onto `STRING` and it rides the
//! existing variable-width machinery. Ordering, for the sorts and joins that
//! ask for one, is defined in `compare_bytes_as_type` and is a *stable* order
//! rather than PostgreSQL's own for the types where the two differ.

use crate::core::db_type::DbType;
use crate::core::value_codec::{civil_from_days, days_from_civil, strip_quotes};

/// Days between the Unix epoch and PostgreSQL's 2000-01-01 epoch.
pub const PG_EPOCH_DAYS: i32 = 10_957;

/// Serialized width of an `INTERVAL`: `i64` microseconds, `i32` days, `i32`
/// months, all big-endian, which is PostgreSQL's binary layout exactly.
pub const INTERVAL_LEN: usize = 16;
/// Serialized width of a `MACADDR`.
pub const MACADDR_LEN: usize = 6;
/// Serialized width of a `MACADDR8`.
pub const MACADDR8_LEN: usize = 8;
/// Serialized width of a `DATE`: `i32` days from the PostgreSQL epoch.
pub const DATE_LEN: usize = 4;
/// Serialized width of a `TIME`: `i64` microseconds since midnight.
pub const TIME_LEN: usize = 8;
/// Serialized width of a `TIMETZ`: `i64` microseconds plus an `i32` UTC offset
/// in seconds *west* of Greenwich, which is the sign convention PostgreSQL uses
/// on the wire and the opposite of the one the text form prints.
pub const TIMETZ_LEN: usize = 12;
/// Serialized width of a `POINT`: two `f64`.
pub const POINT_LEN: usize = 16;
/// Serialized width of an `LSEG` or a `BOX`: four `f64`.
pub const LSEG_LEN: usize = 32;
/// Serialized width of a `LINE` or a `CIRCLE`: three `f64`.
pub const LINE_LEN: usize = 24;

// ------------------------------------------------------------ small helpers

fn be_i32(bytes: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn be_i64(bytes: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn be_f64(bytes: &[u8], at: usize) -> Option<f64> {
    Some(f64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

/// Renders an `f64` the way PostgreSQL renders a coordinate: shortest text that
/// round-trips, with no trailing `.0` on a whole number.
fn format_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        let mut s = format!("{}", v);
        if s.contains('e') {
            s = format!("{:e}", v);
        }
        s
    }
}

/// Splits a comma-separated list, ignoring commas nested in parentheses.
fn split_top_level(text: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    for c in text.chars() {
        match c {
            '(' | '[' => {
                depth += 1;
                current.push(c);
            }
            ')' | ']' => {
                depth = depth.saturating_sub(1);
                current.push(c);
            }
            c if c == sep && depth == 0 => out.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    out.push(current);
    out
}

// ---------------------------------------------------------------- INTERVAL

/// Months, days and microseconds, kept apart the way PostgreSQL keeps them.
///
/// They are not interchangeable: a month is 28–31 days and a day is 23–25 hours
/// across a DST boundary, so collapsing them to one number would make
/// `'1 month'` mean something the calendar does not agree with. Comparison
/// *does* collapse them — see [`interval_sort_key`] — because a sort needs a
/// total order and PostgreSQL's own comparison makes the same approximation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

/// Parses a PostgreSQL interval literal.
///
/// Three input styles are accepted, which between them cover what a client or a
/// dump actually emits:
///
/// * unit form — `'1 year 2 mons 3 days 04:05:06'`, `'-2 days'`, `'1.5 hours'`
/// * ISO 8601 — `'P1Y2M3DT4H5M6S'`
/// * bare clock — `'04:05:06.5'`, `'-04:05:06'`
pub fn parse_interval(text: &str) -> Option<Interval> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if text.starts_with('P') || text.starts_with('p') {
        return parse_iso_interval(text);
    }

    let mut out = Interval::default();
    let mut seen_any = false;
    // `ago` reverses the whole interval, which is how PostgreSQL prints a
    // negative one in the `postgres` datestyle.
    let mut negate_all = false;

    let mut tokens = text.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        if token.eq_ignore_ascii_case("ago") {
            negate_all = true;
            continue;
        }
        // A clock component stands alone: `04:05:06` carries its own units.
        if token.contains(':') {
            let (micros, _) = parse_clock(token)?;
            out.micros = out.micros.checked_add(micros)?;
            seen_any = true;
            continue;
        }
        // `1year` with no space, as well as `1 year`.
        let split_at = token
            .char_indices()
            .find(|(i, c)| c.is_ascii_alphabetic() && *i > 0)
            .map(|(i, _)| i);
        let (number, inline_unit) = match split_at {
            Some(i) => (&token[..i], Some(&token[i..])),
            None => (token, None),
        };
        let value: f64 = number.parse().ok()?;
        let unit = match inline_unit {
            Some(u) => u.to_string(),
            None => tokens.next()?.to_string(),
        };
        apply_unit(&mut out, value, unit.trim_end_matches(','))?;
        seen_any = true;
    }

    if !seen_any {
        return None;
    }
    if negate_all {
        out.months = out.months.checked_neg()?;
        out.days = out.days.checked_neg()?;
        out.micros = out.micros.checked_neg()?;
    }
    Some(out)
}

/// Adds `value` of one named unit to an interval.
///
/// A fractional larger unit spills downward exactly as PostgreSQL spills it:
/// `1.5 months` is one month plus fifteen days, because a month is defined as
/// thirty days for this purpose alone.
fn apply_unit(out: &mut Interval, value: f64, unit: &str) -> Option<()> {
    let unit = unit.trim().to_ascii_lowercase();
    let unit = unit.trim_end_matches('s');
    let (months_per, days_per, micros_per) = match unit {
        "millennium" | "millennia" | "millenium" => (12_000.0, 0.0, 0.0),
        "century" | "centurie" => (1_200.0, 0.0, 0.0),
        "decade" | "decade_" => (120.0, 0.0, 0.0),
        "year" | "yr" | "y" => (12.0, 0.0, 0.0),
        "month" | "mon" | "mo" => (1.0, 0.0, 0.0),
        "week" | "w" => (0.0, 7.0, 0.0),
        "day" | "d" => (0.0, 1.0, 0.0),
        "hour" | "hr" | "h" => (0.0, 0.0, 3_600_000_000.0),
        "minute" | "min" | "m" => (0.0, 0.0, 60_000_000.0),
        "second" | "sec" | "s" => (0.0, 0.0, 1_000_000.0),
        "millisecond" | "msec" | "ms" => (0.0, 0.0, 1_000.0),
        "microsecond" | "usec" | "us" => (0.0, 0.0, 1.0),
        _ => return None,
    };

    if months_per > 0.0 {
        let total = value * months_per;
        let whole = total.trunc();
        out.months = out.months.checked_add(whole as i32)?;
        // The remainder of a month is thirty days, then the remainder of a day
        // is twenty-four hours.
        let rest_days = (total - whole) * 30.0;
        spill_days(out, rest_days)?;
    } else if days_per > 0.0 {
        let total = value * days_per;
        let whole = total.trunc();
        out.days = out.days.checked_add(whole as i32)?;
        out.micros = out
            .micros
            .checked_add(((total - whole) * 86_400_000_000.0).round() as i64)?;
    } else {
        out.micros = out
            .micros
            .checked_add((value * micros_per).round() as i64)?;
    }
    Some(())
}

fn spill_days(out: &mut Interval, days: f64) -> Option<()> {
    let whole = days.trunc();
    out.days = out.days.checked_add(whole as i32)?;
    out.micros = out
        .micros
        .checked_add(((days - whole) * 86_400_000_000.0).round() as i64)?;
    Some(())
}

/// `HH:MM[:SS[.ffffff]]`, returning microseconds and whether it was negative.
fn parse_clock(token: &str) -> Option<(i64, bool)> {
    let (negative, body) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token.strip_prefix('+').unwrap_or(token)),
    };
    let mut parts = body.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    let seconds: f64 = match parts.next() {
        Some(s) => s.parse().ok()?,
        None => 0.0,
    };
    if parts.next().is_some() {
        return None;
    }
    let micros = hours.checked_mul(3_600_000_000)?
        + minutes.checked_mul(60_000_000)?
        + (seconds * 1_000_000.0).round() as i64;
    Some((if negative { -micros } else { micros }, negative))
}

/// `P1Y2M3DT4H5M6S`, including the week form `P3W`.
fn parse_iso_interval(text: &str) -> Option<Interval> {
    let body = &text[1..];
    let mut out = Interval::default();
    let mut number = String::new();
    let mut in_time = false;
    let mut seen_any = false;

    for c in body.chars() {
        match c {
            'T' | 't' => {
                in_time = true;
                number.clear();
            }
            '0'..='9' | '.' | '-' | '+' => number.push(c),
            _ => {
                let value: f64 = number.parse().ok()?;
                number.clear();
                seen_any = true;
                let unit = match (c.to_ascii_uppercase(), in_time) {
                    ('Y', _) => "year",
                    ('W', _) => "week",
                    ('D', _) => "day",
                    ('M', false) => "month",
                    ('M', true) => "minute",
                    ('H', true) => "hour",
                    ('S', true) => "second",
                    _ => return None,
                };
                apply_unit(&mut out, value, unit)?;
            }
        }
    }
    if !number.is_empty() || !seen_any {
        return None;
    }
    Some(out)
}

/// The 16 bytes PostgreSQL puts on the wire.
pub fn encode_interval(iv: Interval) -> Vec<u8> {
    let mut out = Vec::with_capacity(INTERVAL_LEN);
    out.extend_from_slice(&iv.micros.to_be_bytes());
    out.extend_from_slice(&iv.days.to_be_bytes());
    out.extend_from_slice(&iv.months.to_be_bytes());
    out
}

pub fn decode_interval(bytes: &[u8]) -> Option<Interval> {
    if bytes.len() != INTERVAL_LEN {
        return None;
    }
    Some(Interval {
        micros: be_i64(bytes, 0)?,
        days: be_i32(bytes, 8)?,
        months: be_i32(bytes, 12)?,
    })
}

/// Moves an instant by an interval, months first and calendar-aware.
///
/// A month is not a fixed number of microseconds — adding one to 31 January
/// lands on 28 February, not on 3 March — so the months are applied to the
/// calendar date, then the days, then the microseconds. That is the order
/// PostgreSQL applies them in, and the order matters: `'1 month 1 day'` added
/// to 31 January is 1 March, while a day-first reading gives 2 March.
pub fn shift_timestamp(micros_since_epoch: i64, span: Interval) -> Option<i64> {
    let mut days = micros_since_epoch.div_euclid(86_400_000_000);
    let time_of_day = micros_since_epoch.rem_euclid(86_400_000_000);

    if span.months != 0 {
        let (year, month, day) = civil_from_days(days);
        let total = year * 12 + (month as i64 - 1) + span.months as i64;
        let new_year = total.div_euclid(12);
        let new_month = (total.rem_euclid(12) + 1) as u32;
        // A day the target month does not have is clamped to its last, which is
        // what PostgreSQL does — 31 January plus a month is 28 February.
        let last = days_in_month(new_year, new_month);
        days = days_from_civil(new_year, new_month, day.min(last));
    }

    days = days.checked_add(span.days as i64)?;
    days.checked_mul(86_400_000_000)?
        .checked_add(time_of_day)?
        .checked_add(span.micros)
}

/// Days in a month, leap years included.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap { 29 } else { 28 }
        }
        _ => 30,
    }
}

/// A single number that orders two intervals the way PostgreSQL orders them.
///
/// PostgreSQL compares intervals by converting to microseconds with a month
/// taken as thirty days — an approximation it makes knowingly, since the true
/// answer depends on which month. Reproducing the approximation is what keeps
/// `ORDER BY duration` agreeing with the same query run against PostgreSQL.
pub fn interval_sort_key(iv: Interval) -> i128 {
    (iv.months as i128) * 30 * 86_400_000_000
        + (iv.days as i128) * 86_400_000_000
        + iv.micros as i128
}

/// PostgreSQL's default (`postgres`) interval output, e.g.
/// `1 year 2 mons 3 days 04:05:06`.
pub fn format_interval(bytes: &[u8]) -> Option<String> {
    let iv = decode_interval(bytes)?;
    let mut parts: Vec<String> = Vec::new();

    let years = iv.months / 12;
    let months = iv.months % 12;
    if years != 0 {
        parts.push(format!(
            "{} year{}",
            years,
            if years.abs() == 1 { "" } else { "s" }
        ));
    }
    if months != 0 {
        parts.push(format!(
            "{} mon{}",
            months,
            if months.abs() == 1 { "" } else { "s" }
        ));
    }
    if iv.days != 0 {
        parts.push(format!(
            "{} day{}",
            iv.days,
            if iv.days.abs() == 1 { "" } else { "s" }
        ));
    }

    if iv.micros != 0 || parts.is_empty() {
        let negative = iv.micros < 0;
        let magnitude = iv.micros.unsigned_abs();
        let hours = magnitude / 3_600_000_000;
        let minutes = (magnitude % 3_600_000_000) / 60_000_000;
        let seconds = (magnitude % 60_000_000) / 1_000_000;
        let frac = magnitude % 1_000_000;
        let mut clock = format!(
            "{}{:02}:{:02}:{:02}",
            if negative { "-" } else { "" },
            hours,
            minutes,
            seconds
        );
        if frac != 0 {
            clock.push_str(format!(".{:06}", frac).trim_end_matches('0'));
        }
        parts.push(clock);
    }

    Some(parts.join(" "))
}

// ------------------------------------------------------------- MAC ADDRESS

/// Parses a MAC address into `WIDTH` octets.
///
/// Every separator style PostgreSQL accepts is taken — `:`, `-`, `.` and none
/// at all — by reading hex digits and ignoring the punctuation between them.
/// The digit count, not the punctuation, is what decides whether the input fits
/// the column.
pub fn parse_macaddr<const WIDTH: usize>(text: &str) -> Option<[u8; WIDTH]> {
    let mut out = [0u8; WIDTH];
    let mut nibbles = 0usize;
    let mut high: Option<u8> = None;

    for c in text.trim().chars() {
        match c {
            ':' | '-' | '.' | ' ' => continue,
            _ => {}
        }
        let digit = c.to_digit(16)? as u8;
        match high {
            None => high = Some(digit),
            Some(hi) => {
                let index = nibbles / 2;
                if index >= WIDTH {
                    return None;
                }
                out[index] = (hi << 4) | digit;
                high = None;
            }
        }
        nibbles += 1;
    }

    if high.is_some() || nibbles != WIDTH * 2 {
        return None;
    }
    Some(out)
}

/// The canonical `08:00:2b:01:02:03` form.
pub fn format_macaddr(bytes: &[u8]) -> String {
    // A nibble table into a buffer sized once, rather than a `String` per
    // byte and a `Vec` to join them.
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            out.push(':');
        }
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod format_macaddr_tests {
    #[test]
    fn matches_the_per_byte_format() {
        for len in [0usize, 1, 6, 8] {
            let bytes: Vec<u8> = (0..len as u8).map(|i| i.wrapping_mul(53).wrapping_add(7)).collect();
            let expected = bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(":");
            assert_eq!(super::format_macaddr(&bytes), expected);
        }
        assert_eq!(super::format_macaddr(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0xff]), "08:00:2b:01:02:ff");
    }
}

// -------------------------------------------------------------- BIT STRING

/// Parses `'10110'` into PostgreSQL's binary bit-string layout: an `i32`
/// big-endian bit count, then the bits packed most-significant-first with the
/// final byte zero-padded.
///
/// `declared` is the column's `BIT(n)` width. A fixed-width bit string is
/// zero-*extended* to it, which is what PostgreSQL does on input; `BIT VARYING`
/// passes `None` and keeps whatever length it was given.
pub fn parse_bit_string(text: &str, declared: Option<u32>) -> Option<Vec<u8>> {
    let text = text.trim();
    // `B'1010'` and `X'ff'` prefixes, as a literal may carry them.
    let (text, hex) = if let Some(rest) = text.strip_prefix(['b', 'B']) {
        (strip_quotes(rest), false)
    } else if let Some(rest) = text.strip_prefix(['x', 'X']) {
        (strip_quotes(rest), true)
    } else {
        (text, false)
    };

    let mut bits: Vec<bool> = Vec::new();
    if hex {
        for c in text.chars() {
            let digit = c.to_digit(16)?;
            for shift in (0..4).rev() {
                bits.push((digit >> shift) & 1 == 1);
            }
        }
    } else {
        for c in text.chars() {
            match c {
                '0' => bits.push(false),
                '1' => bits.push(true),
                _ => return None,
            }
        }
    }

    if let Some(width) = declared {
        let width = width as usize;
        if bits.len() > width {
            return None;
        }
        bits.resize(width, false);
    }

    Some(encode_bits(&bits))
}

fn encode_bits(bits: &[bool]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + bits.len().div_ceil(8));
    out.extend_from_slice(&(bits.len() as i32).to_be_bytes());
    let mut byte = 0u8;
    for (i, bit) in bits.iter().enumerate() {
        if *bit {
            byte |= 0x80 >> (i % 8);
        }
        if i % 8 == 7 {
            out.push(byte);
            byte = 0;
        }
    }
    if bits.len() % 8 != 0 {
        out.push(byte);
    }
    out
}

pub fn format_bit_string(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return Some(String::new());
    }
    let length = be_i32(bytes, 0)?;
    if length < 0 {
        return None;
    }
    let length = length as usize;
    let payload = bytes.get(4..)?;
    if payload.len() < length.div_ceil(8) {
        return None;
    }
    let mut out = String::with_capacity(length);
    for i in 0..length {
        let byte = payload[i / 8];
        out.push(if byte & (0x80 >> (i % 8)) != 0 {
            '1'
        } else {
            '0'
        });
    }
    Some(out)
}

// ------------------------------------------------------- DATE / TIME / TIMETZ

/// Parses `YYYY-MM-DD` into days from the PostgreSQL epoch.
///
/// A timestamp is accepted too and its time of day dropped, because that is
/// what an assignment to a `DATE` column does in PostgreSQL and a client
/// sending `'2026-08-26 00:00:00'` for a date should not be an error.
pub fn parse_date(text: &str) -> Option<i32> {
    let text = text.trim();
    let date_part = text.split([' ', 'T']).next()?;
    let mut parts = date_part.split('-');
    // A leading `-` is a BC year; the split then yields an empty first field.
    let (negative, first) = match parts.next()? {
        "" => (true, parts.next()?),
        other => (false, other),
    };
    let year: i64 = first.parse().ok()?;
    let year = if negative { -year } else { year };
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // `2026-02-30` is refused, not rolled over to 2 March.
    if day > crate::core::value_codec::days_in_month(year, month) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    i32::try_from(days - PG_EPOCH_DAYS as i64).ok()
}

pub fn format_date(bytes: &[u8]) -> Option<String> {
    let days = be_i32(bytes, 0)?;
    let (year, month, day) = civil_from_days(days as i64 + PG_EPOCH_DAYS as i64);
    Some(format!("{:04}-{:02}-{:02}", year, month, day))
}

/// Parses `HH:MM[:SS[.ffffff]]` into microseconds since midnight.
pub fn parse_time(text: &str) -> Option<i64> {
    let text = text.trim();
    // Tolerate a full timestamp, taking its time of day.
    let clock = if text.contains(' ') {
        text.split(' ').nth(1)?
    } else if text.len() > 10 && text.as_bytes().get(10) == Some(&b'T') {
        &text[11..]
    } else {
        text
    };
    let clock = strip_zone(clock).0;
    let (micros, negative) = parse_clock(clock)?;
    if negative || !(0..86_400_000_000).contains(&micros) {
        return None;
    }
    Some(micros)
}

pub fn format_time(bytes: &[u8]) -> Option<String> {
    let micros = be_i64(bytes, 0)?;
    Some(format_time_of_day(micros))
}

fn format_time_of_day(micros: i64) -> String {
    let hours = micros / 3_600_000_000;
    let minutes = (micros % 3_600_000_000) / 60_000_000;
    let seconds = (micros % 60_000_000) / 1_000_000;
    let frac = micros % 1_000_000;
    let mut out = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
    if frac != 0 {
        out.push_str(format!(".{:06}", frac).trim_end_matches('0'));
    }
    out
}

/// Splits a trailing UTC offset off a time, returning the offset in seconds
/// *east* of Greenwich — the sign the text form prints.
fn strip_zone(text: &str) -> (&str, Option<i32>) {
    let text = text.trim();
    if let Some(rest) = text.strip_suffix(['Z', 'z']) {
        return (rest, Some(0));
    }
    // Scan from the right for the sign that begins the offset, which cannot be
    // the first character of a time.
    let bytes = text.as_bytes();
    for i in (1..bytes.len()).rev() {
        if bytes[i] == b'+' || bytes[i] == b'-' {
            let (body, zone) = text.split_at(i);
            let sign = if zone.starts_with('-') { -1 } else { 1 };
            let zone = &zone[1..];
            let mut parts = zone.split(':');
            let hours: i32 = match parts.next().and_then(|h| h.parse().ok()) {
                Some(h) => h,
                None => return (text, None),
            };
            let minutes: i32 = parts.next().and_then(|m| m.parse().ok()).unwrap_or(0);
            let seconds: i32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            return (body, Some(sign * (hours * 3600 + minutes * 60 + seconds)));
        }
    }
    (text, None)
}

/// Parses a `TIMETZ` into its 12 wire bytes.
///
/// The stored offset is seconds *west* of Greenwich, negated from the text
/// form, because that is the convention PostgreSQL's binary format uses. Both
/// halves are kept: normalising to UTC would make `13:45+02` and `11:45+00`
/// indistinguishable, and PostgreSQL prints them differently.
pub fn parse_timetz(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    let clock = if text.contains(' ') {
        text.split(' ').nth(1)?
    } else {
        text
    };
    let (body, zone) = strip_zone(clock);
    let (micros, negative) = parse_clock(body)?;
    if negative || !(0..=86_400_000_000).contains(&micros) {
        return None;
    }
    let offset_east = zone.unwrap_or(0);
    let mut out = Vec::with_capacity(TIMETZ_LEN);
    out.extend_from_slice(&micros.to_be_bytes());
    out.extend_from_slice(&(-offset_east).to_be_bytes());
    Some(out)
}

pub fn format_timetz(bytes: &[u8]) -> Option<String> {
    let micros = be_i64(bytes, 0)?;
    let offset_west = be_i32(bytes, 8)?;
    let offset_east = -offset_west;
    let sign = if offset_east < 0 { '-' } else { '+' };
    let magnitude = offset_east.abs();
    let hours = magnitude / 3600;
    let minutes = (magnitude % 3600) / 60;
    let mut out = format_time_of_day(micros);
    out.push(sign);
    out.push_str(&format!("{:02}", hours));
    if minutes != 0 {
        out.push_str(&format!(":{:02}", minutes));
    }
    Some(out)
}

/// A `TIMETZ`'s instant in UTC, for ordering. PostgreSQL sorts `timetz` by the
/// UTC-normalised time and only then by the offset.
pub fn timetz_sort_key(bytes: &[u8]) -> Option<(i64, i32)> {
    let micros = be_i64(bytes, 0)?;
    let offset_west = be_i32(bytes, 8)?;
    Some((micros + offset_west as i64 * 1_000_000, offset_west))
}

// -------------------------------------------------------------- GEOMETRIC

/// Parses a list of `f64` out of a geometric literal, ignoring the bracketing
/// punctuation PostgreSQL allows to vary.
///
/// `'(1,2)'`, `'1,2'`, `'[(0,0),(1,1)]'` and `'<(5,5),20>'` all reduce to their
/// numbers, and the type decides how many it needs. Doing it this way is what
/// lets one reader serve all seven types without a grammar per type.
fn parse_coordinates(text: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.trim().chars() {
        match c {
            '(' | ')' | '[' | ']' | '<' | '>' | '{' | '}' | ',' | ' ' | '\t' => {
                if !current.is_empty() {
                    out.push(current.parse().ok()?);
                    current.clear();
                }
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        out.push(current.parse().ok()?);
    }
    Some(out)
}

fn encode_f64s(values: &[f64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values {
        out.extend_from_slice(&v.to_be_bytes());
    }
    out
}

fn decode_f64s(bytes: &[u8], at: usize, count: usize) -> Option<Vec<f64>> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(be_f64(bytes, at + i * 8)?);
    }
    Some(out)
}

/// `POINT`: two `f64`, big-endian.
pub fn parse_point(text: &str) -> Option<Vec<u8>> {
    let coords = parse_coordinates(text)?;
    if coords.len() != 2 {
        return None;
    }
    Some(encode_f64s(&coords))
}

pub fn format_point(bytes: &[u8]) -> Option<String> {
    let c = decode_f64s(bytes, 0, 2)?;
    Some(format!("({},{})", format_f64(c[0]), format_f64(c[1])))
}

/// `LSEG`: the two endpoints.
pub fn parse_lseg(text: &str) -> Option<Vec<u8>> {
    let coords = parse_coordinates(text)?;
    if coords.len() != 4 {
        return None;
    }
    Some(encode_f64s(&coords))
}

pub fn format_lseg(bytes: &[u8]) -> Option<String> {
    let c = decode_f64s(bytes, 0, 4)?;
    Some(format!(
        "[({},{}),({},{})]",
        format_f64(c[0]),
        format_f64(c[1]),
        format_f64(c[2]),
        format_f64(c[3])
    ))
}

/// `BOX`: two corners. PostgreSQL normalises them so the first is the upper
/// right, which is what makes two boxes written corner-swapped compare equal.
pub fn parse_box(text: &str) -> Option<Vec<u8>> {
    let c = parse_coordinates(text)?;
    if c.len() != 4 {
        return None;
    }
    let (x1, x2) = if c[0] >= c[2] {
        (c[0], c[2])
    } else {
        (c[2], c[0])
    };
    let (y1, y2) = if c[1] >= c[3] {
        (c[1], c[3])
    } else {
        (c[3], c[1])
    };
    Some(encode_f64s(&[x1, y1, x2, y2]))
}

pub fn format_box(bytes: &[u8]) -> Option<String> {
    let c = decode_f64s(bytes, 0, 4)?;
    Some(format!(
        "({},{}),({},{})",
        format_f64(c[0]),
        format_f64(c[1]),
        format_f64(c[2]),
        format_f64(c[3])
    ))
}

/// `LINE`: the coefficients of `Ax + By + C = 0`.
///
/// Written as two points, which is the form PostgreSQL accepts and the one a
/// person writes, the coefficients are derived from them.
pub fn parse_line(text: &str) -> Option<Vec<u8>> {
    let c = parse_coordinates(text)?;
    match c.len() {
        3 => Some(encode_f64s(&c)),
        4 => {
            let (x1, y1, x2, y2) = (c[0], c[1], c[2], c[3]);
            if x1 == x2 && y1 == y2 {
                return None;
            }
            let a = y2 - y1;
            let b = x1 - x2;
            let cc = -(a * x1 + b * y1);
            Some(encode_f64s(&[a, b, cc]))
        }
        _ => None,
    }
}

pub fn format_line(bytes: &[u8]) -> Option<String> {
    let c = decode_f64s(bytes, 0, 3)?;
    Some(format!(
        "{{{},{},{}}}",
        format_f64(c[0]),
        format_f64(c[1]),
        format_f64(c[2])
    ))
}

/// `CIRCLE`: centre and radius, `<(x,y),r>`.
pub fn parse_circle(text: &str) -> Option<Vec<u8>> {
    let c = parse_coordinates(text)?;
    if c.len() != 3 || c[2] < 0.0 {
        return None;
    }
    Some(encode_f64s(&c))
}

pub fn format_circle(bytes: &[u8]) -> Option<String> {
    let c = decode_f64s(bytes, 0, 3)?;
    Some(format!(
        "<({},{}),{}>",
        format_f64(c[0]),
        format_f64(c[1]),
        format_f64(c[2])
    ))
}

/// `PATH`: a closedness flag, a point count, then the points.
///
/// Square brackets are an open path and parentheses a closed one, which is the
/// only thing distinguishing the two in the text form.
pub fn parse_path(text: &str) -> Option<Vec<u8>> {
    let trimmed = text.trim();
    let closed = !trimmed.starts_with('[');
    let coords = parse_coordinates(trimmed)?;
    if coords.is_empty() || coords.len() % 2 != 0 {
        return None;
    }
    let points = coords.len() / 2;
    let mut out = Vec::with_capacity(5 + coords.len() * 8);
    out.push(if closed { 1 } else { 0 });
    out.extend_from_slice(&(points as i32).to_be_bytes());
    out.extend_from_slice(&encode_f64s(&coords));
    Some(out)
}

pub fn format_path(bytes: &[u8]) -> Option<String> {
    let closed = *bytes.first()? != 0;
    let points = be_i32(bytes, 1)?;
    if points < 0 {
        return None;
    }
    let coords = decode_f64s(bytes, 5, points as usize * 2)?;
    let body = coords
        .chunks(2)
        .map(|p| format!("({},{})", format_f64(p[0]), format_f64(p[1])))
        .collect::<Vec<_>>()
        .join(",");
    Some(if closed {
        format!("({})", body)
    } else {
        format!("[{}]", body)
    })
}

/// `POLYGON`: a point count then the points. Always closed, so it carries no
/// flag.
pub fn parse_polygon(text: &str) -> Option<Vec<u8>> {
    let coords = parse_coordinates(text)?;
    if coords.is_empty() || coords.len() % 2 != 0 {
        return None;
    }
    let points = coords.len() / 2;
    let mut out = Vec::with_capacity(4 + coords.len() * 8);
    out.extend_from_slice(&(points as i32).to_be_bytes());
    out.extend_from_slice(&encode_f64s(&coords));
    Some(out)
}

pub fn format_polygon(bytes: &[u8]) -> Option<String> {
    let points = be_i32(bytes, 0)?;
    if points < 0 {
        return None;
    }
    let coords = decode_f64s(bytes, 4, points as usize * 2)?;
    let body = coords
        .chunks(2)
        .map(|p| format!("({},{})", format_f64(p[0]), format_f64(p[1])))
        .collect::<Vec<_>>()
        .join(",");
    Some(format!("({})", body))
}

/// The points of any geometric value, for the operators that work on all of
/// them. `None` for a `LINE`, whose stored form is coefficients rather than
/// points.
pub fn geometry_points(db_type: &DbType, bytes: &[u8]) -> Option<Vec<(f64, f64)>> {
    let coords = match db_type {
        DbType::POINT => decode_f64s(bytes, 0, 2)?,
        DbType::LSEG | DbType::BOX => decode_f64s(bytes, 0, 4)?,
        DbType::CIRCLE => {
            let c = decode_f64s(bytes, 0, 3)?;
            vec![c[0], c[1]]
        }
        DbType::PATH => {
            let points = be_i32(bytes, 1)?.max(0) as usize;
            decode_f64s(bytes, 5, points * 2)?
        }
        DbType::POLYGON => {
            let points = be_i32(bytes, 0)?.max(0) as usize;
            decode_f64s(bytes, 4, points * 2)?
        }
        _ => return None,
    };
    Some(
        coords
            .chunks(2)
            .filter(|c| c.len() == 2)
            .map(|c| (c[0], c[1]))
            .collect(),
    )
}

/// A circle's centre and radius.
pub fn circle_parts(bytes: &[u8]) -> Option<(f64, f64, f64)> {
    let c = decode_f64s(bytes, 0, 3)?;
    Some((c[0], c[1], c[2]))
}

/// The axis-aligned bounding box of a geometric value, as
/// `(min_x, min_y, max_x, max_y)`.
///
/// Overlap and containment for the shapes are answered on bounding boxes rather
/// than on exact geometry. That is what PostgreSQL's own `&&` does for every
/// type, and for `<@` it is an approximation this documents rather than hides:
/// a polygon that is inside another's box but outside the polygon itself will
/// be reported as contained.
pub fn bounding_box(db_type: &DbType, bytes: &[u8]) -> Option<(f64, f64, f64, f64)> {
    if matches!(db_type, DbType::CIRCLE) {
        let (x, y, r) = circle_parts(bytes)?;
        return Some((x - r, y - r, x + r, y + r));
    }
    let points = geometry_points(db_type, bytes)?;
    if points.is_empty() {
        return None;
    }
    let mut min_x = f64::INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for (x, y) in points {
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
    }
    Some((min_x, min_y, max_x, max_y))
}

// ------------------------------------------------------------------ RANGES

/// The range flag byte PostgreSQL puts first in the binary form.
pub mod range_flags {
    pub const EMPTY: u8 = 0x01;
    pub const LB_INC: u8 = 0x02;
    pub const UB_INC: u8 = 0x04;
    pub const LB_INF: u8 = 0x08;
    pub const UB_INF: u8 = 0x10;
}

/// A range's two bounds, decoded to the element's text form.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeValue {
    pub empty: bool,
    /// `None` is an infinite bound.
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}

/// The element type a range type ranges over.
pub fn range_element(db_type: &DbType) -> Option<DbType> {
    Some(match db_type {
        DbType::INT4RANGE => DbType::I32,
        DbType::INT8RANGE => DbType::I64,
        DbType::NUMRANGE => DbType::DECIMAL,
        DbType::TSRANGE | DbType::TSTZRANGE => DbType::DATETIME,
        DbType::DATERANGE => DbType::DATE,
        _ => return None,
    })
}

/// Whether a range type's element has a step, so `[1,10]` and `[1,11)` are the
/// same range and PostgreSQL stores both in the second form.
///
/// Canonicalising on input is not cosmetic: without it `int4range(1,10,'[]')`
/// and `int4range(1,11,'[)')` would compare unequal and one would answer a
/// containment test the other refused.
fn range_is_discrete(db_type: &DbType) -> bool {
    matches!(
        db_type,
        DbType::INT4RANGE | DbType::INT8RANGE | DbType::DATERANGE
    )
}

/// One element of a range, in the element type's binary wire form.
fn encode_range_element(element: &DbType, text: &str) -> Option<Vec<u8>> {
    let text = strip_quotes(text.trim());
    match element {
        DbType::I32 => Some(text.parse::<i32>().ok()?.to_be_bytes().to_vec()),
        DbType::I64 => Some(text.parse::<i64>().ok()?.to_be_bytes().to_vec()),
        DbType::DECIMAL => {
            let unscaled = crate::core::value_codec::parse_decimal(text)?;
            Some(crate::core::value_codec::encode_pg_numeric(unscaled))
        }
        DbType::DATETIME => {
            let us = crate::core::value_codec::parse_timestamp_us(text)?;
            Some(
                (us - crate::core::value_codec::PG_EPOCH_OFFSET_US)
                    .to_be_bytes()
                    .to_vec(),
            )
        }
        DbType::DATE => Some(parse_date(text)?.to_be_bytes().to_vec()),
        _ => None,
    }
}

fn format_range_element(element: &DbType, bytes: &[u8]) -> Option<String> {
    match element {
        DbType::I32 => Some(be_i32(bytes, 0)?.to_string()),
        DbType::I64 => Some(be_i64(bytes, 0)?.to_string()),
        DbType::DECIMAL => decode_pg_numeric(bytes),
        DbType::DATETIME => {
            let us = be_i64(bytes, 0)? + crate::core::value_codec::PG_EPOCH_OFFSET_US;
            Some(format!(
                "\"{}\"",
                crate::core::value_codec::format_timestamp_us(us)
            ))
        }
        DbType::DATE => format_date(bytes),
        _ => None,
    }
}

/// A range element as a number, for the containment and overlap operators.
///
/// One `i128` scale carries every element type a built-in range ranges over:
/// integers as themselves, timestamps and dates as microseconds and days, and
/// numerics as their unscaled units. Comparisons only ever happen between two
/// values of the *same* element type, so the differing scales never meet.
pub fn range_element_key(element: &DbType, bytes: &[u8]) -> Option<i128> {
    match element {
        DbType::I32 => Some(be_i32(bytes, 0)? as i128),
        DbType::I64 => Some(be_i64(bytes, 0)? as i128),
        DbType::DATETIME => Some(be_i64(bytes, 0)? as i128),
        DbType::DATE => Some(be_i32(bytes, 0)? as i128),
        DbType::DECIMAL => {
            let text = decode_pg_numeric(bytes)?;
            crate::core::value_codec::parse_decimal(&text)
        }
        _ => None,
    }
}

/// Reads PostgreSQL's binary `numeric` back to text.
///
/// The inverse of [`crate::core::value_codec::encode_pg_numeric_scaled`], and
/// here rather than beside it because a range bound is the only place the
/// engine has to read that format rather than write it.
pub fn decode_pg_numeric(bytes: &[u8]) -> Option<String> {
    let count = i16::from_be_bytes(bytes.get(0..2)?.try_into().ok()?);
    let weight = i16::from_be_bytes(bytes.get(2..4)?.try_into().ok()?);
    let sign = u16::from_be_bytes(bytes.get(4..6)?.try_into().ok()?);
    let dscale = i16::from_be_bytes(bytes.get(6..8)?.try_into().ok()?);
    if count < 0 || dscale < 0 {
        return None;
    }

    let mut digits = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        digits.push(u16::from_be_bytes(
            bytes.get(8 + i * 2..10 + i * 2)?.try_into().ok()?,
        ));
    }

    // Lay the base-10000 groups out on a decimal-digit line, then cut it at the
    // point `weight` puts the radix.
    let mut integral = String::new();
    let mut fractional = String::new();
    for (i, group) in digits.iter().enumerate() {
        let position = weight as i32 - i as i32;
        let text = if i == 0 {
            group.to_string()
        } else {
            format!("{:04}", group)
        };
        if position >= 0 {
            if position > 0 && i == 0 {
                integral.push_str(&text);
                // Groups between the leading one and the radix are full width.
                for _ in 0..position - 1 {
                    // Filled in by later iterations; this only reserves nothing.
                }
            } else {
                integral.push_str(&text);
            }
        } else {
            // A gap between the radix and the first fractional group is zeroes.
            let expected = (-position - 1) as usize * 4;
            while fractional.len() < expected {
                fractional.push('0');
            }
            fractional.push_str(&format!("{:04}", group));
        }
    }
    // Groups the value skipped over between its last digit group and the radix.
    let integral_groups = (weight as i32 + 1).max(0) as usize;
    let present = digits.len().min(integral_groups);
    if integral_groups > present {
        for _ in 0..(integral_groups - present) {
            integral.push_str("0000");
        }
    }
    if integral.is_empty() {
        integral.push('0');
    }

    while fractional.len() < dscale as usize {
        fractional.push('0');
    }
    fractional.truncate(dscale as usize);

    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&integral);
    if !fractional.is_empty() {
        out.push('.');
        out.push_str(&fractional);
    }
    Some(out)
}

/// Parses a range literal — `[1,10)`, `(,5]`, `empty` — into the binary form.
pub fn parse_range(db_type: &DbType, text: &str) -> Option<Vec<u8>> {
    let element = range_element(db_type)?;
    let text = text.trim();

    if text.eq_ignore_ascii_case("empty") {
        return Some(vec![range_flags::EMPTY]);
    }

    let open = text.chars().next()?;
    let close = text.chars().last()?;
    let mut lower_inclusive = match open {
        '[' => true,
        '(' => false,
        _ => return None,
    };
    let mut upper_inclusive = match close {
        ']' => true,
        ')' => false,
        _ => return None,
    };

    let body = &text[1..text.len() - 1];
    let parts = split_top_level(body, ',');
    if parts.len() != 2 {
        return None;
    }
    let raw_lower = parts[0].trim();
    let raw_upper = parts[1].trim();

    let mut lower = if raw_lower.is_empty() {
        None
    } else {
        Some(encode_range_element(&element, raw_lower)?)
    };
    let mut upper = if raw_upper.is_empty() {
        None
    } else {
        Some(encode_range_element(&element, raw_upper)?)
    };

    if lower.is_none() {
        lower_inclusive = false;
    }
    if upper.is_none() {
        upper_inclusive = false;
    }

    // A discrete range is stored `[)`, so two spellings of the same set of
    // values become the same bytes.
    if range_is_discrete(db_type) {
        if !lower_inclusive {
            if let Some(bytes) = &lower {
                lower = Some(step_element(&element, bytes, 1)?);
                lower_inclusive = true;
            }
        }
        if upper_inclusive {
            if let Some(bytes) = &upper {
                upper = Some(step_element(&element, bytes, 1)?);
                upper_inclusive = false;
            }
        }
    }

    // A range whose bounds cross is empty, and PostgreSQL stores it as such
    // rather than as a range nothing satisfies.
    if let (Some(lo), Some(hi)) = (&lower, &upper) {
        let (lo_key, hi_key) = (
            range_element_key(&element, lo)?,
            range_element_key(&element, hi)?,
        );
        let crossed =
            lo_key > hi_key || (lo_key == hi_key && !(lower_inclusive && upper_inclusive));
        if crossed {
            return Some(vec![range_flags::EMPTY]);
        }
    }

    let mut flags = 0u8;
    if lower_inclusive {
        flags |= range_flags::LB_INC;
    }
    if upper_inclusive {
        flags |= range_flags::UB_INC;
    }
    if lower.is_none() {
        flags |= range_flags::LB_INF;
    }
    if upper.is_none() {
        flags |= range_flags::UB_INF;
    }

    let mut out = vec![flags];
    for bound in [&lower, &upper] {
        if let Some(bytes) = bound {
            out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
    }
    Some(out)
}

fn step_element(element: &DbType, bytes: &[u8], by: i64) -> Option<Vec<u8>> {
    match element {
        DbType::I32 => Some(
            be_i32(bytes, 0)?
                .checked_add(by as i32)?
                .to_be_bytes()
                .to_vec(),
        ),
        DbType::I64 => Some(be_i64(bytes, 0)?.checked_add(by)?.to_be_bytes().to_vec()),
        DbType::DATE => Some(
            be_i32(bytes, 0)?
                .checked_add(by as i32)?
                .to_be_bytes()
                .to_vec(),
        ),
        _ => None,
    }
}

pub fn decode_range(db_type: &DbType, bytes: &[u8]) -> Option<RangeValue> {
    let _ = range_element(db_type)?;
    let flags = *bytes.first()?;
    if flags & range_flags::EMPTY != 0 {
        return Some(RangeValue {
            empty: true,
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        });
    }

    let mut at = 1usize;
    let read_bound = |present: bool, at: &mut usize| -> Option<Option<Vec<u8>>> {
        if !present {
            return Some(None);
        }
        let length = be_i32(bytes, *at)?;
        if length < 0 {
            return None;
        }
        let start = *at + 4;
        let end = start + length as usize;
        let value = bytes.get(start..end)?.to_vec();
        *at = end;
        Some(Some(value))
    };

    let lower = read_bound(flags & range_flags::LB_INF == 0, &mut at)?;
    let upper = read_bound(flags & range_flags::UB_INF == 0, &mut at)?;

    Some(RangeValue {
        empty: false,
        lower,
        upper,
        lower_inclusive: flags & range_flags::LB_INC != 0,
        upper_inclusive: flags & range_flags::UB_INC != 0,
    })
}

pub fn format_range(db_type: &DbType, bytes: &[u8]) -> Option<String> {
    let element = range_element(db_type)?;
    let range = decode_range(db_type, bytes)?;
    if range.empty {
        return Some("empty".to_string());
    }
    let lower = match &range.lower {
        Some(b) => format_range_element(&element, b)?,
        None => String::new(),
    };
    let upper = match &range.upper {
        Some(b) => format_range_element(&element, b)?,
        None => String::new(),
    };
    Some(format!(
        "{}{},{}{}",
        if range.lower_inclusive { '[' } else { '(' },
        lower,
        upper,
        if range.upper_inclusive { ']' } else { ')' }
    ))
}

/// Whether a range contains a single element value, given as its key.
pub fn range_contains_key(db_type: &DbType, bytes: &[u8], key: i128) -> Option<bool> {
    let element = range_element(db_type)?;
    let range = decode_range(db_type, bytes)?;
    if range.empty {
        return Some(false);
    }
    if let Some(lower) = &range.lower {
        let bound = range_element_key(&element, lower)?;
        if key < bound || (key == bound && !range.lower_inclusive) {
            return Some(false);
        }
    }
    if let Some(upper) = &range.upper {
        let bound = range_element_key(&element, upper)?;
        if key > bound || (key == bound && !range.upper_inclusive) {
            return Some(false);
        }
    }
    Some(true)
}

/// The `(lower, upper)` keys of a range, with an infinite bound as the
/// saturating end of the scale, for overlap and containment between ranges.
pub fn range_bounds_key(db_type: &DbType, bytes: &[u8]) -> Option<(i128, i128, bool, bool)> {
    let element = range_element(db_type)?;
    let range = decode_range(db_type, bytes)?;
    if range.empty {
        return None;
    }
    let lower = match &range.lower {
        Some(b) => range_element_key(&element, b)?,
        None => i128::MIN,
    };
    let upper = match &range.upper {
        Some(b) => range_element_key(&element, b)?,
        None => i128::MAX,
    };
    Some((lower, upper, range.lower_inclusive, range.upper_inclusive))
}
