//! Calendar arithmetic for the two places backup needs it: the `x-amz-date`
//! header a signature is computed over, and the human-readable timestamps a
//! `RESTORE ... AT TIME` is resolved against.
//!
//! Hand-rolled rather than pulled in: a date library would be a dependency the
//! rest of the engine has managed without, for a job that is proleptic
//! Gregorian conversion in UTC and nothing else — no zones, no leap seconds,
//! no locales.

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        // A clock behind the epoch is a misconfigured machine, not a state
        // worth propagating into every timestamp downstream.
        .unwrap_or(0)
}

/// Days since 1970-01-01 to (year, month, day).
///
/// Howard Hinnant's `civil_from_days`: shifts the era so that the leap-year
/// pattern is a plain repeating cycle, which removes every branch a
/// month-length table would need.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The inverse of [`civil_from_days`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

fn split(unix: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (unix / 86_400) as i64;
    let secs_of_day = unix % 86_400;
    let (y, m, d) = civil_from_days(days);
    (
        y,
        m,
        d,
        (secs_of_day / 3600) as u32,
        ((secs_of_day % 3600) / 60) as u32,
        (secs_of_day % 60) as u32,
    )
}

/// `YYYYMMDDTHHMMSSZ` — the ISO 8601 basic form SigV4 signs over.
pub fn format_basic(unix: u64) -> String {
    let (y, mo, d, h, mi, s) = split(unix);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// `YYYY-MM-DDTHH:MM:SSZ` — what a manifest records and an operator reads.
pub fn format_rfc3339(unix: u64) -> String {
    let (y, mo, d, h, mi, s) = split(unix);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Parses the subset of RFC 3339 a `RESTORE ... AT TIME` can name.
///
/// Accepts `YYYY-MM-DD`, `YYYY-MM-DD HH:MM[:SS]`, the same with `T`, an
/// optional fractional part, and either `Z` or a `±HH:MM` offset. Anything
/// without an offset is read as UTC, which is what every timestamp this module
/// writes is in.
pub fn parse_rfc3339(text: &str) -> Option<u64> {
    let text = text.trim();
    let (datetime, offset_secs) = split_offset(text)?;
    let (date, time) = match datetime.split_once(['T', 't', ' ']) {
        Some((d, t)) => (d, t),
        None => (datetime, "00:00:00"),
    };

    let mut date_parts = date.split('-');
    let y: i64 = date_parts.next()?.parse().ok()?;
    let mo: u32 = date_parts.next()?.parse().ok()?;
    let d: u32 = date_parts.next()?.parse().ok()?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }

    // A fractional second names a point inside the second we would round to;
    // dropping it floors, which is the conservative direction for a recovery
    // target.
    let time = time.split('.').next()?;
    let mut time_parts = time.split(':');
    let h: u32 = time_parts.next()?.parse().ok()?;
    let mi: u32 = time_parts.next().unwrap_or("0").parse().ok()?;
    let s: u32 = time_parts.next().unwrap_or("0").parse().ok()?;
    if h > 23 || mi > 59 || s > 60 {
        return None;
    }

    let days = days_from_civil(y, mo, d);
    let total = days * 86_400 + (h * 3600 + mi * 60 + s) as i64 - offset_secs;
    u64::try_from(total).ok()
}

/// Splits a trailing zone designator off, returning its offset in seconds.
fn split_offset(text: &str) -> Option<(&str, i64)> {
    if let Some(stripped) = text.strip_suffix(['Z', 'z']) {
        return Some((stripped, 0));
    }

    // The sign of an offset can only appear after the time, never inside the
    // date, so searching from the right cannot hit the date's separators.
    let bytes = text.as_bytes();
    for i in (1..bytes.len()).rev() {
        match bytes[i] {
            b'+' | b'-' => {
                // A '-' before any 'T'/space is part of the date.
                let has_time = text[..i].contains(['T', 't', ' ']);
                if !has_time && bytes[i] == b'-' {
                    break;
                }
                let zone = &text[i + 1..];
                let (hh, mm) = match zone.split_once(':') {
                    Some((h, m)) => (h, m),
                    None if zone.len() == 4 => (&zone[..2], &zone[2..]),
                    None => (zone, "0"),
                };
                let secs = hh.parse::<i64>().ok()? * 3600 + mm.parse::<i64>().ok()? * 60;
                let signed = if bytes[i] == b'-' { -secs } else { secs };
                return Some((&text[..i], signed));
            }
            _ => {}
        }
    }

    Some((text, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_the_epoch() {
        assert_eq!(format_basic(0), "19700101T000000Z");
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn round_trips_a_known_instant() {
        // 2026-08-20T12:34:56Z
        let unix = parse_rfc3339("2026-08-20T12:34:56Z").unwrap();
        assert_eq!(format_rfc3339(unix), "2026-08-20T12:34:56Z");
        assert_eq!(format_basic(unix), "20260820T123456Z");
    }

    #[test]
    fn handles_leap_days() {
        let unix = parse_rfc3339("2024-02-29T00:00:00Z").unwrap();
        assert_eq!(format_rfc3339(unix), "2024-02-29T00:00:00Z");
    }

    #[test]
    fn accepts_relaxed_forms() {
        let z = parse_rfc3339("2026-08-20T12:00:00Z").unwrap();
        assert_eq!(parse_rfc3339("2026-08-20 12:00:00").unwrap(), z);
        assert_eq!(parse_rfc3339("2026-08-20T12:00").unwrap(), z);
        assert_eq!(parse_rfc3339("2026-08-20T12:00:00.123Z").unwrap(), z);
        assert_eq!(parse_rfc3339("2026-08-20").unwrap(), z - 12 * 3600);
    }

    #[test]
    fn applies_a_zone_offset() {
        let utc = parse_rfc3339("2026-08-20T12:00:00Z").unwrap();
        assert_eq!(parse_rfc3339("2026-08-20T14:00:00+02:00").unwrap(), utc);
        assert_eq!(parse_rfc3339("2026-08-20T09:00:00-03:00").unwrap(), utc);
    }

    #[test]
    fn rejects_nonsense() {
        assert!(parse_rfc3339("not a date").is_none());
        assert!(parse_rfc3339("2026-13-01").is_none());
        assert!(parse_rfc3339("2026-08-20T25:00:00Z").is_none());
    }
}
