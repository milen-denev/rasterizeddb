//! Values with no Rust standard type: UUIDs, points in time, calendar dates,
//! times of day, intervals, exact decimals and JSON documents.
//!
//! Each one parses and prints the text form PostgreSQL uses, which is also the
//! literal the server reads back, and each one knows the bytes the native
//! protocol carries for it. Those layouts are the engine's (`value_codec.rs`,
//! `pg_types.rs`):
//!
//! | type | tag | bytes |
//! |---|---|---|
//! | [`Uuid`] | 17 | the 16 RFC 4122 octets |
//! | [`Timestamp`] | 15 | `i64` µs since 1970-01-01 UTC, little-endian |
//! | [`Date`] | 29 | `i32` days since 2000-01-01, big-endian |
//! | [`Time`] | 30 | `i64` µs since midnight, big-endian |
//! | [`Interval`] | 24 | `i64` µs, `i32` days, `i32` months, big-endian |
//!
//! [`Numeric`] and [`Json`] carry text. The server sends a `NUMERIC` with its
//! scale and a `JSONB` document as its JSON text; a server that predates that
//! sends a `NUMERIC(p, s)` with no scale and a document in the engine's own
//! encoding, which are refused rather than guessed: select them as
//! `col::text` there (the ORM's queries do so already).

use std::fmt;
use std::str::FromStr;

use crate::{Error, Result};

// ------------------------------------------------------------------ calendar

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to `(y, m, d)`
/// in the proleptic Gregorian calendar. The engine's own copy is
/// `value_codec::civil_from_days`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
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

/// Inverse of [`civil_from_days`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        _ => 28,
    }
}

const US_PER_SEC: i64 = 1_000_000;
const US_PER_DAY: i64 = 86_400 * US_PER_SEC;
/// Days from 1970-01-01 to PostgreSQL's epoch, 2000-01-01.
const PG_EPOCH_DAYS: i32 = 10_957;

fn bad(kind: &str, text: &str) -> Error {
    Error::Param(format!("{text:?} is not a valid {kind}"))
}

fn parse_date_parts(text: &str) -> Option<(i64, u32, u32)> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let mut parts = body.splitn(3, '-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    let year = if negative { -year } else { year };
    ((1..=12).contains(&month) && day >= 1 && day <= days_in_month(year, month))
        .then_some((year, month, day))
}

/// `HH:MM[:SS[.ffffff]]` as microseconds since midnight. Digits past the sixth
/// are refused rather than rounded: a value parsed here is one the caller
/// wrote, and silently changing it is worse than saying no.
fn parse_clock(text: &str) -> Option<i64> {
    let mut parts = text.splitn(3, ':');
    let hour: i64 = parts.next()?.parse().ok()?;
    let minute: i64 = parts.next()?.parse().ok()?;
    let (second, micros) = match parts.next() {
        None => (0, 0),
        Some(field) => match field.split_once('.') {
            None => (field.parse().ok()?, 0),
            Some((whole, frac)) => {
                if frac.is_empty() || frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let scale = 10i64.pow(6 - frac.len() as u32);
                (whole.parse().ok()?, frac.parse::<i64>().ok()? * scale)
            }
        },
    };
    if !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    // 24:00:00 is a valid time of day in PostgreSQL; nothing past it is.
    let total = ((hour * 60 + minute) * 60 + second) * US_PER_SEC + micros;
    (0..=US_PER_DAY).contains(&total).then_some(total)
}

fn write_clock(f: &mut fmt::Formatter<'_>, micros: i64) -> fmt::Result {
    let hours = micros / 3_600_000_000;
    let minutes = (micros % 3_600_000_000) / 60_000_000;
    let seconds = (micros % 60_000_000) / 1_000_000;
    let frac = micros % 1_000_000;
    write!(f, "{hours:02}:{minutes:02}:{seconds:02}")?;
    if frac != 0 {
        let digits = format!("{frac:06}");
        write!(f, ".{}", digits.trim_end_matches('0'))?;
    }
    Ok(())
}

fn write_ymd(f: &mut fmt::Formatter<'_>, (y, m, d): (i64, u32, u32)) -> fmt::Result {
    if y < 0 {
        write!(f, "-{:04}-{m:02}-{d:02}", -y)
    } else {
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

fn fixed<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    bytes.try_into().ok()
}

// ---------------------------------------------------------------------- Uuid

/// A UUID: 16 octets, printed as `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    /// From its 16 octets.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Uuid(bytes)
    }

    /// Its 16 octets.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// From the bytes the native protocol carries (16, in order).
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        fixed(bytes).map(Uuid)
    }

    /// A random (version 4) UUID, from the operating system's generator.
    pub fn new_v4() -> Result<Self> {
        let mut bytes = [0u8; 16];
        fill_random(&mut bytes)?;
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(Uuid(bytes))
    }
}

fn fill_random(out: &mut [u8]) -> Result<()> {
    random_bytes(out).map_err(|e| Error::Param(format!("no random bytes for a UUID: {e}")))
}

/// Fills `out` from the operating system's generator.
pub(crate) fn random_bytes(out: &mut [u8]) -> std::io::Result<()> {
    let mut filled = 0;
    while filled < out.len() {
        // SAFETY: the pointer and length describe the unfilled tail of `out`.
        let n =
            unsafe { libc::getrandom(out[filled..].as_mut_ptr().cast(), out.len() - filled, 0) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        filled += n as usize;
    }
    Ok(())
}

impl FromStr for Uuid {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        let hex: Vec<u8> = text.bytes().filter(|&b| b != b'-').collect();
        if hex.len() != 32 {
            return Err(bad("UUID", text));
        }
        let nibble = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
        let mut out = [0u8; 16];
        for (i, pair) in hex.chunks(2).enumerate() {
            let (Some(hi), Some(lo)) = (nibble(pair[0]), nibble(pair[1])) else {
                return Err(bad("UUID", text));
            };
            out[i] = hi << 4 | lo;
        }
        Ok(Uuid(out))
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Uuid({self})")
    }
}

// ----------------------------------------------------------------- Timestamp

/// A point in time: microseconds since 1970-01-01 00:00:00.
///
/// The same value serves `timestamp` and `timestamptz` columns, which the
/// engine stores alike. As a parameter it is written in UTC; a
/// `timestamptz` column reads it as that instant, a `timestamp` column as that
/// wall-clock time.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Timestamp {
    /// Microseconds since 1970-01-01 00:00:00 UTC.
    pub micros: i64,
}

impl Timestamp {
    /// 1970-01-01 00:00:00 UTC.
    pub const UNIX_EPOCH: Timestamp = Timestamp { micros: 0 };

    /// From microseconds since the Unix epoch.
    pub const fn from_micros(micros: i64) -> Self {
        Timestamp { micros }
    }

    /// From seconds since the Unix epoch.
    pub const fn from_unix_seconds(seconds: i64) -> Self {
        Timestamp {
            micros: seconds * US_PER_SEC,
        }
    }

    /// Microseconds since the Unix epoch.
    pub const fn as_micros(self) -> i64 {
        self.micros
    }

    /// Now, by the system clock.
    pub fn now() -> Self {
        Self::from(std::time::SystemTime::now())
    }

    /// From the eight little-endian bytes the native protocol carries.
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        fixed(bytes).map(|b| Timestamp {
            micros: i64::from_le_bytes(b),
        })
    }

    /// From a calendar date and time of day (UTC); `None` for one that does not exist.
    pub fn from_ymd_hms(year: i64, month: u32, day: u32, h: u32, m: u32, s: u32) -> Option<Self> {
        let date = Date::from_ymd(year, month, day)?;
        if h > 23 || m > 59 || s > 59 {
            return None;
        }
        Some(Timestamp {
            micros: date.unix_days() as i64 * US_PER_DAY
                + ((h as i64 * 60 + m as i64) * 60 + s as i64) * US_PER_SEC,
        })
    }

    /// The calendar date (UTC).
    pub fn date(self) -> Date {
        Date::from_unix_days(self.micros.div_euclid(US_PER_DAY) as i32)
    }

    /// The time of day (UTC).
    pub fn time(self) -> Time {
        Time {
            micros: self.micros.rem_euclid(US_PER_DAY),
        }
    }
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(t: std::time::SystemTime) -> Self {
        let micros = match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(after) => after.as_micros() as i64,
            Err(before) => -(before.duration().as_micros() as i64),
        };
        Timestamp { micros }
    }
}

impl From<Timestamp> for std::time::SystemTime {
    fn from(t: Timestamp) -> Self {
        let magnitude = std::time::Duration::from_micros(t.micros.unsigned_abs());
        if t.micros >= 0 {
            std::time::UNIX_EPOCH + magnitude
        } else {
            std::time::UNIX_EPOCH - magnitude
        }
    }
}

/// `YYYY-MM-DD[( |T)HH:MM[:SS[.ffffff]]][Z|±HH[:MM]]`. A zone is applied: the
/// value is the UTC instant the text names.
impl FromStr for Timestamp {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        let s = text.trim();
        let err = || bad("timestamp", text);
        let (body, offset_minutes) = split_zone(s).ok_or_else(err)?;
        let (date, clock) = match body.find(['T', 't', ' ']) {
            Some(i) if i >= 8 => (&body[..i], Some(body[i + 1..].trim())),
            _ => (body, None),
        };
        let (y, m, d) = parse_date_parts(date).ok_or_else(err)?;
        let micros_of_day = match clock {
            Some(c) => parse_clock(c).ok_or_else(err)?,
            None => 0,
        };
        let micros = days_from_civil(y, m, d)
            .checked_mul(US_PER_DAY)
            .and_then(|v| v.checked_add(micros_of_day))
            .and_then(|v| v.checked_sub(offset_minutes * 60 * US_PER_SEC))
            .ok_or_else(err)?;
        Ok(Timestamp { micros })
    }
}

/// The text without its trailing zone, and the zone's offset in minutes.
fn split_zone(s: &str) -> Option<(&str, i64)> {
    if let Some(body) = s.strip_suffix(['Z', 'z']) {
        return Some((body, 0));
    }
    if let Some(body) = s.strip_suffix(" UTC") {
        return Some((body, 0));
    }
    // A sign after the time part (past `YYYY-MM-DD`) is a zone.
    match s.rfind(['+', '-']) {
        Some(pos) if pos > 10 => {
            let (head, tail) = s.split_at(pos);
            let sign = if tail.starts_with('-') { -1 } else { 1 };
            let digits = &tail[1..];
            let (hh, mm): (i64, i64) = match digits.split_once(':') {
                Some((h, m)) => (h.parse().ok()?, m.parse().ok()?),
                None if digits.len() <= 2 => (digits.parse().ok()?, 0),
                None if digits.len() == 4 => (digits[..2].parse().ok()?, digits[2..].parse().ok()?),
                None => return None,
            };
            Some((head, sign * (hh * 60 + mm)))
        }
        _ => Some((s, 0)),
    }
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]`, in UTC, with no zone written.
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.micros.div_euclid(US_PER_DAY);
        write_ymd(f, civil_from_days(days))?;
        f.write_str(" ")?;
        write_clock(f, self.micros.rem_euclid(US_PER_DAY))
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({self})")
    }
}

// ---------------------------------------------------------------------- Date

/// A calendar date, as days since 1970-01-01.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Date {
    days: i32,
}

impl Date {
    /// From days since 1970-01-01.
    pub const fn from_unix_days(days: i32) -> Self {
        Date { days }
    }

    /// Days since 1970-01-01.
    pub const fn unix_days(self) -> i32 {
        self.days
    }

    /// From year, month and day; `None` for a date that does not exist.
    pub fn from_ymd(year: i64, month: u32, day: u32) -> Option<Self> {
        if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
            return None;
        }
        i32::try_from(days_from_civil(year, month, day))
            .ok()
            .map(Date::from_unix_days)
    }

    /// `(year, month, day)`.
    pub fn ymd(self) -> (i64, u32, u32) {
        civil_from_days(self.days as i64)
    }

    /// From the four big-endian bytes the native protocol carries (days since
    /// 2000-01-01, PostgreSQL's layout).
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        let pg = i32::from_be_bytes(fixed(bytes)?);
        pg.checked_add(PG_EPOCH_DAYS).map(Date::from_unix_days)
    }
}

impl FromStr for Date {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        let (y, m, d) = parse_date_parts(text.trim()).ok_or_else(|| bad("date", text))?;
        Date::from_ymd(y, m, d).ok_or_else(|| bad("date", text))
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_ymd(f, self.ymd())
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({self})")
    }
}

// ---------------------------------------------------------------------- Time

/// A time of day, as microseconds since midnight (`0..=86_400_000_000`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Time {
    micros: i64,
}

impl Time {
    /// 00:00:00.
    pub const MIDNIGHT: Time = Time { micros: 0 };

    /// From microseconds since midnight; `None` past 24:00:00.
    pub fn from_micros(micros: i64) -> Option<Self> {
        (0..=US_PER_DAY)
            .contains(&micros)
            .then_some(Time { micros })
    }

    /// From hours, minutes, seconds and microseconds; `None` for a time that does not exist.
    pub fn from_hms_micro(h: u32, m: u32, s: u32, micro: u32) -> Option<Self> {
        if h > 23 || m > 59 || s > 59 || micro > 999_999 {
            return None;
        }
        Self::from_micros(((h as i64 * 60 + m as i64) * 60 + s as i64) * US_PER_SEC + micro as i64)
    }

    /// Microseconds since midnight.
    pub const fn as_micros(self) -> i64 {
        self.micros
    }

    /// From the eight big-endian bytes the native protocol carries.
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        Self::from_micros(i64::from_be_bytes(fixed(bytes)?))
    }
}

impl FromStr for Time {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        parse_clock(text.trim())
            .map(|micros| Time { micros })
            .ok_or_else(|| bad("time", text))
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_clock(f, self.micros)
    }
}

impl fmt::Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Time({self})")
    }
}

// ------------------------------------------------------------------ Interval

/// A span of time in PostgreSQL's three parts, which do not convert into one
/// another: a month is not a fixed number of days, nor a day (across a
/// daylight-saving change) a fixed number of microseconds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Interval {
    /// Whole months.
    pub months: i32,
    /// Whole days.
    pub days: i32,
    /// Microseconds.
    pub micros: i64,
}

impl Interval {
    /// From its three parts.
    pub const fn new(months: i32, days: i32, micros: i64) -> Self {
        Interval {
            months,
            days,
            micros,
        }
    }

    /// Microseconds only.
    pub const fn from_micros(micros: i64) -> Self {
        Interval {
            months: 0,
            days: 0,
            micros,
        }
    }

    /// From the sixteen big-endian bytes the native protocol carries.
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        let b: [u8; 16] = fixed(bytes)?;
        Some(Interval {
            micros: i64::from_be_bytes(b[0..8].try_into().unwrap()),
            days: i32::from_be_bytes(b[8..12].try_into().unwrap()),
            months: i32::from_be_bytes(b[12..16].try_into().unwrap()),
        })
    }
}

impl From<std::time::Duration> for Interval {
    /// Saturates at `i64::MAX` microseconds (about 292,000 years).
    fn from(d: std::time::Duration) -> Self {
        Interval::from_micros(i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
    }
}

/// As an input literal PostgreSQL reads exactly:
/// `<months> mons <days> days <micros> microseconds`, each part only when it
/// is not zero (`0 microseconds` for the zero interval).
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sep = "";
        if self.months != 0 {
            write!(f, "{} mons", self.months)?;
            sep = " ";
        }
        if self.days != 0 {
            write!(f, "{sep}{} days", self.days)?;
            sep = " ";
        }
        if self.micros != 0 || sep.is_empty() {
            write!(f, "{sep}{} microseconds", self.micros)?;
        }
        Ok(())
    }
}

/// PostgreSQL's output style (`1 year 2 mons 3 days 04:05:06.5`, signs on any
/// part) and the literal form [`Display`](fmt::Display) writes. Units: years,
/// mons/months, weeks, days, hours, minutes, seconds, milliseconds,
/// microseconds, each optionally plural or abbreviated as PostgreSQL's `@`
/// style does not; and a trailing `[-]HH:MM:SS[.f]` clock.
impl FromStr for Interval {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        let err = || bad("interval", text);
        let mut iv = Interval::default();
        let mut words = text.split_whitespace().peekable();
        while let Some(word) = words.next() {
            if word.contains(':') {
                let (negative, clock) = match word.strip_prefix('-') {
                    Some(rest) => (true, rest),
                    None => (false, word.strip_prefix('+').unwrap_or(word)),
                };
                let us = parse_clock_unbounded(clock).ok_or_else(err)?;
                iv.micros = iv
                    .micros
                    .checked_add(if negative { -us } else { us })
                    .ok_or_else(err)?;
                continue;
            }
            let number: f64 = word.parse().map_err(|_| err())?;
            let unit = words.next().ok_or_else(err)?.to_ascii_lowercase();
            let unit = unit.trim_end_matches(',');
            let whole = |n: f64| -> Result<i64> {
                (n.fract() == 0.0 && n.abs() < 9.2e18)
                    .then_some(n as i64)
                    .ok_or_else(err)
            };
            match unit {
                "year" | "years" | "yr" | "yrs" | "y" => {
                    iv.months = iv
                        .months
                        .checked_add(i32::try_from(whole(number * 12.0)?).map_err(|_| err())?)
                        .ok_or_else(err)?
                }
                "mon" | "mons" | "month" | "months" => {
                    iv.months = iv
                        .months
                        .checked_add(i32::try_from(whole(number)?).map_err(|_| err())?)
                        .ok_or_else(err)?
                }
                "week" | "weeks" | "w" => {
                    iv.days = iv
                        .days
                        .checked_add(i32::try_from(whole(number * 7.0)?).map_err(|_| err())?)
                        .ok_or_else(err)?
                }
                "day" | "days" | "d" => {
                    iv.days = iv
                        .days
                        .checked_add(i32::try_from(whole(number)?).map_err(|_| err())?)
                        .ok_or_else(err)?
                }
                _ => {
                    let per = match unit {
                        "hour" | "hours" | "hr" | "hrs" | "h" => 3_600_000_000.0,
                        "minute" | "minutes" | "min" | "mins" | "m" => 60_000_000.0,
                        "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000.0,
                        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => 1_000.0,
                        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => 1.0,
                        _ => return Err(err()),
                    };
                    let us = (number * per).round();
                    if us.abs() >= 9.2e18 {
                        return Err(err());
                    }
                    iv.micros = iv.micros.checked_add(us as i64).ok_or_else(err)?;
                }
            }
        }
        Ok(iv)
    }
}

/// `H+:MM:SS[.ffffff]` with hours unbounded, as an interval's clock may be.
fn parse_clock_unbounded(text: &str) -> Option<i64> {
    let (hours, rest) = text.split_once(':')?;
    let hours: i64 = hours.parse().ok()?;
    let within = parse_clock(&format!("00:{rest}"))?;
    hours.checked_mul(3_600_000_000)?.checked_add(within)
}

// ------------------------------------------------------------------- Numeric

/// An exact decimal, held as its text: `-12.3400`, `1e-5`.
///
/// Kept as text because that is what the server reads and writes exactly, and
/// so no precision is lost on the way to a decimal type of your choosing
/// (`Numeric::as_str().parse::<rust_decimal::Decimal>()`). The server sends a
/// `NUMERIC` with its scale; from a server that predates that, a
/// `NUMERIC(p, s)` column is refused and must be selected as `col::text`.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Numeric(String);

impl Numeric {
    /// `text` if it is a decimal number: `[+-]digits[.digits][e[+-]digits]`
    /// (a leading or trailing `.` allowed, as PostgreSQL allows it), or `NaN`.
    pub fn new(text: impl Into<String>) -> Result<Self> {
        let text = text.into();
        if is_decimal(&text) {
            Ok(Numeric(text))
        } else {
            Err(bad("number", &text))
        }
    }

    /// The number's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The number's text, owned.
    pub fn into_string(self) -> String {
        self.0
    }
}

pub(crate) fn is_decimal(text: &str) -> bool {
    if text.eq_ignore_ascii_case("nan") {
        return true;
    }
    let s = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = digits(int) && digits(frac) && !(int.is_empty() && frac.is_empty());
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && digits(e)
    });
    mantissa_ok && exponent_ok
}

impl FromStr for Numeric {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        Numeric::new(text.trim())
    }
}

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Numeric({})", self.0)
    }
}

macro_rules! numeric_from_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Numeric {
            fn from(v: $t) -> Self {
                Numeric(v.to_string())
            }
        }
    )*};
}
numeric_from_int!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

// ---------------------------------------------------------------------- Json

/// A JSON document, held as its text. The server checks it on the way in;
/// with the `serde_json` feature, [`Json::parse`] and [`Json::from_value`]
/// convert to and from `serde_json::Value`.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Json(pub String);

impl Json {
    /// From a document's text (not checked here; the server checks it).
    pub fn new(text: impl Into<String>) -> Self {
        Json(text.into())
    }

    /// The document's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The document's text, owned.
    pub fn into_string(self) -> String {
        self.0
    }
}

#[cfg(feature = "serde_json")]
impl Json {
    /// `value` serialized.
    pub fn from_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self> {
        serde_json::to_string(value)
            .map(Json)
            .map_err(|e| Error::Param(format!("the value did not serialize as JSON: {e}")))
    }

    /// The document deserialized as `T`.
    pub fn parse<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(&self.0)
            .map_err(|e| Error::Param(format!("the JSON did not deserialize: {e}")))
    }
}

impl fmt::Display for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Json({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_round_trips() {
        let text = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let u: Uuid = text.parse().unwrap();
        assert_eq!(u.to_string(), text);
        assert_eq!(
            "A0EEBC999C0B4EF8BB6D6BB9BD380A11".parse::<Uuid>().unwrap(),
            u
        );
        assert!("a0eebc99".parse::<Uuid>().is_err());
        assert!(
            "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                .parse::<Uuid>()
                .is_err()
        );
        let v4 = Uuid::new_v4().unwrap();
        assert_eq!(v4.0[6] >> 4, 4);
        assert_ne!(v4, Uuid::new_v4().unwrap());
    }

    #[test]
    fn timestamps_parse_and_print() {
        let t: Timestamp = "2026-11-16 12:34:56.789".parse().unwrap();
        assert_eq!(t.to_string(), "2026-11-16 12:34:56.789");
        assert_eq!(
            "2026-11-16T14:34:56.789+02:00"
                .parse::<Timestamp>()
                .unwrap(),
            t
        );
        assert_eq!("2026-11-16 12:34:56.789Z".parse::<Timestamp>().unwrap(), t);
        assert_eq!(
            "1970-01-01".parse::<Timestamp>().unwrap(),
            Timestamp::UNIX_EPOCH
        );
        let before: Timestamp = "1969-12-31 23:59:59.5".parse().unwrap();
        assert_eq!(before.micros, -500_000);
        assert_eq!(before.to_string(), "1969-12-31 23:59:59.5");
        assert!("2026-02-30".parse::<Timestamp>().is_err());
        assert!("2026-01-01 25:00".parse::<Timestamp>().is_err());
        assert!("2026-01-01 10:00:00.1234567".parse::<Timestamp>().is_err());
        assert_eq!(
            Timestamp::from_ymd_hms(2026, 11, 16, 12, 34, 56)
                .unwrap()
                .to_string(),
            "2026-11-16 12:34:56"
        );
        let wire = t.micros.to_le_bytes();
        assert_eq!(Timestamp::from_wire(&wire), Some(t));
        let st: std::time::SystemTime = before.into();
        assert_eq!(Timestamp::from(st), before);
    }

    #[test]
    fn dates_and_times() {
        let d: Date = "2000-01-01".parse().unwrap();
        assert_eq!(d.unix_days(), PG_EPOCH_DAYS);
        assert_eq!(Date::from_wire(&0i32.to_be_bytes()), Some(d));
        assert_eq!(
            Date::from_wire(&(-1i32).to_be_bytes()).unwrap().to_string(),
            "1999-12-31"
        );
        assert!("2023-02-29".parse::<Date>().is_err());
        assert_eq!("2024-02-29".parse::<Date>().unwrap().ymd(), (2024, 2, 29));
        let t: Time = "13:05:09.25".parse().unwrap();
        assert_eq!(t.to_string(), "13:05:09.25");
        assert_eq!(Time::from_wire(&t.as_micros().to_be_bytes()), Some(t));
        assert_eq!("24:00:00".parse::<Time>().unwrap().as_micros(), US_PER_DAY);
        assert!("24:00:01".parse::<Time>().is_err());
    }

    #[test]
    fn intervals_print_as_literals_that_parse_back() {
        for iv in [
            Interval::new(14, -3, 4_005_000_006),
            Interval::default(),
            Interval::from_micros(-1),
            Interval::new(-1, 0, 0),
        ] {
            assert_eq!(iv.to_string().parse::<Interval>().unwrap(), iv, "{iv}");
        }
        assert_eq!(
            "1 year 2 mons 3 days 04:05:06.5"
                .parse::<Interval>()
                .unwrap(),
            Interval::new(14, 3, 14_706_500_000)
        );
        assert_eq!(
            "-5 days +01:00:00".parse::<Interval>().unwrap(),
            Interval::new(0, -5, 3_600_000_000)
        );
        let wire = [
            7i64.to_be_bytes().as_slice(),
            &2i32.to_be_bytes(),
            &1i32.to_be_bytes(),
        ]
        .concat();
        assert_eq!(Interval::from_wire(&wire), Some(Interval::new(1, 2, 7)));
    }

    #[test]
    fn numerics_are_checked() {
        for ok in ["0", "-1.5", "+2.", ".5", "1e10", "1.5E-3", "NaN"] {
            assert!(Numeric::new(ok).is_ok(), "{ok}");
        }
        for no in ["", ".", "1..2", "1e", "--1", "1;", "1 ", "0x10", "1e+"] {
            assert!(Numeric::new(no).is_err(), "{no}");
        }
    }
}
