//! Text ⇄ storage encodings for the engine's non-numeric scalar types.
//!
//! Every type here is stored **fixed-width, and ordered by `memcmp` where an
//! ordering is meaningful**, because that is what the rest of the engine is
//! built around: the comparison evaluator, the bit-sliced fingerprint index and
//! the zone-map bounds all work on raw column bytes. A type that needs a bespoke
//! comparator would have to be special-cased in three places and would drop out
//! of the filter index entirely.
//!
//! * `UUID` — 16 bytes, network order, exactly the RFC 4122 octet sequence.
//! * `INET` — 18 bytes, `[family][address (16, big-endian)][prefix bits]`, which
//!   sorts by family, then address, then netmask.
//! * `DATETIME` — `i64` microseconds since the Unix epoch, little-endian, so it
//!   compares as an ordinary `i64`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Serialized width of a `UUID` column.
pub const UUID_LEN: usize = 16;

/// Serialized width of an `INET` column.
pub const INET_LEN: usize = 18;

// ---------------------------------------------------------------- UUID

/// Parses the canonical UUID text form into its 16 octets.
///
/// Hyphens are optional and braces/`urn:uuid:` prefixes are not accepted, which
/// matches what Postgres' input function takes for the common cases.
pub fn parse_uuid(text: &str) -> Option<[u8; UUID_LEN]> {
    let s = text.trim();
    let mut out = [0u8; UUID_LEN];
    let mut n = 0usize;
    let mut hi: Option<u8> = None;

    for c in s.chars() {
        if c == '-' {
            continue;
        }
        let d = c.to_digit(16)? as u8;
        match hi {
            None => hi = Some(d),
            Some(h) => {
                if n >= UUID_LEN {
                    return None;
                }
                out[n] = (h << 4) | d;
                n += 1;
                hi = None;
            }
        }
    }

    if n == UUID_LEN && hi.is_none() {
        Some(out)
    } else {
        None
    }
}

/// Renders 16 octets in the canonical 8-4-4-4-12 lowercase form.
pub fn format_uuid(raw: &[u8]) -> String {
    let mut s = String::with_capacity(36);
    for (i, b) in raw.iter().take(UUID_LEN).enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push(char::from_digit(u32::from(b >> 4), 16).unwrap());
        s.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap());
    }
    s
}

/// A version 4, variant 1 UUID (RFC 4122) from the cryptographic generator.
///
/// `gen_random_uuid()` values end up as keys and tokens that applications
/// treat as unguessable, which a `fastrand` stream is not.
pub fn random_uuid_v4() -> [u8; UUID_LEN] {
    let mut bytes = [0u8; UUID_LEN];
    crate::auth::fill_random(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

// ---------------------------------------------------------------- INET

/// Parses `addr` or `addr/prefix` (IPv4 or IPv6) into the 18-byte form.
pub fn parse_inet(text: &str) -> Option<[u8; INET_LEN]> {
    let s = text.trim();
    let (addr_part, prefix_part) = match s.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (s, None),
    };

    let addr: IpAddr = addr_part.parse().ok()?;
    let (family, octets, max_prefix): (u8, [u8; 16], u8) = match addr {
        IpAddr::V4(v4) => {
            let mut o = [0u8; 16];
            o[..4].copy_from_slice(&v4.octets());
            (4, o, 32)
        }
        IpAddr::V6(v6) => (6, v6.octets(), 128),
    };

    let prefix = match prefix_part {
        None => max_prefix,
        Some(p) => {
            let n: u8 = p.trim().parse().ok()?;
            if n > max_prefix {
                return None;
            }
            n
        }
    };

    let mut out = [0u8; INET_LEN];
    out[0] = family;
    out[1..17].copy_from_slice(&octets);
    out[17] = prefix;
    Some(out)
}

/// Renders the 18-byte form back to text, omitting the prefix when it covers
/// the whole address (a plain host, which is how it is usually written).
pub fn format_inet(raw: &[u8]) -> Option<String> {
    if raw.len() != INET_LEN {
        return None;
    }
    let prefix = raw[17];
    let (addr, max_prefix) = match raw[0] {
        4 => {
            let o: [u8; 4] = raw[1..5].try_into().ok()?;
            (IpAddr::V4(Ipv4Addr::from(o)), 32u8)
        }
        6 => {
            let o: [u8; 16] = raw[1..17].try_into().ok()?;
            (IpAddr::V6(Ipv6Addr::from(o)), 128u8)
        }
        _ => return None,
    };

    if prefix == max_prefix {
        Some(addr.to_string())
    } else {
        Some(format!("{}/{}", addr, prefix))
    }
}

// ------------------------------------------------------- digit writers

/// `10^i` for every power that fits a `u64`.
const POW10_U64: [u64; 20] = {
    let mut table = [1u64; 20];
    let mut i = 1;
    while i < 20 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// `"00" "01" … "99"`: two decimal digits per lookup.
const TWO_DIGITS: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// Appends `n` (below 100) as exactly two digits.
#[inline]
fn push_two(out: &mut Vec<u8>, n: usize) {
    let i = n * 2;
    out.extend_from_slice(&TWO_DIGITS[i..i + 2]);
}

/// Writes the digits of `n` right-aligned into `buf`, returning where they
/// start.
#[inline]
fn digits_u64(buf: &mut [u8; 20], mut n: u64) -> usize {
    let mut cur = 20;
    while n >= 10_000 {
        let rem = (n % 10_000) as usize;
        n /= 10_000;
        cur -= 4;
        buf[cur..cur + 2].copy_from_slice(&TWO_DIGITS[(rem / 100) * 2..(rem / 100) * 2 + 2]);
        buf[cur + 2..cur + 4].copy_from_slice(&TWO_DIGITS[(rem % 100) * 2..(rem % 100) * 2 + 2]);
    }
    let mut n = n as usize;
    if n >= 100 {
        let i = (n % 100) * 2;
        n /= 100;
        cur -= 2;
        buf[cur..cur + 2].copy_from_slice(&TWO_DIGITS[i..i + 2]);
    }
    if n >= 10 {
        cur -= 2;
        buf[cur..cur + 2].copy_from_slice(&TWO_DIGITS[n * 2..n * 2 + 2]);
    } else {
        cur -= 1;
        buf[cur] = b'0' + n as u8;
    }
    cur
}

/// Appends `n` in decimal — what `n.to_string()` would say.
#[inline]
pub fn write_u64(out: &mut Vec<u8>, n: u64) {
    let mut buf = [0u8; 20];
    let start = digits_u64(&mut buf, n);
    out.extend_from_slice(&buf[start..]);
}

/// Appends `n` in decimal — what `n.to_string()` would say.
#[inline]
pub fn write_i64(out: &mut Vec<u8>, n: i64) {
    if n < 0 {
        out.push(b'-');
    }
    write_u64(out, n.unsigned_abs());
}

/// Appends `n` in decimal — what `n.to_string()` would say.
pub fn write_i128(out: &mut Vec<u8>, n: i128) {
    match i64::try_from(n) {
        Ok(small) => write_i64(out, small),
        Err(_) => out.extend_from_slice(n.to_string().as_bytes()),
    }
}

/// Appends `n` zero-padded to `width` digits — `format!("{:0width$}", n)` for
/// an `n` that fits the width, which every caller guarantees.
#[inline]
fn write_u64_padded(out: &mut Vec<u8>, n: u64, width: usize) {
    let mut buf = [b'0'; 20];
    let start = digits_u64(&mut buf, n);
    debug_assert!(20 - start <= width && width <= 20);
    out.extend_from_slice(&buf[(20 - width).min(start)..]);
}

// ------------------------------------------------------------ DATETIME

/// Microseconds between the Unix epoch and Postgres' 2000-01-01 epoch.
pub const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to `(y, m, d)` in
/// the proleptic Gregorian calendar.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
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
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Parses a timestamp into microseconds since the Unix epoch.
///
/// Accepts the shapes a Postgres client actually emits: `YYYY-MM-DD`,
/// `YYYY-MM-DD HH:MM[:SS[.ffffff]]`, the same with `T` instead of a space, and
/// an optional trailing `Z` or `±HH[:MM]` offset. A bare integer is taken as
/// microseconds already, so existing callers that stored epoch values keep
/// working.
/// Days in `month` of the proleptic Gregorian `year`.
pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        _ => 28,
    }
}

pub fn parse_timestamp_us(text: &str) -> Option<i64> {
    let s = text.trim();
    if s.is_empty() {
        return None;
    }

    // Bare integer: already an epoch value.
    if s.chars()
        .enumerate()
        .all(|(i, c)| c.is_ascii_digit() || (i == 0 && c == '-'))
    {
        return s.parse::<i64>().ok();
    }

    let bytes = s.as_bytes();

    // Trailing zone designator. Scanning from the right stops the `-` of a
    // negative offset from being confused with a date separator.
    let mut offset_minutes = 0i64;
    let mut body = s;
    if bytes.last() == Some(&b'Z') || bytes.last() == Some(&b'z') {
        body = &s[..s.len() - 1];
    } else if let Some(pos) = s.rfind(['+', '-']) {
        // Only a separator that comes after the time part can be a zone.
        let looks_like_zone = pos > 10;
        if looks_like_zone {
            let (head, tail) = s.split_at(pos);
            let sign = if tail.starts_with('-') { -1 } else { 1 };
            let digits = &tail[1..];
            let (hh, mm) = match digits.split_once(':') {
                Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
                None => match digits.len() {
                    // `+02`
                    1 | 2 => (digits.parse::<i64>().ok()?, 0),
                    // `+0200`
                    4 => (
                        digits[..2].parse::<i64>().ok()?,
                        digits[2..].parse::<i64>().ok()?,
                    ),
                    _ => return None,
                },
            };
            offset_minutes = sign * (hh * 60 + mm);
            body = head;
        }
    }

    let body = body.trim();
    let (date_part, time_part) = match body.find(['T', 't', ' ']) {
        Some(i) => (&body[..i], body[i + 1..].trim()),
        None => (body, ""),
    };

    let mut date_iter = date_part.split('-');
    let year: i64 = date_iter.next()?.parse().ok()?;
    let month: u32 = date_iter.next()?.parse().ok()?;
    let day: u32 = date_iter.next()?.parse().ok()?;
    if date_iter.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // `2026-02-30` is refused, as PostgreSQL refuses it. Range-checking the day
    // against 31 alone let it through, and the day count then rolled it over to
    // 2 March: a different date, stored as if it were the one written.
    if day > days_in_month(year, month) {
        return None;
    }

    let (mut hour, mut minute, mut second, mut micros) = (0i64, 0i64, 0i64, 0i64);
    if !time_part.is_empty() {
        let mut time_iter = time_part.split(':');
        hour = time_iter.next()?.parse().ok()?;
        minute = time_iter
            .next()
            .map(str::parse::<i64>)
            .transpose()
            .ok()?
            .unwrap_or(0);
        if let Some(sec_field) = time_iter.next() {
            match sec_field.split_once('.') {
                Some((whole, frac)) => {
                    second = whole.parse().ok()?;
                    // Pad or truncate the fraction to exactly 6 digits.
                    let mut f = String::with_capacity(6);
                    for c in frac.chars().take(6) {
                        if !c.is_ascii_digit() {
                            return None;
                        }
                        f.push(c);
                    }
                    while f.len() < 6 {
                        f.push('0');
                    }
                    micros = f.parse().ok()?;
                }
                None => second = sec_field.parse().ok()?,
            }
        }
        if time_iter.next().is_some() {
            return None;
        }
        if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
            return None;
        }
    }

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some((secs - offset_minutes * 60) * 1_000_000 + micros)
}

/// Renders microseconds since the Unix epoch as `YYYY-MM-DD HH:MM:SS[.ffffff]`.
///
/// The fractional part is dropped when zero and trailing zeroes are trimmed,
/// matching Postgres' own output.
pub fn format_timestamp_us(us: i64) -> String {
    let mut out = Vec::with_capacity(26);
    format_timestamp_us_into(us, &mut out);
    String::from_utf8(out).expect("a timestamp renders as ASCII")
}

/// [`format_timestamp_us`], appended to `out` without a `format!` per field.
pub fn format_timestamp_us_into(us: i64, out: &mut Vec<u8>) {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as usize;
    let (y, m, d) = civil_from_days(days);

    if (0..=9999).contains(&y) {
        let y = y as usize;
        push_two(out, y / 100);
        push_two(out, y % 100);
    } else {
        // `{:04}` counts a sign in the width: year -5 prints as `-005`.
        out.extend_from_slice(format!("{:04}", y).as_bytes());
    }
    out.push(b'-');
    push_two(out, m as usize);
    out.push(b'-');
    push_two(out, d as usize);
    out.push(b' ');
    push_two(out, tod / 3_600);
    out.push(b':');
    push_two(out, (tod % 3_600) / 60);
    out.push(b':');
    push_two(out, tod % 60);

    if frac != 0 {
        out.push(b'.');
        write_u64_padded(out, frac as u64, 6);
        // Trailing zeroes trimmed; `frac` is non-zero, so a digit survives.
        while out.last() == Some(&b'0') {
            out.pop();
        }
    }
}

#[cfg(test)]
fn format_timestamp_us_legacy(us: i64) -> String {
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
        tod / 3_600,
        (tod % 3_600) / 60,
        tod % 60
    );

    if frac == 0 {
        base
    } else {
        let mut f = format!("{:06}", frac);
        while f.ends_with('0') {
            f.pop();
        }
        format!("{}.{}", base, f)
    }
}

// --------------------------------------------------------------- DECIMAL

/// Decimal places every `DECIMAL` value is scaled to.
///
/// A single fixed scale is what makes the type comparable as a plain `i128`:
/// with per-value scales, `1.5` and `1.50` would hold different unscaled
/// integers and neither `memcmp` nor an integer compare could see them as
/// equal. At scale 9 the representable range is about ±1.7 × 10^29, which
/// covers money and rates with room to spare.
pub const DECIMAL_SCALE: u32 = 9;

const DECIMAL_SCALE_FACTOR: i128 = 1_000_000_000;

/// Parses a decimal literal into unscaled `i128` units.
///
/// Accepts an optional sign, digits, an optional fractional part, and an
/// optional exponent. Fractional digits beyond [`DECIMAL_SCALE`] are rejected
/// rather than rounded away: silently dropping precision from a money value is
/// worse than refusing it.
/// Parses a decimal literal at a given scale.
///
/// The scale decides how many fractional digits the stored integer carries, so
/// a column declared `NUMERIC(38,16)` holds sixteen of them rather than the
/// engine's default nine.
pub fn parse_decimal_scaled(text: &str, scale: u32) -> Option<i128> {
    if scale == DECIMAL_SCALE {
        return parse_decimal(text);
    }

    // Parsed *directly* at the declared scale. Going via `parse_decimal` and
    // rescaling would truncate to nine places first, so a `NUMERIC(38,16)`
    // holding `1.2345678901234567` lost its tail before it was ever widened —
    // and `parse_decimal` rejects more than nine places outright, which
    // surfaced as "Cannot insert row due to schema mismatch".
    let s = text.trim();
    if s.is_empty() {
        return None;
    }
    let (negative, rest) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if rest.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    // Fast path: no exponent, and few enough digits to gather in a `u64`.
    // Exactly the steps below, without building the digit string: dropping
    // excess places is dividing by ten per digit dropped.
    if exponent == 0 && int_part.len() + frac_part.len() <= 19 {
        let mut value = gather_digits(int_part, frac_part);
        let mut frac_len = frac_part.len() as u32;
        if frac_len > scale {
            value /= POW10_U64[(frac_len - scale) as usize];
            frac_len = scale;
        }
        let unscaled = (value as i128).checked_mul(10i128.checked_pow(scale - frac_len)?)?;
        return Some(if negative { -unscaled } else { unscaled });
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut frac_len = frac_part.len() as i32 - exponent;
    if frac_len < 0 {
        for _ in 0..(-frac_len) {
            digits.push('0');
        }
        frac_len = 0;
    }
    let mut frac_len = frac_len as u32;

    // More places than the column declared: drop the excess digits rather than
    // refuse the value, which is what a narrower column asks for.
    while frac_len > scale {
        digits.pop();
        frac_len -= 1;
    }

    let mut unscaled: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    unscaled = unscaled.checked_mul(10i128.checked_pow(scale - frac_len)?)?;
    Some(if negative { -unscaled } else { unscaled })
}

/// Parses a decimal literal at `scale`, rounding the places past it half away
/// from zero — PostgreSQL's rule for a value assigned to `NUMERIC(p, s)` and
/// for a cast to one. [`parse_decimal_scaled`] truncates instead, which is
/// what a comparison has to see the literal's digits through, and was wrong
/// for storage: `20.169` and `20.165` into `NUMERIC(12,2)` both stored 20.16.
///
/// Only the first dropped digit decides half away from zero, so the literal is
/// read one place deeper than `scale` (two when that would land on the engine
/// scale, whose reader refuses extra places rather than truncating them) and
/// rounded from there.
pub fn parse_decimal_rounded(text: &str, scale: u32) -> Option<i128> {
    let deeper = if scale + 1 == DECIMAL_SCALE { scale + 2 } else { scale + 1 };
    let raw = parse_decimal_scaled(text, deeper)?;
    let divisor = 10i128.pow(deeper - scale);
    let (quotient, remainder) = (raw / divisor, raw % divisor);
    if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
        quotient.checked_add(raw.signum())
    } else {
        Some(quotient)
    }
}

/// Refuses a value held at `scale` that a `NUMERIC(precision, scale)` column
/// cannot hold: PostgreSQL's `numeric field overflow`, SQLSTATE 22003. The
/// precision was recorded and never enforced, so `NUMERIC(12,2)` stored
/// 100000000000.00, twelve integer digits where it declares room for ten.
///
/// Checked after rounding to the scale, as PostgreSQL checks it:
/// `9999999999.995` rounds to 10000000000.00 and overflows, while
/// `9999999999.994` fits. A precision this engine cannot represent, or one
/// below the scale, is not checked.
pub fn check_numeric_precision(unscaled: i128, precision: u32, scale: u32) -> Result<(), String> {
    if precision == 0 || precision > 38 || precision < scale {
        return Ok(());
    }
    if unscaled.unsigned_abs() >= 10u128.pow(precision) {
        return Err(format!(
            "numeric field overflow: a field with precision {}, scale {} must round to an \
             absolute value less than 10^{}",
            precision,
            scale,
            precision - scale
        ));
    }
    Ok(())
}

/// Renders an unscaled value held at `scale`, keeping every declared place.
///
/// `format_decimal` strips trailing zeros, so a `NUMERIC(10,4)` holding
/// `10.5000` came back as `10.5`. The declared places are significant — they
/// are what the column promised — so they are kept.
pub fn format_decimal_scaled(unscaled: i128, scale: u32) -> String {
    let mut out = Vec::with_capacity(24);
    format_decimal_scaled_into(unscaled, scale, &mut out);
    String::from_utf8(out).expect("a decimal renders as ASCII")
}

/// [`format_decimal_scaled`], appended to `out`.
///
/// A magnitude that fits a `u64` — nearly every stored value — is split with
/// 64-bit arithmetic; the 128-bit path below is a library call per division.
pub fn format_decimal_scaled_into(unscaled: i128, scale: u32, out: &mut Vec<u8>) {
    if scale == 0 {
        write_i128(out, unscaled);
        return;
    }
    let magnitude = unscaled.unsigned_abs();
    if scale <= 19 && magnitude <= u64::MAX as u128 {
        let m = magnitude as u64;
        let factor = POW10_U64[scale as usize];
        // Negative implies a non-zero magnitude, so `-0.00` cannot arise.
        if unscaled < 0 {
            out.push(b'-');
        }
        write_u64(out, m / factor);
        out.push(b'.');
        write_u64_padded(out, m % factor, scale as usize);
        return;
    }
    out.extend_from_slice(format_decimal_scaled_wide(unscaled, scale).as_bytes());
}

/// The general form of [`format_decimal_scaled`], for magnitudes past `u64`
/// and scales past 19.
fn format_decimal_scaled_wide(unscaled: i128, scale: u32) -> String {
    // Scale 0 is `NUMERIC(10)`: stored as the integer itself, exactly as
    // `parse_decimal_scaled(_, 0)` wrote it and `encode_pg_numeric_scaled(_, 0)`
    // sends it. Dividing by the engine default here printed `42` as `0` over
    // the text protocol while the binary protocol said `42`.
    if scale == 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs();
    let factor = 10u128.pow(scale);

    let int_part = magnitude / factor;
    let frac_part = magnitude % factor;

    let mut out = String::new();
    if negative && (int_part != 0 || frac_part != 0) {
        out.push('-');
    }
    out.push_str(&int_part.to_string());
    out.push('.');
    out.push_str(&format!("{:0width$}", frac_part, width = scale as usize));
    out
}

/// The value of the digits of `int_part` then `frac_part`, which the caller
/// has checked are ASCII digits and at most 19 of them together.
#[inline]
fn gather_digits(int_part: &str, frac_part: &str) -> u64 {
    let mut value = 0u64;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        value = value * 10 + u64::from(b - b'0');
    }
    value
}

pub fn parse_decimal(text: &str) -> Option<i128> {
    let s = text.trim();
    if s.is_empty() {
        return None;
    }

    let (negative, rest) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if rest.is_empty() {
        return None;
    }

    // Split off an exponent, which shifts the decimal point.
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };

    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    // Fast path: no exponent, and few enough digits to gather in a `u64`.
    if exponent == 0 && int_part.len() + frac_part.len() <= 19 {
        let frac_len = frac_part.len() as u32;
        if frac_len > DECIMAL_SCALE {
            return None;
        }
        let value = gather_digits(int_part, frac_part) as i128;
        // Below 10^19 times 10^9: cannot overflow.
        let unscaled = value * 10i128.pow(DECIMAL_SCALE - frac_len);
        return Some(if negative { -unscaled } else { unscaled });
    }

    // Fold the exponent into the digit string so only one scaling step remains.
    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut frac_len = frac_part.len() as i32 - exponent;

    if frac_len < 0 {
        // The exponent moved the point right past every digit we have.
        for _ in 0..(-frac_len) {
            digits.push('0');
        }
        frac_len = 0;
    }
    let frac_len = frac_len as u32;
    if frac_len > DECIMAL_SCALE {
        return None;
    }

    let mut unscaled: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    unscaled = unscaled.checked_mul(10i128.checked_pow(DECIMAL_SCALE - frac_len)?)?;
    Some(if negative { -unscaled } else { unscaled })
}

/// Renders unscaled units back to text, trimming trailing fractional zeroes.
pub fn format_decimal(unscaled: i128) -> String {
    let mut out = Vec::with_capacity(24);
    format_decimal_into(unscaled, &mut out);
    String::from_utf8(out).expect("a decimal renders as ASCII")
}

/// [`format_decimal`], appended to `out`.
pub fn format_decimal_into(unscaled: i128, out: &mut Vec<u8>) {
    let magnitude = unscaled.unsigned_abs();
    if magnitude <= u64::MAX as u128 {
        let m = magnitude as u64;
        // Division by a constant: a multiply and a shift.
        let int_part = m / 1_000_000_000;
        let frac_part = m % 1_000_000_000;
        if unscaled < 0 {
            out.push(b'-');
        }
        write_u64(out, int_part);
        if frac_part != 0 {
            out.push(b'.');
            write_u64_padded(out, frac_part, DECIMAL_SCALE as usize);
            while out.last() == Some(&b'0') {
                out.pop();
            }
        }
        return;
    }
    out.extend_from_slice(format_decimal_wide(unscaled).as_bytes());
}

/// The general form of [`format_decimal`], for magnitudes past `u64`.
fn format_decimal_wide(unscaled: i128) -> String {
    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs();
    let scale = DECIMAL_SCALE_FACTOR as u128;

    let int_part = magnitude / scale;
    let frac_part = magnitude % scale;

    let mut out = String::new();
    if negative && (int_part != 0 || frac_part != 0) {
        out.push('-');
    }
    out.push_str(&int_part.to_string());

    if frac_part != 0 {
        let mut f = format!("{:0width$}", frac_part, width = DECIMAL_SCALE as usize);
        while f.ends_with('0') {
            f.pop();
        }
        out.push('.');
        out.push_str(&f);
    }
    out
}

/// Encodes unscaled units into Postgres' binary `numeric` representation:
/// `i16` digit count, `i16` weight, `i16` sign, `i16` display scale, then
/// base-10000 digits most-significant first.
pub fn encode_pg_numeric(unscaled: i128) -> Vec<u8> {
    encode_pg_numeric_scaled(unscaled, DECIMAL_SCALE)
}

/// The same, for a value stored at `scale` fractional digits rather than at
/// the engine's default.
///
/// A `NUMERIC(p, s)` column stores its values scaled by its own `s` — that is
/// what lets a `NUMERIC(38,16)` column hold sixteen fractional digits. The
/// text renderer was told that scale and divided by it; this encoder assumed
/// the engine default of nine and divided by that instead, so a value came off
/// the wire multiplied by ten to the power of the difference. A seeded price
/// of `5.00` in a `NUMERIC(38,16)` column reached the client as `50000000`
/// through a prepared statement while the identical query over the simple
/// protocol returned `5.00`, because only one of the two paths knew the scale.
pub fn encode_pg_numeric_scaled(unscaled: i128, scale: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 2 * 10);
    encode_pg_numeric_scaled_into(unscaled, scale, &mut out);
    out
}

/// [`encode_pg_numeric_scaled`], appended to `out`.
///
/// Two fixes over the original, both silent wrong answers that a client
/// decoded as a different number:
///
/// - **A value below 0.0001 kept weight -1.** Leading all-zero fractional
///   groups were dropped without moving the weight, so `0.00005` reached the
///   client as `0.5`.
/// - **Scales 37 and 38 overflowed.** The fraction was padded to whole groups
///   by multiplying it up, which passes `u128::MAX` at those scales; the
///   groups are now cut out by division instead.
pub fn encode_pg_numeric_scaled_into(unscaled: i128, scale: u32, out: &mut Vec<u8>) {
    if scale > 38 {
        // Past `i128`'s precision; kept exactly as it always behaved.
        out.extend_from_slice(&encode_pg_numeric_scaled_legacy(unscaled, scale));
        return;
    }
    encode_pg_numeric_parts_into(unscaled < 0, unscaled.unsigned_abs(), scale, out);
}

/// An unsigned 128-bit integer as binary `numeric` at scale 0.
///
/// `UBIGINT`, `HUGEINT` and `UHUGEINT` are advertised as `numeric`, as
/// PostgreSQL does for any integer wider than `bigint`; a `UHUGEINT` above
/// `i128::MAX` cannot pass through the signed encoder, so the magnitude goes
/// in directly.
pub fn encode_pg_numeric_u128_into(value: u128, out: &mut Vec<u8>) {
    encode_pg_numeric_parts_into(false, value, 0, out);
}

/// A signed 128-bit integer as binary `numeric` at scale 0.
pub fn encode_pg_numeric_i128_into(value: i128, out: &mut Vec<u8>) {
    encode_pg_numeric_parts_into(value < 0, value.unsigned_abs(), 0, out);
}

/// The encoder proper: a sign and a magnitude at `scale` (at most 38).
fn encode_pg_numeric_parts_into(negative: bool, magnitude: u128, scale: u32, out: &mut Vec<u8>) {
    // Room for 10 integer groups (39 digits) and 10 fractional (38 digits).
    let mut groups = [0u16; 20];
    let int_count;
    let frac_groups = scale.div_ceil(4) as usize;
    let pad = (frac_groups * 4 - scale as usize) as u32;
    let dscale;

    if scale <= 16 && magnitude <= u64::MAX as u128 {
        // The common case, in 64-bit arithmetic: every division below is by a
        // constant or a table entry, not a 128-bit library call.
        let m = magnitude as u64;
        let factor = POW10_U64[scale as usize];
        let (mut int_part, frac_part) = (m / factor, m % factor);

        let mut rev = [0u16; 5];
        let mut k = 0;
        while int_part > 0 {
            rev[k] = (int_part % 10_000) as u16;
            int_part /= 10_000;
            k += 1;
        }
        for i in 0..k {
            groups[i] = rev[k - 1 - i];
        }
        int_count = k;

        // Padded to whole groups; below 10^16, so it cannot overflow.
        let mut padded = frac_part * POW10_U64[pad as usize];
        for g in (0..frac_groups).rev() {
            groups[int_count + g] = (padded % 10_000) as u16;
            padded /= 10_000;
        }

        let mut d = scale;
        let mut f = frac_part;
        while d > 0 && f % 10 == 0 {
            f /= 10;
            d -= 1;
        }
        dscale = d;
    } else {
        let factor = 10u128.pow(scale);
        let (mut int_part, frac_part) = (magnitude / factor, magnitude % factor);

        let mut rev = [0u16; 10];
        let mut k = 0;
        while int_part > 0 {
            rev[k] = (int_part % 10_000) as u16;
            int_part /= 10_000;
            k += 1;
        }
        for i in 0..k {
            groups[i] = rev[k - 1 - i];
        }
        int_count = k;

        // Least significant group first. It holds only `4 - pad` real digits,
        // shifted up by `pad`; every other group holds four.
        let mut rest = frac_part;
        for g in (0..frac_groups).rev() {
            let group = if g == frac_groups - 1 {
                let unit = 10u128.pow(4 - pad);
                let v = (rest % unit) * 10u128.pow(pad);
                rest /= unit;
                v
            } else {
                let v = rest % 10_000;
                rest /= 10_000;
                v
            };
            groups[int_count + g] = group as u16;
        }

        let mut d = scale;
        let mut f = frac_part;
        while d > 0 && f % 10 == 0 {
            f /= 10;
            d -= 1;
        }
        dscale = d;
    }

    let total = int_count + frac_groups;
    // `weight` is the base-10000 exponent of the leading digit.
    let mut weight: i16 = if int_count == 0 {
        -1
    } else {
        int_count as i16 - 1
    };
    // Trailing all-zero groups carry no information.
    let mut end = total;
    while end > 1 && groups[end - 1] == 0 {
        end -= 1;
    }
    // Leading zero groups only occur below 1. Each one dropped moves the
    // first digit a group further right, which the weight has to say.
    let mut start = 0;
    while end - start > 1 && groups[start] == 0 {
        start += 1;
        weight -= 1;
    }
    let digits: &[u16] = if end - start == 1 && groups[start] == 0 {
        &[]
    } else {
        &groups[start..end]
    };

    let sign: u16 = if negative { 0x4000 } else { 0 };
    out.reserve(8 + digits.len() * 2);
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as i16).to_be_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
}

/// The original encoder. Production only reaches it past scale 38, where it
/// behaves as it always did; tests use it as the reference the rewrite is
/// measured against.
fn encode_pg_numeric_scaled_legacy(unscaled: i128, scale: u32) -> Vec<u8> {
    let sign: u16 = if unscaled < 0 { 0x4000 } else { 0 };
    let magnitude = unscaled.unsigned_abs();

    let factor = 10u128.pow(scale);
    let int_part = magnitude / factor;
    let frac_part = magnitude % factor;

    // Integer side: base-10000 groups, least significant first for now.
    let mut int_digits: Vec<u16> = Vec::new();
    let mut n = int_part;
    while n > 0 {
        int_digits.push((n % 10_000) as u16);
        n /= 10_000;
    }
    int_digits.reverse();

    // Fractional side, padded up to a whole number of base-10000 groups —
    // four decimal digits each, which is what the wire format counts in.
    let groups = scale.div_ceil(4);
    let padded_digits = groups * 4;
    let frac_padded = frac_part * 10u128.pow(padded_digits - scale);
    let mut frac_digits: Vec<u16> = Vec::with_capacity(groups as usize);
    for group in 0..groups {
        // Most significant group first.
        let shift = padded_digits - 4 * (group + 1);
        frac_digits.push(((frac_padded / 10u128.pow(shift)) % 10_000) as u16);
    }

    // `weight` is the base-10000 exponent of the leading digit.
    let weight: i16 = if int_digits.is_empty() {
        -1
    } else {
        int_digits.len() as i16 - 1
    };

    let mut digits = int_digits;
    digits.extend_from_slice(&frac_digits);
    // Trailing all-zero groups carry no information.
    while digits.len() > 1 && *digits.last().unwrap() == 0 {
        digits.pop();
    }
    // A leading zero group only appears for values below 1, where `weight`
    // already says the first digit sits after the point.
    while digits.len() > 1 && digits[0] == 0 {
        digits.remove(0);
    }
    if digits == [0] {
        digits.clear();
    }

    // Display scale is the number of fractional digits the value actually
    // needs. Always reporting DECIMAL_SCALE would render every whole number as
    // `19.000000000`. The stored value is unchanged either way; this only
    // affects how a client prints it.
    let mut dscale = scale;
    let mut f = frac_part;
    while dscale > 0 && f % 10 == 0 {
        f /= 10;
        dscale -= 1;
    }

    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as i16).to_be_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

// ----------------------------------------------------------- ARRAY / BYTEA

/// Element OID written into an array whose elements came from a text literal.
const OID_TEXT: i32 = 25;

/// Encodes a Postgres array literal (`{a,b,NULL}`) into the binary array
/// representation.
///
/// The format is: dimension count, a null flag, the element OID, then per
/// dimension a length and lower bound, then each element as a length followed
/// by its bytes — with length `-1` marking `NULL`. Carrying the element OID in
/// the value is what lets one storage type serve arrays of anything.
pub fn parse_array_literal(text: &str) -> Option<Vec<u8>> {
    let elements = parse_array_elements(text)?;

    let mut out = Vec::with_capacity(20 + elements.len() * 8);
    out.extend_from_slice(&1i32.to_be_bytes()); // one dimension
    let has_null = elements.iter().any(Option::is_none);
    out.extend_from_slice(&i32::from(has_null).to_be_bytes());
    out.extend_from_slice(&OID_TEXT.to_be_bytes());
    out.extend_from_slice(&(elements.len() as i32).to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // lower bound, as Postgres uses

    for element in &elements {
        match element {
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(value) => {
                out.extend_from_slice(&(value.len() as i32).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
            }
        }
    }

    Some(out)
}

/// Splits `{a,"b,c",NULL}` into its elements. `None` is an unquoted `NULL`.
fn parse_array_elements(text: &str) -> Option<Vec<Option<String>>> {
    let trimmed = text.trim();
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = inner.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted => {
                // Inside quotes a backslash escapes the next character.
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            ',' if !quoted => {
                out.push(finish_array_element(&current, was_quoted));
                current.clear();
                was_quoted = false;
            }
            _ => current.push(c),
        }
    }
    if quoted {
        return None;
    }
    out.push(finish_array_element(&current, was_quoted));

    Some(out)
}

fn finish_array_element(raw: &str, was_quoted: bool) -> Option<String> {
    // Only an *unquoted* NULL is the null element; `"NULL"` is the four-letter
    // string, exactly as Postgres reads it.
    if !was_quoted && raw.trim().eq_ignore_ascii_case("NULL") {
        return None;
    }
    Some(if was_quoted {
        raw.to_string()
    } else {
        raw.trim().to_string()
    })
}

/// Renders the binary array representation back to `{a,b,NULL}`.
pub fn format_array(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return Some("{}".to_string());
    }
    if raw.len() < 20 {
        return None;
    }

    let be32 =
        |at: usize| -> i32 { i32::from_be_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]) };

    let ndim = be32(0);
    if ndim == 0 {
        return Some("{}".to_string());
    }
    if ndim != 1 {
        // Multi-dimensional arrays round-trip as stored but are not rendered.
        return None;
    }

    let count = be32(12).max(0) as usize;
    let mut at = 20usize;
    let mut parts: Vec<String> = Vec::with_capacity(count);

    for _ in 0..count {
        if at + 4 > raw.len() {
            return None;
        }
        let len = be32(at);
        at += 4;
        if len < 0 {
            parts.push("NULL".to_string());
            continue;
        }
        let len = len as usize;
        if at + len > raw.len() {
            return None;
        }
        let value = String::from_utf8_lossy(&raw[at..at + len]).into_owned();
        at += len;

        // Quote when the value could otherwise be misread as syntax.
        let needs_quotes = value.is_empty()
            || value.eq_ignore_ascii_case("NULL")
            || value.contains([',', '{', '}', '"', '\\', ' ']);
        if needs_quotes {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            parts.push(format!("\"{}\"", escaped));
        } else {
            parts.push(value);
        }
    }

    Some(format!("{{{}}}", parts.join(",")))
}

/// Decodes a `bytea` value's text form: [`byteain`], with the reason dropped.
pub fn parse_bytea_literal(text: &str) -> Option<Vec<u8>> {
    byteain(text).ok()
}

/// PostgreSQL's `byteain`, byte for byte.
///
/// Two formats, chosen by the first two characters and nothing else:
///
/// * **hex** — `\x` (lower-case `x` only), then pairs of hex digits in either
///   case. Whitespace (space, tab, newline, carriage return) may sit *between*
///   pairs but not inside one; an odd digit count is an error.
/// * **escape** — every byte is itself, except a backslash, which must start
///   `\\` (one backslash) or `\nnn` (three octal digits, the first `0`-`3`).
///   Any other backslash is an error. Nothing is trimmed: `' ab '` is four
///   bytes.
///
/// The previous parser trimmed, copied backslash escapes through literally and
/// refused whitespace between hex pairs — each one a different stored value
/// for input PostgreSQL accepts, reported as success.
///
/// The error text is PostgreSQL's.
pub fn byteain(text: &str) -> Result<Vec<u8>, String> {
    let input = text.as_bytes();
    if let Some(hex) = input.strip_prefix(b"\\x") {
        let mut out = Vec::with_capacity(hex.len() / 2);
        let mut i = 0usize;
        while i < hex.len() {
            let c = hex[i];
            if matches!(c, b' ' | b'\n' | b'\t' | b'\r') {
                i += 1;
                continue;
            }
            let hi = hex_digit(c).ok_or_else(|| invalid_hex_digit(text, 2 + i))?;
            i += 1;
            let Some(&c2) = hex.get(i) else {
                return Err("invalid hexadecimal data: odd number of digits".to_string());
            };
            let lo = hex_digit(c2).ok_or_else(|| invalid_hex_digit(text, 2 + i))?;
            i += 1;
            out.push((hi << 4) | lo);
        }
        return Ok(out);
    }

    // Escape format. Copied in runs between backslashes: most values have none.
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0usize;
    while i < input.len() {
        let Some(p) = memchr::memchr(b'\\', &input[i..]) else {
            out.extend_from_slice(&input[i..]);
            break;
        };
        out.extend_from_slice(&input[i..i + p]);
        let at = i + p;
        let d = |k: usize| input.get(at + k).copied().unwrap_or(0);
        if (b'0'..=b'3').contains(&d(1))
            && (b'0'..=b'7').contains(&d(2))
            && (b'0'..=b'7').contains(&d(3))
        {
            out.push(((d(1) - b'0') << 6) | ((d(2) - b'0') << 3) | (d(3) - b'0'));
            i = at + 4;
        } else if d(1) == b'\\' {
            out.push(b'\\');
            i = at + 2;
        } else {
            return Err("invalid input syntax for type bytea".to_string());
        }
    }
    Ok(out)
}

#[inline]
fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// PostgreSQL names the offending character, which may be multi-byte.
fn invalid_hex_digit(text: &str, at: usize) -> String {
    let ch = text[at..].chars().next().unwrap_or('?');
    format!("invalid hexadecimal digit: \"{}\"", ch)
}

/// The bytes a `bytea` column stores for a SQL literal as written in a
/// statement: `'…'` (doubled quotes collapsed), `E'…'` (backslash escapes
/// resolved first, as PostgreSQL's lexer does before `byteain` sees the text),
/// `BYTEA '…'`, or an unquoted value taken as it is.
///
/// Quoting is undone exactly once. The insert path used to collapse the quotes
/// and then hand the result to [`encode_literal`], which stripped an outer pair
/// a second time — so a value that itself began and ended with `'` lost both.
pub fn encode_bytea_sql(literal: &str) -> Result<Vec<u8>, String> {
    let t = literal.trim();
    let b = t.as_bytes();
    if b.len() >= 3 && matches!(b[0], b'E' | b'e') && b[1] == b'\'' && b[b.len() - 1] == b'\'' {
        let text = unescape_e_string(&t[2..t.len() - 1])?;
        return byteain(&text);
    }
    if b.len() >= 2 && b[0] == b'\'' && b[b.len() - 1] == b'\'' {
        return byteain(&unquote_sql(t));
    }
    if b.first().is_some_and(|c| c.is_ascii_alphabetic()) {
        let stripped = strip_type_prefix(t);
        if stripped.len() != t.len() {
            return encode_bytea_sql(stripped);
        }
    }
    byteain(t)
}

/// Resolves the backslash escapes of a PostgreSQL escape string (`E'…'`), given
/// the text between the quotes: `\b \f \n \r \t \v`, `\ooo` octal, `\xhh` hex,
/// `\uXXXX` / `\UXXXXXXXX` code points, `\'` and `''` for a quote, and any
/// other `\c` for `c`. The result must be valid UTF-8 without a NUL byte, as
/// PostgreSQL requires of every string under a UTF-8 server encoding.
pub fn unescape_e_string(inner: &str) -> Result<String, String> {
    let s = inner.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(s.len());
    let mut i = 0usize;
    let mut pending_high: Option<u32> = None;
    let push_cp = |out: &mut Vec<u8>, cp: u32| -> Result<(), String> {
        let c = char::from_u32(cp).ok_or_else(|| "invalid Unicode escape value".to_string())?;
        let mut buf = [0u8; 4];
        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        Ok(())
    };
    while i < s.len() {
        let c = s[i];
        if c == b'\'' {
            // Only `''` can appear inside a well-formed literal.
            out.push(b'\'');
            i += if s.get(i + 1) == Some(&b'\'') { 2 } else { 1 };
            continue;
        }
        if c != b'\\' {
            if pending_high.is_some() {
                return Err("invalid Unicode surrogate pair".to_string());
            }
            out.push(c);
            i += 1;
            continue;
        }
        let Some(&e) = s.get(i + 1) else {
            return Err("unterminated quoted string".to_string());
        };
        if pending_high.is_some() && e != b'u' && e != b'U' {
            return Err("invalid Unicode surrogate pair".to_string());
        }
        match e {
            b'0'..=b'7' => {
                let mut v: u32 = 0;
                let mut k = i + 1;
                while k < s.len() && k < i + 4 && (b'0'..=b'7').contains(&s[k]) {
                    v = v * 8 + u32::from(s[k] - b'0');
                    k += 1;
                }
                out.push(v as u8);
                i = k;
            }
            b'x' if s.get(i + 2).is_some_and(|c| c.is_ascii_hexdigit()) => {
                let mut v: u8 = 0;
                let mut k = i + 2;
                while k < s.len() && k < i + 4 && s[k].is_ascii_hexdigit() {
                    v = (v << 4) | hex_digit(s[k]).unwrap_or(0);
                    k += 1;
                }
                out.push(v);
                i = k;
            }
            b'u' | b'U' => {
                let n = if e == b'u' { 4 } else { 8 };
                let digits = s
                    .get(i + 2..i + 2 + n)
                    .filter(|d| d.iter().all(u8::is_ascii_hexdigit))
                    .ok_or_else(|| "invalid Unicode escape".to_string())?;
                let cp = digits
                    .iter()
                    .fold(0u32, |a, &d| (a << 4) | u32::from(hex_digit(d).unwrap_or(0)));
                i += 2 + n;
                let low = (0xDC00..=0xDFFF).contains(&cp);
                match pending_high.take() {
                    None if (0xD800..=0xDBFF).contains(&cp) => pending_high = Some(cp),
                    None if low => return Err("invalid Unicode surrogate pair".to_string()),
                    None if cp == 0 => return Err("invalid Unicode escape value".to_string()),
                    None => push_cp(&mut out, cp)?,
                    Some(hi) if low => {
                        push_cp(&mut out, 0x10000 + ((hi - 0xD800) << 10) + (cp - 0xDC00))?
                    }
                    Some(_) => return Err("invalid Unicode surrogate pair".to_string()),
                }
            }
            _ => {
                out.push(match e {
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => 0x0b,
                    other => other,
                });
                i += 2;
            }
        }
    }
    if pending_high.is_some() {
        return Err("invalid Unicode surrogate pair".to_string());
    }
    if out.contains(&0) {
        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
    }
    String::from_utf8(out).map_err(|_| "invalid byte sequence for encoding \"UTF8\"".to_string())
}

/// Renders bytes as Postgres' `\x` hex form.
pub fn format_bytea(raw: &[u8]) -> String {
    let mut out = Vec::with_capacity(raw.len() * 2 + 2);
    format_bytea_into(raw, &mut out);
    String::from_utf8(out).expect("hex is ASCII")
}

/// [`format_bytea`], appended to `out`: a table lookup per nibble into space
/// sized once, rather than `char::from_digit` and a `String::push` each.
pub fn format_bytea_into(raw: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\x");
    let start = out.len();
    out.resize(start + raw.len() * 2, 0);
    for (pair, b) in out[start..].chunks_exact_mut(2).zip(raw) {
        pair[0] = HEX[(b >> 4) as usize];
        pair[1] = HEX[(b & 0x0f) as usize];
    }
}

#[cfg(test)]
fn format_bytea_legacy(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 2 + 2);
    out.push_str("\\x");
    for b in raw {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap());
        out.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap());
    }
    out
}

// ------------------------------------------------------- literal coercion

/// Re-encodes a literal so it has the width and representation of the column it
/// is being compared against.
///
/// The comparison evaluator does no coercion: both operands must already be the
/// same type and the same width, or it reports `OperandSizeMismatch`. But a
/// literal's type comes from how it was *written* — `200` is an `i32`, and
/// `'11111111-…'` is text — while the column it meets may be a `DECIMAL` or a
/// `UUID`. Reconciling the two here means `net_amount > 200` and
/// `id = '1111…'` compare as the column's type rather than failing.
///
/// Returns `None` when the literal cannot be represented in the target type, so
/// the caller can report a type error instead of comparing garbage.
pub fn coerce_literal(
    bytes: &[u8],
    from: &crate::core::db_type::DbType,
    to: &crate::core::db_type::DbType,
) -> Option<Vec<u8>> {
    use crate::core::db_type::DbType;

    if from == to {
        return Some(bytes.to_vec());
    }

    // Text literals carry their own syntax, so they are re-parsed at the target
    // type rather than reinterpreted bit-for-bit.
    if matches!(from, DbType::STRING) {
        let text = std::str::from_utf8(bytes).ok()?;
        return match to {
            DbType::STRING => Some(bytes.to_vec()),
            DbType::UUID | DbType::INET | DbType::DATETIME | DbType::DECIMAL | DbType::JSONB => {
                encode_literal(to, text)
            }
            _ if has_text_literal(to) => encode_literal(to, text),
            DbType::BOOL => match text.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "on" | "1" => Some(vec![1]),
                "false" | "f" | "no" | "n" | "off" | "0" => Some(vec![0]),
                _ => None,
            },
            DbType::CHAR => {
                let c = text.chars().next()?;
                if text.chars().count() != 1 {
                    return None;
                }
                Some((c as u32).to_le_bytes().to_vec())
            }
            // Not `parse::<i128>().ok()?`: that `?` returned from the whole
            // function for any text that is not an integer, so the float
            // fallback never ran and `f = '1.5'`, `f = 'NaN'` and
            // `f > 'Infinity'` were refused outright.
            _ => text
                .trim()
                .parse::<i128>()
                .ok()
                .and_then(|v| encode_integer(v, to))
                .or_else(|| encode_float(text.trim().parse::<f64>().ok()?, to)),
        };
    }

    // A NUMERIC at the engine scale read as a float, through its text so it
    // is rounded once: NUMERIC arithmetic meeting a float column is float
    // arithmetic, as in PostgreSQL.
    if matches!(from, DbType::DECIMAL) && matches!(to, DbType::F32 | DbType::F64) {
        let raw = i128::from_le_bytes(bytes.try_into().ok()?);
        return encode_float(format_decimal(raw).parse::<f64>().ok()?, to);
    }

    // A numeric literal keeps its value and changes its width.
    if let Some(v) = decode_integer(bytes, from) {
        if matches!(to, DbType::STRING) {
            return Some(v.to_string().into_bytes());
        }
        if matches!(to, DbType::DECIMAL) {
            return v
                .checked_mul(DECIMAL_SCALE_FACTOR)
                .map(|scaled| scaled.to_le_bytes().to_vec());
        }
        return encode_integer(v, to).or_else(|| encode_float(v as f64, to));
    }

    if let Some(v) = decode_float(bytes, from) {
        if matches!(to, DbType::STRING) {
            return Some(v.to_string().into_bytes());
        }
        if matches!(to, DbType::DECIMAL) {
            return parse_decimal(&format!("{}", v)).map(|d| d.to_le_bytes().to_vec());
        }
        return encode_float(v, to).or_else(|| {
            // An integral float compared against an integer column is exact.
            if v.fract() == 0.0 {
                encode_integer(v as i128, to)
            } else {
                None
            }
        });
    }

    None
}

fn decode_integer(bytes: &[u8], ty: &crate::core::db_type::DbType) -> Option<i128> {
    use crate::core::db_type::DbType;
    fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
        if b.len() != N {
            return None;
        }
        let mut a = [0u8; N];
        a.copy_from_slice(b);
        Some(a)
    }
    Some(match ty {
        DbType::I8 => i8::from_le_bytes(le::<1>(bytes)?) as i128,
        DbType::I16 => i16::from_le_bytes(le::<2>(bytes)?) as i128,
        DbType::I32 => i32::from_le_bytes(le::<4>(bytes)?) as i128,
        DbType::I64 | DbType::DATETIME => i64::from_le_bytes(le::<8>(bytes)?) as i128,
        DbType::I128 | DbType::DECIMAL => i128::from_le_bytes(le::<16>(bytes)?),
        DbType::U8 | DbType::BOOL => u8::from_le_bytes(le::<1>(bytes)?) as i128,
        DbType::U16 => u16::from_le_bytes(le::<2>(bytes)?) as i128,
        DbType::U32 | DbType::CHAR => u32::from_le_bytes(le::<4>(bytes)?) as i128,
        DbType::U64 => u64::from_le_bytes(le::<8>(bytes)?) as i128,
        DbType::U128 => u128::from_le_bytes(le::<16>(bytes)?) as i128,
        _ => return None,
    })
}

fn decode_float(bytes: &[u8], ty: &crate::core::db_type::DbType) -> Option<f64> {
    use crate::core::db_type::DbType;
    match ty {
        DbType::F32 if bytes.len() == 4 => {
            Some(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64)
        }
        DbType::F64 if bytes.len() == 8 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            Some(f64::from_le_bytes(a))
        }
        _ => None,
    }
}

/// Encodes an integer at the target type's width, refusing values it cannot
/// hold rather than silently wrapping.
fn encode_integer(v: i128, to: &crate::core::db_type::DbType) -> Option<Vec<u8>> {
    use crate::core::db_type::DbType;
    Some(match to {
        DbType::I8 => i8::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I16 => i16::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I32 => i32::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I64 | DbType::DATETIME => i64::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I128 => v.to_le_bytes().to_vec(),
        DbType::U8 => u8::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::BOOL => match v {
            0 => vec![0],
            1 => vec![1],
            _ => return None,
        },
        DbType::U16 => u16::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::U32 | DbType::CHAR => u32::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::U64 => u64::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::U128 => u128::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::DECIMAL => v.checked_mul(DECIMAL_SCALE_FACTOR)?.to_le_bytes().to_vec(),
        _ => return None,
    })
}

fn encode_float(v: f64, to: &crate::core::db_type::DbType) -> Option<Vec<u8>> {
    use crate::core::db_type::DbType;
    Some(match to {
        DbType::F32 => (v as f32).to_le_bytes().to_vec(),
        DbType::F64 => v.to_le_bytes().to_vec(),
        _ => return None,
    })
}

/// Single-quotes a value, doubling any quote it contains, so it cannot escape
/// into the surrounding statement as syntax.
/// The inverse of [`quote_sql`]: the text of a quoted literal, with SQL's
/// doubled-quote escape collapsed.
///
/// `'it''s'` is one string containing an apostrophe — SQL escapes a quote by
/// doubling it. Stripping the surrounding pair without collapsing the inner
/// ones stored `it''s`, so any value containing an apostrophe (`O'Brien`,
/// `it's`) round-tripped wrong, and each further re-quoting doubled the damage
/// again.
///
/// Unquoted input is returned unchanged: only a quoted literal carries the
/// escape, and an unquoted `NULL` has to stay distinguishable from `'NULL'`.
pub fn unquote_sql(s: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;

    let trimmed = s.trim();
    let mut chars = trimmed.chars();
    let quote = match chars.next() {
        Some(q @ ('\'' | '"')) if trimmed.len() >= 2 && trimmed.ends_with(q) => q,
        _ => return Cow::Borrowed(trimmed),
    };

    let inner = &trimmed[1..trimmed.len() - 1];
    // Collapses each doubled quote, scanning left to right without overlap —
    // what `str::replace("''", "'")` does — but without building the two
    // needle strings on every call, and without allocating at all when there
    // is nothing to collapse, which is nearly every value.
    let q = quote as u8;
    let bytes = inner.as_bytes();
    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut at = 0usize;
    while let Some(p) = memchr::memchr(q, &bytes[at..]) {
        let k = at + p;
        if bytes.get(k + 1) == Some(&q) {
            // `k` and `k + 1` are ASCII quotes, so every slice here ends on a
            // character boundary.
            let buf = out.get_or_insert_with(|| String::with_capacity(inner.len()));
            buf.push_str(&inner[copied..=k]);
            copied = k + 2;
            at = k + 2;
        } else {
            at = k + 1;
        }
    }
    match out {
        None => Cow::Borrowed(inner),
        Some(mut buf) => {
            buf.push_str(&inner[copied..]);
            Cow::Owned(buf)
        }
    }
}

#[cfg(test)]
mod unquote_sql_reference {
    /// `unquote_sql` as it was, two allocations per call.
    fn reference(s: &str) -> std::borrow::Cow<'_, str> {
        use std::borrow::Cow;
        let trimmed = s.trim();
        let mut chars = trimmed.chars();
        let quote = match chars.next() {
            Some(q @ ('\'' | '"')) if trimmed.len() >= 2 && trimmed.ends_with(q) => q,
            _ => return Cow::Borrowed(trimmed),
        };
        let inner = &trimmed[1..trimmed.len() - 1];
        let single = quote.to_string();
        let doubled = format!("{}{}", quote, quote);
        if inner.contains(&doubled) {
            Cow::Owned(inner.replace(&doubled, &single))
        } else {
            Cow::Borrowed(inner)
        }
    }

    #[test]
    fn matches_the_reference() {
        let pieces = ["'", "''", "'''", "\"", "\"\"", "a", "日本", " ", "\\", "🙂", "x'y", "NULL"];
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for _ in 0..50_000 {
            let mut s = String::new();
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let n = (state % 9) as usize;
            for k in 0..n {
                s.push_str(pieces[((state >> (k * 4)) % pieces.len() as u64) as usize]);
            }
            for wrapped in [s.clone(), format!("'{s}'"), format!("\"{s}\""), format!("  '{s}' ")] {
                assert_eq!(super::unquote_sql(&wrapped), reference(&wrapped), "{:?}", wrapped);
            }
        }
    }
}

pub fn quote_sql(s: &str) -> String {
    // The same quoting as the wire layer's parameter inlining: the text
    // between quotes is copied in runs found with `memchr`, not a character
    // at a time. Every value of an `IN (SELECT …)` list passes through here.
    crate::pg_params::quote_literal(s)
}

#[cfg(test)]
mod quote_sql_reference {
    /// `quote_sql` as it was, a character at a time.
    fn reference(s: &str) -> String {
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

    #[test]
    fn matches_the_reference() {
        let pieces = ["'", "''", "a", "日本", " ", "\\", "🙂", "\"", "x'y", "", "\n"];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let mut s = String::new();
            for k in 0..(state % 9) as usize {
                s.push_str(pieces[((state >> (k * 5)) % pieces.len() as u64) as usize]);
            }
            assert_eq!(super::quote_sql(&s), reference(&s), "{:?}", s);
        }
    }
}

/// Strips one layer of matching single or double quotes, if present.
/// One matching pair of SQL string quotes, and only those.
pub fn strip_single_quotes(s: &str) -> &str {
    let t = s.trim();
    let bytes = t.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'' {
        &t[1..t.len() - 1]
    } else {
        t
    }
}

pub fn strip_quotes(s: &str) -> &str {
    let t = s.trim();
    let bytes = t.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"'))
    {
        &t[1..t.len() - 1]
    } else {
        t
    }
}

/// Encodes a SQL literal into the storage bytes for one of the types this
/// module owns. Returns `None` for any other type, so callers keep their own
/// numeric and string handling.
///
/// A literal that does not parse is rejected rather than coerced: silently
/// storing a zero for `'not-a-uuid'` would make the value indistinguishable
/// from a legitimately zero UUID.
/// Drops a leading type name from `TYPE 'literal'`, leaving the quoted part.
///
/// Only when what follows really is a quoted string — so a bare word, or a
/// literal that merely begins with letters, is returned untouched.
pub fn strip_type_prefix(literal: &str) -> &str {
    let trimmed = literal.trim();
    let bytes = trimmed.as_bytes();
    if bytes.first().is_none_or(|b| !b.is_ascii_alphabetic()) {
        return literal;
    }
    let mut i = 0usize;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let rest = trimmed[i..].trim_start();
    if rest.starts_with('\'') && rest.ends_with('\'') && rest.len() >= 2 {
        return rest;
    }
    literal
}

/// A `DECIMAL` result cell that carries its own display scale: the
/// sixteen-byte unscaled value, then one byte of scale.
///
/// A stored `DECIMAL` is sixteen bytes at its column's scale. A computed
/// `numeric` whose scale PostgreSQL picks *per value* — `AVG`, where
/// `numeric_div` gives `avg(1,2,2)` sixteen places and `avg(0,1)` twenty —
/// cannot be described by one column scale, so its cells say their own. Only
/// result rows carry this form; every reader of `DECIMAL` result bytes goes
/// through [`split_decimal_cell`], which accepts both.
pub fn decimal_cell_with_scale(unscaled: i128, scale: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.extend_from_slice(&unscaled.to_le_bytes());
    out.push(scale.min(u8::MAX as u32) as u8);
    out
}

/// The value of a `DECIMAL` cell, and the scale it carries when it is the
/// seventeen-byte form of [`decimal_cell_with_scale`] (`None` for an ordinary
/// sixteen-byte cell, which is at its column's scale).
pub fn split_decimal_cell(bytes: &[u8]) -> Option<(i128, Option<u32>)> {
    match bytes.len() {
        16 => Some((i128::from_le_bytes(bytes.try_into().ok()?), None)),
        // A display scale past what an i128 can carry is not a cell this
        // engine writes (AVG caps it at 38); treat it as malformed rather than
        // let a formatter compute 10^scale and overflow.
        17 if bytes[16] <= 38 => Some((
            i128::from_le_bytes(bytes[..16].try_into().ok()?),
            Some(u32::from(bytes[16])),
        )),
        _ => None,
    }
}

/// PostgreSQL's binary `numeric` for `unscaled / 10^scale`, with the display
/// scale *kept* at `scale`. [`encode_pg_numeric_scaled`] trims trailing zeros
/// from it; a value whose scale is part of the answer — `0.50000000000000000000`
/// from `AVG` — has to keep them, as PostgreSQL's own does.
pub fn encode_pg_numeric_with_dscale(unscaled: i128, scale: u32) -> Vec<u8> {
    let mut out = encode_pg_numeric_scaled(unscaled, scale);
    if out.len() >= 8 {
        out[6..8].copy_from_slice(&(scale as i16).to_be_bytes());
    }
    out
}

/// An unsigned count as the `bigint` PostgreSQL reports it in.
///
/// Counts here are `u64`; PostgreSQL has no unsigned type, so admin views
/// report them as `bigint`. Reinterpreting the bytes — `n.to_le_bytes()` sent
/// under an `int8` OID, or `n as i64` — turns anything above `i64::MAX` into a
/// negative number. This saturates instead: `i64::MAX` is a value no real
/// count reaches, so it reads as "past the range" rather than as a plausible
/// figure, and the clamp is logged.
pub fn count_as_bigint(n: u64) -> i64 {
    match i64::try_from(n) {
        Ok(v) => v,
        Err(_) => {
            log::warn!(
                "count {} exceeds bigint; reported as {} (saturated)",
                n,
                i64::MAX
            );
            i64::MAX
        }
    }
}

/// The PostgreSQL name of a column type, as its input errors spell it.
pub fn pg_input_type_name(db_type: &crate::core::db_type::DbType) -> &'static str {
    use crate::core::db_type::DbType;
    match db_type {
        DbType::I8 | DbType::U8 | DbType::I16 => "smallint",
        DbType::U16 | DbType::I32 => "integer",
        DbType::U32 | DbType::I64 => "bigint",
        DbType::U64 | DbType::I128 | DbType::U128 | DbType::DECIMAL => "numeric",
        DbType::F32 => "real",
        DbType::F64 => "double precision",
        DbType::BOOL => "boolean",
        DbType::UUID => "uuid",
        DbType::INET => "inet",
        DbType::DATETIME => "timestamp",
        DbType::DATE => "date",
        DbType::TIME => "time",
        DbType::TIMETZ => "time with time zone",
        DbType::INTERVAL => "interval",
        DbType::JSONB | DbType::JSON => "json",
        DbType::BYTEA => "bytea",
        DbType::MACADDR => "macaddr",
        DbType::MACADDR8 => "macaddr8",
        DbType::BIT => "bit",
        DbType::VARBIT => "bit varying",
        DbType::POINT => "point",
        DbType::LINE => "line",
        DbType::LSEG => "lseg",
        DbType::BOX => "box",
        DbType::PATH => "path",
        DbType::POLYGON => "polygon",
        DbType::CIRCLE => "circle",
        DbType::INT4RANGE => "int4range",
        DbType::INT8RANGE => "int8range",
        DbType::NUMRANGE => "numrange",
        DbType::TSRANGE => "tsrange",
        DbType::TSTZRANGE => "tstzrange",
        DbType::DATERANGE => "daterange",
        DbType::CHAR | DbType::STRING => "text",
        _ => "unknown",
    }
}

/// Why `literal` cannot be stored in a column of `db_type`, in PostgreSQL's
/// words, so the SQLSTATE the wire layer derives from the text is PostgreSQL's
/// too: `invalid input syntax for type …` (22P02, or 22007 for a date/time),
/// `value "…" is out of range for type …` (22003), `date/time field value out
/// of range` (22008), `malformed array literal` (22P02), and `byteain`'s own
/// errors. `literal` is the value as written, quotes and all.
///
/// Called only after a conversion has already refused the value — it explains
/// a refusal, it never decides one.
pub fn refusal_message(db_type: &crate::core::db_type::DbType, literal: &str) -> String {
    use crate::core::db_type::DbType;
    let text = unquote_sql(literal.trim());
    let text = text.as_ref();
    let name = pg_input_type_name(db_type);
    let syntax = || format!("invalid input syntax for type {}: \"{}\"", name, text);
    let integral = {
        let t = text.trim();
        let digits = t.strip_prefix(['-', '+']).unwrap_or(t);
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
    };
    match db_type {
        DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128
        | DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64
        | DbType::U128 => {
            if integral {
                format!("value \"{}\" is out of range for type {}", text.trim(), name)
            } else {
                syntax()
            }
        }
        DbType::F32 | DbType::F64 => match text.trim().parse::<f64>() {
            Ok(_) => format!("\"{}\" is out of range for type {}", text.trim(), name),
            Err(_) => syntax(),
        },
        DbType::DECIMAL => match text.trim().parse::<f64>() {
            Ok(_) => format!("numeric field overflow: \"{}\" does not fit the column's precision", text.trim()),
            Err(_) => syntax(),
        },
        DbType::BYTEA => encode_bytea_sql(literal)
            .err()
            .unwrap_or_else(|| "invalid input syntax for type bytea".to_string()),
        DbType::JSONB | DbType::JSON => match crate::core::jsonb::encode_text(text) {
            // Well-formed JSON whose number `numeric` cannot hold: PostgreSQL
            // reports that as an overflow, not as bad syntax.
            Err(e) if e.starts_with(crate::core::jsonb::NUMERIC_OVERFLOW) => {
                crate::core::jsonb::NUMERIC_OVERFLOW.to_string()
            }
            Err(e) => format!("invalid input syntax for type json: {}", e),
            Ok(_) => syntax(),
        },
        DbType::ARRAY => format!("malformed array literal: \"{}\"", text),
        DbType::DATETIME | DbType::DATE | DbType::TIME | DbType::TIMETZ => {
            // Shaped like a date/time but not one (`2026-13-45`): a field is
            // out of range. Anything else is not a date/time at all.
            let shaped = text.bytes().any(|b| b.is_ascii_digit())
                && text
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b" -:./T+Z".contains(&b));
            if shaped {
                format!("date/time field value out of range: \"{}\"", text)
            } else {
                syntax()
            }
        }
        _ => syntax(),
    }
}

pub fn encode_literal(db_type: &crate::core::db_type::DbType, literal: &str) -> Option<Vec<u8>> {
    use crate::core::db_type::DbType;

    // `INTERVAL '1 day'`, `POINT '(1,2)'` — PostgreSQL's typed-literal form,
    // which reaches an `INSERT` with the type name still attached because the
    // value parser hands the literal over as written.
    //
    // Stripped here rather than in each type's parser: without it,
    // `INTERVAL '1 hour'` was handed to the interval reader as the whole
    // string, parsed as nothing, and stored as a zero interval. The insert
    // succeeded and the column read `00:00:00`.
    if *db_type == DbType::BYTEA {
        return encode_bytea_sql(literal).ok();
    }
    let literal = strip_type_prefix(literal);
    // A document keeps its double quotes: `"t1"` is the JSON string `t1`,
    // not an identifier quoting. Stripping them handed `t1` to the encoder,
    // which is not JSON, so `doc @> '"t1"'` failed every row it was tested
    // against.
    let text = if matches!(db_type, DbType::JSONB | DbType::JSON) {
        strip_single_quotes(literal)
    } else {
        strip_quotes(literal)
    };
    match db_type {
        DbType::UUID => parse_uuid(text).map(|b| b.to_vec()),
        DbType::INET => parse_inet(text).map(|b| b.to_vec()),
        DbType::DATETIME => parse_timestamp_us(text).map(|us| us.to_le_bytes().to_vec()),
        DbType::DECIMAL => parse_decimal(text).map(|v| v.to_le_bytes().to_vec()),
        DbType::ARRAY => parse_array_literal(text),
        // Answered above, from the literal as written rather than from `text`:
        // an `E'…'` string has to have its escapes resolved before `byteain`
        // reads it, and `strip_type_prefix` takes the `E` for a type name.
        DbType::BYTEA => encode_bytea_sql(literal).ok(),
        // A document is validated by encoding it: text that is not JSON has no
        // encoding, so it is refused here rather than stored and found later.
        DbType::JSONB => crate::core::jsonb::encode_text(text).ok(),

        // Everything below stores the bytes PostgreSQL puts on the wire, so
        // each of these is a text parser and the binary result path is a copy.
        // See `crate::core::pg_types` for why that choice differs from the one
        // the types above make.
        DbType::INTERVAL => {
            crate::core::pg_types::parse_interval(text).map(crate::core::pg_types::encode_interval)
        }
        DbType::MACADDR => crate::core::pg_types::parse_macaddr::<6>(text).map(|b| b.to_vec()),
        DbType::MACADDR8 => crate::core::pg_types::parse_macaddr::<8>(text).map(|b| b.to_vec()),
        // A `BIT(n)` literal PostgreSQL accepts is already `n` bits long — it
        // rejects any other length — so no declared width has to reach here.
        DbType::BIT | DbType::VARBIT => crate::core::pg_types::parse_bit_string(text, None),
        DbType::DATE => crate::core::pg_types::parse_date(text).map(|d| d.to_be_bytes().to_vec()),
        DbType::TIME => crate::core::pg_types::parse_time(text).map(|t| t.to_be_bytes().to_vec()),
        DbType::TIMETZ => crate::core::pg_types::parse_timetz(text),
        DbType::POINT => crate::core::pg_types::parse_point(text),
        DbType::LINE => crate::core::pg_types::parse_line(text),
        DbType::LSEG => crate::core::pg_types::parse_lseg(text),
        DbType::BOX => crate::core::pg_types::parse_box(text),
        DbType::PATH => crate::core::pg_types::parse_path(text),
        DbType::POLYGON => crate::core::pg_types::parse_polygon(text),
        DbType::CIRCLE => crate::core::pg_types::parse_circle(text),
        DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => crate::core::pg_types::parse_range(db_type, text),

        _ => None,
    }
}

/// Whether a type's literal form is text that [`encode_literal`] parses.
///
/// The insert and update paths both branch on this rather than listing the
/// types twice, which is what stops the two from drifting apart on which
/// literals they accept — they already had, once, over doubled quotes.
pub fn has_text_literal(db_type: &crate::core::db_type::DbType) -> bool {
    use crate::core::db_type::DbType;
    matches!(
        db_type,
        DbType::INTERVAL
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
            | DbType::DATERANGE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_round_trips_with_and_without_hyphens() {
        let s = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        let raw = parse_uuid(s).unwrap();
        assert_eq!(format_uuid(&raw), s);
        assert_eq!(parse_uuid(&s.replace('-', "")).unwrap(), raw);
        assert_eq!(parse_uuid(&s.to_uppercase()).unwrap(), raw);
    }

    #[test]
    fn uuid_rejects_malformed_input() {
        assert!(parse_uuid("not-a-uuid").is_none());
        assert!(
            parse_uuid("3f2504e0-4f89-11d3-9a0c-0305e82c33").is_none(),
            "too short"
        );
        assert!(
            parse_uuid("3f2504e0-4f89-11d3-9a0c-0305e82c330101").is_none(),
            "too long"
        );
        assert!(
            parse_uuid("3f2504e0-4f89-11d3-9a0c-0305e82c330").is_none(),
            "odd nibble count"
        );
    }

    #[test]
    fn uuid_bytes_order_like_the_canonical_text() {
        let a = parse_uuid("00000000-0000-0000-0000-000000000001").unwrap();
        let b = parse_uuid("00000000-0000-0000-0000-000000000002").unwrap();
        assert!(a < b);
    }

    #[test]
    fn inet_round_trips_ipv4_hosts_and_networks() {
        for s in ["10.0.0.1", "192.168.1.0/24", "0.0.0.0/0", "255.255.255.255"] {
            let raw = parse_inet(s).unwrap();
            assert_eq!(format_inet(&raw).unwrap(), s, "{}", s);
        }
    }

    #[test]
    fn inet_round_trips_ipv6_hosts_and_networks() {
        for s in ["::1", "2001:db8::/32", "fe80::1"] {
            let raw = parse_inet(s).unwrap();
            assert_eq!(format_inet(&raw).unwrap(), s, "{}", s);
        }
    }

    #[test]
    fn inet_rejects_bad_addresses_and_prefixes() {
        assert!(parse_inet("999.1.1.1").is_none());
        assert!(
            parse_inet("10.0.0.1/33").is_none(),
            "prefix wider than IPv4"
        );
        assert!(parse_inet("::1/129").is_none(), "prefix wider than IPv6");
        assert!(parse_inet("").is_none());
    }

    #[test]
    fn inet_bytes_sort_by_family_then_address() {
        let v4_low = parse_inet("10.0.0.1").unwrap();
        let v4_high = parse_inet("10.0.0.2").unwrap();
        let v6 = parse_inet("::1").unwrap();
        assert!(v4_low < v4_high);
        assert!(v4_high < v6, "IPv4 sorts before IPv6");
    }

    #[test]
    fn inet_distinguishes_a_host_from_its_network() {
        assert_ne!(
            parse_inet("10.0.0.0").unwrap(),
            parse_inet("10.0.0.0/24").unwrap()
        );
    }

    #[test]
    fn timestamp_parses_the_shapes_clients_send() {
        let expected = days_from_civil(2026, 8, 20) * 86_400 * 1_000_000;
        assert_eq!(parse_timestamp_us("2026-08-20").unwrap(), expected);
        assert_eq!(parse_timestamp_us("2026-08-20 00:00:00").unwrap(), expected);
        assert_eq!(parse_timestamp_us("2026-08-20T00:00:00").unwrap(), expected);
        assert_eq!(
            parse_timestamp_us("2026-08-20T00:00:00Z").unwrap(),
            expected
        );
        assert_eq!(parse_timestamp_us("2026-08-20 00:00").unwrap(), expected);
    }

    #[test]
    fn timestamp_applies_the_zone_offset() {
        let utc = parse_timestamp_us("2026-08-20 12:00:00Z").unwrap();
        assert_eq!(
            parse_timestamp_us("2026-08-20 14:00:00+02:00").unwrap(),
            utc
        );
        assert_eq!(parse_timestamp_us("2026-08-20 14:00:00+0200").unwrap(), utc);
        assert_eq!(parse_timestamp_us("2026-08-20 10:00:00-02").unwrap(), utc);
    }

    #[test]
    fn timestamp_keeps_sub_second_precision() {
        let base = parse_timestamp_us("2026-08-20 00:00:00").unwrap();
        assert_eq!(
            parse_timestamp_us("2026-08-20 00:00:00.5").unwrap(),
            base + 500_000
        );
        assert_eq!(
            parse_timestamp_us("2026-08-20 00:00:00.000001").unwrap(),
            base + 1
        );
        // More than microsecond precision is truncated, not rejected.
        assert_eq!(
            parse_timestamp_us("2026-08-20 00:00:00.1234567").unwrap(),
            base + 123_456
        );
    }

    #[test]
    fn timestamp_round_trips_through_text() {
        for s in [
            "2026-08-20 14:30:15",
            "1970-01-01 00:00:00",
            "1969-12-31 23:59:59",
            "2000-01-01 00:00:00",
            "2026-08-20 14:30:15.25",
        ] {
            let us = parse_timestamp_us(s).unwrap();
            assert_eq!(format_timestamp_us(us), s, "{}", s);
        }
    }

    #[test]
    fn timestamp_handles_dates_before_the_epoch() {
        let us = parse_timestamp_us("1900-06-15 12:00:00").unwrap();
        assert!(us < 0);
        assert_eq!(format_timestamp_us(us), "1900-06-15 12:00:00");
    }

    #[test]
    fn timestamp_accepts_a_bare_epoch_integer() {
        assert_eq!(parse_timestamp_us("0").unwrap(), 0);
        assert_eq!(parse_timestamp_us("-1000").unwrap(), -1000);
    }

    #[test]
    fn timestamp_rejects_nonsense() {
        assert!(parse_timestamp_us("2026-13-01").is_none(), "month 13");
        assert!(parse_timestamp_us("2026-01-32").is_none(), "day 32");
        assert!(
            parse_timestamp_us("2026-01-01 25:00:00").is_none(),
            "hour 25"
        );
        assert!(parse_timestamp_us("not a date").is_none());
        assert!(parse_timestamp_us("2026-01").is_none(), "no day");
    }

    #[test]
    fn decimal_round_trips_through_text() {
        for s in [
            "0",
            "1",
            "-1",
            "20.5",
            "-0.000000001",
            "1234567.891",
            "1000000.25",
        ] {
            let v = parse_decimal(s).unwrap();
            assert_eq!(format_decimal(v), s, "{}", s);
        }
    }

    #[test]
    fn decimal_normalises_equal_values_written_differently() {
        let a = parse_decimal("1.5").unwrap();
        for s in ["1.50", "1.500000000", "1.5e0", "0.15e1", "15e-1", "+1.5"] {
            assert_eq!(parse_decimal(s).unwrap(), a, "{}", s);
        }
    }

    #[test]
    fn decimal_scales_to_fixed_units() {
        assert_eq!(parse_decimal("1").unwrap(), 1_000_000_000);
        assert_eq!(parse_decimal("0.000000001").unwrap(), 1);
        assert_eq!(parse_decimal("-2.5").unwrap(), -2_500_000_000);
    }

    #[test]
    fn decimal_refuses_precision_it_cannot_hold() {
        // Ten fractional digits would have to be rounded away silently.
        assert!(parse_decimal("0.0000000001").is_none());
        assert!(parse_decimal("").is_none());
        assert!(parse_decimal("abc").is_none());
        assert!(parse_decimal("1.2.3").is_none());
        assert!(parse_decimal("-").is_none());
    }

    #[test]
    fn decimal_units_order_like_the_numbers() {
        let vals = ["-100", "-1.5", "0", "0.000000001", "1.5", "100"];
        let units: Vec<i128> = vals.iter().map(|s| parse_decimal(s).unwrap()).collect();
        assert!(units.windows(2).all(|w| w[0] < w[1]), "{:?}", units);
    }

    #[test]
    fn pg_numeric_encoding_matches_the_documented_layout() {
        // 1234.5678 => digits [1234, 5678], weight 0, sign +, dscale 4.
        let raw = encode_pg_numeric(parse_decimal("1234.5678").unwrap());
        assert_eq!(i16::from_be_bytes([raw[0], raw[1]]), 2, "ndigits");
        assert_eq!(i16::from_be_bytes([raw[2], raw[3]]), 0, "weight");
        assert_eq!(u16::from_be_bytes([raw[4], raw[5]]), 0, "sign");
        assert_eq!(i16::from_be_bytes([raw[6], raw[7]]), 4, "dscale");
        assert_eq!(u16::from_be_bytes([raw[8], raw[9]]), 1234);
        assert_eq!(u16::from_be_bytes([raw[10], raw[11]]), 5678);
    }

    /// A value needing no fractional digits must not render as `19.000000000`.
    #[test]
    fn pg_numeric_reports_only_the_scale_a_value_needs() {
        let raw = encode_pg_numeric(parse_decimal("19").unwrap());
        assert_eq!(
            i16::from_be_bytes([raw[6], raw[7]]),
            0,
            "dscale for a whole number"
        );

        let raw = encode_pg_numeric(parse_decimal("1930.50").unwrap());
        assert_eq!(
            i16::from_be_bytes([raw[6], raw[7]]),
            1,
            "trailing zeroes trimmed"
        );
    }

    #[test]
    fn pg_numeric_encodes_zero_as_no_digits() {
        let raw = encode_pg_numeric(0);
        assert_eq!(i16::from_be_bytes([raw[0], raw[1]]), 0, "ndigits");
        assert_eq!(raw.len(), 8);
    }

    #[test]
    fn pg_numeric_marks_negatives() {
        let raw = encode_pg_numeric(parse_decimal("-42").unwrap());
        assert_eq!(u16::from_be_bytes([raw[4], raw[5]]), 0x4000, "sign");
    }

    #[test]
    fn pg_numeric_places_sub_one_values_after_the_point() {
        // 0.5 has no integer digits, so the leading digit sits at weight -1.
        let raw = encode_pg_numeric(parse_decimal("0.5").unwrap());
        assert_eq!(i16::from_be_bytes([raw[2], raw[3]]), -1, "weight");
        assert_eq!(i16::from_be_bytes([raw[0], raw[1]]), 1, "ndigits");
        assert_eq!(u16::from_be_bytes([raw[8], raw[9]]), 5000);
    }

    #[test]
    fn encode_literal_covers_every_type_it_claims() {
        use crate::core::db_type::DbType;
        assert_eq!(
            encode_literal(&DbType::UUID, "'00000000-0000-0000-0000-000000000000'")
                .unwrap()
                .len(),
            UUID_LEN
        );
        assert_eq!(
            encode_literal(&DbType::INET, "'10.0.0.1'").unwrap().len(),
            INET_LEN
        );
        assert_eq!(
            encode_literal(&DbType::DATETIME, "'2026-08-20'")
                .unwrap()
                .len(),
            8
        );
        assert_eq!(
            encode_literal(&DbType::DECIMAL, "'20.00'").unwrap().len(),
            16
        );
        assert!(
            encode_literal(&DbType::I32, "7").is_none(),
            "numeric types stay with their caller"
        );
        assert!(encode_literal(&DbType::UUID, "'nonsense'").is_none());
    }

    #[test]
    fn coercion_widens_a_numeric_literal_to_the_column_type() {
        use crate::core::db_type::DbType;
        // `200` written as i32, compared against an i64 column.
        let lit = 200i32.to_le_bytes();
        let out = coerce_literal(&lit, &DbType::I32, &DbType::I64).unwrap();
        assert_eq!(out, 200i64.to_le_bytes().to_vec());
    }

    #[test]
    fn coercion_scales_a_numeric_literal_into_decimal_units() {
        use crate::core::db_type::DbType;
        let lit = 200i32.to_le_bytes();
        let out = coerce_literal(&lit, &DbType::I32, &DbType::DECIMAL).unwrap();
        assert_eq!(out, parse_decimal("200").unwrap().to_le_bytes().to_vec());
    }

    #[test]
    fn coercion_parses_text_literals_at_the_target_type() {
        use crate::core::db_type::DbType;
        let uuid_text = "11111111-1111-4111-8111-111111111111";
        let out = coerce_literal(uuid_text.as_bytes(), &DbType::STRING, &DbType::UUID).unwrap();
        assert_eq!(out, parse_uuid(uuid_text).unwrap().to_vec());

        let ts = "2026-01-01 08:00:00";
        let out = coerce_literal(ts.as_bytes(), &DbType::STRING, &DbType::DATETIME).unwrap();
        assert_eq!(out, parse_timestamp_us(ts).unwrap().to_le_bytes().to_vec());

        let ip = "10.0.0.1";
        let out = coerce_literal(ip.as_bytes(), &DbType::STRING, &DbType::INET).unwrap();
        assert_eq!(out, parse_inet(ip).unwrap().to_vec());
    }

    #[test]
    fn coercion_maps_boolean_spellings_onto_one_byte() {
        use crate::core::db_type::DbType;
        assert_eq!(
            coerce_literal(b"true", &DbType::STRING, &DbType::BOOL).unwrap(),
            vec![1]
        );
        assert_eq!(
            coerce_literal(b"f", &DbType::STRING, &DbType::BOOL).unwrap(),
            vec![0]
        );
        assert_eq!(
            coerce_literal(&1i32.to_le_bytes(), &DbType::I32, &DbType::BOOL).unwrap(),
            vec![1]
        );
    }

    #[test]
    fn coercion_refuses_values_the_target_cannot_hold() {
        use crate::core::db_type::DbType;
        // 300 does not fit in an i8, and wrapping it to 44 would silently
        // match the wrong rows.
        assert!(coerce_literal(&300i32.to_le_bytes(), &DbType::I32, &DbType::I8).is_none());
        assert!(coerce_literal(b"not-a-uuid", &DbType::STRING, &DbType::UUID).is_none());
        assert!(coerce_literal(&2i32.to_le_bytes(), &DbType::I32, &DbType::BOOL).is_none());
    }

    #[test]
    fn coercion_is_a_no_op_when_types_already_agree() {
        use crate::core::db_type::DbType;
        let lit = 7i64.to_le_bytes();
        assert_eq!(
            coerce_literal(&lit, &DbType::I64, &DbType::I64).unwrap(),
            lit.to_vec()
        );
    }

    #[test]
    fn coercion_result_width_always_matches_the_target() {
        use crate::core::db_type::DbType;
        for to in [
            DbType::I8,
            DbType::I16,
            DbType::I32,
            DbType::I64,
            DbType::I128,
            DbType::U8,
            DbType::U16,
            DbType::U32,
            DbType::U64,
            DbType::U128,
            DbType::F32,
            DbType::F64,
            DbType::BOOL,
            DbType::CHAR,
            DbType::DECIMAL,
            DbType::DATETIME,
        ] {
            let out = coerce_literal(&1i32.to_le_bytes(), &DbType::I32, &to)
                .unwrap_or_else(|| panic!("{:?} should accept 1", to));
            assert_eq!(
                out.len(),
                to.get_size() as usize,
                "{:?} produced the wrong width",
                to
            );
        }
    }

    #[test]
    fn a_json_string_literal_keeps_its_quotes() {
        use crate::core::db_type::DbType;
        // The WHERE path hands the literal over unquoted; the INSERT path
        // with its SQL quotes. Both are the JSON string `t1`.
        let want = crate::core::jsonb::encode_text("\"t1\"").unwrap();
        assert_eq!(encode_literal(&DbType::JSONB, "\"t1\""), Some(want.clone()));
        assert_eq!(encode_literal(&DbType::JSONB, "'\"t1\"'"), Some(want));
        assert_eq!(encode_literal(&DbType::JSONB, "t1"), None);
    }

    #[test]
    fn strip_quotes_removes_one_matching_pair_only() {
        assert_eq!(strip_quotes("'abc'"), "abc");
        assert_eq!(strip_quotes("\"abc\""), "abc");
        assert_eq!(strip_quotes("abc"), "abc");
        assert_eq!(strip_quotes("'abc"), "'abc");
        assert_eq!(strip_quotes("''abc''"), "'abc'");
    }

    #[test]
    fn timestamp_micros_order_like_the_instants() {
        let a = parse_timestamp_us("2026-01-01 00:00:00").unwrap();
        let b = parse_timestamp_us("2026-01-01 00:00:01").unwrap();
        let c = parse_timestamp_us("2027-01-01 00:00:00").unwrap();
        assert!(a < b && b < c);
    }
}

#[cfg(test)]
mod scaled_decimal_tests {
    use super::*;

    #[test]
    fn a_wide_numeric_keeps_every_declared_place() {
        // numeric(38,16). Parsing via the default nine-place scale truncated
        // this to 1.234567890 — and rejected it outright, which reached the
        // client as a schema mismatch.
        let v = parse_decimal_scaled("1.2345678901234567", 16).unwrap();
        assert_eq!(v, 12_345_678_901_234_567i128);
        assert_eq!(format_decimal_scaled(v, 16), "1.2345678901234567");
    }

    #[test]
    fn a_narrow_numeric_truncates_the_excess_rather_than_refusing_it() {
        let v = parse_decimal_scaled("10.56789", 4).unwrap();
        assert_eq!(format_decimal_scaled(v, 4), "10.5678");
    }

    #[test]
    fn a_short_literal_is_padded_to_the_declared_places() {
        let v = parse_decimal_scaled("10.5", 4).unwrap();
        assert_eq!(v, 105_000i128);
        assert_eq!(format_decimal_scaled(v, 4), "10.5000");
    }

    #[test]
    fn an_exponent_still_shifts_the_point() {
        let v = parse_decimal_scaled("1.5e2", 4).unwrap();
        assert_eq!(format_decimal_scaled(v, 4), "150.0000");
    }

    /// `NUMERIC(10)` is scale 0. Text and binary must say the same thing; the
    /// text path used to divide by 10^9 and print `42` as `0`.
    #[test]
    fn scale_zero_text_agrees_with_binary() {
        for (literal, text) in [("42", "42"), ("-7", "-7"), ("0", "0"), ("12345678901", "12345678901")] {
            let v = parse_decimal_scaled(literal, 0).unwrap();
            assert_eq!(format_decimal_scaled(v, 0), text);
            let binary = crate::core::pg_types::decode_pg_numeric(&encode_pg_numeric_scaled(v, 0));
            assert_eq!(binary.as_deref(), Some(text), "binary form of {literal}");
        }
    }

    #[test]
    fn a_non_numeric_literal_is_still_refused() {
        assert!(parse_decimal_scaled("abc", 16).is_none());
        assert!(parse_decimal_scaled("", 16).is_none());
    }

    /// PostgreSQL's answers for `NUMERIC(12,2)`, `NUMERIC(10,8)` and the
    /// engine scale: half away from zero, decided by the first dropped digit.
    #[test]
    fn an_assigned_literal_rounds_half_away_from_zero() {
        for (text, scale, want) in [
            ("20.169", 2, 2017),
            ("20.165", 2, 2017),
            ("-20.165", 2, -2017),
            ("20.1649999", 2, 2016),
            ("0.005", 2, 1),
            ("-0.005", 2, -1),
            ("1.5", 0, 2),
            ("-2.5", 0, -3),
            ("1.123456785", 8, 112345679),
            ("0.1234567895", 9, 123456790),
            ("7", 2, 700),
        ] {
            assert_eq!(parse_decimal_rounded(text, scale), Some(want), "{text} at {scale}");
        }
        assert!(parse_decimal_rounded("abc", 2).is_none());
    }

    #[test]
    fn the_declared_precision_bounds_the_integer_digits() {
        assert!(check_numeric_precision(999_999_999_999, 12, 2).is_ok());
        let err = check_numeric_precision(1_000_000_000_000, 12, 2).unwrap_err();
        assert!(err.starts_with("numeric field overflow"), "{err}");
        assert!(check_numeric_precision(-1_000_000_000_000, 12, 2).is_err());
        // Unchecked where nothing was declared.
        assert!(check_numeric_precision(i128::MAX, 0, 2).is_ok());
    }
}

/// A timestamp floored to `unit`, in microseconds since the Unix epoch.
///
/// `None` for a unit this does not know, so the caller can say which units it
/// does rather than silently returning the instant unchanged — a `GROUP BY
/// date_trunc(...)` that quietly did not truncate would produce one bucket per
/// row and look like data rather than a fault.
///
/// Negative instants (before 1970) floor *downwards*, as truncation must:
/// integer division in Rust rounds toward zero, which would round a pre-epoch
/// instant up into the following bucket.
pub fn truncate_timestamp_us(us: i64, unit: &str) -> Option<i64> {
    const SECOND: i64 = 1_000_000;
    const MINUTE: i64 = 60 * SECOND;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;

    let floor_to = |step: i64| -> i64 {
        let rem = us.rem_euclid(step);
        us - rem
    };

    let step = match unit {
        "microsecond" | "microseconds" => return Some(us),
        "millisecond" | "milliseconds" => 1_000,
        "second" | "seconds" => SECOND,
        "minute" | "minutes" => MINUTE,
        "hour" | "hours" => HOUR,
        "day" | "days" => DAY,
        // 1970-01-01 was a Thursday, so weeks starting on Monday are offset by
        // three days from the epoch.
        "week" | "weeks" => {
            const WEEK: i64 = 7 * DAY;
            const THURSDAY_OFFSET: i64 = 3 * DAY;
            return Some(((us + THURSDAY_OFFSET).div_euclid(WEEK)) * WEEK - THURSDAY_OFFSET);
        }
        // Months, quarters and years vary in length, so they are floored on the
        // calendar rather than by arithmetic.
        "month" | "months" | "quarter" | "quarters" | "year" | "years" => {
            let days = us.div_euclid(1_000_000).div_euclid(86_400);
            let (y, m, _) = civil_from_days(days);
            let month = match unit {
                "year" | "years" => 1,
                "quarter" | "quarters" => ((m - 1) / 3) * 3 + 1,
                _ => m,
            };
            return Some(days_from_civil(y, month, 1) * 86_400 * 1_000_000);
        }
        _ => return None,
    };
    Some(floor_to(step))
}

#[cfg(test)]
mod truncate_tests {
    use super::*;

    fn at(text: &str) -> i64 {
        parse_timestamp_us(text).expect("parses")
    }

    #[test]
    fn each_unit_floors_to_its_boundary() {
        let t = at("2026-08-23 21:47:33.123456");
        for (unit, expected) in [
            ("microseconds", "2026-08-23 21:47:33.123456"),
            ("second", "2026-08-23 21:47:33"),
            ("minute", "2026-08-23 21:47:00"),
            ("hour", "2026-08-23 21:00:00"),
            ("day", "2026-08-23 00:00:00"),
            ("month", "2026-08-01 00:00:00"),
            ("quarter", "2026-07-01 00:00:00"),
            ("year", "2026-01-01 00:00:00"),
        ] {
            let got = truncate_timestamp_us(t, unit).unwrap_or_else(|| panic!("{}", unit));
            assert_eq!(format_timestamp_us(got), expected, "unit {}", unit);
        }
    }

    /// Truncation floors, and integer division rounds toward zero — which would
    /// push a pre-1970 instant *forward* into the next bucket.
    #[test]
    fn an_instant_before_the_epoch_floors_downwards() {
        let t = at("1969-07-20 20:17:40");
        let day = truncate_timestamp_us(t, "day").expect("day");
        assert_eq!(format_timestamp_us(day), "1969-07-20 00:00:00");
        assert!(day <= t, "truncation must never move an instant forward");
        let year = truncate_timestamp_us(t, "year").expect("year");
        assert_eq!(format_timestamp_us(year), "1969-01-01 00:00:00");
    }

    /// A week starts on Monday; 1970-01-01 was a Thursday.
    #[test]
    fn a_week_starts_on_monday() {
        // 2026-08-23 is a Sunday, so its week began on the 17th.
        let got = truncate_timestamp_us(at("2026-08-23 12:00:00"), "week").expect("week");
        assert_eq!(format_timestamp_us(got), "2026-08-17 00:00:00");
        // A Monday is its own week start.
        let got = truncate_timestamp_us(at("2026-08-17 00:00:00"), "week").expect("week");
        assert_eq!(format_timestamp_us(got), "2026-08-17 00:00:00");
    }

    /// An unknown unit is refused rather than returning the instant unchanged:
    /// a GROUP BY that quietly did not truncate yields one bucket per row and
    /// looks like data rather than a fault.
    #[test]
    fn an_unknown_unit_is_refused() {
        assert!(truncate_timestamp_us(at("2026-08-23 12:00:00"), "fortnight").is_none());
        assert!(truncate_timestamp_us(at("2026-08-23 12:00:00"), "").is_none());
    }

    #[test]
    fn truncating_is_idempotent() {
        let t = at("2026-08-23 21:47:33.123456");
        for unit in ["second", "minute", "hour", "day", "week", "month", "year"] {
            let once = truncate_timestamp_us(t, unit).unwrap();
            let twice = truncate_timestamp_us(once, unit).unwrap();
            assert_eq!(once, twice, "unit {}", unit);
        }
    }
}

/// The output-side rewrites (digit writers, decimal and numeric encoders,
/// timestamps, bytea, decimal parsing) against the implementations they
/// replaced, over generated inputs and the edges that matter.
#[cfg(test)]
mod wire_fast_path_tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            // xorshift64*
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Unscaled values: every power of ten and its neighbours, the `u64` and
    /// `i64` boundaries, the `i128` extremes, and random magnitudes of every
    /// bit length, each with both signs.
    fn sample_values() -> Vec<i128> {
        let mut v: Vec<i128> = vec![0, 1, 9, 10, 99, 100, 9999, 10_000, 10_001, 5, 50_000];
        let mut p: i128 = 1;
        for _ in 0..=38 {
            v.extend([p - 1, p, p + 1, p.saturating_mul(5), p / 2]);
            p = p.saturating_mul(10);
        }
        for b in [u64::MAX as i128, i64::MAX as i128, i64::MIN as i128] {
            v.extend([b - 1, b, b + 1]);
        }
        v.extend([i128::MAX, i128::MAX - 1, i128::MIN + 1, i128::MIN]);
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..4000 {
            let hi = rng.next() as u128;
            let lo = rng.next() as u128;
            let shift = rng.below(128) as u32;
            v.push((((hi << 64) | lo) >> shift) as i128);
        }
        let negated: Vec<i128> = v.iter().filter_map(|x| x.checked_neg()).collect();
        v.extend(negated);
        v
    }

    #[test]
    fn digit_writers_match_to_string() {
        let mut rng = Rng(7);
        let mut values: Vec<u64> = vec![0, 1, 9, 10, 99, 100, 999, 1000, 9999, 10_000, u64::MAX, u64::MAX - 1];
        for k in 0..20 {
            values.extend([POW10_U64[k], POW10_U64[k] - 1, POW10_U64[k] + 1]);
        }
        for _ in 0..20_000 {
            values.push(rng.next() >> rng.below(64));
        }
        for &n in &values {
            let mut out = Vec::new();
            write_u64(&mut out, n);
            assert_eq!(out, n.to_string().as_bytes());
            let s = n as i64;
            out.clear();
            write_i64(&mut out, s);
            assert_eq!(out, s.to_string().as_bytes());
            for width in 1..=20usize {
                if (n as u128) < 10u128.pow(width as u32) {
                    out.clear();
                    write_u64_padded(&mut out, n, width);
                    assert_eq!(out, format!("{:0width$}", n, width = width).as_bytes());
                }
            }
        }
        for v in sample_values() {
            let mut out = Vec::new();
            write_i128(&mut out, v);
            assert_eq!(out, v.to_string().as_bytes());
        }
    }

    #[test]
    fn decimal_text_matches_the_wide_implementation() {
        for v in sample_values() {
            for scale in 0..=38u32 {
                let mut out = Vec::new();
                format_decimal_scaled_into(v, scale, &mut out);
                assert_eq!(
                    String::from_utf8(out).unwrap(),
                    format_decimal_scaled_wide(v, scale),
                    "value {v} scale {scale}"
                );
            }
            assert_eq!(format_decimal(v), format_decimal_wide(v), "value {v}");
        }
    }

    /// Whether the original encoder got this value right. It did not when the
    /// value was below 0.0001 (weight not moved past dropped zero groups), and
    /// overflowed at scales 37 and 38.
    fn legacy_is_correct(v: i128, scale: u32) -> bool {
        if scale >= 37 {
            return false;
        }
        let factor = 10u128.pow(scale);
        let magnitude = v.unsigned_abs();
        let int_part = magnitude / factor;
        let frac_part = magnitude % factor;
        if int_part == 0 && frac_part != 0 {
            let padded = scale.div_ceil(4) * 4;
            // The leading fractional group is zero exactly when the value is
            // below 0.0001.
            let leading = frac_part * 10u128.pow(padded - scale) / 10u128.pow(padded - 4);
            return leading != 0;
        }
        true
    }

    #[test]
    fn wide_integers_encode_as_scale_zero_numeric() {
        let decode = |raw: &[u8]| -> (bool, u128, i16) {
            let be16 = |at: usize| i16::from_be_bytes([raw[at], raw[at + 1]]);
            let (n, weight, sign, dscale) = (be16(0) as usize, be16(2), be16(4) as u16, be16(6));
            assert_eq!(dscale, 0);
            let mut v: u128 = 0;
            for i in 0..n {
                v = v * 10_000 + be16(8 + 2 * i) as u128;
            }
            for _ in 0..(weight as i32 + 1 - n as i32).max(0) {
                v *= 10_000;
            }
            (sign == 0x4000, v, weight)
        };
        for v in [0u128, 1, 10_000, 120_000, u64::MAX as u128, i128::MAX as u128, u128::MAX] {
            let mut out = Vec::new();
            encode_pg_numeric_u128_into(v, &mut out);
            assert_eq!(decode(&out).1, v, "{v}");
            assert!(!decode(&out).0);
        }
        for v in [i128::MIN, -1, i128::MAX] {
            let mut out = Vec::new();
            encode_pg_numeric_i128_into(v, &mut out);
            let (neg, mag, _) = decode(&out);
            assert_eq!((neg, mag), (v < 0, v.unsigned_abs()), "{v}");
            // The same bytes the scaled encoder gives at scale 0.
            assert_eq!(out, encode_pg_numeric_scaled(v, 0));
        }
    }

    #[test]
    fn numeric_binary_matches_the_original_where_it_was_right() {
        let mut compared = 0usize;
        for v in sample_values() {
            for scale in 0..=38u32 {
                let new = encode_pg_numeric_scaled(v, scale);
                if legacy_is_correct(v, scale) {
                    assert_eq!(new, encode_pg_numeric_scaled_legacy(v, scale), "value {v} scale {scale}");
                    compared += 1;
                }
                // And, everywhere, it decodes back to the value it encodes.
                // (`i128::MIN`'s magnitude does not parse back as an `i128`.)
                if v == i128::MIN {
                    continue;
                }
                let text = crate::core::pg_types::decode_pg_numeric(&new)
                    .unwrap_or_else(|| panic!("undecodable: value {v} scale {scale}"));
                assert_eq!(
                    parse_decimal_scaled(&text, scale),
                    Some(v),
                    "value {v} scale {scale} decoded as {text}"
                );
            }
        }
        assert!(compared > 100_000, "the comparison must cover most inputs ({compared})");
    }

    /// The two cases the original got wrong, pinned to PostgreSQL's layout.
    #[test]
    fn numeric_binary_below_one_ten_thousandth_moves_the_weight() {
        // 0.00005 = 0.0000 5000: one digit, weight -2, dscale 5.
        let v = parse_decimal_scaled("0.00005", 9).unwrap();
        let header = |b: &[u8]| -> [i16; 4] {
            [0, 2, 4, 6].map(|i| i16::from_be_bytes([b[i], b[i + 1]]))
        };
        let enc = encode_pg_numeric_scaled(v, 9);
        assert_eq!(header(&enc), [1, -2, 0, 5]);
        assert_eq!(&enc[8..], &5000u16.to_be_bytes());
        // -0.000000001 = -0.0000 0000 1000: weight -3.
        let enc = encode_pg_numeric_scaled(-1, 9);
        assert_eq!(header(&enc), [1, -3, 0x4000u16 as i16, 9]);
        assert_eq!(&enc[8..], &1000u16.to_be_bytes());
        assert_eq!(crate::core::pg_types::decode_pg_numeric(&enc).as_deref(), Some("-0.000000001"));
        // Zero keeps the original's shape: no digits, weight -1.
        assert_eq!(header(&encode_pg_numeric_scaled(0, 9)), [0, -1, 0, 0]);
        assert_eq!(header(&encode_pg_numeric_scaled(0, 0)), [0, -1, 0, 0]);
        // Scale 38, which overflowed: the largest fraction there is.
        let v = 10i128.pow(38) - 1;
        let enc = encode_pg_numeric_scaled(v, 38);
        assert_eq!(
            crate::core::pg_types::decode_pg_numeric(&enc).as_deref(),
            Some("0.99999999999999999999999999999999999999")
        );
    }

    #[test]
    fn timestamps_match_the_original() {
        let mut values: Vec<i64> = vec![0, 1, -1, 999_999, -999_999, 1_000_000, -1_000_000, 10, 100_000, 120, i64::MAX, i64::MIN];
        // Year boundaries: 0000, 0999/1000, 9999/10000, and BC.
        for (y, m, d) in [(0, 1, 1), (1, 1, 1), (999, 12, 31), (1000, 1, 1), (1970, 1, 1), (2000, 2, 29), (9999, 12, 31), (10000, 1, 1), (-1, 12, 31), (-5, 6, 15), (-10000, 1, 1), (275_000, 1, 1)] {
            let base = days_from_civil(y, m, d) * 86_400_000_000;
            values.extend([base, base - 1, base + 1, base + 86_399_999_999, base + 500_000, base + 123_456]);
        }
        let mut rng = Rng(0xDEAD_BEEF);
        for _ in 0..50_000 {
            let span = 400 * 366 * 86_400_000_000i64; // ~400 years either side of 1970
            values.push((rng.next() % (2 * span as u64)) as i64 - span);
            values.push(rng.next() as i64);
        }
        for us in values {
            assert_eq!(format_timestamp_us(us), format_timestamp_us_legacy(us), "us {us}");
        }
    }

    #[test]
    fn bytea_hex_matches_the_original() {
        let mut rng = Rng(3);
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(format_bytea(&all), format_bytea_legacy(&all));
        assert_eq!(format_bytea(&[]), "\\x");
        for _ in 0..2000 {
            let len = rng.below(300) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            assert_eq!(format_bytea(&bytes), format_bytea_legacy(&bytes));
        }
    }

    /// Decimal literals: digits of every length around the 19-digit fast-path
    /// limit, signs, fractions longer than the scale, exponents, whitespace,
    /// and junk.
    fn sample_literals() -> Vec<String> {
        let mut rng = Rng(11);
        let mut out: Vec<String> = [
            "", " ", "-", "+", ".", "-.", "0", "-0", "+0", "0.0", ".5", "5.", "-.5", "1e5", "1E-3",
            "1.5e2", "1.2.3", "12a", "1_000", " 42 ", "\t-7.25\n", "e5", "1e", "1e+", "--1", "+-1",
            "00000000000000000000000000000000000001", "9999999999999999999", "10000000000000000000",
            "18446744073709551615", "18446744073709551616", "-9223372036854775808",
            "170141183460469231731687303715884105727", "170141183460469231731687303715884105728",
            "0.000000001", "0.0000000001", "123456789.123456789", "1.2345678901234567890123",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        for _ in 0..20_000 {
            let mut s = String::new();
            match rng.below(4) {
                0 => s.push('-'),
                1 => s.push('+'),
                _ => {}
            }
            let int_len = rng.below(24) as usize;
            for _ in 0..int_len {
                s.push((b'0' + rng.below(10) as u8) as char);
            }
            if rng.below(3) != 0 {
                s.push('.');
                let frac_len = rng.below(24) as usize;
                for _ in 0..frac_len {
                    s.push((b'0' + rng.below(10) as u8) as char);
                }
            }
            match rng.below(10) {
                0 => s.push_str(&format!("e{}", rng.below(30) as i64 - 15)),
                1 => s.push_str("e0"),
                2 => s.insert(s.len() / 2, 'x'),
                3 => s = format!("  {}  ", s),
                _ => {}
            }
            out.push(s);
        }
        out
    }

    #[test]
    fn decimal_parsing_matches_the_original() {
        for text in sample_literals() {
            assert_eq!(parse_decimal(&text), parse_decimal_legacy(&text), "{text:?}");
            for scale in 0..=38u32 {
                assert_eq!(
                    parse_decimal_scaled(&text, scale),
                    parse_decimal_scaled_legacy(&text, scale),
                    "{text:?} at scale {scale}"
                );
            }
        }
    }

    /// Old-vs-new timings, interleaved. Run with
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored wire_codec_bench --nocapture`.
    #[test]
    #[ignore]
    fn wire_codec_bench() {
        use std::hint::black_box;
        use std::time::Instant;

        fn time<F: FnMut() -> usize>(mut f: F) -> f64 {
            let t = Instant::now();
            let n = f();
            t.elapsed().as_nanos() as f64 / n as f64
        }
        fn pair(name: &str, rounds: usize, mut old: impl FnMut() -> usize, mut new: impl FnMut() -> usize) {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            for _ in 0..rounds {
                a.push(time(&mut old));
                b.push(time(&mut new));
            }
            a.sort_by(f64::total_cmp);
            b.sort_by(f64::total_cmp);
            println!(
                "{name:<28} old {:>8.1} ns  new {:>8.1} ns  ({:.2}x)",
                a[rounds / 2],
                b[rounds / 2],
                a[rounds / 2] / b[rounds / 2]
            );
        }

        let mut rng = Rng(42);
        // Money-like values: up to 10^12 units at scale 2, and scale-9 values.
        let money: Vec<i128> = (0..4096).map(|_| (rng.next() % 1_000_000_000_000) as i128 - 500_000_000_000).collect();
        let scale9: Vec<i128> = (0..4096).map(|_| (rng.next() % 1_000_000_000_000_000) as i128).collect();
        let stamps: Vec<i64> = (0..4096).map(|_| 1_700_000_000_000_000 + (rng.next() % 100_000_000_000_000) as i64).collect();
        let blobs: Vec<Vec<u8>> = (0..256).map(|_| (0..64).map(|_| rng.next() as u8).collect()).collect();
        let literals: Vec<String> = money.iter().map(|v| format_decimal_scaled(*v, 2)).collect();

        pair("numeric binary scale 2", 15, || {
            for v in &money { black_box(encode_pg_numeric_scaled_legacy(black_box(*v), 2)); }
            money.len()
        }, || {
            let mut out = Vec::with_capacity(32);
            for v in &money { out.clear(); encode_pg_numeric_scaled_into(black_box(*v), 2, &mut out); black_box(&out); }
            money.len()
        });
        pair("numeric binary scale 9", 15, || {
            for v in &scale9 { black_box(encode_pg_numeric_scaled_legacy(black_box(*v), 9)); }
            scale9.len()
        }, || {
            let mut out = Vec::with_capacity(32);
            for v in &scale9 { out.clear(); encode_pg_numeric_scaled_into(black_box(*v), 9, &mut out); black_box(&out); }
            scale9.len()
        });
        pair("numeric text scale 2", 15, || {
            for v in &money { black_box(format_decimal_scaled_wide(black_box(*v), 2)); }
            money.len()
        }, || {
            let mut out = Vec::with_capacity(32);
            for v in &money { out.clear(); format_decimal_scaled_into(black_box(*v), 2, &mut out); black_box(&out); }
            money.len()
        });
        pair("numeric text default scale", 15, || {
            for v in &scale9 { black_box(format_decimal_wide(black_box(*v))); }
            scale9.len()
        }, || {
            let mut out = Vec::with_capacity(32);
            for v in &scale9 { out.clear(); format_decimal_into(black_box(*v), &mut out); black_box(&out); }
            scale9.len()
        });
        pair("numeric parse scale 2", 15, || {
            for t in &literals { black_box(parse_decimal_scaled_legacy(black_box(t), 2)); }
            literals.len()
        }, || {
            for t in &literals { black_box(parse_decimal_scaled(black_box(t), 2)); }
            literals.len()
        });
        pair("timestamp text", 15, || {
            for us in &stamps { black_box(format_timestamp_us_legacy(black_box(*us))); }
            stamps.len()
        }, || {
            let mut out = Vec::with_capacity(32);
            for us in &stamps { out.clear(); format_timestamp_us_into(black_box(*us), &mut out); black_box(&out); }
            stamps.len()
        });
        pair("bytea hex 64 B", 15, || {
            for b in &blobs { black_box(format_bytea_legacy(black_box(b))); }
            blobs.len()
        }, || {
            let mut out = Vec::with_capacity(160);
            for b in &blobs { out.clear(); format_bytea_into(black_box(b), &mut out); black_box(&out); }
            blobs.len()
        });
    }

    /// The originals, verbatim, as references.
    fn parse_decimal_scaled_legacy(text: &str, scale: u32) -> Option<i128> {
        if scale == DECIMAL_SCALE {
            return parse_decimal_legacy(text);
        }

        // Parsed *directly* at the declared scale. Going via `parse_decimal` and
        // rescaling would truncate to nine places first, so a `NUMERIC(38,16)`
        // holding `1.2345678901234567` lost its tail before it was ever widened —
        // and `parse_decimal` rejects more than nine places outright, which
        // surfaced as "Cannot insert row due to schema mismatch".
        let s = text.trim();
        if s.is_empty() {
            return None;
        }
        let (negative, rest) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        if rest.is_empty() {
            return None;
        }

        let (mantissa, exponent) = match rest.find(['e', 'E']) {
            Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
            None => (rest, 0),
        };
        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }

        let mut digits = String::with_capacity(int_part.len() + frac_part.len());
        digits.push_str(int_part);
        digits.push_str(frac_part);
        let mut frac_len = frac_part.len() as i32 - exponent;
        if frac_len < 0 {
            for _ in 0..(-frac_len) {
                digits.push('0');
            }
            frac_len = 0;
        }
        let mut frac_len = frac_len as u32;

        // More places than the column declared: drop the excess digits rather than
        // refuse the value, which is what a narrower column asks for.
        while frac_len > scale {
            digits.pop();
            frac_len -= 1;
        }

        let mut unscaled: i128 = if digits.is_empty() {
            0
        } else {
            digits.parse().ok()?
        };
        unscaled = unscaled.checked_mul(10i128.checked_pow(scale - frac_len)?)?;
        Some(if negative { -unscaled } else { unscaled })
    }
    fn parse_decimal_legacy(text: &str) -> Option<i128> {
        let s = text.trim();
        if s.is_empty() {
            return None;
        }

        let (negative, rest) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        if rest.is_empty() {
            return None;
        }

        // Split off an exponent, which shifts the decimal point.
        let (mantissa, exponent) = match rest.find(['e', 'E']) {
            Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
            None => (rest, 0),
        };

        let (int_part, frac_part) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }

        // Fold the exponent into the digit string so only one scaling step remains.
        let mut digits = String::with_capacity(int_part.len() + frac_part.len());
        digits.push_str(int_part);
        digits.push_str(frac_part);
        let mut frac_len = frac_part.len() as i32 - exponent;

        if frac_len < 0 {
            // The exponent moved the point right past every digit we have.
            for _ in 0..(-frac_len) {
                digits.push('0');
            }
            frac_len = 0;
        }
        let frac_len = frac_len as u32;
        if frac_len > DECIMAL_SCALE {
            return None;
        }

        let mut unscaled: i128 = if digits.is_empty() {
            0
        } else {
            digits.parse().ok()?
        };
        unscaled = unscaled.checked_mul(10i128.checked_pow(DECIMAL_SCALE - frac_len)?)?;
        Some(if negative { -unscaled } else { unscaled })
    }
}

#[cfg(test)]
mod count_as_bigint_tests {
    use super::count_as_bigint;

    /// A count past `i64::MAX` saturates; it never wraps to a negative.
    #[test]
    fn counts_saturate_at_bigint_max_and_never_go_negative() {
        assert_eq!(count_as_bigint(0), 0);
        assert_eq!(count_as_bigint(42), 42);
        assert_eq!(count_as_bigint(i64::MAX as u64), i64::MAX);
        assert_eq!(count_as_bigint(i64::MAX as u64 + 1), i64::MAX);
        assert_eq!(count_as_bigint(u64::MAX), i64::MAX);
        // What the byte reinterpretation it replaced produced.
        assert_eq!(i64::from_le_bytes(u64::MAX.to_le_bytes()), -1);
    }
}

#[cfg(test)]
mod byteain_tests {
    use super::*;

    #[test]
    fn escape_format_matches_postgres() {
        let ok: &[(&str, &[u8])] = &[
            (" ab ", b" ab "),
            ("", b""),
            ("\\001", &[1]),
            ("\\000", &[0]),
            ("\\377", &[0xff]),
            ("\\\\", b"\\"),
            ("a\\\\b", b"a\\b"),
            ("\\101\\102C", b"ABC"),
            ("\\0012", &[1, b'2']),
            ("é", "é".as_bytes()),
        ];
        for (input, want) in ok {
            assert_eq!(byteain(input).as_deref(), Ok(*want), "{:?}", input);
        }
        for bad in ["\\", "a\\b", "\\400", "\\01", "\\0", "x\\", "\\X00", "\\8"] {
            assert_eq!(
                byteain(bad),
                Err("invalid input syntax for type bytea".to_string()),
                "{:?}",
                bad
            );
        }
    }

    #[test]
    fn hex_format_matches_postgres() {
        let ok: &[(&str, &[u8])] = &[
            ("\\x", b""),
            ("\\xdead", &[0xde, 0xad]),
            ("\\xDeAd", &[0xde, 0xad]),
            ("\\x de ad ", &[0xde, 0xad]),
            ("\\x\tde\n\rad", &[0xde, 0xad]),
            ("\\x   ", b""),
        ];
        for (input, want) in ok {
            assert_eq!(byteain(input).as_deref(), Ok(*want), "{:?}", input);
        }
        assert_eq!(
            byteain("\\x0").unwrap_err(),
            "invalid hexadecimal data: odd number of digits"
        );
        assert_eq!(
            byteain("\\xd e").unwrap_err(),
            "invalid hexadecimal digit: \" \""
        );
        assert_eq!(byteain("\\xzz").unwrap_err(), "invalid hexadecimal digit: \"z\"");
        assert_eq!(byteain("\\x0é").unwrap_err(), "invalid hexadecimal digit: \"é\"");
    }

    #[test]
    fn every_byte_round_trips_through_both_formats() {
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(byteain(&format_bytea(&all)).unwrap(), all);
        let escaped: String = all.iter().map(|b| format!("\\{:03o}", b)).collect();
        assert_eq!(byteain(&escaped).unwrap(), all);
    }

    #[test]
    fn sql_literals_are_unquoted_exactly_once() {
        assert_eq!(encode_bytea_sql("'it''s'").unwrap(), b"it's");
        assert_eq!(encode_bytea_sql("'''ab'''").unwrap(), b"'ab'");
        assert_eq!(encode_bytea_sql("E'\\\\001'").unwrap(), [1]);
        assert_eq!(encode_bytea_sql("E'\\001'").unwrap(), [1]);
        assert_eq!(encode_bytea_sql("e'\\\\x0a0B'").unwrap(), [0x0a, 0x0b]);
        assert_eq!(encode_bytea_sql("E'a\\'b'").unwrap(), b"a'b");
        assert_eq!(encode_bytea_sql("BYTEA '\\xff'").unwrap(), [0xff]);
        assert_eq!(encode_bytea_sql("'\\x de ad'").unwrap(), [0xde, 0xad]);
        // `E'\\'` is one backslash after the lexer: a lone backslash to byteain.
        assert!(encode_bytea_sql("E'\\\\'").is_err());
        assert!(encode_bytea_sql("'\\'").is_err());
        // encode_literal answers the same for BYTEA, E-strings included.
        use crate::core::db_type::DbType;
        assert_eq!(encode_literal(&DbType::BYTEA, "E'\\\\001'").unwrap(), [1]);
        assert_eq!(encode_literal(&DbType::BYTEA, "' ab '").unwrap(), b" ab ");
        assert!(encode_literal(&DbType::BYTEA, "'\\x0'").is_none());
    }

    #[test]
    fn escape_strings_resolve_like_the_postgres_lexer() {
        let ok: &[(&str, &str)] = &[
            ("a\\nb", "a\nb"),
            ("\\b\\f\\r\\t\\v", "\u{8}\u{c}\r\t\u{b}"),
            ("\\101", "A"),
            ("\\x41\\x4", "A\u{4}"),
            ("\\xg", "xg"),
            ("\\u00e9", "é"),
            ("\\U0001F600", "😀"),
            ("\\uD83D\\uDE00", "😀"),
            ("it''s", "it's"),
            ("\\q", "q"),
            ("\\\\", "\\"),
            ("\\é", "é"),
        ];
        for (input, want) in ok {
            assert_eq!(unescape_e_string(input).as_deref(), Ok(*want), "{:?}", input);
        }
        for bad in ["\\000", "\\xff", "\\uD83D", "\\uDE00", "\\u12", "\\"] {
            assert!(unescape_e_string(bad).is_err(), "{:?}", bad);
        }
    }
}
