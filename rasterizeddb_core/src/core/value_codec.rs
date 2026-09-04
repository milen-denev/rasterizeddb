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

/// Renders an unscaled value held at `scale`, keeping every declared place.
///
/// `format_decimal` strips trailing zeros, so a `NUMERIC(10,4)` holding
/// `10.5000` came back as `10.5`. The declared places are significant — they
/// are what the column promised — so they are kept.
pub fn format_decimal_scaled(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return (unscaled / 10i128.pow(DECIMAL_SCALE)).to_string();
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

/// Decodes Postgres' `\x`-prefixed hex form, or the literal bytes.
pub fn parse_bytea_literal(text: &str) -> Option<Vec<u8>> {
    let trimmed = text.trim();
    let Some(hex) = trimmed.strip_prefix("\\x") else {
        return Some(trimmed.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

/// Renders bytes as Postgres' `\x` hex form.
pub fn format_bytea(raw: &[u8]) -> String {
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
            _ => encode_integer(text.trim().parse::<i128>().ok()?, to)
                .or_else(|| encode_float(text.trim().parse::<f64>().ok()?, to)),
        };
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
    let single = quote.to_string();
    let doubled = format!("{}{}", quote, quote);
    if inner.contains(&doubled) {
        Cow::Owned(inner.replace(&doubled, &single))
    } else {
        Cow::Borrowed(inner)
    }
}

pub fn quote_sql(s: &str) -> String {
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

/// Strips one layer of matching single or double quotes, if present.
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
    let literal = strip_type_prefix(literal);
    let text = strip_quotes(literal);
    match db_type {
        DbType::UUID => parse_uuid(text).map(|b| b.to_vec()),
        DbType::INET => parse_inet(text).map(|b| b.to_vec()),
        DbType::DATETIME => parse_timestamp_us(text).map(|us| us.to_le_bytes().to_vec()),
        DbType::DECIMAL => parse_decimal(text).map(|v| v.to_le_bytes().to_vec()),
        DbType::ARRAY => parse_array_literal(text),
        DbType::BYTEA => parse_bytea_literal(text),
        // A document is validated by encoding it: text that is not JSON has no
        // encoding, so it is refused here rather than stored and found later.
        DbType::JSONB => serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .map(|document| crate::core::jsonb::encode(&document)),

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

    #[test]
    fn a_non_numeric_literal_is_still_refused() {
        assert!(parse_decimal_scaled("abc", 16).is_none());
        assert!(parse_decimal_scaled("", 16).is_none());
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
