//! Cells as Rust values.
//!
//! What arrives for each column type was measured against a server, not read
//! from its source (see `tests/live.rs`):
//!
//! * integers, floats, booleans, UUIDs and points in time come with their own
//!   tag and fixed-width bytes, and decode exactly;
//! * `text`, `varchar`, `char`, `bytea`, `json`, `jsonb` and every array all
//!   arrive under one tag, [`tag::STRING`]. For `jsonb` and arrays the bytes
//!   are the engine's binary forms, which a text read would return as
//!   garbage, so read those columns as `col::text`;
//! * `NUMERIC` with no modifier (and `avg`) arrives with its scale; a
//!   `NUMERIC(p, s)` column (and `sum` over `bigint`) does not, and cannot be
//!   decoded without guessing a power of ten. That case is an error naming
//!   the fix: select `col::text`.
//!
//! Every non-text type also reads from a text cell, so a column selected as
//! `col::text` reads back as its own type: `"2026-01-02"` as a [`Date`],
//! `"12.50"` as a [`Numeric`], `"{1,2,NULL}"` as a `Vec<Option<i64>>`.

use crate::row::tag;
use crate::types::{Date, Interval, Json, Numeric, Time, Timestamp, Uuid};
use crate::value::Value;

/// A type a cell can be read as.
///
/// `'r` is the answer's lifetime, so `&str` and `&[u8]` borrow from it.
pub trait FromSql<'r>: Sized {
    /// From a non-NULL cell: its type tag and bytes. The error is a message;
    /// the row adds which column.
    fn from_sql(tag: u8, raw: &'r [u8]) -> Result<Self, String>;

    /// From a NULL: `None` (the default) makes a NULL an error, as for every
    /// type but [`Option`].
    fn from_null() -> Option<Self> {
        None
    }
}

/// A [`FromSql`] type that borrows nothing from the answer.
pub trait FromSqlOwned: for<'r> FromSql<'r> {}
impl<T: for<'r> FromSql<'r>> FromSqlOwned for T {}

impl<'r, T: FromSql<'r>> FromSql<'r> for Option<T> {
    fn from_sql(tag: u8, raw: &'r [u8]) -> Result<Self, String> {
        T::from_sql(tag, raw).map(Some)
    }
    fn from_null() -> Option<Self> {
        Some(None)
    }
}

fn is_text_tag(t: u8) -> bool {
    matches!(t, tag::STRING | tag::CHAR | tag::JSON | tag::JSONTEXT)
}

/// The cell as UTF-8 text, for any text-carrying tag.
fn text<'r>(t: u8, raw: &'r [u8], what: &str) -> Result<&'r str, String> {
    if !is_text_tag(t) {
        return Err(format!("type tag {t} is not {what}"));
    }
    std::str::from_utf8(raw).map_err(|_| "text is not valid UTF-8".to_string())
}

fn fixed<const N: usize>(t: u8, raw: &[u8]) -> Result<[u8; N], String> {
    raw.try_into()
        .map_err(|_| format!("type tag {t} needs {N} bytes, the cell has {}", raw.len()))
}

/// A `NUMERIC` cell: an `i128` and, when the wire carries it, the scale.
const NUMERIC_NO_SCALE: &str = "a NUMERIC arrived without its scale (a NUMERIC(p,s) column or \
     sum() over bigint sends none on the native protocol), so its value cannot be known: select \
     it as col::text (read as Numeric or a string) or cast it, e.g. sum(x)::bigint";

fn numeric_parts(raw: &[u8]) -> Result<(i128, u32), String> {
    match raw.len() {
        17 => Ok((
            i128::from_le_bytes(raw[..16].try_into().unwrap()),
            u32::from(raw[16]),
        )),
        16 => Err(NUMERIC_NO_SCALE.to_string()),
        n => Err(format!(
            "a NUMERIC cell of {n} bytes is not one this client reads"
        )),
    }
}

/// `unscaled × 10^-scale` as decimal text: `-0.000001`, `12.30`.
pub(crate) fn decimal_text(unscaled: i128, scale: u32) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let mut out = String::with_capacity(digits.len() + 3);
    if unscaled < 0 {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(&digits);
    } else if digits.len() > scale {
        let (int, frac) = digits.split_at(digits.len() - scale);
        out.push_str(int);
        out.push('.');
        out.push_str(frac);
    } else {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', scale - digits.len()));
        out.push_str(&digits);
    }
    out
}

/// Any integer cell, or a text or scaled NUMERIC one holding a whole number.
fn integer(t: u8, raw: &[u8]) -> Result<i128, String> {
    Ok(match t {
        tag::I8 => i8::from_le_bytes(fixed(t, raw)?) as i128,
        tag::I16 => i16::from_le_bytes(fixed(t, raw)?) as i128,
        tag::I32 => i32::from_le_bytes(fixed(t, raw)?) as i128,
        tag::I64 => i64::from_le_bytes(fixed(t, raw)?) as i128,
        tag::I128 => i128::from_le_bytes(fixed(t, raw)?),
        tag::U8 => u8::from_le_bytes(fixed(t, raw)?) as i128,
        tag::U16 => u16::from_le_bytes(fixed(t, raw)?) as i128,
        tag::U32 => u32::from_le_bytes(fixed(t, raw)?) as i128,
        tag::U64 => u64::from_le_bytes(fixed(t, raw)?) as i128,
        tag::U128 => {
            let v = u128::from_le_bytes(fixed(t, raw)?);
            i128::try_from(v).map_err(|_| format!("{v} does not fit i128"))?
        }
        tag::DECIMAL => {
            let (unscaled, scale) = numeric_parts(raw)?;
            let factor = 10i128.pow(scale.min(38));
            if unscaled % factor != 0 {
                return Err(format!(
                    "{} is not a whole number",
                    decimal_text(unscaled, scale)
                ));
            }
            unscaled / factor
        }
        t if is_text_tag(t) => {
            let s = text(t, raw, "an integer")?;
            s.trim()
                .parse::<i128>()
                .map_err(|_| format!("{s:?} is not an integer"))?
        }
        other => return Err(format!("type tag {other} is not an integer")),
    })
}

macro_rules! int_from_sql {
    ($($t:ty),*) => {$(
        impl FromSql<'_> for $t {
            fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
                let v = integer(t, raw)?;
                <$t>::try_from(v).map_err(|_| format!("{v} does not fit {}", stringify!($t)))
            }
        }
    )*};
}
int_from_sql!(
    i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize
);

impl FromSql<'_> for f64 {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        match t {
            tag::F64 => Ok(f64::from_le_bytes(fixed(t, raw)?)),
            tag::F32 => Ok(f32::from_le_bytes(fixed(t, raw)?).into()),
            tag::DECIMAL => {
                let (unscaled, scale) = numeric_parts(raw)?;
                decimal_text(unscaled, scale)
                    .parse()
                    .map_err(|e| format!("{e}"))
            }
            t if is_text_tag(t) => {
                let s = text(t, raw, "a number")?.trim();
                s.parse().map_err(|_| format!("{s:?} is not a number"))
            }
            _ => integer(t, raw).map(|v| v as f64),
        }
    }
}

impl FromSql<'_> for f32 {
    /// A `real` column; a `double precision` value only when it is exactly an
    /// `f32` (or NaN or infinite), so nothing is rounded unasked.
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        match t {
            tag::F32 => Ok(f32::from_le_bytes(fixed(t, raw)?)),
            _ => {
                let wide = f64::from_sql(t, raw)?;
                let narrow = wide as f32;
                if wide.is_nan() || f64::from(narrow) == wide {
                    Ok(narrow)
                } else {
                    Err(format!("{wide} is not exactly an f32; read it as f64"))
                }
            }
        }
    }
}

impl FromSql<'_> for bool {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        match (t, raw) {
            (tag::BOOL, [b]) => Ok(*b != 0),
            (t, _) if is_text_tag(t) => match text(t, raw, "a boolean")?.trim() {
                "t" | "true" | "TRUE" | "True" | "1" | "yes" | "on" => Ok(true),
                "f" | "false" | "FALSE" | "False" | "0" | "no" | "off" => Ok(false),
                other => Err(format!("{other:?} is not a boolean")),
            },
            (t, raw) => Err(format!(
                "type tag {t} with {} byte(s) is not a boolean",
                raw.len()
            )),
        }
    }
}

impl<'r> FromSql<'r> for &'r str {
    fn from_sql(t: u8, raw: &'r [u8]) -> Result<Self, String> {
        text(t, raw, "text")
    }
}

impl FromSql<'_> for String {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        text(t, raw, "text").map(str::to_string)
    }
}

impl<'r> FromSql<'r> for std::borrow::Cow<'r, str> {
    fn from_sql(t: u8, raw: &'r [u8]) -> Result<Self, String> {
        text(t, raw, "text").map(std::borrow::Cow::Borrowed)
    }
}

/// A `bytea` column's bytes. The engine stores `bytea` as a string of bytes,
/// so it arrives tagged [`tag::STRING`] as well as [`tag::BYTEA`].
impl<'r> FromSql<'r> for &'r [u8] {
    fn from_sql(t: u8, raw: &'r [u8]) -> Result<Self, String> {
        match t {
            tag::BYTEA | tag::STRING => Ok(raw),
            other => Err(format!("type tag {other} is not bytea")),
        }
    }
}

impl FromSql<'_> for Vec<u8> {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        <&[u8]>::from_sql(t, raw).map(<[u8]>::to_vec)
    }
}

/// A value of a type with a text form, from its own tag or from text.
fn typed<T: std::str::FromStr<Err = crate::Error>>(
    t: u8,
    raw: &[u8],
    own: u8,
    decode: impl Fn(&[u8]) -> Option<T>,
    name: &str,
) -> Result<T, String> {
    if t == own {
        return decode(raw).ok_or_else(|| format!("{} byte(s) are not a {name}", raw.len()));
    }
    let s = text(t, raw, name)?;
    s.parse().map_err(|e: crate::Error| match e {
        crate::Error::Param(m) => m,
        other => other.to_string(),
    })
}

impl FromSql<'_> for Uuid {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        typed(t, raw, tag::UUID, Uuid::from_wire, "UUID")
    }
}

impl FromSql<'_> for Timestamp {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        if t == tag::TIMESTAMP {
            return Timestamp::from_wire(raw).ok_or_else(|| "not a timestamp".to_string());
        }
        typed(t, raw, tag::DATETIME, Timestamp::from_wire, "timestamp")
    }
}

impl FromSql<'_> for Date {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        typed(t, raw, tag::DATE, Date::from_wire, "date")
    }
}

impl FromSql<'_> for Time {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        typed(t, raw, tag::TIME, Time::from_wire, "time")
    }
}

impl FromSql<'_> for Interval {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        typed(t, raw, tag::INTERVAL, Interval::from_wire, "interval")
    }
}

impl FromSql<'_> for Numeric {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        match t {
            tag::DECIMAL => {
                let (unscaled, scale) = numeric_parts(raw)?;
                Ok(Numeric::new(decimal_text(unscaled, scale)).expect("decimal text is a number"))
            }
            t if is_text_tag(t) => {
                let s = text(t, raw, "a number")?;
                Numeric::new(s.trim()).map_err(|_| format!("{s:?} is not a number"))
            }
            tag::F32 | tag::F64 => Err(format!(
                "type tag {t} is a float, which is not an exact number; read it as f64"
            )),
            _ => integer(t, raw).map(Numeric::from),
        }
    }
}

/// A JSON document's text: a `json` column, or a `jsonb` one selected as
/// `col::text`. A `jsonb` column read directly arrives as the engine's binary
/// document, which is refused rather than returned as garbage.
impl FromSql<'_> for Json {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        if t == tag::JSONB
            || raw
                .first()
                .is_some_and(|&b| b < 0x20 && !b.is_ascii_whitespace())
        {
            return Err(
                "this is a stored JSONB document, not JSON text: select the column as col::text"
                    .to_string(),
            );
        }
        text(t, raw, "JSON").map(Json::new)
    }
}

impl FromSql<'_> for Value {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        Value::decode(t, raw)
    }
    fn from_null() -> Option<Self> {
        Some(Value::Null)
    }
}

// --------------------------------------------------------------------- arrays

/// The elements of an array cell, as text (`None` a NULL): the text form
/// `{a,"b c",NULL}` a `col::text` read gives, or the binary form the engine
/// stores, which is what an array column read directly arrives as.
pub(crate) fn array_elements(t: u8, raw: &[u8]) -> Result<Vec<Option<String>>, String> {
    if is_text_tag(t) && raw.first() == Some(&b'{') {
        let s = std::str::from_utf8(raw).map_err(|_| "text is not valid UTF-8".to_string())?;
        return parse_array_text(s);
    }
    if (t == tag::ARRAY || t == tag::STRING)
        && let Some(elements) = decode_binary_array(raw)
    {
        return elements;
    }
    Err(format!("type tag {t} with these bytes is not an array"))
}

/// PostgreSQL's array output: `{1,2,NULL}`, `{"a b","q\"z"}`, `{{1,2},{3,4}}`.
/// A nested array is returned as its own text, to be parsed by the element
/// type (a `Vec<Vec<T>>`).
pub(crate) fn parse_array_text(s: &str) -> Result<Vec<Option<String>>, String> {
    let bad = || format!("{s:?} is not an array literal");
    let body = s.trim();
    // `[1:3]={…}`: explicit bounds; the elements are what matters here.
    let body = match body.find("={") {
        Some(i) if body.starts_with('[') => &body[i + 1..],
        _ => body,
    };
    let inner = body
        .strip_prefix('{')
        .and_then(|b| b.strip_suffix('}'))
        .ok_or_else(bad)?;
    let mut out = Vec::new();
    let chars: Vec<char> = inner.chars().collect();
    if chars.iter().all(|c| c.is_whitespace()) {
        return Ok(out);
    }
    let mut i = 0;
    loop {
        while chars.get(i).is_some_and(|c| c.is_whitespace()) {
            i += 1;
        }
        match chars.get(i) {
            Some('"') => {
                let mut value = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err(bad()),
                        Some('\\') => {
                            value.push(*chars.get(i + 1).ok_or_else(bad)?);
                            i += 2;
                        }
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some(&c) => {
                            value.push(c);
                            i += 1;
                        }
                    }
                }
                out.push(Some(value));
            }
            Some('{') => {
                let start = i;
                let mut depth = 0usize;
                let mut quoted = false;
                while let Some(&c) = chars.get(i) {
                    match c {
                        '\\' if quoted => i += 1,
                        '"' => quoted = !quoted,
                        '{' if !quoted => depth += 1,
                        '}' if !quoted => {
                            depth -= 1;
                            if depth == 0 {
                                i += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                if depth != 0 {
                    return Err(bad());
                }
                out.push(Some(chars[start..i].iter().collect()));
            }
            Some(_) => {
                let mut value = String::new();
                while let Some(&c) = chars.get(i) {
                    if c == ',' {
                        break;
                    }
                    if c == '\\' {
                        value.push(*chars.get(i + 1).ok_or_else(bad)?);
                        i += 2;
                        continue;
                    }
                    value.push(c);
                    i += 1;
                }
                let value = value.trim_end().to_string();
                out.push(if value.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(value)
                });
            }
            None => return Err(bad()),
        }
        while chars.get(i).is_some_and(|c| c.is_whitespace()) {
            i += 1;
        }
        match chars.get(i) {
            None => return Ok(out),
            Some(',') => i += 1,
            Some(_) => return Err(bad()),
        }
    }
}

/// PostgreSQL's binary array (`ndim`, `flags`, element OID, then per dimension
/// its length and lower bound, then each element as a length and bytes, `-1`
/// a NULL), as the engine stores it: elements are canonical text. `None` for
/// bytes that are not one; one-dimensional arrays only.
fn decode_binary_array(raw: &[u8]) -> Option<Result<Vec<Option<String>>, String>> {
    let be = |at: usize| -> Option<i32> {
        Some(i32::from_be_bytes(raw.get(at..at + 4)?.try_into().ok()?))
    };
    let ndim = be(0)?;
    let flags = be(4)?;
    if !(0..=6).contains(&ndim) || !(0..=1).contains(&flags) {
        return None;
    }
    if ndim == 0 {
        return (raw.len() == 12).then(|| Ok(Vec::new()));
    }
    if ndim > 1 {
        return Some(Err(
            "a multi-dimensional array: select it as col::text and read it as Vec<Vec<T>>"
                .to_string(),
        ));
    }
    let count = usize::try_from(be(12)?).ok()?;
    let mut at = 20;
    let mut out = Vec::with_capacity(count.min(raw.len() / 4));
    for _ in 0..count {
        let len = be(at)?;
        at += 4;
        if len < 0 {
            out.push(None);
            continue;
        }
        let end = at.checked_add(len as usize)?;
        let bytes = raw.get(at..end)?;
        out.push(Some(String::from_utf8(bytes.to_vec()).ok()?));
        at = end;
    }
    (at == raw.len()).then_some(Ok(out))
}

/// The elements of an array cell, each read as `T`.
fn array_from_sql<T: FromSqlOwned>(t: u8, raw: &[u8]) -> Result<Vec<T>, String> {
    array_elements(t, raw)?
        .into_iter()
        .enumerate()
        .map(|(i, element)| match element {
            None => T::from_null().ok_or_else(|| format!("element {} is NULL", i + 1)),
            Some(text) => T::from_sql(tag::STRING, text.as_bytes())
                .map_err(|e| format!("element {}: {e}", i + 1)),
        })
        .collect()
}

macro_rules! array_from_sql {
    ($($t:ty),* $(,)?) => {$(
        impl FromSql<'_> for Vec<$t> {
            fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
                array_from_sql(t, raw)
            }
        }
    )*};
}

array_from_sql!(
    bool,
    i16,
    i32,
    i64,
    u32,
    u64,
    f32,
    f64,
    String,
    Uuid,
    Timestamp,
    Date,
    Time,
    Interval,
    Numeric,
    Json,
    Value,
    Option<bool>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<f64>,
    Option<String>,
    Option<Uuid>,
    Option<Timestamp>,
    Option<Date>,
    Option<Numeric>,
    Vec<i16>,
    Vec<i32>,
    Vec<i64>,
    Vec<f64>,
    Vec<String>,
);

// ----------------------------------------------------------- optional crates

#[cfg(feature = "uuid")]
impl FromSql<'_> for uuid::Uuid {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        Uuid::from_sql(t, raw).map(|u| uuid::Uuid::from_bytes(u.0))
    }
}

#[cfg(feature = "chrono")]
mod chrono_impls {
    use super::*;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    impl FromSql<'_> for DateTime<Utc> {
        fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
            let ts = Timestamp::from_sql(t, raw)?;
            DateTime::from_timestamp_micros(ts.micros)
                .ok_or_else(|| format!("{ts} is out of chrono's range"))
        }
    }

    impl FromSql<'_> for NaiveDateTime {
        fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
            DateTime::<Utc>::from_sql(t, raw).map(|d| d.naive_utc())
        }
    }

    impl FromSql<'_> for NaiveDate {
        fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
            let d = Date::from_sql(t, raw)?;
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::TimeDelta::days(d.unix_days().into()))
                .ok_or_else(|| format!("{d} is out of chrono's range"))
        }
    }

    impl FromSql<'_> for NaiveTime {
        fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
            let time = Time::from_sql(t, raw)?;
            let micros = time.as_micros();
            NaiveTime::from_num_seconds_from_midnight_opt(
                (micros / 1_000_000) as u32 % 86_400,
                (micros % 1_000_000) as u32 * 1_000,
            )
            .ok_or_else(|| format!("{time} is not a chrono time"))
        }
    }
}

#[cfg(feature = "serde_json")]
impl FromSql<'_> for serde_json::Value {
    fn from_sql(t: u8, raw: &[u8]) -> Result<Self, String> {
        let json = Json::from_sql(t, raw)?;
        serde_json::from_str(json.as_str()).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_print_at_their_scale() {
        assert_eq!(decimal_text(1234, 2), "12.34");
        assert_eq!(decimal_text(-1, 6), "-0.000001");
        assert_eq!(decimal_text(5, 0), "5");
        assert_eq!(decimal_text(0, 3), "0.000");
        assert_eq!(decimal_text(i128::MIN, 0), i128::MIN.to_string());
    }

    #[test]
    fn numerics_with_and_without_a_scale() {
        let mut raw = (-1i128).to_le_bytes().to_vec();
        raw.push(6);
        assert_eq!(
            Numeric::from_sql(tag::DECIMAL, &raw).unwrap().as_str(),
            "-0.000001"
        );
        assert_eq!(f64::from_sql(tag::DECIMAL, &raw).unwrap(), -0.000001);
        assert!(i64::from_sql(tag::DECIMAL, &raw).is_err(), "not whole");
        let mut whole = 2800i128.to_le_bytes().to_vec();
        whole.push(2);
        assert_eq!(i64::from_sql(tag::DECIMAL, &whole).unwrap(), 28);
        // A NUMERIC(10,2) holding 12.34 and a sum() of 1234 send the same 16
        // bytes; neither is guessed.
        let ambiguous = 1234i128.to_le_bytes();
        let e = Numeric::from_sql(tag::DECIMAL, &ambiguous).unwrap_err();
        assert!(e.contains("::text"), "{e}");
        assert!(i64::from_sql(tag::DECIMAL, &ambiguous).is_err());
        assert!(f64::from_sql(tag::DECIMAL, &ambiguous).is_err());
        assert_eq!(
            Numeric::from_sql(tag::STRING, b"12.34").unwrap().as_str(),
            "12.34"
        );
        assert_eq!(
            Numeric::from_sql(tag::I64, &7i64.to_le_bytes())
                .unwrap()
                .as_str(),
            "7"
        );
        assert!(Numeric::from_sql(tag::F64, &1.5f64.to_le_bytes()).is_err());
    }

    #[test]
    fn floats() {
        assert_eq!(
            f64::from_sql(tag::F64, &(-2.25f64).to_le_bytes()).unwrap(),
            -2.25
        );
        assert_eq!(f64::from_sql(tag::F32, &1.5f32.to_le_bytes()).unwrap(), 1.5);
        assert_eq!(f32::from_sql(tag::F32, &0.1f32.to_le_bytes()).unwrap(), 0.1);
        assert_eq!(f32::from_sql(tag::F64, &1.5f64.to_le_bytes()).unwrap(), 1.5);
        assert!(
            f32::from_sql(tag::F64, &0.1f64.to_le_bytes()).is_err(),
            "would round"
        );
        assert!(f64::from_sql(tag::STRING, b"NaN").unwrap().is_nan());
        assert_eq!(
            f64::from_sql(tag::STRING, b"-Infinity").unwrap(),
            f64::NEG_INFINITY
        );
        assert_eq!(f64::from_sql(tag::I64, &3i64.to_le_bytes()).unwrap(), 3.0);
        assert!(f64::from_sql(tag::BOOL, &[1]).is_err());
    }

    #[test]
    fn times_from_their_tags_and_from_text() {
        let ts: Timestamp = "2026-11-16 12:34:56.789".parse().unwrap();
        assert_eq!(
            Timestamp::from_sql(tag::DATETIME, &ts.micros.to_le_bytes()).unwrap(),
            ts
        );
        assert_eq!(
            Timestamp::from_sql(tag::STRING, b"2026-11-16 12:34:56.789+00").unwrap(),
            ts
        );
        assert_eq!(
            Date::from_sql(tag::DATE, &[0, 0, 0x26, 0x58])
                .unwrap()
                .to_string(),
            "2026-11-16"
        );
        assert_eq!(
            Interval::from_sql(tag::STRING, b"1 year 2 mons -3 days +01:06:45.000006").unwrap(),
            Interval::new(14, -3, 4_005_000_006)
        );
        assert!(Date::from_sql(tag::I64, &[0; 8]).is_err());
    }

    #[test]
    fn json_refuses_a_stored_document() {
        assert_eq!(
            Json::from_sql(tag::STRING, b"{\"b\":1}").unwrap().as_str(),
            "{\"b\":1}"
        );
        assert!(Json::from_sql(tag::STRING, &[0x07, 0x01, 0, 0]).is_err());
        assert!(Json::from_sql(tag::JSONB, b"{}").is_err());
    }

    #[test]
    fn array_text_parses() {
        assert_eq!(
            parse_array_text(r#"{"a\\b","c,d","{e}","NULL","",NULL, plain ,"q\"z"}"#).unwrap(),
            vec![
                Some("a\\b".into()),
                Some("c,d".into()),
                Some("{e}".into()),
                Some("NULL".into()),
                Some("".into()),
                None,
                Some("plain".into()),
                Some("q\"z".into())
            ]
        );
        assert_eq!(parse_array_text("{}").unwrap(), vec![]);
        assert_eq!(
            parse_array_text("{{1,2},{3,NULL}}").unwrap(),
            vec![Some("{1,2}".into()), Some("{3,NULL}".into())]
        );
        assert!(parse_array_text("{1,2").is_err());
        assert!(parse_array_text("1,2").is_err());
        let v: Vec<Option<i64>> = Vec::from_sql(tag::STRING, b"{1,NULL,-3}").unwrap();
        assert_eq!(v, vec![Some(1), None, Some(-3)]);
        assert!(Vec::<i64>::from_sql(tag::STRING, b"{1,NULL}").is_err());
        let nested: Vec<Vec<i32>> = Vec::from_sql(tag::STRING, b"{{1,2},{3,4}}").unwrap();
        assert_eq!(nested, vec![vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn binary_arrays_decode() {
        // `'{1,2,NULL}'` in an INT[] column, as a server sent it.
        let raw = [
            0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0x17, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0x31, 0, 0,
            0, 1, 0x32, 0xff, 0xff, 0xff, 0xff,
        ];
        let v: Vec<Option<i32>> = Vec::from_sql(tag::STRING, &raw).unwrap();
        assert_eq!(v, vec![Some(1), Some(2), None]);
        let empty = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x17];
        assert!(
            Vec::<i32>::from_sql(tag::STRING, &empty)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn text_reads_as_other_types() {
        assert_eq!(i64::from_sql(tag::STRING, b" 42 ").unwrap(), 42);
        assert!(bool::from_sql(tag::STRING, b"t").unwrap());
        assert_eq!(
            Uuid::from_sql(tag::STRING, b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                .unwrap()
                .to_string(),
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        );
        assert!(u8::from_sql(tag::I64, &300i64.to_le_bytes()).is_err());
    }
}
