//! PostgreSQL's everyday scalar functions: text, numbers, dates, encodings,
//! JSON builders and one-dimensional arrays.
//!
//! Each was "function '…' is not supported yet", though `left`, `substring`,
//! `trim`, `concat`, `floor`, `to_char` and `json_build_object` are in most
//! applications' queries. Every answer is PostgreSQL 17's
//! (`.claude/reviews/fn/p3.sql`). A function whose exact PostgreSQL answer
//! this engine cannot give is refused rather than approximated:
//! `exp`/`ln`/`log`/`power` over NUMERIC (PostgreSQL computes those to its
//! own scale rules in arbitrary precision), and `regexp_replace` with a
//! pattern whose match boundaries the translated regex may place elsewhere
//! (`pg_regex`).
//!
//! Strict as PostgreSQL's: a NULL argument makes the result NULL, except
//! for the functions PostgreSQL defines otherwise (`concat`, `concat_ws`,
//! `format`, the JSON builders, `num_nulls`).

use crate::core::db_type::DbType;
use crate::core::rql::expr::{Expr, Value};
use crate::core::rql::pg_numeric::{self as pgn, PgNum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgFunc {
    Left,
    Right,
    Substr,
    Strpos,
    Reverse,
    Repeat,
    Concat,
    ConcatWs,
    SplitPart,
    Translate,
    Ascii,
    Chr,
    StartsWith,
    Initcap,
    Md5,
    ToHex,
    QuoteLiteral,
    Format,
    RegexpReplace,
    Ceil,
    Floor,
    Sign,
    Power,
    Sqrt,
    Cbrt,
    Exp,
    Ln,
    Log,
    Log10,
    Pi,
    Degrees,
    Radians,
    Random,
    Div,
    Gcd,
    Lcm,
    MakeDate,
    MakeTimestamp,
    ToTimestamp,
    ToDate,
    ToChar,
    Age,
    Encode,
    Decode,
    JsonBuildObject,
    JsonBuildArray,
    ToJson,
    /// `json_build_object`, `json_build_array`, `to_json`: the `json`
    /// builders, whose result is text in PostgreSQL's layout (`{"k" : v}`,
    /// arguments in order) rather than a stored document.
    JsonBuildObjectText,
    JsonBuildArrayText,
    ToJsonText,
    JsonbArrayLength,
    NumNonnulls,
    NumNulls,
    StringToArray,
    ArrayToString,
    Cardinality,
}

/// Every name [`lookup`] answers, upper case: what routes a call to the
/// evaluator from a select list or a `WHERE` (`expr::is_function`).
pub const NAMES: &[&str] = &[
    "LEFT", "RIGHT", "SUBSTR", "SUBSTRING", "STRPOS", "POSITION", "REVERSE", "REPEAT", "CONCAT", "CONCAT_WS",
    "SPLIT_PART", "TRANSLATE", "ASCII", "CHR", "STARTS_WITH", "INITCAP", "MD5", "TO_HEX", "QUOTE_LITERAL",
    "FORMAT", "REGEXP_REPLACE", "CEIL", "CEILING", "FLOOR", "SIGN", "POWER", "POW", "SQRT", "CBRT", "EXP", "LN",
    "LOG", "LOG10", "PI", "DEGREES", "RADIANS", "RANDOM", "DIV", "GCD", "LCM", "MAKE_DATE", "MAKE_TIMESTAMP",
    "TO_TIMESTAMP", "TO_DATE", "TO_CHAR", "AGE", "ENCODE", "DECODE", "JSON_BUILD_OBJECT", "JSONB_BUILD_OBJECT",
    "JSON_BUILD_ARRAY", "JSONB_BUILD_ARRAY", "TO_JSON", "TO_JSONB", "JSONB_ARRAY_LENGTH", "JSON_ARRAY_LENGTH",
    "NUM_NONNULLS", "NUM_NULLS", "STRING_TO_ARRAY", "ARRAY_TO_STRING", "CARDINALITY", "TRIM", "PG_TYPEOF",
];

/// `a ~ b`, `a ~* b`, `a !~ b`, `a !~* b` (one, at the top level) as the
/// `regexp_like` call it means, for a select list: the expression grammar
/// reads no regular-expression operator, which a `WHERE` rewrites the same
/// way. `None` for text with none, or more than one.
pub fn regex_operator_as_call(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let (mut depth, mut i) = (0i32, 0usize);
    let mut found: Option<(usize, usize, bool, bool)> = None;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'~' | b'!' if depth == 0 => {
                let negated = bytes[i] == b'!';
                let at = if negated { i + 1 } else { i };
                if bytes.get(at) != Some(&b'~') || bytes.get(at + 1) == Some(&b'~') || (at > 0 && bytes[at - 1] == b'~') {
                    i += 1;
                    continue;
                }
                let insensitive = bytes.get(at + 1) == Some(&b'*');
                let end = at + 1 + usize::from(insensitive);
                if found.is_some() {
                    return None;
                }
                found = Some((i, end, negated, insensitive));
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    let (start, end, negated, insensitive) = found?;
    let (lhs, rhs) = (text[..start].trim(), text[end..].trim());
    if lhs.is_empty() || rhs.is_empty() {
        return None;
    }
    let call = if insensitive { format!("regexp_like({lhs}, {rhs}, 'i')") } else { format!("regexp_like({lhs}, {rhs})") };
    Some(if negated { format!("NOT {call}") } else { call })
}

/// The function `upper` names with `n` arguments; `None` for a name this
/// module does not know, `Err` with PostgreSQL's 42883 for a known name
/// called with a count it has no form for.
pub fn lookup(upper: &str, n: usize) -> Option<Result<PgFunc, String>> {
    use PgFunc::*;
    let (func, counts): (PgFunc, &[usize]) = match upper {
        "LEFT" => (Left, &[2]),
        "RIGHT" => (Right, &[2]),
        "SUBSTR" | "SUBSTRING" => (Substr, &[2, 3]),
        "STRPOS" => (Strpos, &[2]),
        "REVERSE" => (Reverse, &[1]),
        "REPEAT" => (Repeat, &[2]),
        "CONCAT" => (Concat, &[]),
        "CONCAT_WS" => (ConcatWs, &[]),
        "SPLIT_PART" => (SplitPart, &[3]),
        "TRANSLATE" => (Translate, &[3]),
        "ASCII" => (Ascii, &[1]),
        "CHR" => (Chr, &[1]),
        "STARTS_WITH" => (StartsWith, &[2]),
        "INITCAP" => (Initcap, &[1]),
        "MD5" => (Md5, &[1]),
        "TO_HEX" => (ToHex, &[1]),
        "QUOTE_LITERAL" => (QuoteLiteral, &[1]),
        "FORMAT" => (Format, &[]),
        "REGEXP_REPLACE" => (RegexpReplace, &[3, 4]),
        "CEIL" | "CEILING" => (Ceil, &[1]),
        "FLOOR" => (Floor, &[1]),
        "SIGN" => (Sign, &[1]),
        "POWER" | "POW" => (Power, &[2]),
        "SQRT" => (Sqrt, &[1]),
        "CBRT" => (Cbrt, &[1]),
        "EXP" => (Exp, &[1]),
        "LN" => (Ln, &[1]),
        "LOG" => (Log, &[1, 2]),
        "LOG10" => (Log10, &[1]),
        "PI" => (Pi, &[0]),
        "DEGREES" => (Degrees, &[1]),
        "RADIANS" => (Radians, &[1]),
        "RANDOM" => (Random, &[0]),
        "DIV" => (Div, &[2]),
        "GCD" => (Gcd, &[2]),
        "LCM" => (Lcm, &[2]),
        "MAKE_DATE" => (MakeDate, &[3]),
        "MAKE_TIMESTAMP" => (MakeTimestamp, &[6]),
        "TO_TIMESTAMP" => (ToTimestamp, &[1, 2]),
        "TO_DATE" => (ToDate, &[2]),
        "TO_CHAR" => (ToChar, &[2]),
        "AGE" => (Age, &[1, 2]),
        "ENCODE" => (Encode, &[2]),
        "DECODE" => (Decode, &[2]),
        "JSONB_BUILD_OBJECT" => (JsonBuildObject, &[]),
        "JSONB_BUILD_ARRAY" => (JsonBuildArray, &[]),
        "TO_JSONB" => (ToJson, &[1]),
        "JSON_BUILD_OBJECT" => (JsonBuildObjectText, &[]),
        "JSON_BUILD_ARRAY" => (JsonBuildArrayText, &[]),
        "TO_JSON" => (ToJsonText, &[1]),
        "JSONB_ARRAY_LENGTH" | "JSON_ARRAY_LENGTH" => (JsonbArrayLength, &[1]),
        "NUM_NONNULLS" => (NumNonnulls, &[]),
        "NUM_NULLS" => (NumNulls, &[]),
        "STRING_TO_ARRAY" => (StringToArray, &[2, 3]),
        "ARRAY_TO_STRING" => (ArrayToString, &[2, 3]),
        "CARDINALITY" => (Cardinality, &[1]),
        _ => return None,
    };
    let variadic_ok = match func {
        Concat | Format | NumNonnulls | NumNulls => n >= 1,
        ConcatWs => n >= 2,
        JsonBuildObject | JsonBuildObjectText => n % 2 == 0,
        JsonBuildArray | JsonBuildArrayText => true,
        _ => counts.contains(&n),
    };
    if !variadic_ok {
        return Some(Err(if matches!(func, JsonBuildObject | JsonBuildObjectText) {
            "argument list must have even number of elements".to_string()
        } else if n == 0 {
            format!("function {}() does not exist", upper.to_ascii_lowercase())
        } else {
            format!("function {}({} arguments) does not exist", upper.to_ascii_lowercase(), n)
        }));
    }
    Some(Ok(func))
}

/// What a call to `func` over `args` evaluates to, as PostgreSQL types it:
/// `ceil`, `floor`, `sign`, `sqrt` keep NUMERIC and are `double precision`
/// otherwise, `div` is NUMERIC, the text family is text.
pub fn result_type(func: PgFunc, args: &[Expr]) -> DbType {
    use PgFunc::*;
    let first = || args.first().map(crate::core::rql::expr::static_type).unwrap_or(DbType::NULL);
    match func {
        Strpos | Ascii | JsonbArrayLength | NumNonnulls | NumNulls | Cardinality => DbType::I32,
        StartsWith => DbType::BOOL,
        Ceil | Floor | Sign | Sqrt => {
            if first() == DbType::DECIMAL {
                DbType::DECIMAL
            } else {
                DbType::F64
            }
        }
        Power | Exp | Ln | Log | Log10
            if numeric_call_typed(func, &args.iter().map(crate::core::rql::expr::static_type).collect::<Vec<_>>()) =>
        {
            DbType::DECIMAL
        }
        Power | Cbrt | Exp | Ln | Log | Log10 | Pi | Degrees | Radians | Random => DbType::F64,
        Div => DbType::DECIMAL,
        Gcd | Lcm => match first() {
            DbType::I64 => DbType::I64,
            DbType::DECIMAL => DbType::DECIMAL,
            _ => DbType::I32,
        },
        MakeDate | ToDate => DbType::DATE,
        MakeTimestamp | ToTimestamp => DbType::DATETIME,
        Age => DbType::INTERVAL,
        Decode => DbType::BYTEA,
        JsonBuildObject | JsonBuildArray | ToJson => DbType::JSONB,
        JsonBuildObjectText | JsonBuildArrayText | ToJsonText => DbType::JSONTEXT,
        StringToArray => DbType::ARRAY,
        _ => DbType::STRING,
    }
}

/// Whether `func` yields `json`, text in PostgreSQL's layout, which travels
/// as text (`expr::yields_json_text`).
pub fn yields_json_text(func: PgFunc) -> bool {
    matches!(func, PgFunc::JsonBuildObjectText | PgFunc::JsonBuildArrayText | PgFunc::ToJsonText)
}

/// Whether `func` yields a `timestamp` (without time zone), which shows no
/// offset: `make_timestamp`. (`to_timestamp` is a `timestamptz`.)
pub fn yields_timestamp(func: PgFunc) -> bool {
    func == PgFunc::MakeTimestamp
}

/// `SUBSTRING(s FROM a FOR b)`, `POSITION(x IN y)`, `TRIM([BOTH | LEADING |
/// TRAILING] [chars] FROM s)` and `TRIM(s)`: the SQL-standard spellings as
/// the plain calls they mean (`substr`, `strpos`, `btrim`/`ltrim`/`rtrim`),
/// `None` when `args` is already a plain argument list.
pub fn standard_syntax(upper: &str, args: &str) -> Option<(String, String)> {
    use crate::core::rql::join::find_keyword;
    match upper {
        "SUBSTRING" => {
            let from = find_keyword(args, "FROM", 0);
            let for_ = find_keyword(args, "FOR", 0);
            if from.is_none() && for_.is_none() {
                return None;
            }
            let end_of_value = [from, for_].into_iter().flatten().min()?;
            let value = args[..end_of_value].trim();
            let start = match from {
                Some(f) => {
                    let end = for_.filter(|p| *p > f).unwrap_or(args.len());
                    args[f + 4..end].trim().to_string()
                }
                None => "1".to_string(),
            };
            Some(match for_ {
                Some(p) => ("SUBSTR".to_string(), format!("{value}, {start}, {}", args[p + 3..].trim())),
                None => ("SUBSTR".to_string(), format!("{value}, {start}")),
            })
        }
        "POSITION" => {
            let at = find_keyword(args, "IN", 0)?;
            Some(("STRPOS".to_string(), format!("{}, {}", args[at + 2..].trim(), args[..at].trim())))
        }
        "TRIM" => {
            let mut rest = args.trim();
            let mut func = "BTRIM";
            for (word, name) in [("BOTH", "BTRIM"), ("LEADING", "LTRIM"), ("TRAILING", "RTRIM")] {
                if rest.len() > word.len()
                    && rest[..word.len()].eq_ignore_ascii_case(word)
                    && !rest.as_bytes()[word.len()].is_ascii_alphanumeric()
                {
                    func = name;
                    rest = rest[word.len()..].trim_start();
                    break;
                }
            }
            Some(match find_keyword(rest, "FROM", 0) {
                Some(at) => {
                    let chars = rest[..at].trim();
                    let value = rest[at + 4..].trim();
                    if chars.is_empty() {
                        (func.to_string(), value.to_string())
                    } else {
                        (func.to_string(), format!("{value}, {chars}"))
                    }
                }
                None => (func.to_string(), rest.to_string()),
            })
        }
        _ => None,
    }
}

fn data(bytes: Vec<u8>, db_type: DbType) -> Value {
    Value::Data { bytes, db_type }
}

fn text_value(text: String) -> Value {
    data(text.into_bytes(), DbType::STRING)
}

fn int_value(v: i64) -> Value {
    data((v as i32).to_le_bytes().to_vec(), DbType::I32)
}

fn float_value(v: f64) -> Value {
    data(v.to_le_bytes().to_vec(), DbType::F64)
}

/// A value as the text PostgreSQL prints it.
fn text_of(v: &Value) -> String {
    match v {
        // `boolout`: what `concat` and `format` show (JSON writes `true`).
        Value::Data { bytes, db_type: DbType::BOOL } => {
            if bytes.first().is_some_and(|b| *b != 0) { "t" } else { "f" }.to_string()
        }
        Value::Data { bytes, db_type: DbType::INTERVAL } => {
            crate::core::pg_types::format_interval(bytes).unwrap_or_default()
        }
        Value::Data { bytes, db_type: DbType::BYTEA } => crate::core::value_codec::format_bytea(bytes),
        Value::Data { bytes, db_type: DbType::ARRAY } => crate::core::value_codec::format_array(bytes).unwrap_or_default(),
        // At the engine's nine places (no scale of its own): the places the
        // value needs. A NUMERIC whose expression has a display scale was
        // brought to it before the call (`shown_at`).
        Value::Data { db_type: DbType::DECIMAL, bytes } if bytes.len() == 16 => {
            let m = i128::from_le_bytes(bytes.as_slice().try_into().expect("sixteen bytes"));
            PgNum::trimmed(m, crate::core::value_codec::DECIMAL_SCALE).to_text()
        }
        Value::Data { db_type: DbType::DECIMAL, bytes } => {
            crate::core::rql::expr::decimal_value_as_pgnum(bytes).map(PgNum::to_text).unwrap_or_default()
        }
        other => crate::core::rql::expr::as_text(other),
    }
}

/// An integer argument; `Err` for one that is not an integer.
fn int_of(v: &Value, what: &str) -> Result<i64, String> {
    let Value::Data { bytes, db_type } = v else {
        return Err(format!("{what} is NULL"));
    };
    match db_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 => {
            crate::core::rql::expr::as_i64(v).ok_or_else(|| format!("{what} is not an integer"))
        }
        DbType::DECIMAL => {
            let n = crate::core::rql::expr::decimal_value_as_pgnum(bytes)?;
            let r = n.round_to(0).ok_or("integer out of range")?;
            i64::try_from(r.m).map_err(|_| "integer out of range".to_string())
        }
        DbType::F32 | DbType::F64 => {
            let f = crate::core::rql::expr::as_f64(v).ok_or_else(|| format!("{what} is not a number"))?;
            Ok(f.round_ties_even() as i64)
        }
        DbType::STRING => text_of(v).trim().parse().map_err(|_| format!("invalid input syntax for type integer: \"{}\"", text_of(v))),
        _ => Err(format!("{what} is not an integer")),
    }
}

fn float_of(v: &Value) -> Result<f64, String> {
    crate::core::rql::expr::as_f64(v)
        .or_else(|| text_of(v).trim().parse().ok())
        .ok_or_else(|| format!("invalid input syntax for type double precision: \"{}\"", text_of(v)))
}

fn numeric_of(v: &Value) -> Result<PgNum, String> {
    match v {
        Value::Data { bytes, db_type: DbType::DECIMAL } => crate::core::rql::expr::decimal_value_as_pgnum(bytes),
        Value::Data { db_type: DbType::F32 | DbType::F64, .. } => {
            pgn::from_f64(float_of(v)?).ok_or_else(|| "cannot convert infinity or NaN to numeric".to_string())
        }
        other => pgn::parse_text(&text_of(other)).ok_or_else(|| format!("invalid input syntax for type numeric: \"{}\"", text_of(other))),
    }
}

fn numeric_value(v: PgNum) -> Value {
    data(v.to_cell(), DbType::DECIMAL)
}

fn is_numeric(v: &Value) -> bool {
    matches!(v, Value::Data { db_type: DbType::DECIMAL, .. })
}

fn is_float(v: &Value) -> bool {
    matches!(v, Value::Data { db_type: DbType::F32 | DbType::F64, .. })
}

/// Whether a call to one of the functions PostgreSQL defines over both
/// `numeric` and `double precision` takes the `numeric` one: any NUMERIC
/// argument and no float one (a float does not cast to NUMERIC implicitly,
/// NUMERIC does to a float). Integers alone take `double precision`, except
/// `log(b, x)`, which has no float form.
fn numeric_call(func: PgFunc, args: &[Value]) -> bool {
    !args.iter().any(is_float)
        && (args.iter().any(is_numeric) || (func == PgFunc::Log && args.len() == 2))
}

/// The same choice over the arguments' static types, for [`result_type`].
fn numeric_call_typed(func: PgFunc, types: &[DbType]) -> bool {
    !types.iter().any(|t| matches!(t, DbType::F32 | DbType::F64))
        && (types.contains(&DbType::DECIMAL) || (func == PgFunc::Log && types.len() == 2))
}

/// `dpow`: `x ^ y` over `double precision`, with its two domain errors and
/// its range check.
fn float_power(x: f64, y: f64) -> Result<f64, String> {
    use crate::core::rql::pg_numeric_math::{NEGATIVE_TO_FRACTION, ZERO_TO_NEGATIVE};
    if x == 0.0 && y < 0.0 {
        return Err(ZERO_TO_NEGATIVE.to_string());
    }
    if x < 0.0 && y.is_finite() && y.floor() != y {
        return Err(NEGATIVE_TO_FRACTION.to_string());
    }
    let r = x.powf(y);
    if r.is_infinite() && x.is_finite() && y.is_finite() {
        return Err("value out of range: overflow".to_string());
    }
    if r == 0.0 && x != 0.0 && x.is_finite() && y.is_finite() {
        return Err("value out of range: underflow".to_string());
    }
    Ok(r)
}

/// A NUMERIC argument carrying the display scale its expression has
/// (`expr::display_scale`), so it prints as PostgreSQL shows it: `2.50`, a
/// `NUMERIC(8,3)` column's `12.345`. Anything else unchanged.
pub fn shown_at(value: Value, scale: u32) -> Value {
    match &value {
        Value::Data { bytes, db_type: DbType::DECIMAL } => match crate::core::rql::expr::decimal_value_as_pgnum(bytes)
            .ok()
            .and_then(|n| n.round_to(scale))
        {
            Some(n) => numeric_value(n),
            None => value,
        },
        _ => value,
    }
}

/// Calls `func` over its evaluated arguments.
pub fn call(func: PgFunc, args: &[Value]) -> Result<Value, String> {
    use PgFunc::*;
    let non_strict = matches!(func, Concat | ConcatWs | Format | JsonBuildObject | JsonBuildArray | JsonBuildObjectText | JsonBuildArrayText | NumNonnulls | NumNulls | ToJson | ToJsonText | StringToArray | ArrayToString);
    if !non_strict && args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    let s = |i: usize| text_of(&args[i]);
    Ok(match func {
        Left | Right => {
            let chars: Vec<char> = s(0).chars().collect();
            let n = int_of(&args[1], "the length")?;
            let len = chars.len() as i64;
            let take = if n >= 0 { n.min(len) } else { (len + n).max(0) } as usize;
            text_value(if func == Left {
                chars[..take].iter().collect()
            } else {
                chars[chars.len() - take..].iter().collect()
            })
        }
        Substr => {
            let chars: Vec<char> = s(0).chars().collect();
            let start = int_of(&args[1], "the start")?;
            let end = match args.get(2) {
                Some(len) => {
                    let len = int_of(len, "the length")?;
                    if len < 0 {
                        return Err("negative substring length not allowed".to_string());
                    }
                    start.saturating_add(len)
                }
                None => i64::MAX,
            };
            let from = (start.max(1) - 1) as usize;
            let to = (end.max(1) - 1).min(chars.len() as i64) as usize;
            text_value(if from >= to { String::new() } else { chars[from..to].iter().collect() })
        }
        Strpos => {
            let (hay, needle) = (s(0), s(1));
            int_value(match hay.find(&needle) {
                Some(at) => hay[..at].chars().count() as i64 + 1,
                None => 0,
            })
        }
        Reverse => text_value(s(0).chars().rev().collect()),
        Repeat => {
            let n = int_of(&args[1], "the count")?.max(0) as usize;
            let text = s(0);
            if text.len().saturating_mul(n) > crate::core::rql::type_modifier::MAX_TEXT_LENGTH as usize * 4 {
                return Err("requested length too large".to_string());
            }
            text_value(text.repeat(n))
        }
        Concat => text_value(args.iter().filter(|a| !a.is_null()).map(text_of).collect()),
        ConcatWs => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            let sep = s(0);
            text_value(args[1..].iter().filter(|a| !a.is_null()).map(text_of).collect::<Vec<_>>().join(&sep))
        }
        SplitPart => {
            let (text, delim) = (s(0), s(1));
            let n = int_of(&args[2], "the field position")?;
            if n == 0 {
                return Err("field position must not be zero".to_string());
            }
            let parts: Vec<&str> = if delim.is_empty() { vec![text.as_str()] } else { text.split(delim.as_str()).collect() };
            let at = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            text_value(if (0..parts.len() as i64).contains(&at) { parts[at as usize].to_string() } else { String::new() })
        }
        Translate => {
            let (from, to): (Vec<char>, Vec<char>) = (s(1).chars().collect(), s(2).chars().collect());
            text_value(
                s(0)
                    .chars()
                    .filter_map(|c| match from.iter().position(|f| *f == c) {
                        Some(i) => to.get(i).copied(),
                        None => Some(c),
                    })
                    .collect(),
            )
        }
        Ascii => int_value(s(0).chars().next().map_or(0, |c| c as i64)),
        Chr => {
            let code = int_of(&args[0], "the code")?;
            if code == 0 {
                return Err("null character not permitted".to_string());
            }
            let c = u32::try_from(code).ok().and_then(char::from_u32).ok_or_else(|| "requested character too large for encoding".to_string())?;
            text_value(c.to_string())
        }
        StartsWith => data(vec![u8::from(s(0).starts_with(&s(1)))], DbType::BOOL),
        Initcap => {
            let mut out = String::new();
            let mut in_word = false;
            for c in s(0).chars() {
                if c.is_alphanumeric() {
                    if in_word {
                        out.extend(c.to_lowercase());
                    } else {
                        out.extend(c.to_uppercase());
                    }
                    in_word = true;
                } else {
                    out.push(c);
                    in_word = false;
                }
            }
            text_value(out)
        }
        Md5 => {
            use md5::Digest;
            let digest = md5::Md5::digest(match &args[0] {
                Value::Data { bytes, db_type: DbType::BYTEA } => bytes.clone(),
                other => text_of(other).into_bytes(),
            });
            text_value(digest.iter().map(|b| format!("{b:02x}")).collect())
        }
        ToHex => {
            let v = int_of(&args[0], "the value")?;
            let wide = matches!(&args[0], Value::Data { db_type: DbType::I64, .. }) || i32::try_from(v).is_err();
            text_value(if wide { format!("{:x}", v as u64) } else { format!("{:x}", v as i32 as u32) })
        }
        QuoteLiteral => text_value(quote_literal(&s(0))),
        Format => text_value(format_text(args)?),
        RegexpReplace => {
            let flags = args.get(3).map(text_of).unwrap_or_default();
            regexp_replace(&s(0), &s(1), &s(2), &flags)?
        }
        Ceil | Floor => {
            if is_numeric(&args[0]) {
                let n = numeric_of(&args[0])?;
                let t = n.trunc_places(0).ok_or("numeric out of range")?;
                let bump = if func == Ceil { n.cmp_value(t) == std::cmp::Ordering::Greater } else { n.cmp_value(t) == std::cmp::Ordering::Less };
                let step = if func == Ceil { 1 } else { -1 };
                numeric_value(if bump { PgNum::int(t.m + step) } else { PgNum::int(t.m) })
            } else {
                let f = float_of(&args[0])?;
                float_value(if func == Ceil { f.ceil() } else { f.floor() })
            }
        }
        Sign => {
            if is_numeric(&args[0]) {
                let n = numeric_of(&args[0])?;
                numeric_value(PgNum::int(n.m.signum()))
            } else {
                let f = float_of(&args[0])?;
                float_value(if f > 0.0 { 1.0 } else if f < 0.0 { -1.0 } else { 0.0 })
            }
        }
        Sqrt if is_numeric(&args[0]) => {
            let n = numeric_of(&args[0])?;
            if n.m < 0 {
                return Err("cannot take square root of a negative number".to_string());
            }
            numeric_value(numeric_sqrt(n)?)
        }
        // NUMERIC as PostgreSQL resolves them (`numeric_call`), at the scale
        // numeric.c gives each result (`pg_numeric_math`). `log(b, x)` exists
        // only over NUMERIC, so integers take it too.
        Exp | Ln | Log | Log10 | Power if numeric_call(func, args) => {
            use crate::core::rql::pg_numeric_math as math;
            let ten = PgNum::int(10);
            numeric_value(match (func, args.len()) {
                (Exp, _) => math::exp(numeric_of(&args[0])?)?,
                (Ln, _) => math::ln(numeric_of(&args[0])?)?,
                (Log, 2) => math::log(numeric_of(&args[0])?, numeric_of(&args[1])?)?,
                (Log | Log10, _) => math::log(ten, numeric_of(&args[0])?)?,
                _ => math::power(numeric_of(&args[0])?, numeric_of(&args[1])?)?,
            })
        }
        Sqrt | Cbrt | Exp | Ln | Log10 | Degrees | Radians => {
            let x = float_of(&args[0])?;
            float_value(match func {
                Sqrt => {
                    if x < 0.0 {
                        return Err("cannot take square root of a negative number".to_string());
                    }
                    x.sqrt()
                }
                // The C library's `cbrt`, as PostgreSQL's `dcbrt`; its last
                // digit is that library's (3.0000000000000004 in the PG17
                // container for 27, 3 here).
                Cbrt => x.cbrt(),
                // `dexp`: an infinite or zero result from a finite argument
                // is out of range, not a value.
                Exp => match x.exp() {
                    r if r.is_infinite() && x.is_finite() => return Err("value out of range: overflow".to_string()),
                    r if r == 0.0 && x.is_finite() => return Err("value out of range: underflow".to_string()),
                    r => r,
                },
                Ln | Log10 if x == 0.0 => return Err("cannot take logarithm of zero".to_string()),
                Ln | Log10 if x < 0.0 => return Err("cannot take logarithm of a negative number".to_string()),
                Ln => x.ln(),
                Log10 => x.log10(),
                Degrees => x.to_degrees(),
                _ => x.to_radians(),
            })
        }
        Log => {
            if args.len() == 1 {
                let x = float_of(&args[0])?;
                if x <= 0.0 {
                    return Err(if x == 0.0 { "cannot take logarithm of zero" } else { "cannot take logarithm of a negative number" }.to_string());
                }
                float_value(x.log10())
            } else {
                let (b, x) = (float_of(&args[0])?, float_of(&args[1])?);
                float_value(x.ln() / b.ln())
            }
        }
        Power => float_value(float_power(float_of(&args[0])?, float_of(&args[1])?)?),
        Pi => float_value(std::f64::consts::PI),
        Random => float_value(fastrand::f64()),
        Div => {
            let (a, b) = (numeric_of(&args[0])?, numeric_of(&args[1])?);
            if b.m == 0 {
                return Err("division by zero".to_string());
            }
            let q = a.div(b).map_err(|e| e.to_string())?.ok_or("numeric out of range")?;
            numeric_value(PgNum::int(q.trunc_places(0).ok_or("numeric out of range")?.m))
        }
        Gcd | Lcm => {
            let (a, b) = (int_of(&args[0], "the value")? as i128, int_of(&args[1], "the value")? as i128);
            let g = {
                let (mut x, mut y) = (a.abs(), b.abs());
                while y != 0 {
                    (x, y) = (y, x % y);
                }
                x
            };
            let v = if func == Gcd { g } else if g == 0 { 0 } else { (a / g * b).abs() };
            match &args[0] {
                Value::Data { db_type: DbType::I64, .. } => data((v as i64).to_le_bytes().to_vec(), DbType::I64),
                _ => int_value(i64::try_from(v).map_err(|_| "integer out of range")?),
            }
        }
        MakeDate => {
            let (y, m, d) = (int_of(&args[0], "the year")?, int_of(&args[1], "the month")?, int_of(&args[2], "the day")?);
            data(date_bytes(y, m, d)?, DbType::DATE)
        }
        MakeTimestamp => {
            let (y, mo, d) = (int_of(&args[0], "the year")?, int_of(&args[1], "the month")?, int_of(&args[2], "the day")?);
            let (h, mi) = (int_of(&args[3], "the hour")?, int_of(&args[4], "the minute")?);
            let sec = float_of(&args[5])?;
            if !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0.0..60.0).contains(&sec) {
                return Err(format!("date field value out of range: {y}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec}"));
            }
            let days = days_of(y, mo, d)?;
            let us = days * 86_400_000_000 + (h * 3600 + mi * 60) * 1_000_000 + (sec * 1e6).round() as i64;
            data(us.to_le_bytes().to_vec(), DbType::DATETIME)
        }
        ToTimestamp if args.len() == 1 => {
            let secs = float_of(&args[0])?;
            if !secs.is_finite() {
                return Err("timestamp out of range".to_string());
            }
            data(((secs * 1e6).round() as i64).to_le_bytes().to_vec(), DbType::DATETIME)
        }
        ToTimestamp | ToDate => {
            let parsed = parse_with_template(&s(0), &s(1))?;
            if func == ToDate {
                data(date_bytes(parsed.year, parsed.month, parsed.day)?, DbType::DATE)
            } else {
                let days = days_of(parsed.year, parsed.month, parsed.day)?;
                let us = days * 86_400_000_000 + parsed.micros_of_day;
                data(us.to_le_bytes().to_vec(), DbType::DATETIME)
            }
        }
        ToChar => text_value(to_char(&args[0], &s(1))?),
        Age => {
            let later = match args.len() {
                2 => timestamp_of(&args[0])?,
                _ => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| e.to_string())?
                        .as_micros() as i64;
                    now - now.rem_euclid(86_400_000_000)
                }
            };
            let earlier = timestamp_of(args.last().expect("one or two"))?;
            data(crate::core::pg_types::encode_interval(age(later, earlier)), DbType::INTERVAL)
        }
        Encode => {
            let bytes = match &args[0] {
                Value::Data { bytes, .. } => bytes.clone(),
                Value::Null => return Ok(Value::Null),
            };
            text_value(match s(1).to_ascii_lowercase().as_str() {
                "hex" => bytes.iter().map(|b| format!("{b:02x}")).collect(),
                "base64" => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.encode(&bytes)
                }
                "escape" => crate::core::value_codec::format_bytea(&bytes),
                other => return Err(format!("unrecognized encoding: \"{other}\"")),
            })
        }
        Decode => {
            let text = s(0);
            let bytes = match s(1).to_ascii_lowercase().as_str() {
                "hex" => {
                    let clean: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
                    if clean.len() % 2 != 0 {
                        return Err("invalid hexadecimal data: odd number of digits".to_string());
                    }
                    clean
                        .chunks(2)
                        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap_or("zz"), 16))
                        .collect::<Result<Vec<u8>, _>>()
                        .map_err(|_| "invalid hexadecimal digit".to_string())?
                }
                "base64" => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD
                        .decode(text.trim())
                        .map_err(|e| format!("invalid base64 data: {e}"))?
                }
                other => return Err(format!("unrecognized encoding: \"{other}\"")),
            };
            data(bytes, DbType::BYTEA)
        }
        JsonBuildObject => {
            let mut out = String::from("{");
            for (i, pair) in args.chunks(2).enumerate() {
                if pair[0].is_null() {
                    return Err("null value not allowed for object key".to_string());
                }
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&json_string(&text_of(&pair[0])));
                out.push_str(": ");
                out.push_str(&json_of(&pair[1])?);
            }
            out.push('}');
            data(crate::core::jsonb::encode_text(&out)?, DbType::JSONB)
        }
        JsonBuildArray => {
            let items: Vec<String> = args.iter().map(json_of).collect::<Result<_, _>>()?;
            data(crate::core::jsonb::encode_text(&format!("[{}]", items.join(", ")))?, DbType::JSONB)
        }
        // The `json` forms: PostgreSQL's text, `"k" : v` with a space each
        // side of the colon, the arguments in order, a `json` argument as it
        // was written.
        JsonBuildObjectText => {
            let mut out = String::from("{");
            for (i, pair) in args.chunks(2).enumerate() {
                if pair[0].is_null() {
                    return Err("null value not allowed for object key".to_string());
                }
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&json_string(&text_of(&pair[0])));
                out.push_str(" : ");
                out.push_str(&json_of(&pair[1])?);
            }
            out.push('}');
            text_value(out)
        }
        JsonBuildArrayText => {
            let items: Vec<String> = args.iter().map(json_of).collect::<Result<_, _>>()?;
            text_value(format!("[{}]", items.join(", ")))
        }
        ToJsonText => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            text_value(json_of(&args[0])?)
        }
        ToJson => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            data(crate::core::jsonb::encode_text(&json_of(&args[0])?)?, DbType::JSONB)
        }
        JsonbArrayLength => {
            let document: serde_json::Value = match &args[0] {
                Value::Data { bytes, db_type: DbType::JSONB } => crate::core::jsonb::decode(bytes)?,
                other => serde_json::from_str(&text_of(other)).map_err(|e| format!("invalid input syntax for type json: {e}"))?,
            };
            match document {
                serde_json::Value::Array(items) => int_value(items.len() as i64),
                serde_json::Value::Object(_) => return Err("cannot get array length of a non-array".to_string()),
                _ => return Err("cannot get array length of a scalar".to_string()),
            }
        }
        NumNonnulls => int_value(args.iter().filter(|a| !a.is_null()).count() as i64),
        NumNulls => int_value(args.iter().filter(|a| a.is_null()).count() as i64),
        StringToArray => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            let text = s(0);
            let null_as = args.get(2).filter(|a| !a.is_null()).map(text_of);
            let parts: Vec<String> = if args[1].is_null() {
                text.chars().map(|c| c.to_string()).collect()
            } else if s(1).is_empty() {
                if text.is_empty() { Vec::new() } else { vec![text.clone()] }
            } else if text.is_empty() {
                Vec::new()
            } else {
                text.split(s(1).as_str()).map(str::to_string).collect()
            };
            let elements: Vec<String> = parts
                .iter()
                .map(|p| if null_as.as_deref() == Some(p.as_str()) { "NULL".to_string() } else { array_element(p) })
                .collect();
            let literal = format!("{{{}}}", elements.join(","));
            data(
                crate::core::value_codec::parse_array_literal(&literal).ok_or_else(|| format!("could not build the array {literal}"))?,
                DbType::ARRAY,
            )
        }
        ArrayToString => {
            if args[0].is_null() || args[1].is_null() {
                return Ok(Value::Null);
            }
            let elements = array_elements(&s(0))?;
            let null_as = args.get(2).filter(|a| !a.is_null()).map(text_of);
            let shown: Vec<String> = elements.into_iter().filter_map(|e| e.or_else(|| null_as.clone())).collect();
            text_value(shown.join(&s(1)))
        }
        Cardinality => int_value(array_elements(&s(0))?.len() as i64),
    })
}

/// `numeric_sqrt`: the root at PostgreSQL's result scale, at least sixteen
/// significant digits and never fewer places than the argument shows.
fn numeric_sqrt(n: PgNum) -> Result<PgNum, String> {
    // The argument's weight in base-10000 digits, as `NumericVar.weight`.
    let digits_before = {
        let whole = n.m.unsigned_abs() / 10u128.pow(n.s);
        if whole == 0 { 0 } else { whole.to_string().len() as i64 }
    };
    let weight = if digits_before > 0 {
        (digits_before - 1).div_euclid(4)
    } else {
        // Leading zeros after the point, in groups of four.
        let frac = n.m.unsigned_abs();
        let zeros = n.s as i64 - frac.to_string().len() as i64;
        -1 - zeros.div_euclid(4)
    };
    let sweight = (weight + 1) * 4 / 2 - 1;
    let rscale = (16 - sweight).max(n.s as i64).clamp(0, 1000) as u32;
    n.sqrt_at(rscale).ok_or_else(|| "numeric out of range".to_string())
}

fn quote_literal(text: &str) -> String {
    let escaped = text.replace('\'', "''");
    if text.contains('\\') {
        format!("E'{}'", escaped.replace('\\', "\\\\"))
    } else {
        format!("'{escaped}'")
    }
}

/// `format(fmt, …)`: `%s` (NULL as nothing), `%I` (an identifier, quoted
/// as needed), `%L` (a literal, NULL as `NULL`), `%%`, and `%n$` to take
/// the n-th argument.
fn format_text(args: &[Value]) -> Result<String, String> {
    if args[0].is_null() {
        return Ok(String::new());
    }
    let fmt: Vec<char> = text_of(&args[0]).chars().collect();
    let mut out = String::new();
    let mut next = 1usize;
    let mut i = 0;
    while i < fmt.len() {
        if fmt[i] != '%' {
            out.push(fmt[i]);
            i += 1;
            continue;
        }
        i += 1;
        let Some(&c) = fmt.get(i) else {
            return Err("unterminated format() type specifier".to_string());
        };
        if c == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        let mut position = None;
        let start = i;
        while i < fmt.len() && fmt[i].is_ascii_digit() {
            i += 1;
        }
        if i > start && fmt.get(i) == Some(&'$') {
            position = Some(fmt[start..i].iter().collect::<String>().parse::<usize>().map_err(|e| e.to_string())?);
            i += 1;
        } else {
            i = start;
        }
        let kind = *fmt.get(i).ok_or("unterminated format() type specifier")?;
        i += 1;
        let at = position.unwrap_or(next);
        next = at + 1;
        let arg = args.get(at).ok_or("too few arguments for format()")?;
        match kind {
            's' => {
                if !arg.is_null() {
                    out.push_str(&text_of(arg));
                }
            }
            'I' => {
                if arg.is_null() {
                    return Err("null values cannot be formatted as an SQL identifier".to_string());
                }
                let name = text_of(arg);
                let bare = name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
                    && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                out.push_str(&if bare { name } else { format!("\"{}\"", name.replace('"', "\"\"")) });
            }
            'L' => out.push_str(&if arg.is_null() { "NULL".to_string() } else { quote_literal(&text_of(arg)) }),
            other => return Err(format!("unrecognized format() type specifier \"{other}\"")),
        }
    }
    Ok(out)
}

/// `regexp_replace`: the first match, or every one with flag `g`; `\n` in
/// the replacement is the n-th group and `\&` the whole match. Refused for
/// a pattern whose match boundaries the translated regex may not place
/// where PostgreSQL's longest-match engine does: alternation, a lazy
/// quantifier, or a quantified group.
fn regexp_replace(text: &str, pattern: &str, replacement: &str, flags: &str) -> Result<Value, String> {
    let quantified_group = {
        let b = pattern.as_bytes();
        (0..b.len()).any(|i| b[i] == b')' && matches!(b.get(i + 1), Some(b'*' | b'+' | b'?' | b'{')))
    };
    if pattern.contains('|') || pattern.contains("*?") || pattern.contains("+?") || pattern.contains("??") || quantified_group {
        return Err(format!(
            "regexp_replace with the pattern '{pattern}' is not supported yet: alternation, a lazy quantifier and a \
             quantified group may match differently here"
        ));
    }
    let global = flags.contains('g');
    let compile_flags: String = flags.chars().filter(|c| *c != 'g').collect();
    let re = crate::core::rql::pg_regex::compile_cached(pattern, &compile_flags).map_err(|e| e.message())?;
    match re.replace(text, replacement, global) {
        Some(out) => Ok(text_value(out)),
        None => Err(format!("regexp_replace over non-ASCII text with the pattern '{pattern}' is not supported yet")),
    }
}

/// A JSON value's text for `v`, as `to_json` writes it.
fn json_of(v: &Value) -> Result<String, String> {
    Ok(match v {
        Value::Null => "null".to_string(),
        Value::Data { bytes, db_type } => match db_type {
            DbType::BOOL => if bytes.first().is_some_and(|b| *b != 0) { "true" } else { "false" }.to_string(),
            // A `json` argument, marked by the caller: its text as written.
            DbType::JSONTEXT => String::from_utf8_lossy(bytes).into_owned(),
            DbType::JSONB => {
                let mut out = Vec::new();
                crate::core::jsonb::render_text_into(bytes, &mut out)?;
                String::from_utf8(out).map_err(|e| e.to_string())?
            }
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
            | DbType::I128 | DbType::U128 | DbType::DECIMAL => text_of(v),
            DbType::F32 | DbType::F64 => {
                let t = text_of(v);
                if t.parse::<f64>().is_ok_and(f64::is_finite) { t } else { json_string(&t) }
            }
            DbType::DATETIME | DbType::TIMESTAMP => json_string(&text_of(v).replacen(' ', "T", 1)),
            _ => json_string(&text_of(v)),
        },
    })
}

fn json_string(text: &str) -> String {
    serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string())
}

/// One element of an array's text as `array_out` writes it.
fn array_element(text: &str) -> String {
    let quote = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text.chars().any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace());
    if !quote {
        return text.to_string();
    }
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// [`array_elements`], for the table functions (`unnest`).
pub fn array_elements_of(text: &str) -> Result<Vec<Option<String>>, String> {
    array_elements(text)
}

/// The elements of a one-dimensional array's text, `None` for a NULL.
fn array_elements(text: &str) -> Result<Vec<Option<String>>, String> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .ok_or_else(|| format!("malformed array literal: \"{text}\""))?;
    let mut out = Vec::new();
    if inner.trim().is_empty() {
        return Ok(out);
    }
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0;
    loop {
        while i < chars.len() && chars[i] == ' ' {
            i += 1;
        }
        if chars.get(i) == Some(&'{') {
            return Err("a multidimensional array is not supported here".to_string());
        }
        if chars.get(i) == Some(&'"') {
            i += 1;
            let mut e = String::new();
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                }
                if let Some(c) = chars.get(i) {
                    e.push(*c);
                }
                i += 1;
            }
            i += 1;
            out.push(Some(e));
        } else {
            let start = i;
            while i < chars.len() && chars[i] != ',' {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect::<String>().trim().to_string();
            out.push(if raw.eq_ignore_ascii_case("NULL") { None } else { Some(raw) });
        }
        while i < chars.len() && chars[i] != ',' {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        i += 1;
    }
    Ok(out)
}

/// Days since 1970-01-01 of a calendar date, refusing one that is not.
fn days_of(y: i64, m: i64, d: i64) -> Result<i64, String> {
    let ok = (1..=12).contains(&m) && d >= 1 && d <= crate::core::value_codec::days_in_month(y, m as u32) as i64;
    if !ok || y == 0 {
        return Err(format!("date field value out of range: {y}-{m:02}-{d:02}"));
    }
    Ok(crate::core::value_codec::days_from_civil(y, m as u32, d as u32))
}

fn date_bytes(y: i64, m: i64, d: i64) -> Result<Vec<u8>, String> {
    let days = days_of(y, m, d)? - crate::core::pg_types::PG_EPOCH_DAYS as i64;
    Ok(i32::try_from(days).map_err(|_| "date out of range")?.to_be_bytes().to_vec())
}

/// A timestamp or date argument as microseconds since 1970.
fn timestamp_of(v: &Value) -> Result<i64, String> {
    match v {
        Value::Data { bytes, db_type: DbType::DATE } => {
            let days = i32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| "a date could not be read")?);
            Ok((days as i64 + crate::core::pg_types::PG_EPOCH_DAYS as i64) * 86_400_000_000)
        }
        Value::Data { bytes, db_type: DbType::DATETIME | DbType::TIMESTAMP } => {
            Ok(i64::from_le_bytes(bytes.as_slice().try_into().map_err(|_| "a timestamp could not be read")?))
        }
        other => crate::core::value_codec::parse_timestamp_local_us(&text_of(other))
            .ok_or_else(|| format!("invalid input syntax for type timestamp: \"{}\"", text_of(other))),
    }
}

struct Fields {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
}

fn fields_of(us: i64) -> Fields {
    let secs = us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = crate::core::value_codec::civil_from_days(days);
    Fields {
        year: y,
        month: m as i64,
        day: d as i64,
        hour: tod / 3600,
        minute: tod % 3600 / 60,
        second: tod % 60,
        micros: us.rem_euclid(1_000_000),
    }
}

/// `age(later, earlier)`, as PostgreSQL's `timestamp_age`: the difference
/// field by field, each negative field borrowed from the next, a negative
/// day count from the month the borrow crosses.
fn age(later: i64, earlier: i64) -> crate::core::pg_types::Interval {
    let (t1, t2) = (fields_of(later), fields_of(earlier));
    let mut f = [
        t1.micros - t2.micros,
        t1.second - t2.second,
        t1.minute - t2.minute,
        t1.hour - t2.hour,
        t1.day - t2.day,
        t1.month - t2.month,
        t1.year - t2.year,
    ];
    let flip = later < earlier;
    if flip {
        for v in f.iter_mut() {
            *v = -*v;
        }
    }
    while f[0] < 0 {
        f[0] += 1_000_000;
        f[1] -= 1;
    }
    while f[1] < 0 {
        f[1] += 60;
        f[2] -= 1;
    }
    while f[2] < 0 {
        f[2] += 60;
        f[3] -= 1;
    }
    while f[3] < 0 {
        f[3] += 24;
        f[4] -= 1;
    }
    while f[4] < 0 {
        let (y, m) = if flip { (t1.year, t1.month) } else { (t2.year, t2.month) };
        f[4] += crate::core::value_codec::days_in_month(y, m as u32) as i64;
        f[5] -= 1;
    }
    while f[5] < 0 {
        f[5] += 12;
        f[6] -= 1;
    }
    if flip {
        for v in f.iter_mut() {
            *v = -*v;
        }
    }
    crate::core::pg_types::Interval {
        months: (f[6] * 12 + f[5]) as i32,
        days: f[4] as i32,
        micros: ((f[3] * 60 + f[2]) * 60 + f[1]) * 1_000_000 + f[0],
    }
}

const MONTHS: [&str; 12] =
    ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const DAYS: [&str; 7] = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/// A template's patterns, longest first so `HH24` is not read as `HH`.
const PATTERNS: &[&str] = &[
    "HH24", "HH12", "YYYY", "MONTH", "Month", "month", "A.M.", "P.M.", "a.m.", "p.m.", "DDD", "YYY", "MON", "Mon", "mon",
    "DAY", "Day", "day", "HH", "MI", "SS", "MS", "US", "AM", "PM", "am", "pm", "YY", "MM", "DD", "DY", "Dy", "dy", "Y",
    "D", "Q",
];

enum Piece {
    Pattern { name: &'static str, fill: bool },
    Literal(String),
}

fn template(fmt: &str) -> Vec<Piece> {
    let mut out = Vec::new();
    let mut rest = fmt;
    let mut fill = false;
    while !rest.is_empty() {
        if rest.len() >= 2 && rest[..2].eq_ignore_ascii_case("FM") {
            fill = true;
            rest = &rest[2..];
            continue;
        }
        if let Some(stripped) = rest.strip_prefix('"') {
            let end = stripped.find('"').unwrap_or(stripped.len());
            out.push(Piece::Literal(stripped[..end].to_string()));
            rest = stripped.get(end + 1..).unwrap_or("");
            continue;
        }
        if let Some(name) = PATTERNS.iter().find(|p| rest.starts_with(**p)) {
            out.push(Piece::Pattern { name, fill });
            fill = false;
            rest = &rest[name.len()..];
            continue;
        }
        let c = rest.chars().next().expect("non-empty");
        out.push(Piece::Literal(c.to_string()));
        rest = &rest[c.len_utf8()..];
    }
    out
}

fn cased(name: &str, word: &str) -> String {
    match name {
        n if n.chars().all(|c| c.is_ascii_uppercase()) => word.to_ascii_uppercase(),
        n if n.chars().all(|c| c.is_ascii_lowercase()) => word.to_ascii_lowercase(),
        _ => word.to_string(),
    }
}

/// `to_char(value, template)`: a date or timestamp by its fields, a number
/// by the digit template.
fn to_char(value: &Value, fmt: &str) -> Result<String, String> {
    match value {
        Value::Data { db_type: DbType::DATE | DbType::DATETIME | DbType::TIMESTAMP, .. } => {}
        Value::Data { db_type: DbType::INTERVAL, .. } => return Err("to_char of an interval is not supported yet".to_string()),
        _ => return to_char_number(value, fmt),
    }
    let us = timestamp_of(value)?;
    let f = fields_of(us);
    let days = us.div_euclid(86_400_000_000);
    let weekday = (days + 4).rem_euclid(7) as usize;
    let mut out = String::new();
    for piece in template(fmt) {
        let (name, fill) = match piece {
            Piece::Literal(text) => {
                out.push_str(&text);
                continue;
            }
            Piece::Pattern { name, fill } => (name, fill),
        };
        let num = |v: i64, width: usize| if fill { v.to_string() } else { format!("{v:0width$}") };
        let word = |w: &str, width: usize| {
            let w = cased(name, w);
            if fill { w } else { format!("{w:<width$}") }
        };
        let hour12 = if f.hour % 12 == 0 { 12 } else { f.hour % 12 };
        out.push_str(&match name {
            "YYYY" => num(f.year, 4),
            "YYY" => num(f.year % 1000, 3),
            "YY" => num(f.year % 100, 2),
            "Y" => num(f.year % 10, 1),
            "MM" => num(f.month, 2),
            "DD" => num(f.day, 2),
            "DDD" => num(days - crate::core::value_codec::days_from_civil(f.year, 1, 1) + 1, 3),
            "D" => (weekday + 1).to_string(),
            "Q" => ((f.month - 1) / 3 + 1).to_string(),
            "HH24" => num(f.hour, 2),
            "HH12" | "HH" => num(hour12, 2),
            "MI" => num(f.minute, 2),
            "SS" => num(f.second, 2),
            "MS" => num(f.micros / 1000, 3),
            "US" => num(f.micros, 6),
            "AM" | "PM" => (if f.hour < 12 { "AM" } else { "PM" }).to_string(),
            "am" | "pm" => (if f.hour < 12 { "am" } else { "pm" }).to_string(),
            "A.M." | "P.M." => (if f.hour < 12 { "A.M." } else { "P.M." }).to_string(),
            "a.m." | "p.m." => (if f.hour < 12 { "a.m." } else { "p.m." }).to_string(),
            "MONTH" | "Month" | "month" => word(MONTHS[f.month as usize - 1], 9),
            "MON" | "Mon" | "mon" => cased(name, &MONTHS[f.month as usize - 1][..3]),
            "DAY" | "Day" | "day" => word(DAYS[weekday], 9),
            "DY" | "Dy" | "dy" => cased(name, &DAYS[weekday][..3]),
            _ => String::new(),
        });
    }
    Ok(out)
}

/// `to_char(number, template)` over the digit patterns `9` (a digit, a
/// leading zero shown as a space) and `0` (a digit always), `.`, `,`, and
/// `FM` (no padding, no trailing zeros a `9` holds). The value is rounded
/// half away from zero to the template's places; one too wide for it is
/// `#` in every digit place, as PostgreSQL shows it.
fn to_char_number(value: &Value, fmt: &str) -> Result<String, String> {
    let fill = fmt.to_ascii_uppercase().contains("FM");
    let body: String = {
        let mut b = fmt.to_string();
        while let Some(at) = b.to_ascii_uppercase().find("FM") {
            b.replace_range(at..at + 2, "");
        }
        b
    };
    if body.chars().any(|c| !matches!(c, '9' | '0' | '.' | ',' | 'D' | 'G')) {
        return Err(format!("to_char with the template '{fmt}' is not supported yet"));
    }
    let body = body.replace('D', ".").replace('G', ",");
    let (int_part, frac_part) = match body.find('.') {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body.as_str(), None),
    };
    let places = frac_part.map_or(0, |f| f.chars().filter(|c| matches!(c, '9' | '0')).count()) as u32;
    let n = numeric_of(value)?.round_to(places).ok_or("numeric out of range")?;
    let negative = n.m < 0;
    let units = n.m.unsigned_abs();
    let whole = (units / 10u128.pow(places)).to_string();
    let frac_digits = format!("{:0width$}", units % 10u128.pow(places), width = places as usize);
    let int_slots = int_part.chars().filter(|c| matches!(c, '9' | '0')).count();
    let whole_digits: Vec<char> = if whole == "0" { Vec::new() } else { whole.chars().collect() };
    if whole_digits.len() > int_slots {
        let hashes: String = body.chars().map(|c| if matches!(c, '9' | '0') { '#' } else { c }).collect();
        return Ok(format!("{}{}", if fill { "" } else { " " }, hashes));
    }
    let mut int_out = String::new();
    let mut started = false;
    let mut slot = 0usize;
    let first_zero = int_part.chars().filter(|c| matches!(c, '9' | '0')).position(|c| c == '0');
    for c in int_part.chars() {
        match c {
            '9' | '0' => {
                let digit_at = slot as isize - (int_slots - whole_digits.len()) as isize;
                let forced = first_zero.is_some_and(|z| slot >= z);
                if digit_at >= 0 {
                    int_out.push(whole_digits[digit_at as usize]);
                    started = true;
                } else if forced || (c == '0') {
                    int_out.push('0');
                    started = true;
                } else if slot + 1 == int_slots && places == 0 {
                    // A value of zero with no fraction still shows one digit.
                    int_out.push('0');
                    started = true;
                } else if !fill {
                    int_out.push(' ');
                }
                slot += 1;
            }
            ',' => {
                if started {
                    int_out.push(',');
                } else if !fill {
                    int_out.push(' ');
                }
            }
            _ => {}
        }
    }
    let mut frac_out = String::new();
    if let Some(frac) = frac_part {
        let pattern: Vec<char> = frac.chars().filter(|c| matches!(c, '9' | '0')).collect();
        let digits: Vec<char> = frac_digits.chars().collect();
        let mut keep = digits.len();
        if fill {
            while keep > 0 && pattern[keep - 1] == '9' && digits[keep - 1] == '0' {
                keep -= 1;
            }
        }
        frac_out = digits[..keep].iter().collect();
    }
    // The sign sits just before the first character shown.
    let lead = int_out.len() - int_out.trim_start().len();
    let sign = if negative { "-" } else if fill { "" } else { " " };
    let mut out = format!("{}{}{}", &int_out[..lead], sign, &int_out[lead..]);
    if frac_part.is_some() {
        out.push('.');
        out.push_str(&frac_out);
    }
    Ok(out)
}

struct Parsed {
    year: i64,
    month: i64,
    day: i64,
    micros_of_day: i64,
}

/// `to_date`/`to_timestamp(text, template)`: the template's fields read
/// from the text, every other template character skipping one character
/// of it, as PostgreSQL's lenient reading does.
fn parse_with_template(text: &str, fmt: &str) -> Result<Parsed, String> {
    let mut p = Parsed { year: 1, month: 1, day: 1, micros_of_day: 0 };
    let (mut hour, mut minute, mut second, mut frac, mut pm) = (0i64, 0i64, 0i64, 0i64, None::<bool>);
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    let bad = |what: &str| format!("invalid value \"{}\" for \"{what}\"", text);
    for piece in template(fmt) {
        match piece {
            Piece::Literal(lit) => {
                for _ in lit.chars() {
                    if i < chars.len() && !chars[i].is_alphanumeric() {
                        i += 1;
                    }
                }
            }
            Piece::Pattern { name, .. } => {
                while i < chars.len() && chars[i] == ' ' {
                    i += 1;
                }
                let mut read = |max: usize, what: &str| -> Result<i64, String> {
                    let start = i;
                    if chars.get(i) == Some(&'-') {
                        i += 1;
                    }
                    while i < chars.len() && i - start < max && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    chars[start..i].iter().collect::<String>().parse::<i64>().map_err(|_| bad(what))
                };
                match name {
                    "YYYY" => p.year = read(4, name)?,
                    "YY" => p.year = 2000 + read(2, name)?,
                    "MM" => p.month = read(2, name)?,
                    "DD" => p.day = read(2, name)?,
                    "HH24" | "HH12" | "HH" => hour = read(2, name)?,
                    "MI" => minute = read(2, name)?,
                    "SS" => second = read(2, name)?,
                    "MS" => frac = read(3, name)? * 1000,
                    "US" => frac = read(6, name)?,
                    "AM" | "PM" | "am" | "pm" => {
                        let word: String = chars.iter().skip(i).take(2).collect();
                        pm = Some(word.eq_ignore_ascii_case("PM"));
                        i += 2;
                    }
                    "MON" | "Mon" | "mon" | "MONTH" | "Month" | "month" => {
                        let rest: String = chars[i..].iter().collect::<String>().to_ascii_lowercase();
                        let at = MONTHS
                            .iter()
                            .position(|m| rest.starts_with(&m.to_ascii_lowercase()[..3]))
                            .ok_or_else(|| bad(name))?;
                        p.month = at as i64 + 1;
                        let full = MONTHS[at].to_ascii_lowercase();
                        i += if name.len() > 3 && rest.starts_with(&full) { full.len() } else { 3 };
                    }
                    other => return Err(format!("the template pattern \"{other}\" is not supported in to_date/to_timestamp yet")),
                }
            }
        }
    }
    if let Some(pm) = pm {
        hour = hour % 12 + if pm { 12 } else { 0 };
    }
    if !(0..24).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
        return Err(format!("date/time field value out of range: \"{text}\""));
    }
    p.micros_of_day = ((hour * 60 + minute) * 60 + second) * 1_000_000 + frac;
    days_of(p.year, p.month, p.day)?;
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Value {
        text_value(s.to_string())
    }
    fn i(v: i64) -> Value {
        int_value(v)
    }
    fn shown(v: Value) -> String {
        text_of(&v)
    }

    #[test]
    fn text_functions_answer_as_postgresql() {
        assert_eq!(shown(call(PgFunc::Left, &[t("Hello World"), i(-2)]).unwrap()), "Hello Wor");
        assert_eq!(shown(call(PgFunc::Right, &[t("Hello World"), i(-2)]).unwrap()), "llo World");
        assert_eq!(shown(call(PgFunc::Substr, &[t("Hello World"), i(-1), i(4)]).unwrap()), "He");
        assert_eq!(shown(call(PgFunc::SplitPart, &[t("a,b,c"), t(","), i(-1)]).unwrap()), "c");
        assert_eq!(shown(call(PgFunc::Translate, &[t("abcabc"), t("ab"), t("x")]).unwrap()), "xcxc");
        assert_eq!(shown(call(PgFunc::Initcap, &[t("hello wORLD foo-bar 3rd")]).unwrap()), "Hello World Foo-Bar 3rd");
        assert_eq!(shown(call(PgFunc::Md5, &[t("")]).unwrap()), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(shown(call(PgFunc::ToHex, &[i(-1)]).unwrap()), "ffffffff");
        assert_eq!(
            shown(call(PgFunc::Format, &[t("%I.%L|%2$s"), t("my table"), t("it's")]).unwrap()),
            "\"my table\".'it''s'|it's"
        );
        assert!(call(PgFunc::Left, &[Value::Null, i(1)]).unwrap().is_null());
        assert_eq!(shown(call(PgFunc::Concat, &[t("a"), Value::Null, i(1)]).unwrap()), "a1");
    }

    #[test]
    fn standard_spellings_become_calls() {
        assert_eq!(standard_syntax("SUBSTRING", "s from 2 for 3"), Some(("SUBSTR".into(), "s, 2, 3".into())));
        assert_eq!(standard_syntax("SUBSTRING", "s for 4"), Some(("SUBSTR".into(), "s, 1, 4".into())));
        assert_eq!(standard_syntax("SUBSTRING", "s, 2"), None);
        assert_eq!(standard_syntax("POSITION", "'o' in s"), Some(("STRPOS".into(), "s, 'o'".into())));
        assert_eq!(standard_syntax("TRIM", "leading 'x' from s"), Some(("LTRIM".into(), "s, 'x'".into())));
        assert_eq!(standard_syntax("TRIM", "trailing from s"), Some(("RTRIM".into(), "s".into())));
        assert_eq!(standard_syntax("TRIM", "s"), Some(("BTRIM".into(), "s".into())));
    }

    #[test]
    fn age_borrows_as_postgresql_does() {
        let us = |y, m, d| crate::core::value_codec::days_from_civil(y, m, d) * 86_400_000_000;
        let a = age(us(2024, 3, 1), us(2024, 1, 15) + 10 * 3_600_000_000);
        assert_eq!((a.months, a.days, a.micros), (1, 16, 14 * 3_600_000_000));
        let b = age(us(2024, 1, 31), us(2023, 2, 28));
        assert_eq!((b.months, b.days, b.micros), (11, 3, 0));
    }
}
