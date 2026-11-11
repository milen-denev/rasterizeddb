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
    /// `ARRAY[e, …]`, the array constructor (`expr::parse` reads it; no
    /// function of this name can be called).
    ArrayCtor,
    /// `a[i]` (`expr::parse` reads it): its arguments are the array, the
    /// index, and the element type's byte code as a one-byte string (empty
    /// for text), fixed when it was parsed so the value is of the type the
    /// statement was described with.
    ArraySubscript,
    /// `a[lo:hi]`, either bound NULL when it was left out.
    ArraySlice,
    /// `x::T[]` / `CAST(x AS T[])` (`expr::parse` reads it): the value and
    /// the element type's byte code as a one-byte string (empty for text),
    /// as [`ArraySubscript`](Self::ArraySubscript) carries it. The value's
    /// text is read as an array literal (an array's own text when it is one)
    /// and each element as the element type reads it, as a typed column
    /// holds them.
    ArrayCast,
    /// `a & b`, `a | b`, `a # b`, `a << n`, `a >> n` and `~a` (`expr::parse`
    /// reads them): over integers, as the wider operand's type (a shift as
    /// its left operand's), and over bit strings of one length, as `bit`.
    BitAnd,
    BitOr,
    BitXor,
    ShiftLeft,
    ShiftRight,
    BitNot,
    /// `@>`, `<@`, `&&`, `?`, `?|`, `?&`, `<<=`, `>>=` over the values a row
    /// computes (`expr::parse`): the left, the right and the symbol, answered
    /// by `pg_ops::evaluate` as a `WHERE` between a column and a value is.
    StructOp,
    /// `x op ANY (array)` / `ALL`, over an array the row computes: the
    /// value, the array, the operator and `ANY` or `ALL`, as text
    /// (`expr::quantified_condition`). TRUE, FALSE, or NULL when no element
    /// decided it and one was unknown, as PostgreSQL's quantifiers are.
    QuantifiedCmp,
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
    WidthBucket,
    Factorial,
    JustifyHours,
    JustifyDays,
    JustifyInterval,
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
    "LOG", "LOG10", "PI", "DEGREES", "RADIANS", "RANDOM", "DIV", "GCD", "LCM", "WIDTH_BUCKET", "FACTORIAL", "JUSTIFY_HOURS", "JUSTIFY_DAYS", "JUSTIFY_INTERVAL", "MAKE_DATE", "MAKE_TIMESTAMP",
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
        "WIDTH_BUCKET" => (WidthBucket, &[4]),
        "FACTORIAL" => (Factorial, &[1]),
        "JUSTIFY_HOURS" => (JustifyHours, &[1]),
        "JUSTIFY_DAYS" => (JustifyDays, &[1]),
        "JUSTIFY_INTERVAL" => (JustifyInterval, &[1]),
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
        Strpos | Ascii | JsonbArrayLength | NumNonnulls | NumNulls | Cardinality | WidthBucket => DbType::I32,
        Factorial => DbType::DECIMAL,
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
        Age | JustifyHours | JustifyDays | JustifyInterval => DbType::INTERVAL,
        Decode => DbType::BYTEA,
        JsonBuildObject | JsonBuildArray | ToJson => DbType::JSONB,
        JsonBuildObjectText | JsonBuildArrayText | ToJsonText => DbType::JSONTEXT,
        StringToArray | ArrayCtor | ArraySlice | ArrayCast => DbType::ARRAY,
        BitAnd | BitOr | BitXor | ShiftLeft | ShiftRight | BitNot => bitwise_type(func, args),
        StructOp => DbType::BOOL,
        QuantifiedCmp => DbType::BOOL,
        ArraySubscript => subscript_type(args),
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

/// The integer type PostgreSQL resolves a bitwise operand of `t` to: its
/// own for `smallint`, `integer` and `bigint`, the engine's narrower ones
/// widened, `None` for anything else.
fn bitwise_integer(t: &DbType) -> Option<DbType> {
    match t {
        DbType::I16 | DbType::I32 | DbType::I64 => Some(t.clone()),
        DbType::I8 | DbType::U8 => Some(DbType::I16),
        DbType::U16 => Some(DbType::I32),
        DbType::U32 => Some(DbType::I64),
        _ => None,
    }
}

fn integer_width(t: &DbType) -> u8 {
    match t {
        DbType::I16 => 2,
        DbType::I32 => 4,
        _ => 8,
    }
}

/// A bitwise operator's result type from its operands' static types.
fn bitwise_type(func: PgFunc, args: &[Expr]) -> DbType {
    let types: Vec<DbType> = args.iter().map(crate::core::rql::expr::static_type).collect();
    // `<<` / `>>` between networks: containment.
    if types.iter().any(|t| *t == DbType::INET) {
        return DbType::BOOL;
    }
    if types.iter().any(|t| matches!(t, DbType::BIT | DbType::VARBIT)) {
        return DbType::BIT;
    }
    let ints: Vec<DbType> = types.iter().filter_map(bitwise_integer).collect();
    match func {
        PgFunc::ShiftLeft | PgFunc::ShiftRight | PgFunc::BitNot => ints.first().cloned().unwrap_or(DbType::I32),
        _ => ints.into_iter().max_by_key(integer_width).unwrap_or(DbType::I32),
    }
}

fn integer_of(v: i64, t: &DbType) -> Value {
    match t {
        DbType::I16 => data((v as i16).to_le_bytes().to_vec(), DbType::I16),
        DbType::I32 => data((v as i32).to_le_bytes().to_vec(), DbType::I32),
        _ => data(v.to_le_bytes().to_vec(), DbType::I64),
    }
}

/// An operand's integer and the type it is read as. An untyped string
/// literal is read as an integer, as PostgreSQL resolves `'5' & 1`.
fn bitwise_operand(v: &Value, op: &str, other: Option<&Value>) -> Result<(i64, DbType), String> {
    let t = v.db_type().cloned().unwrap_or(DbType::NULL);
    if let Some(int) = bitwise_integer(&t) {
        return Ok((crate::core::rql::expr::as_i64(v).ok_or("an integer could not be read")?, int));
    }
    let name = |v: &Value| v.db_type().map_or("unknown", crate::core::value_codec::pg_input_type_name);
    Err(match other {
        Some(o) => format!("operator does not exist: {} {op} {}", name(v), name(o)),
        None => format!("operator does not exist: {op} {}", name(v)),
    })
}

/// A bit string's length and its bits, MSB first.
fn bits_of(bytes: &[u8]) -> Option<(usize, Vec<bool>)> {
    let len = usize::try_from(i32::from_be_bytes(bytes.get(..4)?.try_into().ok()?)).ok()?;
    let packed = bytes.get(4..)?;
    let bits = (0..len).map(|i| packed.get(i / 8).is_some_and(|b| (b >> (7 - i % 8)) & 1 == 1)).collect();
    Some((len, bits))
}

fn bit_value(bits: &[bool]) -> Value {
    let mut out = (bits.len() as i32).to_be_bytes().to_vec();
    out.extend(bits.chunks(8).map(|c| c.iter().enumerate().fold(0u8, |b, (i, &on)| b | (u8::from(on) << (7 - i)))));
    data(out, DbType::BIT)
}

fn is_bit(v: &Value) -> bool {
    matches!(v, Value::Data { db_type: DbType::BIT | DbType::VARBIT, .. })
}

/// A structural operator (`PgFunc::StructOp`) over its evaluated operands.
fn structural(args: &[Value]) -> Result<Value, String> {
    use crate::core::pg_ops::PgOp;
    let symbol = text_of(&args[2]);
    let op = PgOp::from_symbol(&symbol).ok_or_else(|| format!("operator {symbol} is not supported yet"))?;
    let (Value::Data { bytes: left, db_type: left_type }, Value::Data { bytes: right, db_type: right_type }) = (&args[0], &args[1])
    else {
        return Ok(Value::Null);
    };
    let name = |t: &DbType| crate::core::value_codec::pg_input_type_name(t);
    // `element <@ container` (`3 <@ int4range '[1,5)'`, `point <@ box`) is
    // the container's `@>` of the element.
    if op == PgOp::ContainedBy && !op.applies_to(left_type) && op.applies_to(right_type) {
        let swapped = [args[1].clone(), args[0].clone(), text_value("@>".to_string())];
        return structural(&swapped);
    }
    if !op.applies_to(left_type) {
        return Err(format!("operator does not exist: {} {symbol} {}", name(left_type), name(right_type)));
    }
    // A key list as an array (`?| ARRAY['a', 'b']`): its text, the form the
    // key reader takes.
    let array_text;
    let (right, right_type): (&[u8], DbType) = if *right_type == DbType::ARRAY && op.takes_text_operand() {
        array_text = crate::core::value_codec::format_array(right).unwrap_or_default().into_bytes();
        (&array_text, DbType::STRING)
    } else {
        (right, right_type.clone())
    };
    // A text right operand at the left's type, as the planner reads one
    // (`doc @> '{"a":1}'` is a document, `tags @> '{gift}'` an array).
    let encoded;
    let (right, right_type): (&[u8], DbType) = match (&right_type, op.takes_text_operand()) {
        (DbType::STRING, false) if *left_type != DbType::STRING => {
            let text = String::from_utf8_lossy(right).into_owned();
            match crate::core::value_codec::encode_literal(left_type, &text) {
                Some(bytes) => {
                    encoded = bytes;
                    (&encoded, left_type.clone())
                }
                None if *left_type == DbType::JSONB => {
                    return Err(format!("invalid input syntax for type json: {}", crate::core::value_codec::quote_sql(&text)));
                }
                None => (right, right_type),
            }
        }
        _ => (right, right_type),
    };
    crate::core::pg_ops::evaluate(op, left_type, left, &right_type, right)
        .map(|v| data(vec![u8::from(v)], DbType::BOOL))
        .ok_or_else(|| format!("operator does not exist: {} {symbol} {}", name(left_type), name(&right_type)))
}

/// `&`, `|`, `#`, `<<`, `>>` and `~` over their evaluated operands.
fn bitwise(func: PgFunc, args: &[Value]) -> Result<Value, String> {
    use PgFunc::*;
    let symbol = match func {
        BitAnd => "&",
        BitOr => "|",
        BitXor => "#",
        ShiftLeft => "<<",
        ShiftRight => ">>",
        _ => "~",
    };
    if func == BitNot {
        if let Value::Data { bytes, .. } = &args[0]
            && is_bit(&args[0])
        {
            let (_, bits) = bits_of(bytes).ok_or("could not read a bit string")?;
            return Ok(bit_value(&bits.iter().map(|b| !b).collect::<Vec<_>>()));
        }
        let (v, t) = bitwise_operand(&args[0], symbol, None)?;
        return Ok(integer_of(!v, &t));
    }
    let (a, b) = (&args[0], &args[1]);
    // `<<` / `>>` between networks are containment, the operators PostgreSQL
    // spells the same (a `WHERE` reads them as such between a column and a
    // value; a group reaches them here).
    if let (Value::Data { bytes: l, db_type: DbType::INET }, Value::Data { bytes: r, db_type: DbType::INET }) = (a, b)
        && matches!(func, ShiftLeft | ShiftRight)
    {
        let op = if func == ShiftLeft {
            crate::core::pg_ops::PgOp::NetContainedBy
        } else {
            crate::core::pg_ops::PgOp::NetContains
        };
        return crate::core::pg_ops::evaluate(op, &DbType::INET, l, &DbType::INET, r)
            .map(|v| data(vec![u8::from(v)], DbType::BOOL))
            .ok_or_else(|| "could not compare the networks".to_string());
    }
    let name = |v: &Value| v.db_type().map_or("unknown", crate::core::value_codec::pg_input_type_name);
    let mismatch = || Err(format!("operator does not exist: {} {symbol} {}", name(a), name(b)));
    // A bit string with anything but a bit string (or a shift's count).
    if is_bit(a) != is_bit(b) && !(is_bit(a) && matches!(func, ShiftLeft | ShiftRight)) {
        return mismatch();
    }
    if let (Value::Data { bytes, .. }, true) = (a, is_bit(a)) {
        let (len, bits) = bits_of(bytes).ok_or("could not read a bit string")?;
        if matches!(func, ShiftLeft | ShiftRight) {
            let (n, _) = bitwise_operand(b, symbol, Some(a))?;
            // A negative shift goes the other way, as PostgreSQL's does.
            let left = (func == ShiftLeft) == (n >= 0);
            let n = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX).min(len);
            let shifted: Vec<bool> = if left {
                bits[n..].iter().copied().chain(std::iter::repeat_n(false, n)).collect()
            } else {
                std::iter::repeat_n(false, n).chain(bits[..len - n].iter().copied()).collect()
            };
            return Ok(bit_value(&shifted));
        }
        let Value::Data { bytes: other, .. } = b else { return Ok(Value::Null) };
        let (other_len, other_bits) = bits_of(other).ok_or("could not read a bit string")?;
        if other_len != len {
            let verb = match func {
                BitAnd => "AND",
                BitOr => "OR",
                _ => "XOR",
            };
            return Err(format!("cannot {verb} bit strings of different sizes"));
        }
        let combined: Vec<bool> = bits
            .iter()
            .zip(&other_bits)
            .map(|(x, y)| match func {
                BitAnd => *x && *y,
                BitOr => *x || *y,
                _ => x != y,
            })
            .collect();
        return Ok(bit_value(&combined));
    }
    let (x, xt) = bitwise_operand(a, symbol, Some(b))?;
    let (y, yt) = bitwise_operand(b, symbol, Some(a))?;
    Ok(match func {
        // A shift keeps its left operand's type and width; past the width it
        // wraps as the C shift PostgreSQL compiles to does on x86.
        ShiftLeft | ShiftRight => {
            let n = y as u32;
            let v = match (&xt, func) {
                (DbType::I16, ShiftLeft) => i64::from((x as i16).wrapping_shl(n)),
                (DbType::I16, _) => i64::from((x as i16).wrapping_shr(n)),
                (DbType::I32, ShiftLeft) => i64::from((x as i32).wrapping_shl(n)),
                (DbType::I32, _) => i64::from((x as i32).wrapping_shr(n)),
                (_, ShiftLeft) => x.wrapping_shl(n),
                _ => x.wrapping_shr(n),
            };
            integer_of(v, &xt)
        }
        _ => {
            let t = if integer_width(&xt) >= integer_width(&yt) { xt } else { yt };
            let v = match func {
                BitAnd => x & y,
                BitOr => x | y,
                _ => x ^ y,
            };
            integer_of(v, &t)
        }
    })
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
    let non_strict = matches!(func, ArrayCtor | ArraySlice | QuantifiedCmp | Concat | ConcatWs | Format | JsonBuildObject | JsonBuildArray | JsonBuildObjectText | JsonBuildArrayText | NumNonnulls | NumNulls | ToJson | ToJsonText | StringToArray | ArrayToString);
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
        // PostgreSQL's `width_bucket(operand, low, high, count)`: 0 below the
        // range, `count + 1` at or past its end, otherwise which of `count`
        // equal buckets holds the operand, exactly (each value brought to
        // one scale, the bucket an integer division of their differences).
        WidthBucket => {
            let count = int_of(&args[3], "the count")?;
            if count <= 0 {
                return Err("count must be greater than zero".to_string());
            }
            let (x, lo, hi) = (numeric_of(&args[0])?, numeric_of(&args[1])?, numeric_of(&args[2])?);
            let scale = x.s.max(lo.s).max(hi.s);
            let at = |n: PgNum| n.m.checked_mul(10i128.checked_pow(scale - n.s)?);
            let (Some(x), Some(lo), Some(hi)) = (at(x), at(lo), at(hi)) else {
                return Err("numeric out of range".to_string());
            };
            if lo == hi {
                return Err("lower bound cannot equal upper bound".to_string());
            }
            let (offset, width, past) = if lo < hi {
                (x.checked_sub(lo), hi - lo, x >= hi)
            } else {
                (lo.checked_sub(x), lo - hi, x <= hi)
            };
            let below = if lo < hi { x < lo } else { x > lo };
            let bucket = if below {
                0
            } else if past {
                count + 1
            } else {
                let offset = offset.ok_or("numeric out of range")?;
                let scaled = offset.checked_mul(count as i128).ok_or("numeric out of range")?;
                (scaled / width) as i64 + 1
            };
            int_value(i32::try_from(bucket).map_err(|_| "integer out of range")? as i64)
        }
        // `timestamp.c`'s three: whole days out of the time, whole months
        // out of the days (thirty each), then the signs made to agree.
        JustifyHours | JustifyDays | JustifyInterval => {
            let Value::Data { bytes, db_type: DbType::INTERVAL } = &args[0] else {
                return Err(format!("function {}(unknown) does not exist", format!("{func:?}").to_lowercase()));
            };
            let iv = crate::core::pg_types::decode_interval(bytes).ok_or("an interval could not be read")?;
            let (mut month, mut day, mut time) = (iv.months as i64, iv.days as i64, iv.micros);
            const DAY: i64 = 86_400_000_000;
            if func != JustifyDays {
                let whole = time / DAY;
                time -= whole * DAY;
                day += whole;
            }
            if func != JustifyHours {
                let whole = day / 30;
                day -= whole * 30;
                month += whole;
                if month > 0 && (day < 0 || (func == JustifyInterval && day == 0 && time < 0)) {
                    day += 30;
                    month -= 1;
                } else if month < 0 && (day > 0 || (func == JustifyInterval && day == 0 && time > 0)) {
                    day -= 30;
                    month += 1;
                }
            }
            if func != JustifyDays {
                if day > 0 && time < 0 {
                    time += DAY;
                    day -= 1;
                } else if day < 0 && time > 0 {
                    time -= DAY;
                    day += 1;
                }
            }
            let out = crate::core::pg_types::Interval {
                months: i32::try_from(month).map_err(|_| "interval out of range")?,
                days: i32::try_from(day).map_err(|_| "interval out of range")?,
                micros: time,
            };
            data(crate::core::pg_types::encode_interval(out), DbType::INTERVAL)
        }
        Factorial => {
            let n = int_of(&args[0], "the value")?;
            if n < 0 {
                return Err("factorial of a negative number is undefined".to_string());
            }
            let mut v: i128 = 1;
            for k in 2..=n as i128 {
                v = v.checked_mul(k).ok_or("value overflows numeric format")?;
            }
            numeric_value(PgNum::int(v))
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
        BitAnd | BitOr | BitXor | ShiftLeft | ShiftRight | BitNot => bitwise(func, args)?,
        StructOp => structural(args)?,
        ArrayCast => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            let text = match &args[0] {
                Value::Data { bytes, db_type: DbType::ARRAY } => crate::core::value_codec::format_array(bytes)
                    .ok_or_else(|| "could not read an array".to_string())?,
                other => text_of(other),
            };
            let mut raw = crate::core::value_codec::parse_array_literal_as(&text, crate::core::value_codec::OID_TEXT)
                .ok_or_else(|| format!("malformed array literal: \"{text}\""))?;
            // An array of another type converts element by element, as that
            // type's cast does (`ARRAY[1.5, 2.25]::int[]` is `{2,2}`); its
            // text read as the new type's input would refuse `1.5`. To text,
            // only a boolean's differs from the text kept: `t` is `true`.
            // The element type, and the modifier when the tag carries one
            // (`expr::cast_to`).
            let (tag, modifier) = match &args[1] {
                Value::Data { bytes, .. } if bytes.len() == 5 => (
                    Some(DbType::from_byte(bytes[0])),
                    crate::core::rql::type_modifier::element_modifier(u32::from_le_bytes(
                        bytes[1..5].try_into().expect("four bytes"),
                    )),
                ),
                other => (tag_type(other), None),
            };
            if let Value::Data { bytes, db_type: DbType::ARRAY } = &args[0]
                && let Some(source) = crate::core::value_codec::array_element_type(bytes)
                && let Some(target) = tag.clone().or_else(|| (source == DbType::BOOL).then_some(DbType::STRING))
                && source != target
                && let Some((_, dims, elements)) = crate::core::value_codec::decode_array_dims(bytes)
            {
                let mut cast: Vec<Option<Vec<u8>>> = Vec::with_capacity(elements.len());
                for e in elements {
                    let Some(e) = e else {
                        cast.push(None);
                        continue;
                    };
                    let text = String::from_utf8_lossy(e);
                    let value = match crate::core::value_codec::encode_element(&source, &text) {
                        Some(b) => Value::Data { bytes: b, db_type: source.clone() },
                        None => text_value(text.into_owned()),
                    };
                    let converted = crate::core::rql::expr::cast(value, &target)?;
                    cast.push((!converted.is_null()).then(|| text_of(&converted).into_bytes()));
                }
                raw = crate::core::value_codec::build_array_dims(
                    crate::core::value_codec::OID_TEXT,
                    &dims,
                    cast.iter().map(|e| e.as_deref()),
                );
            }
            match tag {
                None => data(raw, DbType::ARRAY),
                Some(element) => {
                    let held = crate::core::value_codec::hold_array_elements_to(&raw, &element, modifier, true)?;
                    data(held.unwrap_or(raw), DbType::ARRAY)
                }
            }
        }
        ArraySubscript => {
            if args[0].is_null() || args[2..].iter().any(Value::is_null) {
                return Ok(Value::Null);
            }
            let Value::Data { bytes, .. } = &args[0] else { return Ok(Value::Null) };
            let indexes = args[2..]
                .iter()
                .map(|a| crate::core::rql::expr::as_i64(a).ok_or_else(|| "array subscript must have type integer".to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            let Some(element) = crate::core::value_codec::array_element_at_all(bytes, &indexes) else {
                return Err("cannot subscript a value that is not an array".to_string());
            };
            let Some(text) = element else { return Ok(Value::Null) };
            let text = String::from_utf8_lossy(&text).into_owned();
            match tag_type(&args[1]) {
                None => text_value(text),
                // A numeric element at the places it holds (`{2.00}`'s is
                // 2.00, as PostgreSQL's): read into the engine's own scale it
                // was 2, and `a[1] + 1` was 3 where PostgreSQL's is 3.00.
                Some(DbType::DECIMAL) if pgn::parse_text(&text).is_some() => {
                    numeric_value(pgn::parse_text(&text).expect("checked"))
                }
                Some(t) => match crate::core::value_codec::encode_element(&t, &text) {
                    Some(b) => data(b, t),
                    // An element its array's type would not read (none can
                    // be in a typed column): as the text it is.
                    None => text_value(text),
                },
            }
        }
        QuantifiedCmp => {
            let all = text_of(&args[3]) == "ALL";
            let op = text_of(&args[2]);
            let Value::Data { bytes, .. } = &args[1] else { return Ok(Value::Null) };
            let Some((oid, elements)) = crate::core::value_codec::decode_array(bytes) else {
                return Err("op ANY/ALL (array) requires array on right side".to_string());
            };
            let truth = |b: bool| data(vec![u8::from(b)], DbType::BOOL);
            if elements.is_empty() {
                return Ok(truth(all));
            }
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            let typed = crate::core::value_codec::array_element_type(bytes);
            let left_type = args[0].db_type().cloned();
            let mut unknown = false;
            for e in elements {
                let Some(text) = e else {
                    unknown = true;
                    continue;
                };
                let text = String::from_utf8_lossy(text).into_owned();
                // The element as its array's type; an untyped array's as the
                // value's, as an untyped literal's would be (`'{1,5}'`).
                let as_type = typed.clone().or_else(|| left_type.clone().filter(|t| *t != DbType::STRING));
                let element = match as_type.and_then(|t| Some((crate::core::value_codec::encode_element(&t, &text)?, t))) {
                    Some((b, t)) => data(b, t),
                    None => text_value(text),
                };
                let Some(order) = crate::core::rql::expr::compare(&args[0], &element) else {
                    unknown = true;
                    continue;
                };
                use std::cmp::Ordering::*;
                let holds = match op.as_str() {
                    "=" => order == Equal,
                    "<>" | "!=" => order != Equal,
                    "<" => order == Less,
                    "<=" => order != Greater,
                    ">" => order == Greater,
                    ">=" => order != Less,
                    other => return Err(format!("operator {other} is not supported with ANY/ALL")),
                };
                if holds != all {
                    return Ok(truth(!all));
                }
            }
            let _ = oid;
            if unknown { Value::Null } else { truth(all) }
        }
        ArraySlice => {
            if args[0].is_null() {
                return Ok(Value::Null);
            }
            let Value::Data { bytes, .. } = &args[0] else { return Ok(Value::Null) };
            let bound = |v: &Value, open: i64| -> Result<Option<i64>, String> {
                match v {
                    Value::Null => Ok(Some(open)),
                    v => crate::core::rql::expr::as_i64(v)
                        .map(Some)
                        .ok_or_else(|| "array subscript must have type integer".to_string()),
                }
            };
            // An omitted bound is the array's own; a NULL one makes the
            // slice NULL (`expr::parse` passes an omitted one as NULL and a
            // written one as itself, so a written NULL arrives typed).
            let (Some(lo), Some(hi)) = (bound(&args[1], i64::MIN)?, bound(&args[2], i64::MAX)?) else {
                return Ok(Value::Null);
            };
            match crate::core::value_codec::array_slice(bytes, lo, hi) {
                Some(b) => data(b, DbType::ARRAY),
                None => return Err("slicing a multidimensional array is not supported yet".to_string()),
            }
        }
        // `ARRAY[ARRAY[1,2], ARRAY[3,4]]`: the inner arrays, all of one
        // shape, as the rows of one more dimension.
        ArrayCtor if args.iter().any(|a| matches!(a, Value::Data { db_type: DbType::ARRAY, .. })) => {
            let mut dims: Option<Vec<usize>> = None;
            let mut oid = 0i32;
            let mut elements: Vec<Option<Vec<u8>>> = Vec::new();
            for a in args {
                let Value::Data { bytes, db_type: DbType::ARRAY } = a else {
                    return Err("multidimensional arrays must have array expressions with matching dimensions".to_string());
                };
                let (o, d, e) = crate::core::value_codec::decode_array_dims(bytes)
                    .ok_or_else(|| "could not read an inner array".to_string())?;
                match &dims {
                    Some(have) if *have != d => {
                        return Err("multidimensional arrays must have array expressions with matching dimensions".to_string())
                    }
                    Some(_) => {}
                    None => {
                        dims = Some(d.clone());
                        oid = o;
                    }
                }
                elements.extend(e.into_iter().map(|x| x.map(<[u8]>::to_vec)));
            }
            let inner = dims.unwrap_or_default();
            if inner.is_empty() {
                return Ok(data(crate::core::value_codec::parse_array_literal("{}").expect("empty"), DbType::ARRAY));
            }
            let mut all = vec![args.len()];
            all.extend(inner);
            data(
                crate::core::value_codec::build_array_dims(if oid == 0 { 25 } else { oid }, &all, elements.iter().map(|e| e.as_deref())),
                DbType::ARRAY,
            )
        }
        ArrayCtor => {
            // Each element as its type prints it, NULL as the null element,
            // under the elements' type OID when they share one, so the array
            // orders as that type's does (`value_codec::array_order`).
            let mut literal = String::from("{");
            for (i, a) in args.iter().enumerate() {
                if i > 0 {
                    literal.push(',');
                }
                if a.is_null() {
                    literal.push_str("NULL");
                } else {
                    literal.push_str(&array_element(&text_of(a)));
                }
            }
            literal.push('}');
            let oid = array_ctor_oid(args);
            data(
                crate::core::value_codec::parse_array_literal_as(&literal, oid)
                    .ok_or_else(|| format!("could not build the array {literal}"))?,
                DbType::ARRAY,
            )
        }
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
/// The element type `a[i]`'s tag argument names; `None` for text.
fn tag_type(tag: &Value) -> Option<DbType> {
    match tag {
        Value::Data { bytes, .. } if bytes.len() == 1 => Some(DbType::from_byte(bytes[0])),
        _ => None,
    }
}

/// The type `a[i]` is described as: its tag's.
fn subscript_type(args: &[crate::core::rql::expr::Expr]) -> DbType {
    match args.get(1) {
        Some(crate::core::rql::expr::Expr::Literal { bytes, .. }) if bytes.len() == 1 => DbType::from_byte(bytes[0]),
        _ => DbType::STRING,
    }
}

/// The element OID of `ARRAY[…]` over `args`: their type when every
/// non-NULL one shares it (integers of any width as `bigint`, which orders
/// them alike), text's otherwise.
fn array_ctor_oid(args: &[Value]) -> i32 {
    let int = |t: &DbType| matches!(t, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32);
    let mut types = args.iter().filter_map(Value::db_type).map(|t| if int(t) { DbType::I64 } else { t.clone() });
    let Some(first) = types.next() else { return 25 };
    if types.all(|t| t == first) { crate::core::value_codec::array_oid_for(&first) } else { 25 }
}

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
    // Every element, in row-major order, however many dimensions it has:
    // `cardinality`, `array_to_string` and `unnest` read a multidimensional
    // array flat, as PostgreSQL's do.
    crate::core::value_codec::parse_nested_array(text)
        .map(|parsed| parsed.elements)
        .ok_or_else(|| format!("malformed array literal: \"{text}\""))
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

/// A template's patterns: PostgreSQL's keyword table (`DCH_keywords` in
/// `formatting.c`), in its order, which puts the longer of two patterns
/// sharing a start first (`HH24` before `HH`, `IYYY` before `IY`). Each is
/// spelled in capitals or all lower case (`yyyy-mm-dd` is a template), and a
/// few in title case; any other spelling is text. Whatever is no pattern is
/// copied as written, which is why one missing here came back as its own
/// letters (`to_char(d, 'WW')` was `WW`, `IYYY` was `I` and a 3-digit year).
const PATTERNS: &[&str] = &[
    "A.D.", "A.M.", "AD", "AM", "B.C.", "BC", "CC", "DAY", "DDD", "DD", "DY", "Day", "Dy", "D", "FF1", "FF2", "FF3",
    "FF4", "FF5", "FF6", "HH24", "HH12", "HH", "IDDD", "ID", "IW", "IYYY", "IYY", "IY", "I", "J", "MI", "MM", "MONTH",
    "MON", "MS", "Month", "Mon", "OF", "P.M.", "PM", "Q", "RM", "SSSSS", "SSSS", "SS", "TZH", "TZM", "TZ", "US", "WW",
    "W", "Y,YYY", "YYYY", "YYY", "YY", "Y", "a.d.", "a.m.", "ad", "am", "b.c.", "bc", "cc", "day", "ddd", "dd", "dy",
    "d", "ff1", "ff2", "ff3", "ff4", "ff5", "ff6", "hh24", "hh12", "hh", "iddd", "id", "iw", "iyyy", "iyy", "iy", "i",
    "j", "mi", "mm", "month", "mon", "ms", "of", "p.m.", "pm", "q", "rm", "sssss", "ssss", "ss", "tzh", "tzm", "tz",
    "us", "ww", "w", "y,yyy", "yyyy", "yyy", "yy", "y",
];

enum Piece {
    /// `name` as written; `ordinal` for a `TH`/`th` suffix (`DDth` is `01st`);
    /// `tm` for the `TM` prefix, under which a name is not padded.
    Pattern { name: &'static str, fill: bool, tm: bool, ordinal: Option<bool> },
    Literal(String),
}

fn template(fmt: &str) -> Vec<Piece> {
    let mut out = Vec::new();
    let mut rest = fmt;
    let (mut fill, mut tm) = (false, false);
    while !rest.is_empty() {
        // Prefix modifiers: `FM` drops the padding of the pattern after it,
        // `TM` a month or day name's (its translation is the C locale's,
        // the same words); `FX` changes nothing `to_char` prints.
        if let Some(prefix) = ["FM", "fm", "FX", "fx", "TM", "tm"].iter().find(|p| rest.starts_with(**p)) {
            fill |= prefix.eq_ignore_ascii_case("FM");
            tm |= prefix.eq_ignore_ascii_case("TM");
            rest = &rest[2..];
            continue;
        }
        if let Some(stripped) = rest.strip_prefix('"') {
            // Quoted text, `\"` a quote within it.
            let mut text = String::new();
            let mut chars = stripped.char_indices();
            let mut end = stripped.len();
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' => {
                        if let Some((_, next)) = chars.next() {
                            text.push(next);
                        }
                    }
                    '"' => {
                        end = i + 1;
                        break;
                    }
                    c => text.push(c),
                }
            }
            out.push(Piece::Literal(text));
            rest = &stripped[end.min(stripped.len())..];
            continue;
        }
        if let Some(name) = PATTERNS.iter().find(|p| rest.starts_with(**p)) {
            rest = &rest[name.len()..];
            let ordinal = if rest.starts_with("TH") {
                Some(true)
            } else if rest.starts_with("th") {
                Some(false)
            } else {
                None
            };
            if ordinal.is_some() {
                rest = &rest[2..];
            }
            out.push(Piece::Pattern { name, fill, tm, ordinal });
            (fill, tm) = (false, false);
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
        n if n.chars().all(|c| !c.is_ascii_lowercase()) => word.to_ascii_uppercase(),
        n if n.chars().all(|c| !c.is_ascii_uppercase()) => word.to_ascii_lowercase(),
        _ => word.to_string(),
    }
}

/// `n` with its English ordinal suffix, as `TH` (upper) or `th` asks.
fn with_ordinal(digits: String, n: i64, upper: bool) -> String {
    let n = n.abs();
    let suffix = match (n % 100, n % 10) {
        (11..=13, _) => "th",
        (_, 1) => "st",
        (_, 2) => "nd",
        (_, 3) => "rd",
        _ => "th",
    };
    digits + &if upper { suffix.to_ascii_uppercase() } else { suffix.to_string() }
}

/// The ISO 8601 week-numbering year of the day `days` since 1970-01-01, its
/// week (1 to 53) and its weekday (Monday 1 to Sunday 7): the week is the
/// one holding its Thursday, and the year that Thursday's.
fn iso_week(days: i64) -> (i64, i64, i64) {
    let weekday = (days + 3).rem_euclid(7) + 1;
    let thursday = days + 4 - weekday;
    let (year, _, _) = crate::core::value_codec::civil_from_days(thursday);
    let week = (thursday - crate::core::value_codec::days_from_civil(year, 1, 1)) / 7 + 1;
    (year, week, weekday)
}

const ROMAN_MONTHS: [&str; 12] = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"];

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
    let day_of_year = days - crate::core::value_codec::days_from_civil(f.year, 1, 1) + 1;
    let (iso_year, iso_week_no, iso_weekday) = iso_week(days);
    // PostgreSQL's years: 1 BC is year 0 here, and prints as 1.
    let (bc, year) = if f.year <= 0 { (true, 1 - f.year) } else { (false, f.year) };
    let mut out = String::new();
    for piece in template(fmt) {
        let (name, fill, tm, ordinal) = match piece {
            Piece::Literal(text) => {
                out.push_str(&text);
                continue;
            }
            Piece::Pattern { name, fill, tm, ordinal } => (name, fill, tm, ordinal),
        };
        let num = |v: i64, width: usize| {
            let digits = if fill { v.to_string() } else { format!("{v:0width$}") };
            match ordinal {
                Some(upper) => with_ordinal(digits, v, upper),
                None => digits,
            }
        };
        let word = |w: &str, width: usize| {
            let w = cased(name, w);
            if fill || tm { w } else { format!("{w:<width$}") }
        };
        let hour12 = if f.hour % 12 == 0 { 12 } else { f.hour % 12 };
        let upper = name.to_ascii_uppercase();
        out.push_str(&match upper.as_str() {
            "YYYY" => num(year, 4),
            "Y,YYY" => {
                let digits = format!("{},{:03}", year / 1000, year % 1000);
                match ordinal {
                    Some(upper) => with_ordinal(digits, year, upper),
                    None => digits,
                }
            }
            "YYY" => num(year % 1000, 3),
            "YY" => num(year % 100, 2),
            "Y" => num(year % 10, 1),
            "IYYY" => num(iso_year, 4),
            "IYY" => num(iso_year % 1000, 3),
            "IY" => num(iso_year % 100, 2),
            "I" => num(iso_year % 10, 1),
            "IW" => num(iso_week_no, 2),
            "ID" => num(iso_weekday, 1),
            "IDDD" => num((iso_week_no - 1) * 7 + iso_weekday, 3),
            "CC" => num(if bc { -((year + 99) / 100) } else { (year + 99) / 100 }, 2),
            "MM" => num(f.month, 2),
            "DD" => num(f.day, 2),
            "DDD" => num(day_of_year, 3),
            "D" => num(weekday as i64 + 1, 1),
            "W" => num((f.day - 1) / 7 + 1, 1),
            "WW" => num((day_of_year - 1) / 7 + 1, 2),
            "Q" => num((f.month - 1) / 3 + 1, 1),
            "J" => num(days + 2_440_588, 1),
            "HH24" => num(f.hour, 2),
            "HH12" | "HH" => num(hour12, 2),
            "MI" => num(f.minute, 2),
            "SS" => num(f.second, 2),
            "SSSS" | "SSSSS" => num(f.hour * 3600 + f.minute * 60 + f.second, 1),
            "MS" => num(f.micros / 1000, 3),
            "US" => num(f.micros, 6),
            "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" => {
                let places = (upper.as_bytes()[2] - b'0') as u32;
                num(f.micros / 10i64.pow(6 - places), places as usize)
            }
            "AM" | "PM" => cased(name, if f.hour < 12 { "AM" } else { "PM" }),
            "A.M." | "P.M." => cased(name, if f.hour < 12 { "A.M." } else { "P.M." }),
            "AD" | "BC" => cased(name, if bc { "BC" } else { "AD" }),
            "A.D." | "B.C." => cased(name, if bc { "B.C." } else { "A.D." }),
            "MONTH" => word(MONTHS[f.month as usize - 1], 9),
            "MON" => cased(name, &MONTHS[f.month as usize - 1][..3]),
            "DAY" => word(DAYS[weekday], 9),
            "DY" => cased(name, &DAYS[weekday][..3]),
            "RM" => word(ROMAN_MONTHS[f.month as usize - 1], 4),
            // A zone this value does not carry; refused rather than guessed.
            _ => return Err(format!("the template pattern \"{name}\" is not supported in to_char yet")),
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
                let upper = name.to_ascii_uppercase();
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
                match upper.as_str() {
                    "YYYY" => p.year = read(4, name)?,
                    "YY" => p.year = 2000 + read(2, name)?,
                    "MM" => p.month = read(2, name)?,
                    "DD" => p.day = read(2, name)?,
                    "HH24" | "HH12" | "HH" => hour = read(2, name)?,
                    "MI" => minute = read(2, name)?,
                    "SS" => second = read(2, name)?,
                    "MS" => frac = read(3, name)? * 1000,
                    "US" => frac = read(6, name)?,
                    "AM" | "PM" => {
                        let word: String = chars.iter().skip(i).take(2).collect();
                        pm = Some(word.eq_ignore_ascii_case("PM"));
                        i += 2;
                    }
                    "MON" | "MONTH" => {
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
