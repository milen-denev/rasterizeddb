//! Scalar expressions evaluated once per row.
//!
//! `COALESCE`, `NULLIF`, `LEAST`, `GREATEST`, `MOD`, `CASE WHEN` and `::` casts
//! all depend on the row they are applied to, so they cannot be folded into a
//! single value the way a literal can. This module parses them against a schema
//! and evaluates them against a row.
//!
//! # What is refused
//!
//! An expression this cannot parse is an **error**, never a fallback. The
//! write path previously handed an unrecognised expression to the value codec,
//! which stored the text it was given: `SET t = COALESCE(t, 'z')` wrote the
//! characters `COALESCE(t, 'z')` into the column and reported success. A
//! statement the engine cannot evaluate has to say so.

#[cfg(test)]
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::Row;
use crate::core::row::schema::SchemaField;

/// A value produced by evaluating an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL NULL — distinct from an empty string, which is a value.
    Null,
    Data {
        bytes: Vec<u8>,
        db_type: DbType,
    },
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The type this value carries, or `None` for NULL.
    pub fn db_type(&self) -> Option<&DbType> {
        match self {
            Value::Data { db_type, .. } => Some(db_type),
            Value::Null => None,
        }
    }
}

/// A comparison used by `CASE WHEN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
    /// `IS [NOT] TRUE` / `IS [NOT] FALSE` on a boolean, with SQL's third
    /// value: a NULL is neither true nor false, so `IS NOT TRUE` holds for it.
    /// (`IS [NOT] UNKNOWN` is `IS [NOT] NULL`.)
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
}

/// A condition: a comparison between two expressions, or a null test on one.
#[derive(Debug, Clone)]
pub struct Condition {
    pub left: Box<Expr>,
    pub cmp: Cmp,
    pub right: Option<Box<Expr>>,
    /// Whether a side reads a NUMERIC with more places than the engine's,
    /// decided on the first row and kept: [`eval_condition_3vl`] asked it of
    /// both sides on every row, a walk of the tree that cost a `CASE` over
    /// 60k rows some 2.6% of its instructions.
    pub wide: WideOnce,
}

/// [`Condition::wide`]: computed once, on first use.
#[derive(Debug, Clone, Default)]
pub struct WideOnce(std::sync::OnceLock<bool>);

/// A boolean condition tree: comparisons joined by `AND`/`OR`, possibly
/// parenthesised. `CASE WHEN` arms are the consumer — a real application
/// writes `WHEN layout_columns IS NULL OR layout_columns <= 0 THEN …` — while
/// a bare comparison stays a [`Condition`], which the aggregate `FILTER` and
/// join paths keep using unchanged.
#[derive(Debug, Clone)]
pub enum Cond {
    Cmp(Condition),
    /// All parts must hold. Binds tighter than `Or`, as SQL's `AND` does.
    And(Vec<Cond>),
    /// Any part suffices.
    Or(Vec<Cond>),
    /// SQL's three-valued NOT: TRUE for FALSE, FALSE for TRUE, UNKNOWN for
    /// UNKNOWN. Evaluated through [`eval_cond_3vl`] even by [`eval_cond`],
    /// so a NULL comparison under it never becomes TRUE.
    Not(Box<Cond>),
}

/// A scalar expression.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A column of the row being evaluated.
    Column(SchemaField),
    /// A written constant.
    Literal {
        bytes: Vec<u8>,
        db_type: DbType,
    },
    Null,
    /// A nullary function — `now()`, `gen_random_uuid()` — evaluated per row.
    Scalar(crate::core::rql::aggregate::ScalarFunc),
    /// First argument that is not NULL.
    Coalesce(Vec<Expr>),
    /// NULL when the two are equal, otherwise the first.
    NullIf(Box<Expr>, Box<Expr>),
    Least(Vec<Expr>),
    Greatest(Vec<Expr>),
    /// Remainder, over integers.
    Mod(Box<Expr>, Box<Expr>),
    Case {
        whens: Vec<(Cond, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    /// `expr::type` — a change of type, not of value.
    Cast(Box<Expr>, DbType),
    /// `expr::numeric(p, s)`: the value — a `Cast` to `NUMERIC` — rounded to
    /// `s` places, refused past `p` digits. A `Cast` carries no modifier, so
    /// `(3 * 1.5)::numeric(10,2)` was a plain cast and came back as
    /// 4.500000000 where PostgreSQL gives 4.50.
    NumericCast {
        value: Box<Expr>,
        precision: u32,
        scale: u32,
    },
    /// `expr::varchar(n)`, `::char(n)`, `::timestamp(p)`, `::time(p)`,
    /// `::interval(p)`: the value — a `Cast` to the type — held to the
    /// type's modifier, as PostgreSQL's explicit cast holds it ([`CastTypmod`]).
    /// A `Cast` carries no modifier, and these were plain casts: `'abcd'::
    /// varchar(2)` was `abcd`, so `WHERE s::varchar(2) = 'ab'` matched nothing.
    TypmodCast {
        value: Box<Expr>,
        typmod: CastTypmod,
    },
    /// Binary arithmetic: `a + b`, `a * b`, and so on.
    Arith(Box<Expr>, ArithOp, Box<Expr>),
    /// `a || b` — string concatenation. Not arithmetic: its operands are text
    /// and a NULL operand makes the whole result NULL.
    Concat(Box<Expr>, Box<Expr>),
    /// A function over one or more arguments, evaluated per row.
    Call(TextFunc, Vec<Expr>),
    /// `doc->'a'->>'b'` — a value reached inside a stored document.
    ///
    /// The steps are resolved against the encoded bytes, so only the value
    /// asked for is decoded rather than the whole document.
    JsonPath {
        column: SchemaField,
        steps: Vec<crate::core::jsonb::Step>,
        /// `->>` rather than `->`: the value is wanted as text, so a string
        /// yields its own contents instead of a quoted JSON string.
        as_text: bool,
    },
    /// A path into a document that is not a column: `('{"a":1}'::jsonb)->'a'`,
    /// `s::jsonb->>'k'`, `to_jsonb(x)->0`. `base` is `jsonb`, or `json` when
    /// `yields_json_text` says so (its text read where the path leads). Read
    /// as anything else, the arrow was taken for a cast's type name
    /// ("cast to 'jsonb->'a''") or, parenthesised, dropped: the whole
    /// document came back.
    PathOf {
        base: Box<Expr>,
        steps: Vec<crate::core::jsonb::Step>,
        as_text: bool,
    },
}

/// The modifier of a cast the evaluator applies ([`Expr::TypmodCast`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastTypmod {
    /// `character(n)` (padded on output, compared without trailing spaces)
    /// or `character varying(n)`: cut to the length.
    Text(crate::core::rql::type_modifier::TextTypmod),
    /// `timestamp(p)` / `timestamptz(p)`: rounded to `p` fractional digits.
    Timestamp(u32),
    /// `time(p)`.
    Time(u32),
    /// `timetz(p)`.
    TimeTz(u32),
    /// `interval(p)`: its time part rounded to `p` fractional digits.
    Interval(u32),
}

/// The modifier of the cast `target` names, when it has one this evaluator
/// applies: `varchar(n)`, `char(n)` (and a bare `character`, `char(1)`),
/// `timestamp(p)`, `timestamptz(p)`, `time(p)`, `timetz(p)`, `interval(p)`.
fn cast_typmod(target: &str, db_type: &DbType) -> Result<Option<CastTypmod>, String> {
    if *db_type == DbType::STRING {
        return Ok(match crate::core::rql::type_modifier::parse_text_typmod(target) {
            Some(Ok(t)) => Some(CastTypmod::Text(t)),
            Some(Err(e)) => return Err(e),
            None => None,
        });
    }
    if !matches!(db_type, DbType::DATETIME | DbType::TIMESTAMP | DbType::TIME | DbType::TIMETZ | DbType::INTERVAL) {
        return Ok(None);
    }
    // `interval hour to minute`, `interval day to second(0)`: the fields
    // with the precision (`type_modifier::parse_time_precision`).
    if *db_type == DbType::INTERVAL {
        if let Some(t) = crate::core::rql::type_modifier::parse_time_precision(target) {
            return match t? {
                crate::core::rql::type_modifier::TextTypmod::Precision(p) => Ok(Some(CastTypmod::Interval(p))),
                _ => Ok(None),
            };
        }
    }
    let text = target.trim();
    let Some(open) = text.find('(') else { return Ok(None) };
    let Some(close) = text[open..].find(')').map(|c| c + open) else { return Ok(None) };
    let digits = text[open + 1..close].trim();
    let precision: i64 = digits
        .parse()
        .map_err(|_| format!("invalid type modifier '{}'", digits))?;
    if precision < 0 {
        return Err(format!("{} precision must not be negative", crate::core::value_codec::pg_input_type_name(db_type)));
    }
    // Past six PostgreSQL warns and keeps six, which changes nothing.
    let precision = precision.min(6) as u32;
    // By the type the value is cast to, whose layout the rounding reads.
    Ok(match db_type {
        DbType::DATETIME | DbType::TIMESTAMP => Some(CastTypmod::Timestamp(precision)),
        DbType::TIME => Some(CastTypmod::Time(precision)),
        DbType::TIMETZ => Some(CastTypmod::TimeTz(precision)),
        DbType::INTERVAL => Some(CastTypmod::Interval(precision)),
        _ => None,
    })
}

/// `value`, the result of a cast, held to the cast's modifier.
fn apply_cast_typmod(value: Value, typmod: CastTypmod) -> Result<Value, String> {
    let Value::Data { bytes, db_type } = value else { return Ok(Value::Null) };
    let bytes = match typmod {
        CastTypmod::Text(t) => match std::str::from_utf8(&bytes) {
            Ok(text) => t.cast(text).as_bytes().to_vec(),
            Err(_) => bytes,
        },
        // Rounded as an assignment to a column of the precision rounds
        // (`type_modifier::round_time_bytes`), a timestamp on PostgreSQL's
        // epoch: `1999-12-31 23:59:59.5::timestamp(0)` is 23:59:59.
        CastTypmod::Timestamp(p) | CastTypmod::Time(p) | CastTypmod::TimeTz(p) | CastTypmod::Interval(p) => {
            crate::core::rql::type_modifier::round_time_bytes(&db_type, &bytes, p).unwrap_or(bytes)
        }
    };
    Ok(Value::Data { bytes, db_type })
}

/// An integer function argument as a number: an integer of any width, or
/// its text.
fn integer_argument(bytes: &[u8], db_type: &DbType) -> Option<i64> {
    Some(match db_type {
        DbType::I8 => i64::from(*bytes.first()? as i8),
        DbType::I16 => i64::from(i16::from_le_bytes(bytes.try_into().ok()?)),
        DbType::I32 => i64::from(i32::from_le_bytes(bytes.try_into().ok()?)),
        DbType::I64 => i64::from_le_bytes(bytes.try_into().ok()?),
        DbType::STRING => std::str::from_utf8(bytes).ok()?.trim().parse().ok()?,
        _ => return None,
    })
}

/// A `character varying` column compared with a `character(n)` value, read
/// as PostgreSQL resolves `bpchar = varchar`: blank-padded, its trailing
/// spaces not part of it (`rtrim`). A join's condition, evaluated over the
/// joined row, compares two columns this way.
fn blank_pad_varying(expr: &mut Expr) {
    use crate::core::rql::type_modifier::TextTypmod;
    if let Expr::Column(f) = expr {
        if f.db_type == DbType::STRING && matches!(f.text_typmod(), Some(TextTypmod::Varchar(_))) {
            let column = std::mem::replace(expr, Expr::Null);
            *expr = Expr::Call(TextFunc::Rtrim, vec![column]);
        }
    }
}

/// A string literal without its trailing spaces: how one compared with a
/// `character(n)` value is read.
fn strip_trailing_spaces(expr: &mut Expr) {
    if let Expr::Literal { bytes, db_type: DbType::STRING } = expr {
        while bytes.last() == Some(&b' ') {
            bytes.pop();
        }
    }
}

/// Whether `expr` yields a `character(n)` value: a column declared so, or a
/// cast to the type. Such a value is held without its trailing spaces
/// (`type_modifier::TextTypmod`), and a string compared with it is read the
/// same way, as PostgreSQL's `bpchar` comparison reads both sides.
pub fn yields_bpchar(expr: &Expr) -> bool {
    use crate::core::rql::type_modifier::TextTypmod;
    match expr {
        // A legacy `CHAR` is `character(1)` too (`value_codec::legacy_char_text`).
        Expr::Column(f) => {
            (matches!(f.text_typmod(), Some(TextTypmod::Bpchar(_))) && f.db_type == DbType::STRING)
                || f.db_type == DbType::CHAR
        }
        Expr::TypmodCast { typmod: CastTypmod::Text(TextTypmod::Bpchar(_)), .. } => true,
        // `CASE`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` over
        // `character(n)` values and untyped strings are `bpchar`, as
        // PostgreSQL's common type of them is.
        _ => common_bpchar(expr).is_some(),
    }
}

/// The branches of `expr` when it is a `CASE`, `COALESCE`, `NULLIF`,
/// `GREATEST` or `LEAST` whose common type is `bpchar`: at least one branch
/// yields `character(n)` and every other is one too, an untyped string
/// literal or `NULL`. With them, whether any branch is such a literal.
fn common_bpchar(expr: &Expr) -> Option<(Vec<&Expr>, bool)> {
    let branches: Vec<&Expr> = match expr {
        Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => args.iter().collect(),
        Expr::NullIf(a, _) => vec![a.as_ref()],
        // The `ELSE` first, as PostgreSQL resolves the type
        // (`blank_pad_text_branches`).
        Expr::Case { whens, otherwise } => otherwise.as_deref().into_iter().chain(whens.iter().map(|(_, e)| e)).collect(),
        _ => return None,
    };
    let mut any = false;
    let mut literal = false;
    for b in &branches {
        match b {
            Expr::Null => {}
            Expr::Literal { db_type: DbType::STRING, .. } => literal = true,
            other if yields_bpchar(other) => any = true,
            // A text value after a `character(n)` one, coerced to it
            // (`blank_pad_text_branches`).
            Expr::Call(TextFunc::Rtrim, _) if any => {}
            _ => return None,
        }
    }
    any.then_some((branches, literal))
}

/// The branches of a `CASE`, `COALESCE`, `GREATEST` or `LEAST` whose first
/// typed branch yields `character(n)`: PostgreSQL resolves their common type
/// to `bpchar` (the first of two types each casts to implicitly), so a text
/// branch after it is read blank-padded, without its trailing spaces.
/// Wrapped in `rtrim` here, it compares, groups and keys as that type does;
/// the [`common_bpchar`] that follows then reads the whole as `bpchar`.
/// A text branch first makes the common type text, and nothing changes.
fn blank_pad_text_branches<'a>(branches: impl Iterator<Item = &'a mut Expr>) {
    let mut padded: Option<bool> = None;
    for branch in branches {
        match branch {
            Expr::Null | Expr::Literal { db_type: DbType::STRING, .. } => {}
            other => match padded {
                None => padded = Some(yields_bpchar(other)),
                Some(false) => return,
                Some(true) => {
                    if !yields_bpchar(other) && static_type(other) == DbType::STRING {
                        let text = std::mem::replace(other, Expr::Null);
                        *other = Expr::Call(TextFunc::Rtrim, vec![text]);
                    }
                }
            },
        }
    }
}

/// The declared length a select-list item that yields `character(n)` is
/// shown at (`DbType::BPCHAR`), when it has one: a column declared so, or a
/// cast to the type.
pub fn bpchar_width(expr: &Expr) -> Option<u32> {
    use crate::core::rql::type_modifier::TextTypmod;
    match expr {
        Expr::Column(f) if f.db_type == DbType::STRING => match f.text_typmod() {
            Some(TextTypmod::Bpchar(n)) => Some(n),
            _ => None,
        },
        Expr::TypmodCast { typmod: CastTypmod::Text(TextTypmod::Bpchar(n)), .. } => Some(*n),
        // A common `bpchar` of branches of one length keeps it; with another
        // length or an untyped string among them it has none, as in
        // PostgreSQL, and is shown as stored.
        other => {
            let (branches, literal) = common_bpchar(other)?;
            let mut widths = branches.iter().filter(|b| !matches!(b, Expr::Null | Expr::Literal { .. })).map(|b| bpchar_width(b));
            let first = widths.next().flatten()?;
            Some(if !literal && widths.all(|w| w == Some(first)) { first } else { 0 })
        }
    }
}

/// A function taking arguments, as opposed to the nullary [`ScalarFunc`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextFunc {
    /// Quotes an identifier if it needs quoting, as PostgreSQL does.
    QuoteIdent,
    /// The schema a bare name resolves in. Always `public` here.
    CurrentSchema,
    /// `replace(text, from, to)`.
    Replace,
    /// Length in bytes of a stored value.
    ColumnSize,
    /// Bytes a relation occupies on disk, named by oid or by name.
    RelationSize,
    /// Lower/upper case.
    Lower,
    Upper,
    /// `date_trunc('minute', ts)` — an instant floored to a unit.
    DateTrunc,
    /// `to_tsvector(config, document)` — a document reduced to its lexemes.
    ToTsVector,
    /// `to_tsquery(config, query)` — a boolean query over lexemes.
    ToTsQuery,
    /// `plainto_tsquery(config, text)` — every word of the text, `AND`ed.
    PlainToTsQuery,
    /// `phraseto_tsquery(config, text)` — every word, each followed by the
    /// next.
    PhraseToTsQuery,
    /// `websearch_to_tsquery(config, text)` — the search-box syntax.
    WebsearchToTsQuery,
    /// `@@`, as PostgreSQL's `ts_match_vq(tsvector, tsquery)`,
    /// `ts_match_tq(text, tsquery)` and `ts_match_tt(text, text)`: a vector
    /// and a query are both carried as their text here, so which each side
    /// is comes from the expression that made it (`expr::text_match`).
    TsMatchVq,
    TsMatchTq,
    TsMatchTt,
    /// `array_length(array, dimension)` — how many elements it holds.
    ArrayLength,
    /// `array_ndims(array)` — how many dimensions it has.
    ArrayNdims,
    /// `uuid_extract_timestamp(uuid)` — the instant in a v1 or v7 UUID.
    UuidExtractTimestamp,
    /// `uuid_extract_version(uuid)` — the version nibble of an RFC 9562 UUID.
    UuidExtractVersion,
    /// `length` / `char_length` / `character_length` — characters in a text
    /// value; for `bytea`, bytes (as PostgreSQL's `length(bytea)`).
    Length,
    /// `octet_length` — bytes in the stored value.
    OctetLength,
    /// `abs(x)` — the magnitude, in `x`'s own type.
    Abs,
    /// `round(x)` and `round(x, places)`, as PostgreSQL's overloads: a
    /// NUMERIC rounds half away from zero and keeps NUMERIC; an integer or a
    /// float alone becomes `double precision` rounded half to even (`rint`).
    /// `places` is a constant, held as an `I32` literal (or `Expr::Null`).
    Round,
    /// `trunc(x [, places])`: [`TextFunc::Round`] toward zero.
    Trunc,
    /// `EXTRACT(field FROM x)`: a `numeric`, as PostgreSQL 14 and later
    /// return it, at the field's display scale (six places for `SECOND` and
    /// `EPOCH`, three for `MILLISECONDS`, none otherwise).
    /// The field is a canonical name ([`date_field`]) in an argument literal.
    ExtractField,
    /// `date_part('field', x)`: the same fields as a `double precision`.
    DatePart,
    /// `btrim` / `ltrim` / `rtrim(text [, characters])`: the characters
    /// (a space when none are given) taken off both ends, the start or the end.
    Btrim,
    Ltrim,
    Rtrim,
    /// `rpad` / `lpad(text, length [, fill])`: filled to `length`
    /// characters (a space when no fill is given) on the right or the left,
    /// or cut to it. What a pattern match reads a `character(n)` column as.
    Rpad,
    Lpad,
    /// `jsonb_typeof(doc)`: `object`, `array`, `string`, `number`, `boolean`
    /// or `null`.
    JsonbTypeof,
    /// `regexp_like(text, pattern [, flags])`: whether a POSIX regular
    /// expression matches, the boolean `~` (and, with flag `i`, `~*`) is.
    RegexpLike,
    /// `text LIKE pattern` as a value (`like_condition`): the pattern with
    /// PostgreSQL's default escape, a backslash.
    Like,
    /// `text ILIKE pattern`: [`TextFunc::Like`] with both sides lowered
    /// as the WHERE path's matcher lowers them (Unicode, not only ASCII).
    ILike,
    /// `doc @> doc` as a value (`jsonb_condition`); `<@` swaps the sides.
    JsonbContains,
    /// `doc ? key`: a top-level key, or string element (`jsonb::key_exists`).
    JsonbHasKey,
    /// `regexp_like` inside a CHECK (`__check_regexp_like`): a value the
    /// answer cannot be vouched for is NULL, and marks the evaluation
    /// uncertain ([`take_check_uncertainty`]), so the check is not judged
    /// for that row rather than judged another way.
    CheckRegexpLike,
    /// `__check_regexp_uncertain(s, p[, f])`: whether a CHECK's regular
    /// expression has no answer this engine can vouch for on `s` (a locale
    /// class over non-ASCII text). What ADD CONSTRAINT counts its stored
    /// rows not judged by.
    CheckRegexpUncertain,
    /// `similar_to_escape(pattern [, escape])`: a `SIMILAR TO` pattern as the
    /// anchored regular expression it means, as `pg_dump` writes `SIMILAR
    /// TO` (`x ~ similar_to_escape('a%')`).
    SimilarToEscape,
    /// One of PostgreSQL's everyday functions (`rql::pg_functions`).
    Pg(crate::core::rql::pg_functions::PgFunc),
}

/// How many elements an array's text form `{a,b,"c,d"}` holds.
///
/// Counted from the text rather than the binary header because that is the
/// form `format_array` yields, and a comma inside a quoted element is not a
/// separator — `{"a,b"}` holds one element, not two.
fn array_element_count(text: &str) -> usize {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .unwrap_or(trimmed);
    if body.trim().is_empty() {
        return 0;
    }
    let mut count = 1usize;
    let mut in_quotes = false;
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => in_quotes = !in_quotes,
            '\\' if in_quotes => {
                chars.next();
            }
            ',' if !in_quotes => count += 1,
            _ => {}
        }
    }
    count
}

/// A binary arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    fn symbol(self) -> char {
        match self {
            ArithOp::Add => '+',
            ArithOp::Sub => '-',
            ArithOp::Mul => '*',
            ArithOp::Div => '/',
            ArithOp::Rem => '%',
        }
    }
}

/// True when `text` is something this module must handle rather than a plain
/// literal — used to decide whether an assignment needs per-row evaluation.
pub fn needs_evaluation(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.starts_with('\'') {
        // A quoted literal may contain anything, including `::`.
        return false;
    }
    if trimmed.contains("::") {
        return true;
    }
    // `a[1]`, `a[1] + 1`: a subscript (no unquoted name holds a bracket).
    if trimmed.contains('[') && !trimmed.ends_with("[]") {
        return true;
    }
    // `ARRAY[a, b]`, and any expression over one or subscripting one.
    if trimmed.len() >= 6
        && trimmed.as_bytes().windows(6).enumerate().any(|(i, w)| {
            w[..5].eq_ignore_ascii_case(b"ARRAY")
                && (w[5] == b'[' || w[5].is_ascii_whitespace())
                && (i == 0 || !(trimmed.as_bytes()[i - 1].is_ascii_alphanumeric() || trimmed.as_bytes()[i - 1] == b'_'))
        })
    {
        return true;
    }
    // A wholly parenthesised item is the expression inside it. Without this
    // `SELECT (n - 1) AS less` reached the select list's call branch, which
    // read the leading `(` as a call whose name is the empty string and
    // refused the statement with "function '' is not supported yet" — while
    // the identical `n - 1` unparenthesised worked. A parenthesised `SELECT`
    // is a scalar subquery and belongs to the substitution pass instead.
    if trimmed.starts_with('(') {
        if let Some(close) = matching_paren(trimmed, 0) {
            if close == trimmed.len() - 1 {
                let inner = trimmed[1..close].trim();
                return !inner.to_ascii_uppercase().starts_with("SELECT");
            }
        }
    }
    // A path into a document is an expression, not a column name. Asked
    // structurally rather than by looking for `->`, so `#>` and `#>>` are
    // covered too and a column whose *name* contains an arrow is not.
    if crate::core::jsonb::parse_path_expression(trimmed).is_some()
        || crate::core::jsonb::parse_qualified_path_expression(trimmed).is_some()
    {
        return true;
    }
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("CASE ") {
        return true;
    }
    // `CURRENT_TIMESTAMP` is a value written without a call's parentheses, so
    // the `(` test below never saw it and the term was left for the predicate
    // tokenizer to read as a column name.
    if keyword_constant(trimmed).is_some() {
        return true;
    }
    match trimmed.find('(') {
        Some(open) => {
            let name = trimmed[..open].trim();
            !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
        }
        None => false,
    }
}

/// Splits on top-level commas, respecting quotes and nesting.
fn split_args(text: &str) -> Vec<String> {
    crate::core::rql::lexer_ct::split_definitions(text)
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Index of the quote closing the string literal `text` opens with, reading
/// `''` as an escaped quote inside it, or `None` when it never closes.
pub(crate) fn closing_quote(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Index of the parenthesis matching the one at `open`, ignoring quoted text.
pub(crate) fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut i = open;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// The SQL type a `::` cast names, for callers outside this module.
///
/// One definition of what a cast's type name means, so the predicate path and
/// the expression evaluator cannot drift apart on it.
pub fn db_type_of_cast(name: &str) -> Option<DbType> {
    // The predicate tokenizer still reads `::date` and `DATE '…'` as a
    // timestamp at midnight, which is what its comparisons are built on; only
    // the expression evaluator has date arithmetic.
    // Nor does it tell the two timestamp types apart: a literal it reads is
    // already zone-free for `timestamp` (`sql_scan::zone_free_timestamp_literals`).
    match cast_target(name)? {
        DbType::DATE | DbType::TIMESTAMP => Some(DbType::DATETIME),
        other => Some(other),
    }
}

/// The SQL type a `::` cast names.
fn cast_target(name: &str) -> Option<DbType> {
    // `timestamp` (without time zone) and `timestamptz` are one storage type
    // and two types: a value cast to the first is read without a zone and
    // shown without one (`yields_timestamp`).
    if let Some(with_zone) = timestamp_cast_zone(name) {
        return Some(if with_zone { DbType::DATETIME } else { DbType::TIMESTAMP });
    }
    // `interval <fields>`, which `cast_typmod` restricts.
    if let Some(Ok(_)) = crate::core::rql::type_modifier::parse_time_precision(name)
        .filter(|_| name.trim().get(..9).is_some_and(|h| h.eq_ignore_ascii_case("interval ")))
    {
        return Some(DbType::INTERVAL);
    }
    Some(
        match name
            .trim()
            .split('(')
            .next()?
            .trim()
            .to_ascii_uppercase()
            .as_str()
        {
            "BIGINT" | "INT8" | "INT64" => DbType::I64,
            "INT" | "INTEGER" | "INT4" => DbType::I32,
            "SMALLINT" | "INT2" => DbType::I16,
            "TEXT" | "VARCHAR" | "CHARACTER VARYING" => DbType::STRING,
            // `character(n)`: text, held to its length by the cast's
            // modifier (`Expr::TypmodCast`).
            "CHAR" | "CHARACTER" | "BPCHAR" | "NCHAR" => DbType::STRING,
            "BOOL" | "BOOLEAN" => DbType::BOOL,
            "UUID" => DbType::UUID,
            // A real date, not a timestamp at midnight: `date + 3` is three
            // days, and a timestamp plus 3 was three microseconds — the
            // engine's interval unit — rendered as `00:00:00.000003+00`.
            "DATE" => DbType::DATE,
            "NUMERIC" | "DECIMAL" => DbType::DECIMAL,
            "FLOAT8" | "DOUBLE PRECISION" => DbType::F64,
            // `real` IS float4 — mapping it to F64 made the described type float8,
            // and a client decoding the column as f32 then refused the row.
            "REAL" | "FLOAT4" => DbType::F32,
            "JSON" => DbType::JSONTEXT,
            "JSONB" => DbType::JSONB,
            "BYTEA" => DbType::BYTEA,
            "INET" | "CIDR" => DbType::INET,
            "INTERVAL" => DbType::INTERVAL,
            "MACADDR" => DbType::MACADDR,
            "MACADDR8" => DbType::MACADDR8,
            "BIT" => DbType::BIT,
            "VARBIT" | "BIT VARYING" => DbType::VARBIT,
            "TIME" => DbType::TIME,
            "TIMETZ" | "TIME WITH TIME ZONE" => DbType::TIMETZ,
            "POINT" => DbType::POINT,
            "LINE" => DbType::LINE,
            "LSEG" => DbType::LSEG,
            "BOX" => DbType::BOX,
            "PATH" => DbType::PATH,
            "POLYGON" => DbType::POLYGON,
            "CIRCLE" => DbType::CIRCLE,
            "INT4RANGE" => DbType::INT4RANGE,
            "INT8RANGE" => DbType::INT8RANGE,
            "NUMRANGE" => DbType::NUMRANGE,
            "TSRANGE" => DbType::TSRANGE,
            "TSTZRANGE" => DbType::TSTZRANGE,
            "DATERANGE" => DbType::DATERANGE,
            _ => return None,
        },
    )
}

/// A cast of `inner` to the type `target` names, keeping a `NUMERIC(p, s)`
/// modifier as a [`Expr::NumericCast`].
/// For a timestamp type name, whether it carries a zone: `timestamptz` and
/// `timestamp with time zone` do, `timestamp` and `timestamp without time
/// zone` do not, with or without a `(p)`. `None` for any other name.
fn timestamp_cast_zone(name: &str) -> Option<bool> {
    let mut words = String::with_capacity(name.len());
    let mut depth = 0usize;
    for c in name.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            c => words.push(c.to_ascii_uppercase()),
        }
    }
    match words.split_whitespace().collect::<Vec<_>>().join(" ").as_str() {
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Some(true),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Some(false),
        _ => None,
    }
}

fn cast_to(inner: Expr, target: &str) -> Result<Expr, String> {
    // `double` alone names no type in PostgreSQL (`double precision` does).
    if target.trim().eq_ignore_ascii_case("double") {
        return Err("type \"double\" does not exist".to_string());
    }
    // `x::integer[]`, `CAST(x AS float8[][])`: an array of the element type,
    // each element read as a typed array column holds it.
    if let Some(base) = crate::core::rql::type_modifier::array_base(target.trim()) {
        if base.trim().eq_ignore_ascii_case("double") || cast_target(base).is_none() {
            return Err(format!("type \"{}[]\" does not exist", base.trim().to_ascii_lowercase()));
        }
        // The tag: the element type's code, then the whole element value
        // when it holds a modifier (`'{1.234}'::numeric(5,2)[]` is {1.23},
        // `'{abcdef}'::varchar(3)[]` is {abc}); empty for text.
        let tag = match crate::core::rql::type_modifier::parse_array_element(target) {
            Some(crate::core::rql::type_modifier::TextTypmod::Element(raw)) => {
                let mut tag = vec![crate::core::rql::type_modifier::element_code(raw)];
                if crate::core::rql::type_modifier::element_modifier(raw).is_some() {
                    tag.extend_from_slice(&raw.to_le_bytes());
                }
                tag
            }
            // A modified type with no array form here (`varchar(n)` past
            // what an element value holds): refused, not cast unheld.
            None if base.contains('(') => {
                return Err(format!("a cast to an array of {} is not supported yet", base.trim().to_ascii_lowercase()))
            }
            _ => Vec::new(),
        };
        return Ok(Expr::Call(
            TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArrayCast),
            vec![inner, Expr::Literal { bytes: tag, db_type: DbType::STRING }],
        ));
    }
    // Nothing may follow a type modifier but `[]` or a zone clause. The name
    // is read up to its `(`, so `c::numeric(12,3) > 1` cast to NUMERIC(12,3)
    // and dropped the comparison: the select list sent the number, where it
    // only tries an item as a comparison once it is not an expression.
    if let Some(open) = target.find('(') {
        let close = matching_paren(target, open)
            .ok_or_else(|| format!("cast to '{}' is not supported yet", target.trim()))?;
        let mut rest = target[close + 1..].trim();
        while let Some(r) = rest.strip_suffix("[]") {
            rest = r.trim_end();
        }
        let words = rest.split_whitespace().map(|w| w.to_ascii_uppercase()).collect::<Vec<_>>().join(" ");
        if !matches!(words.as_str(), "" | "WITH TIME ZONE" | "WITHOUT TIME ZONE") {
            return Err(format!("cast to '{}' is not supported yet", target.trim()));
        }
    }
    let db_type = cast_target(target)
        .ok_or_else(|| format!("cast to '{}' is not supported yet", target.trim()))?;
    let cast = Expr::Cast(Box::new(inner), db_type.clone());
    if let Some(typmod) = cast_typmod(target, &db_type)? {
        return Ok(Expr::TypmodCast { value: Box::new(cast), typmod });
    }
    if db_type == DbType::DECIMAL {
        if let Some(modifier) = crate::core::rql::type_modifier::parse_numeric_modifier(target) {
            let (precision, scale) = modifier?;
            if u32::from(scale) > crate::core::value_codec::DECIMAL_SCALE {
                return Err(format!(
                    "a cast to numeric with more than {} places is not supported yet",
                    crate::core::value_codec::DECIMAL_SCALE
                ));
            }
            return Ok(Expr::NumericCast {
                value: Box::new(cast),
                precision: u32::from(precision),
                scale: u32::from(scale),
            });
        }
    }
    Ok(cast)
}

/// Position of a top-level `::`, if the expression ends in a cast.
fn top_level_cast(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let nesting = Nesting::of(text);
    let mut found = None;
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] == b':' && bytes[i + 1] == b':' && nesting.top_level(i) {
            found = Some(i);
            i += 1;
        }
        i += 1;
    }
    found
}

/// How deeply each byte of an expression is nested, counting parentheses and
/// `CASE … END` blocks alike, and where its `CASE` keywords are.
///
/// A `CASE` block keeps its arms at parenthesis depth zero, so a scanner that
/// counted only parentheses took an operator inside an arm for one outside the
/// block: `CASE … ELSE 'r' || n END` was split at its `||` into two halves
/// that each failed to parse, and a nested block ended at the first `END`.
/// The `CASE` and `END` words sit at the depth of the block they open and
/// close, so the block as a whole reads as one top-level term.
pub(crate) struct Nesting {
    /// The depth of each byte, `None` inside a quoted literal or identifier.
    depth: Vec<Option<u32>>,
    /// Each `CASE`, `WHEN`, `THEN`, `ELSE` and `END` outside quotes, as
    /// `(start, end, depth)`, in order.
    keywords: Vec<(usize, usize, u32)>,
}

impl Nesting {
    pub(crate) fn of(text: &str) -> Nesting {
        let bytes = text.as_bytes();
        let mut depth: Vec<Option<u32>> = vec![None; bytes.len()];
        let mut keywords = Vec::new();
        let mut level = 0u32;
        let mut quote: Option<u8> = None;
        let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'.';
        let mut i = 0usize;
        while i < bytes.len() {
            let c = bytes[i];
            if let Some(q) = quote {
                if c == q {
                    quote = None;
                }
                i += 1;
                continue;
            }
            match c {
                b'\'' | b'"' => quote = Some(c),
                // Brackets nest as parentheses do: the `+` of `a[i + 1]` is
                // the subscript's.
                b'(' | b'[' => {
                    depth[i] = Some(level);
                    level += 1;
                }
                b')' | b']' => {
                    level = level.saturating_sub(1);
                    depth[i] = Some(level);
                }
                _ if is_word(c) && (i == 0 || !is_word(bytes[i - 1])) => {
                    let mut end = i;
                    while end < bytes.len() && is_word(bytes[end]) {
                        end += 1;
                    }
                    let word = &text[i..end];
                    let at = if word.eq_ignore_ascii_case("END") && level > 0 {
                        level -= 1;
                        Some(level)
                    } else if word.eq_ignore_ascii_case("CASE") {
                        level += 1;
                        Some(level - 1)
                    } else if ["WHEN", "THEN", "ELSE"].iter().any(|k| word.eq_ignore_ascii_case(k)) {
                        Some(level)
                    } else {
                        None
                    };
                    let word_depth = at.unwrap_or(level);
                    if at.is_some() {
                        keywords.push((i, end, word_depth));
                    }
                    for d in &mut depth[i..end] {
                        *d = Some(word_depth);
                    }
                    i = end;
                    continue;
                }
                _ => depth[i] = Some(level),
            }
            i += 1;
        }
        Nesting { depth, keywords }
    }

    /// Whether the byte at `at` is outside every quote, parenthesis and
    /// `CASE` block.
    pub(crate) fn top_level(&self, at: usize) -> bool {
        self.depth.get(at).copied().flatten() == Some(0)
    }
}

/// The offset just past the `END` that closes a `CASE` opening `text`, or
/// `None` when `text` does not begin with one or it never closes.
pub(crate) fn case_block_end(text: &str) -> Option<usize> {
    let nesting = Nesting::of(text);
    let (start, _, depth) = *nesting.keywords.first()?;
    if start != 0 || depth != 0 || !text.as_bytes().get(..4).is_some_and(|w| w.eq_ignore_ascii_case(b"CASE")) {
        return None;
    }
    nesting
        .keywords
        .iter()
        .skip(1)
        .find(|(s, e, d)| *d == 0 && text[*s..*e].eq_ignore_ascii_case("END"))
        .map(|(_, e, _)| *e)
}

/// Every function [`parse`] evaluates from arguments: the names its call
/// dispatch matches on. A `WHERE` hands an operand calling one of them to this
/// evaluator (`predicate_rewrite::lift_evaluated_operands`), as it does a
/// `CASE`; the predicate tokenizer has no functions of its own, and refused
/// `COALESCE(s, 'z') = 'z'` as an unknown column. Kept beside the dispatch, and
/// held to it by `every_listed_function_is_evaluated`.
pub const FUNCTIONS: &[&str] = &[
    "COALESCE", "NULLIF", "LEAST", "GREATEST", "QUOTE_IDENT", "LOWER", "UPPER", "LENGTH", "CHAR_LENGTH",
    "CHARACTER_LENGTH", "OCTET_LENGTH", "DATE_TRUNC", "REPLACE", "ARRAY_LENGTH", "ARRAY_NDIMS",
    "UUID_EXTRACT_TIMESTAMP", "UUID_EXTRACT_VERSION", "ABS", "ROUND", "TRUNC", "DATE_PART", "MOD", "BTRIM", "LTRIM", "RTRIM", "RPAD", "LPAD", "JSONB_TYPEOF",
    "REGEXP_LIKE", "SIMILAR_TO_ESCAPE", "__CHECK_REGEXP_LIKE", "__CHECK_REGEXP_UNCERTAIN",
    "TO_TSVECTOR", "TO_TSQUERY", "PLAINTO_TSQUERY", "PHRASETO_TSQUERY", "WEBSEARCH_TO_TSQUERY", "TS_MATCH_VQ",
    "TS_MATCH_TQ", "TS_MATCH_TT",
];

/// Whether `name` (any case) is a function this evaluator calls: one of
/// [`FUNCTIONS`] or of `pg_functions::NAMES`.
pub fn is_function(name: &str) -> bool {
    FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(name))
        || crate::core::rql::pg_functions::NAMES.iter().any(|f| f.eq_ignore_ascii_case(name))
}

/// PostgreSQL's refusal of a path operator on a column that holds no
/// document: `text -> unknown` does not exist. Read here, the text's bytes
/// were searched as an encoded document, found nothing, and every row
/// answered NULL, so `WHERE s->>'a' IS NULL` matched rows it should have
/// refused.
///
/// Only the types that certainly hold no document are refused. A column a
/// document is stored in under another physical type (`UUID` for a rebuilt
/// one, `JSON`, a scratch relation's sampled type) is still read.
pub(crate) fn path_operator_refusal(column: &SchemaField, text: &str) -> Option<String> {
    use crate::core::rql::type_modifier::TextTypmod;
    if column.json_text {
        return None;
    }
    let name = match column.db_type {
        DbType::STRING if matches!(column.text_typmod(), Some(TextTypmod::Varchar(_))) => "character varying",
        DbType::STRING | DbType::CHAR => "text",
        DbType::BPCHAR(_) => "character",
        DbType::I16 => "smallint",
        DbType::I32 => "integer",
        DbType::I64 => "bigint",
        DbType::F32 => "real",
        DbType::F64 => "double precision",
        DbType::DECIMAL => "numeric",
        DbType::BOOL => "boolean",
        DbType::DATE => "date",
        DbType::DATETIME => "timestamp with time zone",
        DbType::TIMESTAMP => "timestamp without time zone",
        _ => return None,
    };
    // The first operator, past the column (which a quoted name may spell
    // with any character).
    let after_column = match text.trim_start().strip_prefix('"') {
        Some(quoted) => quoted.find('"').map_or("", |end| &quoted[end + 1..]),
        None => text,
    };
    let at = after_column.find(['-', '#'])?;
    let rest = &after_column[at..];
    let operator = ["#>>", "#>", "->>", "->"].into_iter().find(|op| rest.starts_with(op))?;
    Some(format!("operator does not exist: {name} {operator} unknown"))
}

/// `text` as an operand and the path steps after it, when it is a path over
/// something other than a column: the operand a parenthesised group, a cast
/// (`x::jsonb`) or a function call, and everything after it path steps.
/// With the path, its first operator as written, for a refusal to name.
fn path_over_expression(text: &str) -> Option<(&str, (crate::core::jsonb::PathExpr, &'static str))> {
    let bytes = text.as_bytes();
    let (mut depth, mut quote, mut i) = (0usize, None::<u8>, 0usize);
    let at = loop {
        let c = *bytes.get(i)?;
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => depth = depth.checked_sub(1)?,
                b'-' | b'#' if depth == 0 && bytes.get(i + 1) == Some(&b'>') => break i,
                _ => {}
            },
        }
        i += 1;
    };
    let base = text[..at].trim_end();
    if base.is_empty() || !is_path_operand(base) {
        return None;
    }
    let operator = ["#>>", "#>", "->>", "->"].into_iter().find(|op| text[at..].starts_with(op))?;
    // The steps read by the one path parser, over a stand-in column.
    let path = crate::core::jsonb::parse_path_expression(&format!("x{}", &text[at..]))?;
    Some((base, (path, operator)))
}

/// Whether `text` is an operand a path may follow without parentheses being
/// needed around it: a parenthesised group, a function call, or either of
/// those, a column or a string constant cast with `::`. A bare column is
/// not one (a column's own path is read before this), and nor is anything
/// with an operator at its top level.
fn is_path_operand(text: &str) -> bool {
    let text = text.trim();
    let atom = |a: &str| {
        let a = a.trim();
        if a.starts_with('(') {
            return matching_paren(a, 0) == Some(a.len() - 1);
        }
        if a.starts_with('\'') {
            return a.len() >= 2 && a.ends_with('\'') && !a[1..a.len() - 1].replace("''", "").contains('\'');
        }
        let name_end = a.find('(').unwrap_or(a.len());
        let name = &a[..name_end];
        let named = !name.is_empty()
            && name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"');
        named && (name_end == a.len() || matching_paren(a, name_end) == Some(a.len() - 1))
    };
    match find_top_level_cast(text) {
        Some(at) => {
            let target = &text[at + 2..];
            atom(&text[..at])
                && !target.trim().is_empty()
                && target.chars().all(|c| c.is_alphanumeric() || matches!(c, '_' | ' ' | '(' | ')' | ',' | '[' | ']'))
        }
        // A call or a group, not a bare name or constant.
        None => (text.starts_with('(') || text.contains('(')) && atom(text),
    }
}

/// The byte offset of the first `::` outside quotes and parentheses.
fn find_top_level_cast(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let (mut depth, mut quote) = (0usize, None::<u8>);
    for i in 0..bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b':' if depth == 0 && bytes.get(i + 1) == Some(&b':') => return Some(i),
                _ => {}
            },
        }
    }
    None
}

/// PostgreSQL's name for the type of `expr`, as an operator refusal names it.
fn pg_operand_type_name(expr: &Expr) -> String {
    match static_type(expr) {
        _ if yields_json_text(expr) => "json".to_string(),
        DbType::STRING => "text".to_string(),
        DbType::DATETIME if yields_timestamp(expr) => "timestamp without time zone".to_string(),
        other => crate::core::value_codec::pg_input_type_name(&other).to_string(),
    }
}

/// `base[i]` or `base[lo:hi]` when `text` ends in a subscript, parsed;
/// `None` when it does not (`int[]` in a cast is no subscript).
fn parse_subscript(text: &str, schema: &[SchemaField]) -> Result<Option<Expr>, String> {
    use crate::core::rql::pg_functions::PgFunc;
    // Every trailing `[…]` group, right to left: `a[1][2]` is one access with
    // two indexes, as a two-dimensional array is read.
    let mut base_text = text;
    let mut groups: Vec<&str> = Vec::new();
    loop {
        if !base_text.ends_with(']') || array_constructor_body(base_text).is_some() {
            break;
        }
        let bytes = base_text.as_bytes();
        let nesting = Nesting::of(base_text);
        let Some(open) = (1..bytes.len()).find(|&i| {
            bytes[i] == b'['
                && crate::core::sql_scan::matching_close(base_text, i) == Some(base_text.len() - 1)
                && nesting.top_level(i)
        }) else {
            break;
        };
        let inside = base_text[open + 1..base_text.len() - 1].trim();
        let before = base_text[..open].trim_end();
        if inside.is_empty() || before.is_empty() || before.ends_with("::") || (before.contains("::") && !before.ends_with(')') && !before.ends_with(']')) {
            break;
        }
        groups.push(inside);
        base_text = before;
    }
    if groups.is_empty() {
        return Ok(None);
    }
    groups.reverse();
    // `ARRAY[1,2][1]` is a syntax error in PostgreSQL: the constructor is
    // subscripted only in parentheses.
    if array_constructor_body(base_text).is_some() {
        return Err("syntax error at or near \"[\"".to_string());
    }
    let base = parse(base_text, schema)?;
    if static_type(&base) != DbType::ARRAY {
        return Err(format!("cannot subscript type {} because it does not support subscripting", pg_operand_type_name(&base)));
    }
    // One `:` at the top (not `::`): a slice.
    let colon_of = |inside: &str| {
        let ib = inside.as_bytes();
        let nesting = Nesting::of(inside);
        (0..ib.len()).find(|&i| {
            ib[i] == b':' && nesting.top_level(i) && ib.get(i + 1) != Some(&b':') && (i == 0 || ib[i - 1] != b':')
        })
    };
    if groups.iter().any(|g| colon_of(g).is_some()) {
        if groups.len() > 1 {
            return Err("slicing a multidimensional array is not supported yet".to_string());
        }
        let inside = groups[0];
        let at = colon_of(inside).expect("found");
        let bound = |t: &str| -> Result<Expr, String> { if t.trim().is_empty() { Ok(Expr::Null) } else { parse(t.trim(), schema) } };
        let (lo, hi) = (bound(&inside[..at])?, bound(&inside[at + 1..])?);
        // A written NULL bound is a NULL slice; an omitted one is open.
        let written_null = |t: &str| t.trim().eq_ignore_ascii_case("NULL");
        if written_null(&inside[..at]) || written_null(&inside[at + 1..]) {
            return Ok(Some(Expr::Null));
        }
        return Ok(Some(Expr::Call(TextFunc::Pg(PgFunc::ArraySlice), vec![base, lo, hi])));
    }
    let tag = match subscript_element(&base) {
        Some(t) => vec![t.to_byte()],
        None => Vec::new(),
    };
    let mut args = vec![base, Expr::Literal { bytes: tag, db_type: DbType::STRING }];
    for g in groups {
        args.push(parse(g, schema)?);
    }
    Ok(Some(Expr::Call(TextFunc::Pg(PgFunc::ArraySubscript), args)))
}

/// The type an element of the array `base` evaluates to is read back as:
/// a typed array column's element type, or `ARRAY[…]`'s elements' shared
/// one; `None` (text) for anything else, whose elements are text.
fn subscript_element(base: &Expr) -> Option<DbType> {
    use crate::core::rql::type_modifier::TextTypmod;
    match base {
        Expr::Column(f) => match f.text_typmod() {
            Some(TextTypmod::Element(code)) => crate::core::rql::type_modifier::described_element(code)
                .map(DbType::from_byte)
                .and_then(|t| crate::core::value_codec::element_read_type(&t)),
            _ => None,
        },
        Expr::Call(TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArrayCtor), elements) => {
            let int = |t: &DbType| is_integer_type(t);
            let mut types = elements
                .iter()
                .filter(|e| !matches!(e, Expr::Null))
                .map(static_type)
                .map(|t| if int(&t) { DbType::I64 } else { t });
            let first = types.next()?;
            if types.all(|t| t == first) { crate::core::value_codec::element_read_type(&first) } else { None }
        }
        Expr::Call(TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArrayCast), _) => typed_array_element(base)
            .map(DbType::from_byte)
            .and_then(|t| crate::core::value_codec::element_read_type(&t)),
        _ => None,
    }
}

/// The element type code of an array expression that keeps one: a typed
/// array column, or a slice of one (`a[2:3]`). `None` for any other.
pub(crate) fn typed_array_element(expr: &Expr) -> Option<u8> {
    use crate::core::rql::type_modifier::TextTypmod;
    match expr {
        Expr::Column(f) if f.db_type == DbType::ARRAY => match f.text_typmod() {
            Some(TextTypmod::Element(code)) => crate::core::rql::type_modifier::described_element(code),
            _ => None,
        },
        Expr::Call(TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArraySlice), args) => {
            args.first().and_then(typed_array_element)
        }
        Expr::Call(TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArrayCast), args) => match args.get(1) {
            Some(Expr::Literal { bytes, .. }) if !bytes.is_empty() => {
                crate::core::rql::type_modifier::described_element(u32::from(bytes[0]))
            }
            _ => None,
        },
        _ => None,
    }
}

/// The inside of `ARRAY[…]` when `text` is exactly one, else `None`.
pub(crate) fn array_constructor_body(text: &str) -> Option<&str> {
    let t = text.trim();
    // Bytes, not `t[..5]`: a name of multi-byte characters has no character
    // boundary at five (`SELECT "имя" …` panicked here).
    if t.len() < 7 || !t.as_bytes()[..5].eq_ignore_ascii_case(b"ARRAY") {
        return None;
    }
    let open = 5 + (t[5..].len() - t[5..].trim_start().len());
    if t.as_bytes().get(open) != Some(&b'[') {
        return None;
    }
    let close = crate::core::sql_scan::matching_close(t, open)?;
    (close == t.len() - 1).then(|| &t[open + 1..close])
}

/// Parses one expression against `schema`.
pub fn parse(text: &str, schema: &[SchemaField]) -> Result<Expr, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("empty expression".to_string());
    }

    // A wholly parenthesised expression is just the expression: `(a + b)` has
    // to reduce to `a + b` before anything below can recognise it. Only peeled
    // when the opening paren's partner is the very last character, so
    // `(a) + (b)` is left alone.
    if text.starts_with('(') {
        if let Some(close) = matching_paren(text, 0) {
            if close == text.len() - 1 {
                return parse(&text[1..close], schema);
            }
        }
    }

    // `ARRAY[]::integer[]`: an empty array has no type of its own, so it is
    // only one with a cast saying what it is an array of.
    if let Some(rest) = text.strip_prefix("ARRAY[]").or_else(|| text.strip_prefix("array[]"))
        && let Some(target) = rest.trim_start().strip_prefix("::")
        && target.trim_end().ends_with("[]")
    {
        return Ok(Expr::Literal {
            bytes: crate::core::value_codec::parse_array_literal("{}").expect("an empty array"),
            db_type: DbType::ARRAY,
        });
    }
    // `ARRAY[e, …]`: the array constructor, when it is the whole expression.
    if let Some(body) = array_constructor_body(text) {
        if body.trim().is_empty() {
            return Err("cannot determine type of empty array".to_string());
        }
        let elements = crate::core::sql_scan::split_top_level(body, b',');
        // `ARRAY[[1,2],[3,4]]`, the bracket form of a nested constructor.
        if elements.iter().any(|e| e.starts_with('[')) {
            return Err("ARRAY[[…]] is not supported yet; write the inner arrays as ARRAY[…]".to_string());
        }
        let args = elements.iter().map(|e| parse(e, schema)).collect::<Result<Vec<_>, _>>()?;
        return Ok(Expr::Call(TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::ArrayCtor), args));
    }

    // `&`, `|`, `#`, `<<` and `>>` are PostgreSQL's "other operators", one
    // precedence with `||`, looser than arithmetic and grouped left to
    // right: the rightmost of them all splits first (`1 | 2 || 'x'` is
    // `(1 | 2) || 'x'`, `1 << 2 + 1` is 8). They were not read at all, so
    // `flags & 1` was looked up as a column.
    if !is_case_block(text)
        && let Some((at, op)) = top_level_bitwise(text)
        && top_level_concat(text).is_none_or(|c| c < at)
    {
        let lhs = text[..at].trim();
        let rhs = text[at + op.len()..].trim();
        if rhs.is_empty() {
            return Err(format!("'{op}' has no right operand"));
        }
        use crate::core::rql::pg_functions::PgFunc;
        if op == "@@" {
            return Ok(text_match(parse(lhs, schema)?, parse(rhs, schema)?));
        }
        let func = match op {
            "&" => PgFunc::BitAnd,
            "|" => PgFunc::BitOr,
            "#" => PgFunc::BitXor,
            "<<" => PgFunc::ShiftLeft,
            _ => PgFunc::ShiftRight,
        };
        let (mut l, mut r) = (parse(lhs, schema)?, parse(rhs, schema)?);
        // An untyped string literal takes the other operand's type, as
        // PostgreSQL resolves `'6' & 3`; a text value does not (`'a' || 1 |
        // 2` has no operator).
        let untyped = |e: &Expr| matches!(e, Expr::Literal { db_type: DbType::STRING, .. });
        let resolve = |literal: &mut Expr, other: &Expr| {
            let target = match static_type(other) {
                t @ (DbType::I16 | DbType::I32 | DbType::I64 | DbType::BIT | DbType::VARBIT) => t,
                _ => return,
            };
            *literal = Expr::Cast(Box::new(std::mem::replace(literal, Expr::Null)), target);
        };
        if untyped(&l) && !untyped(&r) {
            resolve(&mut l, &r);
        } else if untyped(&r) && !untyped(&l) {
            resolve(&mut r, &l);
        }
        return Ok(Expr::Call(TextFunc::Pg(func), vec![l, r]));
    }

    // `||` and the path operators are one precedence in PostgreSQL, grouped
    // left to right, so the last of them at the top is the root. A path
    // after the last `||` takes everything before it as its base:
    // `'-1' || doc->>'t'` is `('-1' || doc)->>'t'`, and `s || doc->>'t'` is
    // a path over text, refused as PostgreSQL refuses it. With no path after
    // it, the `||` is the root. Read with the path first, both returned the
    // concatenation PostgreSQL never gives.
    if let Some(concat_at) = top_level_concat(text) {
        let nesting = Nesting::of(text);
        let bytes = text.as_bytes();
        let path_at = (concat_at + 2..bytes.len().saturating_sub(1))
            .find(|&i| matches!(bytes[i], b'-' | b'#') && bytes[i + 1] == b'>' && nesting.top_level(i));
        match path_at {
            Some(at) => {
                if let (Some(path), Some(operator)) = (
                    crate::core::jsonb::parse_path_expression(&format!("x{}", &text[at..])),
                    ["#>>", "#>", "->>", "->"].into_iter().find(|op| text[at..].starts_with(op)),
                ) {
                    let base = parse(text[..at].trim_end(), schema)?;
                    if !yields_json_text(&base) && static_type(&base) != DbType::JSONB {
                        return Err(format!(
                            "operator does not exist: {} {} unknown",
                            pg_operand_type_name(&base),
                            operator
                        ));
                    }
                    return Ok(Expr::PathOf { base: Box::new(base), steps: path.steps, as_text: path.as_text });
                }
            }
            None => {
                let lhs = text[..concat_at].trim();
                let rhs = text[concat_at + 2..].trim();
                if !lhs.is_empty() && !rhs.is_empty() {
                    return Ok(Expr::Concat(Box::new(parse(lhs, schema)?), Box::new(parse(rhs, schema)?)));
                }
            }
        }
    }

    // A path into a document binds tighter than any operator below — it is a
    // postfix on a column, not a binary operator over expressions — so it is
    // recognised first. `parse_path_expression` returns `None` for anything
    // that is not one, which is the usual case.
    //
    // A qualified column, `t.doc->>'k'`, is read by its bare name, as a
    // qualified plain column is at the end of this function: the statement
    // has already checked the qualifier names a relation in its FROM, and a
    // join rewrites `a.doc` to its layout's own name before parsing, so two
    // relations' `doc` never meet here under one name. Unread, the whole
    // `t.doc->>'k'` was looked up as a column ("not found in schema"), which
    // failed every ORM query reading a document field.
    let path = crate::core::jsonb::parse_path_expression(text)
        .or_else(|| crate::core::jsonb::parse_qualified_path_expression(text).map(|(_, path)| path));
    if let Some(path) = path {
        if let Some(column) = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(&path.column) && !f.is_deleted)
        {
            if let Some(refused) = path_operator_refusal(column, text) {
                return Err(refused);
            }
            return Ok(Expr::JsonPath {
                column: column.clone(),
                steps: path.steps,
                as_text: path.as_text,
            });
        }
    }

    // A path over a value that is not a column: `(doc)->'a'`,
    // `'{"a":1}'::jsonb->'a'`, `to_jsonb(x)->>0`. `::` binds tighter than
    // `->`, so the cast is the operand rather than the arrow being part of
    // its type name.
    if let Some((base, path)) = path_over_expression(text) {
        let base = parse(base, schema)?;
        let json_text = yields_json_text(&base);
        if !json_text && static_type(&base) != DbType::JSONB {
            let operand = match &base {
                Expr::Literal { db_type: DbType::STRING, .. } => "unknown".to_string(),
                other => pg_operand_type_name(other),
            };
            return Err(format!("operator does not exist: {} {} unknown", operand, path.1));
        }
        return Ok(Expr::PathOf { base: Box::new(base), steps: path.0.steps, as_text: path.0.as_text });
    }

    // `||` binds looser than arithmetic, so `a || b + 1` concatenates `a` with
    // `b + 1` rather than adding 1 to a concatenation.
    if let Some(at) = top_level_concat(text) {
        let lhs = text[..at].trim();
        let rhs = text[at + 2..].trim();
        if !lhs.is_empty() && !rhs.is_empty() {
            return Ok(Expr::Concat(
                Box::new(parse(lhs, schema)?),
                Box::new(parse(rhs, schema)?),
            ));
        }
    }

    // A prefix `~` (bitwise NOT) is an "other operator" too, so it takes the
    // arithmetic after it: `~1 + 1` is `~2`, -3, as PostgreSQL's. Not `~*`
    // or `~~`, which are matches and need a left operand anyway.
    if let Some(rest) = text.strip_prefix('~')
        && !rest.starts_with(['*', '~', '='])
        && !rest.trim().is_empty()
    {
        return Ok(Expr::Call(
            TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::BitNot),
            vec![parse(rest.trim(), schema)?],
        ));
    }

    // Arithmetic binds loosest of all — looser than a cast, so `a::INT + 1`
    // adds to a cast value rather than casting a sum.
    //
    // Two shapes are deliberately not split. A whole-text number is a literal,
    // not a subtraction: `-5` and `1e-5` both contain a `-` that is part of the
    // number. And a `CASE … END` block keeps its arms at paren depth zero, so a
    // `+` inside one looks top-level from out here; splitting there would tear
    // the block in half.
    if !is_number_literal(text) && !is_case_block(text) {
        if let Some((at, op)) = top_level_arith(text) {
            let lhs = text[..at].trim();
            let rhs = text[at + 1..].trim();
            if lhs.is_empty() {
                return Err(format!("'{}' has no left operand", op.symbol()));
            }
            if rhs.is_empty() {
                return Err(format!("'{}' has no right operand", op.symbol()));
            }
            let (l, r) = (date_operand(parse(lhs, schema)?), date_operand(parse(rhs, schema)?));
            if let Some(exact) = exact_literal_arith(&l, op, &r, text) {
                return exact;
            }
            return Ok(fold_float_literal_into_decimal(Expr::Arith(Box::new(l), op, Box::new(r))));
        }

        // `^` binds tighter than `*` and looser than a unary sign, and groups
        // left to right, as PostgreSQL's does: `2 * 3 ^ 2` is 18, `-2 ^ 2` is
        // 4, `2 ^ 3 ^ 2` is 64. It means `power(a, b)`.
        if let Some(at) = top_level_power(text) {
            let lhs = text[..at].trim();
            let rhs = text[at + 1..].trim();
            if rhs.is_empty() {
                return Err("'^' has no right operand".to_string());
            }
            return Ok(Expr::Call(
                TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::Power),
                vec![parse(lhs, schema)?, parse(rhs, schema)?],
            ));
        }

        // No binary operator is left at the top, so a leading sign is unary:
        // `-n`, `-(a + b)`. It binds looser than a cast, as PostgreSQL's does —
        // `-n::bigint` negates the cast value. `-n` used to reach the column
        // resolver whole and be refused as an unknown column.
        if let Some(rest) = text.strip_prefix('+') {
            return parse(rest, schema);
        }
        if let Some(rest) = text.strip_prefix('-') {
            if !rest.trim_start().starts_with('-') {
                return negate(parse(rest, schema)?);
            }
        }
    }

    // A trailing cast binds loosest, so it is peeled first — but not one
    // inside a `CASE … END` block, whose arms sit at depth zero: `CASE … ELSE
    // 1::text END` was split at that `::`, and the cast target came out as
    // `text END`. A cast *after* the block leaves text that no longer ends in
    // `END`, so it is still peeled.
    if let Some(at) = top_level_cast(text).filter(|_| !is_case_block(text)) {
        let inner = &text[..at];
        let target = &text[at + 2..];
        return cast_to(parse(inner, schema)?, target);
    }

    let upper = text.to_ascii_uppercase();

    if upper == "NULL" {
        return Ok(Expr::Null);
    }
    if upper == "TRUE" {
        return Ok(Expr::Literal {
            bytes: vec![1u8],
            db_type: DbType::BOOL,
        });
    }
    if upper == "FALSE" {
        return Ok(Expr::Literal {
            bytes: vec![0u8],
            db_type: DbType::BOOL,
        });
    }

    if upper.starts_with("CASE ") || upper == "CASE" {
        return parse_case(text, schema);
    }

    // A quoted string, and only when the opening quote's partner is the last
    // byte. Starting and ending with a quote is not enough: `'DE', 'A'` was
    // read as the single string `DE', 'A` (breaking the row value
    // `(country, grade) = ('DE', 'A')`), `'x' = 'y'` as the one literal
    // `x' = 'y`, and `a || 'x' = 'ax'` as `a` concatenated with `x' = 'ax`.
    if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 && is_one_literal(text) {
        let value = crate::core::value_codec::unquote_sql(text);
        return Ok(Expr::Literal {
            bytes: value.as_bytes().to_vec(),
            db_type: DbType::STRING,
        });
    }

    // `B'1010'` and `X'1f'`, PostgreSQL's bit-string literals, typed `bit`
    // as there. Without this a `CASE` arm holding one could not be read, and
    // the whole item was refused as an unsupported `CASE`.
    if let Some(rest) = text.strip_prefix(['B', 'b', 'X', 'x']) {
        if rest.len() >= 2 && rest.starts_with('\'') && is_one_literal(rest) {
            let bytes = crate::core::pg_types::parse_bit_string(text, None)
                .ok_or_else(|| format!("{} is not a valid bit string", text))?;
            return Ok(Expr::Literal {
                bytes,
                db_type: DbType::BIT,
            });
        }
    }

    // A number. An integer constant is `int4` when it fits and `int8` when it
    // does not, as PostgreSQL types it. Every one used to be `int8`, and the
    // arithmetic over it then disagreed with the type the result was described
    // as: `n * 3000000000` was announced as `int4`, computed in `int8`, and
    // sent as the low four bytes of the product.
    if let Ok(v) = text.parse::<i64>() {
        return Ok(match i32::try_from(v) {
            Ok(narrow) => Expr::Literal {
                bytes: narrow.to_le_bytes().to_vec(),
                db_type: DbType::I32,
            },
            Err(_) => Expr::Literal {
                bytes: v.to_le_bytes().to_vec(),
                db_type: DbType::I64,
            },
        });
    }
    // A constant with a point or an exponent is `numeric`, not `float8`, so
    // arithmetic on it is exact: `14 * 1.37 + 0.99` is 20.17, where the float
    // it used to be gave 20.169999999999998. Past nine places it is carried
    // with its digits at its own scale (`numeric_constant`); too large for
    // that, it stays a float. Rounding it to nine places lost digits — `1e-10`
    // became 0 in a float column, and `f * 0.0000000001` was 0 — and rounded
    // it a second time when it was then stored at a column's scale, so
    // 20.1649999999 became 20.165 and then 20.17.
    if let Ok(v) = text.parse::<f64>() {
        if text.starts_with(|c: char| c.is_ascii_digit() || matches!(c, '.' | '-' | '+')) {
            if let Some(units) = exact_engine_decimal(text) {
                let literal = Expr::Literal {
                    bytes: units.to_le_bytes().to_vec(),
                    db_type: DbType::DECIMAL,
                };
                return Ok(with_written_scale(literal, text));
            }
            // Not exact at the engine scale: a float or a rounded NUMERIC,
            // whichever keeps the written number (see `numeric_constant`).
            // `123456789012345678901234567890123456.50` was the float
            // 1.2345678901234568e35 and was stored as
            // 123456789012345680000000000000000000.00; it is refused now.
            if text.bytes().any(|b| b.is_ascii_digit()) {
                return match decimal_digits(text) {
                    Some((m, e)) => numeric_constant(m, e, text),
                    // More digits than an i128 holds: a float keeps the
                    // leading ones, as much as a small value has.
                    None if v.is_finite() && v.abs() < FLOAT_KEEPS_BELOW => Ok(Expr::Literal {
                        bytes: v.to_le_bytes().to_vec(),
                        db_type: DbType::F64,
                    }),
                    None => Err(inexact_literal(text)),
                };
            }
        }
        return Ok(Expr::Literal {
            bytes: v.to_le_bytes().to_vec(),
            db_type: DbType::F64,
        });
    }

    // `INTERVAL '1 day'`, as a real `INTERVAL` value.
    //
    // It used to be a count of microseconds typed `I64`, which read the same
    // for a day or an hour and could not represent a month at all — a month is
    // not a fixed number of microseconds, so `INTERVAL '1 month'` was refused.
    // Keeping the three fields apart is what lets the arithmetic move the
    // calendar; see `pg_types::shift_timestamp`.
    if upper.starts_with("INTERVAL") {
        let rest = text["INTERVAL".len()..].trim();
        if rest.starts_with('\'') && rest.ends_with('\'') && rest.len() >= 2 {
            let body = &rest[1..rest.len() - 1];
            if let Some(span) = crate::core::pg_types::parse_interval(body) {
                return Ok(Expr::Literal {
                    bytes: crate::core::pg_types::encode_interval(span),
                    db_type: DbType::INTERVAL,
                });
            }
            // The older reader, kept as the fallback so a spelling it accepted
            // and the new one does not is still understood rather than newly
            // refused.
            let micros = interval_micros(body)?;
            return Ok(Expr::Literal {
                bytes: crate::core::pg_types::encode_interval(crate::core::pg_types::Interval {
                    months: 0,
                    days: 0,
                    micros,
                }),
                db_type: DbType::INTERVAL,
            });
        }
        return Err(format!(
            "INTERVAL needs a quoted value, as in `INTERVAL '1 day'`; got '{}'",
            text
        ));
    }

    // `a[i]`, `a[lo:hi]`, `(ARRAY[…])[i]`: a subscript binds tightest, so it
    // is read here, once the operators around it have been split off.
    if let Some(expr) = parse_subscript(text, schema)? {
        return Ok(expr);
    }

    // A function call.
    if let Some(open) = text.find('(') {
        let name = text[..open].trim();
        if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            let close = matching_paren(text, open)
                .ok_or_else(|| format!("unterminated call in '{}'", text))?;
            if close + 1 != text.len() {
                return Err(format!("trailing text after '{}'", &text[..close + 1]));
            }
            let args_text = &text[open + 1..close];

            // `make_interval` is handled before the arguments are parsed as
            // expressions: its arguments are `name => value` pairs, which are
            // not expressions and would fail there first.
            if name.eq_ignore_ascii_case("make_interval") {
                let mut micros = 0i64;
                for arg in split_args(args_text) {
                    let arg = arg.trim();
                    let Some(at) = arg.find("=>") else {
                        return Err(format!(
                            "make_interval takes named arguments, as in \
                             `make_interval(secs => 30)`; got '{}'",
                            arg
                        ));
                    };
                    let unit = arg[..at].trim();
                    // A bound parameter can arrive quoted — PostgreSQL casts
                    // `'120'` to the argument's type, so the quotes are stripped
                    // rather than treated as a different value.
                    let amount_text = arg[at + 2..].trim().trim_matches('\'');
                    let amount: i64 = amount_text.parse().map_err(|_| {
                        format!("make_interval needs a whole number for '{}'", unit)
                    })?;
                    micros = micros
                        .checked_add(
                            unit_micros(unit)?
                                .checked_mul(amount)
                                .ok_or_else(|| "interval is too large".to_string())?,
                        )
                        .ok_or_else(|| "interval is too large".to_string())?;
                }
                // An interval, not a bare count of microseconds: a count is an
                // integer, and an integer next to a date is a number of days.
                return Ok(Expr::Literal {
                    bytes: crate::core::pg_types::encode_interval(crate::core::pg_types::Interval {
                        months: 0,
                        days: 0,
                        micros,
                    }),
                    db_type: DbType::INTERVAL,
                });
            }

            // `CAST(x AS type)` is the standard spelling of `x::type`, and
            // like `EXTRACT` it separates its arguments with a keyword rather
            // than a comma — so it has to be read before the argument splitter,
            // which would hand `x AS type` to `parse` as one expression.
            //
            // Compiled to the same `Expr::Cast` the `::` form produces, so
            // there is one implementation of what a cast does rather than two
            // that could disagree.
            if name.eq_ignore_ascii_case("cast") {
                let inner = args_text.trim();
                let at = crate::core::rql::join::find_keyword(inner, "AS", 0).ok_or_else(|| {
                    format!(
                        "CAST needs a value and a type, as in `CAST(x AS bigint)`; got '{}'",
                        inner
                    )
                })?;
                let value = parse(inner[..at].trim(), schema)?;
                let type_name = inner[at + "AS".len()..].trim();
                return cast_to(value, type_name);
            }

            // `EXTRACT(EPOCH FROM x)` separates its two arguments with `FROM`,
            // not a comma, so it is read before the argument splitter — which
            // would hand the whole of `EPOCH FROM x` to `parse` as one
            // expression and fail on `EPOCH` as an unknown column. That is
            // exactly what it did: "relation does not exist", about a statement
            // naming no relation.
            if name.eq_ignore_ascii_case("extract") {
                let inner = args_text.trim();
                let at =
                    crate::core::rql::join::find_keyword(inner, "FROM", 0).ok_or_else(|| {
                        format!(
                            "EXTRACT needs a field and a value, as in \
                         `EXTRACT(EPOCH FROM ts)`; got '{}'",
                            inner
                        )
                    })?;
                let field = inner[..at].trim().trim_matches('\'').to_ascii_uppercase();
                let source = parse(inner[at + "FROM".len()..].trim(), schema)?;
                // A `numeric`, as PostgreSQL 14 and later return every field,
                // `EPOCH` included (six places); `date_part` is the `double
                // precision` spelling. `EPOCH` answered a double here, which
                // printed 86400 where PostgreSQL prints 86400.000000.
                let canonical = date_field(&field)?;
                date_field_applies(canonical, &static_type(&source), true)?;
                return Ok(Expr::Call(
                    TextFunc::ExtractField,
                    vec![Expr::Literal { bytes: canonical.as_bytes().to_vec(), db_type: DbType::STRING }, source],
                ));
            }

            // `pg_column_size(t)` names a *relation*, which resolves to no
            // column — the answer is the whole row's width. Parsed specially so
            // the argument's failure to resolve is not the function's failure.
            if name.eq_ignore_ascii_case("pg_column_size")
                && parse(args_text.trim(), schema).is_err()
            {
                return Ok(Expr::Call(TextFunc::ColumnSize, Vec::new()));
            }

            // `SUBSTRING(s FROM 2 FOR 3)`, `POSITION(x IN y)`, `TRIM(LEADING
            // 'x' FROM s)`: the standard spellings, as the calls they mean.
            let standard = crate::core::rql::pg_functions::standard_syntax(&name.to_ascii_uppercase(), args_text);
            let (name, args_text) = match &standard {
                Some((name, args)) => (name.as_str(), args.as_str()),
                None => (name, args_text),
            };
            let args: Vec<Expr> = if args_text.trim().is_empty() {
                Vec::new()
            } else {
                split_args(args_text)
                    .iter()
                    .map(|a| parse(a, schema))
                    .collect::<Result<_, _>>()?
            };

            let upper_name = name.to_ascii_uppercase();
            let mut args = args;
            if matches!(
                upper_name.as_str(),
                "COALESCE" | "LEAST" | "GREATEST" | "NULLIF"
            ) {
                unify_literal_types(&mut args);
            }
            return match upper_name.as_str() {
                "COALESCE" => {
                    if args.is_empty() {
                        return Err("COALESCE needs at least one argument".to_string());
                    }
                    check_branch_types("COALESCE", args.iter())?;
                    blank_pad_text_branches(args.iter_mut());
                    Ok(Expr::Coalesce(args))
                }
                "NULLIF" => {
                    if args.len() != 2 {
                        return Err("NULLIF takes exactly two arguments".to_string());
                    }
                    let mut it = args.into_iter();
                    Ok(Expr::NullIf(
                        Box::new(it.next().expect("checked")),
                        Box::new(it.next().expect("checked")),
                    ))
                }
                "LEAST" => {
                    if args.is_empty() {
                        return Err("LEAST needs at least one argument".to_string());
                    }
                    check_branch_types("LEAST", args.iter())?;
                    blank_pad_text_branches(args.iter_mut());
                    Ok(Expr::Least(args))
                }
                "GREATEST" => {
                    if args.is_empty() {
                        return Err("GREATEST needs at least one argument".to_string());
                    }
                    check_branch_types("GREATEST", args.iter())?;
                    blank_pad_text_branches(args.iter_mut());
                    Ok(Expr::Greatest(args))
                }
                "QUOTE_IDENT" | "LOWER" | "UPPER" => {
                    if args.len() != 1 {
                        return Err(format!("{} takes exactly one argument", upper_name));
                    }
                    let func = match upper_name.as_str() {
                        "QUOTE_IDENT" => TextFunc::QuoteIdent,
                        "LOWER" => TextFunc::Lower,
                        _ => TextFunc::Upper,
                    };
                    Ok(Expr::Call(func, args))
                }
                "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" => {
                    if args.len() != 1 {
                        return Err(format!("{} takes exactly one argument", upper_name.to_lowercase()));
                    }
                    let func = if upper_name == "OCTET_LENGTH" {
                        TextFunc::OctetLength
                    } else {
                        TextFunc::Length
                    };
                    Ok(Expr::Call(func, args))
                }
                "CURRENT_SCHEMA" => {
                    if !args.is_empty() {
                        return Err("current_schema() takes no arguments".to_string());
                    }
                    Ok(Expr::Call(TextFunc::CurrentSchema, Vec::new()))
                }
                "DATE_TRUNC" => {
                    if args.len() != 2 {
                        return Err("date_trunc takes a unit and a timestamp".to_string());
                    }
                    Ok(Expr::Call(TextFunc::DateTrunc, args))
                }
                "DATE_PART" => {
                    if args.len() != 2 {
                        return Err("date_part takes a field and a timestamp, date or interval".to_string());
                    }
                    let Expr::Literal { bytes, db_type: DbType::STRING } = &args[0] else {
                        // A field read from the row (`date_part(unit, ts)`):
                        // checked per value when evaluated. The source's type
                        // is checked now, with a field every type has.
                        if !matches!(static_type(&args[0]), DbType::STRING | DbType::CHAR | DbType::BPCHAR(_) | DbType::NULL) {
                            return Err(format!(
                                "function date_part({}, …) does not exist",
                                static_type(&args[0]).to_string().to_lowercase()
                            ));
                        }
                        date_field_applies("YEAR", &static_type(&args[1]), false)?;
                        return Ok(Expr::Call(TextFunc::DatePart, args));
                    };
                    let canonical = date_field(&String::from_utf8_lossy(bytes).to_ascii_uppercase())?;
                    date_field_applies(canonical, &static_type(&args[1]), false)?;
                    let mut args = args;
                    args[0] = Expr::Literal { bytes: canonical.as_bytes().to_vec(), db_type: DbType::STRING };
                    Ok(Expr::Call(TextFunc::DatePart, args))
                }
                "REPLACE" => {
                    if args.len() != 3 {
                        return Err("replace takes three arguments".to_string());
                    }
                    Ok(Expr::Call(TextFunc::Replace, args))
                }
                // Full-text search. The configuration argument is required
                // here, where PostgreSQL also offers a one-argument form that
                // reads `default_text_search_config` — a setting this server
                // does not have, so asking for it explicitly is the honest
                // spelling rather than one that silently assumes English.
                // One argument reads `default_text_search_config`, which
                // initdb sets to `english` for English and C locales; that is
                // this server's.
                "TO_TSVECTOR" | "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY" => {
                    let mut args = args;
                    match args.len() {
                        1 => args.insert(0, Expr::Literal { bytes: b"english".to_vec(), db_type: DbType::STRING }),
                        2 => {}
                        n => {
                            return Err(format!(
                                "function {}({}) does not exist",
                                upper_name.to_ascii_lowercase(),
                                vec!["unknown"; n].join(", ")
                            ))
                        }
                    }
                    let func = match upper_name.as_str() {
                        "TO_TSVECTOR" => TextFunc::ToTsVector,
                        "TO_TSQUERY" => TextFunc::ToTsQuery,
                        "PLAINTO_TSQUERY" => TextFunc::PlainToTsQuery,
                        "PHRASETO_TSQUERY" => TextFunc::PhraseToTsQuery,
                        _ => TextFunc::WebsearchToTsQuery,
                    };
                    Ok(Expr::Call(func, args))
                }
                "TS_MATCH_VQ" | "TS_MATCH_TQ" | "TS_MATCH_TT" => {
                    if args.len() != 2 {
                        return Err(format!("function {}() takes two arguments", upper_name.to_ascii_lowercase()));
                    }
                    let func = match upper_name.as_str() {
                        "TS_MATCH_VQ" => TextFunc::TsMatchVq,
                        "TS_MATCH_TQ" => TextFunc::TsMatchTq,
                        _ => TextFunc::TsMatchTt,
                    };
                    Ok(Expr::Call(func, args))
                }
                "ARRAY_LENGTH" => {
                    if args.len() != 2 {
                        return Err("array_length takes an array and a dimension".to_string());
                    }
                    Ok(Expr::Call(TextFunc::ArrayLength, args))
                }
                "ARRAY_NDIMS" => {
                    if args.len() != 1 {
                        return Err("array_ndims takes exactly one argument".to_string());
                    }
                    Ok(Expr::Call(TextFunc::ArrayNdims, args))
                }
                "UUID_EXTRACT_TIMESTAMP" | "UUID_EXTRACT_VERSION" => {
                    if args.len() != 1 {
                        return Err(format!("{} takes exactly one argument", upper_name));
                    }
                    let func = if upper_name == "UUID_EXTRACT_TIMESTAMP" {
                        TextFunc::UuidExtractTimestamp
                    } else {
                        TextFunc::UuidExtractVersion
                    };
                    Ok(Expr::Call(func, args))
                }
                "PG_COLUMN_SIZE" => {
                    if args.len() != 1 {
                        return Err("pg_column_size takes exactly one argument".to_string());
                    }
                    Ok(Expr::Call(TextFunc::ColumnSize, args))
                }
                "PG_TOTAL_RELATION_SIZE" | "PG_RELATION_SIZE" => {
                    if args.len() != 1 {
                        return Err(format!("{} takes exactly one argument", upper_name));
                    }
                    Ok(Expr::Call(TextFunc::RelationSize, args))
                }
                "ABS" => {
                    if args.len() != 1 {
                        return Err("abs takes exactly one argument".to_string());
                    }
                    Ok(Expr::Call(TextFunc::Abs, args))
                }
                "ROUND" | "TRUNC" => {
                    let mut args = args;
                    let fname = upper_name.to_lowercase();
                    let func = if upper_name == "TRUNC" { TextFunc::Trunc } else { TextFunc::Round };
                    if !(1..=2).contains(&args.len()) {
                        return Err(format!("{fname} takes a number and optionally the places"));
                    }
                    // An untyped string constant is a numeric one, as
                    // PostgreSQL resolves `round('1.25', 1)`.
                    if matches!(&args[0], Expr::Literal { db_type: DbType::STRING, .. }) {
                        let literal = args.remove(0);
                        args.insert(0, Expr::Cast(Box::new(literal), DbType::DECIMAL));
                    }
                    let ty = static_type(&args[0]);
                    let pg_name = |t: &DbType| match t {
                        DbType::F32 | DbType::F64 => "double precision".to_string(),
                        DbType::DECIMAL => "numeric".to_string(),
                        DbType::STRING => "text".to_string(),
                        DbType::BOOL => "boolean".to_string(),
                        t if is_integer_type(t) => "integer".to_string(),
                        t => t.to_string().to_lowercase(),
                    };
                    let numeric = matches!(ty, DbType::DECIMAL | DbType::F32 | DbType::F64) || is_integer_type(&ty);
                    if !numeric || (args.len() == 2 && matches!(ty, DbType::F32 | DbType::F64)) {
                        let list: Vec<String> = args.iter().map(|a| pg_name(&static_type(a))).collect();
                        return Err(format!("function {fname}({}) does not exist", list.join(", ")));
                    }
                    if args.len() == 2 && !columns_of(&args[1]).is_empty() {
                        // Places read from the row: evaluated per row, each
                        // value at its own scale (`round(v, n)` shows 0.0,
                        // 10.50, 0.000 down a column, as PostgreSQL does).
                        let places_ty = static_type(&args[1]);
                        if !is_integer_type(&places_ty) {
                            return Err(format!("function {fname}(numeric, {}) does not exist", pg_name(&places_ty)));
                        }
                    } else if args.len() == 2 {
                        let places = eval(&args[1], &Row::default(), &[])?;
                        args[1] = match places {
                            Value::Null => Expr::Null,
                            value => {
                                let p = as_i64(&value).ok_or_else(|| {
                                    format!("function {fname}(numeric, {}) does not exist", pg_name(&static_type(&args[1])))
                                })?;
                                // Past either end every answer is the same.
                                Expr::Literal {
                                    bytes: (p.clamp(-1000, 1000) as i32).to_le_bytes().to_vec(),
                                    db_type: DbType::I32,
                                }
                            }
                        };
                    }
                    Ok(Expr::Call(func, args))
                }
                "BTRIM" | "LTRIM" | "RTRIM" => {
                    if !(1..=2).contains(&args.len()) {
                        return Err(format!("{} takes a text and optionally the characters to trim", upper_name.to_lowercase()));
                    }
                    let func = match upper_name.as_str() {
                        "BTRIM" => TextFunc::Btrim,
                        "LTRIM" => TextFunc::Ltrim,
                        _ => TextFunc::Rtrim,
                    };
                    Ok(Expr::Call(func, args))
                }
                "RPAD" | "LPAD" => {
                    if !(2..=3).contains(&args.len()) {
                        return Err(format!("{} takes a text, a length and optionally a fill", upper_name.to_lowercase()));
                    }
                    Ok(Expr::Call(if upper_name == "RPAD" { TextFunc::Rpad } else { TextFunc::Lpad }, args))
                }
                "JSONB_TYPEOF" | "JSON_TYPEOF" => {
                    if args.len() != 1 {
                        return Err(format!("{} takes exactly one argument", upper_name.to_ascii_lowercase()));
                    }
                    Ok(Expr::Call(TextFunc::JsonbTypeof, args))
                }
                "REGEXP_LIKE" | "__CHECK_REGEXP_LIKE" | "__CHECK_REGEXP_UNCERTAIN" => {
                    if !(2..=3).contains(&args.len()) {
                        return Err("regexp_like takes a text, a pattern and optionally flags".to_string());
                    }
                    let func = match upper_name.as_str() {
                        "REGEXP_LIKE" => TextFunc::RegexpLike,
                        "__CHECK_REGEXP_LIKE" => TextFunc::CheckRegexpLike,
                        _ => TextFunc::CheckRegexpUncertain,
                    };
                    Ok(Expr::Call(func, args))
                }
                "SIMILAR_TO_ESCAPE" => {
                    if !(1..=2).contains(&args.len()) {
                        return Err("similar_to_escape takes a pattern and optionally an escape".to_string());
                    }
                    Ok(Expr::Call(TextFunc::SimilarToEscape, args))
                }
                "MOD" => {
                    if args.len() != 2 {
                        return Err("MOD takes exactly two arguments".to_string());
                    }
                    let mut it = args.into_iter();
                    let (a, b) = (it.next().expect("checked"), it.next().expect("checked"));
                    // Over a NUMERIC or a float it is the `%` operator's
                    // remainder, which carries the scale: the integer node
                    // refused `mod(price, 3)` ("MOD needs integer arguments")
                    // where `price % 3` answered.
                    if integer_typed(&a) && integer_typed(&b) {
                        Ok(Expr::Mod(Box::new(a), Box::new(b)))
                    } else {
                        Ok(Expr::Arith(Box::new(a), ArithOp::Rem, Box::new(b)))
                    }
                }
                // `pg_typeof(x)`: the type `x` is, as PostgreSQL names it.
                "PG_TYPEOF" if args.len() == 1 => {
                    let name = match static_type(&args[0]) {
                        _ if yields_json_text(&args[0]) => "json",
                        DbType::DATETIME if yields_timestamp(&args[0]) => "timestamp without time zone",
                        DbType::DATETIME => "timestamp with time zone",
                        DbType::TIMESTAMP => "timestamp without time zone",
                        DbType::STRING => "text",
                        DbType::ARRAY => "text[]",
                        DbType::JSONB => "jsonb",
                        DbType::NULL => "unknown",
                        other => crate::core::value_codec::pg_input_type_name(&other),
                    };
                    Ok(Expr::Literal { bytes: name.as_bytes().to_vec(), db_type: DbType::STRING })
                }
                _ => {
                    if let Some(found) = crate::core::rql::pg_functions::lookup(&upper_name, args.len()) {
                        return Ok(Expr::Call(TextFunc::Pg(found?), args));
                    }
                    if let Some((func, _)) = crate::core::rql::aggregate::ScalarFunc::parse(name) {
                        if !args_text.trim().is_empty() {
                            return Err(format!("{}() takes no arguments", name));
                        }
                        return Ok(Expr::Scalar(func));
                    }
                    Err(format!("function '{}' is not supported yet", name))
                }
            };
        }
    }

    // A column reference, qualified or not.
    let bare = text.rsplit('.').next().unwrap_or(text).trim_matches('"');
    match schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
    {
        Some(field) => Ok(Expr::Column(field.clone())),
        // Reached only once no column answers to the name, so a table that does
        // carry a column called `current_timestamp` still reads its own data.
        None => match keyword_constant(text) {
            Some(expr) => Ok(expr),
            None => Err(format!("'{}' is neither a column nor a value", text)),
        },
    }
}

/// Orders two rows by their already-evaluated `ORDER BY` key values.
///
/// `directions` is `(descending, nulls_first)` per key, in significance order.
/// Shared by the single-table expression sort and the join's, so that
/// `ORDER BY <expr>` means one thing: two comparators that disagree about NULL
/// placement produce wrong rows, not slow ones.
///
/// NULL placement defaults to NULLs largest — last ascending, first descending
/// — and an explicit placement sits outside the descending reversal, which is
/// what `NULLS FIRST` on a `DESC` key has to mean.
pub fn compare_key_values(
    a: &[Value],
    b: &[Value],
    directions: &[(bool, Option<bool>)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (i, (descending, nulls_first)) in directions.iter().enumerate() {
        let (Some(av), Some(bv)) = (a.get(i), b.get(i)) else {
            continue;
        };
        let nulls_come_first = nulls_first.unwrap_or(*descending);
        let ord = match (av.is_null(), bv.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_come_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_come_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                // Incomparable values keep their existing order rather than
                // panicking the comparator.
                let ord = compare_total(av, bv).unwrap_or(Ordering::Equal);
                if *descending { ord.reverse() } else { ord }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// The SQL value keywords spelled without parentheses.
///
/// `CURRENT_TIMESTAMP` is a value, not a call, so every path that recognises a
/// function by its `(` misses it: `WHERE next_attempt_at <= CURRENT_TIMESTAMP`
/// reached the predicate tokenizer as the unknown column `CURRENT_TIMESTAMP`
/// while the identical `now()` worked.
///
/// Quoted and qualified names are left alone. PostgreSQL reserves these words,
/// so a column may only be named one of them by quoting it, and `t.x` is always
/// a column of `t` — folding either would replace stored data with the clock.
fn keyword_constant(text: &str) -> Option<Expr> {
    let trimmed = text.trim();
    if trimmed.contains('"') || trimmed.contains('.') {
        return None;
    }
    let now = crate::core::rql::aggregate::ScalarFunc::Now;
    match trimmed.to_ascii_uppercase().as_str() {
        // The transaction's start (`crate::core::statement_clock`), as
        // `now()` is.
        "CURRENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" => Some(Expr::Scalar(now)),
        // The same instant as a `timestamp` (without time zone): shown with
        // no offset (`yields_timestamp`).
        "LOCALTIMESTAMP" => Some(Expr::Cast(Box::new(Expr::Scalar(now)), DbType::TIMESTAMP)),
        "CURRENT_DATE" => Some(Expr::Scalar(
            crate::core::rql::aggregate::ScalarFunc::CurrentDate,
        )),
        // `CURRENT_TIME` and `LOCALTIME` are deliberately absent: there is no
        // constant that encodes a bare time of day here, and a wrong instant is
        // worse than an honest "not supported". They still fail by name.
        _ => None,
    }
}

/// `CASE WHEN <cond> THEN <expr> [WHEN …] [ELSE <expr>] END`
fn parse_case(text: &str, schema: &[SchemaField]) -> Result<Expr, String> {
    let end = case_block_end(text).ok_or_else(|| "CASE has no END".to_string())?;
    if text[end..].trim() != "" {
        return Err("trailing text after END".to_string());
    }
    let end_at = end - "END".len();

    // The block's own keywords are the ones one level in. A nested block's
    // sit deeper, so an inner `END` or `WHEN` no longer ends or splits this
    // one — the first `END` anywhere used to be taken as this block's.
    let keywords: Vec<(usize, usize, &str)> = Nesting::of(text)
        .keywords
        .into_iter()
        .filter(|(s, _, d)| *d == 1 && *s > 0 && *s < end_at)
        .map(|(s, e, _)| (s, e, &text[s..e]))
        .filter(|(_, _, w)| ["WHEN", "THEN", "ELSE"].iter().any(|k| w.eq_ignore_ascii_case(k)))
        .collect();

    // A `CASE <expr> WHEN …` compares against a value; only the searched form
    // is supported, and the difference is worth an explicit error.
    let Some(&(first_when, _, first)) = keywords.first() else {
        return Err("CASE has no WHEN".to_string());
    };
    if !first.eq_ignore_ascii_case("WHEN") {
        return Err("CASE has no WHEN".to_string());
    }
    // The simple form, `CASE x WHEN v THEN …`: each arm is `x = v`, so a
    // NULL `x` takes no arm, as in PostgreSQL. `x` is read once per arm.
    let subject = text[4..first_when].trim();
    let subject = (!subject.is_empty()).then_some(subject);

    let mut whens: Vec<(Cond, Expr)> = Vec::new();
    let mut otherwise: Option<Box<Expr>> = None;
    // Where each arm's text ends: at the next keyword, or at the block's END.
    let until = |k: usize| keywords.get(k + 1).map(|(s, _, _)| *s).unwrap_or(end_at);
    let mut k = 0usize;
    while k < keywords.len() {
        let (_, after, word) = keywords[k];
        if word.eq_ignore_ascii_case("ELSE") {
            if k + 1 != keywords.len() {
                return Err("CASE has text after its ELSE that is not part of it".to_string());
            }
            otherwise = Some(Box::new(parse(&text[after..end_at], schema)?));
            break;
        }
        if !word.eq_ignore_ascii_case("WHEN") {
            return Err(format!("CASE has a {} where a WHEN belongs", word.to_ascii_uppercase()));
        }
        let Some(&(then_at, then_end, then)) = keywords.get(k + 1) else {
            return Err("CASE WHEN has no THEN".to_string());
        };
        if !then.eq_ignore_ascii_case("THEN") {
            return Err("CASE WHEN has no THEN".to_string());
        }
        let condition = match subject {
            None => parse_cond(&text[after..then_at], schema)?,
            Some(subject) => {
                Cond::Cmp(parse_condition(&format!("({subject}) = ({})", text[after..then_at].trim()), schema)?)
            }
        };
        whens.push((condition, parse(&text[then_end..until(k + 1)], schema)?));
        k += 2;
    }

    check_branch_types(
        "CASE",
        whens
            .iter()
            .map(|(_, e)| e)
            .chain(otherwise.iter().map(|b| b.as_ref())),
    )?;
    // The `ELSE` first: PostgreSQL resolves a `CASE`'s type with its default
    // result ahead of the `THEN`s, so `CASE … THEN a ELSE t END` is text.
    blank_pad_text_branches(otherwise.iter_mut().map(|b| b.as_mut()).chain(whens.iter_mut().map(|(_, e)| e)));
    Ok(Expr::Case { whens, otherwise })
}

/// Parses a single comparison — `a = b`, `a < b`, `a IS NULL`.
///
/// Public because a join's `ON` may be a comparison the predicate tokenizer
/// cannot read (`||`, `replace()`, a cast), and the join then evaluates it here
/// instead of refusing the statement.
/// Parses a boolean condition: comparisons joined by `AND`/`OR`, with
/// parentheses for grouping. `OR` binds loosest, so it is split first.
pub fn parse_cond(text: &str, schema: &[SchemaField]) -> Result<Cond, String> {
    let text = text.trim();
    // `s ~ 'x'`, `!~`, `~*`, `!~*`: the `regexp_like` calls they mean, as a
    // WHERE lowers them; the condition grammar reads no `~`.
    let lowered;
    let text = if text.contains('~') {
        lowered = crate::core::rql::check::lower_regex_operators(text, "regexp_like");
        lowered.as_str()
    } else {
        text
    };
    // A wholly parenthesised group is just the group.
    if text.starts_with('(') {
        if let Some(close) = matching_paren(text, 0) {
            if close == text.len() - 1 {
                return parse_cond(&text[1..close], schema);
            }
        }
    }
    if let Some(parts) = split_on_keyword(text, "OR") {
        let mut out = Vec::with_capacity(parts.len());
        for part in parts {
            out.push(parse_cond(&part, schema)?);
        }
        return Ok(Cond::Or(out));
    }
    if let Some(parts) = split_on_keyword(text, "AND") {
        let mut out = Vec::with_capacity(parts.len());
        for part in parts {
            out.push(parse_cond(&part, schema)?);
        }
        return Ok(Cond::And(out));
    }
    // `NOT` is refused rather than folded to two-valued logic: comparisons
    // with NULL evaluate to false here, and negating that false would take a
    // branch PostgreSQL's three-valued NOT would not. An error is honest; a
    // plausible wrong branch is not.
    // `NOT` is SQL's three-valued one (`Cond::Not`): negating a two-valued
    // FALSE would take a branch PostgreSQL's NOT does not.
    if text.len() > 4 && text.as_bytes()[..4].eq_ignore_ascii_case(b"NOT ") {
        return Ok(Cond::Not(Box::new(parse_cond(&text[4..], schema)?)));
    }
    if text.len() > 4 && text.as_bytes()[..3].eq_ignore_ascii_case(b"NOT") && text.as_bytes()[3] == b'(' {
        return Ok(Cond::Not(Box::new(parse_cond(&text[3..], schema)?)));
    }
    // A row comparison, `(a, b) < (c, d)`, as the comparisons it means,
    // which the WHERE path already spells out this way.
    if text.contains(',') {
        if let Ok(expanded) = crate::core::tokenizer::predicate_rewrite::expand_row_value_comparisons(text) {
            if expanded.trim() != text {
                return parse_cond(&expanded, schema);
            }
        }
    }
    if let Some(cond) = quantified_condition(text, schema)? {
        return Ok(cond);
    }
    if let Some(cond) = jsonb_condition(text, schema)? {
        return Ok(cond);
    }
    if let Some(cond) = like_condition(text, schema)? {
        return Ok(cond);
    }
    if let Some(cond) = distinct_condition(text, schema)? {
        return Ok(cond);
    }
    if let Some(cond) = in_or_between(text, schema)? {
        return Ok(cond);
    }
    match parse_condition(text, schema) {
        Ok(condition) => Ok(Cond::Cmp(condition)),
        Err(e) => bare_boolean(text, schema).map(Cond::Cmp).ok_or(e),
    }
}

/// How many times `keyword` stands at the top level of `text`.
fn count_keyword(text: &str, keyword: &str) -> usize {
    let mut n = 0;
    let mut from = 0;
    while let Some(at) = crate::core::rql::join::find_keyword(text, keyword, from) {
        n += 1;
        from = at + keyword.len();
    }
    n
}

/// `x [NOT] IN (a, b, …)` and `x [NOT] BETWEEN a AND b`, as the comparisons
/// they are: `x = a OR x = b`, `x >= a AND x <= b`, a NOT around either. Under
/// SQL's three values that is PostgreSQL's answer, NULLs included (`2 IN (1,
/// NULL)` and `2 NOT IN (1, NULL)` are both UNKNOWN). Each comparison is read
/// by [`parse_condition`], so it is coerced as `=` coerces. `None` when `text`
/// is neither.
fn in_or_between(text: &str, schema: &[SchemaField]) -> Result<Option<Cond>, String> {
    use crate::core::rql::join::find_keyword;
    let negated_left = |left: &str| -> (String, bool) {
        let left = left.trim_end();
        match left.len().checked_sub(3).and_then(|i| left.get(i..).map(|tail| (i, tail))) {
            Some((i, tail)) if tail.eq_ignore_ascii_case("NOT") && left[..i].ends_with(char::is_whitespace) => {
                (left[..i].trim().to_string(), true)
            }
            _ => (left.trim().to_string(), false),
        }
    };
    let wrap = |cond: Cond, negated: bool| if negated { Cond::Not(Box::new(cond)) } else { cond };
    if let Some(at) = find_keyword(text, "IN", 0) {
        let rest = text[at + 2..].trim();
        if rest.starts_with('(') && matching_paren(rest, 0) == Some(rest.len() - 1) {
            let (left, negated) = negated_left(&text[..at]);
            if left.is_empty() {
                return Ok(None);
            }
            let inside = rest[1..rest.len() - 1].trim();
            if inside.len() >= 6 && inside.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") {
                return Err(format!("condition '{}' with IN (SELECT …) is not supported here yet", text));
            }
            let items = crate::core::rql::lexer_ct::split_definitions(inside);
            if items.iter().all(|i| i.trim().is_empty()) {
                return Err(format!("condition '{}' has an empty IN list", text));
            }
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(Cond::Cmp(parse_condition(&format!("{} = {}", left, item.trim()), schema)?));
            }
            let any = if parts.len() == 1 { parts.pop().expect("one") } else { Cond::Or(parts) };
            return Ok(Some(wrap(any, negated)));
        }
    }
    if let Some(at) = find_keyword(text, "BETWEEN", 0) {
        let (left, negated) = negated_left(&text[..at]);
        let rest = text[at + "BETWEEN".len()..].trim();
        if rest.len() > 9 && rest.as_bytes()[..9].eq_ignore_ascii_case(b"SYMMETRIC") {
            return Err(format!("condition '{}' with BETWEEN SYMMETRIC is not supported here yet", text));
        }
        let rest = match rest.get(..10) {
            Some(head) if head.eq_ignore_ascii_case("ASYMMETRIC") => rest[10..].trim(),
            _ => rest,
        };
        let Some(and) = find_keyword(rest, "AND", 0) else {
            return Err(format!("condition '{}' has BETWEEN without AND", text));
        };
        let (low, high) = (rest[..and].trim(), rest[and + 3..].trim());
        if left.is_empty() || low.is_empty() || high.is_empty() {
            return Err(format!("condition '{}' is not a whole BETWEEN", text));
        }
        let both = Cond::And(vec![
            Cond::Cmp(parse_condition(&format!("{} >= {}", left, low), schema)?),
            Cond::Cmp(parse_condition(&format!("{} <= {}", left, high), schema)?),
        ]);
        return Ok(Some(wrap(both, negated)));
    }
    Ok(None)
}

/// A boolean value standing alone where a condition is expected — `CASE WHEN
/// flag`, `FILTER (WHERE flag)`, `flag AND n > 1` — is its comparison with
/// TRUE, which keeps its NULL (UNKNOWN), as PostgreSQL reads it.
///
/// Here and not in [`parse_condition`]: that one is also asked "is this
/// select item a comparison?" (`join::comparison_value`), and a bare boolean
/// column answering yes there became a computed item, so `SELECT id,
/// is_active` lost the column. Everything that calls `parse_cond` wants a
/// condition, so the reading is safe here.
/// `x op ANY (ARRAY[…])`, `SOME`, `ALL`, also over an array literal
/// `'{…}'`: the comparison with each element, joined by OR for ANY and by AND
/// for ALL, whose three values are exactly the quantifiers' (TRUE if any
/// holds, else UNKNOWN if any is unknown, else FALSE). Over no elements ANY
/// is FALSE and ALL is TRUE, whatever `x` is. A subquery is not this one's
/// (`subquery::quantified_predicate`).
fn quantified_condition(text: &str, schema: &[SchemaField]) -> Result<Option<Cond>, String> {
    use crate::core::rql::join::find_keyword;
    let found = ["ANY", "SOME", "ALL"]
        .into_iter()
        .filter_map(|kw| find_keyword(text, kw, 0).map(|at| (at, kw)))
        .filter(|(at, kw)| text[at + kw.len()..].trim_start().starts_with('('))
        .min_by_key(|(at, _)| *at);
    let Some((at, kw)) = found else {
        return Ok(None);
    };
    let head = text[..at].trim_end();
    let Some(op) = ["<=", ">=", "<>", "!=", "=", "<", ">"].into_iter().find(|op| head.ends_with(op)) else {
        return Ok(None);
    };
    let left = head[..head.len() - op.len()].trim();
    let arg = text[at + kw.len()..].trim();
    let Some(close) = matching_paren(arg, 0) else {
        return Ok(None);
    };
    if left.is_empty() || close != arg.len() - 1 {
        return Ok(None);
    }
    let inner = arg[1..close].trim();
    let elements: Vec<String> = if inner.len() > 5 && inner.as_bytes()[..5].eq_ignore_ascii_case(b"ARRAY") && inner[5..].trim_start().starts_with('[') {
        let list = inner[5..].trim_start();
        if !list.ends_with(']') {
            return Ok(None);
        }
        let body = &list[1..list.len() - 1];
        if body.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level_commas(body).into_iter().map(|e| e.trim().to_string()).collect()
        }
    } else if inner.starts_with('\'') {
        match crate::core::tokenizer::predicate_rewrite::pg_array_elements(inner) {
            Some(elements) => elements,
            None => return Ok(None),
        }
    } else {
        // An array the row computes (`5 = ANY(tags)`): compared element by
        // element as each row is read (`PgFunc::QuantifiedCmp`). Refused as
        // "unsupported keyword 'ANY'" before.
        let array = parse(inner, schema)?;
        if static_type(&array) != DbType::ARRAY {
            return Ok(None);
        }
        let left = parse(left, schema)?;
        let tag = |t: &str| Expr::Literal { bytes: t.as_bytes().to_vec(), db_type: DbType::STRING };
        let call = Expr::Call(
            TextFunc::Pg(crate::core::rql::pg_functions::PgFunc::QuantifiedCmp),
            vec![left, array, tag(op), tag(if kw == "ALL" { "ALL" } else { "ANY" })],
        );
        return Ok(Some(Cond::Cmp(Condition {
            left: Box::new(call),
            cmp: Cmp::Eq,
            right: Some(Box::new(Expr::Literal { bytes: vec![1], db_type: DbType::BOOL })),
            wide: Default::default(),
        })));
    };
    let any = kw != "ALL";
    if elements.is_empty() {
        let truth = Expr::Literal { bytes: vec![1], db_type: DbType::BOOL };
        return Ok(Some(Cond::Cmp(Condition {
            left: Box::new(truth),
            cmp: Cmp::Eq,
            right: Some(Box::new(Expr::Literal { bytes: vec![u8::from(!any)], db_type: DbType::BOOL })),
            wide: Default::default(),
        })));
    }
    let mut parts = Vec::with_capacity(elements.len());
    for element in &elements {
        parts.push(Cond::Cmp(parse_condition(&format!("({left}) {op} ({element})"), schema)?));
    }
    Ok(Some(if any { Cond::Or(parts) } else { Cond::And(parts) }))
}

/// `doc @> x`, `x <@ doc` and `doc ? 'k'` as conditions: the evaluator's
/// containment and key test (`jsonb::contains`, `jsonb::key_exists`), which
/// only the WHERE path read before. A literal side is read as a document.
fn jsonb_condition(text: &str, schema: &[SchemaField]) -> Result<Option<Cond>, String> {
    let nesting = Nesting::of(text);
    let bytes = text.as_bytes();
    let found = (0..bytes.len()).find_map(|i| {
        if !nesting.top_level(i) {
            return None;
        }
        match (bytes[i], bytes.get(i + 1)) {
            (b'@', Some(b'>')) => Some((i, 2, false)),
            (b'<', Some(b'@')) => Some((i, 2, true)),
            (b'?', next) if !matches!(next, Some(b'|' | b'&')) => Some((i, 1, false)),
            _ => None,
        }
    });
    let Some((at, len, swapped)) = found else {
        return Ok(None);
    };
    let (left, right) = (text[..at].trim(), text[at + len..].trim());
    if left.is_empty() || right.is_empty() {
        return Ok(None);
    }
    let (left, right) = (parse(left, schema)?, parse(right, schema)?);
    let call = if len == 1 {
        Expr::Call(TextFunc::JsonbHasKey, vec![left, right])
    } else if swapped {
        Expr::Call(TextFunc::JsonbContains, vec![right, left])
    } else {
        Expr::Call(TextFunc::JsonbContains, vec![left, right])
    };
    Ok(Some(Cond::Cmp(Condition {
        left: Box::new(call),
        cmp: Cmp::Eq,
        right: Some(Box::new(Expr::Literal { bytes: vec![1], db_type: DbType::BOOL })),
        wide: Default::default(),
    })))
}

/// Splits on the commas outside quotes, parentheses and brackets.
fn split_top_level_commas(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let (mut depth, mut quote, mut start) = (0i32, None::<u8>, 0usize);
    let mut out = Vec::new();
    for (i, &c) in bytes.iter().enumerate() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b',' if depth == 0 => {
                    out.push(&text[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    out.push(&text[start..]);
    out
}

/// `x [NOT] LIKE p [ESCAPE e]` and `x [NOT] ILIKE p …` as a condition,
/// which the WHERE path answers with its own matcher and nothing else read:
/// in a select list, a `CASE WHEN` or a join's condition the whole item was
/// refused as an unknown column. Any other escape than the default backslash
/// is rewritten into it.
fn like_condition(text: &str, schema: &[SchemaField]) -> Result<Option<Cond>, String> {
    use crate::core::rql::join::find_keyword;
    let found = [("LIKE", TextFunc::Like), ("ILIKE", TextFunc::ILike)]
        .into_iter()
        .filter_map(|(kw, func)| find_keyword(text, kw, 0).map(|at| (at, kw.len(), func)))
        .min_by_key(|(at, ..)| *at);
    let Some((at, len, func)) = found else {
        return Ok(None);
    };
    let mut left = text[..at].trim_end();
    let negated = left.len() > 4
        && left.as_bytes()[left.len() - 3..].eq_ignore_ascii_case(b"NOT")
        && left.as_bytes()[left.len() - 4].is_ascii_whitespace();
    if negated {
        left = left[..left.len() - 3].trim_end();
    }
    let rest = &text[at + len..];
    let (pattern_text, escape) = match find_keyword(rest, "ESCAPE", 0) {
        Some(e) => (&rest[..e], Some(rest[e + 6..].trim())),
        None => (rest, None),
    };
    if left.is_empty() || pattern_text.trim().is_empty() {
        return Ok(None);
    }
    let mut pattern = parse(pattern_text, schema)?;
    if let Some(escape) = escape {
        let Expr::Literal { bytes, db_type: DbType::STRING } = &pattern else {
            return Err("LIKE … ESCAPE is supported only over a constant pattern".to_string());
        };
        let Ok(Expr::Literal { bytes: esc, db_type: DbType::STRING }) = parse(escape, schema) else {
            return Err("ESCAPE needs a quoted string".to_string());
        };
        let esc = String::from_utf8_lossy(&esc).into_owned();
        let mut chars = esc.chars();
        let escape = match (chars.next(), chars.next()) {
            (None, _) => None,
            (Some(c), None) => Some(c),
            _ => return Err("invalid escape string".to_string()),
        };
        // Into the backslash form the matcher reads.
        let mut out = String::new();
        let source = String::from_utf8_lossy(bytes).into_owned();
        let mut it = source.chars();
        while let Some(c) = it.next() {
            if Some(c) == escape {
                match it.next() {
                    Some(n) => {
                        out.push('\\');
                        out.push(n);
                    }
                    None => return Err("LIKE pattern must not end with escape character".to_string()),
                }
            } else if c == '\\' {
                out.push_str("\\\\");
            } else {
                out.push(c);
            }
        }
        pattern = Expr::Literal { bytes: out.into_bytes(), db_type: DbType::STRING };
    }
    let left = parse(left, schema)?;
    // `~~` has no integer, numeric or boolean operand in PostgreSQL:
    // `a LIKE 'x%'` over an integer is 42883, not a match on its text.
    if !yields_json_text(&left)
        && !matches!(static_type(&left), DbType::STRING | DbType::CHAR | DbType::BPCHAR(_) | DbType::NULL)
        && !matches!(left, Expr::Null)
    {
        return Err(format!(
            "operator does not exist: {} {} unknown",
            pg_operand_type_name(&left),
            if func == TextFunc::ILike { "~~*" } else { "~~" }
        ));
    }
    let call = Expr::Call(func, vec![left, pattern]);
    let cond = Cond::Cmp(Condition {
        left: Box::new(call),
        cmp: Cmp::Eq,
        right: Some(Box::new(Expr::Literal { bytes: vec![1], db_type: DbType::BOOL })),
        wide: Default::default(),
    });
    Ok(Some(if negated { Cond::Not(Box::new(cond)) } else { cond }))
}

/// `a IS [NOT] DISTINCT FROM b`: the comparison that treats two NULLs as
/// equal and a NULL as unequal to any value, never UNKNOWN. Built from
/// conditions the evaluator has: one side NULL and the other not, or both
/// values and unequal. Refused as "not supported" before.
fn distinct_condition(text: &str, schema: &[SchemaField]) -> Result<Option<Cond>, String> {
    use crate::core::rql::join::find_keyword;
    let Some(at) = find_keyword(text, "IS", 0) else {
        return Ok(None);
    };
    let mut words = text[at + 2..].trim_start();
    let negated = words.len() > 4 && words.as_bytes()[..4].eq_ignore_ascii_case(b"NOT ");
    if negated {
        words = words[4..].trim_start();
    }
    if !(words.len() > 9 && words.as_bytes()[..8].eq_ignore_ascii_case(b"DISTINCT") && words.as_bytes()[8].is_ascii_whitespace()) {
        return Ok(None);
    }
    let words = words[9..].trim_start();
    if !(words.len() > 5 && words.as_bytes()[..4].eq_ignore_ascii_case(b"FROM") && words.as_bytes()[4].is_ascii_whitespace()) {
        return Ok(None);
    }
    let (left, right) = (text[..at].trim(), words[5..].trim());
    if left.is_empty() || right.is_empty() {
        return Ok(None);
    }
    let null = |side: &str| parse_condition(&format!("({side}) IS NULL"), schema).map(Cond::Cmp);
    let (left_null, right_null) = (null(left)?, null(right)?);
    let unequal = Cond::Cmp(parse_condition(&format!("({left}) <> ({right})"), schema)?);
    let not = |c: &Cond| Cond::Not(Box::new(c.clone()));
    let distinct = Cond::Or(vec![
        Cond::And(vec![left_null.clone(), not(&right_null)]),
        Cond::And(vec![not(&left_null), right_null.clone()]),
        Cond::And(vec![not(&left_null), not(&right_null), unequal]),
    ]);
    Ok(Some(if negated { Cond::Not(Box::new(distinct)) } else { distinct }))
}

fn bare_boolean(text: &str, schema: &[SchemaField]) -> Option<Condition> {
    let expr = parse(text, schema).ok()?;
    (static_type(&expr) == DbType::BOOL).then(|| Condition {
        left: Box::new(expr),
        cmp: Cmp::Eq,
        right: Some(Box::new(Expr::Literal { bytes: vec![1], db_type: DbType::BOOL })),
        wide: Default::default(),
    })
}

/// Splits on every top-level occurrence of a keyword, outside quotes and
/// parentheses. `None` when the keyword is absent or a side would be empty.
fn split_on_keyword(text: &str, keyword: &str) -> Option<Vec<String>> {
    use crate::core::rql::join::find_keyword;
    let mut parts: Vec<String> = Vec::new();
    let mut start = 0usize;
    let mut search = 0usize;
    // The AND of a `BETWEEN a AND b` is the BETWEEN's, not a conjunction:
    // split there, `x BETWEEN 1 AND 5` became `x BETWEEN 1` and `5`.
    let mut owed = 0usize;
    while let Some(at) = find_keyword(text, keyword, search) {
        if keyword == "AND" && count_keyword(&text[start..at], "BETWEEN") > owed {
            owed += 1;
            search = at + keyword.len();
            continue;
        }
        owed = 0;
        parts.push(text[start..at].trim().to_string());
        start = at + keyword.len();
        search = start;
    }
    if parts.is_empty() {
        return None;
    }
    parts.push(text[start..].trim().to_string());
    if parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    Some(parts)
}

/// Evaluates a condition tree against a row.
///
/// Two-valued, like [`eval_condition`]: a comparison with NULL is false, so
/// `AND`/`OR` see plain booleans.
pub fn eval_cond(cond: &Cond, row: &Row, schema: &[SchemaField]) -> Result<bool, String> {
    Ok(match cond {
        Cond::Not(_) => eval_cond_3vl(cond, row, schema)? == Some(true),
        Cond::Cmp(c) => eval_condition(c, row, schema)?,
        Cond::And(parts) => {
            for part in parts {
                if !eval_cond(part, row, schema)? {
                    return Ok(false);
                }
            }
            true
        }
        Cond::Or(parts) => {
            for part in parts {
                if eval_cond(part, row, schema)? {
                    return Ok(true);
                }
            }
            false
        }
    })
}

/// Evaluates a condition tree with SQL's three values: `None` is UNKNOWN.
///
/// A comparison with NULL is UNKNOWN; `AND` is FALSE when a part is FALSE,
/// else UNKNOWN when a part is; `OR` is TRUE when a part is TRUE, else
/// UNKNOWN when a part is. A `CHECK` needs this: it refuses a row only when
/// it is FALSE, so `x > 0` and `x IS NULL OR x > 0` both pass a NULL `x`,
/// where [`eval_cond`]'s two values read the first as FALSE.
pub fn eval_cond_3vl(cond: &Cond, row: &Row, schema: &[SchemaField]) -> Result<Option<bool>, String> {
    Ok(match cond {
        Cond::Not(inner) => eval_cond_3vl(inner, row, schema)?.map(|b| !b),
        Cond::Cmp(c) => eval_condition_3vl(c, row, schema)?,
        Cond::And(parts) => {
            let mut unknown = false;
            for part in parts {
                match eval_cond_3vl(part, row, schema)? {
                    Some(false) => return Ok(Some(false)),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            if unknown { None } else { Some(true) }
        }
        Cond::Or(parts) => {
            let mut unknown = false;
            for part in parts {
                match eval_cond_3vl(part, row, schema)? {
                    Some(true) => return Ok(Some(true)),
                    None => unknown = true,
                    Some(false) => {}
                }
            }
            if unknown { None } else { Some(false) }
        }
    })
}

pub fn parse_condition(text: &str, schema: &[SchemaField]) -> Result<Condition, String> {
    use crate::core::rql::join::find_keyword;

    let text = text.trim();
    if let Some(at) = find_keyword(text, "IS", 0) {
        let tail = text[at + 2..].trim().to_ascii_uppercase();
        // The operand may itself be a condition, `(s ~ 'x') IS NOT TRUE`,
        // `(a > 1 OR f) IS FALSE`: its value TRUE, FALSE or NULL.
        let left = Box::new(match parse(&text[..at], schema) {
            Ok(expr) => expr,
            Err(e) => match crate::core::rql::join::comparison_value(&text[..at], schema) {
                Ok(Some(value)) => value,
                _ => return Err(e),
            },
        });
        if tail == "NULL" {
            return Ok(Condition {
                left,
                cmp: Cmp::IsNull,
                right: None,
                wide: Default::default(),
            });
        }
        if tail == "NOT NULL" {
            return Ok(Condition {
                left,
                cmp: Cmp::IsNotNull,
                right: None,
                wide: Default::default(),
            });
        }
        // Whitespace between the words is free, as anywhere else.
        let words: Vec<&str> = tail.split_whitespace().collect();
        let cmp = match words.as_slice() {
            ["TRUE"] => Some(Cmp::IsTrue),
            ["NOT", "TRUE"] => Some(Cmp::IsNotTrue),
            ["FALSE"] => Some(Cmp::IsFalse),
            ["NOT", "FALSE"] => Some(Cmp::IsNotFalse),
            ["UNKNOWN"] => Some(Cmp::IsNull),
            ["NOT", "UNKNOWN"] => Some(Cmp::IsNotNull),
            ["NOT", "NULL"] => Some(Cmp::IsNotNull),
            _ => None,
        };
        if let Some(cmp) = cmp {
            return Ok(Condition {
                left,
                cmp,
                right: None,
                wide: Default::default(),
            });
        }
        return Err(format!("condition '{}' is not supported yet", text));
    }

    // Longest operators first so `<=` is not read as `<`.
    for (token, cmp) in [
        ("<=", Cmp::Le),
        (">=", Cmp::Ge),
        ("<>", Cmp::Ne),
        ("!=", Cmp::Ne),
        ("=", Cmp::Eq),
        ("<", Cmp::Lt),
        (">", Cmp::Gt),
    ] {
        if let Some(at) = find_operator(text, token) {
            let mut left = Box::new(parse(&text[..at], schema)?);
            let mut right = Box::new(parse(&text[at + token.len()..], schema)?);
            text_against_number(&left, token, &right)?;
            untyped_literal_as(&left, &mut right)?;
            untyped_literal_as(&right, &mut left)?;
            // A string compared with a `character(n)` value is read without
            // its trailing spaces, as the value is held (`yields_bpchar`).
            if yields_bpchar(&left) {
                strip_trailing_spaces(&mut right);
                blank_pad_varying(&mut right);
            } else if yields_bpchar(&right) {
                strip_trailing_spaces(&mut left);
                blank_pad_varying(&mut left);
            }
            local_timestamp_literal(&left, &mut right);
            local_timestamp_literal(&right, &mut left);
            return Ok(Condition {
                left,
                cmp,
                right: Some(right),
                wide: Default::default(),
            });
        }
    }
    // A boolean function call standing alone (`regexp_like(s, 'x')`) is its
    // comparison with TRUE, which keeps its NULL. Only a call: a bare boolean
    // column read as `flag = TRUE` became a computed select item wherever a
    // comparison is taken for a value (`join::comparison_value`), and
    // `SELECT id, is_active FROM t` lost the column from its rows.
    if let Ok(expr @ Expr::Call(..)) = parse(text, schema) {
        if static_type(&expr) == DbType::BOOL {
            return Ok(Condition {
                left: Box::new(expr),
                cmp: Cmp::Eq,
                right: Some(Box::new(Expr::Literal { bytes: vec![1], db_type: DbType::BOOL })),
                wide: Default::default(),
            });
        }
    }
    Err(format!("condition '{}' is not a comparison", text))
}

/// PostgreSQL's 42883 for a text value compared with a number or a
/// boolean: `text = double precision`, `text = integer`, `text > boolean`
/// have no operator (`t = 5` or `t = '1'::float8` in a select list, a CHECK,
/// a join's condition). A quoted literal is not text here: it is an untyped
/// constant that takes the other side's type, as `f = '1.5'` does. It used
/// to answer false for a float, and for an integer to compare the text.
fn text_against_number(left: &Expr, op: &str, right: &Expr) -> Result<(), String> {
    let text = |e: &Expr| {
        !matches!(e, Expr::Literal { .. } | Expr::Null)
            && matches!(static_type(e), DbType::STRING | DbType::CHAR | DbType::BPCHAR(_))
    };
    let float = |e: &Expr| match static_type(e) {
        DbType::F64 => Some("double precision"),
        DbType::F32 => Some("real"),
        t @ (DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128
        | DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64
        | DbType::U128
        | DbType::DECIMAL
        | DbType::BOOL) => Some(crate::core::value_codec::pg_input_type_name(&t)),
        _ => None,
    };
    if text(left) {
        if let Some(name) = float(right) {
            return Err(format!("operator does not exist: text {op} {name}"));
        }
    }
    if text(right) {
        if let Some(name) = float(left) {
            return Err(format!("operator does not exist: {name} {op} text"));
        }
    }
    Ok(())
}

/// Whether `message` is a value's input error: a constant that is not a value
/// of the type it is read as (22P02), or one past its range (22003).
pub(crate) fn is_input_error(message: &str) -> bool {
    message.starts_with("invalid input syntax for type ")
        || (message.starts_with("value \"") && message.contains("\" is out of range for type "))
        || (message.starts_with('"') && message.contains("\" is out of range for type "))
}

/// A quoted literal compared with a number or a boolean, read as that type,
/// as PostgreSQL resolves an untyped constant: `'1' = 1` is `1 = 1`, and
/// `'a' = 1` is 22P02 (`invalid input syntax for type integer: "a"`). Left
/// as text it was compared byte for byte against the other side's encoding
/// and answered false, so `'1' = 1`, `'2.5' < 3.0` and `'true' = true` were
/// all false. Only when the other side's type is certain: a column, a cast,
/// a typed constant.
fn untyped_literal_as(other: &Expr, literal: &mut Expr) -> Result<(), String> {
    let Expr::Literal { bytes, db_type: DbType::STRING } = &*literal else { return Ok(()) };
    if matches!(other, Expr::Literal { db_type: DbType::STRING, .. } | Expr::Null) {
        return Ok(());
    }
    if !certain_type(other) {
        return Ok(());
    }
    let target = static_type(other);
    let text = String::from_utf8_lossy(bytes).to_string();
    let refuse = || crate::core::value_codec::refusal_message(&target, &crate::core::value_codec::quote_sql(&text));
    let t = text.trim();
    let converted = match target {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => {
            let digits = t.strip_prefix(['-', '+']).unwrap_or(t);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(refuse());
            }
            let v: i128 = t.parse().map_err(|_| refuse())?;
            let (lo, hi): (i128, i128) = match target {
                DbType::I8 => (i8::MIN.into(), i8::MAX.into()),
                DbType::I16 => (i16::MIN.into(), i16::MAX.into()),
                DbType::I32 => (i32::MIN.into(), i32::MAX.into()),
                DbType::U8 => (0, u8::MAX.into()),
                DbType::U16 => (0, u16::MAX.into()),
                DbType::U32 => (0, u32::MAX.into()),
                DbType::U64 => (0, u64::MAX.into()),
                _ => (i64::MIN.into(), i64::MAX.into()),
            };
            if v < lo || v > hi {
                return Err(refuse());
            }
            match i64::try_from(v) {
                Ok(v) => Expr::Literal { bytes: v.to_le_bytes().to_vec(), db_type: DbType::I64 },
                Err(_) => Expr::Literal { bytes: (v as u64).to_le_bytes().to_vec(), db_type: DbType::U64 },
            }
        }
        DbType::F32 | DbType::F64 => {
            let v: f64 = match t.to_ascii_lowercase().as_str() {
                "nan" => f64::NAN,
                "infinity" | "+infinity" | "inf" | "+inf" => f64::INFINITY,
                "-infinity" | "-inf" => f64::NEG_INFINITY,
                _ if t.bytes().any(|b| b.is_ascii_digit()) && t.bytes().all(|b| b.is_ascii_digit() || b"+-.eE".contains(&b)) => {
                    t.parse().map_err(|_| refuse())?
                }
                _ => return Err(refuse()),
            };
            if target == DbType::F32 {
                Expr::Literal { bytes: (v as f32).to_le_bytes().to_vec(), db_type: DbType::F32 }
            } else {
                Expr::Literal { bytes: v.to_le_bytes().to_vec(), db_type: DbType::F64 }
            }
        }
        DbType::DECIMAL => {
            let body = t.strip_prefix(['-', '+']).unwrap_or(t);
            let mantissa = body.split(['e', 'E']).next().unwrap_or("");
            let exponent = body.split_once(['e', 'E']).map(|(_, e)| e);
            let numeric = mantissa.bytes().any(|b| b.is_ascii_digit())
                && mantissa.bytes().all(|b| b.is_ascii_digit() || b == b'.')
                && mantissa.bytes().filter(|&b| b == b'.').count() <= 1
                && exponent.is_none_or(|e| {
                    let e = e.strip_prefix(['-', '+']).unwrap_or(e);
                    !e.is_empty() && e.bytes().all(|b| b.is_ascii_digit())
                });
            if !numeric {
                return Err(refuse());
            }
            match parse(t, &[])? {
                lit @ Expr::Literal { .. } => lit,
                _ => return Err(refuse()),
            }
        }
        DbType::BOOL => {
            let lower = t.to_ascii_lowercase();
            // PostgreSQL's `boolin`: a prefix of true/yes/false/no, on, off
            // (or `of`), 1 or 0, any case; a lone `o` is ambiguous.
            let prefix = |word: &str| !lower.is_empty() && word.starts_with(lower.as_str());
            let truthy = lower == "1" || lower == "on" || prefix("true") || prefix("yes");
            let falsy = lower == "0" || (lower.len() >= 2 && prefix("off")) || prefix("false") || prefix("no");
            match (truthy, falsy) {
                (true, false) => Expr::Literal { bytes: vec![1], db_type: DbType::BOOL },
                (false, true) => Expr::Literal { bytes: vec![0], db_type: DbType::BOOL },
                _ => return Err(refuse()),
            }
        }
        _ => return Ok(()),
    };
    *literal = converted;
    Ok(())
}

/// Whether `expr`'s [`static_type`] is the type PostgreSQL gives it, for
/// [`untyped_literal_as`]: a column, a cast, a typed constant, and arithmetic
/// over those when every part is a number. Not a call, and not arithmetic
/// that may be a date or a time (`a + interval '1 hour'` reads as `bigint`
/// there, and its literal is a timestamp): those keep comparing as before.
fn certain_type(expr: &Expr) -> bool {
    let number = |t: DbType| {
        matches!(
            t,
            DbType::I8
                | DbType::I16
                | DbType::I32
                | DbType::I64
                | DbType::U8
                | DbType::U16
                | DbType::U32
                | DbType::U64
                | DbType::F32
                | DbType::F64
                | DbType::DECIMAL
        )
    };
    match expr {
        Expr::Column(_) | Expr::Cast(..) | Expr::TypmodCast { .. } | Expr::NumericCast { .. } => true,
        Expr::Literal { db_type, .. } => *db_type != DbType::STRING,
        Expr::Arith(lhs, _, rhs) => {
            certain_type(lhs) && certain_type(rhs) && number(static_type(lhs)) && number(static_type(rhs))
        }
        _ => false,
    }
}

/// A quoted literal compared with a `timestamp` (without time zone) value,
/// read as that type reads one: its zone discarded. The value is anything
/// [`yields_timestamp`] says is one: a column, `(a)`, `coalesce(a, …)`, `a +
/// interval '1 hour'`, a cast. Left as text the literal was converted to UTC
/// when compared, so `ts = '… 10:00+02'` looked for 08:00 in a column that
/// holds 10:00, and `'… 09:00+05' < a` read 04:00. The tokenizer does the
/// same for the scan's predicates (`query_tokenizer::local_timestamp_literals`).
fn local_timestamp_literal(column: &Expr, literal: &mut Expr) {
    if !yields_timestamp(column) {
        return;
    }
    let Expr::Literal { bytes, db_type } = literal else { return };
    if *db_type != DbType::STRING {
        return;
    }
    let text = String::from_utf8_lossy(bytes).to_string();
    if let Some(us) = crate::core::value_codec::parse_timestamp_local_us(text.trim_matches('\'')) {
        *bytes = us.to_le_bytes().to_vec();
        *db_type = DbType::DATETIME;
    }
}

/// Whether `expr` yields a `timestamp` (without time zone) rather than a
/// `timestamptz`. The two are one storage type (`DATETIME`, microseconds),
/// so the value cannot say; its expression does, as PostgreSQL types it:
///
/// - a column declared `timestamp` (`SchemaField::literal_type`), and a cast
///   to the type, `::timestamp[(p)]` or `LOCALTIMESTAMP`;
/// - a `timestamp` plus or minus an interval, a `date` plus an interval or a
///   time of day, and `date_trunc` of a `timestamp`;
/// - `CASE`, `COALESCE`, `NULLIF`, `GREATEST` and `LEAST` whose timestamp
///   branches all are (a `timestamptz` one makes the common type
///   `timestamptz`, which PostgreSQL prefers in the category).
///
/// It decides how the value is shown as text (bare, or with `+00`) and how a
/// quoted literal compared with it is read (its zone discarded, or applied):
/// the two questions the storage type cannot answer. F1's `yields_bpchar`
/// answers the same kind of question for `character(n)`.
pub fn yields_timestamp(expr: &Expr) -> bool {
    match expr {
        Expr::Column(f) => f.literal_type() == DbType::TIMESTAMP,
        Expr::Cast(_, db_type) => *db_type == DbType::TIMESTAMP,
        Expr::TypmodCast { value, .. } => yields_timestamp(value),
        Expr::Arith(lhs, op, rhs) => {
            let (l, r) = (static_type(lhs), static_type(rhs));
            match (l, r) {
                (DbType::DATETIME, DbType::INTERVAL) => yields_timestamp(lhs),
                (DbType::INTERVAL, DbType::DATETIME) if matches!(op, ArithOp::Add) => yields_timestamp(rhs),
                // `date + interval`, `date + time`: a `timestamp`.
                (DbType::DATE, DbType::INTERVAL | DbType::TIME) | (DbType::INTERVAL | DbType::TIME, DbType::DATE) => true,
                _ => false,
            }
        }
        Expr::Call(TextFunc::DateTrunc, args) => args.get(1).is_some_and(yields_timestamp),
        Expr::Call(TextFunc::Pg(func), _) => crate::core::rql::pg_functions::yields_timestamp(*func),
        Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => common_timestamp(args.iter()),
        Expr::NullIf(lhs, _) => yields_timestamp(lhs),
        Expr::Case { whens, otherwise } => {
            common_timestamp(whens.iter().map(|(_, e)| e).chain(otherwise.iter().map(|b| b.as_ref())))
        }
        _ => false,
    }
}

/// Whether the common type of `branches` is `timestamp` (without time zone):
/// at least one branch is a timestamp, and every timestamp branch is one. A
/// literal or a NULL takes the type of the others and does not count.
fn common_timestamp<'a>(branches: impl Iterator<Item = &'a Expr>) -> bool {
    let mut any = false;
    for b in branches {
        match b {
            Expr::Null | Expr::Literal { .. } => {}
            other if static_type(other) == DbType::DATETIME => {
                if !yields_timestamp(other) {
                    return false;
                }
                any = true;
            }
            _ => {}
        }
    }
    any
}

/// A `timestamp` value as text: its wall-clock time, no offset.
fn local_timestamp_text(value: Value) -> Result<Value, String> {
    match value {
        Value::Data { bytes, db_type: DbType::DATETIME } => {
            let us = i64::from_le_bytes(bytes.as_slice().try_into().map_err(|_| "a timestamp could not be read".to_string())?);
            Ok(Value::Data {
                bytes: crate::core::value_codec::format_timestamp_us(us).into_bytes(),
                db_type: DbType::STRING,
            })
        }
        other => cast(other, &DbType::STRING),
    }
}

/// Position of a top-level operator, outside quotes and parentheses.
fn find_operator(text: &str, op: &str) -> Option<usize> {
    // Outside quotes, parentheses and `CASE` blocks (`Nesting`): the `<` of
    // `qty > CASE WHEN id < 3 THEN … END` is the block's, and splitting there
    // read `qty > CASE WHEN id` as an operand.
    let bytes = text.as_bytes();
    let nesting = Nesting::of(text);
    let mut i = 0usize;
    while i + op.len() <= bytes.len() {
        // Compared as bytes: `i` steps through every byte, and slicing the
        // text at one inside a multi-byte character (`naïve`) panicked.
        if nesting.top_level(i) && bytes[i..].starts_with(op.as_bytes()) {
            if op == "=" && i > 0 && matches!(bytes[i - 1], b'<' | b'>' | b'!') {
                i += 1;
                continue;
            }
            // `<` and `>` of a shift (`a << 2 > 3`), and the `>` of a path
            // arrow, are not comparisons.
            let doubled = |c: u8| bytes.get(i + 1) == Some(&c) || (i > 0 && bytes[i - 1] == c);
            if (op == "<" && doubled(b'<')) || (op == ">" && (doubled(b'>') || (i > 0 && matches!(bytes[i - 1], b'-' | b'#')))) {
                i += 1;
                continue;
            }
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Reads a column from a row, honouring its NULL companion.
/// Microseconds in one of `unit`.
///
/// Months and years are refused rather than approximated: their length depends
/// on the date they are added to, so a fixed number of days would be wrong for
/// most of them and wrong silently.
fn unit_micros(unit: &str) -> Result<i64, String> {
    const SECOND: i64 = 1_000_000;
    let normalised = unit.trim().trim_end_matches('s').to_ascii_lowercase();
    Ok(match normalised.as_str() {
        "microsecond" | "usec" | "us" => 1,
        "millisecond" | "msec" | "ms" => 1_000,
        "second" | "sec" => SECOND,
        "minute" | "min" => 60 * SECOND,
        "hour" | "hr" => 3_600 * SECOND,
        "day" => 86_400 * SECOND,
        "week" => 7 * 86_400 * SECOND,
        "month" | "year" => {
            return Err(format!(
                "an interval in {}s is not supported: its length depends on the date it is \
                 added to, and a fixed number of days would be wrong for most of them",
                normalised
            ));
        }
        _ => return Err(format!("unknown interval unit '{}'", unit)),
    })
}

/// Parses the inside of `INTERVAL '…'` into microseconds.
///
/// Accepts the repeated `<amount> <unit>` spelling PostgreSQL uses, so
/// `'1 day 6 hours'` is one value rather than two.
fn interval_micros(text: &str) -> Result<i64, String> {
    let mut total = 0i64;
    let mut tokens = text.split_whitespace().peekable();
    let mut seen = false;
    while let Some(amount) = tokens.next() {
        let amount: i64 = amount
            .trim_matches('\'')
            .parse()
            .map_err(|_| format!("interval '{}' needs a number before its unit", text))?;
        let Some(unit) = tokens.next() else {
            return Err(format!("interval '{}' has a number with no unit", text));
        };
        total = total
            .checked_add(
                unit_micros(unit)?
                    .checked_mul(amount)
                    .ok_or_else(|| "interval is too large".to_string())?,
            )
            .ok_or_else(|| "interval is too large".to_string())?;
        seen = true;
    }
    if !seen {
        return Err(format!("interval '{}' is empty", text));
    }
    Ok(total)
}

/// Every column the expression reads, in first-seen order.
///
/// The scan has to fetch these: `eval` looks a column up by write order in the
/// row it is given, and a column that was never fetched is indistinguishable
/// there from one that is NULL.
/// Splits a trailing `::type` cast off an expression, if it has one.
pub fn trailing_cast(text: &str) -> Option<(&str, &str)> {
    let at = top_level_cast(text)?;
    Some((text[..at].trim_end(), text[at + 2..].trim()))
}

/// The columns a condition reads, including both sides of the comparison.
/// The columns a whole boolean tree reads, with duplicates removed.
pub fn cond_columns(cond: &Cond) -> Vec<SchemaField> {
    let mut out: Vec<SchemaField> = Vec::new();
    fn walk(cond: &Cond, out: &mut Vec<SchemaField>) {
        match cond {
            Cond::Cmp(c) => {
                for field in condition_columns(c) {
                    if !out.iter().any(|f: &SchemaField| f.name == field.name) {
                        out.push(field);
                    }
                }
            }
            Cond::And(parts) | Cond::Or(parts) => {
                for part in parts {
                    walk(part, out);
                }
            }
            Cond::Not(inner) => walk(inner, out),
        }
    }
    walk(cond, &mut out);
    out
}

pub fn condition_columns(condition: &Condition) -> Vec<SchemaField> {
    let mut out = columns_of(&condition.left);
    if let Some(right) = &condition.right {
        for field in columns_of(right) {
            if !out.iter().any(|f| f.name == field.name) {
                out.push(field);
            }
        }
    }
    out
}

pub fn columns_of(expr: &Expr) -> Vec<SchemaField> {
    fn walk(expr: &Expr, out: &mut Vec<SchemaField>) {
        match expr {
            Expr::Column(field) => {
                if !out.iter().any(|f| f.name == field.name) {
                    out.push(field.clone());
                }
            }
            Expr::Null | Expr::Literal { .. } | Expr::Scalar(_) => {}
            Expr::JsonPath { column, .. } => {
                if !out.iter().any(|f| f.name == column.name) {
                    out.push(column.clone());
                }
            }
            Expr::PathOf { base, .. } => walk(base, out),
            Expr::Concat(a, b) => {
                walk(a, out);
                walk(b, out);
            }
            Expr::Call(_, args) => {
                for arg in args {
                    walk(arg, out);
                }
            }
            Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => {
                for arg in args {
                    walk(arg, out);
                }
            }
            Expr::NullIf(a, b) | Expr::Mod(a, b) => {
                walk(a, out);
                walk(b, out);
            }
            Expr::Arith(a, _, b) => {
                walk(a, out);
                walk(b, out);
            }
            Expr::Cast(inner, _) => walk(inner, out),
            Expr::NumericCast { value, .. } | Expr::TypmodCast { value, .. } => walk(value, out),
            Expr::Case { whens, otherwise } => {
                fn walk_cond(cond: &Cond, out: &mut Vec<SchemaField>) {
                    match cond {
                        Cond::Cmp(c) => {
                            walk(&c.left, out);
                            if let Some(right) = &c.right {
                                walk(right, out);
                            }
                        }
                        Cond::And(parts) | Cond::Or(parts) => {
                            for part in parts {
                                walk_cond(part, out);
                            }
                        }
                        Cond::Not(inner) => walk_cond(inner, out),
                    }
                }
                for (condition, result) in whens {
                    walk_cond(condition, out);
                    walk(result, out);
                }
                if let Some(other) = otherwise {
                    walk(other, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// The type an expression yields, worked out without evaluating it.
///
/// Needed because a select item's type has to be reported in the row
/// description before any row exists. It follows the same rules `eval` does, so
/// the declared type and the produced bytes agree.
/// Re-types quoted literals to match the other arguments of the same call.
///
/// `COALESCE`, `LEAST`, `GREATEST` and `NULLIF` require their arguments to
/// agree, and each is parsed on its own — so `'2026-08-22 10:00:00'` became a
/// `STRING` while the column beside it was a `DATETIME`. The call then yielded
/// the text bytes, which written into an eight-byte timestamp slot read back as
/// the year 105224, and in a projection came back empty. Reported as success in
/// both cases.
///
/// Only quoted literals move. A column's type is a fact, and a number is
/// already typed.
fn unify_literal_types(args: &mut [Expr]) {
    // Beside timestamps a literal is read as their common type is: its zone
    // discarded for a `timestamp` (`coalesce(a, '… 10:00+02')`), applied for
    // a `timestamptz` (`common_timestamp`).
    let timestamps = args.iter().any(|a| !matches!(a, Expr::Null | Expr::Literal { .. }) && static_type(a) == DbType::DATETIME);
    let target = if timestamps {
        Some(if common_timestamp(args.iter()) { DbType::TIMESTAMP } else { DbType::DATETIME })
    } else {
        args.iter().find_map(|a| match a {
            Expr::Column(field) if field.db_type != DbType::STRING => Some(field.literal_type()),
            Expr::Cast(_, db_type) if *db_type != DbType::STRING => Some(db_type.clone()),
            _ => None,
        })
    };
    let Some(target) = target else { return };

    for arg in args.iter_mut() {
        let Expr::Literal { bytes, db_type } = arg else {
            continue;
        };
        if *db_type != DbType::STRING {
            continue;
        }
        let text = String::from_utf8_lossy(bytes).to_string();
        if let Some(encoded) =
            crate::core::value_codec::encode_literal(&target, text.trim_matches('\''))
        {
            *bytes = encoded;
            // A `timestamp` value is stored as the `DATETIME` it is.
            *db_type = if target == DbType::TIMESTAMP { DbType::DATETIME } else { target.clone() };
        }
    }
}

pub fn static_type(expr: &Expr) -> DbType {
    match expr {
        Expr::Null => DbType::STRING,
        Expr::Literal { db_type, .. } => db_type.clone(),
        Expr::Column(field) => field.db_type.clone(),
        // `->>` is text by definition; `->` stays a document.
        // Of a `json` column, `->` is the member's text too, typed `json`
        // (`yields_json_text`).
        Expr::JsonPath { as_text, column, .. } => {
            if *as_text || column.json_text {
                DbType::STRING
            } else {
                DbType::JSONB
            }
        }
        // As a column's path: `->>` text, `->` the base's own kind, a `json`
        // one travelling as text (`yields_json_text`).
        Expr::PathOf { base, as_text, .. } => {
            if *as_text || yields_json_text(base) {
                DbType::STRING
            } else {
                DbType::JSONB
            }
        }
        Expr::Scalar(func) => match func {
            crate::core::rql::aggregate::ScalarFunc::Now
            | crate::core::rql::aggregate::ScalarFunc::StatementTimestamp
            | crate::core::rql::aggregate::ScalarFunc::ClockTimestamp => DbType::DATETIME,
            crate::core::rql::aggregate::ScalarFunc::CurrentDate => DbType::DATETIME,
            crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => DbType::STRING,
            crate::core::rql::aggregate::ScalarFunc::UuidV7 => DbType::UUID,
            crate::core::rql::aggregate::ScalarFunc::Version => DbType::STRING,
        },
        // A `timestamp` is stored as the `DATETIME` it is; `yields_timestamp`
        // says which of the two it is.
        Expr::Cast(_, DbType::TIMESTAMP) => DbType::DATETIME,
        // A `json` value travels as the text it is (`yields_json_text`).
        Expr::Cast(_, DbType::JSONTEXT) => DbType::STRING,
        Expr::Cast(_, db_type) => db_type.clone(),
        Expr::NumericCast { .. } => DbType::DECIMAL,
        Expr::TypmodCast { value, .. } => static_type(value),
        Expr::Concat(lhs, rhs) if concat_is_jsonb(lhs, rhs) => DbType::JSONB,
        Expr::Concat(_, _) => DbType::STRING,
        Expr::Call(TextFunc::ColumnSize | TextFunc::RelationSize, _) => DbType::I64,
        Expr::Call(TextFunc::DateTrunc, _) => DbType::DATETIME,
        // `double precision`, which is what PostgreSQL's own `EXTRACT` returned
        // before it moved to `numeric`, and what every driver decodes without a
        // decimal type.
        Expr::Call(TextFunc::ExtractField, _) => DbType::DECIMAL,
        Expr::Call(TextFunc::DatePart, _) => DbType::F64,
        Expr::Call(TextFunc::UuidExtractTimestamp, _) => DbType::DATETIME,
        Expr::Call(TextFunc::UuidExtractVersion, _) => DbType::I16,
        // `integer`, as PostgreSQL's length family returns.
        Expr::Call(TextFunc::Length | TextFunc::OctetLength, _) => DbType::I32,
        Expr::Call(TextFunc::TsMatchVq | TextFunc::TsMatchTq | TextFunc::TsMatchTt, _) => DbType::BOOL,
        // The argument's own type, as PostgreSQL's `abs` family returns.
        Expr::Call(TextFunc::Abs, args) => args.first().map(static_type).unwrap_or(DbType::I32),
        // `round(numeric)` and `round(…, places)` are NUMERIC; `round` of an
        // integer or a float alone is `double precision`.
        Expr::Call(TextFunc::Round | TextFunc::Trunc, args) => {
            if args.len() == 1 && args.first().map(static_type) != Some(DbType::DECIMAL) {
                DbType::F64
            } else {
                DbType::DECIMAL
            }
        }
        Expr::Call(
            TextFunc::RegexpLike
            | TextFunc::CheckRegexpLike
            | TextFunc::CheckRegexpUncertain
            | TextFunc::Like
            | TextFunc::ILike
            | TextFunc::JsonbContains
            | TextFunc::JsonbHasKey,
            _,
        ) => DbType::BOOL,
        Expr::Call(TextFunc::Pg(func), args) => crate::core::rql::pg_functions::result_type(*func, args),
        Expr::Call(_, _) => DbType::STRING,
        // The branches are resolved to one type the way PostgreSQL's
        // `select_common_type` does — see `common_type`.
        Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => {
            common_type(args.iter()).0
        }
        // NULLIF's result is its first argument, typed as that argument
        // resolves on its own.
        Expr::NullIf(lhs, _) => common_type(std::iter::once(&**lhs)).0,
        Expr::Mod(lhs, _) => static_type(lhs),
        Expr::Case { whens, otherwise } => {
            common_type(
                whens
                    .iter()
                    .map(|(_, e)| e)
                    .chain(otherwise.iter().map(|b| b.as_ref())),
            )
            .0
        }
        // Mirrors `arith`: a float on either side makes the result a float,
        // then a NUMERIC on either side a NUMERIC, and everything else is
        // done in i64.
        Expr::Arith(lhs, op, rhs) => {
            let ty = |e: &Expr| static_type(e);
            let timestamp = |e: &Expr| matches!(ty(e), DbType::DATETIME);
            // The same ranks as `wider_integer`, unsigned types one up.
            let rank = |e: &Expr| match ty(e) {
                DbType::I8 | DbType::U8 => 1,
                DbType::I16 => 2,
                DbType::I32 | DbType::U16 => 3,
                _ => 4,
            };
            // An integer literal carries PostgreSQL's own type — `int4` when it
            // fits, `int8` when it does not — so the plain ranks give
            // PostgreSQL's answer: `counter - 1` over an `int4` stays `int4`,
            // `smallint * 2` is `int4`, and `n * 3000000000` is `int8`. A
            // literal used to take the other operand's rank whatever its
            // value, which described that last product as `int4`; `eval`
            // computed it in `int8`, and the row went out as its low four
            // bytes.
            let date = |e: &Expr| matches!(ty(e), DbType::DATE);
            if date(lhs) || date(rhs) {
                // Mirrors `date_arith`, and PostgreSQL's operators: a day
                // count moves a date and leaves a date, two dates differ by a
                // day count, and an interval or a time of day makes it a
                // timestamp.
                let other = if date(lhs) { ty(rhs) } else { ty(lhs) };
                return match other {
                    DbType::DATE => DbType::I32,
                    DbType::DATETIME if matches!(op, ArithOp::Sub) => DbType::INTERVAL,
                    DbType::DATETIME | DbType::INTERVAL => DbType::DATETIME,
                    _ => DbType::DATE,
                };
            }
            if timestamp(lhs) && timestamp(rhs) && matches!(op, ArithOp::Sub) {
                // The difference of two instants is a span, not a bigint. It
                // was described as `int8` before the type existed, and a client
                // asking to decode a duration refused the column.
                DbType::INTERVAL
            } else if timestamp(lhs) || timestamp(rhs) {
                DbType::DATETIME
            } else if matches!(ty(lhs), DbType::INTERVAL) && matches!(ty(rhs), DbType::INTERVAL) {
                DbType::INTERVAL
            } else if (matches!(ty(lhs), DbType::INTERVAL) || matches!(ty(rhs), DbType::INTERVAL))
                && matches!(op, ArithOp::Mul | ArithOp::Div)
            {
                DbType::INTERVAL
            } else if matches!(ty(lhs), DbType::F32 | DbType::F64)
                || matches!(ty(rhs), DbType::F32 | DbType::F64)
            {
                DbType::F64
            } else if matches!(ty(lhs), DbType::DECIMAL) || matches!(ty(rhs), DbType::DECIMAL) {
                DbType::DECIMAL
            } else {
                // Mirrors `wider_integer`: the result keeps the wider operand's
                // type rather than widening everything to `bigint`.
                match rank(lhs).max(rank(rhs)) {
                    1 | 2 => DbType::I16,
                    3 => DbType::I32,
                    _ => DbType::I64,
                }
            }
        }
    }
}

/// The scale PostgreSQL displays a `NUMERIC` expression at, where it does not
/// depend on the values: a constant's own places, a column's declared `s`, 0
/// for an integer, the larger of the two for `+`, `-` and `%`, and their sum
/// for `*`. `20.17` for `14 * 1.37 + 0.99`, and `1.650` for a `NUMERIC(10,2)`
/// times `1.5`.
///
/// `None` where it is not known before the row is: a quotient (PostgreSQL
/// picks its places from the operands' magnitudes), an unconstrained `NUMERIC`
/// column (whose values each carry their own), anything that is not a
/// `NUMERIC`. Those are evaluated, and rendered, at the engine scale.
///
/// A product is computed at the engine scale, so its places stop there. A
/// constant's places are those its value needs, so `1.50` counts as `1.5`.
pub fn display_scale(expr: &Expr) -> Option<u32> {
    use crate::core::value_codec::DECIMAL_SCALE;
    let integer = |t: &DbType| {
        matches!(
            t,
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
        )
    };
    match expr {
        Expr::Literal {
            bytes,
            db_type: DbType::DECIMAL,
        } => {
            let mut raw = i128::from_le_bytes(bytes.as_slice().try_into().ok()?);
            let mut scale = DECIMAL_SCALE;
            while scale > 0 && raw % 10 == 0 {
                raw /= 10;
                scale -= 1;
            }
            Some(scale)
        }
        Expr::Literal { db_type, .. } if integer(db_type) => Some(0),
        Expr::Column(field) if field.db_type == DbType::DECIMAL => field.scale,
        Expr::Column(field) if integer(&field.db_type) => Some(0),
        Expr::Arith(lhs, op, rhs) if static_type(expr) == DbType::DECIMAL => {
            let (l, r) = (display_scale(lhs)?, display_scale(rhs)?);
            match op {
                ArithOp::Add | ArithOp::Sub | ArithOp::Rem => Some(l.max(r)),
                ArithOp::Mul => Some((l + r).min(DECIMAL_SCALE)),
                ArithOp::Div => None,
            }
        }
        Expr::Arith(..) if integer(&static_type(expr)) => Some(0),
        Expr::Call(TextFunc::Abs, args) => args.first().and_then(display_scale),
        // A date's epoch has no fraction, and PostgreSQL shows it with none.
        Expr::Call(TextFunc::ExtractField, args) => args.first().and_then(field_literal).map(|field| {
            if field == "EPOCH" && args.get(1).map(static_type) == Some(DbType::DATE) { 0 } else { field_scale(field) }
        }),
        Expr::Call(TextFunc::Round | TextFunc::Trunc, args) if static_type(expr) == DbType::DECIMAL => match args.get(1) {
            None => Some(0),
            Some(places) => round_places(places).map(|p| p.max(0) as u32).filter(|s| *s <= DECIMAL_SCALE),
        },
        Expr::NumericCast { scale, .. } => Some(*scale),
        // A cast to plain `numeric` keeps the places of an exact value.
        Expr::Cast(inner, DbType::DECIMAL)
            if matches!(static_type(inner), DbType::DECIMAL) || integer(&static_type(inner)) =>
        {
            display_scale(inner)
        }
        _ => None,
    }
}

/// A `NUMERIC` value, computed at the engine scale, brought to the display
/// scale its expression was given; anything else unchanged. Exact for every
/// scale [`display_scale`] gives, which never exceeds the engine's.
pub fn at_display_scale(value: Value, scale: Option<u32>) -> Result<Value, String> {
    use crate::core::value_codec::DECIMAL_SCALE;
    let Some(scale) = scale.filter(|s| *s != DECIMAL_SCALE) else {
        return Ok(value);
    };
    match value {
        Value::Data {
            bytes,
            db_type: DbType::DECIMAL,
        } if bytes.len() == 16 => {
            let raw = i128::from_le_bytes(bytes.as_slice().try_into().expect("sixteen bytes"));
            let shown = round_decimal(raw, DECIMAL_SCALE, scale)
                .ok_or_else(|| "value overflows numeric format".to_string())?;
            Ok(Value::Data {
                bytes: shown.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            })
        }
        other => Ok(other),
    }
}

/// Applies a binary arithmetic operator to two non-NULL values.
///
/// Integers stay integers and floats stay floats; a mixed pair is done in
/// floating point, which is the only common type the two have. Anything else —
/// `DECIMAL` above all, whose scaled representation would need its own
/// handling — is refused rather than decoded as though it were an integer,
/// because reading scaled cents as a plain `i64` would return a number that is
/// wrong by a factor of the scale and report success.
pub(crate) fn arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
    let float_operand = |v: &Value| {
        matches!(
            v,
            Value::Data {
                db_type: DbType::F32 | DbType::F64,
                ..
            }
        )
    };
    let is_timestamp = |v: &Value| {
        matches!(
            v,
            Value::Data {
                db_type: DbType::DATETIME,
                ..
            }
        )
    };
    let is_interval = |v: &Value| {
        matches!(
            v,
            Value::Data {
                db_type: DbType::INTERVAL,
                ..
            }
        )
    };

    if matches!(a.db_type(), Some(DbType::DATE)) || matches!(b.db_type(), Some(DbType::DATE)) {
        return date_arith(a, op, b);
    }

    // Interval ± interval is an interval, with the three fields kept apart —
    // a month is not thirty days, and collapsing them here would make
    // `INTERVAL '1 month' + INTERVAL '1 day'` print as 31 days.
    if is_interval(a) && is_interval(b) {
        if !matches!(op, ArithOp::Add | ArithOp::Sub) {
            return Err(format!(
                "'{}' is not defined for two intervals",
                op.symbol()
            ));
        }
        let (Value::Data { bytes: x, .. }, Value::Data { bytes: y, .. }) = (a, b) else {
            return Err("an interval has no value".to_string());
        };
        let (Some(x), Some(y)) = (
            crate::core::pg_types::decode_interval(x),
            crate::core::pg_types::decode_interval(y),
        ) else {
            return Err("an interval could not be read".to_string());
        };
        // Checked, as PostgreSQL's `interval_pl`: past a field's range is
        // "interval out of range", never a wrapped value.
        let out = || "interval out of range".to_string();
        let sum = if matches!(op, ArithOp::Add) {
            crate::core::pg_types::Interval {
                months: x.months.checked_add(y.months).ok_or_else(out)?,
                days: x.days.checked_add(y.days).ok_or_else(out)?,
                micros: x.micros.checked_add(y.micros).ok_or_else(out)?,
            }
        } else {
            crate::core::pg_types::Interval {
                months: x.months.checked_sub(y.months).ok_or_else(out)?,
                days: x.days.checked_sub(y.days).ok_or_else(out)?,
                micros: x.micros.checked_sub(y.micros).ok_or_else(out)?,
            }
        };
        return Ok(Value::Data {
            bytes: crate::core::pg_types::encode_interval(sum),
            db_type: DbType::INTERVAL,
        });
    }

    // `interval * n`, `n * interval`, `interval / n`: PostgreSQL's
    // `interval_mul` and `interval_div`, a fraction of a month carried into
    // days and of a day into the time (`pg_types::interval_div`).
    if (is_interval(a) || is_interval(b)) && matches!(op, ArithOp::Mul | ArithOp::Div) {
        let (span, number) = if is_interval(a) { (a, b) } else { (b, a) };
        if is_interval(number) || (matches!(op, ArithOp::Div) && !is_interval(a)) {
            return Err(format!("operator does not exist: {} {} {}", if is_interval(a) { "interval" } else { "number" }, op.symbol(), "interval"));
        }
        let Value::Data { bytes, .. } = span else {
            return Ok(Value::Null);
        };
        let iv = crate::core::pg_types::decode_interval(bytes).ok_or("an interval could not be read")?;
        let factor = match number {
            Value::Null => return Ok(Value::Null),
            Value::Data { bytes, db_type: DbType::DECIMAL } => decimal_value_as_pgnum(bytes)?.to_text().parse::<f64>().map_err(|e| e.to_string())?,
            other => as_f64(other).ok_or_else(|| format!("'{}' needs a number beside an interval", op.symbol()))?,
        };
        let scaled = match op {
            ArithOp::Div if factor == 0.0 => return Err("division by zero".to_string()),
            ArithOp::Div => crate::core::pg_types::interval_div(iv, factor),
            _ => crate::core::pg_types::interval_mul(iv, factor),
        }
        .ok_or("interval out of range")?;
        return Ok(Value::Data { bytes: crate::core::pg_types::encode_interval(scaled), db_type: DbType::INTERVAL });
    }

    // A timestamp plus an interval is a timestamp; the difference of two
    // timestamps is an interval. Getting the resulting *type* wrong would hand
    // a DATETIME column a bare integer, or describe a duration as a bigint.
    if is_timestamp(a) || is_timestamp(b) {
        if !matches!(op, ArithOp::Add | ArithOp::Sub) {
            return Err(format!("'{}' is not defined for a timestamp", op.symbol()));
        }

        // An interval operand may be a real `INTERVAL` value or the plain
        // microsecond count an `INTERVAL '…'` literal parses to. Only the
        // former can carry months and days, which shift the calendar rather
        // than a fixed number of microseconds.
        if is_timestamp(a) && is_interval(b) {
            let (Some(instant), Value::Data { bytes, .. }) = (as_i64(a), b) else {
                return Err("a timestamp can only be offset by an interval".to_string());
            };
            let Some(mut span) = crate::core::pg_types::decode_interval(bytes) else {
                return Err("an interval could not be read".to_string());
            };
            if matches!(op, ArithOp::Sub) {
                span = crate::core::pg_types::Interval {
                    months: -span.months,
                    days: -span.days,
                    micros: -span.micros,
                };
            }
            let shifted = crate::core::pg_types::shift_timestamp(instant, span)
                .ok_or_else(|| "timestamp arithmetic overflowed".to_string())?;
            return Ok(Value::Data {
                bytes: shifted.to_le_bytes().to_vec(),
                db_type: DbType::DATETIME,
            });
        }

        let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) else {
            return Err("a timestamp can only be offset by an interval".to_string());
        };
        let value = match op {
            ArithOp::Add => x.checked_add(y),
            _ => x.checked_sub(y),
        }
        .ok_or_else(|| "timestamp arithmetic overflowed".to_string())?;

        // Subtracting one timestamp from another leaves an interval, not a
        // point in time — and not a bigint either, which is what it used to
        // report before the type existed.
        if is_timestamp(a) && is_timestamp(b) && matches!(op, ArithOp::Sub) {
            return Ok(Value::Data {
                bytes: crate::core::pg_types::encode_interval(crate::core::pg_types::interval_between(value)),
                db_type: DbType::INTERVAL,
            });
        }
        return Ok(Value::Data {
            bytes: value.to_le_bytes().to_vec(),
            db_type: DbType::DATETIME,
        });
    }

    // `NUMERIC` is an integer in scaled units, so it cannot share the plain
    // integer path: adding two of them works, but multiplying leaves the
    // product scaled twice and dividing loses the scale entirely.
    //
    // After the float path, as PostgreSQL resolves `numeric` against a float:
    // the result is `float8`. A written fraction is `numeric` itself now, so
    // `price * 1.5` is two NUMERICs and never reaches the float path, while
    // `price * 1.5::float8` is the float the author asked for.
    if (is_decimal(a) || is_decimal(b)) && !float_operand(a) && !float_operand(b) {
        return decimal_arith(a, op, b);
    }

    if float_operand(a) || float_operand(b) {
        let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) else {
            return Err(format!("'{}' needs numeric operands", op.symbol()));
        };
        let value = match op {
            ArithOp::Add => x + y,
            ArithOp::Sub => x - y,
            ArithOp::Mul => x * y,
            ArithOp::Div => {
                if y == 0.0 {
                    return Err("division by zero".to_string());
                }
                x / y
            }
            ArithOp::Rem => {
                if y == 0.0 {
                    return Err("division by zero".to_string());
                }
                x % y
            }
        };
        return Ok(Value::Data {
            bytes: value.to_le_bytes().to_vec(),
            db_type: DbType::F64,
        });
    }

    let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) else {
        return Err(format!("'{}' needs numeric operands", op.symbol()));
    };
    // PostgreSQL keeps the wider of the two operand types rather than widening
    // everything to `bigint`. Reporting `int8` for `int4 + int4` is rejected
    // outright by a typed client — the values are right, the declared type is
    // not.
    let db_type = wider_integer(a, b);
    // Overflow is an error, not a wrap: a silently wrapped total is exactly the
    // wrong answer reported as success. It is PostgreSQL's error, naming the
    // result type, so the wire layer reports 22003.
    let out_of_range = || integer_out_of_range(&db_type);
    let value = match op {
        ArithOp::Add => x.checked_add(y),
        ArithOp::Sub => x.checked_sub(y),
        ArithOp::Mul => x.checked_mul(y),
        ArithOp::Div => {
            if y == 0 {
                return Err("division by zero".to_string());
            }
            x.checked_div(y)
        }
        ArithOp::Rem => {
            if y == 0 {
                return Err("division by zero".to_string());
            }
            // `MIN % -1` is 0 in PostgreSQL; only the quotient overflows.
            if y == -1 { Some(0) } else { x.checked_rem(y) }
        }
    }
    .ok_or_else(out_of_range)?;

    let bytes = narrow_integer(value, &db_type).ok_or_else(out_of_range)?;
    Ok(Value::Data { bytes, db_type })
}

/// PostgreSQL's error for an integer result its type cannot hold:
/// `integer out of range`, and likewise for `smallint` and `bigint`.
pub(crate) fn integer_out_of_range(db_type: &DbType) -> String {
    format!(
        "{} out of range",
        match db_type {
            DbType::I8 | DbType::U8 | DbType::I16 | DbType::U16 => "smallint",
            DbType::I32 | DbType::U32 => "integer",
            _ => "bigint",
        }
    )
}

/// Renders a value as the text SQL would print.
/// Whether `lhs || rhs` is `jsonb`'s concatenation rather than text's: both
/// sides are documents, or one is and the other is a written string — an
/// `unknown` literal, which PostgreSQL resolves to the other side's type.
/// A typed text (a `TEXT` column, `'x'::text`) beside a document is text's
/// `anynonarray || text`, the document rendered as JSON.
///
/// Concatenating as text always put the document's encoded bytes beside the
/// literal: `SET doc = doc || '{"b":1}'` stored a document with text trailing
/// it, which read back as the old document, and `SET doc = '{"a":1}' ||
/// '{"b":1}'` stored text no document reader could open.
pub(crate) fn concat_is_jsonb(lhs: &Expr, rhs: &Expr) -> bool {
    let unknown = |e: &Expr| matches!(e, Expr::Null | Expr::Literal { db_type: DbType::STRING, .. });
    let document = |e: &Expr| static_type(e) == DbType::JSONB;
    (document(lhs) && (document(rhs) || unknown(rhs))) || (document(rhs) && unknown(lhs))
}

/// An operand of `jsonb ||` as an encoded document: a document as it is, a
/// string literal read as JSON, refused as PostgreSQL's `jsonb` input
/// refuses it.
fn as_jsonb(value: Value) -> Result<Vec<u8>, String> {
    match value {
        Value::Data { bytes, db_type: DbType::JSONB } => Ok(bytes),
        Value::Data { bytes, db_type } => {
            let text = as_text(&Value::Data { bytes, db_type });
            crate::core::jsonb::encode_text(&text)
                .map_err(|_| crate::core::value_codec::refusal_message(&DbType::JSONB, &text))
        }
        Value::Null => Err("a NULL operand reached jsonb concatenation".to_string()),
    }
}

/// The values an integer type holds, `None` for any other type.
fn integer_range(db_type: &DbType) -> Option<std::ops::RangeInclusive<i128>> {
    Some(match db_type {
        DbType::I8 => i8::MIN as i128..=i8::MAX as i128,
        DbType::I16 => i16::MIN as i128..=i16::MAX as i128,
        DbType::I32 => i32::MIN as i128..=i32::MAX as i128,
        DbType::I64 => i64::MIN as i128..=i64::MAX as i128,
        DbType::U8 => 0..=u8::MAX as i128,
        DbType::U16 => 0..=u16::MAX as i128,
        DbType::U32 => 0..=u32::MAX as i128,
        DbType::U64 => 0..=u64::MAX as i128,
        _ => return None,
    })
}

/// A boolean's text as its cast to text gives it.
fn bool_text(bytes: &[u8]) -> &'static str {
    if bytes.first().copied().unwrap_or(0) != 0 { "true" } else { "false" }
}

/// PostgreSQL's integer input (`pg_strtoint`): surrounding whitespace, an
/// optional sign, then decimal digits, or `0x`/`0o`/`0b` and digits of that
/// base, with single underscores between digits. Nothing else: no fraction,
/// no exponent. `None` when the text is not one, or does not fit an `i128`.
pub(crate) fn pg_integer_text(text: &str) -> Option<i128> {
    let t = text.trim_matches(|c: char| c.is_ascii_whitespace());
    let (negative, t) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let (radix, digits) = match t.get(..2) {
        Some("0x") | Some("0X") => (16, &t[2..]),
        Some("0o") | Some("0O") => (8, &t[2..]),
        Some("0b") | Some("0B") => (2, &t[2..]),
        _ => (10, t),
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') || digits.contains("__") {
        return None;
    }
    let mut value: i128 = 0;
    for c in digits.chars().filter(|&c| c != '_') {
        value = value.checked_mul(radix)?.checked_add(c.to_digit(radix as u32)? as i128)?;
    }
    Some(if negative { -value } else { value })
}

pub(crate) fn as_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Data { bytes, db_type } => match db_type {
            DbType::STRING => String::from_utf8_lossy(bytes).to_string(),
            DbType::CHAR => crate::core::value_codec::legacy_char_text(bytes).to_string(),
            // A document's text is its JSON, not its encoding.
            DbType::JSONB => {
                let mut out = Vec::with_capacity(bytes.len());
                match crate::core::jsonb::render_text_into(bytes, &mut out) {
                    Ok(()) => String::from_utf8_lossy(&out).into_owned(),
                    Err(_) => String::from_utf8_lossy(bytes).to_string(),
                }
            }
            DbType::UUID => crate::core::value_codec::format_uuid(bytes),
            // A number with a fraction has no integer reading, and its raw
            // bytes are not text: `'r' || 1.5` concatenated the float's eight
            // bytes.
            DbType::DECIMAL | DbType::F32 | DbType::F64 | DbType::DATE | DbType::DATETIME => {
                crate::core::rql::subquery::value_to_literal(db_type, bytes)
                    .map(|text| crate::core::value_codec::unquote_sql(&text).to_string())
                    .unwrap_or_else(|| String::from_utf8_lossy(bytes).to_string())
            }
            _ => match as_i64(value) {
                Some(v) => v.to_string(),
                None => String::from_utf8_lossy(bytes).to_string(),
            },
        },
    }
}

/// Applies a function to its evaluated arguments.
fn call(func: TextFunc, args: &[Value], row: &Row) -> Result<Value, String> {
    if let TextFunc::Pg(func) = func {
        return crate::core::rql::pg_functions::call(func, args);
    }
    let string = |text: String| Value::Data {
        bytes: text.into_bytes(),
        db_type: DbType::STRING,
    };

    // Strict in PostgreSQL: a NULL argument makes the result NULL. Read through
    // `as_text`, NULL is the empty string, so `lower(NULL)` came back as '', a
    // real value: a generated column stored it, `IS NULL` missed it, and a
    // unique index refused a second NULL as its duplicate.
    if matches!(
        func,
        TextFunc::Lower
            | TextFunc::Upper
            | TextFunc::QuoteIdent
            | TextFunc::ToTsVector
            | TextFunc::ToTsQuery
            | TextFunc::PlainToTsQuery
            | TextFunc::PhraseToTsQuery
            | TextFunc::WebsearchToTsQuery
            | TextFunc::TsMatchVq
            | TextFunc::TsMatchTq
            | TextFunc::TsMatchTt
    ) && args.iter().any(|a| a.is_null())
    {
        return Ok(Value::Null);
    }

    if matches!(
        func,
        TextFunc::Btrim
            | TextFunc::Ltrim
            | TextFunc::Rtrim
            | TextFunc::Rpad
            | TextFunc::Lpad
            | TextFunc::JsonbTypeof
            | TextFunc::RegexpLike
            | TextFunc::CheckRegexpLike
            | TextFunc::SimilarToEscape
            | TextFunc::Like
            | TextFunc::ILike
            | TextFunc::JsonbContains
            | TextFunc::JsonbHasKey
    ) && args.iter().any(|a| a.is_null())
    {
        return Ok(Value::Null);
    }

    Ok(match func {
        TextFunc::JsonbContains => {
            let (outer, inner) = (as_jsonb(args[0].clone())?, as_jsonb(args[1].clone())?);
            let found = crate::core::jsonb::contains(&outer, &inner).ok_or("a stored document could not be read")?;
            Value::Data { bytes: vec![u8::from(found)], db_type: DbType::BOOL }
        }
        TextFunc::JsonbHasKey => {
            let doc = as_jsonb(args[0].clone())?;
            let found = crate::core::jsonb::key_exists(&doc, &as_text(&args[1])).ok_or("a stored document could not be read")?;
            Value::Data { bytes: vec![u8::from(found)], db_type: DbType::BOOL }
        }
        TextFunc::Like | TextFunc::ILike => {
            let (value, pattern) = (as_text(&args[0]), as_text(&args[1]));
            let matched = if func == TextFunc::ILike {
                crate::core::row::common::like_match(value.to_lowercase().as_bytes(), pattern.to_lowercase().as_bytes(), false)
            } else {
                crate::core::row::common::like_match(value.as_bytes(), pattern.as_bytes(), false)
            };
            Value::Data { bytes: vec![u8::from(matched)], db_type: DbType::BOOL }
        }
        // Answered above, before the strictness checks of this module.
        TextFunc::Pg(func) => return crate::core::rql::pg_functions::call(func, args),
        // NULL arguments: nothing to vouch for (an answer of NULL is exact).
        TextFunc::CheckRegexpUncertain => {
            let flags = args.get(2).map(as_text).unwrap_or_default();
            let uncertain = !args.iter().any(|a| a.is_null())
                && crate::core::rql::pg_regex::compile_cached(&as_text(&args[1]), &flags)
                    .is_ok_and(|re| re.matches(&as_text(&args[0])).is_none());
            Value::Data { bytes: vec![u8::from(uncertain)], db_type: DbType::BOOL }
        }
        TextFunc::RegexpLike | TextFunc::CheckRegexpLike => {
            let flags = args.get(2).map(as_text).unwrap_or_default();
            // A pattern or flags outside the translated subset — here only
            // ones read from the row, as a constant one leaves the check
            // unenforced where it is declared — refuse the row (0A000), and
            // an invalid one is PostgreSQL's 2201B: neither is "not judged",
            // which plain ASCII data reached that way. Only a value whose
            // answer depends on the locale's classes is not judged.
            let re = crate::core::rql::pg_regex::compile_cached(&as_text(&args[1]), &flags).map_err(|e| e.message())?;
            match (re.matches(&as_text(&args[0])), func) {
                (Some(matched), _) => Value::Data { bytes: vec![u8::from(matched)], db_type: DbType::BOOL },
                // Inside a CHECK it is UNKNOWN, which a monotone check
                // still judges (`compile_check`).
                (None, TextFunc::CheckRegexpLike) => {
                    CHECK_UNCERTAIN.with(|u| {
                        u.borrow_mut().get_or_insert_with(|| {
                            "a locale-dependent class or case-insensitivity met a non-ASCII value".to_string()
                        });
                    });
                    Value::Null
                }
                (None, _) => {
                    return Err(crate::core::rql::pg_regex::PgRegexError::Unsupported(
                        "a locale-dependent class or case-insensitivity over a non-ASCII value".to_string(),
                    )
                    .message())
                }
            }
        }
        TextFunc::SimilarToEscape => {
            let escape = match args.get(1) {
                Some(e) => {
                    let e = as_text(e);
                    let mut chars = e.chars();
                    match (chars.next(), chars.next()) {
                        (None, _) => None,
                        (Some(c), None) => Some(c),
                        _ => return Err("invalid escape string (it must be empty or one character)".to_string()),
                    }
                }
                None => Some('\\'),
            };
            string(crate::core::rql::pg_regex::similar_to_escape(&as_text(&args[0]), escape)?)
        }
        TextFunc::Btrim | TextFunc::Ltrim | TextFunc::Rtrim => {
            let text = as_text(&args[0]);
            let set: Vec<char> = match args.get(1) {
                Some(chars) => as_text(chars).chars().collect(),
                None => vec![' '],
            };
            let trimmed = match func {
                TextFunc::Btrim => text.trim_matches(|c| set.contains(&c)),
                TextFunc::Ltrim => text.trim_start_matches(|c| set.contains(&c)),
                _ => text.trim_end_matches(|c| set.contains(&c)),
            };
            string(trimmed.to_string())
        }
        TextFunc::Rpad | TextFunc::Lpad => {
            let text = as_text(&args[0]);
            let length = match &args[1] {
                Value::Data { bytes, db_type } => integer_argument(bytes, db_type)
                    .ok_or_else(|| format!("{} needs an integer length", if func == TextFunc::Rpad { "rpad" } else { "lpad" }))?,
                Value::Null => return Ok(Value::Null),
            };
            let fill: Vec<char> = match args.get(2) {
                Some(f) => as_text(f).chars().collect(),
                None => vec![' '],
            };
            let length = length.clamp(0, i64::from(crate::core::rql::type_modifier::MAX_TEXT_LENGTH)) as usize;
            let chars: Vec<char> = text.chars().collect();
            if chars.len() >= length || fill.is_empty() {
                string(chars.iter().take(length).collect())
            } else {
                let padding: String = fill.iter().cycle().take(length - chars.len()).collect();
                string(if func == TextFunc::Rpad { format!("{text}{padding}") } else { format!("{padding}{text}") })
            }
        }
        TextFunc::JsonbTypeof => {
            let Value::Data { bytes, db_type } = &args[0] else {
                return Ok(Value::Null);
            };
            let document = if *db_type == DbType::JSONB {
                crate::core::jsonb::decode(bytes)?
            } else {
                serde_json::from_str(&as_text(&args[0]))
                    .map_err(|e| format!("invalid input syntax for type json: {}", e))?
            };
            string(
                match document {
                    serde_json::Value::Object(_) => "object",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Null => "null",
                }
                .to_string(),
            )
        }
        // Always `public`: this engine has one schema, and reporting anything
        // else would make a qualified name the caller then builds unresolvable.
        TextFunc::CurrentSchema => string("public".to_string()),
        TextFunc::QuoteIdent => {
            let raw = as_text(&args[0]);
            // PostgreSQL quotes only what needs it: anything that is not a
            // plain lower-case identifier.
            let plain = !raw.is_empty()
                && raw
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
                && raw
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            string(if plain {
                raw
            } else {
                format!("\"{}\"", raw.replace('"', "\"\""))
            })
        }
        TextFunc::Replace => {
            if args.iter().any(|a| a.is_null()) {
                return Ok(Value::Null);
            }
            string(as_text(&args[0]).replace(&as_text(&args[1]), &as_text(&args[2])))
        }
        // A timestamp floored to a unit, which is what a `GROUP BY` over time
        // buckets on. Microseconds since the epoch, so truncation is integer
        // arithmetic — except months and years, whose length varies.
        TextFunc::DateTrunc => {
            let unit = as_text(&args[0]).trim().to_ascii_lowercase();
            let Value::Data { bytes, db_type } = &args[1] else {
                return Ok(Value::Null);
            };
            // A date is truncated as its midnight, the timestamp PostgreSQL
            // casts it to. Only eight bytes were read, so every date —
            // `x::date`, `CURRENT_DATE - 7` — came back NULL, and a GROUP BY
            // over it put every row in one NULL bucket.
            let us = match db_type {
                DbType::DATE => instant_of(db_type, bytes),
                _ => <[u8; 8]>::try_from(bytes.as_slice()).ok().map(i64::from_le_bytes),
            };
            let Some(us) = us else {
                return Ok(Value::Null);
            };
            let Some(truncated) = crate::core::value_codec::truncate_timestamp_us(us, &unit) else {
                return Err(format!(
                    "date_trunc does not know the unit '{}'; it takes microseconds, \
                     milliseconds, second, minute, hour, day, week, month, quarter or year",
                    unit
                ));
            };
            Value::Data {
                bytes: truncated.to_le_bytes().to_vec(),
                db_type: DbType::DATETIME,
            }
        }
        TextFunc::Lower => string(as_text(&args[0]).to_lowercase()),
        TextFunc::Upper => string(as_text(&args[0]).to_uppercase()),

        // Full-text search. Both sides carry their canonical text form — see
        // `crate::core::text_search` for why that rather than a packed layout —
        // so a `tsvector` and a `tsquery` are strings here, and `@@` compares
        // them.
        TextFunc::ToTsVector => string(crate::core::text_search::to_tsvector(
            &as_text(&args[0]),
            &as_text(&args[1]),
        )),
        TextFunc::ToTsQuery => {
            match crate::core::text_search::to_tsquery(&as_text(&args[0]), &as_text(&args[1])) {
                Ok(query) => string(query),
                // A malformed query is reported rather than silently matching
                // nothing, which is what PostgreSQL does too.
                Err(reason) => return Err(reason),
            }
        }
        TextFunc::PlainToTsQuery => string(crate::core::text_search::plainto_tsquery(
            &as_text(&args[0]),
            &as_text(&args[1]),
        )),
        TextFunc::PhraseToTsQuery => string(crate::core::text_search::phraseto_tsquery(
            &as_text(&args[0]),
            &as_text(&args[1]),
        )),
        TextFunc::WebsearchToTsQuery => string(crate::core::text_search::websearch_to_tsquery(
            &as_text(&args[0]),
            &as_text(&args[1]),
        )),
        TextFunc::TsMatchVq | TextFunc::TsMatchTq | TextFunc::TsMatchTt => {
            use crate::core::text_search as ts;
            let left = as_text(&args[0]);
            let right = as_text(&args[1]);
            let vector = if func == TextFunc::TsMatchVq { left } else { ts::to_tsvector("english", &left) };
            let query = if func == TextFunc::TsMatchTt { ts::plainto_tsquery("english", &right) } else { right };
            Value::Data { bytes: vec![u8::from(ts::matches(&vector, &query))], db_type: DbType::BOOL }
        }

        // `array_length(a, 1)` is NULL for an empty array, as in PostgreSQL —
        // not zero, which is the guess that makes an emptiness test read the
        // wrong way round.
        TextFunc::ArrayLength => {
            let Value::Data { bytes, .. } = &args[0] else {
                return Ok(Value::Null);
            };
            let dimension = as_text(&args[1]).trim().parse::<i32>().unwrap_or(1);
            // The dimension's length, NULL past the array's dimensions and
            // for an empty array, as PostgreSQL's.
            let Some((_, dims, _)) = crate::core::value_codec::decode_array_dims(bytes) else {
                return Ok(Value::Null);
            };
            match usize::try_from(dimension).ok().and_then(|d| d.checked_sub(1)).and_then(|d| dims.get(d)) {
                Some(len) => Value::Data { bytes: (*len as i32).to_le_bytes().to_vec(), db_type: DbType::I32 },
                None => return Ok(Value::Null),
            }
        }
        // Always one: the array storage carries a flat element list, so a
        // multi-dimensional array declared `DOUBLE PRECISION[][]` is stored and
        // reported as one-dimensional. Stated here rather than left to be
        // discovered — PostgreSQL would answer 2.
        // NULL for a UUID of another version or variant, as in PostgreSQL. The
        // argument is a `uuid` value, or text that parses as one.
        TextFunc::UuidExtractTimestamp | TextFunc::UuidExtractVersion => {
            let Value::Data { bytes, db_type } = &args[0] else {
                return Ok(Value::Null);
            };
            let uuid: [u8; 16] = match db_type {
                DbType::UUID => match bytes[..].try_into() {
                    Ok(u) => u,
                    Err(_) => return Ok(Value::Null),
                },
                _ => crate::core::value_codec::parse_uuid(&as_text(&args[0]))
                    .ok_or_else(|| {
                        format!(
                            "invalid input syntax for type uuid: {}",
                            crate::core::value_codec::double_quoted(&as_text(&args[0]))
                        )
                    })?,
            };
            if func == TextFunc::UuidExtractVersion {
                match crate::core::rql::aggregate::uuid_extract_version(&uuid) {
                    Some(v) => Value::Data { bytes: v.to_le_bytes().to_vec(), db_type: DbType::I16 },
                    None => Value::Null,
                }
            } else {
                match crate::core::rql::aggregate::uuid_extract_timestamp_us(&uuid) {
                    Some(us) => Value::Data { bytes: us.to_le_bytes().to_vec(), db_type: DbType::DATETIME },
                    None => Value::Null,
                }
            }
        }
        TextFunc::Length | TextFunc::OctetLength => {
            let Value::Data { bytes, db_type } = &args[0] else {
                return Ok(Value::Null);
            };
            let n = match (func, db_type) {
                // Characters, not bytes, for text; bytes for bytea and for
                // octet_length. A text value that is not valid UTF-8 counts
                // its bytes rather than inventing characters.
                (TextFunc::Length, DbType::CHAR) => crate::core::value_codec::legacy_char_text(bytes).chars().count(),
                (TextFunc::Length, DbType::STRING) => match std::str::from_utf8(bytes) {
                    Ok(text) => text.chars().count(),
                    Err(_) => bytes.len(),
                },
                (TextFunc::Length, _) if !matches!(db_type, DbType::BYTEA) => as_text(&args[0]).chars().count(),
                _ => bytes.len(),
            };
            Value::Data {
                bytes: (n as i32).to_le_bytes().to_vec(),
                db_type: DbType::I32,
            }
        }
        // Checked like every other integer result: the magnitude of the
        // smallest value of a type is one past its largest.
        TextFunc::Abs => {
            let value = &args[0];
            let Value::Data { bytes, db_type } = value else {
                return Ok(Value::Null);
            };
            match db_type {
                DbType::DECIMAL => {
                    let raw = i128::from_le_bytes(
                        bytes.as_slice().try_into().map_err(|_| "a numeric could not be read".to_string())?,
                    );
                    Value::Data {
                        bytes: raw
                            .checked_abs()
                            .ok_or_else(|| "value overflows numeric format".to_string())?
                            .to_le_bytes()
                            .to_vec(),
                        db_type: DbType::DECIMAL,
                    }
                }
                DbType::F32 => Value::Data {
                    bytes: as_f64(value).map(|f| (f.abs() as f32).to_le_bytes().to_vec()).unwrap_or_default(),
                    db_type: DbType::F32,
                },
                DbType::F64 => Value::Data {
                    bytes: as_f64(value).map(|f| f.abs().to_le_bytes().to_vec()).unwrap_or_default(),
                    db_type: DbType::F64,
                },
                _ => {
                    let n = as_i64(value)
                        .filter(|_| *db_type != DbType::DATETIME)
                        .ok_or_else(|| format!("abs is not defined for {}", db_type))?;
                    let ty = wider_integer(value, value);
                    let magnitude = n.checked_abs().ok_or_else(|| integer_out_of_range(&ty))?;
                    Value::Data {
                        bytes: narrow_integer(magnitude, &ty).ok_or_else(|| integer_out_of_range(&ty))?,
                        db_type: ty,
                    }
                }
            }
        }
        TextFunc::ExtractField | TextFunc::DatePart => {
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let field = date_field(&as_text(&args[0]).to_ascii_uppercase())?;
            // A field read from the row is checked against the value here,
            // as a constant one is when the call is parsed.
            if let Value::Data { db_type: db_type @ (DbType::DATE | DbType::INTERVAL), .. } = &args[1] {
                date_field_applies(field, db_type, func == TextFunc::ExtractField)?;
            }
            match date_field_value(field, &args[1])? {
                None => Value::Null,
                Some(v) if func == TextFunc::ExtractField => pgnum_as_decimal_value(v),
                Some(v) => Value::Data {
                    bytes: (v.m as f64 / 10f64.powi(v.s as i32)).to_le_bytes().to_vec(),
                    db_type: DbType::F64,
                },
            }
        }
        TextFunc::Round | TextFunc::Trunc => {
            let toward_zero = func == TextFunc::Trunc;
            let Value::Data { bytes, db_type } = &args[0] else {
                return Ok(Value::Null);
            };
            let places = match args.get(1) {
                None => None,
                Some(Value::Null) => return Ok(Value::Null),
                Some(p) => Some(as_i64(p).ok_or_else(|| "the places must be an integer".to_string())?.clamp(-1000, 1000) as i32),
            };
            let exact = match db_type {
                DbType::DECIMAL => Some(decimal_value_as_pgnum(bytes)?),
                t if is_integer_type(t) && places.is_some() => as_i64(&args[0]).map(|v| crate::core::rql::pg_numeric::PgNum::int(v as i128)),
                _ => None,
            };
            match exact {
                Some(v) => {
                    let places = places.unwrap_or(0);
                    let r = if toward_zero { v.trunc_places(places) } else { v.round_places(places) }
                        .ok_or_else(|| PAST_38_DIGITS.to_string())?;
                    pgnum_as_decimal_value(r)
                }
                // `rint`: half to even, as PostgreSQL's float `round`; its
                // `trunc` toward zero.
                None => {
                    let f = as_f64(&args[0])
                        .or_else(|| as_i64(&args[0]).map(|v| v as f64))
                        .ok_or_else(|| format!("round is not defined for {}", db_type))?;
                    let f = if toward_zero { f.trunc() } else { f.round_ties_even() };
                    Value::Data { bytes: f.to_le_bytes().to_vec(), db_type: DbType::F64 }
                }
            }
        }
        TextFunc::ArrayNdims => {
            let Value::Data { bytes, .. } = &args[0] else {
                return Ok(Value::Null);
            };
            match crate::core::value_codec::decode_array_dims(bytes) {
                Some((_, dims, _)) if !dims.is_empty() => {
                    Value::Data { bytes: (dims.len() as i32).to_le_bytes().to_vec(), db_type: DbType::I32 }
                }
                _ => return Ok(Value::Null),
            }
        }
        // `pg_column_size(v)` is the stored width of one value. Given a whole
        // row — `pg_column_size(t)`, which is how a size query is written — the
        // argument names a relation rather than a column and resolves to
        // nothing, so the row's own width is the answer.
        // Answered by the executor, which can reach the storage layer; the
        // expression evaluator sees only a row. Reaching here means the call
        // survived into a context that cannot resolve it, and saying so beats
        // returning a plausible number.
        TextFunc::RelationSize => {
            return Err(
                "pg_total_relation_size is only available in a select list over pg_class"
                    .to_string(),
            );
        }
        TextFunc::ColumnSize => {
            let size = match args.first() {
                Some(Value::Data { bytes, .. }) if !bytes.is_empty() => bytes.len() as i64,
                _ => row
                    .columns
                    .iter()
                    .map(|c| c.data.into_slice().len() as i64)
                    .sum(),
            };
            Value::Data {
                bytes: size.to_le_bytes().to_vec(),
                db_type: DbType::I64,
            }
        }
    })
}

/// The rightmost top-level bitwise operator (`&`, `|`, `#`, `<<`, `>>`)
/// after something that can end an operand, and which it is. Not the
/// operators they are part of: `&&`, `?&`, `&<`, `&>`, `||`, `?|`, `|>>`,
/// `<<|`, `|/`, `|&>`, `#>`, `#>>`, `#-`, `##`, `<<=`, `>>=`, `->>`, `<->`,
/// `<<->>`.
pub(crate) fn top_level_bitwise(text: &str) -> Option<(usize, &'static str)> {
    let bytes = text.as_bytes();
    let nesting = Nesting::of(text);
    let mut found = None;
    let mut i = 0usize;
    while i < bytes.len() {
        if !nesting.top_level(i) {
            i += 1;
            continue;
        }
        if let Some(op) = bitwise_operator_at(bytes, i) {
            found = Some((i, op));
            i += op.len();
            continue;
        }
        i += 1;
    }
    found
}

/// `left @@ right` as the `ts_match_*` PostgreSQL resolves it to, from what
/// each side is: a `to_tsvector` call a vector, a `to_tsquery`-family call a
/// query, anything else text (no column here holds a vector). `tsquery @@
/// tsvector` is read the other way round; a vector against text reads the
/// text as a query.
pub(crate) fn text_match(left: Expr, right: Expr) -> Expr {
    let vector = |e: &Expr| matches!(e, Expr::Call(TextFunc::ToTsVector, _));
    let query = |e: &Expr| {
        matches!(
            e,
            Expr::Call(
                TextFunc::ToTsQuery | TextFunc::PlainToTsQuery | TextFunc::PhraseToTsQuery | TextFunc::WebsearchToTsQuery,
                _
            )
        )
    };
    let (left, right) = if query(&left) && !query(&right) { (right, left) } else { (left, right) };
    let func = match (vector(&left), query(&right)) {
        (true, _) => TextFunc::TsMatchVq,
        (false, true) => TextFunc::TsMatchTq,
        (false, false) => TextFunc::TsMatchTt,
    };
    Expr::Call(func, vec![left, right])
}

/// The bitwise operator (`&`, `|`, `#`, `<<`, `>>`) starting at `i`, after
/// something that can end an operand; `None` there for any other byte and
/// for the operators these are part of ([`top_level_bitwise`]). Says nothing
/// of quotes or depth.
pub(crate) fn bitwise_operator_at(bytes: &[u8], i: usize) -> Option<&'static str> {
    let at = |i: usize| bytes.get(i).copied();
    let before = |i: usize| i.checked_sub(1).and_then(at);
    let op: &'static str = match bytes[i] {
        b'&' if !matches!(before(i), Some(b'&' | b'?' | b'|')) && !matches!(at(i + 1), Some(b'&' | b'<' | b'>')) => "&",
        b'|' if !matches!(before(i), Some(b'|' | b'?' | b'<')) && !matches!(at(i + 1), Some(b'|' | b'>' | b'/' | b'&')) => "|",
        b'#' if !matches!(before(i), Some(b'#')) && !matches!(at(i + 1), Some(b'>' | b'-' | b'#')) => "#",
        // `@@`, the text match, one precedence with these (not `@@@`).
        b'@' if at(i + 1) == Some(b'@') && before(i) != Some(b'@') && at(i + 2) != Some(b'@') => "@@",
        b'<' if at(i + 1) == Some(b'<')
            && !matches!(before(i), Some(b'<'))
            && !matches!(at(i + 2), Some(b'=' | b'|' | b'<' | b'-')) =>
        {
            "<<"
        }
        b'>' if at(i + 1) == Some(b'>')
            && !matches!(before(i), Some(b'>' | b'-' | b'#' | b'|'))
            && !matches!(at(i + 2), Some(b'=' | b'>')) =>
        {
            ">>"
        }
        _ => return None,
    };
    // The nearest byte before it that is not a space ends an operand.
    let prev = bytes[..i].iter().rposition(|b| !b.is_ascii_whitespace()).map(|j| bytes[j]);
    matches!(prev, Some(p) if p.is_ascii_alphanumeric() || matches!(p, b'_' | b')' | b']' | b'"' | b'\'' | b'.')).then_some(op)
}

/// Offset of a top-level `||`, ignoring quotes, parentheses and the arms of
/// a `CASE` block.
fn top_level_concat(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let nesting = Nesting::of(text);
    let mut found = None;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'|' && bytes.get(i + 1) == Some(&b'|') && nesting.top_level(i) {
            // The rightmost wins, so `a || b || c` groups left to right.
            found = Some(i);
            i += 2;
            continue;
        }
        i += 1;
    }
    found
}

/// True when the value is a scaled decimal.
fn is_decimal(value: &Value) -> bool {
    matches!(
        value,
        Value::Data {
            db_type: DbType::DECIMAL,
            ..
        }
    )
}

/// Arithmetic over `NUMERIC`, done at the engine scale, `DECIMAL_SCALE`.
///
/// A column declared `NUMERIC(p, s)` stores its integer scaled by its own `s`;
/// [`eval`] lifts every such read to the engine scale first (see
/// [`to_engine_scale`]), so both operands here share one scale. Mixing them
/// was a silent wrong answer: `v + 1` over a `NUMERIC(10,2)` column holding 100
/// returned 10000100.00, the column's 10000 plus a literal lifted to 10^9.
///
/// Addition and subtraction work directly on the scaled integers. Multiplication
/// produces a doubly-scaled product and has to be divided back down; division
/// has to scale the numerator up first, or the fractional part is truncated
/// away — the same rule `AVG` follows over an exact column. Both intermediates
/// can pass `i128` while the answer fits, so they fall back to a 256-bit
/// product instead of reporting an overflow the result does not have.
fn decimal_arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
    let factor = 10i128.pow(crate::core::value_codec::DECIMAL_SCALE);

    let scaled = |value: &Value| -> Option<i128> {
        match value {
            Value::Data {
                bytes,
                db_type: DbType::DECIMAL,
            } => Some(i128::from_le_bytes(bytes.as_slice().try_into().ok()?)),
            // A fractional literal arrives as a float — `price * 1.5`. A
            // literal meeting a NUMERIC column was already converted when the
            // expression was parsed (`fold_float_literal_into_decimal`); this
            // is the same conversion for a float that is computed.
            Value::Data {
                db_type: DbType::F32 | DbType::F64,
                ..
            } => float_to_engine_decimal(as_f64(value)?),
            // A whole number written into a decimal expression is exact, so it
            // is lifted into scaled units rather than turned into a float.
            other => as_i64(other).map(|v| v as i128 * factor),
        }
    };

    let (Some(x), Some(y)) = (scaled(a), scaled(b)) else {
        // A constant with more places than these nine (`numeric_constant`),
        // which `scaled` does not read: cut to them here, it would be
        // answered wrong. It is computed exactly where a result is output or
        // stored; anywhere else it is refused. Asked only on this failure,
        // so the common path pays nothing for it.
        let wide = |v: &Value| matches!(v, Value::Data { bytes, db_type: DbType::DECIMAL } if bytes.len() == 17);
        if wide(a) || wide(b) {
            return own_scale_arith(a, op, b);
        }
        return Err(format!("'{}' needs numeric operands", op.symbol()));
    };

    let value = match op {
        ArithOp::Add => x.checked_add(y),
        ArithOp::Sub => x.checked_sub(y),
        ArithOp::Mul => decimal_mul_engine(x, y),
        ArithOp::Div => {
            if y == 0 {
                return Err("division by zero".to_string());
            }
            decimal_div_engine(x, y)
        }
        ArithOp::Rem => {
            if y == 0 {
                return Err("division by zero".to_string());
            }
            x.checked_rem(y)
        }
    }
    .ok_or_else(|| format!("numeric overflow in '{}'", op.symbol()))?;

    Ok(Value::Data {
        bytes: value.to_le_bytes().to_vec(),
        db_type: DbType::DECIMAL,
    })
}

/// NUMERIC arithmetic with an operand carrying its own scale — a value of a
/// column that keeps each value's (`value_codec::keeps_own_scale`), or a
/// constant with more places than the engine's nine — computed exactly, as
/// PostgreSQL's `numeric` operators compute it (scales by `pg_numeric`'s
/// rules), the result a cell carrying its own scale. A sixteen-byte NUMERIC
/// beside it is at the engine's nine places; an integer is itself; a float is
/// its fifteen-digit text, as `float8_numeric` reads it.
fn own_scale_arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
    use crate::core::rql::pg_numeric::{self as pgn, PgNum};
    let read = |v: &Value| -> Option<PgNum> {
        match v {
            Value::Data { bytes, db_type: DbType::DECIMAL } => {
                let (m, own) = crate::core::value_codec::split_decimal_cell(bytes)?;
                match own {
                    Some(s) => PgNum::new(m, s),
                    None => Some(PgNum::trimmed(m, crate::core::value_codec::DECIMAL_SCALE)),
                }
            }
            Value::Data { db_type: DbType::F32 | DbType::F64, .. } => pgn::from_f64(as_f64(v)?),
            other => as_i64(other).map(|n| PgNum::int(n as i128)),
        }
    };
    let (Some(x), Some(y)) = (read(a), read(b)) else {
        return Err(format!("'{}' needs numeric operands", op.symbol()));
    };
    let overflow = || format!("numeric overflow in '{}'", op.symbol());
    let value = match op {
        ArithOp::Add => x.add(y).ok_or_else(overflow)?,
        ArithOp::Sub => x.sub(y).ok_or_else(overflow)?,
        ArithOp::Mul => x.mul(y).ok_or_else(overflow)?,
        ArithOp::Div => x.div(y).map_err(|e| e.to_string())?.ok_or_else(overflow)?,
        ArithOp::Rem => x.rem(y).map_err(|e| e.to_string())?.ok_or_else(overflow)?,
    };
    Ok(Value::Data { bytes: value.to_cell(), db_type: DbType::DECIMAL })
}

/// Whether a float's shortest digits need no more places than the engine
/// keeps, so taking it into `NUMERIC` loses nothing.
fn float_is_engine_exact(f: f64) -> bool {
    crate::core::value_codec::parse_decimal(&format!("{}", f)).is_some()
}

/// A float taken into `NUMERIC` at the engine scale, at the digits it was
/// written with: the shortest text that round-trips the float is those digits,
/// and parsing it is exact. A float with more places than the engine keeps is
/// rounded. `None` for a non-finite float.
fn float_to_engine_decimal(f: f64) -> Option<i128> {
    if !f.is_finite() {
        return None;
    }
    let factor = 10i128.pow(crate::core::value_codec::DECIMAL_SCALE);
    crate::core::value_codec::parse_decimal(&format!("{}", f)).or_else(|| {
        let lifted = (f * factor as f64).round();
        (lifted.abs() < i128::MAX as f64).then_some(lifted as i128)
    })
}

/// A written constant as a `NUMERIC` at the engine scale, or `None` when that
/// scale cannot hold it exactly: more places than nine, or too large. Zeros
/// at the end of the fraction are not places it needs, so `1.5000000000` is
/// 1.5 and not a float.
fn exact_engine_decimal(text: &str) -> Option<i128> {
    let at = text.find(['e', 'E']).unwrap_or(text.len());
    let (mantissa, exponent) = text.split_at(at);
    let mantissa = if mantissa.contains('.') { mantissa.trim_end_matches('0') } else { mantissa };
    // `parse_decimal_scaled` at the engine scale refuses the places past it
    // rather than dropping them.
    crate::core::value_codec::parse_decimal_scaled(
        &format!("{}{}{}", mantissa, if mantissa.ends_with('.') { "0" } else { "" }, exponent),
        crate::core::value_codec::DECIMAL_SCALE,
    )
}

/// The refusal of a `numeric` constant this engine can only carry as a
/// float, where the float is not that number: SQLSTATE 22003, as for a
/// `NUMERIC` column whose value the engine scale cannot hold.
fn inexact_literal(text: &str) -> String {
    format!("numeric literal {} is out of range for exact arithmetic", text)
}

/// Whether the float `v`, read from the numeric constant `text`, is that
/// number: the shortest text that round-trips the float has the same digits
/// and exponent as `text`. A literal with more significant digits than a
/// float keeps, or past its range, is not.
fn float_holds_literal(text: &str, v: f64) -> bool {
    /// (negative, significant digits, exponent of the first digit), with
    /// zeros trimmed from both ends of the digits; `None` for anything that
    /// is not a plain decimal or exponent number.
    fn digits(text: &str) -> Option<(bool, String, i64)> {
        let (negative, body) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match body.find(['e', 'E']) {
            Some(at) => (&body[..at], body[at + 1..].parse::<i64>().ok()?),
            None => (body, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        let all = format!("{whole}{fraction}");
        let leading = all.len() - all.trim_start_matches('0').len();
        let significant = all.trim_matches('0').to_string();
        if significant.is_empty() {
            return Some((false, String::new(), 0));
        }
        Some((negative, significant, exponent + whole.len() as i64 - leading as i64))
    }
    v.is_finite() && digits(text).is_some_and(|written| Some(written) == digits(&format!("{:e}", v)))
}

/// `a op b` over two written numbers, at least one of them a constant the
/// engine carries as a float, computed exactly as PostgreSQL's `numeric`
/// does: `+`, `-` and `*` of decimals are exact. The answer is a `NUMERIC`
/// when the engine scale holds it, a float when the float is exactly it
/// (`0.00000000001 * 2`), and refused otherwise (`1e30 + 1`, which the float
/// answered as 1e30). `None` for anything else, which the evaluator computes.
fn exact_literal_arith(l: &Expr, op: ArithOp, r: &Expr, text: &str) -> Option<Result<Expr, String>> {
    let float = |e: &Expr| matches!(e, Expr::Literal { db_type: DbType::F64, .. }) || is_wide_literal(e);
    if !(float(l) || float(r)) || !matches!(op, ArithOp::Add | ArithOp::Sub | ArithOp::Mul) {
        return None;
    }
    let value = |e: &Expr| match e {
        Expr::Literal { bytes, db_type } => exact_decimal(&Value::Data { bytes: bytes.clone(), db_type: db_type.clone() }),
        _ => None,
    };
    let (a, b) = (value(l)?, value(r)?);
    Some(match exact_decimal_arith(a, op, b) {
        Some((m, e)) => numeric_constant(m, e, text),
        None => Err(inexact_literal(text)),
    })
}

/// Below this a float keeps at least as many digits of a number as nine
/// decimal places do (17 significant digits against 8 + 9), so a constant
/// the engine scale cannot hold exactly is better carried as one.
const FLOAT_KEEPS_BELOW: f64 = 1e8;

/// A written number, `m × 10^e`, as the constant the engine carries: a
/// `NUMERIC` when nine places hold it exactly; else a float when the float
/// is that number, or when the number is small enough that a float keeps
/// more of it than nine places would (PostgreSQL rounds a `numeric` at
/// assignment; this rounds once, here); else a `NUMERIC` rounded to nine
/// places. Refused (22003) only when the engine cannot hold its integer
/// part: `123456789012345678901234567890123456.50` was the float
/// 1.2345678901234568e35, stored as 123456789012345680000000000000000000.
fn numeric_constant(m: i128, e: i32, text: &str) -> Result<Expr, String> {
    let scale = crate::core::value_codec::DECIMAL_SCALE as i32;
    if e >= -scale {
        if let Some(units) = 10i128.checked_pow((e + scale) as u32).and_then(|p| m.checked_mul(p)) {
            return Ok(Expr::Literal { bytes: units.to_le_bytes().to_vec(), db_type: DbType::DECIMAL });
        }
    }
    // More places than the engine keeps: the digits as written, at their own
    // scale, in the cell an `AVG` result is carried in. PostgreSQL types the
    // constant `numeric`; carried as a float it made `c + 43e-17` the float
    // 44812.65 (described float8), and `CASE … ELSE n2 - 1.0000000001 END`
    // stored 67268942325258700 for 67268942325258704.12. Read exactly where a
    // result is output or stored (`eval_output`, `eval_into`) and refused by
    // the engine's nine-place arithmetic (`decimal_arith`), never cut.
    if e < -scale && e >= -(WIDE_LITERAL_PLACES as i32) {
        return Ok(Expr::Literal {
            bytes: crate::core::value_codec::decimal_cell_with_scale(m, e.unsigned_abs()),
            db_type: DbType::DECIMAL,
        });
    }
    // Too large for the engine scale (past about 1.7e29) yet whole digits an
    // i128 holds at its own scale: the same own-scale cell, so `1e30` and
    // `123456789012345678901234567890` are `numeric` as PostgreSQL types
    // them. As a float, `s0 * 1e30` was 7e+30 and `1e30 + 1` lost the 1.
    if e >= -scale {
        let own = if e >= 0 { 10i128.checked_pow(e as u32).and_then(|p| m.checked_mul(p)) } else { Some(m) };
        if let Some(own) = own {
            return Ok(Expr::Literal {
                bytes: crate::core::value_codec::decimal_cell_with_scale(own, (-e).max(0) as u32),
                db_type: DbType::DECIMAL,
            });
        }
    }
    let written = format!("{m}e{e}");
    if let Ok(v) = written.parse::<f64>() {
        if v.is_finite() && (float_holds_literal(&written, v) || v.abs() < FLOAT_KEEPS_BELOW) {
            return Ok(Expr::Literal { bytes: v.to_le_bytes().to_vec(), db_type: DbType::F64 });
        }
    }
    match round_to_engine(m, e) {
        Some(units) => Ok(Expr::Literal { bytes: units.to_le_bytes().to_vec(), db_type: DbType::DECIMAL }),
        None => Err(inexact_literal(text)),
    }
}

/// A written `NUMERIC` constant whose places include zeros at the end of its
/// fraction — `2.50`, `2.0`, `100e-2` — as a cast to its own written scale:
/// `numeric_in` gives it that display scale, and the constant's value at
/// the engine scale cannot say it (the bytes of `2.50` and `2.5` are the
/// same). So `2.50 * 2` is `5.00`, as PostgreSQL shows it, not `5.0`.
///
/// The cast's precision is 0, which checks nothing: it says only the scale.
/// A constant whose written places are those its value needs stays the
/// plain literal, whose scale ([`display_scale`], [`pg_numeric`]) is its own.
fn with_written_scale(literal: Expr, text: &str) -> Expr {
    // Only a fraction ending in a zero, or an exponent, can be written with
    // more places than its value needs; every other constant — nearly every
    // one an INSERT carries — is decided on its last byte, unparsed.
    let b = text.as_bytes();
    if !b.iter().any(|c| matches!(c, b'e' | b'E')) && !(b.last() == Some(&b'0') && b.contains(&b'.')) {
        return literal;
    }
    let Some((_, e)) = decimal_digits(text) else { return literal };
    let written = u32::try_from(-(e.min(0) as i64)).unwrap_or(0);
    match display_scale(&literal) {
        Some(own) if written > own && written <= crate::core::rql::pg_numeric::MAX_SCALE => Expr::NumericCast {
            value: Box::new(literal),
            precision: 0,
            scale: written,
        },
        _ => literal,
    }
}

/// The most places a written constant is carried exactly at (an i128 holds
/// 38 digits); one with more is the float or rounded constant it was.
const WIDE_LITERAL_PLACES: u32 = 38;

/// Whether `expr` is a written constant with more places than the engine
/// keeps, carried at its own scale ([`numeric_constant`]).
fn is_wide_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal { bytes, db_type: DbType::DECIMAL } if bytes.len() == 17)
}

/// `m × 10^e` at the engine scale, rounded half away from zero; `None` when
/// its integer part does not fit.
fn round_to_engine(m: i128, e: i32) -> Option<i128> {
    let scale = crate::core::value_codec::DECIMAL_SCALE as i32;
    if e >= -scale {
        return 10i128.checked_pow((e + scale) as u32).and_then(|p| m.checked_mul(p));
    }
    let drop = (-scale - e) as u32;
    let Some(divisor) = 10i128.checked_pow(drop) else {
        // More places to drop than an i128 has digits: it rounds to zero.
        return Some(0);
    };
    let (q, r) = (m / divisor, m % divisor);
    if r.unsigned_abs() >= divisor.unsigned_abs().div_ceil(2) { q.checked_add(m.signum()) } else { Some(q) }
}

/// A written number's digits as `m × 10^e`; `None` for anything that is not
/// a plain decimal or exponent number, or with more digits than an i128.
pub(crate) fn decimal_digits(text: &str) -> Option<(i128, i32)> {
    let (negative, body) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], body[at + 1..].parse::<i32>().ok()?),
        None => (body, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Accumulated in place: more digits than an i128 holds overflow to
    // `None`, as parsing the joined digits did, without the copy.
    let mut m: i128 = 0;
    for b in whole.bytes().chain(fraction.bytes()) {
        m = m.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
    }
    let e = exponent.checked_sub(i32::try_from(fraction.len()).ok()?)?;
    Some((if negative { -m } else { m }, e))
}

/// `m × 10^e`: a number value's exact decimal, for a float through its
/// shortest text. `None` for anything that is not a number.
fn exact_decimal(value: &Value) -> Option<(i128, i32)> {
    let Value::Data { bytes, db_type } = value else {
        return None;
    };
    // Without the zeros a NUMERIC's nine places put on it: 1e29 is 10^38
    // units, which aligning with anything past it would overflow.
    let normal = |(mut m, mut e): (i128, i32)| {
        while m != 0 && m % 10 == 0 {
            m /= 10;
            e += 1;
        }
        Some((m, if m == 0 { 0 } else { e }))
    };
    // A cell carrying its own scale is at that scale, its written places
    // kept: they are the display scale PostgreSQL gives the result.
    if *db_type == DbType::DECIMAL && bytes.len() == 17 {
        let (m, own) = crate::core::value_codec::split_decimal_cell(bytes)?;
        return Some((m, -(own? as i32)));
    }
    normal(match db_type {
        DbType::DECIMAL => Some((
            i128::from_le_bytes(bytes.as_slice().try_into().ok()?),
            -(crate::core::value_codec::DECIMAL_SCALE as i32),
        )),
        DbType::F64 | DbType::F32 => {
            let v = as_f64(value).filter(|v| v.is_finite())?;
            let shown = format!("{:e}", v);
            let (mantissa, exponent) = shown.split_once('e')?;
            let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
            let m: i128 = format!("{whole}{fraction}").parse().ok()?;
            Some((m, exponent.parse::<i32>().ok()? - fraction.len() as i32))
        }
        _ => as_i64(value).map(|v| (v as i128, 0)),
    }?)
}

/// `a op b` exactly, for `+`, `-` and `*`; `None` past `i128`.
fn exact_decimal_arith((ma, ea): (i128, i32), op: ArithOp, (mb, eb): (i128, i32)) -> Option<(i128, i32)> {
    let pow = |n: i32| 10i128.checked_pow(u32::try_from(n).ok()?);
    let (mut m, mut e) = match op {
        ArithOp::Mul => (ma.checked_mul(mb)?, ea.checked_add(eb)?),
        ArithOp::Add | ArithOp::Sub => {
            let e = ea.min(eb);
            let a = ma.checked_mul(pow(ea - e)?)?;
            let b = mb.checked_mul(pow(eb - e)?)?;
            (if op == ArithOp::Add { a.checked_add(b)? } else { a.checked_sub(b)? }, e)
        }
        ArithOp::Div | ArithOp::Rem => return None,
    };
    while m != 0 && m % 10 == 0 {
        m /= 10;
        e += 1;
    }
    Some((m, e))
}

/// Arithmetic between a written constant the engine carries as a float
/// (`lhs` or `rhs`) and an integer or a `NUMERIC`, which PostgreSQL computes
/// in `numeric`: answered from the exact result, as the float nearest it
/// (the expression is described as one), rather than from the float
/// product. `i * 1e30` was 1e30 times the float, and `i + 1e30` 1e30 for
/// every `i`. Refused (22003) only where the engine could not hold the
/// exact result's integer part and the float is not that number. Against a
/// float, and for `/` and `%`, the float's answer stands.
fn numeric_literal_arith(lhs: &Expr, a: &Value, op: ArithOp, rhs: &Expr, b: &Value, got: Value) -> Result<Value, String> {
    let float_literal = |e: &Expr| matches!(e, Expr::Literal { db_type: DbType::F64, .. });
    let exact_typed = |v: &Value| {
        matches!(
            v.db_type(),
            Some(
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32
                    | DbType::U64 | DbType::DECIMAL
            )
        )
    };
    let numeric = (float_literal(lhs) && exact_typed(b)) || (float_literal(rhs) && exact_typed(a));
    if !numeric || matches!(op, ArithOp::Div | ArithOp::Rem) || !matches!(got, Value::Data { db_type: DbType::F64, .. }) {
        return Ok(got);
    }
    let refused = || {
        format!(
            "numeric value of {} {} {} is out of range for exact arithmetic",
            as_text(a),
            op.symbol(),
            as_text(b)
        )
    };
    let Some((m, e)) = exact_decimal(a).zip(exact_decimal(b)).and_then(|(x, y)| exact_decimal_arith(x, op, y)) else {
        // Past an i128: the float product, unless its integer part is past
        // what the engine holds too.
        return match as_f64(&got) {
            Some(f) if f.is_finite() && f.abs() < 1e29 => Ok(got),
            _ => Err(refused()),
        };
    };
    let written = format!("{m}e{e}");
    match written.parse::<f64>() {
        Ok(v) if v.is_finite() && (round_to_engine(m, e).is_some() || float_holds_literal(&written, v)) => {
            Ok(Value::Data { bytes: v.to_le_bytes().to_vec(), db_type: DbType::F64 })
        }
        _ => Err(refused()),
    }
}

/// Whether `expr` evaluates to a `NUMERIC` whenever it is not NULL — so that
/// arithmetic against it is certain to take the decimal path.
///
/// Deliberately narrow: a column or literal of that type, or arithmetic with
/// one such operand (which `arith` sends down the decimal path, or refuses).
/// A `CASE` or `COALESCE` whose static type is NUMERIC may still produce an
/// integer at run time, and a float literal against an integer is float
/// arithmetic — converting it there would change the answer's type.
fn always_decimal(expr: &Expr) -> bool {
    match expr {
        Expr::Column(field) => field.db_type == DbType::DECIMAL,
        Expr::Literal { db_type, .. } => *db_type == DbType::DECIMAL,
        Expr::NumericCast { .. } => true,
        Expr::Arith(a, _, b) => always_decimal(a) || always_decimal(b),
        _ => false,
    }
}

/// `v * 1.5` over a NUMERIC `v`: the float literal is converted to NUMERIC
/// once, here, instead of on every row. `decimal_arith` used to format the
/// float as text and parse it back per row, and the result is the same bytes
/// either way — the conversion is [`float_to_engine_decimal`] in both places.
/// A float column, or a float against an integer, is left alone:
/// `float_col * 1.5` stays float.
fn fold_float_literal_into_decimal(expr: Expr) -> Expr {
    let Expr::Arith(mut lhs, op, mut rhs) = expr else {
        return expr;
    };
    let convert = |side: &mut Box<Expr>, other: &Expr| {
        if let Expr::Literal {
            bytes,
            db_type: db_type @ (DbType::F32 | DbType::F64),
        } = side.as_ref()
        {
            if !always_decimal(other) {
                return;
            }
            let value = Value::Data {
                bytes: bytes.clone(),
                db_type: db_type.clone(),
            };
            // Only a float the engine scale holds. One with more places is
            // left a float: rounded to nine here, `d * 0.00000000011` was 0
            // and `a + 0.000000000001` was `a`. A *written* constant with
            // more places is no float (`numeric_constant` carries its digits);
            // this is a float the statement asked for or computed.
            if let Some(units) = as_f64(&value).filter(|f| float_is_engine_exact(*f)).and_then(float_to_engine_decimal) {
                **side = Expr::Literal {
                    bytes: units.to_le_bytes().to_vec(),
                    db_type: DbType::DECIMAL,
                };
            }
        }
    };
    convert(&mut rhs, &lhs);
    convert(&mut lhs, &rhs);
    Expr::Arith(lhs, op, rhs)
}

/// The product of two values at the engine scale, at the engine scale:
/// truncated toward zero, `None` only when the answer does not fit.
///
/// Shared with the scan's predicate engine (`row::math`), so `WHERE v * 2 = 200`
/// and `SELECT v * 2` cannot disagree about a single digit.
pub(crate) fn decimal_mul_engine(x: i128, y: i128) -> Option<i128> {
    let factor = 10i128.pow(crate::core::value_codec::DECIMAL_SCALE);
    match x.checked_mul(y) {
        Some(product) => Some(product / factor),
        None => mul_div_i128(x, y, factor),
    }
}

/// The quotient of two values at the engine scale, at the engine scale. The
/// caller has refused a zero divisor.
pub(crate) fn decimal_div_engine(x: i128, y: i128) -> Option<i128> {
    let factor = 10i128.pow(crate::core::value_codec::DECIMAL_SCALE);
    match x.checked_mul(factor) {
        Some(lifted) => Some(lifted / y),
        None => mul_div_i128(x, factor, y),
    }
}

/// `a * b / d`, truncated toward zero as `i128` division is, through a 256-bit
/// product. `None` only when the quotient itself does not fit an `i128`.
fn mul_div_i128(a: i128, b: i128, d: i128) -> Option<i128> {
    if d == 0 {
        return None;
    }
    let negative = (a < 0) ^ (b < 0) ^ (d < 0);
    let (hi, lo) = widening_mul_u128(a.unsigned_abs(), b.unsigned_abs());
    let q = div_u256_by_u128(hi, lo, d.unsigned_abs())?;
    if negative {
        // `i128::MIN` has no positive twin, so it is the one extra value the
        // negative side can hold.
        if q == i128::MIN.unsigned_abs() {
            Some(i128::MIN)
        } else {
            i128::try_from(q).ok().map(|v| -v)
        }
    } else {
        i128::try_from(q).ok()
    }
}

/// The full 256-bit product of two `u128`s, as (high, low) halves.
fn widening_mul_u128(a: u128, b: u128) -> (u128, u128) {
    const MASK: u128 = u64::MAX as u128;
    let (a_hi, a_lo) = (a >> 64, a & MASK);
    let (b_hi, b_lo) = (b >> 64, b & MASK);
    let ll = a_lo * b_lo;
    let lh = a_lo * b_hi;
    let hl = a_hi * b_lo;
    let hh = a_hi * b_hi;
    // The middle column: three terms below 2^128 each, summed with carries.
    let mid = (ll >> 64) + (lh & MASK) + (hl & MASK);
    let lo = (ll & MASK) | (mid << 64);
    let hi = hh + (lh >> 64) + (hl >> 64) + (mid >> 64);
    (hi, lo)
}

/// `(hi:lo) / d` for a divisor no wider than 127 bits — every magnitude of an
/// `i128` is — so the running remainder doubled always fits a `u128`. `None`
/// when the quotient needs more than 128 bits.
fn div_u256_by_u128(hi: u128, lo: u128, d: u128) -> Option<u128> {
    debug_assert!(d != 0 && d <= 1u128 << 127);
    if hi == 0 {
        return Some(lo / d);
    }
    // The quotient fits 128 bits exactly when the high half is below `d`.
    if hi >= d {
        return None;
    }
    let mut rem = hi;
    let mut q: u128 = 0;
    for bit in (0..128).rev() {
        rem = (rem << 1) | ((lo >> bit) & 1);
        q <<= 1;
        if rem >= d {
            rem -= d;
            q |= 1;
        }
    }
    Some(q)
}

/// An expression's value, computed to be stored in `target`.
///
/// Expressions compute at the engine's nine places ([`eval`]), which a
/// `NUMERIC(p, s)` column with more places cannot take back without loss:
/// `SET a = a + 0.000000000001` on a NUMERIC(20,12) holding 0.123456789123
/// stored 0.123456789000, its own last digits cut and the literal rounded
/// to 0, and `SET d = d * 0.00000000011` stored 0. So a value bound for a
/// NUMERIC column is first computed exactly ([`exact_numeric`]): `+ - * /`
/// and `COALESCE` over NUMERIC and integer columns and written numbers, at
/// whatever places they need, rounded once to the column's scale (half away
/// from zero, as PostgreSQL's assignment rounds). Anything else, or a value
/// the exact reading cannot settle, takes the engine path as before — which
/// refuses what it cannot carry rather than truncating it
/// ([`to_engine_scale`], [`decimal_arith`]).
/// An aggregate's argument for one row: a NUMERIC one as PostgreSQL computes
/// it (`pg_numeric`), a cell carrying its display scale, so `sum(n * 2)` over
/// a NUMERIC(10,4) shows 24.0000 and `sum(n * 1e30)` keeps its places;
/// anything else as `eval` gives it. At the engine's nine places the first
/// was 24.000000000, and a value past them could not be summed.
pub(crate) fn eval_aggregate_argument(expr: &Expr, row: &Row, schema: &[SchemaField]) -> Result<Value, String> {
    if static_type(expr) == DbType::DECIMAL {
        match pg_numeric(expr, row, schema)? {
            Some(None) => return Ok(Value::Null),
            Some(Some(v)) => return Ok(Value::Data { bytes: v.to_cell(), db_type: DbType::DECIMAL }),
            None => {}
        }
    }
    eval(expr, row, schema)
}

pub(crate) fn eval_into(expr: &Expr, row: &Row, schema: &[SchemaField], target: &SchemaField) -> Result<Value, String> {
    // A `timestamp` (without time zone) value written into a text column: its
    // text with no offset (`from_engine_scale` renders a `timestamptz`'s).
    if matches!(target.db_type, DbType::STRING | DbType::CHAR) && yields_timestamp(expr) {
        return local_timestamp_text(eval(expr, row, schema)?);
    }
    // A NUMERIC written into a text column is its text at the display scale
    // PostgreSQL gives it: `SET v = nu` over a NUMERIC(10,3) 2.500 stores
    // '2.500' and `SET v = 2.50 * 2` '5.00', where the engine-scale value's
    // own text was '2.5' and '5'.
    if matches!(target.db_type, DbType::STRING | DbType::CHAR) && static_type(expr) == DbType::DECIMAL {
        if let Some(value) = pg_numeric(expr, row, schema)? {
            let value = match value {
                None => Value::Null,
                Some(v) => Value::Data { bytes: v.to_text().into_bytes(), db_type: DbType::STRING },
            };
            return from_engine_scale(value, target);
        }
    }
    // `'{"a":1}' || '{"b":1}'` is text concatenation, which PostgreSQL
    // has no assignment cast from to `jsonb`: 42804, not the 22P02 of
    // reading the joined text as a document. (A document on either side
    // is `jsonb ||`, and a single literal is read as a document.)
    if target.db_type == DbType::JSONB && matches!(expr, Expr::Concat(..)) && static_type(expr) == DbType::STRING {
        return Err(format!("column \"{}\" is of type jsonb but expression is of type text", target.name));
    }
    if target.db_type == DbType::DECIMAL {
        // PostgreSQL's value, as its operators compute it — a quotient rounded
        // at `select_div_scale`'s places first — then rounded to the column's
        // scale, as its assignment cast rounds.
        match pg_numeric(expr, row, schema)? {
            Some(None) => return Ok(Value::Null),
            Some(Some(v)) => return pg_numeric_into(v, target),
            None => {}
        }
        let scale = target.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
        let exact = match exact_numeric(expr, row, schema, scale) {
            // The exact form is past an i128. Through the engine this is cut
            // to nine places, which a column keeping more must not take:
            // `SET a = a / 7` on a NUMERIC(38,20) of 1e13 stored
            // …714285714000000000000.
            Err(e) if e == EXACT_RANGE => {
                if scale > crate::core::value_codec::DECIMAL_SCALE {
                    return Err(format!(
                        "the value for column \"{}\" cannot be computed exactly at {} decimal places: its exact \
                         form has more digits than this engine carries (not supported)",
                        target.name, scale
                    ));
                }
                None
            }
            other => other?,
        };
        if let Some(value) = exact {
            return match value {
                None => Ok(Value::Null),
                // An unconstrained column keeps nine places: a value it cannot
                // hold exactly is refused, never rounded (`pg_numeric_into`).
                // A column that keeps each value's own scale takes the exact
                // value whole, to 38 digits; one carried as a float goes the
                // float's way (`from_engine_scale`, `float8_numeric`).
                Some(exact) if crate::core::value_codec::keeps_own_scale(target) && exact.err == 0.0 => {
                    let v = if exact.e >= 0 {
                        10i128
                            .checked_pow(exact.e as u32)
                            .and_then(|p| exact.m.checked_mul(p))
                            .map(crate::core::rql::pg_numeric::PgNum::int)
                    } else {
                        crate::core::rql::pg_numeric::PgNum::new(exact.m, exact.e.unsigned_abs())
                    };
                    match v {
                        Some(v) => pg_numeric_into(v, target),
                        None => Err(crate::core::value_codec::own_scale_refusal(
                            &format!("\"{}\"", exact.text()),
                            &target.name,
                        )),
                    }
                }
                Some(_) if crate::core::value_codec::keeps_own_scale(target) => eval(expr, row, schema)
                    .map_err(|e| column_refusal(e, target))
                    .and_then(|value| from_engine_scale(value, target)),
                Some(exact) if target.scale.is_none() && !exact.exact_at(scale) => {
                    Err(crate::core::value_codec::old_numeric_refusal(
                        &if exact.err == 0.0 { format!("\"{}\"", exact.text()) } else { "of this expression".to_string() },
                        &target.name,
                    ))
                }
                Some(exact) => match exact.settle(scale) {
                    Some(units) => Ok(Value::Data { bytes: units.to_le_bytes().to_vec(), db_type: DbType::DECIMAL }),
                    // Not settled: the engine path's own refusal when it has
                    // one (22003 for `i + 1e30`), and never its answer, which
                    // is the one this could not vouch for.
                    None => match eval(expr, row, schema).and_then(|value| from_engine_scale(value, target)) {
                        Err(e) => Err(e),
                        Ok(_) => Err(inexact_assignment(&target.name, scale)),
                    },
                },
            };
        }
    }
    eval(expr, row, schema)
        .map_err(|e| column_refusal(e, target))
        .and_then(|value| from_engine_scale(value, target))
}

/// An expression's refusal of a number past the nine places the engine
/// carries an evaluated NUMERIC at (`t::numeric` of a 39-place text), bound
/// for an unconstrained NUMERIC column: the column's own refusal
/// (`value_codec::refusal_for_column`), which names its limit, 38 places in
/// the seventeen-byte slot, or nine with the conversion hint in the old one.
/// Any other error is left as it is.
fn column_refusal(e: String, target: &SchemaField) -> String {
    if target.db_type != DbType::DECIMAL || target.scale.is_some() {
        return e;
    }
    let Some(rest) = e.strip_prefix("numeric value \"") else { return e };
    let Some((value, tail)) = rest.split_once('"') else { return e };
    if !tail.contains("decimal places a NUMERIC keeps here") {
        return e;
    }
    crate::core::value_codec::refusal_for_column(target, value)
}

/// A [`pg_numeric`] value stored in the `NUMERIC` column `target`: rounded
/// half away from zero to its declared scale. An unconstrained column keeps
/// the engine's nine places, and PostgreSQL would keep every place of the
/// value; one with digits past nine is refused, as such a written constant
/// is, rather than stored without them (`SET c = 2.0 / 3` stored
/// 0.666666667 for PostgreSQL's 0.66666666666666666667).
pub(crate) fn pg_numeric_into(v: crate::core::rql::pg_numeric::PgNum, target: &SchemaField) -> Result<Value, String> {
    use crate::core::value_codec::DECIMAL_SCALE;
    // An unconstrained column that keeps each value's own scale stores it
    // whole, with its display scale, as PostgreSQL does.
    if crate::core::value_codec::keeps_own_scale(target) {
        return Ok(Value::Data { bytes: v.to_cell(), db_type: DbType::DECIMAL });
    }
    let overflow = || "value overflows numeric format".to_string();
    let units = match target.scale {
        Some(scale) => v.units_at(scale).ok_or_else(overflow)?,
        None if v.exact_at(DECIMAL_SCALE) => v.units_at(DECIMAL_SCALE).ok_or_else(overflow)?,
        None => {
            return Err(crate::core::value_codec::old_numeric_refusal(&format!("\"{}\"", v.to_text()), &target.name))
        }
    };
    Ok(Value::Data { bytes: units.to_le_bytes().to_vec(), db_type: DbType::DECIMAL })
}

/// A number as `m × 10^e`, with an upper bound on how far it may be from the
/// number the statement wrote: a fraction written with more digits than a
/// float keeps is carried as the float, which is within half an ulp of it.
#[derive(Debug, Clone, Copy)]
struct ExactNumber {
    m: i128,
    e: i32,
    err: f64,
}

impl ExactNumber {
    fn exact(m: i128, e: i32) -> Self {
        ExactNumber { m, e, err: 0.0 }
    }

    /// Whether this is certainly the number it says, with nothing past
    /// `places` places but zeros.
    fn exact_at(self, places: u32) -> bool {
        self.err == 0.0
            && (self.e >= -(places as i32)
                || 10i128.checked_pow((-self.e - places as i32) as u32).is_some_and(|d| self.m % d == 0))
    }

    /// The number's text (for a message).
    fn text(self) -> String {
        if self.e >= 0 {
            format!("{}{}", self.m, "0".repeat(self.e as usize))
        } else {
            crate::core::value_codec::format_decimal_scaled(self.m, self.e.unsigned_abs())
        }
    }

    fn approx(self) -> f64 {
        self.m as f64 * 10f64.powi(self.e)
    }

    /// Both at the smaller exponent; `None` past `i128`.
    fn aligned(a: Self, b: Self) -> Option<(i128, i128, i32)> {
        let e = a.e.min(b.e);
        let lift = |x: Self| x.m.checked_mul(10i128.checked_pow((x.e - e) as u32)?);
        Some((lift(a)?, lift(b)?, e))
    }

    /// `self` at `places` places, rounded half away from zero, when the bound
    /// on its error cannot move it to another value there; `None` otherwise,
    /// or when it does not fit.
    fn settle(self, places: u32) -> Option<i128> {
        // Worked fine enough to see the error bound, and two places past the
        // target, so the bound shows which way the rounding goes.
        let resolve = if self.err > 0.0 { (-self.err.log10()).ceil() as i64 + 2 } else { 0 };
        let finest = (places as i64 + 2).max(-(self.e as i64)).max(resolve).min(76) as u32;
        // Down to the target's own places when nothing finer fits.
        let (work, m) = (places..=finest.max(places + 2)).rev().find_map(|work| {
            let shift = work as i32 + self.e;
            let m = if shift >= 0 {
                self.m.checked_mul(10i128.checked_pow(shift as u32)?)?
            } else {
                // Places past `work` are a fraction of its last unit, which
                // the spread below takes in.
                10i128.checked_pow(shift.unsigned_abs()).map_or(0, |p| self.m / p)
            };
            Some((work, m))
        })?;
        let truncated = work as i32 + self.e < 0;
        let spread = if self.err == 0.0 && !truncated {
            0
        } else {
            let units = (self.err * 10f64.powi(work as i32)).ceil();
            if !units.is_finite() || units >= 1e30 {
                return None;
            }
            units as i128 + 1
        };
        let value = round_decimal(m, work, places)?;
        if spread != 0
            && (round_decimal(m.checked_sub(spread)?, work, places)? != value
                || round_decimal(m.checked_add(spread)?, work, places)? != value)
        {
            return None;
        }
        Some(value)
    }
}

fn inexact_assignment(column: &str, places: u32) -> String {
    format!(
        "the value for column \"{column}\" cannot be rounded to {places} decimal places with certainty: it \
         lies too close to a rounding boundary for the precision this expression is carried at (not supported)"
    )
}

/// An expression's value for output — a select list's item, a sort key —
/// computed exactly when it reads a NUMERIC column with more places than the
/// engine's and is exactly computable: `a + 1`, `-a`, `COALESCE(a, 0)`,
/// `a * 2` over a NUMERIC(20,12). Such a value is a cell carrying its own
/// scale, as an `AVG` result is, which the wire writes at that scale. Through
/// [`eval`] it was refused for the places past nine. Anything else is
/// [`eval`]'s.
pub(crate) fn eval_output(expr: &Expr, row: &Row, schema: &[SchemaField]) -> Result<Value, String> {
    eval_output_planned(expr, output_is_wide(expr), row, schema)
}

/// Whether [`eval_output`] reads `expr` exactly: a NUMERIC expression over a
/// NUMERIC column with more places than the engine's. Decided once per
/// statement by the callers that evaluate an expression per row, and handed
/// to [`eval_output_planned`]: decided per row, the walk cost a select list
/// or a sort key some 2–7% more instructions.
pub(crate) fn output_is_wide(expr: &Expr) -> bool {
    // Any computed NUMERIC is read through [`pg_numeric`], whose display scale
    // is PostgreSQL's; a plain column read is at its own declared scale.
    static_type(expr) == DbType::DECIMAL && (!matches!(expr, Expr::Column(_)) || reads_wide_decimal(expr, &[]))
}

/// [`pg_numeric`]'s refusal of a value it cannot carry exactly: more places
/// than 38 with digits there, or more digits than an i128 holds. PostgreSQL
/// computes it; cut to the engine's nine places it was answered wrong.
pub(crate) const PAST_38_DIGITS: &str = "the value of a NUMERIC expression needs more digits than the 38 this engine carries \
     exactly (not supported)";

/// Whether `expr` is integer-typed, as [`static_type`] would say, in one walk:
/// [`pg_numeric`] asks it of every arithmetic node on every row, and
/// `static_type` re-derives each operand's type several times over.
fn integer_typed(expr: &Expr) -> bool {
    let integer = |t: &DbType| {
        matches!(
            t,
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
        )
    };
    match expr {
        Expr::Literal { db_type, .. } => integer(db_type),
        Expr::Column(field) => integer(&field.db_type),
        Expr::Arith(l, _, r) | Expr::Mod(l, r) => integer_typed(l) && integer_typed(r),
        Expr::Cast(_, t) => integer(t),
        Expr::Null | Expr::NumericCast { .. } | Expr::Concat(..) | Expr::JsonPath { .. } | Expr::Scalar(_) => false,
        _ => integer(&static_type(expr)),
    }
}

/// A `NUMERIC` expression's value with the display scale PostgreSQL gives it
/// ([`crate::core::rql::pg_numeric`]): a column's declared scale, a written
/// constant's own places, an integer's none; the larger for `+ - %`, the sum
/// for `*`, `select_div_scale` for `/`; the branch taken for `CASE`,
/// `COALESCE`, `LEAST`/`GREATEST` and `NULLIF`, whose result keeps the
/// chosen value's scale. Every step is exact; a quotient is rounded half away
/// from zero at its scale, as `numeric_div` rounds.
///
/// An unconstrained `NUMERIC` column is stored at the engine's nine places,
/// its own display scale lost, and is read with the places its value needs.
///
/// `Ok(None)` for a part this does not compute (a float, a function it does
/// not know, a value past an `i128`), for the caller's engine path to take;
/// `Ok(Some(None))` for NULL.
pub(crate) fn pg_numeric(
    expr: &Expr,
    row: &Row,
    schema: &[SchemaField],
) -> Result<Option<Option<crate::core::rql::pg_numeric::PgNum>>, String> {
    use crate::core::rql::pg_numeric::{self as pgn, PgNum};
    use crate::core::value_codec::DECIMAL_SCALE;
    let integer = |t: &DbType| {
        matches!(
            t,
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
        )
    };
    // A NUMERIC cell: sixteen bytes at `declared` places (the engine's, its
    // value's own, when nothing declared them), or seventeen carrying its own.
    let decimal = |bytes: &[u8], declared: Option<u32>| -> Option<PgNum> {
        match crate::core::value_codec::split_decimal_cell(bytes)? {
            (m, Some(own)) => PgNum::new(m, own),
            (m, None) => match declared {
                Some(s) => PgNum::new(m, s),
                None => Some(PgNum::trimmed(m, DECIMAL_SCALE)),
            },
        }
    };
    let whole = |value: Value| -> Option<Option<PgNum>> {
        match value {
            Value::Null => Some(None),
            value => as_i64(&value).map(|v| Some(PgNum::int(v as i128))),
        }
    };
    Ok(match expr {
        Expr::Null => Some(None),
        Expr::Literal { bytes, db_type: DbType::DECIMAL } => decimal(bytes, None).map(Some),
        Expr::Literal { bytes, db_type } if integer(db_type) => {
            whole(Value::Data { bytes: bytes.clone(), db_type: db_type.clone() })
        }
        Expr::Column(field) => match column_value(field, row, schema) {
            Value::Null => Some(None),
            Value::Data { bytes, db_type: DbType::DECIMAL } => decimal(&bytes, field.scale).map(Some),
            value @ Value::Data { .. } if integer(&field.db_type) => whole(value),
            _ => None,
        },
        // Integer arithmetic is the engine's own, with PostgreSQL's integer
        // division: `(i / 2) * 1.5` halves `i` as an integer first.
        Expr::Arith(..) | Expr::Mod(..) if integer_typed(expr) => whole(eval(expr, row, schema)?),
        // Anything else over two numbers is NUMERIC; a side that is not one
        // (a float, a date) is `None`, and so is the whole.
        Expr::Arith(l, op, r) => {
            let (Some(a), Some(b)) = (pg_numeric(l, row, schema)?, pg_numeric(r, row, schema)?) else {
                return Ok(None);
            };
            let (Some(a), Some(b)) = (a, b) else { return Ok(Some(None)) };
            let value = match op {
                ArithOp::Add => a.add(b),
                ArithOp::Sub => a.sub(b),
                ArithOp::Mul => a.mul(b),
                ArithOp::Div => a.div(b).map_err(str::to_string)?,
                ArithOp::Rem => a.rem(b).map_err(str::to_string)?,
            };
            // Past what 38 digits hold, refused: the engine's nine places
            // answered `u / 100000000000` over 0.000000001 as 0.000000000.
            Some(Some(value.ok_or_else(|| PAST_38_DIGITS.to_string())?))
        }
        Expr::Mod(l, r) => {
            let (Some(a), Some(b)) = (pg_numeric(l, row, schema)?, pg_numeric(r, row, schema)?) else {
                return Ok(None);
            };
            let (Some(a), Some(b)) = (a, b) else { return Ok(Some(None)) };
            Some(Some(a.rem(b).map_err(str::to_string)?.ok_or_else(|| PAST_38_DIGITS.to_string())?))
        }
        Expr::Cast(inner, DbType::DECIMAL) => match static_type(inner) {
            t if t == DbType::DECIMAL || integer(&t) => pg_numeric(inner, row, schema)?,
            // `float8_numeric` / `float4_numeric`: the float's fifteen (six)
            // digit text, read back.
            DbType::F64 | DbType::F32 => match eval(inner, row, schema)? {
                Value::Null => Some(None),
                Value::Data { bytes, db_type: DbType::F64 } => {
                    <[u8; 8]>::try_from(bytes.as_slice()).ok().and_then(|b| pgn::from_f64(f64::from_le_bytes(b))).map(Some)
                }
                Value::Data { bytes, db_type: DbType::F32 } => {
                    <[u8; 4]>::try_from(bytes.as_slice()).ok().and_then(|b| pgn::from_f32(f32::from_le_bytes(b))).map(Some)
                }
                _ => None,
            },
            // `'1.50'::numeric` is two places, as `numeric_in` reads it.
            DbType::STRING => match eval(inner, row, schema)? {
                Value::Null => Some(None),
                Value::Data { bytes, db_type: DbType::STRING } => {
                    std::str::from_utf8(&bytes).ok().and_then(pgn::parse_text).map(Some)
                }
                _ => None,
            },
            _ => None,
        },
        Expr::NumericCast { value, precision, scale } => match pg_numeric(value, row, schema)? {
            None => None,
            Some(None) => Some(None),
            Some(Some(v)) => {
                let Some(rounded) = v.round_to(*scale) else { return Ok(None) };
                crate::core::value_codec::check_numeric_precision(rounded.m, *precision, *scale)?;
                Some(Some(rounded))
            }
        },
        Expr::Coalesce(args) => {
            for arg in args {
                match pg_numeric(arg, row, schema)? {
                    None => return Ok(None),
                    Some(Some(v)) => return Ok(Some(Some(v))),
                    Some(None) => {}
                }
            }
            Some(None)
        }
        Expr::Case { whens, otherwise } => {
            for (cond, result) in whens {
                if eval_cond_3vl(cond, row, schema)? == Some(true) {
                    return pg_numeric(result, row, schema);
                }
            }
            match otherwise {
                Some(result) => pg_numeric(result, row, schema)?,
                None => Some(None),
            }
        }
        // NULLs ignored, NULL only when every one is. Of equal values the
        // first is kept, scale and all.
        Expr::Least(args) | Expr::Greatest(args) => {
            let greatest = matches!(expr, Expr::Greatest(_));
            let mut best: Option<PgNum> = None;
            for arg in args {
                let Some(value) = pg_numeric(arg, row, schema)? else { return Ok(None) };
                let Some(x) = value else { continue };
                best = Some(match best {
                    None => x,
                    Some(b) => {
                        let order = x.cmp_value(b);
                        if (greatest && order.is_gt()) || (!greatest && order.is_lt()) { x } else { b }
                    }
                });
            }
            Some(best)
        }
        Expr::NullIf(a, b) => {
            let (Some(x), Some(y)) = (pg_numeric(a, row, schema)?, pg_numeric(b, row, schema)?) else {
                return Ok(None);
            };
            match (x, y) {
                (Some(x), Some(y)) if x.cmp_value(y).is_eq() => Some(None),
                (x, _) => Some(x),
            }
        }
        Expr::Call(TextFunc::Abs, args) if args.len() == 1 => match pg_numeric(&args[0], row, schema)? {
            Some(Some(v)) => v.abs().map(Some),
            other => other,
        },
        Expr::Call(TextFunc::ExtractField, args) if args.len() == 2 => {
            let field = args.first().and_then(field_literal).ok_or("EXTRACT lost its field")?;
            Some(date_field_value(field, &eval(&args[1], row, schema)?)?)
        }
        Expr::Call(func @ (TextFunc::Round | TextFunc::Trunc), args) if static_type(expr) == DbType::DECIMAL => {
            let places = match args.get(1) {
                None => Some(0),
                Some(Expr::Null) => None,
                Some(places) => match round_places(places) {
                    Some(p) => Some(p),
                    // Read from the row; past either end every answer is the same.
                    None => as_i64(&eval(places, row, schema)?).map(|p| p.clamp(-1000, 1000) as i32),
                },
            };
            match (pg_numeric(&args[0], row, schema)?, places) {
                (None, _) => None,
                (Some(None), _) | (_, None) => Some(None),
                (Some(Some(v)), Some(p)) => Some(Some(
                    if *func == TextFunc::Trunc { v.trunc_places(p) } else { v.round_places(p) }
                        .ok_or_else(|| PAST_38_DIGITS.to_string())?,
                )),
            }
        }
        // A function that yields NUMERIC (`sqrt`, `ceil`, `exp`, `power`, …)
        // over NUMERIC arguments computed here, exactly and at their display
        // scale. Through the engine `sqrt(1::numeric / 3)` read its argument
        // at nine places and was wrong from the tenth digit on.
        Expr::Call(TextFunc::Pg(func), args) if static_type(expr) == DbType::DECIMAL => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                if static_type(arg) != DbType::DECIMAL {
                    values.push(eval(arg, row, schema)?);
                    continue;
                }
                match pg_numeric(arg, row, schema)? {
                    None => return Ok(None),
                    Some(None) => values.push(Value::Null),
                    Some(Some(v)) => values.push(Value::Data { bytes: v.to_cell(), db_type: DbType::DECIMAL }),
                }
            }
            match crate::core::rql::pg_functions::call(*func, &values)? {
                Value::Null => Some(None),
                Value::Data { bytes, db_type: DbType::DECIMAL } => decimal(&bytes, None).map(Some),
                _ => None,
            }
        }
        // Any other integer-typed value — a cast to an integer, a call
        // returning one — is that integer exactly, at scale 0. Without this
        // `(x::int)::numeric / 3` fell back to the engine's nine places,
        // where PostgreSQL divides at `select_div_scale`'s sixteen or more.
        other if integer(&static_type(other)) => whole(eval(other, row, schema)?),
        _ => None,
    })
}

/// [`eval_output`] with [`output_is_wide`] already decided.
pub(crate) fn eval_output_planned(expr: &Expr, wide: bool, row: &Row, schema: &[SchemaField]) -> Result<Value, String> {
    if wide {
        // PostgreSQL's value at PostgreSQL's display scale, in a cell that
        // carries it: `1.0 / 3` is twenty places, `0.00001 * 0.00001` ten,
        // `2.50 * 2` two. Through the engine each was cut to nine places.
        match pg_numeric(expr, row, schema)? {
            Some(None) => return Ok(Value::Null),
            Some(Some(v)) => return Ok(Value::Data { bytes: v.to_cell(), db_type: DbType::DECIMAL }),
            None => {}
        }
        if let Some(value) = wide_exact(expr, row, schema)? {
            let Some(x) = value else { return Ok(Value::Null) };
            let (m, scale) = if x.e <= 0 {
                (x.m, x.e.unsigned_abs())
            } else {
                (
                    x.m.checked_mul(10i128.checked_pow(x.e as u32).ok_or("value overflows numeric format")?)
                        .ok_or("value overflows numeric format")?,
                    0,
                )
            };
            if scale <= 38 {
                return Ok(Value::Data {
                    bytes: crate::core::value_codec::decimal_cell_with_scale(m, scale),
                    db_type: DbType::DECIMAL,
                });
            }
        }
    }
    eval(expr, row, schema)
}

thread_local! {
    /// Why the last CHECK evaluation on this thread met a regular expression
    /// it could not answer for PostgreSQL (see `TextFunc::CheckRegexpLike`).
    static CHECK_UNCERTAIN: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Takes the reason the CHECK evaluated since the last call could not be
/// vouched for, if it could not.
pub(crate) fn take_check_uncertainty() -> Option<String> {
    CHECK_UNCERTAIN.with(|u| u.borrow_mut().take())
}

/// Whether `expr` holds a `NUMERIC` product, quotient or remainder, whose
/// value the engine's nine places can cut ([`pg_numeric`] computes it
/// exactly). Decided once per condition ([`Condition::wide`]).
fn numeric_product_or_quotient(expr: &Expr) -> bool {
    match expr {
        Expr::Arith(a, op, b) => {
            (matches!(op, ArithOp::Mul | ArithOp::Div | ArithOp::Rem) && static_type(expr) == DbType::DECIMAL)
                || numeric_product_or_quotient(a)
                || numeric_product_or_quotient(b)
        }
        Expr::Mod(a, b) | Expr::NullIf(a, b) | Expr::Concat(a, b) => {
            numeric_product_or_quotient(a) || numeric_product_or_quotient(b)
        }
        Expr::Call(_, args) | Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => {
            args.iter().any(numeric_product_or_quotient)
        }
        Expr::Cast(inner, _) => numeric_product_or_quotient(inner),
        Expr::NumericCast { value, .. } | Expr::TypmodCast { value, .. } => numeric_product_or_quotient(value),
        Expr::Case { whens, otherwise } => {
            whens.iter().any(|(_, r)| numeric_product_or_quotient(r))
                || otherwise.as_deref().is_some_and(numeric_product_or_quotient)
        }
        Expr::Column(_) | Expr::Literal { .. } | Expr::Null | Expr::Scalar(_) | Expr::JsonPath { .. } | Expr::PathOf { .. } => false,
    }
}

/// Whether `expr` reads a NUMERIC column with more places than the engine's.
///
/// Asked per row (a comparison, an output item), so it walks the tree and
/// allocates nothing: collecting `columns_of` cloned every field it met,
/// which cost 2–7% of the instructions of `a * 1.1` over 60k rows.
fn reads_wide_decimal(expr: &Expr, schema: &[SchemaField]) -> bool {
    let _ = schema;
    fn wide(expr: &Expr) -> bool {
        match expr {
            Expr::Column(f) => {
                f.db_type == DbType::DECIMAL && f.scale.is_some_and(|s| s > crate::core::value_codec::DECIMAL_SCALE)
            }
            Expr::Literal { .. } => is_wide_literal(expr),
            Expr::Null | Expr::Scalar(_) | Expr::JsonPath { .. } | Expr::PathOf { .. } => false,
            Expr::Concat(a, b) | Expr::NullIf(a, b) | Expr::Mod(a, b) | Expr::Arith(a, _, b) => wide(a) || wide(b),
            Expr::Call(_, args) | Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => args.iter().any(wide),
            Expr::Cast(inner, _) => wide(inner),
            Expr::NumericCast { value, .. } | Expr::TypmodCast { value, .. } => wide(value),
            Expr::Case { whens, otherwise } => {
                fn wide_cond(cond: &Cond) -> bool {
                    match cond {
                        Cond::Cmp(c) => wide(&c.left) || c.right.as_deref().is_some_and(wide),
                        Cond::And(parts) | Cond::Or(parts) => parts.iter().any(wide_cond),
                        Cond::Not(inner) => wide_cond(inner),
                    }
                }
                whens.iter().any(|(c, r)| wide_cond(c) || wide(r)) || otherwise.as_deref().is_some_and(wide)
            }
        }
    }
    wide(expr)
}

/// The exact value of `expr` when it reads a NUMERIC column with more places
/// than the engine's and is exactly computable (`Some(None)` for NULL);
/// `None` when either is not so, for the engine path to take.
fn wide_exact(expr: &Expr, row: &Row, schema: &[SchemaField]) -> Result<Option<Option<ExactNumber>>, String> {
    if !reads_wide_decimal(expr, schema) {
        return Ok(None);
    }
    Ok(exact_numeric_or_engine(expr, row, schema, 30)?.filter(|v| v.is_none_or(|x| x.err == 0.0)))
}

/// [`eval_into`]'s exact reading of `expr`: `Ok(None)` when it has a part
/// this does not compute, `Ok(Some(None))` for NULL. `places` is the scale
/// the answer is wanted at, which a division works past.
fn exact_numeric(
    expr: &Expr,
    row: &Row,
    schema: &[SchemaField],
    places: u32,
) -> Result<Option<Option<ExactNumber>>, String> {
    let integer = |t: &DbType| {
        matches!(
            t,
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 | DbType::U8 | DbType::U16 | DbType::U32
                | DbType::U64
        )
    };
    Ok(match expr {
        Expr::Null => Some(None),
        Expr::Column(field) if field.db_type == DbType::DECIMAL || integer(&field.db_type) => {
            match column_value(field, row, schema) {
                Value::Null => Some(None),
                Value::Data { bytes, db_type: DbType::DECIMAL } => match <[u8; 16]>::try_from(bytes.as_slice()) {
                    Ok(raw) => Some(Some(ExactNumber::exact(
                        i128::from_le_bytes(raw),
                        -(field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE) as i32),
                    ))),
                    Err(_) => None,
                },
                value => as_i64(&value).map(|v| Some(ExactNumber::exact(v as i128, 0))),
            }
        }
        Expr::Literal { bytes, db_type } if *db_type == DbType::DECIMAL || integer(db_type) => {
            let value = Value::Data { bytes: bytes.clone(), db_type: db_type.clone() };
            exact_decimal(&value).map(|(m, e)| Some(ExactNumber::exact(m, e)))
        }
        // A float literal is the number written when a float holds it, which
        // `numeric_constant` makes certain from 1e8 up; below, it may carry
        // more digits than the float keeps, and is within an ulp of it.
        Expr::Literal { bytes, db_type: DbType::F64 } => {
            let value = Value::Data { bytes: bytes.clone(), db_type: DbType::F64 };
            match (exact_decimal(&value), as_f64(&value)) {
                (Some((m, e)), Some(v)) => Some(Some(ExactNumber {
                    m,
                    e,
                    err: if v.abs() < FLOAT_KEEPS_BELOW { v.abs() * f64::EPSILON } else { 0.0 },
                })),
                _ => None,
            }
        }
        Expr::Coalesce(args) => {
            let mut found = Some(None);
            for arg in args {
                match exact_numeric(arg, row, schema, places)? {
                    None => return Ok(None),
                    Some(Some(v)) => {
                        found = Some(Some(v));
                        break;
                    }
                    Some(None) => {}
                }
            }
            found
        }
        Expr::Cast(inner, DbType::DECIMAL) => exact_numeric(inner, row, schema, places)?,
        // `x::numeric(p, s)`, and a written constant held to its own places
        // (`with_written_scale`): rounded there, when the rounding is certain.
        Expr::NumericCast { value, precision, scale } => match exact_numeric(value, row, schema, places)? {
            Some(Some(x)) => {
                let Some(units) = x.settle(*scale) else { return Ok(None) };
                crate::core::value_codec::check_numeric_precision(units, *precision, *scale)?;
                Some(Some(ExactNumber::exact(units, -(*scale as i32))))
            }
            other => other,
        },
        // The branch taken, exactly: through the engine a branch with more
        // places than nine made the whole CASE a float.
        Expr::Case { whens, otherwise } => {
            for (cond, result) in whens {
                if eval_cond_3vl(cond, row, schema)? == Some(true) {
                    return exact_numeric(result, row, schema, places);
                }
            }
            match otherwise {
                Some(result) => return exact_numeric(result, row, schema, places),
                None => Some(None),
            }
        }
        // NULLs ignored, NULL only when every one is, as PostgreSQL has it.
        Expr::Least(args) | Expr::Greatest(args) => {
            let greatest = matches!(expr, Expr::Greatest(_));
            let mut best: Option<ExactNumber> = None;
            for arg in args {
                let Some(value) = exact_numeric(arg, row, schema, places)? else { return Ok(None) };
                let Some(x) = value else { continue };
                if x.err != 0.0 {
                    return Ok(None);
                }
                best = Some(match best {
                    None => x,
                    Some(b) => {
                        let Some((p, q, _)) = ExactNumber::aligned(x, b) else { return Ok(None) };
                        if (p > q) == greatest && p != q { x } else { b }
                    }
                });
            }
            Some(best)
        }
        // Integer arithmetic is the engine's own, exact already and with
        // PostgreSQL's integer division and range: `1 / 3` is 0.
        Expr::Arith(..) if integer(&static_type(expr)) => match eval(expr, row, schema)? {
            Value::Null => Some(None),
            value => as_i64(&value).map(|v| Some(ExactNumber::exact(v as i128, 0))),
        },
        Expr::Arith(l, op, r) if !matches!(op, ArithOp::Rem) => {
            let (Some(a), Some(b)) = (exact_numeric(l, row, schema, places)?, exact_numeric(r, row, schema, places)?)
            else {
                return Ok(None);
            };
            let (Some(a), Some(b)) = (a, b) else {
                return Ok(Some(None));
            };
            exact_arith(a, *op, b, places)?.map(Some)
        }
        _ => None,
    })
}

/// `a op b` exactly for `+ - *`, and for `/` to `places + 6` places with
/// the cut in the error bound. `None` past `i128`.
fn exact_arith(a: ExactNumber, op: ArithOp, b: ExactNumber, places: u32) -> Result<Option<ExactNumber>, String> {
    let (fa, fb) = (a.approx().abs(), b.approx().abs());
    let range = || EXACT_RANGE.to_string();
    Ok(match op {
        ArithOp::Add | ArithOp::Sub => {
            let (x, y, e) = ExactNumber::aligned(a, b).ok_or_else(range)?;
            let m = if matches!(op, ArithOp::Add) { x.checked_add(y) } else { x.checked_sub(y) }.ok_or_else(range)?;
            Some(ExactNumber { m, e, err: a.err + b.err })
        }
        ArithOp::Mul => Some(ExactNumber {
            m: a.m.checked_mul(b.m).ok_or_else(range)?,
            e: a.e + b.e,
            err: fa * b.err + fb * a.err + a.err * b.err,
        }),
        ArithOp::Div => {
            if b.m == 0 {
                return Err("division by zero".to_string());
            }
            // q = a / b at 10^-k: a.m · 10^(a.e - b.e + k) / b.m, with k
            // large enough that the numerator is lifted, never cut: six
            // places past the target where an i128 holds that, else as few
            // as one past it (`a / 7` on a NUMERIC(38,20) of 1e13).
            let lowest = (places as i32 + 1).max(b.e - a.e);
            let mut found = None;
            for k in (lowest..=(places as i32 + 6).max(lowest)).rev() {
                let shift = (a.e - b.e + k) as u32;
                if let Some(numerator) = 10i128.checked_pow(shift).and_then(|p| a.m.checked_mul(p)) {
                    found = Some((k, numerator));
                    break;
                }
            }
            let (k, numerator) = found.ok_or_else(range)?;
            let (q, r) = (numerator / b.m, numerator % b.m);
            // What each operand's own error makes of the quotient.
            let carried = if fb > 0.0 { a.err / fb + fa * b.err / (fb * fb) } else { 0.0 };
            if r == 0 {
                // Exact: `19.99 / 2` is 9.995, a tie the rounding settles.
                Some(ExactNumber { m: q, e: -k, err: carried })
            } else {
                // A sticky digit past the cut: the quotient lies strictly
                // between q and the next unit, so a tie at the target is
                // never mistaken for one, and the cut is in the bound.
                let sign = numerator.signum() * b.m.signum();
                let m = q.checked_mul(10).and_then(|m| m.checked_add(5 * sign)).ok_or_else(range)?;
                Some(ExactNumber { m, e: -(k + 1), err: 0.5 * 10f64.powi(-k) + carried })
            }
        }
        ArithOp::Rem => None,
    })
}

/// [`exact_arith`]'s word for a value whose exact form an i128 cannot hold:
/// the engine path may still answer it (at nine places), which is fine for a
/// NUMERIC that keeps no more and a cut for one that keeps more.
const EXACT_RANGE: &str = "\u{1}exact form out of range";

/// [`exact_numeric`], with a value past its range taken as one it does not
/// compute, for the engine path to answer or refuse.
fn exact_numeric_or_engine(
    expr: &Expr,
    row: &Row,
    schema: &[SchemaField],
    places: u32,
) -> Result<Option<Option<ExactNumber>>, String> {
    match exact_numeric(expr, row, schema, places) {
        Err(e) if e == EXACT_RANGE => Ok(None),
        other => other,
    }
}

/// Lifts a `NUMERIC(p, s)` column's stored integer to the engine scale, so
/// arithmetic and comparisons see one scale whatever each column declared.
///
/// A scale above the engine's is truncated to it — expressions compute at the
/// engine scale, and a column that keeps more places only keeps them in
/// storage. An up-scale that overflows is refused rather than wrapped.
pub(crate) fn to_engine_scale(value: Value, field: &SchemaField) -> Result<Value, String> {
    use crate::core::value_codec::DECIMAL_SCALE;
    let Some(scale) = field.scale.filter(|s| *s != DECIMAL_SCALE) else {
        return Ok(value);
    };
    match value {
        Value::Data {
            bytes,
            db_type: DbType::DECIMAL,
        } if bytes.len() == 16 => {
            let raw = i128::from_le_bytes(bytes.as_slice().try_into().unwrap());
            let lifted = rescale_decimal(raw, scale, DECIMAL_SCALE).ok_or_else(|| {
                format!(
                    "numeric value of column \"{}\" is out of range for arithmetic",
                    field.name
                )
            })?;
            // Places past the engine's are cut by the move down. A value
            // that has digits there is refused rather than answered without
            // them: `a + 1` over a NUMERIC(20,12) holding 0.123456789123 gave
            // 1.123456789000. A value stored in the column is computed
            // exactly instead (`eval_into`).
            // A finished aggregate (`AVG(v) + 1`) is not a stored value: it
            // is read at nine places, as it always was.
            if scale > DECIMAL_SCALE
                && !field.name.starts_with(crate::core::rql::aggregate::COMPUTED_INPUT_PREFIX)
                && rescale_decimal(lifted, DECIMAL_SCALE, scale).is_none_or(|back| back != raw)
            {
                return Err(format!(
                    "numeric value of column \"{}\" has more than {} decimal places, which an expression \
                     here cannot carry exactly (not supported)",
                    field.name, DECIMAL_SCALE
                ));
            }
            Ok(Value::Data {
                bytes: lifted.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            })
        }
        other => Ok(other),
    }
}

/// An expression result about to be stored in `field`, in the column's own
/// type: the inverse of [`to_engine_scale`] for a `NUMERIC(p, s)` column —
/// `UPDATE … SET v = v + 1` — and PostgreSQL's assignment cast for an integer
/// one.
///
/// The write path copies the bytes it is given into the column's slot, so a
/// value wider than the column lost its high bytes: `SET i = i * 1000000000`
/// on an `int4` stored 705032704, and `SET s = s + 1` at 32767 stored -32768,
/// both reported as success. An integer that does not fit is PostgreSQL's
/// `integer out of range`; a fraction assigned to an integer rounds half away
/// from zero, as its `numeric → int` cast does.
pub(crate) fn from_engine_scale(mut value: Value, field: &SchemaField) -> Result<Value, String> {
    use crate::core::value_codec::DECIMAL_SCALE;
    // A NUMERIC carrying its own scale (a constant past nine places, folded
    // or not) bound for another type: PostgreSQL's assignment cast from
    // `numeric`, rounded half away from zero for an integer. Its seventeen
    // bytes read as an engine-scale NUMERIC failed `SET n = 3 + 0.00000000001`
    // with "a numeric could not be read".
    if let Value::Data { bytes, db_type: DbType::DECIMAL } = &value {
        if bytes.len() == 17 && field.db_type != DbType::DECIMAL {
            let (m, own) = crate::core::value_codec::split_decimal_cell(bytes).ok_or("a numeric could not be read")?;
            let own = own.unwrap_or(DECIMAL_SCALE);
            value = match field.db_type {
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32
                | DbType::U64 => {
                    let whole = round_decimal(m, own, 0).ok_or_else(|| integer_out_of_range(&field.db_type))?;
                    let whole = i64::try_from(whole).map_err(|_| integer_out_of_range(&field.db_type))?;
                    Value::Data { bytes: whole.to_le_bytes().to_vec(), db_type: DbType::I64 }
                }
                DbType::F32 | DbType::F64 => match as_f64(&value) {
                    Some(f) => Value::Data { bytes: f.to_le_bytes().to_vec(), db_type: DbType::F64 },
                    None => return Err("a numeric could not be read".to_string()),
                },
                _ => value,
            };
        }
    }
    // A NUMERIC carrying its own scale, or a float, bound for a NUMERIC
    // column: rounded to a declared scale; into an unconstrained column,
    // which keeps nine places, stored when exact there and refused (0A000)
    // when a nonzero digit lies past them — the same rule on every path in
    // (`pg_numeric_into`). A float is its fifteen-digit text (six for
    // `real`), as PostgreSQL's float-to-numeric cast reads it; it was rounded
    // to nine places in silence: `SET u = f / 3` stored 0.033333333. Into a
    // NUMERIC(p, s) too: a `real` 2.675 is its six-digit text 2.675, and
    // rounds to 2.68 as PostgreSQL's does; its binary value 2.67499995…
    // rounded to 2.67.
    if field.db_type == DbType::DECIMAL {
        use crate::core::rql::pg_numeric::{self as pgn, PgNum};
        let read = match &value {
            Value::Data { bytes, db_type: DbType::DECIMAL } if bytes.len() == 17 => {
                crate::core::value_codec::split_decimal_cell(bytes).and_then(|(m, own)| PgNum::new(m, own?))
            }
            Value::Data { bytes, db_type: DbType::F64 } => {
                <[u8; 8]>::try_from(bytes.as_slice()).ok().map(f64::from_le_bytes).and_then(pgn::from_f64)
            }
            Value::Data { bytes, db_type: DbType::F32 } => {
                <[u8; 4]>::try_from(bytes.as_slice()).ok().map(f32::from_le_bytes).and_then(pgn::from_f32)
            }
            _ => None,
        };
        if let Some(v) = read {
            return pg_numeric_into(v, field);
        }
        // A float `float8_numeric` cannot read into the 38 places a NUMERIC
        // carries here (`1e-300`, `1e-39`), or one with no numeric value (NaN,
        // an infinity): refused, as `f::numeric` is. It fell through to the
        // nine-place path below and was stored as 0 with no error.
        if let Value::Data { bytes, db_type: DbType::F64 | DbType::F32 } = &value {
            let v = match bytes.len() {
                8 => <[u8; 8]>::try_from(bytes.as_slice()).ok().map(f64::from_le_bytes),
                4 => <[u8; 4]>::try_from(bytes.as_slice()).ok().map(|b| f64::from(f32::from_le_bytes(b))),
                _ => None,
            };
            // A declared scale rounds a float too small for 38 places to zero,
            // as PostgreSQL's assignment does (`1e-200` into a NUMERIC(10,2)
            // is 0.00), and one too large for it overflows the field.
            if let (Some(v), Some(_)) = (v, field.scale) {
                if v.is_finite() && v.abs() < 1.0 {
                    return pg_numeric_into(PgNum::int(0), field);
                }
                if v.is_finite() {
                    return Err(format!("numeric field overflow for column \"{}\"", field.name));
                }
            }
            if let Some(v) = v {
                let old_format = !crate::core::value_codec::keeps_own_scale(field);
                return Err(if v.is_finite() && old_format && v.abs() < 1.0 {
                    // A sixteen-byte column keeps nine places: its own refusal,
                    // with the conversion hint.
                    crate::core::value_codec::old_numeric_refusal(
                        &crate::core::value_codec::double_quoted(&crate::core::value_codec::pg_float8_text(v)),
                        &field.name,
                    )
                } else if v.is_finite() && old_format {
                    format!("numeric field overflow for column \"{}\"", field.name)
                } else if v.is_finite() {
                    crate::core::value_codec::own_scale_refusal(
                        &crate::core::value_codec::double_quoted(&crate::core::value_codec::pg_float8_text(v)),
                        &field.name,
                    )
                } else {
                    crate::core::value_codec::numeric_special_refusal(v, field.scale.is_some())
                });
            }
        }
        // Into a column that keeps each value's own scale: an engine-scale
        // NUMERIC at the places its value needs (the zeros past them are the
        // engine's, not the value's), an integer as itself, text as written.
        if crate::core::value_codec::keeps_own_scale(field) {
            let own = match &value {
                Value::Null => return Ok(Value::Null),
                Value::Data { bytes, db_type: DbType::DECIMAL } => <[u8; 16]>::try_from(bytes.as_slice())
                    .ok()
                    .map(|b| PgNum::trimmed(i128::from_le_bytes(b), DECIMAL_SCALE)),
                Value::Data { db_type: DbType::STRING | DbType::CHAR, .. } => {
                    let text = as_text(&value);
                    Some(
                        pgn::parse_text(text.trim())
                            .ok_or_else(|| crate::core::value_codec::refusal_for_column(field, text.trim()))?,
                    )
                }
                other @ Value::Data {
                    db_type:
                        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32
                        | DbType::U64,
                    ..
                } => as_i64(other).map(|n| PgNum::int(n as i128)),
                _ => None,
            };
            if let Some(v) = own {
                return pg_numeric_into(v, field);
            }
        }
    }
    let number = |v: &Value| {
        matches!(
            v.db_type(),
            Some(
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32
                    | DbType::U64 | DbType::F32 | DbType::F64 | DbType::DECIMAL
            )
        )
    };
    // A date, a timestamp and an interval each have their own layout, so one
    // bound for a column of another type is converted, as PostgreSQL's
    // assignment cast does, or refused. Its bytes used to be copied into the
    // slot as they were: `SET ts = '2024-01-01'::date` put a four-byte day
    // count in a timestamp and read back as 1970-01-01 00:17:22, and a
    // timestamp in a date column kept half of itself. A number and a date
    // have no assignment cast either way.
    let temporal = |t: &DbType| matches!(t, DbType::DATE | DbType::DATETIME | DbType::INTERVAL);
    if let Value::Data { db_type: from, .. } = &value {
        if *from != field.db_type && (temporal(from) || temporal(&field.db_type)) {
            let mismatch = || {
                format!(
                    "column \"{}\" is of type {} but expression is of type {}",
                    field.name,
                    crate::core::value_codec::pg_input_type_name(&field.db_type),
                    crate::core::value_codec::pg_input_type_name(from)
                )
            };
            let convertible = (temporal(from) && temporal(&field.db_type))
                || *from == DbType::STRING
                || field.db_type == DbType::STRING;
            if !convertible {
                return Err(mismatch());
            }
            // Text bound for a `timestamp` (without time zone) column is read
            // as that column reads a literal, its zone discarded: `SET a =
            // CASE … THEN '… 10:00+02' END` stored 08:00 where the same
            // literal written directly, and PostgreSQL, store 10:00.
            if *from == DbType::STRING && field.literal_type() == DbType::TIMESTAMP {
                let text = as_text(&value);
                return crate::core::value_codec::encode_literal(&DbType::TIMESTAMP, text.trim())
                    .map(|bytes| Value::Data { bytes, db_type: DbType::DATETIME })
                    .ok_or_else(mismatch);
            }
            return cast(value.clone(), &field.db_type).map_err(|_| mismatch());
        }
    }
    // A document column takes a document, or text it can read as one; any
    // other value's bytes were copied into the slot as they were, so text
    // bound for a `jsonb` column was stored where no reader could open it.
    // A document bound for a text column is its JSON, PostgreSQL's
    // assignment cast to a string type.
    if field.db_type == DbType::JSONB {
        return match value {
            Value::Null | Value::Data { db_type: DbType::JSONB, .. } => Ok(value),
            Value::Data { db_type: DbType::STRING | DbType::CHAR, .. } => {
                as_jsonb(value).map(|bytes| Value::Data { bytes, db_type: DbType::JSONB })
            }
            Value::Data { db_type: from, .. } => Err(format!(
                "column \"{}\" is of type jsonb but expression is of type {}",
                field.name,
                crate::core::value_codec::pg_input_type_name(&from)
            )),
        };
    }
    if matches!(field.db_type, DbType::STRING | DbType::CHAR)
        && matches!(value, Value::Data { db_type: DbType::JSONB, .. })
    {
        return Ok(Value::Data {
            bytes: as_text(&value).into_bytes(),
            db_type: DbType::STRING,
        });
    }
    // Any other value bound for a text column is its text, PostgreSQL's
    // assignment cast to a string type. Its bytes were copied into the slot:
    // `SET v = n + 1` stored the integer's four bytes, `SET v = (1e1)` a
    // decimal's sixteen, and `SET v = (65)` read back as 'A'.
    if matches!(field.db_type, DbType::STRING | DbType::CHAR) {
        match &value {
            Value::Data { db_type: DbType::STRING | DbType::CHAR, .. } | Value::Null => {}
            Value::Data { db_type: DbType::BOOL, bytes } => {
                let text = if bytes.first().copied().unwrap_or(0) != 0 { "true" } else { "false" };
                return Ok(Value::Data { bytes: text.as_bytes().to_vec(), db_type: DbType::STRING });
            }
            Value::Data { .. } => return cast(value, &DbType::STRING),
        }
    }
    // Text bound for a number, a boolean or a UUID is read as one, or refused
    // as that type's input refuses it. Its bytes were copied into the slot:
    // `SET b = '5' || ''` stored 53, the code of the character `5`, and a
    // NUMERIC(10,2) read the same byte as 0.53.
    if matches!(value, Value::Data { db_type: DbType::STRING, .. })
        && (number(&Value::Data { bytes: Vec::new(), db_type: field.db_type.clone() })
            || matches!(field.db_type, DbType::BOOL | DbType::UUID))
    {
        let text = as_text(&value);
        value = cast(value, &field.db_type).map_err(|e| {
            if e.contains(" cannot be read as ") && field.db_type == DbType::DECIMAL {
                // An unconstrained NUMERIC's own refusal, with its limit.
                crate::core::value_codec::refusal_for_column(field, &text)
            } else if e.contains(" cannot be read as ") {
                crate::core::value_codec::refusal_message(&field.db_type, &text)
            } else {
                e
            }
        })?;
    }
    // A float column takes any number as a float, as the assignment cast does.
    // A written fraction is a NUMERIC now, and its sixteen bytes in an
    // eight-byte slot would be read back as an unrelated float.
    if matches!(field.db_type, DbType::F32 | DbType::F64) {
        return if number(&value) { cast(value, &field.db_type) } else { Ok(value) };
    }
    if field.db_type != DbType::DECIMAL {
        return to_integer_column(value, &field.db_type);
    }
    // An integer or a float bound for a NUMERIC column becomes one at the
    // engine scale first; its own bytes in the sixteen-byte slot were not a
    // number at all.
    let value = match &value {
        Value::Data { db_type, .. } if number(&value) && *db_type != DbType::DECIMAL => {
            // A float meets a declared scale once, from the digits that
            // round-trip it. Rounding it to the engine scale first and then to
            // the column's rounded twice: 20.1649999999 became 20.165, then
            // 20.17, where PostgreSQL stores 20.16.
            if let (DbType::F32 | DbType::F64, Some(scale)) = (db_type, field.scale) {
                let stored = as_f64(&value)
                    .filter(|f| f.is_finite())
                    .and_then(|f| crate::core::value_codec::parse_decimal_rounded(&format!("{}", f), scale))
                    .ok_or_else(|| format!("numeric field overflow for column \"{}\"", field.name))?;
                return Ok(Value::Data {
                    bytes: stored.to_le_bytes().to_vec(),
                    db_type: DbType::DECIMAL,
                });
            }
            let units = match db_type {
                DbType::F32 | DbType::F64 => as_f64(&value).and_then(float_to_engine_decimal),
                _ => as_i64(&value).and_then(|v| (v as i128).checked_mul(10i128.pow(DECIMAL_SCALE))),
            }
            .ok_or_else(|| format!("numeric field overflow for column \"{}\"", field.name))?;
            Value::Data {
                bytes: units.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            }
        }
        _ => value,
    };
    let Some(scale) = field.scale.filter(|s| *s != DECIMAL_SCALE) else {
        return Ok(value);
    };
    match value {
        Value::Data {
            bytes,
            db_type: DbType::DECIMAL,
        } if bytes.len() == 16 => {
            let raw = i128::from_le_bytes(bytes.as_slice().try_into().unwrap());
            // Rounded half away from zero, as PostgreSQL rounds into a declared
            // scale; truncating stored 20.26 for `SET p = p * 1.005` at 20.17.
            let stored = round_decimal(raw, DECIMAL_SCALE, scale).ok_or_else(|| {
                format!("numeric field overflow for column \"{}\"", field.name)
            })?;
            Ok(Value::Data {
                bytes: stored.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            })
        }
        other => Ok(other),
    }
}

/// Refuses a value [`from_engine_scale`] brought to a `NUMERIC(p, s)` column
/// that the column's precision cannot hold — `numeric field overflow`. The
/// precision lives in the type-modifier sidecar, not on the field, so the
/// caller passes the table's modifiers.
pub(crate) fn check_declared_precision(
    value: &Value,
    field: &SchemaField,
    modifiers: &[crate::core::rql::type_modifier::TypeModifier],
) -> Result<(), String> {
    let Value::Data {
        bytes,
        db_type: DbType::DECIMAL,
    } = value
    else {
        return Ok(());
    };
    let Some(precision) = crate::core::rql::type_modifier::precision_of(&field.name, modifiers) else {
        return Ok(());
    };
    let Ok(raw) = <[u8; 16]>::try_from(bytes.as_slice()) else {
        return Ok(());
    };
    crate::core::value_codec::check_numeric_precision(
        i128::from_le_bytes(raw),
        precision,
        field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
    )
}

/// `value`, assigned to `field`, held to the field's declared text length
/// (`type_modifier::TextTypmod::assign`): a `character(n)` value without its
/// trailing spaces, past the length an error (22001).
pub(crate) fn held_to_text_length(value: Value, field: &SchemaField) -> Result<Value, String> {
    match &value {
        // A legacy `CHAR` column takes the value's text as `character(1)`
        // does (`value_codec::legacy_char_assign`): `SET c = c || 'zz'` is
        // 22001 and `SET c = 5` is `'5'`. The bytes were copied into the
        // cell, cut at four: the first four of a longer text, a number's
        // little-endian bytes.
        Value::Data { bytes, db_type } if field.db_type == DbType::CHAR => {
            let text = match db_type {
                DbType::CHAR => crate::core::value_codec::legacy_char_text(bytes).to_string(),
                DbType::STRING => std::str::from_utf8(bytes)
                    .map_err(|_| "invalid byte sequence for encoding \"UTF8\"".to_string())?
                    .to_string(),
                _ => as_text(&value),
            };
            let held = crate::core::value_codec::legacy_char_assign(&text)?;
            Ok(Value::Data { bytes: held.to_vec(), db_type: DbType::CHAR })
        }
        // A text value to a declared length, a time value to a declared
        // precision (`type_modifier::assign_bytes` checks the column's type).
        Value::Data { bytes, db_type } if *db_type == field.db_type || *db_type == DbType::STRING => {
            match crate::core::rql::type_modifier::assign_bytes(field, bytes)? {
                Some(held) => Ok(Value::Data { bytes: held, db_type: db_type.clone() }),
                None => Ok(value),
            }
        }
        _ => Ok(value),
    }
}

/// A number assigned to an integer column, at that column's width or refused.
/// Any other target takes the value unchanged.
pub(crate) fn to_integer_column(value: Value, target: &DbType) -> Result<Value, String> {
    if !matches!(
        target,
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
    ) {
        return Ok(value);
    }
    let whole: i128 = match &value {
        Value::Null => return Ok(Value::Null),
        Value::Data {
            bytes,
            db_type: DbType::DECIMAL,
        } => {
            let raw = i128::from_le_bytes(
                bytes.as_slice().try_into().map_err(|_| "a numeric could not be read".to_string())?,
            );
            round_decimal(raw, crate::core::value_codec::DECIMAL_SCALE, 0)
                .ok_or_else(|| integer_out_of_range(target))?
        }
        Value::Data {
            db_type: DbType::F32 | DbType::F64,
            ..
        } => {
            let f = as_f64(&value).unwrap_or(f64::NAN);
            // Ties to even: PostgreSQL converts a float with `rint`, so
            // `2.5::float8::int` is 2 and `-2.5` is -2. Only `numeric` rounds
            // half away from zero (`2.5::numeric::int` is 3), which is the arm
            // above. This used `f64::round` for both, so every exact .5 from a
            // float landed one away from PostgreSQL's answer, with no error.
            let r = f.round_ties_even();
            if !f.is_finite() || r.abs() >= 1.7e38 {
                return Err(integer_out_of_range(target));
            }
            r as i128
        }
        Value::Data { db_type, .. }
            if matches!(
                db_type,
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
            ) =>
        {
            as_i64(&value).ok_or_else(|| integer_out_of_range(target))? as i128
        }
        // Anything else is not a number this conversion knows; it is left as
        // it was, which is what this did for every type before.
        _ => return Ok(value),
    };
    let (min, max): (i128, i128) = match target {
        DbType::I8 => (i8::MIN.into(), i8::MAX.into()),
        DbType::I16 => (i16::MIN.into(), i16::MAX.into()),
        DbType::I32 => (i32::MIN.into(), i32::MAX.into()),
        DbType::U8 => (0, u8::MAX.into()),
        DbType::U16 => (0, u16::MAX.into()),
        DbType::U32 => (0, u32::MAX.into()),
        DbType::U64 => (0, u64::MAX.into()),
        _ => (i64::MIN.into(), i64::MAX.into()),
    };
    if whole < min || whole > max {
        return Err(integer_out_of_range(target));
    }
    let bytes = match target {
        DbType::I8 => (whole as i8).to_le_bytes().to_vec(),
        DbType::I16 => (whole as i16).to_le_bytes().to_vec(),
        DbType::I32 => (whole as i32).to_le_bytes().to_vec(),
        DbType::U8 => (whole as u8).to_le_bytes().to_vec(),
        DbType::U16 => (whole as u16).to_le_bytes().to_vec(),
        DbType::U32 => (whole as u32).to_le_bytes().to_vec(),
        DbType::U64 => (whole as u64).to_le_bytes().to_vec(),
        _ => (whole as i64).to_le_bytes().to_vec(),
    };
    Ok(Value::Data {
        bytes,
        db_type: target.clone(),
    })
}

/// Moves a scaled integer to a smaller scale rounding half away from zero, as
/// PostgreSQL rounds a value into a declared scale; upward it is exact, as in
/// [`rescale_decimal`]. `None` on overflow.
pub(crate) fn round_decimal(raw: i128, from: u32, to: u32) -> Option<i128> {
    if from <= to {
        return rescale_decimal(raw, from, to);
    }
    let divisor = 10i128.checked_pow(from - to)?;
    let quotient = raw / divisor;
    let remainder = raw % divisor;
    // `|remainder| >= divisor / 2`, without doubling a remainder near the top
    // of the range.
    if remainder.unsigned_abs() >= divisor.unsigned_abs() - remainder.unsigned_abs() {
        quotient.checked_add(raw.signum())
    } else {
        Some(quotient)
    }
}

/// Moves a scaled integer from one scale to another: exact upward (or `None`
/// on overflow), truncated toward zero downward, as a declared scale truncates.
pub(crate) fn rescale_decimal(raw: i128, from: u32, to: u32) -> Option<i128> {
    use std::cmp::Ordering;
    match from.cmp(&to) {
        Ordering::Equal => Some(raw),
        Ordering::Less => raw.checked_mul(10i128.checked_pow(to - from)?),
        Ordering::Greater => Some(raw / 10i128.checked_pow(from - to)?),
    }
}

/// Microseconds in a day, the step between two consecutive dates.
const DAY_US: i64 = 86_400_000_000;

/// A date's day count from PostgreSQL's epoch, 2000-01-01.
fn date_days(value: &Value) -> Option<i32> {
    match value {
        Value::Data {
            bytes,
            db_type: DbType::DATE,
        } => Some(i32::from_be_bytes(bytes.as_slice().try_into().ok()?)),
        _ => None,
    }
}

fn date_value(days: i64) -> Result<Value, String> {
    let days = i32::try_from(days).map_err(|_| "date out of range".to_string())?;
    Ok(Value::Data {
        bytes: days.to_be_bytes().to_vec(),
        db_type: DbType::DATE,
    })
}

/// A date or a timestamp as microseconds since the Unix epoch, which is how a
/// timestamp is stored; a date is its midnight.
fn instant_of(db_type: &DbType, bytes: &[u8]) -> Option<i64> {
    match db_type {
        DbType::DATE => {
            let days = i32::from_be_bytes(bytes.try_into().ok()?);
            (i64::from(days) + i64::from(crate::core::pg_types::PG_EPOCH_DAYS)).checked_mul(DAY_US)
        }
        DbType::DATETIME => Some(i64::from_le_bytes(bytes.try_into().ok()?)),
        _ => None,
    }
}

/// `CURRENT_DATE` is evaluated as a timestamp at midnight. As an operand of
/// arithmetic it is the date it names, so `CURRENT_DATE + 1` is tomorrow and
/// not one microsecond past midnight.
fn date_operand(expr: Expr) -> Expr {
    match expr {
        Expr::Scalar(crate::core::rql::aggregate::ScalarFunc::CurrentDate) => {
            Expr::Cast(Box::new(expr), DbType::DATE)
        }
        other => other,
    }
}

/// Arithmetic with a date on either side, by PostgreSQL's operators:
/// `date ± integer` and `integer + date` are a date that many days away,
/// `date - date` is the day count between them (an `integer`), `date ±
/// interval` is a timestamp, and a date against a timestamp is that
/// timestamp's arithmetic with the date's midnight. Anything else is refused.
fn date_arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
    let integer = |v: &Value| {
        matches!(
            v.db_type(),
            Some(DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32)
        )
    };
    let refuse = || {
        format!(
            "operator does not exist: {} {} {}",
            a.db_type().map(crate::core::value_codec::pg_input_type_name).unwrap_or("unknown"),
            op.symbol(),
            b.db_type().map(crate::core::value_codec::pg_input_type_name).unwrap_or("unknown")
        )
    };
    let as_timestamp = |v: &Value| -> Value {
        match v {
            Value::Data {
                bytes,
                db_type: DbType::DATE,
            } => match instant_of(&DbType::DATE, bytes) {
                Some(us) => Value::Data {
                    bytes: us.to_le_bytes().to_vec(),
                    db_type: DbType::DATETIME,
                },
                None => v.clone(),
            },
            other => other.clone(),
        }
    };
    match (date_days(a), date_days(b)) {
        (Some(x), Some(y)) if matches!(op, ArithOp::Sub) => {
            let diff = i64::from(x) - i64::from(y);
            Ok(Value::Data {
                bytes: (diff as i32).to_le_bytes().to_vec(),
                db_type: DbType::I32,
            })
        }
        (Some(x), None) if integer(b) && matches!(op, ArithOp::Add | ArithOp::Sub) => {
            let n = as_i64(b).ok_or_else(refuse)?;
            let n = if matches!(op, ArithOp::Sub) { n.checked_neg().ok_or_else(refuse)? } else { n };
            date_value(i64::from(x).checked_add(n).ok_or_else(|| "date out of range".to_string())?)
        }
        (None, Some(y)) if integer(a) && matches!(op, ArithOp::Add) => {
            let n = as_i64(a).ok_or_else(refuse)?;
            date_value(i64::from(y).checked_add(n).ok_or_else(|| "date out of range".to_string())?)
        }
        // A date meeting an interval or a timestamp is its midnight; the
        // timestamp arithmetic below takes it from there.
        (Some(_), None)
            if matches!(
                (b.db_type(), op),
                (Some(DbType::INTERVAL), ArithOp::Add | ArithOp::Sub) | (Some(DbType::DATETIME), ArithOp::Sub)
            ) =>
        {
            arith(&as_timestamp(a), op, b)
        }
        (None, Some(_)) if matches!((a.db_type(), op), (Some(DbType::DATETIME), ArithOp::Sub)) => {
            arith(a, op, &as_timestamp(b))
        }
        // `interval + date`, which the timestamp arithmetic reads date first.
        (None, Some(_)) if matches!((a.db_type(), op), (Some(DbType::INTERVAL), ArithOp::Add)) => {
            arith(&as_timestamp(b), op, a)
        }
        _ => Err(refuse()),
    }
}

/// Unary minus, as `0 - x` with the zero typed like `x`, so the result keeps
/// `x`'s type and its overflow check: `-(-2147483648)` is `integer out of
/// range`, as it is in PostgreSQL, rather than a wider integer.
fn negate(inner: Expr) -> Result<Expr, String> {
    if matches!(inner, Expr::Null) {
        return Ok(Expr::Null);
    }
    let zero = match static_type(&inner) {
        DbType::I8 | DbType::U8 | DbType::I16 | DbType::U16 => (0i16.to_le_bytes().to_vec(), DbType::I16),
        DbType::I32 | DbType::U32 => (0i32.to_le_bytes().to_vec(), DbType::I32),
        DbType::I64 | DbType::U64 => (0i64.to_le_bytes().to_vec(), DbType::I64),
        DbType::DECIMAL => (0i128.to_le_bytes().to_vec(), DbType::DECIMAL),
        DbType::F32 | DbType::F64 => (0f64.to_le_bytes().to_vec(), DbType::F64),
        DbType::INTERVAL => (
            crate::core::pg_types::encode_interval(crate::core::pg_types::Interval {
                months: 0,
                days: 0,
                micros: 0,
            }),
            DbType::INTERVAL,
        ),
        other => return Err(format!("operator does not exist: - {}", other)),
    };
    Ok(Expr::Arith(
        Box::new(Expr::Literal {
            bytes: zero.0,
            db_type: zero.1,
        }),
        ArithOp::Sub,
        Box::new(inner),
    ))
}

/// The type an integer result takes: the wider of the two operands.
///
/// An unsigned type ranks with the signed type one wider, the narrowest that
/// holds all its values, as `pg_input_type_name` names it: a `UINT` holding
/// 3000000000 plus `1` was computed as `integer` and refused as out of range.
fn wider_integer(a: &Value, b: &Value) -> DbType {
    let rank = |value: &Value| match value {
        Value::Data { db_type, .. } => match db_type {
            DbType::I8 | DbType::U8 => 1,
            DbType::I16 => 2,
            DbType::I32 | DbType::U16 => 3,
            _ => 4,
        },
        Value::Null => 0,
    };
    match rank(a).max(rank(b)) {
        0 | 1 | 2 => DbType::I16,
        3 => DbType::I32,
        _ => DbType::I64,
    }
}

/// Encodes an integer in the width its type declares, or `None` if it does not
/// fit — which PostgreSQL reports as an error rather than wrapping.
fn narrow_integer(value: i64, db_type: &DbType) -> Option<Vec<u8>> {
    Some(match db_type {
        DbType::I16 => i16::try_from(value).ok()?.to_le_bytes().to_vec(),
        DbType::I32 => i32::try_from(value).ok()?.to_le_bytes().to_vec(),
        _ => value.to_le_bytes().to_vec(),
    })
}

pub(crate) fn as_f64(value: &Value) -> Option<f64> {
    let Value::Data { bytes, db_type } = value else {
        return None;
    };
    match db_type {
        DbType::F32 => Some(f32::from_le_bytes(bytes.as_slice().try_into().ok()?) as f64),
        DbType::F64 => Some(f64::from_le_bytes(bytes.as_slice().try_into().ok()?)),
        // Through its text, which `str::parse` reads to the nearest float: a
        // division by the scale would round twice.
        // A cell at its own scale (a wide constant), through `m e-s`.
        DbType::DECIMAL if bytes.len() == 17 => {
            let (m, own) = crate::core::value_codec::split_decimal_cell(bytes)?;
            format!("{m}e-{}", own?).parse().ok()
        }
        DbType::DECIMAL => crate::core::value_codec::format_decimal(i128::from_le_bytes(
            bytes.as_slice().try_into().ok()?,
        ))
        .parse()
        .ok(),
        _ => as_i64(value).map(|v| v as f64),
    }
}

/// True when `text`, which opens with a quote, is one string literal: the
/// quote that closes it, a doubled quote being an escaped one, is the last
/// character.
fn is_one_literal(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return i == bytes.len() - 1;
        }
        i += 1;
    }
    false
}

/// True when the whole text is a single numeric literal, sign and exponent
/// included — so the `-` in `-5` and `1e-5` is never read as subtraction.
/// A written number and nothing else: digits, a point, an exponent and a
/// leading sign — not `inf` or `NaN`, which `f64` would also read.
pub(crate) fn is_plain_number(text: &str) -> bool {
    is_number_literal(text)
        && text.bytes().any(|b| b.is_ascii_digit())
        && text.bytes().all(|b| b.is_ascii_digit() || b".+-eE".contains(&b))
}

fn is_number_literal(text: &str) -> bool {
    text.parse::<i64>().is_ok() || text.parse::<f64>().is_ok()
}

/// True when the text is one whole `CASE … END` block.
///
/// The block's own `END` has to be the last word. Starting with `CASE` and
/// ending with `END` is not enough: `CASE … END + CASE … END` does both.
fn is_case_block(text: &str) -> bool {
    case_block_end(text) == Some(text.len())
}

/// Whether the sign at `at` is an exponent's, as in `1e-10` or `2.5E+3`:
/// directly after an `e` that ends a number. It used to split the literal,
/// so `1e20 * 1e-10` read "'1e' is neither a column nor a value".
fn in_exponent(bytes: &[u8], at: usize) -> bool {
    if at < 2 || !matches!(bytes[at - 1], b'e' | b'E') {
        return false;
    }
    // The run before the `e`: digits with at most one point, starting a word.
    let mut j = at - 1;
    let mut digits = false;
    while j > 0 && (bytes[j - 1].is_ascii_digit() || bytes[j - 1] == b'.') {
        digits |= bytes[j - 1].is_ascii_digit();
        j -= 1;
    }
    digits && (j == 0 || !(bytes[j - 1].is_ascii_alphanumeric() || bytes[j - 1] == b'_'))
}

/// Finds the operator a binary arithmetic expression splits at, if any.
///
/// Scans right to left so the split lands on the *last* operator of a run,
/// which is what makes `a - b - c` group as `(a - b) - c` rather than
/// `a - (b - c)`. `+`/`-` are tried first because they bind loosest; only if
/// there are none does it look for `*`, `/` or `%`.
fn top_level_arith(text: &str) -> Option<(usize, ArithOp)> {
    // Quotes, parentheses and `CASE` arms are all nesting here: a `+` inside
    // `CASE … THEN n + 1 END` is the arm's, not the expression's.
    let nesting = Nesting::of(text);
    for pass in 0..2 {
        let bytes = text.as_bytes();
        // Right-to-left would have to re-derive quote and paren state, so the
        // scan runs forward and keeps the last candidate it saw.
        let mut found: Option<(usize, ArithOp)> = None;
        for (i, ch) in text.char_indices() {
            let top = nesting.top_level(i);
            match ch {
                // `->` and `->>` reach into a document. Their `-` follows a
                // column name, so every test below would take it for a
                // subtraction and split the expression through the middle of
                // the operator.
                '-' if top && bytes.get(i + 1) == Some(&b'>') => continue,
                '+' | '-' | '*' | '/' | '%' if top => {
                    let op = match ch {
                        '+' => ArithOp::Add,
                        '-' => ArithOp::Sub,
                        '*' => ArithOp::Mul,
                        '/' => ArithOp::Div,
                        _ => ArithOp::Rem,
                    };
                    let wanted_additive = matches!(op, ArithOp::Add | ArithOp::Sub);
                    if (pass == 0) != wanted_additive {
                        continue;
                    }
                    // `*` in `COUNT(*)` sits inside parens, so nesting covers it.
                    // A sign is only an operator when something can precede it:
                    // in `1 * -2` the `-` follows an operator and is part of the
                    // literal, not a subtraction.
                    let prev = bytes[..i]
                        .iter()
                        .rposition(|b| !b.is_ascii_whitespace())
                        .map(|j| bytes[j]);
                    let binds = matches!(prev, Some(b)
                        if b.is_ascii_alphanumeric()
                            || b == b'_'
                            || b == b')'
                            || b == b']'
                            || b == b'"'
                            || b == b'\''
                            || b == b'.');
                    if binds && !(wanted_additive && in_exponent(bytes, i)) {
                        found = Some((i, op));
                    }
                }
                _ => {}
            }
        }
        if found.is_some() {
            return found;
        }
    }
    None
}

/// The last top-level `^` (exponentiation) in `text`, which is where a left
/// to right grouping splits: `a ^ b ^ c` is `(a ^ b) ^ c`. Not `^@`
/// (`starts_with`), and only after something that can end an operand.
fn top_level_power(text: &str) -> Option<usize> {
    let nesting = Nesting::of(text);
    let bytes = text.as_bytes();
    let mut found = None;
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'^' || !nesting.top_level(i) || bytes.get(i + 1) == Some(&b'@') {
            continue;
        }
        let prev = bytes[..i].iter().rposition(|b| !b.is_ascii_whitespace()).map(|j| bytes[j]);
        if matches!(prev, Some(p) if p.is_ascii_alphanumeric() || matches!(p, b'_' | b')' | b']' | b'"' | b'\'' | b'.')) {
            found = Some(i);
        }
    }
    found
}

/// Whether `expr` is PostgreSQL's `json`, text kept as written: a `json`
/// column, a member of one by `->`/`#>`, or a cast to `json`. Its value
/// travels as the `STRING` it is stored as; this is what says otherwise, for
/// `pg_typeof` and a describe.
pub(crate) fn yields_json_text(expr: &Expr) -> bool {
    match expr {
        Expr::Column(field) => field.json_text,
        Expr::JsonPath { column, as_text, .. } => column.json_text && !as_text,
        Expr::PathOf { base, as_text, .. } => yields_json_text(base) && !as_text,
        Expr::Cast(_, DbType::JSONTEXT) => true,
        Expr::Call(TextFunc::Pg(func), _) => crate::core::rql::pg_functions::yields_json_text(*func),
        _ => false,
    }
}

fn column_value(field: &SchemaField, row: &Row, schema: &[SchemaField]) -> Value {
    // The schema is searched in place. This used to clone it into a SmallVec
    // first — every field and every field's name, once per column reference
    // per row — only to satisfy `flag_field`'s parameter type.
    if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema) {
        let is_null = row
            .columns
            .iter()
            .find(|c| u64::from(c.schema_id) == flag.write_order)
            .map(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
            .unwrap_or(false);
        if is_null {
            return Value::Null;
        }
    }
    match row
        .columns
        .iter()
        .find(|c| u64::from(c.schema_id) == field.write_order)
    {
        // A cell carrying `NULL` as its type *is* a NULL.
        //
        // The companion lookup above finds one by name, which works while the
        // schema is a table's own, and on a combined join row now that its
        // companion is renamed `__null_s__last_queried_at` with the column's
        // `s__last_queried_at`. It was once `s____null_last_queried_at`, which
        // the lookup could not find, so a NULL reaching a post-join predicate
        // was read as a present value holding its type's zeroes, and
        // `COALESCE(a, b)` returned those zeroes rather than falling through
        // to `b`.
        //
        // The type marker is the other half of the same fact and does not
        // depend on names, so honouring it covers the case the lookup cannot
        // reach. Both stay: the companion is what a scan of a real table
        // carries, and the marker is what a rebuilt row carries.
        Some(column) if column.column_type == DbType::NULL => Value::Null,
        // A shredded JSON column's cell after hydration: the schema still
        // says the `UUID` it is stored as, the cell holds the rebuilt
        // document's text. Read under the schema's type, `doc::text` of
        // `{"a":1}` came back `7b226122-3a31-7d`, a UUID's dashes over the
        // document's hex. Handed on as native JSONB, the document casts,
        // compares and takes `->>` as a stored one does.
        Some(column) if column.column_type == DbType::JSON && field.db_type != DbType::JSON => {
            match std::str::from_utf8(column.data.into_slice())
                .ok()
                .and_then(|text| crate::core::jsonb::encode_text(text).ok())
            {
                Some(bytes) => Value::Data { bytes, db_type: DbType::JSONB },
                None => Value::Data {
                    bytes: column.data.into_slice().to_vec(),
                    db_type: field.db_type.clone(),
                },
            }
        }
        // A legacy `CHAR` cell is text in four bytes
        // (`value_codec::legacy_char_text`), and an expression reads it as the
        // text: `c || '|'` of `'é'` is `'é|'`, not its four bytes.
        Some(column) if field.db_type == DbType::CHAR => Value::Data {
            bytes: crate::core::value_codec::legacy_char_text(column.data.into_slice()).as_bytes().to_vec(),
            db_type: DbType::STRING,
        },
        Some(column) => Value::Data {
            bytes: column.data.into_slice().to_vec(),
            db_type: field.db_type.clone(),
        },
        None => Value::Null,
    }
}

pub(crate) fn is_integer_type(t: &DbType) -> bool {
    matches!(
        t,
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
    )
}

/// A date/time field's canonical name, from any spelling PostgreSQL takes
/// for it (upper case): `YEARS`, `MILLISECOND`, … An unknown one is
/// PostgreSQL's 22023.
fn date_field(upper: &str) -> Result<&'static str, String> {
    Ok(match upper.trim() {
        "YEAR" | "YEARS" | "Y" | "YR" | "YRS" => "YEAR",
        "MONTH" | "MONTHS" | "MON" | "MONS" => "MONTH",
        "DAY" | "DAYS" | "D" => "DAY",
        "HOUR" | "HOURS" | "H" | "HR" | "HRS" => "HOUR",
        "MINUTE" | "MINUTES" | "M" | "MIN" | "MINS" => "MINUTE",
        "SECOND" | "SECONDS" | "S" | "SEC" | "SECS" => "SECOND",
        "MILLISECOND" | "MILLISECONDS" | "MS" | "MSEC" | "MSECS" | "MSECOND" | "MSECONDS" => "MILLISECONDS",
        "MICROSECOND" | "MICROSECONDS" | "US" | "USEC" | "USECS" | "USECOND" | "USECONDS" => "MICROSECONDS",
        "DOW" => "DOW",
        "ISODOW" => "ISODOW",
        "DOY" => "DOY",
        "QUARTER" => "QUARTER",
        "WEEK" => "WEEK",
        "ISOYEAR" => "ISOYEAR",
        "DECADE" | "DECADES" => "DECADE",
        "CENTURY" | "CENTURIES" => "CENTURY",
        "MILLENNIUM" | "MILLENNIA" | "MILLENNIUMS" => "MILLENNIUM",
        "EPOCH" => "EPOCH",
        other => {
            return Err(format!("unit \"{}\" not recognized for type timestamp with time zone", other.to_lowercase()))
        }
    })
}

/// Whether `field` exists for a value of `ty`: an interval has no weekday or
/// week, and `EXTRACT` refuses a time-of-day field of a date (PostgreSQL 14+;
/// `date_part` reads the date as its midnight).
fn date_field_applies(field: &str, ty: &DbType, extract: bool) -> Result<(), String> {
    let time_of_day = matches!(field, "HOUR" | "MINUTE" | "SECOND" | "MILLISECONDS" | "MICROSECONDS");
    match ty {
        DbType::INTERVAL if matches!(field, "DOW" | "ISODOW" | "DOY" | "WEEK" | "ISOYEAR") => {
            Err(format!("unit \"{}\" not supported for type interval", field.to_lowercase()))
        }
        DbType::DATE if extract && time_of_day => {
            Err(format!("unit \"{}\" not supported for type date", field.to_lowercase()))
        }
        DbType::DATE | DbType::DATETIME | DbType::TIMESTAMP | DbType::INTERVAL | DbType::NULL => Ok(()),
        other => Err(format!(
            "function {}({}) does not exist",
            if extract { "extract" } else { "date_part" },
            match other {
                DbType::STRING => "text".to_string(),
                t => t.to_string().to_lowercase(),
            }
        )),
    }
}

/// The canonical field an `ExtractField`/`DatePart` call holds.
fn field_literal(arg: &Expr) -> Option<&'static str> {
    match arg {
        Expr::Literal { bytes, db_type: DbType::STRING } => date_field(&String::from_utf8_lossy(bytes)).ok(),
        _ => None,
    }
}

/// The display scale of `EXTRACT(field …)`'s `numeric`, as PostgreSQL 14+
/// gives it.
fn field_scale(field: &str) -> u32 {
    match field {
        "SECOND" | "EPOCH" => 6,
        "MILLISECONDS" => 3,
        _ => 0,
    }
}

/// `field` of a timestamp, date (its midnight) or interval, exactly, at
/// [`field_scale`]'s places; `None` for NULL. Timestamps are read in UTC,
/// this engine's only time zone.
fn date_field_value(field: &str, value: &Value) -> Result<Option<crate::core::rql::pg_numeric::PgNum>, String> {
    use crate::core::rql::pg_numeric::PgNum;
    let Value::Data { bytes, db_type } = value else {
        return Ok(None);
    };
    let at = |m: i128| PgNum::new(m, field_scale(field)).ok_or_else(|| "a date field could not be read".to_string());
    if *db_type == DbType::INTERVAL {
        let span = crate::core::pg_types::decode_interval(bytes).ok_or("an interval could not be read")?;
        let us = span.micros as i128;
        let minute_us = 60_000_000i128;
        return Ok(Some(match field {
            "YEAR" => PgNum::int(span.months as i128 / 12),
            "MONTH" => PgNum::int(span.months as i128 % 12),
            "DAY" => PgNum::int(span.days as i128),
            "HOUR" => PgNum::int(us / 3_600_000_000),
            "MINUTE" => PgNum::int(us / minute_us % 60),
            "SECOND" | "MILLISECONDS" => at(us % minute_us)?,
            "MICROSECONDS" => PgNum::int(us % minute_us),
            "QUARTER" => PgNum::int((span.months as i128 % 12) / 3 + 1),
            "DECADE" => PgNum::int(span.months as i128 / 120),
            "CENTURY" => PgNum::int(span.months as i128 / 1200),
            "MILLENNIUM" => PgNum::int(span.months as i128 / 12000),
            // A year is 365.25 days (31 557 600 s) and the months left over
            // thirty each, as PostgreSQL's `interval_part` counts them: at
            // twelve thirty-day months, `'2 years 3 mons'` was ten and a half
            // days short.
            "EPOCH" => at(us
                + (span.months / 12) as i128 * 31_557_600_000_000
                + ((span.months % 12) as i128 * 30 + span.days as i128) * 86_400_000_000)?,
            other => return Err(format!("unit \"{}\" not supported for type interval", other.to_lowercase())),
        }));
    }
    let us = match db_type {
        DbType::DATE | DbType::DATETIME => instant_of(db_type, bytes),
        _ if bytes.len() == 8 => Some(i64::from_le_bytes(bytes.as_slice().try_into().expect("eight bytes"))),
        _ => None,
    }
    .ok_or("a timestamp could not be read")?;
    let days = us.div_euclid(DAY_US);
    let tod = us.rem_euclid(DAY_US) as i128;
    let (y, m, d) = crate::core::value_codec::civil_from_days(days);
    if y <= 0 && matches!(field, "YEAR" | "DECADE" | "CENTURY" | "MILLENNIUM" | "ISOYEAR") {
        return Err(format!("EXTRACT({field}) of a year before 1 AD is not supported yet"));
    }
    let dow = (days + 4).rem_euclid(7) as i128;
    let isodow = if dow == 0 { 7 } else { dow };
    // The ISO week is the one holding this week's Thursday.
    let thursday = days - (isodow as i64 - 1) + 3;
    let (iso_year, _, _) = crate::core::value_codec::civil_from_days(thursday);
    let minute_us = 60_000_000i128;
    Ok(Some(match field {
        "YEAR" => PgNum::int(y as i128),
        "MONTH" => PgNum::int(m as i128),
        "DAY" => PgNum::int(d as i128),
        "HOUR" => PgNum::int(tod / 3_600_000_000),
        "MINUTE" => PgNum::int(tod / minute_us % 60),
        "SECOND" | "MILLISECONDS" => at(tod % minute_us)?,
        "MICROSECONDS" => PgNum::int(tod % minute_us),
        "DOW" => PgNum::int(dow),
        "ISODOW" => PgNum::int(isodow),
        "DOY" => PgNum::int((days - crate::core::value_codec::days_from_civil(y, 1, 1) + 1) as i128),
        "QUARTER" => PgNum::int((m as i128 - 1) / 3 + 1),
        "WEEK" => PgNum::int(((thursday - crate::core::value_codec::days_from_civil(iso_year, 1, 1)) / 7 + 1) as i128),
        "ISOYEAR" => PgNum::int(iso_year as i128),
        "DECADE" => PgNum::int(y as i128 / 10),
        "CENTURY" => PgNum::int((y as i128 + 99) / 100),
        "MILLENNIUM" => PgNum::int((y as i128 + 999) / 1000),
        "EPOCH" if *db_type == DbType::DATE => PgNum::int((us / 1_000_000) as i128),
        "EPOCH" => at(us as i128)?,
        other => return Err(format!("unit \"{}\" not supported", other.to_lowercase())),
    }))
}

/// `round`'s constant places, as its parse left them; `None` for NULL.
fn round_places(places: &Expr) -> Option<i32> {
    match places {
        Expr::Literal { bytes, db_type: DbType::I32 } => Some(i32::from_le_bytes(bytes.as_slice().try_into().ok()?)),
        _ => None,
    }
}

/// A NUMERIC cell as an exact value: sixteen bytes at the engine's nine
/// places, or seventeen carrying their own.
pub(crate) fn decimal_value_as_pgnum(bytes: &[u8]) -> Result<crate::core::rql::pg_numeric::PgNum, String> {
    use crate::core::rql::pg_numeric::PgNum;
    let unreadable = || "a numeric could not be read".to_string();
    match crate::core::value_codec::split_decimal_cell(bytes).ok_or_else(unreadable)? {
        (m, Some(own)) => PgNum::new(m, own).ok_or_else(unreadable),
        (m, None) => PgNum::new(m, crate::core::value_codec::DECIMAL_SCALE).ok_or_else(unreadable),
    }
}

/// An exact value as a NUMERIC cell: at the engine's nine places when it is
/// exact there, otherwise in a cell carrying its own scale.
fn pgnum_as_decimal_value(v: crate::core::rql::pg_numeric::PgNum) -> Value {
    use crate::core::value_codec::DECIMAL_SCALE;
    let bytes = match (v.s <= DECIMAL_SCALE).then(|| v.units_at(DECIMAL_SCALE)).flatten() {
        Some(units) => units.to_le_bytes().to_vec(),
        None => v.to_cell(),
    };
    Value::Data { bytes, db_type: DbType::DECIMAL }
}

pub(crate) fn as_i64(value: &Value) -> Option<i64> {
    let Value::Data { bytes, db_type } = value else {
        return None;
    };
    Some(match db_type {
        DbType::I8 => i8::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::I16 => i16::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::I32 => i32::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::I64 => i64::from_le_bytes(bytes.as_slice().try_into().ok()?),
        DbType::U8 => u8::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::U16 => u16::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::U32 => u32::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        DbType::U64 => u64::from_le_bytes(bytes.as_slice().try_into().ok()?) as i64,
        // Microseconds since the epoch, which is what makes timestamp and
        // interval arithmetic the same arithmetic.
        DbType::DATETIME => i64::from_le_bytes(bytes.as_slice().try_into().ok()?),
        _ => return None,
    })
}

/// A number as the double PostgreSQL converts it to when it meets a float:
/// a `real` widened exactly, an integer or a NUMERIC rounded to the nearest
/// double (`numeric_float8` goes through the text, `strtod`).
fn nearest_double(t: &DbType, bytes: &[u8]) -> Option<f64> {
    Some(match t {
        DbType::F64 => f64::from_le_bytes(bytes.try_into().ok()?),
        DbType::F32 => f64::from(f32::from_le_bytes(bytes.try_into().ok()?)),
        DbType::I8 => i8::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::I16 => i16::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::I32 => i32::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::I64 => i64::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::I128 => i128::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::U8 => u8::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::U16 => u16::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::U32 => u32::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::U64 => u64::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::U128 => u128::from_le_bytes(bytes.try_into().ok()?) as f64,
        DbType::DECIMAL => {
            let (m, own) = crate::core::value_codec::split_decimal_cell(bytes)?;
            let scale = own.unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
            crate::core::value_codec::format_decimal_scaled(m, scale).parse::<f64>().ok()?
        }
        _ => return None,
    })
}

/// Orders two values for a SQL operator (`=`, `<`, CASE, NULLIF, GREATEST,
/// LEAST, IN): [`compare_total`], except that a number meeting a float of
/// another type is compared as PostgreSQL resolves it, both sides in double
/// precision (`numeric = float8`, `int8 = float8`, `int4 = real` are all
/// `float8` comparisons). That is the hash join's equality too
/// (`join::KeyNumber::Float`), so a joined pair never projects `n = f8` false.
/// Not transitive alongside exact comparisons (bigint 2^53 equals the double
/// 2^53, which equals bigint 2^53+1), as PostgreSQL's operators are not: a
/// sort orders by [`compare_total`].
pub(crate) fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if let (Value::Data { bytes: ab, db_type: at }, Value::Data { bytes: bb, db_type: bt }) = (a, b) {
        let float = |t: &DbType| matches!(t, DbType::F32 | DbType::F64);
        if at != bt && (float(at) || float(bt)) && !ab.is_empty() && !bb.is_empty() {
            if let (Some(x), Some(y)) = (nearest_double(at, ab), nearest_double(bt, bb)) {
                return Some(crate::core::processor::concurrent_processor::pg_float_cmp(x, y));
            }
        }
    }
    compare_total(a, b)
}

/// Orders two values, for sorting: a total order over every numeric type,
/// mixed widths and floats included, compared at their exact values.
pub(crate) fn compare_total(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    let (
        Value::Data {
            bytes: ab,
            db_type: at,
        },
        Value::Data { bytes: bb, .. },
    ) = (a, b)
    else {
        return None;
    };
    // A date against a date or a timestamp, as instants. A date's stored form
    // is PostgreSQL's big-endian day count, which byte order does not sort,
    // and against a timestamp it means midnight.
    if let Value::Data { db_type: bt, .. } = b {
        if *at == DbType::DATE || *bt == DbType::DATE {
            if let (Some(x), Some(y)) = (instant_of(at, ab), instant_of(bt, bb)) {
                return Some(x.cmp(&y));
            }
        }
    }
    // Two NUMERICs, one carrying its own scale: compared at their scales.
    if let Value::Data { db_type: DbType::DECIMAL, .. } = b {
        if *at == DbType::DECIMAL && (ab.len() == 17 || bb.len() == 17) {
            if let (Some(x), Some(y)) = (numeric_key(at, ab), numeric_key(&DbType::DECIMAL, bb)) {
                return Some(x.cmp_exact(&y));
            }
        }
    }
    // Two numeric types: widened to a common one rather than reading `b`'s
    // bytes as `a`'s type, which made `compare(a, b)` and `compare(b, a)`
    // disagree for mixed widths (`COALESCE(bigint, int)`, `LEAST`, `CASE`) —
    // an inconsistent comparator, which the standard sort may panic on.
    if let Value::Data { db_type: bt, .. } = b {
        if at != bt {
            if let (Some(x), Some(y)) = (numeric_key(at, ab), numeric_key(bt, bb)) {
                return Some(x.cmp_exact(&y));
            }
        }
    }
    if ab.is_empty() || bb.is_empty() {
        // An empty STRING is a real value — `''` equals `''` and precedes any
        // other text — while empty bytes of a fixed-width type mean "absent",
        // which cannot be ordered. Without this arm `NULLIF(x, '')` never
        // fired: the two empty strings compared as incomparable, so the empty
        // string leaked through to whatever consumed it.
        return if matches!(at, DbType::STRING) {
            Some(ab.as_slice().cmp(bb.as_slice()))
        } else {
            None
        };
    }
    Some(crate::core::processor::concurrent_processor::compare_bytes_as_type(at.clone(), ab, bb))
}

/// A numeric value in a form any two can be compared in exactly: an integer
/// or engine-scale `NUMERIC` as sign, magnitude and decimal scale, or a float.
///
/// PostgreSQL resolves mixed operands to one type first — int2/int4/int8 to
/// int8, an integer against `NUMERIC` to `NUMERIC`, anything against a float to
/// float. Comparing exactly gives those answers wherever the conversion is
/// exact, and stays a total order where it is not (an `int8` above 2^53 against
/// a `float8`, which a lossy cast would call equal to its neighbour and so
/// break transitivity).
#[derive(Debug, Clone, Copy)]
enum NumKey {
    Exact { neg: bool, mag: u128, scale: u32 },
    Float(f64),
}

fn numeric_key(db_type: &DbType, bytes: &[u8]) -> Option<NumKey> {
    fn signed(v: i128, scale: u32) -> NumKey {
        NumKey::Exact { neg: v < 0, mag: v.unsigned_abs(), scale }
    }
    let b = bytes;
    Some(match db_type {
        DbType::I8 => signed(i8::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::I16 => signed(i16::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::I32 => signed(i32::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::I64 => signed(i64::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::I128 => signed(i128::from_le_bytes(b.try_into().ok()?), 0),
        DbType::U8 => signed(u8::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::U16 => signed(u16::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::U32 => signed(u32::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::U64 => signed(u64::from_le_bytes(b.try_into().ok()?) as i128, 0),
        DbType::U128 => NumKey::Exact {
            neg: false,
            mag: u128::from_le_bytes(b.try_into().ok()?),
            scale: 0,
        },
        // A cell carrying its own scale (a NUMERIC with more places than
        // the engine's, `eval_output`) at that scale.
        DbType::DECIMAL => {
            let (v, own) = crate::core::value_codec::split_decimal_cell(b)?;
            signed(v, own.unwrap_or(crate::core::value_codec::DECIMAL_SCALE))
        }
        DbType::F32 => NumKey::Float(f32::from_le_bytes(b.try_into().ok()?) as f64),
        DbType::F64 => NumKey::Float(f64::from_le_bytes(b.try_into().ok()?)),
        _ => return None,
    })
}

impl NumKey {
    fn cmp_exact(&self, other: &NumKey) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (*self, *other) {
            (NumKey::Float(x), NumKey::Float(y)) => {
                crate::core::processor::concurrent_processor::pg_float_cmp(x, y)
            }
            (NumKey::Float(x), e @ NumKey::Exact { .. }) => float_vs_exact(x, &e),
            (e @ NumKey::Exact { .. }, NumKey::Float(y)) => float_vs_exact(y, &e).reverse(),
            (
                NumKey::Exact { neg: an, mag: am, scale: asc },
                NumKey::Exact { neg: bn, mag: bm, scale: bsc },
            ) => {
                let (an, bn) = (an && am != 0, bn && bm != 0);
                if an != bn {
                    return if an { Ordering::Less } else { Ordering::Greater };
                }
                // Same sign: compare magnitudes at the larger scale. A
                // magnitude that overflows when rescaled is the larger one.
                let up = |m: u128, by: u32| 10u128.checked_pow(by).and_then(|f| m.checked_mul(f));
                let mags = if asc >= bsc {
                    up(bm, asc - bsc).map(|b| am.cmp(&b)).unwrap_or(Ordering::Less)
                } else {
                    up(am, bsc - asc).map(|a| a.cmp(&bm)).unwrap_or(Ordering::Greater)
                };
                if an { mags.reverse() } else { mags }
            }
        }
    }
}

/// `x` against an exact value, exactly. NaN sorts above everything, as in
/// `pg_float_cmp`; `-0.0` equals zero.
fn float_vs_exact(x: f64, e: &NumKey) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let NumKey::Exact { neg, mag, scale } = *e else {
        unreachable!("called with an exact value")
    };
    let eneg = neg && mag != 0;
    if x.is_nan() {
        return Ordering::Greater;
    }
    if x.is_infinite() {
        return if x > 0.0 { Ordering::Greater } else { Ordering::Less };
    }
    let xneg = x < 0.0;
    if x == 0.0 || mag == 0 || xneg != eneg {
        // Different signs, or either is zero: the signs alone decide.
        let sx = if x == 0.0 { 0 } else if xneg { -1 } else { 1 };
        let se = if mag == 0 { 0 } else if eneg { -1 } else { 1 };
        return sx.cmp(&se);
    }
    // Same sign, both non-zero: compare |x| = m * 2^exp with mag / 10^scale,
    // i.e. m * 10^scale * 2^exp against mag.
    let bits = x.abs().to_bits();
    let raw_exp = ((bits >> 52) & 0x7ff) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (m, exp) = if raw_exp == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), raw_exp - 1075)
    };
    let bitlen = |v: u128| 128 - v.leading_zeros() as i32;
    let ord = match 10u128.checked_pow(scale).and_then(|f| (m as u128).checked_mul(f)) {
        None => Ordering::Greater,
        Some(a) => {
            if exp >= 0 {
                // a * 2^exp against mag.
                let lo = bitlen(a) - 1 + exp; // a * 2^exp >= 2^lo
                if lo >= bitlen(mag) {
                    Ordering::Greater
                } else {
                    (a << exp).cmp(&mag)
                }
            } else {
                // a against mag * 2^k.
                let k = -exp;
                let lo = bitlen(mag) - 1 + k; // mag * 2^k >= 2^lo
                if lo >= bitlen(a) {
                    Ordering::Less
                } else {
                    a.cmp(&(mag << k))
                }
            }
        }
    };
    if xneg { ord.reverse() } else { ord }
}

/// The value `steps` lead to in `document`: a stored `jsonb` document's
/// bytes, or a `json` value's text when `json_text`. `->>` (`as_text`) gives
/// text, `->` a document (`json`'s as the member's own text).
fn path_value(document: &[u8], steps: &[crate::core::jsonb::Step], as_text: bool, json_text: bool) -> Result<Value, String> {
    // A `json` value: the text as stored, read where the path leads
    // (`json_text`): `->` is the member's own text, `->>` a string
    // unescaped.
    if json_text {
        let text = String::from_utf8_lossy(document);
        let Some(span) = crate::core::json_text::get_path(&text, steps)? else {
            return Ok(Value::Null);
        };
        let out = if as_text {
            crate::core::json_text::as_text(&text, span)?
        } else {
            Some(text[span].to_string())
        };
        return Ok(match out {
            Some(t) => Value::Data { bytes: t.into_bytes(), db_type: DbType::STRING },
            None => Value::Null,
        });
    }
    // A path that resolves to nothing is NULL, and so is a JSON null read as
    // text — both are PostgreSQL's answers, and neither is an error: asking
    // for a key a document does not have is ordinary.
    let Some(found) = crate::core::jsonb::get_path(document, steps) else {
        return Ok(Value::Null);
    };
    Ok(if as_text {
        match crate::core::jsonb::as_text(found) {
            Some(text) => Value::Data { bytes: text.into_bytes(), db_type: DbType::STRING },
            None => Value::Null,
        }
    } else {
        // `->` yields a document, so the bytes stay encoded and the value can
        // be reached into again or rendered as JSON.
        Value::Data { bytes: found.to_vec(), db_type: DbType::JSONB }
    })
}

/// Evaluates `expr` for one row.
pub fn eval(expr: &Expr, row: &Row, schema: &[SchemaField]) -> Result<Value, String> {
    Ok(match expr {
        Expr::Null => Value::Null,
        Expr::Literal { bytes, db_type } => Value::Data {
            bytes: bytes.clone(),
            db_type: db_type.clone(),
        },
        Expr::Column(field) => to_engine_scale(column_value(field, row, schema), field)?,
        Expr::JsonPath {
            column,
            steps,
            as_text,
        } => {
            let document = match column_value(column, row, schema) {
                Value::Data { bytes, .. } => bytes,
                Value::Null => return Ok(Value::Null),
            };
            path_value(&document, steps, *as_text, column.json_text)?
        }
        Expr::PathOf { base, steps, as_text } => {
            let json_text = yields_json_text(base);
            let document = match eval(base, row, schema)? {
                Value::Null => return Ok(Value::Null),
                // A `json` value is the text it is.
                Value::Data { bytes, .. } if json_text => bytes,
                // A `jsonb` operand may still arrive as text: a branch of
                // `coalesce(doc, '{"k":1}')` is the literal's. Walked as an
                // encoded document, the text found nothing and read NULL.
                value => as_jsonb(value)?,
            };
            path_value(&document, steps, *as_text, json_text)?
        }
        Expr::Concat(lhs, rhs) => {
            // A NUMERIC operand is its text at its display scale, as `::text`
            // gives it (`pg_numeric`): `'x' || (2.50 * 2)` is 'x5.00', and
            // `'x' || a` over a NUMERIC(10,2) 2.50 'x2.50', where the
            // engine-scale value's own text gave 'x5' and 'x2.5'.
            let operand = |e: &Expr| -> Result<Value, String> {
                if static_type(e) == DbType::DECIMAL {
                    if let Some(v) = pg_numeric(e, row, schema)? {
                        return Ok(match v {
                            None => Value::Null,
                            Some(v) => Value::Data { bytes: v.to_text().into_bytes(), db_type: DbType::STRING },
                        });
                    }
                }
                eval(e, row, schema)
            };
            let a = operand(lhs)?;
            let b = operand(rhs)?;
            // Strict in NULL, as PostgreSQL is: a NULL operand makes the whole
            // expression NULL rather than silently vanishing from the result.
            if a.is_null() || b.is_null() {
                Value::Null
            } else if concat_is_jsonb(lhs, rhs) {
                Value::Data {
                    bytes: crate::core::jsonb::concat(&as_jsonb(a)?, &as_jsonb(b)?)?,
                    db_type: DbType::JSONB,
                }
            } else {
                // A timestamp's text has the `+00` its `cast` to text has,
                // except a `timestamp` (without time zone) one's
                // (`yields_timestamp`).
                // A boolean is its cast to text, 'true' or 'false', as
                // PostgreSQL's `anynonarray || text` casts it (`concat()`, by
                // contrast, uses the output function: 't'). Through `as_text`
                // it was the stored byte, so `'x' || f` read back as 'x'.
                let text_of = |e: &Expr, v: &Value| {
                    if let Value::Data { bytes, db_type: DbType::BOOL } = v {
                        return bool_text(bytes).to_string();
                    }
                    let mut text = as_text(v);
                    if matches!(v, Value::Data { db_type: DbType::DATETIME, .. }) && !yields_timestamp(e) {
                        text.push_str("+00");
                    }
                    text
                };
                let mut text = text_of(lhs, &a);
                text.push_str(&text_of(rhs, &b));
                Value::Data {
                    bytes: text.into_bytes(),
                    db_type: DbType::STRING,
                }
            }
        }
        Expr::Call(func, args) => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(eval(arg, row, schema)?);
            }
            // `octet_length` counts a `character(n)` value's padding, as
            // PostgreSQL's does; every other function reads it as text,
            // without (`length` included).
            if *func == TextFunc::OctetLength {
                if let (Some(n), Some(Value::Data { bytes, .. })) =
                    (args.first().and_then(bpchar_width), values.first_mut())
                {
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        *bytes = crate::core::rql::type_modifier::pad_to(text, n).into_owned().into_bytes();
                    }
                }
            }
            // Each NUMERIC argument at the places its expression shows.
            if let TextFunc::Pg(_) = func {
                for (value, arg) in values.iter_mut().zip(args) {
                    if let Some(scale) = display_scale(arg) {
                        *value = crate::core::rql::pg_functions::shown_at(std::mem::replace(value, Value::Null), scale);
                    }
                    // A `json` argument is a document to a JSON builder, not
                    // a string: its text, marked so for this call only (it
                    // travels as text everywhere else, `yields_json_text`).
                    if yields_json_text(arg) {
                        if let Value::Data { db_type, .. } = value {
                            if *db_type == DbType::STRING {
                                *db_type = DbType::JSONTEXT;
                            }
                        }
                    }
                }
            }
            call(*func, &values, row)?
        }
        Expr::Arith(lhs, op, rhs) => {
            let a = eval(lhs, row, schema)?;
            let b = eval(rhs, row, schema)?;
            // Arithmetic is strict in NULL: any NULL operand makes the whole
            // expression NULL, which is what `COALESCE(MAX(x), 0) + 1` relies on
            // to reach the COALESCE rather than adding to a zero that was never
            // there.
            if a.is_null() || b.is_null() {
                Value::Null
            } else {
                let got = arith(&a, *op, &b)?;
                numeric_literal_arith(lhs, &a, *op, rhs, &b, got)?
            }
        }
        Expr::Scalar(func) => {
            let (_, db_type) = crate::core::rql::aggregate::ScalarFunc::parse(match func {
                crate::core::rql::aggregate::ScalarFunc::Now => "now",
                crate::core::rql::aggregate::ScalarFunc::StatementTimestamp => "statement_timestamp",
                crate::core::rql::aggregate::ScalarFunc::ClockTimestamp => "clock_timestamp",
                crate::core::rql::aggregate::ScalarFunc::CurrentDate => "current_date",
                crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => "gen_random_uuid",
                crate::core::rql::aggregate::ScalarFunc::UuidV7 => "uuidv7",
                crate::core::rql::aggregate::ScalarFunc::Version => "version",
            })
            .expect("known function");
            Value::Data {
                bytes: func.evaluate(),
                db_type,
            }
        }
        Expr::Coalesce(args) => {
            for arg in args {
                let value = eval(arg, row, schema)?;
                if !value.is_null() {
                    return to_common_type(expr, value);
                }
            }
            Value::Null
        }
        Expr::NullIf(a, b) => {
            let left = eval(a, row, schema)?;
            let right = eval(b, row, schema)?;
            if compare(&left, &right) == Some(std::cmp::Ordering::Equal) {
                Value::Null
            } else {
                to_common_type(expr, left)?
            }
        }
        Expr::Least(args) | Expr::Greatest(args) => {
            let want_least = matches!(expr, Expr::Least(_));
            let mut best: Option<Value> = None;
            for arg in args {
                let value = eval(arg, row, schema)?;
                // PostgreSQL ignores NULLs in LEAST/GREATEST.
                if value.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => value,
                    Some(current) => match compare(&value, &current) {
                        Some(std::cmp::Ordering::Less) if want_least => value,
                        Some(std::cmp::Ordering::Greater) if !want_least => value,
                        _ => current,
                    },
                });
            }
            to_common_type(expr, best.unwrap_or(Value::Null))?
        }
        Expr::Mod(a, b) => {
            let left = eval(a, row, schema)?;
            let right = eval(b, row, schema)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            let (Some(l), Some(r)) = (as_i64(&left), as_i64(&right)) else {
                return Err("MOD needs integer arguments".to_string());
            };
            if r == 0 {
                return Err("division by zero".to_string());
            }
            Value::Data {
                bytes: (l % r).to_le_bytes().to_vec(),
                db_type: DbType::I64,
            }
        }
        Expr::Case { whens, otherwise } => {
            for (condition, result) in whens {
                if eval_cond(condition, row, schema)? {
                    return to_common_type(expr, eval(result, row, schema)?);
                }
            }
            match otherwise {
                Some(e) => return to_common_type(expr, eval(e, row, schema)?),
                None => Value::Null,
            }
        }
        // A `timestamp` (without time zone) value cast to text is its
        // wall-clock time with no offset (`yields_timestamp`); a
        // `timestamptz` value's has `+00` (`cast`).
        Expr::Cast(inner, DbType::STRING) if yields_timestamp(inner) => {
            local_timestamp_text(eval(inner, row, schema)?)?
        }
        // A NUMERIC(p, s) column cast to text is rendered from its stored
        // integer at its own scale. Through the engine scale it lost every
        // place past nine (a NUMERIC(20,12) 1.000000000001 printed
        // 1.000000000000) and could not lift a value past ~1.7 × 10^29 at
        // all (a NUMERIC(38,0) failed "out of range for arithmetic").
        Expr::Cast(inner, DbType::STRING)
            if matches!(inner.as_ref(), Expr::Column(f) if f.db_type == DbType::DECIMAL && f.scale.is_some()) =>
        {
            let Expr::Column(field) = inner.as_ref() else { unreachable!("matched above") };
            let scale = field.scale.expect("matched above");
            match column_value(field, row, schema) {
                Value::Data { bytes, db_type: DbType::DECIMAL } if bytes.len() == 16 => Value::Data {
                    bytes: crate::core::value_codec::format_decimal_scaled(
                        i128::from_le_bytes(bytes.as_slice().try_into().expect("sixteen bytes")),
                        scale,
                    )
                    .into_bytes(),
                    db_type: DbType::STRING,
                },
                other => cast(to_engine_scale(other, field)?, &DbType::STRING)?,
            }
        }
        // Over a NUMERIC with more places than the engine's: a float is the
        // nearest to the exact value and text is the exact value (both were
        // refused for the places past nine).
        Expr::Cast(inner, target @ (DbType::F64 | DbType::F32 | DbType::STRING))
            if wide_exact(inner, row, schema)?.is_some() =>
        {
            match wide_exact(inner, row, schema)?.flatten() {
                None => Value::Null,
                Some(x) if *target == DbType::STRING => {
                    let text = if x.e <= 0 {
                        crate::core::value_codec::format_decimal_scaled(x.m, x.e.unsigned_abs())
                    } else {
                        format!("{}{}", x.m, "0".repeat(x.e as usize))
                    };
                    Value::Data { bytes: text.into_bytes(), db_type: DbType::STRING }
                }
                Some(x) => {
                    let f: f64 = format!("{}e{}", x.m, x.e).parse().map_err(|_| "value out of range".to_string())?;
                    if *target == DbType::F32 {
                        Value::Data { bytes: (f as f32).to_le_bytes().to_vec(), db_type: DbType::F32 }
                    } else {
                        Value::Data { bytes: f.to_le_bytes().to_vec(), db_type: DbType::F64 }
                    }
                }
            }
        }
        Expr::Cast(inner, target) => {
            // A NUMERIC's text is at the display scale PostgreSQL gives it:
            // `(1.0 / 3)::text` has twenty places, `(2.50 * 2)::text` is
            // '5.00' (`pg_numeric`). Through the engine both were its
            // nine-place value with the zeros trimmed.
            if *target == DbType::STRING && static_type(inner) == DbType::DECIMAL {
                if let Some(value) = pg_numeric(inner, row, schema)? {
                    return Ok(match value {
                        None => Value::Null,
                        Some(v) => Value::Data { bytes: v.to_text().into_bytes(), db_type: DbType::STRING },
                    });
                }
            }
            let value = eval(inner, row, schema)?;
            // A NUMERIC rendered as text keeps the places PostgreSQL displays
            // it with: a NUMERIC(5,2) 2.50 is '2.50'. The value is at the
            // engine scale here, and the plain rendering trims its zeros, so
            // `discount::text = '2.50'` matched nothing and the select list
            // sent '2.5'.
            match (target, display_scale(inner), &value) {
                (DbType::STRING, Some(scale), Value::Data { bytes, db_type: DbType::DECIMAL })
                    if bytes.len() == 16 =>
                {
                    let raw = i128::from_le_bytes(bytes.as_slice().try_into().expect("sixteen bytes"));
                    let shown = round_decimal(raw, crate::core::value_codec::DECIMAL_SCALE, scale)
                        .ok_or_else(|| "value overflows numeric format".to_string())?;
                    Value::Data {
                        bytes: crate::core::value_codec::format_decimal_scaled(shown, scale).into_bytes(),
                        db_type: DbType::STRING,
                    }
                }
                _ => cast(value, target)?,
            }
        }
        Expr::TypmodCast { value, typmod } => apply_cast_typmod(eval(value, row, schema)?, *typmod)?,
        Expr::NumericCast {
            value,
            precision,
            scale,
        } => {
            use crate::core::value_codec::DECIMAL_SCALE;
            // Over a NUMERIC with more places than the engine's, rounded from
            // the exact value (it was refused for the places past nine).
            if *scale <= DECIMAL_SCALE {
                if let Some(exact) = wide_exact(value, row, schema)? {
                    let Some(exact) = exact else { return Ok(Value::Null) };
                    let rounded = exact
                        .settle(*scale)
                        .ok_or_else(|| "value overflows numeric format".to_string())?;
                    crate::core::value_codec::check_numeric_precision(rounded, *precision, *scale)?;
                    let lifted = rescale_decimal(rounded, *scale, DECIMAL_SCALE)
                        .ok_or_else(|| "value overflows numeric format".to_string())?;
                    return Ok(Value::Data { bytes: lifted.to_le_bytes().to_vec(), db_type: DbType::DECIMAL });
                }
            }
            let Value::Data { bytes, .. } = eval(value, row, schema)? else {
                return Ok(Value::Null);
            };
            // A cell that keeps its own scale (an unconstrained column's
            // seventeen bytes): rounded from its own digits. Read as sixteen
            // bytes it failed `WHERE u::numeric(4,2) = 1.01` with "could not
            // be evaluated".
            if bytes.len() == 17 {
                let (m, own) = crate::core::value_codec::split_decimal_cell(&bytes)
                    .ok_or_else(|| "a numeric could not be read".to_string())?;
                let v = crate::core::rql::pg_numeric::PgNum::new(m, own.unwrap_or(DECIMAL_SCALE))
                    .ok_or_else(|| "a numeric could not be read".to_string())?;
                let rounded = v.round_to(*scale).ok_or_else(|| "value overflows numeric format".to_string())?;
                crate::core::value_codec::check_numeric_precision(rounded.m, *precision, *scale)?;
                if *scale > DECIMAL_SCALE {
                    return Ok(Value::Data { bytes: rounded.to_cell(), db_type: DbType::DECIMAL });
                }
                let lifted = rescale_decimal(rounded.m, *scale, DECIMAL_SCALE)
                    .ok_or_else(|| "value overflows numeric format".to_string())?;
                return Ok(Value::Data { bytes: lifted.to_le_bytes().to_vec(), db_type: DbType::DECIMAL });
            }
            let raw = i128::from_le_bytes(
                bytes.as_slice().try_into().map_err(|_| "a numeric could not be read".to_string())?,
            );
            // Rounded half away from zero and checked at the target scale, as
            // PostgreSQL's cast does, then carried on at the engine scale like
            // every other NUMERIC value an expression produces.
            let rounded = round_decimal(raw, DECIMAL_SCALE, *scale)
                .ok_or_else(|| "value overflows numeric format".to_string())?;
            crate::core::value_codec::check_numeric_precision(rounded, *precision, *scale)?;
            let lifted = rescale_decimal(rounded, *scale, DECIMAL_SCALE)
                .ok_or_else(|| "value overflows numeric format".to_string())?;
            Value::Data {
                bytes: lifted.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            }
        }
    })
}

/// The type PostgreSQL's `select_common_type` gives the branches of a `CASE`,
/// `COALESCE`, `GREATEST`/`LEAST` (and the argument of `NULLIF`), and whether
/// every branch is a number — the case where values are converted to it.
///
/// * A bare `NULL` takes no part.
/// * An integer literal is `int4` when it fits and `int8` when it does not; a
///   literal with a fraction is `numeric`. (Every integer literal parses as
///   `I64` here, so `CASE … THEN 1` used to come out `int8`, and a client
///   that asked for `i32` refused it.)
/// * A quoted literal is of unknown type and adopts the others'; only when
///   every branch is one is the result `text`.
/// * Numbers widen: `int2 < int4 < int8 < numeric < float4 < float8`.
/// * Any other mix keeps the first typed branch, as this did before —
///   PostgreSQL would refuse it, and refusing is left to a later change rather
///   than introduced silently here.
pub fn common_type<'a>(branches: impl Iterator<Item = &'a Expr>) -> (DbType, bool) {
    fn rank(t: &DbType) -> Option<u8> {
        Some(match t {
            DbType::I8 | DbType::U8 | DbType::I16 => 1,
            DbType::U16 | DbType::I32 => 2,
            DbType::U32 | DbType::I64 => 3,
            DbType::U64 | DbType::I128 | DbType::U128 | DbType::DECIMAL => 4,
            DbType::F32 => 5,
            DbType::F64 => 6,
            _ => return None,
        })
    }
    let mut known: Vec<DbType> = Vec::new();
    for e in branches {
        match e {
            Expr::Null => {}
            Expr::Literal {
                db_type: DbType::STRING,
                ..
            } => {}
            Expr::Literal {
                db_type: DbType::I64,
                bytes,
            } => {
                let v = <[u8; 8]>::try_from(bytes.as_slice())
                    .map(i64::from_le_bytes)
                    .unwrap_or(i64::MAX);
                known.push(if i32::try_from(v).is_ok() {
                    DbType::I32
                } else {
                    DbType::I64
                });
            }
            Expr::Literal {
                db_type: DbType::F64,
                ..
            } => known.push(DbType::DECIMAL),
            other => known.push(static_type(other)),
        }
    }
    let Some(first) = known.first().cloned() else {
        return (DbType::STRING, false);
    };
    if known.iter().all(|t| rank(t).is_some()) {
        let widest = known
            .iter()
            .max_by_key(|t| rank(t).expect("checked"))
            .cloned()
            .expect("non-empty");
        return (widest, true);
    }
    if known.iter().all(|t| *t == first) {
        return (first, false);
    }
    (first, false)
}

/// Refuses branches PostgreSQL's `select_common_type` cannot reconcile —
/// `CASE types integer and text cannot be matched` (SQLSTATE 42804).
///
/// Branches must share a type category: every number is one category (and
/// widens, see [`common_type`]), text is one, and every other type is its own
/// (dates and timestamps share one). An untyped quoted literal and a bare
/// `NULL` take no part — they adopt the other branches' type — so
/// `CASE … THEN 1 ELSE '2' END` stays valid, as does `THEN 'a' ELSE 1::text`.
/// Mixing categories used to resolve to whichever branch came first and send
/// the others' bytes under that type.
pub fn check_branch_types<'a>(
    construct: &str,
    branches: impl Iterator<Item = &'a Expr>,
) -> Result<(), String> {
    fn category(t: &DbType) -> DbType {
        match t {
            DbType::I8
            | DbType::U8
            | DbType::I16
            | DbType::U16
            | DbType::I32
            | DbType::U32
            | DbType::I64
            | DbType::U64
            | DbType::I128
            | DbType::U128
            | DbType::DECIMAL
            | DbType::F32
            | DbType::F64 => DbType::DECIMAL,
            DbType::STRING | DbType::CHAR => DbType::STRING,
            DbType::DATE | DbType::DATETIME => DbType::DATETIME,
            other => other.clone(),
        }
    }
    let mut first: Option<DbType> = None;
    for e in branches {
        let ty = match e {
            Expr::Null
            | Expr::Literal {
                db_type: DbType::STRING,
                ..
            } => continue,
            other => static_type(other),
        };
        match &first {
            None => first = Some(ty),
            Some(f) if category(f) == category(&ty) => {}
            Some(f) => {
                return Err(format!(
                    "{} types {} and {} cannot be matched",
                    construct,
                    crate::core::value_codec::pg_input_type_name(f),
                    crate::core::value_codec::pg_input_type_name(&ty)
                ));
            }
        }
    }
    Ok(())
}

/// A branch's value converted to the type the whole expression resolved to,
/// so the payload is what the row description announced. Only for an
/// all-number resolution, where the conversion is a widening (or a literal
/// read as a number); a value that cannot be converted fails the statement
/// rather than going out under the wrong type.
fn to_common_type(expr: &Expr, value: Value) -> Result<Value, String> {
    if value.is_null() {
        return Ok(value);
    }
    let (target, numeric) = match expr {
        Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => common_type(args.iter()),
        Expr::NullIf(lhs, _) => common_type(std::iter::once(&**lhs)),
        Expr::Case { whens, otherwise } => common_type(
            whens
                .iter()
                .map(|(_, e)| e)
                .chain(otherwise.iter().map(|b| b.as_ref())),
        ),
        _ => return Ok(value),
    };
    if !numeric || value.db_type() == Some(&target) {
        return Ok(value);
    }
    let from = value.db_type().cloned();
    match cast(value, &target)? {
        converted @ Value::Data { .. } if converted.db_type() == Some(&target) => Ok(converted),
        _ => Err(format!(
            "a {} branch cannot be read as the {} the expression resolves to",
            from.map(|t| t.to_string()).unwrap_or_default(),
            target
        )),
    }
}

/// Evaluates one comparison against a row.
pub fn eval_condition(
    condition: &Condition,
    row: &Row,
    schema: &[SchemaField],
) -> Result<bool, String> {
    // A comparison with NULL is unknown, which is not true.
    Ok(eval_condition_3vl(condition, row, schema)? == Some(true))
}

/// [`eval_condition`] with SQL's three values: `None` is UNKNOWN, what a
/// comparison with NULL gives. A null test or a truth test is never UNKNOWN.
pub fn eval_condition_3vl(
    condition: &Condition,
    row: &Row,
    schema: &[SchemaField],
) -> Result<Option<bool>, String> {
    // A NUMERIC with more places than the engine's, compared exactly: read
    // through the engine it was refused for the places past nine, so `CASE
    // WHEN a > 1` failed on a NUMERIC(20,12) value with digits there.
    if let (Some(right), true) = (&condition.right, matches!(condition.cmp, Cmp::Eq | Cmp::Ne | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge)) {
        if *condition.wide.0.get_or_init(|| {
            reads_wide_decimal(&condition.left, schema)
                || reads_wide_decimal(right, schema)
                || numeric_product_or_quotient(&condition.left)
                || numeric_product_or_quotient(right)
        }) {
            // A wide NUMERIC against a float, as PostgreSQL compares
            // `numeric` with `float8`: the NUMERIC's nearest double, from its
            // exact digits. Read through the engine it was refused for the
            // places past nine (`p < f8` over a NUMERIC(20,18)).
            let float_side = |e: &Expr| matches!(static_type(e), DbType::F64 | DbType::F32);
            if float_side(&condition.left) != float_side(right) {
                let (exact, float, flipped) =
                    if float_side(right) { (&*condition.left, &**right, false) } else { (&**right, &*condition.left, true) };
                if let Some(n) = pg_numeric(exact, row, schema)? {
                    let Some(n) = n else { return Ok(None) };
                    let f = eval(float, row, schema)?;
                    if f.is_null() {
                        return Ok(None);
                    }
                    let Some(f) = as_f64(&f) else {
                        return Err("a float could not be read".to_string());
                    };
                    let d: f64 = n.to_text().parse().map_err(|_| "a numeric could not be read".to_string())?;
                    let ordering = crate::core::processor::concurrent_processor::pg_float_cmp(d, f);
                    let ordering = if flipped { ordering.reverse() } else { ordering };
                    return Ok(Some(match condition.cmp {
                        Cmp::Eq => ordering.is_eq(),
                        Cmp::Ne => !ordering.is_eq(),
                        Cmp::Lt => ordering.is_lt(),
                        Cmp::Le => ordering.is_le(),
                        Cmp::Gt => ordering.is_gt(),
                        _ => ordering.is_ge(),
                    }));
                }
            }
            // PostgreSQL's values, exactly: through the engine a product or
            // quotient was cut to nine places, so `CASE WHEN x * y > 0` was
            // false for 0.00001 * 0.00001.
            if let (Some(a), Some(b)) = (pg_numeric(&condition.left, row, schema)?, pg_numeric(right, row, schema)?) {
                let (Some(a), Some(b)) = (a, b) else { return Ok(None) };
                let ordering = a.cmp_value(b);
                return Ok(Some(match condition.cmp {
                    Cmp::Eq => ordering.is_eq(),
                    Cmp::Ne => !ordering.is_eq(),
                    Cmp::Lt => ordering.is_lt(),
                    Cmp::Le => ordering.is_le(),
                    Cmp::Gt => ordering.is_gt(),
                    _ => ordering.is_ge(),
                }));
            }
            if let (Some(a), Some(b)) =
                (exact_numeric_or_engine(&condition.left, row, schema, 30)?, exact_numeric_or_engine(right, row, schema, 30)?)
            {
                let (Some(a), Some(b)) = (a, b) else { return Ok(None) };
                if let Some((x, y, e)) = ExactNumber::aligned(a, b) {
                    // A float-carried literal is within its bound of the
                    // number written; the order is certain when the two are
                    // further apart than both bounds.
                    let bound = a.err + b.err;
                    let apart = x.checked_sub(y).map_or(0.0, |d| (d as f64).abs() * 10f64.powi(e));
                    if bound == 0.0 || apart > bound * 1.000001 {
                        let ordering = x.cmp(&y);
                        return Ok(Some(match condition.cmp {
                            Cmp::Eq => ordering.is_eq(),
                            Cmp::Ne => !ordering.is_eq(),
                            Cmp::Lt => ordering.is_lt(),
                            Cmp::Le => ordering.is_le(),
                            Cmp::Gt => ordering.is_gt(),
                            _ => ordering.is_ge(),
                        }));
                    }
                }
            }
        }
    }
    // `IS [NOT] NULL` needs no value: a NUMERIC with places past the
    // engine's, which the evaluator refuses to carry, is a value, not NULL
    // (`p < f8` in a select list is answered as a CASE that asks it first).
    let left = match eval(&condition.left, row, schema) {
        Err(e) if matches!(condition.cmp, Cmp::IsNull | Cmp::IsNotNull) && e.contains("cannot carry exactly") => {
            return Ok(Some(condition.cmp == Cmp::IsNotNull));
        }
        other => other?,
    };
    match condition.cmp {
        Cmp::IsNull => return Ok(Some(left.is_null())),
        Cmp::IsNotNull => return Ok(Some(!left.is_null())),
        Cmp::IsTrue | Cmp::IsNotTrue | Cmp::IsFalse | Cmp::IsNotFalse => {
            // Three-valued: `None` is NULL, neither true nor false.
            let truth = match &left {
                Value::Null => None,
                Value::Data {
                    bytes,
                    db_type: DbType::BOOL,
                } => Some(bytes.first().copied().unwrap_or(0) != 0),
                Value::Data { db_type, .. } => {
                    return Err(format!(
                        "argument of IS TRUE/FALSE must be type boolean, not {}",
                        db_type
                    ));
                }
            };
            return Ok(Some(match condition.cmp {
                Cmp::IsTrue => truth == Some(true),
                Cmp::IsNotTrue => truth != Some(true),
                Cmp::IsFalse => truth == Some(false),
                _ => truth != Some(false),
            }));
        }
        _ => {}
    }
    let right = match &condition.right {
        Some(e) => eval(e, row, schema)?,
        None => return Err("comparison has no right-hand side".to_string()),
    };
    // A comparison with NULL is unknown.
    if left.is_null() || right.is_null() {
        return Ok(None);
    }
    // Read both sides as the same type before ordering them.
    //
    // `compare` reads the right-hand bytes *as the left-hand type*, which is
    // right when both sides are the same type and silently wrong when they are
    // not. A literal written in SQL is text: `COALESCE(a.x, b.y) > '2024-01-15
    // 00:00:00'` puts nineteen ASCII bytes against a timestamp, and reading
    // those as an eight-byte timestamp compares two unrelated numbers. It does
    // not error — it produces an ordering nobody asked for, and in practice
    // every row failed the test. A join whose rows all matched returned the
    // empty set, with nothing anywhere reporting why.
    //
    // Only the text side is converted, and only when the two disagree: the
    // column's type is what the comparison should be carried out in, exactly
    // as a single-table predicate does through the tokenizer. A literal that
    // cannot be read as the column's type is left alone rather than guessed
    // at, which keeps the previous behaviour for anything this cannot vouch
    // for.
    let right = match (&left, &right) {
        (Value::Data { db_type: lt, .. }, Value::Data { db_type: rt, .. })
            if lt != rt && *rt == DbType::STRING =>
        {
            coerce_to(right, lt)
        }
        _ => right,
    };
    let Some(ordering) = compare(&left, &right) else {
        return Ok(Some(false));
    };
    Ok(Some(match condition.cmp {
        Cmp::Eq => ordering.is_eq(),
        Cmp::Ne => !ordering.is_eq(),
        Cmp::Lt => ordering.is_lt(),
        Cmp::Le => ordering.is_le(),
        Cmp::Gt => ordering.is_gt(),
        Cmp::Ge => ordering.is_ge(),
        Cmp::IsNull
        | Cmp::IsNotNull
        | Cmp::IsTrue
        | Cmp::IsNotTrue
        | Cmp::IsFalse
        | Cmp::IsNotFalse => unreachable!("handled above"),
    }))
}

/// Converts a value to another type, or says it cannot.
/// Re-encodes a computed value as the type it is advertised as.
///
/// `eval` does integer arithmetic in `i64` and floating point in `f64`, so a
/// result whose declared type is narrower — `int4` for `counter - 1` on an
/// `int4` column — carries more bytes than the description promises. A client
/// decodes by the description, so the two have to agree: eight bytes under an
/// `int4` OID is not a wrong number, it is an unreadable one. Falls back to the
/// value unchanged when it cannot be re-encoded, which keeps a conversion
/// failure from turning into a lost value.
/// [`coerce_to`], for callers outside this module.
pub fn coerce_to_public(value: Value, target: &DbType) -> Value {
    coerce_to(value, target)
}

pub(crate) fn coerce_to(value: Value, target: &DbType) -> Value {
    if matches!(value, Value::Null) {
        return Value::Null;
    }
    let original = value.clone();
    match cast(value, target) {
        Ok(converted) => converted,
        Err(_) => original,
    }
}

pub fn cast(value: Value, target: &DbType) -> Result<Value, String> {
    let Value::Data { bytes, db_type } = value else {
        return Ok(Value::Null);
    };
    if db_type == *target {
        return Ok(Value::Data { bytes, db_type });
    }
    // To `json`: text checked to be one document and kept as written; a
    // `jsonb` document as its text. A value travels as the `STRING` a `json`
    // column stores (`yields_json_text` says what the expression is).
    if *target == DbType::JSONTEXT {
        return match db_type {
            DbType::STRING | DbType::CHAR | DbType::JSON => {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                crate::core::value_codec::check_json_text(&text)?;
                Ok(Value::Data { bytes: text.into_bytes(), db_type: DbType::STRING })
            }
            DbType::JSONB => Ok(Value::Data {
                bytes: {
                    let mut out = Vec::with_capacity(bytes.len());
                    crate::core::jsonb::render_text_into(&bytes, &mut out)?;
                    out
                },
                db_type: DbType::STRING,
            }),
            other => Err(format!(
                "cannot cast type {} to json",
                crate::core::value_codec::pg_input_type_name(&other)
            )),
        };
    }
    // To a `timestamp` (without time zone): text is read with its zone
    // discarded; any other value is the `timestamptz` cast, the same
    // microseconds (the session is UTC). Either way the value is the
    // `DATETIME` it is stored as; the expression says which type it is
    // (`yields_timestamp`).
    if *target == DbType::TIMESTAMP {
        if matches!(db_type, DbType::STRING | DbType::CHAR) {
            let text = String::from_utf8_lossy(&bytes).to_string();
            return crate::core::value_codec::encode_literal(&DbType::TIMESTAMP, text.trim())
                .map(|bytes| Value::Data { bytes, db_type: DbType::DATETIME })
                .ok_or_else(|| format!("invalid input syntax for type timestamp: \"{}\"", text.trim()));
        }
        return cast(Value::Data { bytes, db_type }, &DbType::DATETIME);
    }

    // Text in, typed out: the literal is re-read as the target type. The
    // numeric and boolean targets are parsed here — `encode_literal` covers
    // only the specialty types (UUID, timestamps, decimals, documents), so
    // `'42'::int` used to be refused as unreadable. Parsing with the target's
    // own width also makes an out-of-range literal an error rather than a
    // silent wrap.
    if db_type == DbType::STRING {
        let text = String::from_utf8_lossy(&bytes).to_string();
        let trimmed = text.trim();
        // Text reaching an integer target is read by PostgreSQL's integer
        // input, which has no fraction: `'1.5'::int` is 22P02, as is
        // `(doc->>'k')::int` over a "1.5". It rounded, so a filter on such a
        // cast silently matched rows PostgreSQL refuses to compare. (A
        // NUMERIC or float source rounds; it does not come through here.)
        fn int_bytes<const N: usize>(
            text: &str,
            min: i128,
            max: i128,
            encode: impl Fn(i128) -> [u8; N],
        ) -> Option<Vec<u8>> {
            let v = pg_integer_text(text)?;
            (min..=max).contains(&v).then(|| encode(v).to_vec())
        }
        let parsed: Option<Vec<u8>> = match target {
            DbType::I8 => int_bytes(trimmed, i8::MIN as i128, i8::MAX as i128, |v| {
                (v as i8).to_le_bytes()
            }),
            DbType::I16 => int_bytes(trimmed, i16::MIN as i128, i16::MAX as i128, |v| {
                (v as i16).to_le_bytes()
            }),
            DbType::I32 => int_bytes(trimmed, i32::MIN as i128, i32::MAX as i128, |v| {
                (v as i32).to_le_bytes()
            }),
            DbType::I64 => int_bytes(trimmed, i64::MIN as i128, i64::MAX as i128, |v| {
                (v as i64).to_le_bytes()
            }),
            DbType::U8 => int_bytes(trimmed, 0, u8::MAX as i128, |v| (v as u8).to_le_bytes()),
            DbType::U16 => int_bytes(trimmed, 0, u16::MAX as i128, |v| (v as u16).to_le_bytes()),
            DbType::U32 => int_bytes(trimmed, 0, u32::MAX as i128, |v| (v as u32).to_le_bytes()),
            DbType::U64 => int_bytes(trimmed, 0, u64::MAX as i128, |v| (v as u64).to_le_bytes()),
            DbType::F32 => crate::core::value_codec::parse_float4(trimmed).map(|v| v.to_le_bytes().to_vec()),
            DbType::F64 => crate::core::value_codec::parse_float8(trimmed).map(|v| v.to_le_bytes().to_vec()),
            DbType::BOOL => match trimmed.to_ascii_lowercase().as_str() {
                "t" | "true" | "1" | "yes" | "on" => Some(vec![1u8]),
                "f" | "false" | "0" | "no" | "off" => Some(vec![0u8]),
                _ => None,
            },
            _ => crate::core::value_codec::encode_literal(target, &text),
        };
        return match parsed {
            Some(encoded) => Ok(Value::Data {
                bytes: encoded,
                db_type: target.clone(),
            }),
            // A number a NUMERIC cannot hold (36 integer digits lifted to the
            // engine's nine places) is an overflow, 22003, not an internal
            // error: PostgreSQL's numeric_value_out_of_range.
            None if *target == DbType::DECIMAL && trimmed.parse::<f64>().is_ok_and(f64::is_finite) => {
                Err(crate::core::value_codec::refusal_message(target, trimmed))
            }
            // A number past a float's range: `"1e400" is out of range for
            // type double precision`, 22003. It was infinity.
            None if matches!(target, DbType::F32 | DbType::F64) && trimmed.parse::<f64>().is_ok() => {
                Err(crate::core::value_codec::refusal_message(target, trimmed))
            }
            // In PostgreSQL's words, so the SQLSTATE is the input function's
            // (22P02 for most): "'x' cannot be read as I32" was XX000, and
            // `sum(t::int)` over a 'x' answered XX000 where PostgreSQL
            // answers 22P02.
            None => Err(crate::core::value_codec::refusal_message(target, &text)),
        };
    }

    // Typed in, text out.
    // A boolean's text is 'true' or 'false' (PostgreSQL's `booltext`). It
    // rendered as its SQL literal, '1' or '0', so `f::text = 'true'` matched
    // nothing and every `bool::text` in a select list sent a digit.
    if *target == DbType::STRING && db_type == DbType::BOOL {
        return Ok(Value::Data { bytes: bool_text(&bytes).as_bytes().to_vec(), db_type: DbType::STRING });
    }
    if *target == DbType::STRING {
        let rendered = crate::core::rql::subquery::value_to_literal(&db_type, &bytes)
            .ok_or_else(|| format!("{:?} cannot be rendered as text", db_type))?;
        let mut unquoted = crate::core::value_codec::unquote_sql(&rendered).to_string();
        // A `DATETIME` is a `timestamptz`, whose text carries its offset, as
        // PostgreSQL shows it (`… 08:00:00+00`; the session is UTC). A
        // `timestamp` column's value is shown bare by the caller that knows
        // it is one ([`local_timestamp_text`]).
        if db_type == DbType::DATETIME {
            unquoted.push_str("+00");
        }
        return Ok(Value::Data {
            bytes: unquoted.into_bytes(),
            db_type: DbType::STRING,
        });
    }

    // A float to an integer rounds ties to even, as PostgreSQL's `rint`
    // does. It must not reach the text path at the end, which re-reads the
    // rendered value with `numeric`'s half-away-from-zero rounding.
    if matches!(db_type, DbType::F32 | DbType::F64)
        && matches!(
            target,
            DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
        )
    {
        return to_integer_column(Value::Data { bytes, db_type }, target);
    }

    // A `real` to `double precision` is the same number, widened exactly, as
    // PostgreSQL's `float4_float8`: `0.1::real::float8` is
    // 0.10000000149011612. Through its text (below) it became the double
    // nearest 0.1, another number, so a REAL stored into a FLOAT8 column did
    // not equal that REAL compared against it.
    if db_type == DbType::F32 && *target == DbType::F64 {
        if let Ok(b) = <[u8; 4]>::try_from(bytes.as_slice()) {
            return Ok(Value::Data { bytes: f64::from(f32::from_le_bytes(b)).to_le_bytes().to_vec(), db_type: DbType::F64 });
        }
    }

    // Between numeric widths.
    let value = Value::Data {
        bytes,
        db_type: db_type.clone(),
    };
    if let Some(n) = as_i64(&value) {
        // A narrowing that does not fit is PostgreSQL's `integer out of range`,
        // never the low bytes: `3000000000::bigint::int` was -1294967296.
        let fits = |ok: bool| -> Result<(), String> {
            if ok { Ok(()) } else { Err(integer_out_of_range(target)) }
        };
        match target {
            DbType::I8 => fits(i8::try_from(n).is_ok())?,
            DbType::I16 => fits(i16::try_from(n).is_ok())?,
            DbType::I32 => fits(i32::try_from(n).is_ok())?,
            DbType::U8 => fits(u8::try_from(n).is_ok())?,
            DbType::U16 => fits(u16::try_from(n).is_ok())?,
            DbType::U32 => fits(u32::try_from(n).is_ok())?,
            DbType::U64 => fits(n >= 0)?,
            _ => {}
        }
        let encoded = match target {
            DbType::I8 => Some((n as i8).to_le_bytes().to_vec()),
            DbType::I16 => Some((n as i16).to_le_bytes().to_vec()),
            DbType::I32 => Some((n as i32).to_le_bytes().to_vec()),
            DbType::I64 => Some(n.to_le_bytes().to_vec()),
            DbType::U8 => Some((n as u8).to_le_bytes().to_vec()),
            DbType::U16 => Some((n as u16).to_le_bytes().to_vec()),
            DbType::U32 => Some((n as u32).to_le_bytes().to_vec()),
            DbType::U64 => Some((n as u64).to_le_bytes().to_vec()),
            DbType::F32 => Some((n as f32).to_le_bytes().to_vec()),
            DbType::F64 => Some((n as f64).to_le_bytes().to_vec()),
            // A target this table does not cover — `NUMERIC` above all — is
            // **not** an error: it falls through to the text rendering below,
            // which reads an integer back as a decimal perfectly well.
            //
            // Returning an error here made `CAST(n AS NUMERIC)` fail, and an
            // aggregate over it then swallowed that failure as a NULL and
            // reported the sum as **0** — a plausible number, on a chart, that
            // nobody would question.
            _ => None,
        };
        if let Some(encoded) = encoded {
            return Ok(Value::Data {
                bytes: encoded,
                db_type: target.clone(),
            });
        }
    }

    // Everything else goes through text: rendered, then re-read as the
    // target. This is what carries `AVG(x)::real` — a DECIMAL or float source
    // reaching a numeric target — without a per-pair conversion table. The
    // STRING source case is handled above, so the recursion terminates.
    if let Value::Data { bytes, .. } = &value {
        if let Some(rendered) = crate::core::rql::subquery::value_to_literal(&db_type, bytes) {
            let unquoted = crate::core::value_codec::unquote_sql(&rendered).to_string();
            // A number into an integer rounds half away from zero, as
            // PostgreSQL's `numeric_int4` does (`2.5::int` is 3, and so is a
            // jsonb `2.5`). Only text is read by the integer input, which has
            // no fraction; a number's own text must not be.
            if let Some(range) = integer_range(target) {
                if let Some(n) = crate::core::rql::pg_numeric::parse_text(unquoted.trim()) {
                    let whole = n.round_to(0).ok_or_else(|| integer_out_of_range(target))?;
                    if !range.contains(&whole.m) {
                        return Err(integer_out_of_range(target));
                    }
                    return cast(Value::Data { bytes: (whole.m as i64).to_le_bytes().to_vec(), db_type: DbType::I64 }, target);
                }
            }
            return cast(
                Value::Data {
                    bytes: unquoted.into_bytes(),
                    db_type: DbType::STRING,
                },
                target,
            );
        }
    }

    Err(format!(
        "cast from {:?} to {:?} is not supported yet",
        db_type, target
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::row::row::Column;
    use crate::memory_pool::MEMORY_POOL;

    /// Each name in [`FUNCTIONS`] reaches the call dispatch: a wrong argument
    /// count or type may be refused, but never as a function this does not
    /// know, which is what a `WHERE` lifting it would then report.
    #[test]
    fn every_listed_function_is_evaluated() {
        let schema = vec![field("s", DbType::STRING, 0)];
        for name in FUNCTIONS {
            for args in ["s", "s, s", "s, s, s"] {
                if let Err(e) = parse(&format!("{name}({args})"), &schema) {
                    assert!(!e.contains("is not supported yet"), "{name}({args}): {e}");
                }
            }
        }
    }

    fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
        SchemaField::new(
            name.to_string(),
            ty.clone(),
            ty.get_size(),
            0,
            write_order,
            false,
        )
    }

    fn column(schema_id: u64, ty: DbType, bytes: &[u8]) -> Column {
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        if !bytes.is_empty() {
            mb.into_slice_mut().copy_from_slice(bytes);
        }
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data: mb,
            column_type: ty,
        }
    }

    /// `v` is I32 at write order 0; `__null_v` is its companion at 1.
    fn schema() -> Vec<SchemaField> {
        vec![
            field("v", DbType::I32, 0),
            field("__null_v", DbType::BOOL, 1),
            field("t", DbType::STRING, 2),
        ]
    }

    fn row_with(v: Option<i32>, t: &str) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        match v {
            Some(n) => {
                columns.push(column(0, DbType::I32, &n.to_le_bytes()));
                columns.push(column(1, DbType::BOOL, &[0u8]));
            }
            None => {
                columns.push(column(0, DbType::I32, &0i32.to_le_bytes()));
                columns.push(column(1, DbType::BOOL, &[1u8]));
            }
        }
        columns.push(column(2, DbType::STRING, t.as_bytes()));
        Row {
            position: 0,
            length: 0,
            columns,
        }
    }

    #[test]
    fn coalesce_keeps_a_present_value_and_falls_back_on_null() {
        let s = schema();
        let e = parse("COALESCE(v, 7)", &s).unwrap();
        // Present: the column wins.
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(3), ""), &s).unwrap()),
            Some(3)
        );
        // NULL: the fallback wins. The companion flag is what distinguishes
        // this from a stored zero, which is why a bare byte check is not enough.
        assert_eq!(as_i64(&eval(&e, &row_with(None, ""), &s).unwrap()), Some(7));
    }

    #[test]
    fn an_unsupported_expression_is_an_error_not_a_value() {
        let s = schema();
        // The bug this module exists to prevent: an unparsed expression must
        // never become data.
        assert!(parse("SOME_FUNC(v)", &s).is_err());
        // `v + 1` used to belong here, then `v ^ 1`. Both are supported now,
        // so the stand-in is an operator that still is not.
        assert!(parse("v ## 1", &s).is_err());
        assert!(parse("v + ", &s).is_err());
    }

    #[test]
    fn a_literal_is_one_only_when_its_quote_closes_at_the_end() {
        let s = schema();
        // Each begins and ends with a quote, and each was one string literal.
        for text in ["'x' = 'y'", "'a''b' = 'a''b'", "t || 'x' = 'xx'", "'x' LIKE 'x%'"] {
            let parsed = parse(text, &s);
            assert!(
                !matches!(&parsed, Ok(Expr::Literal { .. }))
                    && !matches!(&parsed, Ok(Expr::Concat(_, right)) if matches!(**right, Expr::Literal { .. })),
                "{text} parsed as {parsed:?}"
            );
        }
        // A doubled quote inside one literal does not close it.
        match parse("'it''s'", &s) {
            Ok(Expr::Literal { bytes, .. }) => assert_eq!(bytes, b"it's"),
            other => panic!("'it''s' parsed as {other:?}"),
        }
        assert_eq!(closing_quote("'it''s' = 'x'"), Some(6));
        assert_eq!(closing_quote("'open"), None);
    }

    #[test]
    fn intervals_offset_a_timestamp_and_stay_a_timestamp() {
        let s = schema();
        let r = row_with(Some(1), "");
        let now = eval(&parse("now()", &s).unwrap(), &r, &s).unwrap();
        let later = eval(
            &parse("now() + make_interval(secs => 120)", &s).unwrap(),
            &r,
            &s,
        )
        .unwrap();

        // The result must still be a DATETIME. Handing a timestamp column a
        // bare integer is what produced "Cannot insert row due to schema
        // mismatch" from a statement that looked fine.
        assert!(matches!(
            later,
            Value::Data {
                db_type: DbType::DATETIME,
                ..
            }
        ));
        let (Some(a), Some(b)) = (as_i64(&now), as_i64(&later)) else {
            panic!("both should decode");
        };
        // 120 seconds in microseconds, allowing for the clock moving between
        // the two evaluations.
        let delta = b - a;
        assert!(
            (119_000_000..=121_000_000).contains(&delta),
            "expected ~120s, got {}us",
            delta
        );
    }

    #[test]
    fn interval_literals_parse_in_both_spellings() {
        use crate::core::pg_types::{Interval, decode_interval};
        let s = schema();
        let r = row_with(Some(1), "");

        // An interval keeps its months, days and microseconds apart, so a day
        // is one *day* rather than 86,400,000,000 microseconds — the two behave
        // differently across a daylight-saving boundary.
        let bytes = |text: &str| match eval(&parse(text, &s).unwrap(), &r, &s).unwrap() {
            Value::Data { bytes, db_type } => {
                assert_eq!(db_type, DbType::INTERVAL, "{}", text);
                bytes
            }
            Value::Null => panic!("{} evaluated to NULL", text),
        };
        assert_eq!(
            decode_interval(&bytes("INTERVAL '1 day'")),
            Some(Interval {
                months: 0,
                days: 1,
                micros: 0
            })
        );
        // Several units in one literal add up rather than the last one winning.
        assert_eq!(
            decode_interval(&bytes("INTERVAL '1 day 6 hours'")),
            Some(Interval {
                months: 0,
                days: 1,
                micros: 6 * 3_600_000_000
            })
        );
        // Subtraction moves backwards.
        let before = eval(&parse("now() - INTERVAL '1 day'", &s).unwrap(), &r, &s).unwrap();
        let now = eval(&parse("now()", &s).unwrap(), &r, &s).unwrap();
        assert!(as_i64(&before).unwrap() < as_i64(&now).unwrap());
    }

    #[test]
    fn concatenation_joins_text_and_is_strict_in_null() {
        let s = schema();
        let present = row_with(Some(7), "abc");
        let absent = row_with(None, "abc");

        let e = parse("t || '-x'", &s).unwrap();
        assert_eq!(as_text(&eval(&e, &present, &s).unwrap()), "abc-x");

        // Numbers render as they print, so a key can be built from one.
        let e = parse("'id=' || v", &s).unwrap();
        assert_eq!(as_text(&eval(&e, &present, &s).unwrap()), "id=7");

        // NULL anywhere makes the whole thing NULL rather than disappearing.
        let e = parse("'id=' || v", &s).unwrap();
        assert!(eval(&e, &absent, &s).unwrap().is_null());

        // Left-associative across three operands.
        let e = parse("'a' || 'b' || 'c'", &s).unwrap();
        assert_eq!(as_text(&eval(&e, &present, &s).unwrap()), "abc");
    }

    #[test]
    fn the_catalog_text_functions_behave_as_postgres_does() {
        let s = schema();
        let r = row_with(Some(1), "x");
        let text = |sql: &str| as_text(&eval(&parse(sql, &s).unwrap(), &r, &s).unwrap());

        assert_eq!(text("current_schema()"), "public");
        // Only what needs quoting is quoted.
        assert_eq!(text("quote_ident('users')"), "users");
        assert_eq!(text("quote_ident('User Table')"), "\"User Table\"");
        assert_eq!(text("replace('a-b-c', '-', '')"), "abc");
        assert_eq!(text("lower('ABC')"), "abc");
        assert_eq!(text("upper('abc')"), "ABC");

        // The composed shape the reindex service uses.
        assert_eq!(
            text("quote_ident('public') || '.' || quote_ident('my table')"),
            "public.\"my table\""
        );
    }

    #[test]
    fn the_strict_text_functions_give_null_for_a_null_argument() {
        // PostgreSQL 17 answers NULL for each, where these used to give ''.
        let s = schema();
        let r = row_with(Some(1), "x");
        for sql in [
            "lower(NULL)",
            "upper(NULL)",
            "quote_ident(NULL)",
            "to_tsvector('english', NULL)",
            "to_tsvector(NULL, 'cat')",
            "to_tsquery('english', NULL)",
            "plainto_tsquery('english', NULL)",
        ] {
            let value = eval(&parse(sql, &s).unwrap(), &r, &s).unwrap();
            assert!(value.is_null(), "{} gave {:?}", sql, as_text(&value));
        }
    }

    #[test]
    fn a_quoted_number_is_accepted_where_a_number_is_expected() {
        // A bound parameter can reach the parser quoted, and PostgreSQL casts
        // it. Refusing it made `make_interval(secs => $3)` fail only over the
        // extended protocol, which is the only way an ORM sends it.
        let s = schema();
        let r = row_with(Some(1), "");
        let e = parse("make_interval(secs => '120')", &s).unwrap();
        let Value::Data { bytes, db_type } = eval(&e, &r, &s).unwrap() else {
            panic!("make_interval gave NULL");
        };
        assert_eq!(db_type, DbType::INTERVAL);
        assert_eq!(crate::core::pg_types::decode_interval(&bytes).map(|i| i.micros), Some(120_000_000));
        assert!(parse("INTERVAL '1 day'", &s).is_ok());
    }

    #[test]
    fn months_and_years_are_kept_as_months_rather_than_flattened() {
        use crate::core::pg_types::{Interval, decode_interval};
        let s = schema();
        let r = row_with(Some(1), "");

        // These used to be refused, because a month expressed as a fixed number
        // of days is wrong for most months and wrong without saying so. They
        // are accepted now that the months are carried as months and applied to
        // the calendar — see `pg_types::shift_timestamp`.
        let bytes = |text: &str| match eval(&parse(text, &s).unwrap(), &r, &s).unwrap() {
            Value::Data { bytes, .. } => bytes,
            Value::Null => panic!("{} evaluated to NULL", text),
        };
        assert_eq!(
            decode_interval(&bytes("INTERVAL '1 month'")),
            Some(Interval {
                months: 1,
                days: 0,
                micros: 0
            })
        );
        assert_eq!(
            decode_interval(&bytes("INTERVAL '1 year'")),
            Some(Interval {
                months: 12,
                days: 0,
                micros: 0
            })
        );

        // A unit that means nothing is still refused rather than guessed at.
        assert!(parse("INTERVAL '5 fortnights'", &s).is_err());
    }

    #[test]
    fn arithmetic_evaluates_and_is_strict_in_null() {
        let s = schema();
        let present = row_with(Some(5), "");
        let absent = row_with(None, "");

        assert_eq!(
            as_i64(&eval(&parse("v + 1", &s).unwrap(), &present, &s).unwrap()),
            Some(6)
        );
        assert_eq!(
            as_i64(&eval(&parse("v - 2", &s).unwrap(), &present, &s).unwrap()),
            Some(3)
        );
        assert_eq!(
            as_i64(&eval(&parse("v * 3", &s).unwrap(), &present, &s).unwrap()),
            Some(15)
        );
        assert_eq!(
            as_i64(&eval(&parse("v / 2", &s).unwrap(), &present, &s).unwrap()),
            Some(2)
        );
        assert_eq!(
            as_i64(&eval(&parse("v % 3", &s).unwrap(), &present, &s).unwrap()),
            Some(2)
        );

        // NULL + 1 is NULL, not 1. This is what lets `COALESCE(MAX(x), 0) + 1`
        // reach its fallback instead of adding to a zero that was never there.
        assert!(
            eval(&parse("v + 1", &s).unwrap(), &absent, &s)
                .unwrap()
                .is_null()
        );
    }

    #[test]
    fn an_integer_result_keeps_the_wider_operand_type() {
        // PostgreSQL gives `int4` for `int4 + int4`. Reporting `int8` is
        // rejected outright by a typed client — the value is right and the
        // declared type is not, which is the same class of bug as advertising
        // SMALLINT as int4.
        let s = schema();
        let r = row_with(Some(5), "");
        // `1` is an `int4` constant, as in PostgreSQL, so `v + 1` stays I32;
        // a constant past `int4` is `int8`, and widens the result.
        let v = eval(&parse("v + 1", &s).unwrap(), &r, &s).unwrap();
        assert!(matches!(
            v,
            Value::Data {
                db_type: DbType::I32,
                ..
            }
        ));
        let v = eval(&parse("v + 3000000000", &s).unwrap(), &r, &s).unwrap();
        assert!(matches!(
            v,
            Value::Data {
                db_type: DbType::I64,
                ..
            }
        ));

        // Two I32 operands stay I32.
        let both = Expr::Arith(
            Box::new(Expr::Column(field("v", DbType::I32, 0))),
            ArithOp::Add,
            Box::new(Expr::Column(field("v", DbType::I32, 0))),
        );
        let v = eval(&both, &r, &s).unwrap();
        assert!(matches!(
            v,
            Value::Data {
                db_type: DbType::I32,
                ..
            }
        ));
        assert_eq!(as_i64(&v), Some(10));
        assert_eq!(static_type(&both), DbType::I32);
    }

    #[test]
    fn an_out_of_range_narrow_result_is_an_error_not_a_wrap() {
        let s = schema();
        let r = row_with(Some(i32::MAX), "");
        let both = Expr::Arith(
            Box::new(Expr::Column(field("v", DbType::I32, 0))),
            ArithOp::Add,
            Box::new(Expr::Column(field("v", DbType::I32, 0))),
        );
        assert!(eval(&both, &r, &s).is_err());
    }

    #[test]
    fn numeric_arithmetic_keeps_its_scale() {
        use crate::core::value_codec::DECIMAL_SCALE;
        let factor = 10i128.pow(DECIMAL_SCALE);
        let dec = |units: i128| Expr::Literal {
            bytes: units.to_le_bytes().to_vec(),
            db_type: DbType::DECIMAL,
        };
        let s = schema();
        let r = row_with(Some(1), "");
        let read = |e: &Expr| -> i128 {
            match eval(e, &r, &s).unwrap() {
                Value::Data {
                    bytes,
                    db_type: DbType::DECIMAL,
                } => i128::from_le_bytes(bytes.as_slice().try_into().unwrap()),
                other => panic!("expected a DECIMAL, got {:?}", other),
            }
        };

        // 1.50 + 2.25 = 3.75
        let a = (3 * factor) / 2;
        let b = (9 * factor) / 4;
        assert_eq!(
            read(&Expr::Arith(
                Box::new(dec(a)),
                ArithOp::Add,
                Box::new(dec(b))
            )),
            a + b
        );
        // 1.50 * 2 = 3.00 — the product is scaled twice and has to come back
        // down, or the answer is out by a factor of a billion.
        assert_eq!(
            read(&Expr::Arith(
                Box::new(dec(a)),
                ArithOp::Mul,
                Box::new(dec(2 * factor))
            )),
            3 * factor
        );
        // 3.00 / 2 = 1.50 — the numerator has to be lifted first, or the
        // fractional part is truncated to zero.
        assert_eq!(
            read(&Expr::Arith(
                Box::new(dec(3 * factor)),
                ArithOp::Div,
                Box::new(dec(2 * factor))
            )),
            a
        );
    }

    /// The doubly-scaled product of two NUMERICs passes `i128` long before the
    /// answer does: 1e11 * 1e10 = 1e21 is far inside NUMERIC's range, but its
    /// intermediate is 1e39. That used to report "numeric overflow".
    #[test]
    fn numeric_products_and_quotients_past_the_i128_intermediate() {
        use crate::core::value_codec::DECIMAL_SCALE;
        let factor = 10i128.pow(DECIMAL_SCALE);
        let dec = |v: i128| Value::Data {
            bytes: v.to_le_bytes().to_vec(),
            db_type: DbType::DECIMAL,
        };
        let arith = |a: i128, op, b: i128| decimal_arith(&dec(a), op, &dec(b));
        let read = |v: Value| match v {
            Value::Data { bytes, .. } => i128::from_le_bytes(bytes.as_slice().try_into().unwrap()),
            Value::Null => panic!("null"),
        };

        let e11 = 100_000_000_000i128 * factor;
        let e10 = 10_000_000_000i128 * factor;
        let e21 = 1_000_000_000_000_000_000_000i128 * factor;
        assert_eq!(read(arith(e11, ArithOp::Mul, e10).unwrap()), e21);
        assert_eq!(read(arith(-e11, ArithOp::Mul, e10).unwrap()), -e21);
        assert_eq!(read(arith(e21, ArithOp::Div, e10).unwrap()), e11);
        assert_eq!(read(arith(e21, ArithOp::Div, -e11).unwrap()), -e10);

        // Truncation toward zero, as the i128 fast path truncates: 1.000000001
        // * 1.000000001 = 1.000000002000000001, kept to nine places.
        let x = factor + 1;
        assert_eq!(read(arith(x, ArithOp::Mul, x).unwrap()), factor + 2);
        // The same answer through the wide path, scaled up past i128 squares.
        let big = 10i128.pow(19) * factor + 1;
        assert_eq!(
            mul_div_i128(big, big, factor),
            None,
            "1e38 at scale 9 does not fit, and says so"
        );
        assert_eq!(mul_div_i128(big, factor, factor), Some(big));
        assert_eq!(mul_div_i128(-big, factor, factor), Some(-big));

        // A real overflow is still one.
        let huge = i128::MAX / 2;
        assert!(arith(huge, ArithOp::Mul, huge).is_err());
    }

    #[test]
    fn a_fractional_literal_is_exact_in_numeric_arithmetic() {
        use crate::core::value_codec::DECIMAL_SCALE;
        let factor = 10i128.pow(DECIMAL_SCALE);
        let dec = Value::Data {
            bytes: (100 * factor).to_le_bytes().to_vec(),
            db_type: DbType::DECIMAL,
        };
        let float = |f: f64| Value::Data {
            bytes: f.to_le_bytes().to_vec(),
            db_type: DbType::F64,
        };
        let read = |v: Value| match v {
            Value::Data { bytes, db_type } => {
                assert_eq!(db_type, DbType::DECIMAL);
                i128::from_le_bytes(bytes.as_slice().try_into().unwrap())
            }
            Value::Null => panic!("null"),
        };
        // A written fraction is a NUMERIC constant, so it stays exact: 0.1 is
        // not exact as a float, and is as the digits written.
        let literal = |text: &str| match parse(text, &[]).unwrap() {
            Expr::Literal { bytes, db_type } => Value::Data { bytes, db_type },
            other => panic!("{text} parsed as {other:?}"),
        };
        assert_eq!(read(arith(&dec, ArithOp::Mul, &literal("1.5")).unwrap()), 150 * factor);
        assert_eq!(
            read(arith(&dec, ArithOp::Add, &literal("0.1")).unwrap()),
            100 * factor + factor / 10
        );
        // A float *value* against a NUMERIC is float arithmetic, as PostgreSQL
        // resolves `numeric + float8`.
        assert!(matches!(
            arith(&dec, ArithOp::Add, &float(0.5)).unwrap(),
            Value::Data { db_type: DbType::F64, .. }
        ));
    }

    #[test]
    fn rescale_moves_between_declared_scales() {
        assert_eq!(rescale_decimal(150, 2, 9), Some(1_500_000_000));
        assert_eq!(rescale_decimal(1_500_000_000, 9, 2), Some(150));
        // Down is truncation toward zero, as a declared scale truncates.
        assert_eq!(rescale_decimal(-1_999, 3, 0), Some(-1));
        assert_eq!(rescale_decimal(i128::MAX, 0, 9), None);
    }

    #[test]
    fn arithmetic_precedence_and_grouping() {
        let s = schema();
        let r = row_with(Some(2), "");
        // `*` binds tighter than `+`: 2 + 3*4 = 14, not 20.
        assert_eq!(
            as_i64(&eval(&parse("v + 3 * 4", &s).unwrap(), &r, &s).unwrap()),
            Some(14)
        );
        // Parentheses override it.
        assert_eq!(
            as_i64(&eval(&parse("(v + 3) * 4", &s).unwrap(), &r, &s).unwrap()),
            Some(20)
        );
        // Subtraction is left-associative: (10-3)-2 = 5, not 10-(3-2) = 9.
        assert_eq!(
            as_i64(&eval(&parse("10 - 3 - 2", &s).unwrap(), &r, &s).unwrap()),
            Some(5)
        );
    }

    #[test]
    fn a_negative_literal_is_not_a_subtraction() {
        let s = schema();
        let r = row_with(Some(2), "");
        // The whole text is a number, so it must not be split at the `-`.
        assert_eq!(
            as_i64(&eval(&parse("-5", &s).unwrap(), &r, &s).unwrap()),
            Some(-5)
        );
        // And a sign following an operator belongs to the literal: 2 * -3.
        assert_eq!(
            as_i64(&eval(&parse("v * -3", &s).unwrap(), &r, &s).unwrap()),
            Some(-6)
        );
        // A float in exponent form keeps its `-` too, rather than becoming
        // `1e` minus `5`.
        assert!(parse("1e-5", &s).is_ok());
    }

    #[test]
    fn division_by_zero_and_overflow_are_errors_not_values() {
        let s = schema();
        let r = row_with(Some(0), "");
        assert!(eval(&parse("1 / v", &s).unwrap(), &r, &s).is_err());
        assert!(eval(&parse("1 % v", &s).unwrap(), &r, &s).is_err());
        // A wrapped total is the wrong answer reported as success.
        let big = format!("{} * 2", i64::MAX);
        assert!(eval(&parse(&big, &s).unwrap(), &r, &s).is_err());
    }

    #[test]
    fn arithmetic_does_not_split_inside_a_case_block_or_a_string() {
        let s = schema();
        let r = row_with(Some(1), "");
        // The `+` sits at paren depth zero inside the CASE arms; splitting
        // there would tear the block in half.
        let e = parse("CASE WHEN v = 1 THEN v + 10 ELSE v + 20 END", &s).unwrap();
        assert_eq!(as_i64(&eval(&e, &r, &s).unwrap()), Some(11));
        // A `-` inside a quoted literal is text, not an operator.
        let lit = parse("'a-b'", &s).unwrap();
        assert!(matches!(lit, Expr::Literal { .. }));
    }

    #[test]
    fn nullif_and_least_and_greatest() {
        let s = schema();
        let r = row_with(Some(5), "");
        assert!(
            eval(&parse("NULLIF(v, 5)", &s).unwrap(), &r, &s)
                .unwrap()
                .is_null()
        );
        assert_eq!(
            as_i64(&eval(&parse("NULLIF(v, 9)", &s).unwrap(), &r, &s).unwrap()),
            Some(5)
        );
        assert_eq!(
            as_i64(&eval(&parse("LEAST(v, 2)", &s).unwrap(), &r, &s).unwrap()),
            Some(2)
        );
        assert_eq!(
            as_i64(&eval(&parse("GREATEST(v, 2)", &s).unwrap(), &r, &s).unwrap()),
            Some(5)
        );
        // NULLs are ignored rather than winning, as PostgreSQL does.
        let n = row_with(None, "");
        assert_eq!(
            as_i64(&eval(&parse("LEAST(v, 4)", &s).unwrap(), &n, &s).unwrap()),
            Some(4)
        );
    }

    #[test]
    fn mod_and_nested_calls() {
        let s = schema();
        let r = row_with(Some(7), "");
        assert_eq!(
            as_i64(&eval(&parse("MOD(v, 2)", &s).unwrap(), &r, &s).unwrap()),
            Some(1)
        );
        assert_eq!(
            as_i64(&eval(&parse("LEAST(GREATEST(v, 1), 3)", &s).unwrap(), &r, &s).unwrap()),
            Some(3)
        );
        assert!(eval(&parse("MOD(v, 0)", &s).unwrap(), &r, &s).is_err());
    }

    #[test]
    fn case_when_picks_the_first_true_branch() {
        let s = schema();
        let e = parse("CASE WHEN v = 1 THEN 10 WHEN v = 2 THEN 20 ELSE 30 END", &s).unwrap();
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(1), ""), &s).unwrap()),
            Some(10)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(2), ""), &s).unwrap()),
            Some(20)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(9), ""), &s).unwrap()),
            Some(30)
        );
        // No ELSE and nothing matches is NULL, not zero.
        let no_else = parse("CASE WHEN v = 1 THEN 10 END", &s).unwrap();
        assert!(
            eval(&no_else, &row_with(Some(9), ""), &s)
                .unwrap()
                .is_null()
        );
    }

    #[test]
    fn case_when_is_null_reads_the_companion() {
        let s = schema();
        let e = parse("CASE WHEN v IS NULL THEN 1 ELSE 0 END", &s).unwrap();
        assert_eq!(as_i64(&eval(&e, &row_with(None, ""), &s).unwrap()), Some(1));
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(0), ""), &s).unwrap()),
            Some(0)
        );
    }

    #[test]
    fn casts_convert_rather_than_relabel() {
        let s = schema();
        let r = row_with(Some(300), "");
        // Narrowing writes the narrower width, not the original bytes.
        let e = parse("v::smallint", &s).unwrap();
        let Value::Data { bytes, db_type } = eval(&e, &r, &s).unwrap() else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::I16);
        assert_eq!(bytes.len(), 2);
        assert_eq!(i16::from_le_bytes(bytes[..].try_into().unwrap()), 300);

        assert!(parse("v::nosuchtype", &s).is_err());
    }

    /// A cast keeps its type modifier, as PostgreSQL 17's explicit cast
    /// does: text cut (and `character(n)` held without trailing spaces),
    /// times rounded half away from zero, a timestamp on PostgreSQL's epoch.
    #[test]
    fn a_cast_keeps_its_type_modifier() {
        let value = |text: &str| eval(&parse(text, &[]).unwrap(), &Row::default(), &[]).unwrap();
        let bytes = |text: &str| match value(text) {
            Value::Data { bytes, .. } => bytes,
            Value::Null => panic!("{text}: NULL"),
        };
        assert_eq!(bytes("'abcd'::varchar(2)"), b"ab");
        assert_eq!(bytes("CAST('abcd' AS character varying(3))"), b"abc");
        assert_eq!(bytes("'ab'::varchar(5)"), b"ab");
        assert_eq!(bytes("'abcd'::char(2)"), b"ab");
        assert_eq!(bytes("'ab   '::char(4)"), b"ab");
        assert_eq!(bytes("'abc'::character"), b"a");
        let ts = |t: &str| crate::core::value_codec::parse_timestamp_us(t).unwrap().to_le_bytes().to_vec();
        assert_eq!(bytes("'2024-01-01 10:00:00.6'::timestamp(0)"), ts("2024-01-01 10:00:01"));
        assert_eq!(bytes("'2024-01-01 10:00:00.5'::timestamp(0)"), ts("2024-01-01 10:00:01"));
        assert_eq!(bytes("'2024-01-01 10:00:00.123456'::timestamp(2)"), ts("2024-01-01 10:00:00.12"));
        assert_eq!(bytes("'1999-12-31 23:59:59.5'::timestamp(0)"), ts("1999-12-31 23:59:59"));
        assert_eq!(bytes("'2024-01-01 10:00:00.4'::timestamptz(0)"), ts("2024-01-01 10:00:00"));
        assert_eq!(bytes("'10:00:00.56'::time(1)"), 36_000_600_000i64.to_be_bytes());
        assert_eq!(bytes("'23:59:59.5'::time(0)"), 86_400_000_000i64.to_be_bytes());
        let interval = |t: &str| bytes(t)[..8].to_vec();
        assert_eq!(interval("'1.5 seconds'::interval(0)"), 2_000_000i64.to_be_bytes());
        assert_eq!(interval("'-1.5 seconds'::interval(0)"), (-2_000_000i64).to_be_bytes());
        assert_eq!(interval("'1 day 00:00:00.567'::interval(1)"), 600_000i64.to_be_bytes());
        // Past six digits is six: nothing to round.
        assert_eq!(bytes("'2024-01-01 10:00:00.123456'::timestamp(9)"), ts("2024-01-01 10:00:00.123456"));
        assert!(parse("'x'::char(0)", &[]).is_err());
        assert!(parse("'x'::varchar(-1)", &[]).is_err());
        // A `character(n)` comparison reads the literal without trailing spaces.
        let c = parse_cond("'ab'::char(4) = 'ab  '", &[]).unwrap();
        assert!(eval_cond(&c, &Row::default(), &[]).unwrap());
        let c = parse_cond("'ab'::varchar(4) = 'ab  '", &[]).unwrap();
        assert!(!eval_cond(&c, &Row::default(), &[]).unwrap());
    }

    #[test]
    fn rpad_and_lpad_fill_or_cut() {
        let text = |t: &str| match eval(&parse(t, &[]).unwrap(), &Row::default(), &[]).unwrap() {
            Value::Data { bytes, .. } => String::from_utf8(bytes).unwrap(),
            Value::Null => "NULL".to_string(),
        };
        assert_eq!(text("rpad('ab', 4)"), "ab  ");
        assert_eq!(text("rpad('abcdef', 3)"), "abc");
        assert_eq!(text("lpad('ab', 4)"), "  ab");
        assert_eq!(text("rpad('ab', 5, 'xy')"), "abxyx");
        assert_eq!(text("rpad(NULL, 3)"), "NULL");
    }

    #[test]
    fn case_when_takes_compound_conditions() {
        let s = schema();
        // Verbatim from a production migration: an OR joining a null test and
        // a comparison, with a column reference in the ELSE.
        let e = parse(
            "CASE WHEN v IS NULL OR v <= 0 THEN 3 WHEN v = 2 THEN 3 WHEN v > 3 THEN 3 ELSE v END",
            &s,
        )
        .unwrap();
        assert_eq!(as_i64(&eval(&e, &row_with(None, ""), &s).unwrap()), Some(3));
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(0), ""), &s).unwrap()),
            Some(3)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(2), ""), &s).unwrap()),
            Some(3)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(9), ""), &s).unwrap()),
            Some(3)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(3), ""), &s).unwrap()),
            Some(3),
            "ELSE echoes the column"
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(1), ""), &s).unwrap()),
            Some(1),
            "ELSE echoes the column"
        );

        // AND binds tighter than OR: `a OR b AND c` is `a OR (b AND c)`.
        let e = parse(
            "CASE WHEN v = 1 OR v > 3 AND v < 10 THEN 'in' ELSE 'out' END",
            &s,
        )
        .unwrap();
        let text_of = |v: Option<i32>| match eval(&e, &row_with(v, ""), &s).unwrap() {
            Value::Data { bytes, .. } => String::from_utf8(bytes).unwrap(),
            Value::Null => "NULL".to_string(),
        };
        assert_eq!(text_of(Some(1)), "in");
        assert_eq!(text_of(Some(5)), "in");
        assert_eq!(text_of(Some(20)), "out");
        assert_eq!(text_of(Some(2)), "out");

        // Parentheses group.
        let e = parse(
            "CASE WHEN (v = 1 OR v = 2) AND t = 'a' THEN 1 ELSE 0 END",
            &s,
        )
        .unwrap();
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(2), "a"), &s).unwrap()),
            Some(1)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(2), "b"), &s).unwrap()),
            Some(0)
        );

        // A function call inside the condition, verbatim shape from another
        // migration: `MOD(seq - 1, 2) = 0`.
        let e = parse("CASE WHEN MOD(v - 1, 2) = 0 THEN 1 ELSE 4 END", &s).unwrap();
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(1), ""), &s).unwrap()),
            Some(1)
        );
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(2), ""), &s).unwrap()),
            Some(4)
        );

        // NULL as a THEN result stays NULL, distinct from any value.
        let e = parse("CASE WHEN v IS NULL THEN NULL ELSE 1 END", &s).unwrap();
        assert!(eval(&e, &row_with(None, ""), &s).unwrap().is_null());
        assert_eq!(
            as_i64(&eval(&e, &row_with(Some(5), ""), &s).unwrap()),
            Some(1)
        );

        // NOT is SQL's three-valued one, never two-valued logic: NOT of a
        // NULL comparison is NULL, which a WHEN does not take.
        let e = parse("CASE WHEN NOT v = 1 THEN 1 ELSE 0 END", &s).unwrap();
        for (v, want) in [(Some(1), 0), (Some(2), 1), (None, 0)] {
            assert_eq!(as_i64(&eval(&e, &row_with(v, ""), &s).unwrap()), Some(want), "{v:?}");
        }
    }

    #[test]
    fn casts_read_text_and_reach_floats_through_it() {
        let s = schema();
        let r = row_with(Some(1), "");
        // A quoted literal to a numeric target — `'42'::int`.
        let Value::Data { bytes, db_type } =
            eval(&parse("'42'::int", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::I32);
        assert_eq!(i32::from_le_bytes(bytes[..].try_into().unwrap()), 42);
        // Out of range is an error, not a wrap.
        assert!(eval(&parse("'300'::smallint", &s).unwrap(), &r, &s).is_ok());
        assert!(eval(&parse("'70000'::smallint", &s).unwrap(), &r, &s).is_err());
        // `real` is float4, not float8.
        let Value::Data { bytes, db_type } = eval(&parse("v::real", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F32);
        assert_eq!(f32::from_le_bytes(bytes[..].try_into().unwrap()), 1.0);
        // A fractional literal reaches a float target.
        let Value::Data { bytes, db_type } =
            eval(&parse("'2.5'::float8", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F64);
        assert_eq!(f64::from_le_bytes(bytes[..].try_into().unwrap()), 2.5);
        // A float SOURCE reaches another width through the text fallback —
        // the path `AVG(x)::real` takes, since no direct f64→f32 arm exists.
        let Value::Data { bytes, db_type } =
            eval(&parse("('2.5'::float8)::real", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F32);
        assert_eq!(f32::from_le_bytes(bytes[..].try_into().unwrap()), 2.5);
        // Text into an integer is PostgreSQL's integer input: no fraction,
        // no exponent (22P02), whereas a NUMERIC rounds half away from zero.
        // Against PostgreSQL 17: '1092.75'::bigint, '2.5'::int and
        // '1e2'::int are each "invalid input syntax"; ' 7 '::int is 7,
        // '0x1A'::int 26, '1_000'::int 1000, '-0b101'::int -5;
        // '32768'::smallint is out of range; 2.5::int and '2.5'::numeric::int
        // are 3, (-2.5)::int is -3.
        for text in ["'1092.75'::bigint", "'2.5'::int", "'1e2'::int", "'1_'::int", "'_1'::int", "'1__0'::int", "'0x'::int", "''::int", "'32768'::smallint"] {
            assert!(eval(&parse(text, &s).unwrap(), &r, &s).is_err(), "{text} must be refused");
        }
        for (text, want) in [("' 7 '::int", 7i64), ("'0x1A'::int", 26), ("'1_000'::int", 1000), ("'-0b101'::int", -5), ("'+0o17'::int", 15), ("2.5::int", 3), ("(-2.5)::int", -3), ("'2.5'::numeric::int", 3), ("'32767'::smallint", 32767)] {
            let v = eval(&parse(text, &s).unwrap(), &r, &s).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(as_i64(&v), Some(want), "{text}");
        }
        // A boolean's text is its cast to text, as PostgreSQL's `booltext`.
        for (text, want) in [("true::text", "true"), ("false::text", "false"), ("'x' || true", "xtrue"), ("false || 'y'", "falsey")] {
            let v = eval(&parse(text, &s).unwrap(), &r, &s).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(as_text(&v), want, "{text}");
        }
        // Booleans read the forms PostgreSQL accepts.
        for (text, want) in [("'t'", 1u8), ("'true'", 1), ("'0'", 0), ("'off'", 0)] {
            let Value::Data { bytes, .. } =
                eval(&parse(&format!("{}::bool", text), &s).unwrap(), &r, &s).unwrap()
            else {
                panic!("null")
            };
            assert_eq!(bytes.as_slice(), &[want][..], "{}", text);
        }
        assert!(eval(&parse("'maybe'::bool", &s).unwrap(), &r, &s).is_err());
    }

    /// A float rounds into an integer with `rint`, ties to even; only a
    /// `numeric` (and text, which is read as one) rounds half away. Against
    /// PostgreSQL 17, `select 0.5::float8::int4, 1.5::float8::int4,
    /// 2.5::float8::int4, (-2.5)::float8::int8, 2.5::float4::int2,
    /// 2.5::numeric::int4` is `0|2|2|-2|2|3`. Both ways a float reaches an
    /// integer are covered: a cast, and assignment to an integer column.
    #[test]
    fn a_float_rounds_into_an_integer_ties_to_even() {
        let s = schema();
        let r = row_with(Some(1), "");
        let int_of = |v: Value| -> i64 {
            let Value::Data { bytes, db_type } = v else {
                panic!("null")
            };
            match db_type {
                DbType::I16 => i16::from_le_bytes(bytes[..].try_into().unwrap()) as i64,
                DbType::I32 => i32::from_le_bytes(bytes[..].try_into().unwrap()) as i64,
                DbType::I64 => i64::from_le_bytes(bytes[..].try_into().unwrap()),
                other => panic!("not an integer: {other:?}"),
            }
        };
        for (sql, want) in [
            ("('0.5'::float8)::int", 0),
            ("('1.5'::float8)::int", 2),
            ("('2.5'::float8)::int", 2),
            ("('3.5'::float8)::int", 4),
            ("('-2.5'::float8)::bigint", -2),
            ("('2.5'::real)::smallint", 2),
            ("('2.7'::float8)::int", 3),
            ("('-2.7'::float8)::int", -3),
            // Not a float: a numeric rounds half away. (Quoted, `'2.5'` is
            // text, which the integer input refuses, as PostgreSQL's does.)
            ("2.5::int", 3),
            ("(-2.5)::int", -3),
        ] {
            assert_eq!(int_of(eval(&parse(sql, &s).unwrap(), &r, &s).unwrap()), want, "{sql}");
        }
        assert!(eval(&parse("'2.5'::int", &s).unwrap(), &r, &s).is_err());
        // The range check is on the rounded value: 32766.5 rounds to the even
        // 32766, 32767.5 to 32768, which no smallint holds.
        assert_eq!(int_of(eval(&parse("('32766.5'::float8)::smallint", &s).unwrap(), &r, &s).unwrap()), 32766);
        assert!(eval(&parse("('32767.5'::float8)::smallint", &s).unwrap(), &r, &s).is_err());

        let f64v = |f: f64| Value::Data { bytes: f.to_le_bytes().to_vec(), db_type: DbType::F64 };
        let f32v = |f: f32| Value::Data { bytes: f.to_le_bytes().to_vec(), db_type: DbType::F32 };
        assert_eq!(int_of(to_integer_column(f64v(2.5), &DbType::I32).unwrap()), 2);
        assert_eq!(int_of(to_integer_column(f64v(-0.5), &DbType::I64).unwrap()), 0);
        assert_eq!(int_of(to_integer_column(f32v(-2.5), &DbType::I16).unwrap()), -2);
        assert_eq!(int_of(to_integer_column(f64v(1.5), &DbType::I32).unwrap()), 2);
        let decimal = Value::Data {
            bytes: (25 * 10i128.pow(crate::core::value_codec::DECIMAL_SCALE - 1)).to_le_bytes().to_vec(),
            db_type: DbType::DECIMAL,
        };
        assert_eq!(int_of(to_integer_column(decimal, &DbType::I32).unwrap()), 3, "numeric 2.5");
    }

    #[test]
    fn a_cast_inside_a_case_arm_stays_in_its_arm() {
        let schema = vec![SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false)];
        let parsed = parse("CASE WHEN id = 1 THEN 'a' ELSE 1::text END", &schema)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(parsed, Expr::Case { .. }), "{parsed:?}");
        assert_eq!(static_type(&parsed), DbType::STRING);
        // A cast after the block still applies to the block.
        let parsed = parse("CASE WHEN id = 1 THEN 1 ELSE 2 END::text", &schema).unwrap();
        assert!(matches!(parsed, Expr::Cast(_, DbType::STRING)), "{parsed:?}");
        // Mixed categories are refused.
        let err = parse("CASE WHEN id = 1 THEN id ELSE 'x'::text END", &schema).unwrap_err();
        assert!(err.ends_with("cannot be matched"), "{err}");
    }

    #[test]
    fn needs_evaluation_distinguishes_expressions_from_literals() {
        assert!(needs_evaluation("COALESCE(a, 1)"));
        assert!(needs_evaluation("CASE WHEN a = 1 THEN 2 END"));
        assert!(needs_evaluation("x::int"));
        assert!(!needs_evaluation("'plain'"));
        assert!(!needs_evaluation("42"));
        // A literal containing `::` is data, not a cast.
        assert!(!needs_evaluation("'a::b'"));
    }
}

/// A float literal meeting a NUMERIC is converted once, at parse time, and the
/// answer must be the one the per-row conversion gave.
#[cfg(test)]
mod float_literal_fold_tests {
    use super::*;
    use crate::core::row::row::Column;
    use crate::memory_pool::MEMORY_POOL;

    fn field(name: &str, ty: DbType, write_order: u64, scale: Option<u32>) -> SchemaField {
        let mut f = SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, write_order, false);
        f.scale = scale;
        f
    }

    fn schema() -> Vec<SchemaField> {
        vec![
            field("d", DbType::DECIMAL, 0, Some(2)),
            field("f", DbType::F64, 1, None),
            field("i", DbType::I32, 2, None),
            field("e", DbType::DECIMAL, 3, None),
        ]
    }

    fn row(d: i128, f: f64, i: i32, e: i128) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (id, ty, bytes) in [
            (0u64, DbType::DECIMAL, d.to_le_bytes().to_vec()),
            (1, DbType::F64, f.to_le_bytes().to_vec()),
            (2, DbType::I32, i.to_le_bytes().to_vec()),
            (3, DbType::DECIMAL, e.to_le_bytes().to_vec()),
        ] {
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(id),
                data: mb,
                column_type: ty,
            });
        }
        Row {
            position: 0,
            length: 0,
            columns,
        }
    }

    /// The expression as `parse` built it before the fold: the same tree,
    /// with every literal as written.
    fn unfolded(text: &str, schema: &[SchemaField]) -> Expr {
        match top_level_arith(text.trim()) {
            Some((at, op)) if !is_number_literal(text.trim()) => Expr::Arith(
                Box::new(unfolded(&text.trim()[..at], schema)),
                op,
                Box::new(unfolded(&text.trim()[at + 1..], schema)),
            ),
            _ => parse(text, schema).unwrap(),
        }
    }

    fn literal_types(e: &Expr, out: &mut Vec<DbType>) {
        match e {
            Expr::Literal { db_type, .. } => out.push(db_type.clone()),
            // A constant written with zeros past its value's places is held to
            // its written scale (`with_written_scale`): still that literal.
            Expr::NumericCast { value, precision: 0, .. } => literal_types(value, out),
            Expr::Arith(a, _, b) => {
                literal_types(a, out);
                literal_types(b, out);
            }
            _ => {}
        }
    }

    #[test]
    fn a_folded_literal_answers_as_the_per_row_conversion_did() {
        let s = schema();
        let literals = [
            "1.5", "0.1", "0.25", "1.0", "0.0025", "123456.789", "1.0000000005", "0.333333333333",
            "0.000000000001", "99999999.99", "100000000000000000000.5", "1000000000000000000000000000000.0",
            "3.14159265358979",
        ];
        let mut rng = fastrand::Rng::with_seed(0xf10a7);
        let mut checked = 0;
        for lit in literals {
            for template in [
                "d * {}", "d + {}", "d - {}", "d / {}", "{} * d", "{} - d", "e * {}", "d * {} + 1",
                "d + e * {}", "f * {}", "i * {}", "f + {}", "{} * f", "d * 2 * {}",
            ] {
                // A literal the engine scale cannot hold exactly is a float
                // now, and a float meets a NUMERIC as float arithmetic row by
                // row; the fold is what keeps it NUMERIC, so there is no
                // per-row conversion left to compare it with.
                if exact_engine_decimal(lit).is_none() && (template.contains('d') || template.contains('e')) {
                    continue;
                }
                let text = template.replace("{}", lit);
                let folded = parse(&text, &s).unwrap();
                let reference = unfolded(&text, &s);
                for _ in 0..40 {
                    let r = row(
                        rng.i128(-10i128.pow(14)..10i128.pow(14)),
                        (rng.f64() - 0.5) * 1e6,
                        rng.i32(-100_000..100_000),
                        rng.i128(-10i128.pow(20)..10i128.pow(20)),
                    );
                    let (a, b) = (eval(&folded, &r, &s), eval(&reference, &r, &s));
                    match (&a, &b) {
                        (Ok(x), Ok(y)) => assert_eq!(x, y, "{text}"),
                        // Both refuse alike — overflow, division by zero.
                        (Err(x), Err(y)) => assert_eq!(x, y, "{text}"),
                        _ => panic!("{text}: folded {a:?}, per row {b:?}"),
                    }
                    checked += 1;
                }
                assert_eq!(static_type(&folded), static_type(&reference), "{text}");
            }
        }
        assert!(checked > 5000);
    }

    #[test]
    fn only_a_literal_meeting_a_numeric_is_converted() {
        let s = schema();
        for (text, want) in [
            ("d * 1.5", vec![DbType::DECIMAL]),
            ("1.5 * d", vec![DbType::DECIMAL]),
            ("d * 2 * 1.5", vec![DbType::I32, DbType::DECIMAL]),
            // A written fraction is `numeric` wherever it stands, as in
            // PostgreSQL; against a float column the arithmetic is still
            // float, below.
            ("f * 1.5", vec![DbType::DECIMAL]),
            ("i * 1.5", vec![DbType::DECIMAL]),
            ("1.5 * 2.5", vec![DbType::DECIMAL, DbType::DECIMAL]),
            // Past the engine scale too, as PostgreSQL types it: carried
            // with its digits at its own scale, not as a float.
            ("f * 1.5000000000", vec![DbType::DECIMAL]),
            ("f * 0.0000000001", vec![DbType::DECIMAL]),
            ("i * (1e-10)", vec![DbType::DECIMAL]),
        ] {
            let mut got = Vec::new();
            literal_types(&parse(text, &s).unwrap(), &mut got);
            // The integer literal's own width is whatever `parse` gives it.
            got.iter_mut().for_each(|t| {
                if !matches!(t, DbType::DECIMAL | DbType::F64 | DbType::F32) {
                    *t = DbType::I32
                }
            });
            assert_eq!(got, want, "{text}");
        }
        // `float_col * 1.5` is still float arithmetic, a long constant too.
        let r = row(100, 2.0, 1, 0);
        assert!(matches!(
            eval(&parse("f * 1.5", &s).unwrap(), &r, &s).unwrap(),
            Value::Data { db_type: DbType::F64, .. }
        ));
        assert_eq!(as_f64(&eval(&parse("f * 0.0000000001", &s).unwrap(), &r, &s).unwrap()), Some(2.0e-10));
        // Against an integer, exact where a result is output, at the
        // constant's places.
        let out = eval_output(&parse("i * (1e-10)", &s).unwrap(), &r, &s).unwrap();
        assert_eq!(
            out,
            Value::Data { bytes: crate::core::value_codec::decimal_cell_with_scale(1, 10), db_type: DbType::DECIMAL }
        );
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored float_literal_fold_bench --nocapture`
    #[test]
    #[ignore]
    fn float_literal_fold_bench() {
        let s = schema();
        let rows: Vec<Row> = (0..4096i128).map(|k| row(k * 137, 0.0, 0, 0)).collect();
        for text in ["d * 1.5", "d + 0.25", "d * 1.5 + 0.25"] {
            let folded = parse(text, &s).unwrap();
            let reference = unfolded(text, &s);
            for round in 0..4 {
                let t = std::time::Instant::now();
                for r in &rows {
                    std::hint::black_box(eval(&reference, r, &s).unwrap());
                }
                let old = t.elapsed().as_nanos() as f64 / rows.len() as f64;
                let t = std::time::Instant::now();
                for r in &rows {
                    std::hint::black_box(eval(&folded, r, &s).unwrap());
                }
                let new = t.elapsed().as_nanos() as f64 / rows.len() as f64;
                println!("{text:16} round {round}: per-row conversion {old:.0} ns, folded {new:.0} ns ({:.2}x)", old / new);
            }
        }
    }
}

#[cfg(test)]
mod mixed_numeric_compare_tests {
    use super::*;
    use std::cmp::Ordering;

    fn v(db_type: DbType, bytes: Vec<u8>) -> Value {
        Value::Data { bytes, db_type }
    }
    fn i32v(x: i32) -> Value { v(DbType::I32, x.to_le_bytes().to_vec()) }
    fn i64v(x: i64) -> Value { v(DbType::I64, x.to_le_bytes().to_vec()) }
    fn i16v(x: i16) -> Value { v(DbType::I16, x.to_le_bytes().to_vec()) }
    fn u128v(x: u128) -> Value { v(DbType::U128, x.to_le_bytes().to_vec()) }
    fn i128v(x: i128) -> Value { v(DbType::I128, x.to_le_bytes().to_vec()) }
    fn u64v(x: u64) -> Value { v(DbType::U64, x.to_le_bytes().to_vec()) }
    fn f64v(x: f64) -> Value { v(DbType::F64, x.to_le_bytes().to_vec()) }
    fn f32v(x: f32) -> Value { v(DbType::F32, x.to_le_bytes().to_vec()) }
    /// Engine scale: `units` is the value times 10^9.
    fn dec(units: i128) -> Value { v(DbType::DECIMAL, units.to_le_bytes().to_vec()) }
    /// A NUMERIC carrying its own scale.
    fn cell(unscaled: i128, scale: u32) -> Value {
        v(DbType::DECIMAL, crate::core::value_codec::decimal_cell_with_scale(unscaled, scale))
    }

    #[test]
    fn mixed_widths_compare_as_numbers() {
        let cases: Vec<(Value, Value, Ordering)> = vec![
            (i64v(5), i32v(7), Ordering::Less),
            (i32v(7), i64v(5), Ordering::Greater),
            (i64v(-1), i32v(0), Ordering::Less),
            (i16v(-300), i64v(-300), Ordering::Equal),
            (i64v(i64::MAX), i32v(i32::MAX), Ordering::Greater),
            (u64v(u64::MAX), i64v(-1), Ordering::Greater),
            (u128v(u128::MAX), i128v(i128::MAX), Ordering::Greater),
            (i128v(i128::MIN), u128v(0), Ordering::Less),
            (dec(1_500_000_000), i32v(1), Ordering::Greater),
            (dec(1_000_000_000), i64v(1), Ordering::Equal),
            (dec(-1), i32v(0), Ordering::Less),
            (dec(i128::MAX), i128v(i128::MAX), Ordering::Less),
            (i64v((1 << 53) + 1), f64v((1u64 << 53) as f64), Ordering::Greater),
            (i64v(1 << 53), f64v((1u64 << 53) as f64), Ordering::Equal),
            (f64v(f64::NAN), u128v(u128::MAX), Ordering::Greater),
            (f64v(f64::INFINITY), u128v(u128::MAX), Ordering::Greater),
            (f64v(f64::NEG_INFINITY), i128v(i128::MIN), Ordering::Less),
            (f64v(-0.0), i32v(0), Ordering::Equal),
            (f64v(1e300), u128v(u128::MAX), Ordering::Greater),
            (f64v(2.5), dec(2_500_000_000), Ordering::Equal),
            (f64v(5e-324), dec(0), Ordering::Greater),
            (f64v(5e-324), dec(1), Ordering::Less),
            (f64v(-5e-324), i32v(0), Ordering::Less),
            // 0.1f32 is 0.100000001490116…, above the decimal 0.1.
            (f32v(0.1), dec(100_000_000), Ordering::Greater),
            (f32v(0.5), f64v(0.5), Ordering::Equal),
        ];
        for (a, b, want) in cases {
            assert_eq!(compare_total(&a, &b), Some(want), "{:?} vs {:?}", a, b);
            assert_eq!(compare_total(&b, &a), Some(want.reverse()), "{:?} vs {:?}", b, a);
        }
    }

    /// The operator comparator: a number against a float of another type in
    /// double precision, each case PostgreSQL 17's answer (`select
    /// 9007199254740993::int8 = 9007199254740992::float8` is true, `select
    /// 16777217::int4 = 16777216::real` false, …).
    #[test]
    fn a_number_against_a_float_compares_as_postgresql() {
        let cases: Vec<(Value, Value, Ordering)> = vec![
            (f64v(0.1), dec(100_000_000), Ordering::Equal),
            (f64v(-0.1), dec(-100_000_000), Ordering::Equal),
            (f64v(1e-9), dec(1), Ordering::Equal),
            (f64v(0.1), cell(10_000_000_000_000_001, 17), Ordering::Equal),
            (f64v("1234567890.123456789".parse().unwrap()), cell(1_234_567_890_123_456_789, 9), Ordering::Equal),
            (f64v("9007199254740.123456789".parse().unwrap()), cell(9_007_199_254_740_123_456_789, 9), Ordering::Equal),
            (f64v(1e20), cell(99_999_999_999_999_999_999_999_999_999, 9), Ordering::Equal),
            (f64v("1234567890.123456789".parse().unwrap()), cell(1_234_567_890_123_457_789, 9), Ordering::Less),
            (f32v(0.1), cell(10_000_000_149_011_612, 17), Ordering::Equal),
            (f32v(0.1), dec(100_000_000), Ordering::Greater),
            (i64v((1 << 53) + 1), f64v((1u64 << 53) as f64), Ordering::Equal),
            (i32v(16_777_217), f32v(16_777_216.0), Ordering::Greater),
            (i128v(i128::MAX), f64v(i128::MAX as f64), Ordering::Equal),
            (f64v(f64::NAN), dec(i128::MAX), Ordering::Greater),
            (f64v(f64::INFINITY), u128v(u128::MAX), Ordering::Greater),
            (f64v(-0.0), i32v(0), Ordering::Equal),
            (f64v(5e-324), dec(0), Ordering::Greater),
            (f64v(5e-324), dec(1), Ordering::Less),
            (f32v(0.5), f64v(0.5), Ordering::Equal),
        ];
        for (a, b, want) in cases {
            assert_eq!(compare(&a, &b), Some(want), "{:?} vs {:?}", a, b);
            assert_eq!(compare(&b, &a), Some(want.reverse()), "{:?} vs {:?}", b, a);
        }
        // A sort orders by the exact value, not by the operator.
        let up = [(false, None)];
        assert_eq!(
            compare_key_values(&[i64v((1 << 53) + 1)], &[f64v((1u64 << 53) as f64)], &up),
            Ordering::Greater
        );
        // Without a float on either side the operator is the exact order.
        assert_eq!(compare(&i64v((1 << 53) + 1), &dec(9_007_199_254_740_992_000_000_000)), Some(Ordering::Greater));
        assert_eq!(compare(&cell(10_000_000_000_000_001, 17), &dec(100_000_000)), Some(Ordering::Greater));
    }

    /// Antisymmetry over every pair and transitivity over every triple of a
    /// pool mixing every numeric type, at the boundaries where widening or a
    /// float conversion is lossy.
    #[test]
    fn mixed_numeric_compare_is_a_total_order() {
        let mut pool: Vec<Value> = Vec::new();
        let ints: [i64; 12] = [
            0, 1, -1, 7, -7, 127, -128, 1 << 31, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, i64::MAX,
        ];
        for &x in &ints {
            pool.push(i64v(x));
            if let Ok(y) = i32::try_from(x) { pool.push(i32v(y)); }
            if let Ok(y) = i16::try_from(x) { pool.push(i16v(y)); }
            if x >= 0 { pool.push(u64v(x as u64)); pool.push(u128v(x as u128)); }
            pool.push(i128v(x as i128));
            pool.push(f64v(x as f64));
            pool.push(f32v(x as f32));
            if let Some(u) = (x as i128).checked_mul(1_000_000_000) { pool.push(dec(u)); }
            pool.push(dec(x as i128)); // x * 10^-9
        }
        for f in [0.5, -0.5, 0.1, 1e-9, 1.000000001, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 1e30] {
            pool.push(f64v(f));
            pool.push(f32v(f as f32));
        }
        pool.push(u128v(u128::MAX));
        pool.push(i128v(i128::MIN));
        pool.push(dec(i128::MAX));
        pool.push(dec(i128::MIN));
        pool.push(dec(100_000_000));
        pool.push(dec(1_000_000_001));
        pool.push(cell(10_000_000_149_011_612, 17));
        pool.push(cell(10_000_000_000_000_001, 17));
        pool.push(cell(90_071_992_547_409_921, 1));
        pool.push(cell(1, 38));
        // Between the double 2^63's exact value (…808) and its shortest text
        // (…6000): an integer and a NUMERIC that must agree against it.
        pool.push(i128v(9_223_372_036_854_775_900));
        pool.push(cell(9_223_372_036_854_775_900, 0));

        let c = |a: &Value, b: &Value| compare_total(a, b).expect("numbers always compare");
        for a in &pool {
            for b in &pool {
                assert_eq!(c(a, b), c(b, a).reverse(), "antisymmetry {:?} {:?}", a, b);
            }
        }
        for a in &pool {
            for b in &pool {
                let ab = c(a, b);
                for x in &pool {
                    let bx = c(b, x);
                    if ab == bx || bx == Ordering::Equal {
                        assert_eq!(c(a, x), ab, "transitivity {:?} {:?} {:?}", a, b, x);
                    } else if ab == Ordering::Equal {
                        assert_eq!(c(a, x), bx, "transitivity {:?} {:?} {:?}", a, b, x);
                    }
                }
            }
        }
        // And a sort with this comparator is consistent: sorted twice from
        // different starting orders, the keys come out in the same sequence.
        let mut forward = pool.clone();
        let mut backward: Vec<Value> = pool.iter().rev().cloned().collect();
        forward.sort_by(|a, b| c(a, b));
        backward.sort_by(|a, b| c(a, b));
        for (a, b) in forward.iter().zip(&backward) {
            assert_eq!(c(a, b), Ordering::Equal);
        }
    }
}

#[cfg(test)]
mod column_value_bench {
    use super::*;
    use crate::core::row::row::Column;
    use crate::memory_pool::MEMORY_POOL;

    /// `column_value` as it was: the schema cloned into a SmallVec and the
    /// companion's name built with `format!`, once per call.
    fn column_value_reference(field: &SchemaField, row: &Row, schema: &[SchemaField]) -> Value {
        let sv: SmallVec<[SchemaField; 20]> = schema.iter().cloned().collect();
        let name = crate::core::rql::nullable::flag_column_name(&field.name);
        let flag = sv.iter().find(|f| f.name.eq_ignore_ascii_case(&name) && !f.is_deleted);
        if let Some(flag) = flag {
            let is_null = row
                .columns
                .iter()
                .find(|c| u64::from(c.schema_id) == flag.write_order)
                .map(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
                .unwrap_or(false);
            if is_null {
                return Value::Null;
            }
        }
        match row.columns.iter().find(|c| u64::from(c.schema_id) == field.write_order) {
            Some(column) if column.column_type == DbType::NULL => Value::Null,
            Some(column) => Value::Data {
                bytes: column.data.into_slice().to_vec(),
                db_type: field.db_type.clone(),
            },
            None => Value::Null,
        }
    }

    fn fixture(rows: usize) -> (Vec<SchemaField>, Vec<Row>) {
        let names: Vec<(String, DbType)> = vec![
            ("id".into(), DbType::I64),
            ("customer_name".into(), DbType::STRING),
            ("a".into(), DbType::I64),
            ("__null_a".into(), DbType::BOOL),
            ("b".into(), DbType::I64),
            ("__null_b".into(), DbType::BOOL),
            ("shipping_address".into(), DbType::STRING),
            ("created_at".into(), DbType::DATETIME),
            ("status_description".into(), DbType::STRING),
            ("updated_by_user".into(), DbType::STRING),
        ];
        let schema: Vec<SchemaField> = names
            .iter()
            .enumerate()
            .map(|(i, (n, t))| SchemaField::new(n.clone(), t.clone(), t.get_size(), 0, i as u64, false))
            .collect();
        let col = |id: u64, ty: DbType, bytes: &[u8]| {
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(bytes);
            Column { schema_id: crate::core::row::row::schema_id_of(id), data: mb, column_type: ty }
        };
        let rows = (0..rows as i64)
            .map(|i| {
                let mut columns: crate::core::row::row::RowColumns = smallvec::SmallVec::new();
                columns.push(col(2, DbType::I64, &(i % 1000).to_le_bytes()));
                columns.push(col(3, DbType::BOOL, &[0]));
                columns.push(col(4, DbType::I64, &(i % 7).to_le_bytes()));
                columns.push(col(5, DbType::BOOL, &[0]));
                Row { position: 0, length: 0, columns }
            })
            .collect();
        (schema, rows)
    }

    /// Expression-heavy projections over 1M rows.
    ///
    /// Kept for the next attempt at cheaper value bytes. Backing
    /// `Value::Data` with `SmallVec<[u8; 16]>` (no allocation for a number, a
    /// UUID or short text) measured no win against the same build with every
    /// value on the heap, interleaved 3x on 2026-10-08: allocation is not where
    /// these per-row costs are, and the inline/heap branch on every access ate
    /// the rest. So `Vec<u8>` stays. `cargo test --release -p rasterizeddb_core --lib --
    /// --ignored expression_projection_bench --nocapture`.
    #[test]
    #[ignore]
    fn expression_projection_bench() {
        use std::hint::black_box;
        let (schema, rows) = fixture(1_000_000);
        let exprs = [
            "a * b + 1",
            "COALESCE(a, 0) * 2 - b",
            "CASE WHEN a > 500 THEN a ELSE b END",
            "GREATEST(a, b, 7) + LEAST(a, b)",
        ];
        for round in 0..5 {
            let mut line = format!("round {round}:");
            for text in exprs {
                let expr = parse(text, &schema).unwrap();
                let t = std::time::Instant::now();
                let mut n = 0usize;
                for row in &rows {
                    if let Value::Data { bytes, .. } = eval(&expr, black_box(row), &schema).unwrap() {
                        n += bytes.len();
                    }
                }
                black_box(n);
                line.push_str(&format!("  [{text}] {:>8.2?}", t.elapsed()));
            }
            println!("{line}");
        }
    }

    /// `SUM(a * b)`'s per-row work over 1M rows, old `column_value` against
    /// new, interleaved. `cargo test --release -p rasterizeddb_core --lib --
    /// --ignored column_value_sum_bench --nocapture`.
    #[test]
    #[ignore]
    fn column_value_sum_bench() {
        use std::hint::black_box;
        let (schema, rows) = fixture(1_000_000);
        let a = schema[2].clone();
        let b = schema[4].clone();
        let expr = parse("a * b", &schema).unwrap();
        let sum = |v: Value| as_i64(&v).unwrap_or(0) as i128;
        for round in 0..5 {
            let t = std::time::Instant::now();
            let mut s_old = 0i128;
            for row in &rows {
                let x = column_value_reference(&a, black_box(row), &schema);
                let y = column_value_reference(&b, row, &schema);
                s_old += sum(arith(&x, ArithOp::Mul, &y).unwrap());
            }
            let old = t.elapsed();
            let t = std::time::Instant::now();
            let mut s_new = 0i128;
            for row in &rows {
                let x = column_value(&a, black_box(row), &schema);
                let y = column_value(&b, row, &schema);
                s_new += sum(arith(&x, ArithOp::Mul, &y).unwrap());
            }
            let new = t.elapsed();
            let t = std::time::Instant::now();
            let mut s_eval = 0i128;
            for row in &rows {
                s_eval += sum(eval(&expr, black_box(row), &schema).unwrap());
            }
            let full = t.elapsed();
            assert_eq!(s_old, s_new);
            assert_eq!(s_old, s_eval);
            println!("round {round}: old {old:>9.2?}  new {new:>9.2?}  eval(a*b) {full:>9.2?}");
        }
    }
}

#[cfg(test)]
mod case_nesting_tests {
    use super::*;

    /// The operators a `CASE` arm holds are the arm's: the block is one term
    /// of what surrounds it, and an inner block's `END` closes only itself.
    #[test]
    fn a_case_block_nests_like_a_parenthesis() {
        let text = "CASE WHEN a > 1 THEN 'r' || b ELSE CASE WHEN c THEN 1 END END";
        assert!(is_case_block(text));
        assert_eq!(top_level_concat(text), None);
        assert!(!is_case_block("CASE WHEN a THEN 1 END + CASE WHEN b THEN 2 END"));
        let sum = "CASE WHEN a THEN 1 END + CASE WHEN b THEN 2 END";
        assert_eq!(top_level_arith(sum).map(|(at, _)| at), Some(sum.find(" + ").unwrap() + 1));
        let concat = "'<' || CASE WHEN a THEN 'x' || 'y' END || '>'";
        assert_eq!(top_level_concat(concat), Some(concat.rfind("||").unwrap()));
        assert_eq!(case_block_end("CASE WHEN a THEN 'end' END x"), Some(26));
        assert_eq!(top_level_bitwise("a & 1"), Some((2, "&")));
        assert_eq!(top_level_bitwise("a & b | c"), Some((6, "|")));
        assert_eq!(top_level_bitwise("1 << 2 + 1"), Some((2, "<<")));
        assert_eq!(top_level_bitwise("x >> 1"), Some((2, ">>")));
        for not_bitwise in [
            "a && b", "d ?& k", "d ?| k", "a || b", "d #> '{a}'", "d #>> '{a}'", "d #- '{a}'", "d->>'k'", "a <<= b",
            "a >>= b", "a <<| b", "a |>> b", "|/ 4", "a &< b", "a &> b", "p <-> q", "(a & b)", "'a & b'", "~5",
        ] {
            assert_eq!(top_level_bitwise(not_bitwise), None, "{not_bitwise}");
        }
        assert_eq!(top_level_cast("CASE WHEN a THEN 1::text END"), None);
        assert_eq!(top_level_cast("(CASE WHEN a THEN 1 END)::text"), Some(24));
    }

    /// One literal, not two: `'DE', 'A'` opens and closes with a quote and is
    /// still a list.
    #[test]
    fn only_one_quoted_literal_reads_as_a_string() {
        assert!(is_one_literal("'DE'"));
        assert!(is_one_literal("'it''s'"));
        assert!(is_one_literal("''"));
        assert!(!is_one_literal("'DE', 'A'"));
        assert!(parse("'DE', 'A'", &[]).is_err());
        assert!(matches!(
            parse("B'1010'", &[]),
            Ok(Expr::Literal { db_type: DbType::BIT, .. })
        ));
    }
}
