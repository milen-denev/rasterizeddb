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
}

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
    /// `EXTRACT(EPOCH FROM x)` — a field of an instant or a span.
    ///
    /// The field is carried as the first argument, a string literal, because
    /// `Expr::Call` has nowhere else to put it.
    Extract,
    /// `to_tsvector(config, document)` — a document reduced to its lexemes.
    ToTsVector,
    /// `to_tsquery(config, query)` — a boolean query over lexemes.
    ToTsQuery,
    /// `plainto_tsquery(config, text)` — every word of the text, `AND`ed.
    PlainToTsQuery,
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
    if crate::core::jsonb::parse_path_expression(trimmed).is_some() {
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
    match cast_target(name)? {
        DbType::DATE => Some(DbType::DATETIME),
        other => Some(other),
    }
}

/// The SQL type a `::` cast names.
fn cast_target(name: &str) -> Option<DbType> {
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
            "BOOL" | "BOOLEAN" => DbType::BOOL,
            "UUID" => DbType::UUID,
            "TIMESTAMP" | "TIMESTAMPTZ" => DbType::DATETIME,
            // A real date, not a timestamp at midnight: `date + 3` is three
            // days, and a timestamp plus 3 was three microseconds — the
            // engine's interval unit — rendered as `00:00:00.000003+00`.
            "DATE" => DbType::DATE,
            "NUMERIC" | "DECIMAL" => DbType::DECIMAL,
            "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DbType::F64,
            // `real` IS float4 — mapping it to F64 made the described type float8,
            // and a client decoding the column as f32 then refused the row.
            "REAL" | "FLOAT4" => DbType::F32,
            "JSON" | "JSONB" => DbType::JSONB,
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
fn cast_to(inner: Expr, target: &str) -> Result<Expr, String> {
    let db_type = cast_target(target)
        .ok_or_else(|| format!("cast to '{}' is not supported yet", target.trim()))?;
    let cast = Expr::Cast(Box::new(inner), db_type.clone());
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
                b'(' => {
                    depth[i] = Some(level);
                    level += 1;
                }
                b')' => {
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
    if start != 0 || depth != 0 || !text[..4].eq_ignore_ascii_case("CASE") {
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
    "UUID_EXTRACT_TIMESTAMP", "UUID_EXTRACT_VERSION", "ABS", "MOD",
];

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

    // A path into a document binds tighter than any operator below — it is a
    // postfix on a column, not a binary operator over expressions — so it is
    // recognised first. `parse_path_expression` returns `None` for anything
    // that is not one, which is the usual case.
    if let Some(path) = crate::core::jsonb::parse_path_expression(text) {
        if let Some(column) = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(&path.column) && !f.is_deleted)
        {
            return Ok(Expr::JsonPath {
                column: column.clone(),
                steps: path.steps,
                as_text: path.as_text,
            });
        }
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
    // it used to be gave 20.169999999999998. Only a constant the engine scale
    // holds exactly is one, though; past nine places, or too large, it stays a
    // float. Rounding it to nine places lost what a float would have kept —
    // `1e-10` became 0 in a float column, and `f * 0.0000000001` was 0 — and
    // rounded it a second time when it was then stored at a column's scale,
    // so 20.1649999999 became 20.165 and then 20.17.
    if let Ok(v) = text.parse::<f64>() {
        if text.starts_with(|c: char| c.is_ascii_digit() || matches!(c, '.' | '-' | '+')) {
            if let Some(units) = exact_engine_decimal(text) {
                return Ok(Expr::Literal {
                    bytes: units.to_le_bytes().to_vec(),
                    db_type: DbType::DECIMAL,
                });
            }
            // The float stands for the written number only when it holds
            // every digit of it: `123456789012345678901234567890123456.50`
            // was the float 1.2345678901234568e35 and was stored as
            // 123456789012345680000000000000000000.00.
            if text.bytes().any(|b| b.is_ascii_digit()) && !float_holds_literal(text, v) {
                return Err(inexact_literal(text));
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
                if field != "EPOCH" {
                    // Refused by name rather than approximated: the calendar
                    // fields need a civil-date decomposition this does not have,
                    // and a plausible wrong number is worse than a refusal.
                    return Err(format!(
                        "EXTRACT({} FROM …) is not supported yet; only EPOCH is",
                        field
                    ));
                }
                return Ok(Expr::Call(
                    TextFunc::Extract,
                    vec![
                        Expr::Literal {
                            bytes: field.into_bytes(),
                            db_type: DbType::STRING,
                        },
                        source,
                    ],
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

            let args: Vec<Expr> = split_args(args_text)
                .iter()
                .map(|a| parse(a, schema))
                .collect::<Result<_, _>>()?;

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
                    Ok(Expr::Least(args))
                }
                "GREATEST" => {
                    if args.is_empty() {
                        return Err("GREATEST needs at least one argument".to_string());
                    }
                    check_branch_types("GREATEST", args.iter())?;
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
                "TO_TSVECTOR" | "TO_TSQUERY" | "PLAINTO_TSQUERY" => {
                    if args.len() != 2 {
                        return Err(format!(
                            "{} takes a configuration and a text argument, as in                              {}('english', notes)",
                            upper_name.to_ascii_lowercase(),
                            upper_name.to_ascii_lowercase()
                        ));
                    }
                    let func = match upper_name.as_str() {
                        "TO_TSVECTOR" => TextFunc::ToTsVector,
                        "TO_TSQUERY" => TextFunc::ToTsQuery,
                        _ => TextFunc::PlainToTsQuery,
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
                "MOD" => {
                    if args.len() != 2 {
                        return Err("MOD takes exactly two arguments".to_string());
                    }
                    let mut it = args.into_iter();
                    Ok(Expr::Mod(
                        Box::new(it.next().expect("checked")),
                        Box::new(it.next().expect("checked")),
                    ))
                }
                _ => {
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
                let ord = compare(av, bv).unwrap_or(Ordering::Equal);
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
        // PostgreSQL separates these three by *when in the transaction* the
        // clock is read, which this engine does not model yet; they are all the
        // wall clock here, on the same terms `column_default` already accepts
        // them for.
        "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "TRANSACTION_TIMESTAMP" => Some(Expr::Scalar(now)),
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
    if !text[4..first_when].trim().is_empty() {
        return Err(
            "CASE <expression> WHEN … is not supported yet; use CASE WHEN <condition> …"
                .to_string(),
        );
    }

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
        let condition = parse_cond(&text[after..then_at], schema)?;
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
    if text.to_ascii_uppercase().starts_with("NOT ") {
        return Err("NOT in a condition is not supported yet".to_string());
    }
    Ok(Cond::Cmp(parse_condition(text, schema)?))
}

/// Splits on every top-level occurrence of a keyword, outside quotes and
/// parentheses. `None` when the keyword is absent or a side would be empty.
fn split_on_keyword(text: &str, keyword: &str) -> Option<Vec<String>> {
    use crate::core::rql::join::find_keyword;
    let mut parts: Vec<String> = Vec::new();
    let mut start = 0usize;
    let mut search = 0usize;
    while let Some(at) = find_keyword(text, keyword, search) {
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

pub fn parse_condition(text: &str, schema: &[SchemaField]) -> Result<Condition, String> {
    use crate::core::rql::join::find_keyword;

    let text = text.trim();
    if let Some(at) = find_keyword(text, "IS", 0) {
        let tail = text[at + 2..].trim().to_ascii_uppercase();
        let left = Box::new(parse(&text[..at], schema)?);
        if tail == "NULL" {
            return Ok(Condition {
                left,
                cmp: Cmp::IsNull,
                right: None,
            });
        }
        if tail == "NOT NULL" {
            return Ok(Condition {
                left,
                cmp: Cmp::IsNotNull,
                right: None,
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
            let left = Box::new(parse(&text[..at], schema)?);
            let right = Box::new(parse(&text[at + token.len()..], schema)?);
            return Ok(Condition {
                left,
                cmp,
                right: Some(right),
            });
        }
    }
    Err(format!("condition '{}' is not a comparison", text))
}

/// Position of a top-level operator, outside quotes and parentheses.
fn find_operator(text: &str, op: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i + op.len() <= bytes.len() {
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
                b')' => depth = depth.saturating_sub(1),
                _ => {
                    // Compared as bytes: `i` steps through every byte, and
                    // slicing the text at one inside a multi-byte character
                    // (`naïve`) panicked.
                    if depth == 0 && bytes[i..].starts_with(op.as_bytes()) {
                        // `::` is a cast, not two comparisons.
                        if op == "=" && i > 0 && matches!(bytes[i - 1], b'<' | b'>' | b'!') {
                            i += 1;
                            continue;
                        }
                        return Some(i);
                    }
                }
            },
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
            Expr::NumericCast { value, .. } => walk(value, out),
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
    let target = args.iter().find_map(|a| match a {
        Expr::Column(field) if field.db_type != DbType::STRING => Some(field.db_type.clone()),
        Expr::Cast(_, db_type) if *db_type != DbType::STRING => Some(db_type.clone()),
        _ => None,
    });
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
            *db_type = target.clone();
        }
    }
}

pub fn static_type(expr: &Expr) -> DbType {
    match expr {
        Expr::Null => DbType::STRING,
        Expr::Literal { db_type, .. } => db_type.clone(),
        Expr::Column(field) => field.db_type.clone(),
        // `->>` is text by definition; `->` stays a document.
        Expr::JsonPath { as_text, .. } => {
            if *as_text {
                DbType::STRING
            } else {
                DbType::JSONB
            }
        }
        Expr::Scalar(func) => match func {
            crate::core::rql::aggregate::ScalarFunc::Now => DbType::DATETIME,
            crate::core::rql::aggregate::ScalarFunc::CurrentDate => DbType::DATETIME,
            crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => DbType::STRING,
            crate::core::rql::aggregate::ScalarFunc::UuidV7 => DbType::UUID,
        },
        Expr::Cast(_, db_type) => db_type.clone(),
        Expr::NumericCast { .. } => DbType::DECIMAL,
        Expr::Concat(lhs, rhs) if concat_is_jsonb(lhs, rhs) => DbType::JSONB,
        Expr::Concat(_, _) => DbType::STRING,
        Expr::Call(TextFunc::ColumnSize | TextFunc::RelationSize, _) => DbType::I64,
        Expr::Call(TextFunc::DateTrunc, _) => DbType::DATETIME,
        // `double precision`, which is what PostgreSQL's own `EXTRACT` returned
        // before it moved to `numeric`, and what every driver decodes without a
        // decimal type.
        Expr::Call(TextFunc::Extract, _) => DbType::F64,
        Expr::Call(TextFunc::UuidExtractTimestamp, _) => DbType::DATETIME,
        Expr::Call(TextFunc::UuidExtractVersion, _) => DbType::I16,
        // `integer`, as PostgreSQL's length family returns.
        Expr::Call(TextFunc::Length | TextFunc::OctetLength, _) => DbType::I32,
        // The argument's own type, as PostgreSQL's `abs` family returns.
        Expr::Call(TextFunc::Abs, args) => args.first().map(static_type).unwrap_or(DbType::I32),
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
fn arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
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
        let sign = if matches!(op, ArithOp::Add) { 1 } else { -1 };
        let sum = crate::core::pg_types::Interval {
            months: x.months + sign * y.months,
            days: x.days + sign * y.days,
            micros: x.micros + sign as i64 * y.micros,
        };
        return Ok(Value::Data {
            bytes: crate::core::pg_types::encode_interval(sum),
            db_type: DbType::INTERVAL,
        });
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
                bytes: crate::core::pg_types::encode_interval(crate::core::pg_types::Interval {
                    months: 0,
                    days: 0,
                    micros: value,
                }),
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

fn as_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Data { bytes, db_type } => match db_type {
            DbType::STRING | DbType::CHAR => String::from_utf8_lossy(bytes).to_string(),
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
    ) && args.iter().any(|a| a.is_null())
    {
        return Ok(Value::Null);
    }

    Ok(match func {
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
        TextFunc::Extract => {
            let Value::Data { bytes, db_type } = &args[1] else {
                return Ok(Value::Null);
            };
            // An instant and a span are stored differently: a timestamp is
            // eight bytes of little-endian microseconds, while an interval is
            // PostgreSQL's own sixteen-byte wire layout, big-endian, carrying
            // months and days separately because neither is a fixed number of
            // microseconds. Reading the second as the first is what made
            // `EXTRACT(EPOCH FROM (a - b))` come back NULL.
            let seconds = match db_type {
                DbType::INTERVAL => {
                    let Some(span) = crate::core::pg_types::decode_interval(bytes) else {
                        return Ok(Value::Null);
                    };
                    // PostgreSQL's `EPOCH` of an interval counts a month as 30
                    // days, which is the one place it does treat a month as
                    // fixed — the result is a duration, not a calendar move.
                    span.micros as f64 / 1_000_000.0
                        + span.days as f64 * 86_400.0
                        + span.months as f64 * 30.0 * 86_400.0
                }
                // A date is its midnight, as PostgreSQL casts it. Its four
                // bytes failed the width check below and came back NULL.
                DbType::DATE => match instant_of(db_type, bytes) {
                    Some(us) => us as f64 / 1_000_000.0,
                    None => return Ok(Value::Null),
                },
                _ => {
                    if bytes.len() != 8 {
                        return Ok(Value::Null);
                    }
                    let mut raw = [0u8; 8];
                    raw.copy_from_slice(bytes);
                    i64::from_le_bytes(raw) as f64 / 1_000_000.0
                }
            };
            Value::Data {
                bytes: seconds.to_le_bytes().to_vec(),
                db_type: DbType::F64,
            }
        }
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

        // `array_length(a, 1)` is NULL for an empty array, as in PostgreSQL —
        // not zero, which is the guess that makes an emptiness test read the
        // wrong way round.
        TextFunc::ArrayLength => {
            let Value::Data { bytes, .. } = &args[0] else {
                return Ok(Value::Null);
            };
            let dimension = as_text(&args[1]).trim().parse::<i32>().unwrap_or(1);
            if dimension != 1 {
                // One storage type covers every element type, and it records
                // one dimension. A higher dimension is unknown rather than
                // zero.
                return Ok(Value::Null);
            }
            let Some(text) = crate::core::value_codec::format_array(bytes) else {
                return Ok(Value::Null);
            };
            let count = array_element_count(&text);
            if count == 0 {
                return Ok(Value::Null);
            }
            Value::Data {
                bytes: (count as i32).to_le_bytes().to_vec(),
                db_type: DbType::I32,
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
                (TextFunc::Length, DbType::STRING | DbType::CHAR) => match std::str::from_utf8(bytes) {
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
        TextFunc::ArrayNdims => {
            let Value::Data { bytes, .. } = &args[0] else {
                return Ok(Value::Null);
            };
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            Value::Data {
                bytes: 1i32.to_le_bytes().to_vec(),
                db_type: DbType::I32,
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
    let float = |e: &Expr| matches!(e, Expr::Literal { db_type: DbType::F64, .. });
    if !(float(l) || float(r)) || !matches!(op, ArithOp::Add | ArithOp::Sub | ArithOp::Mul) {
        return None;
    }
    let value = |e: &Expr| match e {
        Expr::Literal { bytes, db_type } => exact_decimal(&Value::Data { bytes: bytes.clone(), db_type: db_type.clone() }),
        _ => None,
    };
    let (a, b) = (value(l)?, value(r)?);
    let Some((m, e)) = exact_decimal_arith(a, op, b) else {
        return Some(Err(inexact_literal(text)));
    };
    let scale = crate::core::value_codec::DECIMAL_SCALE as i32;
    if m == 0 || e >= -scale {
        if let Some(units) = 10i128.checked_pow((e + scale).max(0) as u32).and_then(|p| m.checked_mul(p)) {
            return Some(Ok(Expr::Literal {
                bytes: if m == 0 { 0i128 } else { units }.to_le_bytes().to_vec(),
                db_type: DbType::DECIMAL,
            }));
        }
    }
    let written = format!("{m}e{e}");
    match written.parse::<f64>() {
        Ok(v) if float_holds_literal(&written, v) => Some(Ok(Expr::Literal {
            bytes: v.to_le_bytes().to_vec(),
            db_type: DbType::F64,
        })),
        _ => Some(Err(inexact_literal(text))),
    }
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

/// The float answer of arithmetic between a written constant the engine
/// carries as a float (`lhs` or `rhs`) and an integer or a `NUMERIC`, which
/// PostgreSQL computes in `numeric`: kept when it is exactly the `numeric`
/// answer, refused (22003) when it is not. `i + 1e30` answered 1e30 for
/// every `i`, stored as if exact. Against a float, or for `/` and `%`, the
/// float's answer stands, as before.
fn checked_float_literal_arith(lhs: &Expr, a: &Value, op: ArithOp, rhs: &Expr, b: &Value, got: Value) -> Result<Value, String> {
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
            "numeric literal arithmetic {} {} {} is out of range for exact arithmetic",
            as_text(a),
            op.symbol(),
            as_text(b)
        )
    };
    let exact = exact_decimal(a)
        .zip(exact_decimal(b))
        .and_then(|(x, y)| exact_decimal_arith(x, op, y))
        .ok_or_else(refused)?;
    if exact_decimal(&got) == Some(exact) || (exact.0 == 0 && as_f64(&got) == Some(0.0)) {
        Ok(got)
    } else {
        Err(refused())
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
            if let Some(units) = as_f64(&value).and_then(float_to_engine_decimal) {
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
            if e.contains(" cannot be read as ") {
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

fn as_f64(value: &Value) -> Option<f64> {
    let Value::Data { bytes, db_type } = value else {
        return None;
    };
    match db_type {
        DbType::F32 => Some(f32::from_le_bytes(bytes.as_slice().try_into().ok()?) as f64),
        DbType::F64 => Some(f64::from_le_bytes(bytes.as_slice().try_into().ok()?)),
        // Through its text, which `str::parse` reads to the nearest float: a
        // division by the scale would round twice.
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
                            || b == b'"'
                            || b == b'\''
                            || b == b'.');
                    if binds {
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
        Some(column) => Value::Data {
            bytes: column.data.into_slice().to_vec(),
            db_type: field.db_type.clone(),
        },
        None => Value::Null,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
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

/// Orders two values of the same type, for LEAST/GREATEST and comparisons.
pub(crate) fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
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
        DbType::DECIMAL => signed(
            i128::from_le_bytes(b.try_into().ok()?),
            crate::core::value_codec::DECIMAL_SCALE,
        ),
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
            // A path that resolves to nothing is NULL, and so is a JSON null
            // read as text — both are PostgreSQL's answers, and neither is an
            // error: asking for a key a document does not have is ordinary.
            let Some(found) = crate::core::jsonb::get_path(&document, steps) else {
                return Ok(Value::Null);
            };
            if *as_text {
                match crate::core::jsonb::as_text(found) {
                    Some(text) => Value::Data {
                        bytes: text.into_bytes(),
                        db_type: DbType::STRING,
                    },
                    None => Value::Null,
                }
            } else {
                // `->` yields a document, so the bytes stay encoded and the
                // value can be reached into again or rendered as JSON.
                Value::Data {
                    bytes: found.to_vec(),
                    db_type: DbType::JSONB,
                }
            }
        }
        Expr::Concat(lhs, rhs) => {
            let a = eval(lhs, row, schema)?;
            let b = eval(rhs, row, schema)?;
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
                let mut text = as_text(&a);
                text.push_str(&as_text(&b));
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
                checked_float_literal_arith(lhs, &a, *op, rhs, &b, got)?
            }
        }
        Expr::Scalar(func) => {
            let (_, db_type) = crate::core::rql::aggregate::ScalarFunc::parse(match func {
                crate::core::rql::aggregate::ScalarFunc::Now => "now",
                crate::core::rql::aggregate::ScalarFunc::CurrentDate => "current_date",
                crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => "gen_random_uuid",
                crate::core::rql::aggregate::ScalarFunc::UuidV7 => "uuidv7",
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
        Expr::Cast(inner, target) => {
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
        Expr::NumericCast {
            value,
            precision,
            scale,
        } => {
            use crate::core::value_codec::DECIMAL_SCALE;
            let Value::Data { bytes, .. } = eval(value, row, schema)? else {
                return Ok(Value::Null);
            };
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
    let left = eval(&condition.left, row, schema)?;
    match condition.cmp {
        Cmp::IsNull => return Ok(left.is_null()),
        Cmp::IsNotNull => return Ok(!left.is_null()),
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
            return Ok(match condition.cmp {
                Cmp::IsTrue => truth == Some(true),
                Cmp::IsNotTrue => truth != Some(true),
                Cmp::IsFalse => truth == Some(false),
                _ => truth != Some(false),
            });
        }
        _ => {}
    }
    let right = match &condition.right {
        Some(e) => eval(e, row, schema)?,
        None => return Err("comparison has no right-hand side".to_string()),
    };
    // A comparison with NULL is unknown, which is not true.
    if left.is_null() || right.is_null() {
        return Ok(false);
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
        return Ok(false);
    };
    Ok(match condition.cmp {
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
    })
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

    // Text in, typed out: the literal is re-read as the target type. The
    // numeric and boolean targets are parsed here — `encode_literal` covers
    // only the specialty types (UUID, timestamps, decimals, documents), so
    // `'42'::int` used to be refused as unreadable. Parsing with the target's
    // own width also makes an out-of-range literal an error rather than a
    // silent wrap.
    if db_type == DbType::STRING {
        let text = String::from_utf8_lossy(&bytes).to_string();
        let trimmed = text.trim();
        // A fractional value reaching an integer target ROUNDS, half away
        // from zero, as PostgreSQL's numeric → integer cast does —
        // `AVG(bytes)::bigint` hands '1092.75' to this cast, and refusing it
        // failed the billing rollup outright. The bound check survives: a
        // rounded value outside the target's range is still an error, not a
        // wrap.
        fn rounded(text: &str, min: i128, max: i128) -> Option<i128> {
            let v = text.parse::<f64>().ok()?;
            if !v.is_finite() {
                return None;
            }
            let r = v.round();
            if r < min as f64 || r > max as f64 {
                return None;
            }
            Some(r as i128)
        }
        fn int_bytes<const N: usize>(
            text: &str,
            min: i128,
            max: i128,
            encode: impl Fn(i128) -> [u8; N],
        ) -> Option<Vec<u8>> {
            // The exact form first, so a value near the edge is not routed
            // through a lossy f64; the rounded form only for fractions.
            if let Ok(v) = text.parse::<i128>() {
                return (min..=max).contains(&v).then(|| encode(v).to_vec());
            }
            rounded(text, min, max).map(|v| encode(v).to_vec())
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
            None => Err(format!("{} cannot be read as {:?}", crate::core::rql::executor::sql_literal(&text), target)),
        };
    }

    // Typed in, text out.
    if *target == DbType::STRING {
        let rendered = crate::core::rql::subquery::value_to_literal(&db_type, &bytes)
            .ok_or_else(|| format!("{:?} cannot be rendered as text", db_type))?;
        let unquoted = crate::core::value_codec::unquote_sql(&rendered).to_string();
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
        // `v + 1` used to belong here. Arithmetic is supported now, so the
        // stand-in is an operator that still is not.
        assert!(parse("v ^ 1", &s).is_err());
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

        // NOT is refused, not folded to two-valued logic.
        assert!(parse("CASE WHEN NOT v = 1 THEN 1 ELSE 0 END", &s).is_err());
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
        // A fractional value ROUNDS into an integer target, half away from
        // zero, as PostgreSQL's numeric cast does — the billing rollup hands
        // AVG's '1092.75' to ::bigint. Range still checked after rounding.
        let Value::Data { bytes, .. } =
            eval(&parse("'1092.75'::bigint", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(i64::from_le_bytes(bytes[..].try_into().unwrap()), 1093);
        let Value::Data { bytes, .. } = eval(&parse("'2.5'::int", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(
            i32::from_le_bytes(bytes[..].try_into().unwrap()),
            3,
            "half away from zero"
        );
        let Value::Data { bytes, .. } = eval(&parse("'-2.5'::int", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(
            i32::from_le_bytes(bytes[..].try_into().unwrap()),
            -3,
            "half away from zero"
        );
        assert!(eval(&parse("'32767.4'::smallint", &s).unwrap(), &r, &s).is_ok());
        assert!(
            eval(&parse("'32767.6'::smallint", &s).unwrap(), &r, &s).is_err(),
            "rounds out of range"
        );
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
            // Not a float: half away, unchanged.
            ("'2.5'::int", 3),
        ] {
            assert_eq!(int_of(eval(&parse(sql, &s).unwrap(), &r, &s).unwrap()), want, "{sql}");
        }
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
            // Only while the engine scale holds it exactly; trailing zeros
            // past it are not places. Past it, a float keeps what rounding to
            // nine places threw away.
            ("f * 1.5000000000", vec![DbType::DECIMAL]),
            ("f * 0.0000000001", vec![DbType::F64]),
            ("i * (1e-10)", vec![DbType::F64]),
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
        // `float_col * 1.5` is still float arithmetic.
        let r = row(100, 2.0, 1, 0);
        assert!(matches!(
            eval(&parse("f * 1.5", &s).unwrap(), &r, &s).unwrap(),
            Value::Data { db_type: DbType::F64, .. }
        ));
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
            assert_eq!(compare(&a, &b), Some(want), "{:?} vs {:?}", a, b);
            assert_eq!(compare(&b, &a), Some(want.reverse()), "{:?} vs {:?}", b, a);
        }
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

        let c = |a: &Value, b: &Value| compare(a, b).expect("numbers always compare");
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
