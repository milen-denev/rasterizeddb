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

/// Index of the parenthesis matching the one at `open`, ignoring quoted text.
fn matching_paren(text: &str, open: usize) -> Option<usize> {
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
    cast_target(name)
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
            // A date is a timestamp at midnight here: day precision is what the
            // comparisons need, and the parser accepts a date-only literal.
            "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" => DbType::DATETIME,
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

/// Position of a top-level `::`, if the expression ends in a cast.
fn top_level_cast(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut found = None;
    let mut i = 0usize;
    while i + 1 < bytes.len() {
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
                b':' if depth == 0 && bytes[i + 1] == b':' => {
                    found = Some(i);
                    i += 1;
                }
                _ => {}
            },
        }
        i += 1;
    }
    found
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
            return Ok(Expr::Arith(
                Box::new(parse(lhs, schema)?),
                op,
                Box::new(parse(rhs, schema)?),
            ));
        }
    }

    // A trailing cast binds loosest, so it is peeled first.
    if let Some(at) = top_level_cast(text) {
        let inner = &text[..at];
        let target = &text[at + 2..];
        let db_type = cast_target(target)
            .ok_or_else(|| format!("cast to '{}' is not supported yet", target.trim()))?;
        return Ok(Expr::Cast(Box::new(parse(inner, schema)?), db_type));
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

    // A quoted string.
    if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 {
        let value = crate::core::value_codec::unquote_sql(text);
        return Ok(Expr::Literal {
            bytes: value.as_bytes().to_vec(),
            db_type: DbType::STRING,
        });
    }

    // A number.
    if let Ok(v) = text.parse::<i64>() {
        return Ok(Expr::Literal {
            bytes: v.to_le_bytes().to_vec(),
            db_type: DbType::I64,
        });
    }
    if let Ok(v) = text.parse::<f64>() {
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
                return Ok(Expr::Literal {
                    bytes: micros.to_le_bytes().to_vec(),
                    db_type: DbType::I64,
                });
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
                    Ok(Expr::Least(args))
                }
                "GREATEST" => {
                    if args.is_empty() {
                        return Err("GREATEST needs at least one argument".to_string());
                    }
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
        None => Err(format!("'{}' is neither a column nor a value", text)),
    }
}

/// `CASE WHEN <cond> THEN <expr> [WHEN …] [ELSE <expr>] END`
fn parse_case(text: &str, schema: &[SchemaField]) -> Result<Expr, String> {
    use crate::core::rql::join::find_keyword;

    let end_at = find_keyword(text, "END", 0).ok_or_else(|| "CASE has no END".to_string())?;
    if text[end_at + 3..].trim() != "" {
        return Err("trailing text after END".to_string());
    }
    let body = &text[4..end_at];

    // A `CASE <expr> WHEN …` compares against a value; only the searched form
    // is supported, and the difference is worth an explicit error.
    let first_when = find_keyword(body, "WHEN", 0).ok_or_else(|| "CASE has no WHEN".to_string())?;
    if !body[..first_when].trim().is_empty() {
        return Err(
            "CASE <expression> WHEN … is not supported yet; use CASE WHEN <condition> …"
                .to_string(),
        );
    }

    let mut whens: Vec<(Cond, Expr)> = Vec::new();
    let mut otherwise: Option<Box<Expr>> = None;
    let mut rest = &body[first_when..];

    loop {
        // rest starts at WHEN
        let after_when = &rest["WHEN".len()..];
        let then_at = find_keyword(after_when, "THEN", 0)
            .ok_or_else(|| "CASE WHEN has no THEN".to_string())?;
        let condition = parse_cond(&after_when[..then_at], schema)?;
        let after_then = &after_when[then_at + "THEN".len()..];

        let next_when = find_keyword(after_then, "WHEN", 0);
        let next_else = find_keyword(after_then, "ELSE", 0);
        let cut = match (next_when, next_else) {
            (Some(w), Some(e)) => w.min(e),
            (Some(w), None) => w,
            (None, Some(e)) => e,
            (None, None) => after_then.len(),
        };
        whens.push((condition, parse(&after_then[..cut], schema)?));

        match (next_when, next_else) {
            (Some(w), Some(e)) if w < e => {
                rest = &after_then[w..];
            }
            (Some(w), None) => {
                rest = &after_then[w..];
            }
            (_, Some(e)) => {
                otherwise = Some(Box::new(parse(&after_then[e + "ELSE".len()..], schema)?));
                break;
            }
            (None, None) => break,
        }
    }

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
                    if depth == 0 && text[i..].starts_with(op) {
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
            crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => DbType::STRING,
        },
        Expr::Cast(_, db_type) => db_type.clone(),
        Expr::Concat(_, _) => DbType::STRING,
        Expr::Call(TextFunc::ColumnSize | TextFunc::RelationSize, _) => DbType::I64,
        Expr::Call(TextFunc::DateTrunc, _) => DbType::DATETIME,
        Expr::Call(_, _) => DbType::STRING,
        // The branches of these all have to agree in SQL, so the first one that
        // is not a bare NULL decides.
        Expr::Coalesce(args) | Expr::Least(args) | Expr::Greatest(args) => args
            .iter()
            .find(|a| !matches!(a, Expr::Null))
            .map(static_type)
            .unwrap_or(DbType::STRING),
        Expr::NullIf(lhs, _) => static_type(lhs),
        Expr::Mod(lhs, _) => static_type(lhs),
        Expr::Case { whens, otherwise } => whens
            .iter()
            .map(|(_, e)| e)
            .chain(otherwise.iter().map(|b| b.as_ref()))
            .find(|e| !matches!(e, Expr::Null))
            .map(static_type)
            .unwrap_or(DbType::STRING),
        // Mirrors `arith`: a float on either side makes the result a float,
        // and everything else is done in i64.
        Expr::Arith(lhs, op, rhs) => {
            let ty = |e: &Expr| static_type(e);
            let timestamp = |e: &Expr| matches!(ty(e), DbType::DATETIME);
            let rank = |e: &Expr| match ty(e) {
                DbType::I8 | DbType::U8 => 1,
                DbType::I16 | DbType::U16 => 2,
                DbType::I32 | DbType::U32 => 3,
                _ => 4,
            };
            // A written integer constant does not widen the result. Every
            // integer literal parses as `I64` — there is nothing in `5` to say
            // otherwise — so `counter - 1` on an `int4` column came out
            // `bigint`, and a client that asked for `Option<i32>` refused the
            // value it was handed. PostgreSQL types such a literal from its
            // context, and the arithmetic below is done in `i64` and fitted to
            // the result type either way, so taking the column's rank is both
            // Postgres-shaped and safe.
            let int_literal = |e: &Expr| {
                matches!(
                    e,
                    Expr::Literal {
                        db_type: DbType::I64,
                        ..
                    }
                )
            };
            let rank = |e: &Expr| -> u8 {
                if int_literal(e) {
                    // Rank of the *other* operand, so a literal never decides.
                    let other = if std::ptr::eq(e, &**lhs) {
                        &**rhs
                    } else {
                        &**lhs
                    };
                    if int_literal(other) { 4 } else { rank(other) }
                } else {
                    rank(e)
                }
            };
            if timestamp(lhs) && timestamp(rhs) && matches!(op, ArithOp::Sub) {
                // The difference of two instants is a span, not a bigint. It
                // was described as `int8` before the type existed, and a client
                // asking to decode a duration refused the column.
                DbType::INTERVAL
            } else if timestamp(lhs) || timestamp(rhs) {
                DbType::DATETIME
            } else if matches!(ty(lhs), DbType::INTERVAL) && matches!(ty(rhs), DbType::INTERVAL) {
                DbType::INTERVAL
            } else if matches!(ty(lhs), DbType::DECIMAL) || matches!(ty(rhs), DbType::DECIMAL) {
                DbType::DECIMAL
            } else if matches!(ty(lhs), DbType::F32 | DbType::F64)
                || matches!(ty(rhs), DbType::F32 | DbType::F64)
            {
                DbType::F64
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

    // `NUMERIC` is an integer in scaled units, so it cannot share the plain
    // integer path: adding two of them works, but multiplying leaves the
    // product scaled twice and dividing loses the scale entirely.
    if is_decimal(a) || is_decimal(b) {
        return decimal_arith(a, op, b);
    }

    let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) else {
        return Err(format!("'{}' needs numeric operands", op.symbol()));
    };
    // Overflow is an error, not a wrap: a silently wrapped total is exactly the
    // wrong answer reported as success.
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
            x.checked_rem(y)
        }
    }
    .ok_or_else(|| format!("integer overflow in '{}'", op.symbol()))?;

    // PostgreSQL keeps the wider of the two operand types rather than widening
    // everything to `bigint`. Reporting `int8` for `int4 + int4` is rejected
    // outright by a typed client — the values are right, the declared type is
    // not.
    let db_type = wider_integer(a, b);
    let bytes = match narrow_integer(value, &db_type) {
        Some(bytes) => bytes,
        None => return Err(format!("value {} is out of range for {}", value, db_type)),
    };
    Ok(Value::Data { bytes, db_type })
}

/// Renders a value as the text SQL would print.
fn as_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Data { bytes, db_type } => match db_type {
            DbType::STRING | DbType::CHAR => String::from_utf8_lossy(bytes).to_string(),
            DbType::UUID => crate::core::value_codec::format_uuid(bytes),
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
        TextFunc::DateTrunc => {
            let unit = as_text(&args[0]).trim().to_ascii_lowercase();
            let Value::Data { bytes, .. } = &args[1] else {
                return Ok(Value::Null);
            };
            if bytes.len() != 8 {
                return Ok(Value::Null);
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(bytes);
            let us = i64::from_le_bytes(raw);
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

/// Offset of a top-level `||`, ignoring quotes and parentheses.
fn top_level_concat(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut found = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'|' if depth == 0 && bytes.get(i + 1) == Some(&b'|') => {
                    // The rightmost wins, so `a || b || c` groups left to right.
                    found = Some(i);
                    i += 2;
                    continue;
                }
                _ => {}
            },
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

/// Arithmetic over `NUMERIC`, which is stored as an integer scaled by
/// `DECIMAL_SCALE` decimal places.
///
/// Addition and subtraction work directly on the scaled integers. Multiplication
/// produces a doubly-scaled product and has to be divided back down; division
/// has to scale the numerator up first, or the fractional part is truncated
/// away — the same rule `AVG` follows over an exact column.
fn decimal_arith(a: &Value, op: ArithOp, b: &Value) -> Result<Value, String> {
    let factor = 10i128.pow(crate::core::value_codec::DECIMAL_SCALE);

    let scaled = |value: &Value| -> Option<i128> {
        match value {
            Value::Data {
                bytes,
                db_type: DbType::DECIMAL,
            } => Some(i128::from_le_bytes(bytes.as_slice().try_into().ok()?)),
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
        ArithOp::Mul => x.checked_mul(y).map(|product| product / factor),
        ArithOp::Div => {
            if y == 0 {
                return Err("division by zero".to_string());
            }
            x.checked_mul(factor).map(|lifted| lifted / y)
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

/// The type an integer result takes: the wider of the two operands.
fn wider_integer(a: &Value, b: &Value) -> DbType {
    let rank = |value: &Value| match value {
        Value::Data { db_type, .. } => match db_type {
            DbType::I8 | DbType::U8 => 1,
            DbType::I16 | DbType::U16 => 2,
            DbType::I32 | DbType::U32 => 3,
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
        _ => as_i64(value).map(|v| v as f64),
    }
}

/// True when the whole text is a single numeric literal, sign and exponent
/// included — so the `-` in `-5` and `1e-5` is never read as subtraction.
fn is_number_literal(text: &str) -> bool {
    text.parse::<i64>().is_ok() || text.parse::<f64>().is_ok()
}

/// True when the text is one whole `CASE … END` block.
fn is_case_block(text: &str) -> bool {
    let upper = text.to_ascii_uppercase();
    upper.starts_with("CASE") && upper.ends_with("END") && upper.len() > 7
}

/// Finds the operator a binary arithmetic expression splits at, if any.
///
/// Scans right to left so the split lands on the *last* operator of a run,
/// which is what makes `a - b - c` group as `(a - b) - c` rather than
/// `a - (b - c)`. `+`/`-` are tried first because they bind loosest; only if
/// there are none does it look for `*`, `/` or `%`.
fn top_level_arith(text: &str) -> Option<(usize, ArithOp)> {
    for pass in 0..2 {
        let bytes = text.as_bytes();
        let mut depth = 0i32;
        let mut in_quote = false;
        // Right-to-left would have to re-derive quote and paren state, so the
        // scan runs forward and keeps the last candidate it saw.
        let mut found: Option<(usize, ArithOp)> = None;
        for (i, ch) in text.char_indices() {
            if in_quote {
                if ch == '\'' {
                    in_quote = false;
                }
                continue;
            }
            match ch {
                '\'' => in_quote = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                // `->` and `->>` reach into a document. Their `-` follows a
                // column name, so every test below would take it for a
                // subtraction and split the expression through the middle of
                // the operator.
                '-' if depth == 0 && bytes.get(i + 1) == Some(&b'>') => continue,
                '+' | '-' | '*' | '/' | '%' if depth == 0 => {
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
                    // `*` in `COUNT(*)` sits inside parens, so depth covers it.
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
    let sv: SmallVec<[SchemaField; 20]> = schema.iter().cloned().collect();
    if let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, &sv) {
        let is_null = row
            .columns
            .iter()
            .find(|c| c.schema_id == flag.write_order)
            .map(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
            .unwrap_or(false);
        if is_null {
            return Value::Null;
        }
    }
    match row
        .columns
        .iter()
        .find(|c| c.schema_id == field.write_order)
    {
        // A cell carrying `NULL` as its type *is* a NULL.
        //
        // The companion lookup above finds one by name, which works while the
        // schema is a table's own. It cannot work on a synthetic schema: a
        // combined join row renames `last_queried_at` to `s__last_queried_at`
        // and its companion to `s____null_last_queried_at`, and the name the
        // lookup derives — `__null_s__last_queried_at` — is neither. So a NULL
        // reaching a post-join predicate was read as a present value holding
        // its type's zeroes, and `COALESCE(a, b)` returned those zeroes rather
        // than falling through to `b`.
        //
        // The type marker is the other half of the same fact and does not
        // depend on names, so honouring it covers the case the lookup cannot
        // reach. Both stay: the companion is what a scan of a real table
        // carries, and the marker is what a rebuilt row carries.
        Some(column) if column.column_type == DbType::NULL => Value::Null,
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

/// Evaluates `expr` for one row.
pub fn eval(expr: &Expr, row: &Row, schema: &[SchemaField]) -> Result<Value, String> {
    Ok(match expr {
        Expr::Null => Value::Null,
        Expr::Literal { bytes, db_type } => Value::Data {
            bytes: bytes.clone(),
            db_type: db_type.clone(),
        },
        Expr::Column(field) => column_value(field, row, schema),
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
                arith(&a, *op, &b)?
            }
        }
        Expr::Scalar(func) => {
            let (_, db_type) = crate::core::rql::aggregate::ScalarFunc::parse(match func {
                crate::core::rql::aggregate::ScalarFunc::Now => "now",
                crate::core::rql::aggregate::ScalarFunc::GenRandomUuid => "gen_random_uuid",
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
                    return Ok(value);
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
                left
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
            best.unwrap_or(Value::Null)
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
                    return eval(result, row, schema);
                }
            }
            match otherwise {
                Some(e) => return eval(e, row, schema),
                None => Value::Null,
            }
        }
        Expr::Cast(inner, target) => {
            let value = eval(inner, row, schema)?;
            cast(value, target)?
        }
    })
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
        Cmp::IsNull | Cmp::IsNotNull => unreachable!("handled above"),
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

fn cast(value: Value, target: &DbType) -> Result<Value, String> {
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
            DbType::F32 => trimmed
                .parse::<f32>()
                .ok()
                .map(|v| v.to_le_bytes().to_vec()),
            DbType::F64 => trimmed
                .parse::<f64>()
                .ok()
                .map(|v| v.to_le_bytes().to_vec()),
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
            None => Err(format!("'{}' cannot be read as {:?}", text, target)),
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

    // Between numeric widths.
    let value = Value::Data {
        bytes,
        db_type: db_type.clone(),
    };
    if let Some(n) = as_i64(&value) {
        let encoded = match target {
            DbType::I8 => (n as i8).to_le_bytes().to_vec(),
            DbType::I16 => (n as i16).to_le_bytes().to_vec(),
            DbType::I32 => (n as i32).to_le_bytes().to_vec(),
            DbType::I64 => n.to_le_bytes().to_vec(),
            DbType::U8 => (n as u8).to_le_bytes().to_vec(),
            DbType::U16 => (n as u16).to_le_bytes().to_vec(),
            DbType::U32 => (n as u32).to_le_bytes().to_vec(),
            DbType::U64 => (n as u64).to_le_bytes().to_vec(),
            DbType::F32 => (n as f32).to_le_bytes().to_vec(),
            DbType::F64 => (n as f64).to_le_bytes().to_vec(),
            _ => {
                return Err(format!(
                    "cast from {:?} to {:?} is not supported yet",
                    db_type, target
                ));
            }
        };
        return Ok(Value::Data {
            bytes: encoded,
            db_type: target.clone(),
        });
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
            schema_id,
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
        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
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
    fn a_quoted_number_is_accepted_where_a_number_is_expected() {
        // A bound parameter can reach the parser quoted, and PostgreSQL casts
        // it. Refusing it made `make_interval(secs => $3)` fail only over the
        // extended protocol, which is the only way an ORM sends it.
        let s = schema();
        let r = row_with(Some(1), "");
        let e = parse("make_interval(secs => '120')", &s).unwrap();
        assert_eq!(as_i64(&eval(&e, &r, &s).unwrap()), Some(120_000_000));
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
        let v = eval(&parse("v + 1", &s).unwrap(), &r, &s).unwrap();
        // `v` is I32 and `1` parses as I64, so this is the widening case.
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
        assert_eq!(i16::from_le_bytes(bytes.try_into().unwrap()), 300);

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
        assert_eq!(i32::from_le_bytes(bytes.try_into().unwrap()), 42);
        // Out of range is an error, not a wrap.
        assert!(eval(&parse("'300'::smallint", &s).unwrap(), &r, &s).is_ok());
        assert!(eval(&parse("'70000'::smallint", &s).unwrap(), &r, &s).is_err());
        // `real` is float4, not float8.
        let Value::Data { bytes, db_type } = eval(&parse("v::real", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F32);
        assert_eq!(f32::from_le_bytes(bytes.try_into().unwrap()), 1.0);
        // A fractional literal reaches a float target.
        let Value::Data { bytes, db_type } =
            eval(&parse("'2.5'::float8", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F64);
        assert_eq!(f64::from_le_bytes(bytes.try_into().unwrap()), 2.5);
        // A float SOURCE reaches another width through the text fallback —
        // the path `AVG(x)::real` takes, since no direct f64→f32 arm exists.
        let Value::Data { bytes, db_type } =
            eval(&parse("('2.5'::float8)::real", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(db_type, DbType::F32);
        assert_eq!(f32::from_le_bytes(bytes.try_into().unwrap()), 2.5);
        // A fractional value ROUNDS into an integer target, half away from
        // zero, as PostgreSQL's numeric cast does — the billing rollup hands
        // AVG's '1092.75' to ::bigint. Range still checked after rounding.
        let Value::Data { bytes, .. } =
            eval(&parse("'1092.75'::bigint", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(i64::from_le_bytes(bytes.try_into().unwrap()), 1093);
        let Value::Data { bytes, .. } = eval(&parse("'2.5'::int", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(
            i32::from_le_bytes(bytes.try_into().unwrap()),
            3,
            "half away from zero"
        );
        let Value::Data { bytes, .. } = eval(&parse("'-2.5'::int", &s).unwrap(), &r, &s).unwrap()
        else {
            panic!("null")
        };
        assert_eq!(
            i32::from_le_bytes(bytes.try_into().unwrap()),
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
            assert_eq!(bytes, vec![want], "{}", text);
        }
        assert!(eval(&parse("'maybe'::bool", &s).unwrap(), &r, &s).is_err());
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
