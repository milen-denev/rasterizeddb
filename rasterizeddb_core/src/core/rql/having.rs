//! `HAVING`: a boolean over aggregate comparisons, shared by the single-table
//! planner (`lexer_query`) and the join planner (`join`).
//!
//! The clause used to be one comparison by construction — everything after the
//! first operator was the literal — so `HAVING COUNT(*) > 1 AND SUM(x) > 5`
//! compared `COUNT(*)` against the text `1 AND SUM(x) > 5`. That text did not
//! coerce, and an uncoercible literal *kept the group*: every group came back
//! and the statement reported success. Here the clause is parsed as a tree
//! (`AND`/`OR`/`NOT`, parentheses, `BETWEEN`, `IN`, `IS [NOT] NULL`), every
//! literal is coerced against the aggregate it meets **at plan time**, and a
//! literal that does not coerce fails the statement.
//!
//! # API
//!
//! [`parse`] takes the clause text and a callback that resolves one aggregate
//! call's text (`SUM(x)`, `COUNT(*) FILTER (WHERE …)`) to a
//! [`SelectItem::Aggregate`]. That callback is the only planner-specific part:
//! the single-table planner hands the text to its select-item parser, a join
//! planner resolves the argument against its relations. It returns a
//! [`HavingSplit`]:
//!
//! * `having` — the conjuncts that test an aggregate, as one [`Having`] the fold
//!   evaluates per group. `None` when no conjunct reads an aggregate.
//! * `row_terms` — top-level `AND` conjuncts that read **no** aggregate
//!   (`HAVING status = 'x'`). These are the caller's to place: when every
//!   column a term names is a *plain* `GROUP BY` column, the term holds for a
//!   group exactly when it holds for each of the group's rows (they share the
//!   column's bytes), so it can be moved into `WHERE`. That is not true of a
//!   computed key, nor of a query with no `GROUP BY` (whose single group exists
//!   even with no rows) — the caller must refuse those.
//!
//! A conjunct that mixes a plain column with an aggregate (`status = 'a' OR
//! COUNT(*) > 1`) is refused by name rather than half-evaluated.

use std::cmp::Ordering;

use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;
use crate::core::rql::aggregate::{AggFunc, HavingOp, SelectItem};

/// The aggregate part of a `HAVING` clause.
#[derive(Debug, Clone)]
pub struct Having {
    /// Every aggregate the condition reads, each a [`SelectItem::Aggregate`]
    /// folded per group exactly like a select-list aggregate (including its
    /// `FILTER` and a computed argument).
    pub aggregates: Vec<SelectItem>,
    pub cond: HavingCond,
}

/// A boolean over aggregate values, evaluated with SQL's three-valued logic.
#[derive(Debug, Clone)]
pub enum HavingCond {
    And(Box<HavingCond>, Box<HavingCond>),
    Or(Box<HavingCond>, Box<HavingCond>),
    Not(Box<HavingCond>),
    Cmp {
        left: HavingOperand,
        op: HavingOp,
        right: HavingOperand,
    },
    IsNull {
        operand: HavingOperand,
        negated: bool,
    },
    /// Folded at plan time: `SUM(x) = NULL` is unknown for every group.
    Const(Option<bool>),
}

#[derive(Debug, Clone)]
pub enum HavingOperand {
    /// Index into [`Having::aggregates`].
    Aggregate(usize),
    /// A constant, already coerced against the aggregate it is compared with.
    Value(HValue),
}

/// A value as `HAVING` compares it.
///
/// Integers and decimals are exact (`mantissa / 10^scale`), so `SUM(x) > 5.5`
/// over a `BIGINT` compares as numbers rather than refusing the literal or
/// truncating it. Floats compare with PostgreSQL's order (NaN largest, -0 =
/// +0). Everything else compares as stored bytes of one type.
#[derive(Debug, Clone, PartialEq)]
pub enum HValue {
    Exact { mantissa: i128, scale: u32 },
    Float(f64),
    Bytes { bytes: Vec<u8>, db_type: DbType },
}

/// What [`parse`] returns; see the module documentation.
#[derive(Debug, Clone, Default)]
pub struct HavingSplit {
    pub having: Option<Having>,
    pub row_terms: Vec<RowTerm>,
}

/// A top-level conjunct that reads no aggregate.
#[derive(Debug, Clone)]
pub struct RowTerm {
    /// The conjunct exactly as written.
    pub text: String,
    /// Every column name it reads, as written (possibly qualified or quoted).
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    Exact,
    Float,
    Other,
}

fn category(ty: &DbType) -> Category {
    match ty {
        DbType::I8
        | DbType::I16
        | DbType::I32
        | DbType::I64
        | DbType::I128
        | DbType::U8
        | DbType::U16
        | DbType::U32
        | DbType::U64
        | DbType::U128
        | DbType::DECIMAL => Category::Exact,
        DbType::F32 | DbType::F64 => Category::Float,
        _ => Category::Other,
    }
}

/// The type and (for `DECIMAL`) scale of an aggregate item's result.
pub fn aggregate_result(item: &SelectItem) -> Option<(DbType, u32)> {
    match item {
        SelectItem::Aggregate { func, arg, .. } => {
            let ty = func.result_type(arg.as_ref().map(|f| &f.db_type));
            let scale = if ty == DbType::DECIMAL {
                func.decimal_output_scale(arg.as_ref())
            } else {
                0
            };
            Some((ty, scale))
        }
        _ => None,
    }
}

impl HValue {
    /// A finished aggregate's bytes, read as its result type.
    pub fn from_stored(bytes: &[u8], ty: &DbType, scale: u32) -> Option<HValue> {
        fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
            <[u8; N]>::try_from(b).ok()
        }
        let exact = |m: i128, s: u32| Some(HValue::Exact { mantissa: m, scale: s });
        match ty {
            DbType::I8 => exact(i8::from_le_bytes(le::<1>(bytes)?) as i128, 0),
            DbType::I16 => exact(i16::from_le_bytes(le::<2>(bytes)?) as i128, 0),
            DbType::I32 => exact(i32::from_le_bytes(le::<4>(bytes)?) as i128, 0),
            DbType::I64 => exact(i64::from_le_bytes(le::<8>(bytes)?) as i128, 0),
            DbType::I128 => exact(i128::from_le_bytes(le::<16>(bytes)?), 0),
            DbType::DECIMAL => exact(i128::from_le_bytes(le::<16>(bytes)?), scale),
            DbType::U8 => exact(u8::from_le_bytes(le::<1>(bytes)?) as i128, 0),
            DbType::U16 => exact(u16::from_le_bytes(le::<2>(bytes)?) as i128, 0),
            DbType::U32 => exact(u32::from_le_bytes(le::<4>(bytes)?) as i128, 0),
            DbType::U64 => exact(u64::from_le_bytes(le::<8>(bytes)?) as i128, 0),
            DbType::U128 => {
                let v = u128::from_le_bytes(le::<16>(bytes)?);
                match i128::try_from(v) {
                    Ok(v) => exact(v, 0),
                    Err(_) => Some(HValue::Float(v as f64)),
                }
            }
            DbType::F32 => Some(HValue::Float(f32::from_le_bytes(le::<4>(bytes)?) as f64)),
            DbType::F64 => Some(HValue::Float(f64::from_le_bytes(le::<8>(bytes)?))),
            other => Some(HValue::Bytes {
                bytes: bytes.to_vec(),
                db_type: other.clone(),
            }),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            HValue::Exact { mantissa, scale } => {
                Some(*mantissa as f64 / 10f64.powi(*scale as i32))
            }
            HValue::Float(f) => Some(*f),
            HValue::Bytes { .. } => None,
        }
    }
}

/// PostgreSQL's float order: NaN equals NaN and is above everything; -0 = +0.
fn pg_float_cmp(a: f64, b: f64) -> Ordering {
    crate::core::processor::concurrent_processor::pg_float_cmp(a, b)
}

/// Compares two values `HAVING` has in hand. `bytes_cmp` orders two stored
/// values of one non-numeric type.
pub fn compare(
    a: &HValue,
    b: &HValue,
    bytes_cmp: &dyn Fn(&[u8], &[u8], &DbType) -> Ordering,
) -> Result<Ordering, String> {
    match (a, b) {
        (
            HValue::Exact {
                mantissa: m1,
                scale: s1,
            },
            HValue::Exact {
                mantissa: m2,
                scale: s2,
            },
        ) => {
            if s1 == s2 {
                return Ok(m1.cmp(m2));
            }
            let (lift_first, diff) = if s1 < s2 { (true, s2 - s1) } else { (false, s1 - s2) };
            let factor = 10i128.checked_pow(diff);
            let lifted = factor.and_then(|f| if lift_first { m1.checked_mul(f) } else { m2.checked_mul(f) });
            match lifted {
                Some(l) if lift_first => Ok(l.cmp(m2)),
                Some(l) => Ok(m1.cmp(&l)),
                // Too large to lift: the magnitude alone decides, and f64 has
                // the range to see it.
                None => Ok(pg_float_cmp(a.as_f64().unwrap_or(0.0), b.as_f64().unwrap_or(0.0))),
            }
        }
        (HValue::Bytes { bytes: x, db_type: tx }, HValue::Bytes { bytes: y, db_type: ty }) => {
            if tx != ty {
                return Err(format!("cannot compare {} with {} in HAVING", tx, ty));
            }
            Ok(bytes_cmp(x, y, tx))
        }
        _ => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => Ok(pg_float_cmp(x, y)),
            _ => Err("cannot compare a number with a non-number in HAVING".to_string()),
        },
    }
}

impl HavingCond {
    /// Evaluates the condition for one group. `values[i]` is aggregate `i`'s
    /// result, `None` for SQL NULL. The answer is three-valued; `HAVING` keeps
    /// a group only for `Some(true)`.
    pub fn eval(
        &self,
        values: &[Option<HValue>],
        bytes_cmp: &dyn Fn(&[u8], &[u8], &DbType) -> Ordering,
    ) -> Result<Option<bool>, String> {
        let operand = |o: &HavingOperand| -> Result<Option<HValue>, String> {
            match o {
                HavingOperand::Aggregate(i) => values
                    .get(*i)
                    .cloned()
                    .ok_or_else(|| format!("HAVING reads aggregate {} that was not folded", i)),
                HavingOperand::Value(v) => Ok(Some(v.clone())),
            }
        };
        Ok(match self {
            HavingCond::Const(v) => *v,
            HavingCond::Not(inner) => inner.eval(values, bytes_cmp)?.map(|b| !b),
            HavingCond::And(a, b) => {
                let x = a.eval(values, bytes_cmp)?;
                if x == Some(false) {
                    return Ok(Some(false));
                }
                match (x, b.eval(values, bytes_cmp)?) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                }
            }
            HavingCond::Or(a, b) => {
                let x = a.eval(values, bytes_cmp)?;
                if x == Some(true) {
                    return Ok(Some(true));
                }
                match (x, b.eval(values, bytes_cmp)?) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }
            }
            HavingCond::IsNull { operand: o, negated } => {
                let is_null = operand(o)?.is_none();
                Some(is_null != *negated)
            }
            HavingCond::Cmp { left, op, right } => {
                match (operand(left)?, operand(right)?) {
                    (Some(l), Some(r)) => Some(op.holds(compare(&l, &r, bytes_cmp)?)),
                    _ => None,
                }
            }
        })
    }
}

impl Having {
    /// The old single-comparison shape, `<aggregate> <op> <literal>`, for a
    /// caller that has already split the clause itself. The literal is
    /// coerced now; one that does not coerce is an error.
    pub fn single(
        func: AggFunc,
        arg: Option<SchemaField>,
        op: HavingOp,
        literal: &str,
    ) -> Result<Having, String> {
        let item = SelectItem::Aggregate { distinct: false,
            func,
            arg,
            label: func.name().to_string(),
            filter: None,
            arg_expr: None,
        };
        let (ty, scale) = aggregate_result(&item).expect("an aggregate item");
        let lit = lex(literal.trim())?;
        let (text, quoted) = match lit.as_slice() {
            [Token { tk: Tk::Num(n), .. }] => (n.clone(), false),
            [Token { tk: Tk::Str(s), .. }] => (s.clone(), true),
            [
                Token {
                    tk: Tk::Sym(sign), ..
                },
                Token { tk: Tk::Num(n), .. },
            ] if sign == "-" || sign == "+" => (format!("{}{}", sign, n), false),
            _ => {
                return Err(format!(
                    "HAVING compares against '{}', which is not a constant",
                    literal.trim()
                ));
            }
        };
        let value = coerce_constant(&text, quoted, &ty, scale)?;
        Ok(Having {
            aggregates: vec![item],
            cond: HavingCond::Cmp {
                left: HavingOperand::Aggregate(0),
                op,
                right: HavingOperand::Value(value),
            },
        })
    }
}

// ---------------------------------------------------------------- lexing

#[derive(Debug, Clone, PartialEq)]
enum Tk {
    /// An identifier or keyword; a quoted identifier arrives unquoted.
    Word(String),
    Num(String),
    /// A string literal, with its `''` escapes collapsed.
    Str(String),
    /// A comparison operator.
    Op(String),
    LPar,
    RPar,
    Comma,
    /// Any other punctuation: `::`, `+`, `-`, `*`, `.` …
    Sym(String),
}

#[derive(Debug, Clone)]
struct Token {
    tk: Tk,
    start: usize,
    end: usize,
}

impl Token {
    fn is_word(&self, kw: &str) -> bool {
        matches!(&self.tk, Tk::Word(w) if w.eq_ignore_ascii_case(kw))
    }
}

fn lex(text: &str) -> Result<Vec<Token>, String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        let tk = match c {
            b'\'' => {
                let mut s = String::new();
                i += 1;
                loop {
                    let Some(&b) = bytes.get(i) else {
                        return Err("unterminated string literal in HAVING".to_string());
                    };
                    if b == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            s.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    let ch = text[i..].chars().next().expect("in bounds");
                    s.push(ch);
                    i += ch.len_utf8();
                }
                Tk::Str(s)
            }
            b'"' => {
                let close = text[i + 1..]
                    .find('"')
                    .ok_or("unterminated quoted identifier in HAVING")?;
                let name = text[i + 1..i + 1 + close].to_string();
                i += close + 2;
                Tk::Word(name)
            }
            b'(' => {
                i += 1;
                Tk::LPar
            }
            b')' => {
                i += 1;
                Tk::RPar
            }
            b',' => {
                i += 1;
                Tk::Comma
            }
            b'<' | b'>' | b'=' | b'!' => {
                let two = text.get(i..i + 2).unwrap_or("");
                let op = if matches!(two, "<=" | ">=" | "<>" | "!=") {
                    two
                } else if c == b'!' {
                    return Err("unexpected '!' in HAVING".to_string());
                } else {
                    &text[i..i + 1]
                };
                i += op.len();
                // `==` is accepted as `=`, as `HavingOp::parse` always has.
                if op == "=" && bytes.get(i) == Some(&b'=') {
                    i += 1;
                }
                Tk::Op(op.to_string())
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                i += 2;
                Tk::Sym("::".to_string())
            }
            b'0'..=b'9' | b'.' if c != b'.' || bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                    i += 1;
                }
                if i < bytes.len() && matches!(bytes[i], b'e' | b'E') {
                    let mut j = i + 1;
                    if j < bytes.len() && matches!(bytes[j], b'+' | b'-') {
                        j += 1;
                    }
                    if j < bytes.len() && bytes[j].is_ascii_digit() {
                        i = j;
                        while i < bytes.len() && bytes[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                Tk::Num(text[start..i].to_string())
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                while i < bytes.len() {
                    let b = bytes[i];
                    if b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80 {
                        i += 1;
                    } else if b == b'.'
                        && bytes
                            .get(i + 1)
                            .is_some_and(|n| n.is_ascii_alphabetic() || *n == b'_' || *n == b'"')
                    {
                        // A qualified name — `t.col` or `t."col"`.
                        i += 1;
                        if bytes[i] == b'"' {
                            let close = text[i + 1..]
                                .find('"')
                                .ok_or("unterminated quoted identifier in HAVING")?;
                            i += close + 2;
                        }
                    } else {
                        break;
                    }
                }
                Tk::Word(text[start..i].to_string())
            }
            _ => {
                let ch = text[i..].chars().next().expect("in bounds");
                i += ch.len_utf8();
                Tk::Sym(ch.to_string())
            }
        };
        out.push(Token { tk, start, end: i });
    }
    Ok(out)
}

const KEYWORDS: &[&str] = &[
    "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "BETWEEN", "LIKE", "ILIKE",
    "SIMILAR", "TO", "ESCAPE", "DISTINCT", "FROM", "ANY", "ALL", "SOME", "ARRAY", "AS", "CASE",
    "WHEN", "THEN", "ELSE", "END", "FILTER", "WHERE", "UNKNOWN", "SYMMETRIC",
];

fn is_keyword(w: &str) -> bool {
    KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(w))
}

fn is_aggregate_call(toks: &[Token], at: usize) -> bool {
    matches!(&toks[at].tk, Tk::Word(w) if AggFunc::parse(w).is_some())
        && matches!(toks.get(at + 1).map(|t| &t.tk), Some(Tk::LPar))
}

/// Index just past the parenthesis that closes the one at `open`.
fn close_of(toks: &[Token], open: usize) -> Result<usize, String> {
    let mut depth = 0usize;
    for (i, t) in toks.iter().enumerate().skip(open) {
        match t.tk {
            Tk::LPar => depth += 1,
            Tk::RPar => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
    }
    Err("unbalanced parentheses in HAVING".to_string())
}

/// The end (exclusive) of the aggregate call starting at `at`, including a
/// trailing `FILTER (WHERE …)`.
fn aggregate_end(toks: &[Token], at: usize) -> Result<usize, String> {
    let mut end = close_of(toks, at + 1)?;
    if toks.get(end).is_some_and(|t| t.is_word("FILTER"))
        && matches!(toks.get(end + 1).map(|t| &t.tk), Some(Tk::LPar))
    {
        end = close_of(toks, end + 1)?;
    }
    Ok(end)
}

/// Splits the token stream at top-level `AND`, leaving the `AND` of a
/// `BETWEEN … AND …` inside its conjunct.
///
/// A top-level `OR` makes the whole clause one conjunct: `AND` binds tighter,
/// so `a OR b AND c` is `a OR (b AND c)`, and cutting it at the `AND` would
/// read it as `(a OR b) AND c`.
fn split_conjuncts(toks: &[Token]) -> Vec<(usize, usize)> {
    let mut depth = 0usize;
    for t in toks {
        match t.tk {
            Tk::LPar => depth += 1,
            Tk::RPar => depth = depth.saturating_sub(1),
            _ if depth == 0 && t.is_word("OR") => return vec![(0, toks.len())],
            _ => {}
        }
    }
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut between = false;
    let mut start = 0usize;
    for (i, t) in toks.iter().enumerate() {
        match t.tk {
            Tk::LPar => depth += 1,
            Tk::RPar => depth = depth.saturating_sub(1),
            _ if depth == 0 && t.is_word("BETWEEN") => between = true,
            _ if depth == 0 && t.is_word("AND") => {
                if between {
                    between = false;
                } else {
                    out.push((start, i));
                    start = i + 1;
                }
            }
            _ => {}
        }
    }
    out.push((start, toks.len()));
    out
}

/// Parses a `HAVING` clause. See the module documentation for the contract.
pub fn parse(
    text: &str,
    aggregate: &mut dyn FnMut(&str) -> Result<SelectItem, String>,
) -> Result<HavingSplit, String> {
    let toks = lex(text)?;
    if toks.is_empty() {
        return Err("HAVING is missing its condition".to_string());
    }
    let mut split = HavingSplit::default();
    let mut aggregates: Vec<SelectItem> = Vec::new();
    let mut agg_texts: Vec<String> = Vec::new();
    let mut conds: Vec<HavingCond> = Vec::new();

    for (from, to) in split_conjuncts(&toks) {
        let part = &toks[from..to];
        if part.is_empty() {
            return Err("HAVING has an empty condition around AND".to_string());
        }
        // Classify: which aggregates, which bare columns.
        let mut has_aggregate = false;
        let mut columns: Vec<String> = Vec::new();
        let mut i = 0usize;
        while i < part.len() {
            if is_aggregate_call(part, i) {
                has_aggregate = true;
                i = aggregate_end(part, i)?;
                continue;
            }
            if let Tk::Word(w) = &part[i].tk {
                let after_cast = i > 0 && matches!(&part[i - 1].tk, Tk::Sym(s) if s == "::");
                let is_call = matches!(part.get(i + 1).map(|t| &t.tk), Some(Tk::LPar));
                if !after_cast && !is_call && !is_keyword(w) {
                    columns.push(w.clone());
                }
            }
            i += 1;
        }

        if !has_aggregate {
            let text_part = &text[part[0].start..part[part.len() - 1].end];
            split.row_terms.push(RowTerm {
                text: text_part.to_string(),
                columns,
            });
            continue;
        }
        if let Some(column) = columns.first() {
            return Err(format!(
                "HAVING term '{}' tests the column '{}' together with an aggregate; \
                 only a separate top-level AND term may test a grouping column",
                &text[part[0].start..part[part.len() - 1].end],
                column
            ));
        }

        let mut p = Parser {
            text,
            toks: part,
            pos: 0,
            aggregate: &mut *aggregate,
            aggregates: &mut aggregates,
            agg_texts: &mut agg_texts,
        };
        let cond = p.or()?;
        if p.pos != part.len() {
            return Err(format!(
                "unexpected '{}' in HAVING",
                &text[part[p.pos].start..part[part.len() - 1].end]
            ));
        }
        conds.push(cond);
    }

    if let Some(first) = conds.pop() {
        let cond = conds
            .into_iter()
            .rev()
            .fold(first, |acc, c| HavingCond::And(Box::new(c), Box::new(acc)));
        split.having = Some(Having { aggregates, cond });
    }
    Ok(split)
}

/// An operand as parsed, before it is coerced against the other side.
enum Raw {
    Agg(usize),
    Lit { text: String, quoted: bool },
    Null,
}

struct Parser<'a, 'f> {
    text: &'a str,
    toks: &'a [Token],
    pos: usize,
    aggregate: &'f mut dyn FnMut(&str) -> Result<SelectItem, String>,
    aggregates: &'f mut Vec<SelectItem>,
    agg_texts: &'f mut Vec<String>,
}

impl Parser<'_, '_> {
    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.pos)
    }

    fn peek_word(&self, kw: &str) -> bool {
        self.peek().is_some_and(|t| t.is_word(kw))
    }

    fn or(&mut self) -> Result<HavingCond, String> {
        let mut left = self.and()?;
        while self.peek_word("OR") {
            self.pos += 1;
            let right = self.and()?;
            left = HavingCond::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<HavingCond, String> {
        let mut left = self.not()?;
        while self.peek_word("AND") {
            self.pos += 1;
            let right = self.not()?;
            left = HavingCond::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<HavingCond, String> {
        if self.peek_word("NOT") {
            self.pos += 1;
            return Ok(HavingCond::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    /// A parenthesis at the cursor that opens a boolean group, rather than a
    /// parenthesised operand.
    fn opens_group(&self) -> Result<bool, String> {
        if !matches!(self.peek().map(|t| &t.tk), Some(Tk::LPar)) {
            return Ok(false);
        }
        let end = close_of(self.toks, self.pos)?;
        let mut depth = 0usize;
        for t in &self.toks[self.pos + 1..end - 1] {
            match &t.tk {
                Tk::LPar => depth += 1,
                Tk::RPar => depth -= 1,
                Tk::Op(_) if depth == 0 => return Ok(true),
                Tk::Word(w)
                    if depth == 0
                        && ["AND", "OR", "NOT", "IS", "BETWEEN", "IN"]
                            .iter()
                            .any(|k| k.eq_ignore_ascii_case(w)) =>
                {
                    return Ok(true);
                }
                _ => {}
            }
        }
        Ok(false)
    }

    fn primary(&mut self) -> Result<HavingCond, String> {
        if self.opens_group()? {
            self.pos += 1;
            let inner = self.or()?;
            match self.peek().map(|t| &t.tk) {
                Some(Tk::RPar) => self.pos += 1,
                _ => return Err("expected ')' in HAVING".to_string()),
            }
            return Ok(inner);
        }
        self.predicate()
    }

    fn predicate(&mut self) -> Result<HavingCond, String> {
        let left = self.operand()?;

        if let Some(Tk::Op(op)) = self.peek().map(|t| t.tk.clone()) {
            self.pos += 1;
            let op = HavingOp::parse(&op)
                .ok_or_else(|| format!("unknown comparison operator in HAVING: '{}'", op))?;
            let right = self.operand()?;
            return self.compare(left, op, right);
        }

        if self.peek_word("IS") {
            self.pos += 1;
            let negated = self.peek_word("NOT");
            if negated {
                self.pos += 1;
            }
            if !self.peek_word("NULL") {
                return Err("HAVING supports IS [NOT] NULL only".to_string());
            }
            self.pos += 1;
            return Ok(match left {
                Raw::Agg(i) => HavingCond::IsNull {
                    operand: HavingOperand::Aggregate(i),
                    negated,
                },
                Raw::Null => HavingCond::Const(Some(!negated)),
                Raw::Lit { .. } => HavingCond::Const(Some(negated)),
            });
        }

        let negated = self.peek_word("NOT");
        let kw_at = self.pos + usize::from(negated);
        if self.toks.get(kw_at).is_some_and(|t| t.is_word("BETWEEN")) {
            self.pos = kw_at + 1;
            if self.peek_word("SYMMETRIC") {
                return Err("BETWEEN SYMMETRIC is not supported in HAVING".to_string());
            }
            let lo = self.operand()?;
            if !self.peek_word("AND") {
                return Err("BETWEEN bounds must be separated by AND".to_string());
            }
            self.pos += 1;
            let hi = self.operand()?;
            let left2 = self.duplicate(&left);
            return if negated {
                Ok(HavingCond::Or(
                    Box::new(self.compare(left, HavingOp::Lt, lo)?),
                    Box::new(self.compare(left2, HavingOp::Gt, hi)?),
                ))
            } else {
                Ok(HavingCond::And(
                    Box::new(self.compare(left, HavingOp::Ge, lo)?),
                    Box::new(self.compare(left2, HavingOp::Le, hi)?),
                ))
            };
        }
        if self.toks.get(kw_at).is_some_and(|t| t.is_word("IN")) {
            self.pos = kw_at + 1;
            if !matches!(self.peek().map(|t| &t.tk), Some(Tk::LPar)) {
                return Err("IN in HAVING needs a parenthesised list".to_string());
            }
            self.pos += 1;
            let mut terms: Vec<HavingCond> = Vec::new();
            loop {
                let value = self.operand()?;
                let l = self.duplicate(&left);
                terms.push(self.compare(
                    l,
                    if negated { HavingOp::Ne } else { HavingOp::Eq },
                    value,
                )?);
                match self.peek().map(|t| &t.tk) {
                    Some(Tk::Comma) => self.pos += 1,
                    Some(Tk::RPar) => {
                        self.pos += 1;
                        break;
                    }
                    _ => return Err("malformed IN list in HAVING".to_string()),
                }
            }
            let first = terms.remove(0);
            return Ok(terms.into_iter().fold(first, |acc, t| {
                if negated {
                    HavingCond::And(Box::new(acc), Box::new(t))
                } else {
                    HavingCond::Or(Box::new(acc), Box::new(t))
                }
            }));
        }

        Err(match self.peek() {
            Some(t) => format!(
                "HAVING must compare an aggregate, as in `HAVING COUNT(*) > 1`; unexpected '{}'",
                &self.text[t.start..t.end]
            ),
            None => "HAVING must compare an aggregate, as in `HAVING COUNT(*) > 1`".to_string(),
        })
    }

    fn duplicate(&self, raw: &Raw) -> Raw {
        match raw {
            Raw::Agg(i) => Raw::Agg(*i),
            Raw::Lit { text, quoted } => Raw::Lit {
                text: text.clone(),
                quoted: *quoted,
            },
            Raw::Null => Raw::Null,
        }
    }

    fn operand(&mut self) -> Result<Raw, String> {
        let Some(tok) = self.peek().cloned() else {
            return Err("HAVING is missing the value to compare against".to_string());
        };
        let raw = match &tok.tk {
            Tk::Word(_) if is_aggregate_call(self.toks, self.pos) => {
                let end = aggregate_end(self.toks, self.pos)?;
                let call = self.text[tok.start..self.toks[end - 1].end].trim().to_string();
                self.pos = end;
                let at = match self.agg_texts.iter().position(|t| *t == call) {
                    Some(at) => at,
                    None => {
                        let item = (self.aggregate)(&call)?;
                        if !matches!(item, SelectItem::Aggregate { .. }) {
                            return Err(format!("'{}' is not an aggregate HAVING can test", call));
                        }
                        self.aggregates.push(item);
                        self.agg_texts.push(call);
                        self.aggregates.len() - 1
                    }
                };
                if matches!(self.peek().map(|t| &t.tk), Some(Tk::Sym(s)) if s == "::") {
                    return Err("a cast over an aggregate is not supported in HAVING".to_string());
                }
                return Ok(Raw::Agg(at));
            }
            Tk::Num(n) => {
                self.pos += 1;
                Raw::Lit {
                    text: n.clone(),
                    quoted: false,
                }
            }
            Tk::Sym(s) if s == "-" || s == "+" => {
                let Some(Tk::Num(n)) = self.toks.get(self.pos + 1).map(|t| &t.tk) else {
                    return Err(format!("expected a number after '{}' in HAVING", s));
                };
                self.pos += 2;
                Raw::Lit {
                    text: format!("{}{}", s, n),
                    quoted: false,
                }
            }
            Tk::Str(s) => {
                self.pos += 1;
                Raw::Lit {
                    text: s.clone(),
                    quoted: true,
                }
            }
            Tk::Word(w) if w.eq_ignore_ascii_case("NULL") => {
                self.pos += 1;
                Raw::Null
            }
            Tk::Word(w) if w.eq_ignore_ascii_case("TRUE") || w.eq_ignore_ascii_case("FALSE") => {
                self.pos += 1;
                Raw::Lit {
                    text: w.to_ascii_lowercase(),
                    quoted: false,
                }
            }
            Tk::LPar => {
                // A parenthesised operand: `(COUNT(*)) > 1`.
                self.pos += 1;
                let inner = self.operand()?;
                match self.peek().map(|t| &t.tk) {
                    Some(Tk::RPar) => self.pos += 1,
                    _ => return Err("expected ')' after an operand in HAVING".to_string()),
                }
                return Ok(inner);
            }
            _ => {
                return Err(format!(
                    "'{}' cannot be compared in HAVING; it supports aggregates and constants",
                    &self.text[tok.start..tok.end]
                ));
            }
        };
        // A cast on a constant, `'2024-01-01'::timestamp`: the constant is
        // coerced to the type of the aggregate it meets either way, so the
        // cast's type name is read past.
        if matches!(self.peek().map(|t| &t.tk), Some(Tk::Sym(s)) if s == "::") {
            self.pos += 1;
            if !matches!(self.peek().map(|t| &t.tk), Some(Tk::Word(_))) {
                return Err("a cast in HAVING must name a type".to_string());
            }
            self.pos += 1;
            if matches!(self.peek().map(|t| &t.tk), Some(Tk::LPar)) {
                self.pos = close_of(self.toks, self.pos)?;
            }
        }
        Ok(raw)
    }

    fn result_of(&self, at: usize) -> (DbType, u32) {
        aggregate_result(&self.aggregates[at]).expect("only aggregates are recorded")
    }

    fn compare(&mut self, left: Raw, op: HavingOp, right: Raw) -> Result<HavingCond, String> {
        Ok(match (left, right) {
            (Raw::Null, _) | (_, Raw::Null) => HavingCond::Const(None),
            (Raw::Agg(a), Raw::Agg(b)) => {
                let (ta, _) = self.result_of(a);
                let (tb, _) = self.result_of(b);
                let (ca, cb) = (category(&ta), category(&tb));
                let numeric = |c| c == Category::Exact || c == Category::Float;
                if !(numeric(ca) && numeric(cb)) && ta != tb {
                    return Err(format!(
                        "HAVING cannot compare a {} aggregate with a {} one",
                        ta, tb
                    ));
                }
                HavingCond::Cmp {
                    left: HavingOperand::Aggregate(a),
                    op,
                    right: HavingOperand::Aggregate(b),
                }
            }
            (Raw::Agg(a), Raw::Lit { text, quoted }) => {
                let (ty, scale) = self.result_of(a);
                HavingCond::Cmp {
                    left: HavingOperand::Aggregate(a),
                    op,
                    right: HavingOperand::Value(coerce_constant(&text, quoted, &ty, scale)?),
                }
            }
            (Raw::Lit { text, quoted }, Raw::Agg(b)) => {
                let (ty, scale) = self.result_of(b);
                HavingCond::Cmp {
                    left: HavingOperand::Value(coerce_constant(&text, quoted, &ty, scale)?),
                    op,
                    right: HavingOperand::Aggregate(b),
                }
            }
            (Raw::Lit { text: x, .. }, Raw::Lit { text: y, .. }) => {
                let ord = match (parse_number(&x), parse_number(&y)) {
                    (Some(a), Some(b)) => compare(&a, &b, &|a, b, _| a.cmp(b))?,
                    _ => x.cmp(&y),
                };
                HavingCond::Const(Some(op.holds(ord)))
            }
        })
    }
}

/// An exact decimal, `[-+]digits[.digits]`, as mantissa and scale.
fn parse_exact(text: &str) -> Option<(i128, u32)> {
    let t = text.trim();
    let (negative, digits) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (int, frac) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Trailing fractional zeros carry no value and only cost range.
    let frac = frac.trim_end_matches('0');
    let scale = u32::try_from(frac.len()).ok()?;
    if scale > 38 {
        return None;
    }
    let mut m: i128 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        m = m.checked_mul(10)?.checked_add((b - b'0') as i128)?;
    }
    Some((if negative { -m } else { m }, scale))
}

fn parse_float(text: &str) -> Option<f64> {
    let t = text.trim();
    let (sign, body) = match t.as_bytes().first()? {
        b'-' => (-1.0, &t[1..]),
        b'+' => (1.0, &t[1..]),
        _ => (1.0, t),
    };
    if body.eq_ignore_ascii_case("nan") {
        return Some(f64::NAN);
    }
    if body.eq_ignore_ascii_case("infinity") || body.eq_ignore_ascii_case("inf") {
        return Some(sign * f64::INFINITY);
    }
    if !body
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        return None;
    }
    body.parse::<f64>().ok().map(|v| sign * v)
}

fn parse_number(text: &str) -> Option<HValue> {
    match parse_exact(text) {
        Some((mantissa, scale)) => Some(HValue::Exact { mantissa, scale }),
        None => parse_float(text).map(HValue::Float),
    }
}

/// Coerces a constant against an aggregate's result type, or fails.
///
/// Never "keep the group": a constant that means nothing at the aggregate's
/// type is a statement the client must hear about.
fn coerce_constant(text: &str, quoted: bool, ty: &DbType, scale: u32) -> Result<HValue, String> {
    let refused = || {
        format!(
            "invalid input for a {} aggregate in HAVING: {}",
            ty,
            if quoted {
                format!("'{}'", text)
            } else {
                text.to_string()
            }
        )
    };
    match category(ty) {
        Category::Exact => match parse_number(text) {
            // `SUM(x) > 'NaN'`-style values are numbers to a float only.
            Some(HValue::Float(f)) if f.is_nan() || f.is_infinite() => {
                if quoted || text.trim().chars().any(|c| c.is_ascii_alphabetic() && c != 'e' && c != 'E') {
                    Ok(HValue::Float(f))
                } else {
                    Err(refused())
                }
            }
            Some(v) => Ok(v),
            None => Err(refused()),
        },
        Category::Float => parse_float(text).map(HValue::Float).ok_or_else(refused),
        Category::Other => {
            let _ = scale;
            crate::core::value_codec::coerce_literal(text.as_bytes(), &DbType::STRING, ty)
                .map(|bytes| HValue::Bytes {
                    bytes,
                    db_type: ty.clone(),
                })
                .ok_or_else(refused)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, ty: DbType, wo: u64) -> SchemaField {
        SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, wo, false)
    }

    /// A resolver that understands `FUNC(*)` and `FUNC(col)` over a fixed
    /// schema, enough to drive the parser without a planner.
    fn resolver(schema: Vec<SchemaField>) -> impl FnMut(&str) -> Result<SelectItem, String> {
        move |call: &str| {
            let open = call.find('(').ok_or("no call")?;
            let func = AggFunc::parse(call[..open].trim()).ok_or("not an aggregate")?;
            let close = call.find(')').ok_or("no close")?;
            let arg = call[open + 1..close].trim();
            let arg = if arg == "*" {
                None
            } else {
                Some(
                    schema
                        .iter()
                        .find(|f| f.name == arg)
                        .cloned()
                        .ok_or_else(|| format!("no column {}", arg))?,
                )
            };
            Ok(SelectItem::Aggregate { distinct: false,
                func,
                arg,
                label: call.to_string(),
                filter: None,
                arg_expr: None,
            })
        }
    }

    fn eval(h: &Having, values: &[Option<HValue>]) -> Option<bool> {
        h.cond.eval(values, &|a, b, _| a.cmp(b)).unwrap()
    }

    fn int(v: i128) -> Option<HValue> {
        Some(HValue::Exact {
            mantissa: v,
            scale: 0,
        })
    }

    fn schema() -> Vec<SchemaField> {
        vec![field("x", DbType::I32, 0), field("f", DbType::F64, 1)]
    }

    #[test]
    fn a_compound_clause_is_a_tree_not_a_literal() {
        let split = parse("COUNT(*) > 1 AND SUM(x) > 5", &mut resolver(schema())).unwrap();
        let h = split.having.unwrap();
        assert_eq!(h.aggregates.len(), 2);
        assert!(split.row_terms.is_empty());
        assert_eq!(eval(&h, &[int(3), int(6)]), Some(true));
        assert_eq!(eval(&h, &[int(3), int(5)]), Some(false));
        assert_eq!(eval(&h, &[int(1), int(6)]), Some(false));
        // NULL SUM: unknown, which HAVING drops.
        assert_eq!(eval(&h, &[int(3), None]), None);
    }

    #[test]
    fn precedence_and_grouping_follow_sql() {
        let mut r = resolver(schema());
        let h = parse("COUNT(*) = 1 OR COUNT(*) = 3 AND SUM(x) < 10", &mut r)
            .unwrap()
            .having
            .unwrap();
        // One aggregate text, folded once.
        assert_eq!(h.aggregates.len(), 2);
        assert_eq!(eval(&h, &[int(1), int(100)]), Some(true));
        assert_eq!(eval(&h, &[int(3), int(100)]), Some(false));
        let h = parse("(COUNT(*) = 1 OR COUNT(*) = 3) AND SUM(x) < 10", &mut r)
            .unwrap()
            .having
            .unwrap();
        assert_eq!(eval(&h, &[int(1), int(100)]), Some(false));
    }

    #[test]
    fn between_keeps_its_and() {
        let mut r = resolver(schema());
        let split = parse("COUNT(*) BETWEEN 2 AND 3 AND SUM(x) > 0", &mut r).unwrap();
        let h = split.having.unwrap();
        assert_eq!(eval(&h, &[int(2), int(1)]), Some(true));
        assert_eq!(eval(&h, &[int(4), int(1)]), Some(false));
        let h = parse("COUNT(*) NOT BETWEEN 2 AND 3", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[int(4)]), Some(true));
        assert_eq!(eval(&h, &[int(2)]), Some(false));
    }

    #[test]
    fn not_in_with_a_null_never_holds() {
        let mut r = resolver(schema());
        let h = parse("COUNT(*) NOT IN (1, NULL)", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[int(2)]), None);
        assert_eq!(eval(&h, &[int(1)]), Some(false));
        let h = parse("COUNT(*) IN (1, NULL)", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[int(1)]), Some(true));
        assert_eq!(eval(&h, &[int(2)]), None);
    }

    #[test]
    fn an_uncoercible_constant_is_an_error() {
        let mut r = resolver(schema());
        for bad in [
            "COUNT(*) > 'abc'",
            "COUNT(*) > 1 AND SUM(x) > 'many'",
            "COUNT(*) >",
            "SUM(x) > 1 AND",
            "COUNT(*) > 1 AND",
            "COUNT(*) 5",
            "SUM(f) > 'nope'",
        ] {
            assert!(parse(bad, &mut r).is_err(), "{} parsed", bad);
        }
    }

    #[test]
    fn a_string_holding_and_is_one_literal() {
        let schema = vec![field("s", DbType::STRING, 0)];
        let mut r = resolver(schema);
        let h = parse("MAX(s) = 'a AND b'", &mut r).unwrap().having.unwrap();
        let v = |s: &str| {
            Some(HValue::Bytes {
                bytes: s.as_bytes().to_vec(),
                db_type: DbType::STRING,
            })
        };
        assert_eq!(eval(&h, &[v("a AND b")]), Some(true));
        assert_eq!(eval(&h, &[v("a")]), Some(false));
        let h = parse("MAX(s) = 'it''s'", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[v("it's")]), Some(true));
    }

    #[test]
    fn row_terms_are_split_out_with_their_columns() {
        let mut r = resolver(schema());
        let split = parse("status = 'a' AND COUNT(*) > 2 AND t.\"Kind\" <> 'x'", &mut r).unwrap();
        assert_eq!(split.row_terms.len(), 2);
        assert_eq!(split.row_terms[0].text, "status = 'a'");
        assert_eq!(split.row_terms[0].columns, vec!["status".to_string()]);
        assert_eq!(split.row_terms[1].columns, vec!["t.\"Kind\"".to_string()]);
        assert!(split.having.is_some());
        // Mixed under OR is refused, not half-applied.
        assert!(parse("status = 'a' OR COUNT(*) > 1", &mut r).is_err());
    }

    #[test]
    fn numbers_compare_across_scales_and_kinds() {
        let mut r = resolver(schema());
        let h = parse("SUM(x) > 5.5", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[int(6)]), Some(true));
        assert_eq!(eval(&h, &[int(5)]), Some(false));
        let h = parse("SUM(f) > 1", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[Some(HValue::Float(f64::NAN))]), Some(true), "NaN is largest");
        let h = parse("SUM(f) = 'NaN'", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[Some(HValue::Float(f64::NAN))]), Some(true));
        let h = parse("MAX(x) IS NULL", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[None]), Some(true));
        assert_eq!(eval(&h, &[int(0)]), Some(false));
        let h = parse("SUM(x) > COUNT(*)", &mut r).unwrap().having.unwrap();
        assert_eq!(eval(&h, &[int(6), int(3)]), Some(true));
    }

    #[test]
    fn the_single_comparison_constructor_refuses_what_it_cannot_coerce() {
        assert!(Having::single(AggFunc::Count, None, HavingOp::Gt, "1 AND SUM(x) > 5").is_err());
        assert!(Having::single(AggFunc::Count, None, HavingOp::Gt, "'abc'").is_err());
        let h = Having::single(AggFunc::Count, None, HavingOp::Gt, "-1").unwrap();
        assert_eq!(eval(&h, &[int(0)]), Some(true));
    }
}
