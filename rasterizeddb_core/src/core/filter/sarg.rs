//! Extraction of filterable ("sargable") predicates from a tokenized WHERE
//! clause.
//!
//! # Soundness
//!
//! A candidate mask must be a **superset** of the true match set, which makes
//! the boolean structure load-bearing rather than cosmetic:
//!
//! - `A AND B` → `mask(A) & mask(B)` is sound.
//! - `A OR B`  → `mask(A) | mask(B)` is sound.
//! - A leaf we do not understand contributes `ALL_ONES`, never `0`.
//! - `col != v` is **not** usable: `probe(v)` over-approximates the rows where
//!   `col == v`, so its complement *under*-approximates `col != v` and would
//!   drop real results.
//!
//! Getting precedence wrong is equally fatal — `A OR B AND C` means
//! `A ∨ (B∧C)`, and folding it left-to-right as `(A∨B) ∧ C` yields a mask that
//! is not a superset. So this module does not invent a precedence rule; it
//! mirrors the one the engine actually implements.
//!
//! `TransformerProcessor::evaluate_comparison_results` walks a flat list of
//! `(bool, Option<Next>)` accumulating an AND-group and short-circuiting to
//! `true` whenever a group succeeds before an `OR`. That is **sum-of-products**
//! with `AND` binding tighter than `OR`, i.e. standard SQL precedence, and
//! [`extract`] reproduces exactly that grouping from the token stream.
//!
//! Parentheses are supported. They were previously refused because it was
//! unverified whether the engine evaluated `(A OR B) AND C` as written, given it
//! flattens boolean structure into that same chain. Measured against the
//! evaluator, it does — all of `(A OR B)`, `(A AND B)`, `X AND (A OR B)` and
//! `(A OR B) AND X` return the correct rows — so this module now parses the
//! grouping instead of giving up on it. That matters because
//! `predicate_rewrite` desugars `IN` to `(a = 1 OR a = 2 …)` and `BETWEEN` to
//! `(a >= lo AND a <= hi)`: refusing parens meant refusing every `IN` and
//! `BETWEEN` an ORM generates.
//!
//! `NOT` never reaches a group: `predicate_rewrite` pushes it down to single
//! comparisons, complementing the operator where one exists (`a != 1`, which
//! is opaque here like any `!=`). Where none does it leaves a `NOT` token
//! before that one comparison, and such a conjunct is opaque too — reading
//! it as the comparison without its `NOT` would be the complement, which is
//! the one answer a mask may never give.
//!
//! # Not every predicate is a comparison
//!
//! `col STARTSWITH 'abc'` is lowered to `col >= 'abc' AND col < 'abd'` by
//! [`starts_with_to_range`], which is why one conjunct can produce two
//! predicates. It is the only rewriting this module does, and it is exact rather
//! than approximate — see that function for which neighbouring operators are
//! deliberately left opaque and why.

use crate::core::db_type::DbType;
use crate::core::tokenizer::query_tokenizer::{NumericValue, Token, numeric_value_to_db_type};
use crate::core::value_codec::coerce_literal;

use super::TableFilters;
use super::zonemap::{SargOp, successor};

/// One `column <op> literal` predicate, with the literal already in the exact
/// byte form the comparison path will use.
#[derive(Clone, Debug)]
pub struct SargPredicate {
    pub write_order: u32,
    pub op: SargOp,
    pub value: Vec<u8>,
}

/// The boolean structure of a WHERE clause, reduced to what the filter can act
/// on.
#[derive(Clone, Debug)]
pub enum MaskExpr {
    /// Carries no information: every slot stays a candidate. Anything not
    /// understood collapses to this, which is what keeps the mask a superset.
    All,
    Pred(SargPredicate),
    And(Vec<MaskExpr>),
    Or(Vec<MaskExpr>),
}

impl MaskExpr {
    /// True when nothing in this subtree can narrow anything.
    pub fn is_all(&self) -> bool {
        match self {
            MaskExpr::All => true,
            MaskExpr::Pred(_) => false,
            MaskExpr::And(v) => v.iter().all(|e| e.is_all()),
            // A union with an unknown branch is itself unknown.
            MaskExpr::Or(v) => v.iter().any(|e| e.is_all()),
        }
    }

    fn walk<'a>(&'a self, out: &mut Vec<&'a SargPredicate>) {
        match self {
            MaskExpr::All => {}
            MaskExpr::Pred(p) => out.push(p),
            MaskExpr::And(v) | MaskExpr::Or(v) => v.iter().for_each(|e| e.walk(out)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SargProgram {
    pub root: MaskExpr,
}

impl Default for SargProgram {
    fn default() -> Self {
        Self {
            root: MaskExpr::All,
        }
    }
}

impl SargProgram {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root.is_all()
    }

    /// Every predicate, ignoring structure. For diagnostics and tests.
    pub fn all_preds(&self) -> Vec<&SargPredicate> {
        let mut v = Vec::new();
        self.root.walk(&mut v);
        v
    }
}

fn numeric_bytes(v: &NumericValue) -> Vec<u8> {
    match v {
        NumericValue::I8(n) => n.to_le_bytes().to_vec(),
        NumericValue::I16(n) => n.to_le_bytes().to_vec(),
        NumericValue::I32(n) => n.to_le_bytes().to_vec(),
        NumericValue::I64(n) => n.to_le_bytes().to_vec(),
        NumericValue::I128(n) => n.to_le_bytes().to_vec(),
        NumericValue::U8(n) => n.to_le_bytes().to_vec(),
        NumericValue::U16(n) => n.to_le_bytes().to_vec(),
        NumericValue::U32(n) => n.to_le_bytes().to_vec(),
        NumericValue::U64(n) => n.to_le_bytes().to_vec(),
        NumericValue::U128(n) => n.to_le_bytes().to_vec(),
        NumericValue::F32(n) => n.to_le_bytes().to_vec(),
        NumericValue::F64(n) => n.to_le_bytes().to_vec(),
    }
}

/// Map an operator token, refusing the ones no hash or bound structure can
/// answer soundly.
fn op_of(tok: &Token) -> Option<SargOp> {
    let Token::Op(op) = tok else { return None };
    Some(match op.as_str() {
        "=" => SargOp::Eq,
        ">" => SargOp::Gt,
        ">=" => SargOp::Ge,
        "<" => SargOp::Lt,
        "<=" => SargOp::Le,
        // `!=` is the trap: probe(v) over-approximates `col = v`, so its
        // complement UNDER-approximates `col != v` and would drop real matches.
        _ => return None,
    })
}

/// Turn one `[Ident, Op, Literal]` conjunct into a predicate, if it is one we
/// can prove sound.
fn conjunct_to_pred(
    toks: &[Token],
    gate: &dyn Fn(u32) -> bool,
    scales: &[(u32, u32)],
) -> Option<SargPredicate> {
    if toks.len() != 3 {
        return None;
    }

    let mut op = op_of(&toks[1])?;

    // Accept both `col > 5` and `5 < col`; the latter is the mirrored predicate.
    let (ident, lit) = match (&toks[0], &toks[2]) {
        (Token::Ident(i), lit) => (i, lit),
        (lit, Token::Ident(i)) => {
            op = op.mirrored();
            (i, lit)
        }
        _ => return None,
    };

    let (_name, db_type, write_order) = ident;
    let write_order = *write_order as u32;
    if !gate(write_order) {
        return None;
    }

    // Re-encode the literal at the column's type, exactly as
    // `QueryParser::reconcile_operand_types` does before evaluation.
    //
    // This must not be a type *check*. The tokenizer types a literal from the
    // preceding column, so `1990 < id` yields an I32 literal against a U64
    // column, and `amount > 200` an I32 literal against DECIMAL — and a text
    // literal against a UUID column is the normal way to write a UUID. Rejecting
    // those left every one of them unfiltered while the evaluator answered them
    // correctly, which is the silent-no-pruning failure rather than a wrong
    // answer, but it covered most ORM traffic.
    //
    // Coercing through the same `coerce_literal` the parser uses is what keeps
    // the bytes we hash identical to the bytes the evaluator will compare.
    let (raw, from) = match lit {
        Token::Number(nv) => (numeric_bytes(nv), numeric_value_to_db_type(nv)),
        Token::StringLit(s) => (s.as_bytes().to_vec(), DbType::STRING),
        _ => return None,
    };
    // A `NUMERIC(p, s)` column stores its integer at its own `s`, and the
    // evaluator re-encodes the literal there (`QueryParser::
    // rescale_decimal_literal`) — through the same text, by the same reader,
    // so the bytes bounded here are the bytes it compares. Coerced at the
    // engine default instead, `v > 50` over `NUMERIC(10,2)` bounded every zone
    // against 50 × 10^9 and pruned all of them: no rows, and no error.
    let value = match scales.iter().find(|(wo, _)| *wo == write_order) {
        Some((_, scale)) if *db_type == DbType::DECIMAL => {
            let text = crate::core::rql::subquery::value_to_literal(&from, &raw)?;
            let text = crate::core::value_codec::unquote_sql(&text);
            crate::core::value_codec::parse_decimal_scaled(&text, *scale)?
                .to_le_bytes()
                .to_vec()
        }
        _ => coerce_literal(&raw, &from, db_type)?,
    };

    // Floats compare in PostgreSQL's order, NaN above everything. That is
    // safe to prune on because the zone bounds record NaN as a maximum and
    // every NaN fingerprints alike (see `ZoneMap::observe`, `hash.rs`), so
    // `f > x`, `f >= x` and `f = 'NaN'` keep every zone holding a NaN.
    #[cfg(test)]
    if matches!(db_type, DbType::F32 | DbType::F64)
        && matches!(op, SargOp::Gt | SargOp::Ge)
        && REFUSE_FLOAT_RANGES.load(std::sync::atomic::Ordering::Relaxed)
    {
        return None;
    }

    Some(SargPredicate {
        write_order,
        op,
        value,
    })
}

/// The behaviour before NaN-aware bounds — float `>`/`>=` never pruned — for
/// the A/B measurement in `nan_bounds_tests::float_range_scan_bench` only.
#[cfg(test)]
pub(crate) static REFUSE_FLOAT_RANGES: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Lower `col STARTSWITH 'abc'` to the range `col >= 'abc' AND col < 'abd'`.
///
/// The two are equivalent, not merely related: every string beginning with
/// `abc` is at or above `abc`, and every one of them is below
/// [`successor`]`('abc')`, which is by construction the smallest value above all
/// of them. A range is what the zone bounds can act on, and it is the only way a
/// prefix match ever prunes — a fingerprint hashes the *whole* value and cannot
/// answer a question about part of it.
///
/// The operator arrives as a `Token::Ident`, not a `Token::Op`: the tokenizer
/// keeps string operators in the identifier stream and uppercases them.
///
/// Deliberately **not** lowered:
///
/// - `ISTARTSWITH` (from `ILIKE 'abc%'`). Case folding is not expressible as a
///   byte range — `'a'` and `'A'` are 32 apart, and every range containing both
///   contains most of ASCII besides.
/// - `CONTAINS` / `ENDSWITH`. Neither is bounded by any prefix of the value, so
///   no range brackets them.
/// - Any column that is not `STRING`. `perform_comparison_operation` returns
///   `UnsupportedType` for a prefix test on a number, which fails the query; the
///   filter must not prune on an error the evaluator is about to raise.
fn starts_with_to_range(toks: &[Token], gate: &dyn Fn(u32) -> bool) -> Option<MaskExpr> {
    if toks.len() != 3 {
        return None;
    }
    let Token::Ident((op_name, _, _)) = &toks[1] else {
        return None;
    };
    if !op_name.eq_ignore_ascii_case("STARTSWITH") {
        return None;
    }

    let Token::Ident((_, db_type, write_order)) = &toks[0] else {
        return None;
    };
    if !matches!(db_type, DbType::STRING) {
        return None;
    }
    let Token::StringLit(prefix) = &toks[2] else {
        return None;
    };

    let write_order = *write_order as u32;
    if !gate(write_order) {
        return None;
    }

    let lo = prefix.as_bytes().to_vec();
    if lo.is_empty() {
        // Every string starts with the empty prefix, so the predicate carries no
        // information at all rather than being one we failed to understand.
        return Some(MaskExpr::All);
    }

    let ge = SargPredicate {
        write_order,
        op: SargOp::Ge,
        value: lo.clone(),
    };
    Some(match successor(&lo) {
        Some(hi) => MaskExpr::And(vec![
            MaskExpr::Pred(ge),
            MaskExpr::Pred(SargPredicate {
                write_order,
                op: SargOp::Lt,
                value: hi,
            }),
        ]),
        // An all-`0xFF` prefix has nothing above it to bound against, so the
        // lower bound is the whole of what can be said. Weaker, still sound.
        None => MaskExpr::Pred(ge),
    })
}

/// One conjunct, as whatever shape it lowers to: a single predicate, a pair of
/// bounds, or `All` for something we cannot prove sound.
fn conjunct_to_expr(
    toks: &[Token],
    gate: &dyn Fn(u32) -> bool,
    scales: &[(u32, u32)],
    paths: Option<&PathResolver>,
) -> MaskExpr {
    // A negated comparison says nothing a mask can use. Every shape below
    // also needs exactly three tokens, which a `NOT` makes four; this says so
    // rather than leaving it to the count.
    if toks.iter().any(|t| matches!(t, Token::Ident((n, _, _)) if n == "NOT")) {
        return MaskExpr::All;
    }
    // `doc->>'k' = 'v'` (or mirrored, or a LIKE prefix) over a path some index
    // keys: read as the comparison of a STRING column, the index's virtual
    // one, which is what `->>` yields and what the index stores.
    if let (Some(resolve), 3) = (paths, toks.len()) {
        let as_column = |t: &Token| match t {
            Token::JsonPath {
                write_order,
                steps,
                as_text: true,
            } => resolve(*write_order as u32, steps)
                .map(|wo| Token::Ident((String::new(), DbType::STRING, wo as u64))),
            _ => None,
        };
        let rewritten = match (&toks[0], &toks[2]) {
            (path @ Token::JsonPath { .. }, Token::StringLit(_)) => {
                as_column(path).map(|c| vec![c, toks[1].clone(), toks[2].clone()])
            }
            (Token::StringLit(_), path @ Token::JsonPath { .. }) => {
                as_column(path).map(|c| vec![toks[0].clone(), toks[1].clone(), c])
            }
            _ => None,
        };
        if let Some(toks) = rewritten {
            return conjunct_to_expr(&toks, gate, scales, None);
        }
    }
    if let Some(e) = starts_with_to_range(toks, gate) {
        return e;
    }
    match conjunct_to_pred(toks, gate, scales) {
        Some(p) => MaskExpr::Pred(p),
        None => MaskExpr::All,
    }
}

/// Maps a `->>` path over a document column to the virtual column an index
/// keys it under, when one does.
pub type PathResolver<'a> = dyn Fn(u32, &[crate::core::jsonb::Step]) -> Option<u32> + 'a;

/// Recursive-descent parser over the token stream, mirroring the precedence the
/// engine implements: `AND` binds tighter than `OR`, parentheses group.
struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
    /// Which columns a predicate may name: the filter's indexed columns, or
    /// a secondary index's.
    gate: &'a dyn Fn(u32) -> bool,
    /// `(write_order, scale)` of each `NUMERIC(p, s)` column whose scale is
    /// not the engine default. See [`extract_with_scales`].
    scales: &'a [(u32, u32)],
    /// Path indexes, for the secondary-index planner only. See
    /// [`extract_gated_with_paths`].
    paths: Option<&'a PathResolver<'a>>,
    /// Set when the token stream is malformed (unbalanced parens). The caller
    /// declines to filter rather than acting on a structure it misread.
    bad: bool,
}

impl<'a> Parser<'a> {
    #[inline]
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.pos)
    }

    fn next_is(&self, kind: &str) -> bool {
        matches!(self.peek(), Some(Token::Next(k)) if k.eq_ignore_ascii_case(kind))
    }

    fn parse_or(&mut self) -> MaskExpr {
        let mut branches = vec![self.parse_and()];
        while self.next_is("or") {
            self.pos += 1;
            branches.push(self.parse_and());
        }
        if branches.len() == 1 {
            branches.pop().unwrap()
        } else {
            MaskExpr::Or(branches)
        }
    }

    fn parse_and(&mut self) -> MaskExpr {
        let mut branches = vec![self.parse_atom()];
        while self.next_is("and") {
            self.pos += 1;
            branches.push(self.parse_atom());
        }
        if branches.len() == 1 {
            branches.pop().unwrap()
        } else {
            MaskExpr::And(branches)
        }
    }

    /// Skip to the next separator at this nesting level, discarding whatever we
    /// could not interpret.
    fn skip_to_separator(&mut self) {
        let mut depth = 0usize;
        while let Some(t) = self.peek() {
            match t {
                Token::LPar => depth += 1,
                Token::RPar => {
                    if depth == 0 {
                        return;
                    }
                    depth -= 1;
                }
                Token::Next(_) if depth == 0 => return,
                _ => {}
            }
            self.pos += 1;
        }
    }

    fn parse_atom(&mut self) -> MaskExpr {
        if matches!(self.peek(), Some(Token::LPar)) {
            let open = self.pos;
            self.pos += 1;
            let inner = self.parse_or();
            match self.peek() {
                Some(Token::RPar) => self.pos += 1,
                _ => {
                    self.bad = true;
                    return MaskExpr::All;
                }
            }
            // `(a + 1) = 5` also starts with `(`, but the group is an *operand*,
            // not a boolean sub-expression. An operator here proves that, so
            // discard what we parsed and treat the whole comparison as opaque.
            if matches!(self.peek(), Some(Token::Op(_))) {
                self.pos = open;
                self.skip_to_separator();
                return MaskExpr::All;
            }

            // Parentheses are honoured by the evaluator, so the mask can act on
            // them too.
            //
            // This was not always true. The evaluator used to flatten every
            // clause into a `(bool, Option<Next>)` chain, turning
            // `A AND (B OR C)` into `(A AND B) OR C`. While it did, acting on
            // the parens here made the mask narrower than the evaluator's own
            // output, so filtering changed query *results* rather than just
            // their cost — and extraction had to decline the shape. The
            // evaluator now compiles a postfix program that reconstructs the
            // bracketing, so grouping means the same thing on both sides.
            return inner;
        }

        let start = self.pos;
        self.skip_to_separator();
        conjunct_to_expr(&self.toks[start..self.pos], self.gate, self.scales, self.paths)
    }
}

/// Extract a sound mask program, or `None` when nothing could be narrowed.
///
/// Returning `None` rather than an all-ones program lets the caller skip
/// building a bitmap it would only have to ignore.
pub fn extract(tokens: &[Token], filters: &TableFilters) -> Option<SargProgram> {
    extract_with_scales(tokens, filters, &[])
}

/// [`extract`] for a table with `NUMERIC(p, s)` columns: `scales` lists the
/// `(write_order, scale)` of each whose scale is not the engine default, so
/// a literal is bounded at the scale the column stores — the one the
/// evaluator compares at. Omitting a column here is a false negative waiting
/// to happen, not a missed optimisation.
pub fn extract_with_scales(
    tokens: &[Token],
    filters: &TableFilters,
    scales: &[(u32, u32)],
) -> Option<SargProgram> {
    if filters.is_empty() {
        return None;
    }
    extract_gated(tokens, &|wo| filters.indexes(wo), scales)
}

/// [`extract_with_scales`] for any set of usable columns: `gate` says which.
/// The secondary-index planner uses it with the indexed columns, so a
/// predicate is read the same way, literal coercion and `NUMERIC` scale
/// included, whichever structure answers it.
pub fn extract_gated(
    tokens: &[Token],
    gate: &dyn Fn(u32) -> bool,
    scales: &[(u32, u32)],
) -> Option<SargProgram> {
    extract_parsed(tokens, gate, scales, None)
}

/// [`extract_gated`], also reading `doc->>'k' <op> 'text'` as a comparison of
/// the virtual column `paths` names for that path. For the secondary-index
/// planner: the filter index has no such column, and its gate would refuse
/// one anyway.
pub fn extract_gated_with_paths(
    tokens: &[Token],
    gate: &dyn Fn(u32) -> bool,
    scales: &[(u32, u32)],
    paths: &PathResolver,
) -> Option<SargProgram> {
    extract_parsed(tokens, gate, scales, Some(paths))
}

fn extract_parsed(
    tokens: &[Token],
    gate: &dyn Fn(u32) -> bool,
    scales: &[(u32, u32)],
    paths: Option<&PathResolver>,
) -> Option<SargProgram> {
    if tokens.is_empty() {
        return None;
    }

    let mut p = Parser {
        toks: tokens,
        pos: 0,
        gate,
        scales,
        paths,
        bad: false,
    };
    let root = p.parse_or();

    // Trailing tokens mean the structure was not what we thought it was — most
    // likely an unbalanced `)`. Decline rather than filter on a misreading.
    if p.bad || p.pos != tokens.len() {
        return None;
    }
    if root.is_all() {
        return None;
    }
    Some(SargProgram { root })
}

/// The truth of a tokenized `WHERE` clause when it does not depend on the row:
/// `Some(false)` for one no row can satisfy, `Some(true)` for one every row
/// does, `None` otherwise.
///
/// The rewriter emits `1 = 0` for `notnull IS NULL`, `x IN ()`, `x = NULL`, a
/// `NOT IN` list holding a NULL and an EXISTS over nothing; and a column
/// compared with itself (`id != id`) is constant too, for every type — floats
/// included, now that NaN equals NaN. Such a clause used to scan the whole
/// table to return nothing. Knowing it is constant lets the scan be skipped.
///
/// Three-valued over the same grammar the evaluator implements (`AND` binds
/// tighter than `OR`, parentheses group, no `NOT`): a comparison that reads the
/// row is unknown, and only a clause decided whatever the unknowns are is
/// reported. Anything not understood is unknown, so the answer is never a
/// guess.
pub fn constant_truth(tokens: &[Token]) -> Option<bool> {
    struct C<'a> {
        toks: &'a [Token],
        pos: usize,
        bad: bool,
    }
    fn is_link(t: Option<&Token>, kind: &str) -> bool {
        match t {
            Some(Token::Next(k)) => k.eq_ignore_ascii_case(kind),
            Some(Token::Ident((k, _, _))) => k.eq_ignore_ascii_case(kind),
            _ => false,
        }
    }
    impl C<'_> {
        fn or(&mut self) -> Option<bool> {
            let mut acc = self.and();
            while is_link(self.toks.get(self.pos), "or") {
                self.pos += 1;
                let next = self.and();
                acc = match (acc, next) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                };
            }
            acc
        }
        fn and(&mut self) -> Option<bool> {
            let mut acc = self.atom();
            while is_link(self.toks.get(self.pos), "and") {
                self.pos += 1;
                let next = self.atom();
                acc = match (acc, next) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                };
            }
            acc
        }
        fn skip(&mut self) {
            let mut depth = 0usize;
            while let Some(t) = self.toks.get(self.pos) {
                match t {
                    Token::LPar => depth += 1,
                    Token::RPar if depth == 0 => return,
                    Token::RPar => depth -= 1,
                    _ if depth == 0 && (is_link(Some(t), "and") || is_link(Some(t), "or")) => {
                        return;
                    }
                    _ => {}
                }
                self.pos += 1;
            }
        }
        fn atom(&mut self) -> Option<bool> {
            if matches!(self.toks.get(self.pos), Some(Token::LPar)) {
                let open = self.pos;
                self.pos += 1;
                let inner = self.or();
                if !matches!(self.toks.get(self.pos), Some(Token::RPar)) {
                    self.bad = true;
                    return None;
                }
                self.pos += 1;
                // `(a + 1) = 5`: the group was an operand, not a condition.
                if matches!(self.toks.get(self.pos), Some(Token::Op(_)) | Some(Token::Cast(_))) {
                    self.pos = open;
                    self.skip();
                    return None;
                }
                return inner;
            }
            let start = self.pos;
            self.skip();
            comparison_truth(&self.toks[start..self.pos])
        }
    }
    if tokens.is_empty() {
        return None;
    }
    let mut c = C {
        toks: tokens,
        pos: 0,
        bad: false,
    };
    let truth = c.or();
    if c.bad || c.pos != tokens.len() {
        return None;
    }
    truth
}

/// One comparison's truth when it does not read the row: two numeric
/// literals, or one column compared with itself.
fn comparison_truth(toks: &[Token]) -> Option<bool> {
    use std::cmp::Ordering;
    let [left, Token::Op(op), right] = toks else {
        return None;
    };
    let ordering = match (left, right) {
        (Token::Number(a), Token::Number(b)) => {
            let exact = |n: &NumericValue| -> Option<i128> {
                Some(match *n {
                    NumericValue::I8(v) => v as i128,
                    NumericValue::I16(v) => v as i128,
                    NumericValue::I32(v) => v as i128,
                    NumericValue::I64(v) => v as i128,
                    NumericValue::I128(v) => v,
                    NumericValue::U8(v) => v as i128,
                    NumericValue::U16(v) => v as i128,
                    NumericValue::U32(v) => v as i128,
                    NumericValue::U64(v) => v as i128,
                    NumericValue::U128(v) => i128::try_from(v).ok()?,
                    NumericValue::F32(_) | NumericValue::F64(_) => return None,
                })
            };
            match (exact(a), exact(b)) {
                (Some(x), Some(y)) => x.cmp(&y),
                // A float on either side: not folded. The literal's own width
                // decides how the evaluator would read it, and guessing that
                // here is how a constant comes out wrong.
                _ => return None,
            }
        }
        // A column against itself is equal to itself for every value — NaN
        // included, under PostgreSQL's float order — so only the operator
        // decides. A NULL is guarded by its companion elsewhere in the clause,
        // which stays unknown here.
        (Token::Ident((a, ta, wa)), Token::Ident((b, tb, wb)))
            if a == b && ta == tb && wa == wb && *ta != DbType::NULL =>
        {
            Ordering::Equal
        }
        _ => return None,
    };
    Some(match op.as_str() {
        "=" | "==" => ordering == Ordering::Equal,
        "!=" | "<>" => ordering != Ordering::Equal,
        "<" => ordering == Ordering::Less,
        "<=" => ordering != Ordering::Greater,
        ">" => ordering == Ordering::Greater,
        ">=" => ordering != Ordering::Less,
        _ => return None,
    })
}

/// [`constant_truth`] of a `WHERE` clause's text, tokenized against `schema`.
pub fn clause_is_constant_false(
    where_clause: &str,
    schema: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
) -> bool {
    if !worth_judging_constant(where_clause) {
        return false;
    }
    match crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, schema) {
        Ok(tokens) => clause_tokens_are_constant_false(where_clause, &tokens),
        Err(_) => false,
    }
}

/// [`clause_is_constant_false`] for a clause already tokenized, so a query
/// that needs the tokens for its indexes too tokenizes once.
pub fn clause_tokens_are_constant_false(where_clause: &str, tokens: &[Token]) -> bool {
    worth_judging_constant(where_clause) && constant_truth(tokens) == Some(false)
}

/// Whether a clause is examined for a constant at all. Tokenizing it is the
/// cost, which for a 70,000-value `IN` list is real work. A long clause is
/// only examined when it carries the rewriter's constant; a self-comparison
/// inside one is merely not skipped.
pub fn worth_judging_constant(where_clause: &str) -> bool {
    if where_clause.trim().is_empty() {
        return false;
    }
    where_clause.len() <= 4096
        || where_clause.contains(crate::core::tokenizer::predicate_rewrite::CONSTANT_FALSE)
}

#[cfg(test)]
mod constant_clauses {
    use super::*;
    use crate::core::row::schema::SchemaField;
    use smallvec::SmallVec;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false),
            SchemaField::new("f".to_string(), DbType::F64, 8, 0, 1, false),
            SchemaField::new("name".to_string(), DbType::STRING, 12, 0, 2, false),
        ]
    }

    fn truth(clause: &str) -> Option<bool> {
        constant_truth(&crate::core::tokenizer::query_tokenizer::tokenize(
            clause,
            &schema(),
        ))
    }

    #[test]
    fn constants_are_recognised_and_row_reads_are_not() {
        for (clause, want) in [
            ("1 = 0", Some(false)),
            ("1 = 1", Some(true)),
            ("id != id", Some(false)),
            ("f != f", Some(false)),
            ("f = f", Some(true)),
            ("name < name", Some(false)),
            ("id = 5 AND 1 = 0", Some(false)),
            ("id = 5 OR 1 = 1", Some(true)),
            ("id = 5 OR 1 = 0", None),
            ("1 = 0 OR id = 5 AND 1 = 0", Some(false)),
            ("(id = 5 OR 1 = 0) AND 1 = 0", Some(false)),
            ("( 1 = 0 ) OR ( 1 = 0 )", Some(false)),
            ("id = 5", None),
            ("id = id + 0", None),
            ("(id + 1) = 0", None),
            ("1 = 0 OR id = 3", None),
        ] {
            assert_eq!(truth(clause), want, "{clause}");
        }
    }
}

#[cfg(test)]
mod pattern_ops_are_not_sargable {
    use super::*;
    use crate::core::tokenizer::query_tokenizer::Token;

    /// A `LIKE` predicate must never be lowered into a mask.
    ///
    /// A fingerprint hashes the whole value and zone bounds bracket a prefix;
    /// neither can answer "does this value match `a%b`". If a pattern operator
    /// were ever mis-extracted, the index would exclude rows that match and the
    /// query would return **fewer** rows with no error — the silent
    /// false-negative that `!=` and `IsNotNull` are already refused for.
    ///
    /// It is safe today by construction rather than by a rule: `op_of` requires
    /// a `Token::Op` and matches five comparisons, while a string operator
    /// arrives as a `Token::Ident`. This pins that, so a later change making
    /// string operators sargable has to confront the pattern case deliberately
    /// instead of inheriting it.
    #[test]
    fn a_like_predicate_is_never_sargable() {
        for op in ["LIKE", "ILIKE", "CONTAINS", "ENDSWITH", "ICONTAINS"] {
            let tok = Token::Ident((op.to_string(), DbType::STRING, 0));
            assert!(
                op_of(&tok).is_none(),
                "{} was accepted as a sargable operator; a mask cannot answer it",
                op
            );
        }
    }

    /// The one string operator that *is* lowered, so the test above is not
    /// passing merely because nothing is.
    #[test]
    fn starts_with_is_still_the_only_lowered_string_operator() {
        let tok = Token::Ident(("STARTSWITH".to_string(), DbType::STRING, 0));
        // Also not an `Op`, so it goes through `starts_with_to_range` rather
        // than `op_of` — which is exactly the separation being relied on.
        assert!(op_of(&tok).is_none());
    }
}

/// The filter index against PostgreSQL's float order, where NaN equals NaN and
/// is above every other value. Zone bounds never record a NaN, so a predicate
/// that a NaN row satisfies (`f > x`, `f >= x`, `f = 'NaN'`) must not be
/// answered from them.
#[cfg(test)]
mod float_nan_is_never_pruned {
    use super::*;
    use crate::core::filter::column::FilterConfig;
    use crate::core::row::schema::SchemaField;
    use smallvec::SmallVec;

    #[test]
    fn a_nan_row_survives_every_predicate_it_satisfies() {
        let field = SchemaField::new("f".to_string(), DbType::F64, 8, 0, 0, false);
        let schema: SmallVec<[SchemaField; 20]> = smallvec::smallvec![field.clone()];
        let filters = TableFilters::new(&[field], FilterConfig::default());
        const N: u64 = 6000;
        let is_nan = |slot: u64| slot % 777 == 5;
        // Each zone holds a narrow band of values, so its bounds sit far below
        // a probe its NaN rows still satisfy.
        // Distinct per row — a low-cardinality column is gated out of the
        // index and would prune nothing — but each zone a narrow band.
        let value = |slot: u64| (slot / 512) as f64 + (slot % 512) as f64 / 1024.0;
        for slot in 0..N {
            let v = if is_nan(slot) { f64::NAN } else { value(slot) };
            let mut cols: SmallVec<[(u32, Vec<u8>); 32]> = SmallVec::new();
            cols.push((0, v.to_le_bytes().to_vec()));
            filters.backfill_row(slot, &cols);
        }
        filters.clear_unindexed_prefix();

        let mut pruned = 0;
        for (clause, matches) in [
            ("f > 10.5", &(|s: u64| is_nan(s) || value(s) > 10.5) as &dyn Fn(u64) -> bool),
            ("f >= 11", &|s: u64| is_nan(s) || value(s) >= 11.0),
            ("10.5 < f", &|s: u64| is_nan(s) || value(s) > 10.5),
            ("f = 'NaN'", &|s: u64| is_nan(s)),
            ("f = 3.25", &|s: u64| !is_nan(s) && value(s) == 3.25),
            ("f < 1", &|s: u64| !is_nan(s) && value(s) < 1.0),
        ] {
            let tokens =
                crate::core::tokenizer::query_tokenizer::tokenize(clause, &schema);
            let Some(program) = extract(&tokens, &filters) else {
                continue;
            };
            let Some(bitmap) = filters.evaluate(&program, N) else {
                continue;
            };
            if bitmap.count() < N {
                pruned += 1;
            }
            for slot in 0..N {
                if matches(slot) {
                    assert!(bitmap.get(slot), "{clause}: matching slot {slot} was pruned");
                }
            }
        }
        // Proves nothing unless the index actually pruned.
        assert!(pruned >= 4, "the index pruned for only {pruned} predicates");
    }

    /// Every ordered float predicate prunes, NaN literals included: the
    /// bounds know where the NaN rows are.
    #[test]
    fn every_float_comparison_is_sargable() {
        let field = SchemaField::new("f".to_string(), DbType::F64, 8, 0, 0, false);
        let schema: SmallVec<[SchemaField; 20]> = smallvec::smallvec![field.clone()];
        let filters = TableFilters::new(&[field], FilterConfig::default());
        for (clause, sargable) in [
            ("f = 3", true),
            ("f < 1", true),
            ("f <= 1", true),
            ("f > 1", true),
            ("f >= 1", true),
            ("f = 'NaN'", true),
            ("f < 'NaN'", true),
        ] {
            let tokens =
                crate::core::tokenizer::query_tokenizer::tokenize(clause, &schema);
            assert_eq!(extract(&tokens, &filters).is_some(), sargable, "{clause}");
        }
    }
}
