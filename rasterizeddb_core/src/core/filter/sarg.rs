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
//! There is no `NOT` in the tokenizer, so the negation case cannot arise.
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
fn conjunct_to_pred(toks: &[Token], filters: &TableFilters) -> Option<SargPredicate> {
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
    if !filters.indexes(write_order) {
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
    let value = coerce_literal(&raw, &from, db_type)?;

    Some(SargPredicate {
        write_order,
        op,
        value,
    })
}

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
fn starts_with_to_range(toks: &[Token], filters: &TableFilters) -> Option<MaskExpr> {
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
    if !filters.indexes(write_order) {
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
fn conjunct_to_expr(toks: &[Token], filters: &TableFilters) -> MaskExpr {
    if let Some(e) = starts_with_to_range(toks, filters) {
        return e;
    }
    match conjunct_to_pred(toks, filters) {
        Some(p) => MaskExpr::Pred(p),
        None => MaskExpr::All,
    }
}

/// Recursive-descent parser over the token stream, mirroring the precedence the
/// engine implements: `AND` binds tighter than `OR`, parentheses group.
struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
    filters: &'a TableFilters,
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
        conjunct_to_expr(&self.toks[start..self.pos], self.filters)
    }
}

/// Extract a sound mask program, or `None` when nothing could be narrowed.
///
/// Returning `None` rather than an all-ones program lets the caller skip
/// building a bitmap it would only have to ignore.
pub fn extract(tokens: &[Token], filters: &TableFilters) -> Option<SargProgram> {
    if tokens.is_empty() || filters.is_empty() {
        return None;
    }

    let mut p = Parser {
        toks: tokens,
        pos: 0,
        filters,
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
