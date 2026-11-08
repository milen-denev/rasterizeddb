//! Desugars SQL predicate forms into the comparison grammar the engine already
//! evaluates.
//!
//! The evaluator understands exactly one shape: `column <op> value`, combined
//! with `AND` / `OR` / parentheses. Rather than teach the tokenizer, the parser
//! and the transformer about each additional predicate form, the equivalent
//! basic expression is produced here first:
//!
//! ```text
//! x IN (1, 2)          =>  (x = 1 OR x = 2)
//! x NOT IN (1, 2)      =>  (x != 1 AND x != 2)
//! x BETWEEN 1 AND 9    =>  (x >= 1 AND x <= 9)
//! x LIKE 'ab%'         =>  x STARTSWITH 'ab'
//! t.x = 1              =>  x = 1
//! flag = true          =>  flag = 1
//! ```
//!
//! Two consequences are deliberate. The filter index sees the rewritten form,
//! so an `IN` list becomes several indexable equality probes instead of one
//! opaque predicate it would have to skip. And a form with no equivalent —
//! `IS NULL` until nullability lands, or a wildcard in the middle of a `LIKE`
//! pattern — returns an error naming the feature, rather than reaching a
//! tokenizer that would panic on it.

use smallvec::SmallVec;

use crate::core::row::schema::SchemaField;
use crate::core::rql::nullable;

/// The predicate that is false for every row, NaN and NULL included, in the
/// grammar the evaluator reads. The same spelling `fold_null_comparisons` and
/// the subquery rewrites use, which is what lets the planner recognise it.
pub const CONSTANT_FALSE: &str = "1 = 0";
/// Its complement, true for every row.
pub const CONSTANT_TRUE: &str = "1 = 1";

/// The index just past the single-quoted literal opening at `open`, with `''`
/// as an escaped quote.
fn quoted_end(bytes: &[u8], open: usize) -> Option<usize> {
    let mut j = open + 1;
    loop {
        match bytes.get(j)? {
            b'\'' if bytes.get(j + 1) == Some(&b'\'') => j += 2,
            b'\'' => return Some(j + 1),
            _ => j += 1,
        }
    }
}

/// The key list of `?|` / `?&` starting at `at`: `'{a,b}'` or
/// `ARRAY['a', 'b']` (with an optional `::text[]` after either), rendered as
/// one quoted `'{…}'` literal, and where it ended.
fn key_list_operand(clause: &str, at: usize) -> Option<(String, usize)> {
    let bytes = clause.as_bytes();
    let (array_text, mut end) = if bytes.get(at) == Some(&b'\'') {
        let end = quoted_end(bytes, at)?;
        let text = clause[at + 1..end - 1].replace("''", "'");
        (text, end)
    } else if clause.get(at..at + 5)?.eq_ignore_ascii_case("array") {
        let mut j = at + 5;
        while bytes.get(j).is_some_and(|b| b.is_ascii_whitespace()) {
            j += 1;
        }
        if bytes.get(j) != Some(&b'[') {
            return None;
        }
        j += 1;
        let mut elements: Vec<String> = Vec::new();
        loop {
            while bytes.get(j).is_some_and(|b| b.is_ascii_whitespace()) {
                j += 1;
            }
            match bytes.get(j)? {
                b']' if elements.is_empty() => {
                    j += 1;
                    break;
                }
                b'\'' => {
                    let end = quoted_end(bytes, j)?;
                    let text = clause[j + 1..end - 1].replace("''", "'");
                    elements.push(format!(
                        "\"{}\"",
                        text.replace('\\', "\\\\").replace('"', "\\\"")
                    ));
                    j = end;
                }
                _ if clause.get(j..j + 4).is_some_and(|w| w.eq_ignore_ascii_case("null")) => {
                    elements.push("NULL".to_string());
                    j += 4;
                }
                _ => return None,
            }
            while bytes.get(j).is_some_and(|b| b.is_ascii_whitespace()) {
                j += 1;
            }
            match bytes.get(j)? {
                b',' => j += 1,
                b']' => {
                    j += 1;
                    break;
                }
                _ => return None,
            }
        }
        (format!("{{{}}}", elements.join(",")), j)
    } else {
        return None;
    };
    // `::text[]` / `::varchar[]` names the type the list already is.
    let mut k = end;
    while bytes.get(k).is_some_and(|b| b.is_ascii_whitespace()) {
        k += 1;
    }
    if clause.get(k..k + 2) == Some("::") {
        let mut m = k + 2;
        while bytes
            .get(m)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b' ')
        {
            m += 1;
        }
        if clause.get(m..m + 2) == Some("[]") {
            end = m + 2;
        }
    }
    // Validated now, so a malformed list fails the statement here rather than
    // every row.
    text_array_elements(&array_text)?;
    Some((quote_standard(&array_text), end))
}

/// The elements of a PostgreSQL text-array literal's *text* — `{a,"b c",NULL}`
/// — with an unquoted `NULL` as `None`. `None` overall for text that is not an
/// array literal.
pub fn text_array_elements(text: &str) -> Option<Vec<Option<String>>> {
    let inner = text.trim().strip_prefix('{')?.strip_suffix('}')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let mut escaped = false;
    let finish = |current: &mut String, quoted: &mut bool, out: &mut Vec<Option<String>>| {
        let element = if *quoted {
            Some(std::mem::take(current))
        } else {
            let t = current.trim().to_string();
            current.clear();
            if t.eq_ignore_ascii_case("null") {
                None
            } else {
                Some(t)
            }
        };
        *quoted = false;
        out.push(element);
    };
    for c in inner.chars() {
        if escaped {
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => escaped = true,
            '"' => {
                in_quotes = !in_quotes;
                quoted = true;
            }
            ',' if !in_quotes => finish(&mut current, &mut quoted, &mut out),
            _ => current.push(c),
        }
    }
    if in_quotes || escaped {
        return None;
    }
    finish(&mut current, &mut quoted, &mut out);
    Some(out)
}

/// `TRUE`, `FALSE` or `UNKNOWN` after `IS [NOT]`.
fn truth_keyword(tok: &Tok) -> Option<&'static str> {
    ["true", "false", "unknown"]
        .into_iter()
        .find(|k| tok.is_keyword(k))
}

/// The comparison operator that holds exactly where `op` does not, for a
/// non-NULL operand. Exact under every ordering the evaluator uses — floats
/// included, since PostgreSQL's order is total (NaN is the largest value).
fn negated_operator(op: &str) -> Option<&'static str> {
    Some(match op {
        "=" => "!=",
        "!=" | "<>" => "=",
        "<" => ">=",
        "<=" => ">",
        ">" => "<=",
        ">=" => "<",
        _ => return None,
    })
}

/// `x IS [NOT] TRUE | FALSE | UNKNOWN`, with PostgreSQL's three-valued
/// meaning, as the two-valued grammar the evaluator reads.
///
/// `x` is a boolean column (`comparison` is `None`) or the comparison
/// `col <op> literal`. The value is NULL exactly when the column is, so the
/// companion carries the third truth value:
///
/// ```text
/// IS TRUE       ->  not null AND  x         IS NOT TRUE   ->  null OR NOT x
/// IS FALSE      ->  not null AND  NOT x     IS NOT FALSE  ->  null OR x
/// IS UNKNOWN    ->  null                    IS NOT UNKNOWN ->  not null
/// ```
///
/// On a NOT NULL column the companion terms fall away and each test is the
/// plain comparison or its complement, or a constant for `UNKNOWN`.
fn truth_test(
    col: &str,
    comparison: Option<(&str, String)>,
    truth: &str,
    negated: bool,
    schema: &SmallVec<[SchemaField; 20]>,
) -> String {
    let (holds, fails) = match &comparison {
        None => (format!("{} = 1", col), format!("{} = 0", col)),
        Some((op, value)) => (
            format!("{} {} {}", col, op, value),
            format!(
                "{} {} {}",
                col,
                negated_operator(op).expect("checked by the caller"),
                value
            ),
        ),
    };
    let flag = nullable::flag_field(col, schema).map(|f| f.name.clone());
    match (truth, negated, flag) {
        ("unknown", false, Some(f)) => format!("{} = 1", f),
        ("unknown", true, Some(f)) => format!("{} = 0", f),
        ("unknown", false, None) => CONSTANT_FALSE.to_string(),
        ("unknown", true, None) => CONSTANT_TRUE.to_string(),
        (t, false, flag) => {
            let test = if t == "true" { holds } else { fails };
            match flag {
                Some(f) => format!("({} = 0 AND {})", f, test),
                None => test,
            }
        }
        (t, true, flag) => {
            let test = if t == "true" { fails } else { holds };
            match flag {
                Some(f) => format!("({} = 1 OR {})", f, test),
                None => test,
            }
        }
    }
}

/// Rewrites every `( <expression> ) IS [NOT] TRUE | FALSE | UNKNOWN` in the
/// clause into its two-valued expansion, leaving everything else as it was.
fn expand_truth_tests<'c>(
    clause: &'c str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<std::borrow::Cow<'c, str>, String> {
    let upper = clause.to_ascii_uppercase();
    let maybe = upper.contains("IS")
        && (upper.contains("TRUE") || upper.contains("FALSE") || upper.contains("UNKNOWN"));
    if !maybe {
        return Ok(std::borrow::Cow::Borrowed(clause));
    }
    let toks = lex(clause)?;
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    let mut changed = false;
    let mut i = 0usize;
    while i < toks.len() {
        if matches!(toks[i], Tok::LPar) {
            if let Some((text, after)) = parenthesised_truth_test(&toks, i, schema)? {
                out.push(text);
                i = after;
                changed = true;
                continue;
            }
        }
        out.push(toks[i].render());
        i += 1;
    }
    Ok(if changed {
        std::borrow::Cow::Owned(out.join(" "))
    } else {
        std::borrow::Cow::Borrowed(clause)
    })
}

/// `( <boolean expression> ) IS [NOT] TRUE | FALSE | UNKNOWN` starting at the
/// parenthesis at `at`: the rewritten text and the index past it, or `None`
/// when the tokens are not that shape.
///
/// The expression is AND/OR/NOT over comparisons (`a > b`, `a + 1 <= 5`) and
/// boolean columns. SQL gives each of those three values — a comparison is
/// NULL exactly when a column it reads is — and the evaluator has two, so the
/// test is expanded into four two-valued formulas built bottom-up:
///
/// ```text
///            T (is true)          F (is false)        NT (not true)         NF (not false)
/// a op b     nn AND a op b        nn AND a !op b      null OR a !op b       null OR a op b
/// A AND B    T(A) AND T(B)        F(A) OR F(B)        NT(A) OR NT(B)        NF(A) AND NF(B)
/// A OR B     T(A) OR T(B)         F(A) AND F(B)       NT(A) AND NT(B)       NF(A) OR NF(B)
/// NOT A      F(A)                 T(A)                NF(A)                 NT(A)
/// ```
///
/// where `nn` is "every nullable column it reads is set" and `null` its
/// complement, and `!op` the complementary operator (exact: every order the
/// evaluator uses is total, floats included). `IS UNKNOWN` is `NT AND NF`,
/// `IS NOT UNKNOWN` is `T OR F`. Anything else inside — a function, `LIKE`,
/// `IN`, a NULL literal — is refused by name, never approximated.
fn parenthesised_truth_test(
    toks: &[Tok],
    at: usize,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Option<(String, usize)>, String> {
    // The group's extent.
    let mut depth = 0usize;
    let mut close = None;
    for (i, t) in toks.iter().enumerate().skip(at) {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                depth -= 1;
                if depth == 0 {
                    close = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close) = close else {
        return Ok(None);
    };
    if !toks.get(close + 1).is_some_and(|t| t.is_keyword("is")) {
        return Ok(None);
    }
    let negated = toks.get(close + 2).is_some_and(|t| t.is_keyword("not"));
    let truth_at = close + 2 + usize::from(negated);
    let Some(truth) = toks.get(truth_at).and_then(truth_keyword) else {
        return Ok(None);
    };
    let inner = &toks[at + 1..close];
    let tree = TruthParser {
        toks: inner,
        pos: 0,
        schema,
    }
    .parse()
    .map_err(|e| {
        format!(
            "IS {}{} over this expression is not supported: {}",
            if negated { "NOT " } else { "" },
            truth.to_ascii_uppercase(),
            e
        )
    })?;
    let text = match (truth, negated) {
        ("true", false) => tree.render(Want::T),
        ("true", true) => tree.render(Want::NT),
        ("false", false) => tree.render(Want::F),
        ("false", true) => tree.render(Want::NF),
        (_, false) => format!("({} AND {})", tree.render(Want::NT), tree.render(Want::NF)),
        (_, true) => format!("({} OR {})", tree.render(Want::T), tree.render(Want::F)),
    };
    Ok(Some((text, truth_at + 1)))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Want {
    T,
    F,
    NT,
    NF,
}

enum TruthNode {
    /// A comparison, rendered with its operator and with the complementary
    /// one, and the companions of the nullable columns it reads.
    Atom {
        holds: String,
        fails: String,
        flags: Vec<String>,
    },
    /// A comparison with a NULL literal: unknown for every row.
    Unknown,
    And(Box<TruthNode>, Box<TruthNode>),
    Or(Box<TruthNode>, Box<TruthNode>),
    Not(Box<TruthNode>),
}

impl TruthNode {
    fn render(&self, want: Want) -> String {
        match self {
            TruthNode::Atom {
                holds,
                fails,
                flags,
            } => {
                let (test, guarded) = match want {
                    Want::T => (holds, true),
                    Want::F => (fails, true),
                    Want::NT => (fails, false),
                    Want::NF => (holds, false),
                };
                if flags.is_empty() {
                    return format!("( {} )", test);
                }
                // A lifted operand is its own NULL test: it has no companion,
                // and the evaluator says whether it yielded a value.
                let mut parts: Vec<String> = flags
                    .iter()
                    .map(|f| {
                        if f.as_bytes().first() == Some(&LIFTED_OPEN) {
                            format!("{} {}", f, if guarded { "NOTNULL" } else { "ISNULL" })
                        } else {
                            format!("{} = {}", f, if guarded { 0 } else { 1 })
                        }
                    })
                    .collect();
                parts.push(test.clone());
                format!("( {} )", parts.join(if guarded { " AND " } else { " OR " }))
            }
            TruthNode::Unknown => match want {
                Want::T | Want::F => CONSTANT_FALSE.to_string(),
                Want::NT | Want::NF => CONSTANT_TRUE.to_string(),
            },
            TruthNode::Not(inner) => inner.render(match want {
                Want::T => Want::F,
                Want::F => Want::T,
                Want::NT => Want::NF,
                Want::NF => Want::NT,
            }),
            TruthNode::And(a, b) => {
                let joiner = match want {
                    Want::T | Want::NF => " AND ",
                    Want::F | Want::NT => " OR ",
                };
                format!("( {}{}{} )", a.render(want), joiner, b.render(want))
            }
            TruthNode::Or(a, b) => {
                let joiner = match want {
                    Want::T | Want::NF => " OR ",
                    Want::F | Want::NT => " AND ",
                };
                format!("( {}{}{} )", a.render(want), joiner, b.render(want))
            }
        }
    }
}

struct TruthParser<'a> {
    toks: &'a [Tok],
    pos: usize,
    schema: &'a SmallVec<[SchemaField; 20]>,
}

impl TruthParser<'_> {
    fn parse(mut self) -> Result<TruthNode, String> {
        if self.toks.is_empty() {
            return Err("the parentheses are empty".to_string());
        }
        let node = self.or()?;
        if self.pos != self.toks.len() {
            return Err(format!("unexpected '{}'", self.toks[self.pos].render()));
        }
        Ok(node)
    }

    fn or(&mut self) -> Result<TruthNode, String> {
        let mut left = self.and()?;
        while self.toks.get(self.pos).is_some_and(|t| t.is_keyword("or")) {
            self.pos += 1;
            left = TruthNode::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<TruthNode, String> {
        let mut left = self.not()?;
        while self.toks.get(self.pos).is_some_and(|t| t.is_keyword("and")) {
            self.pos += 1;
            left = TruthNode::And(Box::new(left), Box::new(self.not()?));
        }
        Ok(left)
    }

    fn not(&mut self) -> Result<TruthNode, String> {
        if self.toks.get(self.pos).is_some_and(|t| t.is_keyword("not")) {
            self.pos += 1;
            return Ok(TruthNode::Not(Box::new(self.not()?)));
        }
        // A parenthesis opening a boolean group, rather than an operand.
        if matches!(self.toks.get(self.pos), Some(Tok::LPar)) && opens_boolean_group(self.toks, self.pos) {
            let mut depth = 0usize;
            let mut end = self.pos;
            for (i, t) in self.toks.iter().enumerate().skip(self.pos) {
                match t {
                    Tok::LPar => depth += 1,
                    Tok::RPar => {
                        depth -= 1;
                        if depth == 0 {
                            end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            // `(a + 1) > 2` also opens with a group; only a group followed by
            // a connective or the end is a condition.
            let after = self.toks.get(end + 1);
            if after.is_none() || after.is_some_and(|t| t.is_keyword("and") || t.is_keyword("or")) {
                let inner = TruthParser {
                    toks: &self.toks[self.pos + 1..end],
                    pos: 0,
                    schema: self.schema,
                }
                .parse()?;
                self.pos = end + 1;
                return Ok(inner);
            }
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<TruthNode, String> {
        let start = self.pos;
        let mut depth = 0usize;
        while let Some(t) = self.toks.get(self.pos) {
            match t {
                Tok::LPar => depth += 1,
                Tok::RPar => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                _ if depth == 0 && (t.is_keyword("and") || t.is_keyword("or")) => break,
                _ => {}
            }
            self.pos += 1;
        }
        let toks = &self.toks[start..self.pos];
        if toks.is_empty() {
            return Err("a condition is missing".to_string());
        }
        // A computed operand's NULL test, which is how its own `IS [NOT]
        // NULL` and truth tests are spelled: never NULL itself, so it needs
        // no guard and its complement is the other test.
        if let [Tok::Lifted(text), Tok::Word(w)] = toks {
            let (holds, fails) = if w.eq_ignore_ascii_case("notnull") {
                ("NOTNULL", "ISNULL")
            } else if w.eq_ignore_ascii_case("isnull") {
                ("ISNULL", "NOTNULL")
            } else {
                return Err(format!("'{}' is not a condition", render_tokens(toks)));
            };
            return Ok(TruthNode::Atom {
                holds: format!("{} {}", text, holds),
                fails: format!("{} {}", text, fails),
                flags: Vec::new(),
            });
        }

        let mut flags: Vec<String> = Vec::new();
        let mut rendered: Vec<String> = Vec::with_capacity(toks.len());
        let mut null_literal = false;
        let mut op_at = None;
        let mut depth = 0usize;
        for (i, t) in toks.iter().enumerate() {
            match t {
                Tok::LPar => {
                    depth += 1;
                    rendered.push("(".to_string());
                }
                Tok::RPar => {
                    depth = depth.saturating_sub(1);
                    rendered.push(")".to_string());
                }
                Tok::Sym(sym) if negated_operator(sym).is_some() => {
                    if depth != 0 || op_at.is_some() {
                        return Err(format!("'{}' is not a single comparison", render_tokens(toks)));
                    }
                    op_at = Some(i);
                    rendered.push(sym.clone());
                }
                Tok::Sym(sym) if is_arithmetic(sym) => rendered.push(sym.clone()),
                Tok::Str(_) => rendered.push(t.render()),
                Tok::Word(w) => {
                    if let Some(col) = resolve_column(w, self.schema) {
                        if let Some(flag) = nullable::flag_field(&col, self.schema) {
                            if !flags.contains(&flag.name) {
                                flags.push(flag.name.clone());
                            }
                        }
                        rendered.push(col);
                    } else if w.eq_ignore_ascii_case("null") {
                        null_literal = true;
                        rendered.push("NULL".to_string());
                    } else if w.bytes().next().is_some_and(|b| b.is_ascii_digit())
                        || w.eq_ignore_ascii_case("true")
                        || w.eq_ignore_ascii_case("false")
                    {
                        rendered.push(render_value(t));
                    } else {
                        return Err(format!("'{}' cannot be evaluated here", w));
                    }
                }
                // An operand the evaluator computes is NULL exactly when it
                // yields none, which it can be asked directly: it stands in
                // the flags for its own NULL test.
                Tok::Lifted(text) => {
                    if !flags.contains(text) {
                        flags.push(text.clone());
                    }
                    rendered.push(text.clone());
                }
                other => return Err(format!("'{}' cannot be evaluated here", other.render())),
            }
        }
        match op_at {
            Some(op_at) => {
                if op_at == 0 || op_at + 1 == toks.len() {
                    return Err(format!("'{}' is missing an operand", render_tokens(toks)));
                }
                let Tok::Sym(op) = &toks[op_at] else {
                    unreachable!("found above")
                };
                // Any comparison with a NULL — and any arithmetic on one — is
                // NULL, whatever the other side holds.
                if null_literal {
                    return Ok(TruthNode::Unknown);
                }
                let holds = rendered.join(" ");
                let mut negated = rendered.clone();
                negated[op_at] = negated_operator(op).expect("checked").to_string();
                Ok(TruthNode::Atom {
                    holds,
                    fails: negated.join(" "),
                    flags,
                })
            }
            None => {
                // A boolean column standing alone.
                let [Tok::Word(w)] = toks else {
                    return Err(format!("'{}' is not a condition", render_tokens(toks)));
                };
                let col = resolve_column(w, self.schema)
                    .filter(|c| {
                        self.schema
                            .iter()
                            .any(|f| f.name == *c && f.db_type == crate::core::db_type::DbType::BOOL)
                    })
                    .ok_or_else(|| format!("'{}' is not a boolean column", w))?;
                Ok(TruthNode::Atom {
                    holds: format!("{} = 1", col),
                    fails: format!("{} = 0", col),
                    flags,
                })
            }
        }
    }
}

fn render_tokens(toks: &[Tok]) -> String {
    toks.iter().map(|t| t.render()).collect::<Vec<_>>().join(" ")
}

/// Rewrites every prefix `NOT <condition>` in the clause into a condition
/// with no `NOT` over a group, leaving everything else as it was.
///
/// SQL's `NOT` is three-valued: NOT of NULL is NULL, and a WHERE clause keeps
/// only the rows where the whole condition is true. So `WHERE NOT X` keeps
/// exactly the rows where `X` is *false*, never those where it is NULL —
/// `NOT (doc->>'a' = 'x')` drops a row with no `a` at all, in PostgreSQL. The
/// evaluator is two-valued and cannot negate a group, so the negation is
/// pushed down to the comparisons, where "false" can be said directly:
///
/// ```text
///   F(A AND B) = F(A) OR F(B)      F(A OR B) = F(A) AND F(B)      F(NOT A) = T(A)
/// ```
///
/// and for one comparison, F is the comparison that holds exactly where it is
/// false and not NULL (see `negate_atom`). This is sound only because every
/// comparison already answers false on a NULL operand: a nullable column
/// through its guard, a JSON path in the evaluator.
fn expand_not<'c>(
    clause: &'c str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<std::borrow::Cow<'c, str>, String> {
    if !clause.to_ascii_uppercase().contains("NOT") {
        return Ok(std::borrow::Cow::Borrowed(clause));
    }
    let toks = rewrap_casts(lex(clause)?);
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    let mut changed = false;
    let mut i = 0usize;
    while i < toks.len() {
        if toks[i].is_keyword("not") && starts_term(&toks, i) {
            let mut parser = NotParser {
                toks: &toks,
                pos: i,
                end: toks.len(),
                schema,
                depth: 0,
            };
            let node = parser.not()?;
            out.push(node.render(true, schema)?);
            i = parser.pos;
            changed = true;
            continue;
        }
        out.push(toks[i].render());
        i += 1;
    }
    Ok(if changed {
        std::borrow::Cow::Owned(out.join(" "))
    } else {
        std::borrow::Cow::Borrowed(clause)
    })
}

/// Puts back the parentheses `lex` takes off a cast path.
///
/// `lex` reads `(doc->>'k')::int` as `doc ->> 'k' ::int`, which is what the
/// planner needs. But this module renders tokens back to text, and that text
/// is lexed again, where `'k' ::int` reads as a cast on the literal `'k'`. A
/// cast on a literal is dropped by design, so the path was compared as text
/// against a number and matched nothing, silently. That happened in every
/// clause holding a `NOT` (the only kind rendered back) together with a cast
/// path: `(doc->>'tier')::int = 2 AND NOT (qty = 5)` returned no rows.
/// Wrapped again, the rendered text lexes back to the same cast.
fn rewrap_casts(toks: Vec<Tok>) -> Vec<Tok> {
    let is_path_op = |t: &Tok| matches!(t, Tok::Sym(s) if matches!(s.as_str(), "->" | "->>" | "#>" | "#>>"));
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len() + 4);
    for t in toks {
        if matches!(&t, Tok::Sym(sym) if sym.starts_with("::")) {
            // Back over `-> 'key'` pairs to the column the path starts at.
            let mut start = out.len();
            while start >= 2 && is_path_op(&out[start - 2]) && matches!(out[start - 1], Tok::Str(_) | Tok::Word(_)) {
                start -= 2;
            }
            if start < out.len() && start >= 1 && matches!(out[start - 1], Tok::Word(_)) {
                out.insert(start - 1, Tok::LPar);
                out.push(Tok::RPar);
            }
        }
        out.push(t);
    }
    out
}

/// Whether the token at `at` begins a condition: the clause's start, or just
/// after `(`, a connective or a prefix `NOT`. A `NOT` anywhere else is part
/// of `NOT IN`, `NOT LIKE`, `IS NOT` and the like.
fn starts_term(toks: &[Tok], at: usize) -> bool {
    // Back over a run of prefix `NOT`s first, in a loop: asked once per `NOT`
    // and recursing once per `NOT` before it, a long run was quadratic and
    // deep enough to overflow the stack.
    let mut at = at;
    while at > 0 && toks[at - 1].is_keyword("not") {
        at -= 1;
    }
    at == 0 || matches!(toks[at - 1], Tok::LPar) || toks[at - 1].is_keyword("and") || toks[at - 1].is_keyword("or")
}

/// How deeply parentheses may nest under a `NOT`. Each level is a frame of
/// the parser and of the render, so without a bound one statement of
/// opening parentheses could overflow the stack and take the server down.
const MAX_NOT_NESTING: usize = 256;

/// A condition under a `NOT`, as far as negating it needs: its connectives,
/// and its comparisons as they were written.
///
/// `AND` and `OR` hold their terms as lists rather than as nested pairs: a
/// chain of terms is then one level deep, not one level per term.
enum NotNode {
    Atom(Vec<Tok>),
    Not(Box<NotNode>),
    And(Vec<NotNode>),
    Or(Vec<NotNode>),
}

impl NotNode {
    /// The condition when `holds`, else the condition that holds exactly where
    /// it is false.
    fn render(&self, holds: bool, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
        Ok(match self {
            NotNode::Atom(toks) if holds => render_tokens(toks),
            NotNode::Atom(toks) => negate_atom(toks, schema)?,
            NotNode::Not(inner) => inner.render(!holds, schema)?,
            NotNode::And(terms) | NotNode::Or(terms) => {
                let joiner = match (self, holds) {
                    (NotNode::And(..), true) | (NotNode::Or(..), false) => " AND ",
                    _ => " OR ",
                };
                let rendered = terms.iter().map(|t| t.render(holds, schema)).collect::<Result<Vec<_>, _>>()?;
                format!("( {} )", rendered.join(joiner))
            }
        })
    }
}

/// Reads `NOT`'s operand: `NOT` binds tighter than `AND` and `OR` and looser
/// than every comparison, so `NOT a = 1 AND b = 2` is `(NOT a = 1) AND b = 2`
/// and `NOT x IS NULL` is `NOT (x IS NULL)`.
struct NotParser<'a> {
    toks: &'a [Tok],
    pos: usize,
    /// The end of the group being read.
    end: usize,
    schema: &'a SmallVec<[SchemaField; 20]>,
    /// Parentheses entered to reach this group; see [`MAX_NOT_NESTING`].
    depth: usize,
}

impl NotParser<'_> {
    fn or(&mut self) -> Result<NotNode, String> {
        let mut terms = vec![self.and()?];
        while self.pos < self.end && self.toks[self.pos].is_keyword("or") {
            self.pos += 1;
            terms.push(self.and()?);
        }
        Ok(if terms.len() == 1 { terms.pop().expect("one term") } else { NotNode::Or(terms) })
    }

    fn and(&mut self) -> Result<NotNode, String> {
        let mut terms = vec![self.not()?];
        while self.pos < self.end && self.toks[self.pos].is_keyword("and") {
            self.pos += 1;
            terms.push(self.not()?);
        }
        Ok(if terms.len() == 1 { terms.pop().expect("one term") } else { NotNode::And(terms) })
    }

    fn not(&mut self) -> Result<NotNode, String> {
        // A run of `NOT`s is read in a loop, and only its parity is kept:
        // `NOT NOT x` is `x`. One frame per `NOT` overflowed the stack on a
        // long run.
        let mut negations = 0usize;
        while self.pos < self.end && self.toks[self.pos].is_keyword("not") {
            self.pos += 1;
            negations += 1;
        }
        if negations > 0 {
            let operand = self.not()?;
            return Ok(if negations % 2 == 1 { NotNode::Not(Box::new(operand)) } else { operand });
        }
        // A parenthesis opening a condition, rather than an operand: one
        // whose group is followed by nothing a comparison could continue with.
        if self.pos < self.end && matches!(self.toks[self.pos], Tok::LPar) {
            let close = self.closing(self.pos)?;
            let after = self.toks.get(close + 1).filter(|_| close + 1 < self.end);
            let condition = match after {
                None | Some(Tok::RPar) => true,
                Some(t) => t.is_keyword("and") || t.is_keyword("or"),
            };
            if condition {
                if self.depth >= MAX_NOT_NESTING {
                    return Err(format!(
                        "NOT over conditions nested more than {MAX_NOT_NESTING} parentheses deep is not supported"
                    ));
                }
                let mut inner = NotParser {
                    toks: self.toks,
                    pos: self.pos + 1,
                    end: close,
                    schema: self.schema,
                    depth: self.depth + 1,
                };
                let node = inner.or()?;
                if inner.pos != close {
                    return Err(format!(
                        "NOT over this condition is not supported: unexpected '{}'",
                        self.toks[inner.pos].render()
                    ));
                }
                self.pos = close + 1;
                return Ok(node);
            }
        }
        self.atom()
    }

    fn closing(&self, open: usize) -> Result<usize, String> {
        let mut depth = 0usize;
        for (i, t) in self.toks.iter().enumerate().take(self.end).skip(open) {
            match t {
                Tok::LPar => depth += 1,
                Tok::RPar => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(i);
                    }
                }
                _ => {}
            }
        }
        Err("unbalanced parentheses after NOT".to_string())
    }

    /// One comparison: up to the next `AND` / `OR` outside parentheses — the
    /// `AND` of a `BETWEEN` excepted — or the end of the group.
    fn atom(&mut self) -> Result<NotNode, String> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut between = false;
        while self.pos < self.end {
            let t = &self.toks[self.pos];
            match t {
                Tok::LPar => depth += 1,
                Tok::RPar if depth == 0 => break,
                Tok::RPar => depth -= 1,
                _ if depth == 0 && t.is_keyword("between") => between = true,
                _ if depth == 0 && t.is_keyword("and") && between => between = false,
                _ if depth == 0 && (t.is_keyword("and") || t.is_keyword("or")) => break,
                _ => {}
            }
            self.pos += 1;
        }
        if self.pos == start {
            return Err("NOT is missing the condition it negates".to_string());
        }
        Ok(NotNode::Atom(self.toks[start..self.pos].to_vec()))
    }
}

/// The comparison that holds exactly where `toks` is false — not where it is
/// NULL.
///
/// - An ordered comparison takes the complementary operator: every order the
///   evaluator uses is total, and a NULL operand fails both.
/// - `= ANY` and `<> ALL` trade places, as do `IN` and `NOT IN`, `BETWEEN`
///   and `NOT BETWEEN`, `IS` and `IS NOT`, `ISNULL` and `NOTNULL`, and `NOT
///   LIKE` and `LIKE`. Each form already excludes the NULL rows its meaning
///   excludes, and `IS` never yields NULL.
/// - What has no complement — `LIKE`, `SIMILAR TO`, `?`, `@>` and the other
///   structural operators — is negated by the evaluator: a `NOT` handed to it
///   before that one comparison, with the NULL guards of the columns it reads
///   placed outside the `NOT`, where they keep a NULL row out.
fn negate_atom(toks: &[Tok], schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    let refuse = || format!("NOT over '{}' is not supported", render_tokens(toks));
    if let [Tok::Word(w)] = toks {
        if w.eq_ignore_ascii_case("true") {
            return Ok(CONSTANT_FALSE.to_string());
        }
        if w.eq_ignore_ascii_case("false") {
            return Ok(CONSTANT_TRUE.to_string());
        }
        // A boolean column standing alone.
        let col = resolve_column(w, schema)
            .filter(|c| {
                schema
                    .iter()
                    .any(|f| f.name == *c && f.db_type == crate::core::db_type::DbType::BOOL)
            })
            .ok_or_else(refuse)?;
        return Ok(format!("{} = 0", col));
    }

    // The tokens outside parentheses, by position.
    let mut top: Vec<usize> = Vec::new();
    let mut depth = 0usize;
    for (i, t) in toks.iter().enumerate() {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => depth = depth.saturating_sub(1),
            _ if depth == 0 => top.push(i),
            _ => {}
        }
    }
    let keyword_at = |kw: &str| top.iter().copied().find(|&i| toks[i].is_keyword(kw));
    let with = |at: usize, replace: usize, insert: &[Tok]| {
        let mut v: Vec<Tok> = toks[..at].to_vec();
        v.extend_from_slice(insert);
        v.extend_from_slice(&toks[at + replace..]);
        render_tokens(&v)
    };
    let not = Tok::Word("NOT".to_string());
    let preceded_by_not = |at: usize| at > 0 && toks[at - 1].is_keyword("not");

    if keyword_at("exists").is_some() {
        return Err(refuse());
    }
    if let Some(at) = keyword_at("is") {
        return Ok(match toks.get(at + 1) {
            Some(t) if t.is_keyword("not") => with(at + 1, 1, &[]),
            _ => with(at + 1, 0, std::slice::from_ref(&not)),
        });
    }
    if let Some(at) = keyword_at("isnull") {
        return Ok(with(at, 1, &[Tok::Word("NOTNULL".to_string())]));
    }
    if let Some(at) = keyword_at("notnull") {
        return Ok(with(at, 1, &[Tok::Word("ISNULL".to_string())]));
    }
    for kw in ["in", "between"] {
        if let Some(at) = keyword_at(kw) {
            return Ok(if preceded_by_not(at) {
                with(at - 1, 1, &[])
            } else {
                with(at, 0, std::slice::from_ref(&not))
            });
        }
    }
    // `NOT LIKE` and friends: dropping the `NOT` is the complement.
    for kw in ["like", "ilike", "similar"] {
        if let Some(at) = keyword_at(kw) {
            if preceded_by_not(at) {
                return Ok(with(at - 1, 1, &[]));
            }
        }
    }

    let comparison: Vec<usize> = top
        .iter()
        .copied()
        .filter(|&i| matches!(&toks[i], Tok::Sym(s) if negated_operator(s).is_some()))
        .collect();
    let structural = top.iter().any(|&i| {
        matches!(&toks[i], Tok::Sym(s) if PG_OPERATORS.contains(&s.as_str()))
    });
    let pattern = top.iter().any(|&i| {
        [
            "like", "ilike", "similar", "similarto", "contains", "startswith", "endswith",
            "icontains", "istartswith", "iendswith", "iequals",
        ]
        .iter()
        .any(|kw| toks[i].is_keyword(kw))
    });

    if let ([at], false, false) = (comparison.as_slice(), structural, pattern) {
        let Tok::Sym(op) = &toks[*at] else {
            unreachable!("filtered above")
        };
        // `x = ANY('{…}')` and `x <> ALL('{…}')`: the complement of the one is
        // the other, never the operator flipped alone.
        if let Some(q) = toks.get(at + 1).filter(|t| t.is_keyword("any") || t.is_keyword("all")) {
            let swapped = match (op.as_str(), q.is_keyword("any")) {
                ("=", true) => ["<>", "ALL"],
                ("!=" | "<>", false) => ["=", "ANY"],
                _ => return Err(refuse()),
            };
            return Ok(with(
                *at,
                2,
                &[Tok::Sym(swapped[0].to_string()), Tok::Word(swapped[1].to_string())],
            ));
        }
        let flipped = negated_operator(op).expect("filtered above");
        return Ok(with(*at, 1, &[Tok::Sym(flipped.to_string())]));
    }
    if !(structural || pattern) {
        return Err(refuse());
    }

    // No complement: the evaluator negates it, inside the guards of every
    // nullable column it reads.
    let mut flags: Vec<String> = Vec::new();
    for t in toks {
        if let Tok::Word(w) = t {
            if let Some(flag) = resolve_column(w, schema).and_then(|c| nullable::flag_field(&c, schema)) {
                if !flags.contains(&flag.name) {
                    flags.push(flag.name.clone());
                }
            }
        }
    }
    let negated = format!("NOT {}", render_tokens(toks));
    Ok(if flags.is_empty() {
        negated
    } else {
        let guards: Vec<String> = flags.iter().map(|f| format!("{} = 0", f)).collect();
        format!("( {} AND {} )", guards.join(" AND "), negated)
    })
}

/// Wraps a comparison so a `NULL` row cannot satisfy it.
///
/// SQL evaluates any comparison against `NULL` to *unknown*, and returns a row
/// only when the clause is *true*. The evaluator here is two-valued, so the
/// requirement is expressed as an explicit "and the column is not null". That
/// reproduces SQL's answer under `AND`, `OR` and negation alike, and stops
/// `country = ''` from matching a `NULL` row whose stored bytes happen to be
/// empty.
fn guard_null(column: &str, fragment: &str, schema: &SmallVec<[SchemaField; 20]>) -> String {
    match nullable::flag_field(column, schema) {
        None => fragment.to_string(),
        Some(flag) => format!("({} = 0 AND {})", flag.name, fragment),
    }
}

/// PostgreSQL's structural operator symbols, **longest first**.
///
/// The order is load-bearing: a prefix match runs down this list and stops at
/// the first hit, so `<<=` has to precede `<<` or the `=` would be left behind
/// and `client_ip <<= inet '…'` would silently become the strict `<<`, which
/// excludes the network itself.
///
/// `<->` is here too even though it yields a distance rather than a boolean:
/// lexing it is what lets the planner report it as an unsupported *operator*
/// rather than as an unexpected character.
///
/// The JSONB key-existence operators `?|`, `?&` and `?` close the list. This
/// module's own lexer reads them in a dedicated arm (it has to canonicalise the
/// key list), and emits them for the engine tokenizer, which lexes them here.
pub const PG_OPERATORS: &[&str] = &[
    "<<=", ">>=", "<->", "@>", "<@", "&&", "<<", ">>", "@@", "?|", "?&", "?",
];

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bare word: column name, keyword, or unquoted literal.
    Word(String),
    /// A quoted string, carried with its original quoting intact.
    Str(String),
    /// An operator or punctuation run.
    Sym(String),
    LPar,
    RPar,
    Comma,
    /// An operand [`lift_evaluated_operands`] encoded for the expression
    /// evaluator, carried whole and rendered back exactly as it came.
    Lifted(String),
    /// A quoted float under a float cast (`'.5'::float8`): `number` is the
    /// float's own text (`0.5`; a `real` widened to `float8`,
    /// `0.10000000149011612`), which [`rewrite_where`] writes for the
    /// tokenizer; `written` is the literal and cast as written, which every
    /// other pass renders back so a later lex reads it again.
    Float { number: String, written: String },
}

impl Tok {
    fn is_keyword(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
    }

    fn render(&self) -> String {
        match self {
            Tok::Word(w) => w.clone(),
            Tok::Str(s) => s.clone(),
            Tok::Sym(s) => s.clone(),
            Tok::LPar => "(".to_string(),
            Tok::RPar => ")".to_string(),
            Tok::Comma => ",".to_string(),
            Tok::Lifted(text) => text.clone(),
            Tok::Float { written, .. } => written.clone(),
        }
    }
}

fn lex(clause: &str) -> Result<Vec<Tok>, String> {
    let bytes = clause.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];

        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // `E'…'`: PostgreSQL's escape string, where a backslash escapes. Read
        // here and handed on as an ordinary standard-conforming literal, so
        // nothing downstream needs to know the form existed — a `LIKE E'a\\_b'`
        // pattern then carries the single backslash its LIKE escape needs.
        if c == b'\''
            && i > 0
            && matches!(bytes[i - 1], b'E' | b'e')
            && matches!(out.last(), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("e"))
        {
            let (text, end) = escape_string(clause, i)?;
            out.pop();
            out.push(Tok::Str(quote_standard(&text)));
            i = end;
            continue;
        }

        match c {
            LIFTED_OPEN => {
                let (_, end) = read_lifted(clause, i).ok_or_else(|| {
                    "a lifted expression in the WHERE clause is malformed".to_string()
                })?;
                out.push(Tok::Lifted(clause[i..end].to_string()));
                i = end;
            }
            b'\'' | b'"' => {
                let quote = c;
                let start = i;
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err(format!("unterminated string literal at offset {}", start));
                    }
                    if bytes[i] == quote {
                        // A doubled quote is an escaped quote, not the end.
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                // SQL splits the two quote characters by meaning: single
                // quotes make a string literal, double quotes make an
                // identifier. Keeping a double-quoted run as a `Word` lets it
                // resolve to a column; if it resolves to nothing it is rendered
                // back unchanged, which the engine's tokenizer still reads as a
                // literal.
                // A double-quoted run may be one segment of a qualified name:
                // ORMs write `"users"."name"`. Keep consuming `.segment` so the
                // whole reference stays a single token and can be resolved as
                // one column.
                if quote == b'"' {
                    while i < bytes.len() && bytes[i] == b'.' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'"' {
                            i += 1;
                            while i < bytes.len() && bytes[i] != b'"' {
                                i += 1;
                            }
                            if i < bytes.len() {
                                i += 1;
                            }
                        } else {
                            while i < bytes.len()
                                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                            {
                                i += 1;
                            }
                        }
                    }
                }
                let text = clause[start..i].to_string();
                out.push(if quote == b'"' {
                    Tok::Word(text)
                } else {
                    Tok::Str(text)
                });
            }
            b'(' => {
                out.push(Tok::LPar);
                i += 1;
            }
            b')' => {
                out.push(Tok::RPar);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            // A path into a document is one symbol, not arithmetic followed by
            // a comparison. Taken before the arms below because both would
            // otherwise claim a piece of it: `-` reads as subtraction, and the
            // `>` arm greedily takes a second `>` and produces a `>>` that is
            // not an operator anywhere in this grammar.
            b'-' if bytes.get(i + 1) == Some(&b'>') => {
                let start = i;
                i += if bytes.get(i + 2) == Some(&b'>') {
                    3
                } else {
                    2
                };
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            b'#' if bytes.get(i + 1) == Some(&b'>') => {
                let start = i;
                i += if bytes.get(i + 2) == Some(&b'>') {
                    3
                } else {
                    2
                };
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            // PostgreSQL's structural operators. Taken before the ordered
            // comparisons below, which would otherwise cut them in half: `<@`
            // would lex as `<` followed by a stray `@`, which is where
            // "unexpected character '@' in WHERE clause" came from.
            //
            // Longest first — `<<=` before `<<`, `>>=` before `>>` — or a
            // network containment test would lose its `=` and silently become
            // the strict operator.
            _ if c != b'?' && PG_OPERATORS.iter().any(|sym| clause[i..].starts_with(*sym)) => {
                let symbol = PG_OPERATORS
                    .iter()
                    .find(|sym| clause[i..].starts_with(**sym))
                    .expect("the guard just found one");
                out.push(Tok::Sym((*symbol).to_string()));
                i += symbol.len();
            }
            b'=' | b'<' | b'>' | b'!' => {
                let start = i;
                i += 1;
                if i < bytes.len() && matches!(bytes[i], b'=' | b'>') {
                    i += 1;
                }
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            b'+' | b'-' | b'*' | b'/' | b'%' | b'^' => {
                out.push(Tok::Sym((c as char).to_string()));
                i += 1;
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'$' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || bytes[i] == b'_'
                        || bytes[i] == b'.'
                        || bytes[i] == b'$')
                {
                    i += 1;
                }
                // `au."Price"`: a quoted column after a bare qualifier is the
                // same one reference, as `"au"."Price"` already is. Split in
                // two, the qualifier was refused as an unknown column `au.`.
                if bytes[i - 1] == b'.' && bytes.get(i) == Some(&b'"') {
                    if let Some(close) = bytes[i + 1..].iter().position(|b| *b == b'"') {
                        i += close + 2;
                    }
                }
                // A number's signed exponent, `1e-10` or `2.5E+3`, is part of
                // the number: split at its sign, `f > 1e-10` asked for a
                // column `e`.
                let word = &bytes[start..i];
                if word.first().is_some_and(u8::is_ascii_digit)
                    && word.len() >= 2
                    && matches!(word[word.len() - 1], b'e' | b'E')
                    && word[..word.len() - 1].iter().all(|b| b.is_ascii_digit() || *b == b'.')
                    && matches!(bytes.get(i), Some(b'+' | b'-'))
                    && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)
                {
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                out.push(Tok::Word(clause[start..i].to_string()));
            }
            // `?`, `?|` and `?&` are JSONB key existence: `data ? 'k'` asks
            // whether the document has that key at the top level — or, for an
            // array, holds that string as a top-level element, which is what
            // the `data->'k' <> ''` rewrite this replaced could not see:
            // `'["a","b"]' ? 'a'` is true in PostgreSQL and matched nothing
            // here. Each is now an operator of its own (`pg_ops::PgOp`),
            // answered on the encoded document (`jsonb::key_exists`).
            //
            // `?|` / `?&` take an array of keys, written `ARRAY['a','b']` or
            // `'{a,b}'`; both are canonicalised to the second, one string
            // literal the engine tokenizer can carry.
            //
            // Parameters are substituted before a predicate reaches this, so a
            // `?` here is never a placeholder.
            b'?' => {
                let symbol = match bytes.get(i + 1) {
                    Some(b'|') => "?|",
                    Some(b'&') => "?&",
                    _ => "?",
                };
                let mut j = i + symbol.len();
                while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                    j += 1;
                }
                let (operand, end) = if symbol == "?" {
                    if bytes.get(j) != Some(&b'\'') {
                        return Err(
                            "the JSONB operator '?' needs a quoted key, as in `data ? 'name'`"
                                .to_string(),
                        );
                    }
                    let end = quoted_end(bytes, j).ok_or_else(|| {
                        "unterminated key after the JSONB operator '?'".to_string()
                    })?;
                    (clause[j..end].to_string(), end)
                } else {
                    key_list_operand(clause, j).ok_or_else(|| {
                        format!(
                            "the JSONB operator '{}' needs an array of keys, as in \
                             `data {} array['a','b']` or `data {} '{{a,b}}'`",
                            symbol, symbol, symbol
                        )
                    })?
                };
                out.push(Tok::Sym(symbol.to_string()));
                out.push(Tok::Str(operand));
                i = end;
                continue;
            }
            // `::` is a cast, not a stray colon. Saying so sends the reader to
            // the construct rather than to a character they did not think they
            // typed.
            //
            // A cast applied to a *literal* is dropped rather than refused. The
            // column it is compared against already says what type the value
            // is, so `a = 'two'::text` asks exactly what `a = 'two'` asks, and
            // clients emit the cast form whenever a parameter's type would
            // otherwise be ambiguous. Refusing it failed ordinary reads of rows
            // that had been written by the very same client.
            //
            // Only over a literal: a cast on a column or an expression can
            // change what the comparison means, so those still say so plainly
            // rather than being silently reinterpreted.
            b':' if bytes.get(i + 1) == Some(&b':') => {
                let over_literal = matches!(out.last(), Some(Tok::Str(_)))
                    || matches!(
                        out.last(),
                        Some(Tok::Word(w))
                            if !w.is_empty()
                                && w.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-')
                    );
                // Except as an operand of arithmetic, where no column says what
                // the literal is and the cast does: `n / 2::numeric` divides
                // NUMERICs, and without the cast integers, so a CHECK compiled
                // from this text accepted `n = 5` under `n / 2::numeric < 2.2`.
                // A scan never reaches here with one, having lifted the operand
                // to the evaluator; this text is the expression parser's.
                let arithmetic_operand = (out.len() >= 2
                    && matches!(&out[out.len() - 2], Tok::Sym(sym) if is_arithmetic(sym)))
                    || literal_cast_in_arithmetic(clause, i, i);
                let over_literal = over_literal && !arithmetic_operand;
                // Over anything else the cast is *kept*, as one symbol, and
                // carried to the stages that can act on it.
                //
                // `(data->>'code')::bigint = 3` reads a JSON value as text and
                // asks for it to be compared as a number. Dropping the cast
                // compares the text `"3"` against the number `3` and matches
                // nothing — measured, not assumed — so the target type has to
                // travel with the operand rather than be discarded here. The
                // tokenizer turns it into a cast token and the parser applies
                // it to the value the path extracts.
                if !over_literal {
                    let mut k = i + 2;
                    while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                        k += 1;
                    }
                    let name_start = k;
                    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
                    {
                        k += 1;
                    }
                    if k == name_start || matches!(bytes.get(k), Some(b'(') | Some(b'[')) {
                        return Err(
                            "a `::` cast in a WHERE clause is not supported yet".to_string()
                        );
                    }
                    let cast = Tok::Sym(format!("::{}", &clause[name_start..k]));

                    // A wrapper that existed only to carry the cast comes off.
                    //
                    // In a `WHERE`, `(` opens a grouped *predicate*, so
                    // `(doc->>'n')::bigint = 3` reached the planner as
                    // "expected comparison operator, got RPar" — the group was
                    // parsed as a predicate and ended before any operator. The
                    // cast itself is kept, so removing the parentheses changes
                    // nothing about the comparison's type.
                    //
                    // Only when the group holds no arithmetic: parentheses
                    // around `a + b` are load-bearing, and dropping them would
                    // let a neighbouring `*` bind tighter and quietly change
                    // the value. Path operators bind to their own key and
                    // nothing else.
                    if matches!(out.last(), Some(Tok::RPar)) {
                        let mut depth = 0usize;
                        let mut open_at = None;
                        for (at, tok) in out.iter().enumerate().rev() {
                            match tok {
                                Tok::RPar => depth += 1,
                                Tok::LPar => {
                                    depth -= 1;
                                    if depth == 0 {
                                        open_at = Some(at);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        if let Some(open_at) = open_at {
                            let plain = out[open_at + 1..out.len() - 1].iter().all(|t| {
                                !matches!(t, Tok::Sym(sym)
                                    if matches!(sym.as_str(), "+" | "-" | "*" | "/" | "%" | "^"))
                            });
                            if plain {
                                let close_at = out.len() - 1;
                                out.remove(close_at);
                                out.remove(open_at);
                            }
                        }
                    }

                    out.push(cast);
                    i = k;
                    continue;
                }
                // Consume `::` and the type name that follows, keeping the
                // literal token already pushed.
                i += 2;
                while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                    i += 1;
                }
                let name_start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if i == name_start {
                    return Err("a `::` cast in a WHERE clause is not supported yet".to_string());
                }
                // A parameterised or array type — `numeric(10,2)`, `text[]` —
                // is not a bare name, and dropping only part of it would leave
                // stray punctuation for the parser to trip over.
                if matches!(bytes.get(i), Some(b'(') | Some(b'[')) {
                    return Err("a `::` cast in a WHERE clause is not supported yet".to_string());
                }
                // A quoted float under a float cast is that number, not text:
                // `b > '0.5'::float8` over a bigint compared the text '0.5'
                // with the column and the plan refused it (XX000), where
                // PostgreSQL compares the two as float8. A float scalar
                // subquery is substituted in this form, so `b > (SELECT f …)`
                // failed the same way. Kept quoted when it does not read as a
                // finite float (`'NaN'::float8`), as before.
                let name = &clause[name_start..i];
                // `double` alone names no type in PostgreSQL; `double
                // precision` does: its second word is consumed with it.
                if name.eq_ignore_ascii_case("double") {
                    let rest = clause[i..].trim_start();
                    let precision = rest.get(..9).is_some_and(|w| w.eq_ignore_ascii_case("precision"))
                        && !rest[9..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_');
                    if !precision {
                        return Err("type \"double\" does not exist".to_string());
                    }
                    i = clause.len() - rest.len() + 9;
                }
                let name = if name.eq_ignore_ascii_case("double") { "float8" } else { name };
                let single = ["float4", "real"].iter().any(|t| name.eq_ignore_ascii_case(t));
                if single || ["float8", "double", "float"].iter().any(|t| name.eq_ignore_ascii_case(t)) {
                    // Quoted or not: `'0.1'::real` and `0.1::float4` are both
                    // that real (a CHECK spelled the second way still read
                    // the double 0.1).
                    let spelled = match out.last() {
                        Some(Tok::Str(quoted)) => {
                            Some((crate::core::value_codec::unquote_sql(quoted).trim().to_string(), quoted.clone()))
                        }
                        Some(Tok::Word(word)) if is_numeric_literal_word(word) => Some((word.clone(), word.clone())),
                        _ => None,
                    };
                    if let Some((text, quoted)) = spelled {
                        // The float the cast makes, written as its own
                        // shortest text, which the WHERE tokenizer reads back
                        // as that float: `.5` it could not read (42601), and
                        // a `real` is its widened value, so `f8 = '0.1'::real`
                        // compares 0.10000000149011612 as PostgreSQL does.
                        let number = if single {
                            crate::core::value_codec::parse_float4(&text)
                                .filter(|v| v.is_finite())
                                .map(crate::core::value_codec::widened_real_text)
                        } else {
                            crate::core::value_codec::parse_float8(&text)
                                .filter(|v| v.is_finite())
                                .map(crate::core::value_codec::pg_float8_text)
                        };
                        // A float that is not finite (`'NaN'::float8`,
                        // `'-Infinity'::real`) is carried as its quoted
                        // literal, which every reader takes as before; an
                        // exact column reads it through `float_value`, and
                        // answers as PostgreSQL orders NaN and infinity.
                        // It was text, and against an integer or NUMERIC
                        // column the plan refused it (XX000).
                        let number = number.or_else(|| {
                            let v = if single {
                                crate::core::value_codec::parse_float4(&text).map(f64::from)
                            } else {
                                crate::core::value_codec::parse_float8(&text)
                            };
                            v.filter(|v| !v.is_finite()).map(|_| format!("'{}'", text.replace('\'', "''")))
                        });
                        if let Some(number) = number {
                            let written = format!("{quoted}::{name}");
                            *out.last_mut().expect("a literal") = Tok::Float { number, written };
                        }
                    }
                } else if ["numeric", "decimal"].iter().any(|t| name.eq_ignore_ascii_case(t)) {
                    // A quoted number cast to numeric is that numeric
                    // constant, `'0.1'::numeric` as `0.1`. With the cast
                    // dropped it stayed the untyped '0.1', which against a
                    // `real` is a real (`f4 = '0.1'` holds for 0.1f), where
                    // PostgreSQL compares a numeric with a real in double
                    // precision and `f4 = '0.1'::numeric` holds for no row.
                    let number = match out.last() {
                        Some(Tok::Str(quoted)) => {
                            let text = crate::core::value_codec::unquote_sql(quoted).trim().to_string();
                            let digits = text.strip_prefix(['-', '+']).unwrap_or(&text);
                            (is_numeric_literal_word(digits) && digits.bytes().any(|b| b.is_ascii_digit()))
                                .then(|| (text.starts_with('-'), digits.to_string()))
                        }
                        _ => None,
                    };
                    if let Some((negative, digits)) = number {
                        *out.last_mut().expect("a literal") = Tok::Word(digits);
                        if negative {
                            let at = out.len() - 1;
                            out.insert(at, Tok::Sym("-".to_string()));
                        }
                    }
                }

                // With the cast gone, a wrapper that existed only to carry it
                // goes too. `(data->>'code')::bigint` has to become
                // `data->>'code'` and not `(data->>'code')`, because a
                // parenthesised group is not a comparison operand here — it
                // reached the planner as "expected comparison operator, got
                // RPar".
                //
                // Only when the group holds no arithmetic. Parentheses around
                // `a + b` are load-bearing: dropping them would let a
                // neighbouring `*` bind tighter and silently change the value.
                // Path operators are not arithmetic — `->` and `->>` bind to
                // their own key and nothing else — so a path extract unwraps
                // safely.
                if matches!(out.last(), Some(Tok::RPar)) {
                    let mut depth = 0usize;
                    let mut open_at = None;
                    for (at, tok) in out.iter().enumerate().rev() {
                        match tok {
                            Tok::RPar => depth += 1,
                            Tok::LPar => {
                                depth -= 1;
                                if depth == 0 {
                                    open_at = Some(at);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(open_at) = open_at {
                        let plain = out[open_at + 1..out.len() - 1].iter().all(|t| {
                            !matches!(t, Tok::Sym(sym)
                                if matches!(sym.as_str(), "+" | "-" | "*" | "/" | "%" | "^"))
                        });
                        if plain {
                            let close_at = out.len() - 1;
                            out.remove(close_at);
                            out.remove(open_at);
                        }
                    }
                }
            }
            _ => {
                return Err(format!(
                    "unexpected character '{}' in WHERE clause",
                    c as char
                ));
            }
        }
    }

    Ok(out)
}

/// Resolves a possibly-qualified, possibly-quoted identifier to a schema column.
///
/// `users.name`, `u.name`, `"name"` and `NAME` all resolve to `name`, matching
/// Postgres' case-insensitive folding of unquoted identifiers.
fn resolve_column(raw: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<String> {
    let unquoted = raw.trim_matches('"');
    // Try the whole name first, so a column that genuinely contains a dot wins
    // over a qualifier interpretation.
    if let Some(f) = schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(unquoted) && !f.is_deleted)
    {
        return Some(f.name.clone());
    }
    // Each segment carries its own quoting in `"users"."name"`, so the last
    // segment has to be unquoted separately rather than relying on the trim
    // above having reached it.
    let bare = raw.rsplit('.').next()?.trim_matches('"');
    schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
        .map(|f| f.name.clone())
}

/// Whether a word that starts with a digit is a number as PostgreSQL reads
/// one: digits with one point and an exponent (`1.5e-3`), `_` separators,
/// or a `0x`/`0o`/`0b` integer.
fn is_numeric_literal_word(word: &str) -> bool {
    // Read in place, case-insensitively: a copy per numeric word of every
    // predicate was a per-statement allocation.
    let bytes = word.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'0' {
        let digit = match bytes[1].to_ascii_lowercase() {
            b'x' => Some(16u32),
            b'o' => Some(8),
            b'b' => Some(2),
            _ => None,
        };
        if let Some(digit) = digit {
            let rest = &word[2..];
            return !rest.is_empty() && rest.chars().all(|c| c.is_digit(digit) || c == '_');
        }
    }
    let (mantissa, exponent) = match bytes.iter().position(|b| b.eq_ignore_ascii_case(&b'e')) {
        Some(at) => (&word[..at], Some(&word[at + 1..])),
        None => (word, None),
    };
    let mantissa_ok = mantissa.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '_')
        && mantissa.matches('.').count() <= 1;
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && e.chars().all(|c| c.is_ascii_digit())
    });
    mantissa_ok && exponent_ok
}

/// True when the token could begin a value: a literal, or a qualified name.
fn is_value_start(tok: &Tok) -> bool {
    matches!(tok, Tok::Str(_) | Tok::Word(_) | Tok::LPar)
}

/// One bound of a `BETWEEN`: its rendered text and the index after it.
///
/// A negative number arrives as two tokens, the sign and the digits, so
/// `n BETWEEN -3 AND 3` used to fail with "missing its lower bound": the
/// first token after `BETWEEN` was a `-`, which is not where a value
/// starts. A sign directly followed by a number is one value here.
fn bound_at(toks: &[Tok], at: usize) -> Option<(String, usize)> {
    match (toks.get(at), toks.get(at + 1)) {
        (Some(Tok::Sym(sign)), Some(Tok::Word(digits)))
            if (sign == "-" || sign == "+")
                && digits.starts_with(|c: char| c.is_ascii_digit() || c == '.') =>
        {
            let text = if sign == "-" { format!("-{digits}") } else { digits.clone() };
            Some((text, at + 2))
        }
        (Some(tok), _) if is_value_start(tok) => Some((render_value(tok), at + 1)),
        _ => None,
    }
}

/// Whether a word that is not a column leaves a comparison NULL whenever one
/// of its operands is: a number, a boolean, or a comparison keyword. Anything
/// else — a function name, `IS`, `ESCAPE` — may turn a NULL into a value.
fn is_plain_operand_word(word: &str) -> bool {
    word.bytes().next().is_some_and(|b| b.is_ascii_digit() || b == b'-' || b == b'.')
        || [
            "true", "false", "like", "ilike", "similarto", "contains", "startswith", "endswith",
            "icontains", "istartswith", "iendswith", "iequals",
        ]
        .iter()
        .any(|kw| word.eq_ignore_ascii_case(kw))
}

/// Renders one value token, folding boolean keywords into the 1/0 the engine
/// stores. `NULL` is passed through for the caller to reject in context.
fn render_value(tok: &Tok) -> String {
    match tok {
        Tok::Word(w) if w.eq_ignore_ascii_case("true") => "1".to_string(),
        Tok::Word(w) if w.eq_ignore_ascii_case("false") => "0".to_string(),
        other => other.render(),
    }
}

/// Translates a `LIKE` pattern into the substring operator that matches it.
///
/// Returns the operator and the literal to compare against, or an error when
/// the pattern needs wildcard machinery the engine does not have.
/// The characters a pattern token stands for, with its SQL quoting removed: a
/// doubled quote inside is one quote. A bare word is taken as written, as it
/// always was.
fn pattern_text(pattern_tok: &Tok) -> String {
    let raw = pattern_tok.render();
    let quote = raw.chars().next().unwrap_or('\'');
    if raw.len() >= 2 && (quote == '\'' || quote == '"') {
        let doubled: String = [quote, quote].iter().collect();
        raw[1..raw.len() - 1].replace(&doubled, &quote.to_string())
    } else {
        raw
    }
}

/// `text` as a standard-conforming SQL literal: quoted, inner quotes doubled.
fn quote_standard(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Reads the `E'…'` string whose opening quote is at `open`, resolving its
/// backslash escapes as PostgreSQL does. Returns the text and the index just
/// past the closing quote.
fn escape_string(clause: &str, open: usize) -> Result<(String, usize), String> {
    let chars: Vec<(usize, char)> = clause[open + 1..].char_indices().collect();
    let mut out = String::new();
    let mut k = 0usize;
    let hex = |s: &[(usize, char)], from: usize, max: usize| -> (u32, usize) {
        let mut v = 0u32;
        let mut n = 0usize;
        while n < max {
            match s.get(from + n).and_then(|(_, c)| c.to_digit(16)) {
                Some(d) => v = v * 16 + d,
                None => break,
            }
            n += 1;
        }
        (v, n)
    };
    while k < chars.len() {
        let c = chars[k].1;
        match c {
            '\'' => {
                if chars.get(k + 1).map(|(_, c)| *c) == Some('\'') {
                    out.push('\'');
                    k += 2;
                    continue;
                }
                return Ok((out, open + 1 + chars[k].0 + 1));
            }
            '\\' => {
                let Some(&(_, e)) = chars.get(k + 1) else { break };
                k += 2;
                match e {
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    '0'..='7' => {
                        let mut v = e.to_digit(8).unwrap_or(0);
                        let mut n = 0;
                        while n < 2 {
                            match chars.get(k).and_then(|(_, c)| c.to_digit(8)) {
                                Some(d) => v = v * 8 + d,
                                None => break,
                            }
                            k += 1;
                            n += 1;
                        }
                        out.push(char::from_u32(v & 0xFF).unwrap_or('\u{fffd}'));
                    }
                    'x' => {
                        let (v, n) = hex(&chars, k, 2);
                        if n == 0 {
                            out.push('x');
                        } else {
                            k += n;
                            out.push(char::from_u32(v).unwrap_or('\u{fffd}'));
                        }
                    }
                    'u' | 'U' => {
                        let want = if e == 'u' { 4 } else { 8 };
                        let (v, n) = hex(&chars, k, want);
                        if n != want {
                            return Err(format!("invalid Unicode escape in E'' string at offset {}", open));
                        }
                        k += n;
                        out.push(
                            char::from_u32(v)
                                .ok_or_else(|| format!("invalid Unicode escape value {:#x}", v))?,
                        );
                    }
                    other => out.push(other),
                }
            }
            other => {
                out.push(other);
                k += 1;
            }
        }
    }
    Err(format!("unterminated string literal at offset {}", open))
}

/// A `LIKE` pattern as tokens, decoded with its escape character.
#[derive(Clone, Copy, PartialEq)]
enum LikePiece {
    Literal(char),
    /// `_`
    One,
    /// `%`
    Any,
}

/// Decodes `text` as a `LIKE` pattern whose escape character is `escape`
/// (`None` for `ESCAPE ''`). PostgreSQL refuses a pattern that ends in its
/// escape character (22025).
fn like_pieces(text: &str, escape: Option<char>) -> Result<Vec<LikePiece>, String> {
    let mut out = Vec::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            match chars.next() {
                Some(next) => out.push(LikePiece::Literal(next)),
                None => {
                    return Err("LIKE pattern must not end with escape character".to_string());
                }
            }
        } else {
            out.push(match c {
                '%' => LikePiece::Any,
                '_' => LikePiece::One,
                c => LikePiece::Literal(c),
            });
        }
    }
    Ok(out)
}

/// The pattern in the form the engine's matcher reads: `\` is the escape,
/// whatever the statement's own escape character was.
fn canonical_like_pattern(pieces: &[LikePiece]) -> String {
    let mut out = String::with_capacity(pieces.len() + 4);
    for piece in pieces {
        match piece {
            LikePiece::Any => out.push('%'),
            LikePiece::One => out.push('_'),
            LikePiece::Literal(c @ ('%' | '_' | '\\')) => {
                out.push('\\');
                out.push(*c);
            }
            LikePiece::Literal(c) => out.push(*c),
        }
    }
    out
}

/// The escape character a `LIKE` names, and where its clause ends.
///
/// `ESCAPE 'c'` picks the escape character and `ESCAPE ''` turns escaping off.
/// The default is a backslash. `at` is the token just past the pattern.
fn like_escape(toks: &[Tok], at: usize) -> Result<(Option<char>, usize), String> {
    if !toks.get(at).is_some_and(|t| t.is_keyword("escape")) {
        return Ok((Some('\\'), at));
    }
    let Some(escape_tok @ Tok::Str(_)) = toks.get(at + 1) else {
        return Err("ESCAPE needs a quoted string".to_string());
    };
    let text = pattern_text(escape_tok);
    let mut chars = text.chars();
    let escape = match (chars.next(), chars.next()) {
        (None, _) => None,
        (Some(c), None) => Some(c),
        _ => return Err("invalid escape string: it must be empty or one character".to_string()),
    };
    Ok((escape, at + 2))
}

fn like_to_operator(
    text: &str,
    escape: Option<char>,
    case_insensitive: bool,
) -> Result<(&'static str, String), String> {
    let pieces = like_pieces(text, escape)?;
    let leading = pieces.first() == Some(&LikePiece::Any);
    let trailing = pieces.last() == Some(&LikePiece::Any) && pieces.len() > 1;
    let core_start = pieces.iter().take_while(|p| **p == LikePiece::Any).count();
    let core_end = pieces.len()
        - pieces[core_start..]
            .iter()
            .rev()
            .take_while(|p| **p == LikePiece::Any)
            .count();
    let core = &pieces[core_start..core_end];

    // A `_`, or a `%` that is not merely leading or trailing, cannot be reduced
    // to a prefix/suffix/substring test — `a%b` is not
    // `STARTSWITH 'a' AND CONTAINS 'b'`, which does not require `b` to follow
    // `a` and loses the ordering entirely once there are three segments. These
    // go to the pattern matcher with their wildcards intact, escapes written
    // with the backslash the matcher reads.
    //
    // Only *unescaped* wildcards count: `'%a\_b%'` — Django's `contains` — is a
    // substring test for `a_b`, not a pattern.
    //
    // Deliberately not sargable: `sarg::op_of` only accepts `Token::Op` and five
    // comparisons, and a string operator arrives as `Token::Ident`, so this
    // yields `MaskExpr::All` and the scan is unfiltered rather than wrongly
    // pruned. `a_like_predicate_is_never_sargable` pins that.
    if pieces.contains(&LikePiece::One) || core.contains(&LikePiece::Any) {
        let op = if case_insensitive { "ILIKE" } else { "LIKE" };
        return Ok((op, quote_standard(&canonical_like_pattern(&pieces))));
    }
    let core: String = core
        .iter()
        .map(|p| match p {
            LikePiece::Literal(c) => *c,
            _ => unreachable!("wildcards were ruled out above"),
        })
        .collect();

    let op = match (leading, trailing, case_insensitive) {
        (true, true, false) => "CONTAINS",
        (false, true, false) => "STARTSWITH",
        (true, false, false) => "ENDSWITH",
        // No wildcard at all: LIKE degenerates to equality.
        (false, false, false) => "=",
        (true, true, true) => "ICONTAINS",
        (false, true, true) => "ISTARTSWITH",
        (true, false, true) => "IENDSWITH",
        (false, false, true) => "IEQUALS",
    };

    Ok((op, quote_standard(&core)))
}

/// Rewrites a `WHERE` clause into the engine's basic comparison grammar.
///
/// Returns the rewritten clause, or an error naming the unsupported feature.
/// Replaces `<expr> = NULL` and `<expr> <> NULL` with a predicate that never
/// holds.
///
/// `IS NULL` and `IS NOT NULL` are left alone: those ask a different question
/// and the engine answers them.
/// Whether the `(` at `at` opens a function call's argument list rather than a
/// group.
///
/// `substring(x, 1)` has a top-level comma and would otherwise look exactly
/// like the left half of a row comparison — and `substring(x, 1) > (y)` would
/// then be rejected for having two values on the left and one on the right,
/// turning a working query into an error. A call is told apart by what precedes
/// the parenthesis: an identifier character or a closing quote means the
/// parenthesis belongs to a name.
fn opens_a_call(s: &str, at: usize) -> bool {
    s[..at]
        .chars()
        .next_back()
        .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '"' || c == ']')
}

/// Index just past the parenthesis matching the one at `open`, ignoring
/// parentheses inside string literals and quoted identifiers.
fn matching_paren_at(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
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

/// The comma-separated items of `inner`, split only at the top level.
fn split_row_items(inner: &str) -> Vec<&str> {
    let bytes = inner.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut start = 0usize;
    for i in 0..bytes.len() {
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
                b',' if depth == 0 => {
                    out.push(inner[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    out.push(inner[start..].trim());
    out
}

/// The boolean form of a row-value comparison, built right to left.
///
/// `(a, b) > (c, d)` is `a > c OR (a = c AND b > d)` — the leading columns
/// decide unless they tie, and only the last one uses the original operator, so
/// `>=` differs from `>` in exactly one place.
fn row_comparison(left: &[&str], right: &[&str], op: &str) -> String {
    if left.len() == 1 {
        return format!("{} {} {}", left[0], op, right[0]);
    }
    // `=` and `<>` do not cascade: they are all-of and any-of.
    let strict = match op {
        ">" | ">=" => ">",
        "<" | "<=" => "<",
        other => other,
    };
    format!(
        "({} {} {} OR ({} = {} AND {}))",
        left[0],
        strict,
        right[0],
        left[0],
        right[0],
        row_comparison(&left[1..], &right[1..], op)
    )
}

/// `(a, b) [NOT] IN ((x, y), (z, w))` starting at `at`, just past the left
/// row: its boolean form and the offset just past the list, or `None` when
/// the text there is not that shape.
///
/// A row is in a list when it equals some member, so the form is an `OR` of
/// row equalities, each the `AND` of its elements. `NOT IN` is the negation
/// of that, which in three-valued logic is the `AND` of row inequalities, each
/// the `OR` of its elements: a member holding NULL leaves the answer NULL
/// rather than false, and the row out of both forms, as in PostgreSQL.
fn row_in_list(
    current: &str,
    at: usize,
    left: &[&str],
) -> Result<Option<(String, usize)>, String> {
    let word_at = |pos: usize, word: &str| {
        current
            .get(pos..pos + word.len())
            .is_some_and(|w| w.eq_ignore_ascii_case(word))
            && !current[pos + word.len()..]
                .starts_with(|c: char| c.is_alphanumeric() || c == '_')
    };
    let skip_space = |pos: usize| pos + (current[pos..].len() - current[pos..].trim_start().len());
    let (negated, in_at) = if word_at(at, "NOT") {
        (true, skip_space(at + 3))
    } else {
        (false, at)
    };
    if !word_at(in_at, "IN") {
        return Ok(None);
    }
    let open = skip_space(in_at + 2);
    if current.as_bytes().get(open) != Some(&b'(') {
        return Ok(None);
    }
    let Some(close) = matching_paren_at(current, open) else {
        return Ok(None);
    };
    let mut members: Vec<String> = Vec::new();
    for member in split_row_items(&current[open + 1..close]) {
        // A list of plain values, or a subquery, is some other form.
        if !member.starts_with('(') || matching_paren_at(member, 0) != Some(member.len() - 1) {
            return Ok(None);
        }
        let right = split_row_items(&member[1..member.len() - 1]);
        if right.len() != left.len() {
            return Err(format!(
                "a row IN list needs rows of {} values, but one has {}",
                left.len(),
                right.len()
            ));
        }
        let (each, join) = if negated { ("<>", " OR ") } else { ("=", " AND ") };
        let parts: Vec<String> = left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| format!("{l} {each} {r}"))
            .collect();
        members.push(format!("({})", parts.join(join)));
    }
    if members.is_empty() {
        return Ok(None);
    }
    let joined = members.join(if negated { " AND " } else { " OR " });
    Ok(Some((format!("({joined})"), close + 1)))
}

/// Rewrites SQL's row-value comparisons into the boolean form the planner can
/// evaluate: `(a, b) > (c, d)` becomes `(a > c OR (a = c AND b > d))`.
///
/// This is the canonical keyset-pagination idiom — every ORM and every ported
/// query reaches for it, and PostgreSQL supports it — but the parser met a `(`
/// where it wanted a value and reported `expected closing parenthesis`. The
/// message sent two people hunting for a quoting mistake in their own SQL.
///
/// It was worse than an unsupported feature because of *where* it failed: a
/// keyset page has no cursor on the first page, so page one carries no tuple
/// and succeeds. Every page after it fails. A migration would start, die on
/// page two, and look like "the save silently does nothing" — and a table small
/// enough to fit one page worked perfectly, which is the worst possible way for
/// a feature gap to present.
///
/// Only a parenthesised group holding a top-level comma is touched, and only
/// when a comparison operator and a group of the same arity follow it, so an
/// ordinary `(a = 1) AND (b = 2)` and a tuple `IN` list are both left alone.
fn expand_row_value_comparisons(clause: &str) -> Result<String, String> {
    const OPS: &[&str] = &[">=", "<=", "<>", "!=", "=", ">", "<"];
    let mut current = clause.to_string();

    // Bounded: each pass replaces one comparison with a form that has no
    // top-level comma, so the count strictly decreases.
    for _ in 0..64 {
        let bytes = current.as_bytes();
        let mut quote: Option<u8> = None;
        let mut replacement: Option<(usize, usize, String)> = None;

        let mut i = 0usize;
        while i < bytes.len() {
            let c = bytes[i];
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    }
                }
                None => {
                    if c == b'\'' || c == b'"' {
                        quote = Some(c);
                    } else if c == b'(' && !opens_a_call(&current, i) {
                        if let Some(close) = matching_paren_at(&current, i) {
                            let left_items = split_row_items(&current[i + 1..close]);
                            if left_items.len() >= 2 {
                                let after = &current[close + 1..];
                                let lead = after.len() - after.trim_start().len();
                                let rest = after.trim_start();
                                if let Some((text, end)) =
                                    row_in_list(&current, close + 1 + lead, &left_items)?
                                {
                                    replacement = Some((i, end, text));
                                } else if let Some(op) =
                                    OPS.iter().find(|op| rest.starts_with(**op))
                                {
                                    let tail = rest[op.len()..].trim_start();
                                    let consumed = close
                                        + 1
                                        + lead
                                        + op.len()
                                        + (rest[op.len()..].len() - tail.len());
                                    if tail.starts_with('(') {
                                        if let Some(rclose) = matching_paren_at(&current, consumed)
                                        {
                                            let right_items =
                                                split_row_items(&current[consumed + 1..rclose]);
                                            if right_items.len() == left_items.len() {
                                                let expanded = match *op {
                                                    "=" => left_items
                                                        .iter()
                                                        .zip(right_items.iter())
                                                        .map(|(l, r)| format!("{l} = {r}"))
                                                        .collect::<Vec<_>>()
                                                        .join(" AND "),
                                                    "<>" | "!=" => left_items
                                                        .iter()
                                                        .zip(right_items.iter())
                                                        .map(|(l, r)| format!("{l} <> {r}"))
                                                        .collect::<Vec<_>>()
                                                        .join(" OR "),
                                                    other => row_comparison(
                                                        &left_items,
                                                        &right_items,
                                                        other,
                                                    ),
                                                };
                                                replacement =
                                                    Some((i, rclose + 1, format!("({expanded})")));
                                            } else {
                                                return Err(format!(
                                                    "a row comparison needs the same number \
                                                     of values on both sides, but the left \
                                                     has {} and the right has {}",
                                                    left_items.len(),
                                                    right_items.len()
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if replacement.is_some() {
                break;
            }
            i += 1;
        }

        match replacement {
            Some((start, end, text)) => {
                current = format!("{}{}{}", &current[..start], text, &current[end..]);
            }
            None => return Ok(current),
        }
    }
    Err("too many row comparisons in one clause".to_string())
}

fn fold_null_comparisons(clause: &str) -> String {
    let upper = clause.to_ascii_uppercase();
    let bytes = clause.as_bytes();
    let mut out = String::with_capacity(clause.len());
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    // Offset in `out` where the current term began, so the whole comparison can
    // be replaced rather than just its right-hand side.
    let mut term_start = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            // Whole characters, not bytes: pushing a byte of a multi-byte
            // character as a `char` re-encodes it, so `'Müller'` compared
            // against mojibake and matched nothing. Same defect, and same fix,
            // as `lexer_s1::strip_sql_comments`.
            let ch = clause[i..].chars().next().expect("in bounds");
            out.push(ch);
            if b == q {
                quote = None;
            }
            i += ch.len_utf8();
            continue;
        }
        if b == b'\'' || b == b'"' {
            quote = Some(b);
            out.push(b as char);
            i += 1;
            continue;
        }
        // A boolean separator ends the term the replacement would cover.
        for keyword in [" AND ", " OR ", "(", ")"] {
            if upper[i..].starts_with(keyword) {
                out.push_str(&clause[i..i + keyword.len()]);
                i += keyword.len();
                term_start = out.len();
                break;
            }
        }
        if i >= bytes.len() {
            break;
        }

        let is_eq =
            bytes[i] == b'=' || (upper[i..].starts_with("<>") || upper[i..].starts_with("!="));
        if is_eq {
            let width = if bytes[i] == b'=' { 1 } else { 2 };
            let rest = clause[i + width..].trim_start();
            // Only the four bytes after the operator decide this, so only those
            // are looked at. Uppercasing the whole remainder instead — which is
            // what this did — allocated and copied it once per `=` in the
            // clause, making the rewrite quadratic in the number of
            // comparisons: a 51,200-term `OR` chain spent ~98% of its time
            // here, and `__memmove_evex_unaligned_erms` was 46% of the server's
            // profile. The three patterns below are exactly the three the
            // uppercased comparison accepted: `NULL` at the end of the clause,
            // `NULL `, and `NULL)`.
            let rest_bytes = rest.as_bytes();
            let ends_term = rest_bytes.len() >= 4
                && rest_bytes[..4].eq_ignore_ascii_case(b"NULL")
                && matches!(rest_bytes.get(4), None | Some(b' ') | Some(b')'));
            if ends_term {
                // Everything from the start of this term through the NULL.
                out.truncate(term_start);
                out.push_str("1 = 0");
                let consumed =
                    (i + width) + (clause[i + width..].len() - rest.len()) + "NULL".len();
                i = consumed;
                continue;
            }
        }

        let ch = clause[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Opens an operand lifted for the expression evaluator; see
/// [`lift_evaluated_operands`].
pub const LIFTED_OPEN: u8 = 0x02;
/// Separates a lifted operand's encoded text from the columns it reads.
const LIFTED_READS: u8 = 0x1f;
/// Closes a lifted operand.
const LIFTED_CLOSE: u8 = 0x03;

/// An operand for the expression evaluator, wrapped so that nothing between
/// the lift and the tokenizer can read into it.
///
/// Every stage after the lift rewrites the clause as text — the NULL fold, the
/// `NOT` push-down, the guards — and each would rewrite inside a `CASE` it was
/// never meant to see. So the text is spelled in the sixteen letters `g` to
/// `v`, two per byte: no quote, digit or operator, so no stage finds a
/// comparison in it, and a prepared statement's template can never find one
/// of its literals inside it and substitute there. The columns it reads follow
/// in plain text, because the scan's fetch is worked out by finding column
/// names in the clause, and a NULL companion it does not fetch reads as a
/// present zero.
fn encode_lifted(text: &str, reads: &[String]) -> String {
    let mut out = String::with_capacity(text.len() * 2 + reads.len() * 8 + 4);
    out.push(LIFTED_OPEN as char);
    for b in text.bytes() {
        out.push((b'g' + (b >> 4)) as char);
        out.push((b'g' + (b & 0x0f)) as char);
    }
    out.push(LIFTED_READS as char);
    out.push(' ');
    out.push_str(&reads.join(" "));
    out.push(' ');
    out.push(LIFTED_CLOSE as char);
    out
}

/// The expression text of the lifted operand opening at `at`, and the offset
/// just past it. `None` when the bytes there are not one.
pub fn read_lifted(s: &str, at: usize) -> Option<(String, usize)> {
    let bytes = s.as_bytes();
    if bytes.get(at) != Some(&LIFTED_OPEN) {
        return None;
    }
    let reads = at + 1 + bytes[at + 1..].iter().position(|b| *b == LIFTED_READS)?;
    let close = reads + bytes[reads..].iter().position(|b| *b == LIFTED_CLOSE)?;
    let encoded = &bytes[at + 1..reads];
    if encoded.len() % 2 != 0 {
        return None;
    }
    let mut raw = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.chunks(2) {
        let (hi, lo) = (pair[0].checked_sub(b'g')?, pair[1].checked_sub(b'g')?);
        if hi > 15 || lo > 15 {
            return None;
        }
        raw.push((hi << 4) | lo);
    }
    Some((String::from_utf8(raw).ok()?, close + 1))
}

/// [`rewrite_where`] for a clause a scan evaluates — a single table's, or one
/// relation's of a join — with the operands only the expression evaluator can
/// compute lifted out first.
///
/// Kept apart from `rewrite_where` because that one has other callers — the
/// `CHECK` compiler and the join's post-filter — which read its output with the
/// expression parser, and could not read a lifted operand.
pub fn rewrite_scan_where(
    clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<String, String> {
    // Row values first, as `rewrite_where` does them first: `(a, n % 2) =
    // (…)` is two comparisons, and only after it is split can each side be
    // lifted on its own. Expanding again inside `rewrite_where` finds nothing.
    // `s ~ 'x'`, `s ~* 'x'`, `!~`, `!~*`: the `regexp_like` comparison
    // they mean, which the evaluator answers.
    let lowered = crate::core::rql::check::lower_regex_operators(clause, "regexp_like");
    let expanded = expand_row_value_comparisons(&lowered)?;
    let lifted = lift_evaluated_operands(&expanded, schema)?;
    rewrite_where_in(&lifted, schema, true)
}

/// Whether `text` is arithmetic over a column that keeps each value's own
/// scale (`value_codec::keeps_own_scale`): its seventeen-byte cells are not
/// the scan's fixed-width units, so `v + 1 > 2` is the evaluator's, exactly
/// (`expr::own_scale_arith`), rather than an operand-size error. Quoted text is
/// skipped; a leading sign is not arithmetic.
fn own_scale_arithmetic(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    if !schema.iter().any(|f| !f.is_deleted && crate::core::value_codec::keeps_own_scale(f)) {
        return false;
    }
    let bytes = text.as_bytes();
    let mut quote = false;
    let mut arithmetic = false;
    let mut seen_operand = false;
    for &c in bytes {
        if c == b'\'' {
            quote = !quote;
            seen_operand = true;
            continue;
        }
        if quote {
            continue;
        }
        match c {
            b'+' | b'*' | b'/' | b'-' if seen_operand => arithmetic = true,
            b'=' | b'<' | b'>' | b'!' | b'(' | b',' => seen_operand = false,
            c if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b')' => seen_operand = true,
            _ => {}
        }
    }
    arithmetic
        && text
            .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '"'))
            .filter(|w| !w.is_empty())
            .any(|w| {
                resolve_column(w, schema).is_some_and(|name| {
                    schema
                        .iter()
                        .any(|f| !f.is_deleted && f.name == name && crate::core::value_codec::keeps_own_scale(f))
                })
            })
}

/// [`needs_evaluator`], or arithmetic over a column that keeps each value's
/// own scale ([`own_scale_arithmetic`]).
fn evaluated(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    needs_evaluator(text) || own_scale_arithmetic(text, schema)
}

/// Whether `text` holds something only the expression evaluator can compute:
/// a `CASE`, `||`, `%`, a bit-string literal, or a cast over a column (see
/// [`column_cast_at`]). Outside quotes, and `?|` is the JSONB operator, not
/// half a concatenation.
fn needs_evaluator(text: &str) -> bool {
    let bytes = text.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'.';
    let mut quote: Option<u8> = None;
    let mut quote_at = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
                // `"Price"::int` is a column cast like `price::int`, and
                // `'1.5'::numeric * 2` a literal cast that has to be kept.
                let cast = if q == b'"' {
                    evaluated_cast_after(&text[i + 1..])
                } else {
                    literal_cast_in_arithmetic(text, quote_at, i + 1)
                };
                if cast {
                    return true;
                }
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' => {
                quote = Some(c);
                quote_at = i;
            }
            b'%' => return true,
            // `^` (power), which the comparison tokens cannot carry; not
            // `^@`, which is `starts_with`.
            b'^' if bytes.get(i + 1) != Some(&b'@') => return true,
            b'|' if bytes.get(i + 1) == Some(&b'|') && (i == 0 || bytes[i - 1] != b'?') => {
                return true;
            }
            // `(qty)::int`, `(a + b)::numeric`: the group is cast as a column
            // is. A path in parentheses keeps its own cast, which the planner
            // applies to what the path reaches.
            b')' if evaluated_cast_after(&text[i + 1..]) => {
                let group = &text[group_open(bytes, i)..i];
                if !group.contains("->") && !group.contains("#>") {
                    return true;
                }
            }
            _ if is_word(c) && (i == 0 || !is_word(bytes[i - 1])) => {
                let mut end = i;
                while end < bytes.len() && is_word(bytes[end]) {
                    end += 1;
                }
                let word = &text[i..end];
                if word.eq_ignore_ascii_case("CASE")
                    || (word.eq_ignore_ascii_case("B") || word.eq_ignore_ascii_case("X"))
                        && bytes.get(end) == Some(&b'\'')
                    || column_cast_at(text, word, end)
                    // `CAST(qty AS bigint)`, the standard spelling of a cast,
                    // which the rewrite has no form for.
                    || word.eq_ignore_ascii_case("CAST") && text[end..].trim_start().starts_with('(')
                    // `EXTRACT(HOUR FROM ts)`, whose arguments are separated
                    // by FROM rather than a comma, so it is in no function
                    // list; refused as an unknown keyword before.
                    || word.eq_ignore_ascii_case("EXTRACT") && text[end..].trim_start().starts_with('(')
                    // `COALESCE(s, 'z')`, `UPPER(name)`: a call only the
                    // evaluator knows, refused as an unknown column before.
                    || crate::core::rql::expr::is_function(word)
                        && text[end..].trim_start().starts_with('(')
                    || word.bytes().all(|b| b.is_ascii_digit() || b == b'.')
                        && literal_cast_in_arithmetic(text, i, end)
                {
                    return true;
                }
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    false
}

/// Whether the word `word`, ending at `end` in `text`, is a column with a `::`
/// cast after it to a number, text, a date or a timestamp: `qty::int`,
/// `ts::date`, `price::numeric(10,2)`.
///
/// Such an operand goes to the evaluator, which converts it as a select list
/// does: a NUMERIC rounded into an integer from its own scale, a timestamp cut
/// to its day. The comparison's per-row conversion reads neither: it took a
/// NUMERIC's stored integer at the engine scale, and `::date` as the timestamp
/// it was. A literal, a parameter or a keyword before the cast is not a column.
fn column_cast_at(text: &str, word: &str, end: usize) -> bool {
    let first = word.as_bytes()[0];
    if first.is_ascii_digit()
        || first == b'.'
        || first == b'$'
        || ["NULL", "TRUE", "FALSE"].iter().any(|k| word.eq_ignore_ascii_case(k))
    {
        return false;
    }
    evaluated_cast_after(&text[end..])
}

/// Whether the literal spanning `start..end` of `text` carries a `::` cast and
/// is an operand of arithmetic: `n / 2::numeric`, `'1.5'::numeric * 2`,
/// `'2024-01-01'::date + 1`.
///
/// The rewrite drops a cast on a literal, since the column it is compared
/// with says what type the literal is. An arithmetic operand is compared with
/// nothing, and there the cast is the type: without it `n / 2::numeric` was an
/// integer division, so `n / 2::numeric > 1.2` dropped the row where `n` is 3,
/// and a quoted operand was read as text and refused. The evaluator keeps it.
fn literal_cast_in_arithmetic(text: &str, start: usize, end: usize) -> bool {
    let Some(target) = text[end..].trim_start().strip_prefix("::") else {
        return false;
    };
    // Past the type name, its modifier and any array brackets.
    let target = target.trim_start();
    let mut past = target
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(target.len());
    let rest = &target[past..];
    if rest.starts_with('(') {
        past += rest.find(')').map_or(rest.len(), |close| close + 1);
    }
    let after = target[past..].trim_start().trim_start_matches("[]").trim_start();
    let arithmetic = |c: Option<char>| matches!(c, Some('+' | '-' | '*' | '/' | '%' | '^'));
    arithmetic(after.chars().next()) || arithmetic(text[..start].trim_end().chars().last())
}

/// Where the group that the `)` at `close` ends opens, found backwards, with
/// quoted text stepped over; the start of `bytes` when it is not closed.
fn group_open(bytes: &[u8], close: usize) -> usize {
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for at in (0..=close).rev() {
        let c = bytes[at];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == b'\'' || c == b'"' => quote = Some(c),
            None if c == b')' => depth += 1,
            None if c == b'(' => {
                depth -= 1;
                if depth == 0 {
                    return at;
                }
            }
            None => {}
        }
    }
    0
}

/// Whether `after` opens with a `::` cast to a type the evaluator converts to
/// and a `WHERE` compares: an integer, a NUMERIC, a float, text, a boolean, a
/// date or a timestamp. The other targets keep the comparison's own per-row
/// conversion, which renders them correctly.
fn evaluated_cast_after(after: &str) -> bool {
    let Some(target) = after.trim_start().strip_prefix("::") else {
        return false;
    };
    let target = target.trim_start();
    let name_end = target
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(target.len());
    use crate::core::db_type::DbType;
    // A type modifier only the evaluator applies (`expr::CastTypmod`):
    // `tm::time(0)`, `iv::interval(0)`, `t::timetz(2)` round, which the
    // comparison's own per-row conversion does not.
    let modified = target[name_end..].trim_start().starts_with('(');
    if modified
        && matches!(
            crate::core::rql::expr::db_type_of_cast(&target[..name_end]),
            Some(DbType::TIME | DbType::TIMETZ | DbType::INTERVAL)
        )
    {
        return true;
    }
    matches!(
        crate::core::rql::expr::db_type_of_cast(&target[..name_end]),
        Some(
            DbType::I16
                | DbType::I32
                | DbType::I64
                | DbType::DECIMAL
                | DbType::F32
                | DbType::F64
                | DbType::STRING
                | DbType::BOOL
                | DbType::DATETIME
        )
    )
}

/// The words of `text` outside quotes, parentheses and `CASE` blocks, as
/// `(start, end)`.
fn top_level_words(text: &str) -> Vec<(usize, usize)> {
    let nesting = crate::core::rql::expr::Nesting::of(text);
    let bytes = text.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'.';
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if is_word(bytes[i]) && (i == 0 || !is_word(bytes[i - 1])) {
            let mut end = i;
            while end < bytes.len() && is_word(bytes[end]) {
                end += 1;
            }
            if nesting.top_level(i) {
                out.push((i, end));
            }
            i = end;
            continue;
        }
        i += 1;
    }
    out
}

/// Lifts every operand of `clause` that only the expression evaluator can
/// compute — a `CASE`, a `||` chain, `%`, a bit-string literal — into one
/// opaque token, leaving the comparison around it to the ordinary machinery.
///
/// The predicate tokenizer reads columns, literals and `+ - * /`, and a `CASE`
/// in a `WHERE` was refused outright ("unknown column or unsupported keyword
/// 'CASE'"), as were `||` and `%` anywhere in one — while the same expressions
/// worked in the select list. Lifting the whole operand, rather than teaching
/// the tokenizer each construct, gives a `WHERE` the select list's evaluator,
/// so the two cannot disagree about what an expression means.
///
/// Only the operand is lifted, never the comparison. The comparison stays an
/// ordinary one, so a NULL the operand yields leaves it not true whether or
/// not it is negated — the property `expand_not` relies on to push `NOT` onto
/// it — and `IN`, `BETWEEN` and `IS [NOT] NULL` over a lifted operand are
/// spelled out here into the comparisons they mean.
///
/// A clause with nothing to lift comes back unchanged.
pub fn lift_evaluated_operands(
    clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<String, String> {
    if clause.trim().is_empty() {
        return Ok(clause.to_string());
    }
    if !evaluated(clause, schema) && !char_lift_needed(clause, schema) && !reads_legacy_char(clause, schema) {
        return Ok(clause.to_string());
    }
    lift_condition(clause, schema, 0)
}

/// Whether `side` is a string literal that ends in a space, cast to `text`
/// (`'ab  '::text`): a value typed text, which a `character(n)` column
/// compares with exactly, trailing spaces kept. Every other literal compared
/// with one is read blank-padded (`query_tokenizer`'s
/// `canonicalize_bpchar_literals`, which cannot see a cast the rewrite has
/// dropped), so this one is lifted to the evaluator whole, where it keeps its
/// spaces. A parameter declared text is written so
/// (`pg_params::inline_params`). Not in an `IN` list, whose items take the
/// column's type (`lift_list_item`).
fn spaced_text_literal(side: &str) -> bool {
    let t = side.trim();
    let Some(at) = t.rfind("::") else { return false };
    let (literal, cast) = (t[..at].trim_end(), t[at + 2..].trim());
    cast.eq_ignore_ascii_case("text")
        && literal.len() >= 3
        && literal.starts_with('\'')
        && crate::core::sql_scan::quoted_end(literal.as_bytes(), 0, b'\'') == literal.len()
        && literal[..literal.len() - 1].ends_with(' ')
}

/// Whether `text` needs lifting for what `character(n)` and `character
/// varying` read differently from text: a pattern over a `character(n)`
/// value ([`pattern_may_read_bpchar`]), a typed text literal compared with
/// one ([`contains_spaced_text_literal`]), or the two column kinds compared
/// ([`padded_beside_varying`]). Every one of them needs a `character(n)`
/// column in `schema` or a cast (`::`) in `text`; one pass over the fields
/// and one byte search answer most clauses, which have neither, before any
/// of the scans. It runs on every term of every statement.
fn char_lift_needed(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    use crate::core::rql::type_modifier::TextTypmod;
    let cast = memchr::memchr(b':', text.as_bytes()).is_some();
    let (mut padded, mut varying) = (false, false);
    for f in schema.iter() {
        // Most fields have no modifier, and that is one word to test.
        let Some(typmod) = f.typmod_packed else { continue };
        if f.is_deleted || f.db_type != crate::core::db_type::DbType::STRING {
            continue;
        }
        match typmod.unpack() {
            TextTypmod::Bpchar(_) => padded = true,
            TextTypmod::Varchar(_) => varying = true,
            TextTypmod::Precision(_) => {}
        }
    }
    if !padded && !cast {
        // Only a pattern over `CAST(… AS CHAR(n))`, which its own word
        // check answers cheaply.
        return pattern_may_read_bpchar(text, schema) || contains_typed_text_spelling(text);
    }
    contains_typed_text_spelling(text)
        || pattern_may_read_bpchar(text, schema)
        || (cast && contains_spaced_text_literal(text))
        || (padded && varying && padded_beside_varying(text, schema))
}

/// Whether `item`, an `IN` list's item, reads a column anywhere: bare,
/// qualified or quoted, or inside a call, an operator or a cast. PostgreSQL
/// (`transformAExprIn`) compares each such item on its own, as `a = item`,
/// and types only the column-free items together. A word counts when it
/// names a column of `schema` and is neither a function's name nor a type
/// after `::` or `AS`; a column's name inside a string literal is a string.
fn reads_a_column(item: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    let item = item.trim();
    if resolve_column(item, schema).is_some() {
        return true;
    }
    // The evaluator's own reading first: it knows `||`, calls and casts,
    // which the rewrite's lexer refuses.
    if let Ok(expr) = crate::core::rql::expr::parse(item, schema) {
        return !crate::core::rql::expr::columns_of(&expr).is_empty();
    }
    let Ok(toks) = lex(item) else { return false };
    toks.iter().enumerate().any(|(i, tok)| {
        let name = match tok {
            Tok::Word(w) => w.as_str(),
            Tok::Str(s) if s.starts_with('"') => s.as_str(),
            _ => return false,
        };
        let call = matches!(toks.get(i + 1), Some(Tok::LPar));
        let type_name = i > 0 && (matches!(&toks[i - 1], Tok::Sym(s) if s == "::") || toks[i - 1].is_keyword("as"));
        !call && !type_name && resolve_column(name, schema).is_some()
    })
}

/// `item` respelled when it is a typed-literal spelling of a text value:
/// `text 'x'` as `'x'::text`, `varchar 'x'`, `char 'x'` and the rest as the
/// untyped `'x'` (`column_default::typed_text_literal_or`). The rewrite
/// reads neither spelling, and took `text` for a column.
fn typed_text_item(item: &str) -> Option<String> {
    let trimmed = item.trim();
    let literal = crate::core::rql::column_default::typed_text_literal_or(trimmed);
    if literal.len() == trimmed.len() {
        return None;
    }
    let head = trimmed[..trimmed.len() - literal.len()].trim();
    Some(if head.eq_ignore_ascii_case("text") { format!("{literal}::text") } else { literal.to_string() })
}

/// Whether `text` may hold a [`typed_text_item`]. Cheap and generous.
fn contains_typed_text_spelling(text: &str) -> bool {
    let bytes = text.as_bytes();
    memchr::memchr(b'\'', bytes).is_some()
        && [&b"text '"[..], b"char '", b"character varying '"]
            .iter()
            .any(|w| bytes.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w)))
}

/// Whether `text` may hold a [`spaced_text_literal`]. Cheap and generous.
fn contains_spaced_text_literal(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(at) = text[from..].find("::").map(|p| p + from) {
        let cast = text[at + 2..].trim_start();
        if cast.len() >= 4 && cast[..4].eq_ignore_ascii_case("text") && bytes[..at].ends_with(b" '") {
            return true;
        }
        from = at + 2;
    }
    false
}

/// Whether `text` may hold a pattern match (`LIKE`, `ILIKE`, `SIMILAR TO`)
/// over a `character(n)` column of `schema`, which is lifted padded
/// ([`padded_for_pattern`]). Cheap and generous: the lift decides exactly.
fn pattern_may_read_bpchar(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    use crate::core::rql::type_modifier::TextTypmod;
    // The words it looks for first, anywhere and in any case: most clauses
    // have none, and the word scan below runs on every term.
    let mentions = |word: &[u8]| text.as_bytes().windows(word.len()).any(|w| w.eq_ignore_ascii_case(word));
    if !mentions(b"like") && !mentions(b"similar") {
        return false;
    }
    let column = || schema.iter().any(|f| matches!(f.text_typmod(), Some(TextTypmod::Bpchar(n)) if n > 0) && !f.is_deleted);
    let cast = || {
        let lower = text.to_ascii_lowercase();
        ["::char", "::bpchar", "::nchar", " as char", " as bpchar", " as nchar"].iter().any(|c| lower.contains(c))
    };
    (column() || cast())
        && top_level_words(text).iter().any(|&(s, e)| {
            let w = &text[s..e];
            w.eq_ignore_ascii_case("LIKE") || w.eq_ignore_ascii_case("ILIKE") || w.eq_ignore_ascii_case("SIMILAR")
        })
}

/// A bare `character(n)` column as a pattern operator reads it: padded to
/// its length, `rpad(k, n)`.
///
/// Such a column is stored without its trailing spaces
/// (`type_modifier::TextTypmod`), and PostgreSQL matches `LIKE`, `ILIKE`
/// and `SIMILAR TO` against the padded value: `k LIKE 'b'` is false for a
/// `character(3)` 'b' and `k LIKE 'b  '` true. `None` for anything else.
fn padded_for_pattern(operand: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<String> {
    use crate::core::rql::type_modifier::TextTypmod;
    let name = operand.trim();
    // A cast to `character(n)` (`s::char(3) LIKE 'ab '`, `CAST(s AS
    // CHAR(3))`) reads padded too.
    if name.contains("::") || name.get(..5).is_some_and(|h| h.eq_ignore_ascii_case("CAST(")) {
        let n = crate::core::rql::expr::parse(name, schema)
            .ok()
            .and_then(|e| crate::core::rql::expr::bpchar_width(&e))
            .filter(|n| *n > 0)?;
        return Some(format!("rpad({}, {})", name, n));
    }
    if name.is_empty() || name.contains(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | '\'' | ':')) {
        return None;
    }
    let column = resolve_column(name, schema)?;
    let field = schema.iter().find(|f| f.name == column && !f.is_deleted)?;
    match field.text_typmod() {
        Some(TextTypmod::Bpchar(n)) if n > 0 && field.db_type == crate::core::db_type::DbType::STRING => {
            Some(format!("rpad({}, {})", name, n))
        }
        _ => None,
    }
}

/// Whether `text` names a legacy `CHAR` column of `schema` (`DbType::CHAR`,
/// what builds up to f89ed7ad made a bare `CHAR`). Cheap: most schemas have
/// none.
fn reads_legacy_char(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    schema.iter().any(|f| !f.is_deleted && f.db_type == crate::core::db_type::DbType::CHAR)
        && text
            .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '"'))
            .any(|word| legacy_char_operand(word, schema).is_some())
}

/// A bare legacy `CHAR` column as every comparison reads it: its text
/// without trailing spaces (`value_codec::legacy_char_text`), through the
/// evaluator, as `rtrim(c)`. `None` for anything else.
///
/// The cell is the text's UTF-8 in four bytes, and the scan's comparator
/// read those as a little-endian integer: `c BETWEEN 'a' AND 'z'` missed
/// `'b'`, `c < 'b'` and `LIKE 'a%'` missed `'ab'`, `LIKE '%b'` matched
/// nothing past the zero padding, and `c = 'hello'` was refused. The filter,
/// the zone maps, the prefilter and the indexes order and hash the stored
/// bytes, so none of them can answer for the text either; an evaluated
/// operand is one they leave to the scan.
fn legacy_char_operand(operand: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<String> {
    let name = operand.trim();
    if name.is_empty() || name.contains(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | '\'' | ':')) {
        return None;
    }
    let column = resolve_column(name, schema)?;
    let field = schema.iter().find(|f| f.name == column && !f.is_deleted)?;
    (field.db_type == crate::core::db_type::DbType::CHAR).then(|| {
        let lead = &operand[..operand.len() - operand.trim_start().len()];
        let tail = &operand[operand.trim_end().len()..];
        format!("{lead}rtrim({name}){tail}")
    })
}

/// `text` with each plain quoted literal's trailing spaces taken off, as
/// `character(1)` reads a value compared with it (`'a '` is `'a'`). A literal
/// followed by a cast (`'a '::text`) is typed and keeps them.
fn trimmed_literals(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            let literal = &text[i..end];
            let typed = text[end..].trim_start().starts_with("::");
            if !typed && literal.len() >= 2 && literal.ends_with('\'') {
                let inner = literal[1..literal.len() - 1].trim_end_matches(' ');
                out.push('\'');
                out.push_str(inner);
                out.push('\'');
            } else {
                out.push_str(literal);
            }
            i = end;
            continue;
        }
        let ch = text[i..].chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// How a bare text column compares with `character(n)`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TextSide {
    Padded,
    Varying,
}

/// The [`TextSide`] of `operand`, when it is a bare `character(n)` or
/// `character varying` column of `schema`.
fn column_text_kind(operand: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<TextSide> {
    use crate::core::rql::type_modifier::TextTypmod;
    let name = operand.trim();
    if name.is_empty() || name.contains(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | '\'' | ':')) {
        return None;
    }
    let column = resolve_column(name, schema)?;
    let field = schema.iter().find(|f| f.name == column && !f.is_deleted)?;
    if field.db_type != crate::core::db_type::DbType::STRING {
        return None;
    }
    match field.text_typmod() {
        Some(TextTypmod::Bpchar(_)) => Some(TextSide::Padded),
        Some(TextTypmod::Varchar(_)) => Some(TextSide::Varying),
        _ => None,
    }
}

/// Whether `schema` holds both a `character(n)` and a `character varying`
/// column, which a comparison between them reads blank-padded.
fn padded_beside_varying(text: &str, schema: &SmallVec<[SchemaField; 20]>) -> bool {
    if !padded_and_varying_columns(schema) {
        return false;
    }
    // Both kinds named in the text, which only then can compare them.
    let mut padded = false;
    let mut varying = false;
    for word in text.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '"')) {
        match column_text_kind(word, schema) {
            Some(TextSide::Padded) => padded = true,
            Some(TextSide::Varying) => varying = true,
            None => {}
        }
    }
    padded && varying
}

fn padded_and_varying_columns(schema: &SmallVec<[SchemaField; 20]>) -> bool {
    use crate::core::rql::type_modifier::TextTypmod;
    let (mut padded, mut varying) = (false, false);
    for f in schema.iter() {
        if f.is_deleted || f.db_type != crate::core::db_type::DbType::STRING {
            continue;
        }
        match f.text_typmod() {
            Some(TextTypmod::Bpchar(_)) => padded = true,
            Some(TextTypmod::Varchar(_)) => varying = true,
            _ => {}
        }
        if padded && varying {
            return true;
        }
    }
    false
}

/// Whether `rest`, a comparison from its operator on, is a pattern match.
fn is_pattern_operator(rest: &str) -> bool {
    let mut words = rest.split_whitespace();
    let mut first = words.next().unwrap_or("");
    if first.eq_ignore_ascii_case("NOT") {
        first = words.next().unwrap_or("");
    }
    ["LIKE", "ILIKE", "SIMILAR"].iter().any(|k| first.eq_ignore_ascii_case(k))
}

/// `[NOT] LIKE|ILIKE '<pattern>' ESCAPE '<c>'` with the escape folded into
/// the pattern (`canonical_like_pattern`), as a column's match is rewritten;
/// `None` when `rest` names no `ESCAPE`.
fn like_without_escape(rest: &str) -> Result<Option<String>, String> {
    let toks = lex(rest)?;
    let mut at = 0;
    let negated = toks.first().is_some_and(|t| t.is_keyword("not"));
    if negated {
        at += 1;
    }
    let Some(op) = toks.get(at).filter(|t| t.is_keyword("like") || t.is_keyword("ilike")) else {
        return Ok(None);
    };
    let Some(pattern @ Tok::Str(_)) = toks.get(at + 1) else { return Ok(None) };
    if !toks.get(at + 2).is_some_and(|t| t.is_keyword("escape")) {
        return Ok(None);
    }
    let (escape, after) = like_escape(&toks, at + 2)?;
    let canonical = canonical_like_pattern(&like_pieces(&pattern_text(pattern), escape)?);
    let mut out = format!(
        " {}{} {}",
        if negated { "NOT " } else { "" },
        op.render().to_ascii_uppercase(),
        quote_standard(&canonical)
    );
    for tok in &toks[after..] {
        out.push(' ');
        out.push_str(&tok.render());
    }
    Ok(Some(out))
}

/// Lifts within a boolean group: each `AND`/`OR` term on its own. The `AND`
/// of a `BETWEEN` belongs to its term.
fn lift_condition(
    text: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    depth: usize,
) -> Result<String, String> {
    if depth > MAX_NOT_NESTING {
        return Err(format!(
            "conditions nested more than {MAX_NOT_NESTING} parentheses deep are not supported"
        ));
    }
    let mut out = String::with_capacity(text.len() * 2);
    let mut start = 0usize;
    let mut between = false;
    for (s, e) in top_level_words(text) {
        let word = &text[s..e];
        if word.eq_ignore_ascii_case("BETWEEN") {
            between = true;
            continue;
        }
        let and = word.eq_ignore_ascii_case("AND");
        if and && between {
            between = false;
            continue;
        }
        if and || word.eq_ignore_ascii_case("OR") {
            out.push_str(&lift_term(&text[start..s], schema, depth)?);
            out.push_str(word);
            start = e;
        }
    }
    out.push_str(&lift_term(&text[start..], schema, depth)?);
    Ok(out)
}

/// One `AND`/`OR` term: `NOT`s, then a parenthesised group or a comparison.
fn lift_term(
    term: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    depth: usize,
) -> Result<String, String> {
    if !evaluated(term, schema) && !char_lift_needed(term, schema) && !reads_legacy_char(term, schema) {
        return Ok(term.to_string());
    }
    let lead = &term[..term.len() - term.trim_start().len()];
    let tail = &term[term.trim_end().len()..];
    let mut body = term.trim();
    let mut nots = String::new();
    while body.len() > 3
        && body.as_bytes()[..3].eq_ignore_ascii_case(b"NOT")
        && !(body.as_bytes()[3].is_ascii_alphanumeric() || body.as_bytes()[3] == b'_')
    {
        nots.push_str("NOT ");
        body = body[3..].trim_start();
    }
    let lifted = if body.starts_with('(') && matching_paren_at(body, 0) == Some(body.len() - 1) {
        format!("({})", lift_condition(&body[1..body.len() - 1], schema, depth + 1)?)
    } else {
        lift_comparison(body, schema, depth)?
    };
    Ok(format!("{lead}{nots}{lifted}{tail}"))
}

/// The operator a comparison splits at: its offset, its end, and whether it is
/// a keyword (`IS`, `IN`, `BETWEEN`, `LIKE` …) rather than a symbol.
fn comparison_operator(body: &str) -> Option<(usize, usize, bool)> {
    let nesting = crate::core::rql::expr::Nesting::of(body);
    let bytes = body.as_bytes();
    let keyword_at = top_level_words(body);
    let is_operator_word = |w: &str| {
        [
            "IS", "ISNULL", "NOTNULL", "IN", "BETWEEN", "LIKE", "ILIKE", "SIMILAR",
        ]
        .iter()
        .any(|k| w.eq_ignore_ascii_case(k))
    };
    let mut keyword: Option<(usize, usize)> = None;
    for (n, &(s, e)) in keyword_at.iter().enumerate() {
        let word = &body[s..e];
        // `NOT LIKE`, `NOT IN`, `NOT BETWEEN`: the operator begins at `NOT`.
        if word.eq_ignore_ascii_case("NOT")
            && keyword_at
                .get(n + 1)
                .is_some_and(|&(s2, e2)| is_operator_word(&body[s2..e2]))
        {
            keyword = Some((s, e));
            break;
        }
        if is_operator_word(word) {
            keyword = Some((s, e));
            break;
        }
    }
    let mut i = 0usize;
    while i < bytes.len() {
        if keyword.is_some_and(|(s, _)| i >= s) {
            break;
        }
        if !nesting.top_level(i) {
            i += 1;
            continue;
        }
        // Compared as bytes: `i` may sit inside a multi-byte character of an
        // identifier, where slicing the text would panic.
        let here = &bytes[i..];
        // A path into a document is part of its operand.
        if here.starts_with(b"->") || here.starts_with(b"#>") {
            i += if here.get(2) == Some(&b'>') { 3 } else { 2 };
            continue;
        }
        if let Some(sym) = PG_OPERATORS.iter().find(|s| here.starts_with(s.as_bytes())) {
            return Some((i, i + sym.len(), false));
        }
        if matches!(bytes[i], b'<' | b'>' | b'=' | b'!') {
            let width = match (bytes[i], bytes.get(i + 1)) {
                (b'<', Some(b'=' | b'>')) | (b'>' | b'!', Some(b'=')) => 2,
                _ => 1,
            };
            return Some((i, i + width, false));
        }
        i += 1;
    }
    keyword.map(|(s, e)| (s, e, true))
}

/// One comparison, with each side that needs the evaluator lifted.
fn lift_comparison(
    body: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    depth: usize,
) -> Result<String, String> {
    let Some((at, op_end, keyword)) = comparison_operator(body) else {
        // A condition standing alone, as a boolean `CASE` is.
        return Ok(format!("{} = true", lift_operand(body, schema, true)?));
    };
    if !keyword {
        // `character(n)` against a `character varying` column compares
        // blank-padded: the varying side is read without trailing spaces.
        let (left, right) = (&body[..at], &body[op_end..]);
        // A legacy `CHAR` against a boolean or a number, which PostgreSQL has
        // no operator for. Read as text, `c = true` answered no rows.
        for (column, other) in [(left, right), (right, left)] {
            if legacy_char_operand(column, schema).is_some() {
                let other = other.trim();
                let kind = if other.eq_ignore_ascii_case("TRUE") || other.eq_ignore_ascii_case("FALSE") {
                    Some("boolean")
                } else if is_numeric_literal_word(other.trim_start_matches(['-', '+'])) {
                    Some(if other.contains(['.', 'e', 'E']) { "numeric" } else { "integer" })
                } else {
                    None
                };
                if let Some(kind) = kind {
                    let op = body[at..op_end].trim();
                    let op = if op == "!=" { "<>" } else { op };
                    return Err(if std::ptr::eq(column, left) {
                        format!("operator does not exist: character {op} {kind}")
                    } else {
                        format!("operator does not exist: {kind} {op} character")
                    });
                }
            }
        }
        let (left, right): (std::borrow::Cow<str>, std::borrow::Cow<str>) =
            match (column_text_kind(left, schema), column_text_kind(right, schema)) {
                (Some(TextSide::Padded), Some(TextSide::Varying)) => (left.into(), format!(" rtrim({})", right.trim()).into()),
                (Some(TextSide::Varying), Some(TextSide::Padded)) => (format!("rtrim({}) ", left.trim()).into(), right.into()),
                _ => match (legacy_char_operand(left, schema), legacy_char_operand(right, schema)) {
                    (Some(l), Some(r)) => (l.into(), r.into()),
                    (Some(l), None) => (l.into(), trimmed_literals(right).into()),
                    (None, Some(r)) => (trimmed_literals(left).into(), r.into()),
                    (None, None) => (left.into(), right.into()),
                },
            };
        return Ok(format!(
            "{}{}{}",
            lift_side(&left, schema)?,
            &body[at..op_end],
            lift_side(&right, schema)?
        ));
    }

    let rest = &body[at..];
    // A legacy `CHAR` column (`legacy_char_operand`) reads as its text in
    // every form; its list items and bounds without trailing spaces, as
    // `character(1)` compares them. A pattern is matched as written.
    let legacy = legacy_char_operand(&body[..at], schema);
    let legacy_rest;
    let rest: &str = match &legacy {
        Some(_) if !is_pattern_operator(rest) => {
            legacy_rest = trimmed_literals(rest);
            &legacy_rest
        }
        _ => rest,
    };
    // A `character(n)` column under a pattern match reads padded, which only
    // the evaluator gives it (`padded_for_pattern`).
    // A pattern matches `character(1)`'s padded value, as PostgreSQL's
    // does: the blank value is `' '`, so `LIKE '_'` takes it and `LIKE ''`
    // does not.
    let padded = match legacy {
        Some(text) if is_pattern_operator(rest) => {
            let text = text.trim();
            Some(format!("{text} || CASE WHEN {text} = '' THEN ' ' ELSE '' END"))
        }
        Some(text) => Some(text),
        None if is_pattern_operator(rest) => padded_for_pattern(&body[..at], schema),
        None => None,
    };
    // Its `ESCAPE`, which nothing after a lifted operand reads, is folded
    // into the pattern here, in the backslash form the matcher reads: left
    // as written, `a LIKE 'a!%b%' ESCAPE '!'` was refused (42601).
    let escaped = match &padded {
        Some(_) => like_without_escape(rest)?,
        None => None,
    };
    let rest: &str = escaped.as_deref().unwrap_or(rest);
    let left_text: &str = padded.as_deref().unwrap_or(&body[..at]);
    // `(c = 'a') IS TRUE` over a legacy `CHAR` column: the truth test is of a
    // condition, so the lift goes inside the group, as for an evaluated one
    // below. Left whole, the comparison reached the scan's comparator, which
    // reads the four stored bytes as a `u32`: 200 rows of 400.
    let group = left_text.trim();
    if group.starts_with('(')
        && matching_paren_at(group, 0) == Some(group.len() - 1)
        && reads_legacy_char(group, schema)
        && !needs_evaluator(left_text)
    {
        // Redundant parentheses, at any depth: `((c = 'a')) IS TRUE` left the
        // comparison whole as one level did.
        let mut inner = group[1..group.len() - 1].trim();
        while inner.starts_with('(') && matching_paren_at(inner, 0) == Some(inner.len() - 1) {
            inner = inner[1..inner.len() - 1].trim();
        }
        if comparison_operator(inner).is_some()
            || top_level_words(inner)
                .iter()
                .any(|(s, e)| inner[*s..*e].eq_ignore_ascii_case("AND") || inner[*s..*e].eq_ignore_ascii_case("OR"))
        {
            let lifted = lift_condition(inner, schema, depth + 1)?;
            return Ok(format!("({}) {}", lifted, rest.trim_start()));
        }
    }
    if !evaluated(left_text, schema) {
        // The operand is the tokenizer's own. A list item or a bound that
        // needs the evaluator is lifted on its own: `price BETWEEN qty % 7
        // AND 10` was refused as "BETWEEN bounds must be separated by AND",
        // the `%` unreadable to the rewrite. Whatever else follows is left
        // for the ordinary rewrite to read or refuse.
        // Also a list naming a `character varying` column beside a
        // `character(n)` one, which compares blank-padded.
        if evaluated(rest, schema)
            || contains_spaced_text_literal(rest)
            || contains_typed_text_spelling(rest)
            || padded_beside_varying(body, schema)
        {
            if let Some(spelled) = spell_out_list_or_range(left_text.trim(), rest, schema)? {
                return Ok(spelled);
            }
        }
        return Ok(body.to_string());
    }
    // `(n % 2 = 0) IS NOT TRUE` tests a condition, not a value: the lift goes
    // inside the group, and the truth test is expanded over it as over any
    // other condition.
    let group = left_text.trim();
    if group.starts_with('(') && matching_paren_at(group, 0) == Some(group.len() - 1) {
        let inner = &group[1..group.len() - 1];
        let connective = top_level_words(inner).iter().any(|(s, e)| {
            inner[*s..*e].eq_ignore_ascii_case("AND") || inner[*s..*e].eq_ignore_ascii_case("OR")
        });
        if connective || comparison_operator(inner).is_some() {
            let lifted = lift_condition(inner, schema, depth + 1)?;
            return Ok(format!("({}) {}", lifted, rest.trim_start()));
        }
    }
    let (left, left_type) = lift_operand_typed(left_text, schema)?;
    let upper: Vec<String> = rest.split_whitespace().map(|w| w.to_ascii_uppercase()).collect();
    let upper: Vec<&str> = upper.iter().map(|w| w.as_str()).collect();

    // The null tests, in the one-word spellings the tokenizer reads, and the
    // truth tests, which a NULL operand answers through them: `x IS NOT TRUE`
    // holds for a NULL `x`, so it is "NULL, or false" and not merely `<> true`.
    let null_test = |want_null: bool| {
        format!("{} {}", left, if want_null { "ISNULL" } else { "NOTNULL" })
    };
    let truth = |value: &str| -> Result<String, String> {
        if left_type != crate::core::db_type::DbType::BOOL {
            return Err(format!(
                "argument of IS {} must be type boolean, not type {}",
                upper[1..].join(" "),
                crate::core::value_codec::pg_input_type_name(&left_type)
            ));
        }
        Ok(format!("{} = {}", left, value))
    };
    match upper.as_slice() {
        ["ISNULL"] | ["IS", "NULL"] => return Ok(null_test(true)),
        ["NOTNULL"] | ["IS", "NOT", "NULL"] => return Ok(null_test(false)),
        ["IS", "UNKNOWN"] => {
            truth("true")?;
            return Ok(null_test(true));
        }
        ["IS", "NOT", "UNKNOWN"] => {
            truth("true")?;
            return Ok(null_test(false));
        }
        // `IS TRUE` and `IS FALSE` are never NULL, and the comparison alone
        // is NULL for a NULL operand. Without the NULL test, `expand_not`
        // negated `x = true` to `x != true`, which a NULL operand also fails,
        // so `NOT (x IS TRUE)` dropped the very rows it should keep.
        ["IS", "TRUE"] => return Ok(format!("({} AND {})", null_test(false), truth("true")?)),
        ["IS", "FALSE"] => return Ok(format!("({} AND {})", null_test(false), truth("false")?)),
        ["IS", "NOT", "TRUE"] => return Ok(format!("({} OR {})", null_test(true), truth("false")?)),
        ["IS", "NOT", "FALSE"] => return Ok(format!("({} OR {})", null_test(true), truth("true")?)),
        _ => {}
    }

    if let Some(spelled) = spell_out_list_or_range(&left, rest, schema)? {
        return Ok(spelled);
    }
    let negated = upper.first() == Some(&"NOT");
    let after_not = if negated {
        rest.trim_start()[3..].trim_start()
    } else {
        rest.trim_start()
    };
    // `SIMILAR TO` in the one word the tokenizer reads as an operator, as the
    // rewrite spells it over a column. Left as two words after a lifted
    // operand, `SIMILAR` was refused as an unknown column.
    let similar_to = |text: &str| -> String {
        let words: Vec<&str> = text.split_whitespace().collect();
        match words.as_slice() {
            [similar, to, ..]
                if similar.eq_ignore_ascii_case("SIMILAR") && to.eq_ignore_ascii_case("TO") =>
            {
                let pattern = text.trim_start()[similar.len()..].trim_start()[to.len()..].trim_start();
                format!("SIMILARTO {}", pattern)
            }
            _ => text.to_string(),
        }
    };
    // `NOT LIKE` is `NOT` over the match, which the evaluator applies after
    // its NULL test: a NULL operand matches neither form.
    if negated
        && ["LIKE", "ILIKE", "SIMILAR"]
            .iter()
            .any(|k| upper.get(1) == Some(k))
    {
        return Ok(format!("NOT {} {}", left, similar_to(after_not)));
    }
    // `LIKE` and the rest compare the operand's value like any other.
    Ok(format!("{} {}", left, similar_to(rest.trim_start())))
}

/// `left [NOT] IN (a, b)` and `left [NOT] BETWEEN a AND b`, as the comparisons
/// they mean, each list item and bound lifted when it needs the evaluator; or
/// `None` for any other form. `rest` begins at the operator.
///
/// Each comparison with the operand is NULL when it is, so the forms keep
/// PostgreSQL's three-valued answers: a NULL operand is in no list and outside
/// none.
fn spell_out_list_or_range(
    left: &str,
    rest: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Option<String>, String> {
    let negated = rest
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("NOT"));
    let after_not = if negated {
        rest.trim_start()[3..].trim_start()
    } else {
        rest.trim_start()
    };
    let after_upper = after_not.to_ascii_uppercase();
    if after_upper.starts_with("IN") && after_not[2..].trim_start().starts_with('(') {
        let open = after_not.len() - after_not[2..].trim_start().len();
        if matching_paren_at(after_not, open) == Some(after_not.len() - 1) {
            let items = split_row_items(&after_not[open + 1..after_not.len() - 1]);
            if !items.is_empty()
                && !items.iter().any(|item| item.to_ascii_uppercase().starts_with("SELECT"))
            {
                let (each, join) = if negated { ("<>", " AND ") } else { ("=", " OR ") };
                // PostgreSQL types the list as one array only when two or
                // more items are not column references (`transformAExprIn`);
                // then over a `character(n)` column the array is `bpchar`,
                // a computed or typed item too (`a IN ('ab' || '  ', 'x')`),
                // read without its trailing spaces. With fewer the list is
                // `a = item OR …`, each item at its own type, as `=` reads it:
                // a lone `'ab' || '  '` or `'ab  '::text` is text, exact.
                let column = |item: &str| reads_a_column(item, schema);
                let array = items.iter().filter(|item| !column(item)).count() >= 2;
                let padded = array && column_text_kind(left, schema) == Some(TextSide::Padded);
                let mut parts = Vec::with_capacity(items.len());
                for item in items {
                    // `text 'x'` is `'x'::text`; `varchar 'x'` and `char 'x'`
                    // compare with `character(n)` as an untyped `'x'` does.
                    let respelled = typed_text_item(item);
                    let item: &str = respelled.as_deref().unwrap_or(item);
                    // An item reading a column is outside the array: `a = item`,
                    // compared as that comparison is, a `character varying`
                    // beside a `character(n)` blank-padded (`lift_comparison`).
                    if column(item) {
                        let (l, r): (std::borrow::Cow<str>, std::borrow::Cow<str>) =
                            match (column_text_kind(left, schema), column_text_kind(item, schema)) {
                                (Some(TextSide::Padded), Some(TextSide::Varying)) => {
                                    (left.into(), format!("rtrim({})", item.trim()).into())
                                }
                                (Some(TextSide::Varying), Some(TextSide::Padded)) => {
                                    (format!("rtrim({})", left.trim()).into(), item.into())
                                }
                                _ => (left.into(), item.into()),
                            };
                        let l = if l.as_ref() == left { l.into_owned() } else { lift_side(&l, schema)? };
                        parts.push(format!("{} {} {}", l.trim(), each, lift_side(&r, schema)?.trim()));
                        continue;
                    }
                    // Except a subquery's text value, which compares exactly
                    // as `= ANY` over text does: substituted as `('…' || '')`
                    // (`subquery::subquery_literal`).
                    let substituted = item.trim().starts_with("('") && item.trim().ends_with(" || '')");
                    let item: std::borrow::Cow<str> = if padded && !substituted && needs_evaluator(item) {
                        format!("rtrim({})", item.trim()).into()
                    } else {
                        item.into()
                    };
                    // In an array an item takes the column's type, typed or not
                    // (`a IN ('ab  '::text, 'x')` is blank-padded); alone it is
                    // a comparison's operand, and a typed one is lifted as one.
                    let lifted = if array { lift_list_item(&item, schema)? } else { lift_side(&item, schema)? };
                    parts.push(format!("{} {} {}", left, each, lifted));
                }
                return Ok(Some(format!("({})", parts.join(join))));
            }
        }
    }
    if after_upper.starts_with("BETWEEN") {
        let bounds = &after_not["BETWEEN".len()..];
        if let Some(&(s, e)) = top_level_words(bounds)
            .iter()
            .find(|(s, e)| bounds[*s..*e].eq_ignore_ascii_case("AND"))
        {
            let low = lift_side(&bounds[..s], schema)?;
            let high = lift_side(&bounds[e..], schema)?;
            return Ok(Some(if negated {
                format!("({} < {} OR {} > {})", left, low.trim(), left, high.trim())
            } else {
                format!("({} >= {} AND {} <= {})", left, low.trim(), left, high.trim())
            }));
        }
    }
    Ok(None)
}

/// One side of a comparison: lifted when it needs the evaluator, and a bare
/// bit-string literal rewritten as the quoted bits the tokenizer reads.
fn lift_list_item(side: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if !evaluated(side, schema) {
        return Ok(side.to_string());
    }
    lift_side(side, schema)
}

fn lift_side(side: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if !evaluated(side, schema) && !spaced_text_literal(side) {
        return Ok(side.to_string());
    }
    let lead = &side[..side.len() - side.trim_start().len()];
    let tail = &side[side.trim_end().len()..];
    let trimmed = side.trim();
    let bit_literal = trimmed.len() >= 3
        && matches!(trimmed.as_bytes()[0], b'B' | b'b' | b'X' | b'x')
        && trimmed.as_bytes()[1] == b'\''
        && trimmed.ends_with('\'')
        && !trimmed[2..trimmed.len() - 1].contains('\'');
    if bit_literal {
        let bits = crate::core::pg_types::parse_bit_string(trimmed, None)
            .and_then(|bytes| crate::core::pg_types::format_bit_string(&bytes))
            .ok_or_else(|| format!("{} is not a valid bit string", trimmed))?;
        return Ok(format!("{lead}'{bits}'{tail}"));
    }
    Ok(format!("{lead}{}{tail}", lift_operand(side, schema, false)?))
}

/// `text` as a lifted operand, after checking the evaluator can read it — so
/// an unsupported function is refused by name here, not later as a malformed
/// token. `boolean` for a condition standing alone, which has to be one.
fn lift_operand(
    text: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    boolean: bool,
) -> Result<String, String> {
    let (lifted, db_type) = lift_operand_typed(text, schema)?;
    if boolean && db_type != crate::core::db_type::DbType::BOOL {
        return Err(format!(
            "argument of WHERE must be type boolean, not type {}",
            crate::core::value_codec::pg_input_type_name(&db_type)
        ));
    }
    Ok(lifted)
}

/// [`lift_operand`], with the type the operand yields.
fn lift_operand_typed(
    text: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<(String, crate::core::db_type::DbType), String> {
    let trimmed = text.trim();
    let operand = crate::core::processor::transformer::EvaluatedOperand::new(trimmed, schema)?;
    let reads: Vec<String> = operand
        .reads
        .iter()
        .filter_map(|(at, _)| schema.iter().find(|f| f.write_order == *at).map(|f| f.name.clone()))
        .collect();
    Ok((encode_lifted(trimmed, &reads), operand.db_type))
}

/// Every [`Tok::Float`] as the number it is, for the scan's tokenizer — unless
/// the value it is compared with is text (a text, `character` or `varchar`
/// column, or a JSON path read as text, `->>` / `#>>`), which PostgreSQL
/// refuses: `text = double precision` has no operator (42883). Written as a
/// number there, `j->>'x' = '0.5'::float8` compared the text with a float and
/// matched nothing, reporting success.
fn floats_for_the_scan(toks: Vec<Tok>, schema: &SmallVec<[SchemaField; 20]>, scan: bool) -> Result<Vec<Tok>, String> {
    let toks = real_columns_against_doubles(toks, schema);
    let toks = if scan { numeric_columns_against_float_columns(toks, schema) } else { toks };
    if !toks.iter().any(|t| matches!(t, Tok::Float { .. })) {
        return Ok(toks);
    }
    let comparison = |t: Option<&Tok>| {
        matches!(t, Some(Tok::Sym(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="))
    };
    // The PostgreSQL name of a column's type, when `t` names a column of a
    // type with no comparison with a float: text, the date/time types,
    // boolean, bytea, inet, interval.
    let text_column = |t: Option<&Tok>| -> Option<&'static str> {
        let Some(Tok::Word(w)) = t else { return None };
        let name = resolve_column(w, schema)?;
        let field = schema.iter().find(|f| !f.is_deleted && f.name == name)?;
        no_float_comparison_type(field)
    };
    let text_path = |t: &[Tok]| {
        matches!(t, [.., Tok::Sym(op), Tok::Str(_)] if op == "->>" || op == "#>>")
    };
    // A lifted operand (`upper(t)`, a `CASE`), by the type the evaluator
    // gives it: the same names as a column's.
    let lifted_type = |t: Option<&Tok>| -> Option<&'static str> {
        use crate::core::db_type::DbType;
        let Some(Tok::Lifted(text)) = t else { return None };
        let (expression, _) = read_lifted(text, 0)?;
        let operand = crate::core::processor::transformer::EvaluatedOperand::new(&expression, schema).ok()?;
        Some(match operand.db_type {
            DbType::STRING | DbType::CHAR => "text",
            DbType::BPCHAR(_) => "character",
            DbType::TIMESTAMP => "timestamp without time zone",
            DbType::DATETIME => "timestamp with time zone",
            DbType::DATE => "date",
            DbType::TIME => "time without time zone",
            DbType::TIMETZ => "time with time zone",
            DbType::BOOL => "boolean",
            DbType::BYTEA => "bytea",
            DbType::INTERVAL => "interval",
            _ => return None,
        })
    };
    // The text an expression ending just before `end` yields, when it is
    // one this can see is text: a text function's call (`upper(t)`), a
    // `CASE` whose results are text, or a `||` concatenation.
    let text_expression_ending = |end: usize| -> bool {
        match toks.get(end) {
            Some(Tok::RPar) => {
                let mut depth = 0i32;
                let mut k = end;
                loop {
                    match toks.get(k) {
                        Some(Tok::RPar) => depth += 1,
                        Some(Tok::LPar) => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        None => return false,
                        _ => {}
                    }
                    if k == 0 {
                        return false;
                    }
                    k -= 1;
                }
                k > 0 && matches!(toks.get(k - 1), Some(Tok::Word(f)) if TEXT_FUNCTIONS.iter().any(|t| f.eq_ignore_ascii_case(t)))
            }
            Some(t) if t.is_keyword("end") => {
                let mut depth = 0i32;
                let mut k = end;
                let mut results: Vec<&Tok> = Vec::new();
                loop {
                    match toks.get(k) {
                        Some(t) if t.is_keyword("end") => depth += 1,
                        Some(t) if t.is_keyword("case") => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        Some(t) if depth == 1 && (t.is_keyword("then") || t.is_keyword("else")) => {
                            if let Some(r) = toks.get(k + 1) {
                                results.push(r);
                            }
                        }
                        None => return false,
                        _ => {}
                    }
                    if k == 0 {
                        return false;
                    }
                    k -= 1;
                }
                results.iter().any(|r| text_column(Some(r)).is_some_and(|n| n.contains("char") || n == "text"))
                    || (!results.is_empty() && results.iter().all(|r| matches!(r, Tok::Str(_))))
            }
            Some(Tok::Str(_)) | Some(Tok::Word(_)) => {
                end >= 1
                    && matches!(toks.get(end - 1), Some(Tok::Sym(op)) if op == "||")
                    && (matches!(toks.get(end), Some(Tok::Str(_))) || text_column(toks.get(end)).is_some())
            }
            _ => false,
        }
    };
    for (at, tok) in toks.iter().enumerate() {
        if !matches!(tok, Tok::Float { .. }) {
            continue;
        }
        let float = float_type_name(tok);
        // `<text> [NOT] BETWEEN <float> AND …`: PostgreSQL's first
        // comparison is `>=`.
        if at >= 2 && toks[at - 1].is_keyword("between") {
            let subject = if toks[at - 2].is_keyword("not") { at.checked_sub(3) } else { Some(at - 2) };
            if let Some(name) = subject.and_then(|s| text_column(toks.get(s))) {
                return Err(format!("operator does not exist: {name} >= {float}"));
            }
        }
        // `<text> [NOT] IN (…, <float>, …)`: an item of the list.
        {
            let mut k = at;
            while k > 0 && (matches!(toks[k - 1], Tok::Comma | Tok::Float { .. } | Tok::Str(_)) || matches!(&toks[k - 1], Tok::Word(w) if is_numeric_literal_word(w) || w.eq_ignore_ascii_case("null"))) {
                k -= 1;
            }
            if k >= 3 && matches!(toks[k - 1], Tok::LPar) && toks[k - 2].is_keyword("in") {
                let subject = if toks[k - 3].is_keyword("not") { k.checked_sub(4) } else { Some(k - 3) };
                if let Some(name) = subject.and_then(|s| text_column(toks.get(s))) {
                    return Err(format!("operator does not exist: {name} = {float}"));
                }
            }
        }
        // `<text> op <float>`, or `<float> op <text>`.
        let path_op = |t: Option<&Tok>| matches!(t, Some(Tok::Sym(op)) if op.starts_with("->") || op.starts_with("#>"));
        let before = (at >= 2 && comparison(toks.get(at - 1)))
            .then(|| {
                if text_path(&toks[..at - 1]) || text_expression_ending(at - 2) {
                    Some("text")
                } else if let Some(name) = lifted_type(toks.get(at - 2)) {
                    Some(name)
                } else if at < 3 || !path_op(toks.get(at - 3)) {
                    text_column(toks.get(at - 2))
                } else {
                    None
                }
            })
            .flatten();
        let after = comparison(toks.get(at + 1))
            .then(|| {
                if matches!(toks.get(at + 3), Some(Tok::Sym(op)) if op == "->>" || op == "#>>") {
                    Some("text")
                } else if let Some(name) = lifted_type(toks.get(at + 2)) {
                    Some(name)
                } else if !path_op(toks.get(at + 3)) && !matches!(toks.get(at + 3), Some(Tok::Sym(op)) if op == "||") {
                    text_column(toks.get(at + 2))
                } else if matches!(toks.get(at + 3), Some(Tok::Sym(op)) if op == "||") {
                    Some("text")
                } else {
                    None
                }
            })
            .flatten();
        if let Some(name) = before.or(after) {
            let op = match toks.get(if before.is_some() { at - 1 } else { at + 1 }) {
                Some(Tok::Sym(op)) => op.clone(),
                _ => "=".to_string(),
            };
            return Err(if before.is_some() {
                format!("operator does not exist: {name} {op} {float}")
            } else {
                format!("operator does not exist: {float} {op} {name}")
            });
        }
    }
    Ok(exact_columns_against_floats(own_scale_against_floats(toks, schema, scan), schema, scan)
        .into_iter()
        .map(|t| match t {
            Tok::Float { number, .. } => Tok::Word(number),
            other => other,
        })
        .collect())
}

/// An exact column's values as `m × 10^-scale` with `m` in `lo..=hi`, when
/// `name` is an integer or NUMERIC column (a 128-bit integer is left alone).
fn exact_column(name: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<(String, u32, i128, i128)> {
    use crate::core::db_type::DbType;
    let resolved = resolve_column(name, schema)?;
    let field = schema.iter().find(|f| !f.is_deleted && f.name == resolved)?;
    // A column that keeps each value's own scale has no one scale to count a
    // run at (a value may have 38 places, or 38 integer digits):
    // `own_scale_against_floats` compares it as PostgreSQL does instead.
    if crate::core::value_codec::keeps_own_scale(field) {
        return None;
    }
    // A NUMERIC's thirty-eight digits; storage is an i128 of units.
    let digits38 = 10i128.pow(38) - 1;
    let (scale, lo, hi) = match field.db_type {
        DbType::DECIMAL => (field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE), -digits38, digits38),
        DbType::I8 => (0, i8::MIN as i128, i8::MAX as i128),
        DbType::I16 => (0, i16::MIN as i128, i16::MAX as i128),
        DbType::I32 => (0, i32::MIN as i128, i32::MAX as i128),
        DbType::I64 => (0, i64::MIN as i128, i64::MAX as i128),
        DbType::U8 => (0, 0, u8::MAX as i128),
        DbType::U16 => (0, 0, u16::MAX as i128),
        DbType::U32 => (0, 0, u32::MAX as i128),
        DbType::U64 => (0, 0, u64::MAX as i128),
        _ => return None,
    };
    Some((name.to_string(), scale, lo, hi))
}

/// The first `m` in `lo..=hi` where the monotone `holds` is true, searched
/// outward from `guess` and then halved.
fn first_holding(lo: i128, hi: i128, guess: i128, holds: impl Fn(i128) -> bool) -> Option<i128> {
    let g = guess.clamp(lo, hi);
    // `below` fails and `above` holds, below < above.
    let (mut below, mut above);
    let mut step: i128 = 1;
    let mut at = g;
    if holds(g) {
        loop {
            let next = at.saturating_sub(step).max(lo);
            if next == at {
                return Some(at);
            }
            if !holds(next) {
                (below, above) = (next, at);
                break;
            }
            at = next;
            step = step.saturating_mul(2);
        }
    } else {
        loop {
            let next = at.saturating_add(step).min(hi);
            if next == at {
                return None;
            }
            if holds(next) {
                (below, above) = (at, next);
                break;
            }
            at = next;
            step = step.saturating_mul(2);
        }
    }
    while above.checked_sub(below).is_none_or(|d| d > 1) {
        let mid = match above.checked_sub(below) {
            Some(d) => below + d / 2,
            None => below / 2 + above / 2,
        };
        if holds(mid) {
            above = mid;
        } else {
            below = mid;
        }
    }
    Some(above)
}

/// [`float_run`] where the column's step is coarser than the double's
/// spacing at `v`, decided by one conversion; `None` elsewhere.
fn float_run_coarse(scale: u32, lo: i128, hi: i128, v: f64) -> Option<(Option<i128>, Option<i128>)> {
    use std::cmp::Ordering;
    if !v.is_finite() {
        return None;
    }
    let a = v.abs();
    let spacing = f64::from_bits(a.to_bits() + 1) - a;
    let step = 10f64.powi(-(scale as i32));
    if !(spacing.is_finite() && step > 2.0 * spacing) {
        return None;
    }
    // The value nearest `v` at the column's scale, in double arithmetic. Here
    // |v| × 10^scale < 2^51, so the product is off by under a quarter of the
    // column's step; that can pick the farther of two neighbours only when
    // `v` is that close to their midpoint, where neither rounds to `v` (both
    // are more than half the double's spacing away), and either gives the
    // same run. Exact formatting (`{:.N}`) cost most of planning a long list.
    let c = (v * 10f64.powi(scale as i32)).round();
    if !c.is_finite() || c.abs() >= 2f64.powi(100) {
        return None;
    }
    let c = c as i128;
    if !(lo..=hi).contains(&c) {
        return None;
    }
    let nearest = crate::core::value_codec::format_decimal_scaled(c, scale).parse::<f64>().ok()?;
    Some(match crate::core::processor::concurrent_processor::pg_float_cmp(nearest, v) {
        Ordering::Equal => (Some(c), Some(c)),
        Ordering::Greater => (Some(c), (c > lo).then(|| c - 1)),
        Ordering::Less => ((c < hi).then(|| c + 1), Some(c)),
    })
}

/// The run of an exact column's values (`m × 10^-scale`, `m` in `lo..=hi`)
/// whose nearest double is `v`: `l`, the first value whose nearest double
/// is at or above `v` (`None`: no value's is), and `h`, the last at or below
/// it (`None`: none). `l > h` is an empty run.
///
/// Where the column's step is coarser than the double's spacing (an integer
/// below 2^52, a NUMERIC(p,2) 0.1) at most one value rounds to `v`, and it
/// is the one nearest `v`: one conversion decides. Only a column finer than
/// the double there (a NUMERIC(38,20) 0.1, a bigint past 2^52) is searched.
fn float_run(scale: u32, lo: i128, hi: i128, v: f64) -> (Option<i128>, Option<i128>) {
    float_run_coarse(scale, lo, hi, v).unwrap_or_else(|| float_run_search(scale, lo, hi, v))
}

/// [`float_run`] by searching the column's values, at any step.
fn float_run_search(scale: u32, lo: i128, hi: i128, v: f64) -> (Option<i128>, Option<i128>) {
    use std::cmp::Ordering;
    let nearest = |m: i128| -> f64 {
        crate::core::value_codec::format_decimal_scaled(m, scale).parse::<f64>().unwrap_or(f64::NAN)
    };
    let cmp = |m: i128| crate::core::processor::concurrent_processor::pg_float_cmp(nearest(m), v);
    let fixed = || format!("{:.*}", scale as usize, v).replace('.', "").parse::<i128>().ok();
    let guess = if v.is_nan() || v == f64::INFINITY {
        hi
    } else if v == f64::NEG_INFINITY {
        lo
    } else {
        fixed().unwrap_or(if v < 0.0 { lo } else { hi })
    };
    let l = first_holding(lo, hi, guess, |m| cmp(m) != Ordering::Less);
    let h = match first_holding(lo, hi, guess, |m| cmp(m) == Ordering::Greater) {
        Some(m) if m == lo => None,
        Some(m) => Some(m - 1),
        None => Some(hi),
    };
    (l, h)
}

/// `real_column op v`, `v op real_column` and `real_column [NOT] IN (v, …)`
/// where `v` is a double (`'0.1'::float8`) or a decimal constant (`0.1`):
/// PostgreSQL widens the `real` to a double and compares there, so a `real`
/// 0.1 (0.10000000149011612) is not `'0.1'::float8` and is greater than it.
/// Compared as a `real`, the double was narrowed to 0.1f and `f4 = 0.1`
/// matched, `f4 > 0.1` did not. Written as comparisons with the `real`
/// nearest `v`, which the scan reads as the column reads it: the widened
/// column is above `v` exactly when it is at or above the first `real` above
/// `v`, and so on; equality with a `v` no `real` widens to holds for no row
/// (`c <> c AND c = c`, so NULL stays NULL and NaN is not equal), and
/// inequality for every row (`c = c OR c <> c`). `BETWEEN` is its two
/// comparisons. An IN list's constants widen only where PostgreSQL's common
/// type is float8: one typed float8, or a single constant (the scalar `=`); a
/// list of plain numbers compares as `real`. A column in the list is its own
/// scalar `=` (`f4 IN (f8, 0.1)` is `f4 = f8 OR f4 = 0.1`, both in double
/// precision), which the passes after this one compare. A NULL item counts
/// as an item but not toward the type, and stays in the list. A `real`
/// constant and NaN are left as they were; `'0.1'::numeric` reaches here as
/// the decimal it is.
fn real_columns_against_doubles(toks: Vec<Tok>, schema: &SmallVec<[SchemaField; 20]>) -> Vec<Tok> {
    use crate::core::db_type::DbType;
    if !schema.iter().any(|f| !f.is_deleted && f.db_type == DbType::F32) {
        return toks;
    }
    // A negative number arrives as its sign and its digits; read as one
    // constant here (a unary sign: at the start, after `(`, a comma, an
    // operator or AND/OR/NOT/BETWEEN/IN), or `f4 = -0.1` was never widened
    // and matched the real -0.1. Rendered back it is the same text.
    let toks = {
        let mut folded: Vec<Tok> = Vec::with_capacity(toks.len());
        let mut it = toks.into_iter().peekable();
        while let Some(tok) = it.next() {
            let unary = match folded.last() {
                None | Some(Tok::LPar | Tok::Comma | Tok::Sym(_)) => true,
                Some(Tok::Word(w)) => ["and", "or", "not", "between", "in"].iter().any(|k| w.eq_ignore_ascii_case(k)),
                _ => false,
            };
            if let (Tok::Sym(sign), true, Some(Tok::Word(digits))) = (&tok, unary, it.peek()) {
                if sign == "-" && is_numeric_literal_word(digits) && digits.bytes().any(|b| b.is_ascii_digit()) {
                    let digits = format!("-{digits}");
                    it.next();
                    folded.push(Tok::Word(digits));
                    continue;
                }
            }
            folded.push(tok);
        }
        folded
    };
    let comparison = |t: Option<&Tok>| -> Option<String> {
        match t {
            Some(Tok::Sym(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") => Some(op.clone()),
            _ => None,
        }
    };
    let binds = |t: Option<&Tok>| match t {
        Some(Tok::Sym(op)) => !matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="),
        Some(Tok::Word(w)) => w.eq_ignore_ascii_case("between") || w.eq_ignore_ascii_case("collate"),
        _ => false,
    };
    let real_at = |i: usize| -> Option<String> {
        let Some(Tok::Word(w)) = toks.get(i) else { return None };
        let name = resolve_column(w, schema)?;
        schema.iter().any(|f| !f.is_deleted && f.name == name && f.db_type == DbType::F32).then(|| w.clone())
    };
    // A constant a scalar comparison with a real reads in double precision:
    // a float8 constant, a decimal or an integer (`f4 = 16777217` holds for
    // no real; as a real the integer was 16777216); finite, not NaN. The
    // second answer says whether it is typed float8, which decides an IN
    // list's common type.
    let double_at = |i: usize| -> Option<(f64, bool)> {
        let (v, double) = match toks.get(i)? {
            Tok::Float { number, .. } if float_type_name(&toks[i]) == "double precision" => (float_value(number)?, true),
            Tok::Word(w) if is_numeric_literal_word(w.strip_prefix('-').unwrap_or(w)) => (w.parse::<f64>().ok()?, false),
            _ => return None,
        };
        (!v.is_nan()).then_some((v, double))
    };
    let mirror = |op: &str| match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
    .to_string();
    // `column op v`, compared as PostgreSQL's widened real.
    let lowered = |column: &str, op: &str, v: f64| -> Vec<Tok> {
        // A double past a real's range against the largest finite real
        // (every finite real is below it, an infinite one above).
        let r = match v as f32 {
            r if r.is_infinite() && v.is_finite() => f32::MAX.copysign(r),
            r => r,
        };
        let rd = f64::from(r);
        let c = || Tok::Word(column.to_string());
        let cmp = |op: &str| vec![c(), Tok::Sym(op.to_string()), Tok::Word(format!("{r}"))];
        let never = || vec![c(), Tok::Sym("<>".into()), c(), Tok::Word("AND".into()), c(), Tok::Sym("=".into()), c()];
        let always = || vec![c(), Tok::Sym("=".into()), c(), Tok::Word("OR".into()), c(), Tok::Sym("<>".into()), c()];
        let inner = if rd == v {
            cmp(if op == "!=" { "<>" } else { op })
        } else if rd > v {
            match op {
                ">" | ">=" => cmp(">="),
                "<" | "<=" => cmp("<"),
                "=" => never(),
                _ => always(),
            }
        } else {
            match op {
                ">" | ">=" => cmp(">"),
                "<" | "<=" => cmp("<="),
                "=" => never(),
                _ => always(),
            }
        };
        // One comparison stands as it was written; the two-part forms are
        // grouped so a neighbouring AND or OR cannot split them.
        if inner.len() == 3 {
            return inner;
        }
        let mut out = vec![Tok::LPar];
        out.extend(inner);
        out.push(Tok::RPar);
        out
    };
    let before = |i: usize| if i == 0 { None } else { toks.get(i - 1) };
    // A plain numeric column as an IN item, ending at a comma or the close.
    let column_item = |i: usize| -> Option<String> {
        let Some(Tok::Word(w)) = toks.get(i) else { return None };
        if !matches!(toks.get(i + 1), Some(Tok::Comma | Tok::RPar)) {
            return None;
        }
        let name = resolve_column(w, schema)?;
        schema
            .iter()
            .any(|f| {
                !f.is_deleted
                    && f.name == name
                    && matches!(
                        f.db_type,
                        DbType::F32
                            | DbType::F64
                            | DbType::DECIMAL
                            | DbType::I8
                            | DbType::I16
                            | DbType::I32
                            | DbType::I64
                            | DbType::U8
                            | DbType::U16
                            | DbType::U32
                            | DbType::U64
                    )
            })
            .then(|| w.clone())
    };
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    while i < toks.len() {
        // The cheap tests first: a comparison or IN next, before any column
        // is resolved (every statement over a table with a real passes here).
        // `real [NOT] BETWEEN a AND b`: two scalar comparisons, each in
        // double precision (`f4 BETWEEN 0 AND 0.1` holds for no real 0.1).
        let negated_between = toks.get(i + 1).is_some_and(|t| t.is_keyword("not"))
            && toks.get(i + 2).is_some_and(|t| t.is_keyword("between"));
        if (negated_between || toks.get(i + 1).is_some_and(|t| t.is_keyword("between"))) && !binds(before(i)) {
            let at = i + 2 + negated_between as usize;
            let bounds = (double_at(at), toks.get(at + 1).is_some_and(|t| t.is_keyword("and")), double_at(at + 2));
            if let ((Some((low, _)), true, Some((high, _))), false) = (bounds, binds(toks.get(at + 3))) {
                if let Some(column) = real_at(i) {
                    let (a, kw, b) = if negated_between { ("<", "OR", ">") } else { (">=", "AND", "<=") };
                    out.push(Tok::LPar);
                    out.extend(lowered(&column, a, low));
                    out.push(Tok::Word(kw.to_string()));
                    out.extend(lowered(&column, b, high));
                    out.push(Tok::RPar);
                    i = at + 3;
                    continue;
                }
            }
        }
        let compared = comparison(toks.get(i + 1));
        if let Some(op) = compared.filter(|_| !binds(before(i)) && !binds(toks.get(i + 3))) {
            if let Some((v, _)) = double_at(i + 2) {
                if let Some(column) = real_at(i) {
                    out.extend(lowered(&column, &op, v));
                    i += 3;
                    continue;
                }
            }
            if let Some((v, _)) = double_at(i) {
                if let Some(column) = real_at(i + 2) {
                    out.extend(lowered(&column, &mirror(&op), v));
                    i += 3;
                    continue;
                }
            }
        }
        let listed = toks.get(i + 1).is_some_and(|t| t.is_keyword("in"))
            || (toks.get(i + 1).is_some_and(|t| t.is_keyword("not")) && toks.get(i + 2).is_some_and(|t| t.is_keyword("in")));
        if let Some(column) = listed.then(|| real_at(i)).flatten().filter(|_| !binds(before(i))) {
            let negated = toks.get(i + 1).is_some_and(|t| t.is_keyword("not"));
            let open = i + 1 + negated as usize + 1;
            if toks.get(open - 1).is_some_and(|t| t.is_keyword("in")) && matches!(toks.get(open), Some(Tok::LPar)) {
                // Items: constants, and plain numeric columns, which
                // PostgreSQL compares one at a time with the scalar operator
                // (`f4 IN (f8, 0.1)` is `f4 = f8 OR f4 = 0.1`), outside the
                // list's common type.
                let mut values = Vec::new();
                let mut columns: Vec<String> = Vec::new();
                let mut doubles = 0usize;
                let mut nulls = 0usize;
                let mut j = open + 1;
                let mut closed = false;
                loop {
                    if let Some((v, double)) = double_at(j) {
                        values.push(v);
                        doubles += double as usize;
                    } else if let Some(word) = column_item(j) {
                        columns.push(word);
                    } else if toks.get(j).is_some_and(|t| t.is_keyword("null")) {
                        nulls += 1;
                    } else {
                        break;
                    }
                    match toks.get(j + 1) {
                        Some(Tok::Comma) => j += 2,
                        Some(Tok::RPar) => {
                            closed = true;
                            break;
                        }
                        _ => break,
                    }
                }
                // The constants' common type comes from the column and every
                // constant: float8 only when one is typed float8, real when
                // all are plain numbers (`f4 IN (0.1, 0.5)` compares 0.1 as a
                // real). A NULL is a constant with no type: it takes no part
                // in the choice, but it is an item, so `f4 IN (0.1, NULL)` is
                // a list of reals. One constant is the scalar `=`, in double
                // precision. A NULL stays an item of its own (`f4 IN (NULL)`),
                // so IN without a match is unknown and NOT IN never true.
                let widened = doubles > 0 || (values.len() == 1 && nulls == 0);
                if closed && (widened || !columns.is_empty()) {
                    let (op, kw) = if negated { ("<>", "AND") } else { ("=", "OR") };
                    let mut parts: Vec<Vec<Tok>> = Vec::new();
                    // The NULL item: unknown for every row, IN or NOT IN.
                    let in_null = || {
                        vec![Tok::Word(column.clone()), Tok::Word("IN".into()), Tok::LPar, Tok::Word("NULL".into()), Tok::RPar]
                    };
                    if widened {
                        parts.extend(values.iter().map(|v| lowered(&column, op, *v)));
                        if nulls > 0 {
                            parts.push(in_null());
                        }
                    } else if !values.is_empty() || nulls > 0 {
                        // The constants stay a list, compared as real as it
                        // was written; the columns are parted out below.
                        let mut kept = vec![Tok::Word(column.clone())];
                        if negated {
                            kept.push(Tok::Word("NOT".into()));
                        }
                        kept.push(Tok::Word("IN".into()));
                        kept.push(Tok::LPar);
                        for k in (open + 1..=j)
                            .step_by(2)
                            .filter(|&k| double_at(k).is_some() || toks[k].is_keyword("null"))
                        {
                            if kept.len() > 3 + negated as usize {
                                kept.push(Tok::Comma);
                            }
                            kept.push(toks[k].clone());
                        }
                        kept.push(Tok::RPar);
                        parts.push(kept);
                    }
                    for item in &columns {
                        parts.push(vec![Tok::Word(column.clone()), Tok::Sym(op.to_string()), Tok::Word(item.clone())]);
                    }
                    out.push(Tok::LPar);
                    for (k, part) in parts.into_iter().enumerate() {
                        if k > 0 {
                            out.push(Tok::Word(kw.to_string()));
                        }
                        out.extend(part);
                    }
                    out.push(Tok::RPar);
                    i = j + 2;
                    continue;
                }
            }
        }
        out.push(toks[i].clone());
        i += 1;
    }
    out
}

/// `numeric_column op float_column` (either order) for the scan, compared as
/// PostgreSQL compares them: both as double precision (`n::float8 = f8`;
/// a `real` widened too, `n = f4` is `n::float8 = f4::float8`). The scan
/// compared the two columns' bytes as one type and refused the plan
/// (XX000 "operand size does not match"). An integer column against a
/// float one the scan already answers so.
fn numeric_columns_against_float_columns(toks: Vec<Tok>, schema: &SmallVec<[SchemaField; 20]>) -> Vec<Tok> {
    use crate::core::db_type::DbType;
    // Nothing to do (and nothing copied) unless the table has both kinds.
    let live = || schema.iter().filter(|f| !f.is_deleted);
    if !live().any(|f| f.db_type == DbType::DECIMAL) || !live().any(|f| matches!(f.db_type, DbType::F32 | DbType::F64)) {
        return toks;
    }
    let kind = |i: usize| -> Option<(String, DbType)> {
        let Some(Tok::Word(w)) = toks.get(i) else { return None };
        let name = resolve_column(w, schema)?;
        let field = schema.iter().find(|f| !f.is_deleted && f.name == name)?;
        matches!(field.db_type, DbType::DECIMAL | DbType::F32 | DbType::F64).then(|| (w.clone(), field.db_type.clone()))
    };
    let comparison = |t: Option<&Tok>| {
        matches!(t, Some(Tok::Sym(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="))
    };
    let binds = |t: Option<&Tok>| match t {
        Some(Tok::Sym(op)) => !matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="),
        Some(Tok::Word(w)) => w.eq_ignore_ascii_case("between") || w.eq_ignore_ascii_case("collate"),
        _ => false,
    };
    let as_double = |column: &str| -> Option<Tok> {
        lift_operand_typed(&format!("{column}::float8"), schema).ok().map(|(text, _)| Tok::Lifted(text))
    };
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    while i < toks.len() {
        let before = if i == 0 { None } else { toks.get(i - 1) };
        let pair = if comparison(toks.get(i + 1)) { kind(i).zip(kind(i + 2)) } else { None };
        if let Some(((a, ta), (b, tb))) = pair {
            let decimal = ta == DbType::DECIMAL || tb == DbType::DECIMAL;
            let float = matches!(ta, DbType::F32 | DbType::F64) || matches!(tb, DbType::F32 | DbType::F64);
            if decimal && float && !binds(before) && !binds(toks.get(i + 3)) {
                // The NUMERIC side lifted to a double, and a `real` widened;
                // a double column is read as it is.
                let side = |name: &str, t: &DbType| -> Option<Tok> {
                    if *t == DbType::F64 { Some(Tok::Word(name.to_string())) } else { as_double(name) }
                };
                if let (Some(left), Some(right)) = (side(&a, &ta), side(&b, &tb)) {
                    out.extend([Tok::LPar, left, toks[i + 1].clone(), right, Tok::RPar]);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(toks[i].clone());
        i += 1;
    }
    out
}

/// Functions whose result is text, for the 42883 of `upper(t) = 0.1::float8`.
const TEXT_FUNCTIONS: &[&str] = &[
    "upper", "lower", "trim", "btrim", "ltrim", "rtrim", "substr", "substring", "concat", "concat_ws", "replace",
    "left", "right", "lpad", "rpad", "initcap", "md5", "to_char", "reverse", "repeat", "split_part", "translate",
    "format", "quote_ident", "quote_literal", "chr",
];

/// The PostgreSQL name of `field`'s type when it is one with no comparison
/// with a float (PostgreSQL answers 42883): text and character types, the
/// date and time types, boolean, bytea, inet and interval. `None` for a
/// number, and for the rest, which keep their own answers.
fn no_float_comparison_type(field: &SchemaField) -> Option<&'static str> {
    use crate::core::db_type::DbType;
    use crate::core::rql::type_modifier::TextTypmod;
    Some(match field.literal_type() {
        DbType::STRING | DbType::CHAR => match field.text_typmod() {
            Some(TextTypmod::Bpchar(_)) => "character",
            Some(TextTypmod::Varchar(_)) => "character varying",
            _ => "text",
        },
        DbType::BPCHAR(_) => "character",
        DbType::TIMESTAMP => "timestamp without time zone",
        DbType::DATETIME => "timestamp with time zone",
        DbType::DATE => "date",
        DbType::TIME => "time without time zone",
        DbType::TIMETZ => "time with time zone",
        DbType::BOOL => "boolean",
        DbType::BYTEA => "bytea",
        DbType::INET => "inet",
        DbType::INTERVAL => "interval",
        _ => return None,
    })
}

/// The PostgreSQL name of a float token's type: `real` for a `::real` or
/// `::float4` cast, `double precision` otherwise (a 42883 message named a
/// `::real` double precision).
fn float_type_name(tok: &Tok) -> &'static str {
    match tok {
        Tok::Float { written, .. } => {
            let lower = written.to_ascii_lowercase();
            if lower.ends_with("::real") || lower.ends_with("::float4") {
                "real"
            } else {
                "double precision"
            }
        }
        _ => "double precision",
    }
}

/// A float token's number: its shortest text, or for one that is not finite
/// its quoted literal (`'NaN'`, `'-Infinity'`).
fn float_value(number: &str) -> Option<f64> {
    match number.strip_prefix('\'').and_then(|n| n.strip_suffix('\'')) {
        Some(quoted) => crate::core::value_codec::parse_float8(quoted),
        None => number.parse().ok(),
    }
}

/// `column op float` over an exact column, as PostgreSQL decides it — the
/// column's value converted to the nearest double — written as comparisons
/// with exact values the scan and the condition evaluator read at the
/// column's scale: the values that round to the float are one run of the
/// column's values, `l..=h`. Written as the float's own digits, the column
/// was compared exactly, and `n <> 1234567890.123456789::float8` admitted a
/// NUMERIC(30,9) 1234567890.123456789 whose nearest double is that float.
fn exact_against_float(column: &(String, u32, i128, i128), op: &str, number: &str) -> Option<Vec<Tok>> {
    let (name, scale, lo, hi) = column;
    let (scale, lo, hi) = (*scale, *lo, *hi);
    let v = float_value(number)?;
    let (l, h) = float_run(scale, lo, hi, v);
    let lit = |m: i128| Tok::Word(crate::core::value_codec::format_decimal_scaled(m, scale));
    let cmp_tok = |op: &str, m: i128| vec![Tok::Word(name.clone()), Tok::Sym(op.to_string()), lit(m)];
    // Every value / none, each a comparison so NULL stays NULL.
    let all = || cmp_tok("<=", hi);
    let none = || cmp_tok(">", hi);
    let below_l = || l.map_or_else(all, |l| cmp_tok("<", l));
    let from_l = || l.map_or_else(none, |l| cmp_tok(">=", l));
    let to_h = || h.map_or_else(none, |h| cmp_tok("<=", h));
    let above_h = || h.map_or_else(all, |h| cmp_tok(">", h));
    let joined = |a: Vec<Tok>, kw: &str, b: Vec<Tok>| {
        let mut out = vec![Tok::LPar];
        out.extend(a);
        out.push(Tok::Word(kw.to_string()));
        out.extend(b);
        out.push(Tok::RPar);
        out
    };
    let group = |inner: Vec<Tok>| {
        let mut out = vec![Tok::LPar];
        out.extend(inner);
        out.push(Tok::RPar);
        out
    };
    Some(match op {
        "<" => group(below_l()),
        ">=" => group(from_l()),
        "<=" => group(to_h()),
        ">" => group(above_h()),
        "=" => match (l, h) {
            (Some(l), Some(h)) if l == h => group(cmp_tok("=", l)),
            (Some(_), Some(_)) => joined(from_l(), "AND", to_h()),
            _ => group(none()),
        },
        "<>" | "!=" => match (l, h) {
            (Some(l), Some(h)) if l == h => group(cmp_tok("<>", l)),
            (Some(_), Some(_)) => joined(below_l(), "OR", above_h()),
            _ => group(all()),
        },
        _ => return None,
    })
}

/// `column [NOT] IN (float, …)` over an exact column ([`exact_against_float`]
/// per item), as an ordinary IN list of the floats whose run is one value —
/// nearly all of them, which the scan matches as it matches any IN list —
/// and the few runs of many values, sorted and merged, as ranges. Each item
/// written as its own pair of comparisons made a list of 500 floats a chain
/// of 500 ORs every row walked. A NULL stays in the IN list, so `IN` without
/// a match is unknown and `NOT IN` is never true, as PostgreSQL has them.
fn exact_in_floats(
    column: &(String, u32, i128, i128),
    negated: bool,
    numbers: &[&str],
    null: bool,
    lifted: impl FnOnce() -> Option<String>,
) -> Option<Vec<Tok>> {
    let (name, scale, lo, hi) = column;
    let (scale, lo, hi) = (*scale, *lo, *hi);
    let mut singles: Vec<i128> = Vec::new();
    let mut runs: Vec<(i128, i128)> = Vec::new();
    // The floats finer than the column there, for the scan: compared as
    // `column::float8 = v`, which the scan answers from one hashed set
    // (`transformer::collapse_in_sets`), PostgreSQL's comparison itself.
    let mut fine: Vec<&str> = Vec::new();
    let mut lifted = Some(lifted);
    let mut operand: Option<Option<String>> = None;
    for number in numbers {
        let v = float_value(number)?;
        // NaN and the infinities round from no value of the column: their
        // runs (`float_run`) say so, with no float compare to lift.
        if v.is_finite() && float_run_coarse(scale, lo, hi, v).is_none() {
            let op = operand.get_or_insert_with(|| lifted.take().and_then(|f| f()));
            if op.is_some() {
                fine.push(number);
                continue;
            }
        }
        match float_run(scale, lo, hi, v) {
            (Some(l), Some(h)) if l == h => singles.push(l),
            (Some(l), Some(h)) if l < h => runs.push((l, h)),
            _ => {}
        }
    }
    singles.sort_unstable();
    singles.dedup();
    runs.sort_unstable();
    let mut merged: Vec<(i128, i128)> = Vec::with_capacity(runs.len());
    for (l, h) in runs {
        match merged.last_mut() {
            Some(last) if l <= last.1.saturating_add(1) => last.1 = last.1.max(h),
            _ => merged.push((l, h)),
        }
    }
    let word = |w: &str| Tok::Word(w.to_string());
    let lit = |m: i128| Tok::Word(crate::core::value_codec::format_decimal_scaled(m, scale));
    let col = || Tok::Word(name.clone());
    let mut parts: Vec<Vec<Tok>> = Vec::new();
    if !singles.is_empty() || null {
        let mut list = vec![col()];
        if negated {
            list.push(word("NOT"));
        }
        list.push(word("IN"));
        list.push(Tok::LPar);
        for (k, m) in singles.iter().enumerate() {
            if k > 0 {
                list.push(Tok::Comma);
            }
            list.push(lit(*m));
        }
        if null {
            if !singles.is_empty() {
                list.push(Tok::Comma);
            }
            list.push(word("NULL"));
        }
        list.push(Tok::RPar);
        parts.push(list);
    }
    if let (false, Some(Some(operand))) = (fine.is_empty(), &operand) {
        let mut group = vec![Tok::LPar];
        for (k, number) in fine.iter().enumerate() {
            if k > 0 {
                group.push(word(if negated { "AND" } else { "OR" }));
            }
            group.push(Tok::Lifted(operand.clone()));
            group.push(Tok::Sym((if negated { "<>" } else { "=" }).to_string()));
            group.push(Tok::Word(number.to_string()));
        }
        group.push(Tok::RPar);
        parts.push(group);
    }
    for (l, h) in merged {
        let (a, kw, b) = if negated { ("<", "OR", ">") } else { (">=", "AND", "<=") };
        parts.push(vec![
            Tok::LPar,
            col(),
            Tok::Sym(a.to_string()),
            lit(l),
            word(kw),
            col(),
            Tok::Sym(b.to_string()),
            lit(h),
            Tok::RPar,
        ]);
    }
    if parts.is_empty() {
        // Every float rounds to no value: IN holds for none, NOT IN for all.
        parts.push(vec![col(), Tok::Sym((if negated { "<=" } else { ">" }).to_string()), lit(hi)]);
    }
    let mut out = vec![Tok::LPar];
    for (k, part) in parts.into_iter().enumerate() {
        if k > 0 {
            out.push(word(if negated { "AND" } else { "OR" }));
        }
        out.extend(part);
    }
    out.push(Tok::RPar);
    Some(out)
}

/// `column op float`, `float op column` and `column [NOT] IN (float, …)` over
/// a column that keeps each value's own scale (`value_codec::keeps_own_scale`):
/// the column's value converted to its nearest double, as PostgreSQL compares
/// `numeric` with `float8`. Such a column has no one scale to count a run of
/// values at ([`exact_against_float`]), and read at the engine's nine places
/// `n = '0.1'::float8` missed 0.10000000000000001 (and a CHECK admitted it).
///
/// For the scan, a lifted `column::float8` (an IN list a hashed set over it,
/// `transformer::collapse_in_sets`); for the expression evaluator (a CHECK, a
/// join's post-filter), the float typed `'…'::float8`, which `expr::compare`
/// reads by nearest double. A NULL in a NOT IN keeps it never true.
fn own_scale_against_floats(toks: Vec<Tok>, schema: &SmallVec<[SchemaField; 20]>, scan: bool) -> Vec<Tok> {
    if !toks.iter().any(|t| matches!(t, Tok::Float { .. }))
        || !schema.iter().any(|f| !f.is_deleted && crate::core::value_codec::keeps_own_scale(f))
    {
        return toks;
    }
    let comparison = |t: Option<&Tok>| -> Option<String> {
        match t {
            Some(Tok::Sym(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") => Some(op.clone()),
            _ => None,
        }
    };
    let binds = |t: Option<&Tok>| match t {
        Some(Tok::Sym(op)) => !matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="),
        Some(Tok::Word(w)) => w.eq_ignore_ascii_case("between") || w.eq_ignore_ascii_case("collate"),
        _ => false,
    };
    let column_at = |i: usize| -> Option<String> {
        let Some(Tok::Word(w)) = toks.get(i) else { return None };
        let name = resolve_column(w, schema)?;
        schema
            .iter()
            .any(|f| !f.is_deleted && f.name == name && crate::core::value_codec::keeps_own_scale(f))
            .then(|| w.clone())
    };
    let float_at = |i: usize| -> Option<&str> {
        match toks.get(i) {
            Some(Tok::Float { number, .. }) => Some(number.as_str()),
            _ => None,
        }
    };
    let mirror = |op: &str| match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
    .to_string();
    // The column's side, and a float as its other side, in the reader's form.
    let side = |column: &str| -> Option<Tok> {
        if scan {
            lift_operand_typed(&format!("{column}::float8"), schema).ok().map(|(text, _)| Tok::Lifted(text))
        } else {
            Some(Tok::Word(column.to_string()))
        }
    };
    let value = |number: &str| -> Tok {
        if scan {
            // NaN and the infinities as the largest finite double (NaN sorts
            // above every number, as PostgreSQL has it), which compares with
            // every value such a column holds (38 digits) exactly as they
            // do: the scan reads no quoted float.
            match float_value(number).filter(|v| !v.is_finite()) {
                Some(v) if v == f64::NEG_INFINITY => Tok::Word("-1.7976931348623157e308".to_string()),
                Some(_) => Tok::Word("1.7976931348623157e308".to_string()),
                None => Tok::Word(number.to_string()),
            }
        } else if number.starts_with('\'') {
            Tok::Word(format!("{number}::float8"))
        } else {
            Tok::Word(format!("'{number}'::float8"))
        }
    };
    let before = |i: usize| if i == 0 { None } else { toks.get(i - 1) };
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    while i < toks.len() {
        if !binds(before(i)) && !binds(toks.get(i + 3)) {
            // column op float
            if let (Some(column), Some(op), Some(number)) = (column_at(i), comparison(toks.get(i + 1)), float_at(i + 2)) {
                if let Some(left) = side(&column) {
                    out.extend([Tok::LPar, left, Tok::Sym(op), value(number), Tok::RPar]);
                    i += 3;
                    continue;
                }
            }
            // float op column
            if let (Some(number), Some(op), Some(column)) = (float_at(i), comparison(toks.get(i + 1)), column_at(i + 2)) {
                if let Some(left) = side(&column) {
                    out.extend([Tok::LPar, left, Tok::Sym(mirror(&op)), value(number), Tok::RPar]);
                    i += 3;
                    continue;
                }
            }
        }
        // column [NOT] IN (float, …), NULLs among them
        if let Some(column) = column_at(i).filter(|_| !binds(before(i))) {
            let negated = toks.get(i + 1).is_some_and(|t| t.is_keyword("not"));
            let open = i + 1 + negated as usize + 1;
            if toks.get(open - 1).is_some_and(|t| t.is_keyword("in")) && matches!(toks.get(open), Some(Tok::LPar)) {
                let mut numbers = Vec::new();
                let mut null = false;
                let mut floats = 0usize;
                let mut j = open + 1;
                let mut closed = false;
                loop {
                    // A plain number beside a float takes the list's common
                    // type, float8, as PostgreSQL resolves it: `n IN
                    // ('0.1'::float8, 2.5)` compares n's nearest double with
                    // each item's.
                    match (float_at(j), toks.get(j)) {
                        (Some(number), _) => {
                            floats += 1;
                            numbers.push(number)
                        }
                        (None, Some(t)) if t.is_keyword("null") => null = true,
                        (None, Some(Tok::Word(w)))
                            if w.parse::<f64>().is_ok_and(f64::is_finite)
                                && w.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-')) =>
                        {
                            numbers.push(w.as_str())
                        }
                        _ => break,
                    }
                    match toks.get(j + 1) {
                        Some(Tok::Comma) => j += 2,
                        Some(Tok::RPar) => {
                            closed = true;
                            break;
                        }
                        _ => break,
                    }
                }
                // The evaluator has no IN of its own to keep a NULL's unknown in.
                let fits = closed && floats > 0 && (scan || !null);
                if let (true, Some(left)) = (fits, side(&column)) {
                    let (op, kw) = if negated { ("<>", "AND") } else { ("=", "OR") };
                    out.push(Tok::LPar);
                    for (k, number) in numbers.iter().enumerate() {
                        if k > 0 {
                            out.push(Tok::Word(kw.to_string()));
                        }
                        out.extend([left.clone(), Tok::Sym(op.to_string()), value(number)]);
                    }
                    if negated && null {
                        out.extend([
                            Tok::Word("AND".to_string()),
                            Tok::Word(column.clone()),
                            Tok::Word("NOT".to_string()),
                            Tok::Word("IN".to_string()),
                            Tok::LPar,
                            Tok::Word("NULL".to_string()),
                            Tok::RPar,
                        ]);
                    }
                    out.push(Tok::RPar);
                    i = j + 2;
                    continue;
                }
            }
        }
        out.push(toks[i].clone());
        i += 1;
    }
    out
}

/// Every `column op float`, `float op column` and `column [NOT] IN (float,
/// …)` over an exact column, rewritten by [`exact_against_float`]. Only a
/// bare column and a bare float qualify: an arithmetic or cast neighbour
/// makes either an operand of something else, left as it was.
fn exact_columns_against_floats(toks: Vec<Tok>, schema: &SmallVec<[SchemaField; 20]>, scan: bool) -> Vec<Tok> {
    let comparison = |t: Option<&Tok>| -> Option<String> {
        match t {
            Some(Tok::Sym(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") => Some(op.clone()),
            _ => None,
        }
    };
    // A neighbour that makes a column or a float an operand of more.
    let binds = |t: Option<&Tok>| match t {
        Some(Tok::Sym(op)) => !matches!(op.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="),
        Some(Tok::Word(w)) => w.eq_ignore_ascii_case("between") || w.eq_ignore_ascii_case("collate"),
        _ => false,
    };
    let column_at = |i: usize| -> Option<(String, u32, i128, i128)> {
        match toks.get(i) {
            Some(Tok::Word(w)) => exact_column(w, schema),
            _ => None,
        }
    };
    let float_at = |i: usize| -> Option<&str> {
        match toks.get(i) {
            Some(Tok::Float { number, .. }) => Some(number.as_str()),
            _ => None,
        }
    };
    let mirror = |op: &str| match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
    .to_string();
    let before = |i: usize| if i == 0 { None } else { toks.get(i - 1) };
    let mut out: Vec<Tok> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    while i < toks.len() {
        // column op float
        if let (Some(column), Some(op), Some(number)) = (column_at(i), comparison(toks.get(i + 1)), float_at(i + 2)) {
            if !binds(before(i)) && !binds(toks.get(i + 3)) {
                if let Some(lowered) = exact_against_float(&column, &op, number) {
                    out.extend(lowered);
                    i += 3;
                    continue;
                }
            }
        }
        // float op column
        if let (Some(number), Some(op), Some(column)) = (float_at(i), comparison(toks.get(i + 1)), column_at(i + 2)) {
            if !binds(before(i)) && !binds(toks.get(i + 3)) {
                if let Some(lowered) = exact_against_float(&column, &mirror(&op), number) {
                    out.extend(lowered);
                    i += 3;
                    continue;
                }
            }
        }
        // column [NOT] IN (float, …), NULLs among them
        if let Some(column) = column_at(i).filter(|_| !binds(before(i))) {
            let negated = toks.get(i + 1).is_some_and(|t| t.is_keyword("not"));
            let open = i + 1 + negated as usize + 1;
            if toks.get(open - 1).is_some_and(|t| t.is_keyword("in")) && matches!(toks.get(open), Some(Tok::LPar)) {
                let mut numbers = Vec::new();
                let mut null = false;
                let mut floats = 0usize;
                let mut j = open + 1;
                let mut closed = false;
                loop {
                    // A plain number beside a float takes the list's common
                    // type, float8, as PostgreSQL resolves it: `w IN
                    // ('0.1'::float8, 2.5)` compares w's nearest double with
                    // each item's, so it is one more run. Read as the
                    // column's own type the float missed every value it
                    // rounds from (0.10000000000000000001), and NOT IN kept
                    // them.
                    match (float_at(j), toks.get(j)) {
                        (Some(number), _) => {
                            floats += 1;
                            numbers.push(number)
                        }
                        (None, Some(t)) if t.is_keyword("null") => null = true,
                        (None, Some(Tok::Word(w)))
                            if w.parse::<f64>().is_ok_and(f64::is_finite)
                                && w.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-')) =>
                        {
                            numbers.push(w.as_str())
                        }
                        _ => break,
                    }
                    match toks.get(j + 1) {
                        Some(Tok::Comma) => j += 2,
                        Some(Tok::RPar) => {
                            closed = true;
                            break;
                        }
                        _ => break,
                    }
                }
                if closed && floats > 0 {
                    let lifted = || scan.then(|| lift_operand_typed(&format!("{}::float8", column.0), schema).ok().map(|(text, _)| text)).flatten();
                    if let Some(lowered) = exact_in_floats(&column, negated, &numbers, null, lifted) {
                        out.extend(lowered);
                        i = j + 2;
                        continue;
                    }
                }
            }
        }
        out.push(toks[i].clone());
        i += 1;
    }
    out
}

pub fn rewrite_where(clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    rewrite_where_in(clause, schema, false)
}

/// [`rewrite_where`]; `scan` when the scan reads the result
/// ([`rewrite_scan_where`]), which can take a lifted operand.
fn rewrite_where_in(clause: &str, schema: &SmallVec<[SchemaField; 20]>, scan: bool) -> Result<String, String> {
    if clause.trim().is_empty() {
        return Ok(String::new());
    }

    // The regular-expression operators as `regexp_like` (a no-op once
    // `rewrite_scan_where` has done it).
    let clause = &crate::core::rql::check::lower_regex_operators(clause, "regexp_like");
    // Row-value comparisons become their boolean form first, so everything
    // downstream — the NULL fold, the guards, the tokenizer — sees only shapes
    // it already understands.
    let clause = &expand_row_value_comparisons(clause)?;

    // `( <expression> ) IS [NOT] TRUE | FALSE | UNKNOWN` is expanded before
    // `x = NULL` is folded to false: under a truth test a comparison with NULL
    // is *unknown*, not false, and `(a = NULL) IS UNKNOWN` holds for every
    // row. See `parenthesised_truth_test`.
    let clause = &expand_truth_tests(clause, schema)?;

    // `NOT <condition>` is pushed down to its comparisons, three-valued, and
    // before the NULL fold for the same reason as the truth tests. See
    // `expand_not`.
    let clause = &expand_not(clause, schema)?;

    // `x = NULL` is never true in SQL — NULL is not a value anything equals,
    // which is why `IS NULL` exists. Folded to a false predicate here so the
    // comparison never reaches the tokenizer, where an untyped NULL against a
    // typed column is a compile error rather than a row that does not match.
    let clause = &fold_null_comparisons(clause);

    let toks = floats_for_the_scan(lex(clause)?, schema, scan)?;
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    // Where the comparison currently being read began, as an index into `out`.
    //
    // A nullable column that turns up *inside* an arithmetic operand —
    // `subtotal + tax_amount > 100` — cannot guard itself by taking back the
    // two tokens before it, the way a column on the right of a comparison does:
    // the `+` is not the comparison, and popping it left the rest of the
    // comparison outside the guard's parentheses. The whole term has to be
    // wrapped, and this is where it starts.
    let mut term_start = 0usize;
    // The comparison after a `NOT` that `expand_not` left is read without
    // NULL guards up to here: `expand_not` already put them outside the `NOT`,
    // where they belong. A guard inside it would be negated with the
    // comparison, and a NULL row would match.
    let mut unguarded_until = 0usize;

    while i < toks.len() {
        let tok = &toks[i];
        let guarding = i >= unguarded_until;
        let guard = |column: &str, fragment: &str, schema: &SmallVec<[SchemaField; 20]>| {
            if guarding {
                guard_null(column, fragment, schema)
            } else {
                fragment.to_string()
            }
        };

        // A term begins after each connective, and after a parenthesis that
        // opens a *boolean* group.
        //
        // The distinction matters: `(a = 1 OR b = 2)` is a group of terms and a
        // guard belongs inside it, while `(x - y) > z` is an operand and a
        // guard inserted inside it would turn the operand into a boolean group
        // — which is exactly what the planner then refused.
        if tok.is_keyword("and") || tok.is_keyword("or") {
            term_start = out.len() + 1;
        } else if matches!(tok, Tok::LPar) && opens_boolean_group(&toks, i) {
            term_start = out.len() + 1;
        }

        // A lifted operand goes on as it is. It needs no NULL guard: the
        // evaluator answers NULL for it, and the comparison holding it is then
        // not true, negated or not.
        if let Tok::Lifted(text) = tok {
            out.push(text.clone());
            i += 1;
            // Its `IS [NOT] NULL`, which the lift spelled as one word, is a
            // test the evaluator answers rather than a column's companion.
            if let Some(test) = toks
                .get(i)
                .filter(|t| t.is_keyword("isnull") || t.is_keyword("notnull"))
            {
                out.push(test.render().to_ascii_uppercase());
                i += 1;
            }
            continue;
        }

        // A path into a document followed by a form that needs the whole
        // operand rather than the column: `doc->>'a' IN (…)`, `IS NULL`,
        // `BETWEEN`, `NOT LIKE`, also with the path in parentheses.
        if let Some((rewritten, after)) = path_sugar(&toks, i, schema, guarding)? {
            out.push(rewritten);
            i = after;
            continue;
        }

        // A word that names a column may start one of the sugar forms.
        if let Tok::Word(raw) = tok {
            if let Some(col) = resolve_column(raw, schema) {
                // Look past the column at what follows.
                let next = toks.get(i + 1);

                // `col ISNULL` / `col NOTNULL`, PostgreSQL's one-word
                // spellings of `IS NULL` and `IS NOT NULL`.
                let one_word_null_test = next.and_then(|t| {
                    if t.is_keyword("isnull") {
                        Some(true)
                    } else if t.is_keyword("notnull") {
                        Some(false)
                    } else {
                        None
                    }
                });
                if let Some(want_null) = one_word_null_test {
                    out.push(match nullable::flag_field(&col, schema) {
                        Some(flag) => format!("{} = {}", flag.name, u8::from(want_null)),
                        None if want_null => CONSTANT_FALSE.to_string(),
                        None => CONSTANT_TRUE.to_string(),
                    });
                    i += 2;
                    continue;
                }

                // `col IS [NOT] NULL` and `col IS [NOT] DISTINCT FROM x`
                if next.map(|t| t.is_keyword("is")).unwrap_or(false) {
                    let negated = toks
                        .get(i + 2)
                        .map(|t| t.is_keyword("not"))
                        .unwrap_or(false);
                    let null_at = if negated { i + 3 } else { i + 2 };

                    // `IS [NOT] DISTINCT FROM` is the null-safe comparison: it
                    // answers true/false where `=` would answer NULL, which is
                    // why an ORM reaches for it on a nullable column.
                    //
                    // Expressed through the companion rather than a three-valued
                    // evaluator, like every other null-aware form here:
                    //
                    //   a IS DISTINCT FROM v      ->  (__null_a = 1 OR a != v)
                    //   a IS NOT DISTINCT FROM v  ->  (__null_a = 0 AND a = v)
                    //
                    // On a NOT NULL column there is no companion and no third
                    // outcome, so it degenerates to plain `!=` / `=`.
                    if toks
                        .get(null_at)
                        .map(|t| t.is_keyword("distinct"))
                        .unwrap_or(false)
                    {
                        if !toks
                            .get(null_at + 1)
                            .map(|t| t.is_keyword("from"))
                            .unwrap_or(false)
                        {
                            return Err("IS DISTINCT must be followed by FROM".to_string());
                        }
                        // Read as a value, not as a raw token: `false` has to
                        // become the `0` a boolean column stores, and `-1`
                        // arrives as a sign and a number. Taken token by token
                        // they reached the tokenizer as the column `false` and
                        // as a stray `-`.
                        if matches!(toks.get(null_at + 2), Some(Tok::LPar)) {
                            return Err(
                                "IS DISTINCT FROM an expression is not supported yet".to_string()
                            );
                        }
                        let (value, after_value) = bound_at(&toks, null_at + 2)
                            .ok_or("IS DISTINCT FROM names no value")?;

                        // `IS DISTINCT FROM NULL` is exactly `IS NOT NULL`, and
                        // the negation is `IS NULL`. Handled here because the
                        // rewrite below would compare against the word "NULL".
                        if value.eq_ignore_ascii_case("null") {
                            out.push(match nullable::flag_field(&col, schema) {
                                Some(flag) => format!("{} = {}", flag.name, u8::from(negated)),
                                // A constant: see `IS NULL` below for why not
                                // `col = col`.
                                None if negated => CONSTANT_FALSE.to_string(),
                                None => CONSTANT_TRUE.to_string(),
                            });
                            i = after_value;
                            continue;
                        }

                        // The value may be a nullable column itself, whose NULL
                        // is as much a value here as the column's: two NULLs
                        // are not distinct, and a NULL is distinct from any
                        // value. Read as its zeroes, it was neither.
                        let other = resolve_column(&value, schema).and_then(|c| {
                            nullable::flag_field(&c, schema).map(|f| (c, f.name.clone()))
                        });
                        if let Some((value, other_flag)) = other {
                            let fragment = match nullable::flag_field(&col, schema) {
                                Some(flag) if negated => format!(
                                    "(({f} = 1 AND {g} = 1) OR ({f} = 0 AND {g} = 0 AND {c} = {v}))",
                                    f = flag.name,
                                    g = other_flag,
                                    c = col,
                                    v = value
                                ),
                                Some(flag) => format!(
                                    "(({f} = 1 AND {g} = 0) OR ({f} = 0 AND {g} = 1) \
                                     OR ({f} = 0 AND {g} = 0 AND {c} != {v}))",
                                    f = flag.name,
                                    g = other_flag,
                                    c = col,
                                    v = value
                                ),
                                None if negated => {
                                    format!("({} = 0 AND {} = {})", other_flag, col, value)
                                }
                                None => format!("({} = 1 OR {} != {})", other_flag, col, value),
                            };
                            out.push(fragment);
                            i = after_value;
                            continue;
                        }

                        out.push(match nullable::flag_field(&col, schema) {
                            Some(flag) if negated => {
                                format!("({} = 0 AND {} = {})", flag.name, col, value)
                            }
                            Some(flag) => {
                                format!("({} = 1 OR {} != {})", flag.name, col, value)
                            }
                            None if negated => format!("{} = {}", col, value),
                            None => format!("{} != {}", col, value),
                        });
                        i = after_value;
                        continue;
                    }

                    // `flag IS [NOT] TRUE | FALSE | UNKNOWN`.
                    if let Some(truth) = toks.get(null_at).and_then(truth_keyword) {
                        let is_bool = schema.iter().any(|f| {
                            f.name == col && f.db_type == crate::core::db_type::DbType::BOOL
                        });
                        if !is_bool {
                            return Err(format!(
                                "argument of IS {} must be type boolean, not the column '{}'",
                                truth.to_ascii_uppercase(),
                                col
                            ));
                        }
                        out.push(truth_test(&col, None, truth, negated, schema));
                        i = null_at + 1;
                        continue;
                    }

                    if !toks
                        .get(null_at)
                        .map(|t| t.is_keyword("null"))
                        .unwrap_or(false)
                    {
                        return Err(
                            "IS must be followed by NULL, NOT NULL, TRUE, FALSE, UNKNOWN, or [NOT] DISTINCT FROM"
                                .to_string(),
                        );
                    }

                    out.push(match nullable::flag_field(&col, schema) {
                        // The companion holds 1 for NULL, so the test is a
                        // plain equality against it.
                        Some(flag) => format!("{} = {}", flag.name, u8::from(!negated)),
                        // A `NOT NULL` column can never be NULL, so the test is
                        // a constant — written as one. It used to be the column
                        // compared against itself, which is not a constant for
                        // a float: NaN != NaN under IEEE, so on a NOT NULL
                        // float column `IS NULL` returned every NaN row and
                        // `IS NOT NULL` dropped them.
                        None if negated => CONSTANT_TRUE.to_string(),
                        None => CONSTANT_FALSE.to_string(),
                    });
                    i = null_at + 1;
                    continue;
                }

                // `col = ANY('{a,b}')` and `col <> ALL('{a,b}')`.
                //
                // This is how a PostgreSQL client writes `IN` when the list is
                // a bound array rather than written out — sea-orm emits it for
                // every batched fetch by id, so an application doing ordinary
                // work hits it constantly. Rewritten into the same expansion
                // `IN` gets rather than given a path of its own.
                if let Some(rewritten) = any_or_all_expansion(&toks, i, &col, schema)? {
                    out.push(rewritten.0);
                    i = rewritten.1;
                    continue;
                }

                // `col [NOT] IN (...)` and `col [NOT] BETWEEN a AND b`
                let negated = next.map(|t| t.is_keyword("not")).unwrap_or(false);
                let kw_at = if negated { i + 2 } else { i + 1 };

                if toks.get(kw_at).map(|t| t.is_keyword("in")).unwrap_or(false) {
                    let (values, after) = parse_in_list(&toks, kw_at + 1)?;

                    // `x IN ()` arises when a subquery returned nothing. It can
                    // never hold, and `x NOT IN ()` always holds — constants,
                    // for every value including NaN and NULL.
                    if values.is_empty() {
                        out.push(if negated {
                            CONSTANT_TRUE.to_string()
                        } else {
                            CONSTANT_FALSE.to_string()
                        });
                        i = after;
                        continue;
                    }

                    // A NULL in the list: `x IN (1, NULL)` is `x = 1 OR
                    // unknown`, which holds exactly where `x = 1` does, so the
                    // NULL drops out. `x NOT IN (1, NULL)` is `x <> 1 AND
                    // unknown`, which is never true — for any row, NULL or
                    // not — so the whole term is the constant.
                    let has_null = values.iter().any(|v| v.eq_ignore_ascii_case("null"));
                    let values: Vec<&String> = values
                        .iter()
                        .filter(|v| !v.eq_ignore_ascii_case("null"))
                        .collect();
                    if (negated && has_null) || values.is_empty() {
                        out.push(CONSTANT_FALSE.to_string());
                        i = after;
                        continue;
                    }

                    let joiner = if negated { " AND " } else { " OR " };
                    let op = if negated { "!=" } else { "=" };
                    // A column in the list is an operand like any other, with
                    // its own guard: `a IN (b, c)` is `a = b OR a = c`, and a
                    // NULL `b` leaves its part unknown, not `a = 0`.
                    let parts: Vec<String> = values
                        .iter()
                        .map(|v| match resolve_column(v, schema) {
                            Some(c) => guard(&c, &format!("{} {} {}", col, op, c), schema),
                            None => format!("{} {} {}", col, op, v),
                        })
                        .collect();
                    out.push(guard(
                        &col,
                        &format!("({})", parts.join(joiner)),
                        schema,
                    ));
                    i = after;
                    continue;
                }

                if toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("between"))
                    .unwrap_or(false)
                {
                    let (lo, after_lo) =
                        bound_at(&toks, kw_at + 1).ok_or("BETWEEN is missing its lower bound")?;
                    if !toks
                        .get(after_lo)
                        .map(|t| t.is_keyword("and"))
                        .unwrap_or(false)
                    {
                        return Err("BETWEEN bounds must be separated by AND".to_string());
                    }
                    let (hi, after_hi) =
                        bound_at(&toks, after_lo + 1).ok_or("BETWEEN is missing its upper bound")?;

                    // `NOT BETWEEN` is the complement: outside the range on
                    // either side, which is a disjunction rather than a
                    // conjunction.
                    //
                    // A bound that is a nullable column guards its own half:
                    // `a NOT BETWEEN b AND c` holds where `a < b` does even if
                    // `c` is NULL, and `a BETWEEN b AND c` never does then.
                    let half = |op: &str, bound: &str| match resolve_column(bound, schema) {
                        Some(c) => guard(&c, &format!("{} {} {}", col, op, c), schema),
                        None => format!("{} {} {}", col, op, bound),
                    };
                    let fragment = if negated {
                        format!("({} OR {})", half("<", &lo), half(">", &hi))
                    } else {
                        format!("({} AND {})", half(">=", &lo), half("<=", &hi))
                    };
                    out.push(guard(&col, &fragment, schema));
                    i = after_hi;
                    continue;
                }

                // A `::` cast between the column and its operator —
                // `hardware_status::text SIMILAR TO '101%'`. The cast travels
                // with the column into the emitted text; the parser applies it
                // per row.
                //
                // Only into these two forms, under a name of its own. It used
                // to replace `col` for the rest of this branch, so an ordinary
                // comparison emitted `qty::int` and then the cast token again
                // after it: the tokenizer read `qty::int ::int = 5`, and the
                // plan was refused with "expected comparison operator, got
                // Cast". The NULL guard is still the column's: its companion
                // is found by the column's own name.
                let (kw_at, cast_col) = match toks.get(kw_at) {
                    Some(Tok::Sym(sym)) if sym.starts_with("::") => {
                        (kw_at + 1, format!("{}{}", col, sym))
                    }
                    _ => (kw_at, col.clone()),
                };

                // `col [NOT] SIMILAR TO 'pattern'`. Two words, so it is
                // folded into the one-word operator the tokenizer's string-op
                // table can carry — the same trick `IS NULL` uses to become a
                // comparison against the nullability companion.
                if toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("similar"))
                    .unwrap_or(false)
                    && toks
                        .get(kw_at + 1)
                        .map(|t| t.is_keyword("to"))
                        .unwrap_or(false)
                {
                    let pattern = toks
                        .get(kw_at + 2)
                        .ok_or("SIMILAR TO is missing its pattern")?;
                    let Tok::Str(literal) = pattern else {
                        return Err("SIMILAR TO needs a quoted pattern".to_string());
                    };
                    // A pattern the matcher cannot use fails the statement here
                    // rather than matching nothing on every row.
                    if let Some(inner) = literal
                        .strip_prefix('\'')
                        .and_then(|l| l.strip_suffix('\''))
                    {
                        crate::core::similar_to::validate(&inner.replace("''", "'"), Some('\\'))?;
                    }
                    // `Tok::Str` keeps the quotes it was written with, so the
                    // literal is emitted as-is. Adding a pair around it made
                    // `''101%''`, which the tokenizer read as an empty string
                    // followed by a bare `101%`.
                    // `NOT SIMILAR TO` negates the match inside the guard, so
                    // a NULL row matches neither form.
                    out.push(guard(
                        &col,
                        &format!("{}{} SIMILARTO {}", if negated { "NOT " } else { "" }, cast_col, literal),
                        schema,
                    ));
                    i = kw_at + 3;
                    continue;
                }

                // `col [NOT] LIKE 'pattern'` / `ILIKE`
                let case_insensitive = toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("ilike"))
                    .unwrap_or(false);
                let is_like = case_insensitive
                    || toks
                        .get(kw_at)
                        .map(|t| t.is_keyword("like"))
                        .unwrap_or(false);
                if is_like {
                    let pattern = toks.get(kw_at + 1).ok_or("LIKE is missing its pattern")?;
                    // A column as the pattern would be read as the text of its
                    // own name, and `a LIKE b` would match nothing, silently.
                    if let Tok::Word(w) = pattern {
                        if resolve_column(w, schema).is_some() {
                            return Err(format!(
                                "LIKE with a column ('{}') as its pattern is not supported yet",
                                w
                            ));
                        }
                    }
                    let (escape, next) = like_escape(&toks, kw_at + 2)?;
                    let (op, literal) =
                        like_to_operator(&pattern_text(pattern), escape, case_insensitive)?;
                    // `NOT LIKE` likewise, inside the guard: NOT of NULL is
                    // NULL, so a NULL row is in neither answer.
                    out.push(guard(
                        &col,
                        &format!("{}{} {} {}", if negated { "NOT " } else { "" }, cast_col, op, literal),
                        schema,
                    ));
                    i = next;
                    continue;
                }

                // A nullable column inside an arithmetic operand. The guard
                // is inserted in front of the whole term rather than wrapped
                // around it, which reads the same — `AND` binds tighter than
                // `OR`, so a term inside an `OR` chain is still guarded as a
                // unit — and needs nothing taken back.
                let preceded_by_arithmetic =
                    i > 0 && matches!(toks.get(i - 1), Some(Tok::Sym(sym)) if is_arithmetic(sym));
                if preceded_by_arithmetic {
                    if nullable::is_nullable(&col, schema) {
                        if let Some(flag) = nullable::flag_field(&col, schema).filter(|_| guarding) {
                            let at = term_start.min(out.len());
                            out.insert(at, format!("{} = 0 AND", flag.name));
                        }
                    }
                    out.push(col);
                    i += 1;
                    continue;
                }

                // A column on the right of an operator — `5 = age`, `a = b` —
                // is an operand, never a truth test; without this check the
                // `age` in `5 = age` became `age = 1` and the clause read
                // `5 = age = 1`.
                let preceded_by_operator = i > 0 && matches!(toks.get(i - 1), Some(Tok::Sym(_)));
                if preceded_by_operator {
                    // `<value> <op> <nullable col>` at the start of a term: the
                    // two tokens already emitted are the rest of this
                    // comparison, so take them back and guard the whole thing,
                    // as the left-operand form below does.
                    let term_starts_at_value = i >= 2
                        && matches!(toks.get(i - 2), Some(Tok::Word(_)) | Some(Tok::Str(_)))
                        && (i == 2
                            || matches!(toks.get(i - 3), Some(Tok::LPar))
                            || toks
                                .get(i - 3)
                                .map(|t| t.is_keyword("and") || t.is_keyword("or"))
                                .unwrap_or(false));
                    // A path after the column is part of the operand, and
                    // has to be inside the guard with it: left outside, it
                    // trailed the closing parenthesis, as `( … 'true' = doc )
                    // -> 'a'`, and the plan was refused.
                    let operand_end = path_end(&toks, i + 1).unwrap_or(i + 1);
                    let mut operand = col.clone();
                    for tok in &toks[i + 1..operand_end] {
                        operand.push(' ');
                        operand.push_str(&tok.render());
                    }
                    if nullable::is_nullable(&col, schema) && term_starts_at_value && out.len() >= 2
                    {
                        let op = out.pop().expect("two entries");
                        let lhs = out.pop().expect("two entries");
                        let fragment = format!("{} {} {}", lhs, op, operand);
                        out.push(guard(&col, &fragment, schema));
                    } else {
                        // A longer left side, as in `id + 1 > b`: the guard
                        // goes in front of the term, as for a column inside
                        // arithmetic, since `AND` binds tighter than `OR`.
                        // Only after a comparison operator — anything else
                        // here is arithmetic, which the branch above took.
                        let after_comparison = matches!(
                            toks.get(i - 1),
                            Some(Tok::Sym(sym)) if negated_operator(sym).is_some()
                        );
                        if after_comparison {
                            if let Some(flag) =
                                nullable::flag_field(&col, schema).filter(|_| guarding)
                            {
                                let at = term_start.min(out.len());
                                out.insert(at, format!("{} = 0 AND", flag.name));
                            }
                        }
                        out.push(operand);
                    }
                    i = operand_end;
                    continue;
                }

                // A column standing on its own is a truth test — `WHERE flag`
                // — which the evaluator has no form for, so it becomes the
                // equality it means. "On its own" is decided by what follows:
                // an operator means this is an ordinary comparison.
                let followed_by_operator = matches!(toks.get(i + 1), Some(Tok::Sym(_)))
                    || toks
                        .get(i + 1)
                        .map(|t| {
                            t.is_keyword("contains")
                                || t.is_keyword("startswith")
                                || t.is_keyword("endswith")
                        })
                        .unwrap_or(false);

                if !followed_by_operator {
                    // `NOT flag` was emitted as a pending negation; consume it.
                    let negate = out.last().map(|s| s == "\u{1}NOT").unwrap_or(false);
                    let fragment = if negate {
                        out.pop();
                        format!("{} = 0", col)
                    } else {
                        format!("{} = 1", col)
                    };
                    out.push(guard(&col, &fragment, schema));
                    i += 1;
                    continue;
                }

                // An ordinary comparison. When the column can be `NULL` the
                // whole comparison has to be consumed, so the guard can wrap
                // it — emitting the column alone would leave the operator and
                // value outside the parentheses.
                if nullable::is_nullable(&col, schema) {
                    let end = comparison_end(&toks, i + 1);

                    // Unless the run that was consumed holds no comparison at
                    // all. That happens when the column is an *operand* inside
                    // parentheses — `(fulfilled_at - created_at) > duration` —
                    // where `comparison_end` stops at the closing bracket and
                    // the comparison itself is outside it. Wrapping that run
                    // produced `( … AND fulfilled_at - created_at ) > duration`:
                    // a boolean group where an operand belongs, and the planner
                    // refused the statement.
                    //
                    // The guard goes in front of the term instead, which reads
                    // the same — `AND` binds tighter than `OR` — and leaves the
                    // parentheses holding what they held.
                    let holds_comparison = toks[i + 1..end].iter().any(|t| {
                        matches!(t, Tok::Sym(sym) if !is_arithmetic(sym))
                            || t.is_keyword("contains")
                            || t.is_keyword("startswith")
                            || t.is_keyword("endswith")
                            || t.is_keyword("similarto")
                    });
                    if !holds_comparison {
                        if let Some(flag) = nullable::flag_field(&col, schema).filter(|_| guarding) {
                            let at = term_start.min(out.len());
                            out.insert(at, format!("{} = 0 AND", flag.name));
                        }
                        out.push(col);
                        i += 1;
                        continue;
                    }

                    // The other columns the comparison reads need guards of
                    // their own. Consumed here, they never pass through the
                    // right-operand guard below, so `a = b` with both
                    // nullable compared `b`'s NULL as its zeroes: `a <> b`
                    // kept the row, and so did `NOT (a = b)`, which becomes
                    // it. Only a run of plain operands gets them: inside a
                    // call such as `COALESCE(b, 0)` or a test such as `IS NOT
                    // TRUE` a NULL `b` does not make the comparison NULL.
                    let mut operands: Vec<String> = Vec::new();
                    let mut plain = true;
                    let mut fragment = vec![col.clone()];
                    for tok in &toks[i + 1..end] {
                        fragment.push(match tok {
                            Tok::Word(w) => match resolve_column(w, schema) {
                                Some(c) => {
                                    if c != col && !operands.contains(&c) {
                                        operands.push(c.clone());
                                    }
                                    c
                                }
                                None => {
                                    plain &= is_plain_operand_word(w);
                                    render_value(tok)
                                }
                            },
                            Tok::LPar | Tok::RPar | Tok::Comma => {
                                plain = false;
                                tok.render()
                            }
                            other => other.render(),
                        });
                    }
                    let mut guarded = fragment.join(" ");
                    if plain {
                        for other in &operands {
                            guarded = guard(other, &guarded, schema);
                        }
                    }
                    out.push(guard(&col, &guarded, schema));
                    i = end;
                    continue;
                }

                // Not nullable: emit the canonical name so the tokenizer's
                // schema lookup, which is exact-match, finds it.
                out.push(col);
                i += 1;
                continue;
            }

            // Not a column.
            if raw.eq_ignore_ascii_case("null") {
                return Err("NULL literals are not supported yet in WHERE clauses".to_string());
            }

            // A `NOT` starting a term is one `expand_not` left before a single
            // comparison it could not complement — `NOT t LIKE 'p%'`, `NOT doc
            // ? 'k'` — with that comparison's NULL guards already outside it.
            // It is handed to the evaluator, which negates that comparison
            // alone, and the comparison is read without guards of its own.
            //
            // Anywhere else `NOT` is recorded and resolved by whatever follows
            // it, which only `NOT <boolean column>` can do.
            if raw.eq_ignore_ascii_case("not") {
                let term_starts_here = i == 0
                    || matches!(toks.get(i - 1), Some(Tok::LPar))
                    || toks[i - 1].is_keyword("and")
                    || toks[i - 1].is_keyword("or");
                if term_starts_here && toks.get(i + 1).is_some_and(|t| !t.is_keyword("not")) {
                    out.push("NOT".to_string());
                    unguarded_until = comparison_end(&toks, i + 1);
                    i += 1;
                    continue;
                }
                out.push("\u{1}NOT".to_string());
                i += 1;
                continue;
            }

            if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false") {
                out.push(render_value(tok));
                i += 1;
                continue;
            }

            // Words that are part of the grammar the evaluator understands.
            const PASS_THROUGH: [&str; 12] = [
                "AND",
                "OR",
                "LIKE",
                "ILIKE",
                "SIMILARTO",
                "CONTAINS",
                "STARTSWITH",
                "ENDSWITH",
                "ICONTAINS",
                "ISTARTSWITH",
                "IENDSWITH",
                "IEQUALS",
            ];
            if PASS_THROUGH.iter().any(|k| raw.eq_ignore_ascii_case(k)) {
                out.push(raw.to_ascii_uppercase());
                i += 1;
                continue;
            }

            // A number running into a word (`id=1AND v=10`) is PostgreSQL's
            // "trailing junk after numeric literal"; read as a literal, the
            // `AND` and the comparison after it were lost.
            if raw.bytes().next().is_some_and(|b| b.is_ascii_digit()) && !is_numeric_literal_word(raw) {
                return Err(format!("trailing junk after numeric literal at or near \"{}\"", raw));
            }
            // A bare number is a literal.
            if raw.bytes().next().is_some_and(|b| b.is_ascii_digit())
                || (raw.starts_with('-') && raw.len() > 1)
            {
                out.push(raw.clone());
                i += 1;
                continue;
            }

            // Anything else is a name the schema does not have. Reporting it
            // here matters: the tokenizer downstream panics on an unknown
            // identifier, which would kill the connection rather than fail the
            // statement. A word that is not called is a column, in
            // PostgreSQL's words (42703); a called one is a function this
            // does not read.
            // Right after a value (`id = 1 ODER BY id`, `x ANDD y`), a word
            // is where an operator or keyword goes: a syntax error (42601).
            const LEADS_AN_OPERAND: [&str; 16] = [
                "AND", "OR", "NOT", "IS", "IN", "BETWEEN", "LIKE", "ILIKE", "ESCAPE", "CASE", "WHEN", "THEN",
                "ELSE", "DISTINCT", "FROM", "SIMILAR",
            ];
            let after_value = match toks.get(i.wrapping_sub(1)) {
                Some(Tok::Str(_) | Tok::RPar) => true,
                Some(Tok::Word(w)) => !LEADS_AN_OPERAND.iter().any(|k| w.eq_ignore_ascii_case(k)),
                _ => false,
            };
            if i > 0 && after_value {
                return Err(format!("syntax error at or near \"{}\"", raw));
            }
            if !matches!(toks.get(i + 1), Some(Tok::LPar))
                && raw.split('.').all(|part| {
                    let part = part.trim_matches('"');
                    !part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_')
                })
            {
                return Err(format!("column \"{}\" does not exist", raw));
            }
            return Err(format!(
                "unknown column or unsupported keyword '{}' in WHERE clause",
                raw
            ));
        }

        out.push(tok.render());
        i += 1;
    }

    if out.iter().any(|s| s == "\u{1}NOT") {
        return Err(
            "NOT is only supported directly before a boolean column, as in `NOT flag`".to_string(),
        );
    }

    Ok(out.join(" "))
}

/// Finds where a comparison ends: at the next `AND`/`OR` outside parentheses,
/// at a parenthesis that closes a group opened before it, or at the end.
///
/// The result is the index just past the comparison's last token.
/// Whether the parenthesis at `at` opens a group of boolean terms rather than
/// a parenthesised operand.
///
/// Decided by what the group holds at its own top level: a comparison operator
/// or an `AND`/`OR` makes it boolean, and arithmetic alone does not.
fn opens_boolean_group(toks: &[Tok], at: usize) -> bool {
    let mut depth = 0usize;
    for tok in &toks[at..] {
        match tok {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            Tok::Sym(sym) if depth == 1 && !is_arithmetic(sym) && !sym.starts_with("::") => {
                return true;
            }
            t if depth == 1 && (t.is_keyword("and") || t.is_keyword("or")) => return true,
            _ => {}
        }
    }
    false
}

/// Where the JSON path steps after the column at `at - 1` end — `->>'a'`,
/// `->'b'->>'c'`, `#>>'{b,c}'`, `->0` — or `None` when no step follows.
fn path_end(toks: &[Tok], at: usize) -> Option<usize> {
    let mut i = at;
    while let Some(Tok::Sym(sym)) = toks.get(i) {
        if !matches!(sym.as_str(), "->" | "->>" | "#>" | "#>>") {
            break;
        }
        i += match (toks.get(i + 1), toks.get(i + 2)) {
            (Some(Tok::Str(_)), _) => 2,
            (Some(Tok::Word(w)), _) if w.bytes().all(|b| b.is_ascii_digit()) => 2,
            (Some(Tok::Sym(sign)), Some(Tok::Word(w)))
                if sign == "-" && w.bytes().all(|b| b.is_ascii_digit()) =>
            {
                3
            }
            _ => return None,
        };
    }
    (i > at).then_some(i)
}

/// A path operand starting at `at` — `doc->>'a'`, optionally cast, or the
/// same in parentheses — as `(document column, operand text, index past it)`.
fn path_operand(
    toks: &[Tok],
    at: usize,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Option<(String, String, usize)> {
    let parenthesised = matches!(toks.get(at), Some(Tok::LPar));
    let start = at + usize::from(parenthesised);
    let Some(Tok::Word(raw)) = toks.get(start) else {
        return None;
    };
    let col = resolve_column(raw, schema)?;
    let mut end = path_end(toks, start + 1)?;
    if matches!(toks.get(end), Some(Tok::Sym(sym)) if sym.starts_with("::")) {
        end += 1;
    }
    let mut text = col.clone();
    for tok in &toks[start + 1..end] {
        text.push(' ');
        text.push_str(&tok.render());
    }
    if parenthesised {
        if !matches!(toks.get(end), Some(Tok::RPar)) {
            return None;
        }
        end += 1;
    }
    Some((col, text, end))
}

/// `<path> [NOT] IN (…)`, `[NOT] BETWEEN`, `IS [NOT] NULL`, `ISNULL`,
/// `NOTNULL` and `NOT LIKE` / `NOT ILIKE` / `NOT SIMILAR TO` over a JSON path
/// at `at`: the rewritten term and the index past it, or `None` when the
/// tokens are not one of those.
///
/// The column forms of these read one word on the left, and a path is several
/// tokens, so `doc->>'a' IN ('x')` reached the engine with its `IN` intact and
/// was refused as an unknown identifier. They expand here into the
/// comparisons they mean. Each comparison with a path that reaches nothing is
/// not true — the path is SQL NULL — so `NOT IN` and `NOT BETWEEN` need no
/// guard of their own, and `IS NULL` is a test the evaluator answers from the
/// path itself. The document column's own NULL is guarded as usual.
fn path_sugar(
    toks: &[Tok],
    at: usize,
    schema: &SmallVec<[SchemaField; 20]>,
    guarding: bool,
) -> Result<Option<(String, usize)>, String> {
    let Some((col, operand, end)) = path_operand(toks, at, schema) else {
        return Ok(None);
    };
    let guard = |fragment: String| {
        if guarding {
            guard_null(&col, &fragment, schema)
        } else {
            fragment
        }
    };
    let negated = toks.get(end).is_some_and(|t| t.is_keyword("not"));
    let kw_at = end + usize::from(negated);
    let Some(keyword) = toks.get(kw_at) else {
        return Ok(None);
    };

    if keyword.is_keyword("in") {
        let (values, after) = parse_in_list(toks, kw_at + 1)?;
        // The same constants the column form folds to: `IN ()` never holds,
        // `NOT IN ()` always does, and a NULL in a `NOT IN` list makes the
        // whole test unknown for every row.
        let has_null = values.iter().any(|v| v.eq_ignore_ascii_case("null"));
        let values: Vec<&String> = values.iter().filter(|v| !v.eq_ignore_ascii_case("null")).collect();
        if values.is_empty() && !has_null {
            let constant = if negated { CONSTANT_TRUE } else { CONSTANT_FALSE };
            return Ok(Some((constant.to_string(), after)));
        }
        if (negated && has_null) || values.is_empty() {
            return Ok(Some((CONSTANT_FALSE.to_string(), after)));
        }
        let (op, joiner) = if negated { ("!=", " AND ") } else { ("=", " OR ") };
        let parts: Vec<String> = values.iter().map(|v| format!("{} {} {}", operand, op, v)).collect();
        return Ok(Some((guard(format!("({})", parts.join(joiner))), after)));
    }

    if keyword.is_keyword("between") {
        let (lo, after_lo) = bound_at(toks, kw_at + 1).ok_or("BETWEEN is missing its lower bound")?;
        if !toks.get(after_lo).is_some_and(|t| t.is_keyword("and")) {
            return Err("BETWEEN bounds must be separated by AND".to_string());
        }
        let (hi, after) = bound_at(toks, after_lo + 1).ok_or("BETWEEN is missing its upper bound")?;
        let fragment = if negated {
            format!("({0} < {1} OR {0} > {2})", operand, lo, hi)
        } else {
            format!("({0} >= {1} AND {0} <= {2})", operand, lo, hi)
        };
        return Ok(Some((guard(fragment), after)));
    }

    // `IS [NOT] NULL` and its one-word spellings. NULL is never unknown, so
    // the document's companion is an alternative rather than a guard: a NULL
    // document has no path either.
    let null_test = match keyword {
        _ if negated => None,
        t if t.is_keyword("isnull") => Some((true, kw_at + 1)),
        t if t.is_keyword("notnull") => Some((false, kw_at + 1)),
        t if t.is_keyword("is") => {
            let not = toks.get(kw_at + 1).is_some_and(|t| t.is_keyword("not"));
            let null_at = kw_at + 1 + usize::from(not);
            toks.get(null_at)
                .is_some_and(|t| t.is_keyword("null"))
                .then_some((!not, null_at + 1))
        }
        _ => None,
    };
    if let Some((want_null, after)) = null_test {
        let test = format!("{} {}", operand, if want_null { "ISNULL" } else { "NOTNULL" });
        let flag = nullable::flag_field(&col, schema).filter(|_| guarding);
        let text = match flag {
            Some(flag) if want_null => format!("({} = 1 OR {})", flag.name, test),
            Some(flag) => format!("({} = 0 AND {})", flag.name, test),
            None => test,
        };
        return Ok(Some((text, after)));
    }

    // `[NOT] LIKE` and `[NOT] ILIKE`, with an optional `ESCAPE`: the pattern
    // is read here exactly as a column's is, so a pattern ending in its escape
    // character is refused (22025) rather than matched as a literal, and the
    // matcher is handed the backslash-escaped form it reads whatever escape
    // the statement chose. The positive forms used to reach the matcher with
    // the pattern unread.
    if keyword.is_keyword("like") || keyword.is_keyword("ilike") {
        let op = if keyword.is_keyword("ilike") { "ILIKE" } else { "LIKE" };
        let Some(pattern @ Tok::Str(_)) = toks.get(kw_at + 1) else {
            return Err(format!("{op} on a JSON path needs a quoted pattern"));
        };
        let (escape, after) = like_escape(toks, kw_at + 2)?;
        let canonical = canonical_like_pattern(&like_pieces(&pattern_text(pattern), escape)?);
        let fragment = format!(
            "{}{} {} {}",
            if negated { "NOT " } else { "" },
            operand,
            op,
            quote_standard(&canonical)
        );
        return Ok(Some((guard(fragment), after)));
    }

    // `NOT SIMILAR TO`: the match, negated inside the guard. The positive
    // form reads through the ordinary comparison.
    if negated {
        let similar = keyword.is_keyword("similar")
            && toks.get(kw_at + 1).is_some_and(|t| t.is_keyword("to"));
        let op = if similar {
            "SIMILARTO"
        } else {
            return Ok(None);
        };
        let pattern_at = kw_at + if similar { 2 } else { 1 };
        let Some(pattern @ Tok::Str(_)) = toks.get(pattern_at) else {
            return Err(format!("NOT {} needs a quoted pattern", keyword.render().to_ascii_uppercase()));
        };
        let fragment = format!("NOT {} {} {}", operand, op, pattern.render());
        return Ok(Some((guard(fragment), pattern_at + 1)));
    }
    Ok(None)
}

/// Whether a symbol is an arithmetic operator rather than a comparison.
fn is_arithmetic(symbol: &str) -> bool {
    matches!(symbol, "+" | "-" | "*" | "/" | "%" | "^")
}

fn comparison_end(toks: &[Tok], start: usize) -> usize {
    let mut depth = 0usize;
    let mut i = start;

    while i < toks.len() {
        match &toks[i] {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
            }
            t if depth == 0 && (t.is_keyword("and") || t.is_keyword("or")) => return i,
            _ => {}
        }
        i += 1;
    }

    i
}

/// Reads the parenthesised value list of an `IN`, returning the rendered values
/// and the index just past the closing parenthesis.
/// Expands `col = ANY('{…}')` / `col <> ALL('{…}')` into the comparison chain
/// `IN` and `NOT IN` produce, or `None` when the tokens are not that shape.
///
/// `= ANY` and `<> ALL` are the two that have an exact reading. `<> ANY` and
/// `= ALL` are deliberately left alone: they are legal SQL with quite different
/// meanings, and guessing at them would answer a question nobody asked.
fn any_or_all_expansion(
    toks: &[Tok],
    col_at: usize,
    col: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Option<(String, usize)>, String> {
    let Some(Tok::Sym(op)) = toks.get(col_at + 1) else {
        return Ok(None);
    };
    let negated = match op.as_str() {
        "=" => false,
        "!=" | "<>" => true,
        _ => return Ok(None),
    };

    let Some(quantifier) = toks.get(col_at + 2) else {
        return Ok(None);
    };
    let quantifier = match quantifier {
        Tok::Word(w) if w.eq_ignore_ascii_case("any") && !negated => "any",
        Tok::Word(w) if w.eq_ignore_ascii_case("all") && negated => "all",
        _ => return Ok(None),
    };
    let _ = quantifier;

    if toks.get(col_at + 3) != Some(&Tok::LPar) {
        return Ok(None);
    }
    let Some(Tok::Str(literal)) = toks.get(col_at + 4) else {
        // A bound parameter — `= ANY($1)` — reaches here with the placeholder
        // unresolved. Left alone so it fails as an unsupported statement rather
        // than being rewritten into something that reads a `$1` as a value.
        return Ok(None);
    };
    if toks.get(col_at + 5) != Some(&Tok::RPar) {
        return Ok(None);
    }

    let Some(values) = pg_array_elements(literal) else {
        return Ok(None);
    };

    // An empty array: `= ANY('{}')` holds for nothing, `<> ALL('{}')` for
    // everything — constants, which the column compared with itself is not for
    // a float holding NaN.
    if values.is_empty() {
        let _ = col;
        let term = if negated {
            CONSTANT_TRUE.to_string()
        } else {
            CONSTANT_FALSE.to_string()
        };
        return Ok(Some((term, col_at + 6)));
    }

    let joiner = if negated { " AND " } else { " OR " };
    let op = if negated { "!=" } else { "=" };
    let parts: Vec<String> = values
        .iter()
        .map(|v| format!("{} {} {}", col, op, v))
        .collect();
    Ok(Some((
        guard_null(col, &format!("({})", parts.join(joiner)), schema),
        col_at + 6,
    )))
}

/// The elements of a PostgreSQL array literal `'{a,"b c",…}'`, each rendered as
/// a SQL literal.
///
/// `None` when the text is not an array literal, so the caller leaves the
/// statement alone rather than rewriting something it has misread.
fn pg_array_elements(literal: &str) -> Option<Vec<String>> {
    let inner = literal.trim();
    let inner = inner.strip_prefix('\'')?.strip_suffix('\'')?;
    // A doubled quote inside the literal is one quote.
    let inner = inner.replace("''", "'");
    let inner = inner.trim().strip_prefix('{')?.strip_suffix('}')?;

    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for c in inner.chars() {
        if escaped {
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if in_quotes {
        return None; // unterminated element
    }
    out.push(current);

    Some(
        out.into_iter()
            .map(|e| {
                let trimmed = e.trim();
                format!("'{}'", trimmed.replace('\'', "''"))
            })
            .collect(),
    )
}

fn parse_in_list(toks: &[Tok], start: usize) -> Result<(Vec<String>, usize), String> {
    if toks.get(start) != Some(&Tok::LPar) {
        return Err("IN must be followed by a parenthesised value list".to_string());
    }

    let mut values = Vec::new();
    let mut i = start + 1;

    loop {
        match toks.get(i) {
            None => return Err("unterminated IN value list".to_string()),
            Some(Tok::RPar) => {
                i += 1;
                break;
            }
            Some(Tok::Comma) => {
                i += 1;
            }
            Some(Tok::LPar) => {
                return Err("subqueries inside IN are not supported yet".to_string());
            }
            // A signed number is one value. The sign lexes as its own symbol,
            // so `x IN (-1, 2)` used to become `x = - OR x = 1 OR x = 2` and
            // fail to plan.
            Some(Tok::Sym(sign)) if (sign == "-" || sign == "+")
                && matches!(toks.get(i + 1), Some(Tok::Word(w)) if w.bytes().next().is_some_and(|b| b.is_ascii_digit())) =>
            {
                let Some(Tok::Word(number)) = toks.get(i + 1) else {
                    unreachable!("matched above");
                };
                values.push(if sign == "-" {
                    format!("-{}", number)
                } else {
                    number.clone()
                });
                i += 2;
            }
            Some(t) => {
                if t.is_keyword("select") {
                    return Err("subqueries inside IN are not supported yet".to_string());
                }
                values.push(render_value(t));
                i += 1;
            }
        }
    }

    Ok((values, i))
}

#[cfg(test)]
mod tests {

    /// A float constant against an exact column is the run of the column's
    /// values whose nearest double it is (PostgreSQL's `numeric = float8`).
    #[test]
    fn a_float_against_an_exact_column_is_the_run_that_rounds_to_it() {
        let mut schema: SmallVec<[crate::core::row::schema::SchemaField; 20]> = SmallVec::new();
        schema.push(crate::core::row::schema::SchemaField::new(
            "i".into(),
            crate::core::db_type::DbType::I64,
            8,
            0,
            0,
            false,
        ));
        let mut n = crate::core::row::schema::SchemaField::new(
            "n".into(),
            crate::core::db_type::DbType::DECIMAL,
            16,
            8,
            1,
            false,
        );
        n.scale = Some(2);
        schema.push(n);
        let squash = |s: String| s.split_whitespace().collect::<Vec<_>>().join(" ");
        for (sql, want) in [
            // 2^53 + 1 rounds (to even) to 2^53; 2^53 + 2 is its own double.
            ("i = 9007199254740992::float8", "( i >= 9007199254740992 AND i <= 9007199254740993 )"),
            ("i <> 9007199254740992::float8", "( i < 9007199254740992 OR i > 9007199254740993 )"),
            ("i = 2.5::float8", "( i >= 3 AND i <= 2 )"),
            ("i <= 2.5::float8", "( i <= 2 )"),
            ("i < 3::float8", "( i < 3 )"),
            ("i > 2.5::float8", "( i > 2 )"),
            ("i >= 2.5::float8", "( i >= 3 )"),
            ("2.5::float8 < i", "( i > 2 )"),
            ("n = '0.1'::float8", "( n = 0.10 )"),
            ("n = '0.1'::real", "( n >= 0.11 AND n <= 0.10 )"),
        ] {
            let out = squash(rewrite_where(sql, &schema).unwrap_or_else(|e| panic!("{sql}: {e}")));
            assert!(out.contains(want), "{sql} -> {out}");
        }
        // An operand of more than the comparison is left to the evaluator.
        let out = squash(rewrite_where("i * 2 = 5::float8", &schema).unwrap());
        assert!(!out.contains(">="), "{out}");
        let lowered = |sql: &str| {
            exact_columns_against_floats(lex(sql).unwrap(), &schema, false)
                .iter()
                .map(Tok::render)
                .collect::<Vec<_>>()
                .join(" ")
        };
        for sql in ["n + i = 5::float8", "i = 2.5::float8 * 2", "i = 5::float8 - n"] {
            let out = lowered(sql);
            assert!(!out.contains(">=") && !out.contains("<="), "{sql} -> {out}");
        }
        assert!(lowered("n = 1 AND i = 5::float8").contains("( i = 5 )"));
        for (sql, want) in [
            ("n IN ('0.5'::float8, '1e40'::float8)", "( n IN ( 0.50 ) )"),
            // Single-value runs are an ordinary IN list, a NULL kept in it;
            // a run of many values is a range.
            ("n IN ('0.1'::float8, NULL, '2'::float8)", "( n IN ( 0.10 , 2.00 , NULL ) )"),
            ("n NOT IN ('2'::float8, '0.1'::float8)", "( n NOT IN ( 0.10 , 2.00 ) )"),
            ("n IN ('0.125'::float8)", "( n > 999999999999999999999999999999999999.99 )"),
            (
                "i IN (9007199254740992::float8, 9007199254740992::float8, 9007199254740994::float8, NULL)",
                "( i IN ( 9007199254740994 , NULL ) OR ( i >= 9007199254740992 AND i <= 9007199254740993 ) )",
            ),
            (
                "i NOT IN (9007199254740994::float8, 9007199254740992::float8)",
                "( i NOT IN ( 9007199254740994 ) AND ( i < 9007199254740992 OR i > 9007199254740993 ) )",
            ),
            ("n NOT IN ('-1e40'::float8)", "( n <= 999999999999999999999999999999999999.99 )"),
        ] {
            let out = lowered(sql);
            assert!(out.contains(want), "{sql} -> {out}");
        }
        // For the scan, a float finer than the column there is compared as
        // the column's double, one lifted operand the scan hashes the floats
        // against; the coarse ones stay an IN list.
        let scanned = |sql: &str| exact_columns_against_floats(lex(sql).unwrap(), &schema, true);
        let toks = scanned("i IN (9007199254740992::float8, 9007199254740994::float8, 5::float8, NULL)");
        let rendered = toks.iter().map(Tok::render).collect::<Vec<_>>().join(" ");
        let lifted = toks.iter().filter(|t| matches!(t, Tok::Lifted(_))).count();
        assert_eq!(lifted, 2, "{rendered}");
        assert!(rendered.contains("i IN ( 5 , NULL )") && !rendered.contains(">="), "{rendered}");
        assert!(rendered.contains("= 9.007199254740992e+15 OR") && rendered.contains("= 9.007199254740994e+15 )"), "{rendered}");
        // The scan's own entry asks for it; the CHECK's does not.
        let clause = "i IN (9007199254740992::float8, 9007199254740994::float8)";
        assert!(rewrite_scan_where(clause, &schema).unwrap().as_bytes().contains(&LIFTED_OPEN), "scan");
        assert!(!rewrite_where(clause, &schema).unwrap().as_bytes().contains(&LIFTED_OPEN), "check");
        let negated = scanned("i NOT IN (9007199254740992::float8, 9007199254740994::float8)");
        let rendered = negated.iter().map(Tok::render).collect::<Vec<_>>().join(" ");
        assert!(rendered.contains("<> 9.007199254740992e+15 AND") && !rendered.contains(" IN "), "{rendered}");
    }

    /// The one-conversion run agrees with the search wherever it answers:
    /// on values, midpoints and neighbours of the column's steps.
    #[test]
    fn the_coarse_run_agrees_with_the_search() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let (lo, hi) = (-(10i128.pow(38) - 1), 10i128.pow(38) - 1);
        let mut answered = 0;
        for _ in 0..20_000 {
            let scale = (next() % 7) as u32;
            let magnitude = 10f64.powi((next() % 16) as i32 - 3);
            let grid = 10f64.powi(-(scale as i32));
            let base = ((next() % 1_000_000) as f64 / 1_000_000.0 * magnitude / grid).round() * grid;
            let v = match next() % 5 {
                0 => base,
                1 => base + grid / 2.0,
                2 => f64::from_bits(base.to_bits() + 1),
                3 => f64::from_bits(base.to_bits().saturating_sub(1)),
                _ => base + grid * 0.37,
            };
            let v = if next() % 2 == 0 { v } else { -v };
            if let Some(run) = float_run_coarse(scale, lo, hi, v) {
                answered += 1;
                assert_eq!(run, float_run_search(scale, lo, hi, v), "v = {v:e}, scale {scale}");
            }
        }
        assert!(answered > 10_000, "only {answered} answered");
    }

    #[test]
    fn between_takes_negative_bounds() {
        let mut schema: SmallVec<[crate::core::row::schema::SchemaField; 20]> = SmallVec::new();
        schema.push(crate::core::row::schema::SchemaField::new(
            "n".into(),
            crate::core::db_type::DbType::F64,
            8,
            0,
            0,
            false,
        ));
        for (sql, lo, hi) in [
            ("n BETWEEN -3 AND -3", "-3", "-3"),
            ("n BETWEEN -10 AND 5", "-10", "5"),
            ("n BETWEEN +2 AND 7.5", "2", "7.5"),
            ("n BETWEEN -1.5 AND -0.5", "-1.5", "-0.5"),
        ] {
            let out = rewrite_where(sql, &schema).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(out.contains(&format!(">= {lo}")) && out.contains(&format!("<= {hi}")), "{sql} -> {out}");
        }
    }

    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = SmallVec::new();
        for (i, (name, ty)) in [
            ("id", DbType::I32),
            ("name", DbType::STRING),
            ("country", DbType::STRING),
            ("age", DbType::I32),
            ("active", DbType::BOOL),
        ]
        .into_iter()
        .enumerate()
        {
            s.push(SchemaField::new(
                name.to_string(),
                ty.clone(),
                ty.get_size(),
                0,
                i as u64,
                false,
            ));
        }
        s
    }

    fn rw(clause: &str) -> String {
        rewrite_where(clause, &schema()).unwrap()
    }

    fn err(clause: &str) -> String {
        rewrite_where(clause, &schema()).unwrap_err()
    }

    #[test]
    fn plain_comparisons_pass_through() {
        assert_eq!(rw("id = 5"), "id = 5");
        assert_eq!(
            rw("age >= 18 AND name = 'bob'"),
            "age >= 18 AND name = 'bob'"
        );
    }

    /// A cast after a column is emitted once. The cast used to be carried on
    /// the column's name and emitted again after it, `id::text ::text = '5'`,
    /// which the planner refused.
    #[test]
    fn a_cast_on_a_column_is_emitted_once() {
        assert_eq!(rw("id::text = '5'"), "id ::text = '5'");
        assert_eq!(rw("age::bigint > 6 AND id = 1"), "age ::bigint > 6 AND id = 1");
        assert_eq!(rw("name::text LIKE 'a%'"), "name::text STARTSWITH 'a'");
    }

    #[test]
    fn in_becomes_a_disjunction_of_equalities() {
        assert_eq!(
            rw("country IN ('BG', 'DE')"),
            "(country = 'BG' OR country = 'DE')"
        );
        assert_eq!(rw("id IN (1)"), "(id = 1)");
    }

    #[test]
    fn not_in_becomes_a_conjunction_of_inequalities() {
        assert_eq!(rw("country NOT IN ('DE')"), "(country != 'DE')");
        assert_eq!(rw("id NOT IN (1, 2)"), "(id != 1 AND id != 2)");
    }

    #[test]
    fn between_becomes_two_bounds() {
        assert_eq!(rw("age BETWEEN 18 AND 65"), "(age >= 18 AND age <= 65)");
    }

    /// The complement of a range is a disjunction, not a conjunction — getting
    /// this backwards would silently return no rows.
    #[test]
    fn not_between_is_the_complement_not_the_negated_bounds() {
        assert_eq!(rw("age NOT BETWEEN 18 AND 65"), "(age < 18 OR age > 65)");
    }

    #[test]
    fn like_maps_each_wildcard_shape_to_its_operator() {
        assert_eq!(rw("name LIKE 'ab%'"), "name STARTSWITH 'ab'");
        assert_eq!(rw("name LIKE '%ab'"), "name ENDSWITH 'ab'");
        assert_eq!(rw("name LIKE '%ab%'"), "name CONTAINS 'ab'");
        // No wildcard: LIKE is just equality.
        assert_eq!(rw("name LIKE 'ab'"), "name = 'ab'");
    }

    /// `ILIKE` lowers to the case-insensitive counterparts, which are separate
    /// operators so the fingerprint index — which hashes raw bytes — declines
    /// them instead of silently dropping rows whose case differs.
    #[test]
    fn ilike_lowers_to_case_insensitive_operators() {
        assert_eq!(rw("name ILIKE '%ab%'"), "name ICONTAINS 'ab'");
        assert_eq!(rw("name ILIKE 'ab%'"), "name ISTARTSWITH 'ab'");
        assert_eq!(rw("name ILIKE '%ab'"), "name IENDSWITH 'ab'");
        assert_eq!(rw("name ILIKE 'ab'"), "name IEQUALS 'ab'");
        // LIKE stays case-sensitive.
        assert_eq!(rw("name LIKE '%ab%'"), "name CONTAINS 'ab'");
    }

    #[test]
    fn like_escapes_are_honoured_when_choosing_the_operator() {
        // Django's `contains`: every `_` and `%` in the value escaped.
        assert_eq!(rw("name LIKE '%a\\_b%'"), "name CONTAINS 'a_b'");
        assert_eq!(rw("name LIKE '%50\\%%'"), "name CONTAINS '50%'");
        assert_eq!(rw("name LIKE '50\\%'"), "name = '50%'");
        assert_eq!(rw("name LIKE 'a\\%b%'"), "name STARTSWITH 'a%b'");
        assert_eq!(rw("name LIKE '%a\\\\b'"), "name ENDSWITH 'a\\b'");
        assert_eq!(rw("name ILIKE '%a\\_b%'"), "name ICONTAINS 'a_b'");
        assert_eq!(rw("name LIKE 'it''s%'"), "name STARTSWITH 'it''s'");
        // An interior wildcard keeps the pattern, escapes and all.
        assert_eq!(rw("name LIKE 'a\\_%b'"), "name LIKE 'a\\_%b'");
        // ESCAPE picks another escape character, or none.
        assert_eq!(rw("name LIKE 'a!_b%' ESCAPE '!'"), "name STARTSWITH 'a_b'");
        assert_eq!(rw("name LIKE 'a!_%b' ESCAPE '!'"), "name LIKE 'a\\_%b'");
        assert_eq!(rw("name LIKE 'a\\b%' ESCAPE '!'"), "name STARTSWITH 'a\\b'");
        assert_eq!(rw("name LIKE 'a\\_%' ESCAPE ''"), "name LIKE 'a\\\\_%'");
        // An E'' string has its backslash escapes resolved first.
        assert_eq!(rw("name LIKE E'%a\\\\_b%'"), "name CONTAINS 'a_b'");
        assert_eq!(rw("name LIKE E'it\\'s%'"), "name STARTSWITH 'it''s'");
    }

    #[test]
    fn a_like_pattern_ending_in_its_escape_is_refused() {
        assert!(err("name LIKE 'ab\\'").contains("must not end with escape character"));
        assert!(err("name LIKE 'ab!' ESCAPE '!'").contains("must not end with escape character"));
        assert!(err("name LIKE 'ab' ESCAPE 'xy'").contains("invalid escape string"));
    }

    #[test]
    fn like_patterns_the_simple_operators_cannot_express_become_a_pattern_test() {
        // These used to be refused, and refusing was right at the time: `a%b` is
        // not `STARTSWITH 'a' AND CONTAINS 'b'`, which does not require `b` to
        // follow `a`. Now there is a matcher behind them, so they rewrite to a
        // `LIKE` carrying the pattern with its wildcards intact.
        assert_eq!(rw("name LIKE 'a%b'"), "name LIKE 'a%b'");
        assert_eq!(rw("name LIKE 'a_b'"), "name LIKE 'a_b'");
        assert_eq!(rw("name ILIKE 'a%b'"), "name ILIKE 'a%b'");

        // The shapes that *can* be reduced still are, so the cheaper operators
        // — and `STARTSWITH`'s range lowering in the filter — are not lost.
        assert_eq!(rw("name LIKE 'abc%'"), "name STARTSWITH 'abc'");
        assert_eq!(rw("name LIKE '%abc'"), "name ENDSWITH 'abc'");
        assert_eq!(rw("name LIKE '%abc%'"), "name CONTAINS 'abc'");
        assert_eq!(rw("name LIKE 'abc'"), "name = 'abc'");
    }

    #[test]
    fn qualified_and_quoted_column_names_resolve() {
        assert_eq!(rw("users.name = 'bob'"), "name = 'bob'");
        assert_eq!(rw("u.name = 'bob'"), "name = 'bob'");
        assert_eq!(rw("\"name\" = 'bob'"), "name = 'bob'");
    }

    /// Postgres folds unquoted identifiers to lower case, so a client may send
    /// any casing for a column the schema stores in one canonical form.
    #[test]
    fn column_names_resolve_case_insensitively() {
        assert_eq!(rw("NAME = 'bob'"), "name = 'bob'");
        assert_eq!(rw("Country IN ('BG')"), "(country = 'BG')");
    }

    #[test]
    fn boolean_literals_fold_to_stored_form() {
        assert_eq!(rw("active = true"), "active = 1");
        assert_eq!(rw("active = FALSE"), "active = 0");
    }

    #[test]
    fn string_literals_keep_their_contents() {
        assert_eq!(rw("name = 'O''Brien'"), "name = 'O''Brien'");
        // A keyword inside a literal is data, not syntax.
        assert_eq!(rw("name = 'BETWEEN'"), "name = 'BETWEEN'");
        assert_eq!(rw("name = 'a IN b'"), "name = 'a IN b'");
    }

    #[test]
    fn parentheses_and_grouping_survive() {
        assert_eq!(
            rw("(country = 'BG' OR country = 'DE') AND active = true"),
            "( country = 'BG' OR country = 'DE' ) AND active = 1"
        );
    }

    #[test]
    fn sugar_forms_compose_with_boolean_operators() {
        assert_eq!(
            rw("country IN ('BG', 'DE') AND age BETWEEN 18 AND 65"),
            "(country = 'BG' OR country = 'DE') AND (age >= 18 AND age <= 65)"
        );
    }

    #[test]
    fn a_bare_boolean_column_becomes_an_equality() {
        assert_eq!(rw("active"), "active = 1");
        assert_eq!(rw("NOT active"), "active = 0");
        assert_eq!(rw("active AND age > 3"), "active = 1 AND age > 3");
    }

    /// The tokenizer downstream panics on an identifier it cannot find, which
    /// takes the connection with it. Every such name has to be caught here.
    #[test]
    fn unknown_identifiers_are_rejected_rather_than_passed_on() {
        assert!(err("nosuchcolumn = 1").contains("column \"nosuchcolumn\" does not exist"));
        assert!(err("EXISTS (SELECT 1)").contains("unknown column"));
        assert!(err("NOT EXISTS (SELECT 1)").contains("NOT over"));
    }

    #[test]
    fn unsupported_forms_name_the_missing_feature() {
        assert!(err("id IN (SELECT id FROM t)").contains("subqueries"));
    }

    /// `NOT` over a group is pushed down to the comparisons, each written as
    /// the comparison that holds where it is false. These used to be refused
    /// with "NOT is only supported directly before a boolean column", and
    /// `NOT LIKE` with "not supported yet"; PostgreSQL 17 answers all of them
    /// (`jsonb_pg_semantics.rs` holds its answers).
    #[test]
    fn not_is_pushed_down_to_the_comparisons() {
        assert_eq!(rw("NOT (id = 1 AND age = 2)"), "( id != 1 OR age != 2 )");
        assert_eq!(rw("NOT id = 1 AND age = 2"), "id != 1 AND age = 2");
        assert_eq!(rw("NOT (id = 1 OR NOT age < 2)"), "( id != 1 AND age < 2 )");
        assert_eq!(rw("NOT NOT (id = 1)"), "id = 1");
        assert_eq!(rw("NOT (id IN (1, 2))"), "(id != 1 AND id != 2)");
        assert_eq!(rw("NOT (id NOT IN (1, 2))"), "(id = 1 OR id = 2)");
        assert_eq!(rw("NOT (age BETWEEN 1 AND 3) OR id = 1"), "(age < 1 OR age > 3) OR id = 1");
        assert_eq!(rw("NOT (id + 1 > 2)"), "id + 1 <= 2");
        assert_eq!(rw("(NOT (id = 1)) OR id = 3"), "( id != 1 ) OR id = 3");
        // No complement: the evaluator negates the one comparison.
        assert_eq!(rw("NOT (name LIKE 'a%')"), "NOT name STARTSWITH 'a'");
        assert_eq!(rw("name NOT LIKE 'a%'"), "NOT name STARTSWITH 'a'");
        assert_eq!(rw("NOT (name NOT LIKE 'a%')"), "name STARTSWITH 'a'");
        // `= ANY` and `<> ALL` trade places; flipping the operator alone would
        // write `<> ANY`, which means something else.
        assert_eq!(rw("NOT (id = ANY('{1,2}'))"), rw("id <> ALL('{1,2}')"));
        assert!(err("NOT (id <> ANY('{1,2}'))").contains("NOT over"));
    }

    /// NOT of NULL is NULL, so a NULL row is in neither `x` nor `NOT x`: each
    /// negated comparison keeps its column's guard outside the negation.
    #[test]
    fn not_keeps_a_null_row_out_of_both_answers() {
        let flag = nullable::flag_column_name("country");
        assert_eq!(rwn("NOT (country = 'x')"), format!("({flag} = 0 AND country != 'x')"));
        assert_eq!(
            rwn("NOT (country LIKE 'x%')"),
            format!("( {flag} = 0 AND NOT country STARTSWITH 'x' )")
        );
        assert_eq!(rwn("country NOT LIKE 'x%'"), format!("({flag} = 0 AND NOT country STARTSWITH 'x')"));
        assert_eq!(rwn("NOT (country IS NULL)"), format!("{flag} = 0"));
        assert_eq!(rwn("country NOTNULL"), format!("{flag} = 0"));
    }

    fn document_schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = schema();
        s.push(SchemaField::new("doc".to_string(), DbType::JSONB, 0, 0, s.len() as u64, false));
        s.push(SchemaField::new(
            nullable::flag_column_name("doc"),
            DbType::BOOL,
            1,
            0,
            s.len() as u64,
            false,
        ));
        s
    }

    /// `IN`, `IS NULL`, `BETWEEN` and `NOT LIKE` over a JSON path read the
    /// whole path as their operand; they used to see only `doc` and leave the
    /// keyword for the engine to refuse.
    #[test]
    fn sugar_over_a_json_path_reads_the_whole_path() {
        let rd = |clause: &str| rewrite_where(clause, &document_schema()).unwrap();
        let flag = nullable::flag_column_name("doc");
        assert_eq!(
            rd("doc->>'a' IN ('x','y')"),
            format!("({flag} = 0 AND (doc ->> 'a' = 'x' OR doc ->> 'a' = 'y'))")
        );
        assert_eq!(rd("(doc->>'a') NOT IN ('x')"), format!("({flag} = 0 AND (doc ->> 'a' != 'x'))"));
        assert_eq!(rd("doc->>'a' NOT IN ('x', NULL)"), CONSTANT_FALSE);
        assert_eq!(rd("doc->>'a' IN ()"), CONSTANT_FALSE);
        assert_eq!(rd("doc->'b'->>'c' IS NULL"), format!("({flag} = 1 OR doc -> 'b' ->> 'c' ISNULL)"));
        assert_eq!(rd("doc #>> '{b,c}' NOTNULL"), format!("({flag} = 0 AND doc #>> '{{b,c}}' NOTNULL)"));
        assert_eq!(
            rd("doc->>'a' BETWEEN 'a' AND 'x'"),
            format!("({flag} = 0 AND (doc ->> 'a' >= 'a' AND doc ->> 'a' <= 'x'))")
        );
        assert_eq!(rd("doc->>'a' NOT LIKE 'x%'"), format!("({flag} = 0 AND NOT doc ->> 'a' LIKE 'x%')"));
        assert_eq!(rd("NOT (doc ? 'k')"), format!("( {flag} = 0 AND NOT doc ? 'k' )"));
        assert_eq!(rd("NOT (doc->>'a' IS NULL)"), format!("({flag} = 0 AND doc ->> 'a' NOTNULL)"));
        // A path on the right of the operator stays inside the guard.
        assert_eq!(rd("'1' = doc->'n'"), format!("({flag} = 0 AND '1' = doc -> 'n')"));
    }

    #[test]
    fn a_comparison_against_null_matches_nothing_rather_than_failing() {
        // This used to be refused with a message about NULL literals, on the
        // grounds that the author probably meant `IS NULL`. PostgreSQL does not
        // refuse it — `x = NULL` is simply never true — and the difference is
        // not academic: a claim query of the form
        // `WHERE id = (SELECT … LIMIT 1)` substitutes NULL when the subquery
        // finds nothing, so refusing turned "no work to claim" into a hard
        // error on every worker tick.
        assert_eq!(rw("country = NULL"), "1 = 0");
        assert_eq!(rw("country <> NULL"), "1 = 0");
        // The whole comparison goes, not just its right-hand side.
        assert_eq!(rw("id = 5 AND country = NULL"), "id = 5 AND 1 = 0");
        // `IS NULL` asks a different question and must not be folded away —
        // on a nullable column. (On a NOT NULL one it *is* the constant.)
        assert_ne!(
            rewrite_where("country IS NULL", &nullable_schema()).unwrap(),
            "1 = 0"
        );
    }

    /// A schema where `country` is nullable and `id` is not.
    fn nullable_schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = schema();
        s.push(SchemaField::new(
            nullable::flag_column_name("country"),
            DbType::BOOL,
            1,
            0,
            s.len() as u64,
            false,
        ));
        s
    }

    fn rwn(clause: &str) -> String {
        rewrite_where(clause, &nullable_schema()).unwrap()
    }

    #[test]
    fn non_ascii_literals_pass_through_byte_for_byte() {
        assert_eq!(rw("name = 'Müller'"), "name = 'Müller'");
        assert_eq!(
            rw("name = '日本語' AND id = 1"),
            "name = '日本語' AND id = 1"
        );
        assert_eq!(rw("name != 'café'"), "name != 'café'");
        assert_eq!(
            rwn("country = 'Ελλάδα'"),
            "(__null_country = 0 AND country = 'Ελλάδα')"
        );
    }

    #[test]
    fn a_column_on_the_right_of_an_operator_is_an_operand_not_a_truth_test() {
        assert_eq!(rw("5 = age"), "5 = age");
        assert_eq!(rw("18 <= age AND active"), "18 <= age AND active = 1");
        assert_eq!(rw("id = age"), "id = age");
        assert_eq!(rw("(5 = age)"), "( 5 = age )");
    }

    #[test]
    fn a_nullable_column_on_the_right_is_guarded_like_one_on_the_left() {
        assert_eq!(
            rwn("'BG' = country"),
            "(__null_country = 0 AND 'BG' = country)"
        );
        assert_eq!(
            rwn("id = 1 AND 'BG' = country"),
            "id = 1 AND (__null_country = 0 AND 'BG' = country)"
        );
        assert_eq!(
            rwn("name = country"),
            "(__null_country = 0 AND name = country)"
        );
    }

    #[test]
    fn is_null_tests_the_companion_column() {
        assert_eq!(rwn("country IS NULL"), "__null_country = 1");
        assert_eq!(rwn("country IS NOT NULL"), "__null_country = 0");
    }

    /// A `NOT NULL` column can never be `NULL`, so the test is a constant. It
    /// must still be expressible, because an ORM emits `IS NULL` without
    /// knowing the constraint.
    #[test]
    fn is_null_on_a_non_nullable_column_is_a_constant() {
        assert_eq!(rw("id IS NULL"), CONSTANT_FALSE);
        assert_eq!(rw("id IS NOT NULL"), CONSTANT_TRUE);
    }

    /// The guard is what stops `country = ''` matching a `NULL` row: the bytes
    /// stored for a `NULL` string are also empty.
    #[test]
    fn comparisons_against_a_nullable_column_are_guarded() {
        assert_eq!(
            rwn("country = 'BG'"),
            "(__null_country = 0 AND country = 'BG')"
        );
        assert_eq!(
            rwn("country != 'BG'"),
            "(__null_country = 0 AND country != 'BG')"
        );
        assert_eq!(rwn("country = ''"), "(__null_country = 0 AND country = '')");
    }

    #[test]
    fn a_non_nullable_column_is_not_guarded() {
        assert_eq!(rwn("id = 5"), "id = 5");
    }

    #[test]
    fn the_guard_survives_composition_with_and_or_and_groups() {
        assert_eq!(
            rwn("country = 'BG' AND id = 5"),
            "(__null_country = 0 AND country = 'BG') AND id = 5"
        );
        assert_eq!(
            rwn("id = 5 OR country = 'DE'"),
            "id = 5 OR (__null_country = 0 AND country = 'DE')"
        );
        assert_eq!(
            rwn("(country = 'BG' OR id = 5) AND age > 1"),
            "( (__null_country = 0 AND country = 'BG') OR id = 5 ) AND age > 1"
        );
    }

    #[test]
    fn the_sugar_forms_are_guarded_too() {
        assert_eq!(
            rwn("country IN ('BG', 'DE')"),
            "(__null_country = 0 AND (country = 'BG' OR country = 'DE'))"
        );
        assert_eq!(
            rwn("country NOT IN ('DE')"),
            "(__null_country = 0 AND (country != 'DE'))"
        );
        assert_eq!(
            rwn("country LIKE 'B%'"),
            "(__null_country = 0 AND country STARTSWITH 'B')"
        );
    }

    /// `NOT IN` is where the guard changes the answer: SQL excludes a row whose
    /// column is `NULL`, because `NULL != 'DE'` is unknown rather than true.
    #[test]
    fn not_in_excludes_null_rows() {
        let rewritten = rwn("country NOT IN ('DE')");
        assert!(
            rewritten.starts_with("(__null_country = 0 AND"),
            "a NULL row must not satisfy NOT IN: {}",
            rewritten
        );
    }

    #[test]
    fn malformed_input_is_an_error_not_a_panic() {
        assert!(rewrite_where("name = 'unterminated", &schema()).is_err());
        assert!(rewrite_where("id IN (", &schema()).is_err());
        assert!(rewrite_where("age BETWEEN 1", &schema()).is_err());
        assert!(rewrite_where("age BETWEEN 1 OR 2", &schema()).is_err());
    }

    /// An empty list is what a subquery returning nothing leaves behind. It has
    /// to become a predicate that cannot hold, not a syntax error and not a
    /// predicate that matches everything.
    #[test]
    fn an_empty_in_list_can_never_hold() {
        assert_eq!(rw("id IN ()"), CONSTANT_FALSE);
        assert_eq!(rw("id NOT IN ()"), CONSTANT_TRUE);
    }

    #[test]
    fn empty_clause_stays_empty() {
        assert_eq!(rw(""), "");
        assert_eq!(rw("   "), "");
    }

    /// The `= NULL` fold decides on the four bytes after the operator and the
    /// one after those. Uppercasing the whole remainder to decide it — which is
    /// what this did — made the rewrite quadratic in the number of comparisons,
    /// so these pin the shapes the cheap test has to keep accepting, and the
    /// near-misses it must keep refusing.
    #[test]
    fn the_null_fold_accepts_exactly_end_space_and_close_paren() {
        // `NULL` at the end of the clause, before a space, before a `)`.
        assert_eq!(rw("country = NULL"), "1 = 0");
        assert_eq!(rw("country = NULL AND id = 1"), "1 = 0 AND id = 1");
        assert_eq!(rw("(country = NULL) OR id = 1"), "( 1 = 0 ) OR id = 1");
        // Any casing, either inequality spelling, and no space required.
        assert_eq!(rw("country = nUlL"), "1 = 0");
        assert_eq!(rw("country <> NULL"), "1 = 0");
        assert_eq!(rw("country != null"), "1 = 0");
        assert_eq!(rw("country =NULL"), "1 = 0");

        // A longer identifier is not `NULL` and must not be folded away.
        assert!(err("country = NULLX").contains("column \"NULLX\" does not exist"));
        // Too short to be `NULL` at all.
        assert!(err("country = NU").contains("column \"NU\" does not exist"));
        // A tab or newline after `NULL` was never accepted either — only a
        // literal space was — and the statement is refused rather than folded.
        // Preserved deliberately: this is behaviour, not an accident of how the
        // check was written.
        assert!(err("country = NULL\tAND id = 1").contains("NULL literals"));
        assert!(err("country = NULL\nAND id = 1").contains("NULL literals"));
    }
}

#[cfg(test)]
mod any_all_tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s: SmallVec<[SchemaField; 20]> = SmallVec::new();
        for (i, (name, ty)) in [("id", DbType::STRING), ("age", DbType::I32)]
            .into_iter()
            .enumerate()
        {
            s.push(SchemaField::new(
                name.to_string(),
                ty,
                12,
                0,
                i as u64,
                false,
            ));
        }
        s
    }

    /// A PostgreSQL client writes `IN` as `= ANY(array)` whenever the list is
    /// bound rather than written out — sea-orm does it for every batched fetch
    /// by id, so this is an ordinary application's hot read path.
    #[test]
    fn equals_any_becomes_the_same_chain_as_in() {
        let out = rewrite_where("id = ANY('{\"a\",\"b\"}')", &schema()).expect("rewrites");
        assert!(out.contains("id = 'a'"), "{}", out);
        assert!(out.contains("id = 'b'"), "{}", out);
        assert!(out.contains(" OR "), "{}", out);
    }

    #[test]
    fn unquoted_and_spaced_elements_are_read() {
        let out = rewrite_where("id = ANY('{a,b c}')", &schema()).expect("rewrites");
        assert!(out.contains("id = 'a'"), "{}", out);
        assert!(out.contains("id = 'b c'"), "{}", out);
    }

    #[test]
    fn not_equals_all_is_the_negation() {
        let out = rewrite_where("id <> ALL('{\"a\",\"b\"}')", &schema()).expect("rewrites");
        assert!(out.contains("id != 'a'"), "{}", out);
        assert!(out.contains("id != 'b'"), "{}", out);
        assert!(out.contains(" AND "), "{}", out);
    }

    /// An empty array holds for nothing under `= ANY` and for everything under
    /// `<> ALL`, and neither needs a constant of the column's type.
    #[test]
    fn an_empty_array_is_expressed_without_a_typed_constant() {
        let out = rewrite_where("id = ANY('{}')", &schema()).expect("rewrites");
        assert_eq!(out, CONSTANT_FALSE);
        let out = rewrite_where("id <> ALL('{}')", &schema()).expect("rewrites");
        assert_eq!(out, CONSTANT_TRUE);
    }

    /// `<> ANY` and `= ALL` are legal SQL meaning something else entirely.
    /// Guessing at them would answer a question nobody asked, so they are left
    /// for the parser to refuse.
    #[test]
    fn the_quantifiers_without_an_exact_reading_are_left_alone() {
        for clause in ["id <> ANY('{\"a\"}')", "id = ALL('{\"a\"}')"] {
            let out = rewrite_where(clause, &schema());
            if let Ok(text) = out {
                assert!(
                    !text.contains(" OR ") && !text.contains(" AND id !="),
                    "{} should not have been expanded: {}",
                    clause,
                    text
                );
            }
        }
    }

    /// A bound array parameter has no elements to expand yet.
    #[test]
    fn a_bound_parameter_is_not_rewritten() {
        let out = rewrite_where("id = ANY($1)", &schema());
        if let Ok(text) = out {
            assert!(!text.contains(" OR "), "{}", text);
        }
    }

    #[test]
    fn a_uuid_batch_expands() {
        let clause = "id = ANY('{\"8bf0ea43-f179-41c6-ae8f-4a1354be8bdf\",\
                     \"1bb0afa7-1562-4f76-b200-81ce381abd10\"}')";
        let out = rewrite_where(clause, &schema()).expect("rewrites");
        assert!(
            out.contains("id = '8bf0ea43-f179-41c6-ae8f-4a1354be8bdf'"),
            "{}",
            out
        );
        assert!(
            out.contains("id = '1bb0afa7-1562-4f76-b200-81ce381abd10'"),
            "{}",
            out
        );
    }
}

#[cfg(test)]
mod operand_guard_tests {
    use super::*;
    use crate::core::db_type::DbType;

    /// `x` and `y` are nullable, `z` is not.
    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s: SmallVec<[SchemaField; 20]> = SmallVec::new();
        for (i, (name, ty)) in [
            ("id", DbType::I32),
            ("x", DbType::I32),
            ("__null_x", DbType::BOOL),
            ("y", DbType::I32),
            ("__null_y", DbType::BOOL),
            ("z", DbType::I32),
        ]
        .into_iter()
        .enumerate()
        {
            s.push(SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, i as u64, false));
        }
        s
    }

    fn rw(clause: &str) -> String {
        rewrite_where(clause, &schema()).unwrap_or_else(|e| panic!("{clause}: {e}"))
    }

    /// Every nullable column a comparison reads is guarded, not only the one
    /// it starts with. With only `x` guarded, a NULL `y` read as its zeroes,
    /// so `x <> y` and `NOT (x = y)` kept rows PostgreSQL drops.
    #[test]
    fn a_nullable_column_on_the_right_is_guarded_too() {
        assert_eq!(rw("x = y"), "(__null_x = 0 AND (__null_y = 0 AND x = y))");
        assert_eq!(rw("NOT (x = y)"), "(__null_x = 0 AND (__null_y = 0 AND x != y))");
        assert_eq!(rw("x + 1 > y"), "(__null_x = 0 AND (__null_y = 0 AND x + 1 > y))");
        assert_eq!(rw("z = y"), "(__null_y = 0 AND z = y)");
        assert_eq!(rw("z + 1 > y"), "__null_y = 0 AND z + 1 > y");
        assert_eq!(rw("x = y OR id = 1"), "(__null_x = 0 AND (__null_y = 0 AND x = y)) OR id = 1");
    }

    /// A call can turn a NULL into a value, so a column inside one gets no
    /// guard of its own.
    #[test]
    fn a_column_inside_a_call_is_not_guarded() {
        assert_eq!(rw("x = COALESCE(y, 0)"), "(__null_x = 0 AND x = COALESCE ( y , 0 ))");
    }

    /// In a list or a range, each comparison is guarded by its own operand.
    #[test]
    fn list_and_range_operands_guard_their_own_comparison() {
        assert_eq!(rw("x IN (y, z)"), "(__null_x = 0 AND ((__null_y = 0 AND x = y) OR x = z))");
        assert_eq!(rw("x NOT IN (y, 1)"), "(__null_x = 0 AND ((__null_y = 0 AND x != y) AND x != 1))");
        assert_eq!(rw("z BETWEEN x AND y"), "((__null_x = 0 AND z >= x) AND (__null_y = 0 AND z <= y))");
        assert_eq!(rw("NOT (z BETWEEN x AND y)"), "((__null_x = 0 AND z < x) OR (__null_y = 0 AND z > y))");
    }

    /// `IS [NOT] DISTINCT FROM` a nullable column compares the two NULLs.
    #[test]
    fn distinct_from_a_nullable_column_reads_its_null() {
        assert_eq!(
            rw("x IS DISTINCT FROM y"),
            "((__null_x = 1 AND __null_y = 0) OR (__null_x = 0 AND __null_y = 1) \
             OR (__null_x = 0 AND __null_y = 0 AND x != y))"
        );
        assert_eq!(
            rw("x IS NOT DISTINCT FROM y"),
            "((__null_x = 1 AND __null_y = 1) OR (__null_x = 0 AND __null_y = 0 AND x = y))"
        );
        assert_eq!(rw("z IS NOT DISTINCT FROM y"), "(__null_y = 0 AND z = y)");
        assert_eq!(rw("z IS DISTINCT FROM y"), "(__null_y = 1 OR z != y)");
    }

    /// A column as a LIKE pattern was read as the text of its name.
    #[test]
    fn a_column_as_a_like_pattern_is_refused() {
        assert!(rewrite_where("x LIKE y", &schema()).unwrap_err().contains("pattern"));
    }
}

#[cfg(test)]
mod not_depth_tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = SmallVec::new();
        s.push(SchemaField::new("id".to_string(), DbType::I32, DbType::I32.get_size(), 0, 0, false));
        s
    }

    /// Runs `f` on a thread with a deliberately small stack, so a rewrite that
    /// recursed once per `NOT` or per term fails here rather than fitting in
    /// the server's 32 MB.
    fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(1 << 20)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("the rewrite overflowed a 1 MiB stack")
    }

    #[test]
    fn a_long_run_of_nots_keeps_only_its_parity() {
        for n in [1usize, 2, 7, 20_000, 20_001] {
            let clause = format!("{}id = 1", "NOT ".repeat(n));
            let out = on_small_stack(move || expand_not(&clause, &schema()).map(|c| c.into_owned()));
            let out = out.unwrap_or_else(|e| panic!("{n} NOTs: {e}"));
            let want = if n % 2 == 1 { "id != 1" } else { "id = 1" };
            assert_eq!(out.trim(), want, "{n} NOTs");
        }
    }

    #[test]
    fn a_long_conjunction_under_not_is_one_level() {
        let terms: Vec<String> = (0..50_000).map(|i| format!("id = {i}")).collect();
        let clause = format!("NOT ({})", terms.join(" AND "));
        let out = on_small_stack(move || expand_not(&clause, &schema()).map(|c| c.into_owned())).expect("rewrites");
        assert_eq!(out.matches(" OR ").count(), 49_999);
        assert!(out.contains("id != 0") && out.contains("id != 49999"));
    }

    #[test]
    fn deep_parentheses_under_not_are_refused_not_a_crash() {
        let deep = |n: usize| format!("NOT {}id = 1{}", "(".repeat(n), ")".repeat(n));
        let ok = deep(MAX_NOT_NESTING);
        assert!(on_small_stack(move || expand_not(&ok, &schema()).is_ok()));
        let too_deep = deep(100_000);
        let e = on_small_stack(move || expand_not(&too_deep, &schema()).map(|c| c.into_owned())).unwrap_err();
        assert!(e.contains("nested more than"), "{e}");
    }
}

#[cfg(test)]
mod cast_path_under_not_tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = SmallVec::new();
        for (i, (name, ty)) in [("qty", DbType::I32), ("active", DbType::BOOL), ("doc", DbType::JSONB)].into_iter().enumerate() {
            s.push(SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, i as u64, false));
        }
        s
    }

    /// Rendered back for a `NOT`, a cast path keeps its cast: lexing the text
    /// again must not read it as a cast on the key literal and drop it.
    #[test]
    fn a_cast_path_keeps_its_cast_when_a_not_is_rewritten() {
        for clause in [
            "(doc->>'tier')::int = 2 AND NOT (qty = 5)",
            "NOT (qty = 5) AND (doc->>'tier')::int = 2",
            "(doc->'a'->>'b')::int = 2 AND NOT active",
            "NOT ((doc->>'tier')::int = 2)",
            "(doc #>> '{a,b}')::bigint > 1 AND NOT (qty = 5 OR qty = 6)",
        ] {
            let once = expand_not(clause, &schema()).unwrap_or_else(|e| panic!("{clause}: {e}")).into_owned();
            // What the rest of the rewrite does with it: lex it again.
            let again = lex(&once).unwrap_or_else(|e| panic!("{once}: {e}"));
            let casts = again.iter().filter(|t| matches!(t, Tok::Sym(s) if s.starts_with("::"))).count();
            assert!(casts >= 1, "{clause} -> {once}: the cast was lost");
        }
    }
}

#[cfg(test)]
mod lifted_operand_tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = SmallVec::new();
        for (i, (name, ty)) in [
            ("id", DbType::I32),
            ("name", DbType::STRING),
            ("age", DbType::I32),
            ("active", DbType::BOOL),
        ]
        .into_iter()
        .enumerate()
        {
            s.push(SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, i as u64, false));
        }
        s
    }

    /// The scan's entry point must not change a clause it has nothing to lift
    /// in: every shape below already worked, and quotes hide `||` and `%`.
    #[test]
    fn a_clause_with_nothing_to_lift_is_rewritten_as_before() {
        for clause in [
            "id = 1",
            "name LIKE 'a%'",
            "name = 'a || b' AND age > 3",
            "name LIKE '%x%'",
            "id IN (1, 2, 3)",
            "age BETWEEN 1 AND 5 OR NOT (age > 3)",
            "(id, age) > (1, 2)",
            "active = true",
            "id IS DISTINCT FROM 4",
        ] {
            assert_eq!(
                rewrite_scan_where(clause, &schema()),
                rewrite_where(clause, &schema()),
                "{clause}"
            );
        }
    }

    /// No stage after the lift may find a value, a comparison or a keyword
    /// inside a lifted operand: its text has no digit, quote or operator.
    #[test]
    fn a_lifted_operand_hides_its_text_and_round_trips() {
        let text = "CASE WHEN age % 2 = 0 THEN 'e' || name ELSE NULL END";
        let marker = encode_lifted(text, &["age".to_string(), "name".to_string()]);
        let (encoded, _) = marker[1..].split_once(LIFTED_READS as char).expect("separator");
        assert!(encoded.bytes().all(|b| (b'g'..=b'v').contains(&b)), "{encoded}");
        assert_eq!(read_lifted(&marker, 0), Some((text.to_string(), marker.len())));
        assert_eq!(read_lifted("id = 1", 0), None);
    }

    /// What reaches the tokenizer: the operand as one token, the comparison
    /// around it as it was, and the columns it reads named in the clause so
    /// the scan fetches them.
    #[test]
    fn the_tokenizer_reads_a_lifted_operand_as_one_token() {
        let out = rewrite_scan_where("age % 2 = 1 AND id > 3", &schema()).unwrap();
        assert!(out.contains(" age "), "{out:?}");
        let tokens = crate::core::tokenizer::query_tokenizer::try_tokenize(&out, &schema()).unwrap();
        assert!(
            matches!(tokens[0], crate::core::tokenizer::query_tokenizer::Token::Evaluated(_)),
            "{tokens:?}"
        );
        assert_eq!(tokens.len(), 7, "{tokens:?}");
    }

    #[test]
    fn forms_over_a_lifted_operand_are_spelled_out() {
        let lifted = |text: &str| encode_lifted(text, &["age".to_string()]);
        let m = lifted("age % 3");
        let out = lift_evaluated_operands("age % 3 IN (0, 2)", &schema()).unwrap();
        assert_eq!(out, format!("({m} = 0 OR {m} = 2)"));
        let out = lift_evaluated_operands("age % 3 NOT BETWEEN 1 AND 2", &schema()).unwrap();
        assert_eq!(out, format!("({m} < 1 OR {m} > 2)"));
        let out = lift_evaluated_operands("age % 3 IS NOT NULL", &schema()).unwrap();
        assert_eq!(out, format!("{m} NOTNULL"));
        let b = lifted("CASE WHEN age > 1 THEN true END");
        let out = lift_evaluated_operands("CASE WHEN age > 1 THEN true END IS NOT TRUE", &schema()).unwrap();
        assert_eq!(out, format!("({b} ISNULL OR {b} = false)"));
        // The positive truth tests carry their NULL test too, so that a NOT
        // over one holds for a NULL operand.
        let out = lift_evaluated_operands("CASE WHEN age > 1 THEN true END IS TRUE", &schema()).unwrap();
        assert_eq!(out, format!("({b} NOTNULL AND {b} = true)"));
        let out = lift_evaluated_operands("CASE WHEN age > 1 THEN true END IS FALSE", &schema()).unwrap();
        assert_eq!(out, format!("({b} NOTNULL AND {b} = false)"));
        let lifted_not = lift_evaluated_operands("NOT (CASE WHEN age > 1 THEN true END IS TRUE)", &schema()).unwrap();
        let out = expand_not(&lifted_not, &schema()).unwrap().into_owned();
        assert_eq!(out, format!("( {b} ISNULL OR {b} != true )"));
        // A computed operand's null test is a condition an enclosing truth
        // test can read.
        let out = rewrite_scan_where("(CASE WHEN age > 1 THEN true END IS TRUE) IS FALSE", &schema()).unwrap();
        assert!(out.contains(&format!("{b} ISNULL")), "{out}");
        let out = lift_evaluated_operands("CASE WHEN age > 1 THEN true END", &schema()).unwrap();
        assert_eq!(out, format!("{b} = true"));
        let e = lift_evaluated_operands("age % 2", &schema()).unwrap_err();
        assert!(e.contains("must be type boolean"), "{e}");
        let e = lift_evaluated_operands("age % 2 IS TRUE", &schema()).unwrap_err();
        assert!(e.contains("must be type boolean"), "{e}");
    }

    /// A bit-string literal alone is the text of its bits, which the
    /// comparison encodes at the other side's type.
    #[test]
    fn a_bare_bit_literal_becomes_its_bits() {
        assert_eq!(lift_side(" B'1010'", &schema()).unwrap(), " '1010'");
        assert_eq!(lift_side("X'1f'", &schema()).unwrap(), "'00011111'");
    }

    #[test]
    fn deep_parentheses_around_a_lifted_operand_are_refused_not_a_crash() {
        let deep = |n: usize| format!("{}age % 2 = 0{}", "(".repeat(n), ")".repeat(n));
        assert!(lift_evaluated_operands(&deep(10), &schema()).is_ok());
        let e = lift_evaluated_operands(&deep(100_000), &schema()).unwrap_err();
        assert!(e.contains("nested more than"), "{e}");
    }
}

