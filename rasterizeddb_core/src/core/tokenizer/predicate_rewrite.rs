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
                let mut parts: Vec<String> = flags
                    .iter()
                    .map(|f| format!("{} = {}", f, if guarded { 0 } else { 1 }))
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

/// True when the token could begin a value: a literal, or a qualified name.
fn is_value_start(tok: &Tok) -> bool {
    matches!(tok, Tok::Str(_) | Tok::Word(_) | Tok::LPar)
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
                                if let Some(op) = OPS.iter().find(|op| rest.starts_with(**op)) {
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

pub fn rewrite_where(clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if clause.trim().is_empty() {
        return Ok(String::new());
    }

    // Row-value comparisons become their boolean form first, so everything
    // downstream — the NULL fold, the guards, the tokenizer — sees only shapes
    // it already understands.
    let clause = &expand_row_value_comparisons(clause)?;

    // `( <expression> ) IS [NOT] TRUE | FALSE | UNKNOWN` is expanded before
    // `x = NULL` is folded to false: under a truth test a comparison with NULL
    // is *unknown*, not false, and `(a = NULL) IS UNKNOWN` holds for every
    // row. See `parenthesised_truth_test`.
    let clause = &expand_truth_tests(clause, schema)?;

    // `x = NULL` is never true in SQL — NULL is not a value anything equals,
    // which is why `IS NULL` exists. Folded to a false predicate here so the
    // comparison never reaches the tokenizer, where an untyped NULL against a
    // typed column is a compile error rather than a row that does not match.
    let clause = &fold_null_comparisons(clause);

    let toks = lex(clause)?;
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

    while i < toks.len() {
        let tok = &toks[i];

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

        // A word that names a column may start one of the sugar forms.
        if let Tok::Word(raw) = tok {
            if let Some(col) = resolve_column(raw, schema) {
                // Look past the column at what follows.
                let next = toks.get(i + 1);

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
                        let value_at = null_at + 2;
                        let value = toks
                            .get(value_at)
                            .map(|t| t.render())
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
                            i = value_at + 1;
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
                        i = value_at + 1;
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
                    let parts: Vec<String> = values
                        .iter()
                        .map(|v| format!("{} {} {}", col, op, v))
                        .collect();
                    out.push(guard_null(
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
                    let lo = toks
                        .get(kw_at + 1)
                        .filter(|t| is_value_start(t))
                        .ok_or("BETWEEN is missing its lower bound")?;
                    if !toks
                        .get(kw_at + 2)
                        .map(|t| t.is_keyword("and"))
                        .unwrap_or(false)
                    {
                        return Err("BETWEEN bounds must be separated by AND".to_string());
                    }
                    let hi = toks
                        .get(kw_at + 3)
                        .filter(|t| is_value_start(t))
                        .ok_or("BETWEEN is missing its upper bound")?;

                    // `NOT BETWEEN` is the complement: outside the range on
                    // either side, which is a disjunction rather than a
                    // conjunction.
                    let fragment = if negated {
                        format!(
                            "({} < {} OR {} > {})",
                            col,
                            render_value(lo),
                            col,
                            render_value(hi)
                        )
                    } else {
                        format!(
                            "({} >= {} AND {} <= {})",
                            col,
                            render_value(lo),
                            col,
                            render_value(hi)
                        )
                    };
                    out.push(guard_null(&col, &fragment, schema));
                    i = kw_at + 4;
                    continue;
                }

                // A `::` cast between the column and its operator —
                // `hardware_status::text SIMILAR TO '101%'`. The cast travels
                // with the column into the emitted text; the parser applies it
                // per row.
                let (kw_at, col) = match toks.get(kw_at) {
                    Some(Tok::Sym(sym)) if sym.starts_with("::") => {
                        (kw_at + 1, format!("{}{}", col, sym))
                    }
                    _ => (kw_at, col),
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
                    if negated {
                        return Err("NOT SIMILAR TO is not supported yet".to_string());
                    }
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
                    out.push(guard_null(
                        &col,
                        &format!("{} SIMILARTO {}", col, literal),
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
                    // `ESCAPE 'c'` picks the escape character; `ESCAPE ''`
                    // turns escaping off. The default is a backslash.
                    let mut next = kw_at + 2;
                    let mut escape = Some('\\');
                    if toks.get(next).is_some_and(|t| t.is_keyword("escape")) {
                        let Some(escape_tok @ Tok::Str(_)) = toks.get(next + 1) else {
                            return Err("ESCAPE needs a quoted string".to_string());
                        };
                        let text = pattern_text(escape_tok);
                        let mut chars = text.chars();
                        escape = match (chars.next(), chars.next()) {
                            (None, _) => None,
                            (Some(c), None) => Some(c),
                            _ => {
                                return Err(
                                    "invalid escape string: it must be empty or one character"
                                        .to_string(),
                                );
                            }
                        };
                        next += 2;
                    }
                    let (op, literal) =
                        like_to_operator(&pattern_text(pattern), escape, case_insensitive)?;
                    if negated {
                        return Err("NOT LIKE is not supported yet".to_string());
                    }
                    out.push(guard_null(
                        &col,
                        &format!("{} {} {}", col, op, literal),
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
                        if let Some(flag) = nullable::flag_field(&col, schema) {
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
                    if nullable::is_nullable(&col, schema) && term_starts_at_value && out.len() >= 2
                    {
                        let op = out.pop().expect("two entries");
                        let lhs = out.pop().expect("two entries");
                        let fragment = format!("{} {} {}", lhs, op, col);
                        out.push(guard_null(&col, &fragment, schema));
                    } else {
                        out.push(col);
                    }
                    i += 1;
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
                    out.push(guard_null(&col, &fragment, schema));
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
                        if let Some(flag) = nullable::flag_field(&col, schema) {
                            let at = term_start.min(out.len());
                            out.insert(at, format!("{} = 0 AND", flag.name));
                        }
                        out.push(col);
                        i += 1;
                        continue;
                    }

                    let mut fragment = vec![col.clone()];
                    for tok in &toks[i + 1..end] {
                        fragment.push(match tok {
                            Tok::Word(w) => match resolve_column(w, schema) {
                                Some(c) => c,
                                None => render_value(tok),
                            },
                            other => other.render(),
                        });
                    }
                    out.push(guard_null(&col, &fragment.join(" "), schema));
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

            // `NOT` is recorded and resolved by whatever follows it. Only the
            // `NOT <boolean column>` form has an equivalent; `NOT (a AND b)`
            // would need the evaluator to negate a subtree.
            if raw.eq_ignore_ascii_case("not") {
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
            // statement.
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
        assert!(err("nosuchcolumn = 1").contains("unknown column"));
        assert!(err("EXISTS (SELECT 1)").contains("unknown column"));
        assert!(err("NOT (id = 1 AND age = 2)").contains("NOT is only supported"));
    }

    #[test]
    fn unsupported_forms_name_the_missing_feature() {
        assert!(err("id IN (SELECT id FROM t)").contains("subqueries"));
        assert!(err("name NOT LIKE 'a%'").contains("NOT LIKE"));
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
        assert!(err("country = NULLX").contains("unknown column"));
        // Too short to be `NULL` at all.
        assert!(err("country = NU").contains("unknown column"));
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
