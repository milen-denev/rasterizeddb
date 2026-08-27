//! SQL's `SIMILAR TO`.
//!
//! A pattern language that sits between `LIKE` and a regular expression: it
//! keeps `%` and `_` from `LIKE` and adds alternation, grouping, quantifiers
//! and bracket expressions from POSIX. The whole string must match, not a
//! substring — which is the difference that catches people out, and the reason
//! `'101%'` here means "begins with 101" rather than "contains it".
//!
//! # Why a matcher rather than a regex crate
//!
//! Because the pattern language is not a regular expression and would have to
//! be translated into one anyway — `%` means `.*` and `_` means `.`, but a
//! literal `.` in a `SIMILAR TO` pattern is a full stop and would have to be
//! escaped on the way. Translating it correctly is most of the work of matching
//! it directly, and matching it directly adds no dependency to a crate that
//! deliberately has few.
//!
//! # Bounded backtracking
//!
//! The matcher backtracks, so a pattern like `(a+)+b` against a long run of
//! `a`s can take exponential time. Every match runs under a step budget
//! ([`STEP_BUDGET`]); a pattern that exhausts it returns `false` rather than
//! occupying a query worker indefinitely. That is a refusal to answer, not a
//! wrong answer — and a scan evaluates the predicate once per row, so an
//! unbounded matcher would be a way to stall the server with one statement.

/// Steps one match may take before it gives up.
///
/// Reached only by a pathological pattern. A plain `'abc%'` costs a handful of
/// steps per row.
const STEP_BUDGET: u32 = 200_000;

#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// A literal character.
    Literal(char),
    /// `_` — exactly one character.
    AnyChar,
    /// `%` — any run of characters, including none.
    AnyString,
    /// `[abc]`, `[^abc]`, `[a-z]`.
    Class { negated: bool, items: Vec<ClassItem> },
    /// `(a|b|c)`, and the top level of a pattern that has a `|` in it.
    Alt(Vec<Vec<Node>>),
    /// `x*`, `x+`, `x?`, `x{m,n}`.
    Repeat { node: Box<Node>, min: u32, max: Option<u32> },
}

#[derive(Debug, Clone, PartialEq)]
enum ClassItem {
    One(char),
    Range(char, char),
}

/// Whether `text` matches `pattern` in full.
///
/// `escape` is the pattern's escape character — `SIMILAR TO … ESCAPE '\'` — and
/// defaults to a backslash, as it does in PostgreSQL. `None` disables escaping,
/// which is what `ESCAPE ''` asks for.
pub fn matches(text: &str, pattern: &str, escape: Option<char>) -> Result<bool, String> {
    let nodes = parse(pattern, escape)?;
    let text: Vec<char> = text.chars().collect();
    let mut steps = 0u32;
    Ok(match_nodes(&nodes, &text, 0, &mut steps))
}

/// The common case: the default backslash escape.
pub fn matches_default(text: &str, pattern: &str) -> Result<bool, String> {
    matches(text, pattern, Some('\\'))
}

// ------------------------------------------------------------------- parsing

fn parse(pattern: &str, escape: Option<char>) -> Result<Vec<Node>, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut at = 0usize;
    let nodes = parse_alternation(&chars, &mut at, escape)?;
    if at != chars.len() {
        return Err(format!(
            "unbalanced parenthesis in the SIMILAR TO pattern '{}'",
            pattern
        ));
    }
    Ok(nodes)
}

/// One or more sequences separated by `|`, stopping at `)` or the end.
fn parse_alternation(
    chars: &[char],
    at: &mut usize,
    escape: Option<char>,
) -> Result<Vec<Node>, String> {
    let mut branches = vec![parse_sequence(chars, at, escape)?];
    while chars.get(*at) == Some(&'|') {
        *at += 1;
        branches.push(parse_sequence(chars, at, escape)?);
    }
    if branches.len() == 1 {
        return Ok(branches.pop().expect("just checked there is one"));
    }
    Ok(vec![Node::Alt(branches)])
}

fn parse_sequence(
    chars: &[char],
    at: &mut usize,
    escape: Option<char>,
) -> Result<Vec<Node>, String> {
    let mut out: Vec<Node> = Vec::new();
    while *at < chars.len() {
        match chars[*at] {
            '|' | ')' => break,
            _ => {}
        }
        let atom = parse_atom(chars, at, escape)?;
        out.push(parse_quantifier(chars, at, atom)?);
    }
    Ok(out)
}

fn parse_atom(chars: &[char], at: &mut usize, escape: Option<char>) -> Result<Node, String> {
    let c = chars[*at];
    if Some(c) == escape {
        // The next character stands for itself, whatever it is.
        *at += 1;
        let escaped = *chars
            .get(*at)
            .ok_or("the SIMILAR TO pattern ends with its escape character")?;
        *at += 1;
        return Ok(Node::Literal(escaped));
    }
    match c {
        '%' => {
            *at += 1;
            Ok(Node::AnyString)
        }
        '_' => {
            *at += 1;
            Ok(Node::AnyChar)
        }
        '(' => {
            *at += 1;
            let inner = parse_alternation(chars, at, escape)?;
            if chars.get(*at) != Some(&')') {
                return Err("unbalanced parenthesis in the SIMILAR TO pattern".to_string());
            }
            *at += 1;
            // A group is one atom, so a quantifier after it applies to the
            // whole group. A single-element group needs no wrapper.
            Ok(match inner.len() {
                1 => inner.into_iter().next().expect("length is one"),
                _ => Node::Alt(vec![inner]),
            })
        }
        '[' => parse_class(chars, at),
        _ => {
            *at += 1;
            Ok(Node::Literal(c))
        }
    }
}

fn parse_class(chars: &[char], at: &mut usize) -> Result<Node, String> {
    *at += 1; // '['
    let negated = chars.get(*at) == Some(&'^');
    if negated {
        *at += 1;
    }
    let mut items: Vec<ClassItem> = Vec::new();
    // A `]` immediately after the opening bracket is a literal `]`, as in
    // POSIX.
    if chars.get(*at) == Some(&']') {
        items.push(ClassItem::One(']'));
        *at += 1;
    }
    loop {
        let c = *chars
            .get(*at)
            .ok_or("unterminated bracket expression in the SIMILAR TO pattern")?;
        if c == ']' {
            *at += 1;
            break;
        }
        *at += 1;
        // `a-z`, but a `-` before the closing bracket is a literal `-`.
        if chars.get(*at) == Some(&'-') && chars.get(*at + 1).is_some_and(|n| *n != ']') {
            let high = chars[*at + 1];
            *at += 2;
            items.push(ClassItem::Range(c, high));
        } else {
            items.push(ClassItem::One(c));
        }
    }
    Ok(Node::Class { negated, items })
}

fn parse_quantifier(chars: &[char], at: &mut usize, atom: Node) -> Result<Node, String> {
    let (min, max) = match chars.get(*at) {
        Some('*') => {
            *at += 1;
            (0, None)
        }
        Some('+') => {
            *at += 1;
            (1, None)
        }
        Some('?') => {
            *at += 1;
            (0, Some(1))
        }
        Some('{') => {
            let mut k = *at + 1;
            let mut body = String::new();
            while let Some(c) = chars.get(k) {
                if *c == '}' {
                    break;
                }
                body.push(*c);
                k += 1;
            }
            if chars.get(k) != Some(&'}') {
                // Not a quantifier after all — a literal brace, which the atom
                // reader has already consumed as one.
                return Ok(atom);
            }
            *at = k + 1;
            let mut parts = body.splitn(2, ',');
            let min: u32 = parts
                .next()
                .and_then(|p| p.trim().parse().ok())
                .ok_or("a {m,n} quantifier needs a number")?;
            let max = match parts.next() {
                None => Some(min),
                Some("") => None,
                Some(text) => Some(
                    text.trim()
                        .parse()
                        .map_err(|_| "a {m,n} quantifier needs a number".to_string())?,
                ),
            };
            (min, max)
        }
        _ => return Ok(atom),
    };
    Ok(Node::Repeat { node: Box::new(atom), min, max })
}

// ------------------------------------------------------------------ matching

fn match_nodes(nodes: &[Node], text: &[char], pos: usize, steps: &mut u32) -> bool {
    *steps += 1;
    if *steps > STEP_BUDGET {
        return false;
    }
    let Some((first, rest)) = nodes.split_first() else {
        return pos == text.len();
    };

    match first {
        Node::Literal(c) => {
            pos < text.len() && text[pos] == *c && match_nodes(rest, text, pos + 1, steps)
        }
        Node::AnyChar => pos < text.len() && match_nodes(rest, text, pos + 1, steps),
        // Tried longest-first, which finds a match no sooner but keeps the
        // greedy semantics a reader expects when a pattern is also used to
        // capture. Either order answers the same yes/no.
        Node::AnyString => (pos..=text.len()).rev().any(|p| match_nodes(rest, text, p, steps)),
        Node::Class { negated, items } => {
            if pos >= text.len() {
                return false;
            }
            let c = text[pos];
            let hit = items.iter().any(|item| match item {
                ClassItem::One(x) => *x == c,
                ClassItem::Range(low, high) => *low <= c && c <= *high,
            });
            hit != *negated && match_nodes(rest, text, pos + 1, steps)
        }
        Node::Alt(branches) => branches.iter().any(|branch| {
            let mut combined: Vec<Node> = branch.clone();
            combined.extend_from_slice(rest);
            match_nodes(&combined, text, pos, steps)
        }),
        Node::Repeat { node, min, max } => {
            match_repeat(node, *min, *max, rest, text, pos, steps)
        }
    }
}

fn match_repeat(
    node: &Node,
    min: u32,
    max: Option<u32>,
    rest: &[Node],
    text: &[char],
    pos: usize,
    steps: &mut u32,
) -> bool {
    *steps += 1;
    if *steps > STEP_BUDGET {
        return false;
    }
    // Greedy: try to take one more repetition before falling back to the rest.
    if max.is_none_or(|m| m > 0) {
        // One repetition of `node`, then the same repeat with its counts
        // decremented, then `rest`.
        let remaining_min = min.saturating_sub(1);
        let remaining_max = max.map(|m| m - 1);
        let mut tail: Vec<Node> = vec![Node::Repeat {
            node: Box::new(node.clone()),
            min: remaining_min,
            max: remaining_max,
        }];
        tail.extend_from_slice(rest);
        let mut once: Vec<Node> = vec![node.clone()];
        once.extend_from_slice(&tail);
        if match_nodes(&once, text, pos, steps) {
            return true;
        }
    }
    // Stopping here is only allowed once the minimum is met.
    min == 0 && match_nodes(rest, text, pos, steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(text: &str, pattern: &str) -> bool {
        matches_default(text, pattern).unwrap()
    }

    #[test]
    fn a_pattern_must_match_the_whole_string() {
        // The difference from `LIKE`'s cousin `~`: an unanchored substring does
        // not count.
        assert!(m("abc", "abc"));
        assert!(!m("abcd", "abc"));
        assert!(!m("xabc", "abc"));
    }

    #[test]
    fn percent_and_underscore_keep_their_like_meanings() {
        assert!(m("abc", "a%"));
        assert!(m("abc", "%c"));
        assert!(m("abc", "a_c"));
        assert!(!m("abc", "a_"));
        assert!(m("", "%"));
        assert!(m("10110", "101%"));
        assert!(!m("0101", "101%"));
    }

    #[test]
    fn alternation_and_grouping_work() {
        assert!(m("abc", "(abc|def)"));
        assert!(m("def", "(abc|def)"));
        assert!(!m("ghi", "(abc|def)"));
        assert!(m("abcabc", "(abc){2}"));
        assert!(!m("abcabc", "(abc){3}"));
    }

    #[test]
    fn quantifiers_apply_to_the_atom_before_them() {
        assert!(m("aaa", "a*"));
        assert!(m("", "a*"));
        assert!(!m("", "a+"));
        assert!(m("a", "a?"));
        assert!(m("", "a?"));
        assert!(m("aaa", "a{3}"));
        assert!(!m("aa", "a{3}"));
        assert!(m("aaaa", "a{2,}"));
        assert!(m("aaa", "a{2,4}"));
        assert!(!m("aaaaa", "a{2,4}"));
    }

    #[test]
    fn bracket_expressions_match_one_character() {
        assert!(m("b", "[abc]"));
        assert!(!m("d", "[abc]"));
        assert!(m("d", "[^abc]"));
        assert!(m("m", "[a-z]"));
        assert!(!m("M", "[a-z]"));
        assert!(m("2026", "[0-9]{4}"));
    }

    #[test]
    fn the_escape_character_makes_a_wildcard_literal() {
        assert!(m("50%", "50\\%"));
        assert!(!m("5012", "50\\%"));
        assert!(m("a_b", "a\\_b"));
        assert!(!m("axb", "a\\_b"));
    }

    #[test]
    fn a_dot_is_an_ordinary_character_not_a_wildcard() {
        // The difference from a regular expression, and the reason this is not
        // translated into one.
        assert!(m("a.c", "a.c"));
        assert!(!m("abc", "a.c"));
    }

    #[test]
    fn an_unbalanced_pattern_is_refused_rather_than_matched() {
        assert!(matches_default("abc", "(abc").is_err());
        assert!(matches_default("abc", "abc)").is_err());
        assert!(matches_default("abc", "[abc").is_err());
    }

    #[test]
    fn a_pathological_pattern_gives_up_instead_of_hanging() {
        // Classic catastrophic backtracking. The budget turns an unbounded
        // stall into a `false`, which a scan can survive — it evaluates the
        // predicate once per row.
        let text = "a".repeat(40);
        assert!(!m(&text, "(a+)+b"));
    }

    #[test]
    fn the_pattern_from_the_telemetry_query_selects_what_it_should() {
        // `hardware_status::text SIMILAR TO '101%'` — the seeded bit strings
        // are '1010' and '0101'.
        assert!(m("1010", "101%"));
        assert!(!m("0101", "101%"));
    }
}
