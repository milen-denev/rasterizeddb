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
//! # Matching without backtracking
//!
//! A pattern is compiled once into a Thompson automaton ([`Nfa`]) and a value is
//! matched by advancing the set of live states one character at a time. That
//! is linear in the value for a given pattern — `(a+)+b`, `(a*)*` and a
//! 300,000-character literal all cost one pass — and needs no recursion, so no
//! pattern can exhaust a worker's stack.
//!
//! It replaced a backtracking matcher with a step budget, which answered
//! `false` when the budget ran out. That is a wrong answer, not a refusal: a
//! long literal compared with an identical value said "no match", and so did
//! `'aaa' SIMILAR TO '(a*)*'`, whose empty-matching star looped until the budget
//! was gone. What is left of a limit now — [`WORK_LIMIT`] per value and
//! [`MAX_STATES`] per pattern — fails the statement instead of answering it.

/// Live-state visits one value may cost before the statement is failed as too
/// complex (`54001`). Matching costs at most `states × characters`, so this is
/// reached only by a large pattern against a large value — a 1 MB value under
/// a pattern of a hundred states is ten million.
const WORK_LIMIT: u64 = 200_000_000;

/// The largest automaton a pattern may compile to. Bounded repetition is
/// expanded — `x{3,5}` is three copies of `x` and two optional ones — so
/// `(abc){1000000}` would otherwise allocate millions of states per pattern.
const MAX_STATES: usize = 2_000_000;

/// How deeply parentheses may nest in a pattern. Parsing, [`min_chars`] and
/// dropping the parsed tree each recurse once per level; this keeps all three
/// far inside any thread's stack. PostgreSQL's own regex engine refuses deep
/// nesting too.
const MAX_NESTING: usize = 64;

#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// A literal character.
    Literal(char),
    /// `_` — exactly one character.
    AnyChar,
    /// `%` — any run of characters, including none.
    AnyString,
    /// `[abc]`, `[^abc]`, `[a-z]`.
    Class {
        negated: bool,
        items: Vec<ClassItem>,
    },
    /// `(a|b|c)`, and the top level of a pattern that has a `|` in it.
    Alt(Vec<Vec<Node>>),
    /// `x*`, `x+`, `x?`, `x{m,n}`.
    Repeat {
        node: Box<Node>,
        min: u32,
        max: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum ClassItem {
    One(char),
    Range(char, char),
}

/// Why a value could not be matched.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchError {
    /// The pattern is malformed or too large to compile.
    Invalid(String),
    /// Matching this value would cost more than [`WORK_LIMIT`].
    TooComplex,
}

/// Whether `text` matches `pattern` in full.
///
/// `escape` is the pattern's escape character — `SIMILAR TO … ESCAPE '\'` — and
/// defaults to a backslash, as it does in PostgreSQL. `None` disables escaping,
/// which is what `ESCAPE ''` asks for.
///
/// A scan calls this once per row with the same pattern, so the compiled
/// pattern is cached per thread, keyed by the pattern's *text* and escape —
/// never by its address, which a freed and reallocated string would reuse.
pub fn evaluate(text: &str, pattern: &str, escape: Option<char>) -> Result<bool, MatchError> {
    match &*compiled(pattern, escape) {
        Ok(compiled) => compiled.matches(text),
        Err(e) => Err(MatchError::Invalid(e.clone())),
    }
}

/// [`evaluate`] with its error as text.
pub fn matches(text: &str, pattern: &str, escape: Option<char>) -> Result<bool, String> {
    evaluate(text, pattern, escape).map_err(|e| match e {
        MatchError::Invalid(e) => e,
        MatchError::TooComplex => TOO_COMPLEX.to_string(),
    })
}

/// The message for [`MatchError::TooComplex`]; `statement too complex` is what
/// maps it to SQLSTATE `54001`.
pub const TOO_COMPLEX: &str =
    "statement too complex: matching a value against the SIMILAR TO pattern needs too much work";

/// How many distinct patterns each thread keeps parsed. A statement has one
/// or two; this only has to outlast the scan that is using them.
const CACHE_ENTRIES: usize = 16;

type CacheEntry = (String, Option<char>, std::rc::Rc<Result<Compiled, String>>);

thread_local! {
    static COMPILED: std::cell::RefCell<Vec<CacheEntry>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The automaton's working sets, reused across rows rather than allocated
    /// per row — sized by the pattern, which can be large.
    static SCRATCH: std::cell::RefCell<Scratch> =
        const { std::cell::RefCell::new(Scratch::new()) };
}

fn compiled(pattern: &str, escape: Option<char>) -> std::rc::Rc<Result<Compiled, String>> {
    COMPILED.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((_, _, hit)) = cache
            .iter()
            .find(|(p, e, _)| *e == escape && p.as_str() == pattern)
        {
            return hit.clone();
        }
        let entry = std::rc::Rc::new(parse(pattern, escape).and_then(Compiled::new));
        if cache.len() >= CACHE_ENTRIES {
            cache.remove(0);
        }
        cache.push((pattern.to_string(), escape, entry.clone()));
        entry
    })
}

/// A compiled pattern and what can be known about any text it matches.
#[derive(Debug)]
struct Compiled {
    /// The fewest characters a matching text can have.
    min_chars: usize,
    /// The literal characters the top-level sequence starts with, and those it
    /// ends with. A whole-string match must begin and end with them.
    prefix: String,
    suffix: String,
    shape: Shape,
}

/// Patterns simple enough to answer without the automaton.
#[derive(Debug)]
enum Shape {
    /// Literals only: the text must equal `prefix`.
    Exact,
    /// `prefix % suffix`, either side possibly empty — `'101%'`, `'%.csv'`.
    Wrapped,
    General(Nfa),
}

impl Compiled {
    fn new(nodes: Vec<Node>) -> Result<Self, String> {
        let literal = |n: &Node| match n {
            Node::Literal(c) => Some(*c),
            _ => None,
        };
        let prefix: String = nodes.iter().map_while(literal).collect();
        let mut suffix: Vec<char> = nodes.iter().rev().map_while(literal).collect();
        suffix.reverse();
        let suffix: String = suffix.into_iter().collect();
        let prefix_len = prefix.chars().count();
        let suffix_len = suffix.chars().count();

        let shape = if prefix_len == nodes.len() {
            Shape::Exact
        } else if prefix_len + 1 + suffix_len == nodes.len() && nodes[prefix_len] == Node::AnyString
        {
            Shape::Wrapped
        } else {
            Shape::General(Nfa::new(&nodes)?)
        };
        Ok(Compiled {
            min_chars: min_chars(&nodes),
            prefix,
            suffix,
            shape,
        })
    }

    fn matches(&self, text: &str) -> Result<bool, MatchError> {
        // Necessary conditions first. A character is at least one byte, so a
        // text with fewer bytes than the minimum has too few characters.
        if text.len() < self.min_chars
            || !text.starts_with(self.prefix.as_str())
            || !text.ends_with(self.suffix.as_str())
        {
            return Ok(false);
        }
        match &self.shape {
            Shape::Exact => Ok(text.len() == self.prefix.len()),
            // Prefix and suffix are both whole characters, so not overlapping
            // in bytes is not overlapping in characters.
            Shape::Wrapped => Ok(text.len() >= self.prefix.len() + self.suffix.len()),
            Shape::General(nfa) => nfa.matches(text),
        }
    }
}

/// The fewest characters `nodes` can match, in sequence.
fn min_chars(nodes: &[Node]) -> usize {
    nodes.iter().map(min_chars_of).fold(0, usize::saturating_add)
}

fn min_chars_of(node: &Node) -> usize {
    match node {
        Node::Literal(_) | Node::AnyChar | Node::Class { .. } => 1,
        Node::AnyString => 0,
        Node::Alt(branches) => branches.iter().map(|b| min_chars(b)).min().unwrap_or(0),
        Node::Repeat { node, min, .. } => min_chars_of(node).saturating_mul(*min as usize),
    }
}

/// Whether `pattern` is one the matcher accepts, as the error a statement
/// should fail with when it is not.
///
/// Checked when the predicate is planned, so a malformed or too deeply nested
/// pattern fails the statement — PostgreSQL's `2201B invalid regular
/// expression` — rather than silently matching no row at scan time.
pub fn validate(pattern: &str, escape: Option<char>) -> Result<(), String> {
    match &*compiled(pattern, escape) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("invalid regular expression: {}", e)),
    }
}

/// The common case: the default backslash escape.
pub fn matches_default(text: &str, pattern: &str) -> Result<bool, String> {
    matches(text, pattern, Some('\\'))
}

// ------------------------------------------------------------------- parsing

fn parse(pattern: &str, escape: Option<char>) -> Result<Vec<Node>, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut at = 0usize;
    let nodes = parse_alternation(&chars, &mut at, escape, 0)?;
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
    depth: usize,
) -> Result<Vec<Node>, String> {
    let mut branches = vec![parse_sequence(chars, at, escape, depth)?];
    while chars.get(*at) == Some(&'|') {
        *at += 1;
        branches.push(parse_sequence(chars, at, escape, depth)?);
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
    depth: usize,
) -> Result<Vec<Node>, String> {
    let mut out: Vec<Node> = Vec::new();
    while *at < chars.len() {
        match chars[*at] {
            '|' | ')' => break,
            _ => {}
        }
        let atom = parse_atom(chars, at, escape, depth)?;
        out.push(parse_quantifier(chars, at, atom)?);
    }
    Ok(out)
}

fn parse_atom(
    chars: &[char],
    at: &mut usize,
    escape: Option<char>,
    depth: usize,
) -> Result<Node, String> {
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
            // The parser recurses once per level of parentheses, and so do
            // everything that walks the parsed tree and its destructor. The
            // pattern comes from a client, so the depth is bounded here rather
            // than by whatever stack the worker happens to have.
            if depth >= MAX_NESTING {
                return Err(format!(
                    "the SIMILAR TO pattern nests parentheses more than {} deep",
                    MAX_NESTING
                ));
            }
            *at += 1;
            let inner = parse_alternation(chars, at, escape, depth + 1)?;
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
    Ok(Node::Repeat {
        node: Box::new(atom),
        min,
        max,
    })
}

// ------------------------------------------------------------------ matching

/// One automaton state. Consuming states name the state they lead to; a
/// `Split` is a free choice between two.
#[derive(Debug, Clone, Copy)]
enum State {
    Char(char, u32),
    Any(u32),
    /// Index into [`Nfa::classes`].
    Class(u32, u32),
    Split(u32, u32),
    Match,
    /// Matches nothing — a repetition whose maximum is below its minimum.
    Fail,
}

#[derive(Debug)]
struct Nfa {
    states: Vec<State>,
    classes: Vec<(bool, Vec<ClassItem>)>,
    start: u32,
    /// `universal[s]`: once `s` is live, every remaining text matches — `s`
    /// is a `%` loop that can also step straight to the end. A value is
    /// accepted the moment one is reached, without reading the rest:
    /// `'(foo|bar)%'` stops after three characters.
    universal: Vec<bool>,
    /// For a `%` loop state, an index into [`Nfa::loop_sets`]: the states live
    /// after it consumes any character, when that set is small. `u32::MAX`
    /// for every other state. Used to recognise a live set that only a single
    /// character can change, so the matcher can jump to it.
    loop_of: Vec<u32>,
    loop_sets: Vec<SmallVec<[u32; 8]>>,
    /// Whether any loop's set could ever pass [`Nfa::only_char_that_changes`].
    /// When none can, the check is not made at all: it would only cost time.
    can_jump: bool,
}

use smallvec::SmallVec;

/// How many `%` loops are analysed for [`Nfa::universal`] and [`Nfa::loops`],
/// and how many free moves each analysis may follow. Past either the pattern
/// is matched without the shortcuts, which only cost speed.
const LOOPS_ANALYSED: usize = 10_000;
const LOOP_CLOSURE_VISITS: usize = 64;

impl Nfa {
    fn new(nodes: &[Node]) -> Result<Self, String> {
        let mut nfa = Nfa {
            states: Vec::new(),
            classes: Vec::new(),
            start: 0,
            universal: Vec::new(),
            loop_of: Vec::new(),
            loop_sets: Vec::new(),
            can_jump: false,
        };
        let accept = nfa.push(State::Match)?;
        nfa.start = nfa.sequence(nodes, accept)?;
        nfa.analyse_loops();
        nfa.can_jump = nfa
            .loop_sets
            .iter()
            .any(|set| nfa.only_char_that_changes(set).is_some());
        Ok(nfa)
    }

    /// Fills [`Nfa::universal`] and [`Nfa::loops`]. A consuming state `Any(t)`
    /// whose free-move closure from `t` contains itself is a `%` loop: after
    /// any character it is live again, alongside the rest of that closure. If
    /// the closure also holds `Match`, every remaining text is accepted.
    fn analyse_loops(&mut self) {
        self.universal = vec![false; self.states.len()];
        self.loop_of = vec![u32::MAX; self.states.len()];
        let mut analysed = 0usize;
        for s in 0..self.states.len() {
            let State::Any(t) = self.states[s] else { continue };
            if analysed == LOOPS_ANALYSED {
                break;
            }
            analysed += 1;
            let Some(closure) = self.small_closure(t) else { continue };
            if !closure.contains(&(s as u32)) {
                continue;
            }
            if closure
                .iter()
                .any(|&c| matches!(self.states[c as usize], State::Match))
            {
                self.universal[s] = true;
            }
            if closure.len() <= 8 {
                self.loop_of[s] = self.loop_sets.len() as u32;
                self.loop_sets.push(closure.into_iter().collect());
            }
        }
    }

    /// The consuming states and `Match` reachable from `state` by free moves,
    /// sorted — or `None` past [`LOOP_CLOSURE_VISITS`].
    fn small_closure(&self, state: u32) -> Option<Vec<u32>> {
        let mut seen: Vec<u32> = Vec::new();
        let mut out: Vec<u32> = Vec::new();
        let mut stack = vec![state];
        while let Some(s) = stack.pop() {
            if seen.contains(&s) {
                continue;
            }
            if seen.len() == LOOP_CLOSURE_VISITS {
                return None;
            }
            seen.push(s);
            match self.states[s as usize] {
                State::Split(a, b) => {
                    stack.push(b);
                    stack.push(a);
                }
                State::Fail => {}
                _ => out.push(s),
            }
        }
        out.sort_unstable();
        Some(out)
    }

    /// The one character that can change `live`, when every other character
    /// leaves it exactly as it is: it holds at least one `%` loop, each loop
    /// leads back to exactly `live`, and the only other consuming states are
    /// `Char`s of that one character. Then the matcher can jump to that
    /// character's next occurrence instead of reading up to it.
    #[inline]
    fn only_char_that_changes(&self, live: &[u32]) -> Option<char> {
        if live.len() > 8 {
            return None;
        }
        let mut want: Option<char> = None;
        let mut has_loop = false;
        for &s in live {
            match self.states[s as usize] {
                State::Char(c, _) => {
                    if want.is_some_and(|w| w != c) {
                        return None;
                    }
                    want = Some(c);
                }
                State::Any(_) => {
                    let slot = self.loop_of[s as usize];
                    if slot == u32::MAX {
                        return None;
                    }
                    let after = &self.loop_sets[slot as usize];
                    if after.len() != live.len() || !live.iter().all(|x| after.contains(x)) {
                        return None;
                    }
                    has_loop = true;
                }
                State::Match => {}
                _ => return None,
            }
        }
        if has_loop { want } else { None }
    }

    fn push(&mut self, state: State) -> Result<u32, String> {
        if self.states.len() >= MAX_STATES {
            return Err(format!(
                "the SIMILAR TO pattern is too large once its repetitions are expanded \
                 (more than {} states)",
                MAX_STATES
            ));
        }
        self.states.push(state);
        Ok((self.states.len() - 1) as u32)
    }

    /// Compiles `nodes` so that matching them leads on to `next`. Built back
    /// to front, so every state's successor already exists. Recurses only into
    /// groups, which the parser bounds at [`MAX_NESTING`].
    fn sequence(&mut self, nodes: &[Node], next: u32) -> Result<u32, String> {
        let mut next = next;
        for node in nodes.iter().rev() {
            next = self.node(node, next)?;
        }
        Ok(next)
    }

    fn node(&mut self, node: &Node, next: u32) -> Result<u32, String> {
        match node {
            Node::Literal(c) => self.push(State::Char(*c, next)),
            Node::AnyChar => self.push(State::Any(next)),
            Node::AnyString => self.star(next, |nfa, back| nfa.push(State::Any(back))),
            Node::Class { negated, items } => {
                self.classes.push((*negated, items.clone()));
                let class = (self.classes.len() - 1) as u32;
                self.push(State::Class(class, next))
            }
            Node::Alt(branches) => {
                let mut starts = Vec::with_capacity(branches.len());
                for branch in branches {
                    starts.push(self.sequence(branch, next)?);
                }
                let mut entry = starts.pop().unwrap_or(next);
                while let Some(start) = starts.pop() {
                    entry = self.push(State::Split(start, entry))?;
                }
                Ok(entry)
            }
            Node::Repeat { node, min, max } => {
                let (min, max) = (*min as usize, max.map(|m| m as usize));
                if max.is_some_and(|max| max < min) {
                    return self.push(State::Fail);
                }
                // Checked before expanding: a body that compiles to no states
                // at all — `(){1000000000}` — would otherwise loop without
                // ever reaching the state limit.
                let copies = min.saturating_add(max.map_or(1, |max| max - min));
                if copies > MAX_STATES {
                    return Err(format!(
                        "a SIMILAR TO repetition count of {} is too large",
                        copies
                    ));
                }
                // The optional part: a star, or `max - min` nested optional
                // copies, each of which may stop and go on to `next`.
                let mut entry = match max {
                    None => self.star(next, |nfa, back| nfa.node(node, back))?,
                    Some(max) => {
                        let mut entry = next;
                        for _ in min..max {
                            let body = self.node(node, entry)?;
                            entry = self.push(State::Split(body, next))?;
                        }
                        entry
                    }
                };
                for _ in 0..min {
                    entry = self.node(node, entry)?;
                }
                Ok(entry)
            }
        }
    }

    /// A loop: a split that either enters `body` — which leads back to the
    /// split — or leaves for `next`. A body that can match nothing makes a
    /// cycle of free moves, which is harmless here: the closure in
    /// [`Nfa::add`] visits each state once per position. That is what makes
    /// `(a*)*` an ordinary pattern rather than an infinite loop.
    fn star(
        &mut self,
        next: u32,
        body: impl FnOnce(&mut Self, u32) -> Result<u32, String>,
    ) -> Result<u32, String> {
        let split = self.push(State::Split(0, 0))?;
        let entry = body(self, split)?;
        self.states[split as usize] = State::Split(entry, next);
        Ok(split)
    }

    fn matches(&self, text: &str) -> Result<bool, MatchError> {
        self.run(text, true)
    }

    /// `shortcuts` enables the early accept and the jump; off, every character
    /// is read — the reference the tests hold the shortcuts to.
    fn run(&self, text: &str, shortcuts: bool) -> Result<bool, MatchError> {
        SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            let Scratch {
                marks,
                generation,
                current,
                following,
                stack,
            } = &mut *scratch;
            if marks.len() < self.states.len() {
                marks.resize(self.states.len(), 0);
            }
            current.clear();
            following.clear();
            let mut work = 0u64;

            // A fresh generation: marks persist across rows and patterns.
            bump(marks, generation);
            let mut accept_all = false;
            self.add(self.start, current, marks, generation, stack, &mut work, &mut accept_all);
            let bytes = text.as_bytes();
            let mut at = 0usize;
            while at < bytes.len() {
                if accept_all && shortcuts {
                    return Ok(true);
                }
                if current.is_empty() {
                    return Ok(false);
                }
                // Characters that cannot change the live set are skipped by a
                // vectorised search for the one that can.
                if let Some(want) = (shortcuts && self.can_jump)
                    .then(|| self.only_char_that_changes(current))
                    .flatten()
                {
                    let mut buf = [0u8; 4];
                    let needle = want.encode_utf8(&mut buf).as_bytes();
                    match crate::core::row::common::anchor_find::<false>(&bytes[at..], needle) {
                        Some(skip) => at += skip,
                        None => break,
                    }
                }
                let c = text[at..].chars().next().expect("at is a character boundary");
                at += c.len_utf8();
                bump(marks, generation);
                for &s in current.iter() {
                    let to = match self.states[s as usize] {
                        State::Char(want, to) if want == c => to,
                        State::Any(to) => to,
                        State::Class(class, to) => {
                            let (negated, items) = &self.classes[class as usize];
                            let hit = items.iter().any(|item| match item {
                                ClassItem::One(x) => *x == c,
                                ClassItem::Range(low, high) => *low <= c && c <= *high,
                            });
                            if hit == *negated {
                                continue;
                            }
                            to
                        }
                        _ => continue,
                    };
                    self.add(to, following, marks, generation, stack, &mut work, &mut accept_all);
                }
                work += current.len() as u64;
                if work > WORK_LIMIT {
                    return Err(MatchError::TooComplex);
                }
                std::mem::swap(current, following);
                following.clear();
            }
            // `accept_all` is only acted on with a character still to read:
            // the loop consumes it and is live again, with `Match`, so every
            // remaining text is accepted. At the end, `Match` itself decides.
            Ok(current
                .iter()
                .any(|&s| matches!(self.states[s as usize], State::Match)))
        })
    }

    /// Adds `state` and everything reachable from it by free moves to `list`,
    /// each at most once per position. Iterative, with an explicit stack.
    fn add(
        &self,
        state: u32,
        list: &mut Vec<u32>,
        marks: &mut [u32],
        generation: &mut u32,
        stack: &mut Vec<u32>,
        work: &mut u64,
        accept_all: &mut bool,
    ) {
        stack.push(state);
        while let Some(s) = stack.pop() {
            if marks[s as usize] == *generation {
                continue;
            }
            marks[s as usize] = *generation;
            *work += 1;
            match self.states[s as usize] {
                State::Split(a, b) => {
                    stack.push(b);
                    stack.push(a);
                }
                State::Fail => {}
                _ => {
                    *accept_all |= self.universal[s as usize];
                    list.push(s);
                }
            }
        }
    }
}

/// Working memory for [`Nfa::matches`], kept per thread.
struct Scratch {
    /// `marks[s] == generation` when state `s` is already in the list being
    /// built for the current position.
    marks: Vec<u32>,
    generation: u32,
    current: Vec<u32>,
    following: Vec<u32>,
    stack: Vec<u32>,
}

impl Scratch {
    const fn new() -> Self {
        Scratch {
            marks: Vec::new(),
            generation: 1,
            current: Vec::new(),
            following: Vec::new(),
            stack: Vec::new(),
        }
    }
}

/// Starts a new generation of marks, so no list built so far counts as
/// containing anything. Marks left by another pattern are older generations.
fn bump(marks: &mut [u32], generation: &mut u32) {
    if *generation == u32::MAX {
        marks.iter_mut().for_each(|m| *m = 0);
        *generation = 0;
    }
    *generation += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(text: &str, pattern: &str) -> bool {
        matches_default(text, pattern).unwrap()
    }

    /// The recursive matcher this replaced, kept as the reference.
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
            Node::AnyString => (pos..=text.len())
                .rev()
                .any(|p| match_nodes(rest, text, p, steps)),
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
            Node::Repeat { node, min, max } => match_repeat(node, *min, *max, rest, text, pos, steps),
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

    /// The per-row path this replaced: parse, collect, match, every call.
    ///
    /// `Ok(None)` when it ran out of its step budget. Its answer there was
    /// `false`, and that answer was wrong exactly in the cases the automaton
    /// fixes — a long match, or a star over something that can match nothing —
    /// so those inputs are compared against direct expectations instead.
    fn reference(text: &str, pattern: &str, escape: Option<char>) -> Result<Option<bool>, String> {
        let nodes = parse(pattern, escape)?;
        let text: Vec<char> = text.chars().collect();
        let mut steps = 0u32;
        let found = match_nodes(&nodes, &text, 0, &mut steps);
        Ok(if found || steps <= STEP_BUDGET { Some(found) } else { None })
    }

    /// The reference's step budget. The old matcher used 200,000, but it
    /// recurses once per step, and on an empty-matching star that depth
    /// overflows even the 32 MB test stack. Every generated input that has an
    /// answer finds it in far fewer steps; the rest are skipped and covered by
    /// the direct tests below.
    const STEP_BUDGET: u32 = 2_000;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % bound as u64) as usize
        }
    }

    /// The cached, prefiltered path against the per-row one, over generated
    /// patterns (literals, wildcards, groups, alternation, classes, quantifiers,
    /// escapes, malformed ones) and texts (ASCII and multibyte).
    #[test]
    fn the_compiled_path_agrees_with_parsing_every_row() {
        let pattern_pieces: &[&str] = &[
            "a", "b", "c", "é", "€", "%", "%", "_", "(a|b)", "(ab|é)", "(|a)", "[a-c]", "[^b]",
            "a*", "b+", "c?", "(ab){2}", "a{1,3}", "a{2,}", "\\%", "\\_", "(", ")", "[", "|",
            ".", "{", "1", "0", "(a*)*", "(|a)*", "(a?){2,}", "(a*)+", "a{3,1}", "(b?){0,2}",
        ];
        let text_pieces: &[&str] =
            &["a", "b", "c", "é", "€", "%", "_", ".", "{", "1", "0", "ab", "😀", ""];
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let (mut compared, mut exhausted) = (0usize, 0usize);
        for _ in 0..60_000 {
            let mut pattern = String::new();
            for _ in 0..rng.next(6) {
                pattern.push_str(pattern_pieces[rng.next(pattern_pieces.len())]);
            }
            let escape = match rng.next(8) {
                0 => None,
                1 => Some('a'),
                _ => Some('\\'),
            };
            for _ in 0..4 {
                let mut text = String::new();
                for _ in 0..rng.next(9) {
                    text.push_str(text_pieces[rng.next(text_pieces.len())]);
                }
                match reference(&text, &pattern, escape) {
                    Ok(Some(expect)) => {
                        compared += 1;
                        assert_eq!(
                            matches(&text, &pattern, escape),
                            Ok(expect),
                            "text {text:?} pattern {pattern:?} escape {escape:?}"
                        );
                    }
                    Ok(None) => {
                        exhausted += 1;
                        assert!(matches(&text, &pattern, escape).is_ok());
                    }
                    Err(e) => assert_eq!(matches(&text, &pattern, escape), Err(e)),
                }
            }
        }
        assert!(compared > 150_000, "{compared}");
        // The empty-star pieces do make the old matcher run out.
        assert!(exhausted > 0, "no exhausted cases generated");
    }

    /// A star over something that can match nothing: the old matcher looped on
    /// the empty match until its budget ran out and answered `false`.
    /// PostgreSQL's answers, for every quantifier form.
    #[test]
    fn an_empty_matching_star_is_an_ordinary_pattern() {
        for (text, pattern, expect) in [
            ("aaa", "(a*)*", true),
            ("", "(a*)*", true),
            ("aab", "(a*)*", false),
            ("aaa", "(a*)+", true),
            ("aaa", "(|a)*", true),
            ("aaa", "(a?){2,}", true),
            ("", "(a?){2,}", true),
            ("aaa", "(a?){2,3}", true),
            ("aaaa", "(a?){2,3}", false),
            ("aaa", "(a*){0,}b?", true),
            ("abab", "((a|)(b|))*", true),
            ("aaa", "((a*)*)*", true),
            ("aaab", "((a*)*)*b", true),
            ("aaac", "((a*)*)*b", false),
            ("xyz", "(%)*", true),
            ("xyz", "(_*)*z", true),
        ] {
            assert_eq!(m(text, pattern), expect, "{text:?} SIMILAR TO {pattern:?}");
        }
        // And quickly: a long value against a nested empty star.
        let t = std::time::Instant::now();
        assert!(m(&"a".repeat(100_000), "((a*)*)*"));
        assert!(!m(&format!("{}b", "a".repeat(100_000)), "((a*)*)*"));
        assert!(t.elapsed() < std::time::Duration::from_secs(5), "{:?}", t.elapsed());
    }

    /// The early accept and the jump to the only character that can change
    /// the live set give the answer reading every character gives, over long
    /// generated values and patterns built around `%`.
    #[test]
    fn the_shortcuts_agree_with_reading_every_character() {
        let pattern_pieces: &[&str] = &[
            "a", "b", "é", "%", "%", "%", "_", "(a|b)", "(ab|é)", "[a-b]", "a*", "b+", "(%|a)",
            "(a%)", "(%b)*", "x", "€",
        ];
        let text_pieces: &[&str] = &["a", "b", "é", "x", "€", "ab", "ba", "xxxx"];
        let mut rng = Rng(0x0DDB_A11C_AFE0_0001);
        let (mut shortcut_taken, mut cases) = (0usize, 0usize);
        for _ in 0..20_000 {
            let mut pattern = String::new();
            for _ in 0..rng.next(5) + 1 {
                pattern.push_str(pattern_pieces[rng.next(pattern_pieces.len())]);
            }
            let Ok(nodes) = parse(&pattern, Some('\\')) else { continue };
            let Ok(nfa) = Nfa::new(&nodes) else { continue };
            if nfa.universal.iter().any(|u| *u) || !nfa.loop_sets.is_empty() {
                shortcut_taken += 1;
            }
            for _ in 0..4 {
                let mut text = String::new();
                for _ in 0..rng.next(60) {
                    text.push_str(text_pieces[rng.next(text_pieces.len())]);
                }
                cases += 1;
                assert_eq!(
                    nfa.run(&text, true),
                    nfa.run(&text, false),
                    "text {text:?} pattern {pattern:?}"
                );
            }
        }
        assert!(shortcut_taken > 1_000 && cases > 50_000, "{shortcut_taken} {cases}");
        // The shapes the shortcuts are for.
        assert!(m(&format!("foo{}", "z".repeat(100_000)), "(foo|bar)%"));
        assert!(m(&format!("{}x{}", "a".repeat(100_000), "b"), "%x%"));
        assert!(!m(&"a".repeat(100_000), "%x%"));
        assert!(m(&format!("{}€y", "a".repeat(1_000)), "%€_"));
    }

    /// Long matches answer in full now — the old matcher's budget ran out and
    /// said `false`.
    #[test]
    fn long_matches_answer_in_full() {
        let long = "ab".repeat(150_000);
        let lit = "ab".repeat(128);
        assert!(m(&long, &long));
        assert!(m(&long, &format!("{long}%")));
        assert!(m(&format!("{long}x"), &format!("{long}_")));
        assert!(!m(&format!("{long}x"), &format!("{long}y")));
        assert!(m(&long, &format!("{lit}%{lit}")));
        assert!(m(&long, "(ab)*"));
        assert!(m(&long, "(a|b)+"));
        assert!(!m(&format!("{long}c"), "(a|b)+"));
        assert!(m(&long, "%(ba){3}%"));
        assert!(m(&long, &format!("(ab){{{}}}", 150_000)));
        assert!(!m(&long, &format!("(ab){{{}}}", 149_999)));
    }

    /// What remains of a limit fails rather than answers: a pattern too large
    /// to compile, and a value too costly to match against a large pattern.
    #[test]
    fn the_limits_fail_instead_of_answering() {
        let err = matches_default("a", "(abc){100000000}").unwrap_err();
        assert!(err.contains("too large"), "{err}");
        assert!(validate("(abc){100000000}", Some('\\')).is_err());
        assert!(matches_default("a", "(){1000000000}").is_err());
        // ~200k live states for every one of ~3k characters. The trailing
        // `[c]` keeps the last `%` from accepting everything early, and, being
        // a class rather than a literal, from the suffix check.
        let pattern = format!("{}[c]", "(a|b|%)".repeat(100_000));
        let text = "a".repeat(3_000);
        // Without the `c` the last `%` can reach the end, so the answer is
        // known after one character.
        assert_eq!(matches_default(&text, &"(a|b|%)".repeat(100_000)), Ok(true));
        assert_eq!(evaluate(&text, &pattern, Some('\\')), Err(MatchError::TooComplex));
        assert!(matches_default(&text, &pattern).unwrap_err().starts_with("statement too complex"));
    }

    /// The cache is keyed by content: the same text at a different address,
    /// and different text at a reused address, must each get their own entry.
    #[test]
    fn the_cache_is_keyed_by_the_pattern_text() {
        for round in 0..40 {
            // A fresh allocation each time, often at the address just freed.
            let pattern = if round % 2 == 0 { String::from("a%") } else { String::from("b%") };
            let expect = round % 2 == 0;
            assert_eq!(m("abc", &pattern), expect, "round {round}");
            assert_eq!(matches("abc", &pattern, None).unwrap(), expect);
        }
        // More distinct patterns than the cache holds, then the first again.
        for i in 0..(CACHE_ENTRIES * 2) {
            assert!(m(&format!("x{i}"), &format!("x{i}")));
        }
        assert!(m("abc", "a%"));
        assert!(matches_default("abc", "(abc").is_err());
        assert!(matches_default("abc", "(abc").is_err(), "an error is cached as an error");
    }

    /// Runs `f` on a thread with a 2 MiB stack — Rust's default, and far less
    /// than the 32 MB the test threads here and the server's workers get — so
    /// a test passing proves the guard rather than leaning on a big stack.
    fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("the matcher must not overflow a 2 MiB stack")
    }

    /// Client-supplied patterns built to exhaust the stack: each either
    /// answers or fails cleanly, on a 2 MiB stack.
    #[test]
    fn hostile_patterns_cannot_overflow_the_stack() {
        on_small_stack(|| {
            // 300k literals: one frame each in the recursive matcher.
            let long = "ab".repeat(150_000);
            assert_eq!(matches_default(&long, &long), Ok(true));
            assert_eq!(matches_default(&long, &format!("{long}%")), Ok(true));
            assert_eq!(matches_default(&format!("{long}x"), &format!("{long}_")), Ok(true));

            let run = "a".repeat(300_000);
            assert_eq!(matches_default(&run, "a*"), Ok(true));
            assert_eq!(matches_default(&run, "(a|b)*"), Ok(true));
            assert_eq!(matches_default(&run, "(a|b)*c"), Ok(false));

            // Repeated alternations and nested quantifiers.
            // `aa` lets a run of k characters sit in any of ~k/2 groups, so
            // this costs ~n²/4 state visits: 6M here, and at 50,000 groups
            // past the work limit — a refusal, which is asserted too.
            let alts = "(a|aa|b)".repeat(5_000);
            assert_eq!(matches_default(&"a".repeat(5_000), &alts), Ok(true));
            let alts = "(a|aa|b)".repeat(50_000);
            assert_eq!(
                evaluate(&"a".repeat(50_000), &alts, Some('\\')),
                Err(MatchError::TooComplex)
            );
            let nested = format!("{}a{}", "(".repeat(MAX_NESTING), ")*".repeat(MAX_NESTING));
            assert_eq!(matches_default(&format!("{}b", "a".repeat(200)), &nested), Ok(false));
            assert_eq!(matches_default("aaa", &nested), Ok(true));
            assert_eq!(matches_default("abab", "((a(b)+)*)"), Ok(true));

            // Nesting: at the limit it parses, past it it is refused, and a
            // hundred thousand levels is refused without a crash.
            let at_limit = format!("{}a{}", "(".repeat(MAX_NESTING), ")".repeat(MAX_NESTING));
            assert_eq!(matches_default("a", &at_limit), Ok(true));
            for depth in [MAX_NESTING + 1, 100_000] {
                let deep = format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
                let err = matches_default("a", &deep).unwrap_err();
                assert!(err.contains("nests"), "{err}");
                let err = validate(&deep, Some('\\')).unwrap_err();
                assert!(err.starts_with("invalid regular expression"), "{err}");
            }
        });
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// similar_to_bench --nocapture`. 100k rows per pattern.
    #[test]
    #[ignore]
    fn similar_to_bench() {
        use std::hint::black_box;
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        let rows: Vec<String> = (0..100_000)
            .map(|i| match i % 4 {
                0 => format!("{:b}", rng.next(1 << 20)),
                1 => format!("user_{}@example.com", rng.next(100_000)),
                2 => format!("2026-{:02}-{:02} order {}", rng.next(12) + 1, rng.next(28) + 1, i),
                _ => format!("report-{}.csv", rng.next(1000)),
            })
            .collect();
        for round in 0..3 {
            println!("round {round}");
            for pattern in [
                "101%",
                "%.csv",
                "user\\_%@example.com",
                "[0-9]{4}-%",
                "%(order|report)%",
                "(a|b|c)%",
                "(user|report)%",
                "%_order 1%",
                "%(order|report)%",
            ] {
                let t = std::time::Instant::now();
                let old = rows
                    .iter()
                    .filter(|r| reference(black_box(r), pattern, Some('\\')).unwrap() == Some(true))
                    .count();
                let old_t = t.elapsed();
                let t = std::time::Instant::now();
                let new = rows.iter().filter(|r| matches_default(black_box(r), pattern).unwrap()).count();
                let new_t = t.elapsed();
                assert_eq!(old, new);
                println!("  {pattern:<22} old {old_t:>9.2?}  new {new_t:>9.2?}  ({new} hits)");
            }
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// similar_to_shortcut_bench --nocapture`. The automaton with and without
    /// the early accept and the jump, over 20k values of ~2 KB.
    #[test]
    #[ignore]
    fn similar_to_shortcut_bench() {
        use std::hint::black_box;
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        let words = ["foo", "bar", "order", "report", "berlin", "the", "quick", "brown", "fox"];
        let rows: Vec<String> = (0..20_000)
            .map(|_| {
                let mut t = String::new();
                while t.len() < 2_000 {
                    t.push_str(words[rng.next(words.len())]);
                    t.push(' ');
                }
                if rng.next(50) == 0 {
                    t.push('#');
                }
                t
            })
            .collect();
        for round in 0..3 {
            println!("round {round}");
            for pattern in ["(foo|bar)%", "%#%", "%#", "%(order|report)%", "%_rlin z%", "fo_ %"] {
                let nfa = Nfa::new(&parse(pattern, Some('\\')).unwrap()).unwrap();
                let t = std::time::Instant::now();
                let old = rows.iter().filter(|r| nfa.run(black_box(r), false).unwrap()).count();
                let old_t = t.elapsed();
                let t = std::time::Instant::now();
                let new = rows.iter().filter(|r| nfa.run(black_box(r), true).unwrap()).count();
                let new_t = t.elapsed();
                assert_eq!(old, new);
                println!("  {pattern:<20} all chars {old_t:>9.2?}  shortcuts {new_t:>9.2?}  ({new} hits)");
            }
        }
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
