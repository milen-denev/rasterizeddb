//! Full-text search: `to_tsvector`, `to_tsquery`, `plainto_tsquery` and the
//! `@@` match between them.
//!
//! A `tsvector` is a document reduced to its distinct **lexemes** with the
//! positions they occurred at; a `tsquery` is a boolean expression over
//! lexemes. Both sides normalise the same way — lowercase, drop stop words,
//! stem — which is what makes `to_tsquery('english', 'running')` find a
//! document that said "runs".
//!
//! # What is stored, and why it is text
//!
//! Both are carried as their **canonical text form** — `'care':4 'fragil':1`
//! and `'urgent' & !'fragil'` — rather than a packed binary layout. They are
//! computed on the fly by the functions above and never reach storage as a
//! column type, so nothing is gained by a compact form, and the text form is
//! what a client sees and what a test can be written against.
//!
//! # Where this differs from PostgreSQL
//!
//! The stemmer is the classic Porter algorithm. PostgreSQL's `english`
//! configuration uses Snowball's Porter2, which differs from it on a minority
//! of words — `'generously'` stems to `'generous'` under Porter2 and to
//! `'gener'` here. Common vocabulary agrees. The stop-word list is
//! PostgreSQL's `english.stop`. A configuration name other than `english` or
//! `simple` is accepted and treated as `english`, because refusing it would
//! fail a query over a distinction this cannot yet act on.

/// PostgreSQL's `english.stop`, the words dropped from both sides.
const STOP_WORDS: &[&str] = &[
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "ourselves",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "he",
    "him",
    "his",
    "himself",
    "she",
    "her",
    "hers",
    "herself",
    "it",
    "its",
    "itself",
    "they",
    "them",
    "their",
    "theirs",
    "themselves",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "having",
    "do",
    "does",
    "did",
    "doing",
    "a",
    "an",
    "the",
    "and",
    "but",
    "if",
    "or",
    "because",
    "as",
    "until",
    "while",
    "of",
    "at",
    "by",
    "for",
    "with",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "to",
    "from",
    "up",
    "down",
    "in",
    "out",
    "on",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "any",
    "both",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "s",
    "t",
    "can",
    "will",
    "just",
    "don",
    "should",
    "now",
];

/// [`STOP_WORDS`] as a hash set, built once. The list was scanned linearly —
/// ~127 string compares for every word of every document.
static STOP_SET: std::sync::LazyLock<ahash::HashSet<&'static str>> =
    std::sync::LazyLock::new(|| STOP_WORDS.iter().copied().collect());

#[inline]
fn is_stop_word(lower: &str) -> bool {
    STOP_SET.contains(lower)
}

/// Whether a configuration stems. `simple` only lowercases.
fn stems(configuration: &str) -> bool {
    !configuration
        .trim()
        .trim_matches('\'')
        .eq_ignore_ascii_case("simple")
}

/// One lexeme and the positions it occurred at, one-based as PostgreSQL counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lexeme {
    pub text: String,
    pub positions: Vec<u32>,
}

/// Reduces a document to its lexemes.
pub fn to_tsvector(configuration: &str, document: &str) -> String {
    let stem_it = stems(configuration);
    let mut lexemes: Vec<Lexeme> = Vec::new();
    // Where each lexeme sits in `lexemes`; finding it was a linear search per
    // word, quadratic in a document's vocabulary.
    let mut index: ahash::HashMap<String, usize> = ahash::HashMap::default();
    let mut position = 0u32;

    for word in words(document) {
        position += 1;
        let lower = word.to_lowercase();
        if is_stop_word(&lower) {
            continue;
        }
        let text = if stem_it { stem(&lower) } else { lower };
        if text.is_empty() {
            continue;
        }
        match index.get(&text) {
            Some(&at) => lexemes[at].positions.push(position),
            None => {
                index.insert(text.clone(), lexemes.len());
                lexemes.push(Lexeme {
                    text,
                    positions: vec![position],
                });
            }
        }
    }

    // PostgreSQL prints a tsvector with its lexemes in sorted order.
    lexemes.sort_by(|a, b| a.text.cmp(&b.text));
    lexemes
        .iter()
        .map(|l| {
            format!(
                "'{}':{}",
                l.text.replace('\'', "''"),
                l.positions
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Normalises a boolean query over lexemes, keeping its operators.
///
/// Returns `Err` for a query PostgreSQL would also refuse — an operator with
/// nothing to apply to, or unbalanced parentheses — so a typo is reported
/// rather than quietly matching nothing.
pub fn to_tsquery(configuration: &str, query: &str) -> Result<String, String> {
    let node = parse_query(configuration, query)?;
    Ok(render(&node))
}

/// `plainto_tsquery`: every word, `AND`ed, with no operator syntax.
pub fn plainto_tsquery(configuration: &str, text: &str) -> String {
    let stem_it = stems(configuration);
    let terms: Vec<String> = words(text)
        .into_iter()
        .map(|w| w.to_lowercase())
        .filter(|w| !is_stop_word(w))
        .map(|w| if stem_it { stem(&w) } else { w })
        .filter(|w| !w.is_empty())
        .collect();
    terms
        .iter()
        .map(|t| format!("'{}'", t))
        .collect::<Vec<_>>()
        .join(" & ")
}

/// `tsvector @@ tsquery`.
///
/// Both operands are the canonical text the functions above produce. A query
/// that cannot be parsed matches nothing rather than erroring, because by the
/// time the two meet the query has already been through [`to_tsquery`], which
/// is where a malformed one is reported.
pub fn matches(vector: &str, query: &str) -> bool {
    // The query is compiled once and reused for every row of the statement:
    // it used to be tokenized and parsed again per row.
    let Some(compiled) = compiled_query(query) else {
        return false;
    };
    // One pass over the vector marks which of the query's terms occur. The
    // vector used to be copied into a `Vec<String>` per row and each term
    // searched for linearly.
    let mut present: smallvec::SmallVec<[bool; 16]> =
        smallvec::smallvec![false; compiled.terms.len()];
    for entry in vector.split_whitespace() {
        let entry = entry.trim();
        let Some(body) = entry.strip_prefix('\'') else {
            continue;
        };
        let Some(close) = body.rfind('\'') else {
            continue;
        };
        let lexeme = &body[..close];
        if lexeme.contains("''") {
            compiled.mark(&lexeme.replace("''", "'"), &mut present);
        } else {
            compiled.mark(lexeme, &mut present);
        }
    }
    compiled.evaluate(&compiled.root, &present)
}

/// A tsquery parsed once: its distinct terms, numbered, and the tree over
/// those numbers.
#[derive(Debug)]
struct CompiledQuery {
    terms: Vec<String>,
    /// Term to number, for a query too long to search linearly.
    index: Option<ahash::HashMap<String, usize>>,
    root: CNode,
}

#[derive(Debug)]
enum CNode {
    Term(usize),
    Not(Box<CNode>),
    And(Box<CNode>, Box<CNode>),
    Or(Box<CNode>, Box<CNode>),
}

impl CompiledQuery {
    fn compile(node: &Node) -> CompiledQuery {
        fn walk(node: &Node, terms: &mut Vec<String>) -> CNode {
            match node {
                Node::Term(t) => match terms.iter().position(|x| x == t) {
                    Some(at) => CNode::Term(at),
                    None => {
                        terms.push(t.clone());
                        CNode::Term(terms.len() - 1)
                    }
                },
                Node::Not(inner) => CNode::Not(Box::new(walk(inner, terms))),
                // `<->` is matched as `AND`; see `Node::Followed`.
                Node::And(a, b) | Node::Followed(a, b) => {
                    CNode::And(Box::new(walk(a, terms)), Box::new(walk(b, terms)))
                }
                Node::Or(a, b) => CNode::Or(Box::new(walk(a, terms)), Box::new(walk(b, terms))),
            }
        }
        let mut terms = Vec::new();
        let root = walk(node, &mut terms);
        let index = (terms.len() > 8).then(|| {
            terms
                .iter()
                .enumerate()
                .map(|(i, t)| (t.clone(), i))
                .collect()
        });
        CompiledQuery { terms, index, root }
    }

    #[inline]
    fn mark(&self, lexeme: &str, present: &mut [bool]) {
        match &self.index {
            Some(index) => {
                if let Some(&at) = index.get(lexeme) {
                    present[at] = true;
                }
            }
            None => {
                for (at, t) in self.terms.iter().enumerate() {
                    if t == lexeme {
                        present[at] = true;
                    }
                }
            }
        }
    }

    fn evaluate(&self, node: &CNode, present: &[bool]) -> bool {
        match node {
            CNode::Term(at) => present[*at],
            CNode::Not(inner) => !self.evaluate(inner, present),
            CNode::And(a, b) => self.evaluate(a, present) && self.evaluate(b, present),
            CNode::Or(a, b) => self.evaluate(a, present) || self.evaluate(b, present),
        }
    }
}

/// The compiled form of a query text, from a per-thread cache keyed by the
/// text itself — never by its address, which a freed and reissued buffer can
/// share with a different query. `None` for a query that does not parse.
///
/// The query text is already normalised, so it is parsed with the `simple`
/// configuration: stemming it a second time would turn `'fragil'` into
/// something else again.
fn compiled_query(query: &str) -> Option<std::rc::Rc<CompiledQuery>> {
    use std::cell::RefCell;
    use std::rc::Rc;
    /// Enough for every query a statement mix uses at once; past it the cache
    /// starts again rather than growing with the number of distinct texts.
    const CAPACITY: usize = 256;
    thread_local! {
        static CACHE: RefCell<ahash::HashMap<Box<str>, Option<Rc<CompiledQuery>>>> =
            RefCell::new(ahash::HashMap::default());
    }
    CACHE.with(|cache| {
        if let Some(hit) = cache.borrow().get(query) {
            return hit.clone();
        }
        let compiled = parse_query("simple", query)
            .ok()
            .map(|node| Rc::new(CompiledQuery::compile(&node)));
        let mut cache = cache.borrow_mut();
        if cache.len() >= CAPACITY {
            cache.clear();
        }
        cache.insert(query.into(), compiled.clone());
        compiled
    })
}

// ------------------------------------------------------------ query grammar

#[derive(Debug, Clone, PartialEq)]
enum Node {
    Term(String),
    Not(Box<Node>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    /// `<->`, the phrase operator. Matched as an `AND` here: the positions a
    /// phrase search needs are in the vector, but the distance test is not
    /// implemented, so this is *weaker* than PostgreSQL rather than wrong in
    /// the dangerous direction — it can return a row PostgreSQL would not, and
    /// never drops one it would return.
    Followed(Box<Node>, Box<Node>),
}

/// The per-row evaluation as it was, kept as the reference for the
/// differential test.
#[cfg(test)]
fn evaluate(node: &Node, present: &[String]) -> bool {
    match node {
        Node::Term(t) => present.iter().any(|p| p == t),
        Node::Not(inner) => !evaluate(inner, present),
        Node::And(a, b) | Node::Followed(a, b) => evaluate(a, present) && evaluate(b, present),
        Node::Or(a, b) => evaluate(a, present) || evaluate(b, present),
    }
}

fn render(node: &Node) -> String {
    match node {
        Node::Term(t) => format!("'{}'", t.replace('\'', "''")),
        Node::Not(inner) => format!("!{}", render_nested(inner)),
        Node::And(a, b) => format!("{} & {}", render_nested(a), render_nested(b)),
        Node::Or(a, b) => format!("{} | {}", render_nested(a), render_nested(b)),
        Node::Followed(a, b) => format!("{} <-> {}", render_nested(a), render_nested(b)),
    }
}

fn render_nested(node: &Node) -> String {
    match node {
        Node::Term(_) | Node::Not(_) => render(node),
        _ => format!("( {} )", render(node)),
    }
}

struct Parser<'a> {
    tokens: Vec<String>,
    at: usize,
    configuration: &'a str,
}

fn parse_query(configuration: &str, query: &str) -> Result<Node, String> {
    let mut parser = Parser {
        tokens: tokenize_query(query),
        at: 0,
        configuration,
    };
    let node = parser.parse_or()?;
    if parser.at != parser.tokens.len() {
        return Err(format!("syntax error in tsquery: '{}'", query));
    }
    Ok(node)
}

fn tokenize_query(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes: Vec<char> = query.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            // A quoted lexeme, with `''` for a literal quote.
            let mut text = String::new();
            i += 1;
            while i < bytes.len() {
                if bytes[i] == '\'' {
                    if bytes.get(i + 1) == Some(&'\'') {
                        text.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                text.push(bytes[i]);
                i += 1;
            }
            out.push(format!("'{}", text));
        } else if c == '<' && bytes.get(i + 1) == Some(&'-') && bytes.get(i + 2) == Some(&'>') {
            out.push("<->".to_string());
            i += 3;
        } else if matches!(c, '&' | '|' | '!' | '(' | ')') {
            out.push(c.to_string());
            i += 1;
        } else {
            let start = i;
            while i < bytes.len()
                && !bytes[i].is_whitespace()
                && !matches!(bytes[i], '&' | '|' | '!' | '(' | ')')
            {
                i += 1;
            }
            out.push(bytes[start..i].iter().collect());
        }
    }
    out
}

impl Parser<'_> {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.at).map(|s| s.as_str())
    }

    fn parse_or(&mut self) -> Result<Node, String> {
        let mut left = self.parse_and()?;
        while self.peek() == Some("|") {
            self.at += 1;
            let right = self.parse_and()?;
            left = Node::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Node, String> {
        let mut left = self.parse_not()?;
        loop {
            match self.peek() {
                Some("&") => {
                    self.at += 1;
                    let right = self.parse_not()?;
                    left = Node::And(Box::new(left), Box::new(right));
                }
                Some("<->") => {
                    self.at += 1;
                    let right = self.parse_not()?;
                    left = Node::Followed(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Node, String> {
        if self.peek() == Some("!") {
            self.at += 1;
            return Ok(Node::Not(Box::new(self.parse_not()?)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<Node, String> {
        match self.peek() {
            Some("(") => {
                self.at += 1;
                let inner = self.parse_or()?;
                if self.peek() != Some(")") {
                    return Err("unbalanced parentheses in tsquery".to_string());
                }
                self.at += 1;
                Ok(inner)
            }
            Some(token) => {
                let token = token.to_string();
                self.at += 1;
                if matches!(token.as_str(), "&" | "|" | ")" | "<->") {
                    return Err(format!("tsquery operator '{}' has no operand", token));
                }
                // A token the tokenizer quoted is already a lexeme.
                let (raw, quoted) = match token.strip_prefix('\'') {
                    Some(rest) => (rest.to_string(), true),
                    None => (token, false),
                };
                let lower = raw.to_lowercase();
                let text = if quoted || !stems(self.configuration) {
                    lower
                } else {
                    stem(&lower)
                };
                Ok(Node::Term(text))
            }
            None => Err("tsquery ended where a term was expected".to_string()),
        }
    }
}

/// The words of a document: runs of alphanumerics, with apostrophes kept inside
/// a word so `don't` is one token.
fn words(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() || (c == '\'' && !current.is_empty()) {
            current.push(c);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

// ------------------------------------------------------------- Porter stemmer

fn is_vowel(chars: &[char], i: usize) -> bool {
    match chars[i] {
        'a' | 'e' | 'i' | 'o' | 'u' => true,
        // A `y` is a vowel only when what precedes it is not.
        'y' => i > 0 && !is_vowel(chars, i - 1),
        _ => false,
    }
}

/// The `m` of the Porter algorithm: how many vowel-consonant pairs the word has.
fn measure(chars: &[char]) -> usize {
    let mut count = 0;
    let mut previous_vowel = false;
    for i in 0..chars.len() {
        let vowel = is_vowel(chars, i);
        if i > 0 && previous_vowel && !vowel {
            count += 1;
        }
        previous_vowel = vowel;
    }
    count
}

fn contains_vowel(chars: &[char]) -> bool {
    (0..chars.len()).any(|i| is_vowel(chars, i))
}

fn ends_double_consonant(chars: &[char]) -> bool {
    let n = chars.len();
    n >= 2 && chars[n - 1] == chars[n - 2] && !is_vowel(chars, n - 1)
}

/// Consonant–vowel–consonant where the final consonant is not `w`, `x` or `y`.
fn ends_cvc(chars: &[char]) -> bool {
    let n = chars.len();
    if n < 3 {
        return false;
    }
    !is_vowel(chars, n - 3)
        && is_vowel(chars, n - 2)
        && !is_vowel(chars, n - 1)
        && !matches!(chars[n - 1], 'w' | 'x' | 'y')
}

/// Reduces a lowercase word to its stem.
///
/// A word with a digit in it, or shorter than three letters, is returned
/// unchanged — stemming those produces noise rather than a stem, and
/// PostgreSQL leaves them alone too.
///
/// Works in one reusable per-thread buffer, editing suffixes in place and
/// testing them against the buffer's tail without copying. It used to collect
/// every suffix into a fresh `Vec<char>` for each test, copy the stem for each
/// measure, and sort step 4's list on every call — which made it most of
/// `to_tsvector`'s cost. The stems are byte-identical to that version's
/// (`stem_reference`, and the differential test beside it).
pub fn stem(word: &str) -> String {
    if word.len() <= 2 || word.chars().any(|c| c.is_numeric()) {
        return word.to_string();
    }
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<char>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUFFER.with(|buffer| {
        let mut chars = buffer.borrow_mut();
        chars.clear();
        chars.extend(word.chars());

        step_1a(&mut chars);
        step_1b(&mut chars);
        step_1c(&mut chars);
        step_2(&mut chars);
        step_3(&mut chars);
        step_4(&mut chars);
        step_5(&mut chars);

        chars.iter().collect()
    })
}

/// Whether the word ends with the ASCII `suffix`, without allocating.
#[inline]
fn ends_with(chars: &[char], suffix: &str) -> bool {
    let s = suffix.as_bytes();
    chars.len() >= s.len()
        && chars[chars.len() - s.len()..]
            .iter()
            .zip(s)
            .all(|(c, b)| *c == *b as char)
}

/// The word without its last `n` characters.
#[inline]
fn without(chars: &[char], n: usize) -> &[char] {
    &chars[..chars.len() - n]
}

/// Replaces the ASCII `suffix` the word ends with by `with`, in place.
#[inline]
fn replace_suffix(chars: &mut Vec<char>, suffix: &str, with: &str) {
    chars.truncate(chars.len() - suffix.len());
    chars.extend(with.bytes().map(char::from));
}

fn step_1a(chars: &mut Vec<char>) {
    if ends_with(chars, "sses") {
        replace_suffix(chars, "sses", "ss");
    } else if ends_with(chars, "ies") {
        replace_suffix(chars, "ies", "i");
    } else if ends_with(chars, "ss") {
        // Unchanged.
    } else if ends_with(chars, "s") {
        replace_suffix(chars, "s", "");
    }
}

fn step_1b(chars: &mut Vec<char>) {
    let mut second_pass = false;
    if ends_with(chars, "eed") {
        if measure(without(chars, 3)) > 0 {
            replace_suffix(chars, "eed", "ee");
        }
    } else if ends_with(chars, "ed") {
        if contains_vowel(without(chars, 2)) {
            replace_suffix(chars, "ed", "");
            second_pass = true;
        }
    } else if ends_with(chars, "ing") {
        if contains_vowel(without(chars, 3)) {
            replace_suffix(chars, "ing", "");
            second_pass = true;
        }
    }

    if second_pass {
        if ends_with(chars, "at") {
            replace_suffix(chars, "at", "ate");
        } else if ends_with(chars, "bl") {
            replace_suffix(chars, "bl", "ble");
        } else if ends_with(chars, "iz") {
            replace_suffix(chars, "iz", "ize");
        } else if ends_double_consonant(chars) && !matches!(chars[chars.len() - 1], 'l' | 's' | 'z')
        {
            chars.pop();
        } else if measure(chars) == 1 && ends_cvc(chars) {
            chars.push('e');
        }
    }
}

fn step_1c(chars: &mut Vec<char>) {
    if ends_with(chars, "y") && contains_vowel(without(chars, 1)) {
        replace_suffix(chars, "y", "i");
    }
}

/// Applies the first matching suffix rule whose stem has measure above `min`.
fn apply_rules(chars: &mut Vec<char>, rules: &[(&str, &str)], min: usize) {
    for (suffix, with) in rules {
        if ends_with(chars, suffix) {
            if measure(without(chars, suffix.len())) > min {
                replace_suffix(chars, suffix, with);
            }
            return;
        }
    }
}

fn step_2(chars: &mut Vec<char>) {
    const RULES: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("bli", "ble"),
        ("alli", "al"),
        ("entli", "ent"),
        ("eli", "e"),
        ("ousli", "ous"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("iveness", "ive"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("biliti", "ble"),
        ("logi", "log"),
    ];
    apply_rules(chars, RULES, 0);
}

fn step_3(chars: &mut Vec<char>) {
    const RULES: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];
    apply_rules(chars, RULES, 0);
}

fn step_4(chars: &mut Vec<char>) {
    // Longest first, so `ement` is not cut as `ent`. Ties keep the order of
    // the list they were sorted from (a stable sort, which the previous
    // version ran on every call): `ance` before `ence`, `able` before `ible`.
    const ORDERED: &[&str] = &[
        "ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate", "iti",
        "ous", "ive", "ize", "al", "er", "ic", "ou",
    ];
    for suffix in ORDERED {
        if !ends_with(chars, suffix) {
            continue;
        }
        if measure(without(chars, suffix.len())) > 1 {
            replace_suffix(chars, suffix, "");
        }
        return;
    }
    // `ion` only survives after `s` or `t`.
    if ends_with(chars, "ion") {
        let stem = without(chars, 3);
        if measure(stem) > 1 && matches!(stem.last(), Some('s') | Some('t')) {
            replace_suffix(chars, "ion", "");
        }
    }
}

fn step_5(chars: &mut Vec<char>) {
    if ends_with(chars, "e") {
        let stem = without(chars, 1);
        let m = measure(stem);
        if m > 1 || (m == 1 && !ends_cvc(stem)) {
            chars.pop();
        }
    }
    if ends_with(chars, "ll") && measure(chars) > 1 {
        chars.pop();
    }
}

/// The stemmer as it was before the in-place rewrite, kept verbatim as the
/// reference for the differential test.
#[cfg(test)]
mod stem_reference {
    fn is_vowel(chars: &[char], i: usize) -> bool {
        match chars[i] {
            'a' | 'e' | 'i' | 'o' | 'u' => true,
            // A `y` is a vowel only when what precedes it is not.
            'y' => i > 0 && !is_vowel(chars, i - 1),
            _ => false,
        }
    }

    /// The `m` of the Porter algorithm: how many vowel-consonant pairs the word has.
    fn measure(chars: &[char]) -> usize {
        let mut count = 0;
        let mut previous_vowel = false;
        let mut started = false;
        for i in 0..chars.len() {
            let vowel = is_vowel(chars, i);
            if started && previous_vowel && !vowel {
                count += 1;
            }
            if !started {
                started = true;
            }
            previous_vowel = vowel;
        }
        count
    }

    fn contains_vowel(chars: &[char]) -> bool {
        (0..chars.len()).any(|i| is_vowel(chars, i))
    }

    fn ends_double_consonant(chars: &[char]) -> bool {
        let n = chars.len();
        n >= 2 && chars[n - 1] == chars[n - 2] && !is_vowel(chars, n - 1)
    }

    /// Consonant–vowel–consonant where the final consonant is not `w`, `x` or `y`.
    fn ends_cvc(chars: &[char]) -> bool {
        let n = chars.len();
        if n < 3 {
            return false;
        }
        !is_vowel(chars, n - 3)
            && is_vowel(chars, n - 2)
            && !is_vowel(chars, n - 1)
            && !matches!(chars[n - 1], 'w' | 'x' | 'y')
    }

    /// Reduces a lowercase word to its stem.
    ///
    /// A word with a digit in it, or shorter than three letters, is returned
    /// unchanged — stemming those produces noise rather than a stem, and
    /// PostgreSQL leaves them alone too.
    pub(super) fn stem(word: &str) -> String {
        if word.len() <= 2 || word.chars().any(|c| c.is_numeric()) {
            return word.to_string();
        }
        let mut chars: Vec<char> = word.chars().collect();

        step_1a(&mut chars);
        step_1b(&mut chars);
        step_1c(&mut chars);
        step_2(&mut chars);
        step_3(&mut chars);
        step_4(&mut chars);
        step_5(&mut chars);

        chars.into_iter().collect()
    }

    fn ends_with(chars: &[char], suffix: &str) -> bool {
        let s: Vec<char> = suffix.chars().collect();
        chars.len() >= s.len() && chars[chars.len() - s.len()..] == s[..]
    }

    fn replace_suffix(chars: &mut Vec<char>, suffix: &str, with: &str) {
        let n = suffix.chars().count();
        chars.truncate(chars.len() - n);
        chars.extend(with.chars());
    }

    fn step_1a(chars: &mut Vec<char>) {
        if ends_with(chars, "sses") {
            replace_suffix(chars, "sses", "ss");
        } else if ends_with(chars, "ies") {
            replace_suffix(chars, "ies", "i");
        } else if ends_with(chars, "ss") {
            // Unchanged.
        } else if ends_with(chars, "s") {
            replace_suffix(chars, "s", "");
        }
    }

    fn step_1b(chars: &mut Vec<char>) {
        let mut second_pass = false;
        if ends_with(chars, "eed") {
            let stem: Vec<char> = chars[..chars.len() - 3].to_vec();
            if measure(&stem) > 0 {
                replace_suffix(chars, "eed", "ee");
            }
        } else if ends_with(chars, "ed") {
            let stem: Vec<char> = chars[..chars.len() - 2].to_vec();
            if contains_vowel(&stem) {
                replace_suffix(chars, "ed", "");
                second_pass = true;
            }
        } else if ends_with(chars, "ing") {
            let stem: Vec<char> = chars[..chars.len() - 3].to_vec();
            if contains_vowel(&stem) {
                replace_suffix(chars, "ing", "");
                second_pass = true;
            }
        }

        if second_pass {
            if ends_with(chars, "at") {
                replace_suffix(chars, "at", "ate");
            } else if ends_with(chars, "bl") {
                replace_suffix(chars, "bl", "ble");
            } else if ends_with(chars, "iz") {
                replace_suffix(chars, "iz", "ize");
            } else if ends_double_consonant(chars) && !matches!(chars[chars.len() - 1], 'l' | 's' | 'z')
            {
                chars.pop();
            } else if measure(chars) == 1 && ends_cvc(chars) {
                chars.push('e');
            }
        }
    }

    fn step_1c(chars: &mut Vec<char>) {
        if ends_with(chars, "y") {
            let stem: Vec<char> = chars[..chars.len() - 1].to_vec();
            if contains_vowel(&stem) {
                replace_suffix(chars, "y", "i");
            }
        }
    }

    /// Applies the first matching suffix rule whose stem has measure above `min`.
    fn apply_rules(chars: &mut Vec<char>, rules: &[(&str, &str)], min: usize) {
        for (suffix, with) in rules {
            if ends_with(chars, suffix) {
                let stem: Vec<char> = chars[..chars.len() - suffix.chars().count()].to_vec();
                if measure(&stem) > min {
                    replace_suffix(chars, suffix, with);
                }
                return;
            }
        }
    }

    fn step_2(chars: &mut Vec<char>) {
        const RULES: &[(&str, &str)] = &[
            ("ational", "ate"),
            ("tional", "tion"),
            ("enci", "ence"),
            ("anci", "ance"),
            ("izer", "ize"),
            ("bli", "ble"),
            ("alli", "al"),
            ("entli", "ent"),
            ("eli", "e"),
            ("ousli", "ous"),
            ("ization", "ize"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("iveness", "ive"),
            ("fulness", "ful"),
            ("ousness", "ous"),
            ("aliti", "al"),
            ("iviti", "ive"),
            ("biliti", "ble"),
            ("logi", "log"),
        ];
        apply_rules(chars, RULES, 0);
    }

    fn step_3(chars: &mut Vec<char>) {
        const RULES: &[(&str, &str)] = &[
            ("icate", "ic"),
            ("ative", ""),
            ("alize", "al"),
            ("iciti", "ic"),
            ("ical", "ic"),
            ("ful", ""),
            ("ness", ""),
        ];
        apply_rules(chars, RULES, 0);
    }

    fn step_4(chars: &mut Vec<char>) {
        const SUFFIXES: &[&str] = &[
            "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ou",
            "ism", "ate", "iti", "ous", "ive", "ize",
        ];
        // Longest first, so `ement` is not cut as `ent`.
        let mut ordered: Vec<&&str> = SUFFIXES.iter().collect();
        ordered.sort_by_key(|s| std::cmp::Reverse(s.len()));

        for suffix in ordered {
            if !ends_with(chars, suffix) {
                continue;
            }
            let stem: Vec<char> = chars[..chars.len() - suffix.chars().count()].to_vec();
            if measure(&stem) > 1 {
                // `ion` only survives after `s` or `t`.
                replace_suffix(chars, suffix, "");
            }
            return;
        }
        if ends_with(chars, "ion") {
            let stem: Vec<char> = chars[..chars.len() - 3].to_vec();
            if measure(&stem) > 1 && matches!(stem.last(), Some('s') | Some('t')) {
                replace_suffix(chars, "ion", "");
            }
        }
    }

    fn step_5(chars: &mut Vec<char>) {
        if ends_with(chars, "e") {
            let stem: Vec<char> = chars[..chars.len() - 1].to_vec();
            let m = measure(&stem);
            if m > 1 || (m == 1 && !ends_cvc(&stem)) {
                chars.pop();
            }
        }
        if ends_with(chars, "ll") && measure(chars) > 1 {
            chars.pop();
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_becomes_its_sorted_lexemes_with_positions() {
        assert_eq!(
            to_tsvector("english", "urgent delivery please"),
            "'deliveri':2 'pleas':3 'urgent':1"
        );
    }

    #[test]
    fn stop_words_are_dropped_but_still_count_positions() {
        // `with` is a stop word: it takes position 3 and is not emitted, so
        // `care` is at 4 rather than 3.
        assert_eq!(
            to_tsvector("english", "fragile handle with care"),
            "'care':4 'fragil':1 'handl':2"
        );
    }

    #[test]
    fn the_simple_configuration_lowercases_without_stemming() {
        assert_eq!(
            to_tsvector("simple", "Running Quickly"),
            "'quickly':2 'running':1"
        );
    }

    #[test]
    fn a_repeated_word_collects_its_positions() {
        assert_eq!(to_tsvector("english", "cat dog cat"), "'cat':1,3 'dog':2");
    }

    #[test]
    fn a_query_matches_a_document_that_holds_its_terms() {
        let vector = to_tsvector("english", "urgent delivery please");
        let query = to_tsquery("english", "urgent & !fragile").unwrap();
        assert!(matches(&vector, &query));
    }

    #[test]
    fn a_negated_term_excludes_the_document_that_has_it() {
        let vector = to_tsvector("english", "fragile handle with care");
        let query = to_tsquery("english", "urgent & !fragile").unwrap();
        assert!(!matches(&vector, &query));
    }

    #[test]
    fn stemming_makes_an_inflected_query_find_its_root() {
        let vector = to_tsvector("english", "the package is running late");
        let query = to_tsquery("english", "runs").unwrap();
        assert!(matches(&vector, &query), "vector was {}", vector);
    }

    #[test]
    fn or_and_parentheses_group_as_written() {
        let vector = to_tsvector("english", "gift wrapping");
        assert!(matches(
            &vector,
            &to_tsquery("english", "gift | express").unwrap()
        ));
        assert!(!matches(
            &vector,
            &to_tsquery("english", "( express | rush ) & gift").unwrap()
        ));
        assert!(matches(
            &vector,
            &to_tsquery("english", "( gift | rush ) & wrapping").unwrap()
        ));
    }

    #[test]
    fn a_query_with_a_dangling_operator_is_refused() {
        assert!(to_tsquery("english", "urgent &").is_err());
        assert!(to_tsquery("english", "& urgent").is_err());
        assert!(to_tsquery("english", "( urgent").is_err());
    }

    #[test]
    fn plainto_ands_every_word() {
        assert_eq!(
            plainto_tsquery("english", "urgent fragile"),
            "'urgent' & 'fragil'"
        );
    }

    #[test]
    fn the_stemmer_agrees_with_porter_on_common_words() {
        for (word, expected) in [
            ("caresses", "caress"),
            ("ponies", "poni"),
            ("cats", "cat"),
            ("feed", "feed"),
            ("agreed", "agre"),
            ("plastered", "plaster"),
            ("motoring", "motor"),
            ("sing", "sing"),
            ("conflated", "conflat"),
            ("troubled", "troubl"),
            ("hopping", "hop"),
            ("falling", "fall"),
            ("happy", "happi"),
            ("relational", "relat"),
            ("rational", "ration"),
        ] {
            assert_eq!(stem(word), expected, "{}", word);
        }
    }

    #[test]
    fn a_word_with_a_digit_is_left_alone() {
        assert_eq!(stem("sku123"), "sku123");
        assert_eq!(stem("a1"), "a1");
    }
}

/// The matcher and the vectorizer as they were — linear stop-word scan,
/// linear lexeme lookup, the query parsed per row — kept verbatim as the
/// reference the rewritten ones are checked against.
#[cfg(test)]
mod reference {
    use super::*;

    pub(super) fn to_tsvector(configuration: &str, document: &str) -> String {
        let stem_it = stems(configuration);
        let mut lexemes: Vec<Lexeme> = Vec::new();
        let mut position = 0u32;
        for word in words(document) {
            position += 1;
            let lower = word.to_lowercase();
            if STOP_WORDS.contains(&lower.as_str()) {
                continue;
            }
            let text = if stem_it {
                super::stem_reference::stem(&lower)
            } else {
                lower
            };
            if text.is_empty() {
                continue;
            }
            match lexemes.iter_mut().find(|l| l.text == text) {
                Some(existing) => existing.positions.push(position),
                None => lexemes.push(Lexeme {
                    text,
                    positions: vec![position],
                }),
            }
        }
        lexemes.sort_by(|a, b| a.text.cmp(&b.text));
        lexemes
            .iter()
            .map(|l| {
                format!(
                    "'{}':{}",
                    l.text.replace('\'', "''"),
                    l.positions
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub(super) fn matches(vector: &str, query: &str) -> bool {
        let present: Vec<String> = vector
            .split_whitespace()
            .filter_map(|entry| {
                let entry = entry.trim();
                let body = entry.strip_prefix('\'')?;
                let close = body.rfind('\'')?;
                Some(body[..close].replace("''", "'"))
            })
            .collect();
        let Ok(node) = parse_query("simple", query) else {
            return false;
        };
        evaluate(&node, &present)
    }
}

#[cfg(test)]
mod differential {
    use super::*;

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        fn pick<'a>(&mut self, xs: &'a [&'a str]) -> &'a str {
            xs[(self.next() as usize) % xs.len()]
        }
    }

    const VOCAB: &[&str] = &[
        "the", "Running", "runs", "ran", "urgent", "delivery", "fragile", "FRAGILE",
        "don't", "it's", "o'clock", "care", "careful", "a", "an", "is", "über",
        "naïve", "café", "x", "42", "2026", "generously", "connection", "connected",
        "and", "or", "not", "quick", "brown", "fox", "jumps", "lazy", "dogs",
        "caresses", "ponies", "agreed", "plastered", "motoring", "hopping",
    ];

    fn document(rng: &mut Lcg) -> String {
        let n = (rng.next() % 40) as usize;
        let seps = [" ", ", ", ". ", "-", "  ", "\n", "!", "; "];
        let mut out = String::new();
        for _ in 0..n {
            out.push_str(rng.pick(VOCAB));
            out.push_str(rng.pick(&seps));
        }
        out
    }

    fn query(rng: &mut Lcg, depth: u32) -> String {
        let leaf = |rng: &mut Lcg| -> String {
            let w = rng.pick(VOCAB).to_string();
            match rng.next() % 3 {
                0 => format!("'{}'", w.replace('\'', "''")),
                _ => w.replace('\'', ""),
            }
        };
        if depth == 0 || rng.next() % 3 == 0 {
            return leaf(rng);
        }
        match rng.next() % 6 {
            0 => format!("{} & {}", query(rng, depth - 1), query(rng, depth - 1)),
            1 => format!("{} | {}", query(rng, depth - 1), query(rng, depth - 1)),
            2 => format!("!{}", query(rng, depth - 1)),
            3 => format!("( {} )", query(rng, depth - 1)),
            4 => format!("{} <-> {}", query(rng, depth - 1), query(rng, depth - 1)),
            // Malformed now and then: both must answer "no match".
            _ => format!("{} &", query(rng, depth - 1)),
        }
    }

    #[test]
    fn the_rewrite_agrees_with_the_reference() {
        let mut rng = Lcg(0x7e57);
        let mut matched = 0;
        for _ in 0..3000 {
            let doc = document(&mut rng);
            for config in ["english", "simple"] {
                assert_eq!(
                    to_tsvector(config, &doc),
                    reference::to_tsvector(config, &doc),
                    "{config}: {doc:?}"
                );
            }
            let vector = to_tsvector("english", &doc);
            let raw = query(&mut rng, 4);
            // Both the canonical form a query reaches `@@` as, and raw text.
            let mut forms = vec![raw.clone()];
            if let Ok(q) = to_tsquery("english", &raw) {
                forms.push(q);
            }
            for q in forms {
                let got = matches(&vector, &q);
                assert_eq!(got, reference::matches(&vector, &q), "{vector:?} @@ {q:?}");
                matched += usize::from(got);
            }
        }
        // Only worth something if both answers occur.
        assert!(matched > 300, "only {matched} matches");
    }

    /// The in-place stemmer against the one it replaced, over a large word
    /// list: every word the document generator uses, a dictionary-like set
    /// built from roots and the suffixes every Porter step tests, random
    /// letter strings, and Unicode.
    #[test]
    fn the_in_place_stemmer_is_byte_identical() {
        let roots = [
            "relat", "condition", "nation", "connect", "general", "hope", "sing", "run",
            "plaster", "agre", "feed", "bled", "motor", "size", "fil", "fail", "troubl",
            "happ", "sky", "cr", "formal", "sensit", "electr", "adjust", "depend",
            "valen", "hesit", "digit", "effect", "posit", "bomb", "gyroscop", "communism",
            "irrit", "homolog", "analog", "control", "roll", "fall", "cease", "tann",
            "rate", "probat", "trav", "convey", "employ", "boy", "say", "y", "yy", "ay",
            "über", "naïv", "café", "straß", "ünï", "日本", "ðe", "æsthet", "yo", "ye",
        ];
        let suffixes = [
            "", "s", "es", "sses", "ies", "ss", "eed", "ed", "ing", "at", "bl", "iz", "y",
            "ational", "tional", "enci", "anci", "izer", "bli", "alli", "entli", "eli",
            "ousli", "ization", "ation", "ator", "alism", "iveness", "fulness", "ousness",
            "aliti", "iviti", "biliti", "logi", "icate", "ative", "alize", "iciti", "ical",
            "ful", "ness", "al", "ance", "ence", "er", "ic", "able", "ible", "ant",
            "ement", "ment", "ent", "ou", "ism", "ate", "iti", "ous", "ive", "ize", "ion",
            "sion", "tion", "e", "ll", "ly", "ings", "edly", "ingly", "ers",
        ];
        let mut words: Vec<String> = Vec::new();
        for r in roots {
            for a in suffixes {
                words.push(format!("{r}{a}"));
                for b in ["", "s", "ed", "ing", "e", "y", "ness", "al"] {
                    words.push(format!("{r}{a}{b}"));
                }
            }
        }
        for w in VOCAB {
            words.push(w.to_lowercase());
        }
        let mut rng = Lcg(0xabc);
        let letters = ['a', 'e', 'i', 'o', 'u', 'y', 's', 't', 'n', 'l', 'b', 'z', 'c', 'd', 'g', 'w', 'x', 'é', 'ß'];
        for _ in 0..100_000 {
            let n = 1 + (rng.next() % 12) as usize;
            words.push((0..n).map(|_| letters[(rng.next() as usize) % letters.len()]).collect());
        }
        for _ in 0..2000 {
            for w in words_of(&document(&mut rng)) {
                words.push(w);
            }
        }
        assert!(words.len() > 150_000, "{}", words.len());
        for w in &words {
            assert_eq!(stem(w), stem_reference::stem(w), "{w:?}");
        }
    }

    fn words_of(doc: &str) -> Vec<String> {
        super::words(doc).into_iter().map(|w| w.to_lowercase()).collect()
    }

    /// A query cached by its text answers for that text, not for whatever
    /// else later occupies the same memory.
    #[test]
    fn the_cache_is_keyed_by_content() {
        let v = to_tsvector("english", "urgent fragile delivery");
        for _ in 0..3 {
            let a = String::from("'urgent' & 'deliveri'");
            assert!(matches(&v, &a));
            drop(a);
            let b = String::from("'urgent' & 'missing'");
            assert!(!matches(&v, &b));
        }
    }

    /// `@@` and `to_tsvector` over 100k rows, the rewrite against the
    /// reference. `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// text_search_bench --nocapture`
    #[test]
    #[ignore]
    fn text_search_bench() {
        let mut rng = Lcg(42);
        let docs: Vec<String> = (0..100_000).map(|_| document(&mut rng)).collect();
        let vectors: Vec<String> = docs.iter().map(|d| to_tsvector("english", d)).collect();
        let q = to_tsquery("english", "(urgent | fragile) & !lazy & deliveri").unwrap();
        for round in 0..3 {
            let t = std::time::Instant::now();
            let old: usize = vectors.iter().filter(|v| reference::matches(v, &q)).count();
            let old_t = t.elapsed();
            let t = std::time::Instant::now();
            let new: usize = vectors.iter().filter(|v| matches(v, &q)).count();
            let new_t = t.elapsed();
            assert_eq!(old, new);
            let t = std::time::Instant::now();
            let a: usize = docs.iter().map(|d| reference::to_tsvector("english", d).len()).sum();
            let old_v = t.elapsed();
            let t = std::time::Instant::now();
            let b: usize = docs.iter().map(|d| to_tsvector("english", d).len()).sum();
            let new_v = t.elapsed();
            assert_eq!(a, b);
            println!(
                "round {round}: @@ over 100k rows {old_t:?} -> {new_t:?} ({new} matches); to_tsvector over 100k docs {old_v:?} -> {new_v:?}"
            );
        }
    }
}
