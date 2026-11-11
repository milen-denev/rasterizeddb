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
//! The stemmer is Snowball's English (Porter2), as PostgreSQL's `english`
//! configuration's is; it agrees with PostgreSQL 17 on every word of a
//! 63,748-word dictionary. The stop-word list is PostgreSQL's
//! `english.stop`. The word splitter is simpler than PostgreSQL's parser,
//! which also emits a hyphenated word's parts and splits at an apostrophe.
//! A configuration name other than `english` or `simple` is accepted and
//! treated as `english`, because refusing it would fail a query over a
//! distinction this cannot yet act on.

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
        // `simple` has no stop words: `to_tsvector('simple', 'the fox')`
        // keeps `the`.
        if stem_it && is_stop_word(&lower) {
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
    Ok(node.map(|n| render(&n)).unwrap_or_default())
}

/// The words of `text` normalised as a configuration does them, each with
/// its position (one-based, a stop word still counting one): `None` for a
/// stop word, which `english` drops and `simple` keeps.
fn normalised_words(configuration: &str, text: &str) -> Vec<Option<String>> {
    let stem_it = stems(configuration);
    words(text)
        .into_iter()
        .map(|w| w.to_lowercase())
        .map(|w| {
            if stem_it && is_stop_word(&w) {
                None
            } else if stem_it {
                Some(stem(&w)).filter(|s| !s.is_empty())
            } else {
                Some(w)
            }
        })
        .collect()
}

/// `plainto_tsquery`: every word, `AND`ed, with no operator syntax.
pub fn plainto_tsquery(configuration: &str, text: &str) -> String {
    let terms: Vec<Node> = normalised_words(configuration, text).into_iter().flatten().map(Node::Term).collect();
    terms.into_iter().reduce(|a, b| Node::And(Box::new(a), Box::new(b))).map(|n| render(&n)).unwrap_or_default()
}

/// `phraseto_tsquery`: every word, in order, each `<N>` the next, `N` the
/// distance between them (a stop word in between counts, as PostgreSQL's
/// does: `quick the fox` is `'quick' <2> 'fox'`).
pub fn phraseto_tsquery(configuration: &str, text: &str) -> String {
    phrase_of(normalised_words(configuration, text)).map(|n| render(&n)).unwrap_or_default()
}

/// The phrase over `words` (a stop word `None`), as [`phraseto_tsquery`]
/// builds it.
fn phrase_of(words: Vec<Option<String>>) -> Option<Node> {
    let mut out: Option<Node> = None;
    let mut gap = 0u16;
    for word in words {
        match word {
            None => gap = gap.saturating_add(1),
            Some(term) => {
                out = Some(match out {
                    None => Node::Term(term),
                    Some(left) => Node::Followed(Box::new(left), Box::new(Node::Term(term)), gap + 1),
                });
                gap = 0;
            }
        }
    }
    out
}

/// `websearch_to_tsquery`: the search-box syntax. Words are `AND`ed, a
/// quoted run is a phrase, `or` between two parts is `|`, and a leading `-`
/// negates what it is written on; other punctuation is only a separator.
pub fn websearch_to_tsquery(configuration: &str, text: &str) -> String {
    // The parts, in order: (negated, the part), and `or` between them.
    enum Piece {
        Part(bool, Option<Node>),
        Or,
    }
    let mut pieces: Vec<Piece> = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let negated = c == '-' && chars.get(i + 1).is_some_and(|n| !n.is_whitespace());
        if negated {
            i += 1;
        }
        if chars.get(i) == Some(&'"') {
            let start = i + 1;
            let end = (start..chars.len()).find(|&j| chars[j] == '"').unwrap_or(chars.len());
            let quoted: String = chars[start..end].iter().collect();
            pieces.push(Piece::Part(negated, phrase_of(normalised_words(configuration, &quoted))));
            i = end + 1;
            continue;
        }
        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '"' {
            i += 1;
        }
        let word: String = chars[start..i].iter().collect();
        if !negated && word.eq_ignore_ascii_case("or") {
            pieces.push(Piece::Or);
            continue;
        }
        // Punctuation splits a word into parts that are a phrase, as a
        // hyphenated word's are.
        pieces.push(Piece::Part(negated, phrase_of(normalised_words(configuration, &word))));
    }
    // `&` binds tighter than `|`: runs of parts joined by `or`.
    let mut alternatives: Vec<Node> = Vec::new();
    let mut current: Option<Node> = None;
    let mut pending_or = false;
    for piece in pieces {
        match piece {
            Piece::Or => pending_or = current.is_some(),
            Piece::Part(negated, Some(node)) => {
                let node = if negated { Node::Not(Box::new(node)) } else { node };
                if pending_or {
                    alternatives.extend(current.take());
                    pending_or = false;
                }
                current = Some(match current {
                    None => node,
                    Some(left) => Node::And(Box::new(left), Box::new(node)),
                });
            }
            Piece::Part(_, None) => {}
        }
    }
    alternatives.extend(current);
    alternatives.into_iter().reduce(|a, b| Node::Or(Box::new(a), Box::new(b))).map(|n| render(&n)).unwrap_or_default()
}

/// `tsvector @@ tsquery`.
///
/// Both operands are the canonical text the functions above produce. A query
/// that cannot be parsed matches nothing rather than erroring, because by the
/// time the two meet the query has already been through [`to_tsquery`], which
/// is where a malformed one is reported. An empty query (only stop words)
/// matches nothing, as PostgreSQL's does.
pub fn matches(vector: &str, query: &str) -> bool {
    // The query is compiled once and reused for every row of the statement:
    // it used to be tokenized and parsed again per row.
    let Some(compiled) = compiled_query(query) else {
        return false;
    };
    // One pass over the vector collects where each of the query's terms
    // occurs: the positions a phrase (`<->`) is tested against.
    let mut found: smallvec::SmallVec<[smallvec::SmallVec<[u32; 4]>; 16]> =
        smallvec::smallvec![smallvec::SmallVec::new(); compiled.terms.len()];
    for entry in vector.split_whitespace() {
        let entry = entry.trim();
        let Some(body) = entry.strip_prefix('\'') else {
            continue;
        };
        let Some(close) = body.rfind('\'') else {
            continue;
        };
        let lexeme = &body[..close];
        // `:1,3` (a weight letter after a position is ignored here).
        let positions: smallvec::SmallVec<[u32; 4]> = body[close + 1..]
            .strip_prefix(':')
            .map(|p| p.split(',').filter_map(|n| n.trim_end_matches(|c: char| c.is_ascii_alphabetic()).parse().ok()).collect())
            .unwrap_or_default();
        let lexeme = if lexeme.contains("''") { std::borrow::Cow::Owned(lexeme.replace("''", "'")) } else { std::borrow::Cow::Borrowed(lexeme) };
        compiled.mark(&lexeme, &positions, &mut found);
    }
    compiled.evaluate(&compiled.root, &found)
}

/// PostgreSQL's default `ts_rank` weights, for weight classes D, C, B, A.
pub const DEFAULT_RANK_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];

/// `ts_rank([weights,] vector, query [, normalization])`, as PostgreSQL's
/// `calc_rank` computes it (`tsrank.c`), in the same single- and
/// double-precision steps, so the `real` it returns prints the same digits.
/// A position here carries no weight class, so each is class D.
pub fn ts_rank(weights: [f32; 4], vector: &str, query: &str, method: i32) -> f32 {
    // The vector's lexemes, in order, with their positions.
    let mut lexemes: Vec<(String, Vec<i32>)> = Vec::new();
    for entry in vector.split_whitespace() {
        let Some(body) = entry.strip_prefix('\'') else { continue };
        let Some(close) = body.rfind('\'') else { continue };
        let lexeme = body[..close].replace("''", "'");
        let positions = body[close + 1..]
            .strip_prefix(':')
            .map(|p| p.split(',').filter_map(|n| n.trim_end_matches(|c: char| c.is_ascii_alphabetic()).parse().ok()).collect())
            .unwrap_or_default();
        lexemes.push((lexeme, positions));
    }
    let Ok(Some(root)) = parse_query("simple", query) else { return 0.0 };
    if lexemes.is_empty() {
        return 0.0;
    }
    // The query's operands, sorted bytewise and each once (`SortAndUniqItems`).
    let mut items: Vec<(String, bool)> = Vec::new();
    fn collect(node: &Node, out: &mut Vec<(String, bool)>) {
        match node {
            Node::Term(t) => out.push((t.clone(), false)),
            Node::Prefix(t) => out.push((t.clone(), true)),
            Node::Not(n) => collect(n, out),
            Node::And(a, b) | Node::Or(a, b) | Node::Followed(a, b, _) => {
                collect(a, out);
                collect(b, out);
            }
        }
    }
    collect(&root, &mut items);
    items.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    items.dedup_by(|a, b| a.0 == b.0);
    // The vector's entries an operand finds: the one equal to it, or for a
    // prefix every one it begins.
    let found = |item: &(String, bool)| -> Vec<&Vec<i32>> {
        lexemes
            .iter()
            .filter(|(l, _)| if item.1 { l.starts_with(item.0.as_str()) } else { *l == item.0 })
            .map(|(_, p)| p)
            .collect()
    };
    // Every position is class D.
    let wpos = weights[0];

    let and = matches!(root, Node::And(..) | Node::Followed(..));
    let mut res: f32 = if and && items.len() >= 2 {
        // `calc_rank_and`.
        fn word_distance(w: i32) -> f32 {
            if w > 100 {
                return 1e-30;
            }
            (1.0f64 / (1.005 + 0.05 * ((w as f32) as f64 / 1.5 - 2.0).exp())) as f32
        }
        let mut res: f32 = -1.0;
        let mut pos: Vec<Option<&Vec<i32>>> = vec![None; items.len()];
        for i in 0..items.len() {
            for entry in found(&items[i]) {
                pos[i] = Some(entry);
                for k in 0..i {
                    let Some(ct) = pos[k] else { continue };
                    for &l in entry {
                        for &p in ct {
                            let dist = (l - p).abs();
                            if dist != 0 {
                                let curw = ((wpos * wpos * word_distance(dist)) as f64).sqrt() as f32;
                                res = if res < 0.0 { curw } else { (1.0 - (1.0 - res as f64) * (1.0 - curw as f64)) as f32 };
                            }
                        }
                    }
                }
            }
        }
        res
    } else {
        // `calc_rank_or`.
        let mut res: f32 = 0.0;
        for item in &items {
            for entry in found(item) {
                let (mut resj, mut wjm, mut jm): (f32, f32, i32) = (0.0, -1.0, 0);
                for j in 0..entry.len() as i32 {
                    resj += wpos / ((j + 1) * (j + 1)) as f32;
                    if wpos > wjm {
                        wjm = wpos;
                        jm = j;
                    }
                }
                let inner: f32 = wjm + resj - wjm / ((jm + 1) * (jm + 1)) as f32;
                res = (res as f64 + inner as f64 / 1.64493406685) as f32;
            }
        }
        if !items.is_empty() {
            res /= items.len() as f32;
        }
        res
    };
    if res < 0.0 {
        res = 1e-20;
    }
    let length: i32 = lexemes.iter().map(|(_, p)| (p.len() as i32).max(1)).sum();
    let size = lexemes.len() as i32;
    if method & 0x01 != 0 && size > 0 {
        res = (res as f64 / (((length + 1) as f64).ln() / 2f64.ln())) as f32;
    }
    if method & 0x02 != 0 && length > 0 {
        res /= length as f32;
    }
    if method & 0x08 != 0 && size > 0 {
        res /= size as f32;
    }
    if method & 0x10 != 0 && size > 0 {
        res = (res as f64 / (((size + 1) as f64).ln() / 2f64.ln())) as f32;
    }
    if method & 0x20 != 0 {
        res /= res + 1.0;
    }
    res
}

/// A tsquery parsed once: its distinct terms, numbered, and the tree over
/// those numbers.
#[derive(Debug)]
struct CompiledQuery {
    /// Each term, and whether it is a prefix (`'qui':*`).
    terms: Vec<(String, bool)>,
    /// Exact term to number, for a query too long to search linearly.
    index: Option<ahash::HashMap<String, usize>>,
    root: CNode,
}

#[derive(Debug)]
enum CNode {
    Term(usize),
    Not(Box<CNode>),
    And(Box<CNode>, Box<CNode>),
    Or(Box<CNode>, Box<CNode>),
    /// `a <N> b`: `b` starting `N` positions after `a` ends.
    Followed(Box<CNode>, Box<CNode>, u16),
}

impl CompiledQuery {
    fn compile(node: &Node) -> CompiledQuery {
        fn walk(node: &Node, terms: &mut Vec<(String, bool)>) -> CNode {
            let mut term = |t: &String, prefix: bool| match terms.iter().position(|(x, p)| x == t && *p == prefix) {
                Some(at) => CNode::Term(at),
                None => {
                    terms.push((t.clone(), prefix));
                    CNode::Term(terms.len() - 1)
                }
            };
            match node {
                Node::Term(t) => term(t, false),
                Node::Prefix(t) => term(t, true),
                Node::Not(inner) => CNode::Not(Box::new(walk(inner, terms))),
                Node::And(a, b) => CNode::And(Box::new(walk(a, terms)), Box::new(walk(b, terms))),
                Node::Or(a, b) => CNode::Or(Box::new(walk(a, terms)), Box::new(walk(b, terms))),
                Node::Followed(a, b, d) => CNode::Followed(Box::new(walk(a, terms)), Box::new(walk(b, terms)), *d),
            }
        }
        let mut terms = Vec::new();
        let root = walk(node, &mut terms);
        let index = (terms.len() > 8).then(|| {
            terms.iter().enumerate().filter(|(_, (_, p))| !p).map(|(i, (t, _))| (t.clone(), i)).collect()
        });
        CompiledQuery { terms, index, root }
    }

    #[inline]
    fn mark(&self, lexeme: &str, positions: &[u32], found: &mut [smallvec::SmallVec<[u32; 4]>]) {
        // A lexeme with no positions still occurs: it holds position 0, which
        // no phrase can follow from but presence reads.
        let mut add = |at: usize| {
            if positions.is_empty() {
                found[at].push(0);
            } else {
                found[at].extend_from_slice(positions);
            }
        };
        match &self.index {
            Some(index) => {
                if let Some(&at) = index.get(lexeme) {
                    add(at);
                }
            }
            None => {}
        }
        for (at, (t, prefix)) in self.terms.iter().enumerate() {
            if (*prefix && lexeme.starts_with(t.as_str())) || (!prefix && self.index.is_none() && t == lexeme) {
                add(at);
            }
        }
    }

    fn evaluate(&self, node: &CNode, found: &[smallvec::SmallVec<[u32; 4]>]) -> bool {
        match node {
            CNode::Term(at) => !found[*at].is_empty(),
            CNode::Not(inner) => !self.evaluate(inner, found),
            CNode::And(a, b) => self.evaluate(a, found) && self.evaluate(b, found),
            CNode::Or(a, b) => self.evaluate(a, found) || self.evaluate(b, found),
            CNode::Followed(..) => match self.spans(node, found) {
                Some(spans) => !spans.is_empty(),
                // A phrase over `&` or `!`, which this does not place: both
                // sides present, weaker than PostgreSQL's test.
                None => self.both_present(node, found),
            },
        }
    }

    fn both_present(&self, node: &CNode, found: &[smallvec::SmallVec<[u32; 4]>]) -> bool {
        match node {
            CNode::Followed(a, b, _) => self.evaluate(a, found) && self.evaluate(b, found),
            other => self.evaluate(other, found),
        }
    }

    /// The (first, last) positions where `node` occurs, for a phrase: `None`
    /// for a node this does not place (`&`, `!`).
    fn spans(&self, node: &CNode, found: &[smallvec::SmallVec<[u32; 4]>]) -> Option<Vec<(u32, u32)>> {
        Some(match node {
            CNode::Term(at) => found[*at].iter().filter(|&&p| p > 0).map(|&p| (p, p)).collect(),
            CNode::Or(a, b) => {
                let mut all = self.spans(a, found)?;
                all.extend(self.spans(b, found)?);
                all
            }
            CNode::Followed(a, b, d) => {
                let (left, right) = (self.spans(a, found)?, self.spans(b, found)?);
                let mut out = Vec::new();
                for &(ls, le) in &left {
                    for &(rs, re) in &right {
                        if rs == le + u32::from(*d) {
                            out.push((ls, re));
                        }
                    }
                }
                out
            }
            CNode::And(..) | CNode::Not(..) => return None,
        })
    }
}

/// The compiled form of a query text, from a per-thread cache keyed by the
/// text itself — never by its address, which a freed and reissued buffer can
/// share with a different query. `None` for a query that does not parse, or
/// that is empty.
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
            .flatten()
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
    /// `'qui':*`: any lexeme beginning with it.
    Prefix(String),
    Not(Box<Node>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    /// `a <N> b` (`<->` is `<1>`): `b` begins `N` positions after `a` ends.
    Followed(Box<Node>, Box<Node>, u16),
}

/// The per-row evaluation as it was, kept as the reference for the
/// differential test (a phrase as both sides present).
#[cfg(test)]
fn evaluate(node: &Node, present: &[String]) -> bool {
    match node {
        Node::Term(t) => present.iter().any(|p| p == t),
        Node::Prefix(t) => present.iter().any(|p| p.starts_with(t.as_str())),
        Node::Not(inner) => !evaluate(inner, present),
        Node::And(a, b) | Node::Followed(a, b, _) => evaluate(a, present) && evaluate(b, present),
        Node::Or(a, b) => evaluate(a, present) || evaluate(b, present),
    }
}

/// A query as PostgreSQL prints it: an operand in parentheses only where
/// its operator binds looser than the one over it (`!` tightest, then the
/// phrase operators, `&`, `|`), and a phrase on a phrase's right, which
/// does not regroup.
fn render(node: &Node) -> String {
    fn priority(node: &Node) -> u8 {
        match node {
            Node::Term(_) | Node::Prefix(_) => 5,
            Node::Not(_) => 4,
            Node::Followed(..) => 3,
            Node::And(..) => 2,
            Node::Or(..) => 1,
        }
    }
    fn child(parent: &Node, node: &Node, right_of_phrase: bool) -> String {
        let text = render(node);
        let phrase_right = right_of_phrase && matches!(node, Node::Followed(..));
        if priority(node) < priority(parent) || phrase_right {
            format!("( {text} )")
        } else {
            text
        }
    }
    match node {
        Node::Term(t) => format!("'{}'", t.replace('\'', "''")),
        Node::Prefix(t) => format!("'{}':*", t.replace('\'', "''")),
        Node::Not(inner) => format!("!{}", child(node, inner, false)),
        Node::And(a, b) => format!("{} & {}", child(node, a, false), child(node, b, false)),
        Node::Or(a, b) => format!("{} | {}", child(node, a, false), child(node, b, false)),
        Node::Followed(a, b, d) => {
            let op = if *d == 1 { "<->".to_string() } else { format!("<{d}>") };
            format!("{} {op} {}", child(node, a, false), child(node, b, true))
        }
    }
}

struct Parser<'a> {
    tokens: Vec<String>,
    at: usize,
    configuration: &'a str,
}

/// A parsed query with its stop words taken out (`Ok(None)` when nothing is
/// left), or why it does not parse.
fn parse_query(configuration: &str, query: &str) -> Result<Option<Node>, String> {
    let mut parser = Parser {
        tokens: tokenize_query(query),
        at: 0,
        configuration,
    };
    let node = parser.parse_or()?;
    if parser.at != parser.tokens.len() {
        return Err(format!("syntax error in tsquery: '{}'", query));
    }
    Ok(simplify(node).node)
}

/// A node with its stop words out, and what a phrase around it must still
/// count for them: the positions before its first word (`lead`) and after
/// its last (`trail`), or, when nothing is left, how many it spans.
struct Simplified {
    node: Option<Node>,
    lead: u16,
    trail: u16,
}

/// The parse's stand-in for a stop word.
const STOP: &str = "\u{0}stop";

fn simplify(node: Node) -> Simplified {
    let some = |node: Node| Simplified { node: Some(node), lead: 0, trail: 0 };
    let none = |span: u16| Simplified { node: None, lead: span, trail: span };
    match node {
        Node::Term(t) if t == STOP => none(0),
        Node::Term(_) | Node::Prefix(_) => some(node),
        Node::Not(inner) => match simplify(*inner).node {
            Some(n) => some(Node::Not(Box::new(n))),
            None => none(0),
        },
        Node::And(a, b) => match (simplify(*a).node, simplify(*b).node) {
            (Some(x), Some(y)) => some(Node::And(Box::new(x), Box::new(y))),
            (Some(x), None) | (None, Some(x)) => some(x),
            (None, None) => none(0),
        },
        Node::Or(a, b) => match (simplify(*a).node, simplify(*b).node) {
            (Some(x), Some(y)) => some(Node::Or(Box::new(x), Box::new(y))),
            (Some(x), None) | (None, Some(x)) => some(x),
            (None, None) => none(0),
        },
        // A stop word in a phrase still holds its place: `quick <-> the <->
        // fox` is `'quick' <2> 'fox'`.
        Node::Followed(a, b, d) => {
            let (l, r) = (simplify(*a), simplify(*b));
            match (l.node, r.node) {
                (Some(x), Some(y)) => Simplified {
                    node: Some(Node::Followed(Box::new(x), Box::new(y), l.trail + d + r.lead)),
                    lead: l.lead,
                    trail: r.trail,
                },
                (Some(x), None) => Simplified { node: Some(x), lead: l.lead, trail: l.trail + d + r.lead },
                (None, Some(y)) => Simplified { node: Some(y), lead: l.lead + d + r.lead, trail: r.trail },
                (None, None) => none(l.lead + d + r.lead),
            }
        }
    }
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
            // Its `:*` / weights, which follow the closing quote.
            let mut suffix = String::new();
            if bytes.get(i) == Some(&':') {
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j] == '*' || bytes[j].is_ascii_alphabetic()) {
                    j += 1;
                }
                suffix = bytes[i..j].iter().collect();
                i = j;
            }
            out.push(format!("'{}{}", text, if suffix.is_empty() { String::new() } else { format!("\u{0}{suffix}") }));
        } else if c == '<'
            && let Some(close) = (i + 1..bytes.len()).find(|&j| bytes[j] == '>')
            && {
                let inside: String = bytes[i + 1..close].iter().collect();
                inside == "-" || (!inside.is_empty() && inside.bytes().all(|b| b.is_ascii_digit()))
            }
        {
            out.push(bytes[i..=close].iter().collect());
            i = close + 1;
        } else if matches!(c, '&' | '|' | '!' | '(' | ')') {
            out.push(c.to_string());
            i += 1;
        } else {
            let start = i;
            while i < bytes.len()
                && !bytes[i].is_whitespace()
                && !matches!(bytes[i], '&' | '|' | '!' | '(' | ')' | '<')
            {
                i += 1;
            }
            out.push(bytes[start..i].iter().collect());
        }
    }
    out
}

/// A phrase operator token's distance: `<->` is 1, `<N>` is `N`.
fn phrase_distance(token: &str) -> Option<u16> {
    let inside = token.strip_prefix('<')?.strip_suffix('>')?;
    if inside == "-" {
        return Some(1);
    }
    inside.parse().ok()
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
        let mut left = self.parse_phrase()?;
        while self.peek() == Some("&") {
            self.at += 1;
            let right = self.parse_phrase()?;
            left = Node::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `<->` and `<N>` bind tighter than `&`, as PostgreSQL's do.
    fn parse_phrase(&mut self) -> Result<Node, String> {
        let mut left = self.parse_not()?;
        while let Some(d) = self.peek().and_then(phrase_distance) {
            self.at += 1;
            let right = self.parse_not()?;
            left = Node::Followed(Box::new(left), Box::new(right), d);
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
                if matches!(token.as_str(), "&" | "|" | ")") || phrase_distance(&token).is_some() {
                    return Err(format!("tsquery operator '{}' has no operand", token));
                }
                // A token the tokenizer quoted is already a lexeme; its
                // suffix (`:*`, weights) rides after a NUL.
                let (raw, quoted, suffix) = match token.strip_prefix('\'') {
                    Some(rest) => match rest.split_once('\u{0}') {
                        Some((text, suffix)) => (text.to_string(), true, suffix.to_string()),
                        None => (rest.to_string(), true, String::new()),
                    },
                    None => match token.rfind(':') {
                        Some(at) if token[at + 1..].chars().all(|c| c == '*' || c.is_ascii_alphabetic()) && at > 0 => {
                            (token[..at].to_string(), false, token[at..].to_string())
                        }
                        _ => (token, false, String::new()),
                    },
                };
                let prefix = suffix.contains('*');
                if suffix.chars().any(|c| c.is_ascii_alphabetic()) {
                    return Err(format!("tsquery weights ('{suffix}') are not supported yet"));
                }
                let lower = raw.to_lowercase();
                let text = if quoted {
                    lower
                } else if !stems(self.configuration) {
                    lower
                } else if is_stop_word(&lower) {
                    // Taken out after the parse, its place in a phrase kept.
                    STOP.to_string()
                } else {
                    stem(&lower)
                };
                Ok(if prefix && text != STOP { Node::Prefix(text) } else { Node::Term(text) })
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

// ---------------------------------------------------- Snowball English stemmer

/// Snowball's English stemmer (Porter2), as PostgreSQL's `english`
/// configuration runs it (`english_stem`, from `english.sbl`).
///
/// It was the classic Porter algorithm, which PostgreSQL does not use: `day`
/// stemmed to `dai` where PostgreSQL's is `day`, `generously` to `gener` where
/// its is `generous`. A vector built here then held lexemes a query built by
/// PostgreSQL (or a client that stems as it does) never names.
///
/// A word with a digit in it is returned unchanged: PostgreSQL's parser makes
/// it a `numword`, which `english` does not stem.
pub fn stem(word: &str) -> String {
    if word.chars().any(|c| c.is_numeric()) {
        return word.to_string();
    }
    porter2::stem(word)
}

mod porter2 {
    fn is_vowel(c: char) -> bool {
        matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
    }

    fn ends(w: &[char], suffix: &str) -> bool {
        let s: Vec<char> = suffix.chars().collect();
        w.len() >= s.len() && w[w.len() - s.len()..] == s[..]
    }

    /// The longest of `suffixes` `w` ends with.
    fn longest<'a>(w: &[char], suffixes: &[&'a str]) -> Option<&'a str> {
        suffixes.iter().copied().filter(|s| ends(w, s)).max_by_key(|s| s.chars().count())
    }

    fn replace(w: &mut Vec<char>, suffix: &str, with: &str) {
        w.truncate(w.len() - suffix.chars().count());
        w.extend(with.chars());
    }

    /// Whether `w[..end]` ends in a short syllable: a vowel between two
    /// non-vowels, the last not `w`, `x` or `Y`; or, as the whole word, a
    /// vowel then a non-vowel.
    fn short_syllable(w: &[char], end: usize) -> bool {
        (end >= 3
            && !is_vowel(w[end - 3])
            && is_vowel(w[end - 2])
            && !is_vowel(w[end - 1])
            && !matches!(w[end - 1], 'w' | 'x' | 'Y'))
            || (end == 2 && is_vowel(w[0]) && !is_vowel(w[1]))
    }

    /// Where the region after the first non-vowel following a vowel, at or
    /// after `from`, begins; the word's length when there is none.
    fn region_after(w: &[char], from: usize) -> usize {
        (from + 1..w.len()).find(|&i| is_vowel(w[i - 1]) && !is_vowel(w[i])).map_or(w.len(), |i| i + 1)
    }

    pub(super) fn stem(word: &str) -> String {
        // Exceptional forms, whole words.
        match word {
            "skis" => return "ski".into(),
            "skies" => return "sky".into(),
            "dying" => return "die".into(),
            "lying" => return "lie".into(),
            "tying" => return "tie".into(),
            "idly" => return "idl".into(),
            "gently" => return "gentl".into(),
            "ugly" => return "ugli".into(),
            "early" => return "earli".into(),
            "only" => return "onli".into(),
            "singly" => return "singl".into(),
            "sky" | "news" | "howe" | "atlas" | "cosmos" | "bias" | "andes" => return word.into(),
            _ => {}
        }
        let mut w: Vec<char> = word.chars().collect();
        if w.len() < 3 {
            return word.into();
        }
        // Prelude: an initial apostrophe goes; a `y` that begins the word or
        // follows a vowel is a consonant, `Y`.
        if w.first() == Some(&'\'') {
            w.remove(0);
        }
        for i in 0..w.len() {
            if w[i] == 'y' && (i == 0 || is_vowel(w[i - 1])) {
                w[i] = 'Y';
            }
        }
        // The regions.
        let lower: String = w.iter().collect();
        let p1 = ["gener", "commun", "arsen"]
            .iter()
            .find(|p| lower.starts_with(*p))
            .map_or_else(|| region_after(&w, 0), |p| p.len());
        let p2 = region_after(&w, p1);
        let in_r1 = |w: &Vec<char>, suffix: &str| w.len() - suffix.chars().count() >= p1;
        let in_r2 = |w: &Vec<char>, suffix: &str| w.len() - suffix.chars().count() >= p2;

        // Step 1a.
        if let Some(s) = longest(&w, &["'s'", "'s", "'"]) {
            replace(&mut w, s, "");
        }
        match longest(&w, &["sses", "ied", "ies", "s", "us", "ss"]) {
            Some("sses") => replace(&mut w, "sses", "ss"),
            Some(s @ ("ied" | "ies")) => {
                let with = if w.len() - 3 > 1 { "i" } else { "ie" };
                replace(&mut w, s, with);
            }
            Some("s") => {
                // Deleted when a vowel comes before the letter before it.
                if w.len() >= 2 && w[..w.len() - 2].iter().any(|&c| is_vowel(c)) {
                    w.pop();
                }
            }
            _ => {}
        }
        let now: String = w.iter().collect();
        if !matches!(
            now.as_str(),
            "inning" | "outing" | "canning" | "herring" | "earring" | "proceed" | "exceed" | "succeed"
        ) {
            // Step 1b.
            match longest(&w, &["eed", "eedly", "ed", "edly", "ing", "ingly"]) {
                Some(s @ ("eed" | "eedly")) => {
                    if in_r1(&w, s) {
                        replace(&mut w, s, "ee");
                    }
                }
                Some(s) => {
                    let stem_len = w.len() - s.chars().count();
                    if w[..stem_len].iter().any(|&c| is_vowel(c)) {
                        w.truncate(stem_len);
                        if ends(&w, "at") || ends(&w, "bl") || ends(&w, "iz") {
                            w.push('e');
                        } else if ["bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"].iter().any(|d| ends(&w, d)) {
                            w.pop();
                        } else if w.len() == p1 && short_syllable(&w, w.len()) {
                            w.push('e');
                        }
                    }
                }
                None => {}
            }
            // Step 1c.
            let n = w.len();
            if n >= 3 && matches!(w[n - 1], 'y' | 'Y') && !is_vowel(w[n - 2]) {
                w[n - 1] = 'i';
            }
            // Step 2.
            const STEP_2: &[(&str, &str)] = &[
                ("tional", "tion"),
                ("enci", "ence"),
                ("anci", "ance"),
                ("abli", "able"),
                ("entli", "ent"),
                ("izer", "ize"),
                ("ization", "ize"),
                ("ational", "ate"),
                ("ation", "ate"),
                ("ator", "ate"),
                ("alism", "al"),
                ("aliti", "al"),
                ("alli", "al"),
                ("fulness", "ful"),
                ("ousli", "ous"),
                ("ousness", "ous"),
                ("iveness", "ive"),
                ("iviti", "ive"),
                ("biliti", "ble"),
                ("bli", "ble"),
                ("ogi", "og"),
                ("fulli", "ful"),
                ("lessli", "less"),
                ("li", ""),
            ];
            let suffixes: Vec<&str> = STEP_2.iter().map(|(s, _)| *s).collect();
            if let Some(s) = longest(&w, &suffixes)
                && in_r1(&w, s)
            {
                let before = w.len() - s.chars().count();
                let preceding = before.checked_sub(1).map(|i| w[i]);
                let with = STEP_2.iter().find(|(x, _)| *x == s).map(|(_, r)| *r).unwrap_or("");
                match s {
                    "ogi" if preceding == Some('l') => replace(&mut w, s, with),
                    "ogi" => {}
                    "li" if matches!(preceding, Some('c' | 'd' | 'e' | 'g' | 'h' | 'k' | 'm' | 'n' | 'r' | 't')) => {
                        replace(&mut w, s, "")
                    }
                    "li" => {}
                    _ => replace(&mut w, s, with),
                }
            }
            // Step 3.
            const STEP_3: &[(&str, &str)] = &[
                ("tional", "tion"),
                ("ational", "ate"),
                ("alize", "al"),
                ("icate", "ic"),
                ("iciti", "ic"),
                ("ical", "ic"),
                ("ful", ""),
                ("ness", ""),
                ("ative", ""),
            ];
            let suffixes: Vec<&str> = STEP_3.iter().map(|(s, _)| *s).collect();
            if let Some(s) = longest(&w, &suffixes)
                && in_r1(&w, s)
            {
                let with = STEP_3.iter().find(|(x, _)| *x == s).map(|(_, r)| *r).unwrap_or("");
                if s != "ative" || in_r2(&w, s) {
                    replace(&mut w, s, with);
                }
            }
            // Step 4.
            const STEP_4: &[&str] = &[
                "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ism", "ate", "iti", "ous",
                "ive", "ize", "ion",
            ];
            if let Some(s) = longest(&w, STEP_4)
                && in_r2(&w, s)
            {
                let before = w.len() - s.chars().count();
                if s != "ion" || before.checked_sub(1).is_some_and(|i| matches!(w[i], 's' | 't')) {
                    replace(&mut w, s, "");
                }
            }
            // Step 5.
            if ends(&w, "e") {
                let end = w.len() - 1;
                if in_r2(&w, "e") || (in_r1(&w, "e") && !short_syllable(&w, end)) {
                    w.pop();
                }
            } else if ends(&w, "l") && in_r2(&w, "l") && w.len() >= 2 && w[w.len() - 2] == 'l' {
                w.pop();
            }
        }
        // Postlude.
        w.iter().map(|&c| if c == 'Y' { 'y' } else { c }).collect()
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
    fn the_stemmer_agrees_with_postgresql_on_common_words() {
        for (word, expected) in [
            // Where the classic Porter stemmer this replaced differed.
            ("day", "day"),
            ("generously", "generous"),
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

    /// The stemmer against PostgreSQL 17's `to_tsvector('english', w)`:
    /// `text_search_pg17_stems.tsv` holds every twentieth word of
    /// `/usr/share/dict/words` (lowercase letters only) and the stemmer's
    /// exceptional forms, as `word<TAB>lexeme` (`\N` for a stop word), the way
    /// `\copy (SELECT w, (SELECT string_agg(lexeme, ' ') FROM
    /// unnest(to_tsvector('english', w))) FROM words) TO …` writes it.
    /// `RDB_PG_STEMS` names a whole such file instead: all 63,748 dictionary
    /// words agreed when this was written.
    #[test]
    fn the_stemmer_agrees_with_postgresql_over_a_dictionary() {
        let text = match std::env::var("RDB_PG_STEMS") {
            Ok(path) => std::fs::read_to_string(path).expect("the stems file"),
            Err(_) => include_str!("text_search_pg17_stems.tsv").to_string(),
        };
        let mut wrong = Vec::new();
        let mut checked = 0;
        for line in text.lines() {
            let Some((word, lexeme)) = line.split_once('\t') else { continue };
            if lexeme == "\\N" {
                continue;
            }
            checked += 1;
            let ours = stem(word);
            if ours != lexeme {
                wrong.push(format!("{word}: ours {ours}, postgres {lexeme}"));
            }
        }
        assert!(checked > 3_000, "only {checked} words read");
        println!("{checked} words agree with PostgreSQL");
        assert!(wrong.is_empty(), "{} of {checked} differ:\n{}", wrong.len(), wrong.iter().take(60).cloned().collect::<Vec<_>>().join("\n"));
    }

    #[test]
    fn a_phrase_is_matched_by_position() {
        let v = to_tsvector("english", "The quick brown fox jumps");
        let q = |t: &str| to_tsquery("english", t).unwrap();
        assert!(matches(&v, &q("quick <-> brown")));
        assert!(!matches(&v, &q("brown <-> quick")));
        assert!(matches(&v, &q("quick <2> fox")));
        assert!(!matches(&v, &q("quick <-> fox")));
        assert!(matches(&v, &q("quick <-> ( brown | red )")));
        assert!(matches(&v, &q("quick <-> brown <-> fox")));
        assert!(matches(&v, &phraseto_tsquery("english", "quick the brown")) == false);
        assert!(matches(&v, &q("qui:* & jum:*")));
        assert!(!matches(&v, &q("cat:*")));
        // Only stop words: an empty query, matching nothing.
        assert_eq!(q("the & a"), "");
        assert!(!matches(&v, &q("the")));
    }

    #[test]
    fn stop_words_keep_their_place_in_a_phrase() {
        assert_eq!(to_tsquery("english", "quick <-> the <-> fox").unwrap(), "'quick' <2> 'fox'");
        assert_eq!(to_tsquery("english", "the <-> quick").unwrap(), "'quick'");
        assert_eq!(phraseto_tsquery("english", "quick the brown fox"), "'quick' <2> 'brown' <-> 'fox'");
        assert_eq!(to_tsquery("english", "!the & fox").unwrap(), "'fox'");
        assert!(to_tsquery("english", "fox:A").is_err());
    }

    /// Values PostgreSQL 17 printed for these; the arithmetic is its, in
    /// float and double where it uses each, so they agree to the last digit.
    #[test]
    fn ts_rank_is_postgresqls_to_the_last_digit() {
        let v = |d: &str| to_tsvector("english", d);
        let q = |t: &str| to_tsquery("english", t).unwrap();
        let w = DEFAULT_RANK_WEIGHTS;
        let doc = "The quick brown fox jumps over the lazy dog";
        assert_eq!(ts_rank(w, &v(doc), &q("fox"), 0).to_string(), "0.06079271");
        assert_eq!(ts_rank(w, &v(doc), &q("quick & fox"), 0).to_string(), "0.098500855");
        assert_eq!(ts_rank(w, &v("fox fox fox quick quick dog"), &q("quick & fox"), 0).to_string(), "0.46028033");
        assert_eq!(ts_rank(w, &v("nothing to see here"), &q("quick & fox"), 0), 1e-20);
        assert_eq!(ts_rank(w, &v("nothing to see here"), &q("fox"), 0), 0.0);
        assert_eq!(ts_rank(w, &v(doc), &q("quick <-> brown"), 1).to_string(), "0.03530128");
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
            if stem_it && STOP_WORDS.contains(&lower.as_str()) {
                continue;
            }
            let text = if stem_it {
                super::stem(&lower)
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
        let Ok(Some(node)) = parse_query("simple", query) else {
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
            4 => format!("{} & {}", query(rng, depth - 1), query(rng, depth - 1)),
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
