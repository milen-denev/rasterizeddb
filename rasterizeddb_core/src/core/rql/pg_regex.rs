//! PostgreSQL's regular expressions (its "advanced" REs, ARE), translated.
//!
//! A pattern is matched here only where the answer provably equals
//! PostgreSQL's for every input. The pattern is parsed as an ARE and
//! rewritten, construct by construct, into an equivalent `regex` crate
//! expression; it is never handed to the crate as written, whose flavour
//! differs from PostgreSQL's in ways that change answers silently:
//!
//! - `.` and a negated bracket match a newline in PostgreSQL by default
//!   (they do not in newline-sensitive mode, flags `n`/`m`);
//! - `\b` is a backspace and `\B` a backslash; `\y`, `\Y`, `\m`, `\M` and
//!   `[[:<:]]`, `[[:>:]]` are the word boundaries;
//! - `\d` is `[0-9]` and nothing else;
//! - classes such as `\w`, `\s`, `[[:alpha:]]`, and case-insensitive
//!   matching, follow the database's locale (glibc's tables), not Unicode's.
//!
//! **The verified subset.** Literals; `.`; `^ $ \A \Z`; groups `( )` and
//! `(?: )`; alternation; `* + ? {m} {m,} {m,n}` (counts up to 255) and their
//! non-greedy forms; bracket expressions with ASCII ranges, `\d \s \w`
//! inside, and the classes `[:digit:]`, `[:xdigit:]`, `[:alpha:]`,
//! `[:alnum:]`, `[:upper:]`, `[:lower:]`, `[:space:]`, `[:blank:]`,
//! `[:punct:]`, `[:cntrl:]`, `[:graph:]`, `[:print:]`; the escapes `\d \D \s
//! \S \w \W \y \Y \m \M \b \B \n \t \r \f \v \a \e \uXXXX \UXXXXXXXX` and an
//! escaped punctuation character; `[[:<:]]` and `[[:>:]]`; the director
//! `***=` and `***:`; embedded options `(?ins…)` at the start; flags `i c n
//! m s t q`.
//!
//! Of these, the locale-dependent ones (`\w \W \s \S`, word boundaries, the
//! classes other than digit and xdigit, case-insensitivity) are exact for
//! ASCII text only: a value holding any non-ASCII character is *uncertain*
//! ([`PgRegex::matches`] answers `None`), and the caller must not answer for
//! PostgreSQL. Everything else — back-references, look-around, `\x`, `\c`,
//! octal escapes, collating elements other than one ASCII character,
//! equivalence classes, ranges with a non-ASCII endpoint, a non-ASCII
//! character in a case-insensitive pattern, flags `x p w b e`, a brace that
//! is not a repetition count — is [`PgRegexError::Unsupported`].
//!
//! A pattern PostgreSQL itself refuses is [`PgRegexError::Invalid`] for the
//! cases the differential test pins (unbalanced parentheses, a quantifier
//! with nothing to repeat, an unknown letter escape, a repetition count past
//! 255); PostgreSQL raises 2201B for those when a value meets them.

use std::sync::Arc;

/// Why a pattern is not matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgRegexError {
    /// PostgreSQL refuses the pattern too: SQLSTATE 2201B, its message.
    Invalid(String),
    /// A flag PostgreSQL refuses: SQLSTATE 22023, its message.
    BadOption(String),
    /// Outside the verified subset: what it uses.
    Unsupported(String),
}

impl PgRegexError {
    /// The message as an engine error, its words picking its SQLSTATE.
    pub fn message(&self) -> String {
        match self {
            PgRegexError::Invalid(m) => format!("invalid regular expression: {m}"),
            PgRegexError::BadOption(m) => m.clone(),
            PgRegexError::Unsupported(m) => format!("a regular expression using {m} is not supported"),
        }
    }
}

/// A translated pattern.
#[derive(Debug)]
pub struct PgRegex {
    re: regex::Regex,
    /// Exact only over ASCII text.
    ascii_only: bool,
}

impl PgRegex {
    /// Whether the pattern matches `text`, as PostgreSQL answers it; `None`
    /// when that answer cannot be vouched for (non-ASCII text under a
    /// locale-dependent construct).
    pub fn matches(&self, text: &str) -> Option<bool> {
        if self.ascii_only && !text.is_ascii() {
            return None;
        }
        Some(self.re.is_match(text))
    }
}

/// [`compile`], kept per thread by pattern and flags (errors too).
pub fn compile_cached(pattern: &str, flags: &str) -> Result<Arc<PgRegex>, PgRegexError> {
    type Cache = std::collections::HashMap<(String, String), Result<Arc<PgRegex>, PgRegexError>>;
    thread_local! {
        static CACHE: std::cell::RefCell<Cache> = std::cell::RefCell::new(Cache::new());
    }
    let key = (pattern.to_string(), flags.to_string());
    if let Some(hit) = CACHE.with(|c| c.borrow().get(&key).cloned()) {
        return hit;
    }
    let compiled = compile(pattern, flags).map(Arc::new);
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        if cache.len() >= 256 {
            cache.clear();
        }
        cache.insert(key, compiled.clone());
    });
    compiled
}

#[derive(Debug, Clone, Copy, Default)]
struct Opts {
    ci: bool,
    nl: bool,
    literal: bool,
}

fn apply_flag(o: &mut Opts, c: char, embedded: bool) -> Result<(), PgRegexError> {
    match c {
        'i' => o.ci = true,
        'c' => o.ci = false,
        'n' | 'm' => o.nl = true,
        's' => o.nl = false,
        't' => {}
        'q' => o.literal = true,
        'g' if !embedded => {
            return Err(PgRegexError::BadOption("regexp_like() does not support the \"global\" option".into()));
        }
        'x' => return Err(PgRegexError::Unsupported("expanded syntax (flag x)".into())),
        'p' | 'w' => return Err(PgRegexError::Unsupported("partial newline-sensitive matching (flags p, w)".into())),
        'b' | 'e' => return Err(PgRegexError::Unsupported("basic or extended RE syntax (flags b, e)".into())),
        other if embedded => return Err(PgRegexError::Unsupported(format!("the embedded option {other}"))),
        other => return Err(PgRegexError::BadOption(format!("invalid regular expression option: \"{other}\""))),
    }
    Ok(())
}

/// Translates `pattern` under `flags` (`regexp_like`'s; `~*` is `i`).
pub fn compile(pattern: &str, flags: &str) -> Result<PgRegex, PgRegexError> {
    let mut o = Opts::default();
    for c in flags.chars() {
        apply_flag(&mut o, c, false)?;
    }
    let mut body: &str = pattern;
    // Under the `q` flag the whole pattern is literal, a leading `***=` or
    // `***:` included (PostgreSQL reads no director then).
    if o.literal {
    } else if let Some(rest) = body.strip_prefix("***=") {
        o.literal = true;
        body = rest;
    } else if let Some(rest) = body.strip_prefix("***:") {
        body = rest;
    } else if body.starts_with("***") {
        return Err(PgRegexError::Unsupported("a director other than ***= or ***:".into()));
    }
    if !o.literal && body.starts_with("(?") {
        if let Some(close) = body.find(')') {
            let letters = &body[2..close];
            if !letters.is_empty() && letters.chars().all(|c| c.is_ascii_alphabetic()) {
                for c in letters.chars() {
                    apply_flag(&mut o, c, true)?;
                }
                body = &body[close + 1..];
            }
        }
    }
    let mut ascii_only = o.ci;
    let translated = if o.literal {
        if o.ci && !body.is_ascii() {
            return Err(PgRegexError::Unsupported("a non-ASCII character in a case-insensitive pattern".into()));
        }
        regex::escape(body)
    } else {
        let chars: Vec<char> = body.chars().collect();
        let mut p = Parser { c: &chars, i: 0, o, ascii_only: false, depth: 0 };
        let out = p.alt()?;
        if p.i < chars.len() {
            return Err(PgRegexError::Invalid("parentheses () not balanced".into()));
        }
        ascii_only |= p.ascii_only;
        out
    };
    let mut full = String::new();
    if o.ci {
        full.push_str("(?i)");
    }
    if o.nl {
        full.push_str("(?m)");
    }
    full.push_str(&translated);
    let re = regex::RegexBuilder::new(&full)
        .size_limit(1 << 24)
        .build()
        .map_err(|e| PgRegexError::Unsupported(format!("a pattern this engine cannot compile ({})", e.to_string().lines().last().unwrap_or("").trim())))?;
    Ok(PgRegex { re, ascii_only })
}

struct Parser<'a> {
    c: &'a [char],
    i: usize,
    o: Opts,
    ascii_only: bool,
    depth: usize,
}

const QUANT_INVALID: &str = "quantifier operand invalid";

/// ASCII sets for PostgreSQL's classes, as a Rust class body, and whether the
/// class depends on the locale.
fn class_set(name: &str) -> Option<(&'static str, bool)> {
    Some(match name {
        // Locale-dependent too: under an ICU locale `[[:digit:]]` takes
        // '١'. Judged for ASCII text only, as every other class.
        "digit" => ("0-9", true),
        "xdigit" => ("0-9A-Fa-f", true),
        "alpha" => ("A-Za-z", true),
        "alnum" => ("0-9A-Za-z", true),
        "upper" => ("A-Z", true),
        "lower" => ("a-z", true),
        "space" => (" \\t\\n\\x0B\\x0C\\r", true),
        "blank" => (" \\t", true),
        "punct" => ("!-/:-@\\[-`{-~", true),
        "cntrl" => ("\\x00-\\x1F\\x7F", true),
        "graph" => ("!-~", true),
        "print" => (" -~", true),
        _ => return None,
    })
}

/// A character as a member of a Rust class.
fn class_char(c: char) -> String {
    if c.is_ascii_punctuation() {
        format!("\\{c}")
    } else if c.is_control() || c == ' ' {
        format!("\\x{{{:X}}}", c as u32)
    } else {
        c.to_string()
    }
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<char> {
        self.c.get(self.i).copied()
    }

    fn starts(&self, s: &str) -> bool {
        let want: Vec<char> = s.chars().collect();
        self.c.get(self.i..self.i + want.len()) == Some(&want[..])
    }

    fn alt(&mut self) -> Result<String, PgRegexError> {
        let mut s = self.concat()?;
        while self.peek() == Some('|') {
            self.i += 1;
            s.push('|');
            s.push_str(&self.concat()?);
        }
        Ok(s)
    }

    fn concat(&mut self) -> Result<String, PgRegexError> {
        let mut s = String::new();
        loop {
            match self.peek() {
                None | Some('|') => break,
                Some(')') => {
                    if self.depth == 0 {
                        return Err(PgRegexError::Invalid("parentheses () not balanced".into()));
                    }
                    break;
                }
                Some('*') | Some('+') | Some('?') => return Err(PgRegexError::Invalid(QUANT_INVALID.into())),
                Some('{') => {
                    if self.bound_ahead() {
                        return Err(PgRegexError::Invalid(QUANT_INVALID.into()));
                    }
                    return Err(PgRegexError::Unsupported("a brace that is not a repetition count".into()));
                }
                _ => {}
            }
            let (atom, quantifiable) = self.atom()?;
            let q = self.quantifier()?;
            match q {
                None => s.push_str(&atom),
                Some(_) if !quantifiable => {
                    return Err(PgRegexError::Unsupported("a quantified anchor or boundary".into()));
                }
                Some(q) => {
                    s.push_str("(?:");
                    s.push_str(&atom);
                    s.push(')');
                    s.push_str(&q);
                }
            }
        }
        Ok(s)
    }

    /// Whether `{` at the cursor starts `{m}`, `{m,}` or `{m,n}`.
    fn bound_ahead(&self) -> bool {
        let mut j = self.i + 1;
        let digit = |k: usize| self.c.get(k).is_some_and(|c| c.is_ascii_digit());
        if !digit(j) {
            return false;
        }
        while digit(j) {
            j += 1;
        }
        if self.c.get(j) == Some(&',') {
            j += 1;
            while digit(j) {
                j += 1;
            }
        }
        self.c.get(j) == Some(&'}')
    }

    fn quantifier(&mut self) -> Result<Option<String>, PgRegexError> {
        let base = match self.peek() {
            Some(c @ ('*' | '+' | '?')) => {
                self.i += 1;
                c.to_string()
            }
            Some('{') => {
                if !self.bound_ahead() {
                    return Err(PgRegexError::Unsupported("a brace that is not a repetition count".into()));
                }
                self.i += 1;
                let mut text = String::new();
                while let Some(c) = self.peek() {
                    self.i += 1;
                    if c == '}' {
                        break;
                    }
                    text.push(c);
                }
                let (lo, hi) = match text.split_once(',') {
                    Some((lo, hi)) => (lo, Some(hi)),
                    None => (text.as_str(), None),
                };
                let lo: u32 = lo.parse().map_err(|_| PgRegexError::Invalid("invalid repetition count(s)".into()))?;
                let hi: Option<u32> = match hi {
                    None => Some(lo),
                    Some("") => None,
                    Some(h) => Some(h.parse().map_err(|_| PgRegexError::Invalid("invalid repetition count(s)".into()))?),
                };
                if lo > 255 || hi.is_some_and(|h| h > 255 || h < lo) {
                    return Err(PgRegexError::Invalid("invalid repetition count(s)".into()));
                }
                match (text.contains(','), hi) {
                    (false, _) => format!("{{{lo}}}"),
                    (true, None) => format!("{{{lo},}}"),
                    (true, Some(h)) => format!("{{{lo},{h}}}"),
                }
            }
            _ => return Ok(None),
        };
        let lazy = if self.peek() == Some('?') {
            self.i += 1;
            "?"
        } else {
            ""
        };
        if matches!(self.peek(), Some('*' | '+' | '?')) || self.peek() == Some('{') && self.bound_ahead() {
            return Err(PgRegexError::Unsupported("a quantifier after a quantifier".into()));
        }
        Ok(Some(format!("{base}{lazy}")))
    }

    fn atom(&mut self) -> Result<(String, bool), PgRegexError> {
        if self.starts("[[:<:]]") {
            self.i += 7;
            self.ascii_only = true;
            return Ok(("(?-u:\\b{start})".into(), false));
        }
        if self.starts("[[:>:]]") {
            self.i += 7;
            self.ascii_only = true;
            return Ok(("(?-u:\\b{end})".into(), false));
        }
        let c = self.peek().expect("checked by the caller");
        self.i += 1;
        Ok(match c {
            '(' => {
                if self.peek() == Some('?') {
                    if self.starts("?:") {
                        self.i += 2;
                    } else if self.starts("?=") || self.starts("?!") || self.starts("?<") {
                        return Err(PgRegexError::Unsupported("look-around".into()));
                    } else {
                        return Err(PgRegexError::Invalid(QUANT_INVALID.into()));
                    }
                }
                self.depth += 1;
                let inner = self.alt()?;
                self.depth -= 1;
                if self.peek() != Some(')') {
                    return Err(PgRegexError::Invalid("parentheses () not balanced".into()));
                }
                self.i += 1;
                (format!("(?:{inner})"), true)
            }
            '.' => (if self.o.nl { "[^\\n]".into() } else { "(?s:.)".into() }, true),
            '^' => ("^".into(), false),
            '$' => ("$".into(), false),
            '[' => (self.bracket()?, true),
            '\\' => self.escape()?,
            other => (self.literal(other)?, true),
        })
    }

    fn literal(&self, c: char) -> Result<String, PgRegexError> {
        if self.o.ci && !c.is_ascii() {
            return Err(PgRegexError::Unsupported("a non-ASCII character in a case-insensitive pattern".into()));
        }
        Ok(regex::escape(&c.to_string()))
    }

    /// `\X` outside a bracket: (translation, quantifiable).
    fn escape(&mut self) -> Result<(String, bool), PgRegexError> {
        let Some(c) = self.peek() else {
            return Err(PgRegexError::Unsupported("a trailing backslash".into()));
        };
        self.i += 1;
        Ok(match c {
            // `\d` is `[[:digit:]]`, locale-dependent as it is.
            'd' => {
                self.ascii_only = true;
                ("[0-9]".into(), true)
            }
            'D' => {
                self.ascii_only = true;
                ("[^0-9]".into(), true)
            }
            's' => {
                self.ascii_only = true;
                ("[ \\t\\n\\x0B\\x0C\\r]".into(), true)
            }
            'S' => {
                self.ascii_only = true;
                ("[^ \\t\\n\\x0B\\x0C\\r]".into(), true)
            }
            'w' => {
                self.ascii_only = true;
                ("[0-9A-Za-z_]".into(), true)
            }
            'W' => {
                self.ascii_only = true;
                ("[^0-9A-Za-z_]".into(), true)
            }
            'y' => {
                self.ascii_only = true;
                ("(?-u:\\b)".into(), false)
            }
            'Y' => {
                self.ascii_only = true;
                ("(?-u:\\B)".into(), false)
            }
            'm' => {
                self.ascii_only = true;
                ("(?-u:\\b{start})".into(), false)
            }
            'M' => {
                self.ascii_only = true;
                ("(?-u:\\b{end})".into(), false)
            }
            'A' => ("\\A".into(), false),
            'Z' => ("\\z".into(), false),
            other => (regex::escape(&self.escape_char(other)?.to_string()), true),
        })
    }

    /// An escape that stands for one character (in or out of a bracket).
    fn escape_char(&mut self, c: char) -> Result<char, PgRegexError> {
        Ok(match c {
            'b' => '\u{8}',
            'B' => '\\',
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            'f' => '\u{C}',
            'v' => '\u{B}',
            'a' => '\u{7}',
            'e' => '\u{1B}',
            'u' | 'U' => {
                let n = if c == 'u' { 4 } else { 8 };
                let hex: String = self.c.get(self.i..self.i + n).map(|s| s.iter().collect()).unwrap_or_default();
                if hex.len() != n || !hex.chars().all(|h| h.is_ascii_hexdigit()) {
                    return Err(PgRegexError::Unsupported("a malformed \\u escape".into()));
                }
                self.i += n;
                let ch = u32::from_str_radix(&hex, 16)
                    .ok()
                    .and_then(char::from_u32)
                    .ok_or_else(|| PgRegexError::Unsupported("a \\u escape that is no character".into()))?;
                if self.o.ci && !ch.is_ascii() {
                    return Err(PgRegexError::Unsupported("a non-ASCII character in a case-insensitive pattern".into()));
                }
                ch
            }
            'x' | 'c' => return Err(PgRegexError::Unsupported(format!("the \\{c} escape"))),
            '0'..='9' => return Err(PgRegexError::Unsupported("a back-reference or octal escape".into())),
            other if other.is_ascii_alphanumeric() => {
                return Err(PgRegexError::Invalid("invalid escape \\ sequence".into()));
            }
            other => {
                if self.o.ci && !other.is_ascii() {
                    return Err(PgRegexError::Unsupported("a non-ASCII character in a case-insensitive pattern".into()));
                }
                other
            }
        })
    }

    /// A bracket expression, the `[` already read.
    fn bracket(&mut self) -> Result<String, PgRegexError> {
        let negate = if self.peek() == Some('^') {
            self.i += 1;
            true
        } else {
            false
        };
        let mut items = String::new();
        let mut first = true;
        // Whether the member just read was a range, a class or an escape
        // set: a `-` after one starts no range, and anything but `]` after
        // that `-` is PostgreSQL's "invalid character range" (`[a-c-e]`,
        // `[\w-.]`, `[[:alpha:]-z]`), where it was read as a range from `-`.
        let mut completed = false;
        loop {
            let Some(c) = self.peek() else {
                return Err(PgRegexError::Invalid("brackets [] not balanced".into()));
            };
            if c == ']' && !first {
                self.i += 1;
                break;
            }
            if completed && c == '-' && self.c.get(self.i + 1).is_some_and(|n| *n != ']') {
                return Err(PgRegexError::Invalid("invalid character range".into()));
            }
            completed = false;
            first = false;
            // One member: a class, a collating element, an escape or a char.
            let start: Option<char> = if c == '[' && self.c.get(self.i + 1) == Some(&':') {
                let rest: String = self.c[self.i + 2..].iter().collect();
                let Some(end) = rest.find(":]") else {
                    return Err(PgRegexError::Unsupported("an unterminated character class".into()));
                };
                let name = &rest[..end];
                let Some((set, locale)) = class_set(name) else {
                    return Err(PgRegexError::Unsupported(format!("the character class [:{name}:]")));
                };
                self.ascii_only |= locale;
                items.push_str(set);
                self.i += 2 + name.chars().count() + 2;
                None
            } else if c == '[' && self.c.get(self.i + 1) == Some(&'=') {
                return Err(PgRegexError::Unsupported("an equivalence class".into()));
            } else if c == '[' && self.c.get(self.i + 1) == Some(&'.') {
                let rest: String = self.c[self.i + 2..].iter().collect();
                let Some(end) = rest.find(".]") else {
                    return Err(PgRegexError::Unsupported("an unterminated collating element".into()));
                };
                let name = &rest[..end];
                let mut it = name.chars();
                let (Some(one), None) = (it.next(), it.next()) else {
                    return Err(PgRegexError::Unsupported(format!("the collating element [.{name}.]")));
                };
                if !one.is_ascii() {
                    return Err(PgRegexError::Unsupported(format!("the collating element [.{name}.]")));
                }
                self.i += 2 + name.chars().count() + 2;
                Some(one)
            } else if c == '\\' {
                self.i += 1;
                let Some(e) = self.peek() else {
                    return Err(PgRegexError::Unsupported("a trailing backslash".into()));
                };
                self.i += 1;
                match e {
                    'd' => {
                        self.ascii_only = true;
                        items.push_str("0-9");
                        None
                    }
                    's' => {
                        self.ascii_only = true;
                        items.push_str(" \\t\\n\\x0B\\x0C\\r");
                        None
                    }
                    'w' => {
                        self.ascii_only = true;
                        items.push_str("0-9A-Za-z_");
                        None
                    }
                    'D' | 'S' | 'W' => {
                        return Err(PgRegexError::Unsupported(format!("\\{e} inside a bracket expression")));
                    }
                    other => Some(self.escape_char(other)?),
                }
            } else {
                self.i += 1;
                Some(c)
            };
            let Some(lo) = start else {
                completed = true;
                continue;
            };
            // A range `lo-hi`, unless the `-` ends the bracket.
            if self.peek() == Some('-') && self.c.get(self.i + 1).is_some_and(|n| *n != ']') {
                self.i += 1;
                let hi = match self.peek() {
                    Some('\\') => {
                        self.i += 1;
                        let e = self
                            .peek()
                            .ok_or_else(|| PgRegexError::Unsupported("a trailing backslash".into()))?;
                        self.i += 1;
                        self.escape_char(e)?
                    }
                    Some('[') => return Err(PgRegexError::Unsupported("a range ending in a class or element".into())),
                    Some(h) => {
                        self.i += 1;
                        h
                    }
                    None => return Err(PgRegexError::Unsupported("an unterminated bracket expression".into())),
                };
                if !lo.is_ascii() || !hi.is_ascii() {
                    return Err(PgRegexError::Unsupported("a range with a non-ASCII endpoint".into()));
                }
                if lo > hi {
                    return Err(PgRegexError::Unsupported("a reversed range".into()));
                }
                items.push_str(&class_char(lo));
                items.push('-');
                items.push_str(&class_char(hi));
                completed = true;
            } else {
                if self.o.ci && !lo.is_ascii() {
                    return Err(PgRegexError::Unsupported("a non-ASCII character in a case-insensitive pattern".into()));
                }
                items.push_str(&class_char(lo));
            }
        }
        if negate && self.o.nl {
            items.push_str("\\n");
        }
        Ok(format!("[{}{}]", if negate { "^" } else { "" }, items))
    }
}

/// PostgreSQL's `similar_to_escape(pattern [, escape])`: the regular
/// expression a `SIMILAR TO` pattern means, character for character as
/// PostgreSQL writes it (`similar_escape_internal`). `None` escape is none
/// at all (an empty escape string).
pub fn similar_to_escape(pattern: &str, escape: Option<char>) -> Result<String, String> {
    let mut r = String::from("^(?:");
    let mut afterescape = false;
    let mut incharclass = false;
    let mut charclass_pos = 0u8;
    let mut nquotes = 0;
    for pchar in pattern.chars() {
        if afterescape {
            if pchar == '"' && !incharclass {
                match nquotes {
                    0 => r.push_str("){1,1}?("),
                    1 => r.push_str("){1,1}(?:"),
                    _ => {
                        return Err(
                            "SQL regular expression may not contain more than two escape-double-quote separators"
                                .into(),
                        );
                    }
                }
                nquotes += 1;
            } else {
                r.push('\\');
                r.push(pchar);
                charclass_pos = 3;
            }
            afterescape = false;
        } else if Some(pchar) == escape {
            afterescape = true;
        } else if incharclass {
            if pchar == '\\' {
                r.push('\\');
            }
            r.push(pchar);
            if pchar == ']' && charclass_pos > 2 {
                incharclass = false;
            } else if pchar == '^' && charclass_pos == 1 {
                charclass_pos = 2;
            } else {
                charclass_pos = 3;
            }
        } else if pchar == '[' {
            r.push(pchar);
            incharclass = true;
            charclass_pos = 1;
        } else if pchar == '%' {
            r.push_str(".*");
        } else if pchar == '_' {
            r.push('.');
        } else if pchar == '(' {
            r.push_str("(?:");
        } else if matches!(pchar, '\\' | '.' | '^' | '$') {
            r.push('\\');
            r.push(pchar);
        } else {
            r.push(pchar);
        }
    }
    r.push_str(")$");
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &str, f: &str, s: &str) -> Option<bool> {
        compile(p, f).unwrap_or_else(|e| panic!("{p:?}: {e:?}")).matches(s)
    }

    #[test]
    fn newline_word_and_escape_semantics_are_postgres_own() {
        assert_eq!(m("^a.b$", "", "a\nb"), Some(true));
        assert_eq!(m("^a.b$", "n", "a\nb"), Some(false));
        assert_eq!(m("^a[^x]b$", "n", "a\nb"), Some(false));
        assert_eq!(m("^a[^x]b$", "", "a\nb"), Some(true));
        // `\D` and `\W` are not bracket expressions: they take a newline
        // in newline-sensitive mode too.
        assert_eq!(m("\\D", "n", "\n"), Some(true));
        assert_eq!(m("^[^x]+$", "", "a\nb"), Some(true));
        assert_eq!(m("abc$", "", "abc\n"), Some(false));
        assert_eq!(m("^b$", "n", "a\nb"), Some(true));
        assert_eq!(m("\\Bfoo", "", "\\foo"), Some(true));
        assert_eq!(m("\\bfoo", "", "a foo"), Some(false));
        assert_eq!(m("\\yfoo\\y", "", "a foo b"), Some(true));
        assert_eq!(m("[[:<:]]foo[[:>:]]", "", "a foo b"), Some(true));
        assert_eq!(m("[[:<:]]foo[[:>:]]", "", "afoo"), Some(false));
        // `\d` and `[[:digit:]]` are the locale's (an ICU database takes
        // '١'): declined for non-ASCII text, answered for ASCII.
        assert_eq!(m("^\\d+$", "", "١٢٣"), None);
        assert_eq!(m("^[[:digit:]]+$", "", "١٢٣"), None);
        assert_eq!(m("^\\d+$", "", "123"), Some(true));
        assert_eq!(m("^\\w+$", "", "héllo"), None);
        assert_eq!(m("^[a-z]+$", "i", "ſ"), None);
        assert_eq!(m("^\\w{1,30}$", "", "abc"), Some(true));
        assert_eq!(m("^\\d{1,255}$", "", "123"), Some(true));
        assert_eq!(m("***=a.b", "", "a.b"), Some(true));
        assert_eq!(m("***=a.b", "", "axb"), Some(false));
        assert_eq!(m("a.c", "q", "abc"), Some(false));
        // Under `q` a leading director is literal too.
        assert_eq!(m("***=a", "q", "***=a"), Some(true));
        assert_eq!(m("***=a", "q", "a"), Some(false));
        assert_eq!(m("***:a", "q", "x***:ab"), Some(true));
        // A `-` closing a bracket after a range is a literal.
        assert_eq!(m("^[a-c-]+$", "", "a-c"), Some(true));
        assert_eq!(m("^[%--]+$", "", "%-"), Some(true));
        assert_eq!(m("(?n)^b$", "", "a\nb"), Some(true));
        assert_eq!(m("^[]a]+$", "", "]a"), Some(true));
        assert_eq!(m("^[a\\]]+$", "", "a]"), Some(true));
        assert_eq!(m("^[\\w-]+$", "", "a-b"), Some(true));
        assert_eq!(m("[)]", "", "a)"), Some(true));
    }

    #[test]
    fn patterns_postgres_refuses_are_refused_and_the_rest_named() {
        let invalid = |p: &str| assert!(matches!(compile(p, ""), Err(PgRegexError::Invalid(_))), "{p}");
        invalid("(");
        invalid("a(?i)bc");
        invalid("\\p{L}");
        invalid("a{256}");
        // A `-` after a range, a class or an escape set, not closing the
        // bracket: "invalid character range".
        for p in ["[a-c-e]", "[\\d-z]", "[[:alpha:]-z]", "[\\w-.]+@x", "[a-c--]"] {
            invalid(p);
        }
        let unsupported = |p: &str| assert!(matches!(compile(p, ""), Err(PgRegexError::Unsupported(_))), "{p}");
        unsupported("^(a)\\1$");
        unsupported("^a(?=b)");
        unsupported("[[.hyphen.]]");
        unsupported("[[=a=]]");
        assert!(matches!(compile("a", "g"), Err(PgRegexError::BadOption(_))));
        assert!(matches!(compile("a", "z"), Err(PgRegexError::BadOption(_))));
    }

    #[test]
    fn similar_to_escape_writes_what_postgres_writes() {
        assert_eq!(similar_to_escape("a#b.c%", Some('\\')).unwrap(), "^(?:a#b\\.c.*)$");
        assert_eq!(similar_to_escape("%\\d%", Some('\\')).unwrap(), "^(?:.*\\d.*)$");
        assert_eq!(similar_to_escape("(a|b)_", Some('\\')).unwrap(), "^(?:(?:a|b).)$");
        assert_eq!(similar_to_escape("a#%", Some('#')).unwrap(), "^(?:a\\%)$");
    }
}

/// Every answer here against PostgreSQL 17.11's own (`pg_regex_pg17.tsv`,
/// recorded from `regexp_like` on a `postgres:17` server, en_US.utf8):
/// each case is answered the same, or declined (unsupported or uncertain),
/// never answered differently; and a pattern PostgreSQL refuses is never
/// matched. Regenerate with `.claude/reviews/c3/r4/gen_corpus.py`.
#[cfg(test)]
mod differential {
    use super::*;

    fn unescape(s: &str) -> String {
        let mut out = String::new();
        let mut it = s.chars();
        while let Some(c) = it.next() {
            if c == '\\' {
                match it.next() {
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('r') => out.push('\r'),
                    Some('\\') => out.push('\\'),
                    Some(o) => {
                        out.push('\\');
                        out.push(o);
                    }
                    None => out.push('\\'),
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn every_answer_is_postgres_own_or_declined() {
        let corpus = include_str!("pg_regex_pg17.tsv");
        let (mut same, mut declined, mut wrong) = (0usize, 0usize, Vec::new());
        for line in corpus.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
            let cols: Vec<&str> = line.split('\t').collect();
            let (p, f, s, pg) = (unescape(cols[0]), unescape(cols[1]), unescape(cols[2]), cols[3]);
            let ours = compile(&p, &f);
            let agrees = match (&ours, pg) {
                (Err(PgRegexError::Unsupported(_)), _) => None,
                (Ok(re), "t" | "f") => re.matches(&s).map(|m| m == (pg == "t")),
                (Ok(_), _) => Some(false),
                (Err(PgRegexError::Invalid(_)), "E2201B") => Some(true),
                (Err(PgRegexError::BadOption(_)), "E22023") => Some(true),
                (Err(_), _) => Some(false),
            };
            match agrees {
                None => declined += 1,
                Some(true) => same += 1,
                Some(false) => wrong.push(format!("{p:?} flags {f:?} on {s:?}: PostgreSQL {pg}, here {ours:?}")),
            }
        }
        assert!(wrong.is_empty(), "{} answers differ from PostgreSQL's:\n{}", wrong.len(), wrong.join("\n"));
        // Declining everything would pass the check above: most cases must
        // be answered.
        assert!(same * 10 >= (same + declined) * 7, "answered {same}, declined {declined}");
    }
}
