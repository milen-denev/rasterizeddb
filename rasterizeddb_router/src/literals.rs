//! The literal-slot tokenizer (sharding-plan.md §4.9 G3): a simple query's
//! *shape*, with its literals replaced by `$1`, `$2`, …, and the literals.
//!
//! The gateway keys its route-template cache by the shape and asks a shard
//! to classify it once (`rshard::Request::Classify`, source `Shape`); the
//! template then says which slot holds the level-0 key. The statement a
//! shard *runs* is always the client's own text: a shape is only ever
//! classified, so a shape this gets wrong costs a refused classification
//! and a fallback, never a wrong row. (Correctness rests on the shard's
//! ownership checks in any case, R1.)
//!
//! It follows core's `pgwire::text_template` (663c509f) where the two
//! overlap: a slot is `$k`, 1-based, in order; a plain `'…'` string and an
//! unsigned plain number become slots; a signed number does not (core's
//! text path rewrites `-1`); a prefixed string other than `E'…'` stays in
//! the text. It differs where routing needs more than core's WHERE-only
//! template:
//! - every clause and every depth is shaped (an INSERT's `VALUES (42, …)`
//!   carries its key there);
//! - `E'…'` and `$tag$…$tag$` strings become slots, decoded;
//! - comments (`--`, nested `/* */`) are copied, and quotes inside them are
//!   not read as strings;
//! - numbers stay in the text where a slot would change the statement's
//!   meaning or syntax: `ORDER BY 1` / `GROUP BY 1` ordinals, and a type's
//!   modifiers (`numeric(10, 2)`, `varchar(20)`);
//! - a string right after a type name (`DATE '2026-01-01'`, `INTERVAL
//!   '1 day'`) stays in the text, since `DATE $1` is not SQL;
//! - only statements that start with SELECT, INSERT, UPDATE, DELETE or WITH
//!   are shaped; a `$n` parameter outside a string, or two string literals
//!   that PostgreSQL would concatenate (`'a'` newline `'b'`), gives `None`.
//!
//! Byte-walking is UTF-8 safe: it moves only on ASCII bytes it recognises,
//! and copies everything else in whole slices, so a multibyte character is
//! never cut.

/// A literal taken out of a shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    /// A string's value, quotes removed and escapes decoded.
    Str(String),
    /// An unsigned number as written (`42`, `3.5`).
    Num(String),
}

impl Literal {
    /// The literal as an integer key, as PostgreSQL coerces it against an
    /// integer column: a plain integer, or a string holding one (an unknown
    /// literal `'42'` is accepted by an `int8` column).
    pub fn as_int(&self) -> Option<i128> {
        match self {
            Literal::Num(n) => n.parse().ok(),
            Literal::Str(s) => s.trim().parse().ok(),
        }
    }

    /// The literal as a text key: only a string. `text_col = 42` has no
    /// operator in PostgreSQL, so a number never routes a text column.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Literal::Str(s) => Some(s),
            Literal::Num(_) => None,
        }
    }

    /// The literal as a UUID key: a string in any form PostgreSQL's
    /// `uuid_in` reads (hyphens anywhere between digit pairs, optional
    /// braces, either case).
    pub fn as_uuid(&self) -> Option<[u8; 16]> {
        parse_uuid(self.as_text()?)
    }
}

/// A UUID in the forms PostgreSQL accepts.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let t = text.trim();
    let t = t.strip_prefix('{').and_then(|t| t.strip_suffix('}')).unwrap_or(t);
    let mut out = [0u8; 16];
    let mut n = 0usize;
    let mut hi: Option<u8> = None;
    for c in t.bytes() {
        if c == b'-' {
            // A hyphen only between whole bytes.
            if hi.is_some() {
                return None;
            }
            continue;
        }
        let d = (c as char).to_digit(16)? as u8;
        match hi.take() {
            None => hi = Some(d),
            Some(h) => {
                if n == 16 {
                    return None;
                }
                out[n] = h << 4 | d;
                n += 1;
            }
        }
    }
    (n == 16 && hi.is_none()).then_some(out)
}

/// A shape and its literals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shape {
    pub text: String,
    pub slots: Vec<Literal>,
}

/// Type names a string literal may follow as a typed literal (`DATE '…'`),
/// and whose parenthesised numbers are modifiers. Upper case.
const TYPE_WORDS: &[&str] = &[
    "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMETZ", "INTERVAL", "NUMERIC", "DECIMAL", "INT", "INTEGER",
    "BIGINT", "SMALLINT", "INT2", "INT4", "INT8", "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "PRECISION",
    "BOOLEAN", "BOOL", "JSON", "JSONB", "UUID", "TEXT", "VARCHAR", "CHAR", "CHARACTER", "VARYING", "BPCHAR",
    "BYTEA", "INET", "CIDR", "MACADDR", "MACADDR8", "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE",
    "BIT", "VARBIT", "MONEY", "ZONE", "XML", "TSVECTOR", "TSQUERY", "REGCLASS", "REGTYPE", "OID", "NAME",
];

fn is_type_word(w: &str) -> bool {
    w.len() <= 11 && TYPE_WORDS.iter().any(|t| t.eq_ignore_ascii_case(w))
}

fn ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

/// The statement's shape, or `None` when it is not one this shapes (module
/// docs). `None` means "classify the whole text", never an error.
pub fn shape(sql: &str) -> Option<Shape> {
    let text = sql.trim();
    let first = text.split(|c: char| !c.is_ascii_alphabetic()).next()?;
    if !["SELECT", "INSERT", "UPDATE", "DELETE", "WITH"].iter().any(|k| first.eq_ignore_ascii_case(k)) {
        return None;
    }
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 8);
    let mut slots: Vec<Literal> = Vec::new();
    let mut copied = 0usize;
    let mut i = 0usize;
    // The last word seen, and whether only whitespace followed it.
    let mut last_word: (usize, usize) = (0, 0);
    let mut word_adjacent = false;
    // Per parenthesis depth: whether it was opened right after a type name.
    let mut type_parens: Vec<bool> = Vec::new();
    // At depth 0 of the current parenthesis level, inside ORDER BY/GROUP BY.
    let mut ordinal: Vec<bool> = vec![false];
    let mut pending_by = false;

    let mut push_slot = |out: &mut String, copied: &mut usize, start: usize, end: usize, lit: Literal| {
        out.push_str(&text[*copied..start]);
        slots.push(lit);
        out.push('$');
        out.push_str(&slots.len().to_string());
        *copied = end;
    };

    while i < b.len() {
        let c = b[i];
        match c {
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                word_adjacent = false;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return None;
                }
                word_adjacent = false;
            }
            b'(' => {
                let after_type = word_adjacent && is_type_word(&text[last_word.0..last_word.1]);
                type_parens.push(after_type);
                ordinal.push(false);
                i += 1;
                word_adjacent = false;
            }
            b')' => {
                type_parens.pop();
                if ordinal.len() > 1 {
                    ordinal.pop();
                }
                i += 1;
                word_adjacent = false;
            }
            b'"' => {
                i += 1;
                loop {
                    match b.get(i) {
                        None => return None,
                        Some(b'"') if b.get(i + 1) == Some(&b'"') => i += 2,
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(_) => i += 1,
                    }
                }
                word_adjacent = false;
            }
            b'\'' => {
                let start = i;
                // The prefix letter, if the quote is glued to a word.
                let prefix = (start > 0 && ident(b[start - 1])).then(|| {
                    let mut p = start;
                    while p > 0 && ident(b[p - 1]) {
                        p -= 1;
                    }
                    &text[p..start]
                });
                let escape = prefix.is_some_and(|p| p.eq_ignore_ascii_case("E"));
                let (value, end) = read_string(text, start + 1, escape)?;
                i = end;
                // `'a'` newline `'b'` is one literal to PostgreSQL.
                let mut j = i;
                let mut newline = false;
                while j < b.len() && b[j].is_ascii_whitespace() {
                    newline |= b[j] == b'\n';
                    j += 1;
                }
                if newline && b.get(j) == Some(&b'\'') {
                    return None;
                }
                let typed = word_adjacent && is_type_word(&text[last_word.0..last_word.1]);
                let slot_start = if escape { start - 1 } else { start };
                // `U&'…'` has its own escapes: left in the text.
                let unicode = start > 0 && b[start - 1] == b'&';
                if !typed && !unicode && (prefix.is_none() || escape) {
                    push_slot(&mut out, &mut copied, slot_start, i, Literal::Str(value));
                }
                word_adjacent = false;
            }
            b'$' => {
                // `$1` (a parameter) or `$tag$…$tag$` (a dollar quote).
                if b.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                    return None;
                }
                let mut t = i + 1;
                while t < b.len() && (b[t].is_ascii_alphanumeric() || b[t] == b'_') {
                    t += 1;
                }
                if b.get(t) != Some(&b'$') || (i > 0 && ident(b[i - 1])) {
                    i += 1;
                    word_adjacent = false;
                    continue;
                }
                let tag = &text[i..=t];
                let body_start = t + 1;
                let close = text[body_start..].find(tag)? + body_start;
                let value = text[body_start..close].to_string();
                let end = close + tag.len();
                let typed = word_adjacent && is_type_word(&text[last_word.0..last_word.1]);
                if !typed {
                    push_slot(&mut out, &mut copied, i, end, Literal::Str(value));
                }
                i = end;
                word_adjacent = false;
            }
            c if c.is_ascii_digit() && (i == 0 || !(ident(b[i - 1]) || b[i - 1] == b'.')) => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                    i += 1;
                }
                if i < b.len() && (ident(b[i]) || b[i] == b'.') {
                    // `1e5`, `2x`: part of something else, copied.
                    while i < b.len() && ident(b[i]) {
                        i += 1;
                    }
                    word_adjacent = false;
                    continue;
                }
                let signed = text[..start].trim_end().ends_with(['-', '+']);
                let in_type = type_parens.last().copied().unwrap_or(false);
                let in_ordinal = ordinal.last().copied().unwrap_or(false);
                if !signed && !in_type && !in_ordinal {
                    push_slot(&mut out, &mut copied, start, i, Literal::Num(text[start..i].to_string()));
                }
                word_adjacent = false;
            }
            c if ident(c) => {
                let start = i;
                while i < b.len() && ident(b[i]) {
                    i += 1;
                }
                let word = &text[start..i];
                if pending_by && word.eq_ignore_ascii_case("BY") {
                    if let Some(o) = ordinal.last_mut() {
                        *o = true;
                    }
                }
                pending_by = word.eq_ignore_ascii_case("ORDER") || word.eq_ignore_ascii_case("GROUP");
                if ["LIMIT", "OFFSET", "HAVING", "WHERE", "FROM", "UNION", "INTERSECT", "EXCEPT", "FETCH", "FOR", "WINDOW", "RETURNING"]
                    .iter()
                    .any(|k| word.eq_ignore_ascii_case(k))
                {
                    if let Some(o) = ordinal.last_mut() {
                        *o = false;
                    }
                }
                last_word = (start, i);
                word_adjacent = true;
                // Whitespace after a word keeps it adjacent; anything else
                // clears it below.
                while i < b.len() && b[i].is_ascii_whitespace() {
                    i += 1;
                }
                continue;
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            _ => {
                // `::` keeps a type name in front of what follows from
                // counting (`'x'::date` is a slot already shaped).
                i += 1;
                word_adjacent = false;
                // A multibyte character: move to the next char boundary.
                while i < b.len() && !text.is_char_boundary(i) {
                    i += 1;
                }
            }
        }
    }
    out.push_str(&text[copied..]);
    Some(Shape { text: out, slots })
}

/// The value of a string starting just after its opening quote, and the
/// index just past its closing quote. `escape`: an `E'…'` string, whose
/// backslash escapes are decoded as PostgreSQL decodes them.
fn read_string(text: &str, from: usize, escape: bool) -> Option<(String, usize)> {
    let b = text.as_bytes();
    let mut value = String::new();
    let mut i = from;
    let mut run = from;
    loop {
        match *b.get(i)? {
            b'\'' if b.get(i + 1) == Some(&b'\'') => {
                value.push_str(&text[run..i]);
                value.push('\'');
                i += 2;
                run = i;
            }
            b'\'' => {
                value.push_str(&text[run..i]);
                return Some((value, i + 1));
            }
            b'\\' if escape => {
                value.push_str(&text[run..i]);
                let (decoded, next) = escape_at(text, i + 1)?;
                value.push_str(&decoded);
                i = next;
                run = i;
            }
            _ => i += 1,
        }
    }
}

/// One backslash escape of an `E'…'` string, starting after the backslash.
fn escape_at(text: &str, i: usize) -> Option<(String, usize)> {
    let b = text.as_bytes();
    let c = *b.get(i)?;
    let simple = |ch: char| Some((ch.to_string(), i + 1));
    match c {
        b'b' => simple('\u{8}'),
        b'f' => simple('\u{c}'),
        b'n' => simple('\n'),
        b'r' => simple('\r'),
        b't' => simple('\t'),
        b'0'..=b'7' => {
            let mut v = 0u32;
            let mut j = i;
            while j < b.len() && j < i + 3 && (b'0'..=b'7').contains(&b[j]) {
                v = v * 8 + (b[j] - b'0') as u32;
                j += 1;
            }
            Some((char::from_u32(v & 0xFF)?.to_string(), j))
        }
        b'x' => {
            let mut v = 0u32;
            let mut j = i + 1;
            while j < b.len() && j < i + 3 && b[j].is_ascii_hexdigit() {
                v = v * 16 + (b[j] as char).to_digit(16)?;
                j += 1;
            }
            if j == i + 1 {
                return simple('x');
            }
            Some((char::from_u32(v)?.to_string(), j))
        }
        b'u' | b'U' => {
            let n = if c == b'u' { 4 } else { 8 };
            let hex = text.get(i + 1..i + 1 + n)?;
            let v = u32::from_str_radix(hex, 16).ok()?;
            Some((char::from_u32(v)?.to_string(), i + 1 + n))
        }
        _ => {
            // Any other character stands for itself, multibyte included.
            let ch = text[i..].chars().next()?;
            Some((ch.to_string(), i + ch.len_utf8()))
        }
    }
}
