//! One quote-aware scanner for SQL text, and the normalisation that lets every
//! other scanner in the engine get away with knowing only standard strings.
//!
//! PostgreSQL has four ways to write a quoted run:
//!
//! * `'…'` — a standard string; `''` stands for one quote and a backslash is
//!   an ordinary character (`standard_conforming_strings` is on);
//! * `E'…'` — an escape string; a backslash escapes the character after it,
//!   so `\'` does *not* end the literal;
//! * `$tag$…$tag$` — a dollar-quoted string, inside which nothing is special;
//! * `"…"` — a quoted identifier; `""` stands for one quote.
//!
//! The engine has dozens of text scanners — clause splitters, keyword finders,
//! the tuple splitter, the comment stripper, the statement splitter, the
//! redaction and statistics normalisers — and nearly all of them know only the
//! first and the last. An escape string with an escaped quote inside therefore
//! ended early for all of them: `WHERE c LIKE E'it\'s%' ORDER BY id` read as a
//! literal `'it\'`, then the bare word `s%`, then a quote that swallowed the
//! `ORDER BY` — and the error named a column called `ORDER`.
//!
//! Teaching each scanner separately would leave the next one written to get it
//! wrong again. Instead [`normalize_string_literals`] rewrites escape strings
//! and dollar-quoted strings into the standard form they mean, once, where
//! statement text enters the engine. After that the only quoting any scanner
//! can meet is the one they all already handle. [`skip_quoted`] is the
//! scanner for code that sees text *before* that point, or that wants to be
//! right on its own.

use std::borrow::Cow;

/// A byte that continues an identifier. A quote or `$` right after one belongs
/// to (or follows) a name rather than opening something: `ELSE'x'` is the
/// keyword `ELSE` then a standard string, not an escape string.
#[inline]
pub fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// True when the `'` at `quote` opens an escape string: `E'…'` / `e'…'`, with
/// the `E` a word of its own rather than the end of an identifier.
#[inline]
pub fn is_escape_string_quote(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'E' | b'e')
        && (quote < 2 || !(is_identifier_byte(bytes[quote - 2]) || bytes[quote - 2] == b'$'))
}

/// Index just past the string literal opening at `start` (or the end of the
/// text when it is unterminated). In an escape string a backslash escapes
/// whatever byte follows it.
pub fn string_literal_end(bytes: &[u8], start: usize, escapes: bool) -> usize {
    let mut i = start + 1;
    loop {
        let found = if escapes {
            memchr::memchr2(b'\'', b'\\', &bytes[i..])
        } else {
            memchr::memchr(b'\'', &bytes[i..])
        };
        let Some(p) = found else {
            return bytes.len();
        };
        i += p;
        if bytes[i] == b'\\' {
            i = (i + 2).min(bytes.len());
            continue;
        }
        // A quote: doubled, it is an escaped quote and the literal goes on.
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return i + 1;
    }
}

/// Index just past the `quote`-delimited run opening at `start`, where a
/// doubled quote stands for itself (or the end of the text, unterminated).
pub fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while let Some(p) = memchr::memchr(quote, &bytes[i..]) {
        i += p;
        if bytes.get(i + 1) == Some(&quote) {
            i += 2;
            continue;
        }
        return i + 1;
    }
    bytes.len()
}

/// If a dollar quote opens at `start`, the length of its tag (`$$` is 2,
/// `$tag$` is 5). `None` when this `$` opens nothing — a `$1` placeholder
/// among them, since a tag cannot begin with a digit.
pub fn dollar_tag_len(bytes: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] >= 0x80) {
        if j == start + 1 && bytes[j].is_ascii_digit() {
            return None;
        }
        j += 1;
    }
    (j < bytes.len() && bytes[j] == b'$').then(|| j + 1 - start)
}

/// If a dollar-quoted string opens at `start`, the index just past its closing
/// tag — the end of the text when it is unterminated. `None` when this `$` is
/// not a dollar quote.
pub fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let tag_len = dollar_tag_len(bytes, start)?;
    let tag = &bytes[start..start + tag_len];
    let body = start + tag_len;
    Some(
        memchr::memmem::find(&bytes[body..], tag)
            .map_or(bytes.len(), |p| body + p + tag_len),
    )
}

/// Index just past the (possibly nested) block comment opening at `start`.
pub fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start + 2;
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// If a quoted run — string, escape string, dollar-quoted string or quoted
/// identifier — opens at `i`, the index just past it. Nothing inside one is
/// syntax.
#[inline]
pub fn skip_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes[i] {
        b'\'' => Some(string_literal_end(bytes, i, is_escape_string_quote(bytes, i))),
        b'"' => Some(quoted_end(bytes, i, b'"')),
        b'$' if i == 0 || !is_identifier_byte(bytes[i - 1]) => dollar_quote_end(bytes, i),
        _ => None,
    }
}

/// The first quoted identifier in `sql` holding a doubled quote (`"a""b"`,
/// the name `a"b`), as written; `None` when there is none. Strings, escape
/// strings, dollar quotes and comments are skipped.
///
/// The engine's identifier readers each strip a name's quotes their own way,
/// and none reads `""` as a quote: `CREATE TABLE "t"" x ""y" (…)` created a
/// table named `t`, and such a column failed with an unrelated error. Until
/// they share one reader, a statement naming such an identifier is refused
/// where it enters, before anything is created under a different name.
pub fn identifier_with_doubled_quote(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'"' {
            let end = quoted_end(bytes, i, b'"');
            // Between the opening quote and the closing one.
            let inner = &bytes[i + 1..end.saturating_sub(1).max(i + 1)];
            if memchr::memmem::find(inner, b"\"\"").is_some() {
                return Some(&sql[i..end]);
            }
            i = end;
            continue;
        }
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// The identifier `text` starts with (after whitespace), and the offset just
/// past it: a double-quoted one read whole, its spaces and case kept and
/// `""` read as `"`; a bare one up to the next whitespace, quotes trimmed, as
/// the readers that split on whitespace always took it. `None` for an empty
/// or unterminated name.
///
/// The one reader the statement parsers share for a name that may hold a
/// space: split on whitespace, `"c d" TEXT` was the column `c` of a type
/// `d" TEXT`, refused as "column type 'D"' is not supported yet".
pub fn leading_identifier(text: &str) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace())?;
    if bytes[start] == b'"' {
        let mut name = Vec::new();
        let mut i = start + 1;
        loop {
            match bytes.get(i)? {
                b'"' if bytes.get(i + 1) == Some(&b'"') => {
                    name.push(b'"');
                    i += 2;
                }
                b'"' => {
                    i += 1;
                    break;
                }
                b => {
                    name.push(*b);
                    i += 1;
                }
            }
        }
        let name = String::from_utf8(name).ok()?;
        return (!name.is_empty()).then_some((name, i));
    }
    let end = bytes[start..].iter().position(|b| b.is_ascii_whitespace()).map_or(bytes.len(), |n| start + n);
    let name = text[start..end].trim_matches('"').to_string();
    (!name.is_empty()).then_some((name, end))
}

/// Every qualified column reference in `text`: the byte span of its
/// qualifier including the final dot (`public.t.` in `public.t.id`, `t.` in
/// `t.*`), and the qualifier's names unquoted (`["public", "t"]`).
///
/// Quotes, comments and numbers are skipped, and so is a parenthesised
/// subquery, which has its own FROM. A schema-qualified call
/// (`pg_catalog.now()`) and what follows `::` are not column references.
fn qualified_references(text: &str) -> Vec<(usize, usize, Vec<String>)> {
    let bytes = text.as_bytes();
    // No dot, no qualifier: every statement's texts are asked, most of
    // them hold none, and the scan below is the per-statement cost.
    if memchr::memchr(b'.', bytes).is_none() {
        return Vec::new();
    }
    let ident_start = |b: u8| b.is_ascii_alphabetic() || b == b'_' || b == b'"' || b >= 0x80;
    let ident_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80;
    // One name at `i`: (where it starts, the offset past it), read without
    // a copy; only a qualifier's names are made into strings.
    let read = |i: usize| -> Option<(usize, usize)> {
        if bytes.get(i) == Some(&b'"') {
            let end = quoted_end(bytes, i, b'"');
            text.get(i + 1..end.saturating_sub(1))?;
            return Some((i, end));
        }
        let mut j = i;
        while j < bytes.len() && ident_byte(bytes[j]) {
            j += 1;
        }
        (j > i).then_some((i, j))
    };
    // A name as a qualifier carries it: kept quoted, since a quoted name is
    // matched by its exact case.
    let name_of = |(start, end): (usize, usize)| -> String {
        if bytes[start] == b'"' {
            let inner = &text[start + 1..end.saturating_sub(1)];
            format!("\"{}\"", inner.replace("\"\"", "\""))
        } else {
            text[start..end].to_string()
        }
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let b = bytes[i];
        if b == b'\'' || b == b'$' && dollar_tag_len(bytes, i).is_some() {
            i = skip_quoted(bytes, i).unwrap_or(i + 1).max(i + 1);
            continue;
        }
        if b == b'(' {
            // A subquery's names are its own.
            let inner = text[i + 1..].trim_start();
            let upper = inner.get(..6).map(str::to_ascii_uppercase).unwrap_or_default();
            if upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("VALUES") {
                i = matching_close(text, i).map_or(bytes.len(), |c| c + 1);
                continue;
            }
            i += 1;
            continue;
        }
        if b.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.' || bytes[i] == b'_') {
                i += 1;
            }
            continue;
        }
        if b == b':' && bytes.get(i + 1) == Some(&b':') {
            // A cast's type name, which may be schema-qualified.
            i += 2;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            while i < bytes.len() && (ident_byte(bytes[i]) || matches!(bytes[i], b'.' | b'"')) {
                i += 1;
            }
            continue;
        }
        if ident_start(b) && (i == 0 || !ident_byte(bytes[i - 1])) {
            // The whole dotted chain: each name's span.
            let mut names: smallvec::SmallVec<[(usize, usize); 4]> = smallvec::SmallVec::new();
            let mut j = i;
            let mut star = false;
            while let Some((start, end)) = read(j) {
                names.push((start, end));
                if bytes.get(end) == Some(&b'.') {
                    if bytes.get(end + 1) == Some(&b'*') {
                        star = true;
                        j = end + 2;
                        break;
                    }
                    if bytes.get(end + 1).is_some_and(|c| ident_start(*c)) {
                        j = end + 1;
                        continue;
                    }
                }
                j = end;
                break;
            }
            if names.is_empty() {
                i += 1;
                continue;
            }
            // `q.name(` is a schema-qualified function, not a column.
            let call = text[j..].trim_start().starts_with('(');
            let qualifiers = if star { names.len() } else { names.len() - 1 };
            if qualifiers > 0 && !call {
                let span_end = names[qualifiers - 1].1 + 1;
                out.push((i, span_end, names[..qualifiers].iter().map(|n| name_of(*n)).collect()));
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }
    out
}

/// Whether the qualifier `names` is one of `allowed`, each a dotted path
/// (`t`, `public.t`), compared name by name without regard to ASCII case.
fn qualifier_is(names: &[String], allowed: &str) -> bool {
    let parts: Vec<&str> = allowed.split('.').collect();
    parts.len() == names.len()
        && parts.iter().zip(names).all(|(a, n)| {
            // A quoted name is its exact spelling (`"T"` is not `t`), a bare
            // one folds (`T` is `t`), as PostgreSQL reads them. An allowed
            // name written quoted is held to that; one written bare may be a
            // catalogue name some caller unquoted, so it matches either way.
            let a: &str = a;
            let quoted_n = n.strip_prefix('"').and_then(|n| n.strip_suffix('"'));
            match a.strip_prefix('"').and_then(|a| a.strip_suffix('"')) {
                Some(a) => match quoted_n {
                    Some(quoted) => quoted == a,
                    None => n.to_ascii_lowercase() == a,
                },
                None => match quoted_n {
                    Some(quoted) => quoted == a,
                    None => a.eq_ignore_ascii_case(n),
                },
            }
        })
}

/// A qualifier's names as a message shows them, unquoted.
fn written(names: &[String]) -> String {
    names.iter().map(|n| n.trim_matches('"')).collect::<Vec<_>>().join(".")
}

/// The first qualifier in `text` (`q` in `q.col`, `public.t` in
/// `public.t.col`) that is none of `allowed`, as written; `None` when every
/// one names a relation the statement has.
///
/// A single-table statement's readers take the last segment of `q.col` and
/// drop the qualifier, so a qualifier naming no relation was ignored:
/// `DELETE FROM t WHERE nosuch.id = 1` deleted t's row 1, and `WHERE
/// nosuch.v > 0 OR id = 2` read t's `v`. PostgreSQL refuses both ("missing
/// FROM-clause entry", 42P01). `allowed` is what [`relation_qualifiers`]
/// gives, plus what a statement adds (`EXCLUDED`).
pub fn unknown_qualifier(text: &str, allowed: &[&str]) -> Option<String> {
    qualified_references(text)
        .into_iter()
        .find(|(_, _, names)| !allowed.iter().any(|a| qualifier_is(names, a)))
        .map(|(_, _, names)| written(&names))
}

/// [`unknown_qualifier`], [`respell_qualifier`] and [`strip_qualifiers`] in
/// one scan, for a text an `ON CONFLICT DO UPDATE` rewrites every row:
/// `Err` with the first qualifier none of `allowed` names (as written);
/// otherwise the text with `respell_from`'s qualifier written `respell_to.`
/// and each of `strip`'s removed.
pub fn resolve_qualifiers(
    text: &str,
    allowed: &[&str],
    respell_from: &str,
    respell_to: &str,
    strip: &[&str],
) -> Result<String, String> {
    let refs = qualified_references(text);
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for (start, end, names) in refs {
        if !allowed.iter().any(|a| qualifier_is(&names, a)) {
            return Err(written(&names));
        }
        if qualifier_is(&names, respell_from) {
            out.push_str(&text[from..start]);
            out.push_str(respell_to);
            out.push('.');
            from = end;
        } else if strip.iter().any(|s| qualifier_is(&names, s)) {
            out.push_str(&text[from..start]);
            from = end;
        }
    }
    out.push_str(&text[from..]);
    Ok(out)
}

/// Whether `text` is exactly `q.*` for a `q` in `allowed` (`t.*`,
/// `public.t.*`, `"x".*`).
pub fn is_qualified_star(text: &str, allowed: &[&str]) -> bool {
    let text = text.trim();
    let Some(head) = text.strip_suffix(".*") else { return false };
    let refs = qualified_references(text);
    refs.len() == 1
        && refs[0].0 == 0
        && refs[0].1 == head.len() + 1
        && allowed.iter().any(|a| qualifier_is(&refs[0].2, a))
}

/// `text` with each qualifier that is one of `strip` removed: `t0.v` as `v`,
/// `"public"."t"."id"` as `"id"`.
pub fn strip_qualifiers(text: &str, strip: &[&str]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for (start, end, names) in qualified_references(text) {
        if strip.iter().any(|s| qualifier_is(&names, s)) {
            out.push_str(&text[from..start]);
            from = end;
        }
    }
    out.push_str(&text[from..]);
    out
}

/// `text` with each qualifier that is `from` written as `to.`: `"excluded"."v"`
/// as `EXCLUDED."v"`, so one spelling is left for a reader that looks for it.
pub fn respell_qualifier(text: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut from_at = 0;
    for (start, end, names) in qualified_references(text) {
        if qualifier_is(&names, from) {
            out.push_str(&text[from_at..start]);
            out.push_str(to);
            out.push('.');
            from_at = end;
        }
    }
    out.push_str(&text[from_at..]);
    out
}

/// The qualifiers a single relation is known by: its alias alone when it has
/// one (PostgreSQL hides the name behind it), else its name and its name in
/// the default schema (`t`, `public.t`), and the schema-qualified name as
/// written when that is another schema.
pub fn relation_qualifiers(relation: &str, alias: Option<&str>) -> Vec<String> {
    if let Some(alias) = alias {
        // A quoted alias keeps its quotes: [`qualifier_is`] then holds it to
        // its exact spelling, as PostgreSQL does (`AS "X"` is not `x`).
        return vec![alias.to_string()];
    }
    let parts: Vec<String> = relation.split('.').map(|p| p.trim_matches('"').to_string()).collect();
    let name = parts.last().cloned().unwrap_or_default();
    let mut out = vec![name.clone(), format!("public.{name}")];
    if parts.len() > 1 {
        out.push(parts.join("."));
    }
    out
}

/// The refusal for a qualifier [`unknown_qualifier`] found, in PostgreSQL's
/// words (42P01).
pub fn missing_from_entry(qualifier: &str) -> String {
    format!("missing FROM-clause entry for table \"{qualifier}\"")
}

/// Whether `needle` occurs anywhere in `text`, ignoring ASCII case, quotes
/// and comments included: a scanner's cheap precondition, so one that can
/// only match where these bytes are can skip a statement that has none.
/// `needle` must not be empty.
pub fn contains_ignore_ascii_case(text: &str, needle: &[u8]) -> bool {
    let b = text.as_bytes();
    let first = needle[0];
    memchr::memchr2_iter(first.to_ascii_uppercase(), first.to_ascii_lowercase(), b)
        .any(|i| b.len() - i >= needle.len() && b[i..i + needle.len()].eq_ignore_ascii_case(needle))
}

/// If a comment opens at `i`, the index just past it (a line comment ends
/// before its newline).
#[inline]
pub fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match (bytes[i], bytes.get(i + 1)) {
        (b'-', Some(b'-')) => {
            Some(memchr::memchr(b'\n', &bytes[i..]).map_or(bytes.len(), |p| i + p))
        }
        (b'/', Some(b'*')) => Some(block_comment_end(bytes, i)),
        _ => None,
    }
}

/// Splits `list` on `delimiter` where it stands at nesting depth zero —
/// outside every quoted run, comment, `(…)` and `[…]` — returning each piece
/// trimmed and borrowed from `list`. A trailing empty piece is dropped
/// (`1, ` is one piece) while an empty one between delimiters is kept.
///
/// This is the shape every list splitter needs and most got wrong: a
/// splitter that tracks only quotes cuts `f(1, 2)`, `ARRAY[1,2]` and
/// `numeric(10,2)` into pieces, and when the piece count happens to match
/// what was expected the pieces are used as values.
pub fn split_top_level(list: &str, delimiter: u8) -> Vec<&str> {
    let bytes = list.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end;
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            c if c == delimiter && depth == 0 => {
                out.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let last = list[start..].trim();
    if !last.is_empty() {
        out.push(last);
    }
    out
}

/// Index of the bracket closing the one at `open` (`(` or `[`), skipping
/// quoted runs and comments; `None` when it never closes.
pub fn matching_close(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        // Only these bytes can open a quoted run or a comment or change the
        // depth; every other one is stepped over without the two probes,
        // which ran at each byte of every statement's several scans.
        if !matches!(bytes[i], b'(' | b'[' | b')' | b']' | b'\'' | b'"' | b'$' | b'-' | b'/') {
            i += 1;
            continue;
        }
        if i > open {
            if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
                i = end;
                continue;
            }
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrites every escape string (`E'…'`) and dollar-quoted string
/// (`$tag$…$tag$`) into the standard string it means, leaving everything else
/// — standard strings, quoted identifiers, comments, `$1` placeholders — byte
/// for byte as it was.
///
/// Borrowed when there is nothing to rewrite, which is nearly every statement.
/// An escape that does not decode — a byte sequence that is not UTF-8, a NUL,
/// a `\u` that names no character — is an error, as it is in PostgreSQL.
pub fn normalize_string_literals(sql: &str) -> Result<Cow<'_, str>, String> {
    let bytes = sql.as_bytes();
    if !needs_normalizing(bytes) {
        return Ok(zone_free_timestamp_literals(sql));
    }
    escapes_resolved(sql).map(|out| match zone_free_timestamp_literals(&out) {
        Cow::Borrowed(_) => Cow::Owned(out),
        Cow::Owned(rewritten) => Cow::Owned(rewritten),
    })
}

/// [`normalize_string_literals`]'s escape and dollar-quote rewrite.
fn escapes_resolved(sql: &str) -> Result<String, String> {
    let bytes = sql.as_bytes();

    let mut out = String::with_capacity(sql.len() + 8);
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        // Jump to the next byte that can open anything.
        match memchr::memchr3(b'\'', b'$', b'"', &bytes[i..]) {
            Some(p) => {
                // A comment can hide a quote; find whether one opens first.
                let next = i + p;
                if let Some(c) = first_comment_start(bytes, i, next) {
                    i = skip_comment(bytes, c).expect("a comment opens here");
                    continue;
                }
                i = next;
            }
            None => break,
        }
        match bytes[i] {
            b'\'' if is_escape_string_quote(bytes, i) => {
                let Some(end) = terminated_escape_literal_end(bytes, i) else {
                    // Unterminated: left for the parser to report.
                    i = bytes.len();
                    continue;
                };
                let body = &bytes[i + 1..end - 1];
                let decoded = decode_escape_body(body)?;
                // Drop the `E` already copied with the run before the quote.
                out.push_str(&sql[copied..i - 1]);
                push_standard_literal(&mut out, &decoded);
                i = end;
                copied = end;
            }
            b'\'' => i = string_literal_end(bytes, i, false),
            b'"' => i = quoted_end(bytes, i, b'"'),
            b'$' => {
                let opens = i == 0 || !is_identifier_byte(bytes[i - 1]);
                match opens.then(|| dollar_tag_len(bytes, i)).flatten() {
                    Some(tag_len) => {
                        let tag = &bytes[i..i + tag_len];
                        let body_start = i + tag_len;
                        match memchr::memmem::find(&bytes[body_start..], tag) {
                            Some(p) => {
                                let body = &sql[body_start..body_start + p];
                                out.push_str(&sql[copied..i]);
                                push_standard_literal(&mut out, body);
                                i = body_start + p + tag_len;
                                copied = i;
                            }
                            // Unterminated: left as written.
                            None => i = bytes.len(),
                        }
                    }
                    None => i += 1,
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// Every string literal the statement types as `timestamp` or `timestamptz`
/// (`'…'::timestamptz`, `TIMESTAMP '…'`, `CAST('…' AS timestamp with time
/// zone)`, a precision allowed) whose text carries a zone, rewritten with no
/// zone left to read two ways and the same meaning: the wall-clock time for
/// `timestamp`, which discards the zone, and the UTC time with `+00` for
/// `timestamptz`, which converts by it (the session time zone is UTC).
///
/// The two types read the same text differently, and the cast or type name
/// that says which does not reach every reader of the literal: the write path
/// drops a cast on a literal, the predicate rewrite drops one before the
/// tokenizer sees it, the typed-literal prefix is stripped before a value is
/// encoded, and the evaluator's casts name one storage type for both. So a
/// row written with `'… 10:00+02'::timestamptz` into a `timestamp` column was
/// stored as 10:00 (PostgreSQL: 08:00) and a predicate spelled the same way
/// looked for something else. With no zone left, every reader agrees.
///
/// Borrowed when the statement has no such literal: the cheap test is that
/// it never mentions `timestamp`.
pub fn zone_free_timestamp_literals(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    if !contains_ignore_ascii_case(sql, b"timestamp") {
        return Cow::Borrowed(sql);
    }
    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let Some(p) = memchr::memchr3(b'\'', b'"', b'$', &bytes[i..]).map(|p| i + p) else {
            break;
        };
        if let Some(c) = first_comment_start(bytes, i, p) {
            i = skip_comment(bytes, c).expect("a comment opens here");
            continue;
        }
        i = p;
        if bytes[i] != b'\'' || is_escape_string_quote(bytes, i) {
            i = skip_quoted(bytes, i).unwrap_or(i + 1);
            continue;
        }
        let end = string_literal_end(bytes, i, false);
        if end > bytes.len() || end < i + 2 || bytes[end - 1] != b'\'' {
            break;
        }
        let body = &sql[i + 1..end - 1];
        // `'…'::timestamptz::text`: the literal read at its type and shown
        // as that type's text, folded into one text literal here, as
        // PostgreSQL computes it. Through the evaluator both types are one
        // storage type, and `timestamp`'s text came out with `+00`.
        if let Some((text, span)) = (!body.contains('\'')).then(|| timestamp_text_cast(sql, end, body)).flatten() {
            let o = out.get_or_insert_with(|| String::with_capacity(sql.len()));
            o.push_str(&sql[copied..i + 1]);
            o.push_str(&text);
            o.push('\'');
            copied = end + span;
            i = end + span;
            continue;
        }
        let zoned = (!body.contains('\''))
            .then(|| timestamp_type_of_literal(sql, i, end))
            .flatten()
            .and_then(|with_zone| zone_free(body, with_zone));
        if let Some(text) = zoned {
            let o = out.get_or_insert_with(|| String::with_capacity(sql.len()));
            o.push_str(&sql[copied..i + 1]);
            o.push_str(&text);
            copied = end - 1;
        }
        i = end;
    }
    match out {
        None => Cow::Borrowed(sql),
        Some(mut o) => {
            o.push_str(&sql[copied..]);
            Cow::Owned(o)
        }
    }
}

/// After a literal ending at `end`: `::timestamp[tz][(p)][ with[out] time
/// zone]::text` (or `::varchar`), as the literal's text in that type and the
/// length of the casts after the literal; `None` for anything else.
fn timestamp_text_cast(sql: &str, end: usize, body: &str) -> Option<(String, usize)> {
    let rest = &sql[end..];
    let after = rest.trim_start();
    let cast = after.strip_prefix("::")?;
    let cast_trimmed = cast.trim_start();
    let (with_zone, n) = timestamp_type_full(cast_trimmed)?;
    let tail = cast_trimmed[n..].trim_start();
    let second = tail.strip_prefix("::")?.trim_start();
    let word_len = second.bytes().take_while(|b| is_identifier_byte(*b)).count();
    let word = &second[..word_len];
    if !(word.eq_ignore_ascii_case("text") || word.eq_ignore_ascii_case("varchar")) {
        return None;
    }
    // A typmod or an array on the text type is not this fold's.
    if matches!(second[word_len..].trim_start().as_bytes().first(), Some(b'(' | b'[')) {
        return None;
    }
    let text = timestamp_text(body, with_zone)?;
    let consumed = rest.len() - second[word_len..].len();
    Some((text, consumed))
}

/// A timestamp type name at the start of `text`, as [`timestamp_type_at`]
/// reads it, with the `with[out] time zone` words counted in its length.
fn timestamp_type_full(text: &str) -> Option<(bool, usize)> {
    let (with_zone, n) = timestamp_type_at(text)?;
    let rest = &text[n..];
    let mut at = n;
    let mut words = rest.split_whitespace();
    let zone = matches!(
        (words.next(), words.next(), words.next()),
        (Some(a), Some(b), Some(c)) if (a.eq_ignore_ascii_case("with") || a.eq_ignore_ascii_case("without"))
            && b.eq_ignore_ascii_case("time")
            && c.len() >= 4 && c[..4].eq_ignore_ascii_case("zone")
    );
    if zone {
        let lower = rest.to_ascii_lowercase();
        let z = lower.find("zone").expect("seen");
        at += z + 4;
    }
    Some((with_zone, at))
}

/// `body` as a `timestamp` (`with_zone` false) or `timestamptz` value's
/// text: no zone for the first, `+00` for the second (the session is UTC).
fn timestamp_text(body: &str, with_zone: bool) -> Option<String> {
    use crate::core::value_codec::{format_timestamp_us, parse_timestamp_local_us, parse_timestamp_us};
    let us = if with_zone { parse_timestamp_us(body)? } else { parse_timestamp_local_us(body)? };
    let mut text = format_timestamp_us(us);
    if with_zone {
        text.push_str("+00");
    }
    Some(text)
}

/// Whether the timestamps an expression's text makes are all `timestamp`
/// (without time zone): it names that type (`::timestamp`, `AS timestamp`)
/// or `LOCALTIMESTAMP`, and nothing that makes a `timestamptz` (that type, or
/// `now()` and its spellings). The value it computes is one storage type
/// either way, so this is what tells a text column whether it is shown with
/// `+00`: `CASE … THEN '…'::timestamp END` is bare in PostgreSQL, `CASE …
/// THEN now() END` is not. A text-level reading: a value cast back and forth
/// (`'…'::timestamptz::timestamp`) is counted as zoned.
pub fn names_only_local_timestamps(text: &str) -> bool {
    let bytes = text.as_bytes();
    let (mut local, mut zoned) = (false, false);
    let word_at = |i: usize, w: &str| {
        text.get(i..i + w.len()).is_some_and(|h| h.eq_ignore_ascii_case(w))
            && (i == 0 || !is_identifier_byte(bytes[i - 1]))
            && bytes.get(i + w.len()).is_none_or(|b| !is_identifier_byte(*b))
    };
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                i = string_literal_end(bytes, i, false).max(i + 1);
                continue;
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                if let Some((with_zone, _)) = timestamp_type_full(text[i + 2..].trim_start()) {
                    if with_zone { zoned = true } else { local = true }
                }
                i += 2;
                continue;
            }
            _ => {}
        }
        if word_at(i, "AS") {
            if let Some((with_zone, _)) = timestamp_type_full(text[i + 2..].trim_start()) {
                if with_zone { zoned = true } else { local = true }
            }
        } else if word_at(i, "LOCALTIMESTAMP") {
            local = true;
        } else if ["NOW", "CURRENT_TIMESTAMP", "CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP"]
            .iter()
            .any(|w| word_at(i, w))
        {
            zoned = true;
        }
        i += 1;
    }
    local && !zoned
}

/// A lone literal cast to `timestamp` or `timestamptz` (`'…'::timestamptz`,
/// `('…')::timestamp`, `CAST('…' AS timestamptz)`), as PostgreSQL shows the
/// value as text: with `+00` for `timestamptz` (the session is UTC), with no
/// zone for `timestamp`. `None` for anything else.
///
/// The value written into a text column. Evaluated as a cast it becomes the
/// one storage type both are, which is shown as `timestamptz`'s text, so a
/// `timestamp` cast needs reading here to stay bare.
pub fn timestamp_cast_as_text(value: &str) -> Option<String> {
    let value = value.trim();
    // A lone quoted literal at the start of `text`: its body and its length.
    let literal = |text: &str| -> Option<(String, usize)> {
        let bytes = text.as_bytes();
        if bytes.first() != Some(&b'\'') {
            return None;
        }
        let end = string_literal_end(bytes, 0, false);
        if end < 2 || bytes[end - 1] != b'\'' || text[1..end - 1].contains('\'') {
            return None;
        }
        Some((text[1..end - 1].to_string(), end))
    };
    // The whole of `text` is one timestamp type name.
    let whole_type = |text: &str| -> Option<bool> {
        let text = text.trim();
        let (with_zone, n) = timestamp_type_full(text)?;
        text[n..].trim().is_empty().then_some(with_zone)
    };
    // `CAST('…' AS type)`.
    let cast_word = value.get(..4).is_some_and(|w| w.eq_ignore_ascii_case("CAST"));
    if cast_word {
        let inner = value[4..].trim_start().strip_prefix('(')?.strip_suffix(')')?.trim();
        let (body, len) = literal(inner)?;
        let rest = inner[len..].trim_start();
        let as_word = rest.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
            && rest.as_bytes().get(2).is_some_and(|b| b.is_ascii_whitespace());
        if !as_word {
            return None;
        }
        return timestamp_text(&body, whole_type(&rest[2..])?);
    }
    // `'…'::type`, `('…')::type`, `(('…'))::type`.
    let mut inner = value;
    let mut depth = 0usize;
    while let Some(open) = inner.strip_prefix('(') {
        inner = open.trim_start();
        depth += 1;
    }
    let (body, len) = literal(inner)?;
    let mut after = inner[len..].trim_start();
    for _ in 0..depth {
        after = after.strip_prefix(')')?.trim_start();
    }
    let cast = after.strip_prefix("::")?;
    timestamp_text(&body, whole_type(cast)?)
}

/// Whether `text` holds a string literal that reads as a timestamp with a
/// zone in it (`'… 09:00+05'`): one that `timestamp` and `timestamptz` read as
/// two different values.
pub fn has_zoned_timestamp_literal(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while let Some(p) = memchr::memchr(b'\'', &bytes[i..]).map(|p| i + p) {
        let end = string_literal_end(bytes, p, is_escape_string_quote(bytes, p));
        if end >= p + 2 {
            let body = &text[p + 1..end - 1];
            if zone_free(body, true).is_some() {
                return true;
            }
        }
        i = end.max(p + 1);
        if i >= bytes.len() {
            break;
        }
    }
    false
}

/// `body` with its zone resolved away, or `None` when it has none to resolve
/// (or is not a timestamp, or would not read back as the same instant).
fn zone_free(body: &str, with_zone: bool) -> Option<String> {
    use crate::core::value_codec::{format_timestamp_us, parse_timestamp_local_us, parse_timestamp_us};
    let (utc, wall) = (parse_timestamp_us(body)?, parse_timestamp_local_us(body)?);
    if utc == wall {
        return None;
    }
    let us = if with_zone { utc } else { wall };
    // A `timestamptz` keeps an explicit `+00`, which every reader takes as
    // the same instant and which is how PostgreSQL shows it as text: written
    // into a text column, `'… 10:00+02'::timestamptz` is `… 08:00:00+00`.
    let mut text = format_timestamp_us(us);
    if with_zone {
        text.push_str("+00");
    }
    (parse_timestamp_us(&text) == Some(us)).then_some(text)
}

/// Whether the literal at `start..end` is typed `timestamptz` (`Some(true)`)
/// or `timestamp` (`Some(false)`) by a cast after it, a type name before it,
/// or `CAST(… AS …)` around it; `None` when it is not typed either way.
fn timestamp_type_of_literal(sql: &str, start: usize, end: usize) -> Option<bool> {
    let after = sql[end..].trim_start();
    // `('…')::timestamp`, `(('…'))::timestamp`: a literal alone in
    // parentheses (not a call's argument, `f('…')::timestamp`), cast as one.
    let (mut inner_after, mut inner_before) = (after, sql[..start].trim_end());
    while let (Some(rest), Some(open)) = (inner_after.strip_prefix(')'), inner_before.strip_suffix('(')) {
        let open = open.trim_end();
        if open.as_bytes().last().is_some_and(|b| is_identifier_byte(*b)) {
            break;
        }
        (inner_after, inner_before) = (rest.trim_start(), open);
        if let Some(cast) = inner_after.strip_prefix("::") {
            return timestamp_type_at(cast.trim_start()).map(|(tz, _)| tz);
        }
    }
    if let Some(cast) = after.strip_prefix("::") {
        return timestamp_type_at(cast.trim_start()).map(|(tz, _)| tz);
    }
    let before = sql[..start].trim_end();
    if let Some(open) = before.strip_suffix('(') {
        let head = open.trim_end();
        let cast_word = head.len() >= 4 && head[head.len() - 4..].eq_ignore_ascii_case("CAST");
        let word_start = head.len().saturating_sub(4);
        let bounded = word_start == 0 || !is_identifier_byte(head.as_bytes()[word_start - 1]);
        if cast_word && bounded {
            let rest = after;
            let as_word = rest.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
                && rest.as_bytes().get(2).is_some_and(|b| !is_identifier_byte(*b));
            if as_word {
                return timestamp_type_at(rest[2..].trim_start()).map(|(tz, _)| tz);
            }
        }
        return None;
    }
    timestamp_type_before(before)
}

/// A timestamp type name at the start of `text`: whether it carries a zone,
/// and its length. `timestamptz`, `timestamp`, either with a `(p)`, and
/// `timestamp[(p)] with[out] time zone`.
fn timestamp_type_at(text: &str) -> Option<(bool, usize)> {
    let word = |t: &str, w: &str| {
        t.get(..w.len()).is_some_and(|h| h.eq_ignore_ascii_case(w))
            && t.as_bytes().get(w.len()).is_none_or(|b| !is_identifier_byte(*b))
    };
    let precision = |t: &str| -> usize {
        let rest = t.trim_start();
        let lead = t.len() - rest.len();
        match rest.strip_prefix('(') {
            Some(inner) => match inner.find(')') {
                Some(close) if inner[..close].trim().bytes().all(|b| b.is_ascii_digit()) => lead + 1 + close + 1,
                _ => 0,
            },
            None => 0,
        }
    };
    if word(text, "timestamptz") {
        let n = "timestamptz".len();
        return Some((true, n + precision(&text[n..])));
    }
    if !word(text, "timestamp") {
        return None;
    }
    let mut n = "timestamp".len();
    n += precision(&text[n..]);
    let rest = &text[n..];
    let words: Vec<&str> = rest.split_whitespace().take(3).collect();
    let zone = |kind: &str| {
        words.len() == 3
            && words[0].eq_ignore_ascii_case(kind)
            && words[1].eq_ignore_ascii_case("time")
            && words[2].len() >= 4
            && words[2][..4].eq_ignore_ascii_case("zone")
            && words[2].as_bytes().get(4).is_none_or(|b| !is_identifier_byte(*b))
    };
    if zone("with") {
        return Some((true, n));
    }
    Some((false, n))
}

/// A timestamp type name ending `before` (whitespace-trimmed), the type of a
/// typed literal `TIMESTAMP '…'`: whether it carries a zone.
fn timestamp_type_before(before: &str) -> Option<bool> {
    // The last few words, read backwards: identifier runs, with a `(p)`
    // precision folded into the word before it.
    let b = before.as_bytes();
    let mut words: Vec<String> = Vec::new();
    let mut k = b.len();
    let mut first_at = k;
    while words.len() < 4 && k > 0 {
        while k > 0 && b[k - 1].is_ascii_whitespace() {
            k -= 1;
        }
        let mut stop = k;
        if k > 0 && b[k - 1] == b')' {
            let mut j = k - 1;
            while j > 0 && (b[j - 1].is_ascii_digit() || b[j - 1] == b' ') {
                j -= 1;
            }
            if j == 0 || b[j - 1] != b'(' {
                break;
            }
            stop = j - 1;
            while stop > 0 && b[stop - 1].is_ascii_whitespace() {
                stop -= 1;
            }
        }
        let mut j = stop;
        while j > 0 && is_identifier_byte(b[j - 1]) {
            j -= 1;
        }
        if j == stop {
            break;
        }
        words.push(before[j..stop].to_ascii_lowercase());
        first_at = j;
        k = j;
    }
    words.reverse();
    // The type must be a word of its own: not a qualified name (`t.timestamp`),
    // a quoted identifier, or the tail of a cast (`x::timestamp '…'`).
    let lead = |at: usize| at == 0 || !matches!(b[at - 1], b'.' | b'"' | b':');
    let ty = |w: &str| match w {
        "timestamptz" => Some(true),
        "timestamp" => Some(false),
        _ => None,
    };
    let n = words.len();
    if n >= 4 && words[n - 2] == "time" && words[n - 1] == "zone" && ty(&words[n - 4]) == Some(false) {
        // `first_at` is where the earliest word read starts; with four words
        // read that is the type's.
        if n == 4 && !lead(first_at) {
            return None;
        }
        return match words[n - 3].as_str() {
            "with" => Some(true),
            "without" => Some(false),
            _ => None,
        };
    }
    let last = words.last()?;
    // The last word's start: the text before it must let it stand alone.
    let last_start = {
        let trimmed = before.trim_end();
        let tb = trimmed.as_bytes();
        let mut j = tb.len();
        if j > 0 && tb[j - 1] == b')' {
            while j > 0 && tb[j - 1] != b'(' {
                j -= 1;
            }
            j = j.saturating_sub(1);
            while j > 0 && tb[j - 1].is_ascii_whitespace() {
                j -= 1;
            }
        }
        while j > 0 && is_identifier_byte(tb[j - 1]) {
            j -= 1;
        }
        j
    };
    if !lead(last_start) {
        return None;
    }
    ty(last)
}

/// [`string_literal_end`] for an escape string, `None` when it never closes.
fn terminated_escape_literal_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    loop {
        i += memchr::memchr2(b'\'', b'\\', &bytes[i..])?;
        if bytes[i] == b'\\' {
            i += 2;
            if i > bytes.len() {
                return None;
            }
            continue;
        }
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return Some(i + 1);
    }
}

/// Cheap pre-check: is there any `E'` or `$tag$` at all? A false positive only
/// costs the full pass.
fn needs_normalizing(bytes: &[u8]) -> bool {
    for p in memchr::memchr2_iter(b'\'', b'$', bytes) {
        match bytes[p] {
            b'\'' if is_escape_string_quote(bytes, p) => return true,
            b'$' if (p == 0 || !is_identifier_byte(bytes[p - 1]))
                && dollar_tag_len(bytes, p).is_some() =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// The first `--` or `/*` in `bytes[from..to]`, if any.
fn first_comment_start(bytes: &[u8], from: usize, to: usize) -> Option<usize> {
    let window = &bytes[from..to];
    let mut at = 0usize;
    while let Some(p) = memchr::memchr2(b'-', b'/', &window[at..]) {
        let k = from + at + p;
        if skip_comment(bytes, k).is_some() {
            return Some(k);
        }
        at += p + 1;
    }
    None
}

/// `'` + `body` with every quote doubled + `'`.
fn push_standard_literal(out: &mut String, body: &str) {
    out.reserve(body.len() + 2);
    out.push('\'');
    let mut start = 0usize;
    for at in memchr::memchr_iter(b'\'', body.as_bytes()) {
        out.push_str(&body[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&body[start..]);
    out.push('\'');
}

/// Decodes the body of an escape string the way PostgreSQL does: `\b \f \n \r
/// \t`, octal `\o`–`\ooo`, hex `\xh`–`\xhh`, `\uXXXX` and `\UXXXXXXXX`
/// (surrogate pairs combined), `''` as one quote, and any other escaped
/// character as itself.
fn decode_escape_body(body: &[u8]) -> Result<String, String> {
    let mut out: Vec<u8> = Vec::with_capacity(body.len());
    let mut i = 0usize;
    let hex = |b: u8| (b as char).to_digit(16);
    while i < body.len() {
        let b = body[i];
        if b == b'\'' {
            // Only a doubled quote can sit inside the body.
            out.push(b'\'');
            i += 2;
            continue;
        }
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let Some(&e) = body.get(i + 1) else {
            out.push(b'\\');
            break;
        };
        i += 2;
        match e {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'0'..=b'7' => {
                let mut v = u32::from(e - b'0');
                let mut n = 1;
                while n < 3 && i < body.len() && (b'0'..=b'7').contains(&body[i]) {
                    v = v * 8 + u32::from(body[i] - b'0');
                    i += 1;
                    n += 1;
                }
                out.push((v & 0xff) as u8);
            }
            b'x' if body.get(i).and_then(|b| hex(*b)).is_some() => {
                let mut v = hex(body[i]).unwrap();
                i += 1;
                if let Some(d) = body.get(i).and_then(|b| hex(*b)) {
                    v = v * 16 + d;
                    i += 1;
                }
                out.push(v as u8);
            }
            b'u' | b'U' => {
                let width = if e == b'u' { 4 } else { 8 };
                let mut code = read_hex(body, i, width)
                    .ok_or_else(|| "invalid Unicode escape: \\u needs 4 hex digits, \\U 8".to_string())?;
                i += width;
                if (0xD800..0xDC00).contains(&code) {
                    // A high surrogate must be followed by `\uDC00`–`\uDFFF`.
                    let low = (body.get(i) == Some(&b'\\') && body.get(i + 1) == Some(&b'u'))
                        .then(|| read_hex(body, i + 2, 4))
                        .flatten()
                        .filter(|l| (0xDC00..0xE000).contains(l))
                        .ok_or_else(|| "invalid Unicode surrogate pair".to_string())?;
                    i += 6;
                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                }
                let c = char::from_u32(code)
                    .ok_or_else(|| format!("invalid Unicode escape value {:#x}", code))?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
            other => out.push(other),
        }
    }
    if out.contains(&0) {
        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
    }
    String::from_utf8(out).map_err(|e| {
        format!(
            "invalid byte sequence for encoding \"UTF8\" in an escape string at byte {}",
            e.utf8_error().valid_up_to()
        )
    })
}

fn read_hex(body: &[u8], at: usize, width: usize) -> Option<u32> {
    let digits = body.get(at..at + width)?;
    let mut v = 0u32;
    for d in digits {
        v = v * 16 + (*d as char).to_digit(16)?;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    /// Each spelling PostgreSQL types a literal with, rewritten to the value
    /// that type reads; everything else left byte for byte.
    #[test]
    fn a_typed_timestamp_literal_loses_its_zone_and_keeps_its_meaning() {
        use super::zone_free_timestamp_literals as z;
        let t = "'2024-01-01 10:00:00+02'";
        for (sql, want) in [
            (format!("SELECT {t}::timestamptz"), "SELECT '2024-01-01 08:00:00+00'::timestamptz"),
            (format!("SELECT {t} :: TIMESTAMPTZ(3)"), "SELECT '2024-01-01 08:00:00+00' :: TIMESTAMPTZ(3)"),
            (format!("SELECT {t}::timestamp"), "SELECT '2024-01-01 10:00:00'::timestamp"),
            (format!("SELECT {t}::timestamp with time zone"), "SELECT '2024-01-01 08:00:00+00'::timestamp with time zone"),
            (format!("SELECT {t}::timestamp(0) without time zone"), "SELECT '2024-01-01 10:00:00'::timestamp(0) without time zone"),
            (format!("VALUES (TIMESTAMPTZ {t})"), "VALUES (TIMESTAMPTZ '2024-01-01 08:00:00+00')"),
            (format!("VALUES (timestamp {t})"), "VALUES (timestamp '2024-01-01 10:00:00')"),
            (format!("VALUES(TIMESTAMP(3) {t})"), "VALUES(TIMESTAMP(3) '2024-01-01 10:00:00')"),
            (format!("VALUES (TIMESTAMP WITH TIME ZONE {t})"), "VALUES (TIMESTAMP WITH TIME ZONE '2024-01-01 08:00:00+00')"),
            (format!("VALUES (CAST({t} AS timestamptz))"), "VALUES (CAST('2024-01-01 08:00:00+00' AS timestamptz))"),
            (format!("VALUES (cast ( {t} as timestamp))"), "VALUES (cast ( '2024-01-01 10:00:00' as timestamp))"),
            (format!("VALUES (({t})::timestamp)"), "VALUES (('2024-01-01 10:00:00')::timestamp)"),
            (format!("VALUES (( {t} ) :: timestamptz)"), "VALUES (( '2024-01-01 08:00:00+00' ) :: timestamptz)"),
        ] {
            assert_eq!(z(&sql), want, "{sql}");
        }
        // Not typed, not a timestamp, no zone, or not a literal at all.
        for sql in [
            format!("SELECT * FROM t WHERE timestamp_col = {t}"),
            format!("SELECT {t}::text FROM timestamps"),
            format!("SELECT x AT TIME ZONE {t}"),
            "SELECT 'now'::timestamptz".to_string(),
            "SELECT '2024-01-01 10:00:00'::timestamptz".to_string(),
            "SELECT '2024-01-01 10:00:00Z'::timestamp".to_string(),
            format!("SELECT \"{t}\"::timestamptz"),
            format!("SELECT t.timestamp {t}"),
            format!("SELECT f({t})::timestamp"),
            format!("SELECT 1 -- {t}::timestamptz\n"),
            format!("SELECT E{t}::text, $$x$$::timestamp"),
        ] {
            assert_eq!(z(&sql), sql.as_str(), "{sql}");
        }
        // Through the entry point every statement takes.
        assert_eq!(
            super::normalize_string_literals(&format!("SELECT E'a', {t}::timestamptz")).unwrap(),
            "SELECT 'a', '2024-01-01 08:00:00+00'::timestamptz"
        );
        assert!(super::has_zoned_timestamp_literal("a > '2024-01-01 09:00:00+05'"));
        assert!(!super::has_zoned_timestamp_literal("a > '2024-01-01 09:00:00'"));
    }

    #[test]
    fn a_doubled_quote_in_an_identifier_is_found_and_nowhere_else() {
        use super::identifier_with_doubled_quote as found;
        assert_eq!(found(r#"CREATE TABLE "t"" x ""y" (id INT)"#), Some(r#""t"" x ""y""#));
        assert_eq!(found(r#"SELECT "a""b" FROM t"#), Some(r#""a""b""#));
        assert_eq!(found(r#"SELECT "a""" FROM t"#), Some(r#""a""""#));
        assert_eq!(found(r#"SELECT "a" FROM "b""#), None);
        assert_eq!(found(r#"SELECT "" FROM t"#), None);
        assert_eq!(found(r#"SELECT 'x""y', $$""$$ FROM t -- "a""b""#), None);
        assert_eq!(found(r#"SELECT 1 /* "a""b" */"#), None);
    }

    use super::*;

    #[test]
    fn a_relation_star_is_read_as_its_columns() {
        let allowed = ["t", "public.t"];
        assert!(is_qualified_star("t.*", &allowed));
        assert!(is_qualified_star("public.t.*", &allowed));
        assert!(is_qualified_star("T.*", &allowed));
        assert!(!is_qualified_star("u.*", &allowed));
        assert!(!is_qualified_star("t.*, id", &allowed));
        assert!(!is_qualified_star("\"T\".*", &allowed));
    }

    #[test]
    fn a_quoted_alias_is_its_exact_spelling() {
        let quoted = relation_qualifiers("t", Some("\"X\""));
        let quoted: Vec<&str> = quoted.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("\"X\".v + 1", &quoted), None);
        assert_eq!(unknown_qualifier("x.v + 1", &quoted).as_deref(), Some("x"));
        assert_eq!(unknown_qualifier("X.v + 1", &quoted).as_deref(), Some("X"));
        let lower = relation_qualifiers("t", Some("\"x\""));
        let lower: Vec<&str> = lower.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("X.v + x.v + \"x\".v", &lower), None);
        let bare = relation_qualifiers("t", Some("x"));
        let bare: Vec<&str> = bare.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("X.v + x.v", &bare), None);
    }

    fn norm(sql: &str) -> String {
        normalize_string_literals(sql).unwrap().into_owned()
    }

    #[test]
    fn escape_strings_become_standard_strings() {
        assert_eq!(norm(r"SELECT E'it\'s%'"), "SELECT 'it''s%'");
        assert_eq!(norm(r"x = e'a\\b'"), r"x = 'a\b'");
        assert_eq!(norm(r"E'\n\t\x41\101é\U0001F642'"), "'\n\tAAé🙂'");
        assert_eq!(norm(r"E'a''b'"), "'a''b'");
        assert_eq!(norm(r"E'🙂'"), "'🙂'");
        assert_eq!(norm(r"E'\q'"), "'q'");
        // Unterminated (the last quote is escaped): left as written.
        assert_eq!(norm(r"x = E'abc\'"), r"x = E'abc\'");
        // Not an escape-string prefix: the E ends a word.
        assert_eq!(norm(r"CASE WHEN x THEN 1 ELSE'\' END"), r"CASE WHEN x THEN 1 ELSE'\' END");
        // Inside a standard string, a comment or an identifier: untouched.
        assert_eq!(norm(r"'E''x' -- E'\'' "), r"'E''x' -- E'\'' ");
        assert_eq!(norm(r#"/* E'\' */ "E'" "#), r#"/* E'\' */ "E'" "#);
    }

    #[test]
    fn dollar_quotes_become_standard_strings() {
        assert_eq!(norm("SELECT $$a, 'b$$"), "SELECT 'a, ''b'");
        assert_eq!(norm("x = $t$ $$ $1 $t$, $1"), "x = ' $$ $1 ', $1");
        // Placeholders and identifiers with `$` are left alone.
        assert_eq!(norm("WHERE a$b$ = $1 AND c = $2"), "WHERE a$b$ = $1 AND c = $2");
        // Unterminated: as written.
        assert_eq!(norm("SELECT $$abc"), "SELECT $$abc");
    }

    #[test]
    fn bad_escapes_are_errors() {
        assert!(normalize_string_literals(r"E'\0'").is_err());
        assert!(normalize_string_literals(r"E'\xff'").is_err());
        assert!(normalize_string_literals(r"E'\u12'").is_err());
        assert!(normalize_string_literals(r"E'\uD83D'").is_err());
    }

    #[test]
    fn nothing_to_do_borrows() {
        assert!(matches!(
            normalize_string_literals("SELECT 'x' FROM t WHERE a = $1"),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn top_level_split_tracks_nesting() {
        assert_eq!(
            split_top_level(r"1, f(2, 3), ARRAY[4,5], '6,7'::text, E'8\',9', $$1,0$$, ", b','),
            vec!["1", "f(2, 3)", "ARRAY[4,5]", "'6,7'::text", r"E'8\',9'", "$$1,0$$"]
        );
        assert_eq!(split_top_level("a,,b", b','), vec!["a", "", "b"]);
        assert_eq!(matching_close("(a, (b), ')', E'\\')') x", 0), Some(20));
    }
}
