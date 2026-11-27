//! A `WHERE` clause's comparisons under a nondeterministic collation.
//!
//! A comparison with a case-insensitive column as an operand reads the
//! column's key companion (`core::rql::collation`) instead, and its other
//! side is folded the same way: a string literal in place, so the clause
//! stays the column-against-literal shape every index and filter answers,
//! anything else through the key function. `name = 'Bob'` becomes
//! `__ci_name = 'bob'`; `name IN ('A', $1)` becomes `__ci_name IN ('a',
//! __rdb_ci_key($1))`; `name LIKE 'B%'` becomes `__ci_name LIKE 'b%'`.
//!
//! Comparisons answered by the bytes otherwise would be different answers,
//! so what this cannot carry over is refused: a regular expression or
//! `SIMILAR TO` over such a column (PostgreSQL refuses them too:
//! "nondeterministic collations are not supported for regular
//! expressions"), `IN (SELECT …)`, and two columns of different collations
//! compared (42P22).
//!
//! Text made from such a column (a concatenation, `COALESCE`, `upper`,
//! `trim`, `substring`, …: `TEXT_CALLS`) is text under the column's
//! collation, compared on keys through the key function. Any other
//! expression over one (`length(name) = 3`) is compared as it is.

use std::cell::Cell;

use smallvec::SmallVec;

use super::predicate_rewrite::{Tok, lex, resolve_column};
use crate::core::rql::collation::{self, Strength};
use crate::core::row::schema::SchemaField;

/// The clause with each comparison over a nondeterministic column on its
/// key; unchanged when the table has none.
pub(crate) fn rewrite(clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if !collation::any_order(schema) {
        return Ok(clause.to_string());
    }
    rewrite_with(clause, schema)
}

/// [`rewrite`] over the columns `columns` resolves: one table's, or the
/// relations of a join ([`Relations`]).
pub(crate) fn rewrite_with(clause: &str, columns: &dyn Columns) -> Result<String, String> {
    if clause.trim().is_empty() {
        return Ok(clause.to_string());
    }
    // Term by term, and a term this changes nothing in is kept as it was
    // written. The lexer is the scan's: it drops a cast's parentheses and
    // respaces what it reads, which is the same condition to the scan and
    // not to the evaluator's lift after this pass. Rendered back whole, a
    // clause this had nothing to say about came out changed: `(floor(extract
    // (month FROM (f(d.placed, d.__t7_placed))::timestamp)))::int = 4` on a
    // table with a case-insensitive column answered "column d.__t7_placed
    // does not exist".
    let mut out = String::with_capacity(clause.len() + 16);
    for (k, piece) in and_terms(clause).into_iter().enumerate() {
        if k > 0 {
            out.push_str(" AND ");
        }
        // `||` and the bitwise operators (`#`, `&`, `|`, `~`) are not the
        // lexer's tokens (an operand holding one is the evaluator's, lifted
        // after this pass), and the clause was refused here with
        // "unexpected character '|'": every string concatenation in the
        // WHERE of a table with such a column, which for the SQL Server
        // front end is every table. Carried through as words and written
        // back.
        let masked = mask_operators(piece);
        let toks = lex(masked.as_deref().unwrap_or(piece))?;
        let touched = Cell::new(false);
        let rendered = condition(&toks, columns, &touched)?.join(" ");
        if !touched.get() {
            out.push_str(piece);
        } else if masked.is_some() {
            out.push_str(&unmask_operators(&rendered));
        } else {
            out.push_str(&rendered);
        }
    }
    Ok(out)
}

/// `clause` at its top-level `AND`s: not one inside parentheses, a quoted
/// run or a `CASE`, nor a `BETWEEN`'s own. Joined by ` AND ` they are the
/// clause again; an `OR` stays inside its piece.
fn and_terms(clause: &str) -> Vec<&str> {
    let bytes = clause.as_bytes();
    let word_at = |at: usize, word: &str| -> bool {
        clause.get(at..at + word.len()).is_some_and(|w| w.eq_ignore_ascii_case(word))
            && (at == 0 || !(bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_' || bytes[at - 1] == b'.'))
            && bytes.get(at + word.len()).is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
    };
    let mut out = Vec::new();
    let (mut depth, mut cases, mut between) = (0i32, 0i32, false);
    let (mut start, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b if depth == 0 && b.is_ascii_alphabetic() => {
                if word_at(i, "CASE") {
                    cases += 1;
                } else if cases > 0 && word_at(i, "END") {
                    cases -= 1;
                } else if cases == 0 && word_at(i, "BETWEEN") {
                    between = true;
                } else if cases == 0 && word_at(i, "AND") {
                    if between {
                        between = false;
                    } else {
                        out.push(clause[start..i].trim());
                        start = i + "AND".len();
                    }
                }
                // Past the word, so its letters are not read as another.
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        // (A whole character: never into the middle of one.)
        i += clause[i..].chars().next().map_or(1, char::len_utf8);
    }
    out.push(clause[start..].trim());
    out
}

/// What an operator run is carried through the lexer as: this, its bytes
/// in hex, and `__` ([`mask_operators`]).
const MASK: &str = "__rdb_op_";

/// `||`, masked.
const CONCAT: &str = "__rdb_op_7c7c__";

/// `clause` with each run of `|`, `#` and `&`, and each `~` that negates,
/// outside a quoted run, as a word ([`MASK`]); `None` when it holds none. Not the operators the
/// lexer reads itself (`?|`, `&&`, `#>`, `@>` and the rest of
/// `PG_OPERATORS`).
fn mask_operators(clause: &str) -> Option<String> {
    let bytes = clause.as_bytes();
    let masked = |b: u8| matches!(b, b'|' | b'#' | b'&');
    // A `~` that negates (`~flags`, after an operator, a parenthesis or a
    // keyword), not one that matches (`a ~ 'x'`, after an operand): that
    // one is a regular expression's, lowered before this pass or refused
    // by the lexer as it was.
    let negates = |at: usize| -> bool {
        let before = clause[..at].trim_end();
        match before.as_bytes().last() {
            None => true,
            Some(b')' | b'\'' | b'"' | b'!') => false,
            Some(b) if b.is_ascii_alphanumeric() || *b == b'_' || *b >= 0x80 => {
                let start = before.char_indices().rev().find(|&(_, c)| !(c.is_ascii_alphanumeric() || c == '_')).map_or(0, |(p, c)| p + c.len_utf8());
                ["AND", "OR", "NOT", "THEN", "ELSE", "WHEN", "WHERE", "CASE", "IN", "BETWEEN", "LIKE"]
                    .iter()
                    .any(|w| before[start..].eq_ignore_ascii_case(w))
            }
            Some(_) => true,
        }
    };
    let mut out = String::new();
    let (mut copied, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if let Some(own) = super::predicate_rewrite::PG_OPERATORS.iter().find(|op| clause[i..].starts_with(**op)) {
            i += own.len();
            continue;
        }
        if clause[i..].starts_with("#>") {
            i += 2;
            continue;
        }
        let end = if bytes[i] == b'~' && negates(i) {
            i + 1
        } else if masked(bytes[i]) {
            i + bytes[i..].iter().take_while(|b| masked(**b)).count()
        } else {
            // (A whole character: never into the middle of one.)
            i += clause[i..].chars().next().map_or(1, char::len_utf8);
            continue;
        };
        out.push_str(&clause[copied..i]);
        out.push(' ');
        out.push_str(MASK);
        for b in &bytes[i..end] {
            out.push_str(&format!("{b:02x}"));
        }
        out.push_str("__ ");
        i = end;
        copied = end;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&clause[copied..]);
    Some(out)
}

/// [`mask_operators`]' words written back as the operators they are.
fn unmask_operators(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(MASK) {
        let body = &rest[at + MASK.len()..];
        let hex = body.bytes().take_while(u8::is_ascii_hexdigit).count();
        let operator: Option<String> = (hex > 0 && hex % 2 == 0 && body[hex..].starts_with("__"))
            .then(|| (0..hex).step_by(2).filter_map(|k| u8::from_str_radix(&body[k..k + 2], 16).ok()).map(char::from).collect());
        match operator {
            Some(operator) => {
                out.push_str(&rest[..at]);
                out.push_str(&operator);
                rest = &body[hex + 2..];
            }
            None => {
                out.push_str(&rest[..at + MASK.len()]);
                rest = body;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Calls whose value is their text argument's, under its collation.
const TEXT_CALLS: &[&str] = &[
    "coalesce", "nullif", "trim", "ltrim", "rtrim", "btrim", "upper", "lower", "substring", "substr", "left", "right",
    "replace",
];

/// The collation an operand's text is under, when it is text made from a
/// nondeterministic column: a concatenation at depth 0, or one of
/// [`TEXT_CALLS`], reading such a column (parentheses around it aside).
/// `None` for anything else, a bare column included (it reads its key).
/// `Err` when the columns it reads are of two collations (42P22).
fn text_expression_strength(toks: &[Tok], schema: &dyn Columns) -> Result<Option<Strength>, String> {
    let mut toks = toks;
    while matches!(toks.first(), Some(Tok::LPar)) && closing(toks, 0) == Some(toks.len() - 1) {
        toks = &toks[1..toks.len() - 1];
    }
    let mut depth = 0i32;
    let concatenates = toks.iter().any(|t| {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => depth -= 1,
            _ => {}
        }
        depth == 0 && t.is_keyword(CONCAT)
    });
    let call = matches!(toks, [Tok::Word(name), Tok::LPar, ..] if TEXT_CALLS.iter().any(|c| name.eq_ignore_ascii_case(c)))
        && closing(toks, 1) == Some(toks.len() - 1);
    if !concatenates && !call {
        return Ok(None);
    }
    let mut found: Option<Strength> = None;
    for (i, t) in toks.iter().enumerate() {
        let Tok::Word(w) = t else { continue };
        if matches!(toks.get(i + 1), Some(Tok::LPar)) {
            continue;
        }
        let Some((_, strength)) = schema.ci(w) else { continue };
        if strength.orders_only() {
            continue;
        }
        if found.is_some_and(|f| f != strength) {
            return Err("could not determine which collation to use for string comparison".to_string());
        }
        found = Some(strength);
    }
    Ok(found)
}

/// Each window's `PARTITION BY` and `ORDER BY` (`… OVER (…)` in a select
/// list) over a column with a nondeterministic collation, on its key: a
/// partition key becomes the companion, a sort key the companion and then
/// the column (as `ORDER BY` sorts one, `collation::plan_single`). `None`
/// when nothing changed.
pub(crate) fn rewrite_windows(select_list: &str, columns: &dyn Columns) -> Option<String> {
    let bytes = select_list.as_bytes();
    let mut out = String::with_capacity(select_list.len() + 16);
    let mut changed = false;
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let word_start = i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]);
        if word_start && bytes[i..].len() >= 4 && bytes[i..i + 4].eq_ignore_ascii_case(b"OVER") {
            let mut j = i + 4;
            if j < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[j]) {
                i += 1;
                continue;
            }
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if bytes.get(j) == Some(&b'(') {
                let Some(close) = matching_close(bytes, j) else { return None };
                if let Some(body) = window_body(&select_list[j + 1..close], columns) {
                    out.push_str(&select_list[copied..j + 1]);
                    out.push_str(&body);
                    copied = close;
                    changed = true;
                }
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    if !changed {
        return None;
    }
    out.push_str(&select_list[copied..]);
    Some(out)
}

fn matching_close(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
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

/// One window's body, rewritten; `None` when it reads no such column.
fn window_body(body: &str, columns: &dyn Columns) -> Option<String> {
    let upper = body.to_ascii_uppercase();
    let find = |kw: &str| crate::core::rql::join::find_keyword(&upper, kw, 0);
    let partition = find("PARTITION");
    let order = find("ORDER");
    let frame = ["ROWS", "RANGE", "GROUPS"].iter().filter_map(|k| find(k)).min();
    let mut cuts: Vec<usize> = [partition, order, frame].into_iter().flatten().collect();
    cuts.push(body.len());
    cuts.sort();
    let mut out = String::new();
    let mut changed = false;
    let head = cuts[0];
    out.push_str(&body[..head]);
    for w in cuts.windows(2) {
        let (start, end) = (w[0], w[1]);
        let piece = &body[start..end];
        let is_partition = Some(start) == partition;
        let is_order = Some(start) == order;
        if !(is_partition || is_order) {
            out.push_str(piece);
            continue;
        }
        // `PARTITION BY` / `ORDER BY`, then the list.
        let by = crate::core::rql::join::find_keyword(&piece.to_ascii_uppercase(), "BY", 0).map(|b| b + 2);
        let Some(by) = by else {
            out.push_str(piece);
            continue;
        };
        out.push_str(&piece[..by]);
        out.push(' ');
        let mut items = Vec::new();
        for item in crate::core::sql_scan::split_top_level(&piece[by..], b',') {
            let item = item.trim();
            // The key and what follows it (`DESC NULLS LAST`).
            let key_end = item.find(char::is_whitespace).unwrap_or(item.len());
            let (key, rest) = item.split_at(key_end);
            match columns.ci(key) {
                // A `uuid` in SQL Server's order partitions by its value.
                Some((_, strength)) if strength.orders_only() && is_partition => items.push(item.to_string()),
                Some((companion, _)) => {
                    changed = true;
                    if is_partition {
                        items.push(companion);
                    } else {
                        items.push(format!("{companion}{rest}"));
                        items.push(item.to_string());
                    }
                }
                None => items.push(item.to_string()),
            }
        }
        out.push_str(&items.join(", "));
        out.push(' ');
    }
    changed.then_some(out)
}

/// Where a comparison's column operands come from.
pub(crate) trait Columns {
    /// A column operand as written (`name`, `t.name`): the companion it
    /// reads instead, and the collation; `None` when it is no column with a
    /// nondeterministic one.
    fn ci(&self, raw: &str) -> Option<(String, Strength)>;
}

impl Columns for SmallVec<[SchemaField; 20]> {
    fn ci(&self, raw: &str) -> Option<(String, Strength)> {
        let name = resolve_column(raw, self)?;
        let (companion, strength) = collation::companion(&name, self)?;
        // Keep a qualifier as written: `t.name` reads `t.__ci_name`.
        let qualifier = match raw.rfind('.') {
            Some(at) if !raw.trim_matches('"').eq_ignore_ascii_case(&name) => &raw[..=at],
            _ => "",
        };
        Some((format!("{qualifier}{}", collation::ident(&companion.name)), strength))
    }
}

/// The relations of a join, each by the label its columns are qualified
/// with. A bare column is the one relation's that has it.
pub(crate) struct Relations<'a>(pub Vec<(&'a str, &'a [SchemaField])>);

impl Columns for Relations<'_> {
    fn ci(&self, raw: &str) -> Option<(String, Strength)> {
        let (qualifier, column) = match raw.rsplit_once('.') {
            Some((q, c)) => (Some(q.trim_matches('"')), c.trim_matches('"')),
            None => (None, raw.trim_matches('"')),
        };
        let mut found = self.0.iter().filter(|(label, fields)| {
            qualifier.is_none_or(|q| q.eq_ignore_ascii_case(label))
                && fields.iter().any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
        });
        let (label, fields) = found.next()?;
        if qualifier.is_none() && found.next().is_some() {
            return None;
        }
        let field = fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))?;
        let (companion, strength) = collation::companion(&field.name, fields)?;
        Some((format!("{}.{}", collation::ident(label), collation::ident(&companion.name)), strength))
    }
}

/// A column operand: the companion it reads instead, and the collation.
fn ci_column(toks: &[Tok], schema: &dyn Columns) -> Option<(String, Strength)> {
    let [Tok::Word(raw)] = toks else { return None };
    schema.ci(raw)
}

fn depth_split<'a>(toks: &'a [Tok], at_word: &dyn Fn(&Tok, &[Tok], usize) -> bool) -> Vec<(usize, usize)> {
    // (start, end) of each piece between separators at depth 0.
    let mut pieces = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, t) in toks.iter().enumerate() {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => depth -= 1,
            _ if depth == 0 && at_word(t, toks, i) => {
                pieces.push((start, i));
                start = i + 1;
            }
            _ => {}
        }
    }
    pieces.push((start, toks.len()));
    pieces
}

/// A boolean condition: its `AND`/`OR` terms, each rewritten.
fn condition(toks: &[Tok], schema: &dyn Columns, touched: &Cell<bool>) -> Result<Vec<String>, String> {
    // The `AND` of a `BETWEEN … AND …` is not a connective.
    let mut between_open = vec![false; toks.len()];
    {
        let mut depth = 0i32;
        let mut pending = false;
        for (i, t) in toks.iter().enumerate() {
            match t {
                Tok::LPar => depth += 1,
                Tok::RPar => depth -= 1,
                _ if depth == 0 && t.is_keyword("between") => pending = true,
                _ if depth == 0 && pending && t.is_keyword("and") => {
                    between_open[i] = true;
                    pending = false;
                }
                _ => {}
            }
        }
    }
    let is_connective = |t: &Tok, _: &[Tok], i: usize| (t.is_keyword("and") && !between_open[i]) || t.is_keyword("or");
    let pieces = depth_split(toks, &is_connective);
    let mut out = Vec::new();
    for (k, (start, end)) in pieces.iter().enumerate() {
        if k > 0 {
            out.push(toks[start - 1].render());
        }
        out.extend(term(&toks[*start..*end], schema, touched)?);
    }
    Ok(out)
}

/// One term: `NOT`s, a parenthesised condition, or a comparison.
fn term(toks: &[Tok], schema: &dyn Columns, touched: &Cell<bool>) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    while toks.get(i).is_some_and(|t| t.is_keyword("not")) {
        out.push("NOT".to_string());
        i += 1;
    }
    let rest = &toks[i..];
    if matches!(rest.first(), Some(Tok::LPar)) && closing(rest, 0) == Some(rest.len() - 1) {
        let inner = &rest[1..rest.len() - 1];
        // A group of conditions, or one parenthesised operand compared as a
        // whole (`(a) = 'x'`), which a comparison inside it says.
        out.push("(".to_string());
        out.extend(condition(inner, schema, touched)?);
        out.push(")".to_string());
        return Ok(out);
    }
    out.extend(comparison(rest, schema, touched)?);
    Ok(out)
}

fn closing(toks: &[Tok], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (i, t) in toks.iter().enumerate().skip(open) {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn render(toks: &[Tok]) -> Vec<String> {
    toks.iter().map(Tok::render).collect()
}

/// Whether `toks` read a nondeterministic column anywhere, as a word.
fn reads_ci(toks: &[Tok], schema: &dyn Columns) -> bool {
    toks.iter().enumerate().any(|(i, t)| match t {
        Tok::Word(w) => {
            !matches!(toks.get(i + 1), Some(Tok::LPar))
                && schema.ci(w).is_some()
        }
        _ => false,
    })
}

/// The comparison operator at depth 0: its token span.
fn operator(toks: &[Tok]) -> Option<(usize, usize, String)> {
    let mut depth = 0i32;
    for (i, t) in toks.iter().enumerate() {
        match t {
            Tok::LPar => depth += 1,
            Tok::RPar => depth -= 1,
            _ if depth != 0 => {}
            Tok::Sym(s) if matches!(s.as_str(), "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") => {
                return Some((i, i + 1, s.clone()));
            }
            Tok::Sym(s) if matches!(s.as_str(), "~" | "~*" | "!~" | "!~*") => return Some((i, i + 1, "~".into())),
            Tok::Word(w) => {
                let w = w.to_ascii_lowercase();
                let not = toks.get(i + 1).map(|n| n.render().to_ascii_lowercase());
                match w.as_str() {
                    "like" | "ilike" | "in" | "between" => return Some((i, i + 1, w)),
                    "not" if matches!(not.as_deref(), Some("like" | "ilike" | "in" | "between")) => {
                        return Some((i, i + 2, not.unwrap()));
                    }
                    "similar" => return Some((i, i + 2, "similar".into())),
                    "is" => {
                        // IS [NOT] DISTINCT FROM
                        let mut j = i + 1;
                        if toks.get(j).is_some_and(|t| t.is_keyword("not")) {
                            j += 1;
                        }
                        if toks.get(j).is_some_and(|t| t.is_keyword("distinct"))
                            && toks.get(j + 1).is_some_and(|t| t.is_keyword("from"))
                        {
                            return Some((i, j + 2, "distinct".into()));
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    None
}

/// The key of a value written on the other side of a comparison: a string
/// literal folded in place, `NULL` left alone, anything else through the key
/// function.
fn folded(toks: &[Tok], strength: Strength) -> Vec<String> {
    if let [Tok::Str(s)] = toks
        && s.len() >= 2
        && s.starts_with('\'')
        && s.ends_with('\'')
    {
        let text = s[1..s.len() - 1].replace("''", "'");
        return vec![format!("'{}'", collation::key(&text, strength).replace('\'', "''"))];
    }
    if let [Tok::Word(w)] = toks
        && w.eq_ignore_ascii_case("null")
    {
        return vec!["NULL".to_string()];
    }
    let mut out = vec![format!("{}(", strength.function().expect("nondeterministic"))];
    out.extend(render(toks));
    out.push(")".to_string());
    out
}

/// One comparison, on keys when an operand is a nondeterministic column.
fn comparison(toks: &[Tok], schema: &dyn Columns, touched: &Cell<bool>) -> Result<Vec<String>, String> {
    // A regular expression over such a column, as `lower_regex_operators`
    // spelled it.
    for (i, t) in toks.iter().enumerate() {
        if t.is_keyword("regexp_like")
            && matches!(toks.get(i + 1), Some(Tok::LPar))
            && let Some(close) = closing(toks, i + 1)
            && reads_ci(&toks[i + 2..close], schema)
        {
            return Err(regex_refused());
        }
    }
    let Some((op_start, op_end, op)) = operator(toks) else {
        return Ok(render(toks));
    };
    let (left, right) = (&toks[..op_start], &toks[op_end..]);
    let (l, r) = (ci_column(left, schema), ci_column(right, schema));
    if l.is_none() && r.is_none() {
        if (op == "~" || op == "similar") && (reads_ci(left, schema) || reads_ci(right, schema)) {
            return Err(regex_refused());
        }
        // Text made from such a column is text under the column's
        // collation (`name || '!' = 'BOB!'` and `COALESCE(name, '') LIKE
        // '%OB'` hold for 'Bob', as SQL Server and PostgreSQL both have
        // it): compared on keys, both sides.
        let strength = match text_expression_strength(left, schema)? {
            Some(strength) => Some(strength),
            None => text_expression_strength(right, schema)?,
        };
        if let Some(strength) = strength {
            touched.set(true);
            let mut out = folded(left, strength);
            out.extend(render(&toks[op_start..op_end]));
            out.extend(other_side(right, &op, strength)?);
            return Ok(out);
        }
        return Ok(render(toks));
    }
    // A `uuid` in SQL Server's order changes only what orders it: its
    // equality, IN, DISTINCT FROM stay on the column (and its index).
    let orders_only = l.as_ref().or(r.as_ref()).is_some_and(|(_, s)| s.orders_only());
    if orders_only {
        if !matches!(op.as_str(), "<" | "<=" | ">" | ">=" | "between") {
            return Ok(render(toks));
        }
    } else if op == "~" || op == "similar" {
        return Err(regex_refused());
    }
    touched.set(true);
    let mut out = Vec::new();
    match (l, r) {
        (Some((a, sa)), Some((b, sb))) => {
            if sa != sb {
                return Err("could not determine which collation to use for string comparison".to_string());
            }
            out.push(a);
            out.extend(render(&toks[op_start..op_end]));
            out.push(b);
        }
        (Some((column, strength)), None) => {
            out.push(column);
            out.extend(render(&toks[op_start..op_end]));
            out.extend(other_side(right, &op, strength)?);
        }
        (None, Some((column, strength))) => {
            out.extend(folded(left, strength));
            out.extend(render(&toks[op_start..op_end]));
            out.push(column);
        }
        (None, None) => unreachable!(),
    }
    Ok(out)
}

/// The right side of `column <op> …`, folded: an `IN` list item by item, a
/// `BETWEEN`'s two bounds, anything else whole.
fn other_side(right: &[Tok], op: &str, strength: Strength) -> Result<Vec<String>, String> {
    match op {
        "in" => {
            let close = match right.first() {
                Some(Tok::LPar) => closing(right, 0).ok_or("unbalanced IN list")?,
                _ => return Ok(folded(right, strength)),
            };
            let inner = &right[1..close];
            if inner.first().is_some_and(|t| t.is_keyword("with")) {
                return Err("IN (WITH …) over a column with a case-insensitive collation is not supported yet".to_string());
            }
            // `IN (SELECT [DISTINCT] x FROM …)`: the subquery's value through
            // the key function, compared with the column's key.
            if inner.first().is_some_and(|t| t.is_keyword("select")) {
                let mut start = 1;
                if inner.get(1).is_some_and(|t| t.is_keyword("distinct") || t.is_keyword("all")) {
                    start = 2;
                }
                let from = depth_split(inner, &|t, _, _| t.is_keyword("from"))
                    .first()
                    .map(|&(_, e)| e)
                    .filter(|&e| e < inner.len())
                    .ok_or("IN (SELECT …) over a column with a case-insensitive collation needs a FROM")?;
                let item = &inner[start..from];
                if depth_split(item, &|t, _, _| matches!(t, Tok::Comma)).len() != 1
                    || depth_split(&inner[from..], &|t, _, _| {
                        t.is_keyword("union") || t.is_keyword("intersect") || t.is_keyword("except")
                    })
                    .len()
                        != 1
                {
                    return Err("IN (SELECT …) over a column with a case-insensitive collation takes one plain SELECT".to_string());
                }
                // `x AS a`: the expression alone.
                let item = match item.iter().rposition(|t| t.is_keyword("as")) {
                    Some(at) if at > 0 => &item[..at],
                    _ => item,
                };
                let mut out = vec!["(".to_string()];
                out.extend(render(&inner[..start]));
                out.extend(folded(item, strength));
                out.extend(render(&inner[from..]));
                out.push(")".to_string());
                out.extend(render(&right[close + 1..]));
                return Ok(out);
            }
            let mut out = vec!["(".to_string()];
            for (k, (s, e)) in depth_split(inner, &|t, _, _| matches!(t, Tok::Comma)).into_iter().enumerate() {
                if k > 0 {
                    out.push(",".to_string());
                }
                out.extend(folded(&inner[s..e], strength));
            }
            out.push(")".to_string());
            out.extend(render(&right[close + 1..]));
            Ok(out)
        }
        "between" => {
            // [SYMMETRIC] lo AND hi
            let mut start = 0;
            let mut out = Vec::new();
            if right.first().is_some_and(|t| t.is_keyword("symmetric")) {
                out.push("SYMMETRIC".to_string());
                start = 1;
            }
            let body = &right[start..];
            let pieces = depth_split(body, &|t, _, _| t.is_keyword("and"));
            let [(ls, le), (hs, he)] = pieces.as_slice() else {
                return Err("BETWEEN needs AND".to_string());
            };
            out.extend(folded(&body[*ls..*le], strength));
            out.push("AND".to_string());
            out.extend(folded(&body[*hs..*he], strength));
            Ok(out)
        }
        // `LIKE 'p' ESCAPE '\'`: the pattern folded, the escape kept.
        "like" | "ilike" => {
            let escape = right.iter().position(|t| t.is_keyword("escape"));
            let (pattern, rest) = match escape {
                Some(at) => (&right[..at], &right[at..]),
                None => (right, &right[right.len()..]),
            };
            let mut out = folded(pattern, strength);
            out.extend(render(rest));
            Ok(out)
        }
        _ => Ok(folded(right, strength)),
    }
}

fn regex_refused() -> String {
    "nondeterministic collations are not supported for regular expressions".to_string()
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
            ("code", DbType::STRING),
            ("__ci_name", DbType::STRING),
            ("tag", DbType::STRING),
            ("__cia_tag", DbType::STRING),
            ("g", DbType::UUID),
            ("__gs_g", DbType::STRING),
        ]
        .into_iter()
        .enumerate()
        {
            s.push(SchemaField::new(name.into(), ty, 8, 0, i as u64, false));
        }
        s
    }

    fn r(clause: &str) -> String {
        rewrite(clause, &schema()).unwrap()
    }

    #[test]
    fn comparisons_read_the_key() {
        assert_eq!(r("name = 'Bob'"), "__ci_name = 'bob'");
        assert_eq!(r("'Bob' <> t.name"), "'bob' <> t.__ci_name");
        assert_eq!(r("id = 1 AND name IN ('A', $1)"), "id = 1 AND __ci_name IN ( 'a' , __rdb_ci_key( $1 ) )");
        assert_eq!(r("name NOT LIKE 'B%' OR code = 'X'"), "__ci_name NOT LIKE 'b%' OR code = 'X'");
        assert_eq!(r("name BETWEEN 'A' AND 'M' AND id > 2"), "__ci_name BETWEEN 'a' AND 'm' AND id > 2");
        assert_eq!(r("NOT (name = 'Ann' OR tag = 'Été')"), "NOT ( __ci_name = 'ann' OR __cia_tag = 'ete' )");
        assert_eq!(r("name = upper(code)"), "__ci_name = __rdb_ci_key( upper ( code ) )");
        assert_eq!(r("name IS NOT DISTINCT FROM 'X'"), "__ci_name IS NOT DISTINCT FROM 'x'");
        assert_eq!(r("name IS NULL"), "name IS NULL");
        // Text made from the column is compared under its collation.
        assert_eq!(r("upper(name) = 'BOB'"), "__rdb_ci_key( upper ( name ) ) = 'bob'");
        assert_eq!(r("COALESCE(name, '') NOT LIKE '%OB'"), "__rdb_ci_key( COALESCE ( name , '' ) ) NOT LIKE '%ob'");
        assert_eq!(r("name || '!' = 'BOB!'"), "__rdb_ci_key( name || '!' ) = 'bob!'");
        assert_eq!(r("'x' || code = tag || 'Y'"), "__rdb_ci_ai_key( 'x' || code ) = __rdb_ci_ai_key( tag || 'Y' )");
        // Not text made from it, or not from it: as written.
        assert_eq!(r("length(name) = 3"), "length(name) = 3");
        assert_eq!(r("code || 'x' = 'ax'"), "code || 'x' = 'ax'");
        assert!(rewrite("name || tag = 'x'", &schema()).is_err());
    }

    /// A term this has nothing to say about is kept as it was written, byte
    /// for byte: the lexer drops a cast's parentheses and does not read
    /// `||`, `#`, `&`, `|` or a negating `~`, and its rendering of a term it
    /// only passed through was a different text to the passes after it.
    #[test]
    fn a_term_it_does_not_change_is_kept_as_written() {
        for clause in [
            "(floor(extract(month FROM ((f(d.placed, coalesce(d.__t7_placed, 0)))::timestamp)))::int) # 4 = 0",
            "code = 'a' || 'b' AND (id & 3) = 1",
            "CASE WHEN code IS NOT NULL AND id > 1 THEN ((id # 3) = 0) ELSE CAST(0 AS boolean) END = 1",
            "id BETWEEN 1 AND 5 AND ~id = -2 AND (id | 2) = 3",
        ] {
            assert_eq!(r(clause), clause);
        }
        // Beside a term it does change.
        assert_eq!(
            r("name = 'Bob' AND (floor(x))::int # 4 = 0 AND id BETWEEN 1 AND 2"),
            "__ci_name = 'bob' AND (floor(x))::int # 4 = 0 AND id BETWEEN 1 AND 2"
        );
        assert_eq!(r("name = 'A' || code AND id # 1 = 0"), "__ci_name = __rdb_ci_key( 'A' || code ) AND id # 1 = 0");
        assert_eq!(r("(id # 1) = 0 OR name = 'Bob'"), "( id # 1 ) = 0 OR __ci_name = 'bob'");
    }

    /// A `uuid` in SQL Server's order is keyed only where it is ordered:
    /// its equality stays on the column (and its index).
    #[test]
    fn a_guid_order_keys_only_ordering_comparisons() {
        let k = |t: &str| collation::key(t, Strength::GuidOrder);
        let a = "01000000-0000-0000-0000-000000000000";
        assert_eq!(r(&format!("g = '{a}'")), format!("g = '{a}'"));
        assert_eq!(r(&format!("g IN ('{a}', $1)")), format!("g IN ('{a}', $1)"));
        assert_eq!(r("g <> $1"), "g <> $1");
        assert_eq!(r(&format!("g > '{a}'")), format!("__gs_g > '{}'", k(a)));
        assert_eq!(r("t.g <= $2"), "t.__gs_g <= __rdb_guid_key( $2 )");
        assert_eq!(r(&format!("g BETWEEN '{a}' AND $1")), format!("__gs_g BETWEEN '{}' AND __rdb_guid_key( $1 )", k(a)));
        assert_eq!(r("g IS NULL"), "g IS NULL");
    }

    #[test]
    fn what_cannot_be_carried_over_is_refused() {
        let s = schema();
        // `name ~ 'b'` arrives as the `regexp_like` comparison
        // `check::lower_regex_operators` makes of it.
        assert!(rewrite("regexp_like(name, 'b') = TRUE", &s).unwrap_err().contains("regular expressions"));
        assert!(rewrite("name IN (SELECT a, b FROM t)", &s).unwrap_err().contains("IN (SELECT"));
        assert_eq!(
            rewrite("name NOT IN (SELECT DISTINCT u.n AS x FROM t u WHERE u.k = 1)", &s).unwrap(),
            "__ci_name NOT IN ( SELECT DISTINCT __rdb_ci_key( u.n ) FROM t u WHERE u.k = 1 )"
        );
        assert!(rewrite("name = tag", &s).unwrap_err().contains("could not determine which collation"));
    }
}

