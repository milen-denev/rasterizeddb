//! A statement's known table set, read off its text before it runs
//! (slice B3b, `docs/persistent-indexes.md` §7 Acquisition).
//!
//! A statement takes every table lock it can know of at once, in name order
//! ([`crate::core::database::Database::tables_for`]), before any of its parts
//! reads a schema or a row: then nothing it plans against can change under
//! it, and two statements whose sets overlap cannot deadlock on the order
//! they ask in. What this cannot see (a table named only inside a rewrite,
//! an FK child a cascade reaches, an index's table) is found late by
//! `table_for` at the point of use and waited for with graph edges.
//!
//! # What it reads
//!
//! The relations a statement names, at every depth (CTE bodies, derived
//! tables, subqueries), with the mode each needs:
//! - `FROM` and `JOIN` lists, `DELETE … USING` lists: ACCESS SHARE;
//! - `INSERT INTO t`, `UPDATE t`, `DELETE FROM t` (a data-modifying CTE's
//!   included): ROW EXCLUSIVE; a locking clause (`FOR UPDATE`, `FOR SHARE`,
//!   …) makes every relation the statement reads ROW EXCLUSIVE, as §7's
//!   modes table says;
//! - the target of `ALTER TABLE`, `DROP TABLE` (each name), `CREATE INDEX …
//!   ON t`: ACCESS EXCLUSIVE.
//!
//! # How it scans
//!
//! Tokens, never substrings (memory: scanners must track nesting): string
//! literals (`'…'`, `E'…'`, `$tag$…$tag$`), quoted identifiers and comments
//! are skipped or read whole, so a keyword inside a literal is no keyword.
//! `FROM` inside a function call's parentheses (`EXTRACT(year FROM ts)`,
//! `SUBSTRING(s FROM 2)`, `TRIM(BOTH FROM s)`) and in `IS DISTINCT FROM` is
//! not a relation list, but a paren that opens with `SELECT`, `WITH` or
//! `VALUES` is a subquery whatever word precedes it (`ARRAY(SELECT …)`). A
//! `FROM`/`USING` list is followed per paren depth, so a comma after a
//! subquery, a function, a parenthesised join or a join's `ON` still names
//! the next relation. A name that is not a table (a CTE's, an alias that
//! looks like one) costs nothing: the set is filtered by what is
//! registered. A table it misses is still locked, late.

use crate::core::mvcc::table_locks::TableMode;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok<'a> {
    /// An identifier or keyword, borrowed from the statement; `quoted` for
    /// `"…"`, whose text is what is between the quotes (a doubled quote
    /// still doubled: see [`word_text`]).
    Word { text: &'a str, quoted: bool },
    Punct(char),
    /// A literal or number: something that is neither.
    Other,
}

fn tokens(sql: &str) -> Vec<Tok<'_>> {
    let b = sql.as_bytes();
    let mut out = Vec::with_capacity(sql.len() / 4 + 4);
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 1;
                i += 2;
                while i < b.len() && depth > 0 {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'\'' => {
                i = skip_quoted(b, i, b'\'', false);
                out.push(Tok::Other);
            }
            b'"' => {
                let start = i + 1;
                let mut j = start;
                while j < b.len() {
                    if b[j] == b'"' {
                        if b.get(j + 1) == Some(&b'"') {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                let end = j.min(b.len());
                i = (j + 1).min(b.len());
                out.push(Tok::Word {
                    text: &sql[start..end],
                    quoted: true,
                });
            }
            b'$' => {
                // `$1` is a parameter; `$tag$ … $tag$` a dollar-quoted string.
                let mut j = i + 1;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                if j < b.len() && b[j] == b'$' && !b[i + 1..j].first().is_some_and(|d| d.is_ascii_digit()) {
                    let tag = &sql[i..=j];
                    match sql[j + 1..].find(tag) {
                        Some(at) => i = j + 1 + at + tag.len(),
                        None => i = b.len(),
                    }
                } else {
                    i = j;
                }
                out.push(Tok::Other);
            }
            b'0'..=b'9' => {
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_') {
                    i += 1;
                }
                out.push(Tok::Other);
            }
            c if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80) {
                    i += 1;
                }
                // `E'…'` (and `e'…'`): a string with backslash escapes.
                if i == start + 1 && (c == b'E' || c == b'e') && b.get(i) == Some(&b'\'') {
                    i = skip_quoted(b, i, b'\'', true);
                    out.push(Tok::Other);
                    continue;
                }
                out.push(Tok::Word {
                    text: &sql[start..i],
                    quoted: false,
                });
            }
            _ => {
                out.push(Tok::Punct(c as char));
                i += utf8_len(c);
            }
        }
    }
    out
}

/// A word's name: a quoted identifier with its doubled quotes undone.
fn word_text(text: &str, quoted: bool) -> String {
    if quoted && text.contains("\"\"") {
        text.replace("\"\"", "\"")
    } else {
        text.to_string()
    }
}

fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// The index just past the literal that opens at `i`.
fn skip_quoted(b: &[u8], i: usize, quote: u8, backslash: bool) -> usize {
    let mut j = i + 1;
    while j < b.len() {
        if backslash && b[j] == b'\\' {
            j += 2;
            continue;
        }
        if b[j] == quote {
            if b.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    b.len()
}

fn is_kw(t: &Tok<'_>, kw: &str) -> bool {
    matches!(t, Tok::Word { text, quoted: false } if text.eq_ignore_ascii_case(kw))
}

/// Words after which `(` opens a subquery or a list, not a function call.
const NOT_A_CALL: &[&str] = &[
    "IN", "EXISTS", "ANY", "ALL", "SOME", "AS", "FROM", "JOIN", "LATERAL", "SELECT", "WHERE", "AND", "OR", "NOT",
    "ON", "USING", "SET", "THEN", "ELSE", "WHEN", "RETURNING", "WITH", "UNION", "INTERSECT", "EXCEPT", "HAVING",
    "BY", "VALUES", "INTO", "MATERIALIZED", "CASE", "IS", "LIKE", "ILIKE", "BETWEEN", "OVER", "DISTINCT",
];

/// Words that end a relation list or cannot be a relation's alias.
const RESERVED: &[&str] = &[
    "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "ON", "USING", "JOIN", "INNER", "LEFT",
    "RIGHT", "FULL", "CROSS", "NATURAL", "OUTER", "UNION", "INTERSECT", "EXCEPT", "RETURNING", "SET", "FOR",
    "FETCH", "VALUES", "SELECT", "AS", "LATERAL", "ONLY", "WITH", "AND", "OR", "NOT", "TABLESAMPLE", "DEFAULT",
    "DO", "CONFLICT",
];

fn reserved(t: &Tok<'_>) -> bool {
    RESERVED.iter().any(|kw| is_kw(t, kw))
}

/// A relation name at `toks[i]`, schema qualifier dropped (`public.t` is
/// `t`; the engine has one schema), and the index past it.
fn relation_at(toks: &[Tok<'_>], mut i: usize) -> Option<(String, usize)> {
    while toks.get(i).is_some_and(|t| is_kw(t, "ONLY") || is_kw(t, "LATERAL")) {
        i += 1;
    }
    let Some(Tok::Word { text, quoted }) = toks.get(i) else {
        return None;
    };
    if !quoted && reserved(&toks[i]) {
        return None;
    }
    let (mut name, mut name_quoted) = (*text, *quoted);
    let mut j = i + 1;
    while toks.get(j) == Some(&Tok::Punct('.')) {
        match toks.get(j + 1) {
            Some(Tok::Word { text, quoted }) => {
                (name, name_quoted) = (*text, *quoted);
                j += 2;
            }
            _ => break,
        }
    }
    Some((word_text(name, name_quoted), j))
}

/// Past an optional alias (`AS a`, `a`, `a (c1, c2)`) after a relation.
fn skip_alias(toks: &[Tok<'_>], mut i: usize) -> usize {
    if toks.get(i).is_some_and(|t| is_kw(t, "AS")) {
        i += 1;
    }
    if let Some(t @ Tok::Word { quoted, .. }) = toks.get(i) {
        if *quoted || !reserved(t) {
            i += 1;
            if toks.get(i) == Some(&Tok::Punct('(')) {
                i = past_parens(toks, i);
            }
        }
    }
    i
}

fn past_parens(toks: &[Tok<'_>], open: usize) -> usize {
    let mut depth = 0usize;
    let mut i = open;
    while i < toks.len() {
        match toks[i] {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    toks.len()
}

/// Whether `sql` holds a second `SELECT` (a subquery or derived table), by a
/// cheap word count that may say yes to a literal: the cost of a yes is one
/// early [`statement_lock_set`] scan, never a missed lock.
pub fn names_a_subquery(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut seen = 0;
    let mut i = 0;
    while i + 6 <= b.len() {
        // The first byte rules out nearly every position cheaply.
        if b[i] | 0x20 != b's' {
            i += 1;
            continue;
        }
        if b[i..i + 6].eq_ignore_ascii_case(b"select")
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
            && b.get(i + 6).is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'))
        {
            seen += 1;
            if seen == 2 {
                return true;
            }
            i += 6;
        } else {
            i += 1;
        }
    }
    false
}

/// Whether `sql` is a single-table write whose whole known set is its
/// target: an `INSERT … VALUES`, an `UPDATE t SET … WHERE …` or a `DELETE
/// FROM t WHERE …` naming no other relation (no `SELECT`, `JOIN`, `USING`,
/// `WITH`, and no `FROM` beyond a `DELETE`'s own). Its arm takes the target
/// itself, so the statement-level scan is skipped: the one place a set of
/// one would cost a second pass over the text. Conservative: any of those
/// words anywhere, a literal's included, says no.
pub fn is_single_target_write(sql: &str) -> bool {
    let b = sql.trim_start().as_bytes();
    let starts = |kw: &[u8]| b.len() > kw.len() && b[..kw.len()].eq_ignore_ascii_case(kw);
    let (insert, update, delete) = (starts(b"INSERT"), starts(b"UPDATE"), starts(b"DELETE"));
    if !insert && !update && !delete {
        return false;
    }
    let mut froms = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i] | 0x20;
        let word_start = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if word_start && matches!(c, b's' | b'j' | b'u' | b'w' | b'f') {
            let end = b[i..].iter().position(|x| !(x.is_ascii_alphanumeric() || *x == b'_')).map_or(b.len(), |n| i + n);
            let word = &b[i..end];
            if word.eq_ignore_ascii_case(b"select")
                || word.eq_ignore_ascii_case(b"join")
                || word.eq_ignore_ascii_case(b"using")
                || word.eq_ignore_ascii_case(b"with")
            {
                return false;
            }
            if word.eq_ignore_ascii_case(b"from") {
                froms += 1;
            }
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    froms == usize::from(delete)
}

/// Words that end a `FROM`/`USING` list at its own depth: after one, a comma
/// at that depth is no longer a list separator (`ORDER BY a, b`, `SET a = 1,
/// b = 2`, `RETURNING a, b`).
const ENDS_LIST: &[&str] = &[
    "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "UNION", "INTERSECT", "EXCEPT", "RETURNING",
    "SET", "FOR", "FETCH", "VALUES", "SELECT", "DO",
];

/// Words after which `(` is a subquery's, whatever precedes it:
/// `ARRAY(SELECT …)` and `LIMIT (SELECT …)` read relations as much as
/// `IN (SELECT …)` does.
fn opens_a_query(t: Option<&Tok<'_>>) -> bool {
    t.is_some_and(|t| is_kw(t, "SELECT") || is_kw(t, "WITH") || is_kw(t, "VALUES"))
}

/// One level of parentheses in [`statement_lock_set`]'s scan (the
/// statement's top level is the first).
struct Depth {
    /// A function call's arguments: `FROM` here is `EXTRACT(year FROM ts)`'s.
    call: bool,
    /// The `FROM`/`USING` list open at this depth and its mode: a comma here
    /// names the next relation in it, until a word of [`ENDS_LIST`] or the
    /// depth's `)`. Kept across a parenthesised item, a join and its `ON`,
    /// so `FROM (SELECT …) s, t` and `FROM a JOIN b ON …, c` find `t` and `c`.
    list: Option<TableMode>,
}

/// The relations `sql` names and the mode each needs, as written (a name
/// may appear more than once; `tables_for` merges). See the module comment.
pub fn statement_lock_set(sql: &str) -> Vec<(String, TableMode)> {
    let toks = tokens(sql);
    let mut out: Vec<(String, TableMode)> = Vec::new();
    if let Some(ddl) = ddl_targets(&toks) {
        return ddl;
    }
    // The top level, and a level per open paren above it (allocated only
    // once a statement has one, as most single-table statements do not).
    let mut top = Depth { call: false, list: None };
    let mut depths: Vec<Depth> = Vec::new();
    macro_rules! depth {
        () => {
            depths.last_mut().unwrap_or(&mut top)
        };
    }
    // The `(` a relation list stopped at: an item of that list (a subquery,
    // a function, a parenthesised join), not an expression's paren.
    let mut item_paren: Option<usize> = None;
    let mut locking = false;
    let mut i = 0;
    while i < toks.len() {
        let t = &toks[i];
        let prev = i.checked_sub(1).map(|p| &toks[p]);
        match t {
            Tok::Punct('(') => {
                let call = !opens_a_query(toks.get(i + 1))
                    && matches!(
                        prev,
                        Some(prev @ Tok::Word { quoted: false, .. }) if !NOT_A_CALL.iter().any(|kw| is_kw(prev, kw))
                    );
                let outer = depth!().list;
                depths.push(Depth { call, list: None });
                i += 1;
                if item_paren.take() == Some(i - 1) && !call {
                    if let Some(mode) = outer {
                        // `FROM (a JOIN b ON …)`: a parenthesised join is a
                        // relation list of its own; `((…))` nests one.
                        if toks.get(i) == Some(&Tok::Punct('(')) {
                            depth!().list = Some(mode);
                            item_paren = Some(i);
                        } else if !opens_a_query(toks.get(i)) {
                            depth!().list = Some(mode);
                            i = list_from(&toks, i, mode, &mut out, &mut item_paren);
                        }
                    }
                }
                continue;
            }
            Tok::Punct(')') => {
                depths.pop();
                i += 1;
                continue;
            }
            Tok::Punct(',') => {
                let depth = depth!();
                if let (false, Some(mode)) = (depth.call, depth.list) {
                    i = list_from(&toks, i + 1, mode, &mut out, &mut item_paren);
                    continue;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        let in_call = depth!().call;
        if depth!().list.is_some() && ENDS_LIST.iter().any(|kw| is_kw(t, kw)) {
            depth!().list = None;
        }
        if is_kw(t, "FROM") && !in_call && !prev.is_some_and(|p| is_kw(p, "DISTINCT")) {
            let mode = if prev.is_some_and(|p| is_kw(p, "DELETE")) {
                TableMode::RowExclusive
            } else {
                TableMode::AccessShare
            };
            depth!().list = Some(mode);
            i = list_from(&toks, i + 1, mode, &mut out, &mut item_paren);
            continue;
        }
        // `DELETE FROM t USING …`: the one USING that follows the write
        // target's own list (still open, ROW EXCLUSIVE), so its first item
        // may be a subquery; a join's `USING (id)` names columns.
        let deletes_using = depth!().list == Some(TableMode::RowExclusive);
        if is_kw(t, "USING") && !in_call && (deletes_using || toks.get(i + 1) != Some(&Tok::Punct('('))) {
            depth!().list = Some(TableMode::AccessShare);
            i = list_from(&toks, i + 1, TableMode::AccessShare, &mut out, &mut item_paren);
            continue;
        }
        if is_kw(t, "JOIN") && !in_call {
            if let Some((name, next)) = relation_at(&toks, i + 1) {
                if toks.get(next) == Some(&Tok::Punct('(')) {
                    // `JOIN f(x)`: a function, not a relation.
                    item_paren = Some(next);
                    i = next;
                    continue;
                }
                out.push((name, TableMode::AccessShare));
                i = next;
                continue;
            }
            // `JOIN (…)`, `JOIN LATERAL (…)`: an item, as in a list.
            let mut at = i + 1;
            while toks.get(at).is_some_and(|t| is_kw(t, "LATERAL")) {
                at += 1;
            }
            if toks.get(at) == Some(&Tok::Punct('(')) {
                item_paren = Some(at);
                i = at;
                continue;
            }
        }
        if is_kw(t, "INTO") && prev.is_some_and(|p| is_kw(p, "INSERT")) {
            if let Some((name, next)) = relation_at(&toks, i + 1) {
                out.push((name, TableMode::RowExclusive));
                i = next;
                continue;
            }
        }
        if is_kw(t, "UPDATE")
            && !prev.is_some_and(|p| is_kw(p, "FOR") || is_kw(p, "KEY") || is_kw(p, "DO") || is_kw(p, "ON"))
        {
            if let Some((name, next)) = relation_at(&toks, i + 1) {
                out.push((name, TableMode::RowExclusive));
                i = next;
                continue;
            }
        }
        if is_kw(t, "FOR")
            && toks.get(i + 1).is_some_and(|n| {
                is_kw(n, "UPDATE") || is_kw(n, "SHARE") || is_kw(n, "NO") || is_kw(n, "KEY")
            })
        {
            locking = true;
        }
        i += 1;
    }
    if locking {
        for (_, mode) in out.iter_mut() {
            if *mode == TableMode::AccessShare {
                *mode = TableMode::RowExclusive;
            }
        }
    }
    out
}

/// [`relation_list`] from `i`, noting the `(` it stopped at as an item of
/// the list (see `item_paren` in [`statement_lock_set`]).
fn list_from(
    toks: &[Tok<'_>],
    i: usize,
    mode: TableMode,
    out: &mut Vec<(String, TableMode)>,
    item_paren: &mut Option<usize>,
) -> usize {
    let end = relation_list(toks, i, mode, out);
    if toks.get(end) == Some(&Tok::Punct('(')) {
        *item_paren = Some(end);
    }
    end
}

/// A `FROM`/`USING` list starting at `i`: relations separated by commas,
/// each with an optional alias. A parenthesised item (a subquery, a
/// function's arguments, a parenthesised join) is left for the main scan,
/// which sees the paren next and resumes the list at the comma after it.
/// Returns where the list stops.
fn relation_list(toks: &[Tok<'_>], mut i: usize, mode: TableMode, out: &mut Vec<(String, TableMode)>) -> usize {
    loop {
        while toks.get(i).is_some_and(|t| is_kw(t, "LATERAL")) {
            i += 1;
        }
        if toks.get(i) == Some(&Tok::Punct('(')) {
            return i;
        }
        let Some((name, next)) = relation_at(toks, i) else {
            return i;
        };
        // `FROM f(x)`: a set-returning function, not a relation.
        if toks.get(next) == Some(&Tok::Punct('(')) {
            return next;
        }
        out.push((name, mode));
        i = skip_alias(toks, next);
        if toks.get(i) == Some(&Tok::Punct(',')) {
            i += 1;
            continue;
        }
        return i;
    }
}

/// The ACCESS EXCLUSIVE targets of a DDL statement, or `None` if `toks` is
/// not one of the DDL forms this reads.
fn ddl_targets(toks: &[Tok<'_>]) -> Option<Vec<(String, TableMode)>> {
    let ae = TableMode::AccessExclusive;
    let first = toks.first()?;
    let skip_if_exists = |mut i: usize| {
        if toks.get(i).is_some_and(|t| is_kw(t, "IF")) && toks.get(i + 1).is_some_and(|t| is_kw(t, "EXISTS")) {
            i += 2;
        }
        i
    };
    if is_kw(first, "ALTER") && toks.get(1).is_some_and(|t| is_kw(t, "TABLE")) {
        let i = skip_if_exists(2);
        return Some(relation_at(toks, i).map(|(n, _)| vec![(n, ae)]).unwrap_or_default());
    }
    if is_kw(first, "DROP") && toks.get(1).is_some_and(|t| is_kw(t, "TABLE")) {
        let mut i = skip_if_exists(2);
        let mut names = Vec::new();
        while let Some((name, next)) = relation_at(toks, i) {
            names.push((name, ae));
            if toks.get(next) == Some(&Tok::Punct(',')) {
                i = next + 1;
            } else {
                break;
            }
        }
        return Some(names);
    }
    if is_kw(first, "CREATE") {
        let mut i = 1;
        if toks.get(i).is_some_and(|t| is_kw(t, "UNIQUE")) {
            i += 1;
        }
        if toks.get(i).is_some_and(|t| is_kw(t, "INDEX")) {
            // `… ON [ONLY] t`: the first top-level ON.
            let on = toks.iter().position(|t| is_kw(t, "ON"))?;
            return Some(relation_at(toks, on + 1).map(|(n, _)| vec![(n, ae)]).unwrap_or_default());
        }
        // CREATE TABLE and the rest name no existing table to lock here
        // (B4 adds ADD/REFERENCES parents, finding L11).
        return Some(Vec::new());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use TableMode::{AccessExclusive as AE, AccessShare as AS, RowExclusive as RX};

    fn set(sql: &str) -> Vec<(String, TableMode)> {
        let mut v = statement_lock_set(sql);
        v.sort_by(|a, b| a.0.cmp(&b.0).then((a.1 as u8).cmp(&(b.1 as u8))));
        v.dedup();
        v
    }

    fn s(entries: &[(&str, TableMode)]) -> Vec<(String, TableMode)> {
        let mut v: Vec<(String, TableMode)> = entries.iter().map(|(n, m)| (n.to_string(), *m)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0).then((a.1 as u8).cmp(&(b.1 as u8))));
        v
    }

    #[test]
    fn reads_and_writes_get_their_modes() {
        assert_eq!(set("SELECT a FROM t WHERE id = 1"), s(&[("t", AS)]));
        assert_eq!(set("select * from t1, t2 x, public.t3 AS y where 1=1"), s(&[("t1", AS), ("t2", AS), ("t3", AS)]));
        assert_eq!(
            set("SELECT * FROM a JOIN b ON a.id = b.id LEFT OUTER JOIN c USING (id)"),
            s(&[("a", AS), ("b", AS), ("c", AS)])
        );
        assert_eq!(set("INSERT INTO t (a, b) VALUES (1, 2)"), s(&[("t", RX)]));
        assert_eq!(set("INSERT INTO t SELECT * FROM u"), s(&[("t", RX), ("u", AS)]));
        assert_eq!(set("UPDATE t SET a = 1 WHERE id = 2"), s(&[("t", RX)]));
        assert_eq!(set("UPDATE t AS j SET a = s.b FROM s WHERE j.id = s.id"), s(&[("s", AS), ("t", RX)]));
        assert_eq!(set("DELETE FROM t WHERE id = 1"), s(&[("t", RX)]));
        assert_eq!(set("DELETE FROM t USING u, v WHERE t.id = u.id"), s(&[("t", RX), ("u", AS), ("v", AS)]));
        assert_eq!(set("SELECT id FROM t WHERE id = 1 FOR UPDATE"), s(&[("t", RX)]));
        assert_eq!(set("SELECT id FROM t FOR NO KEY UPDATE SKIP LOCKED"), s(&[("t", RX)]));
        assert_eq!(
            set("INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET v = 2"),
            s(&[("t", RX)]),
            "DO UPDATE is not a second target"
        );
    }

    #[test]
    fn nested_relations_are_found_at_every_depth() {
        assert_eq!(
            set("WITH x AS (SELECT * FROM a), y AS (INSERT INTO b VALUES (1) RETURNING *) SELECT * FROM x JOIN c ON true"),
            s(&[("a", AS), ("b", RX), ("c", AS), ("x", AS)])
        );
        assert_eq!(
            set("SELECT * FROM (SELECT id FROM inner_t) d WHERE id IN (SELECT id FROM other WHERE EXISTS (SELECT 1 FROM third))"),
            s(&[("inner_t", AS), ("other", AS), ("third", AS)])
        );
        assert_eq!(
            set("UPDATE t SET v = (SELECT max(v) FROM u) WHERE id = 1"),
            s(&[("t", RX), ("u", AS)])
        );
    }

    #[test]
    fn literals_comments_and_calls_name_no_relation() {
        assert_eq!(set("SELECT 'FROM x' FROM t -- FROM y\n /* FROM z */"), s(&[("t", AS)]));
        assert_eq!(set("SELECT E'it\\'s FROM q' FROM t"), s(&[("t", AS)]));
        assert_eq!(set("SELECT $$ FROM q $$, $1 FROM t"), s(&[("t", AS)]));
        assert_eq!(set("SELECT EXTRACT(year FROM ts), SUBSTRING(s FROM 2) FROM t"), s(&[("t", AS)]));
        assert_eq!(set("SELECT trim(BOTH FROM s) FROM t WHERE a IS DISTINCT FROM b"), s(&[("t", AS)]));
        assert_eq!(set("SELECT * FROM \"Mixed Case\" m"), s(&[("Mixed Case", AS)]));
        assert_eq!(set("SELECT * FROM generate_series(1, 3) g"), s(&[]));
        assert_eq!(set("SELECT 1"), s(&[]));
    }

    #[test]
    fn a_single_target_write_is_recognised_conservatively() {
        for yes in [
            "INSERT INTO t (a, b) VALUES (1, 2)",
            "insert into t values (1, 'x') returning *",
            "UPDATE t SET a = 1 WHERE id = 2",
            "DELETE FROM t WHERE id = 1",
        ] {
            assert!(is_single_target_write(yes), "{yes}");
        }
        for no in [
            "INSERT INTO t SELECT * FROM u",
            "UPDATE t SET a = s.b FROM s WHERE t.id = s.id",
            "UPDATE t SET a = (SELECT max(a) FROM u)",
            "DELETE FROM t USING u WHERE t.id = u.id",
            "DELETE FROM t WHERE id IN (SELECT id FROM u)",
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
            "SELECT * FROM t",
            "INSERT INTO t (a) VALUES ('select')",
        ] {
            assert!(!is_single_target_write(no), "{no}");
        }
    }

    /// Each statement's whole set, as a table: the shapes the first scanner
    /// lost a relation in (a list resumed after a parenthesised item or a
    /// join, a subquery in any call's parens, a parenthesised join), the
    /// shapes it already read, and commas that are not a list's.
    #[test]
    fn every_relation_is_found_past_parens_joins_and_calls() {
        let cases: &[(&str, &[(&str, TableMode)])] = &[
            // A list resumed after a parenthesised item.
            ("SELECT * FROM (SELECT 1) a, t3", &[("t3", AS)]),
            ("SELECT * FROM generate_series(1,3) g, t3", &[("t3", AS)]),
            ("SELECT * FROM t1, LATERAL (SELECT 1) x, t3", &[("t1", AS), ("t3", AS)]),
            ("SELECT jsonb_agg(x) FROM (VALUES (1)) v(x), t", &[("t", AS)]),
            ("SELECT * FROM t, unnest(t.arr) AS x(v), u", &[("t", AS), ("u", AS)]),
            ("SELECT row_to_json(r) FROM (SELECT * FROM u) r", &[("u", AS)]),
            ("UPDATE t SET v = 1 FROM (SELECT 1) s, u", &[("t", RX), ("u", AS)]),
            ("DELETE FROM t USING (SELECT 1) s, u WHERE true", &[("t", RX), ("u", AS)]),
            // A list resumed after a join.
            ("SELECT * FROM t1 x JOIN t2 y USING (id), t3", &[("t1", AS), ("t2", AS), ("t3", AS)]),
            ("SELECT * FROM t1 CROSS JOIN t2, t3", &[("t1", AS), ("t2", AS), ("t3", AS)]),
            ("SELECT * FROM t1 JOIN t2 ON (t1.a = t2.a), t3", &[("t1", AS), ("t2", AS), ("t3", AS)]),
            ("SELECT * FROM t1 LEFT JOIN LATERAL f(t1.x) g ON true, t3", &[("t1", AS), ("t3", AS)]),
            ("SELECT * FROM t1 JOIN (SELECT * FROM u) s ON true, t3", &[("t1", AS), ("t3", AS), ("u", AS)]),
            ("DELETE FROM t USING u JOIN w USING (id), v", &[("t", RX), ("u", AS), ("v", AS), ("w", AS)]),
            // A subquery in a call's parens, whatever the call.
            ("SELECT ARRAY(SELECT x FROM u) FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t WHERE v = ANY(ARRAY(SELECT v FROM u))", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t LIMIT (SELECT n FROM u)", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t OFFSET (SELECT 1 FROM u)", &[("t", AS), ("u", AS)]),
            // A parenthesised join.
            ("SELECT * FROM (a JOIN b ON a.id = b.id)", &[("a", AS), ("b", AS)]),
            (
                "SELECT * FROM ((a JOIN b ON true) JOIN c ON true), d",
                &[("a", AS), ("b", AS), ("c", AS), ("d", AS)],
            ),
            // Read right before, and still.
            ("SELECT * FROM t WHERE x = 1 UNION ALL SELECT * FROM u", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM u)", &[("t", AS), ("u", AS)]),
            ("SELECT coalesce((SELECT max(v) FROM u), 0) FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT GREATEST((SELECT 1 FROM u), 2) FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t WHERE x > ALL (SELECT v FROM u)", &[("t", AS), ("u", AS)]),
            ("SELECT (SELECT v FROM u WHERE u.id = t.id) AS s FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t WHERE v IN (1, 2) AND NOT EXISTS(SELECT 1 FROM u)", &[("t", AS), ("u", AS)]),
            ("SELECT count(*) FILTER (WHERE v IN (SELECT v FROM u)) FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT CAST((SELECT v FROM u) AS int) FROM t", &[("t", AS), ("u", AS)]),
            ("SELECT * FROM t ORDER BY (SELECT 1 FROM u)", &[("t", AS), ("u", AS)]),
            (
                "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = SOME (SELECT y FROM w))",
                &[("t", AS), ("u", AS), ("w", AS)],
            ),
            ("INSERT INTO t SELECT * FROM u ON CONFLICT DO NOTHING", &[("t", RX), ("u", AS)]),
            ("SELECT * FROM t FOR UPDATE OF t", &[("t", RX)]),
            ("SELECT * FROM t1 AS x(a,b), t2", &[("t1", AS), ("t2", AS)]),
            ("SELECT * FROM t WHERE s = 'it''s FROM q'", &[("t", AS)]),
            ("SELECT * FROM \"a\"\"b\"", &[("a\"b", AS)]),
            ("SELECT * FROM t WINDOW w AS (PARTITION BY a)", &[("t", AS)]),
            ("SELECT EXTRACT(year FROM ts), SUBSTRING(s FROM 2 FOR 3) FROM t", &[("t", AS)]),
            // Commas that are not a list's: no entry for what follows them.
            ("SELECT * FROM t1 JOIN t2 USING (id, t3)", &[("t1", AS), ("t2", AS)]),
            ("SELECT a, b FROM t ORDER BY a, b", &[("t", AS)]),
            ("SELECT a FROM t GROUP BY a, b HAVING count(*) > 1", &[("t", AS)]),
            (
                "INSERT INTO t (a, b) SELECT x, y FROM u ON CONFLICT (a) DO UPDATE SET b = 1, c = 2",
                &[("t", RX), ("u", AS)],
            ),
            ("UPDATE t SET a = 1, b = 2 FROM s, u WHERE t.id = s.id", &[("s", AS), ("t", RX), ("u", AS)]),
            ("DELETE FROM t WHERE id = 1 RETURNING a, b", &[("t", RX)]),
            ("SELECT * FROM t1, t2 x WHERE a IN (1, 2) UNION SELECT a, b FROM u", &[("t1", AS), ("t2", AS), ("u", AS)]),
        ];
        let mut wrong = Vec::new();
        for (sql, want) in cases {
            let (got, want) = (set(sql), s(want));
            if got != want {
                wrong.push(format!("{sql}\n    got  {got:?}\n    want {want:?}"));
            }
        }
        assert!(wrong.is_empty(), "{} of {} sets wrong:\n{}", wrong.len(), cases.len(), wrong.join("\n"));
    }

    #[test]
    fn ddl_targets_take_access_exclusive() {
        assert_eq!(set("ALTER TABLE t ADD COLUMN c INT"), s(&[("t", AE)]));
        assert_eq!(set("ALTER TABLE IF EXISTS t DROP COLUMN c"), s(&[("t", AE)]));
        assert_eq!(set("DROP TABLE a, b CASCADE"), s(&[("a", AE), ("b", AE)]));
        assert_eq!(set("DROP TABLE IF EXISTS a"), s(&[("a", AE)]));
        assert_eq!(set("CREATE INDEX ix ON t (a)"), s(&[("t", AE)]));
        assert_eq!(set("CREATE UNIQUE INDEX IF NOT EXISTS ix ON ONLY t ((doc->>'k'))"), s(&[("t", AE)]));
        assert_eq!(set("CREATE TABLE n (id INT REFERENCES p (id))"), s(&[]));
    }
}

#[cfg(test)]
mod cost {
    /// What one scan costs, for B3b's perf gate: run with `--release
    /// --ignored lock_set_cost --nocapture`.
    #[test]
    #[ignore]
    fn lock_set_cost() {
        for sql in [
            "INSERT INTO ins (id, v) VALUES (239, 8647)",
            "UPDATE acc SET balance = 70 WHERE id = 10",
            "SELECT d.balance FROM (SELECT balance FROM acc WHERE id = 5) d",
        ] {
            let n = 1_000_000;
            let started = std::time::Instant::now();
            let mut total = 0;
            for _ in 0..n {
                total += std::hint::black_box(super::statement_lock_set(std::hint::black_box(sql))).len();
            }
            let per = started.elapsed().as_nanos() as f64 / n as f64;
            let started = std::time::Instant::now();
            let mut hits = 0;
            for _ in 0..n {
                hits += std::hint::black_box(super::names_a_subquery(std::hint::black_box(sql))) as usize;
            }
            let per_probe = started.elapsed().as_nanos() as f64 / n as f64;
            println!("{per:7.1} ns scan, {per_probe:6.1} ns subquery probe  {sql}  ({total} {hits})");
        }
    }
}
