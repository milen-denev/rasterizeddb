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
//! An expression over such a column (`upper(name) = 'BOB'`) is compared by
//! its bytes: PostgreSQL would carry the column's collation through it.

use smallvec::SmallVec;

use super::predicate_rewrite::{Tok, lex, resolve_column};
use crate::core::rql::collation::{self, Strength};
use crate::core::row::schema::SchemaField;

/// The clause with each comparison over a nondeterministic column on its
/// key; unchanged when the table has none.
pub(crate) fn rewrite(clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if !collation::any(schema) {
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
    let toks = lex(clause)?;
    let out = condition(&toks, columns)?;
    Ok(out.join(" "))
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
fn condition(toks: &[Tok], schema: &dyn Columns) -> Result<Vec<String>, String> {
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
        out.extend(term(&toks[*start..*end], schema)?);
    }
    Ok(out)
}

/// One term: `NOT`s, a parenthesised condition, or a comparison.
fn term(toks: &[Tok], schema: &dyn Columns) -> Result<Vec<String>, String> {
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
        out.extend(condition(inner, schema)?);
        out.push(")".to_string());
        return Ok(out);
    }
    out.extend(comparison(rest, schema)?);
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
fn comparison(toks: &[Tok], schema: &dyn Columns) -> Result<Vec<String>, String> {
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
        return Ok(render(toks));
    }
    if op == "~" || op == "similar" {
        return Err(regex_refused());
    }
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
            if inner.first().is_some_and(|t| t.is_keyword("select") || t.is_keyword("with")) {
                return Err("IN (SELECT …) over a column with a case-insensitive collation is not supported yet".to_string());
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
        assert_eq!(r("upper(name) = 'BOB'"), "upper ( name ) = 'BOB'");
    }

    #[test]
    fn what_cannot_be_carried_over_is_refused() {
        let s = schema();
        // `name ~ 'b'` arrives as the `regexp_like` comparison
        // `check::lower_regex_operators` makes of it.
        assert!(rewrite("regexp_like(name, 'b') = TRUE", &s).unwrap_err().contains("regular expressions"));
        assert!(rewrite("name IN (SELECT n FROM t)", &s).unwrap_err().contains("IN (SELECT"));
        assert!(rewrite("name = tag", &s).unwrap_err().contains("could not determine which collation"));
    }
}

