//! `PREPARE name [(types)] AS statement`, `EXECUTE name [(args)]` and
//! `DEALLOCATE [PREPARE] {name | ALL}`: the SQL-level prepared statements,
//! kept per session. Each was a syntax error.
//!
//! `EXECUTE` puts each argument in for its `$n` and runs the statement as if
//! it had been sent (`execute_single_statement`), so it gets every rewrite
//! and check a statement does. A parameter's declared type is the cast its
//! argument is read through, as PostgreSQL coerces it.

use crate::core::sql_scan::{is_identifier_byte, matching_close, skip_comment, skip_quoted, split_top_level};

/// One `PREPARE`d statement.
#[derive(Debug, Clone)]
pub(crate) struct Prepared {
    pub types: Vec<String>,
    pub body: String,
}

/// What a statement of this family asks for.
#[derive(Debug, PartialEq)]
pub(crate) enum SqlPrepare {
    Prepare { name: String, types: Vec<String>, body: String },
    Execute { name: String, args: Vec<String> },
    Deallocate { name: Option<String> },
}

fn word_at(text: &str, word: &str) -> Option<usize> {
    let n = word.len();
    (text.len() >= n
        && text.as_bytes()[..n].eq_ignore_ascii_case(word.as_bytes())
        && text.as_bytes().get(n).is_none_or(|b| !is_identifier_byte(*b)))
    .then_some(n)
}

/// A name as PostgreSQL reads it: a quoted one as written, a bare one
/// lowercased; with the rest of the text after it.
fn name(text: &str) -> Option<(String, &str)> {
    let text = text.trim_start();
    if text.starts_with('"') {
        let end = text[1..].find('"')? + 1;
        return Some((text[1..end].to_string(), &text[end + 1..]));
    }
    let len = text.bytes().take_while(|b| is_identifier_byte(*b)).count();
    (len > 0).then(|| (text[..len].to_ascii_lowercase(), &text[len..]))
}

/// The statement as one of the three, `None` for any other; a malformed one
/// is 42601's message.
pub(crate) fn parse(stmt: &str) -> Option<Result<SqlPrepare, String>> {
    let s = stmt.trim().trim_end_matches(';').trim_end();
    if let Some(n) = word_at(s, "PREPARE") {
        let rest = &s[n..];
        // `PREPARE TRANSACTION` is two-phase commit, not this.
        if word_at(rest.trim_start(), "TRANSACTION").is_some() {
            return None;
        }
        let Some((name, rest)) = name(rest) else { return Some(Err("PREPARE needs a name".to_string())) };
        let mut rest = rest.trim_start();
        let mut types = Vec::new();
        if rest.starts_with('(') {
            let Some(close) = matching_close(rest, 0) else { return Some(Err("PREPARE: unclosed parameter list".to_string())) };
            types = split_top_level(&rest[1..close], b',').into_iter().map(str::to_string).collect();
            rest = rest[close + 1..].trim_start();
        }
        let Some(n) = word_at(rest, "AS") else { return Some(Err("PREPARE needs AS and a statement".to_string())) };
        let body = rest[n..].trim().to_string();
        if body.is_empty() {
            return Some(Err("PREPARE needs a statement after AS".to_string()));
        }
        return Some(Ok(SqlPrepare::Prepare { name, types, body }));
    }
    if let Some(n) = word_at(s, "EXECUTE") {
        let Some((name, rest)) = name(&s[n..]) else { return Some(Err("EXECUTE needs a name".to_string())) };
        let rest = rest.trim();
        let args = if rest.is_empty() {
            Vec::new()
        } else if rest.starts_with('(') && matching_close(rest, 0) == Some(rest.len() - 1) {
            split_top_level(&rest[1..rest.len() - 1], b',').into_iter().map(str::to_string).collect()
        } else {
            return Some(Err(format!("syntax error at or near \"{}\"", rest.split_whitespace().next().unwrap_or(rest))));
        };
        return Some(Ok(SqlPrepare::Execute { name, args }));
    }
    if let Some(n) = word_at(s, "DEALLOCATE") {
        let mut rest = s[n..].trim_start();
        if let Some(p) = word_at(rest, "PREPARE") {
            rest = rest[p..].trim_start();
        }
        if word_at(rest, "ALL").is_some() && rest.len() == 3 {
            return Some(Ok(SqlPrepare::Deallocate { name: None }));
        }
        let Some((name, after)) = name(rest) else { return Some(Err("DEALLOCATE needs a name or ALL".to_string())) };
        if !after.trim().is_empty() {
            return Some(Err("DEALLOCATE takes one name".to_string()));
        }
        return Some(Ok(SqlPrepare::Deallocate { name: Some(name) }));
    }
    None
}

/// `prepared`'s statement with `args` in for its parameters: 42P02 for a
/// `$n` past the arguments, 42601 for a count that differs from what it
/// declared.
pub(crate) fn bind(name: &str, prepared: &Prepared, args: &[String]) -> Result<String, (&'static str, String)> {
    if !prepared.types.is_empty() && args.len() != prepared.types.len() {
        return Err((
            "42601",
            format!("wrong number of parameters for prepared statement \"{name}\" (expected {}, got {})", prepared.types.len(), args.len()),
        ));
    }
    let body = &prepared.body;
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len() + 32);
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'$' && (i == 0 || !is_identifier_byte(bytes[i - 1])) {
            let digits = bytes[i + 1..].iter().take_while(|b| b.is_ascii_digit()).count();
            if digits > 0 && bytes.get(i + 1 + digits).is_none_or(|b| !is_identifier_byte(*b)) {
                let n: usize = body[i + 1..i + 1 + digits].parse().unwrap_or(0);
                let Some(arg) = n.checked_sub(1).and_then(|k| args.get(k)) else {
                    return Err(("42P02", format!("there is no parameter ${n}")));
                };
                out.push_str(&body[copied..i]);
                match prepared.types.get(n - 1) {
                    // A row count reads a bare number: `LIMIT CAST(8 AS
                    // bigint)` was cut at its first space and refused.
                    Some(_) if after_row_count_keyword(&out) && is_integer_literal(arg) => out.push_str(arg.trim()),
                    // A string for a text parameter is that text as written:
                    // an untyped string resolves to text anyway, and an
                    // operand such as a JSON key (`doc -> $1 ->> 'k'`) reads
                    // a literal, not `CAST('0' AS text)`, which it refused as
                    // "neither a column nor a value".
                    Some(ty) if is_text_type(ty) && is_string_literal(arg) => out.push_str(arg.trim()),
                    Some(ty) => out.push_str(&format!("CAST({arg} AS {ty})")),
                    None => out.push_str(&format!("({arg})")),
                }
                i += 1 + digits;
                copied = i;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&body[copied..]);
    Ok(out)
}

/// Whether `text` ends with `LIMIT` or `OFFSET` (then whitespace), the
/// keywords whose value the planner reads as a bare row count.
fn after_row_count_keyword(text: &str) -> bool {
    let trimmed = text.trim_end();
    if trimmed.len() == text.len() {
        return false;
    }
    ["LIMIT", "OFFSET"].iter().any(|kw| {
        trimmed.len() >= kw.len()
            && trimmed.as_bytes()[trimmed.len() - kw.len()..].eq_ignore_ascii_case(kw.as_bytes())
            && trimmed[..trimmed.len() - kw.len()]
                .bytes()
                .last()
                .is_none_or(|b| !is_identifier_byte(b))
    })
}

/// A plain non-negative integer, as `EXECUTE p (8)` passes one.
/// `text`, `varchar`/`character varying` without a length (a length would
/// cut the value): the types an untyped string becomes as it is.
fn is_text_type(ty: &str) -> bool {
    let ty = ty.trim().to_ascii_lowercase();
    matches!(ty.as_str(), "text" | "varchar" | "character varying" | "pg_catalog.text")
}

/// One complete `'…'` string, doubled quotes inside.
fn is_string_literal(arg: &str) -> bool {
    let arg = arg.trim();
    arg.len() >= 2
        && arg.starts_with('\'')
        && crate::core::sql_scan::quoted_end(arg.as_bytes(), 0, b'\'') == arg.len()
}

fn is_integer_literal(arg: &str) -> bool {
    let arg = arg.trim();
    !arg.is_empty() && arg.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_count_parameter_goes_in_bare() {
        let p = Prepared {
            types: vec!["bigint".into(), "bigint".into(), "bigint".into()],
            body: "SELECT id FROM t WHERE n < $1 ORDER BY id LIMIT $2 OFFSET $3".into(),
        };
        assert_eq!(
            bind("p", &p, &["5".into(), "8".into(), "2".into()]).unwrap(),
            "SELECT id FROM t WHERE n < CAST(5 AS bigint) ORDER BY id LIMIT 8 OFFSET 2"
        );
        // Not a bare integer: the cast stays, and the planner says why.
        assert_eq!(
            bind("p", &p, &["5".into(), "'8'".into(), "2".into()]).unwrap(),
            "SELECT id FROM t WHERE n < CAST(5 AS bigint) ORDER BY id LIMIT CAST('8' AS bigint) OFFSET 2"
        );
        // A column whose name ends in "limit" is not the keyword.
        let q = Prepared { types: vec!["int".into()], body: "SELECT * FROM t WHERE ratelimit = $1".into() };
        assert_eq!(after_row_count_keyword("WHERE ratelimit "), false);
        assert_eq!(bind("q", &q, &["3".into()]).unwrap(), "SELECT * FROM t WHERE ratelimit = CAST(3 AS int)");
    }

    #[test]
    fn the_three_statements_parse() {
        assert_eq!(
            parse("PREPARE p(int, text) AS SELECT $1 + 1, $2").unwrap().unwrap(),
            SqlPrepare::Prepare { name: "p".into(), types: vec!["int".into(), "text".into()], body: "SELECT $1 + 1, $2".into() }
        );
        assert_eq!(parse("EXECUTE P(4, 'x')").unwrap().unwrap(), SqlPrepare::Execute { name: "p".into(), args: vec!["4".into(), "'x'".into()] });
        assert_eq!(parse("EXECUTE p").unwrap().unwrap(), SqlPrepare::Execute { name: "p".into(), args: vec![] });
        assert_eq!(parse("DEALLOCATE PREPARE p").unwrap().unwrap(), SqlPrepare::Deallocate { name: Some("p".into()) });
        assert_eq!(parse("DEALLOCATE ALL").unwrap().unwrap(), SqlPrepare::Deallocate { name: None });
        assert_eq!(parse("PREPARE TRANSACTION 'x'"), None);
        assert_eq!(parse("SELECT 1"), None);
    }

    #[test]
    fn arguments_go_in_for_their_parameters() {
        let p = Prepared { types: vec!["int".into(), "text".into()], body: "SELECT $1 + 1, '$1', $2".into() };
        assert_eq!(bind("p", &p, &["4".into(), "'x'".into()]).unwrap(), "SELECT CAST(4 AS int) + 1, '$1', 'x'");
        // A string for a text parameter goes in as written, so an operand
        // that reads a literal (a JSON key) takes it; anything else is cast.
        let key = Prepared { types: vec!["text".into(), "varchar(2)".into()], body: "SELECT d -> $1 ->> 'k', $2".into() };
        assert_eq!(
            bind("k", &key, &["'0'".into(), "'abc'".into()]).unwrap(),
            "SELECT d -> '0' ->> 'k', CAST('abc' AS varchar(2))"
        );
        assert_eq!(bind("k", &key, &["upper('a')".into(), "'x'".into()]).unwrap(), "SELECT d -> CAST(upper('a') AS text) ->> 'k', CAST('x' AS varchar(2))");
        assert_eq!(bind("p", &p, &["4".into()]).unwrap_err().0, "42601");
        let untyped = Prepared { types: vec![], body: "SELECT $2".into() };
        assert_eq!(bind("u", &untyped, &["1".into()]).unwrap_err().0, "42P02");
    }
}
