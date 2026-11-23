//! `TRUNCATE [TABLE] [ONLY] name [*] [, …] [RESTART IDENTITY | CONTINUE
//! IDENTITY] [CASCADE | RESTRICT]`.
//!
//! PostgreSQL empties each table at once, under ACCESS EXCLUSIVE, and the
//! statement is transactional: a rollback brings the rows back. Here a
//! table's files cannot be swapped inside a transaction (DDL is not
//! transactional in this engine), so a TRUNCATE is every table's rows
//! deleted as one statement of the caller's transaction, under the same
//! ACCESS EXCLUSIVE locks (`executor::execute_truncate`). It rolls back like
//! PostgreSQL's; the space comes back at the next vacuum, not at once.
//!
//! A table another table's foreign key references is refused (0A000) unless
//! that table is truncated too, by name or by `CASCADE`, as in PostgreSQL.

/// A parsed `TRUNCATE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    /// The tables as written, without quotes, schema or `*`; in order, each
    /// once.
    pub tables: Vec<String>,
    /// `RESTART IDENTITY`: the tables' own sequences start again.
    pub restart_identity: bool,
    /// `CASCADE`: the tables whose foreign keys reference these are
    /// truncated too.
    pub cascade: bool,
}

/// The statement, `None` when it is not a `TRUNCATE`, `Err` with
/// PostgreSQL's 42601 wording when it is one this cannot read.
pub fn parse(sql: &str) -> Option<Result<Truncate, String>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let mut words = crate::core::sql_scan::split_top_level(text, b',').into_iter();
    let first = words.next()?;
    let head: Vec<&str> = first.split_whitespace().collect();
    if !head.first()?.eq_ignore_ascii_case("TRUNCATE") {
        return None;
    }
    // The first piece holds `TRUNCATE [TABLE] [ONLY] name`, the last the
    // options after its name; a middle one only a name.
    let mut pieces: Vec<Vec<&str>> = vec![head[1..].to_vec()];
    pieces.extend(words.map(|p| p.split_whitespace().collect()));
    let skip = |w: &[&str], kw: &str| w.first().is_some_and(|f| f.eq_ignore_ascii_case(kw));
    if skip(&pieces[0], "TABLE") {
        pieces[0].remove(0);
    }

    let mut tables: Vec<String> = Vec::new();
    let mut restart_identity = false;
    let mut cascade = false;
    let last = pieces.len() - 1;
    for (i, mut piece) in pieces.into_iter().enumerate() {
        // `ONLY` excludes descendant tables and `*` includes them; together
        // they are PostgreSQL's syntax error.
        let only = skip(&piece, "ONLY");
        if only {
            piece.remove(0);
        }
        if only && (piece.first().is_some_and(|n| n.ends_with('*')) || piece.get(1) == Some(&"*")) {
            return Some(Err("syntax error at or near \"*\"".to_string()));
        }
        let Some(name) = piece.first() else {
            return Some(Err("syntax error at or near \",\"".to_string()));
        };
        let name = name.strip_suffix('*').unwrap_or(name).trim_end();
        let name = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
        if name.is_empty() {
            return Some(Err("syntax error at end of input".to_string()));
        }
        if !tables.iter().any(|t| t == name) {
            tables.push(name.to_string());
        }
        let mut rest = &piece[1..];
        if rest.first() == Some(&"*") {
            rest = &rest[1..];
        }
        if i != last && !rest.is_empty() {
            return Some(Err(format!("syntax error at or near \"{}\"", rest[0])));
        }
        let mut k = 0;
        while k < rest.len() {
            let w = rest[k].to_ascii_uppercase();
            let next = rest.get(k + 1).map(|n| n.to_ascii_uppercase());
            match (w.as_str(), next.as_deref()) {
                ("RESTART", Some("IDENTITY")) => {
                    restart_identity = true;
                    k += 2;
                }
                ("CONTINUE", Some("IDENTITY")) => {
                    restart_identity = false;
                    k += 2;
                }
                ("CASCADE", _) => {
                    cascade = true;
                    k += 1;
                }
                ("RESTRICT", _) => {
                    cascade = false;
                    k += 1;
                }
                _ => return Some(Err(format!("syntax error at or near \"{}\"", rest[k]))),
            }
        }
    }
    Some(Ok(Truncate {
        tables,
        restart_identity,
        cascade,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> Truncate {
        parse(sql).unwrap_or_else(|| panic!("{sql}: not a TRUNCATE")).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    #[test]
    fn every_form_postgresql_accepts_is_read() {
        let t = ok("TRUNCATE t");
        assert_eq!((t.tables, t.restart_identity, t.cascade), (vec!["t".to_string()], false, false));
        let t = ok("truncate table public.\"a\" *, only b, a RESTART IDENTITY CASCADE;");
        assert_eq!((t.tables, t.restart_identity, t.cascade), (vec!["a".to_string(), "b".to_string()], true, true));
        let t = ok("TRUNCATE a, ONLY b CONTINUE IDENTITY RESTRICT");
        assert_eq!((t.tables, t.restart_identity, t.cascade), (vec!["a".to_string(), "b".to_string()], false, false));
        let t = ok("TRUNCATE TABLE x*");
        assert_eq!(t.tables, vec!["x".to_string()]);
    }

    #[test]
    fn what_it_cannot_read_is_a_syntax_error() {
        assert!(parse("SELECT 1").is_none());
        assert!(parse("TRUNCATED t").is_none());
        for sql in ["TRUNCATE", "TRUNCATE TABLE", "TRUNCATE ONLY s *", "TRUNCATE ONLY s*", "TRUNCATE a CASCADE, b", "TRUNCATE a RESTART", "TRUNCATE a, , b", "TRUNCATE a b"] {
            assert!(matches!(parse(sql), Some(Err(_))), "{sql}: {:?}", parse(sql));
        }
    }
}
