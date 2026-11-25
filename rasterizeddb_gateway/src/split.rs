//! A multi-row `INSERT … VALUES (…), (…)` split by the shard each row goes
//! to (sharding-plan.md B1a): every row routed as the one-row INSERT it
//! would be, the rows of each shard sent to it as one INSERT, all inside one
//! two-phase transaction.
//!
//! Only the text is split: the statement up to `VALUES`, each top-level
//! parenthesised row, and what follows the last row (`ON CONFLICT`,
//! `RETURNING`) are kept as written, so every part is the client's own SQL
//! with fewer rows.

/// An INSERT's text cut around its rows.
#[derive(Debug, PartialEq, Eq)]
pub struct Rows<'a> {
    /// Up to and including `VALUES` and the whitespace after it.
    pub head: &'a str,
    /// Each row, parentheses included.
    pub rows: Vec<&'a str>,
    /// After the last row: empty, or ` ON CONFLICT …`, ` RETURNING …`.
    pub tail: &'a str,
}

impl Rows<'_> {
    /// The INSERT of `rows` (indexes into `self.rows`).
    pub fn statement(&self, rows: &[usize]) -> String {
        let mut s = String::with_capacity(self.head.len() + self.tail.len() + rows.len() * 16);
        s.push_str(self.head);
        for (n, &i) in rows.iter().enumerate() {
            if n > 0 {
                s.push_str(", ");
            }
            s.push_str(self.rows[i]);
        }
        s.push_str(self.tail);
        s
    }
}

fn ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// Where the quoted string, identifier, dollar quote or comment starting
/// at `i` ends (the index after it), or `None` when none starts there. An
/// unterminated one runs to the end.
fn skip(b: &[u8], i: usize) -> Option<usize> {
    match b[i] {
        b'\'' => {
            // E'…' takes backslash escapes.
            let escapes = i > 0 && matches!(b[i - 1], b'e' | b'E') && (i < 2 || !ident(b[i - 2]));
            let mut j = i + 1;
            while j < b.len() {
                match b[j] {
                    b'\\' if escapes => j += 2,
                    b'\'' if b.get(j + 1) == Some(&b'\'') => j += 2,
                    b'\'' => return Some(j + 1),
                    _ => j += 1,
                }
            }
            Some(b.len())
        }
        b'"' => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b'"' {
                    if b.get(j + 1) == Some(&b'"') {
                        j += 2;
                        continue;
                    }
                    return Some(j + 1);
                }
                j += 1;
            }
            Some(b.len())
        }
        b'$' if i == 0 || !ident(b[i - 1]) => {
            // $tag$ … $tag$; `$1` is a parameter, not a quote.
            let mut j = i + 1;
            if b.get(j).is_some_and(|c| c.is_ascii_digit()) {
                return None;
            }
            while j < b.len() && ident(b[j]) {
                j += 1;
            }
            if b.get(j) != Some(&b'$') {
                return None;
            }
            let tag = &b[i..=j];
            let body = j + 1;
            let end = b[body..].windows(tag.len()).position(|w| w == tag).map_or(b.len(), |p| body + p + tag.len());
            Some(end)
        }
        b'-' if b.get(i + 1) == Some(&b'-') => Some(b[i..].iter().position(|&c| c == b'\n').map_or(b.len(), |p| i + p)),
        b'/' if b.get(i + 1) == Some(&b'*') => {
            let (mut depth, mut j) = (1, i + 2);
            while j < b.len() && depth > 0 {
                if b[j] == b'/' && b.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if b[j] == b'*' && b.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        _ => None,
    }
}

/// The rows of `INSERT … VALUES (…), …`, or `None` for anything else (one
/// row, `DEFAULT VALUES`, `INSERT … SELECT`, a `WITH` in front, text this
/// does not follow).
pub fn split(sql: &str) -> Option<Rows<'_>> {
    let t = sql.trim_end();
    let t = t.strip_suffix(';').unwrap_or(t);
    let b = t.as_bytes();
    let start = t.len() - t.trim_start().len();
    if !t[start..].get(..6)?.eq_ignore_ascii_case("insert") {
        return None;
    }
    // VALUES at depth 0.
    let (mut i, mut depth) = (start, 0i32);
    let values_end = loop {
        if i >= b.len() {
            return None;
        }
        if let Some(end) = skip(b, i) {
            i = end;
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            c if depth == 0 && c.is_ascii_alphabetic() && (i == 0 || !ident(b[i - 1])) => {
                let w = i;
                while i < b.len() && ident(b[i]) {
                    i += 1;
                }
                let word = &t[w..i];
                if word.eq_ignore_ascii_case("select") || word.eq_ignore_ascii_case("default") {
                    return None;
                }
                if word.eq_ignore_ascii_case("values") {
                    break i;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    };
    let mut i = values_end;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    let head = &t[..i];
    let mut rows = Vec::new();
    loop {
        if b.get(i) != Some(&b'(') {
            return None;
        }
        let row_start = i;
        let mut depth = 0i32;
        loop {
            if i >= b.len() {
                return None;
            }
            if let Some(end) = skip(b, i) {
                i = end;
                continue;
            }
            match b[i] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        rows.push(&t[row_start..i]);
        let mut j = i;
        while j < b.len() && b[j].is_ascii_whitespace() {
            j += 1;
        }
        if b.get(j) == Some(&b',') {
            i = j + 1;
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        break;
    }
    (rows.len() > 1).then_some(Rows { head, rows, tail: &t[i..] })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_are_cut_at_top_level_only() {
        let r = split("INSERT INTO t (a, b) VALUES (1, 'x,)'), (2, f(3, 4)) RETURNING a;").unwrap();
        assert_eq!(r.head, "INSERT INTO t (a, b) VALUES ");
        assert_eq!(r.rows, vec!["(1, 'x,)')", "(2, f(3, 4))"]);
        assert_eq!(r.tail, " RETURNING a");
        assert_eq!(r.statement(&[1]), "INSERT INTO t (a, b) VALUES (2, f(3, 4)) RETURNING a");
        assert_eq!(r.statement(&[0, 1]), "INSERT INTO t (a, b) VALUES (1, 'x,)'), (2, f(3, 4)) RETURNING a");
    }

    #[test]
    fn quotes_comments_and_dollar_quotes_hide_parentheses() {
        let r = split("insert into t values (E'a\\')', \"q)\"), ($$)$$, /* ) */ 2), ($x$ ( $x$, $1) ON CONFLICT (a) DO NOTHING").unwrap();
        assert_eq!(r.rows, vec!["(E'a\\')', \"q)\")", "($$)$$, /* ) */ 2)", "($x$ ( $x$, $1)"]);
        assert_eq!(r.tail, " ON CONFLICT (a) DO NOTHING");
        let r = split("INSERT INTO t VALUES ('it''s', 1),(2,3)").unwrap();
        assert_eq!(r.rows, vec!["('it''s', 1)", "(2,3)"]);
    }

    #[test]
    fn other_inserts_are_not_split() {
        assert_eq!(split("INSERT INTO t VALUES (1)"), None);
        assert_eq!(split("INSERT INTO t DEFAULT VALUES"), None);
        assert_eq!(split("INSERT INTO t SELECT * FROM (VALUES (1), (2)) v"), None);
        assert_eq!(split("WITH x AS (SELECT 1) INSERT INTO t VALUES (1), (2)"), None);
        assert_eq!(split("UPDATE t SET a = 1"), None);
        assert_eq!(split("INSERT INTO t VALUES (1), (2"), None);
    }
}
