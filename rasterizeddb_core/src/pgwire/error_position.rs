//! Where in the client's text an error points: the `P` field of an
//! `ErrorResponse`, which `psql` prints as `at character 30` (terse) or as a
//! caret under the statement, and drivers expose (`position`).
//!
//! PostgreSQL's parser knows the token each error came from. The engine
//! here reports by message, so the position is found back in the text from
//! what the message names: a column, relation, type, function or
//! `TABLESAMPLE` method's name, the literal an input error quotes, a
//! qualifier with no `FROM` entry: its first use outside quotes and
//! comments, which is the one PostgreSQL reports for a name used twice (it
//! reads the select list, then `WHERE`, in the order they are written).
//! Nothing found, no position, as before.

use crate::core::sql_scan::{is_identifier_byte, quoted_end, skip_comment};

/// An undefined-column message in PostgreSQL's words, `None` for one that
/// already is or is no such message. The engine's readers word it their
/// own way (`Column 'x' not found in schema`, an `ORDER BY` term "neither a
/// column nor a value", `column "x" of table "t"`); a client sees what
/// PostgreSQL says, which [`find`] also positions. `text` is the statement,
/// for the relation an `UPDATE` names.
pub(super) fn pg_wording(text: &str, message: &str) -> Option<String> {
    let plain = |name: &str| !name.is_empty() && name.bytes().all(|b| is_identifier_byte(b) || b == b'.');
    let column = |name: &str| match name.split_once('.') {
        Some(_) => format!("column {name} does not exist"),
        None => format!("column \"{name}\" does not exist"),
    };
    // Only a name: the same words about an expression (`Column 'x + 1' not
    // found`) also report one the reader cannot take, every column of it
    // present.
    if let Some(name) = message.strip_prefix("Column '").and_then(|r| r.strip_suffix("' not found in schema"))
        && plain(name)
    {
        return Some(column(name));
    }
    if let Some(rest) = message.strip_prefix("ORDER BY term '")
        && let Some((name, tail)) = rest.split_once('\'')
        && tail.ends_with(" is neither a column nor a value")
        && plain(name)
    {
        return Some(column(name));
    }
    if let Some(rest) = message.strip_prefix("column \"")
        && let Some((name, table)) = rest.split_once("\" of table \"")
        && let Some(table) = table.strip_suffix("\" does not exist")
    {
        return Some(format!("column \"{name}\" of relation \"{table}\" does not exist"));
    }
    if let Some(name) = message.strip_prefix("column '").and_then(|r| r.strip_suffix("' does not exist"))
        && plain(name)
    {
        // `UPDATE t SET nosuch = …`: PostgreSQL names the relation.
        let head = text.trim_start();
        if head.len() > 7 && head.as_bytes()[..7].eq_ignore_ascii_case(b"UPDATE ") {
            let table: String = head[7..].trim_start().bytes().take_while(|b| is_identifier_byte(*b) || *b == b'.').map(|b| b as char).collect();
            if !table.is_empty() {
                let table = table.rsplit('.').next().unwrap_or(&table).to_ascii_lowercase();
                return Some(format!("column \"{name}\" of relation \"{table}\" does not exist"));
            }
        }
        return Some(column(name));
    }
    None
}

/// The 1-based character position in `text` of what `message` names, `None`
/// when it names nothing found.
pub(super) fn find(text: &str, message: &str) -> Option<usize> {
    let byte = locate(text, message)?;
    Some(text[..byte].chars().count() + 1)
}

fn locate(text: &str, message: &str) -> Option<usize> {
    // The literal an input error quotes: `invalid input value for enum t:
    // "x"`, `invalid input syntax for type integer: "x"`.
    if message.starts_with("invalid input value for enum ") || message.starts_with("invalid input syntax for type ") {
        let value = quoted_tail(message)?;
        return literal(text, &value);
    }
    // `column "x" of relation "t" does not exist`: at the column.
    if let Some(rest) = message.strip_prefix("column \"")
        && let Some((name, _)) = rest.split_once("\" of relation \"")
    {
        return word(text, name, Some(false));
    }
    // `column "x" does not exist`, `column t.x does not exist`.
    if let Some(rest) = message.strip_prefix("column ")
        && let Some(name) = rest.strip_suffix(" does not exist")
    {
        let name = name.trim_matches('"');
        return match name.split_once('.') {
            Some((qualifier, column)) => qualified(text, qualifier, column),
            None => word(text, name, Some(false)),
        };
    }
    if let Some(name) = between(message, "relation \"", "\" does not exist")
        .or_else(|| between(message, "type \"", "\" does not exist"))
        .or_else(|| between(message, "column reference \"", "\" is ambiguous"))
    {
        let name = name.rsplit('.').next().unwrap_or(name);
        return word(text, name, Some(false));
    }
    if let Some(qualifier) = between(message, "missing FROM-clause entry for table \"", "\"") {
        return qualifier_use(text, qualifier);
    }
    if let Some(name) = between(message, "tablesample method ", " does not exist") {
        return word(text, name, None);
    }
    // `function f(integer) does not exist`.
    if let Some(rest) = message.strip_prefix("function ")
        && rest.ends_with(" does not exist")
        && let Some(open) = rest.find('(')
    {
        return match rest[..open].split_once('.') {
            // `public.f(1)`: at the qualifier, where PostgreSQL points.
            Some((qualifier, name)) => qualified(text, qualifier, name),
            None => word(text, &rest[..open], Some(true)),
        };
    }
    None
}

/// The text between `start` and `end` in `message`, when it is one name.
fn between<'m>(message: &'m str, start: &str, end: &str) -> Option<&'m str> {
    let rest = message.strip_prefix(start)?;
    let name = rest.strip_suffix(end).or_else(|| rest.split_once(end).map(|(n, _)| n))?;
    (!name.is_empty() && !name.contains(' ')).then_some(name)
}

/// The value quoted at the end of a message: `…: "x"`.
fn quoted_tail(message: &str) -> Option<String> {
    let at = message.find(": \"")?;
    let inner = message[at + 3..].strip_suffix('"')?;
    Some(inner.to_string())
}

/// The first place outside comments (and inside no literal or quoted name
/// but at its opening quote) a predicate accepts, by byte.
fn scan(text: &str, mut accept: impl FnMut(usize) -> bool) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'"' {
            // A quoted identifier is a name: tried at its opening quote.
            if accept(i) {
                return Some(i);
            }
            i = quoted_end(bytes, i, b'"');
            continue;
        }
        if bytes[i] == b'\'' {
            if accept(i) {
                return Some(i);
            }
            i = quoted_end(bytes, i, b'\'');
            continue;
        }
        if accept(i) {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// The name at `i`, as written (`x`, `"X"`), with where it ends.
fn name_at(text: &str, i: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    if bytes.get(i) == Some(&b'"') {
        let end = quoted_end(bytes, i, b'"');
        return Some((text[i + 1..end.saturating_sub(1).max(i + 1)].replace("\"\"", "\""), end));
    }
    if i > 0 && (is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'$') {
        return None;
    }
    let n = bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
    (n > 0 && !bytes[i].is_ascii_digit()).then(|| (text[i..i + n].to_ascii_lowercase(), i + n))
}

/// `name` used as a whole word: a call when `call` is `Some(true)`, no call
/// when `Some(false)`, either for `None`. Not after a `.`, where it is a
/// column of something else.
fn word(text: &str, name: &str, call: Option<bool>) -> Option<usize> {
    let bytes = text.as_bytes();
    scan(text, |i| {
        if bytes[i] == b'\'' || (i > 0 && bytes[i - 1] == b'.') {
            return false;
        }
        let Some((found, end)) = name_at(text, i) else { return false };
        let same = if bytes[i] == b'"' { found == name } else { found == name.to_ascii_lowercase() };
        // `… AS b` names an output column; a reference to `b` is elsewhere.
        let output_name = || {
            // By bytes: a multi-byte character may end the text before.
            let before = text[..i].trim_end().as_bytes();
            before.len() >= 2
                && before[before.len() - 2..].eq_ignore_ascii_case(b"as")
                && !before[..before.len() - 2].last().is_some_and(|b| is_identifier_byte(*b))
        };
        same && bytes.get(end) != Some(&b'.')
            && call.is_none_or(|c| c == text[end..].trim_start().starts_with('('))
            && !(call == Some(false) && output_name())
    })
}

/// `qualifier.column`, at the qualifier.
fn qualified(text: &str, qualifier: &str, column: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    scan(text, |i| {
        if bytes[i] == b'\'' || (i > 0 && bytes[i - 1] == b'.') {
            return false;
        }
        let Some((q, end)) = name_at(text, i) else { return false };
        if q != qualifier.to_ascii_lowercase() || bytes.get(end) != Some(&b'.') {
            return false;
        }
        name_at(text, end + 1).is_some_and(|(c, _)| c == column.to_ascii_lowercase())
    })
}

/// A qualifier used before a `.`.
fn qualifier_use(text: &str, qualifier: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    scan(text, |i| {
        if bytes[i] == b'\'' || (i > 0 && bytes[i - 1] == b'.') {
            return false;
        }
        name_at(text, i).is_some_and(|(q, end)| q == qualifier.to_ascii_lowercase() && bytes.get(end) == Some(&b'.'))
    })
}

/// The string literal `'value'`, at its opening quote.
fn literal(text: &str, value: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    scan(text, |i| {
        if bytes[i] != b'\'' {
            return false;
        }
        let end = quoted_end(bytes, i, b'\'');
        end >= i + 2 && text[i + 1..end - 1].replace("''", "'") == value
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_are_postgresql_s() {
        // Each is PostgreSQL 17's position for the same statement.
        assert_eq!(find("SELECT nosuch FROM t", "column \"nosuch\" does not exist"), Some(8));
        assert_eq!(find("SELECT 1 FROM ga WHERE nosuch = 1", "column \"nosuch\" does not exist"), Some(24));
        assert_eq!(find("SELECT t.zz FROM t", "column t.zz does not exist"), Some(8));
        assert_eq!(find("SELECT id FROM te2 WHERE m > 'glad'", "invalid input value for enum feel: \"glad\""), Some(30));
        assert_eq!(find("SELECT enum_first('x'::ef)", "invalid input value for enum ef: \"x\""), Some(19));
        assert_eq!(find("SELECT * FROM nosuch", "relation \"nosuch\" does not exist"), Some(15));
        assert_eq!(find("SELECT count(*) FROM ts TABLESAMPLE foo (1)", "tablesample method foo does not exist"), Some(37));
        assert_eq!(find("SELECT x.a FROM t", "missing FROM-clause entry for table \"x\""), Some(8));
        assert_eq!(find("SELECT max(id) FROM ts t JOIN ts u ON t.id = u.id", "column reference \"id\" is ambiguous"), Some(12));
        assert_eq!(find("SELECT nofn(1)", "function nofn(integer) does not exist"), Some(8));
        // Counted in characters, not bytes.
        assert_eq!(find("SELECT 'é', nosuch FROM t", "column \"nosuch\" does not exist"), Some(13));
        // An output name is no reference; a qualified call is at its qualifier.
        assert_eq!(find("SELECT a + 1 AS b FROM fx ORDER BY b + nosuch", "column \"b\" does not exist"), Some(36));
        assert_eq!(find("SELECT a AS é, b FROM fx", "column \"b\" does not exist"), Some(16));
        assert_eq!(find("SELECT public.nofn(1)", "function public.nofn(integer) does not exist"), Some(8));
    }

    #[test]
    fn engine_wording_becomes_postgresql_s() {
        assert_eq!(pg_wording("", "Column 'nosuch' not found in schema").as_deref(), Some("column \"nosuch\" does not exist"));
        assert_eq!(pg_wording("", "Column 'ep.zz' not found in schema").as_deref(), Some("column ep.zz does not exist"));
        assert_eq!(
            pg_wording("", "ORDER BY term 'nosuch' is not a column and could not be read as an expression: 'nosuch' is neither a column nor a value").as_deref(),
            Some("column \"nosuch\" does not exist")
        );
        assert_eq!(
            pg_wording("", "column \"nosuch\" of table \"ep\" does not exist").as_deref(),
            Some("column \"nosuch\" of relation \"ep\" does not exist")
        );
        assert_eq!(
            pg_wording("UPDATE ep SET nosuch = 1", "column 'nosuch' does not exist").as_deref(),
            Some("column \"nosuch\" of relation \"ep\" does not exist")
        );
        assert_eq!(pg_wording("", "Column 'a + b' not found in schema"), None);
        assert_eq!(find("INSERT INTO ep (id, nosuch) VALUES (1, 2)", "column \"nosuch\" of relation \"ep\" does not exist"), Some(21));
        assert_eq!(find("UPDATE ep SET nosuch = 1", "column \"nosuch\" of relation \"ep\" does not exist"), Some(15));
    }

    #[test]
    fn the_first_use_or_none() {
        assert_eq!(find("SELECT a FROM t WHERE a = 1", "column \"a\" does not exist"), Some(8));
        assert_eq!(find("SELECT 'nosuch' FROM t", "column \"nosuch\" does not exist"), None);
        assert_eq!(find("SELECT 1", "some other error"), None);
    }
}
