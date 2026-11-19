//! `MERGE`, as PostgreSQL 17 has it: `MERGE INTO target [AS] alias USING
//! source [AS] alias ON condition`, then `WHEN MATCHED`, `WHEN NOT MATCHED
//! [BY TARGET]` and `WHEN NOT MATCHED BY SOURCE` clauses, each with an
//! optional `AND condition` and an action, and an optional `RETURNING` (with
//! `merge_action()`). Parsed here; run by `pgwire::merge_exec`.

use crate::core::rql::join::find_keyword;

/// A refusal: its SQLSTATE and message.
pub type Refusal = (&'static str, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhenKind {
    Matched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// `UPDATE SET col = expr, …`.
    Update(Vec<(String, String)>),
    Delete,
    /// `INSERT [(cols)] VALUES (exprs)`; `values` `None` for `DEFAULT
    /// VALUES`.
    Insert { columns: Option<Vec<String>>, values: Option<Vec<String>> },
    Nothing,
}

impl Action {
    /// What `merge_action()` reports for it.
    pub fn word(&self) -> &'static str {
        match self {
            Action::Update(_) => "UPDATE",
            Action::Delete => "DELETE",
            Action::Insert { .. } => "INSERT",
            Action::Nothing => "",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhenClause {
    pub kind: WhenKind,
    pub condition: Option<String>,
    pub action: Action,
}

/// A `MERGE`, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merge {
    pub target: String,
    pub target_alias: String,
    /// The source as it can follow `FROM` (`src AS s`, `(SELECT …) AS s`,
    /// `(VALUES …) AS s(a, b)`).
    pub source: String,
    pub source_alias: String,
    pub on: String,
    pub clauses: Vec<WhenClause>,
    pub returning: Option<String>,
}

fn ident(word: &str) -> String {
    let w = word.trim();
    if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

/// `name [AS] alias` (or `name`): the relation and its alias.
fn relation_and_alias(text: &str) -> Option<(String, String)> {
    let text = text.trim();
    let mut words: Vec<&str> = text.split_whitespace().collect();
    if words.is_empty() {
        return None;
    }
    let name = ident(crate::core::sql_scan::last_name_segment(words.remove(0)));
    if words.first().is_some_and(|w| w.eq_ignore_ascii_case("AS")) {
        words.remove(0);
    }
    let alias = words.first().map(|a| ident(a)).unwrap_or_else(|| name.clone());
    Some((name, alias))
}

/// The alias a `USING` source is known by: the word after it (past a column
/// list, `AS s(a, b)`), or a bare table's own name; `None` for a subquery
/// with no alias.
fn source_alias(source: &str) -> Option<String> {
    let mut s = source.trim();
    // A trailing `(…)` after a word is that alias's column list.
    if s.ends_with(')') {
        let bytes = s.as_bytes();
        let mut depth = 0i32;
        let mut open = None;
        for i in (0..bytes.len()).rev() {
            match bytes[i] {
                b')' => depth += 1,
                b'(' => {
                    depth -= 1;
                    if depth == 0 {
                        open = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let open = open?;
        let before = s[..open].trim_end();
        if before.ends_with(|c: char| c.is_alphanumeric() || c == '_' || c == '"') {
            s = before;
        } else {
            return None;
        }
    }
    let words: Vec<&str> = s.split_whitespace().collect();
    let last = *words.last()?;
    if last.ends_with(')') {
        return None;
    }
    Some(ident(crate::core::sql_scan::last_name_segment(last)))
}

/// Parses a `MERGE`; `None` for any other statement.
pub fn parse_merge(sql: &str) -> Option<Result<Merge, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let upper_head: String = text.chars().take(5).collect::<String>().to_ascii_uppercase();
    if upper_head != "MERGE" || !text[5..].starts_with(char::is_whitespace) {
        return None;
    }
    // PostgreSQL's MERGE is `MERGE INTO`; any other word after MERGE is
    // another statement's (`MERGE BRANCH dev`), which its own handler reads.
    let next = text[5..].trim_start();
    if !(next.len() > 4 && next[..4].eq_ignore_ascii_case("INTO") && next.as_bytes()[4].is_ascii_whitespace()) {
        return None;
    }
    Some(parse(text))
}

fn parse(text: &str) -> Result<Merge, Refusal> {
    let syntax = |what: &str| ("42601", format!("syntax error in MERGE: {what}"));
    let into = find_keyword(text, "INTO", 0).ok_or_else(|| syntax("expected INTO"))? + 4;
    let using = find_keyword(text, "USING", into).ok_or_else(|| syntax("expected USING"))?;
    let (target, target_alias) = relation_and_alias(&text[into..using]).ok_or_else(|| syntax("expected a target table"))?;
    let on = find_keyword(text, "ON", using).ok_or_else(|| syntax("expected ON"))?;
    let source = text[using + 5..on].trim().to_string();
    if source.is_empty() {
        return Err(syntax("expected a source"));
    }
    let source_alias = source_alias(&source).ok_or_else(|| ("42601", "the MERGE source needs an alias here".to_string()))?;
    let first_when = find_keyword(text, "WHEN", on).ok_or_else(|| syntax("expected WHEN"))?;
    let on_cond = text[on + 2..first_when].trim().to_string();
    let returning_at = find_keyword(text, "RETURNING", first_when);
    let clauses_text = &text[first_when..returning_at.unwrap_or(text.len())];
    let returning = returning_at.map(|at| text[at + 9..].trim().to_string());

    // Each clause runs from its WHEN to the next top-level WHEN that opens
    // a clause (`WHEN [NOT] MATCHED`), not a CASE's WHEN.
    let mut starts = Vec::new();
    let mut from = 0;
    while let Some(at) = find_keyword(clauses_text, "WHEN", from) {
        let after = clauses_text[at + 4..].trim_start().to_ascii_uppercase();
        if after.starts_with("MATCHED") || after.starts_with("NOT MATCHED") || after.starts_with("NOT\n") {
            starts.push(at);
        }
        from = at + 4;
    }
    let mut clauses = Vec::new();
    for (i, start) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(clauses_text.len());
        clauses.push(clause(clauses_text[start + 4..end].trim())?);
    }
    if clauses.is_empty() {
        return Err(syntax("expected WHEN [NOT] MATCHED"));
    }
    Ok(Merge { target, target_alias, source, source_alias, on: on_cond, clauses, returning })
}

fn clause(text: &str) -> Result<WhenClause, Refusal> {
    let syntax = |what: &str| ("42601", format!("syntax error in MERGE: {what}"));
    let upper = text.to_ascii_uppercase();
    let (kind, rest) = if upper.starts_with("MATCHED") {
        (WhenKind::Matched, &text[7..])
    } else if upper.starts_with("NOT") {
        let after = text[3..].trim_start();
        let upper_after = after.to_ascii_uppercase();
        if !upper_after.starts_with("MATCHED") {
            return Err(syntax("expected NOT MATCHED"));
        }
        let after = after[7..].trim_start();
        let upper_after = after.to_ascii_uppercase();
        if upper_after.starts_with("BY SOURCE") {
            (WhenKind::NotMatchedBySource, &after[9..])
        } else if upper_after.starts_with("BY TARGET") {
            (WhenKind::NotMatchedByTarget, &after[9..])
        } else {
            (WhenKind::NotMatchedByTarget, after)
        }
    } else {
        return Err(syntax("expected MATCHED or NOT MATCHED"));
    };
    let then = find_keyword(rest, "THEN", 0).ok_or_else(|| syntax("expected THEN"))?;
    let head = rest[..then].trim();
    let condition = if head.is_empty() {
        None
    } else if head.len() >= 3 && head[..3].eq_ignore_ascii_case("AND") {
        Some(head[3..].trim().to_string())
    } else {
        return Err(syntax(&format!("unexpected \"{head}\"")));
    };
    let action_text = rest[then + 4..].trim();
    let upper = action_text.to_ascii_uppercase();
    let action = if upper.starts_with("DO NOTHING") {
        Action::Nothing
    } else if upper.starts_with("DELETE") {
        Action::Delete
    } else if upper.starts_with("UPDATE") {
        let set = find_keyword(action_text, "SET", 0).ok_or_else(|| syntax("UPDATE needs SET"))? + 3;
        let mut assignments = Vec::new();
        for piece in crate::core::sql_scan::split_top_level(&action_text[set..], b',') {
            let (column, expr) = piece.split_once('=').ok_or_else(|| syntax("SET needs column = expression"))?;
            let column = column.trim();
            if column.starts_with('(') {
                return Err(("0A000", "MERGE UPDATE SET (a, b) = … is not supported yet".to_string()));
            }
            assignments.push((ident(crate::core::sql_scan::last_name_segment(column)), expr.trim().to_string()));
        }
        Action::Update(assignments)
    } else if upper.starts_with("INSERT") {
        let rest = action_text[6..].trim();
        let (columns, rest) = if rest.starts_with('(') {
            let close = crate::core::sql_scan::matching_close(rest, 0).ok_or_else(|| syntax("unbalanced INSERT column list"))?;
            let columns = crate::core::sql_scan::split_top_level(&rest[1..close], b',').into_iter().map(ident).collect();
            (Some(columns), rest[close + 1..].trim())
        } else {
            (None, rest)
        };
        let upper_rest = rest.to_ascii_uppercase();
        if upper_rest.starts_with("OVERRIDING") {
            return Err(("0A000", "MERGE INSERT OVERRIDING is not supported yet".to_string()));
        }
        if upper_rest.starts_with("DEFAULT VALUES") {
            Action::Insert { columns, values: None }
        } else if upper_rest.starts_with("VALUES") {
            let open = rest.find('(').ok_or_else(|| syntax("VALUES needs a tuple"))?;
            let close = crate::core::sql_scan::matching_close(rest, open).ok_or_else(|| syntax("unbalanced VALUES"))?;
            let values = crate::core::sql_scan::split_top_level(&rest[open + 1..close], b',')
                .into_iter()
                .map(|v| v.trim().to_string())
                .collect();
            Action::Insert { columns, values: Some(values) }
        } else {
            return Err(syntax("INSERT needs VALUES or DEFAULT VALUES"));
        }
    } else {
        return Err(syntax(&format!("unexpected action \"{action_text}\"")));
    };
    match (&kind, &action) {
        (WhenKind::NotMatchedByTarget, Action::Update(_) | Action::Delete) => {
            return Err(syntax("WHEN NOT MATCHED [BY TARGET] can only INSERT or DO NOTHING"));
        }
        (WhenKind::Matched | WhenKind::NotMatchedBySource, Action::Insert { .. }) => {
            return Err(syntax("INSERT is only allowed in WHEN NOT MATCHED [BY TARGET]"));
        }
        _ => {}
    }
    Ok(WhenClause { kind, condition, action })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_merge_is_read() {
        let m = parse_merge(
            "MERGE INTO public.stock AS t USING (VALUES (1, 5), (9, 2)) AS s(id, qty) ON t.id = s.id \
             WHEN MATCHED AND s.qty = 0 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty, note = CASE WHEN s.qty > 3 THEN 'big' END \
             WHEN NOT MATCHED THEN INSERT (id, qty) VALUES (s.id, s.qty) \
             WHEN NOT MATCHED BY SOURCE THEN DO NOTHING \
             RETURNING merge_action(), t.*",
        )
        .unwrap()
        .unwrap();
        assert_eq!((m.target.as_str(), m.target_alias.as_str(), m.source_alias.as_str()), ("stock", "t", "s"));
        assert_eq!(m.source, "(VALUES (1, 5), (9, 2)) AS s(id, qty)");
        assert_eq!(m.on, "t.id = s.id");
        assert_eq!(m.clauses.len(), 4);
        assert_eq!(m.clauses[0], WhenClause { kind: WhenKind::Matched, condition: Some("s.qty = 0".into()), action: Action::Delete });
        let Action::Update(set) = &m.clauses[1].action else { panic!() };
        assert_eq!(set[1], ("note".into(), "CASE WHEN s.qty > 3 THEN 'big' END".into()));
        assert_eq!(m.clauses[3].kind, WhenKind::NotMatchedBySource);
        assert_eq!(m.returning.as_deref(), Some("merge_action(), t.*"));
        let m = parse_merge("MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT DEFAULT VALUES").unwrap().unwrap();
        assert_eq!((m.target_alias.as_str(), m.source_alias.as_str()), ("t", "s"));
        assert!(parse_merge("MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN DELETE").unwrap().is_err());
        assert!(parse_merge("SELECT 1").is_none());
        assert!(parse_merge("MERGE BRANCH dev").is_none());
        assert!(parse_merge("merge branch dev INTO main").is_none());
    }
}
