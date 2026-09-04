//! Parsing for the `ORDER BY` clause.
//!
//! A sort is a *list* of keys, each with its own direction: `ORDER BY dept,
//! salary DESC` groups by department and then puts the best paid first within
//! each one. Only the first key used to be honoured — a second was parsed and
//! then dropped, so that query silently sorted by `dept` alone.
//!
//! Both the single-table planner and the join planner split their statement
//! themselves, so the shared part is here: turning the text between `ORDER BY`
//! and the end of the clause into the keys it names.

/// One `ORDER BY` term, as written.
///
/// `token` is left exactly as the statement spelled it — qualified, quoted, an
/// output label, or an aggregate call — because who can resolve it differs:
/// a plain query resolves against the schema, an aggregate against the labels
/// its fold produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortTerm {
    pub token: String,
    pub descending: bool,
    /// `NULLS FIRST` / `NULLS LAST`, when written. `None` follows PostgreSQL's
    /// default: NULLs sort as the largest value — last ascending, first
    /// descending. Only the expression-sort path can honour it; every other
    /// consumer must refuse a term that carries one rather than ignore it.
    pub nulls_first: Option<bool>,
}

/// Rewrites `ORDER BY 2` into the select item that position names.
///
/// SQL lets a sort key be an ordinal into the select list, and it is how a
/// hand-written query or a migration script usually spells "sort by the second
/// column". Resolved here, once, into the item's own label, so every consumer
/// below — the scan's byte sort, the expression sort, the join, the aggregate
/// fold — sees an ordinary key and none of them needs to know ordinals exist.
///
/// Only a bare integer is one. `ORDER BY 1 + 1` is an expression whose value
/// happens to be a number, and PostgreSQL treats it as such.
pub fn resolve_ordinals(terms: &mut [SortTerm], labels: &[String]) -> Result<(), String> {
    for term in terms.iter_mut() {
        let trimmed = term.token.trim();
        if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let position: usize = trimmed
            .parse()
            .map_err(|_| format!("ORDER BY position '{}' is not a number", trimmed))?;
        // One-based, and zero is not a position at all.
        if position == 0 || position > labels.len() {
            return Err(format!(
                "ORDER BY position {} is not in the select list, which has {} item(s)",
                position,
                labels.len()
            ));
        }
        term.token = labels[position - 1].clone();
    }
    Ok(())
}

/// Splits a comma-separated list on the commas that sit outside parentheses and
/// quotes, so `COUNT(a, b)` stays one term.
fn split_terms(clause: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;

    for c in clause.chars() {
        match c {
            '\'' | '"' if quote.is_none() => {
                quote = Some(c);
                current.push(c);
            }
            c if Some(c) == quote => {
                quote = None;
                current.push(c);
            }
            '(' if quote.is_none() => {
                depth += 1;
                current.push(c);
            }
            ')' if quote.is_none() => {
                depth = depth.saturating_sub(1);
                current.push(c);
            }
            ',' if quote.is_none() && depth == 0 => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    out.push(current);
    out
}

/// Splits one term into the thing being sorted and the words that follow it.
///
/// The split is on the first whitespace outside parentheses, so `COUNT(*) DESC`
/// yields `COUNT(*)` and `DESC` rather than three fragments.
fn split_term(term: &str) -> (&str, &str) {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (i, c) in term.char_indices() {
        match c {
            '\'' | '"' if quote.is_none() => quote = Some(c),
            c if Some(c) == quote => quote = None,
            '(' if quote.is_none() => depth += 1,
            ')' if quote.is_none() => depth = depth.saturating_sub(1),
            c if c.is_whitespace() && quote.is_none() && depth == 0 => {
                return (&term[..i], term[i..].trim());
            }
            _ => {}
        }
    }
    (term, "")
}

/// End index (exclusive) of the `CASE … END` block opening at the start of
/// `term`, counting nested blocks. A CASE keeps its arms at paren depth zero,
/// so the whitespace splitter below would otherwise cut it at its first space.
fn case_block_end(term: &str) -> Option<usize> {
    use crate::core::rql::join::find_keyword;
    let mut depth = 1usize;
    let mut at = "CASE".len();
    loop {
        let next_case = find_keyword(term, "CASE", at);
        let next_end = find_keyword(term, "END", at)?;
        match next_case {
            Some(c) if c < next_end => {
                depth += 1;
                at = c + "CASE".len();
            }
            _ => {
                depth -= 1;
                if depth == 0 {
                    return Some(next_end + "END".len());
                }
                at = next_end + "END".len();
            }
        }
    }
}

/// Reads the keys out of the text between `ORDER BY` and the end of its clause.
///
/// Every key is returned, in the order written. The caller resolves each token
/// against whatever it sorts — a schema, or the labels of a fold.
pub fn parse(clause: &str) -> Result<Vec<SortTerm>, String> {
    let clause = clause.trim();
    if clause.is_empty() {
        return Err("ORDER BY is present but no field was provided".to_string());
    }

    let mut terms = Vec::new();
    for raw in split_terms(clause) {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("ORDER BY has an empty sort key".to_string());
        }
        let (token, rest) = if raw.to_ascii_uppercase().starts_with("CASE ") {
            match case_block_end(raw) {
                Some(end) => (&raw[..end], raw[end..].trim()),
                None => return Err("ORDER BY has a CASE with no END".to_string()),
            }
        } else {
            split_term(raw)
        };
        if token.is_empty() {
            return Err("ORDER BY has an empty sort key".to_string());
        }

        let mut descending = false;
        let mut nulls_first: Option<bool> = None;
        let mut direction_given = false;
        let mut words = rest.split_whitespace().peekable();
        if let Some(word) = words.peek() {
            if word.eq_ignore_ascii_case("DESC") {
                descending = true;
                direction_given = true;
                words.next();
            } else if word.eq_ignore_ascii_case("ASC") {
                direction_given = true;
                words.next();
            }
        }
        if let Some(word) = words.next() {
            if word.eq_ignore_ascii_case("NULLS") {
                match words.next() {
                    Some(w) if w.eq_ignore_ascii_case("FIRST") => nulls_first = Some(true),
                    Some(w) if w.eq_ignore_ascii_case("LAST") => nulls_first = Some(false),
                    _ => {
                        return Err(format!(
                            "ORDER BY '{}' has NULLS without FIRST or LAST",
                            token
                        ));
                    }
                }
            } else if direction_given {
                return Err(format!("unexpected '{}' after ORDER BY '{}'", word, token));
            } else {
                return Err(format!(
                    "ORDER BY '{}' expects ASC or DESC, found '{}'",
                    token, word
                ));
            }
        }
        if let Some(word) = words.next() {
            return Err(format!("unexpected '{}' after ORDER BY '{}'", word, token));
        }

        terms.push(SortTerm {
            token: token.trim_matches('"').to_string(),
            descending,
            nulls_first,
        });
    }

    Ok(terms)
}

/// Renders resolved keys into the `"col, col DESC"` spec the scan carries down
/// to the sorter.
///
/// The direction rides along inside the string so that no signature between the
/// SQL front end and the sort has to know a sort has directions at all — the
/// convention the single-key form already used, widened to a list.
pub fn render_spec(keys: &[(String, bool)]) -> String {
    keys.iter()
        .map(|(name, descending)| {
            if *descending {
                format!("{} DESC", name)
            } else {
                name.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reads back what [`render_spec`] wrote.
pub fn parse_spec(spec: &str) -> Vec<(String, bool)> {
    spec.split(',')
        .filter_map(|term| {
            let mut words = term.split_whitespace();
            let name = words.next()?;
            let descending = words.next().is_some_and(|d| d.eq_ignore_ascii_case("DESC"));
            Some((name.to_string(), descending))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(clause: &str) -> Vec<(String, bool)> {
        parse(clause)
            .unwrap()
            .into_iter()
            .map(|t| (t.token, t.descending))
            .collect()
    }

    #[test]
    fn a_single_key_defaults_to_ascending() {
        assert_eq!(keys("salary"), vec![("salary".to_string(), false)]);
        assert_eq!(keys("salary ASC"), vec![("salary".to_string(), false)]);
        assert_eq!(keys("salary DESC"), vec![("salary".to_string(), true)]);
    }

    /// The direction belongs to its own key: `ORDER BY a, b DESC` leaves `a`
    /// ascending.
    #[test]
    fn every_key_keeps_its_own_direction() {
        assert_eq!(
            keys("dept, salary DESC, name"),
            vec![
                ("dept".to_string(), false),
                ("salary".to_string(), true),
                ("name".to_string(), false),
            ]
        );
    }

    #[test]
    fn a_quoted_or_qualified_key_keeps_its_reference() {
        assert_eq!(keys("\"salary\" DESC"), vec![("salary".to_string(), true)]);
        assert_eq!(keys("u.salary"), vec![("u.salary".to_string(), false)]);
    }

    /// An aggregate call is one term even though it has parentheses.
    #[test]
    fn an_aggregate_call_survives_the_split() {
        assert_eq!(
            keys("COUNT(*) DESC, status"),
            vec![
                ("COUNT(*)".to_string(), true),
                ("status".to_string(), false)
            ]
        );
    }

    #[test]
    fn nulls_ordering_is_parsed_and_carried() {
        // The placement is carried on the term; consumers that cannot honour
        // it (the scan spec, joins, the aggregate fold) refuse it themselves —
        // that refusal is pinned where each of them resolves its terms.
        let t = parse("salary NULLS LAST").unwrap();
        assert_eq!(t[0].nulls_first, Some(false));
        assert!(!t[0].descending);
        let t = parse("salary DESC NULLS FIRST").unwrap();
        assert_eq!(t[0].nulls_first, Some(true));
        assert!(t[0].descending);
        assert!(
            parse("salary NULLS SIDEWAYS")
                .unwrap_err()
                .contains("FIRST or LAST")
        );
    }

    #[test]
    fn a_malformed_clause_is_reported() {
        assert!(parse("").unwrap_err().contains("no field"));
        assert!(parse("a, , b").unwrap_err().contains("empty sort key"));
        assert!(parse("salary UPWARDS").unwrap_err().contains("ASC or DESC"));
        assert!(
            parse("salary DESC junk")
                .unwrap_err()
                .contains("unexpected")
        );
    }

    /// A spec that names no key must read back as no keys.
    ///
    /// The scan decides whether it may stop early on `LIMIT` from whether the
    /// spec is present at all, so a present-but-empty one would leave early
    /// termination off and the sort with nothing to do — the safe pairing. The
    /// planner never produces one (an empty `ORDER BY` is a parse error), and
    /// this pins the behaviour if that ever changes.
    #[test]
    fn an_empty_spec_names_no_keys() {
        assert!(parse_spec("").is_empty());
        assert!(parse_spec("   ").is_empty());
        assert!(parse_spec(",").is_empty());
        assert!(render_spec(&[]).is_empty());
    }

    #[test]
    fn a_spec_round_trips() {
        let keys = vec![("dept".to_string(), false), ("salary".to_string(), true)];
        let spec = render_spec(&keys);
        assert_eq!(spec, "dept, salary DESC");
        assert_eq!(parse_spec(&spec), keys);
    }
}
