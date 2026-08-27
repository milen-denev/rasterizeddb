//! Uncorrelated `IN (SELECT …)` subqueries.
//!
//! The inner query is run first and its results are substituted back into the
//! outer statement as a literal list, which then follows the ordinary
//! `IN`-desugaring path in [`predicate_rewrite`]. That reuses the whole
//! existing pipeline — including filter pruning on the resulting equalities —
//! instead of introducing a second execution model.
//!
//! Only **uncorrelated** subqueries work this way: the inner query must not
//! reference the outer one, because it is executed exactly once with no outer
//! row in scope. A correlated subquery is refused rather than run per row.
//!
//! [`predicate_rewrite`]: crate::core::tokenizer::predicate_rewrite

use crate::core::db_type::DbType;
use crate::core::row::row::Row;
use crate::core::value_codec;

/// The span of one `IN (SELECT …)` and the inner statement it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InSubquery {
    /// Byte range of the parenthesised list, inclusive of both parentheses.
    pub start: usize,
    pub end: usize,
    pub inner_sql: String,
}

/// Finds the first `IN (SELECT …)` in a statement, ignoring string literals.
pub fn find_in_subquery(sql: &str) -> Option<InSubquery> {
    let bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let mut i = 0usize;

    while i < bytes.len() {
        // Skip over string literals so an `IN (SELECT` inside one is not
        // mistaken for syntax.
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // ` IN ` as a whole word.
        if upper.as_bytes()[i..].starts_with(b"IN")
            && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_')
            && i + 2 < bytes.len()
            && !bytes[i + 2].is_ascii_alphanumeric()
            && bytes[i + 2] != b'_'
        {
            // Find the opening parenthesis that follows.
            let mut j = i + 2;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                let mut k = j + 1;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if upper[k..].starts_with("SELECT") {
                    // Match the closing parenthesis.
                    let mut depth = 1usize;
                    let mut m = j + 1;
                    while m < bytes.len() && depth > 0 {
                        match bytes[m] {
                            b'\'' => {
                                m += 1;
                                while m < bytes.len() && bytes[m] != b'\'' {
                                    m += 1;
                                }
                            }
                            b'(' => depth += 1,
                            b')' => depth -= 1,
                            _ => {}
                        }
                        m += 1;
                    }
                    if depth != 0 {
                        return None;
                    }
                    return Some(InSubquery {
                        start: j,
                        end: m,
                        inner_sql: sql[j + 1..m - 1].trim().to_string(),
                    });
                }
            }
        }

        i += 1;
    }

    None
}

/// A parenthesised `SELECT` standing where a value is expected, in the select
/// list.
///
/// It must return exactly one row of one column, which then behaves as a
/// constant — the same shape a literal select item takes.
pub fn find_scalar_subquery(sql: &str) -> Option<InSubquery> {
    let bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();

    // Any statement may contain one: `UPDATE t SET c = (SELECT …)` and
    // `DELETE … WHERE c = (SELECT …)` as much as a SELECT. Restricting this to
    // statements *beginning* with SELECT meant an UPDATE reached its parser
    // with the word SELECT inside an assignment, and was reported as
    // "Cannot parse UPDATE query" — naming neither the clause nor the reason.

    // The whole statement is scanned, not just the select list. A parenthesised
    // SELECT in a `WHERE` — `WHERE user_id = (SELECT id FROM t)` — is a scalar
    // subquery too, and stopping at the top-level `FROM` meant those reached the
    // predicate parser as the literal word `SELECT`.
    //
    // The constructs with their own handlers are skipped by looking at the word
    // in front of the parenthesis: `IN (…)` and `EXISTS (…)` are rewritten
    // elsewhere, and `FROM (…)`/`JOIN (…)` are derived tables, not values.
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut previous_word = String::new();
    let mut word_before_previous = String::new();

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    i += 1;
                }
                // Step *past* the closing quote. Without this the closing quote
                // was read as an opening one and everything up to the next
                // quote was swallowed — so once an earlier subquery had been
                // substituted with a quoted literal, the rest of the statement
                // was never scanned and its subqueries survived.
                i += 1;
            }
            b'(' => {
                let open = i;
                let mut k = i + 1;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                // `FROM (` is a derived table — except in `IS DISTINCT FROM
                // (SELECT …)`, where FROM ends the operator and the parenthesis
                // is a value. Without that distinction the second subquery of
                // `… SET c = (SELECT …) WHERE x IS DISTINCT FROM (SELECT …)`
                // was skipped and reached the parser as the word SELECT.
                let derived_table = matches!(previous_word.as_str(), "FROM" | "JOIN")
                    && word_before_previous != "DISTINCT";
                let is_select = upper[k..].starts_with("SELECT")
                    && !matches!(previous_word.as_str(), "IN" | "EXISTS")
                    && !derived_table;

                let mut d = 1usize;
                let mut m = i + 1;
                while m < bytes.len() && d > 0 {
                    match bytes[m] {
                        b'\'' => {
                            m += 1;
                            while m < bytes.len() && bytes[m] != b'\'' {
                                m += 1;
                            }
                        }
                        b'(' => d += 1,
                        b')' => d -= 1,
                        _ => {}
                    }
                    m += 1;
                }
                if d != 0 {
                    return None;
                }

                if is_select {
                    let inner_sql = sql[open + 1..m - 1].trim().to_string();
                    // A correlated subquery is not a constant: its value
                    // depends on the outer row, so it must be evaluated per
                    // row and is left in place for the caller that can do
                    // that.
                    //
                    // Substituting one here does not fail loudly, which is why
                    // this check exists. The inner parser strips qualifiers, so
                    // `WHERE b.x = a.y` became `WHERE x = y` — a comparison
                    // between two of the *inner* table's own columns. That
                    // returns a plausible number for the wrong question, and
                    // the statement then reports success.
                    if !is_correlated(&inner_sql) {
                        return Some(InSubquery {
                            start: open,
                            end: m,
                            inner_sql,
                        });
                    }
                    i = open + 1;
                    continue;
                }
                // Descend into the group rather than stepping over it: a
                // subquery can sit inside ordinary parentheses, as in
                // `AND (x IS DISTINCT FROM (SELECT …))`. Skipping to `m` jumped
                // the whole group and left the inner SELECT unresolved.
                i = open + 1;
                continue;
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                word_before_previous = std::mem::take(&mut previous_word);
                previous_word = upper[start..i].to_string();
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            _ => {
                // Punctuation such as `=` separates without naming anything, so
                // the word in front of the parenthesis stays what it was.
                i += 1;
            }
        }
        let _ = &mut depth;
    }

    None
}

/// True when `inner_sql` reads a column belonging to some relation it does not
/// itself name — that is, a column of the outer query's row.
///
/// Deliberately conservative: an unqualified column is not evidence either way,
/// so only a qualifier that matches none of the subquery's own relations counts.
pub fn is_correlated(inner_sql: &str) -> bool {
    let from_at = match crate::core::rql::join::find_keyword(inner_sql, "FROM", 0) {
        Some(at) => at,
        // No FROM means no relations of its own, so nothing to correlate with.
        None => return false,
    };
    let after_from = &inner_sql[from_at + "FROM".len()..];
    let end = ["WHERE", "GROUP", "ORDER", "LIMIT", "HAVING"]
        .iter()
        .filter_map(|k| crate::core::rql::join::find_keyword(after_from, k, 0))
        .min()
        .unwrap_or(after_from.len());
    let own = inner_names(after_from[..end].trim());

    let bytes = inner_sql.as_bytes();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            if b == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            quote = Some(b);
            i += 1;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || bytes[i] == b'_'
                    || bytes[i] == b'"'
                    || bytes[i] == b'.')
            {
                i += 1;
            }
            let word = &inner_sql[start..i];
            if let Some(dot) = word.find('.') {
                let qualifier = word[..dot].trim_matches('"');
                if !qualifier.is_empty()
                    && !own.iter().any(|n| n.eq_ignore_ascii_case(qualifier))
                {
                    return true;
                }
            }
            continue;
        }
        i += 1;
    }
    false
}

/// Renders the single value a scalar subquery returned.
///
/// A subquery in value position must yield at most one row of one column;
/// Postgres raises an error otherwise rather than picking one arbitrarily.
pub fn rows_to_scalar(rows: &[Row]) -> Result<String, String> {
    match rows.len() {
        // No rows makes the expression NULL, and NULL is what it becomes.
        // It used to be an empty string, which is a *value*: `WHERE id = ''`
        // against a UUID column is a type error, so a claim query whose
        // subquery simply found nothing failed with "literal cannot be
        // compared against a UUID column" instead of matching no rows.
        0 => Ok("NULL".to_string()),
        1 => {
            let row = &rows[0];
            if row.columns.len() != 1 {
                return Err(
                    "a subquery used as a value must select exactly one column".to_string()
                );
            }
            let column = &row.columns[0];
            value_to_literal(&column.column_type, column.data.into_slice()).ok_or_else(|| {
                format!(
                    "subquery returned a {} value that cannot be used as a constant",
                    column.column_type
                )
            })
        }
        n => Err(format!(
            "a subquery used as a value returned {} rows, but must return at most one",
            n
        )),
    }
}

/// An `EXISTS` clause found in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistsClause {
    /// Byte range covering `[NOT] EXISTS (…)`.
    pub start: usize,
    pub end: usize,
    pub inner_sql: String,
    pub negated: bool,
}

/// Finds the first `[NOT] EXISTS (SELECT …)`, ignoring string literals.
pub fn find_exists(sql: &str) -> Option<ExistsClause> {
    let bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        const KW: usize = "EXISTS".len();
        let is_word_start = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
        if is_word_start
            && i + KW <= bytes.len()
            && upper.as_bytes()[i..].starts_with(b"EXISTS")
            && (i + KW == bytes.len() || !bytes[i + KW].is_ascii_alphanumeric())
        {
            // A preceding NOT belongs to the clause.
            let head = upper[..i].trim_end();
            let negated = head.ends_with("NOT")
                && (head.len() == 3 || !head.as_bytes()[head.len() - 4].is_ascii_alphanumeric());
            let start = if negated { head.len() - 3 } else { i };

            let mut j = i + KW;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] != b'(' {
                i += 1;
                continue;
            }

            let mut depth = 1usize;
            let mut m = j + 1;
            while m < bytes.len() && depth > 0 {
                match bytes[m] {
                    b'\'' => {
                        m += 1;
                        while m < bytes.len() && bytes[m] != b'\'' {
                            m += 1;
                        }
                    }
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                m += 1;
            }
            if depth != 0 {
                return None;
            }

            return Some(ExistsClause {
                start,
                end: m,
                inner_sql: sql[j + 1..m - 1].trim().to_string(),
                negated,
            });
        }

        i += 1;
    }

    None
}

/// Rewrites a correlated `EXISTS` into the equivalent `IN` over the inner
/// relation's join column.
///
/// `EXISTS (SELECT 1 FROM t WHERE t.x = o.y AND <rest>)` asks whether any row
/// of `t` satisfying `<rest>` has `t.x` equal to this row's `o.y` — which is
/// exactly `o.y IN (SELECT t.x FROM t WHERE <rest>)`. Expressing it that way
/// means the existing uncorrelated-`IN` path runs it once rather than once per
/// outer row, and the filter index sees the resulting equalities.
///
/// Returns the replacement text, or an error naming what could not be handled.
pub fn exists_to_in(clause: &ExistsClause, outer_relations: &[String]) -> Result<String, String> {
    let inner = clause.inner_sql.trim();
    let upper = inner.to_ascii_uppercase();

    if !upper.starts_with("SELECT ") {
        return Err("EXISTS must contain a SELECT".to_string());
    }
    let from_at = crate::core::rql::join::find_keyword(inner, "FROM", 0)
        .ok_or_else(|| "the SELECT inside EXISTS has no FROM clause".to_string())?;

    let after_from = &inner[from_at + "FROM".len()..];
    let where_at = crate::core::rql::join::find_keyword(after_from, "WHERE", 0);

    let relation_text = match where_at {
        Some(w) => after_from[..w].trim(),
        None => after_from.trim(),
    };
    // A join inside EXISTS is carried through rather than refused: the whole
    // `FROM` becomes the rewritten subquery's `FROM`, and the join path runs it
    // like any other. Only the *set of inner names* had to grow — the rewrite
    // itself never depended on there being exactly one relation.
    let joined = relation_text.to_ascii_uppercase().contains(" JOIN ");
    let inner = inner_names(relation_text);
    if inner.is_empty() {
        return Err("EXISTS names no relation".to_string());
    }

    let where_clause = match where_at {
        None => return Err(UNCORRELATED.to_string()),
        Some(w) => after_from[w + "WHERE".len()..].trim(),
    };

    // Split the inner predicate into the one term that correlates with the
    // outer query and everything else, which stays inside the subquery.
    let mut correlations: Vec<(String, String)> = Vec::new();
    let mut rest: Vec<String> = Vec::new();

    for conjunct in crate::core::rql::join::split_conjuncts(where_clause) {
        let term = conjunct.trim();
        let Some(eq) = find_bare_equals(term) else {
            rest.push(term.to_string());
            continue;
        };
        let lhs = term[..eq].trim();
        let rhs = term[eq + 1..].trim();

        let lhs_side = qualifier_of(lhs, &inner, outer_relations);
        let rhs_side = qualifier_of(rhs, &inner, outer_relations);

        let found = match (lhs_side, rhs_side) {
            (Side::Inner, Side::Outer) => Some((rhs.to_string(), lhs.to_string())),
            (Side::Outer, Side::Inner) => Some((lhs.to_string(), rhs.to_string())),
            _ => {
                rest.push(term.to_string());
                None
            }
        };

        if let Some(pair) = found {
            correlations.push(pair);
        }
    }

    if correlations.is_empty() {
        return Err(UNCORRELATED.to_string());
    }

    // More than one correlation cannot be expressed as `outer IN (SELECT inner)`,
    // which carries exactly one column — and `x IN (SELECT a) AND y IN (SELECT b)`
    // is a different, weaker question, since different inner rows may satisfy
    // each side. Those are handled by `multi_correlated_exists`, which
    // materialises the inner rows and expands them into a predicate instead.
    if correlations.len() > 1 {
        return Err(MULTI_CORRELATION.to_string());
    }
    let (outer_ref, inner_ref) = correlations.into_iter().next().expect("checked non-empty");

    // With a join the qualifier has to stay: `domain_id` alone would be
    // ambiguous across the joined relations. Without one it is stripped, which
    // is what the single-relation path has always emitted.
    let inner_column = if joined {
        inner_ref.trim_matches('"').to_string()
    } else {
        inner_ref
            .rsplit('.')
            .next()
            .unwrap_or(&inner_ref)
            .trim_matches('"')
            .to_string()
    };

    let subquery = if rest.is_empty() {
        format!("SELECT {} FROM {}", inner_column, relation_text)
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            inner_column,
            relation_text,
            rest.join(" AND ")
        )
    };

    Ok(format!(
        "{} {}IN ({})",
        outer_ref,
        if clause.negated { "NOT " } else { "" },
        subquery
    ))
}

enum Side {
    Inner,
    Outer,
    Unknown,
}

/// Marker for an `EXISTS` that references no outer column, so the caller can run
/// it once and substitute a constant instead of reporting a dead end.
pub(crate) const UNCORRELATED: &str = "EXISTS references no outer column";

/// The query to run for an `EXISTS` that correlates nothing, or `None` when it
/// does correlate.
///
/// An uncorrelated `EXISTS` asks one question about the database, not one per
/// outer row: "did this return anything". PostgreSQL treats it exactly so, and
/// `WHERE NOT EXISTS (SELECT 1 FROM t)` — insert only if nothing matched — is a
/// common seeding idiom.
pub fn uncorrelated_exists(
    clause: &ExistsClause,
    outer_relations: &[String],
) -> Option<String> {
    match exists_to_in(clause, outer_relations) {
        Err(e) if e == UNCORRELATED => Some(clause.inner_sql.trim().to_string()),
        _ => None,
    }
}

/// The predicate an uncorrelated `EXISTS` becomes once its query has run.
pub fn uncorrelated_exists_predicate(negated: bool, any_rows: bool) -> &'static str {
    // `EXISTS` holds when the query returned something; `NOT EXISTS` when it did
    // not. Either way the answer is the same for every outer row.
    let holds = if negated { !any_rows } else { any_rows };
    if holds { "1 = 1" } else { "1 = 0" }
}

/// Marker for the one case `exists_to_in` cannot express, so the caller can tell
/// it apart from a genuine refusal and take the materialising path instead.
pub(crate) const MULTI_CORRELATION: &str = "EXISTS correlates on more than one column";

/// An `EXISTS` correlating on several columns.
///
/// `outer_refs[i]` pairs with column `i` of `inner_sql`'s result.
#[derive(Debug)]
pub struct MultiCorrelatedExists {
    pub inner_sql: String,
    pub outer_refs: Vec<String>,
    pub negated: bool,
}

/// Describes an `EXISTS` that correlates on more than one column, or `None` when
/// it correlates on one (which `exists_to_in` handles as an `IN`).
pub fn multi_correlated_exists(
    clause: &ExistsClause,
    outer_relations: &[String],
) -> Option<Result<MultiCorrelatedExists, String>> {
    match exists_to_in(clause, outer_relations) {
        Ok(_) => None,
        Err(e) if e == MULTI_CORRELATION => Some(exists_parts(clause, outer_relations)),
        // Any other refusal stays a refusal; the caller reports it.
        Err(_) => None,
    }
}

/// Re-parses an `EXISTS` to project every correlated inner column.
fn exists_parts(
    clause: &ExistsClause,
    outer_relations: &[String],
) -> Result<MultiCorrelatedExists, String> {
    let inner = clause.inner_sql.trim();
    let from_at = crate::core::rql::join::find_keyword(inner, "FROM", 0)
        .ok_or_else(|| "the SELECT inside EXISTS has no FROM clause".to_string())?;
    let after_from = &inner[from_at + "FROM".len()..];
    let where_at = crate::core::rql::join::find_keyword(after_from, "WHERE", 0);
    let relation_text = match where_at {
        Some(w) => after_from[..w].trim(),
        None => after_from.trim(),
    };
    let names = inner_names(relation_text);
    let where_clause = match where_at {
        None => return Err("an EXISTS with no WHERE clause correlates nothing".to_string()),
        Some(w) => after_from[w + "WHERE".len()..].trim(),
    };

    let mut outer_refs: Vec<String> = Vec::new();
    let mut inner_refs: Vec<String> = Vec::new();
    let mut rest: Vec<String> = Vec::new();

    for conjunct in crate::core::rql::join::split_conjuncts(where_clause) {
        let term = conjunct.trim();
        let Some(eq) = find_bare_equals(term) else {
            rest.push(term.to_string());
            continue;
        };
        let lhs = term[..eq].trim();
        let rhs = term[eq + 1..].trim();
        match (
            qualifier_of(lhs, &names, outer_relations),
            qualifier_of(rhs, &names, outer_relations),
        ) {
            (Side::Inner, Side::Outer) => {
                outer_refs.push(rhs.to_string());
                inner_refs.push(lhs.to_string());
            }
            (Side::Outer, Side::Inner) => {
                outer_refs.push(lhs.to_string());
                inner_refs.push(rhs.to_string());
            }
            _ => rest.push(term.to_string()),
        }
    }

    if outer_refs.len() < 2 {
        return Err("EXISTS does not correlate on more than one column".to_string());
    }

    let projection = inner_refs.join(", ");
    let inner_sql = if rest.is_empty() {
        format!("SELECT {} FROM {}", projection, relation_text)
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            projection,
            relation_text,
            rest.join(" AND ")
        )
    };

    Ok(MultiCorrelatedExists {
        inner_sql,
        outer_refs,
        negated: clause.negated,
    })
}

/// Expands a multi-column `EXISTS` into a predicate over the rows its subquery
/// produced.
///
/// `EXISTS` becomes a disjunction of full tuple matches, `NOT EXISTS` its De
/// Morgan dual — a conjunction of "differs in at least one column". Both are
/// exact: the tuples are compared as tuples, so no pair is matched that does not
/// exist, which is precisely what a per-column `IN` gets wrong.
///
/// The inner query runs **once**, like the `IN` path, rather than once per outer
/// row.
pub fn multi_exists_predicate(
    spec: &MultiCorrelatedExists,
    rows: &[Row],
) -> Result<String, String> {
    if rows.is_empty() {
        // Nothing to match: `EXISTS` is false for every row, `NOT EXISTS` true.
        return Ok(if spec.negated { "1 = 1" } else { "1 = 0" }.to_string());
    }

    let mut groups: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        if row.columns.len() != spec.outer_refs.len() {
            return Err(format!(
                "the EXISTS subquery returned {} columns but correlates on {}",
                row.columns.len(),
                spec.outer_refs.len()
            ));
        }
        let mut terms: Vec<String> = Vec::with_capacity(row.columns.len());
        for (outer, column) in spec.outer_refs.iter().zip(row.columns.iter()) {
            let literal = value_to_literal(&column.column_type, column.data.into_slice())
                .ok_or_else(|| {
                    format!(
                        "the EXISTS subquery returned a {} value that cannot be compared",
                        column.column_type
                    )
                })?;
            terms.push(format!(
                "{} {} {}",
                outer,
                if spec.negated { "!=" } else { "=" },
                literal
            ));
        }
        let joiner = if spec.negated { " OR " } else { " AND " };
        groups.push(format!("({})", terms.join(joiner)));
    }

    let joiner = if spec.negated { " AND " } else { " OR " };
    Ok(format!("({})", groups.join(joiner)))
}

/// Every table name and alias the `FROM` clause inside an `EXISTS` introduces.
///
/// A joined `EXISTS` has several. Without the full set, a term qualified by the
/// second relation reads as `Unknown` rather than `Inner`, and the correlation
/// is never found.
fn inner_names(relation_text: &str) -> Vec<String> {
    let mut pieces: Vec<&str> = Vec::new();
    let mut rest = relation_text;
    loop {
        let upper = rest.to_ascii_uppercase();
        match upper.find(" JOIN ") {
            Some(at) => {
                pieces.push(&rest[..at]);
                rest = &rest[at + " JOIN ".len()..];
            }
            None => {
                pieces.push(rest);
                break;
            }
        }
    }

    let mut names: Vec<String> = Vec::new();
    for piece in pieces {
        // An `ON` clause belongs to the join, not to the relation it follows.
        let upper = piece.to_ascii_uppercase();
        let body = match upper.find(" ON ") {
            Some(at) => &piece[..at],
            None => piece,
        };
        let mut tokens = body.split_whitespace().filter(|t| {
            !matches!(
                t.to_ascii_uppercase().as_str(),
                "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS" | "FULL" | "AS"
            )
        });
        if let Some(table) = tokens.next() {
            names.push(table.trim_matches('"').to_string());
            if let Some(alias) = tokens.next() {
                names.push(alias.trim_matches('"').to_string());
            }
        }
    }
    names
}

fn qualifier_of(reference: &str, inner: &[String], outer_relations: &[String]) -> Side {
    let cleaned = reference.trim().trim_matches('"');
    let Some((qualifier, _)) = cleaned.rsplit_once('.') else {
        // An unqualified name inside the subquery belongs to the subquery's
        // own relation, which is how SQL resolves it.
        return Side::Inner;
    };
    let qualifier = qualifier.trim_matches('"');

    if inner.iter().any(|n| n.eq_ignore_ascii_case(qualifier)) {
        Side::Inner
    } else if outer_relations
        .iter()
        .any(|r| r.eq_ignore_ascii_case(qualifier))
    {
        Side::Outer
    } else {
        Side::Unknown
    }
}

/// Position of a top-level `=` that is not part of `<=`, `>=` or `!=`.
fn find_bare_equals(term: &str) -> Option<usize> {
    let bytes = term.as_bytes();
    let mut depth = 0usize;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'=' if depth == 0 => {
                let prev = if i == 0 { b' ' } else { bytes[i - 1] };
                if matches!(prev, b'<' | b'>' | b'!' | b'=') {
                    return None;
                }
                return Some(i);
            }
            _ => {}
        }
    }
    None
}

/// Renders one column value as the SQL literal for its type.
pub fn value_to_literal(db_type: &DbType, bytes: &[u8]) -> Option<String> {
    fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
        if b.len() != N {
            return None;
        }
        let mut a = [0u8; N];
        a.copy_from_slice(b);
        Some(a)
    }

    Some(match db_type {
        DbType::STRING => value_codec::quote_sql(std::str::from_utf8(bytes).ok()?),
        DbType::UUID => value_codec::quote_sql(&value_codec::format_uuid(bytes)),
        DbType::INET => value_codec::quote_sql(&value_codec::format_inet(bytes)?),
        DbType::DATETIME => {
            value_codec::quote_sql(&value_codec::format_timestamp_us(i64::from_le_bytes(le::<8>(bytes)?)))
        }
        DbType::DECIMAL => value_codec::format_decimal(i128::from_le_bytes(le::<16>(bytes)?)),
        DbType::BOOL => if bytes.first().copied().unwrap_or(0) != 0 { "1" } else { "0" }.to_string(),
        DbType::I8 => i8::from_le_bytes(le::<1>(bytes)?).to_string(),
        DbType::I16 => i16::from_le_bytes(le::<2>(bytes)?).to_string(),
        DbType::I32 => i32::from_le_bytes(le::<4>(bytes)?).to_string(),
        DbType::I64 => i64::from_le_bytes(le::<8>(bytes)?).to_string(),
        DbType::I128 => i128::from_le_bytes(le::<16>(bytes)?).to_string(),
        DbType::U8 => u8::from_le_bytes(le::<1>(bytes)?).to_string(),
        DbType::U16 => u16::from_le_bytes(le::<2>(bytes)?).to_string(),
        DbType::U32 => u32::from_le_bytes(le::<4>(bytes)?).to_string(),
        DbType::U64 => u64::from_le_bytes(le::<8>(bytes)?).to_string(),
        DbType::U128 => u128::from_le_bytes(le::<16>(bytes)?).to_string(),
        DbType::F32 => f32::from_le_bytes(le::<4>(bytes)?).to_string(),
        DbType::F64 => f64::from_le_bytes(le::<8>(bytes)?).to_string(),
        DbType::CHAR => {
            let code = u32::from_le_bytes(le::<4>(bytes)?);
            value_codec::quote_sql(&char::from_u32(code)?.to_string())
        }
        // A document that has already been rebuilt — the bytes ARE the JSON
        // text, so it needs quoting and nothing else. Without this arm a
        // shredded JSON column had no literal form and materialising any
        // derived table that selected one failed the whole statement.
        DbType::JSON => value_codec::quote_sql(std::str::from_utf8(bytes).ok()?),
        // A stored document, written back as the JSON text a `JSONB` column
        // takes. Materialising a subquery carries every column of its select
        // list through a real table — a paginated count is written that way —
        // so a document with no literal form failed the whole statement rather
        // than just its own column.
        DbType::JSONB => {
            if bytes.is_empty() {
                "NULL".to_string()
            } else {
                value_codec::quote_sql(&crate::core::jsonb::decode(bytes).ok()?.to_string())
            }
        }
        // The types stored in PostgreSQL's own wire layout. Their literal form
        // is their text form quoted — which is exactly what a client would have
        // written to put the value there, so the round trip is closed.
        //
        // Reaching here matters for more than subqueries: `value_to_literal` is
        // also what renders a `::text` cast, and without these arms
        // `hardware_status::text` failed with "BIT cannot be rendered as text"
        // rather than yielding the bits.
        DbType::INTERVAL => {
            value_codec::quote_sql(&crate::core::pg_types::format_interval(bytes)?)
        }
        DbType::MACADDR | DbType::MACADDR8 => {
            value_codec::quote_sql(&crate::core::pg_types::format_macaddr(bytes))
        }
        DbType::BIT | DbType::VARBIT => {
            value_codec::quote_sql(&crate::core::pg_types::format_bit_string(bytes)?)
        }
        DbType::DATE => value_codec::quote_sql(&crate::core::pg_types::format_date(bytes)?),
        DbType::TIME => value_codec::quote_sql(&crate::core::pg_types::format_time(bytes)?),
        DbType::TIMETZ => value_codec::quote_sql(&crate::core::pg_types::format_timetz(bytes)?),
        DbType::POINT => value_codec::quote_sql(&crate::core::pg_types::format_point(bytes)?),
        DbType::LINE => value_codec::quote_sql(&crate::core::pg_types::format_line(bytes)?),
        DbType::LSEG => value_codec::quote_sql(&crate::core::pg_types::format_lseg(bytes)?),
        DbType::BOX => value_codec::quote_sql(&crate::core::pg_types::format_box(bytes)?),
        DbType::PATH => value_codec::quote_sql(&crate::core::pg_types::format_path(bytes)?),
        DbType::POLYGON => value_codec::quote_sql(&crate::core::pg_types::format_polygon(bytes)?),
        DbType::CIRCLE => value_codec::quote_sql(&crate::core::pg_types::format_circle(bytes)?),
        DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => {
            value_codec::quote_sql(&crate::core::pg_types::format_range(db_type, bytes)?)
        }
        // Variable-width types whose literal is their own text syntax.
        DbType::BYTEA => value_codec::quote_sql(&value_codec::format_bytea(bytes)),
        DbType::ARRAY => value_codec::quote_sql(&value_codec::format_array(bytes)?),
        // An empty value is how a NULL arrives for a variable-width column.
        // Anything else of an unknown type has no faithful literal, and
        // inventing one would store a value nobody wrote.
        DbType::NULL => "NULL".to_string(),
        _ => return None,
    })
}

/// Turns the rows a subquery returned into the parenthesised list that replaces
/// it.
///
/// An empty result yields `()`. The `IN` desugaring reads that as a predicate
/// that cannot hold, which is what `x IN (empty set)` means — rather than a
/// syntax error, which is what an empty list would be if a user had written it.
pub fn rows_to_value_list(rows: &[Row]) -> Result<String, String> {
    let mut parts: Vec<String> = Vec::with_capacity(rows.len());

    for row in rows {
        let column = row
            .columns
            .first()
            .ok_or_else(|| "subquery returned a row with no columns".to_string())?;
        if row.columns.len() > 1 {
            return Err("a subquery used with IN must select exactly one column".to_string());
        }
        let literal = value_to_literal(&column.column_type, column.data.into_slice())
            .ok_or_else(|| {
                format!(
                    "subquery returned a {} value that cannot be compared",
                    column.column_type
                )
            })?;
        parts.push(literal);
    }

    Ok(format!("({})", parts.join(", ")))
}

#[cfg(test)]
mod correlation_tests {
    use super::*;

    #[test]
    fn a_subquery_reading_an_outer_column_is_correlated() {
        assert!(is_correlated(
            "SELECT COUNT(*) FROM user_table_column bl2 \
             WHERE bl2.related_table_id = fwd.user_table_id"
        ));
    }

    #[test]
    fn a_self_contained_subquery_is_not_correlated() {
        assert!(!is_correlated("SELECT COUNT(*) FROM t"));
        assert!(!is_correlated("SELECT COUNT(*) FROM t WHERE t.x = 1"));
        assert!(!is_correlated("SELECT COUNT(*) FROM t bl2 WHERE bl2.x = bl2.y"));
        // Unqualified columns say nothing either way, so they are not evidence.
        assert!(!is_correlated("SELECT COUNT(*) FROM t WHERE x = 1"));
    }

    #[test]
    fn a_join_inside_the_subquery_names_both_of_its_own_relations() {
        assert!(!is_correlated(
            "SELECT COUNT(*) FROM a JOIN b ON a.id = b.aid WHERE b.x = 1"
        ));
    }

    #[test]
    fn a_correlated_scalar_subquery_is_not_offered_for_substitution() {
        // This is the bug the check exists for. The inner parser strips
        // qualifiers, so substituting `bl2.rel = fwd.ut` evaluated
        // `rel = ut` against the inner table — two of its own columns — and
        // returned a count for a question nobody asked, reporting success.
        let sql = "UPDATE c fwd SET p = bl.id FROM c bl WHERE bl.x = fwd.y \
                   AND (SELECT COUNT(*) FROM c bl2 WHERE bl2.r = fwd.u) = 1";
        assert!(
            find_scalar_subquery(sql).is_none(),
            "a correlated subquery must be left in place for per-row evaluation"
        );
    }

    #[test]
    fn an_uncorrelated_scalar_subquery_is_still_found() {
        // The guard must not be so broad that it stops the ordinary case.
        let sql = "UPDATE c SET p = 1 WHERE c.x = (SELECT MAX(id) FROM other)";
        let found = find_scalar_subquery(sql).expect("still substitutable");
        assert!(found.inner_sql.contains("MAX(id)"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_join_inside_exists_is_carried_through() {
        let clause = find_exists(
            "SELECT d.id FROM domain d WHERE NOT EXISTS (SELECT 1 FROM user_roles ur \
             JOIN role_ids r ON ur.role_id = r.id WHERE r.domain_id = d.id)",
        )
        .expect("should find the EXISTS");
        let rewritten =
            exists_to_in(&clause, &["domain".to_string(), "d".to_string()]).expect("should rewrite");
        // The whole FROM survives, and the projected column keeps its qualifier
        // — `domain_id` alone would be ambiguous across the joined relations.
        assert!(rewritten.contains("JOIN role_ids r"), "{}", rewritten);
        assert!(rewritten.contains("r.domain_id"), "{}", rewritten);
        assert!(rewritten.contains("NOT IN"), "{}", rewritten);
    }

    #[test]
    fn correlating_on_two_columns_is_routed_to_the_materialising_path() {
        // `EXISTS (… WHERE b.x = a.x AND b.y = a.y)` cannot become a
        // single-column IN. Every correlation but the last used to be dropped
        // silently, so this answered a narrower question and returned too few
        // rows with no error.
        let clause = find_exists(
            "SELECT a.x FROM ti_a a WHERE NOT EXISTS (SELECT 1 FROM ti_b b \
             WHERE b.x = a.x AND b.y = a.y)",
        )
        .expect("should find the EXISTS");
        // `exists_to_in` still declines it — but with the marker that routes it
        // to `multi_correlated_exists` rather than reporting a dead end.
        let err = exists_to_in(&clause, &["ti_a".to_string(), "a".to_string()])
            .expect_err("the IN rewrite cannot express two correlations");
        assert_eq!(err, MULTI_CORRELATION);

        let spec = multi_correlated_exists(&clause, &["ti_a".to_string(), "a".to_string()])
            .expect("should route to the materialising path")
            .expect("should parse");
        assert!(spec.negated);
        assert_eq!(spec.outer_refs, vec!["a.x".to_string(), "a.y".to_string()]);
        // Both correlated columns are projected, in the order of the outer refs.
        assert!(spec.inner_sql.contains("b.x, b.y"), "{}", spec.inner_sql);
    }

    #[test]
    fn multi_exists_expands_tuple_wise_not_column_wise() {
        use crate::core::db_type::DbType;
        use crate::core::row::row::Column;
        use crate::memory_pool::MEMORY_POOL;

        fn i32_column(schema_id: u64, v: i32) -> Column {
            let bytes = v.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            Column { schema_id, data: mb, column_type: DbType::I32 }
        }
        fn row(x: i32, y: i32) -> Row {
            Row {
                position: 0,
                length: 0,
                columns: smallvec::smallvec![i32_column(0, x), i32_column(1, y)],
            }
        }

        let spec = MultiCorrelatedExists {
            inner_sql: "SELECT b.x, b.y FROM ti_b b".to_string(),
            outer_refs: vec!["a.x".to_string(), "a.y".to_string()],
            negated: true,
        };
        let predicate = multi_exists_predicate(&spec, &[row(1, 1)]).unwrap();
        // De Morgan: "differs in at least one column", per inner row.
        assert_eq!(predicate, "((a.x != 1 OR a.y != 1))");

        // The pair (1,1) must be excluded as a *pair*. A per-column rewrite
        // would also exclude (1,2) and (2,1), which is the bug this replaces.
        let two = multi_exists_predicate(&spec, &[row(1, 1), row(2, 2)]).unwrap();
        assert_eq!(two, "((a.x != 1 OR a.y != 1) AND (a.x != 2 OR a.y != 2))");

        let positive = MultiCorrelatedExists { negated: false, ..spec };
        assert_eq!(
            multi_exists_predicate(&positive, &[row(1, 1)]).unwrap(),
            "((a.x = 1 AND a.y = 1))"
        );

        // No inner rows: NOT EXISTS holds for everything, EXISTS for nothing.
        assert_eq!(multi_exists_predicate(&positive, &[]).unwrap(), "1 = 0");
    }

    #[test]
    fn finds_a_subquery_and_its_span() {
        let sql = "SELECT name FROM users WHERE domain_id IN (SELECT id FROM domain) ORDER BY name";
        let found = find_in_subquery(sql).expect("should find the subquery");
        assert_eq!(found.inner_sql, "SELECT id FROM domain");
        assert_eq!(&sql[found.start..found.end], "(SELECT id FROM domain)");
    }

    #[test]
    fn matches_the_right_closing_parenthesis_when_nested() {
        let sql = "SELECT a FROM t WHERE x IN (SELECT y FROM u WHERE (z = 1 OR z = 2)) AND w = 3";
        let found = find_in_subquery(sql).expect("should find the subquery");
        assert_eq!(found.inner_sql, "SELECT y FROM u WHERE (z = 1 OR z = 2)");
        assert!(sql[found.end..].starts_with(" AND w = 3"));
    }

    #[test]
    fn a_plain_value_list_is_not_a_subquery() {
        assert!(find_in_subquery("SELECT a FROM t WHERE x IN (1, 2, 3)").is_none());
        assert!(find_in_subquery("SELECT a FROM t WHERE x = 1").is_none());
    }

    /// `IN (SELECT` inside a string literal is data, not syntax.
    #[test]
    fn ignores_matches_inside_string_literals() {
        let sql = "SELECT a FROM t WHERE note = 'x IN (SELECT y FROM u)'";
        assert!(find_in_subquery(sql).is_none());
    }

    #[test]
    fn does_not_match_in_as_part_of_a_longer_word() {
        assert!(find_in_subquery("SELECT a FROM t WHERE domain (SELECT 1)").is_none());
        assert!(find_in_subquery("SELECT a FROM t WHERE min (SELECT 1)").is_none());
    }

    #[test]
    fn unbalanced_parentheses_are_declined() {
        assert!(find_in_subquery("SELECT a FROM t WHERE x IN (SELECT y FROM u").is_none());
    }

    #[test]
    fn empty_results_render_as_an_empty_list() {
        assert_eq!(rows_to_value_list(&[]).unwrap(), "()");
    }
}
