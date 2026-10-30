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
    /// For `NOT IN (SELECT …)`, where the `NOT` begins; `None` for a plain
    /// `IN` (and for a scalar subquery).
    pub negated_start: Option<usize>,
}

/// Whether `sql` has a `(` and holds `word` (uppercase) anywhere, ignoring
/// ASCII case: what each finder below needs before it can find anything, so
/// a `false` is exactly their `None`. Literals are not skipped, so a word in
/// one only costs the full scan it always had. Every statement passes all
/// three, and most hold neither.
fn may_hold(sql: &str, word: &[u8]) -> bool {
    let b = sql.as_bytes();
    memchr::memchr(b'(', b).is_some()
        && memchr::memchr2_iter(word[0], word[0].to_ascii_lowercase(), b)
            .any(|i| b.len() - i >= word.len() && b[i..i + word.len()].eq_ignore_ascii_case(word))
}

/// Finds the first `IN (SELECT …)` in a statement, ignoring string literals.
pub fn find_in_subquery(sql: &str) -> Option<InSubquery> {
    if !may_hold(sql, b"SELECT") {
        return None;
    }
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
                    let head = sql[..i].trim_end();
                    let negated_start = (head.len() >= 3
                        && head.as_bytes()[head.len() - 3..].eq_ignore_ascii_case(b"NOT")
                        && (head.len() == 3 || {
                            let b = head.as_bytes()[head.len() - 4];
                            !b.is_ascii_alphanumeric() && b != b'_' && b != b'"'
                        }))
                    .then(|| head.len() - 3);
                    return Some(InSubquery {
                        start: j,
                        end: m,
                        inner_sql: sql[j + 1..m - 1].trim().to_string(),
                        negated_start,
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
    scalar_subquery(sql, false)
}

/// The first scalar subquery that reads the outer row, which
/// [`find_scalar_subquery`] leaves in place: a statement with no per-row
/// evaluator for it must refuse it, or its text reaches a value parser.
pub fn find_correlated_scalar_subquery(sql: &str) -> Option<InSubquery> {
    scalar_subquery(sql, true)
}

fn scalar_subquery(sql: &str, correlated: bool) -> Option<InSubquery> {
    if !may_hold(sql, b"SELECT") {
        return None;
    }
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
                // `DELETE … USING (SELECT …) s` names a derived source the
                // same way; read as a value, a source of two rows was refused
                // as "returned 2 rows".
                let derived_table = matches!(previous_word.as_str(), "FROM" | "JOIN" | "USING")
                    && word_before_previous != "DISTINCT";
                // A parenthesised branch of a `UNION` is a query of its own,
                // not a value: `(SELECT … LIMIT 2) UNION (SELECT …)`. Taken for
                // one, a branch of two rows was refused as "returned 2 rows".
                // A branch opens the statement or follows `UNION [ALL]`.
                let union_branch = (previous_word.is_empty() && sql[..open].trim().is_empty())
                    || previous_word == "UNION"
                    || (previous_word == "ALL" && word_before_previous == "UNION");
                let is_select = upper[k..].starts_with("SELECT")
                    && !matches!(previous_word.as_str(), "IN" | "EXISTS")
                    && !derived_table
                    && !union_branch;

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
                    if is_correlated(&inner_sql) == correlated {
                        return Some(InSubquery {
                            start: open,
                            end: m,
                            inner_sql,
                            negated_start: None,
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
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
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
                if !qualifier.is_empty() && !own.iter().any(|n| n.eq_ignore_ascii_case(qualifier)) {
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
///
/// `document` says the column is a stored document, as for
/// [`rows_to_value_list`]: a scan reports one as text, and rendered as text
/// its encoded bytes were refused as a constant, or compared against `doc` as
/// a literal no document parses from.
pub fn rows_to_scalar(rows: &[Row], document: bool, exact_text: bool) -> Result<String, String> {
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
                return Err("a subquery used as a value must select exactly one column".to_string());
            }
            let column = &row.columns[0];
            let db_type = if document && column.column_type == DbType::STRING {
                DbType::JSONB
            } else {
                column.column_type.clone()
            };
            subquery_literal(&db_type, column.data.into_slice(), exact_text).ok_or_else(|| {
                format!(
                    "subquery returned a {} value that cannot be used as a constant",
                    db_type
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
    if !may_hold(sql, b"EXISTS") {
        return None;
    }
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
        let is_word_start =
            i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
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

/// What the trailing `LIMIT` / `OFFSET` of an `EXISTS` body changes about its
/// answer. `ORDER BY` changes nothing and is not recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExistsBounds {
    /// `LIMIT n` as written; `None` for no limit or `LIMIT ALL`.
    pub limit: Option<u64>,
    /// `OFFSET k`; 0 when absent.
    pub offset: u64,
}

/// Splits a top-level `ORDER BY`, `LIMIT`, `OFFSET` (and their SQL:2008
/// spellings) off the body of an `EXISTS`.
///
/// They have to go before the correlated rewrite looks at the `WHERE` clause.
/// Left in place, `EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.on LIMIT 1)` —
/// the shape Django's `Exists()` emits — became `o.b IN (SELECT a FROM t WHERE
/// t.on LIMIT 1)`, an `IN` list of ONE value, and every outer row matching any
/// other value was silently dropped. With the `LIMIT` straight after the
/// correlation the outer reference itself became `o.b LIMIT 1`.
///
/// What they mean for existence:
/// * `ORDER BY` never changes whether a row exists;
/// * `LIMIT n` with `n >= 1` never does either, and `LIMIT 0` makes it false;
/// * `OFFSET k` asks for at least `k + 1` rows, which the caller must honour.
///
/// The search is quote- and parenthesis-aware, so a `LIMIT` inside a nested
/// subquery or a string literal is left alone.
pub fn strip_exists_tail(clause: &ExistsClause) -> Result<(ExistsClause, ExistsBounds), String> {
    let normalized =
        crate::core::tokenizer::pagination::normalize_pagination(clause.inner_sql.trim())?;
    let mut body: &str = normalized.as_str();
    let mut bounds = ExistsBounds::default();

    // `normalize_pagination` leaves at most a canonical ` LIMIT n OFFSET m`
    // tail, in that order, so it is peeled from the end.
    if let Some((head, n)) = take_count_suffix(body, "OFFSET") {
        bounds.offset = n;
        body = head;
    }
    if let Some((head, n)) = take_count_suffix(body, "LIMIT") {
        bounds.limit = Some(n);
        body = head;
    }

    // `ORDER BY` is the last clause left once the pagination is gone.
    if let Some(at) = crate::core::rql::join::find_phrase(body, &["ORDER", "BY"], 0) {
        body = body[..at].trim_end();
    }

    Ok((
        ExistsClause {
            start: clause.start,
            end: clause.end,
            inner_sql: body.trim().to_string(),
            negated: clause.negated,
        },
        bounds,
    ))
}

/// `"… <KEYWORD> <digits>"` -> `("…", digits)`, matching the keyword as a
/// whole, case-insensitive word.
fn take_count_suffix<'a>(text: &'a str, keyword: &str) -> Option<(&'a str, u64)> {
    let digits_at = text.trim_end_matches(|c: char| c.is_ascii_digit()).len();
    if digits_at == text.len() {
        return None;
    }
    let count: u64 = text[digits_at..].parse().ok()?;
    let head = text[..digits_at].trim_end();
    if head.len() == text[..digits_at].len() || head.len() < keyword.len() {
        // No whitespace between the keyword and the count.
        return None;
    }
    let kw_at = head.len() - keyword.len();
    if !head.is_char_boundary(kw_at) || !head[kw_at..].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let before = &head[..kw_at];
    if !before.ends_with(|c: char| c.is_ascii_whitespace()) {
        return None;
    }
    Some((before.trim_end(), count))
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
/// `text` without the parentheses that wrap all of it: `((a = b))` is
/// `a = b`, `(a) = (b)` is left as it is.
fn unwrap_parens(text: &str) -> &str {
    let mut t = text.trim();
    while t.starts_with('(') && crate::core::sql_scan::matching_close(t, 0) == Some(t.len() - 1) {
        t = t[1..t.len() - 1].trim();
    }
    t
}

pub fn exists_to_in(clause: &ExistsClause, outer_relations: &[String]) -> Result<String, String> {
    const SELECT_LEN: usize = "SELECT".len();
    let inner = clause.inner_sql.trim();
    let inner_text = inner;

    if !inner
        .get(..SELECT_LEN + 1)
        .is_some_and(|h| {
            h.as_bytes()[..SELECT_LEN].eq_ignore_ascii_case(b"SELECT")
                && h.as_bytes()[SELECT_LEN].is_ascii_whitespace()
        })
    {
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
    // `GROUP BY` / `HAVING` are not predicates: handed to `split_conjuncts`
    // they were glued onto the last conjunct, where `o.b GROUP BY a` read as an
    // outer reference. Split off here, and refused below once the subquery is
    // known to correlate — the `IN` rewrite has nowhere to put them.
    let (where_clause, grouping) = split_grouping(where_clause);
    // Django parenthesises the whole predicate and each outer reference:
    // `WHERE (U0."tid" = ("t"."id") AND U0."w" < 150)`.
    let where_clause = unwrap_parens(where_clause);

    // Split the inner predicate into the one term that correlates with the
    // outer query and everything else, which stays inside the subquery.
    let mut correlations: Vec<(String, String)> = Vec::new();
    let mut rest: Vec<String> = Vec::new();

    for conjunct in crate::core::rql::join::split_conjuncts(where_clause) {
        let term = unwrap_parens(conjunct.trim());
        let Some(eq) = find_bare_equals(term) else {
            rest.push(term.to_string());
            continue;
        };
        let lhs = unwrap_parens(term[..eq].trim());
        let rhs = unwrap_parens(term[eq + 1..].trim());

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
        // An outer column the rewrite cannot place is no proof of an
        // uncorrelated EXISTS: run once with no outer row, it was refused as
        // 42P01 ("missing FROM-clause entry"), naming a fault the statement
        // does not have.
        if let Some(term) = rest.iter().find(|t| names_outer_column(t, &inner, outer_relations)) {
            return Err(format!(
                "a correlated EXISTS can compare an outer column only with `=`; '{}' is not supported",
                term
            ));
        }
        return Err(UNCORRELATED.to_string());
    }

    if !grouping.is_empty() {
        return Err(
            "a correlated EXISTS with GROUP BY or HAVING is not supported".to_string(),
        );
    }

    // `EXISTS (SELECT COUNT(*) FROM t WHERE t.a = o.b)` — an aggregate with no
    // GROUP BY yields exactly one row whatever matched, so the EXISTS holds for
    // every outer row. The `IN` rewrite would instead ask whether a matching
    // row exists: a plausible, wrong answer.
    let select_list = &inner_text[SELECT_LEN..from_at];
    if !crate::core::rql::lexer_query::find_aggregate_calls(select_list).is_empty()
        && crate::core::rql::join::find_keyword(select_list, "OVER", 0).is_none()
    {
        return Ok(uncorrelated_exists_predicate(clause.negated, true).to_string());
    }

    // An outer column anywhere but an equality — `e2.v > ev.v` — would be
    // left inside the subquery the rewrite runs once, where it names no row
    // of its own: the EXISTS matched nothing and reported success. Refused
    // until it can be answered.
    if let Some(term) = rest.iter().find(|t| names_outer_column(t, &inner, outer_relations)) {
        return Err(format!(
            "a correlated EXISTS can compare an outer column only with `=` so far; '{}' is not supported yet",
            term
        ));
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

    // `EXISTS` is two-valued and `IN` is not, and the difference is NULL. A
    // NULL join value matches nothing, so it is kept out of the list: in it, a
    // `NOT IN` can never hold, and `NOT EXISTS` would have lost every row. The
    // rest of the predicate is parenthesised so the extra term cannot bind to
    // one arm of an `OR`.
    let subquery = if rest.is_empty() {
        format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL",
            inner_column, relation_text, inner_column
        )
    } else {
        format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL AND ({})",
            inner_column,
            relation_text,
            inner_column,
            rest.join(" AND ")
        )
    };

    if clause.negated {
        // An outer NULL matches no inner row, so `NOT EXISTS` holds for it —
        // while `NULL NOT IN (…)` is unknown and drops the row.
        Ok(format!(
            "({} IS NULL OR {} NOT IN ({}))",
            outer_ref, outer_ref, subquery
        ))
    } else {
        Ok(format!("{} IN ({})", outer_ref, subquery))
    }
}

/// Splits `<predicate> GROUP BY … HAVING …` at its first top-level `GROUP BY`
/// or `HAVING`, returning `(predicate, grouping)`; `grouping` is empty when
/// there is neither.
fn split_grouping(where_clause: &str) -> (&str, &str) {
    let at = [
        crate::core::rql::join::find_phrase(where_clause, &["GROUP", "BY"], 0),
        crate::core::rql::join::find_keyword(where_clause, "HAVING", 0),
    ]
    .into_iter()
    .flatten()
    .min();
    match at {
        Some(at) => (where_clause[..at].trim_end(), where_clause[at..].trim()),
        None => (where_clause, ""),
    }
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
pub fn uncorrelated_exists(clause: &ExistsClause, outer_relations: &[String]) -> Option<String> {
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
        Some(w) => unwrap_parens(after_from[w + "WHERE".len()..].trim()),
    };

    let mut outer_refs: Vec<String> = Vec::new();
    let mut inner_refs: Vec<String> = Vec::new();
    let mut rest: Vec<String> = Vec::new();

    for conjunct in crate::core::rql::join::split_conjuncts(where_clause) {
        let term = unwrap_parens(conjunct.trim());
        let Some(eq) = find_bare_equals(term) else {
            rest.push(term.to_string());
            continue;
        };
        let lhs = unwrap_parens(term[..eq].trim());
        let rhs = unwrap_parens(term[eq + 1..].trim());
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
    exact_text: &[bool],
) -> Result<String, String> {
    let mut groups: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        if row.columns.len() != spec.outer_refs.len() {
            return Err(format!(
                "the EXISTS subquery returned {} columns but correlates on {}",
                row.columns.len(),
                spec.outer_refs.len()
            ));
        }
        // An inner row with a NULL in a correlated column equals no outer row,
        // so it can neither satisfy `EXISTS` nor defeat `NOT EXISTS`. Rendered,
        // it became `a.x = NULL`, which the predicate parser refused.
        if row
            .columns
            .iter()
            .any(|c| c.column_type == DbType::NULL)
        {
            continue;
        }
        let mut terms: Vec<String> = Vec::with_capacity(row.columns.len());
        for (at, (outer, column)) in spec.outer_refs.iter().zip(row.columns.iter()).enumerate() {
            let exact = exact_text.get(at).copied().unwrap_or(true);
            let literal = subquery_literal(&column.column_type, column.data.into_slice(), exact)
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

    if groups.is_empty() {
        // Nothing to match: `EXISTS` is false for every row, `NOT EXISTS` true.
        return Ok(if spec.negated { "1 = 1" } else { "1 = 0" }.to_string());
    }

    let joiner = if spec.negated { " AND " } else { " OR " };
    if spec.negated {
        // An outer row with a NULL in any correlated column matches no inner
        // row, so `NOT EXISTS` holds for it; `a.x != 1` alone would be unknown
        // for it and drop it.
        let nulls: Vec<String> = spec
            .outer_refs
            .iter()
            .map(|outer| format!("{} IS NULL", outer))
            .collect();
        return Ok(format!(
            "({} OR ({}))",
            nulls.join(" OR "),
            groups.join(joiner)
        ));
    }
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
        // An aliased relation is named by its alias alone, as SQL scopes it:
        // in `FROM sv s2 WHERE s2.id = sv.id` the `sv` is the outer query's.
        // Counted as the subquery's own, that correlation read as `s2.id =
        // s2.id`, the EXISTS as uncorrelated, and it held for every row.
        if let Some(table) = tokens.next() {
            let name = tokens.next().unwrap_or(table);
            names.push(name.trim_matches('"').to_string());
        }
    }
    names
}

/// Whether `term` reads a column qualified by one of the outer relations, a
/// name the subquery does not give its own relations. Quoted text is skipped.
fn names_outer_column(term: &str, inner: &[String], outer_relations: &[String]) -> bool {
    let bytes = term.as_bytes();
    let is_name = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'"' || !b.is_ascii();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                i += 1;
            }
            i += 1;
            continue;
        }
        if is_name(bytes[i]) {
            let start = i;
            while i < bytes.len() && (is_name(bytes[i]) || bytes[i] == b'.') {
                i += 1;
            }
            if matches!(qualifier_of(&term[start..i], inner, outer_relations), Side::Outer)
                && term[start..i].contains('.')
            {
                return true;
            }
            continue;
        }
        i += 1;
    }
    false
}

/// Whether a cast to `target` (one `::type`) is one the evaluator applies
/// exactly as PostgreSQL does: a type with no modifier, NUMERIC / DECIMAL
/// with `(p, s)` (rounded, 22003 past the precision), or a text or time type
/// with the modifier `expr::CastTypmod` applies.
fn placeable_cast(target: &str) -> bool {
    let t = target.trim().to_ascii_lowercase();
    if t.is_empty()
        || !t.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'"'))
    {
        return false;
    }
    let base = t.split('(').next().unwrap_or("").trim().trim_matches('"');
    // Every modifier the evaluator applies exactly (`expr::CastTypmod`):
    // `varchar(n)` and `character(n)` cut (and `character(n)` compares
    // without trailing spaces, as its literals are read), and `timestamp(p)`,
    // `time(p)`, `timetz(p)` and `interval(p)` round half away from zero.
    !t.contains('(')
        || matches!(
            base,
            "numeric"
                | "decimal"
                | "varchar"
                | "character varying"
                | "char varying"
                | "char"
                | "character"
                | "bpchar"
                | "nchar"
                | "timestamp"
                | "timestamptz"
                | "time"
                | "timetz"
                | "interval"
        )
}

fn qualifier_of(reference: &str, inner: &[String], outer_relations: &[String]) -> Side {
    // A cast of the reference (`t.id::bigint`, `u.y::text`, `x::numeric(10,
    // 2)`) keeps its side: ORMs write them, and the `IN` rewrite carries the
    // cast with the column. Only a type may follow `::`.
    //
    // Not a cast whose value this engine does not compute as PostgreSQL does
    // (`placeable_cast`): those keep no side, and the EXISTS is refused
    // (0A000) rather than answered wrong. The modifiers of `varchar(n)`,
    // `character(n)`, `timestamp(p)`, `time(p)`, `timetz(p)` and
    // `interval(p)` are applied (`expr::CastTypmod`), so they place.
    let cleaned = match reference.trim().split_once("::") {
        Some((column, cast)) if cast.split("::").all(placeable_cast) => column.trim(),
        _ => reference.trim(),
    };
    // Only a column reference has a side: `t.x + 1` is an expression over
    // one, which the `IN` rewrite cannot place (it came out as `t.x + 1 IN
    // (…)`, a syntax error), so it is no correlation.
    let mut segments: Vec<&str> = Vec::new();
    let bytes = cleaned.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        if bytes[i] == b'"' {
            i = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            segments.push(cleaned[start..i].trim_matches('"'));
        } else {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'$') || bytes[i] >= 0x80) {
                i += 1;
            }
            if i == start {
                return Side::Unknown;
            }
            segments.push(&cleaned[start..i]);
        }
        match bytes.get(i) {
            None => break,
            Some(b'.') => i += 1,
            Some(_) => return Side::Unknown,
        }
    }
    let qualifier = match segments.len() {
        // An unqualified name inside the subquery belongs to the subquery's
        // own relation, which is how SQL resolves it.
        0 | 1 => return Side::Inner,
        // `q.col`, or `schema.q.col` (Prisma's `"public"."t"."id"`): the
        // relation is the segment before the column.
        n => segments[n - 2],
    };

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
        DbType::STRING | DbType::BPCHAR(_) => value_codec::quote_sql(std::str::from_utf8(bytes).ok()?),
        DbType::UUID => value_codec::quote_sql(&value_codec::format_uuid(bytes)),
        DbType::INET => value_codec::quote_sql(&value_codec::format_inet(bytes)?),
        DbType::DATETIME => {
            value_codec::quote_sql(&value_codec::format_timestamp_us(i64::from_le_bytes(le::<
                8,
            >(
                bytes,
            )?)))
        }
        // A result value may carry its own display scale (an `AVG`); see
        // `value_codec::decimal_cell_with_scale`.
        DbType::DECIMAL => match value_codec::split_decimal_cell(bytes)? {
            (v, Some(own)) => value_codec::format_decimal_scaled(v, own),
            (v, None) => value_codec::format_decimal(v),
        },
        DbType::BOOL => if bytes.first().copied().unwrap_or(0) != 0 {
            "1"
        } else {
            "0"
        }
        .to_string(),
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
                // Rendered, not decoded: a decoded `Value` holds a fraction
                // as a `f64`, so `1.000000000000000000001` came back as `1.0`,
                // and a number past the `f64` range had no literal at all.
                let mut text = Vec::with_capacity(bytes.len());
                crate::core::jsonb::render_text_into(bytes, &mut text).ok()?;
                value_codec::quote_sql(std::str::from_utf8(&text).ok()?)
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
        DbType::INTERVAL => value_codec::quote_sql(&crate::core::pg_types::format_interval(bytes)?),
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
///
/// Duplicates are rendered once: membership does not depend on multiplicity,
/// and every value in the list becomes one comparison per scanned outer row.
///
/// A NULL is left out of the list and reported as the second element instead.
/// It equals nothing, so for `IN` dropping it is exact in a filter; for
/// `NOT IN` it makes the predicate never true, which the caller expresses —
/// rendered as a literal it reached the predicate parser as an unknown
/// identifier.
///
/// `document` says the column is a stored document. A scan reports one as
/// text, and rendered as text its encoded bytes made a literal no document
/// parses from, so `doc IN (SELECT doc FROM t)` was refused outright. It is
/// rendered as the document it is, and duplicates are found by value.
/// A subquery's value as the literal it is substituted as. A text value
/// with trailing spaces from a text column (`exact_text`) is written as the
/// text expression `('…' || '')`: compared with a `character(n)` column
/// PostgreSQL compares a subquery's text value as text, exactly — in `IN
/// (SELECT …)` and `= (SELECT …)` alike — where an untyped `'ab  '` is
/// `'ab'`. An expression keeps that through the rewrite, which reads an `IN`
/// list's typed literals as the column's type, as a written list is. One
/// from a `character(n)` or `character varying` column compares
/// blank-padded and is written untyped. Without trailing spaces the two
/// readings agree. The exact `('…' || '')` shape is what the rewrite knows
/// a substituted value by, and keeps exact inside an `IN` list over
/// `character(n)` (`predicate_rewrite::spell_out_list_or_range`), where a
/// computed item written in the statement is read blank-padded.
pub fn subquery_literal(db_type: &DbType, bytes: &[u8], exact_text: bool) -> Option<String> {
    let literal = value_to_literal(db_type, bytes)?;
    Some(if exact_text && *db_type == DbType::STRING && bytes.last() == Some(&b' ') {
        format!("({literal} || '')")
    } else {
        literal
    })
}

pub fn rows_to_value_list(rows: &[Row], document: bool, exact_text: bool) -> Result<(String, bool), String> {
    let mut out = String::with_capacity(2 + rows.len() * 8);
    out.push('(');
    let mut had_null = false;
    let mut first = true;
    // Keyed by type tag and stored bytes, so equal values of different types
    // (a UNION can mix them) are never merged.
    let mut seen: ahash::HashSet<(u8, std::borrow::Cow<[u8]>)> =
        ahash::HashSet::with_capacity_and_hasher(rows.len(), Default::default());

    for row in rows {
        let column = row
            .columns
            .first()
            .ok_or_else(|| "subquery returned a row with no columns".to_string())?;
        if row.columns.len() > 1 {
            return Err("a subquery used with IN must select exactly one column".to_string());
        }
        if column.column_type == DbType::NULL {
            had_null = true;
            continue;
        }
        let bytes = column.data.into_slice();
        let db_type = if document && column.column_type == DbType::STRING {
            DbType::JSONB
        } else {
            column.column_type.clone()
        };
        let key = if db_type == DbType::JSONB {
            let mut key = Vec::with_capacity(bytes.len() + 8);
            crate::core::jsonb::push_group_key(bytes, &mut key);
            std::borrow::Cow::Owned(key)
        } else {
            std::borrow::Cow::Borrowed(bytes)
        };
        if !seen.insert((db_type.to_byte(), key)) {
            continue;
        }
        let literal = subquery_literal(&db_type, bytes, exact_text).ok_or_else(|| {
            format!(
                "subquery returned a {} value that cannot be compared",
                db_type
            )
        })?;
        if !first {
            out.push_str(", ");
        }
        first = false;
        out.push_str(&literal);
    }

    out.push(')');
    Ok((out, had_null))
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
        assert!(!is_correlated(
            "SELECT COUNT(*) FROM t bl2 WHERE bl2.x = bl2.y"
        ));
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

    /// `DELETE … USING (SELECT …) s` names a derived source, as `FROM (…)`
    /// does: read as a value, a source of two rows failed the statement
    /// with "returned 2 rows". A scalar subquery in its condition is still
    /// one.
    ///
    /// Mutation: `"USING"` dropped from the derived-table words.
    #[test]
    fn a_using_source_is_not_a_scalar_subquery() {
        assert!(find_scalar_subquery("DELETE FROM t USING (SELECT other FROM u) p WHERE p.other = t.id").is_none());
        let found = find_scalar_subquery(
            "DELETE FROM t USING (SELECT other FROM u) p WHERE p.other = t.id AND t.v = (SELECT max(w) FROM u)",
        )
        .expect("the value subquery is still found");
        assert!(found.inner_sql.contains("max(w)"), "{}", found.inner_sql);
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
        let rewritten = exists_to_in(&clause, &["domain".to_string(), "d".to_string()])
            .expect("should rewrite");
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
            Column {
                schema_id: crate::core::row::row::schema_id_of(schema_id),
                data: mb,
                column_type: DbType::I32,
            }
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
        let predicate = multi_exists_predicate(&spec, &[row(1, 1)], &[]).unwrap();
        // De Morgan: "differs in at least one column", per inner row.
        assert_eq!(
            predicate,
            "(a.x IS NULL OR a.y IS NULL OR ((a.x != 1 OR a.y != 1)))"
        );

        // The pair (1,1) must be excluded as a *pair*. A per-column rewrite
        // would also exclude (1,2) and (2,1), which is the bug this replaces.
        let two = multi_exists_predicate(&spec, &[row(1, 1), row(2, 2)], &[]).unwrap();
        assert_eq!(
            two,
            "(a.x IS NULL OR a.y IS NULL OR ((a.x != 1 OR a.y != 1) AND (a.x != 2 OR a.y != 2)))"
        );

        let positive = MultiCorrelatedExists {
            negated: false,
            ..spec
        };
        assert_eq!(
            multi_exists_predicate(&positive, &[row(1, 1)], &[]).unwrap(),
            "((a.x = 1 AND a.y = 1))"
        );

        // No inner rows: NOT EXISTS holds for everything, EXISTS for nothing.
        assert_eq!(multi_exists_predicate(&positive, &[], &[]).unwrap(), "1 = 0");
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

    fn tail(sql: &str) -> (String, ExistsBounds) {
        let clause = find_exists(sql).expect("an EXISTS");
        let (stripped, bounds) = strip_exists_tail(&clause).expect("strips");
        (stripped.inner_sql, bounds)
    }

    #[test]
    fn the_tail_of_an_exists_body_is_lifted_off() {
        let (body, b) = tail("SELECT 1 FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.on LIMIT 1)");
        assert_eq!(body, "SELECT 1 FROM t WHERE t.a = o.b AND t.on");
        assert_eq!(b, ExistsBounds { limit: Some(1), offset: 0 });

        let (body, b) = tail("SELECT 1 FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b\n ORDER  BY t.id DESC\nlimit 5 offset 2)");
        assert_eq!(body, "SELECT 1 FROM t WHERE t.a = o.b");
        assert_eq!(b, ExistsBounds { limit: Some(5), offset: 2 });

        let (_, b) = tail("SELECT 1 FROM o WHERE EXISTS (SELECT 1 FROM t FETCH FIRST ROW ONLY)");
        assert_eq!(b, ExistsBounds { limit: Some(1), offset: 0 });
        let (_, b) = tail("SELECT 1 FROM o WHERE EXISTS (SELECT 1 FROM t LIMIT ALL)");
        assert_eq!(b, ExistsBounds { limit: None, offset: 0 });
        let (_, b) = tail("SELECT 1 FROM o WHERE NOT EXISTS (SELECT 1 FROM t LIMIT 0)");
        assert_eq!(b.limit, Some(0));

        // Nested and quoted keywords are not the body's own.
        let (body, b) = tail(
            "SELECT 1 FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.c IN (SELECT c FROM u ORDER BY c LIMIT 3) AND t.n = 'LIMIT 9')",
        );
        assert!(body.ends_with("AND t.n = 'LIMIT 9'"), "{body}");
        assert_eq!(b, ExistsBounds::default());
    }

    #[test]
    fn a_correlated_exists_with_a_limit_rewrites_to_a_full_in() {
        let sql = "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b AND t.on = 1 LIMIT 1)";
        let (clause, _) = strip_exists_tail(&find_exists(sql).unwrap()).unwrap();
        let rewritten = exists_to_in(&clause, &["o".to_string()]).unwrap();
        assert_eq!(rewritten, "o.b IN (SELECT a FROM t WHERE a IS NOT NULL AND (t.on = 1))");
        assert!(!rewritten.contains("LIMIT"));
    }

    #[test]
    fn a_correlated_exists_over_an_ungrouped_aggregate_always_holds() {
        let clause = find_exists("SELECT id FROM o WHERE EXISTS (SELECT COUNT(*) FROM t WHERE t.a = o.b)").unwrap();
        assert_eq!(exists_to_in(&clause, &["o".to_string()]).unwrap(), "1 = 1");
        let clause = find_exists("SELECT id FROM o WHERE NOT EXISTS (SELECT max(t.x) FROM t WHERE t.a = o.b)").unwrap();
        assert_eq!(exists_to_in(&clause, &["o".to_string()]).unwrap(), "1 = 0");
        // A window function is not an aggregate: one row per match.
        let clause = find_exists("SELECT id FROM o WHERE EXISTS (SELECT count(*) OVER () FROM t WHERE t.a = o.b)").unwrap();
        assert!(exists_to_in(&clause, &["o".to_string()]).unwrap().contains(" IN ("));
        // GROUP BY in a correlated body is refused rather than mangled.
        let clause = find_exists("SELECT id FROM o WHERE EXISTS (SELECT 1 FROM t WHERE t.a = o.b GROUP BY a)").unwrap();
        assert!(exists_to_in(&clause, &["o".to_string()]).is_err());
    }

    #[test]
    fn not_in_records_where_its_not_begins() {
        let sql = "SELECT a FROM t WHERE x NOT  IN (SELECT y FROM u)";
        let found = find_in_subquery(sql).unwrap();
        assert_eq!(&sql[found.negated_start.unwrap()..found.start], "NOT  IN ");
        assert!(find_in_subquery("SELECT a FROM t WHERE x IN (SELECT y FROM u)").unwrap().negated_start.is_none());
        assert!(find_in_subquery("SELECT a FROM t WHERE knot IN (SELECT y FROM u)").unwrap().negated_start.is_none());
    }

    #[test]
    fn empty_results_render_as_an_empty_list() {
        assert_eq!(rows_to_value_list(&[], false, true).unwrap(), ("()".to_string(), false));
    }

    /// The cheap guard must never turn a finding into `None`: lower case,
    /// mixed case and a keyword only after the parenthesis all still match.
    #[test]
    fn the_guard_keeps_every_subquery_the_scan_finds() {
        assert!(find_in_subquery("select a from t where b in (select c from u)").is_some());
        assert!(find_in_subquery("SELECT a FROM t WHERE b IN (SeLeCt c FROM u)").is_some());
        assert!(find_scalar_subquery("update t set a = (select max(b) from u)").is_some());
        assert!(find_exists("select 1 from t where exists (select 1 from u)").is_some());
        assert!(find_exists("SELECT 1 FROM t WHERE NOT EXISTS (SELECT 1 FROM u)").is_some());
        // And it says no exactly where the scan could not say yes.
        for sql in ["UPDATE acc SET balance = 70 WHERE id = 10", "SELECT selection FROM t", "INSERT INTO t VALUES (1)"] {
            assert!(find_in_subquery(sql).is_none() && find_scalar_subquery(sql).is_none() && find_exists(sql).is_none());
        }
    }
}
