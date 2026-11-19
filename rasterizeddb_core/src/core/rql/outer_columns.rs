//! An outer column a subquery reads unqualified, qualified by the relation
//! it belongs to: `EXISTS (SELECT 1 FROM ord WHERE customer_id = id)` over
//! `FROM cu` is `… customer_id = cu.id`.
//!
//! The passes that run a correlated subquery (`subquery::exists_to_in`,
//! `correlated::rewrite_select`) find its correlation by qualifier, so an
//! unqualified one was read as the inner relation's column and refused:
//! "unknown identifier 'id' is not defined in the schema". PostgreSQL scopes
//! a name to the nearest query level whose relations have a column of that
//! name, the subquery's own first; this does the same with the schema.
//!
//! Only where the answer is certain. A query level whose columns are not all
//! known (a CTE, a function in `FROM`, `*` in a derived table, a relation
//! that does not exist) or a set operation is left as it is written, and so
//! is a name two relations of one level have (PostgreSQL's 42702), a name in
//! a keyword's place (`EXTRACT(year FROM …)`, `NULLS first`) and anything
//! qualified, called or quoted. What is left behind fails as it did.

use crate::core::rql::join::find_keyword;
use crate::core::sql_scan::{is_identifier_byte, matching_close, skip_comment, skip_quoted, split_top_level};

/// A relation of one query level: the qualifier its columns take, as
/// written (`o`, `ord`, `"Ord"`), and its columns, lowercase.
struct Relation {
    qualifier: String,
    columns: Vec<String>,
}

/// One query level's relations, and whether they are all of them.
struct Scope {
    relations: Vec<Relation>,
    complete: bool,
    /// The level's own output names, which its `ORDER BY` may read.
    own: Vec<String>,
}

impl Scope {
    fn has(&self, name: &str) -> bool {
        self.own.iter().any(|c| c == name) || self.relations.iter().any(|r| r.columns.iter().any(|c| c == name))
    }
}

/// The clauses that end a `FROM` list.
const AFTER_FROM: &[&str] = &["WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"];

/// Words never read as a column here: SQL's keywords, and the words that
/// stand in a keyword's place inside a call (`EXTRACT(year …)`, `TRIM(both
/// …)`, `NULLS first`). A column so named stays unqualified.
const KEYWORDS: &[&str] = &[
    "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AT", "BETWEEN", "BIGINT", "BOOLEAN", "BOTH", "BY",
    "BYTEA", "CASE", "CAST", "CENTURY", "CHAR", "CHARACTER", "COLLATE", "CROSS", "CURRENT", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATE", "DAY", "DECADE", "DECIMAL", "DEFAULT", "DESC",
    "DISTINCT", "DOUBLE", "DOW", "DOY", "ELSE", "END", "EPOCH", "ESCAPE", "EXCEPT", "EXCLUDE", "EXISTS", "FALSE",
    "FETCH", "FILTER", "FIRST", "FLOAT", "FOLLOWING", "FOR", "FROM", "FULL", "GROUP", "GROUPS", "HAVING", "HOUR",
    "ILIKE", "IN", "INNER", "INT", "INTEGER", "INTERSECT", "INTERVAL", "IS", "ISNULL", "JOIN", "JSON", "JSONB",
    "LAST", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "LOCKED", "MICROSECONDS",
    "MILLENNIUM", "MILLISECONDS", "MINUTE", "MONTH", "NATURAL", "NEXT", "NO", "NOT", "NOTNULL", "NOWAIT", "NULL",
    "NULLS", "NUMERIC", "OF", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION",
    "PRECEDING", "PRECISION", "QUARTER", "RANGE", "REAL", "RETURNING", "RIGHT", "ROW", "ROWS", "SECOND",
    "SELECT", "SESSION_USER", "SET", "SHARE", "SIMILAR", "SKIP", "SMALLINT", "SOME", "SYMMETRIC", "TEXT", "THEN",
    "TIES", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TO", "TRAILING", "TRUE", "UNBOUNDED", "UNION", "UNKNOWN",
    "UPDATE", "USER", "USING", "UUID", "VALUES", "VARCHAR", "VARYING", "WEEK", "WHEN", "WHERE", "WINDOW", "WITH",
    "WITHIN", "WITHOUT", "YEAR", "ZONE",
];

/// `sql` with each unqualified outer column a subquery reads qualified;
/// `None` when there is none (or the statement holds no subquery).
/// `columns_of` gives a table's column names, `None` for no such table.
pub(crate) fn qualify(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    let head = sql.trim_start();
    let nests = if starts_word(head, "SELECT") {
        crate::core::rql::subquery::may_nest_select(sql)
    } else {
        sql.contains('(') && crate::core::sql_scan::contains_ignore_ascii_case(sql, b"select")
    };
    if !nests {
        return None;
    }
    let lead = sql.len() - head.len();
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut ctx = Ctx { sql, columns_of, edits: &mut edits };
    if starts_word(head, "SELECT") {
        ctx.query(lead, sql.len(), &[]);
    } else if starts_word(head, "UPDATE") || starts_word(head, "DELETE") {
        ctx.write_statement(lead);
    } else {
        return None;
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(sql.len() + edits.len() * 8);
    let mut at = 0;
    for (start, end, text) in edits {
        out.push_str(&sql[at..start]);
        out.push_str(&text);
        at = end;
    }
    out.push_str(&sql[at..]);
    Some(out)
}

/// [`qualify`] over `database`'s tables.
pub(crate) fn qualify_in(database: &crate::core::database::Database, sql: &str) -> Option<String> {
    qualify(sql, &|name: &str| {
        database
            .schema_view(name)
            .map(|view| view.schema().fields.iter().map(|f| f.name.clone()).collect())
    })
}

fn starts_word(text: &str, word: &str) -> bool {
    let t = text.trim_start();
    t.len() >= word.len()
        && t.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes())
        && t.as_bytes().get(word.len()).is_none_or(|b| !is_identifier_byte(*b))
}

/// An identifier as PostgreSQL folds it: a quoted one as written, a bare one
/// lowercase.
fn folded(name: &str) -> String {
    let name = name.trim();
    match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => name.to_ascii_lowercase(),
    }
}

/// The names a select list gives its columns, `None` when one is not known
/// (`*`, `t.*`, an expression PostgreSQL would name by a cast).
fn output_names(list: &str) -> Option<Vec<String>> {
    let mut list = list.trim();
    for word in ["DISTINCT", "ALL"] {
        if starts_word(list, word) {
            list = list[word.len()..].trim_start();
            if word == "DISTINCT" && starts_word(list, "ON") {
                let open = list.find('(')?;
                list = list[matching_close(list, open)? + 1..].trim_start();
            }
        }
    }
    let mut names = Vec::new();
    for item in split_top_level(list, b',') {
        let item = item.trim();
        if item == "*" || item.ends_with(".*") {
            return None;
        }
        // `expr AS name`, or `expr name` after a closing parenthesis or a
        // column: the last top-level `AS`, or a trailing bare word.
        let mut alias_at = None;
        let mut from = 0;
        while let Some(p) = find_keyword(item, "AS", from) {
            alias_at = Some(p);
            from = p + 2;
        }
        if let Some(p) = alias_at {
            names.push(folded(&item[p + 2..]));
            continue;
        }
        let chain = item.split('.').all(|seg| {
            let seg = seg.trim();
            !seg.is_empty()
                && (seg.starts_with('"') && seg.ends_with('"') && seg.len() >= 2
                    || seg.bytes().all(is_identifier_byte) && !seg.as_bytes()[0].is_ascii_digit())
        });
        if chain {
            names.push(folded(item.rsplit('.').next().unwrap_or(item)));
            continue;
        }
        if item.contains("::") {
            return None;
        }
        // `count(*)` is named `count`, a CASE `case`, anything else
        // `?column?`.
        let word: String = item.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
        let rest = item[word.len()..].trim_start();
        names.push(if !word.is_empty() && rest.starts_with('(') {
            word.to_ascii_lowercase()
        } else if word.eq_ignore_ascii_case("CASE") {
            "case".to_string()
        } else {
            "?column?".to_string()
        });
    }
    Some(names)
}

struct Ctx<'a, 'e> {
    sql: &'a str,
    columns_of: &'a dyn Fn(&str) -> Option<Vec<String>>,
    edits: &'e mut Vec<(usize, usize, String)>,
}

impl Ctx<'_, '_> {
    /// The SELECT at `sql[start..end]` (from its `SELECT`), whose enclosing
    /// levels are `outers`, nearest last.
    fn query(&mut self, start: usize, end: usize, outers: &[&Scope]) {
        let body = &self.sql[start..end];
        if ["UNION", "INTERSECT", "EXCEPT"].iter().any(|k| find_keyword(body, k, 0).is_some()) {
            return;
        }
        let select_list_at = start + body.len() - body.trim_start().len() + "SELECT".len();
        let Some(from) = find_keyword(body, "FROM", 0) else {
            let own = output_names(&self.sql[select_list_at..end]).unwrap_or_default();
            let scope = Scope { relations: Vec::new(), complete: true, own };
            self.walk(select_list_at, end, &scope, outers);
            return;
        };
        let from_end = AFTER_FROM.iter().filter_map(|kw| find_keyword(body, kw, from + 4)).min().unwrap_or(body.len());
        let mut scope = self.from_scope(start + from + 4, start + from_end, outers);
        scope.own = output_names(&self.sql[select_list_at..start + from]).unwrap_or_default();
        self.walk(select_list_at, start + from, &scope, outers);
        self.walk(start + from_end, end, &scope, outers);
    }

    /// `UPDATE t … SET … [FROM …] WHERE …`, `DELETE FROM t [USING …] WHERE
    /// …`: the target and the other relations are the level the subqueries
    /// in `SET`, `WHERE` and `RETURNING` correlate with.
    fn write_statement(&mut self, start: usize) {
        let sql = self.sql;
        let body = &sql[start..];
        let (target_at, target_end, list_kw, rest_at) = if starts_word(body, "UPDATE") {
            let Some(set) = find_keyword(body, "SET", 0) else { return };
            (start + 6, start + set, "FROM", start + set)
        } else {
            let Some(from) = find_keyword(body, "FROM", 0) else { return };
            let end = ["USING", "WHERE", "RETURNING"]
                .iter()
                .filter_map(|kw| find_keyword(body, kw, from + 4))
                .min()
                .unwrap_or(body.len());
            (start + from + 4, start + end, "USING", start + end)
        };
        let mut scope = self.from_scope(target_at, target_end, &[]);
        let rest = &sql[rest_at..];
        if let Some(list) = find_keyword(rest, list_kw, 0) {
            let list_end = ["WHERE", "RETURNING"]
                .iter()
                .filter_map(|kw| find_keyword(rest, kw, list + list_kw.len()))
                .min()
                .unwrap_or(rest.len());
            let more = self.from_scope(rest_at + list + list_kw.len(), rest_at + list_end, &[]);
            scope.complete &= more.complete;
            scope.relations.extend(more.relations);
        }
        self.walk(rest_at, sql.len(), &scope, &[]);
    }

    /// The relations of the `FROM` list at `sql[start..end]`. A derived
    /// table's own subqueries are resolved against `outers`, the levels
    /// around the one this list belongs to.
    fn from_scope(&mut self, start: usize, end: usize, outers: &[&Scope]) -> Scope {
        let sql = self.sql;
        let mut scope = Scope { relations: Vec::new(), complete: true, own: Vec::new() };
        let list = &sql[start..end];
        for piece in split_top_level(list, b',') {
            let piece_at = start + (piece.as_ptr() as usize - list.as_ptr() as usize);
            // Each relation of `a JOIN b ON … JOIN c USING (…)`.
            let mut at = 0;
            loop {
                let next = find_keyword(piece, "JOIN", at);
                let chunk_end = next.unwrap_or(piece.len());
                let mut chunk = &piece[at..chunk_end];
                for kw in ["ON", "USING"] {
                    if let Some(p) = find_keyword(chunk, kw, 0) {
                        chunk = &chunk[..p];
                    }
                }
                let chunk_at = piece_at + at;
                match self.relation(chunk, chunk_at, outers) {
                    Some(r) => scope.relations.push(r),
                    None => scope.complete = false,
                }
                match next {
                    Some(p) => at = p + 4,
                    None => break,
                }
            }
        }
        scope
    }

    /// One relation of a `FROM` list (`t`, `s.t AS x`, `(SELECT …) d`),
    /// `None` when its columns cannot be known.
    fn relation(&mut self, chunk: &str, chunk_at: usize, outers: &[&Scope]) -> Option<Relation> {
        // Join words before the relation, and those of the next join after it.
        const JOIN_WORDS: &[&str] = &["LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL"];
        let mut text = chunk.trim_start();
        let mut text_at = chunk_at + (chunk.len() - text.len());
        while let Some(w) = JOIN_WORDS.iter().find(|w| starts_word(text, w)) {
            let rest = text[w.len()..].trim_start();
            text_at += text.len() - rest.len();
            text = rest;
        }
        let mut text = text.trim_end();
        while let Some(w) = JOIN_WORDS.iter().find(|w| {
            text.len() > w.len()
                && text.as_bytes()[text.len() - w.len()..].eq_ignore_ascii_case(w.as_bytes())
                && !is_identifier_byte(text.as_bytes()[text.len() - w.len() - 1])
        }) {
            text = text[..text.len() - w.len()].trim_end();
        }
        if starts_word(text, "LATERAL") {
            return None;
        }
        if starts_word(text, "ONLY") {
            let rest = text[4..].trim_start();
            text_at += text.len() - rest.len();
            text = rest;
        }
        let (columns, after) = if text.starts_with('(') {
            let close = matching_close(text, 0)?;
            let inner = text[1..close].trim();
            if !starts_word(inner, "SELECT") {
                return None;
            }
            let inner_at = text_at + 1 + (text[1..close].len() - text[1..close].trim_start().len());
            self.query(inner_at, inner_at + inner.len(), outers);
            let names = derived_output_names(inner);
            (names, &text[close + 1..])
        } else {
            // A name, `schema.name`, quoted or not; a call is a function.
            let bytes = text.as_bytes();
            let mut i = 0;
            loop {
                if bytes.get(i) == Some(&b'"') {
                    i = crate::core::sql_scan::quoted_end(bytes, i, b'"');
                } else {
                    while i < bytes.len() && is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                }
                if bytes.get(i) == Some(&b'.') {
                    i += 1;
                } else {
                    break;
                }
            }
            if i == 0 || text[i..].trim_start().starts_with('(') {
                return None;
            }
            let name = &text[..i];
            let last = name.rsplit('.').next().unwrap_or(name);
            let columns = (self.columns_of)(&folded(last))
                .map(|cols| cols.into_iter().map(|c| c.to_string()).collect::<Vec<_>>());
            return self.aliased(columns, &text[i..], last);
        };
        let alias = alias_of(after)?;
        self.aliased(columns, after, &alias)
    }

    /// The relation `after` names (`[AS] alias [(c1, c2)]`), whose columns
    /// are `columns` unless the list renames them; `default` when no alias.
    fn aliased(&self, columns: Option<Vec<String>>, after: &str, default: &str) -> Option<Relation> {
        let mut rest = after.trim_start();
        if starts_word(rest, "AS") {
            rest = rest[2..].trim_start();
        }
        let (qualifier, rest) = match alias_of(rest) {
            Some(alias) if !KEYWORDS.iter().any(|k| alias.eq_ignore_ascii_case(k)) => {
                let tail = rest[alias.len()..].trim_start();
                (alias, tail)
            }
            _ => (default.to_string(), rest),
        };
        let columns = if rest.starts_with('(') {
            let close = matching_close(rest, 0)?;
            let renamed: Vec<String> = split_top_level(&rest[1..close], b',').iter().map(|c| folded(c)).collect();
            let mut all = columns.unwrap_or_default();
            if renamed.len() > all.len() {
                return None;
            }
            all.splice(..renamed.len(), renamed);
            all
        } else {
            columns?
        };
        Some(Relation { qualifier, columns })
    }

    /// Walks `sql[start..end]`, a part of a query level `scope` reads, for
    /// its subqueries and, in a subquery, the outer columns it names.
    fn walk(&mut self, start: usize, end: usize, scope: &Scope, outers: &[&Scope]) {
        let sql = self.sql;
        let bytes = sql.as_bytes();
        let mut i = start;
        while i < end {
            if let Some(next) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
                i = next.max(i + 1);
                continue;
            }
            if bytes[i] == b'(' {
                let inner = sql[i + 1..end].trim_start();
                if starts_word(inner, "SELECT") || starts_word(inner, "WITH") || starts_word(inner, "VALUES") {
                    let Some(close) = matching_close(sql, i) else { return };
                    if starts_word(inner, "SELECT") {
                        let inner_at = end - inner.len();
                        let mut nested: Vec<&Scope> = outers.to_vec();
                        nested.push(scope);
                        self.query(inner_at, close, &nested);
                    }
                    i = close + 1;
                    continue;
                }
                i += 1;
                continue;
            }
            let starts = is_identifier_byte(bytes[i])
                && !bytes[i].is_ascii_digit()
                && (i == 0 || !(is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'"' | b'$')));
            if !starts {
                i += 1;
                continue;
            }
            let mut j = i;
            while j < end && is_identifier_byte(bytes[j]) {
                j += 1;
            }
            if !outers.is_empty() {
                if let Some(edit) = self.resolve(i, j, scope, outers) {
                    self.edits.push((i, j, edit));
                }
            }
            i = j;
        }
    }

    /// `sql[start..end]`, a name read where `scope` is, qualified when it is
    /// an outer level's column for certain.
    fn resolve(&self, start: usize, end: usize, scope: &Scope, outers: &[&Scope]) -> Option<String> {
        let sql = self.sql;
        let word = &sql[start..end];
        if KEYWORDS.iter().any(|k| word.eq_ignore_ascii_case(k)) {
            return None;
        }
        let after = sql[end..].trim_start();
        // Qualified (`t.x`), a call, a typed literal (`date '…'`), a named
        // argument.
        if after.starts_with('.') || after.starts_with('(') || after.starts_with('\'') || after.starts_with("=>") {
            return None;
        }
        let before = sql[..start].trim_end();
        if before.ends_with('.') || before.ends_with("::") || before.ends_with('\'') {
            return None;
        }
        let previous: String = before.bytes().rev().take_while(|b| is_identifier_byte(*b)).collect::<Vec<u8>>().into_iter().rev().map(|b| b as char).collect();
        if ["AS", "OVER", "WINDOW"].iter().any(|k| previous.eq_ignore_ascii_case(k)) {
            return None;
        }
        if let Some(call) = before.strip_suffix('(') {
            let name: String = call.trim_end().bytes().rev().take_while(|b| is_identifier_byte(*b)).collect::<Vec<u8>>().into_iter().rev().map(|b| b as char).collect();
            if name.eq_ignore_ascii_case("EXTRACT") {
                return None;
            }
        }
        let name = word.to_ascii_lowercase();
        if scope.has(&name) || !scope.complete {
            return None;
        }
        for outer in outers.iter().rev() {
            let holding: Vec<&Relation> = outer.relations.iter().filter(|r| r.columns.iter().any(|c| *c == name)).collect();
            match holding.len() {
                0 if outer.complete => continue,
                1 => return Some(format!("{}.{}", holding[0].qualifier, word)),
                _ => return None,
            }
        }
        None
    }
}

/// The alias a relation's text starts with: a bare word or a quoted name.
fn alias_of(text: &str) -> Option<String> {
    let text = text.trim_start();
    let text = if starts_word(text, "AS") { text[2..].trim_start() } else { text };
    let bytes = text.as_bytes();
    if bytes.first() == Some(&b'"') {
        let end = crate::core::sql_scan::quoted_end(bytes, 0, b'"');
        return Some(text[..end].to_string());
    }
    let n = bytes.iter().take_while(|b| is_identifier_byte(**b)).count();
    (n > 0 && !bytes[0].is_ascii_digit()).then(|| text[..n].to_string())
}

/// A derived table's column names, from its select list.
fn derived_output_names(inner: &str) -> Option<Vec<String>> {
    if ["UNION", "INTERSECT", "EXCEPT"].iter().any(|k| find_keyword(inner, k, 0).is_some()) {
        return None;
    }
    let list_at = inner.len() - inner.trim_start().len() + "SELECT".len();
    let list_end = find_keyword(inner, "FROM", 0).unwrap_or(inner.len());
    let list_end = AFTER_FROM.iter().filter_map(|kw| find_keyword(&inner[..list_end], kw, list_at)).min().unwrap_or(list_end);
    output_names(&inner[list_at..list_end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sql: &str) -> Option<String> {
        let columns = |table: &str| -> Option<Vec<String>> {
            let cols: &[&str] = match table {
                "cu" => &["id", "name"],
                "ord" => &["oid", "customer_id", "total", "name"],
                "item" => &["oid", "sku"],
                "other" => &["id", "x"],
                _ => return None,
            };
            Some(cols.iter().map(|c| c.to_string()).collect())
        };
        qualify(sql, &columns)
    }

    #[test]
    fn an_outer_column_named_bare_is_qualified() {
        assert_eq!(
            run("SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = id)").as_deref(),
            Some("SELECT name FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = cu.id)")
        );
        assert_eq!(
            run("SELECT name, (SELECT count(*) FROM ord o WHERE o.customer_id = id) FROM cu c").as_deref(),
            Some("SELECT name, (SELECT count(*) FROM ord o WHERE o.customer_id = c.id) FROM cu c")
        );
        // Two levels out, past one that does not have it.
        assert_eq!(
            run("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE EXISTS (SELECT 1 FROM item WHERE item.oid = ord.oid AND sku = name))")
                .as_deref(),
            Some("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE EXISTS (SELECT 1 FROM item WHERE item.oid = ord.oid AND sku = ord.name))"),
            "name is ord's: the nearest level that has it"
        );
        assert_eq!(
            run("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM item WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = id))").as_deref(),
            Some("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM item WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = cu.id))")
        );
        // A derived table names its columns; a renamed one by its list.
        assert_eq!(
            run("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT customer_id AS k FROM ord) s WHERE s.k = id)").as_deref(),
            Some("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT customer_id AS k FROM ord) s WHERE s.k = cu.id)")
        );
        assert_eq!(
            run("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT customer_id, total FROM ord) AS s(k, t) WHERE k = id)").as_deref(),
            Some("SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT customer_id, total FROM ord) AS s(k, t) WHERE k = cu.id)")
        );
        // The target of a write.
        assert_eq!(
            run("DELETE FROM cu WHERE NOT EXISTS (SELECT 1 FROM ord WHERE customer_id = id)").as_deref(),
            Some("DELETE FROM cu WHERE NOT EXISTS (SELECT 1 FROM ord WHERE customer_id = cu.id)")
        );
        assert_eq!(
            run("UPDATE cu SET name = 'x' WHERE id IN (SELECT customer_id FROM ord WHERE total > 1 AND customer_id = id)").as_deref(),
            Some("UPDATE cu SET name = 'x' WHERE id IN (SELECT customer_id FROM ord WHERE total > 1 AND customer_id = cu.id)")
        );
    }

    #[test]
    fn what_is_not_certainly_outer_is_left_alone() {
        for sql in [
            // The subquery's own column wins.
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE name = 'a')",
            // Two outer relations have it: PostgreSQL's 42702.
            "SELECT 1 FROM cu, other WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = id)",
            // A relation whose columns are not known, inside or out.
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM nope WHERE customer_id = id)",
            "SELECT 1 FROM nope WHERE EXISTS (SELECT 1 FROM ord WHERE customer_id = id)",
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM (SELECT * FROM ord) s WHERE s.customer_id = id)",
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM generate_series(1, 3) g WHERE g = id)",
            // Qualified, called, typed, quoted, a keyword's place.
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM ord WHERE ord.customer_id = cu.id)",
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM item WHERE sku = \"id\")",
            "SELECT 1 FROM cu WHERE EXISTS (SELECT 1 FROM item WHERE sku = 'id')",
            // A set operation.
            "SELECT 1 FROM cu WHERE id IN (SELECT customer_id FROM ord UNION SELECT id FROM other)",
            // No subquery reads an outer column.
            "SELECT id FROM cu WHERE id IN (SELECT customer_id FROM ord)",
            "SELECT 1",
        ] {
            assert_eq!(run(sql), None, "{sql}");
        }
        // An alias of the subquery's own select list, in its ORDER BY.
        assert_eq!(run("SELECT 1 FROM cu WHERE id IN (SELECT customer_id AS id2 FROM ord ORDER BY id2 LIMIT 1)"), None);
        // EXTRACT's field is no column even where an outer one has its name.
        let columns = |table: &str| -> Option<Vec<String>> {
            match table {
                "a" => Some(vec!["year".into(), "k".into()]),
                "b" => Some(vec!["d".into(), "k".into()]),
                _ => None,
            }
        };
        assert_eq!(qualify("SELECT 1 FROM a WHERE EXISTS (SELECT 1 FROM b WHERE extract(year FROM d) = 1)", &columns), None);
    }
}
