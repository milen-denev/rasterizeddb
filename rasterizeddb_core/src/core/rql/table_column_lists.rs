//! A table's alias with a column list, `FROM t AS x(a, b)`, as the derived
//! table it means: `FROM (SELECT id AS a, name AS b, v FROM t) AS x`.
//!
//! PostgreSQL renames the table's first columns, in their order, and keeps
//! the rest. The engine read the list as a call and refused the statement
//! ("syntax error at or near "x(a)"", "missing FROM-clause entry for table
//! "x""). The columns come from the schema, so only a table that exists is
//! rewritten. A CTE or a function in `FROM` is left alone, and so is a list
//! longer than the table, which fails as before. A view expanded by then
//! has become a derived table; `sql_scan::derived_column_lists` renames its
//! list here, as it does for one written by hand.

use crate::core::sql_scan::{is_identifier_byte, matching_close, quoted_end, split_top_level};

/// `sql` with each table alias column list written out, `None` when it has
/// none. `columns_of` gives a table's columns in order, `None` for no table.
pub(crate) fn rewrite(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    // Cheap tests first: this runs on every statement.
    if !sql.contains('(')
        || !(crate::core::sql_scan::contains_ignore_ascii_case(sql, b"from")
            || crate::core::sql_scan::contains_ignore_ascii_case(sql, b"join"))
    {
        return None;
    }
    let ctes = crate::core::rql::views::cte_names(sql);
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    for r in crate::core::rql::lock_set::relation_references(sql) {
        if r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare || ctes.contains(&r.name.to_ascii_lowercase())
        {
            continue;
        }
        let Some((alias, list_open, list_close)) = alias_with_list(sql, r.span.end) else { continue };
        let names: Vec<&str> = split_top_level(&sql[list_open + 1..list_close], b',').into_iter().map(str::trim).collect();
        if names.is_empty() || !names.iter().all(|n| plain_name(n)) {
            continue;
        }
        let Some(columns) = columns_of(&r.name) else { continue };
        if names.len() > columns.len() {
            continue;
        }
        let items: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                let column = crate::core::rql::views::quote_ident(column);
                match names.get(i) {
                    Some(name) => format!("{column} AS {name}"),
                    None => column,
                }
            })
            .collect();
        let relation = &sql[r.span.clone()];
        edits.push((r.span.start, list_close + 1, format!("(SELECT {} FROM {relation}) AS {alias}", items.join(", "))));
    }
    let rewritten = if edits.is_empty() {
        None
    } else {
        edits.sort_by_key(|e| e.0);
        let mut out = String::with_capacity(sql.len() + 64);
        let mut copied = 0;
        for (start, end, text) in edits {
            if start < copied {
                return None;
            }
            out.push_str(&sql[copied..start]);
            out.push_str(&text);
            copied = end;
        }
        out.push_str(&sql[copied..]);
        Some(out)
    };
    // A view's list, now on the derived table it expanded to.
    match crate::core::sql_scan::derived_column_lists(rewritten.as_deref().unwrap_or(sql)) {
        Some(text) => Some(text),
        None => rewritten,
    }
}

/// [`rewrite`] over `database`'s tables, their visible columns.
pub(crate) fn rewrite_in(database: &crate::core::database::Database, sql: &str) -> Option<String> {
    rewrite(sql, &|name: &str| {
        database.schema_view(name).map(|view| {
            view.schema()
                .fields
                .iter()
                .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                .map(|f| f.name.clone())
                .collect()
        })
    })
}

/// A plain or quoted name, nothing else.
fn plain_name(n: &str) -> bool {
    (n.len() >= 2 && n.starts_with('"') && n.ends_with('"'))
        || (!n.is_empty() && n.bytes().all(is_identifier_byte) && !n.as_bytes()[0].is_ascii_digit())
}

/// After a relation ending at `at`: `[AS] alias (`…`)`, as the alias and
/// where its list opens and closes.
fn alias_with_list(sql: &str, at: usize) -> Option<(&str, usize, usize)> {
    const NOT_ALIASES: &[&str] = &[
        "AND", "OR", "NOT", "IN", "IS", "AS", "ON", "USING", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS",
        "NATURAL", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW",
        "FETCH", "FOR", "LATERAL", "SELECT", "VALUES", "RETURNING", "SET", "TABLESAMPLE", "WITH",
    ];
    let bytes = sql.as_bytes();
    let skip_ws = |mut i: usize| {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        i
    };
    let mut i = skip_ws(at);
    if i == at {
        // `t(…)` is a call, `t AS` and `t x` need the space.
        return None;
    }
    if bytes.len() > i + 2 && bytes[i..i + 2].eq_ignore_ascii_case(b"AS") && !is_identifier_byte(bytes[i + 2]) {
        i = skip_ws(i + 2);
    }
    let alias_start = i;
    let alias_end = if bytes.get(i) == Some(&b'"') {
        quoted_end(bytes, i, b'"')
    } else {
        let mut j = i;
        while j < bytes.len() && is_identifier_byte(bytes[j]) {
            j += 1;
        }
        j
    };
    if alias_end == alias_start || bytes[alias_start].is_ascii_digit() {
        return None;
    }
    let alias = &sql[alias_start..alias_end];
    if NOT_ALIASES.iter().any(|k| alias.eq_ignore_ascii_case(k)) {
        return None;
    }
    let open = skip_ws(alias_end);
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let close = matching_close(sql, open)?;
    Some((alias, open, close))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(name: &str) -> Option<Vec<String>> {
        match name {
            "ga" => Some(vec!["id".into(), "name".into(), "v".into()]),
            _ => None,
        }
    }

    #[test]
    fn a_table_alias_list_renames_the_first_columns() {
        assert_eq!(
            rewrite("SELECT x.a, x.b FROM ga AS x(a, b) ORDER BY 1", &columns).as_deref(),
            Some(r#"SELECT x.a, x.b FROM (SELECT "id" AS a, "name" AS b, "v" FROM ga) AS x ORDER BY 1"#)
        );
        assert_eq!(
            rewrite("SELECT * FROM ga x (p) JOIN ga y(q, r, s) ON x.p = y.q", &columns).as_deref(),
            Some(
                r#"SELECT * FROM (SELECT "id" AS p, "name", "v" FROM ga) AS x JOIN (SELECT "id" AS q, "name" AS r, "v" AS s FROM ga) AS y ON x.p = y.q"#
            )
        );
    }

    #[test]
    fn what_is_no_table_alias_list_is_left() {
        // An alias without a list, a function, a CTE, an unknown table, a
        // list longer than the table.
        assert_eq!(rewrite("SELECT * FROM ga x WHERE x.id IN (1, 2)", &columns), None);
        assert_eq!(rewrite("SELECT * FROM generate_series(1, 3) AS g(n)", &columns), None);
        assert_eq!(rewrite("WITH ga AS (SELECT 1) SELECT * FROM ga AS x(a)", &columns), None);
        assert_eq!(rewrite("SELECT * FROM nosuch AS x(a)", &columns), None);
        assert_eq!(rewrite("SELECT * FROM ga AS x(a, b, c, d)", &columns), None);
        assert_eq!(rewrite("SELECT * FROM ga WHERE id IN (1)", &columns), None);
    }
}
