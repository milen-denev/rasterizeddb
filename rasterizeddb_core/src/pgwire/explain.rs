//! `EXPLAIN [ANALYZE] statement`, as one `QUERY PLAN` text column.
//!
//! The plan is what this engine does with the statement, from its own
//! passes: the CTEs and derived tables it materialises first, a correlated
//! subquery as the lateral join it runs, the relations it scans and the hash
//! joins between them with their keys, then filtering, grouping, sorting and
//! the limit. Whether a scan is answered from an index is decided as it runs,
//! from the predicate and the index's state, so the plan says so rather than
//! naming one it might not use. `ANALYZE` runs the statement (a write too, as
//! PostgreSQL's does) and adds its rows and time. It was "syntax error or
//! unsupported query"; tools ask it before running a slow query.

use std::io;

use rclite::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

use super::trigger_exec::query_in;
use super::view_statements::run_internal;
use super::{SessionState, fail_statement, send_command_complete, send_message, sqlstate_for};
use crate::core::database::Database;
use crate::core::rql::join::find_keyword;

/// `EXPLAIN`'s options and the statement it explains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Explain {
    pub analyze: bool,
    pub verbose: bool,
    pub statement: String,
}

/// `EXPLAIN …`, or `None` for a statement that is not one.
pub(crate) fn parse_explain(sql: &str) -> Option<Result<Explain, (&'static str, String)>> {
    let t = sql.trim().trim_end_matches(';').trim_end();
    if !(t.len() > 7
        && t.get(..7).is_some_and(|h| h.eq_ignore_ascii_case("EXPLAIN"))
        && t.as_bytes()[7].is_ascii_whitespace())
    {
        return None;
    }
    let mut rest = t[7..].trim_start();
    let mut explain = Explain {
        analyze: false,
        verbose: false,
        statement: String::new(),
    };
    if rest.starts_with('(') {
        let Some(close) = rest.find(')') else {
            return Some(Err((
                "42601",
                "syntax error in EXPLAIN options".to_string(),
            )));
        };
        for option in rest[1..close].split(',') {
            let mut words = option.split_whitespace();
            let name = words.next().unwrap_or("").to_ascii_uppercase();
            let value = words
                .next()
                .map(|v| v.trim_matches('\'').to_ascii_uppercase());
            let on = !matches!(value.as_deref(), Some("FALSE" | "OFF" | "0"));
            match name.as_str() {
                "ANALYZE" | "ANALYSE" => explain.analyze = on,
                "VERBOSE" => explain.verbose = on,
                "FORMAT" if value.as_deref() != Some("TEXT") => {
                    return Some(Err((
                        "0A000",
                        "EXPLAIN formats other than TEXT are not supported here".to_string(),
                    )));
                }
                "FORMAT" | "COSTS" | "BUFFERS" | "TIMING" | "SUMMARY" | "SETTINGS" | "WAL"
                | "SERIALIZE" | "MEMORY" | "GENERIC_PLAN" => {}
                "" => {}
                other => {
                    return Some(Err((
                        "42601",
                        format!(
                            "unrecognized EXPLAIN option \"{}\"",
                            other.to_ascii_lowercase()
                        ),
                    )));
                }
            }
        }
        rest = rest[close + 1..].trim_start();
    } else {
        loop {
            let word = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_uppercase();
            match word.as_str() {
                "ANALYZE" | "ANALYSE" => explain.analyze = true,
                "VERBOSE" => explain.verbose = true,
                _ => break,
            }
            rest = rest[word.len()..].trim_start();
        }
    }
    if rest.is_empty() {
        return Some(Err(("42601", "EXPLAIN needs a statement".to_string())));
    }
    explain.statement = rest.to_string();
    Some(Ok(explain))
}

/// The clause of `sql` starting at `keyword`, to the next clause.
fn clause<'a>(sql: &'a str, keyword: &str, ends: &[&str]) -> Option<&'a str> {
    let at = find_keyword(sql, keyword, 0)?;
    let body = &sql[at + keyword.len()..];
    let end = ends
        .iter()
        .filter_map(|k| find_keyword(body, k, 0))
        .min()
        .unwrap_or(body.len());
    Some(body[..end].trim())
}

const ENDS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "FOR",
    "WINDOW",
    "RETURNING",
    "UNION",
    "INTERSECT",
    "EXCEPT",
];

/// The plan's lines, outermost first.
fn plan_lines(statement: &str) -> Vec<String> {
    let flat = crate::core::sql_scan::flatten_whitespace(statement);
    let sql = flat.trim();
    let head = sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    let mut lines: Vec<String> = Vec::new();
    let mut push =
        |depth: usize, text: String| lines.push(format!("{}{}", "  ".repeat(depth), text));
    let mut depth = 0usize;
    let target = crate::core::rql::lock_set::relation_references(sql)
        .into_iter()
        .find(|r| r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare)
        .map(|r| r.name)
        .unwrap_or_default();
    match head.as_str() {
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" => {
            let verb = match head.as_str() {
                "INSERT" => "Insert",
                "UPDATE" => "Update",
                "DELETE" => "Delete",
                _ => "Merge",
            };
            push(0, format!("{verb} on {target}"));
            depth = 1;
            if head == "INSERT" && find_keyword(sql, "SELECT", 0).is_none() {
                push(depth, "Values".to_string());
                return lines;
            }
        }
        "SELECT" | "WITH" | "VALUES" | "TABLE" => {}
        _ => {
            push(
                0,
                format!("Utility statement: {}", head.to_ascii_lowercase()),
            );
            return lines;
        }
    }
    if crate::core::rql::cte::has_with_clause(sql) {
        push(
            depth,
            "Materialize each CTE first, in order, as a relation".to_string(),
        );
    }
    if find_keyword(sql, "UNION", 0).is_some()
        || find_keyword(sql, "INTERSECT", 0).is_some()
        || find_keyword(sql, "EXCEPT", 0).is_some()
    {
        push(
            depth,
            "Append each branch, then combine (UNION / INTERSECT / EXCEPT)".to_string(),
        );
    }
    if let Some(limit) = clause(sql, "LIMIT", &["OFFSET", "FOR"]) {
        push(depth, format!("Limit: {limit}"));
        depth += 1;
    }
    if let Some(order) = clause(sql, "ORDER BY", &["LIMIT", "OFFSET", "FOR"]) {
        push(depth, "Sort".to_string());
        push(depth + 1, format!("Sort Key: {order}"));
        depth += 1;
    }
    let grouped = clause(
        sql,
        "GROUP BY",
        &["HAVING", "ORDER", "LIMIT", "OFFSET", "WINDOW"],
    );
    let select_list = find_keyword(sql, "FROM", 0)
        .map(|f| &sql[..f])
        .unwrap_or(sql);
    let aggregates = [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "array_agg(",
        "string_agg(",
        "json_agg(",
        "jsonb_agg(",
        "bool_and(",
        "bool_or(",
    ];
    if grouped.is_some()
        || aggregates
            .iter()
            .any(|a| crate::core::sql_scan::contains_ignore_ascii_case(select_list, a.as_bytes()))
    {
        push(
            depth,
            if grouped.is_some() {
                "HashAggregate".to_string()
            } else {
                "Aggregate".to_string()
            },
        );
        if let Some(g) = grouped {
            push(depth + 1, format!("Group Key: {g}"));
        }
        if let Some(having) = clause(sql, "HAVING", &["ORDER", "LIMIT", "OFFSET", "WINDOW"]) {
            push(depth + 1, format!("Filter: ({having})"));
        }
        depth += 1;
    }
    if crate::core::rql::correlated::rewrite_select(sql).is_some() {
        push(
            depth,
            "Correlated subquery: run as a lateral join, once per row".to_string(),
        );
    }
    // An UPDATE scans its target (and joins its FROM, when it has one).
    let from_text = match (head.as_str(), clause(sql, "FROM", ENDS)) {
        ("UPDATE", Some(f)) if !f.is_empty() => format!("{target}, {f}"),
        ("UPDATE", _) => target.clone(),
        (_, Some(f)) => f.to_string(),
        (_, None) => String::new(),
    };
    let from = from_text.as_str();
    let filter = clause(
        sql,
        "WHERE",
        &[
            "GROUP",
            "HAVING",
            "ORDER",
            "LIMIT",
            "OFFSET",
            "FOR",
            "WINDOW",
            "RETURNING",
        ],
    );
    if from.is_empty() {
        push(depth, "Result".to_string());
        return lines;
    }
    if crate::core::rql::cte::next_derived_table(sql).is_some() {
        push(
            depth,
            "Materialize each derived table first, as a relation".to_string(),
        );
    }
    match crate::core::rql::join::parse_from_clause(from) {
        Ok(relations) if relations.len() > 1 => {
            push(depth, "Hash Join".to_string());
            for r in relations.iter().skip(1) {
                if let Some(join) = &r.join {
                    let key = if join.left_qualified.is_empty() {
                        "no equality: every pair is tested".to_string()
                    } else {
                        format!("{} = {}", join.left_qualified, join.right_qualified)
                    };
                    push(
                        depth + 1,
                        format!("{:?} join {}: {}", join.kind, r.label, key),
                    );
                    for extra in &join.extra {
                        push(depth + 2, format!("Join Filter: ({extra})"));
                    }
                }
            }
            if let Some(f) = filter {
                push(depth + 1, format!("Filter: ({f})"));
            }
            for r in &relations {
                push(
                    depth + 1,
                    format!(
                        "Scan on {}{}",
                        r.table,
                        if r.label != r.table {
                            format!(" {}", r.label)
                        } else {
                            String::new()
                        }
                    ),
                );
            }
        }
        Ok(relations) if relations.len() == 1 => {
            let r = &relations[0];
            push(
                depth,
                format!(
                    "Scan on {}{}",
                    r.table,
                    if r.label != r.table {
                        format!(" {}", r.label)
                    } else {
                        String::new()
                    }
                ),
            );
            if let Some(f) = filter {
                push(depth + 1, format!("Filter: ({f})"));
                push(
                    depth + 1,
                    "Index use: decided as the scan runs, from the filter and the indexes' state"
                        .to_string(),
                );
            }
        }
        _ => {
            push(depth, format!("Scan: {from}"));
            if let Some(f) = filter {
                push(depth + 1, format!("Filter: ({f})"));
            }
        }
    }
    lines
}

/// Runs `EXPLAIN`.
pub(super) async fn run_explain<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    explain: Explain,
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The statement must be one this engine reads, as PostgreSQL plans only
    // a statement it parses: a SELECT is described, so an unknown column or
    // relation is the error it would be.
    let is_select = explain
        .statement
        .trim_start()
        .get(..6)
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"));
    if is_select && !explain.analyze {
        match Box::pin(super::describe_query(database, &explain.statement)).await {
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(e) => return fail_statement(state, stream, "XX000", &e.to_string()).await,
        }
    }
    let mut lines = plan_lines(&explain.statement);
    if explain.analyze {
        let started = std::time::Instant::now();
        let outcome = if is_select {
            query_in(database, state, &explain.statement, &[])
                .await?
                .map(|rows| format!("{} rows", rows.rows.len()))
        } else {
            run_internal(database, state, &explain.statement).await?
        };
        let elapsed = started.elapsed();
        match outcome {
            Ok(result) => {
                lines.push(format!("Actual: {result}"));
                lines.push(format!(
                    "Execution Time: {:.3} ms",
                    elapsed.as_secs_f64() * 1000.0
                ));
            }
            Err(e) => return fail_statement(state, stream, sqlstate_for(&e), &e).await,
        }
    }
    let mut description = Vec::new();
    description.extend_from_slice(&1i16.to_be_bytes());
    description.extend_from_slice(b"QUERY PLAN\0");
    description.extend_from_slice(&0i32.to_be_bytes());
    description.extend_from_slice(&0i16.to_be_bytes());
    description.extend_from_slice(&25i32.to_be_bytes());
    description.extend_from_slice(&(-1i16).to_be_bytes());
    description.extend_from_slice(&(-1i32).to_be_bytes());
    description.extend_from_slice(&0i16.to_be_bytes());
    // Under the extended protocol the description is `Describe`'s.
    if !omit_description {
        send_message(stream, b'T', &description).await?;
    }
    for line in &lines {
        let mut row = Vec::with_capacity(line.len() + 6);
        row.extend_from_slice(&1i16.to_be_bytes());
        row.extend_from_slice(&(line.len() as i32).to_be_bytes());
        row.extend_from_slice(line.as_bytes());
        send_message(stream, b'D', &row).await?;
    }
    send_command_complete(stream, "EXPLAIN").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_parse_both_ways() {
        let e = parse_explain("EXPLAIN ANALYZE VERBOSE SELECT 1")
            .unwrap()
            .unwrap();
        assert!(e.analyze && e.verbose);
        assert_eq!(e.statement, "SELECT 1");
        let e = parse_explain("explain (analyze false, costs off, format text) select 1;")
            .unwrap()
            .unwrap();
        assert!(!e.analyze);
        assert_eq!(
            parse_explain("EXPLAIN (FORMAT JSON) SELECT 1")
                .unwrap()
                .unwrap_err()
                .0,
            "0A000"
        );
        assert!(parse_explain("EXPLAINX").is_none());
    }

    #[test]
    fn plans_name_the_engines_passes() {
        let p = plan_lines(
            "SELECT a.x, count(*) FROM a JOIN b ON a.id = b.aid WHERE b.n > 1 GROUP BY a.x ORDER BY 2 LIMIT 5",
        );
        assert_eq!(
            p,
            [
                "Limit: 5",
                "  Sort",
                "    Sort Key: 2",
                "    HashAggregate",
                "      Group Key: a.x",
                "      Hash Join",
                "        Inner join b: a.id = b.aid",
                "        Filter: (b.n > 1)",
                "        Scan on a",
                "        Scan on b",
            ]
        );
        let p = plan_lines("UPDATE t SET v = 1 WHERE id = 3");
        assert_eq!(p[0], "Update on t");
        assert!(
            p.iter()
                .any(|l| l.contains("Index use: decided as the scan runs")),
            "{p:?}"
        );
        assert_eq!(
            plan_lines("INSERT INTO t VALUES (1)"),
            ["Insert on t", "  Values"]
        );
    }
}
