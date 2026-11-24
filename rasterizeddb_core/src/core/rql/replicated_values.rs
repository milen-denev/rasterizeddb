//! A write that replication ships as text carries the values of its
//! non-deterministic calls, not the calls.
//!
//! Replication sends the SQL a transaction ran and each peer runs it again
//! (docs/replication.md §2). `now()`, `random()`, `gen_random_uuid()` and the
//! like were evaluated a second time on every peer, so two nodes held
//! different rows for the same transaction, with nothing reporting it: the
//! one defence was a warning, logged once per process. On a node that is
//! replicating, a write is rewritten before it runs:
//!
//! * the clock functions (`now()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`,
//!   `statement_timestamp()`, …) read the statement's clock, one instant for
//!   every row, so each becomes a literal of that instant wherever it stands;
//! * a function that answers anew at every call (`random()`,
//!   `gen_random_uuid()`, `clock_timestamp()`, `timeofday()`) becomes a
//!   literal of one call where it is evaluated once: a slot of `INSERT …
//!   VALUES`, one row per tuple. Anywhere else (`UPDATE … SET r = random()`,
//!   a `WHERE`, `INSERT … SELECT`) it is evaluated once per row, which no
//!   literal can stand for, and the statement is refused (0A000) rather than
//!   left to diverge.
//!
//! The statement then runs as rewritten, so the origin stores exactly what
//! the peers will. A `RETURNING` list is left alone: it changes no row. A
//! read is never rewritten, and a node that is not replicating runs every
//! statement as written.

/// What [`fold`] made of a statement.
#[derive(Debug, PartialEq, Eq)]
pub enum Fold {
    /// Nothing to replace.
    Unchanged,
    /// The statement with its calls replaced by their values.
    Rewritten(String),
    /// A call that cannot be given one value; the message says which.
    Refused(String),
}

/// How a function's value is fixed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// One value for the whole statement: the statement clock.
    Stable,
    /// A new value at every call.
    Volatile,
}

/// `(name, needs parentheses, kind, the type its literal is cast to)`.
/// The keyword forms (`CURRENT_TIMESTAMP`) take an optional precision.
const FUNCTIONS: &[(&str, bool, Kind, &str)] = &[
    ("now", true, Kind::Stable, "timestamptz"),
    ("transaction_timestamp", true, Kind::Stable, "timestamptz"),
    ("statement_timestamp", true, Kind::Stable, "timestamptz"),
    ("current_timestamp", false, Kind::Stable, "timestamptz"),
    ("localtimestamp", false, Kind::Stable, "timestamp"),
    ("current_date", false, Kind::Stable, "date"),
    ("current_time", false, Kind::Stable, "timetz"),
    ("localtime", false, Kind::Stable, "time"),
    ("random", true, Kind::Volatile, "float8"),
    ("gen_random_uuid", true, Kind::Volatile, "uuid"),
    ("clock_timestamp", true, Kind::Volatile, "timestamptz"),
    ("timeofday", true, Kind::Volatile, "text"),
    // Random bits after its timestamp: a new value at every call, like
    // `gen_random_uuid()`. Left out, `uuidv7()` was run again on every peer.
    ("uuidv7", true, Kind::Volatile, "uuid"),
];

/// One call found in the text.
struct Call {
    start: usize,
    end: usize,
    function: usize,
}

/// Whether `text` calls a function that answers anew at every call
/// (`random()`, `gen_random_uuid()`, `clock_timestamp()`, `timeofday()`):
/// an expression no fold may compute once for the statement.
pub fn calls_volatile(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    if !FUNCTIONS.iter().any(|(name, _, kind, _)| *kind == Kind::Volatile && lower.contains(name)) {
        return false;
    }
    calls_in(text).iter().any(|c| FUNCTIONS[c.function].2 == Kind::Volatile)
}

/// [`fold_defaults`], then [`fold`] on what it made: a replicating node's
/// write, with every value a peer would compute anew written into it.
pub async fn fold_write(sql: &str, database: &crate::core::database::Database) -> Fold {
    match fold_defaults(sql, database).await {
        Fold::Refused(message) => Fold::Refused(message),
        Fold::Unchanged => fold(sql),
        Fold::Rewritten(text) => match fold(&text) {
            Fold::Unchanged => Fold::Rewritten(text),
            other => other,
        },
    }
}

/// See the module documentation. `sql` is one statement.
pub fn fold(sql: &str) -> Fold {
    // Almost every statement mentions none of them: one cheap pass decides.
    let lower = sql.to_ascii_lowercase();
    if !FUNCTIONS.iter().any(|(name, ..)| lower.contains(name)) {
        return Fold::Unchanged;
    }
    let Some(kind) = write_kind(sql) else {
        return Fold::Unchanged;
    };
    let calls = calls_in(sql);
    if calls.is_empty() {
        return Fold::Unchanged;
    }
    let returning_at = top_level_keyword(sql, "RETURNING").unwrap_or(sql.len());
    let values = if kind == "INSERT" { values_region(sql) } else { None };

    let mut out = String::with_capacity(sql.len() + 32);
    let mut copied = 0usize;
    let mut changed = false;
    for call in &calls {
        if call.start >= returning_at {
            continue;
        }
        let (name, _, function_kind, cast) = FUNCTIONS[call.function];
        if function_kind == Kind::Volatile {
            let once = values.is_some_and(|(from, to)| call.start >= from && call.end <= to);
            if !once {
                return Fold::Refused(format!(
                    "{name}() in this {kind} would be evaluated again, to a different value, on every \
                     node replicating it: replication sends the statement, not the rows. Compute the \
                     value in the client, or write it in an INSERT … VALUES, where it is taken once"
                ));
            }
        }
        let text = &sql[call.start..call.end];
        let Some(mut value) = evaluate_as_text(text) else {
            // Something the evaluator does not take (an argument it refuses):
            // left as written, for the statement to report or run as before.
            continue;
        };
        if cast == "date" {
            // `CURRENT_DATE` renders as a midnight timestamp here; its date
            // is the value.
            value.truncate(value.find(' ').unwrap_or(value.len()));
        }
        out.push_str(&sql[copied..call.start]);
        out.push_str(&crate::core::value_codec::quote_sql(&value));
        out.push_str("::");
        out.push_str(cast);
        copied = call.end;
        changed = true;
    }
    if !changed {
        return Fold::Unchanged;
    }
    out.push_str(&sql[copied..]);
    Fold::Rewritten(out)
}

/// The statement's verb when it writes, `None` for anything else. A `WITH`
/// counts when a write follows its CTEs.
fn write_kind(sql: &str) -> Option<&'static str> {
    let first = first_word(sql)?;
    let upper = first.to_ascii_uppercase();
    match upper.as_str() {
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "MERGE" => Some("MERGE"),
        "CREATE" => {
            // CREATE TABLE … AS: its rows are computed here and shipped as
            // the statement.
            let rest = sql.trim_start()[first.len()..].to_ascii_uppercase();
            rest.contains(" AS ").then_some("CREATE TABLE AS")
        }
        "WITH" => ["INSERT", "UPDATE", "DELETE"]
            .iter()
            .find(|kw| top_level_keyword(sql, kw).is_some())
            .copied(),
        _ => None,
    }
}

fn first_word(sql: &str) -> Option<&str> {
    let t = sql.trim_start();
    let end = t.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(t.len());
    (end > 0).then(|| &t[..end])
}

/// Every call of one of [`FUNCTIONS`] outside quotes and comments, in order.
fn calls_in(sql: &str) -> Vec<Call> {
    let bytes = sql.as_bytes();
    let mut calls = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            continue;
        }
        let word_start = c.is_ascii_alphabetic()
            && (i == 0 || !(crate::core::sql_scan::is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.' || bytes[i - 1] == b'$'));
        if !word_start {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[j]) {
            j += 1;
        }
        let word = &sql[i..j];
        let found = FUNCTIONS.iter().position(|(name, ..)| word.eq_ignore_ascii_case(name));
        let Some(function) = found else {
            i = j;
            continue;
        };
        let (_, parens, ..) = FUNCTIONS[function];
        // What follows the name: `(…)`, required for a function and an
        // optional precision for a keyword. A name followed by `.` or by a
        // `(` it does not take is something else (a qualified column).
        let mut k = j;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        let end = if bytes.get(k) == Some(&b'(') {
            match matching_paren(bytes, k) {
                Some(close) => close + 1,
                None => {
                    i = j;
                    continue;
                }
            }
        } else if parens {
            i = j;
            continue;
        } else {
            j
        };
        if bytes.get(j) == Some(&b'.') {
            i = j;
            continue;
        }
        calls.push(Call { start: i, end, function });
        i = end;
    }
    calls
}

/// The `)` that closes the `(` at `open`, quotes and comments skipped.
fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i)?;
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The span of an `INSERT`'s `VALUES` tuples: from the keyword to the first
/// top-level `ON CONFLICT` or `RETURNING`. `None` when the rows come from a
/// query instead.
fn values_region(sql: &str) -> Option<(usize, usize)> {
    let from = top_level_keyword(sql, "VALUES")?;
    if top_level_keyword(sql, "SELECT").is_some_and(|at| at > from) {
        return None;
    }
    let to = ["ON", "RETURNING"]
        .iter()
        .filter_map(|kw| top_level_keyword(&sql[from..], kw).map(|at| from + at))
        .min()
        .unwrap_or(sql.len());
    Some((from, to))
}

/// Where `keyword` first stands as a word outside quotes, comments and
/// parentheses.
fn top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && bytes.len() >= i + keyword.len()
                && bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
                && (i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]))
                && bytes.get(i + keyword.len()).is_none_or(|b| !crate::core::sql_scan::is_identifier_byte(*b)) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// A write whose rows a column's non-deterministic `DEFAULT` would fill
/// (`id uuid DEFAULT gen_random_uuid()`, `created_at DEFAULT now()`), with
/// that default written into the statement, for [`fold`] to turn into the
/// values the peers will store.
///
/// The peers run the statement's text again (see the module documentation),
/// and a column the text leaves out takes its default there too: each node
/// drew its own UUID and read its own clock, so the same row had a different
/// primary key on every node, with nothing reporting it. Here:
///
/// * `INSERT … VALUES` and `DEFAULT VALUES`: each such column the statement
///   leaves out, or fills with `DEFAULT`, gets its default's text in every
///   tuple; [`fold`] then makes it one literal per tuple;
/// * `INSERT … SELECT`: a clock default (`now()`) is added to the query's
///   output, one instant for every row as it is anyway; a per-row one
///   (`gen_random_uuid()`) is refused, since no literal can stand for it;
/// * `UPDATE … SET c = DEFAULT` with such a default is refused;
/// * `ALTER TABLE … ADD COLUMN` with one: see [`fold_add_column_default`].
///
/// The message says what to write instead. Run before [`fold`], on a
/// replicating node only.
pub async fn fold_defaults(sql: &str, database: &crate::core::database::Database) -> Fold {
    let lower = sql.to_ascii_lowercase();
    let first = first_word(sql).map(|w| w.to_ascii_uppercase());
    match first.as_deref() {
        Some("INSERT") => fold_insert_defaults(sql, database),
        Some("UPDATE") if lower.contains("default") => refuse_update_default(sql, database),
        Some("ALTER") if lower.contains("default") => fold_add_column_default(sql, database).await,
        _ => Fold::Unchanged,
    }
}

/// Whether a default expression has a value a peer would compute anew: it
/// calls one of [`FUNCTIONS`].
fn varies(expression: &str) -> bool {
    !calls_in(expression).is_empty()
}

fn varies_per_row(expression: &str) -> bool {
    calls_in(expression).iter().any(|c| FUNCTIONS[c.function].2 == Kind::Volatile)
}

/// An identifier as stored: a quoted one as written, a bare one lowercased,
/// a schema qualifier dropped.
fn stored_name(raw: &str) -> String {
    let last = split_qualified(raw.trim()).pop().unwrap_or_default();
    if last.len() >= 2 && last.starts_with('"') && last.ends_with('"') {
        last[1..last.len() - 1].replace("\"\"", "\"")
    } else {
        last.to_ascii_lowercase()
    }
}

/// `a.b."C"` as its parts, quotes kept.
fn split_qualified(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for c in raw.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                current.push(c);
            }
            '.' if !quoted => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    parts.push(current);
    parts
}

/// The identifier at the start of `text` (bare, quoted, or qualified), and
/// what follows it.
fn take_identifier(text: &str) -> Option<(&str, &str)> {
    let t = text.trim_start();
    let bytes = t.as_bytes();
    let mut i = 0usize;
    loop {
        if bytes.get(i) == Some(&b'"') {
            i = crate::core::sql_scan::skip_quoted(bytes, i)?;
        } else {
            let start = i;
            while i < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[i]) {
                i += 1;
            }
            if i == start {
                return None;
            }
        }
        if bytes.get(i) == Some(&b'.') {
            i += 1;
            continue;
        }
        return Some((&t[..i], &t[i..]));
    }
}

/// `text` split at its top-level commas.
fn split_top_commas(text: &str) -> Vec<&str> {
    crate::core::sql_scan::split_top_level(text, b',')
}

/// The columns of `table` a write can name, in order, and each one's default
/// expression, when it varies.
fn varying_defaults(
    database: &crate::core::database::Database,
    table: &str,
) -> Option<(Vec<String>, Vec<(String, String)>)> {
    let view = database.schema_view(table)?;
    let schema = view.schema();
    let columns: Vec<String> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .map(|f| f.name.clone())
        .collect();
    let defaults: Vec<(String, String)> = columns
        .iter()
        .filter_map(|c| {
            let d = schema.defaults.iter().find(|d| !d.is_deleted && d.column.eq_ignore_ascii_case(c))?;
            (d.identity.is_none() && varies(&d.expression)).then(|| (c.clone(), d.expression.clone()))
        })
        .collect();
    Some((columns, defaults))
}

fn refusal(column: &str, expression: &str, place: &str, instead: &str) -> Fold {
    Fold::Refused(format!(
        "column \"{column}\"'s DEFAULT {expression} would be evaluated again, to a different value, on every \
         node replicating {place}: replication sends the statement, not the rows. {instead}"
    ))
}

fn fold_insert_defaults(sql: &str, database: &crate::core::database::Database) -> Fold {
    let t = sql.trim().trim_end_matches(';').trim_end();
    let Some(rest) = strip_word(t, "INSERT").and_then(|r| strip_word(r, "INTO")) else {
        return Fold::Unchanged;
    };
    let Some((table_text, mut rest)) = take_identifier(rest) else {
        return Fold::Unchanged;
    };
    let table = stored_name(table_text);
    let Some((columns, varying)) = varying_defaults(database, &table) else {
        return Fold::Unchanged;
    };
    if varying.is_empty() {
        return Fold::Unchanged;
    }
    // `INSERT INTO t AS alias`.
    if let Some(after) = strip_word(rest, "AS") {
        let Some((_, after)) = take_identifier(after) else { return Fold::Unchanged };
        rest = after;
    }
    let head_end = t.len() - rest.len();
    let rest_trimmed = rest.trim_start();
    let (listed, mut rest) = if rest_trimmed.starts_with('(') {
        let Some(close) = matching_paren(rest_trimmed.as_bytes(), 0) else { return Fold::Unchanged };
        let names: Vec<String> = split_top_commas(&rest_trimmed[1..close]).iter().map(|n| stored_name(n)).collect();
        (Some(names), &rest_trimmed[close + 1..])
    } else {
        (None, rest_trimmed)
    };
    // `OVERRIDING SYSTEM VALUE` / `OVERRIDING USER VALUE` stays where it is.
    let mut overriding = "";
    if let Some(after) = strip_word(rest, "OVERRIDING") {
        let after = after.trim_start();
        let word_end = after.find(char::is_whitespace).unwrap_or(after.len());
        let Some(after2) = strip_word(&after[word_end..], "VALUE") else { return Fold::Unchanged };
        overriding = &rest[..rest.len() - after2.len()];
        rest = after2;
    }
    let body = rest.trim_start();
    // `ON CONFLICT`, not any `ON`: a query's join has one at its top level.
    let on_conflict = {
        let mut from = 0usize;
        let mut found = None;
        while let Some(at) = top_level_keyword(&body[from..], "ON") {
            let at = from + at;
            if strip_word(&body[at + 2..], "CONFLICT").is_some() {
                found = Some(at);
                break;
            }
            from = at + 2;
        }
        found
    };
    let tail_at = [on_conflict, top_level_keyword(body, "RETURNING")]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(body.len());
    let (body, tail) = (body[..tail_at].trim_end(), &body[tail_at..]);
    let quote = |c: &str| crate::core::rql::ctas::quote_ident(c);

    // DEFAULT VALUES: the varying columns, with their defaults.
    if let Some(after) = strip_word(body, "DEFAULT") {
        if strip_word(after, "VALUES").is_none() {
            return Fold::Unchanged;
        }
        let names: Vec<String> = varying.iter().map(|(c, _)| quote(c)).collect();
        let values: Vec<&str> = varying.iter().map(|(_, e)| e.as_str()).collect();
        return Fold::Rewritten(format!(
            "{} ({}) {}VALUES ({}) {}",
            &t[..head_end].trim_end(),
            names.join(", "),
            overriding,
            values.join(", "),
            tail
        ));
    }

    if let Some(values_text) = strip_word(body, "VALUES") {
        // Each tuple, its items split.
        let mut tuples: Vec<Vec<String>> = Vec::new();
        let bytes = values_text.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => {
                    let Some(close) = matching_paren(bytes, i) else { return Fold::Unchanged };
                    tuples.push(split_top_commas(&values_text[i + 1..close]).iter().map(|s| s.trim().to_string()).collect());
                    i = close + 1;
                }
                b',' | b' ' | b'\t' | b'\n' | b'\r' => i += 1,
                _ => return Fold::Unchanged,
            }
        }
        let Some(arity) = tuples.first().map(Vec::len) else { return Fold::Unchanged };
        let named: Vec<String> = match &listed {
            Some(names) => names.clone(),
            None if arity <= columns.len() => columns[..arity].to_vec(),
            None => return Fold::Unchanged,
        };
        if tuples.iter().any(|tuple| tuple.len() != named.len()) {
            return Fold::Unchanged;
        }
        let missing: Vec<&(String, String)> =
            varying.iter().filter(|(c, _)| !named.iter().any(|n| n.eq_ignore_ascii_case(c))).collect();
        let mut changed = !missing.is_empty();
        for tuple in &mut tuples {
            for (position, item) in tuple.iter_mut().enumerate() {
                if item.eq_ignore_ascii_case("DEFAULT") {
                    if let Some((_, e)) = varying.iter().find(|(c, _)| c.eq_ignore_ascii_case(&named[position])) {
                        *item = e.clone();
                        changed = true;
                    }
                }
            }
            tuple.extend(missing.iter().map(|(_, e)| e.clone()));
        }
        if !changed {
            return Fold::Unchanged;
        }
        let mut names: Vec<String> = named.iter().map(|c| quote(c)).collect();
        names.extend(missing.iter().map(|(c, _)| quote(c)));
        let rows: Vec<String> = tuples.iter().map(|tuple| format!("({})", tuple.join(", "))).collect();
        return Fold::Rewritten(format!(
            "{} ({}) {}VALUES {} {}",
            &t[..head_end].trim_end(),
            names.join(", "),
            overriding,
            rows.join(", "),
            tail
        ));
    }

    // A query: `SELECT …`, `WITH … SELECT`, `(SELECT …)`, `TABLE t`.
    let Some(named) = listed.clone().or_else(|| {
        let select_at = top_level_keyword(body, "SELECT")?;
        let from_at = top_level_keyword(body, "FROM").unwrap_or(body.len());
        if from_at < select_at {
            return None;
        }
        let items = split_top_commas(&body[select_at + "SELECT".len()..from_at]);
        if items.iter().any(|i| i.trim() == "*" || i.trim().ends_with(".*")) || items.len() > columns.len() {
            return None;
        }
        Some(columns[..items.len()].to_vec())
    }) else {
        // Which columns the query fills is unknown: left as it was.
        return Fold::Unchanged;
    };
    let missing: Vec<&(String, String)> =
        varying.iter().filter(|(c, _)| !named.iter().any(|n| n.eq_ignore_ascii_case(c))).collect();
    if missing.is_empty() {
        return Fold::Unchanged;
    }
    if let Some((column, expression)) = missing.iter().find(|(_, e)| varies_per_row(e)) {
        return refusal(
            column,
            expression,
            "this INSERT … SELECT",
            "Name the column and compute its value in the query, the same on every node (for example \
             from a key the rows already have), or insert the rows with VALUES, where it is taken once per row",
        );
    }
    let mut names: Vec<String> = named.iter().map(|c| quote(c)).collect();
    names.extend(missing.iter().map(|(c, _)| quote(c)));
    let extra: Vec<&str> = missing.iter().map(|(_, e)| e.as_str()).collect();
    Fold::Rewritten(format!(
        "{} ({}) {}SELECT __rv.*, {} FROM ({}) __rv {}",
        &t[..head_end].trim_end(),
        names.join(", "),
        overriding,
        extra.join(", "),
        body,
        tail
    ))
}

/// `UPDATE t SET c = DEFAULT` where `c`'s default varies per row.
fn refuse_update_default(sql: &str, database: &crate::core::database::Database) -> Fold {
    let t = sql.trim();
    let Some(rest) = strip_word(t, "UPDATE") else { return Fold::Unchanged };
    let rest = strip_word(rest, "ONLY").unwrap_or(rest);
    let Some((table_text, rest)) = take_identifier(rest) else { return Fold::Unchanged };
    let Some((_, varying)) = varying_defaults(database, &stored_name(table_text)) else { return Fold::Unchanged };
    let Some(set_at) = top_level_keyword(rest, "SET") else { return Fold::Unchanged };
    let after = &rest[set_at + "SET".len()..];
    let end = ["FROM", "WHERE", "RETURNING"].iter().filter_map(|k| top_level_keyword(after, k)).min().unwrap_or(after.len());
    for item in split_top_commas(&after[..end]) {
        let Some((lhs, rhs)) = item.split_once('=') else { continue };
        if !rhs.trim().eq_ignore_ascii_case("DEFAULT") {
            continue;
        }
        let column = stored_name(lhs);
        if let Some((c, e)) = varying.iter().find(|(c, _)| c.eq_ignore_ascii_case(&column)) {
            if varies_per_row(e) {
                return refusal(c, e, "this UPDATE", "Compute the value in the client and assign it");
            }
        }
    }
    Fold::Unchanged
}

/// `ALTER TABLE … ADD [COLUMN] c type … DEFAULT <varying>`: its backfill
/// evaluates the default for every existing row, on each node.
///
/// * a clock default (`now()`, `CURRENT_TIMESTAMP`) is one instant for every
///   row: the column is added with that instant as a literal default, and the
///   same statement then sets the declared one (`ADD COLUMN c … DEFAULT
///   '<instant>', ALTER COLUMN c SET DEFAULT now()`), so every node backfills
///   the same value and later rows still take `now()`;
/// * a per-row one (`gen_random_uuid()`) is refused while the table may hold
///   rows; on an empty table it fills nothing and runs as written;
/// * `ADD COLUMN IF NOT EXISTS` for a column the table has does nothing, and
///   runs as written.
async fn fold_add_column_default(sql: &str, database: &crate::core::database::Database) -> Fold {
    let t = sql.trim().trim_end_matches(';').trim_end();
    let Some(rest) = strip_word(t, "ALTER").and_then(|r| strip_word(r, "TABLE")) else { return Fold::Unchanged };
    let rest = strip_word(rest, "IF").and_then(|r| strip_word(r, "EXISTS")).unwrap_or(rest);
    let rest = strip_word(rest, "ONLY").unwrap_or(rest);
    let Some((table_text, actions_text)) = take_identifier(rest) else { return Fold::Unchanged };
    let table = stored_name(table_text);
    let actions_at = t.len() - actions_text.len();
    let Some(view) = database.schema_view(&table) else { return Fold::Unchanged };
    let existing: Vec<String> = view
        .schema()
        .fields
        .iter()
        .filter(|f| !f.is_deleted)
        .map(|f| f.name.to_ascii_lowercase())
        .collect();
    drop(view);

    let mut actions: Vec<String> = Vec::new();
    let mut appended: Vec<String> = Vec::new();
    let mut changed = false;
    for action in split_top_commas(actions_text) {
        let action = action.trim();
        let Some(after_add) = strip_word(action, "ADD") else {
            actions.push(action.to_string());
            continue;
        };
        let after_column = strip_word(after_add, "COLUMN").unwrap_or(after_add);
        let if_not_exists = strip_word(after_column, "IF")
            .and_then(|r| strip_word(r, "NOT"))
            .and_then(|r| strip_word(r, "EXISTS"));
        let definition = if_not_exists.unwrap_or(after_column);
        let is_constraint = ["CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE"]
            .iter()
            .any(|k| strip_word(definition, k).is_some());
        let (without_default, expression) = crate::core::rql::column_default::split_clause(definition);
        let varying = expression.as_deref().filter(|e| varies(e));
        let (Some(expression), false) = (varying, is_constraint) else {
            actions.push(action.to_string());
            continue;
        };
        let column = take_identifier(definition).map(|(c, _)| stored_name(c)).unwrap_or_default();
        if if_not_exists.is_some() && existing.iter().any(|c| c.eq_ignore_ascii_case(&column)) {
            // Does nothing on any node.
            actions.push(action.to_string());
            continue;
        }
        if varies_per_row(expression) {
            if table_may_have_rows(database, &table).await {
                return refusal(
                    &column,
                    expression,
                    "this ALTER TABLE (its backfill fills every existing row)",
                    "Add the column without the DEFAULT (NULL allowed), fill the existing rows with values \
                     computed in the client, then ALTER COLUMN … SET NOT NULL and SET DEFAULT, which fill no row. \
                     On an empty table it runs as written",
                );
            }
            actions.push(action.to_string());
            continue;
        }
        // A clock default: one instant, as the backfill would take anyway.
        let Some(instant) = evaluate_as_text(expression) else {
            return refusal(
                &column,
                expression,
                "this ALTER TABLE (its backfill fills every existing row)",
                "Add the column with a constant DEFAULT, then ALTER COLUMN … SET DEFAULT",
            );
        };
        let head = &after_add[..after_add.len() - after_column.len()];
        let keywords = if if_not_exists.is_some() { "IF NOT EXISTS " } else { "" };
        actions.push(format!(
            "ADD {}{}{} DEFAULT {}",
            head,
            keywords,
            without_default.trim(),
            crate::core::value_codec::quote_sql(&instant)
        ));
        appended.push(format!(
            "ALTER COLUMN {} SET DEFAULT {}",
            crate::core::rql::ctas::quote_ident(&column),
            expression
        ));
        changed = true;
    }
    if !changed {
        return Fold::Unchanged;
    }
    actions.extend(appended);
    Fold::Rewritten(format!("{} {}", t[..actions_at].trim_end(), actions.join(", ")))
}

/// Whether `table` may hold a row: a plain table with any pointer record
/// (a deleted row's included); a partitioned one, always.
async fn table_may_have_rows(database: &crate::core::database::Database, table: &str) -> bool {
    use crate::core::storage_providers::traits::StorageIO;
    match database.relation(table) {
        Some(crate::core::database::relation::Relation::Plain(t)) => match t.data() {
            Ok(data) => data.io_pointers.get_len().await > 0,
            Err(_) => true,
        },
        _ => true,
    }
}

/// `text` past a leading `word` (any case) and the whitespace after it.
fn strip_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let t = text.trim_start();
    let head = t.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    let after = &t[word.len()..];
    if after.as_bytes().first().is_some_and(|b| crate::core::sql_scan::is_identifier_byte(*b)) {
        return None;
    }
    Some(after.trim_start())
}

/// `call`'s value as PostgreSQL's text for it, evaluated now, under the
/// running statement's clock.
fn evaluate_as_text(call: &str) -> Option<String> {
    let expr = crate::core::rql::expr::parse(&format!("CAST({call} AS TEXT)"), &[]).ok()?;
    let empty = crate::core::row::row::Row {
        position: 0,
        length: 0,
        columns: smallvec::SmallVec::new(),
    };
    match crate::core::rql::expr::eval_output(&expr, &empty, &[]).ok()? {
        crate::core::rql::expr::Value::Data { bytes, .. } => {
            let text = String::from_utf8(bytes).ok()?;
            Some(text.trim_end_matches('\0').to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volatile_calls_are_told_from_stable_ones() {
        assert!(calls_volatile("random() < 0.5"));
        assert!(calls_volatile("gen_random_uuid()::text"));
        assert!(!calls_volatile("now() - interval '1 day'"));
        assert!(!calls_volatile("'random()'"));
        assert!(!calls_volatile("random_col < 0.5"));
        // Never folded to one value for the statement.
        let folded = crate::core::rql::executor::fold_where_constants("SELECT id FROM t WHERE random() < 0.5 AND id < 1 + 1");
        assert_eq!(folded, "SELECT id FROM t WHERE random() < 0.5 AND id < 2");
    }

    fn rewritten(sql: &str) -> String {
        match fold(sql) {
            Fold::Rewritten(s) => s,
            other => panic!("{sql}: {other:?}"),
        }
    }

    #[test]
    fn reads_and_plain_writes_are_untouched() {
        assert_eq!(fold("SELECT now(), random()"), Fold::Unchanged);
        assert_eq!(fold("INSERT INTO t (a) VALUES (1)"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET note = 'now() and random()' WHERE id = 1"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET now_at = 1 WHERE random_id = 2"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET a = x.now WHERE x.random = 2"), Fold::Unchanged);
    }

    #[tokio::test]
    async fn clock_functions_become_the_statements_instant_everywhere() {
        let clock = crate::core::statement_clock::StatementClock {
            transaction_us: 1_700_000_000_123_456,
            statement_us: 1_700_000_005_000_000,
        };
        let sql = "UPDATE t SET at = now(), d = CURRENT_DATE, s = statement_timestamp() WHERE at < CURRENT_TIMESTAMP RETURNING now()";
        let out = crate::core::statement_clock::scope(clock, async { rewritten(sql) }).await;
        assert_eq!(
            out,
            "UPDATE t SET at = '2023-11-14 22:13:20.123456+00'::timestamptz, d = '2023-11-14'::date, \
             s = '2023-11-14 22:13:25+00'::timestamptz WHERE at < '2023-11-14 22:13:20.123456+00'::timestamptz RETURNING now()"
        );
    }

    #[test]
    fn a_per_call_function_is_taken_once_in_values_and_refused_elsewhere() {
        let out = rewritten("INSERT INTO t (id, r, u) VALUES (1, random(), gen_random_uuid()), (2, random(), gen_random_uuid())");
        assert!(!out.contains("random()") && !out.contains("gen_random_uuid()"), "{out}");
        assert_eq!(out.matches("::float8").count(), 2);
        assert_eq!(out.matches("::uuid").count(), 2);
        let parts: Vec<&str> = out.split("::uuid").collect();
        assert_ne!(parts[0].rsplit('\'').nth(1), parts[1].rsplit('\'').nth(1), "each slot is its own call");

        for sql in [
            "UPDATE t SET r = random() WHERE id < 10",
            "DELETE FROM t WHERE random() < 0.5",
            "INSERT INTO t (id, r) SELECT id, random() FROM u",
            "INSERT INTO t (id, r) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET r = random()",
            "WITH x AS (SELECT 1) UPDATE t SET u = gen_random_uuid()",
        ] {
            assert!(matches!(fold(sql), Fold::Refused(_)), "{sql}");
        }
        // RETURNING changes no row.
        assert!(matches!(fold("UPDATE t SET a = 1 RETURNING random()"), Fold::Unchanged));
    }

    /// A column's non-deterministic DEFAULT is written into the write the
    /// peers run, once per tuple; where it cannot be (a query's rows, a
    /// backfill, `SET c = DEFAULT`), the write is refused. Before, each node
    /// evaluated the default itself, and every row had a different UUID on
    /// every node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_varying_default_is_written_into_the_write_or_refused() {
        use crate::core::rql::executor::execute;
        use crate::core::rql::lexer_s1::recognize_query_purpose;
        let dir = tempfile::tempdir().unwrap();
        let db = rclite::Arc::new(
            crate::core::database::Database::new_named(&dir.path().to_string_lossy(), false, "replvaluesdefaults").await,
        );
        for sql in [
            "CREATE TABLE dv (id INTEGER PRIMARY KEY, u UUID DEFAULT gen_random_uuid(), n TIMESTAMPTZ DEFAULT now(), k TEXT DEFAULT 'x')",
            "CREATE TABLE clock_only (id INTEGER PRIMARY KEY, n TIMESTAMPTZ DEFAULT now())",
            "CREATE TABLE plain (id INTEGER PRIMARY KEY, k TEXT DEFAULT 'x')",
        ] {
            let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
            assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response[1..]));
        }
        let fold_now = |sql: &'static str| {
            let db = db.clone();
            async move { fold_write(sql, &db).await }
        };
        let rewritten = |sql: &'static str| {
            let db = db.clone();
            async move {
                match fold_write(sql, &db).await {
                    Fold::Rewritten(text) => text,
                    other => panic!("{sql}: {other:?}"),
                }
            }
        };

        // Left out: a literal per tuple, each tuple its own UUID; the constant
        // default stays the table's to fill.
        let out = rewritten("INSERT INTO dv (id) VALUES (1), (2)").await;
        assert!(out.contains("\"u\"") || out.contains(" u"), "{out}");
        assert!(!out.contains("gen_random_uuid()") && !out.contains("now()"), "{out}");
        assert_eq!(out.matches("::uuid").count(), 2, "{out}");
        assert_eq!(out.matches("::timestamptz").count(), 2, "{out}");
        assert!(!out.contains("'x'"), "{out}");
        let uuids: Vec<&str> = out.split("::uuid").collect();
        assert_ne!(uuids[0].rsplit('\'').nth(1), uuids[1].rsplit('\'').nth(1), "{out}");

        // `DEFAULT` written in a slot, and DEFAULT VALUES.
        assert!(!rewritten("INSERT INTO dv (id, u) VALUES (3, DEFAULT)").await.contains("DEFAULT"));
        assert!(rewritten("INSERT INTO dv DEFAULT VALUES").await.contains("::uuid"));
        // No column list: the tuple's columns are the table's first ones.
        assert!(rewritten("INSERT INTO dv VALUES (4)").await.contains("::uuid"));
        // RETURNING and ON CONFLICT stay where they were.
        let out = rewritten("INSERT INTO dv (id) VALUES (5) ON CONFLICT (id) DO NOTHING RETURNING id").await;
        assert!(out.ends_with("ON CONFLICT (id) DO NOTHING RETURNING id"), "{out}");

        // A query's rows: a clock default is one instant for all of them; a
        // per-row one is refused.
        let out = rewritten("INSERT INTO clock_only (id) SELECT a.id FROM dv a JOIN dv b ON a.id = b.id").await;
        assert!(out.contains("::timestamptz") && out.contains("ON a.id = b.id"), "{out}");
        assert!(matches!(fold_now("INSERT INTO dv (id) SELECT 6").await, Fold::Refused(m) if m.contains("\"u\"")));
        // SET c = DEFAULT and an ADD COLUMN backfill, refused.
        assert!(matches!(fold_now("UPDATE dv SET u = DEFAULT WHERE id = 1").await, Fold::Refused(_)));
        // dv has rows by now? It has none: the folds above wrote nothing.
        // An empty table's backfill fills nothing, so a per-row default runs.
        assert!(matches!(
            fold_now("ALTER TABLE dv ADD COLUMN w UUID DEFAULT gen_random_uuid()").await,
            Fold::Unchanged
        ));
        let response = execute(recognize_query_purpose("INSERT INTO plain (id) VALUES (1)").unwrap(), db.clone()).await;
        assert_ne!(response.first(), Some(&3));
        assert!(matches!(
            fold_now("ALTER TABLE plain ADD COLUMN w UUID NOT NULL DEFAULT gen_random_uuid()").await,
            Fold::Refused(_)
        ));
        // A clock default backfills one instant, then keeps now().
        let out = rewritten("ALTER TABLE plain ADD COLUMN IF NOT EXISTS at TIMESTAMPTZ NOT NULL DEFAULT now()").await;
        assert!(out.contains("DEFAULT '") && out.ends_with("ALTER COLUMN \"at\" SET DEFAULT now()") || out.ends_with("ALTER COLUMN at SET DEFAULT now()"), "{out}");
        // IF NOT EXISTS for a column the table has: nothing to fill.
        assert!(matches!(
            fold_now("ALTER TABLE plain ADD COLUMN IF NOT EXISTS k UUID DEFAULT gen_random_uuid()").await,
            Fold::Unchanged
        ));
        assert!(matches!(fold_now("ALTER TABLE dv ADD COLUMN z INTEGER DEFAULT 7").await, Fold::Unchanged));
        // A table without one, and a write that names the column: as written.
        assert!(matches!(fold_now("INSERT INTO plain (id) VALUES (1)").await, Fold::Unchanged));
        assert!(matches!(
            fold_now("INSERT INTO dv (id, u, n) VALUES (7, '00000000-0000-0000-0000-000000000007', '2020-01-01')").await,
            Fold::Unchanged
        ));
        drop(db);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }
}
