//! Domains, `CREATE DOMAIN name AS base [DEFAULT …] [NOT NULL] [[CONSTRAINT
//! n] CHECK (…)]…`, kept above the engine as enum types are (`user_types`).
//!
//! A domain is a row of `__rdb_domains` (its name and its definition as
//! written after `AS`, re-read on use). A column declared with it is its
//! base type with the domain's `NOT NULL`, each `CHECK` as a constraint of
//! the column (`VALUE` read as the column) named
//! `__domain_<domain>__<column>__<check>`, and the domain's `DEFAULT` unless
//! the column has its own; a domain with no check gets a `CHECK (TRUE)`
//! marker, so every column of one is known (`DROP DOMAIN`'s dependency).
//! A cast to a domain (`x::posint`, `CAST(x AS posint)`) is a cast to its
//! base type, checked as PostgreSQL's `domain_in` checks it when the domain
//! has constraints: `__rdb_domain_value((x)::int, 'posint', FALSE,
//! 'posint_check', CASE WHEN NOT (((x)::int) > 0) THEN 0 ELSE 1 END)`,
//! which answers the value or fails. A
//! violated check reports PostgreSQL's 23514, `value for domain posint
//! violates check constraint "posint_check"`; a NULL cast to a `NOT NULL`
//! domain its 23502, `domain d does not allow null values`.
//!
//! Not done (docs/postgres-compatibility.md): `ALTER DOMAIN`; `pg_type`
//! lists no domain; a domain column's `NOT NULL` reports the column's 23502
//! message.

use std::sync::atomic::Ordering;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::rql::user_types::{fold, refusal, Refusal};
use crate::core::sql_scan::{is_identifier_byte, matching_close, quoted_end, skip_comment, skip_quoted};

pub const DOMAIN_TABLE: &str = "__rdb_domains";
const DOMAIN_TABLE_DDL: &str = "CREATE TABLE __rdb_domains (name TEXT PRIMARY KEY, definition TEXT NOT NULL)";

/// The prefix of a domain column's constraints.
pub const CONSTRAINT_PREFIX: &str = "__domain_";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Domain {
    pub name: String,
    pub base: String,
    pub not_null: bool,
    pub default: Option<String>,
    /// `(name, expression over VALUE)`.
    pub checks: Vec<(String, String)>,
}

impl Domain {
    /// The definition as stored: what follows `AS`.
    pub fn definition(&self) -> String {
        let mut out = self.base.clone();
        if let Some(d) = &self.default {
            out.push_str(&format!(" DEFAULT {d}"));
        }
        if self.not_null {
            out.push_str(" NOT NULL");
        }
        for (name, expr) in &self.checks {
            out.push_str(&format!(" CONSTRAINT {} CHECK ({expr})", crate::core::rql::views::quote_ident(name)));
        }
        out
    }

    /// What a column `column` of this domain is declared as, in place of the
    /// domain's name: the base type, `NOT NULL`, the checks. Its `DEFAULT`
    /// goes at the definition's end ([`rewrite_with`]), as the engine reads
    /// a `DEFAULT` to the end of a definition.
    pub fn column_declaration(&self, column: &str) -> String {
        use crate::core::rql::views::quote_ident;
        let mut out = self.base.clone();
        if self.not_null {
            out.push_str(" NOT NULL");
        }
        if self.checks.is_empty() {
            out.push_str(&format!(" CONSTRAINT {} CHECK (TRUE)", quote_ident(&format!("{CONSTRAINT_PREFIX}{}__{column}", self.name))));
        }
        for (name, expr) in &self.checks {
            let constraint = format!("{CONSTRAINT_PREFIX}{}__{column}__{name}", self.name);
            out.push_str(&format!(" CONSTRAINT {} CHECK ({})", quote_ident(&constraint), value_as(expr, &quote_ident(column))));
        }
        out
    }
}

/// `expr` with each `VALUE` (outside quotes) read as `column`.
fn value_as(expr: &str, column: &str) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len() + 16);
    let mut i = 0;
    let mut copied = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.');
        if boundary
            && bytes.len() >= i + 5
            && bytes[i..i + 5].eq_ignore_ascii_case(b"VALUE")
            && bytes.get(i + 5).is_none_or(|b| !is_identifier_byte(*b))
        {
            out.push_str(&expr[copied..i]);
            out.push_str(column);
            i += 5;
            copied = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&expr[copied..]);
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainStatement {
    Create(Domain),
    Drop { names: Vec<String>, if_exists: bool, cascade: bool },
    Alter { name: String },
}

fn starts_word(text: &str, word: &str) -> bool {
    let t = text.trim_start();
    t.len() >= word.len()
        && t.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes())
        && t.as_bytes().get(word.len()).is_none_or(|b| !is_identifier_byte(*b))
}

fn after_word<'a>(text: &'a str, word: &str) -> &'a str {
    text.trim_start()[word.len()..].trim_start()
}

/// A name at the front of `text` (`d`, `"D"`, `public.d`), folded, and the rest.
fn name_at(text: &str) -> Option<(String, &str)> {
    let t = text.trim_start();
    let bytes = t.as_bytes();
    let mut end = if bytes.first() == Some(&b'"') {
        quoted_end(bytes, 0, b'"')
    } else {
        bytes.iter().take_while(|b| is_identifier_byte(**b)).count()
    };
    if end == 0 {
        return None;
    }
    let mut name = fold(&t[..end]);
    if bytes.get(end) == Some(&b'.') && (name == "public" || name == "pg_catalog") {
        let rest = &t[end + 1..];
        let b = rest.as_bytes();
        let n = if b.first() == Some(&b'"') { quoted_end(b, 0, b'"') } else { b.iter().take_while(|c| is_identifier_byte(**c)).count() };
        if n == 0 {
            return None;
        }
        name = fold(&rest[..n]);
        end = end + 1 + n;
    }
    Some((name, &t[end..]))
}

/// `CREATE DOMAIN`, `DROP DOMAIN`, `ALTER DOMAIN`; `None` for any other
/// statement.
pub fn parse_domain_statement(sql: &str) -> Option<Result<DomainStatement, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let (verb, rest) = if starts_word(text, "CREATE") {
        ("CREATE", after_word(text, "CREATE"))
    } else if starts_word(text, "DROP") {
        ("DROP", after_word(text, "DROP"))
    } else if starts_word(text, "ALTER") {
        ("ALTER", after_word(text, "ALTER"))
    } else {
        return None;
    };
    if !starts_word(rest, "DOMAIN") {
        return None;
    }
    let rest = after_word(rest, "DOMAIN");
    Some(match verb {
        "CREATE" => parse_create(rest),
        "DROP" => parse_drop(rest),
        _ => match name_at(rest) {
            Some((name, _)) => Ok(DomainStatement::Alter { name }),
            None => Err(refusal("42601", "syntax error at or near \"DOMAIN\"")),
        },
    })
}

fn parse_drop(rest: &str) -> Result<DomainStatement, Refusal> {
    let mut rest = rest;
    let if_exists = starts_word(rest, "IF");
    if if_exists {
        rest = after_word(rest, "IF");
        if !starts_word(rest, "EXISTS") {
            return Err(refusal("42601", "syntax error at or near \"IF\""));
        }
        rest = after_word(rest, "EXISTS");
    }
    let mut names = Vec::new();
    loop {
        let Some((name, after)) = name_at(rest) else { return Err(refusal("42601", "syntax error in DROP DOMAIN")) };
        names.push(name);
        let after = after.trim_start();
        match after.strip_prefix(',') {
            Some(more) => rest = more,
            None => {
                rest = after;
                break;
            }
        }
    }
    let cascade = starts_word(rest, "CASCADE");
    if !(rest.is_empty() || cascade || starts_word(rest, "RESTRICT")) {
        return Err(refusal("42601", format!("syntax error at or near \"{}\"", rest.split_whitespace().next().unwrap_or(""))));
    }
    Ok(DomainStatement::Drop { names, if_exists, cascade })
}

fn parse_create(rest: &str) -> Result<DomainStatement, Refusal> {
    let Some((name, mut rest)) = name_at(rest) else { return Err(refusal("42601", "syntax error in CREATE DOMAIN")) };
    if starts_word(rest, "AS") {
        rest = after_word(rest, "AS");
    }
    parse_definition(&name, rest).map(DomainStatement::Create)
}

/// The clauses that end a domain's base type or default.
const CLAUSES: &[&str] = &["COLLATE", "DEFAULT", "CONSTRAINT", "NOT", "NULL", "CHECK"];

/// Where the next clause starts in `text` at depth zero, or its end.
fn clause_end(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            _ => {}
        }
        if depth == 0 && (i == 0 || !is_identifier_byte(bytes[i - 1])) && CLAUSES.iter().any(|k| starts_word(&text[i..], k)) {
            return i;
        }
        i += 1;
    }
    text.len()
}

/// A domain's definition after `AS` (stored, and written by `CREATE`).
pub fn parse_definition(name: &str, text: &str) -> Result<Domain, Refusal> {
    let text = text.trim();
    let base_end = clause_end(text);
    let base = text[..base_end].trim().to_string();
    if base.is_empty() {
        return Err(refusal("42601", "syntax error in CREATE DOMAIN: no base type"));
    }
    let mut domain = Domain { name: name.to_string(), base, not_null: false, default: None, checks: Vec::new() };
    let mut rest = &text[base_end..];
    let mut pending_name: Option<String> = None;
    let mut auto = 0usize;
    while !rest.trim().is_empty() {
        if starts_word(rest, "COLLATE") {
            let after = after_word(rest, "COLLATE");
            let (_, r) = name_at(after).ok_or_else(|| refusal("42601", "syntax error at or near \"COLLATE\""))?;
            rest = r;
        } else if starts_word(rest, "DEFAULT") {
            let after = after_word(rest, "DEFAULT");
            let end = clause_end(after);
            domain.default = Some(after[..end].trim().to_string());
            rest = &after[end..];
        } else if starts_word(rest, "CONSTRAINT") {
            let (n, r) = name_at(after_word(rest, "CONSTRAINT")).ok_or_else(|| refusal("42601", "syntax error at or near \"CONSTRAINT\""))?;
            pending_name = Some(n);
            rest = r;
        } else if starts_word(rest, "NOT") {
            let after = after_word(rest, "NOT");
            if !starts_word(after, "NULL") {
                return Err(refusal("42601", "syntax error at or near \"NOT\""));
            }
            domain.not_null = true;
            pending_name = None;
            rest = after_word(after, "NULL");
        } else if starts_word(rest, "NULL") {
            domain.not_null = false;
            pending_name = None;
            rest = after_word(rest, "NULL");
        } else if starts_word(rest, "CHECK") {
            let after = after_word(rest, "CHECK");
            if !after.starts_with('(') {
                return Err(refusal("42601", "syntax error at or near \"CHECK\""));
            }
            let close = matching_close(after, 0).ok_or_else(|| refusal("42601", "unterminated CHECK in CREATE DOMAIN"))?;
            let expr = after[1..close].trim().to_string();
            let check_name = pending_name.take().unwrap_or_else(|| {
                let n = if auto == 0 { format!("{name}_check") } else { format!("{name}_check{auto}") };
                auto += 1;
                n
            });
            domain.checks.push((check_name, expr));
            rest = &after[close + 1..];
        } else {
            return Err(refusal(
                "42601",
                format!("syntax error at or near \"{}\"", rest.split_whitespace().next().unwrap_or("")),
            ));
        }
    }
    Ok(domain)
}

// ---------------------------------------------------------------------------
// Storage

pub async fn ensure_loaded(database: &Arc<Database>) {
    if database.domains.loaded.load(Ordering::Acquire) {
        return;
    }
    if database.has_table(DOMAIN_TABLE)
        && let Ok(all) = load(database, None, None).await
    {
        for d in all {
            database.domains.note(&d.name);
        }
    }
    database.domains.loaded.store(true, Ordering::Release);
}

/// The domains `tx` sees whose names are in `names`, or all of them.
pub async fn load(database: &Arc<Database>, names: Option<&[String]>, tx: Option<TxId>) -> Result<Vec<Domain>, String> {
    if !database.has_table(DOMAIN_TABLE) {
        return Ok(Vec::new());
    }
    let filter = match names {
        Some([]) => return Ok(Vec::new()),
        Some(names) => format!(" WHERE name IN ({})", names.iter().map(|n| crate::auth::quote(n)).collect::<Vec<_>>().join(", ")),
        None => String::new(),
    };
    let response = crate::core::rql::user_types::run(database, &format!("SELECT name, definition FROM {DOMAIN_TABLE}{filter}"), tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in rows {
        let text = |i: usize| -> String {
            row.columns
                .get(i)
                .filter(|c| c.column_type != crate::core::db_type::DbType::NULL)
                .map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                .unwrap_or_default()
        };
        let name = text(0);
        if let Ok(d) = parse_definition(&name, &text(1)) {
            out.push(d);
        }
    }
    Ok(out)
}

pub async fn store(database: &Arc<Database>, domain: &Domain, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(DOMAIN_TABLE) {
        crate::core::rql::user_types::run(database, DOMAIN_TABLE_DDL, None).await?;
    }
    let sql = format!(
        "INSERT INTO {DOMAIN_TABLE} (name, definition) VALUES ({}, {})",
        quote(&domain.name),
        quote(&domain.definition())
    );
    crate::core::rql::user_types::run(database, &sql, tx).await?;
    database.domains.note(&domain.name);
    Ok(())
}

pub async fn remove(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> Result<(), String> {
    crate::core::rql::user_types::run(database, &format!("DELETE FROM {DOMAIN_TABLE} WHERE name = {}", crate::auth::quote(name)), tx)
        .await
        .map(|_| ())
}

/// Every domain column: (table, column, domain), from the constraints.
pub fn domain_columns(database: &Database) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = Vec::new();
    for table in database.table_names() {
        let Some(view) = database.schema_view(&table) else { continue };
        for check in &view.schema().checks {
            if check.is_deleted {
                continue;
            }
            let Some(rest) = check.name.strip_prefix(CONSTRAINT_PREFIX) else { continue };
            let Some((domain, rest)) = rest.split_once("__") else { continue };
            let column = rest.split("__").next().unwrap_or(rest).to_string();
            if !out.iter().any(|(t, c, d)| *t == table && *c == column && d == domain) {
                out.push((table.clone(), column, domain.to_string()));
            }
        }
    }
    out
}

/// A violated domain check as PostgreSQL reports it: 23514, naming the
/// domain and its constraint.
pub fn violation(message: &str) -> Option<String> {
    let at = message.find(&format!("violates check constraint \"{CONSTRAINT_PREFIX}"))?;
    let name_at = at + "violates check constraint \"".len() + CONSTRAINT_PREFIX.len();
    let rest = &message[name_at..];
    let name = &rest[..rest.find('"')?];
    let (domain, rest) = name.split_once("__")?;
    let (_, check) = rest.split_once("__")?;
    Some(format!(
        "value for domain {} violates check constraint \"{check}\"",
        crate::core::rql::user_types::display_name(domain)
    ))
}

// ---------------------------------------------------------------------------
// Rewriting statements

/// A domain's name at `sql[at..]`, with where it ends.
fn domain_at<'d>(sql: &str, at: usize, defs: &'d [Domain]) -> Option<(&'d Domain, usize)> {
    let (name, rest) = name_at(&sql[at..])?;
    let end = sql.len() - rest.len();
    if sql.as_bytes().get(end).is_some_and(|b| is_identifier_byte(*b) || *b == b'.' || *b == b'(' || *b == b'[') {
        return None;
    }
    defs.iter().find(|d| d.name == name).map(|d| (d, end))
}

/// `sql` with each use of a domain it names in its stored form; `None` when
/// it names none.
pub async fn rewrite(database: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<Option<String>, Refusal> {
    ensure_loaded(database).await;
    let mentioned = database.domains.mentioned_in(sql);
    if mentioned.is_empty() {
        return Ok(None);
    }
    let defs = load(database, Some(&mentioned), tx).await.map_err(|e| refusal("XX000", e))?;
    if defs.is_empty() {
        return Ok(None);
    }
    rewrite_with(sql, &defs)
}

/// [`rewrite`] over the domains `defs`.
pub fn rewrite_with(sql: &str, defs: &[Domain]) -> Result<Option<String>, Refusal> {
    if parse_domain_statement(sql).is_some() {
        return Ok(None);
    }
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    // Column definitions.
    if let Some(found) = crate::core::rql::user_types::column_definitions(sql) {
        for at in &found.type_changes {
            if let Some((d, _)) = domain_at(sql, *at, defs) {
                return Err(refusal(
                    "0A000",
                    format!("changing a column's type to the domain {} is not supported; add a new column of it instead", d.name),
                ));
            }
        }
        for (start, end) in found.definitions {
            let Some((column, type_start)) = crate::core::rql::user_types::column_name_and_type(sql, start, end) else { continue };
            let Some((d, type_end)) = domain_at(sql, type_start, defs) else { continue };
            edits.push((type_start, type_end, d.column_declaration(&column)));
            // The domain's default, unless the column has its own.
            if let Some(default) = &d.default
                && crate::core::rql::join::find_keyword(&sql[type_end..end], "DEFAULT", 0).is_none()
            {
                let end = start + sql[start..end].trim_end().len();
                edits.push((end, end, format!(" DEFAULT {default}")));
            }
        }
    }
    // `x::d` and `CAST(x AS d)`.
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let at = if bytes[i] == b':' && bytes.get(i + 1) == Some(&b':') {
            Some(i + 2 + (sql[i + 2..].len() - sql[i + 2..].trim_start().len()))
        } else if (i == 0 || !is_identifier_byte(bytes[i - 1]))
            && bytes.len() > i + 3
            && bytes[i..i + 2].eq_ignore_ascii_case(b"AS")
            && bytes[i + 2].is_ascii_whitespace()
        {
            Some(i + 2 + (sql[i + 2..].len() - sql[i + 2..].trim_start().len()))
        } else {
            None
        };
        if let Some(at) = at
            && let Some((d, end)) = domain_at(sql, at, defs)
        {
            // `AS d` only inside a CAST: followed by its closing parenthesis.
            let is_cast_as = bytes[i] != b':';
            if !is_cast_as || sql[end..].trim_start().starts_with(')') {
                match checked_cast(sql, i, at, end, d)? {
                    Some(edit) => edits.push(edit),
                    None => edits.push((at, end, d.base.clone())),
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }
    if edits.is_empty() {
        return Ok(None);
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(sql.len() + 64);
    let mut copied = 0;
    for (start, end, text) in edits {
        if start < copied {
            continue;
        }
        out.push_str(&sql[copied..start]);
        out.push_str(&text);
        copied = end;
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// A cast to a domain with constraints as the checked call that answers it:
/// the edit replacing the whole cast, operand included. `None` for a domain
/// with nothing to check (a plain cast to its base), and for an operand this
/// cannot delimit. `cast_at` is the `::` or `AS`, `type_at..type_end` the
/// domain's name.
fn checked_cast(
    sql: &str,
    cast_at: usize,
    type_at: usize,
    type_end: usize,
    d: &Domain,
) -> Result<Option<(usize, usize, String)>, Refusal> {
    if d.checks.is_empty() && !d.not_null {
        return Ok(None);
    }
    let (start, end, operand) = if sql.as_bytes()[cast_at] == b':' {
        let Some(start) = cast_operand_start(sql, cast_at) else { return Ok(None) };
        (start, type_end, sql[start..cast_at].trim())
    } else {
        // `CAST(x AS d)`: from `CAST` to its closing parenthesis.
        let Some(open) = unmatched_open_before(sql, cast_at) else { return Ok(None) };
        let head = sql[..open].trim_end();
        if !(head.len() >= 4 && head[head.len() - 4..].eq_ignore_ascii_case("CAST")) {
            return Ok(None);
        }
        let Some(close) = sql[type_end..].find(')').map(|p| type_end + p) else { return Ok(None) };
        (head.len() - 4, close + 1, sql[open + 1..cast_at].trim())
    };
    let _ = type_at;
    // The operand is read once per check: one whose value changes between
    // readings would be checked as one value and answer another.
    if crate::core::rql::replicated_values::calls_volatile(operand) {
        return Err(refusal(
            "0A000",
            format!("a cast of a volatile expression to the domain {} is not supported yet", d.name),
        ));
    }
    // A `::` operand is a primary expression (`::` binds tightest), written
    // bare so `$1::int` still types its parameter; a CAST's may be any
    // expression, and is parenthesised.
    let value = if sql.as_bytes()[cast_at] == b':' {
        format!("({operand}::{})", d.base)
    } else {
        format!("(({operand})::{})", d.base)
    };
    let mut call = format!(
        "__rdb_domain_value({value}, {}, {}",
        crate::core::value_codec::quote_sql(&crate::core::rql::user_types::display_name(&d.name)),
        if d.not_null { "TRUE" } else { "FALSE" }
    );
    for (name, expr) in &d.checks {
        // As a CASE: a condition is no function argument here. A NULL check
        // passes, as a CHECK constraint's does: `NOT NULL` is NULL, the ELSE.
        call.push_str(&format!(
            ", {}, CASE WHEN NOT ({}) THEN 0 ELSE 1 END",
            crate::core::value_codec::quote_sql(name),
            value_as(expr, &value)
        ));
    }
    call.push(')');
    // A whole select item is named for its type, as PostgreSQL names a cast
    // (`posint`), not for the call that checks it.
    if crate::core::sql_scan::whole_select_item(sql, start, end) {
        call.push_str(&format!(" AS {}", crate::core::rql::user_types::display_name(&d.name)));
    }
    Ok(Some((start, end, call)))
}

/// Where the operand of the `::` at `at` begins: a literal, a quoted or
/// qualified name, a call or parenthesised expression, a subscript, and a
/// cast chain before it (`x::text::d`). `::` binds tighter than any
/// operator, so nothing else belongs to it.
fn cast_operand_start(sql: &str, at: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let back_ws = |mut i: usize| {
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        i
    };
    let mut i = back_ws(at);
    loop {
        if i == 0 {
            return None;
        }
        match bytes[i - 1] {
            b'\'' => {
                // Back over `'…'`, a doubled quote inside it included.
                let mut k = i - 1;
                loop {
                    let p = sql[..k].rfind('\'')?;
                    if p > 0 && bytes[p - 1] == b'\'' {
                        k = p - 1;
                        continue;
                    }
                    k = p;
                    break;
                }
                i = k;
                // `E'…'`, `B'…'`, `X'…'`.
                if i > 0 && matches!(bytes[i - 1], b'E' | b'e' | b'B' | b'b' | b'X' | b'x') && (i < 2 || !is_identifier_byte(bytes[i - 2])) {
                    i -= 1;
                }
            }
            b'"' => {
                i = sql[..i - 1].rfind('"')?;
            }
            b')' | b']' => {
                let (open_byte, close_byte) = if bytes[i - 1] == b')' { (b'(', b')') } else { (b'[', b']') };
                let mut depth = 0usize;
                let mut k = i;
                loop {
                    if k == 0 {
                        return None;
                    }
                    k -= 1;
                    if bytes[k] == close_byte {
                        depth += 1;
                    } else if bytes[k] == open_byte {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                i = k;
                // A call's name, or what a subscript applies to.
                if i > 0 && (is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'"' || bytes[i - 1] == b']' || bytes[i - 1] == b')') {
                    continue;
                }
            }
            b if is_identifier_byte(b) || b == b'.' || b == b'$' => {
                while i > 0 && (is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'$')) {
                    i -= 1;
                    // `t."c"`: the quoted part of a qualified name.
                    if i > 0 && bytes[i - 1] == b'"' {
                        i = sql[..i - 1].rfind('"')?;
                    }
                }
            }
            _ => return None,
        }
        // A cast chain: `x::text::d` casts `x::text`.
        let before = back_ws(i);
        if before >= 2 && &bytes[before - 2..before] == b"::" {
            i = back_ws(before - 2);
            continue;
        }
        return Some(i);
    }
}

/// The `(` before `at` that no `)` between them closes.
fn unmatched_open_before(sql: &str, at: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut k = at;
    while k > 0 {
        k -= 1;
        match bytes[k] {
            b')' => depth += 1,
            b'(' if depth == 0 => return Some(k),
            b'(' => depth -= 1,
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posint() -> Domain {
        match parse_domain_statement("CREATE DOMAIN posint AS int CHECK (VALUE > 0)").unwrap().unwrap() {
            DomainStatement::Create(d) => d,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn domain_statements_parse() {
        assert_eq!(
            posint(),
            Domain { name: "posint".into(), base: "int".into(), not_null: false, default: None, checks: vec![("posint_check".into(), "VALUE > 0".into())] }
        );
        let d = match parse_domain_statement("CREATE DOMAIN \"Code\" AS varchar(10) DEFAULT 'x' NOT NULL CONSTRAINT short CHECK (length(VALUE) < 5) CHECK (VALUE <> '')")
            .unwrap()
            .unwrap()
        {
            DomainStatement::Create(d) => d,
            other => panic!("{other:?}"),
        };
        assert_eq!(d.name, "Code");
        assert_eq!(d.base, "varchar(10)");
        assert_eq!(d.default.as_deref(), Some("'x'"));
        assert!(d.not_null);
        assert_eq!(d.checks, vec![("short".into(), "length(VALUE) < 5".into()), ("Code_check".into(), "VALUE <> ''".into())]);
        // Stored and read back the same.
        assert_eq!(parse_definition("Code", &d.definition()).unwrap(), d);
        assert_eq!(
            parse_domain_statement("DROP DOMAIN IF EXISTS a, public.b CASCADE").unwrap().unwrap(),
            DomainStatement::Drop { names: vec!["a".into(), "b".into()], if_exists: true, cascade: true }
        );
        assert!(parse_domain_statement("CREATE TABLE domain (x int)").is_none());
    }

    #[test]
    fn a_domain_column_is_its_base_type_with_its_constraints() {
        let d = vec![posint(), Domain { name: "nn".into(), base: "text".into(), not_null: true, default: Some("'?'".into()), checks: vec![] }];
        assert_eq!(
            rewrite_with("CREATE TABLE t (id int, x posint, y nn, z nn DEFAULT 'z')", &d).unwrap().as_deref(),
            Some(
                "CREATE TABLE t (id int, x int CONSTRAINT \"__domain_posint__x__posint_check\" CHECK (\"x\" > 0), \
                 y text NOT NULL CONSTRAINT \"__domain_nn__y\" CHECK (TRUE) DEFAULT '?', \
                 z text NOT NULL CONSTRAINT \"__domain_nn__z\" CHECK (TRUE) DEFAULT 'z')"
            )
        );
        let d5 = vec![Domain { name: "five".into(), base: "int".into(), not_null: false, default: Some("5".into()), checks: vec![] }];
        assert_eq!(
            rewrite_with("CREATE TABLE t (x five PRIMARY KEY, y int)", &d5).unwrap().as_deref(),
            Some("CREATE TABLE t (x int CONSTRAINT \"__domain_five__x\" CHECK (TRUE) PRIMARY KEY DEFAULT 5, y int)")
        );
        // A domain with a check: the cast is checked, its operand whole.
        let call = |value: String| {
            format!("__rdb_domain_value({value}, 'posint', FALSE, 'posint_check', CASE WHEN NOT ({value} > 0) THEN 0 ELSE 1 END)")
        };
        // `x::posint`: the operand bare; `CAST(x AS posint)`: parenthesised.
        let checked = |v: &str| call(format!("({v}::int)"));
        let cast = |v: &str| call(format!("(({v})::int)"));
        assert_eq!(
            rewrite_with("SELECT 5::posint, CAST('3' AS posint), x AS posint FROM t", &d).unwrap(),
            Some(format!("SELECT {} AS posint, {} AS posint, x AS posint FROM t", checked("5"), cast("'3'")))
        );
        for (operand, sql) in [
            ("t.a", "SELECT t.a::posint FROM t"),
            ("'it''s'", "SELECT 'it''s'::posint"),
            ("upper(s)::text", "SELECT upper(s)::text::posint FROM t"),
            ("(a + 1)", "SELECT (a + 1)::posint FROM t"),
            ("a[1]", "SELECT a[1]::posint FROM t"),
            ("\"A\"", "SELECT \"A\"::posint FROM t"),
        ] {
            let rewritten = rewrite_with(sql, &d).unwrap().expect(sql);
            assert!(rewritten.contains(&checked(operand)), "{sql}: {rewritten}");
        }
        // `-5::posint` is `-(5::posint)`, as PostgreSQL binds it.
        assert_eq!(rewrite_with("SELECT -5::posint", &d).unwrap(), Some(format!("SELECT -{}", checked("5"))));
        assert_eq!(rewrite_with("SELECT 5::posint + 1", &d).unwrap(), Some(format!("SELECT {} + 1", checked("5"))));
        assert_eq!(rewrite_with("SELECT random()::posint", &d).unwrap_err().0, "0A000");
        // Nothing to check: the base type alone.
        let d5 = vec![match parse_domain_statement("CREATE DOMAIN five AS int DEFAULT 5").unwrap().unwrap() {
            DomainStatement::Create(d) => d,
            other => panic!("{other:?}"),
        }];
        assert_eq!(rewrite_with("SELECT 1::five", &d5).unwrap().as_deref(), Some("SELECT 1::int"));
        assert_eq!(rewrite_with("ALTER TABLE t ALTER COLUMN x TYPE posint", &d).unwrap_err().0, "0A000");
        assert_eq!(rewrite_with("SELECT 'posint'", &d).unwrap(), None);
    }

    #[test]
    fn a_violation_is_postgresql_s() {
        assert_eq!(
            violation("new row for relation \"t\" violates check constraint \"__domain_posint__x__posint_check\"").as_deref(),
            Some("value for domain posint violates check constraint \"posint_check\"")
        );
        assert_eq!(violation("new row for relation \"t\" violates check constraint \"c\""), None);
    }
}
