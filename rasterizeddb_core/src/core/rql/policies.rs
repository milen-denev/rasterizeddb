//! Row-level security (roles plan R3): `CREATE/ALTER/DROP POLICY`, `ALTER
//! TABLE … ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY`, and the
//! predicates a statement is given for the tables that have it.
//!
//! Policies are rows of [`POLICY_TABLE`]. Whether a table has RLS on (and
//! forced) is a marker in the ACL (`RLS`, `RLS FORCE` on the table's key,
//! held by no role), so a statement that touches no such table pays one
//! read of the cached ACL and nothing else.
//!
//! As in PostgreSQL: a table with RLS on and no policy for the command
//! shows a role nothing and lets it write nothing. Permissive policies are
//! ORed, restrictive ones ANDed onto them. Its owner is exempt unless the
//! table is forced, as are superusers and roles with BYPASSRLS. A policy
//! without `WITH CHECK` checks new rows with its `USING`.
//!
//! The predicates go in before the scan (`apply`): a read of the table
//! becomes a read of `(SELECT * FROM t WHERE …)`, and an UPDATE's or a
//! DELETE's own WHERE gains the predicate, so the prefilter and the indexes
//! still serve it. New rows are checked as they are stored, by the
//! row-at-a-time write path (`trigger_exec`), so a default is seen filled.

use super::privileges::{self, AclRow};
use crate::core::database::Database;
use crate::core::mvcc::TxId;
use rclite::Arc;

pub const POLICY_TABLE: &str = "__rdb_policy";

const POLICY_TABLE_DDL: &str = "CREATE TABLE __rdb_policy (\
     id TEXT NOT NULL, \
     tbl TEXT NOT NULL, \
     name TEXT NOT NULL, \
     command TEXT NOT NULL, \
     permissive BOOL NOT NULL, \
     roles TEXT NOT NULL, \
     using_expr TEXT, \
     check_expr TEXT)";

/// The ACL markers on a table's key: RLS on, and on for its owner too.
pub const RLS: &str = "RLS";
pub const RLS_FORCE: &str = "RLS FORCE";

pub type Refusal = (&'static str, String);

/// One policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub table: String,
    pub name: String,
    /// `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`.
    pub command: String,
    pub permissive: bool,
    /// The roles it applies to; `public` for all.
    pub roles: Vec<String>,
    pub using: Option<String>,
    pub check: Option<String>,
}

/// A policy statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyStatement {
    Create(Policy),
    /// `ALTER POLICY name ON t …`: what it changes, `None` where it says
    /// nothing; `rename` for `RENAME TO`.
    Alter { table: String, name: String, rename: Option<String>, roles: Option<Vec<String>>, using: Option<String>, check: Option<String> },
    Drop { table: String, name: String, if_exists: bool },
    /// `ALTER TABLE t ENABLE|DISABLE|FORCE|NO FORCE ROW LEVEL SECURITY`.
    Toggle { table: String, marker: &'static str, on: bool },
}

fn ident(word: &str) -> String {
    let w = word.trim().trim_end_matches(';');
    let w = crate::core::sql_scan::last_name_segment(w);
    if w.starts_with('"') && w.ends_with('"') && w.len() > 1 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

/// The parenthesised expression starting at `text` (after its keyword):
/// its inside and the rest after it.
fn paren_expr(text: &str) -> Option<(String, &str)> {
    let t = text.trim_start();
    if !t.starts_with('(') {
        return None;
    }
    let close = crate::core::sql_scan::matching_close(t, 0)?;
    Some((t[1..close].trim().to_string(), &t[close + 1..]))
}

/// The word at the start of `text` and what follows it.
fn next_word(text: &str) -> (&str, &str) {
    let t = text.trim_start();
    let end = if t.starts_with('"') {
        t[1..].find('"').map_or(t.len(), |e| e + 2)
    } else {
        t.find(|c: char| c.is_whitespace() || c == '(' || c == ',').unwrap_or(t.len())
    };
    (&t[..end], &t[end..])
}

fn starts_kw(text: &str, kw: &str) -> Option<usize> {
    let t = text.trim_start();
    let skipped = text.len() - t.len();
    (t.len() >= kw.len() && t[..kw.len()].eq_ignore_ascii_case(kw) && t[kw.len()..].starts_with(|c: char| c.is_whitespace() || c == '(') || t.len() == kw.len() && t.eq_ignore_ascii_case(kw))
        .then_some(skipped + kw.len())
}

/// The clauses after `ON t` of CREATE or ALTER POLICY.
#[derive(Default)]
struct Clauses {
    permissive: Option<bool>,
    command: Option<String>,
    roles: Option<Vec<String>>,
    using: Option<String>,
    check: Option<String>,
    rename: Option<String>,
}

fn clauses(mut rest: &str) -> Result<Clauses, Refusal> {
    let mut c = Clauses::default();
    let syntax = |what: &str| ("42601", format!("syntax error in POLICY at {what}"));
    loop {
        let t = rest.trim_start();
        if t.is_empty() || t == ";" {
            return Ok(c);
        }
        if let Some(n) = starts_kw(t, "AS") {
            let (w, after) = next_word(&t[n..]);
            c.permissive = Some(match w.to_ascii_uppercase().as_str() {
                "PERMISSIVE" => true,
                "RESTRICTIVE" => false,
                _ => return Err(syntax(w)),
            });
            rest = after;
        } else if let Some(n) = starts_kw(t, "FOR") {
            let (w, after) = next_word(&t[n..]);
            let w = w.to_ascii_uppercase();
            if !matches!(w.as_str(), "ALL" | "SELECT" | "INSERT" | "UPDATE" | "DELETE") {
                return Err(syntax(&w));
            }
            c.command = Some(w);
            rest = after;
        } else if let Some(n) = starts_kw(t, "TO") {
            let mut after = &t[n..];
            let mut roles = Vec::new();
            loop {
                let (w, more) = next_word(after);
                if w.is_empty() {
                    return Err(syntax("TO"));
                }
                let role = match w.to_ascii_uppercase().as_str() {
                    "PUBLIC" => "public".to_string(),
                    _ => ident(w),
                };
                roles.push(role);
                let more_t = more.trim_start();
                if let Some(m) = more_t.strip_prefix(',') {
                    after = m;
                } else {
                    after = more;
                    break;
                }
            }
            c.roles = Some(roles);
            rest = after;
        } else if let Some(n) = starts_kw(t, "USING") {
            let (expr, after) = paren_expr(&t[n..]).ok_or_else(|| syntax("USING"))?;
            c.using = Some(expr);
            rest = after;
        } else if let Some(n) = starts_kw(t, "WITH") {
            let after = &t[n..];
            let Some(m) = starts_kw(after, "CHECK") else {
                return Err(syntax("WITH"));
            };
            let (expr, after) = paren_expr(&after[m..]).ok_or_else(|| syntax("WITH CHECK"))?;
            c.check = Some(expr);
            rest = after;
        } else if let Some(n) = starts_kw(t, "RENAME") {
            let after = &t[n..];
            let Some(m) = starts_kw(after, "TO") else {
                return Err(syntax("RENAME"));
            };
            let (w, after) = next_word(&after[m..]);
            c.rename = Some(ident(w));
            rest = after;
        } else {
            return Err(syntax(t.split_whitespace().next().unwrap_or(t)));
        }
    }
}

/// Parses a policy statement, or `None` for any other statement.
pub fn parse_policy_statement(sql: &str) -> Option<Result<PolicyStatement, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let upper = text.to_ascii_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();
    // ALTER TABLE t … ROW LEVEL SECURITY
    if words.len() >= 6 && words[0] == "ALTER" && words[1] == "TABLE" && upper.ends_with("ROW LEVEL SECURITY") {
        let mut at = 2;
        if words.get(2) == Some(&"IF") {
            at = 4;
        }
        if words.get(at) == Some(&"ONLY") {
            at += 1;
        }
        let table = ident(text.split_whitespace().nth(at)?);
        let action: Vec<&str> = words[at + 1..words.len() - 3].to_vec();
        let (marker, on) = match action.as_slice() {
            ["ENABLE"] => (RLS, true),
            ["DISABLE"] => (RLS, false),
            ["FORCE"] => (RLS_FORCE, true),
            ["NO", "FORCE"] => (RLS_FORCE, false),
            _ => return None,
        };
        return Some(Ok(PolicyStatement::Toggle { table, marker, on }));
    }
    let verb = *words.first()?;
    if words.get(1) != Some(&"POLICY") || !matches!(verb, "CREATE" | "ALTER" | "DROP") {
        return None;
    }
    let body = text.splitn(3, char::is_whitespace).nth(2)?.trim_start();
    Some((|| {
        let mut body = body;
        let mut if_exists = false;
        if verb == "DROP"
            && let Some(n) = starts_kw(body, "IF")
            && let Some(m) = starts_kw(&body[n..], "EXISTS")
        {
            if_exists = true;
            body = &body[n + m..];
        }
        let (name, rest) = next_word(body);
        let name = ident(name);
        let Some(n) = starts_kw(rest, "ON") else {
            return Err(("42601", "POLICY needs ON table".to_string()));
        };
        let (table, rest) = next_word(&rest[n..]);
        let table = ident(table);
        match verb {
            "DROP" => {
                let r = rest.trim().to_ascii_uppercase();
                if !(r.is_empty() || r == "CASCADE" || r == "RESTRICT") {
                    return Err(("42601", format!("syntax error in DROP POLICY at {r}")));
                }
                Ok(PolicyStatement::Drop { table, name, if_exists })
            }
            "CREATE" => {
                let c = clauses(rest)?;
                if c.rename.is_some() {
                    return Err(("42601", "CREATE POLICY takes no RENAME".to_string()));
                }
                let command = c.command.unwrap_or_else(|| "ALL".to_string());
                if command == "INSERT" && c.using.is_some() {
                    return Err(("42601", "only WITH CHECK expression allowed for INSERT".to_string()));
                }
                if matches!(command.as_str(), "SELECT" | "DELETE") && c.check.is_some() {
                    return Err(("42601", "WITH CHECK cannot be applied to SELECT or DELETE".to_string()));
                }
                Ok(PolicyStatement::Create(Policy {
                    table,
                    name,
                    command,
                    permissive: c.permissive.unwrap_or(true),
                    roles: c.roles.unwrap_or_else(|| vec!["public".to_string()]),
                    using: c.using,
                    check: c.check,
                }))
            }
            _ => {
                let c = clauses(rest)?;
                if c.permissive.is_some() || c.command.is_some() {
                    return Err(("42601", "ALTER POLICY cannot change AS or FOR: drop and create it again".to_string()));
                }
                Ok(PolicyStatement::Alter { table, name, rename: c.rename, roles: c.roles, using: c.using, check: c.check })
            }
        }
    })())
}

async fn run(database: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<Vec<u8>, String> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("could not parse: {sql}"))?;
    let response = match tx {
        Some(tx) => executor::execute_in_txn(purpose, database.clone(), tx).await,
        None => executor::execute_as(crate::core::mvcc::owner::OwnerKind::Auth, purpose, database.clone()).await,
    };
    if response.first() == Some(&3) {
        return Err(String::from_utf8_lossy(&response[1..]).to_string());
    }
    Ok(response)
}

fn opt_literal(value: &Option<String>) -> String {
    value.as_deref().map_or("NULL".to_string(), crate::auth::quote)
}

/// The policies `tx` sees, of `table` or of every table.
pub async fn load(database: &Arc<Database>, table: Option<&str>, tx: Option<TxId>) -> Result<Vec<Policy>, String> {
    if !database.has_table(POLICY_TABLE) {
        return Ok(Vec::new());
    }
    let filter = table.map(|t| format!(" WHERE tbl = {}", crate::auth::quote(t))).unwrap_or_default();
    let response = run(database, &format!("SELECT tbl, name, command, permissive, roles, using_expr, check_expr FROM {POLICY_TABLE}{filter}"), tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let text = |i: usize| {
                row.columns.get(i).and_then(|c| {
                    (c.column_type != crate::core::db_type::DbType::NULL).then(|| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                })
            };
            Policy {
                table: text(0).unwrap_or_default(),
                name: text(1).unwrap_or_default(),
                command: text(2).unwrap_or_default(),
                permissive: row.columns.get(3).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0),
                roles: text(4).unwrap_or_default().split(',').filter(|r| !r.is_empty()).map(str::to_string).collect(),
                using: text(5),
                check: text(6),
            }
        })
        .collect())
}

/// Writes `policy`, replacing one of its table and name.
pub async fn store(database: &Arc<Database>, policy: &Policy, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(POLICY_TABLE) {
        run(database, POLICY_TABLE_DDL, None).await?;
    }
    let id = format!("{}\u{1}{}", policy.table, policy.name);
    run(database, &format!("DELETE FROM {POLICY_TABLE} WHERE id = {}", quote(&id)), tx).await?;
    let sql = format!(
        "INSERT INTO {POLICY_TABLE} (id, tbl, name, command, permissive, roles, using_expr, check_expr) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        quote(&id),
        quote(&policy.table),
        quote(&policy.name),
        quote(&policy.command),
        policy.permissive,
        quote(&policy.roles.join(",")),
        opt_literal(&policy.using),
        opt_literal(&policy.check),
    );
    run(database, &sql, tx).await?;
    note_change(database);
    Ok(())
}

/// Removes the policy `name` of `table`, or every policy of `table` for
/// `None` (the table was dropped).
pub async fn remove(database: &Arc<Database>, table: &str, name: Option<&str>, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(POLICY_TABLE) {
        return Ok(());
    }
    let filter = match name {
        Some(n) => format!("id = {}", quote(&format!("{table}\u{1}{n}"))),
        None => format!("tbl = {}", quote(table)),
    };
    run(database, &format!("DELETE FROM {POLICY_TABLE} WHERE {filter}"), tx).await?;
    note_change(database);
    Ok(())
}

/// Bumped by every policy change: a statement's rewrite for one generation
/// is not reused under another.
static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_change(_database: &Database) {
    GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// The policy generation, for a cache that keys on it.
pub fn generation() -> u64 {
    GENERATION.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether `table` has RLS on, and whether it is forced on its owner.
pub fn rls_state(rows: &[AclRow], table: &str) -> (bool, bool) {
    let on = rows.iter().any(|r| r.relation == table && r.privilege == RLS);
    let forced = rows.iter().any(|r| r.relation == table && r.privilege == RLS_FORCE);
    (on, forced)
}

/// Who a statement runs as, for the policies.
pub struct Actor<'a> {
    pub role: &'a str,
    pub superuser: bool,
}

/// Whether `role` has BYPASSRLS (`auth::RoleAttrs`).
fn bypasses(rows: &[AclRow], role: &str) -> bool {
    rows.iter().any(|r| r.relation == crate::auth::ROLE_ATTR_RELATION && r.grantee == role && r.privilege == "BYPASSRLS")
}

/// Whether `actor` is held to `table`'s policies.
fn held(rows: &[AclRow], actor: &Actor, bypass: bool, table: &str) -> bool {
    let (on, forced) = rls_state(rows, table);
    if !on || actor.superuser || bypass {
        return false;
    }
    forced || !privileges::owns(rows, actor.role, table)
}

/// The predicate the policies of `command` give `actor` on its table, from
/// `pick` (the USING or the WITH CHECK of each, a policy's USING standing
/// in for a missing WITH CHECK). `FALSE` when no permissive one applies.
fn predicate(policies: &[Policy], rows: &[AclRow], actor: &Actor, command: &str, check: bool) -> String {
    let acting = privileges::roles_of(rows, actor.role);
    let applies = |p: &&Policy| {
        (p.command == "ALL" || p.command == command) && p.roles.iter().any(|r| r == "public" || acting.contains(r))
    };
    let expr = |p: &Policy| -> String {
        let e = if check { p.check.clone().or_else(|| p.using.clone()) } else { p.using.clone() };
        e.map_or("TRUE".to_string(), |e| format!("({e})"))
    };
    let permissive: Vec<String> = policies.iter().filter(applies).filter(|p| p.permissive).map(expr).collect();
    if permissive.is_empty() {
        return "FALSE".to_string();
    }
    let mut out = format!("({})", permissive.join(" OR "));
    for r in policies.iter().filter(applies).filter(|p| !p.permissive) {
        out.push_str(&format!(" AND {}", expr(r)));
    }
    out
}

/// What [`apply`] made of a statement.
pub struct Applied {
    /// The statement with the read and write predicates in it; `None` when
    /// it needed none.
    pub text: Option<String>,
    /// The table a write stores new rows into and the check they must
    /// pass: the row-at-a-time path stores them.
    pub check: Option<(String, String)>,
}

/// What [`rewrite`] needs, read by [`context`]: the ACL and the policies.
pub struct Context {
    rows: std::sync::Arc<Vec<AclRow>>,
    policies: Vec<Policy>,
    bypass: bool,
}

/// Reads what holding `stmt` to its tables' policies takes; `None` when
/// none of them holds `actor` (no table has RLS on, or none it names, or it
/// is exempt), the usual case, which costs one read of the cached ACL.
pub async fn context(database: &Arc<Database>, stmt: &str, actor: &Actor<'_>, tx: Option<TxId>) -> Result<Option<Context>, Refusal> {
    if actor.superuser {
        return Ok(None);
    }
    let rows = privileges::load_shared(database, tx).await.map_err(|e| ("42501", format!("permission denied: the privilege catalogue could not be read ({e})")))?;
    if !rows.iter().any(|r| r.privilege == RLS) {
        return Ok(None);
    }
    let bypass = bypasses(&rows, actor.role);
    if !crate::core::rql::lock_set::relation_references(stmt).iter().any(|r| held(&rows, actor, bypass, &r.name)) {
        return Ok(None);
    }
    let policies = load(database, None, tx).await.map_err(|e| ("XX000", e))?;
    Ok(Some(Context { rows, policies, bypass }))
}

/// The statement `stmt` held to the policies of the tables it names
/// (module comment). `substitute` puts the session's values in a policy's
/// expression (`current_user`), which runs as part of the statement.
pub fn rewrite(ctx: &Context, stmt: &str, actor: &Actor<'_>, substitute: &dyn Fn(&str) -> String) -> Result<Applied, Refusal> {
    use crate::core::mvcc::table_locks::TableMode;
    let rows = &ctx.rows;
    let bypass = ctx.bypass;
    let references = crate::core::rql::lock_set::relation_references(stmt);
    let held_refs: Vec<_> = references.iter().filter(|r| held(rows, actor, bypass, &r.name)).collect();
    let all = &ctx.policies;
    let of = |t: &str| -> Vec<Policy> { all.iter().filter(|p| p.table == t).cloned().collect() };
    let head = stmt.trim_start().split(|c: char| c.is_whitespace()).next().unwrap_or("").to_ascii_uppercase();
    let mut replaced: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    let mut append: Option<String> = None;
    let mut check = None;
    for r in held_refs {
        let policies = of(&r.name);
        if r.mode == TableMode::AccessShare {
            let pred = substitute(&predicate(&policies, rows, actor, "SELECT", false));
            let quoted = crate::core::rql::views::quote_ident(&r.name);
            let derived = format!("(SELECT * FROM {quoted} WHERE {pred})");
            replaced.push((r.span.clone(), if r.aliased { derived } else { format!("{derived} AS {quoted}") }));
            continue;
        }
        // The statement's own target, at its top level: anything else (a
        // data-modifying CTE, MERGE) is refused rather than half held.
        let top_level = matches!(head.as_str(), "INSERT" | "UPDATE" | "DELETE")
            && crate::core::rql::lock_set::relation_references(stmt)
                .iter()
                .find(|x| x.mode != TableMode::AccessShare)
                .is_some_and(|x| x.span == r.span);
        if !top_level {
            return Err((
                "0A000",
                format!("a write to table {} with row-level security here is supported only as the statement itself, not in a CTE or MERGE", r.name),
            ));
        }
        match head.as_str() {
            "INSERT" => {
                check = Some((r.name.clone(), substitute(&predicate(&policies, rows, actor, "INSERT", true))));
            }
            "UPDATE" | "DELETE" => {
                let mut pred = predicate(&policies, rows, actor, &head, false);
                // The rows it changes are rows it can see.
                let select = predicate(&policies, rows, actor, "SELECT", false);
                pred = format!("{pred} AND {select}");
                append = Some(substitute(&pred));
                if head == "UPDATE" {
                    check = Some((r.name.clone(), substitute(&predicate(&policies, rows, actor, "UPDATE", true))));
                }
            }
            _ => unreachable!("checked above"),
        }
    }
    // Every edit at its place in the original text, applied from the last:
    // a replacement may stand after the WHERE (a subquery in it) as well as
    // before it (a FROM or USING list).
    if let Some(pred) = append {
        let returning = crate::core::rql::lexer_ir::returning_split(stmt).map(|(h, _)| h.len()).unwrap_or(stmt.len());
        match crate::core::rql::join::find_keyword(&stmt[..returning], "WHERE", 0) {
            Some(at) => {
                let after = at + "WHERE".len();
                replaced.push((after..after, format!(" {pred} AND (")));
                replaced.push((returning..returning, ")".to_string()));
            }
            None => replaced.push((returning..returning, format!(" WHERE {pred}"))),
        }
    }
    let mut text = stmt.to_string();
    replaced.sort_by(|(a, _), (b, _)| b.start.cmp(&a.start).then(b.end.cmp(&a.end)));
    for (span, value) in replaced {
        text.replace_range(span, &value);
    }
    Ok(Applied { text: Some(text), check })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r2r3_policy_statements_parse() {
        let Some(Ok(PolicyStatement::Create(p))) = parse_policy_statement(
            "CREATE POLICY own ON docs AS RESTRICTIVE FOR UPDATE TO alice, PUBLIC USING (owner = current_user) WITH CHECK (owner = 'x')",
        ) else {
            panic!()
        };
        assert_eq!((p.table.as_str(), p.name.as_str(), p.command.as_str(), p.permissive), ("docs", "own", "UPDATE", false));
        assert_eq!(p.roles, vec!["alice".to_string(), "public".to_string()]);
        assert_eq!(p.using.as_deref(), Some("owner = current_user"));
        assert_eq!(p.check.as_deref(), Some("owner = 'x'"));
        assert!(matches!(
            parse_policy_statement("ALTER TABLE docs FORCE ROW LEVEL SECURITY"),
            Some(Ok(PolicyStatement::Toggle { marker: RLS_FORCE, on: true, .. }))
        ));
        assert!(matches!(
            parse_policy_statement("ALTER TABLE docs NO FORCE ROW LEVEL SECURITY"),
            Some(Ok(PolicyStatement::Toggle { marker: RLS_FORCE, on: false, .. }))
        ));
        assert!(matches!(parse_policy_statement("DROP POLICY IF EXISTS own ON docs"), Some(Ok(PolicyStatement::Drop { if_exists: true, .. }))));
        // INSERT takes no USING, SELECT no WITH CHECK, as in PostgreSQL.
        assert!(matches!(parse_policy_statement("CREATE POLICY p ON t FOR INSERT USING (true)"), Some(Err(_))));
        assert!(matches!(parse_policy_statement("CREATE POLICY p ON t FOR SELECT WITH CHECK (true)"), Some(Err(_))));
        assert!(parse_policy_statement("ALTER TABLE t ADD COLUMN x INT").is_none());
    }

    fn policy(command: &str, permissive: bool, using: &str) -> Policy {
        Policy {
            table: "t".into(),
            name: format!("{command}{permissive}"),
            command: command.into(),
            permissive,
            roles: vec!["public".into()],
            using: Some(using.into()),
            check: None,
        }
    }

    #[test]
    fn r2r3_policies_combine_as_postgresql_does() {
        let actor = Actor { role: "u", superuser: false };
        let rows: Vec<AclRow> = Vec::new();
        assert_eq!(predicate(&[], &rows, &actor, "SELECT", false), "FALSE", "no permissive policy: nothing");
        let ps = vec![policy("ALL", true, "a"), policy("SELECT", true, "b"), policy("ALL", false, "c"), policy("DELETE", true, "d")];
        assert_eq!(predicate(&ps, &rows, &actor, "SELECT", false), "((a) OR (b)) AND (c)");
        // A restrictive policy alone admits nothing.
        assert_eq!(predicate(&[policy("ALL", false, "c")], &rows, &actor, "SELECT", false), "FALSE");
    }
}
