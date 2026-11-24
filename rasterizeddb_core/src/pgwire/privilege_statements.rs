//! `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, `ALTER … OWNER TO`, the
//! check a non-superuser's statement passes, and the owner a new relation
//! is given (`core::rql::privileges` keeps the rows and decides).

use super::*;
use crate::core::rql::privileges::{self, AclRow, PrivilegeStatement, Target};

/// Whether `name` is a relation: a table, a view or a sequence.
async fn relation_exists(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    database.has_table(name)
        || matches!(crate::core::rql::views::lookup(database, name, tx).await, Ok(Some(_)))
        || matches!(crate::core::rql::sequence_catalog::load(database, Some(&[name.to_string()]), tx).await, Ok(v) if !v.is_empty())
}

async fn role_exists(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    name == "public" || crate::auth::lookup_role_in(database, name, tx).await.is_some()
}

pub(super) async fn handle_privilege_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<PrivilegeStatement, privileges::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let tx = state.current_tx();
    let me = state.principal.name.clone();
    let admin = state.principal.may_administer();
    let rows = match privileges::load(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    let fail = |code: &'static str, message: String| (code, message);
    let result: Result<&'static str, (&'static str, String)> = async {
        match statement {
            PrivilegeStatement::Grant { privileges: privs, target, grantees, grantable } => {
                let tables = match target {
                    Target::Other(_) => return Ok("GRANT"),
                    Target::AllTablesInSchema => {
                        let mut all: Vec<String> = database.relation_names().into_iter().filter(|n| !n.starts_with("__")).collect();
                        all.extend(crate::core::rql::views::load(database, None, tx).await.unwrap_or_default().into_iter().map(|v| v.name));
                        all
                    }
                    Target::Tables(names) => names,
                };
                for p in &privs {
                    if !privileges::TABLE_PRIVILEGES.contains(&p.as_str()) {
                        return Err(fail("0LP01", format!("invalid privilege type {p} for table")));
                    }
                }
                for grantee in &grantees {
                    if !role_exists(database, grantee, tx).await {
                        return Err(fail("42704", format!("role \"{grantee}\" does not exist")));
                    }
                }
                for table in &tables {
                    if !relation_exists(database, table, tx).await {
                        return Err(fail("42P01", format!("relation \"{table}\" does not exist")));
                    }
                    for p in &privs {
                        if !admin && !privileges::may_grant(&rows, &me, table, p) {
                            return Err(fail("42501", format!("permission denied for table {table}")));
                        }
                        for grantee in &grantees {
                            let row = AclRow { relation: table.clone(), grantee: grantee.clone(), privilege: p.clone(), grantor: me.clone(), grantable };
                            privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                        }
                    }
                }
                Ok("GRANT")
            }
            PrivilegeStatement::Revoke { privileges: privs, target, grantees, grant_option_only } => {
                let tables = match target {
                    Target::Other(_) => return Ok("REVOKE"),
                    Target::AllTablesInSchema => {
                        let mut all: Vec<String> = database.relation_names().into_iter().filter(|n| !n.starts_with("__")).collect();
                        all.extend(crate::core::rql::views::load(database, None, tx).await.unwrap_or_default().into_iter().map(|v| v.name));
                        all
                    }
                    Target::Tables(names) => names,
                };
                for table in &tables {
                    for p in &privs {
                        if !admin && !privileges::may_grant(&rows, &me, table, p) {
                            return Err(fail("42501", format!("permission denied for table {table}")));
                        }
                        for grantee in &grantees {
                            let existing = rows.iter().find(|r| r.relation == *table && r.grantee == *grantee && r.privilege == *p);
                            match (existing, grant_option_only) {
                                (Some(row), true) => {
                                    let mut row = row.clone();
                                    row.grantable = false;
                                    privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                                (Some(_), false) => {
                                    let filter = format!(
                                        "relation = {} AND grantee = {} AND privilege = {}",
                                        crate::auth::quote(table),
                                        crate::auth::quote(grantee),
                                        crate::auth::quote(p)
                                    );
                                    privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                                (None, _) => {}
                            }
                        }
                    }
                }
                Ok("REVOKE")
            }
            PrivilegeStatement::GrantRole { roles, members } => {
                if !admin {
                    return Err(fail("42501", "permission denied to grant role: only a superuser grants roles here".to_string()));
                }
                for name in roles.iter().chain(&members) {
                    if name == "public" || !role_exists(database, name, tx).await {
                        return Err(fail("42704", format!("role \"{name}\" does not exist")));
                    }
                }
                for role in &roles {
                    for member in &members {
                        if privileges::roles_of(&rows, role).contains(member) {
                            return Err(fail("0LP01", format!("role \"{member}\" is a member of role \"{role}\"")));
                        }
                        let row = AclRow { relation: format!("@role:{role}"), grantee: member.clone(), privilege: "MEMBER".into(), grantor: me.clone(), grantable: false };
                        privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                    }
                }
                Ok("GRANT ROLE")
            }
            PrivilegeStatement::RevokeRole { roles, members } => {
                if !admin {
                    return Err(fail("42501", "permission denied to revoke role".to_string()));
                }
                for role in &roles {
                    for member in &members {
                        let filter = format!(
                            "relation = {} AND grantee = {} AND privilege = 'MEMBER'",
                            crate::auth::quote(&format!("@role:{role}")),
                            crate::auth::quote(member)
                        );
                        privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                    }
                }
                Ok("REVOKE ROLE")
            }
            PrivilegeStatement::DefaultPrivileges { for_role, grant, privileges: privs, grantees, tables } => {
                let role = for_role.unwrap_or_else(|| me.clone());
                if !admin && role != me {
                    return Err(fail("42501", "permission denied to change default privileges".to_string()));
                }
                if !tables {
                    return Ok("ALTER DEFAULT PRIVILEGES");
                }
                for grantee in &grantees {
                    for p in &privs {
                        if grant {
                            let row = AclRow { relation: format!("@default:{role}"), grantee: grantee.clone(), privilege: p.clone(), grantor: role.clone(), grantable: false };
                            privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                        } else {
                            let filter = format!(
                                "relation = {} AND grantee = {} AND privilege = {}",
                                crate::auth::quote(&format!("@default:{role}")),
                                crate::auth::quote(grantee),
                                crate::auth::quote(p)
                            );
                            privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                        }
                    }
                }
                Ok("ALTER DEFAULT PRIVILEGES")
            }
            PrivilegeStatement::OwnerTo { relation, owner } => {
                if !relation_exists(database, &relation, tx).await {
                    return Err(fail("42P01", format!("relation \"{relation}\" does not exist")));
                }
                if !role_exists(database, &owner, tx).await || owner == "public" {
                    return Err(fail("42704", format!("role \"{owner}\" does not exist")));
                }
                // The owner's, or a superuser's: a relation with no owner
                // is nobody's to give away. And, as PostgreSQL asks, only to a
                // role the giver acts as.
                if !admin {
                    if !privileges::owns(&rows, &me, &relation) {
                        return Err(fail("42501", format!("must be owner of table {relation}")));
                    }
                    if !privileges::roles_of(&rows, &me).iter().any(|r| *r == owner) {
                        return Err(fail("42501", format!("must be able to SET ROLE \"{owner}\"")));
                    }
                }
                let filter = format!("relation = {} AND privilege = 'OWNER'", crate::auth::quote(&relation));
                privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                let row = AclRow { relation, grantee: owner, privilege: "OWNER".into(), grantor: me.clone(), grantable: false };
                privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                Ok("ALTER TABLE")
            }
            PrivilegeStatement::ReassignOwned { from, to } => {
                if !role_exists(database, &to, tx).await {
                    return Err(fail("42704", format!("role \"{to}\" does not exist")));
                }
                // As PostgreSQL asks: the giver acts as every role named.
                let mine = privileges::roles_of(&rows, &me);
                for r in from.iter().chain(std::iter::once(&to)) {
                    if !admin && !mine.iter().any(|m| m == r) {
                        return Err(fail("42501", format!("permission denied to reassign objects: must be able to SET ROLE \"{r}\"")));
                    }
                }
                let owned: Vec<AclRow> =
                    rows.iter().filter(|r| r.privilege == "OWNER" && from.contains(&r.grantee)).cloned().collect();
                for r in owned {
                    let filter = format!("relation = {} AND privilege = 'OWNER'", crate::auth::quote(&r.relation));
                    privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                    let row = AclRow { relation: r.relation, grantee: to.clone(), privilege: "OWNER".into(), grantor: me.clone(), grantable: false };
                    privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                }
                Ok("REASSIGN OWNED")
            }
            PrivilegeStatement::DropOwned { roles } => {
                let mine = privileges::roles_of(&rows, &me);
                for r in &roles {
                    if !admin && !mine.iter().any(|m| m == r) {
                        return Err(fail("42501", format!("permission denied to drop objects: must be able to SET ROLE \"{r}\"")));
                    }
                }
                let owned: Vec<&str> = rows
                    .iter()
                    .filter(|r| r.privilege == "OWNER" && roles.contains(&r.grantee))
                    .map(|r| r.relation.as_str())
                    .take(10)
                    .collect();
                if !owned.is_empty() {
                    return Err(fail(
                        "2BP01",
                        format!(
                            "DROP OWNED here revokes privileges only, and {} still own {}: REASSIGN OWNED or drop them first",
                            roles.join(", "),
                            owned.join(", ")
                        ),
                    ));
                }
                for r in &roles {
                    let filter = format!(
                        "grantee = {} AND privilege <> 'OWNER' AND relation <> {}",
                        crate::auth::quote(r),
                        crate::auth::quote(crate::auth::ROLE_ATTR_RELATION)
                    );
                    privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                    let defaults = format!("relation = {}", crate::auth::quote(&format!("@default:{r}")));
                    privileges::delete_where(database, &defaults, tx).await.map_err(|e| fail("XX000", e))?;
                }
                Ok("DROP OWNED")
            }
        }
    }
    .await;
    match result {
        Ok(tag) => send_command_complete(stream, tag).await,
        Err((code, message)) => fail_statement(state, stream, code, &message).await,
    }
}

/// The refusal for a non-superuser's statement that names a relation it
/// holds no privilege on; `None` when it may run. Read off the statement as
/// written, views unexpanded, so a view's own privileges decide (its query
/// runs with its owner's, as in PostgreSQL).
///
/// Fails closed: an ACL that cannot be read refuses, and a relation with no
/// owner is nobody's but a superuser's (`privileges::allows`). A name that is
/// no relation (a CTE's, an alias) needs nothing, and the engine's own
/// relations (`__…`, which only its rewrites write) are not asked about: the
/// catalogue guard keeps clients off its catalogues before this.
pub(super) async fn check(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<(&'static str, String)> {
    if state.principal.may_administer() {
        return None;
    }
    let me = state.principal.name.as_str();
    let tx = state.current_tx();
    let rows = match privileges::load(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return Some(("42501", format!("permission denied: the privilege catalogue could not be read ({e})"))),
    };
    // Indexes, triggers and the non-relation objects (roles plan R1).
    if let Some(refusal) = object_ddl_refusal(database, &rows, me, stmt, tx).await {
        return Some(refusal);
    }
    // Table DDL: the owner's alone. A `CREATE TABLE … AS` reads its query,
    // which is checked below as any read.
    if let Some(ddl) = crate::core::rql::views::table_ddl(stmt) {
        use crate::core::rql::views::TableDdl;
        let names: Vec<String> = match ddl {
            TableDdl::Drop { names, .. } => names,
            TableDdl::Alter { name, .. } => vec![name],
            TableDdl::Create { .. } => Vec::new(),
        };
        let creating = names.is_empty();
        for name in names {
            // One that is not there is the engine's to report (42P01, or
            // nothing for `IF EXISTS`).
            if !relation_exists(database, &name, tx).await {
                continue;
            }
            if !privileges::owns(&rows, me, &name) {
                return Some(("42501", format!("must be owner of table {name}")));
            }
        }
        if !creating {
            return None;
        }
    }
    let head = stmt.trim_start().split(|c: char| c.is_whitespace()).next().unwrap_or("").to_ascii_uppercase();
    let write = match head.as_str() {
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "MERGE" => "UPDATE",
        _ => "SELECT",
    };
    if head == "TRUNCATE" {
        for name in stmt.split_whitespace().skip(1).filter(|w| !w.eq_ignore_ascii_case("TABLE")) {
            let name = crate::core::sql_scan::last_name_segment(name.trim_end_matches([',', ';'])).trim_matches('"').to_string();
            if matches!(name.to_ascii_uppercase().as_str(), "ONLY" | "RESTART" | "CONTINUE" | "IDENTITY" | "CASCADE" | "RESTRICT") {
                continue;
            }
            if relation_exists(database, &name, tx).await && !privileges::allows(&rows, me, &name, "TRUNCATE") {
                return Some(("42501", format!("permission denied for table {name}")));
            }
        }
        return None;
    }
    let locking = locking_read(stmt);
    for r in crate::core::rql::lock_set::relation_references(stmt) {
        if r.name.starts_with("__") || !relation_exists(database, &r.name, tx).await {
            continue;
        }
        // What this reference does, from the keyword written before it, so
        // a data-modifying CTE (`WITH d AS (DELETE FROM t …) SELECT …`)
        // needs DELETE on t, not SELECT (roles plan R1).
        let privilege = if r.mode == crate::core::mvcc::table_locks::TableMode::AccessShare {
            "SELECT"
        } else {
            written_as(stmt, r.span.start).unwrap_or(write)
        };
        // A locking read needs UPDATE besides SELECT, as in PostgreSQL.
        if privilege == "SELECT" && locking && !privileges::allows(&rows, me, &r.name, "UPDATE") {
            return Some(("42501", format!("permission denied for table {}", r.name)));
        }
        let view = match crate::core::rql::views::lookup(database, &r.name, tx).await {
            Ok(view) => view,
            Err(e) => return Some(("42501", format!("permission denied: the view catalogue could not be read ({e})"))),
        };
        if !privileges::allows(&rows, me, &r.name, privilege) {
            let noun = if view.is_some() { "view" } else { "table" };
            return Some(("42501", format!("permission denied for {noun} {}", r.name)));
        }
        // What a view reads is read with its owner's rights (PostgreSQL's):
        // the owner must hold them on every relation under it, so revoking
        // them from the owner closes the view too.
        if let Some(view) = view
            && let Some(refusal) = Box::pin(view_owner_refusal(database, &rows, tx, &view, privilege, 0)).await
        {
            return Some(refusal);
        }
    }
    None
}

/// The name a DDL word names: unquoted folded, quoted kept, schema and an
/// argument list dropped.
fn object_name(word: &str) -> String {
    let w = word.split('(').next().unwrap_or(word);
    let w = crate::core::sql_scan::last_name_segment(w.trim_end_matches([',', ';']));
    if w.starts_with('"') { w.trim_matches('"').to_string() } else { w.to_ascii_lowercase() }
}

/// Ownership of the objects that are not relations, and of what is built on
/// a table (roles plan R1):
/// - `CREATE INDEX … ON t` and `DROP INDEX`: the table's owner;
/// - `CREATE TRIGGER … ON t`: the table's owner or its TRIGGER privilege;
///   `DROP TRIGGER … ON t`: the owner;
/// - functions, procedures, domains and types: their creator is recorded as
///   owner (`@function:name`, …); only the owner may replace, alter or drop
///   one. One with no owner recorded (made before owners existed) is a
///   superuser's to change, never the first comer's.
async fn object_ddl_refusal(
    database: &Arc<Database>,
    rows: &[privileges::AclRow],
    me: &str,
    stmt: &str,
    tx: Option<TxId>,
) -> Option<(&'static str, String)> {
    let words: Vec<&str> = stmt.split_whitespace().collect();
    let up: Vec<String> = words.iter().map(|w| w.to_ascii_uppercase()).collect();
    let on_table = || {
        up.iter().position(|w| w == "ON").and_then(|i| {
            let mut j = i + 1;
            if up.get(j).is_some_and(|w| w == "ONLY") {
                j += 1;
            }
            words.get(j).map(|w| object_name(w))
        })
    };
    let first = up.first().map(String::as_str).unwrap_or("");
    let second = up.get(1).map(String::as_str).unwrap_or("");
    let third = up.get(2).map(String::as_str).unwrap_or("");
    let is_index = second == "INDEX" || (second == "UNIQUE" && third == "INDEX");
    if first == "CREATE" && is_index {
        if let Some(t) = on_table()
            && relation_exists(database, &t, tx).await
            && !privileges::owns(rows, me, &t)
        {
            return Some(("42501", format!("must be owner of table {t}")));
        }
        return None;
    }
    let trigger = second == "TRIGGER" || (second == "OR" && third == "REPLACE" && up.get(3).is_some_and(|w| w == "TRIGGER"));
    if first == "CREATE" && trigger {
        if let Some(t) = on_table()
            && relation_exists(database, &t, tx).await
            && !privileges::owns(rows, me, &t)
            && !privileges::allows(rows, me, &t, "TRIGGER")
        {
            return Some(("42501", format!("permission denied for table {t}")));
        }
        return None;
    }
    if first == "DROP" && second == "TRIGGER" {
        if let Some(t) = on_table()
            && relation_exists(database, &t, tx).await
            && !privileges::owns(rows, me, &t)
        {
            return Some(("42501", format!("must be owner of table {t}")));
        }
        return None;
    }
    // Functions, procedures, domains, types.
    let replace = second == "OR" && third == "REPLACE";
    let kind_at = if replace { 3 } else { 1 };
    let kind = up.get(kind_at).map(String::as_str).unwrap_or("");
    let kind = match kind {
        "FUNCTION" | "PROCEDURE" | "DOMAIN" | "TYPE" => kind.to_ascii_lowercase(),
        _ => return None,
    };
    if !matches!(first, "CREATE" | "ALTER" | "DROP") {
        return None;
    }
    let mut at = kind_at + 1;
    if first == "DROP" && up.get(at).is_some_and(|w| w == "IF") && up.get(at + 1).is_some_and(|w| w == "EXISTS") {
        at += 2;
    }
    let name = object_name(words.get(at)?);
    let key = format!("@{kind}:{name}");
    match privileges::owner_of(rows, &key) {
        Some(owner) if owner == me => None,
        Some(owner) => Some(("42501", format!("must be owner of {kind} {name} (owned by {owner})"))),
        // Unowned: a CREATE of a new one makes this role its owner (OR
        // REPLACE too, when nothing by that name exists); changing one made
        // before owners existed is a superuser's.
        None if first == "CREATE" && (!replace || !routine_exists(database, &kind, &name, tx).await) => {
            let row = privileges::AclRow {
                relation: key,
                grantee: me.to_string(),
                privilege: "OWNER".to_string(),
                grantor: me.to_string(),
                grantable: false,
            };
            privileges::put(database, &row, tx).await.err().map(|e| ("42501", format!("permission denied: recording the owner of {kind} {name} failed ({e})")))
        }
        None => Some(("42501", format!("must be owner of {kind} {name}"))),
    }
}

/// Whether a function, procedure or domain named `name` exists.
async fn routine_exists(database: &Arc<Database>, kind: &str, name: &str, tx: Option<TxId>) -> bool {
    match kind {
        "function" | "procedure" => crate::core::rql::functions::load(database, Some(name), tx)
            .await
            .map(|r| !r.is_empty())
            // Unreadable: treat as existing, so it is not taken over.
            .unwrap_or(true),
        "domain" => crate::core::rql::domains::load(database, Some(&[name.to_string()]), tx)
            .await
            .map(|r| !r.is_empty())
            .unwrap_or(true),
        _ => true,
    }
}

/// The privilege a write target needs, from the keyword written right
/// before it at byte `at`: `INSERT INTO`, `UPDATE [ONLY]`, `DELETE FROM`,
/// `MERGE INTO`.
fn written_as(stmt: &str, at: usize) -> Option<&'static str> {
    let before = stmt.get(..at)?.to_ascii_uppercase();
    let words: Vec<&str> = before.split_whitespace().rev().take(3).collect();
    match words.as_slice() {
        ["INTO", "INSERT", ..] => Some("INSERT"),
        ["INTO", "MERGE", ..] => Some("UPDATE"),
        ["UPDATE", ..] | ["ONLY", "UPDATE", ..] => Some("UPDATE"),
        ["FROM", "DELETE", ..] | ["ONLY", "FROM", "DELETE"] => Some("DELETE"),
        _ => None,
    }
}

/// Whether `stmt` takes row locks for update outside quotes and comments
/// (`FOR UPDATE`, `FOR NO KEY UPDATE`).
fn locking_read(stmt: &str) -> bool {
    use crate::core::sql_scan::{skip_comment, skip_quoted};
    // Most statements have no `for` at all: no copy, no scan.
    if !crate::core::sql_scan::contains_ignore_ascii_case(stmt, b"for") {
        return false;
    }
    let b = stmt.as_bytes();
    let mut plain = String::with_capacity(stmt.len());
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_comment(b, i).or_else(|| skip_quoted(b, i)) {
            plain.push(' ');
            i = end.max(i + 1);
            continue;
        }
        plain.push(b[i] as char);
        i += 1;
    }
    let up = plain.to_ascii_uppercase();
    let w: Vec<&str> = up.split_whitespace().collect();
    w.windows(2).any(|p| p == ["FOR", "UPDATE"]) || w.windows(4).any(|p| p == ["FOR", "NO", "KEY", "UPDATE"])
}

/// How deep views over views are followed before the check gives up and
/// refuses: further than any schema writes them, short of a stack.
const VIEW_DEPTH: usize = 32;

/// The refusal when `view`'s owner lacks `privilege` on a relation its query
/// reads, following views under it with their own owners' rights; `None`
/// when the owner holds every one. An unowned view, a view nested past
/// [`VIEW_DEPTH`], or a catalogue that cannot be read refuses.
fn view_owner_refusal<'a>(
    database: &'a Arc<Database>,
    rows: &'a [AclRow],
    tx: Option<TxId>,
    view: &'a crate::core::rql::views::ViewDef,
    privilege: &'a str,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(&'static str, String)>> + Send + 'a>> {
    Box::pin(async move {
        if depth >= VIEW_DEPTH {
            return Some(("42501", format!("permission denied for view {}: views nest too deeply to check", view.name)));
        }
        let Some(owner) = privileges::owner_of(rows, &view.name) else {
            return Some(("42501", format!("permission denied for view {}: it has no owner", view.name)));
        };
        if crate::auth::lookup_role_in(database, owner, tx).await.is_some_and(|r| r.is_superuser) {
            return None;
        }
        for r in crate::core::rql::lock_set::relation_references(&view.definition) {
            if r.name.starts_with("__") || !relation_exists(database, &r.name, tx).await {
                continue;
            }
            if !privileges::allows(rows, owner, &r.name, privilege) {
                return Some((
                    "42501",
                    format!("permission denied for table {}: the owner of view {} does not hold {privilege} on it", r.name, view.name),
                ));
            }
            match crate::core::rql::views::lookup(database, &r.name, tx).await {
                Ok(Some(inner)) => {
                    if let Some(refusal) = view_owner_refusal(database, rows, tx, &inner, privilege, depth + 1).await {
                        return Some(refusal);
                    }
                }
                Ok(None) => {}
                Err(e) => return Some(("42501", format!("permission denied: the view catalogue could not be read ({e})"))),
            }
        }
        None
    })
}

/// The refusal for a non-superuser's statement on views it does not own:
/// `names` that are views, each its owner's alone to drop, rename, refresh
/// or replace. A name that is no view is the statement's to report.
pub(super) async fn require_view_owner(database: &Arc<Database>, state: &SessionState, names: &[String]) -> Option<(&'static str, String)> {
    if names.is_empty() || state.principal.may_administer() {
        return None;
    }
    let tx = state.current_tx();
    let rows = match privileges::load(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return Some(("42501", format!("permission denied: the privilege catalogue could not be read ({e})"))),
    };
    for name in names {
        match crate::core::rql::views::lookup(database, name, tx).await {
            Ok(Some(_)) if !privileges::owns(&rows, &state.principal.name, name) => {
                return Some(("42501", format!("must be owner of view {name}")));
            }
            Ok(_) => {}
            Err(e) => return Some(("42501", format!("permission denied: the view catalogue could not be read ({e})"))),
        }
    }
    None
}

/// The relation a `CREATE` statement makes, when it does not exist yet: a
/// table (not a temporary one), a view, a materialized view or a sequence.
pub(super) async fn about_to_create(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<String> {
    let upper = stmt.trim_start().to_ascii_uppercase();
    if !upper.starts_with("CREATE") || upper.contains(" TEMP ") || upper.contains(" TEMPORARY ") {
        return None;
    }
    let tx = state.current_tx();
    let name = if let Some(crate::core::rql::views::TableDdl::Create { name, .. }) = crate::core::rql::views::table_ddl(stmt) {
        name
    } else if let Some(Ok(crate::core::rql::views::ViewStatement::Create { name, .. })) = crate::core::rql::views::parse_view_statement(stmt) {
        name
    } else if let Some(Ok(crate::core::rql::sequence_catalog::SequenceStatement::Create { name, .. })) =
        crate::core::rql::sequence_catalog::parse_sequence_statement(stmt)
    {
        name
    } else {
        return None;
    };
    (!relation_exists(database, &name, tx).await).then_some(name)
}

/// After a `CREATE` made `name`: its creator owns it, and the creator's
/// default privileges are granted on it.
pub(super) async fn record_owner(database: &Arc<Database>, state: &SessionState, name: &str) {
    let tx = state.current_tx();
    if !relation_exists(database, name, tx).await {
        return;
    }
    let me = state.principal.name.clone();
    let owner = AclRow { relation: name.to_string(), grantee: me.clone(), privilege: "OWNER".into(), grantor: me.clone(), grantable: false };
    if let Err(e) = privileges::put(database, &owner, tx).await {
        log::warn!("the owner of \"{name}\" could not be recorded: {e}");
        return;
    }
    let defaults: Vec<AclRow> = privileges::load(database, tx)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.relation == format!("@default:{me}"))
        .collect();
    for d in defaults {
        let row = AclRow { relation: name.to_string(), grantee: d.grantee, privilege: d.privilege, grantor: me.clone(), grantable: false };
        let _ = privileges::put(database, &row, tx).await;
    }
}
