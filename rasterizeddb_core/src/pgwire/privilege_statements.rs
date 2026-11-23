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
                let current = privileges::owner_of(&rows, &relation).map(str::to_string);
                if !admin && current.as_deref().is_some_and(|c| !privileges::roles_of(&rows, &me).iter().any(|r| r == c)) {
                    return Err(fail("42501", format!("must be owner of table {relation}")));
                }
                let filter = format!("relation = {} AND privilege = 'OWNER'", crate::auth::quote(&relation));
                privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                let row = AclRow { relation, grantee: owner, privilege: "OWNER".into(), grantor: me.clone(), grantable: false };
                privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                Ok("ALTER TABLE")
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
pub(super) async fn check(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<(&'static str, String)> {
    if state.principal.may_administer() || !database.has_table(privileges::ACL_TABLE) {
        return None;
    }
    let me = state.principal.name.as_str();
    let rows = privileges::load(database, state.current_tx()).await.ok()?;
    if !rows.iter().any(|r| r.privilege == "OWNER") {
        return None;
    }
    // Table DDL: the owner's alone.
    if let Some(ddl) = crate::core::rql::views::table_ddl(stmt) {
        use crate::core::rql::views::TableDdl;
        let names: Vec<String> = match ddl {
            TableDdl::Drop { names, .. } => names,
            TableDdl::Alter { name, .. } => vec![name],
            TableDdl::Create { .. } => Vec::new(),
        };
        for name in names {
            if let Some(owner) = privileges::owner_of(&rows, &name) {
                if !privileges::roles_of(&rows, me).iter().any(|r| r == owner) {
                    return Some(("42501", format!("must be owner of table {name}")));
                }
            }
        }
        return None;
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
            if !privileges::allows(&rows, me, &name, "TRUNCATE") {
                return Some(("42501", format!("permission denied for table {name}")));
            }
        }
        return None;
    }
    for r in crate::core::rql::lock_set::relation_references(stmt) {
        if r.name.starts_with("__") {
            continue;
        }
        let privilege = if r.mode == crate::core::mvcc::table_locks::TableMode::AccessShare { "SELECT" } else { write };
        if !privileges::allows(&rows, me, &r.name, privilege) {
            let noun = if matches!(crate::core::rql::views::lookup(database, &r.name, state.current_tx()).await, Ok(Some(_))) {
                "view"
            } else {
                "table"
            };
            return Some(("42501", format!("permission denied for {noun} {}", r.name)));
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
